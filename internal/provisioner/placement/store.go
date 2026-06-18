package placement

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var bucketName = []byte("placements")

// record is the stored placement: which backend serves a lease and when we
// first learned that placement (SetAt). SetAt gates the reconciler's pruner so
// a placement set during a slow reconcile sweep is not mistaken for an orphan
// (ENG-335). Encoded as JSON in bbolt.
type record struct {
	Backend string    `json:"backend"`
	SetAt   time.Time `json:"set_at"`
}

// Store is a bbolt-backed placement store with an in-memory cache.
// It maps lease UUIDs to a backend name plus a first-seen timestamp.
//
// Reads hit only the in-memory cache (protected by RWMutex).
// Writes go to bbolt first, then update the cache.
type Store struct {
	db        *bolt.DB
	cache     map[string]record
	now       func() time.Time
	mu        sync.RWMutex
	closeOnce sync.Once
	closeErr  error
}

// Option configures a Store at construction.
type Option func(*Store)

// WithClock injects the clock used to stamp SetAt. Defaults to time.Now.
// This is a real dependency seam (always set), used for deterministic tests.
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// NewStore opens or creates a bbolt database and loads all existing
// placements into memory.
func NewStore(dbPath string, opts ...Option) (*Store, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("placement db path is required")
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open placement db: %w", err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create placement bucket: %w", err)
	}

	// Load all entries into cache.
	cache := make(map[string]record)
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		return b.ForEach(func(k, v []byte) error {
			cache[string(k)] = decodeRecord(string(k), v)
			return nil
		})
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to load placements into cache: %w", err)
	}

	s := &Store{
		db:    db,
		cache: cache,
		now:   time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	// A WithClock(nil) misuse must not nil out the clock and panic later in Set.
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

// decodeRecord parses a stored value. New values are JSON objects (first byte
// '{'); anything else is a legacy raw backend name written before ENG-335,
// loaded with a zero SetAt so the pruner may remove it immediately.
func decodeRecord(leaseUUID string, v []byte) record {
	if len(v) > 0 && v[0] == '{' {
		var r record
		if err := json.Unmarshal(v, &r); err != nil {
			// First byte says JSON but it will not parse — a corrupt entry.
			// Return an empty record (no backend, zero SetAt) so it reads as
			// "no placement" and the pruner can clear it, rather than treating
			// the raw bytes as a backend name. Include the key so the corrupt
			// bbolt entry can be located.
			slog.Warn("placement: dropping unparseable record",
				"lease_uuid", leaseUUID, "error", err)
			return record{}
		}
		return r
	}
	// Legacy pre-ENG-335 value: a raw backend name with no timestamp.
	return record{Backend: string(v)}
}

func encodeRecord(r record) ([]byte, error) {
	return json.Marshal(r)
}

// Get returns the backend name for a lease UUID, or "" if not found.
func (s *Store) Get(leaseUUID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cache[leaseUUID].Backend
}

// SetAt returns the first-seen time recorded for a lease and whether a
// placement exists. A legacy record returns a zero time with ok=true.
func (s *Store) SetAt(leaseUUID string) (time.Time, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.cache[leaseUUID]
	return r.SetAt, ok
}

// Set records a lease→backend mapping, stamping SetAt with the current clock.
// SetAt is stored in UTC: t.UTC() canonicalizes storage AND strips the monotonic
// clock reading, so the in-memory value matches the JSON-reloaded value exactly
// (a time.Time keeps a monotonic reading that JSON drops; UTC removes it up front
// so comparisons are consistent across a persist boundary). Holds the write lock
// for the entire operation so concurrent reads never see stale data.
func (s *Store) Set(leaseUUID, backendName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := record{Backend: backendName, SetAt: s.now().UTC()}
	return s.put(leaseUUID, r)
}

// put writes one record to bbolt then the cache. Caller holds s.mu.
func (s *Store) put(leaseUUID string, r record) error {
	enc, err := encodeRecord(r)
	if err != nil {
		return fmt.Errorf("failed to encode placement: %w", err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(leaseUUID), enc)
	}); err != nil {
		return fmt.Errorf("failed to set placement: %w", err)
	}
	s.cache[leaseUUID] = r
	return nil
}

// Delete removes a placement. Holds the write lock for the entire operation.
func (s *Store) Delete(leaseUUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Delete([]byte(leaseUUID))
	}); err != nil {
		slog.Warn("failed to delete placement from bbolt",
			"lease_uuid", leaseUUID,
			"error", err,
		)
	}

	delete(s.cache, leaseUUID)
}

// SetBatch records multiple placements in a single bbolt transaction. An
// existing record's SetAt is PRESERVED (this is passive sync from backend
// state, run every sweep — it must not reset the first-seen clock); new
// entries are stamped with the current clock.
func (s *Store) SetBatch(placements map[string]string) error {
	if len(placements) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now().UTC() // UTC: canonical + strips monotonic (see Set)
	merged := make(map[string]record, len(placements))
	for leaseUUID, backendName := range placements {
		setAt := now
		if existing, ok := s.cache[leaseUUID]; ok {
			setAt = existing.SetAt
		}
		merged[leaseUUID] = record{Backend: backendName, SetAt: setAt}
	}

	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		for leaseUUID, r := range merged {
			enc, err := encodeRecord(r)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(leaseUUID), enc); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to set batch placements: %w", err)
	}

	maps.Copy(s.cache, merged)
	return nil
}

// Count returns the number of placements in the cache.
func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.cache)
}

// List returns all lease UUIDs that have placements.
func (s *Store) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Collect(maps.Keys(s.cache))
}

// Healthy checks that the bbolt database and bucket are accessible.
func (s *Store) Healthy() error {
	return s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketName) == nil {
			return errors.New("placements bucket missing")
		}
		return nil
	})
}

// Close closes the bbolt database. Safe to call multiple times.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}
