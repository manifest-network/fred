package placement

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var bucketName = []byte("placements")

// Store is a bbolt-backed placement store with an in-memory cache.
// It maps lease UUIDs to backend names so that read operations can be
// routed to the correct backend after round-robin provisioning.
//
// Reads hit only the in-memory cache (protected by RWMutex).
// Writes go to bbolt first, then update the cache.
type Store struct {
	db        *bolt.DB
	cache     map[string]string
	mu        sync.RWMutex
	closeOnce sync.Once
	closeErr  error
}

// NewStore opens or creates a bbolt database and loads all existing
// placements into memory.
func NewStore(dbPath string) (*Store, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("placement db path is required")
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open placement db: %w", err)
	}

	// Ensure bucket exists
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create placement bucket: %w", err)
	}

	// Load all entries into cache
	cache := make(map[string]string)
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		return b.ForEach(func(k, v []byte) error {
			cache[string(k)] = string(v)
			return nil
		})
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to load placements into cache: %w", err)
	}

	return &Store{
		db:    db,
		cache: cache,
	}, nil
}

// Get returns the backend name for a lease UUID, or "" if not found.
func (s *Store) Get(leaseUUID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cache[leaseUUID]
}

// Set records a lease→backend mapping. Holds the write lock for the
// entire operation so that concurrent reads never see stale data.
func (s *Store) Set(leaseUUID, backendName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(leaseUUID), []byte(backendName))
	}); err != nil {
		return fmt.Errorf("failed to set placement: %w", err)
	}

	s.cache[leaseUUID] = backendName
	return nil
}

// Delete removes a placement. Holds the write lock for the entire
// operation so that concurrent reads never see stale data.
func (s *Store) Delete(leaseUUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Best-effort delete from bbolt; cache is always updated.
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

// SetBatch records multiple placements in a single bbolt transaction.
// Holds the write lock for the entire operation for consistency.
func (s *Store) SetBatch(placements map[string]string) error {
	if len(placements) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		for leaseUUID, backendName := range placements {
			if err := b.Put([]byte(leaseUUID), []byte(backendName)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to set batch placements: %w", err)
	}

	for leaseUUID, backendName := range placements {
		s.cache[leaseUUID] = backendName
	}
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
	uuids := make([]string, 0, len(s.cache))
	for k := range s.cache {
		uuids = append(uuids, k)
	}
	return uuids
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
