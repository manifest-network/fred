package shared

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/util"
)

var (
	// callbackBucketName is the v0.13 callback queue. Keep it readable and
	// otherwise untouched by new writes so a rollback binary never sees a key
	// it cannot remove (v0.13 deletes entries by lease UUID).
	callbackBucketName = []byte("pending_callbacks")

	// callbackV2BucketName stores one entry per delivery UUID. Separating the
	// schema from the legacy bucket makes multiple callbacks for one lease safe
	// while keeping rollback binaries from replaying v2 entries forever.
	callbackV2BucketName = []byte("pending_callbacks_v2")
)

type callbackStorageVersion uint8

const (
	callbackStorageUnknown callbackStorageVersion = iota
	callbackStorageLegacy
	callbackStorageV2
)

// CallbackEntry represents a pending callback to be delivered.
//
// Success is retained for backwards compatibility with entries persisted by
// binaries that predate the Status field. New writers populate Success AND
// Status (and Backend). Readers prefer Status when non-empty and fall back to
// Success otherwise; see callback_sender.ReplayPendingCallbacks.
type CallbackEntry struct {
	// DeliveryID uniquely identifies this delivery attempt in the durable v2
	// queue. Legacy v0.13 entries have no ID and remain supported on replay.
	DeliveryID  string                 `json:"delivery_id,omitempty"`
	LeaseUUID   string                 `json:"lease_uuid"`
	CallbackURL string                 `json:"callback_url"`
	Success     bool                   `json:"success"`
	Status      backend.CallbackStatus `json:"status,omitempty"`
	Backend     string                 `json:"backend,omitempty"`
	Error       string                 `json:"error,omitempty"`
	// Retained persists the best-effort deprovision retain-success flag so a
	// restart-replayed callback keeps it. Legacy entries default to false.
	Retained  bool      `json:"retained,omitempty"`
	CreatedAt time.Time `json:"created_at"`

	// storageVersion/storageKey are populated by StoreEntry/ListPending and
	// deliberately excluded from JSON. They let RemoveEntry delete exactly the
	// record that was delivered, including a legacy record without DeliveryID.
	storageVersion callbackStorageVersion
	storageKey     string
	storageDigest  [sha256.Size]byte
}

// CallbackStore persists pending callbacks in bbolt so they survive restarts.
type CallbackStore struct {
	*boltStore
}

// CallbackStoreConfig configures the callback store.
type CallbackStoreConfig struct {
	DBPath          string            // Path to bbolt database file
	MaxAge          time.Duration     // Max age before entries are cleaned up (0 = no expiry)
	CleanupInterval time.Duration     // How often to run cleanup (defaults to MaxAge)
	OnCleanupPanic  util.PanicHandler // Optional: invoked on cleanup-loop panic (e.g., bump a metric)
}

// NewCallbackStore opens or creates a bbolt database for callback persistence.
// If MaxAge > 0, a background cleanup loop removes expired entries periodically
// and an initial cleanup runs immediately to clear stale entries from previous runs.
func NewCallbackStore(cfg CallbackStoreConfig) (*CallbackStore, error) {
	base, err := openBoltStore(boltStoreConfig{
		DBPath:     cfg.DBPath,
		BucketName: callbackBucketName,
		MaxAge:     cfg.MaxAge,
		Label:      "callback",
	})
	if err != nil {
		return nil, err
	}

	s := &CallbackStore{boltStore: base}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.CreateBucketIfNotExists(callbackV2BucketName)
		return createErr
	}); err != nil {
		_ = base.Close()
		return nil, fmt.Errorf("failed to create callback v2 bucket: %w", err)
	}

	if cfg.MaxAge > 0 {
		base.startCleanup("callback", cfg.CleanupInterval, s.RemoveOlderThan, cfg.OnCleanupPanic)
	}

	return s, nil
}

// Store persists a callback entry before attempting delivery.
func (s *CallbackStore) Store(entry CallbackEntry) error {
	_, err := s.StoreEntry(entry)
	return err
}

// StoreEntry persists a callback and returns its durable delivery identity.
// Callers that will remove a successfully delivered callback must retain the
// returned value and pass it to RemoveEntry; removing by lease would also
// discard other independent callbacks for that lease.
func (s *CallbackStore) StoreEntry(entry CallbackEntry) (CallbackEntry, error) {
	if entry.DeliveryID == "" {
		id, err := uuid.NewRandom()
		if err != nil {
			return CallbackEntry{}, fmt.Errorf("failed to allocate callback delivery ID: %w", err)
		}
		entry.DeliveryID = id.String()
	} else if err := validateCallbackDeliveryID(entry.DeliveryID); err != nil {
		return CallbackEntry{}, err
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return CallbackEntry{}, fmt.Errorf("failed to marshal callback entry: %w", err)
	}

	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(callbackV2BucketName)
		if b.Get([]byte(entry.DeliveryID)) != nil {
			return fmt.Errorf("callback delivery ID already exists: %s", entry.DeliveryID)
		}
		return b.Put([]byte(entry.DeliveryID), data)
	})
	if err != nil {
		return CallbackEntry{}, err
	}

	entry.storageVersion = callbackStorageV2
	entry.storageKey = entry.DeliveryID
	entry.storageDigest = sha256.Sum256(data)
	return entry, nil
}

// Remove deletes every pending callback for a lease. It intentionally remains
// lease-wide for deprovision cleanup and compatibility with legacy callers.
// Delivery success paths must use RemoveEntry instead.
func (s *CallbackStore) Remove(leaseUUID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		for _, source := range []struct {
			bucket []byte
			legacy bool
		}{
			{callbackBucketName, true},
			{callbackV2BucketName, false},
		} {
			b := tx.Bucket(source.bucket)
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				// The direct key comparison preserves removal of malformed legacy
				// records whose value can no longer be decoded.
				if source.legacy && string(k) == leaseUUID {
					if err := c.Delete(); err != nil {
						return err
					}
					continue
				}

				var entry CallbackEntry
				if err := json.Unmarshal(v, &entry); err != nil || entry.LeaseUUID != leaseUUID {
					continue
				}
				if err := c.Delete(); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// RemoveEntry deletes exactly one delivered callback. The entry must come
// from StoreEntry or ListPending so its durable bucket/key identity is known.
func (s *CallbackStore) RemoveEntry(entry CallbackEntry) error {
	bucketName := callbackV2BucketName
	key := entry.DeliveryID
	isV2 := true
	switch entry.storageVersion {
	case callbackStorageLegacy:
		bucketName = callbackBucketName
		key = entry.storageKey
		isV2 = false
	case callbackStorageV2:
		key = entry.storageKey
		if entry.DeliveryID != key {
			return fmt.Errorf("callback delivery ID %q does not match durable key %q", entry.DeliveryID, key)
		}
	case callbackStorageUnknown:
		return fmt.Errorf("callback entry has no durable storage capability")
	default:
		return fmt.Errorf("unknown callback storage version %d", entry.storageVersion)
	}
	if key == "" {
		return fmt.Errorf("callback entry has empty durable storage key")
	}
	if entry.storageDigest == ([sha256.Size]byte{}) {
		return fmt.Errorf("callback entry has no durable value capability")
	}
	if isV2 {
		if err := validateCallbackDeliveryID(key); err != nil {
			return err
		}
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		current := b.Get([]byte(key))
		if current == nil {
			return nil
		}
		if sha256.Sum256(current) != entry.storageDigest {
			return fmt.Errorf("callback entry changed before precise removal")
		}
		return b.Delete([]byte(key))
	})
}

// ListPending returns all pending callback entries for replay on startup.
func (s *CallbackStore) ListPending() ([]CallbackEntry, error) {
	var entries []CallbackEntry

	err := s.db.View(func(tx *bolt.Tx) error {
		for _, source := range []struct {
			bucket  []byte
			version callbackStorageVersion
		}{
			{callbackBucketName, callbackStorageLegacy},
			{callbackV2BucketName, callbackStorageV2},
		} {
			b := tx.Bucket(source.bucket)
			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				var entry CallbackEntry
				if err := json.Unmarshal(v, &entry); err != nil {
					slog.Warn("skipping malformed callback entry", "bucket", string(source.bucket), "key", string(k), "error", err)
					continue
				}
				if source.version == callbackStorageV2 {
					if err := validateCallbackDeliveryID(entry.DeliveryID); err != nil || entry.DeliveryID != string(k) {
						slog.Warn("skipping callback entry with invalid delivery identity",
							"bucket", string(source.bucket),
							"key", string(k),
							"delivery_id", entry.DeliveryID,
							"error", err,
						)
						continue
					}
				}
				entry.storageVersion = source.version
				entry.storageKey = string(k)
				entry.storageDigest = sha256.Sum256(v)
				entries = append(entries, entry)
			}
		}
		return nil
	})
	// Replaying oldest-first preserves lifecycle ordering without using the
	// random UUID key order. Delivery identity breaks ties deterministically.
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].CreatedAt.Equal(entries[j].CreatedAt) {
			return entries[i].storageKey < entries[j].storageKey
		}
		return entries[i].CreatedAt.Before(entries[j].CreatedAt)
	})

	return entries, err
}

// RemoveOlderThan deletes callback entries older than maxAge and returns
// the number of entries removed.
func (s *CallbackStore) RemoveOlderThan(maxAge time.Duration) (int, error) {
	legacyRemoved, err := removeOlderThan[CallbackEntry](s.db, callbackBucketName, maxAge, func(e *CallbackEntry) time.Time {
		return e.CreatedAt
	})
	if err != nil {
		return legacyRemoved, err
	}
	v2Removed, err := removeOlderThan[CallbackEntry](s.db, callbackV2BucketName, maxAge, func(e *CallbackEntry) time.Time {
		return e.CreatedAt
	})
	return legacyRemoved + v2Removed, err
}

// Healthy checks both the legacy compatibility queue and the v2 delivery-ID
// queue. The embedded boltStore health check only knows its legacy bucket.
func (s *CallbackStore) Healthy() error {
	return s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(callbackBucketName) == nil {
			return fmt.Errorf("legacy callback bucket missing")
		}
		if tx.Bucket(callbackV2BucketName) == nil {
			return fmt.Errorf("callback v2 bucket missing")
		}
		return nil
	})
}

func validateCallbackDeliveryID(value string) error {
	id, err := uuid.Parse(value)
	if err != nil || id.String() != value || id.Version() != uuid.Version(4) || id.Variant() != uuid.RFC4122 {
		return fmt.Errorf("callback delivery ID must be a canonical UUIDv4: %q", value)
	}
	return nil
}
