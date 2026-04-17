package shared

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

var callbackBucketName = []byte("pending_callbacks")

// CallbackEntry represents a pending callback to be delivered.
//
// Success is retained for backwards compatibility with entries persisted by
// binaries that predate the Status field. New writers populate Success AND
// Status (and Backend). Readers prefer Status when non-empty and fall back to
// Success otherwise; see CallbackSender.ReplayPendingCallbacks.
type CallbackEntry struct {
	LeaseUUID   string                 `json:"lease_uuid"`
	CallbackURL string                 `json:"callback_url"`
	Success     bool                   `json:"success"`
	Status      backend.CallbackStatus `json:"status,omitempty"`
	Backend     string                 `json:"backend,omitempty"`
	Error       string                 `json:"error,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
}

// CallbackStore persists pending callbacks in bbolt so they survive restarts.
type CallbackStore struct {
	*boltStore
}

// CallbackStoreConfig configures the callback store.
type CallbackStoreConfig struct {
	DBPath          string        // Path to bbolt database file
	MaxAge          time.Duration // Max age before entries are cleaned up (0 = no expiry)
	CleanupInterval time.Duration // How often to run cleanup (defaults to MaxAge)
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

	if cfg.MaxAge > 0 {
		base.startCleanup("callback", cfg.CleanupInterval, s.RemoveOlderThan)
	}

	return s, nil
}

// Store persists a callback entry before attempting delivery.
func (s *CallbackStore) Store(entry CallbackEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal callback entry: %w", err)
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(callbackBucketName)
		return b.Put([]byte(entry.LeaseUUID), data)
	})
}

// Remove deletes a callback entry after successful delivery.
func (s *CallbackStore) Remove(leaseUUID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(callbackBucketName)
		return b.Delete([]byte(leaseUUID))
	})
}

// ListPending returns all pending callback entries for replay on startup.
func (s *CallbackStore) ListPending() ([]CallbackEntry, error) {
	var entries []CallbackEntry

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(callbackBucketName)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var entry CallbackEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				slog.Warn("skipping malformed callback entry", "key", string(k), "error", err)
				continue
			}
			entries = append(entries, entry)
		}
		return nil
	})

	return entries, err
}

// RemoveOlderThan deletes callback entries older than maxAge and returns
// the number of entries removed.
func (s *CallbackStore) RemoveOlderThan(maxAge time.Duration) (int, error) {
	return removeOlderThan[CallbackEntry](s.db, callbackBucketName, maxAge, func(e *CallbackEntry) time.Time {
		return e.CreatedAt
	})
}
