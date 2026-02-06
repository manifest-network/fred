package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/util"
)

var callbackBucketName = []byte("pending_callbacks")

// CallbackEntry represents a pending callback to be delivered.
type CallbackEntry struct {
	LeaseUUID   string    `json:"lease_uuid"`
	CallbackURL string    `json:"callback_url"`
	Success     bool      `json:"success"`
	Error       string    `json:"error,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// CallbackStore persists pending callbacks in bbolt so they survive restarts.
type CallbackStore struct {
	db     *bolt.DB
	maxAge time.Duration

	cancel    context.CancelFunc
	wg        *sync.WaitGroup
	closeOnce *sync.Once
	closeErr  error // captured by first Close() call
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
	if cfg.DBPath == "" {
		return nil, fmt.Errorf("callback db path is required")
	}

	db, err := bolt.Open(cfg.DBPath, 0600, &bolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open callback db: %w", err)
	}

	// Create bucket if it doesn't exist
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(callbackBucketName)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create callback bucket: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &CallbackStore{
		db:        db,
		maxAge:    cfg.MaxAge,
		cancel:    cancel,
		wg:        &sync.WaitGroup{},
		closeOnce: &sync.Once{},
	}

	// Start background cleanup if expiry is enabled
	if cfg.MaxAge > 0 {
		// Run initial cleanup to remove stale entries from previous run
		if removed, cleanupErr := s.RemoveOlderThan(cfg.MaxAge); cleanupErr != nil {
			slog.Warn("initial callback cleanup failed", "error", cleanupErr)
		} else if removed > 0 {
			slog.Info("removed expired callbacks on startup", "count", removed, "max_age", cfg.MaxAge)
		}

		interval := cfg.CleanupInterval
		if interval <= 0 {
			interval = cfg.MaxAge
		}
		s.wg.Go(func() {
			util.StartCleanupLoop(ctx, interval, s.cleanup, "callback")
		})
	}

	return s, nil
}

// cleanup removes expired callback entries. Used by the background cleanup loop.
func (s *CallbackStore) cleanup() error {
	removed, err := s.RemoveOlderThan(s.maxAge)
	if err != nil {
		return err
	}
	if removed > 0 {
		slog.Debug("cleaned up expired callbacks", "count", removed)
	}
	return nil
}

// Healthy checks if the bbolt database is accessible and the callback bucket exists.
func (s *CallbackStore) Healthy() error {
	return s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(callbackBucketName) == nil {
			return errors.New("callback bucket missing")
		}
		return nil
	})
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
	cutoff := time.Now().Add(-maxAge)
	removed := 0

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(callbackBucketName)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var entry CallbackEntry
			if err := json.Unmarshal(v, &entry); err != nil {
				// Remove malformed entries
				if delErr := c.Delete(); delErr != nil {
					return delErr
				}
				removed++
				continue
			}
			if entry.CreatedAt.Before(cutoff) {
				if delErr := c.Delete(); delErr != nil {
					return delErr
				}
				removed++
			}
		}
		return nil
	})

	return removed, err
}

// Close shuts down the callback store gracefully.
// Close is idempotent: the first call closes the database and captures
// any error; subsequent calls return the same error.
func (s *CallbackStore) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.wg.Wait()
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}
