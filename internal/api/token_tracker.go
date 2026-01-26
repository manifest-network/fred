package api

import (
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	// ErrTokenAlreadyUsed indicates the token has already been used.
	ErrTokenAlreadyUsed = errors.New("token already used")

	// bucketName is the bbolt bucket for storing used tokens.
	bucketName = []byte("used_tokens")
)

// TokenTracker tracks used authentication tokens to prevent replay attacks.
// It uses bbolt for persistence across restarts.
type TokenTracker struct {
	db              *bolt.DB
	maxAge          time.Duration
	cleanupInterval time.Duration

	// For graceful shutdown
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// TokenTrackerConfig configures the token tracker.
type TokenTrackerConfig struct {
	DBPath          string        // Path to bbolt database file
	MaxAge          time.Duration // How long to track tokens (should match MaxTokenAge)
	CleanupInterval time.Duration // How often to clean up expired entries
}

// NewTokenTracker creates a new token tracker with bbolt persistence.
func NewTokenTracker(cfg TokenTrackerConfig) (*TokenTracker, error) {
	if cfg.DBPath == "" {
		return nil, errors.New("DBPath is required")
	}

	// Apply defaults
	maxAge := cfg.MaxAge
	if maxAge == 0 {
		maxAge = MaxTokenAge
	}
	cleanupInterval := cfg.CleanupInterval
	if cleanupInterval == 0 {
		cleanupInterval = maxAge // Clean up at least as often as tokens expire
	}

	db, err := bolt.Open(cfg.DBPath, 0600, &bolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	// Create bucket if it doesn't exist
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	t := &TokenTracker{
		db:              db,
		maxAge:          maxAge,
		cleanupInterval: cleanupInterval,
		cancel:          cancel,
	}

	// Start background cleanup
	t.wg.Add(1)
	go t.cleanupLoop(ctx)

	// Run initial cleanup to remove any expired entries from previous run
	if err := t.cleanup(); err != nil {
		slog.Warn("initial token cleanup failed", "error", err)
	}

	return t, nil
}

// TryUse attempts to mark a token as used.
// Returns nil if the token was successfully marked (first use).
// Returns ErrTokenAlreadyUsed if the token has already been used.
// The key should be the token's signature (unique per token).
func (t *TokenTracker) TryUse(key string) error {
	keyBytes := []byte(key)
	now := time.Now()
	expiresAt := now.Add(t.maxAge)

	err := t.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)

		// Check if token already exists
		existing := b.Get(keyBytes)
		if existing != nil {
			// Token exists - check if it's still valid (not expired)
			storedExpiry := bytesToTime(existing)
			if now.Before(storedExpiry) {
				return ErrTokenAlreadyUsed
			}
			// Token expired, allow reuse (will be overwritten)
		}

		// Store token with expiry time
		return b.Put(keyBytes, timeToBytes(expiresAt))
	})

	return err
}

// Close shuts down the token tracker gracefully.
func (t *TokenTracker) Close() error {
	t.cancel()
	t.wg.Wait()
	return t.db.Close()
}

// cleanupLoop periodically removes expired tokens.
func (t *TokenTracker) cleanupLoop(ctx context.Context) {
	defer t.wg.Done()

	ticker := time.NewTicker(t.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := t.cleanup(); err != nil {
				slog.Error("token cleanup failed", "error", err)
			}
		}
	}
}

// cleanup removes expired tokens from the database.
func (t *TokenTracker) cleanup() error {
	now := time.Now()
	var expiredCount int

	err := t.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()

		// Collect keys to delete (can't delete while iterating)
		var toDelete [][]byte
		for k, v := c.First(); k != nil; k, v = c.Next() {
			expiresAt := bytesToTime(v)
			if now.After(expiresAt) {
				// Make a copy of the key since cursor reuses the slice
				keyCopy := make([]byte, len(k))
				copy(keyCopy, k)
				toDelete = append(toDelete, keyCopy)
			}
		}

		// Delete expired entries
		for _, k := range toDelete {
			if err := b.Delete(k); err != nil {
				return err
			}
		}

		expiredCount = len(toDelete)
		return nil
	})

	if err == nil && expiredCount > 0 {
		slog.Debug("cleaned up expired tokens", "count", expiredCount)
	}

	return err
}

// Stats returns current statistics about the token tracker.
func (t *TokenTracker) Stats() (total int, err error) {
	err = t.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		total = b.Stats().KeyN
		return nil
	})
	return
}

// timeToBytes converts a time.Time to bytes for storage.
func timeToBytes(t time.Time) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(t.UnixNano()))
	return b
}

// bytesToTime converts bytes back to time.Time.
func bytesToTime(b []byte) time.Time {
	if len(b) != 8 {
		return time.Time{}
	}
	nano := int64(binary.BigEndian.Uint64(b))
	return time.Unix(0, nano)
}
