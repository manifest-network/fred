package api

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/metrics/background"
	"github.com/manifest-network/fred/internal/util"
)

var (
	// ErrTokenAlreadyUsed indicates the token has already been used.
	ErrTokenAlreadyUsed = errors.New("token already used")

	// ErrInvalidReplayClaim indicates absent validation or an expired token.
	ErrInvalidReplayClaim = errors.New("invalid or expired replay claim")

	// bucketName is the bbolt bucket for storing used tokens.
	bucketName = []byte("used_tokens")
)

// TokenReplayClaim binds the canonical signature to its signed validity window.
// Only successful cryptographic validation creates a usable claim; callers cannot
// choose a different cache key or shorten the replay lifetime.
type TokenReplayClaim struct {
	signature string
	expiresAt time.Time
}

// TokenTracker tracks used authentication tokens to prevent replay attacks.
// It uses bbolt for persistence across restarts.
type TokenTracker struct {
	db              *bolt.DB
	cleanupInterval time.Duration

	// For graceful shutdown
	cancel    context.CancelFunc
	wg        *sync.WaitGroup // Pointer to avoid copy-by-value issues
	closeOnce *sync.Once      // Pointer to avoid copy-by-value issues
	closeErr  error
}

// TokenTrackerConfig configures the token tracker.
type TokenTrackerConfig struct {
	DBPath          string        // Path to bbolt database file
	CleanupInterval time.Duration // How often to clean up expired entries
}

// NewTokenTracker creates a new token tracker with bbolt persistence.
func NewTokenTracker(cfg TokenTrackerConfig) (*TokenTracker, error) {
	if cfg.DBPath == "" {
		return nil, errors.New("db path is required")
	}

	// Apply defaults using cmp.Or (returns first non-zero value)
	cleanupInterval := cmp.Or(cfg.CleanupInterval, MaxTokenAge)

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
		_ = db.Close() // Best effort cleanup on init failure
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	t := &TokenTracker{
		db:              db,
		cleanupInterval: cleanupInterval,
		cancel:          cancel,
		wg:              &sync.WaitGroup{},
		closeOnce:       &sync.Once{},
	}

	// Start background cleanup (using WaitGroup.Go for Go 1.25+)
	t.wg.Go(func() { t.cleanupLoop(ctx) })

	// Run initial cleanup to remove any expired entries from previous run
	if err := t.cleanup(); err != nil {
		slog.Warn("initial token cleanup failed", "error", err)
	}

	return t, nil
}

// TryUse atomically consumes a validated token for its entire signed lifetime.
// bbolt serializes write transactions; retrying a closed database cannot repair it.
func (t *TokenTracker) TryUse(claim TokenReplayClaim) error {
	return t.db.Update(func(tx *bolt.Tx) error {
		if claim.signature == "" || !time.Now().Before(claim.expiresAt) {
			return ErrInvalidReplayClaim
		}
		b := tx.Bucket(bucketName)
		key := []byte(claim.signature)
		if b.Get(key) != nil {
			return ErrTokenAlreadyUsed
		}
		return b.Put(key, util.TimeToBytes(claim.expiresAt))
	})
}

// Healthy checks if the bbolt database is accessible and the token bucket exists.
func (t *TokenTracker) Healthy() error {
	return t.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketName) == nil {
			return errors.New("token bucket missing")
		}
		return nil
	})
}

// Close shuts down the token tracker gracefully.
// Close is idempotent and safe to call multiple times.
func (t *TokenTracker) Close() error {
	t.closeOnce.Do(func() {
		// Signal cleanup goroutine to stop
		t.cancel()

		// Wait for cleanup goroutine to finish
		t.wg.Wait()

		// Close the database
		t.closeErr = t.db.Close()
	})
	return t.closeErr
}

// cleanupLoop periodically removes expired tokens.
// Note: WaitGroup.Done is handled by the caller via wg.Go() (Go 1.25+).
func (t *TokenTracker) cleanupLoop(ctx context.Context) {
	util.StartCleanupLoop(ctx, t.cleanupInterval, t.cleanup, "token",
		func(any) { background.CleanupPanicsTotal.WithLabelValues("token").Inc() },
	)
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
			expiresAt := util.BytesToTime(v)
			// Previous releases stored first-use time + MaxTokenAge. Keep
			// those rows through the maximum accepted future skew as well,
			// including across an immediate upgrade/restart. The extra retention
			// is harmless for new claims, which reject their own expiry.
			if !expiresAt.IsZero() && !now.Before(expiresAt.Add(MaxFutureClockSkew)) {
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
