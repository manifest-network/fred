package api

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/util"
)

// Retry configuration for bbolt operations under contention.
const (
	bboltMaxRetries     = 3
	bboltInitialBackoff = 10 * time.Millisecond
	bboltMaxBackoff     = 100 * time.Millisecond
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
	cancel    context.CancelFunc
	wg        *sync.WaitGroup // Pointer to avoid copy-by-value issues
	closeOnce *sync.Once      // Pointer to avoid copy-by-value issues
	closeErr  error
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
		return nil, errors.New("db path is required")
	}

	// Apply defaults using cmp.Or (returns first non-zero value)
	maxAge := cmp.Or(cfg.MaxAge, MaxTokenAge)
	cleanupInterval := cmp.Or(cfg.CleanupInterval, maxAge) // Default: clean up as often as tokens expire

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
		maxAge:          maxAge,
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

// TryUse attempts to mark a token as used.
// Returns nil if the token was successfully marked (first use).
// Returns ErrTokenAlreadyUsed if the token has already been used.
// The key should be the token's signature (unique per token).
func (t *TokenTracker) TryUse(key string) error {
	keyBytes := []byte(key)
	now := time.Now()
	expiresAt := now.Add(t.maxAge)

	// Configure exponential backoff for database contention
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = bboltInitialBackoff
	bo.MaxInterval = bboltMaxBackoff
	bo.MaxElapsedTime = 0 // We control max retries via WithMaxRetries

	// Wrap with max retries
	boWithRetries := backoff.WithMaxRetries(bo, bboltMaxRetries-1) // -1 because first attempt doesn't count

	operation := func() error {
		err := t.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketName)

			// Check if token already exists
			existing := b.Get(keyBytes)
			if existing != nil {
				// Token exists - check if it's still valid (not expired)
				storedExpiry := util.BytesToTime(existing)
				if now.Before(storedExpiry) {
					return ErrTokenAlreadyUsed
				}
				// Token expired, allow reuse (will be overwritten)
			}

			// Store token with expiry time
			return b.Put(keyBytes, util.TimeToBytes(expiresAt))
		})

		// Don't retry for application-level errors (token already used)
		if err == nil || errors.Is(err, ErrTokenAlreadyUsed) {
			return backoff.Permanent(err)
		}

		// Only retry on timeout errors (database locked)
		if errors.Is(err, bolt.ErrTimeout) || errors.Is(err, bolt.ErrDatabaseNotOpen) {
			slog.Debug("bbolt timeout, will retry", "error", err)
			return err
		}

		// Other errors are not retryable
		return backoff.Permanent(err)
	}

	err := backoff.Retry(operation, boWithRetries)

	// Unwrap permanent errors to return the original error
	var permanentErr *backoff.PermanentError
	if errors.As(err, &permanentErr) {
		return permanentErr.Unwrap()
	}

	return err
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
		func(any) { metrics.CleanupPanicsTotal.WithLabelValues("token").Inc() },
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
