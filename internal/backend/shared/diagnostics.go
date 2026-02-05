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

var diagnosticsBucketName = []byte("failure_diagnostics")

// DiagnosticEntry represents a persisted failure diagnostic record.
type DiagnosticEntry struct {
	LeaseUUID    string            `json:"lease_uuid"`
	ProviderUUID string            `json:"provider_uuid"`
	Tenant       string            `json:"tenant"`
	Error        string            `json:"error"`
	Logs         map[string]string `json:"logs,omitempty"`
	FailCount    int               `json:"fail_count"`
	CreatedAt    time.Time         `json:"created_at"`
}

// DiagnosticsStore persists failure diagnostics in bbolt so they survive
// container removal and backend restarts.
type DiagnosticsStore struct {
	db     *bolt.DB
	maxAge time.Duration

	cancel    context.CancelFunc
	wg        *sync.WaitGroup
	closeOnce *sync.Once
	closeErr  error // captured by first Close() call
}

// DiagnosticsStoreConfig configures the diagnostics store.
type DiagnosticsStoreConfig struct {
	DBPath          string        // Path to bbolt database file
	MaxAge          time.Duration // Max age before entries are cleaned up (0 = no expiry)
	CleanupInterval time.Duration // How often to run cleanup (defaults to MaxAge)
}

// NewDiagnosticsStore opens or creates a bbolt database for diagnostics persistence.
// If MaxAge > 0, a background cleanup loop removes expired entries periodically
// and an initial cleanup runs immediately to clear stale entries from previous runs.
func NewDiagnosticsStore(cfg DiagnosticsStoreConfig) (*DiagnosticsStore, error) {
	if cfg.DBPath == "" {
		return nil, fmt.Errorf("diagnostics db path is required")
	}

	db, err := bolt.Open(cfg.DBPath, 0600, &bolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open diagnostics db: %w", err)
	}

	// Create bucket if it doesn't exist
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(diagnosticsBucketName)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create diagnostics bucket: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &DiagnosticsStore{
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
			slog.Warn("initial diagnostics cleanup failed", "error", cleanupErr)
		} else if removed > 0 {
			slog.Info("removed expired diagnostics on startup", "count", removed, "max_age", cfg.MaxAge)
		}

		interval := cfg.CleanupInterval
		if interval <= 0 {
			interval = cfg.MaxAge
		}
		s.wg.Go(func() {
			util.StartCleanupLoop(ctx, interval, s.cleanup, "diagnostics")
		})
	}

	return s, nil
}

// cleanup removes expired diagnostic entries. Used by the background cleanup loop.
func (s *DiagnosticsStore) cleanup() error {
	removed, err := s.RemoveOlderThan(s.maxAge)
	if err != nil {
		return err
	}
	if removed > 0 {
		slog.Debug("cleaned up expired diagnostics", "count", removed)
	}
	return nil
}

// Healthy checks if the bbolt database is accessible and the diagnostics bucket exists.
func (s *DiagnosticsStore) Healthy() error {
	return s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(diagnosticsBucketName) == nil {
			return errors.New("diagnostics bucket missing")
		}
		return nil
	})
}

// Store persists a diagnostic entry, upserting by LeaseUUID.
func (s *DiagnosticsStore) Store(entry DiagnosticEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal diagnostic entry: %w", err)
	}

	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(diagnosticsBucketName)
		return b.Put([]byte(entry.LeaseUUID), data)
	})
}

// Get retrieves a diagnostic entry by lease UUID.
// Returns nil, nil when not found.
func (s *DiagnosticsStore) Get(leaseUUID string) (*DiagnosticEntry, error) {
	var entry *DiagnosticEntry

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(diagnosticsBucketName)
		data := b.Get([]byte(leaseUUID))
		if data == nil {
			return nil
		}

		entry = &DiagnosticEntry{}
		if err := json.Unmarshal(data, entry); err != nil {
			return fmt.Errorf("failed to unmarshal diagnostic entry: %w", err)
		}
		return nil
	})

	return entry, err
}

// Delete removes a diagnostic entry by lease UUID. It is a no-op if the
// entry does not exist.
func (s *DiagnosticsStore) Delete(leaseUUID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(diagnosticsBucketName)
		return b.Delete([]byte(leaseUUID))
	})
}

// RemoveOlderThan deletes diagnostic entries older than maxAge and returns
// the number of entries removed.
func (s *DiagnosticsStore) RemoveOlderThan(maxAge time.Duration) (int, error) {
	cutoff := time.Now().Add(-maxAge)
	removed := 0

	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(diagnosticsBucketName)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var entry DiagnosticEntry
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

// Close shuts down the diagnostics store gracefully.
// Close is idempotent: the first call closes the database and captures
// any error; subsequent calls return the same error.
func (s *DiagnosticsStore) Close() error {
	s.closeOnce.Do(func() {
		s.cancel()
		s.wg.Wait()
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}
