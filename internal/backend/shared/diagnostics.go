package shared

import (
	"encoding/json"
	"fmt"
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
	*boltStore
}

// DiagnosticsStoreConfig configures the diagnostics store.
type DiagnosticsStoreConfig struct {
	DBPath          string            // Path to bbolt database file
	MaxAge          time.Duration     // Max age before entries are cleaned up (0 = no expiry)
	CleanupInterval time.Duration     // How often to run cleanup (defaults to MaxAge)
	OnCleanupPanic  util.PanicHandler // Optional: invoked on cleanup-loop panic.
}

// NewDiagnosticsStore opens or creates a bbolt database for diagnostics persistence.
// If MaxAge > 0, a background cleanup loop removes expired entries periodically
// and an initial cleanup runs immediately to clear stale entries from previous runs.
func NewDiagnosticsStore(cfg DiagnosticsStoreConfig) (*DiagnosticsStore, error) {
	base, err := openBoltStore(boltStoreConfig{
		DBPath:     cfg.DBPath,
		BucketName: diagnosticsBucketName,
		MaxAge:     cfg.MaxAge,
		Label:      "diagnostics",
	})
	if err != nil {
		return nil, err
	}

	s := &DiagnosticsStore{boltStore: base}

	if cfg.MaxAge > 0 {
		base.startCleanup("diagnostics", cfg.CleanupInterval, s.RemoveOlderThan, cfg.OnCleanupPanic)
	}

	return s, nil
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
	return removeOlderThan[DiagnosticEntry](s.db, diagnosticsBucketName, maxAge, func(e *DiagnosticEntry) time.Time {
		return e.CreatedAt
	})
}
