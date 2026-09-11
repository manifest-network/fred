package shared

import (
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/util"
)

var diagnosticsBucketName = []byte("failure_diagnostics")

// DiagnosticEntry represents a persisted failure diagnostic record.
type DiagnosticEntry struct {
	LeaseUUID    string `json:"lease_uuid"`
	ProviderUUID string `json:"provider_uuid"`
	Tenant       string `json:"tenant"`
	Error        string `json:"error"`
	// Reason/Message: curated tenant-safe failure signal (ENG-508). Error stays
	// operator-only. Reason authored at source; Message == on-chain CallbackErr.
	Reason    backend.Reason    `json:"reason,omitempty"`
	Message   string            `json:"message,omitempty"`
	Logs      map[string]string `json:"logs,omitempty"`
	FailCount int               `json:"fail_count"`
	// RuntimeReleaseVersion identifies the observed active release when this is
	// a runtime failure snapshot. It is not authority; publication rechecks it
	// against the current journal under the lease transition gate.
	RuntimeReleaseVersion int `json:"runtime_release_version,omitempty"`
	// LifecycleGeneration is the historical, non-secret observation captured
	// from the callback pair that owned this failure. It keeps a singular
	// diagnostics read consistent after the live projection disappears, but is
	// never lifecycle/settlement authority: diagnostic-only rows remain excluded
	// from fleet inventory. Older rows omit it and remain readable as unknown.
	LifecycleGeneration *backend.LifecycleGenerationObservation `json:"lifecycle_generation,omitempty"`
	CreatedAt           time.Time                               `json:"created_at"`
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
	if err := base.update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{attemptDiagnosticsBucketName, diagnosticPublicationsBucketName} {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = base.Close()
		return nil, fmt.Errorf("initialize attempt diagnostics: %w", err)
	}

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

	return s.update(func(tx *bolt.Tx) error {
		// Attempt publication is owned by a terminal journal proof. A legacy
		// actor snapshot, including an empty post-cleanup log map, cannot replace
		// it. Current runtime observations use the bound publication service.
		if published := tx.Bucket(diagnosticPublicationsBucketName); published != nil && published.Get([]byte(entry.LeaseUUID)) != nil {
			return nil
		}
		b := tx.Bucket(diagnosticsBucketName)
		return b.Put([]byte(entry.LeaseUUID), data)
	})
}

// Get retrieves a diagnostic entry by lease UUID.
// Returns nil, nil when not found.
func (s *DiagnosticsStore) Get(leaseUUID string) (*DiagnosticEntry, error) {
	var entry *DiagnosticEntry

	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(diagnosticsBucketName)
		data := b.Get([]byte(leaseUUID))
		if data == nil {
			return nil
		}

		entry = &DiagnosticEntry{}
		if err := json.Unmarshal(data, entry); err != nil {
			return fmt.Errorf("failed to unmarshal diagnostic entry: %w", err)
		}
		if key := tx.Bucket(diagnosticPublicationsBucketName).Get([]byte(leaseUUID)); key != nil {
			record, err := decodeAttemptDiagnostic(tx.Bucket(attemptDiagnosticsBucketName).Get(key))
			if err != nil {
				return err
			}
			if record.Identity.LeaseUUID != leaseUUID || record.Entry.Tenant != entry.Tenant || record.Entry.ProviderUUID != entry.ProviderUUID {
				return fmt.Errorf("published diagnostic selector has inconsistent identity")
			}
			record.Entry.FailCount = entry.FailCount
			entry = &record.Entry
		}
		return nil
	})

	return entry, err
}

// Delete removes a diagnostic entry by lease UUID. It is a no-op if the
// entry does not exist.
func (s *DiagnosticsStore) Delete(leaseUUID string) error {
	return s.update(func(tx *bolt.Tx) error {
		if published := tx.Bucket(diagnosticPublicationsBucketName); published != nil {
			if previous := published.Get([]byte(leaseUUID)); previous != nil {
				if err := tx.Bucket(attemptDiagnosticsBucketName).Delete(previous); err != nil {
					return err
				}
			}
			if err := published.Delete([]byte(leaseUUID)); err != nil {
				return err
			}
		}
		b := tx.Bucket(diagnosticsBucketName)
		return b.Delete([]byte(leaseUUID))
	})
}

// RemoveOlderThan deletes diagnostic entries older than maxAge and returns
// the number of entries removed.
func (s *DiagnosticsStore) RemoveOlderThan(maxAge time.Duration) (int, error) {
	cutoff := time.Now().Add(-maxAge)
	removed := 0
	err := s.update(func(tx *bolt.Tx) error {
		visible := tx.Bucket(diagnosticsBucketName)
		publications := tx.Bucket(diagnosticPublicationsBucketName)
		cursor := visible.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var entry DiagnosticEntry
			if err := json.Unmarshal(value, &entry); err != nil {
				return fmt.Errorf("decode expiring diagnostic: %w", err)
			}
			if entry.CreatedAt.Before(cutoff) {
				if err := publications.Delete(key); err != nil {
					return err
				}
				if err := cursor.Delete(); err != nil {
					return err
				}
				removed++
			}
		}
		attempts := tx.Bucket(attemptDiagnosticsBucketName)
		cursor = attempts.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			record, err := decodeAttemptDiagnostic(value)
			if err != nil {
				return err
			}
			if record.Entry.CreatedAt.Before(cutoff) {
				if err := cursor.Delete(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return removed, err
}
