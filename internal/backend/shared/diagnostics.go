package shared

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
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
	retentionMu sync.Mutex
	retained    map[string]*retainedDiagnostic
	actions     sync.WaitGroup
	closing     bool
	// awaitsSettlement reports whether the journal still holds an attempt's
	// operation unsettled. NewFailureDiagnostics binds it. Until then expiry
	// keeps every published operation failure (see RemoveOlderThan).
	awaitsSettlement func(diagnosticAttemptIdentity) (bool, error)
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

	s := &DiagnosticsStore{boltStore: base, retained: make(map[string]*retainedDiagnostic)}
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
				if err := s.deleteAttemptTx(tx, previous); err != nil {
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
//
// A published operation failure is kept past maxAge while its operation is
// unsettled. The publication is the durable record that the failure was
// decided, and recovery settles the operation from it (ENG-939); expiring it
// first would return the lease to provisioning. It expires only on a positive
// answer from the journal that the operation no longer awaits settlement. With
// no journal bound yet, as during the cleanup that runs when the store opens,
// every published operation failure is kept.
func (s *DiagnosticsStore) RemoveOlderThan(maxAge time.Duration) (int, error) {
	cutoff := time.Now().Add(-maxAge)
	settled, err := s.settledPublishedOperations(cutoff)
	if err != nil {
		return 0, err
	}
	removed := 0
	err = s.update(func(tx *bolt.Tx) error {
		visible := tx.Bucket(diagnosticsBucketName)
		publications := tx.Bucket(diagnosticPublicationsBucketName)
		attempts := tx.Bucket(attemptDiagnosticsBucketName)
		cursor := visible.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			var entry DiagnosticEntry
			// Diagnostics are observational: preserve a malformed row for
			// inspection, but do not let it prevent unrelated expiry forever.
			if err := json.Unmarshal(value, &entry); err != nil {
				continue
			}
			if entry.CreatedAt.Before(cutoff) {
				if selected := publications.Get(key); selected != nil {
					record, err := decodeAttemptDiagnostic(attempts.Get(selected))
					if err == nil && publishedOperationIsHeld(record.Identity, selected, settled) {
						continue
					}
				}
				if err := publications.Delete(key); err != nil {
					return err
				}
				if err := cursor.Delete(); err != nil {
					return err
				}
				removed++
			}
		}
		cursor = attempts.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			record, err := decodeAttemptDiagnostic(value)
			if err != nil {
				continue
			}
			if record.Entry.CreatedAt.Before(cutoff) {
				if bytes.Equal(publications.Get([]byte(record.Identity.LeaseUUID)), key) &&
					publishedOperationIsHeld(record.Identity, key, settled) {
					continue
				}
				if err := s.deleteAttemptTx(tx, key); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return removed, err
}

// publishedOperationIsHeld reports whether a published attempt is an operation
// failure that the journal has not positively reported settled.
func publishedOperationIsHeld(identity diagnosticAttemptIdentity, key []byte, settled map[string]struct{}) bool {
	if identity.Kind != "operation" {
		return false
	}
	_, isSettled := settled[string(key)]
	return !isSettled
}

// settledPublishedOperations returns the keys of the expired published
// operation failures whose operation the journal reports no longer awaits
// settlement. The journal is read outside any diagnostics transaction. An
// operation only ever moves from unsettled to settled, so an answer read here
// stays true when expiry applies it.
func (s *DiagnosticsStore) settledPublishedOperations(cutoff time.Time) (map[string]struct{}, error) {
	s.retentionMu.Lock()
	awaitsSettlement := s.awaitsSettlement
	s.retentionMu.Unlock()
	settled := make(map[string]struct{})
	if awaitsSettlement == nil {
		return settled, nil
	}
	var expired []diagnosticAttemptIdentity
	err := s.view(func(tx *bolt.Tx) error {
		attempts := tx.Bucket(attemptDiagnosticsBucketName)
		cursor := tx.Bucket(diagnosticPublicationsBucketName).Cursor()
		for lease, selected := cursor.First(); lease != nil; lease, selected = cursor.Next() {
			// A selector without a readable attempt holds nothing.
			record, err := decodeAttemptDiagnostic(attempts.Get(selected))
			if err != nil {
				continue
			}
			if record.Identity.Kind == "operation" && string(selected) == record.Identity.key() &&
				record.Entry.CreatedAt.Before(cutoff) {
				expired = append(expired, record.Identity)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, identity := range expired {
		awaits, err := awaitsSettlement(identity)
		if err != nil {
			return nil, fmt.Errorf("read settlement of the published failure of lease %q: %w", identity.LeaseUUID, err)
		}
		if !awaits {
			settled[identity.key()] = struct{}{}
		}
	}
	return settled, nil
}
