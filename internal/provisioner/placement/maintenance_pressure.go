package placement

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/metrics"
)

const (
	maxPendingMaintenanceCommands = 1024
	maxPendingMaintenanceBytes    = int64(64 << 20)
	// Pending phase changes add only bounded fixed metadata. Reserve the same
	// framing allowance used by the individual pending-record budget.
	pendingMaintenanceTransitionBytes = int64(512)
)

// CompletionChanged is a coalesced scheduling hint, never completion evidence.
// Receivers must reload typed work from the durable journal before mutation.
func (application *MaintenanceApplication) CompletionChanged() <-chan struct{} {
	if !application.Valid() {
		return nil
	}
	return application.coordinator.coordinator.store.maintenanceChanged
}

func (s *Store) wakeMaintenance() {
	select {
	case s.maintenanceChanged <- struct{}{}:
	default:
	}
}

// A completion hint can be consumed while a live dispatch owns its mutex.
// Capture before acquiring that mutex, then requeue only if a durable callback
// changed during ownership. A stuck confirmed payload does not self-schedule
// endless retries merely because its persistence continues failing.
func (s *Store) completionCheckpoint() func() {
	before := s.maintenanceCompletionVersion.Load()
	return func() {
		if s.maintenanceCompletionVersion.Load() != before {
			s.wakeMaintenance()
		}
	}
}

// visitPendingMaintenance reads one encoded record at a time. Aggregate budget
// admission needs sizes only; it never allocates a copy of candidate payloads.
func visitPendingMaintenance(pending, records *bolt.Bucket, visit func([]byte) error) error {
	return pending.ForEach(func(lease, value []byte) error {
		id, err := maintenanceid.Parse(string(value))
		if err != nil || value == nil {
			return fmt.Errorf("%w: invalid pending maintenance head", ErrMaintenanceJournalCorrupt)
		}
		encoded := records.Get(maintenanceReceiptKey(string(lease), id))
		if len(encoded) == 0 {
			return fmt.Errorf("%w: pending maintenance has no receipt", ErrMaintenanceJournalCorrupt)
		}
		return visit(encoded)
	})
}

// This runs only for fresh commands, inside their WAL transaction. Exact
// pending/terminal replays and legacy phase transitions retain their authority
// even when older durable work exceeds today's aggregate admission budget.
func admitPendingMaintenance(pending, records *bolt.Bucket, newBytes int) error {
	count := 1
	used := int64(newBytes) + pendingMaintenanceTransitionBytes
	refuse := func(reason string) error {
		metrics.MaintenanceAdmissionRefusalsTotal.WithLabelValues(reason).Inc()
		return fmt.Errorf("%w: %s", ErrMaintenancePendingFull, reason)
	}
	return visitPendingMaintenance(pending, records, func(encoded []byte) error {
		count++
		if count > maxPendingMaintenanceCommands {
			return refuse("count")
		}
		charge := int64(len(encoded)) + pendingMaintenanceTransitionBytes
		if charge > maxPendingMaintenanceBytes-used {
			return refuse("bytes")
		}
		used += charge
		return nil
	})
}

type maintenancePhasePressure struct {
	count  int
	bytes  int64
	oldest time.Time
}
type maintenancePressure map[maintenanceJournalPhase]maintenancePhasePressure

func (s *Store) maintenancePressure() (maintenancePressure, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pressure := make(maintenancePressure)
	err := s.viewRuntimeAuthority(func(tx *bolt.Tx) error {
		pending, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		return visitPendingMaintenance(pending, records, func(encoded []byte) error {
			// Observability needs no decoded payload. Skipping that field avoids
			// retaining another full pending-journal copy on each recovery pass.
			var record struct {
				Phase     string    `json:"phase"`
				Outcome   string    `json:"outcome"`
				CreatedAt time.Time `json:"created_at"`
			}
			if err := json.Unmarshal(encoded, &record); err != nil {
				return err
			}
			phase, err := decodeMaintenancePhase(record.Phase, MaintenanceOutcomePending)
			if err != nil || record.Outcome != MaintenanceOutcomePending.String() || record.CreatedAt.IsZero() {
				return ErrMaintenanceJournalCorrupt
			}
			current := pressure[phase]
			current.count++
			current.bytes += int64(len(encoded))
			if current.oldest.IsZero() || record.CreatedAt.Before(current.oldest) {
				current.oldest = record.CreatedAt
			}
			pressure[phase] = current
			return nil
		})
	})
	return pressure, err
}

func (application *MaintenanceApplication) observePending() {
	store := application.coordinator.coordinator.store
	pressure, err := store.maintenancePressure()
	if err != nil {
		slog.Warn("maintenance pending observation unavailable", "error", err)
		return
	}
	now := store.now()
	for _, phase := range []maintenanceJournalPhase{maintenanceDeliveryOutstanding, maintenanceCompletionOutstanding, maintenancePayloadConfirmed} {
		current := pressure[phase]
		age := float64(0)
		if !current.oldest.IsZero() {
			age = max(0, now.Sub(current.oldest).Seconds())
		}
		metrics.MaintenancePending.WithLabelValues(phase.String()).Set(float64(current.count))
		metrics.MaintenancePendingBytes.WithLabelValues(phase.String()).Set(float64(current.bytes))
		metrics.MaintenancePendingOldestAge.WithLabelValues(phase.String()).Set(age)
	}
}
