package placement

import (
	"container/heap"
	"fmt"
	"log/slog"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/metrics"
)

const (
	maxPendingMaintenanceCommands       = 1024
	maxPendingMaintenanceBytes          = int64(64 << 20)
	maxTenantPendingMaintenanceCommands = 16
	maxTenantPendingMaintenanceBytes    = int64(8 << 20)
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

// maintenancePendingAccounting is a projection of committed journal rows, built
// once on open and changed only by the journal writer's commit callback. It owns
// both admission shares and the phase observations; request callers supply
// neither a tenant accounting key nor an independent accounting mutation.
type maintenancePendingAccounting struct {
	total   maintenanceUsage
	tenants map[string]maintenanceUsage
	leases  map[string]*maintenancePendingEntry
	phases  map[maintenanceJournalPhase]*maintenancePhaseAccounting
}

type maintenanceUsage struct {
	count int
	bytes int64
}

type maintenancePendingEntry struct {
	lease     string
	tenant    string
	phase     maintenanceJournalPhase
	bytes     int64
	createdAt time.Time
	index     int
}

type maintenancePhaseAccounting struct {
	bytes  int64
	oldest maintenanceAgeHeap
}

type maintenanceAgeHeap []*maintenancePendingEntry

func (h maintenanceAgeHeap) Len() int           { return len(h) }
func (h maintenanceAgeHeap) Less(i, j int) bool { return h[i].createdAt.Before(h[j].createdAt) }
func (h maintenanceAgeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *maintenanceAgeHeap) Push(value any) {
	entry := value.(*maintenancePendingEntry)
	entry.index = len(*h)
	*h = append(*h, entry)
}
func (h *maintenanceAgeHeap) Pop() any {
	last := len(*h) - 1
	entry := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return entry
}

func loadMaintenanceAccounting(tx *bolt.Tx) (*maintenancePendingAccounting, error) {
	accounting := &maintenancePendingAccounting{
		tenants: make(map[string]maintenanceUsage),
		leases:  make(map[string]*maintenancePendingEntry),
		phases:  make(map[maintenanceJournalPhase]*maintenancePhaseAccounting),
	}
	pending, records, err := maintenanceCommandBuckets(tx)
	if err != nil {
		return nil, err
	}
	err = pending.ForEach(func(lease, value []byte) error {
		id, err := maintenanceid.Parse(string(value))
		if err != nil || value == nil {
			return fmt.Errorf("%w: invalid pending maintenance head", ErrMaintenanceJournalCorrupt)
		}
		encoded := records.Get(maintenanceReceiptKey(string(lease), id))
		command, outcome, createdAt, _, _, err := decodeMaintenanceCommand(encoded)
		if err != nil || outcome != MaintenanceOutcomePending || command.leaseUUID != string(lease) || command.id != id {
			return fmt.Errorf("%w: invalid pending maintenance receipt", ErrMaintenanceJournalCorrupt)
		}
		accounting.replace(command, outcome, createdAt, len(encoded))
		return nil
	})
	return accounting, err
}

// Fresh work reserves framing for later phase changes. Exact replay and
// settlement retain their authority even when legacy work exceeds a limit.
func (journal *maintenanceJournalTransaction) admit(command MaintenanceCommand, newBytes int) error {
	refuse := func(reason string) error {
		metrics.MaintenanceAdmissionRefusalsTotal.WithLabelValues(reason).Inc()
		return fmt.Errorf("%w: %s", ErrMaintenancePendingFull, reason)
	}
	charge := int64(newBytes) + pendingMaintenanceTransitionBytes
	if journal.total.count >= maxPendingMaintenanceCommands {
		return refuse("count")
	}
	if charge > maxPendingMaintenanceBytes-journal.total.bytes {
		return refuse("bytes")
	}
	tenant := journal.tenantUsage(command.tenant)
	if tenant.count >= maxTenantPendingMaintenanceCommands {
		return refuse("tenant_count")
	}
	if charge > maxTenantPendingMaintenanceBytes-tenant.bytes {
		return refuse("tenant_bytes")
	}
	return nil
}

func (accounting *maintenancePendingAccounting) replace(command MaintenanceCommand, outcome MaintenanceCommandOutcome, createdAt time.Time, encodedBytes int) {
	if previous := accounting.leases[command.leaseUUID]; previous != nil {
		phase := accounting.phases[previous.phase]
		heap.Remove(&phase.oldest, previous.index)
		phase.bytes -= previous.bytes
		accounting.adjust(previous, -1)
		delete(accounting.leases, command.leaseUUID)
	}
	if outcome != MaintenanceOutcomePending {
		return
	}
	entry := &maintenancePendingEntry{
		lease: command.leaseUUID, tenant: command.tenant, phase: command.phase,
		bytes: int64(encodedBytes), createdAt: createdAt,
	}
	phase := accounting.phases[entry.phase]
	if phase == nil {
		phase = &maintenancePhaseAccounting{}
		accounting.phases[entry.phase] = phase
	}
	heap.Push(&phase.oldest, entry)
	phase.bytes += entry.bytes
	accounting.leases[entry.lease] = entry
	accounting.adjust(entry, 1)
}

func (accounting *maintenancePendingAccounting) adjust(entry *maintenancePendingEntry, direction int) {
	charge := int64(direction) * (entry.bytes + pendingMaintenanceTransitionBytes)
	accounting.total.count += direction
	accounting.total.bytes += charge
	tenant := accounting.tenants[entry.tenant]
	tenant.count += direction
	tenant.bytes += charge
	if tenant.count == 0 {
		delete(accounting.tenants, entry.tenant)
	} else {
		accounting.tenants[entry.tenant] = tenant
	}
}

// maintenanceJournalTransaction owns all pending writes in one durable
// transaction, including their admission charges. Its working totals include
// earlier writes in the transaction; only successful commit publishes them.
type maintenanceJournalTransaction struct {
	tx      *bolt.Tx
	store   *Store
	total   maintenanceUsage
	tenants map[string]maintenanceUsage
}

func (s *Store) updateMaintenanceAuthority(mutate func(*maintenanceJournalTransaction) error) error {
	return s.updateRuntimeAuthority(func(tx *bolt.Tx) error {
		return mutate(&maintenanceJournalTransaction{
			tx: tx, store: s, total: s.maintenanceAccounting.total,
			tenants: make(map[string]maintenanceUsage),
		})
	})
}

func (journal *maintenanceJournalTransaction) tenantUsage(tenant string) maintenanceUsage {
	if usage, changed := journal.tenants[tenant]; changed {
		return usage
	}
	return journal.store.maintenanceAccounting.tenants[tenant]
}

func (journal *maintenanceJournalTransaction) adjust(command MaintenanceCommand, encodedBytes, direction int) {
	charge := int64(direction) * (int64(encodedBytes) + pendingMaintenanceTransitionBytes)
	journal.total.count += direction
	journal.total.bytes += charge
	tenant := journal.tenantUsage(command.tenant)
	tenant.count += direction
	tenant.bytes += charge
	journal.tenants[command.tenant] = tenant
}

// write keeps the durable receipt, pending index and accounting inseparable.
// The encoded validated command supplies the tenant; no caller can select a
// different principal's share. The transaction owner holds Store.mu.
func (journal *maintenanceJournalTransaction) write(encoded []byte) error {
	command, outcome, createdAt, _, _, err := decodeMaintenanceCommand(encoded)
	if err != nil {
		return err
	}
	pending, records, err := maintenanceCommandBuckets(journal.tx)
	if err != nil {
		return err
	}
	key := maintenanceReceiptKey(command.leaseUUID, command.id)
	previous := records.Get(key)
	if outcome == MaintenanceOutcomePending && previous == nil {
		if err := journal.admit(command, len(encoded)); err != nil {
			return err
		}
	}
	if previous != nil {
		previousCommand, previousOutcome, _, _, _, err := decodeMaintenanceCommand(previous)
		if err != nil {
			return err
		}
		if previousOutcome == MaintenanceOutcomePending {
			journal.adjust(previousCommand, len(previous), -1)
		}
	}
	if outcome == MaintenanceOutcomePending {
		journal.adjust(command, len(encoded), 1)
	}
	if err := records.Put(key, encoded); err != nil {
		return err
	}
	if outcome == MaintenanceOutcomePending {
		err = pending.Put([]byte(command.leaseUUID), []byte(command.id.String()))
	} else {
		err = pending.Delete([]byte(command.leaseUUID))
	}
	if err != nil {
		return err
	}
	journal.tx.OnCommit(func() { journal.store.maintenanceAccounting.replace(command, outcome, createdAt, len(encoded)) })
	return nil
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
	if err := s.reattestRuntimeAuthority(); err != nil {
		return nil, err
	}
	pressure := make(maintenancePressure, len(s.maintenanceAccounting.phases))
	for phase, accounting := range s.maintenanceAccounting.phases {
		current := maintenancePhasePressure{count: len(accounting.oldest), bytes: accounting.bytes}
		if len(accounting.oldest) > 0 {
			current.oldest = accounting.oldest[0].createdAt
		}
		pressure[phase] = current
	}
	return pressure, nil
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
