package docker

import (
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// recoveryDeadlines owns one visibility deadline per exact durable attempt.
// Persisted timestamps have no monotonic component. A future timestamp after a
// clock rollback gets one full window in this backend lifetime, rather than a
// fresh window on every observation. A process restart may conservatively grant
// another bounded window; no process-local clock is durable settlement evidence.
type recoveryDeadlines[K comparable] struct {
	mu         sync.Mutex
	generation uint64
	entries    map[K]recoveryDeadline
}

type recoveryDeadline struct {
	deadline   time.Time
	generation uint64
}

func (d *recoveryDeadlines[K]) observe(key K, admittedAt, now time.Time, timeout time.Duration) time.Time {
	candidate := provisionIntentRecoveryDeadline(admittedAt, now, timeout)
	d.mu.Lock()
	defer d.mu.Unlock()
	if entry, found := d.entries[key]; found {
		// Preserve the first observation's monotonic bound. An elapsed durable
		// timestamp or a shorter configured budget may only shorten it.
		if candidate.Before(entry.deadline) {
			entry.deadline = candidate
			d.entries[key] = entry
		}
		return entry.deadline
	}
	if d.entries == nil {
		d.entries = make(map[K]recoveryDeadline)
	}
	d.generation++
	d.entries[key] = recoveryDeadline{deadline: candidate, generation: d.generation}
	return candidate
}

// checkpoint precedes a complete journal inventory. Entries first observed
// while that inventory is in flight cannot be retired by its older snapshot.
func (d *recoveryDeadlines[K]) checkpoint() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.generation
}

func (d *recoveryDeadlines[K]) retainPending(checkpoint uint64, pending map[K]struct{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for key, entry := range d.entries {
		if _, exists := pending[key]; !exists && entry.generation <= checkpoint {
			delete(d.entries, key)
		}
	}
}

func (b *Backend) operationRecoveryDeadline(claim shared.OperationRecoveryState, now time.Time) time.Time {
	return b.operationRecoveryDeadlines.observe(
		keyForOperationIntent(claim), claim.CreatedAt(), now, b.intentRecoveryTimeout(),
	)
}

func (b *Backend) pendingOperationIntentsForRecovery() ([]shared.OperationIntentClaim, error) {
	checkpoint := b.operationRecoveryDeadlines.checkpoint()
	claims, err := b.operationSettlement.ListOperationIntents()
	if err != nil {
		return nil, err
	}
	pending := make(map[operationIntentKey]struct{}, len(claims))
	for _, claim := range claims {
		pending[keyForOperationIntent(claim)] = struct{}{}
	}
	b.operationRecoveryDeadlines.retainPending(checkpoint, pending)
	return claims, nil
}

type maintenanceIntentKey struct {
	leaseUUID     string
	maintenanceID shared.MaintenanceID
}

func keyForMaintenanceIntent(claim shared.MaintenanceIntentClaim) maintenanceIntentKey {
	return maintenanceIntentKey{leaseUUID: claim.LeaseUUID(), maintenanceID: claim.MaintenanceID()}
}

func (b *Backend) maintenanceRecoveryDeadline(claim shared.MaintenanceIntentClaim, now time.Time) time.Time {
	return b.maintenanceRecoveryDeadlines.observe(
		keyForMaintenanceIntent(claim), claim.CreatedAt(), now, b.intentRecoveryTimeout(),
	)
}

func (b *Backend) intentRecoveryTimeout() time.Duration {
	if b.cfg.ProvisionTimeout > 0 {
		return b.cfg.ProvisionTimeout
	}
	return 10 * time.Minute
}
