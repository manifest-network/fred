package docker

import (
	"crypto/sha256"
	"fmt"
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
	deadline, _ := d.observeFirst(key, admittedAt, now, timeout)
	return deadline
}

// observeFirst reports whether this observation opened the runtime window so
// clock-rollback diagnostics can be emitted once without holding the cache lock.
func (d *recoveryDeadlines[K]) observeFirst(key K, admittedAt, now time.Time, timeout time.Duration) (time.Time, bool) {
	return d.observeDeadline(key, provisionIntentRecoveryDeadline(admittedAt, now, timeout), true)
}

func (d *recoveryDeadlines[K]) observeDeadline(key K, candidate time.Time, allowShortening bool) (time.Time, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if entry, found := d.entries[key]; found {
		// Preserve the first observation's monotonic bound. An elapsed durable
		// timestamp or a shorter configured budget may only shorten it.
		if allowShortening && candidate.Before(entry.deadline) {
			entry.deadline = candidate
			d.entries[key] = entry
		}
		return entry.deadline, false
	}
	if d.entries == nil {
		d.entries = make(map[K]recoveryDeadline)
	}
	d.generation++
	d.entries[key] = recoveryDeadline{deadline: candidate, generation: d.generation}
	return candidate, true
}

// checkpoint precedes a complete journal inventory. Entries first observed
// while that inventory is in flight cannot be retired by its older snapshot.
func (d *recoveryDeadlines[K]) checkpoint() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.generation
}

func (d *recoveryDeadlines[K]) retainPending(checkpoint uint64, pending map[K]struct{}) {
	d.retainMatching(checkpoint, func(key K) bool {
		_, exists := pending[key]
		return exists
	})
}

func (d *recoveryDeadlines[K]) retainMatching(checkpoint uint64, keep func(K) bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for key, entry := range d.entries {
		if !keep(key) && entry.generation <= checkpoint {
			delete(d.entries, key)
		}
	}
}

func (b *Backend) operationRecoveryDeadline(claim shared.OperationRecoveryState, now time.Time) time.Time {
	deadline, first := b.operationRecoveryDeadlines.observeFirst(
		keyForOperationIntent(claim), claim.CreatedAt(), now, b.intentRecoveryTimeout(),
	)
	if first && claim.CreatedAt().After(now) {
		b.logger.Warn("future operation admission opened a bounded recovery window",
			"lease_uuid", claim.LeaseUUID(), "operation_fingerprint", claim.OperationID().Fingerprint(),
			"admitted_at", claim.CreatedAt(), "deadline", deadline)
	}
	return deadline
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

// A container's startup age belongs to the exact pending maintenance attempt.
// A replacement container, or a successor attempt using the same source,
// cannot inherit another container's completed observation window.
type maintenanceReadinessKey struct {
	intent      maintenanceIntentKey
	containerID string
}

func (b *Backend) maintenanceContainerAgeReached(
	intent shared.MaintenanceIntentClaim, container ContainerInfo,
) bool {
	now := time.Now()
	createdAt := container.CreatedAt
	if createdAt.IsZero() {
		// Missing age is uncertainty, not evidence that the startup window has
		// elapsed. Observe a full window, just as for a future wall timestamp.
		createdAt = now
	}
	minimumAge := b.cfg.StartupVerifyDuration
	if minimumAge <= 0 {
		minimumAge = 5 * time.Second
	}
	// This is a minimum stabilization age, unlike the maximum visibility
	// horizon. Once observed, even a forward wall-clock correction must not
	// shorten the first monotonic wait for this exact container and attempt.
	deadline, _ := b.maintenanceReadinessDeadlines.observeDeadline(
		maintenanceReadinessKey{intent: keyForMaintenanceIntent(intent), containerID: container.ContainerID},
		provisionIntentRecoveryDeadline(createdAt, now, minimumAge), false,
	)
	return !now.Before(deadline)
}

func keyForMaintenanceIntent(claim shared.MaintenanceIntentClaim) maintenanceIntentKey {
	return maintenanceIntentKey{leaseUUID: claim.LeaseUUID(), maintenanceID: claim.MaintenanceID()}
}

func (b *Backend) maintenanceRecoveryDeadline(claim shared.MaintenanceIntentClaim, now time.Time) time.Time {
	deadline, first := b.maintenanceRecoveryDeadlines.observeFirst(
		keyForMaintenanceIntent(claim), claim.CreatedAt(), now, b.intentRecoveryTimeout(),
	)
	if first && claim.CreatedAt().After(now) {
		fingerprint := sha256.Sum256([]byte(claim.MaintenanceID().String()))
		b.logger.Warn("future maintenance admission opened a bounded recovery window",
			"lease_uuid", claim.LeaseUUID(), "maintenance_fingerprint", fmt.Sprintf("maintenance_%x", fingerprint[:12]),
			"admitted_at", claim.CreatedAt(), "deadline", deadline)
	}
	return deadline
}

func (b *Backend) intentRecoveryTimeout() time.Duration {
	if b.cfg.ProvisionTimeout > 0 {
		return b.cfg.ProvisionTimeout
	}
	return 10 * time.Minute
}
