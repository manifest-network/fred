package provisioner

import (
	"context"
	"log/slog"
	"time"
)

// RestoreOperations exposes only the lifecycle authority consumed by the
// restore application service. The concrete registry and its unrelated
// callback, timeout, and reconciliation transitions remain encapsulated.
func (m *Manager) RestoreOperations() RestoreOperations {
	return m.operations
}

// MaintenanceClaims exposes only the per-lease exclusion used by restart and
// update handlers. A maintenance handler cannot observe or mutate operations.
func (m *Manager) MaintenanceClaims() MaintenanceClaims {
	return m.operations
}

// ReconcilerOperations exposes only the lifecycle authority consumed by the
// level-triggered reconciler. Its static type prevents the reconciler from
// reaching unrelated callback or timeout transitions.
func (m *Manager) ReconcilerOperations() ReconcilerOperations {
	return m.operations
}

// IsInFlight reports whether the lifecycle registry owns leaseUUID.
func (m *Manager) IsInFlight(leaseUUID string) bool {
	return m.operations.Contains(leaseUUID)
}

// InFlightCount returns the number of process-local lifecycle operations.
func (m *Manager) InFlightCount() int {
	return m.operations.Count()
}

// WaitForDrain waits for process-local lifecycle operations to settle before
// shutdown and returns the number still present when the wait ends.
func (m *Manager) WaitForDrain(ctx context.Context, timeout time.Duration) int {
	count := m.operations.PendingWorkCount()
	if count == 0 {
		return 0
	}
	slog.Info("waiting for lifecycle work to drain", "count", count, "timeout", timeout)

	remaining := m.operations.WaitForDrain(ctx, timeout)
	if remaining == 0 {
		slog.Info("all lifecycle work drained successfully")
		return 0
	}
	if ctx != nil && ctx.Err() != nil {
		slog.Warn("drain interrupted by context cancellation",
			"remaining", remaining, "leases", m.operations.PendingLeaseUUIDs())
		return remaining
	}
	slog.Warn("drain timeout expired with provisions still in-flight",
		"remaining", remaining, "leases", m.operations.PendingLeaseUUIDs())
	return remaining
}

// BeginDrain irreversibly rejects new ordinary lifecycle work while preserving
// settlement of operations and durable callbacks that were already accepted.
func (m *Manager) BeginDrain() {
	m.operations.BeginDrain()
}
