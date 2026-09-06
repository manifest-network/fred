package provisioner

import (
	"context"
	"log/slog"
	"time"

	"github.com/manifest-network/fred/internal/provisioner/placement"
)

func (m *Manager) RestoreCoordinator(
	observe placement.RestoreStartObserver,
) (*placement.RestoreCoordinator, error) {
	return m.executionCoordinator.RestoreCoordinator(observe)
}

func (m *Manager) MaintenanceCoordinator(
	payloads placement.MaintenancePayloadPersister,
) (*placement.MaintenanceCoordinator, error) {
	return m.executionCoordinator.MaintenanceCoordinator(payloads)
}

func (m *Manager) ReconciliationCoordinator(
	observe placement.ProvisionStartObserver,
) (*placement.ReconciliationCoordinator, error) {
	return m.executionCoordinator.ReconciliationCoordinator(m.payloadStore, observe)
}

// IsInFlight reports whether the lifecycle registry owns leaseUUID.
func (m *Manager) IsInFlight(leaseUUID string) bool {
	return m.operationRuntime.Contains(leaseUUID)
}

// InFlightCount returns the number of process-local lifecycle operations.
func (m *Manager) InFlightCount() int {
	return m.operationRuntime.Count()
}

// WaitForDrain waits for process-local lifecycle operations to settle before
// shutdown and returns the number still present when the wait ends.
func (m *Manager) WaitForDrain(ctx context.Context, timeout time.Duration) int {
	count := m.operationRuntime.PendingWorkCount()
	if count == 0 {
		return 0
	}
	slog.Info("waiting for lifecycle work to drain", "count", count, "timeout", timeout)

	remaining := m.operationRuntime.WaitForDrain(ctx, timeout)
	if remaining == 0 {
		slog.Info("all lifecycle work drained successfully")
		return 0
	}
	if ctx != nil && ctx.Err() != nil {
		slog.Warn("drain interrupted by context cancellation",
			"remaining", remaining, "leases", m.operationRuntime.PendingLeaseUUIDs())
		return remaining
	}
	slog.Warn("drain timeout expired with provisions still in-flight",
		"remaining", remaining, "leases", m.operationRuntime.PendingLeaseUUIDs())
	return remaining
}

// BeginDrain irreversibly rejects new ordinary lifecycle work while preserving
// settlement of operations and durable callbacks that were already accepted.
func (m *Manager) BeginDrain() {
	m.operationRuntime.BeginDrain()
}
