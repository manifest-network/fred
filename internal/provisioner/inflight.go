package provisioner

import (
	"context"
	"time"

	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// Operations exposes the manager's single typed operation registry to
// composition roots. Lifecycle consumers should depend on their own narrow
// interface over Registry rather than on Manager's legacy raw-generation
// forwarding methods.
func (m *Manager) Operations() *operation.Registry {
	return m.operations
}

// ReconcilerOperations exposes only the lifecycle authority consumed by the
// level-triggered reconciler. It is backed by the same registry returned by
// Operations, but its static type prevents the reconciler from reaching
// unrelated callback or timeout transitions.
func (m *Manager) ReconcilerOperations() ReconcilerOperations {
	return m.operations
}

// IsInFlight delegates to the tracker.
func (m *Manager) IsInFlight(leaseUUID string) bool {
	return m.tracker.IsInFlight(leaseUUID)
}

// InFlightCount delegates to the tracker.
func (m *Manager) InFlightCount() int {
	return m.tracker.InFlightCount()
}

// WaitForDrain delegates to the tracker.
func (m *Manager) WaitForDrain(ctx context.Context, timeout time.Duration) int {
	return m.tracker.WaitForDrain(ctx, timeout)
}
