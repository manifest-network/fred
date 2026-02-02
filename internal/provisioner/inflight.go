package provisioner

import (
	"context"
	"time"

	"github.com/manifest-network/fred/internal/backend"
)

// TrackInFlight delegates to the tracker.
func (m *Manager) TrackInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) {
	m.tracker.TrackInFlight(leaseUUID, tenant, items, backendName)
}

// TryTrackInFlight delegates to the tracker.
func (m *Manager) TryTrackInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) bool {
	return m.tracker.TryTrackInFlight(leaseUUID, tenant, items, backendName)
}

// UntrackInFlight delegates to the tracker.
func (m *Manager) UntrackInFlight(leaseUUID string) {
	m.tracker.UntrackInFlight(leaseUUID)
}

// IsInFlight delegates to the tracker.
func (m *Manager) IsInFlight(leaseUUID string) bool {
	return m.tracker.IsInFlight(leaseUUID)
}

// PopInFlight delegates to the tracker.
func (m *Manager) PopInFlight(leaseUUID string) (InFlightProvision, bool) {
	return m.tracker.PopInFlight(leaseUUID)
}

// GetInFlight delegates to the tracker.
func (m *Manager) GetInFlight(leaseUUID string) (InFlightProvision, bool) {
	return m.tracker.GetInFlight(leaseUUID)
}

// InFlightCount delegates to the tracker.
func (m *Manager) InFlightCount() int {
	return m.tracker.InFlightCount()
}

// GetInFlightLeases delegates to the tracker.
func (m *Manager) GetInFlightLeases() []string {
	return m.tracker.GetInFlightLeases()
}

// WaitForDrain delegates to the tracker.
func (m *Manager) WaitForDrain(ctx context.Context, timeout time.Duration) int {
	return m.tracker.WaitForDrain(ctx, timeout)
}

// GetTimedOutProvisions delegates to the tracker.
func (m *Manager) GetTimedOutProvisions(timeout time.Duration) []InFlightProvision {
	return m.tracker.GetTimedOutProvisions(timeout)
}

// Tracker returns the internal tracker for testing purposes.
// This should only be used in tests.
func (m *Manager) Tracker() InFlightTracker {
	return m.tracker
}

// TimeoutChecker returns the internal timeout checker for testing purposes.
// This should only be used in tests.
func (m *Manager) TimeoutChecker() *TimeoutChecker {
	return m.timeoutChecker
}

// checkCallbackTimeouts delegates to the timeout checker for testing.
// This is called by tests that directly invoke timeout checking.
func (m *Manager) checkCallbackTimeouts(ctx context.Context) {
	m.timeoutChecker.CheckOnce(ctx)
}
