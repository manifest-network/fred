package provisioner

import (
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// These helpers preserve concise setup in older tests without exposing
// operationID-free mutation on the production tracker API.

// TrackInFlight installs or replaces an entry for legacy test setup.
func (t *DefaultInFlightTracker) TrackInFlight(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) {
	t.replaceForLegacy(leaseUUID, tenant, items, backendName, time.Now())
}

// TryTrackInFlight installs a provision entry without requiring a callback
// operationID, matching the historical test helper contract.
func (t *DefaultInFlightTracker) TryTrackInFlight(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) bool {
	_, ok := t.tryTrack(leaseUUID, tenant, items, backendName, KindProvision, false)
	return ok
}

// TryTrackRestoreInFlight installs the corresponding restore entry for tests.
func (t *DefaultInFlightTracker) TryTrackRestoreInFlight(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) bool {
	_, ok := t.tryTrack(leaseUUID, tenant, items, backendName, KindRestore, false)
	return ok
}

// UntrackInFlight removes an unclaimed test entry regardless of operationID.
func (t *DefaultInFlightTracker) UntrackInFlight(leaseUUID string) {
	t.removeForLegacy(leaseUUID)
}

// PopInFlight removes and returns an unclaimed test entry.
func (t *DefaultInFlightTracker) PopInFlight(leaseUUID string) (InFlightProvision, bool) {
	return t.popForLegacy(leaseUUID)
}

type legacyInFlightTestTracker interface {
	TrackInFlight(string, string, []backend.LeaseItem, string)
	TryTrackInFlight(string, string, []backend.LeaseItem, string) bool
	TryTrackRestoreInFlight(string, string, []backend.LeaseItem, string) bool
	UntrackInFlight(string)
	PopInFlight(string) (InFlightProvision, bool)
}

func (m *Manager) legacyTestTracker() legacyInFlightTestTracker {
	tracker, ok := m.tracker.(legacyInFlightTestTracker)
	if !ok {
		panic("test tracker does not implement legacy setup helpers")
	}
	return tracker
}

// TrackInFlight delegates legacy test setup to the manager's tracker.
func (m *Manager) TrackInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) {
	m.legacyTestTracker().TrackInFlight(leaseUUID, tenant, items, backendName)
}

// TryTrackInFlight delegates legacy atomic test setup to the tracker.
func (m *Manager) TryTrackInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) bool {
	return m.legacyTestTracker().TryTrackInFlight(leaseUUID, tenant, items, backendName)
}

// TryTrackRestoreInFlight delegates legacy restore test setup to the tracker.
func (m *Manager) TryTrackRestoreInFlight(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) bool {
	return m.legacyTestTracker().TryTrackRestoreInFlight(leaseUUID, tenant, items, backendName)
}

// UntrackInFlight delegates operationID-free test cleanup to the tracker.
func (m *Manager) UntrackInFlight(leaseUUID string) {
	m.legacyTestTracker().UntrackInFlight(leaseUUID)
}

// PopInFlight delegates legacy destructive test inspection to the tracker.
func (m *Manager) PopInFlight(leaseUUID string) (InFlightProvision, bool) {
	return m.legacyTestTracker().PopInFlight(leaseUUID)
}

// GetInFlight exposes tracker state only to package tests. Production lifecycle
// consumers coordinate through Manager.Operations and opaque capabilities.
func (m *Manager) GetInFlight(leaseUUID string) (InFlightProvision, bool) {
	return m.tracker.GetInFlight(leaseUUID)
}

// GetInFlightLeases exposes the tracker inventory only to package tests.
// Production shutdown uses the narrower WaitForDrain method.
func (m *Manager) GetInFlightLeases() []string {
	return m.tracker.GetInFlightLeases()
}

func (m *Manager) TryTrackInFlightWithOperationID(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) (operation.OperationID, bool) {
	return m.tracker.TryTrackInFlightWithOperationID(leaseUUID, tenant, items, backendName)
}

func (m *Manager) TryTrackInFlightWithOperationIDIfNotNewer(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
	maxRevision uint64,
) (operation.OperationID, bool, bool) {
	return m.tracker.TryTrackInFlightWithOperationIDIfNotNewer(
		leaseUUID, tenant, items, backendName, maxRevision,
	)
}

func (m *Manager) SnapshotMutationRevision() uint64 {
	return m.tracker.SnapshotMutationRevision()
}

func (m *Manager) TryClaimLeaseActionIfNotNewer(
	leaseUUID string,
	maxRevision uint64,
) (bool, bool) {
	return m.tracker.TryClaimLeaseActionIfNotNewer(leaseUUID, maxRevision)
}

func (m *Manager) TryClaimLeaseAction(leaseUUID string) bool {
	return m.tracker.TryClaimLeaseAction(leaseUUID)
}

func (m *Manager) ReleaseLeaseAction(leaseUUID string) bool {
	return m.tracker.ReleaseLeaseAction(leaseUUID)
}

func (m *Manager) TryTrackRestoreInFlightWithOperationID(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) (operation.OperationID, bool) {
	return m.tracker.TryTrackRestoreInFlightWithOperationID(leaseUUID, tenant, items, backendName)
}

func (m *Manager) UntrackInFlightIfOperationID(leaseUUID string, operationID operation.OperationID) bool {
	return m.tracker.UntrackInFlightIfOperationID(leaseUUID, operationID)
}

func (m *Manager) TryClaimInFlight(
	leaseUUID string,
	operationID operation.OperationID,
) (InFlightProvision, bool) {
	return m.tracker.TryClaimInFlight(leaseUUID, operationID)
}

func (m *Manager) TryClaimInFlightForDeprovision(
	leaseUUID string,
	operationID operation.OperationID,
) (InFlightProvision, bool) {
	return m.tracker.TryClaimInFlightForDeprovision(leaseUUID, operationID)
}

func (m *Manager) ReleaseInFlightClaim(leaseUUID string, operationID operation.OperationID) bool {
	return m.tracker.ReleaseInFlightClaim(leaseUUID, operationID)
}

func (m *Manager) FinishClaimedInFlight(leaseUUID string, operationID operation.OperationID) bool {
	return m.tracker.FinishClaimedInFlight(leaseUUID, operationID)
}

func (m *Manager) InFlightCountsByBackend() map[string]int {
	return m.tracker.InFlightCountsByBackend()
}

func (m *Manager) GetTimedOutProvisions(timeout time.Duration) []InFlightProvision {
	return m.tracker.GetTimedOutProvisions(timeout)
}

var _ ReconcilerTracker = (*Manager)(nil)
