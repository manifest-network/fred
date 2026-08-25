package provisioner

import (
	"context"
	"time"

	"github.com/manifest-network/fred/internal/backend"
)

// TryTrackInFlightWithGeneration delegates to the tracker.
func (m *Manager) TryTrackInFlightWithGeneration(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) (uint64, bool) {
	return m.tracker.TryTrackInFlightWithGeneration(leaseUUID, tenant, items, backendName)
}

func (m *Manager) TryTrackInFlightWithGenerationIfNotNewer(
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
	maxRevision uint64,
) (uint64, bool, bool) {
	return m.tracker.TryTrackInFlightWithGenerationIfNotNewer(
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

// TryTrackRestoreInFlightWithGeneration delegates to the tracker.
func (m *Manager) TryTrackRestoreInFlightWithGeneration(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) (uint64, bool) {
	return m.tracker.TryTrackRestoreInFlightWithGeneration(leaseUUID, tenant, items, backendName)
}

// UntrackInFlightIfGeneration delegates to the tracker.
func (m *Manager) UntrackInFlightIfGeneration(leaseUUID string, generation uint64) bool {
	return m.tracker.UntrackInFlightIfGeneration(leaseUUID, generation)
}

func (m *Manager) TryClaimInFlight(leaseUUID string, generation uint64) (InFlightProvision, bool) {
	return m.tracker.TryClaimInFlight(leaseUUID, generation)
}

func (m *Manager) TryClaimInFlightForDeprovision(leaseUUID string, generation uint64) (InFlightProvision, bool) {
	return m.tracker.TryClaimInFlightForDeprovision(leaseUUID, generation)
}

func (m *Manager) ReleaseInFlightClaim(leaseUUID string, generation uint64) bool {
	return m.tracker.ReleaseInFlightClaim(leaseUUID, generation)
}

func (m *Manager) FinishClaimedInFlight(leaseUUID string, generation uint64) bool {
	return m.tracker.FinishClaimedInFlight(leaseUUID, generation)
}

// IsInFlight delegates to the tracker.
func (m *Manager) IsInFlight(leaseUUID string) bool {
	return m.tracker.IsInFlight(leaseUUID)
}

// GetInFlight delegates to the tracker.
func (m *Manager) GetInFlight(leaseUUID string) (InFlightProvision, bool) {
	return m.tracker.GetInFlight(leaseUUID)
}

// InFlightCount delegates to the tracker.
func (m *Manager) InFlightCount() int {
	return m.tracker.InFlightCount()
}

// InFlightCountsByBackend delegates to the tracker.
func (m *Manager) InFlightCountsByBackend() map[string]int {
	return m.tracker.InFlightCountsByBackend()
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

// SetPlacementAttempting durably records restore intent before the backend call.
func (m *Manager) SetPlacementAttempting(leaseUUID, backendName string) (uint64, error) {
	return m.orchestrator.SetPlacementAttempting(leaseUUID, backendName)
}

// ConfirmPlacementIfRevision settles only the restore attempt created by the
// API request carrying revision.
func (m *Manager) ConfirmPlacementIfRevision(leaseUUID, backendName string, revision uint64) (bool, error) {
	return m.orchestrator.ConfirmPlacementIfRevision(leaseUUID, backendName, revision)
}

// ClearPlacementAttemptIfRevision clears only the refused restore attempt
// created by the API request carrying revision.
func (m *Manager) ClearPlacementAttemptIfRevision(leaseUUID, backendName string, revision uint64) (bool, error) {
	return m.orchestrator.ClearPlacementAttemptIfRevision(leaseUUID, backendName, revision)
}

// ConfirmPlacement promotes a matching restore attempt after positive backend
// ownership evidence.
func (m *Manager) ConfirmPlacement(leaseUUID, backendName string) error {
	return m.orchestrator.ConfirmPlacement(leaseUUID, backendName)
}

// ClearPlacementAttempt clears a matching restore attempt after a definitive
// synchronous refusal.
func (m *Manager) ClearPlacementAttempt(leaseUUID, backendName string) error {
	return m.orchestrator.ClearPlacementAttempt(leaseUUID, backendName)
}

// Compile-time check that *Manager can serve as the API's restore placement
// recorder (ENG-333) and restore in-flight tracker (ENG-358). Structural to
// avoid importing the api package.
var _ interface {
	SetPlacementAttempting(leaseUUID, backendName string) (uint64, error)
	ConfirmPlacementIfRevision(leaseUUID, backendName string, revision uint64) (bool, error)
	ClearPlacementAttemptIfRevision(leaseUUID, backendName string, revision uint64) (bool, error)
	TryTrackRestoreInFlightWithGeneration(leaseUUID, tenant string, items []backend.LeaseItem, backendName string) (uint64, bool)
	UntrackInFlightIfGeneration(leaseUUID string, generation uint64) bool
} = (*Manager)(nil)
