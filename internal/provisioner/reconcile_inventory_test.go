package provisioner

import (
	"maps"
	"slices"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

type projectionCapture struct {
	ReconcilerPlacement
	projection placement.InventoryProjection
}

func (capture *projectionCapture) ProjectInventory(
	fence placement.InventoryFence,
	projection placement.InventoryProjection,
) (placement.ProjectionResult, error) {
	capture.projection = placement.InventoryProjection{
		Complete:                 projection.Complete,
		BackendStorageIdentities: maps.Clone(projection.BackendStorageIdentities),
		EmptyBackends:            slices.Clone(projection.EmptyBackends),
		Placements:               maps.Clone(projection.Placements),
		Lifecycles:               maps.Clone(projection.Lifecycles),
		Conflicts:                maps.Clone(projection.Conflicts),
		UntrustedPositives:       maps.Clone(projection.UntrustedPositives),
	}
	return capture.ReconcilerPlacement.ProjectInventory(fence, projection)
}

func TestReconcileInventoryRejectsCrossEndpointDuplicateBackend(t *testing.T) {
	const (
		duplicateLease = "018f47a2-8b1c-7def-8123-456789abcdef"
		goodProvision  = "018f47a2-8b1c-7def-8123-456789abcdee"
		goodRetention  = "018f47a2-8b1c-7def-8123-456789abcded"
	)
	inventory := reconcileInventory{
		fleet: fleetSnapshot{
			provisions: map[string]backend.ProvisionInfo{
				duplicateLease: {LeaseUUID: duplicateLease, BackendName: "backend-a"},
				goodProvision:  {LeaseUUID: goodProvision, BackendName: "backend-b"},
			},
			reportedByBackend: map[string]map[string]struct{}{
				"backend-a": {duplicateLease: {}},
				// The per-lease union selected backend-a above, but backend-b also
				// reported this lease. Raw reporter membership must survive rejection
				// so the collision cannot disappear with the selected payload.
				"backend-b": {duplicateLease: {}, goodProvision: {}},
			},
			answered: answeredSet{"backend-a": true, "backend-b": true},
			complete: true,
		},
		retentions: map[string]string{
			duplicateLease: "backend-a",
			goodRetention:  "backend-b",
		},
		retentionsAnswered: answeredSet{"backend-a": true, "backend-b": true},
		retentionsReportedByBackend: map[string]map[string]struct{}{
			"backend-a": {duplicateLease: {}},
			"backend-b": {goodRetention: {}},
		},
	}

	inventory.rejectCrossEndpointDuplicates()

	assert.False(t, inventory.complete(), "contradictory inventory must not arm the baseline")
	assert.False(t, inventory.fleet.answered.heard("backend-a"))
	assert.False(t, inventory.retentionsAnswered.heard("backend-a"))
	assert.NotContains(t, inventory.fleet.provisions, duplicateLease)
	assert.NotContains(t, inventory.retentions, duplicateLease)
	assert.Contains(t, inventory.fleet.reportedByBackend["backend-a"], duplicateLease,
		"raw positive membership must survive as conservative ambiguity evidence")
	assert.Contains(t, inventory.retentionsReportedByBackend["backend-a"], duplicateLease,
		"raw positive membership must survive as conservative ambiguity evidence")
	assert.Contains(t, inventory.untrustedPositiveObservations[duplicateLease], "backend-a",
		"rejected positives must make apparent absence untrusted")
	assert.Equal(t, []string{"backend-a", "backend-b"}, ambiguousReportedOwners(
		inventory.fleet.reportedByBackend,
		inventory.retentionsReportedByBackend,
	)[duplicateLease], "rejecting the selected union payload must not erase a shared collision")

	require.Contains(t, inventory.fleet.provisions, goodProvision)
	assert.Equal(t, "backend-b", inventory.fleet.provisions[goodProvision].BackendName)
	assert.Equal(t, "backend-b", inventory.retentions[goodRetention])
	assert.True(t, inventory.fleet.answered.heard("backend-b"))
	assert.True(t, inventory.retentionsAnswered.heard("backend-b"))
}

func TestReconcileInventoryEmptyBackendsUsesRawInventoryBeforeCausalFiltering(t *testing.T) {
	const leaseUUID = "018f47a2-8b1c-7def-8123-456789abcdef"
	backendNames := []string{"backend-a", "backend-b"}
	identities := map[string]backendidentity.ID{
		"backend-a": testBackendStorageID("backend-a"),
		"backend-b": testBackendStorageID("backend-b"),
	}
	inventory := reconcileInventory{
		chainLeases: map[string]billingtypes.Lease{},
		fleet: fleetSnapshot{
			provisions: map[string]backend.ProvisionInfo{
				leaseUUID: {LeaseUUID: leaseUUID, BackendName: "backend-a"},
			},
			reportedByBackend: map[string]map[string]struct{}{
				"backend-a": {leaseUUID: {}},
				"backend-b": {},
			},
			answered: answeredSet{"backend-a": true, "backend-b": true},
			complete: true,
		},
		retentions:         map[string]string{},
		retentionsAnswered: answeredSet{"backend-a": true, "backend-b": true},
		retentionsReportedByBackend: map[string]map[string]struct{}{
			"backend-a": {},
			"backend-b": {},
		},
		backendStorageIdentities: identities,
	}

	authority := newTestPlacementAuthority(t)
	configureTestPlacementTopology(t, authority, backendNames)
	capture := &projectionCapture{ReconcilerPlacement: authority}
	fence := capture.BeginInventorySession()
	defer capture.EndInventorySession(fence)
	reconciler := &Reconciler{
		placementAuthority: capture,
		maxWorkers:         1,
	}

	_, err := reconciler.projectPlacementInventory(t.Context(), reconcileProjectionInput{
		inventory:          inventory,
		inventoryFence:     fence,
		inFlightAtSnapshot: map[string]struct{}{leaseUUID: {}},
	})
	require.NoError(t, err)
	require.True(t, capture.projection.Complete)
	assert.Empty(t, capture.projection.Placements,
		"the in-flight positive must be causally filtered from placement authority")
	assert.Equal(t, []string{"backend-b"}, capture.projection.EmptyBackends,
		"raw positive evidence must keep backend-a from being certified empty")
}

func TestWithoutConflictCandidatesDoesNotCertifyOutstandingSideEffectEmpty(t *testing.T) {
	empty := []string{"backend-a", "backend-b", "backend-c"}
	conflicts := map[string][]string{
		"lease-1": {"backend-a", "backend-d"},
		"lease-2": {"backend-b", "backend-e"},
	}

	assert.Equal(t, []string{"backend-c"}, withoutConflictCandidates(empty, conflicts))
	assert.Nil(t, withoutConflictCandidates(nil, conflicts),
		"a partial inventory must keep nil empty-backend evidence")
}
