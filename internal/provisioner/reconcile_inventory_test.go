package provisioner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestReconcileInventoryBackendRejectionPreservesPositiveMembership(t *testing.T) {
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

	inventory.rejectBackend("backend-a")

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
