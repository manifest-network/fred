package docker

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

func TestRecoverMaintenanceWarmActorProjectsTargetResourceProfilesIdempotently(t *testing.T) {
	h := newMaintenanceRecoveryHarness(t)
	h.appendTarget(true)
	require.NoError(t, h.releases.ActivateMaintenance(h.target))
	target, err := h.releases.LatestActive(h.leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, target)
	targetQuantity, err := backend.ValidateOperationQuantities(target.Items)
	require.NoError(t, err)
	targetContainers := h.containersFor(*target, targetQuantity, "running", HealthStatusNone)
	h.inventory.containers = targetContainers

	// Make the warm actor's source projection observably different from the
	// durable target. The Docker wrapper temporarily mirrors this field while
	// construction sites migrate to the actor-owned ProvisionState field; this
	// regression pins both sides of that adapter seam.
	sourceProfiles := shared.CloneSKUResourceSnapshot(target.ResourceProfiles)
	require.NotEmpty(t, sourceProfiles)
	sourceProfiles[0].MemoryMB++
	require.NotEqual(t, sourceProfiles, target.ResourceProfiles)
	h.b.provisions[h.leaseUUID] = &provision{
		ProvisionState: leasesm.ProvisionState{
			LeaseUUID:            h.leaseUUID,
			Tenant:               h.source.RuntimeAuthority.Tenant(),
			ProviderUUID:         h.source.RuntimeAuthority.ProviderUUID(),
			Status:               backend.ProvisionStatusReady,
			CallbackURL:          h.source.RuntimeAuthority.CallbackURL(),
			LifecycleCallbackURL: h.source.RuntimeAuthority.LifecycleCallbackURL(),
			Items:                append([]backend.LeaseItem(nil), h.source.Items...),
			ResourceProfiles:     shared.CloneSKUResourceSnapshot(sourceProfiles),
			ContainerIDs:         []string{"source-container"},
			StackManifest:        h.targetReleaseStack(),
			ServiceContainers:    map[string][]string{"web": {"source-container"}},
		},
		ResourceProfiles: shared.CloneSKUResourceSnapshot(sourceProfiles),
	}
	actor := h.b.actorFor(h.leaseUUID)
	require.Equal(t, backend.ProvisionStatusReady, actor.State())

	// Model the crash boundary after actor projection but before intent→outbox
	// settlement. The first convergence must update both the actor-owned state
	// and Docker's directly-read wrapper field.
	routed, err := h.b.convergeMaintenanceSuccess(
		t.Context(), h.intent, *target, targetContainers,
	)
	require.NoError(t, err)
	require.True(t, routed)
	assertMaintenanceResourceProfiles(t, h.b, h.leaseUUID, target.ResourceProfiles)
	intents, err := h.callbacks.ListMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)

	// Recovery replays the same typed projection before atomically consuming the
	// still-live WAL. The actor is already Ready, so this exercises the dedicated
	// same-state repair path without regressing either resource-profile view.
	require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
	h.assertSettled(backend.CallbackStatusSuccess)
	assertMaintenanceResourceProfiles(t, h.b, h.leaseUUID, target.ResourceProfiles)
	require.Equal(t, backend.ProvisionStatusReady, actor.State())
}

func assertMaintenanceResourceProfiles(
	t *testing.T,
	b *Backend,
	leaseUUID string,
	want []shared.SKUResourceSnapshot,
) {
	t.Helper()
	b.provisionsMu.RLock()
	direct := shared.CloneSKUResourceSnapshot(b.provisions[leaseUUID].ResourceProfiles)
	embedded := shared.CloneSKUResourceSnapshot(b.provisions[leaseUUID].ProvisionState.ResourceProfiles)
	b.provisionsMu.RUnlock()
	require.Equal(t, want, direct)
	require.Equal(t, want, embedded)

	projected, found := b.provisionStore.Get(leaseUUID)
	require.True(t, found)
	require.Equal(t, want, projected.ResourceProfiles)
}
