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
	_, err := activateMaintenanceForTest(t, h.b.maintenanceSettlement, h.target)
	require.NoError(t, err)
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
			ActiveOperationID:    h.source.OperationID,
			Items:                append([]backend.LeaseItem(nil), h.source.Items...),
			ResourceProfiles:     shared.CloneSKUResourceSnapshot(sourceProfiles),
			ContainerIDs:         []string{"source-container"},
			StackManifest:        h.targetReleaseStack(),
			ServiceContainers:    map[string][]string{"web": {"source-container"}},
		},
	}
	actor := h.b.actorFor(h.leaseUUID)
	require.Equal(t, backend.ProvisionStatusReady, actor.State())

	// Model the crash boundary after substrate activation but before
	// intent→outbox settlement. Recovery owns the actor's quiescence claim,
	// retires that stale generation, and publishes the target projection
	// directly; it never re-enters the excluded actor.
	intents, err := h.b.maintenanceSettlement.ListMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)

	require.NoError(t, h.b.recoverMaintenanceIntents(t.Context()))
	h.assertSettled(backend.CallbackStatusSuccess)
	assertMaintenanceResourceProfiles(t, h.b, h.leaseUUID, target.ResourceProfiles)
	freshActor := h.b.actorFor(h.leaseUUID)
	require.NotSame(t, actor, freshActor)
	require.Equal(t, backend.ProvisionStatusReady, freshActor.State())
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
	b.provisionsMu.RUnlock()
	require.Equal(t, want, direct)

}
