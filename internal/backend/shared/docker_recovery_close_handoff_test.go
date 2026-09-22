package shared

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestDockerRecoveryRepairPreservesCloseAfterMaintenanceHandoff(t *testing.T) {
	for _, origin := range []string{"target", "compensation source"} {
		t.Run(origin, func(t *testing.T) {
			fixture := beginBoundMaintenance(t, "close-with-unknown-launch")
			target := fixture.appendAndBind(t)
			state := &compensationTestState{}
			if origin == "target" {
				state.targetErr = errors.New("target Create reply lost")
			} else {
				state.sourceErr = errors.New("compensation Start reply lost")
			}
			// This executor reports the target absent, but that observation cannot
			// settle a request whose daemon reply was lost.
			bindCompensationTest(t, fixture.settlement, state)
			execution, err := fixture.settlement.StartMaintenanceExecution(target)
			require.NoError(t, err)
			require.IsType(t, MaintenanceExecutionAmbiguous{},
				fixture.settlement.ExecuteMaintenance(t.Context(), execution))
			leaseUUID := target.LeaseUUID()
			require.ErrorIs(t, state.journal.CheckNamespace(leaseUUID), ErrVolumeLaunchUnsettled)

			closeSettlement := newCloseSettlementForTest(t, fixture.stores)
			request, err := closeSettlement.NewCloseRequest(leaseUUID, false)
			require.NoError(t, err)
			admission, err := closeSettlement.BeginClose(request)
			require.NoError(t, err)
			require.True(t, admission.MaintenancePreempted())
			claim := admission.Claim()
			require.Equal(t, target.MaintenanceID(), claim.InterruptedMaintenanceID())
			_, found, err := fixture.settlement.GetMaintenanceIntent(leaseUUID)
			require.NoError(t, err)
			require.False(t, found, "close replaced the live maintenance head")
			require.ErrorIs(t, state.journal.CheckNamespace(leaseUUID), ErrVolumeLaunchUnsettled,
				"changing the mutation owner cannot settle an outstanding Docker request")

			stopCloseHandoffStores(t, fixture.stores)
			reopened := reopenCloseHandoffStores(t, fixture.stores)
			journal, err := NewVolumeLaunchJournal(reopened.callbacks)
			require.NoError(t, err)
			require.ErrorIs(t, journal.CheckNamespace(leaseUUID), ErrVolumeLaunchUnsettled,
				"restart must preserve the old request's namespace fence")
			recovered, found, err := newCloseSettlementForTest(t, reopened).GetCloseIntent(leaseUUID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, claim.IntentID(), recovered.IntentID())
			require.Equal(t, claim.InterruptedMaintenanceID(), recovered.InterruptedMaintenanceID())
			stopCloseHandoffStores(t, reopened)

			inspection, err := InspectDockerRecovery(t.Context(), reopened.callbackPath, reopened.storage)
			require.NoError(t, err)
			require.Equal(t, 1, inspection.Launches)
			require.Equal(t, []string{leaseUUID}, inspection.Leases)
			backup := filepath.Join(t.TempDir(), "callbacks.before-fence.db")
			// All fixture journals are stopped. The verifier stands for the
			// external daemon/storage attestation required by the offline command;
			// no runtime inventory or close claim can supply its acknowledgement.
			verify := func(ctx context.Context) error { return ctx.Err() }
			_, err = RepairDockerRecovery(t.Context(), reopened.callbackPath, reopened.storage,
				"close already owns the lease", backup, verify)
			require.ErrorContains(t, err, "acknowledgement does not match")
			result, err := RepairDockerRecovery(t.Context(), reopened.callbackPath, reopened.storage,
				inspection.Acknowledgement, backup, verify)
			require.NoError(t, err)
			require.Equal(t, "DOCKER_EFFECTS_FENCED", result.Verdict)
			preserved, err := InspectDockerRecovery(t.Context(), backup, reopened.storage)
			require.NoError(t, err)
			require.Equal(t, inspection.SnapshotSHA256, preserved.SnapshotSHA256)
			require.Equal(t, 1, preserved.Launches)

			after := reopenCloseHandoffStores(t, reopened)
			settlement := newCloseSettlementForTest(t, after)
			current, found, err := settlement.GetCloseIntent(leaseUUID)
			require.NoError(t, err)
			require.True(t, found, "offline repair must leave close completion to its executor")
			require.Equal(t, claim.IntentID(), current.IntentID())
			require.Equal(t, claim.InterruptedMaintenanceID(), current.InterruptedMaintenanceID())
			journal, err = NewVolumeLaunchJournal(after.callbacks)
			require.NoError(t, err)
			require.NoError(t, journal.CheckNamespace(leaseUUID))
			pending, err := after.callbacks.ListPending()
			require.NoError(t, err)
			require.Len(t, pending, 1)
			require.Equal(t, backend.CallbackStatusFailed, pending[0].Status,
				"repair preserves maintenance's receipt and does not fabricate close success")
		})
	}
}

func stopCloseHandoffStores(t *testing.T, stores operationHandoffStores) {
	t.Helper()
	require.NoError(t, stores.callbacks.Close())
	require.NoError(t, stores.releases.Close())
	require.NoError(t, stores.retentions.Close())
}

func reopenCloseHandoffStores(t *testing.T, stores operationHandoffStores) operationHandoffStores {
	t.Helper()
	var err error
	stores.callbacks, err = OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, stores.gate)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stores.callbacks.Close()) })
	stores.releases, err = OpenIdentityBoundReleaseStore(
		ReleaseStoreConfig{DBPath: stores.releasePath}, stores.storage, stores.gate)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stores.releases.Close()) })
	stores.retentions, err = OpenIdentityBoundRetentionStore(
		RetentionStoreConfig{DBPath: stores.retentionPath}, stores.storage, stores.gate)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stores.retentions.Close()) })
	return stores
}
