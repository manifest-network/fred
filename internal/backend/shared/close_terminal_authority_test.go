package shared

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

func TestCloseTerminalAuthorityPreservesUnsettledLaunch(t *testing.T) {
	for _, retain := range []bool{false, true} {
		name := "destroyed"
		if retain {
			name = "retained"
		}
		t.Run(name, func(t *testing.T) {
			stores := openOperationHandoffStores(t, "docker-close-debt")
			h := newVolumeDebtHarness(t, stores)
			var origin VolumeLaunchOrigin
			h.execute(t, name, func(ctx context.Context, runner substratemutation.Runner, subject OperationPhysicalSubject) error {
				origin = VolumeLaunchForOperation(subject)
				_, err := h.journal.Begin(origin, nil)
				require.NoError(t, err)
				return runner.Step(ctx, MaintenanceTargetLaunchStep, func(context.Context) error {
					return errors.New("daemon dispatch response lost")
				})
			})
			settlement := newCloseSettlementForTest(t, stores)
			bindTestCloseMutation(t, settlement,
				func(substratemutation.Runner, context.Context, ClosePhysicalSubject) error { return nil },
				func(subject ClosePhysicalSubject) (ClosePhysicalEvidence, error) {
					if !retain {
						return NewCloseDestroyed(subject)
					}
					proof, err := settlement.ProveRetention(subject.Intent())
					if err != nil {
						return ClosePhysicalEvidence{}, err
					}
					return NewCloseRetained(subject, proof)
				})
			claim := admitSettlementClose(t, settlement, origin.operation.LeaseUUID(), retain)
			if retain {
				recorded, err := settlement.RecordRetention(claim, "partition-a",
					[]string{"fred-retained-" + claim.LeaseUUID() + "-app-0"})
				require.NoError(t, err)
				require.True(t, recorded)
			}
			execution := startTestCloseExecution(t, settlement, claim)
			outcome := settlement.ExecuteClose(t.Context(), execution)
			pending, ok := outcome.(CloseExecutionPending)
			require.True(t, ok, "physical absence/retention cannot terminalize unknown prior dispatch: %T", outcome)
			require.ErrorIs(t, pending.Cause(), ErrVolumeLaunchUnsettled)
			require.False(t, pending.RetryableNow())
			_, err := h.journal.Begin(origin, nil)
			require.Error(t, err, "a close head revokes the old origin's ability to create new debt")
			_, found, err := settlement.GetCloseIntent(claim.LeaseUUID())
			require.NoError(t, err)
			require.True(t, found)
			count, err := h.journal.PendingCount()
			require.NoError(t, err)
			require.Equal(t, 1, count)
			callbacks, err := stores.callbacks.ListPending()
			require.NoError(t, err)
			require.Len(t, callbacks, 1)
			require.Equal(t, "failed", string(callbacks[0].Status))
		})
	}
}

func TestCloseTerminalAuthorityReattestsBeforeReleaseRetirement(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-close-proof")
	h := newVolumeDebtHarness(t, stores)
	var debt VolumeLaunchDebt
	h.execute(t, "other-lease", func(_ context.Context, _ substratemutation.Runner, subject OperationPhysicalSubject) error {
		var err error
		debt, err = h.journal.Begin(VolumeLaunchForOperation(subject), nil)
		require.NoError(t, err)
		return errors.New("unrelated launch remains unresolved")
	})
	settlement := newCloseSettlementForTest(t, stores)
	bindTestCloseMutation(t, settlement, nil, nil)
	spec := seedCloseSettlementRelease(t, stores, "close-proof-current")
	claim := admitSettlementClose(t, settlement, spec.LeaseUUID, false)
	execution := startTestCloseExecution(t, settlement, claim)
	outcome, ok := settlement.ExecuteClose(t.Context(), execution).(CloseExecutionDestroyed)
	require.True(t, ok, "unrelated valid debt does not block this namespace")
	invalid := outcome
	invalid.authority = closeTerminalAuthority{}
	_, err := settlement.CompleteClose(invalid)
	require.Error(t, err, "physical evidence without journal authority cannot settle")
	otherSpec := seedCloseSettlementRelease(t, stores, "close-proof-other")
	otherExecution := startTestCloseExecution(t, settlement, admitSettlementClose(t, settlement, otherSpec.LeaseUUID, false))
	other, ok := settlement.ExecuteClose(t.Context(), otherExecution).(CloseExecutionDestroyed)
	require.True(t, ok)
	invalid.authority = other.authority
	_, err = settlement.CompleteClose(invalid)
	require.Error(t, err, "another exact subject's quiescence proof cannot settle")

	// Simulate durable evidence changing after observation. Production Begin
	// rejects the closed origin (covered above), but a captured proof must still
	// fail closed if storage is externally replaced or repaired inconsistently.
	changed := debt.record
	changed.LeaseUUID = claim.LeaseUUID()
	data, err := json.Marshal(changed)
	require.NoError(t, err)
	require.NoError(t, stores.callbacks.update(func(tx *bolt.Tx) error {
		return tx.Bucket(volumeLaunchDebtBucketName).Put([]byte(volumeLaunchKey(changed)), data)
	}))
	_, err = settlement.CompleteClose(outcome)
	require.ErrorIs(t, err, ErrVolumeLaunchUnsettled)
	active, err := stores.releases.LatestActive(claim.LeaseUUID())
	require.NoError(t, err)
	require.NotNil(t, active, "terminal re-attestation must precede any release retirement")
	_, found, err := settlement.GetCloseIntent(claim.LeaseUUID())
	require.NoError(t, err)
	require.True(t, found)
}
