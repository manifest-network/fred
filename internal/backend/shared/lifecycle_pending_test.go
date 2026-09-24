package shared

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

func TestLifecyclePendingRequiresJournalIssuedContention(t *testing.T) {
	releases, callbacks, source, target := maintenanceFixture(t, "pending-observation")
	first := newMaintenanceIntentSpec(t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart,
		source, target, "docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"))
	_, err := callbacks.BeginMaintenanceIntent(first)
	require.NoError(t, err)
	second := maintenanceSpecFromLatestActive(t, callbacks, releases, source.LeaseUUID(), MaintenanceIntentRestart)
	_, err = callbacks.BeginMaintenanceIntent(second)
	require.ErrorIs(t, err, ErrMaintenanceIntentConflict)
	require.True(t, IsLifecyclePending(fmt.Errorf("publish maintenance: %w", err)))
	require.False(t, IsLifecyclePending(fmt.Errorf("%w: admitted lifecycle work remains pending", ErrMaintenanceIntentConflict)))
	require.False(t, IsLifecyclePending(CloseExecutionPending{}))
	require.False(t, IsLifecyclePending(errors.New("storage journal corrupt")))
	require.False(t, IsLifecyclePending(fmt.Errorf("%w: fabricated launch debt", ErrVolumeLaunchUnsettled)))
}

func TestCloseTerminalPendingRejectsSentinelAndForeignNamespace(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	bindTestCloseMutation(t, settlement, nil, nil)
	spec := seedCloseSettlementRelease(t, stores, "pending-terminal-source")
	claim := admitSettlementClose(t, settlement, spec.LeaseUUID, false)
	execution := startTestCloseExecution(t, settlement, claim)
	for _, cause := range []error{
		ErrVolumeLaunchUnsettled,
		fmt.Errorf("untrusted observation: %w", ErrVolumeLaunchUnsettled),
		errors.New("corrupt launch journal"),
		volumeLaunchNamespacePending{record: volumeLaunchDebtRecord{LeaseUUID: testLeaseUUID("another-lease")}},
	} {
		pending := settlement.pendingCloseTerminal(execution.subject, cause)
		require.False(t, IsLifecyclePending(pending), "only the exact durable namespace observation may issue lifecycle pending: %v", cause)
	}
}

func TestLifecyclePendingClosePreservesExecutionAndRejectsBoundaryFailures(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	lost := errors.New("response lost")
	bindTestCloseMutation(t, settlement,
		func(runner substratemutation.Runner, ctx context.Context, _ ClosePhysicalSubject) error {
			return runner.Step(ctx, "close uncertain effect", func(context.Context) error { return lost })
		},
		func(subject ClosePhysicalSubject) (ClosePhysicalEvidence, error) { return NewCloseIncomplete(subject) })
	spec := seedCloseSettlementRelease(t, stores, "pending-close-observation")
	claim := admitSettlementClose(t, settlement, spec.LeaseUUID, false)
	execution := startTestCloseExecution(t, settlement, claim)
	pending, ok := settlement.ExecuteClose(t.Context(), execution).(CloseExecutionPending)
	require.True(t, ok)
	require.True(t, IsLifecyclePending(pending))
	require.ErrorIs(t, pending, lost, "internal recovery retains the original cause")
	require.False(t, pending.RetryableNow(), "transport observation cannot authorize replay of ambiguous work")
	_, found, err := settlement.GetCloseIntent(spec.LeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	invalid, ok := settlement.ExecuteClose(t.Context(), CloseExecutionClaim{}).(CloseExecutionPending)
	require.True(t, ok)
	require.False(t, IsLifecyclePending(invalid), "invalid boundaries remain server failures")
	reused, ok := settlement.ExecuteClose(t.Context(), execution).(CloseExecutionPending)
	require.True(t, ok)
	require.False(t, IsLifecyclePending(reused), "a consumed execution cannot manufacture an availability observation")
}
