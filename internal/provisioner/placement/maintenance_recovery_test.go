package placement

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

func TestMaintenanceRecoveryRejectsPendingReceiptWithoutExactHead(t *testing.T) {
	for _, head := range []string{"", maintenanceIDB} {
		t.Run("head="+head, func(t *testing.T) {
			store := newTestStore(t)
			authority := prepareMaintenanceLease(t, store)
			prepared := testMaintenanceCommand(t, authority, maintenanceIDA, MaintenanceCommandRestart, nil)
			admission, err := store.beginMaintenanceCommand(prepared)
			require.NoError(t, err)
			id := admission.Claim().Command().ID()
			// Simulate a damaged durable head after a valid receipt was admitted.
			// A scheduling snapshot is not permission to recover an orphan row.
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				pending, _, err := maintenanceCommandBuckets(tx)
				if err != nil {
					return err
				}
				if head == "" {
					return pending.Delete([]byte(maintenanceLease))
				}
				return pending.Put([]byte(maintenanceLease), []byte(head))
			}))
			claim, pending, err := store.pendingMaintenanceClaim(maintenanceLease, id)
			require.ErrorIs(t, err, ErrMaintenanceJournalCorrupt)
			require.False(t, pending)
			require.False(t, claim.Valid(), "an orphan receipt cannot issue recovery authority")
		})
	}
}

func TestMaintenanceRecoveryReleasesSettledGenerationDespiteNewPendingID(t *testing.T) {
	authority, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), &executionTestBackend{name: "backend-a"})
	old, _ := admitMaintenanceForTest(t, authority)
	application, err := authority.Application(nil, 0)
	require.NoError(t, err)
	require.Contains(t, application.held, maintenanceLease)
	require.NoError(t, store.settleMaintenanceCommand(old, MaintenanceOutcomeValidationRejected))
	prepared := authority.prepareMaintenanceCommand(t.Context(), mustMaintenanceID(t, maintenanceIDB),
		maintenanceLease, "tenant-test", MaintenanceCommandRestart, nil)
	require.True(t, prepared.Authorized(), prepared.Err())
	fresh, err := authority.beginMaintenanceCommand(prepared)
	require.NoError(t, err)
	snapshot, err := store.maintenanceRecoverySnapshot()
	require.NoError(t, err)
	require.Equal(t, fresh.Claim().Command().ID(), snapshot[maintenanceLease].id)
	require.NoError(t, application.releaseSettled(snapshot))
	require.NotContains(t, application.held, maintenanceLease,
		"a different pending generation must not keep the old terminal lifecycle claim")
	claim := authority.tryClaimLeaseNow(maintenanceLease)
	require.True(t, claim.Acquired(), "release must return actual registry exclusion, not only drop the map entry")
	require.True(t, authority.releaseLease(claim.Claim()))
	_, pending, err := store.pendingMaintenanceClaim(maintenanceLease, fresh.Claim().Command().ID())
	require.NoError(t, err)
	require.True(t, pending, "releasing the old owner cannot settle its successor")
}

func TestMaintenanceRecoveryReportsPendingReceiptWithoutLifecycleClaim(t *testing.T) {
	var calls atomic.Int32
	client := &executionTestBackend{name: "backend-a", restart: func(context.Context, backend.RestartRequest) error {
		calls.Add(1)
		return nil
	}}
	authority, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), client)
	admitted, _ := admitMaintenanceForTest(t, authority)
	application, err := authority.Application(nil, 0)
	require.NoError(t, err)
	// Simulate lost in-memory exclusion without changing the durable command.
	// Recovery must surface the invariant violation rather than skip the row.
	application.release(maintenanceLease, application.held[maintenanceLease])
	require.ErrorContains(t, application.RecoverPending(t.Context()), "has no lifecycle claim")
	require.Zero(t, calls.Load(), "a durable receipt alone cannot dispatch without lifecycle exclusion")
	_, pending, err := store.pendingMaintenanceClaim(maintenanceLease, admitted.Command().ID())
	require.NoError(t, err)
	require.True(t, pending, "the failed recovery preserves unresolved durable work")
}
