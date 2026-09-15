package shared

import (
	"context"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/fsidentity"
)

func TestCompletedEffectCommitENOSPCWithdrawsAuthorityBeforeReceiptRetry(t *testing.T) {
	stores := openOperationHandoffStores(t, "completed-effect-enospc")
	h := newVolumeDebtHarness(t, stores)
	ids := []fsidentity.Identity{physicalVolumeForDebt(t)}
	var leaseUUID string
	effects := 0
	tx := &fakeAuthoritativeStoreWriteTransaction{commitErr: syscall.ENOSPC}
	h.execute(t, "enospc", func(ctx context.Context, runner substratemutation.Runner, subject OperationPhysicalSubject) error {
		leaseUUID = subject.LeaseUUID()
		debt, err := h.journal.Begin(VolumeLaunchForOperation(subject), ids)
		require.NoError(t, err)
		completed, result := runner.StepCompleted(ctx, MaintenanceTargetLaunchStep, func(context.Context) error {
			effects++
			return nil
		})
		require.NoError(t, result.Err())

		// Drive the real durability classifier and authority gate through their
		// transaction interface instead of filling the test machine's disk. A
		// Commit error cannot establish whether bbolt published its meta page.
		err = substratemutation.CommitCompletedStep(completed, subject, MaintenanceTargetLaunchStep, func() error {
			return stores.gate.Run(func() error {
				return finishAuthoritativeStoreWriteTransaction(tx, func() error { return nil })
			})
		})
		require.ErrorIs(t, err, syscall.ENOSPC)
		require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
		require.ErrorIs(t, stores.gate.Error(), backendidentity.ErrMutationOutcomeAmbiguous)

		// Receipt retention does not override its journal's withdrawn storage
		// authority. Even if capacity immediately returns, no second persistence
		// attempt may clear debt against the same unverified store lifetime.
		tx.commitErr = nil
		require.ErrorIs(t, h.journal.Complete(debt, completed), backendidentity.ErrMutationOutcomeAmbiguous)
		require.Equal(t, 1, tx.commitCalls)
		require.Equal(t, 1, effects)
		return err
	})

	require.NoError(t, stores.callbacks.Close())
	freshGate := newTestStorageAuthorityGate(t)
	reopened, err := OpenIdentityBoundCallbackStore(CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, freshGate)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	journal, err := NewVolumeLaunchJournal(reopened)
	require.NoError(t, err)
	require.ErrorIs(t, journal.CheckNamespace(leaseUUID), ErrVolumeLaunchUnsettled,
		"reopening storage does not manufacture the lost durable completion record")
	require.Equal(t, 1, effects)
}

func TestCompletedEffectClosedStoreCannotBeRepairedByReceiptRetry(t *testing.T) {
	stores := openOperationHandoffStores(t, "completed-effect-closed")
	h := newVolumeDebtHarness(t, stores)
	ids := []fsidentity.Identity{physicalVolumeForDebt(t)}
	var debt VolumeLaunchDebt
	var completed substratemutation.CompletedStep
	var leaseUUID string
	effects := 0
	h.execute(t, "closed", func(ctx context.Context, runner substratemutation.Runner, subject OperationPhysicalSubject) error {
		leaseUUID = subject.LeaseUUID()
		var err error
		debt, err = h.journal.Begin(VolumeLaunchForOperation(subject), ids)
		require.NoError(t, err)
		var result substratemutation.StepResult
		completed, result = runner.StepCompleted(ctx, MaintenanceTargetLaunchStep, func(context.Context) error {
			effects++
			return nil
		})
		require.NoError(t, result.Err())
		require.NoError(t, stores.callbacks.Close())
		for range 2 {
			err = h.journal.Complete(debt, completed)
			require.ErrorIs(t, err, bolt.ErrDatabaseNotOpen)
		}
		require.Equal(t, 1, effects)
		return err
	})
	require.ErrorContains(t, h.journal.Complete(debt, completed), "stale",
		"a completed receipt cannot escape its closed execution")
	reopened, err := OpenIdentityBoundCallbackStore(CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, stores.gate)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	journal, err := NewVolumeLaunchJournal(reopened)
	require.NoError(t, err)
	require.ErrorIs(t, journal.CheckNamespace(leaseUUID), ErrVolumeLaunchUnsettled)
	require.ErrorContains(t, journal.Complete(debt, completed), "another journal",
		"a fresh journal cannot inherit completion authority from an old lifetime")
	require.Equal(t, 1, effects)
}
