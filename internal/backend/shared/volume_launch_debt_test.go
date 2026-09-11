package shared

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
	"github.com/manifest-network/fred/internal/fsidentity"
)

type volumeDebtHarness struct {
	stores  operationHandoffStores
	journal *VolumeLaunchJournal
	run     func(context.Context, substratemutation.Runner, OperationPhysicalSubject) error
}

func newVolumeDebtHarness(t *testing.T, stores operationHandoffStores) *volumeDebtHarness {
	t.Helper()
	j, err := NewVolumeLaunchJournal(stores.callbacks)
	require.NoError(t, err)
	h := &volumeDebtHarness{stores: stores, journal: j}
	require.NoError(t, BindOperationSubstrateExecutor(stores.settlement,
		func(ctx context.Context, _ string) (context.Context, func(), error) { return ctx, func() {}, nil },
		func(context.Context, string, error) error { return nil },
		func(runner substratemutation.Runner, subject OperationPhysicalSubject) func(context.Context) error {
			return func(ctx context.Context) error { return h.run(ctx, runner, subject) }
		},
		func(ctx context.Context, run func(context.Context) error, _ OperationPhysicalSubject) error {
			return run(ctx)
		},
		func(context.Context, OperationPhysicalSubject) (OperationPhysicalEvidence, error) {
			return OperationPhysicalEvidence{}, errors.New("test leaves operation pending")
		},
	))
	return h
}

func (h *volumeDebtHarness) execute(t *testing.T, name string, run func(context.Context, substratemutation.Runner, OperationPhysicalSubject) error) {
	t.Helper()
	h.run = run
	claim := beginHandoffOperation(t, h.stores.settlement, testOperationIntentSpec(t, "volume-debt-"+name))
	candidate, err := h.stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	execution, err := h.stores.settlement.StartOperationExecution(candidate)
	require.NoError(t, err)
	_ = h.stores.settlement.ExecuteOperation(t.Context(), execution)
}

func physicalVolumeForDebt(t *testing.T) fsidentity.Identity {
	t.Helper()
	root, err := fsidentity.OpenDirectory(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, root.Close()) })
	return root.Identity()
}

func TestVolumeLaunchDebtBlocksSamePhysicalDirectoryAcrossAttemptsAndClose(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-volume-debt")
	h := newVolumeDebtHarness(t, stores)
	ids := []fsidentity.Identity{physicalVolumeForDebt(t)}
	var original VolumeLaunchOrigin
	h.execute(t, "uncertain", func(ctx context.Context, runner substratemutation.Runner, subject OperationPhysicalSubject) error {
		original = VolumeLaunchForOperation(subject)
		require.NoError(t, h.journal.Check(original, ids))
		debt, err := h.journal.Begin(original, ids)
		require.NoError(t, err)
		step, err := runner.StepCompleted(ctx, MaintenanceTargetLaunchStep, func(context.Context) error {
			return errors.New("Start response lost; daemon request still outstanding")
		})
		require.Error(t, err)
		require.Error(t, h.journal.Complete(debt, step), "failed/ambiguous launch cannot produce completion authority")
		return err
	})
	require.ErrorIs(t, h.journal.Check(original, ids), ErrVolumeLaunchUnsettled, "same recovered attempt cannot retry its uncertain launch")
	require.ErrorIs(t, h.journal.Check(original, []fsidentity.Identity{physicalVolumeForDebt(t)}), ErrVolumeLaunchUnsettled, "replaced inode cannot reuse the old bind namespace")
	require.ErrorIs(t, h.journal.CheckNamespace(original.operation.LeaseUUID()), ErrVolumeLaunchUnsettled, "old bind source cannot be renamed or destroyed while daemon launch remains unsettled")
	closeSettlement := newCloseSettlementForTest(t, stores)
	_ = admitSettlementClose(t, closeSettlement, original.operation.LeaseUUID(), false)
	require.ErrorIs(t, h.journal.CheckNamespace(original.operation.LeaseUUID()), ErrVolumeLaunchUnsettled, "close does not erase unresolved canonical-path authority")
	h.execute(t, "successor", func(_ context.Context, _ substratemutation.Runner, subject OperationPhysicalSubject) error {
		origin := VolumeLaunchForOperation(subject)
		err := h.journal.Check(origin, ids)
		require.ErrorIs(t, err, ErrVolumeLaunchUnsettled, "different lease/name cannot bypass physical inode debt")
		_, err = h.journal.Begin(origin, ids)
		require.ErrorIs(t, err, ErrVolumeLaunchUnsettled)
		return err
	})
	h.execute(t, "unrelated-directory", func(ctx context.Context, runner substratemutation.Runner, subject OperationPhysicalSubject) error {
		origin := VolumeLaunchForOperation(subject)
		otherIDs := []fsidentity.Identity{physicalVolumeForDebt(t)}
		require.NoError(t, h.journal.Check(origin, otherIDs))
		debt, err := h.journal.Begin(origin, otherIDs)
		require.NoError(t, err)
		step, err := runner.StepCompleted(ctx, MaintenanceTargetLaunchStep, func(context.Context) error { return nil })
		require.NoError(t, err)
		require.NoError(t, h.journal.Complete(debt, step))
		require.NoError(t, h.journal.Check(origin, otherIDs), "settled launch releases only its exact directories")
		return nil
	})
}

func TestVolumeLaunchDebtCompletionRequiresExactUnconsumedEffect(t *testing.T) {
	h := newVolumeDebtHarness(t, openOperationHandoffStores(t, "docker-volume-proof"))
	ids := []fsidentity.Identity{physicalVolumeForDebt(t)}
	h.execute(t, "receipt", func(ctx context.Context, runner substratemutation.Runner, subject OperationPhysicalSubject) error {
		origin := VolumeLaunchForOperation(subject)
		debt, err := h.journal.Begin(origin, ids)
		require.NoError(t, err)
		require.Error(t, h.journal.Complete(debt, substratemutation.CompletedStep{}))
		wrong, err := runner.StepCompleted(ctx, "inspect helper", func(context.Context) error { return nil })
		require.NoError(t, err)
		require.Error(t, h.journal.Complete(debt, wrong))
		require.ErrorIs(t, h.journal.Check(origin, ids), ErrVolumeLaunchUnsettled)
		complete, err := runner.StepCompleted(ctx, MaintenanceTargetLaunchStep, func(context.Context) error { return nil })
		require.NoError(t, err)
		require.NoError(t, h.journal.Complete(debt, complete))
		require.Error(t, h.journal.Complete(debt, complete), "copies cannot consume completion twice")
		require.NoError(t, h.journal.Check(origin, ids))
		return nil
	})
}

func TestVolumeLaunchDebtReopenCannotInventOldDaemonCompletion(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-volume-reopen")
	h := newVolumeDebtHarness(t, stores)
	ids := []fsidentity.Identity{physicalVolumeForDebt(t)}
	var oldDebt VolumeLaunchDebt
	h.execute(t, "interrupted", func(_ context.Context, _ substratemutation.Runner, subject OperationPhysicalSubject) error {
		var err error
		oldDebt, err = h.journal.Begin(VolumeLaunchForOperation(subject), ids)
		require.NoError(t, err)
		return errors.New("process exits after dispatch before completion receipt")
	})
	require.NoError(t, stores.callbacks.Close())
	reopened, err := OpenIdentityBoundCallbackStore(CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, stores.gate)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	stores.callbacks = reopened
	stores.settlement, err = NewOperationSettlement(reopened, stores.releases)
	require.NoError(t, err)
	next := newVolumeDebtHarness(t, stores)
	require.ErrorContains(t, next.journal.Complete(oldDebt, substratemutation.CompletedStep{}), "another journal")
	next.execute(t, "after-restart", func(_ context.Context, _ substratemutation.Runner, subject OperationPhysicalSubject) error {
		err := next.journal.Check(VolumeLaunchForOperation(subject), ids)
		require.ErrorIs(t, err, ErrVolumeLaunchUnsettled)
		return err
	})
	require.NoError(t, reopened.Healthy(), "unsettled debt is valid durable evidence, not corrupted storage")
}

func TestVolumeLaunchDebtRejectsForeignOriginAndInvalidPhysicalIDs(t *testing.T) {
	h := newVolumeDebtHarness(t, openOperationHandoffStores(t, "docker-volume-origin"))
	foreign := openOperationHandoffStores(t, "docker-volume-other")
	j, err := NewVolumeLaunchJournal(foreign.callbacks)
	require.NoError(t, err)
	require.Error(t, j.Check(VolumeLaunchOrigin{}, nil))
	h.execute(t, "origin", func(_ context.Context, _ substratemutation.Runner, subject OperationPhysicalSubject) error {
		origin := VolumeLaunchForOperation(subject)
		require.ErrorContains(t, j.Check(origin, nil), "another journal")
		require.Error(t, h.journal.Check(origin, []fsidentity.Identity{{}}))
		_, err := h.journal.Begin(origin, []fsidentity.Identity{{}})
		require.Error(t, err)
		return nil
	})
	assert.Error(t, h.journal.Complete(VolumeLaunchDebt{}, substratemutation.CompletedStep{}))
}
