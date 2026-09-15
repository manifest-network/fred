package shared

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

func prepareDockerRepair(t *testing.T) operationHandoffStores {
	t.Helper()
	stores := openOperationHandoffStores(t, "docker-offline-repair")
	h := newVolumeDebtHarness(t, stores)
	helpers, err := NewImageInspectionJournal(stores.callbacks)
	require.NoError(t, err)
	h.execute(t, "unknown", func(_ context.Context, _ substratemutation.Runner, subject OperationPhysicalSubject) error {
		_, err := reserveInspectionForTest(helpers, ImageInspectionForOperation(subject), inspectionJournalTestImage, "source:pinned")
		require.NoError(t, err)
		_, err = h.journal.Begin(VolumeLaunchForOperation(subject), nil)
		require.NoError(t, err)
		return errors.New("unknown daemon outcome")
	})
	require.NoError(t, stores.callbacks.Close())
	return stores
}

func TestDockerRecoveryRepairRequiresExactSnapshotAndPreservesBackup(t *testing.T) {
	stores := prepareDockerRepair(t)
	original, err := os.ReadFile(stores.callbackPath)
	require.NoError(t, err)
	inspection, err := InspectDockerRecovery(t.Context(), stores.callbackPath, stores.storage)
	require.NoError(t, err)
	require.Equal(t, 1, inspection.Launches)
	require.Equal(t, 1, inspection.UnknownHelpers)
	afterInspection, err := os.ReadFile(stores.callbackPath)
	require.NoError(t, err)
	require.Equal(t, original, afterInspection, "inspection is read-only")
	backup := filepath.Join(t.TempDir(), "callbacks.before-repair.db")
	verify := func(context.Context) error { return nil }
	_, err = RepairDockerRecovery(t.Context(), stores.callbackPath, stores.storage, "yes", backup, verify)
	require.ErrorContains(t, err, "acknowledgement does not match")
	_, err = os.Stat(backup)
	require.ErrorIs(t, err, os.ErrNotExist)
	result, err := RepairDockerRecovery(t.Context(), stores.callbackPath, stores.storage, inspection.Acknowledgement, backup, verify)
	require.NoError(t, err)
	require.Equal(t, "DOCKER_EFFECTS_FENCED", result.Verdict)
	require.Equal(t, backup, result.Backup)
	backupInspection, err := InspectDockerRecovery(t.Context(), backup, stores.storage)
	require.NoError(t, err)
	require.Equal(t, inspection.SnapshotSHA256, backupInspection.SnapshotSHA256)
	require.Equal(t, 1, backupInspection.Launches)
	require.Equal(t, 1, backupInspection.UnknownHelpers)
	info, err := os.Stat(backup)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	post, err := InspectDockerRecovery(t.Context(), stores.callbackPath, stores.storage)
	require.NoError(t, err)
	require.Zero(t, post.Launches)
	require.Zero(t, post.UnknownHelpers)
	// Fencing completes the old Create; it does not delete an extant helper.
	callbacks, err := OpenIdentityBoundCallbackStore(CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, stores.gate)
	require.NoError(t, err)
	defer func() { require.NoError(t, callbacks.Close()) }()
	helpers, err := NewImageInspectionJournal(callbacks)
	require.NoError(t, err)
	receipts, err := helpers.List()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	require.Empty(t, receipts[0].ContainerID())
	require.True(t, receipts[0].CreationSettled())
	require.NoError(t, helpers.ForgetRemoved(receipts[0]))
}

func TestDockerRecoveryRepairRejectsStaleSnapshotAndExistingBackup(t *testing.T) {
	for _, scenario := range []string{"stale snapshot", "existing backup", "canceled", "storage changed", "backup tampered", "precommit failure"} {
		t.Run(scenario, func(t *testing.T) {
			stores := prepareDockerRepair(t)
			inspection, err := InspectDockerRecovery(t.Context(), stores.callbackPath, stores.storage)
			require.NoError(t, err)
			backup := filepath.Join(t.TempDir(), "before.db")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			verify := func(context.Context) error {
				calls++
				if scenario == "storage changed" || (scenario == "precommit failure" && calls == 4) {
					return errors.New("storage no longer attested")
				}
				if scenario == "backup tampered" && calls == 3 {
					return os.WriteFile(backup, []byte("tampered"), 0o600)
				}
				return nil
			}
			switch scenario {
			case "stale snapshot":
				db, err := bolt.Open(stores.callbackPath, 0o600, nil)
				require.NoError(t, err)
				require.NoError(t, db.Update(func(*bolt.Tx) error { return nil }))
				require.NoError(t, db.Close())
			case "existing backup":
				require.NoError(t, os.WriteFile(backup, []byte("keep"), 0o600))
			case "canceled":
				cancel()
			}
			_, err = RepairDockerRecovery(ctx, stores.callbackPath, stores.storage, inspection.Acknowledgement, backup, verify)
			require.Error(t, err)
			post, err := InspectDockerRecovery(t.Context(), stores.callbackPath, stores.storage)
			require.NoError(t, err)
			require.Equal(t, 1, post.Launches)
			require.Equal(t, 1, post.UnknownHelpers)
			if scenario == "existing backup" {
				data, err := os.ReadFile(backup)
				require.NoError(t, err)
				require.Equal(t, "keep", string(data))
			}
		})
	}
}

func TestDockerRecoveryRepairSettlesOnlyDispatchPhase(t *testing.T) {
	for _, source := range []bool{false, true} {
		t.Run(map[bool]string{false: "target", true: "source"}[source], func(t *testing.T) {
			f := beginBoundMaintenance(t, "offline-dispatch")
			target := f.appendAndBind(t)
			state := &compensationTestState{}
			if source {
				state.sourceErr = errors.New("unknown source Create")
			} else {
				state.targetErr = errors.New("unknown target Up")
			}
			bindCompensationTest(t, f.settlement, state)
			execution, err := f.settlement.StartMaintenanceExecution(target)
			require.NoError(t, err)
			require.IsType(t, MaintenanceExecutionAmbiguous{}, f.settlement.ExecuteMaintenance(t.Context(), execution))
			intent := execution.subject.Intent()
			path, storage := f.settlement.callbacks.binding.dbPath, f.stores.storage
			require.NoError(t, f.settlement.callbacks.Close())
			inspection, err := InspectDockerRecovery(t.Context(), path, storage)
			require.NoError(t, err)
			require.Equal(t, 1, inspection.Launches)
			_, err = RepairDockerRecovery(t.Context(), path, storage, inspection.Acknowledgement, filepath.Join(t.TempDir(), "before.db"), func(context.Context) error { return nil })
			require.NoError(t, err)
			db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true})
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()
			require.NoError(t, db.View(func(tx *bolt.Tx) error {
				record, err := readCompensationTx(tx, intent)
				if err != nil {
					return err
				}
				if source {
					require.Equal(t, compensationSourceSettled, record.Phase)
				} else {
					require.Equal(t, compensationTargetSettled, record.Phase)
				}
				return nil
			}))
		})
	}
}

func TestDockerRecoveryRepairPreservesObservedSourceReady(t *testing.T) {
	f := beginBoundMaintenance(t, "offline-ready-source")
	target := f.appendAndBind(t)
	state := &compensationTestState{sourceErr: errors.New("Start reply lost")}
	bindCompensationTest(t, f.settlement, state)
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	require.IsType(t, MaintenanceExecutionAmbiguous{}, f.settlement.ExecuteMaintenance(t.Context(), execution))
	intent := execution.subject.Intent()
	state.sourceReady = true // The full cohort becomes observable after the lost reply.
	coordinator := newTestRecoveryCoordinator(t, nil, f.settlement, nil)
	_, err = coordinator.WithLease(t.Context(), target.LeaseUUID(), func(scope LeaseRecoveryScope) error {
		outcome, err := f.settlement.RecoverMaintenanceCompensation(t.Context(), scope, intent)
		require.NoError(t, err)
		require.IsType(t, MaintenanceExecutionFailure{}, outcome)
		require.True(t, outcome.(MaintenanceExecutionFailure).SourceRecovered())
		return nil
	})
	require.NoError(t, err)
	assertCompensationJournalPhase(t, f.settlement, intent, compensationSourceReady, 1)
	path, storage := f.settlement.callbacks.binding.dbPath, f.stores.storage
	require.NoError(t, f.settlement.callbacks.Close())
	inspection, err := InspectDockerRecovery(t.Context(), path, storage)
	require.NoError(t, err)
	result, err := RepairDockerRecovery(t.Context(), path, storage, inspection.Acknowledgement, filepath.Join(t.TempDir(), "before.db"), func(context.Context) error { return nil })
	require.NoError(t, err)
	require.Equal(t, "DOCKER_EFFECTS_FENCED", result.Verdict)
	db, err := bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		record, err := readCompensationTx(tx, intent)
		if err != nil {
			return err
		}
		require.Equal(t, compensationSourceReady, record.Phase)
		return nil
	}))
}

func TestDockerRecoveryRepairReportsCommittedVerificationFailure(t *testing.T) {
	stores := prepareDockerRepair(t)
	inspection, err := InspectDockerRecovery(t.Context(), stores.callbackPath, stores.storage)
	require.NoError(t, err)
	backup := filepath.Join(t.TempDir(), "before.db")
	calls := 0
	result, err := RepairDockerRecovery(t.Context(), stores.callbackPath, stores.storage, inspection.Acknowledgement, backup, func(context.Context) error {
		calls++
		if calls == 5 { // First substrate verification after the durable commit.
			return errors.New("daemon unavailable after commit")
		}
		return nil
	})
	require.ErrorContains(t, err, "COMMITTED")
	require.Equal(t, "REPAIR_COMMITTED", result.Verdict)
	require.Equal(t, backup, result.Backup)
	before, err := InspectDockerRecovery(t.Context(), backup, stores.storage)
	require.NoError(t, err)
	require.Equal(t, inspection.SnapshotSHA256, before.SnapshotSHA256)
	post, err := InspectDockerRecovery(t.Context(), stores.callbackPath, stores.storage)
	require.NoError(t, err)
	require.Zero(t, post.Launches)
	require.Zero(t, post.UnknownHelpers)
}
