package shared

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

const inspectionJournalTestImage = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestImageInspectionJournalCompensationOriginRequiresUndispatchedSource(t *testing.T) {
	f := beginBoundMaintenance(t, "inspection-source-preparation")
	target := f.appendAndBind(t)
	state := &compensationTestState{beforeSourceErr: errors.New("pause before source launch")}
	bindCompensationTest(t, f.settlement, state)
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	require.IsType(t, MaintenanceExecutionAmbiguous{}, f.settlement.ExecuteMaintenance(t.Context(), execution))
	origin := ImageInspectionForCompensation(state.sourceSubject)
	journal, err := NewImageInspectionJournal(f.settlement.callbacks)
	require.NoError(t, err)
	_, err = journal.Reserve(ImageInspectionForCompensation(MaintenanceCompensationSubject{}), inspectionJournalTestImage, "source:captured")
	require.Error(t, err)
	foreign := openOperationHandoffStores(t, "foreign-inspection-source")
	foreignJournal, err := NewImageInspectionJournal(foreign.callbacks)
	require.NoError(t, err)
	_, err = foreignJournal.Reserve(origin, inspectionJournalTestImage, "source:captured")
	require.ErrorContains(t, err, "another journal")
	receipt, err := journal.Reserve(origin, inspectionJournalTestImage, "source:captured")
	require.NoError(t, err)
	require.Equal(t, "compensation", receipt.Kind())
	require.Equal(t, target.LeaseUUID(), receipt.LeaseUUID())
	require.Equal(t, target.MaintenanceID().String(), receipt.SubjectID())
	_, err = state.journal.Begin(VolumeLaunchForCompensation(state.sourceSubject), nil)
	require.NoError(t, err)
	_, err = journal.Reserve(origin, inspectionJournalTestImage, "source:captured")
	require.Error(t, err, "copied preparation origin cannot create helpers after source dispatch")
	receipts, err := journal.List()
	require.NoError(t, err)
	require.Len(t, receipts, 1, "phase transition does not consume independent helper cleanup")
}

func startedInspectionOrigin(t *testing.T, stores operationHandoffStores) ImageInspectionOrigin {
	t.Helper()
	var origin ImageInspectionOrigin
	require.NoError(t, BindOperationSubstrateExecutor(stores.settlement,
		func(ctx context.Context, _ string) (context.Context, func(), error) { return ctx, func() {}, nil },
		func(context.Context, string, error) error { return nil },
		func(runner substratemutation.Runner, subject OperationPhysicalSubject) func(context.Context) error {
			origin = ImageInspectionForOperation(subject)
			return func(ctx context.Context) error {
				return runner.Step(ctx, "prepare inspection fixture", func(context.Context) error { return nil })
			}
		},
		func(ctx context.Context, run func(context.Context) error, _ OperationPhysicalSubject) error {
			return run(ctx)
		},
		func(context.Context, OperationPhysicalSubject) (OperationPhysicalEvidence, error) {
			return OperationPhysicalEvidence{}, errors.New("fixture preserves Started subject")
		},
	))
	claim := beginHandoffOperation(t, stores.settlement, testOperationIntentSpec(t, "image-inspection"))
	candidate, err := stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	execution, err := stores.settlement.StartOperationExecution(candidate)
	require.NoError(t, err)
	_ = stores.settlement.ExecuteOperation(t.Context(), execution)
	require.True(t, origin.operation.Valid())
	return origin
}

func TestImageInspectionJournalRequiresExactStartedSubjectAndOpenStore(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-inspection")
	origin := startedInspectionOrigin(t, stores)
	journal, err := NewImageInspectionJournal(stores.callbacks)
	require.NoError(t, err)
	_, err = journal.Reserve(ImageInspectionOrigin{}, inspectionJournalTestImage, "fixture:latest")
	require.ErrorContains(t, err, "live Started")
	var decoded ImageInspectionOrigin
	require.NoError(t, json.Unmarshal([]byte(`{"operation":"copied-fields","lease_uuid":"ignored"}`), &decoded)) //nolint:staticcheck // SA9005: verify JSON cannot mint authority.
	_, err = journal.Reserve(decoded, inspectionJournalTestImage, "fixture:latest")
	require.ErrorContains(t, err, "live Started")
	foreign := openOperationHandoffStores(t, "docker-inspection")
	foreignJournal, err := NewImageInspectionJournal(foreign.callbacks)
	require.NoError(t, err)
	_, err = foreignJournal.Reserve(origin, inspectionJournalTestImage, "fixture:latest")
	require.ErrorContains(t, err, "another journal")
	receipt, err := journal.Reserve(origin, inspectionJournalTestImage, "fixture:latest")
	require.NoError(t, err)
	assert.Equal(t, origin.operation.LeaseUUID(), receipt.LeaseUUID())
	assert.Equal(t, origin.operation.OperationID().String(), receipt.SubjectID())
	assert.Equal(t, stores.storage.ID(), receipt.StorageID())
	_, err = foreignJournal.RecordCreated(receipt, strings.Repeat("a", 64))
	require.ErrorContains(t, err, "another journal")
	closeSettlement := newCloseSettlementForTest(t, stores)
	_ = admitSettlementClose(t, closeSettlement, receipt.LeaseUUID(), false)
	_, err = journal.Reserve(origin, inspectionJournalTestImage, "fixture:latest")
	require.ErrorContains(t, err, "replaced")
	receipts, err := journal.List()
	require.NoError(t, err)
	require.Len(t, receipts, 1, "close owns no helper cleanup authority")
}

func TestImageInspectionJournalUnknownCreateSurvivesCloseAndReopen(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-inspection-reopen")
	origin := startedInspectionOrigin(t, stores)
	journal, err := NewImageInspectionJournal(stores.callbacks)
	require.NoError(t, err)
	receipt, err := journal.Reserve(origin, inspectionJournalTestImage, "fixture:latest")
	require.NoError(t, err)
	require.ErrorContains(t, journal.ForgetRemoved(receipt), "permanent recovery receipt")
	closeSettlement := newCloseSettlementForTest(t, stores)
	_ = admitSettlementClose(t, closeSettlement, receipt.LeaseUUID(), false)
	require.NoError(t, stores.callbacks.Close())
	reopened, err := OpenIdentityBoundCallbackStore(CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, stores.gate)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	newJournal, err := NewImageInspectionJournal(reopened)
	require.NoError(t, err)
	receipts, err := newJournal.List()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	assert.Equal(t, receipt.ID(), receipts[0].ID())
	assert.Empty(t, receipts[0].ContainerID())
	_, err = newJournal.RecordCreated(receipt, strings.Repeat("a", 64))
	require.ErrorContains(t, err, "another journal", "old process receipt cannot commit through fresh owner")
	_, err = newJournal.Reserve(origin, inspectionJournalTestImage, "fixture:latest")
	require.ErrorContains(t, err, "another journal")
}

func TestImageInspectionJournalCreateResponseTransitionInvalidatesPriorReceipt(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-inspection-created")
	origin := startedInspectionOrigin(t, stores)
	journal, err := NewImageInspectionJournal(stores.callbacks)
	require.NoError(t, err)
	pending, err := journal.Reserve(origin, inspectionJournalTestImage, "fixture:latest")
	require.NoError(t, err)
	created, err := journal.RecordCreated(pending, strings.Repeat("a", 64))
	require.NoError(t, err)
	_, err = journal.RecordCreated(pending, strings.Repeat("b", 64))
	require.ErrorContains(t, err, "changed")
	_, err = journal.RecordCreated(created, strings.Repeat("b", 64))
	require.ErrorContains(t, err, "already recorded")
	require.NoError(t, journal.ForgetRemoved(created))
	receipts, err := journal.List()
	require.NoError(t, err)
	assert.Empty(t, receipts, "known completed Create does not accrue permanent receipts")
}

func TestImageInspectionJournalRejectsCorruptAndForeignDurableRecords(t *testing.T) {
	for _, name := range []string{"duplicate", "unknown field", "future schema", "bad image", "bad container", "foreign lineage", "key mismatch"} {
		t.Run(name, func(t *testing.T) {
			stores := openOperationHandoffStores(t, "docker-inspection-corrupt")
			origin := startedInspectionOrigin(t, stores)
			journal, err := NewImageInspectionJournal(stores.callbacks)
			require.NoError(t, err)
			receipt, err := journal.Reserve(origin, inspectionJournalTestImage, "fixture:latest")
			require.NoError(t, err)
			data, err := json.Marshal(receipt.record)
			require.NoError(t, err)
			key := receipt.ID()
			switch name {
			case "duplicate":
				data = []byte(strings.Replace(string(data), `"schema":1`, `"schema":1,"schema":1`, 1))
			case "unknown field":
				data = []byte(strings.Replace(string(data), `"schema":1`, `"schema":1,"future_authority":true`, 1))
			case "future schema":
				data = []byte(strings.Replace(string(data), `"schema":1`, `"schema":2`, 1))
			case "bad image":
				data = []byte(strings.Replace(string(data), inspectionJournalTestImage, "mutable:latest", 1))
			case "bad container":
				data = []byte(strings.Replace(string(data), `"schema":1`, `"schema":1,"container_id":"short-prefix"`, 1))
			case "foreign lineage":
				data = []byte(strings.Replace(string(data), `"backend":"docker-inspection-corrupt"`, `"backend":"foreign"`, 1))
			case "key mismatch":
				key = "11111111-1111-4111-8111-111111111111"
			}
			require.NoError(t, stores.callbacks.update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(imageInspectionsBucketName)
				if err := bucket.Delete([]byte(receipt.ID())); err != nil {
					return err
				}
				return bucket.Put([]byte(key), data)
			}))
			_, err = journal.List()
			require.Error(t, err)
			require.Error(t, stores.callbacks.Healthy())
		})
	}
}

func TestImageInspectionJournalMaintenanceOriginOwnsExactTarget(t *testing.T) {
	f := beginBoundMaintenance(t, "image-inspection")
	target := f.appendAndBind(t)
	journal, err := NewImageInspectionJournal(f.stores.callbacks)
	require.NoError(t, err)
	var subject MaintenancePhysicalSubject
	bindTestMaintenanceMutation(t, f.settlement, func(current MaintenancePhysicalSubject) (MaintenancePhysicalEvidence, error) {
		subject = current
		receipt, err := journal.Reserve(ImageInspectionForMaintenance(current), inspectionJournalTestImage, "fixture:latest")
		require.NoError(t, err)
		assert.Equal(t, "maintenance", receipt.Kind())
		assert.Equal(t, current.MaintenanceID().String(), receipt.SubjectID())
		return MaintenancePhysicalEvidence{}, errors.New("fixture leaves target pending")
	})
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	_ = f.settlement.ExecuteMaintenance(context.Background(), execution)
	require.True(t, subject.Valid())
	closeSettlement := newCloseSettlementForTest(t, f.stores)
	_ = admitSettlementClose(t, closeSettlement, subject.LeaseUUID(), false)
	_, err = journal.Reserve(ImageInspectionForMaintenance(subject), inspectionJournalTestImage, "fixture:latest")
	require.Error(t, err, "stale maintenance target cannot allocate another helper")
	receipts, err := journal.List()
	require.NoError(t, err)
	assert.Len(t, receipts, 1)
}
