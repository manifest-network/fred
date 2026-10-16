package shared

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// settleCompensatedFailure runs a bound maintenance whose target fails and
// whose compensation reaches state's source outcome, then fails it.
func settleCompensatedFailure(t *testing.T, name string, state *compensationTestState) (boundMaintenanceFixture, MaintenanceReleaseFailure) {
	t.Helper()
	f := beginBoundMaintenance(t, name)
	target := f.appendAndBind(t)
	bindCompensationTest(t, f.settlement, state)
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	outcome := f.settlement.ExecuteMaintenance(testMaintenanceLifetime(t, t.Context()), execution)
	failed, ok := outcome.(MaintenanceExecutionFailure)
	require.True(t, ok, "%T: %v", outcome, outcome)
	proof, err := f.settlement.FailMaintenance(failed, backend.ReasonInternal, "target failed")
	require.NoError(t, err)
	return f, proof
}

func requireSettledSourceReady(t *testing.T, s *MaintenanceSettlement, proof MaintenanceReleaseFailure, want bool) {
	t.Helper()
	settled, found, err := s.SettledMaintenanceByID(proof.LeaseUUID(), proof.MaintenanceID())
	require.NoError(t, err)
	require.True(t, found, "the failure left a receipt")
	require.False(t, settled.Succeeded())
	require.Equal(t, proof.MaintenanceID(), settled.MaintenanceID())
	source, ready := settled.SourceReady()
	require.Equal(t, want, ready)
	if !want {
		require.Zero(t, source)
		return
	}
	require.Equal(t, SettledSourceRelease{
		Version: proof.intent.entry.SourceReleaseVersion,
		Digest:  proof.intent.entry.SourceReleaseDigest,
	}, source)
	latest, err := s.LatestSettledMaintenance(proof.intent.Kind())
	require.NoError(t, err)
	require.Equal(t, []SettledMaintenance{settled}, latest)
}

// A failed maintenance whose compensation recreated its source records, on its
// receipt, that the source release is the ready cohort and which release that
// is. The fact outlives the compensation journal the settlement deletes.
func TestFailedMaintenanceReceiptRecordsARecreatedReadySource(t *testing.T) {
	state := new(compensationTestState)
	f, proof := settleCompensatedFailure(t, "receipt-source-recreated", state)
	require.Equal(t, 1, state.launches, "the source was recreated")
	_, err := resolveMaintenanceFailureForTest(f.settlement, proof, "target failed; source restored")
	require.NoError(t, err)
	requireSettledSourceReady(t, f.settlement, proof, true)
}

// A failure whose target never displaced the source settles with the source
// as the ready cohort too: the classifier proved it without a recreate.
func TestFailedMaintenanceReceiptRecordsAnUndisplacedReadySource(t *testing.T) {
	state := &compensationTestState{captureErr: errors.New("source image inspection failed"), sourceReady: true}
	f, proof := settleCompensatedFailure(t, "receipt-source-undisplaced", state)
	require.Zero(t, state.launches, "nothing was recreated")
	_, err := resolveMaintenanceFailureForTest(f.settlement, proof, "source capture failed")
	require.NoError(t, err)
	requireSettledSourceReady(t, f.settlement, proof, true)
}

// A failure settled without the classifier's proof, as after a restart, still
// records a recreated source from the compensation journal's source-ready
// phase, read in the settling transaction.
func TestFailedMaintenanceReceiptReadsSourceReadyFromTheCompensationJournal(t *testing.T) {
	state := new(compensationTestState)
	f, proof := settleCompensatedFailure(t, "receipt-source-journal", state)
	claim := proof.intent
	unlock := f.settlement.lockLease(claim.LeaseUUID())
	entry, err := prepareMaintenanceIntentCompletion(claim, backend.CallbackStatusFailed, "settled after restart")
	require.NoError(t, err)
	_, err = f.settlement.callbacks.resolveMaintenanceIntentEntriesLocked(claim, []CallbackEntry{entry}, false)
	unlock()
	require.NoError(t, err)
	requireSettledSourceReady(t, f.settlement, proof, true)
}

// A compensation whose source never became ready attests nothing.
func TestFailedMaintenanceReceiptOmitsAnUnreadySource(t *testing.T) {
	state := &compensationTestState{sourceUnready: true}
	f, proof := settleCompensatedFailure(t, "receipt-source-unready", state)
	_, err := resolveMaintenanceFailureForTest(f.settlement, proof, "target and source failed")
	require.NoError(t, err)
	requireSettledSourceReady(t, f.settlement, proof, false)
}

func TestMaintenanceReceiptSourceReadyValidation(t *testing.T) {
	digest := encodeMaintenanceDigest([32]byte{1})
	storageID, err := backendidentity.Parse("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	base := maintenanceCompletionRecord{
		Version: maintenanceCompletionRecordV1, MaintenanceID: newTestMaintenanceID(t),
		Kind: MaintenanceIntentRestart, LeaseUUID: testLeaseUUID("receipt-validation"),
		RequestDigest: digest, CompletionSequence: 1, Backend: "docker-a",
		BackendStorageID: storageID.String(), Tenant: "tenant-a", ProviderUUID: "33333333-3333-4333-8333-333333333333",
		Status: backend.CallbackStatusFailed, Error: "failed", EffectStarted: true,
		TargetReleaseVersion: 2, TargetReleaseDigest: digest, SettledAt: time.Now().UTC(),
	}
	ready := base
	ready.SourceReady, ready.SourceReleaseVersion, ready.SourceReleaseDigest = true, 1, digest
	require.NoError(t, validateMaintenanceCompletionRecord(ready, ready.LeaseUUID))
	require.NoError(t, validateMaintenanceCompletionRecord(base, base.LeaseUUID))

	for name, mutate := range map[string]func(*maintenanceCompletionRecord){
		"on a success": func(r *maintenanceCompletionRecord) { r.Status, r.Error = backend.CallbackStatusSuccess, "" },
		"without a target": func(r *maintenanceCompletionRecord) {
			r.TargetReleaseVersion, r.TargetReleaseDigest, r.EffectStarted = 0, "", false
		},
		"without a version":       func(r *maintenanceCompletionRecord) { r.SourceReleaseVersion = 0 },
		"with a malformed digest": func(r *maintenanceCompletionRecord) { r.SourceReleaseDigest = "not-a-digest" },
		"a source never proved":   func(r *maintenanceCompletionRecord) { r.SourceReady = false },
	} {
		t.Run(name, func(t *testing.T) {
			record := ready
			mutate(&record)
			require.Error(t, validateMaintenanceCompletionRecord(record, record.LeaseUUID))
		})
	}
}
