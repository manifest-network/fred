package shared

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func newMaintenanceDiagnosticsFixture(t *testing.T, name string) (boundMaintenanceFixture, *FailureDiagnostics, MaintenanceExecutionClaim) {
	t.Helper()
	return newMaintenanceDiagnosticsFixtureWithClassifier(t, name, func(subject MaintenancePhysicalSubject) (MaintenancePhysicalEvidence, error) {
		return NewMaintenanceTargetAbsent(subject)
	})
}

func newMaintenanceDiagnosticsFixtureWithClassifier(t *testing.T, name string, classify func(MaintenancePhysicalSubject) (MaintenancePhysicalEvidence, error)) (boundMaintenanceFixture, *FailureDiagnostics, MaintenanceExecutionClaim) {
	t.Helper()
	fixture := beginBoundMaintenance(t, name)
	store, err := NewDiagnosticsStore(DiagnosticsStoreConfig{DBPath: filepath.Join(t.TempDir(), "diagnostics.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	diagnostics, err := NewFailureDiagnostics(store, fixture.stores.settlement, fixture.settlement)
	require.NoError(t, err)
	target := fixture.appendAndBind(t)
	bindTestMaintenanceMutation(t, fixture.settlement, classify)
	execution, err := fixture.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	return fixture, diagnostics, execution
}

func TestFailureDiagnosticsCaptureSurvivesReopenAndEmptyRecovery(t *testing.T) {
	fixture, diagnostics, execution := newMaintenanceDiagnosticsFixture(t, "durable-capture")
	capture, err := diagnostics.CaptureMaintenance(execution.subject, FailureDiagnosticObservation{
		Error: "original physical failure", Reason: backend.ReasonUpdateFailed, Message: "update failed",
		Logs: map[string]string{"web/0": "fatal: required startup file missing"}, ContainerIDs: []string{"target"}, Status: DiagnosticCaptureComplete,
	})
	require.NoError(t, err)
	entry, err := diagnostics.store.Get(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	require.Nil(t, entry, "capture alone has no publication authority")
	path := diagnostics.store.db.Path()
	require.NoError(t, diagnostics.store.Close())
	require.False(t, capture.Valid(), "an old store lifetime cannot grant cleanup")
	reopened, err := NewDiagnosticsStore(DiagnosticsStoreConfig{DBPath: path})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	diagnostics, err = NewFailureDiagnostics(reopened, fixture.stores.settlement, fixture.settlement)
	require.NoError(t, err)
	capture, err = diagnostics.CaptureMaintenance(execution.subject, FailureDiagnosticObservation{Status: DiagnosticCaptureUnavailable})
	require.NoError(t, err)
	snap, status, err := capture.Snapshot()
	require.NoError(t, err)
	require.Equal(t, "fatal: required startup file missing", snap.Logs["web/0"])
	require.Equal(t, "original physical failure", snap.Error)
	require.Equal(t, DiagnosticCaptureComplete, status)
	snap.Logs["web/0"] = "caller mutation"
	again, _, err := capture.Snapshot()
	require.NoError(t, err)
	require.Equal(t, "fatal: required startup file missing", again.Logs["web/0"])
	outcome, ok := fixture.settlement.ExecuteMaintenance(context.Background(), execution).(MaintenanceExecutionFailure)
	require.True(t, ok)
	proof, err := fixture.settlement.FailMaintenance(outcome, backend.ReasonRestartFailed, "recovered interruption")
	require.NoError(t, err)
	publication, err := diagnostics.MaintenanceFailure(proof, FailureDiagnosticObservation{Error: "generic recovery error", Status: DiagnosticCaptureUnavailable})
	require.NoError(t, err)
	require.NoError(t, publication.Publish(1))
	entry, err = reopened.Get(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	require.Equal(t, "original physical failure", entry.Error)
	require.Equal(t, backend.ReasonUpdateFailed, entry.Reason)
	require.Equal(t, "fatal: required startup file missing", entry.Logs["web/0"])
	require.NoError(t, reopened.Store(DiagnosticEntry{LeaseUUID: fixture.intent.LeaseUUID(), Logs: map[string]string{}}))
	entry, err = reopened.Get(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	require.Equal(t, "fatal: required startup file missing", entry.Logs["web/0"], "legacy empty snapshot must not erase published evidence")
	_, err = resolveMaintenanceFailureForTest(fixture.settlement, proof, "failed")
	require.NoError(t, err)
	late, err := diagnostics.CaptureMaintenance(execution.subject, FailureDiagnosticObservation{
		Logs: map[string]string{"late/0": "late same-attempt output"}, Status: DiagnosticCaptureComplete,
	})
	require.NoError(t, err)
	require.NoError(t, diagnostics.RetireHistoricalCapture(late))
	entry, err = reopened.Get(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	require.Equal(t, "late same-attempt output", entry.Logs["late/0"], "an already-authorized selector may gain exact same-attempt evidence")
	// Once the exact head is terminal, a retained publication copy cannot write.
	require.Error(t, publication.Publish(99))
}

func TestFailureDiagnosticsBoundRawAndEscapedLogs(t *testing.T) {
	for _, test := range []struct{ name, value string }{
		{"raw", strings.Repeat("x", MaxFailureDiagnosticLogBytes+1)},
		{"escaped", strings.Repeat("<", MaxFailureDiagnosticLogBytes/4)},
		{"invalid utf8", string([]byte{'a', 0xff, 'b'})},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, diagnostics, execution := newMaintenanceDiagnosticsFixture(t, "bounded-"+test.name)
			capture, err := diagnostics.CaptureMaintenance(execution.subject, FailureDiagnosticObservation{
				Error: strings.Repeat("failure", 16<<10), Logs: map[string]string{"web/0": test.value}, Status: DiagnosticCaptureComplete,
			})
			require.NoError(t, err)
			snap, status, err := capture.Snapshot()
			require.NoError(t, err)
			require.Equal(t, DiagnosticCaptureTruncated, status)
			require.True(t, utf8.ValidString(snap.Logs["web/0"]))
			require.LessOrEqual(t, len(snap.Logs["web/0"]), MaxFailureDiagnosticLogBytes)
			record, err := diagnostics.store.readAttempt(capture.identity)
			require.NoError(t, err)
			encoded, err := json.Marshal(record)
			require.NoError(t, err)
			require.LessOrEqual(t, len(encoded), MaxFailureDiagnosticEncodedBytes)
			require.LessOrEqual(t, len(snap.Error), 16<<10)
		})
	}
}

func TestFailureDiagnosticsRejectForeignAndZeroAuthority(t *testing.T) {
	_, diagnostics, execution := newMaintenanceDiagnosticsFixture(t, "owner")
	_, foreign, _ := newMaintenanceDiagnosticsFixture(t, "foreign")
	observation := FailureDiagnosticObservation{Status: DiagnosticCaptureUnavailable}
	_, err := foreign.CaptureMaintenance(execution.subject, observation)
	require.Error(t, err)
	_, err = diagnostics.CaptureMaintenance(MaintenancePhysicalSubject{}, observation)
	require.Error(t, err)
	_, err = diagnostics.MaintenanceFailure(MaintenanceReleaseFailure{}, observation)
	require.Error(t, err)
	capture, err := diagnostics.CaptureMaintenance(execution.subject, observation)
	require.NoError(t, err)
	sameStore, err := NewFailureDiagnostics(diagnostics.store, foreign.operations, foreign.maintenance)
	require.NoError(t, err)
	require.Error(t, sameStore.RetireHistoricalCapture(capture), "a shared diagnostics file does not bind unrelated journal pairs")
	require.Error(t, (FailureDiagnosticPublication{}).Publish(0))
	_, _, err = (FailureDiagnosticCapture{}).Snapshot()
	require.Error(t, err)
}

func TestFailureDiagnosticsOlderCaptureCannotReplaceNewerSameLifecycleFailure(t *testing.T) {
	fixture, diagnostics, execution := newMaintenanceDiagnosticsFixture(t, "same-lifecycle")
	original, err := diagnostics.CaptureMaintenance(execution.subject, FailureDiagnosticObservation{
		Error: "old failure", Message: "old startup failed", Logs: map[string]string{"web/0": "old startup output"}, Status: DiagnosticCaptureComplete,
	})
	require.NoError(t, err)
	failed, ok := fixture.settlement.ExecuteMaintenance(t.Context(), execution).(MaintenanceExecutionFailure)
	require.True(t, ok)
	proof, err := fixture.settlement.FailMaintenance(failed, backend.ReasonRestartFailed, "old failed")
	require.NoError(t, err)
	oldPublication, err := diagnostics.MaintenanceFailure(proof, FailureDiagnosticObservation{Status: DiagnosticCaptureUnavailable})
	require.NoError(t, err)
	require.NoError(t, oldPublication.Publish(1))
	completion, err := resolveMaintenanceFailureForTest(fixture.settlement, proof, "old failed")
	require.NoError(t, err)
	require.NoError(t, fixture.stores.callbacks.removeEntry(completion))
	source, sourceClaim, err := fixture.settlement.ClaimLatestActive(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	target := cloneRelease(source)
	target.Version = 0
	target.Status = "deploying"
	candidate := maintenanceCandidateForSettlement(t, fixture.settlement, sourceClaim, target, newTestMaintenanceID(t))
	admission, err := fixture.settlement.BeginMaintenanceIntent(candidate)
	require.NoError(t, err)
	appendClaim, err := fixture.settlement.StartMaintenanceAppend(createdMaintenanceDispatch(t, admission))
	require.NoError(t, err)
	targetClaim, err := fixture.settlement.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	targetClaim, err = fixture.settlement.BindMaintenanceIntentTarget(targetClaim)
	require.NoError(t, err)
	refused, err := fixture.settlement.RefuseMaintenanceExecution(targetClaim)
	require.NoError(t, err)
	newer, err := fixture.settlement.FailMaintenance(refused, backend.ReasonRestartFailed, "newer failed")
	require.NoError(t, err)
	publication, err := diagnostics.MaintenanceFailure(newer, FailureDiagnosticObservation{
		Error: "new failure", Message: "new startup failed", Logs: map[string]string{"web/0": "new startup output"}, Status: DiagnosticCaptureComplete,
	})
	require.NoError(t, err)
	oldSnapshot, _, err := original.Snapshot()
	require.NoError(t, err)
	newSnapshot, _, err := publication.Snapshot()
	require.NoError(t, err)
	require.Equal(t, oldSnapshot.LifecycleGeneration, newSnapshot.LifecycleGeneration, "maintenance shares the originating runtime generation")
	require.NoError(t, publication.Publish(1))
	// A late old target may add privately retained evidence. It cannot regain
	// publication authority even when the two attempts share lifecycle labels.
	_, err = diagnostics.CaptureMaintenance(execution.subject, FailureDiagnosticObservation{
		Logs: map[string]string{"late/0": "late old target"}, Status: DiagnosticCaptureComplete,
	})
	require.NoError(t, err)
	require.Error(t, oldPublication.Publish(99))
	entry, err := diagnostics.store.Get(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	require.Equal(t, "new startup output", entry.Logs["web/0"])
	require.NotContains(t, entry.Logs, "late/0")
	require.Equal(t, 2, entry.FailCount)
	require.NoError(t, publication.Publish(1))
	replay, err := diagnostics.store.Get(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	require.Equal(t, 2, replay.FailCount, "publication retry cannot count a new failure")
}

func TestFailureDiagnosticsTryPublicationDoesNotWaitForBusyLease(t *testing.T) {
	fixture, diagnostics, execution := newMaintenanceDiagnosticsFixture(t, "busy-publication")
	failed, ok := fixture.settlement.ExecuteMaintenance(t.Context(), execution).(MaintenanceExecutionFailure)
	require.True(t, ok)
	proof, err := fixture.settlement.FailMaintenance(failed, backend.ReasonRestartFailed, "failed")
	require.NoError(t, err)
	unlock := fixture.settlement.lockLease(fixture.intent.LeaseUUID())
	_, acquired, err := diagnostics.TryPublishMaintenanceFailure(proof, FailureDiagnosticObservation{Status: DiagnosticCaptureUnavailable}, 1)
	unlock()
	require.NoError(t, err)
	require.False(t, acquired)
	entry, err := diagnostics.store.Get(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	require.Nil(t, entry)
	_, acquired, err = diagnostics.TryPublishMaintenanceFailure(proof, FailureDiagnosticObservation{Status: DiagnosticCaptureUnavailable}, 1)
	require.NoError(t, err)
	require.True(t, acquired)
}
