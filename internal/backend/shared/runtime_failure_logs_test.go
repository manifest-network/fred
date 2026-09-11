package shared

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestRuntimeFailureLogsRequireExactCompensatedSourceGeneration(t *testing.T) {
	fixture, diagnostics, execution := newMaintenanceDiagnosticsFixtureWithClassifier(t, "runtime-failure-view", func(subject MaintenancePhysicalSubject) (MaintenancePhysicalEvidence, error) {
		source, ok := subject.SourceRelease()
		require.True(t, ok)
		ids, services := testPhysicalProjection(source)
		return NewMaintenanceSourceReady(subject, ids, services)
	})
	_, err := diagnostics.CaptureMaintenance(execution.subject, FailureDiagnosticObservation{
		Error: "target failed", Message: "target startup failed", Logs: map[string]string{"web/0": "failed target output"}, Status: DiagnosticCaptureComplete,
	})
	require.NoError(t, err)
	failed, ok := fixture.settlement.ExecuteMaintenance(t.Context(), execution).(MaintenanceExecutionFailure)
	require.True(t, ok)
	require.True(t, failed.SourceRecovered())
	proof, err := fixture.settlement.FailMaintenance(failed, backend.ReasonRestartFailed, "target failed")
	require.NoError(t, err)
	publication, err := diagnostics.MaintenanceFailure(proof, FailureDiagnosticObservation{Status: DiagnosticCaptureUnavailable})
	require.NoError(t, err)
	require.NoError(t, publication.Publish(1))
	runtime, err := fixture.stores.releases.ProveRuntimeGeneration(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	view, err := diagnostics.PublishedForRuntime(runtime)
	require.NoError(t, err)
	source, ok := execution.subject.SourceRelease()
	require.True(t, ok)
	authority, ok := source.RuntimeIdentity()
	require.True(t, ok)
	lifecycle := backend.ObserveLifecycleGeneration(authority.CallbackURL(), authority.LifecycleCallbackURL())
	require.True(t, view.MatchesProjection(runtime.Version(), authority.Tenant(), authority.ProviderUUID(), lifecycle))
	require.False(t, view.MatchesProjection(runtime.Version(), "foreign", authority.ProviderUUID(), lifecycle))
	require.Equal(t, "failed target output", view.Logs()["web/0"])
	copied := view.Logs()
	copied["web/0"] = "changed"
	require.Equal(t, "failed target output", view.Logs()["web/0"])
	// Appending a later active release invalidates the old read proof and the
	// persisted source-version match, including shared lifecycle callback IDs.
	source.Version = 0
	require.NoError(t, fixture.stores.releases.appendActive(fixture.intent.LeaseUUID(), source))
	_, err = diagnostics.PublishedForRuntime(runtime)
	require.Error(t, err)
	next, err := fixture.stores.releases.ProveRuntimeGeneration(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	view, err = diagnostics.PublishedForRuntime(next)
	require.NoError(t, err)
	require.Empty(t, view.Logs(), "a later deployment cannot inherit an older failed-attempt view")
}

func TestRuntimeFailureLogsCannotBeForgedByObservationalDiagnosticEntry(t *testing.T) {
	fixture, diagnostics, execution := newMaintenanceDiagnosticsFixture(t, "no-source-proof")
	failed, ok := fixture.settlement.ExecuteMaintenance(t.Context(), execution).(MaintenanceExecutionFailure)
	require.True(t, ok)
	proof, err := fixture.settlement.FailMaintenance(failed, backend.ReasonRestartFailed, "failed")
	require.NoError(t, err)
	publication, err := diagnostics.MaintenanceFailure(proof, FailureDiagnosticObservation{
		Logs: map[string]string{"web/0": "unrecovered failure"}, Status: DiagnosticCaptureComplete,
	})
	require.NoError(t, err)
	require.NoError(t, publication.Publish(1))
	runtime, err := fixture.stores.releases.ProveRuntimeGeneration(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	view, err := diagnostics.PublishedForRuntime(runtime)
	require.NoError(t, err)
	require.Empty(t, view.Logs(), "an active release by itself is not evidence of restored source readiness")
}
