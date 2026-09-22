package shared

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaintenanceCleanupSourceFailureBindsExactCleanupSubject(t *testing.T) {
	fixture := beginBoundMaintenance(t, "cleanup-source-failed")
	target := fixture.appendAndBind(t)
	bindTestMaintenanceMutation(t, fixture.settlement, nil)
	execution, err := fixture.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	source, ok := execution.subject.SourceRelease()
	require.True(t, ok)
	ids, services := testPhysicalProjection(source)
	_, err = NewMaintenanceCleanupSourceFailed(execution.subject, ids, services)
	require.ErrorContains(t, err, "cleanup authority", "ordinary execution cannot turn source unready into terminal proof")
	cleanup := newMaintenancePhysicalSubjectForMode(fixture.settlement, execution.target, execution.subject.state.source, true)
	evidence, err := NewMaintenanceCleanupSourceFailed(cleanup, ids, services)
	require.NoError(t, err)
	require.NoError(t, validateMaintenancePhysicalEvidence(cleanup, evidence))
	require.Error(t, validateMaintenancePhysicalEvidence(execution.subject, evidence), "evidence cannot escape its cleanup subject")
	_, err = NewMaintenanceCleanupSourceFailed(cleanup, nil, nil)
	require.Error(t, err, "complete source failure is distinct from observed absence")
	_, err = NewMaintenanceCleanupSourceFailed(cleanup, ids, map[string][]string{"foreign-service": ids})
	require.Error(t, err, "foreign service topology cannot mint source failure")
}
