package shared

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloseDiagnosticsKeepExactInterruptedAttemptThroughCleanup(t *testing.T) {
	for _, kind := range []string{"operation", "maintenance"} {
		t.Run(kind, func(t *testing.T) {
			var stores operationHandoffStores
			var maintenance *MaintenanceSettlement
			var operation OperationIntentClaim
			var interrupted MaintenanceIntentClaim
			var closeClaim CloseIntentClaim
			var settlement *CloseSettlement
			if kind == "maintenance" {
				fixture := beginBoundMaintenance(t, "close-diagnostics")
				stores, maintenance = fixture.stores, fixture.settlement
				target := fixture.appendAndBind(t)
				bindTestMaintenanceMutation(t, maintenance, nil)
				_, err := maintenance.StartMaintenanceExecution(target)
				require.NoError(t, err)
				interrupted = target.Intent()
				settlement = newCloseSettlementForTest(t, stores)
				request, err := settlement.NewCloseRequest(interrupted.LeaseUUID(), false)
				require.NoError(t, err)
				admitted, err := settlement.BeginClose(request)
				require.NoError(t, err)
				closeClaim = admitted.Claim()
				require.Equal(t, interrupted.MaintenanceID(), closeClaim.InterruptedMaintenanceID())
				require.True(t, closeClaim.InterruptedOperationID().IsZero())
			} else {
				stores = openOperationHandoffStores(t, "docker-a")
				var err error
				maintenance, err = NewMaintenanceSettlement(stores.callbacks, stores.releases)
				require.NoError(t, err)
				operation = beginHandoffOperation(t, stores.settlement, testOperationIntentSpec(t, "close-diagnostic-operation"))
				settlement = newCloseSettlementForTest(t, stores)
				request, err := settlement.NewCleanupCloseRequest(operation.LeaseUUID())
				require.NoError(t, err)
				admitted, err := settlement.BeginCleanupClose(request)
				require.NoError(t, err)
				closeClaim = admitted.Claim()
				require.Equal(t, operation.OperationID(), closeClaim.InterruptedOperationID())
				require.True(t, closeClaim.InterruptedMaintenanceID().IsZero())
			}
			path := filepath.Join(t.TempDir(), "diagnostics.db")
			store, err := NewDiagnosticsStore(DiagnosticsStoreConfig{DBPath: path})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			diagnostics, err := NewFailureDiagnostics(store, stores.settlement, maintenance)
			require.NoError(t, err)
			bindTestCloseMutation(t, settlement, nil, nil)
			execution := startTestCloseExecution(t, settlement, closeClaim)
			attempt, present, err := diagnostics.CloseAttempt(execution.subject)
			require.NoError(t, err)
			require.True(t, present)
			capture, err := attempt.Capture(FailureDiagnosticObservation{Logs: map[string]string{"web/0": "fatal before close"}, Status: DiagnosticCaptureComplete})
			require.NoError(t, err)
			require.True(t, capture.Valid())
			entry, err := store.Get(closeClaim.LeaseUUID())
			require.NoError(t, err)
			require.Nil(t, entry, "close capture cannot publish an unresolved attempt")
			// A crash after capture leaves only durable bytes and the current close
			// head. Fresh store-bound authority can publish after exact terminal proof.
			require.NoError(t, store.Close())
			reopened, err := NewDiagnosticsStore(DiagnosticsStoreConfig{DBPath: path})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			diagnostics, err = NewFailureDiagnostics(reopened, stores.settlement, maintenance)
			require.NoError(t, err)
			terminal, ok := settlement.ExecuteClose(context.Background(), execution).(CloseExecutionDestroyed)
			require.True(t, ok)
			require.NoError(t, diagnostics.PublishCloseFailure(terminal))
			_, err = settlement.CompleteClose(terminal)
			require.NoError(t, err)
			entry, err = reopened.Get(closeClaim.LeaseUUID())
			require.NoError(t, err)
			require.Equal(t, "fatal before close", entry.Logs["web/0"])
			require.NotEmpty(t, entry.Tenant, "orphan close derives diagnostic principal only from the exact interrupted receipt")
			require.Error(t, diagnostics.PublishCloseFailure(terminal), "completed close cannot replay a stale terminal publication")
		})
	}
}

func TestCloseDiagnosticsRejectMixedInterruptedAuthority(t *testing.T) {
	fixture := beginBoundMaintenance(t, "mixed-close-diagnostics")
	settlement := newCloseSettlementForTest(t, fixture.stores)
	request, err := settlement.NewCloseRequest(fixture.intent.LeaseUUID(), false)
	require.NoError(t, err)
	admission, err := settlement.BeginClose(request)
	require.NoError(t, err)
	entry := admission.Claim().entry
	operation := testOperationIntentSpec(t, "mixed-close-operation")
	entry.InterruptedOperationID, err = parseOperationCallbackID(operation.CallbackURL)
	require.NoError(t, err)
	require.Error(t, validateCloseIntentEntry(entry, entry.LeaseUUID))
}
