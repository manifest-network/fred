package placement

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/maintenanceid"
)

func applyMaintenanceCompletionForTest(t *testing.T, authority *MaintenanceCoordinator, id maintenanceid.ID, status backend.CallbackStatus) error {
	t.Helper()
	record, found, err := authority.lookupMaintenanceCommand(maintenanceLease, mustMaintenanceID(t, maintenanceIDA))
	require.NoError(t, err)
	require.True(t, found)
	command := record.Command()
	storageID, ok := authority.coordinator.ExpectedBackendStorageIdentity(command.BackendName())
	require.True(t, ok)
	payload := backend.CallbackPayload{LeaseUUID: maintenanceLease, Status: status, BackendStorageID: storageID.String()}
	if id.Valid() {
		payload.MaintenanceID = id.String()
	}
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	callbackURL, err := url.Parse(command.CallbackURL())
	require.NoError(t, err)
	uri := callbackURL.RequestURI()
	verifier, consumer := hmacauth.NewCallbackProofBoundary()
	callbacks, err := authority.coordinator.execution.AuthenticatedCallbackCoordinator(consumer)
	require.NoError(t, err)
	now := time.Now()
	const secret = "maintenance-completion-test-secret-0123456789"
	proof, err := verifier.VerifyRoutedWithTime(secret, http.MethodPost, uri, body,
		hmacauth.SignWithTime(secret, http.MethodPost, uri, body, now), storageID.String(),
		callbackURL.Path, time.Minute, time.Minute, now)
	require.NoError(t, err)
	_, err = callbacks.Apply(t.Context(), proof)
	return err
}

func TestUpdateCommitsOnlyExactSuccessfulCompletion(t *testing.T) {
	for _, early := range []bool{false, true} {
		for _, success := range []bool{false, true} {
			name := map[bool]string{false: "late", true: "early"}[early] + "/" + map[bool]string{false: "failure", true: "success"}[success]
			t.Run(name, func(t *testing.T) {
				var calls int
				var coordinator *MaintenanceCoordinator
				id := mustMaintenanceID(t, maintenanceIDA)
				status := backend.CallbackStatusFailed
				if success {
					status = backend.CallbackStatusSuccess
				}
				client := &executionTestBackend{name: "backend-a", update: func(context.Context, backend.UpdateRequest) error {
					calls++
					if early {
						require.NoError(t, applyMaintenanceCompletionForTest(t, coordinator, id, status))
					}
					return nil
				}}
				base, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), client)
				payloads := &maintenanceProgressPayloads{bytes: []byte("old committed payload")}
				var err error
				coordinator, err = base.coordinator.execution.MaintenanceCoordinator(payloads)
				require.NoError(t, err)
				application, err := coordinator.Application(nil, 0)
				require.NoError(t, err)
				request, err := NewMaintenanceApplicationRequest(id, maintenanceLease, "tenant-test", MaintenanceCommandUpdate, []byte("new candidate payload"))
				require.NoError(t, err)
				result := application.Execute(t.Context(), request)
				if early && !success {
					require.Equal(t, MaintenanceApplicationBackendInvalidState, result.Outcome())
				} else {
					require.Equal(t, MaintenanceApplicationAccepted, result.Outcome(), result.Err())
				}
				if !early {
					require.Zero(t, payloads.writes, "202 acceptance must not overwrite committed payload")
					require.NoError(t, applyMaintenanceCompletionForTest(t, coordinator, maintenanceid.ID{}, backend.CallbackStatusSuccess))
					require.NoError(t, applyMaintenanceCompletionForTest(t, coordinator, mustMaintenanceID(t, maintenanceIDB), backend.CallbackStatusSuccess))
					require.NoError(t, application.RecoverPending(t.Context()))
					require.Zero(t, payloads.writes, "generic or stale lifecycle success cannot confirm update")
					require.NoError(t, applyMaintenanceCompletionForTest(t, coordinator, id, status))
				}
				require.NoError(t, application.RecoverPending(t.Context()))
				require.NoError(t, applyMaintenanceCompletionForTest(t, coordinator, id, status), "terminal callback replay is idempotent")
				record, found, err := store.LookupMaintenanceCommand(maintenanceLease, id)
				require.NoError(t, err)
				require.True(t, found)
				if success {
					require.Equal(t, MaintenanceOutcomeAccepted, record.Outcome())
					require.Equal(t, []byte("new candidate payload"), payloads.bytes)
					require.Equal(t, 1, payloads.writes)
				} else {
					require.Equal(t, MaintenanceOutcomeExecutionFailed, record.Outcome())
					require.Equal(t, []byte("old committed payload"), payloads.bytes)
					require.Zero(t, payloads.writes, "rolled-back update must never become reprovision payload")
				}
				require.NotContains(t, base.coordinator.RuntimeController().PendingLeaseUUIDs(), maintenanceLease)
				require.Equal(t, 1, calls, "accepted or completed update must never dispatch again")
			})
		}
	}
}
