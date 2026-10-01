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

func TestUpdateCompletionRejectsForeignAuthority(t *testing.T) {
	for _, scenario := range []string{"storage", "lifecycle", "legacy route", "lease", "proof boundary"} {
		t.Run(scenario, func(t *testing.T) {
			base, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), &executionTestBackend{name: "backend-a"})
			payloads := &maintenanceProgressPayloads{bytes: []byte("last committed")}
			authority, err := base.coordinator.execution.MaintenanceCoordinator(payloads)
			require.NoError(t, err)
			application, err := authority.Application(nil, 0)
			require.NoError(t, err)
			id := mustMaintenanceID(t, maintenanceIDA)
			request, err := NewMaintenanceApplicationRequest(id, maintenanceLease, "tenant-test", MaintenanceCommandUpdate, []byte("candidate"))
			require.NoError(t, err)
			require.Equal(t, MaintenanceApplicationAccepted, application.Execute(t.Context(), request).Outcome())
			record, found, err := store.LookupMaintenanceCommand(maintenanceLease, id)
			require.NoError(t, err)
			require.True(t, found)
			command := record.Command()
			route, err := url.Parse(command.CallbackURL())
			require.NoError(t, err)
			payload := backend.CallbackPayload{
				LeaseUUID: maintenanceLease, Status: backend.CallbackStatusSuccess,
				BackendStorageID: command.BackendStorageID().String(), MaintenanceID: id.String(),
			}
			verifier, consumer := hmacauth.NewCallbackProofBoundary()
			callbacks, err := base.coordinator.execution.AuthenticatedCallbackCoordinator(consumer)
			require.NoError(t, err)
			switch scenario {
			case "storage":
				payload.BackendStorageID = testBackendStorageID("backend-b").String()
			case "lifecycle":
				query := route.Query()
				query.Set("lifecycle_id", maintenanceIDB)
				route.RawQuery = query.Encode()
			case "legacy route":
				route.RawQuery = ""
			case "lease":
				payload.LeaseUUID = maintenanceIDB
			case "proof boundary":
				verifier, _ = hmacauth.NewCallbackProofBoundary()
			}
			body, err := json.Marshal(payload)
			require.NoError(t, err)
			now := time.Now()
			const secret = "maintenance-adversarial-callback-secret"
			proof, err := verifier.VerifyRoutedWithTime(secret, http.MethodPost, route.RequestURI(), body,
				hmacauth.SignWithTime(secret, http.MethodPost, route.RequestURI(), body, now),
				payload.BackendStorageID, route.Path, time.Minute, time.Minute, now)
			require.NoError(t, err)
			_, applyErr := callbacks.Apply(t.Context(), proof)
			if scenario == "storage" {
				require.ErrorIs(t, applyErr, ErrCallbackStorageIdentityMismatch)
			} else if scenario == "proof boundary" {
				require.ErrorIs(t, applyErr, ErrCallbackProofBoundaryMismatch)
			} else {
				require.NoError(t, applyErr, "stale lifecycle callbacks should be dropped")
			}
			require.NoError(t, application.RecoverPending(t.Context()))
			require.Zero(t, payloads.writes)
			require.Equal(t, []byte("last committed"), payloads.bytes)
			require.Contains(t, base.coordinator.RuntimeController().PendingLeaseUUIDs(), maintenanceLease)
			// Rejected authority must not consume the real command's completion.
			require.NoError(t, applyMaintenanceCompletionForTest(t, authority, id, backend.CallbackStatusSuccess))
			require.NoError(t, application.RecoverPending(t.Context()))
			require.Equal(t, 1, payloads.writes)
			require.Equal(t, []byte("candidate"), payloads.bytes)
		})
	}
}

func TestUpdateCompletionCannotReverseConfirmedSuccess(t *testing.T) {
	base, _ := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), &executionTestBackend{name: "backend-a"})
	payloads := &maintenanceProgressPayloads{bytes: []byte("last committed")}
	authority, err := base.coordinator.execution.MaintenanceCoordinator(payloads)
	require.NoError(t, err)
	application, err := authority.Application(nil, 0)
	require.NoError(t, err)
	id := mustMaintenanceID(t, maintenanceIDA)
	request, err := NewMaintenanceApplicationRequest(id, maintenanceLease, "tenant-test", MaintenanceCommandUpdate, []byte("candidate"))
	require.NoError(t, err)
	require.Equal(t, MaintenanceApplicationAccepted, application.Execute(t.Context(), request).Outcome())
	require.NoError(t, applyMaintenanceCompletionForTest(t, authority, id, backend.CallbackStatusSuccess))
	require.ErrorContains(t, applyMaintenanceCompletionForTest(t, authority, id, backend.CallbackStatusFailed), "contradicts durable success")
	require.Zero(t, payloads.writes)
	// A separate runtime failure after activation does not reverse the exact
	// successful update receipt or keep the previous manifest authoritative.
	require.NoError(t, applyMaintenanceCompletionForTest(t, authority, maintenanceid.ID{}, backend.CallbackStatusFailed))
	require.NoError(t, application.RecoverPending(t.Context()))
	require.Equal(t, 1, payloads.writes)
	require.Equal(t, []byte("candidate"), payloads.bytes)
}

func TestUpdateCompletionDuringPendingHTTPAcceptance(t *testing.T) {
	for _, status := range []backend.CallbackStatus{backend.CallbackStatusSuccess, backend.CallbackStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			dispatched, respond := make(chan struct{}), make(chan struct{})
			client := &executionTestBackend{name: "backend-a", update: func(ctx context.Context, _ backend.UpdateRequest) error {
				close(dispatched)
				select {
				case <-respond:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}
			base, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), client)
			payloads := &maintenanceProgressPayloads{bytes: []byte("last committed")}
			authority, err := base.coordinator.execution.MaintenanceCoordinator(payloads)
			require.NoError(t, err)
			application, err := authority.Application(nil, 0)
			require.NoError(t, err)
			id := mustMaintenanceID(t, maintenanceIDA)
			request, err := NewMaintenanceApplicationRequest(id, maintenanceLease, "tenant-test", MaintenanceCommandUpdate, []byte("candidate"))
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan MaintenanceApplicationResult, 1)
			go func() { done <- application.Execute(ctx, request) }()
			select {
			case <-dispatched:
			case <-ctx.Done():
				t.Fatal("maintenance did not dispatch")
			}
			require.NoError(t, applyMaintenanceCompletionForTest(t, authority, id, status))
			require.NoError(t, application.RecoverPending(ctx), "recovery must not wait for live request dispatch lock")
			require.Zero(t, payloads.writes)
			require.Contains(t, base.coordinator.RuntimeController().PendingLeaseUUIDs(), maintenanceLease,
				"even a failed callback cannot release a lane owned by a live dispatch")
			close(respond)
			select {
			case result := <-done:
				if status == backend.CallbackStatusSuccess {
					require.Equal(t, MaintenanceApplicationAccepted, result.Outcome(), result.Err())
					require.Equal(t, 1, payloads.writes)
					require.Equal(t, []byte("candidate"), payloads.bytes)
				} else {
					require.Equal(t, MaintenanceApplicationBackendInvalidState, result.Outcome(), result.Err())
					require.Zero(t, payloads.writes)
					require.Equal(t, []byte("last committed"), payloads.bytes)
				}
			case <-ctx.Done():
				t.Fatal("maintenance did not drain its exact completion")
			}
			record, found, err := store.LookupMaintenanceCommand(maintenanceLease, id)
			require.NoError(t, err)
			require.True(t, found)
			require.NotEqual(t, MaintenanceOutcomePending, record.Outcome())
			require.NotContains(t, base.coordinator.RuntimeController().PendingLeaseUUIDs(), maintenanceLease)
		})
	}
}
