package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/maintenanceid"
)

func journalLifecyclePendingError(t *testing.T) error {
	t.Helper()
	return journalPendingError(t, func(operations *shared.OperationSettlement, maintenance *shared.MaintenanceSettlement, candidate shared.OperationReleaseCandidate) error {
		require.NoError(t, shared.BindOperationSubstrateExecutor(operations,
			func(ctx context.Context, _ string) (context.Context, func(), error) { return ctx, func() {}, nil },
			func(context.Context, string, error) error { return nil },
			func(runner substratemutation.Runner, _ shared.OperationPhysicalSubject) substratemutation.Runner {
				return runner
			},
			func(ctx context.Context, runner substratemutation.Runner, _ shared.OperationPhysicalSubject) error {
				return runner.Step(ctx, "materialize test cohort", func(context.Context) error { return nil })
			},
			func(_ context.Context, subject shared.OperationPhysicalSubject) (shared.OperationPhysicalEvidence, error) {
				return shared.NewOperationTargetReady(subject, []string{"app-0"}, map[string][]string{"app": {"app-0"}})
			}))
		execution, err := operations.StartOperationExecution(candidate)
		require.NoError(t, err)
		success, ok := operations.ExecuteOperation(t.Context(), execution).(shared.OperationExecutionSuccess)
		require.True(t, ok)
		_, err = operations.CommitOperationSuccess(success)
		require.NoError(t, err)
		// The active release exists, but its operation still owns the durable
		// head until exact callback settlement. Maintenance cannot overtake it.
		target, source, err := maintenance.ClaimLatestActive(handlerTestLeaseUUID)
		require.NoError(t, err)
		target.Version, target.Status = 0, "deploying"
		identity, ok := target.RuntimeIdentity()
		require.True(t, ok)
		id, err := maintenanceid.New()
		require.NoError(t, err)
		request, err := maintenance.NewMaintenanceRequestAuthority(id, shared.MaintenanceIntentRestart,
			handlerTestLeaseUUID, identity.LifecycleCallbackURL(), nil)
		require.NoError(t, err)
		intent, err := maintenance.NewMaintenanceIntentCandidate(request, source, target)
		require.NoError(t, err)
		_, err = maintenance.BeginMaintenanceIntent(intent)
		require.True(t, shared.IsLifecyclePending(err))
		return err
	})
}

type lifecycleTestIdentity struct{ id backendidentity.ID }

func (r lifecycleTestIdentity) ExpectedBackendStorageIdentity(string) (backendidentity.ID, bool) {
	return r.id, true
}

func TestLifecyclePendingServerAndClientPreserveAmbiguousWork(t *testing.T) {
	pending := journalLifecyclePendingError(t)
	for _, operation := range []string{"restart", "update", "deprovision"} {
		t.Run(operation, func(t *testing.T) {
			var calls atomic.Int32
			result := func() error { calls.Add(1); return pending }
			mock := &mockBackend{
				RestartFunc:               func(context.Context, backend.RestartRequest) error { return result() },
				UpdateFunc:                func(context.Context, backend.UpdateRequest) error { return result() },
				DeprovisionFunc:           func(context.Context, string) error { return result() },
				VerifyStorageIdentityFunc: func(context.Context) error { return nil },
			}
			identity, err := backendidentity.Parse("a8ff9194-0f55-4a31-854e-5f63b236ef3b")
			require.NoError(t, err)
			server, err := NewIdentityBoundServer(mock, testSecret, slog.Default(), docker.DefaultMaxRequestBodySize, identity)
			require.NoError(t, err)
			httpServer := httptest.NewServer(server.Handler())
			defer httpServer.Close()
			policy, err := backend.NewConnectionPolicy(backend.ConnectionConfig{Name: "pending", BaseURL: httpServer.URL, Secret: testSecret})
			require.NoError(t, err)
			client, err := backend.NewIdentityBoundHTTPClient(policy, backend.HTTPClientOptions{CBFailureThresh: 1, CBTimeout: time.Hour}, lifecycleTestIdentity{identity})
			require.NoError(t, err)
			id, err := maintenanceid.Parse(handlerTestMaintenanceID)
			require.NoError(t, err)
			for range 6 {
				var callErr error
				if operation == "deprovision" {
					callErr = client.Deprovision(t.Context(), handlerTestLeaseUUID)
					require.False(t, backend.DeprovisionNotDispatched(client, handlerTestLeaseUUID, callErr))
				} else {
					var outcome backend.MaintenanceCallOutcome
					if operation == "restart" {
						outcome = backend.InvokeRestart(t.Context(), client, backend.RestartRequest{LeaseUUID: handlerTestLeaseUUID, MaintenanceID: id, CallbackURL: "http://localhost/callbacks/provision"})
					} else {
						outcome = backend.InvokeUpdate(t.Context(), client, backend.UpdateRequest{LeaseUUID: handlerTestLeaseUUID, MaintenanceID: id, CallbackURL: "http://localhost/callbacks/provision", Payload: json.RawMessage(`{"services":{"app":{"image":"example.invalid/app:1"}}}`)})
					}
					require.True(t, outcome.Ambiguous(), "unexpected causal outcome: %v", outcome.Err())
					require.False(t, outcome.Refused())
					require.False(t, outcome.NotDispatched())
					callErr = outcome.Err()
				}
				require.Error(t, callErr)
				for _, sentinel := range []error{backend.ErrCircuitOpen, backend.ErrInvalidState, backend.ErrInsufficientResources, backend.ErrCapacityRefused} {
					require.NotErrorIs(t, callErr, sentinel)
				}
			}
			require.EqualValues(t, 6, calls.Load(), "pending observations must not trip the one-failure breaker")
		})
	}
}

func TestLifecyclePendingServerRejectsUnprovenErrors(t *testing.T) {
	for _, failure := range []error{
		errors.New("storage journal corrupt"),
		fmt.Errorf("%w: admitted lifecycle work remains pending", shared.ErrMaintenanceIntentConflict),
		shared.CloseExecutionPending{},
	} {
		mock := &mockBackend{DeprovisionFunc: func(context.Context, string) error { return failure }}
		response := httptest.NewRecorder()
		newMockHandler(mock).ServeHTTP(response, signedPostRequest("/deprovision", fmt.Sprintf(`{"lease_uuid":%q}`, handlerTestLeaseUUID)))
		require.Equal(t, http.StatusInternalServerError, response.Code)
		require.NotContains(t, response.Body.String(), backend.CodeLifecyclePending)
	}
}
