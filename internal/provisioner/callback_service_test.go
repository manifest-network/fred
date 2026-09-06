package provisioner

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

func callbackTestLease(state billingtypes.LeaseState) *billingtypes.Lease {
	return &billingtypes.Lease{
		Uuid:         "11111111-1111-4111-8111-111111111111",
		Tenant:       "tenant-test",
		ProviderUuid: placementstore.ProviderUUID,
		State:        state,
	}
}

type callbackAcknowledgerFunc func(context.Context, string) (bool, string, error)

func (acknowledge callbackAcknowledgerFunc) Acknowledge(
	ctx context.Context,
	leaseUUID string,
) (bool, string, error) {
	return acknowledge(ctx, leaseUUID)
}

type callbackChainStub struct {
	getLease func(context.Context, string) (*billingtypes.Lease, error)
	reject   func(context.Context, []string, string) (uint64, []string, error)
}

type callbackAmbiguousRestoreBackend struct{ backend.Backend }

func (callbackAmbiguousRestoreBackend) Restore(
	context.Context,
	backend.RestoreRequest,
) error {
	return errors.New("test ambiguous restore transport outcome")
}

// callbackAmbiguousProvisionBackend models the only state in which a durable
// attempt remains callback-settleable: the backend call may have reached the
// remote machine, but its transport response was lost.
type callbackAmbiguousProvisionBackend struct{ backend.Backend }

func (client callbackAmbiguousProvisionBackend) Provision(
	ctx context.Context,
	request backend.ProvisionRequest,
) error {
	if err := client.Backend.Provision(ctx, request); err != nil {
		return err
	}
	return errors.New("test ambiguous provision transport outcome")
}

type callbackBlockingDeprovisionBackend struct {
	backend.Backend
	started chan struct{}
	release chan struct{}
}

func (client *callbackBlockingDeprovisionBackend) Deprovision(
	ctx context.Context,
	leaseUUID string,
) error {
	close(client.started)
	select {
	case <-client.release:
		return client.Backend.Deprovision(ctx, leaseUUID)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (chain *callbackChainStub) GetLease(
	ctx context.Context,
	leaseUUID string,
) (*billingtypes.Lease, error) {
	if chain.getLease == nil {
		return nil, nil
	}
	return chain.getLease(ctx, leaseUUID)
}

func (chain *callbackChainStub) RejectLeases(
	ctx context.Context,
	leaseUUIDs []string,
	reason string,
) (uint64, []string, error) {
	if chain.reject == nil {
		return 0, nil, nil
	}
	return chain.reject(ctx, leaseUUIDs, reason)
}

func (*callbackChainStub) GetPendingLeases(
	context.Context,
	string,
) ([]billingtypes.Lease, error) {
	return nil, nil
}

func (*callbackChainStub) GetActiveLeasesByProvider(
	context.Context,
	string,
) ([]billingtypes.Lease, error) {
	return nil, nil
}

func (*callbackChainStub) CloseLeases(
	context.Context,
	[]string,
	string,
) (uint64, []string, error) {
	return 0, nil, nil
}

type callbackEventRecorder struct {
	events []backend.LeaseStatusEvent
	mu     sync.Mutex
}

func (recorder *callbackEventRecorder) PublishCallbackLeaseEvent(
	leaseUUID string,
	status backend.ProvisionStatus,
	errMsg string,
) {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.events = append(recorder.events, backend.LeaseStatusEvent{
		LeaseUUID: leaseUUID,
		Status:    status,
		Error:     errMsg,
	})
}

type callbackPayloadRecorder struct {
	deleted []string
}

func (recorder *callbackPayloadRecorder) Delete(leaseUUID string) {
	recorder.deleted = append(recorder.deleted, leaseUUID)
}

type callbackDeprovisionRecorder struct {
	leaseUUID   string
	backendName string
	calls       int
}

func (recorder *callbackDeprovisionRecorder) ObserveCallbackDeprovisioned(
	leaseUUID, backendName string,
) {
	recorder.leaseUUID = leaseUUID
	recorder.backendName = backendName
	recorder.calls++
}

func trackCallbackOperation(
	t testing.TB,
	leaseUUID, backendName string,
) *callbackOperationToken {
	t.Helper()
	return &callbackOperationToken{leaseUUID: leaseUUID, backendName: backendName}
}

func prepareCallbackOperationForTest(
	t testing.TB,
	store *placement.Store,
	token *callbackOperationToken,
) *placement.OperationCoordinator {
	t.Helper()
	return prepareCallbackOperationWithBackendForTest(
		t, store, token,
		callbackAmbiguousProvisionBackend{Backend: backend.NewMockBackend(
			backend.MockBackendConfig{Name: token.backendName},
		)},
	)
}

func bindCallbackObservationRuntimeForTest(
	t testing.TB,
	coordinator *placement.OperationCoordinator,
	backendName string,
) {
	t.Helper()
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{
		Backend:   backend.NewMockBackend(backend.MockBackendConfig{Name: backendName}),
		IsDefault: true,
	}}})
	require.NoError(t, err)
	bindTestBackendRuntime(t, coordinator, router)
}

func prepareCallbackOperationWithBackendForTest(
	t testing.TB,
	store *placement.Store,
	token *callbackOperationToken,
	client backend.Backend,
) *placement.OperationCoordinator {
	t.Helper()
	require.NotNil(t, token)
	armTestPlacementTopology(t, store, []string{token.backendName})
	coordinator := bindTestOperationCoordinator(t, store)
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{
		Backend:   callbackAmbiguousProvisionBackend{Backend: client},
		IsDefault: true,
	}}})
	require.NoError(t, err)
	execution := bindTestBackendRuntime(t, coordinator, router)
	chain := &callbackChainStub{
		getLease: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: token.leaseUUID, Tenant: "tenant-test",
				ProviderUuid: placementstore.ProviderUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{{
					SkuUuid: "sku-test", Quantity: 1,
				}},
			}, nil
		},
	}
	reconciliation := bindTestReconciliationCoordinator(
		t, store, execution, chain, nil, nil,
	)
	provision, err := execution.ProvisionCoordinator(nil)
	require.NoError(t, err)
	callbackProvisionChains.Store(provision, chain)
	event, err := placement.NewProvisionEventRequest(token.leaseUUID, "tenant-test")
	require.NoError(t, err)
	result := provision.ExecuteCurrentLease(context.Background(), event)
	require.Error(t, result.Err(), "fixture must preserve the write-ahead attempt")
	token.id = store.Lookup(token.leaseUUID).AttemptOperationID()
	require.True(t, token.id.Valid())
	callbackProvisionCoordinators.Store(coordinator, provision)
	callbackOperationCoordinators.Store(provision, coordinator)
	testReconciliationCoordinators.Store(provision, reconciliation)
	return coordinator
}

func callbackProvisionCoordinator(
	t testing.TB,
	store *placement.Store,
	coordinator *placement.OperationCoordinator,
	backendName string,
) *placement.ProvisionCoordinator {
	t.Helper()
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{
		Backend: callbackAmbiguousProvisionBackend{Backend: backend.NewMockBackend(
			backend.MockBackendConfig{Name: backendName},
		)},
		IsDefault: true,
	}}})
	require.NoError(t, err)
	return bindCallbackProvisionCoordinator(t, store, coordinator, router)
}

var callbackProvisionCoordinators sync.Map
var callbackProvisionChains sync.Map
var callbackOperationCoordinators sync.Map

func bindCallbackProvisionCoordinator(
	t testing.TB,
	store *placement.Store,
	coordinator *placement.OperationCoordinator,
	router *backend.Router,
) *placement.ProvisionCoordinator {
	t.Helper()
	if cached, ok := callbackProvisionCoordinators.Load(coordinator); ok {
		return cached.(*placement.ProvisionCoordinator)
	}
	execution := bindTestBackendRuntime(t, coordinator, router)
	chain := &callbackChainStub{}
	provision, err := execution.ProvisionCoordinator(nil)
	require.NoError(t, err)
	reconciliation := bindTestReconciliationCoordinator(
		t, store, execution, chain, nil, nil,
	)
	callbackProvisionCoordinators.Store(coordinator, provision)
	callbackProvisionChains.Store(provision, chain)
	callbackOperationCoordinators.Store(provision, coordinator)
	testReconciliationCoordinators.Store(provision, reconciliation)
	return provision
}

func confirmCallbackAttemptForTest(
	t testing.TB,
	coordinator *placement.OperationCoordinator,
	leaseUUID string,
	id operation.OperationID,
) {
	t.Helper()
	authority, err := authenticatedCallbackCoordinatorForTest(
		coordinator,
		&callbackChainStub{getLease: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: placementstore.ProviderUUID,
				State: billingtypes.LEASE_STATE_PENDING,
			}, nil
		}},
		callbackAcknowledgerFunc(func(context.Context, string) (bool, string, error) {
			return true, "tx-ack", nil
		}),
	)
	require.NoError(t, err)
	_, err = authority.Apply(context.Background(), callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   leaseUUID,
		Status:      backend.CallbackStatusSuccess,
		OperationID: id.String(),
	}))
	require.NoError(t, err)
}

type callbackOperationToken struct {
	id          operation.OperationID
	leaseUUID   string
	backendName string
}

func (token callbackOperationToken) ID() operation.OperationID { return token.id }

func callbackWireID(t testing.TB, id operation.OperationID) string {
	t.Helper()
	require.True(t, id.Valid())
	return id.String()
}

func callbackBackendStorageID(t testing.TB, value string) backendidentity.ID {
	t.Helper()
	id, err := backendidentity.Parse(value)
	require.NoError(t, err)
	return id
}

func projectCallbackLifecycleGeneration(
	t testing.TB,
	provision *placement.ProvisionCoordinator,
	leaseUUID, backendName string,
	generation lifecycle.ID,
) {
	t.Helper()
	value, ok := testReconciliationCoordinators.Load(provision)
	require.True(t, ok)
	reconciliation := value.(*placement.ReconciliationCoordinator)
	sweep, err := reconciliation.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	disposition := collectTestBackendInventory(
		t, reconciliation, sweep, backendName,
		testBackendStorageID(backendName), []backend.ProvisionInfo{{
			LeaseUUID: leaseUUID, BackendName: backendName,
			ProviderUUID: testProviderUUID, Tenant: "tenant-test",
			LifecycleGeneration: &backend.LifecycleGenerationObservation{
				Kind: backend.LifecycleGenerationTyped,
				ID:   generation.String(),
			},
		}}, nil,
	)
	require.Equal(t, placement.BackendInventoryAuthoritative, disposition)
	require.NoError(t, sweep.SealInventory())
	_, err = sweep.Project(placement.ReconciliationProjection{
		Placements: map[string]string{leaseUUID: backendName},
	})
	require.NoError(t, err)
}

func callbackCommand(t testing.TB, callback backend.CallbackPayload) hmacauth.VerifiedRequest {
	t.Helper()
	if callback.BackendStorageID == "" {
		callback.BackendStorageID = testBackendStorageID("backend-a").String()
	}
	return callbackCommandRaw(t, callback)
}

func callbackCommandRaw(t testing.TB, callback backend.CallbackPayload) hmacauth.VerifiedRequest {
	t.Helper()
	command, err := callbackProofForTest(callback)
	require.NoError(t, err)
	return command
}

func TestNewCallbackService_RequiresCompleteAuthority(t *testing.T) {
	_, err := NewCallbackService(CallbackServiceConfig{})
	require.ErrorIs(t, err, errCallbackOperationsUnavailable)

	var typedNil *placement.AuthenticatedCallbackCoordinator
	_, err = NewCallbackService(CallbackServiceConfig{Coordinator: typedNil})
	require.ErrorIs(t, err, errCallbackOperationsUnavailable)

	store := newTestPlacementAuthority(t)
	coordinator, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{
		Backend:   backend.NewMockBackend(backend.MockBackendConfig{Name: "backend-a"}),
		IsDefault: true,
	}}})
	require.NoError(t, err)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	bindTestBackendRuntime(t, coordinator, router)
	chain := &callbackChainStub{}
	acknowledger := callbackAcknowledgerFunc(func(
		context.Context, string,
	) (bool, string, error) {
		return true, "", nil
	})
	callbackCoordinator, err := authenticatedCallbackCoordinatorForTest(
		coordinator, chain, acknowledger,
	)
	require.NoError(t, err)
	valid := CallbackServiceConfig{
		Coordinator: callbackCoordinator,
		Payloads:    (*typedNilCallbackPayloadStore)(nil),
	}
	service, err := NewCallbackService(valid)
	require.NoError(t, err)
	assert.Nil(t, service.payloads, "typed-nil optional capabilities must be normalized")
	require.ErrorIs(t,
		service.HandleCallback(context.Background(), hmacauth.VerifiedRequest{}),
		placement.ErrCallbackProofBoundaryMismatch,
		"the zero command must never authorize a registry lookup or mutation",
	)
}

func TestCallbackServiceRejectsMissingOrMismatchedStorageIdentityBeforeSettlement(t *testing.T) {
	t.Parallel()

	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	token := trackCallbackOperation(t, leaseUUID, "docker-a")
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"docker-a"})
	coordinator := prepareCallbackOperationForTest(t, store, token)
	wrong := callbackBackendStorageID(t, "550e8400-e29b-41d4-a716-446655440000")
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
	})
	require.NoError(t, err)

	for _, test := range []struct {
		name      string
		storageID string
		want      error
	}{
		{name: "missing", want: errCallbackStorageIdentityMissing},
		{name: "mismatch", storageID: wrong.String(), want: errCallbackStorageIdentityMismatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := callbackCommandRaw(t, backend.CallbackPayload{
				LeaseUUID:        leaseUUID,
				Status:           backend.CallbackStatusSuccess,
				OperationID:      callbackWireID(t, token.ID()),
				BackendStorageID: test.storageID,
			})
			err := service.HandleCallback(context.Background(), command)
			assert.ErrorIs(t, err, test.want)
			assert.False(t, coordinator.RuntimeController().Contains(leaseUUID),
				"an ambiguous dispatch retains only durable recovery authority")
			assert.Equal(t, token.ID(), store.Lookup(leaseUUID).AttemptOperationID())
		})
	}
}

type typedNilCallbackPayloadStore struct{}

func (*typedNilCallbackPayloadStore) Delete(string) {}

func TestCallbackService_AuthorizesOnlyMatchingOperation(t *testing.T) {
	tests := []struct {
		name        string
		callbackID  func(testing.TB, operation.OperationID) string
		backend     string
		wantApplied bool
	}{
		{
			name:       "missing token is structurally rejected",
			callbackID: func(testing.TB, operation.OperationID) string { return "" },
		},
		{
			name:        "exact token is accepted",
			callbackID:  callbackWireID,
			backend:     "backend-a",
			wantApplied: true,
		},
		{
			name: "different nonzero token is rejected",
			callbackID: func(t testing.TB, id operation.OperationID) string {
				t.Helper()
				return "d9428888-122b-41e1-b85c-61c67afba0c6"
			},
			backend: "backend-a",
		},
		{
			name:        "legacy metrics backend cannot redirect exact token",
			callbackID:  callbackWireID,
			backend:     "backend-b",
			wantApplied: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token := trackCallbackOperation(t, "11111111-1111-4111-8111-111111111111", "backend-a")
			store := newTestPlacementAuthority(t)
			armTestPlacementTopology(t, store, []string{"backend-a"})
			coordinator := prepareCallbackOperationForTest(t, store, token)
			var acknowledgeCalls atomic.Int32
			service, err := newCallbackServiceForTest(callbackServiceTestConfig{
				Coordinator: coordinator,
				Acknowledger: callbackAcknowledgerFunc(func(
					context.Context, string,
				) (bool, string, error) {
					acknowledgeCalls.Add(1)
					return true, "tx", nil
				}),
			})
			require.NoError(t, err)

			err = service.HandleCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
				LeaseUUID:   "11111111-1111-4111-8111-111111111111",
				Backend:     tt.backend,
				Status:      backend.CallbackStatusSuccess,
				OperationID: tt.callbackID(t, token.ID()),
			}))
			require.NoError(t, err)
			if tt.wantApplied {
				assert.Equal(t, placement.StateConfirmed, store.Lookup("11111111-1111-4111-8111-111111111111").State())
				assert.Equal(t, int32(1), acknowledgeCalls.Load())
				assert.False(t, coordinator.RuntimeController().Contains("11111111-1111-4111-8111-111111111111"))
				return
			}
			assert.Equal(t, placement.StateAttempting, store.Lookup("11111111-1111-4111-8111-111111111111").State())
			assert.Zero(t, acknowledgeCalls.Load())
			assert.Equal(t, token.ID(), store.Lookup("11111111-1111-4111-8111-111111111111").AttemptOperationID(),
				"an unauthorized callback must preserve durable operation authority")
		})
	}
}

func TestCallbackService_EventSinkPanicDoesNotRetryOrWedgeLaterCallback(t *testing.T) {
	const leaseUUID = "11111111-1111-4111-8111-111111111111"

	token := trackCallbackOperation(t, leaseUUID, "backend-a")
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	coordinator := prepareCallbackOperationForTest(t, store, token)

	var publishCalls int
	var observed []backend.LeaseStatusEvent
	events := callbackEventSinkFunc(func(
		leaseUUID string,
		status backend.ProvisionStatus,
		errMsg string,
	) {
		publishCalls++
		if publishCalls == 1 {
			panic("subscriber failure")
		}
		observed = append(observed, backend.LeaseStatusEvent{
			LeaseUUID: leaseUUID,
			Status:    status,
			Error:     errMsg,
		})
	})
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			return true, "tx-ack", nil
		}),
		Events: events,
	})
	require.NoError(t, err)

	before := promtestutil.ToFloat64(metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(
		metrics.LifecycleEventCallback,
	))
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   leaseUUID,
			Status:      backend.CallbackStatusSuccess,
			OperationID: token.ID().String(),
		},
	)))
	assert.False(t, coordinator.RuntimeController().Contains(leaseUUID),
		"an observational sink panic must not turn completed settlement into a retry")
	assert.Equal(t, placement.StateConfirmed, store.Lookup(leaseUUID).State())
	assert.Equal(t, before+1, promtestutil.ToFloat64(
		metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(metrics.LifecycleEventCallback),
	))

	lifecycleID, err := lifecycle.FromOperationID(token.ID())
	require.NoError(t, err)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   leaseUUID,
			Status:      backend.CallbackStatusFailed,
			Error:       "runtime failure",
			LifecycleID: lifecycleID.String(),
		},
	)))
	require.Len(t, observed, 1,
		"the callback after the panicking delivery must still reach the per-lease sink")
	assert.Equal(t, backend.ProvisionStatusFailed, observed[0].Status)
	assert.Equal(t, "runtime failure", observed[0].Error)
}

func TestCallbackService_SuccessSettlesExactDurableAttempt(t *testing.T) {
	token := trackCallbackOperation(t, "11111111-1111-4111-8111-111111111111", "backend-a")
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	coordinator := prepareCallbackOperationForTest(t, store, token)

	var acknowledgeCalls atomic.Int32
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			acknowledgeCalls.Add(1)
			return true, "tx", nil
		}),
	})
	require.NoError(t, err)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "11111111-1111-4111-8111-111111111111",
		Backend:     "backend-a",
		Status:      backend.CallbackStatusSuccess,
		OperationID: callbackWireID(t, token.ID()),
	})))

	assert.Equal(t, int32(1), acknowledgeCalls.Load())
	assert.False(t, coordinator.RuntimeController().Contains("11111111-1111-4111-8111-111111111111"))
	confirmed := store.Lookup("11111111-1111-4111-8111-111111111111")
	assert.Equal(t, placement.StateConfirmed, confirmed.State())
	assert.Equal(t, "backend-a", confirmed.Backend)
	assert.Empty(t, confirmed.Attempt)
	assert.False(t, confirmed.AttemptOperationID().Valid())
}

func TestCallbackService_SuccessRecoversAfterRealPlacementWriteFailure(t *testing.T) {
	const leaseUUID = "22222222-2222-4222-8222-222222222222"
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store, err := placementstore.NewStore(dbPath)
	require.NoError(t, err)
	token := trackCallbackOperation(t, leaseUUID, "backend-a")
	armTestPlacementTopology(t, store, []string{"backend-a"})
	coordinator := prepareCallbackOperationForTest(t, store, token)

	var acknowledgeCalls atomic.Int32
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
		Acknowledger: callbackAcknowledgerFunc(func(context.Context, string) (bool, string, error) {
			acknowledgeCalls.Add(1)
			require.NoError(t, store.Close())
			return true, "tx-ack", nil
		}),
	})
	require.NoError(t, err)
	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID: leaseUUID, Status: backend.CallbackStatusSuccess,
		OperationID: token.ID().String(),
	})
	require.Error(t, service.HandleCallback(context.Background(), command))
	assert.Equal(t, int32(1), acknowledgeCalls.Load())
	// The durable attempt, not process-local Registry state, is the recovery
	// authority. Reopening the Store below proves the failed write preserved it.

	reopened, err := placementstore.NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	recovery := bindTestOperationCoordinator(t, reopened)
	bindCallbackObservationRuntimeForTest(t, recovery, "backend-a")
	recoveryService, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: recovery,
		Acknowledger: callbackAcknowledgerFunc(func(context.Context, string) (bool, string, error) {
			acknowledgeCalls.Add(1)
			return true, "tx-ack-retry", nil
		}),
	})
	require.NoError(t, err)
	require.NoError(t, recoveryService.HandleCallback(context.Background(), command))
	assert.Equal(t, int32(2), acknowledgeCalls.Load())
	assert.Equal(t, placement.StateConfirmed, reopened.Lookup(leaseUUID).State())
}

func TestCallbackService_FailureRecoversAfterRealPlacementWriteFailure(t *testing.T) {
	const leaseUUID = "33333333-3333-4333-8333-333333333333"
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store, err := placementstore.NewStore(dbPath)
	require.NoError(t, err)
	token := trackCallbackOperation(t, leaseUUID, "backend-a")
	armTestPlacementTopology(t, store, []string{"backend-a"})
	coordinator := prepareCallbackOperationForTest(t, store, token)

	var rejectCalls atomic.Int32
	chainState := billingtypes.LEASE_STATE_PENDING
	chain := &callbackChainStub{
		getLease: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: leaseUUID, Tenant: "tenant-test",
				ProviderUuid: placementstore.ProviderUUID, State: chainState,
			}, nil
		},
		reject: func(context.Context, []string, string) (uint64, []string, error) {
			rejectCalls.Add(1)
			chainState = billingtypes.LEASE_STATE_REJECTED
			require.NoError(t, store.Close())
			return 1, []string{"tx-reject"}, nil
		},
	}
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
		Chain:       chain,
	})
	require.NoError(t, err)
	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID: leaseUUID, Status: backend.CallbackStatusFailed,
		OperationID: token.ID().String(), Error: "backend refused provision",
	})
	require.Error(t, service.HandleCallback(context.Background(), command))
	assert.Equal(t, int32(1), rejectCalls.Load())

	reopened, err := placementstore.NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	recovery := bindTestOperationCoordinator(t, reopened)
	bindCallbackObservationRuntimeForTest(t, recovery, "backend-a")
	recoveryService, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: recovery,
		Chain:       chain,
	})
	require.NoError(t, err)
	require.NoError(t, recoveryService.HandleCallback(context.Background(), command))
	assert.Equal(t, int32(1), rejectCalls.Load(),
		"durable recovery must observe terminal chain state instead of rejecting again")
	assert.Equal(t, placement.StateAbsent, reopened.Lookup(leaseUUID).State())
}

func TestCallbackService_RetryableAcknowledgeFailureReleasesExactClaim(t *testing.T) {
	token := trackCallbackOperation(t, "11111111-1111-4111-8111-111111111111", "backend-a")
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	coordinator := prepareCallbackOperationForTest(t, store, token)

	var calls atomic.Int32
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			if calls.Add(1) == 1 {
				return false, "", errors.New("chain unavailable")
			}
			return true, "tx", nil
		}),
	})
	require.NoError(t, err)
	callback := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "11111111-1111-4111-8111-111111111111",
		Backend:     "backend-a",
		Status:      backend.CallbackStatusSuccess,
		OperationID: callbackWireID(t, token.ID()),
	})

	err = service.HandleCallback(context.Background(), callback)
	require.ErrorIs(t, err, ErrAcknowledgeFailed)
	assert.Equal(t, token.ID(), store.Lookup("11111111-1111-4111-8111-111111111111").AttemptOperationID(),
		"transient chain failure must preserve durable operation authority")

	require.NoError(t, service.HandleCallback(context.Background(), callback))
	assert.Equal(t, int32(2), calls.Load())
	assert.False(t, coordinator.RuntimeController().Contains("11111111-1111-4111-8111-111111111111"))
}

func TestCallbackService_AcknowledgeErrorUsesCurrentLeaseState(t *testing.T) {
	readFailure := errors.New("chain read failed")
	tests := []struct {
		name         string
		lease        *billingtypes.Lease
		leaseErr     error
		wantRetry    bool
		wantReady    bool
		wantFinished bool
	}{
		{
			name:         "active lease publishes ready",
			lease:        callbackTestLease(billingtypes.LEASE_STATE_ACTIVE),
			wantReady:    true,
			wantFinished: true,
		},
		{
			name:         "close wins race with success callback",
			lease:        callbackTestLease(billingtypes.LEASE_STATE_CLOSED),
			wantFinished: true,
		},
		{
			name:      "missing lease remains retryable",
			wantRetry: true,
		},
		{
			name:      "pending lease remains retryable",
			lease:     callbackTestLease(billingtypes.LEASE_STATE_PENDING),
			wantRetry: true,
		},
		{
			name:      "unknown lease state remains retryable",
			lease:     callbackTestLease(billingtypes.LEASE_STATE_UNSPECIFIED),
			wantRetry: true,
		},
		{
			name:      "failed exact read remains retryable",
			leaseErr:  readFailure,
			wantRetry: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token := trackCallbackOperation(t, "11111111-1111-4111-8111-111111111111", "backend-a")
			store := newTestPlacementAuthority(t)
			armTestPlacementTopology(t, store, []string{"backend-a"})
			coordinator := prepareCallbackOperationForTest(t, store, token)
			events := &callbackEventRecorder{}
			var reads atomic.Int32
			service, err := newCallbackServiceForTest(callbackServiceTestConfig{
				Coordinator: coordinator,
				Chain: &callbackChainStub{getLease: func(
					context.Context, string,
				) (*billingtypes.Lease, error) {
					reads.Add(1)
					return tt.lease, tt.leaseErr
				}},
				Acknowledger: callbackAcknowledgerFunc(func(
					context.Context, string,
				) (bool, string, error) {
					return false, "", billingtypes.ErrLeaseNotPending
				}),
				Events: events,
			})
			require.NoError(t, err)

			err = service.HandleCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
				LeaseUUID:   "11111111-1111-4111-8111-111111111111",
				Status:      backend.CallbackStatusSuccess,
				OperationID: callbackWireID(t, token.ID()),
			}))
			assert.Equal(t, int32(1), reads.Load())
			if tt.wantRetry {
				require.ErrorIs(t, err, ErrAcknowledgeFailed)
				assert.Equal(t, token.ID(), store.Lookup("11111111-1111-4111-8111-111111111111").AttemptOperationID(),
					"retryable acknowledgement failure must preserve durable operation authority")
				assert.Empty(t, events.events)
				if tt.leaseErr != nil {
					assert.ErrorIs(t, err, tt.leaseErr)
				}
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantFinished, !coordinator.RuntimeController().Contains("11111111-1111-4111-8111-111111111111"))
			if tt.wantReady {
				require.Len(t, events.events, 1)
				assert.Equal(t, backend.ProvisionStatusReady, events.events[0].Status)
				return
			}
			assert.Empty(t, events.events, "a terminal lease must not be resurrected by a stale success callback")
		})
	}
}

func TestCallbackService_RejectResponseLossUsesCurrentLeaseState(t *testing.T) {
	token := trackCallbackOperation(t, "11111111-1111-4111-8111-111111111111", "backend-a")
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	coordinator := prepareCallbackOperationForTest(t, store, token)
	payloads := &callbackPayloadRecorder{}
	events := &callbackEventRecorder{}
	var reads atomic.Int32
	var rejects atomic.Int32
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
		Chain: &callbackChainStub{
			getLease: func(context.Context, string) (*billingtypes.Lease, error) {
				if reads.Add(1) == 1 {
					return callbackTestLease(billingtypes.LEASE_STATE_PENDING), nil
				}
				return callbackTestLease(billingtypes.LEASE_STATE_REJECTED), nil
			},
			reject: func(context.Context, []string, string) (uint64, []string, error) {
				rejects.Add(1)
				return 0, nil, billingtypes.ErrLeaseNotPending
			},
		},
		Payloads: payloads,
		Events:   events,
	})
	require.NoError(t, err)

	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "11111111-1111-4111-8111-111111111111",
		Status:      backend.CallbackStatusFailed,
		Error:       "backend failed",
		OperationID: callbackWireID(t, token.ID()),
	})))

	assert.Equal(t, int32(2), reads.Load(), "reject errors must be resolved by an exact reread")
	assert.Equal(t, int32(1), rejects.Load())
	assert.Equal(t, placement.StateAbsent, store.Lookup("11111111-1111-4111-8111-111111111111").State())
	assert.Equal(t, []string{"11111111-1111-4111-8111-111111111111"}, payloads.deleted)
	assert.False(t, coordinator.RuntimeController().Contains("11111111-1111-4111-8111-111111111111"))
	require.Len(t, events.events, 1)
	assert.Equal(t, backend.ProvisionStatusFailed, events.events[0].Status)
	assert.Equal(t, "backend failed", events.events[0].Error)
}

func TestCallbackService_FailureSettlementUsesCurrentLeaseState(t *testing.T) {
	tests := []struct {
		name            string
		initialLease    *billingtypes.Lease
		afterReject     *billingtypes.Lease
		afterRejectErr  error
		rejectErr       error
		wantRetry       bool
		wantReject      bool
		wantCleanup     bool
		wantFailedEvent bool
	}{
		{
			name:            "pending lease is rejected",
			initialLease:    callbackTestLease(billingtypes.LEASE_STATE_PENDING),
			wantReject:      true,
			wantCleanup:     true,
			wantFailedEvent: true,
		},
		{
			name:            "active lease defers to reconciler without payload cleanup",
			initialLease:    callbackTestLease(billingtypes.LEASE_STATE_ACTIVE),
			wantFailedEvent: true,
		},
		{
			name:         "closed lease finishes without rejection",
			initialLease: callbackTestLease(billingtypes.LEASE_STATE_CLOSED),
			wantCleanup:  true,
		},
		{
			name:      "missing lease remains retryable",
			wantRetry: true,
		},
		{
			name:         "unknown initial state remains retryable",
			initialLease: callbackTestLease(billingtypes.LEASE_STATE_UNSPECIFIED),
			wantRetry:    true,
		},
		{
			name:            "reject error with active reread defers to reconciler",
			initialLease:    callbackTestLease(billingtypes.LEASE_STATE_PENDING),
			afterReject:     callbackTestLease(billingtypes.LEASE_STATE_ACTIVE),
			rejectErr:       billingtypes.ErrLeaseNotPending,
			wantReject:      true,
			wantFailedEvent: true,
		},
		{
			name:         "reject error with closed reread suppresses stale failed event",
			initialLease: callbackTestLease(billingtypes.LEASE_STATE_PENDING),
			afterReject:  callbackTestLease(billingtypes.LEASE_STATE_CLOSED),
			rejectErr:    billingtypes.ErrLeaseNotPending,
			wantReject:   true,
			wantCleanup:  true,
		},
		{
			name:         "reject error with expired reread suppresses stale failed event",
			initialLease: callbackTestLease(billingtypes.LEASE_STATE_PENDING),
			afterReject:  callbackTestLease(billingtypes.LEASE_STATE_EXPIRED),
			rejectErr:    billingtypes.ErrLeaseNotPending,
			wantReject:   true,
			wantCleanup:  true,
		},
		{
			name:           "reject error with missing reread remains retryable",
			initialLease:   callbackTestLease(billingtypes.LEASE_STATE_PENDING),
			afterRejectErr: billingtypes.ErrLeaseNotFound,
			rejectErr:      billingtypes.ErrLeaseNotPending,
			wantReject:     true,
			wantRetry:      true,
		},
		{
			name:         "reject error with pending reread remains retryable",
			initialLease: callbackTestLease(billingtypes.LEASE_STATE_PENDING),
			afterReject:  callbackTestLease(billingtypes.LEASE_STATE_PENDING),
			rejectErr:    billingtypes.ErrLeaseNotPending,
			wantRetry:    true,
			wantReject:   true,
		},
		{
			name:         "reject error with unknown reread remains retryable",
			initialLease: callbackTestLease(billingtypes.LEASE_STATE_PENDING),
			afterReject:  callbackTestLease(billingtypes.LEASE_STATE_UNSPECIFIED),
			rejectErr:    billingtypes.ErrLeaseNotPending,
			wantRetry:    true,
			wantReject:   true,
		},
		{
			name:           "reject error with failed reread remains retryable",
			initialLease:   callbackTestLease(billingtypes.LEASE_STATE_PENDING),
			afterRejectErr: errors.New("reread failed"),
			rejectErr:      billingtypes.ErrLeaseNotPending,
			wantRetry:      true,
			wantReject:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token := trackCallbackOperation(t, "11111111-1111-4111-8111-111111111111", "backend-a")
			store := newTestPlacementAuthority(t)
			armTestPlacementTopology(t, store, []string{"backend-a"})
			coordinator := prepareCallbackOperationForTest(t, store, token)
			payloads := &callbackPayloadRecorder{}
			events := &callbackEventRecorder{}
			var reads atomic.Int32
			var rejects atomic.Int32
			service, err := newCallbackServiceForTest(callbackServiceTestConfig{
				Coordinator: coordinator,
				Chain: &callbackChainStub{
					getLease: func(context.Context, string) (*billingtypes.Lease, error) {
						if reads.Add(1) == 1 {
							return tt.initialLease, nil
						}
						return tt.afterReject, tt.afterRejectErr
					},
					reject: func(context.Context, []string, string) (uint64, []string, error) {
						rejects.Add(1)
						return 1, []string{"tx"}, tt.rejectErr
					},
				},
				Payloads: payloads,
				Events:   events,
			})
			require.NoError(t, err)

			err = service.HandleCallback(context.Background(), callbackCommand(t, backend.CallbackPayload{
				LeaseUUID:   "11111111-1111-4111-8111-111111111111",
				Status:      backend.CallbackStatusFailed,
				OperationID: callbackWireID(t, token.ID()),
			}))
			if tt.wantRetry {
				require.Error(t, err)
				assert.Equal(t, token.ID(), store.Lookup("11111111-1111-4111-8111-111111111111").AttemptOperationID(),
					"retryable reject failure must preserve durable operation authority")
				assert.Empty(t, payloads.deleted)
				assert.Equal(t, placement.StateAttempting, store.Lookup("11111111-1111-4111-8111-111111111111").State())
				assert.Empty(t, events.events)
			} else {
				require.NoError(t, err)
				assert.False(t, coordinator.RuntimeController().Contains("11111111-1111-4111-8111-111111111111"))
				if tt.wantFailedEvent {
					require.Len(t, events.events, 1)
					assert.Equal(t, backend.ProvisionStatusFailed, events.events[0].Status)
				} else {
					assert.Empty(t, events.events,
						"a superseding terminal chain lifecycle must not receive a stale failed event")
				}
				assert.Equal(t, placement.StateAbsent, store.Lookup("11111111-1111-4111-8111-111111111111").State())
			}
			assert.Equal(t, tt.wantReject, rejects.Load() == 1)
			assert.Equal(t, tt.wantCleanup, len(payloads.deleted) == 1)
		})
	}
}

func TestCallbackService_ConcurrentDuplicateCallbacksAcknowledgeOnce(t *testing.T) {
	token := trackCallbackOperation(t, "11111111-1111-4111-8111-111111111111", "backend-a")
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	coordinator := prepareCallbackOperationForTest(t, store, token)

	acknowledgeStarted := make(chan struct{})
	releaseAcknowledge := make(chan struct{})
	var acknowledgeCalls atomic.Int32
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			if acknowledgeCalls.Add(1) == 1 {
				close(acknowledgeStarted)
			}
			<-releaseAcknowledge
			return true, "tx", nil
		}),
	})
	require.NoError(t, err)
	callback := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "11111111-1111-4111-8111-111111111111",
		Backend:     "backend-a",
		Status:      backend.CallbackStatusSuccess,
		OperationID: callbackWireID(t, token.ID()),
	})

	firstResult := make(chan error, 1)
	go func() { firstResult <- service.HandleCallback(context.Background(), callback) }()
	select {
	case <-acknowledgeStarted:
	case <-time.After(time.Second):
		t.Fatal("first callback did not reach acknowledgement")
	}
	contendedCtx, cancel := context.WithCancel(context.Background())
	secondResult := make(chan error, 1)
	go func() { secondResult <- service.HandleCallback(contendedCtx, callback) }()
	cancel()
	require.ErrorIs(t, <-secondResult, placement.ErrCallbackRecoveryBusy,
		"a duplicate cannot acquire the exact one-shot recovery authority")
	close(releaseAcknowledge)
	require.NoError(t, <-firstResult)

	assert.Equal(t, int32(1), acknowledgeCalls.Load())
	assert.False(t, coordinator.RuntimeController().Contains("11111111-1111-4111-8111-111111111111"))
}

func TestCallbackService_TypedLifecycleCapabilityIsRevocableAndObservationOnly(t *testing.T) {
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174099")
	require.NoError(t, err)
	coordinator, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	operationID = beginTestNewPlacementAttempt(
		t, store, callbackProvisionCoordinator(t, store, coordinator, "backend-a"), "11111111-1111-4111-8111-111111111111", "backend-a", operationID,
	)
	confirmCallbackAttemptForTest(t, coordinator, "11111111-1111-4111-8111-111111111111", operationID)
	lifecycleID, err := lifecycle.FromOperationID(operationID)
	require.NoError(t, err)

	events := &callbackEventRecorder{}
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
		Events:      events,
	})
	require.NoError(t, err)

	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "11111111-1111-4111-8111-111111111111",
			Backend:     "body-supplied-backend",
			Status:      backend.CallbackStatusSuccess,
			LifecycleID: lifecycleID.String(),
		},
	)))
	require.Len(t, events.events, 1)
	assert.Equal(t, backend.ProvisionStatusReady, events.events[0].Status)

	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "11111111-1111-4111-8111-111111111111",
			Backend:     "body-supplied-backend",
			Status:      backend.CallbackStatusFailed,
			Error:       "container exited",
			LifecycleID: lifecycleID.String(),
		},
	)))
	require.Len(t, events.events, 2)
	assert.Equal(t, backend.ProvisionStatusFailed, events.events[1].Status)

	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "11111111-1111-4111-8111-111111111111",
			Status:      backend.CallbackStatusDeprovisioned,
			Retained:    true,
			LifecycleID: lifecycleID.String(),
		},
	)))
	require.Len(t, events.events, 3)
	assert.Equal(t, backend.ProvisionStatusRetained, events.events[2].Status)

	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "11111111-1111-4111-8111-111111111111",
			Status:      backend.CallbackStatusFailed,
			LifecycleID: lifecycleID.String(),
		},
	)))
	assert.Len(t, events.events, 3, "a retired capability must be an idempotent no-op")
}

func TestCallbackService_LifecycleMetricsClassifyEveryReceivedCallback(t *testing.T) {
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174095")
	require.NoError(t, err)
	coordinator, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	operationID = beginTestNewPlacementAttempt(t, store, callbackProvisionCoordinator(t, store, coordinator, "backend-a"), "11111111-1111-4111-8111-111111111111", "backend-a", operationID)
	confirmCallbackAttemptForTest(t, coordinator, "11111111-1111-4111-8111-111111111111", operationID)
	lifecycleID, err := lifecycle.FromOperationID(operationID)
	require.NoError(t, err)
	staleID, err := lifecycle.ParseID("123e4567-e89b-42d3-a456-426614174094")
	require.NoError(t, err)

	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
	})
	require.NoError(t, err)

	metric := func(outcome, verdict, status string) float64 {
		t.Helper()
		return promtestutil.ToFloat64(
			metrics.LifecycleCallbackOutcomesTotal.WithLabelValues(outcome, verdict, status),
		)
	}
	metricTotal := func(status string) float64 {
		t.Helper()
		var total float64
		for _, outcome := range []string{
			metrics.LifecycleCallbackOutcomeApplied,
			metrics.LifecycleCallbackOutcomeDropped,
			metrics.LifecycleCallbackOutcomeRetryable,
		} {
			for _, verdict := range []string{
				metrics.LifecycleCallbackVerdictAuthorized,
				metrics.LifecycleCallbackVerdictLegacy,
				metrics.LifecycleCallbackVerdictTeardownOnly,
				metrics.LifecycleCallbackVerdictRetired,
				metrics.LifecycleCallbackVerdictInvalid,
				metrics.LifecycleCallbackVerdictMissing,
				metrics.LifecycleCallbackVerdictStale,
				metrics.LifecycleCallbackVerdictUnusable,
				metrics.LifecycleCallbackVerdictUnavailable,
				metrics.LifecycleCallbackVerdictUnknown,
			} {
				total += metric(outcome, verdict, status)
			}
		}
		return total
	}
	received := func(status string) float64 {
		t.Helper()
		return promtestutil.ToFloat64(
			metrics.NonInFlightCallbacksTotal.WithLabelValues(labelBackendUnknown, status),
		)
	}

	appliedSuccessBefore := metric(
		metrics.LifecycleCallbackOutcomeApplied,
		metrics.LifecycleCallbackVerdictAuthorized,
		string(backend.CallbackStatusSuccess),
	)
	successTotalBefore := metricTotal(string(backend.CallbackStatusSuccess))
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "11111111-1111-4111-8111-111111111111",
			Status:      backend.CallbackStatusSuccess,
			LifecycleID: lifecycleID.String(),
		},
	)))
	assert.Equal(t, 1.0, metric(
		metrics.LifecycleCallbackOutcomeApplied,
		metrics.LifecycleCallbackVerdictAuthorized,
		string(backend.CallbackStatusSuccess),
	)-appliedSuccessBefore)
	assert.Equal(t, 1.0, metricTotal(string(backend.CallbackStatusSuccess))-successTotalBefore,
		"one lifecycle callback must increment exactly one outcome series")

	receivedFailedBefore := received(string(backend.CallbackStatusFailed))
	failedTotalBefore := metricTotal(string(backend.CallbackStatusFailed))
	droppedStaleBefore := metric(
		metrics.LifecycleCallbackOutcomeDropped,
		metrics.LifecycleCallbackVerdictStale,
		string(backend.CallbackStatusFailed),
	)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "11111111-1111-4111-8111-111111111111",
			Status:      backend.CallbackStatusFailed,
			LifecycleID: staleID.String(),
		},
	)))
	assert.Equal(t, 1.0, received(string(backend.CallbackStatusFailed))-receivedFailedBefore,
		"the compatibility metric must count a received lifecycle callback even when authorization drops it")
	assert.Equal(t, 1.0, metric(
		metrics.LifecycleCallbackOutcomeDropped,
		metrics.LifecycleCallbackVerdictStale,
		string(backend.CallbackStatusFailed),
	)-droppedStaleBefore)
	assert.Equal(t, 1.0, metricTotal(string(backend.CallbackStatusFailed))-failedTotalBefore,
		"one lifecycle callback must increment exactly one outcome series")

	deprovisionedTotalBefore := metricTotal(string(backend.CallbackStatusDeprovisioned))
	appliedRetiredBefore := metric(
		metrics.LifecycleCallbackOutcomeApplied,
		metrics.LifecycleCallbackVerdictRetired,
		string(backend.CallbackStatusDeprovisioned),
	)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "11111111-1111-4111-8111-111111111111",
			Status:      backend.CallbackStatusDeprovisioned,
			LifecycleID: lifecycleID.String(),
		},
	)))
	assert.Equal(t, 1.0, metric(
		metrics.LifecycleCallbackOutcomeApplied,
		metrics.LifecycleCallbackVerdictRetired,
		string(backend.CallbackStatusDeprovisioned),
	)-appliedRetiredBefore)
	assert.Equal(t, 1.0,
		metricTotal(string(backend.CallbackStatusDeprovisioned))-deprovisionedTotalBefore,
		"one lifecycle callback must increment exactly one outcome series")

	receivedDeprovisionedBefore := received(string(backend.CallbackStatusDeprovisioned))
	deprovisionedTotalBefore = metricTotal(string(backend.CallbackStatusDeprovisioned))
	droppedRetiredBefore := metric(
		metrics.LifecycleCallbackOutcomeDropped,
		metrics.LifecycleCallbackVerdictRetired,
		string(backend.CallbackStatusDeprovisioned),
	)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID:   "11111111-1111-4111-8111-111111111111",
			Status:      backend.CallbackStatusDeprovisioned,
			LifecycleID: lifecycleID.String(),
		},
	)))
	assert.Equal(t, 1.0,
		received(string(backend.CallbackStatusDeprovisioned))-receivedDeprovisionedBefore,
		"a replay after retirement remains visible as received")
	assert.Equal(t, 1.0, metric(
		metrics.LifecycleCallbackOutcomeDropped,
		metrics.LifecycleCallbackVerdictRetired,
		string(backend.CallbackStatusDeprovisioned),
	)-droppedRetiredBefore)
	assert.Equal(t, 1.0,
		metricTotal(string(backend.CallbackStatusDeprovisioned))-deprovisionedTotalBefore,
		"one lifecycle callback must increment exactly one outcome series")

	// A service without lifecycle authority is no longer constructible in
	// production: the coordinator carries operation, placement, storage, and
	// lifecycle authority as one indivisible dependency.
}

func TestCallbackService_PlacementConflictWithdrawsLifecycleObservationAuthority(t *testing.T) {
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a", "backend-b"})
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174098")
	require.NoError(t, err)
	coordinator, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: callbackAmbiguousProvisionBackend{Backend: backend.NewMockBackend(
			backend.MockBackendConfig{Name: "backend-a"},
		)}, IsDefault: true},
		{Backend: backend.NewMockBackend(backend.MockBackendConfig{Name: "backend-b"})},
	}})
	require.NoError(t, err)
	operationID = beginTestNewPlacementAttempt(
		t, store, bindCallbackProvisionCoordinator(t, store, coordinator, router), "11111111-1111-4111-8111-111111111111", "backend-a", operationID,
	)
	confirmCallbackAttemptForTest(t, coordinator, "11111111-1111-4111-8111-111111111111", operationID)
	lifecycleID, err := lifecycle.FromOperationID(operationID)
	require.NoError(t, err)

	projectTestPlacementInventory(t, testReconciliationCoordinator(t, store), []string{"backend-a", "backend-b"},
		placement.ReconciliationProjection{Conflicts: map[string][]string{
			"11111111-1111-4111-8111-111111111111": {"backend-a", "backend-b"},
		}})
	events := &callbackEventRecorder{}
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
		Events:      events,
	})
	require.NoError(t, err)
	for _, status := range []backend.CallbackStatus{
		backend.CallbackStatusFailed,
		backend.CallbackStatusDeprovisioned,
	} {
		require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
			backend.CallbackPayload{
				LeaseUUID:   "11111111-1111-4111-8111-111111111111",
				Status:      status,
				LifecycleID: lifecycleID.String(),
			},
		)))
	}
	assert.Empty(t, events.events)
}

func TestCallbackService_RecoversExactDurableAttemptWithoutRegistryRecord(t *testing.T) {
	tests := []struct {
		name       string
		status     backend.CallbackStatus
		wantState  placement.State
		wantAck    int
		wantReject int
	}{
		{
			name:      "success confirms and acknowledges",
			status:    backend.CallbackStatusSuccess,
			wantState: placement.StateConfirmed,
			wantAck:   1,
		},
		{
			name:       "failure refuses and rejects",
			status:     backend.CallbackStatusFailed,
			wantState:  placement.StateAbsent,
			wantReject: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTestPlacementAuthority(t)
			armTestPlacementTopology(t, store, []string{"backend-a"})
			operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174090")
			require.NoError(t, err)
			var acknowledgeCalls atomic.Int32
			var rejectCalls atomic.Int32
			coordinator := bindTestOperationCoordinator(t, store)
			operationID = beginTestNewPlacementAttempt(t, store, callbackProvisionCoordinator(t, store, coordinator, "backend-a"), "44444444-4444-4444-8444-444444444444", "backend-a", operationID)
			coordinator.RuntimeController().BeginDrain()
			service, err := newCallbackServiceForTest(callbackServiceTestConfig{
				Coordinator: coordinator,
				Acknowledger: callbackAcknowledgerFunc(func(
					context.Context, string,
				) (bool, string, error) {
					acknowledgeCalls.Add(1)
					return true, "tx-ack", nil
				}),
				Chain: &callbackChainStub{
					getLease: func(context.Context, string) (*billingtypes.Lease, error) {
						return &billingtypes.Lease{
							Uuid:   "44444444-4444-4444-8444-444444444444",
							Tenant: "tenant-test", ProviderUuid: placementstore.ProviderUUID,
							State: billingtypes.LEASE_STATE_PENDING,
						}, nil
					},
					reject: func(context.Context, []string, string) (uint64, []string, error) {
						rejectCalls.Add(1)
						return 1, []string{"tx-reject"}, nil
					},
				},
			})
			require.NoError(t, err)

			require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
				backend.CallbackPayload{
					LeaseUUID:   "44444444-4444-4444-8444-444444444444",
					Status:      tt.status,
					Error:       "backend refused",
					Backend:     "body-controlled-backend",
					OperationID: operationID.String(),
				},
			)))

			current := store.Lookup("44444444-4444-4444-8444-444444444444")
			assert.Equal(t, tt.wantState, current.State())
			assert.Empty(t, current.Attempt)
			assert.False(t, current.AttemptOperationID().Valid())
			assert.Equal(t, int32(tt.wantAck), acknowledgeCalls.Load())
			assert.Equal(t, int32(tt.wantReject), rejectCalls.Load())
			if tt.wantState == placement.StateConfirmed {
				assert.Equal(t, "backend-a", current.Backend,
					"the callback body must not select recovered ownership")
			}
		})
	}
}

func TestCallbackService_RecoveredFailureRetainsAttemptAcrossUnknownChainRead(t *testing.T) {
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	operationID, err := operation.ParseID("a65e5ccb-2423-45be-8fc2-01388b16728e")
	require.NoError(t, err)
	coordinator := bindTestOperationCoordinator(t, store)
	operationID = beginTestNewPlacementAttempt(t, store, callbackProvisionCoordinator(t, store, coordinator, "backend-a"), "55555555-5555-4555-8555-555555555555", "backend-a", operationID)

	payloads := &callbackPayloadRecorder{}
	var reads atomic.Int32
	var rejects atomic.Int32
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
		Chain: &callbackChainStub{
			getLease: func(context.Context, string) (*billingtypes.Lease, error) {
				if reads.Add(1) == 1 {
					return nil, nil
				}
				return &billingtypes.Lease{
					Uuid:   "55555555-5555-4555-8555-555555555555",
					Tenant: "tenant-test", ProviderUuid: placementstore.ProviderUUID,
					State: billingtypes.LEASE_STATE_PENDING,
				}, nil
			},
			reject: func(context.Context, []string, string) (uint64, []string, error) {
				rejects.Add(1)
				return 1, []string{"tx-reject"}, nil
			},
		},
		Payloads: payloads,
	})
	require.NoError(t, err)
	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "55555555-5555-4555-8555-555555555555",
		Status:      backend.CallbackStatusFailed,
		Error:       "backend refused",
		OperationID: operationID.String(),
	})

	err = service.HandleCallback(context.Background(), command)
	require.ErrorContains(t, err, "no result")
	assert.Equal(t, placement.StateAttempting, store.Lookup("55555555-5555-4555-8555-555555555555").State(),
		"an absent RPC view cannot erase exact redelivery authority")
	assert.Empty(t, payloads.deleted,
		"an absent RPC view cannot erase the payload needed by a later sweep")
	assert.Zero(t, rejects.Load())

	require.NoError(t, service.HandleCallback(context.Background(), command))
	assert.Equal(t, placement.StateAbsent, store.Lookup("55555555-5555-4555-8555-555555555555").State())
	assert.Equal(t, []string{"55555555-5555-4555-8555-555555555555"}, payloads.deleted)
	assert.Equal(t, int32(1), rejects.Load())
}

func TestCallbackService_RecoveredFailureFencesOlderPositiveInventory(t *testing.T) {
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174091")
	require.NoError(t, err)
	coordinator := bindTestOperationCoordinator(t, store)
	provision := callbackProvisionCoordinator(t, store, coordinator, "backend-a")
	operationID = beginTestNewPlacementAttempt(t, store, provision, "66666666-6666-4666-8666-666666666666", "backend-a", operationID)

	// Register the inventory snapshot before callback recovery claims the
	// attempt. Projection happens while chain settlement is blocked.
	reconciliationValue, ok := testReconciliationCoordinators.Load(provision)
	require.True(t, ok)
	sweep, err := reconciliationValue.(*placement.ReconciliationCoordinator).BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	reconciliation := reconciliationValue.(*placement.ReconciliationCoordinator)
	disposition := collectTestBackendInventory(
		t, reconciliation, sweep, "backend-a", testBackendStorageID("backend-a"),
		[]backend.ProvisionInfo{{
			LeaseUUID:    "66666666-6666-4666-8666-666666666666",
			BackendName:  "backend-a",
			Tenant:       "tenant-test",
			ProviderUUID: placementstore.ProviderUUID,
			LifecycleGeneration: &backend.LifecycleGenerationObservation{
				Kind: backend.LifecycleGenerationTyped,
				ID:   operationID.String(),
			},
		}}, nil,
	)
	require.Equal(t, placement.BackendInventoryAuthoritative, disposition)
	require.NoError(t, sweep.SealInventory())
	getLeaseEntered := make(chan struct{})
	allowGetLease := make(chan struct{})
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
		Chain: &callbackChainStub{
			getLease: func(context.Context, string) (*billingtypes.Lease, error) {
				close(getLeaseEntered)
				<-allowGetLease
				return &billingtypes.Lease{
					Uuid: "66666666-6666-4666-8666-666666666666", Tenant: "tenant-test",
					ProviderUuid: placementstore.ProviderUUID,
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			},
			reject: func(context.Context, []string, string) (uint64, []string, error) {
				return 1, []string{"tx-reject"}, nil
			},
		},
	})
	require.NoError(t, err)

	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "66666666-6666-4666-8666-666666666666",
		Status:      backend.CallbackStatusFailed,
		OperationID: operationID.String(),
	})
	callbackResult := make(chan error, 1)
	go func() {
		callbackResult <- service.HandleCallback(context.Background(), command)
	}()
	<-getLeaseEntered

	_, err = sweep.Project(placement.ReconciliationProjection{
		Placements: map[string]string{"66666666-6666-4666-8666-666666666666": "backend-a"},
	})
	require.NoError(t, err)
	assert.Equal(t, placement.StateAttempting, store.Lookup("66666666-6666-4666-8666-666666666666").State())

	close(allowGetLease)
	require.NoError(t, <-callbackResult)
	assert.Equal(t, placement.StateAbsent, store.Lookup("66666666-6666-4666-8666-666666666666").State())
}

func TestCallbackService_RecoveredAttemptReleasesClaimForChainRetry(t *testing.T) {
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174092")
	require.NoError(t, err)
	coordinator := bindTestOperationCoordinator(t, store)
	operationID = beginTestNewPlacementAttempt(t, store, callbackProvisionCoordinator(t, store, coordinator, "backend-a"), "77777777-7777-4777-8777-777777777777", "backend-a", operationID)

	var calls atomic.Int32
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			if calls.Add(1) == 1 {
				return false, "", errors.New("temporary chain outage")
			}
			return true, "tx-ack", nil
		}),
	})
	require.NoError(t, err)
	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   "77777777-7777-4777-8777-777777777777",
		Status:      backend.CallbackStatusSuccess,
		OperationID: operationID.String(),
	})

	err = service.HandleCallback(context.Background(), command)
	require.ErrorIs(t, err, ErrAcknowledgeFailed)
	assert.Equal(t, placement.StateAttempting, store.Lookup("77777777-7777-4777-8777-777777777777").State(),
		"retryable chain failure must preserve the durable recovery authority")
	require.NoError(t, service.HandleCallback(context.Background(), command))
	assert.Equal(t, placement.StateConfirmed, store.Lookup("77777777-7777-4777-8777-777777777777").State())
	assert.Equal(t, int32(2), calls.Load())
}

func TestCallbackService_RecoveredCallbackFencesPlanAndDeprovision(t *testing.T) {
	const leaseUUID = "88888888-8888-4888-8888-888888888888"
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	operationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174093")
	require.NoError(t, err)

	tracker := newTestOperationRegistry()
	coordinator, err := tracker.bindPlacementStore(store)
	require.NoError(t, err)
	backendClient := &mockManagerBackend{name: "backend-a"}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{
		Backend: callbackAmbiguousProvisionBackend{Backend: backendClient}, IsDefault: true,
	}}})
	require.NoError(t, err)
	provision := bindCallbackProvisionCoordinator(t, store, coordinator, router)
	operationID = beginTestNewPlacementAttempt(
		t, store, provision, leaseUUID, "backend-a", operationID,
	)

	acknowledgeEntered := make(chan struct{})
	releaseAcknowledge := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseAcknowledge) }) })
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			close(acknowledgeEntered)
			<-releaseAcknowledge
			return true, "tx-ack", nil
		}),
	})
	require.NoError(t, err)
	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID: leaseUUID, Status: backend.CallbackStatusSuccess,
		OperationID: operationID.String(),
	})

	callbackResult := make(chan error, 1)
	go func() { callbackResult <- service.HandleCallback(context.Background(), command) }()
	<-acknowledgeEntered

	deprovisionErr := provision.Deprovision(context.Background(), leaseUUID)
	require.Error(t, deprovisionErr)
	backendClient.mu.Lock()
	assert.Empty(t, backendClient.deprovisionCalls,
		"deprovision must not contact a backend while callback recovery owns the lease")
	backendClient.mu.Unlock()

	releaseOnce.Do(func() { close(releaseAcknowledge) })
	require.NoError(t, <-callbackResult)
	assert.Equal(t, placement.StateConfirmed, store.Lookup(leaseUUID).State())
}

func TestCallbackService_InventoryConfirmedGenerationSettlesChainWithoutDemotion(t *testing.T) {
	tests := []struct {
		name        string
		status      backend.CallbackStatus
		chainState  billingtypes.LeaseState
		wantAck     int32
		wantReads   int32
		wantRejects int32
		wantEvent   backend.ProvisionStatus
	}{
		{
			name: "later success acknowledges", status: backend.CallbackStatusSuccess,
			chainState: billingtypes.LEASE_STATE_PENDING, wantAck: 1,
			wantEvent: backend.ProvisionStatusReady,
		},
		{
			name: "active stale failure preserves live owner", status: backend.CallbackStatusFailed,
			chainState: billingtypes.LEASE_STATE_ACTIVE, wantReads: 1,
		},
		{
			name: "pending failure still rejects and settles", status: backend.CallbackStatusFailed,
			chainState: billingtypes.LEASE_STATE_PENDING, wantReads: 1,
			wantRejects: 1, wantEvent: backend.ProvisionStatusFailed,
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const leaseUUID = "99999999-9999-4999-8999-999999999999"
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			storeBeforeRestart, err := placementstore.NewStore(dbPath)
			require.NoError(t, err)
			armTestPlacementTopology(t, storeBeforeRestart, []string{"backend-a"})
			operationID, err := operation.ParseID(fmt.Sprintf(
				"123e4567-e89b-42d3-a456-4266141741%02d", index,
			))
			require.NoError(t, err)
			seedCoordinator := bindTestOperationCoordinator(
				t, storeBeforeRestart,
			)
			seedProvision := callbackProvisionCoordinator(
				t, storeBeforeRestart, seedCoordinator, "backend-a",
			)
			operationID = beginTestNewPlacementAttempt(
				t, storeBeforeRestart, seedProvision, leaseUUID, "backend-a", operationID,
			)
			generation, err := lifecycle.FromOperationID(operationID)
			require.NoError(t, err)
			projectCallbackLifecycleGeneration(
				t, seedProvision, leaseUUID, "backend-a", generation,
			)
			require.NoError(t, storeBeforeRestart.Close())

			store, err := placementstore.NewStore(dbPath)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			before := store.Lookup(leaseUUID)
			require.Equal(t, placement.StateConfirmed, before.State())
			require.Empty(t, before.Attempt)
			restartedCoordinator := bindTestOperationCoordinator(t, store)
			bindCallbackObservationRuntimeForTest(t, restartedCoordinator, "backend-a")

			var acknowledgeCalls atomic.Int32
			var chainReads atomic.Int32
			var rejectCalls atomic.Int32
			events := &callbackEventRecorder{}
			service, err := newCallbackServiceForTest(callbackServiceTestConfig{
				Coordinator: restartedCoordinator,
				Acknowledger: callbackAcknowledgerFunc(func(
					context.Context, string,
				) (bool, string, error) {
					acknowledgeCalls.Add(1)
					return true, "tx-ack", nil
				}),
				Chain: &callbackChainStub{
					getLease: func(context.Context, string) (*billingtypes.Lease, error) {
						chainReads.Add(1)
						return &billingtypes.Lease{
							Uuid: leaseUUID, Tenant: "tenant-test",
							ProviderUuid: placementstore.ProviderUUID, State: test.chainState,
						}, nil
					},
					reject: func(context.Context, []string, string) (uint64, []string, error) {
						rejectCalls.Add(1)
						return 1, []string{"tx-reject"}, nil
					},
				},
				Events: events,
			})
			require.NoError(t, err)

			require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
				backend.CallbackPayload{
					LeaseUUID: leaseUUID, Status: test.status,
					OperationID: operationID.String(), Error: "stale failure",
				},
			)))
			assert.Equal(t, test.wantAck, acknowledgeCalls.Load())
			assert.Equal(t, test.wantReads, chainReads.Load())
			assert.Equal(t, test.wantRejects, rejectCalls.Load())
			if test.wantEvent != "" {
				require.Len(t, events.events, 1)
				assert.Equal(t, test.wantEvent, events.events[0].Status)
			} else {
				assert.Empty(t, events.events)
			}
			assert.Equal(t, before, store.Lookup(leaseUUID),
				"inventory-confirmed ownership must remain exact and unchanged")
		})
	}
}

func TestCallbackService_PendingFailureSettlesAttemptWithOlderObservedOwner(t *testing.T) {
	const leaseUUID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	storeBeforeRestart, err := placementstore.NewStore(dbPath)
	require.NoError(t, err)
	armTestPlacementTopology(t, storeBeforeRestart, []string{"backend-a"})
	newOperationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174110")
	require.NoError(t, err)
	olderOperationID, err := operation.ParseID("123e4567-e89b-42d3-a456-426614174109")
	require.NoError(t, err)
	olderGeneration, err := lifecycle.FromOperationID(olderOperationID)
	require.NoError(t, err)
	seedCoordinator := bindTestOperationCoordinator(
		t, storeBeforeRestart,
	)
	seedProvision := callbackProvisionCoordinator(
		t, storeBeforeRestart, seedCoordinator, "backend-a",
	)
	newOperationID = beginTestNewPlacementAttempt(
		t, storeBeforeRestart, seedProvision, leaseUUID, "backend-a", newOperationID,
	)
	projectCallbackLifecycleGeneration(
		t, seedProvision, leaseUUID, "backend-a", olderGeneration,
	)
	beforeRestart := storeBeforeRestart.Lookup(leaseUUID)
	require.Equal(t, placement.StateConfirmed, beforeRestart.State())
	require.Equal(t, "backend-a", beforeRestart.Attempt)
	require.Equal(t, newOperationID, beforeRestart.AttemptOperationID())
	require.NoError(t, storeBeforeRestart.Close())

	store, err := placementstore.NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	restartedCoordinator := bindTestOperationCoordinator(t, store)
	bindCallbackObservationRuntimeForTest(t, restartedCoordinator, "backend-a")

	var chainReads atomic.Int32
	var rejectCalls atomic.Int32
	events := &callbackEventRecorder{}
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: restartedCoordinator,
		Chain: &callbackChainStub{
			getLease: func(context.Context, string) (*billingtypes.Lease, error) {
				chainReads.Add(1)
				return &billingtypes.Lease{
					Uuid: leaseUUID, Tenant: "tenant-test",
					ProviderUuid: placementstore.ProviderUUID,
					State:        billingtypes.LEASE_STATE_PENDING,
				}, nil
			},
			reject: func(context.Context, []string, string) (uint64, []string, error) {
				rejectCalls.Add(1)
				return 1, []string{"tx-reject"}, nil
			},
		},
		Events: events,
	})
	require.NoError(t, err)
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID: leaseUUID, Status: backend.CallbackStatusFailed,
			OperationID: newOperationID.String(), Error: "new attempt failed",
		},
	)))

	assert.Equal(t, int32(1), chainReads.Load())
	assert.Equal(t, int32(1), rejectCalls.Load())
	require.Len(t, events.events, 1)
	assert.Equal(t, backend.ProvisionStatusFailed, events.events[0].Status)
	settled := store.Lookup(leaseUUID)
	assert.Equal(t, placement.StateConfirmed, settled.State())
	assert.Equal(t, "backend-a", settled.Backend)
	assert.Empty(t, settled.Attempt)
	assert.False(t, settled.AttemptOperationID().Valid())
	require.NoError(t, service.HandleCallback(context.Background(), callbackCommand(t,
		backend.CallbackPayload{
			LeaseUUID: leaseUUID, Status: backend.CallbackStatusSuccess,
			LifecycleID: olderGeneration.String(),
		},
	)))
	require.Len(t, events.events, 2)
	assert.Equal(t, backend.ProvisionStatusReady, events.events[1].Status,
		"failure of the newer attempt must not rotate or demote the older observed owner")
}

func TestCallbackService_RegistryBackedRestoreFencesRecoveredCallback(t *testing.T) {
	const (
		sourceLease = "11111111-1111-4111-8111-111111111111"
		targetLease = "22222222-2222-4222-8222-222222222222"
	)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a"})
	coordinator := bindTestOperationCoordinator(t, store)
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{
		Backend: callbackAmbiguousRestoreBackend{Backend: backend.NewMockBackend(
			backend.MockBackendConfig{Name: "backend-a"},
		)},
		IsDefault: true,
	}}})
	require.NoError(t, err)
	execution := bindTestBackendRuntime(t, coordinator, router)
	chain := &callbackChainStub{getLease: func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
		switch leaseUUID {
		case sourceLease:
			return &billingtypes.Lease{
				Uuid: sourceLease, Tenant: "tenant-test",
				ProviderUuid: placementstore.ProviderUUID,
				State:        billingtypes.LEASE_STATE_CLOSED,
			}, nil
		case targetLease:
			return &billingtypes.Lease{
				Uuid: targetLease, Tenant: "tenant-test",
				ProviderUuid: placementstore.ProviderUUID,
				State:        billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{{
					SkuUuid: "sku-test", Quantity: 1,
				}},
			}, nil
		default:
			return nil, billingtypes.ErrLeaseNotFound
		}
	}}
	bindTestReconciliationCoordinator(t, store, execution, chain, nil, nil)
	seedTestConfirmedPlacements(t, store, []string{"backend-a"}, map[string]string{
		sourceLease: "backend-a",
	})
	restoreAuthority, err := execution.RestoreCoordinator(nil)
	require.NoError(t, err)
	request, err := placement.NewRestoreApplicationRequest(
		targetLease, "tenant-test", sourceLease,
	)
	require.NoError(t, err)
	completion := restoreAuthority.ExecuteApplication(context.Background(), request)
	require.Error(t, completion.CallErr())
	operationID := store.Lookup(targetLease).AttemptOperationID()
	require.True(t, operationID.Valid())
	assert.False(t, coordinator.RuntimeController().Contains(targetLease),
		"the recovery window begins after synchronous restore removes its operation")

	var acknowledgeCalls atomic.Int32
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			acknowledgeCalls.Add(1)
			return true, "tx-ack", nil
		}),
	})
	require.NoError(t, err)
	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID: targetLease, Status: backend.CallbackStatusSuccess,
		OperationID: operationID.String(),
	})

	require.NoError(t, service.HandleCallback(context.Background(), command))
	assert.Equal(t, int32(1), acknowledgeCalls.Load(),
		"retry uses the exact restore lifecycle generation after Registry exclusion ends")
	assert.Equal(t, placement.StateConfirmed, store.Lookup(targetLease).State())
	assert.Equal(t, "backend-a", store.Lookup(targetLease).Backend)
}
