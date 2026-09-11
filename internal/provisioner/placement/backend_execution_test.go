package placement

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

type executionLeaseReaderFunc func(context.Context, string) (*billingtypes.Lease, error)

func (read executionLeaseReaderFunc) GetLease(
	ctx context.Context,
	leaseUUID string,
) (*billingtypes.Lease, error) {
	return read(ctx, leaseUUID)
}

func executionLeaseReader() executionLeaseReaderFunc {
	return func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
		return &billingtypes.Lease{
			Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
			State: billingtypes.LEASE_STATE_PENDING,
		}, nil
	}
}

func TestProvisionExecuteInvokesBackendOnceAcrossCopiedConcurrentDispatch(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-execute-once")
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	client := &executionTestBackend{name: "backend-a", provision: func(
		context.Context, backend.ProvisionRequest,
	) error {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return nil
	}}
	execution := bindExecutionForTest(
		t, fixture.coordinator, newExecutionTestRuntime(client),
	)
	authority, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)

	results := make(chan DispatchResult, 2)
	first := fixture.dispatch
	second := first
	go func() {
		results <- executeProvision(t.Context(), fixture.coordinator, authority.backends, nil, first, nil)
	}()
	<-entered
	go func() {
		results <- executeProvision(t.Context(), fixture.coordinator, authority.backends, nil, second, nil)
	}()
	close(release)
	firstResult, secondResult := <-results, <-results

	assert.Equal(t, int32(1), calls.Load())
	assert.True(t, firstResult.Applied() || secondResult.Applied())
	assert.True(t,
		firstResult.Disposition() == DispatchInvalid ||
			secondResult.Disposition() == DispatchInvalid,
		"the copied authority must not cross the one-shot call barrier twice",
	)
}

func TestProvisionExecutePanicIsAmbiguousAndPreservesDurableAttempt(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-execute-panic")
	client := &executionTestBackend{name: "backend-a", provision: func(
		context.Context, backend.ProvisionRequest,
	) error {
		panic("physical implementation fault")
	}}
	execution := bindExecutionForTest(
		t, fixture.coordinator, newExecutionTestRuntime(client),
	)
	authority, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)

	result := executeProvision(
		t.Context(), fixture.coordinator, authority.backends, nil, fixture.dispatch, nil,
	)

	assert.True(t, result.CallAmbiguous())
	require.ErrorContains(t, result.CallErr(), "panicked")
	remaining := fixture.store.Lookup("lease-execute-panic")
	assert.Equal(t, StateAttempting, remaining.State())
	assert.Equal(t, fixture.initiation.ID(), remaining.AttemptOperationID())
	assert.False(t, fixture.coordinator.RuntimeController().Contains("lease-execute-panic"))
}

func TestBackendPanicRecoveryDoesNotExposeCallbackCapabilities(t *testing.T) {
	const (
		leaseUUID   = "550e8400-e29b-41d4-a716-446655440000"
		operationID = "2b0fb1e9-b9ad-4f52-a93d-69e8eb72830a"
		lifecycleID = "6ba7b811-9dad-41d1-80b4-00c04fd430c8"
	)
	operationURL := "https://provider.test/callbacks/provision?operation_id=" + operationID
	lifecycleURL := "https://provider.test/callbacks/provision?lifecycle_id=" + lifecycleID
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	client := &executionTestBackend{
		name: "backend-a",
		provision: func(_ context.Context, request backend.ProvisionRequest) error {
			panic(request)
		},
		restore: func(_ context.Context, request backend.RestoreRequest) error {
			panic(request)
		},
		restart: func(_ context.Context, request backend.RestartRequest) error {
			panic(request)
		},
	}
	provisionErr := invokeProvision(t.Context(), client, backend.ProvisionRequest{
		LeaseUUID: leaseUUID, CallbackURL: operationURL, LifecycleCallbackURL: lifecycleURL,
	})
	restoreErr := invokeRestore(t.Context(), client, backend.RestoreRequest{
		LeaseUUID: leaseUUID, CallbackURL: operationURL, LifecycleCallbackURL: lifecycleURL,
	})
	maintenanceID, err := maintenanceid.Parse(operationID)
	require.NoError(t, err)
	maintenanceErr := invokeMaintenance(t.Context(), client, MaintenanceCommand{
		id: maintenanceID, leaseUUID: leaseUUID, backendName: "backend-a",
		kind: MaintenanceCommandRestart, callbackURL: lifecycleURL,
	})

	combined := strings.Join([]string{
		provisionErr.Err().Error(), restoreErr.Err().Error(), maintenanceErr.Err().Error(), logs.String(),
	}, "\n")
	assert.NotContains(t, combined, operationURL)
	assert.NotContains(t, combined, lifecycleURL)
	assert.NotContains(t, combined, operationID)
	assert.NotContains(t, combined, lifecycleID)
	assert.Contains(t, combined, "panic_type")
}

func TestProvisionExecuteInlineCallbackWinsOverSynchronousRefusal(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-execute-inline")
	var once sync.Once
	client := &executionTestBackend{name: "backend-a", provision: func(
		context.Context, backend.ProvisionRequest,
	) error {
		once.Do(func() {
			claim := fixture.coordinator.tryClaimCallback(
				"lease-execute-inline", fixture.initiation.ID(),
			)
			require.True(t, claim.Claimed())
			require.NoError(t,
				fixture.coordinator.finishConfirmedCallback(claim.Claim()))
		})
		return backend.ErrValidation
	}}
	execution := bindExecutionForTest(
		t, fixture.coordinator, newExecutionTestRuntime(client),
	)
	authority, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)

	result := executeProvision(
		t.Context(), fixture.coordinator, authority.backends, nil, fixture.dispatch, nil,
	)

	assert.True(t, result.Superseded(), result.Err())
	assert.Equal(t, StateConfirmed, fixture.store.Lookup("lease-execute-inline").State())
	assert.False(t, fixture.coordinator.RuntimeController().Contains("lease-execute-inline"))
}

func TestProvisionExecuteRejectsWrongNamedBackendBeforeCall(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-wrong-runtime-name")
	var calls atomic.Int32
	wrong := &executionTestBackend{name: "backend-b", provision: func(
		context.Context, backend.ProvisionRequest,
	) error {
		calls.Add(1)
		return nil
	}}
	runtime := &misdirectingExecutionRuntime{
		executionTestRuntime: executionRuntime("backend-a"),
		resolved:             wrong,
	}
	execution := bindExecutionForTest(t, fixture.coordinator, runtime)
	authority, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)

	result := executeProvision(
		t.Context(), fixture.coordinator, authority.backends, nil, fixture.dispatch, nil,
	)

	assert.Zero(t, calls.Load())
	require.ErrorContains(t, result.Err(), "does not match exact name")
}

func TestProvisionRouteCannotAuthorizeAnotherLease(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-route-source")
	execution := bindExecutionForTest(
		t, fixture.coordinator, executionRuntime("backend-a"),
	)
	authority, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)
	route, err := authority.routeProvision(
		t.Context(), "lease-route-source", "sku-test", nil, nil,
	)
	require.NoError(t, err)
	require.True(t, route.valid())
	scope, err := authority.scopeAdmission(fixture.store.CurrentAdmissionBaseline())
	require.NoError(t, err)

	_, applied, err := authority.admitNewProvisionDispatch(
		fixture.initiation, scope, "lease-route-other", route,
		PayloadFingerprint{}, BackendRequestSnapshot{}, CallbackPair{},
	)

	assert.False(t, applied)
	assert.ErrorIs(t, err, ErrProvisionRouteUnresolvable)
}

func TestProvisionApplicationOwnsAuthorizationRoutesCallAndSettlement(t *testing.T) {
	const leaseUUID = "61638ef8-1401-4f14-a355-1ae02afeb35b"
	routes, err := NewCallbackRouteFactory("https://callbacks.provider.test/proxy?trace=a%2Fb")
	require.NoError(t, err)
	store := newTestStore(t, WithCallbackRouteFactory(routes))
	requireAdmissionBaseline(t, store, "backend-a")
	registry := operation.NewRegistry()
	base, err := newOperationCoordinator(store, registry)
	require.NoError(t, err)
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var captured backend.ProvisionRequest
	client := &executionTestBackend{name: "backend-a", provision: func(
		_ context.Context, request backend.ProvisionRequest,
	) error {
		captured = request
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return nil
	}}
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(client))
	reader := executionLeaseReaderFunc(func(_ context.Context, observed string) (*billingtypes.Lease, error) {
		return &billingtypes.Lease{
			Uuid: observed, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
			State: billingtypes.LEASE_STATE_PENDING,
			Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
		}, nil
	})
	setProviderControlPlaneForTest(t, execution, reader)
	authority, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)
	request, err := NewProvisionEventRequest(leaseUUID, "tenant-test")
	require.NoError(t, err)

	results := make(chan ProvisionEventResult, 2)
	go func() { results <- authority.ExecuteCurrentLease(t.Context(), request) }()
	<-entered
	go func() { results <- authority.ExecuteCurrentLease(t.Context(), request) }()
	duplicate := <-results
	assert.Equal(t, ProvisionEventDuplicate, duplicate.Disposition())
	close(release)
	started := <-results
	assert.Equal(t, ProvisionEventStarted, started.Disposition(), started.Err())
	assert.Equal(t, int32(1), calls.Load())
	assert.Equal(t, StateConfirmed, store.Lookup(leaseUUID).State())
	assert.Contains(t, captured.CallbackURL, "https://callbacks.provider.test/proxy/callbacks/provision")
	assert.Contains(t, captured.CallbackURL, "operation_id=")
	assert.Contains(t, captured.LifecycleCallbackURL, "lifecycle_id=")
}

func TestDeprovisionOwnsClaimsTargetsInvocationAndSettlement(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-deprovision-closed-boundary")
	call, calling := fixture.coordinator.beginProvisionCall(fixture.dispatch)
	require.True(t, calling)
	require.True(t, fixture.coordinator.completeProvision(
		call, backend.ConservativeProvisionCallOutcome(nil),
	).Applied())
	var calls atomic.Int32
	client := &executionTestBackend{name: "backend-a", deprovision: func(
		context.Context, string,
	) error {
		calls.Add(1)
		return nil
	}}
	execution := bindExecutionForTest(
		t, fixture.coordinator, newExecutionTestRuntime(client),
	)
	authority, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)

	require.NoError(t, authority.Deprovision(t.Context(), "lease-deprovision-closed-boundary"))
	assert.Equal(t, int32(1), calls.Load())
	assert.False(t, fixture.coordinator.RuntimeController().Contains("lease-deprovision-closed-boundary"))
}

func TestDeprovisionConcurrentCopyCannotInvokeTwice(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-deprovision-once")
	call, calling := fixture.coordinator.beginProvisionCall(fixture.dispatch)
	require.True(t, calling)
	require.True(t, fixture.coordinator.completeProvision(
		call, backend.ConservativeProvisionCallOutcome(nil),
	).Applied())
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	client := &executionTestBackend{name: "backend-a", deprovision: func(
		context.Context, string,
	) error {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return nil
	}}
	execution := bindExecutionForTest(
		t, fixture.coordinator, newExecutionTestRuntime(client),
	)
	authority, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)

	results := make(chan error, 2)
	go func() {
		results <- authority.Deprovision(t.Context(), "lease-deprovision-once")
	}()
	<-entered
	go func() {
		results <- authority.Deprovision(t.Context(), "lease-deprovision-once")
	}()
	second := <-results
	require.ErrorIs(t, second, ErrDeprovisionExecution)
	close(release)
	require.NoError(t, <-results)
	assert.Equal(t, int32(1), calls.Load())
}

func TestRestoreApplicationInvokesBackendOnceAcrossConcurrentCopies(t *testing.T) {
	const sourceLease = "71638ef8-1401-4f14-a355-1ae02afeb35b"
	const targetLease = "81638ef8-1401-4f14-a355-1ae02afeb35b"
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, store, "backend-a")
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{sourceLease: "backend-a"},
	})
	registry := operation.NewRegistry()
	base, err := newOperationCoordinator(store, registry)
	require.NoError(t, err)
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	client := &executionTestBackend{name: "backend-a", restore: func(
		context.Context, backend.RestoreRequest,
	) error {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return nil
	}}
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(client))
	reader := executionLeaseReaderFunc(func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
		state := billingtypes.LEASE_STATE_PENDING
		if leaseUUID == sourceLease {
			state = billingtypes.LEASE_STATE_CLOSED
		}
		return &billingtypes.Lease{
			Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
			State: state,
			Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
		}, nil
	})
	setProviderControlPlaneForTest(t, execution, reader)
	authority, err := execution.RestoreCoordinator(nil)
	require.NoError(t, err)
	request, err := NewRestoreApplicationRequest(targetLease, "tenant-test", sourceLease)
	require.NoError(t, err)

	results := make(chan RestoreApplicationResult, 2)
	go func() { results <- authority.ExecuteApplication(t.Context(), request) }()
	<-entered
	go func() { results <- authority.ExecuteApplication(t.Context(), request) }()
	second := <-results
	assert.Equal(t, RestoreApplicationAlreadyInProgress, second.Disposition())
	close(release)
	first := <-results
	assert.Equal(t, RestoreApplicationAccepted, first.Disposition(), first.Err())
	assert.Equal(t, int32(1), calls.Load())
}

func TestBackendRuntimeBindingIsOneShotAndPurposeFacetsHideBackendClients(t *testing.T) {
	routes := testCallbackRoutes(t)
	store := newTestStore(t, WithCallbackRouteFactory(routes))
	require.NoError(t, store.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a"}, testBackendStorageIDs("backend-a"),
	))
	base, err := NewOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	first := executionRuntime("backend-a")
	var typedNilRuntime *executionTestRuntime
	execution, err := base.BindBackendRuntime(typedNilRuntime, &testProviderControlPlane{
		provider: freshTestProviderUUID,
	})
	require.Error(t, err)
	require.Nil(t, execution)
	require.Nil(t, base.execution,
		"failed runtime validation must not publish a partial execution coordinator")

	var typedNilControlPlane *testProviderControlPlane
	execution, err = base.BindBackendRuntime(first, typedNilControlPlane)
	require.Error(t, err)
	require.Nil(t, execution)
	require.Nil(t, base.execution,
		"failed construction must not publish a partial execution coordinator")

	execution, err = base.BindBackendRuntime(first, &testProviderControlPlane{
		provider: freshTestProviderUUID,
	})
	require.NoError(t, err)
	require.True(t, execution.Valid())
	assert.Same(t, routes, execution.callbacks,
		"execution must derive the one Store-owned callback issuer")

	_, err = base.BindBackendRuntime(executionRuntime("backend-a"), &testProviderControlPlane{
		provider: freshTestProviderUUID,
	})
	require.ErrorContains(t, err, "already bound")

	for _, facet := range []reflect.Type{
		reflect.TypeOf((*ProvisionCoordinator)(nil)),
		reflect.TypeOf((*RestoreCoordinator)(nil)),
		reflect.TypeOf((*MaintenanceCoordinator)(nil)),
	} {
		for index := 0; index < facet.NumMethod(); index++ {
			method := facet.Method(index)
			for output := 0; output < method.Type.NumOut(); output++ {
				assert.False(t,
					method.Type.Out(output).Implements(reflect.TypeOf((*backend.Backend)(nil)).Elem()),
					"%s.%s exposes a mutation-capable backend", facet, method.Name,
				)
			}
		}
	}

	_, exportedComplete := reflect.TypeOf((*ProvisionCoordinator)(nil)).MethodByName("Complete")
	assert.False(t, exportedComplete)
	_, exportedCall := reflect.TypeOf((*ProvisionCoordinator)(nil)).MethodByName("BeginCall")
	assert.False(t, exportedCall)
	for _, methodName := range []string{
		"ProviderUUID", "ReadLease", "RouteProvision", "BackendNames",
		"DeprovisionExact", "Contains", "CountsByBackend", "TryClaimLeaseNow",
		"ReleaseLease", "TryInitiateProvisionClaimed", "TryInitiateRestoreClaimed",
		"AbortInitiation", "Lookup", "List",
		"CurrentAdmissionBaseline", "ScopeAdmission", "BeginNewAttempt",
		"BeginOwnedAttempt", "JoinDispatch", "Execute", "TryClaimDeprovision",
		"ReleaseDeprovision", "FinishDeprovision",
	} {
		_, exposed := reflect.TypeOf((*ProvisionCoordinator)(nil)).MethodByName(methodName)
		assert.False(t, exposed, "ProvisionCoordinator.%s bypasses the application boundary", methodName)
	}
	for _, methodName := range []string{
		"ProviderUUID", "ReadLease", "InspectProvision", "TryClaimLeaseNow",
		"ReleaseLease", "TryInitiateProvisionClaimed", "TryInitiateRestoreClaimed",
		"AbortInitiation", "Lookup",
		"CurrentAdmissionBaseline", "BeginAuthorizedRestore", "JoinDispatch", "Execute",
	} {
		_, exposed := reflect.TypeOf((*RestoreCoordinator)(nil)).MethodByName(methodName)
		assert.False(t, exposed, "RestoreCoordinator.%s bypasses the application boundary", methodName)
	}
}

func TestBackendRuntimeBindingRejectsStoreTopologyMismatchBeforePublication(t *testing.T) {
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, store.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a"}, testBackendStorageIDs("backend-a"),
	))
	base, err := NewOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	control := &testProviderControlPlane{provider: freshTestProviderUUID}

	execution, err := base.BindBackendRuntime(executionRuntime("backend-b"), control)
	require.ErrorIs(t, err, ErrInvalidBackendTopology)
	require.Nil(t, execution)
	require.Nil(t, base.execution,
		"a mismatched runtime must not publish any executable purpose facet")

	execution, err = base.BindBackendRuntime(executionRuntime("backend-a"), control)
	require.NoError(t, err)
	require.True(t, execution.Valid(),
		"failed validation must leave the one-shot binding available for the exact topology")
}
