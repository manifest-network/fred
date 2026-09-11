package provisioner

import (
	"context"
	"reflect"
	"sync"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

// mockBackendRouter is shared by white-box provisioner fixtures. Production
// construction accepts the concrete router and immediately seals it inside
// purpose-specific placement facets; tests retain this structural fake only to
// induce routing and topology failures.
type mockBackendRouter struct {
	routeFn                  func(sku string) backend.Backend
	routeForProvisionFn      func(ctx context.Context, sku string, inFlight map[string]int) backend.Backend
	routeForProvisionAmongFn func(ctx context.Context, sku string, eligible map[string]struct{}, inFlight map[string]int) backend.Backend
	getBackendByNameFn       func(name string) backend.Backend
	backendsFn               func() []backend.Backend
}

func (router *mockBackendRouter) Route(sku string) backend.Backend {
	if router != nil && router.routeFn != nil {
		return router.routeFn(sku)
	}
	return nil
}

func (router *mockBackendRouter) GetBackendByName(name string) backend.Backend {
	if router != nil && router.getBackendByNameFn != nil {
		return router.getBackendByNameFn(name)
	}
	return nil
}

func (router *mockBackendRouter) RouteForProvision(
	ctx context.Context,
	sku string,
	inFlight map[string]int,
) backend.Backend {
	if router != nil && router.routeForProvisionFn != nil {
		return router.routeForProvisionFn(ctx, sku, inFlight)
	}
	return router.Route(sku)
}

func (router *mockBackendRouter) RouteForProvisionAmong(
	ctx context.Context,
	sku string,
	eligible map[string]struct{},
	inFlight map[string]int,
) backend.Backend {
	if router != nil && router.routeForProvisionAmongFn != nil {
		return router.routeForProvisionAmongFn(ctx, sku, eligible, inFlight)
	}
	candidate := router.RouteForProvision(ctx, sku, inFlight)
	if candidate == nil {
		return nil
	}
	if _, allowed := eligible[candidate.Name()]; !allowed {
		return nil
	}
	return candidate
}

func (router *mockBackendRouter) Backends() []backend.Backend {
	if router != nil && router.backendsFn != nil {
		return router.backendsFn()
	}
	if candidate := router.Route(""); candidate != nil {
		return []backend.Backend{candidate}
	}
	return nil
}

func (router *mockBackendRouter) HasBackend(name string) bool {
	if name == "" {
		return false
	}
	if candidate := router.GetBackendByName(name); candidate != nil && candidate.Name() == name {
		return true
	}
	for _, candidate := range router.Backends() {
		if candidate != nil && candidate.Name() == name {
			return true
		}
	}
	return false
}

type orchestratorBackend struct {
	*backend.MockBackend
	mu               sync.Mutex
	provisionCalls   []backend.ProvisionRequest
	deprovisionCalls []string
}

func newOrchestratorBackend(name string) *orchestratorBackend {
	return &orchestratorBackend{MockBackend: backend.NewMockBackend(backend.MockBackendConfig{Name: name})}
}

func (client *orchestratorBackend) Provision(ctx context.Context, request backend.ProvisionRequest) error {
	client.mu.Lock()
	client.provisionCalls = append(client.provisionCalls, request)
	client.mu.Unlock()
	return client.MockBackend.Provision(ctx, request)
}

func (client *orchestratorBackend) Deprovision(ctx context.Context, leaseUUID string) error {
	client.mu.Lock()
	client.deprovisionCalls = append(client.deprovisionCalls, leaseUUID)
	client.mu.Unlock()
	return client.MockBackend.Deprovision(ctx, leaseUUID)
}

func newApplicationOrchestrator(
	t *testing.T,
	client *orchestratorBackend,
	getLease func(context.Context, string) (*billingtypes.Lease, error),
) (*ProvisionOrchestrator, operation.RuntimeController, *placement.Store) {
	t.Helper()
	router := &mockBackendRouter{
		routeFn: func(string) backend.Backend { return client },
		getBackendByNameFn: func(name string) backend.Backend {
			if name == client.Name() {
				return client
			}
			return nil
		},
		backendsFn: func() []backend.Backend { return []backend.Backend{client} },
	}
	store := newTestPlacementAuthority(t)
	armTestPlacementAdmission(t, store, router)
	base := bindTestOperationCoordinator(t, store)
	execution := bindTestBackendRuntime(t, base, router)
	reader := provisionLeaseReaderFunc(getLease)
	chain := testReconciliationChain{ProvisionLeaseReader: reader}
	bindTestReconciliationCoordinator(t, store, execution, chain, nil, nil)
	coordinator, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)
	orchestrator, err := NewProvisionOrchestrator(coordinator)
	require.NoError(t, err)
	return orchestrator, base.RuntimeController(), store
}

type provisionLeaseReaderFunc func(context.Context, string) (*billingtypes.Lease, error)

func (read provisionLeaseReaderFunc) GetLease(
	ctx context.Context,
	leaseUUID string,
) (*billingtypes.Lease, error) {
	return read(ctx, leaseUUID)
}

func TestProvisionOrchestratorExposesOnlyApplicationCapabilities(t *testing.T) {
	_, err := NewProvisionOrchestrator(nil)
	require.Error(t, err)
	_, err = NewProvisionOrchestrator(&placement.ProvisionCoordinator{})
	require.Error(t, err)

	typeOf := reflect.TypeOf((*HandlerEventCoordinator)(nil))
	for _, raw := range []string{
		"Route", "BeginAttempt", "JoinDispatch", "Execute", "TryClaimLeaseNow",
		"ReleaseLease", "Finish", "DeprovisionExact",
	} {
		_, exposed := typeOf.MethodByName(raw)
		assert.False(t, exposed, "HandlerEventCoordinator.%s bypasses the application boundary", raw)
	}
	_, provision := typeOf.MethodByName("startFromCurrentLease")
	assert.False(t, provision,
		"the package-private provision entrypoint must not be exported to arbitrary callers")
	_, deprovision := typeOf.MethodByName("Deprovision")
	assert.True(t, deprovision)
}

func TestHandlerEventCoordinatorProvisionsAndDeprovisionsThroughBoundApplication(t *testing.T) {
	const leaseUUID = "11638ef8-1401-4f14-a355-1ae02afeb35b"
	client := newOrchestratorBackend("backend-a")
	orchestrator, runtime, store := newApplicationOrchestrator(
		t, client,
		func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: leaseUUID, Tenant: "tenant-a", ProviderUuid: placementstore.ProviderUUID,
				State: billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-a", Quantity: 1}},
			}, nil
		},
	)
	events := orchestrator.HandlerEvents()
	require.True(t, events.Valid())
	request, err := placement.NewProvisionEventRequest(leaseUUID, "tenant-a")
	require.NoError(t, err)

	result := events.startFromCurrentLease(t.Context(), request)
	require.Equal(t, placement.ProvisionEventStarted, result.Disposition(), result.Err())
	client.mu.Lock()
	require.Len(t, client.provisionCalls, 1)
	provisionCall := client.provisionCalls[0]
	client.mu.Unlock()
	assert.Equal(t, leaseUUID, provisionCall.LeaseUUID)
	assert.Equal(t, placementstore.ProviderUUID, provisionCall.ProviderUUID)
	assert.NotEmpty(t, provisionCall.CallbackURL)
	assert.NotEmpty(t, provisionCall.LifecycleCallbackURL)
	assert.True(t, runtime.Contains(leaseUUID))
	assert.Equal(t, placement.StateConfirmed, store.Lookup(leaseUUID).State())

	require.NoError(t, events.Deprovision(t.Context(), leaseUUID))
	client.mu.Lock()
	assert.Equal(t, []string{leaseUUID}, client.deprovisionCalls)
	client.mu.Unlock()
	assert.False(t, runtime.Contains(leaseUUID))
}

func TestCapacityVerdictLabel(t *testing.T) {
	assert.Equal(t, metrics.CapacityVerdictCodedRefusal,
		capacityVerdictLabel(backend.ProvisionRefusalCapacity))
	assert.Equal(t, metrics.CapacityVerdictAmbiguous,
		capacityVerdictLabel(backend.ProvisionRefusalNone))
	assert.Equal(t, metrics.CapacityVerdictAmbiguous,
		capacityVerdictLabel(backend.ProvisionRefusalValidation))
}
