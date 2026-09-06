package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const (
	testProviderUUID = "44ed3e51-6912-4f6f-8f29-ac2fdd4455d4"
	testLeaseA       = "11638ef8-1401-4f14-a355-1ae02afeb35b"
	testLeaseB       = "21638ef8-1401-4f14-a355-1ae02afeb35b"
	testRequestA     = "550e8400-e29b-41d4-a716-446655440000"
	testRequestB     = "6ba7b811-9dad-41d1-80b4-00c04fd430c8"
	testStorageID    = "ca38f01d-80aa-4716-a289-16e39a7e9a23"
	testStorageIDB   = "72e7c97d-0602-4f85-86f4-5a9c48e0d733"
	testTenant       = "tenant-test"
)

type maintenanceSeedPlan struct {
	backendNames  []string
	inventories   map[string]placement.BackendInventory
	leaseBackends map[string]string
}

var maintenanceSeedPlans sync.Map

type maintenanceReconciliationChain struct {
	placement.MaintenanceLeaseReader
}

func (maintenanceReconciliationChain) GetPendingLeases(
	context.Context, string,
) ([]billingtypes.Lease, error) {
	return nil, nil
}

func (maintenanceReconciliationChain) GetActiveLeasesByProvider(
	context.Context, string,
) ([]billingtypes.Lease, error) {
	return nil, nil
}

func (maintenanceReconciliationChain) RejectLeases(
	context.Context, []string, string,
) (uint64, []string, error) {
	return 0, nil, nil
}

func (maintenanceReconciliationChain) CloseLeases(
	context.Context, []string, string,
) (uint64, []string, error) {
	return 0, nil, nil
}

func (maintenanceReconciliationChain) Acknowledge(
	context.Context, string,
) (bool, string, error) {
	return true, "", nil
}

// maintenanceBackendRuntime mirrors the private placement composition port.
// Production code never exports the broad mutation-capable backend runtime.
type maintenanceBackendRuntime interface {
	Route(string) backend.Backend
	RouteForProvision(context.Context, string, map[string]int) backend.Backend
	RouteForProvisionAmong(context.Context, string, map[string]struct{}, map[string]int) backend.Backend
	GetBackendByName(string) backend.Backend
	Backends() []backend.Backend
}

func maintenanceCoordinatorForTest(
	t *testing.T,
	authority interface {
		BindOperationCoordinator(func(int)) (*placement.OperationCoordinator, error)
	},
	leases placement.MaintenanceLeaseReader,
	backends maintenanceBackendRuntime,
	payloads placement.MaintenancePayloadPersister,
) *placement.MaintenanceCoordinator {
	t.Helper()
	result, _ := maintenanceCoordinatorWithRuntimeForTest(
		t, authority, leases, backends, payloads,
	)
	return result
}

func maintenanceCoordinatorWithRuntimeForTest(
	t *testing.T,
	authority interface {
		BindOperationCoordinator(func(int)) (*placement.OperationCoordinator, error)
	},
	leases placement.MaintenanceLeaseReader,
	backends maintenanceBackendRuntime,
	payloads placement.MaintenancePayloadPersister,
) (*placement.MaintenanceCoordinator, operation.RuntimeController) {
	t.Helper()
	coordinator, err := authority.BindOperationCoordinator(nil)
	require.NoError(t, err)
	inventoryRuntime := newMaintenanceInventoryRuntime(backends)
	control := maintenanceReconciliationChain{MaintenanceLeaseReader: leases}
	execution, err := coordinator.BindBackendRuntime(inventoryRuntime, control)
	require.NoError(t, err)
	if value, pending := maintenanceSeedPlans.Load(authority); pending {
		plan := value.(maintenanceSeedPlan)
		reconciliation, bindErr := execution.ReconciliationCoordinator(nil, nil)
		require.NoError(t, bindErr)
		maintenanceReconciliationInventories.Store(reconciliation, inventoryRuntime)
		t.Cleanup(func() { maintenanceReconciliationInventories.Delete(reconciliation) })
		projectMaintenanceSeed(t, reconciliation, plan)
		maintenanceSeedPlans.Delete(authority)
	}
	result, err := execution.MaintenanceCoordinator(payloads)
	require.NoError(t, err)
	require.True(t, result.Valid())
	return result, coordinator.RuntimeController()
}

type maintenanceInventoryBackend struct {
	backend.Backend
	mu         sync.Mutex
	storageID  backendidentity.ID
	provisions []backend.ProvisionInfo
	retentions []backend.RetainedLease
}

func (client *maintenanceInventoryBackend) stage(
	storageID backendidentity.ID,
	provisions []backend.ProvisionInfo,
	retentions []backend.RetainedLease,
) {
	client.mu.Lock()
	client.storageID = storageID
	client.provisions = append([]backend.ProvisionInfo(nil), provisions...)
	client.retentions = append([]backend.RetainedLease(nil), retentions...)
	client.mu.Unlock()
}

func (client *maintenanceInventoryBackend) ListProvisionsWithIdentity(
	context.Context,
) ([]backend.ProvisionInfo, backendidentity.ID, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	return append([]backend.ProvisionInfo(nil), client.provisions...), client.storageID, nil
}

func (client *maintenanceInventoryBackend) ListRetentionsWithIdentity(
	context.Context,
) ([]backend.RetainedLease, backendidentity.ID, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	return append([]backend.RetainedLease(nil), client.retentions...), client.storageID, nil
}

func (*maintenanceInventoryBackend) RefreshState(context.Context) error { return nil }

type maintenanceInventoryRuntime struct {
	runtime  maintenanceBackendRuntime
	backends map[string]*maintenanceInventoryBackend
}

func newMaintenanceInventoryRuntime(runtime maintenanceBackendRuntime) *maintenanceInventoryRuntime {
	result := &maintenanceInventoryRuntime{
		runtime: runtime, backends: make(map[string]*maintenanceInventoryBackend),
	}
	for _, client := range runtime.Backends() {
		if client != nil {
			result.backends[client.Name()] = &maintenanceInventoryBackend{Backend: client}
		}
	}
	return result
}

func (runtime *maintenanceInventoryRuntime) wrap(client backend.Backend) backend.Backend {
	if client == nil {
		return nil
	}
	if _, exactTransport := client.(*backend.HTTPClient); exactTransport {
		return client
	}
	wrapper := runtime.backends[client.Name()]
	if wrapper == nil {
		return nil
	}
	// The test router is intentionally mutable so recovery can exercise a
	// replacement backend process. Keep the inventory adapter bound to the
	// backend returned for this operation rather than the one present when the
	// coordinator was constructed.
	wrapper.Backend = client
	return wrapper
}

func (runtime *maintenanceInventoryRuntime) Route(sku string) backend.Backend {
	return runtime.wrap(runtime.runtime.Route(sku))
}

func (runtime *maintenanceInventoryRuntime) RouteForProvision(
	ctx context.Context, sku string, inFlight map[string]int,
) backend.Backend {
	return runtime.wrap(runtime.runtime.RouteForProvision(ctx, sku, inFlight))
}

func (runtime *maintenanceInventoryRuntime) RouteForProvisionAmong(
	ctx context.Context, sku string, eligible map[string]struct{}, inFlight map[string]int,
) backend.Backend {
	return runtime.wrap(runtime.runtime.RouteForProvisionAmong(ctx, sku, eligible, inFlight))
}

func (runtime *maintenanceInventoryRuntime) GetBackendByName(name string) backend.Backend {
	return runtime.wrap(runtime.runtime.GetBackendByName(name))
}

func (runtime *maintenanceInventoryRuntime) Backends() []backend.Backend {
	clients := runtime.runtime.Backends()
	result := make([]backend.Backend, 0, len(clients))
	for _, client := range clients {
		result = append(result, runtime.wrap(client))
	}
	return result
}

var maintenanceReconciliationInventories sync.Map

type emptyChainProof struct{}

func (emptyChainProof) Valid() bool             { return true }
func (emptyChainProof) ProviderUUID() string    { return testProviderUUID }
func (emptyChainProof) BlockHeight() int64      { return 1 }
func (emptyChainProof) TotalLeases() int        { return 0 }
func (emptyChainProof) BlockingLeaseCount() int { return 0 }

func newPlacementAuthority(t *testing.T, leases ...string) (*placement.Store, string) {
	return newPlacementAuthorityWithOptions(t, leases)
}

func reopenPlacementAuthority(t *testing.T, path string) *placement.Store {
	t.Helper()
	routes, err := placement.NewCallbackRouteFactory("https://provider.test")
	require.NoError(t, err)
	store, err := placement.OpenStore(
		path, testProviderUUID, placement.WithCallbackRouteFactory(routes),
	)
	require.NoError(t, err)
	return store
}

func newPlacementAuthorityWithOptions(
	t *testing.T,
	leases []string,
	options ...placement.Option,
) (*placement.Store, string) {
	t.Helper()
	leaseBackends := make(map[string]string, len(leases))
	for _, leaseUUID := range leases {
		leaseBackends[leaseUUID] = "backend-a"
	}
	return newPlacementAuthorityForTopology(t, leaseBackends, options...)
}

func newPlacementAuthorityForTopology(
	t *testing.T,
	leaseBackends map[string]string,
	options ...placement.Option,
) (*placement.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "placements.db")
	backendSet := make(map[string]struct{})
	for _, backendName := range leaseBackends {
		backendSet[backendName] = struct{}{}
	}
	if len(backendSet) == 0 {
		backendSet["backend-a"] = struct{}{}
	}
	backendNames := slices.Sorted(maps.Keys(backendSet))
	inventories := make(map[string]placement.BackendInventory, len(backendNames))
	for _, backendName := range backendNames {
		rawStorageID := testStorageID
		if backendName == "backend-b" {
			rawStorageID = testStorageIDB
		}
		storageID, err := backendidentity.Parse(rawStorageID)
		require.NoError(t, err)
		inventories[backendName] = placement.BackendInventory{
			StorageIdentity: storageID, Provisions: []string{},
			ProvisionProviderUUIDs: map[string]string{}, Retentions: []string{},
		}
	}
	target, err := placement.NewFreshInitializationTarget(
		path, testProviderUUID, backendNames,
	)
	require.NoError(t, err)
	chainProof, err := placement.NewFreshChainProof(emptyChainProof{})
	require.NoError(t, err)
	backendProof, err := placement.NewFreshBackendProof(
		backendNames, inventories,
	)
	require.NoError(t, err)
	quiescence, err := placement.ConfirmFreshQuiescence(target, target.Confirmation())
	require.NoError(t, err)
	proofCtx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	plan, err := placement.NewFreshInitializationPlan(
		proofCtx, target, chainProof, backendProof, quiescence,
	)
	require.NoError(t, err)
	require.NoError(t, placement.InitializeFreshStoreContext(t.Context(), plan))
	routes, err := placement.NewCallbackRouteFactory("https://provider.test")
	require.NoError(t, err)
	options = append(options, placement.WithCallbackRouteFactory(routes))
	store, err := placement.OpenStore(path, testProviderUUID, options...)
	require.NoError(t, err)
	maintenanceSeedPlans.Store(store, maintenanceSeedPlan{
		backendNames: slices.Clone(backendNames), inventories: inventories,
		leaseBackends: maps.Clone(leaseBackends),
	})
	t.Cleanup(func() { maintenanceSeedPlans.Delete(store) })
	return store, path
}

func projectMaintenanceSeed(
	t *testing.T,
	reconciliation *placement.ReconciliationCoordinator,
	plan maintenanceSeedPlan,
) {
	t.Helper()
	sweep, err := reconciliation.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	placements := make(map[string]string, len(plan.leaseBackends))
	reported := make(map[string][]backend.ProvisionInfo, len(plan.backendNames))
	for leaseUUID, backendName := range plan.leaseBackends {
		placements[leaseUUID] = backendName
		lifecycleID, parseErr := lifecycle.ParseID(leaseUUID)
		require.NoError(t, parseErr)
		reported[backendName] = append(reported[backendName],
			backend.ProvisionInfo{
				LeaseUUID: leaseUUID, ProviderUUID: testProviderUUID,
				Tenant: testTenant, BackendName: backendName,
				LifecycleGeneration: &backend.LifecycleGenerationObservation{
					Kind: backend.LifecycleGenerationTyped, ID: lifecycleID.String(),
				},
			},
		)
	}
	for _, backendName := range plan.backendNames {
		storageID := plan.inventories[backendName].StorageIdentity
		value, ok := maintenanceReconciliationInventories.Load(reconciliation)
		require.True(t, ok)
		client := value.(*maintenanceInventoryRuntime).backends[backendName]
		require.NotNil(t, client)
		client.stage(storageID, reported[backendName], nil)
		provisionReceipt, collectErr := sweep.CollectProvisionInventory(t.Context(), backendName)
		require.NoError(t, collectErr)
		retentionReceipt, collectErr := sweep.CollectRetentionInventory(t.Context(), backendName)
		require.NoError(t, collectErr)
		disposition, collectErr := sweep.RecordBackendInventory(
			provisionReceipt, retentionReceipt,
		)
		require.NoError(t, collectErr)
		require.Equal(t, placement.BackendInventoryAuthoritative, disposition)
	}
	require.NoError(t, sweep.SealInventory())
	_, err = sweep.Project(placement.ReconciliationProjection{Placements: placements})
	require.NoError(t, err)
}

type fakeChain struct {
	leases map[string]*billingtypes.Lease
}

func (chain *fakeChain) GetLease(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	lease := chain.leases[leaseUUID]
	if lease == nil {
		return nil, nil
	}
	copy := *lease
	return &copy, nil
}

type sequenceChain struct {
	mu     sync.Mutex
	leases []*billingtypes.Lease
}

func (chain *sequenceChain) GetLease(_ context.Context, _ string) (*billingtypes.Lease, error) {
	chain.mu.Lock()
	defer chain.mu.Unlock()
	if len(chain.leases) == 0 {
		return nil, nil
	}
	lease := chain.leases[0]
	if len(chain.leases) > 1 {
		chain.leases = chain.leases[1:]
	}
	copy := *lease
	return &copy, nil
}

type fakeBackend struct {
	backend.Backend
	name         string
	mu           sync.Mutex
	restartCalls []backend.RestartRequest
	updateCalls  []backend.UpdateRequest
	restart      func(backend.RestartRequest) error
	update       func(backend.UpdateRequest) error
}

func (fake *fakeBackend) Name() string {
	if fake.name != "" {
		return fake.name
	}
	return "backend-a"
}

func (fake *fakeBackend) Restart(_ context.Context, request backend.RestartRequest) error {
	fake.mu.Lock()
	fake.restartCalls = append(fake.restartCalls, request)
	callback := fake.restart
	fake.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil
}

func (fake *fakeBackend) Update(_ context.Context, request backend.UpdateRequest) error {
	fake.mu.Lock()
	request.Payload = append([]byte(nil), request.Payload...)
	fake.updateCalls = append(fake.updateCalls, request)
	callback := fake.update
	fake.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return nil
}

func (fake *fakeBackend) restartCount() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return len(fake.restartCalls)
}

func (fake *fakeBackend) updateCount() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return len(fake.updateCalls)
}

func (fake *fakeBackend) restartedLeases() []string {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	result := make([]string, 0, len(fake.restartCalls))
	for _, request := range fake.restartCalls {
		result = append(result, request.LeaseUUID)
	}
	return result
}

func causalMaintenanceBackendForTest(
	t *testing.T,
	store *placement.Store,
	status int,
	body string,
) (backend.Backend, func() int) {
	t.Helper()
	storageID, bound := store.ExpectedBackendStorageIdentity("backend-a")
	require.True(t, bound)
	var calls atomic.Int32
	planValue, planned := maintenanceSeedPlans.Load(store)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set(backendidentity.ResponseHeader, storageID.String())
		switch request.URL.Path {
		case "/provisions":
			provisions := make([]backend.ProvisionInfo, 0)
			if planned {
				plan := planValue.(maintenanceSeedPlan)
				for leaseUUID, backendName := range plan.leaseBackends {
					if backendName != "backend-a" {
						continue
					}
					lifecycleID, err := lifecycle.ParseID(leaseUUID)
					require.NoError(t, err)
					provisions = append(provisions, backend.ProvisionInfo{
						LeaseUUID: leaseUUID, BackendName: backendName,
						ProviderUUID: testProviderUUID, Tenant: testTenant,
						LifecycleGeneration: &backend.LifecycleGenerationObservation{
							Kind: backend.LifecycleGenerationTyped, ID: lifecycleID.String(),
						},
					})
				}
			}
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(backend.ListProvisionsResponse{
				Provisions: provisions,
			}))
			return
		case "/retentions":
			w.Header().Set("Content-Type", "application/json")
			require.NoError(t, json.NewEncoder(w).Encode(backend.ListRetentionsResponse{
				Retentions: []backend.RetainedLease{},
			}))
			return
		}
		calls.Add(1)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	client, err := backend.NewIdentityBoundHTTPClient(backend.HTTPClientConfig{
		Name: "backend-a", BaseURL: server.URL,
		Secret: "maintenance-causal-outcome-test-key",
	}, store)
	require.NoError(t, err)
	return client, func() int { return int(calls.Load()) }
}

type fakeRouter struct {
	backend  backend.Backend
	backends map[string]backend.Backend
}

func (router fakeRouter) GetBackendByName(name string) backend.Backend {
	if router.backends != nil {
		return router.backends[name]
	}
	if name != "backend-a" {
		return nil
	}
	return router.backend
}

func (router fakeRouter) Backends() []backend.Backend {
	if router.backends != nil {
		result := make([]backend.Backend, 0, len(router.backends))
		for _, client := range router.backends {
			result = append(result, client)
		}
		return result
	}
	if router.backend == nil {
		return nil
	}
	return []backend.Backend{router.backend}
}

func (router fakeRouter) Route(string) backend.Backend { return router.backend }
func (router fakeRouter) RouteForProvision(context.Context, string, map[string]int) backend.Backend {
	return router.backend
}
func (router fakeRouter) RouteForProvisionAmong(
	_ context.Context, _ string, eligible map[string]struct{}, _ map[string]int,
) backend.Backend {
	for name := range eligible {
		if client := router.GetBackendByName(name); client != nil {
			return client
		}
	}
	return nil
}

type fakePayloads struct {
	mu       sync.Mutex
	failures int
	writes   [][]byte
}

type contextBlockingBackend struct {
	backend.Backend
	name    string
	mu      sync.Mutex
	calls   int
	entered chan struct{}
	once    sync.Once
}

type perLeaseBlockingBackend struct {
	backend.Backend
	name  string
	mu    sync.Mutex
	calls []string
}

func (fake *perLeaseBlockingBackend) Name() string { return fake.name }

func (fake *perLeaseBlockingBackend) Restart(
	ctx context.Context,
	request backend.RestartRequest,
) error {
	fake.mu.Lock()
	fake.calls = append(fake.calls, request.LeaseUUID)
	fake.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (fake *perLeaseBlockingBackend) calledLeases() []string {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return slices.Clone(fake.calls)
}

func (fake *contextBlockingBackend) Name() string { return fake.name }

func (fake *contextBlockingBackend) Restart(ctx context.Context, _ backend.RestartRequest) error {
	fake.mu.Lock()
	fake.calls++
	fake.mu.Unlock()
	fake.once.Do(func() { close(fake.entered) })
	<-ctx.Done()
	return ctx.Err()
}

func (fake *contextBlockingBackend) restartCount() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.calls
}

type chainFunc func(context.Context, string) (*billingtypes.Lease, error)

func (function chainFunc) GetLease(
	ctx context.Context,
	leaseUUID string,
) (*billingtypes.Lease, error) {
	return function(ctx, leaseUUID)
}

type recordingOrderedEvents struct {
	called   bool
	calls    int
	start    backend.LeaseStatusEvent
	accepted bool
	err      error
}

func (events *recordingOrderedEvents) DispatchWithOrderedSettlement(
	start backend.LeaseStatusEvent,
	dispatch func() (bool, error),
) (bool, error) {
	events.called = true
	events.calls++
	events.start = start
	events.accepted, events.err = dispatch()
	return events.accepted, events.err
}

func (payloads *fakePayloads) OverwritePayload(_ string, payload []byte) error {
	payloads.mu.Lock()
	defer payloads.mu.Unlock()
	if payloads.failures > 0 {
		payloads.failures--
		return errors.New("injected payload write failure")
	}
	payloads.writes = append(payloads.writes, append([]byte(nil), payload...))
	return nil
}

func (payloads *fakePayloads) writeCount() int {
	payloads.mu.Lock()
	defer payloads.mu.Unlock()
	return len(payloads.writes)
}

func (payloads *fakePayloads) lastWrite() []byte {
	payloads.mu.Lock()
	defer payloads.mu.Unlock()
	if len(payloads.writes) == 0 {
		return nil
	}
	return append([]byte(nil), payloads.writes[len(payloads.writes)-1]...)
}

func testChain(leases ...string) *fakeChain {
	result := &fakeChain{leases: make(map[string]*billingtypes.Lease, len(leases))}
	for _, leaseUUID := range leases {
		result.leases[leaseUUID] = &billingtypes.Lease{
			Uuid: leaseUUID, Tenant: testTenant, ProviderUuid: testProviderUUID,
			State: billingtypes.LEASE_STATE_ACTIVE,
		}
	}
	return result
}

func newTestService(
	t *testing.T,
	store *placement.Store,
	backendClient backend.Backend,
	payloads placement.MaintenancePayloadPersister,
	leases ...string,
) *Service {
	t.Helper()
	service, _ := newTestServiceWithRuntime(
		t, store, backendClient, payloads, leases...,
	)
	return service
}

func newTestServiceWithRuntime(
	t *testing.T,
	store *placement.Store,
	backendClient backend.Backend,
	payloads placement.MaintenancePayloadPersister,
	leases ...string,
) (*Service, operation.RuntimeController) {
	t.Helper()
	chain := testChain(leases...)
	coordinator, runtime := maintenanceCoordinatorWithRuntimeForTest(
		t, store, chain, fakeRouter{backend: backendClient}, payloads,
	)
	service, err := NewService(Config{Coordinator: coordinator})
	require.NoError(t, err)
	return service, runtime
}

func assertMaintenanceLaneHeld(t *testing.T, runtime operation.RuntimeController, leaseUUID string) {
	t.Helper()
	assert.Contains(t, runtime.PendingLeaseUUIDs(), leaseUUID)
}

func assertMaintenanceLaneReleased(t *testing.T, runtime operation.RuntimeController, leaseUUID string) {
	t.Helper()
	assert.NotContains(t, runtime.PendingLeaseUUIDs(), leaseUUID)
}

func requestID(t *testing.T, value string) maintenanceid.ID {
	t.Helper()
	id, err := maintenanceid.Parse(value)
	require.NoError(t, err)
	return id
}

func TestNewServiceRejectsMissingAndTypedNilRequiredDependencies(t *testing.T) {
	valid := func() Config {
		store, _ := newPlacementAuthority(t)
		return Config{
			Coordinator: maintenanceCoordinatorForTest(
				t, store, testChain(),
				fakeRouter{backend: &fakeBackend{}}, nil,
			),
		}
	}
	var typedNilCoordinator *placement.MaintenanceCoordinator
	tests := map[string]func(*Config){
		"coordinator": func(config *Config) { config.Coordinator = nil },
		"typed-nil coordinator": func(config *Config) {
			config.Coordinator = typedNilCoordinator
		},
		"negative recovery timeout": func(config *Config) {
			config.RecoveryTimeout = -time.Nanosecond
		},
	}
	for name, invalidate := range tests {
		t.Run(name, func(t *testing.T) {
			config := valid()
			invalidate(&config)
			service, err := NewService(config)
			assert.Nil(t, service)
			require.Error(t, err)
		})
	}
}

func TestConstructionRejectsRuntimeWhoseNamesDifferFromDurableTopology(t *testing.T) {
	store, _ := newPlacementAuthority(t, testLeaseA)
	coordinator, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	resolved := &fakeBackend{name: "backend-b"}
	execution, err := coordinator.BindBackendRuntime(fakeRouter{backend: resolved},
		maintenanceReconciliationChain{MaintenanceLeaseReader: testChain(testLeaseA)},
	)
	require.ErrorIs(t, err, placement.ErrInvalidBackendTopology)
	assert.Nil(t, execution, "atomic binding cannot publish a partially authorized runtime")
	assert.Zero(t, resolved.restartCount())
}

func TestExecuteRequiresCurrentChainAuthorityBeforeAdmission(t *testing.T) {
	activeLease := func() *billingtypes.Lease {
		return &billingtypes.Lease{
			Uuid: testLeaseA, Tenant: testTenant, ProviderUuid: testProviderUUID,
			State: billingtypes.LEASE_STATE_ACTIVE,
		}
	}
	tests := map[string]struct {
		chain       placement.MaintenanceLeaseReader
		wantOutcome Outcome
	}{
		"chain read failure": {
			chain: chainFunc(func(context.Context, string) (*billingtypes.Lease, error) {
				return nil, errors.New("chain unavailable")
			}),
			wantOutcome: OutcomeServiceUnavailable,
		},
		"lease absent": {
			chain: chainFunc(func(context.Context, string) (*billingtypes.Lease, error) {
				return nil, billingtypes.ErrLeaseNotFound
			}),
			wantOutcome: OutcomeServiceUnavailable,
		},
		"ambiguous empty response": {
			chain: chainFunc(func(context.Context, string) (*billingtypes.Lease, error) {
				return nil, nil
			}),
			wantOutcome: OutcomeServiceUnavailable,
		},
		"mismatched lease identity": {
			chain: chainFunc(func(context.Context, string) (*billingtypes.Lease, error) {
				lease := activeLease()
				lease.Uuid = testLeaseB
				return lease, nil
			}),
			wantOutcome: OutcomeServiceUnavailable,
		},
		"different tenant": {
			chain: chainFunc(func(context.Context, string) (*billingtypes.Lease, error) {
				lease := activeLease()
				lease.Tenant = "different-tenant"
				return lease, nil
			}),
			wantOutcome: OutcomeForbidden,
		},
		"different provider": {
			chain: chainFunc(func(context.Context, string) (*billingtypes.Lease, error) {
				lease := activeLease()
				lease.ProviderUuid = "different-provider"
				return lease, nil
			}),
			wantOutcome: OutcomeForbidden,
		},
		"lease no longer active": {
			chain: chainFunc(func(context.Context, string) (*billingtypes.Lease, error) {
				lease := activeLease()
				lease.State = billingtypes.LEASE_STATE_CLOSED
				return lease, nil
			}),
			wantOutcome: OutcomeNoLongerActive,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			store, _ := newPlacementAuthority(t, testLeaseA)
			backendClient := &fakeBackend{}
			service, err := NewService(Config{
				Coordinator: maintenanceCoordinatorForTest(
					t, store, test.chain,
					fakeRouter{backend: backendClient}, nil,
				),
			})
			require.NoError(t, err)
			id := requestID(t, testRequestA)

			result := service.Execute(t.Context(), Command{
				ID: id, LeaseUUID: testLeaseA, Tenant: testTenant, Kind: KindRestart,
			})
			assert.Equal(t, test.wantOutcome, result.Outcome())
			assert.Zero(t, backendClient.restartCount())
			_, found, lookupErr := store.LookupMaintenanceCommand(testLeaseA, id)
			require.NoError(t, lookupErr)
			assert.False(t, found, "failed chain authorization must not create durable intent")
		})
	}
}

func TestUnauthorizedColdCommandNeverAcquiresVictimLifecycleLane(t *testing.T) {
	store, _ := newPlacementAuthority(t, testLeaseA)
	chainReadStarted := make(chan struct{})
	releaseChainRead := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseChainRead) }) })
	chain := chainFunc(func(context.Context, string) (*billingtypes.Lease, error) {
		close(chainReadStarted)
		<-releaseChainRead
		return &billingtypes.Lease{
			Uuid: testLeaseA, Tenant: testTenant, ProviderUuid: testProviderUUID,
			State: billingtypes.LEASE_STATE_ACTIVE,
		}, nil
	})
	coordinator, runtime := maintenanceCoordinatorWithRuntimeForTest(
		t, store, chain, fakeRouter{backend: &fakeBackend{}}, nil,
	)
	service, err := NewService(Config{Coordinator: coordinator})
	require.NoError(t, err)
	id := requestID(t, testRequestA)

	result := make(chan Result, 1)
	go func() {
		result <- service.Execute(t.Context(), Command{
			ID: id, LeaseUUID: testLeaseA,
			Tenant: "attacker-tenant", Kind: KindRestart,
		})
	}()
	select {
	case <-chainReadStarted:
	case <-time.After(time.Second):
		t.Fatal("unauthorized command did not reach chain authorization")
	}

	assertMaintenanceLaneReleased(t, runtime, testLeaseA)
	releaseOnce.Do(func() { close(releaseChainRead) })

	select {
	case got := <-result:
		assert.Equal(t, OutcomeForbidden, got.Outcome())
	case <-time.After(time.Second):
		t.Fatal("unauthorized command did not return after chain authorization")
	}
	_, found, lookupErr := store.LookupMaintenanceCommand(testLeaseA, id)
	require.NoError(t, lookupErr)
	assert.False(t, found, "unauthorized input must not create durable intent")
}

func TestConcurrentColdExecuteCoalescesExactCommand(t *testing.T) {
	store, _ := newPlacementAuthority(t, testLeaseA)
	entered := make(chan struct{})
	release := make(chan struct{})
	var (
		enteredOnce sync.Once
		releaseOnce sync.Once
	)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	backendClient := &fakeBackend{restart: func(backend.RestartRequest) error {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return nil
	}}
	service := newTestService(
		t, store, backendClient, nil, testLeaseA,
	)
	command := Command{
		ID: requestID(t, testRequestA), LeaseUUID: testLeaseA,
		Tenant: testTenant, Kind: KindRestart,
	}

	firstDone := make(chan Result, 1)
	go func() { firstDone <- service.Execute(t.Context(), command) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first cold execution did not reach the backend")
	}

	secondStarted := make(chan struct{})
	secondDone := make(chan Result, 1)
	go func() {
		close(secondStarted)
		secondDone <- service.Execute(t.Context(), command)
	}()
	<-secondStarted
	require.Never(t, func() bool {
		return backendClient.restartCount() > 1
	}, 50*time.Millisecond, time.Millisecond,
		"an exact concurrent retry must share the admitting command's dispatch lane")

	releaseOnce.Do(func() { close(release) })
	assert.Equal(t, OutcomeAccepted, (<-firstDone).Outcome())
	assert.Equal(t, OutcomeAccepted, (<-secondDone).Outcome())
	assert.Equal(t, 1, backendClient.restartCount(),
		"an exact concurrent retry must replay terminal state without redispatch")
}

func TestConcurrentColdExecuteRejectsDivergentCommandOnSameLease(t *testing.T) {
	store, _ := newPlacementAuthority(t, testLeaseA)
	entered := make(chan struct{})
	release := make(chan struct{})
	var (
		enteredOnce sync.Once
		releaseOnce sync.Once
	)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	backendClient := &fakeBackend{restart: func(backend.RestartRequest) error {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return nil
	}}
	service := newTestService(
		t, store, backendClient, nil, testLeaseA,
	)
	firstID := requestID(t, testRequestA)
	secondID := requestID(t, testRequestB)

	firstDone := make(chan Result, 1)
	go func() {
		firstDone <- service.Execute(t.Context(), Command{
			ID: firstID, LeaseUUID: testLeaseA,
			Tenant: testTenant, Kind: KindRestart,
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first cold execution did not reach the backend")
	}

	divergentDone := make(chan Result, 1)
	go func() {
		divergentDone <- service.Execute(t.Context(), Command{
			ID: secondID, LeaseUUID: testLeaseA,
			Tenant: testTenant, Kind: KindRestart,
		})
	}()
	select {
	case result := <-divergentDone:
		assert.Equal(t, OutcomeAlreadyInProgress, result.Outcome())
	case <-time.After(time.Second):
		t.Fatal("divergent command waited behind work it cannot own")
	}
	assert.Equal(t, 1, backendClient.restartCount())

	releaseOnce.Do(func() { close(release) })
	assert.Equal(t, OutcomeAccepted, (<-firstDone).Outcome())
}

func TestConcurrentColdExecuteAllowsUnrelatedBackendToProgress(t *testing.T) {
	store, _ := newPlacementAuthorityForTopology(t, map[string]string{
		testLeaseA: "backend-a",
		testLeaseB: "backend-b",
	})
	entered := make(chan struct{})
	release := make(chan struct{})
	var (
		enteredOnce sync.Once
		releaseOnce sync.Once
	)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	blocked := &fakeBackend{name: "backend-a", restart: func(backend.RestartRequest) error {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return nil
	}}
	healthy := &fakeBackend{name: "backend-b"}
	chain := testChain(testLeaseA, testLeaseB)
	service, err := NewService(Config{
		Coordinator: maintenanceCoordinatorForTest(
			t, store, chain,
			fakeRouter{backends: map[string]backend.Backend{
				"backend-a": blocked,
				"backend-b": healthy,
			}}, nil,
		),
	})
	require.NoError(t, err)
	request := requestID(t, testRequestA)

	blockedDone := make(chan Result, 1)
	go func() {
		blockedDone <- service.Execute(t.Context(), Command{
			ID: request, LeaseUUID: testLeaseA,
			Tenant: testTenant, Kind: KindRestart,
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first cold execution did not reach its backend")
	}

	healthyDone := make(chan Result, 1)
	go func() {
		healthyDone <- service.Execute(t.Context(), Command{
			ID: request, LeaseUUID: testLeaseB,
			Tenant: testTenant, Kind: KindRestart,
		})
	}()
	select {
	case result := <-healthyDone:
		assert.Equal(t, OutcomeAccepted, result.Outcome())
	case <-time.After(time.Second):
		t.Fatal("an unrelated lease/backend was stalled by another command")
	}
	assert.Equal(t, 1, healthy.restartCount())
	assert.Equal(t, 1, blocked.restartCount())

	releaseOnce.Do(func() { close(release) })
	assert.Equal(t, OutcomeAccepted, (<-blockedDone).Outcome())
}

func TestAmbiguousRestartRehydratesClaimAndRecoversAfterProviderRestart(t *testing.T) {
	store, path := newPlacementAuthority(t, testLeaseA)
	firstBackend := &fakeBackend{restart: func(backend.RestartRequest) error {
		return errors.New("transport outcome unknown")
	}}
	service, firstRuntime := newTestServiceWithRuntime(
		t, store, firstBackend, nil, testLeaseA,
	)
	id := requestID(t, testRequestA)

	result := service.Execute(t.Context(), Command{
		ID: id, LeaseUUID: testLeaseA, Tenant: testTenant, Kind: KindRestart,
	})
	assert.Equal(t, OutcomeServiceUnavailable, result.Outcome())
	forbidden := service.Execute(t.Context(), Command{
		ID: id, LeaseUUID: testLeaseA, Tenant: "different-tenant", Kind: KindRestart,
	})
	assert.Equal(t, OutcomeForbidden, forbidden.Outcome(),
		"a pending retry is authorized by its immutable stored tenant")
	conflict := service.Execute(t.Context(), Command{
		ID: id, LeaseUUID: testLeaseA, Tenant: testTenant,
		Kind: KindUpdate, Payload: []byte("different command"),
	})
	assert.Equal(t, OutcomeCommandConflict, conflict.Outcome(),
		"the same ID cannot be rebound to another kind or payload")
	overtake := service.Execute(t.Context(), Command{
		ID: requestID(t, testRequestB), LeaseUUID: testLeaseA,
		Tenant: testTenant, Kind: KindRestart,
	})
	assert.Equal(t, OutcomeAlreadyInProgress, overtake.Outcome(),
		"a different ID cannot overtake a durable pending command")
	assert.Equal(t, 1, firstBackend.restartCount())
	assertMaintenanceLaneHeld(t, firstRuntime, testLeaseA)
	require.NoError(t, store.Close())

	reopened := reopenPlacementAuthority(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	secondBackend := &fakeBackend{}
	recovered, secondRuntime := newTestServiceWithRuntime(
		t, reopened, secondBackend, nil, testLeaseA,
	)
	assertMaintenanceLaneHeld(t, secondRuntime, testLeaseA)
	require.NoError(t, recovered.RecoverPending(t.Context()))
	assert.Equal(t, 1, secondBackend.restartCount())
	assertMaintenanceLaneReleased(t, secondRuntime, testLeaseA)

	replay := recovered.Execute(t.Context(), Command{
		ID: id, LeaseUUID: testLeaseA, Tenant: testTenant, Kind: KindRestart,
	})
	assert.Equal(t, OutcomeAccepted, replay.Outcome())
	assert.Equal(t, 1, secondBackend.restartCount(), "terminal replay cannot dispatch again")
}

func TestBackendMaintenancePanicIsAmbiguousAndRetainsPendingCommand(t *testing.T) {
	for name, kind := range map[string]Kind{
		"restart": KindRestart,
		"update":  KindUpdate,
	} {
		t.Run(name, func(t *testing.T) {
			store, _ := newPlacementAuthority(t, testLeaseA)
			backendClient := &fakeBackend{
				restart: func(backend.RestartRequest) error { panic(backend.ErrValidation) },
				update:  func(backend.UpdateRequest) error { panic("update implementation fault") },
			}
			payloads := &fakePayloads{}
			service, runtime := newTestServiceWithRuntime(
				t, store, backendClient, payloads, testLeaseA,
			)
			id := requestID(t, testRequestA)
			command := Command{
				ID: id, LeaseUUID: testLeaseA, Tenant: testTenant, Kind: kind,
			}
			if kind == KindUpdate {
				command.Payload = []byte("services: {app: {image: replacement}}")
			}

			result := service.Execute(t.Context(), command)
			assert.Equal(t, OutcomeServiceUnavailable, result.Outcome())
			require.ErrorContains(t, result.Cause(), "panicked")
			record, found, err := store.LookupMaintenanceCommand(testLeaseA, id)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, placement.MaintenanceOutcomePending, record.Outcome(),
				"a panic cannot prove whether the backend applied its physical effect, even when its value is a definitive error")
			assertMaintenanceLaneHeld(t, runtime, testLeaseA)
			assert.Zero(t, payloads.writeCount(),
				"an update panic is not backend acceptance and cannot publish its payload")
		})
	}
}

func TestRecoveryBackendPanicCannotBlockHealthyBackendLane(t *testing.T) {
	store, _ := newPlacementAuthorityForTopology(t, map[string]string{
		testLeaseA: "backend-a",
		testLeaseB: "backend-b",
	})
	router := fakeRouter{backends: map[string]backend.Backend{
		"backend-a": &fakeBackend{name: "backend-a", restart: func(backend.RestartRequest) error {
			return errors.New("initial ambiguous backend-a result")
		}},
		"backend-b": &fakeBackend{name: "backend-b", restart: func(backend.RestartRequest) error {
			return errors.New("initial ambiguous backend-b result")
		}},
	}}
	chain := testChain(testLeaseA, testLeaseB)
	coordinator, runtime := maintenanceCoordinatorWithRuntimeForTest(
		t, store, chain, router, nil,
	)
	service, err := NewService(Config{Coordinator: coordinator})
	require.NoError(t, err)
	idA := requestID(t, testRequestA)
	idB := requestID(t, testRequestB)
	for _, command := range []Command{
		{ID: idA, LeaseUUID: testLeaseA, Tenant: testTenant, Kind: KindRestart},
		{ID: idB, LeaseUUID: testLeaseB, Tenant: testTenant, Kind: KindRestart},
	} {
		require.Equal(t, OutcomeServiceUnavailable,
			service.Execute(t.Context(), command).Outcome())
	}

	panicking := &fakeBackend{name: "backend-a", restart: func(backend.RestartRequest) error {
		panic("recovery backend implementation fault")
	}}
	healthy := &fakeBackend{name: "backend-b"}
	router.backends["backend-a"] = panicking
	router.backends["backend-b"] = healthy

	recoveryErr := service.RecoverPending(t.Context())
	require.ErrorContains(t, recoveryErr, "panicked")
	assert.Equal(t, 1, panicking.restartCount())
	assert.Equal(t, 1, healthy.restartCount(),
		"a panicking backend lane must not suppress an independent healthy lane")

	panickingRecord, found, err := store.LookupMaintenanceCommand(testLeaseA, idA)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, placement.MaintenanceOutcomePending, panickingRecord.Outcome())
	healthyRecord, found, err := store.LookupMaintenanceCommand(testLeaseB, idB)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, placement.MaintenanceOutcomeAccepted, healthyRecord.Outcome())
	assertMaintenanceLaneHeld(t, runtime, testLeaseA)
	assertMaintenanceLaneReleased(t, runtime, testLeaseB)
}

func TestPendingBackendOutageFencesOnlyItsLease(t *testing.T) {
	store, path := newPlacementAuthorityForTopology(t, map[string]string{
		testLeaseA: "backend-a",
		testLeaseB: "backend-b",
	})
	downBackend := &fakeBackend{name: "backend-a", restart: func(backend.RestartRequest) error {
		return errors.New("backend-a node transiently unavailable")
	}}
	firstChain := testChain(testLeaseA, testLeaseB)
	first, err := NewService(Config{
		Coordinator: maintenanceCoordinatorForTest(
			t, store, firstChain,
			fakeRouter{backends: map[string]backend.Backend{
				"backend-a": downBackend,
				"backend-b": &fakeBackend{name: "backend-b"},
			}}, nil,
		),
	})
	require.NoError(t, err)

	result := first.Execute(t.Context(), Command{
		ID: requestID(t, testRequestA), LeaseUUID: testLeaseA,
		Tenant: testTenant, Kind: KindRestart,
	})
	require.Equal(t, OutcomeServiceUnavailable, result.Outcome())
	require.NoError(t, store.Close())

	reopened := reopenPlacementAuthority(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	availableBackend := &fakeBackend{name: "backend-b"}
	restartChain := testChain(testLeaseA, testLeaseB)
	restartCoordinator, restartRuntime := maintenanceCoordinatorWithRuntimeForTest(
		t, reopened, restartChain,
		fakeRouter{backends: map[string]backend.Backend{
			"backend-a": downBackend,
			"backend-b": availableBackend,
		}}, nil,
	)
	restarted, err := NewService(Config{Coordinator: restartCoordinator})
	require.NoError(t, err, "startup must rehydrate claims without contacting the down node")
	require.Error(t, restarted.RecoverPending(t.Context()),
		"the pinned command remains pending while backend-a is unavailable")
	second := restarted.Execute(t.Context(), Command{
		ID: requestID(t, testRequestA), LeaseUUID: testLeaseB,
		Tenant: testTenant, Kind: KindRestart,
	})
	assert.Equal(t, OutcomeAccepted, second.Outcome(),
		"a down node must not block another backend; IDs are lease-scoped")
	assert.Equal(t, 2, downBackend.restartCount(), "recovery retries only the pinned node")
	assert.Equal(t, 1, availableBackend.restartCount())
	assertMaintenanceLaneHeld(t, restartRuntime, testLeaseA)
	assertMaintenanceLaneReleased(t, restartRuntime, testLeaseB)
}

func TestRecoveryPreservesAmbiguousCommandWhenChainTemporarilyOmitsLease(t *testing.T) {
	store, _ := newPlacementAuthority(t, testLeaseA)
	chain := testChain(testLeaseA)
	backendClient := &fakeBackend{restart: func(backend.RestartRequest) error {
		return errors.New("backend response lost after possible restart")
	}}
	coordinator, runtime := maintenanceCoordinatorWithRuntimeForTest(
		t, store, chain, fakeRouter{backend: backendClient}, nil,
	)
	service, err := NewService(Config{Coordinator: coordinator})
	require.NoError(t, err)
	id := requestID(t, testRequestA)
	result := service.Execute(t.Context(), Command{
		ID: id, LeaseUUID: testLeaseA, Tenant: testTenant, Kind: KindRestart,
	})
	require.Equal(t, OutcomeServiceUnavailable, result.Outcome())
	require.Equal(t, 1, backendClient.restartCount())

	delete(chain.leases, testLeaseA)
	require.Error(t, service.RecoverPending(t.Context()))
	assert.Equal(t, 1, backendClient.restartCount(),
		"an absent chain read is uncertainty, not authority to retry or terminalize")
	record, found, err := store.LookupMaintenanceCommand(testLeaseA, id)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, placement.MaintenanceOutcomePending, record.Outcome())
	assertMaintenanceLaneHeld(t, runtime, testLeaseA)
}

func TestRecoveryTimeoutBoundsContextAwareBackendAndRetainsPendingFence(t *testing.T) {
	store, path := newPlacementAuthority(t, testLeaseA)
	first := newTestService(t, store, &fakeBackend{
		restart: func(backend.RestartRequest) error {
			return errors.New("initial ambiguous transport result")
		},
	}, nil, testLeaseA)
	id := requestID(t, testRequestA)
	command := Command{
		ID: id, LeaseUUID: testLeaseA, Tenant: testTenant, Kind: KindRestart,
	}
	require.Equal(t, OutcomeServiceUnavailable, first.Execute(t.Context(), command).Outcome())
	require.NoError(t, store.Close())

	reopened := reopenPlacementAuthority(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	blocking := &contextBlockingBackend{name: "backend-a", entered: make(chan struct{})}
	chain := testChain(testLeaseA)
	coordinator, runtime := maintenanceCoordinatorWithRuntimeForTest(
		t, reopened, chain, fakeRouter{backend: blocking}, nil,
	)
	recovered, err := NewService(Config{
		Coordinator:     coordinator,
		RecoveryTimeout: 100 * time.Millisecond,
	})
	require.NoError(t, err)

	recoveryDone := make(chan error, 1)
	go func() { recoveryDone <- recovered.RecoverPending(t.Context()) }()
	select {
	case <-blocking.entered:
	case <-time.After(time.Second):
		t.Fatal("recovery did not dispatch the retained command")
	}
	select {
	case recoveryErr := <-recoveryDone:
		require.ErrorIs(t, recoveryErr, context.DeadlineExceeded)
	case <-time.After(2 * time.Second):
		t.Fatal("configured recovery timeout did not bound a context-aware backend")
	}
	assert.Equal(t, 1, blocking.restartCount())
	record, found, lookupErr := reopened.LookupMaintenanceCommand(testLeaseA, id)
	require.NoError(t, lookupErr)
	require.True(t, found)
	assert.Equal(t, placement.MaintenanceOutcomePending, record.Outcome(),
		"a recovery timeout is ambiguous and must retain the durable command")
	assertMaintenanceLaneHeld(t, runtime, testLeaseA)
}

func TestPendingRecoveryGivesEachBackendAnIndependentLane(t *testing.T) {
	const unavailableCount = 8
	leaseBackends := make(map[string]string, unavailableCount+1)
	leaseUUIDs := make([]string, 0, unavailableCount+1)
	requestIDs := make([]maintenanceid.ID, 0, unavailableCount+1)
	for index := 1; index <= unavailableCount; index++ {
		leaseUUID := fmt.Sprintf("10000000-0000-4000-8000-%012d", index)
		leaseBackends[leaseUUID] = "backend-a"
		leaseUUIDs = append(leaseUUIDs, leaseUUID)
		requestIDs = append(requestIDs, requestID(t,
			fmt.Sprintf("20000000-0000-4000-8000-%012d", index)))
	}
	healthyLease := "f0000000-0000-4000-8000-000000000009"
	leaseBackends[healthyLease] = "backend-b"
	leaseUUIDs = append(leaseUUIDs, healthyLease)
	requestIDs = append(requestIDs, requestID(t, "f1000000-0000-4000-8000-000000000009"))

	store, path := newPlacementAuthorityForTopology(t, leaseBackends)
	ambiguous := &fakeBackend{name: "backend-a", restart: func(backend.RestartRequest) error {
		return errors.New("ambiguous backend-a result")
	}}
	ambiguousHealthy := &fakeBackend{name: "backend-b", restart: func(backend.RestartRequest) error {
		return errors.New("ambiguous backend-b result")
	}}
	firstChain := testChain(leaseUUIDs...)
	first, err := NewService(Config{
		Coordinator: maintenanceCoordinatorForTest(
			t, store, firstChain,
			fakeRouter{backends: map[string]backend.Backend{
				"backend-a": ambiguous, "backend-b": ambiguousHealthy,
			}}, nil,
		),
	})
	require.NoError(t, err)
	for index, leaseUUID := range leaseUUIDs {
		result := first.Execute(t.Context(), Command{
			ID: requestIDs[index], LeaseUUID: leaseUUID,
			Tenant: testTenant, Kind: KindRestart,
		})
		require.Equal(t, OutcomeServiceUnavailable, result.Outcome())
	}
	require.NoError(t, store.Close())

	reopened := reopenPlacementAuthority(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	releaseUnavailable := make(chan struct{})
	unavailable := &fakeBackend{name: "backend-a", restart: func(backend.RestartRequest) error {
		<-releaseUnavailable
		return errors.New("backend-a remains unavailable")
	}}
	healthyCalled := make(chan struct{})
	var healthyOnce sync.Once
	healthy := &fakeBackend{name: "backend-b", restart: func(backend.RestartRequest) error {
		healthyOnce.Do(func() { close(healthyCalled) })
		return nil
	}}
	restartChain := testChain(leaseUUIDs...)
	recovered, err := NewService(Config{
		Coordinator: maintenanceCoordinatorForTest(
			t, reopened, restartChain,
			fakeRouter{backends: map[string]backend.Backend{
				"backend-a": unavailable, "backend-b": healthy,
			}}, nil,
		),
	})
	require.NoError(t, err)
	recoveryDone := make(chan error, 1)
	go func() { recoveryDone <- recovered.RecoverPending(t.Context()) }()

	healthyProgressed := false
	select {
	case <-healthyCalled:
		healthyProgressed = true
	case <-time.After(500 * time.Millisecond):
	}
	unavailableInFlight := unavailable.restartCount()
	close(releaseUnavailable)
	require.Error(t, <-recoveryDone)
	assert.True(t, healthyProgressed,
		"pending work on one unavailable backend must not occupy recovery capacity for another backend")
	assert.Equal(t, 1, healthy.restartCount())
	assert.LessOrEqual(t, unavailableInFlight, 1,
		"one unavailable backend must use at most one recovery lane regardless of its pending lease count")
}

func preparePendingMaintenanceCommands(
	t *testing.T,
	count int,
) (string, []string) {
	t.Helper()
	leaseBackends := make(map[string]string, count)
	leaseUUIDs := make([]string, 0, count)
	requestIDs := make([]maintenanceid.ID, 0, count)
	for index := 1; index <= count; index++ {
		leaseUUID := fmt.Sprintf("30000000-0000-4000-8000-%012d", index)
		leaseBackends[leaseUUID] = "backend-a"
		leaseUUIDs = append(leaseUUIDs, leaseUUID)
		requestIDs = append(requestIDs, requestID(t,
			fmt.Sprintf("40000000-0000-4000-8000-%012d", index)))
	}
	store, path := newPlacementAuthorityForTopology(t, leaseBackends)
	initial := newTestService(t, store, &fakeBackend{
		restart: func(backend.RestartRequest) error {
			return errors.New("initial ambiguous transport result")
		},
	}, nil, leaseUUIDs...)
	for index, leaseUUID := range leaseUUIDs {
		result := initial.Execute(t.Context(), Command{
			ID: requestIDs[index], LeaseUUID: leaseUUID,
			Tenant: testTenant, Kind: KindRestart,
		})
		require.Equal(t, OutcomeServiceUnavailable, result.Outcome())
	}
	require.NoError(t, store.Close())
	return path, leaseUUIDs
}

func TestPendingRecoveryBoundsEachBackendBatch(t *testing.T) {
	const (
		expectedRecoveryBatchLimit = 32
		pendingCount               = expectedRecoveryBatchLimit + 8
	)
	path, leaseUUIDs := preparePendingMaintenanceCommands(t, pendingCount)
	reopened := reopenPlacementAuthority(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	backendClient := &fakeBackend{restart: func(backend.RestartRequest) error {
		return errors.New("backend remains unavailable")
	}}
	service := newTestService(
		t, reopened, backendClient, nil, leaseUUIDs...,
	)

	require.Error(t, service.RecoverPending(t.Context()))
	firstBatch := backendClient.restartedLeases()
	require.Len(t, firstBatch, expectedRecoveryBatchLimit,
		"one cadence must do bounded work even when failures return immediately")
	assert.Len(t, slices.Compact(slices.Clone(firstBatch)), len(firstBatch))

	require.Error(t, service.RecoverPending(t.Context()))
	allCalls := backendClient.restartedLeases()
	require.Len(t, allCalls, 2*expectedRecoveryBatchLimit)
	seen := make(map[string]struct{}, len(allCalls))
	for _, leaseUUID := range allCalls {
		seen[leaseUUID] = struct{}{}
	}
	assert.Len(t, seen, pendingCount,
		"the rotating batch must reach commands beyond the first bounded window")
}

func TestRecoveryAfterRestartRotatesPastRepeatedlyStalledLease(t *testing.T) {
	const pendingCount = 4
	path, leaseUUIDs := preparePendingMaintenanceCommands(t, pendingCount)
	reopened := reopenPlacementAuthority(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	blocking := &perLeaseBlockingBackend{name: "backend-a"}
	chain := testChain(leaseUUIDs...)
	service, err := NewService(Config{
		Coordinator: maintenanceCoordinatorForTest(
			t, reopened, chain,
			fakeRouter{backend: blocking}, nil,
		),
		RecoveryTimeout: 25 * time.Millisecond,
	})
	require.NoError(t, err)

	for pass := range pendingCount {
		recoveryErr := service.RecoverPending(t.Context())
		require.ErrorIs(t, recoveryErr, context.DeadlineExceeded)
		calls := blocking.calledLeases()
		require.Len(t, calls, pass+1,
			"one shared lane deadline must permit only one fully stalled call per cadence")
	}
	assert.ElementsMatch(t, leaseUUIDs, blocking.calledLeases(),
		"the post-restart cursor must advance past every repeatedly stalled lease")
}

func TestStartRecoversImmediatelyAndKeepsBackendOutageNonfatal(t *testing.T) {
	store, _ := newPlacementAuthority(t, testLeaseA)
	backendClient := &fakeBackend{restart: func(backend.RestartRequest) error {
		return errors.New("initial ambiguous transport result")
	}}
	service := newTestService(
		t, store, backendClient, nil, testLeaseA,
	)
	command := Command{
		ID: requestID(t, testRequestA), LeaseUUID: testLeaseA,
		Tenant: testTenant, Kind: KindRestart,
	}
	require.Equal(t, OutcomeServiceUnavailable, service.Execute(t.Context(), command).Outcome())

	recoveryAttempted := make(chan struct{})
	backendClient.mu.Lock()
	backendClient.restart = func(backend.RestartRequest) error {
		close(recoveryAttempted)
		return errors.New("pinned backend remains unavailable")
	}
	backendClient.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- service.Start(ctx, time.Hour) }()

	select {
	case <-recoveryAttempted:
	case <-time.After(time.Second):
		t.Fatal("background recovery did not run its immediate pass")
	}
	select {
	case err := <-done:
		t.Fatalf("a backend-local recovery failure stopped the worker: %v", err)
	default:
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestFirstDispatchRepeatsChainAuthorizationAfterDurableAdmission(t *testing.T) {
	store, _ := newPlacementAuthority(t, testLeaseA)
	active := &billingtypes.Lease{
		Uuid: testLeaseA, Tenant: testTenant, ProviderUuid: testProviderUUID,
		State: billingtypes.LEASE_STATE_ACTIVE,
	}
	closed := *active
	closed.State = billingtypes.LEASE_STATE_CLOSED
	chain := &sequenceChain{leases: []*billingtypes.Lease{active, &closed}}
	backendClient := &fakeBackend{}
	coordinator, runtime := maintenanceCoordinatorWithRuntimeForTest(
		t, store, chain, fakeRouter{backend: backendClient}, nil,
	)
	service, err := NewService(Config{Coordinator: coordinator})
	require.NoError(t, err)
	id := requestID(t, testRequestA)

	result := service.Execute(t.Context(), Command{
		ID: id, LeaseUUID: testLeaseA, Tenant: testTenant, Kind: KindRestart,
	})
	assert.Equal(t, OutcomeNoLongerActive, result.Outcome())
	assert.Zero(t, backendClient.restartCount(),
		"a close racing admission must be observed before the first backend side effect")
	receipt, found, err := store.LookupMaintenanceCommand(testLeaseA, id)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, placement.MaintenanceOutcomeLeaseEnded, receipt.Outcome())
	assertMaintenanceLaneReleased(t, runtime, testLeaseA)
}

func TestAcceptedUpdatePersistsPayloadBeforeSettlementAndTerminalReplayIsReadOnly(t *testing.T) {
	store, _ := newPlacementAuthority(t, testLeaseA)
	backendClient := &fakeBackend{}
	payloads := &fakePayloads{failures: 1}
	service, runtime := newTestServiceWithRuntime(
		t, store, backendClient, payloads, testLeaseA,
	)
	id := requestID(t, testRequestA)
	command := Command{
		ID: id, LeaseUUID: testLeaseA, Tenant: testTenant,
		Kind: KindUpdate, Payload: []byte("services: {app: {image: example}}"),
	}

	first := service.Execute(t.Context(), command)
	assert.Equal(t, OutcomeInternalFailure, first.Outcome())
	assert.Equal(t, 1, backendClient.updateCount())
	assert.Equal(t, 0, payloads.writeCount())
	assertMaintenanceLaneHeld(t, runtime, testLeaseA)

	second := service.Execute(t.Context(), command)
	assert.Equal(t, OutcomeAccepted, second.Outcome())
	assert.Equal(t, 2, backendClient.updateCount(), "retry must repeat the same typed backend command")
	assert.Equal(t, 1, payloads.writeCount())

	terminal := service.Execute(t.Context(), command)
	assert.Equal(t, OutcomeAccepted, terminal.Outcome())
	assert.Equal(t, 2, backendClient.updateCount())
	assert.Equal(t, 1, payloads.writeCount(), "terminal receipt replay cannot rewrite payload state")
	unauthorized := command
	unauthorized.Tenant = "different-tenant"
	assert.Equal(t, OutcomeForbidden, service.Execute(t.Context(), unauthorized).Outcome(),
		"terminal replay must authenticate against the immutable stored tenant")
	assert.Equal(t, 2, backendClient.updateCount())
	assert.Equal(t, 1, payloads.writeCount())
	divergent := command
	divergent.Payload = []byte("different")
	assert.Equal(t, OutcomeCommandConflict, service.Execute(t.Context(), divergent).Outcome())
}

func TestOrderedStartPublicationUsesDurableDispatchDisposition(t *testing.T) {
	for name, test := range map[string]struct {
		responseStatus  int
		responseBody    string
		wantError       error
		payloadFailures int
		wantAccepted    bool
		wantOutcome     Outcome
	}{
		"accepted backend with local payload failure": {
			payloadFailures: 1,
			wantAccepted:    true,
			wantOutcome:     OutcomeInternalFailure,
		},
		"definitive backend refusal": {
			responseStatus: http.StatusBadRequest,
			responseBody:   `{"error":"invalid maintenance request"}`,
			wantError:      backend.ErrValidation,
			wantAccepted:   false,
			wantOutcome:    OutcomeBackendValidation,
		},
	} {
		t.Run(name, func(t *testing.T) {
			store, _ := newPlacementAuthority(t, testLeaseA)
			var backendClient backend.Backend = &fakeBackend{}
			if test.responseStatus != 0 {
				backendClient, _ = causalMaintenanceBackendForTest(
					t, store, test.responseStatus, test.responseBody,
				)
			}
			payloads := &fakePayloads{failures: test.payloadFailures}
			events := &recordingOrderedEvents{}
			chain := testChain(testLeaseA)
			service, err := NewService(Config{
				Coordinator: maintenanceCoordinatorForTest(
					t, store, chain,
					fakeRouter{backend: backendClient}, payloads,
				),
				Events: events,
			})
			require.NoError(t, err)

			result := service.Execute(t.Context(), Command{
				ID: requestID(t, testRequestA), LeaseUUID: testLeaseA,
				Tenant: testTenant, Kind: KindUpdate, Payload: []byte("payload"),
			})
			assert.Equal(t, test.wantOutcome, result.Outcome())
			require.True(t, events.called)
			assert.Equal(t, testLeaseA, events.start.LeaseUUID)
			assert.Equal(t, backend.ProvisionStatusUpdating, events.start.Status)
			assert.Equal(t, test.wantAccepted, events.accepted,
				"the event gate must publish a start only after backend acceptance")
			if test.wantError != nil {
				assert.ErrorIs(t, events.err, test.wantError)
			} else {
				assert.Error(t, events.err, "local settlement failure must remain visible")
			}
		})
	}
}

func TestAcceptedUpdatePayloadPersistenceRecoversAcrossProviderRestart(t *testing.T) {
	store, path := newPlacementAuthority(t, testLeaseA)
	firstBackend := &fakeBackend{}
	firstPayloads := &fakePayloads{failures: 1}
	first, firstRuntime := newTestServiceWithRuntime(
		t, store, firstBackend, firstPayloads, testLeaseA,
	)
	id := requestID(t, testRequestA)
	command := Command{
		ID: id, LeaseUUID: testLeaseA, Tenant: testTenant,
		Kind: KindUpdate, Payload: []byte("services: {app: {image: recovered}}"),
	}

	result := first.Execute(t.Context(), command)
	require.Equal(t, OutcomeInternalFailure, result.Outcome())
	assert.Equal(t, 1, firstBackend.updateCount())
	assertMaintenanceLaneHeld(t, firstRuntime, testLeaseA)
	require.NoError(t, store.Close())

	reopened := reopenPlacementAuthority(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	secondBackend := &fakeBackend{}
	secondPayloads := &fakePayloads{}
	recovered, secondRuntime := newTestServiceWithRuntime(
		t, reopened, secondBackend, secondPayloads, testLeaseA,
	)
	require.NoError(t, recovered.RecoverPending(t.Context()))
	assert.Equal(t, 1, secondBackend.updateCount(),
		"startup recovery must repeat the exact backend admission after ambiguity")
	assert.Equal(t, 1, secondPayloads.writeCount(),
		"an accepted backend replay is not settled until the payload is durable")
	receipt, found, err := reopened.LookupMaintenanceCommand(testLeaseA, id)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, placement.MaintenanceOutcomeAccepted, receipt.Outcome())
	assertMaintenanceLaneReleased(t, secondRuntime, testLeaseA)
}

func TestRecoveryTerminalizesPendingCommandWhenLeaseEnded(t *testing.T) {
	store, path := newPlacementAuthority(t, testLeaseA)
	down := &fakeBackend{restart: func(backend.RestartRequest) error {
		return errors.New("ambiguous transport result")
	}}
	service := newTestService(t, store, down, nil, testLeaseA)
	id := requestID(t, testRequestA)
	result := service.Execute(t.Context(), Command{
		ID: id, LeaseUUID: testLeaseA, Tenant: testTenant, Kind: KindRestart,
	})
	require.Equal(t, OutcomeServiceUnavailable, result.Outcome())
	require.NoError(t, store.Close())

	reopened := reopenPlacementAuthority(t, path)
	t.Cleanup(func() { _ = reopened.Close() })
	closedChain := testChain(testLeaseA)
	closedChain.leases[testLeaseA].State = billingtypes.LEASE_STATE_CLOSED
	backendClient := &fakeBackend{}
	restartCoordinator, restartRuntime := maintenanceCoordinatorWithRuntimeForTest(
		t, reopened, closedChain,
		fakeRouter{backend: backendClient}, nil,
	)
	recovered, err := NewService(Config{Coordinator: restartCoordinator})
	require.NoError(t, err)
	require.NoError(t, recovered.RecoverPending(t.Context()))
	assert.Zero(t, backendClient.restartCount(), "ended lease cannot receive new backend work")
	receipt, found, err := reopened.LookupMaintenanceCommand(testLeaseA, id)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, placement.MaintenanceOutcomeLeaseEnded, receipt.Outcome())

	assertMaintenanceLaneReleased(t, restartRuntime, testLeaseA)
}

func TestMaintenanceRefusalEvidenceControlsSettlement(t *testing.T) {
	for name, test := range map[string]struct {
		responseStatus int
		responseBody   string
		backendError   error
		outcome        placement.MaintenanceCommandOutcome
		settled        bool
	}{
		"identity-bound coded capacity refusal": {
			responseStatus: http.StatusServiceUnavailable,
			responseBody:   `{"error":"full","code":"insufficient_resources"}`,
			outcome:        placement.MaintenanceOutcomeCapacityRefused,
			settled:        true,
		},
		"unbound fake circuit-open error remains ambiguous": {
			backendError: backend.ErrCircuitOpen,
			outcome:      placement.MaintenanceOutcomePending,
		},
	} {
		t.Run(name, func(t *testing.T) {
			store, _ := newPlacementAuthority(t, testLeaseA)
			fake := &fakeBackend{restart: func(backend.RestartRequest) error {
				return test.backendError
			}}
			var backendClient backend.Backend = fake
			callCount := fake.restartCount
			if test.responseStatus != 0 {
				backendClient, callCount = causalMaintenanceBackendForTest(
					t, store, test.responseStatus, test.responseBody,
				)
			}
			service, runtime := newTestServiceWithRuntime(
				t, store, backendClient, nil, testLeaseA,
			)
			id := requestID(t, testRequestA)
			command := Command{
				ID: id, LeaseUUID: testLeaseA, Tenant: testTenant, Kind: KindRestart,
			}

			result := service.Execute(t.Context(), command)
			assert.Equal(t, OutcomeServiceUnavailable, result.Outcome())
			receipt, found, err := store.LookupMaintenanceCommand(testLeaseA, id)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, test.outcome, receipt.Outcome())
			if test.settled {
				assertMaintenanceLaneReleased(t, runtime, testLeaseA)
			} else {
				assertMaintenanceLaneHeld(t, runtime, testLeaseA)
			}

			replay := service.Execute(t.Context(), command)
			assert.Equal(t, OutcomeServiceUnavailable, replay.Outcome())
			wantCalls := 2
			if test.settled {
				wantCalls = 1
			}
			assert.Equal(t, wantCalls, callCount(),
				"only a transport-minted refusal may make replay terminal")
		})
	}
}

func TestLiveLeaseProviderReceiptNeverRedispatchesAnExactLateRetry(t *testing.T) {
	clock := &fakeMaintenanceClock{now: time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)}
	store, _ := newPlacementAuthorityWithOptions(
		t, []string{testLeaseA}, placement.WithClock(clock.Now),
	)
	var (
		backendMu       sync.Mutex
		backendReceipts = make(map[maintenanceid.ID]struct{})
		mutations       int
	)
	backendClient := &fakeBackend{restart: func(request backend.RestartRequest) error {
		backendMu.Lock()
		defer backendMu.Unlock()
		if _, replay := backendReceipts[request.MaintenanceID]; replay {
			return nil
		}
		backendReceipts[request.MaintenanceID] = struct{}{}
		mutations++
		return nil
	}}
	events := &recordingOrderedEvents{}
	chain := testChain(testLeaseA)
	service, err := NewService(Config{
		Coordinator: maintenanceCoordinatorForTest(
			t, store, chain,
			fakeRouter{backend: backendClient}, nil,
		),
		Events: events,
	})
	require.NoError(t, err)
	command := Command{
		ID: requestID(t, testRequestA), LeaseUUID: testLeaseA,
		Tenant: testTenant, Kind: KindRestart,
	}
	require.Equal(t, OutcomeAccepted, service.Execute(t.Context(), command).Outcome())

	clock.now = clock.now.Add(100 * 365 * 24 * time.Hour)
	require.NoError(t, service.RecoverPending(t.Context()),
		"an idle reclamation pass must preserve receipts for live authority")
	require.Equal(t, OutcomeAccepted, service.Execute(t.Context(), command).Outcome())
	assert.Equal(t, 1, backendClient.restartCount(),
		"an exact retry cannot become a new asynchronous backend request with age")
	assert.Equal(t, 1, events.calls,
		"terminal replay must not publish a new restarting event with no backend worker")
	backendMu.Lock()
	assert.Equal(t, 1, mutations,
		"one command identity must name one logical mutation for the lifetime of the lease")
	backendMu.Unlock()
}

func TestLiveLeaseProviderReceiptPreventsLateUpdateFromOverwritingNewerPayload(t *testing.T) {
	clock := &fakeMaintenanceClock{now: time.Date(2026, time.September, 3, 12, 0, 0, 0, time.UTC)}
	store, _ := newPlacementAuthorityWithOptions(
		t, []string{testLeaseA}, placement.WithClock(clock.Now),
	)
	var (
		backendMu       sync.Mutex
		backendReceipts = make(map[maintenanceid.ID]struct{})
		latestUpdate    maintenanceid.ID
	)
	backendClient := &fakeBackend{update: func(request backend.UpdateRequest) error {
		backendMu.Lock()
		defer backendMu.Unlock()
		if _, replay := backendReceipts[request.MaintenanceID]; replay {
			if request.MaintenanceID != latestUpdate {
				return backend.ErrInvalidState
			}
			return nil
		}
		backendReceipts[request.MaintenanceID] = struct{}{}
		latestUpdate = request.MaintenanceID
		return nil
	}}
	payloads := &fakePayloads{}
	service := newTestService(
		t, store, backendClient, payloads, testLeaseA,
	)
	first := Command{
		ID: requestID(t, testRequestA), LeaseUUID: testLeaseA,
		Tenant: testTenant, Kind: KindUpdate, Payload: []byte("payload-a"),
	}
	second := Command{
		ID: requestID(t, testRequestB), LeaseUUID: testLeaseA,
		Tenant: testTenant, Kind: KindUpdate, Payload: []byte("payload-b"),
	}
	require.Equal(t, OutcomeAccepted, service.Execute(t.Context(), first).Outcome())
	require.Equal(t, OutcomeAccepted, service.Execute(t.Context(), second).Outcome())
	require.Equal(t, []byte("payload-b"), payloads.lastWrite())

	clock.now = clock.now.Add(100 * 365 * 24 * time.Hour)
	require.NoError(t, service.RecoverPending(t.Context()))
	result := service.Execute(t.Context(), first)
	assert.Equal(t, OutcomeAccepted, result.Outcome(),
		"the provider's permanent live-lease receipt must replay the original outcome")
	assert.Equal(t, []byte("payload-b"), payloads.lastWrite(),
		"late update A must never overwrite the desired payload installed by B")
	assert.Equal(t, 2, payloads.writeCount())
	assert.Equal(t, 2, backendClient.updateCount(),
		"an exact late retry must not cross the provider/backend boundary again")
}

func TestRecoveryAndClientRetryShareOnePerCommandDispatchLane(t *testing.T) {
	store, _ := newPlacementAuthority(t, testLeaseA)
	backendClient := &fakeBackend{restart: func(backend.RestartRequest) error {
		return errors.New("initial ambiguous transport result")
	}}
	service := newTestService(
		t, store, backendClient, nil, testLeaseA,
	)
	command := Command{
		ID: requestID(t, testRequestA), LeaseUUID: testLeaseA,
		Tenant: testTenant, Kind: KindRestart,
	}
	require.Equal(t, OutcomeServiceUnavailable, service.Execute(t.Context(), command).Outcome())

	entered := make(chan struct{})
	release := make(chan struct{})
	backendClient.mu.Lock()
	backendClient.restart = func(backend.RestartRequest) error {
		close(entered)
		<-release
		return nil
	}
	backendClient.mu.Unlock()
	recoveryDone := make(chan error, 1)
	go func() { recoveryDone <- service.RecoverPending(t.Context()) }()
	<-entered
	retryDone := make(chan Result, 1)
	go func() { retryDone <- service.Execute(t.Context(), command) }()
	require.Never(t, func() bool {
		return backendClient.restartCount() > 2
	}, 50*time.Millisecond, time.Millisecond,
		"the retry must wait behind recovery rather than redispatch concurrently")
	close(release)
	require.NoError(t, <-recoveryDone)
	assert.Equal(t, OutcomeAccepted, (<-retryDone).Outcome())
	assert.Equal(t, 2, backendClient.restartCount(),
		"the waiter must observe terminal settlement instead of dispatching again")
}

type fakeMaintenanceClock struct{ now time.Time }

func (clock *fakeMaintenanceClock) Now() time.Time { return clock.now }
