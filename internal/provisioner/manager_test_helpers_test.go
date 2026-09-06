package provisioner

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
	"github.com/manifest-network/fred/internal/util"
)

// Legacy fixture ports live in test code only. Production application
// services receive concrete coordinator-minted capabilities instead of these
// independently implementable authority aggregates.
type ReconcilerPlacement interface {
	PlacementView
	VerifyProviderUUID(string) error
	VerifyBackendTopology([]string) error
	ExpectedBackendStorageIdentity(string) (backendidentity.ID, bool)
	CurrentAdmissionBaseline() placement.AdmissionBaseline
}

type PlacementAuthorityStore interface {
	ReconcilerPlacement
	BindOperationCoordinator(func(int)) (*placement.OperationCoordinator, error)
}

// testHandlerDeps preserves concise fixture construction while production
// HandlerDeps stays capability-narrow. The helper below performs the same
// explicit composition as Manager. A concrete Registry/Store pair is joined
// immediately, and only the resulting coordinator reaches CallbackService.
type testHandlerDeps struct {
	ChainClient    ChainClient
	Orchestrator   *ProvisionOrchestrator
	Tracker        *testOperationRegistry
	Acknowledger   Acknowledger
	PayloadStore   *payload.Store
	Publisher      message.Publisher
	BackendRouter  BackendRouter
	Placement      *placement.Store
	Coordinator    *placement.OperationCoordinator
	CallbackEvents CallbackEventSink
	Callbacks      CallbackApplication
}

type testProvisionStartSink struct {
	mu        sync.RWMutex
	publisher message.Publisher
}

type mutableTestProvisionLeaseReader struct {
	mu     sync.RWMutex
	leases map[string]*billingtypes.Lease
}

// testReconciliationChain upgrades a provision-only exact reader into the
// complete chain capability required at the reconciliation construction
// boundary. Inventory and terminal writes are intentionally inert because
// these orchestrator fixtures never execute a reconciliation sweep.
type testReconciliationChain struct {
	placement.ProvisionLeaseReader
}

func (testReconciliationChain) GetPendingLeases(
	context.Context,
	string,
) ([]billingtypes.Lease, error) {
	return nil, nil
}

func (testReconciliationChain) GetActiveLeasesByProvider(
	context.Context,
	string,
) ([]billingtypes.Lease, error) {
	return nil, nil
}

func (testReconciliationChain) RejectLeases(
	context.Context,
	[]string,
	string,
) (uint64, []string, error) {
	return 0, nil, nil
}

func (testReconciliationChain) CloseLeases(
	context.Context,
	[]string,
	string,
) (uint64, []string, error) {
	return 0, nil, nil
}

func (reader *mutableTestProvisionLeaseReader) GetLease(
	_ context.Context,
	leaseUUID string,
) (*billingtypes.Lease, error) {
	reader.mu.RLock()
	defer reader.mu.RUnlock()
	lease := reader.leases[leaseUUID]
	if lease == nil {
		return nil, nil
	}
	clone := *lease
	clone.Items = slices.Clone(lease.Items)
	return &clone, nil
}

func (reader *mutableTestProvisionLeaseReader) set(lease *billingtypes.Lease) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	clone := *lease
	clone.Items = slices.Clone(lease.Items)
	reader.leases[lease.Uuid] = &clone
}

type testProvisionFixture struct {
	reader       *mutableTestProvisionLeaseReader
	sink         *testProvisionStartSink
	providerUUID string
}

var testProvisionFixtures sync.Map
var testReconciliationCoordinators sync.Map
var testManagerPlacements sync.Map
var testManagerChains sync.Map

// canonicalManagerTestChain migrates historical fixture labels to the exact
// provider identity bound to every test placement Store. Values that name a
// genuinely different provider remain different so authorization tests keep
// exercising the production mismatch path.
type canonicalManagerTestChain struct{ ManagerChainClient }

type canonicalReconciliationTestChain struct{ placement.ReconciliationChain }

// seededReconciliationTestChain makes legacy pre-construction in-flight
// fixtures observable through the same exact-read boundary as production.
// The operation itself is replayed later through ProvisionCoordinator; no raw
// Registry mutation surface is reintroduced.
type seededReconciliationTestChain struct {
	placement.ReconciliationChain
	seeds *testOperationSeedReader
}

type testOperationSeedReader struct {
	mu    sync.RWMutex
	seeds map[string]testOperationSeed
}

func newTestOperationSeedReader(seeds []testOperationSeed) *testOperationSeedReader {
	reader := &testOperationSeedReader{seeds: make(map[string]testOperationSeed, len(seeds))}
	for _, seed := range seeds {
		reader.seeds[seed.leaseUUID] = seed
	}
	return reader
}

func (reader *testOperationSeedReader) set(seed testOperationSeed) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.seeds[seed.leaseUUID] = seed
}

func (reader *testOperationSeedReader) delete(leaseUUID string) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	delete(reader.seeds, leaseUUID)
}

func (reader *testOperationSeedReader) get(leaseUUID string) (testOperationSeed, bool) {
	reader.mu.RLock()
	defer reader.mu.RUnlock()
	seed, ok := reader.seeds[leaseUUID]
	return seed, ok
}

func (chain seededReconciliationTestChain) GetLease(
	ctx context.Context, leaseUUID string,
) (*billingtypes.Lease, error) {
	if seed, ok := chain.seeds.get(leaseUUID); ok {
		items := make([]billingtypes.LeaseItem, 0, len(seed.items))
		for _, item := range seed.items {
			items = append(items, billingtypes.LeaseItem{
				SkuUuid: item.SKU, Quantity: uint64(item.Quantity),
				ServiceName: item.ServiceName, CustomDomain: item.CustomDomain,
			})
		}
		return &billingtypes.Lease{
			Uuid: leaseUUID, Tenant: seed.tenant,
			ProviderUuid: placementstore.ProviderUUID,
			State:        billingtypes.LEASE_STATE_PENDING,
			Items:        items,
		}, nil
	}
	return chain.ReconciliationChain.GetLease(ctx, leaseUUID)
}

func canonicalManagerLease(lease *billingtypes.Lease) *billingtypes.Lease {
	if lease == nil {
		return nil
	}
	copy := *lease
	copy.Items = append([]billingtypes.LeaseItem(nil), lease.Items...)
	if copy.ProviderUuid == "" || copy.ProviderUuid == "provider-1" ||
		copy.ProviderUuid == "provider-uuid" {
		copy.ProviderUuid = placementstore.ProviderUUID
	}
	return &copy
}

func canonicalInventoryTestLease(lease billingtypes.Lease) billingtypes.Lease {
	copy := *canonicalManagerLease(&lease)
	return copy
}

func (chain canonicalReconciliationTestChain) GetLease(
	ctx context.Context, leaseUUID string,
) (*billingtypes.Lease, error) {
	lease, err := chain.ReconciliationChain.GetLease(ctx, leaseUUID)
	lease = canonicalManagerLease(lease)
	if lease != nil {
		// Historical handler fixtures often described only the state under test.
		// Fill their omitted positive identity at this test-only chain boundary;
		// explicit foreign UUIDs, tenants, and providers remain untouched so the
		// authorization regressions still exercise the production fail-closed path.
		if lease.Uuid == "" {
			lease.Uuid = leaseUUID
		}
		if lease.Tenant == "" {
			lease.Tenant = "tenant-a"
		}
	}
	return lease, err
}

func (chain canonicalReconciliationTestChain) GetPendingLeases(
	ctx context.Context, providerUUID string,
) ([]billingtypes.Lease, error) {
	leases, err := chain.ReconciliationChain.GetPendingLeases(ctx, providerUUID)
	for index := range leases {
		leases[index] = canonicalInventoryTestLease(leases[index])
	}
	return leases, err
}

func (chain canonicalReconciliationTestChain) GetActiveLeasesByProvider(
	ctx context.Context, providerUUID string,
) ([]billingtypes.Lease, error) {
	leases, err := chain.ReconciliationChain.GetActiveLeasesByProvider(ctx, providerUUID)
	for index := range leases {
		leases[index] = canonicalInventoryTestLease(leases[index])
	}
	return leases, err
}

func (chain canonicalManagerTestChain) GetLease(
	ctx context.Context, leaseUUID string,
) (*billingtypes.Lease, error) {
	lease, err := chain.ManagerChainClient.GetLease(ctx, leaseUUID)
	return canonicalManagerLease(lease), err
}

func (chain canonicalManagerTestChain) GetPendingLeases(
	ctx context.Context, providerUUID string,
) ([]billingtypes.Lease, error) {
	leases, err := chain.ManagerChainClient.GetPendingLeases(ctx, providerUUID)
	for index := range leases {
		leases[index] = *canonicalManagerLease(&leases[index])
	}
	return leases, err
}

func (chain canonicalManagerTestChain) GetActiveLeasesByProvider(
	ctx context.Context, providerUUID string,
) ([]billingtypes.Lease, error) {
	leases, err := chain.ManagerChainClient.GetActiveLeasesByProvider(ctx, providerUUID)
	for index := range leases {
		leases[index] = *canonicalManagerLease(&leases[index])
	}
	return leases, err
}

// testInventoryBackend keeps dependent-package fixtures on the same opaque
// collection path as production. Tests may stage the next endpoint response,
// but only a sweep-bound Collect call can turn it into projection evidence.
type testInventoryBackend struct {
	backend.Backend
	mu sync.Mutex
	// suppressProvision lets fixture setup register an accepted backend call
	// without mutating the behavioral double or consuming its injected error.
	suppressProvision bool

	storageID  backendidentity.ID
	provisions []backend.ProvisionInfo
	retentions []backend.RetainedLease
	staged     bool
}

func (client *testInventoryBackend) Provision(
	ctx context.Context,
	request backend.ProvisionRequest,
) error {
	client.mu.Lock()
	suppress := client.suppressProvision
	client.mu.Unlock()
	if suppress {
		return nil
	}
	return client.Backend.Provision(ctx, request)
}

func (client *testInventoryBackend) RefreshState(ctx context.Context) error {
	client.mu.Lock()
	staged := client.staged
	client.mu.Unlock()
	if staged {
		return nil
	}
	return client.Backend.RefreshState(ctx)
}

func (client *testInventoryBackend) stage(
	storageID backendidentity.ID,
	provisions []backend.ProvisionInfo,
	retentions []backend.RetainedLease,
) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.storageID = storageID
	client.provisions = cloneTestProvisionInfos(provisions)
	client.retentions = slices.Clone(retentions)
	client.staged = true
}

func (client *testInventoryBackend) clearStage() {
	client.mu.Lock()
	client.provisions = nil
	client.retentions = nil
	client.staged = false
	client.mu.Unlock()
}

func (client *testInventoryBackend) ListProvisionsWithIdentity(
	ctx context.Context,
) ([]backend.ProvisionInfo, backendidentity.ID, error) {
	client.mu.Lock()
	if client.staged {
		rows, storageID := cloneTestProvisionInfos(client.provisions), client.storageID
		client.mu.Unlock()
		return rows, storageID, nil
	}
	client.mu.Unlock()
	rows, err := client.ListProvisions(ctx)
	return cloneTestProvisionInfos(rows), testBackendStorageID(client.Name()), err
}

func (client *testInventoryBackend) ListRetentionsWithIdentity(
	ctx context.Context,
) ([]backend.RetainedLease, backendidentity.ID, error) {
	client.mu.Lock()
	if client.staged {
		rows, storageID := slices.Clone(client.retentions), client.storageID
		client.mu.Unlock()
		return rows, storageID, nil
	}
	client.mu.Unlock()
	rows, err := client.ListRetentions(ctx)
	return slices.Clone(rows), testBackendStorageID(client.Name()), err
}

func cloneTestProvisionInfos(rows []backend.ProvisionInfo) []backend.ProvisionInfo {
	cloned := make([]backend.ProvisionInfo, len(rows))
	for index, row := range rows {
		cloned[index] = row
		cloned[index].Items = slices.Clone(row.Items)
		cloned[index].ServiceImages = maps.Clone(row.ServiceImages)
		if row.LifecycleGeneration != nil {
			generation := *row.LifecycleGeneration
			cloned[index].LifecycleGeneration = &generation
		}
	}
	return cloned
}

type testInventoryRouter struct {
	router   BackendRouter
	backends map[string]*testInventoryBackend
}

func newTestInventoryRouter(router BackendRouter) *testInventoryRouter {
	result := &testInventoryRouter{
		router: router, backends: make(map[string]*testInventoryBackend),
	}
	for _, client := range router.Backends() {
		if client != nil {
			if _, exactTransport := client.(*backend.HTTPClient); exactTransport {
				continue
			}
			result.backends[client.Name()] = &testInventoryBackend{Backend: client}
		}
	}
	return result
}

func (router *testInventoryRouter) wrap(client backend.Backend) backend.Backend {
	if client == nil {
		return nil
	}
	// Keep the exact production transport visible to backend.Invoke*. Wrapping
	// it would intentionally erase causal refusal authority and turn every
	// response into a conservative ambiguous outcome.
	if _, exactTransport := client.(*backend.HTTPClient); exactTransport {
		return client
	}
	return router.backends[client.Name()]
}

func (router *testInventoryRouter) Route(sku string) backend.Backend {
	return router.wrap(router.router.Route(sku))
}

func (router *testInventoryRouter) RouteForProvision(
	ctx context.Context, sku string, inFlight map[string]int,
) backend.Backend {
	return router.wrap(router.router.RouteForProvision(ctx, sku, inFlight))
}

func (router *testInventoryRouter) RouteForProvisionAmong(
	ctx context.Context,
	sku string,
	eligible map[string]struct{},
	inFlight map[string]int,
) backend.Backend {
	return router.wrap(router.router.RouteForProvisionAmong(ctx, sku, eligible, inFlight))
}

func (router *testInventoryRouter) GetBackendByName(name string) backend.Backend {
	return router.wrap(router.router.GetBackendByName(name))
}

func (router *testInventoryRouter) HasBackend(name string) bool {
	return router.GetBackendByName(name) != nil
}

func (router *testInventoryRouter) Backends() []backend.Backend {
	clients := router.router.Backends()
	result := make([]backend.Backend, 0, len(clients))
	for _, client := range clients {
		result = append(result, router.wrap(client))
	}
	return result
}

var testExecutionInventoryRouters sync.Map
var testReconciliationInventoryRouters sync.Map
var testReconciliationExecutions sync.Map
var testExecutionProviderControls sync.Map
var testOperationExecutions sync.Map

type testProviderControlPlane struct {
	mu       sync.RWMutex
	chain    placement.ReconciliationChain
	rejecter interface {
		RejectLeases(context.Context, []string, string) (uint64, []string, error)
	}
	ack Acknowledger
}

func (control *testProviderControlPlane) set(
	chain placement.ReconciliationChain,
	ack Acknowledger,
) {
	control.mu.Lock()
	defer control.mu.Unlock()
	if chain != nil {
		control.chain = chain
	}
	if ack != nil {
		control.ack = ack
	}
}

func (control *testProviderControlPlane) snapshot() (
	placement.ReconciliationChain,
	interface {
		RejectLeases(context.Context, []string, string) (uint64, []string, error)
	},
	Acknowledger,
) {
	control.mu.RLock()
	defer control.mu.RUnlock()
	return control.chain, control.rejecter, control.ack
}

func (control *testProviderControlPlane) GetLease(
	ctx context.Context,
	leaseUUID string,
) (*billingtypes.Lease, error) {
	chain, _, _ := control.snapshot()
	if chain != nil {
		return chain.GetLease(ctx, leaseUUID)
	}
	return &billingtypes.Lease{
		Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: placementstore.ProviderUUID,
		State: billingtypes.LEASE_STATE_PENDING,
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
	}, nil
}

func (control *testProviderControlPlane) GetPendingLeases(
	ctx context.Context, providerUUID string,
) ([]billingtypes.Lease, error) {
	chain, _, _ := control.snapshot()
	if chain == nil {
		return nil, nil
	}
	return chain.GetPendingLeases(ctx, providerUUID)
}

func (control *testProviderControlPlane) GetActiveLeasesByProvider(
	ctx context.Context, providerUUID string,
) ([]billingtypes.Lease, error) {
	chain, _, _ := control.snapshot()
	if chain == nil {
		return nil, nil
	}
	return chain.GetActiveLeasesByProvider(ctx, providerUUID)
}

func (control *testProviderControlPlane) RejectLeases(
	ctx context.Context, leaseUUIDs []string, reason string,
) (uint64, []string, error) {
	chain, rejecter, _ := control.snapshot()
	if rejecter != nil {
		return rejecter.RejectLeases(ctx, leaseUUIDs, reason)
	}
	if chain == nil {
		return 0, nil, nil
	}
	return chain.RejectLeases(ctx, leaseUUIDs, reason)
}

func (control *testProviderControlPlane) CloseLeases(
	ctx context.Context, leaseUUIDs []string, reason string,
) (uint64, []string, error) {
	chain, _, _ := control.snapshot()
	if chain == nil {
		return 0, nil, nil
	}
	return chain.CloseLeases(ctx, leaseUUIDs, reason)
}

func (control *testProviderControlPlane) Acknowledge(
	ctx context.Context, leaseUUID string,
) (bool, string, error) {
	_, _, ack := control.snapshot()
	if ack == nil {
		return true, "", nil
	}
	return ack.Acknowledge(ctx, leaseUUID)
}

func setTestProviderRejecter(
	t testing.TB,
	execution *placement.ExecutionCoordinator,
	rejecter interface {
		RejectLeases(context.Context, []string, string) (uint64, []string, error)
	},
) {
	t.Helper()
	value, ok := testExecutionProviderControls.Load(execution)
	require.True(t, ok)
	control := value.(*testProviderControlPlane)
	control.mu.Lock()
	defer control.mu.Unlock()
	control.rejecter = rejecter
}

func setTestProviderControlPlane(
	t testing.TB,
	execution *placement.ExecutionCoordinator,
	chain placement.ReconciliationChain,
	ack Acknowledger,
) {
	t.Helper()
	value, ok := testExecutionProviderControls.Load(execution)
	if !ok {
		// Manager-owned executions already carry the production control plane
		// built from this same chain; there is no mutable test adapter to replace.
		return
	}
	value.(*testProviderControlPlane).set(chain, ack)
}

func authenticatedCallbackCoordinatorForTest(
	coordinator *placement.OperationCoordinator,
	chain CallbackChain,
	ack Acknowledger,
) (*placement.AuthenticatedCallbackCoordinator, error) {
	value, ok := testOperationExecutions.Load(coordinator)
	if !ok {
		return nil, errors.New("test operation coordinator has no backend execution")
	}
	control, ok := chain.(placement.ReconciliationChain)
	if !ok || control == nil {
		return nil, errCallbackChainUnavailable
	}
	if ack == nil {
		return nil, errCallbackAcknowledgerUnavailable
	}
	execution := value.(*placement.ExecutionCoordinator)
	providerControl, exists := testExecutionProviderControls.Load(execution)
	if !exists {
		return nil, errors.New("test execution has no provider control plane")
	}
	providerControl.(*testProviderControlPlane).set(
		canonicalReconciliationTestChain{ReconciliationChain: control}, ack,
	)
	return execution.AuthenticatedCallbackCoordinator(callbackTestProofConsumer)
}

func bindTestBackendRuntime(
	t testing.TB,
	coordinator *placement.OperationCoordinator,
	router BackendRouter,
) *placement.ExecutionCoordinator {
	t.Helper()
	fixture := newTestInventoryRouter(router)
	control := &testProviderControlPlane{}
	execution, err := coordinator.BindBackendRuntime(fixture, control)
	require.NoError(t, err)
	testExecutionInventoryRouters.Store(execution, fixture)
	testExecutionProviderControls.Store(execution, control)
	testOperationExecutions.Store(coordinator, execution)
	t.Cleanup(func() {
		testExecutionInventoryRouters.Delete(execution)
		testExecutionProviderControls.Delete(execution)
		testOperationExecutions.Delete(coordinator)
	})
	return execution
}

func collectTestBackendInventory(
	t testing.TB,
	reconciliation *placement.ReconciliationCoordinator,
	sweep *placement.ReconciliationSweep,
	backendName string,
	storageID backendidentity.ID,
	provisions []backend.ProvisionInfo,
	retentions []backend.RetainedLease,
) placement.BackendInventoryDisposition {
	t.Helper()
	value, ok := testReconciliationInventoryRouters.Load(reconciliation)
	var fixture *testInventoryBackend
	if ok {
		fixture = value.(*testInventoryRouter).backends[backendName]
		if fixture != nil {
			fixture.stage(storageID, provisions, retentions)
			defer fixture.clearStage()
		}
	}
	provisionReceipt, err := sweep.CollectProvisionInventory(t.Context(), backendName)
	require.NoError(t, err)
	retentionReceipt, err := sweep.CollectRetentionInventory(t.Context(), backendName)
	require.NoError(t, err)
	if fixture == nil {
		require.Equal(t, storageID, provisionReceipt.StorageID())
		require.Equal(t, storageID, retentionReceipt.StorageID())
	}
	disposition, err := sweep.RecordBackendInventory(provisionReceipt, retentionReceipt)
	require.NoError(t, err)
	return disposition
}

func (sink *testProvisionStartSink) setPublisher(publisher message.Publisher) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.publisher = publisher
}

func (sink *testProvisionStartSink) PublishProvisionStarting(leaseUUID string) {
	sink.mu.RLock()
	publisher := sink.publisher
	sink.mu.RUnlock()
	publishLeaseStatusEvent(publisher, leaseUUID, backend.ProvisionStatusProvisioning, "")
}

func composeTestHandlerSet(t testing.TB, deps testHandlerDeps) *HandlerSet {
	t.Helper()

	if deps.ChainClient == nil {
		deps.ChainClient = &chaintest.MockClient{}
	}
	if util.IsNilInterface(deps.Acknowledger) {
		deps.Acknowledger = &mockAcknowledger{}
	}
	if deps.Tracker == nil {
		deps.Tracker = newTestOperationRegistry()
	}
	if deps.BackendRouter == nil {
		defaultBackend := &mockManagerBackend{name: "test-backend"}
		deps.BackendRouter = &mockBackendRouter{
			routeFn: func(string) backend.Backend { return defaultBackend },
			getBackendByNameFn: func(name string) backend.Backend {
				if name == defaultBackend.name {
					return defaultBackend
				}
				return nil
			},
			backendsFn: func() []backend.Backend { return []backend.Backend{defaultBackend} },
		}
	}
	if deps.Orchestrator == nil {
		eventTracker := deps.Tracker
		var eventStore any
		if deps.Placement != nil {
			eventStore = deps.Placement
		}
		if deps.Coordinator != nil {
			// Callback-only fixtures may already bind deps.Placement to another
			// Registry. The independent event handler capability is still mandatory,
			// so give its otherwise-unused orchestrator a private exact pair.
			eventTracker = newTestOperationRegistry()
			eventStore = nil
		}
		deps.Orchestrator = newTestProvisionOrchestrator(
			t, "provider-1", "http://callback", deps.BackendRouter, eventTracker, eventStore,
			deps.ChainClient,
		)
		if deps.Placement == nil {
			deps.Placement = deps.Tracker.callbackStore
		}
	}

	if deps.Orchestrator != nil {
		if deps.Placement == nil && deps.Tracker != nil {
			deps.Placement = deps.Tracker.callbackStore
		}
		if fixture, ok := testProvisionFixtures.Load(deps.Orchestrator); ok {
			fixture.(testProvisionFixture).sink.setPublisher(deps.Publisher)
		}
	}
	if deps.Tracker != nil && deps.Placement != nil {
		deps.Tracker.callbackStore = deps.Placement
	}

	callbacks := deps.Callbacks
	coordinator := deps.Coordinator
	if coordinator == nil && deps.Tracker != nil && deps.Placement != nil {
		deps.Tracker.bindingMu.Lock()
		coordinator = deps.Tracker.coordinator
		deps.Tracker.bindingMu.Unlock()
		if coordinator == nil {
			var err error
			coordinator, err = deps.Tracker.bindPlacementStore(deps.Placement)
			require.NoError(t, err)
		}
	}
	if callbacks == nil && coordinator != nil {
		callbackEvents := deps.CallbackEvents
		if callbackEvents == nil {
			// Older handler-unit fixtures observe their adapter output as Watermill
			// messages. Tests of the production ordering boundary inject a direct
			// callback sink explicitly, as Manager does.
			callbackEvents = callbackEventSinkFunc(func(leaseUUID string, status backend.ProvisionStatus, failure string) {
				publishLeaseStatusEvent(deps.Publisher, leaseUUID, status, failure)
			})
		}
		var err error
		callbacks, err = newCallbackServiceForTest(callbackServiceTestConfig{
			Coordinator:  coordinator,
			Chain:        deps.ChainClient,
			Acknowledger: deps.Acknowledger,
			Payloads:     deps.PayloadStore,
			Events:       callbackEvents,
			Backends:     deps.BackendRouter,
		})
		require.NoError(t, err)
	}

	handler, err := NewHandlerSet(HandlerDeps{
		Events:       deps.Orchestrator.HandlerEvents(),
		PayloadStore: deps.PayloadStore,
		Publisher:    deps.Publisher,
		Callbacks:    callbacks,
	})
	require.NoError(t, err)
	return handler
}

// startTestProvisioning mirrors the event handler's lease-claim discipline so
// unit tests cannot reintroduce an unclaimed backend-call path merely for
// convenience. A busy claim is the same idempotent duplicate outcome the
// handler observes when another operation already owns the lease.
func startTestProvisioning(
	t testing.TB,
	orchestrator *ProvisionOrchestrator,
	ctx context.Context,
	lease *billingtypes.Lease,
	opts ProvisionOpts,
) error {
	t.Helper()
	// Event-driven provisioning is dispatched only after an authoritative chain
	// read observes PENDING. Older unit fixtures omit the protobuf enum field,
	// so normalize that zero value at this test boundary without weakening the
	// production guard (covered directly by the non-pending regression tests).
	dispatchLease := lease
	if lease != nil && lease.State == billingtypes.LEASE_STATE_UNSPECIFIED {
		copy := *lease
		copy.State = billingtypes.LEASE_STATE_PENDING
		dispatchLease = &copy
	}
	if fixture, ok := testProvisionFixtures.Load(orchestrator); ok {
		provisionFixture := fixture.(testProvisionFixture)
		if reader := provisionFixture.reader; reader != nil {
			if dispatchLease.ProviderUuid == "" {
				copy := *dispatchLease
				copy.ProviderUuid = provisionFixture.providerUUID
				dispatchLease = &copy
			}
			reader.set(dispatchLease)
		}
	}
	var (
		request placement.ProvisionEventRequest
		err     error
	)
	if dispatchLease.MetaHash != nil {
		request, err = placement.NewPayloadProvisionEventRequest(
			dispatchLease.Uuid, dispatchLease.Tenant,
			func() ([]byte, error) { return slices.Clone(opts.Payload), nil },
		)
	} else {
		request, err = placement.NewProvisionEventRequest(
			dispatchLease.Uuid, dispatchLease.Tenant,
		)
	}
	if err != nil {
		return err
	}
	result := orchestrator.HandlerEvents().startFromCurrentLease(ctx, request)
	return result.Err()
}

func concreteTestCallbackStore(authority any) *placement.Store {
	switch authority := authority.(type) {
	case *placement.Store:
		return authority
	case *testPlacementAuthorityAdapter:
		return authority.authority
	case *testProviderBoundPlacementAuthority:
		if authority == nil {
			return nil
		}
		return concreteTestCallbackStore(authority.PlacementAuthorityStore)
	default:
		return nil
	}
}

func (manager *Manager) callbackPlacementStore() *placement.Store {
	// Manager intentionally retains only purpose-specific capabilities. Tests
	// that need a Store must keep the Store they supplied at construction rather
	// than recovering mutation authority from the composed service.
	return nil
}

// newTestPlacementAuthority gives ordinary manager/orchestrator tests the same
// durable, ready-by-explicit-projection dependency required in production.
// Constructor validation tests deliberately call the production constructors
// directly so missing and typed-nil authorities are never papered over.
func newTestPlacementAuthority(t testing.TB) *placement.Store {
	t.Helper()
	routes, err := placement.NewCallbackRouteFactory("https://provider.test/callback")
	require.NoError(t, err)
	store, err := placementstore.NewStore(
		filepath.Join(t.TempDir(), "placements.db"),
		placement.WithCallbackRouteFactory(routes),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func bindTestOperationCoordinator(
	t testing.TB,
	store *placement.Store,
) *placement.OperationCoordinator {
	t.Helper()
	require.NotNil(t, store)
	coordinator, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	return coordinator
}

func projectTestPlacementInventory(
	t testing.TB,
	reconciliation *placement.ReconciliationCoordinator,
	backendNames []string,
	projection placement.ReconciliationProjection,
) *placement.ProjectedReconciliationSweep {
	t.Helper()
	require.NotEmpty(t, backendNames, "typed placement projection requires an explicit topology")
	require.NotNil(t, reconciliation)
	sweep, err := reconciliation.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	reported := make(map[string][]backend.ProvisionInfo, len(backendNames))
	for leaseUUID, backendName := range projection.Placements {
		reported[backendName] = append(reported[backendName], backend.ProvisionInfo{
			LeaseUUID: leaseUUID, BackendName: backendName,
		})
	}
	for leaseUUID, candidates := range projection.Conflicts {
		for _, backendName := range candidates {
			reported[backendName] = append(reported[backendName], backend.ProvisionInfo{
				LeaseUUID: leaseUUID, BackendName: backendName,
			})
		}
	}
	untrustedByBackend := make(map[string][]string)
	for leaseUUID, backendCandidates := range projection.UntrustedPositives {
		for _, backendName := range backendCandidates {
			untrustedByBackend[backendName] = append(untrustedByBackend[backendName], leaseUUID)
		}
	}
	for _, backendName := range backendNames {
		storageID := testBackendStorageID(backendName)
		rows := reported[backendName]
		wantDisposition := placement.BackendInventoryAuthoritative
		if leaseUUIDs := untrustedByBackend[backendName]; len(leaseUUIDs) != 0 {
			for _, leaseUUID := range leaseUUIDs {
				rows = append(rows, backend.ProvisionInfo{
					LeaseUUID: leaseUUID, BackendName: backendName,
				})
			}
			storageID = backendidentity.ID{}
			wantDisposition = placement.BackendInventoryUntrusted
		}
		disposition := collectTestBackendInventory(
			t, reconciliation, sweep, backendName, storageID, rows, nil,
		)
		require.Equal(t, wantDisposition, disposition)
	}
	require.NoError(t, sweep.SealInventory())
	result, err := sweep.Project(placement.ReconciliationProjection{
		Placements:         projection.Placements,
		Conflicts:          projection.Conflicts,
		UntrustedPositives: projection.UntrustedPositives,
	})
	require.NoError(t, err)
	return result
}

type testTopologyConfigurator interface {
	ConfigureBackendTopologyWithStorageIdentities(
		[]string,
		map[string]backendidentity.ID,
	) error
}

func configureTestTopologyAuthority(
	store any,
	backendNames []string,
	identities map[string]backendidentity.ID,
) error {
	if concrete, ok := store.(*placement.Store); ok {
		return placementstore.ConfigureBackendTopologyWithStorageIdentities(
			concrete, backendNames, identities,
		)
	}
	configurator, ok := store.(testTopologyConfigurator)
	if !ok {
		return errors.New("test placement authority cannot configure topology")
	}
	return configurator.ConfigureBackendTopologyWithStorageIdentities(backendNames, identities)
}

func configureTestPlacementTopology(
	t testing.TB,
	store ReconcilerPlacement,
	backendNames []string,
) {
	t.Helper()
	identities := make(map[string]backendidentity.ID, len(backendNames))
	for _, backendName := range backendNames {
		identities[backendName] = testBackendStorageID(backendName)
	}
	require.NoError(t, configureTestTopologyAuthority(store, backendNames, identities))
}

var (
	pendingTestInventory     sync.Map
	testStoreReconciliations sync.Map
	testReconciliationStores sync.Map
)

func pendingInventoryKey(store ReconcilerPlacement) *placement.Store {
	return concreteTestCallbackStore(store)
}

func armTestPlacementTopology(
	t testing.TB,
	store ReconcilerPlacement,
	backendNames []string,
) {
	t.Helper()
	configureTestPlacementTopology(t, store, backendNames)
	key := pendingInventoryKey(store)
	require.NotNil(t, key)
	pendingTestInventory.Store(key, slices.Clone(backendNames))
}

func bindTestReconciliationCoordinator(
	t testing.TB,
	store ReconcilerPlacement,
	execution *placement.ExecutionCoordinator,
	chain placement.ReconciliationChain,
	payloads placement.AttemptPayloadReader,
	observe placement.ProvisionStartObserver,
) *placement.ReconciliationCoordinator {
	t.Helper()
	chain = canonicalReconciliationTestChain{ReconciliationChain: chain}
	setTestProviderControlPlane(t, execution, chain, nil)
	reconciliation, err := execution.ReconciliationCoordinator(payloads, observe)
	require.NoError(t, err)
	testReconciliationExecutions.Store(reconciliation, execution)
	t.Cleanup(func() { testReconciliationExecutions.Delete(reconciliation) })
	if fixture, ok := testExecutionInventoryRouters.Load(execution); ok {
		testReconciliationInventoryRouters.Store(reconciliation, fixture)
		t.Cleanup(func() { testReconciliationInventoryRouters.Delete(reconciliation) })
	}
	key := pendingInventoryKey(store)
	testStoreReconciliations.Store(key, reconciliation)
	testReconciliationStores.Store(reconciliation, store)
	t.Cleanup(func() {
		testStoreReconciliations.Delete(key)
		testReconciliationStores.Delete(reconciliation)
	})
	if value, pending := pendingTestInventory.LoadAndDelete(key); pending {
		backendNames := value.([]string)
		projectTestPlacementInventory(
			t, reconciliation, backendNames, placement.ReconciliationProjection{},
		)
		require.True(t, store.CurrentAdmissionBaseline().Valid())
	}
	return reconciliation
}

func testReconciliationPlacement(
	t testing.TB,
	reconciliation *placement.ReconciliationCoordinator,
) ReconcilerPlacement {
	t.Helper()
	value, ok := testReconciliationStores.Load(reconciliation)
	require.True(t, ok)
	return value.(ReconcilerPlacement)
}

func setTestReconciliationAcknowledger(
	t testing.TB,
	reconciliation *placement.ReconciliationCoordinator,
	ack Acknowledger,
) {
	t.Helper()
	value, ok := testReconciliationExecutions.Load(reconciliation)
	if !ok {
		return
	}
	execution := value.(*placement.ExecutionCoordinator)
	providerControl, mutable := testExecutionProviderControls.Load(execution)
	if !mutable {
		return
	}
	providerControl.(*testProviderControlPlane).set(nil, ack)
}

func testReconciliationCoordinator(
	t testing.TB,
	store ReconcilerPlacement,
) *placement.ReconciliationCoordinator {
	t.Helper()
	value, ok := testStoreReconciliations.Load(pendingInventoryKey(store))
	require.True(t, ok, "test Store must be bound through a reconciliation coordinator")
	return value.(*placement.ReconciliationCoordinator)
}

func testPlacementCallbackPair(
	t testing.TB,
	id operation.OperationID,
) placement.CallbackPair {
	t.Helper()
	pair, err := makeTestPlacementCallbackPair(id)
	require.NoError(t, err)
	return pair
}

func makeTestPlacementCallbackPair(
	id operation.OperationID,
) (placement.CallbackPair, error) {
	factory, err := placement.NewCallbackRouteFactory("https://provider.test/callback")
	if err != nil {
		return placement.CallbackPair{}, err
	}
	return factory.ForOperation(id)
}

func mustTestPlacementCallbackPair(id operation.OperationID) placement.CallbackPair {
	pair, err := makeTestPlacementCallbackPair(id)
	if err != nil {
		panic(err)
	}
	return pair
}

func testBackendRequestSnapshot(
	t testing.TB,
	store *placement.Store,
) placement.BackendRequestSnapshot {
	t.Helper()
	snapshot, err := store.MintBackendRequestSnapshot(
		"tenant-test",
		[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
	)
	require.NoError(t, err)
	return snapshot
}

func beginTestNewPlacementAttempt(
	t testing.TB,
	store *placement.Store,
	authority *placement.ProvisionCoordinator,
	leaseUUID, backendName string,
	operationID operation.OperationID,
) operation.OperationID {
	return beginTestNewPlacementAttemptWithFingerprint(
		t, store, authority, leaseUUID, backendName, operationID, placement.PayloadFingerprint{},
	)
}

func beginTestNewPlacementAttemptWithFingerprint(
	t testing.TB,
	store *placement.Store,
	authority *placement.ProvisionCoordinator,
	leaseUUID, backendName string,
	operationID operation.OperationID,
	fingerprint placement.PayloadFingerprint,
) operation.OperationID {
	return beginTestNewPlacementAttemptWithSnapshot(
		t, store, authority, leaseUUID, backendName, operationID, fingerprint,
		testBackendRequestSnapshot(t, store),
	)
}

func beginTestNewPlacementAttemptWithSnapshot(
	t testing.TB,
	store *placement.Store,
	authority *placement.ProvisionCoordinator,
	leaseUUID, backendName string,
	operationID operation.OperationID,
	fingerprint placement.PayloadFingerprint,
	requestSnapshot placement.BackendRequestSnapshot,
) operation.OperationID {
	t.Helper()
	require.NotNil(t, store)
	require.NotNil(t, authority)
	chainValue, ok := callbackProvisionChains.Load(authority)
	require.True(t, ok, "test provision coordinator must retain its bound chain reader")
	chain := chainValue.(*callbackChainStub)
	items := requestSnapshot.Items()
	chainItems := make([]billingtypes.LeaseItem, 0, len(items))
	for _, item := range items {
		chainItems = append(chainItems, billingtypes.LeaseItem{
			SkuUuid: item.SKU, Quantity: uint64(item.Quantity),
			ServiceName: item.ServiceName, CustomDomain: item.CustomDomain,
		})
	}
	chain.getLease = func(context.Context, string) (*billingtypes.Lease, error) {
		return &billingtypes.Lease{
			Uuid: leaseUUID, Tenant: requestSnapshot.Tenant(),
			ProviderUuid: placementstore.ProviderUUID,
			State:        billingtypes.LEASE_STATE_PENDING, Items: chainItems,
		}, nil
	}
	if coordinatorValue, known := callbackOperationCoordinators.Load(authority); known {
		coordinator := coordinatorValue.(*placement.OperationCoordinator)
		if executionValue, bound := testOperationExecutions.Load(coordinator); bound {
			execution := executionValue.(*placement.ExecutionCoordinator)
			if controlValue, exists := testExecutionProviderControls.Load(execution); exists {
				control := controlValue.(*testProviderControlPlane)
				original, _, _ := control.snapshot()
				control.set(canonicalReconciliationTestChain{
					ReconciliationChain: chain,
				}, nil)
				defer control.set(original, nil)
			}
		}
	}
	event, err := placement.NewProvisionEventRequest(leaseUUID, requestSnapshot.Tenant())
	require.NoError(t, err)
	result := authority.ExecuteCurrentLease(context.Background(), event)
	if result.Err() == nil {
		return operation.OperationID{}
	}
	actual := store.Lookup(leaseUUID).AttemptOperationID()
	require.True(t, actual.Valid(), "failed fixture dispatch must preserve an exact attempt: %v", result.Err())
	return actual
}

func seedTestConfirmedPlacements(
	t testing.TB,
	store ReconcilerPlacement,
	backendNames []string,
	placements map[string]string,
) {
	t.Helper()
	projectTestPlacementInventory(t, testReconciliationCoordinator(t, store), backendNames, placement.ReconciliationProjection{
		Placements: placements,
	})
}

// seedTestTypedConfirmedPlacements establishes confirmed ownership through the
// same write-ahead attempt transition as production. Use it when a fixture
// needs lifecycle callback authority; passive inventory projection deliberately
// cannot manufacture that authority after the typed-capability migration.
func seedTestTypedConfirmedPlacements(
	t testing.TB,
	store PlacementAuthorityStore,
	backendNames []string,
	placements map[string]string,
) *placement.OperationCoordinator {
	t.Helper()
	entries := make([]backend.BackendEntry, 0, len(backendNames))
	for index, backendName := range backendNames {
		entries = append(entries, backend.BackendEntry{
			Backend:   backend.NewMockBackend(backend.MockBackendConfig{Name: backendName}),
			IsDefault: index == 0,
		})
	}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: entries})
	require.NoError(t, err)
	coordinator, _ := seedTestTypedConfirmedPlacementsWithExecution(
		t, store, router, placements,
	)
	return coordinator
}

func seedTestTypedConfirmedPlacementsWithExecution(
	t testing.TB,
	store PlacementAuthorityStore,
	router BackendRouter,
	placements map[string]string,
) (*placement.OperationCoordinator, *placement.ExecutionCoordinator) {
	t.Helper()
	backendNames := backendTopologyNames(router)
	armTestPlacementTopology(t, store, backendNames)
	coordinator, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindTestBackendRuntime(t, coordinator, router)
	reconciliation := bindTestReconciliationCoordinator(
		t, store, execution,
		testReconciliationChain{ProvisionLeaseReader: &callbackChainStub{}}, nil, nil,
	)
	sweep, err := reconciliation.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	byBackend := make(map[string][]backend.ProvisionInfo, len(backendNames))
	for leaseUUID, backendName := range placements {
		id, parseErr := operation.ParseID(uuid.NewString())
		require.NoError(t, parseErr)
		byBackend[backendName] = append(byBackend[backendName], backend.ProvisionInfo{
			LeaseUUID: leaseUUID, BackendName: backendName,
			ProviderUUID: reconciliation.ProviderUUID(), Tenant: "tenant-test",
			LifecycleGeneration: &backend.LifecycleGenerationObservation{
				Kind: backend.LifecycleGenerationTyped,
				ID:   id.String(),
			},
		})
	}
	for _, backendName := range backendNames {
		disposition := collectTestBackendInventory(
			t, reconciliation, sweep, backendName, testBackendStorageID(backendName),
			byBackend[backendName], nil,
		)
		require.Equal(t, placement.BackendInventoryAuthoritative, disposition)
	}
	require.NoError(t, sweep.SealInventory())
	_, err = sweep.Project(placement.ReconciliationProjection{Placements: placements})
	require.NoError(t, err)
	pendingTestInventory.Delete(pendingInventoryKey(store))
	return coordinator, execution
}

func confirmPlacementOperationForTest(
	coordinator *placement.OperationCoordinator,
	leaseUUID, backendName string,
	id operation.OperationID,
) (bool, error) {
	if coordinator == nil || !coordinator.RuntimeController().Contains(leaseUUID) {
		return false, nil
	}
	callbacks, err := authenticatedCallbackCoordinatorForTest(
		coordinator,
		&callbackChainStub{getLease: func(context.Context, string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: placementstore.ProviderUUID,
				State: billingtypes.LEASE_STATE_PENDING,
			}, nil
		}},
		callbackAcknowledgerFunc(func(context.Context, string) (bool, string, error) {
			return true, "tx-test", nil
		}),
	)
	if err != nil {
		return false, err
	}
	proof, err := callbackProofForTest(backend.CallbackPayload{
		LeaseUUID: leaseUUID, Status: backend.CallbackStatusSuccess,
		OperationID: id.String(), BackendStorageID: testBackendStorageID(backendName).String(),
	})
	if err != nil {
		return false, err
	}
	_, err = callbacks.Apply(context.Background(), proof)
	if err != nil {
		return false, err
	}
	return true, nil
}

func deleteTestPlacement(
	t testing.TB,
	store ReconcilerPlacement,
	leaseUUID string,
) {
	t.Helper()
	revision := store.Lookup(leaseUUID).RecordRevision()
	require.True(t, revision.Valid(), "test placement must exist before deletion")
	deleter, ok := any(store).(interface {
		DeleteRecord(placement.RecordRevision) (bool, error)
	})
	require.True(t, ok, "test placement authority must expose fixture deletion")
	deleted, err := deleter.DeleteRecord(revision)
	require.NoError(t, err)
	require.True(t, deleted)
}

func armTestPlacementAdmission(
	t testing.TB,
	store ReconcilerPlacement,
	router BackendRouter,
) {
	t.Helper()
	backendNames := backendTopologyNames(router)
	if len(backendNames) == 0 {
		// Read-only/no-backend fixtures never reach admission. Tests that dispatch
		// provisioning must expose the same topology production routers do.
		return
	}
	armTestPlacementTopology(t, store, backendNames)
}

func newTestManager(
	t testing.TB,
	cfg ManagerConfig,
	router *backend.Router,
	chainClient ManagerChainClient,
) (*Manager, error) {
	t.Helper()
	if cfg.PlacementStore == nil {
		cfg.PlacementStore = newTestPlacementAuthority(t)
	}
	if !cfg.CallbackProofConsumer.Valid() {
		cfg.CallbackProofConsumer = callbackTestProofConsumer
	}
	// Production accepts only the concrete provider-bound Store. Historical
	// unit fixtures used display labels such as "provider-1"; normalize those
	// fixtures to the canonical authority instead of weakening the constructor
	// with a test-only provider wrapper.
	cfg.ProviderUUID = placementstore.ProviderUUID
	backendNames := backendTopologyNames(router)
	if len(backendNames) > 0 {
		configureTestPlacementTopology(t, cfg.PlacementStore, backendNames)
	}
	manager, err := NewManager(cfg, router, canonicalManagerTestChain{chainClient})
	if err != nil {
		return nil, err
	}
	testManagerPlacements.Store(manager, cfg.PlacementStore)
	testManagerChains.Store(manager, chainClient)
	t.Cleanup(func() {
		testManagerPlacements.Delete(manager)
		testManagerChains.Delete(manager)
	})
	armTestPlacementAdmission(t, cfg.PlacementStore, router)
	if len(backendNames) > 0 {
		bindTestReconciliationCoordinator(
			t, cfg.PlacementStore, manager.executionCoordinator,
			chainClient, manager.PayloadStore(), nil,
		)
	}
	return manager, nil
}

// managerTestPlacement returns the Store explicitly supplied to newTestManager.
// It is test-only observation plumbing: Manager deliberately exposes no raw
// Store or Registry escape hatch in production.
func managerTestPlacement(manager *Manager) *placement.Store {
	if manager == nil {
		return nil
	}
	store, ok := testManagerPlacements.Load(manager)
	if !ok {
		return nil
	}
	return store.(*placement.Store)
}

// legacyTestPlacementStore is confined to test fixtures that predate opaque
// placement capabilities. Production code never accepts this raw mutation
// surface; test adapters translate it into ReconcilerPlacement or
// ProvisionPlacement while the fixture suite is migrated mechanically.
type legacyTestPlacementStore interface {
	PlacementView
	SnapshotRevision() uint64
	BeginInventorySnapshot() uint64
	EndInventorySnapshot(revision uint64)
	SetAttempting(leaseUUID, backendName string) (uint64, error)
	SetAttemptingIfNotNewer(leaseUUID, backendName string, maxRevision uint64) (uint64, bool, error)
	Confirm(leaseUUID, backendName string) error
	ConfirmAttemptIfRevision(leaseUUID, backendName string, revision uint64) (bool, error)
	ClearAttempt(leaseUUID, backendName string) error
	ClearAttemptIfRevision(leaseUUID, backendName string, revision uint64) (bool, error)
	Delete(leaseUUID string) error
	DeleteIfRevision(leaseUUID string, revision uint64) (bool, error)
	SetBatchIfNotNewer(placements map[string]string, maxRevision uint64) (map[string]uint64, map[string]struct{}, error)
	SetConflictsIfNotNewer(conflicts map[string][]string, maxRevision uint64) (map[string]uint64, map[string]struct{}, error)
	ClearConflictsIfNotNewer(leases map[string]struct{}, maxRevision uint64) error
}

func newTestProvisionOrchestrator(
	t testing.TB,
	providerUUID, callbackBaseURL string,
	router BackendRouter,
	tracker *testOperationRegistry,
	store any,
	leaseReaders ...placement.ProvisionLeaseReader,
) *ProvisionOrchestrator {
	t.Helper()
	require.NotNil(t, tracker)
	if len(router.Backends()) == 0 {
		defaultBackend := &mockManagerBackend{name: "test-backend"}
		router = &mockBackendRouter{
			routeFn: func(string) backend.Backend { return defaultBackend },
			getBackendByNameFn: func(name string) backend.Backend {
				if name == defaultBackend.name {
					return defaultBackend
				}
				return nil
			},
			backendsFn: func() []backend.Backend { return []backend.Backend{defaultBackend} },
		}
	}

	authority := testPlacementAuthority(t, store, router)
	coordinator, err := tracker.bindPlacementStore(authority)
	require.NoError(t, err)
	execution := bindTestBackendRuntime(t, coordinator, router)
	var leaseReader placement.ProvisionLeaseReader
	var mutableReader *mutableTestProvisionLeaseReader
	if len(leaseReaders) != 0 {
		leaseReader = leaseReaders[0]
	} else {
		mutableReader = &mutableTestProvisionLeaseReader{leases: make(map[string]*billingtypes.Lease)}
		leaseReader = mutableReader
	}
	startSink := &testProvisionStartSink{}
	callbackChain := &callbackChainStub{
		getLease: leaseReader.GetLease,
	}
	chain := testReconciliationChain{ProvisionLeaseReader: callbackChain}
	setTestProviderControlPlane(t, execution, chain, nil)
	provision, err := execution.ProvisionCoordinator(
		func(leaseUUID, _ string) { startSink.PublishProvisionStarting(leaseUUID) },
	)
	require.NoError(t, err)
	reconciliation := bindTestReconciliationCoordinator(
		t, authority, execution,
		chain, nil, nil,
	)
	if adapter, ok := authority.(*testPlacementAuthorityAdapter); ok {
		adapter.reconciliation = reconciliation
	}
	orch, err := NewProvisionOrchestrator(provision)
	require.NoError(t, err)
	testProvisionFixtures.Store(orch, testProvisionFixture{
		reader: mutableReader, sink: startSink, providerUUID: reconciliation.ProviderUUID(),
	})
	t.Cleanup(func() { testProvisionFixtures.Delete(orch) })
	testReconciliationCoordinators.Store(provision, reconciliation)
	t.Cleanup(func() { testReconciliationCoordinators.Delete(provision) })
	callbackProvisionCoordinators.Store(coordinator, provision)
	callbackProvisionChains.Store(provision, callbackChain)
	callbackOperationCoordinators.Store(provision, coordinator)
	t.Cleanup(func() {
		callbackProvisionChains.Delete(provision)
		callbackOperationCoordinators.Delete(provision)
	})
	tracker.callbackStore = concreteTestCallbackStore(authority)
	require.NotNil(t, tracker.callbackStore)
	return orch
}

func testPlacementAuthority(
	t testing.TB,
	store any,
	router BackendRouter,
) PlacementAuthorityStore {
	t.Helper()
	if store == nil {
		authority := newTestPlacementAuthority(t)
		armTestPlacementAdmission(t, authority, router)
		return authority
	}
	if authority, ok := store.(PlacementAuthorityStore); ok {
		require.False(t, util.IsNilInterface(authority),
			"typed-nil placement authority must be tested through the production constructor")
		armTestPlacementAdmission(t, authority, router)
		return authority
	}
	raw, ok := store.(legacyTestPlacementStore)
	require.True(t, ok, "test placement fixture must expose a typed authority or raw test adapter")
	adapter := &testPlacementAuthorityAdapter{
		legacyTestPlacementStore: raw,
		authority:                newTestPlacementAuthority(t),
		attempts:                 make(map[placement.AttemptToken]testAttemptIdentity),
	}
	armTestPlacementAdmission(t, adapter, router)
	return adapter
}

// testReconcilerPlacement converts legacy observable placement fixtures into a
// typed authority once, then mirrors typed reconciler writes back into the raw
// fixture for assertions. Reconciler itself sees only the production port and
// can consume only capabilities minted by the private durable store.
func testReconcilerPlacement(
	t testing.TB,
	store any,
	router BackendRouter,
	registry *testOperationRegistry,
	coordinator *placement.OperationCoordinator,
) (ReconcilerPlacement, *placement.OperationCoordinator, *placement.ExecutionCoordinator) {
	t.Helper()
	bindExecution := func(coordinator *placement.OperationCoordinator) *placement.ExecutionCoordinator {
		t.Helper()
		return bindTestBackendRuntime(t, coordinator, router)
	}
	if store == nil {
		authority := newTestPlacementAuthority(t)
		armTestPlacementAdmission(t, authority, router)
		if coordinator == nil {
			var err error
			coordinator, err = registry.bindPlacementStore(authority)
			require.NoError(t, err)
		}
		return authority, coordinator, bindExecution(coordinator)
	}
	if authority, ok := store.(ReconcilerPlacement); ok {
		armTestPlacementAdmission(t, authority, router)
		if coordinator == nil {
			binder, ok := store.(PlacementAuthorityStore)
			require.True(t, ok)
			var err error
			coordinator, err = registry.bindPlacementStore(binder)
			require.NoError(t, err)
		}
		return authority, coordinator, bindExecution(coordinator)
	}
	raw, ok := store.(legacyTestPlacementStore)
	require.True(t, ok, "test placement fixture must expose a typed reconciler port or legacy test adapter")

	base := &testPlacementAuthorityAdapter{
		legacyTestPlacementStore: raw,
		authority:                newTestPlacementAuthority(t),
		attempts:                 make(map[placement.AttemptToken]testAttemptIdentity),
	}
	armTestPlacementAdmission(t, base, router)
	if coordinator == nil {
		var err error
		coordinator, err = registry.bindPlacementStore(base.authority)
		require.NoError(t, err)
	}
	execution := bindExecution(coordinator)
	reconciliation := bindTestReconciliationCoordinator(
		t, base.authority, execution, &callbackChainStub{}, nil, nil,
	)
	base.reconciliation = reconciliation
	adapter := &testReconcilerPlacementAdapter{
		testPlacementAuthorityAdapter: base,
		topology:                      backendTopologyNames(router),
		overlay:                       make(map[string]placement.Placement),
	}
	require.NoError(t, adapter.seedRawPlacements(t))
	return adapter, coordinator, execution
}

type testReconcilerPlacementAdapter struct {
	*testPlacementAuthorityAdapter

	topology []string
	// overlay retains deliberately unrepresentable legacy/corrupt fixtures.
	// Their invalid typed revision makes them non-authoritative, while keeping
	// them visible exercises the reconciler's defensive fail-closed branches.
	overlay map[string]placement.Placement
}

func (a *testReconcilerPlacementAdapter) seedRawPlacements(t testing.TB) error {
	t.Helper()
	placements := make(map[string]string)
	conflicts := make(map[string][]string)
	attempts := make(map[string]placement.Placement)
	for leaseUUID, current := range a.legacyTestPlacementStore.List() {
		if !a.representable(current) {
			a.overlay[leaseUUID] = current
			continue
		}
		if current.Conflict {
			conflicts[leaseUUID] = current.ConflictBackends
		} else if current.Backend != "" {
			placements[leaseUUID] = current.Backend
		}
		if current.Attempt != "" {
			attempts[leaseUUID] = current
		}
	}
	if len(placements) != 0 || len(conflicts) != 0 {
		projectTestPlacementInventory(t, a.reconciliation, a.topology, placement.ReconciliationProjection{
			Placements: placements,
			Conflicts:  conflicts,
		})
		pendingTestInventory.Delete(a.authority)
	}

	if len(attempts) != 0 {
		return errors.New("legacy raw attempt fixtures must use a high-level provision application")
	}
	return nil
}

func (a *testReconcilerPlacementAdapter) representable(current placement.Placement) bool {
	configured := func(name string) bool {
		return name == "" || slices.Contains(a.topology, name)
	}
	if !configured(current.Backend) || !configured(current.Attempt) {
		return false
	}
	if !current.Conflict {
		return true
	}
	if current.ConflictOwnersUnknown || len(current.ConflictBackends) < 2 {
		return false
	}
	for _, backendName := range current.ConflictBackends {
		if !configured(backendName) {
			return false
		}
	}
	return true
}

func (a *testReconcilerPlacementAdapter) Lookup(leaseUUID string) placement.Placement {
	if current, ok := a.overlay[leaseUUID]; ok {
		return current
	}
	current := a.authority.Lookup(leaseUUID)
	raw := a.legacyTestPlacementStore.Lookup(leaseUUID)
	if raw.State() == current.State() && !raw.SetAt.IsZero() {
		current.SetAt = raw.SetAt
	}
	return current
}

func (a *testReconcilerPlacementAdapter) List() map[string]placement.Placement {
	result := a.authority.List()
	for leaseUUID := range result {
		result[leaseUUID] = a.Lookup(leaseUUID)
	}
	for leaseUUID, current := range a.overlay {
		result[leaseUUID] = current
	}
	return result
}

var _ ReconcilerPlacement = (*testReconcilerPlacementAdapter)(nil)

// testPlacementAuthorityAdapter keeps the extensive legacy placement mock
// assertions useful while exercising production through opaque capabilities.
// The real bbolt store issues and validates every token; successful mutations
// are mirrored into the raw mock only for test observation/error injection.
type testPlacementAuthorityAdapter struct {
	legacyTestPlacementStore
	authority      *placement.Store
	reconciliation *placement.ReconciliationCoordinator

	mu       sync.Mutex
	attempts map[placement.AttemptToken]testAttemptIdentity
	topology []string
}

func (a *testPlacementAuthorityAdapter) VerifyProviderUUID(providerUUID string) error {
	return a.authority.VerifyProviderUUID(providerUUID)
}

func (a *testPlacementAuthorityAdapter) BindOperationCoordinator(
	countObserver func(int),
) (*placement.OperationCoordinator, error) {
	return a.authority.BindOperationCoordinator(countObserver)
}

type testProviderBoundPlacementAuthority struct {
	PlacementAuthorityStore
	providerUUID string
}

func (authority *testProviderBoundPlacementAuthority) VerifyProviderUUID(providerUUID string) error {
	if authority == nil || authority.PlacementAuthorityStore == nil ||
		providerUUID == "" || providerUUID != authority.providerUUID {
		return placement.ErrProviderAuthorityMismatch
	}
	return nil
}

func (authority *testProviderBoundPlacementAuthority) ConfigureBackendTopologyWithStorageIdentities(
	backendNames []string,
	storageIDs map[string]backendidentity.ID,
) error {
	return configureTestTopologyAuthority(
		authority.PlacementAuthorityStore, backendNames, storageIDs,
	)
}

type testProviderBoundReconcilerPlacement struct {
	ReconcilerPlacement
	providerUUID string
}

func (authority *testProviderBoundReconcilerPlacement) VerifyProviderUUID(providerUUID string) error {
	if authority == nil || authority.ReconcilerPlacement == nil ||
		providerUUID == "" || providerUUID != authority.providerUUID {
		return placement.ErrProviderAuthorityMismatch
	}
	return nil
}

func (authority *testProviderBoundReconcilerPlacement) ConfigureBackendTopologyWithStorageIdentities(
	backendNames []string,
	storageIDs map[string]backendidentity.ID,
) error {
	return configureTestTopologyAuthority(
		authority.ReconcilerPlacement, backendNames, storageIDs,
	)
}

// constructorPlacementAuthoritySpy uses the real provider-bound store while
// recording whether construction advanced to topology validation.
type constructorPlacementAuthoritySpy struct {
	PlacementAuthorityStore
	topologyChecks int
}

func (authority *constructorPlacementAuthoritySpy) VerifyBackendTopology(names []string) error {
	authority.topologyChecks++
	return authority.PlacementAuthorityStore.VerifyBackendTopology(names)
}

type testAttemptIdentity struct {
	leaseUUID   string
	backendName string
}

func (a *testPlacementAuthorityAdapter) Lookup(leaseUUID string) placement.Placement {
	current := a.legacyTestPlacementStore.Lookup(leaseUUID)
	internal := a.authority.Lookup(leaseUUID)
	// Joined dispatch settlement intentionally bypasses this compatibility
	// adapter: production owns the concrete Store, not an independently
	// spliceable mutation port. Once an adapter-issued attempt has reached that
	// coordinator, the concrete Store is therefore the authoritative test view.
	a.mu.Lock()
	joinedAttempt := false
	for _, identity := range a.attempts {
		if identity.leaseUUID == leaseUUID {
			joinedAttempt = true
			break
		}
	}
	a.mu.Unlock()
	if joinedAttempt || internal.State() != placement.StateAbsent {
		return internal
	}
	if current.State() != placement.StateConfirmed || current.Attempt != "" {
		return current
	}
	if internal.State() == placement.StateAbsent {
		if err := a.projectConfirmed(leaseUUID, current.Backend); err != nil {
			return current
		}
		internal = a.authority.Lookup(leaseUUID)
	}
	if internal.State() == placement.StateConfirmed && internal.Backend == current.Backend {
		return internal
	}
	return current
}

func (a *testPlacementAuthorityAdapter) VerifyBackendTopology(names []string) error {
	return a.authority.VerifyBackendTopology(names)
}

func (a *testPlacementAuthorityAdapter) ConfigureBackendTopologyWithStorageIdentities(
	names []string,
	identities map[string]backendidentity.ID,
) error {
	if err := placementstore.ConfigureBackendTopologyWithStorageIdentities(
		a.authority, names, identities,
	); err != nil {
		return err
	}
	a.topology = slices.Clone(names)
	return nil
}

func (a *testPlacementAuthorityAdapter) CurrentAdmissionBaseline() placement.AdmissionBaseline {
	return a.authority.CurrentAdmissionBaseline()
}

func (a *testPlacementAuthorityAdapter) ExpectedBackendStorageIdentity(
	backendName string,
) (backendidentity.ID, bool) {
	return a.authority.ExpectedBackendStorageIdentity(backendName)
}

func (a *testPlacementAuthorityAdapter) projectConfirmed(leaseUUID, backendName string) error {
	if a.reconciliation == nil || !a.reconciliation.Valid() {
		return errors.New("test reconciliation coordinator is unavailable")
	}
	sweep, err := a.reconciliation.BeginSweep()
	if err != nil {
		return err
	}
	defer sweep.End()
	for _, name := range a.topology {
		present := []backend.ProvisionInfo(nil)
		if name == backendName {
			present = []backend.ProvisionInfo{{
				LeaseUUID: leaseUUID, BackendName: backendName,
			}}
		}
		storageID := testBackendStorageID(name)
		value, ok := testReconciliationInventoryRouters.Load(a.reconciliation)
		if !ok {
			return errors.New("test reconciliation inventory runtime is unavailable")
		}
		fixture := value.(*testInventoryRouter).backends[name]
		if fixture == nil {
			return fmt.Errorf("test reconciliation backend %q is unavailable", name)
		}
		fixture.stage(storageID, present, nil)
		provisionReceipt, collectErr := sweep.CollectProvisionInventory(context.Background(), name)
		if collectErr != nil {
			fixture.clearStage()
			return collectErr
		}
		retentionReceipt, collectErr := sweep.CollectRetentionInventory(context.Background(), name)
		fixture.clearStage()
		if collectErr != nil {
			return collectErr
		}
		if disposition, collectErr := sweep.RecordBackendInventory(
			provisionReceipt, retentionReceipt,
		); collectErr != nil {
			return collectErr
		} else if disposition != placement.BackendInventoryAuthoritative {
			return fmt.Errorf("test reconciliation backend %q inventory was not authoritative", name)
		}
	}
	if err := sweep.SealInventory(); err != nil {
		return err
	}
	_, err = sweep.Project(placement.ReconciliationProjection{
		Placements: map[string]string{leaseUUID: backendName},
	})
	return err
}

var _ PlacementAuthorityStore = (*testPlacementAuthorityAdapter)(nil)
