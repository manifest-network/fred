package placement

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

const causalOutcomeTestSecret = "placement-causal-outcome-test-secret"

type causalOutcomeTestIdentity struct{ id backendidentity.ID }

func (identity causalOutcomeTestIdentity) ExpectedBackendStorageIdentity(
	string,
) (backendidentity.ID, bool) {
	return identity.id, identity.id.Valid()
}

func causalOutcomeHTTPClient(
	t testing.TB,
	status int,
	body string,
) *backend.HTTPClient {
	t.Helper()
	id, err := backendidentity.Parse("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(backendidentity.ResponseHeader, id.String())
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	client, err := backend.NewIdentityBoundHTTPClient(backend.HTTPClientConfig{
		Name: "backend-a", BaseURL: server.URL, Secret: causalOutcomeTestSecret,
	}, causalOutcomeTestIdentity{id: id})
	require.NoError(t, err)
	return client
}

func refusedProvisionOutcomeForTest(t testing.TB) backend.ProvisionCallOutcome {
	t.Helper()
	client := causalOutcomeHTTPClient(
		t, http.StatusBadRequest, `{"error":"invalid request"}`,
	)
	return backend.InvokeProvision(t.Context(), client, backend.ProvisionRequest{})
}

func refusedRestoreOutcomeForTest(t testing.TB) backend.RestoreCallOutcome {
	t.Helper()
	client := causalOutcomeHTTPClient(
		t, http.StatusBadRequest, `{"error":"invalid request"}`,
	)
	return backend.InvokeRestore(t.Context(), client, backend.RestoreRequest{})
}

// NewOperationCoordinator preserves concise focused package tests without
// reopening the production constructor. External code must bind through Store.
func NewOperationCoordinator(
	store *Store,
	registry *operation.Registry,
) (*OperationCoordinator, error) {
	return newOperationCoordinator(store, registry)
}

// executionTestBackend is the deliberately small physical collaborator used
// by coordinator tests. Each mutation can be replaced independently while the
// construction-bound runtime still resolves it by exact name.
type executionTestBackend struct {
	name        string
	provision   func(context.Context, backend.ProvisionRequest) error
	restore     func(context.Context, backend.RestoreRequest) error
	deprovision func(context.Context, string) error
	restart     func(context.Context, backend.RestartRequest) error
	update      func(context.Context, backend.UpdateRequest) error
	get         func(context.Context, string) (*backend.ProvisionInfo, error)
}

func (client *executionTestBackend) Name() string { return client.name }
func (client *executionTestBackend) Provision(ctx context.Context, request backend.ProvisionRequest) error {
	if client.provision != nil {
		return client.provision(ctx, request)
	}
	return nil
}
func (client *executionTestBackend) Restore(ctx context.Context, request backend.RestoreRequest) error {
	if client.restore != nil {
		return client.restore(ctx, request)
	}
	return nil
}
func (client *executionTestBackend) Deprovision(ctx context.Context, leaseUUID string) error {
	if client.deprovision != nil {
		return client.deprovision(ctx, leaseUUID)
	}
	return nil
}
func (client *executionTestBackend) Restart(ctx context.Context, request backend.RestartRequest) error {
	if client.restart != nil {
		return client.restart(ctx, request)
	}
	return nil
}
func (client *executionTestBackend) Update(ctx context.Context, request backend.UpdateRequest) error {
	if client.update != nil {
		return client.update(ctx, request)
	}
	return nil
}
func (client *executionTestBackend) GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
	if client.get != nil {
		return client.get(ctx, leaseUUID)
	}
	return &backend.ProvisionInfo{LeaseUUID: leaseUUID, BackendName: client.name}, nil
}
func (*executionTestBackend) GetInfo(context.Context, string) (*backend.LeaseInfo, error) {
	return nil, nil
}
func (*executionTestBackend) ListProvisions(context.Context) ([]backend.ProvisionInfo, error) {
	return nil, nil
}
func (*executionTestBackend) LookupProvisions(context.Context, []string) ([]backend.ProvisionInfo, error) {
	return nil, nil
}
func (*executionTestBackend) Health(context.Context) error       { return nil }
func (*executionTestBackend) RefreshState(context.Context) error { return nil }
func (*executionTestBackend) GetLogs(context.Context, string, int) (map[string]string, error) {
	return nil, nil
}
func (*executionTestBackend) ReconcileCustomDomain(context.Context, string, []backend.LeaseItem) error {
	return nil
}
func (*executionTestBackend) GetReleases(context.Context, string) ([]backend.ReleaseInfo, error) {
	return nil, nil
}
func (*executionTestBackend) GetLoadStats(context.Context) (*backend.LoadStats, error) {
	return &backend.LoadStats{}, nil
}
func (*executionTestBackend) ListRetentions(context.Context) ([]backend.RetainedLease, error) {
	return nil, nil
}
func (*executionTestBackend) ListProvisionsWithIdentity(context.Context) ([]backend.ProvisionInfo, backendidentity.ID, error) {
	return nil, backendidentity.ID{}, nil
}
func (*executionTestBackend) ListRetentionsWithIdentity(context.Context) ([]backend.RetainedLease, backendidentity.ID, error) {
	return nil, backendidentity.ID{}, nil
}

type executionTestRuntime struct{ backends map[string]backend.Backend }

// misdirectingExecutionRuntime advertises one exact topology but returns a
// differently named client from exact lookup. It models a runtime that changes
// or lies after construction without making the durable-topology fixture itself
// invalid.
type misdirectingExecutionRuntime struct {
	*executionTestRuntime
	resolved backend.Backend
}

func (runtime *misdirectingExecutionRuntime) GetBackendByName(string) backend.Backend {
	return runtime.resolved
}

// testProviderControlPlane is the package-test composition adapter. Production
// has no mutable equivalent: Manager binds its one real chain+ack plane once.
// Tests may replace the delegate before constructing a purpose coordinator so
// focused fakes do not need unrelated broad methods.
type testProviderControlPlane struct {
	mu       sync.RWMutex
	delegate any
	provider string
}

func (control *testProviderControlPlane) set(delegate any) {
	control.mu.Lock()
	defer control.mu.Unlock()
	control.delegate = delegate
}

func (control *testProviderControlPlane) current() any {
	control.mu.RLock()
	defer control.mu.RUnlock()
	return control.delegate
}

func (control *testProviderControlPlane) GetLease(
	ctx context.Context,
	leaseUUID string,
) (*billingtypes.Lease, error) {
	if reader, ok := control.current().(interface {
		GetLease(context.Context, string) (*billingtypes.Lease, error)
	}); ok {
		return reader.GetLease(ctx, leaseUUID)
	}
	return &billingtypes.Lease{
		Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: control.provider,
		State: billingtypes.LEASE_STATE_PENDING,
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1, ServiceName: "app"}},
	}, nil
}

func (control *testProviderControlPlane) GetPendingLeases(
	ctx context.Context,
	providerUUID string,
) ([]billingtypes.Lease, error) {
	if reader, ok := control.current().(interface {
		GetPendingLeases(context.Context, string) ([]billingtypes.Lease, error)
	}); ok {
		return reader.GetPendingLeases(ctx, providerUUID)
	}
	return nil, nil
}

func (control *testProviderControlPlane) GetActiveLeasesByProvider(
	ctx context.Context,
	providerUUID string,
) ([]billingtypes.Lease, error) {
	if reader, ok := control.current().(interface {
		GetActiveLeasesByProvider(context.Context, string) ([]billingtypes.Lease, error)
	}); ok {
		return reader.GetActiveLeasesByProvider(ctx, providerUUID)
	}
	return nil, nil
}

func (control *testProviderControlPlane) RejectLeases(
	ctx context.Context,
	leaseUUIDs []string,
	reason string,
) (uint64, []string, error) {
	if rejecter, ok := control.current().(interface {
		RejectLeases(context.Context, []string, string) (uint64, []string, error)
	}); ok {
		return rejecter.RejectLeases(ctx, leaseUUIDs, reason)
	}
	return 0, nil, nil
}

func (control *testProviderControlPlane) CloseLeases(
	ctx context.Context,
	leaseUUIDs []string,
	reason string,
) (uint64, []string, error) {
	if closer, ok := control.current().(interface {
		CloseLeases(context.Context, []string, string) (uint64, []string, error)
	}); ok {
		return closer.CloseLeases(ctx, leaseUUIDs, reason)
	}
	return 0, nil, nil
}

func (control *testProviderControlPlane) Acknowledge(
	ctx context.Context,
	leaseUUID string,
) (bool, string, error) {
	if acknowledger, ok := control.current().(interface {
		Acknowledge(context.Context, string) (bool, string, error)
	}); ok {
		return acknowledger.Acknowledge(ctx, leaseUUID)
	}
	return true, "", nil
}

func setProviderControlPlaneForTest(
	t testing.TB,
	execution *ExecutionCoordinator,
	delegate any,
) {
	t.Helper()
	control, ok := execution.controlPlane.control.(*testProviderControlPlane)
	if !ok {
		t.Fatal(errors.New("execution does not use the mutable test control plane"))
	}
	control.set(delegate)
}

func reconciliationCoordinatorWithReaderForTest(
	t testing.TB,
	execution *ExecutionCoordinator,
	reader any,
) (*ReconciliationCoordinator, error) {
	t.Helper()
	setProviderControlPlaneForTest(t, execution, reader)
	return execution.ReconciliationCoordinator(nil, nil)
}

func provisionCoordinatorWithReaderForTest(
	t testing.TB,
	execution *ExecutionCoordinator,
	reader any,
) (*ProvisionCoordinator, error) {
	t.Helper()
	setProviderControlPlaneForTest(t, execution, reader)
	return execution.ProvisionCoordinator(nil)
}

func maintenanceCoordinatorWithReaderForTest(
	t testing.TB,
	execution *ExecutionCoordinator,
	reader any,
) (*MaintenanceCoordinator, error) {
	t.Helper()
	setProviderControlPlaneForTest(t, execution, reader)
	return execution.MaintenanceCoordinator(nil)
}

func newExecutionTestRuntime(clients ...backend.Backend) *executionTestRuntime {
	runtime := &executionTestRuntime{backends: make(map[string]backend.Backend, len(clients))}
	for _, client := range clients {
		runtime.backends[client.Name()] = client
	}
	return runtime
}

func (runtime *executionTestRuntime) GetBackendByName(name string) backend.Backend {
	return runtime.backends[name]
}
func (runtime *executionTestRuntime) Backends() []backend.Backend {
	out := make([]backend.Backend, 0, len(runtime.backends))
	for _, client := range runtime.backends {
		out = append(out, client)
	}
	return out
}
func (runtime *executionTestRuntime) Route(string) backend.Backend {
	for _, client := range runtime.backends {
		return client
	}
	return nil
}
func (runtime *executionTestRuntime) RouteForProvision(context.Context, string, map[string]int) backend.Backend {
	return runtime.Route("")
}
func (runtime *executionTestRuntime) RouteForProvisionAmong(
	_ context.Context, _ string, eligible map[string]struct{}, _ map[string]int,
) backend.Backend {
	for name := range eligible {
		if client := runtime.backends[name]; client != nil {
			return client
		}
	}
	return nil
}

func executionRuntime(names ...string) *executionTestRuntime {
	clients := make([]backend.Backend, 0, len(names))
	for _, name := range names {
		clients = append(clients, &executionTestBackend{name: name})
	}
	return newExecutionTestRuntime(clients...)
}

func bindExecutionForTest(
	t testing.TB,
	coordinator *OperationCoordinator,
	runtime backendRuntime,
) *ExecutionCoordinator {
	t.Helper()
	names, err := backendNames(runtime)
	require.NoError(t, err)
	coordinator.store.mu.RLock()
	configured := len(coordinator.store.backendTopology) != 0
	coordinator.store.mu.RUnlock()
	if !configured {
		require.NoError(t, coordinator.store.ConfigureBackendTopologyWithStorageIdentities(
			names, testBackendStorageIDs(names...),
		))
	}
	if coordinator.store.callbackRoutes == nil {
		coordinator.store.callbackRoutes = testCallbackRoutes(t)
	}
	execution, err := coordinator.BindBackendRuntime(runtime, &testProviderControlPlane{
		provider: coordinator.store.providerUUID,
	})
	require.NoError(t, err)
	return execution
}
