package restore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

const (
	testProvider = "44ed3e51-6912-4f6f-8f29-ac2fdd4455d4"
	testTarget   = "target-lease"
	testSource   = "source-lease"
	testTenant   = "tenant-1"
	testBackend  = "backend-a"
)

func TestRestoreAdmissionFailure_BackendOutsideTopologyIsServiceUnavailable(t *testing.T) {
	cause := fmt.Errorf("retained source is unreachable: %w", placement.ErrBackendNotInTopology)
	result := Result{Outcome: OutcomeServiceUnavailable, cause: cause}

	assert.Equal(t, OutcomeServiceUnavailable, result.Outcome)
	assert.ErrorIs(t, result.cause, placement.ErrBackendNotInTopology)
}

func testCallbackRouteFactory(t testing.TB) *placement.CallbackRouteFactory {
	t.Helper()
	factory, err := placement.NewCallbackRouteFactory("https://provider.test")
	require.NoError(t, err)
	return factory
}

type targetReader struct {
	mu     sync.Mutex
	leases map[string]*billingtypes.Lease
	errs   map[string]error
	hook   func(string)
	calls  int
}

type restoreReconciliationChain struct {
	*targetReader
}

func (restoreReconciliationChain) GetPendingLeases(
	context.Context, string,
) ([]billingtypes.Lease, error) {
	return nil, nil
}

func (restoreReconciliationChain) GetActiveLeasesByProvider(
	context.Context, string,
) ([]billingtypes.Lease, error) {
	return nil, nil
}

func (restoreReconciliationChain) RejectLeases(
	context.Context, []string, string,
) (uint64, []string, error) {
	return 0, nil, nil
}

func (restoreReconciliationChain) CloseLeases(
	context.Context, []string, string,
) (uint64, []string, error) {
	return 0, nil, nil
}

func (restoreReconciliationChain) Acknowledge(
	context.Context, string,
) (bool, string, error) {
	return true, "", nil
}

func (reader *targetReader) GetLease(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	reader.mu.Lock()
	reader.calls++
	hook := reader.hook
	err := reader.errs[leaseUUID]
	lease := reader.leases[leaseUUID]
	reader.mu.Unlock()
	if hook != nil {
		hook(leaseUUID)
	}
	if err != nil || lease == nil {
		return nil, err
	}
	copy := *lease
	copy.Items = append([]billingtypes.LeaseItem(nil), lease.Items...)
	return &copy, nil
}

type fakeBackend struct {
	mu                  sync.Mutex
	name                string
	err                 error
	getInfo             *backend.ProvisionInfo
	getErr              error
	get                 func(context.Context, string) (*backend.ProvisionInfo, error)
	restore             func(context.Context, backend.RestoreRequest) error
	request             backend.RestoreRequest
	calls               int
	gets                int
	inventoryProvisions []backend.ProvisionInfo
	inventoryRetentions []backend.RetainedLease
}

func (fake *fakeBackend) Name() string { return fake.name }

func (fake *fakeBackend) GetProvision(
	ctx context.Context,
	leaseUUID string,
) (*backend.ProvisionInfo, error) {
	fake.mu.Lock()
	fake.gets++
	get := fake.get
	getErr := fake.getErr
	getInfo := fake.getInfo
	fake.mu.Unlock()
	if get != nil {
		return get(ctx, leaseUUID)
	}
	if getErr != nil || getInfo == nil {
		return nil, getErr
	}
	info := *getInfo
	return &info, nil
}

func (fake *fakeBackend) Restore(ctx context.Context, request backend.RestoreRequest) error {
	fake.mu.Lock()
	fake.calls++
	fake.request = request
	restore := fake.restore
	err := fake.err
	fake.mu.Unlock()
	if restore != nil {
		return restore(ctx, request)
	}
	return err
}

func (fake *fakeBackend) setRestore(restore func(context.Context, backend.RestoreRequest) error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.restore = restore
}

func (fake *fakeBackend) callCount() int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.calls
}

func (fake *fakeBackend) lastRequest() backend.RestoreRequest {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.request
}

func (*fakeBackend) Provision(context.Context, backend.ProvisionRequest) error   { return nil }
func (*fakeBackend) GetInfo(context.Context, string) (*backend.LeaseInfo, error) { return nil, nil }
func (*fakeBackend) Deprovision(context.Context, string) error                   { return nil }
func (fake *fakeBackend) ListProvisions(context.Context) ([]backend.ProvisionInfo, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]backend.ProvisionInfo(nil), fake.inventoryProvisions...), nil
}
func (fake *fakeBackend) ListProvisionsWithIdentity(
	ctx context.Context,
) ([]backend.ProvisionInfo, backendidentity.ID, error) {
	rows, err := fake.ListProvisions(ctx)
	return rows, restoreTestBackendStorageID(fake.name), err
}
func (*fakeBackend) LookupProvisions(context.Context, []string) ([]backend.ProvisionInfo, error) {
	return nil, nil
}
func (*fakeBackend) Health(context.Context) error                                    { return nil }
func (*fakeBackend) RefreshState(context.Context) error                              { return nil }
func (*fakeBackend) GetLogs(context.Context, string, int) (map[string]string, error) { return nil, nil }
func (*fakeBackend) Restart(context.Context, backend.RestartRequest) error           { return nil }
func (*fakeBackend) Update(context.Context, backend.UpdateRequest) error             { return nil }
func (*fakeBackend) ReconcileCustomDomain(context.Context, string, []backend.LeaseItem) error {
	return nil
}
func (*fakeBackend) GetReleases(context.Context, string) ([]backend.ReleaseInfo, error) {
	return nil, nil
}
func (*fakeBackend) GetLoadStats(context.Context) (*backend.LoadStats, error) {
	return &backend.LoadStats{}, nil
}
func (fake *fakeBackend) ListRetentions(context.Context) ([]backend.RetainedLease, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]backend.RetainedLease(nil), fake.inventoryRetentions...), nil
}
func (fake *fakeBackend) ListRetentionsWithIdentity(
	ctx context.Context,
) ([]backend.RetainedLease, backendidentity.ID, error) {
	rows, err := fake.ListRetentions(ctx)
	return rows, restoreTestBackendStorageID(fake.name), err
}

func (fake *fakeBackend) setInventory(
	provisions []backend.ProvisionInfo,
	retentions []backend.RetainedLease,
) {
	fake.mu.Lock()
	fake.inventoryProvisions = append([]backend.ProvisionInfo(nil), provisions...)
	fake.inventoryRetentions = append([]backend.RetainedLease(nil), retentions...)
	fake.mu.Unlock()
}

type backendLookup map[string]backend.Backend

var restoreReconciliationBackends sync.Map

func (lookup backendLookup) GetBackendByName(name string) backend.Backend {
	return lookup[name]
}
func (lookup backendLookup) Backends() []backend.Backend {
	out := make([]backend.Backend, 0, len(lookup))
	for _, client := range lookup {
		out = append(out, client)
	}
	return out
}
func (lookup backendLookup) Route(string) backend.Backend {
	for _, client := range lookup {
		return client
	}
	return nil
}
func (lookup backendLookup) RouteForProvision(context.Context, string, map[string]int) backend.Backend {
	return lookup.Route("")
}
func (lookup backendLookup) RouteForProvisionAmong(
	_ context.Context, _ string, eligible map[string]struct{}, _ map[string]int,
) backend.Backend {
	for name := range eligible {
		if client := lookup[name]; client != nil {
			return client
		}
	}
	return nil
}

type eventSink struct {
	mu     sync.Mutex
	events []backend.LeaseStatusEvent
	hook   func(backend.LeaseStatusEvent)
}

func (sink *eventSink) Publish(event backend.LeaseStatusEvent) {
	sink.mu.Lock()
	sink.events = append(sink.events, event)
	hook := sink.hook
	sink.mu.Unlock()
	if hook != nil {
		hook(event)
	}
}

func (sink *eventSink) snapshot() []backend.LeaseStatusEvent {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return append([]backend.LeaseStatusEvent(nil), sink.events...)
}

func testCallbackURL(operationID operation.OperationID) (string, error) {
	text, err := operationID.MarshalText()
	if err != nil {
		return "", err
	}
	return "https://fred.example.test/base/callbacks/provision?" +
		operation.QueryParameter + "=" + string(text), nil
}

type fixture struct {
	service        *Service
	targets        *targetReader
	backend        *fakeBackend
	backends       backendLookup
	runtime        operation.RuntimeController
	store          *placement.Store
	coordinator    *placement.OperationCoordinator
	execution      *placement.ExecutionCoordinator
	reconciliation *placement.ReconciliationCoordinator
	restore        *placement.RestoreCoordinator
	events         *eventSink
}

func pendingLease(uuid string) *billingtypes.Lease {
	return &billingtypes.Lease{
		Uuid:         uuid,
		Tenant:       testTenant,
		ProviderUuid: testProvider,
		State:        billingtypes.LEASE_STATE_PENDING,
		Items: []billingtypes.LeaseItem{{
			SkuUuid:      "sku-1",
			Quantity:     2,
			ServiceName:  "web",
			CustomDomain: "tenant.example.test",
		}},
	}
}

func sourceLease(uuid string) *billingtypes.Lease {
	lease := pendingLease(uuid)
	lease.State = billingtypes.LEASE_STATE_CLOSED
	return lease
}

func newFixture(t *testing.T, inventoryReady bool) *fixture {
	t.Helper()
	store, err := placementstore.NewStoreForProvider(
		filepath.Join(t.TempDir(), "placements.db"), testProvider,
		placement.WithCallbackRouteFactory(testCallbackRouteFactory(t)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, placementstore.ConfigureBackendTopologyWithStorageIdentities(store,
		[]string{testBackend, "backend-b"},
		restoreTestBackendStorageIDs(testBackend, "backend-b"),
	))
	backendClient := &fakeBackend{name: testBackend}
	peerBackend := &fakeBackend{name: "backend-b"}
	result := &fixture{
		targets: &targetReader{leases: map[string]*billingtypes.Lease{
			testTarget: pendingLease(testTarget),
			testSource: sourceLease(testSource),
		}},
		backend:  backendClient,
		backends: backendLookup{testBackend: backendClient, "backend-b": peerBackend},
		store:    store,
		events:   &eventSink{},
	}
	result.coordinator, err = store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	result.runtime = result.coordinator.RuntimeController()
	result.execution, err = result.coordinator.BindBackendRuntime(result.backends,
		restoreReconciliationChain{targetReader: result.targets},
	)
	require.NoError(t, err)
	result.reconciliation, err = result.execution.ReconciliationCoordinator(nil, nil)
	require.NoError(t, err)
	restoreReconciliationBackends.Store(result.reconciliation, result.backends)
	t.Cleanup(func() { restoreReconciliationBackends.Delete(result.reconciliation) })
	projectRestoreTestPlacements(t, result.reconciliation, inventoryReady, map[string]string{
		testSource: testBackend,
	})
	if inventoryReady {
		require.True(t, store.CurrentAdmissionBaseline().Valid())
	}
	result.restore, err = result.execution.RestoreCoordinator(
		func(leaseUUID, _ string) {
			result.events.Publish(backend.LeaseStatusEvent{
				LeaseUUID: leaseUUID, Status: backend.ProvisionStatusRestarting,
				Timestamp: time.Unix(123, 0).UTC(),
			})
		},
	)
	require.NoError(t, err)
	result.service, err = NewService(Config{
		Coordinator: result.restore,
		Events:      result.events,
	})
	require.NoError(t, err)
	return result
}

func (fixture *fixture) useCausalRestoreResponse(
	t *testing.T,
	status int,
	body string,
) {
	t.Helper()
	storageID, bound := fixture.store.ExpectedBackendStorageIdentity(testBackend)
	require.True(t, bound)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(backendidentity.ResponseHeader, storageID.String())
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	policy, err := backend.NewConnectionPolicy(backend.ConnectionConfig{
		Name: testBackend, BaseURL: server.URL,
		Secret: "restore-causal-outcome-test-key-at-least-32-bytes",
	})
	require.NoError(t, err)
	client, err := backend.NewIdentityBoundHTTPClient(policy, backend.HTTPClientOptions{}, fixture.store)
	require.NoError(t, err)
	fixture.backends[testBackend] = client
}

func projectRestoreTestPlacements(
	t *testing.T,
	reconciliation *placement.ReconciliationCoordinator,
	complete bool,
	placements map[string]string,
) *placement.ProjectedReconciliationSweep {
	t.Helper()
	return projectRestoreTestPlacementsInSweep(t, reconciliation, complete,
		placement.ReconciliationProjection{Placements: placements})
}

func projectRestoreTestPlacementsInSweep(
	t *testing.T,
	reconciliation *placement.ReconciliationCoordinator,
	complete bool,
	projection placement.ReconciliationProjection,
) *placement.ProjectedReconciliationSweep {
	t.Helper()
	backendNames := []string{testBackend, "backend-b"}
	sweep, err := reconciliation.BeginSweep()
	require.NoError(t, err)
	t.Cleanup(sweep.End)
	reported := make(map[string][]backend.ProvisionInfo, len(backendNames))
	for leaseUUID, backendName := range projection.Placements {
		reported[backendName] = append(reported[backendName], backend.ProvisionInfo{
			LeaseUUID: leaseUUID, BackendName: backendName,
		})
	}
	observed := backendNames
	if !complete {
		observed = observed[:0]
		seen := make(map[string]struct{})
		for _, backendName := range projection.Placements {
			if _, exists := seen[backendName]; !exists {
				seen[backendName] = struct{}{}
				observed = append(observed, backendName)
			}
		}
	}
	for _, backendName := range observed {
		value, ok := restoreReconciliationBackends.Load(reconciliation)
		require.True(t, ok)
		client, ok := value.(backendLookup)[backendName].(*fakeBackend)
		require.True(t, ok)
		client.setInventory(reported[backendName], nil)
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
	result, err := sweep.Project(placement.ReconciliationProjection{
		Placements:         projection.Placements,
		Conflicts:          projection.Conflicts,
		UntrustedPositives: projection.UntrustedPositives,
	})
	require.NoError(t, err)
	return result
}

func restoreTestBackendStorageID(name string) backendidentity.ID {
	digest := sha256.Sum256([]byte("fred-restore-test-storage:" + name))
	digest[6] = (digest[6] & 0x0f) | 0x40
	digest[8] = (digest[8] & 0x3f) | 0x80
	id, err := backendidentity.Parse(fmt.Sprintf("%x-%x-%x-%x-%x",
		digest[0:4], digest[4:6], digest[6:8], digest[8:10], digest[10:16]))
	if err != nil {
		panic(err)
	}
	return id
}

func restoreTestBackendStorageIDs(names ...string) map[string]backendidentity.ID {
	identities := make(map[string]backendidentity.ID, len(names))
	for _, name := range names {
		identities[name] = restoreTestBackendStorageID(name)
	}
	return identities
}

func validCommand() Command {
	return Command{
		TargetLeaseUUID: testTarget,
		Tenant:          testTenant,
		SourceLeaseUUID: testSource,
	}
}

func testOperationID(t *testing.T, value uint64) operation.OperationID {
	t.Helper()
	id, err := operation.ParseID(fmt.Sprintf("00000000-0000-4000-8000-%012x", value))
	require.NoError(t, err)
	return id
}

func testCallbackPair(t *testing.T, id operation.OperationID) placement.CallbackPair {
	t.Helper()
	factory, err := placement.NewCallbackRouteFactory("https://provider.test")
	require.NoError(t, err)
	pair, err := factory.ForOperation(id)
	require.NoError(t, err)
	return pair
}

func requireSourceReusable(t *testing.T, fixture *fixture, target string) {
	t.Helper()
	fixture.targets.mu.Lock()
	fixture.targets.leases[target] = pendingLease(target)
	fixture.targets.mu.Unlock()
	fixture.backend.mu.Lock()
	previousErr := fixture.backend.err
	previousRestore := fixture.backend.restore
	fixture.backend.err = backend.ErrCircuitOpen
	fixture.backend.restore = nil
	fixture.backend.mu.Unlock()
	result := fixture.service.Execute(t.Context(), Command{
		TargetLeaseUUID: target, Tenant: testTenant, SourceLeaseUUID: testSource,
	})
	fixture.backend.mu.Lock()
	fixture.backend.err = previousErr
	fixture.backend.restore = previousRestore
	fixture.backend.mu.Unlock()
	require.Equal(t, OutcomeBackendUnavailable, result.Outcome, result.Cause())
}

func requireLeaseClaimsReleased(t *testing.T, runtime operation.RuntimeController, leaseUUIDs ...string) {
	t.Helper()
	pending := runtime.PendingLeaseUUIDs()
	for _, leaseUUID := range leaseUUIDs {
		assert.NotContains(t, pending, leaseUUID, "lease claim %q leaked", leaseUUID)
	}
}

func TestNewServiceNormalizesNilCapabilities(t *testing.T) {
	t.Parallel()
	store, err := placementstore.NewStoreForProvider(
		filepath.Join(t.TempDir(), "placements.db"), testProvider,
		placement.WithCallbackRouteFactory(testCallbackRouteFactory(t)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	coordinator, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	targets := &targetReader{}
	require.NoError(t, placementstore.ConfigureBackendTopologyWithStorageIdentities(
		store, []string{testBackend}, restoreTestBackendStorageIDs(testBackend),
	))
	execution, err := coordinator.BindBackendRuntime(backendLookup{
		testBackend: &fakeBackend{name: testBackend},
	},
		restoreReconciliationChain{targetReader: targets},
	)
	require.NoError(t, err)
	restoreCoordinator, err := execution.RestoreCoordinator(nil)
	require.NoError(t, err)
	valid := Config{
		Coordinator: restoreCoordinator,
	}

	var (
		typedNilCoordinator *placement.RestoreCoordinator
		typedNilEvents      *eventSink
	)
	tests := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{name: "coordinator", edit: func(c *Config) { c.Coordinator = nil }, want: "chain/runtime-bound coordinator"},
		{name: "typed nil coordinator", edit: func(c *Config) { c.Coordinator = typedNilCoordinator }, want: "chain/runtime-bound coordinator"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := valid
			test.edit(&config)
			service, err := NewService(config)
			assert.Nil(t, service)
			assert.ErrorContains(t, err, test.want)
		})
	}

	valid.Events = typedNilEvents
	service, err := NewService(valid)
	require.NoError(t, err)
	assert.Nil(t, service.events, "typed-nil optional events must normalize to nil")
}

func TestRestoreCoordinatorRejectsProviderAndChainSplices(t *testing.T) {
	store, err := placementstore.NewStoreForProvider(
		filepath.Join(t.TempDir(), "placements.db"), testProvider,
		placement.WithCallbackRouteFactory(testCallbackRouteFactory(t)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	var typedNilControl *restoreReconciliationChain

	for name, control := range map[string]placement.ProviderControlPlane{
		"nil control plane":       nil,
		"typed-nil control plane": typedNilControl,
	} {
		t.Run(name, func(t *testing.T) {
			execution, bindErr := base.BindBackendRuntime(backendLookup{}, control)
			assert.Error(t, bindErr)
			assert.Nil(t, execution)
			assert.Nil(t, base.RuntimeController().PendingLeaseUUIDs())
		})
	}
}

func TestServiceRereadsAndValidatesTargetUnderBothClaims(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*fixture)
		command func() Command
		outcome Outcome
	}{
		{name: "incomplete", command: func() Command { return Command{} }, outcome: OutcomeInvalid},
		{name: "same source and target", command: func() Command {
			command := validCommand()
			command.SourceLeaseUUID = command.TargetLeaseUUID
			return command
		}, outcome: OutcomeInvalid},
		{name: "read error", prepare: func(f *fixture) {
			f.targets.errs = map[string]error{testTarget: errors.New("chain unavailable")}
		}, command: validCommand, outcome: OutcomeServiceUnavailable},
		{name: "missing", prepare: func(f *fixture) { delete(f.targets.leases, testTarget) }, command: validCommand, outcome: OutcomeServiceUnavailable},
		{name: "wrong UUID", prepare: func(f *fixture) { f.targets.leases[testTarget].Uuid = "other" }, command: validCommand, outcome: OutcomeServiceUnavailable},
		{name: "tenant changed", prepare: func(f *fixture) { f.targets.leases[testTarget].Tenant = "other" }, command: validCommand, outcome: OutcomeInvalid},
		{name: "provider changed", prepare: func(f *fixture) { f.targets.leases[testTarget].ProviderUuid = "other" }, command: validCommand, outcome: OutcomeInvalid},
		{name: "pending became terminal", prepare: func(f *fixture) { f.targets.leases[testTarget].State = billingtypes.LEASE_STATE_CLOSED }, command: validCommand, outcome: OutcomeTargetNotPending},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, true)
			if test.prepare != nil {
				test.prepare(fixture)
			}
			result := fixture.service.Execute(t.Context(), test.command())
			assert.Equal(t, test.outcome, result.Outcome)
			assert.Zero(t, fixture.backend.callCount())
			assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
			assert.False(t, fixture.runtime.Contains(testTarget))
			requireLeaseClaimsReleased(t, fixture.runtime, testSource, testTarget)
		})
	}

	t.Run("target read executes under source and target claims", func(t *testing.T) {
		fixture := newFixture(t, true)
		fixture.targets.hook = func(leaseUUID string) {
			if leaseUUID != testTarget {
				return
			}
			assert.ElementsMatch(t, []string{testSource, testTarget},
				fixture.runtime.PendingLeaseUUIDs(),
				"the target read must run under both restore lease claims")
		}
		result := fixture.service.Execute(t.Context(), validCommand())
		require.Equal(t, OutcomeAccepted, result.Outcome)
	})
}

func TestServiceAuthorizesSourceBeforeTakingLeaseClaims(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*fixture)
		outcome Outcome
	}{
		{name: "source read error", prepare: func(f *fixture) {
			f.targets.errs = map[string]error{testSource: errors.New("chain unavailable")}
		}, outcome: OutcomeSourceUnavailable},
		{name: "source read returns no result", prepare: func(f *fixture) {
			delete(f.targets.leases, testSource)
		}, outcome: OutcomeSourceUnavailable},
		{name: "source UUID mismatch", prepare: func(f *fixture) {
			f.targets.leases[testSource].Uuid = "other"
		}, outcome: OutcomeSourceUnavailable},
		{name: "source belongs to another tenant", prepare: func(f *fixture) {
			f.targets.leases[testSource].Tenant = "tenant-2"
		}, outcome: OutcomeSourceNotFound},
		{name: "source belongs to another provider", prepare: func(f *fixture) {
			f.targets.leases[testSource].ProviderUuid = "provider-2"
		}, outcome: OutcomeSourceNotFound},
		{name: "source is not positively closed", prepare: func(f *fixture) {
			f.targets.leases[testSource].State = billingtypes.LEASE_STATE_ACTIVE
		}, outcome: OutcomeNotRetained},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, true)
			test.prepare(fixture)
			fixture.targets.hook = func(leaseUUID string) {
				if leaseUUID != testSource {
					return
				}
				requireLeaseClaimsReleased(t, fixture.runtime, testSource, testTarget)
			}

			result := fixture.service.Execute(t.Context(), validCommand())

			assert.Equal(t, test.outcome, result.Outcome)
			assert.Zero(t, fixture.backend.callCount())
			assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
			assert.False(t, fixture.runtime.Contains(testTarget))
			requireLeaseClaimsReleased(t, fixture.runtime, testSource, testTarget)
		})
	}
}

func retainedSourceInfo(tenant string) *backend.ProvisionInfo {
	return &backend.ProvisionInfo{
		LeaseUUID:    testSource,
		Tenant:       tenant,
		ProviderUUID: testProvider,
		Status:       backend.ProvisionStatusRetained,
	}
}

func TestServiceTypedAbsentSourceNeverAuthorizesBackendRestore(t *testing.T) {
	fixture := newFixture(t, true)
	fixture.targets.errs = map[string]error{testSource: billingtypes.ErrLeaseNotFound}
	fixture.backend.getInfo = retainedSourceInfo(testTenant)
	require.Equal(t, placement.LifecycleVerdictUnusable,
		fixture.store.CurrentLifecycle(testSource).Verdict(),
		"retention-only inventory must not establish runtime lifecycle authority")

	result := fixture.service.Execute(t.Context(), validCommand())

	require.Equal(t, OutcomeSourceUnavailable, result.Outcome)
	assert.ErrorIs(t, result.Cause(), billingtypes.ErrLeaseNotFound)
	assert.Zero(t, fixture.backend.callCount())
	assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
	assert.Equal(t, placement.LifecycleVerdictUnusable,
		fixture.store.CurrentLifecycle(testSource).Verdict(),
		"chain absence must not repair or retire lifecycle authority")
}

func TestServiceNoResultSourceIsUnavailableBeforeClaims(t *testing.T) {
	fixture := newFixture(t, true)
	delete(fixture.targets.leases, testSource)
	fixture.backend.getInfo = retainedSourceInfo("tenant-other")

	result := fixture.service.Execute(t.Context(), validCommand())

	assert.Equal(t, OutcomeSourceUnavailable, result.Outcome)
	assert.Zero(t, fixture.backend.callCount())
	assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
	requireLeaseClaimsReleased(t, fixture.runtime, testSource, testTarget)
}

func TestServiceUnknownSourceNeverFallsBackToBackendInspection(t *testing.T) {
	tests := []struct {
		name    string
		getInfo *backend.ProvisionInfo
		getErr  error
	}{
		{name: "silent miss"},
		{name: "explicit miss", getErr: backend.ErrNotProvisioned},
		{name: "backend error", getErr: errors.New("backend unavailable")},
		{name: "live record is not retained", getInfo: &backend.ProvisionInfo{
			LeaseUUID: testSource, Tenant: testTenant, ProviderUUID: testProvider,
			Status: backend.ProvisionStatusReady,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, true)
			delete(fixture.targets.leases, testSource)
			fixture.backend.getInfo = test.getInfo
			fixture.backend.getErr = test.getErr

			result := fixture.service.Execute(t.Context(), validCommand())

			assert.Equal(t, OutcomeSourceUnavailable, result.Outcome)
			assert.Zero(t, fixture.backend.callCount())
			assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
			requireLeaseClaimsReleased(t, fixture.runtime, testSource, testTarget)
		})
	}
}

func TestServiceAcceptedBindsOneIdentityAndAuthority(t *testing.T) {
	fixture := newFixture(t, true)
	fixture.events.hook = func(event backend.LeaseStatusEvent) {
		if event.Status != backend.ProvisionStatusRestarting || event.LeaseUUID != testTarget {
			return
		}
		record, exists := fixture.coordinator.Lookup(testTarget)
		require.True(t, exists)
		assert.Equal(t, operation.PhaseCalling, record.Phase(),
			"the call barrier must precede the Restarting event")
	}
	fixture.backend.setRestore(func(_ context.Context, request backend.RestoreRequest) error {
		if request.LeaseUUID != testTarget {
			return backend.ErrCircuitOpen
		}
		record, exists := fixture.coordinator.Lookup(testTarget)
		require.True(t, exists)
		assert.Equal(t, operation.PhaseCalling, record.Phase())
		assert.Equal(t, testBackend, record.Backend())
		return nil
	})

	result := fixture.service.Execute(t.Context(), validCommand())

	require.Equal(t, OutcomeAccepted, result.Outcome)
	assert.Equal(t, testBackend, result.BackendName)
	assert.Equal(t, 1, fixture.backend.callCount())
	request := fixture.backend.lastRequest()
	assert.Equal(t, testTarget, request.LeaseUUID)
	assert.Equal(t, testSource, request.FromLeaseUUID)
	assert.Equal(t, testTenant, request.Tenant)
	assert.Equal(t, testProvider, request.ProviderUUID)
	require.Equal(t, []backend.LeaseItem{{
		SKU: "sku-1", Quantity: 2, ServiceName: "web", CustomDomain: "tenant.example.test",
	}}, request.Items)
	callbackURL, err := url.Parse(request.CallbackURL)
	require.NoError(t, err)
	callbackID, present, err := operation.ParseQuery(callbackURL.Query())
	require.NoError(t, err)
	require.True(t, present)
	wantLifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(request.CallbackURL, "")
	require.NoError(t, err)
	assert.Equal(t, wantLifecycleCallbackURL, request.LifecycleCallbackURL)

	target := fixture.store.Lookup(testTarget)
	assert.Equal(t, placement.StateConfirmed, target.State())
	assert.Equal(t, testBackend, target.Backend)
	record, exists := fixture.coordinator.Lookup(testTarget)
	require.True(t, exists)
	assert.Equal(t, callbackID, record.ID())
	assert.Equal(t, operation.KindRestore, record.Kind())
	assert.Equal(t, operation.PhaseActive, record.Phase())
	events := fixture.events.snapshot()
	require.Len(t, events, 1)
	assert.Equal(t, backend.ProvisionStatusRestarting, events[0].Status)
	assert.Equal(t, time.Unix(123, 0).UTC(), events[0].Timestamp)
	requireSourceReusable(t, fixture, "probe-target")
}

func TestServiceTwoConcurrentTargetsOneSourceDispatchesOnce(t *testing.T) {
	fixture := newFixture(t, true)
	secondTarget := "target-lease-2"
	fixture.targets.leases[secondTarget] = pendingLease(secondTarget)
	entered := make(chan struct{})
	release := make(chan struct{})
	fixture.backend.setRestore(func(context.Context, backend.RestoreRequest) error {
		close(entered)
		<-release
		return nil
	})

	firstResult := make(chan Result, 1)
	go func() { firstResult <- fixture.service.Execute(t.Context(), validCommand()) }()
	<-entered
	secondCommand := validCommand()
	secondCommand.TargetLeaseUUID = secondTarget
	second := fixture.service.Execute(t.Context(), secondCommand)
	assert.Equal(t, OutcomeAlreadyInProgress, second.Outcome)
	assert.Equal(t, 1, fixture.backend.callCount())
	assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(secondTarget).State())
	close(release)
	assert.Equal(t, OutcomeAccepted, (<-firstResult).Outcome)
}

func TestServiceClaimsAndProjectionFenceSynchronousRestore(t *testing.T) {
	fixture := newFixture(t, true)
	entered := make(chan struct{})
	release := make(chan struct{})
	fixture.backend.setRestore(func(context.Context, backend.RestoreRequest) error {
		close(entered)
		<-release
		return nil
	})
	resultCh := make(chan Result, 1)
	go func() { resultCh <- fixture.service.Execute(t.Context(), validCommand()) }()
	<-entered

	assert.ElementsMatch(t, []string{testSource, testTarget},
		fixture.runtime.PendingLeaseUUIDs(),
		"the synchronous restore must retain both lease claims through dispatch")
	_, exists := fixture.coordinator.Lookup(testTarget)
	require.True(t, exists)
	projectRestoreTestPlacementsInSweep(t, fixture.reconciliation, false, placement.ReconciliationProjection{
		Placements: map[string]string{testSource: "backend-b"},
	})
	close(release)
	assert.Equal(t, OutcomeAccepted, (<-resultCh).Outcome)
	assert.Equal(t, testBackend, fixture.store.Lookup(testSource).Backend)
	requireLeaseClaimsReleased(t, fixture.runtime, testSource)
}

func TestServiceTargetPlacementAdmissionFailsBeforeDispatch(t *testing.T) {
	fixture := newFixture(t, true)
	projectRestoreTestPlacements(t, fixture.reconciliation, false, map[string]string{
		testTarget: testBackend,
	})
	before := fixture.store.Lookup(testTarget)

	result := fixture.service.Execute(t.Context(), validCommand())

	assert.Equal(t, OutcomeAlreadyInProgress, result.Outcome)
	assert.ErrorIs(t, result.Cause(), placement.ErrRestoreTargetUnavailable)
	assert.Zero(t, fixture.backend.callCount())
	assert.Equal(t, before, fixture.store.Lookup(testTarget))
	assert.False(t, fixture.runtime.Contains(testTarget))
	requireLeaseClaimsReleased(t, fixture.runtime, testSource, testTarget)
}

func TestServiceRequiresCurrentAdmissionBaselineBeforeDispatch(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*fixture)
	}{
		{name: "complete inventory not yet committed"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, false)
			if test.prepare != nil {
				test.prepare(fixture)
			}

			result := fixture.service.Execute(t.Context(), validCommand())

			assert.Equal(t, OutcomeServiceUnavailable, result.Outcome)
			assert.ErrorIs(t, result.Cause(), placement.ErrInvalidAdmissionBaseline)
			assert.Zero(t, fixture.backend.callCount())
			assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
			assert.False(t, fixture.runtime.Contains(testTarget))
			requireLeaseClaimsReleased(t, fixture.runtime, testSource, testTarget)
		})
	}
}

func TestServicePreDispatchFailuresRefuseAtomicRestore(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*fixture)
	}{
		{name: "target read", prepare: func(f *fixture) {
			f.targets.errs = map[string]error{testTarget: errors.New("chain unavailable")}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, true)
			test.prepare(fixture)
			result := fixture.service.Execute(t.Context(), validCommand())
			assert.Equal(t, OutcomeServiceUnavailable, result.Outcome)
			assert.Zero(t, fixture.backend.callCount())
			assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
			assert.False(t, fixture.runtime.Contains(testTarget))
			requireLeaseClaimsReleased(t, fixture.runtime, testSource, testTarget)
		})
	}
}

func TestServiceEventSinkPanicDoesNotPreventRestoreDispatch(t *testing.T) {
	fixture := newFixture(t, true)
	fixture.events.hook = func(backend.LeaseStatusEvent) {
		panic("event sink fault")
	}
	panics := metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(
		metrics.LifecycleEventRestoreRestarting,
	)
	before := promtestutil.ToFloat64(panics)

	result := fixture.service.Execute(t.Context(), validCommand())

	require.Equal(t, OutcomeAccepted, result.Outcome)
	assert.Equal(t, 1, fixture.backend.callCount(),
		"best-effort event delivery must not suppress restore dispatch")
	assert.Equal(t, placement.StateConfirmed, fixture.store.Lookup(testTarget).State())
	record, exists := fixture.coordinator.Lookup(testTarget)
	require.True(t, exists)
	assert.Equal(t, operation.PhaseActive, record.Phase(),
		"the recovered panic must not strand the restore operation in Calling")
	assert.Equal(t, before+1, promtestutil.ToFloat64(panics))
	requireSourceReusable(t, fixture, "probe-after-event-panic")
}

func TestServiceRefusalEventSinkPanicDoesNotUndoDefinitiveSettlement(t *testing.T) {
	fixture := newFixture(t, true)
	fixture.useCausalRestoreResponse(
		t, http.StatusBadRequest, `{"error":"invalid request"}`,
	)
	fixture.events.hook = func(event backend.LeaseStatusEvent) {
		if event.Status == backend.ProvisionStatusFailed {
			panic("refusal event sink fault")
		}
	}
	panics := metrics.LifecycleEventSinkPanicsTotal.WithLabelValues(
		metrics.LifecycleEventRestoreRefused,
	)
	before := promtestutil.ToFloat64(panics)

	result := fixture.service.Execute(t.Context(), validCommand())

	require.Equal(t, OutcomeInvalidRequest, result.Outcome)
	assert.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State(),
		"event delivery cannot roll back an exact refusal settlement")
	assert.False(t, fixture.runtime.Contains(testTarget))
	assert.Equal(t, before+1, promtestutil.ToFloat64(panics))
	events := fixture.events.snapshot()
	require.Len(t, events, 2)
	assert.Equal(t, backend.ProvisionStatusRestarting, events[0].Status)
	assert.Equal(t, backend.ProvisionStatusFailed, events[1].Status)
	fixture.backends[testBackend] = fixture.backend
	requireSourceReusable(t, fixture, "probe-after-refusal-event-panic")
}

func TestServiceSynchronousSettlementModes(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		status     int
		body       string
		outcome    Outcome
		wantState  placement.State
		wantEvents []backend.ProvisionStatus
		verdict    string
	}{
		{name: "accepted", outcome: OutcomeAccepted, wantState: placement.StateConfirmed, wantEvents: []backend.ProvisionStatus{backend.ProvisionStatusRestarting}},
		{name: "legacy already-provisioned sentinel is ambiguous", err: backend.ErrAlreadyProvisioned, outcome: OutcomeInternalFailure, wantState: placement.StateAttempting, wantEvents: []backend.ProvisionStatus{backend.ProvisionStatusRestarting}},
		{name: "legacy backend-refused sentinel is ambiguous", err: backend.ErrRestoreRefused, outcome: OutcomeInternalFailure, wantState: placement.StateAttempting, wantEvents: []backend.ProvisionStatus{backend.ProvisionStatusRestarting}},
		{name: "definitive refusal", status: http.StatusBadRequest, body: `{"error":"invalid request"}`, outcome: OutcomeInvalidRequest, wantState: placement.StateAbsent, wantEvents: []backend.ProvisionStatus{backend.ProvisionStatusRestarting, backend.ProvisionStatusFailed}},
		{name: "coded capacity refusal", status: http.StatusServiceUnavailable, body: `{"error":"full","code":"insufficient_resources"}`, outcome: OutcomeInsufficientResources, wantState: placement.StateAbsent, wantEvents: []backend.ProvisionStatus{backend.ProvisionStatusRestarting, backend.ProvisionStatusFailed}, verdict: metrics.CapacityVerdictCodedRefusal},
		{name: "ambiguous capacity response", err: backend.ErrInsufficientResources, outcome: OutcomeInsufficientResources, wantState: placement.StateAttempting, wantEvents: []backend.ProvisionStatus{backend.ProvisionStatusRestarting}, verdict: metrics.CapacityVerdictAmbiguous},
		{name: "legacy validation sentinel", err: errors.Join(backend.ErrValidation, context.DeadlineExceeded), outcome: OutcomeInternalFailure, wantState: placement.StateAttempting, wantEvents: []backend.ProvisionStatus{backend.ProvisionStatusRestarting}},
		{name: "ambiguous error", err: context.DeadlineExceeded, outcome: OutcomeInternalFailure, wantState: placement.StateAttempting, wantEvents: []backend.ProvisionStatus{backend.ProvisionStatusRestarting}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, true)
			if test.status != 0 {
				fixture.useCausalRestoreResponse(t, test.status, test.body)
			} else {
				fixture.backend.err = test.err
			}
			var capacityCounter prometheus.Counter
			var capacityBefore float64
			if test.verdict != "" {
				capacityCounter = metrics.BackendInsufficientResourcesTotal.WithLabelValues(
					testBackend, test.verdict,
				)
				capacityBefore = promtestutil.ToFloat64(capacityCounter)
			}
			result := fixture.service.Execute(t.Context(), validCommand())
			assert.Equal(t, test.outcome, result.Outcome)
			assert.Equal(t, test.wantState, fixture.store.Lookup(testTarget).State())
			assert.Equal(t, test.outcome == OutcomeAccepted,
				fixture.runtime.Contains(testTarget))
			events := fixture.events.snapshot()
			require.Len(t, events, len(test.wantEvents))
			for index, status := range test.wantEvents {
				assert.Equal(t, status, events[index].Status)
				if index == 0 {
					assert.Equal(t, time.Unix(123, 0).UTC(), events[index].Timestamp)
				} else {
					assert.WithinDuration(t, time.Now(), events[index].Timestamp, time.Minute)
				}
			}
			if len(events) == 2 {
				assert.Equal(t, "restore did not start", events[1].Error)
			}
			if capacityCounter != nil {
				assert.Equal(t, capacityBefore+1, promtestutil.ToFloat64(capacityCounter))
			}
		})
	}
}

func TestServiceAmbiguousAlreadyProvisionedAwaitsExactObservedGeneration(t *testing.T) {
	fixture := newFixture(t, true)
	fixture.backend.err = backend.ErrAlreadyProvisioned

	result := fixture.service.Execute(t.Context(), validCommand())

	require.Equal(t, OutcomeInternalFailure, result.Outcome)
	pending := fixture.store.Lookup(testTarget)
	require.Equal(t, placement.StateAttempting, pending.State())
	operationID := pending.AttemptOperationID()
	require.True(t, operationID.Valid())
	attemptedID, err := lifecycle.FromOperationID(operationID)
	require.NoError(t, err)
	olderID, err := lifecycle.ParseID("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	require.NotEqual(t, attemptedID, olderID)

}

func TestServiceBackendPanicReleasesClaimsAndRetainsAttempt(t *testing.T) {
	fixture := newFixture(t, true)
	fixture.backend.setRestore(func(context.Context, backend.RestoreRequest) error {
		panic("restore exploded")
	})

	result := fixture.service.Execute(t.Context(), validCommand())

	assert.Equal(t, OutcomeInternalFailure, result.Outcome)
	assert.ErrorContains(t, result.Cause(), "panicked")
	assert.Equal(t, placement.StateAttempting, fixture.store.Lookup(testTarget).State())
	assert.False(t, fixture.runtime.Contains(testTarget))
	requireLeaseClaimsReleased(t, fixture.runtime, testSource, testTarget)
	requireSourceReusable(t, fixture, "probe-after-panic")
}
