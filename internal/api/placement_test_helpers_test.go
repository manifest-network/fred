package api

import (
	"context"
	"crypto/sha256"
	"fmt"
	"slices"
	"sync"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

type apiInventoryBackend struct {
	backend.Backend
	mu         sync.Mutex
	storageID  backendidentity.ID
	provisions []backend.ProvisionInfo
	retentions []backend.RetainedLease
}

func (client *apiInventoryBackend) stage(
	storageID backendidentity.ID,
	provisions []backend.ProvisionInfo,
	retentions []backend.RetainedLease,
) {
	client.mu.Lock()
	client.storageID = storageID
	client.provisions = append([]backend.ProvisionInfo(nil), provisions...)
	client.retentions = slices.Clone(retentions)
	client.mu.Unlock()
}

func (client *apiInventoryBackend) ListProvisionsWithIdentity(
	context.Context,
) ([]backend.ProvisionInfo, backendidentity.ID, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	return append([]backend.ProvisionInfo(nil), client.provisions...), client.storageID, nil
}

func (client *apiInventoryBackend) ListRetentionsWithIdentity(
	context.Context,
) ([]backend.RetainedLease, backendidentity.ID, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	return slices.Clone(client.retentions), client.storageID, nil
}

type apiBackendRuntime interface {
	Route(string) backend.Backend
	RouteForProvision(context.Context, string, map[string]int) backend.Backend
	RouteForProvisionAmong(context.Context, string, map[string]struct{}, map[string]int) backend.Backend
	GetBackendByName(string) backend.Backend
	Backends() []backend.Backend
}

type apiInventoryRuntime struct {
	runtime  apiBackendRuntime
	backends map[string]*apiInventoryBackend
}

func newAPIInventoryRuntime(runtime apiBackendRuntime) *apiInventoryRuntime {
	result := &apiInventoryRuntime{
		runtime: runtime, backends: make(map[string]*apiInventoryBackend),
	}
	for _, client := range runtime.Backends() {
		if client != nil {
			result.backends[client.Name()] = &apiInventoryBackend{Backend: client}
		}
	}
	return result
}

func (runtime *apiInventoryRuntime) wrap(client backend.Backend) backend.Backend {
	if client == nil {
		return nil
	}
	return runtime.backends[client.Name()]
}

func (runtime *apiInventoryRuntime) Route(sku string) backend.Backend {
	return runtime.wrap(runtime.runtime.Route(sku))
}

func (runtime *apiInventoryRuntime) RouteForProvision(
	ctx context.Context, sku string, inFlight map[string]int,
) backend.Backend {
	return runtime.wrap(runtime.runtime.RouteForProvision(ctx, sku, inFlight))
}

func (runtime *apiInventoryRuntime) RouteForProvisionAmong(
	ctx context.Context, sku string, eligible map[string]struct{}, inFlight map[string]int,
) backend.Backend {
	return runtime.wrap(runtime.runtime.RouteForProvisionAmong(ctx, sku, eligible, inFlight))
}

func (runtime *apiInventoryRuntime) GetBackendByName(name string) backend.Backend {
	return runtime.backends[name]
}

func (runtime *apiInventoryRuntime) Backends() []backend.Backend {
	clients := runtime.runtime.Backends()
	result := make([]backend.Backend, 0, len(clients))
	for _, client := range clients {
		if wrapped := runtime.wrap(client); wrapped != nil {
			result = append(result, wrapped)
		}
	}
	return result
}

var apiReconciliationInventories sync.Map

func bindAPIInventoryRuntime(
	t *testing.T,
	coordinator *placement.OperationCoordinator,
	runtime apiBackendRuntime,
	chain placement.ReconciliationChain,
) (*placement.ExecutionCoordinator, *apiInventoryRuntime) {
	t.Helper()
	inventoryRuntime := newAPIInventoryRuntime(runtime)
	execution, err := coordinator.BindBackendRuntime(inventoryRuntime,
		apiProviderControlPlane{ReconciliationChain: chain})
	require.NoError(t, err)
	return execution, inventoryRuntime
}

func registerAPIReconciliationInventory(
	t *testing.T,
	reconciliation *placement.ReconciliationCoordinator,
	runtime *apiInventoryRuntime,
) {
	t.Helper()
	apiReconciliationInventories.Store(reconciliation, runtime)
	t.Cleanup(func() { apiReconciliationInventories.Delete(reconciliation) })
}

type apiReconciliationChain struct {
	placement.PruneLeaseReader
}

type apiProviderControlPlane struct {
	placement.ReconciliationChain
}

func (apiProviderControlPlane) Acknowledge(
	context.Context, string,
) (bool, string, error) {
	return true, "", nil
}

func (apiReconciliationChain) GetPendingLeases(
	context.Context, string,
) ([]billingtypes.Lease, error) {
	return nil, nil
}

func (apiReconciliationChain) GetActiveLeasesByProvider(
	context.Context, string,
) ([]billingtypes.Lease, error) {
	return nil, nil
}

func (apiReconciliationChain) RejectLeases(
	context.Context, []string, string,
) (uint64, []string, error) {
	return 0, nil, nil
}

func (apiReconciliationChain) CloseLeases(
	context.Context, []string, string,
) (uint64, []string, error) {
	return 0, nil, nil
}

func testAPICallbackPair(t *testing.T, id operation.OperationID) placement.CallbackPair {
	t.Helper()
	pair, err := makeAPICallbackPair(id)
	require.NoError(t, err)
	return pair
}

func makeAPICallbackPair(id operation.OperationID) (placement.CallbackPair, error) {
	factory, err := placement.NewCallbackRouteFactory("https://provider.test")
	if err != nil {
		return placement.CallbackPair{}, err
	}
	return factory.ForOperation(id)
}

func testAPICallbackPairFromID(id operation.OperationID) placement.CallbackPair {
	pair, err := makeAPICallbackPair(id)
	if err != nil {
		panic(err)
	}
	return pair
}

func testAPIBackendStorageID(name string) backendidentity.ID {
	digest := sha256.Sum256([]byte("fred-api-test-storage:" + name))
	digest[6] = (digest[6] & 0x0f) | 0x40
	digest[8] = (digest[8] & 0x3f) | 0x80
	id, err := backendidentity.Parse(fmt.Sprintf("%x-%x-%x-%x-%x",
		digest[0:4], digest[4:6], digest[6:8], digest[8:10], digest[10:16]))
	if err != nil {
		panic(err)
	}
	return id
}

func testAPIBackendStorageIDs(names ...string) map[string]backendidentity.ID {
	identities := make(map[string]backendidentity.ID, len(names))
	for _, name := range names {
		identities[name] = testAPIBackendStorageID(name)
	}
	return identities
}

func testAPIEmptyBackends(
	names []string,
	placements map[string]string,
) []string {
	nonempty := make(map[string]struct{}, len(placements))
	for _, backendName := range placements {
		nonempty[backendName] = struct{}{}
	}
	empty := make([]string, 0, len(names))
	for _, backendName := range names {
		if _, present := nonempty[backendName]; !present {
			empty = append(empty, backendName)
		}
	}
	return empty
}

func configureAPIPlacementTopology(
	t *testing.T,
	store *placement.Store,
	names []string,
) {
	t.Helper()
	require.NoError(t, placementstore.ConfigureBackendTopologyWithStorageIdentities(store,
		names, testAPIBackendStorageIDs(names...),
	))
}

func projectAPIPlacementInventory(
	t *testing.T,
	reconciliation *placement.ReconciliationCoordinator,
	backendNames []string,
	storageIDs map[string]backendidentity.ID,
	projection placement.ReconciliationProjection,
) *placement.ProjectedReconciliationSweep {
	return projectAPIInventory(t, reconciliation, backendNames, storageIDs, projection, false)
}

// projectAPIRetentionInventory models a placement learned exclusively from
// retained-data inventory. Restore fixtures must not manufacture a live
// runtime principal merely to prove where a source lease's data is stored.
func projectAPIRetentionInventory(
	t *testing.T,
	reconciliation *placement.ReconciliationCoordinator,
	backendNames []string,
	storageIDs map[string]backendidentity.ID,
	projection placement.ReconciliationProjection,
) *placement.ProjectedReconciliationSweep {
	return projectAPIInventory(t, reconciliation, backendNames, storageIDs, projection, true)
}

func projectAPIInventory(
	t *testing.T,
	reconciliation *placement.ReconciliationCoordinator,
	backendNames []string,
	storageIDs map[string]backendidentity.ID,
	projection placement.ReconciliationProjection,
	retentionOnly bool,
) *placement.ProjectedReconciliationSweep {
	t.Helper()
	result, sweep := projectAPIInventoryWithExplicitLifetime(
		t, reconciliation, backendNames, storageIDs, projection, retentionOnly,
	)
	t.Cleanup(sweep.End)
	return result
}

// projectAPIInventoryWithExplicitLifetime lets restart fixtures end the
// inventory session before closing and reopening the durable placement store.
// Ordinary fixtures should use projectAPIInventory so cleanup cannot be
// forgotten.
func projectAPIInventoryWithExplicitLifetime(
	t *testing.T,
	reconciliation *placement.ReconciliationCoordinator,
	backendNames []string,
	storageIDs map[string]backendidentity.ID,
	projection placement.ReconciliationProjection,
	retentionOnly bool,
) (*placement.ProjectedReconciliationSweep, *placement.ReconciliationSweep) {
	t.Helper()
	require.NotNil(t, reconciliation)
	sweep, err := reconciliation.BeginSweep()
	require.NoError(t, err)
	reported := make(map[string][]string, len(backendNames))
	for leaseUUID, backendName := range projection.Placements {
		reported[backendName] = append(reported[backendName], leaseUUID)
	}
	for leaseUUID, candidates := range projection.Conflicts {
		for _, backendName := range candidates {
			reported[backendName] = append(reported[backendName], leaseUUID)
		}
	}
	untrustedByBackend := make(map[string][]string)
	for leaseUUID, candidates := range projection.UntrustedPositives {
		for _, backendName := range candidates {
			untrustedByBackend[backendName] = append(untrustedByBackend[backendName], leaseUUID)
		}
	}
	value, ok := apiReconciliationInventories.Load(reconciliation)
	require.True(t, ok)
	inventoryRuntime := value.(*apiInventoryRuntime)
	for _, backendName := range backendNames {
		provisions := []backend.ProvisionInfo{}
		retentions := []backend.RetainedLease{}
		if retentionOnly {
			for _, leaseUUID := range reported[backendName] {
				retentions = append(retentions, backend.RetainedLease{LeaseUUID: leaseUUID})
			}
		} else {
			provisions = make([]backend.ProvisionInfo, 0, len(reported[backendName]))
			for _, leaseUUID := range reported[backendName] {
				provisions = append(provisions, backend.ProvisionInfo{
					LeaseUUID: leaseUUID, BackendName: backendName,
				})
			}
		}
		storageID := storageIDs[backendName]
		wantDisposition := placement.BackendInventoryAuthoritative
		if leaseUUIDs := untrustedByBackend[backendName]; len(leaseUUIDs) != 0 {
			for _, leaseUUID := range leaseUUIDs {
				provisions = append(provisions, backend.ProvisionInfo{
					LeaseUUID: leaseUUID, BackendName: backendName,
				})
			}
			storageID = backendidentity.ID{}
			wantDisposition = placement.BackendInventoryUntrusted
		}
		client := inventoryRuntime.backends[backendName]
		require.NotNil(t, client)
		client.stage(storageID, provisions, retentions)
		provisionReceipt, collectErr := sweep.CollectProvisionInventory(t.Context(), backendName)
		require.NoError(t, collectErr)
		retentionReceipt, collectErr := sweep.CollectRetentionInventory(t.Context(), backendName)
		require.NoError(t, collectErr)
		disposition, collectErr := sweep.RecordBackendInventory(
			provisionReceipt, retentionReceipt,
		)
		require.NoError(t, collectErr)
		require.Equal(t, wantDisposition, disposition)
	}
	require.NoError(t, sweep.SealInventory())
	result, err := sweep.Project(placement.ReconciliationProjection{
		Placements:         projection.Placements,
		Conflicts:          projection.Conflicts,
		UntrustedPositives: projection.UntrustedPositives,
	})
	require.NoError(t, err)
	return result, sweep
}
