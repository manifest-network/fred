package placement

import (
	"context"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// unusedIdentityResolver satisfies the client constructor. A fenced client
// never prepares a request, so it is never consulted.
type unusedIdentityResolver struct{}

func (unusedIdentityResolver) ExpectedBackendStorageIdentity(string) (backendidentity.ID, bool) {
	return backendidentity.ID{}, false
}

func fencedClientForTest(t *testing.T, name string) *backend.HTTPClient {
	t.Helper()
	policy, err := backend.NewFencedConnectionPolicy(name)
	require.NoError(t, err)
	client, err := backend.NewIdentityBoundHTTPClient(policy, backend.HTTPClientOptions{}, unusedIdentityResolver{})
	require.NoError(t, err)
	return client
}

func TestMaintenanceOnAFencedBackendIsRefusedBeforeAdmission(t *testing.T) {
	authority, _ := newMaintenanceCoordinatorForTest(
		t, maintenanceActiveLeaseReader(), fencedClientForTest(t, "backend-a"),
	)
	id := mustMaintenanceID(t, maintenanceIDA)
	preparation := authority.prepareMaintenanceCommand(
		t.Context(), id, maintenanceLease, "tenant-test", MaintenanceCommandRestart, nil,
	)
	require.False(t, preparation.Authorized())
	require.ErrorIs(t, preparation.Err(), backend.ErrBackendFenced)
	assert.Equal(t, MaintenanceApplicationServiceUnavailable, resultForPreparation(preparation).outcome)

	_, found, err := authority.lookupMaintenanceCommand(maintenanceLease, id)
	require.NoError(t, err)
	assert.False(t, found, "nothing is journaled to dispatch when the fence lifts")
}

func TestRestoreFromAFencedBackendIsRefusedBeforeAdmission(t *testing.T) {
	const sourceLease = "71638ef8-1401-4f14-a355-1ae02afeb35c"
	const targetLease = "81638ef8-1401-4f14-a355-1ae02afeb35c"
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, store, "backend-a")
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{sourceLease: "backend-a"},
	})
	base, err := newOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(fencedClientForTest(t, "backend-a")))
	setProviderControlPlaneForTest(t, execution, executionLeaseReaderFunc(
		func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			state := billingtypes.LEASE_STATE_PENDING
			if leaseUUID == sourceLease {
				state = billingtypes.LEASE_STATE_CLOSED
			}
			return &billingtypes.Lease{
				Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
				State: state, Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
			}, nil
		}))
	authority, err := execution.RestoreCoordinator(nil)
	require.NoError(t, err)
	request, err := NewRestoreApplicationRequest(targetLease, "tenant-test", sourceLease)
	require.NoError(t, err)

	result := authority.ExecuteApplication(t.Context(), request)
	assert.Equal(t, RestoreApplicationSourceUnavailable, result.Disposition())
	require.ErrorIs(t, result.Err(), backend.ErrBackendFenced)
	assert.Equal(t, StateAbsent, store.Lookup(targetLease).State(), "no restore attempt was journaled")
}

func TestReprovisionOnAFencedOwnerFindsNoRoute(t *testing.T) {
	const lease = "91638ef8-1401-4f14-a355-1ae02afeb35c"
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, store, "backend-a")
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{lease: "backend-a"},
	})
	base, err := newOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(fencedClientForTest(t, "backend-a")))
	setProviderControlPlaneForTest(t, execution, executionLeaseReaderFunc(
		func(context.Context, string) (*billingtypes.Lease, error) { return nil, nil }))
	authority, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)

	route, err := authority.routeProvision(t.Context(), lease, "sku-test", nil, nil)
	require.NoError(t, err)
	assert.False(t, route.valid(), "a lease confirmed on a fenced backend waits")
}

func TestReconciliationReprovisionOnAFencedOwnerFindsNoRoute(t *testing.T) {
	const lease = "a1638ef8-1401-4f14-a355-1ae02afeb35c"
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, store, "backend-a")
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{lease: "backend-a"},
	})
	base, err := newOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(fencedClientForTest(t, "backend-a")))
	authority, err := reconciliationCoordinatorWithReaderForTest(t, execution, &reconciliationSweepReader{})
	require.NoError(t, err)

	owned := store.Lookup(lease).RecordRevision()
	require.True(t, owned.Valid())
	route, err := authority.routeProvision(t.Context(), lease, "sku-test", owned, nil, nil)
	require.NoError(t, err)
	assert.False(t, route.valid(), "reconciliation never writes an attempt against a fenced owner")
}
