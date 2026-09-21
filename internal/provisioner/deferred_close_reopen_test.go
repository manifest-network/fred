package provisioner

import (
	"context"
	"path/filepath"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

func TestReconcilerRecoversDiscardedDeferredCloseAcrossReopen(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "discarded event"
		if restart {
			name = "provider restart"
		}
		t.Run(name, func(t *testing.T) {
			const leaseUUID = "00000000-0000-4000-8000-000000003107"
			const tenant = "tenant-close-recovery"
			path := filepath.Join(t.TempDir(), "placements.db")
			store, err := placementstore.NewStore(path)
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })
			require.NoError(t, placementstore.ConfigureBackendTopologyWithStorageIdentities(store,
				[]string{"backend-a"}, map[string]backendidentity.ID{"backend-a": testBackendStorageID("backend-a")}))
			client := &mockReconcilerBackend{
				name: "backend-a",
				provisions: []backend.ProvisionInfo{{
					LeaseUUID: leaseUUID, BackendName: "backend-a", Tenant: tenant, ProviderUUID: placementstore.ProviderUUID,
					Status:              backend.ProvisionStatusReady,
					LifecycleGeneration: &backend.LifecycleGenerationObservation{Kind: backend.LifecycleGenerationLegacy},
				}},
			}
			router, err := backend.NewRouter(backend.RouterConfig{
				Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}},
			})
			require.NoError(t, err)
			chain := &chaintest.MockClient{GetLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
				require.Equal(t, leaseUUID, uuid)
				return &billingtypes.Lease{Uuid: uuid, Tenant: tenant, ProviderUuid: placementstore.ProviderUUID,
					State: billingtypes.LEASE_STATE_CLOSED}, nil
			}}
			bind := func(current *placement.Store) (*placement.ExecutionCoordinator, *placement.ReconciliationCoordinator) {
				base, bindErr := current.BindOperationCoordinator(nil)
				require.NoError(t, bindErr)
				execution := bindTestBackendRuntime(t, base, router)
				return execution, bindTestReconciliationCoordinator(t, current, execution, chain, nil, nil)
			}
			execution, coordinator := bind(store)
			sweep, err := coordinator.BeginSweep()
			require.NoError(t, err)
			_, err = sweep.CollectProvisionInventory(t.Context(), "backend-a")
			require.NoError(t, err)
			closeAuthority, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
			require.NoError(t, err)
			result := closeAuthority.DeprovisionEvent(t.Context(), leaseUUID)
			require.Equal(t, placement.DeprovisionEventDeferred, result.Disposition())
			require.True(t, result.Deferred().Valid())
			require.Empty(t, client.deprovisionCalls)
			sweep.End() // Lose the queued hint and its unprojected sweep together.
			if restart {
				require.NoError(t, store.Close())
				store, err = placementstore.NewStore(path)
				require.NoError(t, err)
				_, coordinator = bind(store)
			}
			reconciler, err := newTestReconciler(t, ReconcilerConfig{Coordinator: coordinator}, chain, noopAck, router, nil, store)
			require.NoError(t, err)
			require.NoError(t, reconciler.ReconcileAll(t.Context()))
			client.mu.Lock()
			defer client.mu.Unlock()
			require.Equal(t, []string{leaseUUID}, client.deprovisionCalls,
				"the terminal chain read and new paired inventory recover the same close without its event or queue")
		})
	}
}
