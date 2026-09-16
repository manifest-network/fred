package docker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	networktypes "github.com/docker/docker/api/types/network"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

func TestConcurrentDeprovisionsDoNotWaitForFleetNetworkInventory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const closeCount = 8
		var inventories, removals atomic.Int32
		inventoryRelease := make(chan struct{})
		releaseInventory := sync.OnceFunc(func() { close(inventoryRelease) })
		mock := &mockDockerClient{
			RemoveContainerFn: func(context.Context, string) error { return nil },
			ListManagedNetworksFn: func(ctx context.Context) ([]networktypes.Inspect, error) {
				inventories.Add(1)
				select {
				case <-inventoryRelease:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return []networktypes.Inspect{{
					Name: TenantNetworkName("tenant-a"), Labels: map[string]string{LabelTenant: "tenant-a"},
				}}, nil
			},
			RemoveTenantNetworkIfEmptyFn: func(context.Context, string) error {
				removals.Add(1)
				return nil
			},
		}
		leases := make([]string, closeCount)
		projections := make(map[string]*provision, closeCount)
		for index := range leases {
			lease := uuid.NewString()
			leases[index] = lease
			projections[lease] = &provision{ProvisionState: leasesm.ProvisionState{
				LeaseUUID: lease, Tenant: "tenant-a", Status: backend.ProvisionStatusReady,
				Quantity: 1, ContainerIDs: []string{lease + "-container"},
			}}
		}
		b := newBackendForProvisionTest(t, mock, projections)
		b.cfg.NetworkIsolation = ptrBool(true)
		for _, lease := range leases {
			installReadyRuntimeProofForTest(t, b, lease)
		}
		var closes sync.WaitGroup
		defer func() {
			releaseInventory()
			b.stopCancel()
			closes.Wait()
			b.wg.Wait()
		}()
		results := make(chan error, closeCount)
		for _, lease := range leases {
			closes.Go(func() { results <- b.Deprovision(t.Context(), lease) })
		}
		synctest.Wait()
		require.Zero(t, inventories.Load(), "each completed close must stay out of the fleet network workflow")
		require.Len(t, results, closeCount, "all close replies must complete while fleet inventory would block")
		for range closeCount {
			require.NoError(t, <-results)
		}
		receipts, err := b.callbackStore.LookupClosedLeaseReceipts(leases)
		require.NoError(t, err)
		require.Len(t, receipts, closeCount, "reply success still requires each durable close receipt")
		require.Zero(t, removals.Load())

		// The next level-triggered state pass retains ownership of network
		// cleanup. No per-close goroutine or notification is needed for convergence.
		releaseInventory()
		require.NoError(t, b.recoverState(t.Context()))
		require.Equal(t, int32(1), inventories.Load())
		require.Equal(t, int32(1), removals.Load())
	})
}

func TestNetworkCleanupCoalescesConcurrentSweepsAndRetriesAfterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var inventories atomic.Int32
		mock := &mockDockerClient{
			ListManagedNetworksFn: func(ctx context.Context) ([]networktypes.Inspect, error) {
				if inventories.Add(1) == 1 {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return nil, nil
			},
		}
		b := newBackendForTest(mock, nil)
		defer b.stopCancel()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		firstDone := make(chan struct{})
		go func() {
			b.cleanupOrphanedNetworks(ctx)
			close(firstDone)
		}()
		synctest.Wait()
		require.Equal(t, int32(1), inventories.Load())
		const concurrentSweeps = 8
		done := make(chan struct{}, concurrentSweeps)
		for range concurrentSweeps {
			go func() {
				b.cleanupOrphanedNetworks(t.Context())
				done <- struct{}{}
			}()
		}
		synctest.Wait()
		require.Len(t, done, concurrentSweeps, "overlapping requests must coalesce without waiting for the active sweep")
		require.Equal(t, int32(1), inventories.Load(), "one fleet inventory, regardless of concurrent callers")
		cancel()
		synctest.Wait()
		<-firstDone
		b.cleanupOrphanedNetworks(ctx)
		require.Equal(t, int32(1), inventories.Load(), "canceled passes cannot start another inventory")
		b.cleanupOrphanedNetworks(t.Context())
		require.Equal(t, int32(2), inventories.Load(), "a canceled sweep must release ownership for the next cadence")
	})
}

func TestNetworkCleanupRechecksTenantAfterFleetInventory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var removals atomic.Int32
		inventoryRelease := make(chan struct{})
		releaseInventory := sync.OnceFunc(func() { close(inventoryRelease) })
		defer releaseInventory()
		mock := &mockDockerClient{
			ListManagedNetworksFn: func(context.Context) ([]networktypes.Inspect, error) {
				<-inventoryRelease
				return []networktypes.Inspect{{
					Name: TenantNetworkName("tenant-a"), Labels: map[string]string{LabelTenant: "tenant-a"},
				}}, nil
			},
			RemoveTenantNetworkIfEmptyFn: func(context.Context, string) error {
				removals.Add(1)
				return nil
			},
		}
		b := newBackendForTest(mock, nil)
		defer b.stopCancel()
		done := make(chan struct{})
		go func() {
			b.cleanupOrphanedNetworks(t.Context())
			close(done)
		}()
		synctest.Wait()
		// Provision admission can publish a tenant owner after the fleet
		// inventory began, before this pass reaches its per-tenant recheck.
		b.provisionsMu.Lock()
		b.provisions[durableCallbackTestLeaseUUID] = &provision{ProvisionState: leasesm.ProvisionState{
			LeaseUUID: durableCallbackTestLeaseUUID, Tenant: "tenant-a", Status: backend.ProvisionStatusProvisioning,
		}}
		b.provisionsMu.Unlock()
		releaseInventory()
		synctest.Wait()
		<-done
		require.Zero(t, removals.Load(), "new tenant ownership must veto a stale empty-network snapshot")
	})
}
