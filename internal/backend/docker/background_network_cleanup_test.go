package docker

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	networktypes "github.com/docker/docker/api/types/network"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestConcurrentDeprovisionsDoNotWaitForFleetNetworkInventory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const closeCount = 8
		var inventories, removals atomic.Int32
		inventoryRelease := make(chan struct{})
		releaseInventory := sync.OnceFunc(func() { close(inventoryRelease) })
		mock := &mockDockerClient{
			RemoveContainerFn: func(context.Context, string) error { return nil },
			ListIdleManagedNetworksFn: func(ctx context.Context) ([]networktypes.Inspect, error) {
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
			RemoveTenantNetworkIfEmptyFn: func(context.Context, string) (tenantNetworkRemoval, error) {
				removals.Add(1)
				return tenantNetworkRemoved, nil
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

		// The independent network worker owns eventual reclamation. No per-close
		// goroutine, state-recovery sweep, or notification is needed.
		releaseInventory()
		b.wg.Go(b.networkCleanupLoop)
		synctest.Wait()
		require.Equal(t, int32(1), inventories.Load())
		require.Equal(t, int32(1), removals.Load())
	})
}

func TestNetworkCleanupWorkerBoundsPassesAndStopsWithBackend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var inventories atomic.Int32
		budgetBefore := testutil.ToFloat64(networkReclamationTotal.WithLabelValues("budget_exhausted"))
		listErrorsBefore := testutil.ToFloat64(networkReclamationTotal.WithLabelValues("list_error"))
		mock := &mockDockerClient{
			ListIdleManagedNetworksFn: func(ctx context.Context) ([]networktypes.Inspect, error) {
				inventories.Add(1)
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}
		b := newBackendForTest(mock, nil)
		b.cfg.ReconcileInterval = time.Minute
		defer func() { b.stopCancel(); b.wg.Wait() }()
		b.wg.Go(b.networkCleanupLoop)
		synctest.Wait()
		require.Equal(t, int32(1), inventories.Load())
		time.Sleep(30 * time.Second)
		synctest.Wait()
		require.Equal(t, budgetBefore+1, testutil.ToFloat64(networkReclamationTotal.WithLabelValues("budget_exhausted")))
		require.Equal(t, int32(1), inventories.Load(), "a timed-out pass waits for the next cadence")
		time.Sleep(30 * time.Second)
		synctest.Wait()
		require.Equal(t, int32(2), inventories.Load(), "the next pass receives a fresh independent budget")
		b.stopCancel()
		b.wg.Wait()
		require.Equal(t, listErrorsBefore, testutil.ToFloat64(networkReclamationTotal.WithLabelValues("list_error")), "shutdown cancellation is not a reclamation failure")
		require.Equal(t, budgetBefore+1, testutil.ToFloat64(networkReclamationTotal.WithLabelValues("budget_exhausted")))
		time.Sleep(time.Minute)
		require.Equal(t, int32(2), inventories.Load(), "the worker must stop before its dependencies can close")
		b.networkCleanupLoop()
		require.Equal(t, int32(2), inventories.Load(), "a canceled lifetime cannot start another sweep")
	})
}

func TestNetworkBacklogDoesNotStarvePendingOperationRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
		require.NoError(t, err)
		store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
			DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		spec := dockerOperationIntentSpec(t, storageID)
		_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
		require.NoError(t, err)
		container := dockerIntentContainer(spec, "recovered-container", spec.Items[0].SKU, 0)
		container.CreatedAt = time.Now().Add(-time.Hour)
		b := newOperationIntentRecoveryBackend(t, store, storageID, []ContainerInfo{container}, readyIntentProjection(spec, container.ContainerID))
		b.cfg.NetworkIsolation = ptrBool(true)
		b.cfg.ReconcileInterval = time.Minute
		defer func() { b.stopCancel(); b.wg.Wait() }()
		var attempts, removals atomic.Int32
		mock := b.docker.(*mockDockerClient)
		mock.ListIdleManagedNetworksFn = func(context.Context) ([]networktypes.Inspect, error) {
			networks := make([]networktypes.Inspect, 1000)
			for index := range networks {
				tenant := fmt.Sprintf("idle-tenant-%d", index)
				networks[index] = networktypes.Inspect{Name: TenantNetworkName(tenant), Labels: map[string]string{LabelTenant: tenant}}
			}
			return networks, nil
		}
		mock.RemoveTenantNetworkIfEmptyFn = func(ctx context.Context, _ string) (tenantNetworkRemoval, error) {
			attempts.Add(1)
			select {
			case <-time.After(250 * time.Millisecond):
				removals.Add(1)
				return tenantNetworkRemoved, nil
			case <-ctx.Done():
				return tenantNetworkRemovalUnknown, ctx.Err()
			}
		}
		b.wg.Go(b.networkCleanupLoop)
		synctest.Wait()
		require.Equal(t, int32(1), attempts.Load(), "the orphan backlog must already be in flight")
		ctx, cancel := context.WithTimeout(b.stopCtx, 2*time.Second)
		defer cancel()
		require.NoError(t, b.reconcileStateAndOperations(ctx))
		require.NoError(t, ctx.Err(), "network removal must not consume the operation tick's budget")
		intents, err := b.operationSettlement.ListOperationIntents()
		require.NoError(t, err)
		require.Empty(t, intents, "the real pending operation must settle before the orphan backlog drains")
		callbacks, err := store.ListPending()
		require.NoError(t, err)
		require.Len(t, callbacks, 1)
		require.Equal(t, backend.CallbackStatusSuccess, callbacks[0].Status)
		require.Zero(t, removals.Load(), "operation progress must not wait for even the first slow network removal")
	})
}

func TestNetworkCleanupRechecksTenantAfterFleetInventory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var removals atomic.Int32
		inventoryRelease := make(chan struct{})
		releaseInventory := sync.OnceFunc(func() { close(inventoryRelease) })
		defer releaseInventory()
		mock := &mockDockerClient{
			ListIdleManagedNetworksFn: func(context.Context) ([]networktypes.Inspect, error) {
				<-inventoryRelease
				return []networktypes.Inspect{{
					Name: TenantNetworkName("tenant-a"), Labels: map[string]string{LabelTenant: "tenant-a"},
				}}, nil
			},
			RemoveTenantNetworkIfEmptyFn: func(context.Context, string) (tenantNetworkRemoval, error) {
				removals.Add(1)
				return tenantNetworkRemoved, nil
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

func TestNetworkCleanupCountsReclamationOutcomes(t *testing.T) {
	outcomes := map[string]tenantNetworkRemoval{
		"removed": tenantNetworkRemoved,
		"absent":  tenantNetworkAbsent,
		"in_use":  tenantNetworkInUse,
		"error":   tenantNetworkRemovalUnknown,
	}
	before := make(map[string]float64)
	networks := make([]networktypes.Inspect, 0, len(outcomes)+1)
	for _, label := range []string{"removed", "absent", "in_use", "error", "tenant_active"} {
		before[label] = testutil.ToFloat64(networkReclamationTotal.WithLabelValues(label))
		networks = append(networks, networktypes.Inspect{Name: TenantNetworkName(label), Labels: map[string]string{LabelTenant: label}})
	}
	mock := &mockDockerClient{
		ListIdleManagedNetworksFn: func(context.Context) ([]networktypes.Inspect, error) { return networks, nil },
		RemoveTenantNetworkIfEmptyFn: func(_ context.Context, tenant string) (tenantNetworkRemoval, error) {
			if tenant == "error" {
				return tenantNetworkRemovalUnknown, errors.New("daemon removal refused")
			}
			outcome, ok := outcomes[tenant]
			require.True(t, ok, "an owned tenant must never reach the mutation sink")
			return outcome, nil
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		"owned": {ProvisionState: leasesm.ProvisionState{Tenant: "tenant_active"}},
	})
	defer b.stopCancel()
	b.cleanupOrphanedNetworks(t.Context())
	for label, count := range before {
		require.Equal(t, count+1, testutil.ToFloat64(networkReclamationTotal.WithLabelValues(label)), label)
	}
	listErrorsBefore := testutil.ToFloat64(networkReclamationTotal.WithLabelValues("list_error"))
	mock.ListIdleManagedNetworksFn = func(context.Context) ([]networktypes.Inspect, error) {
		return nil, errors.New("daemon inventory unavailable")
	}
	b.cleanupOrphanedNetworks(t.Context())
	require.Equal(t, listErrorsBefore+1, testutil.ToFloat64(networkReclamationTotal.WithLabelValues("list_error")))
}

func TestNetworkCleanupBusyTenantDoesNotBlockShutdownAndRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var removals atomic.Int32
		mock := &mockDockerClient{
			ListIdleManagedNetworksFn: func(context.Context) ([]networktypes.Inspect, error) {
				return []networktypes.Inspect{{Name: TenantNetworkName("tenant-a"), Labels: map[string]string{LabelTenant: "tenant-a"}}}, nil
			},
			RemoveTenantNetworkIfEmptyFn: func(context.Context, string) (tenantNetworkRemoval, error) {
				removals.Add(1)
				return tenantNetworkRemoved, nil
			},
		}
		b := newBackendForTest(mock, nil)
		b.cfg.ReconcileInterval = time.Minute
		defer func() { b.stopCancel(); b.wg.Wait() }()
		stripe := b.tenantNetworkMu("tenant-a")
		stripe.Lock()
		locked := true
		defer func() {
			if locked {
				stripe.Unlock()
			}
		}()
		busyBefore := testutil.ToFloat64(networkReclamationTotal.WithLabelValues("tenant_busy"))
		b.wg.Go(b.networkCleanupLoop)
		synctest.Wait()
		require.Zero(t, removals.Load(), "a busy tenant stripe must never reach the removal sink")
		require.Equal(t, busyBefore+1, testutil.ToFloat64(networkReclamationTotal.WithLabelValues("tenant_busy")))
		stripe.Unlock()
		locked = false
		time.Sleep(time.Minute)
		synctest.Wait()
		require.Equal(t, int32(1), removals.Load(), "a previously busy tenant remains eligible on the next pass")
		stripe.Lock()
		locked = true
		time.Sleep(time.Minute)
		synctest.Wait()
		require.Equal(t, busyBefore+2, testutil.ToFloat64(networkReclamationTotal.WithLabelValues("tenant_busy")))
		b.stopCancel()
		b.wg.Wait()
		require.Equal(t, int32(1), removals.Load(), "shutdown must drain the network worker while the foreground stripe remains held")
	})
}
