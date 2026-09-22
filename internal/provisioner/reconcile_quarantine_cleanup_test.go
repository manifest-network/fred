package provisioner

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

// Exercise the public composition that stranded terminal-chain workloads:
// accepted provision -> rejected inventory -> blocked callback -> fresh exact
// inventory -> callback settlement -> orphan teardown without another event.
func TestReconcilerQuarantinedInflightOperationsRecoverAndCleanTerminalLeases(t *testing.T) {
	const (
		first   = "00000000-0000-4000-8000-000000000291"
		second  = "00000000-0000-4000-8000-000000000292"
		sibling = "00000000-0000-4000-8000-000000000293"
	)
	owner := &mockReconcilerBackend{name: "backend-a"}
	peer := &mockReconcilerBackend{name: "backend-b"}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: owner, IsDefault: true}, {Backend: peer},
	}})
	require.NoError(t, err)
	store, err := placementstore.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	leases := make(map[string]billingtypes.Lease)
	chain := &chaintest.MockClient{
		GetLeaseFunc: func(_ context.Context, id string) (*billingtypes.Lease, error) {
			lease, found := leases[id]
			if !found {
				return nil, nil
			}
			return &lease, nil
		},
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			var pending []billingtypes.Lease
			for _, lease := range leases {
				if lease.State == billingtypes.LEASE_STATE_PENDING {
					pending = append(pending, lease)
				}
			}
			return pending, nil
		},
	}
	acknowledged := make(chan string, 8)
	ack := &mockAcknowledger{acknowledgeFn: func(_ context.Context, id string) (bool, string, error) {
		acknowledged <- id
		if leases[id].State == billingtypes.LEASE_STATE_CLOSED {
			return false, "", errors.New("lease is already closed")
		}
		return true, "tx", nil
	}}
	tracker := newMockInFlightTracker(nil)
	r, err := newTestReconciler(t, ReconcilerConfig{}, chain, ack, router, tracker, store)
	require.NoError(t, err)
	require.NoError(t, r.ReconcileAll(t.Context()))
	executionValue, ok := testReconciliationExecutions.Load(r.coordinator)
	require.True(t, ok)
	execution := executionValue.(*placement.ExecutionCoordinator)
	setTestProviderControlPlane(t, execution, chain, ack)
	provision, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)
	callbacks, err := execution.AuthenticatedCallbackCoordinator(callbackTestProofConsumer)
	require.NoError(t, err)
	for _, id := range []string{first, second} {
		lease := redeliveryLease(id, billingtypes.LEASE_STATE_PENDING)
		lease.Items = []billingtypes.LeaseItem{
			{SkuUuid: "sku-app", Quantity: 1, ServiceName: "app"},
			{SkuUuid: "sku-db", Quantity: 1, ServiceName: "db"},
		}
		leases[id] = lease
		request, requestErr := placement.NewProvisionEventRequest(id, lease.Tenant)
		require.NoError(t, requestErr)
		result := provision.ExecuteCurrentLease(t.Context(), request)
		require.NoError(t, result.Err())
		require.Equal(t, placement.ProvisionEventStarted, result.Disposition())
		metadata, found := tracker.GetInFlight(id)
		require.True(t, found)
		owner.provisions = append(owner.provisions, backend.ProvisionInfo{
			LeaseUUID: id, ProviderUUID: lease.ProviderUuid, Tenant: lease.Tenant,
			Status: backend.ProvisionStatusReady, Items: ExtractLeaseItems(&lease),
			LifecycleGeneration: &backend.LifecycleGenerationObservation{
				Kind: backend.LifecycleGenerationTyped, ID: metadata.OperationID.String(),
			},
		})
	}
	leases[sibling] = redeliveryLease(sibling, billingtypes.LEASE_STATE_CLOSED)
	peer.provisions = []backend.ProvisionInfo{{LeaseUUID: sibling, Status: backend.ProvisionStatusReady}}
	owner.retentionErr = errors.New("retention inventory unavailable")
	require.NoError(t, r.ReconcileAll(t.Context()))
	require.Equal(t, []string{sibling}, peer.deprovisionCalls,
		"an unavailable owner inventory must not prevent terminal sibling cleanup")
	peer.provisions = nil
	for _, id := range []string{first, second} {
		require.Equal(t, placement.StateUnusable, store.Lookup(id).State())
		metadata, found := tracker.GetInFlight(id)
		require.True(t, found)
		_, applyErr := callbacks.Apply(t.Context(), callbackCommand(t, backend.CallbackPayload{
			LeaseUUID: id, OperationID: metadata.OperationID.String(),
			BackendStorageID: testBackendStorageID(owner.name).String(), Status: backend.CallbackStatusSuccess,
		}))
		require.ErrorIs(t, applyErr, placement.ErrOperationSettlementGenerationUnavailable)
		lease := leases[id]
		lease.State = billingtypes.LEASE_STATE_CLOSED
		leases[id] = lease
	}
	require.Empty(t, drainOverlapCalls(acknowledged), "quarantine must block chain settlement")
	require.Empty(t, owner.deprovisionCalls, "no teardown authority has been restored yet")

	owner.retentionErr = nil
	require.NoError(t, r.ReconcileAll(t.Context()))
	for _, id := range []string{first, second} {
		require.Equal(t, placement.StateConfirmed, store.Lookup(id).State(),
			"a fresh exact generation must recover the sole-owner quarantine despite its in-flight callback")
		metadata, found := tracker.GetInFlight(id)
		require.True(t, found, "inventory cannot complete the existing operation")
		settled, applyErr := callbacks.Apply(t.Context(), callbackCommand(t, backend.CallbackPayload{
			LeaseUUID: id, OperationID: metadata.OperationID.String(),
			BackendStorageID: testBackendStorageID(owner.name).String(), Status: backend.CallbackStatusSuccess,
		}))
		require.NoError(t, applyErr)
		_, _, publishes := settled.Event()
		require.False(t, publishes, "terminal chain state must not publish a Ready event")
		require.False(t, tracker.IsInFlight(id), "authenticated terminal-chain callback settles exact work")
	}
	require.ElementsMatch(t, []string{first, second}, drainOverlapCalls(acknowledged))
	require.Empty(t, owner.deprovisionCalls,
		"the recovery sweep cannot reuse inventory captured before callback settlement for cleanup")
	require.NoError(t, r.ReconcileAll(t.Context()))
	require.ElementsMatch(t, []string{first, second}, owner.deprovisionCalls,
		"fresh level-triggered reconciliation must close both terminal leases without event redelivery")
	require.Len(t, owner.provisionCalls, 2, "recovery must never launch a replacement")
	require.Empty(t, peer.provisionCalls)
}
