package provisioner

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

func TestReconcilerEndpointOverlapQuarantinesOnlyAmbiguousLease(t *testing.T) {
	for _, status := range []backend.ProvisionStatus{backend.ProvisionStatusReady, backend.ProvisionStatusFailed} {
		for _, peerReports := range []bool{false, true} {
			name := string(status) + "/sole_reporter"
			if peerReports {
				name = string(status) + "/peer_also_reports"
			}
			t.Run(name, func(t *testing.T) {
				const ambiguousLease = "00000000-0000-4000-8000-000000000271"
				const healthyLease = "00000000-0000-4000-8000-000000000272"
				const closedLease = "00000000-0000-4000-8000-000000000273"
				var leases []billingtypes.Lease
				acknowledged := make(chan string, 8)
				rejected := make(chan string, 8)
				chain := &chaintest.MockClient{
					GetLeaseFunc: func(_ context.Context, id string) (*billingtypes.Lease, error) {
						if id == closedLease {
							return &billingtypes.Lease{Uuid: id, Tenant: "tenant-overlap", ProviderUuid: placementstore.ProviderUUID, State: billingtypes.LEASE_STATE_CLOSED}, nil
						}
						for _, lease := range leases {
							if lease.Uuid == id {
								return &lease, nil
							}
						}
						return nil, nil
					},
					GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
						return leases, nil
					},
					RejectLeasesFunc: func(_ context.Context, ids []string, _ string) (uint64, []string, error) {
						for _, id := range ids {
							rejected <- id
						}
						return 1, []string{"tx"}, nil
					},
				}
				ack := &mockAcknowledger{acknowledgeFn: func(_ context.Context, id string) (bool, string, error) {
					acknowledged <- id
					return true, "tx", nil
				}}
				owner := &mockReconcilerBackend{name: "backend-a"}
				peer := &mockReconcilerBackend{name: "backend-b"}
				router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
					{Backend: owner, IsDefault: true}, {Backend: peer},
				}})
				require.NoError(t, err)
				store, err := placementstore.NewStore(filepath.Join(t.TempDir(), "placements.db"))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, store.Close()) })
				r, err := newTestReconciler(t, ReconcilerConfig{}, chain, ack, router, nil, store)
				require.NoError(t, err)
				require.NoError(t, r.ReconcileAll(t.Context()), "establish the topology baseline")

				leases = overlapPendingLeases(ambiguousLease, healthyLease)
				owner.provisions = []backend.ProvisionInfo{
					{LeaseUUID: ambiguousLease, Status: status, FailCount: 100},
					{LeaseUUID: healthyLease, Status: backend.ProvisionStatusReady},
					{LeaseUUID: closedLease, Status: backend.ProvisionStatusReady},
				}
				owner.retentions = []backend.RetainedLease{{LeaseUUID: ambiguousLease}}
				if peerReports {
					peer.provisions = []backend.ProvisionInfo{{LeaseUUID: ambiguousLease, Status: backend.ProvisionStatusReady}}
				}
				require.NoError(t, r.ReconcileAll(t.Context()))

				assert.Equal(t, []string{healthyLease}, drainOverlapCalls(acknowledged),
					"one contradictory lease must not suppress a fresh Ready sibling from the same backend")
				assert.Empty(t, drainOverlapCalls(rejected), "an ambiguous Failed row is not chain rejection authority")
				quarantine := store.Lookup(ambiguousLease)
				assert.Equal(t, placement.StateUnusable, quarantine.State())
				assert.True(t, quarantine.Conflict)
				wantReporters := []string{owner.name}
				if peerReports {
					wantReporters = append(wantReporters, peer.name)
				}
				assert.ElementsMatch(t, wantReporters, quarantine.ConflictBackends,
					"filtering the overlap payload must preserve every raw positive reporter")
				assert.Equal(t, placement.StateConfirmed, store.Lookup(healthyLease).State())
				assert.Equal(t, owner.name, store.Lookup(healthyLease).Backend)
				for _, client := range []*mockReconcilerBackend{owner, peer} {
					client.mu.Lock()
					assert.Empty(t, client.provisionCalls, "neither overlap nor healthy presence authorizes a new launch")
					if client == owner {
						assert.Equal(t, []string{closedLease}, client.deprovisionCalls,
							"overlap filtering must preserve terminal sibling orphan cleanup")
					} else {
						assert.Empty(t, client.deprovisionCalls)
					}
					client.mu.Unlock()
				}
			})
		}
	}
}

func TestReconcilerOverlapQuarantineRequiresFreshBoundResolution(t *testing.T) {
	for _, fault := range []string{
		"owner_refresh_failed", "owner_storage_identity_changed",
		"peer_refresh_failed", "peer_storage_identity_changed",
	} {
		t.Run(fault, func(t *testing.T) {
			const ambiguousLease = "00000000-0000-4000-8000-000000000281"
			const healthyLease = "00000000-0000-4000-8000-000000000282"
			var leases []billingtypes.Lease
			acknowledged := make(chan string, 8)
			chain := &chaintest.MockClient{GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
				return leases, nil
			}}
			ack := &mockAcknowledger{acknowledgeFn: func(_ context.Context, id string) (bool, string, error) {
				acknowledged <- id
				return true, "tx", nil
			}}
			owner := &mockReconcilerBackend{name: "backend-a"}
			peer := &mockReconcilerBackend{name: "backend-b"}
			healthy := &mockReconcilerBackend{name: "backend-c"}
			router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
				{Backend: owner, IsDefault: true}, {Backend: peer}, {Backend: healthy},
			}})
			require.NoError(t, err)
			store, err := placementstore.NewStore(filepath.Join(t.TempDir(), "placements.db"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			r, err := newTestReconciler(t, ReconcilerConfig{}, chain, ack, router, nil, store)
			require.NoError(t, err)
			require.NoError(t, r.ReconcileAll(t.Context()))
			leases = overlapPendingLeases(ambiguousLease, healthyLease)
			owner.provisions = []backend.ProvisionInfo{{LeaseUUID: ambiguousLease, Status: backend.ProvisionStatusReady}}
			owner.retentions = []backend.RetainedLease{{LeaseUUID: ambiguousLease}}
			healthy.provisions = []backend.ProvisionInfo{{LeaseUUID: healthyLease, Status: backend.ProvisionStatusReady}}
			require.NoError(t, r.ReconcileAll(t.Context()))
			require.Equal(t, placement.StateUnusable, store.Lookup(ambiguousLease).State())
			require.Equal(t, []string{healthyLease}, drainOverlapCalls(acknowledged))

			owner.retentions = nil
			switch fault {
			case "owner_refresh_failed":
				owner.refreshErr = errors.New("owner inventory refresh failed")
			case "peer_refresh_failed":
				peer.refreshErr = errors.New("peer inventory refresh failed")
			default:
				changed := owner
				if fault == "peer_storage_identity_changed" {
					changed = peer
				}
				value, found := testReconciliationInventoryRouters.Load(r.coordinator)
				require.True(t, found)
				fixture := value.(*testInventoryRouter).backends[changed.name]
				fixture.stage(testBackendStorageID("different-storage"), changed.provisions, nil)
			}
			require.NoError(t, r.ReconcileAll(t.Context()))
			assert.Equal(t, []string{healthyLease}, drainOverlapCalls(acknowledged),
				"a clean endpoint shape cannot upgrade stale or foreign-storage observations")
			quarantine := store.Lookup(ambiguousLease)
			assert.Equal(t, placement.StateUnusable, quarantine.State())
			assert.Equal(t, []string{owner.name}, quarantine.ConflictBackends)
			assert.Equal(t, placement.StateConfirmed, store.Lookup(healthyLease).State())

			owner.refreshErr = nil
			peer.refreshErr = nil
			value, found := testReconciliationInventoryRouters.Load(r.coordinator)
			require.True(t, found)
			for _, fixture := range value.(*testInventoryRouter).backends {
				fixture.clearStage()
			}
			require.NoError(t, r.ReconcileAll(t.Context()))
			assert.ElementsMatch(t, []string{ambiguousLease, healthyLease}, drainOverlapCalls(acknowledged),
				"preserving uncertainty must not turn a recoverable sole-reporter quarantine into a permanent conflict")
			assert.Equal(t, placement.StateConfirmed, store.Lookup(ambiguousLease).State())
			assert.Equal(t, owner.name, store.Lookup(ambiguousLease).Backend)
			for _, client := range []*mockReconcilerBackend{owner, peer, healthy} {
				client.mu.Lock()
				assert.Empty(t, client.provisionCalls)
				assert.Empty(t, client.deprovisionCalls)
				client.mu.Unlock()
			}
		})
	}
}

func overlapPendingLeases(ids ...string) []billingtypes.Lease {
	leases := make([]billingtypes.Lease, 0, len(ids))
	for _, id := range ids {
		leases = append(leases, billingtypes.Lease{
			Uuid: id, Tenant: "tenant-overlap", ProviderUuid: placementstore.ProviderUUID,
			State: billingtypes.LEASE_STATE_PENDING,
			Items: []billingtypes.LeaseItem{{SkuUuid: "sku-overlap", Quantity: 1}},
		})
	}
	return leases
}

func drainOverlapCalls(calls <-chan string) []string {
	var drained []string
	for {
		select {
		case call := <-calls:
			drained = append(drained, call)
		default:
			return drained
		}
	}
}

func TestReconcilerPairedOverlapPreservesOwnerWithoutLifecycleActions(t *testing.T) {
	const source = "00000000-0000-4000-8000-000000000291"
	const sibling = "00000000-0000-4000-8000-000000000292"
	var leases []billingtypes.Lease
	acknowledged := make(chan string, 8)
	rejected := make(chan string, 8)
	chain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) { return leases, nil },
		RejectLeasesFunc: func(_ context.Context, ids []string, _ string) (uint64, []string, error) {
			for _, id := range ids {
				rejected <- id
			}
			return 1, []string{"tx"}, nil
		},
	}
	ack := &mockAcknowledger{acknowledgeFn: func(_ context.Context, id string) (bool, string, error) {
		acknowledged <- id
		return true, "tx", nil
	}}
	owner := &mockReconcilerBackend{name: "backend-a"}
	peer := &mockReconcilerBackend{name: "backend-b"}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: owner, IsDefault: true}, {Backend: peer},
	}})
	require.NoError(t, err)
	store, err := placementstore.NewStore(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	r, err := newTestReconciler(t, ReconcilerConfig{}, chain, ack, router, nil, store)
	require.NoError(t, err)
	require.NoError(t, r.ReconcileAll(t.Context()))
	leases = overlapPendingLeases(source, sibling)
	owner.provisions = []backend.ProvisionInfo{
		{
			LeaseUUID: source, Status: backend.ProvisionStatusReady,
			Tenant: "tenant-overlap", ProviderUUID: placementstore.ProviderUUID,
			LifecycleGeneration: &backend.LifecycleGenerationObservation{
				Kind: backend.LifecycleGenerationTyped, ID: "123e4567-e89b-42d3-a456-426614174000",
			},
		},
		{LeaseUUID: sibling, Status: backend.ProvisionStatusReady},
	}
	require.NoError(t, r.ReconcileAll(t.Context()))
	require.ElementsMatch(t, []string{source, sibling}, drainOverlapCalls(acknowledged))
	before := store.Lookup(source)
	owner.provisions[0].Status = backend.ProvisionStatusFailed
	owner.provisions[0].FailCount = 100
	owner.retentions = []backend.RetainedLease{{LeaseUUID: source}}
	require.NoError(t, r.ReconcileAll(t.Context()))
	assert.Equal(t, before, store.Lookup(source), "lifecycle overlap must preserve established owner affinity")
	assert.Equal(t, []string{sibling}, drainOverlapCalls(acknowledged))
	assert.Empty(t, drainOverlapCalls(rejected), "overlap cannot expose the stale Failed row")
	for _, client := range []*mockReconcilerBackend{owner, peer} {
		client.mu.Lock()
		assert.Empty(t, client.provisionCalls)
		assert.Empty(t, client.deprovisionCalls)
		client.mu.Unlock()
	}
}
