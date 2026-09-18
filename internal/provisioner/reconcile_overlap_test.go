package provisioner

import (
	"context"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

func TestReconcilerRotatingOverlapRecoversCleanLease(t *testing.T) {
	for _, recoveredEndpoint := range []string{"provision", "retention"} {
		t.Run(recoveredEndpoint, func(t *testing.T) {
			const first = "00000000-0000-4000-8000-000000000261"
			const second = "00000000-0000-4000-8000-000000000262"
			store, err := placementstore.NewStore(t.TempDir() + "/placements.db")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			client := &mockReconcilerBackend{name: "backend-a"}
			router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
				{Backend: client, IsDefault: true},
			}})
			require.NoError(t, err)
			chain := &chaintest.MockClient{GetLeaseFunc: func(_ context.Context, id string) (*billingtypes.Lease, error) {
				return &billingtypes.Lease{Uuid: id, State: billingtypes.LEASE_STATE_ACTIVE}, nil
			}}
			r, err := newTestReconciler(t, ReconcilerConfig{}, chain, noopAck, router, nil, store)
			require.NoError(t, err)
			projectTestPlacementInventory(t, r.coordinator, []string{"backend-a"},
				placement.ReconciliationProjection{Placements: map[string]string{
					first: "backend-a", second: "backend-a",
				}})
			row := func(id string) backend.ProvisionInfo {
				return backend.ProvisionInfo{LeaseUUID: id, BackendName: "backend-a"}
			}
			project := func() {
				t.Helper()
				sweep, beginErr := r.coordinator.BeginSweep()
				require.NoError(t, beginErr)
				defer sweep.End()
				collected, collectErr := r.collectInventory(t.Context(), sweep, nil, nil)
				require.NoError(t, collectErr)
				require.False(t, sweep.InventoryComplete(), "each sweep intentionally has one ambiguous lease")
				result, projectErr := r.projectPlacementInventory(t.Context(), reconcileProjectionInput{
					inventory: collected, sweep: sweep,
				})
				require.NoError(t, projectErr)
				require.True(t, result.syncOK)
			}
			client.provisions = []backend.ProvisionInfo{row(first), row(second)}
			client.retentions = []backend.RetainedLease{{LeaseUUID: first}}
			project()
			require.Equal(t, placement.StateUnusable, store.Lookup(first).State())
			require.Equal(t, placement.StateConfirmed, store.Lookup(second).State())

			// The old ambiguity clears while a different lease closes. The fleet
			// never has a globally clear sweep, but first has complete evidence.
			client.retentions = []backend.RetainedLease{{LeaseUUID: second}}
			if recoveredEndpoint == "retention" {
				client.provisions = []backend.ProvisionInfo{row(second)}
				client.retentions = append(client.retentions, backend.RetainedLease{LeaseUUID: first})
			}
			project()
			assert.Equal(t, placement.StateConfirmed, store.Lookup(first).State(),
				"unrelated continuous close traffic must not strand a now-clean sole owner")
			assert.Equal(t, placement.StateUnusable, store.Lookup(second).State())
			assert.True(t, store.CurrentAdmissionBaseline().Valid())
		})
	}
}
