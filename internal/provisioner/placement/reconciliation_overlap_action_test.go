package placement

import (
	"context"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

type overlapActionBackend struct {
	*unrecordedPositiveInventoryBackend
	customDomains []string
}

func (client *overlapActionBackend) ReconcileCustomDomain(_ context.Context, leaseUUID string, _ []backend.LeaseItem) error {
	client.customDomains = append(client.customDomains, leaseUUID)
	return nil
}

type overlapActionChain struct {
	*reconciliationSweepReader
	leases       map[string]billingtypes.Lease
	acknowledged []string
}

func (chain *overlapActionChain) GetLease(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	chain.mu.Lock()
	defer chain.mu.Unlock()
	chain.exactReads++
	lease, found := chain.leases[leaseUUID]
	if !found {
		return nil, nil
	}
	copy := cloneReconciliationLease(&lease)
	return &copy, nil
}

func (chain *overlapActionChain) Acknowledge(_ context.Context, leaseUUID string) (bool, string, error) {
	chain.mu.Lock()
	defer chain.mu.Unlock()
	chain.acknowledged = append(chain.acknowledged, leaseUUID)
	return true, "tx-ack", nil
}

func TestPreservedPairedOverlapCannotIssueReconciliationActions(t *testing.T) {
	for _, state := range []billingtypes.LeaseState{billingtypes.LEASE_STATE_PENDING, billingtypes.LEASE_STATE_ACTIVE} {
		t.Run(state.String(), func(t *testing.T) {
			const (
				owner   = "00000000-0000-4000-8000-000000000291"
				sibling = "00000000-0000-4000-8000-000000000292"
			)
			store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
			baseline := requireAdmissionBaseline(t, store, "backend-a")
			request, err := newBackendRequestSnapshot("tenant-test", freshTestProviderUUID,
				[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}})
			require.NoError(t, err)
			var rows []backend.ProvisionInfo
			for index, leaseUUID := range []string{owner, sibling} {
				id := requireOperationID(t, []string{"2295", "2296"}[index])
				attempt, applied, beginErr := store.beginNewAttempt(
					requireAdmissionScope(t, store, baseline, "backend-a"), leaseUUID,
					"backend-a", id, PayloadFingerprint{}, request, testCallbackPair(id),
				)
				require.NoError(t, beginErr)
				require.True(t, applied)
				confirmed, confirmErr := confirmAttemptForTest(store, attempt)
				require.NoError(t, confirmErr)
				require.True(t, confirmed)
				rows = append(rows, backend.ProvisionInfo{
					LeaseUUID: leaseUUID, BackendName: "backend-a", Tenant: "tenant-test",
					ProviderUUID: freshTestProviderUUID, Status: backend.ProvisionStatusReady,
					LifecycleGeneration: &backend.LifecycleGenerationObservation{
						Kind: backend.LifecycleGenerationTyped,
						ID:   store.CurrentLifecycle(leaseUUID).ID().String(),
					},
				})
			}
			var provisions []backend.ProvisionRequest
			var deprovisions []string
			client := &overlapActionBackend{unrecordedPositiveInventoryBackend: &unrecordedPositiveInventoryBackend{
				executionTestBackend: &executionTestBackend{
					name: "backend-a",
					provision: func(_ context.Context, req backend.ProvisionRequest) error {
						provisions = append(provisions, req)
						return nil
					},
					deprovision: func(_ context.Context, leaseUUID string) error {
						deprovisions = append(deprovisions, leaseUUID)
						return nil
					},
				},
				provisions: rows, retentions: []backend.RetainedLease{{LeaseUUID: owner}},
				storageID: testBackendStorageID("backend-a"),
			}}
			chain := &overlapActionChain{
				reconciliationSweepReader: &reconciliationSweepReader{},
				leases:                    map[string]billingtypes.Lease{},
			}
			for _, leaseUUID := range []string{owner, sibling} {
				chain.leases[leaseUUID] = billingtypes.Lease{
					Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
					State: billingtypes.LEASE_STATE_PENDING,
					Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1, ServiceName: "app"}},
				}
			}
			ownerLease := chain.leases[owner]
			ownerLease.State = state
			chain.leases[owner] = ownerLease
			base, err := store.BindOperationCoordinator(nil)
			require.NoError(t, err)
			execution := bindExecutionForTest(t, base, newExecutionTestRuntime(client))
			coordinator, err := reconciliationCoordinatorWithReaderForTest(t, execution, chain)
			require.NoError(t, err)

			collect := func(overlap bool) (*ReconciliationSweep, *ProjectedReconciliationSweep) {
				t.Helper()
				sweep, beginErr := coordinator.BeginSweep()
				require.NoError(t, beginErr)
				t.Cleanup(sweep.End)
				provisionRead, readErr := sweep.CollectProvisionInventory(t.Context(), "backend-a")
				require.NoError(t, readErr)
				retentionRead, readErr := sweep.CollectRetentionInventory(t.Context(), "backend-a")
				require.NoError(t, readErr)
				result, recordErr := sweep.RecordBackendInventory(provisionRead, retentionRead)
				require.NoError(t, recordErr)
				projection := ReconciliationProjection{Placements: map[string]string{sibling: "backend-a"}}
				if overlap {
					require.Equal(t, BackendInventoryPartial, result.Disposition())
					projection.UntrustedPositives = map[string][]string{owner: {"backend-a"}}
				} else {
					require.Equal(t, BackendInventoryAuthoritative, result.Disposition())
					projection.Placements[owner] = "backend-a"
				}
				require.NoError(t, sweep.SealInventory())
				projected, projectErr := sweep.Project(projection)
				require.NoError(t, projectErr)
				return sweep, projected
			}
			before, beforeLifecycle := store.Lookup(owner), store.CurrentLifecycle(owner)
			sweep, projected := collect(true)
			require.Equal(t, before, store.Lookup(owner), "redundant overlap preserves exact restore affinity")
			require.Equal(t, beforeLifecycle, store.CurrentLifecycle(owner))
			require.False(t, coordinator.AbsenceUntrusted(owner), "this test exercises preserved authority, not an unresolved barrier")
			action, disposition, err := projected.ObserveLiveAction(t.Context(), owner)
			require.NoError(t, err)
			require.Equal(t, ReconciliationObservationStale, disposition)
			require.False(t, action.Valid(), "overlap cannot authorize any runtime action")
			require.Zero(t, chain.exactReads, "the sealed membership must reject construction before another chain read")
			assert.ErrorIs(t, coordinator.Provision(t.Context(), action, nil, PayloadFingerprint{}).Err(), ErrReconciliationBoundaryStale)
			assert.Error(t, coordinator.DeprovisionObserved(t.Context(), action))
			assert.Error(t, coordinator.ReconcileObservedCustomDomain(t.Context(), action))
			_, _, err = coordinator.AcknowledgeObserved(t.Context(), action)
			assert.Error(t, err)
			_, _, err = coordinator.RejectObserved(t.Context(), action, "ambiguous inventory")
			assert.Error(t, err)
			_, _, err = coordinator.CloseObserved(t.Context(), action, "ambiguous inventory")
			assert.Error(t, err)
			assert.False(t, coordinator.ReleaseAction(action))
			assert.Empty(t, provisions)
			assert.Empty(t, deprovisions)
			assert.Empty(t, client.customDomains)
			assert.Empty(t, chain.acknowledged)
			assert.Empty(t, chain.rejected)
			assert.Empty(t, chain.closed)
			assert.Equal(t, before, store.Lookup(owner))

			healthy, disposition, err := projected.ObserveLiveAction(t.Context(), sibling)
			require.NoError(t, err)
			require.Equal(t, ReconciliationObservationReady, disposition)
			acked, _, err := coordinator.AcknowledgeObserved(t.Context(), healthy)
			require.NoError(t, err)
			require.True(t, acked)
			require.Equal(t, []string{sibling}, chain.acknowledged)
			require.True(t, coordinator.ReleaseAction(healthy))
			sweep.End()

			client.retentions = nil
			cleanSweep, clean := collect(false)
			defer cleanSweep.End()
			action, disposition, err = clean.ObserveLiveAction(t.Context(), owner)
			require.NoError(t, err)
			require.Equal(t, ReconciliationObservationReady, disposition)
			defer coordinator.ReleaseAction(action)
			result := coordinator.Provision(t.Context(), action, nil, PayloadFingerprint{})
			require.NoError(t, result.Err())
			require.Len(t, provisions, 1, "a fresh clear observation can recover normal owned provisioning")
			assert.Equal(t, owner, provisions[0].LeaseUUID)
			assert.Equal(t, "backend-a", result.BackendName())
		})
	}
}
