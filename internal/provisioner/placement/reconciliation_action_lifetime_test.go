package placement

import (
	"context"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestReleasedReconciliationActionCannotUseReplacementLeaseClaim(t *testing.T) {
	for _, state := range []billingtypes.LeaseState{billingtypes.LEASE_STATE_PENDING, billingtypes.LEASE_STATE_ACTIVE} {
		t.Run(state.String(), func(t *testing.T) {
			const (
				owner   = "00000000-0000-4000-8000-000000000293"
				sibling = "00000000-0000-4000-8000-000000000294"
			)
			store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
			requireAdmissionBaseline(t, store, "backend-a")
			requireConfirmedPlacement(t, store, owner, "backend-a")
			requireConfirmedPlacement(t, store, sibling, "backend-a")
			var provisions []backend.ProvisionRequest
			var deprovisions []string
			client := &overlapActionBackend{unrecordedPositiveInventoryBackend: &unrecordedPositiveInventoryBackend{
				executionTestBackend: &executionTestBackend{
					name: "backend-a",
					provision: func(_ context.Context, request backend.ProvisionRequest) error {
						provisions = append(provisions, request)
						return nil
					},
					deprovision: func(_ context.Context, leaseUUID string) error {
						deprovisions = append(deprovisions, leaseUUID)
						return nil
					},
				},
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
			lease := chain.leases[owner]
			lease.State = state
			chain.leases[owner] = lease
			base, err := store.BindOperationCoordinator(nil)
			require.NoError(t, err)
			execution := bindExecutionForTest(t, base, newExecutionTestRuntime(client))
			coordinator, err := reconciliationCoordinatorWithReaderForTest(t, execution, chain)
			require.NoError(t, err)
			sweep, projected := completeEmptyReconciliationSweep(t, coordinator)
			defer sweep.End()
			action, disposition, err := projected.ObserveLiveAction(t.Context(), owner)
			require.NoError(t, err)
			require.Equal(t, ReconciliationObservationReady, disposition)
			require.True(t, action.Valid())
			require.NoError(t, coordinator.ReconcileObservedCustomDomain(t.Context(), action))
			require.Equal(t, []string{owner}, client.customDomains, "the current held claim remains executable")
			client.customDomains = nil
			require.True(t, coordinator.ReleaseAction(action))
			assert.False(t, action.Valid(), "historical issuance is not a live lease claim")
			assert.Empty(t, action.Lease().Uuid)
			assert.Equal(t, Placement{}, action.Placement())

			replacement := coordinator.coordinator.operations.TryClaimLeaseNow(owner)
			require.True(t, replacement.Acquired())
			defer coordinator.coordinator.operations.ReleaseLease(replacement.Claim())
			assert.False(t, coordinator.ReleaseAction(action), "an old release cannot consume a newer claim")
			assert.True(t, projected.Valid(), "the sweep remains live; only this action's exclusion ended")
			assert.Error(t, coordinator.Provision(t.Context(), action, nil, PayloadFingerprint{}).Err())
			assert.Error(t, coordinator.DeprovisionObserved(t.Context(), action))
			assert.Error(t, coordinator.ReconcileObservedCustomDomain(t.Context(), action))
			_, _, err = coordinator.AcknowledgeObserved(t.Context(), action)
			assert.Error(t, err)
			_, _, err = coordinator.RejectObserved(t.Context(), action, "released action")
			assert.Error(t, err)
			_, _, err = coordinator.CloseObserved(t.Context(), action, "released action")
			assert.Error(t, err)
			assert.Empty(t, provisions)
			assert.Empty(t, deprovisions)
			assert.Empty(t, client.customDomains)
			assert.Empty(t, chain.acknowledged)
			assert.Empty(t, chain.rejected)
			assert.Empty(t, chain.closed)
			assert.True(t, coordinator.coordinator.operations.HoldsLeaseClaim(replacement.Claim(), owner))

			healthy, disposition, err := projected.ObserveLiveAction(t.Context(), sibling)
			require.NoError(t, err)
			require.Equal(t, ReconciliationObservationReady, disposition)
			require.True(t, healthy.Valid())
			require.NoError(t, coordinator.ReconcileObservedCustomDomain(t.Context(), healthy))
			acked, _, err := coordinator.AcknowledgeObserved(t.Context(), healthy)
			require.NoError(t, err)
			require.True(t, acked)
			assert.Equal(t, []string{sibling}, client.customDomains)
			assert.Equal(t, []string{sibling}, chain.acknowledged)
			require.True(t, coordinator.ReleaseAction(healthy))
		})
	}
}
