package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/inventory"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// The two inventory endpoints are independent reads. A close completing after
// the provision response legitimately puts the same lease in both responses.
// That transition must not quarantine an unrelated, unchanged exact owner.
func TestCollectedInventoryCloseTransitionPreservesSiblingAuthority(t *testing.T) {
	for _, interveningAttempt := range []bool{false, true} {
		name := "settled overlap isolates healthy sibling"
		if interveningAttempt {
			name = "newer attempt preserves unresolved evidence across restart"
		}
		t.Run(name, func(t *testing.T) {
			const (
				closingLease = "00000000-0000-4000-8000-000000000251"
				healthyLease = "00000000-0000-4000-8000-000000000252"
			)
			store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
			baseline := requireAdmissionBaseline(t, store, "backend-a")
			request, err := newBackendRequestSnapshot(
				"tenant-test", freshTestProviderUUID,
				[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
			)
			require.NoError(t, err)
			rows := make([]backend.ProvisionInfo, 0, 2)
			var healthyID operation.OperationID
			for index, leaseUUID := range []string{closingLease, healthyLease} {
				id := requireOperationID(t, []string{"2251", "2252"}[index])
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
					LeaseUUID: leaseUUID, BackendName: "backend-a",
					ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
					LifecycleGeneration: &backend.LifecycleGenerationObservation{
						Kind: backend.LifecycleGenerationTyped,
						ID:   store.CurrentLifecycle(leaseUUID).ID().String(),
					},
				})
				if leaseUUID == healthyLease {
					healthyID = id
				}
			}
			client := &unrecordedPositiveInventoryBackend{
				executionTestBackend: &executionTestBackend{name: "backend-a"},
				provisions:           rows, storageID: testBackendStorageID("backend-a"),
			}
			base, err := store.BindOperationCoordinator(nil)
			require.NoError(t, err)
			execution := bindExecutionForTest(t, base, newExecutionTestRuntime(client))
			coordinator, err := reconciliationCoordinatorWithReaderForTest(t, execution, &reconciliationSweepReader{})
			require.NoError(t, err)
			sweep, err := coordinator.BeginSweep()
			require.NoError(t, err)
			defer sweep.End()
			if interveningAttempt {
				id := requireOperationID(t, "2253")
				_, applied, beginErr := store.beginOwnedAttempt(
					baseline, store.Lookup(closingLease).RecordRevision(), "backend-a", id,
					PayloadFingerprint{}, request, testCallbackPair(id),
				)
				require.NoError(t, beginErr)
				require.True(t, applied)
			}
			provisions, err := sweep.CollectProvisionInventory(t.Context(), "backend-a")
			require.NoError(t, err)

			// Complete the close between the real endpoint reads. The retained response
			// is newer, while the sibling still has the exact same provision generation.
			client.provisions = rows[1:]
			client.retentions = []backend.RetainedLease{{LeaseUUID: closingLease}}
			retentions, err := sweep.CollectRetentionInventory(t.Context(), "backend-a")
			require.NoError(t, err)
			disposition, err := sweep.RecordBackendInventory(provisions, retentions)
			require.NoError(t, err)
			require.Equal(t, BackendInventoryPartial, disposition.Disposition())
			require.Equal(t, []string{closingLease}, disposition.UntrustedLeaseUUIDs())
			require.NoError(t, sweep.SealInventory())
			assert.False(t, sweep.InventoryComplete())

			projection := ReconciliationProjection{
				Placements:         map[string]string{healthyLease: "backend-a"},
				UntrustedPositives: make(map[string][]string),
			}
			for _, leaseUUID := range disposition.UntrustedLeaseUUIDs() {
				projection.UntrustedPositives[leaseUUID] = []string{"backend-a"}
			}
			projected, err := sweep.Project(projection)
			require.NoError(t, err)
			if interveningAttempt {
				assert.True(t, projected.fenced(closingLease))
				assert.NotEqual(t, StateUnusable, store.Lookup(closingLease).State(),
					"old inventory cannot quarantine a newer attempt")
				assert.True(t, coordinator.AbsenceUntrusted(closingLease))
				assert.True(t, store.inventoryRecoveryRequired)
				assert.NotZero(t, store.pendingInventorySweepID)
				_, closeErr := store.placementForDeprovision(closingLease)
				assert.ErrorIs(t, closeErr, ErrUnprojectedInventoryPositive)
				assert.False(t, store.CurrentAdmissionBaseline().Valid(),
					"unrepresented ambiguity must not clear the global recovery marker")
				sweep.End()
				dbPath := store.db.Path()
				require.NoError(t, store.Close())
				reopened, openErr := OpenStore(dbPath, freshTestProviderUUID,
					WithCallbackRouteFactory(testCallbackRoutes(t)))
				require.NoError(t, openErr)
				t.Cleanup(func() { require.NoError(t, reopened.Close()) })
				assert.True(t, reopened.inventoryRecoveryRequired)
				assert.NotZero(t, reopened.pendingInventorySweepID)
				_, closeErr = reopened.placementForDeprovision(closingLease)
				assert.ErrorIs(t, closeErr, ErrUnprojectedInventoryPositive)
				return
			}
			assert.True(t, store.CurrentAdmissionBaseline().Valid(),
				"represented lease-local ambiguity must preserve the established baseline")
			assert.False(t, store.inventoryRecoveryRequired)

			assert.Equal(t, StateUnusable, store.Lookup(closingLease).State(),
				"the overlapping lease must retain conservative positive membership")
			assert.Equal(t, StateConfirmed, store.Lookup(healthyLease).State(),
				"a normal close on one lease must not quarantine the whole backend")
			claim, claimed, err := store.claimAttempt(healthyLease, healthyID)
			assert.NoError(t, err)
			assert.True(t, claimed, "the unchanged exact generation must still admit callback settlement")
			if claimed {
				store.releaseAttemptClaim(claim)
			}
			placement, err := store.placementForDeprovision(healthyLease)
			assert.NoError(t, err)
			assert.Equal(t, StateConfirmed, placement.State(),
				"an unrelated close must retain its confirmed owner")
		})
	}
}

func TestCollectedInventoryMalformedBackendStillRejectsEveryPositive(t *testing.T) {
	for _, failure := range []string{"changed storage", "duplicate provision", "duplicate retention", "wrong backend"} {
		t.Run(failure, func(t *testing.T) {
			rows := []backend.ProvisionInfo{
				{LeaseUUID: "healthy", BackendName: "backend-a"},
				{LeaseUUID: "closing", BackendName: "backend-a"},
			}
			retained := []backend.RetainedLease{{LeaseUUID: "closing"}}
			if failure == "duplicate provision" {
				rows = append(rows, rows[0])
			}
			if failure == "duplicate retention" {
				retained = append(retained, retained[0])
			}
			_, _, sweep := newCollectedInventorySweep(t, rows, retained)
			defer sweep.End()
			provisions, err := sweep.CollectProvisionInventory(t.Context(), "backend-a")
			require.NoError(t, err)
			retentions, err := sweep.CollectRetentionInventory(t.Context(), "backend-a")
			require.NoError(t, err)
			if failure == "wrong backend" {
				canonical := sweep.pendingProvisions[provisions.receipt]
				canonical.provisions[0].BackendName = "backend-b"
				sweep.pendingProvisions[provisions.receipt] = canonical
			}
			if failure == "changed storage" {
				// Change the canonical receipt, not its detached public DTO.
				canonical := sweep.pendingRetentions[retentions.receipt]
				canonical.storageID = testBackendStorageID("backend-b")
				sweep.pendingRetentions[retentions.receipt] = canonical
			}
			result, err := sweep.RecordBackendInventory(provisions, retentions)
			require.NoError(t, err)
			assert.Equal(t, BackendInventoryUntrusted, result.Disposition())
			require.NoError(t, sweep.SealInventory())
			binding := sweep.coordinator.projector.collector.Binding()
			assert.Empty(t, sweep.sealed.StorageIdentities(binding))
			assert.True(t, sweep.sealed.UntrustedReporter(binding, "backend-a", "healthy"))
			assert.True(t, sweep.sealed.UntrustedReporter(binding, "backend-a", "closing"))
			assert.False(t, sweep.sealed.Complete(binding))
			_, err = sweep.collection.Seal()
			assert.ErrorIs(t, err, inventory.ErrInvalidSession)
		})
	}
}
