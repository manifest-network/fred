package placement

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestPairedOverlapRecoversInheritedInventoryMarker(t *testing.T) {
	for _, process := range []string{"continued", "reopened"} {
		for _, peer := range []string{"paired", "missing retention", "changed identity"} {
			t.Run(process+"/"+peer, func(t *testing.T) {
				const (
					previous = "00000000-0000-4000-8000-000000000281"
					overlap  = "00000000-0000-4000-8000-000000000282"
				)
				path := filepath.Join(t.TempDir(), "placements.db")
				routes := testCallbackRoutes(t)
				store, err := newStoreForTest(path, WithCallbackRouteFactory(routes))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, store.Close()) })
				requireAdmissionBaseline(t, store, "backend-a", "backend-b")
				requireConfirmedPlacement(t, store, previous, "backend-a")
				row := func(id string) backend.ProvisionInfo {
					return backend.ProvisionInfo{LeaseUUID: id, BackendName: "backend-a"}
				}
				backendA := &unrecordedPositiveInventoryBackend{
					executionTestBackend: &executionTestBackend{name: "backend-a"},
					provisions:           []backend.ProvisionInfo{row(previous)},
					storageID:            testBackendStorageID("backend-a"),
				}
				backendB := &unrecordedPositiveInventoryBackend{
					executionTestBackend: &executionTestBackend{name: "backend-b"},
					storageID:            testBackendStorageID("backend-b"),
				}
				bind := func() *ReconciliationCoordinator {
					t.Helper()
					base, bindErr := store.BindOperationCoordinator(nil)
					require.NoError(t, bindErr)
					execution := bindExecutionForTest(t, base, newExecutionTestRuntime(backendA, backendB))
					coordinator, bindErr := reconciliationCoordinatorWithReaderForTest(t, execution, &reconciliationSweepReader{})
					require.NoError(t, bindErr)
					return coordinator
				}
				collect := func(sweep *ReconciliationSweep, missingPeerRetention bool) {
					t.Helper()
					for _, name := range []string{"backend-a", "backend-b"} {
						provisions, collectErr := sweep.CollectProvisionInventory(t.Context(), name)
						require.NoError(t, collectErr)
						if name == "backend-b" && missingPeerRetention {
							require.NoError(t, sweep.RejectProvisionInventory(provisions))
							continue
						}
						retentions, collectErr := sweep.CollectRetentionInventory(t.Context(), name)
						require.NoError(t, collectErr)
						_, collectErr = sweep.RecordBackendInventory(provisions, retentions)
						require.NoError(t, collectErr)
					}
					require.NoError(t, sweep.SealInventory())
				}
				coordinator := bind()
				interrupted, err := coordinator.BeginSweep()
				require.NoError(t, err)
				collect(interrupted, false)
				// A later durable mutation wins over the collected observation. Its
				// positive cannot be projected, so both the durable pending marker
				// and the live per-lease barrier must survive this sweep.
				requireDeleteRecord(t, store, previous)
				projected, err := interrupted.Project(ReconciliationProjection{
					Placements: map[string]string{previous: "backend-a"},
				})
				require.NoError(t, err)
				require.True(t, projected.fenced(previous))
				require.True(t, store.inventoryRecoveryRequired)
				require.NotEmpty(t, store.unprojectedPositives[previous])
				interrupted.End()
				if process == "reopened" {
					require.NoError(t, store.Close())
					store, err = OpenStore(path, freshTestProviderUUID, WithCallbackRouteFactory(routes))
					require.NoError(t, err)
					coordinator = bind()
				}
				require.True(t, store.inventoryRecoveryRequired)
				require.ErrorIs(t, store.leaseSideEffectError(previous), ErrUnprojectedInventoryPositive)

				// The old lease now has clear evidence while a different close
				// overlaps the endpoints. Every current positive is represented,
				// but the snapshot deliberately remains globally incomplete.
				backendA.provisions = []backend.ProvisionInfo{row(previous), row(overlap)}
				backendA.retentions = []backend.RetainedLease{{LeaseUUID: overlap}}
				if peer == "changed identity" {
					backendB.storageID = testBackendStorageID("replacement-b")
				}
				fresh, err := coordinator.BeginSweep()
				require.NoError(t, err)
				defer fresh.End()
				collect(fresh, peer == "missing retention")
				require.False(t, fresh.InventoryComplete())
				resolved, err := fresh.Project(ReconciliationProjection{
					Placements:         map[string]string{previous: "backend-a"},
					UntrustedPositives: map[string][]string{overlap: {"backend-a"}},
				})
				require.NoError(t, err)
				assert.False(t, resolved.Complete(), "coverage must not promote ambiguous evidence to global completeness")
				assert.Equal(t, StateUnusable, store.Lookup(overlap).State())
				assert.Equal(t, StateConfirmed, store.Lookup(previous).State())
				if peer != "paired" {
					assert.True(t, store.inventoryRecoveryRequired)
					assert.ErrorIs(t, store.leaseSideEffectError(previous), ErrUnprojectedInventoryPositive)
					assert.False(t, resolved.AdmissionBaseline().Valid())
					return
				}
				assert.False(t, store.inventoryRecoveryRequired,
					"a fresh paired topology can durably account for the old uncertainty without a globally quiet sweep")
				assert.Zero(t, store.pendingInventorySweepID)
				assert.Empty(t, store.unprojectedPositives[previous], "retiring only the global marker leaves a live lease fenced")
				assert.NoError(t, store.leaseSideEffectError(previous))
				owner, ownerErr := store.placementForDeprovision(previous)
				assert.NoError(t, ownerErr)
				assert.Equal(t, "backend-a", owner.Backend)
				assert.True(t, resolved.AdmissionBaseline().Valid(), "reuse the previously established baseline")
			})
		}
	}
}
