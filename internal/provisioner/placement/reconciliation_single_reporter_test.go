package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestSingleReporterResolutionPreservesAuthorityFences(t *testing.T) {
	for _, scenario := range []string{
		"known retention owner", "retention preserves attempt", "unknown retention owner",
		"captured operation ended", "unbound peer identity", "historical multiple owners",
	} {
		t.Run(scenario, func(t *testing.T) {
			fixture := newFencedAvailabilityFixture(t)
			leaseUUID := fencedAvailabilityOwner
			if scenario == "unknown retention owner" {
				leaseUUID = fencedAvailabilitySibling
			}
			attemptID := requireOperationID(t, "2263")
			if scenario == "retention preserves attempt" {
				request, err := newBackendRequestSnapshot("tenant-test", freshTestProviderUUID,
					[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}})
				require.NoError(t, err)
				_, applied, err := fixture.store.beginOwnedAttempt(fixture.baseline,
					fixture.store.Lookup(leaseUUID).RecordRevision(), "backend-a", attemptID,
					PayloadFingerprint{}, request, testCallbackPair(attemptID))
				require.NoError(t, err)
				require.True(t, applied)
			}
			seed, err := fixture.coordinator.BeginSweep()
			require.NoError(t, err)
			for _, name := range []string{"backend-a", "backend-b", "backend-c"} {
				require.NoError(t, seed.RecordProvision(name, testBackendStorageID(name), nil))
				require.NoError(t, seed.RecordRetention(name, testBackendStorageID(name), nil))
			}
			require.NoError(t, seed.RecordUntrusted("backend-a", []string{leaseUUID}))
			projection := ReconciliationProjection{UntrustedPositives: map[string][]string{
				leaseUUID: {"backend-a"},
			}}
			if scenario == "historical multiple owners" {
				require.NoError(t, seed.RecordUntrusted("backend-b", []string{leaseUUID}))
				projection = ReconciliationProjection{Conflicts: map[string][]string{
					leaseUUID: {"backend-a", "backend-b"},
				}}
			}
			require.NoError(t, seed.SealInventory())
			_, err = seed.Project(projection)
			require.NoError(t, err)
			seed.End()
			require.Equal(t, StateUnusable, fixture.store.Lookup(leaseUUID).State())

			// Capture a real Registry claim, then release it without mutating Store.
			// A direct Project caller cannot omit this causal boundary.
			var releaseClaim func()
			if scenario == "captured operation ended" {
				claim := fixture.coordinator.coordinator.operations.TryClaimLeaseNow(leaseUUID)
				require.True(t, claim.Acquired())
				releaseClaim = func() {
					require.True(t, fixture.coordinator.coordinator.operations.ReleaseLease(claim.Claim()))
				}
			}
			sweep, err := fixture.coordinator.BeginSweep()
			require.NoError(t, err)
			defer sweep.End()
			if releaseClaim != nil {
				releaseClaim()
				require.True(t, sweep.WasInFlight(leaseUUID))
			}
			const rotating = "00000000-0000-4000-8000-000000000263"
			for _, name := range []string{"backend-a", "backend-b", "backend-c"} {
				var rows []backend.ProvisionInfo
				var retained []string
				if name == "backend-a" {
					rows = []backend.ProvisionInfo{{LeaseUUID: rotating, BackendName: name}}
					retained = []string{rotating, leaseUUID}
				}
				require.NoError(t, sweep.RecordProvision(name, testBackendStorageID(name), rows))
				require.NoError(t, sweep.RecordRetention(name, testBackendStorageID(name), retained))
			}
			require.NoError(t, sweep.SealInventory())
			if scenario == "unbound peer identity" {
				// Exercise the Store consumer rather than relying on receipt filtering.
				fixture.store.mu.Lock()
				delete(fixture.store.backendStorageIDs, "backend-b")
				fixture.store.mu.Unlock()
			}
			result, err := sweep.Project(ReconciliationProjection{
				Placements:         map[string]string{leaseUUID: "backend-a"},
				UntrustedPositives: map[string][]string{rotating: {"backend-a"}},
			})
			if scenario == "unbound peer identity" {
				assert.ErrorIs(t, err, ErrBackendStorageIdentityUnbound)
				assert.Equal(t, StateUnusable, fixture.store.Lookup(leaseUUID).State())
				return
			}
			require.NoError(t, err)
			switch scenario {
			case "known retention owner", "retention preserves attempt":
				assert.Equal(t, StateConfirmed, fixture.store.Lookup(leaseUUID).State())
				assert.False(t, fixture.store.CurrentLifecycle(leaseUUID).ID().Valid(),
					"retention affinity cannot reactivate runtime lifecycle authority")
				if scenario == "retention preserves attempt" {
					assert.Equal(t, "backend-a", fixture.store.Lookup(leaseUUID).Attempt)
					claim, claimed, claimErr := fixture.store.claimAttempt(leaseUUID, attemptID)
					require.NoError(t, claimErr)
					require.True(t, claimed, "the exact unresolved attempt must still be settleable")
					fixture.store.releaseAttemptClaim(claim)
				}
			default:
				assert.Equal(t, StateUnusable, fixture.store.Lookup(leaseUUID).State())
				if scenario == "captured operation ended" {
					assert.True(t, result.fenced(leaseUUID))
					assert.True(t, fixture.store.Lookup(leaseUUID).CanResolveUntrustedPositive("backend-a"),
						"deferral must not turn a repairable observation into an operator-only conflict")
				}
			}
		})
	}
}
