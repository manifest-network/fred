package restore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// The sweep predates both Registry claims. Its complete retention-only
// projection can change lifecycle metadata (and therefore placement revision)
// while preserving the source owner. Source reservation must start before the
// target chain read, not merely before the eventual backend call.
func TestServiceReservesSourceBeforeTargetAuthorization(t *testing.T) {
	for _, generation := range []backend.LifecycleGenerationKind{
		backend.LifecycleGenerationLegacy, backend.LifecycleGenerationTyped,
	} {
		t.Run(string(generation), func(t *testing.T) {
			const sourceUUID = "00000000-0000-4000-8000-000000002310"
			const targetUUID = "00000000-0000-4000-8000-000000002311"
			const siblingUUID = "00000000-0000-4000-8000-000000002312"
			fixture := newOverlapRestoreFixture(t, sourceUUID, targetUUID)
			row := backend.ProvisionInfo{
				LeaseUUID: sourceUUID, BackendName: testBackend, Tenant: testTenant, ProviderUUID: testProvider,
				LifecycleGeneration: &backend.LifecycleGenerationObservation{Kind: generation},
			}
			if generation == backend.LifecycleGenerationTyped {
				row.LifecycleGeneration.ID = testOperationID(t, 2310).String()
			}
			fixture.backend.setInventory([]backend.ProvisionInfo{row}, nil)
			seed := collectRestoreReservationSweep(t, fixture)
			_, err := seed.Project(placement.ReconciliationProjection{Placements: map[string]string{sourceUUID: testBackend}})
			require.NoError(t, err)
			seed.End()
			before := fixture.store.Lookup(sourceUUID)
			lifecycleBefore := fixture.store.CurrentLifecycle(sourceUUID)

			fixture.backend.setInventory(nil, []backend.RetainedLease{{LeaseUUID: sourceUUID}})
			peer := fixture.backends["backend-b"].(*fakeBackend)
			peer.setInventory([]backend.ProvisionInfo{{
				LeaseUUID: siblingUUID, BackendName: "backend-b", Tenant: testTenant, ProviderUUID: testProvider,
				LifecycleGeneration: &backend.LifecycleGenerationObservation{Kind: backend.LifecycleGenerationTyped, ID: testOperationID(t, 2312).String()},
			}}, nil)
			sweep := collectRestoreReservationSweep(t, fixture)
			fixture.targets.hook = func(leaseUUID string) {
				if leaseUUID != targetUUID {
					return
				}
				_, projectErr := sweep.Project(placement.ReconciliationProjection{Placements: map[string]string{
					sourceUUID: testBackend, siblingUUID: "backend-b",
				}})
				require.NoError(t, projectErr)
				sweep.End()
				assert.Equal(t, before, fixture.store.Lookup(sourceUUID), "authorization must hold the exact source revision")
				assert.Equal(t, lifecycleBefore, fixture.store.CurrentLifecycle(sourceUUID))
				assert.Equal(t, placement.StateConfirmed, fixture.store.Lookup(siblingUUID).State(), "the reserved source must not block a healthy sibling")
			}
			result := fixture.service.Execute(t.Context(), Command{
				TargetLeaseUUID: targetUUID, Tenant: testTenant, SourceLeaseUUID: sourceUUID,
			})
			require.Equal(t, OutcomeAccepted, result.Outcome, result.Cause())
			require.Equal(t, 1, fixture.backend.callCount())
			require.Equal(t, sourceUUID, fixture.backend.lastRequest().FromLeaseUUID)
			require.Equal(t, testBackend, fixture.store.Lookup(targetUUID).Backend)
			requireLeaseClaimsReleased(t, fixture.runtime, sourceUUID)
			require.True(t, fixture.runtime.Contains(targetUUID), "accepted dispatch retains its exact pending target operation")
		})
	}
}

func TestServiceSourceReservationDoesNotOverrideContradictoryInventory(t *testing.T) {
	fixture := newFixture(t, true)
	before := fixture.store.Lookup(testSource)
	fixture.backend.setInventory(nil, []backend.RetainedLease{{LeaseUUID: testSource}})
	fixture.backends["backend-b"].(*fakeBackend).setInventory(nil, []backend.RetainedLease{{LeaseUUID: testSource}})
	sweep := collectRestoreReservationSweep(t, fixture)
	fixture.targets.hook = func(leaseUUID string) {
		if leaseUUID == testTarget {
			_, err := sweep.Project(placement.ReconciliationProjection{
				Conflicts: map[string][]string{testSource: {testBackend, "backend-b"}},
			})
			require.NoError(t, err)
			sweep.End()
		}
	}
	result := fixture.service.Execute(t.Context(), validCommand())
	require.Equal(t, OutcomeServiceUnavailable, result.Outcome)
	require.ErrorIs(t, result.Cause(), placement.ErrInvalidAdmissionBaseline,
		"the unresolved conflicting positive must keep inventory recovery admission closed")
	require.Equal(t, before, fixture.store.Lookup(testSource))
	require.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
	require.Zero(t, fixture.backend.callCount())
	requireLeaseClaimsReleased(t, fixture.runtime, testSource, testTarget)
}

func TestServiceReleasesSourceReservationAfterTargetReadCancellation(t *testing.T) {
	fixture := newFixture(t, true)
	fixture.targets.errs = map[string]error{testTarget: context.Canceled}
	result := fixture.service.Execute(t.Context(), validCommand())
	require.Equal(t, OutcomeServiceUnavailable, result.Outcome)
	require.ErrorIs(t, result.Cause(), context.Canceled)
	require.Equal(t, placement.StateAbsent, fixture.store.Lookup(testTarget).State())
	require.False(t, fixture.runtime.Contains(testTarget))
	requireLeaseClaimsReleased(t, fixture.runtime, testSource, testTarget)
	fixture.targets.errs = nil
	result = fixture.service.Execute(t.Context(), validCommand())
	require.Equal(t, OutcomeAccepted, result.Outcome, "target refusal must release the exact early source reservation")
	require.Equal(t, 1, fixture.backend.callCount())
}

func collectRestoreReservationSweep(t *testing.T, fixture *fixture) *placement.ReconciliationSweep {
	t.Helper()
	sweep, err := fixture.reconciliation.BeginSweep()
	require.NoError(t, err)
	t.Cleanup(sweep.End)
	for _, name := range []string{testBackend, "backend-b"} {
		provisions, collectErr := sweep.CollectProvisionInventory(t.Context(), name)
		require.NoError(t, collectErr)
		retentions, collectErr := sweep.CollectRetentionInventory(t.Context(), name)
		require.NoError(t, collectErr)
		_, collectErr = sweep.RecordBackendInventory(provisions, retentions)
		require.NoError(t, collectErr)
	}
	require.NoError(t, sweep.SealInventory())
	return sweep
}
