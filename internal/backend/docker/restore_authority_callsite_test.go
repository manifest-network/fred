package docker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// The cross-journal relation must remain enforced at both operation-recovery
// entry points, including after the exact active Release has already committed.
// The fixtures issue real claims and Releases; only the stopped retention row
// is changed to model an independently valid but contradictory journal.
func TestRestoreOperationRecoveryCallsitesValidateSemanticAuthority(t *testing.T) {
	for _, caller := range []struct {
		name             string
		committed        bool
		currentSubstrate bool
	}{
		{name: "source without destination"},
		{name: "source with destination", currentSubstrate: true},
		{name: "committed release", committed: true},
	} {
		t.Run(caller.name, func(t *testing.T) {
			for _, test := range []struct {
				name       string
				mutateSpec func(*shared.OperationIntentSpec)
				finalizer  func(*shared.RetentionEntry)
				wantErr    string
			}{
				{name: "matching"},
				{
					name: "health-check authority differs",
					mutateSpec: func(spec *shared.OperationIntentSpec) {
						spec.HealthCheckServices = []string{"app"}
					},
					wantErr: "health-check authority differs",
				},
				{
					name: "only effective items differ",
					mutateSpec: func(spec *shared.OperationIntentSpec) {
						// A valid DNS deferral leaves the effective domain empty.
						spec.Items[0].CustomDomain = "desired.example"
					},
					finalizer: func(entry *shared.RetentionEntry) {
						entry.DestinationItems[0].CustomDomain = "desired.example"
					},
					wantErr: "topology or resource profiles differ",
				},
			} {
				t.Run(test.name, func(t *testing.T) {
					b, _, calls := newRestoreAuthorityRecoveryFixture(t, test.mutateSpec)
					claims, err := b.operationSettlement.ListOperationIntents()
					require.NoError(t, err)
					require.Len(t, claims, 1)
					claim := claims[0]
					if caller.committed {
						commitOperationReleaseWithoutPublishingTest(t, b.operationSettlement, claim)
						active, readErr := b.releaseStore.LatestActive(claim.LeaseUUID())
						require.NoError(t, readErr)
						matches, matchErr := operationReleaseMatchesIntent(active, claim)
						require.NoError(t, matchErr)
						require.True(t, matches, "the call must reach finalizer validation after the exact Release check")
					}
					if test.finalizer != nil {
						require.NoError(t, b.retentionStore.Close())
						mutateStoppedRestoreFinalizer(t, b.cfg.RetentionDBPath, test.finalizer)
						retentions, openErr := shared.OpenIdentityBoundRetentionStore(
							shared.RetentionStoreConfig{DBPath: b.cfg.RetentionDBPath},
							b.storageAuthority, b.storeAuthorityGate,
						)
						require.NoError(t, openErr, "the mismatch is semantic, not an invalid retention encoding")
						b.retentionStore = retentions
						t.Cleanup(func() { require.NoError(t, retentions.Close()) })
						entry, readErr := retentions.Get(claim.SourceLeaseUUID())
						require.NoError(t, readErr)
						require.NotNil(t, entry)
						require.Equal(t, claim.Items(), entry.DestinationItems)
						require.NotEqual(t, claim.EffectiveItems(), entry.DestinationItems,
							"only the effective-items comparison can reject this topology")
					}
					before := restoreAuthorityJournalBytes(t, b)
					beforeAllocation := b.pool.GetAllocation(restoreAuthorityDestination + "-app-0")
					require.NotNil(t, beforeAllocation)
					beforeProjection := recoveredFromProvision(b.provisions[restoreAuthorityDestination])
					if caller.committed {
						var committed bool
						committed, err = b.operationIntentHasCommittedRelease(claim)
						assert.Equal(t, test.wantErr == "", committed)
					} else {
						err = b.validateRestoreIntentSource(claim, caller.currentSubstrate)
					}
					if test.wantErr == "" {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, test.wantErr)
					}
					assert.Equal(t, before, restoreAuthorityJournalBytes(t, b))
					assert.Equal(t, *beforeAllocation, *b.pool.GetAllocation(restoreAuthorityDestination + "-app-0"))
					assert.True(t, provisionMatchesRecovered(b.provisions[restoreAuthorityDestination], beforeProjection))
					assert.Equal(t, restoreAuthoritySubstrateCalls{}, *calls)
				})
			}
		})
	}
}
