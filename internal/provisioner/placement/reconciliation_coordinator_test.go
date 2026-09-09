package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

// A rejected peer positive and an overlapping placement mutation leave both
// reporters outside the durable projection. Successful exact settlement then
// restores the real owner, but cannot consume the separate inventory evidence.
func newMultiReporterExclusionFixture(t *testing.T, settleAttempt bool) fencedAvailabilityFixture {
	t.Helper()
	fixture := newFencedAvailabilityFixture(t)
	sweep := beginFencedAvailabilitySweep(t, fixture)
	defer sweep.End()
	for _, name := range []string{"backend-a", "backend-b", "backend-c"} {
		var provisions []backend.ProvisionInfo
		if name == "backend-a" {
			provisions = []backend.ProvisionInfo{excludedReporterProvision(fixture, name)}
		}
		storageID := testBackendStorageID(name)
		require.NoError(t, sweep.RecordProvision(name, storageID, provisions))
		require.NoError(t, sweep.RecordRetention(name, storageID, nil))
	}
	require.NoError(t, sweep.RecordUntrusted("backend-b", []string{fencedAvailabilityOwner}))
	require.NoError(t, sweep.SealInventory())
	projected, err := sweep.Project(ReconciliationProjection{
		Conflicts: map[string][]string{fencedAvailabilityOwner: {"backend-a", "backend-b"}},
	})
	require.NoError(t, err)
	require.True(t, projected.fenced(fencedAvailabilityOwner))
	require.True(t, fixture.coordinator.AbsenceUntrusted(fencedAvailabilityOwner))
	require.Len(t, fixture.coordinator.absenceUntrusted[fencedAvailabilityOwner], 2)
	require.False(t, fixture.store.Lookup(fencedAvailabilityOwner).Conflict,
		"the newer attempt fenced this observation before it could quarantine the owner")
	sweep.End()

	if !settleAttempt {
		return fixture
	}
	confirmed, err := confirmOperationForTest(
		fixture.store, fencedAvailabilityOwner, "backend-a", requireOperationID(t, "2112"),
	)
	require.NoError(t, err)
	require.True(t, confirmed)
	fixture.lifecycleID = fixture.store.CurrentLifecycle(fencedAvailabilityOwner).ID()
	require.True(t, fixture.lifecycleID.Valid())
	return fixture
}

func excludedReporterProvision(fixture fencedAvailabilityFixture, name string) backend.ProvisionInfo {
	return backend.ProvisionInfo{
		LeaseUUID: fencedAvailabilityOwner, BackendName: name,
		ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
		LifecycleGeneration: &backend.LifecycleGenerationObservation{
			Kind: backend.LifecycleGenerationTyped, ID: fixture.lifecycleID.String(),
		},
	}
}

func TestReconciliationRetiresMultiReporterExclusionAfterFreshOwnerAndPeerAbsence(t *testing.T) {
	for _, quarantineFirst := range []bool{false, true} {
		name := "confirmed owner"
		if quarantineFirst {
			name = "confirmed after sole-reporter quarantine"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newMultiReporterExclusionFixture(t, true)
			if quarantineFirst {
				sweep, err := fixture.coordinator.BeginSweep()
				require.NoError(t, err)
				defer sweep.End()
				for _, backendName := range []string{"backend-a", "backend-b", "backend-c"} {
					storageID := testBackendStorageID(backendName)
					require.NoError(t, sweep.RecordProvision(backendName, storageID, nil))
					require.NoError(t, sweep.RecordRetention(backendName, storageID, nil))
				}
				require.NoError(t, sweep.RecordUntrusted("backend-a", []string{fencedAvailabilityOwner}))
				require.NoError(t, sweep.SealInventory())
				_, err = sweep.Project(ReconciliationProjection{
					UntrustedPositives: map[string][]string{fencedAvailabilityOwner: {"backend-a"}},
				})
				require.NoError(t, err)
				require.True(t, fixture.store.Lookup(fencedAvailabilityOwner).CanResolveUntrustedPositive("backend-a"))
				require.True(t, fixture.coordinator.AbsenceUntrusted(fencedAvailabilityOwner))
				sweep.End()
			}

			for range 2 {
				sweep, err := fixture.coordinator.BeginSweep()
				require.NoError(t, err)
				defer sweep.End()
				for _, backendName := range []string{"backend-a", "backend-b", "backend-c"} {
					var provisions []backend.ProvisionInfo
					if backendName == "backend-a" {
						provisions = []backend.ProvisionInfo{excludedReporterProvision(fixture, backendName)}
					}
					storageID := testBackendStorageID(backendName)
					require.NoError(t, sweep.RecordProvision(backendName, storageID, provisions))
					require.NoError(t, sweep.RecordRetention(backendName, storageID, nil))
				}
				require.NoError(t, sweep.SealInventory())
				projected, err := sweep.Project(ReconciliationProjection{
					Placements: map[string]string{fencedAvailabilityOwner: "backend-a"},
				})
				require.NoError(t, err)
				require.True(t, projected.Complete())
				require.True(t, projected.AdmissionBaseline().Valid())
				require.Equal(t, StateConfirmed, fixture.store.Lookup(fencedAvailabilityOwner).State())
				require.False(t, fixture.coordinator.AbsenceUntrusted(fencedAvailabilityOwner),
					"the same sealed epoch now accounts for both historical reporters")
				assert.Empty(t, fixture.coordinator.AbsenceUntrustedLeaseUUIDs())
				sweep.End()
			}
		})
	}
}

func TestReconciliationKeepsMultiReporterExclusionWithoutExactEvidence(t *testing.T) {
	for _, test := range []struct {
		name           string
		peer           string
		ownerAbsent    bool
		attemptPending bool
	}{
		{name: "peer unavailable", peer: "unavailable"},
		{name: "peer provision endpoint only", peer: "provision only"},
		{name: "peer retention endpoint only", peer: "retention only"},
		{name: "peer storage replaced", peer: "foreign storage"},
		{name: "peer still rejected", peer: "rejected"},
		{name: "peer still retained", peer: "retained"},
		{name: "peer still provisioned", peer: "provisioned"},
		{name: "complete silence is not an owner", ownerAbsent: true},
		{name: "unsettled attempt", attemptPending: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMultiReporterExclusionFixture(t, !test.attemptPending)
			sweep, err := fixture.coordinator.BeginSweep()
			require.NoError(t, err)
			defer sweep.End()
			var owner []backend.ProvisionInfo
			projection := ReconciliationProjection{}
			if !test.ownerAbsent {
				owner = []backend.ProvisionInfo{excludedReporterProvision(fixture, "backend-a")}
				projection.Placements = map[string]string{fencedAvailabilityOwner: "backend-a"}
			}
			require.NoError(t, sweep.RecordProvision("backend-a", testBackendStorageID("backend-a"), owner))
			require.NoError(t, sweep.RecordRetention("backend-a", testBackendStorageID("backend-a"), nil))
			require.NoError(t, sweep.RecordProvision("backend-c", testBackendStorageID("backend-c"), nil))
			require.NoError(t, sweep.RecordRetention("backend-c", testBackendStorageID("backend-c"), nil))

			peerStorage := testBackendStorageID("backend-b")
			if test.peer == "foreign storage" {
				peerStorage = testBackendStorageID("replacement-b")
			}
			var peerProvisions []backend.ProvisionInfo
			var peerRetentions []string
			if test.peer == "provisioned" {
				peerProvisions = []backend.ProvisionInfo{excludedReporterProvision(fixture, "backend-b")}
			}
			if test.peer == "retained" {
				peerRetentions = []string{fencedAvailabilityOwner}
			}
			if test.peer != "unavailable" && test.peer != "retention only" {
				require.NoError(t, sweep.RecordProvision("backend-b", peerStorage, peerProvisions))
			}
			if test.peer != "unavailable" && test.peer != "provision only" {
				require.NoError(t, sweep.RecordRetention("backend-b", peerStorage, peerRetentions))
			}
			if test.peer == "rejected" {
				require.NoError(t, sweep.RecordUntrusted("backend-b", []string{fencedAvailabilityOwner}))
			}
			peerPresent := test.peer == "rejected" || test.peer == "provisioned" || test.peer == "retained"
			if peerPresent {
				projection = ReconciliationProjection{
					Conflicts: map[string][]string{fencedAvailabilityOwner: {"backend-a", "backend-b"}},
				}
			}
			require.NoError(t, sweep.SealInventory())
			_, err = sweep.Project(projection)
			if test.peer == "foreign storage" {
				require.ErrorIs(t, err, ErrBackendStorageIdentityMismatch)
			} else {
				require.NoError(t, err)
			}
			require.True(t, fixture.coordinator.AbsenceUntrusted(fencedAvailabilityOwner))
			require.Len(t, fixture.coordinator.absenceUntrusted[fencedAvailabilityOwner], 2,
				"partial evidence cannot incrementally forget an earlier reporter")
			if test.attemptPending {
				assert.Equal(t, "backend-a", fixture.store.Lookup(fencedAvailabilityOwner).Attempt)
			}
			if !peerPresent {
				return
			}
			sweep.End()

			// An actual multi-owner quarantine is durable authority, not a stale
			// diagnostic. A later healthy snapshot must not clear either fence.
			healed, err := fixture.coordinator.BeginSweep()
			require.NoError(t, err)
			defer healed.End()
			for _, name := range []string{"backend-a", "backend-b", "backend-c"} {
				var provisions []backend.ProvisionInfo
				if name == "backend-a" {
					provisions = owner
				}
				storageID := testBackendStorageID(name)
				require.NoError(t, healed.RecordProvision(name, storageID, provisions))
				require.NoError(t, healed.RecordRetention(name, storageID, nil))
			}
			require.NoError(t, healed.SealInventory())
			_, err = healed.Project(ReconciliationProjection{
				Placements: map[string]string{fencedAvailabilityOwner: "backend-a"},
			})
			require.NoError(t, err)
			record := fixture.store.Lookup(fencedAvailabilityOwner)
			assert.Equal(t, StateUnusable, record.State())
			assert.Equal(t, []string{"backend-a", "backend-b"}, record.ConflictBackends)
			assert.True(t, fixture.coordinator.AbsenceUntrusted(fencedAvailabilityOwner))
		})
	}
}
