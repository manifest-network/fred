package placement

import (
	"context"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

// A rejected peer positive and an overlapping placement mutation leave both
// reporters outside the durable projection. Successful exact settlement then
// restores the real owner, but cannot consume the separate inventory evidence.
func newMultiReporterExclusionFixture(t *testing.T, settleAttempt bool) fencedAvailabilityFixture {
	t.Helper()
	return newReporterExclusionFixture(t, settleAttempt, true)
}

func newReporterExclusionFixture(t *testing.T, settleAttempt, ownerReported bool) fencedAvailabilityFixture {
	t.Helper()
	fixture := newFencedAvailabilityFixture(t)
	sweep := beginFencedAvailabilitySweep(t, fixture)
	defer sweep.End()
	for _, name := range []string{"backend-a", "backend-b", "backend-c"} {
		var provisions []backend.ProvisionInfo
		if name == "backend-a" && ownerReported {
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
	if ownerReported {
		require.Len(t, fixture.coordinator.absenceUntrusted[fencedAvailabilityOwner], 2)
	} else {
		require.Equal(t, map[string]struct{}{"backend-b": {}}, fixture.coordinator.absenceUntrusted[fencedAvailabilityOwner])
	}
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

func TestReconciliationRetiresExcludedObservationAfterOwnerDisappears(t *testing.T) {
	fixture := newMultiReporterExclusionFixture(t, true)
	lease := &billingtypes.Lease{
		Uuid: fencedAvailabilityOwner, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
		State: billingtypes.LEASE_STATE_ACTIVE,
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1, ServiceName: "app"}},
	}
	setProviderControlPlaneForTest(t, fixture.coordinator.coordinator.execution,
		&reconciliationSweepReader{lease: lease})
	var requests []backend.ProvisionRequest
	ownerBackend := fixture.coordinator.backends.GetBackendByName("backend-a").(*executionTestBackend)
	ownerBackend.provision = func(_ context.Context, request backend.ProvisionRequest) error {
		requests = append(requests, request)
		return nil
	}
	before := fixture.store.Lookup(fencedAvailabilityOwner)
	beforeLifecycle := fixture.store.CurrentLifecycle(fencedAvailabilityOwner)

	// The candidate died before the peer healed. Both owner endpoints and every
	// earlier reporter now prove absence; that retires only the stale diagnostic,
	// not the confirmed affinity or its durable lifecycle capability.
	for range 2 {
		sweep, err := fixture.coordinator.BeginSweep()
		require.NoError(t, err)
		defer sweep.End()
		for _, name := range []string{"backend-a", "backend-b", "backend-c"} {
			storageID := testBackendStorageID(name)
			require.NoError(t, sweep.RecordProvision(name, storageID, nil))
			require.NoError(t, sweep.RecordRetention(name, storageID, nil))
		}
		require.NoError(t, sweep.SealInventory())
		projected, err := sweep.Project(ReconciliationProjection{})
		require.NoError(t, err)
		require.True(t, projected.Complete())
		require.False(t, fixture.coordinator.AbsenceUntrusted(fencedAvailabilityOwner),
			"a confirmed owner need not resurrect its old container to resolve inventory uncertainty")
		require.Equal(t, before, fixture.store.Lookup(fencedAvailabilityOwner))
		require.Equal(t, beforeLifecycle, fixture.store.CurrentLifecycle(fencedAvailabilityOwner))
		action, disposition, err := projected.ObserveLiveAction(t.Context(), fencedAvailabilityOwner)
		require.NoError(t, err)
		require.Equal(t, ReconciliationObservationReady, disposition)
		require.True(t, action.Valid())
		require.True(t, fixture.coordinator.ReleaseAction(action))
		sweep.End()
	}

	// A fresh chain/operation claim can re-provision on the preserved owner, not
	// the lower-load sibling selected for genuinely recordless admission.
	sweep, err := fixture.coordinator.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	for _, name := range []string{"backend-a", "backend-b", "backend-c"} {
		storageID := testBackendStorageID(name)
		require.NoError(t, sweep.RecordProvision(name, storageID, nil))
		require.NoError(t, sweep.RecordRetention(name, storageID, nil))
	}
	require.NoError(t, sweep.SealInventory())
	projected, err := sweep.Project(ReconciliationProjection{})
	require.NoError(t, err)
	action, disposition, err := projected.ObserveLiveAction(t.Context(), fencedAvailabilityOwner)
	require.NoError(t, err)
	require.Equal(t, ReconciliationObservationReady, disposition)
	defer fixture.coordinator.ReleaseAction(action)
	result := fixture.coordinator.Provision(t.Context(), action, nil, PayloadFingerprint{})
	require.NoError(t, result.Err())
	assert.Equal(t, "backend-a", result.BackendName())
	require.Len(t, requests, 1)
	assert.Equal(t, fencedAvailabilityOwner, requests[0].LeaseUUID)
}

func TestReconciliationRetiresExcludedObservationForRetainedOwner(t *testing.T) {
	fixture := newMultiReporterExclusionFixture(t, true)
	before := fixture.store.Lookup(fencedAvailabilityOwner)
	sweep, err := fixture.coordinator.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	for _, name := range []string{"backend-a", "backend-b", "backend-c"} {
		var retained []string
		if name == "backend-a" {
			retained = []string{fencedAvailabilityOwner}
		}
		storageID := testBackendStorageID(name)
		require.NoError(t, sweep.RecordProvision(name, storageID, nil))
		require.NoError(t, sweep.RecordRetention(name, storageID, retained))
	}
	require.NoError(t, sweep.SealInventory())
	_, err = sweep.Project(ReconciliationProjection{
		Placements: map[string]string{fencedAvailabilityOwner: "backend-a"},
	})
	require.NoError(t, err)
	assert.False(t, fixture.coordinator.AbsenceUntrusted(fencedAvailabilityOwner))
	assert.True(t, equalPlacementIgnoringRevision(before, fixture.store.Lookup(fencedAvailabilityOwner)))
	assert.False(t, fixture.store.CurrentLifecycle(fencedAvailabilityOwner).Authorized(),
		"retention membership is not evidence of a current runtime generation")
	assert.True(t, sweep.sealed.RetentionReporter(
		fixture.coordinator.projector.collector.Binding(), "backend-a", fencedAvailabilityOwner,
	), "the live-lease retention gate still sees the exact positive; diagnostic retirement does not turn it into absence")
}

func TestReconciliationAbsentDiagnosticDoesNotBypassOperationBoundary(t *testing.T) {
	for _, phase := range []string{"active at capture", "completed after capture", "claimed after capture"} {
		t.Run(phase, func(t *testing.T) {
			fixture := newMultiReporterExclusionFixture(t, true)
			setProviderControlPlaneForTest(t, fixture.coordinator.coordinator.execution, &reconciliationSweepReader{
				lease: &billingtypes.Lease{
					Uuid: fencedAvailabilityOwner, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
					State: billingtypes.LEASE_STATE_ACTIVE,
				},
			})
			registry := fixture.coordinator.coordinator.operations
			var release func()
			claim := func() {
				result := registry.TryClaimLeaseNow(fencedAvailabilityOwner)
				require.True(t, result.Acquired())
				release = func() { require.True(t, registry.ReleaseLease(result.Claim())) }
			}
			if phase != "claimed after capture" {
				claim()
			}
			sweep, err := fixture.coordinator.BeginSweep()
			require.NoError(t, err)
			defer sweep.End()
			if phase == "claimed after capture" {
				claim()
			}
			if phase != "active at capture" {
				release()
			}
			for _, name := range []string{"backend-a", "backend-b", "backend-c"} {
				storageID := testBackendStorageID(name)
				require.NoError(t, sweep.RecordProvision(name, storageID, nil))
				require.NoError(t, sweep.RecordRetention(name, storageID, nil))
			}
			require.NoError(t, sweep.SealInventory())
			projected, err := sweep.Project(ReconciliationProjection{})
			require.NoError(t, err)
			require.False(t, fixture.coordinator.AbsenceUntrusted(fencedAvailabilityOwner))
			action, disposition, err := projected.ObserveLiveAction(t.Context(), fencedAvailabilityOwner)
			require.NoError(t, err)
			assert.Equal(t, ReconciliationObservationStale, disposition)
			assert.False(t, action.Valid(), "diagnostic retirement cannot mint a claim across the operation fence")
			if phase == "active at capture" {
				release()
			}
		})
	}
}

func TestReconciliationExcludedAbsenceRequiresCurrentOwnerEvenIfNeverReported(t *testing.T) {
	for _, endpoint := range []string{"unavailable", "provision only", "retention only"} {
		t.Run(endpoint, func(t *testing.T) {
			fixture := newReporterExclusionFixture(t, true, false)
			before := fixture.store.Lookup(fencedAvailabilityOwner)
			sweep, err := fixture.coordinator.BeginSweep()
			require.NoError(t, err)
			defer sweep.End()
			storageA := testBackendStorageID("backend-a")
			if endpoint == "provision only" {
				require.NoError(t, sweep.RecordProvision("backend-a", storageA, nil))
			}
			if endpoint == "retention only" {
				require.NoError(t, sweep.RecordRetention("backend-a", storageA, nil))
			}
			for _, name := range []string{"backend-b", "backend-c"} {
				storageID := testBackendStorageID(name)
				require.NoError(t, sweep.RecordProvision(name, storageID, nil))
				require.NoError(t, sweep.RecordRetention(name, storageID, nil))
			}
			require.NoError(t, sweep.SealInventory())
			_, err = sweep.Project(ReconciliationProjection{})
			require.NoError(t, err)
			assert.True(t, fixture.coordinator.AbsenceUntrusted(fencedAvailabilityOwner),
				"absence of every historical reporter cannot substitute for the missing owner's evidence")
			assert.Equal(t, before, fixture.store.Lookup(fencedAvailabilityOwner))
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
		{name: "unsettled attempt", attemptPending: true},
		{name: "absent owner and unavailable peer", ownerAbsent: true, peer: "unavailable"},
		{name: "absent owner and peer provision endpoint only", ownerAbsent: true, peer: "provision only"},
		{name: "absent owner and peer retention endpoint only", ownerAbsent: true, peer: "retention only"},
		{name: "absent owner and replaced peer storage", ownerAbsent: true, peer: "foreign storage"},
		{name: "absent owner with unsettled attempt", ownerAbsent: true, attemptPending: true},
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
			healed.End()

			absent, err := fixture.coordinator.BeginSweep()
			require.NoError(t, err)
			defer absent.End()
			for _, name := range []string{"backend-a", "backend-b", "backend-c"} {
				storageID := testBackendStorageID(name)
				require.NoError(t, absent.RecordProvision(name, storageID, nil))
				require.NoError(t, absent.RecordRetention(name, storageID, nil))
			}
			require.NoError(t, absent.SealInventory())
			_, err = absent.Project(ReconciliationProjection{})
			require.NoError(t, err)
			assert.Equal(t, record, fixture.store.Lookup(fencedAvailabilityOwner),
				"even complete absence cannot resolve a durable ownership conflict")
			assert.True(t, fixture.coordinator.AbsenceUntrusted(fencedAvailabilityOwner))
		})
	}
}
