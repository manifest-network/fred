package placement

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
)

func TestRetirePlacementDecidesByDurableOwner(t *testing.T) {
	const retired = "backend-c"
	setAt := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	lost := Placement{lostBackend: retired, unusable: true}
	for _, test := range []struct {
		name        string
		in          Placement
		want        Placement
		disposition retirementDisposition
	}{
		{
			name:        "unrelated owner",
			in:          Placement{Backend: "backend-a", SetAt: setAt},
			want:        Placement{Backend: "backend-a", SetAt: setAt},
			disposition: retirementUnchanged,
		},
		{
			name:        "owned by the retired backend",
			in:          Placement{Backend: retired, SetAt: setAt},
			want:        lost,
			disposition: retirementLost,
		},
		{
			name:        "attempt only on the retired backend",
			in:          Placement{Attempt: retired, SetAt: setAt},
			want:        lost,
			disposition: retirementLost,
		},
		{
			name:        "survivor owner with an attempt on the retired backend",
			in:          Placement{Backend: "backend-a", Attempt: retired, SetAt: setAt},
			want:        Placement{Backend: "backend-a", SetAt: setAt},
			disposition: retirementStripped,
		},
		{
			name: "survivor owner contradicted only by the retired backend",
			in: Placement{
				Backend: "backend-a", SetAt: setAt, Conflict: true,
				ConflictBackends: []string{"backend-a", retired},
			},
			want:        Placement{Backend: "backend-a", SetAt: setAt},
			disposition: retirementStripped,
		},
		{
			name: "survivor owner contradicted by another survivor too",
			in: Placement{
				Backend: "backend-a", SetAt: setAt, Conflict: true,
				ConflictBackends: []string{"backend-a", "backend-b", retired},
			},
			want: Placement{
				Backend: "backend-a", SetAt: setAt, Conflict: true,
				ConflictBackends: []string{"backend-a", "backend-b"},
			},
			disposition: retirementStripped,
		},
		{
			// Adopting the one surviving report would continue the lease on a
			// copy nobody chose.
			name: "ownerless conflict leaving one survivor reporter is lost",
			in: Placement{
				SetAt: setAt, Conflict: true, ConflictBackends: []string{"backend-b", retired},
			},
			want:        lost,
			disposition: retirementLost,
		},
		{
			name: "ownerless conflict leaving two survivor reporters stays operator-only",
			in: Placement{
				SetAt: setAt, Conflict: true, ConflictBackends: []string{"backend-a", "backend-b", retired},
			},
			want: Placement{
				SetAt: setAt, Conflict: true, ConflictBackends: []string{"backend-a", "backend-b"},
			},
			disposition: retirementStripped,
		},
		{
			name: "untrusted quarantine whose only candidate is retired",
			in: Placement{
				SetAt: setAt, Conflict: true, ConflictBackends: []string{retired},
				untrustedPositive: true,
			},
			want:        lost,
			disposition: retirementLost,
		},
		{
			name: "owned by the retired backend despite a survivor candidate",
			in: Placement{
				Backend: retired, SetAt: setAt, Conflict: true,
				ConflictBackends: []string{"backend-a", retired},
			},
			want:        lost,
			disposition: retirementLost,
		},
		{
			name: "unknown-owner conflict is left exactly as it is",
			in: Placement{
				Backend: "backend-a", SetAt: setAt, Conflict: true,
				ConflictBackends: []string{"backend-a", retired}, ConflictOwnersUnknown: true,
			},
			want: Placement{
				Backend: "backend-a", SetAt: setAt, Conflict: true,
				ConflictBackends: []string{"backend-a", retired}, ConflictOwnersUnknown: true,
			},
			disposition: retirementUnchanged,
		},
		{
			// A survivor may hold the data of a lease whose owners are unknown,
			// so it is never ended as lost, even when the retired backend is its
			// only known candidate.
			name: "unknown-owner conflict owned by the retired backend is never lost",
			in: Placement{
				Backend: retired, SetAt: setAt, Conflict: true,
				ConflictBackends: []string{retired}, ConflictOwnersUnknown: true,
			},
			want: Placement{
				Backend: retired, SetAt: setAt, Conflict: true,
				ConflictBackends: []string{retired}, ConflictOwnersUnknown: true,
			},
			disposition: retirementUnchanged,
		},
		{
			name:        "already lost to another retirement",
			in:          Placement{SetAt: setAt, lostBackend: "backend-z", unusable: true},
			want:        Placement{SetAt: setAt, lostBackend: "backend-z", unusable: true},
			disposition: retirementUnchanged,
		},
		{
			name:        "uninterpretable row",
			in:          Placement{unusable: true},
			want:        Placement{unusable: true},
			disposition: retirementUnchanged,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, disposition := retirePlacement(test.in, retired)
			assert.Equal(t, test.disposition, disposition)
			assert.True(t, equalPlacementIgnoringRevision(test.want, got),
				"want %+v, got %+v", test.want, got)
		})
	}
}

func TestRetireLifecycleScrubsEveryReferenceToTheRetiredBackend(t *testing.T) {
	const retired = "backend-c"
	sentinel := lifecycleCapability{unusable: true}
	owned := lifecycleCapability{backend: retired}
	survivor := lifecycleCapability{backend: "backend-a"}
	attemptID := requireLifecycleID(t, "501")
	// A recordless attempt on the retired backend: its marker is the
	// capability's only evidence (ENG-1119).
	attemptOnly := lifecycleCapability{attemptBackend: retired, attemptID: attemptID}
	quarantinedAttempt := lifecycleCapability{unusable: true, attemptBackend: retired, attemptID: attemptID}
	survivorWithAttempt := lifecycleCapability{backend: "backend-a", attemptBackend: retired, attemptID: attemptID}
	for _, test := range []struct {
		name             string
		in               lifecycleCapability
		disposition      retirementDisposition
		placementExists  bool
		want             lifecycleCapability
		changed, deleted bool
	}{
		{"lost lease keeps only the sentinel", owned, retirementLost, true, sentinel, true, false},
		{"lost lease with a survivor capability", survivor, retirementLost, true, sentinel, true, false},
		{"surviving lease with an unrelated capability", survivor, retirementStripped, true, survivor, false, false},
		{"surviving lease whose capability owner was retired", owned, retirementStripped, true, sentinel, true, false},
		{"detached capability naming the retired backend", owned, retirementUnchanged, false, owned, true, true},
		{"detached unrelated capability", survivor, retirementUnchanged, false, survivor, false, false},
		{"stripped conflict whose capability was only the retired attempt", attemptOnly, retirementStripped, true, sentinel, true, false},
		{"uninterpretable placement whose capability was only the retired attempt", attemptOnly, retirementUnchanged, true, sentinel, true, false},
		{"lost lease whose capability was only the retired attempt", attemptOnly, retirementLost, true, sentinel, true, false},
		{"detached capability that was only the retired attempt", attemptOnly, retirementUnchanged, false, attemptOnly, true, true},
		{"quarantined capability with a retired attempt", quarantinedAttempt, retirementStripped, true, sentinel, true, false},
		{"survivor-owned capability with a retired attempt", survivorWithAttempt, retirementStripped, true, survivor, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, changed, deleted := retireLifecycle(test.in, retired, test.disposition, test.placementExists)
			assert.Equal(t, test.want, got)
			assert.Equal(t, test.changed, changed)
			assert.Equal(t, test.deleted, deleted)
		})
	}
}

// TestRetireLifecycleOutputAlwaysEncodes enumerates every valid capability
// shape against every disposition. A retirement plan encodes each rewritten
// capability, so an output the encoder refuses would make the whole retirement
// refuse (ENG-1119). Every kept output must encode, never name the retired
// backend, never lift a quarantine, and never gain authority.
func TestRetireLifecycleOutputAlwaysEncodes(t *testing.T) {
	const retired = "backend-c"
	ownerID := requireLifecycleID(t, "502")
	attemptID := requireLifecycleID(t, "503")
	principal := runtimePrincipal{tenant: "tenant-test", providerUUID: freshTestProviderUUID}
	type attempt struct {
		backend string
		id      lifecycle.ID
	}
	inputs, ownerlessRetiredAttempts := 0, 0
	for _, backendName := range []string{"", "backend-a", retired} {
		for _, id := range []lifecycle.ID{{}, ownerID} {
			for _, marker := range []attempt{{}, {"backend-a", attemptID}, {retired, attemptID}} {
				for _, unusable := range []bool{false, true} {
					for _, retiredFlag := range []bool{false, true} {
						for _, owner := range []runtimePrincipal{{}, principal} {
							in := lifecycleCapability{
								backend: backendName, id: id, principal: owner, retired: retiredFlag,
								attemptBackend: marker.backend, attemptID: marker.id, unusable: unusable,
							}
							if validateLifecycleCapability(in) != nil {
								continue
							}
							inputs++
							if in.backend == "" && in.attemptBackend == retired && !in.unusable {
								ownerlessRetiredAttempts++
							}
							for _, disposition := range []retirementDisposition{
								retirementUnchanged, retirementLost, retirementStripped,
							} {
								for _, placementExists := range []bool{true, false} {
									got, _, deleted := retireLifecycle(in, retired, disposition, placementExists)
									if deleted {
										continue
									}
									_, err := encodeLifecycleCapability(got)
									require.NoError(t, err,
										"in=%+v disposition=%d placement=%t got=%+v", in, disposition, placementExists, got)
									assert.NotEqual(t, retired, got.backend, "in=%+v disposition=%d", in, disposition)
									assert.NotEqual(t, retired, got.attemptBackend, "in=%+v disposition=%d", in, disposition)
									if in.unusable {
										assert.True(t, got.unusable, "a retirement never lifts a quarantine: in=%+v", in)
									}
									if !got.unusable {
										assert.Equal(t, in.backend, got.backend, "in=%+v", in)
										assert.Equal(t, in.id, got.id, "in=%+v", in)
										assert.Equal(t, in.principal, got.principal, "in=%+v", in)
										assert.Equal(t, in.retired, got.retired, "in=%+v", in)
									}
								}
							}
						}
					}
				}
			}
		}
	}
	require.NotZero(t, inputs)
	require.NotZero(t, ownerlessRetiredAttempts, "the refused shape must be enumerated")
}

// TestQuarantineOwnerlessLifecycleIsTheOnlyRewrite pins the normalizer every
// retireLifecycle output passes through: it is the identity on every
// capability the encoder accepts, and it turns exactly the ownerless usable
// shape the encoder refuses into the evidence-free quarantine sentinel.
func TestQuarantineOwnerlessLifecycleIsTheOnlyRewrite(t *testing.T) {
	ownerID := requireLifecycleID(t, "504")
	attemptID := requireLifecycleID(t, "505")
	principal := runtimePrincipal{tenant: "tenant-test", providerUUID: freshTestProviderUUID}
	type attempt struct {
		backend string
		id      lifecycle.ID
	}
	ownerless := 0
	for _, backendName := range []string{"", "backend-a"} {
		for _, id := range []lifecycle.ID{{}, ownerID} {
			for _, marker := range []attempt{{}, {"backend-a", attemptID}} {
				for _, unusable := range []bool{false, true} {
					for _, retiredFlag := range []bool{false, true} {
						for _, owner := range []runtimePrincipal{{}, principal} {
							in := lifecycleCapability{
								backend: backendName, id: id, principal: owner, retired: retiredFlag,
								attemptBackend: marker.backend, attemptID: marker.id, unusable: unusable,
							}
							got := quarantineOwnerlessLifecycle(in)
							if validateLifecycleCapability(in) == nil {
								assert.Equal(t, in, got, "an encodable capability is never rewritten")
								continue
							}
							if !unusable && backendName == "" && marker.backend == "" && !id.Valid() && !retiredFlag {
								ownerless++
								assert.Equal(t, lifecycleCapability{unusable: true}, got)
								_, err := encodeLifecycleCapability(got)
								require.NoError(t, err)
							}
						}
					}
				}
			}
		}
	}
	require.Equal(t, 2, ownerless, "both principal shapes of the ownerless capability are enumerated")
	corrupt := lifecycleCapability{rawCorrupt: true}
	assert.Equal(t, corrupt, quarantineOwnerlessLifecycle(corrupt), "undecodable bytes are never rewritten")
}

// retirementFixture is a stopped provider database with three backends: a
// lease owned by backend-c with a pending restart, a lease backend-c merely
// contradicted on backend-a, and an unrelated lease on backend-b.
type retirementFixture struct {
	dbPath      string
	owned       string
	contested   string
	unrelated   string
	pinC        backendidentity.ID
	maintenance maintenanceid.ID
}

func newRetirementFixture(t *testing.T) retirementFixture {
	t.Helper()
	fixture := retirementFixture{
		dbPath:    filepath.Join(t.TempDir(), "placements.db"),
		owned:     "00000000-0000-4000-8000-000000000401",
		contested: "00000000-0000-4000-8000-000000000402",
		unrelated: "00000000-0000-4000-8000-000000000403",
		pinC:      testBackendStorageID("backend-c"),
	}
	store, err := newStoreForTest(fixture.dbPath, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, err)
	baseline := requireAdmissionBaseline(t, store, "backend-a", "backend-b", "backend-c")
	scope := requireAdmissionScope(t, store, baseline, "backend-c")
	seedID := requireOperationID(t, "401")
	request, err := newBackendRequestSnapshot(
		"tenant-test", freshTestProviderUUID,
		[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
	)
	require.NoError(t, err)
	attempt, applied, err := store.beginNewAttempt(
		scope, fixture.owned, "backend-c", seedID, PayloadFingerprint{}, request, testCallbackPair(seedID),
	)
	require.NoError(t, err)
	require.True(t, applied)
	confirmed, err := confirmAttemptForTest(store, attempt)
	require.NoError(t, err)
	require.True(t, confirmed)
	fixture.maintenance, err = maintenanceid.Parse(maintenanceIDA)
	require.NoError(t, err)
	prepared, err := store.prepareMaintenanceCommand(
		fixture.maintenance, fixture.owned, MaintenanceCommandRestart, nil,
	)
	require.NoError(t, err)
	admission, err := store.beginMaintenanceCommand(prepared)
	require.NoError(t, err)
	require.True(t, admission.Pending())

	requireConfirmedPlacement(t, store, fixture.contested, "backend-a")
	requireConflictPlacement(t, store, fixture.contested, "backend-a", "backend-c")
	requireConfirmedPlacement(t, store, fixture.unrelated, "backend-b")
	require.NoError(t, store.Close())
	return fixture
}

func TestBackendRetirementClosesOnlyWhatTheRetiredBackendOwned(t *testing.T) {
	fixture := newRetirementFixture(t)
	repair, err := OpenAttemptRepair(fixture.dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })

	_, err = repair.PlanBackendRetirement("backend-c", testBackendStorageID("backend-a"))
	require.ErrorIs(t, err, ErrBackendRetirementTarget, "the storage identity must be backend-c's pin")
	_, err = repair.PlanBackendRetirement("backend-z", fixture.pinC)
	require.ErrorIs(t, err, ErrBackendRetirementTarget, "only an active backend can be retired")

	plan, err := repair.PlanBackendRetirement("backend-c", fixture.pinC)
	require.NoError(t, err)
	facts := plan.Facts()
	assert.Equal(t, []string{fixture.owned}, facts.LostLeases)
	assert.Equal(t, []string{fixture.contested}, facts.StrippedLeases)
	assert.Equal(t, []string{fixture.owned}, facts.MaintenanceSettled)
	assert.Equal(t, []string{"backend-a", "backend-b"}, facts.TopologyAfter)
	assert.False(t, facts.RecordlessUnproven, "a current admission baseline covered backend-c")

	_, err = repair.RetireBackend(plan, LostBackendAttestationText)
	require.Error(t, err, "the exact rollback image must be published first")
	publishRetirementBackup(t, repair)
	_, err = repair.RetireBackend(plan, "yes")
	require.ErrorIs(t, err, ErrBackendRetirementTarget, "the attestation must be exact")
	result, err := repair.RetireBackend(plan, LostBackendAttestationText)
	require.NoError(t, err)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())

	inspector, err := OpenRepairInspector(fixture.dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	require.NoError(t, inspector.VerifyBackendRetirementPostcondition(result))
	require.NoError(t, inspector.Close())

	store, err := OpenStore(fixture.dbPath, freshTestProviderUUID, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	owned := store.Lookup(fixture.owned)
	name, lost := owned.LostBackend()
	assert.True(t, lost)
	assert.Equal(t, "backend-c", name)
	assert.Equal(t, StateUnusable, owned.State())
	contested := store.Lookup(fixture.contested)
	assert.Equal(t, StateConfirmed, contested.State(), "a survivor-owned lease keeps running")
	assert.Equal(t, "backend-a", contested.Backend)
	assert.False(t, contested.Conflict)
	assert.Equal(t, "backend-b", store.Lookup(fixture.unrelated).Backend)
	assert.False(t, store.CurrentAdmissionBaseline().Valid(),
		"admission waits for one complete inventory of the survivors")

	_, err = store.prepareMaintenanceCommand(fixture.maintenance, fixture.owned, MaintenanceCommandRestart, nil)
	require.ErrorIs(t, err, ErrPlacementLost)
	_, err = store.reserveRestoreSource(fixture.owned)
	require.ErrorIs(t, err, ErrPlacementLost, "a lost lease can never be a restore source")
	assert.Equal(t, RestoreApplicationSourceLost, restoreAdmissionApplicationFailure(err).Disposition())
	require.ErrorIs(t, store.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a", "backend-b", "backend-c"},
		testBackendStorageIDs("backend-a", "backend-b", "backend-c"),
	), ErrBackendRetired, "a retired name can never rejoin")
	_, err = store.BackendTopologyRequiresIdentityProbe([]string{"backend-a", "backend-b", "backend-c"})
	require.ErrorIs(t, err, ErrBackendRetired, "startup refuses a stale config before any probe")
	require.NoError(t, store.VerifyBackendTopology([]string{"backend-a", "backend-b"}))

	metadata := persistedTopologyMetadata(t, store)
	record, retired := metadata.RetiredBackends["backend-c"]
	require.True(t, retired)
	assert.False(t, record.RecordlessUnproven)
	assert.Equal(t, fixture.pinC.String(), metadata.KnownBackendStorageIDs["backend-c"],
		"the retired storage stays pinned so no other name can claim it")

	reused := testBackendStorageIDs("backend-a", "backend-b")
	reused["backend-d"] = fixture.pinC
	require.ErrorIs(t, store.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a", "backend-b", "backend-d"}, reused,
	), ErrBackendStorageIdentityConflict, "a new name cannot claim the retired storage")
	require.NoError(t, store.ConfigureBackendTopologyWithStorageIdentities(
		[]string{"backend-a", "backend-b", "backend-d"},
		testBackendStorageIDs("backend-a", "backend-b", "backend-d"),
	), "a replacement host joins under a new name and fresh storage")
}

func publishRetirementBackup(t *testing.T, repair *AttemptRepair) {
	t.Helper()
	target, err := BindExactBackupTarget(filepath.Join(t.TempDir(), "pre-retirement.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = target.Close() })
	require.NoError(t, repair.CreateExactBackup(target))
}

// retiredFixtureStore applies the fixture's retirement of backend-c and
// reopens the store as providerd would.
func retiredFixtureStore(t *testing.T) (*Store, retirementFixture) {
	t.Helper()
	fixture := newRetirementFixture(t)
	repair, err := OpenAttemptRepair(fixture.dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	plan, err := repair.PlanBackendRetirement("backend-c", fixture.pinC)
	require.NoError(t, err)
	publishRetirementBackup(t, repair)
	_, err = repair.RetireBackend(plan, LostBackendAttestationText)
	require.NoError(t, err)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())
	store, err := OpenStore(fixture.dbPath, freshTestProviderUUID, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store, fixture
}

// TestRetirementProbeTargetKnowsEveryPinTheDatabaseRecorded pins the probe's
// comparison set to the database's whole pin history, so an address that
// reaches a retired backend is never read as unknown storage.
func TestRetirementProbeTargetKnowsEveryPinTheDatabaseRecorded(t *testing.T) {
	store, fixture := retiredFixtureStore(t)
	require.NoError(t, store.Close())
	repair, err := OpenAttemptRepair(fixture.dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	pinB := testBackendStorageID("backend-b")
	plan, err := repair.PlanBackendRetirement("backend-b", pinB)
	require.NoError(t, err)

	target, err := plan.ProbeTarget()
	require.NoError(t, err)
	assert.Equal(t, "backend-b", target.BackendName())
	assert.Equal(t, pinB, target.Pin())
	for backendName, pin := range map[string]backendidentity.ID{
		"backend-a": testBackendStorageID("backend-a"),
		"backend-b": pinB,
		"backend-c": fixture.pinC, // retired earlier, so no longer active
	} {
		owner, known := target.StorageOwner(pin)
		assert.True(t, known, backendName)
		assert.Equal(t, backendName, owner)
	}
	_, known := target.StorageOwner(testBackendStorageID("backend-never-pinned"))
	assert.False(t, known)

	_, err = BackendRetirementPlan{}.ProbeTarget()
	require.Error(t, err, "only a plan mints a probe target")
}

// TestProjectionNeverRewritesALostPlacement pins the store-side absorbing
// rule independently of the reconciler's own filtering: no observation a
// caller submits can rewrite, quarantine, or resurrect a lost placement.
func TestProjectionNeverRewritesALostPlacement(t *testing.T) {
	for name, projection := range map[string]func(lease string) InventoryProjection{
		"survivor positive": func(lease string) InventoryProjection {
			return InventoryProjection{Placements: map[string]string{lease: "backend-a"}}
		},
		"untrusted positive": func(lease string) InventoryProjection {
			return InventoryProjection{UntrustedPositives: map[string][]string{lease: {"backend-a"}}}
		},
		"conflict": func(lease string) InventoryProjection {
			return InventoryProjection{Conflicts: map[string][]string{lease: {"backend-a", "backend-b"}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			store, fixture := retiredFixtureStore(t)
			before := store.Lookup(fixture.owned)
			projectInventoryForTest(t, store, projection(fixture.owned))
			after := store.Lookup(fixture.owned)
			lostBackend, lost := after.LostBackend()
			require.True(t, lost, "a lost placement absorbs every observation")
			assert.Equal(t, "backend-c", lostBackend)
			assert.Equal(t, before.Revision(), after.Revision(), "nothing rewrites the lost row")
			assert.False(t, after.Conflict)
		})
	}
}

// TestDeprovisionOfALostLeaseCompletesWithoutABackendCall covers the lease
// closed event for a lost lease: its storage is attested gone, so completion
// asks no survivor to tear down a lease it never held.
func TestDeprovisionOfALostLeaseCompletesWithoutABackendCall(t *testing.T) {
	store, fixture := retiredFixtureStore(t)
	var called []string
	record := func(_ context.Context, leaseUUID string) error { called = append(called, leaseUUID); return nil }
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(
		&executionTestBackend{name: "backend-a", deprovision: record},
		&executionTestBackend{name: "backend-b", deprovision: record},
	))
	provision, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)

	result := provision.DeprovisionEvent(t.Context(), fixture.owned)
	require.Equal(t, DeprovisionEventCompleted, result.Disposition(), result.Err())
	require.NoError(t, result.Err())
	assert.Empty(t, called)
	_, lost := store.Lookup(fixture.owned).LostBackend()
	assert.True(t, lost, "only an exact terminal chain read prunes the lost row")
}

// TestRetirementLeavesAnUnknownOwnerQuarantineOperatorOnly covers a legacy
// quarantine whose only known candidate is the retired backend: its owners are
// unknown, so a survivor may hold its data. The retirement lists it, leaves it
// exactly as it is, and the reopened store keeps serving around it.
func TestRetirementLeavesAnUnknownOwnerQuarantineOperatorOnly(t *testing.T) {
	fixture := newRetirementFixture(t)
	const quarantined = "00000000-0000-4000-8000-000000000404"
	row, err := encodePlacement(Placement{
		Backend: "backend-c", SetAt: time.Now().UTC(), Conflict: true,
		ConflictBackends: []string{"backend-c"}, revision: 1000,
	})
	require.NoError(t, err)
	capability, err := encodeLifecycleCapability(lifecycleCapability{unusable: true})
	require.NoError(t, err)
	db, err := bolt.Open(fixture.dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketName).Put([]byte(quarantined), row); err != nil {
			return err
		}
		return tx.Bucket(lifecycleCapabilityBucketName).Put([]byte(quarantined), capability)
	}))
	require.NoError(t, db.Close())

	repair, err := OpenAttemptRepair(fixture.dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	plan, err := repair.PlanBackendRetirement("backend-c", fixture.pinC)
	require.NoError(t, err)
	facts := plan.Facts()
	assert.Contains(t, facts.UnknownOwnerConflicts, quarantined)
	assert.NotContains(t, facts.LostLeases, quarantined)
	assert.NotContains(t, facts.StrippedLeases, quarantined)
	publishRetirementBackup(t, repair)
	result, err := repair.RetireBackend(plan, LostBackendAttestationText)
	require.NoError(t, err)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())
	inspector, err := OpenRepairInspector(fixture.dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	require.NoError(t, inspector.VerifyBackendRetirementPostcondition(result))
	require.NoError(t, inspector.Close())

	store, err := OpenStore(fixture.dbPath, freshTestProviderUUID, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	kept := store.Lookup(quarantined)
	_, lost := kept.LostBackend()
	assert.False(t, lost)
	assert.True(t, kept.Conflict)
	assert.True(t, kept.ConflictOwnersUnknown)
	assert.Equal(t, "backend-c", kept.Backend)
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{fixture.unrelated: "backend-b"},
	})
	assert.Equal(t, "backend-b", store.Lookup(fixture.unrelated).Backend)
	assert.Equal(t, kept.Revision(), store.Lookup(quarantined).Revision())
}

// TestRetirementRecordsAMigratedRowWithoutSetAtAsLost covers a lease whose
// row came from a v0.13 raw backend name: preparation keeps its zero SetAt,
// and the lost row must still decode as lost, with its revision, so the lease
// is closed, answered 410, and pruned instead of staying unusable forever.
func TestRetirementRecordsAMigratedRowWithoutSetAtAsLost(t *testing.T) {
	fixture := newRetirementFixture(t)
	const migrated = "00000000-0000-4000-8000-000000000405"
	row, err := encodePlacement(Placement{Backend: "backend-c", revision: 1001})
	require.NoError(t, err)
	db, err := bolt.Open(fixture.dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(migrated), row)
	}))
	require.NoError(t, db.Close())

	repair, err := OpenAttemptRepair(fixture.dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	plan, err := repair.PlanBackendRetirement("backend-c", fixture.pinC)
	require.NoError(t, err)
	require.Contains(t, plan.Facts().LostLeases, migrated)
	publishRetirementBackup(t, repair)
	result, err := repair.RetireBackend(plan, LostBackendAttestationText)
	require.NoError(t, err)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())
	inspector, err := OpenRepairInspector(fixture.dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	require.NoError(t, inspector.VerifyBackendRetirementPostcondition(result))
	require.NoError(t, inspector.Close())

	store, err := OpenStore(fixture.dbPath, freshTestProviderUUID, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	lost := store.Lookup(migrated)
	lostBackend, isLost := lost.LostBackend()
	require.True(t, isLost, "a migrated row must become a decodable lost row")
	assert.Equal(t, "backend-c", lostBackend)
	assert.NotZero(t, lost.Revision())
	assert.False(t, lost.SetAt.IsZero(), "the lost fact carries the retirement time")
}

// TestRetirementListsLostLeasesASurvivorAlsoReported pins the operator's view
// of a lease the retired backend owned while a survivor also reported it: it
// is lost, and the plan says a survivor holds a copy.
func TestRetirementListsLostLeasesASurvivorAlsoReported(t *testing.T) {
	fixture := newRetirementFixture(t)
	const contradicted = "00000000-0000-4000-8000-000000000406"
	row, err := encodePlacement(Placement{
		Backend: "backend-c", SetAt: time.Now().UTC(), Conflict: true,
		ConflictBackends: []string{"backend-b", "backend-c"}, revision: 1002,
	})
	require.NoError(t, err)
	db, err := bolt.Open(fixture.dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(contradicted), row)
	}))
	require.NoError(t, db.Close())

	repair, err := OpenAttemptRepair(fixture.dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	plan, err := repair.PlanBackendRetirement("backend-c", fixture.pinC)
	require.NoError(t, err)
	facts := plan.Facts()
	assert.Contains(t, facts.LostLeases, contradicted)
	assert.Equal(t, []string{contradicted}, facts.LostWithSurvivorCopies,
		"the owned lease without a survivor report is lost without a copy")
}

// TestRetirementBindsTheReceiptsItReclaims covers a detached lease: its
// lifecycle authority names the retired backend but it has no placement row,
// so deleting that authority reclaims its settled receipts. The plan lists
// them, its confirmation covers their bytes, and the apply deletes them.
func TestRetirementBindsTheReceiptsItReclaims(t *testing.T) {
	fixture := newRetirementFixture(t)
	receiptKey := maintenanceReceiptKey(fixture.owned, fixture.maintenance)
	settle := func(outcome MaintenanceCommandOutcome) {
		t.Helper()
		db, err := bolt.Open(fixture.dbPath, 0o600, nil)
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			pending, records, err := maintenanceCommandBuckets(tx)
			if err != nil {
				return err
			}
			command, _, createdAt, _, _, err := decodeMaintenanceCommand(records.Get(receiptKey))
			if err != nil {
				return err
			}
			settled, _, err := encodeMaintenanceSettlement(
				command, maintenanceSettlement{outcome: outcome}, createdAt, createdAt,
			)
			if err != nil {
				return err
			}
			if err := records.Put(receiptKey, settled); err != nil {
				return err
			}
			if err := pending.Delete([]byte(fixture.owned)); err != nil {
				return err
			}
			return tx.Bucket(bucketName).Delete([]byte(fixture.owned))
		}))
		require.NoError(t, db.Close())
	}
	plan := func() BackendRetirementPlan {
		t.Helper()
		repair, err := OpenAttemptRepair(fixture.dbPath, freshTestProviderUUID)
		require.NoError(t, err)
		t.Cleanup(func() { _ = repair.Close() })
		plan, err := repair.PlanBackendRetirement("backend-c", fixture.pinC)
		require.NoError(t, err)
		require.NoError(t, repair.Close())
		return plan
	}

	settle(MaintenanceOutcomeExecutionFailed)
	first := plan()
	facts := first.Facts()
	assert.Equal(t, []string{fixture.owned}, facts.ReclaimedReceiptLeases)
	assert.Contains(t, facts.LifecycleScrubbed, fixture.owned)
	assert.NotContains(t, facts.LostLeases, fixture.owned, "a detached lease has no placement to lose")
	settle(MaintenanceOutcomeInvalidState)
	assert.NotEqual(t, first.ConfirmationValue(), plan().ConfirmationValue(),
		"the confirmation covers the bytes of every receipt the apply deletes")

	repair, err := OpenAttemptRepair(fixture.dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	applied, err := repair.PlanBackendRetirement("backend-c", fixture.pinC)
	require.NoError(t, err)
	publishRetirementBackup(t, repair)
	_, err = repair.RetireBackend(applied, LostBackendAttestationText)
	require.NoError(t, err)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())
	db, err := bolt.Open(fixture.dbPath, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		_, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		assert.Nil(t, records.Get(receiptKey), "the reclaimed receipt is gone")
		assert.Nil(t, tx.Bucket(lifecycleCapabilityBucketName).Get([]byte(fixture.owned)))
		return nil
	}))
}

// TestRetirementQuarantinesAnAttemptOnlyCapability covers ENG-1119: a
// recordless attempt on backend-c wrote a usable capability whose only
// evidence is that attempt, and two survivors then both reported the lease.
// Stripping the attempt used to leave an ownerless capability the encoder
// refuses, so the whole retirement refused in dry run and apply. The lease now
// keeps its two-survivor conflict and the evidence-free quarantine sentinel.
func TestRetirementQuarantinesAnAttemptOnlyCapability(t *testing.T) {
	fixture := newRetirementFixture(t)
	const attempted = "00000000-0000-4000-8000-000000000407"
	attemptOperation := requireOperationID(t, "407")
	store, err := OpenStore(fixture.dbPath, freshTestProviderUUID, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, err)
	baseline := store.CurrentAdmissionBaseline()
	require.True(t, baseline.Valid())
	scope := requireAdmissionScope(t, store, baseline, "backend-c")
	_, applied, err := store.beginNewAttempt(
		scope, attempted, "backend-c", attemptOperation, PayloadFingerprint{},
		testBackendRequestSnapshot(t), testCallbackPair(attemptOperation),
	)
	require.NoError(t, err)
	require.True(t, applied)
	// backend-c never reports the lease; two survivors both do.
	requireConflictPlacement(t, store, attempted, "backend-a", "backend-b")
	conflict := store.Lookup(attempted)
	require.Equal(t, "backend-c", conflict.Attempt)
	require.Empty(t, conflict.Backend)
	require.Equal(t, []string{"backend-a", "backend-b", "backend-c"}, conflict.ConflictBackends)
	require.False(t, conflict.ConflictOwnersUnknown)
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		capability, err := decodeLifecycleCapability(tx.Bucket(lifecycleCapabilityBucketName).Get([]byte(attempted)))
		require.NoError(t, err)
		require.Empty(t, capability.backend)
		require.Equal(t, "backend-c", capability.attemptBackend)
		require.False(t, capability.unusable, "the durable capability names backend-c only through its attempt")
		return nil
	}))
	require.NoError(t, store.Close())

	repair, err := OpenAttemptRepair(fixture.dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	plan, err := repair.PlanBackendRetirement("backend-c", fixture.pinC)
	require.NoError(t, err, "an attempt-only capability must not refuse the retirement")
	facts := plan.Facts()
	assert.Contains(t, facts.StrippedLeases, attempted)
	assert.Contains(t, facts.LifecycleScrubbed, attempted)
	assert.NotContains(t, facts.LostLeases, attempted)
	publishRetirementBackup(t, repair)
	result, err := repair.RetireBackend(plan, LostBackendAttestationText)
	require.NoError(t, err)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())
	inspector, err := OpenRepairInspector(fixture.dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	require.NoError(t, inspector.VerifyBackendRetirementPostcondition(result))
	require.NoError(t, inspector.Close())

	store, err = OpenStore(fixture.dbPath, freshTestProviderUUID, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	kept := store.Lookup(attempted)
	_, lost := kept.LostBackend()
	assert.False(t, lost, "two survivors may hold the lease's data")
	assert.True(t, kept.Conflict)
	assert.Empty(t, kept.Backend)
	assert.Empty(t, kept.Attempt)
	assert.Equal(t, []string{"backend-a", "backend-b"}, kept.ConflictBackends)
	requireLifecycleVerdict(t, store, attempted, lifecycleIDFromOperation(t, attemptOperation), LifecycleVerdictUnusable)
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		capability, err := decodeLifecycleCapability(tx.Bucket(lifecycleCapabilityBucketName).Get([]byte(attempted)))
		require.NoError(t, err)
		assert.Equal(t, lifecycleCapability{unusable: true}, capability,
			"the capability keeps only the evidence-free quarantine sentinel")
		return nil
	}))
}

// TestRetirementQuarantinesTheAttemptOnlyCapabilityOfAnUninterpretableRow is
// the second shape of ENG-1119: a current-schema placement row Fred cannot
// interpret (here an attempt on backend-c without its exact operation
// metadata) is left exactly as it is, while its decodable capability, whose
// only evidence was an attempt on backend-c, becomes the evidence-free
// quarantine sentinel.
func TestRetirementQuarantinesTheAttemptOnlyCapabilityOfAnUninterpretableRow(t *testing.T) {
	fixture := newRetirementFixture(t)
	const garbled = "00000000-0000-4000-8000-000000000408"
	garbage := []byte(`{"schema":1,"backend":"","attempt":"backend-c","set_at":"2026-09-01T00:00:00Z","revision":1003}`)
	require.True(t, decodeRecord(garbled, garbage).unusable, "the fixture row must be uninterpretable")
	capability, err := encodeLifecycleCapability(lifecycleCapability{
		attemptBackend: "backend-c", attemptID: requireLifecycleID(t, "408"),
	})
	require.NoError(t, err)
	db, err := bolt.Open(fixture.dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(bucketName).Put([]byte(garbled), garbage); err != nil {
			return err
		}
		return tx.Bucket(lifecycleCapabilityBucketName).Put([]byte(garbled), capability)
	}))
	require.NoError(t, db.Close())

	repair, err := OpenAttemptRepair(fixture.dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { _ = repair.Close() })
	plan, err := repair.PlanBackendRetirement("backend-c", fixture.pinC)
	require.NoError(t, err, "an attempt-only capability must not refuse the retirement")
	facts := plan.Facts()
	assert.Contains(t, facts.UninterpretableLeases, garbled)
	assert.Contains(t, facts.LifecycleScrubbed, garbled)
	assert.NotContains(t, facts.LostLeases, garbled)
	assert.NotContains(t, facts.StrippedLeases, garbled)
	publishRetirementBackup(t, repair)
	result, err := repair.RetireBackend(plan, LostBackendAttestationText)
	require.NoError(t, err)
	require.NoError(t, repair.Sync())
	require.NoError(t, repair.Close())
	inspector, err := OpenRepairInspector(fixture.dbPath, freshTestProviderUUID)
	require.NoError(t, err)
	require.NoError(t, inspector.VerifyBackendRetirementPostcondition(result))
	require.NoError(t, inspector.Close())

	db, err = bolt.Open(fixture.dbPath, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		assert.Equal(t, garbage, tx.Bucket(bucketName).Get([]byte(garbled)),
			"an uninterpretable placement is left exactly as it is")
		capability, err := decodeLifecycleCapability(tx.Bucket(lifecycleCapabilityBucketName).Get([]byte(garbled)))
		require.NoError(t, err)
		assert.Equal(t, lifecycleCapability{unusable: true}, capability)
		return nil
	}))
}
