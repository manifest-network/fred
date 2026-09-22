package placement

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

type quarantineReaffirmationFixture struct {
	coordinatorFixture
	reconciliation *ReconciliationCoordinator
	row            backend.ProvisionInfo
}

func newQuarantineReaffirmationFixture(t *testing.T, confirmed bool, kinds ...operation.Kind) quarantineReaffirmationFixture {
	t.Helper()
	store := newTestStore(t)
	requireAdmissionBaseline(t, store, "backend-a", "backend-b")
	coordinator, err := NewOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	leaseClaim := coordinator.operations.TryClaimLeaseNow(reconciliationSweepLease)
	require.True(t, leaseClaim.Acquired())
	kind := operation.KindProvision
	if len(kinds) != 0 {
		kind = kinds[0]
	}
	var initiation operation.Initiation
	switch kind {
	case operation.KindProvision:
		requested := testProvisionInitiation(t, reconciliationSweepLease, "tenant-test", "backend-a")
		started := coordinator.operations.TryInitiateProvisionClaimed(leaseClaim.Claim(), requested)
		require.True(t, started.Started())
		initiation = started.Capability()
		attempt := requireTypedAttempt(t, store, reconciliationSweepLease, "backend-a", initiation.ID())
		dispatch, dispatchErr := coordinator.joinProvisionDispatch(initiation, attempt)
		require.NoError(t, dispatchErr)
		call := requireProvisionCall(t, coordinator, dispatch)
		contended := coordinator.tryClaimCallback(reconciliationSweepLease, initiation.ID())
		require.True(t, contended.Claimed())
		require.True(t, coordinator.completeProvision(call, backend.ConservativeProvisionCallOutcome(nil)).Superseded())
		require.True(t, coordinator.releaseCallback(contended.Claim()))
	case operation.KindRestore:
		const source = "018f47a2-8b1c-7def-8123-456789abcdee"
		sourceAttempt := requireTypedAttempt(t, store, source, "backend-a", requireOperationID(t, "4301"))
		applied, sourceErr := confirmAttemptForTest(store, sourceAttempt)
		require.NoError(t, sourceErr)
		require.True(t, applied)
		started := coordinator.operations.TryInitiateRestoreClaimed(leaseClaim.Claim(),
			testRestoreInitiation(t, reconciliationSweepLease, "tenant-test"))
		require.True(t, started.Started())
		initiation = started.Capability()
		claim, claimErr := store.beginAuthorizedRestore(store.CurrentAdmissionBaseline(),
			store.Lookup(source).RecordRevision(), reconciliationSweepLease, initiation.ID(),
			testBackendRequestSnapshot(t), testCallbackPair(initiation.ID()))
		require.NoError(t, claimErr)
		dispatch, dispatchErr := coordinator.joinRestoreDispatch(initiation, claim)
		require.NoError(t, dispatchErr)
		bound, boundOK := coordinator.bindRestoreBackend(dispatch)
		require.True(t, boundOK)
		call := requireRestoreCall(t, coordinator, bound)
		contended := coordinator.tryClaimCallback(reconciliationSweepLease, initiation.ID())
		require.True(t, contended.Claimed())
		require.True(t, coordinator.completeRestore(call, backend.ConservativeRestoreCallOutcome(nil)).Superseded())
		require.True(t, coordinator.releaseCallback(contended.Claim()))
	default:
		t.Fatalf("unsupported fixture operation %v", kind)
	}
	require.True(t, coordinator.operations.ReleaseLease(leaseClaim.Claim()))
	if confirmed {
		claim, claimed, claimErr := store.claimAttempt(reconciliationSweepLease, initiation.ID())
		require.NoError(t, claimErr)
		require.True(t, claimed)
		applied, confirmErr := store.confirmClaimedAttempt(claim)
		require.NoError(t, confirmErr)
		require.True(t, applied)
	}
	execution := bindExecutionForTest(t, coordinator, executionRuntime("backend-a", "backend-b"))
	reconciliation, err := reconciliationCoordinatorWithReaderForTest(t, execution, &reconciliationSweepReader{})
	require.NoError(t, err)
	quarantine, err := reconciliation.BeginSweep()
	require.NoError(t, err)
	// A failed retention endpoint leaves the successfully read provision as
	// conservative rejected membership, exactly as the collector adapter does.
	require.NoError(t, quarantine.collection.RecordUntrusted("backend-a", []string{reconciliationSweepLease}))
	require.NoError(t, quarantine.SealInventory())
	_, err = quarantine.Project(ReconciliationProjection{
		UntrustedPositives: map[string][]string{reconciliationSweepLease: {"backend-a"}},
	})
	require.NoError(t, err)
	quarantine.End()
	require.Equal(t, StateUnusable, store.Lookup(reconciliationSweepLease).State())
	blocked := coordinator.tryClaimCallback(reconciliationSweepLease, initiation.ID())
	require.False(t, blocked.Claimed())
	require.ErrorIs(t, blocked.Err(), ErrOperationSettlementGenerationUnavailable)
	return quarantineReaffirmationFixture{
		coordinatorFixture: coordinatorFixture{store: store, coordinator: coordinator, initiation: initiation},
		reconciliation:     reconciliation,
		row: backend.ProvisionInfo{
			LeaseUUID: reconciliationSweepLease, BackendName: "backend-a", Tenant: "tenant-test",
			ProviderUUID: freshTestProviderUUID, Status: backend.ProvisionStatusReady,
			LifecycleGeneration: &backend.LifecycleGenerationObservation{
				Kind: backend.LifecycleGenerationTyped, ID: initiation.ID().String(),
			},
		},
	}
}

func (fixture quarantineReaffirmationFixture) collect(t *testing.T, scenario string) *ReconciliationSweep {
	t.Helper()
	sweep, err := fixture.reconciliation.BeginSweep()
	require.NoError(t, err)
	t.Cleanup(sweep.End)
	require.True(t, sweep.WasInFlight(reconciliationSweepLease))
	row := fixture.row
	switch scenario {
	case "wrong generation":
		row.LifecycleGeneration = &backend.LifecycleGenerationObservation{
			Kind: backend.LifecycleGenerationTyped, ID: "123e4567-e89b-42d3-a456-426614174000",
		}
	case "unknown generation":
		row.LifecycleGeneration = nil
	case "legacy generation":
		row.LifecycleGeneration = &backend.LifecycleGenerationObservation{Kind: backend.LifecycleGenerationLegacy}
	case "wrong tenant":
		row.Tenant = "foreign-tenant"
	case "wrong provider":
		row.ProviderUUID = "foreign-provider"
	case "missing principal":
		row.Tenant = ""
	}
	for _, name := range []string{"backend-a", "backend-b"} {
		if name == "backend-b" && scenario == "missing peer" {
			continue
		}
		var rows []backend.ProvisionInfo
		var retentions []string
		if name == "backend-a" {
			if scenario == "retention only" {
				retentions = []string{reconciliationSweepLease}
			} else {
				rows = []backend.ProvisionInfo{row}
			}
		}
		id := testBackendStorageID(name)
		if name == "backend-b" && scenario == "changed peer identity" {
			id = testBackendStorageID("replacement")
		}
		require.NoError(t, sweep.RecordProvision(name, id, rows))
		require.NoError(t, sweep.RecordRetention(name, id, retentions))
	}
	require.NoError(t, sweep.SealInventory())
	return sweep
}

func TestQuarantineReaffirmationPreservesExactInFlightGeneration(t *testing.T) {
	for _, kind := range []operation.Kind{operation.KindProvision, operation.KindRestore} {
		for _, confirmed := range []bool{false, true} {
			for _, submitted := range []bool{false, true} {
				t.Run(kind.String()+"/"+map[bool]string{false: "attempt", true: "confirmed"}[confirmed]+"/"+
					map[bool]string{false: "omitted policy", true: "submitted policy"}[submitted], func(t *testing.T) {
					fixture := newQuarantineReaffirmationFixture(t, confirmed, kind)
					before := fixture.store.Lookup(reconciliationSweepLease)
					lifecycleBefore := fixture.store.lifecycleCache[reconciliationSweepLease]
					sweep := fixture.collect(t, "exact")
					projection := ReconciliationProjection{}
					if submitted {
						projection.Placements = map[string]string{reconciliationSweepLease: "backend-a"}
					}
					_, err := sweep.Project(projection)
					require.NoError(t, err)
					after := fixture.store.Lookup(reconciliationSweepLease)
					require.False(t, after.Conflict, "a complete exact read must release the callback/quarantine cycle")
					require.Equal(t, lifecycleBefore, fixture.store.lifecycleCache[reconciliationSweepLease])
					before.Conflict, before.untrustedPositive = false, false
					before.ConflictBackends = nil
					before.revision, before.recordRevision = after.revision, after.recordRevision
					require.Equal(t, before, after, "reaffirmation must preserve every original operation field")
					claim := fixture.coordinator.tryClaimCallback(reconciliationSweepLease, fixture.initiation.ID())
					require.True(t, claim.Claimed(), "%v", claim.Err())
					want := OperationGenerationAttempt
					if confirmed {
						want = OperationGenerationConfirmed
					}
					require.Equal(t, want, claim.Claim().Generation())
					require.True(t, fixture.coordinator.releaseCallback(claim.Claim()))
				})
			}
		}
	}
}

func TestQuarantineReaffirmationRequiresExactCompleteLineage(t *testing.T) {
	for _, confirmed := range []bool{false, true} {
		for _, scenario := range []string{
			"wrong generation", "unknown generation", "legacy generation", "wrong tenant", "wrong provider",
			"missing principal", "missing peer", "changed peer identity", "unbound peer identity", "retention only",
			"unknown historical owners", "multiple historical owners", "unusable lifecycle", "missing attempt marker",
		} {
			if confirmed && scenario == "missing attempt marker" {
				continue
			}
			t.Run(map[bool]string{false: "attempt", true: "confirmed"}[confirmed]+"/"+scenario, func(t *testing.T) {
				fixture := newQuarantineReaffirmationFixture(t, confirmed)
				switch scenario {
				case "unknown historical owners", "multiple historical owners":
					record := fixture.store.cache[reconciliationSweepLease]
					if scenario == "unknown historical owners" {
						record.ConflictOwnersUnknown = true
					} else {
						record.ConflictBackends = append(record.ConflictBackends, "backend-b")
					}
					fixture.store.cache[reconciliationSweepLease] = record
				case "unusable lifecycle", "missing attempt marker":
					capability := fixture.store.lifecycleCache[reconciliationSweepLease]
					if scenario == "unusable lifecycle" {
						capability.unusable = true
					} else {
						capability.attemptBackend = ""
					}
					fixture.store.lifecycleCache[reconciliationSweepLease] = capability
				case "unbound peer identity":
					delete(fixture.store.backendStorageIDs, "backend-b")
					_, err := fixture.reconciliation.BeginSweep()
					require.ErrorIs(t, err, ErrBackendStorageIdentityUnbound)
					require.Equal(t, StateUnusable, fixture.store.Lookup(reconciliationSweepLease).State())
					return
				}
				before := fixture.store.Lookup(reconciliationSweepLease)
				lifecycleBefore := fixture.store.lifecycleCache[reconciliationSweepLease]
				sweep := fixture.collect(t, scenario)
				_, err := sweep.Project(ReconciliationProjection{})
				if scenario == "changed peer identity" {
					require.ErrorIs(t, err, ErrBackendStorageIdentityMismatch)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, before, fixture.store.Lookup(reconciliationSweepLease))
				require.Equal(t, lifecycleBefore, fixture.store.lifecycleCache[reconciliationSweepLease])
				require.False(t, fixture.coordinator.tryClaimCallback(reconciliationSweepLease, fixture.initiation.ID()).Claimed())
			})
		}
	}
}

func TestQuarantineReaffirmationProofRejectsZeroForeignAndSupersededInventory(t *testing.T) {
	fixture := newQuarantineReaffirmationFixture(t, false)
	sweep := fixture.collect(t, "exact")
	fixture.store.mu.Lock()
	proof := fixture.store.observeQuarantineReaffirmationLocked(sweep.fence, sweep.sealed, reconciliationSweepLease)
	_, valid := proof.placementLocked(fixture.store)
	_, zero := (quarantineReaffirmation{}).placementLocked(fixture.store)
	fixture.store.mu.Unlock()
	require.True(t, valid)
	require.False(t, zero)
	foreign := newTestStore(t)
	foreign.mu.Lock()
	_, valid = proof.placementLocked(foreign)
	foreign.mu.Unlock()
	require.False(t, valid)
	_, err := sweep.Project(ReconciliationProjection{})
	require.NoError(t, err)
	fixture.store.mu.Lock()
	_, valid = proof.placementLocked(fixture.store)
	fixture.store.mu.Unlock()
	require.False(t, valid, "a committed record transition consumes the old proof")
	sweep.End()
	next, err := fixture.reconciliation.BeginSweep()
	require.NoError(t, err)
	defer next.End()
	fixture.store.mu.Lock()
	_, valid = proof.placementLocked(fixture.store)
	fixture.store.mu.Unlock()
	require.False(t, valid)
}

func TestQuarantinedOperationRecoversAfterStoreReopen(t *testing.T) {
	for _, kind := range []operation.Kind{operation.KindProvision, operation.KindRestore} {
		t.Run(kind.String(), func(t *testing.T) {
			fixture := newQuarantineReaffirmationFixture(t, false, kind)
			before := fixture.store.Lookup(reconciliationSweepLease)
			path := fixture.store.db.Path()
			require.NoError(t, fixture.store.Close())
			store, err := newStoreForTest(path)
			require.NoError(t, err)
			defer func() { require.NoError(t, store.Close()) }()
			require.Equal(t, StateUnusable, store.Lookup(reconciliationSweepLease).State())
			coordinator, err := NewOperationCoordinator(store, operation.NewRegistry())
			require.NoError(t, err)
			execution := bindExecutionForTest(t, coordinator, executionRuntime("backend-a", "backend-b"))
			reconciliation, err := reconciliationCoordinatorWithReaderForTest(t, execution, &reconciliationSweepReader{})
			require.NoError(t, err)
			sweep, err := reconciliation.BeginSweep()
			require.NoError(t, err)
			defer sweep.End()
			require.False(t, sweep.WasInFlight(reconciliationSweepLease))
			for _, name := range []string{"backend-a", "backend-b"} {
				var rows []backend.ProvisionInfo
				if name == "backend-a" {
					rows = []backend.ProvisionInfo{fixture.row}
				}
				require.NoError(t, sweep.RecordProvision(name, testBackendStorageID(name), rows))
				require.NoError(t, sweep.RecordRetention(name, testBackendStorageID(name), nil))
			}
			require.NoError(t, sweep.SealInventory())
			_, err = sweep.Project(ReconciliationProjection{Placements: map[string]string{reconciliationSweepLease: "backend-a"}})
			require.NoError(t, err)
			after := store.Lookup(reconciliationSweepLease)
			require.Equal(t, StateConfirmed, after.State())
			require.Empty(t, after.Attempt, "with no live Registry transition, ordinary exact inventory can confirm the attempt")
			require.Equal(t, before.attemptOperationID, after.attemptOperationID)
			require.Equal(t, before.attemptOperationKind, after.attemptOperationKind)
			require.Equal(t, before.attemptRestoreSourceLeaseUUID, after.attemptRestoreSourceLeaseUUID)
			require.Equal(t, before.attemptPayloadFingerprint, after.attemptPayloadFingerprint)
			require.Equal(t, before.attemptRequestSnapshot, after.attemptRequestSnapshot)
			require.Equal(t, before.attemptCallbackPair, after.attemptCallbackPair)
			claim := coordinator.tryClaimRecoveryCallback(reconciliationSweepLease, fixture.initiation.ID())
			require.True(t, claim.Claimed(), "%v", claim.Err())
			require.Equal(t, OperationGenerationConfirmed, claim.Claim().Generation())
			require.True(t, coordinator.releaseRecoveryCallback(claim.Claim()))
		})
	}
}
