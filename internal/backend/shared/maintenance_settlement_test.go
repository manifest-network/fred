package shared

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

type testMaintenanceMutation struct {
	run func(context.Context) error
}

func bindTestMaintenanceMutation(
	t *testing.T,
	settlement *MaintenanceSettlement,
	classify func(MaintenancePhysicalSubject) (MaintenancePhysicalEvidence, error),
) {
	t.Helper()
	if classify == nil {
		classify = func(subject MaintenancePhysicalSubject) (MaintenancePhysicalEvidence, error) {
			release, ok := subject.TargetRelease()
			if !ok {
				return MaintenancePhysicalEvidence{}, errors.New("missing maintenance target release")
			}
			ids, services := testPhysicalProjection(release)
			return NewMaintenanceTargetReady(subject, ids, services)
		}
	}
	err := BindMaintenanceSubstrateExecutor(
		settlement,
		func(ctx context.Context, _ string) (context.Context, func(), error) {
			return ctx, func() {}, nil
		},
		func(context.Context, string, error) error { return nil },
		func(runner substratemutation.Runner, _ MaintenancePhysicalSubject) testMaintenanceMutation {
			return testMaintenanceMutation{run: func(ctx context.Context) error {
				return runner.Step(ctx, "test maintenance mutation", func(context.Context) error { return nil })
			}}
		},
		func(ctx context.Context, mutation testMaintenanceMutation, _ MaintenancePhysicalSubject) error {
			return mutation.run(ctx)
		},
		func(_ context.Context, subject MaintenancePhysicalSubject) (MaintenancePhysicalEvidence, error) {
			return classify(subject)
		},
	)
	require.NoError(t, err)
}

func TestMaintenanceExecutionStartInvalidatesPreEffectTargetCopies(t *testing.T) {
	fixture := beginBoundMaintenance(t, "execution-start-copy")
	target := fixture.appendAndBind(t)
	bindTestMaintenanceMutation(t, fixture.settlement, nil)
	copied := target

	execution, err := fixture.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	require.NotNil(t, execution.settlement)
	_, err = fixture.settlement.StartMaintenanceExecution(copied)
	require.Error(t, err)
	_, err = fixture.settlement.RefuseMaintenanceExecution(copied)
	require.Error(t, err)

	current, found, err := fixture.settlement.GetMaintenanceIntent(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	require.True(t, found)
	assert.False(t, current.entry.EffectNotStarted)
}

func TestMaintenanceExecutionPhaseMissingBitFailsSafeAndUnknownFieldIsRejected(t *testing.T) {
	fixture := beginBoundMaintenance(t, "execution-phase-schema")
	require.True(t, fixture.intent.entry.EffectNotStarted)

	legacy := cloneMaintenanceIntentEntry(fixture.intent.entry)
	legacy.EffectNotStarted = false
	data, err := marshalMaintenanceIntent(legacy)
	require.NoError(t, err)
	decoded, err := decodeMaintenanceIntent([]byte(legacy.LeaseUUID), data)
	require.NoError(t, err)
	assert.False(t, decoded.entry.EffectNotStarted,
		"a missing legacy bit must be treated as potentially started")

	var object map[string]any
	require.NoError(t, json.Unmarshal(data, &object))
	object["unexpected_execution_phase"] = true
	data, err = json.Marshal(object)
	require.NoError(t, err)
	_, err = decodeMaintenanceIntent([]byte(legacy.LeaseUUID), data)
	require.Error(t, err)
}

func TestMaintenanceStartedPhaseRecoversTypedPhysicalOutcomeAfterReopen(t *testing.T) {
	for _, test := range []struct {
		name    string
		success bool
	}{
		{name: "absent becomes failure"},
		{name: "present becomes success", success: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := beginBoundMaintenance(t, "started-reopen-"+test.name)
			target := fixture.appendAndBind(t)
			bindTestMaintenanceMutation(t, fixture.settlement, nil)
			_, err := fixture.settlement.StartMaintenanceExecution(target)
			require.NoError(t, err)

			require.NoError(t, fixture.stores.callbacks.Close())
			fixture.stores.callbacks = nil
			require.NoError(t, fixture.stores.releases.Close())
			fixture.stores.releases = nil
			callbacks, err := OpenIdentityBoundCallbackStore(
				CallbackStoreConfig{DBPath: fixture.stores.callbackPath},
				fixture.stores.storage, fixture.stores.gate,
			)
			require.NoError(t, err)
			fixture.stores.callbacks = callbacks
			releases, err := OpenIdentityBoundReleaseStore(
				ReleaseStoreConfig{DBPath: fixture.stores.releasePath},
				fixture.stores.storage, fixture.stores.gate,
			)
			require.NoError(t, err)
			fixture.stores.releases = releases
			reopened, err := NewMaintenanceSettlement(callbacks, releases)
			require.NoError(t, err)
			inventoryChecked := false
			bindTestMaintenanceMutation(t, reopened, func(subject MaintenancePhysicalSubject) (MaintenancePhysicalEvidence, error) {
				inventoryChecked = true
				if !test.success {
					return NewMaintenanceTargetAbsent(subject)
				}
				release, ok := subject.TargetRelease()
				if !ok {
					return MaintenancePhysicalEvidence{}, errors.New("missing maintenance target release")
				}
				ids, services := testPhysicalProjection(release)
				return NewMaintenanceTargetReady(subject, ids, services)
			})
			intent, found, err := reopened.GetMaintenanceIntent(fixture.intent.LeaseUUID())
			require.NoError(t, err)
			require.True(t, found)
			assert.False(t, intent.entry.EffectNotStarted)

			coordinator := newTestRecoveryCoordinator(t, nil, reopened, nil)
			var outcome MaintenanceExecutionOutcome
			acquired, err := coordinator.WithLease(
				context.Background(), intent.LeaseUUID(),
				func(scope LeaseRecoveryScope) error {
					outcome, err = reopened.RecoverMaintenanceExecution(
						context.Background(), scope, intent,
					)
					return err
				},
			)
			require.NoError(t, err)
			require.True(t, acquired)
			if test.success {
				success, ok := outcome.(MaintenanceExecutionSuccess)
				require.True(t, ok)
				active, err := reopened.ActivateMaintenance(success)
				require.NoError(t, err)
				require.True(t, active.Valid())
			} else {
				failure, ok := outcome.(MaintenanceExecutionFailure)
				require.True(t, ok)
				failed, err := reopened.FailMaintenance(
					failure, backend.ReasonInternal, "strict inventory proved absence",
				)
				require.NoError(t, err)
				require.True(t, failed.Valid())
			}
			assert.True(t, inventoryChecked)
		})
	}
}

func TestMaintenanceExecutionRejectsOutcomeFromAnotherStartedGeneration(t *testing.T) {
	fixture := beginBoundMaintenance(t, "execution-lineage-a")
	targetA := fixture.appendAndBind(t)
	bindTestMaintenanceMutation(t, fixture.settlement, nil)

	leaseB := testLeaseUUID("bound-maintenance-execution-lineage-b")
	require.NoError(t, fixture.stores.releases.appendActive(leaseB, validRuntimeAuthorityRelease()))
	activeB, sourceB, err := fixture.settlement.ClaimLatestActive(leaseB)
	require.NoError(t, err)
	targetBTemplate := cloneRelease(activeB)
	targetBTemplate.Version = 0
	targetBTemplate.Status = "deploying"
	targetBTemplate.CreatedAt = time.Now()
	candidateB := maintenanceCandidateForSettlement(
		t, fixture.settlement, sourceB, targetBTemplate, newTestMaintenanceID(t),
	)
	admissionB, err := fixture.settlement.BeginMaintenanceIntent(candidateB)
	require.NoError(t, err)
	appendB, err := fixture.settlement.StartMaintenanceAppend(
		createdMaintenanceDispatch(t, admissionB),
	)
	require.NoError(t, err)
	targetB, err := fixture.settlement.AppendMaintenance(appendB)
	require.NoError(t, err)
	targetB, err = fixture.settlement.BindMaintenanceIntentTarget(targetB)
	require.NoError(t, err)

	executionA, err := fixture.settlement.StartMaintenanceExecution(targetA)
	require.NoError(t, err)
	executionB, err := fixture.settlement.StartMaintenanceExecution(targetB)
	require.NoError(t, err)
	acceptedA := fixture.settlement.ExecuteMaintenance(context.Background(), executionA)
	require.IsType(t, MaintenanceExecutionSuccess{}, acceptedA)
	forgedB := executionB
	forgedB.started = executionA.started
	rejectedB := fixture.settlement.ExecuteMaintenance(context.Background(), forgedB)
	require.IsType(t, MaintenanceExecutionAmbiguous{}, rejectedB)
}

type boundMaintenanceFixture struct {
	stores     operationHandoffStores
	settlement *MaintenanceSettlement
	intent     MaintenanceIntentClaim
	dispatch   MaintenanceIntentDispatch
	append     MaintenanceAppendClaim
}

func maintenanceCandidateForSettlement(
	t *testing.T,
	settlement *MaintenanceSettlement,
	source MaintenanceSourceClaim,
	target Release,
	id MaintenanceID,
) MaintenanceIntentCandidate {
	t.Helper()
	authority, ok := target.RuntimeIdentity()
	require.True(t, ok)
	request, err := settlement.NewMaintenanceRequestAuthority(
		id, MaintenanceIntentRestart, source.LeaseUUID(),
		authority.LifecycleCallbackURL(), nil,
	)
	require.NoError(t, err)
	candidate, err := settlement.NewMaintenanceIntentCandidate(request, source, target)
	require.NoError(t, err)
	return candidate
}

func beginBoundMaintenance(t *testing.T, name string) boundMaintenanceFixture {
	t.Helper()
	stores := openOperationHandoffStores(t, "docker-a")
	settlement, err := NewMaintenanceSettlement(stores.callbacks, stores.releases)
	require.NoError(t, err)
	leaseUUID := testLeaseUUID("bound-maintenance-" + name)
	require.NoError(t, stores.releases.appendActive(leaseUUID, validRuntimeAuthorityRelease()))
	active, source, err := settlement.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	authority, ok := target.RuntimeIdentity()
	require.True(t, ok)
	request, err := settlement.NewMaintenanceRequestAuthority(
		newTestMaintenanceID(t), MaintenanceIntentRestart, leaseUUID,
		authority.LifecycleCallbackURL(), nil,
	)
	require.NoError(t, err)
	candidate, err := settlement.NewMaintenanceIntentCandidate(request, source, target)
	require.NoError(t, err)
	admission, err := settlement.BeginMaintenanceIntent(candidate)
	require.NoError(t, err)
	appendClaim, err := settlement.StartMaintenanceAppend(
		createdMaintenanceDispatch(t, admission),
	)
	require.NoError(t, err)
	return boundMaintenanceFixture{
		stores: stores, settlement: settlement,
		intent: appendClaim.Intent(), dispatch: createdMaintenanceDispatch(t, admission),
		append: appendClaim,
	}
}

func TestMaintenanceSettlementConsumesFailedSuccessorOverExactActivePredecessor(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	_, active, failed := settleActivePredecessorThenFailedSuccessorForClose(
		t, stores, "maintenance-failed-successor",
	)
	settlement, err := NewMaintenanceSettlement(stores.callbacks, stores.releases)
	require.NoError(t, err)
	sourceRelease, source, err := settlement.ClaimLatestActive(failed.LeaseUUID())
	require.NoError(t, err)
	target := cloneRelease(sourceRelease)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	candidate := maintenanceCandidateForSettlement(
		t, settlement, source, target, newTestMaintenanceID(t),
	)

	admission, err := settlement.BeginMaintenanceIntent(candidate)
	require.NoError(t, err)
	require.Equal(t, MaintenanceIntentAdmissionCreated, admission.Disposition())
	dispatch := createdMaintenanceDispatch(t, admission)
	assert.Equal(t, active.Version(), dispatch.intent.SourceRelease().Version())
	assert.NotEqual(t, failed.OperationID(), dispatch.intent.TargetRelease().OperationID)
}

func (fixture *boundMaintenanceFixture) appendAndBind(
	t *testing.T,
) MaintenanceReleaseClaim {
	t.Helper()
	target, err := fixture.settlement.AppendMaintenance(fixture.append)
	require.NoError(t, err)
	target, err = fixture.settlement.BindMaintenanceIntentTarget(target)
	require.NoError(t, err)
	fixture.intent = target.Intent()
	return target
}

func TestBindMaintenanceIntentTargetReturnsTheOnlyExecutableTarget(t *testing.T) {
	fixture := beginBoundMaintenance(t, "bound-target-is-executable")
	stale, err := fixture.settlement.AppendMaintenance(fixture.append)
	require.NoError(t, err)
	bindTestMaintenanceMutation(t, fixture.settlement, nil)

	_, err = fixture.settlement.StartMaintenanceExecution(stale)
	require.ErrorContains(t, err, "bound not-started",
		"an unbound target must not cross the physical-effect boundary")

	bound, err := fixture.settlement.BindMaintenanceIntentTarget(stale)
	require.NoError(t, err)
	require.True(t, bound.Valid())
	require.True(t, bound.Intent().Valid())

	_, err = fixture.settlement.StartMaintenanceExecution(bound)
	require.NoError(t, err,
		"binding must return the refreshed target capability rather than a detached intent")
}

func TestMaintenanceSourceSnapshotIsExactDetachedAndOpenStoreBound(t *testing.T) {
	fixture := beginBoundMaintenance(t, "source-snapshot")

	snapshot, err := fixture.settlement.SnapshotMaintenanceSource(fixture.intent)
	require.NoError(t, err)
	require.True(t, snapshot.Valid())
	assert.Equal(t, fixture.intent.LeaseUUID(), snapshot.LeaseUUID())
	assert.Equal(t, fixture.intent.MaintenanceID(), snapshot.MaintenanceID())

	first := snapshot.Release()
	require.NotEmpty(t, first.Items)
	first.Items[0].SKU = "caller-mutated"
	first.Manifest[0] ^= 0xff
	second := snapshot.Release()
	assert.NotEqual(t, "caller-mutated", second.Items[0].SKU)
	assert.NotEqual(t, first.Manifest, second.Manifest)

	other := beginBoundMaintenance(t, "source-snapshot-other-pair")
	_, err = other.settlement.SnapshotMaintenanceSource(fixture.intent)
	require.Error(t, err)

	require.NoError(t, fixture.stores.releases.Close())
	fixture.stores.releases = nil
	assert.False(t, snapshot.Valid(), "closing the exact source journal invalidates the snapshot")
}

func TestMaintenanceSettlementRejectsZeroAndCrossPairAuthority(t *testing.T) {
	_, err := NewMaintenanceSettlement(nil, nil)
	require.Error(t, err)

	fixtureA := beginBoundMaintenance(t, "pair-a")
	fixtureB := beginBoundMaintenance(t, "pair-b")
	_, err = NewMaintenanceSettlement(fixtureA.stores.callbacks, fixtureB.stores.releases)
	require.Error(t, err)

	target := fixtureA.appendAndBind(t)
	active := activateMaintenanceOutcomeForTest(t, fixtureA.settlement, target)
	_, err = resolveMaintenanceSuccessForTest(fixtureB.settlement, active)
	require.Error(t, err)

	_, err = resolveMaintenanceSuccessForTest(fixtureA.settlement,
		MaintenanceReleaseActive{})

	require.Error(t, err)
	claims, err := fixtureA.stores.callbacks.listMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
}

func TestMaintenanceCapabilitiesBindExactCoordinatorWithSameStores(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-maintenance-exact-coordinator")
	first, err := NewMaintenanceSettlement(stores.callbacks, stores.releases)
	require.NoError(t, err)
	other, err := NewMaintenanceSettlement(stores.callbacks, stores.releases)
	require.NoError(t, err)

	leaseUUID := testLeaseUUID("maintenance-same-stores")
	require.NoError(t, stores.releases.appendActive(leaseUUID, validRuntimeAuthorityRelease()))
	active, source, err := first.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	_, otherSource, err := other.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	maintenanceID := newTestMaintenanceID(t)
	authority, ok := target.RuntimeIdentity()
	require.True(t, ok)
	request, err := first.NewMaintenanceRequestAuthority(
		maintenanceID, MaintenanceIntentRestart, leaseUUID,
		authority.LifecycleCallbackURL(), nil,
	)
	require.NoError(t, err)
	_, err = other.NewMaintenanceIntentCandidate(request, otherSource, target)
	require.ErrorContains(t, err, "request authority was not minted by this journal pair")
	candidate, err := first.NewMaintenanceIntentCandidate(request, source, target)
	require.NoError(t, err)
	_, err = other.BeginMaintenanceIntent(candidate)
	require.ErrorContains(t, err, "candidate was not minted by this journal pair")

	admission, err := first.BeginMaintenanceIntent(candidate)
	require.NoError(t, err)
	dispatch := createdMaintenanceDispatch(t, admission)
	_, err = other.StartMaintenanceAppend(dispatch)
	require.ErrorContains(t, err, "dispatch was not minted by this journal pair")
	require.ErrorContains(t, other.CancelMaintenanceIntent(dispatch), "dispatch was not minted by this journal pair")

	appendClaim, err := first.StartMaintenanceAppend(dispatch)
	require.NoError(t, err)
	_, err = other.AppendMaintenance(appendClaim)
	require.ErrorContains(t, err, "another journal pair")
	targetClaim, err := first.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	_, err = other.BindMaintenanceIntentTarget(targetClaim)
	require.ErrorContains(t, err, "not issued by this journal pair")

	targetClaim, err = first.BindMaintenanceIntentTarget(targetClaim)
	require.NoError(t, err)
	_, err = other.ProveMaintenanceActive(targetClaim.Intent())
	require.ErrorContains(t, err, "not minted by this journal pair")
	_, err = other.StartMaintenanceExecution(targetClaim)
	require.ErrorContains(t, err, "pre-effect phase")

	bindTestMaintenanceMutation(t, first, nil)
	execution, err := first.StartMaintenanceExecution(targetClaim)
	require.NoError(t, err)
	outcome := first.ExecuteMaintenance(context.Background(), execution)
	success, ok := outcome.(MaintenanceExecutionSuccess)
	require.True(t, ok)
	proof, err := first.ActivateMaintenance(success)
	require.NoError(t, err)
	_, err = resolveMaintenanceSuccessForTest(other, proof)
	require.ErrorContains(t, err, "not minted by this journal pair")

	failure := beginBoundMaintenance(t, "same-store-failure-proof")
	failureOther, err := NewMaintenanceSettlement(failure.stores.callbacks, failure.stores.releases)
	require.NoError(t, err)
	failureTarget := failure.appendAndBind(t)
	refused, err := failure.settlement.RefuseMaintenanceExecution(failureTarget)
	require.NoError(t, err)
	failed, err := failure.settlement.FailMaintenance(
		refused, backend.ReasonUpdateFailed, "refused",
	)
	require.NoError(t, err)
	_, err = resolveMaintenanceFailureForTest(failureOther, failed, "refused")
	require.ErrorContains(t, err, "not minted by this journal pair")
}

func TestMaintenanceAdmissionRejectsCrossPairSourceAndCandidate(t *testing.T) {
	storesA := openOperationHandoffStores(t, "docker-a")
	storesB := openOperationHandoffStores(t, "docker-a")
	settlementA, err := NewMaintenanceSettlement(storesA.callbacks, storesA.releases)
	require.NoError(t, err)
	settlementB, err := NewMaintenanceSettlement(storesB.callbacks, storesB.releases)
	require.NoError(t, err)

	leaseUUID := testLeaseUUID("maintenance-cross-pair-admission")
	base := validRuntimeAuthorityRelease()
	require.NoError(t, storesA.releases.appendActive(leaseUUID, base))
	require.NoError(t, storesB.releases.appendActive(leaseUUID, base))
	activeA, sourceA, err := settlementA.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	_, sourceB, err := settlementB.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(activeA)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	authority, ok := target.RuntimeIdentity()
	require.True(t, ok)
	requestA, err := settlementA.NewMaintenanceRequestAuthority(
		newTestMaintenanceID(t), MaintenanceIntentRestart, leaseUUID,
		authority.LifecycleCallbackURL(), nil,
	)
	require.NoError(t, err)

	_, err = settlementA.NewMaintenanceIntentCandidate(requestA, sourceB, target)
	require.ErrorContains(t, err, "source release was not claimed by this journal pair")
	candidateA, err := settlementA.NewMaintenanceIntentCandidate(requestA, sourceA, target)
	require.NoError(t, err)
	_, err = settlementB.BeginMaintenanceIntent(candidateA)
	require.ErrorContains(t, err, "candidate was not minted by this journal pair")

	admissionA, err := settlementA.BeginMaintenanceIntent(candidateA)
	require.NoError(t, err)
	claimA, found, err := settlementA.GetMaintenanceIntent(leaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, claimA.Valid())
	_, err = settlementB.ProveMaintenanceFailure(claimA)
	require.ErrorContains(t, err, "not minted by this journal pair")
	dispatchA, ok := admissionA.CreatedDispatch()
	require.True(t, ok)
	_, err = settlementB.StartMaintenanceAppend(dispatchA)
	require.ErrorContains(t, err, "dispatch was not minted by this journal pair")
}

func TestMaintenanceAdmissionCapabilitiesExpireAcrossReopen(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement, err := NewMaintenanceSettlement(stores.callbacks, stores.releases)
	require.NoError(t, err)
	leaseUUID := testLeaseUUID("maintenance-reopen-admission")
	require.NoError(t, stores.releases.appendActive(leaseUUID, validRuntimeAuthorityRelease()))
	active, source, err := settlement.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	candidate := maintenanceCandidateForSettlement(
		t, settlement, source, target, newTestMaintenanceID(t),
	)
	admission, err := settlement.BeginMaintenanceIntent(candidate)
	require.NoError(t, err)
	dispatch, ok := admission.CreatedDispatch()
	require.True(t, ok)

	require.NoError(t, stores.callbacks.Close())
	require.NoError(t, stores.releases.Close())
	callbacks, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	releases, err := OpenIdentityBoundReleaseStore(
		ReleaseStoreConfig{DBPath: stores.releasePath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})
	reopened, err := NewMaintenanceSettlement(callbacks, releases)
	require.NoError(t, err)

	_, err = reopened.BeginMaintenanceIntent(candidate)
	require.ErrorContains(t, err, "candidate was not minted by this journal pair")
	_, err = reopened.StartMaintenanceAppend(dispatch)
	require.ErrorContains(t, err, "dispatch was not minted by this journal pair")
	freshRequest, err := reopened.NewMaintenanceRequestAuthority(
		candidate.Request().MaintenanceID(), candidate.Request().Kind(), leaseUUID,
		candidate.Request().CallbackURL(), nil,
	)
	require.NoError(t, err)
	_, err = reopened.NewMaintenanceIntentCandidate(freshRequest, source, target)
	require.ErrorContains(t, err, "source release was not claimed by this journal pair")

	active, freshSource, err := reopened.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	target = cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	freshCandidate, err := reopened.NewMaintenanceIntentCandidate(
		freshRequest, freshSource, target,
	)
	require.NoError(t, err)
	replay, err := reopened.BeginMaintenanceIntent(freshCandidate)
	require.NoError(t, err)
	assert.Equal(t, MaintenanceIntentAdmissionExisting, replay.Disposition())
}

func TestMaintenanceFirstDispatchIsConsumedByAppendStart(t *testing.T) {
	fixture := beginBoundMaintenance(t, "consumed-dispatch")
	_, err := fixture.settlement.StartMaintenanceAppend(fixture.dispatch)
	require.ErrorContains(t, err, "changed before precise mutation")
	err = fixture.settlement.CancelMaintenanceIntent(fixture.dispatch)
	require.ErrorContains(t, err, "changed before precise mutation")
	require.True(t, fixture.append.Valid())
}

func TestMaintenanceFailureAbsenceProofCannotCrossLaterAppend(t *testing.T) {
	fixture := beginBoundMaintenance(t, "stale-absence")
	absent, err := fixture.settlement.ProveMaintenanceFailure(fixture.intent)
	require.NoError(t, err)
	require.True(t, absent.Valid())

	target, err := fixture.settlement.AppendMaintenance(fixture.append)
	require.NoError(t, err)
	_, err = resolveMaintenanceFailureForTest(fixture.settlement,
		absent, "restart interrupted")

	require.ErrorContains(t, err, "appeared after absence proof")

	release, _, found, err := fixture.settlement.FindMaintenanceRelease(
		fixture.intent.LeaseUUID(), fixture.intent.MaintenanceID(),
	)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "deploying", release.Status)
	assert.True(t, target.Valid())
	pending, err := fixture.stores.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestMaintenancePairGateSerializesAppendWithSettlement(t *testing.T) {
	fixture := beginBoundMaintenance(t, "pair-gate")
	unlocked := fixture.stores.callbacks.lockDeliveryLease(fixture.intent.LeaseUUID())
	type result struct {
		target MaintenanceReleaseClaim
		err    error
	}
	done := make(chan result, 1)
	go func() {
		target, err := fixture.settlement.AppendMaintenance(fixture.append)
		done <- result{target: target, err: err}
	}()
	select {
	case result := <-done:
		t.Fatalf("maintenance append crossed held pair gate: %v", result.err)
	case <-time.After(50 * time.Millisecond):
	}
	unlocked()
	select {
	case result := <-done:
		require.NoError(t, result.err)
		require.True(t, result.target.Valid())
	case <-time.After(time.Second):
		t.Fatal("maintenance append did not resume after pair gate release")
	}
}

func TestMaintenanceTerminalProofMustBeReissuedAfterReopen(t *testing.T) {
	fixture := beginBoundMaintenance(t, "reopen-proof")
	target := fixture.appendAndBind(t)
	oldProof := activateMaintenanceOutcomeForTest(t, fixture.settlement, target)

	require.NoError(t, fixture.stores.callbacks.Close())
	require.NoError(t, fixture.stores.releases.Close())
	fixture.stores.callbacks = nil
	fixture.stores.releases = nil

	callbacks, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: fixture.stores.callbackPath},
		fixture.stores.storage, fixture.stores.gate,
	)
	require.NoError(t, err)
	releases, err := OpenIdentityBoundReleaseStore(
		ReleaseStoreConfig{DBPath: fixture.stores.releasePath},
		fixture.stores.storage, fixture.stores.gate,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})
	reopened, err := NewMaintenanceSettlement(callbacks, releases)
	require.NoError(t, err)
	_, err = reopened.ProveMaintenanceActive(fixture.intent)
	require.ErrorContains(t, err, "not minted by this journal pair")
	raw, found, err := callbacks.getMaintenanceIntent(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, raw.Valid(), "raw callback decoding must not mint pair authority")
	claim, found, err := reopened.GetMaintenanceIntent(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, claim.Valid())

	_, err = resolveMaintenanceSuccessForTest(reopened, oldProof)
	require.ErrorContains(t, err, "not minted by this journal pair")
	freshProof, err := reopened.ProveMaintenanceActive(claim)
	require.NoError(t, err)
	entry, err := resolveMaintenanceSuccessForTest(reopened, freshProof)
	require.NoError(t, err)
	assert.Equal(t, backend.CallbackStatusSuccess, entry.Status)
}

func TestMaintenanceActiveProofRejectsLaterSupersession(t *testing.T) {
	fixture := beginBoundMaintenance(t, "stale-active")
	target := fixture.appendAndBind(t)
	active := activateMaintenanceOutcomeForTest(t, fixture.settlement, target)

	// Raw compatibility mutation remains a separately reported surface gap. The
	// terminal proof still fails closed if such a generation appears between
	// proof issuance and callback settlement.
	next := validRuntimeAuthorityRelease()
	next.CreatedAt = time.Now().Add(time.Second)
	require.NoError(t, fixture.stores.releases.appendActive(fixture.intent.LeaseUUID(), next))
	_, err := resolveMaintenanceSuccessForTest(fixture.settlement, active)
	require.ErrorContains(t, err, "not \"active\"")
}

func TestMaintenanceSenderDoesNotReacquirePairGate(t *testing.T) {
	fixture := beginBoundMaintenance(t, "sender-lock")
	target := fixture.appendAndBind(t)
	active := activateMaintenanceOutcomeForTest(t, fixture.settlement, target)
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      fixture.stores.callbacks,
		HTTPClient: &http.Client{},
		Secret:     "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		Logger:     slog.Default(),
	})
	publisher, err := NewCallbackPublisher(CallbackPublisherConfig{
		OperationSettlement:   fixture.stores.settlement,
		MaintenanceSettlement: fixture.settlement,
		StorageAttestor:       sender.attestor,
		Logger:                sender.logger,
	})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- publisher.PublishMaintenanceSuccessContext(context.Background(), active)
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("maintenance sender recursively reacquired the pair gate")
	}
	_, found, err := fixture.stores.callbacks.getMaintenanceIntent(fixture.intent.LeaseUUID())
	require.NoError(t, err)
	assert.False(t, found)
}
