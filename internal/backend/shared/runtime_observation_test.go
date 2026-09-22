package shared

import (
	"context"
	"log/slog"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func successfulRuntimeForObservationTest(
	t *testing.T,
	fixture callbackPublisherFixture,
	name string,
) (OperationIntentSpec, RuntimeGenerationProof) {
	t.Helper()
	spec := testOperationIntentSpec(t, name)
	claim := beginHandoffOperation(t, fixture.stores.settlement, spec)
	candidate, err := fixture.stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	committed := commitHandoffOperation(t, fixture.stores.settlement, candidate)
	settleAndAcknowledgeRuntimeOperationForTest(t, fixture, committed)
	proof, err := fixture.stores.releases.ProveRuntimeGeneration(spec.LeaseUUID)
	require.NoError(t, err)
	return spec, proof
}

func beginObservationTestMaintenance(
	t *testing.T,
	fixture callbackPublisherFixture,
	leaseUUID string,
) MaintenanceIntentDispatch {
	t.Helper()
	active, source, err := fixture.maintenance.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	authority, ok := target.RuntimeIdentity()
	require.True(t, ok)
	request, err := fixture.maintenance.NewMaintenanceRequestAuthority(
		newTestMaintenanceID(t), MaintenanceIntentRestart, leaseUUID,
		authority.LifecycleCallbackURL(), nil,
	)
	require.NoError(t, err)
	candidate, err := fixture.maintenance.NewMaintenanceIntentCandidate(request, source, target)
	require.NoError(t, err)
	admission, err := fixture.maintenance.BeginMaintenanceIntent(candidate)
	require.NoError(t, err)
	dispatch, created := admission.CreatedDispatch()
	require.True(t, created)
	return dispatch
}

func failSuccessorForRuntimeObservationTest(
	t *testing.T,
	fixture callbackPublisherFixture,
	leaseUUID, name string,
) OperationReleaseUncommitted {
	t.Helper()
	spec := testOperationIntentSpec(t, name)
	spec.LeaseUUID = leaseUUID
	claim := beginHandoffOperation(t, fixture.stores.settlement, spec)
	uncommitted := commitHandoffRefusal(t, fixture.stores.settlement, claim)
	require.NoError(t, fixture.publisher.PublishOperationFailureContext(
		context.Background(), uncommitted, "replacement refused",
	))
	return uncommitted
}

func acknowledgeOperationCallbackForRuntimeObservationTest(
	t *testing.T,
	store *CallbackStore,
	leaseUUID string,
) {
	t.Helper()
	pending, err := store.ListPending()
	require.NoError(t, err)
	for _, entry := range pending {
		if entry.LeaseUUID == leaseUUID && entry.DeliveryKind == CallbackDeliveryKindOperation {
			require.NoError(t, store.removeEntry(entry))
			return
		}
	}
	t.Fatalf("operation completion for %q was not queued", leaseUUID)
}

func TestRuntimeObservationPermitRejectsPendingOperation(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	spec := testOperationIntentSpec(t, "runtime-observation-pending-operation")
	claim := beginHandoffOperation(t, fixture.stores.settlement, spec)
	candidate, err := fixture.stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	commitHandoffOperation(t, fixture.stores.settlement, candidate)
	proof, err := fixture.stores.releases.ProveRuntimeGeneration(spec.LeaseUUID)
	require.NoError(t, err)

	permit, err := fixture.publisher.AuthorizeRuntimeObservationContext(context.Background(), proof)
	require.ErrorContains(t, err, "pending operation phase")
	assert.False(t, permit.Valid())
	pending, listErr := fixture.stores.callbacks.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending)
}

func TestRuntimeObservationPermitRejectsMaintenanceAndClosePhases(t *testing.T) {
	t.Run("maintenance", func(t *testing.T) {
		fixture := newCallbackPublisherFixture(t, "docker-a")
		spec, proof := successfulRuntimeForObservationTest(
			t, fixture, "runtime-observation-maintenance",
		)
		beginObservationTestMaintenance(t, fixture, spec.LeaseUUID)

		permit, err := fixture.publisher.AuthorizeRuntimeObservationContext(context.Background(), proof)
		require.ErrorContains(t, err, "maintenance mutation phase")
		assert.False(t, permit.Valid())
	})

	t.Run("close", func(t *testing.T) {
		fixture := newCallbackPublisherFixture(t, "docker-a")
		spec, proof := successfulRuntimeForObservationTest(
			t, fixture, "runtime-observation-close",
		)
		closeSettlement, err := NewCloseSettlement(
			fixture.stores.callbacks, fixture.stores.releases, fixture.stores.retentions,
		)
		require.NoError(t, err)
		request, err := closeSettlement.NewCloseRequest(spec.LeaseUUID, false)
		require.NoError(t, err)
		_, err = closeSettlement.BeginClose(request)
		require.NoError(t, err)

		permit, err := fixture.publisher.AuthorizeRuntimeObservationContext(context.Background(), proof)
		require.ErrorContains(t, err, "close mutation phase")
		assert.False(t, permit.Valid())
	})
}

func TestRuntimeObservationPermitAcceptsFailedSuccessorOverExactActiveRuntime(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	spec, proof := successfulRuntimeForObservationTest(
		t, fixture, "runtime-observation-failed-successor",
	)
	failSuccessorForRuntimeObservationTest(
		t, fixture, spec.LeaseUUID, "runtime-observation-failed-successor-replacement",
	)

	permit := authorizeRuntimeObservationForTest(t, fixture.publisher, proof)
	acknowledgeOperationCallbackForRuntimeObservationTest(
		t, fixture.stores.callbacks, spec.LeaseUUID,
	)
	require.NoError(t, fixture.publisher.PublishLifecycleFailureContext(
		context.Background(), permit, "predecessor runtime exited",
	))
	pending, err := fixture.stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[0].DeliveryKind)
}

func TestRuntimeObservationPermitRejectsFailedOperationWithoutPredecessor(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	spec := testOperationIntentSpec(t, "runtime-observation-initial-failure")
	claim := beginHandoffOperation(t, fixture.stores.settlement, spec)
	uncommitted := commitHandoffRefusal(t, fixture.stores.settlement, claim)
	require.NoError(t, fixture.publisher.PublishOperationFailureContext(
		context.Background(), uncommitted, "initial refusal",
	))
	acknowledgeOperationCallbackForRuntimeObservationTest(
		t, fixture.stores.callbacks, spec.LeaseUUID,
	)

	// A package-private fixture may install a later active runtime without
	// replacing the Failed head. Production transitions cannot manufacture
	// predecessor authority after the failure was settled.
	require.NoError(t, fixture.stores.releases.appendActive(
		spec.LeaseUUID, validRuntimeAuthorityRelease(),
	))
	proof, err := fixture.stores.releases.ProveRuntimeGeneration(spec.LeaseUUID)
	require.NoError(t, err)
	permit, err := fixture.publisher.AuthorizeRuntimeObservationContext(context.Background(), proof)
	require.ErrorContains(t, err, "not an exact successor")
	assert.False(t, permit.Valid())
}

func TestRuntimeObservationPermitAcceptsFailedSuccessorOverLegacyRuntime(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	leaseUUID := testLeaseUUID("runtime-observation-legacy-predecessor")
	legacy := validRuntimeAuthorityRelease()
	legacyAuthority, err := NewLegacyRuntimeAuthority(
		legacy.RuntimeAuthority.Tenant(), legacy.RuntimeAuthority.ProviderUUID(),
		"https://fred.example/callbacks/provision?route=legacy",
		"https://fred.example/callbacks/provision?route=legacy",
	)
	require.NoError(t, err)
	legacy.OperationID = OperationID{}
	legacy.RuntimeAuthority = nil
	legacy.LegacyRuntimeAuthority = &legacyAuthority
	require.NoError(t, fixture.stores.releases.appendActive(leaseUUID, legacy))
	proof, err := fixture.stores.releases.ProveRuntimeGeneration(leaseUUID)
	require.NoError(t, err)
	require.Equal(t, ReleaseAuthorityLegacy, proof.AuthorityClass())

	failSuccessorForRuntimeObservationTest(
		t, fixture, leaseUUID, "runtime-observation-legacy-replacement",
	)
	permit := authorizeRuntimeObservationForTest(t, fixture.publisher, proof)
	require.True(t, permit.Valid())
}

func TestRuntimeObservationPermitReattestsMutationPhase(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	spec, proof := successfulRuntimeForObservationTest(
		t, fixture, "runtime-observation-stale-phase",
	)
	permit := authorizeRuntimeObservationForTest(t, fixture.publisher, proof)
	beginObservationTestMaintenance(t, fixture, spec.LeaseUUID)

	err := fixture.publisher.PublishLifecycleFailureContext(
		context.Background(), permit, "stale container exit",
	)
	require.ErrorContains(t, err, "maintenance mutation phase")
	pending, listErr := fixture.stores.callbacks.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending)
}

func TestRuntimeObservationPermitSerializesReleaseBackfillWithCallbackGate(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	leaseUUID := testLeaseUUID("runtime-observation-backfill")
	release := validRuntimeAuthorityRelease()
	require.NoError(t, fixture.stores.releases.appendActive(leaseUUID, release))
	active, err := fixture.stores.releases.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	proof, err := fixture.stores.releases.ProveRuntimeGeneration(leaseUUID)
	require.NoError(t, err)
	permit := authorizeRuntimeObservationForTest(t, fixture.publisher, proof)

	unlock := fixture.stores.callbacks.lockDeliveryLease(leaseUUID)
	backfiller, err := NewReleaseBackfiller(
		fixture.stores.callbacks, fixture.stores.releases,
	)
	require.NoError(t, err)
	backfillDone := make(chan error, 1)
	go func() {
		backfillDone <- backfiller.BackfillActiveResourceProfilesContext(
			context.Background(), leaseUUID, active.Version, active.Items,
			active.ResourceProfiles,
		)
	}()
	select {
	case backfillErr := <-backfillDone:
		t.Fatalf("release backfill bypassed the callback lease gate: %v", backfillErr)
	case <-time.After(50 * time.Millisecond):
	}
	unlock()
	require.NoError(t, <-backfillDone)

	err = fixture.publisher.PublishLifecycleFailureContext(
		context.Background(), permit, "observation after idempotent backfill",
	)
	require.NoError(t, err)
	pending, listErr := fixture.stores.callbacks.ListPending()
	require.NoError(t, listErr)
	require.Len(t, pending, 1)
}

func TestReleaseBackfillerSerializesActualMutationWithCallbackGate(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	leaseUUID := testLeaseUUID("release-backfill-actual-mutation")
	legacy := Release{
		Manifest:  []byte(`{"services":{"app":{"image":"nginx:1.27"}}}`),
		Image:     "stack",
		Status:    "active",
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, fixture.stores.releases.append(leaseUUID, legacy))
	active, err := fixture.stores.releases.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	items := []backend.LeaseItem{{SKU: "sku-a", ServiceName: "app", Quantity: 1}}
	profiles := []SKUResourceSnapshot{{SKU: "sku-a", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}}

	backfiller, err := NewReleaseBackfiller(
		fixture.stores.callbacks, fixture.stores.releases,
	)
	require.NoError(t, err)
	unblock := fixture.stores.callbacks.lockDeliveryLease(leaseUUID)
	var unblockOnce sync.Once
	releaseGate := func() { unblockOnce.Do(unblock) }
	t.Cleanup(releaseGate)
	result := make(chan error, 1)
	go func() {
		result <- backfiller.BackfillLegacyActiveAuthorityContext(
			context.Background(), leaseUUID, *active, items, profiles,
		)
	}()
	require.Eventually(t, func() bool {
		fixture.stores.callbacks.deliveryLocksMu.Lock()
		defer fixture.stores.callbacks.deliveryLocksMu.Unlock()
		lock := fixture.stores.callbacks.deliveryLocks[leaseUUID]
		return lock != nil && lock.refs == 2
	}, time.Second, time.Millisecond, "actual release mutation never queued on the callback gate")
	releaseGate()
	require.NoError(t, <-result)

	backfilled, err := fixture.stores.releases.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, backfilled)
	assert.Equal(t, items, backfilled.Items)
	assert.Equal(t, profiles, backfilled.ResourceProfiles)
}

func TestReleaseBackfillerContextCancellationCannotOutliveOwner(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	leaseUUID := testLeaseUUID("release-backfill-context-cancel")
	release := validRuntimeAuthorityRelease()
	require.NoError(t, fixture.stores.releases.appendActive(leaseUUID, release))
	active, err := fixture.stores.releases.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)

	backfiller, err := NewReleaseBackfiller(
		fixture.stores.callbacks, fixture.stores.releases,
	)
	require.NoError(t, err)
	unblock := fixture.stores.callbacks.lockDeliveryLease(leaseUUID)
	var unblockOnce sync.Once
	releaseGate := func() { unblockOnce.Do(unblock) }
	t.Cleanup(releaseGate)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- backfiller.BackfillActiveResourceProfilesContext(
			ctx, leaseUUID, active.Version, active.Items, active.ResourceProfiles,
		)
	}()
	require.Eventually(t, func() bool {
		fixture.stores.callbacks.deliveryLocksMu.Lock()
		defer fixture.stores.callbacks.deliveryLocksMu.Unlock()
		lock := fixture.stores.callbacks.deliveryLocks[leaseUUID]
		return lock != nil && lock.refs == 2
	}, time.Second, time.Millisecond, "release backfill never queued on the per-lease gate")
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled release backfill remained blocked on the per-lease gate")
	}
	releaseGate()

	after, err := fixture.stores.releases.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.Equal(t, active, after)
}

func TestRuntimeObservationPermitAllowsCurrentRuntimeWithoutHead(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	spec, proof := successfulRuntimeForObservationTest(
		t, fixture, "runtime-observation-no-head",
	)
	dispatch := beginObservationTestMaintenance(t, fixture, spec.LeaseUUID)
	require.NoError(t, fixture.maintenance.CancelMaintenanceIntent(dispatch))

	permit := authorizeRuntimeObservationForTest(t, fixture.publisher, proof)
	require.NoError(t, fixture.publisher.PublishLifecycleFailureContext(
		context.Background(), permit, "container exited",
	))
	pending, err := fixture.stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[0].DeliveryKind)
}

func TestCopiedRuntimeObservationPermitCannotDuplicateOrWidenAuthority(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	_, proof := successfulRuntimeForObservationTest(
		t, fixture, "runtime-observation-copied-permit",
	)
	permit := authorizeRuntimeObservationForTest(t, fixture.publisher, proof)
	copied := permit

	require.NoError(t, fixture.publisher.PublishLifecycleFailureContext(
		context.Background(), permit, "container exited",
	))
	require.NoError(t, fixture.publisher.PublishLifecycleFailureContext(
		context.Background(), copied, "container exited",
	))
	pending, err := fixture.stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "a copied permit remains bound to coalescing observation semantics")
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[0].DeliveryKind)
}

func TestRuntimeObservationPublicationCancelsWhileWaitingForLeaseGate(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	_, proof := successfulRuntimeForObservationTest(
		t, fixture, "runtime-observation-canceled-wait",
	)
	permit := authorizeRuntimeObservationForTest(t, fixture.publisher, proof)

	unlock := fixture.stores.callbacks.lockDeliveryLease(permit.LeaseUUID())
	var unlockOnce sync.Once
	release := func() { unlockOnce.Do(unlock) }
	t.Cleanup(release)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- fixture.publisher.PublishLifecycleFailureContext(
			ctx, permit, "container exited",
		)
	}()
	require.Eventually(t, func() bool {
		fixture.stores.callbacks.deliveryLocksMu.Lock()
		defer fixture.stores.callbacks.deliveryLocksMu.Unlock()
		lock := fixture.stores.callbacks.deliveryLocks[permit.LeaseUUID()]
		return lock != nil && lock.refs == 2
	}, time.Second, time.Millisecond, "publisher never queued on the per-lease gate")

	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled lifecycle publication remained blocked on the per-lease gate")
	}
	release()
	pending, err := fixture.stores.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestRuntimeObservationPermitIsRevokedByStoreCloseAndReopen(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	_, proof := successfulRuntimeForObservationTest(
		t, fixture, "runtime-observation-reopen",
	)
	permit := authorizeRuntimeObservationForTest(t, fixture.publisher, proof)

	require.NoError(t, fixture.stores.callbacks.Close())
	require.NoError(t, fixture.stores.releases.Close())
	require.False(t, permit.Valid())
	require.False(t, proof.Valid())

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
	operations, err := NewOperationSettlement(callbacks, releases)
	require.NoError(t, err)
	maintenance, err := NewMaintenanceSettlement(callbacks, releases)
	require.NoError(t, err)
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: callbacks, HTTPClient: http.DefaultClient,
		Secret: "runtime-observation-reopen-secret", Logger: slog.Default(),
	})
	reopened := mustNewCallbackPublisherForTest(t, CallbackPublisherConfig{
		OperationSettlement: operations, MaintenanceSettlement: maintenance,
		StorageAttestor: sender.attestor, Logger: sender.logger,
	})

	require.Error(t, reopened.PublishLifecycleFailureContext(
		context.Background(), permit, "copied old authority",
	))
	freshProof, err := releases.ProveRuntimeGeneration(proof.claim.LeaseUUID())
	require.NoError(t, err)
	freshPermit := authorizeRuntimeObservationForTest(t, reopened, freshProof)
	require.NoError(t, reopened.PublishLifecycleFailureContext(
		context.Background(), freshPermit, "current observation",
	))
}

func TestJournalPairConstructionRejectsClosedStores(t *testing.T) {
	t.Run("callback", func(t *testing.T) {
		fixture := newCallbackPublisherFixture(t, "docker-a")
		backfiller, err := NewReleaseBackfiller(
			fixture.stores.callbacks, fixture.stores.releases,
		)
		require.NoError(t, err)
		require.NoError(t, fixture.stores.callbacks.Close())
		_, err = NewOperationSettlement(fixture.stores.callbacks, fixture.stores.releases)
		require.ErrorContains(t, err, "exact identity-bound")
		_, err = NewReleaseBackfiller(fixture.stores.callbacks, fixture.stores.releases)
		require.ErrorContains(t, err, "exact identity-bound")
		require.ErrorContains(t, backfiller.BackfillActiveResourceProfilesContext(
			context.Background(), testLeaseUUID("closed-backfiller"), 1, nil, nil,
		), "open journal pair")
	})
	t.Run("release", func(t *testing.T) {
		fixture := newCallbackPublisherFixture(t, "docker-a")
		backfiller, err := NewReleaseBackfiller(
			fixture.stores.callbacks, fixture.stores.releases,
		)
		require.NoError(t, err)
		require.NoError(t, fixture.stores.releases.Close())
		_, err = NewOperationSettlement(fixture.stores.callbacks, fixture.stores.releases)
		require.ErrorContains(t, err, "exact identity-bound")
		_, err = NewReleaseBackfiller(fixture.stores.callbacks, fixture.stores.releases)
		require.ErrorContains(t, err, "exact identity-bound")
		require.ErrorContains(t, backfiller.BackfillLegacyRuntimeAuthorityContext(
			context.Background(), testLeaseUUID("closed-release-backfiller"), Release{}, LegacyRuntimeAuthority{},
		), "open journal pair")
	})
}

func TestReleaseBackfillerExposesContextBoundAuthorityOnly(t *testing.T) {
	typeOf := reflect.TypeFor[*ReleaseBackfiller]()
	want := map[string]struct{}{
		"BackfillActiveResourceProfilesContext": {},
		"BackfillLegacyActiveAuthorityContext":  {},
		"BackfillLegacyRuntimeAuthorityContext": {},
	}
	require.Equal(t, len(want), typeOf.NumMethod())
	for index := range typeOf.NumMethod() {
		method := typeOf.Method(index)
		_, expected := want[method.Name]
		assert.True(t, expected, "ReleaseBackfiller exposes unexpected method %s", method.Name)
	}
}
