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

type callbackPublisherFixture struct {
	stores      operationHandoffStores
	maintenance *MaintenanceSettlement
	sender      *CallbackSender
	publisher   *CallbackPublisher
}

func mustNewCallbackPublisherForTest(
	t *testing.T,
	cfg CallbackPublisherConfig,
) *CallbackPublisher {
	t.Helper()
	publisher, err := NewCallbackPublisher(cfg)
	require.NoError(t, err)
	return publisher
}

func authorizeRuntimeObservationForTest(
	t *testing.T,
	publisher *CallbackPublisher,
	proof RuntimeGenerationProof,
) RuntimeObservationPermit {
	t.Helper()
	permit, err := publisher.AuthorizeRuntimeObservationContext(context.Background(), proof)
	require.NoError(t, err)
	require.True(t, permit.Valid())
	return permit
}

func settleAndAcknowledgeRuntimeOperationForTest(
	t *testing.T,
	fixture callbackPublisherFixture,
	committed OperationReleaseCommitted,
) {
	t.Helper()
	require.NoError(t, fixture.publisher.PublishOperationSuccessContext(context.Background(), committed))
	pending, err := fixture.stores.callbacks.ListPending()
	require.NoError(t, err)
	for _, entry := range pending {
		if entry.LeaseUUID == committed.LeaseUUID() &&
			entry.DeliveryKind == CallbackDeliveryKindOperation {
			require.NoError(t, fixture.stores.callbacks.removeEntry(entry))
			return
		}
	}
	t.Fatalf("operation completion for runtime %q was not queued", committed.LeaseUUID())
}

func newCallbackPublisherFixture(t *testing.T, name string) callbackPublisherFixture {
	t.Helper()
	stores := openOperationHandoffStores(t, name)
	maintenance, err := NewMaintenanceSettlement(stores.callbacks, stores.releases)
	require.NoError(t, err)
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      stores.callbacks,
		HTTPClient: http.DefaultClient,
		Secret:     "callback-publisher-test-secret-value",
		Logger:     slog.Default(),
	})
	publisher, err := NewCallbackPublisher(CallbackPublisherConfig{
		OperationSettlement:   stores.settlement,
		MaintenanceSettlement: maintenance,
		StorageAttestor:       sender.attestor,
		Logger:                sender.logger,
		OnStoreError:          sender.onStoreError,
	})
	require.NoError(t, err)
	return callbackPublisherFixture{
		stores: stores, maintenance: maintenance, sender: sender, publisher: publisher,
	}
}

func TestCallbackPublisherConstructionRejectsZeroAndCrossStoreComposition(t *testing.T) {
	fixtureA := newCallbackPublisherFixture(t, "docker-a")
	fixtureB := newCallbackPublisherFixture(t, "docker-b")

	_, err := NewCallbackPublisher(CallbackPublisherConfig{})
	require.Error(t, err)
	_, err = NewCallbackPublisher(CallbackPublisherConfig{
		OperationSettlement:   fixtureA.stores.settlement,
		MaintenanceSettlement: fixtureA.maintenance,
		StorageAttestor:       fixtureB.sender.attestor,
		Logger:                fixtureA.sender.logger,
	})
	require.ErrorContains(t, err, "storage attestor")
	_, err = NewCallbackPublisher(CallbackPublisherConfig{
		OperationSettlement:   fixtureA.stores.settlement,
		MaintenanceSettlement: fixtureB.maintenance,
		StorageAttestor:       fixtureA.sender.attestor,
		Logger:                fixtureA.sender.logger,
	})
	require.ErrorContains(t, err, "different journals")
}

func TestCallbackPublisherZeroValueFailsClosedWithoutPanic(t *testing.T) {
	var publisher CallbackPublisher
	require.Error(t, publisher.PublishOperationSuccessContext(context.Background(), OperationReleaseCommitted{}))
	require.Error(t, publisher.PublishOperationFailureContext(
		context.Background(), OperationReleaseUncommitted{}, "failed",
	))
	_, err := publisher.AuthorizeRuntimeObservationContext(context.Background(), RuntimeGenerationProof{})
	require.Error(t, err)
	require.Error(t, publisher.PublishLifecycleFailureContext(
		context.Background(), RuntimeObservationPermit{}, "failed",
	))
	require.Error(t, publisher.PublishMaintenanceSuccessContext(
		context.Background(), MaintenanceReleaseActive{},
	))
	require.Error(t, publisher.PublishMaintenanceFailureContext(
		context.Background(), MaintenanceReleaseFailure{}, "failed",
	))
	_, err = publisher.TryPublishMaintenanceSuccessContext(
		context.Background(), MaintenanceReleaseActive{},
	)
	require.Error(t, err)
	_, err = publisher.TryPublishMaintenanceFailureContext(
		context.Background(), MaintenanceReleaseFailure{}, "failed",
	)
	require.Error(t, err)
	_, err = publisher.TryPublishMaintenanceRuntimeFailureContext(
		context.Background(), MaintenanceReleaseActive{}, "failed",
	)
	require.Error(t, err)
}

func TestCallbackPublisherRejectsProofFromAnotherExactJournal(t *testing.T) {
	fixtureA := newCallbackPublisherFixture(t, "docker-a")
	fixtureB := newCallbackPublisherFixture(t, "docker-b")
	claimB := beginHandoffOperation(
		t, fixtureB.stores.settlement, testOperationIntentSpec(t, "publisher-cross-proof"),
	)
	proofB := commitHandoffRefusal(t, fixtureB.stores.settlement, claimB)

	err := fixtureA.publisher.PublishOperationFailureContext(context.Background(), proofB, "failed")
	require.ErrorContains(t, err, "another journal pair")
	claims, err := fixtureB.stores.settlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1, "cross-store publication must not consume the source intent")
	pending, err := fixtureA.stores.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackPublisherFixedOperationAndLifecycleStatuses(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")

	failureSpec := testOperationIntentSpec(t, "publisher-failure")
	failure := beginHandoffOperation(t, fixture.stores.settlement, failureSpec)
	uncommitted := commitHandoffRefusal(t, fixture.stores.settlement, failure)
	require.NoError(t, fixture.publisher.PublishOperationFailureContext(
		context.Background(), uncommitted, "image pull failed",
	))

	successSpec := testOperationIntentSpec(t, "publisher-success")
	success := beginHandoffOperation(t, fixture.stores.settlement, successSpec)
	candidate, err := fixture.stores.settlement.PrepareOperationRelease(success)
	require.NoError(t, err)
	committed := commitHandoffOperation(t, fixture.stores.settlement, candidate)
	require.NoError(t, fixture.publisher.PublishOperationSuccessContext(context.Background(), committed))

	lifecycleSpec := testOperationIntentSpec(t, "publisher-lifecycle")
	lifecycleClaim := beginHandoffOperation(t, fixture.stores.settlement, lifecycleSpec)
	lifecycleCandidate, err := fixture.stores.settlement.PrepareOperationRelease(lifecycleClaim)
	require.NoError(t, err)
	lifecycleCommitted := commitHandoffOperation(t, fixture.stores.settlement, lifecycleCandidate)
	settleAndAcknowledgeRuntimeOperationForTest(t, fixture, lifecycleCommitted)
	lifecycleProof, err := fixture.stores.releases.ProveRuntimeGeneration(lifecycleSpec.LeaseUUID)
	require.NoError(t, err)
	lifecyclePermit := authorizeRuntimeObservationForTest(t, fixture.publisher, lifecycleProof)
	require.NoError(t, fixture.publisher.PublishLifecycleFailureContext(
		context.Background(), lifecyclePermit, "container exited",
	))

	pending, err := fixture.stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 3)
	byLease := make(map[string]CallbackEntry, len(pending))
	for _, entry := range pending {
		byLease[entry.LeaseUUID] = entry
	}
	assert.Equal(t, backend.CallbackStatusFailed, byLease[failureSpec.LeaseUUID].Status)
	assert.Equal(t, CallbackDeliveryKindOperation, byLease[failureSpec.LeaseUUID].DeliveryKind)
	assert.False(t, byLease[failureSpec.LeaseUUID].Retained)
	assert.Equal(t, backend.CallbackStatusSuccess, byLease[successSpec.LeaseUUID].Status)
	lifecycle := byLease[lifecycleSpec.LeaseUUID]
	assert.Equal(t, backend.CallbackStatusFailed, lifecycle.Status)
	assert.Equal(t, CallbackDeliveryKindLifecycle, lifecycle.DeliveryKind)
	assert.False(t, lifecycle.Retained)
	assert.Equal(t, lifecycleSpec.LifecycleCallbackURL, lifecycle.CallbackURL)
	assert.Equal(t, "docker-a", lifecycle.Backend)
	assert.Equal(t, fixture.stores.storage.ID().String(), lifecycle.BackendStorageID)
}

func TestCallbackPublisherLifecycleProofRejectsStaleRelease(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	spec := testOperationIntentSpec(t, "publisher-lifecycle-stale")
	claim := beginHandoffOperation(t, fixture.stores.settlement, spec)
	candidate, err := fixture.stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	committed := commitHandoffOperation(t, fixture.stores.settlement, candidate)
	settleAndAcknowledgeRuntimeOperationForTest(t, fixture, committed)

	proof, err := fixture.stores.releases.ProveRuntimeGeneration(spec.LeaseUUID)
	require.NoError(t, err)
	permit := authorizeRuntimeObservationForTest(t, fixture.publisher, proof)
	require.NoError(t, fixture.stores.releases.updateLatestStatus(
		spec.LeaseUUID, "failed", backend.ReasonInternal, "runtime replaced",
	))
	require.Error(t, fixture.publisher.PublishLifecycleFailureContext(
		context.Background(), permit, "late observation",
	))
	pending, err := fixture.stores.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "a stale runtime proof must not enqueue an observation")
}

func TestCallbackPublisherLifecycleProofIsExactToJournalPair(t *testing.T) {
	fixtureA := newCallbackPublisherFixture(t, "docker-a")
	fixtureB := newCallbackPublisherFixture(t, "docker-b")
	spec := testOperationIntentSpec(t, "publisher-lifecycle-cross-pair")
	claim := beginHandoffOperation(t, fixtureA.stores.settlement, spec)
	candidate, err := fixtureA.stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	committed := commitHandoffOperation(t, fixtureA.stores.settlement, candidate)
	settleAndAcknowledgeRuntimeOperationForTest(t, fixtureA, committed)
	proof, err := fixtureA.stores.releases.ProveRuntimeGeneration(spec.LeaseUUID)
	require.NoError(t, err)
	permit := authorizeRuntimeObservationForTest(t, fixtureA.publisher, proof)

	require.ErrorContains(t,
		fixtureB.publisher.PublishLifecycleFailureContext(
			context.Background(), permit, "cross-pair observation",
		),
		"another journal pair",
	)
	pending, err := fixtureB.stores.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackPublisherFailsClosedAfterBoundStoreCloses(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	spec := testOperationIntentSpec(t, "publisher-closed-store")
	claim := beginHandoffOperation(t, fixture.stores.settlement, spec)
	uncommitted := commitHandoffRefusal(t, fixture.stores.settlement, claim)

	require.NoError(t, fixture.stores.callbacks.Close())
	fixture.stores.callbacks = nil // prevent the fixture cleanup from closing twice
	require.Error(t, fixture.publisher.PublishOperationFailureContext(
		context.Background(), uncommitted, "late failure",
	))
}

func TestCallbackPublisherCanceledAttestorLifetimePreservesPendingOperation(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	maintenance, err := NewMaintenanceSettlement(stores.callbacks, stores.releases)
	require.NoError(t, err)
	stopCtx, cancel := context.WithCancel(context.Background())
	attestor := newTestCallbackStorageAttestor(t, stores.callbacks, stopCtx, nil, 0)
	publisher := mustNewCallbackPublisherForTest(t, CallbackPublisherConfig{
		OperationSettlement:   stores.settlement,
		MaintenanceSettlement: maintenance,
		StorageAttestor:       attestor,
		Logger:                slog.Default(),
	})
	spec := testOperationIntentSpec(t, "publisher-canceled-attestor")
	claim := beginHandoffOperation(t, stores.settlement, spec)
	uncommitted := commitHandoffRefusal(t, stores.settlement, claim)

	cancel()
	require.ErrorContains(t, publisher.PublishOperationFailureContext(
		context.Background(), uncommitted, "late failure",
	), "publisher is invalid")

	pending, err := stores.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "canceled backend lifetime must not publish a completion")
	intents, err := stores.settlement.ListOperationIntents()
	require.NoError(t, err)
	assert.Len(t, intents, 1,
		"canceled backend lifetime must preserve the operation for recovery")
}

func TestCallbackPublisherFailureContextCancelsWhileWaitingForLeaseGate(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	spec := testOperationIntentSpec(t, "publisher-canceled-fifo-wait")
	claim := beginHandoffOperation(t, fixture.stores.settlement, spec)
	uncommitted := commitHandoffRefusal(t, fixture.stores.settlement, claim)

	unlock := fixture.stores.callbacks.lockDeliveryLease(spec.LeaseUUID)
	var unlockOnce sync.Once
	release := func() { unlockOnce.Do(unlock) }
	t.Cleanup(release)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- fixture.publisher.PublishOperationFailureContext(
			ctx, uncommitted, "worker stopped",
		)
	}()
	require.Eventually(t, func() bool {
		fixture.stores.callbacks.deliveryLocksMu.Lock()
		defer fixture.stores.callbacks.deliveryLocksMu.Unlock()
		lock := fixture.stores.callbacks.deliveryLocks[spec.LeaseUUID]
		return lock != nil && lock.refs == 2
	}, time.Second, time.Millisecond, "publisher never queued on the per-lease gate")

	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled publication remained blocked on the per-lease gate")
	}
	release()

	pending, err := fixture.stores.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "canceled publication must not create a completion")
	intents, err := fixture.stores.settlement.ListOperationIntents()
	require.NoError(t, err)
	assert.Len(t, intents, 1, "canceled publication must preserve recovery authority")
}

func TestCallbackPublisherMaintenanceContextCancelsWhileWaitingForLeaseGate(t *testing.T) {
	fixture := beginBoundMaintenance(t, "publisher-canceled-maintenance-wait")
	target := fixture.appendAndBind(t)
	active := activateMaintenanceOutcomeForTest(t, fixture.settlement, target)
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: fixture.stores.callbacks, HTTPClient: http.DefaultClient,
		Secret: "callback-publisher-maintenance-cancel-secret", Logger: slog.Default(),
	})
	publisher := mustNewCallbackPublisherForTest(t, CallbackPublisherConfig{
		OperationSettlement:   fixture.stores.settlement,
		MaintenanceSettlement: fixture.settlement,
		StorageAttestor:       sender.attestor, Logger: sender.logger,
	})

	unlock := fixture.stores.callbacks.lockDeliveryLease(active.LeaseUUID())
	var unlockOnce sync.Once
	release := func() { unlockOnce.Do(unlock) }
	t.Cleanup(release)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- publisher.PublishMaintenanceSuccessContext(ctx, active)
	}()
	require.Eventually(t, func() bool {
		fixture.stores.callbacks.deliveryLocksMu.Lock()
		defer fixture.stores.callbacks.deliveryLocksMu.Unlock()
		lock := fixture.stores.callbacks.deliveryLocks[active.LeaseUUID()]
		return lock != nil && lock.refs == 2
	}, time.Second, time.Millisecond, "publisher never queued on the per-lease gate")

	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled maintenance publication remained blocked on the per-lease gate")
	}
	release()

	pending, err := fixture.stores.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "canceled publication must not create a completion")
	_, found, err := fixture.settlement.GetMaintenanceIntent(active.LeaseUUID())
	require.NoError(t, err)
	assert.True(t, found, "canceled publication must preserve recovery authority")
}

func TestCallbackPublisherConcurrentLifecycleFailureCoalescesToOneObservation(t *testing.T) {
	fixture := newCallbackPublisherFixture(t, "docker-a")
	spec := testOperationIntentSpec(t, "publisher-lifecycle-race")
	claim := beginHandoffOperation(t, fixture.stores.settlement, spec)
	candidate, err := fixture.stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	committed := commitHandoffOperation(t, fixture.stores.settlement, candidate)
	settleAndAcknowledgeRuntimeOperationForTest(t, fixture, committed)
	proof, err := fixture.stores.releases.ProveRuntimeGeneration(spec.LeaseUUID)
	require.NoError(t, err)
	permit := authorizeRuntimeObservationForTest(t, fixture.publisher, proof)

	const publishers = 16
	var wg sync.WaitGroup
	errs := make(chan error, publishers)
	for range publishers {
		wg.Go(func() {
			errs <- fixture.publisher.PublishLifecycleFailureContext(
				context.Background(), permit, "container exited",
			)
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	pending, err := fixture.stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[0].DeliveryKind)
}

func TestCallbackSenderExportsTransportControlsOnly(t *testing.T) {
	typeOf := reflect.TypeFor[*CallbackSender]()
	for _, forbidden := range []string{
		"SendOperationSuccess", "SendOperationFailure", "SendOperationFailureContext",
		"SendMaintenanceSuccess", "SendMaintenanceFailure", "SendLifecycleCallback",
		"DeliverCallback", "ReplayPendingCallbacks",
	} {
		_, found := typeOf.MethodByName(forbidden)
		assert.False(t, found, "CallbackSender must not expose semantic method %s", forbidden)
	}
}

func TestCallbackSenderConfigHasNoBareStorageVerificationSurface(t *testing.T) {
	typeOf := reflect.TypeFor[CallbackSenderConfig]()
	for _, forbidden := range []string{"VerifyStorage", "IdentityVerificationTimeout"} {
		_, found := typeOf.FieldByName(forbidden)
		assert.False(t, found, "CallbackSenderConfig must not expose %s", forbidden)
	}
}

func TestCallbackPublisherConstructionHasNoTransportDependency(t *testing.T) {
	config := reflect.TypeFor[CallbackPublisherConfig]()
	for _, forbidden := range []string{
		"Sender", "HTTPClient", "Secret", "StopCtx", "Backoff",
		"DeliveryTimeout", "ReplayInterval",
	} {
		_, found := config.FieldByName(forbidden)
		assert.False(t, found, "CallbackPublisherConfig must not expose transport field %s", forbidden)
	}

	publisher := reflect.TypeFor[CallbackPublisher]()
	for _, forbidden := range []string{"sender", "httpClient", "secret", "replayWake", "replayRetry"} {
		_, found := publisher.FieldByName(forbidden)
		assert.False(t, found, "CallbackPublisher must not retain transport field %s", forbidden)
	}
}

func TestCallbackPublisherDoesNotExposeCallerSelectedLifecycleTerminals(t *testing.T) {
	typeOf := reflect.TypeFor[*CallbackPublisher]()
	for _, forbidden := range []string{
		"PublishLifecycleReady", "PublishLifecycleSuccess",
		"PublishLifecycleDeprovisioned", "PublishLifecycleRetained",
	} {
		_, found := typeOf.MethodByName(forbidden)
		assert.False(t, found, "CallbackPublisher must not expose semantic method %s", forbidden)
	}
}

func TestCallbackPublisherExposesNoUnboundedBackgroundPublication(t *testing.T) {
	typeOf := reflect.TypeFor[*CallbackPublisher]()
	for _, forbidden := range []string{
		"PublishOperationSuccess", "PublishOperationFailure",
		"AuthorizeRuntimeObservation", "PublishLifecycleFailure",
		"PublishMaintenanceSuccess", "PublishMaintenanceFailure",
		"TryPublishMaintenanceSuccess", "TryPublishMaintenanceFailure",
		"TryPublishMaintenanceRuntimeFailure",
	} {
		_, found := typeOf.MethodByName(forbidden)
		assert.False(t, found, "CallbackPublisher must not expose unbounded method %s", forbidden)
	}
}
