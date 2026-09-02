package shared

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

func maintenanceFixture(
	t *testing.T,
	name string,
) (*ReleaseStore, *CallbackStore, ReleaseClaim, Release) {
	t.Helper()
	dir := t.TempDir()
	releases, err := NewReleaseStore(ReleaseStoreConfig{DBPath: filepath.Join(dir, "releases.db")})
	require.NoError(t, err)
	callbacks, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(dir, "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})
	leaseUUID := testLeaseUUID("maintenance-" + name)
	source := validRuntimeAuthorityRelease()
	require.NoError(t, releases.AppendActive(leaseUUID, source))
	active, sourceClaim, err := releases.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	return releases, callbacks, sourceClaim, target
}

func beginMaintenanceFixture(
	t *testing.T,
	name string,
) (*ReleaseStore, *CallbackStore, MaintenanceIntentClaim, MaintenanceAppendClaim) {
	t.Helper()
	releases, callbacks, source, target := maintenanceFixture(t, name)
	admission, err := callbacks.BeginMaintenanceIntent(MaintenanceIntentSpec{
		Kind:             MaintenanceIntentRestart,
		SourceRelease:    source,
		TargetRelease:    target,
		Backend:          "docker-a",
		BackendStorageID: callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	})
	require.NoError(t, err)
	appendClaim, err := callbacks.StartMaintenanceAppend(admission)
	require.NoError(t, err)
	return releases, callbacks, appendClaim.Intent(), appendClaim
}

func maintenanceSpecFromLatestActive(
	t *testing.T,
	releases *ReleaseStore,
	leaseUUID string,
	kind MaintenanceIntentKind,
) MaintenanceIntentSpec {
	t.Helper()
	active, source, err := releases.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.MaintenanceID = ""
	target.CreatedAt = time.Now()
	return MaintenanceIntentSpec{
		Kind:             kind,
		SourceRelease:    source,
		TargetRelease:    target,
		Backend:          "docker-a",
		BackendStorageID: callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	}
}

func TestMaintenanceIntentSurvivesEveryCommitBoundaryAndSettlesExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	releasePath := filepath.Join(dir, "releases.db")
	callbackPath := filepath.Join(dir, "callbacks.db")
	releases, err := NewReleaseStore(ReleaseStoreConfig{DBPath: releasePath})
	require.NoError(t, err)
	callbacks, err := NewCallbackStore(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	leaseUUID := testLeaseUUID("maintenance-restart-boundaries")
	require.NoError(t, releases.AppendActive(leaseUUID, validRuntimeAuthorityRelease()))
	active, source, err := releases.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()

	admission, err := callbacks.BeginMaintenanceIntent(MaintenanceIntentSpec{
		Kind:             MaintenanceIntentRestart,
		SourceRelease:    source,
		TargetRelease:    target,
		Backend:          "docker-a",
		BackendStorageID: callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	})
	require.NoError(t, err)
	require.True(t, admission.MaintenanceID().Valid())
	assert.Equal(t, admission.MaintenanceID(), admission.TargetRelease().MaintenanceID)
	require.NoError(t, releases.CheckAppendMaintenanceCapacity(admission))
	appendClaim, err := callbacks.StartMaintenanceAppend(admission)
	require.NoError(t, err)
	require.NoError(t, callbacks.Close())

	callbacks, err = NewCallbackStore(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	intents, err := callbacks.ListMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	targetClaim, err := releases.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	require.NoError(t, releases.Close())
	require.NoError(t, callbacks.Close())

	releases, err = NewReleaseStore(ReleaseStoreConfig{DBPath: releasePath})
	require.NoError(t, err)
	callbacks, err = NewCallbackStore(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})
	intents, err = callbacks.ListMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	intent := intents[0]
	_, recoveredTargetClaim, found, err := releases.FindMaintenanceRelease(
		leaseUUID, intent.MaintenanceID(),
	)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, targetClaim.Digest(), recoveredTargetClaim.Digest())
	intent, err = callbacks.BindMaintenanceIntentTarget(intent, recoveredTargetClaim)
	require.NoError(t, err)
	require.NoError(t, releases.ActivateMaintenance(recoveredTargetClaim))

	entry, err := callbacks.ResolveMaintenanceIntent(intent, backend.CallbackStatusSuccess, "")
	require.NoError(t, err)
	assert.Equal(t, CallbackDeliveryKindMaintenance, entry.DeliveryKind)
	assert.Equal(t, intent.LifecycleCallbackURL(), entry.CallbackURL)
	_, err = callbacks.ResolveMaintenanceIntent(intent, backend.CallbackStatusSuccess, "")
	require.ErrorContains(t, err, "no longer exists")
	intents, err = callbacks.ListMaintenanceIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	assert.Equal(t, CallbackDeliveryKindMaintenance, pending[0].DeliveryKind)
	activeAfter, err := releases.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, activeAfter)
	assert.Equal(t, intent.MaintenanceID(), activeAfter.MaintenanceID)
}

func TestBeginMaintenanceIntentWaitsForPriorCompletionAcrossReopenAndPreciseRemoval(t *testing.T) {
	dir := t.TempDir()
	releasePath := filepath.Join(dir, "releases.db")
	callbackPath := filepath.Join(dir, "callbacks.db")
	releases, err := NewReleaseStore(ReleaseStoreConfig{DBPath: releasePath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, releases.Close()) })
	callbacks, err := NewCallbackStore(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)

	leaseUUID := testLeaseUUID("maintenance-completion-admission-fence")
	require.NoError(t, releases.AppendActive(leaseUUID, validRuntimeAuthorityRelease()))
	spec := maintenanceSpecFromLatestActive(t, releases, leaseUUID, MaintenanceIntentRestart)

	// A non-maintenance observation may be older in the FIFO. The admission
	// guard must find the exact maintenance completion anywhere in the lease
	// queue rather than inspecting only its head.
	olderLifecycle, err := callbacks.StoreEntry(CallbackEntry{
		LeaseUUID:        leaseUUID,
		CallbackURL:      spec.TargetRelease.RuntimeAuthority.LifecycleCallbackURL(),
		DeliveryKind:     CallbackDeliveryKindLifecycle,
		Status:           backend.CallbackStatusFailed,
		Backend:          spec.Backend,
		BackendStorageID: spec.BackendStorageID.String(),
		Error:            "older runtime observation",
		CreatedAt:        time.Now(),
	})
	require.NoError(t, err)

	admission, err := callbacks.BeginMaintenanceIntent(spec)
	require.NoError(t, err, "an ordinary lifecycle observation is not an exact maintenance completion")
	appendClaim, err := callbacks.StartMaintenanceAppend(admission)
	require.NoError(t, err)
	target, err := releases.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	intent, err := callbacks.BindMaintenanceIntentTarget(appendClaim.Intent(), target)
	require.NoError(t, err)
	require.NoError(t, releases.ActivateMaintenance(target))
	completion, err := callbacks.ResolveMaintenanceIntent(
		intent, backend.CallbackStatusSuccess, "",
	)
	require.NoError(t, err)

	next := maintenanceSpecFromLatestActive(t, releases, leaseUUID, MaintenanceIntentUpdate)
	_, err = callbacks.BeginMaintenanceIntent(next)
	require.ErrorIs(t, err, backend.ErrInvalidState)
	require.ErrorContains(t, err, "pending delivery")

	// Removing another FIFO member cannot release the generation fence.
	require.NoError(t, callbacks.RemoveEntry(olderLifecycle))
	_, err = callbacks.BeginMaintenanceIntent(next)
	require.ErrorIs(t, err, backend.ErrInvalidState)

	require.NoError(t, callbacks.Close())
	callbacks, err = NewCallbackStore(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbacks.Close()) })
	_, err = callbacks.BeginMaintenanceIntent(next)
	require.ErrorIs(t, err, backend.ErrInvalidState,
		"the undelivered generation fence must survive backend restart")

	pending, err := callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, completion.DeliveryID, pending[0].DeliveryID)
	require.NoError(t, callbacks.RemoveEntry(pending[0]))

	nextAdmission, err := callbacks.BeginMaintenanceIntent(next)
	require.NoError(t, err, "precise removal after synchronous application releases admission")
	require.NoError(t, callbacks.CancelMaintenanceIntent(nextAdmission))
}

func TestResolveMaintenanceIntentAndNextBeginHaveNoAdmissionGap(t *testing.T) {
	releases, callbacks, _, _ := maintenanceFixture(t, "resolve-next-begin-race")
	const iterations = 32

	for i := range iterations {
		leaseUUID := testLeaseUUID(fmt.Sprintf("maintenance-generation-race-%d", i))
		require.NoError(t, releases.AppendActive(leaseUUID, validRuntimeAuthorityRelease()))
		firstSpec := maintenanceSpecFromLatestActive(t, releases, leaseUUID, MaintenanceIntentRestart)
		firstAdmission, err := callbacks.BeginMaintenanceIntent(firstSpec)
		require.NoError(t, err)
		appendClaim, err := callbacks.StartMaintenanceAppend(firstAdmission)
		require.NoError(t, err)
		target, err := releases.AppendMaintenance(appendClaim)
		require.NoError(t, err)
		intent, err := callbacks.BindMaintenanceIntentTarget(appendClaim.Intent(), target)
		require.NoError(t, err)
		require.NoError(t, releases.ActivateMaintenance(target))
		next := maintenanceSpecFromLatestActive(t, releases, leaseUUID, MaintenanceIntentUpdate)

		start := make(chan struct{})
		resolveErr := make(chan error, 1)
		beginErr := make(chan error, 1)
		go func() {
			<-start
			_, resolve := callbacks.ResolveMaintenanceIntent(
				intent, backend.CallbackStatusSuccess, "",
			)
			resolveErr <- resolve
		}()
		go func() {
			<-start
			_, begin := callbacks.BeginMaintenanceIntent(next)
			beginErr <- begin
		}()
		close(start)

		require.NoError(t, <-resolveErr)
		err = <-beginErr
		require.Error(t, err)
		assert.True(t,
			errors.Is(err, ErrMaintenanceIntentConflict) || errors.Is(err, backend.ErrInvalidState),
			"next admission must see either the unresolved intent or its atomic completion: %v", err,
		)
		intents, listErr := callbacks.ListMaintenanceIntents()
		require.NoError(t, listErr)
		assert.Empty(t, intents)
		pending, listErr := callbacks.ListPending()
		require.NoError(t, listErr)
		var exact CallbackEntry
		for _, entry := range pending {
			if entry.LeaseUUID == leaseUUID && entry.DeliveryKind == CallbackDeliveryKindMaintenance {
				exact = entry
				break
			}
		}
		require.NotEmpty(t, exact.DeliveryID)
		require.NoError(t, callbacks.RemoveEntry(exact))
	}
}

func TestSendMaintenanceCallback_DurableSettlementOnlyNotifiesReplay(t *testing.T) {
	releases, callbacks, intent, appendClaim := beginMaintenanceFixture(t, "enqueue-only")
	target, err := releases.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	intent, err = callbacks.BindMaintenanceIntentTarget(intent, target)
	require.NoError(t, err)
	require.NoError(t, releases.ActivateMaintenance(target))

	var requests atomic.Int32
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: callbacks,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		})},
		Secret:          "secret",
		Logger:          slog.Default(),
		StopCtx:         context.Background(),
		Backoff:         &zeroBackoff,
		StorageIdentity: intent.BackendStorageID(),
	})

	require.NoError(t, sender.SendMaintenanceCallback(
		intent, backend.CallbackStatusSuccess, "",
	))
	assert.Zero(t, requests.Load(),
		"maintenance settlement must not perform callback HTTP inline")
	assert.Len(t, sender.replayWake, 1,
		"maintenance settlement must wake its replay owner")
	_, found, err := callbacks.GetMaintenanceIntent(intent.LeaseUUID())
	require.NoError(t, err)
	assert.False(t, found, "intent removal and outbox publication must settle atomically")
	pending, err := callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, CallbackDeliveryKindMaintenance, pending[0].DeliveryKind)

	sender.ReplayPendingCallbacks()
	assert.Equal(t, int32(1), requests.Load())
	pending, err = callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendMaintenanceCallback_DoesNotWaitForSlowReplay(t *testing.T) {
	releases, callbacks, intent, appendClaim := beginMaintenanceFixture(t, "slow-replay")
	target, err := releases.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	intent, err = callbacks.BindMaintenanceIntentTarget(intent, target)
	require.NoError(t, err)
	require.NoError(t, releases.ActivateMaintenance(target))

	_, err = callbacks.storeValidTestEntry(CallbackEntry{
		LeaseUUID:        intent.LeaseUUID(),
		CallbackURL:      intent.LifecycleCallbackURL(),
		DeliveryKind:     CallbackDeliveryKindLifecycle,
		Status:           backend.CallbackStatusFailed,
		Backend:          "docker-a",
		BackendStorageID: intent.BackendStorageID().String(),
		CreatedAt:        time.Now(),
	})
	require.NoError(t, err)

	headStarted := make(chan struct{})
	releaseHead := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseHead) }) })
	var requests atomic.Int32
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: callbacks,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			if requests.Add(1) == 1 {
				close(headStarted)
				<-releaseHead
			}
			return callbackHTTPResponse(http.StatusNoContent), nil
		})},
		Secret:          "secret",
		Logger:          slog.Default(),
		StopCtx:         context.Background(),
		Backoff:         &zeroBackoff,
		StorageIdentity: intent.BackendStorageID(),
	})

	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		sender.ReplayPendingCallbacks()
	}()
	select {
	case <-headStarted:
	case <-time.After(time.Second):
		t.Fatal("replay did not begin the older lifecycle callback")
	}

	settled := make(chan error, 1)
	go func() {
		settled <- sender.SendMaintenanceCallback(
			intent, backend.CallbackStatusSuccess, "",
		)
	}()
	select {
	case err = <-settled:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("maintenance settlement waited behind callback HTTP")
	}
	_, found, err := callbacks.GetMaintenanceIntent(intent.LeaseUUID())
	require.NoError(t, err)
	assert.False(t, found, "settlement must consume the intent before HTTP completes")
	pending, err := callbacks.listPending(intent.LeaseUUID())
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[0].DeliveryKind)
	assert.Equal(t, CallbackDeliveryKindMaintenance, pending[1].DeliveryKind)

	releaseOnce.Do(func() { close(releaseHead) })
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("same replay did not drain the appended maintenance completion")
	}
	assert.Equal(t, int32(2), requests.Load())
	pending, err = callbacks.listPending(intent.LeaseUUID())
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestMaintenanceIntentUnboundFailureAndBoundCancellationFence(t *testing.T) {
	t.Run("before append start can be canceled", func(t *testing.T) {
		_, callbacks, source, target := maintenanceFixture(t, "cancelable-admission")
		admission, err := callbacks.BeginMaintenanceIntent(MaintenanceIntentSpec{
			Kind:             MaintenanceIntentRestart,
			SourceRelease:    source,
			TargetRelease:    target,
			Backend:          "docker-a",
			BackendStorageID: callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
		})
		require.NoError(t, err)
		require.NoError(t, callbacks.CancelMaintenanceIntent(admission))
		_, found, err := callbacks.GetMaintenanceIntent(admission.LeaseUUID())
		require.NoError(t, err)
		require.False(t, found)
		_, err = callbacks.StartMaintenanceAppend(admission)
		require.ErrorContains(t, err, "no longer exists")
	})

	t.Run("before target append resolves failure", func(t *testing.T) {
		_, callbacks, intent, _ := beginMaintenanceFixture(t, "unbound-failure")
		_, err := callbacks.ResolveMaintenanceIntent(intent, backend.CallbackStatusSuccess, "")
		require.ErrorContains(t, err, "unbound")
		_, err = callbacks.ResolveMaintenanceIntent(intent, backend.CallbackStatusFailed, "restart interrupted")
		require.NoError(t, err)
	})

	t.Run("append-started admission cannot be canceled", func(t *testing.T) {
		releases, callbacks, source, targetTemplate := maintenanceFixture(t, "bound-cancel")
		admission, err := callbacks.BeginMaintenanceIntent(MaintenanceIntentSpec{
			Kind:             MaintenanceIntentRestart,
			SourceRelease:    source,
			TargetRelease:    targetTemplate,
			Backend:          "docker-a",
			BackendStorageID: callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
		})
		require.NoError(t, err)
		appendClaim, err := callbacks.StartMaintenanceAppend(admission)
		require.NoError(t, err)
		// This is the cross-store window: callback WAL advancement committed,
		// but releases.db has not. A copied pre-start admission must already be
		// powerless to orphan the only recovery index.
		require.ErrorContains(t, callbacks.CancelMaintenanceIntent(admission), "changed")
		_, _, found, err := releases.FindMaintenanceRelease(
			admission.LeaseUUID(), admission.MaintenanceID(),
		)
		require.NoError(t, err)
		require.False(t, found)
		target, err := releases.AppendMaintenance(appendClaim)
		require.NoError(t, err)
		bound, err := callbacks.BindMaintenanceIntentTarget(appendClaim.Intent(), target)
		require.NoError(t, err)
		require.True(t, bound.Valid())
		claims, err := callbacks.ListMaintenanceIntents()
		require.NoError(t, err)
		require.Len(t, claims, 1)
	})
}

func TestTryResolveMaintenanceIntentDefersBehindConcurrentMutation(t *testing.T) {
	releases, callbacks, intent, appendClaim := beginMaintenanceFixture(t, "try-resolve-busy")
	target, err := releases.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	intent, err = callbacks.BindMaintenanceIntentTarget(intent, target)
	require.NoError(t, err)
	require.NoError(t, releases.ActivateMaintenance(target))

	unlock := callbacks.lockDeliveryLease(intent.LeaseUUID())
	_, acquired, err := callbacks.TryResolveMaintenanceIntent(
		intent, backend.CallbackStatusSuccess, "",
	)
	require.NoError(t, err)
	require.False(t, acquired)
	claims, err := callbacks.ListMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	pending, err := callbacks.ListPending()
	require.NoError(t, err)
	require.Empty(t, pending)
	unlock()

	_, acquired, err = callbacks.TryResolveMaintenanceIntent(
		intent, backend.CallbackStatusSuccess, "",
	)
	require.NoError(t, err)
	require.True(t, acquired)
	pending, err = callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
}

func TestTryBindMaintenanceIntentTargetDefersBehindConcurrentMutation(t *testing.T) {
	releases, callbacks, intent, appendClaim := beginMaintenanceFixture(t, "try-bind-busy")
	target, err := releases.AppendMaintenance(appendClaim)
	require.NoError(t, err)

	unlock := callbacks.lockDeliveryLease(intent.LeaseUUID())
	bound, acquired, err := callbacks.TryBindMaintenanceIntentTarget(intent, target)
	require.NoError(t, err)
	require.False(t, acquired)
	require.False(t, bound.Valid())
	stored, found, err := callbacks.GetMaintenanceIntent(intent.LeaseUUID())
	require.NoError(t, err)
	require.True(t, found)
	_, targetBound := stored.TargetReleaseClaim()
	assert.False(t, targetBound, "busy binding must leave the exact intent untouched")
	unlock()

	bound, acquired, err = callbacks.TryBindMaintenanceIntentTarget(intent, target)
	require.NoError(t, err)
	require.True(t, acquired)
	require.True(t, bound.Valid())
	boundTarget, targetBound := bound.TargetReleaseClaim()
	require.True(t, targetBound)
	assert.Equal(t, target.Version(), boundTarget.Version())
	assert.Equal(t, target.Digest(), boundTarget.Digest())
}

func TestTryResolveMaintenanceIntentWithRuntimeFailureIsAtomicAndOrdered(t *testing.T) {
	dir := t.TempDir()
	releasePath := filepath.Join(dir, "releases.db")
	callbackPath := filepath.Join(dir, "callbacks.db")
	releases, err := NewReleaseStore(ReleaseStoreConfig{DBPath: releasePath})
	require.NoError(t, err)
	callbacks, err := NewCallbackStore(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})

	leaseUUID := testLeaseUUID("maintenance-runtime-divergence")
	require.NoError(t, releases.AppendActive(leaseUUID, validRuntimeAuthorityRelease()))
	active, source, err := releases.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	admission, err := callbacks.BeginMaintenanceIntent(MaintenanceIntentSpec{
		Kind:             MaintenanceIntentUpdate,
		SourceRelease:    source,
		TargetRelease:    target,
		Backend:          "docker-a",
		BackendStorageID: callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	})
	require.NoError(t, err)
	appendClaim, err := callbacks.StartMaintenanceAppend(admission)
	require.NoError(t, err)
	intent := appendClaim.Intent()
	targetClaim, err := releases.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	intent, err = callbacks.BindMaintenanceIntentTarget(intent, targetClaim)
	require.NoError(t, err)
	require.NoError(t, releases.ActivateMaintenance(targetClaim))

	// A concurrent journal mutation owns the same lease lock. The try form must
	// leave both journals byte-for-byte in their pre-settlement state.
	unlock := callbacks.lockDeliveryLease(leaseUUID)
	acquired, err := callbacks.TryResolveMaintenanceIntentWithRuntimeFailure(
		intent, "committed runtime cohort is missing",
	)
	require.NoError(t, err)
	require.False(t, acquired)
	_, found, err := callbacks.GetMaintenanceIntent(leaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	pending, err := callbacks.ListPending()
	require.NoError(t, err)
	require.Empty(t, pending)
	unlock()

	acquired, err = callbacks.TryResolveMaintenanceIntentWithRuntimeFailure(
		intent, "committed runtime cohort is missing",
	)
	require.NoError(t, err)
	require.True(t, acquired)

	// Reopen the database to prove that intent removal and both ordered facts
	// share one durable transaction rather than merely one process snapshot.
	require.NoError(t, callbacks.Close())
	callbacks, err = NewCallbackStore(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	_, found, err = callbacks.GetMaintenanceIntent(leaseUUID)
	require.NoError(t, err)
	require.False(t, found)
	pending, err = callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, CallbackDeliveryKindMaintenance, pending[0].DeliveryKind)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	assert.Equal(t, CallbackDeliveryKindMaintenance, pending[1].DeliveryKind)
	assert.Equal(t, backend.CallbackStatusFailed, pending[1].Status)
	assert.Equal(t, "committed runtime cohort is missing", pending[1].Error)
	assert.Less(t, pending[0].Sequence, pending[1].Sequence)

	// Delivering only the Success head must not admit a newer maintenance
	// generation ahead of its paired runtime-failure fact. Both rows are exact
	// barriers even though the second carries lifecycle Failed on the wire.
	require.NoError(t, callbacks.RemoveEntry(pending[0]))
	latest, nextSource, err := releases.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	nextTarget := cloneRelease(latest)
	nextTarget.Version = 0
	nextTarget.MaintenanceID = ""
	nextTarget.Status = "deploying"
	nextTarget.CreatedAt = time.Now()
	_, err = callbacks.BeginMaintenanceIntent(MaintenanceIntentSpec{
		Kind:             MaintenanceIntentRestart,
		SourceRelease:    nextSource,
		TargetRelease:    nextTarget,
		Backend:          "docker-a",
		BackendStorageID: callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	})
	require.ErrorIs(t, err, backend.ErrInvalidState)
	require.NoError(t, callbacks.RemoveEntry(pending[1]))
	nextAdmission, err := callbacks.BeginMaintenanceIntent(MaintenanceIntentSpec{
		Kind:             MaintenanceIntentRestart,
		SourceRelease:    nextSource,
		TargetRelease:    nextTarget,
		Backend:          "docker-a",
		BackendStorageID: callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	})
	require.NoError(t, err)
	require.NoError(t, callbacks.CancelMaintenanceIntent(nextAdmission))
}

func TestMaintenanceReleaseExactMutationNeverTargetsLatest(t *testing.T) {
	releases, callbacks, firstIntent, firstAppend := beginMaintenanceFixture(t, "not-latest")
	firstTarget, err := releases.AppendMaintenance(firstAppend)
	require.NoError(t, err)
	firstIntent, err = callbacks.BindMaintenanceIntentTarget(firstIntent, firstTarget)
	require.NoError(t, err)
	require.NoError(t, releases.ActivateMaintenance(firstTarget))
	completion, err := callbacks.ResolveMaintenanceIntent(firstIntent, backend.CallbackStatusSuccess, "")
	require.NoError(t, err)
	// A protected exact completion deliberately blocks a later maintenance
	// admission until delivery consumes that precise row. Model the successful
	// delivery before exercising the next generation.
	require.NoError(t, callbacks.RemoveEntry(completion))

	active, secondSource, err := releases.ClaimLatestActive(firstIntent.LeaseUUID())
	require.NoError(t, err)
	secondTemplate := cloneRelease(active)
	secondTemplate.Version = 0
	secondTemplate.MaintenanceID = ""
	secondTemplate.Status = "deploying"
	secondTemplate.CreatedAt = time.Now().Add(time.Second)
	secondAdmission, err := callbacks.BeginMaintenanceIntent(MaintenanceIntentSpec{
		Kind:             MaintenanceIntentUpdate,
		SourceRelease:    secondSource,
		TargetRelease:    secondTemplate,
		Backend:          "docker-a",
		BackendStorageID: callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	})
	require.NoError(t, err)
	secondAppend, err := callbacks.StartMaintenanceAppend(secondAdmission)
	require.NoError(t, err)
	secondTarget, err := releases.AppendMaintenance(secondAppend)
	require.NoError(t, err)

	// Replaying the first claim must remain an idempotent operation on its exact
	// row; it cannot activate the newer deploying generation.
	require.NoError(t, releases.ActivateMaintenance(firstTarget))
	list, err := releases.List(firstIntent.LeaseUUID())
	require.NoError(t, err)
	require.Len(t, list, 3)
	assert.Equal(t, "active", list[1].Status)
	assert.Equal(t, firstIntent.MaintenanceID(), list[1].MaintenanceID)
	assert.Equal(t, "deploying", list[2].Status)
	assert.Equal(t, secondTarget.MaintenanceID(), list[2].MaintenanceID)
}

func TestMaintenanceReleaseRejectsRawMutationBypasses(t *testing.T) {
	t.Run("append APIs require typed admission", func(t *testing.T) {
		releases, _, intent, _ := beginMaintenanceFixture(t, "raw-append")
		target := intent.TargetRelease()
		require.ErrorIs(t, releases.Append(intent.LeaseUUID(), target), ErrMaintenanceReleaseClaimRequired)
		target.Status = "active"
		require.ErrorIs(t, releases.CheckAppendActiveCapacity(intent.LeaseUUID(), target), ErrMaintenanceReleaseClaimRequired)
		require.ErrorIs(t, releases.AppendActive(intent.LeaseUUID(), target), ErrMaintenanceReleaseClaimRequired)
		history, err := releases.List(intent.LeaseUUID())
		require.NoError(t, err)
		require.Len(t, history, 1)
	})

	t.Run("latest and raw delete APIs cannot mutate a typed target", func(t *testing.T) {
		releases, _, intent, appendClaim := beginMaintenanceFixture(t, "raw-terminal")
		target, err := releases.AppendMaintenance(appendClaim)
		require.NoError(t, err)
		raw := validRuntimeAuthorityRelease()
		raw.Status = "failed"
		raw.CreatedAt = time.Now().Add(time.Second)
		require.ErrorIs(t, releases.Append(intent.LeaseUUID(), raw), ErrMaintenanceReleaseClaimRequired)
		raw.Status = "active"
		require.ErrorIs(t,
			releases.CheckAppendActiveCapacity(intent.LeaseUUID(), raw),
			ErrMaintenanceReleaseClaimRequired,
		)
		require.ErrorIs(t, releases.AppendActive(intent.LeaseUUID(), raw), ErrMaintenanceReleaseClaimRequired)
		require.ErrorIs(t,
			releases.UpdateLatestStatus(intent.LeaseUUID(), "failed", backend.ReasonUpdateFailed, "bypass"),
			ErrMaintenanceReleaseClaimRequired,
		)
		require.ErrorIs(t, releases.ActivateLatest(intent.LeaseUUID()), ErrMaintenanceReleaseClaimRequired)
		targetRelease := intent.TargetRelease()
		require.ErrorIs(t, releases.BackfillActiveResourceProfiles(
			intent.LeaseUUID(),
			intent.SourceRelease().Version(),
			targetRelease.Items,
			targetRelease.ResourceProfiles,
		), ErrMaintenanceReleaseClaimRequired)
		require.ErrorIs(t, releases.CheckRecordMigrationCapacity(
			intent.LeaseUUID(),
			targetRelease.Manifest,
			targetRelease.Items,
			targetRelease.ResourceProfiles,
			time.Now(),
		), ErrMaintenanceReleaseClaimRequired)
		require.ErrorIs(t, releases.RecordMigration(
			intent.LeaseUUID(),
			targetRelease.Manifest,
			targetRelease.Items,
			targetRelease.ResourceProfiles,
		), ErrMaintenanceReleaseClaimRequired)
		require.ErrorIs(t, releases.Delete(intent.LeaseUUID()), ErrMaintenanceReleaseClaimRequired)

		release, _, found, err := releases.FindMaintenanceRelease(intent.LeaseUUID(), intent.MaintenanceID())
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "deploying", release.Status)
		require.NoError(t, releases.ActivateMaintenance(target))
	})
}

func TestMaintenanceReleaseTerminalHistoryDoesNotBlockLaterGeneration(t *testing.T) {
	for _, terminal := range []string{"active", "failed"} {
		t.Run(terminal, func(t *testing.T) {
			releases, callbacks, intent, appendClaim := beginMaintenanceFixture(t, "terminal-followed-by-"+terminal)
			target, err := releases.AppendMaintenance(appendClaim)
			require.NoError(t, err)
			intent, err = callbacks.BindMaintenanceIntentTarget(intent, target)
			require.NoError(t, err)
			if terminal == "active" {
				require.NoError(t, releases.ActivateMaintenance(target))
			} else {
				require.NoError(t, releases.FailMaintenance(
					target, backend.ReasonUpdateFailed, "maintenance failed",
				))
			}

			next := validRuntimeAuthorityRelease()
			next.CreatedAt = time.Now().Add(time.Second)
			require.NoError(t, releases.CheckAppendActiveCapacity(intent.LeaseUUID(), next))
			require.NoError(t, releases.AppendActive(intent.LeaseUUID(), next))

			latest, err := releases.LatestActive(intent.LeaseUUID())
			require.NoError(t, err)
			require.NotNil(t, latest)
			assert.Empty(t, latest.MaintenanceID)
			assert.Equal(t, next.OperationID, latest.OperationID)
		})
	}
}

func TestMaintenanceAppendPreservesRuntimeAuthorityIdentity(t *testing.T) {
	t.Run("trusted callback route base may rotate", func(t *testing.T) {
		releases, callbacks, source, target := maintenanceFixture(t, "rotated-route-base")
		operationID := target.OperationID
		authority, err := NewReleaseRuntimeAuthority(
			operationID,
			target.RuntimeAuthority.Tenant(),
			target.RuntimeAuthority.ProviderUUID(),
			"https://rotated.example/callbacks/provision?operation_id="+operationID.String(),
			"https://rotated.example/callbacks/provision?lifecycle_id="+operationID.String(),
		)
		require.NoError(t, err)
		target.RuntimeAuthority = &authority
		intent, err := callbacks.BeginMaintenanceIntent(MaintenanceIntentSpec{
			Kind:             MaintenanceIntentRestart,
			SourceRelease:    source,
			TargetRelease:    target,
			Backend:          "docker-a",
			BackendStorageID: callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
		})
		require.NoError(t, err)
		require.NoError(t, releases.CheckAppendMaintenanceCapacity(intent))
	})

	for _, test := range []struct {
		name      string
		wantError string
		mutate    func(*testing.T, *Release)
	}{
		{
			name:      "tenant",
			wantError: "changes tenant authority",
			mutate: func(t *testing.T, target *Release) {
				authority, err := NewReleaseRuntimeAuthority(
					target.OperationID,
					"tenant-b",
					target.RuntimeAuthority.ProviderUUID(),
					target.RuntimeAuthority.CallbackURL(),
					target.RuntimeAuthority.LifecycleCallbackURL(),
				)
				require.NoError(t, err)
				target.RuntimeAuthority = &authority
			},
		},
		{
			name:      "provider",
			wantError: "changes provider authority",
			mutate: func(t *testing.T, target *Release) {
				authority, err := NewReleaseRuntimeAuthority(
					target.OperationID,
					target.RuntimeAuthority.Tenant(),
					"33333333-3333-4333-8333-333333333333",
					target.RuntimeAuthority.CallbackURL(),
					target.RuntimeAuthority.LifecycleCallbackURL(),
				)
				require.NoError(t, err)
				target.RuntimeAuthority = &authority
			},
		},
		{
			name:      "operation ID",
			wantError: "changes operation lineage",
			mutate: func(t *testing.T, target *Release) {
				operationID := OperationID(uuid.NewString())
				authority, err := NewReleaseRuntimeAuthority(
					operationID,
					target.RuntimeAuthority.Tenant(),
					target.RuntimeAuthority.ProviderUUID(),
					"https://fred.example/callbacks/provision?operation_id="+operationID.String(),
					"https://fred.example/callbacks/provision?lifecycle_id="+operationID.String(),
				)
				require.NoError(t, err)
				target.OperationID = operationID
				target.RuntimeAuthority = &authority
			},
		},
	} {
		t.Run("rejects "+test.name+" divergence", func(t *testing.T) {
			releases, callbacks, source, target := maintenanceFixture(t, "authority-"+test.name)
			test.mutate(t, &target)
			intent, err := callbacks.BeginMaintenanceIntent(MaintenanceIntentSpec{
				Kind:             MaintenanceIntentRestart,
				SourceRelease:    source,
				TargetRelease:    target,
				Backend:          "docker-a",
				BackendStorageID: callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
			})
			require.NoError(t, err)
			require.ErrorContains(t, releases.CheckAppendMaintenanceCapacity(intent), test.wantError)
		})
	}

	t.Run("constructor rejects malformed mixed callback pair", func(t *testing.T) {
		operationID := OperationID(uuid.NewString())
		_, err := NewReleaseRuntimeAuthority(
			operationID,
			"tenant-a",
			"22222222-2222-4222-8222-222222222222",
			"https://fred.example/callbacks/provision?operation_id="+operationID.String(),
			"https://fred.example/callbacks/provision?lifecycle_id="+uuid.NewString(),
		)
		require.Error(t, err)
	})
}

func TestMaintenanceIntentRejectsOperationAndCloseOverlap(t *testing.T) {
	t.Run("operation already owns lease", func(t *testing.T) {
		releases, callbacks, source, target := maintenanceFixture(t, "operation-first")
		_ = releases
		op := testOperationIntentSpec(t, "maintenance-operation-first")
		op.LeaseUUID = source.LeaseUUID()
		op.Backend = "docker-a"
		op.BackendStorageID = callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000")
		_, err := callbacks.BeginOperationIntent(op)
		require.NoError(t, err)
		_, err = callbacks.BeginMaintenanceIntent(MaintenanceIntentSpec{
			Kind:             MaintenanceIntentRestart,
			SourceRelease:    source,
			TargetRelease:    target,
			Backend:          "docker-a",
			BackendStorageID: op.BackendStorageID,
		})
		require.ErrorIs(t, err, ErrMaintenanceIntentConflict)
	})

	t.Run("maintenance already owns lease", func(t *testing.T) {
		_, callbacks, intent, _ := beginMaintenanceFixture(t, "maintenance-first")
		op := testOperationIntentSpec(t, "maintenance-second-operation")
		op.LeaseUUID = intent.LeaseUUID()
		op.Backend = intent.Backend()
		op.BackendStorageID = intent.BackendStorageID()
		_, err := callbacks.BeginOperationIntent(op)
		require.ErrorIs(t, err, ErrOperationIntentConflict)
	})
}

func TestBeginCloseIntentAtomicallyPreemptsMaintenanceBeforeCloseDelivery(t *testing.T) {
	releases, callbacks, intent, _ := beginMaintenanceFixture(t, "close-preempt")
	_ = releases
	closeSpec := testCloseIntentSpec(t, "maintenance-preempt")
	closeSpec.LeaseUUID = intent.LeaseUUID()
	closeSpec.Backend = intent.Backend()
	closeSpec.BackendStorageID = intent.BackendStorageID()
	closeSpec.Tenant = intent.Tenant()
	closeSpec.ProviderUUID = intent.ProviderUUID()
	closeSpec.CallbackURL = intent.CallbackURL()
	closeSpec.LifecycleCallbackURL = intent.LifecycleCallbackURL()
	closeSpec.ActiveReleaseVersion = intent.SourceRelease().Version()
	closeSpec.ActiveReleaseDigest = intent.SourceRelease().Digest()

	admission, err := callbacks.BeginCloseIntent(closeSpec)
	require.NoError(t, err)
	assert.True(t, admission.MaintenancePreempted)
	intents, err := callbacks.ListMaintenanceIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Contains(t, pending[0].Error, "preempted")

	_, err = callbacks.ResolveCloseIntent(admission.Claim, backend.CallbackStatusDeprovisioned, "", false)
	require.NoError(t, err)
	pending, err = callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Less(t, pending[0].Sequence, pending[1].Sequence)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, backend.CallbackStatusDeprovisioned, pending[1].Status)
}

func TestBeginCloseIntentRejectsMaintenanceWithoutExactSourceFence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*CloseIntentSpec)
	}{
		{name: "zero fence", mutate: func(spec *CloseIntentSpec) {
			spec.ActiveReleaseVersion = 0
			spec.ActiveReleaseDigest = [sha256.Size]byte{}
			spec.LegacyRollbackTargets = nil
		}},
		{name: "stale version", mutate: func(spec *CloseIntentSpec) {
			spec.ActiveReleaseVersion++
		}},
		{name: "stale digest", mutate: func(spec *CloseIntentSpec) {
			spec.ActiveReleaseDigest = sha256.Sum256([]byte("stale"))
		}},
		{name: "cleanup only", mutate: func(spec *CloseIntentSpec) {
			spec.CleanupOnly = true
			spec.Tenant = ""
			spec.ProviderUUID = ""
			spec.CallbackURL = ""
			spec.LifecycleCallbackURL = ""
			spec.RetainOnClose = false
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, callbacks, intent, _ := beginMaintenanceFixture(t, "close-fence-"+test.name)
			spec := testCloseIntentSpec(t, "maintenance-fence-"+test.name)
			spec.LeaseUUID = intent.LeaseUUID()
			spec.Backend = intent.Backend()
			spec.BackendStorageID = intent.BackendStorageID()
			spec.Tenant = intent.Tenant()
			spec.ProviderUUID = intent.ProviderUUID()
			spec.CallbackURL = intent.CallbackURL()
			spec.LifecycleCallbackURL = intent.LifecycleCallbackURL()
			spec.ActiveReleaseVersion = intent.SourceRelease().Version()
			spec.ActiveReleaseDigest = intent.SourceRelease().Digest()
			test.mutate(&spec)
			_, err := callbacks.BeginCloseIntent(spec)
			require.ErrorContains(t, err, "does not fence the maintenance source release")
			claims, listErr := callbacks.ListMaintenanceIntents()
			require.NoError(t, listErr)
			require.Len(t, claims, 1)
			closes, listErr := callbacks.ListCloseIntents()
			require.NoError(t, listErr)
			assert.Empty(t, closes)
		})
	}

	t.Run("already active target is not fabricated as failure", func(t *testing.T) {
		releases, callbacks, intent, appendClaim := beginMaintenanceFixture(t, "close-active-target")
		target, err := releases.AppendMaintenance(appendClaim)
		require.NoError(t, err)
		intent, err = callbacks.BindMaintenanceIntentTarget(intent, target)
		require.NoError(t, err)
		require.NoError(t, releases.ActivateMaintenance(target))
		_, active, err := releases.ClaimLatestActive(intent.LeaseUUID())
		require.NoError(t, err)

		spec := testCloseIntentSpec(t, "maintenance-active-target")
		spec.LeaseUUID = intent.LeaseUUID()
		spec.Backend = intent.Backend()
		spec.BackendStorageID = intent.BackendStorageID()
		spec.Tenant = intent.Tenant()
		spec.ProviderUUID = intent.ProviderUUID()
		spec.CallbackURL = intent.CallbackURL()
		spec.LifecycleCallbackURL = intent.LifecycleCallbackURL()
		spec.ActiveReleaseVersion = active.Version()
		spec.ActiveReleaseDigest = active.Digest()
		_, err = callbacks.BeginCloseIntent(spec)
		require.ErrorContains(t, err, "does not fence the maintenance source release")
		pending, listErr := callbacks.ListPending()
		require.NoError(t, listErr)
		assert.Empty(t, pending)
	})
}

func TestMaintenanceIntentCorruptTargetDigestFailsClosed(t *testing.T) {
	_, callbacks, intent, _ := beginMaintenanceFixture(t, "corrupt-digest")
	require.NoError(t, callbacks.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackMaintenanceIntentBucketName)
		value := bucket.Get([]byte(intent.LeaseUUID()))
		var entry maintenanceIntentEntry
		if err := json.Unmarshal(value, &entry); err != nil {
			return err
		}
		entry.TargetReleaseVersion = 2
		entry.TargetReleaseDigest = encodeMaintenanceDigest(sha256.Sum256([]byte("wrong")))
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(intent.LeaseUUID()), data)
	}))
	_, err := callbacks.ListMaintenanceIntents()
	require.ErrorContains(t, err, "does not match target template")
	require.Error(t, callbacks.Healthy())
}
