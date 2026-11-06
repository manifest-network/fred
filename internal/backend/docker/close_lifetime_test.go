package docker

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// A close runs on the actor's lifetime, and its caller only waits. A caller
// that stops waiting learns the close is pending while teardown keeps stopping
// containers. A retry queued behind the running close reaches an actor that
// retires once the close completes; that actor rejects it unhandled, so the
// retry is routed to the replacement actor and answered there, not told to
// retry.
func TestCloseOutlivesItsCallerAndAnswersTheRetryQueuedBehindIt(t *testing.T) {
	b, stores := openCloseRecoveryBackend(t, t.TempDir(), &mockDockerClient{}, nil)
	seedCloseDeprovisionLease(t, b, stores)
	teardown := make(chan context.Context, 1)
	releaseTeardown := make(chan struct{})
	b.compose = &mockComposeExecutor{DownFn: func(ctx context.Context, _ string, _ time.Duration) error {
		teardown <- ctx
		<-releaseTeardown
		return ctx.Err()
	}}

	waiting, stopWaiting := context.WithCancel(t.Context())
	first := make(chan error, 1)
	go func() { first <- b.Deprovision(waiting, closeDeprovisionLeaseUUID) }()
	var teardownCtx context.Context
	select {
	case teardownCtx = <-teardown:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not reach teardown")
	}
	stopWaiting()
	select {
	case err := <-first:
		require.True(t, IsLifecyclePending(err), "a caller that stops waiting learns the close is pending: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("close kept its caller waiting after the caller stopped")
	}
	require.NoError(t, teardownCtx.Err(), "the caller stopping its wait must not cancel the teardown")
	deadline, bounded := teardownCtx.Deadline()
	require.True(t, bounded, "a close attempt is bounded")
	require.False(t, deadline.After(time.Now().Add(b.closeAttemptTimeout())))

	closing := b.actorFor(closeDeprovisionLeaseUUID)
	retry := make(chan error, 1)
	go func() { retry <- b.Deprovision(t.Context(), closeDeprovisionLeaseUUID) }()
	require.Eventually(t, func() bool { return closing.InboxDepth() == 1 },
		5*time.Second, time.Millisecond, "the retry must queue behind the running close")
	close(releaseTeardown)
	select {
	case err := <-retry:
		require.NoError(t, err, "a retry the retiring actor rejected unhandled is answered by its replacement")
	case <-time.After(5 * time.Second):
		t.Fatal("the retry queued behind the close was not answered")
	}
	select {
	case <-closing.Done():
	default:
		t.Fatal("the completed close must have retired its actor")
	}
	_, found, err := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.False(t, found, "the close settled")
	closeCloseRecoveryBackend(t, b, stores)
}

// A close takes over the lease's command fence from the caller that checked it
// may begin, and keeps it until its durable head commits, whatever its caller
// does. Releasing it when the caller stopped waiting would let a restore admit
// the lease between that check and the head. Teardown then runs without the
// fence, and a queued close that never runs gives it back.
func TestCloseKeepsTheCommandFenceUntilItsHeadCommits(t *testing.T) {
	b, stores := openCloseRecoveryBackend(t, t.TempDir(), &mockDockerClient{}, nil)
	seedCloseDeprovisionLease(t, b, stores)
	tearingDown := make(chan struct{}, 1)
	finishTeardown := make(chan struct{})
	b.compose = &mockComposeExecutor{DownFn: func(ctx context.Context, _ string, _ time.Duration) error {
		tearingDown <- struct{}{}
		<-finishTeardown
		return ctx.Err()
	}}
	fenceFree := func() bool {
		fence, free := b.commandFence.TryLock(closeDeprovisionLeaseUUID)
		fence.Release()
		return free
	}

	first, stopFirst := context.WithCancel(t.Context())
	defer stopFirst()
	firstDone := make(chan error, 1)
	go func() { firstDone <- b.Deprovision(first, closeDeprovisionLeaseUUID) }()
	select {
	case <-tearingDown:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not reach teardown")
	}
	require.True(t, fenceFree(), "a committed close head gives the fence back before teardown")
	_, found, err := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.True(t, found)

	closing := b.actorFor(closeDeprovisionLeaseUUID)
	second, stopSecond := context.WithCancel(t.Context())
	secondDone := make(chan error, 1)
	go func() { secondDone <- b.Deprovision(second, closeDeprovisionLeaseUUID) }()
	require.Eventually(t, func() bool { return closing.InboxDepth() == 1 },
		5*time.Second, time.Millisecond, "the second close must queue behind the running one")
	stopSecond()
	select {
	case err := <-secondDone:
		require.True(t, IsLifecyclePending(err), "%v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("the second close kept its caller waiting")
	}
	require.False(t, fenceFree(), "a queued close keeps the fence after its caller stops waiting")

	close(finishTeardown)
	require.NoError(t, <-firstDone)
	require.Eventually(t, fenceFree, 5*time.Second, time.Millisecond,
		"the retiring actor gives back the fence of the close it never ran")
	closeCloseRecoveryBackend(t, b, stores)
}

// While other lifecycle work holds the lease's command fence, a close caller
// stops waiting at its deadline with a pending answer instead of outlasting it.
func TestCloseCallerStopsWaitingForAHeldCommandFence(t *testing.T) {
	b, stores := openCloseRecoveryBackend(t, t.TempDir(), &mockDockerClient{}, nil)
	seedCloseDeprovisionLease(t, b, stores)
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error { return nil }}
	unlock := b.commandFence.Lock(closeDeprovisionLeaseUUID).Release

	waiting, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err := b.Deprovision(waiting, closeDeprovisionLeaseUUID)
	require.True(t, IsLifecyclePending(err), "a close blocked on the fence answers pending: %v", err)
	_, found, readErr := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.False(t, found, "nothing ran while the fence was held")

	unlock()
	require.NoError(t, b.Deprovision(t.Context(), closeDeprovisionLeaseUUID))
	closeCloseRecoveryBackend(t, b, stores)
}

// A close caller waiting behind another storage identity verification stops at
// its own deadline instead of outlasting it.
func TestCloseCallerStopsWaitingForIdentityVerification(t *testing.T) {
	b, stores := openCloseRecoveryBackend(t, t.TempDir(), &mockDockerClient{}, nil)
	seedCloseDeprovisionLease(t, b, stores)
	fixtureVerifier := b.storageVerifier
	b.storageVerifier = productionDockerStorageIdentityVerifier{backend: b, authority: b.storageAuthority}
	b.identityVerifyMu.lock()
	waiting, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := b.Deprovision(waiting, closeDeprovisionLeaseUUID)
	require.Less(t, time.Since(started), 5*time.Second, "the caller outlasted its deadline")
	require.True(t, IsLifecyclePending(err), "%v", err)
	b.identityVerifyMu.unlock()
	b.storageVerifier = fixtureVerifier
	require.NoError(t, b.terminalStorageAuthorityError(), "a caller giving up is not an identity failure")
	_, found, readErr := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, readErr)
	require.False(t, found, "nothing ran before identity was verified")
	require.NoError(t, b.Deprovision(t.Context(), closeDeprovisionLeaseUUID))
	closeCloseRecoveryBackend(t, b, stores)
}

func TestIsLifecyclePendingRecognizesOnlyAClosePendingWithItsLease(t *testing.T) {
	require.True(t, IsLifecyclePending(closePending{leaseUUID: closeDeprovisionLeaseUUID}))
	require.False(t, IsLifecyclePending(closePending{}), "a zero value proves nothing")
	require.False(t, IsLifecyclePending(errors.New("close of lease is still pending")))
	require.False(t, IsLifecyclePending(context.DeadlineExceeded))
}

// A close attempt allows every dependency level its full stop grace, plus the
// rest of the close, and stays finite however large the configured grace.
func TestCloseAttemptTimeoutCoversEveryLevelsStopGrace(t *testing.T) {
	for _, tc := range []struct {
		stop time.Duration
		want time.Duration
	}{
		{0, 11*30*time.Second + closeWorkAllowance},
		{time.Minute, 11*time.Minute + closeWorkAllowance},
		{time.Duration(math.MaxInt64), time.Duration(math.MaxInt64)},
	} {
		b := &Backend{cfg: Config{ContainerStopTimeout: tc.stop}}
		require.Equal(t, tc.want, b.closeAttemptTimeout(), "container_stop_timeout %s", tc.stop)
	}
	require.Equal(t, 11, manifest.MaxDependencyLevels)
}
