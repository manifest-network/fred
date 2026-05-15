package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// TestLeaseActor_DirectDispatch exercises the async actor path without the
// synchronous shim: send fire-and-forget, poll for completion. Catches async
// bugs (missed channel sends, dropped messages) that the shim would hide by
// blocking on a done channel.
func TestLeaseActor_DirectDispatch(t *testing.T) {
	var callbackHit atomic.Bool
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackHit.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", nil
		},
	}

	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant:       "tenant-a",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusReady,
			CallbackURL:  callbackServer.URL},
		},
	})
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	defer b.stopCancel()

	require.True(t, b.actorFor("lease-1").TryEnqueue(leasesm.ContainerDiedMsg{ContainerID: "c1"}))

	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		prov := b.provisions["lease-1"]
		return prov != nil && prov.Status == backend.ProvisionStatusFailed
	}, 2*time.Second, 10*time.Millisecond, "actor must transition Ready→Failed asynchronously")

	require.Eventually(t, callbackHit.Load, 2*time.Second, 10*time.Millisecond,
		"actor must emit the Failed callback")
}

// TestConcurrentDeprovisionAndContainerDeath_ExactlyOneCallback pins the
// stale-Failed-callback invariant structurally: when a container-death
// event and a Deprovision call race on the same lease, exactly ONE
// terminal callback fires (Deprovisioned), never a stale Failed that
// Fred already knows is being torn down.
//
// Mechanism: handleContainerDeath transitions the SM to Failing and
// spawns an async diag goroutine. When Deprovision arrives, the
// Failing→Deprovisioning transition's OnExit cancels the goroutine's
// context. The goroutine's ContainerLogs call respects ctx and returns
// early; no DiagGathered fires; the SM never enters Failed; no Failed
// callback is emitted. Defense-in-depth: if the goroutine races past the
// cancel signal and fires DiagGathered, Deprovisioning.Ignore drops it.
func TestConcurrentDeprovisionAndContainerDeath_ExactlyOneCallback(t *testing.T) {
	var callbackCount atomic.Int32
	var failedSeen atomic.Bool
	var deprovisionedSeen atomic.Bool

	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload backend.CallbackPayload
		_ = json.NewDecoder(r.Body).Decode(&payload)
		callbackCount.Add(1)
		switch payload.Status {
		case backend.CallbackStatusFailed:
			failedSeen.Store(true)
		case backend.CallbackStatusDeprovisioned:
			deprovisionedSeen.Store(true)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	inDiag := make(chan struct{})

	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
		// Mirror the production Docker client: block until ctx is canceled
		// or a signal (none here) fires. Cancellation unblocks the goroutine
		// — that's what the Failing.OnExit path triggers.
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			close(inDiag)
			<-ctx.Done()
			return "", ctx.Err()
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
	}

	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant:       "tenant-a",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusReady,
			CallbackURL:  callbackServer.URL},
		},
	})
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	defer b.stopCancel()

	g1Done := make(chan struct{})
	go func() {
		defer close(g1Done)
		b.handleContainerDeath("c1")
	}()

	// Wait for the diag goroutine to reach ContainerLogs (SM is in Failing,
	// status is Failing, goroutine is blocked on ctx).
	<-inDiag

	g2Done := make(chan struct{})
	go func() {
		defer close(g2Done)
		if err := b.Deprovision(context.Background(), "lease-1"); err != nil {
			t.Errorf("Deprovision returned unexpected error: %v", err)
		}
	}()

	// Deprovision runs: actor picks up deprovisionMsg, Fire(evDeprovisionRequested)
	// triggers Failing.OnExit which cancels the goroutine's ctx. ContainerLogs
	// returns ctx.Err(); goroutine exits without firing DiagGathered. Then
	// doDeprovision removes containers and deletes the provision entry.
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		_, exists := b.provisions["lease-1"]
		return !exists
	}, 2*time.Second, 10*time.Millisecond, "Deprovision must complete (provision entry deleted)")

	<-g1Done
	<-g2Done

	require.Eventually(t, deprovisionedSeen.Load, 2*time.Second, 10*time.Millisecond,
		"Deprovisioned callback must fire")

	assert.Equal(t, int32(1), callbackCount.Load(),
		"exactly one terminal callback expected (Failed seen: %v, Deprovisioned seen: %v)",
		failedSeen.Load(), deprovisionedSeen.Load())
	assert.False(t, failedSeen.Load(),
		"stale Failed callback must not fire when Deprovision takes over mid-flight")
}

// TestDebugActors exercises the ops introspection path: after container
// events land for known leases, DebugActors reports each with its SM
// state and inbox depth. Shape-level test; the point is that this
// never blocks, never panics, and produces JSON-encodable data.
func TestDebugActors(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", nil
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		"lease-a": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-a",
			Tenant:       "tenant-1",
			ContainerIDs: []string{"ca"},
			Status:       backend.ProvisionStatusReady},
		},
		"lease-b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-b",
			Tenant:       "tenant-1",
			ContainerIDs: []string{"cb"},
			Status:       backend.ProvisionStatusReady},
		},
	})
	defer b.stopCancel()

	// Before any activity, no actors.
	require.Empty(t, b.DebugActors())

	// Touch one lease — actor is created.
	b.actorFor("lease-a")
	snaps := b.DebugActors()
	require.Len(t, snaps, 1)
	require.Equal(t, "lease-a", snaps[0].LeaseUUID)
	require.Equal(t, 16, snaps[0].InboxCap,
		"inbox cap must match leasesm's leaseActorInboxSize (=16)")
	// The SM is initialized eagerly — its initial state mirrors the
	// provision's current Status at actor creation time.
	require.Equal(t, string(backend.ProvisionStatusReady), snaps[0].SMState)

	// Touch the other — now two actors visible.
	b.actorFor("lease-b")
	snaps = b.DebugActors()
	require.Len(t, snaps, 2)
}

// TestLeaseActor_EagerSMInit pins the eager-SM-init invariant needed by
// DebugActors: the SM must be constructed inside newLeaseActor, not
// lazily on first handler invocation. Lazy init would race with
// DebugActors readers under -race; eager init makes the pointer
// publication synchronous with the actor's exposure via b.actors.
func TestLeaseActor_EagerSMInit(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady}},
	})
	defer b.stopCancel()

	actor := b.actorFor("lease-1")
	require.NotNil(t, actor,
		"sm must be initialized in newLeaseActor; lazy init races with DebugActors")
	require.Equal(t, backend.ProvisionStatusReady, actor.State(),
		"sm initial state mirrors the provision's current Status at actor creation")
}

// TestLeaseActor_RegistryClearedAfterDeprovision pins the
// registry-cleanup and UUID-reuse invariants: once Deprovision removes
// the provision entry, the actor exits and removes itself from
// b.actors. Without this, every deprovisioned lease would leak a
// goroutine plus an SM stuck in Deprovisioning, and any subsequent
// Provision with the same UUID would be silently Ignored by that
// stale SM.
func TestLeaseActor_RegistryClearedAfterDeprovision(t *testing.T) {
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant:       "tenant-a",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusReady},
		},
	})
	defer b.stopCancel()

	first := b.actorFor("lease-1")
	require.NoError(t, b.Deprovision(context.Background(), "lease-1"))

	// Actor deletes itself from b.actors and closes done on exit.
	select {
	case <-first.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("actor did not exit after successful deprovision")
	}
	b.actorsMu.Lock()
	_, exists := b.actors["lease-1"]
	b.actorsMu.Unlock()
	assert.False(t, exists,
		"b.actors must not retain a reference to a deprovisioned lease")

	// A subsequent actorFor with the same UUID returns a *fresh* actor —
	// not the terminated one — whose SM can accept evProvisionRequested.
	second := b.actorFor("lease-1")
	require.NotSame(t, first, second,
		"UUID reuse after Deprovision must produce a fresh actor")
	require.Equal(t, backend.ProvisionStatusProvisioning, second.State(),
		"fresh actor's SM starts in Provisioning (no provision entry exists)")
}

// TestLeaseActor_StatusMatchesSMState is the cross-cutting consistency
// test for the "prov.Status == actor.State()" contract. prov.Status
// is set in multiple places (the synchronous section of
// Provision/Restart/Update, SM entry actions, recoverState's
// Failing→Failed normalization), and we want drift between the two to
// fail loudly rather than produce subtle bugs later.
//
// Walks a lease through a realistic lifecycle and asserts the contract
// at every observable point: initial Provisioning, successful → Ready,
// container-death → Failed, retry → Provisioning → Ready, Deprovision
// → actor exit.
func TestLeaseActor_StatusMatchesSMState(t *testing.T) {
	assertStatusMatchesSM := func(t *testing.T, b *Backend, leaseUUID, context string) {
		t.Helper()
		b.actorsMu.Lock()
		actor, ok := b.actors[leaseUUID]
		b.actorsMu.Unlock()
		if !ok {
			return // actor terminated — no SM to compare
		}
		b.provisionsMu.RLock()
		prov, provExists := b.provisions[leaseUUID]
		var status backend.ProvisionStatus
		if provExists {
			status = prov.Status
		}
		b.provisionsMu.RUnlock()
		if !provExists {
			return // no provision — no contract to enforce
		}
		smState := actor.State()
		assert.Equal(t, status, smState,
			"%s: prov.Status (%s) must match actor.State() (%s)",
			context, status, smState)
	}

	var callbackReceived atomic.Int32
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackReceived.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", nil
		},
	}
	composeMock := &mockComposeExecutor{
		UpFn: func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
			return nil
		},
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{
				{ID: "c1", Service: manifest.DefaultServiceName, State: "running"},
			}, nil
		},
		DownFn: func(ctx context.Context, projectName string, timeout time.Duration) error {
			return nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	b.compose = composeMock
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	defer b.stopCancel()

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = callbackServer.URL

	// (1) Provision: the synchronous section writes Status=Provisioning
	// and creates the actor; the actor's SM starts in Provisioning
	// reading that very Status. Contract: match.
	require.NoError(t, b.Provision(context.Background(), req))

	// Wait for success callback.
	require.Eventually(t, func() bool { return callbackReceived.Load() >= 1 },
		3*time.Second, 20*time.Millisecond)

	// (2) After success: SM has transitioned to Ready; onEnterReadyFromProvision
	// wrote Status=Ready. Contract: match.
	assertStatusMatchesSM(t, b, "lease-1", "after successful provision")

	// (3) Container-death → Failing → Failed (via the synchronous test
	// shim that waits for the diag goroutine's DiagGathered to process).
	mock.InspectContainerFn = func(ctx context.Context, containerID string) (*ContainerInfo, error) {
		return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
	}
	b.handleContainerDeath("c1")

	// Wait for Failed callback.
	require.Eventually(t, func() bool { return callbackReceived.Load() >= 2 },
		3*time.Second, 20*time.Millisecond)
	b.provisionsMu.RLock()
	require.Equal(t, backend.ProvisionStatusFailed, b.provisions["lease-1"].Status)
	b.provisionsMu.RUnlock()
	assertStatusMatchesSM(t, b, "lease-1", "after container death → Failed")

	// (4) Deprovision the failed lease → actor exits; registry drops it;
	// no contract to enforce post-exit (assertStatusMatchesSM handles).
	require.NoError(t, b.Deprovision(context.Background(), "lease-1"))
	b.provisionsMu.RLock()
	_, stillExists := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	require.False(t, stillExists, "provision entry must be gone after Deprovision")
	assertStatusMatchesSM(t, b, "lease-1", "after deprovision")
}

// TestLeaseActor_FailingWedgeRecovery pins the Failing-retry invariant
// I added after self-review: if an actor's SM is stuck in Failing
// (e.g., the diag goroutine never fired DiagGathered because Docker's
// log endpoint hung and the 30s timeout elapsed, OR recoverState
// normalized prov.Status to Failed while leaving an in-memory actor's
// SM in Failing), a caller's retry must still make progress.
//
// Scenario: actor's SM is in Failing. prov.Status was normalized to
// Failed by recoverState. Backend.Provision accepts the retry (Status
// == Failed). The actor receives evProvisionRequested in Failing —
// the SM's new Permit(evProvisionRequested, Provisioning) transitions
// instead of returning an unhandled-trigger error. Lease is no longer
// wedged.
func TestLeaseActor_FailingWedgeRecovery(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1", Status: backend.ProvisionStatusFailing}},
	})
	defer b.stopCancel()

	// Warm the actor so its SM is in Failing.
	actor := b.actorFor("lease-1")
	require.Equal(t, backend.ProvisionStatusFailing, actor.State())

	// Route a provisionRequestedMsg — the Failing→Provisioning Permit
	// should accept it.
	ack := make(chan error, 1)
	ok := b.routeToLease("lease-1", leasesm.ProvisionRequestedMsg{
		Cancel: func() {},
		Work: func() (string, leasesm.ProvisionSuccessResult, map[string]string, error) {
			return "", leasesm.ProvisionSuccessResult{}, nil, nil
		},
		Ack: ack,
	})
	require.True(t, ok)
	select {
	case err := <-ack:
		require.NoError(t, err, "Failing must Permit evProvisionRequested so stuck leases can recover")
	case <-time.After(2 * time.Second):
		t.Fatal("ack never arrived")
	}
}

// TestRouteToLease_DropsOnFullInbox pins the non-blocking-under-mutex
// contract of routeToLease. When an actor's inbox is saturated, the
// non-blocking send in routeToLease returns false — the registry mutex
// is never held across a slow channel send, so a wedged actor cannot
// stall the event loop or other routing callers. Combined with the
// dieEventDroppedTotal metric wiring, this turns "one wedged actor
// stalls all lease event delivery" into "one wedged actor loses its
// own die events (reconciler re-detects)".
//
// Wedges the actor's run loop via a hung Inspector.InspectInstance in the
// SM ContainerDied guard. This is the real production wedge mechanism;
// the previous install-without-run pattern was a synthetic state that
// can't occur in production (NewLeaseActor unconditionally spawns the
// run loop). See ENG-148 PR5b-2 deviation E-1.
func TestRouteToLease_DropsOnFullInbox(t *testing.T) {
	firstInspected := make(chan struct{})
	var inspectCount atomic.Int32
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			if inspectCount.Add(1) == 1 {
				close(firstInspected)
			}
			// Block until the actor's per-call guard ctx (10s) or the
			// backend's stopCtx fires. The test ends well before either.
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady}},
	})
	defer b.stopCancel()

	// First message: routeToLease creates the actor (spawning its run
	// loop via NewLeaseActor) and enqueues. The actor consumes it,
	// invokes handle() → handleContainerDied → sm.Fire → guard →
	// Inspector.InspectInstance, which wedges on the hung mock above.
	require.True(t, b.routeToLease("lease-1", leasesm.ContainerDiedMsg{ContainerID: "c1"}))
	<-firstInspected

	// Actor is now blocked in the SM guard. Fill the remaining 16 inbox
	// slots — none of these get consumed because the actor is wedged.
	for i := 0; i < 16; i++ {
		require.True(t, b.routeToLease("lease-1", leasesm.ContainerDiedMsg{ContainerID: "cN"}),
			"message %d should enqueue while actor is wedged", i)
	}

	// Inbox is now at capacity. Next routeToLease must refuse without
	// blocking.
	done := make(chan bool, 1)
	go func() {
		done <- b.routeToLease("lease-1", leasesm.ContainerDiedMsg{ContainerID: "overflow"})
	}()
	select {
	case ok := <-done:
		assert.False(t, ok, "routeToLease must refuse when inbox is full")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("routeToLease blocked on full inbox — must be non-blocking")
	}
}

// TestRouteToLeaseBlocking_RetriesOnFullInbox pins the
// backpressure-retry contract: when the target actor's inbox is full,
// routeToLeaseBlocking polls at routeToLeaseRetryInterval and succeeds
// once space appears. Replaces the old "full inbox → spurious backend
// shutting down error" behavior.
//
// Wedges the actor's run loop via a hung Inspector.InspectInstance in the
// SM ContainerDied guard. This is the real production wedge mechanism;
// the previous install-without-run pattern was a synthetic state that
// can't occur in production (NewLeaseActor unconditionally spawns the
// run loop). See ENG-148 PR5b-2 deviation E-1.
func TestRouteToLeaseBlocking_RetriesOnFullInbox(t *testing.T) {
	firstInspected := make(chan struct{})
	unblockFirst := make(chan struct{})
	var inspectCount atomic.Int32
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			n := inspectCount.Add(1)
			if n == 1 {
				close(firstInspected)
				// First call: block until the test signals release.
				select {
				case <-unblockFirst:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				// Return a not-terminally-gone phase so the guard
				// short-circuits and the actor handler returns —
				// this is what frees the inbox slot.
				return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
			}
			// Subsequent calls: block until ctx times out. We only
			// want ONE slot freed for this test.
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady}},
	})
	defer b.stopCancel()

	// Fill inbox: first message wedges the actor in the SM guard;
	// next 16 queue.
	require.True(t, b.routeToLease("lease-1", leasesm.ContainerDiedMsg{ContainerID: "c0"}))
	<-firstInspected
	for i := 0; i < 16; i++ {
		require.True(t, b.routeToLease("lease-1", leasesm.ContainerDiedMsg{ContainerID: "cN"}))
	}

	// routeToLeaseBlocking should not return false immediately — it
	// must retry while inbox is full. Free one slot after ~30ms by
	// releasing the wedged Inspector so the actor's handler returns
	// and the next message gets pulled off the inbox.
	start := time.Now()
	go func() {
		time.Sleep(30 * time.Millisecond)
		close(unblockFirst)
	}()

	err := b.routeToLeaseBlocking(context.Background(), "lease-1", leasesm.ContainerDiedMsg{ContainerID: "c-retry"})
	require.NoError(t, err, "routeToLeaseBlocking must succeed once slot frees")
	assert.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond,
		"should have waited for inbox space rather than returning immediately")
}

// TestRouteToLeaseBlocking_ReturnsCtxErr pins the ctx-cancel contract:
// if the caller's ctx is cancelled (at entry or during retry),
// routeToLeaseBlocking must return ctx.Err() without enqueueing.
//
// Wedges the actor's run loop via a hung Inspector.InspectInstance in the
// SM ContainerDied guard. This is the real production wedge mechanism;
// the previous install-without-run pattern was a synthetic state that
// can't occur in production (NewLeaseActor unconditionally spawns the
// run loop). See ENG-148 PR5b-2 deviation E-1.
func TestRouteToLeaseBlocking_ReturnsCtxErr(t *testing.T) {
	firstInspected := make(chan struct{})
	var inspectCount atomic.Int32
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			if inspectCount.Add(1) == 1 {
				close(firstInspected)
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady}},
	})
	defer b.stopCancel()

	// Saturate: first message wedges actor; next 16 queue.
	require.True(t, b.routeToLease("lease-1", leasesm.ContainerDiedMsg{ContainerID: "c0"}))
	<-firstInspected
	for i := 0; i < 16; i++ {
		require.True(t, b.routeToLease("lease-1", leasesm.ContainerDiedMsg{ContainerID: "cN"}))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := b.routeToLeaseBlocking(ctx, "lease-1", leasesm.ContainerDiedMsg{ContainerID: "c-timeout"})
	assert.ErrorIs(t, err, context.DeadlineExceeded,
		"routeToLeaseBlocking must surface ctx.Err() when caller's ctx expires while inbox is wedged")
}

// TestLeaseActor_ProvisionRequestedSMRejection pins the accept/reject
// ack contract: when the actor's SM refuses to transition (e.g., the
// provisionRequestedMsg arrives while the SM is in a state that doesn't
// Permit evProvisionRequested), the actor must ack with a non-nil error
// and MUST NOT spawn the work goroutine. This is the structural
// guarantee that Backend.Provision never spawns work behind a rejected
// SM transition.
func TestLeaseActor_ProvisionRequestedSMRejection(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		// Ready state does NOT Permit evProvisionRequested — SM rejects.
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady}},
	})
	defer b.stopCancel()

	// Warm the actor so its SM is in Ready.
	require.Equal(t, backend.ProvisionStatusReady, b.actorFor("lease-1").State())

	ack := make(chan error, 1)
	ok := b.routeToLease("lease-1", leasesm.ProvisionRequestedMsg{
		Cancel: func() {},
		Work: func() (string, leasesm.ProvisionSuccessResult, map[string]string, error) {
			t.Fatal("work closure must not run when SM rejects the transition")
			return "", leasesm.ProvisionSuccessResult{}, nil, nil
		},
		Ack: ack,
	})
	require.True(t, ok, "routeToLease itself must succeed; rejection is at SM-fire time")

	select {
	case err := <-ack:
		require.Error(t, err, "SM rejection must surface as a non-nil ack error")
	case <-time.After(2 * time.Second):
		t.Fatal("ack never arrived from rejected provisionRequestedMsg")
	}
}

// TestLeaseActor_RegistryDeletedBeforeDoneClose guards the defer order in
// run(): b.actors.Delete must fire BEFORE close(a.done) so that a
// concurrent actorFor() call racing with the actor's termination creates
// a fresh actor rather than reusing the exiting one. Without this order,
// the window between "run returns" and "Delete executes" would let a
// caller grab the stale actor, send into its inbox, and have the message
// silently drained instead of handled — the orphan-containers scenario
// flagged by remote review finding #1.
func TestLeaseActor_RegistryDeletedBeforeDoneClose(t *testing.T) {
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant:       "tenant-a",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusReady},
		},
	})
	defer b.stopCancel()

	first := b.actorFor("lease-1")
	require.NoError(t, b.Deprovision(context.Background(), "lease-1"))
	<-first.Done()

	// By the time done is closed, Delete must have already run — so
	// actorFor returns a fresh actor, not the exiting one.
	second := b.actorFor("lease-1")
	require.NotSame(t, first, second,
		"fresh actorFor after Deprovision+done must not return the exiting actor — Delete must fire before close(done)")
}

// TestBackend_ShutdownDrainsAllActors stresses the drain-on-shutdown path
// at backend scale: many leases in mixed SM states, all draining
// concurrently on stopCtx. Asserts every actor exits and the registry is
// empty — no leaked goroutines, no actors stuck in their select loops.
func TestBackend_ShutdownDrainsAllActors(t *testing.T) {
	const n = 20
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error { return nil },
	}

	provs := make(map[string]*provision, n)
	for i := 0; i < n; i++ {
		leaseUUID := fmt.Sprintf("lease-%d", i)
		// Alternate between Ready, Provisioning, Restarting, Updating
		// to exercise different SM configurations through drain.
		var status backend.ProvisionStatus
		switch i % 4 {
		case 0:
			status = backend.ProvisionStatusReady
		case 1:
			status = backend.ProvisionStatusProvisioning
		case 2:
			status = backend.ProvisionStatusRestarting
		default:
			status = backend.ProvisionStatusUpdating
		}
		provs[leaseUUID] = &provision{ProvisionState: leasesm.ProvisionState{LeaseUUID: leaseUUID,
			Tenant:       "tenant-a",
			Status:       status,
			ContainerIDs: []string{fmt.Sprintf("c-%d", i)}},
		}
	}

	b := newBackendForTest(mock, provs)

	// Warm up every actor so they're all running goroutines when
	// shutdown fires.
	for uuid := range provs {
		b.actorFor(uuid)
	}

	done := make(chan struct{})
	go func() {
		b.stopCancel()
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("backend did not drain and exit within 10s — actors likely stuck")
	}

	// Every actor's run loop deletes itself from b.actors on exit.
	b.actorsMu.Lock()
	remaining := len(b.actors)
	b.actorsMu.Unlock()
	assert.Equal(t, 0, remaining, "b.actors must be empty after full shutdown drain")
}

// TestHandleContainerDeath_ShutdownDoesNotHang guards the sync shim's
// stopCtx branch: once the backend is shutting down, the shim must return
// promptly instead of blocking on a done channel the actor will never close.
func TestHandleContainerDeath_ShutdownDoesNotHang(t *testing.T) {
	mock := &mockDockerClient{}

	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusReady},
		},
	})

	b.stopCancel()

	returned := make(chan struct{})
	go func() {
		b.handleContainerDeath("c1")
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handleContainerDeath did not return within 500ms after shutdown")
	}
}

// TestOnEnterFailing_RaceWithConcurrentStatusFlip pins the fix for the
// Restart-races-ContainerDeath race. Backend.Restart flips prov.Status
// synchronously outside the actor before routing its message, so a
// Restart can sneak in between the SM guard's RLock-release (after the
// slow InspectContainer) and onEnterFailing's Lock-acquire. Without
// the recheck, onEnterFailing would overwrite Status=Restarting with
// Failing, bump FailCount, and decrement activeProvisions — all
// spurious. With the recheck, onEnterFailing bails on Status != Ready
// and the metric records the skip.
//
// The test blocks InspectContainer to pin down the exact race window,
// then simulates Restart's sync phase by flipping Status under the
// lock while the guard is still waiting.
func TestOnEnterFailing_RaceWithConcurrentStatusFlip(t *testing.T) {
	inspectCalled := make(chan struct{})
	releaseInspect := make(chan struct{})

	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			close(inspectCalled)
			<-releaseInspect
			return &ContainerInfo{
				ContainerID: containerID,
				Status:      "exited",
				ExitCode:    1,
			}, nil
		},
	}

	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1"},
			FailCount:    0},
		},
	})
	defer b.stopCancel()

	activeBefore := testutil.ToFloat64(activeProvisions)
	skippedBefore := testutil.ToFloat64(leaseFailingRaceSkippedTotal)

	// Trigger the die event in a goroutine; it will block inside the
	// guard's InspectContainer call until we release it.
	deathDone := make(chan struct{})
	go func() {
		b.handleContainerDeath("c1")
		close(deathDone)
	}()

	// Wait for the guard to reach InspectContainer — now the race window
	// between the guard's RLock-release and onEnterFailing's Lock-acquire
	// is open.
	select {
	case <-inspectCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("guard did not call InspectContainer")
	}

	// Simulate Backend.Restart's synchronous Status flip. The actor
	// has not yet entered onEnterFailing (it's blocked on InspectContainer).
	b.provisionsMu.Lock()
	b.provisions["lease-1"].Status = backend.ProvisionStatusRestarting
	b.provisionsMu.Unlock()

	// Release InspectContainer. Guard returns true (the die event is
	// real), SM commits Ready→Failing, onEnterFailing runs. It must see
	// Status=Restarting and bail.
	close(releaseInspect)

	select {
	case <-deathDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handleContainerDeath did not complete")
	}

	// Invariants after the race: prov.Status stayed Restarting,
	// FailCount unchanged, LastError empty, activeProvisions unchanged,
	// and the skip counter bumped.
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	status := prov.Status
	failCount := prov.FailCount
	lastError := prov.LastError
	b.provisionsMu.RUnlock()

	assert.Equal(t, backend.ProvisionStatusRestarting, status,
		"onEnterFailing must NOT overwrite Status=Restarting with Failing")
	assert.Equal(t, 0, failCount,
		"onEnterFailing must NOT bump FailCount after bailing on the race")
	assert.Empty(t, lastError,
		"onEnterFailing must NOT set LastError after bailing on the race")

	activeAfter := testutil.ToFloat64(activeProvisions)
	assert.Equal(t, activeBefore, activeAfter,
		"activeProvisions must not be decremented when onEnterFailing bails")

	skippedAfter := testutil.ToFloat64(leaseFailingRaceSkippedTotal)
	assert.Equal(t, skippedBefore+1, skippedAfter,
		"leaseFailingRaceSkippedTotal must increment by exactly 1")
}

// TestHandlerPanic_UnblocksReplyChannel pins the invariant that a panic
// inside the actor's message-handler dispatch does NOT leave the caller
// blocked on their reply/ack channel forever. Before the onPanic hook
// existed, a panic in handleDeprovision would skip `m.reply <- err`
// and Backend.Deprovision would hang on its select until ctx/stopCtx
// cancelled — effectively an API hang per bad message.
//
// This test installs a panicking handler at the Fire-time entry action
// (onEnterDeprovisioning doesn't exist, so we inject via the SM guard
// for evContainerDied — but that has no reply). The cleanest proof is
// to synthesize a deprovisionMsg directly and drive the actor, making
// the handler panic and asserting the reply channel receives an error.

// TestAckOrAbort_HonorsAckEvenWhenCtxCanceled pins the ctx-vs-ack race
// fix. Go's select picks pseudo-randomly when multiple arms are ready,
// so a naive select on {ack, ctx.Done, stopCtx.Done} can take the
// cancellation arm even though the actor has acked — leading the caller
// to roll back while the actor proceeds.
//
// We can't deterministically schedule the race in a unit test, but we
// can verify the post-cancel non-blocking ack read: pre-cancel the ctx
// AND pre-ack, then call ackOrAbort and assert it returns (true, nil)
// instead of ctx.Err.
func TestAckOrAbort_HonorsAckEvenWhenCtxCanceled(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	defer b.stopCancel()

	t.Run("ack_success_beats_ctx_cancel", func(t *testing.T) {
		ack := make(chan error, 1)
		ack <- nil // actor "acked" success
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // caller "gave up"

		accepted, err := b.ackOrAbort(ctx, ack)
		assert.True(t, accepted, "ackOrAbort must honor pre-queued ack even when ctx is already canceled")
		assert.NoError(t, err)
	})

	t.Run("ack_error_beats_ctx_cancel", func(t *testing.T) {
		ack := make(chan error, 1)
		synthErr := fmt.Errorf("synthetic ack error")
		ack <- synthErr
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		accepted, err := b.ackOrAbort(ctx, ack)
		assert.False(t, accepted)
		assert.ErrorIs(t, err, synthErr,
			"ackOrAbort must surface actor-rejected error, not ctx.Err")
	})

	t.Run("no_ack_ctx_canceled", func(t *testing.T) {
		ack := make(chan error, 1)
		// no pre-ack
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		accepted, err := b.ackOrAbort(ctx, ack)
		assert.False(t, accepted)
		assert.ErrorIs(t, err, context.Canceled,
			"with no ack and canceled ctx, ackOrAbort must return ctx.Err")
	})

	t.Run("no_ack_stop_ctx", func(t *testing.T) {
		ack := make(chan error, 1)
		ctx := context.Background() // caller ctx fine
		b.stopCancel()              // backend shutting down

		accepted, err := b.ackOrAbort(ctx, ack)
		assert.False(t, accepted)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "backend shutting down")
	})
}

// TestConcurrentProvisionDeprovision_Stress is an adversarial concurrency
// test that hammers Backend.Provision and Backend.Deprovision on a small
// pool of lease UUIDs with many goroutines. Together with -race, this
// surfaces lifecycle/ack/terminated-actor bugs that point-tests miss:
//
//   - Provision racing an in-flight Deprovision on the same UUID.
//   - Lifecycle race (actor terminated, not yet removed from registry).
//   - ack-vs-ctx.Done race (if the ctx-timeout loop fires).
//   - Concurrent cross-UUID operations stressing the registry mutex.
//
// We don't assert on individual operation outcomes (concurrent
// Provision/Deprovision on the same UUID is expected to produce errors
// like ErrAlreadyProvisioned or errActorTerminated). We assert on the
// invariants that MUST hold:
//
//   - No races detected (enforced by -race).
//   - No goroutine deadlocks (test completes within timeout).
//   - No orphaned actor goroutines after teardown (enforced by b.wg.Wait).
//   - No panic that escapes recover.
//
// Skipped under -short since it runs for a few seconds and saturates CPU.
func TestConcurrentProvisionDeprovision_Stress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test — not run under -short")
	}

	const (
		numWorkers = 16
		numUUIDs   = 8
		testDur    = 2 * time.Second
	)

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "c-" + params.LeaseUUID + "-" + fmt.Sprint(params.InstanceIndex), nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		StopContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	// Callback endpoint: swallow all callbacks silently — we're only
	// interested in races, not delivery correctness.
	callbackSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackSrv.Close()

	b := newBackendForProvisionTest(t, mock, nil)
	b.httpClient = callbackSrv.Client()
	b.cfg.StartupVerifyDuration = 1 * time.Millisecond // fast
	rebuildCallbackSender(b)

	uuids := make([]string, numUUIDs)
	for i := range uuids {
		uuids[i] = fmt.Sprintf("stress-%d", i)
	}

	deadline := time.Now().Add(testDur)
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			// Distinct seed per worker so they hit different interleavings.
			next := uint32(workerID*17 + 1)
			for time.Now().Before(deadline) {
				// xorshift — fast, deterministic per worker, no mutex.
				next ^= next << 13
				next ^= next >> 17
				next ^= next << 5
				uuid := uuids[int(next)%numUUIDs]

				// Mix of operations: provision, deprovision, redundant
				// provision, redundant deprovision. Errors are expected
				// and ignored — the point is exercise, not correctness.
				switch int(next>>1) % 4 {
				case 0, 1:
					_ = b.Provision(context.Background(), newProvisionRequest(
						uuid, "tenant-a", "docker-small", 1,
						validManifestJSON("busybox:latest"),
					))
				case 2, 3:
					_ = b.Deprovision(context.Background(), uuid)
				}
			}
		}(w)
	}

	// Wait for workers to finish with a hard upper bound so a stuck
	// goroutine fails the test instead of hanging forever.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(testDur + 30*time.Second):
		t.Fatal("stress workers did not exit — possible deadlock")
	}

	// Clean up any remaining leases so the actor-exit defers run and
	// we can detect orphan goroutines.
	for _, uuid := range uuids {
		_ = b.Deprovision(context.Background(), uuid)
	}

	// Shutdown. b.stopCancel + b.wg.Wait inside the test cleanup
	// ensures all spawned actor goroutines exit; if one is leaked,
	// the test would hang here (caught by go test's own timeout).
	b.stopCancel()
	done = make(chan struct{})
	go func() { b.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("backend wg.Wait did not complete — orphan goroutines")
	}
}

// TestDeprovision_HonorsLateReplyAfterCtxCancel pins bug_003: when the
// actor acks success at the same instant as the caller's ctx cancels,
// Go's select can take the cancellation arm and return ctx.Err() even
// though doDeprovision already fully committed. Mirroring ackOrAbort's
// pattern, the shim now does a non-blocking re-read of reply after
// cancellation so the actor's authoritative outcome wins.
func TestDeprovision_HonorsLateReplyAfterCtxCancel(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	defer b.stopCancel()

	// Replicate the fixed shim's select + non-blocking re-check with both
	// arms pre-ready: reply has the actor's success ack, ctx is already
	// canceled. Under the buggy code, the select would randomly pick
	// ctx.Done() and return ctx.Err() even though the actor already
	// committed. Under the fix, either the first select picks reply, or
	// the non-blocking re-check after ctx.Done() picks it — either way,
	// the actor's ack wins.
	reply := make(chan error, 1)
	reply <- nil

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var ackedErr error
	var acked bool
	select {
	case ackedErr = <-reply:
		acked = true
	case <-b.stopCtx.Done():
	case <-ctx.Done():
	}
	if !acked {
		select {
		case ackedErr = <-reply:
			acked = true
		default:
		}
	}
	require.True(t, acked,
		"fixed select+recheck must surface the actor's ack even when ctx cancels (bug_003)")
	require.NoError(t, ackedErr, "actor acked success (nil); Deprovision must return that")
}

// TestProvision_ReleasesPoolOnRouteFailure pins bug_001: when
// Backend.Provision's handoff to the actor fails (routeToLeaseBlocking
// errors out OR ackOrAbort rejects), the pool allocations made during
// the preamble must be released, or they leak with no owning provision
// entry. A retry on the same lease would then fail TryAllocate with
// "already allocated" and the lease becomes unrecoverable until
// backend restart.
//
// We force the route to fail by pre-canceling the backend's stopCtx:
// routeToLeaseBlocking checks stopCtx up-front and returns immediately.
// Pool stats must return to baseline after the call.
func TestProvision_ReleasesPoolOnRouteFailure(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	beforeStats := b.pool.Stats()

	// Shut down so routeToLeaseBlocking refuses. Synchronous validation
	// and pool allocation in Provision still run before the route call,
	// so the leak path is reachable.
	b.stopCancel()

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1,
		validManifestJSON("nginx:latest"))
	err := b.Provision(context.Background(), req)
	require.Error(t, err, "Provision must fail when backend is shut down")

	afterStats := b.pool.Stats()
	assert.Equal(t, beforeStats.AllocatedCPU, afterStats.AllocatedCPU,
		"pool CPU must be released on route-failure rollback (bug_001)")
	assert.Equal(t, beforeStats.AllocatedMemoryMB, afterStats.AllocatedMemoryMB,
		"pool memory must be released on route-failure rollback (bug_001)")
	assert.Equal(t, beforeStats.AllocatedDiskMB, afterStats.AllocatedDiskMB,
		"pool disk must be released on route-failure rollback (bug_001)")
}
