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

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
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
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusReady,
			CallbackURL:  callbackServer.URL,
		},
	})
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	defer b.stopCancel()

	require.True(t, b.actorFor("lease-1").send(containerDiedMsg{containerID: "c1"}))

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
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusReady,
			CallbackURL:  callbackServer.URL,
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
		"lease-a": {
			LeaseUUID:    "lease-a",
			Tenant:       "tenant-1",
			ContainerIDs: []string{"ca"},
			Status:       backend.ProvisionStatusReady,
		},
		"lease-b": {
			LeaseUUID:    "lease-b",
			Tenant:       "tenant-1",
			ContainerIDs: []string{"cb"},
			Status:       backend.ProvisionStatusReady,
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
	require.Equal(t, leaseActorInboxSize, snaps[0].InboxCap)
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
		"lease-1": {LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
	})
	defer b.stopCancel()

	actor := b.actorFor("lease-1")
	require.NotNil(t, actor.sm,
		"sm must be initialized in newLeaseActor; lazy init races with DebugActors")
	require.Equal(t, backend.ProvisionStatusReady, actor.sm.State(),
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
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusReady,
		},
	})
	defer b.stopCancel()

	first := b.actorFor("lease-1")
	require.NoError(t, b.Deprovision(context.Background(), "lease-1"))

	// Actor deletes itself from b.actors and closes done on exit.
	select {
	case <-first.done:
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
	require.Equal(t, backend.ProvisionStatusProvisioning, second.sm.State(),
		"fresh actor's SM starts in Provisioning (no provision entry exists)")
}

// TestLeaseActor_StatusMatchesSMState is the cross-cutting consistency
// test for the "prov.Status == actor.sm.State()" contract. prov.Status
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
		smState := actor.sm.State()
		assert.Equal(t, status, smState,
			"%s: prov.Status (%s) must match actor.sm.State() (%s)",
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
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "c1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
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
	b := newBackendForProvisionTest(t, mock, nil)
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
		"lease-1": {LeaseUUID: "lease-1", Status: backend.ProvisionStatusFailing},
	})
	defer b.stopCancel()

	// Warm the actor so its SM is in Failing.
	actor := b.actorFor("lease-1")
	require.Equal(t, backend.ProvisionStatusFailing, actor.sm.State())

	// Route a provisionRequestedMsg — the Failing→Provisioning Permit
	// should accept it.
	ack := make(chan error, 1)
	ok := b.routeToLease("lease-1", provisionRequestedMsg{
		cancel: func() {},
		work: func() (string, provisionSuccessResult, map[string]string, error) {
			return "", provisionSuccessResult{}, nil, nil
		},
		ack: ack,
	})
	require.True(t, ok)
	select {
	case err := <-ack:
		require.NoError(t, err, "Failing must Permit evProvisionRequested so stuck leases can recover")
	case <-time.After(2 * time.Second):
		t.Fatal("ack never arrived")
	}
}

// TestLeaseActor_SurvivesHandlerPanic pins the defer-recover in handle():
// a panic in a handler path (SM guard, entry action, or downstream I/O)
// must be contained to a single message. Without recovery, the actor
// goroutine dies and senders block on a full inbox forever.
func TestLeaseActor_SurvivesHandlerPanic(t *testing.T) {
	var inspectCalls atomic.Int32
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			if inspectCalls.Add(1) == 1 {
				panic("simulated inspect failure")
			}
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", nil
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusReady,
		},
	})
	defer b.stopCancel()

	actor := b.actorFor("lease-1")
	before := testutil.ToFloat64(leaseActorPanicsTotal)

	// First send: panics inside the SM guard (InspectContainer).
	require.True(t, actor.send(containerDiedMsg{containerID: "c1"}))
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(leaseActorPanicsTotal) > before
	}, 2*time.Second, 10*time.Millisecond,
		"leaseActorPanicsTotal must increment when a handler panics")

	// Second send: must be processed — the actor survived the panic.
	done := make(chan struct{})
	require.True(t, actor.send(containerDiedMsg{containerID: "c1", done: done}))
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("actor did not process a message after recovering from panic")
	}
}

// TestLeaseActor_DrainsTerminalEventsOnShutdown pins the structural
// invariant for terminal-event delivery during shutdown: when stopCtx
// fires with an in-flight work goroutine, the actor must (1) wait for
// the goroutine to complete (via workers.Zero), (2) drain the inbox so the
// terminal SM event is processed via handle(), and only then exit.
// Without the drain, the event would be dropped at the send-check
// because the actor had already exited on stopCtx.
func TestLeaseActor_DrainsTerminalEventsOnShutdown(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusProvisioning,
			ContainerIDs: nil,
		},
	})

	actor := b.actorFor("lease-1")
	require.Equal(t, backend.ProvisionStatusProvisioning, actor.sm.State())

	// Simulate an in-flight worker via workers.Add. The actor's exit-path
	// waitForWorkers defer will block until we Done().
	actor.workers.Add()

	// Fire shutdown BEFORE the worker completes. The actor's run loop
	// returns immediately but waitForWorkers must block.
	b.stopCancel()

	// Simulate the worker: send terminal event, then Done(). The actor
	// should then drain the inbox (via handle()) and process the event,
	// flipping Status to Ready.
	go func() {
		ok := actor.sendTerminal(provisionCompletedMsg{
			result: provisionSuccessResult{
				containerIDs: []string{"c1"},
			},
		})
		require.True(t, ok, "sendTerminal must not refuse during shutdown drain")
		actor.workers.Done()
	}()

	// The actor must exit cleanly, and the provision must be Ready.
	select {
	case <-actor.done:
	case <-time.After(5 * time.Second):
		t.Fatal("actor did not exit after shutdown drain")
	}

	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()
	prov := b.provisions["lease-1"]
	require.NotNil(t, prov)
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status,
		"terminal provisionCompletedMsg sent during shutdown must have been drained and processed")
	assert.Equal(t, []string{"c1"}, prov.ContainerIDs,
		"ContainerIDs must reflect the SM entry action running on the drained event")
}

// TestLeaseActor_SendTerminalRefusesAfterActorExit guards the sendTerminal
// contract: once the actor has fully exited (a.done closed), sendTerminal
// must return false rather than block forever on a channel nobody will
// drain. This lets call sites count + log the dropped event instead of
// wedging the goroutine.
func TestLeaseActor_SendTerminalRefusesAfterActorExit(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)

	actor := b.actorFor("lease-gone")
	// Force the actor to exit by shutting down and waiting for it.
	b.stopCancel()
	<-actor.done

	ok := actor.sendTerminal(provisionCompletedMsg{})
	assert.False(t, ok, "sendTerminal must refuse once the actor has exited")
}

// TestLeaseActor_SendRefusesAfterActorExit mirrors the sendTerminal test
// for the non-terminal send path. Before the defer-reorder fix, post-exit
// send() could queue a message into an inbox nobody would drain because
// the hasExited check didn't exist on this path.
func TestLeaseActor_SendRefusesAfterActorExit(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)

	actor := b.actorFor("lease-gone")
	b.stopCancel()
	<-actor.done

	ok := actor.send(containerDiedMsg{containerID: "c1"})
	assert.False(t, ok, "send must refuse once the actor has exited")
}

// TestRouteToLease_DropsOnFullInbox pins the non-blocking-under-mutex
// contract of routeToLease. When an actor's inbox is saturated, the
// non-blocking send in routeToLease returns false — the registry mutex
// is never held across a slow channel send, so a wedged actor cannot
// stall the event loop or other routing callers. Combined with the
// dieEventDroppedTotal metric wiring, this turns "one wedged actor
// stalls all lease event delivery" into "one wedged actor loses its
// own die events (reconciler re-detects)".
func TestRouteToLease_DropsOnFullInbox(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	defer b.stopCancel()

	// Install an actor directly without starting its run loop, so its
	// inbox is never drained and we can fill it to capacity.
	actor := newLeaseActor(b, "lease-1")
	b.actorsMu.Lock()
	b.actors["lease-1"] = actor
	b.actorsMu.Unlock()

	// Fill inbox to capacity.
	for i := 0; i < leaseActorInboxSize; i++ {
		ok := b.routeToLease("lease-1", containerDiedMsg{containerID: "c1"})
		require.True(t, ok, "message %d should enqueue", i)
	}

	// Inbox is full → routeToLease must refuse without blocking.
	done := make(chan bool, 1)
	go func() {
		done <- b.routeToLease("lease-1", containerDiedMsg{containerID: "overflow"})
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
func TestRouteToLeaseBlocking_RetriesOnFullInbox(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	defer b.stopCancel()

	// Install an actor directly with an unstarted run loop so its
	// inbox doesn't drain. Fill to capacity.
	actor := newLeaseActor(b, "lease-1")
	b.actorsMu.Lock()
	b.actors["lease-1"] = actor
	b.actorsMu.Unlock()
	for i := 0; i < leaseActorInboxSize; i++ {
		require.True(t, b.routeToLease("lease-1", containerDiedMsg{containerID: "cN"}))
	}

	// routeToLeaseBlocking should not return false immediately — it
	// must retry while inbox is full. Drain one slot after ~30ms so
	// retry finds space and the call returns nil.
	start := time.Now()
	drained := make(chan struct{})
	go func() {
		time.Sleep(30 * time.Millisecond)
		<-actor.inbox // free one slot
		close(drained)
	}()

	err := b.routeToLeaseBlocking(context.Background(), "lease-1", containerDiedMsg{containerID: "c-retry"})
	<-drained
	require.NoError(t, err, "routeToLeaseBlocking must succeed once slot frees")
	assert.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond,
		"should have waited for inbox space rather than returning immediately")
}

// TestRouteToLeaseBlocking_ReturnsCtxErr pins the ctx-cancel contract:
// if the caller's ctx is cancelled (at entry or during retry),
// routeToLeaseBlocking must return ctx.Err() without enqueueing.
func TestRouteToLeaseBlocking_ReturnsCtxErr(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	defer b.stopCancel()

	// Install a saturated actor.
	actor := newLeaseActor(b, "lease-1")
	b.actorsMu.Lock()
	b.actors["lease-1"] = actor
	b.actorsMu.Unlock()
	for i := 0; i < leaseActorInboxSize; i++ {
		require.True(t, b.routeToLease("lease-1", containerDiedMsg{}))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := b.routeToLeaseBlocking(ctx, "lease-1", containerDiedMsg{containerID: "c-timeout"})
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
		"lease-1": {LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
	})
	defer b.stopCancel()

	// Warm the actor so its SM is in Ready.
	require.Equal(t, backend.ProvisionStatusReady, b.actorFor("lease-1").sm.State())

	ack := make(chan error, 1)
	ok := b.routeToLease("lease-1", provisionRequestedMsg{
		cancel: func() {},
		work: func() (string, provisionSuccessResult, map[string]string, error) {
			t.Fatal("work closure must not run when SM rejects the transition")
			return "", provisionSuccessResult{}, nil, nil
		},
		ack: ack,
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
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusReady,
		},
	})
	defer b.stopCancel()

	first := b.actorFor("lease-1")
	require.NoError(t, b.Deprovision(context.Background(), "lease-1"))
	<-first.done

	// By the time done is closed, Delete must have already run — so
	// actorFor returns a fresh actor, not the exiting one.
	second := b.actorFor("lease-1")
	require.NotSame(t, first, second,
		"fresh actorFor after Deprovision+done must not return the exiting actor — Delete must fire before close(done)")
}

// TestLeaseActor_RestartDeprovisionWaitsForInFlightGoroutine pins the
// orphan-containers invariant for the Restart flow: when Deprovision
// preempts an in-flight restart, Restarting.OnExit must cancel the
// work goroutine and wait on workDone before doDeprovision reads
// ContainerIDs. Guards against future refactors that move the replace
// spawn sites without plumbing the done channel.
//
// The Update flow uses the exact same SM transition and OnExit handler,
// so this single test covers both — the behaviour is identical.
func TestLeaseActor_RestartDeprovisionWaitsForInFlightGoroutine(t *testing.T) {
	var removedMu sync.Mutex
	var removedIDs []string
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			removedMu.Lock()
			removedIDs = append(removedIDs, containerID)
			removedMu.Unlock()
			return nil
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusRestarting,
			ContainerIDs: []string{"old-container"},
		},
	})
	defer b.stopCancel()

	actor := b.actorFor("lease-1")
	require.Equal(t, backend.ProvisionStatusRestarting, actor.sm.State())

	// Simulate an in-flight replace worker via workers.Add + cancel func.
	var cancelCalled atomic.Bool
	workerRelease := make(chan struct{})
	actor.workCancel = func() { cancelCalled.Store(true) }
	actor.workers.Add()
	go func() {
		<-workerRelease
		// Publish new container IDs before Done — mirrors the real
		// replace worker's pre-publish step.
		b.provisionsMu.Lock()
		b.provisions["lease-1"].ContainerIDs = []string{"new-container"}
		b.provisionsMu.Unlock()
		actor.workers.Done()
	}()

	deprovErr := make(chan error, 1)
	go func() {
		deprovErr <- b.Deprovision(context.Background(), "lease-1")
	}()

	require.Eventually(t, cancelCalled.Load, 1*time.Second, 5*time.Millisecond,
		"OnExit must call workCancel before waiting for the worker (Restart path)")

	select {
	case err := <-deprovErr:
		t.Fatalf("Deprovision returned before worker finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Release the worker → wg.Done → onExitProvisioning unblocks →
	// doDeprovision runs.
	close(workerRelease)

	select {
	case err := <-deprovErr:
		require.NoError(t, err, "Deprovision must succeed after worker completes")
	case <-time.After(3 * time.Second):
		t.Fatal("Deprovision did not complete after worker finished")
	}

	removedMu.Lock()
	defer removedMu.Unlock()
	require.Contains(t, removedIDs, "new-container",
		"doDeprovision must see the new containerIDs published by the replace worker")
}

// TestLeaseActor_ExitWaitsForWorkers pins the structural fix for the
// shutdown-drain race: the actor's exit-path waitForWorkers defer must
// block until every worker goroutine (provision/restart/update/diag)
// has returned. Without it, a worker's sendTerminal could land after
// the actor's drainInbox had already run, dropping the terminal event.
//
// Installs a synthetic worker via workers.Add, fires shutdown,
// verifies the actor does not exit until the worker Done()s.
func TestLeaseActor_ExitWaitsForWorkers(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", Status: backend.ProvisionStatusFailing},
	})

	actor := b.actorFor("lease-1")
	actor.workers.Add()

	b.stopCancel()

	// Actor must block in waitForWorkers until we Done().
	select {
	case <-actor.done:
		t.Fatal("actor exited before worker Done() — waitForWorkers did not block")
	case <-time.After(150 * time.Millisecond):
	}

	// Mark worker complete. Actor must now exit cleanly.
	actor.workers.Done()
	select {
	case <-actor.done:
	case <-time.After(2 * time.Second):
		t.Fatal("actor did not exit after worker Done()")
	}
}

// TestLeaseActor_DiagGathered_ShutdownDrain exercises the drain-on-shutdown
// path for a lease in the Failing state: stopCtx fires with a diagGatheredMsg
// freshly queued in the inbox, and the actor must process it (transitioning
// Failing→Failed) before exiting. Pre-fix: the actor would exit on
// stopCtx.Done and the diag message would be discarded by drainInbox
// without handling, leaving Status stuck at Failing.
//
// Asserts on the SM-observable outcome (Status flip) rather than HTTP
// callback delivery — the callbackSender uses stopCtx and legitimately
// cannot reach an external HTTP endpoint during shutdown (the callback
// is persisted to the bbolt store for replay on next start instead).
func TestLeaseActor_DiagGathered_ShutdownDrain(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusFailing,
		},
	})

	actor := b.actorFor("lease-1")
	require.Equal(t, backend.ProvisionStatusFailing, actor.sm.State())

	// Queue a diagGatheredMsg, then fire shutdown. Whatever the inbox
	// select picks next (msg or stopCtx.Done), drainOnShutdown must
	// process the queued message before the actor exits.
	ok := actor.sendTerminal(diagGatheredMsg{
		result: diagResult{
			containerID: "c1",
			info:        &ContainerInfo{ContainerID: "c1", Status: "exited", ExitCode: 1},
			diag:        "synthetic diag",
		},
	})
	require.True(t, ok, "sendTerminal must enqueue while the actor is alive")
	b.stopCancel()

	select {
	case <-actor.done:
	case <-time.After(3 * time.Second):
		t.Fatal("actor did not exit after shutdown drain")
	}

	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()
	prov := b.provisions["lease-1"]
	require.NotNil(t, prov)
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status,
		"drained diagGatheredMsg must transition SM Failing→Failed")
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
		provs[leaseUUID] = &provision{
			LeaseUUID:    leaseUUID,
			Tenant:       "tenant-a",
			Status:       status,
			ContainerIDs: []string{fmt.Sprintf("c-%d", i)},
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
		"lease-1": {
			LeaseUUID:    "lease-1",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusReady,
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
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1"},
			FailCount:    0,
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

// TestSpawnProvisionWorker_PanicRecovery pins the invariant that a
// panic in the provision worker does NOT crash fred: the recover logs
// the panic with stack, bumps lease_worker_panics_total{worker_type=
// "provision"}, drives the SM to Failed, and lets the actor keep
// serving other messages. Without this recovery an unrecovered panic
// would take down the entire fred binary (Go's panic semantics).
func TestSpawnProvisionWorker_PanicRecovery(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusProvisioning,
		},
	})
	defer b.stopCancel()

	actor := b.actorFor("lease-1")
	panicsBefore := testutil.ToFloat64(leaseWorkerPanicsTotal.WithLabelValues("provision"))

	// Inject a worker that panics instead of doing real work. The
	// recover must catch the panic, bump the metric, and fire a
	// provisionErrored terminal so the SM transitions to Failed.
	actor.spawnProvisionWorker(func() (string, provisionSuccessResult, map[string]string, error) {
		panic("synthetic provision panic")
	})

	// SM must reach Failed within a short window — proves the recover
	// fired the terminal event and the actor processed it.
	require.Eventually(t, func() bool {
		return actor.sm.State() == backend.ProvisionStatusFailed
	}, 2*time.Second, 10*time.Millisecond,
		"SM must transition to Failed after worker panic recovery")

	panicsAfter := testutil.ToFloat64(leaseWorkerPanicsTotal.WithLabelValues("provision"))
	assert.Equal(t, panicsBefore+1, panicsAfter,
		"lease_worker_panics_total{worker_type=provision} must increment by 1")

	// Actor is still alive and responsive — send a container-death
	// event and verify it gets handled (the run loop didn't die).
	done := make(chan struct{})
	require.True(t, actor.send(containerDiedMsg{containerID: "nonexistent", done: done}))
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("actor stopped processing after worker panic — fred would have crashed")
	}
}

// TestSpawnReplaceWorker_PanicRecovery: same invariant for the
// restart/update worker path.
func TestSpawnReplaceWorker_PanicRecovery(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusRestarting,
		},
	})
	defer b.stopCancel()

	actor := b.actorFor("lease-1")
	panicsBefore := testutil.ToFloat64(leaseWorkerPanicsTotal.WithLabelValues("replace"))

	actor.spawnReplaceWorker(func() replaceResult {
		panic("synthetic replace panic")
	})

	require.Eventually(t, func() bool {
		return actor.sm.State() == backend.ProvisionStatusFailed
	}, 2*time.Second, 10*time.Millisecond,
		"SM must transition to Failed after replace worker panic recovery")

	panicsAfter := testutil.ToFloat64(leaseWorkerPanicsTotal.WithLabelValues("replace"))
	assert.Equal(t, panicsBefore+1, panicsAfter,
		"lease_worker_panics_total{worker_type=replace} must increment by 1")

	done := make(chan struct{})
	require.True(t, actor.send(containerDiedMsg{containerID: "nonexistent", done: done}))
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("actor stopped processing after replace worker panic")
	}
}

// TestGatherDiagAsync_PanicRecovery: the diag worker (spawned from
// Failing.OnEntry) must also recover. A panic in containerFailureDiagnostics
// used to crash fred; now it bumps the metric and drives Failing→Failed
// with empty diag so the lease isn't wedged in Failing.
func TestGatherDiagAsync_PanicRecovery(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			panic("synthetic diag panic")
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusFailing,
			ContainerIDs: []string{"c1"},
		},
	})
	defer b.stopCancel()

	actor := b.actorFor("lease-1")
	panicsBefore := testutil.ToFloat64(leaseWorkerPanicsTotal.WithLabelValues("diag"))

	// Drive Failing.OnEntry by invoking gatherDiagAsync directly with a
	// cancellable context matching what onEnterFailing would set up.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor.workers.Add()
	go func() {
		defer actor.workers.Done()
		actor.gatherDiagAsync(ctx, "c1", &ContainerInfo{ContainerID: "c1", Status: "exited", ExitCode: 1})
	}()

	// SM must reach Failed once the panic recovery fires diagGatheredMsg.
	require.Eventually(t, func() bool {
		return actor.sm.State() == backend.ProvisionStatusFailed
	}, 2*time.Second, 10*time.Millisecond,
		"SM must transition Failing→Failed after diag worker panic recovery")

	panicsAfter := testutil.ToFloat64(leaseWorkerPanicsTotal.WithLabelValues("diag"))
	assert.Equal(t, panicsBefore+1, panicsAfter,
		"lease_worker_panics_total{worker_type=diag} must increment by 1")
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
func TestHandlerPanic_UnblocksReplyChannel(t *testing.T) {
	// A mock whose InspectContainer panics — handleDeprovision calls
	// doDeprovision which reads ContainerIDs and calls RemoveContainer;
	// panicking RemoveContainer causes the handler to panic after the
	// actor has entered its SM transition to Deprovisioning.
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			panic("synthetic handler panic")
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1"},
		},
	})
	defer b.stopCancel()

	panicsBefore := testutil.ToFloat64(leaseActorPanicsTotal)

	// Deprovision must return within a short window — not hang on the
	// reply channel waiting for a message that will never arrive.
	done := make(chan error, 1)
	go func() {
		done <- b.Deprovision(context.Background(), "lease-1")
	}()

	select {
	case err := <-done:
		// Panic recovered; reply channel received the panic-error.
		// Any error value is fine; what matters is NOT hanging.
		_ = err
	case <-time.After(3 * time.Second):
		t.Fatal("Backend.Deprovision hung after handler panic — onPanic hook did not unblock the reply channel")
	}

	panicsAfter := testutil.ToFloat64(leaseActorPanicsTotal)
	assert.Greater(t, panicsAfter, panicsBefore,
		"leaseActorPanicsTotal must increment for handler panic")
}

// TestTerminatedActor_RejectsCallerFacingRequests pins the fix for the
// lifecycle race between handleDeprovision setting a.terminated=true
// and the actor's deferred removeFromRegistry() actually running.
//
// Scenario: a Backend.Provision (or Restart/Update) routes a message
// after Deprovision has removed the provision entry but before the
// actor has been removed from b.actors. Without the terminated check,
// the handler fires evProvisionRequested into the SM — which is in
// Deprovisioning state — and Deprovisioning.Ignore(evProvisionRequested)
// causes Fire to return nil. The handler interprets that as "SM
// accepted the transition," acks success, and spawns a worker under
// a terminated actor. Containers get created, no callback fires,
// lease is wedged.
//
// With the fix, the handler checks a.terminated first and returns
// errActorTerminated on ack. The caller (Backend.X) rolls back via
// removeProvision; a retry resolves-or-creates a fresh actor.
func TestTerminatedActor_RejectsCallerFacingRequests(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusDeprovisioning,
		},
	})

	actor := b.actorFor("lease-1")

	// Shut the actor's run goroutine down first so the test goroutine
	// is the sole reader/writer of actor.terminated and of the SM.
	// In production, these handlers run in the actor's own goroutine
	// after handleDeprovision has set terminated=true and the main
	// loop is draining the inbox via the run-exit defers.
	b.stopCancel()
	select {
	case <-actor.done:
	case <-time.After(2 * time.Second):
		t.Fatal("actor did not exit after stopCancel")
	}
	actor.terminated = true // simulate post-handleDeprovision state

	t.Run("Provision", func(t *testing.T) {
		var workerSpawned atomic.Bool
		msg := provisionRequestedMsg{
			cancel: func() {},
			work: func() (string, provisionSuccessResult, map[string]string, error) {
				workerSpawned.Store(true)
				return "", provisionSuccessResult{}, nil, nil
			},
			ack: make(chan error, 1),
		}
		actor.handleProvisionRequested(msg)

		select {
		case err := <-msg.ack:
			assert.ErrorIs(t, err, errActorTerminated,
				"terminated actor must reject Provision with errActorTerminated")
		case <-time.After(time.Second):
			t.Fatal("ack channel never received a value — handler did not unblock caller")
		}
		// Give any stray goroutine time to fire before we assert.
		time.Sleep(20 * time.Millisecond)
		assert.False(t, workerSpawned.Load(),
			"terminated actor must NOT spawn a provision worker")
	})

	t.Run("Restart", func(t *testing.T) {
		var workerSpawned atomic.Bool
		msg := restartRequestedMsg{
			cancel: func() {},
			work: func() replaceResult {
				workerSpawned.Store(true)
				return replaceResult{}
			},
			ack: make(chan error, 1),
		}
		actor.handleRestartRequested(msg)

		select {
		case err := <-msg.ack:
			assert.ErrorIs(t, err, errActorTerminated,
				"terminated actor must reject Restart with errActorTerminated")
		case <-time.After(time.Second):
			t.Fatal("ack channel never received a value")
		}
		time.Sleep(20 * time.Millisecond)
		assert.False(t, workerSpawned.Load(),
			"terminated actor must NOT spawn a replace worker for Restart")
	})

	t.Run("Update", func(t *testing.T) {
		var workerSpawned atomic.Bool
		msg := updateRequestedMsg{
			cancel: func() {},
			work: func() replaceResult {
				workerSpawned.Store(true)
				return replaceResult{}
			},
			ack: make(chan error, 1),
		}
		actor.handleUpdateRequested(msg)

		select {
		case err := <-msg.ack:
			assert.ErrorIs(t, err, errActorTerminated,
				"terminated actor must reject Update with errActorTerminated")
		case <-time.After(time.Second):
			t.Fatal("ack channel never received a value")
		}
		time.Sleep(20 * time.Millisecond)
		assert.False(t, workerSpawned.Load(),
			"terminated actor must NOT spawn a replace worker for Update")
	})
}

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

// TestSendTerminal_RejectsAfterExitingClosed pins the fix for the
// post-drain / pre-done window. Without the exiting signal, a late
// worker sendTerminal in that window would succeed and the message
// would rot in an unread inbox. With exiting closed, sendTerminal
// rejects (returns false) and the caller can correctly count the drop.
//
// We can't easily reproduce the real waitForWorkers-timeout scenario
// without wedging a worker for 75s, so we exercise the refusal directly:
// close exiting, then call sendTerminal, assert false. The closeExiting
// helper is idempotent (sync.Once) so the production exit defer running
// later on b.stopCancel() will not double-close.
func TestSendTerminal_RejectsAfterExitingClosed(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	defer b.stopCancel()
	actor := b.actorFor("lease-1")

	// Actor is still running; a.done is NOT closed. Before exiting is
	// closed, sendTerminal accepts.
	require.True(t, actor.sendTerminal(provisionCompletedMsg{}),
		"baseline: sendTerminal should succeed on a live actor")

	// Simulate the exit-sequence defer that closes this channel just
	// before drainInbox runs. After this, sendTerminal must reject.
	actor.closeExiting()

	rejected := !actor.sendTerminal(provisionCompletedMsg{})
	assert.True(t, rejected,
		"sendTerminal must reject once exiting is closed — otherwise a post-drain send would rot in the inbox")
}

// TestGatherDiagAsync_SendsOnDeadlineExceeded pins bug_002: before the fix,
// gatherDiagAsync suppressed the terminal send on ANY ctx.Err(), including
// the 30s diagnosticsGatherTimeout elapsing. The SM stayed in Failing
// forever and the Failed callback never fired. After the fix, only
// context.Canceled (from Failing.OnExit's diagCancel) suppresses; timeout
// expiry falls through to send diagGatheredMsg (which always carries at
// least "exit_code=N") and drives Failing→Failed.
func TestGatherDiagAsync_SendsOnDeadlineExceeded(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, _ string, _ int) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		},
	}
	b := newBackendForTest(mock, nil)
	defer b.stopCancel()

	// Build an actor without running its dispatch loop so we can observe
	// the inbox directly after gatherDiagAsync returns.
	actor := newLeaseActor(b, "lease-1")
	info := &ContainerInfo{ContainerID: "c1", Status: "exited", ExitCode: 1}

	// Build a ctx that's already DeadlineExceeded (simulating the 30s
	// diag timeout having elapsed without any Failing.OnExit cancel).
	diagCtx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)
	require.ErrorIs(t, diagCtx.Err(), context.DeadlineExceeded,
		"test precondition: diagCtx must be DeadlineExceeded, not Canceled")

	actor.gatherDiagAsync(diagCtx, "c1", info)

	select {
	case msg := <-actor.inbox:
		_, ok := msg.(diagGatheredMsg)
		assert.True(t, ok, "gatherDiagAsync must enqueue diagGatheredMsg on DeadlineExceeded, got %T", msg)
	default:
		t.Fatal("gatherDiagAsync suppressed terminal send on DeadlineExceeded — lease would wedge in Failing (bug_002)")
	}
}

// TestGatherDiagAsync_SuppressesOnCanceled is the other half of bug_002:
// when Failing.OnExit's diagCancel fires (a Deprovision/Restart/Update
// preempt), the SM has already left Failing and any diagGatheredMsg
// would hit Deprovisioning.Ignore. Suppression is the correct behavior
// there, so the fix must NOT turn this case into a spurious send.
func TestGatherDiagAsync_SuppressesOnCanceled(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, _ string, _ int) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		},
	}
	b := newBackendForTest(mock, nil)
	defer b.stopCancel()

	actor := newLeaseActor(b, "lease-1")
	info := &ContainerInfo{ContainerID: "c1", Status: "exited", ExitCode: 1}

	diagCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, diagCtx.Err(), context.Canceled,
		"test precondition: diagCtx must be Canceled")

	actor.gatherDiagAsync(diagCtx, "c1", info)

	select {
	case msg := <-actor.inbox:
		t.Fatalf("gatherDiagAsync must suppress terminal send on Canceled; got %T", msg)
	default:
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

// TestHandleProvisionRequested_RejectsWhenSMInDeprovisioning pins bug_005:
// when a Deprovision partially fails, the provision entry is preserved
// with Status=Failed but the actor is NOT terminated and the SM remains
// in Deprovisioning. A retry Provision (accepted by the sync-phase
// Failed-guard) routes to the same actor. Fire(evProvisionRequested) on
// Deprovisioning is Ignored → returns nil. Without the post-Fire state
// check, the handler would ack success and spawn a worker that creates
// real containers whose provisionCompletedMsg would ALSO be Ignored,
// wedging the lease. The fix verifies sm.State() == Provisioning after
// Fire and rejects otherwise.
func TestHandleProvisionRequested_RejectsWhenSMInDeprovisioning(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusDeprovisioning,
			ContainerIDs: []string{"c1"},
		},
	})
	defer b.stopCancel()

	actor := b.actorFor("lease-1")
	// actorForLocked initializes the SM from prov.Status, so SM is in
	// Deprovisioning. terminated stays false because handleDeprovision
	// was never run (simulating a partial-deprov scenario where the
	// actor's run loop is alive with SM=Deprovisioning).
	require.Equal(t, backend.ProvisionStatusDeprovisioning, actor.sm.State(),
		"test precondition: SM must be in Deprovisioning")

	ack := make(chan error, 1)
	msg := provisionRequestedMsg{
		cancel: func() {},
		work: func() (string, provisionSuccessResult, map[string]string, error) {
			return "", provisionSuccessResult{}, nil, nil
		},
		ack: ack,
	}
	actor.handleProvisionRequested(msg)

	select {
	case err := <-ack:
		require.Error(t, err,
			"handleProvisionRequested must reject when SM silently Ignores the event (bug_005); got ack success")
	default:
		t.Fatal("handler did not send on ack channel")
	}
}
