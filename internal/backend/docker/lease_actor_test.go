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
// cc62f3b invariant structurally: when a container-death event and a
// Deprovision call race on the same lease, exactly ONE terminal callback
// fires (Deprovisioned), never a stale Failed that Fred already knows is
// being torn down.
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

// TestLeaseActor_EagerSMInit pins the fix for the DebugActors data race:
// the SM must be constructed inside newLeaseActor, not lazily on first
// handler invocation. Lazy init raced with DebugActors readers under
// -race; the eager init makes the pointer publication synchronous with
// the actor's exposure via b.actors.
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

// TestLeaseActor_RegistryClearedAfterDeprovision pins the fix for the
// actor-registry leak and its downstream UUID-reuse wedge: once
// Deprovision removes the provision entry, the actor exits and removes
// itself from b.actors. Without this, every deprovisioned lease would
// leak a goroutine plus an SM stuck in Deprovisioning, and any
// subsequent Provision with the same UUID would be silently Ignored by
// that stale SM.
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
	_, exists := b.actors.Load("lease-1")
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

// TestLeaseActor_DrainsTerminalEventsOnShutdown pins the structural fix
// for bug_004: when stopCtx fires with an in-flight work goroutine, the
// actor must (1) wait for the goroutine to close workDone, (2) drain the
// inbox so the terminal SM event is processed, and only then exit. The
// pre-fix behavior dropped the event at the send-check because the actor
// had already exited on stopCtx, leaving the release store / callback
// record out of sync with the physical host.
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

	// Install synthetic work-goroutine handles on the actor (same
	// mechanism the real provision.go goroutine uses).
	workDone := make(chan struct{})
	actor.workCancel = func() {}
	actor.workDone = workDone

	// Fire shutdown BEFORE the goroutine has finished. The actor should
	// not exit yet — it has to wait for workDone + drain the inbox.
	b.stopCancel()

	// Simulate the work goroutine: send its terminal event via
	// sendTerminal (which must not be refused on shutdown), then close
	// workDone. The actor should then drain the inbox and process the
	// event, flipping Status to Ready.
	go func() {
		ok := actor.sendTerminal(provisionCompletedMsg{
			result: provisionSuccessResult{
				containerIDs: []string{"c1"},
			},
		})
		require.True(t, ok, "sendTerminal must not refuse during shutdown drain")
		close(workDone)
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

// TestLeaseActor_RestartDeprovisionWaitsForInFlightGoroutine mirrors the
// Provision variant of the bug_012 test for the Restart flow: when
// Deprovision preempts an in-flight restart, Restarting.OnExit must
// cancel the work goroutine and wait on workDone before doDeprovision
// reads ContainerIDs. Guards against future refactors that move the
// replace spawn sites without plumbing the done channel.
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

	// Same synthetic-goroutine pattern as the Provision variant.
	var cancelCalled atomic.Bool
	goroutineFinished := make(chan struct{})
	actor.workCancel = func() { cancelCalled.Store(true) }
	actor.workDone = goroutineFinished

	deprovErr := make(chan error, 1)
	go func() {
		deprovErr <- b.Deprovision(context.Background(), "lease-1")
	}()

	require.Eventually(t, cancelCalled.Load, 1*time.Second, 5*time.Millisecond,
		"OnExit must call workCancel before waiting on workDone (Restart path)")

	select {
	case err := <-deprovErr:
		t.Fatalf("Deprovision returned before workDone closed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Simulate the replace goroutine publishing new containerIDs (as
	// fireReplaceOutcome does today) and closing done.
	b.provisionsMu.Lock()
	b.provisions["lease-1"].ContainerIDs = []string{"new-container"}
	b.provisionsMu.Unlock()
	close(goroutineFinished)

	select {
	case err := <-deprovErr:
		require.NoError(t, err, "Deprovision must succeed after workDone closes")
	case <-time.After(3 * time.Second):
		t.Fatal("Deprovision did not complete after workDone closed")
	}

	removedMu.Lock()
	defer removedMu.Unlock()
	require.Contains(t, removedIDs, "new-container",
		"doDeprovision must see the new containerIDs published before fireReplaceOutcome")
}

// TestLeaseActor_DrainWaitsForDiagDone pins the structural fix for the
// diag-shutdown race: drainOnShutdown must wait on a.diagDone before
// exiting, regardless of which SM state the actor is in. Without the
// wait, a diag goroutine mid-execution when shutdown fires could have
// its sendTerminal land after drainOnShutdown returned — the message
// would queue into the inbox but be silently dropped by drainInbox
// because the actor is already tearing down.
//
// Installs a synthetic diagDone (the actor believes a diag goroutine is
// in flight), fires shutdown, verifies the actor does not exit until
// diagDone is closed.
func TestLeaseActor_DrainWaitsForDiagDone(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", Status: backend.ProvisionStatusFailing},
	})

	actor := b.actorFor("lease-1")
	diagHeld := make(chan struct{})
	actor.diagDone = diagHeld

	b.stopCancel()

	// Actor must block in drainOnShutdown waiting on diagDone.
	select {
	case <-actor.done:
		t.Fatal("actor exited before diagDone was closed — drainOnShutdown did not wait")
	case <-time.After(150 * time.Millisecond):
	}

	// Close diagDone. Actor must now exit cleanly.
	close(diagHeld)
	select {
	case <-actor.done:
	case <-time.After(2 * time.Second):
		t.Fatal("actor did not exit after diagDone was closed")
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
	var remaining int
	b.actors.Range(func(key, value any) bool {
		remaining++
		return true
	})
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
