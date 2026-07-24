package leasesm

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

// Tests in this file exercise the LeaseActor's internal semantics
// (SM transitions, worker spawning, terminal-event drain, panic recovery,
// send/sendTerminal mechanics). They live in `package leasesm` (internal
// tests) per Deviation #1 — they use unexported types/fields/methods
// directly because constructing test scenarios at this layer is much
// cleaner with same-package access than going through the docker
// substrate's exported surface.
//
// Backend-routing integration tests (b.routeToLease, b.routeToLeaseBlocking,
// b.Deprovision, etc.) stay in docker/lease_actor_test.go where they
// exercise the substrate's actor registry + HTTP callback machinery.

// TestLeaseActor_SendRefusesAfterActorExit pins the post-exit refusal
// contract on send(): once the actor's run loop has returned (a.done
// closed), send must return false rather than queue into an inbox no
// one will drain. Before the defer-reorder fix, post-exit send() could
// silently rot a message because the hasExited check didn't exist on
// this path.
func TestLeaseActor_SendRefusesAfterActorExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	actor := newTestActor(t, "lease-gone", testActorOpts{StopCtx: ctx})

	// Force the actor to exit by cancelling the test ctx (= StopCtx).
	cancel()
	<-actor.Done()

	ok := actor.send(ContainerDiedMsg{ContainerID: "c1"})
	assert.False(t, ok, "send must refuse once the actor has exited")
}

// TestLeaseActor_SendTerminalRefusesAfterActorExit guards the
// sendTerminal contract: once the actor has fully exited (a.done
// closed), sendTerminal must return false rather than block forever on
// a channel nobody will drain. This lets call sites count + log the
// dropped event instead of wedging the goroutine.
func TestLeaseActor_SendTerminalRefusesAfterActorExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	actor := newTestActor(t, "lease-gone", testActorOpts{StopCtx: ctx})

	cancel()
	<-actor.Done()

	ok := actor.sendTerminal(provisionCompletedMsg{})
	assert.False(t, ok, "sendTerminal must refuse once the actor has exited")
}

// TestSendTerminal_RejectsAfterExitingClosed pins the fix for the
// narrow window where a late-worker sendTerminal could queue into an
// inbox no one would drain.
//
// Sequence: the actor's exit-defer order is
//
//	waitForWorkers → removeFromRegistry → closeExiting → drainInbox → close(done)
//
// Between closeExiting and close(done), the inbox is no longer
// monitored. A worker calling sendTerminal in that window would land in
// a slot that gets dropped silently. The fix: sendTerminal checks
// isExiting() and refuses there, so the drop is counted via
// leaseTerminalEventDroppedTotal rather than rotting.
//
// Test drives the contract: close exiting, then call sendTerminal,
// assert false. closeExiting is idempotent (sync.Once) so the
// production exit defer running later via stopCtx cancellation won't
// double-close.
func TestSendTerminal_RejectsAfterExitingClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, "lease-1", testActorOpts{StopCtx: ctx})

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

// TestLeaseActor_SurvivesHandlerPanic pins the defer-recover in
// handle(): a panic in a handler path (SM guard, entry action, or
// downstream I/O) must be contained to a single message. Without
// recovery, the actor goroutine dies and senders block on a full
// inbox forever.
func TestLeaseActor_SurvivesHandlerPanic(t *testing.T) {
	var inspectCalls atomic.Int32
	var actorPanicsBefore int64

	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		ContainerIDs: []string{"c1"},
		Status:       backend.ProvisionStatusReady,
	})

	inspector := &mockInstanceInspector{
		InspectInstanceFn: func(ctx context.Context, instanceID string) (*InstanceState, error) {
			if inspectCalls.Add(1) == 1 {
				panic("simulated inspect failure")
			}
			exitCode := 1
			return &InstanceState{Phase: PhaseExited, ExitCode: &exitCode}, nil
		},
	}
	// Custom metrics to observe ActorPanic increments without reaching for
	// the docker prometheus counter (which the leasesm package can't
	// reference). The counter behavior is what we assert here; the
	// substrate adapter's prometheus mapping is exercised elsewhere.
	metrics := &countingMetrics{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:        ctx,
		Inspector:      inspector,
		ProvisionStore: store,
		Metrics:        metrics,
	})

	// First send: panics inside the SM guard.
	require.True(t, actor.TryEnqueue(ContainerDiedMsg{ContainerID: "c1"}))
	require.Eventually(t, func() bool {
		return metrics.actorPanic.Load() > actorPanicsBefore
	}, 2*time.Second, 10*time.Millisecond,
		"ActorPanic must be invoked when a handler panics")

	// Second send: must be processed — the actor survived the panic.
	done := make(chan struct{})
	require.True(t, actor.TryEnqueue(ContainerDiedMsg{ContainerID: "c1", Done: done}))
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("actor did not process a message after recovering from panic")
	}
}

// TestLeaseActor_DrainsTerminalEventsOnShutdown pins the structural
// invariant for terminal-event delivery during shutdown: when stopCtx
// fires with an in-flight work goroutine, the actor must (1) wait for
// the goroutine to complete (via workers.Zero), (2) drain the inbox so
// the terminal SM event is processed via handle(), and only then exit.
// Without the drain, the event would be dropped at the send-check
// because the actor had already exited on stopCtx.
func TestLeaseActor_DrainsTerminalEventsOnShutdown(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		Status:       backend.ProvisionStatusProvisioning,
		ContainerIDs: nil,
	})

	ctx, cancel := context.WithCancel(context.Background())
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
	})
	require.Equal(t, backend.ProvisionStatusProvisioning, actor.State())

	// Simulate an in-flight worker via workers.Add. The actor's exit-path
	// waitForWorkers defer will block until we Done().
	actor.workers.Add()

	// Fire shutdown BEFORE the worker completes. The actor's run loop
	// returns immediately but waitForWorkers must block.
	cancel()

	// Simulate the worker: send terminal event, then Done(). The actor
	// should then drain the inbox (via handle()) and process the event,
	// flipping Status to Ready.
	go func() {
		ok := actor.sendTerminal(provisionCompletedMsg{
			result: ProvisionSuccessResult{
				ContainerIDs: []string{"c1"},
			},
		})
		require.True(t, ok, "sendTerminal must not refuse during shutdown drain")
		actor.workers.Done()
	}()

	// The actor must exit cleanly, and the provision must be Ready.
	select {
	case <-actor.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("actor did not exit after shutdown drain")
	}

	prov, ok := store.Get("lease-1")
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status,
		"terminal provisionCompletedMsg sent during shutdown must have been drained and processed")
	assert.Equal(t, []string{"c1"}, prov.ContainerIDs,
		"ContainerIDs must reflect the SM entry action running on the drained event")
}

// TestLeaseActor_ExitWaitsForWorkers pins the structural fix for the
// shutdown-drain race: the actor's exit-path waitForWorkers defer must
// block until every worker goroutine (provision/restart/update/diag)
// has returned. Without it, a worker's sendTerminal could land after
// the actor's drainInbox had already run, dropping the terminal event.
//
// Installs a synthetic worker via workers.Add, fires shutdown, verifies
// the actor does not exit until the worker Done()s.
func TestLeaseActor_ExitWaitsForWorkers(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID: "lease-1",
		Status:    backend.ProvisionStatusFailing,
	})

	ctx, cancel := context.WithCancel(context.Background())
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
	})
	actor.workers.Add()

	cancel()

	// Actor must block in waitForWorkers until we Done().
	select {
	case <-actor.Done():
		t.Fatal("actor exited before worker Done() — waitForWorkers did not block")
	case <-time.After(150 * time.Millisecond):
	}

	actor.workers.Done()

	select {
	case <-actor.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("actor did not exit after worker Done()")
	}
}

// TestGatherDiagAsync_SendsOnDeadlineExceeded pins bug_002: before the
// fix, gatherDiagAsync suppressed the terminal send on ANY ctx.Err(),
// including the 30s diagnosticsGatherTimeout elapsing. The SM stayed
// in Failing forever and the Failed callback never fired. After the
// fix, only context.Canceled (from Failing.OnExit's diagCancel)
// suppresses; timeout expiry falls through to send diagGatheredMsg
// (which always carries at least "exit_code=N") and drives
// Failing→Failed.
func TestGatherDiagAsync_SendsOnDeadlineExceeded(t *testing.T) {
	diag := &mockDiagnosticsGatherer{
		GatherDiagnosticsFn: func(ctx context.Context, _ string, _ *InstanceState) string {
			<-ctx.Done()
			return ""
		},
	}
	actor := newTestActorNoSpawn(t, "lease-1", testActorOpts{Diag: diag})
	exitCode := 1
	info := &InstanceState{Phase: PhaseExited, ExitCode: &exitCode}

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
	diag := &mockDiagnosticsGatherer{
		GatherDiagnosticsFn: func(ctx context.Context, _ string, _ *InstanceState) string {
			<-ctx.Done()
			return ""
		},
	}
	actor := newTestActorNoSpawn(t, "lease-1", testActorOpts{Diag: diag})
	exitCode := 1
	info := &InstanceState{Phase: PhaseExited, ExitCode: &exitCode}

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

// TestLeaseActor_RestartDeprovisionWaitsForInFlightGoroutine guards
// the orphan-containers invariant for the Restart flow: when
// Deprovision preempts an in-flight restart, Restarting.OnExit must
// cancel the work goroutine and wait on workers before doDeprovision
// runs. Guards against future refactors that move the replace spawn
// sites without plumbing the workers barrier.
//
// The Update flow uses the exact same SM transition and OnExit
// handler, so this single test covers both — the behaviour is
// identical.
func TestLeaseActor_RestartDeprovisionWaitsForInFlightGoroutine(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		Status:       backend.ProvisionStatusRestarting,
		ContainerIDs: []string{"old-container"},
	})

	var deprovDoneIDs []string
	var deprovMu sync.Mutex
	deprovRan := make(chan struct{})
	doDeprovision := func(ctx context.Context, leaseUUID string) error {
		// At the point doDeprovision runs, the worker has Done() and
		// pre-published "new-container". Capture what we see.
		state, _ := store.Get(leaseUUID)
		deprovMu.Lock()
		deprovDoneIDs = append(deprovDoneIDs, state.ContainerIDs...)
		deprovMu.Unlock()
		// Mirror real doDeprovision's side effect: remove the provision
		// entry so handleDeprovision's post-check sets terminated=true.
		store.remove(leaseUUID)
		close(deprovRan)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:         ctx,
		ProvisionStore:  store,
		DoDeprovisionFn: doDeprovision,
	})
	require.Equal(t, backend.ProvisionStatusRestarting, actor.State())

	// Simulate an in-flight replace worker via workers.Add + workCancel
	// closure. Same construction as the production spawnReplaceWorker:
	// onExitProvisioning will call workCancel then wait on workers.Zero().
	var cancelCalled atomic.Bool
	workerRelease := make(chan struct{})
	actor.workCancel = func() { cancelCalled.Store(true) }
	actor.workers.Add()
	go func() {
		<-workerRelease
		// Publish new container IDs before Done — mirrors the real
		// replace worker's pre-publish step (so doDeprovision sees
		// the new IDs).
		store.UpdateFn("lease-1", func(p *ProvisionState) {
			p.ContainerIDs = []string{"new-container"}
		})
		actor.workers.Done()
	}()

	// Route the Deprovision message through the actor's inbox. The
	// actor's handler fires evDeprovisionRequested → Restarting.OnExit
	// (cancels worker, waits on workers.Zero) → handleDeprovision body
	// runs (calls DoDeprovisionFn).
	reply := make(chan error, 1)
	require.True(t, actor.TryEnqueue(DeprovisionMsg{Ctx: context.Background(), Reply: reply}))

	require.Eventually(t, cancelCalled.Load, 1*time.Second, 5*time.Millisecond,
		"OnExit must call workCancel before waiting for the worker (Restart path)")

	// doDeprovision must NOT have run yet — it's blocked behind
	// waitForWorkers waiting for the in-flight goroutine.
	select {
	case <-deprovRan:
		t.Fatal("doDeprovision ran before worker finished — onExitProvisioning didn't wait on workers")
	case <-time.After(100 * time.Millisecond):
	}

	// Release the worker → workers.Done → onExitProvisioning unblocks →
	// doDeprovision runs.
	close(workerRelease)

	select {
	case err := <-reply:
		require.NoError(t, err, "Deprovision must succeed after worker completes")
	case <-time.After(3 * time.Second):
		t.Fatal("Deprovision did not complete after worker finished")
	}

	deprovMu.Lock()
	defer deprovMu.Unlock()
	require.Contains(t, deprovDoneIDs, "new-container",
		"doDeprovision must see the new containerIDs published by the replace worker")
}

// TestLeaseActor_DiagGathered_ShutdownDrain exercises the
// drain-on-shutdown path for a lease in the Failing state: stopCtx
// fires with a diagGatheredMsg freshly queued in the inbox, and the
// actor must process it (transitioning Failing→Failed) before
// exiting. Pre-fix: the actor would exit on stopCtx.Done and the diag
// message would be discarded by drainInbox without handling, leaving
// Status stuck at Failing.
//
// Asserts on the SM-observable outcome (Status flip) rather than HTTP
// callback delivery — the substrate's callback sender during shutdown
// is a separate concern tested elsewhere.
func TestLeaseActor_DiagGathered_ShutdownDrain(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		ContainerIDs: []string{"c1"},
		Status:       backend.ProvisionStatusFailing,
	})

	ctx, cancel := context.WithCancel(context.Background())
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
	})
	require.Equal(t, backend.ProvisionStatusFailing, actor.State())

	// Queue a diagGatheredMsg, then fire shutdown. Whatever the inbox
	// select picks next (msg or stopCtx.Done), drainInbox must process
	// the queued message before the actor exits.
	exitCode := 1
	ok := actor.sendTerminal(diagGatheredMsg{
		result: diagResult{
			containerID: "c1",
			info:        &InstanceState{Phase: PhaseExited, ExitCode: &exitCode},
			diag:        "synthetic diag",
		},
	})
	require.True(t, ok, "sendTerminal must enqueue while the actor is alive")
	cancel()

	select {
	case <-actor.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("actor did not exit after shutdown drain")
	}

	prov, ok := store.Get("lease-1")
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status,
		"drained diagGatheredMsg must transition SM Failing→Failed")
}

// TestSpawnProvisionWorker_PanicRecovery pins the invariant that a
// panic in the provision worker does NOT crash fred: the recover logs
// the panic with stack, bumps WorkerPanic("provision"), drives the SM
// to Failed, and lets the actor keep serving other messages. Without
// this recovery an unrecovered panic would take down the entire fred
// binary (Go's panic semantics).
func TestSpawnProvisionWorker_PanicRecovery(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID: "lease-1",
		Tenant:    "tenant-a",
		Status:    backend.ProvisionStatusProvisioning,
	})

	metrics := &countingMetrics{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		Metrics:        metrics,
	})

	panicsBefore := metrics.workerPanic.Load()

	// Inject a worker that panics instead of doing real work. The
	// recover must catch the panic, bump the metric, and fire a
	// provisionErrored terminal so the SM transitions to Failed.
	actor.spawnProvisionWorker(func() (string, backend.Reason, ProvisionSuccessResult, map[string]string, error) {
		panic("synthetic provision panic")
	})

	// SM must reach Failed within a short window — proves the recover
	// fired the terminal event and the actor processed it.
	require.Eventually(t, func() bool {
		return actor.State() == backend.ProvisionStatusFailed
	}, 2*time.Second, 10*time.Millisecond,
		"SM must transition to Failed after worker panic recovery")

	panicsAfter := metrics.workerPanic.Load()
	assert.Equal(t, panicsBefore+1, panicsAfter,
		"WorkerPanic metric must increment by 1 after provision worker panic")

	// Actor is still alive and responsive — send a container-death
	// event and verify it gets handled (the run loop didn't die).
	done := make(chan struct{})
	require.True(t, actor.TryEnqueue(ContainerDiedMsg{ContainerID: "nonexistent", Done: done}))
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("actor stopped processing after worker panic — fred would have crashed")
	}
}

// TestSpawnReplaceWorker_PanicRecovery: same invariant for the
// restart/update worker path.
func TestSpawnReplaceWorker_PanicRecovery(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID: "lease-1",
		Tenant:    "tenant-a",
		Status:    backend.ProvisionStatusRestarting,
	})

	metrics := &countingMetrics{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		Metrics:        metrics,
	})

	panicsBefore := metrics.workerPanic.Load()

	actor.spawnReplaceWorker(func() ReplaceResult {
		panic("synthetic replace panic")
	})

	require.Eventually(t, func() bool {
		return actor.State() == backend.ProvisionStatusFailed
	}, 2*time.Second, 10*time.Millisecond,
		"SM must transition to Failed after replace worker panic recovery")

	panicsAfter := metrics.workerPanic.Load()
	assert.Equal(t, panicsBefore+1, panicsAfter,
		"WorkerPanic metric must increment by 1 after replace worker panic")

	done := make(chan struct{})
	require.True(t, actor.TryEnqueue(ContainerDiedMsg{ContainerID: "nonexistent", Done: done}))
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("actor stopped processing after replace worker panic")
	}
}

// TestGatherDiagAsync_PanicRecovery: the diag worker (spawned from
// Failing.OnEntry) must also recover. A panic in the diag gatherer
// used to crash fred; now it bumps the metric and drives Failing→Failed
// with empty diag so the lease isn't wedged in Failing.
func TestGatherDiagAsync_PanicRecovery(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		Status:       backend.ProvisionStatusFailing,
		ContainerIDs: []string{"c1"},
	})

	diag := &mockDiagnosticsGatherer{
		GatherDiagnosticsFn: func(ctx context.Context, _ string, _ *InstanceState) string {
			panic("synthetic diag panic")
		},
	}

	metrics := &countingMetrics{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		Diag:           diag,
		Metrics:        metrics,
	})

	panicsBefore := metrics.workerPanic.Load()

	// Drive Failing.OnEntry by invoking gatherDiagAsync directly with
	// a cancellable context matching what onEnterFailing would set up.
	gatherCtx, gatherCancel := context.WithCancel(context.Background())
	defer gatherCancel()
	actor.workers.Add()
	go func() {
		defer actor.workers.Done()
		exitCode := 1
		actor.gatherDiagAsync(gatherCtx, "c1", &InstanceState{Phase: PhaseExited, ExitCode: &exitCode})
	}()

	// SM must reach Failed once the panic recovery fires diagGatheredMsg.
	require.Eventually(t, func() bool {
		return actor.State() == backend.ProvisionStatusFailed
	}, 2*time.Second, 10*time.Millisecond,
		"SM must transition Failing→Failed after diag worker panic recovery")

	panicsAfter := metrics.workerPanic.Load()
	assert.Equal(t, panicsBefore+1, panicsAfter,
		"WorkerPanic metric must increment by 1 after diag worker panic")
}

// TestTerminatedActor_RejectsCallerFacingRequests pins the fix for
// the lifecycle race between handleDeprovision setting
// a.terminated=true and the actor's deferred removeFromRegistry()
// actually running.
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
// errActorTerminated on ack. The caller rolls back via removeProvision;
// a retry resolves-or-creates a fresh actor.
func TestTerminatedActor_RejectsCallerFacingRequests(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID: "lease-1",
		Tenant:    "tenant-a",
		Status:    backend.ProvisionStatusDeprovisioning,
	})

	// Use the no-spawn variant so we are the sole reader/writer of
	// a.terminated and of the SM. In production, these handlers run
	// in the actor's own goroutine after handleDeprovision has set
	// terminated=true and the main loop is draining via the run-exit
	// defers.
	actor := newTestActorNoSpawn(t, "lease-1", testActorOpts{ProvisionStore: store})
	actor.terminated = true // simulate post-handleDeprovision state

	t.Run("Provision", func(t *testing.T) {
		var workerSpawned atomic.Bool
		msg := ProvisionRequestedMsg{
			Cancel: func() {},
			Work: func() (string, backend.Reason, ProvisionSuccessResult, map[string]string, error) {
				workerSpawned.Store(true)
				return "", "", ProvisionSuccessResult{}, nil, nil
			},
			Ack: make(chan error, 1),
		}
		actor.handleProvisionRequested(msg)

		select {
		case err := <-msg.Ack:
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
		msg := RestartRequestedMsg{
			Cancel: func() {},
			Work: func() ReplaceResult {
				workerSpawned.Store(true)
				return ReplaceResult{}
			},
			Ack: make(chan error, 1),
		}
		actor.handleRestartRequested(msg)

		select {
		case err := <-msg.Ack:
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
		msg := UpdateRequestedMsg{
			Cancel: func() {},
			Work: func() ReplaceResult {
				workerSpawned.Store(true)
				return ReplaceResult{}
			},
			Ack: make(chan error, 1),
		}
		actor.handleUpdateRequested(msg)

		select {
		case err := <-msg.Ack:
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

// TestHandleProvisionRequested_RejectsWhenSMInDeprovisioning pins
// bug_005: between handleDeprovision setting a.terminated=true and
// removeFromRegistry running, a Backend.Provision (passing the
// Backend.Provision Failed-guard) routes to the same actor.
// Fire(evProvisionRequested) on Deprovisioning is Ignored → returns
// nil. Without the post-Fire state check, the handler would ack
// success and spawn a worker that creates real containers whose
// provisionCompletedMsg would ALSO be Ignored, wedging the lease.
// The fix verifies sm.State() == Provisioning after Fire and rejects
// otherwise.
func TestHandleProvisionRequested_RejectsWhenSMInDeprovisioning(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		Status:       backend.ProvisionStatusDeprovisioning,
		ContainerIDs: []string{"c1"},
	})

	// No-spawn so we drive handleProvisionRequested synchronously
	// without racing the run loop's draining of the message we're
	// about to construct.
	actor := newTestActorNoSpawn(t, "lease-1", testActorOpts{ProvisionStore: store})
	// newLeaseSM initializes the SM from prov.Status, so SM is in
	// Deprovisioning. terminated stays false because handleDeprovision
	// was never run (simulating a partial-deprov scenario where the
	// actor's run loop is alive with SM=Deprovisioning).
	require.Equal(t, backend.ProvisionStatusDeprovisioning, actor.State(),
		"test precondition: SM must be in Deprovisioning")

	ack := make(chan error, 1)
	msg := ProvisionRequestedMsg{
		Cancel: func() {},
		Work: func() (string, backend.Reason, ProvisionSuccessResult, map[string]string, error) {
			return "", "", ProvisionSuccessResult{}, nil, nil
		},
		Ack: ack,
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

// TestProvision_DeprovisionWaitsForInFlightGoroutine guards the
// orphan-containers invariant for the Provision flow: when Deprovision
// preempts an in-flight doProvision, Provisioning.OnExit must (1)
// cancel the goroutine's context and (2) wait for the goroutine to
// exit before doDeprovision reads ContainerIDs. Without this, the
// goroutine's successful container creations stay on the host even
// though the provision struct reports none.
//
// Migrated from docker/provision_test.go at PR5b-2 E sub-batch 3.
// Mirrors TestLeaseActor_RestartDeprovisionWaitsForInFlightGoroutine
// but with the Provisioning→Deprovisioning SM transition instead of
// Restarting→Deprovisioning.
func TestProvision_DeprovisionWaitsForInFlightGoroutine(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		Status:       backend.ProvisionStatusProvisioning,
		ContainerIDs: nil,
	})

	var deprovDoneIDs []string
	var deprovMu sync.Mutex
	deprovRan := make(chan struct{})
	doDeprovision := func(ctx context.Context, leaseUUID string) error {
		state, _ := store.Get(leaseUUID)
		deprovMu.Lock()
		deprovDoneIDs = append(deprovDoneIDs, state.ContainerIDs...)
		deprovMu.Unlock()
		store.remove(leaseUUID)
		close(deprovRan)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:         ctx,
		ProvisionStore:  store,
		DoDeprovisionFn: doDeprovision,
	})
	require.Equal(t, backend.ProvisionStatusProvisioning, actor.State())

	// Simulate an in-flight provision worker via workers + workCancel.
	// onExitProvisioning will call workCancel then waitForWorkers.
	var cancelCalled atomic.Bool
	workerRelease := make(chan struct{})
	actor.workCancel = func() { cancelCalled.Store(true) }
	actor.workers.Add()
	go func() {
		<-workerRelease
		// Simulate the worker's pre-publish step before Done — the real
		// production spawnProvisionWorker pre-publishes new ContainerIDs
		// to provision-state under UpdateFn so a preempting Deprovision
		// can tear them down rather than leaving orphans.
		store.UpdateFn("lease-1", func(p *ProvisionState) {
			p.ContainerIDs = []string{"published-container"}
		})
		actor.workers.Done()
	}()

	// Route the Deprovision message. The actor fires
	// evDeprovisionRequested → Provisioning.OnExit (workCancel +
	// waitForWorkers) → handleDeprovision body runs DoDeprovisionFn.
	reply := make(chan error, 1)
	require.True(t, actor.TryEnqueue(DeprovisionMsg{Ctx: context.Background(), Reply: reply}))

	require.Eventually(t, cancelCalled.Load, 1*time.Second, 5*time.Millisecond,
		"OnExit must call workCancel before waitForWorkers")

	// doDeprovision must NOT have run yet — blocked in waitForWorkers.
	select {
	case <-deprovRan:
		t.Fatal("doDeprovision ran before worker finished — Provisioning.OnExit didn't wait")
	case <-time.After(100 * time.Millisecond):
	}

	// Release the worker → workers.Done → waitForWorkers unblocks →
	// doDeprovision runs and reads the pre-published ContainerIDs.
	close(workerRelease)

	select {
	case err := <-reply:
		require.NoError(t, err, "Deprovision must succeed after worker completes")
	case <-time.After(3 * time.Second):
		t.Fatal("Deprovision did not complete after worker finished")
	}

	deprovMu.Lock()
	defer deprovMu.Unlock()
	require.Contains(t, deprovDoneIDs, "published-container",
		"doDeprovision must see the pre-published ContainerIDs published by the worker")
}

// TestRestartRequested_WritesStatusBeforeAck pins the ENG-230
// handler-publish contract: the actor must write prov.Status=Restarting
// (and apply the request's CallbackURL) BEFORE acking the
// RestartRequestedMsg, so api/handlers.go can publish a "restarting"
// event after Restart() returns and have it reflect already-committed
// state. The Status/CallbackURL writes now live in onEnterRestarting
// (the actor goroutine), not the HTTP prelude.
func TestRestartRequested_WritesStatusBeforeAck(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-a",
		Status:      backend.ProvisionStatusReady,
		CallbackURL: "old-cb",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
	})

	// Block the worker so the lease stays in Restarting while we assert —
	// otherwise the replace worker could flip it to Ready before we read.
	workerRelease := make(chan struct{})
	ack := make(chan error, 1)
	require.True(t, actor.TryEnqueue(RestartRequestedMsg{
		Cancel:      func() {},
		CallbackURL: "new-cb",
		Work: func() ReplaceResult {
			<-workerRelease
			return ReplaceResult{Success: ReplaceSuccessResult{}}
		},
		Ack: ack,
	}))

	select {
	case err := <-ack:
		require.NoError(t, err, "restart from Ready must be accepted by the SM")
	case <-time.After(2 * time.Second):
		t.Fatal("no ack received from handleRestartRequested")
	}

	prov, ok := store.Get("lease-1")
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusRestarting, prov.Status,
		"actor must write Status=Restarting BEFORE acking (handler-publish contract)")
	assert.Equal(t, "new-cb", prov.CallbackURL,
		"actor must apply the message CallbackURL before acking")

	close(workerRelease) // let the worker finish so the actor can quiesce
}

// TestUpdateRequested_WritesStatusBeforeAck is the Update mirror of
// TestRestartRequested_WritesStatusBeforeAck.
func TestUpdateRequested_WritesStatusBeforeAck(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-a",
		Status:      backend.ProvisionStatusReady,
		CallbackURL: "old-cb",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
	})

	workerRelease := make(chan struct{})
	ack := make(chan error, 1)
	require.True(t, actor.TryEnqueue(UpdateRequestedMsg{
		Cancel:      func() {},
		CallbackURL: "new-cb",
		Work: func() ReplaceResult {
			<-workerRelease
			return ReplaceResult{Success: ReplaceSuccessResult{}}
		},
		Ack: ack,
	}))

	select {
	case err := <-ack:
		require.NoError(t, err, "update from Ready must be accepted by the SM")
	case <-time.After(2 * time.Second):
		t.Fatal("no ack received from handleUpdateRequested")
	}

	prov, ok := store.Get("lease-1")
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusUpdating, prov.Status,
		"actor must write Status=Updating BEFORE acking (handler-publish contract)")
	assert.Equal(t, "new-cb", prov.CallbackURL,
		"actor must apply the message CallbackURL before acking")

	close(workerRelease)
}

// routeReplace enqueues the op-appropriate replace request ("restart" or
// "update") onto the actor's inbox. Shared by the ENG-230 §6.3 matrix tests
// so the restart and update paths are exercised by identical logic.
func routeReplace(actor *LeaseActor, op, callbackURL string, cancelFn func(), work func() ReplaceResult, ack chan error) bool {
	if op == "update" {
		return actor.TryEnqueue(UpdateRequestedMsg{Cancel: cancelFn, CallbackURL: callbackURL, Work: work, Ack: ack})
	}
	return actor.TryEnqueue(RestartRequestedMsg{Cancel: cancelFn, CallbackURL: callbackURL, Work: work, Ack: ack})
}

// busyStateFor maps a replace op to the busy SM state its entry action sets.
func busyStateFor(op string) backend.ProvisionStatus {
	if op == "update" {
		return backend.ProvisionStatusUpdating
	}
	return backend.ProvisionStatusRestarting
}

// runConcurrentReplaceRejectedTest is the ENG-230 §6.3(a) matrix case: with
// one replace worker in flight (SM busy), a second same-lease replace request
// that lost the TOCTOU race must (1) be rejected with backend.ErrInvalidState
// (→ HTTP 409, not 500), (2) NOT spawn a second worker, and (3) NOT clobber
// the in-flight worker's cancel func — proven by then routing a real
// Deprovision that drives onExitProvisioning → workCancel and asserting the
// FIRST request's cancel marker fired (func values aren't comparable, so we
// prove identity via distinct side-effecting markers, not pointer equality).
func runConcurrentReplaceRejectedTest(t *testing.T, op string) {
	t.Helper()
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady})

	doDeprovision := func(ctx context.Context, leaseUUID string) error {
		store.remove(leaseUUID)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:         ctx,
		ProvisionStore:  store,
		DoDeprovisionFn: doDeprovision,
	})

	var workerCount atomic.Int64
	var firstCancelCalled, secondCancelCalled atomic.Bool
	worker1Release := make(chan struct{})

	// Request #1 wins: SM → busy, worker #1 spawned and blocks.
	ack1 := make(chan error, 1)
	require.True(t, routeReplace(actor, op, "",
		func() { firstCancelCalled.Store(true) },
		func() ReplaceResult {
			workerCount.Add(1)
			<-worker1Release
			return ReplaceResult{Success: ReplaceSuccessResult{}}
		},
		ack1,
	))
	select {
	case err := <-ack1:
		require.NoError(t, err, "first %s must be accepted", op)
	case <-time.After(2 * time.Second):
		t.Fatalf("no ack from first %s", op)
	}
	require.Equal(t, busyStateFor(op), actor.State())

	// Request #2 loses the race: SM already busy → rejected with 409.
	ack2 := make(chan error, 1)
	require.True(t, routeReplace(actor, op, "",
		func() { secondCancelCalled.Store(true) },
		func() ReplaceResult {
			workerCount.Add(1)
			return ReplaceResult{Success: ReplaceSuccessResult{}}
		},
		ack2,
	))
	select {
	case err := <-ack2:
		require.ErrorIs(t, err, backend.ErrInvalidState,
			"second concurrent %s must be rejected with ErrInvalidState (→409)", op)
	case <-time.After(2 * time.Second):
		t.Fatalf("no ack from second %s", op)
	}

	// Deprovision preempts: onExitProvisioning cancels the in-flight worker
	// (whose workCancel must still be request #1's) then waits for it.
	reply := make(chan error, 1)
	require.True(t, actor.TryEnqueue(DeprovisionMsg{Ctx: context.Background(), Reply: reply}))

	require.Eventually(t, firstCancelCalled.Load, 1*time.Second, 5*time.Millisecond,
		"onExitProvisioning must cancel the FIRST (in-flight) %s worker", op)
	assert.False(t, secondCancelCalled.Load(),
		"the rejected second %s must NOT have clobbered workCancel", op)

	// Release worker #1 so waitForWorkers unblocks and the deprovision completes.
	close(worker1Release)
	select {
	case err := <-reply:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("deprovision did not complete after the worker was released")
	}

	assert.Equal(t, int64(1), workerCount.Load(),
		"exactly one %s worker must have been spawned (the rejected duplicate must not spawn one)", op)
}

func TestSecondConcurrentRestartRejected(t *testing.T) {
	runConcurrentReplaceRejectedTest(t, "restart")
}
func TestSecondConcurrentUpdateRejected(t *testing.T) { runConcurrentReplaceRejectedTest(t, "update") }

// runReplaceLosesToDeprovisionTest is the ENG-230 §6.3(b) matrix case: a
// replace request arriving while the SM is already Deprovisioning (not the
// exact duplicate of §6.3(a)) must also be rejected with ErrInvalidState
// (→409) and spawn NO worker — proving classifyReplaceReject is general
// across busy states, not duplicate-only.
func runReplaceLosesToDeprovisionTest(t *testing.T, op string) {
	t.Helper()
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady})

	// No-spawn: drive the handlers synchronously. The default DoDeprovisionFn
	// is a no-op that leaves the provision in place, so the SM stays in
	// Deprovisioning with terminated=false (the partial-deprovision actor).
	actor := newTestActorNoSpawn(t, "lease-1", testActorOpts{ProvisionStore: store})

	require.NoError(t, actor.handleDeprovision(context.Background()))
	require.Equal(t, backend.ProvisionStatusDeprovisioning, actor.State(),
		"test precondition: SM must be Deprovisioning")

	var workerCount atomic.Int64
	ack := make(chan error, 1)
	work := func() ReplaceResult { workerCount.Add(1); return ReplaceResult{} }
	if op == "update" {
		actor.handleUpdateRequested(UpdateRequestedMsg{Cancel: func() {}, Work: work, Ack: ack})
	} else {
		actor.handleRestartRequested(RestartRequestedMsg{Cancel: func() {}, Work: work, Ack: ack})
	}

	select {
	case err := <-ack:
		require.ErrorIs(t, err, backend.ErrInvalidState,
			"%s against a deprovisioning lease must be rejected with ErrInvalidState (→409)", op)
	default:
		t.Fatalf("handler did not ack the rejected %s", op)
	}
	assert.Equal(t, int64(0), workerCount.Load(),
		"no %s worker may be spawned when the lease is deprovisioning", op)
}

func TestRestartLosesToDeprovision(t *testing.T) { runReplaceLosesToDeprovisionTest(t, "restart") }
func TestUpdateLosesToDeprovision(t *testing.T)  { runReplaceLosesToDeprovisionTest(t, "update") }

// runReplaceFromFailedSucceedsTest is the ENG-230 §6.3 case covering the
// fresh-actor-init-in-Failed path that this change unmasks (the old off-actor
// pre-write hid it by initializing the SM in Restarting). A replace from
// Status=Failed must SUCCEED: SM → Restarting/Updating via the Failed Permits
// (lease_sm.go:236-240), Status (+CallbackURL) written before the ack, exactly
// one worker spawned.
func runReplaceFromFailedSucceedsTest(t *testing.T, op string) {
	t.Helper()
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID:   "lease-1",
		Status:      backend.ProvisionStatusFailed,
		CallbackURL: "old-cb",
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, "lease-1", testActorOpts{StopCtx: ctx, ProvisionStore: store})

	var workerCount atomic.Int64
	workerRelease := make(chan struct{})
	ack := make(chan error, 1)
	require.True(t, routeReplace(actor, op, "new-cb",
		func() {},
		func() ReplaceResult {
			workerCount.Add(1)
			<-workerRelease
			return ReplaceResult{Success: ReplaceSuccessResult{}}
		},
		ack,
	))
	select {
	case err := <-ack:
		require.NoError(t, err, "%s from Failed must be accepted (fresh-actor-init-in-Failed path)", op)
	case <-time.After(2 * time.Second):
		t.Fatalf("no ack from %s-from-Failed", op)
	}

	prov, ok := store.Get("lease-1")
	require.True(t, ok)
	assert.Equal(t, busyStateFor(op), prov.Status, "Status must be written before the ack")
	assert.Equal(t, "new-cb", prov.CallbackURL, "CallbackURL must be applied by the entry action")

	close(workerRelease) // let the worker finish so the actor can quiesce
	require.Eventually(t, func() bool { return workerCount.Load() == 1 }, time.Second, 5*time.Millisecond,
		"exactly one %s worker must be spawned", op)
}

func TestRestartFromFailed_Succeeds(t *testing.T) { runReplaceFromFailedSucceedsTest(t, "restart") }
func TestUpdateFromFailed_Succeeds(t *testing.T)  { runReplaceFromFailedSucceedsTest(t, "update") }

// TestRestoreRequestedMsg_FiresEventAndSpawnsWorker pins the restore
// plumbing (ENG-325 Task 7a): a RestoreRequestedMsg rides the existing
// replace machinery from the Provisioning state. The new restore lease
// was reserved at Status=Provisioning (it was never running), so:
//
//   - evRestoreRequested is permitted Provisioning→Restarting (whereas
//     evRestartRequested fires from Ready/Failed).
//   - The entry action (onEnterRestarting, reused) writes Status=Restarting
//     and applies the CallbackURL before the ack (handler-publish contract).
//   - replaceWasActive ends up false (prior Status was Provisioning, not
//     Ready), so onEnterReadyFromReplaceCompleted Inc's activeProvisions —
//     a restore brings a lease from absent to active.
//
// On a successful ReplaceResult the SM reaches Ready via evReplaceCompleted
// and the gauge increments exactly once.
func TestRestoreRequestedMsg_FiresEventAndSpawnsWorker(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID:   "lease-1",
		Tenant:      "tenant-a",
		Status:      backend.ProvisionStatusProvisioning,
		CallbackURL: "old-cb",
	})

	metrics := &countingMetrics{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		Metrics:        metrics,
	})
	require.Equal(t, backend.ProvisionStatusProvisioning, actor.State(),
		"test precondition: SM must start in Provisioning (restore reserves the new lease there)")

	// Block the worker so we can observe Restarting + CallbackURL before it
	// flips the lease to Ready.
	workerRelease := make(chan struct{})
	ack := make(chan error, 1)
	require.True(t, actor.TryEnqueue(RestoreRequestedMsg{
		Cancel:      func() {},
		CallbackURL: "new-cb",
		Work: func() ReplaceResult {
			<-workerRelease
			return ReplaceResult{Success: ReplaceSuccessResult{ContainerIDs: []string{"c1"}}}
		},
		Ack: ack,
	}))

	select {
	case err := <-ack:
		require.NoError(t, err, "restore from Provisioning must be accepted by the SM")
	case <-time.After(2 * time.Second):
		t.Fatal("no ack received from handleRestoreRequested")
	}

	prov, ok := store.Get("lease-1")
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusRestarting, prov.Status,
		"actor must write Status=Restarting BEFORE acking (handler-publish contract)")
	assert.Equal(t, "new-cb", prov.CallbackURL,
		"actor must apply the message CallbackURL before acking")
	assert.False(t, actor.replaceWasActive,
		"replaceWasActive must be false for a Provisioning→Restarting restore (lease was absent, not active)")

	// Release the worker → evReplaceCompleted → Ready, gauge Inc once.
	close(workerRelease)
	require.Eventually(t, func() bool {
		return actor.State() == backend.ProvisionStatusReady
	}, 2*time.Second, 10*time.Millisecond,
		"SM must reach Ready via replaceCompleted after the restore worker succeeds")

	final, ok := store.Get("lease-1")
	require.True(t, ok)
	assert.Equal(t, []string{"c1"}, final.ContainerIDs,
		"ContainerIDs must reflect the restore worker's success result")
	assert.Equal(t, int64(1), metrics.activeProvisionsInc.Load(),
		"a restore that brings a lease from absent to active must Inc activeProvisions exactly once")
	assert.Equal(t, int64(0), metrics.activeProvisionsDec.Load(),
		"a successful restore must not Dec activeProvisions")
}

// TestRestoreRequested_RejectedFromBadState mirrors the restart
// loses-to-deprovision case: evRestoreRequested is NOT permitted from
// Deprovisioning, so a restore against a deprovisioning lease must be
// rejected via classifyReplaceReject (ErrInvalidState → 409) and spawn
// no worker. (Restore is only permitted from Provisioning.)
func TestRestoreRequested_RejectedFromBadState(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady})

	// No-spawn: drive the handlers synchronously. The default DoDeprovisionFn
	// is a no-op that leaves the provision in place, so the SM stays in
	// Deprovisioning with terminated=false (the partial-deprovision actor).
	actor := newTestActorNoSpawn(t, "lease-1", testActorOpts{ProvisionStore: store})

	require.NoError(t, actor.handleDeprovision(context.Background()))
	require.Equal(t, backend.ProvisionStatusDeprovisioning, actor.State(),
		"test precondition: SM must be Deprovisioning")

	var workerCount atomic.Int64
	ack := make(chan error, 1)
	actor.handleRestoreRequested(RestoreRequestedMsg{
		Cancel: func() {},
		Work:   func() ReplaceResult { workerCount.Add(1); return ReplaceResult{} },
		Ack:    ack,
	})

	select {
	case err := <-ack:
		require.ErrorIs(t, err, backend.ErrInvalidState,
			"restore against a deprovisioning lease must be rejected with ErrInvalidState (→409)")
	default:
		t.Fatal("handler did not ack the rejected restore")
	}
	assert.Equal(t, int64(0), workerCount.Load(),
		"no restore worker may be spawned when the lease is deprovisioning")
}

// TestRestoreRequestedMsg_RejectsWhenTerminated mirrors the Restart arm
// of TestTerminatedActor_RejectsCallerFacingRequests: a restore routed
// to a terminated actor (post-handleDeprovision, pre-registry-removal)
// must reject with errActorTerminated and spawn no worker.
func TestRestoreRequestedMsg_RejectsWhenTerminated(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID: "lease-1",
		Status:    backend.ProvisionStatusDeprovisioning,
	})
	actor := newTestActorNoSpawn(t, "lease-1", testActorOpts{ProvisionStore: store})
	actor.terminated = true

	var workerSpawned atomic.Bool
	msg := RestoreRequestedMsg{
		Cancel: func() {},
		Work: func() ReplaceResult {
			workerSpawned.Store(true)
			return ReplaceResult{}
		},
		Ack: make(chan error, 1),
	}
	actor.handleRestoreRequested(msg)

	select {
	case err := <-msg.Ack:
		assert.ErrorIs(t, err, errActorTerminated,
			"terminated actor must reject Restore with errActorTerminated")
	case <-time.After(time.Second):
		t.Fatal("ack channel never received a value")
	}
	time.Sleep(20 * time.Millisecond)
	assert.False(t, workerSpawned.Load(),
		"terminated actor must NOT spawn a replace worker for Restore")
}

// TestSpawnReplaceWorker_RestorePanicRecovery exercises a panic in the
// restore Work closure end-to-end through handleRestoreRequested: the
// replace worker's recover must drive the SM to Failed (via
// evReplaceFailed) and ack the restore without crashing fred. The
// restore reuses spawnReplaceWorker, so this is the restore-shaped
// mirror of TestSpawnReplaceWorker_PanicRecovery.
func TestSpawnReplaceWorker_RestorePanicRecovery(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID: "lease-1",
		Tenant:    "tenant-a",
		Status:    backend.ProvisionStatusProvisioning,
	})

	metrics := &countingMetrics{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		Metrics:        metrics,
	})

	panicsBefore := metrics.workerPanic.Load()

	ack := make(chan error, 1)
	require.True(t, actor.TryEnqueue(RestoreRequestedMsg{
		Cancel: func() {},
		Work: func() ReplaceResult {
			panic("synthetic restore panic")
		},
		Ack: ack,
	}))

	select {
	case err := <-ack:
		require.NoError(t, err, "restore from Provisioning must be accepted before the worker runs")
	case <-time.After(2 * time.Second):
		t.Fatal("no ack received from handleRestoreRequested")
	}

	require.Eventually(t, func() bool {
		return actor.State() == backend.ProvisionStatusFailed
	}, 2*time.Second, 10*time.Millisecond,
		"SM must transition to Failed after restore worker panic recovery")

	assert.Equal(t, panicsBefore+1, metrics.workerPanic.Load(),
		"WorkerPanic metric must increment by 1 after restore worker panic")
}

// countingMetrics implements SMMetrics with atomic counters so tests
// can assert on metric calls without needing the docker prometheus
// adapter. Useful for tests that verify panic-recovery hooks or other
// metric-emission contracts where the precise prometheus
// representation is exercised in a different test (in package docker
// against the dockerSMMetrics adapter).
type countingMetrics struct {
	smTransition         atomic.Int64
	actorCreated         atomic.Int64
	workerPanic          atomic.Int64
	actorPanic           atomic.Int64
	terminalEventDropped atomic.Int64
	activeProvisionsInc  atomic.Int64
	activeProvisionsDec  atomic.Int64
}

func (m *countingMetrics) SMTransition(_, _, _ string)   { m.smTransition.Add(1) }
func (m *countingMetrics) ActorCreated()                 { m.actorCreated.Add(1) }
func (m *countingMetrics) WorkerPanic(_ string)          { m.workerPanic.Add(1) }
func (m *countingMetrics) ActorPanic()                   { m.actorPanic.Add(1) }
func (m *countingMetrics) TerminalEventDropped(_ string) { m.terminalEventDropped.Add(1) }
func (m *countingMetrics) ActiveProvisionsInc()          { m.activeProvisionsInc.Add(1) }
func (m *countingMetrics) ActiveProvisionsDec()          { m.activeProvisionsDec.Add(1) }

var _ SMMetrics = (*countingMetrics)(nil)

// Compile-time guards: ensure the mocks implement their interfaces. If
// SMMetrics changes (e.g., a new method is added per BC-1 adding
// ActorPanic), these guards force the mocks to be updated.
var _ LeaseProvisionStore = (*mockProvisionStore)(nil)
var _ InstanceInspector = (*mockInstanceInspector)(nil)
var _ DiagnosticsGatherer = (*mockDiagnosticsGatherer)(nil)
var _ SMMetrics = mockSMMetrics{}

// Re-export package-level sync to make sure the import is used in case
// future tests need it; otherwise the linter would flag the unused
// import that newTestActor's helpers may or may not exercise.
var _ = sync.Mutex{}

// TestProvisionErrored_AuthorsReasonMessage asserts that the
// Provisioning→Failed entry action (onEnterFailedFromProvision) authors
// the curated (Reason, Message) pair alongside the verbose LastError
// (ENG-508). Reason is the machine-readable category code the caller
// supplied; Message is the on-chain-safe CallbackErr; LastError is the
// operator-only verbose diagnostic — the three must be independent.
func TestProvisionErrored_AuthorsReasonMessage(t *testing.T) {
	a := newTestActorNoSpawn(t, "lease-1", testActorOpts{})
	FireProvisionRequestedForTest(a)
	FireProvisionErroredForTest(a, "image pull failed", backend.ReasonImagePullFailed, "pull /data/fred/... exit 1", nil)

	got, ok := a.cfg.ProvisionStore.Get("lease-1")
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusFailed, got.Status)
	assert.Equal(t, backend.ReasonImagePullFailed, got.Reason,
		"Reason must be the authored category code, not derived from LastError")
	assert.Equal(t, "image pull failed", got.Message,
		"Message must equal the on-chain CallbackErr")
	assert.Equal(t, "pull /data/fred/... exit 1", got.LastError,
		"verbose LastError must be untouched (operator-only)")
}

// TestReplaceFailed_AuthorsReasonMessage asserts that the
// Restarting→Failed entry action (onEnterFailedFromReplace) authors the
// curated (Reason, Message) pair alongside the verbose LastError. Drives
// the replace-failed path white-box: the store is seeded in Restarting so
// readProvisionStatus initializes the SM there, then handleReplaceFailed
// fires evReplaceFailed → Failed with a composed CallbackErr.
func TestReplaceFailed_AuthorsReasonMessage(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID: "lease-1",
		Status:    backend.ProvisionStatusRestarting,
	})
	a := newTestActorNoSpawn(t, "lease-1", testActorOpts{ProvisionStore: store})

	a.handleReplaceFailed(ReplaceFailureInfo{
		Operation:   "restart",
		Reason:      backend.ReasonRestartFailed,
		CallbackErr: "restart failed; rolled back to previous version",
		LastError:   "compose up exit 1: /data/fred/... permission denied",
	})

	got, ok := store.Get("lease-1")
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusFailed, got.Status)
	assert.Equal(t, backend.ReasonRestartFailed, got.Reason,
		"Reason must be the authored category code even when CallbackErr is composed")
	assert.Equal(t, "restart failed; rolled back to previous version", got.Message,
		"Message must equal the composed on-chain CallbackErr")
	assert.Equal(t, "compose up exit 1: /data/fred/... permission denied", got.LastError,
		"verbose LastError must be untouched (operator-only)")
}
