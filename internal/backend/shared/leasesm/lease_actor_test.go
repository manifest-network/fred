package leasesm

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// Tests in this file exercise the LeaseActor's internal semantics
// (SM transitions, worker spawning, terminal-event drain, panic recovery,
// sendTerminal mechanics). They live in `package leasesm` (internal
// tests) per Deviation #1 — they use unexported types/fields/methods
// directly because constructing test scenarios at this layer is much
// cleaner with same-package access than going through the docker
// substrate's exported surface.
//
// Backend-routing integration tests (b.routeToLease, b.routeToLeaseBlocking,
// b.Deprovision, etc.) stay in docker/lease_actor_test.go where they
// exercise the substrate's actor registry + HTTP callback machinery.

// TestLeaseSM_ProjectionAbsenceRefusesBeforeTransition pins the ordering
// required by qmuntal/stateless: the library changes its internal state before
// invoking OnEntry and does not roll that change back when OnEntry returns an
// error. Every entry reduction therefore proves its actor-owned projection
// exists before Fire. If recovery has removed the projection, the transition
// is refused while the FSM remains at its source state.
func TestLeaseSM_ProjectionAbsenceRefusesBeforeTransition(t *testing.T) {
	runtime := newTestRuntimeGenerationProof(t, testActorLeaseUUID)
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{
		LeaseUUID: testActorLeaseUUID,
		Status:    backend.ProvisionStatusReady,
	})
	actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{
		ProvisionStore: store,
	})
	store.remove(testActorLeaseUUID)

	err := actor.sm.cohortDiverged(context.Background(), runtime)
	require.Error(t, err)
	assert.Equal(t, backend.ProvisionStatusReady, actor.sm.State())
}

// TestSendTerminal_RejectsAfterExitingClosed pins the fix for the
// narrow window where a late-worker sendTerminal could queue into an
// inbox no one would drain.
//
// Sequence: the actor's retirement order is
//
//	stopAdmission → initial drain → waitForWorkers → closeTerminalAdmission → final drain → removeFromRegistry → close(done)
//
// Once closeTerminalAdmission runs, a late worker must not enqueue after the
// final drain. The same mutex orders closing against the send, so the message
// is either visible to the final drain or definitively refused.
//
// Test drives the contract: close terminal admission, then call sendTerminal,
// assert false. Closing is idempotent, so the production exit defer may repeat
// it safely.
func TestSendTerminal_RejectsAfterExitingClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, "lease-1", testActorOpts{StopCtx: ctx})

	// Actor is still running; a.done is NOT closed. Before exiting is
	// closed, sendTerminal accepts.
	require.True(t, actor.sendTerminal(provisionCompletedMsg{}),
		"baseline: sendTerminal should succeed on a live actor")

	// Simulate the retirement step that closes this channel just before the final
	// drain. After this, sendTerminal must reject.
	actor.closeTerminalAdmission()

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
	leaseUUID := testActorLeaseUUID
	runtime := newTestRuntimeGenerationProof(t, leaseUUID)

	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID:            leaseUUID,
		Tenant:               "tenant-a",
		ContainerIDs:         []string{"c1"},
		Status:               backend.ProvisionStatusReady,
		ActiveReleaseVersion: runtime.Version(),
		ActiveOperationID:    runtime.OperationID(),
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
	actor := newTestActor(t, leaseUUID, testActorOpts{
		StopCtx:        ctx,
		Inspector:      inspector,
		ProvisionStore: store,
		Metrics:        metrics,
	})

	// First send: panics inside the SM guard.
	first, err := NewContainerDiedObservation("c1", runtime)
	require.NoError(t, err)
	require.True(t, actor.TryEnqueueObservation(first))
	require.Eventually(t, func() bool {
		return metrics.actorPanic.Load() > actorPanicsBefore
	}, 2*time.Second, 10*time.Millisecond,
		"ActorPanic must be invoked when a handler panics")

	// Second send: must be processed — the actor survived the panic.
	second, completion, err := NewTrackedContainerDiedObservation("c1", runtime)
	require.NoError(t, err)
	require.True(t, actor.TryEnqueueObservation(second))
	select {
	case <-completion.Done():
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
	leaseUUID := testActorLeaseUUID
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID:    leaseUUID,
		Tenant:       "tenant-a",
		Status:       backend.ProvisionStatusProvisioning,
		ContainerIDs: nil,
	})

	ctx, cancel := context.WithCancel(context.Background())
	actor := newTestActor(t, leaseUUID, testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
	})
	require.NoError(t, actor.sm.requestProvision(context.Background()),
		"test must model an admitted worker-owning Provisioning state, not a reservation")
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
	_, result := testProvisionSuccess(t, leaseUUID, ProvisionSuccessProjection{
		ContainerIDs: []string{"c1"}, ServiceContainers: map[string][]string{"app": {"c1"}},
	})
	go func() {
		ok := actor.sendTerminal(provisionCompletedMsg{
			result: result,
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

	prov, ok := store.Get(leaseUUID)
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
// the actor's retirement drain had already run, dropping the terminal event.
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

func TestLeaseActor_QuiescenceClaimSpansWorkerTerminalHandoff(t *testing.T) {
	leaseUUID := testActorLeaseUUID
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID: leaseUUID,
		Tenant:    "tenant-a",
		Status:    backend.ProvisionStatusFailed,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerStarted := make(chan struct{})
	workerRelease := make(chan struct{})
	ack := make(chan error, 1)
	operation, provisionSuccess := testProvisionSuccess(t, leaseUUID, ProvisionSuccessProjection{ContainerIDs: []string{"c1"}})
	require.True(t, provisionSuccess.operationRelease.MatchesIntent(operation),
		"test fixture must preserve exact operation lineage")
	actor := newTestActor(t, leaseUUID, testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		ProvisionWorkFn: func(context.Context, shared.OperationIntentClaim) ProvisionWorkOutcome {
			close(workerStarted)
			<-workerRelease
			return provisionWorkSuccess{result: provisionSuccess}
		},
	})
	require.True(t, actor.tryEnqueue(provisionRequestedMsg{
		Ctx: context.Background(), Ack: ack, Operation: operation,
	}))
	require.NoError(t, <-ack)
	<-workerStarted
	assert.Nil(t, actor.TryClaimQuiescence(),
		"a running worker must prevent a recovery quiescence claim")

	close(workerRelease)
	var claim *QuiescenceClaim
	require.Eventually(t, func() bool {
		claim = actor.TryClaimQuiescence()
		return claim != nil
	}, time.Second, time.Millisecond,
		"quiescence becomes claimable only after the terminal message is handled")
	claim.Release()
	claim.Release()
	provision, exists := store.Get(leaseUUID)
	require.True(t, exists)
	assert.Equal(t, backend.ProvisionStatusReady, provision.Status)
}

func TestLeaseActor_QuiescenceClaimPinsAdmissionAndRetirement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	actor := newTestActor(t, "lease-1", testActorOpts{StopCtx: ctx})
	claim := actor.TryClaimQuiescence()
	require.NotNil(t, claim)
	assert.False(t, actor.tryEnqueue(containerDiedMsg{ContainerID: "c1"}),
		"routing must remain non-blocking and refuse while recovery owns the actor")

	cancel()
	select {
	case <-actor.Done():
		t.Fatal("the actor retired while its admission gate was pinned")
	case <-time.After(50 * time.Millisecond):
	}
	claim.Release()
	claim.Release()
	select {
	case <-actor.Done():
	case <-time.After(time.Second):
		t.Fatal("the actor did not retire after quiescence was released")
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
	actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{Diag: diag})
	exitCode := 1
	info := &InstanceState{Phase: PhaseExited, ExitCode: &exitCode}

	// Build a ctx that's already DeadlineExceeded (simulating the 30s
	// diag timeout having elapsed without any Failing.OnExit cancel).
	diagCtx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)
	require.ErrorIs(t, diagCtx.Err(), context.DeadlineExceeded,
		"test precondition: diagCtx must be DeadlineExceeded, not Canceled")

	actor.gatherDiagAsync(diagCtx, "c1", info, newTestRuntimeGenerationProof(t, testActorLeaseUUID))

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
	actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{Diag: diag})
	exitCode := 1
	info := &InstanceState{Phase: PhaseExited, ExitCode: &exitCode}

	diagCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, diagCtx.Err(), context.Canceled,
		"test precondition: diagCtx must be Canceled")

	actor.gatherDiagAsync(diagCtx, "c1", info, newTestRuntimeGenerationProof(t, testActorLeaseUUID))

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
	doDeprovision := func(ctx context.Context, scope ActorCloseScope) error {
		leaseUUID := scope.LeaseUUID()
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
	require.True(t, actor.tryEnqueue(deprovisionMsg{Ctx: context.Background(), Reply: reply}))

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

// Restore uses the same replace worker but performs its durable source
// finalization/rollback in Work's defer. A preempting Deprovision holds the
// backend command fence while the actor's Restarting.OnExit waits for workers;
// it must not enter substrate close until that terminal defer is fully done.
// This transitive ordering is why the restore worker must not try to re-lock the
// command fence itself (doing so would deadlock against Deprovision's wait).
func TestLeaseActor_RestoreDeprovisionWaitsForTerminalDefer(t *testing.T) {
	leaseUUID := testActorLeaseUUID
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID: leaseUUID,
		Tenant:    "tenant-a",
		Status:    backend.ProvisionStatusProvisioning,
	})

	terminalDeferEntered := make(chan struct{})
	allowTerminalDefer := make(chan struct{})
	deprovisionRan := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	restoreWorkMayReturn := make(chan struct{})
	operation, restoreSuccess := testRestoreSuccess(t, leaseUUID, ReplaceSuccessProjection{ContainerIDs: []string{"restored-container"}})
	actor := newTestActor(t, leaseUUID, testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		RestoreWorkFn: func(context.Context, shared.OperationIntentClaim) (outcome ReplaceWorkOutcome) {
			defer func() {
				close(terminalDeferEntered)
				<-allowTerminalDefer
			}()
			<-restoreWorkMayReturn
			return replaceWorkTerminal{result: restoreSuccess}
		},
		DoDeprovisionFn: func(context.Context, ActorCloseScope) error {
			store.remove(leaseUUID)
			close(deprovisionRan)
			return nil
		},
	})

	restoreAck := make(chan error, 1)
	require.True(t, actor.tryEnqueue(restoreRequestedMsg{
		Ctx: context.Background(), Ack: restoreAck, Operation: operation,
	}))
	require.NoError(t, <-restoreAck)

	deprovisionReply := make(chan error, 1)
	require.True(t, actor.tryEnqueue(deprovisionMsg{
		Ctx: context.Background(), Reply: deprovisionReply,
	}))
	close(restoreWorkMayReturn)
	select {
	case <-terminalDeferEntered:
	case <-time.After(time.Second):
		t.Fatal("restore worker did not enter its terminal defer")
	}
	select {
	case <-deprovisionRan:
		t.Fatal("deprovision entered substrate cleanup before restore terminal settlement finished")
	case <-time.After(100 * time.Millisecond):
	}

	close(allowTerminalDefer)
	select {
	case err := <-deprovisionReply:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("deprovision did not resume after restore terminal settlement")
	}
}

// TestLeaseActor_DiagGathered_ShutdownDrain exercises the
// drain-on-shutdown path for a lease in the Failing state: stopCtx
// fires with a diagGatheredMsg freshly queued in the inbox, and the
// actor must process it (transitioning Failing→Failed) before
// exiting. Pre-fix: the actor would exit on stopCtx.Done and the diag
// message would be discarded by retirement without handling, leaving
// Status stuck at Failing.
//
// Asserts on the SM-observable outcome (Status flip) rather than HTTP
// callback delivery — the substrate's callback sender during shutdown
// is a separate concern tested elsewhere.
func TestLeaseActor_DiagGathered_ShutdownDrain(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID:            "lease-1",
		Tenant:               "tenant-a",
		ContainerIDs:         []string{"c1"},
		Status:               backend.ProvisionStatusFailing,
		CallbackURL:          "https://fred.example/callbacks/provision?operation_id=exact",
		LifecycleCallbackURL: "https://fred.example/callbacks/provision",
	})

	ctx, cancel := context.WithCancel(context.Background())
	var lifecycleFailure bool
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		SendOperationCallbackFn: func(string, string, backend.CallbackStatus, string) {
			t.Fatal("container death must not reuse the operation settlement callback path")
		},
		SendLifecycleFailureFn: func(shared.RuntimeGenerationProof, string) {
			lifecycleFailure = true
		},
	})
	require.Equal(t, backend.ProvisionStatusFailing, actor.State())

	// Queue a diagGatheredMsg, then fire shutdown. Whatever the inbox
	// select picks next (msg or stopCtx.Done), the retirement drain must process
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
	assert.True(t, lifecycleFailure, "autonomous failure must be published")
}

// TestSpawnProvisionWorker_PanicRecovery pins the invariant that a
// panic in the provision worker does NOT crash fred: the recover logs
// the panic with stack, bumps WorkerPanic("provision"), preserves the
// nonterminal Started operation for recovery, and lets the actor keep serving
// other messages. Without
// this recovery an unrecovered panic would take down the entire fred
// binary (Go's panic semantics).
func TestSpawnProvisionWorker_PanicRecovery(t *testing.T) {
	leaseUUID := testActorLeaseUUID
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID: leaseUUID,
		Tenant:    "tenant-a",
		Status:    backend.ProvisionStatusProvisioning,
	})

	metrics := &countingMetrics{}
	operation := newTestOperationFixture(t, leaseUUID, shared.OperationIntentProvision).claim
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, leaseUUID, testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		Metrics:        metrics,
		ProvisionWorkFn: func(context.Context, shared.OperationIntentClaim) ProvisionWorkOutcome {
			panic("synthetic provision panic")
		},
	})

	panicsBefore := metrics.workerPanic.Load()

	// Inject a worker that panics instead of doing real work. The
	// Recovery must catch the panic and publish only an explicit ambiguous
	// handoff. A panic cannot prove substrate absence, so Failed is forbidden.
	actor.spawnProvisionWorker(context.Background(), operation)

	// The worker drains, but the Started operation remains nonterminal.
	require.Eventually(t, func() bool {
		return metrics.workerPanic.Load() == panicsBefore+1
	}, 2*time.Second, 10*time.Millisecond,
		"worker panic must be observed")
	assert.Equal(t, backend.ProvisionStatusProvisioning, actor.State())

	panicsAfter := metrics.workerPanic.Load()
	assert.Equal(t, panicsBefore+1, panicsAfter,
		"WorkerPanic metric must increment by 1 after provision worker panic")

	// Actor is still alive and responsive — a caller command is acknowledged.
	command, reply, err := NewDeprovisionCommand(context.Background())
	require.NoError(t, err)
	require.True(t, actor.TryEnqueueCommand(command))
	require.NoError(t, reply.Wait(context.Background()))
}

// TestSpawnReplaceWorker_PanicRecovery: same invariant for the
// restart/update worker path.
func TestSpawnReplaceWorker_PanicRecovery(t *testing.T) {
	leaseUUID := testActorLeaseUUID
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID: leaseUUID,
		Tenant:    "tenant-a",
		Status:    backend.ProvisionStatusRestarting,
	})

	metrics := &countingMetrics{}
	operation := newTestOperationFixture(t, leaseUUID, shared.OperationIntentRestore).claim
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, leaseUUID, testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		Metrics:        metrics,
		RestoreWorkFn: func(context.Context, shared.OperationIntentClaim) ReplaceWorkOutcome {
			panic("synthetic replace panic")
		},
	})

	panicsBefore := metrics.workerPanic.Load()

	actor.spawnReplaceWorker(context.Background(), shared.MaintenanceReleaseClaim{}, operation)

	require.Eventually(t, func() bool {
		return metrics.workerPanic.Load() == panicsBefore+1
	}, 2*time.Second, 10*time.Millisecond,
		"replace panic must be observed")
	assert.Equal(t, backend.ProvisionStatusRestarting, actor.State())

	panicsAfter := metrics.workerPanic.Load()
	assert.Equal(t, panicsBefore+1, panicsAfter,
		"WorkerPanic metric must increment by 1 after replace worker panic")

	command, reply, err := NewDeprovisionCommand(context.Background())
	require.NoError(t, err)
	require.True(t, actor.TryEnqueueCommand(command))
	require.NoError(t, reply.Wait(context.Background()))
}

// TestGatherDiagAsync_PanicRecovery: the diag worker (spawned from
// Failing.OnEntry) must also recover. A panic in the diag gatherer
// used to crash fred; now it bumps the metric and drives Failing→Failed
// with empty diag so the lease isn't wedged in Failing.
func TestGatherDiagAsync_PanicRecovery(t *testing.T) {
	leaseUUID := testActorLeaseUUID
	runtime := newTestRuntimeGenerationProof(t, leaseUUID)
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID:            leaseUUID,
		Tenant:               "tenant-a",
		Status:               backend.ProvisionStatusFailing,
		ContainerIDs:         []string{"c1"},
		ActiveReleaseVersion: runtime.Version(),
		ActiveOperationID:    runtime.OperationID(),
	})

	diag := &mockDiagnosticsGatherer{
		GatherDiagnosticsFn: func(ctx context.Context, _ string, _ *InstanceState) string {
			panic("synthetic diag panic")
		},
	}

	metrics := &countingMetrics{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, leaseUUID, testActorOpts{
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
		actor.gatherDiagAsync(
			gatherCtx, "c1", &InstanceState{Phase: PhaseExited, ExitCode: &exitCode},
			runtime,
		)
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
	var provisionWorkerSpawned atomic.Bool
	var maintenanceWorkerSpawned atomic.Bool
	actor := newTestActorNoSpawn(t, "lease-1", testActorOpts{
		ProvisionStore: store,
		ProvisionWorkFn: func(context.Context, shared.OperationIntentClaim) ProvisionWorkOutcome {
			provisionWorkerSpawned.Store(true)
			return nil
		},
		MaintenanceWorkFn: func(context.Context, shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
			maintenanceWorkerSpawned.Store(true)
			return nil
		},
	})
	actor.terminated = true // simulate post-handleDeprovision state

	t.Run("Provision", func(t *testing.T) {
		msg := provisionRequestedMsg{
			Ctx: context.Background(),
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
		assert.False(t, provisionWorkerSpawned.Load(),
			"terminated actor must NOT spawn a provision worker")
	})

	t.Run("Restart", func(t *testing.T) {
		msg := restartRequestedMsg{
			Ctx: context.Background(),
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
		assert.False(t, maintenanceWorkerSpawned.Load(),
			"terminated actor must NOT spawn a replace worker for Restart")
	})

	t.Run("Update", func(t *testing.T) {
		msg := updateRequestedMsg{
			Ctx: context.Background(),
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
		assert.False(t, maintenanceWorkerSpawned.Load(),
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
	leaseUUID := testActorLeaseUUID
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID:    leaseUUID,
		Tenant:       "tenant-a",
		Status:       backend.ProvisionStatusDeprovisioning,
		ContainerIDs: []string{"c1"},
	})

	// No-spawn so we drive handleProvisionRequested synchronously
	// without racing the run loop's draining of the message we're
	// about to construct.
	operation := newTestOperationFixture(t, leaseUUID, shared.OperationIntentProvision).claim
	actor := newTestActorNoSpawn(t, leaseUUID, testActorOpts{ProvisionStore: store})
	// newLeaseSM initializes the SM from prov.Status, so SM is in
	// Deprovisioning. terminated stays false because handleDeprovision
	// was never run (simulating a partial-deprov scenario where the
	// actor's run loop is alive with SM=Deprovisioning).
	require.Equal(t, backend.ProvisionStatusDeprovisioning, actor.State(),
		"test precondition: SM must be in Deprovisioning")

	var previousCancelCalled, rejectedCancelCalled atomic.Bool
	actor.workCancel = func() { previousCancelCalled.Store(true) }
	ack := make(chan error, 1)
	msg := provisionRequestedMsg{
		Ctx: context.Background(), Ack: ack, Operation: operation,
	}
	actor.handleProvisionRequested(msg)

	select {
	case err := <-ack:
		require.Error(t, err,
			"handleProvisionRequested must reject when SM silently Ignores the event (bug_005); got ack success")
	default:
		t.Fatal("handler did not send on ack channel")
	}
	require.NotNil(t, actor.workCancel)
	actor.workCancel()
	assert.True(t, previousCancelCalled.Load(),
		"a rejected provision must preserve the existing worker's cancel capability")
	assert.False(t, rejectedCancelCalled.Load(),
		"a rejected provision must not install its own cancel capability")
}

func TestHandleProvisionRequested_AcceptsReservedLeaseExactlyOnce(t *testing.T) {
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{
		LeaseUUID: testActorLeaseUUID,
		Status:    backend.ProvisionStatusProvisioning,
	})
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	operation, failure := newTestOperationFailure(t, testActorLeaseUUID, shared.OperationIntentProvision)
	actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{
		ProvisionStore: store,
		ProvisionWorkFn: func(context.Context, shared.OperationIntentClaim) ProvisionWorkOutcome {
			if calls.Add(1) == 1 {
				close(started)
			}
			<-release
			outcome, err := NewProvisionWorkFailure(
				errors.New("stopped"), ErrMsgInternal, backend.ReasonInternal, nil, failure,
			)
			require.NoError(t, err)
			return outcome
		},
	})
	firstAck := make(chan error, 1)
	actor.handleProvisionRequested(provisionRequestedMsg{
		Ctx: context.Background(), Ack: firstAck, Operation: operation,
	})
	require.NoError(t, <-firstAck)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("accepted provision worker did not start")
	}

	secondAck := make(chan error, 1)
	actor.handleProvisionRequested(provisionRequestedMsg{
		Ctx: context.Background(), Ack: secondAck, Operation: operation,
	})
	require.Error(t, <-secondAck)
	assert.Equal(t, int64(1), calls.Load(), "running Provisioning must not accept a duplicate worker")
	close(release)
}

func TestRestartRedeliveryFromDurableStartedStateCannotSpawnSecondWorker(t *testing.T) {
	const leaseUUID = "76767676-7676-4676-8676-767676767676"
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID: leaseUUID,
		Status:    backend.ProvisionStatusRestarting,
	})
	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	var workerCalls atomic.Int32
	actor := newTestActor(t, leaseUUID, testActorOpts{
		StopCtx:        stopCtx,
		ProvisionStore: store,
		MaintenanceWorkFn: func(context.Context, shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
			workerCalls.Add(1)
			return nil
		},
	})

	claim := newTestMaintenanceClaim(t, leaseUUID, shared.MaintenanceIntentRestart)
	target := testMaintenanceTarget(t, claim)
	command, reply, err := NewRestartCommand(
		t.Context(),
		target,
	)
	require.NoError(t, err)
	require.True(t, actor.TryEnqueueCommand(command))
	require.ErrorIs(t, reply.Wait(t.Context()), backend.ErrInvalidState)
	assert.Equal(t, int32(0), workerCalls.Load(),
		"redelivery after durable Started must not duplicate physical restart work")
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
	doDeprovision := func(ctx context.Context, scope ActorCloseScope) error {
		leaseUUID := scope.LeaseUUID()
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
	require.NoError(t, actor.sm.requestProvision(context.Background()),
		"test must model an admitted worker-owning Provisioning state, not a reservation")
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
	require.True(t, actor.tryEnqueue(deprovisionMsg{Ctx: context.Background(), Reply: reply}))

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
// and apply the caller's prevalidated callback pair BEFORE acking the
// restartRequestedMsg, so api/handlers.go can publish a "restarting"
// event after Restart() returns and have it reflect already-committed
// state. The Status/CallbackURL writes now live in onEnterRestarting
// (the actor goroutine), not the HTTP prelude.
func TestRestartRequested_WritesStatusBeforeAck(t *testing.T) {
	claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentRestart)
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{
		LeaseUUID:            testActorLeaseUUID,
		Tenant:               "tenant-a",
		Status:               backend.ProvisionStatusReady,
		CallbackURL:          "https://old.example/callbacks/provision?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8",
		LifecycleCallbackURL: "https://old.example/callbacks/provision?lifecycle_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerRelease := make(chan struct{})
	success := testMaintenanceSuccess(t, claim, ReplaceSuccessProjection{})
	actor := newTestActor(t, testActorLeaseUUID, testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		MaintenanceWorkFn: func(context.Context, shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
			<-workerRelease
			return replaceWorkTerminal{result: success}
		},
	})

	// Block the worker so the lease stays in Restarting while we assert —
	// otherwise the replace worker could flip it to Ready before we read.
	ack := make(chan error, 1)
	require.True(t, actor.tryEnqueue(restartRequestedMsg{
		Ctx:                  context.Background(),
		CallbackURL:          claim.CallbackURL(),
		LifecycleCallbackURL: claim.LifecycleCallbackURL(),
		Maintenance:          claim,
		Target:               testMaintenanceTarget(t, claim),
		Ack:                  ack,
	}))

	select {
	case err := <-ack:
		require.NoError(t, err, "restart from Ready must be accepted by the SM")
	case <-time.After(2 * time.Second):
		t.Fatal("no ack received from handleRestartRequested")
	}

	prov, ok := store.Get(testActorLeaseUUID)
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusRestarting, prov.Status,
		"actor must write Status=Restarting BEFORE acking (handler-publish contract)")
	assert.Equal(t, "https://old.example/callbacks/provision?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8", prov.CallbackURL,
		"accepted maintenance must not replace committed runtime authority before success")
	assert.Equal(t, "https://old.example/callbacks/provision?lifecycle_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8", prov.LifecycleCallbackURL,
		"accepted maintenance must keep the committed lifecycle route pending")
	assert.Equal(t, claim.CallbackURL(), actor.pendingReplaceCallbackURL)
	assert.Equal(t, claim.LifecycleCallbackURL(), actor.pendingReplaceLifecycleCallbackURL)

	close(workerRelease) // let the worker finish so the actor can quiesce
	require.Eventually(t, func() bool {
		committed, exists := store.Get(testActorLeaseUUID)
		return exists && committed.Status == backend.ProvisionStatusReady &&
			committed.CallbackURL == claim.CallbackURL() &&
			committed.LifecycleCallbackURL == claim.LifecycleCallbackURL()
	}, time.Second, 5*time.Millisecond)
}

// TestUpdateRequested_WritesStatusBeforeAck is the Update mirror of
// TestRestartRequested_WritesStatusBeforeAck.
func TestUpdateRequested_WritesStatusBeforeAck(t *testing.T) {
	claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentUpdate)
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{
		LeaseUUID:            testActorLeaseUUID,
		Tenant:               "tenant-a",
		Status:               backend.ProvisionStatusReady,
		CallbackURL:          "https://old.example/callbacks/provision?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8",
		LifecycleCallbackURL: "https://old.example/callbacks/provision?lifecycle_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerRelease := make(chan struct{})
	success := testMaintenanceSuccess(t, claim, ReplaceSuccessProjection{})
	actor := newTestActor(t, testActorLeaseUUID, testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		MaintenanceWorkFn: func(context.Context, shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
			<-workerRelease
			return replaceWorkTerminal{result: success}
		},
	})

	ack := make(chan error, 1)
	require.True(t, actor.tryEnqueue(updateRequestedMsg{
		Ctx:                  context.Background(),
		CallbackURL:          claim.CallbackURL(),
		LifecycleCallbackURL: claim.LifecycleCallbackURL(),
		Maintenance:          claim,
		Target:               testMaintenanceTarget(t, claim),
		Ack:                  ack,
	}))

	select {
	case err := <-ack:
		require.NoError(t, err, "update from Ready must be accepted by the SM")
	case <-time.After(2 * time.Second):
		t.Fatal("no ack received from handleUpdateRequested")
	}

	prov, ok := store.Get(testActorLeaseUUID)
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusUpdating, prov.Status,
		"actor must write Status=Updating BEFORE acking (handler-publish contract)")
	assert.Equal(t, "https://old.example/callbacks/provision?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8", prov.CallbackURL,
		"accepted maintenance must not replace committed runtime authority before success")
	assert.Equal(t, "https://old.example/callbacks/provision?lifecycle_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8", prov.LifecycleCallbackURL,
		"accepted maintenance must keep the committed lifecycle route pending")
	assert.Equal(t, claim.CallbackURL(), actor.pendingReplaceCallbackURL)
	assert.Equal(t, claim.LifecycleCallbackURL(), actor.pendingReplaceLifecycleCallbackURL)

	close(workerRelease)
	require.Eventually(t, func() bool {
		committed, exists := store.Get(testActorLeaseUUID)
		return exists && committed.Status == backend.ProvisionStatusReady &&
			committed.CallbackURL == claim.CallbackURL() &&
			committed.LifecycleCallbackURL == claim.LifecycleCallbackURL()
	}, time.Second, 5*time.Millisecond)
}

// routeReplace enqueues the op-appropriate replace request ("restart" or
// "update") onto the actor's inbox. Shared by the ENG-230 §6.3 matrix tests
// so the restart and update paths are exercised by identical logic.
func routeReplace(
	actor *LeaseActor,
	op string,
	target shared.MaintenanceReleaseClaim,
	ack chan error,
) bool {
	claim := target.Intent()
	if op == "update" {
		return actor.tryEnqueue(updateRequestedMsg{
			Ctx:                  context.Background(),
			CallbackURL:          claim.CallbackURL(),
			LifecycleCallbackURL: claim.LifecycleCallbackURL(),
			Maintenance:          claim,
			Target:               target,
			Ack:                  ack,
		})
	}
	return actor.tryEnqueue(restartRequestedMsg{
		Ctx:                  context.Background(),
		CallbackURL:          claim.CallbackURL(),
		LifecycleCallbackURL: claim.LifecycleCallbackURL(),
		Maintenance:          claim,
		Target:               target,
		Ack:                  ack,
	})
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
	kind := shared.MaintenanceIntentRestart
	if op == "update" {
		kind = shared.MaintenanceIntentUpdate
	}
	firstClaim := newTestMaintenanceClaim(t, testActorLeaseUUID, kind)
	secondClaim := newTestMaintenanceClaim(t, testActorLeaseUUID, kind)
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusReady})

	doDeprovision := func(ctx context.Context, scope ActorCloseScope) error {
		leaseUUID := scope.LeaseUUID()
		store.remove(leaseUUID)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var workerCount atomic.Int64
	var firstCancelObserved, secondWorkerRan atomic.Bool
	worker1Release := make(chan struct{})
	firstSuccess := testMaintenanceSuccess(t, firstClaim, ReplaceSuccessProjection{})
	firstTarget := testMaintenanceTarget(t, firstClaim)
	secondTarget := testMaintenanceTarget(t, secondClaim)
	actor := newTestActor(t, testActorLeaseUUID, testActorOpts{
		StopCtx:         ctx,
		ProvisionStore:  store,
		DoDeprovisionFn: doDeprovision,
		MaintenanceWorkFn: func(workerCtx context.Context, target shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
			workerCount.Add(1)
			if target.MaintenanceID() == secondTarget.MaintenanceID() {
				secondWorkerRan.Store(true)
			}
			<-workerCtx.Done()
			firstCancelObserved.Store(true)
			<-worker1Release
			return replaceWorkTerminal{result: firstSuccess}
		},
	})

	// Request #1 wins: SM → busy, worker #1 spawned and blocks.
	ack1 := make(chan error, 1)
	require.True(t, routeReplace(actor, op, firstTarget, ack1))
	select {
	case err := <-ack1:
		require.NoError(t, err, "first %s must be accepted", op)
	case <-time.After(2 * time.Second):
		t.Fatalf("no ack from first %s", op)
	}
	require.Equal(t, busyStateFor(op), actor.State())

	// Request #2 loses the race: SM already busy → rejected with 409.
	ack2 := make(chan error, 1)
	require.True(t, routeReplace(actor, op, secondTarget, ack2))
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
	require.True(t, actor.tryEnqueue(deprovisionMsg{Ctx: context.Background(), Reply: reply}))

	require.Eventually(t, firstCancelObserved.Load, 1*time.Second, 5*time.Millisecond,
		"onExitProvisioning must cancel the FIRST (in-flight) %s worker", op)
	assert.False(t, secondWorkerRan.Load(),
		"the rejected second %s must not run", op)

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
	kind := shared.MaintenanceIntentRestart
	if op == "update" {
		kind = shared.MaintenanceIntentUpdate
	}
	claim := newTestMaintenanceClaim(t, testActorLeaseUUID, kind)
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusReady})

	// No-spawn: drive the handlers synchronously. The default DoDeprovisionFn
	// is a no-op that leaves the provision in place, so the SM stays in
	// Deprovisioning with terminated=false (the partial-deprovision actor).
	var workerCount atomic.Int64
	actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{
		ProvisionStore: store,
		MaintenanceWorkFn: func(context.Context, shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
			workerCount.Add(1)
			return nil
		},
	})

	require.NoError(t, actor.handleDeprovision(context.Background()))
	require.Equal(t, backend.ProvisionStatusDeprovisioning, actor.State(),
		"test precondition: SM must be Deprovisioning")

	ack := make(chan error, 1)
	target := testMaintenanceTarget(t, claim)
	if op == "update" {
		actor.handleUpdateRequested(updateRequestedMsg{
			Ctx: context.Background(), Target: target, Ack: ack,
			CallbackURL: claim.CallbackURL(), LifecycleCallbackURL: claim.LifecycleCallbackURL(), Maintenance: claim,
		})
	} else {
		actor.handleRestartRequested(restartRequestedMsg{
			Ctx: context.Background(), Target: target, Ack: ack,
			CallbackURL: claim.CallbackURL(), LifecycleCallbackURL: claim.LifecycleCallbackURL(), Maintenance: claim,
		})
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
// (lease_sm.go:236-240), Status and the prevalidated callback pair written
// before the ack, exactly one worker spawned.
func runReplaceFromFailedSucceedsTest(t *testing.T, op string) {
	t.Helper()
	kind := shared.MaintenanceIntentRestart
	if op == "update" {
		kind = shared.MaintenanceIntentUpdate
	}
	claim := newTestMaintenanceClaim(t, testActorLeaseUUID, kind)
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{
		LeaseUUID:            testActorLeaseUUID,
		Status:               backend.ProvisionStatusFailed,
		CallbackURL:          "old-cb",
		LifecycleCallbackURL: "old-lifecycle-cb",
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var operationCallbacks atomic.Int64
	var lifecycleCallbacks atomic.Int64
	var maintenanceCallbacks atomic.Int64
	var workerCount atomic.Int64
	workerRelease := make(chan struct{})
	success := testMaintenanceSuccess(t, claim, ReplaceSuccessProjection{})
	target := testMaintenanceTarget(t, claim)
	actor := newTestActor(t, testActorLeaseUUID, testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		MaintenanceWorkFn: func(context.Context, shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
			workerCount.Add(1)
			<-workerRelease
			return replaceWorkTerminal{result: success}
		},
		SendOperationCallbackFn: func(string, string, backend.CallbackStatus, string) {
			operationCallbacks.Add(1)
		},
		SendLifecycleFailureFn: func(shared.RuntimeGenerationProof, string) {
			lifecycleCallbacks.Add(1)
		},
		SendMaintenanceCallbackFn: func(got shared.MaintenanceIntentClaim, _ backend.CallbackStatus, _ string) {
			assert.Equal(t, claim.MaintenanceID(), got.MaintenanceID())
			maintenanceCallbacks.Add(1)
		},
	})

	ack := make(chan error, 1)
	require.True(t, routeReplace(actor, op, target, ack))
	select {
	case err := <-ack:
		require.NoError(t, err, "%s from Failed must be accepted (fresh-actor-init-in-Failed path)", op)
	case <-time.After(2 * time.Second):
		t.Fatalf("no ack from %s-from-Failed", op)
	}

	prov, ok := store.Get(testActorLeaseUUID)
	require.True(t, ok)
	assert.Equal(t, busyStateFor(op), prov.Status, "Status must be written before the ack")
	assert.Equal(t, "old-cb", prov.CallbackURL, "committed operation route must remain until success")
	assert.Equal(t, "old-lifecycle-cb", prov.LifecycleCallbackURL, "committed lifecycle route must remain until success")

	close(workerRelease) // let the worker finish so the actor can quiesce
	require.Eventually(t, func() bool {
		committed, exists := store.Get(testActorLeaseUUID)
		return workerCount.Load() == 1 && maintenanceCallbacks.Load() == 1 && exists &&
			committed.CallbackURL == claim.CallbackURL() &&
			committed.LifecycleCallbackURL == claim.LifecycleCallbackURL()
	}, time.Second, 5*time.Millisecond, "exactly one %s worker and maintenance completion must run", op)
	assert.Zero(t, operationCallbacks.Load(), "%s completion must not enter the operation outbox", op)
	assert.Zero(t, lifecycleCallbacks.Load(), "%s exact completion must not use observational delivery", op)
}

func TestRestartFromFailed_Succeeds(t *testing.T) { runReplaceFromFailedSucceedsTest(t, "restart") }
func TestUpdateFromFailed_Succeeds(t *testing.T)  { runReplaceFromFailedSucceedsTest(t, "update") }

func TestMaintenanceFailureKeepsCommittedRuntimeRoute(t *testing.T) {
	for _, tt := range []struct {
		name              string
		op                string
		err               error
		restored          bool
		recoverFromSource bool
		details           ReplaceFailureDetails
	}{
		{
			name:              "restart preflight before mutation",
			op:                "restart",
			err:               errors.New("preflight failed"),
			recoverFromSource: true,
			details:           ReplaceFailureDetails{CallbackErr: "restart failed", LastError: "preflight failed", Reason: backend.ReasonRestartFailed},
		},
		{
			name:     "update rollback restored old runtime",
			op:       "update",
			err:      errors.New("replacement failed"),
			restored: true,
			details:  ReplaceFailureDetails{CallbackErr: "update failed; rolled back", LastError: "replacement failed", Reason: backend.ReasonUpdateFailed},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			kind := shared.MaintenanceIntentRestart
			if tt.op == "update" {
				kind = shared.MaintenanceIntentUpdate
			}
			claim := newTestMaintenanceClaim(t, testActorLeaseUUID, kind)
			store := newMockProvisionStore()
			store.put(testActorLeaseUUID, &ProvisionState{
				LeaseUUID:            testActorLeaseUUID,
				Status:               backend.ProvisionStatusReady,
				CallbackURL:          "old-operation",
				LifecycleCallbackURL: "old-lifecycle",
			})
			callback := make(chan string, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := testMaintenanceFailure(t, claim, tt.err, tt.restored, tt.recoverFromSource, tt.details)
			target := testMaintenanceTarget(t, claim)
			actor := newTestActor(t, testActorLeaseUUID, testActorOpts{
				StopCtx:        ctx,
				ProvisionStore: store,
				MaintenanceWorkFn: func(context.Context, shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
					return replaceWorkTerminal{result: result}
				},
				SendMaintenanceCallbackFn: func(_ shared.MaintenanceIntentClaim, status backend.CallbackStatus, _ string) {
					assert.Equal(t, backend.CallbackStatusFailed, status)
					callback <- claim.LifecycleCallbackURL()
				},
			})
			ack := make(chan error, 1)
			require.True(t, routeReplace(actor, tt.op, target, ack))
			require.NoError(t, <-ack)
			select {
			case callbackURL := <-callback:
				assert.Equal(t, claim.LifecycleCallbackURL(), callbackURL,
					"completion belongs to the accepted maintenance request")
			case <-time.After(time.Second):
				t.Fatal("maintenance failure callback was not sent")
			}
			committed, exists := store.Get(testActorLeaseUUID)
			require.True(t, exists)
			assert.Equal(t, backend.ProvisionStatusReady, committed.Status)
			assert.Equal(t, "old-operation", committed.CallbackURL)
			assert.Equal(t, "old-lifecycle", committed.LifecycleCallbackURL)
		})
	}
}

// TestRestoreRequestedMsg_FiresEventAndSpawnsWorker pins the restore
// plumbing (ENG-325 Task 7a): a restoreRequestedMsg rides the existing
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
	leaseUUID := testActorLeaseUUID
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID:   leaseUUID,
		Tenant:      "tenant-a",
		Status:      backend.ProvisionStatusProvisioning,
		CallbackURL: "old-cb",
	})

	metrics := &countingMetrics{}
	var operationCallbacks atomic.Int64
	var lifecycleCallbacks atomic.Int64
	workerRelease := make(chan struct{})
	operation, restoreSuccess := testRestoreSuccess(t, leaseUUID, ReplaceSuccessProjection{ContainerIDs: []string{"c1"}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, leaseUUID, testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		Metrics:        metrics,
		RestoreWorkFn: func(context.Context, shared.OperationIntentClaim) ReplaceWorkOutcome {
			<-workerRelease
			return replaceWorkTerminal{result: restoreSuccess}
		},
		SendOperationCallbackFn: func(_ string, callbackURL string, _ backend.CallbackStatus, _ string) {
			assert.Equal(t, operation.CallbackURL(), callbackURL)
			operationCallbacks.Add(1)
		},
		SendLifecycleFailureFn: func(shared.RuntimeGenerationProof, string) {
			lifecycleCallbacks.Add(1)
		},
	})
	require.Equal(t, backend.ProvisionStatusProvisioning, actor.State(),
		"test precondition: SM must start in Provisioning (restore reserves the new lease there)")

	// Block the worker so we can observe Restarting + CallbackURL before it
	// flips the lease to Ready.
	ack := make(chan error, 1)
	require.True(t, actor.tryEnqueue(restoreRequestedMsg{
		Ctx:                  context.Background(),
		CallbackURL:          operation.CallbackURL(),
		LifecycleCallbackURL: operation.LifecycleCallbackURL(),
		Ack:                  ack,
		Operation:            operation,
	}))

	select {
	case err := <-ack:
		require.NoError(t, err, "restore from Provisioning must be accepted by the SM")
	case <-time.After(2 * time.Second):
		t.Fatal("no ack received from handleRestoreRequested")
	}

	prov, ok := store.Get(leaseUUID)
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusRestarting, prov.Status,
		"actor must write Status=Restarting BEFORE acking (handler-publish contract)")
	assert.Equal(t, operation.CallbackURL(), prov.CallbackURL,
		"actor must apply the message CallbackURL before acking")
	assert.False(t, actor.replaceWasActive,
		"replaceWasActive must be false for a Provisioning→Restarting restore (lease was absent, not active)")

	// Release the worker → evReplaceCompleted → Ready.
	close(workerRelease)
	require.Eventually(t, func() bool {
		return actor.State() == backend.ProvisionStatusReady
	}, 2*time.Second, 10*time.Millisecond,
		"SM must reach Ready via replaceCompleted after the restore worker succeeds")

	final, ok := store.Get(leaseUUID)
	require.True(t, ok)
	assert.Equal(t, []string{"c1"}, final.ContainerIDs,
		"ContainerIDs must reflect the restore worker's success result")
	require.Eventually(t, func() bool { return operationCallbacks.Load() == 1 }, time.Second, 5*time.Millisecond)
	assert.Zero(t, lifecycleCallbacks.Load(), "restore completion must use the exact operation outbox")
}

// TestRestoreRequested_RejectedFromBadState mirrors the restart
// loses-to-deprovision case: evRestoreRequested is NOT permitted from
// Deprovisioning, so a restore against a deprovisioning lease must be
// rejected via classifyReplaceReject (ErrInvalidState → 409) and spawn
// no worker. (Restore is only permitted from Provisioning.)
func TestRestoreRequested_RejectedFromBadState(t *testing.T) {
	const leaseUUID = "11111111-1111-4111-8111-111111111111"
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{LeaseUUID: leaseUUID, Status: backend.ProvisionStatusReady})

	// No-spawn: drive the handlers synchronously. The default DoDeprovisionFn
	// is a no-op that leaves the provision in place, so the SM stays in
	// Deprovisioning with terminated=false (the partial-deprovision actor).
	var workerCount atomic.Int64
	actor := newTestActorNoSpawn(t, leaseUUID, testActorOpts{
		ProvisionStore: store,
		RestoreWorkFn: func(context.Context, shared.OperationIntentClaim) ReplaceWorkOutcome {
			workerCount.Add(1)
			return nil
		},
	})

	require.NoError(t, actor.handleDeprovision(context.Background()))
	require.Equal(t, backend.ProvisionStatusDeprovisioning, actor.State(),
		"test precondition: SM must be Deprovisioning")

	operation := newTestOperationFixture(t, leaseUUID, shared.OperationIntentRestore).claim
	command, reply, err := NewRestoreCommand(context.Background(), operation)
	require.NoError(t, err)
	actor.handleRestoreRequested(command.envelope.message.(restoreRequestedMsg))

	select {
	case err := <-reply.Result():
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
	const leaseUUID = "22222222-2222-4222-8222-222222222222"
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID: leaseUUID,
		Status:    backend.ProvisionStatusDeprovisioning,
	})
	var workerSpawned atomic.Bool
	actor := newTestActorNoSpawn(t, leaseUUID, testActorOpts{
		ProvisionStore: store,
		RestoreWorkFn: func(context.Context, shared.OperationIntentClaim) ReplaceWorkOutcome {
			workerSpawned.Store(true)
			return nil
		},
	})
	actor.terminated = true

	operation := newTestOperationFixture(t, leaseUUID, shared.OperationIntentRestore).claim
	command, reply, err := NewRestoreCommand(context.Background(), operation)
	require.NoError(t, err)
	actor.handleRestoreRequested(command.envelope.message.(restoreRequestedMsg))

	select {
	case err := <-reply.Result():
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
// construction-bound restore handler end-to-end through handleRestoreRequested:
// the worker's recover must preserve the nonterminal state because a panic is
// ambiguous physical evidence, and ack the restore without crashing fred. The
// restore reuses spawnReplaceWorker, so this is the restore-shaped
// mirror of TestSpawnReplaceWorker_PanicRecovery.
func TestSpawnReplaceWorker_RestorePanicRecovery(t *testing.T) {
	const leaseUUID = "44444444-4444-4444-8444-444444444444"
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID: leaseUUID,
		Tenant:    "tenant-a",
		Status:    backend.ProvisionStatusProvisioning,
	})

	metrics := &countingMetrics{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actor := newTestActor(t, leaseUUID, testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		Metrics:        metrics,
		RestoreWorkFn: func(context.Context, shared.OperationIntentClaim) ReplaceWorkOutcome {
			panic("synthetic restore panic")
		},
	})

	panicsBefore := metrics.workerPanic.Load()

	operation := newTestOperationFixture(t, leaseUUID, shared.OperationIntentRestore).claim
	command, reply, err := NewRestoreCommand(context.Background(), operation)
	require.NoError(t, err)
	require.True(t, actor.TryEnqueueCommand(command))

	select {
	case err := <-reply.Result():
		require.NoError(t, err, "restore from Provisioning must be accepted before the worker runs")
	case <-time.After(2 * time.Second):
		t.Fatal("no ack received from handleRestoreRequested")
	}

	require.Eventually(t, func() bool {
		return metrics.workerPanic.Load() == panicsBefore+1
	}, 2*time.Second, 10*time.Millisecond,
		"restore worker panic must be observed")
	assert.Equal(t, backend.ProvisionStatusRestarting, actor.State(),
		"ambiguous worker failure must not manufacture definitive failure evidence")

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
}

func (m *countingMetrics) SMTransition(_, _, _ string)   { m.smTransition.Add(1) }
func (m *countingMetrics) ActorCreated()                 { m.actorCreated.Add(1) }
func (m *countingMetrics) WorkerPanic(_ string)          { m.workerPanic.Add(1) }
func (m *countingMetrics) ActorPanic()                   { m.actorPanic.Add(1) }
func (m *countingMetrics) TerminalEventDropped(_ string) { m.terminalEventDropped.Add(1) }

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
	leaseUUID := testActorLeaseUUID
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{LeaseUUID: leaseUUID, Status: backend.ProvisionStatusProvisioning})
	a := newTestActorNoSpawn(t, leaseUUID, testActorOpts{ProvisionStore: store})
	require.NoError(t, a.sm.requestProvision(context.Background()))
	_, failure := newTestOperationFailure(t, leaseUUID, shared.OperationIntentProvision)
	require.NoError(t, a.sm.provisionErrored(context.Background(), provisionErrorInfo{
		callbackErr:      "image pull failed",
		reason:           backend.ReasonImagePullFailed,
		lastError:        "pull /data/fred/... exit 1",
		operationFailure: failure,
	}))

	got, ok := store.Get(leaseUUID)
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
		operation:   "restart",
		reason:      backend.ReasonRestartFailed,
		callbackErr: "restart failed; rolled back to previous version",
		lastError:   "compose up exit 1: /data/fred/... permission denied",
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

// TestReplaceCompleted_ClearsStaleReasonMessage asserts that the
// Restarting→Ready success entry action (onEnterReadyFromReplaceCompleted)
// clears the curated (Reason, Message) pair when a previously-failed lease
// recovers (ENG-508). ProvisionState persists across transitions, so a
// lease that failed (Reason=ContainerExited) and is then restarted still
// carries the stale failure Reason/Message into Restarting; the success
// transition MUST wipe them so /status|/provision|/releases no longer
// surface a stale failure reason for a now-healthy Ready lease. Drives the
// path white-box mirroring TestReplaceFailed_AuthorsReasonMessage: the
// store is seeded in Restarting (so readProvisionStatus initializes the SM
// there) carrying the stale Reason/Message, then handleReplaceCompleted
// fires evReplaceCompleted → Ready.
func TestReplaceCompleted_ClearsStaleReasonMessage(t *testing.T) {
	leaseUUID := testActorLeaseUUID
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID: leaseUUID,
		Status:    backend.ProvisionStatusRestarting,
		Reason:    backend.ReasonContainerExited,
		Message:   "container exited unexpectedly",
		LastError: "compose ps: container 'app' exited with code 137",
	})
	a := newTestActorNoSpawn(t, leaseUUID, testActorOpts{ProvisionStore: store})
	_, success := testRestoreSuccess(t, leaseUUID, ReplaceSuccessProjection{ContainerIDs: []string{"c1"}})

	a.handleReplaceCompleted(success.success)

	got, ok := store.Get(leaseUUID)
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusReady, got.Status,
		"a successful replace must land the lease in Ready")
	assert.Equal(t, backend.Reason(""), got.Reason,
		"stale failure Reason must be cleared on healthy recovery")
	assert.Equal(t, "", got.Message,
		"stale failure Message must be cleared on healthy recovery")
}

// TestProvisionCompleted_ClearsStaleReasonMessage is the defense-in-depth
// companion for the successful-provision entry action
// (onEnterReadyFromProvision): a Failed lease that is re-provisioned (retry)
// must not carry its prior failure Reason/Message into the healthy Ready
// record (ENG-508). Drives Provisioning→Ready by firing the SM directly (in-package).
func TestProvisionCompleted_ClearsStaleReasonMessage(t *testing.T) {
	leaseUUID := testActorLeaseUUID
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID: leaseUUID,
		Status:    backend.ProvisionStatusFailed,
		Reason:    backend.ReasonImagePullFailed,
		Message:   "image pull failed",
		LastError: "pull /data/fred/... exit 1",
	})
	a := newTestActorNoSpawn(t, leaseUUID, testActorOpts{ProvisionStore: store})

	require.NoError(t, a.sm.requestProvision(context.Background()))
	_, success := testProvisionSuccess(t, leaseUUID, ProvisionSuccessProjection{ContainerIDs: []string{"c1"}})
	require.NoError(t, a.sm.provisionCompleted(context.Background(), success))

	got, ok := store.Get(leaseUUID)
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusReady, got.Status,
		"a successful provision must land the lease in Ready")
	assert.Equal(t, backend.Reason(""), got.Reason,
		"stale failure Reason must be cleared on healthy provision")
	assert.Equal(t, "", got.Message,
		"stale failure Message must be cleared on healthy provision")
}
