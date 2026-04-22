package docker

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type leaseMessage interface {
	isLeaseMessage()
	// doneChan returns the channel to close when processing finishes, or nil
	// for fire-and-forget messages. Lifting this out of the handler dispatch
	// lets shutdown drain pending messages without a per-type switch.
	doneChan() chan struct{}
}

// containerDiedMsg signals a container belonging to this lease has died.
// If done is non-nil, the actor closes it after the message is processed,
// letting synchronous callers block until completion.
type containerDiedMsg struct {
	containerID string
	done        chan struct{}
}

func (containerDiedMsg) isLeaseMessage()           {}
func (m containerDiedMsg) doneChan() chan struct{} { return m.done }

// deprovisionMsg requests that the actor run the Deprovision flow. The
// reply channel receives the outcome; doneChan is always nil because
// callers block on reply instead.
type deprovisionMsg struct {
	ctx   context.Context
	reply chan error
}

func (deprovisionMsg) isLeaseMessage()         {}
func (deprovisionMsg) doneChan() chan struct{} { return nil }

// diagGatheredMsg is sent by the async diag goroutine when it finishes.
// Carries the gather output into the Failing→Failed transition.
type diagGatheredMsg struct {
	result diagResult
}

func (diagGatheredMsg) isLeaseMessage()         {}
func (diagGatheredMsg) doneChan() chan struct{} { return nil }

// provisionRequestedMsg asks the actor to drive a provision flow. Carries
// the cancel func (stored on the actor so Provisioning.OnExit can preempt)
// and a `work` closure containing everything doProvision / doProvisionStack
// needs — the actor doesn't need to know the arg shapes. After firing the SM
// transition, the actor sends the Fire result on `ack` and spawns a worker
// (tracked by workersWg) to run `work`. Backend.Provision blocks on `ack` so
// it knows whether the SM accepted the transition before returning.
type provisionRequestedMsg struct {
	cancel context.CancelFunc
	work   func() (callbackErr string, result provisionSuccessResult, failureLogs map[string]string, err error)
	ack    chan error
}

func (provisionRequestedMsg) isLeaseMessage()         {}
func (provisionRequestedMsg) doneChan() chan struct{} { return nil }

// provisionCompletedMsg is sent by the doProvision goroutine on success.
// Drives the Provisioning→Ready transition. Carries the result data
// (containerIDs, manifest, stackManifest, serviceContainers) that the
// Ready entry action writes into the provision struct.
type provisionCompletedMsg struct {
	result provisionSuccessResult
}

func (provisionCompletedMsg) isLeaseMessage()         {}
func (provisionCompletedMsg) doneChan() chan struct{} { return nil }

// provisionErroredMsg is sent by the doProvision goroutine on failure.
// callbackErr is the hardcoded on-chain-safe message; lastError is the
// full diagnostic string (from err.Error()) that the Failed entry
// action writes into provision.LastError. logs is the pre-captured
// container-log map (fetched BEFORE the cleanup defer removed the
// failed containers) so persistDiagnostics doesn't attempt to re-fetch
// from already-deleted containers — see doProvision's captureContainerLogs
// call.
type provisionErroredMsg struct {
	callbackErr string
	lastError   string
	logs        map[string]string
}

func (provisionErroredMsg) isLeaseMessage()         {}
func (provisionErroredMsg) doneChan() chan struct{} { return nil }

// restartRequestedMsg / updateRequestedMsg carry a cancel func + work
// closure + ack chan, analogous to provisionRequestedMsg. The work closure
// returns a replaceResult consumed by the actor to pick the right terminal
// SM event (completed / recovered / failed).
type restartRequestedMsg struct {
	cancel context.CancelFunc
	work   func() replaceResult
	ack    chan error
}

func (restartRequestedMsg) isLeaseMessage()         {}
func (restartRequestedMsg) doneChan() chan struct{} { return nil }

type updateRequestedMsg struct {
	cancel context.CancelFunc
	work   func() replaceResult
	ack    chan error
}

func (updateRequestedMsg) isLeaseMessage()         {}
func (updateRequestedMsg) doneChan() chan struct{} { return nil }

// replaceCompletedMsg / replaceRecoveredMsg / replaceFailedMsg fire the
// Restarting|Updating exit transition after the goroutine finishes. The
// goroutine picks which to send based on (err == nil, final Status):
//
//	err == nil                → replaceCompletedMsg  → Ready (Success)
//	err != nil, Status=Ready  → replaceRecoveredMsg  → Ready (Failed+suffix)
//	err != nil, Status=Failed → replaceFailedMsg     → Failed (Failed)
//
// Both replaceRecoveredMsg and replaceFailedMsg carry the callbackErr
// string that the SM entry action emits verbatim.
type replaceCompletedMsg struct {
	result replaceSuccessResult
}

func (replaceCompletedMsg) isLeaseMessage()         {}
func (replaceCompletedMsg) doneChan() chan struct{} { return nil }

type replaceRecoveredMsg struct {
	info replaceFailureInfo
}

func (replaceRecoveredMsg) isLeaseMessage()         {}
func (replaceRecoveredMsg) doneChan() chan struct{} { return nil }

type replaceFailedMsg struct {
	info replaceFailureInfo
}

func (replaceFailedMsg) isLeaseMessage()         {}
func (replaceFailedMsg) doneChan() chan struct{} { return nil }

// leaseActor owns all state transitions and async work for a single lease.
//
// One concept: the actor is the scope of atomicity for its messages and
// its workers. Everything else falls out of that:
//
//   - Registry atomicity — b.actors is guarded by actorsMu.
//     routeToLease() resolves-or-creates AND enqueues under the mutex;
//     removeFromRegistry (on actor exit) deletes under the same mutex.
//     External callers never hold a *leaseActor pointer, so stale-
//     pointer races are unreachable by construction.
//
//   - Worker ownership — every worker goroutine (provision, restart,
//     update, diag) is spawned by the actor (via spawnProvisionWorker /
//     spawnReplaceWorker / onEnterFailing's goroutine) and tracked by
//     workersWg. The actor's exit defers waitForWorkers BEFORE
//     registry-delete / close(done) / drainInbox — the actor cannot
//     exit while a worker is in flight. Workers always have a live
//     actor to sendTerminal to, so orphan-worker races are eliminated.
//
//   - Drain-with-handle — the drainInbox defer calls handle() on every
//     queued message, so a worker's terminal sendTerminal that landed
//     while the actor was waiting for workers still drives its SM
//     transition. Silent drops are gone.
//
//   - Non-blocking routing — routeToLease does a non-blocking send
//     under the registry mutex. A wedged actor cannot stall the
//     event-loop or other routing callers; refusals are counted in
//     die_event_dropped_total and the reconciler re-detects.
//
// Messages are processed serially from inbox, so handlers never race
// with themselves. SM transitions are synchronous inside handle() —
// Fire dispatches OnExit / OnEntry in the actor's own goroutine.
type leaseActor struct {
	leaseUUID string
	backend   *Backend
	inbox     chan leaseMessage
	done      chan struct{}
	sm        *leaseSM
	// pendingDeathInfo carries the Docker Inspect result from the SM's guard
	// into the onEnterFailing action. Single-field handoff works because the
	// actor processes messages serially; no two messages read/write this
	// field concurrently.
	pendingDeathInfo *ContainerInfo
	// diagCancel is set when entering Failing (spawning the async diag
	// goroutine) and called by Failing.OnExit to signal cancellation. The
	// cc62f3b structural mechanism: any transition out of Failing cancels
	// the goroutine before any stale Failed callback can be emitted.
	diagCancel context.CancelFunc
	// workCancel is set when a Provision/Restart/Update worker is spawned
	// from handleProvisionRequested / handleRestartRequested /
	// handleUpdateRequested, and called by Provisioning/Restarting/
	// Updating.OnExit on DeprovisionRequested preemption.
	workCancel context.CancelFunc
	// workersWg tracks every worker goroutine spawned by this actor
	// (provision, restart, update, diag). The actor's run-loop exit path
	// waits on workersWg BEFORE the registry-delete / close-done /
	// drainInbox defers — structurally guaranteeing that every worker's
	// terminal sendTerminal has landed and been handled before the actor
	// is torn down. Same wg is used by onExit* to wait for the active
	// worker when the SM is transitioning out of a work-owning state
	// (Deprovision preempt).
	//
	// The SM enforces at-most-one-worker-at-a-time across the work-owning
	// states, so workersWg.Wait effectively waits for "the one worker
	// currently running"; the count happens to always be 0 or 1.
	workersWg sync.WaitGroup
	// currentMessageStart is the UnixNano timestamp of the message the
	// actor is currently processing in handle(), or 0 when idle. Used by
	// the stuck-actor sampler to detect hung handlers. Written by the
	// actor's goroutine, read atomically by the sampler goroutine.
	currentMessageStart atomic.Int64
	// terminated is set by handleDeprovision once the provision entry has
	// been fully removed. The run loop checks it after every handle() and
	// exits, allowing the actor's slot in b.actors to be reused by a new
	// lease with the same UUID (a stale actor stuck in Deprovisioning
	// would otherwise Ignore evProvisionRequested and wedge the lease).
	// Read and written only on the actor's own goroutine — no atomic.
	terminated bool
}

// Bounded inbox: full inbox blocks senders so Docker event bursts cannot
// grow memory without bound.
const leaseActorInboxSize = 16

func newLeaseActor(b *Backend, leaseUUID string) *leaseActor {
	a := &leaseActor{
		leaseUUID: leaseUUID,
		backend:   b,
		inbox:     make(chan leaseMessage, leaseActorInboxSize),
		done:      make(chan struct{}),
	}
	// Initialize the SM eagerly so that the actor's goroutine and any
	// external reader (DebugActors over /debug/actors) see the same
	// pointer without synchronization on a lazy-init field. Construction
	// is cheap and safe to call from the caller's goroutine before the
	// actor goroutine starts.
	a.sm = newLeaseSM(a)
	return a
}

func (a *leaseActor) run() {
	// Defer ordering is deliberate (LIFO executes in reverse of declaration):
	//   1. waitForWorkers runs FIRST — blocks until every in-flight worker
	//      (provision/restart/update/diag) goroutine has returned. Workers
	//      deliver their terminal SM event via sendTerminal BEFORE returning,
	//      so by the time this unblocks, all terminal messages that WILL
	//      arrive have already landed in a.inbox.
	//   2. removeFromRegistry runs SECOND — concurrent routeToLease calls
	//      immediately create a fresh actor under actorsMu.
	//   3. drainInbox runs THIRD — processes every message in the inbox via
	//      handle(), so terminal events from workers actually drive their
	//      SM transitions before the actor is gone.
	//   4. close(a.done) runs LAST — makes actor.done a clean "fully
	//      quiesced" signal. Any waiter on a.done is guaranteed every
	//      queued message has been handled and every SM transition
	//      committed. hasExited becomes true here too; any stale
	//      sendTerminal after this point refuses deterministically.
	defer close(a.done)
	defer a.drainInbox()
	defer a.removeFromRegistry()
	defer a.waitForWorkers()
	for {
		if a.terminated {
			return
		}
		select {
		case msg := <-a.inbox:
			a.handle(msg)
		case <-a.backend.stopCtx.Done():
			// Shutdown: exit the main loop. The deferred waitForWorkers +
			// drainInbox guarantee in-flight workers finish and their
			// terminal events get handled before the actor is torn down.
			return
		}
	}
}

// waitForWorkers blocks until every worker goroutine this actor has
// spawned has returned. Bounded by workExitWaitTimeout so a wedged
// worker (Docker daemon hang with ctx ignored) can't pin the actor.
// If the timeout fires we log and continue — worker becomes a zombie
// goroutine; recoverState reconciles state on next start.
func (a *leaseActor) waitForWorkers() {
	done := make(chan struct{})
	go func() {
		a.workersWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(workExitWaitTimeout):
		a.backend.logger.Warn("actor waitForWorkers: worker did not exit within timeout",
			"lease_uuid", a.leaseUUID,
			"timeout", workExitWaitTimeout,
		)
	}
}

// drainInbox processes every message queued in the inbox via handle().
// Called as the final defer in run(), after waitForWorkers has ensured no
// more messages will arrive. handle() is safe to call post-main-loop —
// the SM is still alive, Ignore declarations catch events that arrive in
// the wrong state, and every msg.doneChan() gets closed so synchronous
// callers unblock.
func (a *leaseActor) drainInbox() {
	for {
		select {
		case msg := <-a.inbox:
			a.handle(msg)
		default:
			return
		}
	}
}

func (a *leaseActor) handle(msg leaseMessage) {
	a.currentMessageStart.Store(time.Now().UnixNano())
	defer a.currentMessageStart.Store(0)
	defer func() {
		if ch := msg.doneChan(); ch != nil {
			close(ch)
		}
	}()
	// Contain the blast radius of a handler panic to a single message:
	// log with stack, bump a counter, and let the actor keep processing.
	// Without this, a panic in an SM entry action or handler kills the
	// actor goroutine, leaving the actor in b.actors with a full inbox
	// and senders blocking forever.
	defer func() {
		if r := recover(); r != nil {
			a.backend.logger.Error("lease actor panic",
				"lease_uuid", a.leaseUUID,
				"panic", r,
				"stack", string(debug.Stack()),
			)
			leaseActorPanicsTotal.Inc()
		}
	}()
	switch m := msg.(type) {
	case containerDiedMsg:
		a.handleContainerDied(m.containerID)
	case deprovisionMsg:
		m.reply <- a.handleDeprovision(m.ctx)
	case diagGatheredMsg:
		a.handleDiagGathered(m.result)
	case provisionRequestedMsg:
		a.handleProvisionRequested(m)
	case restartRequestedMsg:
		a.handleRestartRequested(m)
	case updateRequestedMsg:
		a.handleUpdateRequested(m)
	case provisionCompletedMsg:
		a.handleProvisionCompleted(m.result)
	case provisionErroredMsg:
		a.handleProvisionErrored(m.callbackErr, m.lastError, m.logs)
	case replaceCompletedMsg:
		a.handleReplaceCompleted(m.result)
	case replaceRecoveredMsg:
		a.handleReplaceRecovered(m.info)
	case replaceFailedMsg:
		a.handleReplaceFailed(m.info)
	default:
		a.backend.logger.Warn("lease actor: unknown message type",
			"lease_uuid", a.leaseUUID,
		)
	}
}

func (a *leaseActor) handleContainerDied(containerID string) {
	_ = a.sm.Fire(a.backend.stopCtx, evContainerDied, containerID)
}

func (a *leaseActor) handleDiagGathered(result diagResult) {
	// If we're no longer in Failing (e.g., Deprovision preempted), the SM's
	// Ignore declarations on Failed/Deprovisioning drop this event; Fire
	// returns an unhandled-trigger error we can safely discard.
	_ = a.sm.Fire(a.backend.stopCtx, evDiagGathered, result)
}

// handleProvisionRequested transitions the SM into Provisioning (or from
// Failed on retry), acks the caller, and spawns the work goroutine.
// Spawning inside the actor (rather than in Backend.Provision) means the
// worker is tracked by workersWg and the actor cannot exit until the
// worker's terminal sendTerminal has landed and been handled — the
// orphan-worker race class is eliminated by construction.
func (a *leaseActor) handleProvisionRequested(msg provisionRequestedMsg) {
	a.workCancel = msg.cancel
	if err := a.sm.Fire(a.backend.stopCtx, evProvisionRequested); err != nil {
		msg.ack <- err
		return
	}
	msg.ack <- nil
	a.spawnProvisionWorker(msg.work)
}

// spawnProvisionWorker runs doProvision (or doProvisionStack, supplied as
// the work closure), pre-publishes container IDs on success so a preempting
// Deprovision sees them under lock, and sends the terminal SM event via
// sendTerminal. Tracked by workersWg so the actor waits for this worker
// before exit. On failure, the worker captures container logs BEFORE
// cleanup (see doProvision's defer) so the persisted diagnostic entry
// contains useful debugging output even though the failed containers
// have been removed.
func (a *leaseActor) spawnProvisionWorker(work func() (string, provisionSuccessResult, map[string]string, error)) {
	a.workersWg.Add(1)
	a.backend.wg.Go(func() {
		defer a.workersWg.Done()
		callbackErr, result, failureLogs, err := work()
		if err == nil {
			// Pre-publish so a concurrent Deprovision-preempt reading
			// prov.ContainerIDs sees the new IDs (bug_012).
			a.backend.provisionsMu.Lock()
			if p, ok := a.backend.provisions[a.leaseUUID]; ok {
				p.ContainerIDs = result.containerIDs
			}
			a.backend.provisionsMu.Unlock()
		}
		var ok bool
		var event string
		if err != nil {
			event = "provision_errored"
			ok = a.sendTerminal(provisionErroredMsg{
				callbackErr: callbackErr,
				lastError:   err.Error(),
				logs:        failureLogs,
			})
		} else {
			event = "provision_completed"
			ok = a.sendTerminal(provisionCompletedMsg{result: result})
		}
		if !ok {
			leaseTerminalEventDroppedTotal.WithLabelValues(event).Inc()
			a.backend.logger.Warn("terminal provision event dropped (actor exited or inbox wedged)",
				"lease_uuid", a.leaseUUID,
				"event", event,
			)
		}
	})
}

func (a *leaseActor) handleProvisionCompleted(result provisionSuccessResult) {
	_ = a.sm.Fire(a.backend.stopCtx, evProvisionCompleted, result)
}

func (a *leaseActor) handleProvisionErrored(callbackErr, lastError string, logs map[string]string) {
	_ = a.sm.Fire(a.backend.stopCtx, evProvisionErrored, provisionErrorInfo{
		callbackErr: callbackErr,
		lastError:   lastError,
		logs:        logs,
	})
}

func (a *leaseActor) handleRestartRequested(msg restartRequestedMsg) {
	a.workCancel = msg.cancel
	if err := a.sm.Fire(a.backend.stopCtx, evRestartRequested); err != nil {
		msg.ack <- err
		return
	}
	msg.ack <- nil
	a.spawnReplaceWorker(msg.work)
}

func (a *leaseActor) handleUpdateRequested(msg updateRequestedMsg) {
	a.workCancel = msg.cancel
	if err := a.sm.Fire(a.backend.stopCtx, evUpdateRequested); err != nil {
		msg.ack <- err
		return
	}
	msg.ack <- nil
	a.spawnReplaceWorker(msg.work)
}

// spawnReplaceWorker runs a replace operation (restart or update) and
// dispatches the correct terminal SM event based on (err, restored).
// Pre-publishes new ContainerIDs / ServiceContainers on success so a
// preempting Deprovision reading prov observes the new set under lock.
func (a *leaseActor) spawnReplaceWorker(work func() replaceResult) {
	a.workersWg.Add(1)
	a.backend.wg.Go(func() {
		defer a.workersWg.Done()
		result := work()
		if result.err == nil {
			a.backend.provisionsMu.Lock()
			if p, ok := a.backend.provisions[a.leaseUUID]; ok {
				p.ContainerIDs = result.success.containerIDs
				if result.success.serviceContainers != nil {
					p.ServiceContainers = result.success.serviceContainers
				}
			}
			a.backend.provisionsMu.Unlock()
		}
		var event string
		var ok bool
		switch {
		case result.err == nil:
			event = "replace_completed"
			ok = a.sendTerminal(replaceCompletedMsg{result: result.success})
		case result.restored:
			event = "replace_recovered"
			ok = a.sendTerminal(replaceRecoveredMsg{info: result.failure})
		default:
			event = "replace_failed"
			ok = a.sendTerminal(replaceFailedMsg{info: result.failure})
		}
		if !ok {
			leaseTerminalEventDroppedTotal.WithLabelValues(event).Inc()
			a.backend.logger.Warn("terminal replace event dropped (actor exited or inbox wedged)",
				"lease_uuid", a.leaseUUID,
				"event", event,
			)
		}
	})
}

func (a *leaseActor) handleReplaceCompleted(result replaceSuccessResult) {
	_ = a.sm.Fire(a.backend.stopCtx, evReplaceCompleted, result)
}

func (a *leaseActor) handleReplaceRecovered(info replaceFailureInfo) {
	_ = a.sm.Fire(a.backend.stopCtx, evReplaceRecovered, info)
}

func (a *leaseActor) handleReplaceFailed(info replaceFailureInfo) {
	_ = a.sm.Fire(a.backend.stopCtx, evReplaceFailed, info)
}

// send enqueues a message. Blocks when the inbox is full (backpressure).
// Returns false if the backend is shutting down OR the actor has exited.
//
// Test-only since Phase 1: production code uses b.routeToLease(uuid, msg)
// which never exposes an actor pointer to the caller. This method is
// retained for test code that needs synthetic direct-send scenarios.
//
// For TERMINAL events delivered by in-flight work goroutines — whose
// physical work has already happened on the host and MUST be recorded
// by the SM even during shutdown — use sendTerminal instead.
func (a *leaseActor) send(msg leaseMessage) bool {
	if a.backend.stopCtx.Err() != nil || a.hasExited() {
		return false
	}
	select {
	case <-a.backend.stopCtx.Done():
		return false
	case <-a.done:
		return false
	case a.inbox <- msg:
		return true
	}
}

// hasExited reports whether the actor's run loop has returned (a.done
// closed). Used by sendTerminal to make the "actor-already-exited" case
// a definitive refusal rather than a select-randomized 50/50 between
// queueing into an inbox nobody will drain and the closed-done arm.
func (a *leaseActor) hasExited() bool {
	select {
	case <-a.done:
		return true
	default:
		return false
	}
}

// sendTerminal enqueues a terminal SM event from an in-flight work
// goroutine. Bypasses the stopCtx refusal that send() applies because the
// goroutine has already done its physical work (containers created,
// swapped, removed) — the SM must record the outcome even during
// shutdown to keep releaseStore / in-memory state / the callback record
// consistent with the host. Returns false only if the actor has fully
// exited (inbox no longer drained) or the bounded inbox is wedged; in
// either case the drop is counted via leaseTerminalEventDroppedTotal at
// the call site.
//
// The hasExited fast-path is required for correctness in the edge case
// where waitForWorkers has timed out (wedged worker, 45s elapsed): the
// actor proceeded to exit with the worker still running, and the late
// sendTerminal must refuse deterministically rather than queue into an
// inbox nobody will drain. Without the pre-check, Go's select would
// pick non-deterministically between `a.inbox <- msg` and `<-a.done`.
// In normal operation workersWg.Wait ensures sendTerminal always runs
// with the actor alive, so the check is effectively a defense against
// the timeout edge case.
func (a *leaseActor) sendTerminal(msg leaseMessage) bool {
	if a.hasExited() {
		return false
	}
	select {
	case a.inbox <- msg:
		return true
	case <-a.done:
		return false
	case <-time.After(terminalSendTimeout):
		return false
	}
}

// terminalSendTimeout bounds how long a terminal send will wait for inbox
// space. Long enough for the actor to drain typical backlogs, short
// enough that a wedged actor doesn't pin the goroutine indefinitely.
const terminalSendTimeout = 10 * time.Second

// removeFromRegistry deletes this actor from b.actors under actorsMu.
// CompareAndDelete semantics: only deletes if the registered entry is
// THIS actor, so a fresh actor stored for the same UUID after our exit
// started isn't clobbered. Used as a deferred action on actor exit.
func (a *leaseActor) removeFromRegistry() {
	a.backend.actorsMu.Lock()
	defer a.backend.actorsMu.Unlock()
	if reg, ok := a.backend.actors[a.leaseUUID]; ok && reg == a {
		delete(a.backend.actors, a.leaseUUID)
	}
}

// actorForLocked returns the lease actor for leaseUUID, creating + starting
// one on first access. Caller MUST hold b.actorsMu. The whole point of the
// registry mutex: resolve-or-create and any subsequent state change (enqueue,
// exit) serialize through the same lock.
func (b *Backend) actorForLocked(leaseUUID string) *leaseActor {
	if existing, ok := b.actors[leaseUUID]; ok {
		return existing
	}
	candidate := newLeaseActor(b, leaseUUID)
	b.actors[leaseUUID] = candidate
	leaseActorsCreatedTotal.Inc()
	b.wg.Go(candidate.run)
	return candidate
}

// routeToLease is the ONLY way external code delivers a message to a lease's
// actor. It resolves-or-creates the actor AND enqueues atomically under
// actorsMu — so the stale-pointer race class (caller retained an actor
// reference while the actor was terminating) cannot occur by construction:
// callers never hold a *leaseActor pointer.
//
// Returns false if the backend is shutting down OR the inbox is full (the
// enqueue is non-blocking to avoid holding the registry mutex across a
// potentially-slow channel send). Fire-and-forget callers
// (containerEventLoop, reconcile) treat refusal as "reconciler will
// re-detect". Caller-facing API paths that need backpressure-retry
// semantics should use routeToLeaseBlocking instead.
func (b *Backend) routeToLease(leaseUUID string, msg leaseMessage) bool {
	b.actorsMu.Lock()
	defer b.actorsMu.Unlock()
	if b.stopCtx.Err() != nil {
		return false
	}
	actor := b.actorForLocked(leaseUUID)
	select {
	case actor.inbox <- msg:
		return true
	default:
		return false
	}
}

// routeToLeaseBlocking wraps routeToLease with ctx-bounded retry so
// caller-facing API paths (Provision, Deprovision, Restart, Update)
// don't spuriously fail on transient inbox saturation. Returns nil on
// successful enqueue, ctx.Err() on caller cancellation, or a "backend
// shutting down" error when stopCtx fires. Polls on
// routeToLeaseRetryInterval while the inbox is full — a few ms of
// latency is acceptable for API calls; the alternative is turning
// backpressure into a 5xx.
//
// The up-front ctx / stopCtx check guarantees we don't enqueue a
// message the caller is about to abandon: without it, the first
// routeToLease could succeed and start async work while the caller
// returns ctx.Err() having seen nothing.
func (b *Backend) routeToLeaseBlocking(ctx context.Context, leaseUUID string, msg leaseMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.stopCtx.Err() != nil {
		return fmt.Errorf("backend shutting down")
	}
	for {
		if b.routeToLease(leaseUUID, msg) {
			return nil
		}
		if b.stopCtx.Err() != nil {
			return fmt.Errorf("backend shutting down")
		}
		select {
		case <-b.stopCtx.Done():
			return fmt.Errorf("backend shutting down")
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(routeToLeaseRetryInterval):
		}
	}
}

// routeToLeaseRetryInterval is the poll interval for routeToLeaseBlocking
// when the target inbox is momentarily full. Short enough that API-call
// latency from backpressure is negligible in normal operation (inbox
// rarely fills); long enough to avoid hot-spinning a contended actor.
const routeToLeaseRetryInterval = 10 * time.Millisecond

// ActorSnapshot is a point-in-time view of one lease actor's state for
// operator introspection. Safe to marshal to JSON for a /debug/actors
// endpoint when integrated with the HTTP layer.
type ActorSnapshot struct {
	LeaseUUID  string `json:"lease_uuid"`
	SMState    string `json:"sm_state"`    // current SM state
	InboxDepth int    `json:"inbox_depth"` // pending messages not yet processed
	InboxCap   int    `json:"inbox_cap"`
}

// actorMetricsSampleInterval paces sampleActorMetrics. Short enough for
// the stuck-actor gauge to react within an alerting window, long enough
// that walking the registry and sampling inbox depth stays negligible.
const actorMetricsSampleInterval = 5 * time.Second

// sampleActorMetrics walks every live actor, observing inbox depth into
// the histogram and finding the oldest in-flight handle() start across
// all actors for the stuck-seconds gauge. Called periodically from
// actorMetricsSampleLoop.
//
// Holds actorsMu only long enough to snapshot the actor list, then observes
// outside the lock. inbox len() and currentMessageStart are both safe to
// read concurrently with actor work (inbox len is racy-but-fine; atomic
// load for currentMessageStart).
func (b *Backend) sampleActorMetrics() {
	now := time.Now().UnixNano()
	b.actorsMu.Lock()
	actors := make([]*leaseActor, 0, len(b.actors))
	for _, actor := range b.actors {
		actors = append(actors, actor)
	}
	b.actorsMu.Unlock()

	var oldestStart int64
	for _, actor := range actors {
		leaseActorInboxDepth.Observe(float64(len(actor.inbox)))
		if start := actor.currentMessageStart.Load(); start != 0 {
			if oldestStart == 0 || start < oldestStart {
				oldestStart = start
			}
		}
	}
	if oldestStart == 0 {
		leaseActorStuckSeconds.Set(0)
	} else {
		leaseActorStuckSeconds.Set(float64(now-oldestStart) / float64(time.Second))
	}
}

// actorMetricsSampleLoop runs sampleActorMetrics on a ticker until the
// backend shuts down. Spawned once from Start() via b.wg.Go.
func (b *Backend) actorMetricsSampleLoop() {
	ticker := time.NewTicker(actorMetricsSampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stopCtx.Done():
			return
		case <-ticker.C:
			b.sampleActorMetrics()
		}
	}
}

// DebugActors returns a snapshot of every live lease actor. The result
// is stable for the caller: it's a copy; the registry may grow or
// change state after return. Intended for ops introspection during
// incidents — pair with a /debug/actors HTTP handler that JSON-encodes
// the return.
func (b *Backend) DebugActors() []ActorSnapshot {
	b.actorsMu.Lock()
	actors := make(map[string]*leaseActor, len(b.actors))
	for uuid, actor := range b.actors {
		actors[uuid] = actor
	}
	b.actorsMu.Unlock()

	snapshots := make([]ActorSnapshot, 0, len(actors))
	for leaseUUID, actor := range actors {
		snapshots = append(snapshots, ActorSnapshot{
			LeaseUUID:  leaseUUID,
			SMState:    fmt.Sprintf("%v", actor.sm.State()),
			InboxDepth: len(actor.inbox),
			InboxCap:   cap(actor.inbox),
		})
	}
	return snapshots
}
