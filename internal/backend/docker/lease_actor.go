package docker

import (
	"context"
	"fmt"
	"runtime/debug"
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

// provisionRequestedMsg initializes the SM in Provisioning state and stores
// the cancel func + done channel for preemption. Sent by the public Provision
// shim before the async goroutine is spawned, so the SM is ready before any
// completion event arrives. The `done` channel is closed by the goroutine on
// exit, letting Provisioning.OnExit wait for the goroutine before the actor
// proceeds into Deprovisioning (closing the orphan-containers race where the
// goroutine has created containers but not yet published their IDs).
type provisionRequestedMsg struct {
	cancel context.CancelFunc
	done   chan struct{}
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
// action writes into provision.LastError.
type provisionErroredMsg struct {
	callbackErr string
	lastError   string
}

func (provisionErroredMsg) isLeaseMessage()         {}
func (provisionErroredMsg) doneChan() chan struct{} { return nil }

// restartRequestedMsg / updateRequestedMsg fire the Ready|Failed → Restarting
// or → Updating transition and register the goroutine's cancel func + done
// channel. The done channel is closed by the goroutine's outermost defer;
// Restarting/Updating.OnExit waits on it so a preempting Deprovision observes
// a post-cleanup container set, not a mid-flight window.
type restartRequestedMsg struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (restartRequestedMsg) isLeaseMessage()         {}
func (restartRequestedMsg) doneChan() chan struct{} { return nil }

type updateRequestedMsg struct {
	cancel context.CancelFunc
	done   chan struct{}
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

// leaseActor owns all state transitions for a single lease. Messages are
// processed serially from inbox, so handlers never race with themselves.
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
	// diagDone is closed by gatherDiagAsync's defer on exit. Failing.OnExit
	// and the actor's run loop (drainOnShutdown) wait on it to make sure
	// the goroutine has finished before the actor proceeds — closing the
	// narrow shutdown race where diag's sendTerminal would otherwise land
	// after drainOnShutdown had already exited.
	diagDone chan struct{}
	// workCancel mirrors diagCancel for the Provision/Restart/Update flows.
	// Set when the work goroutine is spawned; called by
	// Provisioning/Restarting/Updating.OnExit on DeprovisionRequested
	// preemption.
	workCancel context.CancelFunc
	// workDone is closed by the in-flight work goroutine (provision,
	// restart, or update) on exit. Provisioning/Restarting/Updating.OnExit
	// waits on it (bounded) before returning so a preempting doDeprovision
	// sees either pre-published ContainerIDs or the cleanup result of the
	// goroutine's defer — not an empty snapshot taken mid-flight. Also
	// used by the actor's run() loop on shutdown to let the in-flight
	// goroutine deliver its terminal SM event before the actor exits.
	workDone chan struct{}
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
	//   1. Delete runs FIRST — concurrent actorFor calls immediately start
	//      creating a fresh actor instead of reusing this exiting one.
	//   2. close(a.done) runs SECOND — from here on, send/sendTerminal see
	//      hasExited==true and refuse deterministically via their pre-check.
	//   3. drainInbox runs LAST — catches any message that raced into the
	//      inbox between Delete and close(a.done). Content is dropped (we
	//      can't handle it; run has returned), but doneChans are closed so
	//      synchronous callers don't hang.
	defer a.drainInbox()
	defer close(a.done)
	defer a.backend.actors.Delete(a.leaseUUID)
	for {
		if a.terminated {
			return
		}
		// On shutdown, the actor does NOT exit immediately. It first waits
		// for any in-flight work goroutine (provision/restart/update) to
		// finish and drains the inbox so the SM records terminal outcomes
		// before the actor is torn down. This closes the bug_004 window
		// where a successful goroutine delivered its terminal event to an
		// actor that had already exited on stopCtx, leaving releaseStore /
		// callback state out of sync with the physical host.
		select {
		case msg := <-a.inbox:
			a.handle(msg)
		case <-a.backend.stopCtx.Done():
			a.drainOnShutdown()
			return
		}
	}
}

// drainOnShutdown waits for the in-flight async work (provision/restart/
// update goroutine OR diag goroutine) to deliver its terminal SM event,
// then processes any queued messages so the SM records every observable
// outcome before the actor exits. Bounded so a wedged goroutine can't
// block shutdown indefinitely.
//
// At most one of workDone / diagDone is non-nil at a time (they
// correspond to mutually-exclusive SM states), but the code checks both
// defensively so a future state-machine addition doesn't silently skip a
// wait.
func (a *leaseActor) drainOnShutdown() {
	a.waitForAsync(a.workDone, "work")
	a.waitForAsync(a.diagDone, "diag")
	// Drain whatever landed in the inbox — typically the goroutine's
	// terminal SM event, plus any messages queued between stopCtx firing
	// and drain beginning.
	for {
		select {
		case msg := <-a.inbox:
			a.handle(msg)
			if a.terminated {
				return
			}
		default:
			return
		}
	}
}

// waitForAsync blocks on a per-goroutine done channel (nil is a no-op),
// bounded by workExitWaitTimeout. Shared by drainOnShutdown so both the
// work-goroutine and diag-goroutine waits use identical semantics and
// logging.
func (a *leaseActor) waitForAsync(done <-chan struct{}, kind string) {
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-time.After(workExitWaitTimeout):
		a.backend.logger.Warn("actor shutdown drain: async goroutine did not exit within timeout",
			"lease_uuid", a.leaseUUID,
			"kind", kind,
			"timeout", workExitWaitTimeout,
		)
	}
}

// drainInbox closes done channels on any remaining messages so synchronous
// callers blocked on completion don't hang after shutdown.
func (a *leaseActor) drainInbox() {
	for {
		select {
		case msg := <-a.inbox:
			if ch := msg.doneChan(); ch != nil {
				close(ch)
			}
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
		a.handleProvisionRequested(m.cancel, m.done)
	case restartRequestedMsg:
		a.handleRestartRequested(m.cancel, m.done)
	case updateRequestedMsg:
		a.handleUpdateRequested(m.cancel, m.done)
	case provisionCompletedMsg:
		a.handleProvisionCompleted(m.result)
	case provisionErroredMsg:
		a.handleProvisionErrored(m.callbackErr, m.lastError)
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
// Failed on retry) and records the cancel func + done channel. Runs before
// the async goroutine is observable in the inbox, so subsequent
// ProvisionCompleted / ProvisionErrored messages land in a correctly-
// positioned SM.
func (a *leaseActor) handleProvisionRequested(cancel context.CancelFunc, done chan struct{}) {
	a.workCancel = cancel
	a.workDone = done
	_ = a.sm.Fire(a.backend.stopCtx, evProvisionRequested)
}

func (a *leaseActor) handleProvisionCompleted(result provisionSuccessResult) {
	_ = a.sm.Fire(a.backend.stopCtx, evProvisionCompleted, result)
}

func (a *leaseActor) handleProvisionErrored(callbackErr, lastError string) {
	_ = a.sm.Fire(a.backend.stopCtx, evProvisionErrored, provisionErrorInfo{
		callbackErr: callbackErr,
		lastError:   lastError,
	})
}

func (a *leaseActor) handleRestartRequested(cancel context.CancelFunc, done chan struct{}) {
	a.workCancel = cancel
	a.workDone = done
	_ = a.sm.Fire(a.backend.stopCtx, evRestartRequested)
}

func (a *leaseActor) handleUpdateRequested(cancel context.CancelFunc, done chan struct{}) {
	a.workCancel = cancel
	a.workDone = done
	_ = a.sm.Fire(a.backend.stopCtx, evUpdateRequested)
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
// Use this for NEW work originating from external API paths (Provision,
// Deprovision, Restart, Update, containerEventLoop). On shutdown these
// paths should refuse new requests; the false return is the signal.
//
// For TERMINAL events delivered by in-flight work goroutines — whose
// physical work has already happened on the host and MUST be recorded by
// the SM — use sendTerminal instead.
//
// The hasExited pre-check + a.done select arm give the same deterministic
// refusal semantics as sendTerminal: if the actor has exited (run loop
// returned, b.actors.Delete fired), subsequent sends through a
// still-referenced actor pointer refuse rather than queue into an inbox
// nobody will drain.
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
// The hasExited fast-path is required for correctness: once a.done is
// closed AND the inbox has space, Go's select picks non-deterministically
// between `a.inbox <- msg` and `<-a.done`. Without the pre-check,
// post-shutdown sendTerminal would succeed half the time, queueing into
// an inbox nobody will drain — a silent drop returning true. A narrow
// race remains if a.done closes *between* hasExited and the select, but
// that window is microseconds, and within that window the message is
// still caught by drainOnShutdown's final inbox pass.
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

// actorFor returns the lease actor for leaseUUID, creating and starting it on
// first access. Concurrent callers see the same actor; the losing goroutine
// discards its allocation.
func (b *Backend) actorFor(leaseUUID string) *leaseActor {
	if existing, ok := b.actors.Load(leaseUUID); ok {
		return existing.(*leaseActor)
	}
	candidate := newLeaseActor(b, leaseUUID)
	actual, loaded := b.actors.LoadOrStore(leaseUUID, candidate)
	if loaded {
		return actual.(*leaseActor)
	}
	leaseActorsCreatedTotal.Inc()
	b.wg.Go(candidate.run)
	return candidate
}

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
// actorMetricsSampleLoop. Safe to run concurrently with actor work:
// inbox len() is racy-but-fine, and currentMessageStart is atomic.
func (b *Backend) sampleActorMetrics() {
	now := time.Now().UnixNano()
	var oldestStart int64
	b.actors.Range(func(_, value any) bool {
		actor, ok := value.(*leaseActor)
		if !ok {
			return true
		}
		leaseActorInboxDepth.Observe(float64(len(actor.inbox)))
		if start := actor.currentMessageStart.Load(); start != 0 {
			if oldestStart == 0 || start < oldestStart {
				oldestStart = start
			}
		}
		return true
	})
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
	var snapshots []ActorSnapshot
	b.actors.Range(func(key, value any) bool {
		leaseUUID, ok := key.(string)
		if !ok {
			return true
		}
		actor, ok := value.(*leaseActor)
		if !ok {
			return true
		}
		snap := ActorSnapshot{
			LeaseUUID:  leaseUUID,
			SMState:    fmt.Sprintf("%v", actor.sm.State()),
			InboxDepth: len(actor.inbox),
			InboxCap:   cap(actor.inbox),
		}
		snapshots = append(snapshots, snap)
		return true
	})
	return snapshots
}
