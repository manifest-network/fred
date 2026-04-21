package docker

import (
	"context"
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
// the cancel func for preemption. Sent by the public Provision shim before
// the async goroutine is spawned, so the SM is ready before any completion
// event arrives.
type provisionRequestedMsg struct {
	cancel context.CancelFunc
}

func (provisionRequestedMsg) isLeaseMessage()         {}
func (provisionRequestedMsg) doneChan() chan struct{} { return nil }

// provisionCompletedMsg is sent by the doProvision goroutine on success.
// Drives the Provisioning→Ready transition + Success callback.
type provisionCompletedMsg struct{}

func (provisionCompletedMsg) isLeaseMessage()         {}
func (provisionCompletedMsg) doneChan() chan struct{} { return nil }

// provisionErroredMsg is sent by the doProvision goroutine on failure.
// callbackErr is the hardcoded on-chain-safe message; err is the rich
// diagnostic (stashed in provision.LastError by doProvision's defer).
type provisionErroredMsg struct {
	callbackErr string
}

func (provisionErroredMsg) isLeaseMessage()         {}
func (provisionErroredMsg) doneChan() chan struct{} { return nil }

// restartRequestedMsg / updateRequestedMsg fire the Ready|Failed → Restarting
// or → Updating transition and register the goroutine's cancel func so
// preemption by Deprovision can abort the in-flight work.
type restartRequestedMsg struct {
	cancel context.CancelFunc
}

func (restartRequestedMsg) isLeaseMessage()         {}
func (restartRequestedMsg) doneChan() chan struct{} { return nil }

type updateRequestedMsg struct {
	cancel context.CancelFunc
}

func (updateRequestedMsg) isLeaseMessage()         {}
func (updateRequestedMsg) doneChan() chan struct{} { return nil }

// replaceCompletedMsg / replaceFailedMsg fire the Restarting|Updating → Ready
// or → Failed transition after the goroutine finishes. The goroutine reads
// the final provision.Status to decide which one to send — so rollback
// cases that restored Ready are correctly reported as Completed even
// though the caller's err was non-nil.
type replaceCompletedMsg struct{}

func (replaceCompletedMsg) isLeaseMessage()         {}
func (replaceCompletedMsg) doneChan() chan struct{} { return nil }

type replaceFailedMsg struct{}

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
	// provisionCancel mirrors diagCancel for the Provision flow. Set when
	// the doProvision goroutine is spawned; called by Provisioning.OnExit
	// on DeprovisionRequested preemption.
	provisionCancel context.CancelFunc
}

// Bounded inbox: full inbox blocks senders so Docker event bursts cannot
// grow memory without bound.
const leaseActorInboxSize = 16

func newLeaseActor(b *Backend, leaseUUID string) *leaseActor {
	return &leaseActor{
		leaseUUID: leaseUUID,
		backend:   b,
		inbox:     make(chan leaseMessage, leaseActorInboxSize),
		done:      make(chan struct{}),
	}
}

func (a *leaseActor) run() {
	defer close(a.done)
	defer a.drainInbox()
	for {
		// If shutdown fired before this iteration, exit before Go's select
		// has a chance to pick a ready inbox over a ready stopCtx (select
		// randomises among ready cases). Shutdown racing *inside* the select
		// below can still process one more message; that window is not
		// reachable from the production event loop, which exits first.
		if a.backend.stopCtx.Err() != nil {
			return
		}
		select {
		case <-a.backend.stopCtx.Done():
			return
		case msg := <-a.inbox:
			a.handle(msg)
		}
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
	defer func() {
		if ch := msg.doneChan(); ch != nil {
			close(ch)
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
		a.handleProvisionRequested(m.cancel)
	case provisionCompletedMsg:
		a.handleProvisionCompleted()
	case provisionErroredMsg:
		a.handleProvisionErrored(m.callbackErr)
	case restartRequestedMsg:
		a.handleRestartRequested(m.cancel)
	case updateRequestedMsg:
		a.handleUpdateRequested(m.cancel)
	case replaceCompletedMsg:
		a.handleReplaceCompleted()
	case replaceFailedMsg:
		a.handleReplaceFailed()
	default:
		a.backend.logger.Warn("lease actor: unknown message type",
			"lease_uuid", a.leaseUUID,
		)
	}
}

func (a *leaseActor) handleContainerDied(containerID string) {
	if a.sm == nil {
		a.sm = newLeaseSM(a)
	}
	_ = a.sm.Fire(a.backend.stopCtx, evContainerDied, containerID)
}

func (a *leaseActor) handleDiagGathered(result diagResult) {
	if a.sm == nil {
		a.sm = newLeaseSM(a)
	}
	// If we're no longer in Failing (e.g., Deprovision preempted), the SM's
	// Ignore declarations on Failed/Deprovisioning drop this event; Fire
	// returns an unhandled-trigger error we can safely discard.
	_ = a.sm.Fire(a.backend.stopCtx, evDiagGathered, result)
}

// handleProvisionRequested initializes the SM in Provisioning (or transitions
// from Failed on retry) and records the cancel func. Runs before the async
// goroutine is observable in the inbox, so subsequent ProvisionCompleted /
// ProvisionErrored messages land in a correctly-initialized SM.
func (a *leaseActor) handleProvisionRequested(cancel context.CancelFunc) {
	a.provisionCancel = cancel
	if a.sm == nil {
		a.sm = newLeaseSM(a)
	}
	_ = a.sm.Fire(a.backend.stopCtx, evProvisionRequested)
}

func (a *leaseActor) handleProvisionCompleted() {
	if a.sm == nil {
		a.sm = newLeaseSM(a)
	}
	_ = a.sm.Fire(a.backend.stopCtx, evProvisionCompleted)
}

func (a *leaseActor) handleProvisionErrored(callbackErr string) {
	if a.sm == nil {
		a.sm = newLeaseSM(a)
	}
	_ = a.sm.Fire(a.backend.stopCtx, evProvisionErrored, callbackErr)
}

func (a *leaseActor) handleRestartRequested(cancel context.CancelFunc) {
	a.provisionCancel = cancel
	if a.sm == nil {
		a.sm = newLeaseSM(a)
	}
	_ = a.sm.Fire(a.backend.stopCtx, evRestartRequested)
}

func (a *leaseActor) handleUpdateRequested(cancel context.CancelFunc) {
	a.provisionCancel = cancel
	if a.sm == nil {
		a.sm = newLeaseSM(a)
	}
	_ = a.sm.Fire(a.backend.stopCtx, evUpdateRequested)
}

func (a *leaseActor) handleReplaceCompleted() {
	if a.sm == nil {
		a.sm = newLeaseSM(a)
	}
	_ = a.sm.Fire(a.backend.stopCtx, evReplaceCompleted)
}

func (a *leaseActor) handleReplaceFailed() {
	if a.sm == nil {
		a.sm = newLeaseSM(a)
	}
	_ = a.sm.Fire(a.backend.stopCtx, evReplaceFailed)
}

// send enqueues a message. Blocks when the inbox is full (backpressure).
// Returns false if the backend is shutting down.
func (a *leaseActor) send(msg leaseMessage) bool {
	// If shutdown already happened, refuse before the select — otherwise
	// Go's random choice between a ready stopCtx and a free inbox slot could
	// still queue a message post-shutdown.
	if a.backend.stopCtx.Err() != nil {
		return false
	}
	select {
	case <-a.backend.stopCtx.Done():
		return false
	case a.inbox <- msg:
		return true
	}
}

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
	b.wg.Go(candidate.run)
	return candidate
}
