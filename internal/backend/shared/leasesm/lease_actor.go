package leasesm

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backend/shared/workbarrier"
)

// errActorTerminated is returned on the ack/reply channel when a
// caller-facing request (Provision/Restart/Update) arrives at an actor
// that has already terminated via handleDeprovision but whose
// removeFromRegistry defer hasn't yet fired. The caller (Backend.X)
// rolls back its pre-routing state via its error path (e.g.,
// removeProvision); a subsequent retry resolves-or-creates a fresh
// actor through actorForLocked. Without this check, such messages
// would hit SM.Ignore in Deprovisioning state and Fire would return
// nil — the handler would then ack success and spawn a worker under
// a terminated actor, wedging the lease.
var errActorTerminated = errors.New("lease actor terminated; retry will create a fresh actor")

// ErrWorkerDrainTimeout means a state transition could not prove that the
// worker owning the source state had stopped. Callers must treat the outcome as
// still in flight: in particular, deprovision must not tear substrate state
// down or report success while that worker may still publish a later write.
var ErrWorkerDrainTimeout = errors.New("lease mutation worker did not drain before the safety deadline")

type LeaseMessage interface {
	isLeaseMessage()
	// doneChan returns the channel to close when processing finishes, or nil
	// for fire-and-forget messages. Lifting this out of the handler dispatch
	// lets shutdown drain pending messages without a per-type switch.
	doneChan() chan struct{}
	// onPanic is called by the actor's recover when a message's handler
	// panics. Messages with reply/ack channels must non-blocking-send an
	// error here so their caller (Backend.Deprovision / Provision /
	// Restart / Update blocking on the channel) unblocks instead of
	// hanging until ctx cancellation. Messages without a caller to
	// unblock implement this as a no-op.
	onPanic(err error)
}

// workerTerminalMessage is the closed set of messages produced by actor-owned
// workers. Retirement may still apply these messages after external admission
// has closed: they settle work that was accepted before retirement. Keeping the
// distinction in the type system prevents a future caller-facing command from
// being accidentally executed by the post-worker shutdown drain.
type workerTerminalMessage interface {
	LeaseMessage
	isWorkerTerminalMessage()
}

// ContainerDiedMsg signals a container belonging to this lease has died.
// If Done is non-nil, the actor closes it after the message is processed,
// letting synchronous callers block until completion. Exported so the
// substrate's container event loop can construct values and route them
// through the actor's inbox.
type ContainerDiedMsg struct {
	ContainerID string
	Done        chan struct{}
}

func (ContainerDiedMsg) isLeaseMessage()           {}
func (m ContainerDiedMsg) doneChan() chan struct{} { return m.Done }

// onPanic is a no-op: the caller (if any) unblocks via the done
// channel which is closed by handle()'s defer regardless of panic.
func (ContainerDiedMsg) onPanic(error) {}

// DeprovisionMsg requests that the actor run the Deprovision flow. The
// Reply channel receives the outcome; doneChan is always nil because
// callers block on Reply instead. Exported so the substrate's Deprovision
// shim can construct values and route them through the actor's inbox.
type DeprovisionMsg struct {
	Ctx   context.Context
	Reply chan error
}

func (DeprovisionMsg) isLeaseMessage()         {}
func (DeprovisionMsg) doneChan() chan struct{} { return nil }
func (m DeprovisionMsg) onPanic(err error) {
	// Non-blocking send: Reply is buffered-1, caller receives at most
	// once. On recover, make sure the caller gets something.
	select {
	case m.Reply <- err:
	default:
	}
}

// CohortDivergedMsg reports that recovery observed a running instance cohort
// which does not match the durable desired release. It deliberately carries no
// caller-authored error string: the state machine owns the fixed, tenant-safe
// failure reason and callback message. Reply must be buffered by one, matching
// DeprovisionMsg, so recovery can wait until both state mutation and durable
// lifecycle callback enqueue have completed.
type CohortDivergedMsg struct {
	Ctx   context.Context
	Reply chan error
}

func (CohortDivergedMsg) isLeaseMessage()         {}
func (CohortDivergedMsg) doneChan() chan struct{} { return nil }
func (m CohortDivergedMsg) onPanic(err error) {
	select {
	case m.Reply <- err:
	default:
	}
}

// maintenanceRecoveryOutcome is deliberately private. Docker recovery can
// construct only one of the four valid messages below, so an invalid
// success/failure/projection combination cannot cross the actor boundary.
type maintenanceRecoveryOutcome uint8

const (
	maintenanceRecoveredSuccess maintenanceRecoveryOutcome = iota + 1
	maintenanceRecoveredFailureReady
	maintenanceRecoveredFailureFailed
	maintenanceRecoveredSuccessRuntimeFailed
)

// MaintenanceRecoveryProjection contains only substrate observations. Runtime
// identity, routes, desired items and manifest are deliberately absent: the
// typed constructors derive those authorities from claim.TargetRelease(), so a
// caller cannot redirect or rewrite a durable target through actor recovery.
type MaintenanceRecoveryProjection struct {
	ContainerIDs      []string
	ServiceContainers map[string][]string
}

// maintenanceRecoveredMsg converges a stale Restarting/Updating actor after
// the exact durable maintenance Release has already reached a terminal state.
// Its fields are private so callers cannot forge an outcome or bypass the
// MaintenanceID fence; use one of the typed constructors below.
//
// The actor suppresses callback delivery for this message. Recovery resolves
// the durable MaintenanceIntent only after the projection transition and
// pending-route cleanup succeed, preserving Release -> actor -> outbox order.
type maintenanceRecoveredMsg struct {
	leaseUUID     string
	maintenanceID shared.MaintenanceID
	outcome       maintenanceRecoveryOutcome
	success       ReplaceSuccessResult
	failure       ReplaceFailureInfo
	reply         chan error
}

func (maintenanceRecoveredMsg) isLeaseMessage()         {}
func (maintenanceRecoveredMsg) doneChan() chan struct{} { return nil }
func (m maintenanceRecoveredMsg) onPanic(err error) {
	select {
	case m.reply <- err:
	default:
	}
}

// NewMaintenanceRecoveredSuccessMsg constructs the only recovery message that
// may promote the pending target route and workload projection.
func NewMaintenanceRecoveredSuccessMsg(
	claim shared.MaintenanceIntentClaim,
	projection MaintenanceRecoveryProjection,
	reply chan error,
) (LeaseMessage, error) {
	result, err := validateMaintenanceRecoveryProjection(claim, projection, true)
	if err != nil {
		return nil, err
	}
	if reply == nil {
		return nil, errors.New("maintenance recovery requires a reply channel")
	}
	return maintenanceRecoveredMsg{
		leaseUUID:     claim.LeaseUUID(),
		maintenanceID: claim.MaintenanceID(),
		outcome:       maintenanceRecoveredSuccess,
		success:       result,
		reply:         reply,
	}, nil
}

// NewMaintenanceRecoveredFailureReadyMsg constructs a failed-maintenance
// recovery whose exact source cohort was independently proven Ready.
func NewMaintenanceRecoveredFailureReadyMsg(
	claim shared.MaintenanceIntentClaim,
	projection MaintenanceRecoveryProjection,
	info ReplaceFailureInfo,
	reply chan error,
) (LeaseMessage, error) {
	return newMaintenanceRecoveredFailureMsg(
		claim, maintenanceRecoveredFailureReady, projection, info, reply,
	)
}

// NewMaintenanceRecoveredFailureFailedMsg constructs a failed-maintenance
// recovery for every case where an exact Ready source cohort was not proven.
func NewMaintenanceRecoveredFailureFailedMsg(
	claim shared.MaintenanceIntentClaim,
	projection MaintenanceRecoveryProjection,
	info ReplaceFailureInfo,
	reply chan error,
) (LeaseMessage, error) {
	return newMaintenanceRecoveredFailureMsg(
		claim, maintenanceRecoveredFailureFailed, projection, info, reply,
	)
}

// NewMaintenanceRecoveredRuntimeFailureMsg constructs the compound recovery
// outcome for an exact committed target whose runtime cohort is definitively
// divergent. Applying target authority and failing the projection is one actor
// transition, so a deferred outbox settlement cannot oscillate Ready/Failed or
// replay ordinary transition side effects.
func NewMaintenanceRecoveredRuntimeFailureMsg(
	claim shared.MaintenanceIntentClaim,
	projection MaintenanceRecoveryProjection,
	reply chan error,
) (LeaseMessage, error) {
	result, err := validateMaintenanceRecoveryProjection(claim, projection, true)
	if err != nil {
		return nil, err
	}
	if reply == nil {
		return nil, errors.New("maintenance recovery requires a reply channel")
	}
	return maintenanceRecoveredMsg{
		leaseUUID:     claim.LeaseUUID(),
		maintenanceID: claim.MaintenanceID(),
		outcome:       maintenanceRecoveredSuccessRuntimeFailed,
		success:       result,
		failure: ReplaceFailureInfo{
			Operation:   string(claim.Kind()),
			CallbackErr: ErrMsgCohortDiverged,
			Reason:      backend.ReasonInternal,
			LastError:   ErrMsgCohortDiverged,
		},
		reply: reply,
	}, nil
}

func newMaintenanceRecoveredFailureMsg(
	claim shared.MaintenanceIntentClaim,
	outcome maintenanceRecoveryOutcome,
	projection MaintenanceRecoveryProjection,
	info ReplaceFailureInfo,
	reply chan error,
) (LeaseMessage, error) {
	result, err := validateMaintenanceRecoveryProjection(claim, projection, false)
	if err != nil {
		return nil, err
	}
	if reply == nil {
		return nil, errors.New("maintenance recovery requires a reply channel")
	}
	info.PreserveMaintenance = true
	return maintenanceRecoveredMsg{
		leaseUUID:     claim.LeaseUUID(),
		maintenanceID: claim.MaintenanceID(),
		outcome:       outcome,
		success:       result,
		failure:       info,
		reply:         reply,
	}, nil
}

func validateMaintenanceRecoveryProjection(
	claim shared.MaintenanceIntentClaim,
	projection MaintenanceRecoveryProjection,
	requireTargetRoute bool,
) (ReplaceSuccessResult, error) {
	if !claim.Valid() {
		return ReplaceSuccessResult{}, errors.New("maintenance recovery requires an exact intent claim")
	}
	serviceContainers := make(map[string][]string, len(projection.ServiceContainers))
	for service, ids := range projection.ServiceContainers {
		serviceContainers[service] = append([]string(nil), ids...)
	}
	result := ReplaceSuccessResult{
		ContainerIDs:                  append([]string(nil), projection.ContainerIDs...),
		ServiceContainers:             serviceContainers,
		suppressMaintenanceSettlement: true,
	}
	if !requireTargetRoute {
		return result, nil
	}
	target := claim.TargetRelease()
	if target.RuntimeAuthority == nil {
		return ReplaceSuccessResult{}, errors.New("maintenance recovery target has no runtime authority")
	}
	stack, err := manifest.ParsePayload(target.Manifest)
	if err != nil {
		return ReplaceSuccessResult{}, fmt.Errorf("maintenance recovery target manifest: %w", err)
	}
	items := append([]backend.LeaseItem(nil), target.Items...)
	resourceProfiles := shared.CloneSKUResourceSnapshot(target.ResourceProfiles)
	authority := target.RuntimeAuthority
	result.OnSuccess = func(state *ProvisionState) {
		state.Tenant = authority.Tenant()
		state.ProviderUUID = authority.ProviderUUID()
		state.Items = append([]backend.LeaseItem(nil), items...)
		state.ResourceProfiles = shared.CloneSKUResourceSnapshot(resourceProfiles)
		state.StackManifest = stack
	}
	result.applyRecoveredRuntimeAuthority = true
	result.recoveredCallbackURL = authority.CallbackURL()
	result.recoveredLifecycleCallbackURL = authority.LifecycleCallbackURL()
	return result, nil
}

// diagGatheredMsg is sent by the async diag goroutine when it finishes.
// Carries the gather output into the Failing→Failed transition.
type diagGatheredMsg struct {
	result diagResult
}

func (diagGatheredMsg) isLeaseMessage()          {}
func (diagGatheredMsg) doneChan() chan struct{}  { return nil }
func (diagGatheredMsg) onPanic(error)            {} // no caller to unblock
func (diagGatheredMsg) isWorkerTerminalMessage() {}

// ProvisionRequestedMsg asks the actor to drive a provision flow. Carries
// the Cancel func (stored on the actor so Provisioning.OnExit can preempt)
// and a Work closure containing everything doProvision needs — the actor
// doesn't need to know the arg shapes. After firing the SM
// transition, the actor sends the Fire result on Ack and spawns a worker
// (tracked by workers) to run Work. Backend.Provision blocks on Ack so it
// knows whether the SM accepted the transition before returning. Exported
// so the substrate's Provision shim can construct values and route them
// through the actor's inbox.
type ProvisionRequestedMsg struct {
	Cancel context.CancelFunc
	Work   func() (callbackErr string, reason backend.Reason, result ProvisionSuccessResult, failureLogs map[string]string, err error)
	Ack    chan error
}

func (ProvisionRequestedMsg) isLeaseMessage()         {}
func (ProvisionRequestedMsg) doneChan() chan struct{} { return nil }
func (m ProvisionRequestedMsg) onPanic(err error) {
	select {
	case m.Ack <- err:
	default:
	}
}

// provisionCompletedMsg is sent by the doProvision goroutine on success.
// Drives the Provisioning→Ready transition. Carries the result data
// (containerIDs, manifest, stackManifest, serviceContainers) that the
// Ready entry action writes into the provision struct.
type provisionCompletedMsg struct {
	result ProvisionSuccessResult
}

func (provisionCompletedMsg) isLeaseMessage()          {}
func (provisionCompletedMsg) doneChan() chan struct{}  { return nil }
func (provisionCompletedMsg) onPanic(error)            {} // no caller to unblock
func (provisionCompletedMsg) isWorkerTerminalMessage() {}

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
	reason      backend.Reason // ENG-508
	lastError   string
	logs        map[string]string
}

func (provisionErroredMsg) isLeaseMessage()          {}
func (provisionErroredMsg) doneChan() chan struct{}  { return nil }
func (provisionErroredMsg) onPanic(error)            {} // no caller to unblock
func (provisionErroredMsg) isWorkerTerminalMessage() {}

// RestartRequestedMsg / UpdateRequestedMsg carry a Cancel func + Work
// closure + Ack chan, analogous to ProvisionRequestedMsg. The Work closure
// returns a ReplaceResult consumed by the actor to pick the right terminal
// SM event (completed / recovered / failed). Exported so the substrate's
// Restart / Update shims can construct values and route them through the
// actor's inbox.
type RestartRequestedMsg struct {
	Cancel context.CancelFunc
	Work   func() ReplaceResult
	Ack    chan error
	// The callback pair remains pending until replacement succeeds. New
	// containers and the completion callback use it immediately, but failed
	// maintenance leaves the committed runtime pair unchanged.
	CallbackURL          string
	LifecycleCallbackURL string
	Maintenance          shared.MaintenanceIntentClaim
}

func (RestartRequestedMsg) isLeaseMessage()         {}
func (RestartRequestedMsg) doneChan() chan struct{} { return nil }
func (m RestartRequestedMsg) onPanic(err error) {
	select {
	case m.Ack <- err:
	default:
	}
}

type UpdateRequestedMsg struct {
	Cancel context.CancelFunc
	Work   func() ReplaceResult
	Ack    chan error
	// The callback pair remains pending until replacement succeeds. New
	// containers and the completion callback use it immediately, but failed
	// maintenance leaves the committed runtime pair unchanged.
	CallbackURL          string
	LifecycleCallbackURL string
	Maintenance          shared.MaintenanceIntentClaim
}

func (UpdateRequestedMsg) isLeaseMessage()         {}
func (UpdateRequestedMsg) doneChan() chan struct{} { return nil }
func (m UpdateRequestedMsg) onPanic(err error) {
	select {
	case m.Ack <- err:
	default:
	}
}

// RestoreRequestedMsg drives a restore (ENG-325) through the EXISTING
// replace machinery — same Cancel/Work/Ack/callback-pair shape as
// RestartRequestedMsg. The difference is purely the SM event it fires:
// evRestoreRequested, which is permitted only from Provisioning (a
// restore's new lease is reserved there), versus evRestartRequested
// from Ready/Failed. The Work closure returns a ReplaceResult consumed
// by spawnReplaceWorker exactly as for restart/update. Exported so the
// substrate's Restore shim can construct values and route them through
// the actor's inbox.
type RestoreRequestedMsg struct {
	Cancel context.CancelFunc
	Work   func() ReplaceResult
	Ack    chan error
	// The callback pair is applied to the provision state by
	// onEnterRestarting (reused for restore) before the actor acks — the
	// actor, not the HTTP prelude, is the sole writer of those fields.
	CallbackURL          string
	LifecycleCallbackURL string
}

func (RestoreRequestedMsg) isLeaseMessage()         {}
func (RestoreRequestedMsg) doneChan() chan struct{} { return nil }
func (m RestoreRequestedMsg) onPanic(err error) {
	select {
	case m.Ack <- err:
	default:
	}
}

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
	result        ReplaceSuccessResult
	maintenanceID shared.MaintenanceID
}

func (replaceCompletedMsg) isLeaseMessage()          {}
func (replaceCompletedMsg) doneChan() chan struct{}  { return nil }
func (replaceCompletedMsg) onPanic(error)            {} // no caller to unblock
func (replaceCompletedMsg) isWorkerTerminalMessage() {}

type replaceRecoveredMsg struct {
	info          ReplaceFailureInfo
	maintenanceID shared.MaintenanceID
}

func (replaceRecoveredMsg) isLeaseMessage()          {}
func (replaceRecoveredMsg) doneChan() chan struct{}  { return nil }
func (replaceRecoveredMsg) onPanic(error)            {} // no caller to unblock
func (replaceRecoveredMsg) isWorkerTerminalMessage() {}

type replaceFailedMsg struct {
	info          ReplaceFailureInfo
	maintenanceID shared.MaintenanceID
}

func (replaceFailedMsg) isLeaseMessage()          {}
func (replaceFailedMsg) doneChan() chan struct{}  { return nil }
func (replaceFailedMsg) onPanic(error)            {} // no caller to unblock
func (replaceFailedMsg) isWorkerTerminalMessage() {}

// LeaseActor owns all state transitions and async work for a single lease.
//
// One concept: the actor is the scope of atomicity for its messages and
// its workers. Everything else falls out of that:
//
//   - Registry atomicity — the substrate's actor registry (e.g. docker's
//     b.actors) is guarded by the substrate's mutex; routeToLease resolves-
//     or-creates AND enqueues under that mutex; removeFromRegistry (on
//     actor exit, via cfg.OnTerminated) deletes under the same mutex.
//     External callers never hold a *LeaseActor pointer, so stale-
//     pointer races are unreachable by construction.
//
//   - Worker ownership — every worker goroutine (provision, restart,
//     update, diag) is spawned by the actor (via spawnProvisionWorker /
//     spawnReplaceWorker / onEnterFailing's goroutine) and tracked by
//     workers. Retirement stops external admission, waits for workers, and
//     applies their terminal events before registry deletion. The wait is
//     bounded by WorkerDrainTimeout. A timed-out state transition is refused
//     before substrate teardown; a timed-out backend shutdown is surfaced by
//     Backend.Stop and the process exits uncleanly.
//
//   - Typed retirement drain — worker-terminal messages still drive their SM
//     transitions, while queued caller commands are rejected and unblocked.
//     The workerTerminalMessage marker prevents a future change from accidentally
//     spawning new work after the retirement barrier.
//
//   - Non-blocking routing — routeToLease does a non-blocking send
//     under the registry mutex. A wedged actor cannot stall the
//     event-loop or other routing callers; refusals are counted in
//     die_event_dropped_total and the reconciler re-detects.
//
// Messages are processed serially from inbox, so handlers never race
// with themselves. SM transitions are synchronous inside handle() —
// Fire dispatches OnExit / OnEntry in the actor's own goroutine.
//
// Exported as `LeaseActor` so the closure-builder NewLeaseActor can be
// called from substrate packages and they can hold pointers into their
// registries.
type LeaseActor struct {
	leaseUUID string
	// cfg holds the substrate-agnostic dependencies the SM/actor consume.
	// Set by NewLeaseActor via the supplied buildCfg closure; immutable
	// after construction. All reach-back into substrate state goes
	// through these closures — the actor holds no substrate pointer of
	// its own.
	cfg   LeaseActorConfig
	inbox chan LeaseMessage
	done  chan struct{}
	sm    *leaseSM
	// pendingDeathInfo carries the inspected instance state from the SM's
	// guard into the onEnterFailing action. Single-field handoff works
	// because the actor processes messages serially; no two messages
	// read/write this field concurrently. The service name for the
	// death-event log line rides on pendingDeathInfo.ServiceName.
	pendingDeathInfo *InstanceState
	// diagCancel is set when entering Failing (spawning the async diag
	// goroutine) and called by Failing.OnExit to signal cancellation. Any
	// transition out of Failing cancels the goroutine before a stale Failed
	// callback can be emitted.
	diagCancel context.CancelFunc
	// workCancel is set when a Provision/Restart/Update worker is spawned
	// from handleProvisionRequested / handleRestartRequested /
	// handleUpdateRequested, and called by Provisioning/Restarting/
	// Updating.OnExit on DeprovisionRequested preemption.
	workCancel context.CancelFunc
	// replaceWasActive records whether the lease was counted in
	// activeProvisions (i.e. Status == Ready) at the instant a restart/update
	// transition began — captured by onEnterRestarting/onEnterUpdating reading
	// prov.Status BEFORE overwriting it. The replace-outcome entry actions key
	// the activeProvisions gauge on THIS actor-observed (serial) value instead
	// of a prelude-captured route-time status snapshot, which is stale when an
	// intervening Ready→Failing already Dec'd the gauge (the death-before-queued-restart
	// ordering, ENG-230 / PR#93 finding #2). Single-field handoff is safe: the
	// SM permits at most one in-flight replace at a time and the actor is
	// serial — same discipline as pendingDeathInfo.
	replaceWasActive bool
	// replaceCallbackKind distinguishes provision/restore operation completion
	// from restart/update lifecycle observation. It is set by the serial actor
	// with the replace entry transition and consumed by the terminal entry
	// action, so callback classification never depends on URL inspection.
	replaceCallbackKind replaceCallbackKind
	// pendingReplaceCallbackURL and pendingReplaceLifecycleCallbackURL are the
	// requested maintenance route. Restart/update keep this pair separate from
	// ProvisionState until the substrate and active Release commit, so rollback
	// continues to emit the old committed runtime labels by construction.
	pendingReplaceCallbackURL          string
	pendingReplaceLifecycleCallbackURL string
	pendingMaintenance                 shared.MaintenanceIntentClaim
	maintenanceWorkerMu                sync.RWMutex
	maintenanceWorkerID                shared.MaintenanceID
	// workers tracks every worker goroutine spawned by this actor
	// (provision, restart, update, diag). The actor's retirement path stops
	// caller admission, waits on workers.Zero(), then applies the queued worker
	// terminal event before unregistering. The wait is bounded by
	// WorkerDrainTimeout; a transition timeout aborts before the destination
	// state or substrate teardown can commit.
	// The same barrier is used by onExit* to wait for the
	// active worker when the SM is transitioning out of a work-owning
	// state (Deprovision preempt).
	//
	// The SM enforces at-most-one-worker-at-a-time across the work-owning
	// states, so workers.Zero effectively waits for "the one worker
	// currently running"; the count happens to always be 0 or 1.
	workers *workbarrier.Barrier
	// currentMessageStart is the UnixNano timestamp of the message the
	// actor is currently processing in handle(), or 0 when idle. Used by
	// the stuck-actor sampler to detect hung handlers. Written by the
	// actor's goroutine, read atomically by the sampler goroutine.
	currentMessageStart atomic.Int64
	// terminated is set by handleDeprovision once the provision entry has
	// been fully removed. The run loop checks it after every handle() and
	// exits, allowing the actor's slot in the registry to be reused by a
	// new lease with the same UUID (a stale actor stuck in Deprovisioning
	// would otherwise Ignore evProvisionRequested and wedge the lease).
	// Read and written only on the actor's own goroutine — no atomic.
	terminated bool
	// exiting is closed by the actor's exit sequence just before its final
	// retirement drain, and observed by sendTerminal to reject late
	// worker sends that would otherwise land in the inbox during the
	// tiny window between the final drain completing and close(a.done)
	// executing. Without this signal, a late orphan worker's
	// sendTerminal (which can happen if waitForWorkers timed out)
	// would see hasExited() == false (done not yet closed), succeed
	// into the now-unmonitored inbox, and silently drop — under-
	// counting leaseTerminalEventDroppedTotal.
	//
	// Closed-channel pattern chosen over atomic.Bool so the gate
	// composes directly with sendTerminal's select, mirroring how
	// `done` works and the broader Go idiom for one-shot broadcast
	// signals (see context.Done()). exitingOnce wraps the close so
	// tests can force the signal without panicking the production
	// retirement path when it also closes the signal.
	exiting     chan struct{}
	exitingOnce sync.Once
	// admissionMu linearizes external TryEnqueue calls with retirement. The
	// substrate holds its registry mutex while calling TryEnqueue, but the actor
	// cannot take that registry mutex while beginning retirement without a lock
	// inversion. This private gate lets retirement stop admission first, release
	// the gate, and only later invoke OnTerminated to remove the actor.
	admissionMu sync.Mutex
	retiring    bool
}

// Bounded inbox: full inbox blocks senders so Docker event bursts cannot
// grow memory without bound.
const leaseActorInboxSize = 16

// NewLeaseActor constructs a lease actor from a substrate-supplied config
// builder. The builder closure receives the just-constructed *LeaseActor
// so it can build closures (e.g. OnTerminated) that close over the actor
// pointer for the "delete iff still-registered" CompareAndDelete check.
//
// CRITICAL: NewLeaseActor creates the actor AND spawns its run-loop
// goroutine; callers needing registry-spawn atomicity (so a concurrent
// resolve-or-create sees a runnable actor or no actor at all, never a
// constructed-but-unspawned half-state) must hold the registry mutex
// across the NewLeaseActor call. The Docker backend honors this by
// invoking NewLeaseActor only from actorForLocked, which holds
// b.actorsMu. K3s and other substrate implementers must apply the
// same discipline: call NewLeaseActor under whatever mutex guards the
// actor registry — otherwise a concurrent caller could observe a
// registry entry whose actor hasn't started its inbox consumer.
//
// Construction order:
//  1. Allocate the actor with all per-actor primitives (inbox, done,
//     exiting, workers).
//  2. Call buildCfg(a) to obtain the substrate-supplied LeaseActorConfig
//     and store it on the actor.
//  3. Lift LeaseUUID off the cfg onto the actor struct (it's hot enough
//     to merit avoiding the cfg dereference on every message).
//  4. Initialize the SM eagerly so any external reader (DebugActors over
//     /debug/actors) sees a non-nil pointer without synchronization on
//     a lazy-init field.
//  5. Record one ActorCreated metric event — moved here from the
//     substrate's resolve-or-create function so a single SMMetrics
//     adapter sees every actor construction regardless of substrate.
//  6. Spawn the actor's goroutine via cfg.WG.Go(a.run) — moved here for
//     the same "one place creates, one place spawns" reason. Substrate
//     code calls NewLeaseActor while holding its registry mutex, so the
//     resolve-or-create-then-enqueue invariant is preserved (spawn
//     happens before NewLeaseActor returns, before the substrate
//     releases its mutex).
//
// Most LeaseActorConfig fields do NOT depend on the actor pointer — they
// capture substrate state (e.g., a backend pointer). The only fields
// where the builder closure typically needs the actor pointer are
// OnTerminated (for the "reg == a" CompareAndDelete check). Substrate
// implementers should treat the actor pointer arg as a tool for that
// closure, not as a general-purpose handle (LeaseUUID is already in
// the cfg alongside it).
func NewLeaseActor(buildCfg func(*LeaseActor) LeaseActorConfig) *LeaseActor {
	a := &LeaseActor{
		inbox:   make(chan LeaseMessage, leaseActorInboxSize),
		done:    make(chan struct{}),
		exiting: make(chan struct{}),
		workers: workbarrier.New(),
	}
	a.cfg = buildCfg(a)
	a.leaseUUID = a.cfg.LeaseUUID
	a.sm = newLeaseSM(a)
	a.cfg.Metrics.ActorCreated()
	a.cfg.WG.Go(a.run)
	return a
}

func (a *LeaseActor) run() {
	defer a.clearAnyMaintenanceWorker()
	defer a.retire()
	for {
		if a.terminated {
			return
		}
		select {
		case msg := <-a.inbox:
			a.handle(msg)
		case <-a.cfg.StopCtx.Done():
			// Shutdown: exit the main loop. retire stops caller admission,
			// waits for in-flight workers, and applies their terminal events
			// before the actor is torn down.
			return
		}
	}
}

// retire makes actor replacement and accepted-work settlement one ordered
// operation:
//
//  1. close external admission while this actor is still registered;
//  2. reject commands accepted before the gate closed, while applying any
//     already-queued worker terminal events;
//  3. wait for the actor-owned worker, whose terminal send precedes Done;
//  4. close terminal admission and drain that final terminal event;
//  5. unregister the now-quiescent actor and close Done.
//
// The first drain is necessary even though the inbox is bounded: without it,
// accepted caller commands could fill the inbox while run is waiting for a
// worker, preventing that worker from publishing the terminal event on which
// state convergence depends. Neither drain executes caller work, so no worker
// can be spawned after the sole worker barrier.
func (a *LeaseActor) retire() {
	a.stopAdmission()
	a.drainRetiringInbox()
	_ = a.waitForWorkers()
	a.closeExiting()
	a.drainRetiringInbox()
	a.removeFromRegistry()
	close(a.done)
}

// stopAdmission linearizes with TryEnqueue. It deliberately does not invoke
// OnTerminated while holding admissionMu: routing calls TryEnqueue under the
// substrate registry mutex, so doing so would invert that lock order.
func (a *LeaseActor) stopAdmission() {
	a.admissionMu.Lock()
	a.retiring = true
	a.admissionMu.Unlock()
}

// waitForWorkers blocks until every worker goroutine this actor has
// spawned has returned. Bounded by workExitWaitTimeout so a wedged
// worker (Docker daemon hang with ctx ignored) can't pin the actor.
// If the timeout fires the error aborts an SM transition before its state
// changes. This is load-bearing for deprovision: teardown is refused while a
// prior worker may still write. The actor run-loop's shutdown defer ignores the
// same error after logging; its backend-level WaitGroup remains non-zero, so
// Backend.Stop's independent deadline forces a process-level failure instead of
// claiming a clean shutdown. workbarrier.Barrier's Zero() is a real channel, so
// the select here spawns no helper goroutine.
func (a *LeaseActor) waitForWorkers() error {
	timeout := a.cfg.WorkerDrainTimeout
	if timeout <= 0 {
		timeout = workExitWaitTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-a.workers.Zero():
		return nil
	case <-timer.C:
		a.cfg.Logger.Warn("actor waitForWorkers: worker did not exit within timeout",
			"lease_uuid", a.leaseUUID,
			"timeout", timeout,
		)
		return fmt.Errorf("%w: lease %s after %s", ErrWorkerDrainTimeout, a.leaseUUID, timeout)
	}
}

// drainRetiringInbox settles worker terminal events but explicitly rejects
// caller-facing messages that were accepted immediately before retirement.
// Running normal handlers here would permit a queued Provision/Restart/Update
// to spawn a worker after waitForWorkers, and unregistering before the drain
// would permit a replacement actor to mutate the same lease concurrently.
func (a *LeaseActor) drainRetiringInbox() {
	for {
		select {
		case msg := <-a.inbox:
			if terminal, ok := msg.(workerTerminalMessage); ok {
				a.handle(terminal)
				continue
			}
			a.rejectRetiringMessage(msg)
		default:
			return
		}
	}
}

func (a *LeaseActor) rejectRetiringMessage(msg LeaseMessage) {
	msg.onPanic(errActorTerminated)
	if ch := msg.doneChan(); ch != nil {
		close(ch)
	}
}

func (a *LeaseActor) handle(msg LeaseMessage) {
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
			a.cfg.Logger.Error("lease actor panic",
				"lease_uuid", a.leaseUUID,
				"panic", r,
				"stack", string(debug.Stack()),
			)
			a.cfg.Metrics.ActorPanic()
			// Unblock any caller waiting on this message's reply/ack
			// channel. Without this, a panic in a handler leaves
			// Backend.Deprovision / Provision / Restart / Update
			// stuck on their select{} until ctx/stopCtx cancels —
			// effectively an API hang per bad message.
			msg.onPanic(fmt.Errorf("handler panic: %v", r))
		}
	}()
	switch m := msg.(type) {
	case ContainerDiedMsg:
		a.handleContainerDied(m.ContainerID)
	case DeprovisionMsg:
		m.Reply <- a.handleDeprovision(m.Ctx)
	case CohortDivergedMsg:
		m.Reply <- a.handleCohortDiverged(m.Ctx)
	case maintenanceRecoveredMsg:
		m.reply <- a.handleMaintenanceRecovered(m)
	case diagGatheredMsg:
		a.handleDiagGathered(m.result)
	case ProvisionRequestedMsg:
		a.handleProvisionRequested(m)
	case RestartRequestedMsg:
		a.handleRestartRequested(m)
	case UpdateRequestedMsg:
		a.handleUpdateRequested(m)
	case RestoreRequestedMsg:
		a.handleRestoreRequested(m)
	case provisionCompletedMsg:
		a.handleProvisionCompleted(m.result)
	case provisionErroredMsg:
		a.handleProvisionErrored(m.callbackErr, m.reason, m.lastError, m.logs)
	case replaceCompletedMsg:
		a.handleReplaceCompleted(m.result, m.maintenanceID)
	case replaceRecoveredMsg:
		a.handleReplaceRecovered(m.info, m.maintenanceID)
	case replaceFailedMsg:
		a.handleReplaceFailed(m.info, m.maintenanceID)
	default:
		a.cfg.Logger.Warn("lease actor: unknown message type",
			"lease_uuid", a.leaseUUID,
		)
	}
}

func (a *LeaseActor) handleContainerDied(containerID string) {
	_ = a.sm.Fire(a.cfg.StopCtx, evContainerDied, containerID)
}

func (a *LeaseActor) handleDiagGathered(result diagResult) {
	// If we're no longer in Failing (e.g., Deprovision preempted), the SM's
	// Ignore declarations on Failed/Deprovisioning drop this event; Fire
	// returns an unhandled-trigger error we can safely discard.
	_ = a.sm.Fire(a.cfg.StopCtx, evDiagGathered, result)
}

func (a *LeaseActor) handleCohortDiverged(ctx context.Context) error {
	return a.sm.Fire(ctx, evCohortDiverged)
}

func (a *LeaseActor) handleMaintenanceRecovered(msg maintenanceRecoveredMsg) error {
	if !msg.maintenanceID.Valid() {
		return errors.New("maintenance recovery message has no exact identity")
	}
	if msg.leaseUUID != a.leaseUUID {
		return errors.New("maintenance recovery message belongs to another lease")
	}
	if a.OwnsMaintenance(msg.maintenanceID) {
		return errors.New("maintenance recovery refused while exact worker remains active")
	}
	if a.pendingMaintenance.Valid() &&
		a.pendingMaintenance.MaintenanceID() != msg.maintenanceID {
		return errors.New("maintenance recovery identity differs from actor generation")
	}

	state := a.sm.State()
	desired := backend.ProvisionStatusFailed
	if msg.outcome == maintenanceRecoveredSuccess ||
		msg.outcome == maintenanceRecoveredFailureReady {
		desired = backend.ProvisionStatusReady
	}
	if state == desired {
		// The worker terminal event (or an entry-action panic after the FSM state
		// changed) may have won the inbox race. Reapply the exact durable
		// projection without replaying gauge, diagnostic, fail-count, or callback
		// side effects. This makes settlement retries genuinely idempotent.
		var err error
		switch msg.outcome {
		case maintenanceRecoveredSuccess:
			err = a.sm.applyMaintenanceRecoveredSuccess(msg.success)
		case maintenanceRecoveredFailureReady, maintenanceRecoveredFailureFailed:
			err = a.sm.applyMaintenanceRecoveredFailure(msg.success, msg.failure, desired)
		case maintenanceRecoveredSuccessRuntimeFailed:
			err = a.sm.applyMaintenanceRecoveredRuntimeFailure(msg.success, msg.failure)
		default:
			return errors.New("maintenance recovery message has invalid outcome")
		}
		if err != nil {
			return err
		}
		if a.pendingMaintenance.Valid() {
			a.sm.clearPendingReplaceRoute()
		}
		return nil
	}

	switch state {
	case backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating:
		if !a.pendingMaintenance.Valid() {
			return errors.New("busy actor has no exact maintenance generation")
		}
	case backend.ProvisionStatusReady, backend.ProvisionStatusFailed:
		// A durable terminal Release outranks a contradictory volatile terminal
		// state (for example Activate committed before a later failed terminal
		// message won the inbox). The dedicated recovery triggers below perform
		// the correction without accepting a stale ordinary worker event.
	default:
		return fmt.Errorf("maintenance recovery cannot converge actor state %s", a.sm.State())
	}
	var err error
	switch msg.outcome {
	case maintenanceRecoveredSuccess:
		err = a.fireAndVerify(evMaintenanceRecoveredSuccess, backend.ProvisionStatusReady, msg.success)
	case maintenanceRecoveredFailureReady:
		err = a.fireAndVerify(
			evMaintenanceRecoveredFailureReady, backend.ProvisionStatusReady,
			maintenanceRecoveryFailureArgs{projection: msg.success, failure: msg.failure},
		)
	case maintenanceRecoveredFailureFailed:
		err = a.fireAndVerify(
			evMaintenanceRecoveredFailureFailed, backend.ProvisionStatusFailed,
			maintenanceRecoveryFailureArgs{projection: msg.success, failure: msg.failure},
		)
	case maintenanceRecoveredSuccessRuntimeFailed:
		err = a.fireAndVerify(
			evMaintenanceRecoveredSuccessRuntimeFailed, backend.ProvisionStatusFailed,
			maintenanceRecoveryFailureArgs{projection: msg.success, failure: msg.failure},
		)
	default:
		return errors.New("maintenance recovery message has invalid outcome")
	}
	if err != nil {
		return err
	}
	// Recovery-owned terminal entry actions intentionally suppress delivery.
	// Clear the exact pending authority only after the transition commits; the
	// caller then atomically converts the durable intent into its outbox row.
	a.sm.clearPendingReplaceRoute()
	return nil
}

// fireAndVerify fires an SM event and verifies the transition landed in
// the expected destination state. This is the ONLY way caller-facing
// handlers should commit an SM transition — the raw sm.Fire call is
// unsafe because stateless returns nil both on Permit (SM transitioned)
// and Ignore (SM deliberately did nothing), so a naive
// `if err := sm.Fire(...); err != nil` lets Ignore paths masquerade as
// success. That trap bit handleProvisionRequested on the
// partial-Deprovision lifecycle (Deprovisioning.Ignore(evProvisionRequested)
// → Fire returns nil → worker spawned against an SM still in
// Deprovisioning → terminal provisionCompletedMsg also Ignored →
// lease wedges with live containers, no Success callback).
//
// Returns nil iff the SM is in wantState after Fire; otherwise returns
// the Fire error (unhandled trigger) OR an errFireIgnored wrapping the
// actual state (Ignore-returns-nil).
func (a *LeaseActor) fireAndVerify(ev leaseEvent, wantState backend.ProvisionStatus, args ...any) error {
	if err := a.sm.Fire(a.cfg.StopCtx, ev, args...); err != nil {
		return err
	}
	if state := a.sm.State(); state != wantState {
		return fmt.Errorf("%v not accepted by SM from state %v", ev, state)
	}
	return nil
}

// classifyReplaceReject maps a failed restart/update fireAndVerify into the
// error the caller (Backend.Restart/Update → HTTP handler) should see.
//
// The safety basis for restart/update is NOT the HTTP prelude's fast-fail
// (that is only a route-time precondition — the lease can change state
// between routing and the actor dequeuing the message). It is that the
// actor inbox is the ONLY path mutating prov.Status, processed serially on
// the actor goroutine. So when fireAndVerify fails here, the SM is
// authoritatively in some non-restartable state, and we classify by that
// state rather than by parsing stateless's untyped unhandled-trigger error
// string (which has no exported sentinel and could change across versions):
//
//   - State ∈ {Restarting, Updating, Deprovisioning, Provisioning} — the
//     lease is busy. This is the concurrent-duplicate / lost-the-race case
//     (e.g. two same-lease restarts: the loser arrives after the winner's
//     onEnterRestarting). Wrap as backend.ErrInvalidState so the HTTP
//     handler returns a clean 409 (not a 500). The duplicate is REJECTED
//     here, not prevented earlier.
//   - State ∈ {Ready, Failed} — restartable; a fireAndVerify failure is
//     unexpected (defensive), so forward the raw error unchanged.
//
// NOTE: callers must NOT route the a.terminated early-return through this —
// that path returns errActorTerminated (caller retries against a fresh
// actor) and must stay distinct from a 409.
func (a *LeaseActor) classifyReplaceReject(err error) error {
	switch a.sm.State() {
	case backend.ProvisionStatusReady, backend.ProvisionStatusFailed:
		return err
	default:
		return fmt.Errorf("%w: lease not in a restartable state (%s)", backend.ErrInvalidState, a.sm.State())
	}
}

// handleProvisionRequested transitions the SM into Provisioning (or from
// Failed on retry), acks the caller, and spawns the work goroutine.
// Spawning inside the actor (rather than in Backend.Provision) means the
// worker is tracked by workers and, under normal operation, the actor
// waits for the worker's terminal sendTerminal to land and be handled
// before exit (bounded by WorkerDrainTimeout). The orphan-worker race class is
// eliminated under that happy-path wait; a timeout refuses the transition
// which would otherwise permit conflicting substrate work.
func (a *LeaseActor) handleProvisionRequested(msg ProvisionRequestedMsg) {
	if a.terminated {
		// Actor has already completed Deprovision but not yet been
		// removed from the registry (defer ordering). Reject so the
		// caller rolls back and retries — a fresh actor will be
		// created on the next routeToLease.
		msg.Ack <- errActorTerminated
		return
	}
	if err := a.fireAndVerify(evProvisionRequested, backend.ProvisionStatusProvisioning); err != nil {
		msg.Ack <- err
		return
	}
	// Publish the cancel capability only after the state machine accepted this
	// request. A rejected/ignored duplicate must not replace the cancel function
	// belonging to the worker that is already in flight.
	a.workCancel = msg.Cancel
	msg.Ack <- nil
	a.spawnProvisionWorker(msg.Work)
}

// spawnProvisionWorker runs doProvision (supplied as the work closure),
// pre-publishes container IDs on success so a preempting
// Deprovision sees them under lock, and sends the terminal SM event via
// sendTerminal. Tracked by workers so the actor waits for this worker
// before exit. On failure, the worker captures container logs BEFORE
// cleanup (see doProvision's defer) so the persisted diagnostic entry
// contains useful debugging output even though the failed containers
// have been removed.
func (a *LeaseActor) spawnProvisionWorker(work func() (string, backend.Reason, ProvisionSuccessResult, map[string]string, error)) {
	a.workers.Add()
	a.cfg.WG.Go(func() {
		// Exactly one sendTerminal call site (the middle defer), driven
		// by terminalMsg. Defer ordering (LIFO):
		//   1. recover (innermost, runs FIRST on panic) — may override
		//      terminalMsg to a panic-error variant.
		//   2. sendTerminal (middle, always runs) — fires whatever
		//      terminalMsg was set to.
		//   3. workers.Done (outermost, runs LAST) — releases the
		//      barrier after sendTerminal has landed the event.
		// This structure guarantees at-most-one terminal per worker and
		// eliminates both the double-send race and any possibility of a
		// wedged SM if a panic occurs before the normal path sets the
		// message.
		var terminalMsg workerTerminalMessage
		var event string
		defer a.workers.Done()
		defer func() {
			if terminalMsg == nil {
				// Defensive: should not occur if the normal path runs
				// to completion. If it does, treat as an error so the
				// SM reaches Failed rather than wedging.
				terminalMsg = provisionErroredMsg{
					callbackErr: errMsgInternal,
					reason:      backend.ReasonInternal,
					lastError:   "provision worker exited without setting terminal event",
				}
				event = "provision_no_result"
			}
			if !a.sendTerminal(terminalMsg) {
				a.cfg.Metrics.TerminalEventDropped(event)
				a.cfg.Logger.Warn("terminal provision event dropped (actor exited or inbox wedged)",
					"lease_uuid", a.leaseUUID,
					"event", event,
				)
			}
		}()
		defer func() {
			if r := recover(); r != nil {
				a.cfg.Logger.Error("provision worker panic — recovering to keep fred alive",
					"lease_uuid", a.leaseUUID,
					"panic", r,
					"stack", string(debug.Stack()),
				)
				a.cfg.Metrics.WorkerPanic("provision")
				// Override any terminalMsg set by the normal path —
				// the panic means the post-set work did not complete.
				terminalMsg = provisionErroredMsg{
					callbackErr: errMsgInternal,
					reason:      backend.ReasonInternal,
					lastError:   fmt.Sprintf("worker panic: %v", r),
				}
				event = "provision_panic"
			}
		}()
		callbackErr, reason, result, failureLogs, err := work()
		if err == nil {
			// Pre-publish so a concurrent Deprovision-preempt reading
			// prov.ContainerIDs sees the new IDs and can tear them down
			// rather than leaving orphans.
			a.cfg.ProvisionStore.UpdateFn(a.leaseUUID, func(p *ProvisionState) {
				p.ContainerIDs = result.ContainerIDs
			})
			terminalMsg = provisionCompletedMsg{result: result}
			event = "provision_completed"
		} else {
			terminalMsg = provisionErroredMsg{
				callbackErr: callbackErr,
				reason:      reason, // authored by doProvision (ENG-508); "" ⇒ Unknown default at read boundary, fail-closed
				lastError:   err.Error(),
				logs:        failureLogs,
			}
			event = "provision_errored"
		}
	})
}

func (a *LeaseActor) handleProvisionCompleted(result ProvisionSuccessResult) {
	_ = a.sm.Fire(a.cfg.StopCtx, evProvisionCompleted, result)
}

func (a *LeaseActor) handleProvisionErrored(callbackErr string, reason backend.Reason, lastError string, logs map[string]string) {
	_ = a.sm.Fire(a.cfg.StopCtx, evProvisionErrored, provisionErrorInfo{
		callbackErr: callbackErr,
		reason:      reason,
		lastError:   lastError,
		logs:        logs,
	})
}

func (a *LeaseActor) handleRestartRequested(msg RestartRequestedMsg) {
	if a.terminated {
		msg.Ack <- errActorTerminated
		return
	}
	if err := a.validateMaintenanceRequest(
		msg.Maintenance, msg.CallbackURL, msg.LifecycleCallbackURL,
	); err != nil {
		msg.Ack <- err
		return
	}
	// onEnterRestarting publishes Status=Restarting and captures the validated
	// callback pair as pending inside this Fire before the ack.
	if err := a.fireAndVerify(evRestartRequested, backend.ProvisionStatusRestarting,
		replaceEntryArgs{
			CallbackURL: msg.CallbackURL, LifecycleCallbackURL: msg.LifecycleCallbackURL,
			CallbackKind: replaceCallbackLifecycle, Maintenance: msg.Maintenance,
		}); err != nil {
		// A concurrent same-lease restart that lost the race finds the SM
		// already busy → classifyReplaceReject returns ErrInvalidState (409).
		msg.Ack <- a.classifyReplaceReject(err)
		return
	}
	// Set workCancel only AFTER a successful fire (ENG-230 §4): a rejected
	// concurrent restart must not clobber the in-flight worker's cancel
	// func, which onExitProvisioning uses on Deprovision-preempt. workCancel
	// is consumed only by onExitProvisioning, which can run only after the
	// state was entered (i.e. after a successful fire).
	a.workCancel = msg.Cancel
	a.spawnReplaceWorker(msg.Work)
	msg.Ack <- nil
}

// handleRestoreRequested drives a restore (ENG-325) onto the existing
// replace machinery. It is a clone of handleRestartRequested with one
// difference: it fires evRestoreRequested (permitted only from
// Provisioning, the state a restore's new lease is reserved in) instead
// of evRestartRequested. The destination state is still Restarting, so
// onEnterRestarting (reused via OnEntryFrom(evRestoreRequested)) writes
// Status=Restarting + the callback pair before the ack, and spawnReplaceWorker
// + the evReplace{Completed,Recovered,Failed} terminal events behave
// identically. Because the prior Status was Provisioning (not Ready),
// applyReplaceEntry sets replaceWasActive=false, so a successful restore
// Inc's activeProvisions — bringing the lease from absent to active.
func (a *LeaseActor) handleRestoreRequested(msg RestoreRequestedMsg) {
	if a.terminated {
		msg.Ack <- errActorTerminated
		return
	}
	// onEnterRestarting writes Status=Restarting and the callback pair inside
	// this Fire, before the ack — preserving the handler-publish contract.
	if err := a.fireAndVerify(evRestoreRequested, backend.ProvisionStatusRestarting,
		replaceEntryArgs{
			CallbackURL: msg.CallbackURL, LifecycleCallbackURL: msg.LifecycleCallbackURL,
			CallbackKind: replaceCallbackOperation,
		}); err != nil {
		// Restore is permitted only from Provisioning; from any other state
		// (e.g. a duplicate after the SM already left Provisioning, or a
		// concurrent Deprovision) classifyReplaceReject yields ErrInvalidState
		// (→409).
		msg.Ack <- a.classifyReplaceReject(err)
		return
	}
	// Set workCancel only AFTER a successful fire (ENG-230 §4); see
	// handleRestartRequested for the rationale.
	a.workCancel = msg.Cancel
	a.spawnReplaceWorker(msg.Work)
	msg.Ack <- nil
}

func (a *LeaseActor) handleUpdateRequested(msg UpdateRequestedMsg) {
	if a.terminated {
		msg.Ack <- errActorTerminated
		return
	}
	if err := a.validateMaintenanceRequest(
		msg.Maintenance, msg.CallbackURL, msg.LifecycleCallbackURL,
	); err != nil {
		msg.Ack <- err
		return
	}
	// onEnterUpdating publishes Status=Updating and captures the validated
	// callback pair as pending inside this Fire before the ack.
	if err := a.fireAndVerify(evUpdateRequested, backend.ProvisionStatusUpdating,
		replaceEntryArgs{
			CallbackURL: msg.CallbackURL, LifecycleCallbackURL: msg.LifecycleCallbackURL,
			CallbackKind: replaceCallbackLifecycle, Maintenance: msg.Maintenance,
		}); err != nil {
		// A concurrent same-lease update that lost the race finds the SM
		// already busy → classifyReplaceReject returns ErrInvalidState (409).
		msg.Ack <- a.classifyReplaceReject(err)
		return
	}
	// Set workCancel only AFTER a successful fire (ENG-230 §4); see
	// handleRestartRequested for the rationale.
	a.workCancel = msg.Cancel
	a.spawnReplaceWorker(msg.Work)
	msg.Ack <- nil
}

func (a *LeaseActor) validateMaintenanceRequest(
	claim shared.MaintenanceIntentClaim,
	callbackURL string,
	lifecycleCallbackURL string,
) error {
	if !claim.Valid() {
		return errors.New("restart/update requires a valid maintenance intent claim")
	}
	if claim.LeaseUUID() != a.leaseUUID {
		return errors.New("maintenance intent belongs to another lease")
	}
	if callbackURL != claim.CallbackURL() || lifecycleCallbackURL != claim.LifecycleCallbackURL() {
		return errors.New("maintenance callback route differs from durable intent")
	}
	return nil
}

// spawnReplaceWorker runs a replace operation (restart or update) and
// dispatches the correct terminal SM event based on (err, recovered).
// Pre-publishes new ContainerIDs / ServiceContainers on success so a
// preempting Deprovision reading prov observes the new set under lock.
//
// wasActive (whether the lease was Status==Ready at replace-start) is read
// HERE, on the actor goroutine, before spawning the worker — never inside
// the worker closure. It is used only when the worker's result sets
// RecoveredIfSourceActive (the doRestart preflight branch): there the
// recovered-vs-failed decision keys on wasActive (the actor-observed source)
// instead of result.Restored, fixing the stale-route-time-snapshot edge.
func (a *LeaseActor) spawnReplaceWorker(work func() ReplaceResult) {
	wasActive := a.replaceWasActive
	hasMaintenanceAuthority := a.pendingMaintenance.Valid()
	maintenanceID := a.pendingMaintenance.MaintenanceID()
	if hasMaintenanceAuthority {
		a.markMaintenanceWorker(maintenanceID)
	}
	a.workers.Add()
	a.cfg.WG.Go(func() {
		// Same defer structure as spawnProvisionWorker — see that
		// function for the rationale. One sendTerminal call site in
		// the middle defer; normal path and panic recover both write
		// to terminalMsg.
		var terminalMsg workerTerminalMessage
		var event string
		defer a.workers.Done()
		defer func() {
			if terminalMsg == nil {
				terminalMsg = replaceFailedMsg{info: ReplaceFailureInfo{
					CallbackErr:         errMsgInternal,
					Reason:              backend.ReasonInternal,
					LastError:           "replace worker exited without setting terminal event",
					PreserveMaintenance: hasMaintenanceAuthority,
				}, maintenanceID: maintenanceID}
				event = "replace_no_result"
			}
			if !a.sendTerminal(terminalMsg) {
				a.clearMaintenanceWorker(maintenanceID)
				a.cfg.Metrics.TerminalEventDropped(event)
				a.cfg.Logger.Warn("terminal replace event dropped (actor exited or inbox wedged)",
					"lease_uuid", a.leaseUUID,
					"event", event,
				)
			}
		}()
		defer func() {
			if r := recover(); r != nil {
				a.cfg.Logger.Error("replace worker panic — recovering to keep fred alive",
					"lease_uuid", a.leaseUUID,
					"panic", r,
					"stack", string(debug.Stack()),
				)
				a.cfg.Metrics.WorkerPanic("replace")
				terminalMsg = replaceFailedMsg{info: ReplaceFailureInfo{
					CallbackErr:         errMsgInternal,
					Reason:              backend.ReasonInternal,
					LastError:           fmt.Sprintf("worker panic: %v", r),
					PreserveMaintenance: hasMaintenanceAuthority,
				}, maintenanceID: maintenanceID}
				event = "replace_panic"
			}
		}()
		result := work()
		if result.Err == nil {
			a.cfg.ProvisionStore.UpdateFn(a.leaseUUID, func(p *ProvisionState) {
				p.ContainerIDs = result.Success.ContainerIDs
				if result.Success.ServiceContainers != nil {
					p.ServiceContainers = result.Success.ServiceContainers
				}
			})
		}
		// recovered-vs-failed: normally result.Restored, but a doRestart
		// preflight failure (no container touched) sets RecoveredIfSourceActive
		// so the decision keys on the actor-observed pre-replace activeness
		// (wasActive, captured above on the actor goroutine) — recovered iff
		// the lease was Ready/running at replace-start. Every other path
		// leaves the flag false → recovered == result.Restored (unchanged).
		recovered := result.Restored
		if result.RecoveredIfSourceActive {
			recovered = wasActive
		}
		switch {
		case result.Err == nil:
			terminalMsg = replaceCompletedMsg{result: result.Success, maintenanceID: maintenanceID}
			event = "replace_completed"
		case recovered:
			terminalMsg = replaceRecoveredMsg{info: result.Failure, maintenanceID: maintenanceID}
			event = "replace_recovered"
		default:
			terminalMsg = replaceFailedMsg{info: result.Failure, maintenanceID: maintenanceID}
			event = "replace_failed"
		}
	})
}

// OwnsMaintenance is the concurrency-safe, exact worker-ownership proof used
// by periodic recovery. A projected Restarting/Updating status alone is not an
// ownership proof: a dropped terminal event can leave it stale indefinitely.
func (a *LeaseActor) OwnsMaintenance(id shared.MaintenanceID) bool {
	if a == nil || !id.Valid() {
		return false
	}
	a.maintenanceWorkerMu.RLock()
	defer a.maintenanceWorkerMu.RUnlock()
	return a.maintenanceWorkerID == id
}

func (a *LeaseActor) markMaintenanceWorker(id shared.MaintenanceID) {
	if !id.Valid() {
		return
	}
	a.maintenanceWorkerMu.Lock()
	a.maintenanceWorkerID = id
	a.maintenanceWorkerMu.Unlock()
}

func (a *LeaseActor) clearMaintenanceWorker(id shared.MaintenanceID) {
	if !id.Valid() {
		return
	}
	a.maintenanceWorkerMu.Lock()
	if a.maintenanceWorkerID == id {
		a.maintenanceWorkerID = ""
	}
	a.maintenanceWorkerMu.Unlock()
}

func (a *LeaseActor) clearAnyMaintenanceWorker() {
	a.maintenanceWorkerMu.Lock()
	a.maintenanceWorkerID = ""
	a.maintenanceWorkerMu.Unlock()
}

func (a *LeaseActor) handleReplaceCompleted(result ReplaceSuccessResult, ids ...shared.MaintenanceID) {
	if len(ids) == 1 {
		defer a.clearMaintenanceWorker(ids[0])
	}
	_ = a.sm.Fire(a.cfg.StopCtx, evReplaceCompleted, result)
}

func (a *LeaseActor) handleReplaceRecovered(info ReplaceFailureInfo, ids ...shared.MaintenanceID) {
	if len(ids) == 1 {
		defer a.clearMaintenanceWorker(ids[0])
	}
	_ = a.sm.Fire(a.cfg.StopCtx, evReplaceRecovered, info)
}

func (a *LeaseActor) handleReplaceFailed(info ReplaceFailureInfo, ids ...shared.MaintenanceID) {
	if len(ids) == 1 {
		defer a.clearMaintenanceWorker(ids[0])
	}
	_ = a.sm.Fire(a.cfg.StopCtx, evReplaceFailed, info)
}

// hasExited reports whether the actor's run loop has returned (a.done
// closed). Used by SendTerminal to make the "actor-already-exited" case
// a definitive refusal rather than a select-randomized 50/50 between
// queueing into an inbox nobody will drain and the closed-done arm.
// External callers observe lifecycle through the exported Done()
// channel getter instead — `<-actor.Done()` is the canonical wait.
func (a *LeaseActor) hasExited() bool {
	select {
	case <-a.done:
		return true
	default:
		return false
	}
}

// sendTerminal enqueues a terminal SM event from an in-flight work
// goroutine. Deliberately does NOT refuse on a canceled stopCtx: the
// goroutine has already done its physical work (containers created,
// swapped, removed) — the SM must record the outcome even during
// shutdown to keep releaseStore / in-memory state / the callback record
// consistent with the host. Returns false only if the actor has fully
// exited (inbox no longer drained), the exiting channel is closed
// (actor is in its exit sequence at the final drain), or the bounded
// inbox is wedged; in either case the drop is counted via
// leaseTerminalEventDroppedTotal at the call site.
//
// Two refusal gates on the fast path:
//   - hasExited(): a.done closed. Actor is fully torn down.
//   - isExiting(): a.exiting closed just before the final retirement drain.
//     Covers the narrow post-drain / pre-done window where a late
//     worker could otherwise successfully enqueue into an inbox that
//     no goroutine will drain, silently losing the event. Both
//     channels also appear as select arms below so the main wait
//     composes correctly, and a post-send non-blocking re-check
//     guarantees the metric reflects reality when a late close
//     races with a successful enqueue.
//
// In normal operation (waitForWorkers returns cleanly) workers finish
// before terminal admission closes, so these gates are pure defense
// against the waitForWorkers-timeout edge case.
func (a *LeaseActor) sendTerminal(msg workerTerminalMessage) bool {
	if a.hasExited() || a.isExiting() {
		return false
	}
	select {
	case a.inbox <- msg:
		// Re-check: exiting may have been closed between the pre-check
		// and this send. Report as dropped so the metric is accurate.
		// The message will rot in the inbox but no one relies on it.
		if a.isExiting() {
			return false
		}
		return true
	case <-a.done:
		return false
	case <-a.exiting:
		return false
	case <-time.After(terminalSendTimeout):
		return false
	}
}

// isExiting reports whether the actor has closed its `exiting` channel, i.e.
// retirement no longer admits terminal sends. The final drain runs immediately
// after this gate closes. Symmetric with hasExited() but for the earlier gate.
// Intra-package only — external lifecycle synchronization goes through Done().
func (a *LeaseActor) isExiting() bool {
	select {
	case <-a.exiting:
		return true
	default:
		return false
	}
}

// closeExiting closes the exiting channel exactly once, even across
// retirement and a test-driven close. Using
// sync.Once keeps the close idempotent without a select-default
// double-check idiom that would itself race under concurrent callers.
func (a *LeaseActor) closeExiting() {
	a.exitingOnce.Do(func() { close(a.exiting) })
}

// terminalSendTimeout bounds how long a terminal send will wait for inbox
// space. Long enough for the actor to drain typical backlogs, short
// enough that a wedged actor doesn't pin the goroutine indefinitely.
const terminalSendTimeout = 10 * time.Second

// removeFromRegistry deletes this actor from the substrate's actor
// registry via the OnTerminated callback wired at construction. The
// docker-side closure preserves CompareAndDelete semantics: only
// deletes if the registered entry is THIS actor, so a fresh actor
// stored for the same UUID after our exit started isn't clobbered.
// Used as a deferred action on actor exit.
func (a *LeaseActor) removeFromRegistry() {
	a.cfg.OnTerminated(a.leaseUUID)
}

// State returns the SM's current ProvisionStatus.
func (a *LeaseActor) State() backend.ProvisionStatus {
	return a.sm.State()
}

// InboxDepth returns the number of pending messages in the actor's
// inbox (best-effort read; the value may shift between this call and
// any subsequent use, but len() on a buffered channel is safe to read
// concurrently with sends/receives).
func (a *LeaseActor) InboxDepth() int {
	return len(a.inbox)
}

// InboxCap returns the inbox channel capacity (constant for the
// actor's lifetime).
func (a *LeaseActor) InboxCap() int {
	return cap(a.inbox)
}

// CurrentMessageStart returns the UnixNano timestamp of the message
// the actor is currently processing in handle(), or 0 when idle.
// Atomic read; safe to call from the metrics sample loop concurrently
// with the actor goroutine. Method name uses the noun form (vs the
// underlying field's verb-implying `currentMessageStart atomic.Int64`)
// to avoid the Go field/method name collision while preserving intent.
func (a *LeaseActor) CurrentMessageStart() int64 {
	return a.currentMessageStart.Load()
}

// Done returns the channel that closes when the actor's run loop has
// fully torn down (after external admission closes, workers and their
// terminal messages settle, and the actor is removed from its registry).
// Tests and substrate adapters can block on this to wait for an actor's full
// quiescence — e.g., to
// assert that a subsequent re-provision with the same lease UUID
// creates a fresh actor. Returns a `<-chan struct{}` so callers cannot
// accidentally close the underlying channel.
func (a *LeaseActor) Done() <-chan struct{} {
	return a.done
}

// TryEnqueue does a non-blocking enqueue of msg into the actor's inbox.
// Returns true on success, false if the actor is retiring or the inbox is
// full. This is the primary entrypoint the substrate's routing layer uses to
// deliver messages atomically with its registry-resolve operation; the caller
// holds the registry mutex across this call so a successful enqueue
// implies the actor is still registered and consuming its inbox.
//
// admissionMu makes a successful send linearizable with retirement: a message
// is either accepted for the live run loop, or refused while the still-
// registered actor completes its retirement. The mutex is held only for the
// non-blocking send and is never held while unregistering the actor.
func (a *LeaseActor) TryEnqueue(msg LeaseMessage) bool {
	a.admissionMu.Lock()
	defer a.admissionMu.Unlock()
	if a.retiring {
		return false
	}
	select {
	case a.inbox <- msg:
		return true
	default:
		return false
	}
}

// handleDeprovision runs inside the lease actor's message handler. It fires
// the SM transition then runs the work synchronously, returning the outcome
// on the DeprovisionMsg's reply channel.
//
// Migrated from internal/backend/docker/deprovision.go at PR5b-2 BC. The
// body now reaches substrate-private state exclusively via the cfg seams:
//   - lease existence check via cfg.ProvisionStore.Get (was b.provisions[uuid])
//   - deprovision dispatch via cfg.DoDeprovisionFn (was b.doDeprovision)
//   - logger via cfg.Logger
//
// The ctx threaded in is the actor-owned ctx from the inbound
// DeprovisionMsg (which carries the caller's ctx from Backend.Deprovision).
func (a *LeaseActor) handleDeprovision(ctx context.Context) error {
	// Attempt the SM transition. If it's not permitted, check whether the
	// provision is already gone or we're in an unexpected state. Absence from
	// this volatile projection is not terminal authority: a substrate finalizer
	// may still own containers, volumes, or a durable close journal, so it must
	// still receive the idempotent request.
	transitioned := true
	if err := a.sm.Fire(ctx, evDeprovisionRequested); err != nil {
		transitioned = false
		if _, exists := a.cfg.ProvisionStore.Get(a.leaseUUID); exists {
			a.cfg.Logger.Warn("deprovision transition denied by SM",
				"lease_uuid", a.leaseUUID, "error", err)
			if errors.Is(err, ErrWorkerDrainTimeout) {
				// OnExit failed before stateless changed state, so the mutation
				// worker still owns the lease. Running teardown here would allow
				// that worker to land a later Compose Up after close and resurrect
				// the workload. Preserve both state and substrate, and let a retry
				// proceed once the barrier reaches zero.
				return fmt.Errorf("deprovision refused while prior mutation remains in flight: %w", err)
			}
			// Fall through to the work anyway — the SM may not know every state
			// (partial port). The work itself is idempotent.
		}
	}
	if transitioned && a.replaceCallbackKind == replaceCallbackLifecycle &&
		a.pendingReplaceCallbackURL != "" && a.pendingReplaceLifecycleCallbackURL != "" {
		// Teardown eliminates the runtime cohort whose labels were protected by
		// the old pair. Promote an accepted maintenance route before close
		// admission so its lifecycle capability owns the terminal callback even
		// when Deprovision preempted the replace terminal event.
		a.cfg.ProvisionStore.UpdateFn(a.leaseUUID, func(p *ProvisionState) {
			p.CallbackURL = a.pendingReplaceCallbackURL
			p.LifecycleCallbackURL = a.pendingReplaceLifecycleCallbackURL
		})
	}
	err := a.cfg.DoDeprovisionFn(ctx, a.leaseUUID)
	// If the provision entry was fully removed (success path), signal the
	// run loop to exit so a subsequent re-provision with the same UUID
	// creates a fresh actor instead of being Ignored by a stale SM.
	if _, exists := a.cfg.ProvisionStore.Get(a.leaseUUID); !exists {
		a.terminated = true
	}
	return err
}
