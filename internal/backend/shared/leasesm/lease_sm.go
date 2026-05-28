package leasesm

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/qmuntal/stateless"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// leaseEvent is the typed event enum fired into the lease state machine.
// The underlying stateless library accepts any comparable; this enum is the
// only thing callers ever pass to Fire, so state transitions are checked at
// call sites even though the library stores triggers as any.
type leaseEvent int

const (
	evContainerDied leaseEvent = iota
	evDeprovisionRequested
	evRestartRequested
	evUpdateRequested
	evProvisionRequested
	evProvisionCompleted
	evProvisionErrored
	evDiagGathered
	evContainersRemoved
	// evReplaceCompleted / evReplaceRecovered / evReplaceFailed represent
	// the outcome of a Restart or Update operation. Three events cover the
	// four observable outcomes (success, rollback-restored, rollback-failed,
	// preflight-restored):
	//
	//   err == nil                              → evReplaceCompleted → Ready
	//   err != nil, final Status == Ready       → evReplaceRecovered → Ready
	//   err != nil, final Status == Failed      → evReplaceFailed    → Failed
	//
	// evReplaceRecovered exists to distinguish "lease ended up Ready but
	// the requested change did NOT take effect" from the normal success
	// case — same destination (Ready), different callback (Failed with
	// rollback suffix vs Success).
	evReplaceCompleted
	evReplaceRecovered
	evReplaceFailed
)

func (e leaseEvent) String() string {
	switch e {
	case evContainerDied:
		return "ContainerDied"
	case evDeprovisionRequested:
		return "DeprovisionRequested"
	case evRestartRequested:
		return "RestartRequested"
	case evUpdateRequested:
		return "UpdateRequested"
	case evProvisionRequested:
		return "ProvisionRequested"
	case evProvisionCompleted:
		return "ProvisionCompleted"
	case evProvisionErrored:
		return "ProvisionErrored"
	case evDiagGathered:
		return "DiagGathered"
	case evContainersRemoved:
		return "ContainersRemoved"
	case evReplaceCompleted:
		return "ReplaceCompleted"
	case evReplaceRecovered:
		return "ReplaceRecovered"
	case evReplaceFailed:
		return "ReplaceFailed"
	}
	return fmt.Sprintf("leaseEvent(%d)", int(e))
}

// leaseSM wraps a stateless.StateMachine with a typed façade. Each lease
// actor owns one SM; transitions are serialized by the actor's inbox.
type leaseSM struct {
	actor *LeaseActor
	sm    *stateless.StateMachine
}

func newLeaseSM(actor *LeaseActor) *leaseSM {
	initial := readProvisionStatus(actor)
	sm := stateless.NewStateMachine(initial)

	// Count every transition for operator visibility. Runs inside Fire
	// in the actor's goroutine, so no additional synchronization needed.
	sm.OnTransitioned(func(_ context.Context, tr stateless.Transition) {
		actor.cfg.Metrics.SMTransition(
			fmt.Sprintf("%v", tr.Source),
			fmt.Sprintf("%v", tr.Destination),
			fmt.Sprintf("%v", tr.Trigger),
		)
	})

	lsm := &leaseSM{actor: actor, sm: sm}

	// Configure all existing states so Fire never hits an unconfigured state.
	for _, s := range []backend.ProvisionStatus{
		backend.ProvisionStatusProvisioning,
		backend.ProvisionStatusReady,
		backend.ProvisionStatusFailing,
		backend.ProvisionStatusFailed,
		backend.ProvisionStatusRestarting,
		backend.ProvisionStatusUpdating,
		backend.ProvisionStatusDeprovisioning,
		backend.ProvisionStatusUnknown,
	} {
		sm.Configure(s)
	}

	// Ready: a container died (guard confirms); a deprovision arrived;
	// or the operator initiated a Restart or Update.
	sm.Configure(backend.ProvisionStatusReady).
		Permit(evContainerDied, backend.ProvisionStatusFailing, lsm.guardContainerActuallyDied).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning).
		Permit(evRestartRequested, backend.ProvisionStatusRestarting).
		Permit(evUpdateRequested, backend.ProvisionStatusUpdating)

	// Failing: transitional. The async diag goroutine is running. Either
	// DiagGathered arrives (→ Failed, emit terminal callback) or a
	// DeprovisionRequested preempts (→ Deprovisioning, no callback). On any
	// exit the goroutine's context is canceled — this is the structural
	// suppression that prevents a stale Failed callback from being emitted
	// after the lease has moved on. Subsequent ContainerDied/DiagGathered
	// events after we've moved past Failing are Ignore'd so the race between
	// cancellation signal and an in-flight goroutine firing DiagGathered
	// can't resurrect a stale Failed callback.
	sm.Configure(backend.ProvisionStatusFailing).
		OnEntryFrom(evContainerDied, lsm.onEnterFailing).
		OnExit(lsm.onExitFailing).
		Permit(evDiagGathered, backend.ProvisionStatusFailed).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning).
		// Accept retry events from Failing the same way Failed does. If a
		// diag goroutine is wedged and never fires DiagGathered, a
		// subsequent Provision/Restart/Update retry would otherwise hit an
		// unhandled trigger. Allowing the retry from Failing removes the
		// wedge; Failing.OnExit cancels the stale diag goroutine on the
		// way out via diagCancel + waitForWorkers.
		Permit(evProvisionRequested, backend.ProvisionStatusProvisioning).
		Permit(evRestartRequested, backend.ProvisionStatusRestarting).
		Permit(evUpdateRequested, backend.ProvisionStatusUpdating).
		Ignore(evContainerDied)

	// Failed: terminal callback already emitted on entry from DiagGathered.
	// Deprovision can still be requested; later ContainerDied/DiagGathered
	// are ignored.
	sm.Configure(backend.ProvisionStatusFailed).
		OnEntryFrom(evDiagGathered, lsm.onEnterFailedFromDiag).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning).
		Ignore(evContainerDied).
		Ignore(evDiagGathered)

	// Deprovisioning: work runs in actor.handleDeprovision after Fire returns.
	// Ignore die events and any stale DiagGathered from a canceled-too-late
	// async goroutine.
	sm.Configure(backend.ProvisionStatusDeprovisioning).
		Ignore(evContainerDied).
		Ignore(evDiagGathered)

	// Provisioning: async goroutine is running. Exits by ProvisionCompleted
	// (→ Ready, emit Success), ProvisionErrored (→ Failed, emit Failed), or
	// DeprovisionRequested (→ Deprovisioning, OnExit cancels goroutine — the
	// structural suppression for Provision+Deprovision races, analogous to
	// Failing's cancel-on-exit mechanism).
	sm.Configure(backend.ProvisionStatusProvisioning).
		Permit(evProvisionCompleted, backend.ProvisionStatusReady).
		Permit(evProvisionErrored, backend.ProvisionStatusFailed).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning).
		OnExit(lsm.onExitProvisioning).
		Ignore(evContainerDied).
		Ignore(evProvisionRequested)

	// Restarting/Updating: same shape as Provisioning — a goroutine is
	// doing the work; OnExit cancels it on preemption. The goroutine fires
	// evReplaceCompleted (full success), evReplaceRecovered (failure but
	// lease ended up Ready via rollback / preflight-restored), or
	// evReplaceFailed (ended up Failed).
	//
	// onEnterRestarting/onEnterUpdating are the SOLE writers of
	// prov.Status (Restarting/Updating) + prov.CallbackURL for these
	// paths post-ENG-230. They run inside Fire on the actor goroutine,
	// before handleRestartRequested/handleUpdateRequested ack the
	// request — preserving the "Restart()/Update() returns ⇒ Status is
	// Restarting/Updating" contract the HTTP handler's event publish
	// depends on.
	//
	// No Ignore(evRestartRequested/evUpdateRequested) guard is needed any
	// more. Those guards existed only because the off-actor prelude
	// pre-wrote prov.Status=Restarting/Updating, so newLeaseSM initialized
	// a freshly-created actor's SM directly in Restarting/Updating and the
	// incoming request event had to be Ignored as a self-event. With the
	// prelude flip gone, readProvisionStatus reads the lease's TRUE status
	// (Ready/Failed) and the event is a real Permit transition. recover.go
	// can still leave a lease at rest in Restarting/Updating (it PRESERVES
	// those statuses; only Failing is normalized to Failed) — but such a
	// lease is unreachable by a restart/update event: the prelude fast-fail
	// is a ROUTE-TIME precondition that refuses to route on a non-Ready/
	// Failed lease. The one case where evRestartRequested meets an already-
	// Restarting SM is a concurrent duplicate (handleRestartRequested
	// TOCTOU), correctly rejected as an invalid transition → ErrInvalidState
	// (409); keeping Ignore would instead no-op it and spawn a second
	// worker. NOTE: this comment must NOT claim the prelude guarantees
	// processing-time state — it is only a route-time precondition; the
	// actor's serial inbox is what guarantees Status consistency.
	sm.Configure(backend.ProvisionStatusRestarting).
		OnEntryFrom(evRestartRequested, lsm.onEnterRestarting).
		Permit(evReplaceCompleted, backend.ProvisionStatusReady).
		Permit(evReplaceRecovered, backend.ProvisionStatusReady).
		Permit(evReplaceFailed, backend.ProvisionStatusFailed).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning).
		OnExit(lsm.onExitProvisioning).
		Ignore(evContainerDied)
	sm.Configure(backend.ProvisionStatusUpdating).
		OnEntryFrom(evUpdateRequested, lsm.onEnterUpdating).
		Permit(evReplaceCompleted, backend.ProvisionStatusReady).
		Permit(evReplaceRecovered, backend.ProvisionStatusReady).
		Permit(evReplaceFailed, backend.ProvisionStatusFailed).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning).
		OnExit(lsm.onExitProvisioning).
		Ignore(evContainerDied)

	// Ready entry actions: emit Success from a Provision or Replace success,
	// or emit Failed-with-rollback-suffix from a Replace recovery. Status
	// and LastError were set by the underlying do* defer — entry actions
	// just send callbacks.
	sm.Configure(backend.ProvisionStatusReady).
		OnEntryFrom(evProvisionCompleted, lsm.onEnterReadyFromProvision).
		OnEntryFrom(evReplaceCompleted, lsm.onEnterReadyFromReplaceCompleted).
		OnEntryFrom(evReplaceRecovered, lsm.onEnterReadyFromReplaceRecovered)

	// Failed entry actions: emit Failed from a Provision error or Replace
	// failure. Permit(ProvisionRequested) for re-provision retries.
	sm.Configure(backend.ProvisionStatusFailed).
		OnEntryFrom(evProvisionErrored, lsm.onEnterFailedFromProvision).
		OnEntryFrom(evReplaceFailed, lsm.onEnterFailedFromReplace).
		Permit(evProvisionRequested, backend.ProvisionStatusProvisioning)

	// Deprovisioning ignores stale provision/replace-completion events that
	// might fire from an async goroutine that already started but hadn't
	// noticed cancellation. Defense-in-depth mirroring Failing's pattern.
	sm.Configure(backend.ProvisionStatusDeprovisioning).
		Ignore(evProvisionCompleted).
		Ignore(evProvisionErrored).
		Ignore(evProvisionRequested).
		Ignore(evReplaceCompleted).
		Ignore(evReplaceRecovered).
		Ignore(evReplaceFailed)

	// Failed: can accept restart/update retry requests in addition to
	// re-provision.
	sm.Configure(backend.ProvisionStatusFailed).
		Permit(evRestartRequested, backend.ProvisionStatusRestarting).
		Permit(evUpdateRequested, backend.ProvisionStatusUpdating)

	return lsm
}

// Fire is the typed entry point. Compile error if callers pass a non-leaseEvent.
func (lsm *leaseSM) Fire(ctx context.Context, ev leaseEvent, args ...any) error {
	return lsm.sm.FireCtx(ctx, ev, args...)
}

func (lsm *leaseSM) State() backend.ProvisionStatus {
	s, err := lsm.sm.State(context.Background())
	if err != nil {
		return backend.ProvisionStatusUnknown
	}
	status, ok := s.(backend.ProvisionStatus)
	if !ok {
		return backend.ProvisionStatusUnknown
	}
	return status
}

// guardContainerActuallyDied verifies via Docker Inspect that the container is
// actually exited. Docker events can be duplicated or arrive out of order.
// Stashes the inspect result on the actor for the entry action to use
// (stateless doesn't forward guard-captured data).
func (lsm *leaseSM) guardContainerActuallyDied(ctx context.Context, args ...any) bool {
	if len(args) < 1 {
		return false
	}
	containerID, ok := args[0].(string)
	if !ok {
		return false
	}
	cfg := &lsm.actor.cfg

	// Status guard via the substrate-agnostic LeaseProvisionStore. This is
	// the one in-flow site in PR4 where a single lease-status read
	// migrates cleanly to the seam (no other field reads/writes share
	// the critical section). All compound critical sections in this file
	// keep direct b.provisionsMu access — see LeaseProvisionStore
	// docstring for the rationale.
	if state, ok := cfg.ProvisionStore.Get(lsm.actor.leaseUUID); !ok || state.Status != backend.ProvisionStatusReady {
		return false
	}

	reqCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Inspect via the substrate-agnostic InstanceInspector. ServiceName
	// rides along on InstanceState (populated by the docker adapter from
	// the underlying ContainerInfo's label) so the SM does not reach for
	// substrate-specific metadata at this seam. cfg.Inspector is
	// exercised here and in recover.go.
	state, err := cfg.Inspector.InspectInstance(reqCtx, containerID)
	if err != nil {
		cfg.Logger.Warn("failed to inspect container after die event",
			"container_id", ShortID(containerID),
			"lease_uuid", lsm.actor.leaseUUID,
			"error", err,
		)
		return false
	}
	// "Terminally gone?" check: PhaseExited and PhaseFailed cover the
	// Docker statuses {"exited", "removing", "dead"} that previously
	// mapped to ProvisionStatusFailed. PhaseRunning and PhaseUnknown
	// (which subsumes "created", "restarting", and unrecognized) are
	// not terminal — same as the prior containerStatusToProvisionStatus
	// behavior.
	if state == nil || (state.Phase != PhaseExited && state.Phase != PhaseFailed) {
		return false
	}
	lsm.actor.pendingDeathInfo = state
	return true
}

// onEnterFailing runs as the Ready→Failing entry action. Flips provision
// fields under lock and spawns the async diag goroutine whose context
// cancellation is the structural suppression mechanism for stale
// Failed callbacks.
func (lsm *leaseSM) onEnterFailing(ctx context.Context, args ...any) error {
	if len(args) < 1 {
		return fmt.Errorf("onEnterFailing: missing containerID")
	}
	containerID, ok := args[0].(string)
	if !ok {
		return fmt.Errorf("onEnterFailing: containerID not string")
	}
	leaseUUID := lsm.actor.leaseUUID
	info := lsm.actor.pendingDeathInfo

	// No Status recheck needed (ENG-230). Off-actor Status writes for
	// restart/update are eliminated — onEnterRestarting/onEnterUpdating
	// now own those writes on the actor goroutine — so every write to
	// prov.Status is actor-serial. A Ready→Failing entry therefore always
	// observes Status == Ready: nothing off-actor can flip it between the
	// SM guard's read and this entry action. The previous recheck +
	// lease_failing_race_skipped_total metric defended against the
	// off-actor prelude flip that no longer exists. The `exists` guard is
	// kept (cheap nil-guard: UpdateFn returns false if the lease was
	// removed); ActiveProvisionsDec is now unconditional on the applied
	// path.
	exists := lsm.actor.cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
		p.Status = backend.ProvisionStatusFailing
		p.FailCount++
		p.LastError = errMsgContainerExited
	})
	if !exists {
		return nil
	}
	lsm.actor.cfg.Metrics.ActiveProvisionsDec()

	// Spawn the async diag gather. Its context is derived from
	// context.Background() with a bounded timeout — NOT from stopCtx —
	// so a backend shutdown doesn't cancel diag prematurely and leave
	// the SM wedged in Failing. gatherDiagAsync's ctx.Err check is now
	// driven only by the timeout (diagnosticsGatherTimeout) or by
	// Failing.OnExit calling diagCancel on DeprovisionRequested
	// preemption. Shutdown will still complete promptly because the
	// actor's waitForWorkers is bounded by workExitWaitTimeout, after
	// which the diag goroutine becomes an orphan that finishes (or gets
	// killed with the process) on its own schedule.
	diagCtx, diagCancel := context.WithTimeout(context.Background(), diagnosticsGatherTimeout)
	lsm.actor.diagCancel = diagCancel
	lsm.actor.workers.Add()
	lsm.actor.cfg.WG.Go(func() {
		defer lsm.actor.workers.Done()
		lsm.actor.gatherDiagAsync(diagCtx, containerID, info)
	})
	return nil
}

// onEnterRestarting / onEnterUpdating are the Ready|Failed|Failing →
// Restarting|Updating entry actions. They are the SOLE writers of
// prov.Status (Restarting/Updating) and prov.CallbackURL for the
// restart/update paths (ENG-230). Fired synchronously inside
// handleRestartRequested/handleUpdateRequested's fireAndVerify, BEFORE
// the ack, so the "Restart()/Update() returns ⇒ Status is
// Restarting/Updating" contract relied on by api/handlers.go holds.
func (lsm *leaseSM) onEnterRestarting(ctx context.Context, args ...any) error {
	return lsm.applyReplaceEntry(args, backend.ProvisionStatusRestarting)
}

func (lsm *leaseSM) onEnterUpdating(ctx context.Context, args ...any) error {
	return lsm.applyReplaceEntry(args, backend.ProvisionStatusUpdating)
}

// applyReplaceEntry flips Status to the requested replace state and, when
// the request carried a non-empty CallbackURL, applies it — all under one
// UpdateFn critical section. No metric/log side effect inside the closure,
// per the LeaseProvisionStore idempotence contract.
func (lsm *leaseSM) applyReplaceEntry(args []any, status backend.ProvisionStatus) error {
	var callbackURL string
	if len(args) > 0 {
		if a, ok := args[0].(replaceEntryArgs); ok {
			callbackURL = a.CallbackURL
		}
	}
	// Capture whether the lease was active (Status==Ready) BEFORE overwriting
	// it — this is the actor-authoritative gauge key the replace-outcome
	// entry actions use (see LeaseActor.replaceWasActive). Read inside the
	// closure (under the store lock) so it's consistent with the overwrite;
	// the actor-field assignment happens after the closure (no side effect
	// inside UpdateFn, per the idempotence contract).
	var wasActive bool
	lsm.actor.cfg.ProvisionStore.UpdateFn(lsm.actor.leaseUUID, func(p *ProvisionState) {
		wasActive = p.Status == backend.ProvisionStatusReady
		p.Status = status
		if callbackURL != "" {
			p.CallbackURL = callbackURL
		}
	})
	lsm.actor.replaceWasActive = wasActive
	return nil
}

// diagnosticsGatherTimeout bounds the async diag goroutine's lifetime
// independently of backend shutdown. Must be short enough that a wedged
// Docker daemon doesn't leak goroutines indefinitely; long enough that
// normal log fetches complete. Keep this comfortably below
// workExitWaitTimeout so diag can finish and sendTerminal before the
// actor's shutdown-drain gives up on it.
const diagnosticsGatherTimeout = 30 * time.Second

// onExitFailing cancels the in-flight diag goroutine whenever we leave
// Failing — whether by DiagGathered (normal) or DeprovisionRequested
// (preemption) — then waits via workers. Three-layer suppression of
// stale Failed callbacks: cancel signal (happy path), workers.Zero wait
// (prevents post-OnExit race where the goroutine is mid-sendTerminal),
// and the Ignore declarations on Failed/Deprovisioning (backstop for
// trigger reordering). Bounded by workExitWaitTimeout via waitForWorkers.
func (lsm *leaseSM) onExitFailing(ctx context.Context, args ...any) error {
	if lsm.actor.diagCancel != nil {
		lsm.actor.diagCancel()
		lsm.actor.diagCancel = nil
	}
	lsm.actor.waitForWorkers()
	return nil
}

// onExitProvisioning is the analog for Provision/Restart/Update. Same
// structural invariant as onExitFailing: cancel then wait via workers.
// Closes the orphan-containers race — the goroutine's
// pre-publish-then-sendTerminal sequence is observable to the preempting
// doDeprovision, and the wait prevents an orphan container set from being
// stranded when the handler is still in flight.
func (lsm *leaseSM) onExitProvisioning(ctx context.Context, args ...any) error {
	if lsm.actor.workCancel != nil {
		lsm.actor.workCancel()
		lsm.actor.workCancel = nil
	}
	lsm.actor.waitForWorkers()
	return nil
}

// workExitWaitTimeout bounds how long Provisioning/Restarting/Updating.OnExit
// blocks waiting for the work goroutine to finish. Must exceed the worker's
// worst-case failure-path cleanup budget: up to 30s to capture container
// logs plus up to 30s for container removal / rollback (the two phases run
// sequentially in the failure defer), with additional slack for Docker
// call cancellation to propagate. Under-budgeting here lets the actor exit
// while work is still in flight, which can drop the terminal SM event.
const workExitWaitTimeout = 75 * time.Second

// onEnterReadyFromProvision fires when doProvision signals success. Owns
// the Status flip, ContainerIDs/Manifest/ServiceContainers update, gauge
// increment, and Success callback emission.
func (lsm *leaseSM) onEnterReadyFromProvision(ctx context.Context, args ...any) error {
	if len(args) < 1 {
		return fmt.Errorf("onEnterReadyFromProvision: missing result")
	}
	result, ok := args[0].(ProvisionSuccessResult)
	if !ok {
		return fmt.Errorf("onEnterReadyFromProvision: arg not ProvisionSuccessResult")
	}
	cfg := &lsm.actor.cfg
	leaseUUID := lsm.actor.leaseUUID

	var callbackURL string
	var applied bool
	cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
		p.Status = backend.ProvisionStatusReady
		p.ContainerIDs = result.ContainerIDs
		p.LastError = ""
		if result.StackManifest != nil {
			p.StackManifest = result.StackManifest
		}
		if result.ServiceContainers != nil {
			p.ServiceContainers = result.ServiceContainers
		}
		callbackURL = p.CallbackURL
		applied = true
	})
	if applied {
		cfg.Metrics.ActiveProvisionsInc()
	}

	cfg.SendCallbackFn(leaseUUID, callbackURL, backend.CallbackStatusSuccess, "")
	return nil
}

// onEnterReadyFromReplaceCompleted fires when doReplace* signals success.
// Owns Status flip, ContainerIDs/ServiceContainers update, optional
// OnSuccess hook (update flow sets Manifest/StackManifest), gauge
// increment (iff the lease was NOT active at replace-start), and Success
// callback emission.
func (lsm *leaseSM) onEnterReadyFromReplaceCompleted(ctx context.Context, args ...any) error {
	if len(args) < 1 {
		return fmt.Errorf("onEnterReadyFromReplaceCompleted: missing result")
	}
	result, ok := args[0].(ReplaceSuccessResult)
	if !ok {
		return fmt.Errorf("onEnterReadyFromReplaceCompleted: arg not ReplaceSuccessResult")
	}
	cfg := &lsm.actor.cfg
	leaseUUID := lsm.actor.leaseUUID

	// Inc iff the lease was NOT active (Status != Ready) when the replace
	// began — actor-observed via replaceWasActive, NOT a stale prelude
	// route-time snapshot (which under-counts the death-before-restart
	// ordering). Computed outside the closure: it's an actor field, not
	// ProvisionState.
	incActive := !lsm.actor.replaceWasActive
	var callbackURL string
	applied := cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
		p.ContainerIDs = result.ContainerIDs
		if result.ServiceContainers != nil {
			p.ServiceContainers = result.ServiceContainers
		}
		p.Status = backend.ProvisionStatusReady
		p.LastError = ""
		if result.OnSuccess != nil {
			result.OnSuccess(p)
		}
		callbackURL = p.CallbackURL
	})
	// Gate the gauge on applied (UpdateFn ran ⇒ lease still exists): never
	// move the gauge for a lease the closure didn't touch. On every reachable
	// path applied is true (entry action runs inside Fire after the SM
	// transitioned, on the serial actor goroutine), so this is behavior-
	// identical; it only closes the unreachable lease-removed drift, matching
	// onEnterReadyFromProvision.
	if applied && incActive {
		cfg.Metrics.ActiveProvisionsInc()
	}

	cfg.SendCallbackFn(leaseUUID, callbackURL, backend.CallbackStatusSuccess, "")
	return nil
}

// onEnterReadyFromReplaceRecovered fires when doReplace* failed but the
// rollback restored the lease to Ready (or the preflight check failed
// without touching containers). Status ends up Ready; LastError is set
// to the rich failure diagnostic. For the restart-with-oldStopped case,
// LastError is cleared because we're back to the exact same state as
// before the restart. Gauge: Inc iff the lease was NOT active at
// replace-start (e.g. recovered-from-Failed/Failing) — previously this
// path did no gauge op, under-counting that case.
func (lsm *leaseSM) onEnterReadyFromReplaceRecovered(ctx context.Context, args ...any) error {
	if len(args) < 1 {
		return fmt.Errorf("onEnterReadyFromReplaceRecovered: missing info")
	}
	info, ok := args[0].(ReplaceFailureInfo)
	if !ok {
		return fmt.Errorf("onEnterReadyFromReplaceRecovered: arg not ReplaceFailureInfo")
	}
	cfg := &lsm.actor.cfg
	leaseUUID := lsm.actor.leaseUUID

	// Inc iff the lease was NOT active when the replace began (actor-observed
	// via replaceWasActive); ending in Ready re-counts it. Computed outside
	// the closure: it's an actor field, not ProvisionState.
	incActive := !lsm.actor.replaceWasActive
	var callbackURL string
	var diagSnap shared.DiagnosticEntry
	applied := cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
		p.LastError = info.LastError
		p.FailCount++
		p.Status = backend.ProvisionStatusReady
		// Restart: if we actually stopped old containers and then restored
		// them, we're back to the exact same state — no persistent error.
		// Update: keep LastError so the UI shows why the update failed.
		if info.OldStopped && info.Operation == "restart" {
			p.LastError = ""
		}
		diagSnap = DiagnosticSnapshot(p)
		callbackURL = p.CallbackURL
	})
	// Gate the gauge on applied (lease still exists); behavior-identical on
	// reachable paths — see onEnterReadyFromReplaceCompleted.
	if applied && incActive {
		cfg.Metrics.ActiveProvisionsInc()
	}

	if diagSnap.LeaseUUID != "" {
		// Use logs captured by doReplace*'s defer BEFORE rollback tore
		// the failed containers down — post-cleanup log fetches would
		// hit already-deleted containers and record an empty entry.
		cfg.PersistDiagnosticsWithLogsFn(diagSnap, info.Logs)
	}

	cfg.SendCallbackFn(leaseUUID, callbackURL, backend.CallbackStatusFailed, info.CallbackErr)
	return nil
}

// onEnterFailedFromReplace fires when doReplace* failed AND rollback
// failed (or no rollback was possible). Status ends up Failed; gauge is
// decremented iff the lease WAS active at replace-start (the operation
// took a counted lease and ended it Failed).
func (lsm *leaseSM) onEnterFailedFromReplace(ctx context.Context, args ...any) error {
	if len(args) < 1 {
		return fmt.Errorf("onEnterFailedFromReplace: missing info")
	}
	info, ok := args[0].(ReplaceFailureInfo)
	if !ok {
		return fmt.Errorf("onEnterFailedFromReplace: arg not ReplaceFailureInfo")
	}
	cfg := &lsm.actor.cfg
	leaseUUID := lsm.actor.leaseUUID

	// Dec iff the lease WAS active when the replace began (actor-observed via
	// replaceWasActive, NOT a stale prelude route-time snapshot). Computed
	// outside the closure: it's an actor field, not ProvisionState.
	decActive := lsm.actor.replaceWasActive
	var callbackURL string
	var diagSnap shared.DiagnosticEntry
	applied := cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
		p.LastError = info.LastError
		p.FailCount++
		p.Status = backend.ProvisionStatusFailed
		diagSnap = DiagnosticSnapshot(p)
		callbackURL = p.CallbackURL
	})
	// Gate the gauge on applied (lease still exists); behavior-identical on
	// reachable paths — see onEnterReadyFromReplaceCompleted.
	if applied && decActive {
		cfg.Metrics.ActiveProvisionsDec()
	}

	if diagSnap.LeaseUUID != "" {
		// Use logs captured by doReplace*'s defer BEFORE rollback tore
		// the failed containers down — post-cleanup log fetches would
		// hit already-deleted containers and record an empty entry.
		cfg.PersistDiagnosticsWithLogsFn(diagSnap, info.Logs)
	}

	cfg.SendCallbackFn(leaseUUID, callbackURL, backend.CallbackStatusFailed, info.CallbackErr)
	return nil
}

// onEnterFailedFromProvision fires when doProvision signals a failure.
// Owns Status flip, FailCount++, LastError update, persistDiagnostics,
// and the Failed callback. Cleanup (container/volume removal) still
// runs in the goroutine's defer — it's I/O that shouldn't block the
// actor.
func (lsm *leaseSM) onEnterFailedFromProvision(ctx context.Context, args ...any) error {
	if len(args) < 1 {
		return fmt.Errorf("onEnterFailedFromProvision: missing error info")
	}
	info, ok := args[0].(provisionErrorInfo)
	if !ok {
		return fmt.Errorf("onEnterFailedFromProvision: arg not provisionErrorInfo")
	}
	cfg := &lsm.actor.cfg
	leaseUUID := lsm.actor.leaseUUID

	var callbackURL string
	var diagSnap shared.DiagnosticEntry
	cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
		p.Status = backend.ProvisionStatusFailed
		p.FailCount++
		p.LastError = info.lastError
		diagSnap = DiagnosticSnapshot(p)
		callbackURL = p.CallbackURL
	})

	if diagSnap.LeaseUUID != "" {
		// Use the logs captured by doProvision's cleanup defer (before the
		// failed containers were removed). If the worker didn't capture
		// any (e.g., failure before any containers were created), the
		// entry is persisted without logs.
		cfg.PersistDiagnosticsWithLogsFn(diagSnap, info.logs)
	}

	cfg.SendCallbackFn(leaseUUID, callbackURL, backend.CallbackStatusFailed, info.callbackErr)
	return nil
}

// onEnterFailedFromDiag runs as the Failing→Failed entry action when
// DiagGathered fires. Owns all state mutations for this transition: flips
// Status, applies diag to LastError, persists diagnostics, emits the
// terminal Failed callback. Running in the actor's goroutine means no
// mutex races with the gathering goroutine (which is pure I/O now).
func (lsm *leaseSM) onEnterFailedFromDiag(ctx context.Context, args ...any) error {
	if len(args) < 1 {
		return fmt.Errorf("onEnterFailedFromDiag: missing diag info")
	}
	result, ok := args[0].(diagResult)
	if !ok {
		return fmt.Errorf("onEnterFailedFromDiag: arg not diagResult")
	}
	cfg := &lsm.actor.cfg
	leaseUUID := lsm.actor.leaseUUID

	var callbackURL string
	var failCount int
	var diagSnap shared.DiagnosticEntry
	var diagContainerIDs []string
	var diagKeys map[string]string
	exists := cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
		p.Status = backend.ProvisionStatusFailed
		if result.diag != "" {
			p.LastError = errMsgContainerExited + ": " + result.diag
		}
		callbackURL = p.CallbackURL
		failCount = p.FailCount
		diagSnap = DiagnosticSnapshot(p)
		diagContainerIDs = append([]string(nil), p.ContainerIDs...)
		diagKeys = ContainerLogKeys(p)
	})
	if !exists {
		return nil
	}

	// Persist diagnostics (bbolt write). Runs in the actor goroutine —
	// briefly blocks other messages for this lease, but matches the
	// "actor owns all state" invariant. Bbolt writes are ~ms.
	if diagSnap.LeaseUUID != "" {
		cfg.PersistDiagnosticsFn(diagSnap, diagContainerIDs, diagKeys)
	}

	cfg.SendCallbackFn(leaseUUID, callbackURL, backend.CallbackStatusFailed, errMsgContainerExited)

	logAttrs := []any{
		"lease_uuid", leaseUUID,
		"container_id", ShortID(result.containerID),
		"fail_count", failCount,
	}
	if result.info != nil && result.info.ServiceName != "" {
		logAttrs = append(logAttrs, "service_name", result.info.ServiceName)
	}
	cfg.Logger.Warn("container death detected via events API", logAttrs...)
	return nil
}

// diagResult carries gather output from the async goroutine into the Failed
// entry action via Fire args.
//
// info is the substrate-agnostic InstanceState the SM consumes. The service
// name (used for death-event logging continuity in multi-service stacks)
// rides along on info.ServiceName — InstanceState carries it because it is
// the only piece of substrate-shaped metadata the SM otherwise needs at the
// post-death log line, and lifting it into InstanceState avoids a parallel
// pending-state field. Substrate adapters populate ServiceName from their
// own conventions (Docker labels, K8s pod annotations); leases without a
// meaningful service name leave it empty.
type diagResult struct {
	containerID string
	info        *InstanceState
	diag        string
}

// ProvisionSuccessResult carries doProvision output into
// Ready.OnEntryFrom(evProvisionCompleted) via Fire args. Every provision
// is stack-shaped post-Task-15; StackManifest + ServiceContainers are
// always populated. Exported because the substrate's provision worker
// closure constructs
// the value and returns it via the ProvisionRequestedMsg.Work signature.
type ProvisionSuccessResult struct {
	ContainerIDs      []string
	StackManifest     *manifest.StackManifest
	ServiceContainers map[string][]string
}

// provisionErrorInfo carries doProvision failure data into
// Failed.OnEntryFrom(evProvisionErrored). callbackErr is the on-chain-safe
// hardcoded message; lastError is the full diagnostic string stashed in
// provision.LastError for authenticated API access. logs is the
// pre-captured log map (fetched by doProvision BEFORE its cleanup defer
// removed the failed containers). If nil (e.g., failure before any
// containers were created), onEnterFailedFromProvision persists the
// diagnostic entry without container logs — it does not re-fetch, since
// the containers are gone by the time the SM entry action runs.
type provisionErrorInfo struct {
	callbackErr string
	lastError   string
	logs        map[string]string
}

// replaceEntryArgs carries the new CallbackURL from a Restart/Update
// request message into the onEnterRestarting / onEnterUpdating entry
// actions via Fire args, so the actor (not the HTTP prelude) is the
// sole writer of prov.CallbackURL for these paths.
type replaceEntryArgs struct{ CallbackURL string }

// ReplaceSuccessResult carries doReplaceContainers / doReplaceStackContainers
// success output into onEnterReadyFromReplace. The goroutine returns these;
// the entry action writes them to provision under a single lock acquisition.
// OnSuccess is the optional hook supplied by the caller (update flow sets
// Manifest/StackManifest there). Exported because the substrate's replace
// worker closure builds the value and the SM entry action reads it.
type ReplaceSuccessResult struct {
	ContainerIDs      []string
	ServiceContainers map[string][]string // non-nil for stack
	OnSuccess         func(*ProvisionState)
}

// ReplaceFailureInfo carries doReplace* failure data. Used by both
// onEnterReadyFromReplaceRecovered (Status ends up Ready) and
// onEnterFailedFromReplace (Status ends up Failed). The entry actions
// set LastError, increment FailCount, persist diagnostics, and adjust
// the activeProvisions gauge via the actor-observed replaceWasActive.
// Exported because the substrate's replace worker closure builds the value.
type ReplaceFailureInfo struct {
	Operation   string // "restart" or "update"
	OldStopped  bool   // only meaningful on the recovery path (restart-with-oldStopped clears LastError)
	CallbackErr string
	LastError   string
	// Logs is the pre-captured container-log map from the NEW (failed)
	// containers. Populated by doReplace*'s defer BEFORE rollback tears
	// those containers down — persistDiagnostics would otherwise find
	// them gone and record an empty entry. For stacks, keys are
	// "serviceName/instanceIndex"; for single-container, raw indices.
	Logs map[string]string
}

// ReplaceResult is doReplace*'s return value bundling everything the
// goroutine wrapper needs to fire the right SM event. The callback path
// depends on (Err, recovered) where recovered = RecoveredIfSourceActive ?
// LeaseActor.replaceWasActive : Restored (computed by spawnReplaceWorker):
//
//	Err == nil             → fire evReplaceCompleted with .Success
//	Err != nil, recovered  → fire evReplaceRecovered with .Failure
//	Err != nil, !recovered → fire evReplaceFailed    with .Failure
//
// Exported because the substrate's replace worker constructs the value
// and the actor dispatches on its fields.
type ReplaceResult struct {
	CallbackErr string
	Err         error
	Restored    bool                 // only meaningful if Err != nil && !RecoveredIfSourceActive
	Success     ReplaceSuccessResult // populated when Err == nil
	Failure     ReplaceFailureInfo   // populated when Err != nil
	// RecoveredIfSourceActive, when true, tells the actor to derive the
	// recovered-vs-failed outcome from LeaseActor.replaceWasActive (the
	// actor-observed SM source) INSTEAD of Restored. Set ONLY by doRestart's
	// preflight-failure branch: no container was touched, so "recovered to
	// Ready" is correct iff the lease was Ready/running at replace-start —
	// which the prelude's route-time status snapshot got wrong under the
	// death-before-queued-restart ordering (ENG-230 PR#93 finding). Every
	// other failure leaves this false: update-preflight stays Restored=false
	// (intentional), post-replace keeps Restored=rollback result.
	RecoveredIfSourceActive bool
}

// readProvisionStatus snapshots the lease's current provision status via
// the substrate-agnostic LeaseProvisionStore seam. Used on SM creation to
// pick the initial state. Returns ProvisionStatusProvisioning when the
// lease is unknown (the same default as the prior in-docker reach-through).
func readProvisionStatus(actor *LeaseActor) backend.ProvisionStatus {
	state, ok := actor.cfg.ProvisionStore.Get(actor.leaseUUID)
	if !ok {
		return backend.ProvisionStatusProvisioning
	}
	return state.Status
}

// gatherDiagAsync runs in a goroutine (spawned by onEnterFailing), doing
// pure I/O: Docker log fetch. All state mutations (LastError update,
// persist, callback) are done by onEnterFailedFromDiag, which runs in
// the actor's goroutine after Fire commits the Failing→Failed transition.
//
// Tracked by workers at the spawn site (onEnterFailing), so
// Failing.OnExit's waitForWorkers blocks (up to workExitWaitTimeout)
// until this goroutine has returned. Under normal operation that means
// sendTerminal lands before the actor proceeds past the transition;
// on timeout the SM proceeds and a late sendTerminal is refused by the
// Deprovisioning.Ignore backstop.
func (a *LeaseActor) gatherDiagAsync(ctx context.Context, containerID string, info *InstanceState) {
	// Same single-send defer pattern as spawnProvisionWorker /
	// spawnReplaceWorker (see lease_actor.go). One sendTerminal site;
	// the recover overrides terminalMsg on panic, the normal path
	// sets it on completion. ctx-cancel is special: we suppress the
	// send entirely (the Deprovisioning.Ignore path handles that case).
	var terminalMsg LeaseMessage
	var event = "diag_gathered"
	var suppress bool
	defer func() {
		if suppress {
			return
		}
		if terminalMsg == nil {
			terminalMsg = diagGatheredMsg{
				result: diagResult{containerID: containerID, info: info, diag: ""},
			}
			event = "diag_no_result"
		}
		if !a.sendTerminal(terminalMsg) {
			a.cfg.Metrics.TerminalEventDropped(event)
			a.cfg.Logger.Warn("terminal diag event dropped (actor exited or inbox wedged)",
				"lease_uuid", a.leaseUUID,
			)
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			a.cfg.Logger.Error("diag worker panic — recovering to keep fred alive",
				"lease_uuid", a.leaseUUID,
				"container_id", ShortID(containerID),
				"panic", r,
				"stack", string(debug.Stack()),
			)
			a.cfg.Metrics.WorkerPanic("diag")
			// Drive Failing→Failed via an empty diagGatheredMsg —
			// Deprovisioning.Ignore still covers the case where the
			// SM has already moved past Failing.
			terminalMsg = diagGatheredMsg{
				result: diagResult{containerID: containerID, info: info, diag: ""},
			}
			event = "diag_panic"
			suppress = false
		}
	}()
	diag := a.cfg.Diag.GatherDiagnostics(ctx, containerID, info)
	// Distinguish the two cancellation causes:
	//   - context.Canceled: diagCancel() fired from Failing.OnExit (preempt
	//     by Deprovision/Restart/Update). SM has left Failing; any
	//     diagGatheredMsg would hit Deprovisioning.Ignore anyway, so
	//     suppress to avoid a spurious sendTerminal.
	//   - context.DeadlineExceeded: the 30s diagnosticsGatherTimeout
	//     elapsed without any preempt. SM is still in Failing, and
	//     nothing else will drive Failing→Failed. We MUST send the
	//     diagGatheredMsg (containerFailureDiagnostics always returns at
	//     least "exit_code=N") so the SM transitions and the Failed
	//     callback fires; otherwise the lease wedges indefinitely.
	if errors.Is(ctx.Err(), context.Canceled) {
		suppress = true
		return
	}
	terminalMsg = diagGatheredMsg{
		result: diagResult{containerID: containerID, info: info, diag: diag},
	}
}

// errMsgContainerExited is the canonical on-chain-safe callback message
// for containers that exit unexpectedly, used by the Failing/Failed
// transition's callback path. The exported alias ErrMsgContainerExited
// is what substrate adapters reach for — divergence between docker/'s
// recover code and the SM's callback emission would produce different
// on-chain strings for the same failure, violating the
// callback-error-sanitization invariant. Both names refer to the same
// constant so drift is structurally impossible.
const errMsgContainerExited = "container exited unexpectedly"

// ErrMsgContainerExited is the exported view of errMsgContainerExited for
// substrate adapters that emit failure callbacks outside the SM's own
// onEnter actions (e.g., the Docker backend's recover.go records this
// string as the on-chain Error when reconstructing a Failed lease's
// LastError on startup). All on-chain callback strings for
// "container-exited" failures MUST reference this constant.
const ErrMsgContainerExited = errMsgContainerExited

// errMsgInternal is the canonical hardcoded on-chain-safe callback
// message used when a worker goroutine panics. Full diagnostics (panic
// value, stack) go to ProvisionState.LastError and the structured log;
// the callback message itself stays generic so internals do not leak
// on-chain. The exported alias ErrMsgInternal mirrors this for substrate
// adapters that emit failure callbacks outside the SM.
const errMsgInternal = "internal error"

// ErrMsgInternal is the exported view of errMsgInternal for substrate
// adapters; see ErrMsgContainerExited's docstring for the
// "no-divergence" rationale.
const ErrMsgInternal = errMsgInternal

// shortIDLen is the truncation length for ShortID; matches Docker's
// stringid.TruncateID 12-character convention so log lines stay
// consistent with Docker CLI output for the same container.
const shortIDLen = 12

// ShortID truncates a substrate-side instance ID to a 12-character
// shorthand for use in lease/container log formatting only. NOT a
// general string-truncation utility — speculative reuse for unrelated
// truncation needs should write its own helper.
//
// Mirrors github.com/docker/docker/pkg/stringid.TruncateID semantics
// (strip leading "type:" prefix such as "sha256:", then byte-bound to
// 12 chars) without taking the docker library as a dependency. The
// prefix strip is a no-op when there is no colon, and the byte-bounded
// truncate is universal — K3s adapters can safely call this on pod
// UIDs. Kept here so leasesm stays free of any docker import.
func ShortID(id string) string {
	if i := strings.IndexRune(id, ':'); i >= 0 {
		id = id[i+1:]
	}
	if len(id) > shortIDLen {
		id = id[:shortIDLen]
	}
	return id
}

// DiagnosticSnapshot captures the ProvisionState fields needed for
// diagnostics persistence. Built inside an UpdateFn closure (under the
// store's mutex) so the snapshot is consistent; PersistDiagnosticsFn /
// PersistDiagnosticsWithLogsFn write it out after the closure returns.
// Use only for diagnostics-store writes; not a general ProvisionState
// projection helper.
//
// Omits SKU, Image, Status, Quantity, CallbackURL, Items, ContainerIDs,
// Manifest, StackManifest, and ServiceContainers — those are operational
// state that's either already on the lease's authoritative record or
// not relevant to the on-disk diagnostic blob a future operator reads
// to understand a failure.
func DiagnosticSnapshot(prov *ProvisionState) shared.DiagnosticEntry {
	return shared.DiagnosticEntry{
		LeaseUUID:    prov.LeaseUUID,
		ProviderUUID: prov.ProviderUUID,
		Tenant:       prov.Tenant,
		Error:        prov.LastError,
		FailCount:    prov.FailCount,
		CreatedAt:    time.Now(),
	}
}

// ContainerLogKeys builds a containerID → display key mapping for stack
// provisions (e.g., "web/0", "web/1") so diagnostic log entries use the
// service-name view rather than raw indices. Returns nil for non-stack
// provisions; nil is the documented "default index-based keys" signal
// to PersistDiagnosticsFn. Use only for diagnostics-log key formatting;
// not a general ServiceContainers projection helper.
//
// Reads ONLY ServiceContainers — never inspects Manifest / ContainerIDs
// / Items / etc. The mapping is shape-only (containerID → service/index)
// and stays stable across stack lifecycle events as long as the
// ServiceContainers map is consistent.
func ContainerLogKeys(prov *ProvisionState) map[string]string {
	if prov == nil || len(prov.ServiceContainers) == 0 {
		return nil
	}
	keys := make(map[string]string)
	for svcName, cids := range prov.ServiceContainers {
		for i, cid := range cids {
			keys[cid] = fmt.Sprintf("%s/%d", svcName, i)
		}
	}
	return keys
}
