package leasesm

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"slices"
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
	// evRestoreRequested rides the EXISTING replace machinery (same as
	// evRestartRequested/evUpdateRequested) but is permitted ONLY from
	// Provisioning → Restarting: a restore's NEW lease is reserved at
	// Status=Provisioning (it was never running), whereas a restart/update
	// fire from Ready/Failed. Entering Restarting via evRestoreRequested
	// reuses onEnterRestarting; the provision store derives active counts
	// directly from the resulting status transitions.
	evRestoreRequested
	evProvisionRequested
	evProvisionCompleted
	evProvisionErrored
	evDiagGathered
	evContainersRemoved
	// evCohortDiverged is a recovery-authored, substrate-agnostic signal that
	// observed instances no longer match the durable desired release. It is
	// separate from evContainerDied so no fake instance identity or inspection
	// result is needed to reach the fail-closed state.
	evCohortDiverged
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
	// Recovery-only equivalents are reachable solely through the opaque
	// MaintenanceRecovered message constructors. Keeping distinct triggers
	// prevents an old worker terminal message from correcting a later state.
	evMaintenanceRecoveredSuccess
	evMaintenanceRecoveredFailureReady
	evMaintenanceRecoveredFailureFailed
	evMaintenanceRecoveredSuccessRuntimeFailed
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
	case evRestoreRequested:
		return "RestoreRequested"
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
	case evCohortDiverged:
		return "CohortDiverged"
	case evReplaceCompleted:
		return "ReplaceCompleted"
	case evReplaceRecovered:
		return "ReplaceRecovered"
	case evReplaceFailed:
		return "ReplaceFailed"
	case evMaintenanceRecoveredSuccess:
		return "MaintenanceRecoveredSuccess"
	case evMaintenanceRecoveredFailureReady:
		return "MaintenanceRecoveredFailureReady"
	case evMaintenanceRecoveredFailureFailed:
		return "MaintenanceRecoveredFailureFailed"
	case evMaintenanceRecoveredSuccessRuntimeFailed:
		return "MaintenanceRecoveredSuccessRuntimeFailed"
	}
	return fmt.Sprintf("leaseEvent(%d)", int(e))
}

// leaseSM wraps a stateless.StateMachine with a typed façade. Each lease
// actor owns one SM; transitions are serialized by the actor's inbox.
type leaseSM struct {
	actor *LeaseActor
	sm    *stateless.StateMachine
}

// reservedState is deliberately distinct from the externally visible
// Provisioning projection. A newly admitted provision/restore has reserved its
// resources, but no actor-owned worker exists until the corresponding command
// crosses Reserved -> Provisioning/Restarting. Keeping those facts distinct
// makes a second command unrepresentable as an idempotent self-transition.
type reservedState uint8

const leaseReserved reservedState = 1

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
	// stateless itself is dynamically typed. Register exact payload shapes as a
	// backstop so malformed internal calls fail before the library mutates state;
	// production code reaches these triggers only through the typed methods below.
	sm.SetTriggerParameters(
		evContainerDied,
		reflect.TypeFor[string](),
		reflect.TypeFor[shared.RuntimeGenerationProof](),
	)
	sm.SetTriggerParameters(evDeprovisionRequested)
	sm.SetTriggerParameters(evRestartRequested, reflect.TypeFor[replaceEntryArgs]())
	sm.SetTriggerParameters(evUpdateRequested, reflect.TypeFor[replaceEntryArgs]())
	sm.SetTriggerParameters(evRestoreRequested, reflect.TypeFor[replaceEntryArgs]())
	sm.SetTriggerParameters(evProvisionRequested)
	sm.SetTriggerParameters(evProvisionCompleted, reflect.TypeFor[ProvisionSuccessResult]())
	sm.SetTriggerParameters(evProvisionErrored, reflect.TypeFor[provisionErrorInfo]())
	sm.SetTriggerParameters(evDiagGathered, reflect.TypeFor[diagResult]())
	sm.SetTriggerParameters(evCohortDiverged, reflect.TypeFor[shared.RuntimeGenerationProof]())
	sm.SetTriggerParameters(evReplaceCompleted, reflect.TypeFor[ReplaceSuccessResult]())
	sm.SetTriggerParameters(evReplaceRecovered, reflect.TypeFor[ReplaceFailureInfo]())
	sm.SetTriggerParameters(evReplaceFailed, reflect.TypeFor[ReplaceFailureInfo]())
	sm.SetTriggerParameters(evMaintenanceRecoveredSuccess, reflect.TypeFor[ReplaceSuccessResult]())
	sm.SetTriggerParameters(evMaintenanceRecoveredFailureReady, reflect.TypeFor[maintenanceRecoveryFailureArgs]())
	sm.SetTriggerParameters(evMaintenanceRecoveredFailureFailed, reflect.TypeFor[maintenanceRecoveryFailureArgs]())
	sm.SetTriggerParameters(evMaintenanceRecoveredSuccessRuntimeFailed, reflect.TypeFor[maintenanceRecoveryFailureArgs]())

	// Reserved is an actor-control state, not a tenant-visible status. Both a
	// fresh provision and restore publish Provisioning while awaiting actor
	// admission, but each command is accepted exactly once from this state.
	sm.Configure(leaseReserved).
		Permit(evProvisionRequested, backend.ProvisionStatusProvisioning).
		Permit(evRestoreRequested, backend.ProvisionStatusRestarting).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning)

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
		Permit(evCohortDiverged, backend.ProvisionStatusFailed).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning).
		Permit(evRestartRequested, backend.ProvisionStatusRestarting).
		Permit(evUpdateRequested, backend.ProvisionStatusUpdating).
		Permit(evMaintenanceRecoveredFailureFailed, backend.ProvisionStatusFailed).
		Permit(evMaintenanceRecoveredSuccessRuntimeFailed, backend.ProvisionStatusFailed)

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
		Ignore(evCohortDiverged).
		Ignore(evDiagGathered).
		Permit(evMaintenanceRecoveredSuccess, backend.ProvisionStatusReady).
		Permit(evMaintenanceRecoveredFailureReady, backend.ProvisionStatusReady)

	// Deprovisioning: work runs in actor.handleDeprovision after Fire returns.
	// Ignore die events and any stale DiagGathered from a canceled-too-late
	// async goroutine.
	sm.Configure(backend.ProvisionStatusDeprovisioning).
		Ignore(evContainerDied).
		Ignore(evDiagGathered).
		// Durable finalizers are retried while the live projection remains
		// Deprovisioning. This idempotent self-event authorizes only another run of
		// the same teardown; it cannot transition back to a mutable workload state.
		Ignore(evDeprovisionRequested)

	// Unknown is fail-closed for creation/replacement, but close remains safe:
	// teardown is idempotent and is the only transition out of this state.
	sm.Configure(backend.ProvisionStatusUnknown).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning)

	// Provisioning: async goroutine is running. Exits by ProvisionCompleted
	// (→ Ready, emit Success), ProvisionErrored (→ Failed, emit Failed), or
	// DeprovisionRequested (→ Deprovisioning, OnExit cancels goroutine — the
	// structural suppression for Provision+Deprovision races, analogous to
	// Failing's cancel-on-exit mechanism).
	sm.Configure(backend.ProvisionStatusProvisioning).
		OnEntryFrom(evProvisionRequested, lsm.onEnterProvisioning).
		Permit(evProvisionCompleted, backend.ProvisionStatusReady).
		Permit(evProvisionErrored, backend.ProvisionStatusFailed).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning).
		OnExit(lsm.onExitProvisioning).
		Ignore(evContainerDied)

	// Restarting/Updating: same shape as Provisioning — a goroutine is
	// doing the work; OnExit cancels it on preemption. The goroutine fires
	// evReplaceCompleted (full success), evReplaceRecovered (failure but
	// lease ended up Ready via rollback / preflight-restored), or
	// evReplaceFailed (ended up Failed).
	//
	// onEnterRestarting/onEnterUpdating are the SOLE writers of
	// prov.Status (Restarting/Updating) + the callback URL pair for these
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
		// Restore (ENG-325) enters Restarting from Provisioning and reuses the
		// SAME entry action: applyReplaceEntry writes Status=Restarting + the
		// callback URL pair. The provision store derives readiness accounting
		// directly from the resulting status transitions.
		OnEntryFrom(evRestoreRequested, lsm.onEnterRestarting).
		Permit(evReplaceCompleted, backend.ProvisionStatusReady).
		Permit(evReplaceRecovered, backend.ProvisionStatusReady).
		Permit(evReplaceFailed, backend.ProvisionStatusFailed).
		Permit(evMaintenanceRecoveredSuccess, backend.ProvisionStatusReady).
		Permit(evMaintenanceRecoveredFailureReady, backend.ProvisionStatusReady).
		Permit(evMaintenanceRecoveredFailureFailed, backend.ProvisionStatusFailed).
		Permit(evMaintenanceRecoveredSuccessRuntimeFailed, backend.ProvisionStatusFailed).
		Permit(evDeprovisionRequested, backend.ProvisionStatusDeprovisioning).
		OnExit(lsm.onExitProvisioning).
		Ignore(evContainerDied)
	sm.Configure(backend.ProvisionStatusUpdating).
		OnEntryFrom(evUpdateRequested, lsm.onEnterUpdating).
		Permit(evReplaceCompleted, backend.ProvisionStatusReady).
		Permit(evReplaceRecovered, backend.ProvisionStatusReady).
		Permit(evReplaceFailed, backend.ProvisionStatusFailed).
		Permit(evMaintenanceRecoveredSuccess, backend.ProvisionStatusReady).
		Permit(evMaintenanceRecoveredFailureReady, backend.ProvisionStatusReady).
		Permit(evMaintenanceRecoveredFailureFailed, backend.ProvisionStatusFailed).
		Permit(evMaintenanceRecoveredSuccessRuntimeFailed, backend.ProvisionStatusFailed).
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
		OnEntryFrom(evReplaceRecovered, lsm.onEnterReadyFromReplaceRecovered).
		OnEntryFrom(evMaintenanceRecoveredSuccess, lsm.onEnterReadyFromMaintenanceRecoverySuccess).
		OnEntryFrom(evMaintenanceRecoveredFailureReady, lsm.onEnterReadyFromMaintenanceRecoveryFailure)

	// Failed entry actions: emit Failed from a Provision error or Replace
	// failure. Permit(ProvisionRequested) for re-provision retries.
	sm.Configure(backend.ProvisionStatusFailed).
		OnEntryFrom(evProvisionErrored, lsm.onEnterFailedFromProvision).
		OnEntryFrom(evCohortDiverged, lsm.onEnterFailedFromCohortDivergence).
		OnEntryFrom(evReplaceFailed, lsm.onEnterFailedFromReplace).
		OnEntryFrom(evMaintenanceRecoveredFailureFailed, lsm.onEnterFailedFromMaintenanceRecoveryFailure).
		OnEntryFrom(evMaintenanceRecoveredSuccessRuntimeFailed, lsm.onEnterFailedFromMaintenanceRuntimeFailure).
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

func (lsm *leaseSM) fireRaw(ctx context.Context, ev leaseEvent, args ...any) error {
	return lsm.sm.FireCtx(ctx, ev, args...)
}

func (lsm *leaseSM) fireAndExpect(ctx context.Context, ev leaseEvent, want backend.ProvisionStatus, args ...any) error {
	if err := lsm.fireRaw(ctx, ev, args...); err != nil {
		return err
	}
	if state := lsm.State(); state != want {
		return fmt.Errorf("%v not accepted by SM from state %v", ev, state)
	}
	return nil
}

// requireProjection is the fallible half of every transition whose entry
// action reduces the live provision projection. qmuntal/stateless commits the
// destination state before invoking OnEntry and does not roll it back when an
// entry action returns an error. Refusing absence here, while the lease actor
// exclusively owns this projection, makes UpdateFn failure in the subsequent
// entry reduction structurally unreachable.
//
// Recovery may delete or replace a projection only while holding the actor's
// quiescence capability, and ordinary deletion runs through this same actor.
// Consequently the existence proof cannot be invalidated between this check
// and Fire on the serial actor goroutine.
func (lsm *leaseSM) requireProjection(ev leaseEvent) error {
	if lsm.actor.cfg.ProvisionStore.Exists(lsm.actor.leaseUUID) {
		return nil
	}
	return fmt.Errorf("%v refused: lease projection no longer exists", ev)
}

func (lsm *leaseSM) containerDied(
	ctx context.Context,
	id string,
	runtime shared.RuntimeGenerationProof,
) error {
	if id == "" || !runtime.Valid() {
		return errors.New("container death requires an exact instance and runtime generation")
	}
	if err := lsm.requireProjection(evContainerDied); err != nil {
		return err
	}
	return lsm.fireRaw(ctx, evContainerDied, id, runtime)
}
func (lsm *leaseSM) diagnosticsGathered(ctx context.Context, result diagResult) error {
	if err := lsm.requireProjection(evDiagGathered); err != nil {
		return err
	}
	return lsm.fireRaw(ctx, evDiagGathered, result)
}

func (lsm *leaseSM) cohortDiverged(
	ctx context.Context,
	runtime shared.RuntimeGenerationProof,
) error {
	if !runtime.Valid() {
		return errors.New("cohort divergence requires an exact runtime generation")
	}
	if err := lsm.requireProjection(evCohortDiverged); err != nil {
		return err
	}
	return lsm.fireRaw(ctx, evCohortDiverged, runtime)
}
func (lsm *leaseSM) requestDeprovision(ctx context.Context) error {
	return lsm.fireRaw(ctx, evDeprovisionRequested)
}
func (lsm *leaseSM) requestProvision(ctx context.Context) error {
	return lsm.fireAndExpect(ctx, evProvisionRequested, backend.ProvisionStatusProvisioning)
}
func (lsm *leaseSM) requestRestart(ctx context.Context, entry replaceEntryArgs) error {
	if err := lsm.requireProjection(evRestartRequested); err != nil {
		return err
	}
	return lsm.fireAndExpect(ctx, evRestartRequested, backend.ProvisionStatusRestarting, entry)
}
func (lsm *leaseSM) requestUpdate(ctx context.Context, entry replaceEntryArgs) error {
	if err := lsm.requireProjection(evUpdateRequested); err != nil {
		return err
	}
	return lsm.fireAndExpect(ctx, evUpdateRequested, backend.ProvisionStatusUpdating, entry)
}
func (lsm *leaseSM) requestRestore(ctx context.Context, entry replaceEntryArgs) error {
	if err := lsm.requireProjection(evRestoreRequested); err != nil {
		return err
	}
	return lsm.fireAndExpect(ctx, evRestoreRequested, backend.ProvisionStatusRestarting, entry)
}
func (lsm *leaseSM) provisionCompleted(ctx context.Context, result ProvisionSuccessResult) error {
	release, ok := result.operationRelease.Release()
	if !ok || release.Version <= 0 || result.operationRelease.LeaseUUID() != lsm.actor.leaseUUID {
		return errors.New("provision completion requires this lease's exact committed release")
	}
	if err := lsm.requireProjection(evProvisionCompleted); err != nil {
		return err
	}
	return lsm.fireRaw(ctx, evProvisionCompleted, result)
}
func (lsm *leaseSM) provisionErrored(ctx context.Context, info provisionErrorInfo) error {
	if !info.operationFailure.Valid() || info.operationFailure.LeaseUUID() != lsm.actor.leaseUUID {
		return errors.New("provision failure requires this lease's exact operation")
	}
	if err := lsm.requireProjection(evProvisionErrored); err != nil {
		return err
	}
	return lsm.fireRaw(ctx, evProvisionErrored, info)
}
func (lsm *leaseSM) replaceCompleted(ctx context.Context, result ReplaceSuccessResult) error {
	if result.release == nil || result.release.Version <= 0 || result.stackManifest == nil {
		return errors.New("replace completion requires an exact active release projection")
	}
	if _, ok := result.release.RuntimeIdentity(); !ok || result.leaseUUID() != lsm.actor.leaseUUID {
		return errors.New("replace completion release belongs to another lease")
	}
	if err := lsm.requireProjection(evReplaceCompleted); err != nil {
		return err
	}
	return lsm.fireRaw(ctx, evReplaceCompleted, result)
}
func (lsm *leaseSM) replaceRecovered(ctx context.Context, info ReplaceFailureInfo) error {
	if err := lsm.requireProjection(evReplaceRecovered); err != nil {
		return err
	}
	return lsm.fireRaw(ctx, evReplaceRecovered, info)
}
func (lsm *leaseSM) replaceFailed(ctx context.Context, info ReplaceFailureInfo) error {
	if err := lsm.requireProjection(evReplaceFailed); err != nil {
		return err
	}
	return lsm.fireRaw(ctx, evReplaceFailed, info)
}
func (lsm *leaseSM) maintenanceRecoveredSuccess(ctx context.Context, result ReplaceSuccessResult) error {
	if result.release == nil || result.release.Version <= 0 || result.stackManifest == nil {
		return errors.New("maintenance recovery requires an exact active release projection")
	}
	if _, ok := result.release.RuntimeIdentity(); !ok || result.leaseUUID() != lsm.actor.leaseUUID {
		return errors.New("maintenance recovery release belongs to another lease")
	}
	if err := lsm.requireProjection(evMaintenanceRecoveredSuccess); err != nil {
		return err
	}
	return lsm.fireAndExpect(ctx, evMaintenanceRecoveredSuccess, backend.ProvisionStatusReady, result)
}
func (lsm *leaseSM) maintenanceRecoveredFailureReady(ctx context.Context, args maintenanceRecoveryFailureArgs) error {
	if err := lsm.requireProjection(evMaintenanceRecoveredFailureReady); err != nil {
		return err
	}
	return lsm.fireAndExpect(ctx, evMaintenanceRecoveredFailureReady, backend.ProvisionStatusReady, args)
}
func (lsm *leaseSM) maintenanceRecoveredFailureFailed(ctx context.Context, args maintenanceRecoveryFailureArgs) error {
	if err := lsm.requireProjection(evMaintenanceRecoveredFailureFailed); err != nil {
		return err
	}
	return lsm.fireAndExpect(ctx, evMaintenanceRecoveredFailureFailed, backend.ProvisionStatusFailed, args)
}
func (lsm *leaseSM) maintenanceRecoveredRuntimeFailed(ctx context.Context, args maintenanceRecoveryFailureArgs) error {
	if args.projection.release == nil || args.projection.release.Version <= 0 || args.projection.stackManifest == nil {
		return errors.New("maintenance runtime failure requires an exact active release projection")
	}
	if _, ok := args.projection.release.RuntimeIdentity(); !ok || args.projection.leaseUUID() != lsm.actor.leaseUUID {
		return errors.New("maintenance runtime failure release belongs to another lease")
	}
	if err := lsm.requireProjection(evMaintenanceRecoveredSuccessRuntimeFailed); err != nil {
		return err
	}
	return lsm.fireAndExpect(ctx, evMaintenanceRecoveredSuccessRuntimeFailed, backend.ProvisionStatusFailed, args)
}

func (lsm *leaseSM) State() backend.ProvisionStatus {
	s, err := lsm.sm.State(context.Background())
	if err != nil {
		return backend.ProvisionStatusUnknown
	}
	if s == leaseReserved {
		return backend.ProvisionStatusProvisioning
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
	if status, ok := cfg.ProvisionStore.LookupStatus(lsm.actor.leaseUUID); !ok || status != backend.ProvisionStatusReady {
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
	// SetTriggerParameters plus containerDied's typed preflight establish this
	// exact payload before stateless can mutate its state. Entry actions must not
	// discover fallible validation after that mutation.
	containerID := args[0].(string)
	runtime := args[1].(shared.RuntimeGenerationProof)
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
	// removed). The provision-store implementation derives observability from
	// the Ready→Failing mutation in the same critical section.
	lsm.actor.cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
		p.Status = backend.ProvisionStatusFailing
		p.FailCount++
		p.LastError = errMsgContainerExited
		p.Reason = backend.ReasonContainerExited
		p.Message = errMsgContainerExited
	})
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
	lsm.actor.beginWorkerActivity()
	lsm.actor.cfg.WG.Go(func() {
		defer lsm.actor.endWorkerActivity()
		lsm.actor.gatherDiagAsync(diagCtx, containerID, info, runtime)
	})
	return nil
}

// onEnterProvisioning publishes the accepted actor transition before its
// acknowledgement and worker start. On a retry, the predecessor's runtime and
// reservation remain available to the Started executor, but its terminal
// status and diagnostics no longer describe the in-flight operation.
func (lsm *leaseSM) onEnterProvisioning(_ context.Context, _ ...any) error {
	lsm.actor.cfg.ProvisionStore.UpdateFn(lsm.actor.leaseUUID, func(p *ProvisionState) {
		p.Status = backend.ProvisionStatusProvisioning
		p.LastError = ""
		p.Reason = ""
		p.Message = ""
	})
	return nil
}

// onEnterRestarting / onEnterUpdating are the Ready|Failed|Failing →
// Restarting|Updating entry actions. They are the SOLE writers of
// prov.Status (Restarting/Updating) and both callback URLs for the
// restart/update paths (ENG-230). Fired synchronously inside
// handleRestartRequested/handleUpdateRequested's fireAndVerify, BEFORE
// the ack, so the "Restart()/Update() returns ⇒ Status is
// Restarting/Updating" contract relied on by api/handlers.go holds.
//
// onEnterRestarting is ALSO the entry action for the RESTORE path:
// evRestoreRequested (Provisioning → Restarting, fired by
// handleRestoreRequested) reuses it (ENG-325). For that source the prior
// Status is Provisioning; the provision store counts the lease as active
// only after the completion transition to Ready.
func (lsm *leaseSM) onEnterRestarting(ctx context.Context, args ...any) error {
	return lsm.applyReplaceEntry(args, backend.ProvisionStatusRestarting)
}

func (lsm *leaseSM) onEnterUpdating(ctx context.Context, args ...any) error {
	return lsm.applyReplaceEntry(args, backend.ProvisionStatusUpdating)
}

// applyReplaceEntry flips Status to the requested replace state. A restore
// operation installs its exact-completion/lifecycle callback pair immediately
// because no prior destination runtime exists. Restart/update keep their
// validated pair pending on the actor until successful substrate + Release
// commit; rollback therefore reads and re-emits the old committed pair. No
// metric/log side effect belongs inside the closure, per the LeaseProvisionStore
// idempotence contract.
func (lsm *leaseSM) applyReplaceEntry(args []any, status backend.ProvisionStatus) error {
	// The private typed request methods validate existence and construct this
	// payload before Fire; SetTriggerParameters checks its concrete type before
	// stateless changes state. The entry reduction therefore contains no
	// fallible decoding or validation.
	entry := args[0].(replaceEntryArgs)
	callbackURL := entry.CallbackURL
	lifecycleCallbackURL := entry.LifecycleCallbackURL
	callbackKind := entry.CallbackKind
	maintenance := entry.Maintenance
	lsm.actor.cfg.ProvisionStore.UpdateFn(lsm.actor.leaseUUID, func(p *ProvisionState) {
		p.Status = status
		if callbackKind == replaceCallbackOperation && callbackURL != "" {
			p.CallbackURL = callbackURL
		}
		if callbackKind == replaceCallbackOperation && lifecycleCallbackURL != "" {
			p.LifecycleCallbackURL = lifecycleCallbackURL
		}
	})
	lsm.actor.replaceCallbackKind = callbackKind
	lsm.actor.pendingReplaceCallbackURL = callbackURL
	lsm.actor.pendingReplaceLifecycleCallbackURL = lifecycleCallbackURL
	lsm.actor.pendingMaintenance = maintenance
	return nil
}

func (lsm *leaseSM) clearPendingReplaceRoute() {
	lsm.actor.pendingReplaceCallbackURL = ""
	lsm.actor.pendingReplaceLifecycleCallbackURL = ""
	lsm.actor.pendingMaintenance = shared.MaintenanceIntentClaim{}
}

// maintenanceRecoveryFailureArgs carries one exact Release projection and its
// terminal failure surface through a dedicated recovery-only transition.
// Ordinary replacement entry actions are intentionally not reused: they own
// non-idempotent gauges, diagnostics and callbacks that WAL retries must never
// replay.
type maintenanceRecoveryFailureArgs struct {
	projection ReplaceSuccessResult
	failure    ReplaceFailureInfo
}

func (lsm *leaseSM) applyMaintenanceRecoveredProjection(
	result ReplaceSuccessResult,
	status backend.ProvisionStatus,
) error {
	applied := lsm.actor.cfg.ProvisionStore.UpdateFn(lsm.actor.leaseUUID, func(p *ProvisionState) {
		applyReplaceReleaseAuthority(p, result)
		p.ContainerIDs = result.containerIDs
		if result.serviceContainers != nil {
			p.ServiceContainers = result.serviceContainers
		}
		p.Status = status
		if result.applyRecoveredRuntimeAuthority {
			p.CallbackURL = result.recoveredCallbackURL
			p.LifecycleCallbackURL = result.recoveredLifecycleCallbackURL
		}
	})
	if !applied {
		return errors.New("maintenance recovery provision no longer exists")
	}
	return nil
}

func applyReplaceReleaseAuthority(state *ProvisionState, result ReplaceSuccessResult) {
	if result.release == nil || result.stackManifest == nil {
		return
	}
	authority, ok := result.release.RuntimeIdentity()
	if !ok {
		return
	}
	state.Tenant = authority.Tenant()
	state.ProviderUUID = authority.ProviderUUID()
	state.ActiveReleaseVersion = result.release.Version
	state.ActiveOperationID = authority.OperationID()
	state.Items = slices.Clone(result.release.Items)
	state.ResourceProfiles = shared.CloneSKUResourceSnapshot(result.release.ResourceProfiles)
	state.StackManifest = result.stackManifest
}

// applyMaintenanceRecoveredSuccess refreshes an actor from the exact committed
// target without replaying entry-action side effects. It is also used when the
// FSM already reached Ready but its ProvisionStore entry is stale because an
// earlier entry action panicked.
func (lsm *leaseSM) applyMaintenanceRecoveredSuccess(result ReplaceSuccessResult) error {
	if err := lsm.applyMaintenanceRecoveredProjection(result, backend.ProvisionStatusReady); err != nil {
		return err
	}
	lsm.actor.cfg.ProvisionStore.UpdateFn(lsm.actor.leaseUUID, func(p *ProvisionState) {
		p.LastError = ""
		p.Reason = ""
		p.Message = ""
	})
	return nil
}

func (lsm *leaseSM) applyMaintenanceRecoveredFailure(
	projection ReplaceSuccessResult,
	info ReplaceFailureInfo,
	status backend.ProvisionStatus,
) error {
	if err := lsm.applyMaintenanceRecoveredProjection(projection, status); err != nil {
		return err
	}
	lsm.actor.cfg.ProvisionStore.UpdateFn(lsm.actor.leaseUUID, func(p *ProvisionState) {
		p.LastError = info.lastError
		p.Reason = info.reason
		p.Message = info.callbackErr
	})
	return nil
}

func (lsm *leaseSM) applyMaintenanceRecoveredRuntimeFailure(
	projection ReplaceSuccessResult,
	info ReplaceFailureInfo,
) error {
	return lsm.applyMaintenanceRecoveredFailure(
		projection, info, backend.ProvisionStatusFailed,
	)
}

func (lsm *leaseSM) onEnterReadyFromMaintenanceRecoverySuccess(
	_ context.Context,
	args ...any,
) error {
	projection := args[0].(ReplaceSuccessResult)
	// maintenanceRecoveredSuccess proves the projection exists and validates the
	// exact release before Fire. Actor serialization makes failure unreachable.
	_ = lsm.applyMaintenanceRecoveredSuccess(projection)
	return nil
}

func (lsm *leaseSM) onEnterReadyFromMaintenanceRecoveryFailure(
	_ context.Context,
	args ...any,
) error {
	return lsm.applyMaintenanceRecoveryFailureArgs(
		args, backend.ProvisionStatusReady,
	)
}

func (lsm *leaseSM) onEnterFailedFromMaintenanceRecoveryFailure(
	_ context.Context,
	args ...any,
) error {
	return lsm.applyMaintenanceRecoveryFailureArgs(
		args, backend.ProvisionStatusFailed,
	)
}

func (lsm *leaseSM) onEnterFailedFromMaintenanceRuntimeFailure(
	_ context.Context,
	args ...any,
) error {
	recovery := args[0].(maintenanceRecoveryFailureArgs)
	_ = lsm.applyMaintenanceRecoveredRuntimeFailure(
		recovery.projection, recovery.failure,
	)
	return nil
}

func (lsm *leaseSM) applyMaintenanceRecoveryFailureArgs(
	args []any,
	status backend.ProvisionStatus,
) error {
	recovery := args[0].(maintenanceRecoveryFailureArgs)
	_ = lsm.applyMaintenanceRecoveredFailure(
		recovery.projection, recovery.failure, status,
	)
	return nil
}

func (lsm *leaseSM) sendReplaceSuccessCallback(
	committed shared.OperationReleaseCommitted,
	active shared.MaintenanceReleaseActive,
) {
	if lsm.actor.replaceCallbackKind == replaceCallbackLifecycle {
		lsm.actor.cfg.SendMaintenanceSuccessFn(active)
		return
	}
	lsm.actor.cfg.SendOperationSuccessFn(committed)
}

func (lsm *leaseSM) sendReplaceFailureCallback(
	errMsg string,
	failed shared.MaintenanceReleaseFailure,
	operationFailure shared.OperationReleaseUncommitted,
) {
	if lsm.actor.replaceCallbackKind == replaceCallbackLifecycle {
		lsm.actor.cfg.SendMaintenanceFailureFn(failed, errMsg)
		return
	}
	lsm.actor.cfg.SendOperationFailureFn(operationFailure, errMsg)
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
	return lsm.actor.waitForWorkers()
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
	return lsm.actor.waitForWorkers()
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
	result := args[0].(ProvisionSuccessResult)
	cfg := &lsm.actor.cfg
	leaseUUID := lsm.actor.leaseUUID

	// provisionCompleted validates and binds this release before Fire.
	release, _ := result.operationRelease.Release()
	cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
		p.Status = backend.ProvisionStatusReady
		p.ContainerIDs = result.containerIDs
		p.LastError = ""
		// Clear any stale curated failure surface (ENG-508): a retried
		// provision that lands Ready must not carry a prior failure's
		// Reason/Message into the healthy record. Defense-in-depth so
		// leasesm owns the reset, not the substrate.
		p.Reason = ""
		p.Message = ""
		if result.stackManifest != nil {
			p.StackManifest = result.stackManifest
		}
		if result.serviceContainers != nil {
			p.ServiceContainers = result.serviceContainers
		}
		p.ActiveReleaseVersion = release.Version
		p.ActiveOperationID = result.operationRelease.OperationID()
	})

	// ORDERING CONTRACT: SendOperationSuccessFn MUST remain the last statement of
	// this entry action. The docker provision tests synchronize on the
	// callback round trip as their "entry action finished" barrier
	// (docker.doProvisionAndFire / observeCallbacks), so a store write
	// added below this line can land after the barrier has already
	// released the asserting goroutine.
	//
	// This comment is the entire enforcement. Nothing automated catches
	// the benign shape: on the docker substrate every store write goes
	// through backendProvisionStore, which holds provisionsMu, and those
	// tests read under the same mutex — so a late write is properly
	// synchronized and the race detector has nothing to report no matter
	// how late it is. A late write of a DIFFERENT value than the test
	// expects fails an assertion; a late write of the SAME value is
	// invisible to the suite, with or without -race. Do not add a write
	// here on the assumption that -race would have caught it.
	cfg.SendOperationSuccessFn(result.operationRelease)
	return nil
}

// onEnterReadyFromReplaceCompleted fires when doReplace* signals success.
// Owns the Status flip, exact-release projection, and success callback
// emission. ProvisionStore derives Ready-count observability from the status
// mutation atomically; no substrate callback may mutate actor-owned authority.
func (lsm *leaseSM) onEnterReadyFromReplaceCompleted(ctx context.Context, args ...any) error {
	result := args[0].(ReplaceSuccessResult)
	cfg := &lsm.actor.cfg
	leaseUUID := lsm.actor.leaseUUID

	cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
		p.ContainerIDs = result.containerIDs
		if result.serviceContainers != nil {
			p.ServiceContainers = result.serviceContainers
		}
		p.Status = backend.ProvisionStatusReady
		p.LastError = ""
		// Clear any stale curated failure surface (ENG-508): a lease that
		// failed (Reason/Message authored) and then successfully restarts
		// or updates must not keep surfacing the prior failure reason once
		// it is healthy again. ProvisionState persists across transitions.
		p.Reason = ""
		p.Message = ""
		if result.suppressMaintenanceSettlement {
			p.CallbackURL = result.recoveredCallbackURL
			p.LifecycleCallbackURL = result.recoveredLifecycleCallbackURL
		} else if lsm.actor.replaceCallbackKind == replaceCallbackLifecycle {
			p.CallbackURL = lsm.actor.pendingReplaceCallbackURL
			p.LifecycleCallbackURL = lsm.actor.pendingReplaceLifecycleCallbackURL
		}
		applyReplaceReleaseAuthority(p, result)
	})
	if !result.suppressMaintenanceSettlement {
		lsm.sendReplaceSuccessCallback(
			result.operationRelease, result.maintenanceRelease,
		)
	}
	lsm.clearPendingReplaceRoute()
	return nil
}

// onEnterReadyFromReplaceRecovered fires when doReplace* failed but the
// rollback restored the lease to Ready (or the preflight check failed
// without touching containers). Status ends up Ready; LastError is set
// to the rich failure diagnostic. For the restart-with-oldStopped case,
// LastError is cleared because we're back to the exact same state as
// before the restart.
func (lsm *leaseSM) onEnterReadyFromReplaceRecovered(ctx context.Context, args ...any) error {
	info := args[0].(ReplaceFailureInfo)
	cfg := &lsm.actor.cfg
	leaseUUID := lsm.actor.leaseUUID

	cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
		if source := info.recoveredSource; source != nil {
			applyReplaceReleaseAuthority(p, *source)
			p.ContainerIDs = slices.Clone(source.containerIDs)
			p.ServiceContainers = cloneServiceContainers(source.serviceContainers)
			p.CallbackURL = source.recoveredCallbackURL
			p.LifecycleCallbackURL = source.recoveredLifecycleCallbackURL
		}
		p.LastError = info.lastError
		p.Reason = info.reason
		p.Message = info.callbackErr
		p.FailCount++
		p.Status = backend.ProvisionStatusReady
		// Restart: if we actually stopped old containers and then restored
		// them, we're back to the exact same state — no persistent error.
		// Update: keep LastError so the UI shows why the update failed.
		if info.oldStopped && info.operation == "restart" {
			p.LastError = ""
			p.Reason = ""
			p.Message = ""
		}
	})

	if !info.preserveMaintenance {
		lsm.sendReplaceFailureCallback(
			info.callbackErr, info.maintenanceRelease, info.operationRelease,
		)
		lsm.clearPendingReplaceRoute()
	}
	return nil
}

// onEnterFailedFromReplace fires when doReplace* failed AND rollback
// failed (or no rollback was possible). Status ends up Failed.
func (lsm *leaseSM) onEnterFailedFromReplace(ctx context.Context, args ...any) error {
	info := args[0].(ReplaceFailureInfo)
	cfg := &lsm.actor.cfg
	leaseUUID := lsm.actor.leaseUUID

	cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
		p.LastError = info.lastError
		p.Reason = info.reason
		p.Message = info.callbackErr
		p.FailCount++
		p.Status = backend.ProvisionStatusFailed
	})

	if !info.preserveMaintenance {
		lsm.sendReplaceFailureCallback(
			info.callbackErr, info.maintenanceRelease, info.operationRelease,
		)
		lsm.clearPendingReplaceRoute()
	}
	return nil
}

// onEnterFailedFromProvision fires when doProvision signals a failure.
// Owns Status, FailCount and LastError projection updates and the Failed
// callback. The substrate boundary has already captured durable diagnostics;
// callback publication requires that capture before settling the journal.
func (lsm *leaseSM) onEnterFailedFromProvision(ctx context.Context, args ...any) error {
	info := args[0].(provisionErrorInfo)
	cfg := &lsm.actor.cfg
	leaseUUID := lsm.actor.leaseUUID

	cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
		p.Status = backend.ProvisionStatusFailed
		p.FailCount++
		p.LastError = info.lastError
		p.Reason = info.reason
		p.Message = info.callbackErr
	})

	// ORDERING CONTRACT: SendOperationFailureFn MUST remain the last statement of
	// this entry action. The docker provision tests synchronize on the
	// callback round trip as their "entry action finished" barrier
	// (docker.doProvisionAndFire / observeCallbacks), so a store write
	// added below this line can land after the barrier has already
	// released the asserting goroutine.
	//
	// This comment is the entire enforcement. Nothing automated catches
	// the benign shape: on the docker substrate every store write goes
	// through backendProvisionStore, which holds provisionsMu, and those
	// tests read under the same mutex — so a late write is properly
	// synchronized and the race detector has nothing to report no matter
	// how late it is. A late write of a DIFFERENT value than the test
	// expects fails an assertion; a late write of the SAME value is
	// invisible to the suite, with or without -race. Do not add a write
	// here on the assumption that -race would have caught it.
	cfg.SendOperationFailureFn(info.operationFailure, info.callbackErr)
	return nil
}

// onEnterFailedFromDiag runs as the Failing→Failed entry action when
// DiagGathered fires. Owns all state mutations for this transition: flips
// Status, applies diag to LastError, persists diagnostics, emits the
// terminal Failed callback. Running in the actor's goroutine means no
// mutex races with the gathering goroutine (which is pure I/O now).
func (lsm *leaseSM) onEnterFailedFromDiag(ctx context.Context, args ...any) error {
	result := args[0].(diagResult)
	cfg := &lsm.actor.cfg
	leaseUUID := lsm.actor.leaseUUID

	var failCount int
	var diagSnap shared.DiagnosticEntry
	var diagContainerIDs []string
	var diagKeys map[string]string
	cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
		p.Status = backend.ProvisionStatusFailed
		// Reason/Message authored unconditionally, matching the fixed
		// errMsgContainerExited callback sent below. LastError keeps its
		// existing conditional shape (only overwritten when diag is set).
		p.Reason = backend.ReasonContainerExited
		p.Message = errMsgContainerExited
		if result.diag != "" {
			p.LastError = errMsgContainerExited + ": " + result.diag
		}
		failCount = p.FailCount
		diagSnap = DiagnosticSnapshot(p)
		diagContainerIDs = append([]string(nil), p.ContainerIDs...)
		diagKeys = ContainerLogKeys(p)
	})
	// Persist diagnostics (bbolt write). Runs in the actor goroutine —
	// briefly blocks other messages for this lease, but matches the
	// "actor owns all state" invariant. Bbolt writes are ~ms.
	if diagSnap.LeaseUUID != "" {
		cfg.PersistDiagnosticsFn(diagSnap, diagContainerIDs, diagKeys)
	}

	cfg.SendLifecycleFailureFn(result.runtime, errMsgContainerExited)

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

// onEnterFailedFromCohortDivergence is the fail-closed recovery path for a
// Ready lease whose observed instances do not match its durable desired
// release. It intentionally does not route through the container-death flow:
// no particular instance is necessarily dead, and fabricating one would make
// the inspector guard and diagnostics misleading. The callback text is fixed
// here, rather than carried by the cohort-divergence observation, so substrate observations
// can never become tenant-visible text by accident.
func (lsm *leaseSM) onEnterFailedFromCohortDivergence(ctx context.Context, args ...any) error {
	cfg := &lsm.actor.cfg
	leaseUUID := lsm.actor.leaseUUID
	runtime := args[0].(shared.RuntimeGenerationProof)

	var failCount int
	var diagSnap shared.DiagnosticEntry
	var diagInstanceIDs []string
	var diagKeys map[string]string
	applied := cfg.ProvisionStore.UpdateFn(leaseUUID, func(p *ProvisionState) {
		p.Status = backend.ProvisionStatusFailed
		p.FailCount++
		p.LastError = errMsgCohortDiverged
		p.Reason = backend.ReasonInternal
		p.Message = errMsgCohortDiverged
		failCount = p.FailCount
		diagSnap = DiagnosticSnapshot(p)
		diagInstanceIDs = append([]string(nil), p.ContainerIDs...)
		diagKeys = ContainerLogKeys(p)
	})
	if !applied {
		return nil
	}

	if diagSnap.LeaseUUID != "" {
		cfg.PersistDiagnosticsFn(diagSnap, diagInstanceIDs, diagKeys)
	}
	cfg.SendLifecycleFailureFn(runtime, errMsgCohortDiverged)
	cfg.Logger.Warn("recovered workload cohort diverges from durable release",
		"lease_uuid", leaseUUID,
		"fail_count", failCount,
	)
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
	runtime     shared.RuntimeGenerationProof
}

// ProvisionSuccessResult carries doProvision output into
// Ready.OnEntryFrom(evProvisionCompleted) via Fire args. Every provision
// is stack-shaped post-Task-15; StackManifest + ServiceContainers are
// always populated. Its fields are opaque; substrate workers can create only a
// validated value through NewProvisionSuccessResult.
type ProvisionSuccessResult struct {
	containerIDs      []string
	stackManifest     *manifest.StackManifest
	serviceContainers map[string][]string
	operationRelease  shared.OperationReleaseCommitted
}

// ProvisionSuccessProjection contains substrate observations only. The exact
// operation identity is deliberately supplied separately as a committed proof
// so a caller cannot manufacture a successful projection with zero or mixed
// settlement authority.
type ProvisionSuccessProjection struct {
	ContainerIDs      []string
	ServiceContainers map[string][]string
}

func NewProvisionSuccessResult(
	projection ProvisionSuccessProjection,
	proof shared.OperationReleaseCommitted,
) (ProvisionSuccessResult, error) {
	if !proof.Valid() {
		return ProvisionSuccessResult{}, errors.New("provision success requires an exact committed operation release")
	}
	release, ok := proof.Release()
	if !ok {
		return ProvisionSuccessResult{}, errors.New("provision success has no committed release")
	}
	stack, err := validateCompleteReleaseProjection(release, projection.ContainerIDs, projection.ServiceContainers)
	if err != nil {
		return ProvisionSuccessResult{}, fmt.Errorf("provision success projection: %w", err)
	}
	return ProvisionSuccessResult{
		containerIDs:      slices.Clone(projection.ContainerIDs),
		stackManifest:     stack,
		serviceContainers: cloneServiceContainers(projection.ServiceContainers),
		operationRelease:  proof,
	}, nil
}

// validateCompleteReleaseProjection binds a substrate cohort to the exact
// desired topology sealed in a durable Release. Structural ID coverage alone
// is insufficient: each desired service must be present exactly once and carry
// exactly its requested quantity.
func validateCompleteReleaseProjection(
	release shared.Release,
	containerIDs []string,
	serviceContainers map[string][]string,
) (*manifest.StackManifest, error) {
	if len(release.Items) == 0 {
		return nil, errors.New("release has no exact item topology")
	}
	if err := validateWorkerProjection(containerIDs, serviceContainers); err != nil {
		return nil, err
	}
	expected, err := backend.ValidateOperationQuantities(release.Items)
	if err != nil {
		return nil, fmt.Errorf("release item topology: %w", err)
	}
	if len(containerIDs) != expected || len(serviceContainers) != len(release.Items) {
		return nil, errors.New("container projection cardinality differs from release")
	}
	services := make(map[string]struct{}, len(release.Items))
	for _, item := range release.Items {
		if item.ServiceName == "" {
			return nil, errors.New("release item has no service name")
		}
		if _, duplicate := services[item.ServiceName]; duplicate {
			return nil, fmt.Errorf("release service %q is duplicated", item.ServiceName)
		}
		services[item.ServiceName] = struct{}{}
		if len(serviceContainers[item.ServiceName]) != item.Quantity {
			return nil, fmt.Errorf("service %q container quantity differs from release", item.ServiceName)
		}
	}
	stack, err := manifest.ParsePayload(release.Manifest)
	if err != nil {
		return nil, fmt.Errorf("release manifest: %w", err)
	}
	if len(stack.Services) != len(services) {
		return nil, errors.New("release manifest services differ from item topology")
	}
	for service := range services {
		if stack.Services[service] == nil {
			return nil, fmt.Errorf("release manifest is missing service %q", service)
		}
	}
	return stack, nil
}

func validateWorkerProjection(containerIDs []string, serviceContainers map[string][]string) error {
	if len(containerIDs) == 0 || len(serviceContainers) == 0 {
		return errors.New("container and service projections are required")
	}
	want := make(map[string]struct{}, len(containerIDs))
	for _, id := range containerIDs {
		if id == "" {
			return errors.New("container id is empty")
		}
		if _, duplicate := want[id]; duplicate {
			return fmt.Errorf("container id %q is duplicated", id)
		}
		want[id] = struct{}{}
	}
	got := make(map[string]struct{}, len(containerIDs))
	for service, ids := range serviceContainers {
		if service == "" || len(ids) == 0 {
			return errors.New("service projection contains an empty service")
		}
		for _, id := range ids {
			if _, known := want[id]; !known {
				return fmt.Errorf("service %q references unknown container %q", service, id)
			}
			if _, duplicate := got[id]; duplicate {
				return fmt.Errorf("container %q appears in more than one service slot", id)
			}
			got[id] = struct{}{}
		}
	}
	if len(got) != len(want) {
		return errors.New("service projection does not cover every container")
	}
	return nil
}

func cloneServiceContainers(in map[string][]string) map[string][]string {
	if in == nil {
		return nil
	}
	out := make(map[string][]string, len(in))
	for service, containers := range in {
		out[service] = slices.Clone(containers)
	}
	return out
}

// provisionErrorInfo carries exact terminal authority and its in-memory
// projection. Durable attempt diagnostics belong to the substrate publisher.
type provisionErrorInfo struct {
	callbackErr      string
	reason           backend.Reason // ENG-508
	lastError        string
	operationFailure shared.OperationReleaseUncommitted
}

// replaceEntryArgs carries the new exact-completion and lifecycle callback
// URLs from a Restart/Update/Restore request into the replace entry actions,
// so the actor (not the HTTP prelude) is the sole writer of the persisted pair.
type replaceCallbackKind uint8

const (
	replaceCallbackOperation replaceCallbackKind = iota
	replaceCallbackLifecycle
)

type replaceEntryArgs struct {
	CallbackURL          string
	LifecycleCallbackURL string
	CallbackKind         replaceCallbackKind
	Maintenance          shared.MaintenanceIntentClaim
}

// ReplaceSuccessResult carries doReplaceContainers / doReplaceStackContainers
// success output into onEnterReadyFromReplace. The goroutine returns an opaque
// constructor-validated value; the entry action derives actor-owned authority
// from its exact release proof and writes the projection under one lock.
type ReplaceSuccessResult struct {
	containerIDs      []string
	serviceContainers map[string][]string // non-nil for stack
	release           *shared.Release
	stackManifest     *manifest.StackManifest
	// OperationRelease is present only for Restore. Provision has its own
	// success result; maintenance/lifecycle replacements do not settle an
	// operation intent and deliberately carry the invalid zero proof.
	operationRelease shared.OperationReleaseCommitted
	// MaintenanceRelease is present only for a successful restart/update.
	// The actor can settle success only by forwarding this exact active-release
	// proof; a caller-selected success enum is not representable.
	maintenanceRelease shared.MaintenanceReleaseActive
	authorityKind      replaceAuthorityKind
	maintenance        shared.MaintenanceIntentClaim
	// suppressMaintenanceSettlement is set only by the typed recovery
	// constructor after an exact terminal Release is observed. Recovery owns
	// the later intent -> outbox transaction and must not enqueue a duplicate.
	suppressMaintenanceSettlement  bool
	applyRecoveredRuntimeAuthority bool
	recoveredCallbackURL           string
	recoveredLifecycleCallbackURL  string
}

func (r ReplaceSuccessResult) leaseUUID() string {
	if r.operationRelease.Valid() {
		return r.operationRelease.LeaseUUID()
	}
	if r.maintenance.Valid() {
		return r.maintenance.LeaseUUID()
	}
	return ""
}

type replaceAuthorityKind uint8

const (
	replaceAuthorityRecovery replaceAuthorityKind = iota + 1
	replaceAuthorityMaintenance
	replaceAuthorityRestoreCommitted
)

// ReplaceSuccessProjection contains only substrate observations. Terminal and
// runtime authority are supplied to distinct typed constructors and cannot be
// written into this value.
type ReplaceSuccessProjection struct {
	ContainerIDs      []string
	ServiceContainers map[string][]string
}

func newReplaceSuccessProjection(projection ReplaceSuccessProjection) ReplaceSuccessResult {
	return ReplaceSuccessResult{
		containerIDs:      slices.Clone(projection.ContainerIDs),
		serviceContainers: cloneServiceContainers(projection.ServiceContainers),
	}
}

// ReplaceFailureInfo carries doReplace* failure data. Used by both
// onEnterReadyFromReplaceRecovered (Status ends up Ready) and
// onEnterFailedFromReplace (Status ends up Failed). The entry actions
// set LastError, increment FailCount, and atomically update readiness. Its
// fields are opaque; substrate workers obtain validated variants from typed
// constructors.
type ReplaceFailureInfo struct {
	operation   string // "restart" or "update"
	oldStopped  bool   // only meaningful on the recovery path (restart-with-oldStopped clears LastError)
	callbackErr string
	reason      backend.Reason // ENG-508: category code; message is CallbackErr
	lastError   string
	// PreserveMaintenance suppresses callback settlement when the exact
	// release terminal write was ambiguous. Periodic recovery owns resolution.
	preserveMaintenance bool
	// MaintenanceRelease is present only after the exact restart/update target
	// committed failed. The actor can settle failure only by forwarding this
	// proof; ambiguous terminal writes deliberately leave it invalid and keep
	// PreserveMaintenance true for recovery.
	maintenanceRelease shared.MaintenanceReleaseFailure
	// recoveredSource exists only when the failed release carries exact
	// SourceReady evidence. It supplies both runtime identity and cohort IDs.
	recoveredSource *ReplaceSuccessResult
	// OperationRelease is present only for a failed Restore and binds the
	// callback to the exact operation generation accepted before substrate work.
	operationRelease shared.OperationReleaseUncommitted
	authorityKind    replaceAuthorityKind
	maintenance      shared.MaintenanceIntentClaim
}

type ReplaceFailureDetails struct {
	OldStopped  bool
	CallbackErr string
	Reason      backend.Reason
	LastError   string
}

func NewMaintenanceRecoveryFailureInfo(
	intent shared.MaintenanceIntentClaim,
	details ReplaceFailureDetails,
) (ReplaceFailureInfo, error) {
	if !intent.Valid() {
		return ReplaceFailureInfo{}, errors.New("replace failure requires an exact maintenance intent")
	}
	kind := intent.Kind()
	if kind != shared.MaintenanceIntentRestart && kind != shared.MaintenanceIntentUpdate {
		return ReplaceFailureInfo{}, errors.New("replace failure requires restart or update kind")
	}
	return ReplaceFailureInfo{
		operation: string(kind), oldStopped: details.OldStopped,
		callbackErr: details.CallbackErr, reason: details.Reason,
		lastError:     details.LastError,
		authorityKind: replaceAuthorityRecovery, maintenance: intent,
	}, nil
}

func (i ReplaceFailureInfo) WithOldStopped(oldStopped bool) ReplaceFailureInfo {
	i.oldStopped = oldStopped
	return i
}

func (i ReplaceFailureInfo) CallbackError() string  { return i.callbackErr }
func (i ReplaceFailureInfo) LastError() string      { return i.lastError }
func (i ReplaceFailureInfo) Reason() backend.Reason { return i.reason }

func NewMaintenanceReplaceFailure(err error, details ReplaceFailureDetails, proof shared.MaintenanceReleaseFailure) (ReplaceResult, error) {
	if err == nil || !proof.Valid() {
		return ReplaceResult{}, errors.New("maintenance failure requires an error and exact failed release proof")
	}
	info, infoErr := NewMaintenanceRecoveryFailureInfo(proof.Intent(), details)
	if infoErr != nil {
		return ReplaceResult{}, infoErr
	}
	info.authorityKind = replaceAuthorityMaintenance
	info.maintenanceRelease = proof
	info.maintenance = proof.Intent()
	if ready, ok := proof.SourceReady(); ok {
		release, ids, services := ready.Projection()
		stack, projectionErr := validateCompleteReleaseProjection(release, ids, services)
		if projectionErr != nil {
			return ReplaceResult{}, fmt.Errorf("restored maintenance source: %w", projectionErr)
		}
		authority, ok := release.RuntimeIdentity()
		if !ok {
			return ReplaceResult{}, errors.New("restored source has no runtime identity")
		}
		projection := newReplaceSuccessProjection(ReplaceSuccessProjection{ContainerIDs: ids, ServiceContainers: services})
		projection.release, projection.stackManifest = &release, stack
		projection.recoveredCallbackURL = authority.CallbackURL()
		projection.recoveredLifecycleCallbackURL = authority.LifecycleCallbackURL()
		info.recoveredSource = &projection
	}
	return ReplaceResult{err: err, failure: info}, nil
}

func NewRestoreReplaceFailure(err error, details ReplaceFailureDetails, proof shared.OperationReleaseUncommitted) (ReplaceResult, error) {
	if err == nil || !proof.Valid() {
		return ReplaceResult{}, errors.New("restore failure requires an error and exact operation proof")
	}
	info := ReplaceFailureInfo{
		operation: "restore", oldStopped: details.OldStopped,
		callbackErr: details.CallbackErr, reason: details.Reason,
		lastError: details.LastError,
	}
	info.authorityKind = replaceAuthorityRestoreCommitted
	info.operationRelease = proof
	return ReplaceResult{err: err, failure: info}, nil
}

// ReplaceResult is doReplace*'s return value bundling everything the
// goroutine wrapper needs to fire the right SM event. The callback path
// depends on (Err, recovered), where recovery requires exact SourceReady
// evidence in the failed maintenance release:
//
//	Err == nil             → fire evReplaceCompleted with .Success
//	Err != nil, recovered  → fire evReplaceRecovered with .Failure
//	Err != nil, !recovered → fire evReplaceFailed    with .Failure
//
// Its fields are opaque. Substrate workers can return only constructor-minted
// success and failure variants, while the actor alone dispatches on them.
type ReplaceResult struct {
	err     error
	success ReplaceSuccessResult
	failure ReplaceFailureInfo
}

func (r ReplaceResult) Err() error { return r.err }

// Restored reports whether the failed substrate operation restored its source
// cohort. It is observation only; callers cannot use it to construct another
// terminal outcome.
func (r ReplaceResult) Restored() bool { return r.failure.recoveredSource != nil }

// FailureInfo exposes the sealed failure's read-only diagnostic surface.
func (r ReplaceResult) FailureInfo() ReplaceFailureInfo { return r.failure }

// SuccessContainerIDs returns a copy of the sealed success projection.
func (r ReplaceResult) SuccessContainerIDs() []string {
	return slices.Clone(r.success.containerIDs)
}

// PreservesMaintenance reports whether recovery, rather than this worker,
// owns exact terminal settlement.
func (i ReplaceFailureInfo) PreservesMaintenance() bool { return i.preserveMaintenance }

func (r ReplaceResult) validForMaintenance(intent shared.MaintenanceIntentClaim) bool {
	if !intent.Valid() {
		return false
	}
	if r.err == nil {
		return r.success.authorityKind == replaceAuthorityMaintenance &&
			r.success.maintenanceRelease.MatchesIntent(intent)
	}
	return r.failure.authorityKind == replaceAuthorityMaintenance &&
		r.failure.maintenanceRelease.MatchesIntent(intent)
}

// readProvisionStatus snapshots the lease's current provision status via
// the substrate-agnostic LeaseProvisionStore seam. Used on SM creation to
// pick the initial state. Returns ProvisionStatusProvisioning when the
// lease is unknown (the same default as the prior in-docker reach-through).
func readProvisionStatus(actor *LeaseActor) any {
	status, ok := actor.cfg.ProvisionStore.LookupStatus(actor.leaseUUID)
	if !ok {
		return leaseReserved
	}
	if status == backend.ProvisionStatusProvisioning {
		return leaseReserved
	}
	return status
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
func (a *LeaseActor) gatherDiagAsync(
	ctx context.Context,
	containerID string,
	info *InstanceState,
	runtime shared.RuntimeGenerationProof,
) {
	// Same single-send defer pattern as spawnProvisionWorker /
	// spawnReplaceWorker (see lease_actor.go). One sendTerminal site;
	// the recover overrides terminalMsg on panic, the normal path
	// sets it on completion. ctx-cancel is special: we suppress the
	// send entirely (the Deprovisioning.Ignore path handles that case).
	var terminalMsg workerTerminalMessage
	var event = "diag_gathered"
	var suppress bool
	defer func() {
		if suppress {
			return
		}
		if terminalMsg == nil {
			terminalMsg = diagGatheredMsg{
				result: diagResult{containerID: containerID, info: info, diag: "", runtime: runtime},
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
				result: diagResult{containerID: containerID, info: info, diag: "", runtime: runtime},
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
		result: diagResult{containerID: containerID, info: info, diag: diag, runtime: runtime},
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

// errMsgCohortDiverged is the fixed, tenant-safe lifecycle callback and
// provision Message used when recovery proves that the observed workload no
// longer matches its durable desired release.
const errMsgCohortDiverged = "running workload cohort does not match its durable release"

// ErrMsgCohortDiverged exposes the canonical callback text to substrate tests
// and read-model adapters without allowing callers to author alternate text.
const ErrMsgCohortDiverged = errMsgCohortDiverged

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
// store's mutex) so the snapshot is consistent; PersistDiagnosticsFn writes
// it after the closure returns.
// Use only for diagnostics-store writes; not a general ProvisionState
// projection helper.
//
// Omits SKU, Image, Status, Quantity, the raw callback URLs, Items,
// ContainerIDs, Manifest, StackManifest, and ServiceContainers — those are
// operational state that's either already on the lease's authoritative record
// or not relevant to the on-disk diagnostic blob a future operator reads. The
// callback pair is reduced to its historical, non-secret lifecycle-generation
// observation so a singular diagnostics fallback preserves read-model parity.
// It does not grant causal authority and must not enter fleet inventory or
// settlement.
func DiagnosticSnapshot(prov *ProvisionState) shared.DiagnosticEntry {
	lifecycleGeneration := backend.ObserveLifecycleGeneration(
		prov.CallbackURL, prov.LifecycleCallbackURL,
	)
	return shared.DiagnosticEntry{
		LeaseUUID:             prov.LeaseUUID,
		ProviderUUID:          prov.ProviderUUID,
		Tenant:                prov.Tenant,
		Error:                 prov.LastError,
		Reason:                prov.Reason,
		Message:               prov.Message,
		FailCount:             prov.FailCount,
		RuntimeReleaseVersion: prov.ActiveReleaseVersion,
		LifecycleGeneration:   &lifecycleGeneration,
		CreatedAt:             time.Now(),
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
