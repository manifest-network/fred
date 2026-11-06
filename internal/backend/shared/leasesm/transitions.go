package leasesm

import (
	"context"
	"fmt"

	"github.com/qmuntal/stateless"

	"github.com/manifest-network/fred/internal/backend"
)

// leaseTransition is one legal edge of the lease state machine.
type leaseTransition struct {
	from  stateless.State // a backend.ProvisionStatus, or leaseReserved
	event leaseEvent
	to    backend.ProvisionStatus
	// guard, when set, must also accept the event's arguments.
	guard func(*leaseSM, context.Context, ...any) bool
}

// leaseTransitions is every edge the lease state machine permits, and the only
// place one is declared: newLeaseSM permits exactly these. Entry and exit
// actions, and the events a state ignores, stay with its configuration in
// newLeaseSM. A substrate binds one transition series per element of
// Transitions(), so every transition it can be told about exists from
// startup.
var leaseTransitions = [...]leaseTransition{
	// Reserved is an actor-control state, not a tenant-visible status. Both a
	// fresh provision and restore publish Provisioning while awaiting actor
	// admission, but each command is accepted exactly once from this state.
	{from: leaseReserved, event: evProvisionRequested, to: backend.ProvisionStatusProvisioning},
	{from: leaseReserved, event: evRestoreRequested, to: backend.ProvisionStatusRestarting},
	{from: leaseReserved, event: evDeprovisionRequested, to: backend.ProvisionStatusDeprovisioning},

	// Ready: a container died (guard confirms); a deprovision arrived;
	// or the operator initiated a Restart or Update.
	{
		from: backend.ProvisionStatusReady, event: evContainerDied, to: backend.ProvisionStatusFailing,
		guard: (*leaseSM).guardContainerActuallyDied,
	},
	{from: backend.ProvisionStatusReady, event: evCohortDiverged, to: backend.ProvisionStatusFailed},
	{from: backend.ProvisionStatusReady, event: evDeprovisionRequested, to: backend.ProvisionStatusDeprovisioning},
	{from: backend.ProvisionStatusReady, event: evRestartRequested, to: backend.ProvisionStatusRestarting},
	{from: backend.ProvisionStatusReady, event: evUpdateRequested, to: backend.ProvisionStatusUpdating},
	{from: backend.ProvisionStatusReady, event: evMaintenanceRecoveredFailureFailed, to: backend.ProvisionStatusFailed},
	{from: backend.ProvisionStatusReady, event: evMaintenanceRecoveredSuccessRuntimeFailed, to: backend.ProvisionStatusFailed},

	// Failing: either DiagGathered arrives (→ Failed, emit terminal callback)
	// or a DeprovisionRequested preempts (→ Deprovisioning, no callback).
	{from: backend.ProvisionStatusFailing, event: evDiagGathered, to: backend.ProvisionStatusFailed},
	{from: backend.ProvisionStatusFailing, event: evDeprovisionRequested, to: backend.ProvisionStatusDeprovisioning},
	// Accept retry events from Failing the same way Failed does. If a
	// diag goroutine is wedged and never fires DiagGathered, a
	// subsequent Provision/Restart/Update retry would otherwise hit an
	// unhandled trigger. Allowing the retry from Failing removes the
	// wedge; Failing.OnExit cancels the stale diag goroutine on the
	// way out via diagCancel + waitForWorkers.
	{from: backend.ProvisionStatusFailing, event: evProvisionRequested, to: backend.ProvisionStatusProvisioning},
	{from: backend.ProvisionStatusFailing, event: evRestartRequested, to: backend.ProvisionStatusRestarting},
	{from: backend.ProvisionStatusFailing, event: evUpdateRequested, to: backend.ProvisionStatusUpdating},

	// Failed: terminal callback already emitted on entry. Deprovision can still
	// be requested, maintenance recovery can restore Ready, and a re-provision,
	// restart or update can retry.
	{from: backend.ProvisionStatusFailed, event: evDeprovisionRequested, to: backend.ProvisionStatusDeprovisioning},
	{from: backend.ProvisionStatusFailed, event: evMaintenanceRecoveredSuccess, to: backend.ProvisionStatusReady},
	{from: backend.ProvisionStatusFailed, event: evMaintenanceRecoveredFailureReady, to: backend.ProvisionStatusReady},
	{from: backend.ProvisionStatusFailed, event: evProvisionRequested, to: backend.ProvisionStatusProvisioning},
	{from: backend.ProvisionStatusFailed, event: evRestartRequested, to: backend.ProvisionStatusRestarting},
	{from: backend.ProvisionStatusFailed, event: evUpdateRequested, to: backend.ProvisionStatusUpdating},

	// Unknown is fail-closed for creation/replacement, but close remains safe:
	// teardown is idempotent and is the only transition out of this state.
	{from: backend.ProvisionStatusUnknown, event: evDeprovisionRequested, to: backend.ProvisionStatusDeprovisioning},

	// Provisioning: exits by ProvisionCompleted (→ Ready, emit Success),
	// ProvisionErrored (→ Failed, emit Failed), or DeprovisionRequested
	// (→ Deprovisioning, OnExit cancels the worker).
	{from: backend.ProvisionStatusProvisioning, event: evProvisionCompleted, to: backend.ProvisionStatusReady},
	{from: backend.ProvisionStatusProvisioning, event: evProvisionErrored, to: backend.ProvisionStatusFailed},
	{from: backend.ProvisionStatusProvisioning, event: evDeprovisionRequested, to: backend.ProvisionStatusDeprovisioning},

	// Restarting/Updating: the worker fires evReplaceCompleted (full
	// success), evReplaceRecovered (failure, but the lease ended up Ready via
	// rollback or preflight restore) or evReplaceFailed (ended up Failed).
	// Maintenance recovery reports the same outcomes through its own events.
	{from: backend.ProvisionStatusRestarting, event: evReplaceCompleted, to: backend.ProvisionStatusReady},
	{from: backend.ProvisionStatusRestarting, event: evReplaceRecovered, to: backend.ProvisionStatusReady},
	{from: backend.ProvisionStatusRestarting, event: evReplaceFailed, to: backend.ProvisionStatusFailed},
	{from: backend.ProvisionStatusRestarting, event: evMaintenanceRecoveredSuccess, to: backend.ProvisionStatusReady},
	{from: backend.ProvisionStatusRestarting, event: evMaintenanceRecoveredFailureReady, to: backend.ProvisionStatusReady},
	{from: backend.ProvisionStatusRestarting, event: evMaintenanceRecoveredFailureFailed, to: backend.ProvisionStatusFailed},
	{from: backend.ProvisionStatusRestarting, event: evMaintenanceRecoveredSuccessRuntimeFailed, to: backend.ProvisionStatusFailed},
	{from: backend.ProvisionStatusRestarting, event: evDeprovisionRequested, to: backend.ProvisionStatusDeprovisioning},
	{from: backend.ProvisionStatusUpdating, event: evReplaceCompleted, to: backend.ProvisionStatusReady},
	{from: backend.ProvisionStatusUpdating, event: evReplaceRecovered, to: backend.ProvisionStatusReady},
	{from: backend.ProvisionStatusUpdating, event: evReplaceFailed, to: backend.ProvisionStatusFailed},
	{from: backend.ProvisionStatusUpdating, event: evMaintenanceRecoveredSuccess, to: backend.ProvisionStatusReady},
	{from: backend.ProvisionStatusUpdating, event: evMaintenanceRecoveredFailureReady, to: backend.ProvisionStatusReady},
	{from: backend.ProvisionStatusUpdating, event: evMaintenanceRecoveredFailureFailed, to: backend.ProvisionStatusFailed},
	{from: backend.ProvisionStatusUpdating, event: evMaintenanceRecoveredSuccessRuntimeFailed, to: backend.ProvisionStatusFailed},
	{from: backend.ProvisionStatusUpdating, event: evDeprovisionRequested, to: backend.ProvisionStatusDeprovisioning},
}

// permitLeaseTransitions configures every edge of leaseTransitions on sm.
func permitLeaseTransitions(sm *stateless.StateMachine, lsm *leaseSM) {
	for _, edge := range leaseTransitions {
		state := sm.Configure(edge.from)
		if edge.guard == nil {
			state.Permit(edge.event, edge.to)
			continue
		}
		state.Permit(edge.event, edge.to, func(ctx context.Context, args ...any) bool {
			return edge.guard(lsm, ctx, args...)
		})
	}
}

// Transition is one legal edge of the lease state machine, as a substrate's
// transition metric labels it. Only this package creates one, from
// leaseTransitions.
type Transition struct{ from, to, event string }

// From is the source state's label.
func (t Transition) From() string { return t.from }

// To is the destination state's label.
func (t Transition) To() string { return t.to }

// Event is the label of the event that caused the transition.
func (t Transition) Event() string { return t.event }

type transitionKey struct {
	from  stateless.State
	event leaseEvent
}

var transitions, transitionsByKey = indexLeaseTransitions()

func indexLeaseTransitions() ([]Transition, map[transitionKey]Transition) {
	list := make([]Transition, 0, len(leaseTransitions))
	byKey := make(map[transitionKey]Transition, len(leaseTransitions))
	for _, edge := range leaseTransitions {
		transition := Transition{
			from: fmt.Sprint(edge.from), to: fmt.Sprint(edge.to), event: edge.event.String(),
		}
		list = append(list, transition)
		byKey[transitionKey{from: edge.from, event: edge.event}] = transition
	}
	return list, byKey
}

// Transitions is every legal edge of the lease state machine, so a substrate
// can create each edge's series at startup.
func Transitions() []Transition {
	return append([]Transition(nil), transitions...)
}

// transitionOf is the edge a stateless transition took. Every transition the
// machine makes is one of leaseTransitions, because those are all it permits.
func transitionOf(tr stateless.Transition) (Transition, bool) {
	event, ok := tr.Trigger.(leaseEvent)
	if !ok {
		return Transition{}, false
	}
	transition, ok := transitionsByKey[transitionKey{from: tr.Source, event: event}]
	if !ok || transition.to != fmt.Sprint(tr.Destination) {
		return Transition{}, false
	}
	return transition, true
}
