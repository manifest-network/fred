package leasesm

import (
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
)

// Terminal failure budget (ENG-799).
//
// providerd closes an ACTIVE lease on-chain when its own workload keeps
// failing. The close cannot be undone and stops a paying tenant, so the backend
// authors the decision here, at the failure source, and providerd acts only on
// the resulting typed verdict:
//
//   - Only consecutive failures of the tenant's own workload count
//     (failurecause.Cause.Counts). Restarts, updates, rollbacks, restores,
//     platform failures, signaled or vanished containers, host reboots and
//     unknown causes are recorded but never counted, as Kubernetes
//     podFailurePolicy keeps disruptions out of backoffLimit (KEP-3329).
//   - The count resets once the lease has been Ready for
//     terminalBudgetResetAfter. The reset is anchored on entering Ready and is
//     checked at every exit from Ready, the way the kubelet resets crash-loop
//     backoff after 10 minutes of trouble-free running and Cloud Foundry Diego
//     resets its crash count for an instance Running longer than its reset
//     timeout. The ECS deployment circuit breaker likewise counts only
//     consecutive failures and resets on a healthy task.
//   - A tenant restart or update also resets the count, as `docker restart`
//     resets Docker's RestartCount.
//   - The budget lives in memory on the actor-owned projection. A backend
//     restart resets it, as moby's daemon restore resets RestartCount; losing
//     it can only delay a close, never cause one.
//
// The verdict is exhausted only while the lease is Failed, and only if the
// failure that made it Failed was itself counted.
//
// The Ready boundary is structural rather than wired into each transition by
// hand: SetStatus is the only writer of ProvisionState.Status, and
// InheritTerminalBudget the only way a budget moves onto a replacement
// projection. Both apply crossStatus, so a transition into or out of Ready
// that a future change adds gets the anchor and the reset without remembering
// them. internal/testutil's guard rejects any other write to Status or
// TerminalBudget, and to the budget's fields outside this file.

const (
	// terminalBudgetThreshold is the number of consecutive counted failures
	// that exhausts the budget: the ECS circuit-breaker floor and fred's
	// historical limit. A compile-time constant, not a knob.
	terminalBudgetThreshold = 3
	// terminalBudgetResetAfter is the sustained-Ready period that resets the
	// count: the kubelet's 10-minute crash-loop backoff reset (KEP-4603).
	terminalBudgetResetAfter = 10 * time.Minute
)

// TerminalBudget is one lease's consecutive-failure budget. It is opaque: a
// substrate may carry it onto a rebuilt projection only through
// InheritTerminalBudget, and only the lease actor can advance it, so no
// off-actor path can make a lease closable. The zero value is a fresh budget.
type TerminalBudget struct {
	// leaseUUID binds the budget to its lease. A copy carried onto another
	// lease's projection reads, and is mutated, as a fresh budget.
	leaseUUID string
	// consecutive is the recorded count of consecutive counted failures. Time
	// alone never decays it, so the wire observation changes only at
	// transitions.
	consecutive int
	// readySince is when the lease last entered Ready. crossStatus writes it
	// on every Ready entry and clears it at every Ready exit; nothing else
	// does.
	readySince time.Time
	// lastFailureCounted is true only from a counted Ready -> Failing failure
	// until the next status change other than Failing -> Failed.
	lastFailureCounted bool
}

// budgetOutcome is what one recorded failure did to the budget. The actor
// captures it inside the store closure and acts on it afterwards (metrics and
// logs stay outside UpdateFn, per the closure contract).
type budgetOutcome struct {
	cause       failurecause.Cause
	counted     bool
	consecutive int
	exhausted   bool
}

// reasonEligibleForBudget is a necessary condition for a failure to count and
// for the verdict to read exhausted. It is an allowlist: every other declared
// Reason, and any unrecognized value, is ineligible. It is never sufficient on
// its own, because Reason is tenant-facing text that mixes causes (ENG-508).
// At record time the death entry action has just written ContainerExited, so
// the check there is defense in depth; it decides at observe time.
func reasonEligibleForBudget(reason backend.Reason) bool {
	switch reason {
	case backend.ReasonContainerExited:
		return true
	default:
		return false
	}
}

// tenantInitiatedMaintenance is an allowlist of the accepted maintenance
// commands a tenant starts, which reset the streak like `docker restart`
// resets RestartCount. A custom-domain redeploy is started by the platform's
// reconciler, and a restore admits a new lease whose budget is already fresh;
// neither resets, nor does any kind added later until it is decided here.
func tenantInitiatedMaintenance(kind shared.MaintenanceIntentKind) bool {
	switch kind {
	case shared.MaintenanceIntentRestart, shared.MaintenanceIntentUpdate:
		return true
	default:
		return false
	}
}

// boundBudget returns this projection's budget, replacing a copy bound to
// another lease (or never bound) with a fresh one.
func (p *ProvisionState) boundBudget() *TerminalBudget {
	if p.TerminalBudget.leaseUUID != p.LeaseUUID {
		p.TerminalBudget = TerminalBudget{leaseUUID: p.LeaseUUID}
	}
	return &p.TerminalBudget
}

// SetStatus is the only writer of ProvisionState.Status (ENG-799). The lease
// state machine's entry actions and a substrate's own projection writes both
// go through it, so the terminal budget's Ready boundary holds for every
// present and future transition:
//
//   - entering Ready anchors the sustained-Ready period at now (R1);
//   - leaving Ready resets the consecutive count when the lease was Ready for
//     terminalBudgetResetAfter, then clears the anchor (R2);
//   - a counted failure stays current only from Failing to Failed; any other
//     change ends it.
//
// It never increments the count, so a substrate may call it outside the lease
// actor: it can only move the budget toward a reset.
func (p *ProvisionState) SetStatus(status backend.ProvisionStatus, now time.Time) {
	p.boundBudget().crossStatus(p.Status, status, now)
	p.Status = status
}

// InheritTerminalBudget carries predecessor's budget onto p, a rebuilt
// projection that replaces it, such as a recovery rebuild or an awaited-Ready
// promotion. The replacement is a status change from the predecessor's status
// to p's own, so the Ready boundary applies exactly as SetStatus applies it;
// re-observing the same status crosses nothing and changes nothing. A nil
// predecessor, or one for another lease, leaves a fresh budget, anchored when
// p is Ready. Call it after p's status is final. Like SetStatus it can only
// move the budget toward a reset; it is the only way a budget moves from one
// projection to another.
func (p *ProvisionState) InheritTerminalBudget(predecessor *ProvisionState, now time.Time) {
	from := backend.ProvisionStatus("")
	p.TerminalBudget = TerminalBudget{}
	if predecessor != nil && predecessor.LeaseUUID == p.LeaseUUID &&
		predecessor.TerminalBudget.leaseUUID == p.LeaseUUID {
		from = predecessor.Status
		p.TerminalBudget = predecessor.TerminalBudget
	}
	budget := p.boundBudget()
	if from != p.Status {
		budget.crossStatus(from, p.Status, now)
	}
}

// crossStatus is the one implementation of the Ready boundary. SetStatus and
// InheritTerminalBudget are its only callers.
func (b *TerminalBudget) crossStatus(from, to backend.ProvisionStatus, now time.Time) {
	wasReady := from == backend.ProvisionStatusReady
	isReady := to == backend.ProvisionStatusReady
	switch {
	case wasReady && !isReady:
		if !b.readySince.IsZero() && now.Sub(b.readySince) >= terminalBudgetResetAfter {
			b.consecutive = 0
		}
		b.readySince = time.Time{}
	case !wasReady && isReady:
		b.readySince = now
	}
	if from != backend.ProvisionStatusFailing || to != backend.ProvisionStatusFailed {
		b.lastFailureCounted = false
	}
}

// recordFailure is how the lease state machine records a failure: the status
// change the failure caused, through SetStatus, and then the failure itself.
// Fixing the order here means the sustained-Ready reset always applies before
// a counted failure increments the streak. Only the death of a Ready lease's
// workload (Ready -> Failing) can count, whatever cause a caller passes. It
// must run after the closure has written p.Reason.
func (p *ProvisionState) recordFailure(
	to backend.ProvisionStatus,
	cause failurecause.Cause,
	now time.Time,
) budgetOutcome {
	from := p.Status
	p.SetStatus(to, now)
	budget := p.boundBudget()
	deathOfReadyWorkload := from == backend.ProvisionStatusReady && to == backend.ProvisionStatusFailing
	if !deathOfReadyWorkload || !cause.Counts() || !reasonEligibleForBudget(p.Reason) {
		return budgetOutcome{cause: cause, consecutive: budget.consecutive}
	}
	budget.consecutive++
	budget.lastFailureCounted = true
	return budgetOutcome{
		cause:       cause,
		counted:     true,
		consecutive: budget.consecutive,
		exhausted:   budget.consecutive >= terminalBudgetThreshold,
	}
}

// budgetResetByTenant: an accepted tenant restart or update starts a fresh
// streak.
func (p *ProvisionState) budgetResetByTenant() {
	budget := p.boundBudget()
	budget.consecutive = 0
	budget.lastFailureCounted = false
}

// deathTermination maps the death guard's fresh substrate inspection to the
// classifier's input. Only an exited instance that reported a status is an
// observed exit; removing, dead and positively absent instances are gone;
// anything else is unknown and never counts.
func deathTermination(info *InstanceState) failurecause.Termination {
	switch {
	case info == nil:
		return failurecause.Termination{}
	case info.Phase == PhaseFailed || info.Phase == PhaseAbsent:
		return failurecause.Gone()
	case info.Phase == PhaseExited && info.ExitCode != nil:
		return failurecause.Exited()
	default:
		return failurecause.Termination{}
	}
}

// observeBudgetOutcome emits the side effects of one recorded failure after
// the store closure returned: the attribution metric for every recorded
// failure, and an operator log line for a counted one.
func (lsm *leaseSM) observeBudgetOutcome(
	outcome budgetOutcome,
	info *InstanceState,
	provenance failurecause.Provenance,
) {
	cfg := &lsm.actor.cfg
	cfg.Metrics.LeaseFailureRecorded(outcome.cause)
	if !outcome.counted {
		return
	}
	attrs := []any{
		"lease_uuid", lsm.actor.leaseUUID,
		"consecutive_failures", outcome.consecutive,
		"threshold", terminalBudgetThreshold,
		"budget_exhausted", outcome.exhausted,
		"provenance", provenance.Label(),
	}
	if info != nil {
		if info.ExitCode != nil {
			attrs = append(attrs, "exit_code", *info.ExitCode)
		}
		attrs = append(attrs, "oom_killed", info.OOMKilled)
		if info.ServiceName != "" {
			attrs = append(attrs, "service_name", info.ServiceName)
		}
	}
	cfg.Logger.Warn("tenant workload failure counted", attrs...)
}

// ObserveTerminalBudget is the single place a backend mints the wire
// observation of this lease's budget. It reads the projection's own identity,
// status and reason, so a caller cannot supply a different gate. The verdict is
// exhausted only for a Failed lease whose current failure was counted, whose
// recorded streak reached the threshold, and whose reason is eligible.
// ConsecutiveFailures is the recorded count, which time alone never decays.
func (p *ProvisionState) ObserveTerminalBudget() *backend.TerminalBudgetObservation {
	budget := p.TerminalBudget
	if budget.leaseUUID != p.LeaseUUID {
		budget = TerminalBudget{}
	}
	observation := &backend.TerminalBudgetObservation{
		Verdict:             backend.TerminalVerdictRetry,
		ConsecutiveFailures: budget.consecutive,
	}
	if p.Status == backend.ProvisionStatusFailed && budget.lastFailureCounted &&
		budget.consecutive >= terminalBudgetThreshold && reasonEligibleForBudget(p.Reason) {
		observation.Verdict = backend.TerminalVerdictExhausted
	}
	return observation
}
