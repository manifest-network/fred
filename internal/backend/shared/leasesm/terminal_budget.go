package leasesm

import (
	"time"

	"github.com/manifest-network/fred/internal/backend"
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
// substrate may carry it across projection rebuilds or replace it with the
// zero value, but only the lease actor can advance it, so no off-actor path can
// make a lease closable. The zero value is a fresh budget.
type TerminalBudget struct {
	// leaseUUID binds the budget to its lease. A copy carried onto another
	// lease's projection reads, and is mutated, as a fresh budget.
	leaseUUID string
	// consecutive is the recorded count of consecutive counted failures. Time
	// alone never decays it, so the wire observation changes only at
	// transitions.
	consecutive int
	// readySince is when the lease last entered Ready. It is zero while the
	// lease is not Ready; an off-actor Ready projection may leave an older,
	// earlier anchor in place, which can only bring a reset forward.
	readySince time.Time
	// lastFailureCounted is true only from a counted failure until the next
	// Provisioning or Ready entry or uncounted failure.
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
func reasonEligibleForBudget(reason backend.Reason) bool {
	switch reason {
	case backend.ReasonContainerExited:
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

// budgetEnterProvisioning: a new attempt is not the failure that preceded it.
func (p *ProvisionState) budgetEnterProvisioning() {
	p.boundBudget().lastFailureCounted = false
}

// budgetEnterReady anchors the sustained-Ready reset on this Ready entry.
func (p *ProvisionState) budgetEnterReady(now time.Time) {
	p.ObserveReadyProjection(now)
}

// budgetExitReady applies the sustained-Ready reset when the lease leaves
// Ready, before the transition's own effect, then clears the anchor. It is a
// no-op for a lease that is not Ready.
func (p *ProvisionState) budgetExitReady(now time.Time) {
	budget := p.boundBudget()
	if readyLongEnough(budget, now) {
		budget.consecutive = 0
	}
	budget.readySince = time.Time{}
}

// budgetRecordFailure records one failure that ends the current state. It
// must run after the closure has written p.Reason. A failure from Ready is an
// exit from Ready, so the sustained-Ready reset applies first.
func (p *ProvisionState) budgetRecordFailure(cause failurecause.Cause, now time.Time) budgetOutcome {
	p.budgetExitReady(now)
	budget := p.boundBudget()
	if !cause.Counts() || !reasonEligibleForBudget(p.Reason) {
		budget.lastFailureCounted = false
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

// budgetResetByTenant: a tenant restart or update starts a fresh streak.
func (p *ProvisionState) budgetResetByTenant() {
	budget := p.boundBudget()
	budget.consecutive = 0
	budget.lastFailureCounted = false
}

// budgetClearCurrentFailure re-asserts, without recording anything new, that
// the current failure is not a counted one. Recovery re-application uses it so
// a replay can neither count nor emit a metric twice.
func (p *ProvisionState) budgetClearCurrentFailure() {
	p.boundBudget().lastFailureCounted = false
}

func readyLongEnough(budget *TerminalBudget, now time.Time) bool {
	return !budget.readySince.IsZero() && now.Sub(budget.readySince) >= terminalBudgetResetAfter
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

// ObserveReadyProjection records a Ready projection that a substrate wrote
// outside the lease actor (a recovery rebuild or promotion). It can only move
// the budget toward a reset: it anchors the Ready period only if none is
// recorded, so an earlier anchor wins, and it marks the current failure as not
// counted. It never increases the count.
func (p *ProvisionState) ObserveReadyProjection(now time.Time) {
	budget := p.boundBudget()
	if budget.readySince.IsZero() {
		budget.readySince = now
	}
	budget.lastFailureCounted = false
}

// ObserveUncountedFailureProjection records a failure that a substrate wrote
// outside the lease actor, which never counts. It can only move the budget
// toward a reset: it applies the sustained-Ready reset when one is due and
// marks the current failure as not counted. It leaves the Ready anchor in
// place, so a later reset can only come sooner.
func (p *ProvisionState) ObserveUncountedFailureProjection(now time.Time) {
	budget := p.boundBudget()
	if readyLongEnough(budget, now) {
		budget.consecutive = 0
	}
	budget.lastFailureCounted = false
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
