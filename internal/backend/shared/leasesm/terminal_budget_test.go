package leasesm

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
)

// --- provenance and inspection fixtures --------------------------------------

// The provenance fixtures mint for one instance, as the event loop does; the
// harness passes the dying container's ID.

func observedRun(id string) failurecause.Provenance {
	session := failurecause.NewEventSession()
	session.ObserveStart(id)
	return session.ObserveExit(id)
}

func signaledRun(id string) failurecause.Provenance {
	session := failurecause.NewEventSession()
	session.ObserveStart(id)
	session.ObserveSignal(id)
	return session.ObserveExit(id)
}

func partialRun(id string) failurecause.Provenance {
	return failurecause.NewEventSession().ObserveExit(id)
}

// unobserved is a death found without a live event, such as by the sweep.
func unobserved(string) failurecause.Provenance { return failurecause.Provenance{} }

// observedOtherRun is an observed run minted for some other container.
func observedOtherRun(string) failurecause.Provenance { return observedRun("another-container") }

// exitedWith is an exit as the substrate adapter classifies it: an observed
// exit with a status.
func exitedWith(code int) InstanceState {
	return InstanceState{Phase: PhaseExited, ExitCode: &code, Termination: failurecause.Exited()}
}

// gone is an instance the substrate adapter classifies as removing, dead or
// absent.
func gone(phase Phase) InstanceState {
	return InstanceState{Phase: phase, Termination: failurecause.Gone()}
}

func tenantCause() failurecause.Cause {
	return failurecause.ClassifyDeath("run", observedRun("run"), failurecause.Exited())
}

// --- unit tests: the budget rules ---------------------------------------------

// readyAt returns a projection of the test lease with the given recorded
// streak, which entered Ready at t0 through SetStatus. A non-empty streak
// started an hour before t0, so the minimum-span floor is already met and the
// count alone decides; readyWithStreak chooses the start.
func readyAt(t0 time.Time, consecutive int) *ProvisionState {
	return readyWithStreak(t0, consecutive, t0.Add(-time.Hour))
}

func readyWithStreak(t0 time.Time, consecutive int, streakStartedAt time.Time) *ProvisionState {
	budget := TerminalBudget{leaseUUID: testActorLeaseUUID, consecutive: consecutive}
	if consecutive > 0 {
		budget.streakStartedAt = streakStartedAt
	}
	p := &ProvisionState{
		LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusProvisioning,
		Reason:         backend.ReasonContainerExited,
		TerminalBudget: budget,
	}
	p.SetStatus(backend.ProvisionStatusReady, t0)
	return p
}

// everyStatus is the declared status vocabulary plus values a projection must
// tolerate without a special case.
var everyStatus = []backend.ProvisionStatus{
	backend.ProvisionStatusProvisioning, backend.ProvisionStatusReady, backend.ProvisionStatusFailing,
	backend.ProvisionStatusFailed, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating,
	backend.ProvisionStatusDeprovisioning, backend.ProvisionStatusRetained, backend.ProvisionStatusUnknown,
	"", "bogus",
}

// everyStanding is the closed standingFailure vocabulary plus an undeclared
// value, which must read as not exhausting.
var everyStanding = []standingFailure{noCountedFailure, countedWithinBudget, countedExhausting, 99}

func uncountedCauses() []failurecause.Cause {
	return []failurecause.Cause{
		{}, failurecause.Platform(), failurecause.Maintenance(),
		failurecause.ClassifyDeath("run", signaledRun("run"), failurecause.Exited()),
	}
}

// streakVerdict is the one decision between the counted standings: both the
// count threshold and the minimum span, measured from the streak's start to
// the failure being counted, must be met. A streak with no recorded start
// never exhausts.
func TestStreakVerdict_BothFloors(t *testing.T) {
	start := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	spans := []time.Duration{
		0, time.Minute, terminalBudgetMinStreakSpan - time.Nanosecond,
		terminalBudgetMinStreakSpan, terminalBudgetMinStreakSpan + time.Hour, -time.Hour,
	}
	for consecutive := range terminalBudgetThreshold + 3 {
		for _, span := range spans {
			got := streakVerdict(consecutive, start, start.Add(span))
			want := countedWithinBudget
			if consecutive >= terminalBudgetThreshold && span >= terminalBudgetMinStreakSpan {
				want = countedExhausting
			}
			assert.Equal(t, want, got, "consecutive=%d span=%s", consecutive, span)
		}
		assert.Equal(t, countedWithinBudget, streakVerdict(consecutive, time.Time{}, start.Add(time.Hour)),
			"no recorded start")
	}
	for _, standing := range everyStanding {
		assert.Equal(t, standing == countedExhausting, standing.exhausts(), "standing %d", standing)
	}
}

// countFailure starts the streak as the count leaves zero, never moves the
// start of a running streak, and records the standing the extended streak
// decides; resetStreak clears the count, start and standing together.
func TestTerminalBudget_StreakStartAndReset(t *testing.T) {
	t0 := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	var budget TerminalBudget
	assert.Equal(t, countedWithinBudget, budget.countFailure(t0))
	assert.Equal(t, t0, budget.streakStartedAt, "the first counted failure starts the streak")
	assert.Equal(t, countedWithinBudget, budget.countFailure(t0.Add(10*time.Minute)))
	assert.Equal(t, countedWithinBudget, budget.countFailure(t0.Add(20*time.Minute)),
		"three failures inside the minimum span only retry")
	assert.Equal(t, t0, budget.streakStartedAt, "a running streak keeps its start")
	assert.Equal(t, countedExhausting, budget.countFailure(t0.Add(terminalBudgetMinStreakSpan)),
		"the next counted failure re-evaluates and exhausts once the streak is old enough")
	assert.Equal(t, 4, budget.consecutive)

	budget.resetStreak()
	assert.Equal(t, TerminalBudget{}, budget, "a reset clears the count, its start and the standing")

	// A streak with a count but no recorded start (never produced by the
	// actor) starts at the failure being counted: it can only delay.
	orphan := TerminalBudget{consecutive: 5}
	assert.Equal(t, countedWithinBudget, orphan.countFailure(t0))
	assert.Equal(t, t0, orphan.streakStartedAt)
}

func TestTerminalBudget_ConsecutiveCountAndSustainedReadyReset(t *testing.T) {
	t0 := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)

	// Each death comes one minute after Ready; between deaths the lease sits
	// Failed for gap. Failed time is not Ready time, so it never resets.
	crashLoop := func(t *testing.T, gap time.Duration) []budgetOutcome {
		p := readyAt(t0, 0)
		now := t0
		var outcomes []budgetOutcome
		for want := 1; want <= terminalBudgetThreshold+1; want++ {
			now = now.Add(time.Minute)
			outcome := p.recordFailure(backend.ProvisionStatusFailing, tenantCause(), now)
			require.True(t, outcome.counted)
			assert.Equal(t, want, outcome.consecutive)
			p.SetStatus(backend.ProvisionStatusFailed, now) // diagnostics gathered
			assert.NotEqual(t, noCountedFailure, p.TerminalBudget.standing, "Failing -> Failed keeps the counted failure")
			assert.Equal(t, outcome.exhausted, p.ObserveTerminalBudget().Verdict == backend.TerminalVerdictExhausted)
			now = now.Add(gap)
			p.SetStatus(backend.ProvisionStatusProvisioning, now)
			assert.Equal(t, noCountedFailure, p.TerminalBudget.standing, "a new attempt ends the counted failure")
			p.SetStatus(backend.ProvisionStatusReady, now)
			outcomes = append(outcomes, outcome)
		}
		return outcomes
	}

	t.Run("deaths inside the minimum span never exhaust", func(t *testing.T) {
		for _, outcome := range crashLoop(t, 5*time.Minute) {
			assert.False(t, outcome.exhausted, "death %d at most 18 minutes into the streak", outcome.consecutive)
		}
	})

	t.Run("the third death past the minimum span exhausts", func(t *testing.T) {
		outcomes := crashLoop(t, 15*time.Minute) // deaths at 1, 17 and 33 minutes
		for _, outcome := range outcomes {
			assert.Equal(t, outcome.consecutive >= terminalBudgetThreshold, outcome.exhausted,
				"death %d", outcome.consecutive)
		}
	})

	t.Run("a death after the reset period starts a new streak", func(t *testing.T) {
		for _, ready := range []time.Duration{terminalBudgetResetAfter, terminalBudgetResetAfter + time.Hour} {
			p := readyAt(t0, 2)
			now := t0.Add(ready)
			outcome := p.recordFailure(backend.ProvisionStatusFailing, tenantCause(), now)
			assert.Equal(t, 1, outcome.consecutive, "Ready for %s must reset the streak", ready)
			assert.Equal(t, now, p.TerminalBudget.streakStartedAt, "the new streak starts at its first failure")
			assert.False(t, outcome.exhausted)
		}
		p := readyAt(t0, 2)
		outcome := p.recordFailure(backend.ProvisionStatusFailing, tenantCause(),
			t0.Add(terminalBudgetResetAfter-time.Nanosecond))
		assert.Equal(t, 3, outcome.consecutive, "just short of the reset period the streak continues")
		assert.Equal(t, t0.Add(-time.Hour), p.TerminalBudget.streakStartedAt, "and keeps its start")
		assert.True(t, outcome.exhausted)
	})

	t.Run("a counting cause counts only at a counting transition", func(t *testing.T) {
		// The closed set: the death of a Ready workload, and a provision's
		// definite startup failure (ENG-1125).
		counting := map[[2]backend.ProvisionStatus]bool{
			{backend.ProvisionStatusReady, backend.ProvisionStatusFailing}:       true,
			{backend.ProvisionStatusProvisioning, backend.ProvisionStatusFailed}: true,
		}
		for transition := range counting {
			p := &ProvisionState{
				LeaseUUID: testActorLeaseUUID, Status: transition[0], Reason: backend.ReasonContainerExited,
				TerminalBudget: TerminalBudget{
					leaseUUID: testActorLeaseUUID, consecutive: 2, streakStartedAt: t0.Add(-time.Hour),
				},
			}
			outcome := p.recordFailure(transition[1], tenantCause(), t0)
			assert.True(t, outcome.counted, "%s -> %s", transition[0], transition[1])
			assert.Equal(t, 3, p.TerminalBudget.consecutive, "%s -> %s", transition[0], transition[1])
		}
		for _, from := range everyStatus {
			for _, to := range everyStatus {
				if counting[[2]backend.ProvisionStatus{from, to}] {
					continue
				}
				p := &ProvisionState{
					LeaseUUID: testActorLeaseUUID, Status: from, Reason: backend.ReasonContainerExited,
					TerminalBudget: TerminalBudget{
						leaseUUID: testActorLeaseUUID, consecutive: 2, streakStartedAt: t0.Add(-time.Hour),
					},
				}
				outcome := p.recordFailure(to, tenantCause(), t0)
				assert.False(t, outcome.counted, "%s -> %s", from, to)
				assert.Equal(t, 2, p.TerminalBudget.consecutive, "%s -> %s", from, to)
				assert.Equal(t, noCountedFailure, p.TerminalBudget.standing, "%s -> %s", from, to)
				assert.Equal(t, to, p.Status, "the failure is the status change")
			}
		}
	})

	// BACKEND_GUIDE's normative rule: an uncounted failure never increments the
	// streak, and as any exit from Ready it still applies the sustained-Ready
	// reset; short of the reset period it leaves the streak unchanged.
	t.Run("an uncounted failure never increments; leaving Ready applies the reset", func(t *testing.T) {
		for _, cause := range uncountedCauses() {
			short := readyAt(t0, 2)
			outcome := short.recordFailure(backend.ProvisionStatusFailing, cause, t0.Add(time.Minute))
			assert.False(t, outcome.counted, cause.Label())
			assert.Equal(t, 2, short.TerminalBudget.consecutive, cause.Label())
			assert.Equal(t, t0.Add(-time.Hour), short.TerminalBudget.streakStartedAt, cause.Label())
			assert.Equal(t, noCountedFailure, short.TerminalBudget.standing, cause.Label())
			assert.True(t, short.TerminalBudget.readySince.IsZero(), "the failure ends the Ready period")

			sustained := readyAt(t0, 2)
			outcome = sustained.recordFailure(backend.ProvisionStatusFailing, cause, t0.Add(11*time.Minute))
			assert.False(t, outcome.counted, cause.Label())
			assert.Zero(t, sustained.TerminalBudget.consecutive, "%s after a sustained Ready", cause.Label())
			assert.True(t, sustained.TerminalBudget.streakStartedAt.IsZero(), "%s clears the start", cause.Label())
		}
	})

	t.Run("an ineligible reason never counts", func(t *testing.T) {
		for _, reason := range []backend.Reason{"", backend.ReasonInternal, backend.ReasonUnknown, "Bogus"} {
			p := readyAt(t0, 0)
			p.Reason = reason
			outcome := p.recordFailure(backend.ProvisionStatusFailing, tenantCause(), t0)
			assert.False(t, outcome.counted, "reason %q", reason)
			assert.Zero(t, p.TerminalBudget.consecutive)
			assert.True(t, p.TerminalBudget.streakStartedAt.IsZero(), "an uncounted failure starts no streak")
		}
	})

	t.Run("a budget carried onto another lease is fresh", func(t *testing.T) {
		p := readyAt(t0, 0)
		p.TerminalBudget = TerminalBudget{
			leaseUUID: "22222222-2222-4222-8222-222222222222", consecutive: 2,
			streakStartedAt: t0.Add(-time.Hour), standing: countedExhausting,
		}
		assert.Equal(t, 0, p.ObserveTerminalBudget().ConsecutiveFailures)
		outcome := p.recordFailure(backend.ProvisionStatusFailing, tenantCause(), t0)
		assert.Equal(t, 1, outcome.consecutive, "a foreign streak never contributes")
		assert.Equal(t, t0, p.TerminalBudget.streakStartedAt, "nor does its start")
		assert.Equal(t, testActorLeaseUUID, p.TerminalBudget.leaseUUID)
	})

	t.Run("a tenant reset clears the streak", func(t *testing.T) {
		p := readyAt(t0, 2)
		p.TerminalBudget.standing = countedExhausting
		p.budgetResetByTenant()
		assert.Zero(t, p.TerminalBudget.consecutive)
		assert.True(t, p.TerminalBudget.streakStartedAt.IsZero())
		assert.Equal(t, noCountedFailure, p.TerminalBudget.standing)
	})
}

// SetStatus is the one Ready boundary. Every pair of statuses, from every
// anchor and standing failure: entering Ready anchors at now, leaving Ready
// resets the streak (count and start) after a sustained Ready and clears the
// anchor, nothing else touches either, and a counted failure survives only
// Failing -> Failed.
func TestSetStatus_ReadyBoundaryForEveryTransition(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	streakStart := now.Add(-time.Hour)
	anchors := []time.Time{{}, now.Add(-11 * time.Minute), now.Add(-time.Minute)}
	for _, from := range everyStatus {
		for _, to := range everyStatus {
			for _, anchor := range anchors {
				for _, standing := range everyStanding {
					p := &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: from}
					p.TerminalBudget = TerminalBudget{
						leaseUUID: testActorLeaseUUID, consecutive: 2, streakStartedAt: streakStart,
						readySince: anchor, standing: standing,
					}
					p.SetStatus(to, now)
					label := fmt.Sprintf("%q -> %q anchor=%v standing=%d", from, to, anchor, standing)
					budget := p.TerminalBudget
					require.Equal(t, to, p.Status, label)
					switch {
					case from == backend.ProvisionStatusReady && to != backend.ProvisionStatusReady:
						want, wantStart := 2, streakStart
						if !anchor.IsZero() && now.Sub(anchor) >= terminalBudgetResetAfter {
							want, wantStart = 0, time.Time{}
						}
						assert.Equal(t, want, budget.consecutive, label)
						assert.Equal(t, wantStart, budget.streakStartedAt, label)
						assert.True(t, budget.readySince.IsZero(), label)
					case from != backend.ProvisionStatusReady && to == backend.ProvisionStatusReady:
						assert.Equal(t, 2, budget.consecutive, label)
						assert.Equal(t, streakStart, budget.streakStartedAt, label)
						assert.Equal(t, now, budget.readySince, label)
					default:
						assert.Equal(t, 2, budget.consecutive, label)
						assert.Equal(t, streakStart, budget.streakStartedAt, label)
						assert.Equal(t, anchor, budget.readySince, label)
					}
					want := noCountedFailure
					if from == to || (from == backend.ProvisionStatusFailing && to == backend.ProvisionStatusFailed) {
						want = standing
					}
					assert.Equal(t, want, budget.standing, label)
					if from == to {
						assert.Equal(t, TerminalBudget{
							leaseUUID: testActorLeaseUUID, consecutive: 2, streakStartedAt: streakStart,
							readySince: anchor, standing: standing,
						}, budget, "%s: re-asserting the status changes nothing", label)
					}
				}
			}
		}
	}
}

// A defensive re-write of Failed onto a lease that is already Failed and
// exhausted must not turn its verdict back into retry: the lease would then be
// re-provisioned every pass and never closed (ENG-799 review). SetStatus and
// InheritTerminalBudget agree on it.
func TestSetStatus_SameStatusKeepsAnExhaustedVerdict(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	exhausted := func() *ProvisionState {
		return &ProvisionState{
			LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusFailed,
			Reason: backend.ReasonContainerExited,
			TerminalBudget: TerminalBudget{
				leaseUUID: testActorLeaseUUID, consecutive: terminalBudgetThreshold,
				streakStartedAt: now.Add(-time.Hour), standing: countedExhausting,
			},
		}
	}
	p := exhausted()
	require.Equal(t, backend.TerminalVerdictExhausted, p.ObserveTerminalBudget().Verdict)
	p.SetStatus(backend.ProvisionStatusFailed, now)
	assert.Equal(t, exhausted().TerminalBudget, p.TerminalBudget)
	assert.Equal(t, backend.TerminalVerdictExhausted, p.ObserveTerminalBudget().Verdict)

	rebuilt := &ProvisionState{
		LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusFailed,
		Reason: backend.ReasonContainerExited,
	}
	rebuilt.InheritTerminalBudget(exhausted(), now)
	assert.Equal(t, exhausted().TerminalBudget, rebuilt.TerminalBudget)
	assert.Equal(t, backend.TerminalVerdictExhausted, rebuilt.ObserveTerminalBudget().Verdict)
}

// A replacement projection inherits its predecessor's budget and crosses the
// Ready boundary from the predecessor's status to its own; re-observing the
// same status changes nothing. Without a predecessor of the same lease the
// budget is fresh, anchored when the replacement is Ready.
func TestInheritTerminalBudget_ReplacementCrossesTheReadyBoundary(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	readySince := now.Add(-11 * time.Minute)
	for _, from := range everyStatus {
		for _, to := range everyStatus {
			predecessor := &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: from}
			predecessor.TerminalBudget = TerminalBudget{
				leaseUUID: testActorLeaseUUID, consecutive: 2, streakStartedAt: now.Add(-time.Hour),
				standing: countedWithinBudget,
			}
			if from == backend.ProvisionStatusReady {
				predecessor.TerminalBudget.readySince = readySince
				predecessor.TerminalBudget.standing = noCountedFailure
			}
			replacement := &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: to}
			replacement.InheritTerminalBudget(predecessor, now)
			label := fmt.Sprintf("%q -> %q", from, to)
			if from == to {
				assert.Equal(t, predecessor.TerminalBudget, replacement.TerminalBudget, label)
				continue
			}
			want := predecessor.TerminalBudget
			want.crossStatus(from, to, now)
			assert.Equal(t, want, replacement.TerminalBudget, label)
		}
	}

	fresh := map[string]*ProvisionState{
		"none": nil,
		"another lease": {
			LeaseUUID: "22222222-2222-4222-8222-222222222222", Status: backend.ProvisionStatusFailed,
			TerminalBudget: TerminalBudget{
				leaseUUID: "22222222-2222-4222-8222-222222222222", consecutive: 3,
				streakStartedAt: now.Add(-time.Hour), standing: countedExhausting,
			},
		},
		"an unbound budget": {
			LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusFailed,
			TerminalBudget: TerminalBudget{consecutive: 3, streakStartedAt: now.Add(-time.Hour), standing: countedExhausting},
		},
	}
	for name, predecessor := range fresh {
		for _, to := range everyStatus {
			replacement := &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: to}
			replacement.InheritTerminalBudget(predecessor, now)
			want := TerminalBudget{leaseUUID: testActorLeaseUUID}
			if to == backend.ProvisionStatusReady {
				want.readySince = now
			}
			assert.Equal(t, want, replacement.TerminalBudget, "%s -> %q", name, to)
		}
	}
}

func TestTerminalBudget_ObserveVerdictMatrix(t *testing.T) {
	reasons := []backend.Reason{backend.ReasonContainerExited, backend.ReasonInternal, backend.ReasonUnknown, "", "Bogus"}
	bindings := map[string]string{
		"same": testActorLeaseUUID, "other": "22222222-2222-4222-8222-222222222222", "unbound": "",
	}
	for _, status := range everyStatus {
		for _, reason := range reasons {
			for bindingName, binding := range bindings {
				for consecutive := range terminalBudgetThreshold + 2 {
					for _, standing := range everyStanding {
						p := ProvisionState{
							LeaseUUID: testActorLeaseUUID, Status: status, Reason: reason,
							TerminalBudget: TerminalBudget{
								leaseUUID: binding, consecutive: consecutive, standing: standing,
							},
						}
						got := p.ObserveTerminalBudget()
						require.NotNil(t, got)
						wantExhausted := status == backend.ProvisionStatusFailed && standing == countedExhausting &&
							consecutive >= terminalBudgetThreshold && reason == backend.ReasonContainerExited &&
							bindingName == "same"
						name := fmt.Sprintf("%s/%s/%s/%d/%d", status, reason, bindingName, consecutive, standing)
						if wantExhausted {
							assert.Equal(t, backend.TerminalVerdictExhausted, got.Verdict, name)
						} else {
							assert.Equal(t, backend.TerminalVerdictRetry, got.Verdict, name)
						}
						wantCount := consecutive
						if bindingName != "same" {
							wantCount = 0
						}
						assert.Equal(t, wantCount, got.ConsecutiveFailures, name)
					}
				}
			}
		}
	}
	zero := ProvisionState{LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusFailed}
	assert.Equal(t, &backend.TerminalBudgetObservation{Verdict: backend.TerminalVerdictRetry}, zero.ObserveTerminalBudget(),
		"a fresh budget reports retry with no failures")
}

// The observation is the recorded fact. Neither the Ready anchor nor the
// streak's age changes it: a streak recorded as within the budget stays retry
// however old it grows, until the next counted failure re-evaluates it.
func TestTerminalBudget_ObservationIgnoresElapsedTime(t *testing.T) {
	t0 := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	p := ProvisionState{LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusReady}
	p.TerminalBudget = TerminalBudget{leaseUUID: testActorLeaseUUID, consecutive: 2, readySince: t0}
	first := p.ObserveTerminalBudget()
	p.TerminalBudget.readySince = t0.Add(-365 * 24 * time.Hour)
	assert.Equal(t, first, p.ObserveTerminalBudget(),
		"the observation is the recorded count: time alone never changes it")

	failed := ProvisionState{
		LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusFailed, Reason: backend.ReasonContainerExited,
	}
	failed.TerminalBudget = TerminalBudget{
		leaseUUID: testActorLeaseUUID, consecutive: terminalBudgetThreshold,
		streakStartedAt: t0, standing: countedWithinBudget,
	}
	young := failed.ObserveTerminalBudget()
	assert.Equal(t, backend.TerminalVerdictRetry, young.Verdict)
	failed.TerminalBudget.streakStartedAt = t0.Add(-365 * 24 * time.Hour)
	assert.Equal(t, young, failed.ObserveTerminalBudget(),
		"a streak recorded as too young stays retry however old it grows")
}

// The exported methods of *ProvisionState are the only budget mutators a
// substrate can reach. The set is pinned, and every exported mutator in it is
// driven below: whatever the starting budget and statuses, it can only move
// the budget toward a reset. It never raises the count, never moves a running
// streak's start, never records a counted failure, and an exhausted verdict
// after it rests on an exhausting failure the actor had already recorded.
func TestTerminalBudget_ExportedMutatorsOnlyMoveTowardReset(t *testing.T) {
	var exported []string
	methods := reflect.TypeFor[*ProvisionState]()
	for index := range methods.NumMethod() {
		exported = append(exported, methods.Method(index).Name)
	}
	require.Equal(t, []string{"AwaitOperation", "InheritTerminalBudget", "ObserveTerminalBudget", "SetStatus"}, exported,
		"a new exported *ProvisionState method needs a driver here")

	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	mutators := map[string]func(p *ProvisionState, to backend.ProvisionStatus, predecessor *ProvisionState){
		"SetStatus": func(p *ProvisionState, to backend.ProvisionStatus, _ *ProvisionState) {
			p.SetStatus(to, now)
		},
		"InheritTerminalBudget": func(p *ProvisionState, to backend.ProvisionStatus, predecessor *ProvisionState) {
			p.Status = to
			p.InheritTerminalBudget(predecessor, now)
		},
	}
	// AwaitOperation (ENG-1125) stamps the awaited operation and never reads or
	// writes the budget at all: driven separately below.
	for _, name := range exported {
		if name != "ObserveTerminalBudget" && name != "AwaitOperation" {
			require.Contains(t, mutators, name)
		}
	}
	anchors := []time.Time{{}, now.Add(-time.Hour), now.Add(-time.Minute), now}
	streakStart := now.Add(-2 * time.Hour)
	operation := newTestOperationFixture(t, testActorLeaseUUID, shared.OperationIntentProvision).admission.Operation()
	for _, binding := range []string{testActorLeaseUUID, "22222222-2222-4222-8222-222222222222"} {
		for consecutive := range terminalBudgetThreshold + 2 {
			for _, standing := range everyStanding {
				before := TerminalBudget{
					leaseUUID: binding, consecutive: consecutive, streakStartedAt: streakStart, standing: standing,
				}
				p := &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusFailed, TerminalBudget: before}
				require.True(t, p.AwaitOperation(operation))
				assert.Equal(t, before, p.TerminalBudget, "AwaitOperation leaves the budget byte-identical")
			}
		}
	}
	for name, apply := range mutators {
		for _, binding := range []string{testActorLeaseUUID, "22222222-2222-4222-8222-222222222222"} {
			for consecutive := range terminalBudgetThreshold + 2 {
				for _, standing := range everyStanding {
					for _, anchor := range anchors {
						for _, from := range everyStatus {
							for _, to := range everyStatus {
								before := TerminalBudget{
									leaseUUID: binding, consecutive: consecutive, streakStartedAt: streakStart,
									readySince: anchor, standing: standing,
								}
								p := &ProvisionState{
									LeaseUUID: testActorLeaseUUID, Status: from,
									Reason: backend.ReasonContainerExited, TerminalBudget: before,
								}
								predecessor := &ProvisionState{
									LeaseUUID: testActorLeaseUUID, Status: from,
									Reason: backend.ReasonContainerExited, TerminalBudget: before,
								}
								apply(p, to, predecessor)
								after := p.TerminalBudget
								label := fmt.Sprintf("%s %q -> %q %+v", name, from, to, before)
								bound := binding == testActorLeaseUUID
								if bound {
									assert.LessOrEqual(t, after.consecutive, before.consecutive, label)
								} else {
									assert.Zero(t, after.consecutive, label)
								}
								if after.consecutive > 0 {
									assert.Equal(t, streakStart, after.streakStartedAt, "%s: a running streak keeps its start", label)
								} else if after.consecutive == 0 && before.consecutive > 0 {
									assert.True(t, after.streakStartedAt.IsZero(), "%s: a reset clears the start", label)
								}
								if after.standing != noCountedFailure {
									assert.True(t, bound && after.standing == before.standing, label)
								}
								if bound && from == to {
									assert.Equal(t, before, after, "%s: a same-status write is not a crossing", label)
								}
								if p.ObserveTerminalBudget().Verdict == backend.TerminalVerdictExhausted {
									assert.True(t, bound && before.standing == countedExhausting &&
										before.consecutive >= terminalBudgetThreshold, label)
								}
							}
						}
					}
				}
			}
		}
	}
}

// TestReasonEligibleForBudget_EveryDeclaredReason requires an explicit decision
// for every Reason declared in internal/backend/reason.go. Adding a Reason
// without deciding whether it may count fails here.
func TestReasonEligibleForBudget_EveryDeclaredReason(t *testing.T) {
	decisions := map[string]bool{
		"ReasonContainerExited": true,
		// A health check that never passed is a definite failure that never
		// counts (ENG-1125).
		"ReasonHealthCheckFailed": false,
		// A start the container runtime refused ran no tenant process; it is a
		// definite failure that never counts (ENG-1125).
		"ReasonContainerStartFailed":   false,
		"ReasonImagePullFailed":        false,
		"ReasonInternal":               false,
		"ReasonRestartFailed":          false,
		"ReasonUpdateFailed":           false,
		"ReasonRestoreFailed":          false,
		"ReasonVolumeCleanupExhausted": false,
		"ReasonCleanupFailed":          false,
		"ReasonBackendStorageLost":     false,
		"ReasonUnknown":                false,
	}
	declared := declaredConstants(t, filepath.Join("..", "..", "reason.go"), "Reason")
	require.Equal(t, slices.Sorted(maps.Keys(decisions)), slices.Sorted(maps.Keys(declared)),
		"decide budget eligibility for every declared Reason")
	for name, reason := range declared {
		assert.Equal(t, decisions[name], reasonEligibleForBudget(backend.Reason(reason)), name)
	}
	assert.False(t, reasonEligibleForBudget(""))
	assert.False(t, reasonEligibleForBudget("Bogus"))
}

// TestTenantInitiatedMaintenance_EveryDeclaredKind requires an explicit
// decision for every maintenance kind declared in shared/maintenance_intent.go:
// only a tenant's own restart or update resets the streak.
func TestTenantInitiatedMaintenance_EveryDeclaredKind(t *testing.T) {
	decisions := map[string]bool{
		"MaintenanceIntentRestart":      true,
		"MaintenanceIntentUpdate":       true,
		"MaintenanceIntentCustomDomain": false,
	}
	declared := declaredConstants(t, filepath.Join("..", "maintenance_intent.go"), "MaintenanceIntentKind")
	require.Equal(t, slices.Sorted(maps.Keys(decisions)), slices.Sorted(maps.Keys(declared)),
		"decide whether every declared maintenance kind is tenant-initiated")
	for name, kind := range declared {
		assert.Equal(t, decisions[name], tenantInitiatedMaintenance(shared.MaintenanceIntentKind(kind)), name)
	}
	assert.False(t, tenantInitiatedMaintenance(""))
	assert.False(t, tenantInitiatedMaintenance("bogus"))
}

// declaredConstants parses the string constants of one named type from a
// source file.
func declaredConstants(t *testing.T, path, typeName string) map[string]string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	require.NoError(t, err)
	declared := make(map[string]string)
	for _, decl := range file.Decls {
		general, ok := decl.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			value := spec.(*ast.ValueSpec)
			typeIdent, typed := value.Type.(*ast.Ident)
			if !typed || typeIdent.Name != typeName {
				continue
			}
			for index, name := range value.Names {
				literal, ok := value.Values[index].(*ast.BasicLit)
				require.True(t, ok, "%s must be a literal", name.Name)
				text, err := strconv.Unquote(literal.Value)
				require.NoError(t, err)
				declared[name.Name] = text
			}
		}
	}
	require.NotEmpty(t, declared, "no %s constants in %s", typeName, path)
	return declared
}

// TestEveryFailCountIncrementRecordsBudgetFailure pins the choke point: every
// store closure in this package's production code that mutates the lifetime
// FailCount, in any spelling, must also record the failure through
// recordFailure, so no failure path can skip attribution.
func TestEveryFailCountIncrementRecordsBudgetFailure(t *testing.T) {
	files, err := filepath.Glob("*.go")
	require.NoError(t, err)
	fset := token.NewFileSet()
	increments := 0
	var violations []string
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		found, fileViolations := failCountClosureViolations(fset, file)
		increments += found
		violations = append(violations, fileViolations...)
	}
	assert.GreaterOrEqual(t, increments, 5, "the guard must see every FailCount increment site")
	assert.Empty(t, violations)
}

func TestFailCountGuardFires(t *testing.T) {
	const src = `package leasesm
func a(store LeaseProvisionStore) {
	store.UpdateFn("l", func(p *ProvisionState) { p.FailCount++ })
	store.UpdateFn("l", func(p *ProvisionState) { p.FailCount++; _ = p.recordFailure(s, c, n) })
	store.UpdateFn("l", func(p *ProvisionState) { p.FailCount += 1 })
	store.UpdateFn("l", func(p *ProvisionState) { p.FailCount = p.FailCount + 1 })
	record := func(p *ProvisionState) { p.FailCount++ }
	store.UpdateFn("l", record)
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "synthetic.go", src, parser.SkipObjectResolution)
	require.NoError(t, err)
	increments, violations := failCountClosureViolations(fset, file)
	assert.Equal(t, 5, increments)
	require.Len(t, violations, 4, "%v", violations)
	for _, line := range []string{"synthetic.go:3", "synthetic.go:5", "synthetic.go:6", "synthetic.go:7"} {
		assert.True(t, slices.ContainsFunc(violations, func(v string) bool { return strings.HasPrefix(v, line) }),
			"missing %s in %v", line, violations)
	}
}

// failCountClosureViolations finds every function literal that mutates a
// FailCount (++, an op-assign or a plain assignment) and reports it unless the
// same literal records the failure through recordFailure. Any function literal
// counts, not only one passed inline to UpdateFn, so a named closure cannot
// escape the rule.
func failCountClosureViolations(fset *token.FileSet, file *ast.File) (int, []string) {
	increments := 0
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		closure, ok := node.(*ast.FuncLit)
		if !ok {
			return true
		}
		mutatesFailCount, records := false, false
		ast.Inspect(closure.Body, func(inner ast.Node) bool {
			switch typed := inner.(type) {
			case *ast.FuncLit:
				return false // a nested literal is checked on its own
			case *ast.IncDecStmt:
				if target, ok := typed.X.(*ast.SelectorExpr); ok && target.Sel.Name == "FailCount" {
					mutatesFailCount = true
				}
			case *ast.AssignStmt:
				for _, lhs := range typed.Lhs {
					if target, ok := lhs.(*ast.SelectorExpr); ok && target.Sel.Name == "FailCount" {
						mutatesFailCount = true
					}
				}
			case *ast.CallExpr:
				if target, ok := typed.Fun.(*ast.SelectorExpr); ok && target.Sel.Name == "recordFailure" {
					records = true
				}
			}
			return true
		})
		if mutatesFailCount {
			increments++
			if !records {
				violations = append(violations, fset.Position(closure.Pos()).String()+
					": closure mutates FailCount without recording the budget failure")
			}
		}
		return true
	})
	return increments, violations
}

// --- state machine sequences --------------------------------------------------

// budgetHarness drives one lease's real state machine synchronously: an actor
// without a run loop whose transitions are fired directly, with each worker's
// terminal message handed back by hand. Time is the synctest bubble's fake
// clock, so the sequences sleep through minutes instantly.
type budgetHarness struct {
	t                *testing.T
	ctx              context.Context
	actor            *LeaseActor
	store            *mockProvisionStore
	metrics          *countingMetrics
	provisionRuntime shared.RuntimeGenerationProof
	runtime          shared.RuntimeGenerationProof
	inspection       atomic.Pointer[InstanceState]

	maintenanceMu      sync.Mutex
	maintenanceOutcome ReplaceWorkOutcome
}

func newBudgetHarness(t *testing.T) *budgetHarness {
	t.Helper()
	h := &budgetHarness{
		t: t, ctx: context.Background(), store: newMockProvisionStore(), metrics: &countingMetrics{},
	}
	h.store.put(testActorLeaseUUID, &ProvisionState{
		LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusProvisioning,
	})
	h.actor = newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{
		ProvisionStore: h.store,
		Metrics:        h.metrics,
		Inspector: &mockInstanceInspector{InspectInstanceFn: func(context.Context, string) (*InstanceState, error) {
			state := h.inspection.Load()
			if state == nil {
				return nil, errors.New("no inspection configured")
			}
			observed := *state
			return &observed, nil
		}},
		MaintenanceWorkFn: func(shared.MaintenanceWorkerLifetime, shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
			h.maintenanceMu.Lock()
			defer h.maintenanceMu.Unlock()
			return h.maintenanceOutcome
		},
	})
	h.provisionRuntime = newTestRuntimeGenerationProof(t, testActorLeaseUUID)
	h.runtime = h.provisionRuntime
	return h
}

// startProvision enters Provisioning from the reserved or Failed state.
func (h *budgetHarness) startProvision() {
	h.t.Helper()
	require.NoError(h.t, h.actor.sm.requestProvision(h.ctx, shared.OperationIntentClaim{}))
	require.Equal(h.t, backend.ProvisionStatusProvisioning, h.actor.sm.State())
}

// completeProvision commits a successful provision: Provisioning -> Ready.
func (h *budgetHarness) completeProvision() {
	h.t.Helper()
	_, success := testProvisionSuccess(h.t, testActorLeaseUUID, ProvisionSuccessProjection{
		ContainerIDs: []string{"container-a"},
	})
	require.NoError(h.t, h.actor.sm.provisionCompleted(h.ctx, success))
	require.Equal(h.t, backend.ProvisionStatusReady, h.actor.sm.State())
	h.runtime = h.provisionRuntime
}

func (h *budgetHarness) provisionReady() {
	h.t.Helper()
	h.startProvision()
	h.completeProvision()
}

// die delivers one container death to the Ready lease and completes
// Failing -> Failed with the diagnostics worker's real terminal message.
func (h *budgetHarness) die(mint func(id string) failurecause.Provenance, inspection InstanceState) {
	h.t.Helper()
	current, ok := h.store.Get(testActorLeaseUUID)
	require.True(h.t, ok)
	require.NotEmpty(h.t, current.ContainerIDs)
	h.inspection.Store(&inspection)
	containerID := current.ContainerIDs[0]
	require.NoError(h.t, h.actor.sm.containerDied(h.ctx, containerID, h.runtime, mint(containerID)))
	require.Equal(h.t, backend.ProvisionStatusFailing, h.actor.sm.State())
	h.handleNextTerminal()
	require.Equal(h.t, backend.ProvisionStatusFailed, h.actor.sm.State())
}

// crash is the tenant workload exiting on its own: an observed run, exit 1.
func (h *budgetHarness) crash() {
	h.t.Helper()
	h.die(observedRun, exitedWith(1))
}

func (h *budgetHarness) divergeCohort() {
	h.t.Helper()
	require.NoError(h.t, h.actor.sm.cohortDiverged(h.ctx, h.runtime))
	require.Equal(h.t, backend.ProvisionStatusFailed, h.actor.sm.State())
}

// refuseProvision is a pre-effect provision refusal (image admission or pull).
func (h *budgetHarness) refuseProvision() {
	h.t.Helper()
	h.startProvision()
	_, failure := newTestProvisionFailure(h.t, testActorLeaseUUID)
	require.NoError(h.t, h.actor.sm.provisionErrored(h.ctx, provisionErrorInfo{
		callbackErr: backend.MsgImagePullFailed, reason: backend.ReasonImagePullFailed,
		lastError: "image admission refused", operationFailure: failure,
	}))
	require.Equal(h.t, backend.ProvisionStatusFailed, h.actor.sm.State())
}

// maintain runs one accepted tenant restart or update to the given worker
// outcome and returns its claim.
func (h *budgetHarness) maintain(
	kind shared.MaintenanceIntentKind,
	outcome func(*testing.T, shared.MaintenanceIntentClaim) ReplaceResult,
) shared.MaintenanceIntentClaim {
	h.t.Helper()
	claim := newTestMaintenanceClaim(h.t, testActorLeaseUUID, kind)
	result := outcome(h.t, claim)
	h.maintenanceMu.Lock()
	h.maintenanceOutcome = replaceWorkTerminal{result: result}
	h.maintenanceMu.Unlock()
	ack := make(chan error, 1)
	lifetime := testMaintenanceHandoff(h.t, h.ctx)
	// The worker outcome is bound to the target the fixture executed; the
	// command carries that exact intent, as production routing does.
	target := testMaintenanceTarget(h.t, claim)
	intent := target.Intent()
	if kind == shared.MaintenanceIntentUpdate {
		h.actor.handleUpdateRequested(updateRequestedMsg{
			Lifetime: lifetime, CallbackURL: intent.CallbackURL(), LifecycleCallbackURL: intent.LifecycleCallbackURL(),
			Maintenance: intent, Target: target, Ack: ack,
		})
	} else {
		h.actor.handleRestartRequested(restartRequestedMsg{
			Lifetime: lifetime, CallbackURL: intent.CallbackURL(), LifecycleCallbackURL: intent.LifecycleCallbackURL(),
			Maintenance: intent, Target: target, Ack: ack,
		})
	}
	require.NoError(h.t, <-ack)
	h.handleNextTerminal()
	if result.err == nil {
		// A committed update runs a new release generation.
		value, ok := maintenanceAuthorities.Load(claim.MaintenanceID())
		require.True(h.t, ok)
		proof, err := value.(testMaintenanceAuthority).releases.ProveRuntimeGeneration(testActorLeaseUUID)
		require.NoError(h.t, err)
		h.runtime = proof
	}
	return claim
}

func maintenanceSucceeds(t *testing.T, claim shared.MaintenanceIntentClaim) ReplaceResult {
	return testMaintenanceSuccess(t, claim, ReplaceSuccessProjection{})
}

func maintenanceRollsBack(t *testing.T, claim shared.MaintenanceIntentClaim) ReplaceResult {
	return testMaintenanceFailure(t, claim, errors.New("new release failed health"), true, false,
		ReplaceFailureDetails{
			CallbackErr: "update failed; rolled back", Reason: backend.ReasonUpdateFailed, LastError: "unhealthy",
		})
}

func maintenanceRollbackFails(t *testing.T, claim shared.MaintenanceIntentClaim) ReplaceResult {
	return testFailedCompensation(t, claim, false)
}

func (h *budgetHarness) handleNextTerminal() {
	h.t.Helper()
	select {
	case msg := <-h.actor.inbox:
		if ambiguous, ok := msg.(operationAmbiguousMsg); ok {
			h.t.Fatalf("worker outcome was ambiguous: %v", ambiguous.err)
		}
		h.actor.handleAcceptedMessage(msg)
	case <-time.After(time.Minute):
		h.t.Fatal("actor worker did not hand back its terminal message")
	}
}

func (h *budgetHarness) projection() ProvisionState {
	h.t.Helper()
	state, ok := h.store.Get(testActorLeaseUUID)
	require.True(h.t, ok)
	return *state
}

func (h *budgetHarness) observation() *backend.TerminalBudgetObservation {
	h.t.Helper()
	state := h.projection()
	return state.ObserveTerminalBudget()
}

func (h *budgetHarness) requireBudget(consecutive int, verdict backend.TerminalVerdict) {
	h.t.Helper()
	observation := h.observation()
	assert.Equal(h.t, consecutive, observation.ConsecutiveFailures, "consecutive failures")
	assert.Equal(h.t, verdict, observation.Verdict, "verdict")
}

// streakOfTwo leaves a Ready lease whose recorded streak is two tenant crashes
// and already older than the minimum span: the lease sat Failed for that span
// after the second crash (Failed time is not Ready time, so it never resets).
// The next counted crash therefore exhausts unless something reset the streak.
func (h *budgetHarness) streakOfTwo() {
	h.t.Helper()
	h.provisionReady()
	h.crash()
	h.provisionReady()
	h.crash()
	h.requireBudget(2, backend.TerminalVerdictRetry)
	time.Sleep(terminalBudgetMinStreakSpan)
	h.requireBudget(2, backend.TerminalVerdictRetry)
	h.provisionReady()
}

// exhaust runs a real crash loop to an exhausted verdict: each crash soon
// after Ready, with the lease Failed for half the minimum span between
// crashes, so the third lands past the span.
func (h *budgetHarness) exhaust() {
	h.t.Helper()
	for want := 1; want <= terminalBudgetThreshold; want++ {
		if want > 1 {
			time.Sleep(terminalBudgetMinStreakSpan / 2)
		}
		h.provisionReady()
		time.Sleep(time.Minute)
		h.crash()
	}
	h.requireBudget(terminalBudgetThreshold, backend.TerminalVerdictExhausted)
}

// ENG-799 AC1: the reported regression. Two updates that failed and rolled
// back, then one crash: the old lifetime FailCount reached 3 and closed the
// lease. Only the crash counts now.
func TestBudgetSequence_RecoveredUpdatesThenOneCrashDoNotClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newBudgetHarness(t)
		h.provisionReady()
		h.maintain(shared.MaintenanceIntentUpdate, maintenanceRollsBack)
		require.Equal(t, backend.ProvisionStatusReady, h.actor.sm.State())
		h.maintain(shared.MaintenanceIntentUpdate, maintenanceRollsBack)
		require.Equal(t, backend.ProvisionStatusReady, h.actor.sm.State())
		time.Sleep(time.Minute)
		h.crash()

		h.requireBudget(1, backend.TerminalVerdictRetry)
		assert.Equal(t, 3, h.projection().FailCount, "fail_count stays a lifetime diagnostic")
		assert.Equal(t, []string{"maintenance", "maintenance", "tenant_workload"}, h.metrics.recordedFailures())
	})
}

// ENG-799 AC2: successful re-provisions between deaths. A real crash loop
// (deaths shortly after each Ready) exhausts at the first counted death that is
// both the third or later and 30 minutes into the streak; a workload that
// survives the reset period between crashes never exhausts.
func TestBudgetSequence_RepeatedCrashesAfterReprovision(t *testing.T) {
	t.Run("crashing soon after each ready exhausts once the streak spans the minimum", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newBudgetHarness(t)
			h.provisionReady()
			time.Sleep(2 * time.Minute)
			h.crash()
			streakStart := time.Now()
			for want := 2; ; want++ {
				h.provisionReady()
				time.Sleep(2 * time.Minute)
				h.crash()
				if time.Since(streakStart) >= terminalBudgetMinStreakSpan {
					h.requireBudget(want, backend.TerminalVerdictExhausted)
					assert.Equal(t, 16, want, "a death every 2 minutes exhausts at the 16th, 30 minutes in")
					return
				}
				h.requireBudget(want, backend.TerminalVerdictRetry)
			}
		})
	})
	t.Run("crashing after a sustained ready never exhausts", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newBudgetHarness(t)
			for range 2 * terminalBudgetThreshold {
				h.provisionReady()
				time.Sleep(15 * time.Minute)
				h.crash()
				h.requireBudget(1, backend.TerminalVerdictRetry)
			}
		})
	})
}

// The minimum streak span, through the real state machine. An outage that
// kills the workload again and again in quick succession never closes the
// lease on its own; the verdict is decided only at a counted failure, so a
// streak that reached the threshold too young waits for its next one; and the
// sustained-Ready reset clears the streak's start with its count.
func TestBudgetSequence_MinimumStreakSpan(t *testing.T) {
	t.Run("an outage loop inside the span never exhausts", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newBudgetHarness(t)
			start := time.Now()
			for want := 1; time.Since(start) < terminalBudgetMinStreakSpan-time.Minute; want++ {
				h.provisionReady()
				time.Sleep(2 * time.Minute)
				h.crash()
				h.requireBudget(want, backend.TerminalVerdictRetry)
			}
			assert.Greater(t, h.observation().ConsecutiveFailures, 2*terminalBudgetThreshold)
		})
	})

	t.Run("the third counted failure after the span exhausts", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newBudgetHarness(t)
			h.provisionReady()
			h.crash()
			time.Sleep(10 * time.Minute) // Failed, retrying
			h.provisionReady()
			h.crash()
			time.Sleep(terminalBudgetMinStreakSpan - 10*time.Minute)
			h.provisionReady()
			h.crash()
			h.requireBudget(3, backend.TerminalVerdictExhausted)
		})
	})

	t.Run("a young streak at the threshold waits for its next counted failure", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newBudgetHarness(t)
			for want := 1; want <= terminalBudgetThreshold; want++ {
				h.provisionReady()
				time.Sleep(time.Minute)
				h.crash()
				h.requireBudget(want, backend.TerminalVerdictRetry)
			}
			time.Sleep(time.Hour) // Failed: time alone changes nothing
			h.requireBudget(terminalBudgetThreshold, backend.TerminalVerdictRetry)
			h.provisionReady()
			time.Sleep(time.Minute)
			h.crash()
			h.requireBudget(terminalBudgetThreshold+1, backend.TerminalVerdictExhausted)
		})
	})

	t.Run("an uncounted failure past the span does not exhaust a young streak", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newBudgetHarness(t)
			for range terminalBudgetThreshold {
				h.provisionReady()
				h.crash()
			}
			time.Sleep(time.Hour)
			h.provisionReady()
			h.die(signaledRun, exitedWith(137)) // operator kill: never counts
			h.requireBudget(terminalBudgetThreshold, backend.TerminalVerdictRetry)
		})
	})

	t.Run("a sustained ready resets the streak start with its count", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newBudgetHarness(t)
			h.streakOfTwo() // two crashes, more than 30 minutes old
			time.Sleep(terminalBudgetResetAfter + time.Minute)
			h.crash() // the sustained Ready reset the streak: this starts a new one
			h.requireBudget(1, backend.TerminalVerdictRetry)
			for want := 2; want <= terminalBudgetThreshold; want++ {
				h.provisionReady()
				time.Sleep(time.Minute)
				h.crash()
				h.requireBudget(want, backend.TerminalVerdictRetry)
			}
		})
	})

	t.Run("ready just short of the reset period exhausts once the streak spans the minimum", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newBudgetHarness(t)
			// A death every 9 minutes of Ready: the reset never fires, so the
			// streak keeps its start. Deaths at 9, 18, 27, 36 and 45 minutes;
			// the fifth is the first 30 minutes after the first.
			for want := 1; want <= 5; want++ {
				h.provisionReady()
				time.Sleep(terminalBudgetResetAfter - time.Minute)
				h.crash()
				verdict := backend.TerminalVerdictRetry
				if want == 5 {
					verdict = backend.TerminalVerdictExhausted
				}
				h.requireBudget(want, verdict)
			}
		})
	})

	t.Run("ready for the reset period between deaths never exhausts", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newBudgetHarness(t)
			for range 8 {
				h.provisionReady()
				time.Sleep(terminalBudgetResetAfter)
				h.crash()
				h.requireBudget(1, backend.TerminalVerdictRetry)
			}
		})
	})
}

// ENG-799 AC4: mixed causes. Only the tenant's own exits count; every other
// cause is recorded under its attribution and neither counts nor resets.
func TestBudgetSequence_MixedCausesCountOnlyTenantExits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newBudgetHarness(t)
		h.provisionReady()
		h.crash() // tenant exit 1: counts
		h.provisionReady()
		oom := exitedWith(137)
		oom.OOMKilled = true
		h.die(observedRun, oom) // OOM kill at its own limit: counts
		h.requireBudget(2, backend.TerminalVerdictRetry)

		h.provisionReady()
		h.die(signaledRun, exitedWith(137)) // operator kill
		h.provisionReady()
		h.die(observedRun, gone(PhaseAbsent)) // vanished
		h.provisionReady()
		h.die(observedRun, gone(PhaseFailed)) // removing or dead
		h.provisionReady()
		h.die(unobserved, exitedWith(1)) // found by the sweep
		h.provisionReady()
		h.die(partialRun, exitedWith(1)) // stream reconnected mid-run
		h.provisionReady()
		h.die(observedRun, InstanceState{Phase: PhaseExited}) // no exit status
		h.provisionReady()
		h.divergeCohort()
		h.refuseProvision()
		h.requireBudget(2, backend.TerminalVerdictRetry)

		time.Sleep(terminalBudgetMinStreakSpan) // Failed: the streak ages, nothing resets
		h.provisionReady()
		h.crash()
		h.requireBudget(3, backend.TerminalVerdictExhausted)
		assert.Equal(t, []string{
			"tenant_workload", "tenant_workload",
			"disruption", "disruption", "disruption", "unknown", "unknown", "unknown",
			"platform", "platform", "tenant_workload",
		}, h.metrics.recordedFailures())
	})
}

// ENG-799 AC5: a real crash loop reaches exhausted; a later failure that does
// not count returns the verdict to retry, because exhausted describes only the
// failure that made the lease Failed.
func TestBudgetSequence_ExhaustedOnlyWhileTheCountedFailureStands(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newBudgetHarness(t)
		h.exhaust()

		h.startProvision()
		h.requireBudget(3, backend.TerminalVerdictRetry)
		h.completeProvision()
		h.requireBudget(3, backend.TerminalVerdictRetry)
		h.divergeCohort()
		h.requireBudget(3, backend.TerminalVerdictRetry)
		h.refuseProvision()
		h.requireBudget(3, backend.TerminalVerdictRetry)
	})
}

// The review's P1 sequences. A streak of two, then Ready for 11 minutes, then
// a boundary event, then a crash one minute after the next Ready. The
// sustained Ready period ended the streak, whatever the boundary was, so the
// crash starts a new streak at one.
func TestBudgetSequence_SustainedReadyResetsAcrossAnyBoundary(t *testing.T) {
	boundaries := map[string]func(*budgetHarness){
		"successful update": func(h *budgetHarness) {
			h.maintain(shared.MaintenanceIntentUpdate, maintenanceSucceeds)
			require.Equal(h.t, backend.ProvisionStatusReady, h.actor.sm.State())
		},
		"cohort divergence": func(h *budgetHarness) {
			h.divergeCohort()
			h.provisionReady()
		},
		"update whose rollback fails": func(h *budgetHarness) {
			h.maintain(shared.MaintenanceIntentUpdate, maintenanceRollbackFails)
			require.Equal(h.t, backend.ProvisionStatusFailed, h.actor.sm.State())
			h.provisionReady()
		},
		"external kill": func(h *budgetHarness) {
			h.die(signaledRun, exitedWith(137))
			h.provisionReady()
		},
	}
	for name, boundary := range boundaries {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newBudgetHarness(t)
				h.streakOfTwo()
				time.Sleep(11 * time.Minute)
				boundary(h)
				time.Sleep(time.Minute)
				h.crash()
				h.requireBudget(1, backend.TerminalVerdictRetry)
			})
		})
	}
}

// Control for the P1 sequences: without the sustained Ready period, a
// boundary that does not count also does not reset, so the crash completes
// the streak. This keeps the P1 test from passing for the wrong reason.
func TestBudgetSequence_ShortReadyBoundaryKeepsTheStreak(t *testing.T) {
	boundaries := map[string]func(*budgetHarness){
		"cohort divergence": func(h *budgetHarness) {
			h.divergeCohort()
			h.provisionReady()
		},
		"external kill": func(h *budgetHarness) {
			h.die(signaledRun, exitedWith(137))
			h.provisionReady()
		},
	}
	for name, boundary := range boundaries {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newBudgetHarness(t)
				h.streakOfTwo()
				time.Sleep(time.Minute)
				boundary(h)
				time.Sleep(time.Minute)
				h.crash()
				h.requireBudget(3, backend.TerminalVerdictExhausted)
			})
		})
	}
}

// The review's P2: the reset is anchored on Ready entry, not on the start of
// the attempt, so a slow health-gated startup does not hide a crash loop. The
// slow startups also carry the streak past the minimum span: deaths at 16.5,
// 33 and 49.5 minutes.
func TestBudgetSequence_SlowStartupCrashLoopExhausts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newBudgetHarness(t)
		for want := 1; want <= terminalBudgetThreshold; want++ {
			h.startProvision()
			time.Sleep(16 * time.Minute) // health-gated startup
			h.completeProvision()
			time.Sleep(30 * time.Second)
			h.crash()
			assert.Equal(t, want, h.observation().ConsecutiveFailures)
		}
		h.requireBudget(3, backend.TerminalVerdictExhausted)
	})
}

// An accepted tenant restart or update starts a fresh streak, from Ready or
// from Failed, as `docker restart` resets Docker's RestartCount.
func TestBudgetSequence_TenantRestartOrUpdateResetsTheStreak(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{shared.MaintenanceIntentRestart, shared.MaintenanceIntentUpdate} {
		t.Run(string(kind)+" from ready", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newBudgetHarness(t)
				h.streakOfTwo()
				h.maintain(kind, maintenanceSucceeds)
				h.requireBudget(0, backend.TerminalVerdictRetry)
				h.crash()
				h.requireBudget(1, backend.TerminalVerdictRetry)
			})
		})
		t.Run(string(kind)+" from failed", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newBudgetHarness(t)
				h.exhaust()
				h.maintain(kind, maintenanceSucceeds)
				h.requireBudget(0, backend.TerminalVerdictRetry)
			})
		})
	}
}

// Provenance decides attribution; the exit status never does. A live death
// whose whole run the stream observed counts whatever its status, including
// 0, 137, 143 and an OOM kill. A signaled, vanished, swept or partially
// observed death never counts.
func TestBudgetSequence_ProvenanceDecidesAttribution(t *testing.T) {
	oom := exitedWith(137)
	oom.OOMKilled = true
	tests := []struct {
		name       string
		provenance func(id string) failurecause.Provenance
		inspection InstanceState
		want       string
	}{
		{"self exit 1", observedRun, exitedWith(1), "tenant_workload"},
		{"clean exit 0", observedRun, exitedWith(0), "tenant_workload"},
		{"self SIGKILL 137", observedRun, exitedWith(137), "tenant_workload"},
		{"self SIGTERM 143", observedRun, exitedWith(143), "tenant_workload"},
		{"OOM killed", observedRun, oom, "tenant_workload"},
		{"api kill then exit", signaledRun, exitedWith(1), "disruption"},
		{"api kill then OOM", signaledRun, oom, "disruption"},
		{"absent", observedRun, gone(PhaseAbsent), "disruption"},
		{"removing or dead", observedRun, InstanceState{Phase: PhaseFailed, ExitCode: oom.ExitCode, Termination: failurecause.Gone()}, "disruption"},
		{"found by the sweep", unobserved, exitedWith(1), "unknown"},
		{"run start not observed", partialRun, exitedWith(1), "unknown"},
		{"no exit status", observedRun, InstanceState{Phase: PhaseExited}, "unknown"},
		{"exit the adapter did not classify", observedRun, InstanceState{Phase: PhaseFailed, ExitCode: oom.ExitCode}, "unknown"},
		{"observed run of another container", observedOtherRun, exitedWith(1), "unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newBudgetHarness(t)
				h.provisionReady()
				h.die(test.provenance, test.inspection)
				assert.Equal(t, []string{test.want}, h.metrics.recordedFailures())
				wantCount := 0
				if test.want == "tenant_workload" {
					wantCount = 1
				}
				h.requireBudget(wantCount, backend.TerminalVerdictRetry)
				assert.Equal(t, 1, h.projection().FailCount, "every death stays in the lifetime diagnostic")
			})
		})
	}
}

// A live death observation takes its container from the provenance minted for
// it, so one container's observed run cannot be attached to another's death,
// and a death without live provenance cannot pose as a live one.
func TestLiveContainerDiedObservationIsBoundToItsProvenance(t *testing.T) {
	runtime := newTestRuntimeGenerationProof(t, testActorLeaseUUID)
	_, err := NewLiveContainerDiedObservation(runtime, failurecause.Provenance{})
	require.Error(t, err, "a live observation needs provenance minted for its container")
	_, err = NewLiveContainerDiedObservation(shared.RuntimeGenerationProof{}, observedRun("container-a"))
	require.Error(t, err, "and an exact runtime generation")

	observation, err := NewLiveContainerDiedObservation(runtime, observedRun("container-a"))
	require.NoError(t, err)
	died, ok := observation.envelope.message.(containerDiedMsg)
	require.True(t, ok)
	assert.Equal(t, "container-a", died.ContainerID)
	assert.Equal(t, "observed_run", died.provenance.Label())

	swept, err := NewContainerDiedObservation("container-a", runtime)
	require.NoError(t, err)
	sweptDeath, ok := swept.envelope.message.(containerDiedMsg)
	require.True(t, ok)
	assert.Equal(t, failurecause.Provenance{}, sweptDeath.provenance, "a swept death carries no provenance")
}

// Recovery re-application of a maintenance outcome must not record the
// failure twice: the entry action records it once, a retry only re-asserts it.
func TestMaintenanceRecoveredFailureRecordsOnce(t *testing.T) {
	claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentUpdate)
	active := activeMaintenanceForRecovery(t, claim)
	actor, store, _ := maintenanceRecoveryActor(t, backend.ProvisionStatusUpdating, claim)
	metrics := &countingMetrics{}
	actor.cfg.Metrics = metrics
	store.UpdateFn(testActorLeaseUUID, func(state *ProvisionState) {
		state.TerminalBudget = TerminalBudget{
			leaseUUID: testActorLeaseUUID, consecutive: 2, standing: countedWithinBudget,
		}
	})
	projection := MaintenanceRecoveryProjection{
		ContainerIDs:      []string{"target-1"},
		ServiceContainers: map[string][]string{"app": {"target-1"}},
	}
	for range 2 {
		msg, reply, err := NewMaintenanceRecoveredRuntimeFailureMsg(active, projection)
		require.NoError(t, err)
		handleRecoveryCommand(t, actor, msg)
		require.NoError(t, reply.Wait(t.Context()))
	}
	assert.Equal(t, []string{"platform"}, metrics.recordedFailures())
	state, ok := store.Get(testActorLeaseUUID)
	require.True(t, ok)
	assert.Equal(t, backend.TerminalVerdictRetry, state.ObserveTerminalBudget().Verdict)
	assert.Equal(t, 2, state.ObserveTerminalBudget().ConsecutiveFailures, "an uncounted failure keeps the streak")
}
