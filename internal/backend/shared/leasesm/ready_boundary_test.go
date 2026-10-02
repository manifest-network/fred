package leasesm

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// The terminal budget's Ready boundary (ENG-799) is applied by SetStatus, the
// only writer of a projection's status, so it holds for any transition by
// construction. These tests pin the other half: every transition the state
// machine permits into or out of Ready is listed, and driving each one moves
// the projection across the boundary. A new Permit into or out of Ready fails
// TestReadyBoundaryTransitionsArePinned until it has a driver here.

// stateMachineEdge matches one transition line of stateless's DOT graph.
var stateMachineEdge = regexp.MustCompile(`^\t("?[^" ]+"?) -> ("?[^" ]+"?) \[label="(.*)"\];$`)

// readyBoundaryTransitions lists every transition into or out of Ready in the
// state machine's own graph, as "source -> destination on Trigger".
func readyBoundaryTransitions(graph string) []string {
	var transitions []string
	for _, line := range strings.Split(graph, "\n") {
		match := stateMachineEdge.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		source, destination := strings.Trim(match[1], `"`), strings.Trim(match[2], `"`)
		ready := string(backend.ProvisionStatusReady)
		if (source == ready) == (destination == ready) {
			continue
		}
		for _, entry := range strings.Split(match[3], `\l`) {
			if entry == "" {
				continue
			}
			trigger, _, _ := strings.Cut(entry, " ")
			transitions = append(transitions, fmt.Sprintf("%s -> %s on %s", source, destination, trigger))
		}
	}
	slices.Sort(transitions)
	return transitions
}

// readyBoundaryCase drives one transition at the current fake time and
// returns the projection after it. Out-of-Ready drivers start from a streak of
// two that has been Ready for eleven minutes; into-Ready drivers start from
// the transition's source state.
type readyBoundaryCase struct {
	into  bool
	drive func(t *testing.T) ProvisionState
}

func outOfReady(fire func(h *budgetHarness)) readyBoundaryCase {
	return readyBoundaryCase{drive: func(t *testing.T) ProvisionState {
		h := newBudgetHarness(t)
		h.streakOfTwo()
		time.Sleep(11 * time.Minute)
		fire(h)
		return h.projection()
	}}
}

func intoReadyFromHarness(fire func(h *budgetHarness)) readyBoundaryCase {
	return readyBoundaryCase{into: true, drive: func(t *testing.T) ProvisionState {
		h := newBudgetHarness(t)
		h.provisionReady()
		h.crash()
		time.Sleep(time.Minute)
		fire(h)
		return h.projection()
	}}
}

// intoReadyByRecovery converges a pending maintenance from source to Ready
// through the recovery-only triggers, on the maintenance recovery fixture.
func intoReadyByRecovery(source backend.ProvisionStatus, success bool) readyBoundaryCase {
	return readyBoundaryCase{into: true, drive: func(t *testing.T) ProvisionState {
		kind := shared.MaintenanceIntentUpdate
		if source == backend.ProvisionStatusRestarting {
			kind = shared.MaintenanceIntentRestart
		}
		claim := newTestMaintenanceClaim(t, testActorLeaseUUID, kind)
		actor, store, _ := maintenanceRecoveryActor(t, source, claim)
		var command RecoveryCommand
		var reply ActorReply
		var err error
		if success {
			command, reply, err = NewMaintenanceRecoveredSuccessMsg(
				activeMaintenanceForRecovery(t, claim), targetMaintenanceRecoveryProjection(claim),
			)
		} else {
			command, reply, err = NewMaintenanceRecoveredFailureReadyMsg(claim, MaintenanceRecoveryProjection{},
				maintenanceRecoveryFailureInfo(t, claim, ReplaceFailureDetails{
					CallbackErr: "interrupted", LastError: "interrupted",
				}))
		}
		require.NoError(t, err)
		handleRecoveryCommand(t, actor, command)
		require.NoError(t, reply.Wait(t.Context()))
		state, ok := store.Get(testActorLeaseUUID)
		require.True(t, ok)
		return *state
	}}
}

func recoveredFailureFromReady(h *budgetHarness) {
	h.t.Helper()
	claim := newTestMaintenanceClaim(h.t, testActorLeaseUUID, shared.MaintenanceIntentUpdate)
	command, reply, err := NewMaintenanceRecoveredFailureFailedMsg(claim, MaintenanceRecoveryProjection{},
		maintenanceRecoveryFailureInfo(h.t, claim, ReplaceFailureDetails{
			CallbackErr: "interrupted", LastError: "interrupted",
		}))
	require.NoError(h.t, err)
	handleRecoveryCommand(h.t, h.actor, command)
	require.NoError(h.t, reply.Wait(h.t.Context()))
	require.Equal(h.t, backend.ProvisionStatusFailed, h.actor.sm.State())
}

func recoveredRuntimeFailureFromReady(h *budgetHarness) {
	h.t.Helper()
	claim := newTestMaintenanceClaim(h.t, testActorLeaseUUID, shared.MaintenanceIntentUpdate)
	command, reply, err := NewMaintenanceRecoveredRuntimeFailureMsg(
		activeMaintenanceForRecovery(h.t, claim), targetMaintenanceRecoveryProjection(claim),
	)
	require.NoError(h.t, err)
	handleRecoveryCommand(h.t, h.actor, command)
	require.NoError(h.t, reply.Wait(h.t.Context()))
	require.Equal(h.t, backend.ProvisionStatusFailed, h.actor.sm.State())
}

var readyBoundaryCases = map[string]readyBoundaryCase{
	// Out of Ready: the eleven-minute Ready period resets the streak of two.
	"ready -> failing on ContainerDied": outOfReady(func(h *budgetHarness) { h.crash() }),
	"ready -> failed on CohortDiverged": outOfReady(func(h *budgetHarness) { h.divergeCohort() }),
	"ready -> restarting on RestartRequested": outOfReady(func(h *budgetHarness) {
		// No maintenance claim, so the tenant reset cannot mask the boundary.
		require.NoError(h.t, h.actor.sm.requestRestart(h.ctx, replaceEntryArgs{CallbackKind: replaceCallbackLifecycle}))
	}),
	"ready -> updating on UpdateRequested": outOfReady(func(h *budgetHarness) {
		require.NoError(h.t, h.actor.sm.requestUpdate(h.ctx, replaceEntryArgs{CallbackKind: replaceCallbackLifecycle}))
	}),
	"ready -> deprovisioning on DeprovisionRequested": outOfReady(func(h *budgetHarness) {
		require.NoError(h.t, h.actor.sm.requestDeprovision(h.ctx))
		// Deprovisioning has no entry action: the substrate's close writes the
		// status (docker: doClosePhysical), through SetStatus like every writer.
		now := time.Now()
		h.store.UpdateFn(testActorLeaseUUID, func(p *ProvisionState) {
			p.SetStatus(backend.ProvisionStatusDeprovisioning, now)
		})
	}),
	"ready -> failed on MaintenanceRecoveredFailureFailed":        outOfReady(recoveredFailureFromReady),
	"ready -> failed on MaintenanceRecoveredSuccessRuntimeFailed": outOfReady(recoveredRuntimeFailureFromReady),
	"provisioning -> ready on ProvisionCompleted":                 intoReadyFromHarness(func(h *budgetHarness) { h.provisionReady() }),
	"restarting -> ready on ReplaceCompleted":                     intoReadyFromHarness(maintainedBy(shared.MaintenanceIntentRestart, maintenanceSucceeds)),
	"restarting -> ready on ReplaceRecovered":                     intoReadyFromHarness(maintainedBy(shared.MaintenanceIntentRestart, maintenanceRollsBack)),
	"updating -> ready on ReplaceCompleted":                       intoReadyFromHarness(maintainedBy(shared.MaintenanceIntentUpdate, maintenanceSucceeds)),
	"updating -> ready on ReplaceRecovered":                       intoReadyFromHarness(maintainedBy(shared.MaintenanceIntentUpdate, maintenanceRollsBack)),
	"restarting -> ready on MaintenanceRecoveredSuccess":          intoReadyByRecovery(backend.ProvisionStatusRestarting, true),
	"updating -> ready on MaintenanceRecoveredSuccess":            intoReadyByRecovery(backend.ProvisionStatusUpdating, true),
	"failed -> ready on MaintenanceRecoveredSuccess":              intoReadyByRecovery(backend.ProvisionStatusFailed, true),
	"restarting -> ready on MaintenanceRecoveredFailureReady":     intoReadyByRecovery(backend.ProvisionStatusRestarting, false),
	"updating -> ready on MaintenanceRecoveredFailureReady":       intoReadyByRecovery(backend.ProvisionStatusUpdating, false),
	"failed -> ready on MaintenanceRecoveredFailureReady":         intoReadyByRecovery(backend.ProvisionStatusFailed, false),
}

func maintainedBy(
	kind shared.MaintenanceIntentKind,
	outcome func(*testing.T, shared.MaintenanceIntentClaim) ReplaceResult,
) func(*budgetHarness) {
	return func(h *budgetHarness) {
		// From Failed, a re-provision first; the maintenance runs from Ready a
		// minute later, so its own Ready entry must move the anchor.
		h.provisionReady()
		time.Sleep(time.Minute)
		h.maintain(kind, outcome)
		require.Equal(h.t, backend.ProvisionStatusReady, h.actor.sm.State())
	}
}

// The graph reader must see guarded, multi-trigger and quoted edges and skip
// self-edges, or the pin below could pass vacuously.
func TestReadyBoundaryGraphReaderSeesEveryEdgeShape(t *testing.T) {
	const graph = "digraph {\n" +
		"\tready -> failing [label=\"ContainerDied [guard]\"];\n" +
		`	provisioning -> ready [label="ProvisionCompleted / onEnter\lOther\l"];` + "\n" +
		"\t\"ready\" -> \"1\" [label=\"Quoted\"];\n" +
		"\tready -> ready [label=\"🚫 Ignored\"];\n" +
		"\tfailed -> failing [label=\"Elsewhere\"];\n" +
		"}\n"
	assert.Equal(t, []string{
		"provisioning -> ready on Other",
		"provisioning -> ready on ProvisionCompleted",
		"ready -> 1 on Quoted",
		"ready -> failing on ContainerDied",
	}, readyBoundaryTransitions(graph))
}

func TestReadyBoundaryTransitionsArePinned(t *testing.T) {
	actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{ProvisionStore: newMockProvisionStore()})
	graphed := readyBoundaryTransitions(actor.sm.sm.ToGraph())
	require.NotEmpty(t, graphed, "the graph parser must see the Ready transitions")
	assert.Equal(t, slices.Sorted(maps.Keys(readyBoundaryCases)), graphed,
		"every transition into or out of Ready needs a driver in readyBoundaryCases")
}

func TestReadyBoundaryAppliedOnEveryTransition(t *testing.T) {
	for name, test := range readyBoundaryCases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				state := test.drive(t)
				budget := state.TerminalBudget
				assert.False(t, budget.lastFailureCounted && state.Status != backend.ProvisionStatusFailing &&
					state.Status != backend.ProvisionStatusFailed, "a counted failure outlived its failure")
				if test.into {
					require.Equal(t, backend.ProvisionStatusReady, state.Status)
					assert.True(t, budget.readySince.Equal(time.Now()),
						"entering Ready anchors the period now, got %v", budget.readySince)
					return
				}
				require.NotEqual(t, backend.ProvisionStatusReady, state.Status)
				assert.True(t, budget.readySince.IsZero(), "leaving Ready clears the anchor")
				want := 0
				if name == "ready -> failing on ContainerDied" {
					want = 1 // reset first, then the death itself counts
				}
				assert.Equal(t, want, budget.consecutive, "leaving a sustained Ready resets the streak")
			})
		})
	}
}

// A custom-domain redeploy is started by the platform's reconciler. It shares
// the lifecycle callback with a tenant restart, but only the tenant's own
// restart or update resets the streak.
func TestBudgetSequence_CustomDomainRedeployDoesNotReset(t *testing.T) {
	for kind, want := range map[shared.MaintenanceIntentKind]int{
		shared.MaintenanceIntentCustomDomain: 3,
		shared.MaintenanceIntentRestart:      1,
	} {
		t.Run(string(kind), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newBudgetHarness(t)
				h.streakOfTwo()
				time.Sleep(time.Minute)
				h.maintain(kind, maintenanceSucceeds)
				require.Equal(t, backend.ProvisionStatusReady, h.actor.sm.State())
				time.Sleep(time.Minute)
				h.crash()
				verdict := backend.TerminalVerdictRetry
				if want == terminalBudgetThreshold {
					verdict = backend.TerminalVerdictExhausted
				}
				h.requireBudget(want, verdict)
			})
		})
	}
}

// A recovered maintenance runtime failure leaves Ready like any other exit:
// after a sustained Ready it ends the streak, so a crash one minute after the
// next Ready starts a new one. Short of the reset period the streak stands.
func TestBudgetSequence_RecoveredRuntimeFailureIsAReadyExit(t *testing.T) {
	for ready, want := range map[time.Duration]int{11 * time.Minute: 1, time.Minute: 3} {
		t.Run(ready.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newBudgetHarness(t)
				h.streakOfTwo()
				time.Sleep(ready)
				recoveredRuntimeFailureFromReady(h)
				h.provisionReady()
				time.Sleep(time.Minute)
				h.crash()
				assert.Equal(t, want, h.observation().ConsecutiveFailures)
			})
		})
	}
}

// An off-actor failure (a maintenance outcome converged without an actor)
// ends the Ready period like any other exit. It used to leave the old anchor
// in place, so the next Ready inherited it and reset the streak early.
func TestTerminalBudget_OffActorFailureEndsTheReadyPeriod(t *testing.T) {
	t0 := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	p := readyAt(t0, 2)
	p.SetStatus(backend.ProvisionStatusFailed, t0.Add(5*time.Minute)) // off-actor, uncounted
	p.SetStatus(backend.ProvisionStatusProvisioning, t0.Add(20*time.Minute))
	p.SetStatus(backend.ProvisionStatusReady, t0.Add(20*time.Minute))
	outcome := p.recordFailure(backend.ProvisionStatusFailing, tenantCause(), t0.Add(21*time.Minute))
	assert.Equal(t, 3, outcome.consecutive, "one minute of Ready must not inherit the earlier period")
	assert.True(t, outcome.exhausted)
}
