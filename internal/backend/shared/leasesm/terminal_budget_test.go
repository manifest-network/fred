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
	"slices"
	"strconv"
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

func observedRun() failurecause.Provenance {
	session := failurecause.NewEventSession()
	session.ObserveStart("run")
	return session.ObserveExit("run")
}

func signaledRun() failurecause.Provenance {
	session := failurecause.NewEventSession()
	session.ObserveStart("run")
	session.ObserveSignal("run")
	return session.ObserveExit("run")
}

func partialRun() failurecause.Provenance {
	return failurecause.NewEventSession().ObserveExit("run")
}

func exitedWith(code int) InstanceState {
	return InstanceState{Phase: PhaseExited, ExitCode: &code}
}

func tenantCause() failurecause.Cause {
	return failurecause.ClassifyDeath(observedRun(), failurecause.Exited())
}

// --- unit tests: the budget rules ---------------------------------------------

func TestTerminalBudget_ConsecutiveCountAndSustainedReadyReset(t *testing.T) {
	t0 := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	newProjection := func() *ProvisionState {
		return &ProvisionState{LeaseUUID: testActorLeaseUUID, Reason: backend.ReasonContainerExited}
	}

	t.Run("three counted failures inside the window exhaust", func(t *testing.T) {
		p := newProjection()
		now := t0
		for want := 1; want <= terminalBudgetThreshold; want++ {
			p.budgetEnterProvisioning()
			p.budgetEnterReady(now)
			now = now.Add(time.Minute)
			outcome := p.budgetRecordFailure(tenantCause(), now)
			require.True(t, outcome.counted)
			assert.Equal(t, want, outcome.consecutive)
			assert.Equal(t, want >= terminalBudgetThreshold, outcome.exhausted)
		}
	})

	t.Run("a failure after the reset period starts a new streak", func(t *testing.T) {
		for _, ready := range []time.Duration{terminalBudgetResetAfter, terminalBudgetResetAfter + time.Hour} {
			p := newProjection()
			p.TerminalBudget = TerminalBudget{leaseUUID: testActorLeaseUUID, consecutive: 2}
			p.budgetEnterReady(t0)
			outcome := p.budgetRecordFailure(tenantCause(), t0.Add(ready))
			assert.Equal(t, 1, outcome.consecutive, "Ready for %s must reset the streak", ready)
		}
		p := newProjection()
		p.TerminalBudget = TerminalBudget{leaseUUID: testActorLeaseUUID, consecutive: 2}
		p.budgetEnterReady(t0)
		outcome := p.budgetRecordFailure(tenantCause(), t0.Add(terminalBudgetResetAfter-time.Nanosecond))
		assert.Equal(t, 3, outcome.consecutive, "just short of the reset period the streak continues")
		assert.True(t, outcome.exhausted)
	})

	t.Run("a failure while not Ready never resets", func(t *testing.T) {
		p := newProjection()
		p.TerminalBudget = TerminalBudget{leaseUUID: testActorLeaseUUID, consecutive: 2}
		outcome := p.budgetRecordFailure(tenantCause(), t0.Add(24*time.Hour))
		assert.Equal(t, 3, outcome.consecutive)
	})

	t.Run("an uncounted failure neither increments nor resets", func(t *testing.T) {
		for _, cause := range []failurecause.Cause{
			{}, failurecause.Platform(), failurecause.Maintenance(),
			failurecause.ClassifyDeath(signaledRun(), failurecause.Exited()),
		} {
			p := newProjection()
			p.TerminalBudget = TerminalBudget{
				leaseUUID: testActorLeaseUUID, consecutive: 2, lastFailureCounted: true,
			}
			p.budgetEnterReady(t0)
			outcome := p.budgetRecordFailure(cause, t0.Add(time.Minute))
			assert.False(t, outcome.counted, cause.Label())
			assert.Equal(t, 2, p.TerminalBudget.consecutive, cause.Label())
			assert.False(t, p.TerminalBudget.lastFailureCounted, cause.Label())
			assert.True(t, p.TerminalBudget.readySince.IsZero(), "the failure ends the Ready period")
		}
	})

	t.Run("an ineligible reason never counts", func(t *testing.T) {
		for _, reason := range []backend.Reason{"", backend.ReasonInternal, backend.ReasonUnknown, "Bogus"} {
			p := newProjection()
			p.Reason = reason
			outcome := p.budgetRecordFailure(tenantCause(), t0)
			assert.False(t, outcome.counted, "reason %q", reason)
			assert.Zero(t, p.TerminalBudget.consecutive)
		}
	})

	t.Run("a budget carried onto another lease is fresh", func(t *testing.T) {
		p := newProjection()
		p.TerminalBudget = TerminalBudget{
			leaseUUID: "22222222-2222-4222-8222-222222222222", consecutive: 2, lastFailureCounted: true,
		}
		assert.Equal(t, 0, p.ObserveTerminalBudget().ConsecutiveFailures)
		outcome := p.budgetRecordFailure(tenantCause(), t0)
		assert.Equal(t, 1, outcome.consecutive, "a foreign streak never contributes")
		assert.Equal(t, testActorLeaseUUID, p.TerminalBudget.leaseUUID)
	})

	t.Run("a tenant reset clears the streak", func(t *testing.T) {
		p := newProjection()
		p.TerminalBudget = TerminalBudget{
			leaseUUID: testActorLeaseUUID, consecutive: 2, lastFailureCounted: true,
		}
		p.budgetResetByTenant()
		assert.Zero(t, p.TerminalBudget.consecutive)
		assert.False(t, p.TerminalBudget.lastFailureCounted)
	})

	t.Run("provisioning and ready entries clear the counted failure, not the streak", func(t *testing.T) {
		p := newProjection()
		p.TerminalBudget = TerminalBudget{
			leaseUUID: testActorLeaseUUID, consecutive: 2, lastFailureCounted: true,
		}
		p.budgetEnterProvisioning()
		assert.Equal(t, 2, p.TerminalBudget.consecutive)
		assert.False(t, p.TerminalBudget.lastFailureCounted)
		p.TerminalBudget.lastFailureCounted = true
		p.budgetEnterReady(t0)
		assert.Equal(t, 2, p.TerminalBudget.consecutive)
		assert.False(t, p.TerminalBudget.lastFailureCounted)
		p.budgetEnterReady(t0.Add(time.Hour))
		assert.Equal(t, t0, p.TerminalBudget.readySince, "an existing Ready anchor is kept")
	})
}

func TestTerminalBudget_ObserveVerdictMatrix(t *testing.T) {
	statuses := []backend.ProvisionStatus{
		backend.ProvisionStatusProvisioning, backend.ProvisionStatusReady, backend.ProvisionStatusFailing,
		backend.ProvisionStatusFailed, backend.ProvisionStatusRestarting, backend.ProvisionStatusUpdating,
		backend.ProvisionStatusDeprovisioning, backend.ProvisionStatusRetained, backend.ProvisionStatusUnknown, "bogus",
	}
	reasons := []backend.Reason{backend.ReasonContainerExited, backend.ReasonInternal, backend.ReasonUnknown, "", "Bogus"}
	bindings := map[string]string{
		"same": testActorLeaseUUID, "other": "22222222-2222-4222-8222-222222222222", "unbound": "",
	}
	for _, status := range statuses {
		for _, reason := range reasons {
			for bindingName, binding := range bindings {
				for consecutive := range terminalBudgetThreshold + 2 {
					for _, counted := range []bool{false, true} {
						p := ProvisionState{
							LeaseUUID: testActorLeaseUUID, Status: status, Reason: reason,
							TerminalBudget: TerminalBudget{
								leaseUUID: binding, consecutive: consecutive, lastFailureCounted: counted,
							},
						}
						got := p.ObserveTerminalBudget()
						require.NotNil(t, got)
						wantExhausted := status == backend.ProvisionStatusFailed && counted &&
							consecutive >= terminalBudgetThreshold && reason == backend.ReasonContainerExited &&
							bindingName == "same"
						name := fmt.Sprintf("%s/%s/%s/%d/%t", status, reason, bindingName, consecutive, counted)
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

func TestTerminalBudget_ObservationIgnoresElapsedTime(t *testing.T) {
	t0 := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	p := ProvisionState{LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusReady}
	p.TerminalBudget = TerminalBudget{leaseUUID: testActorLeaseUUID, consecutive: 2, readySince: t0}
	first := p.ObserveTerminalBudget()
	p.TerminalBudget.readySince = t0.Add(-365 * 24 * time.Hour)
	assert.Equal(t, first, p.ObserveTerminalBudget(),
		"the observation is the recorded count: time alone never changes it")
}

// The exported helpers are the only budget mutators a substrate can reach.
// Whatever the starting budget, they can only move it toward a reset: never
// a higher count, never a counted failure, never an exhausted verdict, never
// a later Ready anchor.
func TestTerminalBudget_OffActorHelpersOnlyMoveTowardReset(t *testing.T) {
	t0 := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	anchors := []time.Time{{}, t0.Add(-time.Hour), t0.Add(-time.Minute), t0}
	helpers := map[string]func(*ProvisionState, time.Time){
		"ready":            (*ProvisionState).ObserveReadyProjection,
		"uncountedFailure": (*ProvisionState).ObserveUncountedFailureProjection,
	}
	for name, apply := range helpers {
		for _, binding := range []string{testActorLeaseUUID, "22222222-2222-4222-8222-222222222222"} {
			for consecutive := range terminalBudgetThreshold + 2 {
				for _, counted := range []bool{false, true} {
					for _, anchor := range anchors {
						for _, status := range []backend.ProvisionStatus{backend.ProvisionStatusFailed, backend.ProvisionStatusReady} {
							before := TerminalBudget{
								leaseUUID: binding, consecutive: consecutive, readySince: anchor, lastFailureCounted: counted,
							}
							p := &ProvisionState{
								LeaseUUID: testActorLeaseUUID, Status: status,
								Reason: backend.ReasonContainerExited, TerminalBudget: before,
							}
							apply(p, t0)
							after := p.TerminalBudget
							label := fmt.Sprintf("%s %+v", name, before)
							if binding == testActorLeaseUUID {
								assert.LessOrEqual(t, after.consecutive, before.consecutive, label)
								if !before.readySince.IsZero() {
									assert.False(t, after.readySince.After(before.readySince), label)
								}
							} else {
								assert.Zero(t, after.consecutive, label)
							}
							assert.False(t, after.lastFailureCounted, label)
							assert.Equal(t, backend.TerminalVerdictRetry, p.ObserveTerminalBudget().Verdict, label)
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
		"ReasonContainerExited":        true,
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
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join("..", "..", "reason.go"), nil, parser.SkipObjectResolution)
	require.NoError(t, err)
	declared := make(map[string]backend.Reason)
	for _, decl := range file.Decls {
		general, ok := decl.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			value := spec.(*ast.ValueSpec)
			typeIdent, typed := value.Type.(*ast.Ident)
			if !typed || typeIdent.Name != "Reason" {
				continue
			}
			for index, name := range value.Names {
				literal, ok := value.Values[index].(*ast.BasicLit)
				require.True(t, ok, "%s must be a literal", name.Name)
				text, err := strconv.Unquote(literal.Value)
				require.NoError(t, err)
				declared[name.Name] = backend.Reason(text)
			}
		}
	}
	require.NotEmpty(t, declared)
	require.Equal(t, slices.Sorted(maps.Keys(decisions)), slices.Sorted(maps.Keys(declared)),
		"decide budget eligibility for every declared Reason")
	for name, reason := range declared {
		assert.Equal(t, decisions[name], reasonEligibleForBudget(reason), name)
	}
	assert.False(t, reasonEligibleForBudget(""))
	assert.False(t, reasonEligibleForBudget("Bogus"))
}

// TestEveryFailCountIncrementRecordsBudgetFailure pins the choke point: every
// store closure in the state machine that increments the lifetime FailCount
// must also record the failure in the terminal budget, so no failure path can
// skip attribution.
func TestEveryFailCountIncrementRecordsBudgetFailure(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "lease_sm.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err)
	increments, violations := failCountClosureViolations(fset, file)
	assert.GreaterOrEqual(t, increments, 5, "the guard must see every FailCount increment site")
	assert.Empty(t, violations)
}

func TestFailCountGuardFires(t *testing.T) {
	const src = `package leasesm
func a(store LeaseProvisionStore) {
	store.UpdateFn("l", func(p *ProvisionState) { p.FailCount++ })
	store.UpdateFn("l", func(p *ProvisionState) { p.FailCount++; _ = p.budgetRecordFailure(c, n) })
}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "synthetic.go", src, parser.SkipObjectResolution)
	require.NoError(t, err)
	increments, violations := failCountClosureViolations(fset, file)
	assert.Equal(t, 2, increments)
	require.Len(t, violations, 1)
	assert.Contains(t, violations[0], "synthetic.go:3")
}

func failCountClosureViolations(fset *token.FileSet, file *ast.File) (int, []string) {
	increments := 0
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "UpdateFn" || len(call.Args) != 2 {
			return true
		}
		closure, ok := call.Args[1].(*ast.FuncLit)
		if !ok {
			return true
		}
		incrementsFailCount, records := false, false
		ast.Inspect(closure.Body, func(inner ast.Node) bool {
			switch typed := inner.(type) {
			case *ast.IncDecStmt:
				if target, ok := typed.X.(*ast.SelectorExpr); ok && target.Sel.Name == "FailCount" {
					incrementsFailCount = true
				}
			case *ast.CallExpr:
				if target, ok := typed.Fun.(*ast.SelectorExpr); ok && target.Sel.Name == "budgetRecordFailure" {
					records = true
				}
			}
			return true
		})
		if incrementsFailCount {
			increments++
			if !records {
				violations = append(violations, fset.Position(call.Pos()).String()+
					": closure increments FailCount without recording the budget failure")
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
	require.NoError(h.t, h.actor.sm.requestProvision(h.ctx))
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
func (h *budgetHarness) die(provenance failurecause.Provenance, inspection InstanceState) {
	h.t.Helper()
	current, ok := h.store.Get(testActorLeaseUUID)
	require.True(h.t, ok)
	require.NotEmpty(h.t, current.ContainerIDs)
	h.inspection.Store(&inspection)
	require.NoError(h.t, h.actor.sm.containerDied(h.ctx, current.ContainerIDs[0], h.runtime, provenance))
	require.Equal(h.t, backend.ProvisionStatusFailing, h.actor.sm.State())
	h.handleNextTerminal()
	require.Equal(h.t, backend.ProvisionStatusFailed, h.actor.sm.State())
}

// crash is the tenant workload exiting on its own: an observed run, exit 1.
func (h *budgetHarness) crash() {
	h.t.Helper()
	h.die(observedRun(), exitedWith(1))
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

// streakOfTwo leaves a Ready lease whose recorded streak is two tenant crashes.
func (h *budgetHarness) streakOfTwo() {
	h.t.Helper()
	h.provisionReady()
	h.crash()
	h.provisionReady()
	h.crash()
	h.requireBudget(2, backend.TerminalVerdictRetry)
	h.provisionReady()
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
// (deaths shortly after each Ready) exhausts at the third death; a workload
// that survives the reset period between crashes never exhausts.
func TestBudgetSequence_RepeatedCrashesAfterReprovision(t *testing.T) {
	t.Run("crashing soon after each ready exhausts at the third", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newBudgetHarness(t)
			for want := 1; want <= terminalBudgetThreshold; want++ {
				h.provisionReady()
				time.Sleep(2 * time.Minute)
				h.crash()
				verdict := backend.TerminalVerdictRetry
				if want == terminalBudgetThreshold {
					verdict = backend.TerminalVerdictExhausted
				}
				h.requireBudget(want, verdict)
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
		h.die(observedRun(), oom) // OOM kill at its own limit: counts
		h.requireBudget(2, backend.TerminalVerdictRetry)

		h.provisionReady()
		h.die(signaledRun(), exitedWith(137)) // operator kill
		h.provisionReady()
		h.die(observedRun(), InstanceState{Phase: PhaseAbsent}) // vanished
		h.provisionReady()
		h.die(observedRun(), InstanceState{Phase: PhaseFailed}) // removing or dead
		h.provisionReady()
		h.die(failurecause.Provenance{}, exitedWith(1)) // found by the sweep
		h.provisionReady()
		h.die(partialRun(), exitedWith(1)) // stream reconnected mid-run
		h.provisionReady()
		h.die(observedRun(), InstanceState{Phase: PhaseExited}) // no exit status
		h.provisionReady()
		h.divergeCohort()
		h.refuseProvision()
		h.requireBudget(2, backend.TerminalVerdictRetry)

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
		for range terminalBudgetThreshold {
			h.provisionReady()
			h.crash()
		}
		h.requireBudget(3, backend.TerminalVerdictExhausted)

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
			h.die(signaledRun(), exitedWith(137))
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
			h.die(signaledRun(), exitedWith(137))
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
// the attempt, so a slow health-gated startup does not hide a crash loop.
func TestBudgetSequence_SlowStartupCrashLoopExhausts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newBudgetHarness(t)
		for want := 1; want <= terminalBudgetThreshold; want++ {
			h.startProvision()
			time.Sleep(12 * time.Minute) // health-gated startup
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
				for range terminalBudgetThreshold {
					h.provisionReady()
					h.crash()
				}
				h.requireBudget(3, backend.TerminalVerdictExhausted)
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
		provenance failurecause.Provenance
		inspection InstanceState
		want       string
	}{
		{"self exit 1", observedRun(), exitedWith(1), "tenant_workload"},
		{"clean exit 0", observedRun(), exitedWith(0), "tenant_workload"},
		{"self SIGKILL 137", observedRun(), exitedWith(137), "tenant_workload"},
		{"self SIGTERM 143", observedRun(), exitedWith(143), "tenant_workload"},
		{"OOM killed", observedRun(), oom, "tenant_workload"},
		{"api kill then exit", signaledRun(), exitedWith(1), "disruption"},
		{"api kill then OOM", signaledRun(), oom, "disruption"},
		{"absent", observedRun(), InstanceState{Phase: PhaseAbsent}, "disruption"},
		{"removing or dead", observedRun(), InstanceState{Phase: PhaseFailed, ExitCode: oom.ExitCode}, "disruption"},
		{"found by the sweep", failurecause.Provenance{}, exitedWith(1), "unknown"},
		{"run start not observed", partialRun(), exitedWith(1), "unknown"},
		{"no exit status", observedRun(), InstanceState{Phase: PhaseExited}, "unknown"},
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
			leaseUUID: testActorLeaseUUID, consecutive: 2, lastFailureCounted: true,
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
