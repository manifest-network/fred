package leasesm

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
)

const startupContainer = "startup-container"

// startupFailure enters Provisioning and completes it with a definite startup
// failure carrying terms, as a provision worker's sealed outcome does.
func (h *budgetHarness) startupFailure(terms shared.OperationStartupFailureTerms) {
	h.t.Helper()
	h.startProvision()
	startup, err := shared.NewOperationStartupFailure(terms)
	require.NoError(h.t, err)
	_, failure := newTestProvisionFailure(h.t, testActorLeaseUUID)
	require.NoError(h.t, h.actor.sm.provisionErrored(h.ctx, provisionErrorInfo{
		callbackErr: startup.Message(), reason: startup.Reason(), lastError: startup.Detail(),
		operationFailure: failure, startup: startup,
	}))
	require.Equal(h.t, backend.ProvisionStatusFailed, h.actor.sm.State())
}

func startupExitTerms(mint func(id string) failurecause.Provenance) shared.OperationStartupFailureTerms {
	return shared.OperationStartupFailureTerms{
		Reason: backend.ReasonContainerExited, Message: backend.MsgContainerExitedDuringStartup,
		Detail: "exit_code=1", InstanceID: startupContainer, Service: "app",
		Termination: failurecause.Exited(), Provenance: mint(startupContainer), ExitCode: 1,
	}
}

// startupCrash is the tenant workload exiting on its own during startup
// verification: an observed run, exit 1.
func (h *budgetHarness) startupCrash() {
	h.t.Helper()
	h.startupFailure(startupExitTerms(observedRun))
}

func (h *budgetHarness) startupUnhealthy() {
	h.t.Helper()
	h.startupFailure(shared.OperationStartupFailureTerms{
		Reason: backend.ReasonHealthCheckFailed, Message: backend.MsgContainerUnhealthy,
		InstanceID: startupContainer, Service: "app",
	})
}

// A crash loop that never reaches Ready (ENG-1125): startup crashes count like
// deaths of a Ready workload and exhaust the budget only once the streak is
// three long and 30 minutes old.
func TestBudgetSequence_StartupCrashLoopExhaustsOnlyAfterTheFloor(t *testing.T) {
	t.Run("an outage loop inside the span never exhausts", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newBudgetHarness(t)
			start := time.Now()
			for want := 1; time.Since(start) < terminalBudgetMinStreakSpan-2*time.Minute; want++ {
				h.startupCrash()
				h.requireBudget(want, backend.TerminalVerdictRetry)
				time.Sleep(2 * time.Minute) // providerd re-provisions every pass
			}
			assert.Greater(t, h.observation().ConsecutiveFailures, 2*terminalBudgetThreshold)
		})
	})
	t.Run("the third crash after the span exhausts", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newBudgetHarness(t)
			for want := 1; want <= terminalBudgetThreshold; want++ {
				if want > 1 {
					time.Sleep(terminalBudgetMinStreakSpan / 2)
				}
				h.startupCrash()
				verdict := backend.TerminalVerdictRetry
				if want == terminalBudgetThreshold {
					verdict = backend.TerminalVerdictExhausted
				}
				h.requireBudget(want, verdict)
			}
			assert.Equal(t, []string{"tenant_workload", "tenant_workload", "tenant_workload"}, h.metrics.recordedFailures())
			assert.Equal(t, backend.ReasonContainerExited, h.projection().Reason)
		})
	})
}

// A Ready lease's death and the startup crashes of its re-provisions share one
// streak: Ready -> crash (1) -> startup crash (2) -> startup crash (3).
func TestBudgetSequence_ReadyDeathThenStartupCrashesShareTheStreak(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newBudgetHarness(t)
		h.provisionReady()
		time.Sleep(time.Minute)
		h.crash()
		h.requireBudget(1, backend.TerminalVerdictRetry)
		time.Sleep(terminalBudgetMinStreakSpan / 2)
		h.startupCrash()
		h.requireBudget(2, backend.TerminalVerdictRetry)
		time.Sleep(terminalBudgetMinStreakSpan / 2)
		h.startupCrash()
		h.requireBudget(3, backend.TerminalVerdictExhausted)
	})
}

// Only an observed exit of a launch the platform completed counts. A startup
// failure whose death the stream did not observe whole, after a signal, of a
// degraded launch, or a health check that never passed, is recorded but never
// counts.
func TestBudgetSequence_StartupFailureAttribution(t *testing.T) {
	degraded := startupExitTerms(observedRun)
	degraded.Degraded = true
	unhealthy := shared.OperationStartupFailureTerms{
		Reason: backend.ReasonHealthCheckFailed, Message: backend.MsgContainerUnhealthy,
		InstanceID: startupContainer, Service: "app",
	}
	refused := shared.OperationStartupFailureTerms{
		Reason: backend.ReasonContainerStartFailed, Message: backend.MsgContainerStartRefused,
		InstanceID: startupContainer, Service: "app",
	}
	tests := []struct {
		name  string
		terms shared.OperationStartupFailureTerms
		want  string
	}{
		{"observed run", startupExitTerms(observedRun), "tenant_workload"},
		{"signaled during verification", startupExitTerms(signaledRun), "disruption"},
		{"run start not observed", startupExitTerms(partialRun), "unknown"},
		{"no live event", startupExitTerms(unobserved), "unknown"},
		{"degraded launch", degraded, "platform"},
		{"health check never passed", unhealthy, "unhealthy"},
		{"start refused by the container runtime", refused, "platform"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newBudgetHarness(t)
				h.startupFailure(test.terms)
				assert.Equal(t, []string{test.want}, h.metrics.recordedFailures())
				wantCount := 0
				if test.want == "tenant_workload" {
					wantCount = 1
				}
				h.requireBudget(wantCount, backend.TerminalVerdictRetry)
				assert.Equal(t, 1, h.projection().FailCount, "every startup failure stays in the lifetime diagnostic")
				assert.Equal(t, test.terms.Reason, h.projection().Reason)
			})
		})
	}
}

// A health check that never passed neither counts nor resets the streak: the
// lease never became Ready, so no sustained-Ready period ended.
func TestBudgetSequence_UnhealthyStartupNeitherCountsNorResets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newBudgetHarness(t)
		h.startupCrash()
		time.Sleep(terminalBudgetMinStreakSpan / 2)
		h.startupCrash()
		h.requireBudget(2, backend.TerminalVerdictRetry)
		for range 3 {
			time.Sleep(2 * time.Minute)
			h.startupUnhealthy()
			h.requireBudget(2, backend.TerminalVerdictRetry)
		}
		time.Sleep(terminalBudgetMinStreakSpan / 2)
		h.startupCrash()
		h.requireBudget(3, backend.TerminalVerdictExhausted)
	})
}

// A provision refusal before any effect still records a platform failure that
// never counts, and leaves a running streak untouched.
func TestBudgetSequence_RefusalBetweenStartupCrashesIsUncounted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newBudgetHarness(t)
		h.startupCrash()
		h.refuseProvision()
		h.requireBudget(1, backend.TerminalVerdictRetry)
		assert.Equal(t, []string{"tenant_workload", "platform"}, h.metrics.recordedFailures())
	})
}
