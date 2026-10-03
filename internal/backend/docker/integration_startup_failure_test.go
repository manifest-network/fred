//go:build integration

package docker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// TestIntegration_Docker_StartupExitSettlesFailedAndReprovisions pins
// ENG-1125 against a real daemon. The workload exits on its own one second
// after it starts (a timed self-exit, never `docker kill`, which would be a
// signal), so it is still running when Compose returns and exits during
// startup verification. Each attempt must end Failed within seconds, with the
// curated reason and no container left, never Provisioning, and the lease
// must re-provision at once. Each exit is an observed run (start then die,
// with no kill), so it counts, but three counted failures minutes apart stay
// short of the 30-minute floor and never exhaust the budget.
//
// It also pins the Docker assumptions the definite path rests on: a
// self-exiting container reports "exited" with its own code under restart
// policy "no", and its events are a start and a die without a kill.
func TestIntegration_Docker_StartupExitSettlesFailedAndReprovisions(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)
	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.ContainerReadonlyRootfs = ptrBool(false)
		// Long enough that only the worker's own definite path can settle the
		// failure in time: neither the operation deadline nor a sweep helps.
		cfg.ProvisionTimeout = 10 * time.Minute
		// The single fixed-wait pass runs after the self-exit.
		cfg.StartupVerifyDuration = 5 * time.Second
	})
	leaseUUID := newIntegrationLeaseUUID()
	payload, err := json.Marshal(manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sh", "-c", "sleep 1; exit 3"},
	})
	require.NoError(t, err)

	for attempt := 1; attempt <= 3; attempt++ {
		callbacks := newIntegrationCallbackAuthority(t, callbackServer.URL)
		started := time.Now()
		require.NoError(t, b.Provision(context.Background(), backend.ProvisionRequest{
			LeaseUUID:            leaseUUID,
			Tenant:               "test-tenant",
			ProviderUUID:         testProviderUUID,
			Items:                []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
			CallbackURL:          callbacks.operationURL,
			LifecycleCallbackURL: callbacks.lifecycleURL,
			Payload:              payload,
		}), "attempt %d: a Failed lease re-provisions at once", attempt)

		awaitDefiniteStartupFailure(t, b, callbackCh, leaseUUID,
			backend.ReasonContainerExited, backend.MsgContainerExitedDuringStartup)
		assert.Less(t, time.Since(started), time.Minute, "attempt %d: settled by the worker, not a deadline", attempt)

		info := getProvisionInfo(t, b, leaseUUID)
		assert.Equal(t, attempt, info.FailCount)
		require.NotNil(t, info.TerminalBudget)
		assert.Equal(t, backend.TerminalBudgetObservation{
			Verdict: backend.TerminalVerdictRetry, ConsecutiveFailures: attempt,
		}, *info.TerminalBudget, "attempt %d: an observed self-exit counts, behind the floor", attempt)
	}
}
