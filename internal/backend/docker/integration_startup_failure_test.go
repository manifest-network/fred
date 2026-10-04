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

// TestIntegration_Stack_HealthyDependencyExitFailsDefinitely pins ENG-1125's
// rejected-launch path against a real daemon and Compose: a depends_on
// dependency gated by service_healthy exits before it becomes healthy, so
// `compose up` itself fails, after its exchange settled, and leaves the
// dependent service created but never started. The backend decides from the
// cohort, never from Compose's error: the exited dependency is a positive
// fact, so the provision fails definitely within seconds, with nothing left.
func TestIntegration_Stack_HealthyDependencyExitFailsDefinitely(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)
	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.ContainerReadonlyRootfs = ptrBool(false)
		cfg.ProvisionTimeout = 10 * time.Minute
	})
	leaseUUID := newIntegrationLeaseUUID()
	stack := manifest.StackManifest{Services: map[string]*manifest.Manifest{
		"web": {
			Image:     "busybox:latest",
			Command:   []string{"sleep", "3600"},
			DependsOn: map[string]manifest.DependsOnCondition{"db": {Condition: "service_healthy"}},
		},
		"db": {
			Image:   "busybox:latest",
			Command: []string{"sh", "-c", "sleep 1; exit 3"},
			HealthCheck: &manifest.HealthCheckConfig{
				Test:     []string{"CMD-SHELL", "test -f /tmp/never"},
				Interval: testHealthDuration(10 * time.Second),
				Timeout:  testHealthDuration(time.Second),
				Retries:  3,
			},
		},
	}}
	payload, err := json.Marshal(stack)
	require.NoError(t, err)
	callbacks := newIntegrationCallbackAuthority(t, callbackServer.URL)
	started := time.Now()
	require.NoError(t, b.Provision(context.Background(), backend.ProvisionRequest{
		LeaseUUID: leaseUUID, Tenant: "test-tenant", ProviderUUID: testProviderUUID,
		Items: []backend.LeaseItem{
			{SKU: "docker-micro", Quantity: 1, ServiceName: "web"},
			{SKU: "docker-micro", Quantity: 1, ServiceName: "db"},
		},
		CallbackURL: callbacks.operationURL, LifecycleCallbackURL: callbacks.lifecycleURL,
		Payload: payload,
	}))

	awaitDefiniteStartupFailure(t, b, callbackCh, leaseUUID,
		backend.ReasonContainerExited, backend.MsgContainerExitedDuringHealthCheck)
	assert.Less(t, time.Since(started), time.Minute, "settled by the worker, not a deadline")
}

// TestIntegration_Docker_RefusedStartFailsDefinitelyAndNeverCounts pins the
// daemon behavior ENG-1125's refused-start path rests on: a container whose
// entrypoint does not exist is rejected by the runtime at Start with a final
// error response, and stays "created". The provision fails definitely with
// ContainerStartFailed, nothing of it is left, and no tenant process ran, so
// it never counts.
func TestIntegration_Docker_RefusedStartFailsDefinitelyAndNeverCounts(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)
	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.ContainerReadonlyRootfs = ptrBool(false)
		cfg.ProvisionTimeout = 10 * time.Minute
	})
	leaseUUID := newIntegrationLeaseUUID()
	payload, err := json.Marshal(manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"/fred-integration-no-such-entrypoint"},
	})
	require.NoError(t, err)
	callbacks := newIntegrationCallbackAuthority(t, callbackServer.URL)
	started := time.Now()
	require.NoError(t, b.Provision(context.Background(), backend.ProvisionRequest{
		LeaseUUID: leaseUUID, Tenant: "test-tenant", ProviderUUID: testProviderUUID,
		Items:       []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
		CallbackURL: callbacks.operationURL, LifecycleCallbackURL: callbacks.lifecycleURL,
		Payload: payload,
	}))

	awaitDefiniteStartupFailure(t, b, callbackCh, leaseUUID,
		backend.ReasonContainerStartFailed, backend.MsgContainerStartRefused)
	assert.Less(t, time.Since(started), time.Minute, "settled by the worker, not a deadline")
	info := getProvisionInfo(t, b, leaseUUID)
	if info.TerminalBudget != nil {
		assert.Zero(t, info.TerminalBudget.ConsecutiveFailures, "a refused start never counts")
	}
}
