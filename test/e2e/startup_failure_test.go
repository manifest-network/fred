//go:build integration

package e2e

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file deploy workloads that never come up. Each must end
// as a failed lease that says why, in the backend's fixed vocabulary, with
// nothing of the workload left on the host. The backend must reach that
// verdict itself, from what the containers did: every test here gives it a
// ten-minute provision timeout and fails if the lease is not settled within
// one minute (ENG-1125).

// What a backend reports for the failures in this file.
const (
	reasonContainerExited      = "ContainerExited"
	reasonHealthCheckFailed    = "HealthCheckFailed"
	reasonContainerStartFailed = "ContainerStartFailed"

	messageExitedDuringStartup     = "container exited during startup"
	messageExitedDuringHealthCheck = "container exited during health check"
	messageUnhealthy               = "container reported unhealthy"
	messageStartRefused            = "container runtime refused to start the container"
)

// failsAtStartup are the settings of a backend that could not settle a
// startup failure by running out of time.
var failsAtStartup = map[string]any{"provision_timeout": "10m"}

// requireStartupFailure waits until the backend reports the lease failed for
// the given reason, within a minute of when it was applied, and checks that
// no container of the lease is left, running or not.
func requireStartupFailure(t *testing.T, b *Backend, leaseUUID string, applied time.Time, reason, message string) Lease {
	t.Helper()
	failed := requireFailed(t, b, leaseUUID)
	require.Less(t, time.Since(applied), time.Minute, "the backend must settle a startup failure itself, not wait for a timeout")
	require.Equal(t, reason, failed.Reason)
	require.Equal(t, message, failed.Message)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		containers, err := b.TryContainers(leaseUUID)
		if assert.NoError(collect, err) {
			assert.Empty(collect, containers, "a failed deploy must leave no container")
		}
	}, 30*time.Second, 500*time.Millisecond)
	return failed
}

// leaseRunning is a new lease without a volume that runs the given command.
func leaseRunning(command ...string) Spec {
	lease := stateless("tenant-a")
	lease.Manifest = map[string]any{"image": BusyboxImage, "command": command}
	return lease
}

// TestBackend_WorkloadThatExitsAtOnceFailsTheLease deploys a workload that
// exits before the backend has finished starting it.
func TestBackend_WorkloadThatExitsAtOnceFailsTheLease(t *testing.T) {
	b := StartBackendWithConfig(t, failsAtStartup)
	lease := leaseRunning("false")
	applied := time.Now()
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStartupFailure(t, b, lease.LeaseUUID, applied, reasonContainerExited, messageExitedDuringStartup)
}

// applyAgain applies a lease whose previous deploy failed, and returns when
// the backend accepted it. A 0.14 backend reports a failure before its
// callback about it is acknowledged, and refuses the lease's next command
// until then. Only that refusal is retried, for thirty seconds; the sink
// acknowledges every callback at once.
func applyAgain(t *testing.T, b *Backend, lease Spec) time.Time {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		applied := time.Now()
		err := b.Client.Apply(t.Context(), lease)
		var refused *StatusError
		if !errors.As(err, &refused) || refused.Code != CodeCompletionPending || applied.After(deadline) {
			require.NoError(t, err, "apply lease %s again", lease.LeaseUUID)
			return applied
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestBackend_WorkloadThatExitsWhileWatchedCountsEveryTime deploys a workload
// that starts and exits a second later, by itself, while the backend is still
// watching it start. That is an observed run, so it counts towards the
// failures that can close a lease. A failed lease can be applied again, and
// each of three attempts must fail the same way and be counted.
func TestBackend_WorkloadThatExitsWhileWatchedCountsEveryTime(t *testing.T) {
	// The watch must outlast the workload's one second.
	b := StartBackendWithConfig(t, map[string]any{"provision_timeout": "10m", "startup_verify_duration": "5s"})
	lease := leaseRunning("sh", "-c", "sleep 1; exit 3")
	for attempt := 1; attempt <= 3; attempt++ {
		applied := applyAgain(t, b, lease)
		failed := requireStartupFailure(t, b, lease.LeaseUUID, applied, reasonContainerExited, messageExitedDuringStartup)
		require.Equal(t, attempt, failed.FailCount, "attempt %d", attempt)
		require.Equal(t, attempt, failed.ConsecutiveFailures, "attempt %d counts towards closing the lease", attempt)
		// Three failures within minutes are far from the half hour a lease
		// must keep failing for before it is closed.
		require.False(t, failed.BudgetExhausted, "attempt %d", attempt)
	}
}

// TestBackend_UnhealthyWorkloadFailsTheLease deploys a workload whose health
// check never passes, alone and as one service of a stack. A health check
// that never passed is not a run that failed, so it does not count towards
// closing the lease. In the stack, the healthy service goes with the rest.
func TestBackend_UnhealthyWorkloadFailsTheLease(t *testing.T) {
	b := StartBackendWithConfig(t, failsAtStartup)
	unhealthy := func() map[string]any {
		manifest := sleeper(BusyboxImage)
		manifest["health_check"] = map[string]any{"test": []string{"CMD", "false"}, "interval": "1s", "timeout": "1s", "retries": 1}
		return manifest
	}

	alone := stateless("tenant-a")
	alone.Manifest = unhealthy()
	applied := time.Now()
	require.NoError(t, b.Client.Apply(t.Context(), alone))
	failed := requireStartupFailure(t, b, alone.LeaseUUID, applied, reasonHealthCheckFailed, messageUnhealthy)
	require.Zero(t, failed.ConsecutiveFailures, "a health check that never passed does not count")

	stack := Spec{
		LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatelessSKU, Services: []string{"web", "db"},
		Manifest: map[string]any{"services": map[string]any{"web": sleeper(BusyboxImage), "db": unhealthy()}},
	}
	applied = time.Now()
	require.NoError(t, b.Client.Apply(t.Context(), stack))
	requireStartupFailure(t, b, stack.LeaseUUID, applied, reasonHealthCheckFailed, messageUnhealthy)
}

// TestBackend_DependencyThatExitsFailsTheStack deploys a stack whose web
// service waits for db to be healthy, while db exits before its first health
// check. Docker then refuses to bring the stack up and leaves web created and
// never started. The backend must report why db is gone, not Docker's
// refusal, and remove both.
func TestBackend_DependencyThatExitsFailsTheStack(t *testing.T) {
	b := StartBackendWithConfig(t, failsAtStartup)
	web := sleeper(BusyboxImage)
	web["depends_on"] = map[string]any{"db": map[string]any{"condition": "service_healthy"}}
	lease := Spec{
		LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatelessSKU, Services: []string{"web", "db"},
		Manifest: map[string]any{"services": map[string]any{
			"web": web,
			"db": map[string]any{
				"image": BusyboxImage, "command": []string{"sh", "-c", "sleep 1; exit 3"},
				// The first check comes long after db has exited.
				"health_check": map[string]any{"test": []string{"CMD-SHELL", "test -f /tmp/never"}, "interval": "10s", "timeout": "1s", "retries": 3},
			},
		}},
	}
	applied := time.Now()
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStartupFailure(t, b, lease.LeaseUUID, applied, reasonContainerExited, messageExitedDuringHealthCheck)
}

// TestBackend_WorkloadTheRuntimeRefusesFailsTheLease deploys a workload whose
// command does not exist in its image. The container runtime refuses to start
// it, so nothing ever ran: the failure does not count towards closing the
// lease.
func TestBackend_WorkloadTheRuntimeRefusesFailsTheLease(t *testing.T) {
	b := StartBackendWithConfig(t, failsAtStartup)
	lease := leaseRunning("/fred-e2e-no-such-command")
	applied := time.Now()
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	failed := requireStartupFailure(t, b, lease.LeaseUUID, applied, reasonContainerStartFailed, messageStartRefused)
	require.Zero(t, failed.ConsecutiveFailures, "a start the runtime refused does not count")
}

// TestBackend_FailedDeployStaysExplained fails a deploy and reads why from
// the backend three times: at once, after the backend has restarted, and
// after the lease is closed. The reason must be the same each time. A tenant
// asks why a deploy failed after the fact, when the provider has already
// closed the lease for it (ENG-939).
func TestBackend_FailedDeployStaysExplained(t *testing.T) {
	b := StartBackendWithConfig(t, failsAtStartup)
	lease := leaseRunning("false")
	applied := time.Now()
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	failed := requireStartupFailure(t, b, lease.LeaseUUID, applied, reasonContainerExited, messageExitedDuringStartup)
	require.Equal(t, 1, failed.FailCount)

	requireExplained := func(when string) {
		t.Helper()
		reported, err := b.Client.Get(t.Context(), lease.LeaseUUID)
		require.NoError(t, err, when)
		require.Equal(t, statusFailed, reported.Status, when)
		require.Equal(t, reasonContainerExited, reported.Reason, when)
		require.Equal(t, messageExitedDuringStartup, reported.Message, when)
		require.Equal(t, 1, reported.FailCount, when)
	}
	b.Stop()
	b.Start()
	requireExplained("after a restart")

	closeLease(t, b, lease.LeaseUUID)
	requireExplained("after the close")
	requireNotListed(t, b, lease.LeaseUUID)
	b.Stop()
	b.Start()
	requireExplained("after the close and a restart")
}
