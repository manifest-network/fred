//go:build integration

package e2e

import (
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// The tests in this file run leases of several services: a stack. A stack is
// one lease, so it becomes ready, fails and is closed as a whole. Its
// isolation from other leases is in tenant_test.go.

// byService groups a lease's containers by the service they run.
func byService(containers []container.InspectResponse) map[string][]container.InspectResponse {
	grouped := map[string][]container.InspectResponse{}
	for _, one := range containers {
		service := one.Config.Labels[labelServiceName]
		grouped[service] = append(grouped[service], one)
	}
	return grouped
}

// startedAt is when Docker last started a container.
func startedAt(t *testing.T, c container.InspectResponse) time.Time {
	t.Helper()
	started, err := time.Parse(time.RFC3339Nano, c.State.StartedAt)
	require.NoError(t, err, "start time of %s", c.Name)
	return started
}

// TestBackend_StackLifecycle runs a stack of two services, one of them in two
// instances. Every instance of every service must run, and a close must
// remove all of them before it answers.
func TestBackend_StackLifecycle(t *testing.T) {
	b := StartBackend(t)
	lease := Spec{
		LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatelessSKU,
		Services: []string{"web", "db"}, Quantity: map[string]int{"web": 2},
		Manifest: map[string]any{"services": map[string]any{
			"web": sleeper(BusyboxImage),
			"db":  sleeper(BusyboxImage),
		}},
	}
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	services := byService(requireRunningContainers(t, b, lease.LeaseUUID, 3))
	require.Len(t, services["web"], 2, "web runs two instances")
	require.Len(t, services["db"], 1, "db runs one instance")

	closeLease(t, b, lease.LeaseUUID)
	require.Empty(t, b.Containers(t, lease.LeaseUUID), "an accepted close has removed every container")
	requireGone(t, b, lease.LeaseUUID)
}

// TestBackend_StackStartsInDependencyOrder runs a stack whose web service
// waits for two others: for db to pass its health check, and for cache to
// have started. db passes its health check only once the test lets it. Until
// then web must not be started, though db and cache both run. After that the
// stack must become ready, with web started only after db could be healthy.
func TestBackend_StackStartsInDependencyOrder(t *testing.T) {
	const gate = "/tmp/e2e-gate"
	b := StartBackend(t)
	db := sleeper(BusyboxImage)
	// Failed checks do not count during the start period, so db stays
	// "starting" and is never reported unhealthy.
	db["health_check"] = map[string]any{"test": []string{"CMD", "test", "-f", gate}, "interval": "1s", "start_period": "1h"}
	web := sleeper(BusyboxImage)
	web["depends_on"] = map[string]any{
		"db":    map[string]any{"condition": "service_healthy"},
		"cache": map[string]any{"condition": "service_started"},
	}
	lease := Spec{
		LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatelessSKU,
		Services: []string{"web", "db", "cache"},
		Manifest: map[string]any{"services": map[string]any{"web": web, "db": db, "cache": sleeper(BusyboxImage)}},
	}
	require.NoError(t, b.Client.Apply(t.Context(), lease))

	// running returns the lease's running containers by service. A container
	// that was created and not started yet is not among them.
	running := func() map[string][]container.InspectResponse {
		var started []container.InspectResponse
		for _, one := range b.Containers(t, lease.LeaseUUID) {
			if one.State.Running {
				started = append(started, one)
			}
		}
		return byService(started)
	}
	require.Eventually(t, func() bool {
		services := running()
		return len(services["db"]) == 1 && len(services["cache"]) == 1
	}, 3*time.Minute, 200*time.Millisecond, "db and cache must be started")
	// web is held back for as long as db is not healthy, not just for the
	// moment it takes to start db.
	for until := time.Now().Add(5 * time.Second); time.Now().Before(until); {
		services := running()
		require.Empty(t, services["web"], "web must not be started before db is healthy")
		require.Len(t, services["db"], 1)
		require.NotNil(t, services["db"][0].State.Health, "db runs with its health check")
		require.Equal(t, "starting", services["db"][0].State.Health.Status)
		time.Sleep(200 * time.Millisecond)
	}
	opened := time.Now()
	b.Exec(t, running()["db"][0].ID, "touch", gate)

	requireStatus(t, b, lease.LeaseUUID, statusReady)
	services := byService(requireRunningContainers(t, b, lease.LeaseUUID, 3))
	for _, name := range lease.Services {
		require.Len(t, services[name], 1, "service %s", name)
	}
	require.Equal(t, "healthy", services["db"][0].State.Health.Status)
	webStarted := startedAt(t, services["web"][0])
	require.True(t, webStarted.After(opened), "web must start only after db could pass its health check")
	require.True(t, webStarted.After(startedAt(t, services["db"][0])), "web must start after db")
	require.True(t, webStarted.After(startedAt(t, services["cache"][0])), "web must start after cache")
}
