//go:build integration

package chain

import (
	"errors"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/test/e2e"
)

// sleeper is a manifest whose workload does nothing for the given number of
// seconds, so a lease is ready as soon as its container runs. The duration
// also tells two manifests of one lease apart.
func sleeper(seconds string) map[string]any {
	return map[string]any{"image": e2e.BusyboxImage, "command": []string{"sleep", seconds}}
}

// eventually polls a condition on the test's own goroutine, so the condition
// may use helpers that fail the test. The budget covers a first image
// admission.
func eventually(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for !condition() {
		require.True(t, time.Now().Before(deadline), "not within 3 minutes: %s", what)
		time.Sleep(500 * time.Millisecond)
	}
}

// requireActive waits until the chain reports the lease active.
func requireActive(t *testing.T, c *Chain, leaseUUID string) {
	t.Helper()
	eventually(t, "lease "+leaseUUID+" is active on the chain", func() bool {
		onChain := c.Lease(t, leaseUUID)
		if onChain.State == LeaseActive {
			return true
		}
		// Pending is the only state on the way to active.
		require.Equal(t, LeasePending, onChain.State, "the lease left pending without becoming active: %+v", onChain)
		return false
	})
}

// deployActive deploys a lease and waits until it is active on the chain,
// which the provider makes it only after the backend reported it ready.
func deployActive(t *testing.T, p *Provider, tenant *Tenant, sku string, manifest map[string]any) string {
	t.Helper()
	leaseUUID := tenant.Deploy(t, sku, manifest)
	requireActive(t, p.Chain, leaseUUID)
	return leaseUUID
}

// requireStatus waits until the backend reports the lease in the wanted
// status.
func requireStatus(t *testing.T, backend *e2e.Backend, leaseUUID, want string) {
	t.Helper()
	eventually(t, "backend "+backend.Name+" reports lease "+leaseUUID+" "+want, func() bool {
		lease, err := backend.Client.Get(t.Context(), leaseUUID)
		return err == nil && lease.Status == want
	})
}

// requireGone waits until the backend neither knows the lease nor runs
// anything for it.
func requireGone(t *testing.T, backend *e2e.Backend, leaseUUID string) {
	t.Helper()
	eventually(t, "backend "+backend.Name+" has nothing of lease "+leaseUUID, func() bool {
		_, err := backend.Client.Get(t.Context(), leaseUUID)
		return errors.Is(err, e2e.ErrNotFound) && len(backend.Containers(t, leaseUUID)) == 0
	})
}

// requireAbsent asserts that the backend knows nothing of the lease now.
func requireAbsent(t *testing.T, backend *e2e.Backend, leaseUUID string) {
	t.Helper()
	_, err := backend.Client.Get(t.Context(), leaseUUID)
	require.ErrorIs(t, err, e2e.ErrNotFound, "backend %s must not know lease %s", backend.Name, leaseUUID)
	require.Empty(t, backend.Containers(t, leaseUUID), "backend %s must run nothing for lease %s", backend.Name, leaseUUID)
}

// running returns the one container the backend runs for the lease.
func running(t *testing.T, backend *e2e.Backend, leaseUUID string) container.InspectResponse {
	t.Helper()
	containers := backend.Containers(t, leaseUUID)
	require.Len(t, containers, 1, "backend %s must run one container for lease %s", backend.Name, leaseUUID)
	require.True(t, containers[0].State.Running, "container state: %s", containers[0].State.Status)
	return containers[0]
}

// requireUntouched asserts that the backend still runs the same container,
// started at the same time, as when before was taken.
func requireUntouched(t *testing.T, backend *e2e.Backend, leaseUUID string, before container.InspectResponse) {
	t.Helper()
	now := running(t, backend, leaseUUID)
	require.Equal(t, before.ID, now.ID, "lease %s must still run its original container", leaseUUID)
	require.Equal(t, before.State.StartedAt, now.State.StartedAt, "the container of lease %s must not have been restarted", leaseUUID)
}

// replaced waits until the backend runs one container for the lease that is
// not before, and reports the lease ready.
func replaced(t *testing.T, backend *e2e.Backend, leaseUUID string, before container.InspectResponse) container.InspectResponse {
	t.Helper()
	var now container.InspectResponse
	eventually(t, "backend "+backend.Name+" runs lease "+leaseUUID+" in a new container", func() bool {
		containers := backend.Containers(t, leaseUUID)
		if len(containers) != 1 || containers[0].ID == before.ID || !containers[0].State.Running {
			return false
		}
		now = containers[0]
		lease, err := backend.Client.Get(t.Context(), leaseUUID)
		return err == nil && lease.Status == "ready"
	})
	return now
}
