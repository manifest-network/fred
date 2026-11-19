//go:build integration

package chain

import (
	"strconv"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/test/e2e"
)

// staleWindow is how long a test watches for a stale command to take effect:
// two of the backend's periodic passes.
const staleWindow = 20 * time.Second

// requireNoStart waits out staleWindow and checks from the daemon's event
// stream that no container of the lease was started since the given time.
// Reading the lease's containers afterwards would not be enough: a workload
// that ran for a moment has already acted.
func requireNoStart(t *testing.T, backend *e2e.Backend, leaseUUID string, since time.Time, why string) {
	t.Helper()
	time.Sleep(staleWindow)
	require.Empty(t, backend.Started(t, leaseUUID, since), why)
}

// TestStaleCommand_NeverStartsTheOlderWorkload delivers an update to the
// backend again after a newer update was applied, as a network that
// duplicates or delays a request would. The older workload must never start:
// not while the newer one runs, and not when the lease has lost its
// containers and the backend has restarted, where nothing running contradicts
// the older command.
func TestStaleCommand_NeverStartsTheOlderWorkload(t *testing.T) {
	p, backend, link, tenant := lossy(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	first := running(t, backend, lease)

	tenant.Update(t, lease, sleeper("7200"))
	older := replaced(t, backend, lease, first)
	olderCommand := link.Sent(t, "update")
	tenant.Update(t, lease, sleeper("10800"))
	newer := replaced(t, backend, lease, older)
	require.Equal(t, []string{"sleep", "10800"}, command(newer))

	// The older command arrives again while the newer workload runs.
	resent := time.Now()
	status := olderCommand.Resend(t)
	requireNoStart(t, backend, lease, resent, "the backend answered the older command "+strconv.Itoa(status)+" and started a container")
	requireUntouched(t, backend, lease, newer)

	// The older command arrives again after the lease lost its containers and
	// the backend restarted. providerd is stopped, so nothing puts the newer
	// workload back in the meantime.
	p.Stop()
	backend.LoseContainers(t, lease)
	backend.Stop()
	backend.Start()
	resent = time.Now()
	status = olderCommand.Resend(t)
	requireNoStart(t, backend, lease, resent, "the backend answered the older command "+strconv.Itoa(status)+" and started a container")

	// providerd puts the lease back on the newer workload.
	p.Start()
	var restored container.InspectResponse
	eventually(t, "lease "+lease+" runs again", func() bool {
		reported, err := backend.Client.Get(t.Context(), lease)
		if err != nil || reported.Status != "ready" {
			return false
		}
		containers := backend.Containers(t, lease)
		if len(containers) != 1 || !containers[0].State.Running {
			return false
		}
		restored = containers[0]
		return true
	})
	require.Equal(t, []string{"sleep", "10800"}, command(restored), "the lease must return on the newer workload")
	require.Equal(t, LeaseActive, p.Chain.Lease(t, lease).State)
}
