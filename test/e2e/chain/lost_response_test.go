//go:build integration

package chain

import (
	"context"
	"errors"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/test/e2e"
)

// The tests in this file lose the backend's answer to a command. The backend
// receives the command and finishes the work, and providerd never hears back,
// so it cannot tell a command that failed from one that worked. Whatever it
// does next, the work must end up done once: one container set, started once.
//
// Each test loses the answer only after the work is finished on the backend,
// and asserts that it was.

// lossy starts a provider whose one backend providerd reaches through a
// link, and returns the pieces a test uses.
func lossy(t *testing.T) (p *Provider, backend *e2e.Backend, link *e2e.Link, tenant *Tenant) {
	t.Helper()
	p = StartProviderWith(t, 1, Options{Links: true})
	return p, p.Backends[0], p.Links[0], p.NewTenant(t)
}

// readyIn reports whether the backend runs the lease, ready, in exactly one
// container that accept approves. It never fails the test.
func readyIn(backend *e2e.Backend, leaseUUID string, accept func(container.InspectResponse) bool) bool {
	lease, err := backend.Client.Get(context.Background(), leaseUUID)
	if err != nil || lease.Status != "ready" {
		return false
	}
	containers, err := backend.TryContainers(leaseUUID)
	return err == nil && len(containers) == 1 && containers[0].State.Running && accept(containers[0])
}

// oneReady reports whether exactly one lease on the backend is ready. It
// never fails the test. A test uses it for a lease that does not exist when
// the response to lose is named.
func oneReady(backend *e2e.Backend) bool {
	leases, err := backend.Client.List(context.Background())
	if err != nil {
		return false
	}
	ready := 0
	for _, lease := range leases {
		if lease.Status == "ready" {
			ready++
		}
	}
	return ready == 1
}

// otherThan accepts every container but the given one.
func otherThan(previous container.InspectResponse) func(container.InspectResponse) bool {
	return func(now container.InspectResponse) bool { return now.ID != previous.ID }
}

// requireDoneOnce waits out what providerd does after the lost answer, and
// checks that the lease still runs the container the lost command produced.
func requireDoneOnce(t *testing.T, p *Provider, backend *e2e.Backend, leaseUUID string, done container.InspectResponse) {
	t.Helper()
	requireActive(t, p.Chain, leaseUUID)
	p.AwaitSweeps(t, 2, SweepComplete)
	requireStatus(t, backend, leaseUUID, "ready")
	requireUntouched(t, backend, leaseUUID, done)
}

// TestLostResponse_Create loses the backend's answer to a provision, after
// the workload runs. The lease must become active on that one workload.
func TestLostResponse_Create(t *testing.T) {
	p, backend, link, tenant := lossy(t)
	lost := link.LoseResponse("provision", func() bool { return oneReady(backend) })

	lease := tenant.Deploy(t, p.SKUs.Stateless, sleeper("3600"))
	lost.Require(t)
	done := running(t, backend, lease)

	requireDoneOnce(t, p, backend, lease, done)
}

// TestLostResponse_Restore loses the backend's answer to a restore, after
// the restored workload runs. The target must become active on that one
// workload and on the source's data.
func TestLostResponse_Restore(t *testing.T) {
	p, backend, link, tenant := lossy(t)
	stateful := map[string]any{"image": e2e.RedisImage, "command": []string{"sleep", "3600"}}
	source := deployActive(t, p, tenant, p.SKUs.Stateful, stateful)
	backend.Exec(t, running(t, backend, source).ID, "sh", "-c", "echo kept > /data/sentinel")
	tenant.Close(t, source)
	requireStatus(t, backend, source, "retained")

	lost := link.LoseResponse("restore", func() bool { return oneReady(backend) })
	target := tenant.Restore(t, p.SKUs.Stateful, source, stateful)
	lost.Require(t)
	done := running(t, backend, target)

	requireDoneOnce(t, p, backend, target, done)
	require.Equal(t, "kept\n", backend.Exec(t, done.ID, "cat", "/data/sentinel"))
	requireGone(t, backend, source)
}

// TestLostResponse_Restart loses the backend's answer to a restart, after
// the new container runs. The tenant repeats the request, and the lease must
// have been restarted once.
func TestLostResponse_Restart(t *testing.T) {
	p, backend, link, tenant := lossy(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	previous := running(t, backend, lease)

	lost := link.LoseResponse("restart", func() bool { return readyIn(backend, lease, otherThan(previous)) })
	tenant.Restart(t, lease)
	lost.Require(t)
	done := running(t, backend, lease)
	require.NotEqual(t, previous.ID, done.ID)

	requireDoneOnce(t, p, backend, lease, done)
}

// TestLostResponse_Update loses the backend's answer to an update, after the
// new workload runs. The tenant repeats the request, and the lease must have
// been updated once.
func TestLostResponse_Update(t *testing.T) {
	p, backend, link, tenant := lossy(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	previous := running(t, backend, lease)

	lost := link.LoseResponse("update", func() bool { return readyIn(backend, lease, otherThan(previous)) })
	tenant.Update(t, lease, sleeper("7200"))
	lost.Require(t)
	done := running(t, backend, lease)
	require.Equal(t, []string{"sleep", "7200"}, []string(done.Config.Cmd), "the lease must run the updated workload")

	requireDoneOnce(t, p, backend, lease, done)
}

// TestLostResponse_Close loses the backend's answer to a close, after the
// lease is gone from the backend. providerd must settle without it, and a
// lease beside it must not be touched.
func TestLostResponse_Close(t *testing.T) {
	p, backend, link, tenant := lossy(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	beside := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	containerBeside := running(t, backend, beside)

	lost := link.LoseResponse("deprovision", func() bool {
		_, err := backend.Client.Get(context.Background(), lease)
		containers, listErr := backend.TryContainers(lease)
		return errors.Is(err, e2e.ErrNotFound) && listErr == nil && len(containers) == 0
	})
	tenant.Close(t, lease)
	lost.Require(t)

	requireNothingLeft(t, tenant, backend, lease)
	p.AwaitSweeps(t, 2, SweepComplete)
	require.Empty(t, backend.Leftovers(lease))
	requireUntouched(t, backend, beside, containerBeside)
	require.Equal(t, LeaseActive, p.Chain.Lease(t, beside).State)
}
