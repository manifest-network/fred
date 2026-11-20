//go:build integration

package chain

import (
	"context"
	"errors"
	"testing"
	"time"

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
// and asserts that it was. The container the work produced is recorded then,
// before the answer is lost: whoever sent the command repeats it as soon as
// the answer is missing, and a container read after that could already be the
// product of a second run.

// lossy starts a provider whose one backend providerd reaches through a
// link, and returns the pieces a test uses.
func lossy(t *testing.T) (p *Provider, backend *e2e.Backend, link *e2e.Link, tenant *Tenant) {
	t.Helper()
	p = StartProviderWith(t, 1, Options{Links: true})
	return p, p.Backends[0], p.Links[0], p.NewTenant(t)
}

// readyIn reports whether the backend runs the lease, ready, in exactly one
// container that accept approves, and records that container in done. It
// never fails the test. It runs while a link holds a response, so a test
// reads done only once the response is lost.
func readyIn(backend *e2e.Backend, leaseUUID string, accept func(container.InspectResponse) bool, done *container.InspectResponse) bool {
	lease, err := backend.Client.Get(context.Background(), leaseUUID)
	if err != nil || lease.Status != "ready" {
		return false
	}
	containers, err := backend.TryContainers(leaseUUID)
	if err != nil || len(containers) != 1 || !containers[0].State.Running || !accept(containers[0]) {
		return false
	}
	*done = containers[0]
	return true
}

// oneReady reports whether exactly one lease on the backend is ready, in
// exactly one container, and records that container in done as readyIn does.
// A test uses it for a lease that does not exist when the response to lose is
// named.
func oneReady(backend *e2e.Backend, done *container.InspectResponse) bool {
	leases, err := backend.Client.List(context.Background())
	if err != nil {
		return false
	}
	var ready []string
	for _, lease := range leases {
		if lease.Status == "ready" {
			ready = append(ready, lease.UUID)
		}
	}
	return len(ready) == 1 && readyIn(backend, ready[0], func(container.InspectResponse) bool { return true }, done)
}

// otherThan accepts every container but the given one.
func otherThan(previous container.InspectResponse) func(container.InspectResponse) bool {
	return func(now container.InspectResponse) bool { return now.ID != previous.ID }
}

// requireDoneOnce waits out what providerd does after the lost answer, and
// checks that the lease still runs the container the lost command produced,
// and that no other container of the lease was started since the command was
// sent, not even for a moment.
func requireDoneOnce(t *testing.T, p *Provider, backend *e2e.Backend, leaseUUID string, done container.InspectResponse, sent time.Time) {
	t.Helper()
	require.NotEmpty(t, done.ID, "the container the lost command produced was not recorded")
	requireActive(t, p.Chain, leaseUUID)
	p.AwaitSweeps(t, 2, SweepComplete)
	requireStatus(t, backend, leaseUUID, "ready")
	requireUntouched(t, backend, leaseUUID, done)
	require.Equal(t, []string{done.ID}, backend.Started(t, leaseUUID, sent), "the command must have started one container, once")
}

// TestLostResponse_Create loses the backend's answer to a provision, after
// the workload runs. The lease must become active on that one workload.
func TestLostResponse_Create(t *testing.T) {
	p, backend, link, tenant := lossy(t)
	var done container.InspectResponse
	lost := link.LoseResponse("provision", func() bool { return oneReady(backend, &done) })

	sent := time.Now()
	lease := tenant.Deploy(t, p.SKUs.Stateless, sleeper("3600"))
	lost.Require(t)

	requireDoneOnce(t, p, backend, lease, done, sent)
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

	var done container.InspectResponse
	lost := link.LoseResponse("restore", func() bool { return oneReady(backend, &done) })
	sent := time.Now()
	target := tenant.Restore(t, p.SKUs.Stateful, source, stateful)
	lost.Require(t)

	requireDoneOnce(t, p, backend, target, done, sent)
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

	var done container.InspectResponse
	lost := link.LoseResponse("restart", func() bool { return readyIn(backend, lease, otherThan(previous), &done) })
	sent := time.Now()
	tenant.Restart(t, lease)
	lost.Require(t)
	require.NotEqual(t, previous.ID, done.ID)

	requireDoneOnce(t, p, backend, lease, done, sent)
}

// TestLostResponse_Update loses the backend's answer to an update, after the
// new workload runs. The tenant repeats the request, and the lease must have
// been updated once.
func TestLostResponse_Update(t *testing.T) {
	p, backend, link, tenant := lossy(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	previous := running(t, backend, lease)

	var done container.InspectResponse
	lost := link.LoseResponse("update", func() bool { return readyIn(backend, lease, otherThan(previous), &done) })
	sent := time.Now()
	tenant.Update(t, lease, sleeper("7200"))
	lost.Require(t)
	require.Equal(t, []string{"sleep", "7200"}, command(done), "the lease must run the updated workload")

	requireDoneOnce(t, p, backend, lease, done, sent)
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
