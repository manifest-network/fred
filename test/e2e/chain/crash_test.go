//go:build integration

package chain

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/test/e2e"
)

// The tests in this file kill a daemon in the middle of an operation and
// check that the lease ends where the operation was heading, with nothing
// started twice and nothing left behind.
//
// The middle is chosen from outside. A workload's health check passes only
// once the test creates a file in its container, so an operation stays at
// "started, not healthy" for as long as the test wants. A close is held by a
// workload that ignores SIGTERM, so stopping it lasts the whole stop grace.
// Each test asserts that the operation is held there before it kills.

const (
	// gateFile is the file a gated workload's health check waits for.
	gateFile = "/tmp/e2e-gate"
	// stopGrace is how long the backends of the close tests wait for a
	// workload to stop.
	stopGrace = "30s"
)

// heldSettings gives an operation time to be held: by default the fixture's
// backends give up on a workload that is not healthy within a minute.
func heldSettings(settings map[string]any) Options {
	merged := map[string]any{"provision_timeout": "10m"}
	for key, value := range settings {
		merged[key] = value
	}
	return Options{BackendSettings: merged}
}

// gated makes a workload report healthy only once the test opens its gate.
// Every new container of the workload starts with its gate closed.
func gated(manifest map[string]any) map[string]any {
	manifest["health_check"] = map[string]any{
		"test":     []string{"CMD", "test", "-f", gateFile},
		"interval": "1s",
		// Failed checks do not count during the start period, so the
		// container stays "starting" and is never reported unhealthy.
		"start_period": "1h",
	}
	return manifest
}

// requireHeld waits until an operation on the lease is held at "started, not
// healthy": the backend reports the given status, and exactly one container
// of the lease is running without having passed its health check. It returns
// that container.
func requireHeld(t *testing.T, backend *e2e.Backend, leaseUUID, status string) container.InspectResponse {
	t.Helper()
	var held container.InspectResponse
	eventually(t, "lease "+leaseUUID+" is held "+status+" on backend "+backend.Name, func() bool {
		lease, err := backend.Client.Get(t.Context(), leaseUUID)
		if err != nil || lease.Status != status {
			return false
		}
		starting := startingContainers(t, backend, leaseUUID)
		if len(starting) != 1 {
			return false
		}
		held = starting[0]
		return true
	})
	requireLaunchesRecorded(t, backend)
	return held
}

// requireLaunchesRecorded waits until the backend has recorded the outcome
// of every container launch it asked Docker for. Until then the backend
// cannot know, after a crash, what Docker did with the request. For a restart
// or an update, 0.14 then refuses to start until an operator settles it
// offline (ENG-2194; OPERATIONS.md, "Unsettled Docker effects"). These tests
// crash a backend that knows what it started.
func requireLaunchesRecorded(t *testing.T, backend *e2e.Backend) {
	t.Helper()
	eventually(t, "backend "+backend.Name+" has recorded its container launches", func() bool {
		// The backend samples the gauge when its health is inspected.
		return backend.Serving() && backend.Metric(t, "fred_docker_backend_volume_launches_pending") == 0
	})
}

// startingContainers lists the lease's running containers that have not
// passed their health check yet.
func startingContainers(t *testing.T, backend *e2e.Backend, leaseUUID string) []container.InspectResponse {
	t.Helper()
	// A container can go away between the listing and its inspection while
	// a backend recovers. That reads as none, and the caller looks again.
	all, err := backend.TryContainers(leaseUUID)
	if err != nil {
		t.Logf("containers of lease %s: %v", leaseUUID, err)
		return nil
	}
	var starting []container.InspectResponse
	for _, one := range all {
		if one.State.Running && one.State.Health != nil && one.State.Health.Status == container.Starting {
			starting = append(starting, one)
		}
	}
	return starting
}

// requireStillHeld checks that the held container is still the lease's one
// starting container, and was not started again.
func requireStillHeld(t *testing.T, backend *e2e.Backend, leaseUUID string, held container.InspectResponse) {
	t.Helper()
	starting := startingContainers(t, backend, leaseUUID)
	require.Len(t, starting, 1, "lease %s must still have its one starting container", leaseUUID)
	require.Equal(t, held.ID, starting[0].ID, "lease %s must still be held in the same container", leaseUUID)
	require.Equal(t, held.State.StartedAt, starting[0].State.StartedAt, "the held container of lease %s must not have been started again", leaseUUID)
}

// openGate lets the held container pass its health check.
func openGate(t *testing.T, backend *e2e.Backend, held container.InspectResponse) {
	t.Helper()
	backend.Exec(t, held.ID, "touch", gateFile)
}

// requireSettled waits until the lease is ready and active with exactly one
// container, and checks that it is the container the interrupted operation
// had started: the operation was finished, not run again.
func requireSettled(t *testing.T, p *Provider, backend *e2e.Backend, leaseUUID string, held container.InspectResponse) container.InspectResponse {
	t.Helper()
	var now container.InspectResponse
	eventually(t, "lease "+leaseUUID+" is ready in one healthy container on backend "+backend.Name, func() bool {
		lease, err := backend.Client.Get(t.Context(), leaseUUID)
		if err != nil || lease.Status != "ready" {
			return false
		}
		containers := backend.Containers(t, leaseUUID)
		if len(containers) != 1 {
			return false
		}
		now = containers[0]
		return now.State.Running && now.State.Health != nil && now.State.Health.Status == container.Healthy
	})
	require.Equal(t, held.ID, now.ID, "lease %s must run the container the interrupted operation started", leaseUUID)
	require.Equal(t, held.State.StartedAt, now.State.StartedAt, "the container of lease %s must not have been started again", leaseUUID)
	requireActive(t, p.Chain, leaseUUID)
	return now
}

// command is the process a container runs.
func command(c container.InspectResponse) []string {
	return append([]string{c.Path}, c.Args...)
}

// restartBackendOnto starts the killed backend again and waits until it
// serves and the lease is ready in exactly one running container, which it
// returns. While it waits, it opens the gate of every container the backend
// is waiting on.
//
// 0.14 does not take up an interrupted restart or update where it was. At
// its next start it removes the container the operation had started, puts
// the previous workload back in a new one, and waits for that one to pass
// its health check before it serves anything (ENG-2193). The held container
// is therefore let through only once the backend serves, when it is known to
// be kept.
func restartBackendOnto(t *testing.T, backend *e2e.Backend, leaseUUID string, held container.InspectResponse) container.InspectResponse {
	t.Helper()
	backend.Launch()
	var now container.InspectResponse
	eventually(t, "backend "+backend.Name+" serves again with lease "+leaseUUID+" ready in one container", func() bool {
		serving := backend.Serving()
		for _, waiting := range startingContainers(t, backend, leaseUUID) {
			if waiting.ID != held.ID || serving {
				openGate(t, backend, waiting)
			}
		}
		if !serving {
			return false
		}
		lease, err := backend.Client.Get(t.Context(), leaseUUID)
		if err != nil || lease.Status != "ready" {
			return false
		}
		containers := backend.Containers(t, leaseUUID)
		if len(containers) != 1 || !containers[0].State.Running {
			return false
		}
		now = containers[0]
		return now.State.Health == nil || now.State.Health.Status == container.Healthy
	})
	backend.AwaitServing()
	return now
}

// requireNothingLeft closes the lease and waits until the backend no longer
// lists it and the host holds nothing of it: no container, no network and no
// volume directory. Asking the backend for the lease by name proves less: a
// lease that once failed keeps answering with that failure's diagnostics.
func requireNothingLeft(t *testing.T, tenant *Tenant, backend *e2e.Backend, leaseUUID string) {
	t.Helper()
	if tenant.provider.Chain.Lease(t, leaseUUID).State != LeaseClosed {
		tenant.Close(t, leaseUUID)
	}
	var left []string
	eventually(t, "nothing is left of lease "+leaseUUID, func() bool {
		leases, err := backend.Client.List(t.Context())
		if err != nil {
			return false
		}
		left = backend.Leftovers(leaseUUID)
		for _, lease := range leases {
			if lease.UUID == leaseUUID {
				left = append(left, "listed as "+lease.Status)
			}
		}
		if len(left) != 0 {
			t.Logf("still held of lease %s: %s", leaseUUID, strings.Join(left, "; "))
		}
		return len(left) == 0
	})
	require.Equal(t, LeaseClosed, tenant.provider.Chain.Lease(t, leaseUUID).State)
}

// dataDir is the host directory behind a container's /data volume.
func dataDir(t *testing.T, c container.InspectResponse) string {
	t.Helper()
	for _, mount := range c.Mounts {
		if mount.Destination == "/data" {
			return mount.Source
		}
	}
	require.Failf(t, "no data volume", "container %s mounts nothing at /data: %+v", c.ID, c.Mounts)
	return ""
}

// inode is the inode number of a host path.
func inode(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Sys().(*syscall.Stat_t).Ino
}

// holdCreate deploys a gated workload and waits until its creation is held.
func holdCreate(t *testing.T, p *Provider, tenant *Tenant, sku string, manifest map[string]any) (leaseUUID string, held container.InspectResponse) {
	t.Helper()
	leaseUUID = tenant.Deploy(t, sku, gated(manifest))
	held = requireHeld(t, p.Backends[0], leaseUUID, "provisioning")
	require.Equal(t, LeasePending, p.Chain.Lease(t, leaseUUID).State, "a lease whose workload is not healthy is not acknowledged")
	return leaseUUID, held
}

// restoreSource is a retained lease to restore from, and what its data
// looked like on the host when it was closed.
type restoreSource struct {
	leaseUUID string
	manifest  map[string]any
	inode     uint64
	project   int
}

// retainSource runs a gated workload with a volume, writes to the volume and
// closes the lease, so the backend retains its data. A restore runs the
// source's own workload, so the restored container starts with its gate
// closed.
func retainSource(t *testing.T, p *Provider, tenant *Tenant) restoreSource {
	t.Helper()
	backend := p.Backends[0]
	source := restoreSource{manifest: map[string]any{"image": e2e.RedisImage, "command": []string{"sleep", "3600"}}}
	var held container.InspectResponse
	source.leaseUUID, held = holdCreate(t, p, tenant, p.SKUs.Stateful, source.manifest)
	openGate(t, backend, held)
	requireSettled(t, p, backend, source.leaseUUID, held)
	backend.Exec(t, held.ID, "sh", "-c", "echo kept > /data/sentinel")
	source.inode, source.project = inode(t, dataDir(t, held)), e2e.ProjectID(t, dataDir(t, held))
	require.NotZero(t, source.project, "a volume belongs to a project")

	tenant.Close(t, source.leaseUUID)
	requireStatus(t, backend, source.leaseUUID, "retained")
	return source
}

// holdRestore restores the source into a new lease and waits until the
// restore is held.
func holdRestore(t *testing.T, p *Provider, tenant *Tenant, source restoreSource) (leaseUUID string, held container.InspectResponse) {
	t.Helper()
	leaseUUID = tenant.Restore(t, p.SKUs.Stateful, source.leaseUUID, source.manifest)
	held = requireHeld(t, p.Backends[0], leaseUUID, "restarting")
	require.Equal(t, LeasePending, p.Chain.Lease(t, leaseUUID).State, "a lease whose workload is not healthy is not acknowledged")
	return leaseUUID, held
}

// requireRestored checks that a settled restore runs on the source's own
// data, with its quota, and that the source no longer holds it.
func requireRestored(t *testing.T, backend *e2e.Backend, source restoreSource, settled container.InspectResponse) {
	t.Helper()
	require.Equal(t, "kept\n", backend.Exec(t, settled.ID, "cat", "/data/sentinel"))
	data := dataDir(t, settled)
	require.Equal(t, source.inode, inode(t, data), "the restored volume must be the source's own directory")
	require.Equal(t, source.project, e2e.ProjectID(t, data), "the restored volume must keep its project")
	root := filepath.Join(backend.VolumePath, strings.SplitN(strings.TrimPrefix(data, backend.VolumePath+"/"), "/", 2)[0])
	require.Equal(t, 64*1024, backend.QuotaLimitKB(t, root), "the restored volume must keep its quota")
	require.Equal(t, 1, backend.QuotaEntries(t), "one volume, one quota entry")
	requireGone(t, backend, source.leaseUUID)
}

// TestCrash_BackendDuringCreate kills the backend while a new lease's
// workload is started but not healthy. The restarted backend must take the
// attempt up where it was: the same container becomes the lease.
func TestCrash_BackendDuringCreate(t *testing.T) {
	p := StartProviderWith(t, 1, heldSettings(nil))
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease, held := holdCreate(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))

	backend.Kill()
	backend.Start()
	requireStillHeld(t, backend, lease, held)
	require.Equal(t, LeasePending, p.Chain.Lease(t, lease).State)

	openGate(t, backend, held)
	requireSettled(t, p, backend, lease, held)
	requireNothingLeft(t, tenant, backend, lease)
}

// TestCrash_BackendDuringRestore kills the backend while a restored
// workload is started but not healthy. The restarted backend must finish the
// restore into the same container, on the source's data.
func TestCrash_BackendDuringRestore(t *testing.T) {
	p := StartProviderWith(t, 1, heldSettings(nil))
	backend, tenant := p.Backends[0], p.NewTenant(t)
	source := retainSource(t, p, tenant)
	target, held := holdRestore(t, p, tenant, source)

	backend.Kill()
	backend.Start()
	requireStillHeld(t, backend, target, held)

	openGate(t, backend, held)
	requireRestored(t, backend, source, requireSettled(t, p, backend, target, held))
}

// TestCrash_BackendDuringUpdate kills the backend while an update's new
// workload is started but not healthy. The update is lost, and nothing else
// is: the restarted backend puts the lease back on its previous workload, in
// one container, and the tenant can ask for the update again.
func TestCrash_BackendDuringUpdate(t *testing.T) {
	p := StartProviderWith(t, 1, heldSettings(nil))
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	previous := running(t, backend, lease)
	describeOnFailure(t, backend, lease)
	tenant.Update(t, lease, gated(sleeper("7200")))
	held := requireHeld(t, backend, lease, "updating")
	require.NotEqual(t, previous.ID, held.ID, "an update starts a new container")

	backend.Kill()
	restored := restartBackendOnto(t, backend, lease, held)
	require.Equal(t, []string{"sleep", "3600"}, command(restored), "the lease must be back on its previous workload")
	require.Equal(t, LeaseActive, p.Chain.Lease(t, lease).State)

	// The same update, asked again, goes through.
	tenant.Update(t, lease, gated(sleeper("7200")))
	again := requireHeld(t, backend, lease, "updating")
	openGate(t, backend, again)
	require.Equal(t, []string{"sleep", "7200"}, command(requireSettled(t, p, backend, lease, again)), "the lease must run the updated workload")
	requireNothingLeft(t, tenant, backend, lease)
}

// TestCrash_ProviderdDuringUpdate kills providerd while an update is under
// way that it never got its backend's answer to. The backend has the update
// and is starting the new workload; providerd told the tenant it could not
// say whether the update was taken, and then died. The restarted providerd
// must take the update up again and not run it twice: the tenant's same
// request is accepted, the lease becomes ready in the one container the
// update started, and the update is what the provider keeps for the lease.
// So when the lease later loses its container, it comes back on the update's
// image and not on the image it was first deployed with (ENG-619).
func TestCrash_ProviderdDuringUpdate(t *testing.T) {
	options := heldSettings(nil)
	options.Links = true
	p := StartProviderWith(t, 1, options)
	backend, link, tenant := p.Backends[0], p.Links[0], p.NewTenant(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, runs(e2e.BusyboxImage))
	previous := running(t, backend, lease)
	describeOnFailure(t, backend, lease)

	// One idempotency key, so the request sent again names the same update.
	key := uuid.NewString()
	updated := gated(runs(e2e.RedisImage))
	update := func() Answer {
		return tenant.Ask(t, http.MethodPost, lease, "update", map[string]string{"Idempotency-Key": key}, UpdateTo(t, updated))
	}

	// The backend's answer is lost once the update is under way: the new
	// container runs and has not passed its health check.
	var held container.InspectResponse
	lost := link.LoseResponse("update", func() bool {
		reported, err := backend.Client.Get(context.Background(), lease)
		if err != nil || reported.Status != "updating" {
			return false
		}
		containers, err := backend.TryContainers(lease)
		if err != nil || len(containers) != 1 {
			return false
		}
		one := containers[0]
		if one.ID == previous.ID || !one.State.Running || one.State.Health == nil || one.State.Health.Status != container.Starting {
			return false
		}
		held = one
		return true
	})
	sent := time.Now()
	unanswered := update()
	lost.Require(t)
	require.Equal(t, http.StatusServiceUnavailable, unanswered.Status, "providerd cannot say that an update it got no answer to was taken: %s", unanswered.Body)

	p.Kill()
	requireStillHeld(t, backend, lease, held)
	openGate(t, backend, held)
	requireStatus(t, backend, lease, "ready")
	p.Start()

	accepted(t, "the same update, asked again", update, http.StatusServiceUnavailable)
	settled := requireSettled(t, p, backend, lease, held)
	requireImage(t, settled, e2e.RedisImage)
	p.AwaitSweeps(t, 2, SweepComplete)
	requireUntouched(t, backend, lease, settled)
	require.Equal(t, []string{held.ID}, backend.Started(t, lease, sent), "the update must have started one container, once")

	// The lease loses its container. What the provider runs again is the
	// update, in a new container that waits for its own gate.
	backend.LoseContainers(t, lease)
	again := requireHeld(t, backend, lease, "provisioning")
	require.NotEqual(t, settled.ID, again.ID)
	requireImage(t, again, e2e.RedisImage)
	openGate(t, backend, again)
	requireSettled(t, p, backend, lease, again)
	onChain := p.Chain.Lease(t, lease)
	require.Equal(t, LeaseActive, onChain.State, "the provider closed the lease: %q", onChain.ClosureReason)
}

// TestCrash_BackendDuringRestart kills the backend while a restarted
// workload is started but not healthy. The restarted backend must bring the
// lease back to ready on the same workload, in one container.
func TestCrash_BackendDuringRestart(t *testing.T) {
	p := StartProviderWith(t, 1, heldSettings(nil))
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease, first := holdCreate(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	openGate(t, backend, first)
	requireSettled(t, p, backend, lease, first)
	describeOnFailure(t, backend, lease)
	tenant.Restart(t, lease)
	held := requireHeld(t, backend, lease, "restarting")
	require.NotEqual(t, first.ID, held.ID, "a restart starts a new container")

	backend.Kill()
	restored := restartBackendOnto(t, backend, lease, held)
	require.Equal(t, []string{"sleep", "3600"}, command(restored))
	require.Equal(t, LeaseActive, p.Chain.Lease(t, lease).State)

	// The lease can be restarted again.
	tenant.Restart(t, lease)
	again := requireHeld(t, backend, lease, "restarting")
	require.NotEqual(t, restored.ID, again.ID, "a restart starts a new container")
	openGate(t, backend, again)
	requireSettled(t, p, backend, lease, again)
	requireNothingLeft(t, tenant, backend, lease)
}

// holdClose closes a lease whose workload ignores SIGTERM, and waits until
// the backend is in the middle of stopping it. It returns the lease and the
// workload being stopped.
func holdClose(t *testing.T, p *Provider, tenant *Tenant) (leaseUUID string, workload container.InspectResponse) {
	t.Helper()
	backend := p.Backends[0]
	// sleep runs as the container's first process, which gets no default
	// signal handling: SIGTERM does nothing, and the stop lasts its grace.
	leaseUUID = deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	workload = running(t, backend, leaseUUID)
	tenant.Close(t, leaseUUID)
	eventually(t, "the close of lease "+leaseUUID+" is held while its workload stops", func() bool {
		lease, err := backend.Client.Get(t.Context(), leaseUUID)
		return err == nil && lease.Status == "deprovisioning"
	})
	requireCloseHeld(t, backend, leaseUUID, workload)
	return leaseUUID, workload
}

// requireCloseHeld checks that the lease's workload is still being stopped:
// the close has started and has not finished.
func requireCloseHeld(t *testing.T, backend *e2e.Backend, leaseUUID string, workload container.InspectResponse) {
	t.Helper()
	containers := backend.Containers(t, leaseUUID)
	require.Len(t, containers, 1)
	require.Equal(t, workload.ID, containers[0].ID)
	require.True(t, containers[0].State.Running, "the workload must still be stopping: %s", containers[0].State.Status)
}

// TestCrash_BackendDuringClose kills the backend while it stops a closed
// lease's workload. The restarted backend must finish the close by itself.
func TestCrash_BackendDuringClose(t *testing.T) {
	p := StartProviderWith(t, 1, heldSettings(map[string]any{"container_stop_timeout": stopGrace}))
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease, workload := holdClose(t, p, tenant)

	// providerd asks a backend again to close every closed lease it still
	// finds there, which would finish this close whatever the backend does.
	// It is down until the backend has finished alone. It is killed, not
	// stopped: a stop could outlast the workload's stop grace.
	p.Kill()
	requireCloseHeld(t, backend, lease, workload)
	backend.Kill()
	backend.Start()
	requireNothingLeft(t, tenant, backend, lease)

	p.Start()
	p.AwaitSweeps(t, 2, SweepComplete)
	require.Empty(t, backend.Leftovers(lease))
}

// TestCrash_ProviderdDuringCreate kills providerd while a new lease's
// workload is started but not healthy. The workload turns healthy while
// providerd is down, so the backend has nobody to report to. The restarted
// providerd must still acknowledge the lease, and must not start it again.
func TestCrash_ProviderdDuringCreate(t *testing.T) {
	p := StartProviderWith(t, 1, heldSettings(nil))
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease, held := holdCreate(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))

	p.Kill()
	requireStillHeld(t, backend, lease, held)
	openGate(t, backend, held)
	requireStatus(t, backend, lease, "ready")
	require.Equal(t, LeasePending, p.Chain.Lease(t, lease).State, "nothing can acknowledge the lease while providerd is down")
	p.Start()

	requireSettled(t, p, backend, lease, held)
	requireNothingLeft(t, tenant, backend, lease)
}

// TestCrash_ProviderdDuringRestore kills providerd while a restored
// workload is started but not healthy, and lets the restore finish on the
// backend while providerd is down. The restarted providerd must acknowledge
// the restored lease, and must not run the restore again.
func TestCrash_ProviderdDuringRestore(t *testing.T) {
	p := StartProviderWith(t, 1, heldSettings(nil))
	backend, tenant := p.Backends[0], p.NewTenant(t)
	source := retainSource(t, p, tenant)
	target, held := holdRestore(t, p, tenant, source)

	p.Kill()
	requireStillHeld(t, backend, target, held)
	openGate(t, backend, held)
	requireStatus(t, backend, target, "ready")
	p.Start()

	requireRestored(t, backend, source, requireSettled(t, p, backend, target, held))
}

// TestCrash_ProviderdDuringClose kills providerd while the backend stops a
// closed lease's workload. The backend must finish the close without it, and
// the restarted providerd must find nothing left to do.
func TestCrash_ProviderdDuringClose(t *testing.T) {
	p := StartProviderWith(t, 1, heldSettings(map[string]any{"container_stop_timeout": stopGrace}))
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease, _ := holdClose(t, p, tenant)

	p.Kill()
	requireNothingLeft(t, tenant, backend, lease)
	p.Start()

	p.AwaitSweeps(t, 2, SweepComplete)
	require.Empty(t, backend.Leftovers(lease))
}
