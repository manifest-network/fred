//go:build integration

package chain

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/docker/docker/api/types/container"
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
func heldSettings(settings map[string]any) map[string]any {
	merged := map[string]any{"provision_timeout": "10m"}
	for key, value := range settings {
		merged[key] = value
	}
	return merged
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
	return held
}

// startingContainers lists the lease's running containers that have not
// passed their health check yet.
func startingContainers(t *testing.T, backend *e2e.Backend, leaseUUID string) []container.InspectResponse {
	t.Helper()
	var starting []container.InspectResponse
	for _, one := range backend.Containers(t, leaseUUID) {
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

// requireNothingLeft closes the lease and waits until the host holds nothing
// of it: no container, no network and no volume directory.
func requireNothingLeft(t *testing.T, tenant *Tenant, backend *e2e.Backend, leaseUUID string) {
	t.Helper()
	if tenant.provider.Chain.Lease(t, leaseUUID).State != LeaseClosed {
		tenant.Close(t, leaseUUID)
	}
	requireGone(t, backend, leaseUUID)
	var left []string
	eventually(t, "the host holds nothing of lease "+leaseUUID, func() bool {
		left = backend.Leftovers(leaseUUID)
		if len(left) != 0 {
			t.Logf("still held of lease %s: %s", leaseUUID, strings.Join(left, "; "))
		}
		return len(left) == 0
	})
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
// workload is started but not healthy. The restarted backend must finish the
// update: the lease ends on the new workload, in the container the update
// had started.
func TestCrash_BackendDuringUpdate(t *testing.T) {
	p := StartProviderWith(t, 1, heldSettings(nil))
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	previous := running(t, backend, lease)
	tenant.Update(t, lease, gated(sleeper("7200")))
	held := requireHeld(t, backend, lease, "updating")
	require.NotEqual(t, previous.ID, held.ID, "an update starts a new container")

	backend.Kill()
	backend.Start()
	requireStillHeld(t, backend, lease, held)

	openGate(t, backend, held)
	settled := requireSettled(t, p, backend, lease, held)
	require.Equal(t, []string{"sleep", "7200"}, []string(settled.Config.Cmd), "the lease must run the updated workload")
	requireNothingLeft(t, tenant, backend, lease)
}

// TestCrash_BackendDuringRestart kills the backend while a restarted
// workload is started but not healthy. The restarted backend must finish the
// restart into the container it had started.
func TestCrash_BackendDuringRestart(t *testing.T) {
	p := StartProviderWith(t, 1, heldSettings(nil))
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease, first := holdCreate(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	openGate(t, backend, first)
	requireSettled(t, p, backend, lease, first)
	tenant.Restart(t, lease)
	held := requireHeld(t, backend, lease, "restarting")
	require.NotEqual(t, first.ID, held.ID, "a restart starts a new container")

	backend.Kill()
	backend.Start()
	requireStillHeld(t, backend, lease, held)

	openGate(t, backend, held)
	requireSettled(t, p, backend, lease, held)
	requireNothingLeft(t, tenant, backend, lease)
}

// holdClose closes a lease whose workload ignores SIGTERM, and waits until
// the backend is in the middle of stopping it.
func holdClose(t *testing.T, p *Provider, tenant *Tenant) (leaseUUID string) {
	t.Helper()
	backend := p.Backends[0]
	// sleep runs as the container's first process, which gets no default
	// signal handling: SIGTERM does nothing, and the stop lasts its grace.
	leaseUUID = deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	workload := running(t, backend, leaseUUID)
	tenant.Close(t, leaseUUID)
	eventually(t, "the close of lease "+leaseUUID+" is held while its workload stops", func() bool {
		lease, err := backend.Client.Get(t.Context(), leaseUUID)
		return err == nil && lease.Status == "deprovisioning"
	})
	containers := backend.Containers(t, leaseUUID)
	require.Len(t, containers, 1)
	require.Equal(t, workload.ID, containers[0].ID)
	require.True(t, containers[0].State.Running, "the workload must still be stopping: %s", containers[0].State.Status)
	return leaseUUID
}

// TestCrash_BackendDuringClose kills the backend while it stops a closed
// lease's workload. The restarted backend must finish the close by itself.
func TestCrash_BackendDuringClose(t *testing.T) {
	p := StartProviderWith(t, 1, heldSettings(map[string]any{"container_stop_timeout": stopGrace}))
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease := holdClose(t, p, tenant)

	backend.Kill()
	backend.Start()

	requireNothingLeft(t, tenant, backend, lease)
	p.AwaitSweeps(t, 2, SweepComplete)
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
	lease := holdClose(t, p, tenant)

	p.Kill()
	requireNothingLeft(t, tenant, backend, lease)
	p.Start()

	p.AwaitSweeps(t, 2, SweepComplete)
	require.Empty(t, backend.Leftovers(lease))
}
