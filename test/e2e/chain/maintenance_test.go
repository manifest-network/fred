//go:build integration

package chain

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/test/e2e"
)

// The tests in this file restart and update leases the way a tenant does:
// through the provider's API, on a lease that is active on the chain. A
// restart replaces a lease's containers and keeps its manifest; an update
// replaces them with those of a new manifest. Either way the lease keeps its
// data, and one that cannot be completed leaves the lease on the workload it
// had. What an interrupted restart or update does is in crash_test.go,
// lost_response_test.go and stale_test.go.

// imageReferenceLabel is the label docker-backend puts on a container with
// the image its manifest names.
const imageReferenceLabel = "fred.image_reference"

// What a backend reports for a replacement whose image cannot be pulled.
const (
	reasonImagePullFailed  = "ImagePullFailed"
	messageImagePullFailed = "image pull failed"
)

// runs is a manifest that runs an image doing nothing.
func runs(image string) map[string]any {
	return map[string]any{"image": image, "command": []string{"sleep", "3600"}}
}

// says is a manifest whose workload writes a line to its log and then does
// nothing, as the image's own user.
func says(image, line string) map[string]any {
	return map[string]any{"image": image, "command": []string{"sh", "-c", "echo " + line + "; exec sleep 3600"}}
}

// replacedAll waits until the backend reports the lease ready in the given
// number of running containers, none of which is one of before. It returns
// them.
func replacedAll(t *testing.T, backend *e2e.Backend, leaseUUID string, before []container.InspectResponse, count int) []container.InspectResponse {
	t.Helper()
	old := map[string]bool{}
	for _, one := range before {
		old[one.ID] = true
	}
	var now []container.InspectResponse
	eventually(t, "backend "+backend.Name+" runs lease "+leaseUUID+" in new containers", func() bool {
		now = backend.Containers(t, leaseUUID)
		if len(now) != count {
			return false
		}
		for _, one := range now {
			if old[one.ID] || !one.State.Running {
				return false
			}
		}
		lease, err := backend.Client.Get(t.Context(), leaseUUID)
		return err == nil && lease.Status == "ready"
	})
	return now
}

// requireImage asserts that a container runs the image a manifest names.
func requireImage(t *testing.T, c container.InspectResponse, image string) {
	t.Helper()
	require.Equal(t, image, c.Config.Labels[imageReferenceLabel], "container %s must run the manifest's image", c.Name)
}

// release is one entry of a lease's release history, as a tenant reads it.
type release struct {
	Version  int    `json:"version"`
	Status   string `json:"status"`
	Reason   string `json:"reason"`
	Message  string `json:"message"`
	Manifest []byte `json:"manifest"`
}

// image is the image the release's manifest names for its one workload.
func (r release) image(t *testing.T) string {
	t.Helper()
	var manifest struct {
		Image    string `json:"image"`
		Services map[string]struct {
			Image string `json:"image"`
		} `json:"services"`
	}
	require.NoError(t, json.Unmarshal(r.Manifest, &manifest), "manifest of release %d: %s", r.Version, r.Manifest)
	if manifest.Image != "" {
		return manifest.Image
	}
	require.Len(t, manifest.Services, 1, "manifest of release %d: %s", r.Version, r.Manifest)
	for _, service := range manifest.Services {
		return service.Image
	}
	return ""
}

// readReleases reads a lease's release history as its tenant. It returns nil
// while the provider has none to give.
func readReleases(t *testing.T, tenant *Tenant, leaseUUID string) []release {
	t.Helper()
	var answer struct {
		Releases []release `json:"releases"`
	}
	if tenant.Read(t, leaseUUID, "releases", &answer) != http.StatusOK {
		return nil
	}
	return answer.Releases
}

// awaitFailedRelease waits until the lease's history ends in a failed
// release, which is how a tenant learns that a restart or an update did not
// go through. It returns the history.
func awaitFailedRelease(t *testing.T, tenant *Tenant, leaseUUID string) []release {
	t.Helper()
	var history []release
	eventually(t, "the last release of lease "+leaseUUID+" is failed", func() bool {
		history = readReleases(t, tenant, leaseUUID)
		return len(history) > 0 && history[len(history)-1].Status == "failed"
	})
	return history
}

// connection is where a lease can be reached, as a tenant reads it.
type connection struct {
	Connection struct {
		Host     string `json:"host"`
		Services map[string]struct {
			Instances []struct {
				Status string `json:"status"`
			} `json:"instances"`
		} `json:"services"`
	} `json:"connection"`
}

// readLogs reads the last lines of a lease's logs as its tenant, by
// "service/instance". It waits for the provider to have them.
func readLogs(t *testing.T, tenant *Tenant, leaseUUID string) map[string]string {
	t.Helper()
	var answer struct {
		Logs map[string]string `json:"logs"`
	}
	eventually(t, "the provider serves the logs of lease "+leaseUUID, func() bool {
		return tenant.Read(t, leaseUUID, "logs?tail=100", &answer) == http.StatusOK
	})
	return answer.Logs
}

// TestMaintenance_RestartAndUpdateKeepTheData restarts a lease with a volume
// and then updates it to another image. Each must replace the container, and
// the new container must find the data the old one wrote, in the same
// directory on the host.
func TestMaintenance_RestartAndUpdateKeepTheData(t *testing.T) {
	p := StartProvider(t, 1)
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateful, runs(e2e.RedisImage))
	describeOnFailure(t, backend, lease)
	first := running(t, backend, lease)
	backend.Exec(t, first.ID, "sh", "-c", "echo kept > /data/sentinel")
	volume := inode(t, dataDir(t, first))
	requireData := func(now container.InspectResponse, after string) {
		t.Helper()
		require.Equal(t, "kept\n", backend.Exec(t, now.ID, "cat", "/data/sentinel"), "after %s", after)
		require.Equal(t, dataDir(t, first), dataDir(t, now), "after %s the lease must mount the same directory", after)
		require.Equal(t, volume, inode(t, dataDir(t, now)), "after %s the volume must be the same directory, not a copy", after)
	}

	var reachable connection
	require.Equal(t, http.StatusOK, tenant.Read(t, lease, "connection", &reachable))
	require.Equal(t, "127.0.0.1", reachable.Connection.Host)

	tenant.Restart(t, lease)
	restarted := replaced(t, backend, lease, first)
	requireImage(t, restarted, e2e.RedisImage)
	require.Equal(t, first.Image, restarted.Image, "a restart runs the same image")
	require.Equal(t, command(first), command(restarted))
	requireData(restarted, "a restart")

	tenant.Update(t, lease, runs(e2e.RedisAlpineImage))
	updated := replaced(t, backend, lease, restarted)
	requireImage(t, updated, e2e.RedisAlpineImage)
	require.NotEqual(t, restarted.Image, updated.Image, "an update to another image runs that image")
	requireData(updated, "an update")
	require.Equal(t, LeaseActive, p.Chain.Lease(t, lease).State)
}

// TestMaintenance_ReleaseHistory takes a lease through an update, a restart
// and an update back to its first manifest, and reads the history a tenant
// is given: one release per deploy, restart or update, numbered in order,
// each with the manifest it ran, and only the last one active. Once the lease
// is closed there is no history to read.
func TestMaintenance_ReleaseHistory(t *testing.T) {
	p := StartProvider(t, 1)
	backend, tenant := p.Backends[0], p.NewTenant(t)
	original, other := runs(e2e.BusyboxImage), runs(e2e.RedisImage)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, original)
	describeOnFailure(t, backend, lease)
	now := running(t, backend, lease)

	tenant.Update(t, lease, other)
	now = replaced(t, backend, lease, now)
	requireImage(t, now, e2e.RedisImage)
	tenant.Restart(t, lease)
	now = replaced(t, backend, lease, now)
	requireImage(t, now, e2e.RedisImage)
	tenant.Update(t, lease, original)
	now = replaced(t, backend, lease, now)
	requireImage(t, now, e2e.BusyboxImage)

	history := readReleases(t, tenant, lease)
	require.Len(t, history, 4, "a deploy, an update, a restart and an update: %+v", history)
	images := []string{e2e.BusyboxImage, e2e.RedisImage, e2e.RedisImage, e2e.BusyboxImage}
	for i, entry := range history {
		require.Equal(t, i+1, entry.Version, "releases are numbered in order")
		require.Equal(t, images[i], entry.image(t), "release %d", entry.Version)
		if i == len(history)-1 {
			require.Equal(t, "active", entry.Status, "the last release is the one running")
		} else {
			require.Equal(t, "superseded", entry.Status, "release %d was replaced", entry.Version)
		}
	}

	tenant.Close(t, lease)
	requireGone(t, backend, lease)
	require.NotEqual(t, http.StatusOK, tenant.Read(t, lease, "releases", nil), "a closed lease has no release history")
}

// TestMaintenance_EveryInstanceIsReplaced restarts and updates a lease for
// two units of a SKU. Each must replace both instances.
func TestMaintenance_EveryInstanceIsReplaced(t *testing.T) {
	p := StartProvider(t, 1)
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease := tenant.DeployItems(t, runs(e2e.BusyboxImage), p.SKUs.Stateless+":2")
	requireActive(t, p.Chain, lease)
	describeOnFailure(t, backend, lease)
	first := replacedAll(t, backend, lease, nil, 2)

	tenant.Restart(t, lease)
	restarted := replacedAll(t, backend, lease, first, 2)
	for _, one := range restarted {
		requireImage(t, one, e2e.BusyboxImage)
	}

	tenant.Update(t, lease, runs(e2e.RedisImage))
	for _, one := range replacedAll(t, backend, lease, restarted, 2) {
		requireImage(t, one, e2e.RedisImage)
	}
	require.Equal(t, LeaseActive, p.Chain.Lease(t, lease).State)
}

// TestMaintenance_Stack runs a stack of two services through a restart, an
// update and a close. The tenant must be able to reach and read each service,
// a restart and an update must replace the containers of both, and a close
// must leave nothing.
func TestMaintenance_Stack(t *testing.T) {
	p := StartProvider(t, 1)
	backend, tenant := p.Backends[0], p.NewTenant(t)
	stack := func(image string) map[string]any {
		return map[string]any{"services": map[string]any{
			"web": says(image, "web-is-up"),
			"db":  says(image, "db-is-up"),
		}}
	}
	lease := tenant.DeployItems(t, stack(e2e.BusyboxImage), p.SKUs.Stateless+":1:web", p.SKUs.Stateless+":1:db")
	requireActive(t, p.Chain, lease)
	describeOnFailure(t, backend, lease)
	first := replacedAll(t, backend, lease, nil, 2)

	var reachable connection
	require.Equal(t, http.StatusOK, tenant.Read(t, lease, "connection", &reachable))
	require.Equal(t, "127.0.0.1", reachable.Connection.Host)
	require.Len(t, reachable.Connection.Services, 2, "connection: %+v", reachable)
	for _, service := range []string{"web", "db"} {
		instances := reachable.Connection.Services[service].Instances
		require.Len(t, instances, 1, "service %s", service)
		require.Equal(t, "running", instances[0].Status, "service %s", service)
	}
	logs := readLogs(t, tenant, lease)
	require.Contains(t, logs["web/0"], "web-is-up", "logs: %v", logs)
	require.Contains(t, logs["db/0"], "db-is-up", "logs: %v", logs)

	tenant.Restart(t, lease)
	restarted := replacedAll(t, backend, lease, first, 2)
	for _, one := range restarted {
		requireImage(t, one, e2e.BusyboxImage)
	}

	tenant.Update(t, lease, stack(e2e.RedisImage))
	for _, one := range replacedAll(t, backend, lease, restarted, 2) {
		requireImage(t, one, e2e.RedisImage)
	}
	require.Equal(t, LeaseActive, p.Chain.Lease(t, lease).State)

	tenant.Close(t, lease)
	requireGone(t, backend, lease)
	require.NotEqual(t, http.StatusOK, tenant.Read(t, lease, "connection", nil), "a closed lease cannot be reached")
}

// TestMaintenance_FromAFailedLease restarts, and then updates, a lease whose
// container has died. A tenant does not have to wait for the provider to put
// a failed lease back: its own restart or update must be accepted and bring
// the lease up.
//
// The provider puts a failed lease back itself on its periodic pass. The test
// gives it an hour between passes, so that what brings the lease up here can
// only be the tenant's command.
func TestMaintenance_FromAFailedLease(t *testing.T) {
	p := StartProviderWith(t, 1, Options{ProviderSettings: map[string]any{"reconciliation_interval": "1h"}})
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, runs(e2e.BusyboxImage))
	describeOnFailure(t, backend, lease)
	first := running(t, backend, lease)

	backend.KillContainers(t, lease)
	requireStatus(t, backend, lease, "failed")
	tenant.Restart(t, lease)
	restarted := replaced(t, backend, lease, first)
	requireImage(t, restarted, e2e.BusyboxImage)

	backend.KillContainers(t, lease)
	requireStatus(t, backend, lease, "failed")
	tenant.Update(t, lease, runs(e2e.RedisImage))
	requireImage(t, replaced(t, backend, lease, restarted), e2e.RedisImage)
	require.Equal(t, LeaseActive, p.Chain.Lease(t, lease).State)
}

// TestMaintenance_UpdateToAnImageThatCannotBePulledChangesNothing updates a
// lease to an image that does not exist. The update fails before anything of
// the lease is replaced, so the lease must stay ready in the container it
// had, never restarted. The tenant reads why from the release history: a
// failed release with the manifest it asked for, after the active one, and
// the same after the provider has restarted (ENG-932, ENG-938).
func TestMaintenance_UpdateToAnImageThatCannotBePulledChangesNothing(t *testing.T) {
	// Do not wait out the production pull timeout.
	p := StartProviderWith(t, 1, Options{BackendSettings: map[string]any{"image_pull_timeout": "15s"}})
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, runs(e2e.BusyboxImage))
	describeOnFailure(t, backend, lease)
	first := running(t, backend, lease)

	// A tag that does not exist, in a registry the backend allows.
	const missing = "busybox:fred-e2e-no-such-tag"
	tenant.Update(t, lease, runs(missing))
	requireUnchanged := func(when string) {
		t.Helper()
		history := awaitFailedRelease(t, tenant, lease)
		require.Len(t, history, 2, "%s: %+v", when, history)
		require.Equal(t, "active", history[0].Status, when)
		require.Equal(t, e2e.BusyboxImage, history[0].image(t), when)
		require.Equal(t, reasonImagePullFailed, history[1].Reason, when)
		require.Equal(t, messageImagePullFailed, history[1].Message, when)
		require.Equal(t, missing, history[1].image(t), when)
		requireStatus(t, backend, lease, "ready")
		requireUntouched(t, backend, lease, first)
		require.Equal(t, LeaseActive, p.Chain.Lease(t, lease).State, when)
	}
	requireUnchanged("after the failed update")

	p.Stop()
	p.Start()
	requireUnchanged("after the provider restarted")
}

// TestMaintenance_UnhealthyReplacementReturnsThePreviousWorkload updates a
// lease to a manifest whose workload starts and never passes its health
// check. By then the lease's old container is gone: it was stopped before the
// new one started. The backend must put the previous workload back as it was,
// on the same data, and report the lease ready. The tenant reads what
// happened from the release history and from the logs, which keep the failed
// workload's output (ENG-932).
//
// It runs once for a lease without a volume and once for one with.
func TestMaintenance_UnhealthyReplacementReturnsThePreviousWorkload(t *testing.T) {
	p := StartProvider(t, 1)
	backend, tenant := p.Backends[0], p.NewTenant(t)
	source := says(e2e.RedisImage, "source-is-up")
	source["env"] = map[string]string{"ROLE": "source"}
	source["user"] = "999:999"
	target := says(e2e.RedisAlpineImage, "target-is-up")
	target["env"] = map[string]string{"ROLE": "target"}
	target["user"] = "999:999"
	target["health_check"] = map[string]any{"test": []string{"CMD", "false"}, "interval": "1s", "timeout": "1s", "retries": 1}

	for name, sku := range map[string]string{"without a volume": p.SKUs.Stateless, "with a volume": p.SKUs.Stateful} {
		t.Run(name, func(t *testing.T) {
			stateful := sku == p.SKUs.Stateful
			lease := deployActive(t, p, tenant, sku, source)
			describeOnFailure(t, backend, lease)
			before := running(t, backend, lease)
			var volume uint64
			if stateful {
				backend.Exec(t, before.ID, "sh", "-c", "echo kept > /data/sentinel")
				volume = inode(t, dataDir(t, before))
			}

			tenant.Update(t, lease, target)
			history := awaitFailedRelease(t, tenant, lease)
			require.Len(t, history, 2, "%+v", history)
			require.Equal(t, "active", history[0].Status)
			require.Equal(t, e2e.RedisImage, history[0].image(t))
			require.Equal(t, e2e.RedisAlpineImage, history[1].image(t))

			// The previous workload, in a container of its own.
			restored := replaced(t, backend, lease, before)
			requireImage(t, restored, e2e.RedisImage)
			require.Equal(t, before.Image, restored.Image)
			require.Equal(t, command(before), command(restored))
			require.Equal(t, before.Config.User, restored.Config.User)
			require.Equal(t, before.Config.Env, restored.Config.Env)
			require.Equal(t, before.Config.Healthcheck, restored.Config.Healthcheck)
			require.Equal(t, before.HostConfig.ReadonlyRootfs, restored.HostConfig.ReadonlyRootfs)
			require.Equal(t, before.HostConfig.Resources, restored.HostConfig.Resources)
			require.Equal(t, before.HostConfig.CapDrop, restored.HostConfig.CapDrop)
			require.Equal(t, before.HostConfig.SecurityOpt, restored.HostConfig.SecurityOpt)
			if stateful {
				require.Equal(t, dataDir(t, before), dataDir(t, restored))
				require.Equal(t, volume, inode(t, dataDir(t, restored)), "the volume must be the same directory, not a copy")
				require.Equal(t, "kept\n", backend.Exec(t, restored.ID, "cat", "/data/sentinel"))
			}

			logs := readLogs(t, tenant, lease)
			require.Contains(t, logs["app/0"], "source-is-up", "logs: %v", logs)
			var failedOutput []string
			for key, lines := range logs {
				if strings.HasPrefix(key, "failed/") {
					failedOutput = append(failedOutput, lines)
				}
			}
			require.Contains(t, strings.Join(failedOutput, "\n"), "target-is-up", "the failed workload's output must stay readable: %v", logs)
			require.Equal(t, LeaseActive, p.Chain.Lease(t, lease).State)
		})
	}
}

// TestMaintenance_AFailedUpdateDoesNotStopTheBackendForOthers leaves one
// tenant's lease with a failed update behind it, and checks that the backend
// goes on working for everyone: its periodic passes succeed, it restarts,
// the lease is still on its previous workload afterwards, and another
// tenant's lease deploys.
//
// 0.14 keeps a record of a failed update that started something. This test
// does not reach the case that once stopped a whole backend, where the
// release that record names had since been dropped from a history grown past
// its size limit: that takes some thirty megabyte-sized updates of one lease.
// TestReleaseCompactionDropsAFailedMaintenanceTarget and
// TestFailedReceiptMissingItsTargetStaysLeaseLocal, in
// internal/backend/shared, pin that case.
func TestMaintenance_AFailedUpdateDoesNotStopTheBackendForOthers(t *testing.T) {
	p := StartProvider(t, 1)
	backend, tenant, other := p.Backends[0], p.NewTenant(t), p.NewTenant(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, runs(e2e.BusyboxImage))
	describeOnFailure(t, backend, lease)
	before := running(t, backend, lease)

	unhealthy := runs(e2e.RedisImage)
	unhealthy["health_check"] = map[string]any{"test": []string{"CMD", "false"}, "interval": "1s", "timeout": "1s", "retries": 1}
	tenant.Update(t, lease, unhealthy)
	require.Len(t, awaitFailedRelease(t, tenant, lease), 2)
	previous := replaced(t, backend, lease, before)
	requireImage(t, previous, e2e.BusyboxImage)

	const passes = "fred_docker_backend_reconciliation_total"
	succeeded := backend.Metric(t, passes, `outcome="success"`)
	eventually(t, "backend "+backend.Name+" completes two more periodic passes", func() bool {
		return backend.Metric(t, passes, `outcome="success"`)-succeeded >= 2
	})

	backend.Stop()
	backend.Start()
	requireStatus(t, backend, lease, "ready")
	requireUntouched(t, backend, lease, previous)

	neighbour := deployActive(t, p, other, p.SKUs.Stateless, runs(e2e.BusyboxImage))
	running(t, backend, neighbour)
	require.Equal(t, LeaseActive, p.Chain.Lease(t, lease).State)
}

// TestDeploy_FailedDeployStaysExplained deploys a workload that exits at
// once. The provider rejects the lease and has the backend remove it. The
// tenant must still be able to read why the deploy failed, and what the
// workload wrote before it died, also after the backend and the provider
// have each restarted (ENG-939).
func TestDeploy_FailedDeployStaysExplained(t *testing.T) {
	p := StartProvider(t, 1)
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease := tenant.Deploy(t, p.SKUs.Stateless, map[string]any{
		"image": e2e.BusyboxImage, "command": []string{"sh", "-c", "echo the-workload-said-why; exit 3"},
	})
	describeOnFailure(t, backend, lease)
	eventually(t, "the provider rejects lease "+lease, func() bool {
		onChain := p.Chain.Lease(t, lease)
		if onChain.State == LeaseRejected {
			t.Logf("the chain records the rejection as %q", onChain.RejectionReason)
			return true
		}
		require.Equal(t, LeasePending, onChain.State, "the lease left pending without being rejected: %+v", onChain)
		return false
	})
	eventually(t, "backend "+backend.Name+" runs nothing for lease "+lease, func() bool {
		return len(backend.Containers(t, lease)) == 0
	})

	requireExplained := func(when string) {
		t.Helper()
		var provision struct {
			Status  string `json:"status"`
			Reason  string `json:"reason"`
			Message string `json:"message"`
		}
		eventually(t, "the provider explains lease "+lease+" "+when, func() bool {
			return tenant.Read(t, lease, "provision", &provision) == http.StatusOK
		})
		require.Equal(t, "failed", provision.Status, when)
		require.Equal(t, "ContainerExited", provision.Reason, when)
		require.Equal(t, "container exited during startup", provision.Message, when)
		var output []string
		for _, lines := range readLogs(t, tenant, lease) {
			output = append(output, lines)
		}
		require.Contains(t, strings.Join(output, "\n"), "the-workload-said-why", "%s the workload's output must stay readable", when)
	}
	requireExplained("after the rejection")

	backend.Stop()
	backend.Start()
	requireExplained("after the backend restarted")

	p.Stop()
	p.Start()
	requireExplained("after the provider restarted")
}
