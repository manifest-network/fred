//go:build integration

package e2e

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file are regression tests for bugs a tenant could reach.
// Each names its ticket and pins the outcome from outside: what the tenant's
// own container can do, and what the host shows afterwards.

const statusFailed = "failed"

// volumeSource returns the host directory the container mounts at destination.
func volumeSource(t *testing.T, c container.InspectResponse, destination string) string {
	t.Helper()
	for _, mount := range c.Mounts {
		if mount.Destination == destination {
			return mount.Source
		}
	}
	require.Failf(t, "mount missing", "container %s has no mount at %s", c.ID, destination)
	return ""
}

// projectID reads the XFS project a host file is charged to.
func projectID(t *testing.T, path string) int {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), "xfs_io", "-r", "-c", "lsproj", path).CombinedOutput()
	require.NoError(t, err, "xfs_io lsproj %s: %s", path, out)
	_, value, found := strings.Cut(strings.TrimSpace(string(out)), "=")
	require.True(t, found, "unexpected xfs_io output: %s", out)
	id, err := strconv.Atoi(strings.TrimSpace(value))
	require.NoError(t, err, "unexpected xfs_io output: %s", out)
	return id
}

// ownerUID is the account that owns a host path.
func ownerUID(t *testing.T, path string) uint32 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Sys().(*syscall.Stat_t).Uid
}

// TestBackend_LeasesOfOneTenantAreIsolated runs the same two-service stack in
// two leases of one tenant. Each lease must have a network of its own: a
// service name resolves to the lease's own container only, the other lease's
// container cannot be named at all, and a lease's network is removed after
// its close (ENG-1516).
func TestBackend_LeasesOfOneTenantAreIsolated(t *testing.T) {
	b := StartBackend(t)
	ctx := t.Context()
	stack := map[string]any{"services": map[string]any{
		"web": sleeper(BusyboxImage),
		"db":  sleeper(BusyboxImage),
	}}
	newLease := func() Spec {
		return Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatelessSKU, Services: []string{"web", "db"}, Manifest: stack}
	}
	leaseA, leaseB := newLease(), newLease()
	require.NoError(t, b.Client.Apply(ctx, leaseA))
	require.NoError(t, b.Client.Apply(ctx, leaseB))
	requireStatus(t, b, leaseA.LeaseUUID, statusReady)
	requireStatus(t, b, leaseB.LeaseUUID, statusReady)

	// service returns a lease's container for a service, the one network it
	// is attached to, and its address there.
	service := func(lease Spec, name string) (c container.InspectResponse, networkName, address string) {
		t.Helper()
		for _, one := range b.Containers(t, lease.LeaseUUID) {
			if one.Config.Labels[labelServiceName] != name {
				continue
			}
			require.Len(t, one.NetworkSettings.Networks, 1, "service %s of lease %s must be on one network", name, lease.LeaseUUID)
			for networkName, endpoint := range one.NetworkSettings.Networks {
				return one, networkName, endpoint.IPAddress
			}
		}
		require.Failf(t, "service missing", "lease %s has no %s container", lease.LeaseUUID, name)
		return container.InspectResponse{}, "", ""
	}
	webA, networkA, _ := service(leaseA, "web")
	dbA, dbNetworkA, dbAddressA := service(leaseA, "db")
	dbB, networkB, dbAddressB := service(leaseB, "db")
	require.Equal(t, networkA, dbNetworkA, "the services of one lease share its network")
	require.NotEqual(t, networkA, networkB, "two leases of one tenant must not share a network")
	require.NotEmpty(t, dbAddressA)
	require.NotEmpty(t, dbAddressB)

	// Names are queried absolute, with a trailing dot: busybox nslookup
	// otherwise appends the host's search domain and never asks Docker's
	// resolver for the bare name.
	resolved := b.Exec(t, webA.ID, "nslookup", "db.")
	require.Contains(t, resolved, dbAddressA)
	require.NotContains(t, resolved, dbAddressB, "the other lease's service must not resolve")

	// A container name resolves inside its own lease, so the refusal below is
	// not a resolver that answers nothing.
	b.Exec(t, webA.ID, "nslookup", strings.TrimPrefix(dbA.Name, "/")+".")
	exitCode, out := b.TryExec(t, webA.ID, "nslookup", strings.TrimPrefix(dbB.Name, "/")+".")
	require.NotZero(t, exitCode, "the other lease's container name must not resolve: %s", out)

	closeLease(t, b, leaseA.LeaseUUID)
	closeLease(t, b, leaseB.LeaseUUID)
	// The backend's periodic pass removes a closed lease's network.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		for _, lease := range []Spec{leaseA, leaseB} {
			assert.Empty(collect, b.networkNames(lease.LeaseUUID), "the network of lease %s must be removed", lease.LeaseUUID)
		}
	}, 2*time.Minute, time.Second)
}

// TestBackend_TenantCannotChangeProjectID has a tenant try to move one of its
// own files to another XFS project. A volume's quota counts the files of its
// project, so a tenant that could retag a file would put its usage on another
// volume's quota. The attempt must be refused, and the file must stay where
// it was (ENG-1118).
func TestBackend_TenantCannotChangeProjectID(t *testing.T) {
	b := StartBackend(t)
	lease := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatefulSKU, Manifest: sleeper(RedisImage)}
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	c := requireOneRunningContainer(t, b, lease.LeaseUUID)

	b.Exec(t, c.ID, "sh", "-c", "echo data > /data/file")
	hostFile := filepath.Join(volumeSource(t, c, "/data"), "file")
	before := projectID(t, hostFile)
	require.NotZero(t, before, "a file in a stateful volume is charged to the volume's project")

	exitCode, out := b.TryExec(t, c.ID, "chattr", "-p", strconv.Itoa(before+1), "/data/file")
	require.NotZero(t, exitCode, "the tenant must not be able to change the project of its file: %s", out)
	require.Contains(t, out, "Operation not permitted", "the change must be refused, not unsupported")
	require.Equal(t, before, projectID(t, hostFile))
}

// TestBackend_RootContainerWritesItsVolume runs a workload as root on a
// stateful volume. Tenant containers hold no capability and the backend is
// not root, so root in the container can write its volume only if the
// backend handed the directory to root. A directory an earlier deploy left
// owned by the backend's own account must be handed over on the next deploy
// (ENG-2091).
func TestBackend_RootContainerWritesItsVolume(t *testing.T) {
	b := StartBackend(t)
	lease := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatefulSKU, Manifest: sleeper(RedisImage)}
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	first := requireOneRunningContainer(t, b, lease.LeaseUUID)
	require.Equal(t, "0\n", b.Exec(t, first.ID, "id", "-u"), "the workload must be running as root")
	source := volumeSource(t, first, "/data")
	require.Zero(t, ownerUID(t, source), "the volume directory must belong to the container's user")
	b.Exec(t, first.ID, "touch", "/data/first")
	require.FileExists(t, filepath.Join(source, "first"))

	// The workload dies, and the directory is found owned by the backend's
	// account, as releases before the fix left it.
	b.KillContainers(t, lease.LeaseUUID)
	requireStatus(t, b, lease.LeaseUUID, statusFailed)
	require.NoError(t, os.Chown(source, serviceUID, serviceGID))

	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	second := requireOneRunningContainer(t, b, lease.LeaseUUID)
	require.NotEqual(t, first.ID, second.ID)
	require.Equal(t, source, volumeSource(t, second, "/data"))
	require.Zero(t, ownerUID(t, source), "the next deploy must hand the directory back to root")
	b.Exec(t, second.ID, "touch", "/data/second")
	require.FileExists(t, filepath.Join(source, "first"), "the data of the first deploy must still be there")
}
