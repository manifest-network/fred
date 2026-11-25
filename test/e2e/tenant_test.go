//go:build integration

package e2e

import (
	"errors"
	"fmt"
	"os"
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
//
// Each test was shown to fail against a docker-backend built without the
// guard its comment names (CONTRIBUTING.md, "Proving a regression test"). If
// you change a guard, run that proof again: a test that still passes with
// its guard removed is testing something else.

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

// ownerOf is the account and the group that own a host path, as "uid:gid".
func ownerOf(t *testing.T, path string) string {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	stat := info.Sys().(*syscall.Stat_t)
	return fmt.Sprintf("%d:%d", stat.Uid, stat.Gid)
}

// TestBackend_LeasesOfOneTenantAreIsolated runs the same two-service stack in
// two leases of one tenant. Each lease must have a network of its own: a
// service name resolves to the lease's own container only, the other lease's
// container cannot be named at all, and a lease's network is removed after
// its close (ENG-1516).
//
// Guard: the lease's own network, internal/backend/docker/lease_network.go.
// Proof: with every lease given the same network, the test fails on the two
// leases sharing it.
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

// TestBackend_TenantCannotChangeProjectID has a tenant try to take its own
// data out of its volume's XFS project. A volume's quota counts the files of
// its project, so a tenant that could retag a file would put its usage on
// another volume's quota, or on none. The kernel lets a file's owner do it,
// with no capability: the backend's seccomp profile is what refuses it. Each
// attempt must fail and change nothing (ENG-1118).
//
// There are two ways out. One is to move a file to another project. The
// other is through a directory: it hands its project to what is created in
// it, until it is told not to, and there are two commands to tell it. The
// image's probe issues one changing command per run, so each answer is that
// command's. The chattr tool rewrites a file's flags before it changes the
// project, which hides the answer to the change, and has no letter for what
// a directory hands down.
//
// Guard: the FS_IOC_FSSETXATTR and FS_IOC_SETFLAGS entries of
// deniedIoctlCubes, internal/backend/docker/tenantseccomp/profile_linux.go.
// Proof: without the first the probe prints "changed" for the project;
// without the second, for the flag.
func TestBackend_TenantCannotChangeProjectID(t *testing.T) {
	b := StartBackend(t)
	lease := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatefulSKU, Manifest: sleeper(ProjectProbeImage)}
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	c := requireOneRunningContainer(t, b, lease.LeaseUUID)
	volume := volumeSource(t, c, "/data")

	b.Exec(t, c.ID, "sh", "-c", "echo data > /data/file")
	hostFile := filepath.Join(volume, "file")
	before := ProjectID(t, hostFile)
	require.NotZero(t, before, "a file in a stateful volume is charged to the volume's project")

	// To another project, and to none, which no quota counts.
	for _, project := range []int{before + 1, 0} {
		exitCode, out := b.TryExec(t, c.ID, "/probe", "project", "/data/file", strconv.Itoa(project))
		require.NotZero(t, exitCode, "the tenant must not be able to move its file to project %d: %s", project, out)
		require.Contains(t, out, "change project: operation not permitted", "the change itself must be what is refused")
		require.Equal(t, before, ProjectID(t, hostFile), "after the attempt to move the file to project %d", project)
	}

	for _, command := range []string{"attribute", "flag"} {
		dir := "by-" + command
		b.Exec(t, c.ID, "mkdir", "/data/"+dir)
		exitCode, out := b.TryExec(t, c.ID, "/probe", command, "/data/"+dir)
		require.NotZero(t, exitCode, "the tenant must not be able to stop its directory handing its project down (%s): %s", command, out)
		require.Contains(t, out, "clear "+command+": operation not permitted", "the change itself must be what is refused")
		b.Exec(t, c.ID, "sh", "-c", "echo data > /data/"+dir+"/child")
		require.Equal(t, before, ProjectID(t, filepath.Join(volume, dir)), "the directory must still belong to the volume's project")
		require.Equal(t, before, ProjectID(t, filepath.Join(volume, dir, "child")), "a file created in the directory afterwards must be charged to the volume's project")
	}
}

// TestBackend_RootContainerWritesItsVolume runs a workload as root on a
// stateful volume. Tenant containers hold no capability and the backend is
// not root, so root in the container can write its volume only if the
// backend handed the directory to root. A directory an earlier deploy left
// owned by the backend's own account must be handed over on the next deploy
// (ENG-2091).
//
// Guard: the unconditional Lchown in buildStatefulVolumeBindsContext,
// internal/backend/docker/provision.go. Proof: skipped for root, the
// directory stays owned by the backend's account.
func TestBackend_RootContainerWritesItsVolume(t *testing.T) {
	b := StartBackend(t)
	// The image sets no user and its volume directories belong to root, so
	// the backend runs it as root. The redis image would run as its own
	// account.
	lease := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatefulSKU, Manifest: sleeper(NestedVolumesImage)}
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	first := requireOneRunningContainer(t, b, lease.LeaseUUID)
	require.Equal(t, "0\n", b.Exec(t, first.ID, "id", "-u"), "the workload must be running as root")
	source := volumeSource(t, first, "/data/b")
	require.Equal(t, "0:0", ownerOf(t, source), "the volume directory must belong to the container's user and group")
	b.Exec(t, first.ID, "touch", "/data/b/first")
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
	require.Equal(t, source, volumeSource(t, second, "/data/b"))
	require.Equal(t, "0:0", ownerOf(t, source), "the next deploy must hand the directory back to root, group included")
	b.Exec(t, second.ID, "touch", "/data/b/second")
	require.FileExists(t, filepath.Join(source, "first"), "the data of the first deploy must still be there")
}

// TestBackend_SymlinkedVolumeDirectoryIsRefused replaces the directory behind
// a lease's volume mount with a symlink to another directory of the same
// volume, then deploys the lease again. A tenant reaches that state from
// inside its container when a later image declares a volume at a path its
// earlier data made a link. Docker follows the link when it mounts, so the
// backend must refuse before it asks Docker, start nothing, and leave the
// tenant's data and the link as they are (ENG-539, ENG-795).
//
// Guard: the leaf-symlink check in buildStatefulVolumeBindsContext,
// internal/backend/docker/provision.go. Proof: without it the refusal
// counter never moves.
func TestBackend_SymlinkedVolumeDirectoryIsRefused(t *testing.T) {
	b := StartBackend(t)
	lease := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatefulSKU, Manifest: sleeper(RedisImage)}
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	c := requireOneRunningContainer(t, b, lease.LeaseUUID)
	source := volumeSource(t, c, "/data")
	// The healthy mount is a real directory, so the refusal below is about
	// the link and not about every deploy.
	info, err := os.Lstat(source)
	require.NoError(t, err)
	require.True(t, info.IsDir(), "the healthy volume directory must be a real directory")
	b.Exec(t, c.ID, "sh", "-c", "echo kept > /data/sentinel")

	b.KillContainers(t, lease.LeaseUUID)
	requireStatus(t, b, lease.LeaseUUID, statusFailed)
	moved := filepath.Join(filepath.Dir(source), "realdata")
	require.NoError(t, os.Rename(source, moved))
	// Relative and inside the volume. A link that leaves the volume is
	// refused by an older, separate check.
	require.NoError(t, os.Symlink("realdata", source))

	const refusals = "fred_docker_backend_volume_bind_symlink_rejected_total"
	refusedBefore := b.Metric(t, refusals)
	applied := time.Now()
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Greater(collect, b.metricOrZero(refusals), refusedBefore, "the backend must refuse the linked directory")
	}, 2*time.Minute, time.Second)

	// The lease has no container: not a running one, and not one that was
	// created on the linked directory and left stopped.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		containers, err := b.TryContainers(lease.LeaseUUID)
		if assert.NoError(collect, err) {
			assert.Empty(collect, containers, "no container may be left on a refused volume")
		}
	}, 30*time.Second, 500*time.Millisecond)
	for _, created := range b.Created(t, applied) {
		require.True(t, InspectionHelper(created), "the backend must refuse before it asks Docker for the workload's container: %s", created.Attributes["name"])
	}
	require.Equal(t, refusedBefore+1, b.Metric(t, refusals), "one deploy is one refusal")
	requireUntouched := func(when string) {
		t.Helper()
		kept, err := os.ReadFile(filepath.Join(moved, "sentinel"))
		require.NoError(t, err, when)
		require.Equal(t, "kept\n", string(kept), "the tenant's data must be untouched %s", when)
		info, err := os.Lstat(source)
		require.NoError(t, err, when)
		require.NotZero(t, info.Mode()&os.ModeSymlink, "the backend refuses the link; it does not remove it (%s)", when)
	}
	requireUntouched("after the refusal")

	// The refusal is about this lease. The backend goes on serving: it runs
	// another tenant's lease. After a restart it reports this one failed,
	// and accepts its close. A running backend may take its deploy timeout
	// to settle a deploy that ended this way.
	other := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-b", SKU: StatefulSKU, Manifest: sleeper(RedisImage)}
	require.NoError(t, b.Client.Apply(t.Context(), other))
	requireStatus(t, b, other.LeaseUUID, statusReady)
	b.Stop()
	b.Start()
	requireStatus(t, b, lease.LeaseUUID, statusFailed)
	requireStatus(t, b, other.LeaseUUID, statusReady)
	requireUntouched("after a restart")
	closeLease(t, b, lease.LeaseUUID)
}

// TestBackend_LinkedVolumeDoesNotBlockOtherLeases has one tenant arrange links
// inside its own volume so that the path Docker reports for one of its mounts
// no longer resolves. Before a launch, the backend looks at every container
// that could write to the volume it is about to use. That look must not fail
// on another lease's unresolvable path: one tenant would stop every stateful
// launch on the backend (ENG-2181).
//
// Guard: volumeWriterMountEvidence, internal/backend/docker/volume_launch.go.
// Proof: with a writer's source resolved whole, the other tenant's lease
// fails.
func TestBackend_LinkedVolumeDoesNotBlockOtherLeases(t *testing.T) {
	b := StartBackend(t)
	linked := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatefulSKU, Manifest: sleeper(RedisImage)}
	require.NoError(t, b.Client.Apply(t.Context(), linked))
	requireStatus(t, b, linked.LeaseUUID, statusReady)
	first := requireOneRunningContainer(t, b, linked.LeaseUUID)
	// The first deploy has one volume, /data. In it the tenant makes a a link
	// to b/l, and b/l a link back to b.
	b.Exec(t, first.ID, "sh", "-c", "mkdir /data/b && ln -s . /data/b/l && ln -s b/l /data/a")

	// The next deploy's image declares volumes at /data/b and /data/a/c. The
	// second is created through the tenant's links.
	b.KillContainers(t, linked.LeaseUUID)
	requireStatus(t, b, linked.LeaseUUID, statusFailed)
	linked.Manifest = sleeper(NestedVolumesImage)
	require.NoError(t, b.Client.Apply(t.Context(), linked))
	requireStatus(t, b, linked.LeaseUUID, statusReady)
	second := requireOneRunningContainer(t, b, linked.LeaseUUID)
	through := volumeSource(t, second, "/data/a/c")
	_, err := filepath.EvalSymlinks(through)
	require.NoError(t, err, "the mount's source resolves until the tenant changes its link")

	// The running workload turns its link into a loop. Its mounts do not
	// move; only the path Docker reports for one of them stops resolving.
	b.Exec(t, second.ID, "ln", "-sfn", "l", "/data/b/l")
	_, err = filepath.EvalSymlinks(through)
	require.Error(t, err, "the reported source must no longer resolve, or the test shows nothing")

	other := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-b", SKU: StatefulSKU, Manifest: sleeper(RedisImage)}
	require.NoError(t, b.Client.Apply(t.Context(), other))
	requireStatus(t, b, other.LeaseUUID, statusReady)
	requireVolumeBind(t, b, requireOneRunningContainer(t, b, other.LeaseUUID), "/data")
}

// TestBackend_CloseDuringRestoreKeepsTheData closes a restore's destination
// the moment the restore is requested. Whether the close lands before or
// after the data changed hands, the data must end up held by exactly one of
// the two leases, intact, and restorable (ENG-658).
//
// Guard: ensureCommittedRestoreDestinationForClose,
// internal/backend/docker/deprovision.go. Proof: without it the test fails
// because the close is never accepted: a later check answers that the
// destination is not durably committed, for good. The proof therefore shows
// that the test depends on the guard, not that data is lost without it.
func TestBackend_CloseDuringRestoreKeepsTheData(t *testing.T) {
	b := StartBackend(t)
	ctx := t.Context()
	source := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatefulSKU, Manifest: sleeper(RedisImage)}
	require.NoError(t, b.Client.Apply(ctx, source))
	requireStatus(t, b, source.LeaseUUID, statusReady)
	b.Exec(t, requireOneRunningContainer(t, b, source.LeaseUUID).ID, "sh", "-c", "echo kept > /data/sentinel")
	closeLease(t, b, source.LeaseUUID)
	requireStatus(t, b, source.LeaseUUID, statusRetained)

	destination := Spec{LeaseUUID: uuid.NewString(), Tenant: source.Tenant, SKU: StatefulSKU}
	require.NoError(t, b.Client.Restore(ctx, destination, source.LeaseUUID))
	closeLease(t, b, destination.LeaseUUID)

	// retained reports whether the backend holds retained data for a lease,
	// and whether it could tell.
	retained := func(leaseUUID string) (holds, known bool) {
		lease, err := b.Client.Get(ctx, leaseUUID)
		switch {
		case err == nil:
			return lease.Status == statusRetained, lease.Status == statusRetained
		case errors.Is(err, ErrNotFound):
			return false, true
		default:
			return false, false
		}
	}
	var holder string
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		sourceHolds, sourceKnown := retained(source.LeaseUUID)
		destinationHolds, destinationKnown := retained(destination.LeaseUUID)
		if !assert.True(collect, sourceKnown && destinationKnown, "both leases must settle as retained or gone") {
			return
		}
		if !assert.NotEqual(collect, sourceHolds, destinationHolds, "exactly one lease must hold the data") {
			return
		}
		holder = source.LeaseUUID
		if destinationHolds {
			holder = destination.LeaseUUID
		}
	}, 3*time.Minute, time.Second)
	require.Empty(t, b.Containers(t, destination.LeaseUUID))

	final := Spec{LeaseUUID: uuid.NewString(), Tenant: source.Tenant, SKU: StatefulSKU}
	require.NoError(t, b.Client.Restore(ctx, final, holder))
	requireStatus(t, b, final.LeaseUUID, statusReady)
	require.Equal(t, "kept\n", b.Exec(t, requireOneRunningContainer(t, b, final.LeaseUUID).ID, "cat", "/data/sentinel"))
}
