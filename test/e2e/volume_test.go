//go:build integration

package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file are about the disk a lease is given. A lease on a
// SKU with disk gets one directory per instance on the backend's XFS volume
// path, in an XFS project of its own that caps its bytes and its inodes. The
// image's volumes and writable paths are mounted from that directory.

// destroysOnClose are the settings of a backend that retains nothing: a
// close destroys the lease's volumes.
var destroysOnClose = map[string]any{"retain_on_close": false}

// redisServer is a manifest that runs redis on its volume. Redis saves only
// when asked, so a test decides what is on disk.
func redisServer() map[string]any {
	return map[string]any{"image": RedisImage, "command": []string{"redis-server", "--save", ""}}
}

// redis sends one command to the redis server in a container and returns its
// answer. A lease is ready a moment before its server accepts connections.
func redis(t *testing.T, b *Backend, containerID string, command ...string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		code, out := b.TryExec(t, containerID, append([]string{"redis-cli", "-e", "--raw"}, command...)...)
		if code == 0 {
			return strings.TrimSpace(out)
		}
		require.True(t, time.Now().Before(deadline), "redis-cli %v: %s", command, out)
		time.Sleep(500 * time.Millisecond)
	}
}

// volumeRootOf returns the lease's volume directory that the container
// mounts destination from.
func volumeRootOf(t *testing.T, b *Backend, c container.InspectResponse, destination string) string {
	t.Helper()
	source := volumeSource(t, c, destination)
	relative, err := filepath.Rel(b.VolumePath, source)
	require.NoError(t, err)
	require.False(t, strings.HasPrefix(relative, ".."), "%s is mounted from %s, outside %s", destination, source, b.VolumePath)
	return filepath.Join(b.VolumePath, strings.Split(relative, string(filepath.Separator))[0])
}

// requireNoQuotaLeft waits until the given projects have neither usage nor a
// limit, and the backend's storage carries the given number of quota entries.
func requireNoQuotaLeft(t *testing.T, b *Backend, entries int, projects ...int) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		report := b.quotaReport(t)
		for _, project := range projects {
			assert.Zero(collect, report[project], "the quota of project %d", project)
		}
		limited := 0
		for _, quota := range report {
			if quota.limitKB > 0 {
				limited++
			}
		}
		assert.Equal(collect, entries, limited, "quota entries on the backend's storage")
	}, time.Minute, time.Second)
}

// refusedForSpace reports whether a command's output says that the disk
// refused a write. XFS answers a project over its quota with either error,
// depending on the kernel.
func refusedForSpace(output string) bool {
	lower := strings.ToLower(output)
	return strings.Contains(lower, "quota") || strings.Contains(lower, "no space left")
}

// instanceOf is which instance of its service a container is, counted from
// zero.
func instanceOf(t *testing.T, c container.InspectResponse) int {
	t.Helper()
	index, err := strconv.Atoi(c.Config.Labels[labelInstanceIndex])
	require.NoError(t, err, "container %s carries no instance index", c.Name)
	return index
}

// TestBackend_VolumeLifecycle follows one volume from the deploy to the
// close of a lease that is not retained. The volume is a directory named
// after the lease, in a project that caps it at the SKU's disk. The
// workload's own writes land in it. After the close the directory is gone
// and so is the project's quota entry: an entry left behind slows every
// later quota scan on the host (ENG-459).
func TestBackend_VolumeLifecycle(t *testing.T) {
	b := StartBackendWithConfig(t, destroysOnClose)
	lease := stateful("tenant-a")
	lease.Manifest = redisServer()
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	workload := requireOneRunningContainer(t, b, lease.LeaseUUID)
	requireVolumeBind(t, b, workload, "/data")

	root := oneVolumeRoot(t, b, lease.LeaseUUID)
	require.Equal(t, root, volumeRootOf(t, b, workload, "/data"), "the workload must mount the lease's own directory")
	project := ProjectID(t, root)
	require.NotZero(t, project, "a volume belongs to a project")
	data := volumeSource(t, workload, "/data")
	require.Equal(t, project, ProjectID(t, data), "what is mounted must be in the volume's project")
	require.Equal(t, statefulDiskMB*1024, b.QuotaLimitKB(t, root))
	require.Equal(t, 1, b.QuotaEntries(t), "one volume, one quota entry")

	require.Equal(t, "OK", redis(t, b, workload.ID, "SET", "key", "value"))
	require.Equal(t, "OK", redis(t, b, workload.ID, "SAVE"))
	require.FileExists(t, filepath.Join(data, "dump.rdb"), "the workload's save must be in the volume")

	closeLease(t, b, lease.LeaseUUID)
	requireDestroyed(t, b, lease.LeaseUUID)
	requireNoQuotaLeft(t, b, 0, project)
	requireNotListed(t, b, lease.LeaseUUID)
}

// TestBackend_VolumeOutlivesAFailedWorkload kills a workload after it saved
// to its volume, and applies the lease again. The new container must get the
// same volume, and the workload must find what it saved.
func TestBackend_VolumeOutlivesAFailedWorkload(t *testing.T) {
	b := StartBackend(t)
	lease := stateful("tenant-a")
	lease.Manifest = redisServer()
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	first := requireOneRunningContainer(t, b, lease.LeaseUUID)
	require.Equal(t, "OK", redis(t, b, first.ID, "SET", "key", "kept"))
	require.Equal(t, "OK", redis(t, b, first.ID, "SAVE"))
	root := oneVolumeRoot(t, b, lease.LeaseUUID)
	inode, project := inodeOf(t, root), ProjectID(t, root)

	b.KillContainers(t, lease.LeaseUUID)
	requireStatus(t, b, lease.LeaseUUID, statusFailed)
	require.Equal(t, inode, inodeOf(t, oneVolumeRoot(t, b, lease.LeaseUUID)), "a failed lease keeps its volume")

	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	second := requireOneRunningContainer(t, b, lease.LeaseUUID)
	require.NotEqual(t, first.ID, second.ID)
	require.Equal(t, root, volumeRootOf(t, b, second, "/data"))
	require.Equal(t, inode, inodeOf(t, root), "the new container must get the same directory")
	require.Equal(t, project, ProjectID(t, root))
	require.Equal(t, statefulDiskMB*1024, b.QuotaLimitKB(t, root))
	require.Equal(t, 1, b.QuotaEntries(t), "one volume, one quota entry")
	require.Equal(t, "kept", redis(t, b, second.ID, "GET", "key"))
}

// TestBackend_LeaseWithoutDiskGetsNoVolume runs an image that declares a
// volume on a SKU without disk. The volume's path must be memory inside the
// container, and nothing of the lease may be on the backend's volume path.
func TestBackend_LeaseWithoutDiskGetsNoVolume(t *testing.T) {
	b := StartBackend(t)
	lease := stateless("tenant-a")
	lease.Manifest = sleeper(RedisImage)
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	workload := requireOneRunningContainer(t, b, lease.LeaseUUID)

	_, tmpfs := workload.HostConfig.Tmpfs["/data"]
	for _, mount := range workload.Mounts {
		if mount.Destination == "/data" {
			require.Equal(t, "tmpfs", string(mount.Type), "/data is mounted from %s", mount.Source)
			tmpfs = true
		}
	}
	require.True(t, tmpfs, "/data must be a tmpfs: %+v", workload.Mounts)

	roots, err := volumeRoots(b, lease.LeaseUUID)
	require.NoError(t, err)
	require.Empty(t, roots, "a lease without disk must have no volume directory")
	require.Zero(t, b.QuotaEntries(t), "a lease without disk must have no quota entry")
}

// TestBackend_EveryInstanceHasItsOwnVolume runs a lease of two instances.
// Each must get a directory and a project of its own, with the whole disk of
// the SKU, and see only what it wrote itself. The close destroys both, and
// both quota entries (ENG-459).
func TestBackend_EveryInstanceHasItsOwnVolume(t *testing.T) {
	b := StartBackendWithConfig(t, destroysOnClose)
	lease := stateful("tenant-a")
	lease.Quantity = map[string]int{"": 2}
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	instances := requireRunningContainers(t, b, lease.LeaseUUID, 2)

	roots := make([]string, 2)
	projects := make([]int, 2)
	for _, instance := range instances {
		index := instanceOf(t, instance)
		roots[index] = volumeRootOf(t, b, instance, "/data")
		projects[index] = ProjectID(t, roots[index])
		require.NotZero(t, projects[index], "a volume belongs to a project")
		require.Equal(t, statefulDiskMB*1024, b.QuotaLimitKB(t, roots[index]), "each instance has the SKU's whole disk")
		b.Exec(t, instance.ID, "sh", "-c", fmt.Sprintf("echo instance-%d > /data/marker", index))
	}
	require.NotEqual(t, roots[0], roots[1], "each instance must have its own directory")
	require.NotEqual(t, projects[0], projects[1], "each instance must have its own project")
	require.Equal(t, 2, b.QuotaEntries(t), "two volumes, two quota entries")
	onDisk, err := volumeRoots(b, lease.LeaseUUID)
	require.NoError(t, err)
	require.ElementsMatch(t, roots, onDisk, "the lease's directories are the ones its instances mount")

	for _, instance := range instances {
		index := instanceOf(t, instance)
		want := fmt.Sprintf("instance-%d\n", index)
		require.Equal(t, want, b.Exec(t, instance.ID, "cat", "/data/marker"), "an instance must read what it wrote")
		require.Equal(t, want, volumeFile(t, roots[index], "marker"), "and it must be in its own directory")
	}

	closeLease(t, b, lease.LeaseUUID)
	requireDestroyed(t, b, lease.LeaseUUID)
	requireNoQuotaLeft(t, b, 0, projects...)
}

// TestBackend_VolumeQuotaIsEnforced fills a volume from inside the workload,
// first with bytes and then with files (G20). Each must be refused at the
// volume's limit, and a write below the limit must go through before and
// after. For the files, the test also creates one outside the volume at the
// moment the workload is refused, so the refusal is the volume's limit and
// not a full filesystem (ENG-548).
//
// The lease has a gigabyte of memory. With the fixture's 128 MB, the shell
// creating the files was killed three quarters of the way to the limit. The
// likely cause: the kernel counts the inodes a container creates against the
// container's memory for as long as it caches them. The test logs the
// container's peak memory.
func TestBackend_VolumeQuotaIsEnforced(t *testing.T) {
	b := StartBackendWithConfig(t, map[string]any{"sku_profiles": map[string]any{
		"e2e-stateless": map[string]any{"cpu_cores": 0.25, "memory_mb": 128, "disk_mb": 0},
		"e2e-stateful":  map[string]any{"cpu_cores": 0.25, "memory_mb": 1024, "disk_mb": statefulDiskMB},
	}})
	lease := stateful("tenant-a")
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	workload := requireOneRunningContainer(t, b, lease.LeaseUUID)
	root := oneVolumeRoot(t, b, lease.LeaseUUID)
	project := ProjectID(t, root)
	limitKB := b.QuotaLimitKB(t, root)
	require.Equal(t, statefulDiskMB*1024, limitKB)

	// Bytes: an eighth of the disk fits, twice the disk does not.
	b.Exec(t, workload.ID, "dd", "if=/dev/zero", "of=/data/fits", "bs=1M", fmt.Sprintf("count=%d", statefulDiskMB/8))
	code, out := b.TryExec(t, workload.ID, "dd", "if=/dev/zero", "of=/data/too-much", "bs=1M", fmt.Sprintf("count=%d", 2*statefulDiskMB))
	require.NotZero(t, code, "twice the volume's disk was written to it: %s", out)
	require.True(t, refusedForSpace(out), "the write must be refused for space: %s", out)
	require.LessOrEqual(t, b.QuotaUsedKB(t, project), limitKB, "the volume must not hold more than its limit")
	b.Exec(t, workload.ID, "rm", "/data/fits", "/data/too-much")
	b.Exec(t, workload.ID, "dd", "if=/dev/zero", "of=/data/fits-again", "bs=1M", "count=1")
	b.Exec(t, workload.ID, "rm", "/data/fits-again")

	// Files: exactly as many as the volume has left.
	used, limit := b.InodeQuota(t, root)
	require.Positive(t, limit, "a volume must have an inode limit")
	t.Logf("the volume holds %d of %d inodes", used, limit)
	const batch = 20_000
	for from := 0; from < limit-used; from += batch {
		create := fmt.Sprintf("i=%d; while [ $i -lt %d ]; do : > /data/f$i || exit 1; i=$((i+1)); done", from, min(from+batch, limit-used))
		if code, out := b.TryExec(t, workload.ID, "sh", "-c", create); code != 0 {
			_, memory := b.TryExec(t, workload.ID, "cat", "/sys/fs/cgroup/memory.events")
			require.Failf(t, "creating files below the limit failed", "from file %d, exit code %d: %s\nthe container's memory events:\n%s", from, code, out, memory)
		}
	}
	_, peak := b.TryExec(t, workload.ID, "cat", "/sys/fs/cgroup/memory.peak")
	t.Logf("the container's memory peaked at %s bytes", strings.TrimSpace(peak))
	used, _ = b.InodeQuota(t, root)
	require.Equal(t, limit, used, "the volume must hold exactly its limit")

	code, out = b.TryExec(t, workload.ID, "sh", "-c", ": > /data/one-too-many")
	require.NotZero(t, code, "a file was created in a volume at its inode limit")
	require.True(t, refusedForSpace(out), "the file must be refused for space: %s", out)
	// The filesystem has room for it: only the volume is full.
	outside := filepath.Join(filepath.Dir(b.VolumePath), "outside-any-volume")
	require.NoError(t, os.WriteFile(outside, nil, 0o600), "the filesystem itself must have inodes left")
	require.NoError(t, os.Remove(outside))

	b.Exec(t, workload.ID, "rm", "/data/f0")
	b.Exec(t, workload.ID, "sh", "-c", ": > /data/fits-again")
}

// TestBackend_VolumeBelongsToTheWorkloadsAccount runs an image whose volume
// belongs to an account that is not root. Tenant containers hold no
// capability, so the workload can write its volume only if the directory on
// the host belongs to the account it runs as. The same for an image that
// runs as root is TestBackend_RootContainerWritesItsVolume.
func TestBackend_VolumeBelongsToTheWorkloadsAccount(t *testing.T) {
	b := StartBackend(t)
	lease := stateful("tenant-a")
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	workload := requireOneRunningContainer(t, b, lease.LeaseUUID)

	uid, err := strconv.ParseUint(strings.TrimSpace(b.Exec(t, workload.ID, "id", "-u")), 10, 32)
	require.NoError(t, err)
	gid, err := strconv.ParseUint(strings.TrimSpace(b.Exec(t, workload.ID, "id", "-g")), 10, 32)
	require.NoError(t, err)
	require.NotZero(t, uid, "the redis image's volume belongs to its own account, and the workload must run as it")

	data := volumeSource(t, workload, "/data")
	owner := func(path string) (uint32, uint32) {
		info, err := os.Stat(path)
		require.NoError(t, err)
		stat := info.Sys().(*syscall.Stat_t)
		return stat.Uid, stat.Gid
	}
	ownerUID, ownerGID := owner(data)
	require.Equal(t, uint32(uid), ownerUID, "the volume directory must belong to the workload's account")
	require.Equal(t, uint32(gid), ownerGID, "the volume directory must belong to the workload's group")

	b.Exec(t, workload.ID, "sh", "-c", "echo written > /data/file")
	fileUID, _ := owner(filepath.Join(data, "file"))
	require.Equal(t, uint32(uid), fileUID)
}

// TestBackend_LeaseWithOnlyAWritablePathIsNotRetained closes a lease whose
// image declares no volume. Its account owns one directory of the image, and
// the backend backs that directory with the lease's disk so the workload can
// write there. A deploy fills it from the image again, so nothing in it is
// worth keeping: the close must destroy it and retain nothing, on a backend
// that retains volumes (ENG-406).
func TestBackend_LeaseWithOnlyAWritablePathIsNotRetained(t *testing.T) {
	b := StartBackend(t)
	lease := stateful("tenant-a")
	lease.Manifest = sleeper(WritablePathImage)
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	workload := requireOneRunningContainer(t, b, lease.LeaseUUID)

	// Without this the image no longer has a writable path, and the test
	// would show nothing.
	requireVolumeBind(t, b, workload, "/var/lib/app")
	root := oneVolumeRoot(t, b, lease.LeaseUUID)
	require.Equal(t, root, volumeRootOf(t, b, workload, "/var/lib/app"))
	project := ProjectID(t, root)
	require.NotZero(t, project, "the lease's disk belongs to a project")
	require.Equal(t, statefulDiskMB*1024, b.QuotaLimitKB(t, root))
	b.Exec(t, workload.ID, "sh", "-c", "echo written > /var/lib/app/file")
	require.FileExists(t, filepath.Join(volumeSource(t, workload, "/var/lib/app"), "file"))

	closeLease(t, b, lease.LeaseUUID)
	requireDestroyed(t, b, lease.LeaseUUID)
	requireNotListed(t, b, lease.LeaseUUID)
	requireNoQuotaLeft(t, b, 0, project)
}
