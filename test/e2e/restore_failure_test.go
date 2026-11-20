//go:build integration

package e2e

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file make a restore fail and check what is left of the
// data. A restore moves a closed lease's volume to a new lease, so a restore
// that fails must not leave the volume with nobody, or with two.
//
// 0.14 does not undo a failed restore at once. It waits until the restore can
// no longer succeed, and its next periodic pass then gives the volume back to
// the source lease. The tests wait for that from outside: the volume's
// directory carries the name of the lease that holds it.

const (
	// largeStatefulSKU has twice the disk of StatefulSKU. Only the backends
	// of this file serve it.
	largeStatefulSKU = "5f0c2f0e-6a3b-4e52-9c58-1a2b3c4d5e04"
	largeDiskMB      = 2 * statefulDiskMB
	// failNextStart is the file whose presence makes failingOnce's workload
	// fail.
	failNextStart = "/data/fail-next-start"
)

// twoTierBackend starts a backend that serves largeStatefulSKU beside the
// fixture's SKUs.
func twoTierBackend(t *testing.T, settings map[string]any) *Backend {
	t.Helper()
	merged := map[string]any{
		"sku_mapping": map[string]string{
			StatelessSKU:     "e2e-stateless",
			StatefulSKU:      "e2e-stateful",
			largeStatefulSKU: "e2e-stateful-large",
		},
		"sku_profiles": map[string]any{
			"e2e-stateless":      map[string]any{"cpu_cores": 0.25, "memory_mb": 128, "disk_mb": 0},
			"e2e-stateful":       map[string]any{"cpu_cores": 0.25, "memory_mb": 128, "disk_mb": statefulDiskMB},
			"e2e-stateful-large": map[string]any{"cpu_cores": 0.25, "memory_mb": 128, "disk_mb": largeDiskMB},
		},
	}
	for key, value := range settings {
		merged[key] = value
	}
	return StartBackendWithConfig(t, merged)
}

// retainedVolume is a closed lease's volume as the host showed it when the
// lease was closed.
type retainedVolume struct {
	lease   string
	tenant  string
	inode   uint64
	project int
	// image is the image its workload ran, as the daemon names it, and
	// container the name its container had.
	image     string
	container string
}

// retainVolume runs a lease with a volume on StatefulSKU, lets prepare write
// to the volume from inside the workload, and closes the lease.
func retainVolume(t *testing.T, b *Backend, manifest map[string]any, prepare ...string) retainedVolume {
	t.Helper()
	lease := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatefulSKU, Manifest: manifest}
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	workload := requireOneRunningContainer(t, b, lease.LeaseUUID)
	b.Exec(t, workload.ID, "sh", "-c", "echo kept > /data/sentinel")
	for _, command := range prepare {
		b.Exec(t, workload.ID, "sh", "-c", command)
	}
	root := oneVolumeRoot(t, b, lease.LeaseUUID)
	kept := retainedVolume{lease: lease.LeaseUUID, tenant: lease.Tenant, inode: inodeOf(t, root), project: ProjectID(t, root), image: workload.Image, container: strings.TrimPrefix(workload.Name, "/")}
	require.Contains(t, kept.container, kept.lease, "a container is named after its lease")
	require.NotZero(t, kept.project, "a volume belongs to a project")

	closeLease(t, b, lease.LeaseUUID)
	requireRetained(t, b, lease.LeaseUUID)
	return kept
}

// oneVolumeRoot returns the one directory on the backend's volume path that
// carries the lease's name.
func oneVolumeRoot(t *testing.T, b *Backend, leaseUUID string) string {
	t.Helper()
	roots, err := volumeRoots(b, leaseUUID)
	require.NoError(t, err)
	require.Len(t, roots, 1, "lease %s must hold exactly one volume directory", leaseUUID)
	return roots[0]
}

// inodeOf is the inode number of a host path.
func inodeOf(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	return info.Sys().(*syscall.Stat_t).Ino
}

// volumeFile reads a file of a volume from the host. The image's /data is one
// directory below the volume's root.
func volumeFile(t *testing.T, root, name string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(root, "*", name))
	require.NoError(t, err)
	require.Len(t, matches, 1, "%s must hold one %s", root, name)
	content, err := os.ReadFile(matches[0]) //nolint:gosec // G304: a path under the fixture's volume directory
	require.NoError(t, err)
	return string(content)
}

// requireHandedBack waits until the volume is back with the source lease and
// nothing of the target is left, and checks that it is the same volume: the
// same directory, in the same project, under the source's quota, and the only
// one. It returns the volume's root.
//
// A restore that has been accepted and has not moved the volume yet looks
// the same from outside. The caller first waits for a sign that the restore
// took the volume.
func requireHandedBack(t *testing.T, b *Backend, source retainedVolume, target string) string {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		ofSource, err := volumeRoots(b, source.lease)
		assert.NoError(collect, err)
		assert.Len(collect, ofSource, 1, "the source's volume directory")
		ofTarget, err := volumeRoots(b, target)
		assert.NoError(collect, err)
		assert.Empty(collect, ofTarget, "the target's volume directory")
		containers, err := b.TryContainers(target)
		assert.NoError(collect, err)
		assert.Empty(collect, containers, "the target's containers")
		// The directory gets its name back a moment before its quota.
		assert.Equal(collect, statefulDiskMB*1024, b.quotaReport(t)[source.project].limitKB, "the quota of the source's project")
	}, 4*time.Minute, time.Second)

	root := oneVolumeRoot(t, b, source.lease)
	require.Equal(t, source.inode, inodeOf(t, root), "the source must get its own directory back")
	require.Equal(t, source.project, ProjectID(t, root), "the volume must keep its project")
	require.Equal(t, 1, b.QuotaEntries(t), "one volume, one quota entry")
	require.Equal(t, "kept\n", volumeFile(t, root, "sentinel"))
	requireRetained(t, b, source.lease)
	return root
}

// restoreAgain restores the source into a new lease once the backend lets it,
// and returns that lease. The volume is back in place a moment before the
// source lease may be restored from again, and until then the backend
// refuses. A refused restore is not sent again: each attempt is a new lease.
func restoreAgain(t *testing.T, b *Backend, source retainedVolume, sku string) Spec {
	t.Helper()
	var target Spec
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		target = Spec{LeaseUUID: uuid.NewString(), Tenant: source.tenant, SKU: sku}
		assert.NoError(collect, b.Client.Restore(t.Context(), target, source.lease))
	}, time.Minute, 2*time.Second)
	requireStatus(t, b, target.LeaseUUID, statusReady)
	return target
}

// TestBackend_RestoreFailingBeforeAnyWorkloadReturnsTheVolume restores a
// retained volume into a lease with more disk, and makes Docker refuse the
// new lease's container: its name is taken. The restore has moved the volume
// and raised its quota by then, and no workload has run. The source lease
// must get the volume back as it was: the same directory and project, its
// data, and its own smaller quota. Nothing of the target may remain, a second
// restore must succeed, and the failed one must have released its capacity.
func TestBackend_RestoreFailingBeforeAnyWorkloadReturnsTheVolume(t *testing.T) {
	// A restore that started nothing is given up after provision_timeout.
	b := twoTierBackend(t, map[string]any{"provision_timeout": "1m"})
	source := retainVolume(t, b, sleeper(RedisImage))

	failing := Spec{LeaseUUID: uuid.NewString(), Tenant: source.tenant, SKU: largeStatefulSKU}
	// The target's container will be named as the source's was, after its
	// own lease.
	b.OccupyContainerName(t, strings.Replace(source.container, source.lease, failing.LeaseUUID, 1), source.image)
	require.NoError(t, b.Client.Restore(t.Context(), failing, source.lease))

	// The restore got as far as it can: the volume is the target's, under
	// the larger quota, and no container of the target exists.
	var adopted string
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		roots, err := volumeRoots(b, failing.LeaseUUID)
		assert.NoError(collect, err)
		if assert.Len(collect, roots, 1, "the target's volume directory") {
			adopted = roots[0]
			assert.Equal(collect, largeDiskMB*1024, b.quotaReport(t)[ProjectID(t, adopted)].limitKB, "the quota of the adopted volume")
		}
	}, time.Minute, time.Second)
	require.Equal(t, source.inode, inodeOf(t, adopted), "the target must have been given the source's own directory")
	require.Empty(t, b.Containers(t, failing.LeaseUUID), "no workload of the target may have been created")

	requireHandedBack(t, b, source, failing.LeaseUUID)

	// The same restore, into a new lease, goes through.
	retry := restoreAgain(t, b, source, largeStatefulSKU)
	restored := requireOneRunningContainer(t, b, retry.LeaseUUID)
	require.Equal(t, "kept\n", b.Exec(t, restored.ID, "cat", "/data/sentinel"))
	root := oneVolumeRoot(t, b, retry.LeaseUUID)
	require.Equal(t, source.inode, inodeOf(t, root), "the restored volume must be the source's own directory")
	require.Equal(t, largeDiskMB*1024, b.QuotaLimitKB(t, root), "the restored volume must have the new lease's quota")
	requireGone(t, b, source.lease)

	// The backend's disk is 256 MB and the restored lease holds 128. Two more
	// volumes fit only if the failed restore released what it had reserved,
	// and a third does not fit either way.
	for range 2 {
		fits := stateful("tenant-b")
		require.NoError(t, b.Client.Apply(t.Context(), fits), "the failed restore must have released its capacity")
		requireStatus(t, b, fits.LeaseUUID, statusReady)
	}
	requireRefusedForCapacity(t, b, "with the disk full")
}

// failingOnce is a workload that runs until it is stopped, unless its volume
// holds failNextStart: then it writes to the volume and fails.
func failingOnce() map[string]any {
	return map[string]any{"image": RedisImage, "command": []string{
		"sh", "-c", "if [ -e " + failNextStart + " ]; then echo late > /data/written-by-target; exit 1; fi; exec sleep 3600",
	}}
}

// TestBackend_RestoreFailingAfterAWriteKeepsTheVolume restores a retained
// volume whose workload, started on it again, writes to it and then fails.
// What the workload wrote cannot be undone, and is not: the volume is kept
// whole, with the earlier data and the new, and exactly one lease holds it.
// It can still be restored once its workload starts.
func TestBackend_RestoreFailingAfterAWriteKeepsTheVolume(t *testing.T) {
	b := StartBackend(t)
	source := retainVolume(t, b, failingOnce(), "touch "+failNextStart)

	failing := Spec{LeaseUUID: uuid.NewString(), Tenant: source.tenant, SKU: StatefulSKU}
	require.NoError(t, b.Client.Restore(t.Context(), failing, source.lease))

	// The restored workload ran on the volume: its file is there, whichever
	// lease's name the directory carries at this moment.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		written, err := filepath.Glob(filepath.Join(b.VolumePath, "*", "*", "written-by-target"))
		assert.NoError(collect, err)
		assert.Len(collect, written, 1, "the file the restored workload writes")
	}, time.Minute, 500*time.Millisecond)

	// On 0.14 the lease that holds the volume afterwards is the source.
	root := requireHandedBack(t, b, source, failing.LeaseUUID)
	require.Equal(t, "late\n", volumeFile(t, root, "written-by-target"), "what the failed workload wrote must be kept")
	leases, err := b.Client.List(t.Context())
	require.NoError(t, err)
	var holders []string
	for _, lease := range leases {
		if lease.Status == statusRetained || lease.Status == statusReady {
			holders = append(holders, lease.UUID)
		}
	}
	require.Equal(t, []string{source.lease}, holders, "exactly one lease must hold the volume")

	// The volume is not stranded: with the reason for the failure removed,
	// it is restored with everything in it.
	matches, err := filepath.Glob(filepath.Join(root, "*", filepath.Base(failNextStart)))
	require.NoError(t, err)
	require.Len(t, matches, 1)
	require.NoError(t, os.Remove(matches[0]))
	retry := restoreAgain(t, b, source, StatefulSKU)
	restored := requireOneRunningContainer(t, b, retry.LeaseUUID)
	require.Equal(t, "kept\nlate\n", b.Exec(t, restored.ID, "cat", "/data/sentinel", "/data/written-by-target"))
	require.Equal(t, source.inode, inodeOf(t, oneVolumeRoot(t, b, retry.LeaseUUID)), "the restored volume must be the source's own directory")
	requireGone(t, b, source.lease)
}
