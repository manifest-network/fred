//go:build integration

package e2e

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// The tests in this file close a lease and follow its data: kept until its
// age limit, or handed to a new lease by a restore. A close and a restore
// move the volume's directory, they never copy it, so the data is the same
// data afterwards. The tests read that from the host.

// keptFile is what the host shows of one file of a volume.
type keptFile struct {
	Inode    uint64
	UID, GID uint32
	Mode     os.FileMode
	Modified int64
	// Content is the file's bytes, or a link's target.
	Content string
	// Attribute is the value of keptAttribute, when the file carries it.
	Attribute string
}

// keptAttribute is the extended attribute the tests give a file.
const keptAttribute = "user.fred-e2e"

// keptFiles reads the files with the given names, relative to dir.
func keptFiles(t *testing.T, dir string, names ...string) map[string]keptFile {
	t.Helper()
	files := map[string]keptFile{}
	for _, name := range names {
		path := filepath.Join(dir, name)
		info, err := os.Lstat(path)
		require.NoError(t, err)
		stat := info.Sys().(*syscall.Stat_t)
		file := keptFile{Inode: stat.Ino, UID: stat.Uid, GID: stat.Gid, Mode: info.Mode(), Modified: info.ModTime().Unix()}
		if info.Mode()&os.ModeSymlink != 0 {
			file.Content, err = os.Readlink(path)
			require.NoError(t, err)
		} else {
			content, err := os.ReadFile(path) //nolint:gosec // G304: a path under the fixture's volume directory
			require.NoError(t, err)
			file.Content = string(content)
			value := make([]byte, 64)
			if size, err := syscall.Getxattr(path, keptAttribute, value); err == nil {
				file.Attribute = string(value[:size])
			}
		}
		files[name] = file
	}
	return files
}

// TestBackend_RestoreKeepsTheVolumeExactly writes files of several kinds
// into a volume, closes the lease and restores it into a new one. Retained
// and restored, the volume must be the same directory in the same project,
// and every file the same file: its bytes, owner, mode, modification time,
// link target and extended attribute. The new lease's workload must read
// the bytes. The backend has no age limit for retained data, and must
// report none.
func TestBackend_RestoreKeepsTheVolumeExactly(t *testing.T) {
	b := StartBackendWithConfig(t, map[string]any{"retention_max_age": "0s"})
	source := stateful("tenant-a")
	require.NoError(t, b.Client.Apply(t.Context(), source))
	requireStatus(t, b, source.LeaseUUID, statusReady)
	workload := requireOneRunningContainer(t, b, source.LeaseUUID)

	const top, deep = "exact bytes, no newline", "nested\nbytes\n"
	b.Exec(t, workload.ID, "sh", "-c", fmt.Sprintf(
		"mkdir -p /data/nested/dir && printf '%%s' '%s' > /data/top.txt && printf '%%s' '%s' > /data/nested/dir/deep.txt"+
			" && chmod 640 /data/top.txt && ln -s deep.txt /data/nested/dir/link", top, deep))
	// One file gets what the workload's account cannot give it: another
	// owner, an old modification time and an extended attribute.
	root := oneVolumeRoot(t, b, source.LeaseUUID)
	data, err := filepath.Rel(root, volumeSource(t, workload, "/data"))
	require.NoError(t, err)
	stamped := filepath.Join(root, data, "nested", "dir", "stamped")
	require.NoError(t, os.WriteFile(stamped, []byte("stamped"), 0o604)) //nolint:gosec // G306: the mode is what the test checks
	require.NoError(t, os.Chown(stamped, 4242, 4343))
	longAgo := time.Date(2001, 2, 3, 4, 5, 6, 0, time.UTC)
	require.NoError(t, os.Chtimes(stamped, longAgo, longAgo))
	require.NoError(t, syscall.Setxattr(stamped, keptAttribute, []byte("kept"), 0))

	names := []string{"top.txt", "nested/dir/deep.txt", "nested/dir/link", "nested/dir/stamped"}
	inode, project := inodeOf(t, root), ProjectID(t, root)
	before := keptFiles(t, filepath.Join(root, data), names...)
	require.Equal(t, top, before["top.txt"].Content)
	require.Equal(t, os.FileMode(0o640), before["top.txt"].Mode)
	require.Equal(t, "deep.txt", before["nested/dir/link"].Content)
	require.Equal(t, keptFile{
		Inode: before["nested/dir/stamped"].Inode, UID: 4242, GID: 4343, Mode: 0o604,
		Modified: longAgo.Unix(), Content: "stamped", Attribute: "kept",
	}, before["nested/dir/stamped"])

	closeLease(t, b, source.LeaseUUID)
	retained := requireRetained(t, b, source.LeaseUUID)[0]
	require.True(t, retained.RetainedUntil.IsZero(), "a backend without an age limit must report no expiry, not %s", retained.RetainedUntil)
	require.Equal(t, source.Tenant, retained.Tenant, "a retained lease must still name its tenant")
	require.Empty(t, b.Containers(t, source.LeaseUUID))
	kept := oneVolumeRoot(t, b, source.LeaseUUID)
	require.Equal(t, inode, inodeOf(t, kept), "the retained volume must be the lease's own directory")
	require.Equal(t, project, ProjectID(t, kept), "the retained volume must keep its project")
	require.Equal(t, before, keptFiles(t, filepath.Join(kept, data), names...), "the retained files")

	target := Spec{LeaseUUID: uuid.NewString(), Tenant: source.Tenant, SKU: StatefulSKU}
	require.NoError(t, b.Client.Restore(t.Context(), target, source.LeaseUUID))
	requireStatus(t, b, target.LeaseUUID, statusReady)
	restoredWorkload := requireOneRunningContainer(t, b, target.LeaseUUID)
	restored := oneVolumeRoot(t, b, target.LeaseUUID)
	require.Equal(t, filepath.Join(restored, data), volumeSource(t, restoredWorkload, "/data"))
	require.Equal(t, inode, inodeOf(t, restored), "the restored volume must be the source's own directory")
	require.Equal(t, project, ProjectID(t, restored), "the restored volume must keep its project")
	require.Equal(t, before, keptFiles(t, filepath.Join(restored, data), names...), "the restored files")
	require.Equal(t, statefulDiskMB*1024, b.QuotaLimitKB(t, restored))
	require.Equal(t, 1, b.QuotaEntries(t), "one volume, one quota entry")

	require.Equal(t, top, b.Exec(t, restoredWorkload.ID, "cat", "/data/top.txt"))
	require.Equal(t, deep, b.Exec(t, restoredWorkload.ID, "cat", "/data/nested/dir/link"))
	require.Equal(t, "640\n", b.Exec(t, restoredWorkload.ID, "stat", "-c", "%a", "/data/top.txt"))

	// The source lease has nothing left.
	requireGone(t, b, source.LeaseUUID)
	left, err := volumeRoots(b, source.LeaseUUID)
	require.NoError(t, err)
	require.Empty(t, left, "nothing may stay under the source lease's name")
}

// TestBackend_RestoreKeepsEachInstancesData closes a lease of two instances
// and restores it. Each instance of the new lease must get the volume of the
// instance with the same number, not the other's.
func TestBackend_RestoreKeepsEachInstancesData(t *testing.T) {
	// Disk for the two retained volumes and the two of the new lease.
	b := StartBackendWithConfig(t, map[string]any{"total_disk_mb": 8 * statefulDiskMB})
	source := stateful("tenant-a")
	source.Quantity = map[string]int{"": 2}
	require.NoError(t, b.Client.Apply(t.Context(), source))
	requireStatus(t, b, source.LeaseUUID, statusReady)
	inodes := make([]uint64, 2)
	for _, instance := range requireRunningContainers(t, b, source.LeaseUUID, 2) {
		index := instanceOf(t, instance)
		b.Exec(t, instance.ID, "sh", "-c", fmt.Sprintf("echo instance-%d > /data/marker", index))
		inodes[index] = inodeOf(t, volumeRootOf(t, b, instance, "/data"))
	}
	require.NotEqual(t, inodes[0], inodes[1])

	closeLease(t, b, source.LeaseUUID)
	requireRetained(t, b, source.LeaseUUID)
	kept, err := volumeRoots(b, source.LeaseUUID)
	require.NoError(t, err)
	require.Len(t, kept, 2, "both instances' volumes must be retained")

	target := Spec{LeaseUUID: uuid.NewString(), Tenant: source.Tenant, SKU: StatefulSKU, Quantity: source.Quantity}
	require.NoError(t, b.Client.Restore(t.Context(), target, source.LeaseUUID))
	requireStatus(t, b, target.LeaseUUID, statusReady)
	for _, instance := range requireRunningContainers(t, b, target.LeaseUUID, 2) {
		index := instanceOf(t, instance)
		require.Equal(t, fmt.Sprintf("instance-%d\n", index), b.Exec(t, instance.ID, "cat", "/data/marker"),
			"instance %d must get the data of the source's instance %d", index, index)
		require.Equal(t, inodes[index], inodeOf(t, volumeRootOf(t, b, instance, "/data")),
			"instance %d must get the directory of the source's instance %d", index, index)
	}
	requireGone(t, b, source.LeaseUUID)
	require.Equal(t, 2, b.QuotaEntries(t), "two volumes, two quota entries")
}

// TestBackend_RetainedDataIsDestroyedAtItsAgeLimit closes a lease on a
// backend that keeps retained data for 45 seconds. The backend must say
// until when it keeps the data, keep it until then, and destroy it after:
// the lease, its directory and its quota entry.
//
// This is 0.14's rule, where a backend destroys by age alone.
func TestBackend_RetainedDataIsDestroyedAtItsAgeLimit(t *testing.T) {
	const ageLimit = 45 * time.Second
	b := StartBackendWithConfig(t, map[string]any{"retention_max_age": ageLimit.String(), "retention_reap_interval": "1s"})
	source := retainVolume(t, b, sleeper(RedisImage))

	retained, err := b.Client.Get(t.Context(), source.lease)
	require.NoError(t, err)
	require.Equal(t, statusRetained, retained.Status)
	// The age counts from the close, not from the deploy before it.
	require.WithinRange(t, retained.RetainedUntil, source.closing.Add(ageLimit), source.closed.Add(ageLimit), "the expiry the backend reports")
	root := oneVolumeRoot(t, b, source.lease)
	require.Equal(t, "kept\n", volumeFile(t, root, "sentinel"))

	requireDestroyed(t, b, source.lease)
	require.False(t, time.Now().Before(retained.RetainedUntil), "the data was destroyed %s before its age limit", time.Until(retained.RetainedUntil))
	requireNoQuotaLeft(t, b, 0, source.project)
	requireNotListed(t, b, source.lease)
}

// writeDirect is a dd that writes megabytes of zeros straight to the disk,
// to the file and in the number its arguments name. A dd that wrote 96 MB
// through the page cache was killed (exit 137) once in CI, in a container
// with 128 MB of memory: a buffered write is counted against the container's
// memory until it reaches the disk, which is the likely cause.
func writeDirect(file string, megabytes int) []string {
	return []string{"dd", "if=/dev/zero", "bs=1M", "oflag=direct", "of=" + file, fmt.Sprintf("count=%d", megabytes)}
}

// filled runs a lease with a volume on the given SKU and writes megabytes of
// data to the volume. It returns the lease and its container.
func filled(t *testing.T, b *Backend, sku string, megabytes int) (Spec, string) {
	t.Helper()
	lease := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: sku, Manifest: sleeper(RedisImage)}
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	workload := requireOneRunningContainer(t, b, lease.LeaseUUID)
	b.Exec(t, workload.ID, writeDirect("/data/payload", megabytes)...)
	return lease, workload.ID
}

// retired closes a lease with one volume and returns the volume as the host
// showed it before the close.
func retired(t *testing.T, b *Backend, lease Spec) retainedVolume {
	t.Helper()
	root := oneVolumeRoot(t, b, lease.LeaseUUID)
	kept := retainedVolume{lease: lease.LeaseUUID, tenant: lease.Tenant, inode: inodeOf(t, root), project: ProjectID(t, root)}
	closeLease(t, b, lease.LeaseUUID)
	requireStatus(t, b, lease.LeaseUUID, statusRetained)
	return kept
}

// TestBackend_RestoreIntoAnotherTier restores retained data into a lease
// with less disk and into one with more (G20). The volume must get the new
// lease's limit, and the limit must hold: after a restore into less disk a
// write that only the larger tier holds is refused, and after a restore into
// more disk a write that was refused before goes through. A restore into
// less disk than the data takes must be refused and change nothing; the same
// data restores into a lease it fits.
func TestBackend_RestoreIntoAnotherTier(t *testing.T) {
	// Three sources and their restores, on a disk that holds them all.
	b := twoTierBackend(t, map[string]any{"total_disk_mb": 1024})
	const (
		// little fits both tiers; between fits only the large one.
		little  = statefulDiskMB / 8
		between = statefulDiskMB + statefulDiskMB/2
	)
	// fill brings a volume that holds little to between.
	fill := writeDirect("/data/more", between-little)
	restore := func(t *testing.T, source retainedVolume, sku string) (Spec, error) {
		target := Spec{LeaseUUID: uuid.NewString(), Tenant: source.tenant, SKU: sku}
		return target, b.Client.Restore(t.Context(), target, source.lease)
	}
	// requireRestored checks the restored lease and returns its container.
	requireRestored := func(t *testing.T, source retainedVolume, target Spec, diskMB, payload int) string {
		requireStatus(t, b, target.LeaseUUID, statusReady)
		workload := requireOneRunningContainer(t, b, target.LeaseUUID)
		root := oneVolumeRoot(t, b, target.LeaseUUID)
		require.Equal(t, source.inode, inodeOf(t, root), "the restored volume must be the source's own directory")
		require.Equal(t, source.project, ProjectID(t, root), "the restored volume must keep its project")
		require.Equal(t, diskMB*1024, b.QuotaLimitKB(t, root), "the restored volume must have the new lease's limit")
		require.Equal(t, fmt.Sprintf("%d\n", payload<<20), b.Exec(t, workload.ID, "stat", "-c", "%s", "/data/payload"))
		requireGone(t, b, source.lease)
		return workload.ID
	}

	t.Run("into less disk", func(t *testing.T) {
		lease, _ := filled(t, b, largeStatefulSKU, little)
		source := retired(t, b, lease)
		target, err := restore(t, source, StatefulSKU)
		require.NoError(t, err)
		workload := requireRestored(t, source, target, statefulDiskMB, little)
		code, out := b.TryExec(t, workload, fill...)
		require.NotZero(t, code, "the volume took what only the larger tier holds: %s", out)
		require.True(t, refusedForSpace(out), "the write must be refused for space: %s", out)
	})

	t.Run("into more disk", func(t *testing.T) {
		lease, before := filled(t, b, StatefulSKU, little)
		code, out := b.TryExec(t, before, fill...)
		require.NotZero(t, code, "the smaller tier took what only the larger one holds: %s", out)
		require.True(t, refusedForSpace(out), "the write must be refused for space: %s", out)
		b.Exec(t, before, "rm", "/data/more")
		source := retired(t, b, lease)

		target, err := restore(t, source, largeStatefulSKU)
		require.NoError(t, err)
		workload := requireRestored(t, source, target, largeDiskMB, little)
		b.Exec(t, workload, fill...)
	})

	t.Run("into less disk than the data takes", func(t *testing.T) {
		lease, _ := filled(t, b, largeStatefulSKU, between)
		source := retired(t, b, lease)
		refused, err := restore(t, source, StatefulSKU)
		requireRefused(t, b, refused, err, http.StatusUnprocessableEntity, CodeDemoteExceedsTier)
		_, err = b.Client.Get(t.Context(), refused.LeaseUUID)
		require.ErrorIs(t, err, ErrNotFound, "the backend must know nothing of the refused lease")
		ofRefused, err := volumeRoots(b, refused.LeaseUUID)
		require.NoError(t, err)
		require.Empty(t, ofRefused, "a refused restore must create no volume")

		// The source is as it was.
		retained, err := b.Client.Get(t.Context(), source.lease)
		require.NoError(t, err)
		require.Equal(t, statusRetained, retained.Status)
		kept := oneVolumeRoot(t, b, source.lease)
		require.Equal(t, source.inode, inodeOf(t, kept))
		require.Equal(t, source.project, ProjectID(t, kept))
		require.Equal(t, largeDiskMB*1024, b.QuotaLimitKB(t, kept), "the retained volume keeps its own limit")
		payload, err := filepath.Glob(filepath.Join(kept, "*", "payload"))
		require.NoError(t, err)
		require.Len(t, payload, 1)
		info, err := os.Stat(payload[0])
		require.NoError(t, err)
		require.Equal(t, int64(between)<<20, info.Size())

		fits, err := restore(t, source, largeStatefulSKU)
		require.NoError(t, err, "the same data must restore into a lease it fits")
		requireRestored(t, source, fits, largeDiskMB, between)
	})
}
