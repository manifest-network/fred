//go:build integration

package e2e

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file are about what the backend counts as used: disk it
// may not hand out a second time, and usage it may not charge to the wrong
// volume. Both are judged from outside, by what a new lease is given.

// stateful is a new lease with a volume.
func stateful(tenant string) Spec {
	return Spec{LeaseUUID: uuid.NewString(), Tenant: tenant, SKU: StatefulSKU, Manifest: sleeper(RedisImage)}
}

// requireRefusedForCapacity sends a new lease with a volume and checks that
// the backend refuses it for lack of capacity and starts nothing.
func requireRefusedForCapacity(t *testing.T, b *Backend, when string) {
	t.Helper()
	lease := stateful("tenant-b")
	err := b.Client.Apply(t.Context(), lease)
	var status *StatusError
	require.True(t, errors.As(err, &status), "%s, a lease that does not fit must be refused: %v", when, err)
	require.Equal(t, CodeInsufficientResources, status.Code, "%s: %v", when, err)
	require.Empty(t, b.Containers(t, lease.LeaseUUID))
}

// awaitPasses waits until the backend has finished the given number of its
// periodic passes, counted from now. Two is the least that guarantees one
// pass that started after the call.
func awaitPasses(t *testing.T, b *Backend, passes float64) {
	t.Helper()
	const finished = "fred_docker_backend_reconciliation_total"
	before := b.Metric(t, finished, `outcome="success"`)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.GreaterOrEqual(collect, b.Metric(t, finished, `outcome="success"`)-before, passes)
	}, 2*time.Minute, time.Second)
}

// TestBackend_DiskOfALeaseWithoutContainersStaysCounted fills the backend's
// disk, then removes one lease's container behind the backend's back. The
// lease has failed and runs nothing, but its volume is still on disk, so a new
// lease that would fit only in that volume's place must be refused: after the
// backend's periodic pass, and after a restart, where the backend rebuilds
// what is in use from what it finds (ENG-567). Once the lease is closed and
// its volume destroyed, the new lease fits.
func TestBackend_DiskOfALeaseWithoutContainersStaysCounted(t *testing.T) {
	// A close destroys the volume, so the test can free a lease's disk.
	b := StartBackendWithConfig(t, map[string]any{"retain_on_close": false})
	// The fixture's total_disk_mb is 256: four volumes.
	leases := make([]Spec, 4)
	for i := range leases {
		leases[i] = stateful("tenant-a")
		require.NoError(t, b.Client.Apply(t.Context(), leases[i]))
		requireStatus(t, b, leases[i].LeaseUUID, statusReady)
	}
	lost := leases[0].LeaseUUID
	workload := requireOneRunningContainer(t, b, lost)
	b.Exec(t, workload.ID, "sh", "-c", "echo kept > /data/sentinel")
	data := volumeSource(t, workload, "/data")

	b.LoseContainers(t, lost)
	requireStatus(t, b, lost, statusFailed)
	awaitPasses(t, b, 2)
	require.Empty(t, b.Containers(t, lost), "nothing puts a failed lease's container back")
	requireRefusedForCapacity(t, b, "after the periodic pass")

	b.Stop()
	b.Start()
	requireStatus(t, b, lost, statusFailed)
	requireRefusedForCapacity(t, b, "after a restart")
	kept, err := os.ReadFile(filepath.Join(data, "sentinel")) //nolint:gosec // G304: a path under the fixture's volume directory
	require.NoError(t, err, "the lease's data must still be on disk")
	require.Equal(t, "kept\n", string(kept))

	// The refusals were for that volume's disk: without it, a lease fits.
	closeLease(t, b, lost)
	var fits Spec
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		// A refused lease is not sent again: each attempt is a new lease.
		fits = stateful("tenant-b")
		assert.NoError(collect, b.Client.Apply(t.Context(), fits))
	}, 2*time.Minute, 2*time.Second)
	requireStatus(t, b, fits.LeaseUUID, statusReady)
	roots, err := volumeRoots(b, lost)
	require.NoError(t, err)
	require.Empty(t, roots, "the closed lease's volume must be gone")
}

// TestBackend_VolumeAfterADestroyWithOpenFilesStartsEmpty destroys a volume
// while a process on the host still holds one of its files open. XFS keeps
// charging an open file to its project after the file is deleted, so the
// project's usage outlives the volume. A volume created meanwhile must get a
// project of its own and start empty: the leftover usage is never charged to
// the next tenant. When the file is closed, the destroyed volume's quota
// entry goes away.
func TestBackend_VolumeAfterADestroyWithOpenFilesStartsEmpty(t *testing.T) {
	const heldKB = 8 * 1024
	// A close destroys the volume: nothing is retained.
	b := StartBackendWithConfig(t, map[string]any{"retain_on_close": false})
	first := stateful("tenant-a")
	require.NoError(t, b.Client.Apply(t.Context(), first))
	requireStatus(t, b, first.LeaseUUID, statusReady)
	workload := requireOneRunningContainer(t, b, first.LeaseUUID)
	b.Exec(t, workload.ID, "dd", "if=/dev/zero", "of=/data/held", "bs=1M", "count=8")
	data := volumeSource(t, workload, "/data")
	project := ProjectID(t, data)
	require.NotZero(t, project, "a volume belongs to a project")
	require.GreaterOrEqual(t, b.QuotaUsedKB(t, project), heldKB)

	held, err := os.Open(filepath.Join(data, "held")) //nolint:gosec // G304: a path under the fixture's volume directory
	require.NoError(t, err)
	t.Cleanup(func() { _ = held.Close() })

	closeLease(t, b, first.LeaseUUID)
	requireGone(t, b, first.LeaseUUID)
	require.NoDirExists(t, data, "the volume's directory must be gone")
	require.GreaterOrEqual(t, b.QuotaUsedKB(t, project), heldKB, "the open file must still be charged to the destroyed volume's project")

	second := stateful("tenant-b")
	require.NoError(t, b.Client.Apply(t.Context(), second))
	requireStatus(t, b, second.LeaseUUID, statusReady)
	fresh := volumeSource(t, requireOneRunningContainer(t, b, second.LeaseUUID), "/data")
	freshProject := ProjectID(t, fresh)
	require.NotZero(t, freshProject, "a volume belongs to a project")
	require.NotEqual(t, project, freshProject, "the new volume must not share the project that still has usage")
	require.Less(t, b.QuotaUsedKB(t, freshProject), 1024, "the new volume must start empty")
	require.GreaterOrEqual(t, b.QuotaUsedKB(t, project), heldKB, "the leftover usage stays with the destroyed volume's project")

	// The backend looks again at a volume it could not finish destroying
	// about every half minute.
	require.NoError(t, held.Close())
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Zero(collect, b.QuotaUsedKB(t, project), "usage of the destroyed volume's project")
		assert.Equal(collect, 1, b.QuotaEntries(t), "only the new volume keeps a quota entry")
	}, 4*time.Minute, 2*time.Second)
	require.Equal(t, statefulDiskMB*1024, b.QuotaLimitKB(t, fresh), "the new volume keeps its own quota")
	requireStatus(t, b, second.LeaseUUID, statusReady)
}
