//go:build integration

package e2e

import (
	"errors"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file pin what a backend keeps when a lease with data is
// closed, and what it gives up when a cap is reached. A retained lease's
// footprint is the disk its SKU reserved, 64 MB here, not the bytes it uses:
// every lease below writes almost nothing, and the caps still count 64 MB
// for each.
//
// Each test reads the outcome three ways: the leases the backend lists as
// retained, the volume directories on the host, and their XFS quota limits.

const (
	// partitionKey is the manifest environment variable the partitioned
	// backends read a lease's partition from.
	partitionKey = "CUSTOMER"
	// statefulDiskMB is the disk the stateful SKU reserves.
	statefulDiskMB = 64
)

// retentionSettings is the configuration every retention test starts from.
// The reaper runs often, so a lease that lost its place is removed while the
// test watches. The production default is an hour.
func retentionSettings(settings map[string]any) map[string]any {
	merged := map[string]any{"retention_reap_interval": "5s"}
	for key, value := range settings {
		merged[key] = value
	}
	return merged
}

// partitionedBackend starts a backend that reads partitions from the
// manifest and gives tenant-a the budget described by limits.
func partitionedBackend(t *testing.T, limits map[string]any) *Backend {
	t.Helper()
	budget := map[string]any{"max_retained_leases": 10, "max_retained_disk_mb": 4 * statefulDiskMB, "max_partitions": 4}
	for key, value := range limits {
		budget[key] = value
	}
	return StartBackendWithConfig(t, retentionSettings(map[string]any{
		"retention_partition_source": "manifest.env:" + partitionKey,
		"retention_tenant_budgets":   map[string]any{"tenant-a": budget},
	}))
}

// statefulManifest is a workload with a volume to retain. partition, when not
// empty, is the value of the partition variable.
func statefulManifest(partition string) map[string]any {
	manifest := sleeper(RedisImage)
	if partition != "" {
		manifest["env"] = map[string]string{partitionKey: partition}
	}
	return manifest
}

// retire runs a lease with a volume until it is ready, then closes it, and
// returns its UUID. Whether the backend kept its data is for the caller to
// assert.
func retire(t *testing.T, b *Backend, tenant, partition string) string {
	t.Helper()
	lease := Spec{LeaseUUID: uuid.NewString(), Tenant: tenant, SKU: StatefulSKU, Manifest: statefulManifest(partition)}
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	closeLease(t, b, lease.LeaseUUID)
	return lease.LeaseUUID
}

// volumeRoots lists the directories on the backend's volume path that belong
// to a lease, under its live or its retained name.
func volumeRoots(b *Backend, leaseUUID string) ([]string, error) {
	return filepath.Glob(filepath.Join(b.VolumePath, "*"+leaseUUID+"*"))
}

// requireRetained waits until the backend holds retained data for exactly
// the given leases. Each must report itself retained, and each of its volumes
// must be on disk with its quota still in force. It returns the leases as
// the backend reports them, in the order given.
func requireRetained(t *testing.T, b *Backend, want ...string) []Lease {
	t.Helper()
	sorted := slices.Sorted(slices.Values(want))
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		leases, err := b.Client.List(t.Context())
		if !assert.NoError(collect, err) {
			return
		}
		var retained []string
		for _, lease := range leases {
			if lease.Status == statusRetained {
				retained = append(retained, lease.UUID)
			}
		}
		slices.Sort(retained)
		assert.Equal(collect, sorted, retained, "the leases the backend lists as retained")
	}, 2*time.Minute, time.Second)

	reported := make([]Lease, 0, len(want))
	for _, leaseUUID := range want {
		lease, err := b.Client.Get(t.Context(), leaseUUID)
		require.NoError(t, err)
		require.Equal(t, statusRetained, lease.Status)
		reported = append(reported, lease)

		roots, err := volumeRoots(b, leaseUUID)
		require.NoError(t, err)
		require.NotEmpty(t, roots, "a retained lease keeps its volumes")
		for _, root := range roots {
			require.Equal(t, statefulDiskMB*1024, quotaLimitKB(t, b, root), "%s keeps its quota", root)
		}
	}
	return reported
}

// requireDestroyed waits until the backend no longer knows the lease and its
// volumes are gone from disk.
func requireDestroyed(t *testing.T, b *Backend, leaseUUID string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		_, err := b.Client.Get(t.Context(), leaseUUID)
		assert.ErrorIs(collect, err, ErrNotFound)
		roots, err := volumeRoots(b, leaseUUID)
		assert.NoError(collect, err)
		assert.Empty(collect, roots, "the lease's volumes must be gone")
	}, 2*time.Minute, time.Second)
}

// quotaLimitKB reads the hard block limit, in KiB, of the XFS project a
// volume directory belongs to.
func quotaLimitKB(t *testing.T, b *Backend, root string) int {
	t.Helper()
	project := projectID(t, root)
	require.NotZero(t, project, "%s belongs to no project", root)
	out, err := exec.CommandContext(t.Context(), "xfs_quota", "-x", "-c", "report -p -b -n -N", filepath.Dir(b.VolumePath)).CombinedOutput()
	require.NoError(t, err, "xfs_quota report: %s", out)
	// One line per project: "#<id> <used> <soft> <hard> ...", in KiB.
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 4 && fields[0] == "#"+strconv.Itoa(project) {
			limit, err := strconv.Atoi(fields[3])
			require.NoError(t, err, "xfs_quota line: %s", line)
			return limit
		}
	}
	require.Failf(t, "project missing", "xfs_quota reports no project %d:\n%s", project, out)
	return 0
}

// TestRetention_TenantCountCapEvictsTheOldest fills a tenant's count cap.
// The next close keeps the closing lease and gives up the tenant's oldest
// retained one. Another tenant's older retained lease is not touched.
func TestRetention_TenantCountCapEvictsTheOldest(t *testing.T) {
	b := StartBackendWithConfig(t, retentionSettings(map[string]any{"max_retained_leases_per_tenant": 2}))
	other := retire(t, b, "tenant-b", "")
	oldest := retire(t, b, "tenant-a", "")
	second := retire(t, b, "tenant-a", "")
	requireRetained(t, b, other, oldest, second)

	third := retire(t, b, "tenant-a", "")
	requireDestroyed(t, b, oldest)
	requireRetained(t, b, other, second, third)
}

// TestRetention_PartitionCountSubCapEvictsThePartitionsOldest fills one
// partition's count sub-cap. The next close in that partition gives up the
// partition's oldest lease. The tenant's oldest lease is in another
// partition, and it stays.
func TestRetention_PartitionCountSubCapEvictsThePartitionsOldest(t *testing.T) {
	b := partitionedBackend(t, map[string]any{"per_partition_max_leases": 2})
	elsewhere := retire(t, b, "tenant-a", "p2")
	oldest := retire(t, b, "tenant-a", "p1")
	second := retire(t, b, "tenant-a", "p1")
	requireRetained(t, b, elsewhere, oldest, second)

	third := retire(t, b, "tenant-a", "p1")
	requireDestroyed(t, b, oldest)
	requireRetained(t, b, elsewhere, second, third)
}

// TestRetention_GlobalDiskCapRefusesTheClosingLease fills the provider-wide
// retained-disk cap with one lease of each of two tenants. The next closing
// lease is destroyed, not retained, although its tenant has a budget of its
// own with room in it. No already retained lease is given up for it, the
// tenant's own or the other tenant's.
func TestRetention_GlobalDiskCapRefusesTheClosingLease(t *testing.T) {
	b := StartBackendWithConfig(t, retentionSettings(map[string]any{
		"max_retained_disk_mb": 2 * statefulDiskMB,
		"retention_tenant_budgets": map[string]any{
			"tenant-a": map[string]any{"max_retained_leases": 10, "max_retained_disk_mb": 2 * statefulDiskMB},
		},
	}))
	others := retire(t, b, "tenant-b", "")
	own := retire(t, b, "tenant-a", "")
	requireRetained(t, b, others, own)

	refused := retire(t, b, "tenant-a", "")
	requireDestroyed(t, b, refused)
	requireRetained(t, b, others, own)
}

// TestRetention_TenantDiskCapRefusesTheClosingLease fills one tenant's
// retained-disk cap. That tenant's next closing lease is destroyed; its
// retained lease stays, and another tenant can still retain.
func TestRetention_TenantDiskCapRefusesTheClosingLease(t *testing.T) {
	b := StartBackendWithConfig(t, retentionSettings(map[string]any{"max_retained_disk_mb_per_tenant": statefulDiskMB}))
	kept := retire(t, b, "tenant-a", "")
	requireRetained(t, b, kept)

	refused := retire(t, b, "tenant-a", "")
	requireDestroyed(t, b, refused)
	requireRetained(t, b, kept)

	requireRetained(t, b, kept, retire(t, b, "tenant-b", ""))
}

// TestRetention_PartitionDiskSubCapRefusesTheClosingLease fills one
// partition's retained-disk sub-cap. The partition's next closing lease is
// destroyed; its retained lease stays, and another partition of the same
// tenant can still retain.
func TestRetention_PartitionDiskSubCapRefusesTheClosingLease(t *testing.T) {
	// The backend accepts a disk sub-cap only next to a count sub-cap. This
	// one is never reached: the disk sub-cap holds one lease.
	b := partitionedBackend(t, map[string]any{"per_partition_max_disk_mb": statefulDiskMB, "per_partition_max_leases": 2})
	kept := retire(t, b, "tenant-a", "p1")
	requireRetained(t, b, kept)

	refused := retire(t, b, "tenant-a", "p1")
	requireDestroyed(t, b, refused)
	requireRetained(t, b, kept)

	requireRetained(t, b, kept, retire(t, b, "tenant-a", "p2"))
}

// TestRetention_PartitionNeedsAListedTenantAndOneValidValue checks which
// closes land in a partition. Only a tenant on the budget list gets one, only
// from a valid value that all its services agree on, and only while it has
// partitions left. Every other close is retained in the tenant's default
// partition: a partition never decides whether data is kept.
func TestRetention_PartitionNeedsAListedTenantAndOneValidValue(t *testing.T) {
	// The disk figures are the size of the fixture's loopback, so that the
	// eight volumes below fit.
	b := StartBackendWithConfig(t, retentionSettings(map[string]any{
		"total_disk_mb":              512,
		"retention_partition_source": "manifest.env:" + partitionKey,
		"retention_tenant_budgets": map[string]any{
			"tenant-a": map[string]any{"max_retained_leases": 10, "max_retained_disk_mb": 512, "max_partitions": 1},
		},
	}))
	var retained []string
	// requirePartition waits for the lease to join the retained ones, and
	// checks the partition the backend reports for it.
	requirePartition := func(leaseUUID, want, why string) {
		t.Helper()
		retained = append(retained, leaseUUID)
		leases := requireRetained(t, b, retained...)
		require.Equal(t, want, leases[len(leases)-1].Partition, why)
	}

	requirePartition(retire(t, b, "tenant-b", "p1"), "", "a tenant that is not on the budget list gets no partition")
	requirePartition(retire(t, b, "tenant-a", "p1"), "p1", "a listed tenant's valid value is its partition")
	requirePartition(retire(t, b, "tenant-a", ""), "", "a lease that names no partition gets none")
	requirePartition(retire(t, b, "tenant-a", "not a valid value!"), "", "an invalid value is not a partition")

	stack := Spec{
		LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatefulSKU, Services: []string{"one", "two"},
		Manifest: map[string]any{"services": map[string]any{"one": statefulManifest("p1"), "two": statefulManifest("p2")}},
	}
	require.NoError(t, b.Client.Apply(t.Context(), stack))
	requireStatus(t, b, stack.LeaseUUID, statusReady)
	closeLease(t, b, stack.LeaseUUID)
	requirePartition(stack.LeaseUUID, "", "services that disagree give the lease no partition")
	roots, err := volumeRoots(b, stack.LeaseUUID)
	require.NoError(t, err)
	require.Len(t, roots, 2, "each service of the stack keeps its volume")

	requirePartition(retire(t, b, "tenant-a", "p2"), "", "a tenant at its partition limit gets no new partition")
	requirePartition(retire(t, b, "tenant-a", "p1"), "p1", "a partition the tenant already has still takes leases")
}

// TestRetention_RestartKeepsCountsFootprintsAndPartitions restarts a backend
// that holds retained leases, and checks that it decides the next closes as
// it would have without the restart. A disk cap must still count the leases
// retained before the restart, and a count sub-cap must still find the lease
// of the closing lease's partition.
func TestRetention_RestartKeepsCountsFootprintsAndPartitions(t *testing.T) {
	b := partitionedBackend(t, map[string]any{"max_retained_disk_mb": 2 * statefulDiskMB, "per_partition_max_leases": 1})
	first := retire(t, b, "tenant-a", "p1")
	elsewhere := retire(t, b, "tenant-a", "p2")
	before := requireRetained(t, b, first, elsewhere)
	require.Equal(t, []string{"p1", "p2"}, []string{before[0].Partition, before[1].Partition})

	b.Stop()
	b.Start()
	require.Equal(t, before, requireRetained(t, b, first, elsewhere), "retained leases and their partitions after a restart")

	// Footprints: the tenant's two retained leases fill its disk budget, so a
	// lease closing in a new partition is refused.
	refused := retire(t, b, "tenant-a", "p3")
	requireDestroyed(t, b, refused)
	requireRetained(t, b, first, elsewhere)

	// Counts and partitions: p1 already holds its one lease, so a close in p1
	// replaces it. That frees the disk the new lease needs.
	second := retire(t, b, "tenant-a", "p1")
	requireDestroyed(t, b, first)
	requireRetained(t, b, elsewhere, second)
}

// TestRetention_RetainedDiskCountsAgainstCapacity fills the backend's disk
// with retained leases and runs nothing. A new lease with a volume is
// refused for capacity: the disk it needs is held only by closed leases. No
// retained lease is given up to admit it.
func TestRetention_RetainedDiskCountsAgainstCapacity(t *testing.T) {
	// The fixture's total_disk_mb is 256: four footprints.
	b := StartBackendWithConfig(t, retentionSettings(nil))
	retained := make([]string, 0, 4)
	for range cap(retained) {
		retained = append(retained, retire(t, b, "tenant-a", ""))
	}
	requireRetained(t, b, retained...)

	refused := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-b", SKU: StatefulSKU, Manifest: statefulManifest("")}
	err := b.Client.Apply(t.Context(), refused)
	var status *StatusError
	require.True(t, errors.As(err, &status), "a lease that does not fit must be refused: %v", err)
	require.Equal(t, CodeInsufficientResources, status.Code)
	require.Empty(t, b.Containers(t, refused.LeaseUUID))
	requireRetained(t, b, retained...)
}
