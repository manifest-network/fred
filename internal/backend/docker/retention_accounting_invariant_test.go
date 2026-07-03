package docker

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// ENG-456: the cached admission projection (pool.retainedDisk, read via
// Stats().RetainedDiskMB) is PUSH-maintained — every retention transition must
// call refreshRetentionAccounting to re-derive it from the store. A future
// transition path that mutates the store but forgets that call drifts the cache
// stale-low → admission over-admits → tenant ENOSPC. These tests pin the
// invariant "cache == recompute-from-store" after every self-refreshing
// transition entry point, and prove (teeth) that the check detects a skipped
// refresh. The design decision to keep this a recompute-checked cache rather
// than a maintained in-store aggregate is recorded on the ticket.

// assertRetentionAccountingConsistent asserts the ENG-456 invariant: the cached
// admission projection equals the value recomputed from the retention store
// (active + reaping). This is exactly what refreshRetentionAccounting sets, so it
// must hold at rest after any transition whose contract is to leave accounting
// consistent. A transition that mutates the store but skips the refresh breaks it.
func assertRetentionAccountingConsistent(t *testing.T, b *Backend, msgAndArgs ...any) {
	t.Helper()
	activeMB, _, err := b.computeRetainedDiskMB()
	require.NoError(t, err)
	reapingMB, _, err := b.computeReapingDiskMB()
	require.NoError(t, err)
	assert.Equal(t, activeMB+reapingMB, b.pool.Stats().RetainedDiskMB, msgAndArgs...)
}

// TestRetentionAccountingInvariant_TeethDetectsSkippedRefresh is the negative
// control: it proves assertRetentionAccountingConsistent has teeth. It mutates
// the store WITHOUT refreshing (modelling a new transition path that forgot the
// refresh) and asserts the exact equality the helper uses reports drift — then
// that a refresh restores it. Without this, the positive cases below (which pass
// against already-correct code) would prove nothing.
func TestRetentionAccountingInvariant_TeethDetectsSkippedRefresh(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 512)

	// Consistent baseline: empty store, refreshed → 0 == 0.
	b.refreshRetentionAccounting()
	assertRetentionAccountingConsistent(t, b, "baseline: empty store")

	// Mutate the store WITHOUT refreshing. docker-micro qty 2 → 2 × 512 = 1024 MB.
	require.NoError(t, rs.Put(retentionEntryFixture("drifted", "t1", time.Now())))

	activeMB, _, err := b.computeRetainedDiskMB()
	require.NoError(t, err)
	reapingMB, _, err := b.computeReapingDiskMB()
	require.NoError(t, err)
	require.Equal(t, int64(1024), activeMB+reapingMB, "store reflects the new active record")
	require.Equal(t, int64(0), b.pool.Stats().RetainedDiskMB, "cache is stale — refresh was skipped")

	// The equality the invariant helper asserts (assert.Equal → ObjectsAreEqual)
	// must report drift here. If it did NOT, the helper would be a tautology.
	require.False(t,
		assert.ObjectsAreEqual(activeMB+reapingMB, b.pool.Stats().RetainedDiskMB),
		"invariant check must detect a skipped-refresh drift (else it has no teeth)")

	// After the refresh, the invariant is restored.
	b.refreshRetentionAccounting()
	assertRetentionAccountingConsistent(t, b, "after refresh: cache re-derived from store")
	require.Equal(t, int64(1024), b.pool.Stats().RetainedDiskMB)
}

// TestRetentionAccountingInvariant_HoldsAfterTransition drives each real,
// self-refreshing retention transition entry point and asserts the invariant
// holds afterward. Each case leaves a NON-ZERO retained footprint so the
// assertion is non-trivial (not a vacuous 0 == 0). Methods that deliberately
// delegate the refresh to their caller (retryReapingRecords,
// reconcileOrphanedRetentions) are intentionally NOT asserted in isolation —
// they are exercised via runRetentionSweep, which owns the trailing refresh.
func TestRetentionAccountingInvariant_HoldsAfterTransition(t *testing.T) {
	// A fake volume backend whose Destroy/Rename/List all succeed, so reap/evict
	// fully reap (delete) their records and restore rollbacks re-quarantine.
	newVols := func() *fakeVolumeBackend { return &fakeVolumeBackend{} }

	cases := []struct {
		name string
		// run seeds state and drives the real transition. b already has
		// withMicroSKU(512) applied and an empty retention store.
		run func(t *testing.T, b *Backend, rs *shared.RetentionStore)
	}{
		{
			name: "reapExpiredRetentions",
			run: func(t *testing.T, b *Backend, rs *shared.RetentionStore) {
				b.volumes = newVols()
				b.cfg.RetentionMaxAge = time.Hour
				// Expired record → reaped (destroyed + deleted); fresh record survives.
				require.NoError(t, rs.Put(retentionEntryFixture("expired", "t1", time.Now().Add(-2*time.Hour))))
				require.NoError(t, rs.Put(retentionEntryFixture("fresh", "t1", time.Now())))

				n, err := b.reapExpiredRetentions(context.Background())
				require.NoError(t, err)
				require.Equal(t, 1, n, "exactly the expired record is reaped")
				// Only the fresh record remains active → 2 × 512 = 1024 MB.
				require.Equal(t, int64(1024), b.pool.Stats().RetainedDiskMB)
			},
		},
		{
			name: "evictRetentionsToCap",
			run: func(t *testing.T, b *Backend, rs *shared.RetentionStore) {
				b.volumes = newVols()
				// Three active records for one tenant; cap = 2 → the two oldest are
				// evicted (active→reaping→destroyed+deleted), leaving one active.
				base := time.Now().Add(-3 * time.Hour)
				require.NoError(t, rs.Put(retentionEntryFixture("old1", "t1", base)))
				require.NoError(t, rs.Put(retentionEntryFixture("old2", "t1", base.Add(time.Hour))))
				require.NoError(t, rs.Put(retentionEntryFixture("newest", "t1", base.Add(2*time.Hour))))

				require.NoError(t, b.evictRetentionsToCap(context.Background(), "t1", 2, ""))
				// One active record remains → 2 × 512 = 1024 MB.
				require.Equal(t, int64(1024), b.pool.Stats().RetainedDiskMB)
			},
		},
		{
			name: "runRetentionSweep",
			run: func(t *testing.T, b *Backend, rs *shared.RetentionStore) {
				b.volumes = newVols()
				b.cfg.RetentionMaxAge = time.Hour
				// Composite sweep: reap expired + retry a reaping tombstone + refresh.
				// A fresh active record survives; both the expired and the reaping
				// record are destroyed and deleted.
				require.NoError(t, rs.Put(retentionEntryFixture("survives", "t1", time.Now())))
				require.NoError(t, rs.Put(retentionEntryFixture("expired", "t1", time.Now().Add(-2*time.Hour))))
				reaping := retentionEntryFixture("tombstone", "t1", time.Now())
				reaping.Status = shared.RetentionStatusReaping
				require.NoError(t, rs.Put(reaping))

				require.NoError(t, b.runRetentionSweep(context.Background()))
				// Only the survivor remains active → 1024 MB, reaping cleared to 0.
				require.Equal(t, int64(1024), b.pool.Stats().RetainedDiskMB)
			},
		},
		{
			name: "reconcileRestoring_orphanedRevert",
			run: func(t *testing.T, b *Backend, rs *shared.RetentionStore) {
				b.volumes = newVols() // RenameVolume succeeds → re-quarantine works
				const origLease, newLease = "orig-r", "new-r"
				entry := shared.RetentionEntry{
					OriginalLeaseUUID:   origLease,
					NewLeaseUUID:        newLease,
					Tenant:              "t1",
					Status:              shared.RetentionStatusRestoring,
					Generation:          1,
					Items:               []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}},
					RetainedVolumeNames: []string{"fred-retained-orig-r-web-0"},
				}
				require.NoError(t, rs.Put(entry))
				// New-lease live allocation (no live provision → orphaned arm reverts it).
				require.NoError(t, b.pool.TryAllocate(newLease+"-web-0", "docker-micro", "t1"))
				require.Equal(t, int64(0), b.pool.Stats().RetainedDiskMB, "pre: record restoring → not counted")

				b.reconcileRestoring(context.Background(), entry)
				// Record reverted to active → 1 × 512 = 512 MB retained, live released.
				require.Equal(t, int64(512), b.pool.Stats().RetainedDiskMB)
				require.Equal(t, int64(0), b.pool.Stats().AllocatedDiskMB, "new-lease live released")
			},
		},
		{
			name: "rollbackRestoreAdoption",
			run: func(t *testing.T, b *Backend, rs *shared.RetentionStore) {
				b.volumes = newVols() // RenameVolume succeeds → record reverts to active
				const origLease, newLease = "orig-rb", "new-rb"
				rec := shared.RetentionEntry{
					OriginalLeaseUUID:   origLease,
					NewLeaseUUID:        newLease,
					Tenant:              "t1",
					Status:              shared.RetentionStatusRestoring,
					Generation:          1,
					Items:               []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}},
					RetainedVolumeNames: []string{"fred-retained-orig-rb-web-0"},
				}
				require.NoError(t, rs.Put(rec))
				allocID := newLease + "-web-0"
				require.NoError(t, b.pool.TryAllocate(allocID, "docker-micro", "t1"))

				b.rollbackRestoreAdoption(context.Background(), newLease, []string{allocID}, &rec, false, slog.Default())
				// Record reverted to active → 512 MB retained, live released.
				require.Equal(t, int64(512), b.pool.Stats().RetainedDiskMB)
				require.Equal(t, int64(0), b.pool.Stats().AllocatedDiskMB, "new-lease live released")
			},
		},
		{
			name: "recoverState",
			run: func(t *testing.T, b *Backend, rs *shared.RetentionStore) {
				// recoverState rebuilds the retained projection from the store after
				// Reset zeroes live. No live containers.
				b.docker = &mockDockerClient{
					ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) { return nil, nil },
				}
				require.NoError(t, rs.Put(retentionEntryFixture("recovered", "t1", time.Now())))

				require.NoError(t, b.recoverState(context.Background()))
				// One active record → 2 × 512 = 1024 MB rebuilt into the pool.
				require.Equal(t, int64(1024), b.pool.Stats().RetainedDiskMB)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, rs := newBackendWithRetention(t)
			withMicroSKU(b, 512) // align the pool resolver (defaultTestSKUProfiles: 512) and computeRetainedDiskMB

			tc.run(t, b, rs)

			assertRetentionAccountingConsistent(t, b, "invariant must hold after "+tc.name)
			assert.Positive(t, b.pool.Stats().RetainedDiskMB,
				"case must leave a non-trivial retained footprint (not a vacuous 0 == 0 check)")
		})
	}
}
