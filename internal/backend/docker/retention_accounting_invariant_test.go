package docker

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
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
	activeMB, _, _, err := b.computeRetainedDiskMB()
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

	activeMB, _, _, err := b.computeRetainedDiskMB()
	require.NoError(t, err)
	reapingMB, _, err := b.computeReapingDiskMB()
	require.NoError(t, err)
	require.Equal(t, int64(1024), activeMB+reapingMB, "store reflects the new active record")
	require.Equal(t, int64(0), b.pool.Stats().RetainedDiskMB, "cache is stale — refresh was skipped")

	// The two values must differ here — require.NotEqual is the exact public
	// inverse of the assert.Equal the invariant helper uses, so this proves a
	// skipped-refresh drift is detectable. Without it, the positive cases pass
	// against already-correct code and prove nothing.
	require.NotEqual(t, activeMB+reapingMB, b.pool.Stats().RetainedDiskMB,
		"invariant check must detect a skipped-refresh drift (else it has no teeth)")

	// After the refresh, the invariant is restored.
	b.refreshRetentionAccounting()
	assertRetentionAccountingConsistent(t, b, "after refresh: cache re-derived from store")
	require.Equal(t, int64(1024), b.pool.Stats().RetainedDiskMB)
}

// TestRetentionAccountingInvariant_HoldsAfterTransition drives the
// unit-drivable self-refreshing retention transition entry points and asserts
// the invariant holds afterward. Each case leaves a NON-ZERO retained footprint
// so the assertion is non-trivial (not a vacuous 0 == 0). The close path
// (Deprovision with RetainOnClose=true) is the primary refresh site but needs
// the httptest/callback wiring the table intentionally avoids, so it is a
// standalone case: TestRetentionAccountingInvariant_AfterDeprovisionRetainClose.
// Methods that deliberately delegate the refresh to their caller
// (retryReapingRecords, reconcileOrphanedRetentions) are intentionally NOT
// asserted in isolation — they are exercised via runRetentionSweep, which owns
// the trailing refresh.
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

				require.NoError(t, b.evictRetentionsToCap(context.Background(), "t1", retentionBudget{CountCap: 2}, "", retentionTenantSnapshot(t, rs, "t1"), ""))
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
		{
			name: "evictRetentionsToCap two-level",
			run: func(t *testing.T, b *Backend, rs *shared.RetentionStore) {
				b.volumes = newVols()
				// One aggregator, two partitions. Partition A holds 3, B holds 1.
				// Budget: L2 per-partition cap 2 (keeps 1 in A, evicts 2) and L1
				// aggregate cap 10 (no-op). The closing lease is in partition A.
				base := time.Now().Add(-4 * time.Hour)
				putActivePart(t, rs, "a1", "agg", "A", base)
				putActivePart(t, rs, "a2", "agg", "A", base.Add(time.Hour))
				putActivePart(t, rs, "a3", "agg", "A", base.Add(2*time.Hour))
				putActivePart(t, rs, "b1", "agg", "B", base.Add(3*time.Hour))

				budget := retentionBudget{CountCap: 10, PerPartCount: 2, MaxPartitions: 4}
				require.NoError(t, b.evictRetentionsToCap(context.Background(), "agg", budget, "A",
					retentionTenantSnapshot(t, rs, "agg"), ""))
				// A evicted to 1 (a3 survives), B untouched → 2 active × 512 = 1024 MB.
				require.Equal(t, int64(1024), b.pool.Stats().RetainedDiskMB)
			},
		},
		{
			name: "scoped disk refusal",
			run: func(t *testing.T, b *Backend, rs *shared.RetentionStore) {
				b.volumes = newVols()
				// Two held records fill the tenant's disk budget (2 × 512 = 1024).
				putActivePart(t, rs, "held-1", "agg", "", time.Now().Add(-2*time.Hour))
				putActivePart(t, rs, "held-2", "agg", "", time.Now().Add(-time.Hour))
				b.refreshRetentionAccounting()

				// An incoming close of one more docker-micro breaches the tenant disk
				// cap → refuse at scope=tenant. The refusal destroys only the incoming
				// lease's (canonical) volumes; nothing RETAINED is mutated.
				budget := retentionBudget{DiskCapMB: 1024}
				incoming := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}}
				scope, refuse := b.shouldRefuseRetention("incoming", "agg", "", incoming, budget)
				require.True(t, refuse)
				require.Equal(t, refuseScopeTenant, scope)
				errs := b.destroyOnRefuseToRetain(context.Background(), []string{"fred-incoming-app-0"},
					"incoming", "agg", "", scope, slog.Default())
				require.Empty(t, errs)
				// Both held records survive; the projection is unchanged at 1024 MB.
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

// TestRetentionAccountingInvariant_AfterDeprovisionRetainClose drives the real
// close path — Deprovision with RetainOnClose=true — end to end and asserts the
// retained-disk accounting invariant holds after the live→retained hand-off.
// Close is the primary refresh site (deprovision.go:166,597; the first entry in
// refreshRetentionAccounting's "close, reap, evict" doc list) and needs the full
// httptest/callback wiring the table above avoids, so it is a standalone case.
func TestRetentionAccountingInvariant_AfterDeprovisionRetainClose(t *testing.T) {
	callbackDone := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case callbackDone <- struct{}{}:
		default:
		}
	}))
	defer server.Close()

	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	// docker-micro at 512 MB; qty=2 → footprint F = 1024 MB.
	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}}
	canonical0 := canonicalVolumeName("lease-close", "web", 0)
	canonical1 := canonicalVolumeName("lease-close", "web", 1)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-close": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-close", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "nginx:1.25"}}},
		}},
	})

	withMicroSKU(b, 512) // align the pool resolver (defaultTestSKUProfiles: 512) and computeRetainedDiskMB
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-close-web-0", "docker-micro", "tenant-a"))
	require.NoError(t, b.pool.TryAllocate("lease-close-web-1", "docker-micro", "tenant-a"))
	require.Equal(t, int64(1024), b.pool.Stats().AllocatedDiskMB, "pre: live F=1024 MB")

	b.volumes = &mockVolumeManager{
		ListFn:         func() ([]string, error) { return []string{canonical0, canonical1}, nil },
		RenameVolumeFn: func(_, _ string) error { return nil },
	}

	require.NoError(t, b.Deprovision(context.Background(), "lease-close"))
	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	// After a successful retain-close hand-off: live released, F counted as
	// retained, and the invariant holds (cache == recompute-from-store).
	s := b.pool.Stats()
	assert.Equal(t, int64(0), s.AllocatedDiskMB, "live released after retain close")
	assert.Equal(t, int64(1024), s.RetainedDiskMB, "F counted as retained after close")
	assertRetentionAccountingConsistent(t, b, "invariant must hold after Deprovision retain-close")
}
