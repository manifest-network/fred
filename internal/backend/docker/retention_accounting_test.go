package docker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// withMicroSKU sets a known stateful SKU on the test backend's config so the
// projection (which reads b.cfg.GetSKUProfile) resolves a deterministic DiskMB.
func withMicroSKU(b *Backend, diskMB int64) {
	b.cfg.SKUProfiles = map[string]shared.SKUProfile{
		"docker-micro": {CPUCores: 1, MemoryMB: 256, DiskMB: diskMB},
	}
}

func TestComputeRetainedDiskMB(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1024)

	// Two active retained leases: one with qty 2, one with qty 1 → 3 * 1024 MB.
	require.NoError(t, rs.Put(retentionEntryFixture("lease-a", "t1", time.Now()))) // qty 2
	one := retentionEntryFixture("lease-b", "t1", time.Now())
	one.Items = []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}}
	require.NoError(t, rs.Put(one))
	// A restoring record must NOT count toward the active projection.
	restoring := retentionEntryFixture("lease-c", "t1", time.Now())
	restoring.Status = shared.RetentionStatusRestoring
	require.NoError(t, rs.Put(restoring))

	mb, count, err := b.computeRetainedDiskMB()
	require.NoError(t, err)
	assert.Equal(t, int64(3*1024), mb)
	assert.Equal(t, 2, count)
}

func TestRefreshRetentionAccounting_PushesToPoolAndMetrics(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	require.NoError(t, rs.Put(retentionEntryFixture("lease-a", "t1", time.Now()))) // qty 2 → 2048 MB

	b.refreshRetentionAccounting()
	assert.Equal(t, int64(2048), b.pool.Stats().RetainedDiskMB)
	assert.Equal(t, float64(2048)*bytesPerMiB, testutil.ToFloat64(retainedVolumeBytes))
	assert.Equal(t, float64(1), testutil.ToFloat64(retainedVolumes))

	// Remove the record and refresh → projection drops to 0.
	require.NoError(t, rs.Delete("lease-a"))
	b.refreshRetentionAccounting()
	assert.Equal(t, int64(0), b.pool.Stats().RetainedDiskMB)
	assert.Equal(t, float64(0), testutil.ToFloat64(retainedVolumeBytes))
	assert.Equal(t, float64(0), testutil.ToFloat64(retainedVolumes))
}

func TestRecoverRebuildsRetainedProjection(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	// 1024 (not 512 like the ordering test) is fine: this test exercises only the
	// computeRetainedDiskMB path (reads b.cfg.GetSKUProfile), never b.pool.TryAllocate,
	// so the pool's constructor-time resolver is not involved.
	require.NoError(t, rs.Put(retentionEntryFixture("lease-a", "t1", time.Now()))) // 2048 MB

	// Wire ListManagedContainers to return no containers so recoverState can
	// run without a full Docker daemon; the test exercises the retained
	// projection rebuild only.
	b.docker = &mockDockerClient{
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}

	// recoverState with no live containers must still rebuild the retained
	// projection from the store (pool.Reset zeroes live; refresh re-pushes
	// retained).
	require.NoError(t, b.recoverState(context.Background()))
	assert.Equal(t, int64(2048), b.pool.Stats().RetainedDiskMB)
}

// TestBreachRetentionCap covers the cap predicate: an unset cap (0) never
// breaches, and the boundary where current retained + the incoming lease's
// footprint crosses max_retained_disk_mb. Baseline is seeded via the retention
// store (the authoritative source) — not the cached pool projection.
func TestBreachRetentionCap(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1000) // 1000 MB per unit for clean math
	// Store holds one active retained lease = qty2 * 1000 = 2000 MB (TRUE retained).
	require.NoError(t, rs.Put(retentionEntryFixture("other", "t1", time.Now())))
	incoming := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}} // 2000 MB

	// Cap unset (0) → never breaches (even with a stale-high cache).
	b.cfg.MaxRetainedDiskMB = 0
	b.pool.SetRetainedDisk(1_000_000)
	assert.False(t, b.breachRetentionCap(incoming))

	// Cap 3000: true 2000 + incoming 2000 = 4000 > 3000 → breach.
	b.cfg.MaxRetainedDiskMB = 3000
	assert.True(t, b.breachRetentionCap(incoming))

	// Cap 5000: 2000 + 2000 = 4000 <= 5000 → no breach.
	b.cfg.MaxRetainedDiskMB = 5000
	assert.False(t, b.breachRetentionCap(incoming))
}

// TestBreachRetentionCap_UsesStoreNotStaleCache pins that the cap decision reads
// the retention store (the source of truth), not the cached pool projection. A
// stale-HIGH cache must not trigger a false breach.
func TestBreachRetentionCap_UsesStoreNotStaleCache(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1000)
	b.cfg.MaxRetainedDiskMB = 5000
	require.NoError(t, rs.Put(retentionEntryFixture("other", "t1", time.Now())))            // TRUE retained = 2000
	b.pool.SetRetainedDisk(9_999_999)                                                       // stale-HIGH cache (e.g. a just-reaped record not yet refreshed)
	incoming := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}} // 2000
	// TRUE: 2000 + 2000 = 4000 <= 5000 → must NOT breach, even though the stale cache (9.9M) would.
	assert.False(t, b.breachRetentionCap(incoming),
		"cap decision must use the authoritative store total, not the stale cached projection")
}

// TestBreachRetentionCap_StoreErrorFailsOpen verifies that a retention store read
// error causes breachRetentionCap to fail open (not refuse). Refuse-to-retain
// destroys volumes; uncertainty must never trigger destruction.
func TestBreachRetentionCap_StoreErrorFailsOpen(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1000)
	b.cfg.MaxRetainedDiskMB = 100  // tight: would breach if it reached the comparison
	require.NoError(t, rs.Close()) // computeRetainedDiskMB now errors
	assert.False(t, b.breachRetentionCap([]backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}}),
		"a store read error must fail open (not breach → not destroy)")
}

// TestRestoreOrdering_NeverUnderCounts verifies that live+retained never dips
// below the lease's footprint during the claim window (active→restoring flip).
// Note: the pool resolver was bound at construction time to the original cfg
// (DiskMB=512 for docker-micro from defaultTestSKUProfiles), while
// b.cfg.GetSKUProfile (used by computeRetainedDiskMB) sees the withMicroSKU
// override. To keep the invariant consistent, this test uses DiskMB=512 so
// both the pool allocator and the retention projection agree on the per-unit
// cost. The fixture has qty=2 → footprint = 2 * 512 = 1024 MB.
func TestRestoreOrdering_NeverUnderCounts(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	// Use 512 MB to match the pool resolver's view of docker-micro
	// (defaultTestSKUProfiles: DiskMB=512). computeRetainedDiskMB also
	// reads b.cfg.GetSKUProfile; withMicroSKU(b, 512) aligns both views.
	withMicroSKU(b, 512)
	require.NoError(t, rs.Put(retentionEntryFixture("lease-a", "t1", time.Now()))) // active, 2 x 512 = 1024 MB
	b.refreshRetentionAccounting()
	require.Equal(t, int64(1024), b.pool.Stats().RetainedDiskMB)

	// Claim ordering: the restored lease is allocated LIVE before the record
	// flips active→restoring (which a refresh drops from the active projection).
	// Model that order; live+retained must never dip below the footprint
	// (under-count == over-admit, the dangerous direction).
	require.NoError(t, b.pool.TryAllocate("lease-a-web-0", "docker-micro", "t1")) // +512 live
	require.NoError(t, b.pool.TryAllocate("lease-a-web-1", "docker-micro", "t1")) // +512 live
	s := b.pool.Stats()
	assert.GreaterOrEqual(t, s.AllocatedDiskMB+s.RetainedDiskMB, int64(1024),
		"live+retained must never under-count during the claim window")

	// Record flips to restoring; refresh drops it from the active projection.
	entry := retentionEntryFixture("lease-a", "t1", time.Now())
	entry.Status = shared.RetentionStatusRestoring
	require.NoError(t, rs.Put(entry))
	b.refreshRetentionAccounting()
	s = b.pool.Stats()
	assert.Equal(t, int64(0), s.RetainedDiskMB, "restoring record leaves the active projection")
	assert.Equal(t, int64(1024), s.AllocatedDiskMB, "bytes are now counted as live")
	assert.GreaterOrEqual(t, s.AllocatedDiskMB+s.RetainedDiskMB, int64(1024))
}

func TestDiskOverProvisioned(t *testing.T) {
	assert.True(t, diskOverProvisioned(200, 100), "total above usable is over-provisioned")
	assert.False(t, diskOverProvisioned(100, 100), "equal is fine")
	assert.False(t, diskOverProvisioned(50, 100), "below usable is fine")
}

func TestRetentionCapNeedsTenantLever(t *testing.T) {
	// cap set, no per-tenant count cap → needs the lever (warn).
	assert.True(t, retentionCapNeedsTenantLever(Config{MaxRetainedDiskMB: 4096, MaxRetainedLeasesPerTenant: 0}))
	// cap set, per-tenant count cap also set → fine.
	assert.False(t, retentionCapNeedsTenantLever(Config{MaxRetainedDiskMB: 4096, MaxRetainedLeasesPerTenant: 5}))
	// no cap → nothing to warn about.
	assert.False(t, retentionCapNeedsTenantLever(Config{MaxRetainedDiskMB: 0, MaxRetainedLeasesPerTenant: 0}))
}

func TestRefuseToRetain_DestroysAndCounts(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	b.cfg.MaxRetainedDiskMB = 1000 // smaller than a single 2 x 1024 = 2048 MB lease
	b.pool.SetRetainedDisk(0)

	// A capturing fake volume manager records Destroy calls (mirrors the
	// fakeVolumeBackend pattern in testsupport_test.go).
	fv := &fakeVolumeBackend{}
	b.volumes = fv

	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}}
	require.True(t, b.breachRetentionCap(items), "a 2048 MB lease must breach a 1000 MB cap")

	before := testutil.ToFloat64(retentionRefusedTotal)
	canonical := []string{"fred-lease-z-web-0", "fred-lease-z-web-1"}
	errs := b.destroyOnRefuseToRetain(context.Background(), canonical, "lease-z", "t1", b.logger)

	assert.Empty(t, errs)
	assert.Equal(t, before+1, testutil.ToFloat64(retentionRefusedTotal), "production code increments the counter")
	assert.Equal(t, canonical, fv.destroyed, "both canonical volumes must be destroyed on refuse-to-retain")
}

func TestShouldRefuseRetention_UnlimitedSkipsStoreRead(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	b.cfg.MaxRetainedDiskMB = 0 // unlimited
	// Close the store so any Get would error; the unlimited path must not touch it.
	require.NoError(t, rs.Close())
	assert.False(t, b.shouldRefuseRetention("lease-x",
		[]backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}}),
		"unlimited cap must return false without reading the retention store")
}

func TestShouldRefuseRetention_SkipsWhenAlreadyRetained(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	b.cfg.MaxRetainedDiskMB = 1000 // tight: a 2 x 1024 = 2048 MB lease breaches
	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}}

	// No record yet → breach → refuse.
	b.pool.SetRetainedDisk(0)
	assert.True(t, b.shouldRefuseRetention("lease-x", items))

	// A prior attempt already wrote an ACTIVE record for this lease (retry case);
	// its footprint is already in retainedDisk. Re-deciding must NOT refuse
	// (no double-count, no inconsistent destroy-some/retain-some state).
	require.NoError(t, rs.Put(retentionEntryFixture("lease-x", "t1", time.Now()))) // active, docker-micro qty 2
	b.pool.SetRetainedDisk(2048)                                                   // reflects the existing record
	assert.False(t, b.shouldRefuseRetention("lease-x", items), "retry of an already-retained lease must not be refused")
}

func TestLeaseDiskMB_UnknownSKUSkipped(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	items := []backend.LeaseItem{
		{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}, // 2048
		{SKU: "ghost-sku", Quantity: 5, ServiceName: "db"},     // unknown → skipped + reported
	}
	mb, unresolved := b.leaseDiskMB(items)
	assert.Equal(t, int64(2048), mb)
	assert.Equal(t, []string{"ghost-sku"}, unresolved)
}

func TestShouldRefuseRetention_SkipsWhenRestoring(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	b.cfg.MaxRetainedDiskMB = 1000 // tight: a 2x1024 lease would breach
	b.pool.SetRetainedDisk(2048)
	entry := retentionEntryFixture("lease-x", "t1", time.Now())
	entry.Status = shared.RetentionStatusRestoring
	require.NoError(t, rs.Put(entry))
	assert.False(t, b.shouldRefuseRetention("lease-x",
		[]backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}}),
		"a restore-claimed (restoring) record must not be refused — defer to PutActiveMerged ok=false")
}

func TestShouldRefuseRetention_StoreReadErrorDoesNotRefuse(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	b.cfg.MaxRetainedDiskMB = 1000 // tight: would breach if the cap check were reached
	b.pool.SetRetainedDisk(2048)
	require.NoError(t, rs.Close()) // any Get now errors (closeOnce makes the t.Cleanup double-close harmless)
	assert.False(t, b.shouldRefuseRetention("lease-x",
		[]backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}}),
		"a retention-store read error must NOT refuse (refuse destroys volumes; fail-open is data-safe)")
}

// TestCloseRetainOrdering_NeverUnderCounts verifies that the close-retain
// hand-off keeps the closing lease's footprint F continuously counted (as live,
// then as retained) with no gap while the renamed volume persists on disk.
//
// Mirrors TestRestoreOrdering_NeverUnderCounts for the inverse direction:
// restore moves bytes from retained→live before the record flips active→restoring;
// close moves bytes from live→retained before/during the release.
//
// Use DiskMB=512 to match the pool resolver's view (defaultTestSKUProfiles).
// The fixture has qty=2 → footprint F = 2 × 512 = 1024 MB.
func TestCloseRetainOrdering_NeverUnderCounts(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 512) // aligns pool resolver and computeRetainedDiskMB

	// Simulate a live lease: allocate its two units in the pool.
	require.NoError(t, b.pool.TryAllocate("lease-x-web-0", "docker-micro", "t1"))
	require.NoError(t, b.pool.TryAllocate("lease-x-web-1", "docker-micro", "t1"))
	s := b.pool.Stats()
	require.Equal(t, int64(1024), s.AllocatedDiskMB, "pre-condition: live F must be counted")
	require.Equal(t, int64(0), s.RetainedDiskMB)

	// Write the retained record (models doDeprovision having called PutActiveMerged).
	require.NoError(t, rs.Put(retentionEntryFixture("lease-x", "t1", time.Now()))) // active, 2×512=1024 MB

	// The correct hand-off order: refresh (counts F as retained) THEN release
	// (removes F from live). At every observed step, live+retained >= F.
	b.refreshRetentionAccounting() // retained now reflects the store (F counted there)
	s = b.pool.Stats()
	assert.GreaterOrEqual(t, s.AllocatedDiskMB+s.RetainedDiskMB, int64(1024),
		"after refresh but before release: live+retained must be >= F (overlap, no gap)")
	assert.Equal(t, int64(1024), s.RetainedDiskMB, "retained must count F after refresh")

	// Now release live (hand-off complete).
	b.pool.Release(fmt.Sprintf("%s-%s-%d", "lease-x", "web", 0))
	b.pool.Release(fmt.Sprintf("%s-%s-%d", "lease-x", "web", 1))
	s = b.pool.Stats()
	assert.Equal(t, int64(0), s.AllocatedDiskMB, "live must be zero after hand-off")
	assert.Equal(t, int64(1024), s.RetainedDiskMB, "retained must still count F after live released")
	assert.GreaterOrEqual(t, s.AllocatedDiskMB+s.RetainedDiskMB, int64(1024),
		"live+retained must be >= F throughout")
}

// TestCloseRetainOrdering_FailedRetainKeepsLiveCounted verifies that when
// the retain path fails (e.g. PutActiveMerged error or rename failure), the
// live allocation is NOT released — the volumes are still on disk under the
// lease's canonical names, and the footprint must stay counted to prevent
// a concurrent TryAllocate from over-admitting into that space.
func TestCloseRetainOrdering_FailedRetainKeepsLiveCounted(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 512)

	require.NoError(t, b.pool.TryAllocate("lease-y-web-0", "docker-micro", "t1"))
	require.NoError(t, b.pool.TryAllocate("lease-y-web-1", "docker-micro", "t1"))
	s := b.pool.Stats()
	require.Equal(t, int64(1024), s.AllocatedDiskMB, "pre-condition: live F must be counted")

	// NO retained record written (PutActiveMerged failed) — retained store is empty.
	// The refresh finds nothing active → retainedDiskMB stays 0.
	b.refreshRetentionAccounting()
	s = b.pool.Stats()
	assert.Equal(t, int64(0), s.RetainedDiskMB, "no record → retained must be 0")
	// On failure, the live release must NOT happen (caller keeps live counted).
	// This models the fix: releaseLiveOnRetainPath=false → releaseLive() not called.
	// Assert the invariant that, after refresh without a valid record, live is still counted.
	assert.Equal(t, int64(1024), s.AllocatedDiskMB,
		"on failed retain, live must remain counted (volumes still on disk)")
	assert.GreaterOrEqual(t, s.AllocatedDiskMB+s.RetainedDiskMB, int64(1024),
		"live+retained must never undercount while volumes are on disk")

	// Cleanup so the test doesn't leave pool state dirty.
	_ = rs.Close()
}

// TestDeprovision_RetainHandoff_LiveMovesToRetained is an end-to-end test
// that drives doDeprovision with RetainOnClose=true on a pre-allocated lease
// and asserts the pool accounting after the call returns:
//   - AllocatedDiskMB == 0 (live released)
//   - RetainedDiskMB == F (retained counted)
//
// This catches the early-release bug: before the fix, the pool.Release loop
// ran before PutActiveMerged+refreshRetentionAccounting, so between release
// and refresh the footprint was counted in neither pool — a concurrent
// TryAllocate could over-admit (ENOSPC).
func TestDeprovision_RetainHandoff_LiveMovesToRetained(t *testing.T) {
	callbackDone := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	// docker-micro at 512 MB to match the pool resolver (defaultTestSKUProfiles).
	// Qty=2 → footprint F = 1024 MB.
	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}}
	canonical0 := canonicalVolumeName("lease-z", "web", 0)
	canonical1 := canonicalVolumeName("lease-z", "web", 1)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-z": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-z", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "nginx:1.25"}}},
		}},
	})

	// Use 512 MB to match the pool resolver for docker-micro.
	withMicroSKU(b, 512)
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	// Pre-allocate the lease's footprint in the pool (simulates a live provision).
	require.NoError(t, b.pool.TryAllocate("lease-z-web-0", "docker-micro", "tenant-a"))
	require.NoError(t, b.pool.TryAllocate("lease-z-web-1", "docker-micro", "tenant-a"))
	require.Equal(t, int64(1024), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=1024 MB")

	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return []string{canonical0, canonical1}, nil
		},
		RenameVolumeFn: func(_, _ string) error { return nil },
	}

	require.NoError(t, b.Deprovision(context.Background(), "lease-z"))

	// Wait for the terminal callback so the deferred hand-off has run.
	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	s := b.pool.Stats()
	// After a successful retain close:
	//   live must be zero (released as part of the hand-off), and
	//   retained must equal F (refreshed after PutActiveMerged wrote the record).
	assert.Equal(t, int64(0), s.AllocatedDiskMB,
		"after successful retain close, live allocation must be zero")
	assert.Equal(t, int64(1024), s.RetainedDiskMB,
		"after successful retain close, F must be counted as retained (hand-off complete)")
}

// TestDeprovision_BranchSelection_OverCap pins doDeprovision's refuse-to-retain
// branch: when max_retained_disk_mb is tighter than the lease footprint (store
// empty → no existing retained bytes → retained+incoming > cap), doDeprovision
// must:
//   - Destroy the canonical volumes (NOT rename them)
//   - NOT write any retention record (retentionStore.Get returns nil after close)
//   - Increment retention_refused_total by 1
//
// This is an end-to-end drive of the real doDeprovision; the volume fake records
// Destroy calls so the test can assert the volumes were destroyed, not retained.
func TestDeprovision_BranchSelection_OverCap(t *testing.T) {
	callbackDone := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	// Cap = 500 MB < 1024 MB → shouldRefuseRetention returns true (breach).
	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}}
	canonical0 := canonicalVolumeName("lease-oc", "web", 0)
	canonical1 := canonicalVolumeName("lease-oc", "web", 1)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-oc": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-oc", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "nginx:1.25"}}},
		}},
	})

	withMicroSKU(b, 512)
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.cfg.MaxRetainedDiskMB = 500 // tight: 1024 MB lease breaches immediately
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-oc-web-0", "docker-micro", "tenant-a"))
	require.NoError(t, b.pool.TryAllocate("lease-oc-web-1", "docker-micro", "tenant-a"))

	// Track which volumes were destroyed vs renamed.
	var destroyed []string
	var renamed [][2]string
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return []string{canonical0, canonical1}, nil
		},
		DestroyFn: func(_ context.Context, id string) error {
			destroyed = append(destroyed, id)
			return nil
		},
		RenameVolumeFn: func(old, newName string) error {
			renamed = append(renamed, [2]string{old, newName})
			return nil
		},
	}

	before := testutil.ToFloat64(retentionRefusedTotal)
	require.NoError(t, b.Deprovision(context.Background(), "lease-oc"))

	// Wait for terminal callback (deferred accounting runs before it).
	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	// Volumes must be DESTROYED (not renamed).
	assert.ElementsMatch(t, []string{canonical0, canonical1}, destroyed,
		"over-cap: canonical volumes must be destroyed, not retained")
	assert.Empty(t, renamed, "over-cap: no volumes must be renamed")

	// Counter must have incremented by exactly 1.
	assert.Equal(t, before+1, testutil.ToFloat64(retentionRefusedTotal),
		"over-cap: retention_refused_total must increment by 1")

	// No retention record must exist in the store.
	rec, err := rs.Get("lease-oc")
	require.NoError(t, err)
	assert.Nil(t, rec, "over-cap: no active retention record must be written")
}

// TestDeprovision_BranchSelection_UnderCap pins doDeprovision's normal retain
// branch: when the cap is generous (or unlimited), doDeprovision must:
//   - Rename the canonical volumes into the retained namespace (NOT destroy them)
//   - Write an ACTIVE retention record in the store
//   - NOT increment retention_refused_total
//
// This is the inverse of TestDeprovision_BranchSelection_OverCap and together
// they pin the branch-selecting predicate inside doDeprovision end-to-end.
func TestDeprovision_BranchSelection_UnderCap(t *testing.T) {
	callbackDone := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	// Cap = 0 (unlimited) → shouldRefuseRetention returns false immediately.
	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}}
	canonical0 := canonicalVolumeName("lease-uc", "web", 0)
	canonical1 := canonicalVolumeName("lease-uc", "web", 1)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-uc": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-uc", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "nginx:1.25"}}},
		}},
	})

	withMicroSKU(b, 512)
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.cfg.MaxRetainedDiskMB = 0 // unlimited: never refuse
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-uc-web-0", "docker-micro", "tenant-a"))
	require.NoError(t, b.pool.TryAllocate("lease-uc-web-1", "docker-micro", "tenant-a"))

	// Track which volumes were renamed vs destroyed.
	var destroyed []string
	var renamed [][2]string
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return []string{canonical0, canonical1}, nil
		},
		DestroyFn: func(_ context.Context, id string) error {
			destroyed = append(destroyed, id)
			return nil
		},
		RenameVolumeFn: func(old, newName string) error {
			renamed = append(renamed, [2]string{old, newName})
			return nil
		},
	}

	before := testutil.ToFloat64(retentionRefusedTotal)
	require.NoError(t, b.Deprovision(context.Background(), "lease-uc"))

	// Wait for terminal callback (deferred accounting runs before it).
	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	// Volumes must be RENAMED into retained namespace (not destroyed).
	assert.Empty(t, destroyed, "under-cap: no volumes must be destroyed")
	assert.Len(t, renamed, 2, "under-cap: both canonical volumes must be renamed")
	for _, r := range renamed {
		assert.True(t, r[0] == canonical0 || r[0] == canonical1,
			"under-cap: renamed source must be a canonical volume")
		assert.Equal(t, retainedName(r[0]), r[1],
			"under-cap: rename target must be the retained-namespace name")
	}

	// Counter must NOT have incremented.
	assert.Equal(t, before, testutil.ToFloat64(retentionRefusedTotal),
		"under-cap: retention_refused_total must not increment")

	// An ACTIVE retention record must exist in the store.
	rec, err := rs.Get("lease-uc")
	require.NoError(t, err)
	require.NotNil(t, rec, "under-cap: an active retention record must be written")
	assert.Equal(t, shared.RetentionStatusActive, rec.Status,
		"under-cap: retention record must be active")
}

// TestDeprovision_RetainFailure_KeepsLiveCounted verifies that when the retain
// path fails (rename fails → volume-cleanup error → lease kept Failed), the
// live pool allocation is NOT released. Volumes are still on disk under canonical
// names; releasing live would open a window where F is counted in neither pool.
func TestDeprovision_RetainFailure_KeepsLiveCounted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}}
	canonical := canonicalVolumeName("lease-w", "web", 0)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-w": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-w", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "nginx:1.25"}}},
		}},
	})

	withMicroSKU(b, 512) // F = 1 × 512 = 512 MB
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-w-web-0", "docker-micro", "tenant-a"))
	require.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=512 MB")

	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return []string{canonical}, nil },
		RenameVolumeFn: func(_, _ string) error {
			return fmt.Errorf("rename failed: disk full")
		},
	}

	// Rename failure → volume-cleanup error (under limit) → returns error, lease kept Failed.
	err = b.Deprovision(context.Background(), "lease-w")
	require.Error(t, err, "rename failure must bubble as a volume-cleanup error")

	// The live allocation must NOT have been released — the volume is still on
	// disk under the canonical name (the retained-namespace rename failed), so
	// the bytes are physically present and must stay counted as live.
	s := b.pool.Stats()
	assert.Equal(t, int64(512), s.AllocatedDiskMB,
		"after failed retain close, live must remain counted (volume still on disk under canonical name)")
}
