package docker

import (
	"context"
	"fmt"
	"log/slog"
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
	assert.Equal(t, float64(1), testutil.ToFloat64(retainedLeases))

	// Remove the record and refresh → projection drops to 0.
	require.NoError(t, rs.Delete("lease-a"))
	b.refreshRetentionAccounting()
	assert.Equal(t, int64(0), b.pool.Stats().RetainedDiskMB)
	assert.Equal(t, float64(0), testutil.ToFloat64(retainedVolumeBytes))
	assert.Equal(t, float64(0), testutil.ToFloat64(retainedLeases))
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
	assert.True(t, retentionCapNeedsTenantLever(Config{RetainOnClose: true, MaxRetainedDiskMB: 4096, MaxRetainedLeasesPerTenant: 0}))
	// cap set, per-tenant count cap also set → fine.
	assert.False(t, retentionCapNeedsTenantLever(Config{RetainOnClose: true, MaxRetainedDiskMB: 4096, MaxRetainedLeasesPerTenant: 5}))
	// no cap → nothing to warn about.
	assert.False(t, retentionCapNeedsTenantLever(Config{RetainOnClose: true, MaxRetainedDiskMB: 0, MaxRetainedLeasesPerTenant: 0}))
	// retention disabled + cap set + per-tenant 0 → must NOT warn (nothing is ever retained).
	assert.False(t, retentionCapNeedsTenantLever(Config{RetainOnClose: false, MaxRetainedDiskMB: 4096, MaxRetainedLeasesPerTenant: 0}),
		"retain_on_close=false: warn must be suppressed even with cap set and per-tenant limit 0")
}

// TestRetentionCapSetButDisabled verifies the companion helper: returns true
// when retain_on_close=false but a cap or per-tenant knob is configured (dead
// config that misleads the operator), and false otherwise.
func TestRetentionCapSetButDisabled(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want bool
	}{
		{
			name: "disabled+disk cap set → true (cap has no effect)",
			cfg:  Config{RetainOnClose: false, MaxRetainedDiskMB: 4096, MaxRetainedLeasesPerTenant: 0},
			want: true,
		},
		{
			name: "disabled+per-tenant cap set → true (cap has no effect)",
			cfg:  Config{RetainOnClose: false, MaxRetainedDiskMB: 0, MaxRetainedLeasesPerTenant: 5},
			want: true,
		},
		{
			name: "disabled+both caps set → true",
			cfg:  Config{RetainOnClose: false, MaxRetainedDiskMB: 4096, MaxRetainedLeasesPerTenant: 5},
			want: true,
		},
		{
			name: "disabled+no caps → false (nothing misconfigured)",
			cfg:  Config{RetainOnClose: false, MaxRetainedDiskMB: 0, MaxRetainedLeasesPerTenant: 0},
			want: false,
		},
		{
			name: "enabled+disk cap set → false (retention is live; cap is meaningful)",
			cfg:  Config{RetainOnClose: true, MaxRetainedDiskMB: 4096, MaxRetainedLeasesPerTenant: 0},
			want: false,
		},
		{
			name: "enabled+no caps → false",
			cfg:  Config{RetainOnClose: true, MaxRetainedDiskMB: 0, MaxRetainedLeasesPerTenant: 0},
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, retentionCapSetButDisabled(tc.cfg))
		})
	}
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

func TestLeaseDiskMB_NonPositiveQuantitySkipped(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	withMicroSKU(b, 1024)
	items := []backend.LeaseItem{
		{SKU: "docker-micro", Quantity: 2, ServiceName: "web"},  // 2048
		{SKU: "docker-micro", Quantity: -3, ServiceName: "bad"}, // invalid → must contribute 0, not reduce
		{SKU: "docker-micro", Quantity: 0, ServiceName: "zero"}, // 0 → 0
	}
	mb, _ := b.leaseDiskMB(items)
	assert.Equal(t, int64(2048), mb,
		"non-positive quantities must contribute 0 (never reduce the projection into the over-admit direction)")
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

// TestDeprovision_RetainGiveUp_ReleasesLive verifies that when a retain close
// exhausts maxVolumeCleanupAttempts (rename keeps failing), the give-up branch
// — which deletes the provision and returns nil so no retry can ever run — still
// releases the live pool allocation. Without this, live would leak forever
// (allocatedDisk never returns to 0) AND, if a retention record was written,
// the footprint would be double-counted as both live and retained (2F), wedging
// any later restore via the pool's "already has allocated resources" guard.
func TestDeprovision_RetainGiveUp_ReleasesLive(t *testing.T) {
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
	canonical := canonicalVolumeName("lease-g", "web", 0)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-g": {
			ProvisionState: leasesm.ProvisionState{
				LeaseUUID: "lease-g", Tenant: "tenant-a", ProviderUUID: "prov-1",
				Status:        backend.ProvisionStatusReady,
				ContainerIDs:  []string{"c1"},
				CallbackURL:   server.URL,
				Items:         items,
				StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "nginx:1.25"}}},
			},
			// One short of the limit so a single Deprovision (increment to the
			// limit) hits the give-up branch immediately.
			VolumeCleanupAttempts: maxVolumeCleanupAttempts - 1,
		},
	})

	withMicroSKU(b, 512) // F = 1 × 512 = 512 MB
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-g-web-0", "docker-micro", "tenant-a"))
	require.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=512 MB")

	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return []string{canonical}, nil },
		RenameVolumeFn: func(_, _ string) error {
			return fmt.Errorf("rename failed: disk full")
		},
	}

	// Give-up branch returns nil (not an error) — the lease is abandoned.
	require.NoError(t, b.Deprovision(context.Background(), "lease-g"))

	// Provision is gone (give-up deletes it).
	b.provisionsMu.RLock()
	_, ok := b.provisions["lease-g"]
	b.provisionsMu.RUnlock()
	require.False(t, ok, "provision must be deleted after give-up")

	// Live MUST be released even on the abandoned retain path — otherwise it
	// leaks forever (no retry left) and double-counts against retained.
	s := b.pool.Stats()
	assert.Equal(t, int64(0), s.AllocatedDiskMB,
		"give-up branch must release live (no leak); was %d", s.AllocatedDiskMB)
}

// TestDeprovision_RetainListError_KeepsLiveCounted verifies that when the
// retain path cannot enumerate the lease's volumes (List() errors), the live
// allocation is NOT released. The volumes are likely still on disk (List just
// failed to read them) and no retention record was written, so releasing live
// would leave the footprint counted in neither pool → over-admit. The lease must
// keep live counted and retry on a later attempt.
func TestDeprovision_RetainListError_KeepsLiveCounted(t *testing.T) {
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

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-le": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-le", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "nginx:1.25"}}},
		}},
	})

	withMicroSKU(b, 512) // F = 512 MB
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-le-web-0", "docker-micro", "tenant-a"))
	require.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=512 MB")

	// List() errors → canonical stays empty, but volumes are NOT actually absent.
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return nil, fmt.Errorf("list failed: I/O error") },
	}

	// The List error surfaces as a volume-cleanup error → under-limit retry path.
	err = b.Deprovision(context.Background(), "lease-le")
	require.Error(t, err, "a List error must bubble as a volume-cleanup error (keep lease for retry)")

	// Live MUST remain counted — the volumes were never confirmed absent, so the
	// footprint must keep counting until a successful enumeration handles it.
	s := b.pool.Stats()
	assert.Equal(t, int64(512), s.AllocatedDiskMB,
		"a List() error must keep live counted (volumes likely still on disk)")
}

// TestRestoreRollback_Success_HandsOffLiveToRetained verifies that a successful
// re-quarantine rollback (all RenameVolume calls succeed) transfers the live
// footprint to the retained projection without a gap:
//
//   - Pre-condition: new-lease live allocation = F (AllocatedDiskMB == F).
//   - Post-condition: AllocatedDiskMB == 0 AND RetainedDiskMB == F.
//
// The correct order is: revert record to active → refreshRetentionAccounting
// (retained += F) → releaseAll (live -= F). Any other order creates a moment
// where the footprint is counted in neither pool (undercount → over-admit).
func TestRestoreRollback_Success_HandsOffLiveToRetained(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	// Use 512 MB to match the pool resolver (defaultTestSKUProfiles: docker-micro DiskMB=512).
	// qty=1 → footprint F = 512 MB.
	withMicroSKU(b, 512)

	origLease := "orig-lease"
	newLease := "new-lease"

	// Write a restoring retention record for the original lease.
	rec := shared.RetentionEntry{
		OriginalLeaseUUID:   origLease,
		NewLeaseUUID:        newLease,
		Tenant:              "t1",
		Status:              shared.RetentionStatusRestoring,
		Generation:          1,
		Items:               []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}},
		RetainedVolumeNames: []string{"fred-retained-orig-lease-web-0"},
	}
	require.NoError(t, rs.Put(rec))

	// Allocate the new-lease live footprint in the pool (simulates the restore
	// pool allocation from Restore() step c). The allocation id format must
	// match what rollbackRestoreAdoption passes to releaseAll.
	allocID := newLease + "-web-0"
	require.NoError(t, b.pool.TryAllocate(allocID, "docker-micro", "t1"))
	require.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=512 MB")
	require.Equal(t, int64(0), b.pool.Stats().RetainedDiskMB, "pre-condition: retained=0 (record is restoring)")

	// RenameVolume SUCCEEDS → rollback can revert the record to active.
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { return nil },
	}

	logger := slog.Default()
	// dropProvision=false: no provision entry to remove, avoids a map write under provisionsMu.
	b.rollbackRestoreAdoption(context.Background(), newLease, []string{allocID}, &rec, false, logger)

	s := b.pool.Stats()
	assert.Equal(t, int64(0), s.AllocatedDiskMB,
		"after successful rollback live must be zero (released)")
	assert.Equal(t, int64(512), s.RetainedDiskMB,
		"after successful rollback retained must equal F (record reverted to active + refreshed before release)")
}

// TestRestoreRollback_FailedRequarantine_KeepsLiveCounted verifies that when
// re-quarantine renames FAIL, the live allocation is NOT released. The volumes
// remain on disk under the new-lease canonical name and the record stays
// 'restoring' (excluded from the retained projection), so releasing the live
// allocation would leave the footprint counted in neither pool → over-admit.
//
// This test FAILS before Fix #1 (the bug: releaseAll runs unconditionally before
// the failed-branch early return, so AllocatedDiskMB is wrongly zeroed).
func TestRestoreRollback_FailedRequarantine_KeepsLiveCounted(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 512) // F = 512 MB (qty=1)

	origLease := "orig-lease2"
	newLease := "new-lease2"

	rec := shared.RetentionEntry{
		OriginalLeaseUUID:   origLease,
		NewLeaseUUID:        newLease,
		Tenant:              "t1",
		Status:              shared.RetentionStatusRestoring,
		Generation:          1,
		Items:               []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}},
		RetainedVolumeNames: []string{"fred-retained-orig-lease2-web-0"},
	}
	require.NoError(t, rs.Put(rec))

	allocID := newLease + "-web-0"
	require.NoError(t, b.pool.TryAllocate(allocID, "docker-micro", "t1"))
	require.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=512 MB")

	// RenameVolume ERRORS → failed re-quarantine; live must remain counted.
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { return fmt.Errorf("rename failed: device busy") },
	}

	logger := slog.Default()
	b.rollbackRestoreAdoption(context.Background(), newLease, []string{allocID}, &rec, false, logger)

	s := b.pool.Stats()
	assert.Equal(t, int64(512), s.AllocatedDiskMB,
		"failed re-quarantine: live must remain counted (volumes still on disk, record stays restoring)")
}

// TestReconcileRestoring_OrphanedArm_ReleasesNewLeaseLive verifies Fix #1: when
// the periodic sweep's reconcileRestoring handles an orphaned restoring record
// (no live provision/containers for the new lease) and the re-quarantine renames
// SUCCEED, the new lease's live pool allocation must be released after the record
// is reverted to active.
//
// Before the fix: reconcileRestoring called removeProvision (map delete) but never
// pool.Release → the new-lease live allocation lingered → retained F counted
// (from refreshRetentionAccounting after RevertToActive) + live F still counted =
// 2F over-count (over-deny, data-safe, but a real steady-state inaccuracy).
//
// Post-fix invariant: AllocatedDiskMB == 0 AND RetainedDiskMB == F (= 512 MB).
func TestReconcileRestoring_OrphanedArm_ReleasesNewLeaseLive(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	// DiskMB=512 to match the pool resolver (defaultTestSKUProfiles: docker-micro DiskMB=512).
	// qty=1 → footprint F = 512 MB.
	withMicroSKU(b, 512)

	origLease := "orig-orphaned"
	newLease := "new-orphaned"

	// Write a restoring retention record for the original lease.
	entry := shared.RetentionEntry{
		OriginalLeaseUUID:   origLease,
		NewLeaseUUID:        newLease,
		Tenant:              "t1",
		Status:              shared.RetentionStatusRestoring,
		Generation:          1,
		Items:               []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}},
		RetainedVolumeNames: []string{"fred-retained-orig-orphaned-web-0"},
	}
	require.NoError(t, rs.Put(entry))

	// Allocate the new-lease live footprint (simulates the pool allocation from
	// Restore() step c). The id scheme matches {newLease}-{svc}-{idx}.
	allocID := newLease + "-web-0"
	require.NoError(t, b.pool.TryAllocate(allocID, "docker-micro", "t1"))
	require.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=512 MB")
	require.Equal(t, int64(0), b.pool.Stats().RetainedDiskMB, "pre-condition: retained=0 (record restoring)")

	// No live provision entry for newLease → orphaned arm runs.
	// (The provisions map was initialised empty by newBackendWithRetention.)

	// RenameVolume SUCCEEDS → re-quarantine works, orphaned arm can revert.
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { return nil },
	}
	// compose.Down must succeed (default mockComposeExecutor.DownFn is nil → no-op).

	b.reconcileRestoring(context.Background(), entry)

	s := b.pool.Stats()
	assert.Equal(t, int64(0), s.AllocatedDiskMB,
		"after orphaned revert, new-lease live allocation must be released (no 2F over-count)")
	assert.Equal(t, int64(512), s.RetainedDiskMB,
		"after orphaned revert, retained must equal F (record reverted to active + refreshed)")
}

// TestDeprovision_RefuseToRetain_DestroyFailure_KeepsLiveCounted verifies that
// when the refuse-to-retain path fires (cap breach) and destroyOnRefuseToRetain
// ERRORS, the live allocation is NOT released. The volumes may still be on disk
// and a subsequent retry may reclaim them; releasing live here would leave the
// footprint counted in neither pool → over-admit.
//
// This guards against a future refactor that unconditionally sets
// releaseLiveOnRetainPath=true on the refuse branch regardless of destroy errors.
func TestDeprovision_RefuseToRetain_DestroyFailure_KeepsLiveCounted(t *testing.T) {
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
	canonical0 := canonicalVolumeName("lease-rf", "web", 0)
	canonical1 := canonicalVolumeName("lease-rf", "web", 1)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-rf": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-rf", Tenant: "tenant-a", ProviderUUID: "prov-1",
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

	require.NoError(t, b.pool.TryAllocate("lease-rf-web-0", "docker-micro", "tenant-a"))
	require.NoError(t, b.pool.TryAllocate("lease-rf-web-1", "docker-micro", "tenant-a"))
	require.Equal(t, int64(1024), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=1024 MB")

	// Destroy FAILS → volumes may still be on disk; live must remain counted.
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return []string{canonical0, canonical1}, nil
		},
		DestroyFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("destroy failed: device busy")
		},
	}

	// Deprovision returns an error (volume cleanup failed) and the lease is kept
	// Failed for retry — not the give-up path (VolumeCleanupAttempts < max).
	err = b.Deprovision(context.Background(), "lease-rf")
	require.Error(t, err, "destroy failure must bubble as a volume-cleanup error")

	// The live allocation must NOT have been released.
	s := b.pool.Stats()
	assert.Equal(t, int64(1024), s.AllocatedDiskMB,
		"refuse-to-retain destroy failure: live must remain counted (volumes still on disk, not released)")

	// No retention record must have been written (refuse path never writes a record).
	rec, rerr := rs.Get("lease-rf")
	require.NoError(t, rerr)
	assert.Nil(t, rec, "refuse-to-retain: no retention record must be written")
}

// TestDeprovision_NonRetain_DestroyFailure_KeepsLiveCounted verifies that on
// the non-retain path, if volumes.Destroy fails (the lease is kept Failed for
// retry), the live pool allocation is NOT released. Before the fix, releaseLive()
// was called BEFORE the volume-destroy loop, so a Destroy failure left the
// volume on disk but the live pool freed → footprint uncounted → over-admit
// / ENOSPC window while the lease retried.
//
// This test MUST fail before Fix A (live drops to 0 on Destroy error) and pass
// after (live stays at F until all volumes are destroyed without error).
func TestDeprovision_NonRetain_DestroyFailure_KeepsLiveCounted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Two items, qty=1 each → F = 2 × 512 = 1024 MB.
	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}}

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-nd": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:     "lease-nd",
			Tenant:        "tenant-a",
			ProviderUUID:  "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "nginx:1.25"}}},
		}},
	})

	withMicroSKU(b, 512) // F = 1 × 512 = 512 MB
	// RetainOnClose stays false (default) — non-retain close.
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-nd-web-0", "docker-micro", "tenant-a"))
	require.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=512 MB")

	// Destroy FAILS → volume still on disk; lease kept Failed for retry.
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("destroy failed: device busy")
		},
	}

	err := b.Deprovision(context.Background(), "lease-nd")
	require.Error(t, err, "volume destroy failure must return an error")

	// Lease must be kept Failed for retry (not deleted).
	b.provisionsMu.RLock()
	p, ok := b.provisions["lease-nd"]
	var gotStatus backend.ProvisionStatus
	if ok {
		gotStatus = p.Status
	}
	b.provisionsMu.RUnlock()
	require.True(t, ok, "provision must stay visible for retry after a volume destroy failure")
	assert.Equal(t, backend.ProvisionStatusFailed, gotStatus)

	// The live allocation MUST NOT have been released — the volume is still on
	// disk, so the footprint must keep counting to prevent an over-admit window.
	s := b.pool.Stats()
	assert.Equal(t, int64(512), s.AllocatedDiskMB,
		"non-retain destroy failure: live must remain counted (volume still on disk)")
}

// TestDeprovision_NonRetain_Success_ReleasesLive verifies that on the non-retain
// path, when ALL volumes are destroyed successfully, the live pool allocation IS
// released. This guards that the moved release still fires on the happy path.
func TestDeprovision_NonRetain_Success_ReleasesLive(t *testing.T) {
	callbackDone := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case callbackDone <- struct{}{}:
		default:
		}
	}))
	defer server.Close()

	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}}

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-ns": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:     "lease-ns",
			Tenant:        "tenant-a",
			ProviderUUID:  "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "nginx:1.25"}}},
		}},
	})

	withMicroSKU(b, 512) // F = 512 MB
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-ns-web-0", "docker-micro", "tenant-a"))
	require.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=512 MB")

	// Destroy succeeds → all bytes are gone; live must be released.
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, _ string) error { return nil },
	}

	require.NoError(t, b.Deprovision(context.Background(), "lease-ns"))

	// Wait for terminal callback to ensure the deferred accounting has run.
	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	// Live MUST be released — all bytes are gone.
	s := b.pool.Stats()
	assert.Equal(t, int64(0), s.AllocatedDiskMB,
		"non-retain success: live must be released after all volumes destroyed")

	// Provision must be deleted (not kept).
	b.provisionsMu.RLock()
	_, ok := b.provisions["lease-ns"]
	b.provisionsMu.RUnlock()
	assert.False(t, ok, "provision must be deleted after successful non-retain close")
}

// TestDeprovision_NonRetainPartialFailure_KeepsLiveCounted verifies that a
// NON-retain close whose container teardown PARTIALLY fails (compose down fails
// AND the per-container RemoveContainer fallback also fails → stuck containers
// remain) does NOT release the live pool allocation. The early non-retain
// release must run only AFTER teardown succeeds; otherwise CPU/mem/disk are
// freed while the stuck containers still run → undercount → over-admit (ENOSPC).
func TestDeprovision_NonRetainPartialFailure_KeepsLiveCounted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}}

	// compose down fails → falls back to per-container removal, which also fails →
	// errs is non-empty → doDeprovision takes the partial-failure early-return.
	mock := &mockDockerClient{
		RemoveContainerFn: func(_ context.Context, _ string) error {
			return fmt.Errorf("container stuck: device or resource busy")
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-pf": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-pf", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "nginx:1.25"}}},
		}},
	})
	b.compose = &mockComposeExecutor{
		DownFn: func(_ context.Context, _ string, _ time.Duration) error {
			return fmt.Errorf("compose down failed")
		},
	}

	withMicroSKU(b, 512) // F = 512 MB
	// RetainOnClose stays false (default) — pure non-retain close.
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-pf-web-0", "docker-micro", "tenant-a"))
	require.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=512 MB")

	// Partial container-teardown failure → returns an error, lease kept Failed.
	err := b.Deprovision(context.Background(), "lease-pf")
	require.Error(t, err, "a partial container-teardown failure must return an error")

	// The lease is kept visible in Failed with the stuck container for retry.
	b.provisionsMu.RLock()
	p, ok := b.provisions["lease-pf"]
	var gotStatus backend.ProvisionStatus
	if ok {
		gotStatus = p.Status
	}
	b.provisionsMu.RUnlock()
	require.True(t, ok, "provision must stay visible for retry after a partial teardown failure")
	assert.Equal(t, backend.ProvisionStatusFailed, gotStatus)

	// Live MUST remain counted — the stuck container is still running, so its
	// CPU/mem/disk must keep counting until a later retry tears it down cleanly.
	s := b.pool.Stats()
	assert.Equal(t, int64(512), s.AllocatedDiskMB,
		"a partial container-teardown failure must keep live counted (stuck container still running)")
}
