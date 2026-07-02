package docker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// TestReconcileVolumeQuotas_ReAppliesActiveAndRetained pins the enumeration logic
// of the startup quota backfill. Every filter is made load-bearing by putting a
// volume that MUST be excluded on disk, so the existence gate can't mask a broken
// filter. It must re-apply to exactly:
//   - active stateful instances (at disk_mb),
//   - active ephemeral instances WITH an on-disk (writable-path) volume (at the
//     tmpfs fallback size — mirrors provision.go),
//   - active-status retained volumes named in RetainedVolumeNames (at disk_mb),
//
// and must SKIP: instances absent from disk, unknown-SKU items, retained derived
// names not in RetainedVolumeNames, and non-active (restoring) retained entries.
func TestReconcileVolumeQuotas_ReAppliesActiveAndRetained(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	b.cfg.VolumeDataPath = "/data/fred/volumes" // a real quota backend has one
	tmpfsMB := int64(b.cfg.GetTmpfsSizeMB())    // ephemeral writable-path fallback (default 64)
	require.Positive(t, tmpfsMB)

	b.cfg.SKUProfiles = map[string]SKUProfile{
		"stateful":  {CPUCores: 1, MemoryMB: 512, DiskMB: 100},
		"stateful2": {CPUCores: 1, MemoryMB: 512, DiskMB: 250},
		"ephemeral": {CPUCores: 1, MemoryMB: 512, DiskMB: 0},
	}

	// Active lease: 2 stateful, 1 ephemeral (on disk → writable-path volume), and
	// 1 unknown-SKU item (on disk → must be skipped).
	b.provisions["laa"] = &provision{ProvisionState: leasesm.ProvisionState{
		LeaseUUID: "laa",
		Items: []backend.LeaseItem{
			{SKU: "stateful", Quantity: 2, ServiceName: "web"},
			{SKU: "ephemeral", Quantity: 1, ServiceName: "cache"},
			{SKU: "ghost", Quantity: 1, ServiceName: "unknown"}, // unknown SKU → skip
		},
	}}

	// Retained active: db-0 is retained; sidecar-0's derived name is NOT in
	// RetainedVolumeNames (stateless in that lease) → must skip even though it's
	// on disk.
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID: "lbb", Tenant: "t1", ProviderUUID: "p1",
		Items: []backend.LeaseItem{
			{SKU: "stateful2", Quantity: 1, ServiceName: "db"},
			{SKU: "stateful2", Quantity: 1, ServiceName: "sidecar"},
		},
		RetainedVolumeNames: []string{"fred-retained-lbb-db-0"},
		Status:              shared.RetentionStatusActive,
		CreatedAt:           time.Now(),
	}))
	// Restoring → skip entirely.
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID: "lcc", Tenant: "t1", ProviderUUID: "p1",
		Items:               []backend.LeaseItem{{SKU: "stateful2", Quantity: 1, ServiceName: "db"}},
		RetainedVolumeNames: []string{"fred-retained-lcc-db-0"},
		Status:              shared.RetentionStatusRestoring,
		CreatedAt:           time.Now(),
	}))

	// On disk. Every to-be-skipped volume is present so the SKIP is proven by the
	// filter, not by absence: unknown-0 (unknown SKU), lbb-sidecar-0 (not in
	// RetainedVolumeNames), lcc-db-0 (restoring). web-1 is deliberately ABSENT to
	// pin the existence gate.
	onDisk := []string{
		"fred-laa-web-0", "fred-laa-cache-0", "fred-laa-unknown-0",
		"fred-retained-lbb-db-0", "fred-retained-lbb-sidecar-0", "fred-retained-lcc-db-0",
	}

	var mu sync.Mutex
	got := map[string]int64{}
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return onDisk, nil },
		EnsureQuotaFn: func(_ context.Context, id string, sizeMB int64) error {
			mu.Lock()
			defer mu.Unlock()
			got[id] = sizeMB
			return nil
		},
	}

	b.reconcileVolumeQuotas(context.Background())

	want := map[string]int64{
		"fred-laa-web-0":         100,     // active stateful
		"fred-laa-cache-0":       tmpfsMB, // active ephemeral + on-disk writable volume
		"fred-retained-lbb-db-0": 250,     // retained active, in RetainedVolumeNames
	}
	assert.Equal(t, want, got,
		"backfill must re-apply to exactly these on-disk volumes at these sizes")
}

// TestReconcileVolumeQuotas_EnsureQuotaFailureIsBestEffort pins that a single
// volume's EnsureQuota failure does not abort the backfill (the others are still
// processed) and is counted as an outcome=failed on the metric — while a success
// increments outcome=applied.
func TestReconcileVolumeQuotas_EnsureQuotaFailureIsBestEffort(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	b.cfg.VolumeDataPath = "/data/fred/volumes"
	b.cfg.SKUProfiles = map[string]SKUProfile{"s": {CPUCores: 1, MemoryMB: 512, DiskMB: 100}}
	b.provisions["laa"] = &provision{ProvisionState: leasesm.ProvisionState{
		LeaseUUID: "laa",
		Items:     []backend.LeaseItem{{SKU: "s", Quantity: 2, ServiceName: "web"}},
	}}

	appliedBefore := testutil.ToFloat64(volumeQuotaBackfillTotal.WithLabelValues("applied"))
	failedBefore := testutil.ToFloat64(volumeQuotaBackfillTotal.WithLabelValues("failed"))

	var mu sync.Mutex
	seen := map[string]struct{}{}
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return []string{"fred-laa-web-0", "fred-laa-web-1"}, nil },
		EnsureQuotaFn: func(_ context.Context, id string, _ int64) error {
			mu.Lock()
			defer mu.Unlock()
			seen[id] = struct{}{}
			if id == "fred-laa-web-0" {
				return errors.New("simulated raced deprovision")
			}
			return nil
		},
	}

	b.reconcileVolumeQuotas(context.Background())

	// Both volumes were attempted despite the first failing (not aborted early).
	assert.Len(t, seen, 2, "a per-volume failure must not stop the backfill")
	assert.Equal(t, appliedBefore+1, testutil.ToFloat64(volumeQuotaBackfillTotal.WithLabelValues("applied")),
		"the successful volume increments outcome=applied")
	assert.Equal(t, failedBefore+1, testutil.ToFloat64(volumeQuotaBackfillTotal.WithLabelValues("failed")),
		"the failed volume increments outcome=failed")
}

// TestReconcileVolumeQuotas_NoopWhenNoVolumeDataPath verifies the backfill is a
// no-op (no List/EnsureQuota) on a backend without a volume data path.
func TestReconcileVolumeQuotas_NoopWhenNoVolumeDataPath(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	b.cfg.VolumeDataPath = ""
	listed := false
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { listed = true; return nil, nil },
		EnsureQuotaFn: func(_ context.Context, _ string, _ int64) error {
			t.Fatal("EnsureQuota must not be called when volume_data_path is empty")
			return nil
		},
	}
	b.reconcileVolumeQuotas(context.Background())
	assert.False(t, listed, "must not even enumerate volumes without a data path")
}
