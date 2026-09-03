package docker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

// quotaInventoryOverride keeps each concrete manager's EnsureQuota behavior
// while supplying the already-proved startup inventory to the reconciliation
// layer. It makes the manager boundary testable without a privileged quota
// filesystem.
type quotaInventoryOverride struct {
	volumeManager
	names []string
}

func (m quotaInventoryOverride) List() ([]string, error) {
	return append([]string(nil), m.names...), nil
}

func (m quotaInventoryOverride) ListForProof(context.Context) ([]string, error) {
	return append([]string(nil), m.names...), nil
}

func (m quotaInventoryOverride) AttestManagedVolume(context.Context, managedVolumeName) error {
	return nil
}

// TestReconcileVolumeQuotas_ReAppliesActiveAndRetained pins the enumeration logic
// of the startup quota backfill. Every filter is made load-bearing by putting a
// volume that MUST be excluded on disk, so the existence gate can't mask a broken
// filter. It must re-apply to exactly:
//   - active stateful instances (at disk_mb),
//   - active ephemeral instances WITH an on-disk (writable-path) volume (at the
//     tmpfs fallback size — mirrors provision.go),
//   - active-status retained volumes named in RetainedVolumeNames (at disk_mb),
//
// and must SKIP: instances absent from disk, retained derived names not in
// RetainedVolumeNames, and non-active (restoring) retained entries.
func TestReconcileVolumeQuotas_ReAppliesActiveAndRetained(t *testing.T) {
	const (
		liveLease      = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		retainedLease  = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
		restoringLease = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	)
	b, rs := newBackendWithRetention(t)
	b.cfg.VolumeDataPath = "/data/fred/volumes" // a real quota backend has one
	tmpfsMB := int64(b.cfg.GetTmpfsSizeMB())    // ephemeral writable-path fallback (default 64)
	require.Positive(t, tmpfsMB)

	b.cfg.SKUProfiles = map[string]SKUProfile{
		"stateful":  {CPUCores: 1, MemoryMB: 512, DiskMB: 100},
		"stateful2": {CPUCores: 1, MemoryMB: 512, DiskMB: 250},
		"ephemeral": {CPUCores: 1, MemoryMB: 512, DiskMB: 0},
	}

	// Active lease: 2 stateful and 1 ephemeral (on disk → writable-path
	// volume), all sized from the immutable authority captured at admission.
	liveItems := []backend.LeaseItem{
		{SKU: "stateful", Quantity: 2, ServiceName: "web"},
		{SKU: "ephemeral", Quantity: 1, ServiceName: "cache"},
	}
	b.provisions[liveLease] = &provision{
		ProvisionState: leasesm.ProvisionState{LeaseUUID: liveLease, Items: liveItems},
		ResourceProfiles: []shared.SKUResourceSnapshot{
			{SKU: "ephemeral", CPUCores: 1, MemoryMB: 512, ScratchDiskMB: tmpfsMB},
			{SKU: "stateful", CPUCores: 1, MemoryMB: 512, DiskMB: 100},
		},
	}
	// Retained active: db-0 is retained; sidecar-0's derived name is NOT in
	// RetainedVolumeNames (stateless in that lease) → must skip even though it's
	// on disk.
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID: retainedLease, Tenant: "t1", ProviderUUID: "p1",
		Items: []backend.LeaseItem{
			{SKU: "stateful2", Quantity: 1, ServiceName: "db"},
			{SKU: "stateful2", Quantity: 1, ServiceName: "sidecar"},
		},
		RetainedVolumeNames: []string{retainedName(canonicalVolumeName(retainedLease, "db", 0))},
		Status:              shared.RetentionStatusActive,
		CreatedAt:           time.Now(),
	}))
	// Restoring → skip entirely.
	restoringProfiles := []shared.SKUResourceSnapshot{{
		SKU: "stateful2", CPUCores: 1, MemoryMB: 512, DiskMB: 250,
	}}
	putRestoringRetention(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID: restoringLease, Tenant: "t1", ProviderUUID: "p1",
		Items:                       []backend.LeaseItem{{SKU: "stateful2", Quantity: 1, ServiceName: "db"}},
		ResourceProfiles:            restoringProfiles,
		DestinationResourceProfiles: restoringProfiles,
		RetainedVolumeNames:         []string{retainedName(canonicalVolumeName(restoringLease, "db", 0))},
		Status:                      shared.RetentionStatusRestoring,
		CreatedAt:                   time.Now(),
	})

	// On disk. Every to-be-skipped volume is present so the SKIP is proven by the
	// filter, not by absence: lbb-sidecar-0 (not in RetainedVolumeNames) and
	// lcc-db-0 (restoring). web-1 is deliberately ABSENT to pin the existence
	// gate.
	onDisk := []string{
		canonicalVolumeName(liveLease, "web", 0), canonicalVolumeName(liveLease, "cache", 0),
		retainedName(canonicalVolumeName(retainedLease, "db", 0)),
		retainedName(canonicalVolumeName(retainedLease, "sidecar", 0)),
		retainedName(canonicalVolumeName(restoringLease, "db", 0)),
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

	require.NoError(t, b.reconcileVolumeQuotas(context.Background()))

	want := map[string]int64{
		canonicalVolumeName(liveLease, "web", 0):                  100,     // active stateful
		canonicalVolumeName(liveLease, "cache", 0):                tmpfsMB, // active ephemeral + on-disk writable volume
		retainedName(canonicalVolumeName(retainedLease, "db", 0)): 250,     // retained active, in RetainedVolumeNames
	}
	assert.Equal(t, want, got,
		"backfill must re-apply to exactly these on-disk volumes at these sizes")
}

// TestReconcileVolumeQuotas_EnsureQuotaFailureIsAggregated pins that a single
// volume's EnsureQuota failure does not abort the backfill (the others are still
// processed), but the completed walk returns the error so startup cannot report
// readiness. The failed and successful attempts retain their existing metrics.
func TestReconcileVolumeQuotas_EnsureQuotaFailureIsAggregated(t *testing.T) {
	const leaseUUID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	volume0 := canonicalVolumeName(leaseUUID, "web", 0)
	volume1 := canonicalVolumeName(leaseUUID, "web", 1)
	volume2 := canonicalVolumeName(leaseUUID, "web", 2)
	b, _ := newBackendWithRetention(t)
	b.cfg.VolumeDataPath = "/data/fred/volumes"
	b.cfg.SKUProfiles = map[string]SKUProfile{"s": {CPUCores: 1, MemoryMB: 512, DiskMB: 100}}
	b.provisions[leaseUUID] = &provision{ProvisionState: leasesm.ProvisionState{
		LeaseUUID: leaseUUID,
		Items:     []backend.LeaseItem{{SKU: "s", Quantity: 3, ServiceName: "web"}},
	}}

	appliedBefore := testutil.ToFloat64(volumeQuotaBackfillTotal.WithLabelValues("applied"))
	failedBefore := testutil.ToFloat64(volumeQuotaBackfillTotal.WithLabelValues("failed"))

	var mu sync.Mutex
	seen := map[string]struct{}{}
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return []string{volume0, volume1, volume2}, nil
		},
		EnsureQuotaFn: func(_ context.Context, id string, _ int64) error {
			mu.Lock()
			defer mu.Unlock()
			seen[id] = struct{}{}
			switch id {
			case volume0:
				return errors.New("simulated quota command failure zero")
			case volume2:
				return errors.New("simulated quota command failure two")
			}
			return nil
		},
	}

	err := b.reconcileVolumeQuotas(context.Background())

	// All volumes were attempted despite the first failing (not aborted early).
	assert.Len(t, seen, 3, "a per-volume failure must not stop the backfill")
	require.ErrorContains(t, err, `managed volume "`+volume0+`"`)
	require.ErrorContains(t, err, "simulated quota command failure zero")
	require.ErrorContains(t, err, `managed volume "`+volume2+`"`)
	require.ErrorContains(t, err, "simulated quota command failure two")
	assert.Equal(t, appliedBefore+1, testutil.ToFloat64(volumeQuotaBackfillTotal.WithLabelValues("applied")),
		"the successful volume increments outcome=applied")
	assert.Equal(t, failedBefore+2, testutil.ToFloat64(volumeQuotaBackfillTotal.WithLabelValues("failed")),
		"each failed volume increments outcome=failed")
}

func TestReconcileVolumeQuotas_InvalidAuthorityFailsAfterValidLeaseIsEnforced(t *testing.T) {
	const (
		badLease   = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		validLease = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	)
	badVolume := canonicalVolumeName(badLease, "db", 0)
	validVolume := canonicalVolumeName(validLease, "web", 0)
	b, _ := newBackendWithRetention(t)
	b.cfg.VolumeDataPath = "/data/fred/volumes"
	b.cfg.SKUProfiles = map[string]SKUProfile{
		"valid": {CPUCores: 1, MemoryMB: 512, DiskMB: 100},
	}
	b.provisions[badLease] = &provision{ProvisionState: leasesm.ProvisionState{
		LeaseUUID: badLease,
		Items:     []backend.LeaseItem{{SKU: "removed", Quantity: 1, ServiceName: "db"}},
	}}
	b.provisions[validLease] = &provision{ProvisionState: leasesm.ProvisionState{
		LeaseUUID: validLease,
		Items:     []backend.LeaseItem{{SKU: "valid", Quantity: 1, ServiceName: "web"}},
	}}

	var enforced []string
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return []string{badVolume, validVolume}, nil
		},
		EnsureQuotaFn: func(_ context.Context, id string, _ int64) error {
			enforced = append(enforced, id)
			return nil
		},
	}

	err := b.reconcileVolumeQuotas(context.Background())
	require.ErrorContains(t, err, `resolve live lease "`+badLease+`" quota authority`)
	require.ErrorContains(t, err, `unknown SKU: removed`)
	assert.Equal(t, []string{validVolume}, enforced,
		"bad durable authority must not prevent independent valid volumes from being capped")
}

func TestReconcileVolumeQuotas_InventoryFailureIsFatal(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	b.cfg.VolumeDataPath = "/data/fred/volumes"
	inventoryErr := errors.New("simulated volume inventory I/O error")
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return nil, inventoryErr },
		EnsureQuotaFn: func(context.Context, string, int64) error {
			t.Fatal("quota must not be guessed without a trustworthy volume inventory")
			return nil
		},
	}

	err := b.reconcileVolumeQuotas(context.Background())
	require.ErrorIs(t, err, inventoryErr)
	require.ErrorContains(t, err, "attest managed volumes for quota reconciliation")
}

func TestReconcileVolumeQuotas_InventoryAttestationFailureIsFatal(t *testing.T) {
	b, _ := newBackendWithRetention(t)
	b.cfg.VolumeDataPath = "/data/fred/volumes"
	attestationErr := errors.New("simulated managed-volume substrate mismatch")
	volumeName := canonicalVolumeName("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "web", 0)
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return []string{volumeName}, nil },
		AttestManagedVolumeFn: func(context.Context, managedVolumeName) error {
			return attestationErr
		},
		EnsureQuotaFn: func(context.Context, string, int64) error {
			t.Fatal("quota mutation must not run after inventory attestation fails")
			return nil
		},
	}

	err := b.reconcileVolumeQuotas(context.Background())
	require.ErrorIs(t, err, attestationErr)
	require.ErrorContains(t, err, `attest managed volume "`+volumeName+`"`)
}

func TestReconcileVolumeQuotas_ConcreteManagerFailuresReachReadinessGate(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	volumeName := canonicalVolumeName(leaseUUID, "app", 0)

	for _, kind := range []string{"btrfs", "xfs", "zfs"} {
		t.Run(kind, func(t *testing.T) {
			b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
			root := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(root, volumeName), 0o700))
			t.Setenv("PATH", "") // concrete CLI-backed managers fail without invoking host tools

			var manager volumeManager
			switch kind {
			case "btrfs":
				manager = &btrfsVolumeManager{dataPath: root, logger: b.logger}
			case "xfs":
				manager = &xfsVolumeManager{
					dataPath: root, mountPoint: root, logger: b.logger,
					activeIDs: make(map[uint32]string), volumeToID: make(map[string]uint32),
				}
			case "zfs":
				manager = &zfsVolumeManager{
					dataPath: root, parentDataset: "tank/fred", logger: b.logger,
				}
			default:
				t.Fatalf("unexpected manager kind %q", kind)
			}

			b.cfg.VolumeDataPath = root
			b.cfg.SKUProfiles = map[string]SKUProfile{
				"stateful": {CPUCores: 1, MemoryMB: 512, DiskMB: 100},
			}
			b.provisions[leaseUUID] = &provision{ProvisionState: leasesm.ProvisionState{
				LeaseUUID: leaseUUID,
				Items:     []backend.LeaseItem{{SKU: "stateful", Quantity: 1, ServiceName: "app"}},
			}}
			b.volumes = quotaInventoryOverride{volumeManager: manager, names: []string{volumeName}}

			err := b.reconcileVolumeQuotas(context.Background())
			require.Error(t, err)
			require.ErrorContains(t, err, `managed volume "`+volumeName+`"`)
			require.ErrorContains(t, err, kind)
		})
	}
}

func TestStart_QuotaReconciliationFailureFailsReadiness(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	mock, ok := b.docker.(*mockDockerClient)
	require.True(t, ok)
	mock.PingFn = func(context.Context) error { return nil }
	bindTestStorageIdentity(t, b, mock)
	t.Cleanup(b.stopCancel)

	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	volumeName := retainedName(canonicalVolumeName(leaseUUID, "app", 0))
	items := []backend.LeaseItem{{SKU: "stateful", Quantity: 1, ServiceName: "app"}}
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   leaseUUID,
		Tenant:              "tenant-a",
		ProviderUUID:        nominalDockerProviderUUID,
		Items:               items,
		ResourceProfiles:    []shared.SKUResourceSnapshot{{SKU: "stateful", CPUCores: 1, MemoryMB: 512, DiskMB: 100}},
		RetainedVolumeNames: []string{volumeName},
		Status:              shared.RetentionStatusActive,
		CreatedAt:           time.Now(),
	}))

	b.cfg.VolumeDataPath = "/data/fred/volumes"
	quotaErr := errors.New("simulated substrate quota refusal")
	var quotaCalls int
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) { return []string{volumeName}, nil },
		EnsureQuotaFn: func(_ context.Context, got string, sizeMB int64) error {
			quotaCalls++
			assert.Equal(t, volumeName, got)
			assert.Equal(t, int64(100), sizeMB)
			return quotaErr
		},
	}

	err := b.Start(context.Background())
	require.ErrorIs(t, err, quotaErr)
	require.ErrorContains(t, err, "reconcile startup volume quotas")
	assert.Equal(t, 1, quotaCalls)
}

func TestReconcileVolumeQuotas_UsesPinnedProfilesAfterConfigDrift(t *testing.T) {
	const (
		liveLease     = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
		retainedLease = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	)
	liveAppVolume := canonicalVolumeName(liveLease, "app", 0)
	liveCacheVolume := canonicalVolumeName(liveLease, "cache", 0)
	retainedDBVolume := retainedName(canonicalVolumeName(retainedLease, "db", 0))
	retainedSidecarVolume := retainedName(canonicalVolumeName(retainedLease, "sidecar", 0))
	tests := []struct {
		name       string
		configured map[string]SKUProfile
	}{
		{
			name: "profiles resized",
			configured: map[string]SKUProfile{
				"live":     {CPUCores: 8, MemoryMB: 8192, DiskMB: 900},
				"retained": {CPUCores: 8, MemoryMB: 8192, DiskMB: 800},
			},
		},
		{name: "profiles removed", configured: map[string]SKUProfile{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b, rs := newBackendWithRetention(t)
			b.cfg.VolumeDataPath = "/data/fred/volumes"
			b.cfg.ContainerTmpfsSizeMB = 999
			b.cfg.SKUProfiles = tc.configured
			liveItems := []backend.LeaseItem{
				{SKU: "live", Quantity: 1, ServiceName: "app"},
				{SKU: "live-scratch", Quantity: 1, ServiceName: "cache"},
			}
			liveProfiles := []shared.SKUResourceSnapshot{
				{SKU: "live", CPUCores: 1, MemoryMB: 512, DiskMB: 100},
				{SKU: "live-scratch", CPUCores: 0.25, MemoryMB: 128, ScratchDiskMB: 64},
			}
			b.provisions[liveLease] = &provision{
				ProvisionState:   leasesm.ProvisionState{LeaseUUID: liveLease, Items: liveItems},
				ResourceProfiles: liveProfiles,
			}

			retainedItems := []backend.LeaseItem{
				{SKU: "retained", Quantity: 1, ServiceName: "db"},
				{SKU: "retained-scratch", Quantity: 1, ServiceName: "sidecar"},
			}
			retainedProfiles := []shared.SKUResourceSnapshot{
				{SKU: "retained", CPUCores: 2, MemoryMB: 1024, DiskMB: 250},
				{SKU: "retained-scratch", CPUCores: 0.25, MemoryMB: 128, ScratchDiskMB: 72},
			}
			require.NoError(t, rs.Put(shared.RetentionEntry{
				OriginalLeaseUUID: retainedLease,
				Tenant:            "tenant-a",
				ProviderUUID:      "provider-a",
				Items:             retainedItems,
				ResourceProfiles:  retainedProfiles,
				RetainedVolumeNames: []string{
					retainedDBVolume,
					retainedSidecarVolume,
				},
				Status:    shared.RetentionStatusActive,
				CreatedAt: time.Now(),
			}))

			got := map[string]int64{}
			b.volumes = &mockVolumeManager{
				ListFn: func() ([]string, error) {
					return []string{
						liveAppVolume,
						liveCacheVolume,
						retainedDBVolume,
						retainedSidecarVolume,
					}, nil
				},
				EnsureQuotaFn: func(_ context.Context, id string, sizeMB int64) error {
					got[id] = sizeMB
					return nil
				},
			}

			require.NoError(t, b.reconcileVolumeQuotas(context.Background()))

			assert.Equal(t, map[string]int64{
				liveAppVolume:         100,
				liveCacheVolume:       64,
				retainedDBVolume:      250,
				retainedSidecarVolume: 72,
			}, got)
		})
	}
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
	require.NoError(t, b.reconcileVolumeQuotas(context.Background()))
	assert.False(t, listed, "must not even enumerate volumes without a data path")
}
