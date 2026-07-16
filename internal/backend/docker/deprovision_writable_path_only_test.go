package docker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
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

// retainCloseServer returns an httptest server that signals on the returned
// channel each time fred POSTs the terminal deprovision callback. Tests wait on
// the channel so the deferred retain-accounting hand-off has run before they
// assert pool/store state.
func retainCloseServer(t *testing.T) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	done := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case done <- struct{}{}:
		default:
		}
	}))
	t.Cleanup(server.Close)
	return server, done
}

// stageVolumeDirs creates on-host volume directories under root for each
// (name -> relative subpaths) entry, mirroring the on-disk layout the deprovision
// writable-path-only detector inspects. A canonical fred-{lease}-{svc}-{idx}
// volume's stateful VOLUME data lives in subdirs DIRECTLY under the volume root,
// while writable-path scaffolding lives under a single _wp/ subtree.
func stageVolumeDirs(t *testing.T, root string, layout map[string][]string) {
	t.Helper()
	for name, subpaths := range layout {
		for _, sub := range subpaths {
			require.NoError(t, os.MkdirAll(filepath.Join(root, name, sub), 0o755))
		}
	}
}

// TestDeprovision_WritablePathOnly_DestroyedNotRetained pins the ENG-406 contract:
// a lease whose only on-disk volume is writable-path-only (content lives solely
// under _wp/, no declared-VOLUME data) is EPHEMERAL by the ENG-367 wipe-contract
// (its _wp content is reseeded from the image on every restore), so retaining it
// preserves nothing restorable. Closing such a lease must:
//   - DESTROY the volume (reclaim), not rename it into the retained namespace
//   - write NO retention record (no per-tenant slot, no fred-retained-* dir)
//   - contribute 0 to the retained-disk budget even though disk_mb > 0
//
// This is the idiomatic per-class reclaim policy: ephemeral writable-path
// scaffolding (≈ Kubernetes emptyDir) is reclaimed on teardown; declared-VOLUME
// data (≈ a PersistentVolume with Retain) is soft-deleted + grace-restored.
func TestDeprovision_WritablePathOnly_DestroyedNotRetained(t *testing.T) {
	server, callbackDone := retainCloseServer(t)

	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	// docker-micro at 512 MB (disk_mb > 0) — proves the budget contribution is 0
	// because the volume is destroyed, NOT because the SKU is sized at 0.
	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}}
	canonical0 := canonicalVolumeName("lease-wp", "web", 0)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-wp": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-wp", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "grafana/grafana:11.1.0"}}},
		}},
	})

	withMicroSKU(b, 512)
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	// Pre-allocate the live footprint (simulates a running provision).
	require.NoError(t, b.pool.TryAllocate("lease-wp-web-0", "docker-micro", "tenant-a"))
	require.Equal(t, int64(512), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=512 MB")

	reclaimedBefore := testutil.ToFloat64(retentionWritablePathReclaimedTotal)

	// Stage the on-disk layout: the canonical volume contains ONLY a _wp/ subtree
	// (no declared-VOLUME subdir) → writable-path-only.
	volRoot := t.TempDir()
	stageVolumeDirs(t, volRoot, map[string][]string{
		canonical0: {filepath.Join(writablePathSubdir, "var", "lib", "grafana")},
	})

	var destroyed []string
	var renamed [][2]string
	b.volumes = &mockVolumeManager{
		defaultDir: volRoot,
		ListFn:     func() ([]string, error) { return []string{canonical0}, nil },
		DestroyFn: func(_ context.Context, id string) error {
			destroyed = append(destroyed, id)
			return nil
		},
		RenameVolumeFn: func(old, newName string) error {
			renamed = append(renamed, [2]string{old, newName})
			return nil
		},
	}

	require.NoError(t, b.Deprovision(context.Background(), "lease-wp"))

	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	// The writable-path-only volume must be DESTROYED, not renamed.
	assert.Equal(t, []string{canonical0}, destroyed,
		"writable-path-only volume must be destroyed (reclaimed) on close")
	assert.Empty(t, renamed,
		"writable-path-only volume must NOT be renamed into the retained namespace")

	// No retention record must exist — no per-tenant slot, nothing to restore.
	rec, err := rs.Get("lease-wp")
	require.NoError(t, err)
	assert.Nil(t, rec, "writable-path-only close must write no retention record")

	// Budget: live released, nothing retained — 0 contribution despite disk_mb=512.
	s := b.pool.Stats()
	assert.Equal(t, int64(0), s.AllocatedDiskMB, "live allocation must be released")
	assert.Equal(t, int64(0), s.RetainedDiskMB,
		"writable-path-only lease must contribute 0 to the retained-disk budget")

	// Observability: the reclaim must be counted for the ENG-405 dashboards.
	assert.Equal(t, reclaimedBefore+1, testutil.ToFloat64(retentionWritablePathReclaimedTotal),
		"reclaiming a writable-path-only volume must increment retention_writable_path_reclaimed_total")
}

// TestDeprovision_StatefulVolume_RetainedNotDestroyed is the safe-direction
// control for ENG-406: the reclaim gate's action is DESTROY, so misclassifying a
// stateful volume as writable-path-only would destroy tenant data. A volume that
// holds declared-VOLUME data — even when it ALSO contains a _wp/ subtree (an
// image that declares a VOLUME *and* trips writable-path detection shares one
// managed volume) — must be RETAINED (renamed), with a record written and the
// volume NOT destroyed. This pins the catastrophic-direction guard.
func TestDeprovision_StatefulVolume_RetainedNotDestroyed(t *testing.T) {
	server, callbackDone := retainCloseServer(t)

	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}}
	canonical0 := canonicalVolumeName("lease-sf", "web", 0)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-sf": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-sf", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "redis:7"}}},
		}},
	})

	withMicroSKU(b, 512)
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-sf-web-0", "docker-micro", "tenant-a"))

	// Mixed-content volume: a declared-VOLUME subdir (data/) AND a _wp/ subtree.
	// The presence of stateful data must force RETAIN despite the _wp subtree.
	volRoot := t.TempDir()
	stageVolumeDirs(t, volRoot, map[string][]string{
		canonical0: {"data", filepath.Join(writablePathSubdir, "etc")},
	})

	var destroyed []string
	var renamed [][2]string
	b.volumes = &mockVolumeManager{
		defaultDir: volRoot,
		ListFn:     func() ([]string, error) { return []string{canonical0}, nil },
		DestroyFn: func(_ context.Context, id string) error {
			destroyed = append(destroyed, id)
			return nil
		},
		RenameVolumeFn: func(old, newName string) error {
			renamed = append(renamed, [2]string{old, newName})
			return nil
		},
	}

	require.NoError(t, b.Deprovision(context.Background(), "lease-sf"))

	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	assert.Empty(t, destroyed, "stateful volume must NEVER be destroyed (would lose tenant data)")
	assert.Equal(t, [][2]string{{canonical0, retainedName(canonical0)}}, renamed,
		"stateful volume must be retained (renamed into the retained namespace)")

	rec, err := rs.Get("lease-sf")
	require.NoError(t, err)
	require.NotNil(t, rec, "stateful close must write a retention record")
	assert.Equal(t, []string{retainedName(canonical0)}, rec.RetainedVolumeNames)
	assert.Equal(t, int64(512), b.pool.Stats().RetainedDiskMB, "stateful footprint must be retained")
}

// TestDeprovision_MixedLease_RetainsStatefulReclaimsWritablePathOnly pins the
// per-volume (not per-lease) classification AND the B1 restore-safety invariant: a
// lease with one stateful service and one writable-path-only service retains the
// stateful volume and reclaims (destroys) the writable-path-only one — narrowing
// only RetainedVolumeNames — while the record's Items and StackManifest stay the
// FULL chain set so a later restore's itemsShapeMatch still passes (filtering them
// would permanently break restore → reaper destroys the retained stateful volume →
// data loss). The retained-disk budget conservatively over-counts the reclaimed
// service (safe deny-only direction).
func TestDeprovision_MixedLease_RetainsStatefulReclaimsWritablePathOnly(t *testing.T) {
	server, callbackDone := retainCloseServer(t)

	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	items := []backend.LeaseItem{
		{SKU: "docker-micro", Quantity: 1, ServiceName: "db"},   // stateful (declared VOLUME)
		{SKU: "docker-micro", Quantity: 1, ServiceName: "dash"}, // writable-path-only (grafana-style)
	}
	dbVol := canonicalVolumeName("lease-mx", "db", 0)
	dashVol := canonicalVolumeName("lease-mx", "dash", 0)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-mx": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-mx", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1"},
			CallbackURL:  server.URL,
			Items:        items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
				"db":   {Image: "redis:7"},
				"dash": {Image: "grafana/grafana:11.1.0"},
			}},
		}},
	})

	withMicroSKU(b, 512)
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-mx-db-0", "docker-micro", "tenant-a"))
	require.NoError(t, b.pool.TryAllocate("lease-mx-dash-0", "docker-micro", "tenant-a"))
	require.Equal(t, int64(1024), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=1024 MB")

	volRoot := t.TempDir()
	stageVolumeDirs(t, volRoot, map[string][]string{
		dbVol:   {"data"},                                                     // stateful
		dashVol: {filepath.Join(writablePathSubdir, "var", "lib", "grafana")}, // writable-path-only
	})

	var destroyed []string
	var renamed [][2]string
	b.volumes = &mockVolumeManager{
		defaultDir: volRoot,
		ListFn:     func() ([]string, error) { return []string{dbVol, dashVol}, nil },
		DestroyFn: func(_ context.Context, id string) error {
			destroyed = append(destroyed, id)
			return nil
		},
		RenameVolumeFn: func(old, newName string) error {
			renamed = append(renamed, [2]string{old, newName})
			return nil
		},
	}

	require.NoError(t, b.Deprovision(context.Background(), "lease-mx"))

	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	// dash (writable-path-only) destroyed; db (stateful) retained.
	assert.Equal(t, []string{dashVol}, destroyed, "only the writable-path-only volume must be destroyed")
	assert.Equal(t, [][2]string{{dbVol, retainedName(dbVol)}}, renamed, "only the stateful volume must be retained")

	rec, err := rs.Get("lease-mx")
	require.NoError(t, err)
	require.NotNil(t, rec, "mixed lease must write a record for its stateful service")

	// Only the durable volume name is narrowed.
	assert.Equal(t, []string{retainedName(dbVol)}, rec.RetainedVolumeNames,
		"RetainedVolumeNames must list only the durable (stateful) volume")

	// CRITICAL B1 REGRESSION GUARD (ENG-406 adversarial review): the record's Items
	// and StackManifest MUST stay the FULL chain set, including the reclaimed
	// writable-path-only service. Restore validates the record against the chain's
	// full item set (itemsShapeMatch); narrowing Items here makes EVERY restore fail
	// (shape mismatch), stranding the retained `db` volume until the reaper destroys
	// it — unrecoverable tenant data loss. On restore, `dash` simply gets a fresh
	// reseeded volume.
	assert.Equal(t, items, rec.Items,
		"record Items MUST remain the FULL chain set so restore's shape check still matches (else mixed-lease data loss)")
	assert.Len(t, rec.StackManifest.Services, 2,
		"record StackManifest must stay full (both services), consistent with full Items")

	s := b.pool.Stats()
	assert.Equal(t, int64(0), s.AllocatedDiskMB, "live released")
	// The retained-disk budget conservatively charges the FULL declared footprint
	// (db + dash = 1024), even though dash's volume was reclaimed. Over-counting a
	// reclaimed service's declared DiskMB is the SAFE (deny-only) direction; the
	// alternative (drop it) would under-count → over-admit/ENOSPC. ENG-401 (measured
	// SizeMB on the record) would let this reflect actual retained bytes.
	assert.Equal(t, int64(1024), s.RetainedDiskMB,
		"retained budget conservatively counts the full declared footprint (safe over-count)")
}

// TestDeprovision_WritablePathOnly_WithXFSMarker_StillReclaimed pins the xfs
// housekeeping whitelist: xfs writes a .fred-project-id quota marker inside every
// volume root, so a writable-path-only volume on xfs contains {_wp/,
// .fred-project-id}. The detector must still classify it as writable-path-only
// (the marker is fred housekeeping, not tenant data) and reclaim it.
func TestDeprovision_WritablePathOnly_WithXFSMarker_StillReclaimed(t *testing.T) {
	server, callbackDone := retainCloseServer(t)

	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}}
	canonical0 := canonicalVolumeName("lease-xfs", "web", 0)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-xfs": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-xfs", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "grafana/grafana:11.1.0"}}},
		}},
	})

	withMicroSKU(b, 512)
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-xfs-web-0", "docker-micro", "tenant-a"))

	volRoot := t.TempDir()
	stageVolumeDirs(t, volRoot, map[string][]string{
		canonical0: {filepath.Join(writablePathSubdir, "var")},
	})
	// Simulate the xfs in-volume quota marker beside _wp/.
	require.NoError(t, os.WriteFile(filepath.Join(volRoot, canonical0, projectIDFile), []byte("p"), 0o600))

	var destroyed []string
	var renamed [][2]string
	b.volumes = &mockVolumeManager{
		defaultDir: volRoot,
		ListFn:     func() ([]string, error) { return []string{canonical0}, nil },
		DestroyFn: func(_ context.Context, id string) error {
			destroyed = append(destroyed, id)
			return nil
		},
		RenameVolumeFn: func(old, newName string) error {
			renamed = append(renamed, [2]string{old, newName})
			return nil
		},
	}

	require.NoError(t, b.Deprovision(context.Background(), "lease-xfs"))

	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	assert.Equal(t, []string{canonical0}, destroyed,
		"writable-path-only volume must be reclaimed even with the xfs .fred-project-id marker present")
	assert.Empty(t, renamed)
	rec, err := rs.Get("lease-xfs")
	require.NoError(t, err)
	assert.Nil(t, rec)
}

// TestDeprovision_AmbiguousVolume_RetainedConservatively pins the conservative
// default: when the volume's content cannot be proven writable-path-only — here
// the host directory does not exist, so ReadDir errors — the volume is RETAINED,
// never destroyed. (An empty volume, or any unexpected top-level entry, takes the
// same safe path.)
func TestDeprovision_AmbiguousVolume_RetainedConservatively(t *testing.T) {
	server, callbackDone := retainCloseServer(t)

	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}}
	canonical0 := canonicalVolumeName("lease-amb", "web", 0)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-amb": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-amb", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "redis:7"}}},
		}},
	})

	withMicroSKU(b, 512)
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-amb-web-0", "docker-micro", "tenant-a"))

	// defaultDir points at an empty temp dir; the volume's own directory is never
	// created, so HostPath(name) resolves to a non-existent path → ReadDir errors.
	var destroyed []string
	var renamed [][2]string
	b.volumes = &mockVolumeManager{
		defaultDir: t.TempDir(),
		ListFn:     func() ([]string, error) { return []string{canonical0}, nil },
		DestroyFn: func(_ context.Context, id string) error {
			destroyed = append(destroyed, id)
			return nil
		},
		RenameVolumeFn: func(old, newName string) error {
			renamed = append(renamed, [2]string{old, newName})
			return nil
		},
	}

	require.NoError(t, b.Deprovision(context.Background(), "lease-amb"))

	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	assert.Empty(t, destroyed, "ambiguous (uninspectable) volume must NOT be destroyed")
	assert.Equal(t, [][2]string{{canonical0, retainedName(canonical0)}}, renamed,
		"ambiguous volume must be retained conservatively")
	rec, err := rs.Get("lease-amb")
	require.NoError(t, err)
	assert.NotNil(t, rec, "ambiguous volume must be retained with a record")
}

// TestBuildStatefulVolumeBinds_RejectsReservedWritablePathName guards a data-loss
// name collision the ENG-406 review surfaced: sanitizeVolumePath can legally
// produce "_wp" (an image declaring VOLUME /_wp, or /_wp/...). Without a guard,
// buildStatefulVolumeBinds would create that service's STATEFUL data under the
// volume root's _wp directory, which isWritablePathOnly then misclassifies as
// ephemeral scaffolding and DESTROYS at close. Since _wp is reserved for
// writable-path scaffolding, a colliding declared VOLUME must be rejected at
// provision (fail closed) rather than silently lost at close.
func TestBuildStatefulVolumeBinds_RejectsReservedWritablePathName(t *testing.T) {
	host := t.TempDir()

	_, err := buildStatefulVolumeBinds(host, []string{"/" + writablePathSubdir}, 0, 0)
	require.Error(t, err, "a VOLUME that sanitizes to the reserved _wp name must be rejected")

	_, err = buildStatefulVolumeBinds(host, []string{"/" + writablePathSubdir + "/data"}, 0, 0)
	require.Error(t, err, "a VOLUME nested under the reserved _wp dir must also be rejected")

	// A normal declared VOLUME is unaffected.
	binds, err := buildStatefulVolumeBinds(host, []string{"/data"}, 0, 0)
	require.NoError(t, err)
	require.Len(t, binds, 1)
}

// TestDeprovision_MarkerNamedStatefulDirPlusWp_Retained guards a data-loss edge
// (ENG-406 review): a declared VOLUME that sanitizes to the xfs marker name
// (.fred-project-id) creates a STATEFUL directory of that name under the volume
// root. The detector must whitelist the marker only as a regular FILE (the real
// xfs quota marker) — a directory of that name is tenant data, so a volume holding
// such a dir alongside a _wp scaffolding subtree must be RETAINED, not destroyed.
// (On btrfs/zfs there is no real marker; on xfs such a VOLUME collides with the
// marker file and fails at provision.)
func TestDeprovision_MarkerNamedStatefulDirPlusWp_Retained(t *testing.T) {
	server, callbackDone := retainCloseServer(t)

	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}}
	canonical0 := canonicalVolumeName("lease-mk", "web", 0)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-mk": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-mk", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "redis:7"}}},
		}},
	})

	withMicroSKU(b, 512)
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-mk-web-0", "docker-micro", "tenant-a"))

	// A stateful directory named like the xfs marker, alongside a _wp subtree.
	volRoot := t.TempDir()
	stageVolumeDirs(t, volRoot, map[string][]string{
		canonical0: {projectIDFile, filepath.Join(writablePathSubdir, "var")},
	})

	var destroyed []string
	var renamed [][2]string
	b.volumes = &mockVolumeManager{
		defaultDir: volRoot,
		ListFn:     func() ([]string, error) { return []string{canonical0}, nil },
		DestroyFn: func(_ context.Context, id string) error {
			destroyed = append(destroyed, id)
			return nil
		},
		RenameVolumeFn: func(old, newName string) error {
			renamed = append(renamed, [2]string{old, newName})
			return nil
		},
	}

	require.NoError(t, b.Deprovision(context.Background(), "lease-mk"))

	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	assert.Empty(t, destroyed, "a marker-NAMED stateful directory is tenant data → must not be destroyed")
	assert.Equal(t, [][2]string{{canonical0, retainedName(canonical0)}}, renamed,
		"volume with a marker-named stateful dir (plus _wp) must be retained")
	rec, err := rs.Get("lease-mk")
	require.NoError(t, err)
	assert.NotNil(t, rec)
}

// TestSetupWritablePathBinds_WipesStaleContentAndReseeds pins the ENG-367
// writable-path wipe mechanism at the unit level: setupWritablePathBinds
// RemoveAll's any prior _wp content (a stale extraction or tenant writes) and
// re-extracts fresh from the image on every call. This is the durability contract
// that makes ENG-406's reclaim-at-close behavior-preserving — writable-path data
// is image-derived and never survives a redeploy/restore. It re-pins, at the unit
// level, the contract the repurposed grafana integration test used to assert.
func TestSetupWritablePathBinds_WipesStaleContentAndReseeds(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{})

	hostVol := t.TempDir()
	wpDir := filepath.Join(hostVol, writablePathSubdir)

	// Stale content from a prior extraction / tenant write under the writable path.
	staleFile := filepath.Join(wpDir, "var", "lib", "grafana", "tenant-stale.txt")
	require.NoError(t, os.MkdirAll(filepath.Dir(staleFile), 0o755))
	require.NoError(t, os.WriteFile(staleFile, []byte("STALE"), 0o644))

	// Extraction reseeds fresh image content into the (RemoveAll'd) wpDir.
	freshFile := filepath.Join(wpDir, "var", "lib", "grafana", "fresh-from-image.txt")
	mock.ExtractImageContentFn = func(_ context.Context, _ string, _ []string, destDir string, _ int64) map[string]error {
		require.Equal(t, wpDir, destDir, "extraction target must be the volume's _wp subdir")
		require.NoError(t, os.MkdirAll(filepath.Dir(freshFile), 0o755))
		require.NoError(t, os.WriteFile(freshFile, []byte("FRESH"), 0o644))
		return nil
	}

	binds := b.setupWritablePathBinds(context.Background(), "grafana/grafana:11.1.0",
		[]string{"/var/lib/grafana"}, hostVol, 64<<20)

	// Stale tenant content is wiped; fresh image content is reseeded.
	_, statErr := os.Stat(staleFile)
	assert.True(t, os.IsNotExist(statErr), "stale writable-path content must be removed before reseed")
	got, readErr := os.ReadFile(freshFile)
	require.NoError(t, readErr)
	assert.Equal(t, "FRESH", string(got), "writable path must be reseeded fresh from the image")

	// The bind maps the host _wp subdir to the in-container writable path.
	assert.Equal(t, map[string]string{
		filepath.Join(wpDir, "var", "lib", "grafana"): "/var/lib/grafana",
	}, binds)
}

// TestSetupWritablePathBinds_RejectsSymlinkBindSource pins the ENG-543 defense.
// The writable-path bind Source is mounted read-write into the container and Docker
// resolves it host-side. Docker's CopyFromContainer does NOT follow a
// final-component symlink, so if extraction ever produced a symlink at the Source
// path (a malicious image whose writable path is a symlink), the mount would be
// redirected outside the volume — host escape. DetectWritablePaths only yields real
// directories today, but setupWritablePathBinds must be self-defending: it must not
// emit a bind whose Source is a symlink.
func TestSetupWritablePathBinds_RejectsSymlinkBindSource(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{})

	hostVol := t.TempDir()
	wpDir := filepath.Join(hostVol, writablePathSubdir)
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "host-secret"), []byte("x"), 0o600))

	// Simulate extraction planting a SYMLINK at the bind Source position
	// (wpDir/var/lib/grafana -> outside), as would happen if a symlink writable
	// path ever reached extraction (CopyFromContainer archives it as a symlink).
	mock.ExtractImageContentFn = func(_ context.Context, _ string, _ []string, destDir string, _ int64) map[string]error {
		require.Equal(t, wpDir, destDir)
		require.NoError(t, os.MkdirAll(filepath.Join(destDir, "var", "lib"), 0o755))
		require.NoError(t, os.Symlink(outside, filepath.Join(destDir, "var", "lib", "grafana")))
		return nil
	}

	binds := b.setupWritablePathBinds(context.Background(), "img",
		[]string{"/var/lib/grafana"}, hostVol, 64<<20)

	assert.NotContains(t, binds, filepath.Join(wpDir, "var", "lib", "grafana"),
		"a symlink bind source must not be mounted into the container")
	assert.Empty(t, binds, "no bind should be emitted for a symlink source")
}

// TestWritablePathExtractDir pins the ENG-543 confinement of the extraction target
// directory. The raw os.MkdirAll it replaced would follow a symlink extracted for an
// earlier writable path and create image content outside destDir on the host;
// creation must instead be confined to destDir and fail closed on an escaping symlink.
func TestWritablePathExtractDir(t *testing.T) {
	t.Run("creates the confined extract dir for a nested path", func(t *testing.T) {
		destDir := filepath.Join(t.TempDir(), writablePathSubdir)
		got, err := writablePathExtractDir(destDir, "var/lib/grafana")
		require.NoError(t, err)
		assert.Equal(t, filepath.Join(destDir, "var", "lib"), got)
		assert.DirExists(t, filepath.Join(destDir, "var", "lib"))
	})

	t.Run("single-component path extracts at destDir", func(t *testing.T) {
		destDir := filepath.Join(t.TempDir(), writablePathSubdir)
		got, err := writablePathExtractDir(destDir, "data")
		require.NoError(t, err)
		assert.Equal(t, destDir, got)
		assert.DirExists(t, destDir)
	})

	t.Run("escaping symlink at a dir component is rejected", func(t *testing.T) {
		destDir := filepath.Join(t.TempDir(), writablePathSubdir)
		outside := t.TempDir()
		require.NoError(t, os.MkdirAll(destDir, 0o700))
		// An earlier writable path's extraction planted destDir/var -> outside.
		require.NoError(t, os.Symlink(outside, filepath.Join(destDir, "var")))

		_, err := writablePathExtractDir(destDir, "var/lib/grafana")
		require.Error(t, err, "must refuse to create an extract dir through an escaping symlink")
		assert.NoDirExists(t, filepath.Join(outside, "lib"), "must not have created through the symlink")
	})
}

// TestDeprovision_MultiItemPartialRenameRetry_KeepsFullItems guards the
// retained-disk UNDER-count regression the ENG-406 review flagged: two stateful
// services where db renames OK on attempt 1 but cache's rename fails, forcing a
// retry. On the retry db is already in the retained namespace and gone from
// volumes.List(), so a List-derived record Items would DROP db — under-counting
// the still-on-disk retained db volume (→ over-admit/ENOSPC) and corrupting the
// restore shape. The fix keeps the record's Items the FULL closure set, so both
// services stay counted across the retry. (No volume is writable-path-only here;
// the point is the record's Items stability, not the reclaim path.)
func TestDeprovision_MultiItemPartialRenameRetry_KeepsFullItems(t *testing.T) {
	server, callbackDone := retainCloseServer(t)

	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	items := []backend.LeaseItem{
		{SKU: "docker-micro", Quantity: 1, ServiceName: "db"},
		{SKU: "docker-micro", Quantity: 1, ServiceName: "cache"},
	}
	dbVol := canonicalVolumeName("lease-rt", "db", 0)
	cacheVol := canonicalVolumeName("lease-rt", "cache", 0)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-rt": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-rt", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1"},
			CallbackURL:  server.URL,
			Items:        items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
				"db":    {Image: "redis:7"},
				"cache": {Image: "memcached:1"},
			}},
		}},
	})

	withMicroSKU(b, 512)
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-rt-db-0", "docker-micro", "tenant-a"))
	require.NoError(t, b.pool.TryAllocate("lease-rt-cache-0", "docker-micro", "tenant-a"))

	volRoot := t.TempDir()
	stageVolumeDirs(t, volRoot, map[string][]string{
		dbVol:    {"data"}, // stateful
		cacheVol: {"data"}, // stateful
	})

	renamedSet := map[string]bool{}
	cacheRenameAttempts := 0
	var renameLog [][2]string
	b.volumes = &mockVolumeManager{
		defaultDir: volRoot,
		ListFn: func() ([]string, error) {
			// Return only volumes still under their canonical name (not yet renamed).
			var out []string
			for _, v := range []string{dbVol, cacheVol} {
				if !renamedSet[v] {
					out = append(out, v)
				}
			}
			return out, nil
		},
		RenameVolumeFn: func(old, newName string) error {
			if old == cacheVol {
				cacheRenameAttempts++
				if cacheRenameAttempts == 1 {
					return assert.AnError // transient failure on the first attempt
				}
			}
			renamedSet[old] = true
			renameLog = append(renameLog, [2]string{old, newName})
			return nil
		},
		DestroyFn: func(_ context.Context, id string) error {
			t.Errorf("no stateful volume must be destroyed; got Destroy(%s)", id)
			return nil
		},
	}

	// Attempt 1 fails on the cache rename → lease kept Failed for retry.
	require.Error(t, b.Deprovision(context.Background(), "lease-rt"),
		"attempt 1 must fail on the transient cache-rename error")

	// Retry succeeds.
	require.NoError(t, b.Deprovision(context.Background(), "lease-rt"))
	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	// Both stateful volumes end up retained (db on attempt 1, cache on the retry).
	assert.ElementsMatch(t, [][2]string{
		{dbVol, retainedName(dbVol)},
		{cacheVol, retainedName(cacheVol)},
	}, renameLog)

	rec, err := rs.Get("lease-rt")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, items, rec.Items,
		"record Items must stay the FULL set across the partial-rename retry (no under-count, no restore-shape corruption)")
	assert.ElementsMatch(t, []string{retainedName(dbVol), retainedName(cacheVol)}, rec.RetainedVolumeNames)

	s := b.pool.Stats()
	assert.Equal(t, int64(0), s.AllocatedDiskMB, "live released after the retry completes")
	assert.Equal(t, int64(1024), s.RetainedDiskMB,
		"both retained volumes must be counted (the dangerous direction is UNDER-count → over-admit)")
}

// TestDeprovision_RefuseToRetain_WpDestroyFail_KeepsLiveCounted guards the
// over-admit hazard the ENG-406 review flagged (Copilot): in the refuse-to-retain
// branch, releasing the live allocation must be gated on the OVERALL volume-error
// count, not just NEW errors from destroyOnRefuseToRetain. A writable-path-only
// Destroy that failed BEFORE the switch leaves bytes on disk; if the subsequent
// refuse-destroy succeeds, the old `len(volumeErrs) == prevErrCount` guard would
// still release live (under-count → over-admit/ENOSPC). Here the wp-only Destroy
// fails while the refused stateful Destroy succeeds, so live must stay counted.
func TestDeprovision_RefuseToRetain_WpDestroyFail_KeepsLiveCounted(t *testing.T) {
	server, _ := retainCloseServer(t)

	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	items := []backend.LeaseItem{
		{SKU: "docker-micro", Quantity: 1, ServiceName: "db"},   // stateful → refused (destroyed OK)
		{SKU: "docker-micro", Quantity: 1, ServiceName: "dash"}, // writable-path-only → Destroy FAILS
	}
	dbVol := canonicalVolumeName("lease-rf", "db", 0)
	dashVol := canonicalVolumeName("lease-rf", "dash", 0)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-rf": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-rf", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1"},
			CallbackURL:  server.URL,
			Items:        items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
				"db":   {Image: "redis:7"},
				"dash": {Image: "grafana/grafana:11.1.0"},
			}},
		}},
	})

	withMicroSKU(b, 512)
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.cfg.MaxRetainedDiskMB = 300 // < 512 → durableItems=[db]=512 breaches → refuse-to-retain
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-rf-db-0", "docker-micro", "tenant-a"))
	require.NoError(t, b.pool.TryAllocate("lease-rf-dash-0", "docker-micro", "tenant-a"))
	require.Equal(t, int64(1024), b.pool.Stats().AllocatedDiskMB, "pre-condition: live F=1024 MB")

	volRoot := t.TempDir()
	stageVolumeDirs(t, volRoot, map[string][]string{
		dbVol:   {"data"},                                   // stateful
		dashVol: {filepath.Join(writablePathSubdir, "var")}, // writable-path-only
	})

	b.volumes = &mockVolumeManager{
		defaultDir: volRoot,
		ListFn:     func() ([]string, error) { return []string{dbVol, dashVol}, nil },
		DestroyFn: func(_ context.Context, id string) error {
			if id == dashVol {
				return assert.AnError // the writable-path-only Destroy fails (bytes remain on disk)
			}
			return nil // the refused stateful Destroy succeeds
		},
		RenameVolumeFn: func(_, _ string) error { return nil },
	}

	// The close errors because the wp-only Destroy failed; the lease stays Failed
	// for retry. The deferred hand-off runs on return.
	require.Error(t, b.Deprovision(context.Background(), "lease-rf"),
		"close must surface the failed writable-path-only Destroy")

	// Live must NOT be released: dash's bytes remain on disk, so counting them
	// keeps admission honest until the retry destroys them.
	assert.Equal(t, int64(1024), b.pool.Stats().AllocatedDiskMB,
		"live must stay counted when a reclaimed wp-only volume's Destroy failed (bytes still on disk)")
}

// TestDeprovision_WritablePathSubdirIsFile_RetainedConservatively guards the
// detector hardening (Copilot): a top-level entry NAMED _wp but that is a file or
// symlink (not a directory) is unexpected — setupWritablePathBinds always creates
// _wp as a directory and the container cannot write the volume root — so it must
// NOT count as writable-path scaffolding. Since the gate action is DESTROY, such a
// volume is retained conservatively, never reclaimed.
func TestDeprovision_WritablePathSubdirIsFile_RetainedConservatively(t *testing.T) {
	server, callbackDone := retainCloseServer(t)

	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "web"}}
	canonical0 := canonicalVolumeName("lease-wpf", "web", 0)

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-wpf": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-wpf", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "redis:7"}}},
		}},
	})

	withMicroSKU(b, 512)
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-wpf-web-0", "docker-micro", "tenant-a"))

	// Stage a volume whose ONLY top-level entry named _wp is a FILE, not a directory.
	volRoot := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(volRoot, canonical0), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(volRoot, canonical0, writablePathSubdir), []byte("x"), 0o600))

	var destroyed []string
	var renamed [][2]string
	b.volumes = &mockVolumeManager{
		defaultDir: volRoot,
		ListFn:     func() ([]string, error) { return []string{canonical0}, nil },
		DestroyFn: func(_ context.Context, id string) error {
			destroyed = append(destroyed, id)
			return nil
		},
		RenameVolumeFn: func(old, newName string) error {
			renamed = append(renamed, [2]string{old, newName})
			return nil
		},
	}

	require.NoError(t, b.Deprovision(context.Background(), "lease-wpf"))

	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	assert.Empty(t, destroyed, "a non-directory _wp must NOT be treated as scaffolding → never destroyed")
	assert.Equal(t, [][2]string{{canonical0, retainedName(canonical0)}}, renamed,
		"volume with a non-directory _wp must be retained conservatively")
	rec, err := rs.Get("lease-wpf")
	require.NoError(t, err)
	assert.NotNil(t, rec, "conservatively-retained volume must have a record")
}

// TestDeprovision_PartialInstanceRetain_CapCheckCountsOnlyRetained guards the
// durableItems per-instance fix (Copilot): with per-volume classification a
// Quantity>1 service can have a SUBSET of instances retained. The cap check
// (shouldRefuseRetention, whose action is DESTROY) must count only the instances
// actually retained, not the service's full Quantity — otherwise the over-count
// could spuriously breach the cap and destroy the retained durable volume.
//
// Setup: one service, Quantity=2; instance 0 is writable-path-only (reclaimed),
// instance 1 is stateful (retained). Cap = 600 MB. The accurate retained footprint
// is 1×512 = 512 ≤ 600 (retain), but the buggy full-Quantity count would be
// 2×512 = 1024 > 600 (refuse → destroy the retained instance).
func TestDeprovision_PartialInstanceRetain_CapCheckCountsOnlyRetained(t *testing.T) {
	server, callbackDone := retainCloseServer(t)

	rs, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retention.db"),
	})
	require.NoError(t, err)
	defer rs.Close()

	items := []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "svc"}}
	inst0 := canonicalVolumeName("lease-pi", "svc", 0) // writable-path-only → reclaimed
	inst1 := canonicalVolumeName("lease-pi", "svc", 1) // stateful → retained

	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-pi": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "lease-pi", Tenant: "tenant-a", ProviderUUID: "prov-1",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   server.URL,
			Items:         items,
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"svc": {Image: "redis:7"}}},
		}},
	})

	withMicroSKU(b, 512)
	b.retentionStore = rs
	b.cfg.RetainOnClose = true
	b.cfg.MaxRetainedDiskMB = 600 // accurate 512 retains; over-counted 1024 would refuse+destroy
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.pool.TryAllocate("lease-pi-svc-0", "docker-micro", "tenant-a"))
	require.NoError(t, b.pool.TryAllocate("lease-pi-svc-1", "docker-micro", "tenant-a"))

	volRoot := t.TempDir()
	stageVolumeDirs(t, volRoot, map[string][]string{
		inst0: {filepath.Join(writablePathSubdir, "var")}, // writable-path-only
		inst1: {"data"},                                   // stateful
	})

	var destroyed []string
	var renamed [][2]string
	b.volumes = &mockVolumeManager{
		defaultDir: volRoot,
		ListFn:     func() ([]string, error) { return []string{inst0, inst1}, nil },
		DestroyFn: func(_ context.Context, id string) error {
			destroyed = append(destroyed, id)
			return nil
		},
		RenameVolumeFn: func(old, newName string) error {
			renamed = append(renamed, [2]string{old, newName})
			return nil
		},
	}

	require.NoError(t, b.Deprovision(context.Background(), "lease-pi"))

	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	// The retained stateful instance must NOT be destroyed by a spurious cap breach.
	assert.Equal(t, []string{inst0}, destroyed, "only the writable-path-only instance must be reclaimed")
	assert.Equal(t, [][2]string{{inst1, retainedName(inst1)}}, renamed,
		"the retained stateful instance must survive (cap check must count only retained instances)")
	rec, err := rs.Get("lease-pi")
	require.NoError(t, err)
	require.NotNil(t, rec, "the partially-retained lease must keep a record for its durable instance")
}
