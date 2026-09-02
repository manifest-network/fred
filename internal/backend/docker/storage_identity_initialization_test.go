package docker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestStorageIdentityEvidenceEmptyV013LineageRequiresExplicitNewMode(t *testing.T) {
	cfg := storageIdentityEvidenceTestConfig(t)
	writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
	writeLegacyAuthorityStores(t, cfg)
	dockerClient := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
			return DaemonSecurityInfo{SystemID: "daemon-system-a"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	volumes := &mockVolumeManager{}
	paths := bindStorageIdentityEvidenceTestPaths(t, cfg)

	err := verifyStorageIdentityInitializationEvidence(
		t.Context(), cfg, dockerClient, volumes, StorageIdentityInitializeAdopt,
		backendidentity.InitializationProfileExisting, false, paths,
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "drained v0.13 callback outbox")
	assert.ErrorContains(t, err, "no managed containers, retentions, or volumes")
	assert.ErrorContains(t, err, "-initialize-storage-identity new")
	assert.ErrorContains(t, err, "expected empty v0.13 substrate")
	assert.ErrorContains(t, err, "not lost state")
	assert.ErrorContains(t, err, "legacy callback outbox was fully drained")

	require.NoError(t, verifyStorageIdentityInitializationEvidence(
		t.Context(), cfg, dockerClient, volumes, StorageIdentityInitializeNew,
		backendidentity.InitializationProfileExisting, false, paths,
	))
}

func TestStorageIdentityEvidenceNeverSuggestsNewWithPendingLegacyCallback(t *testing.T) {
	cfg := storageIdentityEvidenceTestConfig(t)
	writeLegacyCallbackStore(t, cfg.CallbackDBPath, map[string][]byte{
		"550e8400-e29b-41d4-a716-446655440000": []byte(`{"status":"ready"}`),
	})
	dockerClient := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
			return DaemonSecurityInfo{SystemID: "daemon-system-a"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			t.Fatal("container inventory must not run before the callback drain proof")
			return nil, nil
		},
	}
	paths := bindStorageIdentityEvidenceTestPaths(t, cfg)

	err := verifyStorageIdentityInitializationEvidence(
		t.Context(), cfg, dockerClient, &mockVolumeManager{}, StorageIdentityInitializeAdopt,
		backendidentity.InitializationProfileExisting, false, paths,
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "requires a drained callback outbox")
	assert.NotContains(t, err.Error(), "use new")
}

func TestStorageIdentityEvidenceAdmitsExactV013Boundary2WithRenamedVolume(t *testing.T) {
	const (
		leaseUUID    = "550e8400-e29b-41d4-a716-446655440000"
		providerUUID = "22222222-2222-4222-8222-222222222222"
		callbackURL  = "https://fred.example/callbacks/provision"
	)
	cfg := storageIdentityEvidenceTestConfig(t)
	cfg.VolumeDataPath = t.TempDir()
	cfg.SKUMapping = map[string]string{"sku-stateful": "stateful"}
	cfg.SKUProfiles = map[string]SKUProfile{
		"stateful": {CPUCores: 1, MemoryMB: 512, DiskMB: 1024},
	}
	writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
	writeLegacyAuthorityStores(t, cfg)
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: cfg.ReleasesDBPath})
	require.NoError(t, err)
	require.NoError(t, releases.Append(leaseUUID, shared.Release{
		Manifest:  []byte(`{"image":"nginx:1.27"}`),
		Image:     "nginx:1.27",
		Status:    "active",
		CreatedAt: time.Unix(1_700_000_000, 0),
	}))
	require.NoError(t, releases.Close())

	newVolume := canonicalVolumeName(leaseUUID, manifest.DefaultServiceName, 0)
	require.NoError(t, os.MkdirAll(filepath.Join(cfg.VolumeDataPath, newVolume, "data"), 0o700))
	oldSource := filepath.Join(cfg.VolumeDataPath, "fred-"+leaseUUID+"-0", "data")
	_, statErr := os.Stat(oldSource)
	require.ErrorIs(t, statErr, os.ErrNotExist,
		"fixture must match Docker's stale bind source after v0.13 renamed the parent")

	dockerClient := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
			return DaemonSecurityInfo{SystemID: "daemon-system-a"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{{
				ContainerID:   "prev-0",
				Name:          "fred-" + leaseUUID + "-app-0-prev",
				LeaseUUID:     leaseUUID,
				Tenant:        "tenant-a",
				ProviderUUID:  providerUUID,
				SKU:           "sku-stateful",
				InstanceIndex: 0,
				CallbackURL:   callbackURL,
				Image:         "nginx:1.27",
				Status:        "exited",
				Mounts: []ContainerMount{{
					Type: "bind", Source: oldSource, Target: "/data",
				}},
			}}, nil
		},
	}
	volumes := &mockVolumeManager{ListFn: func() ([]string, error) {
		return []string{newVolume}, nil
	}}

	verdict, err := preflightStorageIdentityAdoptionWithDependencies(
		t.Context(), cfg, dockerClient, volumes,
	)
	require.NoError(t, err,
		"old-absent/new-present is the exact idempotently resumable v0.13 boundary-2 shape")
	assert.Equal(t, StorageIdentityAdoptionReady, verdict)
}

func TestInitializeStorageIdentityRejectsSeparateStoreParentReplacementBeforePublication(t *testing.T) {
	root := t.TempDir()
	controlParent := filepath.Join(root, "control")
	releaseParent := filepath.Join(root, "releases")
	retiredReleaseParent := filepath.Join(root, "releases-retired")
	require.NoError(t, os.Mkdir(controlParent, 0o700))
	require.NoError(t, os.Mkdir(releaseParent, 0o700))

	cfg := DefaultConfig()
	cfg.CallbackDBPath = filepath.Join(controlParent, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(controlParent, "diagnostics.db")
	cfg.RetentionDBPath = filepath.Join(controlParent, "retentions.db")
	cfg.ReleasesDBPath = filepath.Join(releaseParent, "releases.db")

	var identityReads atomic.Int64
	dockerClient := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
			// Initial read, pre-inspection barrier, then the final barrier after
			// empty journal/container/volume evidence but before publication.
			if identityReads.Add(1) == 3 {
				if err := os.Rename(releaseParent, retiredReleaseParent); err != nil {
					return DaemonSecurityInfo{}, err
				}
				if err := os.Mkdir(releaseParent, 0o700); err != nil {
					return DaemonSecurityInfo{}, err
				}
			}
			return DaemonSecurityInfo{SystemID: "daemon-a"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}

	_, err := initializeStorageIdentityWithDependencies(
		t.Context(), cfg, StorageIdentityInitializeNew, dockerClient, &mockVolumeManager{},
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "releases storage parent changed during lineage proof")

	replacementEntries, readErr := os.ReadDir(releaseParent)
	require.NoError(t, readErr)
	assert.Empty(t, replacementEntries, "replacement release storage must remain unsealed")
	retiredEntries, readErr := os.ReadDir(retiredReleaseParent)
	require.NoError(t, readErr)
	assert.Empty(t, retiredEntries, "the final parent barrier must precede store binding")
	_, markerErr := os.Lstat(filepath.Clean(cfg.CallbackDBPath) + ".storage-identity-anchor.json")
	assert.ErrorIs(t, markerErr, os.ErrNotExist, "no pending marker may precede the final parent barrier")
}

func TestInitializeStorageIdentityCommittedRerunRejectsCompleteSameParentLineageSwap(t *testing.T) {
	parent := t.TempDir()
	lineageA := dockerStorageLineageTestConfig(parent, "lineage-a")
	lineageB := dockerStorageLineageTestConfig(parent, "lineage-b")
	stableClient := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
			return DaemonSecurityInfo{SystemID: "daemon-a"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}

	idA, err := initializeStorageIdentityWithDependencies(
		t.Context(), lineageA, StorageIdentityInitializeNew, stableClient, &mockVolumeManager{},
	)
	require.NoError(t, err)
	idB, err := initializeStorageIdentityWithDependencies(
		t.Context(), lineageB, StorageIdentityInitializeNew, stableClient, &mockVolumeManager{},
	)
	require.NoError(t, err)
	require.NotEqual(t, idA, idB, "the regression requires two independently sealed lineages")

	var identityReads atomic.Int64
	swappingClient := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
			// The committed rerun reads the daemon initially, before its first
			// marker pass, and between its two marker passes. Swap every authority
			// entry on that middle observation while retaining the same physical
			// parent and daemon identity.
			if identityReads.Add(1) == 3 {
				if err := swapDockerStorageLineages(lineageA, lineageB); err != nil {
					return DaemonSecurityInfo{}, err
				}
			}
			return DaemonSecurityInfo{SystemID: "daemon-a"}, nil
		},
	}

	observed, err := initializeStorageIdentityWithDependencies(
		t.Context(), lineageA, StorageIdentityInitializeNew, swappingClient, &mockVolumeManager{},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, backendidentity.ErrMarkerBindingMismatch)
	assert.ErrorContains(t, err, "Docker backend storage identity changed after sealing")
	assert.Equal(t, backendidentity.ID{}, observed)
	assert.Equal(t, int64(3), identityReads.Load(),
		"the replacement must be rejected by the second lineage pass before the final daemon read")
}

func dockerStorageLineageTestConfig(parent, prefix string) Config {
	cfg := DefaultConfig()
	cfg.CallbackDBPath = filepath.Join(parent, prefix+"-callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(parent, prefix+"-diagnostics.db")
	cfg.ReleasesDBPath = filepath.Join(parent, prefix+"-releases.db")
	cfg.RetentionDBPath = filepath.Join(parent, prefix+"-retentions.db")
	return cfg
}

func swapDockerStorageLineages(lineageA, lineageB Config) error {
	filesA := []string{
		lineageA.CallbackDBPath,
		lineageA.ReleasesDBPath,
		lineageA.RetentionDBPath,
		filepath.Clean(lineageA.CallbackDBPath) + ".storage-identity.json",
		filepath.Clean(lineageA.CallbackDBPath) + ".storage-identity-anchor.json",
	}
	filesB := []string{
		lineageB.CallbackDBPath,
		lineageB.ReleasesDBPath,
		lineageB.RetentionDBPath,
		filepath.Clean(lineageB.CallbackDBPath) + ".storage-identity.json",
		filepath.Clean(lineageB.CallbackDBPath) + ".storage-identity-anchor.json",
	}
	for i, pathA := range filesA {
		staged := pathA + ".lineage-swap"
		if err := os.Rename(pathA, staged); err != nil {
			return fmt.Errorf("stage Docker lineage entry %q: %w", pathA, err)
		}
		if err := os.Rename(filesB[i], pathA); err != nil {
			return fmt.Errorf("install alternate Docker lineage entry %q: %w", filesB[i], err)
		}
		if err := os.Rename(staged, filesB[i]); err != nil {
			return fmt.Errorf("retain original Docker lineage entry %q: %w", pathA, err)
		}
	}
	return nil
}

func bindStorageIdentityEvidenceTestPaths(
	t *testing.T,
	cfg Config,
) *dockerStorageInitializationPaths {
	t.Helper()
	markerPath := filepath.Clean(cfg.CallbackDBPath) + ".storage-identity.json"
	anchorPath := filepath.Clean(cfg.CallbackDBPath) + ".storage-identity-anchor.json"
	paths, err := bindDockerStorageInitializationPaths(cfg, markerPath, anchorPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, paths.Close()) })
	return paths
}

func storageIdentityEvidenceTestConfig(t *testing.T) Config {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.CallbackDBPath = filepath.Join(dir, "callbacks.db")
	cfg.ReleasesDBPath = filepath.Join(dir, "releases.db")
	cfg.RetentionDBPath = filepath.Join(dir, "retentions.db")
	return cfg
}

func writeLegacyAuthorityStores(t *testing.T, cfg Config) {
	t.Helper()
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: cfg.ReleasesDBPath})
	require.NoError(t, err)
	require.NoError(t, releases.Close())
	retentions, err := shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: cfg.RetentionDBPath})
	require.NoError(t, err)
	require.NoError(t, retentions.Close())
}

func writeLegacyCallbackStore(t *testing.T, path string, rows map[string][]byte) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		// This is intentionally a wire-level v0.13 fixture. Importing a current
		// schema constructor would also create v2 buckets and stop exercising the
		// legacy-only upgrade boundary.
		bucket, err := tx.CreateBucket([]byte("pending_callbacks"))
		if err != nil {
			return err
		}
		for key, value := range rows {
			if err := bucket.Put([]byte(key), value); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())
}
