package docker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestStorageInitializationRefusesJournalsWithoutIdentity(t *testing.T) {
	for _, test := range []struct {
		name  string
		write func(*testing.T, Config)
		want  string
	}{
		{
			name: "complete v0.13 lineage",
			write: func(t *testing.T, cfg Config) {
				writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
				writeLegacyAuthorityStores(t, cfg)
			},
			want: "3 of 3 authoritative backend journals exist without a storage identity",
		},
		{
			name: "callback store only",
			write: func(t *testing.T, cfg Config) {
				writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
			},
			want: "1 of 3 authoritative backend journals exist without a storage identity",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := storageIdentityEvidenceTestConfig(t)
			test.write(t, cfg)
			paths := bindStorageIdentityEvidenceTestPaths(t, cfg)

			resuming, err := requireFreshStorageInitialization(cfg, paths, "daemon-system-a")
			require.ErrorContains(t, err, test.want)
			assert.ErrorContains(t, err, "final 0.14 release")
			assert.False(t, resuming)
		})
	}

	t.Run("empty backend", func(t *testing.T) {
		cfg := storageIdentityEvidenceTestConfig(t)
		paths := bindStorageIdentityEvidenceTestPaths(t, cfg)

		resuming, err := requireFreshStorageInitialization(cfg, paths, "daemon-system-a")
		require.NoError(t, err)
		assert.False(t, resuming)
	})
}

// An interrupted seal leaves a pending marker that records how it was started.
// A fresh one is resumed. One started as a v0.13 adoption is refused: only the
// final 0.14 release can finish it.
func TestStorageInitializationResumesOnlyAFreshPendingSeal(t *testing.T) {
	for _, test := range []struct {
		profile backendidentity.InitializationProfile
		wantErr string
	}{
		{profile: backendidentity.InitializationProfileFresh},
		{
			profile: backendidentity.InitializationProfileExisting,
			wantErr: "only the final 0.14 release can finish adopting a v0.13 lineage",
		},
	} {
		t.Run(string(test.profile), func(t *testing.T) {
			cfg := storageIdentityEvidenceTestConfig(t)
			paths := bindStorageIdentityEvidenceTestPaths(t, cfg)
			interrupted := errors.New("injected prepare interruption")
			_, err := paths.markers.InitializeWithStores(cfg.Name, "daemon-system-a", backendidentity.MarkerPairStoreHooks{
				Profile: test.profile,
				Prepare: func(backendidentity.PendingStorage, backendidentity.InitializationProfile) error {
					return interrupted
				},
				Check:  func(backendidentity.PendingStorage) error { return nil },
				Verify: func(backendidentity.VerifiedStorage) error { return nil },
			})
			require.ErrorIs(t, err, interrupted)

			resuming, err := requireFreshStorageInitialization(cfg, paths, "daemon-system-a")
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				assert.False(t, resuming)
				return
			}
			require.NoError(t, err)
			assert.True(t, resuming)
		})
	}
}

func TestFreshStorageIdentityEvidenceRequiresDrainedCallbacksBeforeInventory(t *testing.T) {
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

	err := verifyFreshStorageIdentityEvidence(
		t.Context(), dockerClient, &mockVolumeManager{}, false, paths,
	)
	require.ErrorContains(t, err, "requires a drained callback outbox")
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
		t.Context(), cfg, dockerClient, &mockVolumeManager{},
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
		t.Context(), lineageA, stableClient, &mockVolumeManager{},
	)
	require.NoError(t, err)
	idB, err := initializeStorageIdentityWithDependencies(
		t.Context(), lineageB, stableClient, &mockVolumeManager{},
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
		t.Context(), lineageA, swappingClient, &mockVolumeManager{},
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
	for path, bucketName := range map[string]string{
		cfg.ReleasesDBPath:  "releases",
		cfg.RetentionDBPath: "retention",
	} {
		db, err := bolt.Open(path, 0o600, nil)
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			_, createErr := tx.CreateBucket([]byte(bucketName))
			return createErr
		}))
		require.NoError(t, db.Close())
	}
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

// The storage identity marker lives beside the callback journal, or at the
// root of the managed volume path when one is configured. A volume path that
// is missing or is not a directory is an error, never a fallback.
func TestDockerStorageIdentityMarkerPath(t *testing.T) {
	cfg := storageIdentityEvidenceTestConfig(t)
	path, err := dockerStorageIdentityMarkerPath(cfg)
	require.NoError(t, err)
	assert.Equal(t, filepath.Clean(cfg.CallbackDBPath)+".storage-identity.json", path)

	root := t.TempDir()
	cfg.VolumeDataPath = root + string(filepath.Separator)
	path, err = dockerStorageIdentityMarkerPath(cfg)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(root, backendidentity.MarkerFilename), path)

	file := filepath.Join(root, "not-a-directory")
	require.NoError(t, os.WriteFile(file, nil, 0o600))
	cfg.VolumeDataPath = file
	_, err = dockerStorageIdentityMarkerPath(cfg)
	require.ErrorContains(t, err, "is not a directory")

	cfg.VolumeDataPath = filepath.Join(root, "missing")
	_, err = dockerStorageIdentityMarkerPath(cfg)
	require.ErrorIs(t, err, os.ErrNotExist)
}

// The managed volume path must resolve inside the declared mount, after
// symlinks: a path that leaves the mount through a link or a parent step is
// refused, and one that does not exist is reported as missing.
func TestRequireExistingPathUnderRoot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	inside := filepath.Join(root, "volumes")
	require.NoError(t, os.Mkdir(inside, 0o700))
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "escape")))
	require.NoError(t, os.Symlink(inside, filepath.Join(root, "alias")))

	require.NoError(t, requireExistingPathUnderRoot(root, root))
	require.NoError(t, requireExistingPathUnderRoot(root, inside))
	require.NoError(t, requireExistingPathUnderRoot(root, filepath.Join(root, "alias")),
		"a link that stays inside the root resolves inside it")
	require.ErrorContains(t, requireExistingPathUnderRoot(root, outside), "escapes configured root")
	require.ErrorContains(t, requireExistingPathUnderRoot(root, filepath.Join(root, "escape")), "escapes configured root")
	require.ErrorContains(t, requireExistingPathUnderRoot(inside, root), "escapes configured root")
	require.ErrorIs(t, requireExistingPathUnderRoot(root, filepath.Join(root, "missing")), os.ErrNotExist)
	require.ErrorIs(t, requireExistingPathUnderRoot(filepath.Join(root, "missing"), inside), os.ErrNotExist)
}
