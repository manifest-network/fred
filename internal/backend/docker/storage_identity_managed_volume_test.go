package docker

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func storageIdentityEmptyBackendDockerMock() *mockDockerClient {
	return &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
			return DaemonSecurityInfo{SystemID: "daemon-system-a"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
}

// assertStorageIdentityNotInitialized proves a refused initialization created
// neither a marker nor an authoritative journal.
func assertStorageIdentityNotInitialized(t *testing.T, cfg Config) {
	t.Helper()
	assertNoStorageIdentityMarkers(t, cfg)
	for _, path := range []string{cfg.CallbackDBPath, cfg.ReleasesDBPath, cfg.RetentionDBPath} {
		assert.NoFileExists(t, path)
	}
}

func TestInitializeStorageIdentityRejectsMalformedManagedVolumeBeforePublication(t *testing.T) {
	cfg := storageIdentityEvidenceTestConfig(t)

	const malformed = "fred-11111111-1111-4111-8111-111111111111-app-01"
	volumes := &mockVolumeManager{ListFn: func() ([]string, error) {
		return []string{malformed}, nil
	}}

	_, err := initializeStorageIdentityWithDependencies(
		t.Context(), cfg, storageIdentityEmptyBackendDockerMock(), volumes,
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "name is invalid")
	assertStorageIdentityNotInitialized(t, cfg)
}

func TestInitializeStorageIdentityRejectsUnattestedManagedVolumeBeforePublication(t *testing.T) {
	cfg := storageIdentityEvidenceTestConfig(t)
	name := canonicalVolumeName("11111111-1111-4111-8111-111111111111", "app", 0)
	var attested bool
	volumes := &mockVolumeManager{
		ListFn: func() ([]string, error) { return []string{name}, nil },
		AttestManagedVolumeFn: func(ctx context.Context, got managedVolumeName) error {
			attested = true
			assert.Equal(t, name, got.value())
			return errors.New("plain directory has no quota substrate")
		},
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	_, err := initializeStorageIdentityWithDependencies(
		ctx, cfg, storageIdentityEmptyBackendDockerMock(), volumes,
	)
	require.ErrorContains(t, err, "plain directory has no quota substrate")
	assert.True(t, attested)
	assertStorageIdentityNotInitialized(t, cfg)
}

// A managed volume on the host means the backend is not empty, whatever the
// journals say.
func TestInitializeStorageIdentityRefusesBackendWithManagedVolume(t *testing.T) {
	cfg := storageIdentityEvidenceTestConfig(t)
	name := canonicalVolumeName("11111111-1111-4111-8111-111111111111", "app", 0)
	volumes := &mockVolumeManager{ListFn: func() ([]string, error) { return []string{name}, nil }}

	_, err := initializeStorageIdentityWithDependencies(
		t.Context(), cfg, storageIdentityEmptyBackendDockerMock(), volumes,
	)
	require.ErrorContains(t, err, "new storage identity requires an empty backend")
	assert.ErrorContains(t, err, "managed_volumes=1")
	assertStorageIdentityNotInitialized(t, cfg)
}

func TestAttestManagedVolumeInventoryPreservesNonVolumeNamespaceEntries(t *testing.T) {
	const leaseUUID = "11111111-1111-4111-8111-111111111111"
	for _, test := range []struct {
		name  string
		plant func(*testing.T, string)
	}{
		{
			name: "regular file",
			plant: func(t *testing.T, path string) {
				t.Helper()
				require.NoError(t, os.WriteFile(path, []byte("foreign"), 0o600))
			},
		},
		{
			name: "symlink",
			plant: func(t *testing.T, path string) {
				t.Helper()
				require.NoError(t, os.Symlink(t.TempDir(), path))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			name := canonicalVolumeName(leaseUUID, "app", 0)
			artifact := filepath.Join(root, name)
			test.plant(t, artifact)
			manager := &xfsVolumeManager{
				dataPath: root, mountPoint: root, logger: slog.Default(),
				activeIDs: make(map[uint32]string), volumeToID: make(map[string]uint32),
			}

			inventory, err := attestManagedVolumeInventory(t.Context(), manager)
			require.NoError(t, err)
			assert.Empty(t, inventory)
			_, statErr := os.Lstat(artifact)
			require.NoError(t, statErr,
				"an unattributed non-volume artifact must be preserved, not consumed as storage")
		})
	}
}
