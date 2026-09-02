package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestStorageIdentityContainerVolumeEvidenceRequiresExactContainerIdentity(t *testing.T) {
	const (
		leaseA = "11111111-1111-4111-8111-111111111111"
		leaseB = "22222222-2222-4222-8222-222222222222"
	)
	tests := []struct {
		name        string
		volumeName  string
		serviceName string
		index       int
		wantError   bool
	}{
		{name: "current", volumeName: canonicalVolumeName(leaseA, "web", 2), serviceName: "web", index: 2},
		{name: "legacy v0.13", volumeName: "fred-" + leaseA + "-2", index: 2},
		{name: "foreign lease", volumeName: canonicalVolumeName(leaseB, "web", 2), serviceName: "web", index: 2, wantError: true},
		{name: "wrong service", volumeName: canonicalVolumeName(leaseA, "worker", 2), serviceName: "web", index: 2, wantError: true},
		{name: "wrong index", volumeName: canonicalVolumeName(leaseA, "web", 1), serviceName: "web", index: 2, wantError: true},
		{name: "retained namespace", volumeName: retainedName(canonicalVolumeName(leaseA, "web", 2)), serviceName: "web", index: 2, wantError: true},
		{name: "noncanonical index", volumeName: "fred-" + leaseA + "-web-02", serviceName: "web", index: 2, wantError: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			source := filepath.Join(root, test.volumeName, "data")
			require.NoError(t, os.MkdirAll(source, 0o700))
			cfg := DefaultConfig()
			cfg.VolumeDataPath = root
			cfg.SKUProfiles = map[string]SKUProfile{
				"stateful": {CPUCores: 1, MemoryMB: 512, DiskMB: 1024},
			}
			container := ContainerInfo{
				ContainerID:   "container-a",
				LeaseUUID:     leaseA,
				SKU:           "stateful",
				ServiceName:   test.serviceName,
				InstanceIndex: test.index,
				Mounts: []ContainerMount{{
					Type: "bind", Source: source, Target: "/data",
				}},
			}

			evidence, err := storageIdentityContainerVolumeEvidence(cfg, []ContainerInfo{container})
			if test.wantError {
				require.Error(t, err)
				assert.Empty(t, evidence)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, map[string]struct{}{test.volumeName: {}}, evidence)
		})
	}
}

func TestStorageIdentityContainerVolumeEvidenceRejectsCrossVolumeSymlink(t *testing.T) {
	const (
		leaseA = "11111111-1111-4111-8111-111111111111"
		leaseB = "22222222-2222-4222-8222-222222222222"
	)
	root := t.TempDir()
	volumeA := canonicalVolumeName(leaseA, "app", 0)
	volumeB := canonicalVolumeName(leaseB, "app", 0)
	require.NoError(t, os.Mkdir(filepath.Join(root, volumeA), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(root, volumeB, "data"), 0o700))
	require.NoError(t, os.Symlink(
		filepath.Join("..", volumeB, "data"),
		filepath.Join(root, volumeA, "data"),
	))
	cfg := DefaultConfig()
	cfg.VolumeDataPath = root
	cfg.SKUProfiles = map[string]SKUProfile{
		"stateful": {CPUCores: 1, MemoryMB: 512, DiskMB: 1024},
	}

	_, err := storageIdentityContainerVolumeEvidence(cfg, []ContainerInfo{
		{
			ContainerID: "container-a", LeaseUUID: leaseA, SKU: "stateful",
			ServiceName: "app", InstanceIndex: 0,
			Mounts: []ContainerMount{{
				Type: "bind", Source: filepath.Join(root, volumeA, "data"), Target: "/data",
			}},
		},
		{
			ContainerID: "container-b", LeaseUUID: leaseB, SKU: "stateful",
			ServiceName: "app", InstanceIndex: 0,
			Mounts: []ContainerMount{{
				Type: "bind", Source: filepath.Join(root, volumeB, "data"), Target: "/data",
			}},
		},
	})
	require.ErrorContains(t, err, "is not a real directory")
}

func TestStorageIdentityContainerVolumeEvidenceRejectsWrongSourceSubtree(t *testing.T) {
	const leaseUUID = "11111111-1111-4111-8111-111111111111"
	root := t.TempDir()
	volumeName := canonicalVolumeName(leaseUUID, "app", 0)
	wrongSource := filepath.Join(root, volumeName, "other")
	require.NoError(t, os.MkdirAll(wrongSource, 0o700))
	cfg := DefaultConfig()
	cfg.VolumeDataPath = root
	cfg.SKUProfiles = map[string]SKUProfile{
		"stateful": {CPUCores: 1, MemoryMB: 512, DiskMB: 1024},
	}

	_, err := storageIdentityContainerVolumeEvidence(cfg, []ContainerInfo{{
		ContainerID: "container-a", LeaseUUID: leaseUUID, SKU: "stateful",
		ServiceName: "app", InstanceIndex: 0,
		Mounts: []ContainerMount{{
			Type: "bind", Source: wrongSource, Target: "/data",
		}},
	}})
	require.ErrorContains(t, err, "does not match target-derived subtree")
}

func TestManagedVolumeEvidenceAuthorityPreservesExactV013Forms(t *testing.T) {
	const leaseUUID = "019d1234-5678-7abc-8def-0123456789ab"
	authority, err := managedVolumeEvidenceAuthorityForLease(
		leaseUUID,
		[]backend.LeaseItem{{SKU: "stateful", Quantity: 2}},
	)
	require.NoError(t, err)

	for _, value := range []string{
		canonicalVolumeName(leaseUUID, "app", 0),
		retainedName(canonicalVolumeName(leaseUUID, "app", 1)),
		fmt.Sprintf("fred-%s-0", leaseUUID),
		fmt.Sprintf("fred-retained-%s-1", leaseUUID),
	} {
		name, parseErr := parseManagedVolumeName(value)
		require.NoError(t, parseErr)
		assert.True(t, authority.containsEither(name), value)
	}

	for _, value := range []string{
		canonicalVolumeName(leaseUUID, "db", 0),
		canonicalVolumeName(leaseUUID, "app", 2),
		fmt.Sprintf("fred-%s-2", leaseUUID),
	} {
		name, parseErr := parseManagedVolumeName(value)
		require.NoError(t, parseErr)
		assert.False(t, authority.containsEither(name), value)
	}

	live, err := parseManagedVolumeName(canonicalVolumeName(leaseUUID, "app", 0))
	require.NoError(t, err)
	assert.False(t, authority.containsRetained(live), "retention rows cannot claim the live namespace")
}

func TestInitializeStorageIdentityRejectsMalformedManagedVolumeBeforePublication(t *testing.T) {
	cfg := storageIdentityLegacyRetentionTestConfig(t)
	writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
	writeLegacyAuthorityStores(t, cfg)
	before := snapshotStorageIdentityAuthorityFiles(t, cfg)

	const malformed = "fred-11111111-1111-4111-8111-111111111111-app-01"
	dockerClient := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
			return DaemonSecurityInfo{SystemID: "daemon-system-a"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	volumes := &mockVolumeManager{ListFn: func() ([]string, error) {
		return []string{malformed}, nil
	}}

	_, err := initializeStorageIdentityWithDependencies(
		t.Context(), cfg, StorageIdentityInitializeAdopt, dockerClient, volumes,
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "name is invalid")
	assertStorageIdentityAuthorityUnchanged(t, cfg, before)
}

func TestStorageIdentityProofRejectsUnattestedManagedVolumeBeforePublication(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(context.Context, Config, storageIdentityProofClient, storageIdentityProofVolumes) error
	}{
		{
			name: "read-only preflight",
			run: func(ctx context.Context, cfg Config, dockerClient storageIdentityProofClient, volumes storageIdentityProofVolumes) error {
				_, err := preflightStorageIdentityAdoptionWithDependencies(ctx, cfg, dockerClient, volumes)
				return err
			},
		},
		{
			name: "initializer",
			run: func(ctx context.Context, cfg Config, dockerClient storageIdentityProofClient, volumes storageIdentityProofVolumes) error {
				_, err := initializeStorageIdentityWithDependencies(
					ctx, cfg, StorageIdentityInitializeAdopt, dockerClient, volumes,
				)
				return err
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := storageIdentityLegacyRetentionTestConfig(t)
			writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
			writeLegacyAuthorityStores(t, cfg)
			before := snapshotStorageIdentityAuthorityFiles(t, cfg)
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

			err := test.run(ctx, cfg, storageIdentityPreflightDockerMock(t), volumes)
			require.ErrorContains(t, err, "plain directory has no quota substrate")
			assert.True(t, attested)
			assertStorageIdentityAuthorityUnchanged(t, cfg, before)
		})
	}
}

func TestAttestManagedVolumeInventoryRejectsNonDirectoryNamespaceEntries(t *testing.T) {
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
			test.plant(t, filepath.Join(root, name))
			manager := &btrfsVolumeManager{dataPath: root, logger: slog.Default()}

			inventory, err := attestManagedVolumeInventory(t.Context(), manager)
			require.Error(t, err)
			assert.Empty(t, inventory)
			assert.ErrorContains(t, err, "not a real directory")
		})
	}
}

func TestInitializeStorageIdentityRejectsHiddenZFSChildBeforePublication(t *testing.T) {
	cfg := storageIdentityLegacyRetentionTestConfig(t)
	cfg.VolumeDataPath = t.TempDir()
	writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
	writeLegacyAuthorityStores(t, cfg)
	before := snapshotStorageIdentityAuthorityFiles(t, cfg)
	name := canonicalVolumeName("11111111-1111-4111-8111-111111111111", "app", 0)

	binDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "zfs.log")
	fakeZFS := filepath.Join(binDir, "zfs")
	require.NoError(t, os.WriteFile(fakeZFS, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$FRED_TEST_ZFS_LOG"
if [ "$*" = "list -H -o name $FRED_TEST_ZFS_ROOT" ]; then
  printf '%s\n' 'tank/fred'
  exit 0
fi
if [ "$*" = 'list -H -r -d 1 -o name tank/fred' ]; then
  printf '%s\n%s\n' 'tank/fred' "tank/fred/$FRED_TEST_ZFS_CHILD"
  exit 0
fi
if [ "$*" = "list -H -p -o name,mounted,mountpoint tank/fred/$FRED_TEST_ZFS_CHILD" ]; then
  printf '%s\tno\t%s/%s\n' "tank/fred/$FRED_TEST_ZFS_CHILD" "$FRED_TEST_ZFS_ROOT" "$FRED_TEST_ZFS_CHILD"
  exit 0
fi
exit 97
`), 0o700))
	t.Setenv("PATH", binDir)
	t.Setenv("FRED_TEST_ZFS_LOG", logPath)
	t.Setenv("FRED_TEST_ZFS_ROOT", cfg.VolumeDataPath)
	t.Setenv("FRED_TEST_ZFS_CHILD", name)
	volumes := &zfsVolumeManager{dataPath: cfg.VolumeDataPath, logger: slog.Default()}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	_, err := initializeStorageIdentityWithDependencies(
		ctx,
		cfg,
		StorageIdentityInitializeAdopt,
		storageIdentityPreflightDockerMock(t),
		volumes,
	)
	require.ErrorContains(t, err, "interrupted unmounted create")
	commands, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	assert.Contains(t, string(commands), "list -H -r -d 1 -o name tank/fred",
		"the proof must inventory datasets that have no expected directory")
	assert.NotContains(t, string(commands), "mount tank/fred/",
		"one-shot lineage proof must not recover an interrupted dataset")
	assertStorageIdentityAuthorityUnchanged(t, cfg, before)
}

func TestStorageIdentityAdoptionRejectsRetentionVolumeIdentityMismatch(t *testing.T) {
	const (
		source = "33333333-3333-4333-8333-333333333333"
		other  = "44444444-4444-4444-8444-444444444444"
	)
	tests := []struct {
		name       string
		volumeName string
	}{
		{name: "wrong namespace", volumeName: canonicalVolumeName(source, "app", 0)},
		{name: "wrong lease", volumeName: retainedName(canonicalVolumeName(other, "app", 0))},
		{name: "wrong service", volumeName: retainedName(canonicalVolumeName(source, "db", 0))},
		{name: "index outside quantity", volumeName: retainedName(canonicalVolumeName(source, "app", 1))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := storageIdentityLegacyRetentionTestConfig(t)
			cfg.VolumeDataPath = t.TempDir()
			writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
			writeLegacyAuthorityStores(t, cfg)
			require.NoError(t, os.Mkdir(filepath.Join(cfg.VolumeDataPath, test.volumeName), 0o700))
			row := []byte(`{"original_lease_uuid":"` + source +
				`","tenant":"tenant-a","provider_uuid":"55555555-5555-4555-8555-555555555555",` +
				`"items":[{"sku":"sku-stateful","quantity":1,"service_name":"app"}],` +
				`"stack_manifest":{"services":{"app":{"image":"docker.io/library/alpine:3.22"}}},` +
				`"callback_url":"https://fred.example/callbacks/provision",` +
				`"retained_volume_names":["` + test.volumeName + `"],"status":"active",` +
				`"generation":0,"created_at":"2026-01-01T02:03:04Z",` +
				`"restoring_since":"0001-01-01T00:00:00Z","reaping_since":"0001-01-01T00:00:00Z"}`)
			writeRawLegacyRetentionRow(t, cfg.RetentionDBPath, source, row)
			before := snapshotStorageIdentityAuthorityFiles(t, cfg)
			volumes := &mockVolumeManager{ListFn: func() ([]string, error) {
				return []string{test.volumeName}, nil
			}}

			verdict, err := preflightStorageIdentityAdoptionWithDependencies(
				t.Context(), cfg, storageIdentityPreflightDockerMock(t), volumes,
			)
			require.Error(t, err)
			assert.Empty(t, verdict)
			assert.ErrorContains(t, err, "is not an exact retained identity")
			assertStorageIdentityAuthorityUnchanged(t, cfg, before)
		})
	}
}

func TestStorageIdentityAdoptionRejectsCrossLeaseContainerVolumeEvidence(t *testing.T) {
	const (
		leaseA       = "66666666-6666-4666-8666-666666666666"
		leaseB       = "77777777-7777-4777-8777-777777777777"
		providerUUID = "88888888-8888-4888-8888-888888888888"
	)
	cfg := storageIdentityLegacyRetentionTestConfig(t)
	cfg.VolumeDataPath = t.TempDir()
	writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
	writeLegacyAuthorityStores(t, cfg)
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: cfg.ReleasesDBPath})
	require.NoError(t, err)
	require.NoError(t, releases.Append(leaseA, shared.Release{
		Manifest:  []byte(`{"image":"docker.io/library/alpine:3.22"}`),
		Image:     "docker.io/library/alpine:3.22",
		Status:    "active",
		CreatedAt: time.Unix(1_700_000_000, 0),
	}))
	require.NoError(t, releases.Close())

	foreignVolume := fmt.Sprintf("fred-%s-0", leaseB)
	foreignSource := filepath.Join(cfg.VolumeDataPath, foreignVolume, "data")
	require.NoError(t, os.MkdirAll(foreignSource, 0o700))
	dockerClient := &mockDockerClient{
		PingFn: func(context.Context) error { return nil },
		DaemonInfoFn: func(context.Context) (DaemonSecurityInfo, error) {
			return DaemonSecurityInfo{SystemID: "daemon-system-a"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{{
				ContainerID: "container-a", Name: "fred-" + leaseA + "-0",
				LeaseUUID: leaseA, Tenant: "tenant-a", ProviderUUID: providerUUID,
				BackendName: "docker", SKU: "sku-stateful", InstanceIndex: 0,
				CallbackURL: "https://fred.example/callbacks/provision",
				Image:       "docker.io/library/alpine:3.22", Status: "exited",
				Mounts: []ContainerMount{{Type: "bind", Source: foreignSource, Target: "/data"}},
			}}, nil
		},
	}
	volumes := &mockVolumeManager{ListFn: func() ([]string, error) {
		return []string{foreignVolume}, nil
	}}
	before := snapshotStorageIdentityAuthorityFiles(t, cfg)

	verdict, err := preflightStorageIdentityAdoptionWithDependencies(
		t.Context(), cfg, dockerClient, volumes,
	)
	require.Error(t, err)
	assert.Empty(t, verdict)
	assert.ErrorContains(t, err, "expected exact live identity")
	assertStorageIdentityAuthorityUnchanged(t, cfg, before)
}
