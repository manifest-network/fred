package docker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestCompensationReseedsSourceWritablePathsAfterDifferentTargetImage(t *testing.T) {
	const lease = "550e8400-e29b-41d4-a716-446655440000"
	name := canonicalVolumeName(lease, "app", 0)
	root := t.TempDir()
	volume := filepath.Join(root, name)
	oldPath := filepath.Join(volume, writablePathSubdir, "var", "lib", "old-app")
	newPath := filepath.Join(volume, writablePathSubdir, "var", "cache", "new-app")
	require.NoError(t, os.MkdirAll(newPath, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(newPath, "target-only"), []byte("new"), 0o600))
	dataPath := filepath.Join(volume, "data", "tenant-data")
	require.NoError(t, os.MkdirAll(filepath.Dir(dataPath), 0o700))
	require.NoError(t, os.WriteFile(dataPath, []byte("retained data"), 0o600))
	image := admittedFixtureImage(t, "old-app:stable")
	var extracted bool
	mock := &mockDockerClient{
		ListVolumeWritersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
		ExtractImageContentFn: func(_ context.Context, gotImage string, paths []string, destination string, maxBytes, maxEntries int64) map[string]error {
			extracted = true
			require.Equal(t, image.ID(), gotImage)
			require.Equal(t, []string{"/var/lib/old-app"}, paths)
			require.Equal(t, filepath.Join(volume, writablePathSubdir), destination)
			require.Equal(t, int64(64*bytesPerMiB), maxBytes)
			require.Positive(t, maxEntries)
			require.NoError(t, os.MkdirAll(oldPath, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(oldPath, "image-default"), []byte("old"), 0o600))
			return nil
		},
	}
	b := newBackendForTest(mock, nil)
	defer b.stopCancel()
	b.cfg.VolumeDataPath = root
	b.cfg.ContainerTmpfsSizeMB = 999
	plan := compensationLaunchPlan{
		Source: shared.Release{
			Items:            []backend.LeaseItem{{SKU: "diskless", ServiceName: "app", Quantity: 1}},
			ResourceProfiles: []shared.SKUResourceSnapshot{{SKU: "diskless", CPUCores: 1, MemoryMB: 64, ScratchDiskMB: 64}},
		},
		Containers: []compensationContainer{{
			Image:  image,
			Config: &container.Config{Labels: map[string]string{LabelServiceName: "app", LabelSKU: "diskless", LabelInstanceIndex: "0"}},
			Mounts: []ContainerMount{{Type: "bind", Source: oldPath, Target: "/var/lib/old-app"}},
		}},
	}
	err := runSubjectStorageMutationForTest(t, b, lease, func(mutations *storageMutations) error {
		q := quiescedVolumeForTest(t, mutations, volume)
		defer q.release()
		return q.prepareCompensationBinds(t.Context(), plan)
	})
	require.NoError(t, err)
	require.True(t, extracted)
	require.FileExists(t, filepath.Join(oldPath, "image-default"))
	require.NoDirExists(t, newPath)
	data, err := os.ReadFile(dataPath)
	require.NoError(t, err)
	require.Equal(t, "retained data", string(data))
}

func TestCompensationMountsRequireExactFrozenDaemonObservation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		binds []string
	}{
		{name: "unobserved source", binds: []string{"/foreign:/data:rw"}},
		{name: "duplicate target", binds: []string{"/managed:/data:rw", "/managed:/data:rw"}},
		{name: "different access", binds: []string{"/managed:/data:ro"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := compensationMountProject([]compensationContainer{{Name: "app", Host: &container.HostConfig{Binds: tc.binds}, Mounts: []ContainerMount{{Type: "bind", Source: "/managed", Target: "/data"}}}})
			require.Error(t, err)
		})
	}
	_, err := compensationMountProject([]compensationContainer{{Name: "app", Host: &container.HostConfig{Binds: []string{"/managed:/data:rw,rprivate"}}, Mounts: []ContainerMount{{Type: "bind", Source: "/managed", Target: "/data"}}}})
	require.NoError(t, err)
}
