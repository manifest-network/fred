package docker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
)

func TestLaunchVolumeRejectsZeroAndReleasedCopies(t *testing.T) {
	assertUnavailable := func(volume launchVolume) {
		t.Helper()
		_, err := volume.rootPath()
		require.Error(t, err)
		require.Error(t, volume.removeWritablePaths(t.Context()))
		_, err = volume.extractImageContent(t.Context(), imageexec.Image{}, []string{"/cache"}, 1024, 16)
		require.Error(t, err)
		_, err = volume.prepareStatefulVolumeBinds(t.Context(), []string{"/data"}, 0, 0)
		require.Error(t, err)
	}
	assertUnavailable(launchVolume{})
	mock := &mockDockerClient{ListVolumeWritersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil }}
	mock.ExtractImageContentFn = func(context.Context, string, []string, string, int64, int64) map[string]error {
		t.Fatal("expired volume authority reached extraction")
		return nil
	}
	b := newBackendForTest(mock, nil)
	defer b.stopCancel()
	lease := "550e8400-e29b-41d4-a716-446655440000"
	path := filepath.Join(t.TempDir(), canonicalVolumeName(lease, "app", 0))
	marker := filepath.Join(path, writablePathSubdir, "keep")
	require.NoError(t, os.MkdirAll(filepath.Dir(marker), 0o700))
	require.NoError(t, os.WriteFile(marker, []byte("unchanged"), 0o600))
	runSubjectStorageObservationForTest(t, b, lease, func(mutations *storageMutations) bool {
		q := quiescedVolumeForTest(t, mutations, path)
		volume, err := q.lookup(filepath.Base(path))
		require.NoError(t, err)
		copied := volume
		q.release()
		assertUnavailable(volume)
		assertUnavailable(copied)
		_, err = q.lookup(filepath.Base(path))
		require.Error(t, err)
		return true
	})
	data, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, "unchanged", string(data))
	require.NoDirExists(t, filepath.Join(path, "data"))
}

func TestLaunchVolumeCanPrepareOnlyItsReservedDirectory(t *testing.T) {
	mock := &mockDockerClient{ListVolumeWritersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil }}
	b := newBackendForTest(mock, nil)
	defer b.stopCancel()
	lease := "550e8400-e29b-41d4-a716-446655440000"
	root := t.TempDir()
	pathA := filepath.Join(root, canonicalVolumeName(lease, "app", 0))
	pathB := filepath.Join(root, canonicalVolumeName(lease, "app", 1))
	for _, path := range []string{pathA, pathB} {
		require.NoError(t, os.MkdirAll(filepath.Join(path, writablePathSubdir), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(path, writablePathSubdir, "old"), []byte("keep"), 0o600))
	}
	extractions := 0
	mock.ExtractImageContentFn = func(_ context.Context, _ string, _ []string, destination string, _, _ int64) map[string]error {
		extractions++
		require.Equal(t, filepath.Join(pathA, writablePathSubdir), destination)
		require.NoFileExists(t, filepath.Join(destination, "old"))
		require.NoError(t, os.MkdirAll(filepath.Join(destination, "cache"), 0o700))
		return nil
	}
	runSubjectStorageMutationForTest(t, b, lease, func(mutations *storageMutations) bool {
		q := quiescedVolumeForTest(t, mutations, pathA)
		defer q.release()
		unreserved, err := parseManagedVolumeName(filepath.Base(pathB))
		require.NoError(t, err)
		require.True(t, mutations.volumeNameInScope(unreserved), "lease scope alone would admit the unreserved sibling")
		_, err = q.lookup(unreserved.value())
		require.ErrorContains(t, err, "outside this launch")
		volume, err := q.lookup(filepath.Base(pathA))
		require.NoError(t, err)
		require.NoError(t, volume.removeWritablePaths(t.Context()))
		_, err = volume.extractImageContent(t.Context(), admittedFixtureImage(t, "nginx"), []string{"/cache"}, 1024, 16)
		require.NoError(t, err)
		binds, err := volume.prepareStatefulVolumeBinds(t.Context(), []string{"/data"}, 0, 0)
		require.NoError(t, err)
		require.Equal(t, map[string]string{filepath.Join(pathA, "data"): "/data"}, binds)
		return true
	})
	require.Equal(t, 1, extractions)
	data, err := os.ReadFile(filepath.Join(pathB, writablePathSubdir, "old"))
	require.NoError(t, err)
	require.Equal(t, "keep", string(data))
	require.NoDirExists(t, filepath.Join(pathB, "data"))
	require.NoDirExists(t, filepath.Join(pathB, writablePathSubdir, "cache"))
}
