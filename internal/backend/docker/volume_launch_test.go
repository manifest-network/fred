package docker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/fsidentity"
)

func TestProtectedVolumeRejectsCopiedAuthorityAfterRelease(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{ListVolumeWritersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil }}, nil)
	defer b.stopCancel()
	lease := "550e8400-e29b-41d4-a716-446655440000"
	path := filepath.Join(t.TempDir(), canonicalVolumeName(lease, "app", 0))
	require.NoError(t, os.Mkdir(path, 0o700))
	runSubjectStorageObservationForTest(t, b, lease, func(mutations *storageMutations) bool {
		q := quiescedVolumeForTest(t, mutations, path)
		copied := *q
		q.release()
		require.Error(t, copied.requireActive())
		copied.release()
		return true
	})
}

func TestProtectedVolumeValidatesCompleteMountGraph(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{ListVolumeWritersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil }}, nil)
	defer b.stopCancel()
	lease := "550e8400-e29b-41d4-a716-446655440000"
	path := filepath.Join(t.TempDir(), canonicalVolumeName(lease, "app", 0))
	require.NoError(t, os.MkdirAll(filepath.Join(path, "data", "child"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(path, "sibling"), 0o700))
	runSubjectStorageObservationForTest(t, b, lease, func(mutations *storageMutations) bool {
		q := quiescedVolumeForTest(t, mutations, path)
		defer q.release()
		project := &composetypes.Project{Services: composetypes.Services{
			"first":  {Volumes: []composetypes.ServiceVolumeConfig{{Type: composetypes.VolumeTypeBind, Source: filepath.Join(path, "data")}}},
			"second": {Volumes: []composetypes.ServiceVolumeConfig{{Type: composetypes.VolumeTypeBind, Source: filepath.Join(path, "data", "child")}}},
		}}
		require.ErrorContains(t, q.validateProject(project), "pending bind source")
		second := project.Services["second"]
		second.Volumes[0].Source = filepath.Join(path, "sibling")
		project.Services["second"] = second
		require.NoError(t, q.validateProject(project))
		outside := t.TempDir()
		require.NoError(t, os.Symlink(outside, filepath.Join(path, "escaped")))
		second.Volumes[0].Source = filepath.Join(path, "escaped")
		project.Services["second"] = second
		require.ErrorContains(t, q.validateProject(project), "escaped")
		return true
	})
}

func TestProtectedVolumeIdentityChangeRefusesBeforeWriterRetirement(t *testing.T) {
	read := false
	b := newBackendForTest(&mockDockerClient{ListVolumeWritersFn: func(context.Context) ([]ContainerInfo, error) {
		read = true
		return nil, nil
	}}, nil)
	defer b.stopCancel()
	lease := "550e8400-e29b-41d4-a716-446655440000"
	name := canonicalVolumeName(lease, "app", 0)
	path := filepath.Join(t.TempDir(), name)
	require.NoError(t, os.Mkdir(path, 0o700))
	identity, err := fsidentity.InspectDirectory(path)
	require.NoError(t, err)
	require.NoError(t, os.Rename(path, path+"-old"))
	require.NoError(t, os.Mkdir(path, 0o700))
	installProtectedVolumeFixture(t, b, path)
	err = runSubjectStorageObservationForTest(t, b, lease, func(mutations *storageMutations) error {
		q, err := b.quiesceLaunchVolumes(context.Background(), mutations, map[string]string{name: path}, nil, map[string]fsidentity.Identity{name: identity})
		q.release()
		return err
	})
	require.ErrorContains(t, err, "physical identity changed")
	require.False(t, read)
}

func TestProtectedVolumeUnrelatedRegularFileBindIsNotInterference(t *testing.T) {
	root, err := fsidentity.OpenDirectory(t.TempDir())
	require.NoError(t, err)
	defer root.Close()
	other := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(other, []byte("config"), 0o600))
	q := &quiescedVolumes{volumes: map[string]protectedVolume{"volume": {root: root}}}
	affected, err := q.affects(other)
	require.NoError(t, err)
	require.False(t, affected)
	affected, err = q.affects(filepath.Dir(root.Path()))
	require.NoError(t, err)
	require.True(t, affected, "a writable parent can exchange the reserved directory")
}
