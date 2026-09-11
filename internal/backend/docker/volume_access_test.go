package docker

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/fsidentity"
)

func TestVolumeAccessReservationSurvivesAliasRename(t *testing.T) {
	root := t.TempDir()
	oldPath, newPath := filepath.Join(root, "old"), filepath.Join(root, "new")
	require.NoError(t, os.Mkdir(oldPath, 0o700))
	oldID, err := fsidentity.InspectDirectory(oldPath)
	require.NoError(t, err)
	require.NoError(t, os.Rename(oldPath, newPath))
	newID, err := fsidentity.InspectDirectory(newPath)
	require.NoError(t, err)
	synctest.Test(t, func(t *testing.T) {
		var access volumeAccessCoordinator
		first, err := access.reserve(context.Background(), []fsidentity.Identity{oldID})
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			second, err := access.reserve(ctx, []fsidentity.Identity{newID})
			second.release()
			result <- err
		}()
		synctest.Wait()
		select {
		case <-result:
			t.Fatal("renaming a reserved directory admitted a second writer")
		default:
		}
		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		// A copied handle shares revocation and may not release capacity twice.
		copied := *first
		first.release()
		copied.release()
		last, err := access.reserve(context.Background(), []fsidentity.Identity{newID})
		require.NoError(t, err)
		last.release()
		require.Empty(t, access.active)
	})
}

func TestVolumeAccessNamespaceMutationWaitsForLaunch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var access volumeAccessCoordinator
		release, err := access.retainNamespace(context.Background())
		require.NoError(t, err)
		completed := make(chan error, 1)
		go func() {
			completed <- access.mutateNamespace(context.Background(), func(context.Context) error { return nil })
		}()
		synctest.Wait()
		select {
		case <-completed:
			t.Fatal("namespace mutation entered while launch retained its paths")
		default:
		}
		release()
		release()
		require.NoError(t, <-completed)
	})
}

func TestVolumeAccessIndependentVolumesDoNotBlock(t *testing.T) {
	var access volumeAccessCoordinator
	first, err := access.reserve(context.Background(), []fsidentity.Identity{{Device: 1, Inode: 1}})
	require.NoError(t, err)
	defer first.release()
	second, err := access.reserve(t.Context(), []fsidentity.Identity{{Device: 1, Inode: 2}})
	require.NoError(t, err)
	defer second.release()
	_, err = access.reserve(t.Context(), []fsidentity.Identity{{}})
	require.Error(t, err)
}
