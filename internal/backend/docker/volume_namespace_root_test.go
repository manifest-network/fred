package docker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/fsidentity"
)

// The default mock models a manager with no physical volumes. Fixtures that
// provide a real root must exercise the same retained directory proof as production.
func (m *mockVolumeManager) PinNamespaceRoot(name managedVolumeName) (*fsidentity.Directory, error) {
	if m.defaultDir == "" {
		return nil, nil
	}
	return pinManagedNamespaceRoot(m.HostPath(name.value()))
}

// This accounting fixture records logical operations without creating a filesystem.
func (f *fakeVolumeBackend) PinNamespaceRoot(managedVolumeName) (*fsidentity.Directory, error) {
	return nil, nil
}

func TestVolumeAccessNamespaceRootProofDistinguishesAbsenceFromUnreadable(t *testing.T) {
	name, _ := namespaceNames(t)
	rootPath := t.TempDir()
	managers := []volumeReader{
		&btrfsVolumeManager{dataPath: rootPath},
		&xfsVolumeManager{dataPath: rootPath},
		&zfsVolumeManager{dataPath: rootPath},
	}
	path := filepath.Join(rootPath, name.value())
	for _, manager := range managers {
		root, err := manager.PinNamespaceRoot(name)
		require.NoError(t, err)
		require.Nil(t, root)
	}
	require.NoError(t, os.Mkdir(path, 0o700))
	for _, manager := range managers {
		root, err := manager.PinNamespaceRoot(name)
		require.NoError(t, err)
		require.NotNil(t, root)
		require.NoError(t, root.VerifyPath())
		require.NoError(t, root.Close())
	}
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Symlink(t.TempDir(), path))
	for _, manager := range managers {
		root, err := manager.PinNamespaceRoot(name)
		require.Error(t, err, "a substituted directory is not positive absence")
		require.Nil(t, root)
	}
	root, err := (&noopVolumeManager{}).PinNamespaceRoot(name)
	require.NoError(t, err)
	require.Nil(t, root)
}
