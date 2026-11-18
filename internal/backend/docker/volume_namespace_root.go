package docker

import (
	"errors"
	"os"

	"github.com/manifest-network/fred/internal/fsidentity"
)

func pinManagedNamespaceRoot(path string) (*fsidentity.Directory, error) {
	root, err := fsidentity.OpenDirectory(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return root, err
}

func (x *xfsVolumeManager) PinNamespaceRoot(name managedVolumeName) (*fsidentity.Directory, error) {
	return pinManagedNamespaceRoot(x.HostPath(name.value()))
}

func (n *noopVolumeManager) PinNamespaceRoot(managedVolumeName) (*fsidentity.Directory, error) {
	return nil, nil
}
