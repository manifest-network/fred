package docker

import "os"

type fixedXFSProjectAttributeReader struct {
	attr linuxFSXAttr
	err  error
}

func (r fixedXFSProjectAttributeReader) ReadProjectAttributes(*os.Root) (linuxFSXAttr, error) {
	return r.attr, r.err
}

// The path-scoped wrappers below are test fixtures. Production code already
// holds an attested directory root and uses the root-scoped primitives, which
// keeps path re-resolution out of the mutation boundary.
func (x *xfsVolumeManager) assignProjectID(volumeID string) (uint32, error) {
	projectID, _, err := x.reserveProjectID(volumeID)
	return projectID, err
}

func writeProjectIDFile(dirPath string, id uint32) error {
	root, err := os.OpenRoot(dirPath)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return writeProjectIDFileInVolumeRoot(root, id)
}

func readProjectIDFile(dirPath string) (uint32, error) {
	root, err := os.OpenRoot(dirPath)
	if err != nil {
		return 0, err
	}
	defer func() { _ = root.Close() }()
	return readProjectIDFileInVolumeRoot(root)
}

func writeProjectIDFileAtRoot(root *os.Root, volumeID managedVolumeName, id uint32) error {
	volumeRoot, err := openAttestedManagedVolumeRoot(root, volumeID)
	if err != nil {
		return err
	}
	defer func() { _ = volumeRoot.Close() }()
	return writeProjectIDFileInVolumeRoot(volumeRoot, id)
}
