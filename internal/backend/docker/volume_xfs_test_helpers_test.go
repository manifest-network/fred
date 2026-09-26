package docker

import (
	"errors"
	"os"
)

type xfsProjectAttributeReaderFunc func(*os.Root) (linuxFSXAttr, error)

func (read xfsProjectAttributeReaderFunc) ReadProjectAttributes(root *os.Root) (linuxFSXAttr, error) {
	return read(root)
}

func (xfsProjectAttributeReaderFunc) SetProjectID(*os.Root, uint32) error {
	return errors.New("unexpected project-ID write")
}

type xfsProjectAttributeFuncs struct {
	read func(*os.Root) (linuxFSXAttr, error)
	set  func(*os.Root, uint32) error
}

func (a xfsProjectAttributeFuncs) ReadProjectAttributes(root *os.Root) (linuxFSXAttr, error) {
	return a.read(root)
}

func (a xfsProjectAttributeFuncs) SetProjectID(root *os.Root, projectID uint32) error {
	return a.set(root, projectID)
}

type fixedXFSProjectAttributeReader struct {
	attr   linuxFSXAttr
	err    error
	setErr error
}

func (r fixedXFSProjectAttributeReader) ReadProjectAttributes(*os.Root) (linuxFSXAttr, error) {
	return r.attr, r.err
}

func (r fixedXFSProjectAttributeReader) SetProjectID(*os.Root, uint32) error {
	return r.setErr
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
