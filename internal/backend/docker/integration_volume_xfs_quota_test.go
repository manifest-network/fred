//go:build integration

package docker

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// Exercise the real kernel attributes. Root repair
// must restore inheritance without recursively retagging historical descendants;
// such descendants require offline repair outside the startup readiness budget.
func TestIntegration_XFS_QuotaRepairTouchesOnlyRoot(t *testing.T) {
	mount := setupXFSLoopback(t)
	mgr, err := newVolumeManager(mount, "xfs", 1024, slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.Validate())
	const name = "fred-550e8400-e29b-41d4-a716-446655440107-app-0"
	hostPath, created, err := mgr.Create(t.Context(), name, 100)
	require.NoError(t, err)
	require.True(t, created)
	t.Cleanup(func() { _ = volDestroyer(t, mgr).Destroy(t.Context(), name) })
	projID, err := readProjectIDFile(hostPath)
	require.NoError(t, err)

	// Model a root left untagged by an older daemon, with an existing child
	// created before root inheritance has been repaired.
	volumeRoot, err := os.OpenRoot(hostPath)
	require.NoError(t, err)
	require.NoError(t, (linuxXFSProjectAttributes{}).SetProjectID(volumeRoot, 0))
	require.NoError(t, volumeRoot.Close())
	oldChild := filepath.Join(hostPath, "historical-child")
	require.NoError(t, os.Mkdir(oldChild, 0o700))
	readAttr := func(path string) linuxFSXAttr {
		t.Helper()
		root, err := os.OpenRoot(path)
		require.NoError(t, err)
		defer func() { _ = root.Close() }()
		attr, err := (linuxXFSProjectAttributes{}).ReadProjectAttributes(root)
		require.NoError(t, err)
		return attr
	}
	require.Zero(t, readAttr(oldChild).ProjectID)

	require.NoError(t, mgr.EnsureQuota(t.Context(), name, 100))
	rootAttr := readAttr(hostPath)
	assert.Equal(t, projID, rootAttr.ProjectID)
	assert.NotZero(t, rootAttr.XFlags&linuxFSXFlagProjInherit)
	assert.Zero(t, readAttr(oldChild).ProjectID, "quota repair must not visit existing tenant entries")

	newChild := filepath.Join(hostPath, "new-child")
	require.NoError(t, os.Mkdir(newChild, 0o700))
	assert.Equal(t, projID, readAttr(newChild).ProjectID, "future children must inherit the enforced project")
	_, created, err = mgr.Create(t.Context(), name, 100)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Zero(t, readAttr(oldChild).ProjectID, "reuse must not retag existing tenant entries")
}

func TestIntegration_XFS_QuotaRootIOCTLPreservesPinnedInodeAndOtherAttributes(t *testing.T) {
	mount := setupXFSLoopback(t)
	displayPath := filepath.Join(mount, "root-before-rename")
	require.NoError(t, os.Mkdir(displayPath, 0o700))
	root, err := os.OpenRoot(displayPath)
	require.NoError(t, err)
	defer func() { _ = root.Close() }()
	kernel := linuxXFSProjectAttributes{}

	// Preserve an unrelated, real XFS flag and an extent-size inheritance hint.
	// The test's raw ioctl changes fixture metadata; production exposes only
	// project assignment on the already-attested directory capability.
	file, err := root.Open(".")
	require.NoError(t, err)
	before, err := readXFSProjectAttributes(file)
	require.NoError(t, err)
	const noDump, extentSizeInherit = uint32(0x80), uint32(0x1000)
	before.XFlags |= noDump | extentSizeInherit
	before.ExtentSize = 64 << 10
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, file.Fd(), linuxFSIOCFSSetXAttr,
		uintptr(unsafe.Pointer(&before))) // #nosec G103 -- test fixture Linux fsxattr UAPI buffer
	require.Zero(t, errno)
	require.NoError(t, file.Close())
	before, err = kernel.ReadProjectAttributes(root)
	require.NoError(t, err)
	require.NotZero(t, before.XFlags&noDump)
	require.Equal(t, uint32(64<<10), before.ExtentSize)

	// Old descendants stay in project 0. Their depth and count cannot become
	// part of the root repair operation.
	deep := "."
	for index := range 32 {
		deep = filepath.Join(deep, fmt.Sprintf("depth-%d", index))
		require.NoError(t, root.Mkdir(deep, 0o700))
	}
	for index := range 256 {
		require.NoError(t, root.Mkdir(fmt.Sprintf("child-%d", index), 0o700))
	}
	movedPath := filepath.Join(mount, "pinned-root")
	require.NoError(t, os.Rename(displayPath, movedPath))
	require.NoError(t, os.Mkdir(displayPath, 0o700))
	replacement, err := os.OpenRoot(displayPath)
	require.NoError(t, err)
	defer func() { _ = replacement.Close() }()

	const projectID = uint32(424243)
	require.NoError(t, kernel.SetProjectID(root, projectID))
	after, err := kernel.ReadProjectAttributes(root)
	require.NoError(t, err)
	want := before
	want.ProjectID = projectID
	want.XFlags |= linuxFSXFlagProjInherit
	// Directory growth may change extent counts; fields configurable through
	// FSSETXATTR must remain byte-for-byte intact apart from project inheritance.
	assert.Equal(t, want.XFlags, after.XFlags)
	assert.Equal(t, want.ExtentSize, after.ExtentSize)
	assert.Equal(t, want.CowExtSize, after.CowExtSize)
	assert.Equal(t, projectID, after.ProjectID)
	untouched, err := kernel.ReadProjectAttributes(replacement)
	require.NoError(t, err)
	assert.Zero(t, untouched.ProjectID, "a replacement at the old pathname must not be modified")
	for _, path := range []string{deep, "child-0", "child-255"} {
		child, err := root.OpenRoot(path)
		require.NoError(t, err)
		attr, err := kernel.ReadProjectAttributes(child)
		require.NoError(t, err)
		require.NoError(t, child.Close())
		assert.Zero(t, attr.ProjectID, "existing descendant %s must remain unmodified", path)
	}
	require.NoError(t, root.Mkdir("new-child", 0o700))
	newChild, err := root.OpenRoot("new-child")
	require.NoError(t, err)
	attr, err := kernel.ReadProjectAttributes(newChild)
	require.NoError(t, err)
	require.NoError(t, newChild.Close())
	assert.Equal(t, projectID, attr.ProjectID)
}
