package docker

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// devOf returns the st_dev of path (the filesystem it lives on).
func devOf(t *testing.T, path string) uint64 {
	t.Helper()
	var st syscall.Stat_t
	require.NoError(t, syscall.Stat(path, &st))
	return st.Dev
}

// TestResolveMountpoint_ReturnsMountBoundary verifies that resolveMountpoint
// returns a real directory that is a mount boundary: either the filesystem
// root, or a directory whose parent lives on a different device. This is the
// invariant xfs_quota requires of its trailing filesystem argument.
func TestResolveMountpoint_ReturnsMountBoundary(t *testing.T) {
	dir := t.TempDir()

	mp, err := resolveMountpoint(dir)
	require.NoError(t, err)

	// Result must exist and be a genuine path ancestor of the input (or the
	// input itself). filepath.Rel is directory-boundary aware, unlike a raw
	// string prefix which would accept "/tmp" as a "prefix" of "/tmp2/x".
	_, statErr := os.Stat(mp)
	require.NoError(t, statErr, "mountpoint %q does not exist", mp)
	rel, relErr := filepath.Rel(mp, dir)
	require.NoError(t, relErr)
	assert.False(t, strings.HasPrefix(rel, ".."),
		"mountpoint %q is not an ancestor of %q (rel=%q)", mp, dir, rel)

	// Mount-boundary invariant: root, or parent is on a different device.
	if mp != string(os.PathSeparator) {
		parent := filepath.Dir(mp)
		assert.NotEqual(t, devOf(t, mp), devOf(t, parent),
			"resolveMountpoint returned %q whose parent %q is on the SAME device — not a mount boundary", mp, parent)
	}
}

// TestResolveMountpoint_NonexistentLeafWalksUp verifies that a volume_data_path
// that does not exist yet (its subdirectories are created lazily by Create)
// still resolves to the mountpoint of its nearest existing ancestor.
func TestResolveMountpoint_NonexistentLeafWalksUp(t *testing.T) {
	dir := t.TempDir()
	deep := filepath.Join(dir, "does", "not", "exist", "yet")

	got, err := resolveMountpoint(deep)
	require.NoError(t, err)

	want, err := resolveMountpoint(dir)
	require.NoError(t, err)

	assert.Equal(t, want, got,
		"nonexistent leaf should resolve to the same mountpoint as its existing ancestor")
}

// TestResolveMountpoint_Root verifies the root filesystem is its own mountpoint.
func TestResolveMountpoint_Root(t *testing.T) {
	got, err := resolveMountpoint("/")
	require.NoError(t, err)
	assert.Equal(t, "/", got)
}

// TestNewVolumeManager_XFS_ResolvesMountpoint verifies that constructing the
// xfs manager over a volume_data_path that is a *subdirectory* of the mount
// (the production layout) resolves and stores the containing mount point
// separately from the subdirectory. xfs_quota's filesystem argument must be
// that mount point, not the subdir (ENG-449).
func TestNewVolumeManager_XFS_ResolvesMountpoint(t *testing.T) {
	mount := t.TempDir()
	dataPath := filepath.Join(mount, "volumes") // subdir, like /data/fred/volumes
	require.NoError(t, os.MkdirAll(dataPath, 0700))

	mgr, err := newVolumeManager(dataPath, "xfs", slog.Default())
	require.NoError(t, err)

	xm, ok := mgr.(*xfsVolumeManager)
	require.True(t, ok)

	assert.Equal(t, dataPath, xm.dataPath, "dataPath must stay the configured subdir (volumes live here)")

	want, err := resolveMountpoint(dataPath)
	require.NoError(t, err)
	assert.Equal(t, want, xm.mountPoint, "mountPoint must be the resolved XFS mount")
	assert.NotEqual(t, xm.dataPath, xm.mountPoint,
		"regression (ENG-449): the xfs_quota filesystem arg must differ from the volumes subdir")
}

// TestXfsQuotaArgs_TrailingArgIsMountpoint pins the exact shape of every
// xfs_quota invocation: the trailing filesystem argument is the mount point,
// and per-directory commands name the subdirectory only inside -c. This is the
// unit-level guard for the ENG-449 regression (subdir passed as the fs arg).
func TestXfsQuotaArgs_TrailingArgIsMountpoint(t *testing.T) {
	const mount = "/data/fred"
	const dir = "/data/fred/volumes/fred-x-app-0"
	const projID = uint32(1501154529)

	setup := xfsQuotaArgs(xfsProjectSetupCmd(dir, projID), mount)
	assert.Equal(t, mount, setup[len(setup)-1], "project -s: trailing fs arg must be the mount point")
	assert.Contains(t, strings.Join(setup, " "), "project -s -p "+dir,
		"project -s must name the subdir inside -c, not as the fs arg")

	limit := xfsQuotaArgs(xfsLimitCmd(projID, "100m"), mount)
	assert.Equal(t, mount, limit[len(limit)-1], "limit -p: trailing fs arg must be the mount point")
	assert.Contains(t, strings.Join(limit, " "), "limit -p bhard=100m")

	report := xfsQuotaArgs("report -p -b -N", mount)
	assert.Equal(t, mount, report[len(report)-1], "report: trailing fs arg must be the mount point")
}
