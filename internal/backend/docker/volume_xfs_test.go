package docker

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
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

// TestXfsEnsureQuota_MissingVolumeIsNoop verifies the non-resurrecting property:
// EnsureQuota on a volume that does not exist on disk returns nil WITHOUT running
// any xfs_quota command (so it needs no privilege and can't recreate a volume a
// concurrent deprovision just removed). Runs unprivileged — the missing-path
// branch returns before any exec.
func TestXfsEnsureQuota_MissingVolumeIsNoop(t *testing.T) {
	dir := t.TempDir()
	mgr, err := newVolumeManager(dir, "xfs", slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.EnsureQuota(context.Background(), "fred-does-not-exist-app-0", 100),
		"EnsureQuota on a missing volume must be a no-op")
}

// TestBtrfsEnsureQuota_MissingVolumeIsNoop is the btrfs analogue: EnsureQuota on
// a subvolume that does not exist returns nil without running any btrfs command
// (root-free — the missing-path branch returns before any exec).
func TestBtrfsEnsureQuota_MissingVolumeIsNoop(t *testing.T) {
	mgr := &btrfsVolumeManager{dataPath: t.TempDir(), logger: slog.Default()}
	require.NoError(t, mgr.EnsureQuota(context.Background(), "fred-does-not-exist-app-0", 100),
		"btrfs EnsureQuota on a missing subvolume must be a no-op")
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

// newXfsManagerForTest builds a bare xfsVolumeManager over dataPath with empty
// maps — enough to exercise the marker/map logic (resolveProjectID) and the
// Destroy teardown path without any live XFS mount or xfs_quota tooling.
func newXfsManagerForTest(dataPath string) *xfsVolumeManager {
	return &xfsVolumeManager{
		dataPath:   dataPath,
		mountPoint: dataPath,
		logger:     slog.Default(),
		activeIDs:  make(map[uint32]string),
		volumeToID: make(map[string]uint32),
	}
}

// TestResolveProjectID_PrefersMarkerFile pins the ENG-459 resolution rule: the
// on-disk .fred-project-id marker (the authoritative record of the projID that was
// actually tagged + limited, and the only source that survives a restart) wins over
// the in-memory map. Resolving via the marker — never by recomputing crc32(id) — is
// required because assignProjectID's collision-probe can make the derived candidate
// differ from the assigned id, so a recompute-based clear could zero the wrong project.
func TestResolveProjectID_PrefersMarkerFile(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)

	const id = "fred-vol-app-0"
	dir := filepath.Join(dataPath, id)
	require.NoError(t, os.MkdirAll(dir, 0700))
	require.NoError(t, writeProjectIDFile(dir, 4242))
	mgr.volumeToID[id] = 9999 // map disagrees; the on-disk marker must win

	got, ok := mgr.resolveProjectID(id)
	require.True(t, ok)
	assert.Equal(t, uint32(4242), got, "the on-disk marker is authoritative over the in-memory map")
}

// TestResolveProjectID_FallsBackToMapWhenNoMarker covers a volume whose directory
// (and marker) is already gone but which the live process still tracks in-memory —
// the projID is still resolvable so its quota can be cleared.
func TestResolveProjectID_FallsBackToMapWhenNoMarker(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)

	const id = "fred-vol-app-0" // no directory / marker on disk
	mgr.volumeToID[id] = 777

	got, ok := mgr.resolveProjectID(id)
	require.True(t, ok)
	assert.Equal(t, uint32(777), got)
}

// TestResolveProjectID_NotFoundWhenNeitherKnows covers the idempotent re-Destroy /
// never-created case: neither the marker nor the map knows the id, so there is no
// projID to clear (the caller skips the clear).
func TestResolveProjectID_NotFoundWhenNeitherKnows(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)

	_, ok := mgr.resolveProjectID("fred-unknown-app-0")
	assert.False(t, ok, "an already-cleared / never-created volume resolves to no projID")
}

// TestDestroy_QuotaClearFailure_StillRemovesDirAndCounts pins the ENG-459
// best-effort contract on the failure branch (per fred's "test the error branch"
// discipline): if the xfs_quota clear fails, Destroy must still remove the
// directory, still return nil, and record the leak on the observable counter.
// Returning an error would break the caller invariant (a Destroy error means
// "bytes still on disk" → the lease is kept Failed and its footprint counted), and
// a zero-byte quota-clear failure must not strand the lease. The clear is forced to
// fail hermetically by pointing PATH at an empty dir so xfs_quota is not found —
// exercising the exact err!=nil branch a real EPERM (missing CAP_SYS_ADMIN) hits,
// with no root or live XFS mount required.
func TestDestroy_QuotaClearFailure_StillRemovesDirAndCounts(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)

	const id = "fred-vol-app-0"
	dir := filepath.Join(dataPath, id)
	require.NoError(t, os.MkdirAll(dir, 0700))
	require.NoError(t, writeProjectIDFile(dir, 4242))

	// Force the clear's xfs_quota exec to fail (binary not found) — a stand-in for
	// any real clear failure. t.Setenv forbids t.Parallel, which is fine here.
	t.Setenv("PATH", t.TempDir())

	before := testutil.ToFloat64(volumeQuotaClearFailedTotal)
	err := mgr.Destroy(context.Background(), id)
	require.NoError(t, err, "a quota-clear failure must not fail Destroy (would strand the lease)")

	assert.NoDirExists(t, dir, "the directory must still be removed despite the clear failure")
	assert.Equal(t, before+1, testutil.ToFloat64(volumeQuotaClearFailedTotal),
		"a clear failure must be recorded on the leak counter")
}
