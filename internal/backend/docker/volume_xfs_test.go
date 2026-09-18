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
	// Built directly rather than through newVolumeManager, exactly as the btrfs sibling
	// below already is: the constructor now verifies that volume_data_path really is on the
	// filesystem it claims (ENG-687), and a t.TempDir() is tmpfs. This test is about
	// EnsureQuota's missing-path branch, not about construction.
	mgr := &xfsVolumeManager{
		dataPath:   t.TempDir(),
		mountPoint: "/",
		logger:     slog.Default(),
		activeIDs:  make(map[uint32]string),
		volumeToID: make(map[string]uint32),
	}
	require.NoError(t, mgr.EnsureQuota(context.Background(), "fred-550e8400-e29b-41d4-a716-446655440000-app-0", 100),
		"EnsureQuota on a missing volume must be a no-op")
}

// TestBtrfsEnsureQuota_MissingVolumeIsNoop is the btrfs analogue: EnsureQuota on
// a subvolume that does not exist returns nil without running any btrfs command
// (root-free — the missing-path branch returns before any exec).
func TestBtrfsEnsureQuota_MissingVolumeIsNoop(t *testing.T) {
	mgr := &btrfsVolumeManager{dataPath: t.TempDir(), logger: slog.Default()}
	require.NoError(t, mgr.EnsureQuota(context.Background(), "fred-550e8400-e29b-41d4-a716-446655440000-app-0", 100),
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

	// newVolumeManager now verifies the path is really on the filesystem it is configured
	// as (ENG-687), and a t.TempDir() is tmpfs — so the manager is assembled here the same
	// way the constructor does. What this test pins is the ENG-449 field split, which lives
	// in resolveMountpoint and the struct, not in the probe.
	mount, err := resolveMountpoint(dataPath)
	require.NoError(t, err)
	xm := &xfsVolumeManager{dataPath: dataPath, mountPoint: mount, logger: slog.Default()}

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

	limit := xfsQuotaArgs(xfsLimitCmd(projID, "100m", 262144), mount)
	assert.Equal(t, mount, limit[len(limit)-1], "limit -p: trailing fs arg must be the mount point")
	assert.Contains(t, strings.Join(limit, " "), "limit -p bhard=100m")

	report := xfsQuotaArgs("report -p -b -n -N", mount)
	assert.Equal(t, mount, report[len(report)-1], "report: trailing fs arg must be the mount point")
	assert.Equal(t, "report -p -b -n -N -L 1501154529 -U 1501154529", xfsProjectReportCmd("b", projID))
}

// newXfsManagerForTest builds a bare xfsVolumeManager over dataPath with empty
// maps — enough to exercise the marker/map logic (resolveProjectID) and the
// Destroy teardown path without any live XFS mount or xfs_quota tooling.
func newXfsManagerForTest(dataPath string) *xfsVolumeManager {
	return &xfsVolumeManager{
		dataPath:          dataPath,
		mountPoint:        dataPath,
		logger:            slog.Default(),
		projectAttributes: fixedXFSProjectAttributeReader{attr: linuxFSXAttr{XFlags: linuxFSXFlagProjInherit}},
		activeIDs:         make(map[uint32]string),
		volumeToID:        make(map[string]uint32),
	}
}

func installLoggingXFSQuota(t *testing.T) string {
	t.Helper()
	binDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "xfs-quota.log")
	fakeQuota := filepath.Join(binDir, "xfs_quota")
	require.NoError(t, os.WriteFile(fakeQuota, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$FRED_TEST_XFS_LOG"
`), 0o700))
	t.Setenv("PATH", binDir)
	t.Setenv("FRED_TEST_XFS_LOG", logPath)
	return logPath
}

func TestXFSCreateExistingRetagsBeforeApplyingLimit(t *testing.T) {
	dataPath := t.TempDir()
	const (
		name   = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
		projID = uint32(4242)
	)
	dir := filepath.Join(dataPath, name)
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, writeProjectIDFile(dir, projID))

	binDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "xfs-quota.log")
	fakeQuota := filepath.Join(binDir, "xfs_quota")
	require.NoError(t, os.WriteFile(fakeQuota, []byte(`#!/bin/sh
printf '%s\n' "$*" >> "$FRED_TEST_XFS_LOG"
`), 0o700))
	t.Setenv("PATH", binDir)
	t.Setenv("FRED_TEST_XFS_LOG", logPath)
	mgr := newXfsManagerForTest(dataPath)

	hostPath, created, err := mgr.Create(t.Context(), name, 100)
	require.NoError(t, err)
	assert.False(t, created)
	assert.Equal(t, dir, hostPath)
	commands, err := os.ReadFile(logPath)
	require.NoError(t, err)
	logText := string(commands)
	setupAt := strings.Index(logText, "project -s -p "+dir)
	limitAt := strings.Index(logText, "limit -p bhard=100m")
	assert.GreaterOrEqual(t, setupAt, 0, "existing recovery directory must be re-tagged")
	assert.Greater(t, limitAt, setupAt, "quota limit must follow project tagging")
}

func TestXFSCreateEEXISTNeverGrantsCleanupAuthority(t *testing.T) {
	dataPath := t.TempDir()
	const name = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	dirPath := filepath.Join(dataPath, name)
	sentinelPath := filepath.Join(dirPath, "racer-owned")
	require.NoError(t, os.Mkdir(dirPath, 0o700))
	require.NoError(t, os.WriteFile(sentinelPath, []byte("keep"), 0o600))
	mgr := newXfsManagerForTest(dataPath)

	logPath := installLoggingXFSQuota(t)
	_, _, err := mgr.Create(t.Context(), name, 100)
	require.ErrorContains(t, err, "project ID marker")
	assert.FileExists(t, sentinelPath, "an EEXIST loser must not remove the racer's directory")
	assert.NoFileExists(t, filepath.Join(dirPath, projectIDFile), "an EEXIST loser must not retag the racer's directory")
	assert.NoFileExists(t, logPath, "unverified EEXIST must not reach xfs_quota")
	assert.Empty(t, mgr.activeIDs)
	assert.Empty(t, mgr.volumeToID)
}

func TestXFSCreateAssignmentFailurePreservesPreexistingAuthority(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	const (
		name      = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
		otherName = "fred-550e8400-e29b-41d4-a716-446655440000-app-1"
		projID    = uint32(4242)
	)
	// Deliberately inconsistent preexisting evidence makes reservation fail.
	// The fresh directory belongs to this call, but neither map entry does.
	mgr.volumeToID[name] = projID
	mgr.activeIDs[projID] = otherName

	_, _, err := mgr.Create(t.Context(), name, 100)
	require.ErrorContains(t, err, "project ID maps are inconsistent")
	assert.NoDirExists(t, filepath.Join(dataPath, name), "the definitely-created directory may be compensated")
	assert.Equal(t, projID, mgr.volumeToID[name], "preexisting reverse authority must survive compensation")
	assert.Equal(t, otherName, mgr.activeIDs[projID], "preexisting forward authority must survive compensation")
}

func TestXFSDurableStageCleanupRetainsProjectIDUntilRemoval(t *testing.T) {
	dataPath := t.TempDir()
	const name = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	volumeID, err := parseManagedVolumeName(name)
	require.NoError(t, err)
	mgr := newXfsManagerForTest(dataPath)
	reservedID, createdProjectID, err := mgr.reserveProjectID(name)
	require.NoError(t, err)
	require.NotNil(t, createdProjectID)
	stage, err := newXFSStageName(reservedID, volumeID)
	require.NoError(t, err)
	creationRoot, parent, err := openXFSRootCapabilities(dataPath)
	require.NoError(t, err)
	createdStage, err := createXFSStageDirectory(creationRoot, stage)
	require.NoError(t, err)
	require.NotNil(t, createdStage)
	require.NoError(t, parent.Sync())
	require.NoError(t, mgr.rememberDurableStage(stage))
	require.NoError(t, ensureXFSStageMarker(creationRoot, stage))
	require.NoError(t, creationRoot.Close())
	require.NoError(t, parent.Close())

	t.Setenv("PATH", t.TempDir())
	err = mgr.cleanupXFSStage(t.Context(), stage)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	assert.Equal(t, reservedID, mgr.volumeToID[name], "failed cleanup must retain the reverse reservation")
	assert.Equal(t, name, mgr.activeIDs[reservedID], "failed cleanup must retain the forward reservation")
	assert.DirExists(t, stage.hostPath(dataPath))

	installLoggingXFSQuota(t)
	require.NoError(t, mgr.cleanupXFSStage(t.Context(), stage))
	assert.NotContains(t, mgr.volumeToID, name)
	assert.NotContains(t, mgr.activeIDs, reservedID)
	assert.NoDirExists(t, stage.hostPath(dataPath))
}

func resolveProjectIDForTest(t *testing.T, mgr *xfsVolumeManager, id string) (uint32, bool) {
	t.Helper()
	volumeID, err := parseManagedVolumeName(id)
	require.NoError(t, err)
	root, err := os.OpenRoot(mgr.dataPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })
	return mgr.resolveProjectID(root, volumeID)
}

// TestResolveProjectID_UsesMarkerFile pins the ENG-459 resolution rule: the
// on-disk .fred-project-id marker (the authoritative record of the projID that was
// actually tagged + limited, and the only source that survives a restart) is used
// when no live map contradicts it. Resolving via the marker — never by recomputing crc32(id) — is
// required because assignProjectID's collision-probe can make the derived candidate
// differ from the assigned id, so a recompute-based clear could zero the wrong project.
func TestResolveProjectID_UsesMarkerFile(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)

	const id = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	dir := filepath.Join(dataPath, id)
	require.NoError(t, os.MkdirAll(dir, 0700))
	require.NoError(t, writeProjectIDFile(dir, 4242))

	got, ok := resolveProjectIDForTest(t, mgr, id)
	require.True(t, ok)
	assert.Equal(t, uint32(4242), got, "the on-disk marker is authoritative over the in-memory map")
}

// TestResolveProjectID_FallsBackToMapWhenNoMarker covers a volume whose directory
// (and marker) is already gone but which the live process still tracks in-memory —
// the projID is still resolvable so its quota can be cleared.
func TestResolveProjectID_FallsBackToMapWhenNoMarker(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)

	const id = "fred-550e8400-e29b-41d4-a716-446655440000-app-0" // no directory / marker on disk
	mgr.activeIDs[777] = id
	mgr.volumeToID[id] = 777

	got, ok := resolveProjectIDForTest(t, mgr, id)
	require.True(t, ok)
	assert.Equal(t, uint32(777), got)
}

// TestResolveProjectID_NotFoundWhenNeitherKnows covers the idempotent re-Destroy /
// never-created case: neither the marker nor the map knows the id, so there is no
// projID to clear (the caller skips the clear).
func TestResolveProjectID_NotFoundWhenNeitherKnows(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)

	_, ok := resolveProjectIDForTest(t, mgr, "fred-550e8400-e29b-41d4-a716-446655440000-app-0")
	assert.False(t, ok, "an already-cleared / never-created volume resolves to no projID")
}

// TestResolveProjectID_RejectsZeroMarker guards project ID 0 (XFS's reserved
// default project): a corrupt/hostile marker reading "0" must NOT resolve, so
// Destroy can never reset the default project's limits.
func TestResolveProjectID_RejectsZeroMarker(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)

	const id = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	dir := filepath.Join(dataPath, id)
	require.NoError(t, os.MkdirAll(dir, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, projectIDFile), []byte("0"), 0o600))

	_, ok := resolveProjectIDForTest(t, mgr, id)
	assert.False(t, ok, "project ID 0 (reserved default project) must never resolve")
}

func TestXFSVolumeManagerRejectsDefaultProjectMarkerBeforeMutation(t *testing.T) {
	t.Parallel()

	dataPath := t.TempDir()
	const id = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	dir := filepath.Join(dataPath, id)
	require.NoError(t, os.Mkdir(dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, projectIDFile), []byte("0"), 0o600))
	mgr := newXfsManagerForTest(dataPath)

	_, _, err := mgr.Create(context.Background(), id, 100)
	require.ErrorContains(t, err, "project ID 0 is reserved")
	require.ErrorContains(t, mgr.EnsureQuota(context.Background(), id, 100), "project ID 0 is reserved")
	require.ErrorContains(t, mgr.loadProjectIDs(), "project ID 0 is reserved")
	assert.Empty(t, mgr.volumeToID)
	assert.Empty(t, mgr.activeIDs)
}

func TestXFSVolumeManagerAttestsRealDirectoryAndNonzeroMarker(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	name := canonicalVolumeName("550e8400-e29b-41d4-a716-446655440000", "app", 0)
	dirPath := filepath.Join(rootPath, name)
	require.NoError(t, os.Mkdir(dirPath, 0o700))
	require.NoError(t, writeProjectIDFile(dirPath, 4242))
	managedName, err := parseManagedVolumeName(name)
	require.NoError(t, err)
	mgr := newXfsManagerForTest(rootPath)

	require.NoError(t, mgr.AttestManagedVolume(t.Context(), managedName))
	require.NoError(t, os.WriteFile(filepath.Join(dirPath, projectIDFile), []byte("0"), 0o600))
	require.ErrorContains(t, mgr.AttestManagedVolume(t.Context(), managedName), "project ID 0 is reserved")
}

// TestResolveProjectID_SkipsOnUnreadableMarker verifies that a marker that EXISTS
// but is unreadable/corrupt (not ErrNotExist) makes resolveProjectID skip rather
// than fall back to the in-memory map — the marker is authoritative, so guessing a
// possibly-wrong project when it is present-but-corrupt could clear a foreign one.
func TestResolveProjectID_SkipsOnUnreadableMarker(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)

	const id = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	dir := filepath.Join(dataPath, id)
	require.NoError(t, os.MkdirAll(dir, 0700))
	// A present-but-corrupt marker (unparseable content); the map even knows a projID.
	require.NoError(t, os.WriteFile(filepath.Join(dir, projectIDFile), []byte("not-a-number"), 0600))
	mgr.volumeToID[id] = 555

	_, ok := resolveProjectIDForTest(t, mgr, id)
	assert.False(t, ok, "a present-but-corrupt marker must skip the clear, not fall back to the map")
}

func TestProjectIDMarker_RejectsCrossVolumeSymlink(t *testing.T) {
	t.Parallel()

	dataPath := t.TempDir()
	const sourceName = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	const targetName = "fred-550e8400-e29b-41d4-a716-446655440000-app-1"
	sourceDir := filepath.Join(dataPath, sourceName)
	targetDir := filepath.Join(dataPath, targetName)
	require.NoError(t, os.Mkdir(sourceDir, 0o700))
	require.NoError(t, os.Mkdir(targetDir, 0o700))
	require.NoError(t, writeProjectIDFile(targetDir, 4242))
	require.NoError(t, os.Symlink(filepath.Join("..", targetName, projectIDFile), filepath.Join(sourceDir, projectIDFile)))

	root, err := os.OpenRoot(dataPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = root.Close() })
	source, err := parseManagedVolumeName(sourceName)
	require.NoError(t, err)

	_, err = readProjectIDFileAtRoot(root, source)
	require.Error(t, err)
	assert.ErrorContains(t, err, "not a regular file")
	err = writeProjectIDFileAtRoot(root, source, 9999)
	require.Error(t, err, "exclusive no-follow creation must reject a pre-existing marker symlink")

	got, err := readProjectIDFile(targetDir)
	require.NoError(t, err)
	assert.Equal(t, uint32(4242), got, "a foreign volume's marker must remain untouched")
}

// TestDestroy_QuotaClearFailure_RetainsTypedTombstoneAndCounts pins the durable
// delete protocol's post-content-removal crash window. The tombstone stays
// named until its dquot is definitely clear, so an xfs_quota failure is
// retryable after restart rather than leaking the collision-probed project ID.
func TestDestroy_QuotaClearFailure_RetainsTypedTombstoneAndCounts(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)

	const id = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	dir := filepath.Join(dataPath, id)
	require.NoError(t, os.MkdirAll(dir, 0700))
	require.NoError(t, writeProjectIDFile(dir, 4242))

	// Let both zero-usage proofs succeed, then fail only the dquot clear.
	binDir := t.TempDir()
	fakeQuota := filepath.Join(binDir, "xfs_quota")
	require.NoError(t, os.WriteFile(fakeQuota, []byte(`#!/bin/sh
case "$*" in
  *"bhard=0 bsoft=0 ihard=0 isoft=0"*) exit 19 ;;
esac
`), 0o700))
	t.Setenv("PATH", binDir)

	before := testutil.ToFloat64(volumeQuotaClearFailedTotal)
	err := mgr.Destroy(context.Background(), id)
	require.ErrorContains(t, err, "clear xfs project quota for delete-stage")
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)

	assert.NoDirExists(t, dir, "tenant bytes must no longer remain under the live volume name")
	volumeID, parseErr := parseManagedVolumeName(id)
	require.NoError(t, parseErr)
	stage, stageErr := newXFSDeleteStageName(4242, volumeID)
	require.NoError(t, stageErr)
	assert.DirExists(t, stage.hostPath(dataPath), "failed clear must retain durable delete authority")
	entries, readErr := os.ReadDir(stage.hostPath(dataPath))
	require.NoError(t, readErr)
	assert.Empty(t, entries, "all tenant bytes are gone before quota clear is attempted")
	assert.Equal(t, before+1, testutil.ToFloat64(volumeQuotaClearFailedTotal),
		"a clear failure must be recorded on the leak counter")
	assert.Equal(t, uint32(4242), mgr.volumeToID[id], "the project ID must remain reserved for retry")

	// A crash here loses every in-memory map after the tenant tree is gone but
	// before quota clear. The sibling must independently rebuild authority and
	// make the idempotent clear/removal retry possible.
	restarted := newXfsManagerForTest(dataPath)
	require.NoError(t, restarted.loadProjectIDs())
	installLoggingXFSQuota(t)
	require.NoError(t, restarted.RecoverInterruptedVolumeMutations(t.Context()))
	assert.NoDirExists(t, stage.hostPath(dataPath))
	assert.Empty(t, restarted.volumeToID)
}

func TestXFSDestroyQuotaClearSurvivesCanceledCaller(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	const (
		name   = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
		projID = uint32(4242)
	)
	dirPath := filepath.Join(dataPath, name)
	require.NoError(t, os.Mkdir(dirPath, 0o700))
	require.NoError(t, writeProjectIDFile(dirPath, projID))
	logPath := installLoggingXFSQuota(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, mgr.Destroy(ctx, name))
	assert.NoDirExists(t, dirPath)
	commands, err := os.ReadFile(logPath)
	require.NoError(t, err, "the detached quota clear must execute despite caller cancellation")
	assert.Contains(t, string(commands), xfsLimitClearCmd(projID))
}

func TestXFSRejectsForeignProjectIDAuthorityBeforeQuotaMutation(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	const (
		victimName = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
		ownerName  = "fred-550e8400-e29b-41d4-a716-446655440000-app-1"
		projID     = uint32(4242)
	)
	victimDir := filepath.Join(dataPath, victimName)
	sentinelPath := filepath.Join(victimDir, "tenant-data")
	require.NoError(t, os.Mkdir(victimDir, 0o700))
	require.NoError(t, writeProjectIDFile(victimDir, projID))
	require.NoError(t, os.WriteFile(sentinelPath, []byte("keep"), 0o600))
	mgr.mu.Lock()
	registerErr := mgr.registerProjectIDLocked(ownerName, projID)
	mgr.mu.Unlock()
	require.NoError(t, registerErr)
	logPath := installLoggingXFSQuota(t)

	_, _, err := mgr.Create(t.Context(), victimName, 100)
	require.ErrorContains(t, err, "already registered to volume")
	require.ErrorContains(t, mgr.EnsureQuota(t.Context(), victimName, 100), "already registered to volume")
	require.ErrorContains(t, mgr.Destroy(t.Context(), victimName), "project ID authority conflicts")

	assert.NoFileExists(t, logPath, "foreign project authority must be rejected before xfs_quota")
	assert.FileExists(t, sentinelPath, "conflicting authority must not authorize recursive deletion")
	assert.Equal(t, ownerName, mgr.activeIDs[projID])
	assert.Equal(t, projID, mgr.volumeToID[ownerName])
	assert.NotContains(t, mgr.volumeToID, victimName)
}

func TestDestroy_RejectsTraversalBeforeFilesystemAccess(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	dataPath := filepath.Join(parent, "volumes")
	require.NoError(t, os.Mkdir(dataPath, 0o700))
	victimDir := filepath.Join(parent, "victim")
	require.NoError(t, os.Mkdir(victimDir, 0o700))
	victim := filepath.Join(victimDir, "tenant-data")
	require.NoError(t, os.WriteFile(victim, []byte("keep"), 0o600))

	mgr := newXfsManagerForTest(dataPath)
	err := mgr.Destroy(context.Background(), filepath.Join("..", filepath.Base(victimDir)))
	require.Error(t, err)
	assert.ErrorContains(t, err, "validate xfs volume ID for destroy")
	assert.FileExists(t, victim, "an invalid volume ID must not reach recursive deletion")
}

// TestDestroy_RemoveAllFailure_KeepsQuotaAndReturnsError pins the ordering
// invariant: the quota limit is cleared only AFTER every child of the durable
// delete-stage is removed. A partial failure retains the tombstone and project
// map so a retry/restart never depends on the marker surviving.
func TestDestroy_RemoveAllFailure_KeepsQuotaAndReturnsError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root bypasses directory permissions, so recursive removal would not fail")
	}
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)

	const id = "fred-550e8400-e29b-41d4-a716-446655440000-app-0"
	const projID = uint32(4242)
	dir := filepath.Join(dataPath, id)
	require.NoError(t, os.MkdirAll(dir, 0700))
	require.NoError(t, writeProjectIDFile(dir, projID))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.bin"), []byte("x"), 0600))
	// Track the projID in-memory too, so we can assert the mapping survives a failed
	// removal (a retry must still resolve it; the projID must not be reallocated).
	mgr.activeIDs[projID] = id
	mgr.volumeToID[id] = projID
	// Make the volume dir non-writable so its contents cannot be unlinked -> RemoveAll fails.
	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { _ = os.Chmod(dir, 0700) }) // let t.TempDir cleanup remove the tree

	logPath := installLoggingXFSQuota(t)
	before := testutil.ToFloat64(volumeQuotaClearFailedTotal)

	err := mgr.Destroy(context.Background(), id)
	require.Error(t, err, "a RemoveAll failure must surface (bytes still on disk -> caller retries)")
	require.ErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	assert.DirExists(t, dir, "a partial recursive removal retains the managed root for an exact retry")
	volumeID, parseErr := parseManagedVolumeName(id)
	require.NoError(t, parseErr)
	deleteStage, stageErr := newXFSDeleteStageName(projID, volumeID)
	require.NoError(t, stageErr)
	assert.DirExists(t, deleteStage.hostPath(dataPath), "a partial removal must retain its typed tombstone")
	assert.Equal(t, before, testutil.ToFloat64(volumeQuotaClearFailedTotal),
		"the quota limit must be left intact (clear not attempted) while the volume survives")
	commands, readErr := os.ReadFile(logPath)
	require.NoError(t, readErr)
	assert.NotContains(t, string(commands), xfsLimitClearCmd(projID),
		"partial recursive deletion must not reach dquot clear")

	// The in-memory mapping must survive the failed removal: a retry must still
	// resolve the projID, and it must not be reallocated while the inodes exist.
	mgr.mu.Lock()
	mappedID, stillMapped := mgr.volumeToID[id]
	_, stillReserved := mgr.activeIDs[projID]
	mgr.mu.Unlock()
	assert.True(t, stillMapped, "the volumeToID entry must survive a failed removal (retry-resolvable)")
	assert.Equal(t, projID, mappedID)
	assert.True(t, stillReserved, "the projID must stay reserved in activeIDs (no premature reallocation)")
}

func TestInodeHardLimit(t *testing.T) {
	cases := []struct {
		name   string
		sizeMB int64
		minAvg int64
		want   int64
	}{
		{"small SKU, default ratio", 30720, 1024, 31457280},
		{"nano SKU, default ratio", 15360, 1024, 15728640},
		{"xlarge SKU, no overflow", 245760, 1024, 251658240},
		{"minAvg 0 uses default 1024", 30720, 0, 31457280},
		{"negative minAvg uses default", 30720, -5, 31457280},
		{"minAvg 2048 halves the ceiling", 30720, 2048, 15728640},
		{"ephemeral 64MB floored", 64, 1024, 262144}, // raw 65536 < floor
		{"sizeMB 0 floored", 0, 1024, 262144},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, inodeHardLimit(tc.sizeMB, tc.minAvg))
		})
	}
}

func TestXfsLimitCmd_IncludesIhard(t *testing.T) {
	assert.Equal(t, "limit -p bhard=30720m ihard=31457280 42", xfsLimitCmd(42, "30720m", 31457280))
}

func TestXfsLimitClearCmd_ClearsIhard(t *testing.T) {
	assert.Equal(t, "limit -p bhard=0 bsoft=0 ihard=0 isoft=0 42", xfsLimitClearCmd(42))
}
