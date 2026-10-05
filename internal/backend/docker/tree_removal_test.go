package docker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
	"github.com/manifest-network/fred/internal/fstree"
)

func TestClassifyTreeRemovalIsTotal(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		err     error
		class   treeRemovalClass
		outcome string
	}{
		{"removed", nil, treeRemovalRemoved, treeRemovalOutcomeRemoved},
		{"deadline", fmt.Errorf("walk: %w", context.DeadlineExceeded), treeRemovalDeadline, treeRemovalOutcomeCanceled},
		{"canceled", fmt.Errorf("walk: %w", context.Canceled), treeRemovalCanceled, treeRemovalOutcomeCanceled},
		{"cross device", fmt.Errorf("x: %w", fstree.ErrCrossDevice), treeRemovalCrossDevice, treeRemovalOutcomeCrossDevice},
		{
			// fstree reports a failed cut-anchor detach as a refused cut and
			// keeps the hook's own error, even a cross-device refusal, behind
			// Cause() instead of errors.Is. It is a cut-refused hold.
			"cut refused by a cross-device detach",
			fmt.Errorf("remove: %w", beforeFirstCutError{cause: fmt.Errorf("x: %w", fstree.ErrCrossDevice)}),
			treeRemovalCutRefused, treeRemovalOutcomeCutRefused,
		},
		{"cut refused", fmt.Errorf("x: %w: %w", fstree.ErrCutRefused, unix.EXDEV), treeRemovalCutRefused, treeRemovalOutcomeCutRefused},
		{"tree changed", fstree.ErrTreeChanged, treeRemovalTreeChanged, treeRemovalOutcomeTreeChanged},
		{"undeletable", fmt.Errorf("x: %w: %w", fstree.ErrUndeletable, unix.EPERM), treeRemovalUndeletable, treeRemovalOutcomeUndeletable},
		{"descriptor exhaustion", fmt.Errorf("x: %w", unix.EMFILE), treeRemovalFailed, treeRemovalOutcomeError},
		{"unknown", errors.New("never seen before"), treeRemovalFailed, treeRemovalOutcomeError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			class := classifyTreeRemoval(tc.err)
			assert.Equal(t, tc.class, class)
			assert.Equal(t, tc.outcome, class.metricOutcome())
		})
	}
}

// beforeFirstCutError models the error fstree.RemoveBeneath returns when its
// BeforeFirstCut hook fails: it unwraps only to fstree.ErrCutRefused, and the
// hook's error is reachable through Cause(), never through errors.Is.
type beforeFirstCutError struct{ cause error }

func (e beforeFirstCutError) Error() string {
	return fstree.ErrCutRefused.Error() + ": before the first cut: " + e.cause.Error()
}

func (beforeFirstCutError) Unwrap() error { return fstree.ErrCutRefused }

func (e beforeFirstCutError) Cause() error { return e.cause }

// Start's kernel probe passes on a kernel that reports mount IDs (this one),
// and an unreadable root is an error, never a pass; fstree's own tests pin
// that a kernel without mount IDs is ErrUnsupportedKernel.
func TestRequireTreeRemovalSupport(t *testing.T) {
	t.Parallel()

	require.NoError(t, requireTreeRemovalSupport(t.TempDir()))
	err := requireTreeRemovalSupport(filepath.Join(t.TempDir(), "missing"))
	require.ErrorIs(t, err, os.ErrNotExist)
	assert.NotErrorIs(t, err, fstree.ErrUnsupportedKernel)
}

func TestTreeRemovalMetricsArePreinitialized(t *testing.T) {
	t.Parallel()

	assert.Equal(t, len(treeRemovalSites)*len(treeRemovalOutcomes), testutil.CollectAndCount(treeRemovalsTotal),
		"every site/outcome pair must export a series before the first removal")
	assert.Equal(t, len(treeRemovalSites), testutil.CollectAndCount(treeRemovalCutsTotal))
}

func TestRemoveManagedVolumeSubtreeCountsRemovals(t *testing.T) {
	volumeRoot := t.TempDir()
	volumeName, err := parseManagedVolumeName("fred-550e8400-e29b-41d4-a716-446655440000-app-0")
	require.NoError(t, err)
	wpName, err := parseStoragePathComponent(writablePathSubdir)
	require.NoError(t, err)
	wpPath := filepath.Join(volumeName.hostPath(volumeRoot), writablePathSubdir)
	require.NoError(t, os.MkdirAll(filepath.Join(wpPath, "a", "b"), 0o700))

	removed := treeRemovalsTotal.WithLabelValues(treeRemovalSiteWritablePath, treeRemovalOutcomeRemoved)
	before := testutil.ToFloat64(removed)
	require.NoError(t, removeManagedVolumeSubtree(t.Context(), volumeRoot, volumeName, wpName))
	assert.NoDirExists(t, wpPath)
	assert.Equal(t, before+1, testutil.ToFloat64(removed))
}

const deepWritablePathChildEnv = "FRED_DOCKER_TEST_DEEP_WP_CHILD"

// The writable-path wipe runs on every launch of a lease with writable paths.
// Go's RemoveAll holds one descriptor per level, so a tenant chain deeper than
// RLIMIT_NOFILE used to fail the wipe on every relaunch, and the failed Step
// made the whole launch session ambiguous. The wipe now succeeds at any depth
// with constant descriptors, so the Step records no issue. The chain is built
// and removed in a child process whose soft RLIMIT_NOFILE is 128.
func TestRemoveManagedVolumeSubtreeRemovesChainDeeperThanFDLimit(t *testing.T) {
	if os.Getenv(deepWritablePathChildEnv) == "1" {
		removeDeepWritablePathUnderLowFDLimit(t)
		return
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), executable,
		"-test.run=^TestRemoveManagedVolumeSubtreeRemovesChainDeeperThanFDLimit$", "-test.v")
	command.Env = append(os.Environ(), deepWritablePathChildEnv+"=1")
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.Contains(t, string(output), "--- PASS: TestRemoveManagedVolumeSubtreeRemovesChainDeeperThanFDLimit (")
}

func removeDeepWritablePathUnderLowFDLimit(t *testing.T) {
	const depth = 4096
	volumeRoot := t.TempDir()
	volumeName, err := parseManagedVolumeName("fred-550e8400-e29b-41d4-a716-446655440000-app-0")
	require.NoError(t, err)
	wpName, err := parseStoragePathComponent(writablePathSubdir)
	require.NoError(t, err)
	volumePath := volumeName.hostPath(volumeRoot)
	require.NoError(t, os.Mkdir(volumePath, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(volumePath, "keep"), []byte("x"), 0o600))
	buildDirectoryChain(t, volumePath, writablePathSubdir, depth)
	controlPath := t.TempDir()
	buildDirectoryChain(t, controlPath, "control", depth)

	var limit unix.Rlimit
	require.NoError(t, unix.Getrlimit(unix.RLIMIT_NOFILE, &limit))
	require.NoError(t, unix.Setrlimit(unix.RLIMIT_NOFILE, &unix.Rlimit{Cur: 128, Max: limit.Max}))
	t.Cleanup(func() { _ = unix.Setrlimit(unix.RLIMIT_NOFILE, &limit) })

	controlRoot, err := os.OpenRoot(controlPath)
	require.NoError(t, err)
	// Control: the unbounded remover this change replaced.
	controlErr := controlRoot.RemoveAll("control")
	require.NoError(t, controlRoot.Close())
	require.ErrorIs(t, controlErr, syscall.EMFILE, "control: Go's RemoveAll needs one descriptor per level")

	result := substratemutation.RunStep(t.Context(), "remove managed writable path",
		func(ctx context.Context, _ string) (context.Context, func(), error) { return ctx, func() {}, nil },
		func(context.Context, string, error) error { return nil },
		func(ctx context.Context) error {
			return removeManagedVolumeSubtree(ctx, volumeRoot, volumeName, wpName)
		},
	)
	require.NoError(t, result.Err())
	assert.Equal(t, substratemutation.Attested, result.Kind(), "the wipe Step must record no issue")
	assert.NoDirExists(t, filepath.Join(volumePath, writablePathSubdir))
	assert.FileExists(t, filepath.Join(volumePath, "keep"), "only the writable-path subtree is removed")
}

// buildDirectoryChain creates name inside parentPath with depth nested "d"
// directories below it, using only mkdirat and openat so PATH_MAX does not
// limit the depth.
func buildDirectoryChain(t *testing.T, parentPath, name string, depth int) {
	t.Helper()
	const flags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC
	parent, err := os.Open(parentPath)
	require.NoError(t, err)
	defer func() { _ = parent.Close() }()
	require.NoError(t, unix.Mkdirat(int(parent.Fd()), name, 0o755))
	fd, err := unix.Openat(int(parent.Fd()), name, flags, 0)
	require.NoError(t, err)
	for range depth {
		require.NoError(t, unix.Mkdirat(fd, "d", 0o755))
		next, err := unix.Openat(fd, "d", flags, 0)
		require.NoError(t, err)
		require.NoError(t, unix.Close(fd))
		fd = next
	}
	require.NoError(t, unix.Close(fd))
}

// The cut-anchor detach is reachable only from a condemned volume, acts only
// on an anchor fstree has lent, and passes the anchor descriptor together
// with the device of the volume root the deletion attested.
func TestCondemnedXFSVolumeCutHookDetachesWithAttestedDevice(t *testing.T) {
	t.Parallel()

	var gotFD int
	var gotDevice uint64
	calls := 0
	volume := condemnedXFSVolume{
		device: 0xfeed,
		attributes: xfsProjectAttributeFuncs{detach: func(anchorFD int, device uint64) error {
			calls++
			gotFD, gotDevice = anchorFD, device
			return nil
		}},
	}
	options := volume.removeOptions()
	require.NotNil(t, options.BeforeFirstCut)
	// Only fstree can lend an anchor. A directory that is not on loan (here
	// the zero BorrowedDir) is refused before the detach runs.
	require.Error(t, options.BeforeFirstCut(fstree.BorrowedDir{}))
	assert.Zero(t, calls, "an anchor that is not on loan must never be detached")

	require.NoError(t, volume.detachAnchor(42))
	assert.Equal(t, 1, calls)
	assert.Equal(t, 42, gotFD)
	assert.Equal(t, uint64(0xfeed), gotDevice)

	injected := errors.New("injected detach failure")
	volume.attributes = xfsProjectAttributeFuncs{detach: func(int, uint64) error { return injected }}
	require.ErrorIs(t, volume.detachAnchor(42), injected)
}

// The production detach refuses any anchor that is not on the attested
// volume's device, or not on XFS, before it reads or writes an attribute.
func TestDetachCondemnedAnchorRefusesForeignDeviceAndNonXFS(t *testing.T) {
	t.Parallel()

	dir, err := os.Open(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dir.Close() })
	var stat unix.Stat_t
	require.NoError(t, unix.Fstat(int(dir.Fd()), &stat))

	err = linuxXFSProjectAttributes{}.DetachCondemnedAnchor(int(dir.Fd()), stat.Dev+1)
	require.ErrorIs(t, err, fstree.ErrCrossDevice, "an anchor on another device must never be detached")

	var filesystem unix.Statfs_t
	require.NoError(t, unix.Fstatfs(int(dir.Fd()), &filesystem))
	if uint64(filesystem.Type) == linuxXFSFilesystemMagic {
		t.Skip("the temporary directory is on XFS; the non-XFS refusal cannot be observed here")
	}
	err = linuxXFSProjectAttributes{}.DetachCondemnedAnchor(int(dir.Fd()), stat.Dev)
	require.ErrorIs(t, err, fstree.ErrCrossDevice, "an anchor off XFS must never be detached")
}
