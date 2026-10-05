//go:build linux

package fstree

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const mountChildEnv = "FSTREE_TEST_MOUNT_CHILD"

// mountCases names every case runMountCases runs, as go test prints them.
// Each holds on every kernel: where mount IDs are unavailable, the
// same-filesystem bind mount cases are refused as ErrCrossDevice all the
// same.
var mountCases = []string{
	"removal_stops_at_a_mount_point_inside_the_tree",
	"removal_stops_at_a_file_bind_mount_inside_the_tree",
	"removal_refuses_an_entry_that_is_a_mount_point",
	"removal_refuses_an_entry_that_is_a_same-filesystem_bind_mount",
	"walk_refuses_a_mount_point_inside_the_tree",
	"walk_refuses_a_same-filesystem_bind_mount_inside_the_tree",
	"walk_refuses_an_entry_that_is_a_mount_point",
}

// runMountChild runs the test named test again in a child process, with
// mountChildEnv set and the namespaces attr asks for, and returns the
// child's combined output.
func runMountChild(t *testing.T, test string, attr *syscall.SysProcAttr) ([]byte, error) {
	t.Helper()
	executable, err := os.Executable()
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), executable, "-test.run=^"+test+"$", "-test.v")
	command.Env = append(os.Environ(), mountChildEnv+"=1")
	command.SysProcAttr = attr
	return command.CombinedOutput()
}

// requireEveryMountCasePassed requires the child's output to show test and
// every one of its cases passing.
func requireEveryMountCasePassed(t *testing.T, test string, output []byte) {
	t.Helper()
	require.Contains(t, string(output), "--- PASS: "+test+" (")
	for _, name := range mountCases {
		require.Contains(t, string(output), "--- PASS: "+test+"/"+name+" (")
	}
}

// Neither traversal crosses a mount (I3). Mounting needs privilege, so the
// cases run in a child process inside a new user and mount namespace; the
// test is skipped where unprivileged namespaces are not permitted.
// TestIntegration_FstreeMountBoundaries runs the same cases as root, without
// a user namespace, where a skip fails CI.
func TestTraversalsNeverCrossAMount(t *testing.T) {
	const test = "TestTraversalsNeverCrossAMount"
	if os.Getenv(mountChildEnv) == "1" {
		runMountCases(t)
		return
	}
	output, err := runMountChild(t, test, &syscall.SysProcAttr{
		Cloneflags:  syscall.CLONE_NEWUSER | syscall.CLONE_NEWNS,
		UidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}},
		GidMappings: []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}},
	})
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		t.Skipf("cannot start a process in new user and mount namespaces: %v", err)
	}
	if bytes.Contains(output, []byte("--- SKIP: "+test+" (")) {
		t.Skipf("mounting inside a user namespace is not permitted here:\n%s", output)
	}
	require.NoError(t, err, "%s", output)
	t.Logf("child process:\n%s", output)
	requireEveryMountCasePassed(t, test, output)
}

func runMountCases(t *testing.T) {
	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		t.Skipf("cannot make this namespace's mounts private: %v", err)
	}
	probe := tempDir(t)
	if err := unix.Mount("fstree-probe", probe, "tmpfs", 0, "size=64k"); err != nil {
		t.Skipf("cannot mount tmpfs inside a user namespace: %v", err)
	}
	require.NoError(t, unix.Unmount(probe, 0))
	ctx := context.Background()

	// A mount point inside the tree cannot be removed, so the removal stops
	// there and the mounted filesystem is untouched.
	t.Run("removal stops at a mount point inside the tree", func(t *testing.T) {
		parentPath := tempDir(t)
		anchor := filepath.Join(parentPath, "anchor")
		mkdirAll(t, filepath.Join(anchor, "mounted"))
		writeFile(t, filepath.Join(anchor, "file"), "x")
		mountTmpfs(t, filepath.Join(anchor, "mounted"))
		writeFile(t, filepath.Join(anchor, "mounted", "sentinel"), "kept")
		parent := openDir(t, parentPath)

		requireNoLeak(t, func() {
			_, err := removeBeneath(ctx, parent, mustName("anchor"), RemoveOptions{}, maxDepth)
			require.ErrorIs(t, err, ErrUndeletable)
			require.ErrorIs(t, err, unix.EBUSY)
		})
		requireContent(t, filepath.Join(anchor, "mounted", "sentinel"), "kept")
	})

	t.Run("removal stops at a file bind mount inside the tree", func(t *testing.T) {
		base := tempDir(t)
		outside := makeOutside(t, base)
		anchor := filepath.Join(base, "parent", "anchor")
		mkdirAll(t, anchor)
		writeFile(t, filepath.Join(anchor, "target"), "")
		bindMount(t, filepath.Join(outside, "file"), filepath.Join(anchor, "target"))
		parent := openDir(t, filepath.Join(base, "parent"))

		requireNoLeak(t, func() {
			_, err := removeBeneath(ctx, parent, mustName("anchor"), RemoveOptions{}, maxDepth)
			require.ErrorIs(t, err, ErrUndeletable)
			require.ErrorIs(t, err, unix.EBUSY)
		})
		requireContent(t, filepath.Join(outside, "file"), "sentinel")
	})

	// The parent-versus-anchor check runs before anything in the anchor is
	// touched: even a tree deep enough to need a cut is left whole, and
	// BeforeFirstCut never sees the mounted anchor.
	t.Run("removal refuses an entry that is a mount point", func(t *testing.T) {
		parentPath := tempDir(t)
		anchor := filepath.Join(parentPath, "anchor")
		mkdirAll(t, anchor)
		mountTmpfs(t, anchor)
		writeFile(t, filepath.Join(anchor, "sentinel"), "kept")
		mkdirAll(t, filepath.Join(anchor, "a", "b", "c"))
		writeFile(t, filepath.Join(anchor, "a", "b", "c", "deep"), "kept")
		before := snapshot(t, anchor)
		parent := openDir(t, parentPath)

		requireNoLeak(t, func() {
			report, err := removeBeneath(ctx, parent, mustName("anchor"), RemoveOptions{
				BeforeFirstCut: func(BorrowedDir) error { t.Error("BeforeFirstCut ran on a mounted anchor"); return nil },
			}, 1)
			require.ErrorIs(t, err, ErrCrossDevice)
			require.Equal(t, RemoveReport{}, report, "nothing is removed")
		})
		require.Equal(t, before, snapshot(t, anchor))
	})

	// A bind mount of the same filesystem keeps st_dev; only the mount ID
	// tells it apart, and without one the entry is refused all the same.
	t.Run("removal refuses an entry that is a same-filesystem bind mount", func(t *testing.T) {
		base := tempDir(t)
		outside := makeOutside(t, base)
		before := snapshot(t, outside)
		anchor := filepath.Join(base, "parent", "anchor")
		mkdirAll(t, anchor)
		bindMount(t, filepath.Join(outside, "dir"), anchor)
		parent := openDir(t, filepath.Join(base, "parent"))

		requireNoLeak(t, func() {
			_, err := removeBeneath(ctx, parent, mustName("anchor"), RemoveOptions{}, maxDepth)
			require.ErrorIs(t, err, ErrCrossDevice)
		})
		require.Equal(t, before, snapshot(t, outside))
	})

	t.Run("walk refuses a mount point inside the tree", func(t *testing.T) {
		parentPath := tempDir(t)
		anchor := filepath.Join(parentPath, "anchor")
		mkdirAll(t, filepath.Join(anchor, "mounted"))
		mountTmpfs(t, filepath.Join(anchor, "mounted"))
		writeFile(t, filepath.Join(anchor, "mounted", "sentinel"), "kept")
		parent := openDir(t, parentPath)

		requireNoLeak(t, func() {
			_, err := walkBeneath(ctx, parent, mustName("anchor"), newRecorder(t, anchor), maxDepth)
			require.ErrorIs(t, err, ErrCrossDevice)
		})
	})

	t.Run("walk refuses a same-filesystem bind mount inside the tree", func(t *testing.T) {
		base := tempDir(t)
		parentPath := filepath.Join(base, "parent")
		anchor := filepath.Join(parentPath, "anchor")
		mkdirAll(t, filepath.Join(anchor, "loop"))
		// A bind of the tree's own parent would make the walk circle.
		bindMount(t, parentPath, filepath.Join(anchor, "loop"))
		parent := openDir(t, parentPath)

		requireNoLeak(t, func() {
			_, err := walkBeneath(ctx, parent, mustName("anchor"), newRecorder(t, anchor), maxDepth)
			require.ErrorIs(t, err, ErrCrossDevice)
		})
	})

	t.Run("walk refuses an entry that is a mount point", func(t *testing.T) {
		parentPath := tempDir(t)
		anchor := filepath.Join(parentPath, "anchor")
		mkdirAll(t, anchor)
		mountTmpfs(t, anchor)
		parent := openDir(t, parentPath)

		requireNoLeak(t, func() {
			_, err := walkBeneath(ctx, parent, mustName("anchor"), newRecorder(t, anchor), maxDepth)
			require.ErrorIs(t, err, ErrCrossDevice)
		})
	})
}

func mountTmpfs(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, unix.Mount("fstree-test", dir, "tmpfs", 0, "size=1m"))
	t.Cleanup(func() { _ = unix.Unmount(dir, unix.MNT_DETACH) })
}

func bindMount(t *testing.T, source, target string) {
	t.Helper()
	require.NoError(t, unix.Mount(source, target, "", unix.MS_BIND, ""))
	t.Cleanup(func() { _ = unix.Unmount(target, unix.MNT_DETACH) })
}
