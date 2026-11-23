package testutil

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// SetupXFSLoopback creates an XFS filesystem on a loopback file, mounted with
// the pquota option required for project-quota enforcement. Returns the mount
// path. Cleanup (umount) is registered via t.Cleanup.
// Skips if root, mkfs.xfs, xfs_quota, or a usable loop device are absent.
//
// 512M: modern xfsprogs refuses to mkfs an XFS filesystem smaller than 300MB
// ("Filesystem must be larger than 300MB"). The per-volume project quotas
// (tens of MiB) are independent of this backing-image size.
func SetupXFSLoopback(t testing.TB) string {
	t.Helper()
	return SetupXFSLoopbackOfSize(t, "512M")
}

// SetupXFSLoopbackOfSize is SetupXFSLoopback with a backing image of the
// given size, as truncate(1) takes it. The image is sparse: a larger one
// costs no more disk, and holds more inodes.
func SetupXFSLoopbackOfSize(t testing.TB, size string) string {
	t.Helper()

	if os.Getuid() != 0 {
		t.Skip("xfs loopback requires root")
	}
	if _, err := exec.LookPath("mkfs.xfs"); err != nil {
		t.Skip("mkfs.xfs not found:", err)
	}
	if _, err := exec.LookPath("xfs_quota"); err != nil {
		t.Skip("xfs_quota not found:", err)
	}

	tmpDir := t.TempDir()
	imgFile := filepath.Join(tmpDir, "xfs.img")
	mountDir := filepath.Join(tmpDir, "mnt")
	if err := os.MkdirAll(mountDir, 0o755); err != nil { //nolint:gosec // G301: a test mount point, traversed by the non-root processes under test
		t.Fatalf("create mount point: %v", err)
	}

	if out, err := exec.Command("truncate", "-s", size, imgFile).CombinedOutput(); err != nil { //nolint:gosec // G204: constant binary, a size the test names, path under t.TempDir
		t.Fatalf("truncate: %v: %s", err, out)
	}
	if out, err := exec.Command("mkfs.xfs", "-f", imgFile).CombinedOutput(); err != nil { //nolint:gosec // G204: constant binary, path under t.TempDir
		t.Fatalf("mkfs.xfs: %v: %s", err, out)
	}

	// pquota enables project-quota enforcement. Loop-device support is a
	// prerequisite just like root and xfsprogs above, but it can't be probed
	// reliably ahead of time. Some environments (containers, restricted/hardened
	// hosts, many CI runners) have no usable loop devices even as root, where
	// `mount -o loop` fails with a "failed to set up loop device" error. Skip
	// there rather than fail — only a genuine (non-loop) mount error should
	// fail the test.
	if out, err := exec.Command("mount", "-o", "loop,pquota", imgFile, mountDir).CombinedOutput(); err != nil { //nolint:gosec // G204: constant binary, paths under t.TempDir
		if strings.Contains(string(out), "set up loop device") {
			t.Skipf("xfs loopback unavailable (loop device setup failed): %s", out)
		}
		t.Fatalf("mount: %v: %s", err, out)
	}

	t.Cleanup(func() {
		_ = exec.Command("umount", "-l", mountDir).Run() //nolint:gosec // G204: constant binary, path under t.TempDir
	})

	return mountDir
}
