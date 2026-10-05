//go:build integration && linux

package fstree

import (
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIntegration_FstreeMountBoundaries pins, against the real kernel, the
// beliefs behind I3: unlink and rmdir of a mount point fail with EBUSY, a
// bind mount of the same filesystem is told apart from the directory it
// covers, and BeforeFirstCut never runs on a mounted anchor. It runs the
// cases of TestTraversalsNeverCrossAMount as root in a new mount namespace,
// with no user namespace, so nothing a distribution does to restrict user
// namespaces can make them skip. Every case must pass: a skip in the child
// fails this test, and a skip of this test fails the integration job.
func TestIntegration_FstreeMountBoundaries(t *testing.T) {
	const test = "TestIntegration_FstreeMountBoundaries"
	if os.Getenv(mountChildEnv) == "1" {
		runMountCases(t)
		return
	}
	if os.Geteuid() != 0 {
		t.Skip("mounting needs root: run with sudo -E env \"PATH=$PATH\" make test-integration")
	}
	output, err := runMountChild(t, test, &syscall.SysProcAttr{Cloneflags: syscall.CLONE_NEWNS})
	require.NoError(t, err, "%s", output)
	t.Logf("child process:\n%s", output)
	require.NotContains(t, string(output), "--- SKIP:", "as root, every case must run")
	requireEveryMountCasePassed(t, test, output)
}
