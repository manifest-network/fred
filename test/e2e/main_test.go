//go:build integration

package e2e

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestMain gives the run one fixed scratch root and first removes what a
// killed earlier run left there and on the Docker daemon. A killed run never
// reaches its cleanups, so its containers keep running and its loopbacks stay
// mounted. The sweep is why only one e2e test binary may run per daemon.
func TestMain(m *testing.M) {
	if os.Getuid() == 0 {
		root := filepath.Join(os.TempDir(), "fred-e2e")
		if err := sweepLeftovers(root); err != nil {
			fmt.Fprintln(os.Stderr, "e2e: sweep leftovers:", err)
			os.Exit(1)
		}
		// Every t.TempDir, and so every loopback mount, now lands under root.
		if err := os.Setenv("TMPDIR", root); err != nil {
			fmt.Fprintln(os.Stderr, "e2e: set TMPDIR:", err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}

// sweepLeftovers removes this suite's containers and networks, unmounts every
// filesystem under root, and empties root.
func sweepLeftovers(root string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// Containers first: they hold the bind mounts into the loopbacks. An
	// unreachable daemon is not an error here; each test skips on it.
	if docker, err := newDockerClient(); err == nil {
		defer func() { _ = docker.Close() }()
		if _, err := docker.Ping(ctx); err == nil {
			if err := sweepDockerResources(ctx, docker, ownedByThisSuite); err != nil {
				return fmt.Errorf("remove Docker resources: %w", err)
			}
		}
	}
	mounts, err := mountsUnder(root)
	if err != nil {
		return err
	}
	for _, mount := range mounts {
		if out, err := exec.CommandContext(ctx, "umount", "-l", mount).CombinedOutput(); err != nil {
			return fmt.Errorf("umount %s: %w: %s", mount, err, out)
		}
	}
	if err := os.RemoveAll(root); err != nil {
		return err
	}
	return os.MkdirAll(root, 0o755)
}

// mountsUnder lists the mount points below root, deepest first.
func mountsUnder(root string) ([]string, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	var mounts []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// Field 5 is the mount point; the kernel escapes spaces in it, and
		// the suite's paths have none.
		fields := strings.Fields(scanner.Text())
		if len(fields) > 4 && strings.HasPrefix(fields[4], root+string(os.PathSeparator)) {
			mounts = append(mounts, fields[4])
		}
	}
	slices.SortFunc(mounts, func(a, b string) int { return len(b) - len(a) })
	return mounts, scanner.Err()
}
