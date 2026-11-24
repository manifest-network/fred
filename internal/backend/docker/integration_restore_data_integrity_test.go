//go:build integration

package docker

// ENG-365 — restore data-integrity hardening.
//
// What a restore keeps of a volume (exact bytes, nested files, modes, each
// instance's own data) is asserted by the black-box suite, in
// test/e2e/restore_test.go. This file keeps the one test of that work that
// needs a hand-built volume:
//
//   - OwnershipBoundary  : characterization of the non-recursive chown. Restore
//                          re-owns ONLY the top VOLUME dir to the freshly-resolved
//                          image owner; a nested subtree keeps its prior owner.
//
// The OwnershipBoundary test documents a deliberate contract, not a bug we fix
// here. Cross-platform prior art (verified against primary sources) says
// re-owning restored persistent data to a (possibly drifted) workload user is the
// TENANT's responsibility, not the platform's:
//   - AWS EBS / GCP PD / Fly.io restore/re-attach preserve on-disk uid/gid
//     verbatim and never normalize to a new user (block/fs-level restore).
//   - Docker never re-chowns reused volume data: copyExistingContents returns
//     early once the destination is non-empty; bind mounts get no chown at all.
//   - The sole counterexample is Kubernetes securityContext.fsGroup, and even
//     that is opt-in; its modern mechanism (fsGroupChangePolicy=OnRootMismatch,
//     GA in 1.23) is a *bounded* recursive chown — it re-owns only when the
//     volume root's owner/perms mismatch, otherwise skips the walk.
//
// fred drops CAP_CHOWN by design and runs each container AS the image's detected
// VOLUME owner, so the platform contract is "the image's VOLUME owner is stable
// across a restore" (pin an immutable tag/digest). A uid drift between close and
// restore is only reachable under a mutable tag whose rebuilt image changed its
// VOLUME-dir owner. If fred ever decides to own this, buildStatefulVolumeBinds is
// the place to add a Kubernetes-style OnRootMismatch bounded recursive chown —
// but gated on a SUBTREE / persisted-prior-owner sentinel (NOT the root dir,
// which fred already overwrites first), so it does not regress restore latency
// (ENG-357) on the common no-drift path.

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ownerUID returns the on-disk owning uid of path (no symlink follow).
func ownerUID(t *testing.T, path string) uint32 {
	t.Helper()
	fi, err := os.Lstat(path)
	require.NoError(t, err)
	st, ok := fi.Sys().(*syscall.Stat_t)
	require.True(t, ok, "expected *syscall.Stat_t for %s", path)
	return st.Uid
}

// inodeNumber returns path's inode number (no symlink follow).
func inodeNumber(t *testing.T, path string) uint64 {
	t.Helper()
	fi, err := os.Lstat(path)
	require.NoError(t, err)
	st, ok := fi.Sys().(*syscall.Stat_t)
	require.True(t, ok, "expected *syscall.Stat_t for %s", path)
	return st.Ino
}

// TestIntegration_Docker_RetainRestore_OwnershipBoundary characterizes the
// non-recursive chown that restore re-applies. buildStatefulVolumeBinds (run on
// restore via doReplaceContainers→setupVolBinds with the freshly-resolved image
// owner) chowns ONLY the top VOLUME directory; a nested subtree keeps whatever
// owner it had on disk. So if the image's effective user DRIFTS between close and
// restore, the restored subtree stays owned by the prior uid and the container
// (running as the new uid) can hit EACCES.
//
// This is a deliberate contract, not a bug fixed here: re-owning drifted data is
// the tenant's responsibility (pin an immutable tag/digest), matching EBS/PD/
// Fly.io (preserve-as-is) and Docker (never re-chowns reused data); Kubernetes
// fsGroup is the lone, opt-in counterexample. The assertions below FAIL loudly if
// someone later makes the chown recursive — forcing that change to be a conscious
// contract update (and, per the file header, an OnRootMismatch-style bounded one).
func TestIntegration_Docker_RetainRestore_OwnershipBoundary(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root to chown to arbitrary uids")
	}

	const (
		priorUID, priorGID = 5000, 5000 // owner the prior lease's data was written as
		driftUID, driftGID = 6000, 6000 // freshly-resolved owner after an image-user drift
	)

	hostPath := t.TempDir()
	dataDir := filepath.Join(hostPath, "data")
	nestedDir := filepath.Join(dataDir, "nested")
	nestedFile := filepath.Join(nestedDir, "tenant.db")

	require.NoError(t, os.MkdirAll(nestedDir, 0o700))
	require.NoError(t, os.WriteFile(nestedFile, []byte("tenant-data"), 0o600))
	// Stamp the whole simulated retained tree as the prior owner (chown children
	// before parents is irrelevant for plain chown).
	for _, p := range []string{nestedFile, nestedDir, dataDir} {
		require.NoError(t, os.Chown(p, priorUID, priorGID))
	}
	require.Equal(t, uint32(priorUID), ownerUID(t, nestedFile), "precondition: nested file owned by prior uid")

	// Re-deploy ownership step with a DRIFTED resolved owner.
	binds, err := buildStatefulVolumeBindsContext(context.Background(), hostPath, []string{"/data"}, driftUID, driftGID)
	require.NoError(t, err)
	require.Equal(t, "/data", binds[dataDir], "the VOLUME subdir must be bound to /data")

	// Boundary: the top VOLUME dir follows the drift; the subtree does NOT.
	assert.Equal(t, uint32(driftUID), ownerUID(t, dataDir),
		"top VOLUME dir IS re-owned to the freshly-resolved image user on restore")
	assert.Equal(t, uint32(priorUID), ownerUID(t, nestedDir),
		"nested dir keeps its prior owner (chown is non-recursive) — tenant responsibility on drift")
	assert.Equal(t, uint32(priorUID), ownerUID(t, nestedFile),
		"nested file keeps its prior owner (chown is non-recursive) — tenant responsibility on drift")
}
