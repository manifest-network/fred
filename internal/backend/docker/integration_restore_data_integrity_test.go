//go:build integration

package docker

// ENG-365 — restore data-integrity hardening.
//
// Background: the pre-existing restore round-trip tests
// (TestIntegration_Docker_RetainRestoreLifecycle and friends) prove a sentinel
// survives close→restore, but only with a single top-level file checked via
// substring (assert.Contains), same image/user, single volume, and no
// ownership/permission assertion. This file closes those gaps:
//
//   - DataIntegrity      : exact byte-equality + a NESTED file + mode preservation.
//   - MultiInstance      : Quantity=2 — each instance's data lands on the correct
//                          new canonical volume (pins retainedToNewCanonical's
//                          per-index suffix arithmetic).
//   - PreservesNestedMetadata : the RenameVolume primitive is metadata-only — nested
//                          uid/gid, mode, mtime, symlink (and xattr where supported)
//                          survive the rename, so bytes+ownership are preserved on
//                          disk by construction.
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
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// retainRestoreBackend builds a real-Docker backend wired for the soft-delete +
// restore round-trip: btrfs at mountPath, RetainOnClose on, an isolated
// retention DB, and the background reaper disabled so it can't race assertions.
// (Mirrors the config block in integration_restore_test.go.)
func retainRestoreBackend(t *testing.T, mountPath string) *Backend {
	t.Helper()
	return testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.VolumeDataPath = mountPath
		cfg.VolumeFilesystem = "btrfs"
		cfg.RetainOnClose = true
		cfg.RetentionDBPath = filepath.Join(t.TempDir(), "retention.db")
		cfg.RetentionMaxAge = 0 // reaping disabled
		cfg.RetentionReapInterval = 0
	})
}

// ownerUID returns the on-disk owning uid of path (no symlink follow).
func ownerUID(t *testing.T, path string) uint32 {
	t.Helper()
	fi, err := os.Lstat(path)
	require.NoError(t, err)
	st, ok := fi.Sys().(*syscall.Stat_t)
	require.True(t, ok, "expected *syscall.Stat_t for %s", path)
	return st.Uid
}

// TestIntegration_Docker_RetainRestore_DataIntegrity strengthens the sentinel
// round-trip into a real data-integrity check: it writes a top-level AND a nested
// file with known bytes and a non-default mode, then asserts EXACT byte equality
// (not substring) on read-back from the restored lease's container AND on the
// host volume, plus mode preservation.
func TestIntegration_Docker_RetainRestore_DataIntegrity(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	callbackServer, callbackCh := startCallbackServer(t)
	b := retainRestoreBackend(t, mountPath)

	ctx := context.Background()
	origLease := fmt.Sprintf("retain-di-orig-%d", time.Now().UnixNano())

	const topContent = "top-exact-ALPHA"           // no trailing newline → exact-byte assertable
	const nestedContent = "deep-nested-BRAVO-1234" // distinct, nested under /data/nested/dir

	appManifest := manifest.Manifest{Image: "redis:7", Command: []string{"sleep", "3600"}}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)

	require.NoError(t, b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    origLease,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	}))
	cb := waitForCallback(t, callbackCh, origLease, 3*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, cb.Status)

	// Write a top-level + nested file with exact bytes and a 0640 mode.
	containerID := getContainerID(t, origLease)
	require.True(t, containerHasBindMount(t, containerID, "/data"),
		"container must bind-mount /data for the data to persist")
	execInContainer(t, containerID, []string{"sh", "-c",
		fmt.Sprintf("mkdir -p /data/nested/dir && printf '%%s' '%s' > /data/top.txt && "+
			"printf '%%s' '%s' > /data/nested/dir/deep.txt && chmod 0640 /data/top.txt",
			topContent, nestedContent)})

	// Soft-delete (retain).
	require.NoError(t, b.Deprovision(ctx, origLease))
	cb = waitForCallback(t, callbackCh, origLease, 30*time.Second)
	require.Equal(t, backend.CallbackStatusDeprovisioned, cb.Status)
	require.True(t, cb.Retained, "real btrfs retain must set the ground-truth Retained flag")

	// Restore into a new lease.
	newLease := fmt.Sprintf("retain-di-new-%d", time.Now().UnixNano())
	require.NoError(t, b.Restore(ctx, backend.RestoreRequest{
		LeaseUUID:     newLease,
		FromLeaseUUID: origLease,
		Tenant:        "test-tenant",
		ProviderUUID:  "test-provider",
		Items:         []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		CallbackURL:   callbackServer.URL,
	}))
	cb = waitForCallback(t, callbackCh, newLease, 3*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, cb.Status, "restore must succeed; error: %s", cb.Error)

	// ── Exact byte-equality from the RESTORED lease's container ───────────
	newContainerID := getContainerID(t, newLease)
	assert.Equal(t, topContent,
		execInContainer(t, newContainerID, []string{"cat", "/data/top.txt"}),
		"top-level file bytes must be identical after restore (exact, not substring)")
	assert.Equal(t, nestedContent,
		execInContainer(t, newContainerID, []string{"cat", "/data/nested/dir/deep.txt"}),
		"NESTED file bytes must be identical after restore")
	assert.Equal(t, "640",
		strings.TrimSpace(execInContainer(t, newContainerID, []string{"stat", "-c", "%a", "/data/top.txt"})),
		"file mode must be preserved across restore")

	// ── Exact byte-equality on the HOST volume ───────────────────────────
	newCanonical := canonicalVolumeName(newLease, manifest.DefaultServiceName, 0)
	topHost, err := os.ReadFile(filepath.Join(mountPath, newCanonical, "data", "top.txt"))
	require.NoError(t, err)
	assert.Equal(t, topContent, string(topHost))
	nestedHost, err := os.ReadFile(filepath.Join(mountPath, newCanonical, "data", "nested", "dir", "deep.txt"))
	require.NoError(t, err)
	assert.Equal(t, nestedContent, string(nestedHost))

	// Cleanup: hard-delete the restored lease (reaper is off in this test).
	b.cfg.RetainOnClose = false
	require.NoError(t, b.Deprovision(ctx, newLease))
}

// TestIntegration_Docker_RetainRestore_MultiInstance pins the per-instance volume
// mapping through close→restore. With Quantity=2 there are two retained volumes
// (…-app-0, …-app-1); adoptRetainedVolumes must rename each onto the new lease's
// matching index. Distinct data written into each original instance must reappear
// on the correctly-indexed new canonical volume — a single-volume sentinel test
// can't catch a cross-index mis-mapping in retainedToNewCanonical.
func TestIntegration_Docker_RetainRestore_MultiInstance(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	callbackServer, callbackCh := startCallbackServer(t)
	b := retainRestoreBackend(t, mountPath)

	ctx := context.Background()
	origLease := fmt.Sprintf("retain-multi-orig-%d", time.Now().UnixNano())

	appManifest := manifest.Manifest{Image: "redis:7", Command: []string{"sleep", "3600"}}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)

	require.NoError(t, b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    origLease,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 2}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	}))
	cb := waitForCallback(t, callbackCh, origLease, 3*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, cb.Status)

	// Write distinct data into each instance's volume (host-side: the bind mount
	// makes {vol}/data identical to the container's /data). Host writes keep the
	// per-index assertion deterministic without container-ordering games.
	instanceContent := map[int]string{0: "instance-0-CHARLIE", 1: "instance-1-DELTA"}
	for idx, content := range instanceContent {
		vol := canonicalVolumeName(origLease, manifest.DefaultServiceName, idx)
		require.NoError(t, os.WriteFile(
			filepath.Join(mountPath, vol, "data", "marker.txt"), []byte(content), 0o644))
	}

	// Soft-delete (retain both volumes).
	require.NoError(t, b.Deprovision(ctx, origLease))
	cb = waitForCallback(t, callbackCh, origLease, 30*time.Second)
	require.Equal(t, backend.CallbackStatusDeprovisioned, cb.Status)
	rec, err := b.retentionStore.Get(origLease)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.Len(t, rec.RetainedVolumeNames, 2, "both instance volumes must be retained")

	// Restore Quantity=2 into a new lease.
	newLease := fmt.Sprintf("retain-multi-new-%d", time.Now().UnixNano())
	require.NoError(t, b.Restore(ctx, backend.RestoreRequest{
		LeaseUUID:     newLease,
		FromLeaseUUID: origLease,
		Tenant:        "test-tenant",
		ProviderUUID:  "test-provider",
		Items:         []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: manifest.DefaultServiceName}},
		CallbackURL:   callbackServer.URL,
	}))
	cb = waitForCallback(t, callbackCh, newLease, 3*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, cb.Status, "multi-instance restore must succeed; error: %s", cb.Error)

	// Each instance's data must land on the correctly-indexed new volume.
	for idx, want := range instanceContent {
		vol := canonicalVolumeName(newLease, manifest.DefaultServiceName, idx)
		got, err := os.ReadFile(filepath.Join(mountPath, vol, "data", "marker.txt"))
		require.NoError(t, err, "instance %d marker must exist on its new canonical volume", idx)
		assert.Equal(t, want, string(got),
			"instance %d data must map to new volume index %d (retainedToNewCanonical suffix arithmetic)", idx, idx)
	}

	// Cleanup.
	b.cfg.RetainOnClose = false
	require.NoError(t, b.Deprovision(ctx, newLease))
}

// TestIntegration_Docker_BtrfsRenameVolume_PreservesNestedMetadata extends the
// subvol-id check (TestIntegration_Docker_BtrfsRenameVolume_PreservesSubvolID)
// to NESTED file metadata: ownership (uid/gid), mode, mtime, symlink target, and
// (where the fs supports it) a user xattr must all survive RenameVolume. This is
// the foundation of restore data integrity — the rename is metadata-only, so
// bytes AND on-disk ownership are preserved by construction; the only thing
// restore re-applies afterward is the top-dir chown (see OwnershipBoundary).
func TestIntegration_Docker_BtrfsRenameVolume_PreservesNestedMetadata(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	mgr := &btrfsVolumeManager{dataPath: mountPath, logger: slog.Default()}

	const oldName = "fred-meta-legacy-app-0"
	const newName = "fred-meta-new-app-0"
	const (
		wantUID  = 4242
		wantGID  = 4243
		wantMode = 0o640
	)
	wantMtime := time.Unix(1_600_000_000, 0)

	oldPath := filepath.Join(mountPath, oldName)
	out, err := exec.Command("btrfs", "subvolume", "create", oldPath).CombinedOutput()
	require.NoError(t, err, "btrfs subvolume create: %s", out)

	// Populate a nested file + a symlink, then stamp ownership/mode/mtime.
	nestedDir := filepath.Join(oldPath, "nested", "dir")
	require.NoError(t, os.MkdirAll(nestedDir, 0o755))
	nestedFile := filepath.Join(nestedDir, "file.txt")
	require.NoError(t, os.WriteFile(nestedFile, []byte("payload"), os.FileMode(wantMode)))
	symlinkPath := filepath.Join(nestedDir, "link.txt")
	require.NoError(t, os.Symlink("file.txt", symlinkPath))

	require.NoError(t, os.Chmod(nestedFile, os.FileMode(wantMode)))
	require.NoError(t, os.Chown(nestedFile, wantUID, wantGID))
	require.NoError(t, os.Chtimes(nestedFile, wantMtime, wantMtime))

	// Optional: a user xattr, only asserted if the fs accepts it.
	const xattrName = "user.fred_test"
	const xattrVal = "eng365"
	xattrSupported := syscall.Setxattr(nestedFile, xattrName, []byte(xattrVal), 0) == nil
	if !xattrSupported {
		t.Log("user xattr not supported on this fs; skipping the xattr assertion")
	}

	originalID := extractSubvolID(t, oldPath)
	require.NotEmpty(t, originalID)

	// Rename via the manager (the restore adopt primitive).
	require.NoError(t, mgr.RenameVolume(oldName, newName))
	newPath := filepath.Join(mountPath, newName)
	_, err = os.Stat(oldPath)
	require.True(t, errors.Is(err, fs.ErrNotExist), "old path must be gone after rename")

	// All nested metadata must survive at the new path.
	newNestedFile := filepath.Join(newPath, "nested", "dir", "file.txt")
	fi, err := os.Stat(newNestedFile)
	require.NoError(t, err, "nested file must exist after rename")
	st, ok := fi.Sys().(*syscall.Stat_t)
	require.True(t, ok)
	assert.Equal(t, uint32(wantUID), st.Uid, "nested file uid must survive rename")
	assert.Equal(t, uint32(wantGID), st.Gid, "nested file gid must survive rename")
	assert.Equal(t, os.FileMode(wantMode), fi.Mode().Perm(), "nested file mode must survive rename")
	assert.Equal(t, wantMtime.Unix(), fi.ModTime().Unix(), "nested file mtime must survive rename")

	target, err := os.Readlink(filepath.Join(newPath, "nested", "dir", "link.txt"))
	require.NoError(t, err, "symlink must survive rename")
	assert.Equal(t, "file.txt", target, "symlink target must survive rename")

	if xattrSupported {
		buf := make([]byte, 64)
		n, err := syscall.Getxattr(newNestedFile, xattrName, buf)
		require.NoError(t, err, "xattr must survive rename")
		assert.Equal(t, xattrVal, string(buf[:n]), "xattr value must survive rename")
	}

	// Subvol identity preserved (reinforces the rename-is-metadata-only invariant).
	assert.Equal(t, originalID, extractSubvolID(t, newPath),
		"btrfs subvolume ID must be preserved across rename")

	out, _ = exec.Command("btrfs", "subvolume", "delete", newPath).CombinedOutput()
	t.Logf("cleanup btrfs subvolume delete: %s", out)
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
	binds, err := buildStatefulVolumeBinds(hostPath, []string{"/data"}, driftUID, driftGID)
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

// TestIntegration_Docker_Close_WritablePathOnly_Reclaimed pins the ENG-406
// reclaim policy end-to-end on real btrfs: a lease whose only on-disk volume is
// writable-path-only — backed solely by a _wp subtree, no declared VOLUME — is
// DESTROYED (reclaimed) at close, NOT soft-deleted into the retained namespace.
//
// Such a volume is ephemeral by the ENG-367 wipe-contract: setupWritablePathBinds
// RemoveAll's and re-extracts _wp fresh from the image on every deploy including
// restore, so its content is image-derived and never restore-durable. Retaining
// it would preserve nothing restorable while minting a retention record, a
// per-tenant retained-lease slot, a fred-retained-* dir, and a retained-disk
// budget charge — pure pollution. ENG-406 reclaims it instead.
//
// This SUPERSEDES the prior ENG-367 pin (RetainRestore_WritablePathWipeContract),
// which retained + restored this same pure-writable-path-only grafana lease and
// asserted the _wp data was wiped on restore. Under ENG-406 a pure-writable-path-
// only lease is no longer retained — hence not restorable (a re-provision yields
// identical reseeded content) — so the wipe-on-restore code path
// (setupWritablePathBinds, unchanged) is now reachable only for MIXED leases (a
// stateful VOLUME alongside a writable path), where the VOLUME data is restored
// and the _wp data is reseeded. (No mixed-fixture integration test covers that
// residual path today; the unchanged wipe behavior is otherwise unaffected.)
//
// Fixture: grafana/grafana:11.1.0 — non-root (uid 472), declares NO VOLUMEs,
// and /var/lib/grafana is owned by 472, so fred detects it as a writable path
// and backs it with _wp. Command is overridden to `sleep` so grafana-server
// never starts (the writable path is detected at image-inspect time and bound
// regardless of the command).
func TestIntegration_Docker_Close_WritablePathOnly_Reclaimed(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	callbackServer, callbackCh := startCallbackServer(t)
	b := retainRestoreBackend(t, mountPath)
	// Writable-path detection only runs under a read-only rootfs. That is the
	// DefaultConfig default retainRestoreBackend inherits (config.go); set here
	// explicitly to document this test's dependency on it.
	b.cfg.ContainerReadonlyRootfs = ptrBool(true)

	ctx := context.Background()
	origLease := fmt.Sprintf("retain-wp-orig-%d", time.Now().UnixNano())

	const wpPath = "/var/lib/grafana" // grafana writable path (NOT a declared VOLUME)
	const sentinelName = "tenant-wrote-this.txt"
	const sentinelContent = "wp-sentinel-ZULU"

	appManifest := manifest.Manifest{Image: "grafana/grafana:11.1.0", Command: []string{"sleep", "3600"}}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)

	require.NoError(t, b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    origLease,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	}))
	cb := waitForCallback(t, callbackCh, origLease, 3*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, cb.Status)

	// Guard against a vacuous pass: the writable path must actually be wired as a
	// bind mount (i.e. fred detected /var/lib/grafana and backed it with _wp). If
	// this fails, the fixture no longer exercises the _wp path and the rest of the
	// test would prove nothing.
	containerID := getContainerID(t, origLease)
	require.True(t, containerHasBindMount(t, containerID, wpPath),
		"%s must be a detected writable-path bind mount on the original lease", wpPath)

	// Tenant writes data under the writable path.
	execInContainer(t, containerID, []string{"sh", "-c",
		fmt.Sprintf("printf '%%s' '%s' > %s/%s", sentinelContent, wpPath, sentinelName)})

	// It must exist on the host _wp subdir before close (fixture sanity).
	canonical := canonicalVolumeName(origLease, manifest.DefaultServiceName, 0)
	hostVolDir := filepath.Join(mountPath, canonical)
	_, err = os.Stat(filepath.Join(hostVolDir, "_wp", "var", "lib", "grafana", sentinelName))
	require.NoError(t, err, "writable-path sentinel must exist on host _wp before close")

	// Close with RetainOnClose=true. ENG-406: a writable-path-only volume must be
	// RECLAIMED (destroyed), not soft-deleted into the retained namespace.
	require.NoError(t, b.Deprovision(ctx, origLease))
	cb = waitForCallback(t, callbackCh, origLease, 30*time.Second)
	require.Equal(t, backend.CallbackStatusDeprovisioned, cb.Status)
	require.False(t, cb.Retained,
		"a writable-path-only lease must NOT report retained — its volume is reclaimed, not soft-deleted")

	// No retention record was written: no per-tenant slot, nothing restorable.
	rec, err := b.retentionStore.Get(origLease)
	require.NoError(t, err)
	assert.Nil(t, rec, "writable-path-only close must leave no retention record")

	// The canonical volume directory is destroyed, and no fred-retained-* tombstone
	// dir was created for it.
	_, err = os.Stat(hostVolDir)
	assert.True(t, errors.Is(err, fs.ErrNotExist),
		"the writable-path-only volume directory must be destroyed on close")
	_, err = os.Stat(filepath.Join(mountPath, retainedName(canonical)))
	assert.True(t, errors.Is(err, fs.ErrNotExist),
		"no fred-retained-* tombstone must be left for a reclaimed writable-path-only volume")
}
