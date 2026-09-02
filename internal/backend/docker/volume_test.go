package docker

import (
	"context"
	"fmt"
	"hash/crc32"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestSanitizeVolumePath(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// Valid paths
		{"simple", "/data", "data"},
		{"nested", "/var/lib/postgresql/data", "var/lib/postgresql/data"},
		{"deep nesting", "/opt/app/storage/data", "opt/app/storage/data"},

		// Trailing slashes cleaned
		{"trailing slash", "/data/", "data"},

		// Double slashes cleaned
		{"double slash", "/data//subdir", "data/subdir"},

		// Invalid paths return ""
		{"root", "/", ""},
		{"empty", "", ""},
		{"dot", ".", ""},
		{"dotdot", "..", ""},
		// /../etc/passwd resolves to /etc/passwd (.. from root stays at root)
		{"parent from root", "/../etc/passwd", "etc/passwd"},
		{"relative parent", "../etc", ""},

		// No leading slash (unusual but valid)
		{"no leading slash", "data", "data"},
		{"relative nested", "var/lib/data", "var/lib/data"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeVolumePath(tt.in)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBuildStatefulVolumeBinds(t *testing.T) {
	t.Run("empty volumes returns empty map", func(t *testing.T) {
		dir := t.TempDir()
		binds, err := buildStatefulVolumeBindsContext(context.Background(), dir, nil, 0, 0)
		require.NoError(t, err)
		assert.Empty(t, binds)
	})

	t.Run("single volume path", func(t *testing.T) {
		dir := t.TempDir()
		binds, err := buildStatefulVolumeBindsContext(context.Background(), dir, []string{"/data"}, 0, 0)
		require.NoError(t, err)
		require.Len(t, binds, 1)
		expected := filepath.Join(dir, "data")
		assert.Equal(t, "/data", binds[expected])
		assert.DirExists(t, expected)
		assertNoSymlinkBindSources(t, binds)
	})

	t.Run("multiple volume paths", func(t *testing.T) {
		dir := t.TempDir()
		binds, err := buildStatefulVolumeBindsContext(context.Background(), dir, []string{"/data", "/var/lib/postgresql/data"}, 0, 0)
		require.NoError(t, err)
		assert.Len(t, binds, 2)
		assert.Equal(t, "/data", binds[filepath.Join(dir, "data")])
		assert.Equal(t, "/var/lib/postgresql/data", binds[filepath.Join(dir, "var/lib/postgresql/data")])
		assert.DirExists(t, filepath.Join(dir, "data"))
		assert.DirExists(t, filepath.Join(dir, "var/lib/postgresql/data"))
		assertNoSymlinkBindSources(t, binds)
	})

	t.Run("unsupported volume path returns error", func(t *testing.T) {
		dir := t.TempDir()
		_, err := buildStatefulVolumeBindsContext(context.Background(), dir, []string{".."}, 0, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported VOLUME path")
		assert.Contains(t, err.Error(), "..")
	})

	t.Run("unsupported path among valid paths returns error and stops", func(t *testing.T) {
		dir := t.TempDir()
		// "/" sanitizes to "" which is unsupported
		_, err := buildStatefulVolumeBindsContext(context.Background(), dir, []string{"/data", "/"}, 0, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported VOLUME path")
	})

	t.Run("uid gid zero skips chown", func(t *testing.T) {
		dir := t.TempDir()
		binds, err := buildStatefulVolumeBindsContext(context.Background(), dir, []string{"/data"}, 0, 0)
		require.NoError(t, err)
		assert.Len(t, binds, 1)
		// Just verify it succeeds without chown — no permission error
		assertNoSymlinkBindSources(t, binds)
	})

	t.Run("mkdir on read-only parent fails", func(t *testing.T) {
		dir := t.TempDir()
		roDir := filepath.Join(dir, "readonly")
		require.NoError(t, os.MkdirAll(roDir, 0o700))
		require.NoError(t, os.Chmod(roDir, 0o500))
		t.Cleanup(func() { os.Chmod(roDir, 0o700) })

		_, err := buildStatefulVolumeBindsContext(context.Background(), roDir, []string{"/data"}, 0, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "volume subdir")
	})

	t.Run("subdirectory permissions are 0700", func(t *testing.T) {
		dir := t.TempDir()
		_, err := buildStatefulVolumeBindsContext(context.Background(), dir, []string{"/data"}, 0, 0)
		require.NoError(t, err)

		info, err := os.Stat(filepath.Join(dir, "data"))
		require.NoError(t, err)
		assert.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	})

	// ENG-539: a tenant can plant a symlink inside its read-write stateful volume
	// on one deploy, then on a later deploy declare a VOLUME whose path traverses
	// that symlink. sanitizeVolumePath only validates the *string*, so the raw
	// os.MkdirAll/os.Chown used to follow the on-disk symlink and escape the volume
	// root — bind-mounting / chowning arbitrary host paths into the container.
	// buildStatefulVolumeBinds must confine every operation to the volume root and
	// fail closed when a path component is a symlink that escapes it.
	t.Run("symlink leaf component escaping the volume root is rejected", func(t *testing.T) {
		dir := t.TempDir()
		outside := t.TempDir()
		// Sentinel proving the escape target is a pre-existing directory the
		// attacker points at (e.g. host "/" or another tenant's volume root).
		require.NoError(t, os.WriteFile(filepath.Join(outside, "victim"), []byte("x"), 0o600))
		// Tenant plants `dir/data -> outside` from inside its container on deploy 1.
		require.NoError(t, os.Symlink(outside, filepath.Join(dir, "data")))

		// Deploy 2 declares VOLUME /data.
		_, err := buildStatefulVolumeBindsContext(context.Background(), dir, []string{"/data"}, 0, 0)
		require.Error(t, err, "must refuse to resolve a VOLUME path through a symlink that escapes the volume root")
	})

	t.Run("symlink intermediate component escaping the volume root is rejected", func(t *testing.T) {
		dir := t.TempDir()
		outside := t.TempDir()
		// Tenant plants `dir/esc -> outside`.
		require.NoError(t, os.Symlink(outside, filepath.Join(dir, "esc")))

		// Deploy 2 declares VOLUME /esc/pwned, which would traverse the symlink.
		_, err := buildStatefulVolumeBindsContext(context.Background(), dir, []string{"/esc/pwned"}, 0, 0)
		require.Error(t, err, "must refuse to create a VOLUME subdir through an escaping symlink")
		// The escape target must not be written to.
		assert.NoDirExists(t, filepath.Join(outside, "pwned"), "MkdirAll must not have followed the symlink out of the volume root")
	})

	// ENG-795: the half of ENG-539 the two subtests above do NOT cover. Both of them
	// point the planted symlink at a separate t.TempDir(), whose absolute path does not
	// exist *relative to the volume root* — so os.Root's Stat returns ErrNotExist,
	// MkdirAll keeps its EEXIST and errors. They pin the ESCAPING case, which os.Root
	// genuinely handles.
	//
	// os.Root is escape-safe, not symlink-free: rootMkdirAll SUCCEEDS on a leaf symlink
	// that resolves, inside the root, to a directory (os/root_openat.go, "succeed if the
	// link resolves to a directory"), and the leaf stays a symlink on disk. The bind
	// Source is the raw joined string and Docker resolves it host-side — runc trusts the
	// mount source and lets the kernel follow it — so the mount lands on whatever the
	// link names. `..` from a first-level subdir names the lease's own volume root, where
	// the .fred-project-id quota marker lives (volume_xfs.go) and whose unwritability
	// isWritablePathOnly's classification depends on.
	t.Run("in-root symlink leaf is rejected", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "data"), 0o700))
		// Sentinel at the volume ROOT: what the redirected mount would expose.
		require.NoError(t, os.WriteFile(filepath.Join(dir, projectIDFile), []byte("42"), 0o600))
		// Deploy 1, from inside the tenant's read-write /data mount: `ln -s .. /data/x`.
		require.NoError(t, os.Symlink("..", filepath.Join(dir, "data", "x")))

		// Deploy 2 declares VOLUME /data/x. Nothing upstream stops it: sanitizeVolumePath
		// sees no ".." in the *string*, and update preflight never inspects the image's
		// VOLUME set or on-disk state.
		binds, err := buildStatefulVolumeBindsContext(context.Background(), dir, []string{"/data/x"}, 0, 0)
		require.Error(t, err, "a leaf symlink that stays INSIDE the volume root must be rejected")
		assert.Empty(t, binds, "no bind may be emitted once the leaf is refused")
	})

	t.Run("in-root symlink leaf pointing at a sibling is rejected", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "data", "y"), 0o700))
		require.NoError(t, os.Symlink("y", filepath.Join(dir, "data", "x")))

		binds, err := buildStatefulVolumeBindsContext(context.Background(), dir, []string{"/data/x"}, 0, 0)
		require.Error(t, err, "a leaf symlink is unsafe as a bind Source wherever it points")
		assert.Empty(t, binds, "no bind may be emitted once the leaf is refused")
	})

	// The refusal is SCOPED to the leaves the current image declares, and that scoping is
	// the operational contract: volume_bind_symlink_rejected_total's Help and the
	// CHANGELOG both promise a planted link does not wedge the volume and needs no
	// operator cleanup. A review draft of this change claimed the refusal was
	// "input-independent" — it is not, and nothing pinned the difference. Without this
	// subtest, a later "harden it" change that scanned the whole volume root instead of
	// the declared leaf would pass every other test here while making a poisoned volume
	// permanently un-deployable and un-restorable, silently inverting that contract.
	t.Run("a planted leaf does not block an image that does not declare it", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "data"), 0o700))
		require.NoError(t, os.Symlink("..", filepath.Join(dir, "data", "x")))

		// The image that declares the poisoned leaf is refused...
		_, err := buildStatefulVolumeBindsContext(context.Background(), dir, []string{"/data/x"}, 0, 0)
		require.Error(t, err)

		// ...but the same volume still serves an image declaring only /data. That is the
		// tenant's own way out: /data comes back mounted read-write, so its container can
		// delete the link without an operator.
		binds, err := buildStatefulVolumeBindsContext(context.Background(), dir, []string{"/data"}, 0, 0)
		require.NoError(t, err, "a planted leaf must not wedge the rest of the volume")
		assert.Equal(t, "/data", binds[filepath.Join(dir, "data")])
		assertNoSymlinkBindSources(t, binds)

		// And the guard rejected without repairing — the link is still the tenant's to remove.
		info, lerr := os.Lstat(filepath.Join(dir, "data", "x"))
		require.NoError(t, lerr)
		assert.NotZero(t, info.Mode()&os.ModeSymlink, "the guard rejects; it never unlinks")
	})
}

// assertNoSymlinkBindSources pins the invariant the ENG-539/ENG-795 subtests exist to
// protect, which require.Error alone does not: whatever buildStatefulVolumeBinds
// returns, no emitted bind Source may be a symlink. Docker resolves the Source
// host-side, so a guard that returned a symlinked Source without erroring would satisfy
// every "must reject" assertion above and still hand the tenant a redirected mount.
func assertNoSymlinkBindSources(t *testing.T, binds map[string]string) {
	t.Helper()
	for source := range binds {
		info, err := os.Lstat(source)
		require.NoError(t, err, "emitted bind source %q must exist on disk", source)
		assert.Zero(t, info.Mode()&os.ModeSymlink, "emitted bind source %q must not be a symlink", source)
	}
}

func TestNoopVolumeManager(t *testing.T) {
	vm := &noopVolumeManager{}

	t.Run("Create returns error", func(t *testing.T) {
		_, created, err := vm.Create(context.Background(), "test-vol", 1024)
		require.Error(t, err)
		assert.False(t, created)
		assert.Contains(t, err.Error(), "noop volume manager")
	})

	t.Run("Destroy is no-op", func(t *testing.T) {
		err := vm.Destroy(context.Background(), "test-vol")
		require.NoError(t, err)
	})

	t.Run("List returns nil", func(t *testing.T) {
		ids, err := vm.List()
		require.NoError(t, err)
		assert.Nil(t, ids)
	})

	t.Run("Validate succeeds", func(t *testing.T) {
		err := vm.Validate()
		require.NoError(t, err)
	})
}

func TestListVolumeIDs(t *testing.T) {
	t.Run("returns directory names", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "fred-abc-0"), 0755))
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "fred-abc-1"), 0755))
		// Create a file — should be excluded
		require.NoError(t, os.WriteFile(filepath.Join(dir, "stale.lock"), nil, 0644))
		// Non-prefixed directories should be excluded
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "lost+found"), 0755))
		require.NoError(t, os.MkdirAll(filepath.Join(dir, ".snapshots"), 0755))

		ids, err := listVolumeIDs(dir)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{"fred-abc-0", "fred-abc-1"}, ids)
	})

	// Previously "nonexistent path returns nil": ENOENT was mapped to (nil, nil).
	//
	// That collapsed two states a caller must tell apart — "this root holds no volumes" and
	// "this root is gone" — into one value, so anyone needing the difference had to stat
	// separately, and a separate stat is a separate point in time. The reaping finalizer
	// DELETES a record when it sees an empty footprint, so an unmount landing between the
	// probe and the read would have dropped the record accounting for bytes that come back
	// with the mount. Keeping the question inside the one syscall that can answer it makes
	// that race impossible rather than merely unlikely (ENG-676).
	//
	// A configured root cannot legitimately be absent at runtime: newVolumeManager statfs's
	// it at construction (and resolves the xfs mount point), so startup fails first. An
	// unconfigured root yields the noopVolumeManager, whose List never reaches here.
	t.Run("nonexistent path is an error, not an empty node", func(t *testing.T) {
		ids, err := listVolumeIDs("/nonexistent/path")
		require.Error(t, err)
		assert.Nil(t, ids)
	})

	t.Run("empty directory returns nil", func(t *testing.T) {
		dir := t.TempDir()
		ids, err := listVolumeIDs(dir)
		require.NoError(t, err)
		assert.Nil(t, ids)
	})
}

func TestNewVolumeManager_EmptyPath(t *testing.T) {
	vm, err := newVolumeManager("", "", 1024, nil)
	require.NoError(t, err)
	assert.IsType(t, &noopVolumeManager{}, vm)
}

func TestNewVolumeManager_UnsupportedFilesystem(t *testing.T) {
	_, err := newVolumeManager("/tmp", "ext4", 1024, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported volume_filesystem")
}

// TestNewVolumeManager_ConfiguredFilesystemMustMatchReality is the startup half of
// ENG-687, and the deployed fleet sits exactly on the branch it fixes: the docker-backend
// template pins `volume_filesystem: "xfs"`, which used to skip the probe entirely.
//
// That matters because `volume_data_path` is a SUBDIRECTORY of the XFS mount
// (/data/fred/volumes under /data), and the provisioning role creates that directory — so
// if the mount is not up when it runs, the same path exists, empty, on the root
// filesystem. Booting there enumerates zero volumes and is indistinguishable from a fresh
// node, which is how reconcileOrphanedRetentions comes to prune live retention records and
// the next boot's orphan sweep destroys the data behind them. Refusing to start is the
// right answer: a missing mount needs an operator, not a sweep.
func TestNewVolumeManager_ConfiguredFilesystemMustMatchReality(t *testing.T) {
	// The configured value is chosen to disagree with whatever the temp dir REALLY is, rather
	// than assuming it is tmpfs: on a host whose /tmp is XFS, hardcoding "xfs" would describe
	// the truth instead of the lie and the constructor would rightly succeed.
	dir := t.TempDir()
	configured := "xfs"
	if detected, derr := detectFilesystem(dir); derr == nil && detected == configured {
		configured = "btrfs" // any supported type the temp dir is not
	}

	_, err := newVolumeManager(dir, configured, 1024, slog.Default())
	require.Error(t, err, "a configured filesystem that does not match the disk must fail startup")
	assert.Contains(t, err.Error(), "is the volume mounted?",
		"the error must name the likely cause; this fires on hosts where the mount unit failed")
}

// TestVolumeRootWatch_EmptyAfterSeenVolumes_OnADifferentDeviceIsAnError is the runtime
// half of ENG-687: the root does not disappear on a plain unmount, it is simply served by
// the parent filesystem afterwards, so ReadDir succeeds and returns the empty stub. ENOENT
// never fires and every consumer reads "no volumes here".
//
// The baseline is learned from a populated read, so the guard covers the transition that
// matters — held volumes, then suddenly none — without needing to be configured.
func TestVolumeRootWatch_EmptyAfterSeenVolumes_OnADifferentDeviceIsAnError(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "fred-u1-app-0"), 0o755))

	var w volumeRootWatch
	ids, err := w.list(root)
	require.NoError(t, err)
	require.Len(t, ids, 1, "precondition: the baseline is learned from a populated read")
	require.True(t, w.seen)

	// Now the unmount. Rather than hunting for a second filesystem — which a CI box may not
	// have, and a skipped test guards nothing — the recorded baseline is moved out from under
	// the path directly. That is the same state an unmount produces (the path is now served
	// by a different device than the one we learned) and it is deterministic everywhere.
	require.NoError(t, os.RemoveAll(filepath.Join(root, "fred-u1-app-0")))
	w.dev++

	_, err = w.list(root)
	require.Error(t, err, "an empty root on a different device is uncertainty, not emptiness")
	assert.Contains(t, err.Error(), "is it unmounted?")
}

// TestVolumeRootWatch_EmptyBeforeAnyVolumeIsFine keeps the guard from making a genuinely
// fresh provider unusable: a root that has never held a volume has no baseline and nothing
// to lose, so emptiness there is simply the truth.
func TestVolumeRootWatch_EmptyBeforeAnyVolumeIsFine(t *testing.T) {
	var w volumeRootWatch
	ids, err := w.list(t.TempDir())
	require.NoError(t, err)
	assert.Empty(t, ids)
}

// TestVolumeRootWatch_EmptyOnTheSameDeviceIsStillEmpty is the other non-regression: once
// the volumes really are reclaimed, the root reports empty and the reaper must be able to
// act on it. Without this the guard would wedge every finalizer permanently.
func TestVolumeRootWatch_EmptyOnTheSameDeviceIsStillEmpty(t *testing.T) {
	root := t.TempDir()
	vol := filepath.Join(root, "fred-u1-app-0")
	require.NoError(t, os.MkdirAll(vol, 0o755))

	var w volumeRootWatch
	ids, err := w.list(root)
	require.NoError(t, err)
	require.Len(t, ids, 1)

	require.NoError(t, os.RemoveAll(vol)) // the reaper did its job
	ids, err = w.list(root)
	require.NoError(t, err, "same device, genuinely empty — the reaper must be able to finish")
	assert.Empty(t, ids)
}

func TestCleanupOrphanedVolumes_ListFailure(t *testing.T) {
	vm := &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return nil, fmt.Errorf("I/O error")
		},
	}

	cfg := DefaultConfig()
	cfg.NetworkIsolation = ptrBool(false)
	pool := shared.NewResourcePool(cfg.TotalCPUCores, cfg.TotalMemoryMB, cfg.TotalDiskMB, cfg.GetSKUProfile, nil)
	stopCtx, stopCancel := context.WithCancel(context.Background())
	defer stopCancel()

	b := &Backend{
		cfg:        cfg,
		pool:       pool,
		volumes:    vm,
		logger:     slog.Default(),
		provisions: make(map[string]*provision),
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
	}
	b.callbackSender = shared.MustNewEphemeralCallbackSender(shared.CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     b.logger,
		StopCtx:    b.stopCtx,
	})

	err := b.cleanupOrphanedVolumes(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list volumes")
}

// --- XFS project ID collision resolution tests ---

func newTestXFSManager(t *testing.T) *xfsVolumeManager {
	t.Helper()
	return &xfsVolumeManager{
		dataPath:   t.TempDir(),
		logger:     slog.Default(),
		activeIDs:  make(map[uint32]string),
		volumeToID: make(map[string]uint32),
	}
}

func TestXFSProjectIDCollisionResolution(t *testing.T) {
	mgr := newTestXFSManager(t)

	// Craft a collision: insert vol1 at vol2's CRC32 hash so vol2 is
	// forced to probe.
	vol1 := "fred-vol-a"
	vol2 := "fred-vol-b"

	id1, err := mgr.assignProjectID(vol1)
	require.NoError(t, err)

	// Remove vol1's real entry, then place it at vol2's hash.
	mgr.mu.Lock()
	delete(mgr.activeIDs, id1)
	delete(mgr.volumeToID, vol1)
	vol2Hash := crc32.ChecksumIEEE([]byte(vol2))
	mgr.activeIDs[vol2Hash] = vol1
	mgr.volumeToID[vol1] = vol2Hash
	mgr.mu.Unlock()

	id2, err := mgr.assignProjectID(vol2)
	require.NoError(t, err)

	assert.NotEqual(t, vol2Hash, id2, "vol2 should have been probed away from the colliding hash")
	assert.Equal(t, vol2Hash+1, id2, "vol2 should get the next available ID")

	// Both should be tracked with distinct IDs.
	mgr.mu.Lock()
	assert.Equal(t, vol1, mgr.activeIDs[vol2Hash])
	assert.Equal(t, vol2, mgr.activeIDs[id2])
	mgr.mu.Unlock()
}

func TestXFSProjectIDCollisionProbesNextSlot(t *testing.T) {
	mgr := newTestXFSManager(t)

	// Block a volume's CRC32 slot to verify probing lands on the next slot.
	testVol := "fred-test-probe"
	testHash := crc32.ChecksumIEEE([]byte(testVol))

	mgr.mu.Lock()
	mgr.activeIDs[testHash] = "fred-occupant"
	mgr.volumeToID["fred-occupant"] = testHash
	mgr.mu.Unlock()

	id, err := mgr.assignProjectID(testVol)
	require.NoError(t, err)
	assert.Equal(t, testHash+1, id, "should probe to next slot")
}

func TestXFSProjectIDSkipsReservedZero(t *testing.T) {
	mgr := newTestXFSManager(t)

	// Fill slots at MaxUint32 and 1 so that a probe starting at MaxUint32
	// wraps through 0 (skipped) and past 1 (taken) to land on 2.
	mgr.mu.Lock()
	mgr.activeIDs[math.MaxUint32] = "fred-at-max"
	mgr.volumeToID["fred-at-max"] = math.MaxUint32
	mgr.activeIDs[1] = "fred-at-one"
	mgr.volumeToID["fred-at-one"] = 1
	mgr.mu.Unlock()

	// We need a volumeID whose CRC32 is MaxUint32. Since we can't easily
	// find one, directly set up the candidate by assigning a known volume
	// that already has MaxUint32 blocked — but assignProjectID seeds from
	// CRC32 which won't be MaxUint32. Instead, verify the logic by blocking
	// the CRC32 slot AND MaxUint32, confirming ID 0 is never returned.
	testVol := "fred-zero-check"
	testHash := crc32.ChecksumIEEE([]byte(testVol))

	mgr.mu.Lock()
	mgr.activeIDs[testHash] = "fred-blocker"
	mgr.volumeToID["fred-blocker"] = testHash
	mgr.mu.Unlock()

	id, err := mgr.assignProjectID(testVol)
	require.NoError(t, err)
	assert.NotEqual(t, uint32(0), id, "project ID 0 is reserved by XFS")
}

func TestXFSProjectIDMarkerRoundtrip(t *testing.T) {
	dir := t.TempDir()
	var id uint32 = 42

	err := writeProjectIDFile(dir, id)
	require.NoError(t, err)

	got, err := readProjectIDFile(dir)
	require.NoError(t, err)
	assert.Equal(t, id, got)
}

func TestXFSProjectIDMarkerRoundtripLargeValue(t *testing.T) {
	dir := t.TempDir()
	var id uint32 = math.MaxUint32

	err := writeProjectIDFile(dir, id)
	require.NoError(t, err)

	got, err := readProjectIDFile(dir)
	require.NoError(t, err)
	assert.Equal(t, id, got)
}

func TestXFSProjectIDIdempotent(t *testing.T) {
	mgr := newTestXFSManager(t)

	vol := "fred-vol-idem"
	id1, err := mgr.assignProjectID(vol)
	require.NoError(t, err)
	id2, err := mgr.assignProjectID(vol)
	require.NoError(t, err)

	assert.Equal(t, id1, id2, "same volumeID should always return the same project ID")
}

func TestXFSProjectIDDestroyFreesSlot(t *testing.T) {
	mgr := newTestXFSManager(t)

	vol := "fred-vol-destroy"
	id, err := mgr.assignProjectID(vol)
	require.NoError(t, err)

	mgr.mu.Lock()
	_, exists := mgr.activeIDs[id]
	_, revExists := mgr.volumeToID[vol]
	mgr.mu.Unlock()
	require.True(t, exists, "project ID should be in activeIDs after assign")
	require.True(t, revExists, "volumeID should be in volumeToID after assign")

	mgr.removeProjectID(vol)

	mgr.mu.Lock()
	_, exists = mgr.activeIDs[id]
	_, revExists = mgr.volumeToID[vol]
	mgr.mu.Unlock()
	assert.False(t, exists, "project ID should be freed after removeProjectID")
	assert.False(t, revExists, "volumeToID should be cleared after removeProjectID")

	// A new volume should be able to reuse the freed slot.
	vol2 := "fred-vol-reuse"
	mgr.mu.Lock()
	mgr.activeIDs = make(map[uint32]string)
	mgr.volumeToID = make(map[string]uint32)
	mgr.mu.Unlock()

	id2, err := mgr.assignProjectID(vol2)
	require.NoError(t, err)
	assert.Equal(t, crc32.ChecksumIEEE([]byte(vol2)), id2, "freed slot should be reusable")
}

func TestXFSValidatePopulatesActiveIDs(t *testing.T) {
	mgr := newTestXFSManager(t)

	// Create fake volume directories with marker files.
	vol1 := "fred-vol-1"
	vol2 := "fred-vol-2"
	var projID1 uint32 = 100
	var projID2 uint32 = 200

	dir1 := filepath.Join(mgr.dataPath, vol1)
	dir2 := filepath.Join(mgr.dataPath, vol2)
	require.NoError(t, os.MkdirAll(dir1, 0755))
	require.NoError(t, os.MkdirAll(dir2, 0755))
	require.NoError(t, writeProjectIDFile(dir1, projID1))
	require.NoError(t, writeProjectIDFile(dir2, projID2))

	// Simulate the scan portion of Validate (can't call Validate directly
	// because it requires xfs_quota).
	ids, err := mgr.List()
	require.NoError(t, err)
	assert.Len(t, ids, 2)

	mgr.mu.Lock()
	for _, vid := range ids {
		dirPath := filepath.Join(mgr.dataPath, vid)
		projID, err := readProjectIDFile(dirPath)
		require.NoError(t, err)
		mgr.trackProjectID(vid, projID)
	}
	mgr.mu.Unlock()

	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	assert.Len(t, mgr.activeIDs, 2)
	assert.Equal(t, vol1, mgr.activeIDs[projID1])
	assert.Equal(t, vol2, mgr.activeIDs[projID2])
	assert.Equal(t, projID1, mgr.volumeToID[vol1])
	assert.Equal(t, projID2, mgr.volumeToID[vol2])
}

func TestXFSValidateErrorsOnMissingMarker(t *testing.T) {
	mgr := newTestXFSManager(t)

	// Volume directory without a marker file should cause an error.
	dir := filepath.Join(mgr.dataPath, "fred-vol-nomarker")
	require.NoError(t, os.MkdirAll(dir, 0755))

	ids, err := mgr.List()
	require.NoError(t, err)
	require.Len(t, ids, 1)

	dirPath := filepath.Join(mgr.dataPath, ids[0])
	_, err = readProjectIDFile(dirPath)
	require.Error(t, err)
}

func TestXFSValidateErrorsOnDuplicateProjectID(t *testing.T) {
	mgr := newTestXFSManager(t)

	// Two volumes with the same project ID in their marker files.
	dir1 := filepath.Join(mgr.dataPath, "fred-vol-dup1")
	dir2 := filepath.Join(mgr.dataPath, "fred-vol-dup2")
	require.NoError(t, os.MkdirAll(dir1, 0755))
	require.NoError(t, os.MkdirAll(dir2, 0755))
	require.NoError(t, writeProjectIDFile(dir1, 42))
	require.NoError(t, writeProjectIDFile(dir2, 42))

	ids, err := mgr.List()
	require.NoError(t, err)

	// Simulate the Validate scan loop — should detect the duplicate.
	mgr.mu.Lock()
	var scanErr error
	for _, vid := range ids {
		dirPath := filepath.Join(mgr.dataPath, vid)
		projID, err := readProjectIDFile(dirPath)
		require.NoError(t, err)
		if existing, ok := mgr.activeIDs[projID]; ok && existing != vid {
			scanErr = fmt.Errorf("duplicate project ID %d: volumes %s and %s", projID, existing, vid)
			break
		}
		mgr.trackProjectID(vid, projID)
	}
	mgr.mu.Unlock()

	require.Error(t, scanErr)
	assert.Contains(t, scanErr.Error(), "duplicate project ID")
}

// --- RenameVolume tests (Task 10) ------------------------------------------
//
// These tests exercise the shared atomicRenameVolumeDir helper and the
// per-backend RenameVolume wrappers. They avoid filesystem-specific
// tooling (xfs_quota, btrfs CLI, zfs CLI) by operating on plain
// directories under t.TempDir() — the rename semantics are identical
// across xfs and btrfs (os.Rename on a managed root). The zfs path
// shells out to the `zfs` binary and is integration-test territory; it's
// not covered by this unit suite.

func TestAtomicRenameVolumeDir_Idempotent(t *testing.T) {
	root := t.TempDir()
	oldPath := filepath.Join(root, "fred-lease-1-0")
	newPath := filepath.Join(root, "fred-lease-1-app-0")
	require.NoError(t, os.MkdirAll(oldPath, 0o755))

	// First call: rename succeeds.
	require.NoError(t, atomicRenameVolumeDir(context.Background(), oldPath, newPath))
	_, err := os.Stat(newPath)
	require.NoError(t, err, "new path should exist after rename")
	_, err = os.Stat(oldPath)
	require.True(t, os.IsNotExist(err), "old path should be gone after rename")

	// Second call: old gone, new exists — idempotent no-op.
	require.NoError(t, atomicRenameVolumeDir(context.Background(), oldPath, newPath))
}

func TestAtomicRenameVolumeDir_BothExistFails(t *testing.T) {
	root := t.TempDir()
	oldPath := filepath.Join(root, "a")
	newPath := filepath.Join(root, "b")
	require.NoError(t, os.MkdirAll(oldPath, 0o755))
	require.NoError(t, os.MkdirAll(newPath, 0o755))

	err := atomicRenameVolumeDir(context.Background(), oldPath, newPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "manual intervention required")
}

func TestAtomicRenameVolumeDir_NeitherExistsFails(t *testing.T) {
	root := t.TempDir()
	err := atomicRenameVolumeDir(context.Background(), filepath.Join(root, "a"), filepath.Join(root, "b"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "neither")
}

// TestXFSVolumeManager_RenameVolume_UpdatesProjectIDMap pins the xfs-
// specific behavior: after a rename, the in-memory volumeToID /
// activeIDs maps must point at the new name with the same project ID.
// The test fakes the directory + marker file rather than invoking
// xfs_quota.
func TestXFSVolumeManager_RenameVolume_UpdatesProjectIDMap(t *testing.T) {
	root := t.TempDir()
	mgr := &xfsVolumeManager{
		dataPath:   root,
		logger:     slog.Default(),
		activeIDs:  make(map[uint32]string),
		volumeToID: make(map[string]uint32),
	}

	const oldName = "fred-lease-1-0"
	const newName = "fred-lease-1-app-0"
	const projID = uint32(42)
	require.NoError(t, os.MkdirAll(filepath.Join(root, oldName), 0o755))
	require.NoError(t, writeProjectIDFile(filepath.Join(root, oldName), projID))
	mgr.mu.Lock()
	mgr.trackProjectID(oldName, projID)
	mgr.mu.Unlock()

	require.NoError(t, mgr.RenameVolume(context.Background(), oldName, newName))

	mgr.mu.Lock()
	gotID, ok := mgr.volumeToID[newName]
	_, oldStillTracked := mgr.volumeToID[oldName]
	gotName, activeOK := mgr.activeIDs[projID]
	mgr.mu.Unlock()

	require.True(t, ok, "newName should be tracked after rename")
	assert.Equal(t, projID, gotID)
	assert.False(t, oldStillTracked, "oldName should be removed from volumeToID")
	require.True(t, activeOK, "projID should still be in activeIDs")
	assert.Equal(t, newName, gotName, "activeIDs[projID] should point at newName")

	// Second invocation: idempotent — old path gone, new path exists.
	require.NoError(t, mgr.RenameVolume(context.Background(), oldName, newName))
}

func TestBtrfsVolumeManager_RenameVolume_Idempotent(t *testing.T) {
	root := t.TempDir()
	mgr := &btrfsVolumeManager{dataPath: root, logger: slog.Default()}

	const oldName = "fred-lease-1-0"
	const newName = "fred-lease-1-app-0"
	require.NoError(t, os.MkdirAll(filepath.Join(root, oldName), 0o755))

	require.NoError(t, mgr.RenameVolume(context.Background(), oldName, newName))
	require.NoError(t, mgr.RenameVolume(context.Background(), oldName, newName)) // idempotent

	_, err := os.Stat(filepath.Join(root, newName))
	require.NoError(t, err)
}

// TestXFSVolumeManager_RenameVolume_FailureLeavesMapUnchanged covers
// the failure path of xfs RenameVolume: when atomicRenameVolumeDir
// returns an error (e.g. neither path exists, or both exist), the
// in-memory volumeToID / activeIDs maps must NOT be modified — a
// partial map update on a failed rename would desynchronise the
// in-memory state from the on-disk reality, and subsequent
// Create/Destroy on either name would resolve to a stale project ID.
func TestXFSVolumeManager_RenameVolume_FailureLeavesMapUnchanged(t *testing.T) {
	root := t.TempDir()
	mgr := &xfsVolumeManager{
		dataPath:   root,
		logger:     slog.Default(),
		activeIDs:  make(map[uint32]string),
		volumeToID: make(map[string]uint32),
	}

	const oldName = "fred-lease-1-0"
	const newName = "fred-lease-1-app-0"
	const projID = uint32(99)
	// Both directories exist → atomicRenameVolumeDir refuses (manual intervention required).
	require.NoError(t, os.MkdirAll(filepath.Join(root, oldName), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(root, newName), 0o755))
	mgr.mu.Lock()
	mgr.trackProjectID(oldName, projID)
	mgr.mu.Unlock()

	err := mgr.RenameVolume(context.Background(), oldName, newName)
	require.Error(t, err, "rename of both-exist must fail")

	// Maps must be unchanged: oldName still tracked, newName not.
	mgr.mu.Lock()
	oldID, oldOK := mgr.volumeToID[oldName]
	_, newOK := mgr.volumeToID[newName]
	activeName, activeOK := mgr.activeIDs[projID]
	mgr.mu.Unlock()

	require.True(t, oldOK, "oldName must still be tracked after a failed rename")
	assert.Equal(t, projID, oldID, "oldName projID must be unchanged")
	assert.False(t, newOK, "newName must not have been added on failure")
	require.True(t, activeOK, "activeIDs entry must remain")
	assert.Equal(t, oldName, activeName, "activeIDs[projID] must still point at oldName on failure")
}

func TestVolumeManager_HostPath(t *testing.T) {
	t.Run("xfs", func(t *testing.T) {
		mgr := &xfsVolumeManager{dataPath: "/var/lib/fred/volumes"}
		assert.Equal(t, "/var/lib/fred/volumes/fred-lease-1-app-0", mgr.HostPath("fred-lease-1-app-0"))
	})
	t.Run("btrfs", func(t *testing.T) {
		mgr := &btrfsVolumeManager{dataPath: "/var/lib/fred/volumes"}
		assert.Equal(t, "/var/lib/fred/volumes/fred-lease-1-app-0", mgr.HostPath("fred-lease-1-app-0"))
	})
	t.Run("zfs", func(t *testing.T) {
		mgr := &zfsVolumeManager{dataPath: "/var/lib/fred/volumes"}
		assert.Equal(t, "/var/lib/fred/volumes/fred-lease-1-app-0", mgr.HostPath("fred-lease-1-app-0"))
	})
	t.Run("noop returns empty", func(t *testing.T) {
		mgr := &noopVolumeManager{}
		assert.Equal(t, "", mgr.HostPath("fred-anything"))
	})
}

// TestListVolumeIDsWithDevice_IdentityMatchesTheListedDirectory pins that the identity and the
// enumeration come from ONE handle rather than two lookups of the path.
//
// A stat of the path is a second observation, and an unmount landing between the two pairs
// volumes read from the old mount with the parent filesystem's device. That pairing is worse
// than no check: volumeRootWatch's populated branch adopts it as the baseline, after which
// every later empty reading matches and is accepted — the guard inverts into a rubber stamp.
// An fd cannot disagree with itself about which filesystem it read.
func TestListVolumeIDsWithDevice_IdentityMatchesTheListedDirectory(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "fred-u1-app-0"), 0o755))

	ids, dev, err := listVolumeIDsWithDevice(root)
	require.NoError(t, err)
	assert.Equal(t, []string{"fred-u1-app-0"}, ids)
	assert.Equal(t, devOf(t, root), dev,
		"the reported device must be the one backing the directory that was enumerated")
	assert.NotZero(t, dev)
}

// TestVolumeRootWatch_ConcurrentListsDoNotInstallAStaleBaseline pins the serialization half.
// Two readings applied out of order would let the older one install a baseline that was
// already superseded — the same stale-snapshot-wins hazard the destroy path had to fix,
// arriving through a different door. Run with -race.
func TestVolumeRootWatch_ConcurrentListsDoNotInstallAStaleBaseline(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "fred-u1-app-0"), 0o755))
	want := devOf(t, root)

	var w volumeRootWatch
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids, err := w.list(root)
			assert.NoError(t, err)
			assert.Len(t, ids, 1)
		}()
	}
	wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()
	assert.True(t, w.seen)
	assert.Equal(t, want, w.dev, "every reading saw the same root, so the baseline must be it")
}
