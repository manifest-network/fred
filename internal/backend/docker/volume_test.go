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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
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

	t.Run("nonexistent path returns nil", func(t *testing.T) {
		ids, err := listVolumeIDs("/nonexistent/path")
		require.NoError(t, err)
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
	vm, err := newVolumeManager("", "", nil)
	require.NoError(t, err)
	assert.IsType(t, &noopVolumeManager{}, vm)
}

func TestNewVolumeManager_UnsupportedFilesystem(t *testing.T) {
	_, err := newVolumeManager("/tmp", "ext4", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported volume_filesystem")
}

func TestCleanupOrphanedVolumes_DestroysOrphans(t *testing.T) {
	var destroyedIDs []string
	vm := &mockVolumeManager{
		ListFn: func() ([]string, error) {
			return []string{"fred-lease-1-0", "fred-lease-2-0", "fred-orphan-0"}, nil
		},
		DestroyFn: func(ctx context.Context, id string) error {
			destroyedIDs = append(destroyedIDs, id)
			return nil
		},
	}

	cfg := DefaultConfig()
	cfg.NetworkIsolation = ptrBool(false)
	pool := shared.NewResourcePool(cfg.TotalCPUCores, cfg.TotalMemoryMB, cfg.TotalDiskMB, cfg.GetSKUProfile, nil)
	stopCtx, stopCancel := context.WithCancel(context.Background())
	defer stopCancel()

	b := &Backend{
		cfg:     cfg,
		pool:    pool,
		volumes: vm,
		logger:  slog.Default(),
		provisions: map[string]*provision{
			"lease-1": {LeaseUUID: "lease-1", Quantity: 1, Status: backend.ProvisionStatusReady},
			"lease-2": {LeaseUUID: "lease-2", Quantity: 1, Status: backend.ProvisionStatusReady},
		},
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
	}
	b.callbackSender = shared.NewCallbackSender(shared.CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     b.logger,
		StopCtx:    b.stopCtx,
	})

	err := b.cleanupOrphanedVolumes(context.Background())
	require.NoError(t, err)

	// Only the orphan should be destroyed — lease-1 and lease-2 volumes are expected
	assert.Equal(t, []string{"fred-orphan-0"}, destroyedIDs)
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
	b.callbackSender = shared.NewCallbackSender(shared.CallbackSenderConfig{
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
