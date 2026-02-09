package docker

import (
	"context"
	"fmt"
	"log/slog"
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
		cfg:    cfg,
		pool:   pool,
		volumes: vm,
		logger: slog.Default(),
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
