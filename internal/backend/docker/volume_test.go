package docker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		_, err := vm.Create(context.Background(), "test-vol", 1024)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "noop volume manager")
	})

	t.Run("Destroy is no-op", func(t *testing.T) {
		err := vm.Destroy(context.Background(), "test-vol")
		require.NoError(t, err)
	})

	t.Run("Validate succeeds", func(t *testing.T) {
		err := vm.Validate()
		require.NoError(t, err)
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
