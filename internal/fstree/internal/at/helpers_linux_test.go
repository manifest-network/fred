//go:build linux

package at

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// tempDir is t.TempDir with symlinks resolved.
func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	return dir
}

// openDir opens path as a directory for the rest of the test.
func openDir(t *testing.T, path string) *os.File {
	t.Helper()
	dir, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dir.Close() })
	return dir
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

// mustName parses a name the test knows to be valid.
func mustName(t *testing.T, name string) Name {
	t.Helper()
	parsed, ok := ParseName(name)
	require.True(t, ok, "%q", name)
	return parsed
}

// borrowed runs fn with path's directory borrowed as a Dir.
func borrowed(t *testing.T, path string, fn func(d *Dir)) {
	t.Helper()
	require.NoError(t, Borrow(openDir(t, path), fn))
}

// openOwned opens the directory name inside parentPath as an owned Dir,
// closed at the end of the test.
func openOwned(t *testing.T, parentPath, name string) *Dir {
	t.Helper()
	var child *Dir
	borrowed(t, parentPath, func(d *Dir) {
		var err error
		child, err = d.OpenChild(mustName(t, name))
		require.NoError(t, err)
	})
	t.Cleanup(child.Close)
	return child
}
