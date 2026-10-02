//go:build linux

package fstree

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// tempDir is t.TempDir with symlinks resolved, so that it matches the paths
// /proc/self/fd reports.
func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	return dir
}

// mustName parses a name the test knows to be valid.
func mustName(name string) Name {
	parsed, err := ParseName(name)
	if err != nil {
		panic(err)
	}
	return parsed
}

// openDir opens path as a directory for the rest of the test.
func openDir(t *testing.T, path string) *os.File {
	t.Helper()
	dir, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dir.Close() })
	return dir
}

// fdCount counts this process's open descriptors.
func fdCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	return len(entries)
}

// requireNoLeak runs fn and requires the number of open descriptors to be
// the same afterwards.
func requireNoLeak(t *testing.T, fn func()) {
	t.Helper()
	before := fdCount(t)
	fn()
	require.Equal(t, before, fdCount(t), "descriptors leaked")
}

// buildChain creates the directory name inside parent with depth nested
// directories "d" below it, the deepest holding the file "leaf". It uses only
// mkdirat and openat, so PATH_MAX does not limit the depth.
func buildChain(t *testing.T, parent *os.File, name string, depth int) {
	t.Helper()
	const flags = unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC
	require.NoError(t, unix.Mkdirat(int(parent.Fd()), name, 0o755))
	fd, err := unix.Openat(int(parent.Fd()), name, flags, 0)
	require.NoError(t, err)
	for range depth {
		require.NoError(t, unix.Mkdirat(fd, "d", 0o755))
		next, err := unix.Openat(fd, "d", flags, 0)
		require.NoError(t, err)
		require.NoError(t, unix.Close(fd))
		fd = next
	}
	leaf, err := unix.Openat(fd, "leaf", unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC, 0o644)
	require.NoError(t, err)
	require.NoError(t, unix.Close(leaf))
	require.NoError(t, unix.Close(fd))
}

// buildBushyChain creates name inside parentPath as a chain of depth
// directories where every level also holds files, a symlink and an empty
// directory.
func buildBushyChain(t *testing.T, parentPath, name string, depth, files int) {
	t.Helper()
	dir := filepath.Join(parentPath, name)
	require.NoError(t, os.Mkdir(dir, 0o755))
	for level := range depth {
		for i := range files {
			writeFile(t, filepath.Join(dir, fmt.Sprintf("f%d", i)), fmt.Sprintf("level %d file %d", level, i))
		}
		require.NoError(t, os.Symlink("/", filepath.Join(dir, "root-link")))
		require.NoError(t, os.Mkdir(filepath.Join(dir, "empty"), 0o755))
		dir = filepath.Join(dir, "next")
		require.NoError(t, os.Mkdir(dir, 0o755))
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
}

func mkdirAll(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(path, 0o755))
}

// snapshot describes every object under root without following symlinks:
// its type and permission bits, a symlink's target, and a file's content.
func snapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(root, func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		desc := info.Mode().String()
		switch {
		case info.Mode()&fs.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			desc += " -> " + target
		case info.Mode().IsRegular():
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			desc += " " + string(content)
		}
		out[rel] = desc
		return nil
	})
	require.NoError(t, err)
	return out
}

// makeOutside creates sentinel content next to the tree under test: a
// directory with nested files and a lone file.
func makeOutside(t *testing.T, base string) string {
	t.Helper()
	outside := filepath.Join(base, "outside")
	mkdirAll(t, filepath.Join(outside, "dir", "sub"))
	writeFile(t, filepath.Join(outside, "dir", "a"), "alpha")
	writeFile(t, filepath.Join(outside, "dir", "sub", "b"), "beta")
	writeFile(t, filepath.Join(outside, "file"), "sentinel")
	return outside
}

func requireContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, want, string(got))
}

func requireAbsent(t *testing.T, path string) {
	t.Helper()
	_, err := os.Lstat(path)
	require.ErrorIs(t, err, fs.ErrNotExist)
}

// listNames returns the sorted names inside dir.
func listNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	slices.Sort(names)
	return names
}

func inode(t *testing.T, path string) uint64 {
	t.Helper()
	var st unix.Stat_t
	require.NoError(t, unix.Lstat(path, &st))
	return st.Ino
}

// cancelAfter is a context whose Err reports cancellation from its n+1th
// call on, which stops a traversal after a chosen number of checks.
type cancelAfter struct {
	context.Context
	left atomic.Int64
}

func newCancelAfter(n int64) *cancelAfter {
	ctx := &cancelAfter{Context: context.Background()}
	ctx.left.Store(n)
	return ctx
}

func (c *cancelAfter) Err() error {
	if c.left.Add(-1) < 0 {
		return context.Canceled
	}
	return nil
}

// drive runs r the way run does, calling between after every step that
// left the anchor non-empty.
func drive(r *remover, between func()) error {
	opened, err := r.begin()
	if err != nil || !opened {
		return err
	}
	for {
		empty, err := r.step()
		if err != nil {
			return err
		}
		if empty {
			return r.finish()
		}
		if between != nil {
			between()
		}
	}
}
