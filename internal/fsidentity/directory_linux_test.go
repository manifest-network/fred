//go:build linux

package fsidentity

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDirectoryRetainsPhysicalParentAcrossRenameAndRecreate(t *testing.T) {
	root := t.TempDir()
	parentPath := filepath.Join(root, "authority")
	movedPath := filepath.Join(root, "authority-moved")
	require.NoError(t, os.Mkdir(parentPath, 0o700))

	parent, err := OpenDirectory(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parent.Close() })
	expected := parent.Identity()
	require.True(t, expected.Valid())
	require.NoError(t, parent.VerifyPath())

	require.NoError(t, os.Rename(parentPath, movedPath))
	require.NoError(t, os.Mkdir(parentPath, 0o700))
	replacementIdentity, err := InspectDirectory(parentPath)
	require.NoError(t, err)
	require.False(t, expected.Equal(replacementIdentity))
	require.ErrorIs(t, parent.VerifyPath(), ErrDirectoryIdentityChanged)

	name, file, err := parent.CreateTemp(".bound-", 0o600)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.NoError(t, parent.LinkNoReplace(name, "published"))
	require.NoError(t, parent.Sync(),
		"directory fsync must continue to target the retained physical parent")

	_, err = os.Stat(filepath.Join(movedPath, "published"))
	require.NoError(t, err)
	_, err = os.Lstat(filepath.Join(parentPath, "published"))
	require.ErrorIs(t, err, os.ErrNotExist,
		"descriptor-relative publication must not move into the replacement")

	reopened, err := OpenBoundDirectory(parentPath, expected)
	require.ErrorIs(t, err, ErrDirectoryIdentityChanged)
	require.Nil(t, reopened)
}

func TestDirectoryNeverFollowsFinalComponentSymlinks(t *testing.T) {
	root := t.TempDir()
	parentPath := filepath.Join(root, "authority")
	aliasPath := filepath.Join(root, "alias")
	require.NoError(t, os.Mkdir(parentPath, 0o700))
	require.NoError(t, os.Symlink(parentPath, aliasPath))

	parent, err := OpenDirectory(aliasPath)
	require.Error(t, err)
	require.Nil(t, parent)
	parent, err = OpenBoundDirectory(parentPath, Identity{Device: 1})
	require.ErrorContains(t, err, "identity is malformed")
	require.Nil(t, parent)

	parent, err = OpenDirectory(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parent.Close() })
	require.NoError(t, os.WriteFile(filepath.Join(parentPath, "target"), []byte("secret"), 0o600))
	require.NoError(t, os.Symlink("target", filepath.Join(parentPath, "link")))

	exists, err := parent.EntryExists("link")
	require.NoError(t, err)
	require.True(t, exists)
	info, err := parent.Lstat("link")
	require.NoError(t, err)
	require.NotZero(t, info.Mode()&os.ModeSymlink)
	_, err = parent.OpenFile("link", os.O_RDONLY, 0)
	require.Error(t, err)

	for _, name := range []string{
		"", ".", "..", "/", "nested/file", "/absolute", "nul\x00entry",
	} {
		_, err := parent.Entry(name)
		require.ErrorIs(t, err, ErrInvalidEntryName)
		require.Empty(t, parent.DisplayPath(name))
		_, err = parent.OpenFile(name, os.O_RDONLY, 0)
		require.ErrorIs(t, err, ErrInvalidEntryName)
	}
}

func TestDirectoryLinkNoReplacePreservesExistingDestination(t *testing.T) {
	parentPath := t.TempDir()
	parent, err := OpenDirectory(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parent.Close() })

	require.NoError(t, os.WriteFile(filepath.Join(parentPath, "source"), []byte("source"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(parentPath, "destination"), []byte("operator"), 0o600))
	err = parent.LinkNoReplace("source", "destination")
	require.ErrorIs(t, err, os.ErrExist)
	contents, err := os.ReadFile(filepath.Join(parentPath, "destination"))
	require.NoError(t, err)
	require.Equal(t, []byte("operator"), contents)

	require.NoError(t, parent.LinkNoReplace("source", "published"))
	sourceInfo, err := os.Stat(filepath.Join(parentPath, "source"))
	require.NoError(t, err)
	publishedInfo, err := os.Stat(filepath.Join(parentPath, "published"))
	require.NoError(t, err)
	require.True(t, os.SameFile(sourceInfo, publishedInfo))
}

func TestDirectoryRenameNoReplacePreservesExistingDestinationAndUsesOneName(t *testing.T) {
	parentPath := t.TempDir()
	parent, err := OpenDirectory(parentPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = parent.Close() })

	require.NoError(t, os.WriteFile(filepath.Join(parentPath, "source"), []byte("source"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(parentPath, "destination"), []byte("operator"), 0o600))
	err = parent.RenameNoReplace("source", "destination")
	require.ErrorIs(t, err, os.ErrExist)
	contents, err := os.ReadFile(filepath.Join(parentPath, "destination"))
	require.NoError(t, err)
	require.Equal(t, []byte("operator"), contents)

	require.NoError(t, parent.RenameNoReplace("source", "published"))
	_, err = os.Lstat(filepath.Join(parentPath, "source"))
	require.ErrorIs(t, err, os.ErrNotExist)
	publishedInfo, err := os.Stat(filepath.Join(parentPath, "published"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), publishedInfo.Sys().(*syscall.Stat_t).Nlink)
}

func TestDirectoryTypedEntryReadDirRenameAndRevocation(t *testing.T) {
	parentPath := t.TempDir()
	parent, err := OpenDirectory(parentPath)
	require.NoError(t, err)

	name, file, err := parent.CreateTemp(".private-", 0o600)
	require.NoError(t, err)
	info, err := file.Stat()
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	require.NoError(t, file.Close())

	entry, err := parent.Entry(name)
	require.NoError(t, err)
	require.True(t, entry.Valid())
	require.Equal(t, name, entry.Name())
	require.Equal(t, filepath.Join(parentPath, name), entry.DisplayPath())
	exists, err := entry.Exists()
	require.NoError(t, err)
	require.True(t, exists)
	_, err = entry.Lstat()
	require.NoError(t, err)
	opened, err := entry.OpenFile(os.O_RDONLY, 0)
	require.NoError(t, err)
	require.NoError(t, opened.Close())
	require.NoError(t, entry.SyncParent())

	entries, err := parent.ReadDir(1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	self, err := parent.OpenSelf()
	require.NoError(t, err)
	selfInfo, err := self.Stat()
	require.NoError(t, err)
	require.True(t, selfInfo.IsDir())
	require.NoError(t, self.Close())

	require.NoError(t, os.WriteFile(filepath.Join(parentPath, "replacement"), []byte("new"), 0o600))
	require.NoError(t, parent.Rename(name, "replacement"))
	contents, err := os.ReadFile(filepath.Join(parentPath, "replacement"))
	require.NoError(t, err)
	require.Empty(t, contents)

	require.NoError(t, parent.Close())
	require.NoError(t, parent.Close())
	require.ErrorIs(t, parent.Sync(), ErrDirectoryClosed)
	_, err = parent.OpenFile("replacement", os.O_RDONLY, 0)
	require.ErrorIs(t, err, ErrDirectoryClosed)
	_, err = parent.ReadDir(1)
	require.ErrorIs(t, err, ErrDirectoryClosed)

	var zero Entry
	require.False(t, zero.Valid())
	_, err = zero.OpenFile(os.O_RDONLY, 0)
	require.True(t, errors.Is(err, ErrInvalidEntryName))

	var nilDirectory *Directory
	_, err = nilDirectory.OpenFile("entry", os.O_RDONLY, 0)
	require.ErrorIs(t, err, ErrDirectoryClosed)
	_, _, err = nilDirectory.CreateTemp(".private-", 0o600)
	require.ErrorIs(t, err, ErrDirectoryClosed)
	require.ErrorIs(t, nilDirectory.LinkNoReplace("a", "b"), ErrDirectoryClosed)
	require.ErrorIs(t, nilDirectory.RenameNoReplace("a", "b"), ErrDirectoryClosed)
	require.ErrorIs(t, nilDirectory.Rename("a", "b"), ErrDirectoryClosed)
	require.ErrorIs(t, nilDirectory.Remove("entry"), ErrDirectoryClosed)
}
