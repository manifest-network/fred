//go:build linux

package at

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestParseName(t *testing.T) {
	for _, name := range []string{"a", "anchor", "...", ".a", "a.", " ", "\xff", strings.Repeat("n", MaxNameLen)} {
		parsed, ok := ParseName(name)
		require.True(t, ok, "%q", name)
		require.Equal(t, name, parsed.String())
		require.True(t, parsed.Valid())
	}
	for _, name := range []string{
		"", ".", "..", "a/b", "/", "/abs", "trailing/", "../up", "nul\x00byte", strings.Repeat("n", MaxNameLen+1),
	} {
		parsed, ok := ParseName(name)
		require.False(t, ok, "%q", name)
		require.Equal(t, Name{}, parsed, "a refused name yields the zero Name")
	}
	require.False(t, Name{}.Valid())
	require.Empty(t, Name{}.String())
}

// An Identity carries a mount ID or does not exist: a statx result without
// one is refused, so nothing is ever compared by device alone.
func TestIdentityRequiresTheMountID(t *testing.T) {
	full := unix.Statx_t{
		Mask:      unix.STATX_TYPE | unix.STATX_INO | unix.STATX_MNT_ID,
		Mode:      unix.S_IFDIR | 0o755,
		Ino:       42,
		Mnt_id:    7,
		Dev_major: 8,
		Dev_minor: 1,
	}
	id, err := identityOf(&full)
	require.NoError(t, err)
	require.True(t, id.IsDir())
	require.Equal(t, uint64(42), id.Ino())
	require.True(t, id.Same(id))

	withoutMount := full
	withoutMount.Mask &^= unix.STATX_MNT_ID
	_, err = identityOf(&withoutMount)
	require.ErrorIs(t, err, ErrNoMountID)

	for _, missing := range []uint32{unix.STATX_TYPE, unix.STATX_INO} {
		partial := full
		partial.Mask &^= missing
		_, err = identityOf(&partial)
		require.ErrorIs(t, err, ErrIncompleteStat)
	}

	// Same device, other mount: a bind mount of the same filesystem.
	bind := full
	bind.Mnt_id = 8
	other, err := identityOf(&bind)
	require.NoError(t, err)
	require.False(t, id.SameMount(other))
	require.False(t, id.Same(other))
}

// The zero Identity, which no stat produced, shares a mount with nothing,
// itself included, so a forgotten identity can never pass a mount check.
func TestZeroIdentityMatchesNothing(t *testing.T) {
	var zero Identity
	require.False(t, zero.SameMount(zero))
	require.False(t, zero.Same(zero))
	require.False(t, zero.IsDir())

	borrowed(t, tempDir(t), func(d *Dir) {
		id, err := d.Stat()
		require.NoError(t, err)
		require.False(t, id.SameMount(zero))
		require.False(t, zero.SameMount(id))
	})
}

// Pins an assumption about the running kernel: statx reports mount IDs
// (Linux 5.8 and later). fstree refuses every traversal without them, so a
// kernel that stops reporting them must fail here, never skip.
func TestKernelReportsMountIDs(t *testing.T) {
	parentPath := tempDir(t)
	require.NoError(t, os.Mkdir(filepath.Join(parentPath, "child"), 0o755))
	borrowed(t, parentPath, func(d *Dir) {
		id, err := d.Stat()
		require.NoError(t, err, "statx must report the mount ID")
		child, err := d.StatChild(mustName(t, "child"))
		require.NoError(t, err)
		require.True(t, id.SameMount(child))
		require.False(t, id.Same(child))
	})
}

// Close poisons a Dir: every later use fails with ErrClosed, never with an
// errno a caller could read as an answer about the tree.
func TestDirCloseIsFinal(t *testing.T) {
	parentPath := tempDir(t)
	require.NoError(t, os.Mkdir(filepath.Join(parentPath, "dir"), 0o755))
	d := openOwned(t, parentPath, "dir")
	writeFile(t, filepath.Join(parentPath, "dir", "f"), "kept")
	reader := NewReader()
	listed, _, _, err := d.ReadBatch(&reader, 0)
	require.NoError(t, err)
	require.Len(t, listed, 1)

	d.Close()
	d.Close()
	name := mustName(t, "f")
	_, err = d.OpenChild(name)
	require.ErrorIs(t, err, ErrClosed)
	_, err = d.OpenParent()
	require.ErrorIs(t, err, ErrClosed)
	_, err = d.Dup()
	require.ErrorIs(t, err, ErrClosed)
	_, err = d.Stat()
	require.ErrorIs(t, err, ErrClosed)
	_, err = d.StatChild(name)
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, d.Unlink(name), ErrClosed)
	require.ErrorIs(t, d.Rmdir(name), ErrClosed)
	_, _, _, err = d.ReadBatch(&reader, 0)
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, listed[0].Unlink(), ErrClosed, "an entry outlives no Close of its directory")
	require.FileExists(t, filepath.Join(parentPath, "dir", "f"))

	var nilDir *Dir
	nilDir.Close()
	_, err = nilDir.Stat()
	require.ErrorIs(t, err, ErrClosed)
	_, err = View{}.Stat()
	require.ErrorIs(t, err, ErrClosed)
}

// A borrowed Dir never closes the caller's file, even when closed itself,
// and is poisoned once the borrow ends.
func TestBorrowNeverClosesTheCallersFile(t *testing.T) {
	parentPath := tempDir(t)
	writeFile(t, filepath.Join(parentPath, "f"), "")
	f := openDir(t, parentPath)

	var kept *Dir
	require.NoError(t, Borrow(f, func(d *Dir) {
		_, err := d.Stat()
		require.NoError(t, err)
		d.Close()
		_, err = d.Stat()
		require.ErrorIs(t, err, ErrClosed)
		kept = d
	}))
	names, err := f.Readdirnames(-1)
	require.NoError(t, err, "the caller's file is still open")
	require.Equal(t, []string{"f"}, names)

	require.NoError(t, Borrow(f, func(d *Dir) { kept = d }))
	_, err = kept.Stat()
	require.ErrorIs(t, err, ErrClosed, "a Dir kept past its borrow is poisoned")

	var keptView View
	require.NoError(t, BorrowView(f, func(v View) { keptView = v }))
	_, err = keptView.Stat()
	require.ErrorIs(t, err, ErrClosed)

	closed, err := os.Open(parentPath)
	require.NoError(t, err)
	require.NoError(t, closed.Close())
	called := false
	require.Error(t, Borrow(closed, func(*Dir) { called = true }))
	require.False(t, called, "a closed file is never used by number")
	require.Error(t, Borrow(nil, func(*Dir) { called = true }))
	require.False(t, called)
}

// The zero Name is refused before any syscall. unlinkat with an empty name
// fails with ENOENT, which a caller would read as "already gone".
func TestZeroNameIsRefusedBeforeAnySyscall(t *testing.T) {
	parentPath := tempDir(t)
	require.NoError(t, os.Mkdir(filepath.Join(parentPath, "dir"), 0o755))
	writeFile(t, filepath.Join(parentPath, "dir", "f"), "")
	d := openOwned(t, parentPath, "dir")

	var zero Name
	_, err := d.OpenChild(zero)
	require.ErrorIs(t, err, ErrZeroName)
	_, err = d.StatChild(zero)
	require.ErrorIs(t, err, ErrZeroName)
	require.ErrorIs(t, d.Unlink(zero), ErrZeroName)
	require.ErrorIs(t, d.Rmdir(zero), ErrZeroName)
	_, err = d.View().OpenChild(zero)
	require.ErrorIs(t, err, ErrZeroName)

	reader := NewReader()
	listed, _, _, err := d.ReadBatch(&reader, 0)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.ErrorIs(t, listed[0].RenameNoReplaceInto(d, zero), ErrZeroName)
	require.FileExists(t, filepath.Join(parentPath, "dir", "f"))
}

// A listed entry acts only inside the directory it was listed from: a
// same-named entry elsewhere is untouched. The zero Listed acts nowhere.
func TestListedStaysBoundToItsDirectory(t *testing.T) {
	parentPath := tempDir(t)
	for _, dir := range []string{"a", "b"} {
		require.NoError(t, os.MkdirAll(filepath.Join(parentPath, dir, "sub"), 0o755))
		writeFile(t, filepath.Join(parentPath, dir, "f"), dir)
	}
	a := openOwned(t, parentPath, "a")
	reader := NewReader()
	listed, _, _, err := a.ReadBatch(&reader, 0)
	require.NoError(t, err)
	byName := map[string]Listed{}
	for _, entry := range listed {
		byName[entry.String()] = entry
	}

	require.NoError(t, byName["f"].Unlink())
	require.ErrorIs(t, byName["sub"].Unlink(), unix.EISDIR)
	sub, err := byName["sub"].OpenDir()
	require.NoError(t, err)
	subID, err := sub.Stat()
	sub.Close()
	require.NoError(t, err)
	require.True(t, subID.IsDir())
	require.NoError(t, byName["sub"].Rmdir())

	_, err = os.Lstat(filepath.Join(parentPath, "a", "f"))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoDirExists(t, filepath.Join(parentPath, "a", "sub"))
	require.FileExists(t, filepath.Join(parentPath, "b", "f"))
	require.DirExists(t, filepath.Join(parentPath, "b", "sub"))

	var zero Listed
	require.ErrorIs(t, zero.Unlink(), ErrClosed)
	require.ErrorIs(t, zero.Rmdir(), ErrClosed)
	_, err = zero.OpenDir()
	require.ErrorIs(t, err, ErrClosed)
	require.ErrorIs(t, zero.RenameNoReplaceInto(a, mustName(t, "x")), ErrClosed)
	var zeroView ListedView
	_, _, err = zeroView.Type()
	require.ErrorIs(t, err, ErrClosed)
	_, err = zeroView.OpenDir()
	require.ErrorIs(t, err, ErrClosed)
}

// RenameNoReplaceInto moves the entry into another directory and never
// replaces what is already there.
func TestListedRenameNoReplaceInto(t *testing.T) {
	parentPath := tempDir(t)
	require.NoError(t, os.MkdirAll(filepath.Join(parentPath, "src", "moved"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(parentPath, "dst", "taken"), 0o755))
	writeFile(t, filepath.Join(parentPath, "dst", "taken", "kept"), "kept")
	src := openOwned(t, parentPath, "src")
	dst := openOwned(t, parentPath, "dst")
	reader := NewReader()
	listed, _, _, err := src.ReadBatch(&reader, 0)
	require.NoError(t, err)
	require.Len(t, listed, 1)

	require.ErrorIs(t, listed[0].RenameNoReplaceInto(dst, mustName(t, "taken")), unix.EEXIST)
	require.FileExists(t, filepath.Join(parentPath, "dst", "taken", "kept"))
	require.NoError(t, listed[0].RenameNoReplaceInto(dst, mustName(t, "fresh")))
	require.DirExists(t, filepath.Join(parentPath, "dst", "fresh"))
	require.NoDirExists(t, filepath.Join(parentPath, "src", "moved"))
}

// A listing that does not say the type (DT_UNKNOWN, or a value this package
// does not know) is resolved without following a symlink; an entry that
// vanished first is reported absent, not as an error.
func TestListedViewType(t *testing.T) {
	parentPath := tempDir(t)
	require.NoError(t, os.MkdirAll(filepath.Join(parentPath, "anchor", "dir"), 0o755))
	writeFile(t, filepath.Join(parentPath, "anchor", "file"), "x")
	require.NoError(t, os.Symlink("dir", filepath.Join(parentPath, "anchor", "link")))
	anchor := openOwned(t, parentPath, "anchor")

	for name, want := range map[string]uint8{"dir": unix.DT_DIR, "file": unix.DT_REG, "link": unix.DT_LNK} {
		for _, listed := range []uint8{unix.DT_UNKNOWN, 3, 0xff} {
			entry := ListedView{dir: anchor, ent: dirent{name: name, typ: listed}}
			typ, present, err := entry.Type()
			require.NoError(t, err)
			require.True(t, present)
			require.Equal(t, want, typ, "%s listed as %d", name, listed)
		}
	}
	_, present, err := ListedView{dir: anchor, ent: dirent{name: "vanished", typ: unix.DT_UNKNOWN}}.Type()
	require.NoError(t, err)
	require.False(t, present)
	typ, present, err := ListedView{dir: anchor, ent: dirent{name: "anything", typ: unix.DT_SOCK}}.Type()
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, uint8(unix.DT_SOCK), typ, "a known listed type is taken as is")

	// O_DIRECTORY|O_NOFOLLOW on a symlink fails with ENOTDIR (the directory
	// check comes first) or ELOOP; either way it is not followed.
	link := ListedView{dir: anchor, ent: dirent{name: "link", typ: unix.DT_LNK}}
	opened, err := link.OpenDir()
	require.True(t, errors.Is(err, unix.ENOTDIR) || errors.Is(err, unix.ELOOP), "a symlink is never followed: %v", err)
	require.Equal(t, View{}, opened)
}

// OpenParent reaches the directory a child was opened from.
func TestOpenParentReachesTheParent(t *testing.T) {
	parentPath := tempDir(t)
	require.NoError(t, os.MkdirAll(filepath.Join(parentPath, "dir", "child"), 0o755))
	dir := openOwned(t, parentPath, "dir")
	child, err := dir.OpenChild(mustName(t, "child"))
	require.NoError(t, err)
	defer child.Close()
	up, err := child.OpenParent()
	require.NoError(t, err)
	defer up.Close()
	dirID, err := dir.Stat()
	require.NoError(t, err)
	upID, err := up.Stat()
	require.NoError(t, err)
	require.True(t, dirID.Same(upID))

	view, err := child.View().OpenParent()
	require.NoError(t, err)
	defer view.Close()
	viewID, err := view.Stat()
	require.NoError(t, err)
	require.True(t, dirID.Same(viewID))
}
