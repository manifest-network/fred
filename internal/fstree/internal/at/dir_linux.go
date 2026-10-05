//go:build linux

package at

import (
	"os"

	"golang.org/x/sys/unix"
)

// atError is the type of this package's errors. Its values are constants,
// so nothing can reassign them.
type atError string

func (e atError) Error() string { return string(e) }

const (
	// ErrClosed means a Dir, View, Listed or ListedView was used after its
	// directory was closed, or after the borrow it came from ended. It is
	// never a filesystem answer such as ENOENT, so a caller cannot mistake it
	// for an absent entry.
	ErrClosed = atError("fstree: directory used after it was closed")
	// ErrZeroName means the zero Name was passed where a Name is needed. It
	// is refused before any syscall: unlinkat with an empty name would fail
	// with ENOENT, which reads as an entry already gone.
	ErrZeroName = atError("fstree: the zero Name names no entry")
	// ErrNoMountID means the kernel did not report a mount ID, as kernels
	// before Linux 5.8 do not. Without one, two objects on the same device
	// cannot be told apart from a bind mount of it, so no Identity is made.
	ErrNoMountID = atError("fstree: statx reported no mount ID (needs Linux 5.8+)")
	// ErrIncompleteStat means the filesystem did not report the file type and
	// inode.
	ErrIncompleteStat = atError("fstree: the filesystem did not report the file type and inode")
)

// Identity is what fstree compares to decide that two descriptors or names
// denote the same directory, or lie on the same mount. Only a successful stat
// of this package makes one, and every one it makes carries the device, the
// inode and the mount ID. The zero Identity matches nothing, itself included.
type Identity struct {
	dev  uint64 // device major and minor
	ino  uint64
	mnt  uint64 // mount ID
	mode uint16 // S_IFMT bits
	made bool   // set only by identityOf
}

// identityOf turns a statx result into an Identity. A result without the
// file type and inode is ErrIncompleteStat, and one without the mount ID is
// ErrNoMountID: an identity compared by device alone would let a bind mount
// of the same filesystem pass for the directory it covers.
func identityOf(st *unix.Statx_t) (Identity, error) {
	const required = unix.STATX_TYPE | unix.STATX_INO
	if st.Mask&required != required {
		return Identity{}, ErrIncompleteStat
	}
	if st.Mask&unix.STATX_MNT_ID == 0 {
		return Identity{}, ErrNoMountID
	}
	return Identity{
		dev:  uint64(st.Dev_major)<<32 | uint64(st.Dev_minor),
		ino:  st.Ino,
		mnt:  st.Mnt_id,
		mode: st.Mode & unix.S_IFMT,
		made: true,
	}, nil
}

// SameMount reports whether b lies on a's filesystem and mount. Both must
// come from a stat: the zero Identity shares no mount with anything.
func (a Identity) SameMount(b Identity) bool {
	return a.made && b.made && a.dev == b.dev && a.mnt == b.mnt
}

// Same reports whether a and b denote the same inode on the same mount.
func (a Identity) Same(b Identity) bool {
	return a.SameMount(b) && a.ino == b.ino
}

// Ino returns the inode number.
func (a Identity) Ino() uint64 { return a.ino }

// IsDir reports whether a is a directory.
func (a Identity) IsDir() bool { return a.made && a.mode == unix.S_IFDIR }

// DirentType maps the file type to the matching d_type value.
func (a Identity) DirentType() uint8 {
	switch a.mode {
	case unix.S_IFIFO:
		return unix.DT_FIFO
	case unix.S_IFCHR:
		return unix.DT_CHR
	case unix.S_IFDIR:
		return unix.DT_DIR
	case unix.S_IFBLK:
		return unix.DT_BLK
	case unix.S_IFREG:
		return unix.DT_REG
	case unix.S_IFLNK:
		return unix.DT_LNK
	case unix.S_IFSOCK:
		return unix.DT_SOCK
	default:
		return unix.DT_UNKNOWN
	}
}

// Dir is an open directory. Its descriptor is unexported and was opened with
// fixed flags: read-only, O_DIRECTORY, O_NOFOLLOW, O_NONBLOCK and O_CLOEXEC.
// Every method that takes a Name resolves it inside this directory and
// nowhere else.
//
// Close poisons a Dir: afterwards every method, and every method of a Listed
// it produced, fails with ErrClosed. A stale Dir therefore never acts on a
// descriptor number the kernel has since handed to something else.
//
// A Dir is not safe for concurrent use.
type Dir struct {
	fd int // -1 once closed
	// borrowed marks the descriptor of a caller's *os.File: Close forgets it
	// and never closes it.
	borrowed bool
}

// Borrow runs fn with f's directory as a Dir that is valid only while fn
// runs. f keeps its descriptor: the Dir never closes it, and after fn
// returns the Dir is poisoned, so a Dir retained past the call fails with
// ErrClosed. f is held open for the whole call, even against a concurrent
// Close. Borrow's error is f's (nil, closed, or not backed by a descriptor),
// never fn's; fn reports its own results through its closure.
func Borrow(f *os.File, fn func(*Dir)) error {
	return control(f, func(fd int) {
		d := &Dir{fd: fd, borrowed: true}
		defer d.Close()
		fn(d)
	})
}

// BorrowView is Borrow for a caller that needs only the read-only View.
func BorrowView(f *os.File, fn func(View)) error {
	return Borrow(f, func(d *Dir) { fn(View{d: d}) })
}

// fdOf returns d's descriptor, or ErrClosed once d was closed.
func (d *Dir) fdOf() (int, error) {
	if d == nil || d.fd < 0 {
		return -1, ErrClosed
	}
	return d.fd, nil
}

// Close closes the directory, unless it was borrowed, and poisons d either
// way. It is safe to call more than once, and on a nil Dir.
func (d *Dir) Close() {
	if d == nil || d.fd < 0 {
		return
	}
	if !d.borrowed {
		closeFD(d.fd)
	}
	d.fd = -1
}

// View returns the read-only side of d. The View shares d's descriptor:
// closing either closes both.
func (d *Dir) View() View { return View{d: d} }

// OpenChild opens the directory name inside d. It never follows a symlink:
// a symlink fails with ELOOP, and a non-directory with ENOTDIR.
func (d *Dir) OpenChild(name Name) (*Dir, error) {
	fd, err := d.fdOf()
	if err != nil {
		return nil, err
	}
	if !name.Valid() {
		return nil, ErrZeroName
	}
	child, err := openDirAt(fd, name.s)
	if err != nil {
		return nil, err
	}
	return &Dir{fd: child}, nil
}

// OpenParent opens d's parent directory. It is the only way to reach above a
// directory: no Name can, and no other method passes "..". The caller must
// verify the parent's Identity before trusting it, because the directory may
// have moved since it was entered.
func (d *Dir) OpenParent() (*Dir, error) {
	fd, err := d.fdOf()
	if err != nil {
		return nil, err
	}
	up, err := openDirAt(fd, "..")
	if err != nil {
		return nil, err
	}
	return &Dir{fd: up}, nil
}

// Dup returns a second, independently closed Dir for the same open
// directory.
func (d *Dir) Dup() (*Dir, error) {
	fd, err := d.fdOf()
	if err != nil {
		return nil, err
	}
	dup, err := dupFD(fd)
	if err != nil {
		return nil, err
	}
	return &Dir{fd: dup}, nil
}

// Stat describes the directory itself.
func (d *Dir) Stat() (Identity, error) {
	fd, err := d.fdOf()
	if err != nil {
		return Identity{}, err
	}
	return statFD(fd)
}

// StatChild describes name inside d without following a final symlink.
func (d *Dir) StatChild(name Name) (Identity, error) {
	fd, err := d.fdOf()
	if err != nil {
		return Identity{}, err
	}
	if !name.Valid() {
		return Identity{}, ErrZeroName
	}
	return statEntry(fd, name.s)
}

// Unlink removes the non-directory name inside d. On a directory it fails
// with EISDIR; on a symlink it removes the link.
func (d *Dir) Unlink(name Name) error {
	fd, err := d.fdOf()
	if err != nil {
		return err
	}
	if !name.Valid() {
		return ErrZeroName
	}
	return unlinkEntry(fd, name.s)
}

// Rmdir removes the empty directory name inside d.
func (d *Dir) Rmdir(name Name) error {
	fd, err := d.fdOf()
	if err != nil {
		return err
	}
	if !name.Valid() {
		return ErrZeroName
	}
	return removeDir(fd, name.s)
}

// View is the read-only side of a Dir. It lists the directory, describes it
// and its entries, and opens its children and its parent as further Views;
// it has no method that changes the filesystem, and no way back to the Dir.
// Code that reaches the tree only through Views and ListedViews, as fstree's
// walker does, cannot change it: the compiler, not review, rules the call
// out.
//
// The zero View is closed.
type View struct {
	d *Dir
}

// OpenChild is (*Dir).OpenChild, returning a View.
func (v View) OpenChild(name Name) (View, error) {
	child, err := v.d.OpenChild(name)
	if err != nil {
		return View{}, err
	}
	return View{d: child}, nil
}

// OpenParent is (*Dir).OpenParent, returning a View.
func (v View) OpenParent() (View, error) {
	up, err := v.d.OpenParent()
	if err != nil {
		return View{}, err
	}
	return View{d: up}, nil
}

// Stat describes the directory itself.
func (v View) Stat() (Identity, error) { return v.d.Stat() }

// StatChild describes name inside the directory without following a final
// symlink.
func (v View) StatChild(name Name) (Identity, error) { return v.d.StatChild(name) }

// Close closes the directory, as (*Dir).Close does. It is safe on the zero
// View.
func (v View) Close() { v.d.Close() }
