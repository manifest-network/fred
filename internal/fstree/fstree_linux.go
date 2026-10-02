//go:build linux

package fstree

import (
	"context"
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/fstree/internal/at"
)

const (
	// maxDepth bounds the ancestry RemoveBeneath and WalkBeneath keep:
	// 65,536 levels, at most 512 KiB of inode numbers for a removal and 1 MiB
	// of (inode, offset) frames for a walk. A removal cuts a deeper tree; a
	// walk returns ErrTooDeep.
	maxDepth = 1 << 16
	// errorNameLen bounds how much of an entry name an error message carries.
	errorNameLen = 64
)

// fstreeError is the type of this package's sentinel errors. The sentinels
// are constants of it, so no importer can reassign them.
type fstreeError string

func (e fstreeError) Error() string { return string(e) }

const (
	// ErrTreeChanged means the tree changed under the traversal: a parent
	// reached on the way up was not the directory recorded on the way down,
	// a directory the traversal held was removed, the parent's name no longer
	// binds the emptied anchor, the anchor gained entries after it was
	// emptied, or two iterations in a row made no progress.
	ErrTreeChanged = fstreeError("fstree: tree changed during traversal")
	// ErrCrossDevice means the entry, or a directory inside it, lies on
	// another filesystem or mount than its parent, or that this cannot be
	// established because the kernel does not report mount IDs (Linux 5.8 or
	// later is required). Either way nothing inside the entry is touched.
	ErrCrossDevice = fstreeError("fstree: tree crosses a filesystem boundary")
	// ErrCutRefused means RemoveBeneath reached its depth bound and could
	// not move the deeper subtree into the anchor: BeforeFirstCut failed, or
	// the rename failed with EXDEV, EDQUOT, ENOSPC or EMLINK.
	ErrCutRefused = fstreeError("fstree: depth bound reached and the subtree could not be moved")
	// ErrUndeletable means an entry cannot be removed: an unlink, rmdir or
	// cut rename, or the open of a directory to empty it, failed with EPERM,
	// EACCES, EBUSY (a mount point, for example) or EROFS.
	ErrUndeletable = fstreeError("fstree: entry cannot be removed")
	// ErrTooDeep means WalkBeneath met a directory deeper than its bound.
	ErrTooDeep = fstreeError("fstree: tree deeper than the walk bound")
	// ErrInvalidName means a name is not a single path component: 1 to 255
	// bytes, free of '/' and NUL, and neither "." nor "..". ParseName returns
	// it, and RemoveBeneath and WalkBeneath return it for the zero Name.
	ErrInvalidName = fstreeError("fstree: invalid entry name")
)

// BorrowedDir is a directory fstree lends to BeforeFirstCut or to a Visitor
// method for the duration of that one call. It is never a bare descriptor:
// the descriptor is reachable only inside Control, and only while the call
// it was lent for runs. Once that call returns, Control and Stat fail without
// touching any descriptor, so a BorrowedDir kept past its call, or handed to
// a goroutine that outlives it, cannot reach a descriptor number the kernel
// has since reused.
//
// The zero BorrowedDir is never valid.
type BorrowedDir struct {
	lent at.Borrowed
}

// Control runs fn with the directory's descriptor and returns fn's error, in
// the manner of syscall.RawConn's Control. It fails without running fn once
// the call the directory was lent for has returned.
//
// The descriptor is fstree's, valid only while fn runs. fn must not close
// it, keep it, or wrap it with os.NewFile, whose finalizer would close it at
// some later garbage collection, after the number may belong to another
// descriptor. Nor may fn rely on the descriptor's file offset.
func (d BorrowedDir) Control(fn func(fd int) error) error {
	return d.lent.Control(fn)
}

// Stat returns fstat(2) of the directory, under the same rules as Control.
func (d BorrowedDir) Stat() (unix.Stat_t, error) {
	return d.lent.Stat()
}

func checkCall(ctx context.Context, parent *os.File, name Name, limit int) error {
	switch {
	case ctx == nil:
		return errors.New("fstree: nil context")
	case parent == nil:
		return errors.New("fstree: nil parent directory")
	case !name.Valid():
		// Only the zero Name gets here: ParseName built every other one.
		return fmt.Errorf("%w: the zero Name", ErrInvalidName)
	case limit < 1:
		return fmt.Errorf("fstree: depth bound %d is below 1", limit)
	}
	return nil
}

// parentError reports that the caller's parent directory could not be used:
// it is closed, or has no descriptor.
func parentError(err error) error {
	return fmt.Errorf("fstree: parent directory: %w", err)
}

// shortName bounds how much of an entry name an error message carries, so a
// long tenant-chosen name cannot inflate logs or diagnostics.
func shortName(name string) string {
	if len(name) <= errorNameLen {
		return name
	}
	return name[:errorNameLen] + "..."
}

// undeletable reports whether a failed mutation means the entry cannot be
// removed as things stand: a permission, an immutable or append-only flag, a
// mount point, or a read-only filesystem.
func undeletable(err error) bool {
	return errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) ||
		errors.Is(err, unix.EBUSY) || errors.Is(err, unix.EROFS)
}

// classify maps a failed mutation of the entry name at depth to an error.
// EPERM, EACCES, EBUSY and EROFS wrap ErrUndeletable; anything else goes to
// opError. Either way the message names one bounded entry name, never a
// path.
func classify(op, name string, depth int, err error) error {
	if undeletable(err) {
		return fmt.Errorf("%w: %s %q at depth %d: %w", ErrUndeletable, op, shortName(name), depth, err)
	}
	return opError(op, name, depth, err)
}

// classifyDir is classify for an operation on the directory at depth itself,
// whose name the traversal does not keep.
func classifyDir(op string, depth int, err error) error {
	if undeletable(err) {
		return fmt.Errorf("%w: %s the directory at depth %d: %w", ErrUndeletable, op, depth, err)
	}
	return dirError(op, depth, err)
}

// opError describes a failed operation on the entry name at depth. A stat
// that failed because the kernel reports no mount IDs is ErrCrossDevice:
// whether the entry shares the anchor's mount cannot be established, and
// fstree refuses rather than compare devices alone.
func opError(op, name string, depth int, err error) error {
	if errors.Is(err, at.ErrNoMountID) {
		return fmt.Errorf("%w: %s %q at depth %d: %w", ErrCrossDevice, op, shortName(name), depth, err)
	}
	return fmt.Errorf("fstree: %s %q at depth %d: %w", op, shortName(name), depth, err)
}

// dirError is opError for the directory at depth itself.
func dirError(op string, depth int, err error) error {
	if errors.Is(err, at.ErrNoMountID) {
		return fmt.Errorf("%w: %s the directory at depth %d: %w", ErrCrossDevice, op, depth, err)
	}
	return fmt.Errorf("fstree: %s the directory at depth %d: %w", op, depth, err)
}

// readError maps a failed read of the directory at depth. Reading a
// directory removed while the traversal held it fails with ENOENT, and
// reads that keep returning only the dot entries mean the directory keeps
// changing; both are ErrTreeChanged.
func readError(depth int, err error) error {
	switch {
	case errors.Is(err, unix.ENOENT):
		return removedWhileHeld(depth)
	case errors.Is(err, at.ErrOnlyDots):
		return fmt.Errorf("%w: at depth %d: %w", ErrTreeChanged, depth, err)
	default:
		return dirError("read", depth, err)
	}
}

// removedWhileHeld reports that the directory at depth was removed while the
// traversal held it open: reading it, or opening its parent, fails with
// ENOENT.
func removedWhileHeld(depth int) error {
	return fmt.Errorf("%w: the directory at depth %d was removed while held", ErrTreeChanged, depth)
}

// movedError reports that the parent reached from the directory at depth is
// not the directory it was entered from.
func movedError(depth int) error {
	return fmt.Errorf("%w: the parent of the directory at depth %d is not the directory it was entered from",
		ErrTreeChanged, depth)
}

// crossDevice reports that the directory name at depth lies on another
// filesystem or mount.
func crossDevice(name string, depth int) error {
	return fmt.Errorf("%w: %q at depth %d", ErrCrossDevice, shortName(name), depth)
}
