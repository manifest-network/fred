//go:build linux

package fstree

import (
	"context"
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

const (
	// maxDepth bounds the ancestry RemoveBeneath and WalkBeneath keep:
	// 65,536 levels, at most 512 KiB of inode numbers for a removal and 1 MiB
	// of (inode, offset) frames for a walk. A removal cuts a deeper tree; a
	// walk returns ErrTooDeep.
	maxDepth = 1 << 16
	// maxNameLen is NAME_MAX, the longest single path component Linux
	// filesystems accept.
	maxNameLen = 255
	// errorNameLen bounds how much of an entry name an error message carries.
	errorNameLen = 64
)

var (
	// ErrTreeChanged means the tree changed under the traversal: a ".."
	// ascent reached a directory other than the recorded ancestor, a
	// directory the traversal held was removed, the parent's name no longer
	// binds the emptied anchor, the anchor gained entries after it was
	// emptied, or two iterations in a row made no progress.
	ErrTreeChanged = errors.New("fstree: tree changed during traversal")
	// ErrCrossDevice means the entry, or a directory inside it, lies on
	// another filesystem or mount than its parent.
	ErrCrossDevice = errors.New("fstree: tree crosses a filesystem boundary")
	// ErrCutRefused means RemoveBeneath reached its depth bound and could
	// not move the deeper subtree into the anchor: BeforeFirstCut failed, or
	// the rename failed with EXDEV, EDQUOT, ENOSPC or EMLINK.
	ErrCutRefused = errors.New("fstree: depth bound reached and the subtree could not be moved")
	// ErrUndeletable means an entry cannot be removed: an unlink, rmdir or
	// cut rename, or the open of a directory to empty it, failed with EPERM,
	// EACCES, EBUSY (a mount point, for example) or EROFS.
	ErrUndeletable = errors.New("fstree: entry cannot be removed")
	// ErrTooDeep means WalkBeneath met a directory deeper than its bound.
	ErrTooDeep = errors.New("fstree: tree deeper than the walk bound")
	// ErrInvalidName means a name is not a single path component: 1 to 255
	// bytes, free of '/' and NUL, and neither "." nor "..". ParseName returns
	// it, and RemoveBeneath and WalkBeneath return it for the zero Name.
	ErrInvalidName = errors.New("fstree: invalid entry name")
)

// RemoveOptions configures RemoveBeneath.
type RemoveOptions struct {
	// BeforeFirstCut, when set, runs on the anchor (the opened top
	// directory) immediately before the call's first cut, and at most once
	// per call. A removal that needs no cut, because the tree is no deeper
	// than the depth bound, never calls it. When it fails, that cut is
	// refused: RemoveBeneath stops with an error wrapping both ErrCutRefused
	// and the hook's error, and keeps everything it has not yet removed.
	//
	// It exists so that XFS volume deletion can detach the condemned anchor
	// from its quota project only when a cut makes that necessary, after
	// which a cut into the anchor cannot fail with EXDEV or EDQUOT. It must
	// be idempotent, because a rerun calls it again at its own first cut. The
	// descriptor is valid only during the call, which must neither close it
	// nor rely on its file offset.
	BeforeFirstCut func(anchorFD int) error
}

// RemoveReport counts what one RemoveBeneath call removed. A failed call
// reports what it removed before it stopped.
type RemoveReport struct {
	// Entries counts the non-directory entries unlinked.
	Entries uint64
	// Dirs counts the directories removed, the anchor included.
	Dirs uint64
	// Cuts counts the subtrees moved into the anchor because they lay
	// deeper than the depth bound. Any cut means the tree had more than
	// 65,536 levels.
	Cuts uint64
	// MaxDepth is the deepest directory level entered; the anchor is
	// level 0.
	MaxDepth int
}

// RemoveBeneath removes the entry name inside the directory parent and, when
// it is a directory, everything beneath it. It
// holds at most three descriptors of its own, keeps at most 65,536 levels of
// ancestry (512 KiB) plus one directory batch, and does not recurse; it never
// follows symlinks, never crosses a mount, and never ascends above the entry.
// The package documentation describes the algorithm, its invariants and its
// resource bounds.
//
// Precondition: no other actor mutates the tree concurrently (its writers
// are stopped). A concurrent rename is detected (ErrTreeChanged), not
// prevented.
//
// An absent name counts as removed. A name that is not a directory,
// including a symlink to one, is unlinked without being opened. On failure
// RemoveBeneath stops and leaves in place everything it has not removed, and
// a rerun continues from there. The error wraps ErrTreeChanged,
// ErrCrossDevice, ErrCutRefused (alone, or with BeforeFirstCut's error),
// ErrUndeletable, ErrInvalidName, the context's error, or the failing
// syscall's errno.
//
// parent must stay open for the duration of the call, and BeforeFirstCut
// must not close it.
func RemoveBeneath(ctx context.Context, parent *os.File, name Name, opts RemoveOptions) (RemoveReport, error) {
	return removeBeneath(ctx, parent, name, opts, maxDepth)
}

// removeBeneath is RemoveBeneath with the depth bound as a parameter, so a
// white-box test can reach the cut path without building 65,536 levels.
func removeBeneath(
	ctx context.Context,
	parent *os.File,
	name Name,
	opts RemoveOptions,
	limit int,
) (RemoveReport, error) {
	if err := checkCall(ctx, parent, name, limit); err != nil {
		return RemoveReport{}, err
	}
	var (
		report RemoveReport
		runErr error
	)
	err := withFD(parent, func(pfd int) {
		r := newRemover(ctx, pfd, name, opts, limit)
		defer r.release()
		runErr = r.run()
		report = r.report
	})
	if err != nil {
		return report, err
	}
	return report, runErr
}

// Visitor receives what WalkBeneath visits. A non-nil error from either
// method stops the walk, and WalkBeneath returns it unchanged.
type Visitor interface {
	// Directory is called once for every directory, including the top one
	// at depth 0, with an open O_RDONLY|O_DIRECTORY descriptor that is valid
	// only during the call. It must neither close the descriptor nor rely on
	// its file offset.
	Directory(fd int, depth int) error
	// Entry is called for every non-directory entry with the descriptor of
	// its parent directory, its name and its depth. dtype is the d_type the
	// listing reported, or the type fstatat(AT_SYMLINK_NOFOLLOW) found when
	// the listing reported DT_UNKNOWN; it is advisory, because the entry can
	// change after it was listed. A symlink is reported, never followed.
	// When the top entry itself is not a directory, Entry receives the
	// parent's descriptor and depth 0.
	Entry(parentFD int, name string, dtype uint8, depth int) error
}

// WalkReport counts what one WalkBeneath call visited.
type WalkReport struct {
	// Dirs counts the directories visited, the top one included.
	Dirs uint64
	// Entries counts the non-directory entries visited.
	Entries uint64
	// MaxDepth is the deepest directory level entered; the top is level 0.
	MaxDepth int
}

// WalkBeneath visits, read-only, the entry name inside parent and everything
// beneath it, with the same resource bounds and containment rules as
// RemoveBeneath: no symlink is followed, no mount is crossed, and every ".."
// ascent is verified. Where RemoveBeneath would cut a tree deeper than its
// bound, WalkBeneath returns an error wrapping ErrTooDeep.
//
// Tenants may be writing concurrently. The walk is best effort: entries that
// change while it runs may be skipped or visited twice, and an ancestor that
// moves, or a directory removed while the walk holds it, fails the walk with
// ErrTreeChanged. The walk never acts outside the tree: it follows no symlink
// and crosses no mount, and only a directory moved out of the tree while the
// walk holds it is listed before the move is detected. An absent name fails
// with an error satisfying errors.Is(err, fs.ErrNotExist).
//
// parent must stay open for the duration of the call, and v must not close
// it.
func WalkBeneath(ctx context.Context, parent *os.File, name Name, v Visitor) (WalkReport, error) {
	return walkBeneath(ctx, parent, name, v, maxDepth)
}

// walkBeneath is WalkBeneath with the depth bound as a parameter, so a
// white-box test can reach ErrTooDeep without building 65,536 levels.
func walkBeneath(ctx context.Context, parent *os.File, name Name, v Visitor, limit int) (WalkReport, error) {
	if err := checkCall(ctx, parent, name, limit); err != nil {
		return WalkReport{}, err
	}
	if v == nil {
		return WalkReport{}, errors.New("fstree: nil visitor")
	}
	var (
		report WalkReport
		runErr error
	)
	err := withFD(parent, func(pfd int) {
		w := newWalker(ctx, pfd, name, v, limit)
		defer w.release()
		runErr = w.run()
		report = w.report
	})
	if err != nil {
		return report, err
	}
	return report, runErr
}

func checkCall(ctx context.Context, parent *os.File, name Name, limit int) error {
	switch {
	case ctx == nil:
		return errors.New("fstree: nil context")
	case parent == nil:
		return errors.New("fstree: nil parent directory")
	case !validName(name.s):
		// Only the zero Name gets here: ParseName built every other one.
		return fmt.Errorf("%w: the zero Name", ErrInvalidName)
	case limit < 1:
		return fmt.Errorf("fstree: depth bound %d is below 1", limit)
	}
	return nil
}

// withFD runs fn with parent's descriptor. Control holds a reference on
// parent for the whole call, so a concurrent Close does not release the
// descriptor before fn returns and its number cannot be reused underneath the
// traversal.
func withFD(parent *os.File, fn func(fd int)) error {
	raw, err := parent.SyscallConn()
	if err != nil {
		return fmt.Errorf("fstree: parent directory: %w", err)
	}
	if err := raw.Control(func(fd uintptr) { fn(int(fd)) }); err != nil {
		return fmt.Errorf("fstree: parent directory: %w", err)
	}
	return nil
}

// shortName bounds how much of an entry name an error message carries, so a
// long tenant-chosen name cannot inflate logs or diagnostics.
func shortName(name string) string {
	if len(name) <= errorNameLen {
		return name
	}
	return name[:errorNameLen] + "..."
}

// classify maps a failed mutation of the entry name at depth to an error.
// EPERM, EACCES, EBUSY and EROFS mean the entry cannot be removed as things
// stand (a permission, an immutable or append-only flag, a mount point, a
// read-only filesystem) and wrap ErrUndeletable; anything else keeps its
// errno. Either way the message names one bounded entry name, never a path.
func classify(op, name string, depth int, err error) error {
	if errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES) ||
		errors.Is(err, unix.EBUSY) || errors.Is(err, unix.EROFS) {
		return fmt.Errorf("%w: %s %q at depth %d: %w", ErrUndeletable, op, shortName(name), depth, err)
	}
	return opError(op, name, depth, err)
}

// opError describes a failed operation on the entry name at depth.
func opError(op, name string, depth int, err error) error {
	return fmt.Errorf("fstree: %s %q at depth %d: %w", op, shortName(name), depth, err)
}

// dirError describes a failed operation on the directory at depth itself,
// whose name the traversal does not keep.
func dirError(op string, depth int, err error) error {
	return fmt.Errorf("fstree: %s directory at depth %d: %w", op, depth, err)
}

// removedWhileHeld reports that the directory at depth was removed while the
// traversal held it open; reading a removed directory fails with ENOENT.
func removedWhileHeld(depth int) error {
	return fmt.Errorf("%w: the directory at depth %d was removed while held", ErrTreeChanged, depth)
}

// crossDevice reports that the directory name at depth lies on another
// filesystem or mount.
func crossDevice(name string, depth int) error {
	return fmt.Errorf("%w: %q at depth %d", ErrCrossDevice, shortName(name), depth)
}
