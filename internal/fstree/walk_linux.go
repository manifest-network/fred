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

// This file holds the whole walk path, and it is read-only by construction:
// it reaches the filesystem only through at.View and at.ListedView, which
// have no method that changes anything. A guard test pins that this file
// names nothing from package at outside that read-only side, and nothing of
// the remover.

// Visitor receives what WalkBeneath visits. A non-nil error from either
// method stops the walk, and WalkBeneath returns it unchanged.
//
// Each method receives its directory as a BorrowedDir that is valid only
// during the call; see BorrowedDir for what the descriptor may be used for.
type Visitor interface {
	// Directory is called once for every directory, including the top one
	// at depth 0.
	Directory(dir BorrowedDir, depth int) error
	// Entry is called for every non-directory entry with its parent
	// directory, its name and its depth. dtype is the d_type the listing
	// reported, or the type fstatat(AT_SYMLINK_NOFOLLOW) found when the
	// listing reported DT_UNKNOWN; it is advisory, because the entry can
	// change after it was listed. A symlink is reported, never followed.
	// When the top entry itself is not a directory, Entry receives the
	// caller's parent directory and depth 0.
	Entry(parent BorrowedDir, name string, dtype uint8, depth int) error
}

// WalkReport counts what one WalkBeneath call visited, and what it passed
// over.
type WalkReport struct {
	// Dirs counts the directories visited, the top one included.
	Dirs uint64
	// Entries counts the non-directory entries visited.
	Entries uint64
	// MaxDepth is the deepest directory level entered; the top is level 0.
	MaxDepth int
	// Vanished counts listed entries that were gone by the time the walk
	// came to them, so that it passed over them: a directory it could not
	// open, or an entry of unreported type it could not stat.
	Vanished uint64
	// Retyped counts listed directories that were no longer directories by
	// the time the walk came to open them, replaced by a file or by a
	// symlink, which the walk never follows; it passed over them.
	Retyped uint64
}

// Complete reports that the walk passed over nothing it listed: no entry
// vanished, and no directory changed type, before the walk reached it. A
// walk that returned a nil error but is not complete did not observe the
// whole tree, and must never be reported as having found it clean.
//
// Complete is an observation, not a fact, while the tree's writers are
// running: an entry renamed into a part of the tree the walk has already
// listed is not seen, and nothing reports it.
func (r WalkReport) Complete() bool {
	return r.Vanished == 0 && r.Retyped == 0
}

// WalkBeneath visits, read-only, the entry name inside parent and everything
// beneath it, with the same resource bounds and containment rules as
// RemoveBeneath: no symlink is followed, no mount is crossed, and every
// ascent to a parent directory is verified. Where RemoveBeneath would cut a
// tree deeper than its bound, WalkBeneath returns an error wrapping
// ErrTooDeep.
//
// Tenants may be writing concurrently. The walk is best effort: entries that
// change while it runs may be passed over or visited twice, and an ancestor
// that moves, or a directory removed while the walk holds it, fails the walk
// with ErrTreeChanged. Every listed entry the walk passes over is counted in
// the report, and WalkReport.Complete is false for such a walk, so it is not
// mistaken for one that saw everything it listed. The walk never acts
// outside the tree: it follows no symlink and crosses no mount, and only a
// directory moved out of the tree while the walk holds it is listed before
// the move is detected. An absent name fails with an error satisfying
// errors.Is(err, fs.ErrNotExist).
//
// parent stays open for the whole call, even against a concurrent Close; a
// parent closed before the call is refused.
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
	err := at.BorrowView(parent, func(p at.View) {
		w := newWalker(ctx, p, name, v, limit)
		defer w.release()
		runErr = w.run()
		report = w.report
	})
	if err != nil {
		return report, parentError(err)
	}
	return report, runErr
}

// walkFrame records where to resume an ancestor once the walk climbs back.
type walkFrame struct {
	ino    uint64 // inode of the directory to return to
	cookie int64  // d_off that resumes it after the child entered
}

// walker is one WalkBeneath call. It owns at most two descriptors: the
// directory being listed and, while it descends or ascends, one transient
// descriptor. The parent is the caller's, borrowed for the call.
type walker struct {
	ctx    context.Context
	parent at.View // borrowed; never closed here
	name   Name
	limit  int
	visit  Visitor
	lender at.Lender

	anchorID at.Identity
	cur      at.View // directory being listed; the zero View when not open
	curIno   uint64
	cookie   int64 // d_off at which to read cur next
	stack    []walkFrame

	dir    at.Reader
	report WalkReport
}

func newWalker(ctx context.Context, parent at.View, name Name, v Visitor, limit int) *walker {
	return &walker{
		ctx:    ctx,
		parent: parent,
		name:   name,
		limit:  limit,
		visit:  v,
		dir:    at.NewReader(),
	}
}

// run visits the entry and, when it is a directory, everything beneath it.
func (w *walker) run() error {
	opened, err := w.begin()
	if err != nil || !opened {
		return err
	}
	for {
		done, err := w.step()
		if err != nil || done {
			return err
		}
	}
}

// release closes the descriptor the walker still holds. It is safe to call
// more than once.
func (w *walker) release() {
	w.cur.Close()
	w.cur = at.View{}
}

// depth is the level of the directory being listed; the top is 0.
func (w *walker) depth() int { return len(w.stack) }

func (w *walker) canceled(err error) error {
	return fmt.Errorf("fstree: walk of %q stopped at depth %d: %w", shortName(w.name.String()), w.depth(), err)
}

// visitDirectory lends the directory being listed to Visitor.Directory.
func (w *walker) visitDirectory(depth int) error {
	return w.lender.Lend(w.cur, func(dir at.Borrowed) error {
		return w.visit.Directory(BorrowedDir{lent: dir}, depth)
	})
}

// lendEntry lends dir, the parent of the entry name, to Visitor.Entry.
func (w *walker) lendEntry(dir at.View, name string, dtype uint8, depth int) error {
	return w.lender.Lend(dir, func(parent at.Borrowed) error {
		return w.visit.Entry(BorrowedDir{lent: parent}, name, dtype, depth)
	})
}

// begin opens the top entry. A top entry that is not a directory is visited
// as an entry, and opened=false reports that nothing is left to do.
func (w *walker) begin() (opened bool, err error) {
	if err := w.ctx.Err(); err != nil {
		return false, w.canceled(err)
	}
	name := w.name.String()
	top, err := w.parent.OpenChild(w.name)
	switch {
	case err == nil:
		w.cur = top
	case errors.Is(err, unix.ENOTDIR), errors.Is(err, unix.ELOOP):
		return false, w.visitTop()
	default:
		return false, opError("open", name, 0, err)
	}
	parentID, err := w.parent.Stat()
	if err != nil {
		return false, opError("stat the parent of", name, 0, err)
	}
	topID, err := w.cur.Stat()
	if err != nil {
		return false, opError("stat", name, 0, err)
	}
	if !parentID.SameMount(topID) {
		return false, crossDevice(name, 0)
	}
	w.anchorID, w.curIno = topID, topID.Ino()
	w.report.Dirs++
	return true, w.visitDirectory(0)
}

// visitTop visits a top entry that is not a directory.
func (w *walker) visitTop() error {
	name := w.name.String()
	id, err := w.parent.StatChild(w.name)
	if err != nil {
		return opError("stat", name, 0, err)
	}
	if id.IsDir() {
		return fmt.Errorf("%w: %q became a directory while it was opened", ErrTreeChanged, shortName(name))
	}
	w.report.Entries++
	return w.lendEntry(w.parent, name, id.DirentType(), 0)
}

// step reads one batch of the directory being listed from the saved cookie
// and visits it in order, entering the first child directory it meets. At
// the end of the directory it ascends, or, at the top, reports done.
func (w *walker) step() (done bool, err error) {
	if err := w.ctx.Err(); err != nil {
		return false, w.canceled(err)
	}
	start := w.cookie
	entries, next, eof, err := w.cur.ReadBatch(&w.dir, start)
	if err != nil {
		return false, readError(w.depth(), err)
	}
	if eof {
		if w.depth() == 0 {
			return true, nil
		}
		return false, w.ascend()
	}
	if err := advanced(start, next, w.depth()); err != nil {
		return false, err
	}
	for _, e := range entries {
		if err := w.ctx.Err(); err != nil {
			return false, w.canceled(err)
		}
		descended, err := w.visitEntry(e)
		if err != nil || descended {
			return false, err
		}
		w.cookie = e.Offset()
	}
	w.dir.Grow()
	return false, nil
}

// advanced enforces progress for the walk. Resuming a directory at the
// cookie a batch ended on must move past that batch; a filesystem whose
// offsets do not advance would otherwise have the walk re-read the same
// batch forever.
func advanced(start, next int64, depth int) error {
	if next == start {
		return fmt.Errorf("%w: directory offset did not advance at depth %d", ErrTreeChanged, depth)
	}
	return nil
}

// visitEntry visits one listed entry, entering it when it is a directory.
// An entry of unreported type that vanished before it could be described is
// counted as Vanished and passed over.
func (w *walker) visitEntry(e at.ListedView) (descended bool, err error) {
	depth := w.depth() + 1
	typ, present, err := e.Type()
	switch {
	case err != nil:
		return false, opError("stat", e.String(), depth, err)
	case !present:
		w.report.Vanished++
		return false, nil
	case typ == unix.DT_DIR:
		return w.descend(e)
	}
	w.report.Entries++
	return false, w.lendEntry(w.cur, e.String(), typ, depth)
}

// descend enters the listed child directory e. A child that vanished
// (Vanished) or stopped being a directory (Retyped) is counted and passed
// over. A child deeper than the bound fails the walk with ErrTooDeep, and
// one on another device or mount with ErrCrossDevice.
func (w *walker) descend(e at.ListedView) (descended bool, err error) {
	depth := w.depth() + 1
	if depth > w.limit {
		return false, fmt.Errorf("%w: %q at depth %d exceeds the bound of %d",
			ErrTooDeep, shortName(e.String()), depth, w.limit)
	}
	child, err := e.OpenDir()
	switch {
	case err == nil:
	case errors.Is(err, unix.ENOENT):
		w.report.Vanished++
		return false, nil
	case errors.Is(err, unix.ENOTDIR), errors.Is(err, unix.ELOOP):
		w.report.Retyped++
		return false, nil
	default:
		return false, opError("open", e.String(), depth, err)
	}
	childID, err := child.Stat()
	if err != nil {
		child.Close()
		return false, opError("stat", e.String(), depth, err)
	}
	if !w.anchorID.SameMount(childID) {
		child.Close()
		return false, crossDevice(e.String(), depth)
	}
	w.stack = append(w.stack, walkFrame{ino: w.curIno, cookie: e.Offset()})
	w.cur.Close()
	w.cur, w.curIno, w.cookie = child, childID.Ino(), 0
	w.report.MaxDepth = max(w.report.MaxDepth, depth)
	w.report.Dirs++
	w.dir.Full()
	return true, w.visitDirectory(depth)
}

// ascend returns from a fully listed directory to its parent, and only when
// that parent is the directory it was entered from, then resumes the parent
// after the child.
func (w *walker) ascend() error {
	if err := w.ctx.Err(); err != nil {
		return w.canceled(err)
	}
	depth := w.depth()
	frame := w.stack[depth-1]
	up, err := w.cur.OpenParent()
	switch {
	case err == nil:
	case errors.Is(err, unix.ENOENT):
		return removedWhileHeld(depth)
	default:
		return dirError("open the parent of", depth, err)
	}
	upID, err := up.Stat()
	if err != nil {
		up.Close()
		return dirError("stat the parent of", depth, err)
	}
	if !w.anchorID.SameMount(upID) || upID.Ino() != frame.ino {
		up.Close()
		return movedError(depth)
	}
	w.cur.Close()
	w.cur, w.curIno, w.cookie = up, frame.ino, frame.cookie
	w.stack = w.stack[:depth-1]
	w.dir.Shrink()
	return nil
}
