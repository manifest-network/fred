//go:build linux

package fstree

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

// walkFrame records where to resume an ancestor once the walk climbs back.
type walkFrame struct {
	ino    uint64 // inode of the directory to return to
	cookie int64  // d_off that resumes it after the child entered
}

// walker is one WalkBeneath call. It owns at most two descriptors: the
// directory being listed and, while it descends or ascends, one transient
// descriptor.
type walker struct {
	ctx   context.Context
	pfd   int
	name  string
	limit int
	visit Visitor

	anchor identity
	cur    int // descriptor of the directory being listed; -1 when not open
	curIno uint64
	cookie int64 // d_off at which to read cur next
	stack  []walkFrame

	dir    dirReader
	report WalkReport
}

func newWalker(ctx context.Context, pfd int, name Name, v Visitor, limit int) *walker {
	return &walker{
		ctx:   ctx,
		pfd:   pfd,
		name:  name.s,
		limit: limit,
		visit: v,
		cur:   -1,
		dir:   newDirReader(),
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
	if w.cur >= 0 {
		closeFD(w.cur)
		w.cur = -1
	}
}

// depth is the level of the directory being listed; the top is 0.
func (w *walker) depth() int { return len(w.stack) }

func (w *walker) canceled(err error) error {
	return fmt.Errorf("fstree: walk of %q stopped at depth %d: %w", shortName(w.name), w.depth(), err)
}

// begin opens the top entry. A top entry that is not a directory is visited
// as an entry, and opened=false reports that nothing is left to do.
func (w *walker) begin() (opened bool, err error) {
	if err := w.ctx.Err(); err != nil {
		return false, w.canceled(err)
	}
	fd, err := openDirAt(w.pfd, w.name)
	switch {
	case err == nil:
		w.cur = fd
	case errors.Is(err, unix.ENOTDIR), errors.Is(err, unix.ELOOP):
		return false, w.visitTop()
	default:
		return false, opError("open", w.name, 0, err)
	}
	parent, err := statFD(w.pfd)
	if err != nil {
		return false, opError("stat the parent of", w.name, 0, err)
	}
	top, err := statFD(w.cur)
	if err != nil {
		return false, opError("stat", w.name, 0, err)
	}
	if !parent.sameMount(top) {
		return false, crossDevice(w.name, 0)
	}
	w.anchor, w.curIno = top, top.ino
	w.report.Dirs++
	return true, w.visit.Directory(w.cur, 0)
}

// visitTop visits a top entry that is not a directory.
func (w *walker) visitTop() error {
	id, err := statEntry(w.pfd, w.name)
	if err != nil {
		return opError("stat", w.name, 0, err)
	}
	if id.isDir() {
		return fmt.Errorf("%w: %q became a directory while it was opened", ErrTreeChanged, shortName(w.name))
	}
	w.report.Entries++
	return w.visit.Entry(w.pfd, w.name, id.direntType(), 0)
}

// step reads one batch of the directory being listed from the saved cookie
// and visits it in order, entering the first child directory it meets. At
// the end of the directory it ascends, or, at the top, reports done.
func (w *walker) step() (done bool, err error) {
	if err := w.ctx.Err(); err != nil {
		return false, w.canceled(err)
	}
	start := w.cookie
	entries, next, eof, err := w.dir.read(w.cur, start)
	switch {
	case err == nil:
	case errors.Is(err, unix.ENOENT):
		return false, removedWhileHeld(w.depth())
	default:
		return false, dirError("read", w.depth(), err)
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
		w.cookie = e.off
	}
	w.dir.grow()
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
func (w *walker) visitEntry(e dirent) (descended bool, err error) {
	depth := w.depth() + 1
	typ, present, err := w.entryType(e)
	if err != nil || !present {
		return false, err
	}
	if typ == unix.DT_DIR {
		return w.descend(e)
	}
	w.report.Entries++
	return false, w.visit.Entry(w.cur, e.name, typ, depth)
}

// entryType returns e's d_type, asking the filesystem without following a
// symlink when the listing did not say: DT_UNKNOWN, or a value this package
// does not know. present is false when the entry vanished first.
func (w *walker) entryType(e dirent) (typ uint8, present bool, err error) {
	switch e.typ {
	case unix.DT_FIFO, unix.DT_CHR, unix.DT_DIR, unix.DT_BLK, unix.DT_REG, unix.DT_LNK, unix.DT_SOCK, unix.DT_WHT:
		return e.typ, true, nil
	}
	id, err := statEntry(w.cur, e.name)
	switch {
	case err == nil:
		return id.direntType(), true, nil
	case errors.Is(err, unix.ENOENT):
		return 0, false, nil
	default:
		return 0, false, opError("stat", e.name, w.depth()+1, err)
	}
}

// descend enters the child directory e. A child that vanished or stopped
// being a directory is skipped. A child deeper than the bound fails the walk
// with ErrTooDeep, and one on another device or mount with ErrCrossDevice.
func (w *walker) descend(e dirent) (descended bool, err error) {
	depth := w.depth() + 1
	if depth > w.limit {
		return false, fmt.Errorf("%w: %q at depth %d exceeds the bound of %d",
			ErrTooDeep, shortName(e.name), depth, w.limit)
	}
	fd, err := openDirAt(w.cur, e.name)
	switch {
	case err == nil:
	case errors.Is(err, unix.ENOENT), errors.Is(err, unix.ENOTDIR), errors.Is(err, unix.ELOOP):
		return false, nil
	default:
		return false, opError("open", e.name, depth, err)
	}
	child, err := statFD(fd)
	if err != nil {
		closeFD(fd)
		return false, opError("stat", e.name, depth, err)
	}
	if !w.anchor.sameMount(child) {
		closeFD(fd)
		return false, crossDevice(e.name, depth)
	}
	w.stack = append(w.stack, walkFrame{ino: w.curIno, cookie: e.off})
	closeFD(w.cur)
	w.cur, w.curIno, w.cookie = fd, child.ino, 0
	w.report.MaxDepth = max(w.report.MaxDepth, depth)
	w.report.Dirs++
	w.dir.full()
	return true, w.visit.Directory(w.cur, depth)
}

// ascend returns from a fully listed directory to its parent through "..",
// and only when that parent is the directory it was entered from, then
// resumes the parent after the child.
func (w *walker) ascend() error {
	if err := w.ctx.Err(); err != nil {
		return w.canceled(err)
	}
	depth := w.depth()
	frame := w.stack[depth-1]
	fd, err := openDirAt(w.cur, "..")
	switch {
	case err == nil:
	case errors.Is(err, unix.ENOENT):
		return removedWhileHeld(depth)
	default:
		return opError("open", "..", depth, err)
	}
	up, err := statFD(fd)
	if err != nil {
		closeFD(fd)
		return opError("stat", "..", depth, err)
	}
	if !w.anchor.sameMount(up) || up.ino != frame.ino {
		closeFD(fd)
		return fmt.Errorf("%w: the parent of the directory at depth %d is not the directory it was entered from",
			ErrTreeChanged, depth)
	}
	closeFD(w.cur)
	w.cur, w.curIno, w.cookie = fd, frame.ino, frame.cookie
	w.stack = w.stack[:depth-1]
	w.dir.shrink()
	return nil
}
