//go:build linux

package fstree

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"

	"golang.org/x/sys/unix"
)

const (
	// cutPrefix names a subtree the remover moved into the anchor because it
	// lay deeper than the depth bound. The anchor's later scans remove it
	// like any other child, and so does a rerun.
	cutPrefix = ".fred-cut-"
	// cutAttempts bounds the fresh names a cut tries while names are taken.
	cutAttempts = 8
)

// entryOutcome is what removing one listed entry achieved.
type entryOutcome uint8

const (
	// entryRemoved: this call unlinked the entry or removed the empty
	// directory.
	entryRemoved entryOutcome = iota
	// entryAbsent: the entry was already gone, or changed type between the
	// listing and the removal; the next scan sees whatever is there now.
	entryAbsent
	// entryNonEmpty: the entry is a directory that still has entries.
	entryNonEmpty
)

// remover is one RemoveBeneath call. Its phases are separate methods that
// run drives in order: begin opens the anchor, step runs one iteration, and
// finish removes the emptied anchor. Keeping them apart lets a white-box test
// change the tree between iterations without any hook in this file.
//
// It owns at most three descriptors: the anchor, the directory being emptied
// and, while it descends or ascends, one transient descriptor.
type remover struct {
	ctx            context.Context
	pfd            int
	name           string
	limit          int
	beforeFirstCut func(anchorFD int) error

	anchor identity // the anchor as opened through name
	afd    int      // anchor descriptor; -1 when not open
	cur    int      // descriptor of the directory being emptied; -1 when not open
	curIno uint64
	stack  []uint64 // inode of each ancestor of cur, from the anchor down
	stalls int      // consecutive iterations without progress

	cutsBegun bool   // the first cut was prepared: hook run, names seeded
	cutNext   uint64 // next cut-name counter value

	dir    dirReader
	report RemoveReport
}

func newRemover(ctx context.Context, pfd int, name Name, opts RemoveOptions, limit int) *remover {
	return &remover{
		ctx:            ctx,
		pfd:            pfd,
		name:           name.s,
		limit:          limit,
		beforeFirstCut: opts.BeforeFirstCut,
		afd:            -1,
		cur:            -1,
		dir:            newDirReader(),
	}
}

// run removes the entry: begin, then steps until the anchor is empty, then
// finish.
func (r *remover) run() error {
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
	}
}

// release closes every descriptor the remover still holds. It is safe to
// call more than once.
func (r *remover) release() {
	if r.cur >= 0 {
		closeFD(r.cur)
		r.cur = -1
	}
	if r.afd >= 0 {
		closeFD(r.afd)
		r.afd = -1
	}
}

// depth is the level of the directory being emptied; the anchor is 0.
func (r *remover) depth() int { return len(r.stack) }

func (r *remover) canceled(err error) error {
	return fmt.Errorf("fstree: removal of %q stopped at depth %d: %w", shortName(r.name), r.depth(), err)
}

// begin unlinks the entry outright when it is not a directory, and reports
// opened=false when nothing is left to do. Otherwise it opens the entry as
// the anchor, requires the anchor to share its parent's device and mount,
// and reports opened. A host mount at the entry is therefore refused with
// ErrCrossDevice before anything in it is touched.
func (r *remover) begin() (opened bool, err error) {
	if err := r.ctx.Err(); err != nil {
		return false, r.canceled(err)
	}
	err = unlinkAt(r.pfd, r.name, 0)
	switch {
	case err == nil:
		r.report.Entries++
		return false, nil
	case errors.Is(err, unix.ENOENT):
		return false, nil
	case !errors.Is(err, unix.EISDIR):
		return false, classify("unlink", r.name, 0, err)
	}

	afd, err := openDirAt(r.pfd, r.name)
	switch {
	case err == nil:
		r.afd = afd
	case errors.Is(err, unix.ENOENT):
		return false, nil
	case errors.Is(err, unix.ENOTDIR), errors.Is(err, unix.ELOOP):
		// The name stopped being a directory after the unlink saw one.
		// Unlink it once more, and report whatever that says.
		err = unlinkAt(r.pfd, r.name, 0)
		switch {
		case err == nil:
			r.report.Entries++
			return false, nil
		case errors.Is(err, unix.ENOENT):
			return false, nil
		default:
			return false, classify("unlink", r.name, 0, err)
		}
	default:
		return false, classify("open", r.name, 0, err)
	}

	parent, err := statFD(r.pfd)
	if err != nil {
		return false, opError("stat the parent of", r.name, 0, err)
	}
	anchor, err := statFD(r.afd)
	if err != nil {
		return false, opError("stat", r.name, 0, err)
	}
	if !parent.sameMount(anchor) {
		return false, crossDevice(r.name, 0)
	}
	r.anchor = anchor
	cur, err := dupFD(r.afd)
	if err != nil {
		return false, opError("dup", r.name, 0, err)
	}
	r.cur, r.curIno = cur, anchor.ino
	return true, nil
}

// step runs one iteration at the directory being emptied: it reads one batch
// from the directory's start and removes entries until the first non-empty
// child directory, which it enters (or cuts, at the depth bound). When the
// directory is empty it ascends, or, at the anchor, reports empty.
func (r *remover) step() (empty bool, err error) {
	if err := r.ctx.Err(); err != nil {
		return false, r.canceled(err)
	}
	entries, _, eof, err := r.dir.read(r.cur, 0)
	switch {
	case err == nil:
	case errors.Is(err, unix.ENOENT):
		return false, removedWhileHeld(r.depth())
	default:
		return false, dirError("read", r.depth(), err)
	}
	if eof {
		if r.depth() == 0 {
			return true, nil
		}
		return false, r.ascend()
	}
	progressed, err := r.drain(entries)
	if err != nil {
		return false, err
	}
	return false, r.account(progressed)
}

// drain removes a batch's entries in listing order and stops at the first
// child that is a non-empty directory. It enters that child, or cuts it when
// the directory being emptied is already at the depth bound. Everything
// listed before the child is gone by then, so a later scan from the start of
// this directory meets the child again and, once it is empty, removes it.
func (r *remover) drain(entries []dirent) (progressed bool, err error) {
	for _, e := range entries {
		if err := r.ctx.Err(); err != nil {
			return progressed, r.canceled(err)
		}
		outcome, err := r.remove(e.name)
		if err != nil {
			return progressed, err
		}
		switch outcome {
		case entryRemoved:
			progressed = true
		case entryAbsent:
		case entryNonEmpty:
			var moved bool
			if r.depth() >= r.limit {
				moved, err = r.cut(e.name)
			} else {
				moved, err = r.descend(e.name)
			}
			return progressed || moved, err
		}
	}
	r.dir.grow()
	return progressed, nil
}

// account enforces I4. An iteration that neither removed an entry nor moved
// (descended, ascended or cut) is retried once; a second one in a row fails
// with ErrTreeChanged. That happens only when a listing keeps naming entries
// that lookups no longer find, and it must not make the remover spin.
func (r *remover) account(progressed bool) error {
	if progressed {
		r.stalls = 0
		return nil
	}
	r.stalls++
	if r.stalls > 1 {
		return fmt.Errorf("%w: no progress at depth %d", ErrTreeChanged, r.depth())
	}
	return nil
}

// remove unlinks name from the directory being emptied. A directory is
// removed only when empty; a non-empty one is reported so that it is emptied
// first.
func (r *remover) remove(name string) (entryOutcome, error) {
	depth := r.depth() + 1
	err := unlinkAt(r.cur, name, 0)
	switch {
	case err == nil:
		r.report.Entries++
		return entryRemoved, nil
	case errors.Is(err, unix.ENOENT):
		return entryAbsent, nil
	case !errors.Is(err, unix.EISDIR):
		return entryAbsent, classify("unlink", name, depth, err)
	}
	err = unlinkAt(r.cur, name, unix.AT_REMOVEDIR)
	switch {
	case err == nil:
		r.report.Dirs++
		return entryRemoved, nil
	case errors.Is(err, unix.ENOENT), errors.Is(err, unix.ENOTDIR):
		return entryAbsent, nil
	case errors.Is(err, unix.ENOTEMPTY), errors.Is(err, unix.EEXIST):
		return entryNonEmpty, nil
	default:
		return entryAbsent, classify("rmdir", name, depth, err)
	}
}

// descend enters the child directory name. A child that vanished or stopped
// being a directory is skipped (moved=false), and the next scan sees what is
// there now. A child on another device or mount fails with ErrCrossDevice.
func (r *remover) descend(name string) (moved bool, err error) {
	if err := r.ctx.Err(); err != nil {
		return false, r.canceled(err)
	}
	depth := r.depth() + 1
	fd, err := openDirAt(r.cur, name)
	switch {
	case err == nil:
	case errors.Is(err, unix.ENOENT), errors.Is(err, unix.ENOTDIR), errors.Is(err, unix.ELOOP):
		return false, nil
	default:
		return false, classify("open", name, depth, err)
	}
	child, err := statFD(fd)
	if err != nil {
		closeFD(fd)
		return false, opError("stat", name, depth, err)
	}
	if !r.anchor.sameMount(child) {
		closeFD(fd)
		return false, crossDevice(name, depth)
	}
	r.stack = append(r.stack, r.curIno)
	closeFD(r.cur)
	r.cur, r.curIno = fd, child.ino
	r.report.MaxDepth = max(r.report.MaxDepth, depth)
	r.dir.full()
	return true, nil
}

// ascend returns from an emptied directory to its parent through "..", and
// only when that parent is the directory it was entered from: same device,
// mount and inode. Anything else means the tree moved, and the remover stops
// with ErrTreeChanged rather than continue somewhere it never descended.
func (r *remover) ascend() error {
	if err := r.ctx.Err(); err != nil {
		return r.canceled(err)
	}
	depth := r.depth()
	want := r.stack[depth-1]
	fd, err := openDirAt(r.cur, "..")
	switch {
	case err == nil:
	case errors.Is(err, unix.ENOENT):
		return removedWhileHeld(depth)
	default:
		return classify("open", "..", depth, err)
	}
	up, err := statFD(fd)
	if err != nil {
		closeFD(fd)
		return opError("stat", "..", depth, err)
	}
	if !r.anchor.sameMount(up) || up.ino != want {
		closeFD(fd)
		return fmt.Errorf("%w: the parent of the directory at depth %d is not the directory it was entered from",
			ErrTreeChanged, depth)
	}
	closeFD(r.cur)
	r.cur, r.curIno = fd, want
	r.stack = r.stack[:depth-1]
	r.stalls = 0
	r.dir.shrink()
	return nil
}

// cut moves the non-empty child name of the directory being emptied, which
// sits at the depth bound, into the anchor under a fresh name. The subtree
// is then removed from the anchor like any other child, so the ancestor
// stack never grows past the bound.
//
// The call's first cut runs beginCuts first. Cut names count up from a
// random origin: names planted in the anchor beforehand cannot be predicted
// to collide, and a rerun of an interrupted removal does not retry the names
// it left behind.
func (r *remover) cut(name string) (moved bool, err error) {
	depth := r.depth() + 1
	if !r.cutsBegun {
		if err := r.beginCuts(name, depth); err != nil {
			return false, err
		}
	}
	for range cutAttempts {
		target := fmt.Sprintf("%s%016x", cutPrefix, r.cutNext)
		r.cutNext++
		err = renameNoReplace(r.cur, name, r.afd, target)
		switch {
		case err == nil:
			r.report.Cuts++
			return true, nil
		case errors.Is(err, unix.EEXIST):
			continue
		case errors.Is(err, unix.ENOENT):
			// The child vanished after rmdir saw it; the next scan decides.
			return false, nil
		default:
			return false, cutError(name, depth, err)
		}
	}
	return false, cutError(name, depth, err)
}

// beginCuts prepares the call's first cut, once: it runs BeforeFirstCut on
// the anchor and seeds the cut names. It marks the cuts begun before the
// hook runs, so the hook runs at most once per call even when it fails; a
// failure refuses the cut with ErrCutRefused, and the call stops there.
func (r *remover) beginCuts(name string, depth int) error {
	r.cutsBegun = true
	if r.beforeFirstCut != nil {
		if err := r.beforeFirstCut(r.afd); err != nil {
			return fmt.Errorf("%w: before the first cut, of %q at depth %d: %w",
				ErrCutRefused, shortName(name), depth, err)
		}
	}
	var seed [8]byte
	// crypto/rand.Read never fails since Go 1.24.
	_, _ = rand.Read(seed[:])
	r.cutNext = binary.NativeEndian.Uint64(seed[:])
	return nil
}

// cutError classifies a failed cut. EXDEV, EDQUOT, ENOSPC and EMLINK mean
// the anchor cannot take the subtree and wrap ErrCutRefused; anything else
// is classified like any other failed mutation.
func cutError(name string, depth int, err error) error {
	if errors.Is(err, unix.EXDEV) || errors.Is(err, unix.EDQUOT) ||
		errors.Is(err, unix.ENOSPC) || errors.Is(err, unix.EMLINK) {
		return fmt.Errorf("%w: rename %q at depth %d: %w", ErrCutRefused, shortName(name), depth, err)
	}
	return classify("rename", name, depth, err)
}

// finish removes the emptied anchor, but only while the parent's name still
// binds the directory that was emptied. A name swapped for another object is
// left alone and reported as ErrTreeChanged; a name already gone counts as
// removed.
func (r *remover) finish() error {
	if r.cur >= 0 {
		closeFD(r.cur)
		r.cur = -1
	}
	now, err := statEntry(r.pfd, r.name)
	switch {
	case err == nil:
	case errors.Is(err, unix.ENOENT):
		return nil
	default:
		return opError("stat", r.name, 0, err)
	}
	if !now.same(r.anchor) {
		return fmt.Errorf("%w: %q no longer names the emptied directory", ErrTreeChanged, shortName(r.name))
	}
	err = unlinkAt(r.pfd, r.name, unix.AT_REMOVEDIR)
	switch {
	case err == nil:
		r.report.Dirs++
		return nil
	case errors.Is(err, unix.ENOENT):
		return nil
	case errors.Is(err, unix.ENOTEMPTY), errors.Is(err, unix.EEXIST):
		return fmt.Errorf("%w: %q gained entries after it was emptied: %w", ErrTreeChanged, shortName(r.name), err)
	default:
		return classify("rmdir", r.name, 0, err)
	}
}
