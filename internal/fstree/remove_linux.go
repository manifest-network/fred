//go:build linux

package fstree

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/fstree/internal/at"
)

const (
	// cutPrefix names a subtree the remover moved into the anchor because it
	// lay deeper than the depth bound. The anchor's later scans remove it
	// like any other child, and so does a rerun.
	cutPrefix = ".fred-cut-"
	// cutAttempts bounds the fresh names a cut tries while names are taken.
	cutAttempts = 8
)

// RemoveOptions configures RemoveBeneath.
type RemoveOptions struct {
	// BeforeFirstCut, when set, runs on the anchor (the opened top
	// directory) immediately before the call's first cut, and at most once
	// per call. A removal that needs no cut, because the tree is no deeper
	// than the depth bound, never calls it. When it fails, that cut is
	// refused: RemoveBeneath stops with an error wrapping ErrCutRefused, and
	// keeps everything it has not yet removed.
	//
	// It exists so that XFS volume deletion can detach the condemned anchor
	// from its quota project only when a cut makes that necessary, after
	// which a cut into the anchor cannot fail with EXDEV or EDQUOT. It must
	// be idempotent, because a rerun calls it again at its own first cut.
	//
	// The anchor is lent as a BorrowedDir backed by a duplicate of fstree's
	// own descriptor, valid only during the call; the cuts rename into a
	// descriptor that is never lent.
	//
	// Whatever the hook returns, it cannot pass for one of this package's
	// verdicts. The error RemoveBeneath returns unwraps to ErrCutRefused and
	// to nothing else: the hook's error appears in its text, and a caller
	// that needs it reaches it through errors.As with an
	// interface{ Cause() error }, never through errors.Is.
	BeforeFirstCut func(anchor BorrowedDir) error
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
// it is a directory, everything beneath it. It holds at most three
// descriptors of its own, keeps at most 65,536 levels of ancestry (512 KiB)
// plus one directory batch, and does not recurse; it never follows symlinks,
// never crosses a mount, and never ascends above the entry. The package
// documentation describes the algorithm, its invariants and its resource
// bounds.
//
// Precondition: no other actor mutates the tree concurrently (its writers
// are stopped). A concurrent rename is detected (ErrTreeChanged), not
// prevented.
//
// An absent name counts as removed. A name that is not a directory,
// including a symlink to one, is unlinked without being opened. On failure
// RemoveBeneath stops and leaves in place everything it has not removed, and
// a rerun continues from there. The error wraps ErrTreeChanged,
// ErrCrossDevice, ErrCutRefused, ErrUndeletable, ErrInvalidName, the
// context's error, or the failing syscall's errno.
//
// parent stays open for the whole call, even against a concurrent Close;
// a parent closed before the call is refused.
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
	err := at.Borrow(parent, func(p *at.Dir) {
		r := newRemover(ctx, p, name, opts, limit)
		defer r.release()
		runErr = r.run()
		report = r.report
	})
	if err != nil {
		return report, parentError(err)
	}
	return report, runErr
}

// entryOutcome is what removing one listed entry achieved. Its zero value is
// entryUnknown, which is no outcome at all: a forgotten or defaulted outcome
// fails the removal instead of reading as progress.
type entryOutcome uint8

const (
	// entryUnknown is the zero value and never a valid outcome.
	entryUnknown entryOutcome = iota
	// entryRemoved: this call unlinked the entry or removed the empty
	// directory.
	entryRemoved
	// entryAbsent: the entry was already gone, or changed type between the
	// listing and the removal; the next scan sees whatever is there now.
	entryAbsent
	// entryNonEmpty: the entry is a directory that still has entries.
	entryNonEmpty
)

// next says what an outcome means for the batch being drained: whether it
// was progress, and whether the entry is a non-empty directory to enter (or
// cut) next. It is total: entryUnknown, or any value outside the set, is an
// error.
func (o entryOutcome) next() (progress, enter bool, err error) {
	switch o {
	case entryRemoved:
		return true, false, nil
	case entryAbsent:
		return false, false, nil
	case entryNonEmpty:
		return false, true, nil
	default:
		return false, false, fmt.Errorf("fstree: invalid removal outcome %d", o)
	}
}

// hookError is a failed BeforeFirstCut. It unwraps to ErrCutRefused and to
// nothing else, so an fstree sentinel in a removal's error is always
// fstree's own verdict: a hook that returns, say, ErrCrossDevice does not
// make the removal report one. Cause returns the hook's error.
type hookError struct {
	name  string // bounded by shortName
	depth int
	cause error
}

func (e *hookError) Error() string {
	return fmt.Sprintf("%s: BeforeFirstCut refused the cut of %q at depth %d: %v",
		ErrCutRefused, e.name, e.depth, e.cause)
}

// Unwrap returns ErrCutRefused, never the hook's error.
func (e *hookError) Unwrap() error { return ErrCutRefused }

// Cause returns the error BeforeFirstCut returned.
func (e *hookError) Cause() error { return e.cause }

// remover is one RemoveBeneath call. Its phases are separate methods that
// run drives in order: begin opens the anchor, step runs one iteration, and
// finish removes the emptied anchor. Keeping them apart lets a white-box test
// change the tree between iterations without any hook in this file.
//
// It owns at most three descriptors: the anchor, the directory being emptied
// and, while it descends, ascends or lends the anchor, one transient
// descriptor. The parent is the caller's, borrowed for the call.
type remover struct {
	ctx            context.Context
	parent         *at.Dir // borrowed; never closed here
	name           Name
	limit          int
	beforeFirstCut func(BorrowedDir) error

	anchorID at.Identity // the anchor as opened through name
	anchor   *at.Dir     // nil when not open
	cur      *at.Dir     // directory being emptied; nil when not open
	curIno   uint64
	stack    []uint64 // inode of each ancestor of cur, from the anchor down
	stalls   int      // consecutive iterations without progress

	cutsBegun bool   // the first cut was prepared: hook run, names seeded
	cutNext   uint64 // next cut-name counter value

	dir    at.Reader
	report RemoveReport
}

func newRemover(ctx context.Context, parent *at.Dir, name Name, opts RemoveOptions, limit int) *remover {
	return &remover{
		ctx:            ctx,
		parent:         parent,
		name:           name,
		limit:          limit,
		beforeFirstCut: opts.BeforeFirstCut,
		dir:            at.NewReader(),
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
	r.cur.Close()
	r.cur = nil
	r.anchor.Close()
	r.anchor = nil
}

// depth is the level of the directory being emptied; the anchor is 0.
func (r *remover) depth() int { return len(r.stack) }

func (r *remover) canceled(err error) error {
	return fmt.Errorf("fstree: removal of %q stopped at depth %d: %w", shortName(r.name.String()), r.depth(), err)
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
	name := r.name.String()
	err = r.parent.Unlink(r.name)
	switch {
	case err == nil:
		r.report.Entries++
		return false, nil
	case errors.Is(err, unix.ENOENT):
		return false, nil
	case !errors.Is(err, unix.EISDIR):
		return false, classify("unlink", name, 0, err)
	}

	anchor, err := r.parent.OpenChild(r.name)
	switch {
	case err == nil:
		r.anchor = anchor
	case errors.Is(err, unix.ENOENT):
		return false, nil
	case errors.Is(err, unix.ENOTDIR), errors.Is(err, unix.ELOOP):
		// The name stopped being a directory after the unlink saw one.
		// Unlink it once more, and report whatever that says.
		err = r.parent.Unlink(r.name)
		switch {
		case err == nil:
			r.report.Entries++
			return false, nil
		case errors.Is(err, unix.ENOENT):
			return false, nil
		default:
			return false, classify("unlink", name, 0, err)
		}
	default:
		return false, classify("open", name, 0, err)
	}

	parentID, err := r.parent.Stat()
	if err != nil {
		return false, opError("stat the parent of", name, 0, err)
	}
	anchorID, err := r.anchor.Stat()
	if err != nil {
		return false, opError("stat", name, 0, err)
	}
	if !parentID.SameMount(anchorID) {
		return false, crossDevice(name, 0)
	}
	r.anchorID = anchorID
	cur, err := r.anchor.Dup()
	if err != nil {
		return false, opError("duplicate", name, 0, err)
	}
	r.cur, r.curIno = cur, anchorID.Ino()
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
	entries, _, eof, err := r.cur.ReadBatch(&r.dir, 0)
	if err != nil {
		return false, readError(r.depth(), err)
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
func (r *remover) drain(entries []at.Listed) (progressed bool, err error) {
	for _, e := range entries {
		if err := r.ctx.Err(); err != nil {
			return progressed, r.canceled(err)
		}
		outcome, err := r.remove(e)
		if err != nil {
			return progressed, err
		}
		progress, enter, err := outcome.next()
		if err != nil {
			return progressed, err
		}
		progressed = progressed || progress
		if enter {
			var moved bool
			if r.depth() >= r.limit {
				moved, err = r.cut(e)
			} else {
				moved, err = r.descend(e)
			}
			return progressed || moved, err
		}
	}
	r.dir.Grow()
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

// remove unlinks the listed entry e from the directory being emptied, the
// directory it was listed from. A directory is removed only when empty; a
// non-empty one is reported so that it is emptied first. Every error comes
// with entryUnknown.
func (r *remover) remove(e at.Listed) (entryOutcome, error) {
	depth := r.depth() + 1
	err := e.Unlink()
	switch {
	case err == nil:
		r.report.Entries++
		return entryRemoved, nil
	case errors.Is(err, unix.ENOENT):
		return entryAbsent, nil
	case !errors.Is(err, unix.EISDIR):
		return entryUnknown, classify("unlink", e.String(), depth, err)
	}
	err = e.Rmdir()
	switch {
	case err == nil:
		r.report.Dirs++
		return entryRemoved, nil
	case errors.Is(err, unix.ENOENT), errors.Is(err, unix.ENOTDIR):
		return entryAbsent, nil
	case errors.Is(err, unix.ENOTEMPTY), errors.Is(err, unix.EEXIST):
		return entryNonEmpty, nil
	default:
		return entryUnknown, classify("rmdir", e.String(), depth, err)
	}
}

// descend enters the listed child directory e. A child that vanished or
// stopped being a directory is skipped (moved=false), and the next scan sees
// what is there now. A child on another device or mount fails with
// ErrCrossDevice.
func (r *remover) descend(e at.Listed) (moved bool, err error) {
	if err := r.ctx.Err(); err != nil {
		return false, r.canceled(err)
	}
	depth := r.depth() + 1
	child, err := e.OpenDir()
	switch {
	case err == nil:
	case errors.Is(err, unix.ENOENT), errors.Is(err, unix.ENOTDIR), errors.Is(err, unix.ELOOP):
		return false, nil
	default:
		return false, classify("open", e.String(), depth, err)
	}
	childID, err := child.Stat()
	if err != nil {
		child.Close()
		return false, opError("stat", e.String(), depth, err)
	}
	if !r.anchorID.SameMount(childID) {
		child.Close()
		return false, crossDevice(e.String(), depth)
	}
	r.stack = append(r.stack, r.curIno)
	r.cur.Close()
	r.cur, r.curIno = child, childID.Ino()
	r.report.MaxDepth = max(r.report.MaxDepth, depth)
	r.dir.Full()
	return true, nil
}

// ascend returns from an emptied directory to its parent, and only when that
// parent is the directory it was entered from: same device, mount and inode.
// Anything else means the tree moved, and the remover stops with
// ErrTreeChanged rather than continue somewhere it never descended.
func (r *remover) ascend() error {
	if err := r.ctx.Err(); err != nil {
		return r.canceled(err)
	}
	depth := r.depth()
	want := r.stack[depth-1]
	up, err := r.cur.OpenParent()
	switch {
	case err == nil:
	case errors.Is(err, unix.ENOENT):
		return removedWhileHeld(depth)
	default:
		return classifyDir("open the parent of", depth, err)
	}
	upID, err := up.Stat()
	if err != nil {
		up.Close()
		return dirError("stat the parent of", depth, err)
	}
	if !r.anchorID.SameMount(upID) || upID.Ino() != want {
		up.Close()
		return movedError(depth)
	}
	r.cur.Close()
	r.cur, r.curIno = up, want
	r.stack = r.stack[:depth-1]
	r.stalls = 0
	r.dir.Shrink()
	return nil
}

// cut moves the non-empty listed child e of the directory being emptied,
// which sits at the depth bound, into the anchor under a fresh name. The
// subtree is then removed from the anchor like any other child, so the
// ancestor stack never grows past the bound.
//
// The call's first cut runs beginCuts first. Cut names count up from a
// random origin: names planted in the anchor beforehand cannot be predicted
// to collide, and a rerun of an interrupted removal does not retry the names
// it left behind.
func (r *remover) cut(e at.Listed) (moved bool, err error) {
	depth := r.depth() + 1
	if !r.cutsBegun {
		if err := r.beginCuts(e.String(), depth); err != nil {
			return false, err
		}
	}
	for range cutAttempts {
		var target Name
		target, err = cutName(r.cutNext)
		if err != nil {
			return false, err
		}
		r.cutNext++
		err = e.RenameNoReplaceInto(r.anchor, target)
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
			return false, cutError(e.String(), depth, err)
		}
	}
	return false, cutError(e.String(), depth, err)
}

// beginCuts prepares the call's first cut, once: it runs BeforeFirstCut on
// the anchor and seeds the cut names. It marks the cuts begun before the
// hook runs, so the hook runs at most once per call even when it fails; a
// failure refuses the cut with ErrCutRefused, and the call stops there.
func (r *remover) beginCuts(name string, depth int) error {
	r.cutsBegun = true
	if r.beforeFirstCut != nil {
		if err := r.runBeforeFirstCut(name, depth); err != nil {
			return err
		}
	}
	var seed [8]byte
	// crypto/rand.Read never fails since Go 1.24.
	_, _ = rand.Read(seed[:])
	r.cutNext = binary.NativeEndian.Uint64(seed[:])
	return nil
}

// runBeforeFirstCut lends BeforeFirstCut a duplicate of the anchor's
// descriptor for the duration of the call. Cuts rename into the anchor's own
// descriptor, which is never lent, so a hook that mishandles what it was
// lent, by closing it for example, cannot redirect a cut. The hook's error
// becomes a hookError.
func (r *remover) runBeforeFirstCut(name string, depth int) error {
	lent, err := r.anchor.Dup()
	if err != nil {
		return opError("duplicate the anchor for the cut of", name, depth, err)
	}
	defer lent.Close()
	var (
		lender  at.Lender
		hookErr error
	)
	if err := lender.Lend(lent.View(), func(anchor at.Borrowed) error {
		hookErr = r.beforeFirstCut(BorrowedDir{lent: anchor})
		return nil
	}); err != nil {
		return opError("lend the anchor for the cut of", name, depth, err)
	}
	if hookErr != nil {
		return &hookError{name: shortName(name), depth: depth, cause: hookErr}
	}
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
	r.cur.Close()
	r.cur = nil
	name := r.name.String()
	now, err := r.parent.StatChild(r.name)
	switch {
	case err == nil:
	case errors.Is(err, unix.ENOENT):
		return nil
	default:
		return opError("stat", name, 0, err)
	}
	if !now.Same(r.anchorID) {
		return fmt.Errorf("%w: %q no longer names the emptied directory", ErrTreeChanged, shortName(name))
	}
	err = r.parent.Rmdir(r.name)
	switch {
	case err == nil:
		r.report.Dirs++
		return nil
	case errors.Is(err, unix.ENOENT):
		return nil
	case errors.Is(err, unix.ENOTEMPTY), errors.Is(err, unix.EEXIST):
		return fmt.Errorf("%w: %q gained entries after it was emptied: %w", ErrTreeChanged, shortName(name), err)
	default:
		return classify("rmdir", name, 0, err)
	}
}
