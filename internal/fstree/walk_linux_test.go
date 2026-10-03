//go:build linux

package fstree

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/fstree/internal/at"
)

// recorder is a Visitor that records every visit by path, read back from
// /proc/self/fd, so tests can compare a walk with a path-based listing.
type recorder struct {
	t           *testing.T
	root        string // path the recorded paths are made relative to
	types       map[string]uint8
	visits      map[string]int
	onDirectory func(path string, depth int) error
	onEntry     func(path string, depth int) error
}

func newRecorder(t *testing.T, root string) *recorder {
	return &recorder{t: t, root: root, types: map[string]uint8{}, visits: map[string]int{}}
}

// dirPath reads the path of a lent directory back from /proc/self/fd.
func dirPath(t *testing.T, dir BorrowedDir) string {
	t.Helper()
	var path string
	require.NoError(t, dir.Control(func(fd int) error {
		var err error
		path, err = os.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd))
		return err
	}))
	return path
}

func (r *recorder) record(path string, dtype uint8, depth int) string {
	r.t.Helper()
	rel, err := filepath.Rel(r.root, path)
	require.NoError(r.t, err)
	require.False(r.t, rel == ".." || strings.HasPrefix(rel, "../"), "visited %q outside the tree", path)
	wantDepth := 0
	if rel != "." {
		wantDepth = strings.Count(rel, "/") + 1
	}
	require.Equal(r.t, wantDepth, depth, "depth of %q", rel)
	r.types[rel] = dtype
	r.visits[rel]++
	return path
}

func (r *recorder) Directory(dir BorrowedDir, depth int) error {
	path := r.record(dirPath(r.t, dir), unix.DT_DIR, depth)
	if r.onDirectory != nil {
		return r.onDirectory(path, depth)
	}
	return nil
}

func (r *recorder) Entry(parent BorrowedDir, name string, dtype uint8, depth int) error {
	path := r.record(filepath.Join(dirPath(r.t, parent), name), dtype, depth)
	if r.onEntry != nil {
		return r.onEntry(path, depth)
	}
	return nil
}

// walkTypes walks the entry at anchorPath, which must be a child of parent,
// requires every object to be visited exactly once, and returns each path
// relative to the anchor with the type the walk reported.
func walkTypes(t *testing.T, parent *os.File, anchorPath string) map[string]uint8 {
	t.Helper()
	rec := newRecorder(t, anchorPath)
	requireNoLeak(t, func() {
		_, err := walkBeneath(context.Background(), parent, mustName(filepath.Base(anchorPath)), rec, maxDepth)
		require.NoError(t, err)
	})
	for path, n := range rec.visits {
		require.Equal(t, 1, n, "visits of %q", path)
	}
	return rec.types
}

// listTypes lists root with filepath.WalkDir, which never follows symlinks,
// and returns each path relative to root with its d_type.
func listTypes(t *testing.T, root string) map[string]uint8 {
	t.Helper()
	out := map[string]uint8{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		switch mode := entry.Type(); {
		case mode.IsDir():
			out[rel] = unix.DT_DIR
		case mode&fs.ModeSymlink != 0:
			out[rel] = unix.DT_LNK
		case mode&fs.ModeNamedPipe != 0:
			out[rel] = unix.DT_FIFO
		case mode&fs.ModeSocket != 0:
			out[rel] = unix.DT_SOCK
		case mode.IsRegular():
			out[rel] = unix.DT_REG
		default:
			return fmt.Errorf("unexpected type %v at %q", mode, path)
		}
		return nil
	})
	require.NoError(t, err)
	return out
}

// A static tree is visited exactly once per object, with every object's
// type, in agreement with a separate path-based listing. Symlinks are
// reported and never followed: nothing outside the tree is visited.
func TestWalkBeneathVisitsEveryEntryOnce(t *testing.T) {
	base := tempDir(t)
	outside := makeOutside(t, base)
	parentPath := filepath.Join(base, "parent")
	anchor := filepath.Join(parentPath, "anchor")
	mkdirAll(t, filepath.Join(anchor, "nested", "deeper", "deepest"))
	mkdirAll(t, filepath.Join(anchor, "empty"))
	mkdirAll(t, filepath.Join(anchor, "wide"))
	mkdirAll(t, filepath.Join(anchor, "many"))
	writeFile(t, filepath.Join(anchor, "nested", "deeper", "deepest", "file"), "x")
	writeFile(t, filepath.Join(anchor, strings.Repeat("L", at.MaxNameLen)), "long name")
	writeFile(t, filepath.Join(anchor, "odd\nname\xff"), "odd name")
	require.NoError(t, os.Symlink(filepath.Join(outside, "dir"), filepath.Join(anchor, "to-outside")))
	require.NoError(t, os.Symlink("..", filepath.Join(anchor, "nested", "to-parent")))
	require.NoError(t, os.Symlink(parentPath, filepath.Join(anchor, "nested", "deeper", "to-tree-parent")))
	require.NoError(t, unix.Mkfifo(filepath.Join(anchor, "fifo"), 0o644))
	require.NoError(t, unix.Mknod(filepath.Join(anchor, "nested", "socket"), unix.S_IFSOCK|0o644, 0))
	// More than one batch in a directory, and descents in the middle of
	// batches, so that resuming at saved offsets is exercised.
	for i := range 600 {
		writeFile(t, filepath.Join(anchor, "wide", fmt.Sprintf("f%03d", i)), "")
	}
	for i := range 300 {
		dir := filepath.Join(anchor, "many", fmt.Sprintf("d%03d", i))
		require.NoError(t, os.Mkdir(dir, 0o755))
		writeFile(t, filepath.Join(dir, "x"), "")
		writeFile(t, filepath.Join(anchor, "many", fmt.Sprintf("f%03d", i)), "")
	}
	parent := openDir(t, parentPath)

	got := walkTypes(t, parent, anchor)
	require.Equal(t, listTypes(t, anchor), got)
	require.Equal(t, uint8(unix.DT_LNK), got["to-outside"])
	require.Equal(t, uint8(unix.DT_SOCK), got["nested/socket"])

	rec := newRecorder(t, anchor)
	report, err := walkBeneath(context.Background(), parent, mustName("anchor"), rec, maxDepth)
	require.NoError(t, err)
	require.Equal(t, 3, report.MaxDepth)
	var dirs, entries uint64
	for _, dtype := range got {
		if dtype == unix.DT_DIR {
			dirs++
		} else {
			entries++
		}
	}
	require.Equal(t, WalkReport{Dirs: dirs, Entries: entries, MaxDepth: 3}, report)
	require.True(t, report.Complete(), "a static tree is walked whole")
}

// A tree deeper than the bound fails with ErrTooDeep; one exactly at the
// bound is walked.
func TestWalkBeneathStopsAtTheDepthBound(t *testing.T) {
	parentPath := tempDir(t)
	parent := openDir(t, parentPath)
	buildChain(t, parent, "anchor", 20)

	requireNoLeak(t, func() {
		report, err := walkBeneath(context.Background(), parent, mustName("anchor"), newRecorder(t, filepath.Join(parentPath, "anchor")), 8)
		require.ErrorIs(t, err, ErrTooDeep)
		require.Equal(t, 8, report.MaxDepth)
	})
	requireNoLeak(t, func() {
		report, err := walkBeneath(context.Background(), parent, mustName("anchor"), newRecorder(t, filepath.Join(parentPath, "anchor")), 20)
		require.NoError(t, err)
		require.Equal(t, WalkReport{Dirs: 21, Entries: 1, MaxDepth: 20}, report)
	})
}

// funcVisitor adapts two functions to Visitor.
type funcVisitor struct {
	directory func(dir BorrowedDir, depth int) error
	entry     func(parent BorrowedDir, name string, dtype uint8, depth int) error
}

func (v funcVisitor) Directory(dir BorrowedDir, depth int) error {
	if v.directory == nil {
		return nil
	}
	return v.directory(dir, depth)
}

func (v funcVisitor) Entry(parent BorrowedDir, name string, dtype uint8, depth int) error {
	if v.entry == nil {
		return nil
	}
	return v.entry(parent, name, dtype, depth)
}

// A directory moved while the walk is inside it is detected when the walk
// climbs back, whether it moved within the tree or out of it.
func TestWalkBeneathDetectsAMovedAncestor(t *testing.T) {
	for name, destination := range map[string]string{
		"within the tree": filepath.Join("parent", "anchor", "moved"),
		"out of the tree": filepath.Join("outside", "moved"),
	} {
		t.Run(name, func(t *testing.T) {
			base := tempDir(t)
			outside := makeOutside(t, base)
			parentPath := filepath.Join(base, "parent")
			anchor := filepath.Join(parentPath, "anchor")
			mkdirAll(t, filepath.Join(anchor, "a", "b", "c"))
			writeFile(t, filepath.Join(anchor, "a", "b", "c", "f"), "x")
			parent := openDir(t, parentPath)
			before := snapshot(t, outside)

			// The walk keeps listing the directory it holds after the move,
			// as every descriptor-based walk does; it must stop when it climbs
			// out of it.
			moveHeld := funcVisitor{directory: func(dir BorrowedDir, depth int) error {
				if depth == 2 {
					require.NoError(t, os.Rename(dirPath(t, dir), filepath.Join(base, destination)))
				}
				return nil
			}}
			requireNoLeak(t, func() {
				_, err := walkBeneath(context.Background(), parent, mustName("anchor"), moveHeld, maxDepth)
				require.ErrorIs(t, err, ErrTreeChanged)
			})
			after := snapshot(t, outside)
			if destination == filepath.Join("outside", "moved") {
				for _, moved := range []string{"moved", "moved/c", "moved/c/f"} {
					require.Contains(t, after, moved)
					delete(after, moved)
				}
			}
			require.Equal(t, before, after, "a read-only walk changes nothing")
		})
	}
}

// A directory removed while the walk lists it fails the walk with
// ErrTreeChanged rather than an untyped read error.
func TestWalkBeneathDetectsARemovedDirectory(t *testing.T) {
	parentPath := tempDir(t)
	anchor := filepath.Join(parentPath, "anchor")
	mkdirAll(t, filepath.Join(anchor, "a", "b"))
	parent := openDir(t, parentPath)

	removeHeld := funcVisitor{directory: func(dir BorrowedDir, depth int) error {
		if depth == 2 {
			require.NoError(t, os.Remove(dirPath(t, dir)))
		}
		return nil
	}}
	requireNoLeak(t, func() {
		_, err := walkBeneath(context.Background(), parent, mustName("anchor"), removeHeld, maxDepth)
		require.ErrorIs(t, err, ErrTreeChanged)
	})
}

// A top entry that is not a directory is visited as one entry at depth 0
// with the parent's descriptor; an absent one fails with fs.ErrNotExist.
func TestWalkBeneathTopEntryKinds(t *testing.T) {
	base := tempDir(t)
	outside := makeOutside(t, base)
	parentPath := filepath.Join(base, "parent")
	mkdirAll(t, parentPath)
	writeFile(t, filepath.Join(parentPath, "file"), "x")
	require.NoError(t, os.Symlink(filepath.Join(outside, "dir"), filepath.Join(parentPath, "link")))
	require.NoError(t, unix.Mkfifo(filepath.Join(parentPath, "fifo"), 0o644))
	parent := openDir(t, parentPath)

	for name, want := range map[string]uint8{"file": unix.DT_REG, "link": unix.DT_LNK, "fifo": unix.DT_FIFO} {
		rec := newRecorder(t, filepath.Join(parentPath, name))
		requireNoLeak(t, func() {
			report, err := walkBeneath(context.Background(), parent, mustName(name), rec, maxDepth)
			require.NoError(t, err)
			require.Equal(t, WalkReport{Entries: 1}, report)
		})
		require.Equal(t, map[string]uint8{".": want}, rec.types, name)
	}

	_, err := WalkBeneath(context.Background(), parent, mustName("absent"), newRecorder(t, parentPath))
	require.ErrorIs(t, err, fs.ErrNotExist)
}

// The visitor's error stops the walk and comes back unchanged.
func TestWalkBeneathReturnsTheVisitorsError(t *testing.T) {
	parentPath := tempDir(t)
	anchor := filepath.Join(parentPath, "anchor")
	mkdirAll(t, filepath.Join(anchor, "a", "b"))
	writeFile(t, filepath.Join(anchor, "a", "b", "f"), "x")
	parent := openDir(t, parentPath)
	stop := errors.New("stop")

	rec := newRecorder(t, anchor)
	rec.onDirectory = func(_ string, depth int) error {
		if depth == 1 {
			return stop
		}
		return nil
	}
	requireNoLeak(t, func() {
		_, err := walkBeneath(context.Background(), parent, mustName("anchor"), rec, maxDepth)
		require.Same(t, stop, err)
	})

	rec = newRecorder(t, anchor)
	rec.onEntry = func(string, int) error { return stop }
	requireNoLeak(t, func() {
		_, err := walkBeneath(context.Background(), parent, mustName("anchor"), rec, maxDepth)
		require.Same(t, stop, err)
	})
}

func TestWalkBeneathHonorsCancellation(t *testing.T) {
	parentPath := tempDir(t)
	buildBushyChain(t, parentPath, "anchor", 20, 3)
	parent := openDir(t, parentPath)
	for _, checks := range []int64{0, 1, 5, 30} {
		requireNoLeak(t, func() {
			_, err := walkBeneath(newCancelAfter(checks), parent, mustName("anchor"), newRecorder(t, filepath.Join(parentPath, "anchor")), maxDepth)
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

func TestWalkBeneathRejectsInvalidCalls(t *testing.T) {
	parentPath := tempDir(t)
	parent := openDir(t, parentPath)
	_, err := WalkBeneath(context.Background(), parent, mustName("x"), nil)
	require.Error(t, err)
	_, err = WalkBeneath(context.Background(), parent, Name{}, newRecorder(t, parentPath))
	require.ErrorIs(t, err, ErrInvalidName, "the zero Name is refused")
	_, err = walkBeneath(context.Background(), parent, mustName("x"), newRecorder(t, parentPath), 0)
	require.Error(t, err)
}

// Tenants may write while an audit walks. Files appearing and disappearing
// in a directory being listed do not fail the walk, and the walk stays
// inside the tree.
func TestWalkBeneathToleratesConcurrentWriters(t *testing.T) {
	parentPath := tempDir(t)
	anchor := filepath.Join(parentPath, "anchor")
	hot := filepath.Join(anchor, "hot")
	mkdirAll(t, hot)
	buildBushyChain(t, anchor, "chain", 30, 2)
	parent := openDir(t, parentPath)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			path := filepath.Join(hot, fmt.Sprintf("f%d", i%16))
			if err := os.WriteFile(path, nil, 0o644); err == nil {
				_ = os.Remove(path)
			}
		}
	})
	defer wg.Wait()
	defer close(stop)

	for range 3 {
		rec := newRecorder(t, anchor)
		_, err := walkBeneath(context.Background(), parent, mustName("anchor"), rec, maxDepth)
		require.NoError(t, err)
		require.Equal(t, uint8(unix.DT_DIR), rec.types["chain"])
	}
}

// A listed directory that vanished, or stopped being a directory, before the
// walk came to it is passed over and counted, so the report says the walk
// did not observe the whole tree. Nothing behind a symlink is visited.
func TestWalkBeneathCountsWhatItPassesOver(t *testing.T) {
	cases := map[string]struct {
		change            func(t *testing.T, path, outside string)
		vanished, retyped uint64
	}{
		"vanished": {
			change:   func(t *testing.T, path, _ string) { require.NoError(t, os.Remove(path)) },
			vanished: 1,
		},
		"replaced by a file": {
			change: func(t *testing.T, path, _ string) {
				require.NoError(t, os.Remove(path))
				writeFile(t, path, "now a file")
			},
			retyped: 1,
		},
		"replaced by a symlink": {
			change: func(t *testing.T, path, outside string) {
				require.NoError(t, os.Remove(path))
				require.NoError(t, os.Symlink(filepath.Join(outside, "dir"), path))
			},
			retyped: 1,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			base := tempDir(t)
			outside := makeOutside(t, base)
			parentPath := filepath.Join(base, "parent")
			anchor := filepath.Join(parentPath, "anchor")
			mkdirAll(t, filepath.Join(anchor, "victim"))
			parent := openDir(t, parentPath)
			rec := newRecorder(t, anchor)

			requireNoLeak(t, func() {
				borrowParentView(t, parent, func(p at.View) {
					w := newWalker(context.Background(), p, mustName("anchor"), rec, maxDepth)
					defer w.release()
					opened, err := w.begin()
					require.NoError(t, err)
					require.True(t, opened)
					listed, _, eof, err := w.cur.ReadBatch(&w.dir, 0)
					require.NoError(t, err)
					require.False(t, eof)
					require.Len(t, listed, 1)

					tc.change(t, filepath.Join(anchor, "victim"), outside)
					descended, err := w.visitEntry(listed[0])
					require.NoError(t, err, "a passed-over entry does not fail the walk")
					require.False(t, descended)
					require.Equal(t, tc.vanished, w.report.Vanished)
					require.Equal(t, tc.retyped, w.report.Retyped)
					require.False(t, w.report.Complete())
				})
			})
			require.Equal(t, map[string]int{".": 1}, rec.visits, "nothing but the anchor was visited")
		})
	}
}

// Complete holds only when nothing was passed over.
func TestWalkReportComplete(t *testing.T) {
	require.True(t, WalkReport{}.Complete())
	require.True(t, WalkReport{Dirs: 3, Entries: 9, MaxDepth: 2}.Complete())
	require.False(t, WalkReport{Vanished: 1}.Complete())
	require.False(t, WalkReport{Retyped: 1}.Complete())
}

// A visitor's BorrowedDir is valid only during its call: one kept from an
// earlier call is refused during a later one, and every kept one is refused
// after the walk.
func TestWalkBeneathLendsDirectoriesOnlyForTheCall(t *testing.T) {
	parentPath := tempDir(t)
	anchor := filepath.Join(parentPath, "anchor")
	mkdirAll(t, filepath.Join(anchor, "sub"))
	writeFile(t, filepath.Join(anchor, "sub", "f"), "")
	parent := openDir(t, parentPath)

	var keptTop, keptParent BorrowedDir
	var staleChecked bool
	visitor := funcVisitor{
		directory: func(dir BorrowedDir, depth int) error {
			_, err := dir.Stat()
			require.NoError(t, err, "a lent directory works during its call")
			if depth == 0 {
				keptTop = dir
				return nil
			}
			ran := false
			require.Error(t, keptTop.Control(func(int) error { ran = true; return nil }),
				"a directory lent for an earlier call is refused")
			require.False(t, ran)
			staleChecked = true
			return nil
		},
		entry: func(parent BorrowedDir, _ string, _ uint8, _ int) error {
			keptParent = parent
			return nil
		},
	}
	requireNoLeak(t, func() {
		report, err := walkBeneath(context.Background(), parent, mustName("anchor"), visitor, maxDepth)
		require.NoError(t, err)
		require.True(t, report.Complete())
	})
	require.True(t, staleChecked)
	for _, kept := range []BorrowedDir{keptTop, keptParent} {
		ran := false
		require.Error(t, kept.Control(func(int) error { ran = true; return nil }))
		require.False(t, ran, "nothing runs after the walk")
		_, err := kept.Stat()
		require.Error(t, err)
	}
}

// A directory whose offsets do not advance would make the walk re-read the
// same batch forever; it fails instead.
func TestWalkBeneathRequiresOffsetsToAdvance(t *testing.T) {
	require.NoError(t, advanced(0, 7, 1))
	require.NoError(t, advanced(7, 3, 1))
	require.ErrorIs(t, advanced(7, 7, 1), ErrTreeChanged)
}
