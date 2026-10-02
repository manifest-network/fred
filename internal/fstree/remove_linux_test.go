//go:build linux

package fstree

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const deepChainChildEnv = "FSTREE_TEST_DEEP_CHAIN_CHILD"

// Go's RemoveAll holds one descriptor per level, so a chain deeper than
// RLIMIT_NOFILE fails with EMFILE on every attempt. RemoveBeneath's
// descriptor use does not depend on depth. The chain is built and removed in
// a child process whose soft RLIMIT_NOFILE is 64.
func TestRemoveBeneathRemovesChainDeeperThanTheDescriptorLimit(t *testing.T) {
	if os.Getenv(deepChainChildEnv) == "1" {
		removeDeepChainUnderLowFDLimit(t)
		return
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), executable,
		"-test.run=^TestRemoveBeneathRemovesChainDeeperThanTheDescriptorLimit$", "-test.v")
	command.Env = append(os.Environ(), deepChainChildEnv+"=1")
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.Contains(t, string(output), "--- PASS: TestRemoveBeneathRemovesChainDeeperThanTheDescriptorLimit (")
}

func removeDeepChainUnderLowFDLimit(t *testing.T) {
	const depth = 4096
	parentPath := tempDir(t)
	parent := openDir(t, parentPath)
	buildChain(t, parent, "anchor", depth)

	var limit unix.Rlimit
	require.NoError(t, unix.Getrlimit(unix.RLIMIT_NOFILE, &limit))
	require.NoError(t, unix.Setrlimit(unix.RLIMIT_NOFILE, &unix.Rlimit{Cur: 64, Max: limit.Max}))
	t.Cleanup(func() { _ = unix.Setrlimit(unix.RLIMIT_NOFILE, &limit) })

	root, err := os.OpenRoot(parentPath)
	require.NoError(t, err)
	controlErr := root.RemoveAll("anchor")
	require.NoError(t, root.Close())
	require.ErrorIs(t, controlErr, syscall.EMFILE, "control: Go's RemoveAll needs one descriptor per level")

	before := fdCount(t)
	report, err := removeBeneath(context.Background(), parent, mustName("anchor"), RemoveOptions{}, maxDepth)
	require.NoError(t, err)
	require.Equal(t, before, fdCount(t), "descriptors leaked")
	requireAbsent(t, filepath.Join(parentPath, "anchor"))
	require.Equal(t, RemoveReport{Entries: 1, Dirs: depth + 1, MaxDepth: depth}, report)
}

// Past the depth bound, RemoveBeneath moves the deeper subtree into the
// anchor and removes it from there; nothing it creates outlives the call.
func TestRemoveBeneathCutsATreeDeeperThanTheBound(t *testing.T) {
	const depth, limit = 1000, 8
	parentPath := tempDir(t)
	writeFile(t, filepath.Join(parentPath, "keep"), "sibling")
	parent := openDir(t, parentPath)
	buildChain(t, parent, "anchor", depth)

	var report RemoveReport
	requireNoLeak(t, func() {
		var err error
		report, err = removeBeneath(context.Background(), parent, mustName("anchor"), RemoveOptions{}, limit)
		require.NoError(t, err)
	})
	require.GreaterOrEqual(t, report.Cuts, uint64(124))
	require.Equal(t, uint64(depth+1), report.Dirs)
	require.Equal(t, uint64(1), report.Entries)
	require.Equal(t, limit, report.MaxDepth)
	require.Equal(t, []string{"keep"}, listNames(t, parentPath))
}

// A canceled removal leaves a consistent tree, cut subtrees included, and a
// rerun finishes it (I5).
func TestRemoveBeneathRerunFinishesACanceledRemoval(t *testing.T) {
	var canceled, canceledWithCuts, completed int
	for _, checks := range []int64{0, 1, 2, 3, 5, 8, 13, 40, 100, 200, 300, 400, 1000, 1 << 20} {
		t.Run(fmt.Sprintf("after_%d_checks", checks), func(t *testing.T) {
			parentPath := tempDir(t)
			buildBushyChain(t, parentPath, "anchor", 40, 3)
			anchor := filepath.Join(parentPath, "anchor")
			parent := openDir(t, parentPath)

			requireNoLeak(t, func() {
				_, err := removeBeneath(newCancelAfter(checks), parent, mustName("anchor"), RemoveOptions{}, 8)
				if err == nil {
					completed++
					return
				}
				require.ErrorIs(t, err, context.Canceled)
				require.DirExists(t, anchor)
				canceled++
				for _, name := range listNames(t, anchor) {
					if strings.HasPrefix(name, cutPrefix) {
						canceledWithCuts++
						break
					}
				}
			})
			requireNoLeak(t, func() {
				_, err := removeBeneath(context.Background(), parent, mustName("anchor"), RemoveOptions{}, 8)
				require.NoError(t, err)
			})
			requireAbsent(t, anchor)
			require.Empty(t, listNames(t, parentPath))
		})
	}
	require.Positive(t, canceled)
	require.Positive(t, canceledWithCuts, "some cancellation must leave cut subtrees for the rerun")
	require.Positive(t, completed)
}

// Symlinks inside the tree are removed as links; nothing they point at is
// touched (I2).
func TestRemoveBeneathNeverFollowsSymlinks(t *testing.T) {
	base := tempDir(t)
	outside := makeOutside(t, base)
	before := snapshot(t, outside)
	parentPath := filepath.Join(base, "parent")
	anchor := filepath.Join(parentPath, "anchor")
	mkdirAll(t, filepath.Join(anchor, "sub", "deeper"))
	require.NoError(t, os.Symlink(filepath.Join(outside, "dir"), filepath.Join(anchor, "to-dir")))
	require.NoError(t, os.Symlink(filepath.Join(outside, "file"), filepath.Join(anchor, "to-file")))
	require.NoError(t, os.Symlink("../../../outside/dir", filepath.Join(anchor, "sub", "relative")))
	require.NoError(t, os.Symlink("/", filepath.Join(anchor, "sub", "deeper", "to-root")))
	require.NoError(t, os.Symlink("..", filepath.Join(anchor, "sub", "deeper", "to-parent")))
	require.NoError(t, os.Symlink(base, filepath.Join(anchor, "sub", "to-base")))
	require.NoError(t, os.Symlink(filepath.Join(outside, "missing"), filepath.Join(anchor, "dangling")))
	require.NoError(t, os.Link(filepath.Join(outside, "file"), filepath.Join(anchor, "hard-link")))
	parent := openDir(t, parentPath)

	var report RemoveReport
	requireNoLeak(t, func() {
		var err error
		report, err = removeBeneath(context.Background(), parent, mustName("anchor"), RemoveOptions{}, maxDepth)
		require.NoError(t, err)
	})
	requireAbsent(t, anchor)
	require.Equal(t, before, snapshot(t, outside))
	require.Equal(t, RemoveReport{Entries: 8, Dirs: 3, MaxDepth: 2}, report)
}

// When the entry itself is a symlink, only the link goes, whatever it points
// at.
func TestRemoveBeneathRemovesOnlyTheLinkWhenTheEntryIsASymlink(t *testing.T) {
	for _, target := range []string{"dir", "file", "missing"} {
		t.Run(target, func(t *testing.T) {
			base := tempDir(t)
			outside := makeOutside(t, base)
			before := snapshot(t, outside)
			parentPath := filepath.Join(base, "parent")
			mkdirAll(t, parentPath)
			require.NoError(t, os.Symlink(filepath.Join(outside, target), filepath.Join(parentPath, "anchor")))
			parent := openDir(t, parentPath)
			var called bool

			report, err := removeBeneath(context.Background(), parent, mustName("anchor"), RemoveOptions{
				BeforeFirstCut: func(int) error { called = true; return nil },
			}, 1)
			require.NoError(t, err)
			require.Equal(t, RemoveReport{Entries: 1}, report)
			require.False(t, called, "a non-directory entry has no anchor and needs no cut")
			requireAbsent(t, filepath.Join(parentPath, "anchor"))
			require.Equal(t, before, snapshot(t, outside))
		})
	}
}

// A directory the remover holds that is moved out of the tree is detected
// at the next ascent; the remover never continues in the directory it
// landed in (I1).
func TestRemoveBeneathDetectsAHeldDirectoryMovedOutOfTheTree(t *testing.T) {
	base := tempDir(t)
	outside := makeOutside(t, base)
	parentPath := filepath.Join(base, "parent")
	anchor := filepath.Join(parentPath, "anchor")
	mkdirAll(t, filepath.Join(anchor, "a", "b", "c"))
	writeFile(t, filepath.Join(anchor, "a", "b", "c", "f"), "doomed")
	writeFile(t, filepath.Join(anchor, "a", "sibling"), "kept: a is never re-entered")
	held := filepath.Join(anchor, "a", "b")
	heldIno := inode(t, held)
	before := snapshot(t, outside)
	parent := openDir(t, parentPath)

	requireNoLeak(t, func() {
		r := newRemover(context.Background(), int(parent.Fd()), mustName("anchor"), RemoveOptions{}, maxDepth)
		defer r.release()
		opened, err := r.begin()
		require.NoError(t, err)
		require.True(t, opened)
		for r.depth() < 2 {
			empty, err := r.step()
			require.NoError(t, err)
			require.False(t, empty)
		}
		require.Equal(t, heldIno, r.curIno, "the remover holds a/b")

		require.NoError(t, os.Rename(held, filepath.Join(outside, "b")))
		var stepErr error
		for range 64 {
			if _, stepErr = r.step(); stepErr != nil {
				break
			}
		}
		require.ErrorIs(t, stepErr, ErrTreeChanged)
	})
	// The moved directory was emptied (every descriptor-based remover does
	// that), but the remover never acted in its new parent.
	after := snapshot(t, outside)
	require.Equal(t, "drwxr-xr-x", after["b"])
	delete(after, "b")
	require.Equal(t, before, after)
	require.Equal(t, "-rw-r--r-- kept: a is never re-entered", snapshot(t, anchor)["a/sibling"])
}

// The anchor is removed only while the parent's name still binds the
// directory that was emptied.
func TestRemoveBeneathLeavesASwappedAnchorAlone(t *testing.T) {
	cases := map[string]func(t *testing.T, anchor string){
		"directory": func(t *testing.T, anchor string) {
			mkdirAll(t, anchor)
			writeFile(t, filepath.Join(anchor, "newcomer"), "kept")
		},
		"empty directory": func(t *testing.T, anchor string) { mkdirAll(t, anchor) },
		"file":            func(t *testing.T, anchor string) { writeFile(t, anchor, "kept") },
		"symlink":         func(t *testing.T, anchor string) { require.NoError(t, os.Symlink("/", anchor)) },
	}
	for name, swapIn := range cases {
		t.Run(name, func(t *testing.T) {
			parentPath := tempDir(t)
			anchor := filepath.Join(parentPath, "anchor")
			mkdirAll(t, filepath.Join(anchor, "x", "y"))
			writeFile(t, filepath.Join(anchor, "x", "y", "z"), "doomed")
			parent := openDir(t, parentPath)

			requireNoLeak(t, func() {
				r := newRemover(context.Background(), int(parent.Fd()), mustName("anchor"), RemoveOptions{}, maxDepth)
				defer r.release()
				opened, err := r.begin()
				require.NoError(t, err)
				require.True(t, opened)
				for {
					empty, err := r.step()
					require.NoError(t, err)
					if empty {
						break
					}
				}
				require.NoError(t, os.Rename(anchor, filepath.Join(parentPath, "emptied")))
				swapIn(t, anchor)
				want := snapshot(t, parentPath)

				require.ErrorIs(t, r.finish(), ErrTreeChanged)
				require.Equal(t, want, snapshot(t, parentPath), "nothing is removed")
				require.Equal(t, uint64(2), r.report.Dirs, "only x and y were removed")
			})
		})
	}
}

// A directory the remover holds that someone else removes ends the removal
// with ErrTreeChanged instead of an untyped read error.
func TestRemoveBeneathDetectsAHeldDirectoryRemovedByAnother(t *testing.T) {
	parentPath := tempDir(t)
	anchor := filepath.Join(parentPath, "anchor")
	mkdirAll(t, filepath.Join(anchor, "a", "b"))
	writeFile(t, filepath.Join(anchor, "a", "b", "f1"), "")
	writeFile(t, filepath.Join(anchor, "a", "b", "f2"), "")
	parent := openDir(t, parentPath)

	requireNoLeak(t, func() {
		r := newRemover(context.Background(), int(parent.Fd()), mustName("anchor"), RemoveOptions{}, maxDepth)
		defer r.release()
		opened, err := r.begin()
		require.NoError(t, err)
		require.True(t, opened)
		for r.depth() < 2 {
			_, err := r.step()
			require.NoError(t, err)
		}
		require.NoError(t, os.RemoveAll(filepath.Join(anchor, "a", "b")))
		_, err = r.step()
		require.ErrorIs(t, err, ErrTreeChanged)
	})
	require.DirExists(t, filepath.Join(anchor, "a"))
}

// Entries that appear in the anchor after it was emptied keep it: the
// final rmdir fails, and the remover reports ErrTreeChanged.
func TestRemoveBeneathKeepsAnAnchorThatGainsEntries(t *testing.T) {
	parentPath := tempDir(t)
	anchor := filepath.Join(parentPath, "anchor")
	mkdirAll(t, filepath.Join(anchor, "x"))
	parent := openDir(t, parentPath)

	requireNoLeak(t, func() {
		r := newRemover(context.Background(), int(parent.Fd()), mustName("anchor"), RemoveOptions{}, maxDepth)
		defer r.release()
		opened, err := r.begin()
		require.NoError(t, err)
		require.True(t, opened)
		for {
			empty, err := r.step()
			require.NoError(t, err)
			if empty {
				break
			}
		}
		writeFile(t, filepath.Join(anchor, "late"), "kept")
		err = r.finish()
		require.ErrorIs(t, err, ErrTreeChanged)
		require.ErrorIs(t, err, unix.ENOTEMPTY)
	})
	require.Equal(t, []string{"late"}, listNames(t, anchor))
}

// A cut whose name is taken tries the next one; when every attempt is
// taken it fails without losing the subtree, and not as ErrCutRefused.
func TestRemoveBeneathCutRetriesTakenNames(t *testing.T) {
	for _, taken := range []int{2, cutAttempts} {
		t.Run(fmt.Sprintf("%d_taken", taken), func(t *testing.T) {
			parentPath := tempDir(t)
			anchor := filepath.Join(parentPath, "anchor")
			mkdirAll(t, filepath.Join(anchor, "a", "b"))
			writeFile(t, filepath.Join(anchor, "a", "b", "f"), "moved, never lost")
			parent := openDir(t, parentPath)

			r := newRemover(context.Background(), int(parent.Fd()), mustName("anchor"), RemoveOptions{}, 1)
			defer r.release()
			opened, err := r.begin()
			require.NoError(t, err)
			require.True(t, opened)
			_, err = r.step()
			require.NoError(t, err)
			require.Equal(t, 1, r.depth(), "the remover holds a, at the depth bound")

			r.cutsBegun, r.cutNext = true, 0x10
			for i := range taken {
				mkdirAll(t, filepath.Join(anchor, fmt.Sprintf("%s%016x", cutPrefix, 0x10+i)))
			}
			_, err = r.step()
			if taken == cutAttempts {
				require.ErrorIs(t, err, unix.EEXIST)
				require.NotErrorIs(t, err, ErrCutRefused)
				require.FileExists(t, filepath.Join(anchor, "a", "b", "f"))
				return
			}
			require.NoError(t, err)
			require.Equal(t, uint64(1), r.report.Cuts)
			moved := filepath.Join(anchor, fmt.Sprintf("%s%016x", cutPrefix, 0x10+taken))
			require.FileExists(t, filepath.Join(moved, "f"))
			for {
				empty, err := r.step()
				require.NoError(t, err)
				if empty {
					break
				}
			}
			require.NoError(t, r.finish())
			requireAbsent(t, anchor)
		})
	}
}

// An anchor whose name is gone by the time it is emptied counts as removed.
func TestRemoveBeneathTreatsAVanishedAnchorAsRemoved(t *testing.T) {
	parentPath := tempDir(t)
	anchor := filepath.Join(parentPath, "anchor")
	mkdirAll(t, filepath.Join(anchor, "x"))
	parent := openDir(t, parentPath)

	r := newRemover(context.Background(), int(parent.Fd()), mustName("anchor"), RemoveOptions{}, maxDepth)
	defer r.release()
	err := drive(r, func() {
		if _, err := os.Lstat(anchor); err == nil {
			require.NoError(t, os.Rename(anchor, filepath.Join(parentPath, "elsewhere")))
		}
	})
	require.NoError(t, err)
	requireAbsent(t, anchor)
}

// A 20,000-entry directory with NAME_MAX names and a directory of 2,000
// non-empty subdirectories are removed without the live heap growing with
// either: the remover holds one batch, never the directory.
func TestRemoveBeneathRemovesWideTreesInBoundedMemory(t *testing.T) {
	const files, subdirs = 20000, 2000
	parentPath := tempDir(t)
	anchor := filepath.Join(parentPath, "anchor")
	mkdirAll(t, filepath.Join(anchor, "wide"))
	mkdirAll(t, filepath.Join(anchor, "many"))
	wide := openDir(t, filepath.Join(anchor, "wide"))
	padding := strings.Repeat("n", maxNameLen-6)
	for i := range files {
		fd, err := unix.Openat(int(wide.Fd()), fmt.Sprintf("%s%06d", padding, i),
			unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC, 0o644)
		require.NoError(t, err)
		require.NoError(t, unix.Close(fd))
	}
	for i := range subdirs {
		dir := filepath.Join(anchor, "many", fmt.Sprintf("d%04d", i))
		require.NoError(t, os.Mkdir(dir, 0o755))
		writeFile(t, filepath.Join(dir, "x"), "")
	}
	require.NoError(t, wide.Close())
	parent := openDir(t, parentPath)

	r := newRemover(context.Background(), int(parent.Fd()), mustName("anchor"), RemoveOptions{}, maxDepth)
	defer r.release()
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var steps int
	var peak uint64
	err := drive(r, func() {
		if steps++; steps%64 != 0 {
			return
		}
		runtime.GC()
		var now runtime.MemStats
		runtime.ReadMemStats(&now)
		if now.HeapAlloc > base.HeapAlloc {
			peak = max(peak, now.HeapAlloc-base.HeapAlloc)
		}
	})
	require.NoError(t, err)
	requireAbsent(t, anchor)
	require.Equal(t, uint64(files+subdirs), r.report.Entries)
	require.Equal(t, uint64(subdirs+3), r.report.Dirs)
	// The names alone are about 5 MB; the remover keeps one batch of them.
	t.Logf("peak live heap growth over %d steps: %d bytes", steps, peak)
	require.Less(t, peak, uint64(2<<20), "live heap grew with the tree")
}

// An entry the caller may not remove stops the removal with ErrUndeletable,
// keeps what is left, and a rerun after the cause is fixed finishes (I6).
func TestRemoveBeneathStopsAtAnEntryItCannotRemove(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permission bits do not bind root")
	}
	// locked holds the content that cannot go: a file, or with nested a
	// directory, so that the unlink refused is that of a directory.
	cases := map[string]struct {
		mode   os.FileMode
		nested bool
	}{
		"file in an unwritable directory":      {mode: 0o500},
		"directory in an unwritable directory": {mode: 0o555, nested: true},
		"unreadable directory":                 {mode: 0o300},
		"inaccessible directory":               {mode: 0o000},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			parentPath := tempDir(t)
			anchor := filepath.Join(parentPath, "anchor")
			locked := filepath.Join(anchor, "locked")
			kept := filepath.Join(locked, "file")
			if tc.nested {
				kept = filepath.Join(locked, "inner", "file")
			}
			mkdirAll(t, filepath.Dir(kept))
			writeFile(t, kept, "kept")
			writeFile(t, filepath.Join(anchor, "other"), "")
			require.NoError(t, os.Chmod(locked, tc.mode))
			t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
			parent := openDir(t, parentPath)

			requireNoLeak(t, func() {
				_, err := removeBeneath(context.Background(), parent, mustName("anchor"), RemoveOptions{}, maxDepth)
				require.ErrorIs(t, err, ErrUndeletable)
				require.ErrorIs(t, err, unix.EACCES)
			})
			require.NoError(t, os.Chmod(locked, 0o700))
			requireContent(t, kept, "kept")
			requireNoLeak(t, func() {
				_, err := removeBeneath(context.Background(), parent, mustName("anchor"), RemoveOptions{}, maxDepth)
				require.NoError(t, err)
			})
			requireAbsent(t, anchor)
		})
	}
}

// An empty directory needs only its parent's permission, whatever its own
// mode is.
func TestRemoveBeneathRemovesAnEmptyInaccessibleDirectory(t *testing.T) {
	parentPath := tempDir(t)
	locked := filepath.Join(parentPath, "anchor", "locked")
	mkdirAll(t, locked)
	require.NoError(t, os.Chmod(locked, 0o000))
	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
	parent := openDir(t, parentPath)

	report, err := removeBeneath(context.Background(), parent, mustName("anchor"), RemoveOptions{}, maxDepth)
	require.NoError(t, err)
	require.Equal(t, RemoveReport{Dirs: 2}, report)
	requireAbsent(t, filepath.Join(parentPath, "anchor"))
}

// Every kind of top entry is handled: non-directories are unlinked without
// being opened, an absent name is already removed.
func TestRemoveBeneathTopEntryKinds(t *testing.T) {
	cases := map[string]struct {
		create func(t *testing.T, path string)
		want   RemoveReport
	}{
		"absent":          {create: func(*testing.T, string) {}, want: RemoveReport{}},
		"regular file":    {create: func(t *testing.T, path string) { writeFile(t, path, "x") }, want: RemoveReport{Entries: 1}},
		"empty directory": {create: func(t *testing.T, path string) { mkdirAll(t, path) }, want: RemoveReport{Dirs: 1}},
		"fifo": {
			create: func(t *testing.T, path string) { require.NoError(t, unix.Mkfifo(path, 0o644)) },
			want:   RemoveReport{Entries: 1},
		},
		"socket": {
			create: func(t *testing.T, path string) { require.NoError(t, unix.Mknod(path, unix.S_IFSOCK|0o644, 0)) },
			want:   RemoveReport{Entries: 1},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			parentPath := tempDir(t)
			tc.create(t, filepath.Join(parentPath, "entry"))
			parent := openDir(t, parentPath)
			requireNoLeak(t, func() {
				report, err := removeBeneath(context.Background(), parent, mustName("entry"), RemoveOptions{}, maxDepth)
				require.NoError(t, err)
				require.Equal(t, tc.want, report)
			})
			require.Empty(t, listNames(t, parentPath))
		})
	}
}

func TestRemoveBeneathRejectsInvalidCalls(t *testing.T) {
	parentPath := tempDir(t)
	writeFile(t, filepath.Join(parentPath, "keep"), "x")
	parent := openDir(t, parentPath)
	ctx := context.Background()

	_, err := RemoveBeneath(ctx, parent, Name{}, RemoveOptions{})
	require.ErrorIs(t, err, ErrInvalidName, "the zero Name is refused")
	_, err = RemoveBeneath(nil, parent, mustName("keep"), RemoveOptions{}) //nolint:staticcheck // the nil context is the case under test
	require.Error(t, err)
	_, err = RemoveBeneath(ctx, nil, mustName("keep"), RemoveOptions{})
	require.Error(t, err)
	_, err = removeBeneath(ctx, parent, mustName("keep"), RemoveOptions{}, 0)
	require.Error(t, err)

	closed, err := os.Open(parentPath)
	require.NoError(t, err)
	require.NoError(t, closed.Close())
	_, err = RemoveBeneath(ctx, closed, mustName("keep"), RemoveOptions{})
	require.Error(t, err, "a closed parent is refused, never used by number")
	require.Equal(t, []string{"keep"}, listNames(t, parentPath))
}

// ParseName accepts exactly the single path components, and its error names
// at most a bounded prefix of what it refused.
func TestParseName(t *testing.T) {
	for _, name := range []string{"a", "anchor", "...", ".a", "a.", " ", "\xff", strings.Repeat("n", maxNameLen)} {
		parsed, err := ParseName(name)
		require.NoError(t, err, "%q", name)
		require.Equal(t, name, parsed.String())
	}
	for _, name := range []string{
		"", ".", "..", "a/b", "/", "/abs", "trailing/", "../up", "nul\x00byte", strings.Repeat("n", maxNameLen+1),
	} {
		parsed, err := ParseName(name)
		require.ErrorIs(t, err, ErrInvalidName, "%q", name)
		require.Equal(t, Name{}, parsed, "a refused name yields the zero Name")
		require.LessOrEqual(t, len(err.Error()), 120, "error text is bounded")
	}
	require.Empty(t, Name{}.String())
}

// A tree no deeper than the depth bound needs no cut, so BeforeFirstCut is
// never called; one level more needs a cut and calls it once.
func TestRemoveBeneathBeforeFirstCutOnlyWhenACutIsNeeded(t *testing.T) {
	const limit = 4
	for _, tc := range []struct {
		depth     int
		wantCalls int
	}{
		{depth: 0},
		{depth: 1},
		{depth: limit},
		{depth: limit + 1, wantCalls: 1},
		{depth: 10 * limit, wantCalls: 1},
	} {
		t.Run(fmt.Sprintf("depth_%d", tc.depth), func(t *testing.T) {
			parentPath := tempDir(t)
			parent := openDir(t, parentPath)
			buildChain(t, parent, "anchor", tc.depth)
			var calls int
			var report RemoveReport
			requireNoLeak(t, func() {
				var err error
				report, err = removeBeneath(context.Background(), parent, mustName("anchor"), RemoveOptions{
					BeforeFirstCut: func(int) error { calls++; return nil },
				}, limit)
				require.NoError(t, err)
			})
			require.Equal(t, tc.wantCalls, calls)
			require.Equal(t, tc.wantCalls > 0, report.Cuts > 0, "the hook runs exactly when a cut happens")
			requireAbsent(t, filepath.Join(parentPath, "anchor"))
		})
	}

	// A bushy tree within the bound never calls it either.
	parentPath := tempDir(t)
	buildBushyChain(t, parentPath, "anchor", limit, 3)
	parent := openDir(t, parentPath)
	report, err := removeBeneath(context.Background(), parent, mustName("anchor"), RemoveOptions{
		BeforeFirstCut: func(int) error { t.Fatal("BeforeFirstCut called for a tree within the bound"); return nil },
	}, limit+1)
	require.NoError(t, err)
	require.Zero(t, report.Cuts)
	requireAbsent(t, filepath.Join(parentPath, "anchor"))
}

// BeforeFirstCut runs on the anchor, once per call, at the depth bound and
// before the first cut; every later cut proceeds without it.
func TestRemoveBeneathBeforeFirstCutRunsOnceBeforeTheFirstCut(t *testing.T) {
	const depth, limit = 200, 8
	parentPath := tempDir(t)
	parent := openDir(t, parentPath)
	buildChain(t, parent, "anchor", depth)
	anchorIno := inode(t, filepath.Join(parentPath, "anchor"))

	var calls int
	var r *remover
	r = newRemover(context.Background(), int(parent.Fd()), mustName("anchor"), RemoveOptions{
		BeforeFirstCut: func(fd int) error {
			calls++
			var st unix.Stat_t
			require.NoError(t, unix.Fstat(fd, &st))
			require.Equal(t, anchorIno, st.Ino, "the hook receives the anchor")
			require.Equal(t, limit, r.depth(), "the remover is at the depth bound")
			require.Zero(t, r.report.Cuts, "nothing was cut before the hook")
			return nil
		},
	}, limit)
	requireNoLeak(t, func() {
		defer r.release()
		require.NoError(t, drive(r, nil))
	})
	require.Equal(t, 1, calls)
	require.Greater(t, r.report.Cuts, uint64(1), "later cuts do not call the hook again")
	requireAbsent(t, filepath.Join(parentPath, "anchor"))
}

// A failing BeforeFirstCut refuses the cut: the call stops with an error
// wrapping ErrCutRefused and the hook's error, having removed what lay
// within the bound and cut nothing, and a rerun calls the hook again.
func TestRemoveBeneathBeforeFirstCutErrorRefusesTheCut(t *testing.T) {
	const depth, limit = 50, 4
	parentPath := tempDir(t)
	parent := openDir(t, parentPath)
	buildChain(t, parent, "anchor", depth)
	anchor := filepath.Join(parentPath, "anchor")
	refuse := errors.New("refused")

	var calls int
	var report RemoveReport
	requireNoLeak(t, func() {
		var err error
		report, err = removeBeneath(context.Background(), parent, mustName("anchor"), RemoveOptions{
			BeforeFirstCut: func(int) error { calls++; return refuse },
		}, limit)
		require.ErrorIs(t, err, ErrCutRefused)
		require.ErrorIs(t, err, refuse)
		require.Less(t, len(err.Error()), 400, "error text stays bounded: %s", err)
	})
	require.Equal(t, 1, calls)
	require.Zero(t, report.Cuts)
	require.Equal(t, limit, report.MaxDepth, "removal proceeded down to the bound")
	for _, name := range listNames(t, anchor) {
		require.False(t, strings.HasPrefix(name, cutPrefix), "nothing was cut: %q", name)
	}
	require.FileExists(t, filepath.Join(anchor, strings.Repeat("d/", depth)+"leaf"), "the deep subtree is kept")

	calls = 0
	requireNoLeak(t, func() {
		_, err := removeBeneath(context.Background(), parent, mustName("anchor"), RemoveOptions{
			BeforeFirstCut: func(int) error { calls++; return nil },
		}, limit)
		require.NoError(t, err)
	})
	require.Equal(t, 1, calls, "a rerun calls the hook at its own first cut")
	requireAbsent(t, anchor)
}

// A non-directory entry has no anchor and needs no cut.
func TestRemoveBeneathBeforeFirstCutNotCalledForANonDirectory(t *testing.T) {
	parentPath := tempDir(t)
	writeFile(t, filepath.Join(parentPath, "entry"), "x")
	parent := openDir(t, parentPath)
	report, err := removeBeneath(context.Background(), parent, mustName("entry"), RemoveOptions{
		BeforeFirstCut: func(int) error { t.Fatal("BeforeFirstCut called for a file"); return nil },
	}, 1)
	require.NoError(t, err)
	require.Equal(t, RemoveReport{Entries: 1}, report)
}

// A listing that keeps naming entries lookups cannot find fails with
// ErrTreeChanged on the second empty-handed iteration instead of spinning
// (I4).
func TestRemoveBeneathFailsInsteadOfSpinning(t *testing.T) {
	parentPath := tempDir(t)
	mkdirAll(t, filepath.Join(parentPath, "anchor"))
	parent := openDir(t, parentPath)
	r := newRemover(context.Background(), int(parent.Fd()), mustName("anchor"), RemoveOptions{}, maxDepth)
	defer r.release()
	opened, err := r.begin()
	require.NoError(t, err)
	require.True(t, opened)

	ghosts := []dirent{{name: "ghost-1"}, {name: "ghost-2"}}
	progressed, err := r.drain(ghosts)
	require.NoError(t, err)
	require.False(t, progressed)
	require.NoError(t, r.account(progressed), "one empty-handed iteration is retried")
	progressed, err = r.drain(ghosts)
	require.NoError(t, err)
	require.ErrorIs(t, r.account(progressed), ErrTreeChanged)

	r.stalls = 1
	require.NoError(t, r.account(true), "progress resets the count")
	require.Zero(t, r.stalls)
}

func TestClassifyAndCutErrors(t *testing.T) {
	for _, errno := range []unix.Errno{unix.EPERM, unix.EACCES, unix.EBUSY, unix.EROFS} {
		err := classify("unlink", "x", 3, errno)
		require.ErrorIs(t, err, ErrUndeletable)
		require.ErrorIs(t, err, errno)
		require.ErrorIs(t, cutError("x", 3, errno), ErrUndeletable)
	}
	for _, errno := range []unix.Errno{unix.EIO, unix.ENOTEMPTY, unix.EMFILE, unix.EXDEV} {
		err := classify("rmdir", "x", 3, errno)
		require.NotErrorIs(t, err, ErrUndeletable)
		require.ErrorIs(t, err, errno)
	}
	for _, errno := range []unix.Errno{unix.EXDEV, unix.EDQUOT, unix.ENOSPC, unix.EMLINK} {
		err := cutError("x", 9, errno)
		require.ErrorIs(t, err, ErrCutRefused)
		require.ErrorIs(t, err, errno)
		require.Contains(t, err.Error(), `rename "x" at depth 9`)
	}
	err := cutError("x", 9, unix.EEXIST)
	require.NotErrorIs(t, err, ErrCutRefused)
	require.ErrorIs(t, err, unix.EEXIST)
	// The default arm: an error that is no errno at all keeps its identity
	// and joins no sentinel.
	other := errors.New("not an errno")
	for _, err := range []error{classify("open", "x", 1, other), cutError("x", 1, other)} {
		require.ErrorIs(t, err, other)
		for _, sentinel := range []error{ErrUndeletable, ErrCutRefused, ErrTreeChanged, ErrCrossDevice} {
			require.NotErrorIs(t, err, sentinel)
		}
	}

	long := strings.Repeat("\xff", maxNameLen)
	for _, err := range []error{
		classify("unlink", long, 1<<16, unix.EACCES),
		classify("unlink", long, 1<<16, unix.EIO),
		cutError(long, 1<<16, unix.EXDEV),
		crossDevice(long, 1<<16),
	} {
		require.Less(t, len(err.Error()), 400, "error text stays bounded: %s", err)
	}
}

// Disjoint removals share no state; run under -race.
func TestRemoveBeneathConcurrentRemovalsOfDisjointTrees(t *testing.T) {
	const trees = 4
	parentPath := tempDir(t)
	for i := range trees {
		buildBushyChain(t, parentPath, fmt.Sprintf("anchor-%d", i), 30, 4)
	}
	parent := openDir(t, parentPath)
	errs := make([]error, trees)
	var wg sync.WaitGroup
	for i := range trees {
		wg.Go(func() {
			_, errs[i] = removeBeneath(context.Background(), parent, mustName(fmt.Sprintf("anchor-%d", i)), RemoveOptions{}, 4)
		})
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "tree %d", i)
	}
	require.Empty(t, listNames(t, parentPath))
}

// Random trees mixing directories, files, symlinks, hard links, FIFOs,
// sockets and empty directories, with random names and a random depth
// bound: a nil result means the entry is gone and everything outside it is
// byte-identical. The walk over the same tree visits each object once.
func TestRemoveBeneathRandomTrees(t *testing.T) {
	for seed := range uint64(40) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 0xf57ee))
			base := tempDir(t)
			outside := makeOutside(t, base)
			before := snapshot(t, outside)
			parentPath := filepath.Join(base, "parent")
			mkdirAll(t, parentPath)
			gen := treeGen{rng: rng, outside: outside, want: map[string]uint8{}}
			gen.dir(t, filepath.Join(parentPath, "anchor"), ".", 0, 2+rng.IntN(5), true)
			parent := openDir(t, parentPath)

			require.Equal(t, gen.want, walkTypes(t, parent, filepath.Join(parentPath, "anchor")))

			limit := 1 + rng.IntN(5)
			var report RemoveReport
			requireNoLeak(t, func() {
				var err error
				report, err = removeBeneath(context.Background(), parent, mustName("anchor"), RemoveOptions{}, limit)
				require.NoError(t, err)
			})
			t.Logf("%d objects, depth bound %d: %+v", len(gen.want), limit, report)
			require.Empty(t, listNames(t, parentPath))
			require.Equal(t, before, snapshot(t, outside))
			var dirs uint64
			for _, dtype := range gen.want {
				if dtype == unix.DT_DIR {
					dirs++
				}
			}
			require.Equal(t, dirs, report.Dirs, "every directory removed once")
			require.Equal(t, uint64(len(gen.want))-dirs, report.Entries, "every other object unlinked once")
			require.LessOrEqual(t, report.MaxDepth, limit)
		})
	}
}

// treeGen builds a random tree and records the type of every object in it.
type treeGen struct {
	rng     *rand.Rand
	outside string
	want    map[string]uint8 // path relative to the anchor -> d_type
}

// dir creates the directory path and random children. A spine directory's
// first child is a directory that continues the spine, so every tree
// reaches maxTreeDepth.
func (g *treeGen) dir(t *testing.T, path, rel string, depth, maxTreeDepth int, spine bool) {
	t.Helper()
	require.NoError(t, os.Mkdir(path, 0o755))
	g.want[rel] = unix.DT_DIR
	used := map[string]bool{}
	for i := range 1 + g.rng.IntN(5) {
		name := g.name(used)
		child, childRel := filepath.Join(path, name), filepath.Join(rel, name)
		kind := g.rng.IntN(10)
		if spine && i == 0 {
			kind = 0
		}
		switch {
		case kind < 4 && depth < maxTreeDepth:
			g.dir(t, child, childRel, depth+1, maxTreeDepth, spine && i == 0)
			continue
		case kind < 4:
			require.NoError(t, os.Mkdir(child, 0o755))
			g.want[childRel] = unix.DT_DIR
		case kind == 4:
			content := make([]byte, g.rng.IntN(64))
			for i := range content {
				content[i] = byte(g.rng.Uint32())
			}
			require.NoError(t, os.WriteFile(child, content, 0o644))
			g.want[childRel] = unix.DT_REG
		case kind == 5:
			require.NoError(t, os.Symlink(filepath.Join(g.outside, "dir"), child))
			g.want[childRel] = unix.DT_LNK
		case kind == 6:
			up := strings.Repeat("../", depth+2)
			require.NoError(t, os.Symlink(up+"outside/file", child))
			g.want[childRel] = unix.DT_LNK
		case kind == 7:
			require.NoError(t, unix.Mkfifo(child, 0o644))
			g.want[childRel] = unix.DT_FIFO
		case kind == 8:
			require.NoError(t, unix.Mknod(child, unix.S_IFSOCK|0o644, 0))
			g.want[childRel] = unix.DT_SOCK
		default:
			require.NoError(t, os.Link(filepath.Join(g.outside, "file"), child))
			g.want[childRel] = unix.DT_REG
		}
	}
}

// name returns a fresh random entry name: arbitrary bytes other than '/'
// and NUL, sometimes NAME_MAX long.
func (g *treeGen) name(used map[string]bool) string {
	const alphabet = "abcXYZ019 .-_\n\t\xff\xc3\xa9~"
	for {
		n := 1 + g.rng.IntN(12)
		if g.rng.IntN(8) == 0 {
			n = maxNameLen
		}
		b := make([]byte, n)
		for i := range b {
			b[i] = alphabet[g.rng.IntN(len(alphabet))]
		}
		name := string(b)
		if validName(name) && !used[name] {
			used[name] = true
			return name
		}
	}
}
