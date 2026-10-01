package placementsnapshot

import (
	"errors"
	"io"
	"os"
	"slices"
	"syscall"
)

// maxDirectoryEntries bounds one directory listing. Classification checks each
// seen set's files by name rather than trusting the listing, so a truncated
// listing only hides sets: a set with retain newer complete sets in view has
// at least as many in the whole directory.
const maxDirectoryEntries = 8192

// pruneFailure is why pruning kept a file it would otherwise have deleted, or
// failed to delete it. The zero value is invalid.
type pruneFailure uint8

const (
	pruneFailureInvalid pruneFailure = iota
	pruneFailureList
	pruneFailureInspect
	pruneFailureNotOwned
	pruneFailureLiveDatabase
	pruneFailureRemove
	pruneFailureSync
)

var pruneFailures = [...]pruneFailure{
	pruneFailureList,
	pruneFailureInspect,
	pruneFailureNotOwned,
	pruneFailureLiveDatabase,
	pruneFailureRemove,
	pruneFailureSync,
}

func (failure pruneFailure) label() string {
	switch failure {
	case pruneFailureList:
		return "list"
	case pruneFailureInspect:
		return "inspect"
	case pruneFailureNotOwned:
		return "not_owned"
	case pruneFailureLiveDatabase:
		return "live_database"
	case pruneFailureRemove:
		return "remove"
	case pruneFailureSync:
		return "sync"
	default:
		return ""
	}
}

// pruneReport is one pruning pass's result.
type pruneReport struct {
	removed  int
	failures map[pruneFailure]int
}

func (report *pruneReport) fail(failure pruneFailure) {
	if report.failures == nil {
		report.failures = make(map[pruneFailure]int)
	}
	report.failures[failure]++
}

// listing is what one pass saw in the snapshot directory.
type listing struct {
	sets     []setName
	temps    []string
	complete []setName // newest first
	doubtful map[setName]bool
}

func (snapshots *Directory) list(report *pruneReport) (listing, bool) {
	entries, err := snapshots.directory.ReadDir(maxDirectoryEntries)
	if err != nil && !errors.Is(err, io.EOF) {
		report.fail(pruneFailureList)
		return listing{}, false
	}
	if len(entries) == maxDirectoryEntries {
		report.fail(pruneFailureList) // truncated: still safe, but visible
	}
	seen := make(map[setName]bool)
	view := listing{doubtful: make(map[setName]bool)}
	for _, entry := range entries {
		name := entry.Name()
		if set, _, ok := snapshots.names.parse(name); ok {
			if !seen[set] {
				seen[set] = true
				view.sets = append(view.sets, set)
			}
			continue
		}
		if snapshots.names.isTemp(name) {
			view.temps = append(view.temps, name)
		}
	}
	for _, set := range view.sets {
		complete, doubt := snapshots.classify(set)
		switch {
		case doubt != pruneFailureInvalid:
			report.fail(doubt)
			view.doubtful[set] = true
		case complete:
			view.complete = append(view.complete, set)
		}
	}
	slices.SortFunc(view.complete, func(a, b setName) int {
		switch {
		case a.newer(b):
			return -1
		case b.newer(a):
			return 1
		default:
			return 0
		}
	})
	return view, true
}

// classify reports whether set is complete: its manifest strictly decodes and
// both data files are owned regular files of the recorded sizes. A read error
// is doubt, never a verdict.
func (snapshots *Directory) classify(set setName) (bool, pruneFailure) {
	raw, ok, doubt := snapshots.readManifest(snapshots.names.file(set, fileKindManifest))
	if doubt != pruneFailureInvalid || !ok {
		return false, doubt
	}
	decoded, err := snapshots.names.decodeManifest(set, raw)
	if err != nil {
		return false, pruneFailureInvalid
	}
	for _, kind := range []fileKind{fileKindPlacements, fileKindPayloads} {
		info, err := snapshots.directory.Lstat(snapshots.names.file(set, kind))
		switch {
		case errors.Is(err, os.ErrNotExist):
			return false, pruneFailureInvalid
		case err != nil:
			return false, pruneFailureInspect
		case !snapshots.owned(info) || info.Size() != decoded.size(kind):
			return false, pruneFailureInvalid
		}
	}
	return true, pruneFailureInvalid
}

// readManifest reads an owned regular manifest without following a link or
// blocking on a FIFO. ok is false when there is no such manifest.
func (snapshots *Directory) readManifest(name string) ([]byte, bool, pruneFailure) {
	info, err := snapshots.directory.Lstat(name)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil, false, pruneFailureInvalid
	case err != nil:
		return nil, false, pruneFailureInspect
	case !snapshots.owned(info):
		return nil, false, pruneFailureInvalid
	}
	file, err := snapshots.directory.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, pruneFailureInvalid
	}
	if err != nil {
		return nil, false, pruneFailureInspect
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil {
		return nil, false, pruneFailureInspect
	}
	if !os.SameFile(info, opened) || !snapshots.owned(opened) {
		return nil, false, pruneFailureInvalid
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxManifestBytes+1))
	if err != nil {
		return nil, false, pruneFailureInspect
	}
	return raw, true, pruneFailureInvalid
}

// newestComplete returns the newest complete set, if any.
func (snapshots *Directory) newestComplete() (setName, bool) {
	view, ok := snapshots.list(&pruneReport{})
	if !ok || len(view.complete) == 0 {
		return setName{}, false
	}
	return view.complete[0], true
}

// prune keeps the newest retain complete sets and keep, which this pass just
// published. It deletes older complete sets, incomplete sets older than the
// newest complete one, and this provider's staged files: no attempt is running
// while prune does, so a staged file is left over from a crash.
func (snapshots *Directory) prune(keep setName, retain int) pruneReport {
	var report pruneReport
	view, ok := snapshots.list(&report)
	if !ok {
		return report
	}
	retained := make(map[setName]bool, retain+1)
	for i, set := range view.complete {
		if i >= retain {
			break
		}
		retained[set] = true
	}
	if keep.valid() {
		retained[keep] = true
	}
	var newest setName
	if len(view.complete) > 0 {
		newest = view.complete[0]
	}
	for _, set := range view.sets {
		switch {
		case retained[set], view.doubtful[set]:
			continue
		case !slices.Contains(view.complete, set) && (!newest.valid() || !newest.newer(set)):
			continue // incomplete, and not older than the newest complete set
		}
		snapshots.removeSet(set, &report)
	}
	for _, temp := range view.temps {
		snapshots.remove(temp, &report)
	}
	if report.removed > 0 {
		if err := snapshots.directory.Sync(); err != nil {
			report.fail(pruneFailureSync)
		}
	}
	return report
}

// removeSet deletes a set's manifest, makes that durable, then deletes its
// data. If any step fails the rest of the set is kept.
func (snapshots *Directory) removeSet(set setName, report *pruneReport) {
	for _, kind := range setFileKinds {
		if !snapshots.remove(snapshots.names.file(set, kind), report) {
			return
		}
		if kind == fileKindManifest {
			if err := snapshots.directory.Sync(); err != nil {
				report.fail(pruneFailureSync)
				return
			}
		}
	}
}

// remove deletes one owned regular file that is not a live database. It
// reports true when the name is gone.
func (snapshots *Directory) remove(name string, report *pruneReport) bool {
	info, err := snapshots.directory.Lstat(name)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return true
	case err != nil:
		report.fail(pruneFailureInspect)
		return false
	case snapshots.isLive(info):
		report.fail(pruneFailureLiveDatabase)
		return false
	case !snapshots.owned(info):
		report.fail(pruneFailureNotOwned)
		return false
	}
	if err := snapshots.directory.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
		report.fail(pruneFailureRemove)
		return false
	}
	report.removed++
	return true
}
