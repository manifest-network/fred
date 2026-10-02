package docker

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/fstree"
)

const auditTestProjectID uint32 = 4242

// fakeProjectAttributes stands in for FS_IOC_FSGETXATTR on a filesystem
// without project quotas. Inodes default to the volume's project with
// PROJINHERIT; a test plants other attributes by inode, and may react to a
// read of a given inode.
type fakeProjectAttributes struct {
	mu      sync.Mutex
	planted map[uint64]linuxFSXAttr
	onRead  func(ino uint64)
	failing map[uint64]error
}

func (f *fakeProjectAttributes) GetProjectAttributes(fd int) (linuxFSXAttr, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return linuxFSXAttr{}, err
	}
	f.mu.Lock()
	hook := f.onRead
	attr, planted := f.planted[stat.Ino]
	failure := f.failing[stat.Ino]
	f.mu.Unlock()
	if hook != nil {
		hook(stat.Ino)
	}
	if failure != nil {
		return linuxFSXAttr{}, failure
	}
	if planted {
		return attr, nil
	}
	return linuxFSXAttr{ProjectID: auditTestProjectID, XFlags: linuxFSXFlagProjInherit}, nil
}

func (f *fakeProjectAttributes) plant(t *testing.T, path string, attr linuxFSXAttr) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.planted == nil {
		f.planted = make(map[uint64]linuxFSXAttr)
	}
	f.planted[inodeOf(t, path)] = attr
}

func (f *fakeProjectAttributes) unplant(t *testing.T, path string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.planted, inodeOf(t, path))
}

func inodeOf(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Lstat(path)
	require.NoError(t, err)
	return info.Sys().(*syscall.Stat_t).Ino
}

// fakeAuditVolumes serves volume directories from a temporary tree. deleting
// names the volumes the manager reports as pending deletion; stagedAfterRead
// names volumes whose deletion is registered only after the audit read the
// pending deletions, so only the opener sees it.
type fakeAuditVolumes struct {
	root            string
	names           []string
	deleting        map[string]bool
	stagedAfterRead map[string]bool
	unsupported     bool
	beforeOpen      func(ctx context.Context, name string) error
	opened          []string
}

func (f *fakeAuditVolumes) ListForProof(context.Context) ([]string, error) {
	return slices.Clone(f.names), nil
}

func (f *fakeAuditVolumes) VolumeDeleteHolds() volumeDeleteHoldSnapshot {
	snapshot := volumeDeleteHoldSnapshot{pending: make(map[string]struct{})}
	for name, deleting := range f.deleting {
		if deleting {
			snapshot.pending[name] = struct{}{}
		}
	}
	return snapshot
}

func (f *fakeAuditVolumes) OpenProjectIDAudit(ctx context.Context, name managedVolumeName) (*projidAuditVolume, error) {
	if f.unsupported {
		return nil, errProjectIDAuditUnsupported
	}
	if f.deleting[name.value()] || f.stagedAfterRead[name.value()] {
		return nil, errProjectIDAuditDeletePending
	}
	f.opened = append(f.opened, name.value())
	if f.beforeOpen != nil {
		if err := f.beforeOpen(ctx, name.value()); err != nil {
			return nil, err
		}
	}
	directory, err := os.Open(filepath.Join(f.root, name.value()))
	if err != nil {
		return nil, err
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(directory.Fd()), &stat); err != nil {
		_ = directory.Close()
		return nil, err
	}
	return &projidAuditVolume{root: directory, dev: stat.Dev, projID: auditTestProjectID}, nil
}

func auditVolumeName(index int) string {
	return canonicalVolumeName(fmt.Sprintf("0192f1a0-1111-4abc-8def-%012d", index), "web", 0)
}

// newAuditVolume lays out a volume the way fred does: the project marker, the
// writable-path tree and one declared VOLUME directory.
func newAuditVolume(t *testing.T, root, name string) string {
	t.Helper()
	volume := filepath.Join(root, name)
	for _, dir := range []string{"_wp/etc", "data/db"} {
		require.NoError(t, os.MkdirAll(filepath.Join(volume, dir), 0o700))
	}
	require.NoError(t, os.WriteFile(filepath.Join(volume, projectIDFile), []byte(fmt.Sprint(auditTestProjectID)), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(volume, "_wp/etc/app.conf"), []byte("x"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(volume, "data/db/table"), []byte("y"), 0o600))
	return volume
}

func auditOne(t *testing.T, volumes *fakeAuditVolumes, attrs projectAttributeGetter, name string) (projidAuditOutcome, projidAuditFindings) {
	t.Helper()
	parsed, err := parseManagedVolumeName(name)
	require.NoError(t, err)
	auditor := newProjidAuditor(volumes, attrs, slog.Default())
	outcome, findings, unsupported := auditor.auditVolume(t.Context(), parsed)
	require.False(t, unsupported)
	return outcome, findings
}

func TestProjidAuditCleanVolume(t *testing.T) {
	root := t.TempDir()
	name := auditVolumeName(1)
	volume := newAuditVolume(t, root, name)
	require.NoError(t, os.Symlink("/etc/passwd", filepath.Join(volume, "data/db/link")))
	require.NoError(t, unix.Mkfifo(filepath.Join(volume, "data/db/pipe"), 0o600))
	attrs := &fakeProjectAttributes{}
	// The marker is fred's own file: a foreign value there is not audited.
	attrs.plant(t, filepath.Join(volume, projectIDFile), linuxFSXAttr{ProjectID: 1})

	outcome, findings := auditOne(t, &fakeAuditVolumes{root: root, names: []string{name}}, attrs, name)
	require.Equal(t, projidAuditClean, outcome)
	require.Equal(t, uint64(2), findings.unauditable, "symlinks and FIFOs are counted, never opened")
	require.Equal(t, uint64(2), findings.auditedFiles)
	require.Equal(t, uint64(5), findings.auditedDirs, "the volume directory, _wp, _wp/etc, data and data/db")
}

func TestProjidAuditFindsDrift(t *testing.T) {
	for name, plant := range map[string]struct {
		path  string
		attr  linuxFSXAttr
		dirs  uint64
		files uint64
	}{
		"directory without inheritance": {path: "data/db", attr: linuxFSXAttr{ProjectID: auditTestProjectID}, dirs: 1},
		"directory in another project":  {path: "_wp", attr: linuxFSXAttr{ProjectID: 7, XFlags: linuxFSXFlagProjInherit}, dirs: 1},
		"file in another project":       {path: "data/db/table", attr: linuxFSXAttr{ProjectID: 0}, files: 1},
		"volume directory":              {path: ".", attr: linuxFSXAttr{ProjectID: auditTestProjectID}, dirs: 1},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			volumeName := auditVolumeName(2)
			volume := newAuditVolume(t, root, volumeName)
			attrs := &fakeProjectAttributes{}
			attrs.plant(t, filepath.Join(volume, plant.path), plant.attr)
			outcome, findings := auditOne(t, &fakeAuditVolumes{root: root, names: []string{volumeName}}, attrs, volumeName)
			require.Equal(t, projidAuditDrift, outcome)
			require.Equal(t, plant.dirs, findings.driftedDirs)
			require.Equal(t, plant.files, findings.driftedFiles)
			require.Equal(t, auditTestProjectID, findings.projID)
		})
	}
}

// A directory moved out of the walked tree while the walk is inside it is
// reported as changed, never clean.
func TestProjidAuditReportsATreeChangedMidWalk(t *testing.T) {
	root := t.TempDir()
	name := auditVolumeName(4)
	volume := newAuditVolume(t, root, name)
	require.NoError(t, os.MkdirAll(filepath.Join(volume, "data/a/b"), 0o700))
	target := inodeOf(t, filepath.Join(volume, "data/a/b"))
	attrs := &fakeProjectAttributes{}
	var once sync.Once
	attrs.onRead = func(ino uint64) {
		if ino == target {
			once.Do(func() {
				require.NoError(t, os.Rename(filepath.Join(volume, "data/a"), filepath.Join(volume, "moved")))
			})
		}
	}
	outcome, _ := auditOne(t, &fakeAuditVolumes{root: root, names: []string{name}}, attrs, name)
	require.Equal(t, projidAuditChanged, outcome)
}

// listedFirst returns a file and a directory name, both absent from dir,
// such that a listing of dir holding just the two lists the file first.
// Directory order is the filesystem's (os.ReadDir would sort it, so the
// listing is read unsorted); candidates are tried until one fits.
func listedFirst(t *testing.T, dir string) (file, sub string) {
	t.Helper()
	listing := func() []os.DirEntry {
		directory, err := os.Open(dir)
		require.NoError(t, err)
		defer func() { require.NoError(t, directory.Close()) }()
		entries, err := directory.ReadDir(-1)
		require.NoError(t, err)
		return entries
	}
	for i := range 64 {
		file, sub = fmt.Sprintf("f%02d", i), fmt.Sprintf("d%02d", i)
		require.NoError(t, os.WriteFile(filepath.Join(dir, file), []byte("z"), 0o600))
		require.NoError(t, os.Mkdir(filepath.Join(dir, sub), 0o700))
		entries := listing()
		require.Len(t, entries, 2)
		if entries[0].Name() == file {
			return file, sub
		}
		require.NoError(t, os.Remove(filepath.Join(dir, file)))
		require.NoError(t, os.Remove(filepath.Join(dir, sub)))
	}
	t.Fatalf("no candidate pair lists its file first in %s", dir)
	return "", ""
}

// A listed directory that vanishes, or stops being a directory, before the
// walk opens it is passed over by fstree, so its subtree was never audited:
// the volume is changed, never clean.
func TestProjidAuditReportsADirectoryPassedOverAfterListing(t *testing.T) {
	for name, replace := range map[string]func(t *testing.T, path string){
		"vanished": func(t *testing.T, path string) { require.NoError(t, os.Remove(path)) },
		"retyped": func(t *testing.T, path string) {
			require.NoError(t, os.Remove(path))
			require.NoError(t, os.WriteFile(path, []byte("now a file"), 0o600))
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			volumeName := auditVolumeName(7)
			volume := newAuditVolume(t, root, volumeName)
			parent := filepath.Join(volume, "data", "batch")
			require.NoError(t, os.Mkdir(parent, 0o700))
			file, sub := listedFirst(t, parent)
			trigger := inodeOf(t, filepath.Join(parent, file))
			attrs := &fakeProjectAttributes{}
			var once sync.Once
			attrs.onRead = func(ino uint64) {
				if ino == trigger {
					once.Do(func() { replace(t, filepath.Join(parent, sub)) })
				}
			}
			outcome, findings := auditOne(t, &fakeAuditVolumes{root: root, names: []string{volumeName}}, attrs, volumeName)
			require.Equal(t, projidAuditChanged, outcome)
			require.True(t, findings.changed)
			require.Zero(t, findings.driftedDirs+findings.driftedFiles)
		})
	}
}

// A regular file replaced by a FIFO after it was listed is opened without
// blocking, seen to be a FIFO, and leaves the volume changed.
func TestProjidAuditReportsAFileReplacedAfterListing(t *testing.T) {
	root := t.TempDir()
	name := auditVolumeName(5)
	volume := newAuditVolume(t, root, name)
	first, second := filepath.Join(volume, "data/db/table"), filepath.Join(volume, "data/db/other")
	require.NoError(t, os.WriteFile(second, []byte("z"), 0o600))
	inodes := map[uint64]string{inodeOf(t, first): second, inodeOf(t, second): first}
	attrs := &fakeProjectAttributes{}
	var once sync.Once
	attrs.onRead = func(ino uint64) {
		other, ok := inodes[ino]
		if !ok {
			return
		}
		once.Do(func() {
			require.NoError(t, os.Remove(other))
			require.NoError(t, unix.Mkfifo(other, 0o600))
		})
	}
	outcome, findings := auditOne(t, &fakeAuditVolumes{root: root, names: []string{name}}, attrs, name)
	require.Equal(t, projidAuditChanged, outcome)
	require.True(t, findings.changed)
}

func TestProjidAuditReadFailureIsAnError(t *testing.T) {
	root := t.TempDir()
	name := auditVolumeName(6)
	volume := newAuditVolume(t, root, name)
	attrs := &fakeProjectAttributes{failing: map[uint64]error{inodeOf(t, filepath.Join(volume, "data/db")): unix.EIO}}
	outcome, _ := auditOne(t, &fakeAuditVolumes{root: root, names: []string{name}}, attrs, name)
	require.Equal(t, projidAuditError, outcome)
}

func TestClassifyProjidAuditOpen(t *testing.T) {
	for errno, want := range map[unix.Errno]projidAuditOutcome{
		unix.ENOENT: projidAuditChanged, unix.ELOOP: projidAuditChanged, unix.ENXIO: projidAuditChanged,
		unix.ENOTDIR: projidAuditChanged, unix.EAGAIN: projidAuditIncomplete, // EWOULDBLOCK is EAGAIN on Linux
		unix.EACCES: projidAuditError, unix.EPERM: projidAuditError, unix.EIO: projidAuditError, unix.EMFILE: projidAuditError,
	} {
		require.Equal(t, want, classifyProjidAuditOpen(fmt.Errorf("open: %w", errno)), errno.Error())
	}
}

func TestClassifyProjidAuditWalkIsTotal(t *testing.T) {
	drift := projidAuditFindings{driftedFiles: 1}
	for name, tc := range map[string]struct {
		findings projidAuditFindings
		err      error
		want     projidAuditOutcome
	}{
		"clean":                    {want: projidAuditClean},
		"drift":                    {findings: drift, want: projidAuditDrift},
		"drift before a stop":      {findings: drift, err: fstree.ErrTreeChanged, want: projidAuditDrift},
		"changed entry":            {findings: projidAuditFindings{changed: true}, want: projidAuditChanged},
		"refused open":             {findings: projidAuditFindings{incomplete: true}, want: projidAuditIncomplete},
		"changed and refused":      {findings: projidAuditFindings{changed: true, incomplete: true}, want: projidAuditChanged},
		"too deep":                 {err: fmt.Errorf("walk: %w", fstree.ErrTooDeep), want: projidAuditTooDeep},
		"tree changed":             {err: fmt.Errorf("walk: %w", fstree.ErrTreeChanged), want: projidAuditChanged},
		"vanished":                 {err: fmt.Errorf("open: %w", fs.ErrNotExist), want: projidAuditChanged},
		"vanished errno":           {err: fmt.Errorf("open: %w", unix.ENOENT), want: projidAuditChanged},
		"budget":                   {err: context.DeadlineExceeded, want: projidAuditIncomplete},
		"canceled":                 {err: context.Canceled, want: projidAuditIncomplete},
		"cross device":             {err: fstree.ErrCrossDevice, want: projidAuditError},
		"unexpected":               {err: errors.New("unexpected"), want: projidAuditError},
		"changed entry then error": {findings: projidAuditFindings{changed: true}, err: unix.EIO, want: projidAuditError},
	} {
		require.Equal(t, tc.want, classifyProjidAuditWalk(tc.findings, tc.err), name)
	}
}

func TestProjidAuditOutcomeLabelsAreClosed(t *testing.T) {
	seen := map[string]bool{}
	for _, outcome := range projidAuditOutcomes {
		label := outcome.label()
		require.NotEmpty(t, label)
		require.False(t, seen[label])
		seen[label] = true
	}
	require.Len(t, seen, 7)
	require.Empty(t, projidAuditInvalid.label())
}

func auditCounters() map[projidAuditOutcome]float64 {
	counts := make(map[projidAuditOutcome]float64, len(projidAuditOutcomes))
	for _, outcome := range projidAuditOutcomes {
		counts[outcome] = testutil.ToFloat64(volumeProjidAuditTotal.WithLabelValues(outcome.label()))
	}
	return counts
}

func requireAuditDeltas(t *testing.T, before map[projidAuditOutcome]float64, want map[projidAuditOutcome]float64) {
	t.Helper()
	after := auditCounters()
	for _, outcome := range projidAuditOutcomes {
		require.Equal(t, want[outcome], after[outcome]-before[outcome], outcome.label())
	}
}

// Each pass resumes after the last audited volume, keeps every volume's last
// outcome, and recomputes the drift gauge after every pass, complete or not.
func TestProjidAuditPassesRoundRobinAndKeepTheLastOutcome(t *testing.T) {
	root := t.TempDir()
	names := []string{auditVolumeName(10), auditVolumeName(11), auditVolumeName(12)}
	for _, name := range names {
		newAuditVolume(t, root, name)
	}
	attrs := &fakeProjectAttributes{}
	attrs.plant(t, filepath.Join(root, names[1], "data/db"), linuxFSXAttr{ProjectID: auditTestProjectID})
	volumes := &fakeAuditVolumes{root: root, names: names}
	auditor := newProjidAuditor(volumes, attrs, slog.Default())

	// The pass budget ends while the first volume is open: it is recorded as
	// incomplete and the pass stops there.
	volumes.beforeOpen = func(ctx context.Context, _ string) error {
		<-ctx.Done()
		return ctx.Err()
	}
	before := auditCounters()
	auditor.passWithin(t.Context(), 20*time.Millisecond)
	requireAuditDeltas(t, before, map[projidAuditOutcome]float64{projidAuditIncomplete: 1})
	require.Equal(t, names[:1], volumes.opened)
	require.Equal(t, 0.0, testutil.ToFloat64(volumesWithProjidDrift))

	// The next pass resumes after it and wraps around.
	volumes.beforeOpen, volumes.opened = nil, nil
	before = auditCounters()
	auditor.passWithin(t.Context(), time.Minute)
	require.Equal(t, []string{names[1], names[2], names[0]}, volumes.opened)
	requireAuditDeltas(t, before, map[projidAuditOutcome]float64{projidAuditClean: 2, projidAuditDrift: 1})
	require.Equal(t, 1.0, testutil.ToFloat64(volumesWithProjidDrift))

	// A volume being deleted is skipped without being opened. Its recorded
	// drift is kept: skipping it observed nothing.
	volumes.opened = nil
	volumes.deleting = map[string]bool{names[1]: true}
	before = auditCounters()
	auditor.passWithin(t.Context(), time.Minute)
	requireAuditDeltas(t, before, map[projidAuditOutcome]float64{projidAuditClean: 2, projidAuditSkipped: 1})
	require.NotContains(t, volumes.opened, names[1])
	require.Equal(t, 1.0, testutil.ToFloat64(volumesWithProjidDrift))

	// So is one whose stage is registered after the pending deletions were
	// read: the opener refuses it.
	volumes.deleting, volumes.stagedAfterRead = nil, map[string]bool{names[1]: true}
	before = auditCounters()
	auditor.passWithin(t.Context(), time.Minute)
	requireAuditDeltas(t, before, map[projidAuditOutcome]float64{projidAuditClean: 2, projidAuditSkipped: 1})
	require.Equal(t, 1.0, testutil.ToFloat64(volumesWithProjidDrift))
	volumes.stagedAfterRead = nil

	// Once the drift is gone, a clean walk clears it.
	attrs.unplant(t, filepath.Join(root, names[1], "data/db"))
	auditor.passWithin(t.Context(), time.Minute)
	require.Equal(t, 0.0, testutil.ToFloat64(volumesWithProjidDrift))

	// Drift again, then the volume leaves the inventory: it is forgotten.
	attrs.plant(t, filepath.Join(root, names[1], "data/db"), linuxFSXAttr{ProjectID: auditTestProjectID})
	auditor.passWithin(t.Context(), time.Minute)
	require.Equal(t, 1.0, testutil.ToFloat64(volumesWithProjidDrift))
	volumes.names = []string{names[0], names[2]}
	auditor.passWithin(t.Context(), time.Minute)
	require.Equal(t, 0.0, testutil.ToFloat64(volumesWithProjidDrift))
	require.NotContains(t, auditor.last, names[1])
}

// A recorded drift is a fact fred never repairs. A later walk that does not
// finish undisturbed observes nothing, so it must not erase the drift: only a
// clean walk does.
func TestProjidAuditKeepsDriftThroughInconclusiveWalks(t *testing.T) {
	root := t.TempDir()
	name := auditVolumeName(13)
	volume := newAuditVolume(t, root, name)
	attrs := &fakeProjectAttributes{}
	attrs.plant(t, filepath.Join(volume, "data/db/table"), linuxFSXAttr{ProjectID: 0})
	volumes := &fakeAuditVolumes{root: root, names: []string{name}}
	auditor := newProjidAuditor(volumes, attrs, slog.Default())
	auditor.passWithin(t.Context(), time.Minute)
	require.Equal(t, projidAuditDrift, auditor.last[name])
	require.Equal(t, 1.0, testutil.ToFloat64(volumesWithProjidDrift))

	for err, want := range map[error]projidAuditOutcome{
		fmt.Errorf("walk: %w", fstree.ErrTooDeep):     projidAuditTooDeep,
		fmt.Errorf("walk: %w", fstree.ErrTreeChanged): projidAuditChanged,
		context.DeadlineExceeded:                      projidAuditIncomplete,
		unix.EIO:                                      projidAuditError,
	} {
		volumes.beforeOpen = func(context.Context, string) error { return err }
		before := auditCounters()
		auditor.passWithin(t.Context(), time.Minute)
		requireAuditDeltas(t, before, map[projidAuditOutcome]float64{want: 1})
		require.Equal(t, want, auditor.last[name])
		require.Equal(t, 1.0, testutil.ToFloat64(volumesWithProjidDrift), "%s must keep the recorded drift", want.label())
	}

	// A walk that passed over a listed entry is changed, and keeps it too.
	volumes.beforeOpen = nil
	attrs.unplant(t, filepath.Join(volume, "data/db/table"))
	parent := filepath.Join(volume, "data", "batch")
	require.NoError(t, os.Mkdir(parent, 0o700))
	file, sub := listedFirst(t, parent)
	trigger := inodeOf(t, filepath.Join(parent, file))
	var once sync.Once
	attrs.onRead = func(ino uint64) {
		if ino == trigger {
			once.Do(func() { require.NoError(t, os.Remove(filepath.Join(parent, sub))) })
		}
	}
	auditor.passWithin(t.Context(), time.Minute)
	require.Equal(t, projidAuditChanged, auditor.last[name])
	require.Equal(t, 1.0, testutil.ToFloat64(volumesWithProjidDrift))

	// The next walk finishes undisturbed and finds the volume clean.
	auditor.passWithin(t.Context(), time.Minute)
	require.Equal(t, projidAuditClean, auditor.last[name])
	require.Equal(t, 0.0, testutil.ToFloat64(volumesWithProjidDrift))
}

func TestProjidAuditSkipsUnsupportedManagers(t *testing.T) {
	volumes := &fakeAuditVolumes{root: t.TempDir(), names: []string{auditVolumeName(20)}, unsupported: true}
	auditor := newProjidAuditor(volumes, &fakeProjectAttributes{}, slog.Default())
	before := auditCounters()
	auditor.passWithin(t.Context(), time.Minute)
	requireAuditDeltas(t, before, nil)
	for _, manager := range []volumeReader{&noopVolumeManager{}, &btrfsVolumeManager{}, &zfsVolumeManager{}} {
		_, err := manager.OpenProjectIDAudit(t.Context(), mustManagedVolumeName(t, auditVolumeName(20)))
		require.ErrorIs(t, err, errProjectIDAuditUnsupported)
	}
	var nilAuditor *projidAuditor
	nilAuditor.pass(t.Context())
}

func mustManagedVolumeName(t *testing.T, value string) managedVolumeName {
	t.Helper()
	name, err := parseManagedVolumeName(value)
	require.NoError(t, err)
	return name
}

// The XFS manager refuses to open a volume whose deletion is registered, and
// reports a missing volume as absent, which the audit classifies as changed.
// Opening a real volume needs XFS and is covered by the root integration test
// TestIntegration_XFS_ProjectIDAuditDetectsHistoricalDrift.
func TestXFSOpenProjectIDAuditRefusesDeletingAndMissingVolumes(t *testing.T) {
	x := newXfsManagerForTest(t.TempDir())
	deleting := mustManagedVolumeName(t, auditVolumeName(30))
	stage, err := newXFSDeleteStageName(auditTestProjectID, deleting)
	require.NoError(t, err)
	require.NoError(t, x.rememberDurableDeleteStage(stage))
	require.True(t, x.VolumeDeleteHolds().deletePending(deleting.value()),
		"the audit skips a name the manager reports as pending deletion")
	_, err = x.OpenProjectIDAudit(t.Context(), deleting)
	require.ErrorIs(t, err, errProjectIDAuditDeletePending)

	_, err = x.OpenProjectIDAudit(t.Context(), mustManagedVolumeName(t, auditVolumeName(31)))
	require.ErrorIs(t, err, fs.ErrNotExist)
	require.Equal(t, projidAuditChanged, classifyProjidAuditWalk(projidAuditFindings{}, err))
}

// The pacer lets a batch through at once and then pauses for whatever is
// left of the batch's share of a second, so the walk averages at most
// projidAuditInodesPerSecond opens.
func TestProjidAuditPacerCapsTheOpenRate(t *testing.T) {
	share := projidAuditPaceBatch * time.Second / projidAuditInodesPerSecond
	require.Positive(t, share)
	var pacer projidAuditPacer
	start := time.Unix(1_700_000_000, 0)
	for i := range projidAuditPaceBatch - 1 {
		require.Zero(t, pacer.next(start.Add(time.Duration(i)*time.Microsecond)), "open %d", i)
	}
	require.Equal(t, share-time.Millisecond, pacer.next(start.Add(time.Millisecond)),
		"a batch done early pauses until its share has elapsed")

	// A batch that took longer than its share does not pause.
	slow := start.Add(time.Second)
	for i := range projidAuditPaceBatch - 1 {
		require.Zero(t, pacer.next(slow.Add(time.Duration(i)*time.Microsecond)))
	}
	require.Zero(t, pacer.next(slow.Add(2*share)))
}

// O_NOATIME is the kernel's to grant: the audit uses it where it may, and
// falls back to a plain read-only open where Linux refuses it with EPERM (a
// file another user owns, opened without CAP_FOWNER).
func TestOpenAuditedFileUsesNoatimeWhenPermitted(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "own"), []byte("x"), 0o600))
	parent, err := os.Open(dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, parent.Close()) }()
	fd, err := openAuditedFile(int(parent.Fd()), "own")
	require.NoError(t, err)
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	require.NoError(t, err)
	require.NoError(t, unix.Close(fd))
	require.NotZero(t, flags&unix.O_NOATIME, "the owner's open keeps O_NOATIME")
	require.Equal(t, unix.O_RDONLY, flags&unix.O_ACCMODE)

	if os.Geteuid() == 0 {
		t.Log("running as root, which may always use O_NOATIME; the EPERM fallback is not reachable here")
		return
	}
	etc, err := os.Open("/etc")
	require.NoError(t, err)
	defer func() { require.NoError(t, etc.Close()) }()
	info, err := os.Stat("/etc/passwd")
	require.NoError(t, err)
	require.NotEqual(t, uint32(os.Geteuid()), info.Sys().(*syscall.Stat_t).Uid, "the fallback check needs a file another user owns")
	_, err = unix.Openat(int(etc.Fd()), "passwd", projidAuditFileFlags|unix.O_NOATIME, 0)
	require.ErrorIs(t, err, unix.EPERM, "Linux refuses O_NOATIME on another user's file")
	fd, err = openAuditedFile(int(etc.Fd()), "passwd")
	require.NoError(t, err)
	flags, err = unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	require.NoError(t, err)
	require.NoError(t, unix.Close(fd))
	require.Zero(t, flags&unix.O_NOATIME)
}
