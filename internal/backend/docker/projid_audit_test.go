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

func inodeOf(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Lstat(path)
	require.NoError(t, err)
	return info.Sys().(*syscall.Stat_t).Ino
}

// fakeAuditVolumes serves volume directories from a temporary tree.
type fakeAuditVolumes struct {
	root        string
	names       []string
	deleting    map[string]bool
	unsupported bool
	beforeOpen  func(ctx context.Context, name string) error
	opened      []string
}

func (f *fakeAuditVolumes) ListForProof(context.Context) ([]string, error) {
	return slices.Clone(f.names), nil
}

func (f *fakeAuditVolumes) OpenProjectIDAudit(ctx context.Context, name managedVolumeName) (*projidAuditVolume, error) {
	if f.unsupported {
		return nil, errProjectIDAuditUnsupported
	}
	if f.deleting[name.value()] {
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

func auditOne(t *testing.T, volumes *fakeAuditVolumes, attrs projectAttributeGetter, name string, depthLimit int) (projidAuditOutcome, projidAuditFindings) {
	t.Helper()
	parsed, err := parseManagedVolumeName(name)
	require.NoError(t, err)
	auditor := newProjidAuditor(volumes, attrs, slog.Default())
	outcome, findings, unsupported := auditor.auditVolume(t.Context(), parsed, depthLimit)
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

	outcome, findings := auditOne(t, &fakeAuditVolumes{root: root, names: []string{name}}, attrs, name, projidAuditMaxDepth)
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
			outcome, findings := auditOne(t, &fakeAuditVolumes{root: root, names: []string{volumeName}}, attrs, volumeName, projidAuditMaxDepth)
			require.Equal(t, projidAuditDrift, outcome)
			require.Equal(t, plant.dirs, findings.driftedDirs)
			require.Equal(t, plant.files, findings.driftedFiles)
			require.Equal(t, auditTestProjectID, findings.projID)
		})
	}
}

func TestProjidAuditStopsAtTheDepthBound(t *testing.T) {
	root := t.TempDir()
	name := auditVolumeName(3)
	volume := newAuditVolume(t, root, name)
	require.NoError(t, os.MkdirAll(filepath.Join(volume, "data/a/b/c/d"), 0o700))
	outcome, _ := auditOne(t, &fakeAuditVolumes{root: root, names: []string{name}}, &fakeProjectAttributes{}, name, 2)
	require.Equal(t, projidAuditTooDeep, outcome)
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
	outcome, _ := auditOne(t, &fakeAuditVolumes{root: root, names: []string{name}}, attrs, name, projidAuditMaxDepth)
	require.Equal(t, projidAuditChanged, outcome)
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
	outcome, findings := auditOne(t, &fakeAuditVolumes{root: root, names: []string{name}}, attrs, name, projidAuditMaxDepth)
	require.Equal(t, projidAuditChanged, outcome)
	require.True(t, findings.changed)
}

func TestProjidAuditReadFailureIsAnError(t *testing.T) {
	root := t.TempDir()
	name := auditVolumeName(6)
	volume := newAuditVolume(t, root, name)
	attrs := &fakeProjectAttributes{failing: map[uint64]error{inodeOf(t, filepath.Join(volume, "data/db")): unix.EIO}}
	outcome, _ := auditOne(t, &fakeAuditVolumes{root: root, names: []string{name}}, attrs, name, projidAuditMaxDepth)
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

	// A volume being deleted is skipped, and its old drift no longer counts.
	volumes.opened = nil
	volumes.deleting = map[string]bool{names[1]: true}
	before = auditCounters()
	auditor.passWithin(t.Context(), time.Minute)
	requireAuditDeltas(t, before, map[projidAuditOutcome]float64{projidAuditClean: 2, projidAuditSkipped: 1})
	require.Equal(t, 0.0, testutil.ToFloat64(volumesWithProjidDrift))

	// Drift again, then the volume leaves the inventory: it is forgotten.
	volumes.deleting = nil
	auditor.passWithin(t.Context(), time.Minute)
	require.Equal(t, 1.0, testutil.ToFloat64(volumesWithProjidDrift))
	volumes.names = []string{names[0], names[2]}
	auditor.passWithin(t.Context(), time.Minute)
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

// The audit's attribute access is read-only by construction: the type that
// production passes has no other method.
func TestProjidAuditAttributeGetterHasNoSetter(t *testing.T) {
	var getter projectAttributeGetter = xfsProjectAttributeGetter{}
	_, isSetter := getter.(interface{ SetProjectID(*os.Root, uint32) error })
	require.False(t, isSetter)
}

// The XFS manager refuses to open a volume whose deletion is registered, and
// reports a missing volume as absent, which the audit classifies as changed.
// Opening a real volume needs XFS and is covered by the root integration test.
func TestXFSOpenProjectIDAuditRefusesDeletingAndMissingVolumes(t *testing.T) {
	x := newXfsManagerForTest(t.TempDir())
	deleting := mustManagedVolumeName(t, auditVolumeName(30))
	stage, err := newXFSDeleteStageName(auditTestProjectID, deleting)
	require.NoError(t, err)
	require.NoError(t, x.rememberDurableDeleteStage(stage))
	_, err = x.OpenProjectIDAudit(t.Context(), deleting)
	require.ErrorIs(t, err, errProjectIDAuditDeletePending)

	_, err = x.OpenProjectIDAudit(t.Context(), mustManagedVolumeName(t, auditVolumeName(31)))
	require.ErrorIs(t, err, fs.ErrNotExist)
	require.Equal(t, projidAuditChanged, classifyProjidAuditWalk(projidAuditFindings{}, err))
}
