package docker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"slices"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/fstree"
	"github.com/manifest-network/fred/internal/metrics/background"
	"github.com/manifest-network/fred/internal/util"
)

// The project-ID audit is detect-only. It walks managed XFS volumes one at a
// time and reports inodes whose project attributes differ from their
// volume's: a directory without PROJINHERIT or with another project ID, or a
// regular file with another project ID. It never repairs, never latches, never
// blocks Start, and takes no lease lock.
//
// This file is the whole audit path: the opener, the walker and the attribute
// getter. It only reads. A guard test (projid_audit_guard_test.go) pins that
// it names no mutating API, that its one ioctl is FS_IOC_FSGETXATTR, and that
// no other production file opens a volume for the audit.
const (
	projidAuditFirstDelay   = 10 * time.Minute
	projidAuditInterval     = 24 * time.Hour
	projidAuditVolumeBudget = 2 * time.Minute
	projidAuditPassBudget   = 30 * time.Minute
	// projidAuditRootBatch bounds one read of a volume root's entries.
	projidAuditRootBatch = 256
	// projidAuditInodesPerSecond caps the inodes, directories and regular
	// files, the audit opens per second, so a pass never bursts metadata reads
	// on a host its tenants share. At this rate one volume's budget covers
	// 1,200,000 inodes; a larger tree is reported incomplete.
	projidAuditInodesPerSecond = 10_000
	// projidAuditPaceBatch is how many opens the pacer lets through between
	// pauses, so that no pause is shorter than a timer reliably honors.
	projidAuditPaceBatch = 100
)

var (
	// errProjectIDAuditUnsupported is returned by volume managers without XFS
	// project quotas; the audit skips them entirely.
	errProjectIDAuditUnsupported = errors.New("project-ID audit is supported only on XFS volumes")
	// errProjectIDAuditDeletePending marks a volume whose deletion has been
	// registered; the audit skips it.
	errProjectIDAuditDeletePending = errors.New("managed volume has a registered delete stage")
)

// projidAuditOutcome is the closed result of auditing one volume. Drift is
// reported only from completed attribute reads; a failed, vanished or
// interrupted read, or a walk that passed over a listed entry, is changed,
// incomplete or error, never clean.
type projidAuditOutcome uint8

const (
	projidAuditInvalid projidAuditOutcome = iota
	projidAuditClean
	projidAuditDrift
	projidAuditTooDeep
	projidAuditIncomplete
	projidAuditChanged
	projidAuditError
	projidAuditSkipped
)

var projidAuditOutcomes = [...]projidAuditOutcome{
	projidAuditClean, projidAuditDrift, projidAuditTooDeep, projidAuditIncomplete,
	projidAuditChanged, projidAuditError, projidAuditSkipped,
}

func (o projidAuditOutcome) label() string {
	switch o {
	case projidAuditClean:
		return "clean"
	case projidAuditDrift:
		return "drift"
	case projidAuditTooDeep:
		return "too_deep"
	case projidAuditIncomplete:
		return "incomplete"
	case projidAuditChanged:
		return "changed"
	case projidAuditError:
		return "error"
	case projidAuditSkipped:
		return "skipped"
	default:
		return ""
	}
}

// projectAttributeGetter reads the XFS project attributes of an open inode.
// It is the audit's only attribute access, and it has no setter.
type projectAttributeGetter interface {
	GetProjectAttributes(fd int) (linuxFSXAttr, error)
}

// xfsProjectAttributeGetter is the project-ID audit's attribute reader:
// FS_IOC_FSGETXATTR on an open descriptor. It deliberately has no setter.
type xfsProjectAttributeGetter struct{}

func (xfsProjectAttributeGetter) GetProjectAttributes(fd int) (linuxFSXAttr, error) {
	var attr linuxFSXAttr
	_, _, errno := unix.Syscall(
		unix.SYS_IOCTL,
		uintptr(fd),
		linuxFSIOCFSGetXAttr,
		uintptr(unsafe.Pointer(&attr)), // #nosec G103 -- stable Linux fsxattr UAPI buffer
	)
	if errno != 0 {
		return linuxFSXAttr{}, errno
	}
	return attr, nil
}

// projidAuditVolumes is the volume surface the audit needs: the inventory,
// the manager's pending deletions, and a read-only opener. volumeReader
// satisfies it.
type projidAuditVolumes interface {
	ListForProof(context.Context) ([]string, error)
	VolumeDeleteHolds() volumeDeleteHoldSnapshot
	OpenProjectIDAudit(context.Context, managedVolumeName) (*projidAuditVolume, error)
}

// projidAuditVolume is one managed volume opened for the audit: its
// directory, the device it lives on, and the project ID its marker records.
// The caller closes it.
type projidAuditVolume struct {
	root   *os.File
	dev    uint64
	projID uint32
}

func (v *projidAuditVolume) Close() error {
	if v == nil || v.root == nil {
		return nil
	}
	return v.root.Close()
}

// OpenProjectIDAudit opens one managed volume for the read-only project-ID
// audit, the same way the manager opens volume roots: descriptor-relative from
// the data root, refusing a symlinked or replaced volume entry. It returns the
// volume directory, its device and the project ID its marker records. A
// volume with a delete stage is not opened: the audit skips it, and this check
// covers a stage registered after the audit read the manager's pending
// deletions.
func (x *xfsVolumeManager) OpenProjectIDAudit(ctx context.Context, name managedVolumeName) (*projidAuditVolume, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, deleting, err := x.deleteStage(name); err != nil {
		return nil, err
	} else if deleting {
		return nil, errProjectIDAuditDeletePending
	}
	root, err := os.OpenRoot(x.dataPath)
	if err != nil {
		return nil, fmt.Errorf("open xfs volume root: %w", err)
	}
	defer func() { _ = root.Close() }()
	volumeRoot, err := openAttestedManagedVolumeRoot(root, name)
	if err != nil {
		return nil, fmt.Errorf("open managed volume: %w", err)
	}
	defer func() { _ = volumeRoot.Close() }()
	projID, err := readProjectIDFileInVolumeRoot(volumeRoot)
	if err != nil {
		return nil, fmt.Errorf("read the volume's project ID marker: %w", err)
	}
	directory, err := openXFSRootDirectory(volumeRoot)
	if err != nil {
		return nil, fmt.Errorf("open the volume directory: %w", err)
	}
	info, err := directory.Stat()
	if err != nil {
		_ = directory.Close()
		return nil, fmt.Errorf("stat the volume directory: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		_ = directory.Close()
		return nil, errors.New("the volume directory has no device identity")
	}
	return &projidAuditVolume{root: directory, dev: stat.Dev, projID: projID}, nil
}

// projidAuditFindings counts what one volume's walk observed.
type projidAuditFindings struct {
	projID                    uint32
	driftedDirs, driftedFiles uint64
	auditedDirs, auditedFiles uint64
	unauditable               uint64
	changed, incomplete       bool
}

// projidAuditor keeps the round-robin cursor, each volume's last outcome and
// the volumes with recorded drift between passes. Only the audit loop uses
// it.
//
// Drift is a fact read from completed attribute reads, and fred never repairs
// it, so it is sticky: a later walk that does not finish undisturbed (too
// deep, changed, incomplete, error) or that skips the volume is an
// observation of nothing and keeps it. Only a later clean walk clears it, or
// the volume leaving the inventory.
type projidAuditor struct {
	volumes projidAuditVolumes
	attrs   projectAttributeGetter
	logger  *slog.Logger

	mu      sync.Mutex
	cursor  string
	last    map[string]projidAuditOutcome
	drifted map[string]struct{}
}

// newProjidAuditor binds the audit to its volumes and attribute getter.
// Production passes the backend's volume reader and the ioctl getter.
func newProjidAuditor(volumes projidAuditVolumes, attrs projectAttributeGetter, logger *slog.Logger) *projidAuditor {
	return &projidAuditor{
		volumes: volumes, attrs: attrs, logger: logger,
		last: make(map[string]projidAuditOutcome), drifted: make(map[string]struct{}),
	}
}

// projidAuditLoop runs the first pass projidAuditFirstDelay after Start and
// then one every projidAuditInterval, until shutdown.
func (b *Backend) projidAuditLoop() {
	timer := time.NewTimer(projidAuditFirstDelay)
	defer timer.Stop()
	for {
		select {
		case <-b.stopCtx.Done():
			return
		case <-timer.C:
		}
		util.RunCleanupIteration(func() error {
			b.projidAudit.pass(b.stopCtx)
			return nil
		}, "docker_projid_audit", func(any) {
			background.CleanupPanicsTotal.WithLabelValues("docker_projid_audit").Inc()
		})
		timer.Reset(projidAuditInterval)
	}
}

func (a *projidAuditor) pass(parent context.Context) {
	a.passWithin(parent, projidAuditPassBudget)
}

// passWithin audits volumes one at a time, resuming after the last volume
// audited, until every volume is done or the pass budget runs out. Each
// volume's outcome is counted and kept; the drift gauge is recomputed after
// every pass, complete or not.
func (a *projidAuditor) passWithin(parent context.Context, budget time.Duration) {
	if a == nil || util.IsNilInterface(a.volumes) || util.IsNilInterface(a.attrs) {
		return
	}
	ctx, cancel := context.WithTimeout(parent, budget)
	defer cancel()
	listed, err := a.volumes.ListForProof(ctx)
	if err != nil {
		if parent.Err() == nil {
			a.logger.Warn("project-ID audit could not list managed volumes", "error", err)
		}
		return
	}
	names := make([]managedVolumeName, 0, len(listed))
	present := make(map[string]bool, len(listed))
	for _, raw := range listed {
		name, err := parseManagedVolumeName(raw)
		if err != nil || present[name.value()] {
			continue
		}
		present[name.value()] = true
		names = append(names, name)
	}
	slices.SortFunc(names, func(x, y managedVolumeName) int { return cmp.Compare(x.value(), y.value()) })
	a.mu.Lock()
	// A volume that left the inventory no longer exists to report on.
	for name := range a.last {
		if !present[name] {
			delete(a.last, name)
		}
	}
	for name := range a.drifted {
		if !present[name] {
			delete(a.drifted, name)
		}
	}
	start, found := slices.BinarySearchFunc(names, a.cursor, func(name managedVolumeName, cursor string) int {
		return cmp.Compare(name.value(), cursor)
	})
	if found {
		start++
	}
	a.mu.Unlock()
	defer a.publish()

	for offset := range names {
		if ctx.Err() != nil {
			return
		}
		name := names[(start+offset)%len(names)]
		outcome, findings, unsupported := a.auditVolume(ctx, name)
		if unsupported || parent.Err() != nil {
			return
		}
		a.record(name, outcome, findings)
	}
}

// record counts and keeps one volume's outcome. Drift is recorded, and only a
// clean outcome clears a recorded drift; every other outcome leaves it.
func (a *projidAuditor) record(name managedVolumeName, outcome projidAuditOutcome, findings projidAuditFindings) {
	a.mu.Lock()
	a.last[name.value()] = outcome
	a.cursor = name.value()
	switch outcome {
	case projidAuditDrift:
		a.drifted[name.value()] = struct{}{}
	case projidAuditClean:
		delete(a.drifted, name.value())
	}
	_, driftRecorded := a.drifted[name.value()]
	a.mu.Unlock()
	if label := outcome.label(); label != "" {
		volumeProjidAuditTotal.WithLabelValues(label).Inc()
	}
	switch outcome {
	case projidAuditDrift:
		a.logger.Warn("managed volume has inodes outside its project; investigate, fred does not repair them",
			"volume", name.value(), "project_id", findings.projID,
			"drifted_directories", findings.driftedDirs, "drifted_files", findings.driftedFiles,
			"unauditable_entries", findings.unauditable)
	case projidAuditClean:
	case projidAuditSkipped:
		if driftRecorded {
			a.logger.Info("project-ID audit skipped a managed volume being deleted; its recorded drift is kept",
				"volume", name.value())
		}
	default:
		a.logger.Info("project-ID audit did not complete for a managed volume",
			"volume", name.value(), "outcome", outcome.label(), "drift_recorded", driftRecorded)
	}
}

// publish sets the drift gauge to the number of volumes with recorded drift.
func (a *projidAuditor) publish() {
	a.mu.Lock()
	drifted := len(a.drifted)
	a.mu.Unlock()
	volumesWithProjidDrift.Set(float64(drifted))
}

// auditVolume walks one volume under its own time budget. A volume whose
// deletion the manager has registered is skipped without being opened.
// unsupported reports a manager without project quotas, which ends the pass
// uncounted.
func (a *projidAuditor) auditVolume(ctx context.Context, name managedVolumeName) (outcome projidAuditOutcome, findings projidAuditFindings, unsupported bool) {
	if a.volumes.VolumeDeleteHolds().deletePending(name.value()) {
		return projidAuditSkipped, findings, false
	}
	ctx, cancel := context.WithTimeout(ctx, projidAuditVolumeBudget)
	defer cancel()
	volume, err := a.volumes.OpenProjectIDAudit(ctx, name)
	switch {
	case errors.Is(err, errProjectIDAuditUnsupported):
		return projidAuditInvalid, findings, true
	case errors.Is(err, errProjectIDAuditDeletePending):
		return projidAuditSkipped, findings, false
	case err != nil:
		return classifyProjidAuditWalk(findings, err), findings, false
	}
	defer func() { _ = volume.Close() }()
	walk := &projidAuditWalk{attrs: a.attrs, projID: volume.projID, dev: volume.dev}
	walk.findings.projID = volume.projID
	err = walk.volume(ctx, volume.root)
	return classifyProjidAuditWalk(walk.findings, err), walk.findings, false
}

// projidAuditPacer spaces the inodes one walk opens: at most
// projidAuditInodesPerSecond on average, measured over batches of
// projidAuditPaceBatch opens.
type projidAuditPacer struct {
	start  time.Time
	opened int
}

// wait is called before each open and pauses when the current batch came too
// fast. The walk checks its context between entries; a pause is at most one
// batch's share of a second.
func (p *projidAuditPacer) wait() {
	if pause := p.next(time.Now()); pause > 0 {
		time.Sleep(pause)
	}
}

// next counts one open at now and returns how long to pause before it.
func (p *projidAuditPacer) next(now time.Time) time.Duration {
	if p.opened == 0 {
		p.start = now
	}
	p.opened++
	if p.opened < projidAuditPaceBatch {
		return 0
	}
	p.opened = 0
	minimum := projidAuditPaceBatch * time.Second / projidAuditInodesPerSecond
	if elapsed := now.Sub(p.start); elapsed < minimum {
		return minimum - elapsed
	}
	return 0
}

// projidAuditWalk is the fstree visitor of one volume.
type projidAuditWalk struct {
	attrs    projectAttributeGetter
	projID   uint32
	dev      uint64
	pace     projidAuditPacer
	findings projidAuditFindings
}

// volume audits the volume directory itself and then each of its top-level
// entries, the writable-path tree and every declared VOLUME directory. The
// project-ID marker is fred's own file and is not audited. A walk that passed
// over a listed entry leaves the volume changed.
func (w *projidAuditWalk) volume(ctx context.Context, root *os.File) error {
	raw, err := root.SyscallConn()
	if err != nil {
		return err
	}
	var rootErr error
	if err := raw.Control(func(fd uintptr) { rootErr = w.directory(int(fd), 0) }); err != nil {
		return err
	}
	if rootErr != nil {
		return rootErr
	}
	for {
		entries, err := root.ReadDir(projidAuditRootBatch)
		for _, entry := range entries {
			if entry.Name() == projectIDFile {
				continue
			}
			name, parseErr := fstree.ParseName(entry.Name())
			if parseErr != nil {
				return parseErr
			}
			report, walkErr := fstree.WalkBeneath(ctx, root, name, w)
			if !report.Complete() {
				w.findings.changed = true
			}
			if walkErr != nil {
				return walkErr
			}
		}
		if errors.Is(err, io.EOF) || (err == nil && len(entries) == 0) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// Directory checks a directory fstree lends the walk. The top of each walked
// entry is depth 0.
func (w *projidAuditWalk) Directory(dir fstree.BorrowedDir, depth int) error {
	return dir.Control(func(fd int) error { return w.directory(fd, depth) })
}

// directory checks the project ID and inheritance of the directory open at
// fd. The volume directory itself is checked here too, at depth 0.
func (w *projidAuditWalk) directory(fd int, depth int) error {
	w.pace.wait()
	attr, err := w.attrs.GetProjectAttributes(fd)
	if err != nil {
		return fmt.Errorf("read directory project attributes at depth %d: %w", depth, err)
	}
	w.findings.auditedDirs++
	if attr.ProjectID != w.projID || attr.XFlags&linuxFSXFlagProjInherit == 0 {
		w.findings.driftedDirs++
	}
	return nil
}

// Entry checks a regular file's project ID. Symlinks, sockets, FIFOs and
// device nodes are counted as unauditable: reading their project ID would
// require opening them.
func (w *projidAuditWalk) Entry(parent fstree.BorrowedDir, name string, dtype uint8, depth int) error {
	if dtype != unix.DT_REG {
		w.findings.unauditable++
		return nil
	}
	return parent.Control(func(dirfd int) error { return w.file(dirfd, name, depth) })
}

// file checks the regular file name in the directory open at dirfd. The file
// is opened without following links or blocking, and must still be a regular
// file on the volume's device; an entry that changed, vanished or could not
// be opened without blocking leaves the volume changed or incomplete, never
// clean.
func (w *projidAuditWalk) file(dirfd int, name string, depth int) error {
	w.pace.wait()
	fd, err := openAuditedFile(dirfd, name)
	if err != nil {
		switch classifyProjidAuditOpen(err) {
		case projidAuditChanged:
			w.findings.changed = true
			return nil
		case projidAuditIncomplete:
			w.findings.incomplete = true
			return nil
		default:
			return fmt.Errorf("open a regular file at depth %d: %w", depth, err)
		}
	}
	defer func() { _ = unix.Close(fd) }()
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return fmt.Errorf("stat a regular file at depth %d: %w", depth, err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Dev != w.dev {
		w.findings.changed = true
		return nil
	}
	attr, err := w.attrs.GetProjectAttributes(fd)
	if err != nil {
		return fmt.Errorf("read file project attributes at depth %d: %w", depth, err)
	}
	w.findings.auditedFiles++
	if attr.ProjectID != w.projID {
		w.findings.driftedFiles++
	}
	return nil
}

// projidAuditFileFlags open a listed regular file only to read its
// attributes: read-only, never following a link, never blocking on a FIFO or
// a lease, and never becoming a controlling terminal.
const projidAuditFileFlags = unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_NONBLOCK | unix.O_NOCTTY | unix.O_CLOEXEC

// openAuditedFile opens name in dirfd for the audit, with O_NOATIME when the
// kernel permits it, so that the audit never updates a tenant file's access
// time. Linux permits O_NOATIME only to the file's owner or a caller with
// CAP_FOWNER and refuses it otherwise with EPERM; the open is then retried
// without it.
func openAuditedFile(dirfd int, name string) (int, error) {
	fd, err := unix.Openat(dirfd, name, projidAuditFileFlags|unix.O_NOATIME, 0)
	if errors.Is(err, unix.EPERM) {
		fd, err = unix.Openat(dirfd, name, projidAuditFileFlags, 0)
	}
	return fd, err
}

// classifyProjidAuditOpen maps the errno of opening a listed regular file.
// The entry was removed or replaced (ENOENT, ELOOP for a symlink, ENXIO for a
// socket, ENOTDIR) or another holder refused a non-blocking open (EAGAIN and
// EWOULDBLOCK, such as a lease). Every other errno is an error.
func classifyProjidAuditOpen(err error) projidAuditOutcome {
	switch {
	case errors.Is(err, unix.ENOENT), errors.Is(err, unix.ELOOP), errors.Is(err, unix.ENXIO), errors.Is(err, unix.ENOTDIR):
		return projidAuditChanged
	case errors.Is(err, unix.EAGAIN), errors.Is(err, unix.EWOULDBLOCK):
		return projidAuditIncomplete
	default:
		return projidAuditError
	}
}

// classifyProjidAuditWalk is the total classification of one volume's walk.
// Drift seen in completed reads is reported even when the walk stopped early:
// the counts are then a lower bound. Otherwise the walk's error decides, and
// only an undisturbed, finished walk that passed over nothing is clean.
func classifyProjidAuditWalk(findings projidAuditFindings, err error) projidAuditOutcome {
	switch {
	case findings.driftedDirs+findings.driftedFiles > 0:
		return projidAuditDrift
	case err == nil && findings.changed:
		return projidAuditChanged
	case err == nil && findings.incomplete:
		return projidAuditIncomplete
	case err == nil:
		return projidAuditClean
	case errors.Is(err, fstree.ErrTooDeep):
		return projidAuditTooDeep
	case errors.Is(err, fstree.ErrTreeChanged), errors.Is(err, fs.ErrNotExist):
		return projidAuditChanged
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return projidAuditIncomplete
	default:
		return projidAuditError
	}
}
