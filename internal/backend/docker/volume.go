package docker

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const volumeCleanupTimeout = 10 * time.Second

// ErrVolumeMutationRecoveryPending means a volume manager has already made an
// irreversible mutation decision durable, but could not finish consuming its
// typed recovery evidence. The mutation adapter fail-stops the live Backend on
// this class so no later close, restore, or collector can infer completion from
// a partially removed namespace. A fresh process must run startup recovery.
var ErrVolumeMutationRecoveryPending = errors.New("volume mutation recovery pending")

// newDetachedBoundedContext lets compensation finish after request
// cancellation, while still honoring an earlier aggregate phase deadline. A
// plain context.WithoutCancel followed by WithTimeout would silently extend an
// already-expired Start budget by the full local timeout on every stage.
func newDetachedBoundedContext(parent context.Context, maximum time.Duration) (context.Context, context.CancelFunc) {
	base := context.WithoutCancel(parent)
	deadline := time.Now().Add(maximum)
	if parentDeadline, ok := parent.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	return context.WithDeadline(base, deadline)
}

// newVolumeCleanupContext gives a compensating cleanup a bounded lifetime that
// survives request cancellation while preserving tracing and other context
// values. Callers may use it only after proving this invocation created the
// unpublished storage object being cleaned up; ambiguous outcomes must retain
// their evidence for retry instead of guessing through destruction.
func newVolumeCleanupContext(parent context.Context) (context.Context, context.CancelFunc) {
	return newDetachedBoundedContext(parent, volumeCleanupTimeout)
}

// volumeManager manages quota-enforced host directories for container volumes.
//
// It deliberately does NOT include Destroy. Every concrete manager implements it, but
// exposing it here would put an irreversible RemoveAll on tenant data one method call
// away from any code holding b.volumes — which is how six call sites came to derive
// their own destroy sets, three of them from a name that does not prove ownership.
// Destruction is reached only through volumeOp.destroy (volume_destroy.go), which asks
// who owns the bytes first; leaving the method off this interface is what turns
// forgetting to ask into a compile error rather than a review comment. (ENG-658)
type volumeManager interface {
	// Create creates a quota-enforced directory for a container.
	// Idempotent: if the volume already exists, updates the quota and returns
	// the existing path. Returns the host path, whether the volume was newly
	// created (vs reused), and any error. sizeMB is the quota in megabytes.
	Create(ctx context.Context, id string, sizeMB int64) (hostPath string, created bool, err error)

	// EnsureQuota re-applies the quota (for xfs: project-tag + block (bhard)
	// and inode (ihard) limits) to an EXISTING volume, so a volume created
	// before the daemon could set quotas (ENG-454) gets its immutable effective
	// cap (durable disk or pinned diskless scratch)
	// enforced without a re-provision or data move. Unlike Create it NEVER
	// creates: if the volume is absent it is a no-op (returns nil), so a
	// concurrently-deprovisioning volume cannot be resurrected. Idempotent.
	// Used by the startup backfill (reconcileVolumeQuotas).
	EnsureQuota(ctx context.Context, id string, sizeMB int64) error

	// List returns the IDs of all managed volumes in the data directory.
	// Used for orphan detection at startup.
	List() ([]string, error)

	// ListForProof returns the complete managed-volume substrate inventory under
	// a caller-owned deadline. For directory-backed managers this is the same
	// namespace as List. ZFS additionally inventories child datasets so an
	// unmounted or externally-mounted dataset cannot disappear from the proof
	// merely because its expected directory is absent.
	ListForProof(context.Context) ([]string, error)

	// AttestManagedVolume proves that name is backed by this manager's exact
	// quota substrate, not merely by a directory with a syntactically valid
	// managed name. The typed name keeps unvalidated path and dataset strings
	// out of the proof boundary; the context bounds any filesystem CLI probe.
	// It is read-only and is used before storage identity publication and
	// backend startup, as well as before reusing an existing volume.
	AttestManagedVolume(context.Context, managedVolumeName) error

	// RequireNoInterruptedVolumeMutations is the read-only publication boundary
	// for first-time storage-lineage initialization and post-recovery startup. It
	// rejects manager-private mutation evidence (for example an XFS create/delete
	// stage or an exact but unmounted ZFS child) without repairing or deleting it.
	// Explicit adoption calls this while the old daemon is stopped, so a proof
	// command can never mutate the lineage it is measuring.
	RequireNoInterruptedVolumeMutations(context.Context) error

	// RecoverInterruptedVolumeMutations resolves manager-private mutation evidence
	// after the identity-bound stores have been opened exclusively but before
	// ordinary operation-intent recovery. Implementations may only act on strictly
	// typed substrate evidence and must retain it on an ambiguous or failed
	// cleanup. The storage mutation adapter surrounds this method with before/after
	// lineage verification.
	RecoverInterruptedVolumeMutations(context.Context) error

	// Validate checks filesystem support and permissions, and rebuilds any
	// internal state (e.g. active project IDs) from on-disk volumes. Called at startup.
	Validate() error

	// RenameVolume atomically renames a managed volume from oldName to
	// newName, preserving data and per-volume metadata (xfs project ID,
	// btrfs subvolume identity, zfs dataset name). Idempotent: if the new
	// name already exists and the old does not, returns nil; if both
	// exist, returns an error so the operator can intervene; if neither
	// exists, returns an error.
	//
	// Used by Task 9's recover-time migration to convert legacy
	// fred-{leaseUUID}-{idx} volumes into the service-aware
	// fred-{leaseUUID}-{service}-{idx} naming convention without copying
	// data.
	RenameVolume(ctx context.Context, oldName, newName string) error

	// HostPath returns the absolute on-host path for a managed volume of
	// the given name. The volume need not yet exist — this lets callers
	// compute paths for not-yet-renamed or about-to-be-created volumes.
	// The path returned is the conventional mount point under the
	// configured volume_data_path; the actual mount target may differ on
	// zfs if mountpoint properties were overridden, but production code
	// expects default inheritance.
	HostPath(name string) string

	// Usage returns the volume's current data footprint in BYTES. Backends
	// without a usage primitive (noop) return an error wrapping
	// errors.ErrUnsupported (Go 1.21+); callers detect it with
	// errors.Is(err, errors.ErrUnsupported). Used by the restore demote
	// fit-gate (checkDemoteFit) to refuse a tier-down that would not fit.
	Usage(ctx context.Context, id string) (int64, error)

	// Kind returns the backend filesystem name ("btrfs", "xfs", "zfs",
	// "noop") for metric labeling and logging.
	Kind() string
}

type identityRootPinner interface {
	PinIdentityRoot() error
	VerifyIdentityRoot() error
}

var errVolumeRootIdentityDrift = errors.New("volume root identity drift")

// noopVolumeManager is used when volume_data_path is unset. Create returns an
// error; stateful SKUs are rejected by config validation and diskless
// writable-path setup treats the unavailable optional scratch volume as a
// best-effort seeding failure. Destroy is a no-op; Validate always succeeds.
type noopVolumeManager struct{}

func (n *noopVolumeManager) Create(_ context.Context, _ string, _ int64) (string, bool, error) {
	return "", false, fmt.Errorf("noop volume manager cannot create volumes")
}

func (n *noopVolumeManager) Destroy(_ context.Context, _ string) error {
	return nil
}

// EnsureQuota on the noop manager is a no-op: it manages no quota-enforced
// volumes, so there is nothing to re-apply.
func (n *noopVolumeManager) EnsureQuota(_ context.Context, _ string, _ int64) error {
	return nil
}

func (n *noopVolumeManager) List() ([]string, error) {
	return nil, nil
}

func (n *noopVolumeManager) ListForProof(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, nil
}

func (n *noopVolumeManager) AttestManagedVolume(_ context.Context, name managedVolumeName) error {
	return fmt.Errorf("noop volume manager cannot attest managed volume %q", name.value())
}

func (n *noopVolumeManager) RequireNoInterruptedVolumeMutations(context.Context) error { return nil }

func (n *noopVolumeManager) RecoverInterruptedVolumeMutations(context.Context) error { return nil }

func (n *noopVolumeManager) Validate() error {
	return nil
}

// RenameVolume on the noop manager is a no-op so migrate code paths can
// run on hosts with no stateful SKUs without a special case. The legacy
// lease cannot have had a managed volume to begin with.
func (n *noopVolumeManager) RenameVolume(_ context.Context, _, _ string) error {
	return nil
}

// HostPath on the noop manager returns an empty string — there is no
// configured volume root. Callers should not invoke this; the migration
// pipeline guards on stateful-volume presence before constructing host
// paths.
func (n *noopVolumeManager) HostPath(_ string) string {
	return ""
}

// Usage on the noop manager is unsupported — it manages no quota-enforced
// volumes. Returns a wrapped errors.ErrUnsupported so the demote gate's
// "unmeasurable" branch detects it via errors.Is. Unreachable in practice:
// a noop backend never holds a retained stateful volume.
func (n *noopVolumeManager) Usage(_ context.Context, _ string) (int64, error) {
	return 0, fmt.Errorf("noop volume manager cannot measure usage: %w", errors.ErrUnsupported)
}

// Kind identifies the noop backend.
func (n *noopVolumeManager) Kind() string { return "noop" }

// Filesystem magic numbers from statfs(2).
const (
	btrfsMagic = 0x9123683E
	xfsMagic   = 0x58465342
	zfsMagic   = 0x2FC12FC1
)

// detectFilesystem returns the filesystem type of the given path
// using statfs(2) magic numbers.
func detectFilesystem(path string) (string, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return "", fmt.Errorf("statfs %s: %w", path, err)
	}
	switch stat.Type {
	case btrfsMagic:
		return "btrfs", nil
	case xfsMagic:
		return "xfs", nil
	case zfsMagic:
		return "zfs", nil
	default:
		return "", fmt.Errorf("unsupported filesystem (magic 0x%X) at %s; volume quotas require btrfs, xfs, or zfs", stat.Type, path)
	}
}

// newVolumeManager creates a volumeManager for the given data path and filesystem.
// If dataPath is empty, returns a noopVolumeManager.
// If filesystem is empty, it is auto-detected from the data path.
func newVolumeManager(dataPath, filesystem string, minAvgFileBytes int64, logger *slog.Logger) (volumeManager, error) {
	if dataPath == "" {
		return &noopVolumeManager{}, nil
	}

	// Reject a nonsense volume_filesystem before touching the disk, so a typo reports itself
	// as a typo rather than as whatever the probe below happens to find.
	switch filesystem {
	case "", "btrfs", "xfs", "zfs":
	default:
		return nil, fmt.Errorf("unsupported volume_filesystem: %s", filesystem)
	}

	// The data path is probed EVEN WHEN the filesystem is configured explicitly. It used to
	// be probed only on the auto-detect branch, which meant a provider that pinned
	// volume_filesystem started happily on a volume root that was not the filesystem it
	// named — including the case that matters, an unmounted mountpoint, where the directory
	// survives and is served by the parent filesystem. Booting there enumerates zero volumes
	// and looks exactly like a fresh node, which is how the orphan pruner comes to prune
	// live retention records (ENG-687). Failing startup is the right response: it is a
	// misconfiguration or a missing mount, and both need an operator, not a sweep.
	detected, err := detectFilesystem(dataPath)
	if err != nil {
		return nil, fmt.Errorf("probe filesystem at volume_data_path %q (is the volume mounted?): %w", dataPath, err)
	}
	switch {
	case filesystem == "":
		filesystem = detected
		logger.Info("auto-detected volume filesystem", "path", dataPath, "filesystem", filesystem)
	case filesystem != detected:
		return nil, fmt.Errorf("volume_data_path %q is on %s, but volume_filesystem is configured as %s "+
			"(is the volume mounted?)", dataPath, detected, filesystem)
	}

	switch filesystem {
	case "btrfs":
		return &btrfsVolumeManager{dataPath: dataPath, logger: logger}, nil
	case "xfs":
		// xfs_quota requires the XFS mount point as its filesystem argument;
		// dataPath is typically a subdirectory of that mount (ENG-449).
		mountPoint, err := resolveMountpoint(dataPath)
		if err != nil {
			return nil, fmt.Errorf("resolve xfs mount point for volume_data_path %q: %w", dataPath, err)
		}
		return &xfsVolumeManager{
			dataPath:          dataPath,
			mountPoint:        mountPoint,
			logger:            logger,
			minAvgFileBytes:   minAvgFileBytes,
			projectAttributes: linuxXFSProjectAttributeReader{},
			activeIDs:         make(map[uint32]string),
			volumeToID:        make(map[string]uint32),
		}, nil
	case "zfs":
		return &zfsVolumeManager{dataPath: dataPath, logger: logger}, nil
	default:
		return nil, fmt.Errorf("unsupported volume_filesystem %q; must be btrfs, xfs, or zfs", filesystem)
	}
}

// volumePrefix is the naming prefix for all managed volume directories.
const volumePrefix = "fred-"

// volumeRootWatch makes "this root holds no managed volumes" a claim the enumeration has
// to earn, rather than one it can arrive at by accident.
//
// listVolumeIDs distinguishes a MISSING root from an empty one, which covers a data path
// that disappears. It cannot cover the other way a root stops being itself: when
// volume_data_path is a MOUNTPOINT, a plain `umount` leaves the directory in place on the
// parent filesystem, so ReadDir succeeds and returns the empty stub with no error at all.
// Every consumer then reads a confident "no volumes here" — and acts on it.
//
// The consequence is not confined to accounting. reconcileOrphanedRetentions treats an
// absent volume as evidence a retention record is orphaned, so an empty enumeration makes
// allVolumesAbsent vacuously true for EVERY active record; after the confirmation streak it
// prunes them, and once the filesystem is mounted again those volumes have no record naming
// them — so the next boot's orphan sweep destroys retained tenant data. (ENG-687)
//
// Before a storage lineage is sealed, the watch can learn a provisional baseline: the first
// enumeration that actually finds volumes records the device backing the root, and from then
// on an EMPTY result must come from that same device or it is treated as uncertainty. This
// learned mode remains useful to isolated tests and to read-only initialization evidence.
// Production startup does not rely on it: loadStorageIdentity pins the root's exact device and
// inode before loading the persistent marker pair, whose primary marker lives inside this root.
// A daemon that starts with the mount already absent therefore fails marker loading instead of
// accepting the empty parent-filesystem stub.
//
// Every enumeration pays one stat, not just the empty ones: a populated read is where the
// baseline is LEARNED, so skipping it there would leave nothing to compare against later.
// In provisional learned mode, only an empty mismatch is rejected and a populated read may
// re-learn. Once production pins the root, both populated and empty mismatches are rejected;
// a deliberate remount is a storage-lineage change that requires explicit operator handling,
// never automatic convergence. The tempting shortcut — refusing to believe an empty root
// while retention records still name volumes — remains wrong: it cannot distinguish an
// unmount from a genuine reclaim of every volume, so it disables the orphan pruner on exactly
// the nodes it was built for (pinned by TestRunRetentionSweep_PrunesOrphans).
type volumeRootWatch struct {
	mu     sync.Mutex
	dev    uint64
	ino    uint64
	seen   bool
	pinned bool
}

// pin freezes the filesystem device that carries the identity marker. Once
// pinned, a populated replacement is rejected just like an empty replacement;
// runtime code can no longer relearn a different substrate.
func (w *volumeRootWatch) pin(dataPath string) error {
	identity, err := statVolumeRootIdentity(dataPath)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.seen && (w.dev != identity.dev || (w.ino != 0 && w.ino != identity.ino)) {
		return fmt.Errorf("volume data root %s changed before identity pin (device/inode %d/%d, was %d/%d)",
			dataPath, identity.dev, identity.ino, w.dev, w.ino)
	}
	w.dev, w.ino, w.seen, w.pinned = identity.dev, identity.ino, true, true
	return nil
}

// verify proves that the configured path still resolves to the exact directory
// inode pinned before the backend identity was loaded. Device-only comparison
// misses a same-filesystem rename/replacement, which is enough to redirect all
// subsequent volume mutations to unrelated bytes.
func (w *volumeRootWatch) verify(dataPath string) error {
	identity, err := statVolumeRootIdentity(dataPath)
	if err != nil {
		// A configured, pinned root disappearing is not a transient probe
		// failure. Continuing would redirect subsequent creates/destroys to the
		// parent filesystem if the path is recreated, so latch the same permanent
		// drift class as an inode/device replacement.
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%w: pinned volume data root %s disappeared: %w",
				errVolumeRootIdentityDrift, dataPath, err)
		}
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.pinned || !w.seen {
		return errors.New("volume data root identity is not pinned")
	}
	if identity.dev != w.dev || identity.ino != w.ino {
		return fmt.Errorf("%w: volume data root %s changed identity (device/inode %d/%d, expected %d/%d)",
			errVolumeRootIdentityDrift,
			dataPath, identity.dev, identity.ino, w.dev, w.ino)
	}
	return nil
}

// list enumerates dataPath and refuses to report emptiness it cannot vouch for.
func (w *volumeRootWatch) list(dataPath string) ([]string, error) {
	return w.listMatching(dataPath, false)
}

// listForProof inventories every entry in fred's reserved managed namespace,
// regardless of filesystem type. Runtime List deliberately returns only real
// directories because its consumers may derive deletion candidates; a lineage
// proof has the opposite requirement and must surface a canonical-looking
// symlink, regular file, or malformed fred-* collision so the typed parser and
// concrete attester can reject it rather than seal an incomplete view.
func (w *volumeRootWatch) listForProof(dataPath string) ([]string, error) {
	return w.listMatching(dataPath, true)
}

func (w *volumeRootWatch) listMatching(dataPath string, includeNonDirectories bool) ([]string, error) {
	// The lock spans the OBSERVATION as well as the update, not just the update. Two
	// concurrent readings can otherwise be applied out of order — the older one landing last
	// and installing a baseline that was already superseded — which is the same
	// stale-snapshot-wins hazard the destroy path had to fix, arriving by a different door.
	// List runs once per sweep or close, so serializing it costs nothing worth measuring.
	w.mu.Lock()
	defer w.mu.Unlock()

	ids, identity, err := listVolumeEntriesWithRootIdentity(dataPath, includeNonDirectories)
	if err != nil {
		return nil, err
	}
	if len(ids) > 0 {
		if w.pinned && (identity.dev != w.dev || identity.ino != w.ino) {
			return nil, fmt.Errorf("volume data root %s moved to a different identity-bound filesystem "+
				"(device/inode %d/%d, expected %d/%d)",
				dataPath, identity.dev, identity.ino, w.dev, w.ino)
		}
		// Ground truth, and the only place a provisional, unpinned baseline is set.
		// Production pins before startup work, so it never re-learns a replacement.
		if !w.pinned {
			w.dev, w.ino, w.seen = identity.dev, identity.ino, true
		}
		return ids, nil
	}
	if w.seen && (identity.dev != w.dev || (w.pinned && identity.ino != w.ino)) {
		return nil, fmt.Errorf("volume data root %s is empty but now lives on a different filesystem "+
			"(device/inode %d/%d, was %d/%d) — refusing to report it as empty; is it unmounted?",
			dataPath, identity.dev, identity.ino, w.dev, w.ino)
	}
	return ids, nil
}

// listVolumeIDs returns the names of all managed volume subdirectories in dataPath.
// Only directories with the "fred-" prefix are returned — other directories
// (e.g., lost+found, .snapshots) are ignored to avoid accidental deletion.
//
// AN ABSENT ROOT IS AN ERROR, NOT AN EMPTY NODE. This used to map ENOENT to
// (nil, nil), which collapsed two states a caller must distinguish — "the volume root
// holds no volumes" and "the volume root is gone" — into one indistinguishable value.
// Every consumer of that value then had to reconstruct the difference with a separate
// stat, and a separate stat is a separate point in time: the root could vanish between
// the probe and the read, and the caller would see a confident, empty, error-free answer.
// The reaping finalizer acts on exactly that answer by DELETING the record that accounts
// for the bytes, so the ambiguity was one unmount away from silent data-accounting loss.
//
// Returning the error keeps the whole question inside the single syscall that can answer
// it, which is what makes the race impossible rather than merely unlikely — there is no
// second observation to disagree with the first. Callers that must tolerate an
// unconfigured root already do: volume_data_path == "" yields the noopVolumeManager,
// whose List is a different implementation entirely. A CONFIGURED root cannot legitimately
// be absent at runtime — newVolumeManager statfs's it (detectFilesystem) and, for xfs,
// resolves its mount point, so construction fails before Start if it is missing — which
// makes ENOENT here unambiguously "it disappeared underneath us".
func listVolumeIDs(dataPath string) ([]string, error) {
	ids, _, err := listVolumeIDsWithDevice(dataPath)
	return ids, err
}

// listVolumeIDsWithDevice is listVolumeIDs plus the identity of the filesystem the listing
// actually came from, taken from THE SAME open directory handle.
//
// One handle, not two calls against the path, and that is the whole point. A stat of the path
// is a SECOND observation: an unmount landing between the enumeration and the stat pairs
// volumes read from the old mount with the parent filesystem's device, which is worse than no
// check at all — the populated branch treats that pairing as ground truth and adopts the
// parent device as the baseline, after which every later empty reading matches and is
// accepted. The guard would silently invert into a rubber stamp.
//
// An fd does not have that problem. It refers to the opened inode on the opened filesystem for
// as long as it is held, whatever happens to the path underneath it, so fstat and getdents on
// the same descriptor cannot disagree about which filesystem they read. That is the same rule
// this file already applies elsewhere — keep the question inside the single operation that can
// answer it — applied one level deeper than it was.
func listVolumeIDsWithDevice(dataPath string) ([]string, uint64, error) {
	ids, identity, err := listVolumeIDsWithRootIdentity(dataPath)
	return ids, identity.dev, err
}

type volumeRootIdentity struct {
	dev uint64
	ino uint64
}

// statVolumeRootIdentity performs the cheap runtime attestation path: one
// open+fstat and no directory enumeration. HTTP requests call it frequently,
// so making identity proof O(number of tenant volumes) would serialize every
// backend request behind an increasingly expensive global check.
func statVolumeRootIdentity(dataPath string) (volumeRootIdentity, error) {
	f, err := os.Open(dataPath) //nolint:gosec // operator-configured root, validated at construction
	if err != nil {
		return volumeRootIdentity{}, fmt.Errorf("open volume data directory %s: %w", dataPath, err)
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		return volumeRootIdentity{}, fmt.Errorf("stat volume data directory %s: %w", dataPath, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return volumeRootIdentity{}, fmt.Errorf("stat %s: no syscall.Stat_t available on this platform", dataPath)
	}
	return volumeRootIdentity{dev: st.Dev, ino: st.Ino}, nil
}

// listVolumeIDsWithRootIdentity is the stronger identity-bearing primitive
// used after storage-lineage pinning. The directory inode distinguishes an
// in-place path replacement on the same filesystem; both values come from the
// same descriptor used for enumeration.
func listVolumeIDsWithRootIdentity(dataPath string) ([]string, volumeRootIdentity, error) {
	return listVolumeEntriesWithRootIdentity(dataPath, false)
}

func listVolumeEntriesWithRootIdentity(
	dataPath string,
	includeNonDirectories bool,
) ([]string, volumeRootIdentity, error) {
	f, err := os.Open(dataPath) //nolint:gosec // G304: dataPath is the operator-configured volume_data_path, validated at construction — never tenant-reachable (volume names are appended by callers, not by this open)
	if err != nil {
		return nil, volumeRootIdentity{}, fmt.Errorf("open volume data directory %s: %w", dataPath, err)
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		return nil, volumeRootIdentity{}, fmt.Errorf("stat volume data directory %s: %w", dataPath, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, volumeRootIdentity{}, fmt.Errorf("stat %s: no syscall.Stat_t available on this platform", dataPath)
	}

	entries, err := f.ReadDir(-1)
	if err != nil {
		return nil, volumeRootIdentity{}, fmt.Errorf("read volume data directory %s: %w", dataPath, err)
	}
	var ids []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), volumePrefix) && (includeNonDirectories || e.IsDir()) {
			ids = append(ids, e.Name())
		}
	}
	return ids, volumeRootIdentity{dev: st.Dev, ino: st.Ino}, nil
}

// pathExists reports whether p exists on the filesystem. Distinct from
// `_, err := os.Stat(p); err == nil` because it surfaces non-ENOENT stat
// errors (permission denied, I/O failure) to the caller rather than
// silently treating them as "doesn't exist".
func pathExists(p string) (bool, error) {
	_, err := os.Stat(p)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// sanitizeVolumePath converts a container volume path to a safe subdirectory name.
// e.g., "/data" -> "data", "/var/lib/postgresql/data" -> "var/lib/postgresql/data"
// Returns "" for invalid paths (root, empty, or paths that escape the parent).
func sanitizeVolumePath(containerPath string) string {
	cleaned := filepath.Clean(containerPath)
	cleaned = strings.TrimPrefix(cleaned, "/")
	if cleaned == "" || cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return ""
	}
	return cleaned
}
