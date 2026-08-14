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
)

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
	// before the daemon could set quotas (ENG-454) gets its disk_mb cap
	// enforced without a re-provision or data move. Unlike Create it NEVER
	// creates: if the volume is absent it is a no-op (returns nil), so a
	// concurrently-deprovisioning volume cannot be resurrected. Idempotent.
	// Used by the startup backfill (reconcileVolumeQuotas).
	EnsureQuota(ctx context.Context, id string, sizeMB int64) error

	// List returns the IDs of all managed volumes in the data directory.
	// Used for orphan detection at startup.
	List() ([]string, error)

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
	RenameVolume(oldName, newName string) error

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

// noopVolumeManager is used when no SKUs have disk_mb > 0.
// Create returns an error (callers must guard with disk_mb > 0 checks);
// Destroy is a no-op; Validate always succeeds.
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

func (n *noopVolumeManager) Validate() error {
	return nil
}

// RenameVolume on the noop manager is a no-op so migrate code paths can
// run on hosts with no stateful SKUs without a special case. The legacy
// lease cannot have had a managed volume to begin with.
func (n *noopVolumeManager) RenameVolume(_, _ string) error {
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
			dataPath:        dataPath,
			mountPoint:      mountPoint,
			logger:          logger,
			minAvgFileBytes: minAvgFileBytes,
			activeIDs:       make(map[uint32]string),
			volumeToID:      make(map[string]uint32),
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
// The baseline is learned rather than configured: the first enumeration that actually finds
// volumes records the device backing the root, and from then on an EMPTY result must come
// from that same device or it is treated as uncertainty. That ordering is what makes it
// safe — a root we have never seen hold anything has nothing to lose, and the transition
// this guards is precisely "held volumes, then suddenly none".
//
// Every enumeration pays one stat, not just the empty ones: a populated read is where the
// baseline is LEARNED, so skipping it there would leave nothing to compare against later.
// Only the empty result is REJECTED on a mismatch — a populated one re-learns instead,
// which is also what lets a deliberate remount converge rather than wedge.
//
// KNOWN LIMIT, deliberately not papered over: the baseline is process-local, so a daemon that
// STARTS while the mount is already down has never seen this root hold anything and its first
// empty enumeration is accepted. The startup filesystem probe covers that only when the parent
// filesystem differs from the configured one — on a host whose root and data volume are both
// XFS, both guards pass.
//
// Closing it needs a mount identity that survives a restart, which is a different mechanism
// (a marker file in the root, or persisted identity, plus an upgrade story for roots that
// predate it) and is tracked separately. The obvious shortcut — refusing to believe an empty
// root while retention records still name volumes — was tried and rejected: it cannot tell an
// unmount from a genuine reclaim of every volume, so it disables the orphan pruner on exactly
// the nodes it was built for (pinned by TestRunRetentionSweep_PrunesOrphans).
type volumeRootWatch struct {
	mu   sync.Mutex
	dev  uint64
	seen bool
}

// list enumerates dataPath and refuses to report emptiness it cannot vouch for.
func (w *volumeRootWatch) list(dataPath string) ([]string, error) {
	ids, err := listVolumeIDs(dataPath)
	if err != nil {
		return nil, err
	}
	dev, devErr := rootDevice(dataPath)
	if devErr != nil {
		// The directory enumerated a moment ago but will not stat now. Whatever that is,
		// it is not proof of emptiness.
		return nil, fmt.Errorf("identify volume data root %s: %w", dataPath, devErr)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(ids) > 0 {
		// Ground truth, and the only place the baseline is set. Re-learning it on every
		// populated read also means a deliberate remount onto a different device converges
		// on the next sweep instead of wedging until restart.
		w.dev, w.seen = dev, true
		return ids, nil
	}
	if w.seen && dev != w.dev {
		return nil, fmt.Errorf("volume data root %s is empty but now lives on a different filesystem "+
			"(device %d, was %d) — refusing to report it as empty; is it unmounted?", dataPath, dev, w.dev)
	}
	return ids, nil
}

// rootDevice returns the id of the device backing path. It is what changes under a plain
// unmount — the path survives, but it is served by the parent filesystem afterwards — which
// is exactly the transition an ENOENT check cannot see.
func rootDevice(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("stat %s: no syscall.Stat_t available on this platform", path)
	}
	return st.Dev, nil
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
	entries, err := os.ReadDir(dataPath)
	if err != nil {
		return nil, fmt.Errorf("read volume data directory %s: %w", dataPath, err)
	}
	var ids []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), volumePrefix) {
			ids = append(ids, e.Name())
		}
	}
	return ids, nil
}

// atomicRenameVolumeDir renames oldPath → newPath via os.Rename with
// idempotency guards. Shared by the xfs and btrfs backends, both of which
// support os.Rename on their volume roots (xfs: a plain directory; btrfs:
// a subvolume root, which the kernel renames as a metadata operation
// without touching the contained data).
//
// Idempotency semantics:
//   - neither exists  → error (caller asked to rename a non-existent volume)
//   - only old exists → os.Rename
//   - only new exists → nil (previous run already renamed; retry is safe)
//   - both exist      → error (operator must reconcile; we won't merge or pick a winner)
func atomicRenameVolumeDir(oldPath, newPath string) error {
	oldExists, oldErr := pathExists(oldPath)
	if oldErr != nil {
		return fmt.Errorf("stat old volume path %s: %w", oldPath, oldErr)
	}
	newExists, newErr := pathExists(newPath)
	if newErr != nil {
		return fmt.Errorf("stat new volume path %s: %w", newPath, newErr)
	}
	switch {
	case !oldExists && newExists:
		return nil // idempotent
	case oldExists && newExists:
		return fmt.Errorf("both old (%s) and new (%s) volume paths exist; manual intervention required", oldPath, newPath)
	case !oldExists && !newExists:
		return fmt.Errorf("neither old (%s) nor new (%s) volume path exists", oldPath, newPath)
	}
	return os.Rename(oldPath, newPath)
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
