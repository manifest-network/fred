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
	"syscall"
)

// volumeManager manages quota-enforced host directories for container volumes.
type volumeManager interface {
	// Create creates a quota-enforced directory for a container.
	// Idempotent: if the volume already exists, updates the quota and returns
	// the existing path. Returns the host path, whether the volume was newly
	// created (vs reused), and any error. sizeMB is the quota in megabytes.
	Create(ctx context.Context, id string, sizeMB int64) (hostPath string, created bool, err error)

	// EnsureQuota re-applies the quota (for xfs: project-tag + bhard limit) to an
	// EXISTING volume, so a volume created before the daemon could set quotas
	// (ENG-454) gets its disk_mb cap enforced without a re-provision or data
	// move. Unlike Create it NEVER creates: if the volume is absent it is a no-op
	// (returns nil), so a concurrently-deprovisioning volume cannot be
	// resurrected. Idempotent. Used by the startup backfill (reconcileVolumeQuotas).
	EnsureQuota(ctx context.Context, id string, sizeMB int64) error

	// Destroy removes the directory and quota. Idempotent.
	Destroy(ctx context.Context, id string) error

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

	if filesystem == "" {
		detected, err := detectFilesystem(dataPath)
		if err != nil {
			return nil, fmt.Errorf("auto-detect filesystem for volume_data_path: %w", err)
		}
		filesystem = detected
		logger.Info("auto-detected volume filesystem", "path", dataPath, "filesystem", filesystem)
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

// listVolumeIDs returns the names of all managed volume subdirectories in dataPath.
// Only directories with the "fred-" prefix are returned — other directories
// (e.g., lost+found, .snapshots) are ignored to avoid accidental deletion.
func listVolumeIDs(dataPath string) ([]string, error) {
	entries, err := os.ReadDir(dataPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
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
