package docker

import (
	"context"
	"fmt"
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

	// Destroy removes the directory and quota. Idempotent.
	Destroy(ctx context.Context, id string) error

	// List returns the IDs of all managed volumes in the data directory.
	// Used for orphan detection at startup.
	List() ([]string, error)

	// Validate checks filesystem support and permissions, and rebuilds any
	// internal state (e.g. active project IDs) from on-disk volumes. Called at startup.
	Validate() error
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

func (n *noopVolumeManager) List() ([]string, error) {
	return nil, nil
}

func (n *noopVolumeManager) Validate() error {
	return nil
}

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
func newVolumeManager(dataPath, filesystem string, logger *slog.Logger) (volumeManager, error) {
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
		return &xfsVolumeManager{
			dataPath:   dataPath,
			logger:     logger,
			activeIDs:  make(map[uint32]string),
			volumeToID: make(map[string]uint32),
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
		if os.IsNotExist(err) {
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
