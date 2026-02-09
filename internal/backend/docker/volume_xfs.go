package docker

import (
	"context"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
)

// xfsVolumeManager creates directories with XFS project quotas.
type xfsVolumeManager struct {
	dataPath string
	logger   *slog.Logger
}

// projectID returns a deterministic project ID from the volume ID.
// Uses CRC32 to map arbitrary strings to uint32 (4.3 billion values),
// avoiding the need for a persistent mapping file.
//
// Collision note: the birthday paradox gives ~50% collision probability
// at ~65,000 volumes. In practice, volumes are short-lived (destroyed on
// deprovision), so the active set is much smaller than total-ever-created.
// If collisions become a concern at scale, replace with a sequential
// allocator backed by a persistent store.
func projectID(volumeID string) uint32 {
	return crc32.ChecksumIEEE([]byte(volumeID))
}

func (x *xfsVolumeManager) Create(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
	dirPath := filepath.Join(x.dataPath, id)
	projID := projectID(id)
	quota := fmt.Sprintf("%dm", sizeMB)

	// Idempotent: if directory already exists, update quota and return.
	_, statErr := os.Stat(dirPath)
	if statErr == nil {
		cmd := fmt.Sprintf("limit -p bhard=%s %d", quota, projID)
		if out, err := exec.CommandContext(ctx, "xfs_quota", "-x", "-c", cmd, x.dataPath).CombinedOutput(); err != nil {
			return "", false, fmt.Errorf("xfs_quota limit on existing %s (id=%d, quota=%s): %w: %s", dirPath, projID, quota, err, out)
		}
		x.logger.Debug("reusing existing xfs quota directory", "path", dirPath, "project_id", projID, "quota_mb", sizeMB)
		return dirPath, false, nil
	}
	if !os.IsNotExist(statErr) {
		return "", false, fmt.Errorf("stat volume dir %s: %w", dirPath, statErr)
	}

	// Create new directory.
	if err := os.MkdirAll(dirPath, 0777); err != nil {
		return "", false, fmt.Errorf("create directory %s: %w", dirPath, err)
	}

	// Assign project ID to directory
	cmd := fmt.Sprintf("project -s -p %s %d", dirPath, projID)
	if out, err := exec.CommandContext(ctx, "xfs_quota", "-x", "-c", cmd, x.dataPath).CombinedOutput(); err != nil {
		if cleanupErr := os.RemoveAll(dirPath); cleanupErr != nil {
			x.logger.Warn("failed to cleanup directory after xfs project setup failure", "path", dirPath, "error", cleanupErr)
		}
		return "", false, fmt.Errorf("xfs_quota project setup for %s (id=%d): %w: %s", dirPath, projID, err, out)
	}

	// Set quota limit
	cmd = fmt.Sprintf("limit -p bhard=%s %d", quota, projID)
	if out, err := exec.CommandContext(ctx, "xfs_quota", "-x", "-c", cmd, x.dataPath).CombinedOutput(); err != nil {
		if cleanupErr := os.RemoveAll(dirPath); cleanupErr != nil {
			x.logger.Warn("failed to cleanup directory after xfs quota limit failure", "path", dirPath, "error", cleanupErr)
		}
		return "", false, fmt.Errorf("xfs_quota limit for %s (id=%d, quota=%s): %w: %s", dirPath, projID, quota, err, out)
	}

	x.logger.Debug("created xfs project quota directory", "path", dirPath, "project_id", projID, "quota_mb", sizeMB)
	return dirPath, true, nil
}

func (x *xfsVolumeManager) Destroy(ctx context.Context, id string) error {
	dirPath := filepath.Join(x.dataPath, id)

	// Remove the directory (quota is implicitly freed)
	if err := os.RemoveAll(dirPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove directory %s: %w", dirPath, err)
	}

	x.logger.Debug("destroyed xfs quota directory", "path", dirPath)
	return nil
}

func (x *xfsVolumeManager) List() ([]string, error) {
	return listVolumeIDs(x.dataPath)
}

func (x *xfsVolumeManager) Validate() error {
	// Check xfs_quota binary exists
	if _, err := exec.LookPath("xfs_quota"); err != nil {
		return fmt.Errorf("xfs_quota binary not found: %w", err)
	}

	// Check pquota mount option by attempting a quota report
	out, err := exec.Command("xfs_quota", "-x", "-c", "report -p", x.dataPath).CombinedOutput()
	if err != nil {
		return fmt.Errorf("xfs project quotas not available at %s (mount with pquota option): %w: %s",
			x.dataPath, err, out)
	}

	return nil
}
