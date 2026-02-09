package docker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// zfsVolumeManager creates ZFS child datasets with quotas.
type zfsVolumeManager struct {
	dataPath      string
	parentDataset string // cached during Validate()
	logger        *slog.Logger
}

// resolveParentDataset looks up the ZFS dataset name for the data path.
func resolveParentDataset(ctx context.Context, dataPath string) (string, error) {
	out, err := exec.CommandContext(ctx, "zfs", "list", "-H", "-o", "name", dataPath).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("zfs list for %s: %w: %s", dataPath, err, out)
	}
	name := strings.TrimSpace(string(out))
	if name == "" {
		return "", fmt.Errorf("no zfs dataset found for path %s", dataPath)
	}
	return name, nil
}

func (z *zfsVolumeManager) Create(ctx context.Context, id string, sizeMB int64) (string, error) {
	dataset := z.parentDataset + "/" + id
	quota := fmt.Sprintf("%dM", sizeMB)

	if out, err := exec.CommandContext(ctx, "zfs", "create", "-o", "quota="+quota, dataset).CombinedOutput(); err != nil {
		// Cleanup partially created dataset (zfs create is not atomic with quota).
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if cleanupOut, cleanupErr := exec.CommandContext(cleanupCtx, "zfs", "destroy", "-f", dataset).CombinedOutput(); cleanupErr != nil {
			z.logger.Warn("failed to cleanup zfs dataset after create failure", "dataset", dataset, "error", cleanupErr, "output", string(cleanupOut))
		}
		return "", fmt.Errorf("zfs create %s (quota=%s): %w: %s", dataset, quota, err, out)
	}

	// The dataset mountpoint is the child of the parent mountpoint.
	// Verify it actually exists — a non-default mountpoint property or
	// canmount=noauto would cause data to land on the parent filesystem
	// without quota enforcement.
	mountpoint := filepath.Join(z.dataPath, id)
	if _, err := os.Stat(mountpoint); err != nil {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if cleanupOut, cleanupErr := exec.CommandContext(cleanupCtx, "zfs", "destroy", "-f", dataset).CombinedOutput(); cleanupErr != nil {
			z.logger.Warn("failed to cleanup zfs dataset after mountpoint check failure", "dataset", dataset, "error", cleanupErr, "output", string(cleanupOut))
		}
		return "", fmt.Errorf("zfs dataset %s created but mountpoint %s not found: %w", dataset, mountpoint, err)
	}

	z.logger.Debug("created zfs dataset", "dataset", dataset, "mountpoint", mountpoint, "quota_mb", sizeMB)
	return mountpoint, nil
}

func (z *zfsVolumeManager) Destroy(ctx context.Context, id string) error {
	dataset := z.parentDataset + "/" + id

	out, err := exec.CommandContext(ctx, "zfs", "destroy", "-f", dataset).CombinedOutput()
	if err != nil {
		// Check if dataset doesn't exist (idempotent)
		if strings.Contains(string(out), "does not exist") {
			z.logger.Debug("zfs dataset does not exist (idempotent)", "dataset", dataset)
			return nil
		}
		return fmt.Errorf("zfs destroy %s: %w: %s", dataset, err, out)
	}

	z.logger.Debug("destroyed zfs dataset", "dataset", dataset)
	return nil
}

func (z *zfsVolumeManager) Validate() error {
	// Check zfs binary exists
	if _, err := exec.LookPath("zfs"); err != nil {
		return fmt.Errorf("zfs binary not found: %w", err)
	}

	// Resolve and cache parent dataset name
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	parent, err := resolveParentDataset(ctx, z.dataPath)
	if err != nil {
		return fmt.Errorf("zfs parent dataset validation failed: %w", err)
	}
	z.parentDataset = parent

	return nil
}
