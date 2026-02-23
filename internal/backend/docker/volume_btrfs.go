package docker

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// btrfsVolumeManager creates btrfs subvolumes with qgroup quotas.
type btrfsVolumeManager struct {
	dataPath string
	logger   *slog.Logger
}

func (b *btrfsVolumeManager) Create(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
	subvolPath := filepath.Join(b.dataPath, id)
	quota := fmt.Sprintf("%dm", sizeMB)

	// Idempotent: if subvolume already exists, update quota and return.
	_, statErr := os.Stat(subvolPath)
	if statErr == nil {
		if out, err := exec.CommandContext(ctx, "btrfs", "qgroup", "limit", quota, subvolPath).CombinedOutput(); err != nil {
			return "", false, fmt.Errorf("btrfs qgroup limit %s on existing %s: %w: %s", quota, subvolPath, err, out)
		}
		b.logger.Debug("reusing existing btrfs subvolume", "path", subvolPath, "quota_mb", sizeMB)
		return subvolPath, false, nil
	}
	if !errors.Is(statErr, fs.ErrNotExist) {
		return "", false, fmt.Errorf("stat subvolume %s: %w", subvolPath, statErr)
	}

	// Create btrfs subvolume
	if out, err := exec.CommandContext(ctx, "btrfs", "subvolume", "create", subvolPath).CombinedOutput(); err != nil {
		return "", false, fmt.Errorf("btrfs subvolume create %s: %w: %s", subvolPath, err, out)
	}

	// Set quota on the subvolume
	if out, err := exec.CommandContext(ctx, "btrfs", "qgroup", "limit", quota, subvolPath).CombinedOutput(); err != nil {
		// Clean up the subvolume on quota failure. Use background context
		// because the caller's context may already be canceled (which could
		// have caused the quota failure), and this volume ID won't be in
		// createdVolumeIDs so the caller's cleanup loop won't cover it.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if cleanupOut, cleanupErr := exec.CommandContext(cleanupCtx, "btrfs", "subvolume", "delete", subvolPath).CombinedOutput(); cleanupErr != nil {
			b.logger.Warn("failed to cleanup subvolume after quota failure", "path", subvolPath, "error", cleanupErr, "output", string(cleanupOut))
		}
		return "", false, fmt.Errorf("btrfs qgroup limit %s on %s: %w: %s", quota, subvolPath, err, out)
	}

	b.logger.Debug("created btrfs subvolume", "path", subvolPath, "quota_mb", sizeMB)
	return subvolPath, true, nil
}

func (b *btrfsVolumeManager) Destroy(ctx context.Context, id string) error {
	subvolPath := filepath.Join(b.dataPath, id)

	out, err := exec.CommandContext(ctx, "btrfs", "subvolume", "delete", subvolPath).CombinedOutput()
	if err != nil {
		outStr := string(out)
		// Check if the subvolume doesn't exist (idempotent).
		// Match specific error strings rather than exit codes to avoid
		// swallowing permission, busy, or I/O errors.
		if strings.Contains(outStr, "cannot find") ||
			strings.Contains(outStr, "No such file or directory") ||
			strings.Contains(outStr, "not a btrfs subvolume") {
			b.logger.Debug("btrfs subvolume does not exist (idempotent)", "path", subvolPath)
			return nil
		}
		return fmt.Errorf("btrfs subvolume delete %s: %w: %s", subvolPath, err, outStr)
	}

	b.logger.Debug("destroyed btrfs subvolume", "path", subvolPath)
	return nil
}

func (b *btrfsVolumeManager) List() ([]string, error) {
	return listVolumeIDs(b.dataPath)
}

func (b *btrfsVolumeManager) Validate() error {
	// Check btrfs binary exists
	if _, err := exec.LookPath("btrfs"); err != nil {
		return fmt.Errorf("btrfs binary not found: %w", err)
	}

	// Check quotas are enabled on the filesystem
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "btrfs", "qgroup", "show", b.dataPath).CombinedOutput()
	if err != nil {
		return fmt.Errorf("btrfs quotas not enabled at %s (run 'btrfs quota enable %s'): %w: %s",
			b.dataPath, b.dataPath, err, out)
	}

	return nil
}
