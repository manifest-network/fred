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

// RenameVolume renames a btrfs subvolume root via plain os.Rename. The
// btrfs kernel module treats a subvolume root as a directory for rename
// purposes — the underlying subvolume identity (subvol-id), data, and
// qgroup attachment are preserved across the rename. No btrfs CLI call
// is needed.
func (b *btrfsVolumeManager) RenameVolume(oldName, newName string) error {
	oldPath := filepath.Join(b.dataPath, oldName)
	newPath := filepath.Join(b.dataPath, newName)
	return atomicRenameVolumeDir(oldPath, newPath)
}

// HostPath returns the absolute path of the subvolume under the
// configured data path. The subvolume may or may not exist; callers use
// this to compute paths for not-yet-renamed or about-to-be-created
// volumes.
func (b *btrfsVolumeManager) HostPath(name string) string {
	return filepath.Join(b.dataPath, name)
}

// Kind identifies the btrfs backend.
func (b *btrfsVolumeManager) Kind() string { return "btrfs" }

// Usage is implemented in Task 2.
func (b *btrfsVolumeManager) Usage(_ context.Context, _ string) (int64, error) {
	return 0, fmt.Errorf("btrfs usage not implemented: %w", errors.ErrUnsupported)
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
