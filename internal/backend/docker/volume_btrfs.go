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
	"strconv"
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

// Usage returns the subvolume's referenced bytes (rfer) via its qgroup.
// rfer is the column `btrfs qgroup limit` enforces (max_rfer) and is the
// safe over-count (it also accounts the subvolume's own fs-tree metadata).
// --sync forces a transaction commit so the figure reflects committed
// writes (retained volumes have no live writer, so this is cheap insurance,
// not a recount — never a `quota rescan`). rfer != excl is logged
// (informational), never an error: gating on rfer is correct regardless.
func (b *btrfsVolumeManager) Usage(ctx context.Context, id string) (int64, error) {
	subvolPath := filepath.Join(b.dataPath, id)
	showOut, err := exec.CommandContext(ctx, "btrfs", "subvolume", "show", subvolPath).CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("btrfs subvolume show %s: %w: %s", subvolPath, err, showOut)
	}
	subvolID, err := parseBtrfsSubvolumeID(string(showOut))
	if err != nil {
		return 0, fmt.Errorf("resolve subvolume id for %s: %w", subvolPath, err)
	}
	qOut, err := exec.CommandContext(ctx, "btrfs", "qgroup", "show", "--raw", "--sync", b.dataPath).CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("btrfs qgroup show %s: %w: %s", b.dataPath, err, qOut)
	}
	rfer, excl, err := parseBtrfsQgroupRfer(string(qOut), subvolID)
	if err != nil {
		return 0, fmt.Errorf("read qgroup usage for subvol %d under %s: %w", subvolID, subvolPath, err)
	}
	if rfer != excl {
		b.logger.Info("btrfs qgroup rfer != excl (shared extents?); gating on rfer",
			"path", subvolPath, "subvol_id", subvolID, "rfer", rfer, "excl", excl)
	}
	return rfer, nil
}

// parseBtrfsSubvolumeID extracts the "Subvolume ID: <N>" field from
// `btrfs subvolume show` output.
func parseBtrfsSubvolumeID(out string) (uint64, error) {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Subvolume ID:") {
			fields := strings.Fields(line)
			id, err := strconv.ParseUint(fields[len(fields)-1], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse subvolume id %q: %w", line, err)
			}
			return id, nil
		}
	}
	return 0, fmt.Errorf("no \"Subvolume ID:\" line in btrfs subvolume show output")
}

// parseBtrfsQgroupRfer finds the qgroup line "0/<subvolID>" in
// `btrfs qgroup show --raw` output and returns (rfer, excl) in bytes.
func parseBtrfsQgroupRfer(out string, subvolID uint64) (int64, int64, error) {
	want := fmt.Sprintf("0/%d", subvolID)
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[0] != want {
			continue
		}
		rfer, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("parse rfer %q: %w", fields[1], err)
		}
		excl, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("parse excl %q: %w", fields[2], err)
		}
		return rfer, excl, nil
	}
	return 0, 0, fmt.Errorf("qgroup %s not found in qgroup show output (quota enabled?)", want)
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
