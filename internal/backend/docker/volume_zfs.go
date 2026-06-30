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

func (z *zfsVolumeManager) Create(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
	dataset := z.parentDataset + "/" + id
	mountpoint := filepath.Join(z.dataPath, id)
	quota := fmt.Sprintf("%dM", sizeMB)

	// Idempotent: if dataset already exists (mountpoint present), update quota and return.
	_, statErr := os.Stat(mountpoint)
	if statErr == nil {
		// Set refquota= and clear any legacy quota= in one atomic call.
		// Before ENG-438, Create used `zfs set quota=` instead of refquota=, so
		// datasets created on an older binary carry a stale quota= property.
		// ZFS enforces the tighter of quota and refquota simultaneously; a stale
		// smaller quota= would silently bind on a promote (newCap > oldCap).
		// Setting quota=none removes the legacy limit so refquota is the sole cap.
		if out, err := exec.CommandContext(ctx, "zfs", "set", "refquota="+quota, "quota=none", dataset).CombinedOutput(); err != nil {
			return "", false, fmt.Errorf("zfs set refquota and clear legacy quota on existing %s: %w: %s", dataset, err, out)
		}
		z.logger.Debug("reusing existing zfs dataset", "dataset", dataset, "mountpoint", mountpoint, "quota_mb", sizeMB)
		return mountpoint, false, nil
	}
	if !errors.Is(statErr, fs.ErrNotExist) {
		return "", false, fmt.Errorf("stat mountpoint %s: %w", mountpoint, statErr)
	}

	// Mountpoint absent — check if the dataset exists but is unmounted
	// (e.g., after a pool import anomaly or manual zfs unmount). If so,
	// mount it and update quota rather than creating a new dataset.
	if existing, _ := exec.CommandContext(ctx, "zfs", "list", "-H", "-o", "name", dataset).CombinedOutput(); strings.TrimSpace(string(existing)) == dataset {
		if out, err := exec.CommandContext(ctx, "zfs", "mount", dataset).CombinedOutput(); err != nil {
			return "", false, fmt.Errorf("zfs mount existing unmounted dataset %s: %w: %s", dataset, err, out)
		}
		// Clear legacy quota= alongside refquota= (see idempotent-reuse path above for rationale).
		if out, err := exec.CommandContext(ctx, "zfs", "set", "refquota="+quota, "quota=none", dataset).CombinedOutput(); err != nil {
			return "", false, fmt.Errorf("zfs set refquota and clear legacy quota on remounted %s: %w: %s", dataset, err, out)
		}
		z.logger.Info("remounted existing zfs dataset", "dataset", dataset, "mountpoint", mountpoint, "quota_mb", sizeMB)
		return mountpoint, false, nil
	}

	if out, err := exec.CommandContext(ctx, "zfs", "create", "-o", "refquota="+quota, dataset).CombinedOutput(); err != nil {
		// Cleanup partially created dataset (zfs create is not atomic with quota).
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if cleanupOut, cleanupErr := exec.CommandContext(cleanupCtx, "zfs", "destroy", "-f", dataset).CombinedOutput(); cleanupErr != nil {
			z.logger.Warn("failed to cleanup zfs dataset after create failure", "dataset", dataset, "error", cleanupErr, "output", string(cleanupOut))
		}
		return "", false, fmt.Errorf("zfs create %s (refquota=%s): %w: %s", dataset, quota, err, out)
	}

	// The dataset mountpoint is the child of the parent mountpoint.
	// Verify it actually exists — a non-default mountpoint property or
	// canmount=noauto would cause data to land on the parent filesystem
	// without quota enforcement.
	if _, err := os.Stat(mountpoint); err != nil {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if cleanupOut, cleanupErr := exec.CommandContext(cleanupCtx, "zfs", "destroy", "-f", dataset).CombinedOutput(); cleanupErr != nil {
			z.logger.Warn("failed to cleanup zfs dataset after mountpoint check failure", "dataset", dataset, "error", cleanupErr, "output", string(cleanupOut))
		}
		return "", false, fmt.Errorf("zfs dataset %s created but mountpoint %s not found: %w", dataset, mountpoint, err)
	}

	z.logger.Debug("created zfs dataset", "dataset", dataset, "mountpoint", mountpoint, "quota_mb", sizeMB)
	return mountpoint, true, nil
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

func (z *zfsVolumeManager) List() ([]string, error) {
	return listVolumeIDs(z.dataPath)
}

// datasetExists reports whether the named child dataset exists under the
// parent dataset. Used by RenameVolume for idempotency checks. A
// non-existent dataset returns (false, nil) — only command failures
// surface as errors.
func (z *zfsVolumeManager) datasetExists(ctx context.Context, dataset string) (bool, error) {
	out, err := exec.CommandContext(ctx, "zfs", "list", "-H", "-o", "name", dataset).CombinedOutput()
	if err != nil {
		// Treat "dataset does not exist" as a non-error; anything else is real.
		if strings.Contains(string(out), "does not exist") || strings.Contains(string(out), "no datasets available") {
			return false, nil
		}
		return false, fmt.Errorf("zfs list %s: %w: %s", dataset, err, out)
	}
	return strings.TrimSpace(string(out)) == dataset, nil
}

// RenameVolume issues a `zfs rename` for the underlying dataset. zfs
// treats rename as a metadata-only operation: data blocks, snapshots,
// quota, and the dataset's mountpoint inheritance all move atomically.
//
// Idempotency mirrors atomicRenameVolumeDir's semantics — if the old
// dataset is gone and the new one exists, the rename was already done.
func (z *zfsVolumeManager) RenameVolume(oldName, newName string) error {
	oldDataset := z.parentDataset + "/" + oldName
	newDataset := z.parentDataset + "/" + newName

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	oldExists, err := z.datasetExists(ctx, oldDataset)
	if err != nil {
		return fmt.Errorf("check old zfs dataset %s: %w", oldDataset, err)
	}
	newExists, err := z.datasetExists(ctx, newDataset)
	if err != nil {
		return fmt.Errorf("check new zfs dataset %s: %w", newDataset, err)
	}
	switch {
	case !oldExists && newExists:
		return nil // idempotent — previous run already renamed
	case oldExists && newExists:
		return fmt.Errorf("both old (%s) and new (%s) zfs datasets exist; manual intervention required", oldDataset, newDataset)
	case !oldExists && !newExists:
		return fmt.Errorf("neither old (%s) nor new (%s) zfs dataset exists", oldDataset, newDataset)
	}

	out, err := exec.CommandContext(ctx, "zfs", "rename", oldDataset, newDataset).CombinedOutput()
	if err != nil {
		return fmt.Errorf("zfs rename %s → %s: %w: %s", oldDataset, newDataset, err, out)
	}
	z.logger.Debug("renamed zfs dataset", "old", oldDataset, "new", newDataset)
	return nil
}

// HostPath returns the conventional mountpoint for a volume under the
// data path. Production-deployed ZFS dataset trees use default mountpoint
// inheritance, so this matches the actual mountpoint zfs creates. If an
// operator overrode the mountpoint property to a non-default location,
// migrations would need to consult `zfs get mountpoint` instead — out
// of scope for now.
func (z *zfsVolumeManager) HostPath(name string) string {
	return filepath.Join(z.dataPath, name)
}

// Kind identifies the zfs backend.
func (z *zfsVolumeManager) Kind() string { return "zfs" }

// Usage returns the dataset's referenced bytes — its own data footprint,
// excluding snapshots and descendant datasets. refquota is enforced against
// exactly this value. `-Hp` yields tab-stripped exact bytes.
func (z *zfsVolumeManager) Usage(ctx context.Context, id string) (int64, error) {
	dataset := z.parentDataset + "/" + id
	out, err := exec.CommandContext(ctx, "zfs", "get", "-Hp", "-o", "value", "referenced", dataset).CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("zfs get referenced %s: %w: %s", dataset, err, out)
	}
	return parseZfsReferenced(string(out))
}

// parseZfsReferenced parses the exact-byte `referenced` value from
// `zfs get -Hp -o value referenced`.
func parseZfsReferenced(out string) (int64, error) {
	v, err := strconv.ParseInt(strings.TrimSpace(out), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse zfs referenced %q: %w", strings.TrimSpace(out), err)
	}
	return v, nil
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
