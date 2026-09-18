package docker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// btrfsVolumeManager creates btrfs subvolumes with qgroup quotas.
type btrfsVolumeManager struct {
	dataPath string
	logger   *slog.Logger
	// rootWatch refuses to report an emptiness it cannot vouch for (ENG-687).
	// Zero value is ready to use: it has simply not seen this root hold volumes yet.
	rootWatch volumeRootWatch
}

func (b *btrfsVolumeManager) PinIdentityRoot() error { return b.rootWatch.pin(b.dataPath) }

func (b *btrfsVolumeManager) VerifyIdentityRoot() error { return b.rootWatch.verify(b.dataPath) }

func (b *btrfsVolumeManager) Create(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
	volumeID, err := parseManagedVolumeName(id)
	if err != nil {
		return "", false, fmt.Errorf("validate btrfs volume ID for create: %w", err)
	}
	subvolPath := volumeID.hostPath(b.dataPath)
	quota := fmt.Sprintf("%dm", sizeMB)
	root, err := os.OpenRoot(b.dataPath)
	if err != nil {
		return "", false, fmt.Errorf("open btrfs volume root %s: %w", b.dataPath, err)
	}
	defer func() { _ = root.Close() }()

	// Idempotent: if subvolume already exists, update quota and return.
	exists, statErr := managedDirectoryExistsAtRoot(root, volumeID)
	if statErr == nil && exists {
		if err := b.AttestManagedVolume(ctx, volumeID); err != nil {
			return "", false, fmt.Errorf("attest existing btrfs subvolume %s: %w", subvolPath, err)
		}
		if out, err := exec.CommandContext(ctx, "btrfs", "qgroup", "limit", quota, subvolPath).CombinedOutput(); err != nil {
			return "", false, fmt.Errorf("btrfs qgroup limit %s on existing %s: %w: %s", quota, subvolPath, err, out)
		}
		b.logger.Debug("reusing existing btrfs subvolume", "path", subvolPath, "quota_mb", sizeMB)
		return subvolPath, false, nil
	}
	if statErr != nil {
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
		cleanupCtx, cleanupCancel := newVolumeCleanupContext(ctx)
		defer cleanupCancel()
		if cleanupOut, cleanupErr := exec.CommandContext(cleanupCtx, "btrfs", "subvolume", "delete", subvolPath).CombinedOutput(); cleanupErr != nil {
			b.logger.Warn("failed to cleanup subvolume after quota failure", "path", subvolPath, "error", cleanupErr, "output", string(cleanupOut))
		}
		return "", false, fmt.Errorf("btrfs qgroup limit %s on %s: %w: %s", quota, subvolPath, err, out)
	}

	b.logger.Debug("created btrfs subvolume", "path", subvolPath, "quota_mb", sizeMB)
	return subvolPath, true, nil
}

// EnsureQuota re-applies the qgroup limit to an existing subvolume (a btrfs
// subvolume's quota is inherent to it, so there is no separate "tag" step).
// No-op if the subvolume is absent (never creates). See ENG-454.
func (b *btrfsVolumeManager) EnsureQuota(ctx context.Context, id string, sizeMB int64) error {
	volumeID, err := parseManagedVolumeName(id)
	if err != nil {
		return fmt.Errorf("validate btrfs volume ID for quota: %w", err)
	}
	subvolPath := volumeID.hostPath(b.dataPath)
	root, err := os.OpenRoot(b.dataPath)
	if err != nil {
		return fmt.Errorf("open btrfs volume root %s: %w", b.dataPath, err)
	}
	defer func() { _ = root.Close() }()
	exists, err := managedDirectoryExistsAtRoot(root, volumeID)
	if err != nil {
		return fmt.Errorf("stat subvolume %s: %w", subvolPath, err)
	}
	if !exists {
		return nil
	}
	if err := b.AttestManagedVolume(ctx, volumeID); err != nil {
		return fmt.Errorf("attest existing btrfs subvolume %s before quota: %w", subvolPath, err)
	}
	quota := fmt.Sprintf("%dm", sizeMB)
	if out, err := exec.CommandContext(ctx, "btrfs", "qgroup", "limit", quota, subvolPath).CombinedOutput(); err != nil {
		return fmt.Errorf("btrfs qgroup limit %s on %s: %w: %s", quota, subvolPath, err, out)
	}
	return nil
}

func (b *btrfsVolumeManager) Destroy(ctx context.Context, id string) error {
	volumeID, err := parseManagedVolumeName(id)
	if err != nil {
		return fmt.Errorf("validate btrfs volume ID for destroy: %w", err)
	}
	subvolPath := volumeID.hostPath(b.dataPath)
	root, err := os.OpenRoot(b.dataPath)
	if err != nil {
		return fmt.Errorf("open btrfs volume root %s: %w", b.dataPath, err)
	}
	defer func() { _ = root.Close() }()
	exists, err := managedDirectoryExistsAtRoot(root, volumeID)
	if err != nil {
		return fmt.Errorf("stat subvolume %s: %w", subvolPath, err)
	}
	if !exists {
		return nil
	}
	if err := b.AttestManagedVolume(ctx, volumeID); err != nil {
		return fmt.Errorf("attest btrfs subvolume %s before destroy: %w", subvolPath, err)
	}

	out, err := exec.CommandContext(ctx, "btrfs", "subvolume", "delete", subvolPath).CombinedOutput()
	if err != nil {
		outStr := string(out)
		// Check if the subvolume doesn't exist (idempotent).
		// Match specific error strings rather than exit codes to avoid
		// swallowing permission, busy, or I/O errors.
		if strings.Contains(outStr, "cannot find") ||
			strings.Contains(outStr, "No such file or directory") {
			b.logger.Debug("btrfs subvolume does not exist (idempotent)", "path", subvolPath)
			return nil
		}
		return fmt.Errorf("btrfs subvolume delete %s: %w: %s", subvolPath, err, outStr)
	}

	b.logger.Debug("destroyed btrfs subvolume", "path", subvolPath)
	return nil
}

func (b *btrfsVolumeManager) List() ([]string, error) {
	return b.rootWatch.list(b.dataPath)
}

func (b *btrfsVolumeManager) ListForProof(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.rootWatch.listForProof(b.dataPath)
}

// AttestManagedVolume proves that the exact managed directory is a Btrfs
// subvolume root. A successful directory stat alone is insufficient: a plain
// directory would place tenant writes on the parent filesystem without the
// subvolume qgroup identity the quota boundary depends on.
func (b *btrfsVolumeManager) AttestManagedVolume(ctx context.Context, name managedVolumeName) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := os.OpenRoot(b.dataPath)
	if err != nil {
		return fmt.Errorf("open btrfs volume root %s: %w", b.dataPath, err)
	}
	defer func() { _ = root.Close() }()
	exists, err := managedDirectoryExistsAtRoot(root, name)
	if err != nil {
		return fmt.Errorf("stat btrfs volume %s: %w", name.hostPath(b.dataPath), err)
	}
	if !exists {
		return fmt.Errorf("btrfs volume %s does not exist", name.hostPath(b.dataPath))
	}
	out, err := exec.CommandContext(ctx, "btrfs", "subvolume", "show", name.hostPath(b.dataPath)).CombinedOutput()
	if err != nil {
		return fmt.Errorf("btrfs subvolume show %s: %w: %s", name.hostPath(b.dataPath), err, out)
	}
	if _, err := parseBtrfsSubvolumeID(string(out)); err != nil {
		return fmt.Errorf("validate btrfs subvolume identity for %s: %w", name.hostPath(b.dataPath), err)
	}
	return nil
}

// Btrfs publishes the subvolume name before applying its qgroup limit, but a
// crash in that window leaves an ordinary, structurally attested managed
// subvolume. Durable operation recovery classifies it and the fatal startup
// quota gate repairs every claimed live/retained volume before readiness;
// unclaimed subvolumes are collected as orphans. There is no private create
// namespace to reject or recover here.
func (b *btrfsVolumeManager) RequireNoInterruptedVolumeMutations(context.Context) error { return nil }

func (b *btrfsVolumeManager) RecoverInterruptedVolumeMutations(context.Context) error { return nil }

// RenameVolume renames a btrfs subvolume root through the descriptor-rooted
// VFS rename path. The btrfs kernel module treats a subvolume root as a
// directory for rename purposes — the underlying subvolume identity
// (subvol-id), data, and qgroup attachment are preserved across the rename.
// No btrfs CLI call is needed.
func (b *btrfsVolumeManager) RenameVolume(ctx context.Context, oldName, newName string) error {
	oldVolume, err := parseManagedVolumeName(oldName)
	if err != nil {
		return fmt.Errorf("validate old btrfs volume ID for rename: %w", err)
	}
	newVolume, err := parseManagedVolumeName(newName)
	if err != nil {
		return fmt.Errorf("validate new btrfs volume ID for rename: %w", err)
	}
	root, err := os.OpenRoot(b.dataPath)
	if err != nil {
		return fmt.Errorf("open btrfs volume root %s: %w", b.dataPath, err)
	}
	oldExists, oldStatErr := managedDirectoryExistsAtRoot(root, oldVolume)
	newExists, newStatErr := managedDirectoryExistsAtRoot(root, newVolume)
	_ = root.Close()
	if oldStatErr != nil {
		return fmt.Errorf("stat old btrfs subvolume %s: %w", oldVolume.hostPath(b.dataPath), oldStatErr)
	}
	if newStatErr != nil {
		return fmt.Errorf("stat new btrfs subvolume %s: %w", newVolume.hostPath(b.dataPath), newStatErr)
	}
	if oldExists {
		if err := b.AttestManagedVolume(ctx, oldVolume); err != nil {
			return fmt.Errorf("attest old btrfs subvolume before rename: %w", err)
		}
	}
	if newExists {
		if err := b.AttestManagedVolume(ctx, newVolume); err != nil {
			return fmt.Errorf("attest new btrfs subvolume before rename: %w", err)
		}
	}
	if err := renameAtStorageRoot(ctx, b.dataPath, oldVolume, newVolume); err != nil {
		return err
	}
	if err := b.AttestManagedVolume(ctx, newVolume); err != nil {
		return fmt.Errorf("attest renamed btrfs subvolume: %w", err)
	}
	return nil
}

// HostPath returns the absolute path of the subvolume under the
// configured data path. The subvolume may or may not exist; callers use
// this to compute paths for not-yet-renamed or about-to-be-created
// volumes.
func (b *btrfsVolumeManager) HostPath(name string) string {
	volumeID, err := parseManagedVolumeName(name)
	if err != nil {
		return rejectedManagedVolumeHostPath(b.dataPath, name)
	}
	return volumeID.hostPath(b.dataPath)
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
	volumeID, err := parseManagedVolumeName(id)
	if err != nil {
		return 0, fmt.Errorf("validate btrfs volume ID for usage: %w", err)
	}
	subvolPath := volumeID.hostPath(b.dataPath)
	root, err := os.OpenRoot(b.dataPath)
	if err != nil {
		return 0, fmt.Errorf("open btrfs volume root %s: %w", b.dataPath, err)
	}
	defer func() { _ = root.Close() }()
	exists, err := managedDirectoryExistsAtRoot(root, volumeID)
	if err != nil {
		return 0, fmt.Errorf("stat subvolume %s: %w", subvolPath, err)
	}
	if !exists {
		return 0, fmt.Errorf("subvolume %s does not exist", subvolPath)
	}
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

	// `btrfs subvolume create` and `btrfs qgroup limit` are privileged ioctls
	// requiring CAP_SYS_ADMIN (no delegation alternative, unlike zfs). The
	// qgroup show read above succeeds without it, so fail fast at startup rather
	// than rejecting every provision at runtime (ENG-454).
	if err := requireCapSysAdmin(b.Kind(), b.logger); err != nil {
		return err
	}

	return nil
}
