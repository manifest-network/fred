package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// zfsVolumeManager creates ZFS child datasets with quotas.
type zfsVolumeManager struct {
	dataPath      string
	parentDataset string // cached during Validate()
	logger        *slog.Logger
	// rootWatch refuses to report an emptiness it cannot vouch for (ENG-687).
	rootWatch volumeRootWatch
}

func (z *zfsVolumeManager) PinIdentityRoot() error { return z.rootWatch.pin(z.dataPath) }

func (z *zfsVolumeManager) VerifyIdentityRoot() error { return z.rootWatch.verify(z.dataPath) }

func (z *zfsVolumeManager) volumeDataset(name managedVolumeName) (string, error) {
	parent := strings.TrimSpace(z.parentDataset)
	if parent == "" || parent != z.parentDataset || strings.ContainsAny(parent, "\x00\r\n") {
		return "", fmt.Errorf("invalid resolved zfs parent dataset %q", z.parentDataset)
	}
	return parent + "/" + name.value(), nil
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
	name, err := parseManagedVolumeName(id)
	if err != nil {
		return "", false, err
	}
	dataset, err := z.volumeDataset(name)
	if err != nil {
		return "", false, err
	}
	mountpoint := name.hostPath(z.dataPath)
	quota := fmt.Sprintf("%dM", sizeMB)
	root, err := os.OpenRoot(z.dataPath)
	if err != nil {
		return "", false, fmt.Errorf("open zfs volume root %s: %w", z.dataPath, err)
	}
	defer func() { _ = root.Close() }()

	// Idempotent: if dataset already exists (mountpoint present), update quota and return.
	exists, statErr := managedDirectoryExistsAtRoot(root, name)
	if statErr != nil {
		return "", false, fmt.Errorf("stat mountpoint %s: %w", mountpoint, statErr)
	}
	if exists {
		mounted, attestErr := z.attestManagedDatasetState(ctx, name)
		if attestErr != nil {
			return "", false, fmt.Errorf("attest existing zfs volume %s: %w", dataset, attestErr)
		}
		if mounted {
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
	}

	// Mountpoint absent — check if the dataset exists but is unmounted
	// (e.g., after a pool import anomaly or manual zfs unmount). If so,
	// mount it and update quota rather than creating a new dataset.
	datasetPresent, err := z.datasetExists(ctx, name)
	if err != nil {
		return "", false, fmt.Errorf("check for existing unmounted zfs dataset %s: %w", dataset, err)
	}
	if datasetPresent {
		if out, err := exec.CommandContext(ctx, "zfs", "mount", dataset).CombinedOutput(); err != nil {
			return "", false, fmt.Errorf("zfs mount existing unmounted dataset %s: %w: %s", dataset, err, out)
		}
		mounted, mountErr := managedDirectoryExistsAtRoot(root, name)
		if mountErr != nil {
			return "", false, fmt.Errorf("verify remounted zfs dataset %s at %s: %w", dataset, mountpoint, mountErr)
		}
		if !mounted {
			// This was an existing dataset, so never destroy it on a mountpoint
			// mismatch. An overridden mountpoint or canmount behavior needs an
			// operator; returning the expected-but-absent bind source would send
			// tenant writes to the unquotaed parent filesystem.
			return "", false, fmt.Errorf("remounted zfs dataset %s did not appear at managed mountpoint %s", dataset, mountpoint)
		}
		if err := z.requireMountedManagedVolume(ctx, name); err != nil {
			return "", false, fmt.Errorf("attest remounted zfs volume %s: %w", dataset, err)
		}
		// Clear legacy quota= alongside refquota= (see idempotent-reuse path above for rationale).
		if out, err := exec.CommandContext(ctx, "zfs", "set", "refquota="+quota, "quota=none", dataset).CombinedOutput(); err != nil {
			return "", false, fmt.Errorf("zfs set refquota and clear legacy quota on remounted %s: %w: %s", dataset, err, out)
		}
		z.logger.Info("remounted existing zfs dataset", "dataset", dataset, "mountpoint", mountpoint, "quota_mb", sizeMB)
		return mountpoint, false, nil
	}

	if out, err := exec.CommandContext(ctx, "zfs", "create", "-o", "refquota="+quota, dataset).CombinedOutput(); err != nil {
		// A failed command is not proof that the dataset was never created: the
		// process can observe cancellation after ZFS committed, and another actor
		// can win the list/create race. Preserve the exact dataset for retry rather
		// than turning an ambiguous create into an unauthorized destroy.
		return "", false, fmt.Errorf("zfs create %s (refquota=%s): %w: %s", dataset, quota, err, out)
	}

	// The dataset mountpoint is the child of the parent mountpoint.
	// Verify it actually exists — a non-default mountpoint property or
	// canmount=noauto would cause data to land on the parent filesystem
	// without quota enforcement.
	mounted, statErr := managedDirectoryExistsAtRoot(root, name)
	if statErr == nil && mounted {
		statErr = z.requireMountedManagedVolume(ctx, name)
	}
	if statErr != nil || !mounted {
		if statErr == nil {
			statErr = fmt.Errorf("expected mountpoint is absent")
		}
		cleanupCtx, cleanupCancel := newVolumeCleanupContext(ctx)
		defer cleanupCancel()
		if cleanupOut, cleanupErr := exec.CommandContext(cleanupCtx, "zfs", "destroy", "-f", dataset).CombinedOutput(); cleanupErr != nil {
			z.logger.Warn("failed to cleanup zfs dataset after mountpoint check failure", "dataset", dataset, "error", cleanupErr, "output", string(cleanupOut))
		}
		return "", false, fmt.Errorf("zfs dataset %s created but mountpoint %s is invalid: %w", dataset, mountpoint, statErr)
	}

	z.logger.Debug("created zfs dataset", "dataset", dataset, "mountpoint", mountpoint, "quota_mb", sizeMB)
	return mountpoint, true, nil
}

// EnsureQuota re-applies refquota (and clears any legacy quota=) to an existing
// dataset. No-op if the dataset is absent (never creates). ZFS quota is inherent
// to the dataset, so there is no separate "tag" step. See ENG-454.
func (z *zfsVolumeManager) EnsureQuota(ctx context.Context, id string, sizeMB int64) error {
	name, err := parseManagedVolumeName(id)
	if err != nil {
		return err
	}
	dataset, err := z.volumeDataset(name)
	if err != nil {
		return err
	}
	exists, err := z.datasetExists(ctx, name)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if err := z.requireMountedManagedVolume(ctx, name); err != nil {
		return fmt.Errorf("attest existing zfs volume %s before quota: %w", dataset, err)
	}
	quota := fmt.Sprintf("%dM", sizeMB)
	if out, err := exec.CommandContext(ctx, "zfs", "set", "refquota="+quota, "quota=none", dataset).CombinedOutput(); err != nil {
		return fmt.Errorf("zfs set refquota on %s: %w: %s", dataset, err, out)
	}
	return nil
}

func (z *zfsVolumeManager) Destroy(ctx context.Context, id string) error {
	name, err := parseManagedVolumeName(id)
	if err != nil {
		return err
	}
	dataset, err := z.volumeDataset(name)
	if err != nil {
		return err
	}
	exists, err := z.datasetExists(ctx, name)
	if err != nil {
		return fmt.Errorf("check zfs dataset %s before destroy: %w", dataset, err)
	}
	if !exists {
		return nil
	}
	if err := z.AttestManagedVolume(ctx, name); err != nil {
		return fmt.Errorf("attest zfs dataset %s before destroy: %w", dataset, err)
	}

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

const zfsVolumeInventoryTimeout = 10 * time.Second

func (z *zfsVolumeManager) List() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), zfsVolumeInventoryTimeout)
	defer cancel()
	return z.ListForProof(ctx)
}

// ListForProof unions the directory view with exact depth-one ZFS children.
// The union is load-bearing: an unmounted dataset or one mounted elsewhere has
// no directory under dataPath, while a plain same-named directory has no child
// dataset. Returning either side lets the per-volume attester reject the
// mismatch instead of silently omitting it from storage-identity evidence.
func (z *zfsVolumeManager) ListForProof(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	directoryNames, err := z.rootWatch.listForProof(z.dataPath)
	if err != nil {
		return nil, err
	}
	datasetNames, err := z.listManagedChildDatasets(ctx)
	if err != nil {
		return nil, err
	}
	union := make(map[string]struct{}, len(directoryNames)+len(datasetNames))
	for _, name := range directoryNames {
		union[name] = struct{}{}
	}
	for _, name := range datasetNames {
		union[name] = struct{}{}
	}
	result := make([]string, 0, len(union))
	for name := range union {
		result = append(result, name)
	}
	slices.Sort(result)
	return result, nil
}

func (z *zfsVolumeManager) listManagedChildDatasets(ctx context.Context) ([]string, error) {
	parent := strings.TrimSpace(z.parentDataset)
	if parent == "" || parent != z.parentDataset || strings.ContainsAny(parent, "\x00\r\n") {
		return nil, fmt.Errorf("invalid resolved zfs parent dataset %q", z.parentDataset)
	}
	out, err := exec.CommandContext(
		ctx,
		"zfs", "list", "-H", "-r", "-d", "1", "-o", "name", parent,
	).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("enumerate zfs child datasets under %s: %w: %s", parent, err, out)
	}
	trimmed := strings.TrimRight(string(out), "\r\n")
	if trimmed == "" {
		return nil, fmt.Errorf("zfs child dataset inventory for %s is empty", parent)
	}
	prefix := parent + "/"
	seenParent := false
	seenChildren := make(map[string]struct{})
	var names []string
	for _, line := range strings.Split(trimmed, "\n") {
		if strings.ContainsRune(line, '\r') {
			return nil, fmt.Errorf("zfs child dataset inventory for %s contains an invalid row %q", parent, line)
		}
		if line == parent {
			if seenParent {
				return nil, fmt.Errorf("zfs child dataset inventory contains duplicate parent %q", parent)
			}
			seenParent = true
			continue
		}
		if !strings.HasPrefix(line, prefix) {
			return nil, fmt.Errorf("zfs child dataset inventory for %s returned foreign dataset %q", parent, line)
		}
		child := strings.TrimPrefix(line, prefix)
		if child == "" || strings.ContainsRune(child, '/') {
			return nil, fmt.Errorf("zfs depth-one inventory for %s returned invalid child %q", parent, line)
		}
		if _, duplicate := seenChildren[child]; duplicate {
			return nil, fmt.Errorf("zfs child dataset inventory contains duplicate child %q", line)
		}
		seenChildren[child] = struct{}{}
		if strings.HasPrefix(child, volumePrefix) {
			names = append(names, child)
		}
	}
	if !seenParent {
		return nil, fmt.Errorf("zfs child dataset inventory omitted parent %q", parent)
	}
	return names, nil
}

// AttestManagedVolume proves the deletion/identity authority for a ZFS-backed
// volume: the exact depth-one child exists and its immutable mountpoint
// authority names the exact managed host path. An exact but currently
// unmounted child is structurally valid interrupted-create evidence, not a
// bind-ready volume; RequireNoInterruptedVolumeMutations rejects it at publication and
// readiness boundaries, while RecoverInterruptedVolumeMutations remounts it after the
// identity-bound stores are exclusively open.
func (z *zfsVolumeManager) AttestManagedVolume(ctx context.Context, name managedVolumeName) error {
	_, err := z.attestManagedDatasetState(ctx, name)
	return err
}

func (z *zfsVolumeManager) requireMountedManagedVolume(
	ctx context.Context,
	name managedVolumeName,
) error {
	mounted, err := z.attestManagedDatasetState(ctx, name)
	if err != nil {
		return err
	}
	if !mounted {
		dataset, datasetErr := z.volumeDataset(name)
		if datasetErr != nil {
			return datasetErr
		}
		return fmt.Errorf("zfs dataset %s is not mounted", dataset)
	}
	return nil
}

func (z *zfsVolumeManager) attestManagedDatasetState(
	ctx context.Context,
	name managedVolumeName,
) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	mounted, err := z.managedDatasetMountState(ctx, name)
	if err != nil {
		return false, err
	}
	root, err := os.OpenRoot(z.dataPath)
	if err != nil {
		return false, fmt.Errorf("open zfs volume root %s: %w", z.dataPath, err)
	}
	defer func() { _ = root.Close() }()
	exists, err := managedDirectoryExistsAtRoot(root, name)
	if err != nil {
		return false, fmt.Errorf("stat zfs mountpoint %s: %w", name.hostPath(z.dataPath), err)
	}
	if mounted && !exists {
		return false, fmt.Errorf("zfs mountpoint %s does not exist", name.hostPath(z.dataPath))
	}
	if !mounted && exists {
		volumeRoot, openErr := openAttestedManagedVolumeRoot(root, name)
		if openErr != nil {
			return false, fmt.Errorf("open unmounted zfs mountpoint stub: %w", openErr)
		}
		directory, openErr := volumeRoot.Open(".")
		if openErr != nil {
			_ = volumeRoot.Close()
			return false, fmt.Errorf("open unmounted zfs mountpoint stub for enumeration: %w", openErr)
		}
		entries, readErr := directory.ReadDir(1)
		closeErr := errors.Join(directory.Close(), volumeRoot.Close())
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return false, fmt.Errorf("enumerate unmounted zfs mountpoint stub: %w", readErr)
		}
		if closeErr != nil {
			return false, fmt.Errorf("close unmounted zfs mountpoint stub: %w", closeErr)
		}
		if len(entries) != 0 {
			return false, fmt.Errorf(
				"unmounted zfs dataset for %s would hide existing data at its managed mountpoint",
				name.value(),
			)
		}
	}
	return mounted, nil
}

func (z *zfsVolumeManager) managedDatasetMountState(
	ctx context.Context,
	name managedVolumeName,
) (bool, error) {
	dataset, err := z.volumeDataset(name)
	if err != nil {
		return false, err
	}
	out, err := exec.CommandContext(
		ctx,
		"zfs", "list", "-H", "-p", "-o", "name,mounted,mountpoint", dataset,
	).CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("zfs list substrate for %s: %w: %s", dataset, err, out)
	}
	return parseZFSManagedDatasetState(string(out), dataset, name.hostPath(z.dataPath))
}

func parseZFSManagedDatasetState(out, expectedDataset, expectedMountpoint string) (bool, error) {
	line := strings.TrimRight(out, "\r\n")
	if line == "" || strings.ContainsAny(line, "\r\n") {
		return false, fmt.Errorf("zfs substrate inventory for %s returned an invalid row %q", expectedDataset, line)
	}
	fields := strings.Split(line, "\t")
	if len(fields) != 3 {
		return false, fmt.Errorf("zfs substrate inventory for %s returned %d fields, expected 3", expectedDataset, len(fields))
	}
	if fields[0] != expectedDataset {
		return false, fmt.Errorf("zfs substrate inventory for %s returned unexpected dataset %q", expectedDataset, fields[0])
	}
	if fields[2] != expectedMountpoint || filepath.Clean(fields[2]) != fields[2] {
		return false, fmt.Errorf(
			"zfs dataset %s mountpoint %q does not equal exact managed path %q",
			expectedDataset,
			fields[2],
			expectedMountpoint,
		)
	}
	switch fields[1] {
	case "yes":
		return true, nil
	case "no":
		return false, nil
	default:
		return false, fmt.Errorf("zfs dataset %s returned invalid mounted state %q", expectedDataset, fields[1])
	}
}

func parseZFSDatasetAttestation(out, expectedDataset, expectedMountpoint string) error {
	mounted, err := parseZFSManagedDatasetState(out, expectedDataset, expectedMountpoint)
	if err != nil {
		return err
	}
	if !mounted {
		return fmt.Errorf("zfs dataset %s is not mounted", expectedDataset)
	}
	return nil
}

func (z *zfsVolumeManager) RequireNoInterruptedVolumeMutations(ctx context.Context) error {
	children, err := z.listManagedChildDatasets(ctx)
	if err != nil {
		return err
	}
	for _, child := range children {
		name, parseErr := parseManagedVolumeName(child)
		if parseErr != nil {
			return fmt.Errorf("validate zfs child %q while checking interrupted creates: %w", child, parseErr)
		}
		mounted, attestErr := z.attestManagedDatasetState(ctx, name)
		if attestErr != nil {
			return fmt.Errorf("attest zfs child %q while checking interrupted creates: %w", child, attestErr)
		}
		if !mounted {
			return fmt.Errorf("zfs child dataset %q is an interrupted unmounted create", child)
		}
	}
	return nil
}

// RecoverInterruptedVolumeMutations mounts exact unmounted managed children before
// ordinary operation-intent recovery. The dataset already carries both its
// typed final identity and exact mountpoint property, so mounting publishes no
// guessed quota or tenant identity. A failed/ambiguous mount is re-read; only a
// fully attested mounted result counts as success, and no dataset is destroyed.
func (z *zfsVolumeManager) RecoverInterruptedVolumeMutations(ctx context.Context) error {
	children, err := z.listManagedChildDatasets(ctx)
	if err != nil {
		return err
	}
	var recoveryErrs []error
	for _, child := range children {
		if err := ctx.Err(); err != nil {
			recoveryErrs = append(recoveryErrs, err)
			break
		}
		name, parseErr := parseManagedVolumeName(child)
		if parseErr != nil {
			recoveryErrs = append(recoveryErrs, fmt.Errorf(
				"validate zfs child %q during interrupted-create recovery: %w", child, parseErr,
			))
			continue
		}
		mounted, attestErr := z.attestManagedDatasetState(ctx, name)
		if attestErr != nil {
			recoveryErrs = append(recoveryErrs, fmt.Errorf(
				"attest zfs child %q during interrupted-create recovery: %w", child, attestErr,
			))
			continue
		}
		if mounted {
			continue
		}
		dataset, datasetErr := z.volumeDataset(name)
		if datasetErr != nil {
			recoveryErrs = append(recoveryErrs, datasetErr)
			continue
		}
		mountOut, mountErr := exec.CommandContext(ctx, "zfs", "mount", dataset).CombinedOutput()
		// The command can commit and then lose its acknowledgement. Re-attest the
		// exact dataset and mountpoint before classifying the result.
		confirmCtx, confirmCancel := newVolumeCleanupContext(ctx)
		mounted, attestErr = z.attestManagedDatasetState(confirmCtx, name)
		confirmCancel()
		if attestErr == nil && !mounted {
			attestErr = fmt.Errorf("dataset remains unmounted")
		}
		if attestErr != nil {
			observedErr := mountErr
			if output := strings.TrimSpace(string(mountOut)); output != "" {
				observedErr = errors.Join(observedErr, fmt.Errorf("zfs mount output: %s", output))
			}
			recoveryErrs = append(recoveryErrs, fmt.Errorf(
				"recover interrupted zfs create %s: %w",
				dataset,
				errors.Join(observedErr, attestErr),
			))
			continue
		}
		z.logger.Info("recovered interrupted zfs volume mount", "dataset", dataset)
	}
	return errors.Join(recoveryErrs...)
}

// datasetExists reports whether the named child dataset exists under the
// parent dataset. Used by RenameVolume for idempotency checks. A
// non-existent dataset returns (false, nil) — only command failures
// surface as errors.
func (z *zfsVolumeManager) datasetExists(ctx context.Context, name managedVolumeName) (bool, error) {
	dataset, err := z.volumeDataset(name)
	if err != nil {
		return false, err
	}
	out, err := exec.CommandContext(ctx, "zfs", "list", "-H", "-o", "name", dataset).CombinedOutput()
	if err != nil {
		// Treat "dataset does not exist" as a non-error; anything else is real.
		if strings.Contains(string(out), "does not exist") || strings.Contains(string(out), "no datasets available") {
			return false, nil
		}
		return false, fmt.Errorf("zfs list %s: %w: %s", dataset, err, out)
	}
	observed := strings.TrimSpace(string(out))
	if observed != dataset {
		return false, fmt.Errorf("zfs list for %s returned unexpected dataset identity %q", dataset, observed)
	}
	return true, nil
}

// RenameVolume issues a `zfs rename` for the underlying dataset. zfs
// treats rename as a metadata-only operation: data blocks, snapshots,
// quota, and the dataset's mountpoint inheritance all move atomically.
//
// Idempotency mirrors the filesystem managers' rename semantics — if the old
// dataset is gone and the new one exists, the rename was already done.
func (z *zfsVolumeManager) RenameVolume(ctx context.Context, oldName, newName string) error {
	oldVolume, err := parseManagedVolumeName(oldName)
	if err != nil {
		return fmt.Errorf("old volume: %w", err)
	}
	newVolume, err := parseManagedVolumeName(newName)
	if err != nil {
		return fmt.Errorf("new volume: %w", err)
	}
	oldDataset, err := z.volumeDataset(oldVolume)
	if err != nil {
		return err
	}
	newDataset, err := z.volumeDataset(newVolume)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	oldExists, err := z.datasetExists(ctx, oldVolume)
	if err != nil {
		return fmt.Errorf("check old zfs dataset %s: %w", oldDataset, err)
	}
	newExists, err := z.datasetExists(ctx, newVolume)
	if err != nil {
		return fmt.Errorf("check new zfs dataset %s: %w", newDataset, err)
	}
	if oldExists {
		if err := z.requireMountedManagedVolume(ctx, oldVolume); err != nil {
			return fmt.Errorf("attest old zfs dataset before rename: %w", err)
		}
	}
	if newExists {
		if err := z.requireMountedManagedVolume(ctx, newVolume); err != nil {
			return fmt.Errorf("attest new zfs dataset before rename: %w", err)
		}
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
	if err := z.requireMountedManagedVolume(ctx, newVolume); err != nil {
		return fmt.Errorf("attest renamed zfs dataset %s: %w", newDataset, err)
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
	volume, err := parseManagedVolumeName(name)
	if err != nil {
		return rejectedManagedVolumeHostPath(z.dataPath, name)
	}
	return volume.hostPath(z.dataPath)
}

// Kind identifies the zfs backend.
func (z *zfsVolumeManager) Kind() string { return "zfs" }

// Usage returns the dataset's referenced bytes — its own data footprint,
// excluding snapshots and descendant datasets. refquota is enforced against
// exactly this value. `-Hp` yields tab-stripped exact bytes.
func (z *zfsVolumeManager) Usage(ctx context.Context, id string) (int64, error) {
	name, err := parseManagedVolumeName(id)
	if err != nil {
		return 0, err
	}
	dataset, err := z.volumeDataset(name)
	if err != nil {
		return 0, err
	}
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

	// NOTE: deliberately NO CAP_SYS_ADMIN check here (unlike xfs/btrfs, see
	// requireCapSysAdmin). ZFS create/set can be delegated to a non-root user via
	// `zfs allow`, so the daemon may legitimately set quotas without the
	// capability. A cap check would false-positive on a properly-delegated host.
	// zfs privilege failures surface as a create/set error at provision time.
	return nil
}
