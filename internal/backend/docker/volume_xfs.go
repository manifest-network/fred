package docker

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// projectIDFile is the marker file written inside each volume directory
// to record the assigned XFS project ID. This allows the active ID map
// to be rebuilt after a restart and avoids re-deriving via CRC32 (which
// could silently collide with another volume).
const projectIDFile = ".fred-project-id"

// xfsVolumeManager creates directories with XFS project quotas.
type xfsVolumeManager struct {
	dataPath string
	// mountPoint is the XFS mount that contains dataPath, resolved once at
	// construction. xfs_quota requires the mount point (not a subdirectory of
	// it) as its trailing filesystem argument; dataPath is typically a subdir
	// of this mount. See resolveMountpoint and ENG-449.
	mountPoint string
	logger     *slog.Logger

	mu         sync.Mutex
	activeIDs  map[uint32]string // projectID → volumeID
	volumeToID map[string]uint32 // volumeID → projectID (reverse index)
}

// assignProjectID returns a collision-free XFS project ID for volumeID.
// It uses CRC32 as the initial candidate and probes (increments) on
// collision. The caller must NOT hold x.mu.
func (x *xfsVolumeManager) assignProjectID(volumeID string) (uint32, error) {
	candidate := crc32.ChecksumIEEE([]byte(volumeID))

	x.mu.Lock()
	defer x.mu.Unlock()

	// If this volumeID already owns a project ID, return it (idempotent).
	if id, ok := x.volumeToID[volumeID]; ok {
		return id, nil
	}

	// Probe until we find a free slot. Project ID 0 is reserved by XFS,
	// so skip it. Cap iterations to prevent an infinite loop if the
	// ID space is exhausted.
	for range int64(math.MaxUint32) {
		if candidate == 0 {
			candidate++
		}
		if _, taken := x.activeIDs[candidate]; !taken {
			break
		}
		candidate++
	}

	if _, taken := x.activeIDs[candidate]; taken {
		return 0, fmt.Errorf("xfs project ID space exhausted (%d active IDs)", len(x.activeIDs))
	}

	x.activeIDs[candidate] = volumeID
	x.volumeToID[volumeID] = candidate
	return candidate, nil
}

// trackProjectID registers a known projectID → volumeID mapping.
// Used by the idempotent Create path and Validate to populate the maps.
// Cleans up stale entries if the volume was previously tracked with a
// different project ID. The caller must hold x.mu.
func (x *xfsVolumeManager) trackProjectID(volumeID string, projID uint32) {
	// Clean up stale forward entry if the volume previously had a different ID.
	if oldID, ok := x.volumeToID[volumeID]; ok && oldID != projID {
		delete(x.activeIDs, oldID)
	}
	// Clean up stale reverse entry if the project ID was previously owned
	// by a different volume.
	if oldVol, ok := x.activeIDs[projID]; ok && oldVol != volumeID {
		delete(x.volumeToID, oldVol)
	}
	x.activeIDs[projID] = volumeID
	x.volumeToID[volumeID] = projID
}

// resolveProjectID returns the XFS project ID assigned to volumeID, for teardown.
// It prefers the on-disk .fred-project-id marker — the authoritative record of the
// projID that was actually project-tagged and limited, and the only source that
// survives a restart. The in-memory reverse map is used only when the marker is
// genuinely ABSENT (ErrNotExist), e.g. the directory was already removed
// out-of-band; if the marker exists but is unreadable/corrupt the clear is skipped
// rather than guessing a possibly-wrong project. Returns ok=false when nothing
// resolvable is known — an already-cleared or never-created volume: nothing to clear.
//
// Resolution goes through the marker/map, NEVER a recomputed crc32(volumeID):
// assignProjectID's collision-probe means the derived candidate can differ from
// the id actually assigned, so a recompute-based clear could zero the wrong
// project. Project ID 0 (XFS's reserved default project) is never returned, so a
// corrupt "0" marker cannot make Destroy reset the default project's limits. The
// caller must NOT hold x.mu.
func (x *xfsVolumeManager) resolveProjectID(volumeID string) (uint32, bool) {
	dirPath := filepath.Join(x.dataPath, volumeID)
	projID, err := readProjectIDFile(dirPath)
	switch {
	case err == nil:
		return projID, projID != 0
	case errors.Is(err, fs.ErrNotExist):
		x.mu.Lock()
		defer x.mu.Unlock()
		id, ok := x.volumeToID[volumeID]
		return id, ok && id != 0
	default:
		// Marker present but unreadable/corrupt: do not guess from the map — skip
		// the clear rather than risk zeroing a wrong/foreign project. (A genuinely
		// corrupt marker would also have failed Validate's scan at startup.)
		x.logger.Warn("xfs project-id marker unreadable on destroy; skipping quota clear",
			"path", dirPath, "error", err)
		return 0, false
	}
}

// removeProjectID removes the volumeID's entry from the active maps.
func (x *xfsVolumeManager) removeProjectID(volumeID string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if id, ok := x.volumeToID[volumeID]; ok {
		delete(x.activeIDs, id)
		delete(x.volumeToID, volumeID)
	}
}

// writeProjectIDFile persists the project ID as a decimal string inside
// the volume directory.
func writeProjectIDFile(dirPath string, id uint32) error {
	p := filepath.Join(dirPath, projectIDFile)
	return os.WriteFile(p, []byte(strconv.FormatUint(uint64(id), 10)), 0600)
}

// readProjectIDFile reads the project ID back from the marker file.
func readProjectIDFile(dirPath string) (uint32, error) {
	p := filepath.Join(dirPath, projectIDFile)
	data, err := os.ReadFile(p)
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse project ID from %s: %w", p, err)
	}
	return uint32(v), nil
}

// resolveMountpoint returns the mount point of the filesystem that contains
// path, by walking up parent directories until the device number (st_dev)
// changes — the classic mountpoint(1) test.
//
// xfs_quota requires a real mount point as its trailing filesystem argument;
// the configured volume_data_path is typically a *subdirectory* of the XFS
// mount (e.g. /data/fred/volumes under the /data/fred mount), which xfs_quota
// rejects with "cannot setup path for mount ...: No such device or address".
// The `project -s`/`limit -p`/`report -p` commands take the subdirectory in
// their -p argument and the mount point as the filesystem argument.
//
// The subdirectory need not exist yet (Create makes per-volume directories
// lazily), so resolution starts from the nearest existing ancestor — the mount
// that ancestor lives on is the same mount the subdirectory will inherit.
func resolveMountpoint(path string) (string, error) {
	// Resolve to an absolute path first. A relative volume_data_path is allowed
	// by config validation, and would otherwise walk up to "." (not a real mount
	// point) because filepath.Dir(".") == ".".
	p, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve absolute path for %s: %w", path, err)
	}

	// Walk up to the nearest existing ancestor.
	var st syscall.Stat_t
	for {
		if err := syscall.Stat(p, &st); err == nil {
			break
		} else if !errors.Is(err, fs.ErrNotExist) {
			return "", fmt.Errorf("stat %s: %w", p, err)
		}
		parent := filepath.Dir(p)
		if parent == p {
			return "", fmt.Errorf("no existing ancestor for %s", path)
		}
		p = parent
	}

	// Walk up until the device number changes; that boundary is the mount root.
	dev := st.Dev
	for {
		parent := filepath.Dir(p)
		if parent == p {
			return p, nil // reached the filesystem root
		}
		var pst syscall.Stat_t
		if err := syscall.Stat(parent, &pst); err != nil {
			return "", fmt.Errorf("stat %s: %w", parent, err)
		}
		if pst.Dev != dev {
			return p, nil // p's parent is a different filesystem: p is the mount root
		}
		p = parent
	}
}

// xfsQuotaArgs builds the argument vector for an `xfs_quota -x -c <cmd>
// <mountPoint>` invocation. The mount point is ALWAYS the trailing filesystem
// argument (never a subdirectory of it); per-directory commands reference the
// subdirectory inside cmd's -p option instead. See resolveMountpoint / ENG-449.
func xfsQuotaArgs(cmd, mountPoint string) []string {
	return []string{"-x", "-c", cmd, mountPoint}
}

// xfsProjectSetupCmd is the `project -s` command that tags dirPath's inode (and
// its existing children) with projID. dirPath is the volume subdirectory.
func xfsProjectSetupCmd(dirPath string, projID uint32) string {
	return fmt.Sprintf("project -s -p %s %d", dirPath, projID)
}

// xfsLimitCmd is the `limit -p` command that sets the block hard limit for projID.
func xfsLimitCmd(projID uint32, quota string) string {
	return fmt.Sprintf("limit -p bhard=%s %d", quota, projID)
}

// xfsLimitClearCmd is the `limit -p` command that resets projID's block limits to
// 0 (0 == "no limit" in XFS), returning its dquot to the uninitialized state so the
// project drops out of `report -p` once its usage is also 0. Create/EnsureQuota only
// ever set bhard (xfsLimitCmd), so zeroing bhard+bsoft clears every limit fred wrote.
// Used by Destroy to keep the project-quota table bounded to live volumes (ENG-459).
func xfsLimitClearCmd(projID uint32) string {
	return fmt.Sprintf("limit -p bhard=0 bsoft=0 %d", projID)
}

func (x *xfsVolumeManager) Create(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
	dirPath := filepath.Join(x.dataPath, id)
	quota := fmt.Sprintf("%dm", sizeMB)

	// Idempotent: if directory already exists, read the marker file for the
	// real project ID (avoids re-deriving via CRC32 which may have been probed).
	_, statErr := os.Stat(dirPath)
	if statErr == nil {
		projID, err := readProjectIDFile(dirPath)
		if err != nil {
			return "", false, fmt.Errorf("read project ID marker for existing volume %s: %w", dirPath, err)
		}

		// Ensure this volume is tracked in the active maps.
		x.mu.Lock()
		x.trackProjectID(id, projID)
		x.mu.Unlock()

		cmd := xfsLimitCmd(projID, quota)
		if out, err := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(cmd, x.mountPoint)...).CombinedOutput(); err != nil {
			return "", false, fmt.Errorf("xfs_quota limit on existing %s (id=%d, quota=%s): %w: %s", dirPath, projID, quota, err, out)
		}
		x.logger.Debug("reusing existing xfs quota directory", "path", dirPath, "project_id", projID, "quota_mb", sizeMB)
		return dirPath, false, nil
	}
	if !errors.Is(statErr, fs.ErrNotExist) {
		return "", false, fmt.Errorf("stat volume dir %s: %w", dirPath, statErr)
	}

	// Allocate a collision-free project ID.
	projID, err := x.assignProjectID(id)
	if err != nil {
		return "", false, err
	}

	// Create new directory.
	if err := os.MkdirAll(dirPath, 0700); err != nil {
		x.removeProjectID(id)
		return "", false, fmt.Errorf("create directory %s: %w", dirPath, err)
	}

	// Write marker file so Validate can rebuild the map after restart.
	if err := writeProjectIDFile(dirPath, projID); err != nil {
		if cleanupErr := os.RemoveAll(dirPath); cleanupErr != nil {
			x.logger.Warn("failed to cleanup directory after marker write failure", "path", dirPath, "error", cleanupErr)
		}
		x.removeProjectID(id)
		return "", false, fmt.Errorf("write project ID marker for %s: %w", dirPath, err)
	}

	// Assign project ID to directory. The subdirectory is named in -p; the XFS
	// mount point is the xfs_quota filesystem argument (ENG-449).
	cmd := xfsProjectSetupCmd(dirPath, projID)
	if out, err := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(cmd, x.mountPoint)...).CombinedOutput(); err != nil {
		if cleanupErr := os.RemoveAll(dirPath); cleanupErr != nil {
			x.logger.Warn("failed to cleanup directory after xfs project setup failure", "path", dirPath, "error", cleanupErr)
		}
		x.removeProjectID(id)
		return "", false, fmt.Errorf("xfs_quota project setup for %s (id=%d): %w: %s", dirPath, projID, err, out)
	}

	// Set quota limit.
	cmd = xfsLimitCmd(projID, quota)
	if out, err := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(cmd, x.mountPoint)...).CombinedOutput(); err != nil {
		if cleanupErr := os.RemoveAll(dirPath); cleanupErr != nil {
			x.logger.Warn("failed to cleanup directory after xfs quota limit failure", "path", dirPath, "error", cleanupErr)
		}
		x.removeProjectID(id)
		return "", false, fmt.Errorf("xfs_quota limit for %s (id=%d, quota=%s): %w: %s", dirPath, projID, quota, err, out)
	}

	x.logger.Debug("created xfs project quota directory", "path", dirPath, "project_id", projID, "quota_mb", sizeMB)
	return dirPath, true, nil
}

// EnsureQuota re-applies the project tag + bhard limit to an existing volume,
// recovering the projID from its .fred-project-id marker. It re-tags the inode
// (project -s) — which heals a volume left untagged by a pre-CAP_SYS_ADMIN
// daemon (ENG-454) — then re-applies the limit. No-op if the directory is absent
// (never creates), so a concurrent deprovision is never resurrected.
func (x *xfsVolumeManager) EnsureQuota(ctx context.Context, id string, sizeMB int64) error {
	dirPath := filepath.Join(x.dataPath, id)
	if _, err := os.Stat(dirPath); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil // vanished (e.g. concurrent deprovision): nothing to enforce
		}
		return fmt.Errorf("stat volume dir %s: %w", dirPath, err)
	}
	projID, err := readProjectIDFile(dirPath)
	if err != nil {
		return fmt.Errorf("read project ID marker for %s: %w", dirPath, err)
	}

	x.mu.Lock()
	x.trackProjectID(id, projID)
	x.mu.Unlock()

	tagCmd := xfsProjectSetupCmd(dirPath, projID)
	if out, err := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(tagCmd, x.mountPoint)...).CombinedOutput(); err != nil {
		return fmt.Errorf("xfs_quota project re-tag for %s (id=%d): %w: %s", dirPath, projID, err, out)
	}
	limitCmd := xfsLimitCmd(projID, fmt.Sprintf("%dm", sizeMB))
	if out, err := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(limitCmd, x.mountPoint)...).CombinedOutput(); err != nil {
		return fmt.Errorf("xfs_quota limit for %s (id=%d): %w: %s", dirPath, projID, err, out)
	}
	x.logger.Debug("re-applied xfs project quota", "path", dirPath, "project_id", projID, "quota_mb", sizeMB)
	return nil
}

func (x *xfsVolumeManager) Destroy(ctx context.Context, id string) error {
	dirPath := filepath.Join(x.dataPath, id)

	// Resolve the projID BEFORE removing the directory — its .fred-project-id marker
	// lives inside dirPath — and hold it for the clear below, which runs only once
	// the removal succeeds.
	projID, hasProjID := x.resolveProjectID(id)

	if err := os.RemoveAll(dirPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		// The volume is still (at least partly) on disk. Leave the on-disk quota
		// limit intact (still enforced) AND keep the in-memory projID mapping (see
		// removeProjectID below), then return the error so the caller retries — a
		// later Destroy re-resolves the projID (from the marker if it survives, else
		// the still-present map entry) and finishes removal + clear. (Clearing before
		// removal would leave a surviving volume uncapped; freeing the map entry here
		// would let the projID be reallocated while these inodes are still tagged.)
		return fmt.Errorf("remove directory %s: %w", dirPath, err)
	}

	// The directory and its tagged inodes are gone, so the project's usage is now 0.
	// Reset its block limits to 0 so the dquot returns to the uninitialized state and
	// the kernel's GETNEXTQUOTA walk skips it — the entry then disappears from
	// report -p. XFS does NOT implicitly free a project's limit when its files are
	// deleted: left set, the bhard limit persists in the quota table and leaks one
	// entry per volume, and every xfs_quota op (report -p in Usage / Validate) scans
	// the whole table — so provisioning latency degrades cumulatively as leases
	// churn (ENG-459).
	if hasProjID {
		// Detach from the caller's ctx: a deprovision that was canceled or whose
		// deadline elapsed must not skip this cleanup (mirrors the provision-failure
		// teardown, which uses a fresh context). Skipping it would leave the entry
		// leaked with the directory already gone — unrecoverable on retry.
		clearCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		clearCmd := xfsLimitClearCmd(projID)
		if out, err := exec.CommandContext(clearCtx, "xfs_quota", xfsQuotaArgs(clearCmd, x.mountPoint)...).CombinedOutput(); err != nil {
			// Best-effort: a leaked limit entry holds no disk and must not wedge
			// teardown. Every Destroy caller reads a returned error as "the volume's
			// bytes are still on disk" and keeps the lease Failed with its footprint
			// counted (see deprovision/restore), so a zero-byte quota-clear failure
			// must NOT propagate. Record + log it as the observable backstop; the
			// stale entry needs a one-time operator cleanup (ENG-459).
			volumeQuotaClearFailedTotal.Inc()
			x.logger.Warn("xfs project-quota clear failed on destroy; entry leaked until operator cleanup",
				"path", dirPath, "project_id", projID, "error", err, "output", string(out))
		}
	}

	// Drop the in-memory projID mapping only now, once the on-disk volume is gone.
	// Keeping it until here means a failed/partial removal above stays retryable
	// (the map is the resolve fallback when the marker was already unlinked) and the
	// projID is never reallocated to a new volume while its inodes still exist.
	x.removeProjectID(id)

	x.logger.Debug("destroyed xfs quota directory", "path", dirPath)
	return nil
}

func (x *xfsVolumeManager) List() ([]string, error) {
	return listVolumeIDs(x.dataPath)
}

// RenameVolume renames the volume directory and updates the in-memory
// projectID maps so subsequent Create/Destroy calls on the new name
// resolve to the same XFS project ID (preserving the quota and the
// .fred-project-id marker file inside the directory).
//
// xfs project metadata is keyed by inode and survives a directory
// rename, so no xfs_quota reapplication is needed. The maps are updated
// atomically under x.mu after a successful os.Rename so a concurrent
// Create on the new name cannot observe an inconsistent state.
func (x *xfsVolumeManager) RenameVolume(oldName, newName string) error {
	oldPath := filepath.Join(x.dataPath, oldName)
	newPath := filepath.Join(x.dataPath, newName)
	if err := atomicRenameVolumeDir(oldPath, newPath); err != nil {
		return err
	}

	x.mu.Lock()
	defer x.mu.Unlock()
	if projID, ok := x.volumeToID[oldName]; ok {
		delete(x.volumeToID, oldName)
		x.volumeToID[newName] = projID
		x.activeIDs[projID] = newName
	}
	// If oldName wasn't in the map (idempotent rerun, or volume created
	// outside the live process), the next Create/Destroy on newName will
	// rebuild the entry via the marker-file read path.
	return nil
}

// HostPath returns the absolute path of the volume directory under the
// configured data path. The directory may or may not exist; callers use
// this to compute paths for not-yet-renamed or about-to-be-created
// volumes (see migrate.go in Task 9).
func (x *xfsVolumeManager) HostPath(name string) string {
	return filepath.Join(x.dataPath, name)
}

// Kind identifies the xfs backend.
func (x *xfsVolumeManager) Kind() string { return "xfs" }

// xfsBlockBytes is the XFS quota report block unit (1 KiB blocks).
const xfsBlockBytes = 1024

// Usage returns the project's used bytes from the XFS project-quota report.
// The "Used" column is in 1 KiB blocks; multiply by xfsBlockBytes. XFS quota
// accounting is kernel-maintained and real-time (no rescan). Must use
// `xfs_quota -x`; generic quota/repquota do not work with XFS project quotas.
func (x *xfsVolumeManager) Usage(ctx context.Context, id string) (int64, error) {
	dirPath := filepath.Join(x.dataPath, id)
	projID, err := readProjectIDFile(dirPath)
	if err != nil {
		return 0, fmt.Errorf("read project ID marker for %s: %w", dirPath, err)
	}
	out, err := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs("report -p -b -N", x.mountPoint)...).CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("xfs_quota report for %s (proj %d): %w: %s", dirPath, projID, err, out)
	}
	blocks, err := parseXfsReportUsedBlocks(string(out), projID)
	if err != nil {
		return 0, fmt.Errorf("read used blocks for proj %d under %s: %w", projID, dirPath, err)
	}
	return blocks * xfsBlockBytes, nil
}

// parseXfsReportUsedBlocks finds the report row for projID and returns its
// "Used" value in 1 KiB blocks. Rows look like:
//
//	#<projid>   <used>   <soft>   <hard>   <warn/grace>
//
// The first token may be the bare id or "#<id>".
func parseXfsReportUsedBlocks(out string, projID uint32) (int64, error) {
	want := strconv.FormatUint(uint64(projID), 10)
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		first := strings.TrimPrefix(fields[0], "#")
		if first != want {
			continue
		}
		used, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse used blocks %q: %w", fields[1], err)
		}
		return used, nil
	}
	return 0, fmt.Errorf("project id %d not found in xfs_quota report output", projID)
}

func (x *xfsVolumeManager) Validate() error {
	// Check xfs_quota binary exists.
	if _, err := exec.LookPath("xfs_quota"); err != nil {
		return fmt.Errorf("xfs_quota binary not found: %w", err)
	}

	// Check pquota mount option by attempting a quota report.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs("report -p", x.mountPoint)...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("xfs project quotas not available at %s (mount with pquota option): %w: %s",
			x.mountPoint, err, out)
	}

	// Setting an XFS project quota (project -s / limit -p → quotactl
	// Q_XSETQLIM) requires CAP_SYS_ADMIN. The report probe above is a READ and
	// succeeds without it, so it cannot detect a missing capability — which is
	// exactly how an under-privileged daemon silently failed to enforce quotas
	// (ENG-454). Fail fast at startup rather than rejecting every provision.
	if err := requireCapSysAdmin(x.Kind(), x.logger); err != nil {
		return err
	}

	// Populate activeIDs from existing volume marker files.
	ids, err := x.List()
	if err != nil {
		return fmt.Errorf("list volumes for active ID scan: %w", err)
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, vid := range ids {
		dirPath := filepath.Join(x.dataPath, vid)
		projID, err := readProjectIDFile(dirPath)
		if err != nil {
			return fmt.Errorf("read project ID marker for volume %s: %w", vid, err)
		}
		if existing, ok := x.activeIDs[projID]; ok && existing != vid {
			return fmt.Errorf("duplicate project ID %d: volumes %s and %s", projID, existing, vid)
		}
		x.trackProjectID(vid, projID)
	}

	return nil
}
