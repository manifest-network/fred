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
	logger   *slog.Logger

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

		cmd := fmt.Sprintf("limit -p bhard=%s %d", quota, projID)
		if out, err := exec.CommandContext(ctx, "xfs_quota", "-x", "-c", cmd, x.dataPath).CombinedOutput(); err != nil {
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

	// Assign project ID to directory.
	cmd := fmt.Sprintf("project -s -p %s %d", dirPath, projID)
	if out, err := exec.CommandContext(ctx, "xfs_quota", "-x", "-c", cmd, x.dataPath).CombinedOutput(); err != nil {
		if cleanupErr := os.RemoveAll(dirPath); cleanupErr != nil {
			x.logger.Warn("failed to cleanup directory after xfs project setup failure", "path", dirPath, "error", cleanupErr)
		}
		x.removeProjectID(id)
		return "", false, fmt.Errorf("xfs_quota project setup for %s (id=%d): %w: %s", dirPath, projID, err, out)
	}

	// Set quota limit.
	cmd = fmt.Sprintf("limit -p bhard=%s %d", quota, projID)
	if out, err := exec.CommandContext(ctx, "xfs_quota", "-x", "-c", cmd, x.dataPath).CombinedOutput(); err != nil {
		if cleanupErr := os.RemoveAll(dirPath); cleanupErr != nil {
			x.logger.Warn("failed to cleanup directory after xfs quota limit failure", "path", dirPath, "error", cleanupErr)
		}
		x.removeProjectID(id)
		return "", false, fmt.Errorf("xfs_quota limit for %s (id=%d, quota=%s): %w: %s", dirPath, projID, quota, err, out)
	}

	x.logger.Debug("created xfs project quota directory", "path", dirPath, "project_id", projID, "quota_mb", sizeMB)
	return dirPath, true, nil
}

func (x *xfsVolumeManager) Destroy(ctx context.Context, id string) error {
	dirPath := filepath.Join(x.dataPath, id)

	x.removeProjectID(id)

	// Remove the directory (quota is implicitly freed).
	if err := os.RemoveAll(dirPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove directory %s: %w", dirPath, err)
	}

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
	out, err := exec.CommandContext(ctx, "xfs_quota", "-x", "-c", "report -p -b -N", x.dataPath).CombinedOutput()
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
	out, err := exec.CommandContext(ctx, "xfs_quota", "-x", "-c", "report -p", x.dataPath).CombinedOutput()
	if err != nil {
		return fmt.Errorf("xfs project quotas not available at %s (mount with pquota option): %w: %s",
			x.dataPath, err, out)
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
