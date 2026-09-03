package docker

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/fsidentity"
)

// projectIDFile is the marker file written inside each volume directory
// to record the assigned XFS project ID. This allows the active ID map
// to be rebuilt after a restart and avoids re-deriving via CRC32 (which
// could silently collide with another volume).
const (
	projectIDFile        = ".fred-project-id"
	xfsStagePrefix       = ".fred-xfs-stage-"
	xfsDeleteStagePrefix = ".fred-xfs-delete-"
)

// inodeHardFloor is the minimum XFS inode hard limit applied to any volume,
// regardless of how small its block quota is. It keeps small/ephemeral volumes
// (e.g. the 64 MB writable-path-only fallback) from a starved inode ceiling.
// 256 Ki ≈ a shared-hosting "starter" allowance. See ENG-548.
const inodeHardFloor = 262_144

// inodeHardLimit derives a volume's XFS inode hard limit from its block quota
// (sizeMB) and the configured minimum average file size (minAvgFileBytes). The
// ratio is the mkfs "-i bytes-per-inode" idiom: ihard = sizeMB*MiB/minAvgFileBytes.
// minAvgFileBytes <= 0 uses the defaultMinAvgFileBytes default (1024 inodes per
// MiB). The result is floored at inodeHardFloor. The intermediate
// (sizeMB*bytesPerMiB) stays far below the int64 max for any realistic SKU size
// (e.g. 245,760 MB → 2.577e11, ~7 orders of magnitude below the int64 max). See
// ENG-548.
func inodeHardLimit(sizeMB, minAvgFileBytes int64) int64 {
	if minAvgFileBytes <= 0 {
		minAvgFileBytes = defaultMinAvgFileBytes
	}
	ihard := (sizeMB * bytesPerMiB) / minAvgFileBytes
	if ihard < inodeHardFloor {
		ihard = inodeHardFloor
	}
	return ihard
}

// xfsVolumeManager creates directories with XFS project quotas.
type xfsProjectAttributeReader interface {
	ReadProjectAttributes(*os.Root) (linuxFSXAttr, error)
}

type xfsVolumeManager struct {
	dataPath string
	// mountPoint is the XFS mount that contains dataPath, resolved once at
	// construction. xfs_quota requires the mount point (not a subdirectory of
	// it) as its trailing filesystem argument; dataPath is typically a subdir
	// of this mount. See resolveMountpoint and ENG-449.
	mountPoint string
	logger     *slog.Logger
	// minAvgFileBytes is the ratio used by inodeHardLimit to derive each volume's
	// XFS inode hard limit from its block quota. Set from Config.GetMinAvgFileBytes().
	minAvgFileBytes int64
	// projectAttributes is a descriptor-rooted kernel reader. Keeping the
	// interface at its consumer boundary lets filesystem-free unit tests model
	// the UAPI result without weakening the production XFS check.
	projectAttributes xfsProjectAttributeReader

	// rootWatch refuses to report an emptiness it cannot vouch for (ENG-687).
	rootWatch volumeRootWatch

	mu         sync.Mutex
	activeIDs  map[uint32]string // projectID → volumeID
	volumeToID map[string]uint32 // volumeID → projectID (reverse index)
	// durableStages contains only stages created and parent-synced by this
	// process. Startup-scanned stages are cleanup-only capabilities and never
	// enter this map, so Create cannot accidentally publish recovery evidence
	// whose original quota is unknown.
	durableStages map[string]xfsStageName // final volumeID → exact stage
	// recoveredStages is replaced atomically by each strict startup scan. It is
	// consumed only by pinned-root startup cleanup; adoption/preflight sees the
	// non-empty set and refuses without mutating it.
	recoveredStages map[string]recoveredXFSStage
	// durableDeleteStages records empty, parent-synced teardown-authority
	// directories created before recursive removal. Their typed basename remains
	// authoritative even when a partial RemoveAll has already unlinked the marker
	// inside the separately named managed volume.
	durableDeleteStages map[string]xfsDeleteStageName
	// recoveredDeleteStages is rebuilt only from strictly parsed, real deletion
	// directories at startup. It lets recovery finish a marker-first partial
	// recursive delete without guessing or recomputing the collision-probed ID.
	recoveredDeleteStages map[string]recoveredXFSDeleteStage
}

func (x *xfsVolumeManager) PinIdentityRoot() error { return x.rootWatch.pin(x.dataPath) }

func (x *xfsVolumeManager) VerifyIdentityRoot() error { return x.rootWatch.verify(x.dataPath) }

// xfsStageName is a typed write-ahead directory name. Its basename durably
// records both the reserved nonzero project ID and the exact final managed
// volume name, allowing startup to recover allocation authority even when a
// crash happened before the marker was written.
type xfsStageName struct {
	entry    storagePathComponent
	projID   uint32
	volumeID managedVolumeName
}

func newXFSStageName(projID uint32, volumeID managedVolumeName) (xfsStageName, error) {
	if projID == 0 {
		return xfsStageName{}, errors.New("xfs stage project ID 0 is reserved")
	}
	return parseXFSStageName(fmt.Sprintf("%s%d-%s", xfsStagePrefix, projID, volumeID.value()))
}

func parseXFSStageName(value string) (xfsStageName, error) {
	entry, err := parseStoragePathComponent(value)
	if err != nil {
		return xfsStageName{}, fmt.Errorf("xfs stage name: %w", err)
	}
	if !strings.HasPrefix(value, xfsStagePrefix) {
		return xfsStageName{}, fmt.Errorf("xfs stage name %q is outside the reserved namespace", value)
	}
	idText, finalName, found := strings.Cut(strings.TrimPrefix(value, xfsStagePrefix), "-")
	if !found || idText == "" || finalName == "" {
		return xfsStageName{}, fmt.Errorf("xfs stage name %q has no project ID and final volume", value)
	}
	parsedID, err := strconv.ParseUint(idText, 10, 32)
	if err != nil || parsedID == 0 || strconv.FormatUint(parsedID, 10) != idText {
		return xfsStageName{}, fmt.Errorf("xfs stage name %q has invalid canonical project ID %q", value, idText)
	}
	volumeID, err := parseManagedVolumeName(finalName)
	if err != nil {
		return xfsStageName{}, fmt.Errorf("xfs stage name %q has invalid final volume: %w", value, err)
	}
	return xfsStageName{entry: entry, projID: uint32(parsedID), volumeID: volumeID}, nil
}

func (stage xfsStageName) value() string { return string(stage.entry) }

func (stage xfsStageName) hostPath(rootPath string) string {
	return filepath.Join(rootPath, stage.value())
}

// xfsDeleteStageName is durable teardown authority. Destroy creates this empty
// sibling with no-replace semantics and fsyncs the parent after attesting the
// managed volume; only then may recursive deletion start. Consequently a failed
// deletion can remove any subset of the volume contents, including
// .fred-project-id, without erasing the project ID or managed-volume identity
// needed by a retry/restart. The sibling itself is not tagged with the retiring
// project ID, so exact block/inode usage can reach zero while it remains named.
type xfsDeleteStageName struct {
	entry    storagePathComponent
	projID   uint32
	volumeID managedVolumeName
}

func newXFSDeleteStageName(projID uint32, volumeID managedVolumeName) (xfsDeleteStageName, error) {
	if projID == 0 {
		return xfsDeleteStageName{}, errors.New("xfs delete-stage project ID 0 is reserved")
	}
	return parseXFSDeleteStageName(fmt.Sprintf("%s%d-%s", xfsDeleteStagePrefix, projID, volumeID.value()))
}

func parseXFSDeleteStageName(value string) (xfsDeleteStageName, error) {
	entry, err := parseStoragePathComponent(value)
	if err != nil {
		return xfsDeleteStageName{}, fmt.Errorf("xfs delete-stage name: %w", err)
	}
	if !strings.HasPrefix(value, xfsDeleteStagePrefix) {
		return xfsDeleteStageName{}, fmt.Errorf("xfs delete-stage name %q is outside the reserved namespace", value)
	}
	idText, finalName, found := strings.Cut(strings.TrimPrefix(value, xfsDeleteStagePrefix), "-")
	if !found || idText == "" || finalName == "" {
		return xfsDeleteStageName{}, fmt.Errorf("xfs delete-stage name %q has no project ID and final volume", value)
	}
	parsedID, err := strconv.ParseUint(idText, 10, 32)
	if err != nil || parsedID == 0 || strconv.FormatUint(parsedID, 10) != idText {
		return xfsDeleteStageName{}, fmt.Errorf("xfs delete-stage name %q has invalid canonical project ID %q", value, idText)
	}
	volumeID, err := parseManagedVolumeName(finalName)
	if err != nil {
		return xfsDeleteStageName{}, fmt.Errorf("xfs delete-stage name %q has invalid final volume: %w", value, err)
	}
	return xfsDeleteStageName{entry: entry, projID: uint32(parsedID), volumeID: volumeID}, nil
}

func (stage xfsDeleteStageName) value() string { return string(stage.entry) }

func (stage xfsDeleteStageName) hostPath(rootPath string) string {
	return filepath.Join(rootPath, stage.value())
}

// recoveredXFSStage is minted only by the strict startup scanner. It grants
// cleanup authority, never publication authority: startup does not know the
// original requested quota and therefore must not complete provisioning.
type recoveredXFSStage struct {
	stage xfsStageName
}

// recoveredXFSDeleteStage is minted only by the strict startup scanner. The
// typed, parent-durable basename authorizes continuing deletion of its one exact
// managed volume; it cannot name any path outside the typed managed namespace.
type recoveredXFSDeleteStage struct {
	stage xfsDeleteStageName
}

// createdXFSStageDirectory is minted only by a successful atomic stage mkdir.
// Once its parent directory is synced, the stage is promoted into the
// manager's durableStages map and this pre-durability rollback capability is no
// longer used.
type createdXFSStageDirectory struct {
	stage xfsStageName
}

// createdXFSProjectIDReservation is proof that this invocation inserted both
// sides of a project-ID mapping. A reused preexisting mapping deliberately
// yields no capability, so compensation cannot erase authority it did not own.
type createdXFSProjectIDReservation struct {
	volumeID string
	projID   uint32
}

// reserveProjectID returns a collision-free XFS project ID for volumeID and a
// cleanup capability only when this call inserted the reservation. It uses
// CRC32 as the initial candidate and probes (increments) on collision. The
// caller must NOT hold x.mu.
func (x *xfsVolumeManager) reserveProjectID(
	volumeID string,
) (uint32, *createdXFSProjectIDReservation, error) {
	candidate := crc32.ChecksumIEEE([]byte(volumeID))

	x.mu.Lock()
	defer x.mu.Unlock()

	// If this volumeID already owns a project ID, return it (idempotent).
	if id, ok := x.volumeToID[volumeID]; ok {
		if id == 0 || x.activeIDs[id] != volumeID {
			return 0, nil, fmt.Errorf("xfs project ID maps are inconsistent for volume %q", volumeID)
		}
		return id, nil, nil
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
		return 0, nil, fmt.Errorf("xfs project ID space exhausted (%d active IDs)", len(x.activeIDs))
	}

	x.activeIDs[candidate] = volumeID
	x.volumeToID[volumeID] = candidate
	return candidate, &createdXFSProjectIDReservation{volumeID: volumeID, projID: candidate}, nil
}

// registerProjectIDLocked records an observed marker without allowing it to
// overwrite another volume's authority. A duplicate/corrupt marker must stop
// quota work; silently repairing either map could make two tenants share one
// dquot. The caller must hold x.mu.
func (x *xfsVolumeManager) registerProjectIDLocked(volumeID string, projID uint32) error {
	if projID == 0 {
		return errors.New("xfs project ID 0 is reserved for the default project")
	}
	if oldID, ok := x.volumeToID[volumeID]; ok && oldID != projID {
		return fmt.Errorf("xfs volume %q is already registered to project ID %d, marker names %d",
			volumeID, oldID, projID)
	}
	if oldVolume, ok := x.activeIDs[projID]; ok && oldVolume != volumeID {
		return fmt.Errorf("duplicate project ID %d: already registered to volume %q, marker belongs to %q",
			projID, oldVolume, volumeID)
	}
	x.activeIDs[projID] = volumeID
	x.volumeToID[volumeID] = projID
	return nil
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
func (x *xfsVolumeManager) resolveProjectID(root *os.Root, volumeID managedVolumeName) (uint32, bool) {
	dirPath := volumeID.hostPath(x.dataPath)
	projID, err := readProjectIDFileAtRoot(root, volumeID)
	switch {
	case err == nil:
		x.mu.Lock()
		mappedID, volumeKnown := x.volumeToID[volumeID.value()]
		mappedVolume, idKnown := x.activeIDs[projID]
		x.mu.Unlock()
		if (volumeKnown && mappedID != projID) ||
			(idKnown && mappedVolume != volumeID.value()) || volumeKnown != idKnown {
			x.logger.Warn("xfs project-id marker conflicts with active authority; skipping quota clear",
				"path", dirPath, "marker_project_id", projID,
				"mapped_project_id", mappedID, "mapped_volume", mappedVolume)
			return 0, false
		}
		return projID, projID != 0
	case errors.Is(err, fs.ErrNotExist):
		x.mu.Lock()
		defer x.mu.Unlock()
		id, ok := x.volumeToID[volumeID.value()]
		return id, ok && id != 0 && x.activeIDs[id] == volumeID.value()
	default:
		// Marker present but unreadable/corrupt: do not guess from the map — skip
		// the clear rather than risk zeroing a wrong/foreign project, and log it.
		// Validate's startup scan rejects a marker already corrupt at open, but one
		// that corrupts later reaches here; skipping is still the safe choice —
		// at worst a logged, leaked entry for operator cleanup, never a wrong project
		// cleared.
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
		if x.activeIDs[id] == volumeID {
			delete(x.activeIDs, id)
		}
		delete(x.volumeToID, volumeID)
	}
}

func (x *xfsVolumeManager) releaseCreatedProjectID(
	reservation *createdXFSProjectIDReservation,
) {
	if reservation == nil {
		return
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.volumeToID[reservation.volumeID] != reservation.projID ||
		x.activeIDs[reservation.projID] != reservation.volumeID {
		return
	}
	delete(x.volumeToID, reservation.volumeID)
	delete(x.activeIDs, reservation.projID)
}

func (x *xfsVolumeManager) rememberDurableStage(stage xfsStageName) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.durableStages == nil {
		x.durableStages = make(map[string]xfsStageName)
	}
	if existing, ok := x.durableStages[stage.volumeID.value()]; ok && existing != stage {
		return fmt.Errorf("xfs volume %q already has durable stage %q, refusing %q",
			stage.volumeID.value(), existing.value(), stage.value())
	}
	x.durableStages[stage.volumeID.value()] = stage
	return nil
}

func (x *xfsVolumeManager) durableStage(volumeID managedVolumeName) (xfsStageName, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	stage, ok := x.durableStages[volumeID.value()]
	return stage, ok
}

func (x *xfsVolumeManager) forgetDurableStage(stage xfsStageName) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.durableStages[stage.volumeID.value()] == stage {
		delete(x.durableStages, stage.volumeID.value())
	}
}

func (x *xfsVolumeManager) recoveredStage(volumeID managedVolumeName) (recoveredXFSStage, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	stage, ok := x.recoveredStages[volumeID.value()]
	return stage, ok
}

func (x *xfsVolumeManager) rememberDurableDeleteStage(stage xfsDeleteStageName) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.durableDeleteStages == nil {
		x.durableDeleteStages = make(map[string]xfsDeleteStageName)
	}
	volumeID := stage.volumeID.value()
	if existing, ok := x.durableDeleteStages[volumeID]; ok && existing != stage {
		return fmt.Errorf("xfs volume %q already has durable delete-stage %q, refusing %q",
			volumeID, existing.value(), stage.value())
	}
	if recovered, ok := x.recoveredDeleteStages[volumeID]; ok && recovered.stage != stage {
		return fmt.Errorf("xfs volume %q already has recovered delete-stage %q, refusing %q",
			volumeID, recovered.stage.value(), stage.value())
	}
	x.durableDeleteStages[volumeID] = stage
	return nil
}

func (x *xfsVolumeManager) deleteStage(volumeID managedVolumeName) (xfsDeleteStageName, bool, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	durable, durableOK := x.durableDeleteStages[volumeID.value()]
	recovered, recoveredOK := x.recoveredDeleteStages[volumeID.value()]
	if durableOK && recoveredOK && durable != recovered.stage {
		return xfsDeleteStageName{}, false, fmt.Errorf(
			"%w: xfs volume %q has conflicting live and recovered delete-stages",
			ErrVolumeMutationRecoveryPending, volumeID.value(),
		)
	}
	if durableOK {
		return durable, true, nil
	}
	if recoveredOK {
		return recovered.stage, true, nil
	}
	return xfsDeleteStageName{}, false, nil
}

func (x *xfsVolumeManager) pendingMutationNamesLocked(volumeID managedVolumeName) []string {
	var names []string
	appendUnique := func(name string) {
		for _, existing := range names {
			if existing == name {
				return
			}
		}
		names = append(names, name)
	}
	if stage, ok := x.durableStages[volumeID.value()]; ok {
		appendUnique(stage.value())
	}
	if recovered, ok := x.recoveredStages[volumeID.value()]; ok {
		appendUnique(recovered.stage.value())
	}
	if stage, ok := x.durableDeleteStages[volumeID.value()]; ok {
		appendUnique(stage.value())
	}
	if recovered, ok := x.recoveredDeleteStages[volumeID.value()]; ok {
		appendUnique(recovered.stage.value())
	}
	sort.Strings(names)
	return names
}

func (x *xfsVolumeManager) hasDurableDeleteStage(stage xfsDeleteStageName) bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.durableDeleteStages[stage.volumeID.value()] == stage
}

func (x *xfsVolumeManager) removeDeleteStageAuthorityLocked(stage xfsDeleteStageName) error {
	volumeID := stage.volumeID.value()
	if x.volumeToID[volumeID] != stage.projID || x.activeIDs[stage.projID] != volumeID {
		return fmt.Errorf("xfs delete-stage %q project-ID authority changed before cleanup commit", stage.value())
	}
	delete(x.volumeToID, volumeID)
	delete(x.activeIDs, stage.projID)
	if x.durableDeleteStages[volumeID] == stage {
		delete(x.durableDeleteStages, volumeID)
	}
	if recovered, ok := x.recoveredDeleteStages[volumeID]; ok && recovered.stage == stage {
		delete(x.recoveredDeleteStages, volumeID)
	}
	return nil
}

func (x *xfsVolumeManager) removeStageAuthorityLocked(stage xfsStageName) error {
	volumeID := stage.volumeID.value()
	if x.volumeToID[volumeID] != stage.projID || x.activeIDs[stage.projID] != volumeID {
		return fmt.Errorf("xfs stage %q project-ID authority changed before cleanup commit", stage.value())
	}
	delete(x.volumeToID, volumeID)
	delete(x.activeIDs, stage.projID)
	if x.durableStages[volumeID] == stage {
		delete(x.durableStages, volumeID)
	}
	if recovered, ok := x.recoveredStages[volumeID]; ok && recovered.stage == stage {
		delete(x.recoveredStages, volumeID)
	}
	return nil
}

func readProjectIDFileAtRoot(root *os.Root, volumeID managedVolumeName) (uint32, error) {
	volumeRoot, err := openAttestedManagedVolumeRoot(root, volumeID)
	if err != nil {
		return 0, err
	}
	defer func() { _ = volumeRoot.Close() }()
	return readProjectIDFileInVolumeRoot(volumeRoot)
}

func writeProjectIDFileInVolumeRoot(volumeRoot *os.Root, id uint32) error {
	if id == 0 {
		return errors.New("xfs project ID 0 is reserved for the default project")
	}
	file, err := volumeRoot.OpenFile(
		projectIDFile,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW,
		0600,
	)
	if err != nil {
		return err
	}
	data := []byte(strconv.FormatUint(uint64(id), 10))
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func readProjectIDFileInVolumeRoot(volumeRoot *os.Root) (uint32, error) {
	data, _, err := readProjectIDMarkerInVolumeRoot(volumeRoot)
	if err != nil {
		return 0, err
	}
	return parseProjectIDFile(data, projectIDFile)
}

func readProjectIDMarkerInVolumeRoot(volumeRoot *os.Root) ([]byte, os.FileInfo, error) {
	before, err := volumeRoot.Lstat(projectIDFile)
	if err != nil {
		return nil, nil, err
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, nil, fmt.Errorf("xfs project ID marker is not a regular file")
	}
	file, err := volumeRoot.OpenFile(projectIDFile, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = file.Close() }()
	after, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	if !os.SameFile(before, after) {
		return nil, nil, fmt.Errorf("xfs project ID marker changed while opening it")
	}
	const maxProjectIDMarkerBytes = 64
	data, err := io.ReadAll(io.LimitReader(file, maxProjectIDMarkerBytes+1))
	if err != nil {
		return nil, nil, err
	}
	if len(data) > maxProjectIDMarkerBytes {
		return nil, nil, fmt.Errorf("xfs project ID marker exceeds %d bytes", maxProjectIDMarkerBytes)
	}
	return data, after, nil
}

func parseProjectIDFile(data []byte, path string) (uint32, error) {
	v, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("parse project ID from %s: %w", path, err)
	}
	if v == 0 {
		return 0, fmt.Errorf("parse project ID from %s: project ID 0 is reserved for the default project", path)
	}
	return uint32(v), nil
}

func openXFSRootCapabilities(dataPath string) (*os.Root, *fsidentity.Directory, error) {
	absolutePath, err := filepath.Abs(dataPath)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve absolute xfs volume root: %w", err)
	}
	absolutePath = filepath.Clean(absolutePath)
	parent, err := fsidentity.OpenDirectory(absolutePath)
	if err != nil {
		return nil, nil, fmt.Errorf("open identity-bound xfs volume root: %w", err)
	}
	root, err := os.OpenRoot(absolutePath)
	if err != nil {
		_ = parent.Close()
		return nil, nil, fmt.Errorf("open descriptor-rooted xfs volume root: %w", err)
	}
	rootInfo, err := root.Stat(".")
	if err != nil {
		_ = root.Close()
		_ = parent.Close()
		return nil, nil, fmt.Errorf("stat descriptor-rooted xfs volume root: %w", err)
	}
	stat, ok := rootInfo.Sys().(*syscall.Stat_t)
	identity := parent.Identity()
	if !ok || stat.Dev != identity.Device || stat.Ino != identity.Inode {
		_ = root.Close()
		_ = parent.Close()
		return nil, nil, errors.New("xfs root capabilities do not bind the same physical directory")
	}
	return root, parent, nil
}

func syncOSRoot(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}

func openAttestedXFSStageRoot(root *os.Root, stage xfsStageName) (*os.Root, error) {
	before, err := root.Lstat(stage.value())
	if err != nil {
		return nil, err
	}
	if !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("xfs stage entry %q is not a real directory", stage.value())
	}
	stageRoot, err := root.OpenRoot(stage.value())
	if err != nil {
		return nil, err
	}
	after, err := stageRoot.Stat(".")
	if err != nil {
		_ = stageRoot.Close()
		return nil, err
	}
	if !os.SameFile(before, after) {
		_ = stageRoot.Close()
		return nil, fmt.Errorf("xfs stage entry %q changed while opening it", stage.value())
	}
	return stageRoot, nil
}

func openAttestedXFSDeleteStageRoot(root *os.Root, stage xfsDeleteStageName) (*os.Root, error) {
	before, err := root.Lstat(stage.value())
	if err != nil {
		return nil, err
	}
	if !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("xfs delete-stage entry %q is not a real directory", stage.value())
	}
	stageRoot, err := root.OpenRoot(stage.value())
	if err != nil {
		return nil, err
	}
	after, err := stageRoot.Stat(".")
	if err != nil {
		_ = stageRoot.Close()
		return nil, err
	}
	if !os.SameFile(before, after) {
		_ = stageRoot.Close()
		return nil, fmt.Errorf("xfs delete-stage entry %q changed while opening it", stage.value())
	}
	return stageRoot, nil
}

// inspectXFSDeleteStage proves that the typed teardown authority is still the
// separate empty directory created by Destroy. Tenant data is never moved into
// this namespace, so any content is foreign/corrupt evidence and must stop
// recovery before the corresponding managed volume can be touched.
func inspectXFSDeleteStage(root *os.Root, stage xfsDeleteStageName) error {
	stageRoot, err := openAttestedXFSDeleteStageRoot(root, stage)
	if err != nil {
		return err
	}
	defer func() { _ = stageRoot.Close() }()
	entries, err := readXFSRootEntries(stageRoot)
	if err != nil {
		return fmt.Errorf("list xfs delete-stage %q: %w", stage.value(), err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("xfs delete-stage %q is not empty", stage.value())
	}
	return nil
}

// inspectXFSStage accepts only the two crash-valid shapes: an empty directory
// (mkdir committed before marker creation) or a directory containing exactly
// one regular marker whose ID equals the allocation encoded in the stage name.
func inspectXFSStage(root *os.Root, stage xfsStageName) (bool, error) {
	stageRoot, err := openAttestedXFSStageRoot(root, stage)
	if err != nil {
		return false, err
	}
	defer func() { _ = stageRoot.Close() }()
	directory, err := stageRoot.Open(".")
	if err != nil {
		return false, err
	}
	entries, readErr := directory.ReadDir(2)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return false, readErr
	}
	if closeErr != nil {
		return false, closeErr
	}
	if len(entries) == 0 {
		return false, nil
	}
	if len(entries) != 1 || entries[0].Name() != projectIDFile {
		return false, fmt.Errorf("xfs stage %q contains data outside its project marker", stage.value())
	}
	projID, err := readProjectIDFileInVolumeRoot(stageRoot)
	if err != nil {
		return false, fmt.Errorf("read xfs stage %q project marker: %w", stage.value(), err)
	}
	if projID != stage.projID {
		return false, fmt.Errorf("xfs stage %q encodes project ID %d but marker names %d",
			stage.value(), stage.projID, projID)
	}
	return true, nil
}

// xfsStageCleanupProof is cleanup-only authority for one crash-valid private
// stage shape. Unlike inspectXFSStage, it accepts any bounded content in the
// sole regular marker: the parent-synced stage basename already carries the
// exact project ID and final managed name, while a power loss before marker
// fsync may recover the small write as empty, partial, or zero-filled bytes.
// Publication never consumes this weaker proof.
type xfsStageCleanupProof struct {
	stageInfo     os.FileInfo
	markerPresent bool
	markerInfo    os.FileInfo
	markerData    string
}

func inspectXFSStageForCleanup(root *os.Root, stage xfsStageName) (xfsStageCleanupProof, error) {
	stageInfo, err := root.Lstat(stage.value())
	if err != nil {
		return xfsStageCleanupProof{}, err
	}
	stageRoot, err := openAttestedXFSStageRoot(root, stage)
	if err != nil {
		return xfsStageCleanupProof{}, err
	}
	defer func() { _ = stageRoot.Close() }()
	directory, err := stageRoot.Open(".")
	if err != nil {
		return xfsStageCleanupProof{}, err
	}
	entries, readErr := directory.ReadDir(2)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return xfsStageCleanupProof{}, readErr
	}
	if closeErr != nil {
		return xfsStageCleanupProof{}, closeErr
	}
	proof := xfsStageCleanupProof{stageInfo: stageInfo}
	if len(entries) == 0 {
		return proof, nil
	}
	if len(entries) != 1 || entries[0].Name() != projectIDFile {
		return xfsStageCleanupProof{}, fmt.Errorf("xfs stage %q contains data outside its project marker", stage.value())
	}
	markerData, markerInfo, err := readProjectIDMarkerInVolumeRoot(stageRoot)
	if err != nil {
		return xfsStageCleanupProof{}, fmt.Errorf("read xfs stage %q project marker for cleanup: %w", stage.value(), err)
	}
	observed := string(markerData)
	// A decimal uint32 project ID occupies at most ten bytes.
	const maxXFSStageMarkerBytes = 10
	if len(markerData) > maxXFSStageMarkerBytes {
		return xfsStageCleanupProof{}, fmt.Errorf(
			"xfs stage %q marker is %d bytes; the create writer emits at most %d",
			stage.value(), len(markerData), maxXFSStageMarkerBytes,
		)
	}
	proof.markerPresent = true
	proof.markerInfo = markerInfo
	proof.markerData = observed
	return proof, nil
}

func sameXFSStageCleanupProof(before, after xfsStageCleanupProof) bool {
	if before.stageInfo == nil || after.stageInfo == nil || !os.SameFile(before.stageInfo, after.stageInfo) ||
		before.markerPresent != after.markerPresent {
		return false
	}
	if !before.markerPresent {
		return true
	}
	return before.markerInfo != nil && after.markerInfo != nil &&
		os.SameFile(before.markerInfo, after.markerInfo) && before.markerData == after.markerData
}

func ensureXFSStageMarker(root *os.Root, stage xfsStageName) error {
	markerPresent, err := inspectXFSStage(root, stage)
	if err != nil {
		return err
	}
	if markerPresent {
		return nil
	}
	stageRoot, err := openAttestedXFSStageRoot(root, stage)
	if err != nil {
		return err
	}
	defer func() { _ = stageRoot.Close() }()
	if err := writeProjectIDFileInVolumeRoot(stageRoot, stage.projID); err != nil {
		return err
	}
	if err := syncOSRoot(stageRoot); err != nil {
		return fmt.Errorf("sync xfs stage %q after marker creation: %w", stage.value(), err)
	}
	return nil
}

func readXFSRootEntries(root *os.Root) ([]os.DirEntry, error) {
	directory, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	entries, readErr := directory.ReadDir(-1)
	closeErr := directory.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return entries, nil
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

// xfsProjectResetToDefaultCmd assigns only the empty teardown-authority inode
// to project 0. `-d 0` is load-bearing: the authority directory must remain a
// separate, untagged sibling while the managed tree keeps its original project
// ID and quota until byte deletion and both usage proofs complete.
func xfsProjectResetToDefaultCmd(dirPath string) string {
	return fmt.Sprintf("project -s -d 0 -p %s 0", dirPath)
}

// linuxFSXAttr is the stable Linux UAPI struct consumed by
// FS_IOC_FSGETXATTR. Keep the explicit padding: the ioctl request encodes the
// structure's 28-byte size.
type linuxFSXAttr struct {
	XFlags     uint32
	ExtentSize uint32
	Nextents   uint32
	ProjectID  uint32
	CowExtSize uint32
	Padding    [8]byte
}

// _IOR('X', 31, struct fsxattr), from linux/fs.h. Linux architecture families
// differ in the read-direction bits, so inherit those from x/sys' generated
// FS_IOC_GETFLAGS constant instead of baking in the amd64 request value.
// Querying the inode directly avoids treating localized or version-specific
// xfsprogs prose as authority.
const linuxFSIOCFSGetXAttr = (uintptr(unix.FS_IOC_GETFLAGS) & 0xc0000000) |
	(unsafe.Sizeof(linuxFSXAttr{}) << 16) |
	(uintptr('X') << 8) |
	31

const (
	linuxXFSFilesystemMagic = 0x58465342
	linuxFSXFlagProjInherit = 0x00000200
)

type linuxXFSProjectAttributeReader struct{}

func (linuxXFSProjectAttributeReader) ReadProjectAttributes(root *os.Root) (linuxFSXAttr, error) {
	file, err := root.Open(".")
	if err != nil {
		return linuxFSXAttr{}, err
	}
	defer func() { _ = file.Close() }()
	var filesystem unix.Statfs_t
	if err := unix.Fstatfs(int(file.Fd()), &filesystem); err != nil {
		return linuxFSXAttr{}, err
	}
	if uint64(filesystem.Type) != linuxXFSFilesystemMagic {
		return linuxFSXAttr{}, fmt.Errorf("opened delete-stage is on filesystem type %#x, want XFS", filesystem.Type)
	}
	var attr linuxFSXAttr
	_, _, errno := unix.Syscall(
		unix.SYS_IOCTL,
		file.Fd(),
		linuxFSIOCFSGetXAttr,
		uintptr(unsafe.Pointer(&attr)), // #nosec G103 -- stable Linux fsxattr UAPI buffer
	)
	runtime.KeepAlive(file)
	if errno != 0 {
		return linuxFSXAttr{}, errno
	}
	return attr, nil
}

func validateXFSDefaultProject(attr linuxFSXAttr) error {
	if attr.ProjectID != 0 {
		return fmt.Errorf("inode remains assigned to project %d, want default project 0", attr.ProjectID)
	}
	if attr.XFlags&linuxFSXFlagProjInherit == 0 {
		return errors.New("inode has no project inheritance flag")
	}
	return nil
}

func (x *xfsVolumeManager) projectAttributeReader() xfsProjectAttributeReader {
	if x.projectAttributes != nil {
		return x.projectAttributes
	}
	return linuxXFSProjectAttributeReader{}
}

// xfsLimitCmd is the `limit -p` command that sets the block hard limit (bhard) and
// the inode hard limit (ihard) for projID in a single quotactl. ihard is a plain
// integer count (no unit suffix). See ENG-548.
func xfsLimitCmd(projID uint32, quota string, ihard int64) string {
	return fmt.Sprintf("limit -p bhard=%s ihard=%d %d", quota, ihard, projID)
}

// xfsLimitClearCmd resets projID's block AND inode limits to 0 (0 == "no limit"),
// returning its dquot to the uninitialized state so the project drops out of
// `report -p` once its usage is also 0. Create/EnsureQuota set bhard+ihard, so
// zeroing all four clears every limit fred wrote. Used by Destroy (ENG-459/548).
func xfsLimitClearCmd(projID uint32) string {
	return fmt.Sprintf("limit -p bhard=0 bsoft=0 ihard=0 isoft=0 %d", projID)
}

// xfsProjectReportCmd requests a numeric, headerless row for exactly one
// project. Numeric output prevents /etc/projid aliases from turning a present
// dquot into an apparent miss; the bounds avoid scanning or parsing unrelated
// project rows when proving deletion safety.
func xfsProjectReportCmd(resource string, projID uint32) string {
	return fmt.Sprintf("report -p -%s -n -N -L %d -U %d", resource, projID, projID)
}

// xfsInodeGCTriggerCmd reads exactly project 0. XFS treats an ID-0 quota read
// as the start of a reporting scan and flushes or expedites pending inode-GC
// work for the containing mount (the kernel implementation is version-specific).
// The `quota` command uses XFS_GETQUOTA for that single ID rather than scanning
// the filesystem-global quota table. Cleanup still polls the exact retiring ID
// under a bounded context because accounting visibility is not guaranteed by
// the command boundary on every supported kernel.
func xfsInodeGCTriggerCmd() string {
	return "quota -p -i -n -N 0"
}

func createXFSStageDirectory(
	root *os.Root,
	stage xfsStageName,
) (*createdXFSStageDirectory, error) {
	err := root.Mkdir(stage.value(), 0700)
	switch {
	case err == nil:
		return &createdXFSStageDirectory{stage: stage}, nil
	case errors.Is(err, fs.ErrExist):
		return nil, nil
	default:
		return nil, err
	}
}

func (x *xfsVolumeManager) rollbackUndurableStage(
	root *os.Root,
	parent *fsidentity.Directory,
	createdStage *createdXFSStageDirectory,
	createdProjectID *createdXFSProjectIDReservation,
	phase string,
) error {
	return x.rollbackUndurableStageWith(
		createdStage,
		createdProjectID,
		phase,
		root.Remove,
		parent.Sync,
	)
}

func (x *xfsVolumeManager) rollbackUndurableStageWith(
	createdStage *createdXFSStageDirectory,
	createdProjectID *createdXFSProjectIDReservation,
	phase string,
	removeStage func(string) error,
	syncParent func() error,
) error {
	if createdStage == nil {
		return errors.New("refuse xfs stage rollback without mkdir authority")
	}
	if err := removeStage(createdStage.stage.value()); err != nil {
		return fmt.Errorf("%w: %w: remove empty xfs stage after %s failure: %w",
			ErrVolumeMutationRecoveryPending,
			backendidentity.ErrMutationOutcomeAmbiguous,
			phase, err)
	}
	if err := syncParent(); err != nil {
		return fmt.Errorf("%w: sync xfs root after %s rollback: %w",
			backendidentity.ErrMutationOutcomeAmbiguous, phase, err)
	}
	x.releaseCreatedProjectID(createdProjectID)
	return nil
}

func verifyXFSStagePublication(
	root *os.Root,
	stage xfsStageName,
	stagedInfo os.FileInfo,
) (bool, error) {
	stageInfo, stageErr := root.Lstat(stage.value())
	finalInfo, finalErr := root.Lstat(stage.volumeID.value())
	stageExists := stageErr == nil
	finalExists := finalErr == nil
	if stageErr != nil && !errors.Is(stageErr, fs.ErrNotExist) {
		return false, fmt.Errorf("re-read xfs stage after publish: %w", stageErr)
	}
	if finalErr != nil && !errors.Is(finalErr, fs.ErrNotExist) {
		return false, fmt.Errorf("re-read final xfs volume after publish: %w", finalErr)
	}
	if stageExists && (!stageInfo.IsDir() || stageInfo.Mode()&os.ModeSymlink != 0) {
		return false, fmt.Errorf("xfs stage %q changed type during publication", stage.value())
	}
	if finalExists && (!finalInfo.IsDir() || finalInfo.Mode()&os.ModeSymlink != 0) {
		return false, fmt.Errorf("final xfs volume %q is not a real directory", stage.volumeID.value())
	}
	switch {
	case !stageExists && finalExists:
		if !os.SameFile(stagedInfo, finalInfo) {
			return false, fmt.Errorf("final xfs volume %q is not the staged inode", stage.volumeID.value())
		}
		projID, err := readProjectIDFileAtRoot(root, stage.volumeID)
		if err != nil {
			return false, fmt.Errorf("read published xfs project marker: %w", err)
		}
		if projID != stage.projID {
			return false, fmt.Errorf("published xfs marker names project ID %d, expected %d", projID, stage.projID)
		}
		return true, nil
	case stageExists && !finalExists:
		if !os.SameFile(stagedInfo, stageInfo) {
			return false, fmt.Errorf("xfs stage %q changed inode during publication", stage.value())
		}
		return false, nil
	default:
		return false, fmt.Errorf("ambiguous xfs stage publication: stage_exists=%t final_exists=%t",
			stageExists, finalExists)
	}
}

func (x *xfsVolumeManager) publishXFSStage(
	root *os.Root,
	parent *fsidentity.Directory,
	stage xfsStageName,
) error {
	return x.publishXFSStageWith(root, parent, stage, parent.RenameNoReplace)
}

func (x *xfsVolumeManager) publishXFSStageWith(
	root *os.Root,
	parent *fsidentity.Directory,
	stage xfsStageName,
	rename func(oldName, newName string) error,
) error {
	stagedInfo, err := root.Lstat(stage.value())
	if err != nil {
		return fmt.Errorf("stat xfs stage before publication: %w", err)
	}
	if !stagedInfo.IsDir() || stagedInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("xfs stage %q is not a real directory", stage.value())
	}
	publishErr := rename(stage.value(), stage.volumeID.value())
	published, rereadErr := verifyXFSStagePublication(root, stage, stagedInfo)
	if rereadErr != nil {
		ambiguous := fmt.Errorf("%w: verify xfs stage %q publication: %w",
			backendidentity.ErrMutationOutcomeAmbiguous, stage.value(), rereadErr)
		if publishErr == nil {
			return ambiguous
		}
		return errors.Join(fmt.Errorf("publish xfs stage %q: %w", stage.value(), publishErr), ambiguous)
	}
	if !published {
		if publishErr == nil {
			return fmt.Errorf("publish xfs stage %q reported success but the stage remains unpublished", stage.value())
		}
		return fmt.Errorf("publish xfs stage %q: %w", stage.value(), publishErr)
	}
	if err := parent.Sync(); err != nil {
		return fmt.Errorf("%w: xfs stage %q reached its final name but the parent sync failed: %w",
			backendidentity.ErrMutationOutcomeAmbiguous, stage.value(), err)
	}
	return nil
}

func (x *xfsVolumeManager) prepareXFSDeleteStage(
	ctx context.Context,
	root *os.Root,
	parent *fsidentity.Directory,
	stage xfsDeleteStageName,
) error {
	return x.prepareXFSDeleteStageWith(ctx, root, parent, stage, parent.Sync, root.Remove)
}

func (x *xfsVolumeManager) prepareXFSDeleteStageWith(
	ctx context.Context,
	root *os.Root,
	parent *fsidentity.Directory,
	stage xfsDeleteStageName,
	syncParent func() error,
	removeStage func(string) error,
) error {
	if err := root.Mkdir(stage.value(), 0o700); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("create xfs delete-stage %q: %w", stage.value(), err)
		}
		known, ok, knownErr := x.deleteStage(stage.volumeID)
		if knownErr != nil {
			return knownErr
		}
		if !ok || known != stage {
			return fmt.Errorf("%w: refuse untracked preexisting xfs delete-stage %q",
				ErrVolumeMutationRecoveryPending, stage.value())
		}
		if err := inspectXFSDeleteStage(root, stage); err != nil {
			return fmt.Errorf("%w: attest existing xfs delete-stage %q: %w",
				ErrVolumeMutationRecoveryPending, stage.value(), err)
		}
		return nil
	}
	rollbackUndurableDeleteStage := func(cause error) error {
		rollbackErr := removeStage(stage.value())
		if rollbackErr == nil {
			rollbackErr = syncParent()
		}
		if rollbackErr == nil {
			return cause
		}
		return errors.Join(
			cause,
			fmt.Errorf("%w: delete-stage rollback was not durably committed: %w",
				backendidentity.ErrMutationOutcomeAmbiguous, rollbackErr),
		)
	}
	stageRoot, err := openAttestedXFSDeleteStageRoot(root, stage)
	if err != nil {
		return rollbackUndurableDeleteStage(
			fmt.Errorf("open project-reset xfs delete-stage %q: %w", stage.value(), err),
		)
	}
	normalizeErr := x.normalizeXFSDeleteStageProjectWith(ctx, root, parent, stage, stageRoot, syncParent)
	stageCloseErr := stageRoot.Close()
	if err := errors.Join(normalizeErr, stageCloseErr); err != nil {
		return rollbackUndurableDeleteStage(
			fmt.Errorf("normalize xfs delete-stage %q to project 0: %w", stage.value(), err),
		)
	}
	if err := x.rememberDurableDeleteStage(stage); err != nil {
		// The durable name is deliberately retained. Removing it after the
		// fsync would require a second commit protocol; startup can safely
		// recover the exact empty capability if this process cannot record it.
		return fmt.Errorf("%w: %w: record parent-durable xfs delete-stage %q: %w",
			ErrVolumeMutationRecoveryPending,
			backendidentity.ErrMutationOutcomeAmbiguous,
			stage.value(), err)
	}
	return nil
}

func (x *xfsVolumeManager) normalizeXFSDeleteStageProjectWith(
	ctx context.Context,
	root *os.Root,
	parent *fsidentity.Directory,
	stage xfsDeleteStageName,
	stageRoot *os.Root,
	syncParent func() error,
) error {
	stageInfo, err := stageRoot.Stat(".")
	if err != nil {
		return fmt.Errorf("stat xfs delete-stage %q before project reset: %w", stage.value(), err)
	}
	// Repeat this normalization during recovery, not only initial prepare. A
	// crash can replay the mkdir without the following project-ID change, and a
	// configured root may cause that inode to inherit the retiring project.
	resetCtx, cancel := newDetachedBoundedContext(ctx, 30*time.Second)
	resetCmd := xfsProjectResetToDefaultCmd(parent.DisplayPath(stage.value()))
	out, resetErr := exec.CommandContext(resetCtx, "xfs_quota", xfsQuotaArgs(resetCmd, x.mountPoint)...).CombinedOutput()
	if resetErr != nil {
		cancel()
		return fmt.Errorf("reset xfs delete-stage %q to project 0: %w: %s", stage.value(), resetErr, out)
	}
	cancel()
	attr, attrErr := x.projectAttributeReader().ReadProjectAttributes(stageRoot)
	if attrErr != nil {
		return fmt.Errorf("read xfs delete-stage %q project attributes: %w", stage.value(), attrErr)
	}
	// The sibling is an empty, private deletion capability, never an allocation
	// root. Its project ID is the authority; the independent identity and
	// emptiness checks below prevent content from being smuggled beneath it.
	if err := validateXFSDefaultProject(attr); err != nil {
		return fmt.Errorf("attest xfs delete-stage %q project 0: %w", stage.value(), err)
	}
	if err := syncOSRoot(stageRoot); err != nil {
		return fmt.Errorf("sync project-0 xfs delete-stage %q: %w", stage.value(), err)
	}
	if err := syncParent(); err != nil {
		return fmt.Errorf("sync xfs root after normalizing delete-stage %q: %w", stage.value(), err)
	}
	currentInfo, err := root.Lstat(stage.value())
	if err != nil {
		return fmt.Errorf("re-read normalized xfs delete-stage %q: %w", stage.value(), err)
	}
	if !os.SameFile(stageInfo, currentInfo) {
		return fmt.Errorf("xfs delete-stage %q changed identity during project-0 normalization", stage.value())
	}
	entries, err := readXFSRootEntries(stageRoot)
	if err != nil {
		return fmt.Errorf("re-list normalized xfs delete-stage %q: %w", stage.value(), err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("xfs delete-stage %q gained content during project-0 normalization", stage.value())
	}
	return nil
}

type xfsRemoveAll func(root *os.Root, name string) error

type xfsRemove func(root *os.Root, name string) error

// xfsDeletePhaseDeadline reports only an exhausted caller deadline. Destroy
// historically finishes local cleanup after request cancellation, and quota
// clear deliberately uses a detached bounded context; preserving that contract
// avoids abandoning a deletion merely because the client disconnected. An
// aggregate Start deadline is different: once exhausted, recovery must stop
// between filesystem syscalls instead of starting another uninterruptible
// recursive entry operation.
func xfsDeletePhaseDeadline(ctx context.Context) error {
	if err := ctx.Err(); errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

func removeAllFromXFSRoot(root *os.Root, name string) error {
	return root.RemoveAll(name)
}

func removeFromXFSRoot(root *os.Root, name string) error {
	return root.Remove(name)
}

// cleanupXFSDeleteStage continues one parent-durable deletion. Tenant data
// remains under the final managed name; the separately named empty stage keeps
// project/name authority intact on every partial or ambiguous failure.
func (x *xfsVolumeManager) cleanupXFSDeleteStage(
	ctx context.Context,
	stage xfsDeleteStageName,
) error {
	return x.cleanupXFSDeleteStageWith(
		ctx, stage, removeAllFromXFSRoot, removeFromXFSRoot, removeFromXFSRoot,
	)
}

func (x *xfsVolumeManager) cleanupXFSDeleteStageWith(
	ctx context.Context,
	stage xfsDeleteStageName,
	removeContent xfsRemoveAll,
	removeFinal xfsRemove,
	removeStage xfsRemove,
) (err error) {
	defer func() {
		if err != nil && !errors.Is(err, ErrVolumeMutationRecoveryPending) {
			err = fmt.Errorf("%w: xfs delete-stage %q remains authoritative: %w",
				ErrVolumeMutationRecoveryPending, stage.value(), err)
		}
	}()
	root, parent, err := openXFSRootCapabilities(x.dataPath)
	if err != nil {
		return fmt.Errorf("open xfs root for delete-stage cleanup: %w", err)
	}
	defer func() { _ = root.Close() }()
	defer func() { _ = parent.Close() }()

	stageExists := true
	if err := inspectXFSDeleteStage(root, stage); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("attest xfs delete-stage %q for cleanup: %w", stage.value(), err)
		}
		stageExists = false
		if !x.hasDurableDeleteStage(stage) {
			return fmt.Errorf("refuse absent xfs delete-stage %q without live durable removal authority", stage.value())
		}
	}

	x.mu.Lock()
	authorityOK := x.volumeToID[stage.volumeID.value()] == stage.projID &&
		x.activeIDs[stage.projID] == stage.volumeID.value()
	x.mu.Unlock()
	if !authorityOK {
		return fmt.Errorf("refuse to clean xfs delete-stage %q: project-ID authority conflicts with the active map",
			stage.value())
	}

	if !stageExists {
		// The only implementation path that removes a durable delete-stage does
		// so after a successful dquot clear. A live retry after an fsync error only
		// needs to durably commit that already-safe absence.
		if err := parent.Sync(); err != nil {
			return fmt.Errorf("%w: xfs delete-stage %q is absent but the parent sync failed: %w",
				backendidentity.ErrMutationOutcomeAmbiguous, stage.value(), err)
		}
		x.mu.Lock()
		err = x.removeDeleteStageAuthorityLocked(stage)
		x.mu.Unlock()
		return err
	}

	stageRoot, err := openAttestedXFSDeleteStageRoot(root, stage)
	if err != nil {
		return fmt.Errorf("open xfs delete-stage %q for cleanup: %w", stage.value(), err)
	}
	stageInfo, err := stageRoot.Stat(".")
	if err != nil {
		_ = stageRoot.Close()
		return fmt.Errorf("stat opened xfs delete-stage %q: %w", stage.value(), err)
	}
	if err := x.normalizeXFSDeleteStageProjectWith(ctx, root, parent, stage, stageRoot, parent.Sync); err != nil {
		_ = stageRoot.Close()
		return fmt.Errorf("normalize recovered xfs delete-stage %q before cleanup: %w", stage.value(), err)
	}
	if err := xfsDeletePhaseDeadline(ctx); err != nil {
		_ = stageRoot.Close()
		return fmt.Errorf("xfs delete-stage %q deadline exhausted after normalization: %w", stage.value(), err)
	}

	finalExists, err := managedDirectoryExistsAtRoot(root, stage.volumeID)
	if err != nil {
		_ = stageRoot.Close()
		return fmt.Errorf("inspect final volume before xfs delete-stage cleanup: %w", err)
	}
	if finalExists {
		volumeRoot, openErr := openAttestedManagedVolumeRoot(root, stage.volumeID)
		if openErr != nil {
			_ = stageRoot.Close()
			return fmt.Errorf("open xfs volume %q under delete authority: %w", stage.volumeID.value(), openErr)
		}
		volumeInfo, statErr := volumeRoot.Stat(".")
		if statErr != nil {
			_ = volumeRoot.Close()
			_ = stageRoot.Close()
			return fmt.Errorf("stat xfs volume %q under delete authority: %w", stage.volumeID.value(), statErr)
		}
		markerID, markerErr := readProjectIDFileInVolumeRoot(volumeRoot)
		switch {
		case markerErr == nil && markerID != stage.projID:
			_ = volumeRoot.Close()
			_ = stageRoot.Close()
			return fmt.Errorf("xfs delete-stage %q encodes project ID %d but volume marker names %d",
				stage.value(), stage.projID, markerID)
		case markerErr == nil:
		case errors.Is(markerErr, fs.ErrNotExist):
			// A prior RemoveAll may have removed the marker before another child
			// failed. The parent-durable typed sibling is the surviving authority.
		default:
			_ = volumeRoot.Close()
			_ = stageRoot.Close()
			return fmt.Errorf("read marker from xfs volume %q under delete authority: %w",
				stage.volumeID.value(), markerErr)
		}
		entries, readErr := readXFSRootEntries(volumeRoot)
		if readErr != nil {
			_ = volumeRoot.Close()
			_ = stageRoot.Close()
			return fmt.Errorf("list xfs volume %q under delete authority: %w", stage.volumeID.value(), readErr)
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
		for _, entry := range entries {
			if err := xfsDeletePhaseDeadline(ctx); err != nil {
				_ = volumeRoot.Close()
				_ = stageRoot.Close()
				return fmt.Errorf("xfs delete-stage %q deadline exhausted before removing entry %q: %w",
					stage.value(), entry.Name(), err)
			}
			if err := removeContent(volumeRoot, entry.Name()); err != nil && !errors.Is(err, fs.ErrNotExist) {
				_ = volumeRoot.Close()
				_ = stageRoot.Close()
				return fmt.Errorf("remove content %q from xfs volume %q under delete-stage %q: %w",
					entry.Name(), stage.volumeID.value(), stage.value(), err)
			}
		}
		if err := xfsDeletePhaseDeadline(ctx); err != nil {
			_ = volumeRoot.Close()
			_ = stageRoot.Close()
			return fmt.Errorf("xfs delete-stage %q deadline exhausted after recursive cleanup: %w", stage.value(), err)
		}
		if err := syncOSRoot(volumeRoot); err != nil {
			_ = volumeRoot.Close()
			_ = stageRoot.Close()
			return fmt.Errorf("sync emptied xfs volume %q: %w", stage.volumeID.value(), err)
		}
		remaining, readErr := readXFSRootEntries(volumeRoot)
		if readErr != nil {
			_ = volumeRoot.Close()
			_ = stageRoot.Close()
			return fmt.Errorf("re-list emptied xfs volume %q: %w", stage.volumeID.value(), readErr)
		}
		if len(remaining) != 0 {
			_ = volumeRoot.Close()
			_ = stageRoot.Close()
			return fmt.Errorf("xfs volume %q gained content during recursive cleanup", stage.volumeID.value())
		}
		currentVolumeInfo, rereadErr := root.Lstat(stage.volumeID.value())
		if rereadErr != nil {
			_ = volumeRoot.Close()
			_ = stageRoot.Close()
			return fmt.Errorf("%w: re-read emptied xfs volume %q: %w",
				backendidentity.ErrMutationOutcomeAmbiguous, stage.volumeID.value(), rereadErr)
		}
		if !os.SameFile(volumeInfo, currentVolumeInfo) {
			_ = volumeRoot.Close()
			_ = stageRoot.Close()
			return fmt.Errorf("xfs volume %q changed identity during recursive cleanup", stage.volumeID.value())
		}
		if err := volumeRoot.Close(); err != nil {
			_ = stageRoot.Close()
			return fmt.Errorf("close emptied xfs volume %q: %w", stage.volumeID.value(), err)
		}
		if err := xfsDeletePhaseDeadline(ctx); err != nil {
			_ = stageRoot.Close()
			return fmt.Errorf("xfs delete-stage %q deadline exhausted before final-root removal: %w", stage.value(), err)
		}
		removeErr := removeFinal(root, stage.volumeID.value())
		_, rereadErr = root.Lstat(stage.volumeID.value())
		switch {
		case rereadErr == nil:
			_ = stageRoot.Close()
			if removeErr == nil {
				return fmt.Errorf("remove empty xfs volume %q reported success but it remains", stage.volumeID.value())
			}
			return fmt.Errorf("remove empty xfs volume %q under delete-stage %q: %w",
				stage.volumeID.value(), stage.value(), removeErr)
		case !errors.Is(rereadErr, fs.ErrNotExist):
			_ = stageRoot.Close()
			return errors.Join(
				fmt.Errorf("remove empty xfs volume %q: %w", stage.volumeID.value(), removeErr),
				fmt.Errorf("%w: re-read xfs volume after root removal: %w",
					backendidentity.ErrMutationOutcomeAmbiguous, rereadErr),
			)
		}
	}
	if err := xfsDeletePhaseDeadline(ctx); err != nil {
		_ = stageRoot.Close()
		return fmt.Errorf("xfs delete-stage %q deadline exhausted before absence commit: %w", stage.value(), err)
	}

	// Persist the managed volume's absence while the independent typed sibling
	// remains durable. A crash before quota clear can therefore always resume by
	// scanning the sibling, even if recursive deletion removed the inner marker.
	if err := parent.Sync(); err != nil {
		_ = stageRoot.Close()
		return fmt.Errorf("%w: xfs volume %q is absent but the parent sync failed: %w",
			backendidentity.ErrMutationOutcomeAmbiguous, stage.volumeID.value(), err)
	}
	currentStageInfo, err := root.Lstat(stage.value())
	if err != nil {
		_ = stageRoot.Close()
		return fmt.Errorf("re-read xfs delete-stage %q before quota clear: %w", stage.value(), err)
	}
	if !os.SameFile(stageInfo, currentStageInfo) {
		_ = stageRoot.Close()
		return fmt.Errorf("xfs delete-stage %q changed identity before quota clear", stage.value())
	}
	stageEntries, err := readXFSRootEntries(stageRoot)
	if err != nil {
		_ = stageRoot.Close()
		return fmt.Errorf("re-list xfs delete-stage %q before quota clear: %w", stage.value(), err)
	}
	if len(stageEntries) != 0 {
		_ = stageRoot.Close()
		return fmt.Errorf("xfs delete-stage %q gained content before quota clear", stage.value())
	}
	if err := xfsDeletePhaseDeadline(ctx); err != nil {
		_ = stageRoot.Close()
		return fmt.Errorf("xfs delete-stage %q deadline exhausted before quota proof: %w", stage.value(), err)
	}

	// The sibling is not tagged with stage.projID, so zero means no linked or
	// open-unlinked project inode remains. The higher-level teardown gate also
	// requires containers to be stopped; this is the XFS defense in depth.
	clearCtx, cancel := newDetachedBoundedContext(ctx, 30*time.Second)
	blocks, inodes, usageErr := x.waitForZeroProjectQuotaUsage(clearCtx, stage.projID)
	if usageErr != nil && blocks == 0 && inodes == 0 {
		cancel()
		_ = stageRoot.Close()
		return fmt.Errorf("prove zero usage for xfs delete-stage %q: %w", stage.value(), usageErr)
	}
	if blocks != 0 || inodes != 0 {
		cancel()
		_ = stageRoot.Close()
		refusal := fmt.Errorf(
			"refuse to clear xfs project quota for delete-stage %q: project %d still uses %d blocks and %d inodes",
			stage.value(), stage.projID, blocks, inodes,
		)
		if usageErr != nil {
			return errors.Join(refusal, fmt.Errorf("wait for pending xfs inode cleanup: %w", usageErr))
		}
		return refusal
	}
	clearCmd := xfsLimitClearCmd(stage.projID)
	out, clearErr := exec.CommandContext(clearCtx, "xfs_quota", xfsQuotaArgs(clearCmd, x.mountPoint)...).CombinedOutput()
	cancel()
	if clearErr != nil {
		_ = stageRoot.Close()
		volumeQuotaClearFailedTotal.Inc()
		return fmt.Errorf("clear xfs project quota for delete-stage %q (id=%d): %w: %s",
			stage.value(), stage.projID, clearErr, out)
	}

	stageEntries, err = readXFSRootEntries(stageRoot)
	if err != nil {
		_ = stageRoot.Close()
		return fmt.Errorf("re-list xfs delete-stage %q after quota clear: %w", stage.value(), err)
	}
	if len(stageEntries) != 0 {
		_ = stageRoot.Close()
		return fmt.Errorf("xfs delete-stage %q gained content after quota clear", stage.value())
	}
	if err := stageRoot.Close(); err != nil {
		return fmt.Errorf("close xfs delete-stage %q after quota clear: %w", stage.value(), err)
	}
	removeErr := removeStage(root, stage.value())
	_, rereadErr := root.Lstat(stage.value())
	switch {
	case rereadErr == nil:
		if removeErr == nil {
			return fmt.Errorf("remove xfs delete-stage %q reported success but the directory remains", stage.value())
		}
		return fmt.Errorf("remove xfs delete-stage %q after quota clear: %w", stage.value(), removeErr)
	case !errors.Is(rereadErr, fs.ErrNotExist):
		return errors.Join(
			fmt.Errorf("remove xfs delete-stage %q: %w", stage.value(), removeErr),
			fmt.Errorf("%w: re-read xfs delete-stage after removal: %w",
				backendidentity.ErrMutationOutcomeAmbiguous, rereadErr),
		)
	}
	if err := parent.Sync(); err != nil {
		return fmt.Errorf("%w: xfs delete-stage %q is absent but the parent sync failed: %w",
			backendidentity.ErrMutationOutcomeAmbiguous, stage.value(), err)
	}

	x.mu.Lock()
	err = x.removeDeleteStageAuthorityLocked(stage)
	x.mu.Unlock()
	return err
}

// cleanupXFSStage consumes an exact, previously registered staging capability.
// It clears the dquot while the typed directory still records which project ID
// is being retired, then removes only an empty-or-marker-only stage. Every
// failure retains the stage and both map entries for an exact retry.
func (x *xfsVolumeManager) cleanupXFSStage(ctx context.Context, stage xfsStageName) (err error) {
	defer func() {
		if err != nil && !errors.Is(err, ErrVolumeMutationRecoveryPending) {
			err = fmt.Errorf("%w: xfs create-stage %q remains authoritative: %w",
				ErrVolumeMutationRecoveryPending, stage.value(), err)
		}
	}()
	root, parent, err := openXFSRootCapabilities(x.dataPath)
	if err != nil {
		return fmt.Errorf("open xfs root for stage cleanup: %w", err)
	}
	defer func() { _ = root.Close() }()
	defer func() { _ = parent.Close() }()

	finalExists, err := managedDirectoryExistsAtRoot(root, stage.volumeID)
	if err != nil {
		return fmt.Errorf("inspect final volume before xfs stage cleanup: %w", err)
	}
	if finalExists {
		return fmt.Errorf("refuse to clean xfs stage %q while final volume %q exists",
			stage.value(), stage.volumeID.value())
	}
	cleanupProof, err := inspectXFSStageForCleanup(root, stage)
	if err != nil {
		return fmt.Errorf("attest xfs stage %q for cleanup: %w", stage.value(), err)
	}

	x.mu.Lock()
	authorityOK := x.volumeToID[stage.volumeID.value()] == stage.projID &&
		x.activeIDs[stage.projID] == stage.volumeID.value()
	x.mu.Unlock()
	if !authorityOK {
		return fmt.Errorf("refuse to clean xfs stage %q: project-ID authority conflicts with the active map",
			stage.value())
	}

	clearCtx, cancel := newDetachedBoundedContext(ctx, 30*time.Second)
	defer cancel()
	clearCmd := xfsLimitClearCmd(stage.projID)
	if out, err := exec.CommandContext(clearCtx, "xfs_quota", xfsQuotaArgs(clearCmd, x.mountPoint)...).CombinedOutput(); err != nil {
		volumeQuotaClearFailedTotal.Inc()
		return fmt.Errorf("clear xfs project quota for stage %q (id=%d): %w: %s",
			stage.value(), stage.projID, err, out)
	}

	// Re-attest after the external command. An operator or second process must
	// not turn a proven empty stage into deletion authority for arbitrary data.
	cleanupProofAfterClear, err := inspectXFSStageForCleanup(root, stage)
	if err != nil {
		return fmt.Errorf("re-attest xfs stage %q after quota clear: %w", stage.value(), err)
	}
	if !sameXFSStageCleanupProof(cleanupProof, cleanupProofAfterClear) {
		return fmt.Errorf("xfs stage %q changed identity or marker state during quota cleanup", stage.value())
	}
	finalExists, err = managedDirectoryExistsAtRoot(root, stage.volumeID)
	if err != nil {
		return fmt.Errorf("re-inspect final volume after xfs stage quota clear: %w", err)
	}
	if finalExists {
		return fmt.Errorf("final xfs volume %q appeared during stage cleanup", stage.volumeID.value())
	}

	if cleanupProof.markerPresent {
		stageRoot, err := openAttestedXFSStageRoot(root, stage)
		if err != nil {
			return fmt.Errorf("reopen marked xfs stage %q for cleanup: %w", stage.value(), err)
		}
		if err := stageRoot.Remove(projectIDFile); err != nil {
			_ = stageRoot.Close()
			return fmt.Errorf("remove xfs stage %q marker: %w", stage.value(), err)
		}
		if err := syncOSRoot(stageRoot); err != nil {
			_ = stageRoot.Close()
			return fmt.Errorf("sync xfs stage %q after marker removal: %w", stage.value(), err)
		}
		if err := stageRoot.Close(); err != nil {
			return fmt.Errorf("close xfs stage %q after marker removal: %w", stage.value(), err)
		}
	}
	if err := root.Remove(stage.value()); err != nil {
		return fmt.Errorf("remove empty xfs stage %q: %w", stage.value(), err)
	}
	if err := parent.Sync(); err != nil {
		return fmt.Errorf("sync xfs root after removing stage %q: %w", stage.value(), err)
	}

	x.mu.Lock()
	err = x.removeStageAuthorityLocked(stage)
	x.mu.Unlock()
	return err
}

func (x *xfsVolumeManager) failDurableXFSStageCreate(
	ctx context.Context,
	stage xfsStageName,
	cause error,
) error {
	cleanupErr := x.cleanupXFSStage(ctx, stage)
	if cleanupErr == nil {
		return cause
	}
	return errors.Join(
		cause,
		fmt.Errorf("%w: compensate failed xfs create while retaining stage %q: %w",
			ErrVolumeMutationRecoveryPending, stage.value(), cleanupErr),
	)
}

func (x *xfsVolumeManager) reuseExistingVolume(
	ctx context.Context,
	root *os.Root,
	volumeID managedVolumeName,
	quota string,
	ihard int64,
	sizeMB int64,
) (string, error) {
	dirPath := volumeID.hostPath(x.dataPath)
	exists, err := managedDirectoryExistsAtRoot(root, volumeID)
	if err != nil {
		return "", fmt.Errorf("inspect existing xfs volume %s: %w", dirPath, err)
	}
	if !exists {
		return "", fmt.Errorf("existing xfs volume %s disappeared before verification", dirPath)
	}
	projID, err := readProjectIDFileAtRoot(root, volumeID)
	if err != nil {
		return "", fmt.Errorf("read project ID marker for existing volume %s: %w", dirPath, err)
	}

	// Register the marker's authority before issuing any quota command.
	x.mu.Lock()
	registerErr := x.registerProjectIDLocked(volumeID.value(), projID)
	x.mu.Unlock()
	if registerErr != nil {
		return "", fmt.Errorf("register existing xfs volume authority: %w", registerErr)
	}

	// A prior process can have crashed after writing the marker but before
	// tagging the inode. Reapply the project association before its limit so an
	// existing recovery directory cannot be returned as unquotaed storage.
	cmd := xfsProjectSetupCmd(dirPath, projID)
	if out, err := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(cmd, x.mountPoint)...).CombinedOutput(); err != nil {
		return "", fmt.Errorf("xfs_quota project setup on existing %s (id=%d): %w: %s", dirPath, projID, err, out)
	}
	cmd = xfsLimitCmd(projID, quota, ihard)
	if out, err := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(cmd, x.mountPoint)...).CombinedOutput(); err != nil {
		return "", fmt.Errorf("xfs_quota limit on existing %s (id=%d, quota=%s): %w: %s", dirPath, projID, quota, err, out)
	}
	x.logger.Debug("reusing existing xfs quota directory", "path", dirPath, "project_id", projID, "quota_mb", sizeMB)
	return dirPath, nil
}

func (x *xfsVolumeManager) Create(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
	volumeID, err := parseManagedVolumeName(id)
	if err != nil {
		return "", false, fmt.Errorf("validate xfs volume ID for create: %w", err)
	}
	dirPath := volumeID.hostPath(x.dataPath)
	quota := fmt.Sprintf("%dm", sizeMB)
	ihard := inodeHardLimit(sizeMB, x.minAvgFileBytes)
	root, parent, err := openXFSRootCapabilities(x.dataPath)
	if err != nil {
		return "", false, fmt.Errorf("open xfs volume root %s: %w", x.dataPath, err)
	}
	defer func() { _ = root.Close() }()
	defer func() { _ = parent.Close() }()
	if deleteStage, known, stageErr := x.deleteStage(volumeID); stageErr != nil {
		return "", false, stageErr
	} else if known {
		return "", false, fmt.Errorf("%w: refuse to create xfs volume %q while delete-stage %q is pending",
			ErrVolumeMutationRecoveryPending, volumeID.value(), deleteStage.value())
	}

	finalExists, err := managedDirectoryExistsAtRoot(root, volumeID)
	if err != nil {
		return "", false, fmt.Errorf("inspect final xfs volume %s: %w", dirPath, err)
	}
	if finalExists {
		if stage, known := x.durableStage(volumeID); known {
			stageExists, statErr := root.Lstat(stage.value())
			if statErr == nil && stageExists != nil {
				return "", false, fmt.Errorf("xfs final volume %q and durable stage %q coexist",
					volumeID.value(), stage.value())
			}
			if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
				return "", false, fmt.Errorf("stat durable xfs stage %q: %w", stage.value(), statErr)
			}
		}
		resultPath, reuseErr := x.reuseExistingVolume(ctx, root, volumeID, quota, ihard, sizeMB)
		if reuseErr == nil {
			if stage, known := x.durableStage(volumeID); known {
				x.forgetDurableStage(stage)
			}
		}
		return resultPath, false, reuseErr
	}

	// Reserve the ID before naming the stage; the name is the durable allocation
	// record once the parent directory is synced.
	projID, createdProjectID, err := x.reserveProjectID(volumeID.value())
	if err != nil {
		return "", false, err
	}
	stage, err := newXFSStageName(projID, volumeID)
	if err != nil {
		x.releaseCreatedProjectID(createdProjectID)
		return "", false, err
	}
	createdStage, err := createXFSStageDirectory(root, stage)
	if err != nil {
		x.releaseCreatedProjectID(createdProjectID)
		return "", false, fmt.Errorf("create xfs stage %s: %w", stage.hostPath(x.dataPath), err)
	}
	if createdStage == nil {
		known, ok := x.durableStage(volumeID)
		if !ok || known != stage {
			x.releaseCreatedProjectID(createdProjectID)
			return "", false, fmt.Errorf("%w: refuse untracked preexisting xfs stage %q",
				ErrVolumeMutationRecoveryPending, stage.value())
		}
	} else {
		if err := parent.Sync(); err != nil {
			rollbackErr := x.rollbackUndurableStage(root, parent, createdStage, createdProjectID, "parent_sync")
			return "", false, errors.Join(
				fmt.Errorf("sync xfs root after creating stage %q: %w", stage.value(), err),
				rollbackErr,
			)
		}
		if err := x.rememberDurableStage(stage); err != nil {
			return "", false, fmt.Errorf("%w: %w: record parent-durable xfs create-stage %q: %w",
				ErrVolumeMutationRecoveryPending,
				backendidentity.ErrMutationOutcomeAmbiguous,
				stage.value(), err)
		}
	}

	if err := ensureXFSStageMarker(root, stage); err != nil {
		return "", false, x.failDurableXFSStageCreate(
			ctx, stage, fmt.Errorf("prepare xfs stage marker %q: %w", stage.value(), err),
		)
	}
	stagePath := parent.DisplayPath(stage.value())
	cmd := xfsProjectSetupCmd(stagePath, projID)
	if out, err := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(cmd, x.mountPoint)...).CombinedOutput(); err != nil {
		return "", false, x.failDurableXFSStageCreate(
			ctx,
			stage,
			fmt.Errorf("xfs_quota project setup for stage %s (id=%d): %w: %s", stagePath, projID, err, out),
		)
	}
	cmd = xfsLimitCmd(projID, quota, ihard)
	if out, err := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(cmd, x.mountPoint)...).CombinedOutput(); err != nil {
		return "", false, x.failDurableXFSStageCreate(
			ctx,
			stage,
			fmt.Errorf("xfs_quota limit for stage %s (id=%d, quota=%s): %w: %s", stagePath, projID, quota, err, out),
		)
	}
	if _, err := inspectXFSStage(root, stage); err != nil {
		return "", false, x.failDurableXFSStageCreate(
			ctx, stage, fmt.Errorf("re-attest xfs stage before publication: %w", err),
		)
	}
	if err := x.publishXFSStage(root, parent, stage); err != nil {
		return "", false, x.failDurableXFSStageCreate(ctx, stage, err)
	}
	x.forgetDurableStage(stage)

	x.logger.Debug("created xfs project quota directory", "path", dirPath, "project_id", projID, "quota_mb", sizeMB)
	return dirPath, true, nil
}

// EnsureQuota re-applies the project tag + block (bhard) and inode (ihard)
// limits to an existing volume, recovering the projID from its
// .fred-project-id marker. It re-tags the inode (project -s) — which heals a
// volume left untagged by a pre-CAP_SYS_ADMIN daemon (ENG-454) — then
// re-applies the limit. No-op if the directory is absent (never creates), so
// a concurrent deprovision is never resurrected.
func (x *xfsVolumeManager) EnsureQuota(ctx context.Context, id string, sizeMB int64) error {
	volumeID, err := parseManagedVolumeName(id)
	if err != nil {
		return fmt.Errorf("validate xfs volume ID for quota: %w", err)
	}
	dirPath := volumeID.hostPath(x.dataPath)
	root, err := os.OpenRoot(x.dataPath)
	if err != nil {
		return fmt.Errorf("open xfs volume root %s: %w", x.dataPath, err)
	}
	defer func() { _ = root.Close() }()
	info, err := root.Lstat(volumeID.value())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil // vanished (e.g. concurrent deprovision): nothing to enforce
		}
		return fmt.Errorf("stat volume dir %s: %w", dirPath, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("existing xfs volume %s is not a real directory", dirPath)
	}
	projID, err := readProjectIDFileAtRoot(root, volumeID)
	if err != nil {
		return fmt.Errorf("read project ID marker for %s: %w", dirPath, err)
	}

	x.mu.Lock()
	registerErr := x.registerProjectIDLocked(volumeID.value(), projID)
	x.mu.Unlock()
	if registerErr != nil {
		return fmt.Errorf("register existing xfs volume authority: %w", registerErr)
	}

	tagCmd := xfsProjectSetupCmd(dirPath, projID)
	if out, err := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(tagCmd, x.mountPoint)...).CombinedOutput(); err != nil {
		return fmt.Errorf("xfs_quota project re-tag for %s (id=%d): %w: %s", dirPath, projID, err, out)
	}
	limitCmd := xfsLimitCmd(projID, fmt.Sprintf("%dm", sizeMB), inodeHardLimit(sizeMB, x.minAvgFileBytes))
	if out, err := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(limitCmd, x.mountPoint)...).CombinedOutput(); err != nil {
		return fmt.Errorf("xfs_quota limit for %s (id=%d): %w: %s", dirPath, projID, err, out)
	}
	x.logger.Debug("re-applied xfs project quota", "path", dirPath, "project_id", projID, "quota_mb", sizeMB)
	return nil
}

func (x *xfsVolumeManager) Destroy(ctx context.Context, id string) error {
	return x.destroyWith(ctx, id, removeAllFromXFSRoot)
}

func (x *xfsVolumeManager) destroyWith(ctx context.Context, id string, removeAll xfsRemoveAll) error {
	volumeID, err := parseManagedVolumeName(id)
	if err != nil {
		return fmt.Errorf("validate xfs volume ID for destroy: %w", err)
	}
	dirPath := volumeID.hostPath(x.dataPath)
	root, parent, err := openXFSRootCapabilities(x.dataPath)
	if err != nil {
		return fmt.Errorf("open xfs volume root %s: %w", x.dataPath, err)
	}
	defer func() { _ = root.Close() }()
	defer func() { _ = parent.Close() }()
	exists, err := managedDirectoryExistsAtRoot(root, volumeID)
	if err != nil {
		return fmt.Errorf("stat xfs volume %s for destroy: %w", dirPath, err)
	}
	if deleteStage, known, stageErr := x.deleteStage(volumeID); stageErr != nil {
		return stageErr
	} else if known {
		return x.cleanupXFSDeleteStageWith(
			ctx, deleteStage, removeAll, removeFromXFSRoot, removeFromXFSRoot,
		)
	}
	if stage, known := x.durableStage(volumeID); known {
		stageInfo, statErr := root.Lstat(stage.value())
		switch {
		case statErr == nil && exists:
			return fmt.Errorf("refuse to destroy xfs volume %q while durable stage %q coexists",
				volumeID.value(), stage.value())
		case statErr == nil:
			if !stageInfo.IsDir() || stageInfo.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("durable xfs stage %q is not a real directory", stage.value())
			}
			return x.cleanupXFSStage(ctx, stage)
		case !errors.Is(statErr, fs.ErrNotExist):
			return fmt.Errorf("stat durable xfs stage %q for destroy: %w", stage.value(), statErr)
		case !exists:
			return fmt.Errorf("durable xfs stage %q and final volume %q are both absent; refusing ambiguous cleanup",
				stage.value(), volumeID.value())
		}
	}
	if recovered, known := x.recoveredStage(volumeID); known {
		return fmt.Errorf("xfs volume %q has startup-recovered stage %q; startup recovery must clean it",
			volumeID.value(), recovered.stage.value())
	}

	// Resolve the projID before recursive removal: its .fred-project-id marker is
	// the original authority. Deletion starts only after a separate typed sibling
	// has made that project/name authority parent-durable.
	if exists {
		// A canonical-looking plain directory is not sufficient deletion
		// authority. Require the same real-directory and marker proof used by
		// startup inventory before recursively removing any tenant data. Destroy
		// historically did not honor caller cancellation for local removal, so
		// preserve that behavior while retaining context values for diagnostics.
		if err := x.AttestManagedVolume(context.WithoutCancel(ctx), volumeID); err != nil {
			return fmt.Errorf("attest xfs volume %s for destroy: %w", dirPath, err)
		}
	}
	projID, hasProjID := x.resolveProjectID(root, volumeID)
	if exists && !hasProjID {
		return fmt.Errorf("refuse to destroy xfs volume %s: project ID authority conflicts with the active map", dirPath)
	}

	if exists {
		// Register the exact marker before minting the durable delete capability.
		// Validate normally populated both maps at startup; this also keeps direct,
		// isolated manager use safe and makes a partial deletion retryable in-process.
		x.mu.Lock()
		registerErr := x.registerProjectIDLocked(volumeID.value(), projID)
		x.mu.Unlock()
		if registerErr != nil {
			return fmt.Errorf("register xfs volume authority before destroy: %w", registerErr)
		}
	} else if !hasProjID {
		// Distinguish an idempotent already-gone volume from a corrupt one-sided
		// in-memory authority map. Silently dropping the latter could make its
		// project ID reusable while an open-unlinked inode is still charged to it.
		x.mu.Lock()
		_, mapped, mapErr := x.mappedProjectIDLocked(volumeID.value())
		x.mu.Unlock()
		if mapErr != nil {
			return fmt.Errorf("validate absent xfs volume authority before destroy: %w", mapErr)
		}
		if mapped {
			return fmt.Errorf("absent xfs volume %q has project-ID authority that could not be resolved",
				volumeID.value())
		}
		x.logger.Debug("xfs quota directory already absent", "path", dirPath)
		return nil
	}

	// Even if the final directory disappeared out-of-band before this call, the
	// live collision-checked map still proves which dquot belongs to the volume.
	// Persist that authority into the same typed sibling before checking usage.
	// This forbids the old unsafe shortcut of clearing a limit merely because the
	// namespace is absent: an open-unlinked project inode can still consume and
	// grow after its pathname is gone.
	deleteStage, stageErr := newXFSDeleteStageName(projID, volumeID)
	if stageErr != nil {
		return stageErr
	}
	if stageErr := x.prepareXFSDeleteStage(ctx, root, parent, deleteStage); stageErr != nil {
		return stageErr
	}
	return x.cleanupXFSDeleteStageWith(
		ctx, deleteStage, removeAll, removeFromXFSRoot, removeFromXFSRoot,
	)
}

func (x *xfsVolumeManager) List() ([]string, error) {
	return x.rootWatch.list(x.dataPath)
}

func (x *xfsVolumeManager) ListForProof(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return x.rootWatch.listForProof(x.dataPath)
}

// AttestManagedVolume proves that the exact XFS volume entry is a real
// directory with a descriptor-read, regular, non-symlink project marker and a
// nonzero project ID. Validate rebuilds the in-memory map from the same marker;
// this narrower method lets the storage-identity proof repeat the check under
// its caller-owned deadline immediately before publication.
func (x *xfsVolumeManager) AttestManagedVolume(ctx context.Context, name managedVolumeName) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := os.OpenRoot(x.dataPath)
	if err != nil {
		return fmt.Errorf("open xfs volume root %s: %w", x.dataPath, err)
	}
	defer func() { _ = root.Close() }()
	exists, err := managedDirectoryExistsAtRoot(root, name)
	if err != nil {
		return fmt.Errorf("stat xfs volume %s: %w", name.hostPath(x.dataPath), err)
	}
	if !exists {
		return fmt.Errorf("xfs volume %s does not exist", name.hostPath(x.dataPath))
	}
	markerID, markerErr := readProjectIDFileAtRoot(root, name)
	deleteStage, deleting, stageErr := x.deleteStage(name)
	if stageErr != nil {
		return stageErr
	}
	if deleting {
		if err := inspectXFSDeleteStage(root, deleteStage); err != nil {
			return fmt.Errorf("attest delete-stage for xfs volume %s: %w", name.hostPath(x.dataPath), err)
		}
		x.mu.Lock()
		authorityOK := x.volumeToID[name.value()] == deleteStage.projID &&
			x.activeIDs[deleteStage.projID] == name.value()
		x.mu.Unlock()
		if !authorityOK {
			return fmt.Errorf("xfs delete-stage %q conflicts with active project-ID authority", deleteStage.value())
		}
		switch {
		case markerErr == nil && markerID == deleteStage.projID:
			return nil
		case errors.Is(markerErr, fs.ErrNotExist):
			// Existing-identity startup must be able to reach explicit recovery
			// after a marker-first partial RemoveAll. New/adopt/preflight reject
			// this same typed stage through RequireNoInterruptedVolumeMutations.
			return nil
		case markerErr == nil:
			return fmt.Errorf("xfs delete-stage %q names project ID %d but marker names %d",
				deleteStage.value(), deleteStage.projID, markerID)
		default:
			return fmt.Errorf("read xfs project ID marker for deleting volume %s: %w",
				name.hostPath(x.dataPath), markerErr)
		}
	}
	if markerErr != nil {
		return fmt.Errorf("read xfs project ID marker for %s: %w", name.hostPath(x.dataPath), markerErr)
	}
	return nil
}

func (x *xfsVolumeManager) mappedProjectIDLocked(volumeID string) (uint32, bool, error) {
	reverseID, reverseOK := x.volumeToID[volumeID]
	forwardID := uint32(0)
	forwardCount := 0
	for projID, owner := range x.activeIDs {
		if owner == volumeID {
			forwardID = projID
			forwardCount++
		}
	}
	if forwardCount > 1 {
		return 0, false, fmt.Errorf("xfs volume %q has multiple forward project-ID mappings", volumeID)
	}
	if reverseOK != (forwardCount == 1) {
		return 0, false, fmt.Errorf("xfs volume %q has one-sided project-ID authority", volumeID)
	}
	if !reverseOK {
		return 0, false, nil
	}
	if reverseID == 0 || forwardID != reverseID || x.activeIDs[reverseID] != volumeID {
		return 0, false, fmt.Errorf("xfs volume %q has inconsistent project-ID authority", volumeID)
	}
	return reverseID, true, nil
}

// commitXFSVolumeRename crosses the filesystem publication boundary for one
// already-attested source. A rename syscall error is not by itself proof that
// the namespace stayed unchanged: re-read both names and accept a committed
// result only when the destination is the exact source inode with the expected
// marker. Once committed, failure to verify or sync the parent leaves crash
// durability unknown and is therefore a typed ambiguous outcome.
func commitXFSVolumeRename(
	root *os.Root,
	oldVolume, newVolume managedVolumeName,
	projID uint32,
	sourceInfo os.FileInfo,
	rename func(string, string) error,
	syncParent func() error,
) error {
	renameErr := rename(oldVolume.value(), newVolume.value())
	joinAmbiguity := func(ambiguous error) error {
		if renameErr == nil {
			return ambiguous
		}
		return errors.Join(
			fmt.Errorf("rename xfs volume %q to %q: %w", oldVolume.value(), newVolume.value(), renameErr),
			ambiguous,
		)
	}

	oldInfo, oldErr := root.Lstat(oldVolume.value())
	newInfo, newErr := root.Lstat(newVolume.value())
	oldExists := oldErr == nil
	newExists := newErr == nil
	if oldErr != nil && !errors.Is(oldErr, fs.ErrNotExist) {
		return joinAmbiguity(fmt.Errorf("%w: re-read old xfs volume after rename: %w",
			backendidentity.ErrMutationOutcomeAmbiguous, oldErr))
	}
	if newErr != nil && !errors.Is(newErr, fs.ErrNotExist) {
		return joinAmbiguity(fmt.Errorf("%w: re-read destination xfs volume after rename: %w",
			backendidentity.ErrMutationOutcomeAmbiguous, newErr))
	}

	switch {
	case oldExists && !newExists:
		if !oldInfo.IsDir() || oldInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(sourceInfo, oldInfo) {
			return fmt.Errorf("%w: xfs rename source %q changed identity while the destination remained absent",
				backendidentity.ErrMutationOutcomeAmbiguous, oldVolume.value())
		}
		if renameErr == nil {
			return fmt.Errorf("%w: xfs rename %q to %q reported success but the source remains",
				backendidentity.ErrMutationOutcomeAmbiguous, oldVolume.value(), newVolume.value())
		}
		return fmt.Errorf("rename xfs volume %q to %q: %w", oldVolume.value(), newVolume.value(), renameErr)
	case !oldExists && newExists:
		if !newInfo.IsDir() || newInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(sourceInfo, newInfo) {
			return fmt.Errorf("%w: xfs rename destination %q is not the attested source inode",
				backendidentity.ErrMutationOutcomeAmbiguous, newVolume.value())
		}
		newProjID, err := readProjectIDFileAtRoot(root, newVolume)
		if err != nil {
			return fmt.Errorf("%w: attest renamed xfs volume: %w",
				backendidentity.ErrMutationOutcomeAmbiguous, err)
		}
		if newProjID != projID {
			return fmt.Errorf("%w: renamed xfs marker changed from project ID %d to %d",
				backendidentity.ErrMutationOutcomeAmbiguous, projID, newProjID)
		}
	default:
		return joinAmbiguity(fmt.Errorf("%w: ambiguous xfs rename result: old_exists=%t new_exists=%t",
			backendidentity.ErrMutationOutcomeAmbiguous, oldExists, newExists))
	}

	if err := syncParent(); err != nil {
		return fmt.Errorf("%w: xfs volume %q reached destination %q but the parent sync failed: %w",
			backendidentity.ErrMutationOutcomeAmbiguous, oldVolume.value(), newVolume.value(), err)
	}
	return nil
}

// RenameVolume renames the volume directory and updates the in-memory
// projectID maps so subsequent Create/Destroy calls on the new name
// resolve to the same XFS project ID (preserving the quota and the
// .fred-project-id marker file inside the directory).
//
// xfs project metadata is keyed by inode and survives a directory
// rename, so no xfs_quota reapplication is needed. The maps are updated
// atomically under x.mu after a successful descriptor-rooted rename so a
// concurrent Create on the new name cannot observe an inconsistent state.
func (x *xfsVolumeManager) RenameVolume(ctx context.Context, oldName, newName string) error {
	oldVolume, err := parseManagedVolumeName(oldName)
	if err != nil {
		return fmt.Errorf("validate old xfs volume ID for rename: %w", err)
	}
	newVolume, err := parseManagedVolumeName(newName)
	if err != nil {
		return fmt.Errorf("validate new xfs volume ID for rename: %w", err)
	}
	if oldVolume == newVolume {
		return errors.New("old and new xfs volume names must differ")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	root, parent, err := openXFSRootCapabilities(x.dataPath)
	if err != nil {
		return fmt.Errorf("open xfs volume root %s: %w", x.dataPath, err)
	}
	defer func() { _ = root.Close() }()
	defer func() { _ = parent.Close() }()
	// The lock covers stage refusal, prevalidation, the no-replace rename and
	// the checked map transition. No local Create/Destroy can publish map
	// authority for either name while this decision is in progress.
	x.mu.Lock()
	defer x.mu.Unlock()
	for _, candidate := range []managedVolumeName{oldVolume, newVolume} {
		if pending := x.pendingMutationNamesLocked(candidate); len(pending) != 0 {
			return fmt.Errorf("refuse to rename xfs volume %q while volume mutation %s is pending",
				candidate.value(), strings.Join(pending, ", "))
		}
	}
	oldExists, oldStatErr := managedDirectoryExistsAtRoot(root, oldVolume)
	newExists, newStatErr := managedDirectoryExistsAtRoot(root, newVolume)
	if oldStatErr != nil {
		return fmt.Errorf("stat old xfs volume %s: %w", oldVolume.hostPath(x.dataPath), oldStatErr)
	}
	if newStatErr != nil {
		return fmt.Errorf("stat new xfs volume %s: %w", newVolume.hostPath(x.dataPath), newStatErr)
	}
	if oldExists == newExists {
		if oldExists {
			return fmt.Errorf("both old (%s) and new (%s) xfs volume paths exist; manual intervention required",
				oldVolume.hostPath(x.dataPath), newVolume.hostPath(x.dataPath))
		}
		return fmt.Errorf("neither old (%s) nor new (%s) xfs volume path exists",
			oldVolume.hostPath(x.dataPath), newVolume.hostPath(x.dataPath))
	}
	physicalVolume := oldVolume
	if newExists {
		physicalVolume = newVolume
	}
	physicalInfo, err := root.Lstat(physicalVolume.value())
	if err != nil {
		return fmt.Errorf("stat xfs volume before rename: %w", err)
	}
	projID, err := readProjectIDFileAtRoot(root, physicalVolume)
	if err != nil {
		return fmt.Errorf("attest xfs volume before rename: %w", err)
	}

	oldID, oldMapped, err := x.mappedProjectIDLocked(oldVolume.value())
	if err != nil {
		return fmt.Errorf("validate old xfs rename authority: %w", err)
	}
	newID, newMapped, err := x.mappedProjectIDLocked(newVolume.value())
	if err != nil {
		return fmt.Errorf("validate destination xfs rename authority: %w", err)
	}
	if oldMapped && oldID != projID {
		return fmt.Errorf("old xfs volume %q maps to project ID %d, marker names %d",
			oldVolume.value(), oldID, projID)
	}
	if newMapped && newID != projID {
		return fmt.Errorf("destination xfs volume %q maps to project ID %d, marker names %d",
			newVolume.value(), newID, projID)
	}
	if oldMapped && newMapped {
		return fmt.Errorf("old and destination xfs volume names both carry project-ID authority")
	}
	if owner, ok := x.activeIDs[projID]; ok && owner != oldVolume.value() && owner != newVolume.value() {
		return fmt.Errorf("xfs project ID %d is owned by foreign volume %q", projID, owner)
	}
	if oldExists && newMapped {
		return fmt.Errorf("destination xfs volume %q already has project-ID authority before rename",
			newVolume.value())
	}

	if oldExists {
		if err := commitXFSVolumeRename(
			root,
			oldVolume,
			newVolume,
			projID,
			physicalInfo,
			parent.RenameNoReplace,
			parent.Sync,
		); err != nil {
			return err
		}
	} else {
		// Idempotent retry: this invocation did not mutate the namespace, so a
		// failed attestation is an ordinary refusal rather than a newly ambiguous
		// side effect.
		newInfo, err := root.Lstat(newVolume.value())
		if err != nil {
			return fmt.Errorf("re-read destination xfs volume on rename retry: %w", err)
		}
		if !newInfo.IsDir() || newInfo.Mode()&os.ModeSymlink != 0 || !os.SameFile(physicalInfo, newInfo) {
			return fmt.Errorf("destination xfs volume %q changed identity during rename retry", newVolume.value())
		}
		newProjID, err := readProjectIDFileAtRoot(root, newVolume)
		if err != nil {
			return fmt.Errorf("attest renamed xfs volume on retry: %w", err)
		}
		if newProjID != projID {
			return fmt.Errorf("renamed xfs marker changed from project ID %d to %d", projID, newProjID)
		}
	}

	if oldMapped {
		delete(x.volumeToID, oldVolume.value())
	}
	delete(x.activeIDs, projID)
	x.activeIDs[projID] = newVolume.value()
	x.volumeToID[newVolume.value()] = projID
	return nil
}

// HostPath returns the absolute path of the volume directory under the
// configured data path. The directory may or may not exist; callers use
// this to compute paths for not-yet-renamed or about-to-be-created
// volumes (see migrate.go in Task 9).
func (x *xfsVolumeManager) HostPath(name string) string {
	volumeID, err := parseManagedVolumeName(name)
	if err != nil {
		return rejectedManagedVolumeHostPath(x.dataPath, name)
	}
	return volumeID.hostPath(x.dataPath)
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
	volumeID, err := parseManagedVolumeName(id)
	if err != nil {
		return 0, fmt.Errorf("validate xfs volume ID for usage: %w", err)
	}
	dirPath := volumeID.hostPath(x.dataPath)
	root, err := os.OpenRoot(x.dataPath)
	if err != nil {
		return 0, fmt.Errorf("open xfs volume root %s: %w", x.dataPath, err)
	}
	defer func() { _ = root.Close() }()
	projID, err := readProjectIDFileAtRoot(root, volumeID)
	if err != nil {
		return 0, fmt.Errorf("read project ID marker for %s: %w", dirPath, err)
	}
	command := xfsProjectReportCmd("b", projID)
	out, err := runXFSQuotaReport(ctx, command, x.mountPoint)
	if err != nil {
		return 0, fmt.Errorf("xfs_quota report for %s (proj %d): %w", dirPath, projID, err)
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
	used, found, err := parseXfsReportUsed(out, projID)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, fmt.Errorf("project id %d not found in xfs_quota report output", projID)
	}
	return used, nil
}

func parseXfsReportUsed(out string, projID uint32) (int64, bool, error) {
	want := strconv.FormatUint(uint64(projID), 10)
	var (
		result int64
		found  bool
	)
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) < 2 {
			return 0, false, fmt.Errorf("malformed xfs_quota report row %q", line)
		}
		first := strings.TrimPrefix(fields[0], "#")
		numericID, idErr := strconv.ParseUint(first, 10, 32)
		if idErr != nil {
			return 0, false, fmt.Errorf("xfs_quota report returned nonnumeric project ID %q: %w", fields[0], idErr)
		}
		if strconv.FormatUint(numericID, 10) != want {
			continue
		}
		used, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, false, fmt.Errorf("parse project %d used value %q: %w", projID, fields[1], err)
		}
		if used < 0 {
			return 0, false, fmt.Errorf("project %d has negative used value %d", projID, used)
		}
		if found {
			return 0, false, fmt.Errorf("project id %d appears more than once in xfs_quota report output", projID)
		}
		result, found = used, true
	}
	return result, found, nil
}

// runXFSQuotaReport keeps machine-readable stdout separate from diagnostics.
// A successful exit with stderr is still uncertain: report callers interpret
// an absent project row as authoritative zero, so discarding a warning could
// incorrectly authorize quota teardown. Reject diagnostics explicitly while
// retaining strict parsing for stdout.
func runXFSQuotaReport(ctx context.Context, command, mountPoint string) ([]byte, error) {
	process := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(command, mountPoint)...)
	var stderr strings.Builder
	process.Stderr = &stderr
	stdout, err := process.Output()
	diagnostic := strings.TrimSpace(stderr.String())
	if err != nil {
		if diagnostic == "" {
			return nil, err
		}
		return nil, fmt.Errorf("%w: stderr: %s", err, diagnostic)
	}
	if diagnostic != "" {
		return nil, fmt.Errorf("xfs_quota exited successfully with diagnostic stderr: %s", diagnostic)
	}
	return stdout, nil
}

func (x *xfsVolumeManager) readProjectQuotaUsage(ctx context.Context, projID uint32, resource string) (int64, error) {
	if resource != "b" && resource != "i" {
		return 0, fmt.Errorf("unsupported xfs project quota resource %q", resource)
	}
	command := xfsProjectReportCmd(resource, projID)
	out, err := runXFSQuotaReport(ctx, command, x.mountPoint)
	if err != nil {
		return 0, fmt.Errorf("xfs_quota %s for project %d: %w", command, projID, err)
	}
	used, found, err := parseXfsReportUsed(string(out), projID)
	if err != nil {
		return 0, err
	}
	if !found {
		// With no report row the kernel has no initialized dquot for the ID, so
		// there can be neither usage nor a limit left to clear.
		return 0, nil
	}
	return used, nil
}

func (x *xfsVolumeManager) waitForZeroProjectQuotaUsage(
	ctx context.Context,
	projID uint32,
) (blocks int64, inodes int64, err error) {
	const (
		initialPollInterval = 100 * time.Millisecond
		maximumPollInterval = time.Second
	)
	pollInterval := initialPollInterval

	triggerCmd := xfsInodeGCTriggerCmd()
	if out, triggerErr := exec.CommandContext(
		ctx, "xfs_quota", xfsQuotaArgs(triggerCmd, x.mountPoint)...,
	).CombinedOutput(); triggerErr != nil {
		return 0, 0, fmt.Errorf("xfs_quota %s before project %d usage proof: %w: %s",
			triggerCmd, projID, triggerErr, out)
	}

	for {
		blocks, err = x.readProjectQuotaUsage(ctx, projID, "b")
		if err != nil {
			return blocks, inodes, err
		}
		inodes, err = x.readProjectQuotaUsage(ctx, projID, "i")
		if err != nil {
			return blocks, inodes, err
		}
		if blocks == 0 && inodes == 0 {
			return 0, 0, nil
		}

		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return blocks, inodes, ctx.Err()
		case <-timer.C:
		}
		pollInterval = min(2*pollInterval, maximumPollInterval)
	}
}

func (x *xfsVolumeManager) loadProjectIDs() error {
	ids, err := x.List()
	if err != nil {
		return fmt.Errorf("list volumes for active ID scan: %w", err)
	}
	root, err := os.OpenRoot(x.dataPath)
	if err != nil {
		return fmt.Errorf("open xfs volume root for active ID scan: %w", err)
	}
	defer func() { _ = root.Close() }()

	candidateActive := make(map[uint32]string)
	candidateReverse := make(map[string]uint32)
	candidateRecovered := make(map[string]recoveredXFSStage)
	candidateRecoveredDeletes := make(map[string]recoveredXFSDeleteStage)
	candidatePrivateTargets := make(map[string]string)
	registerCandidate := func(volumeID string, projID uint32) error {
		if projID == 0 {
			return errors.New("xfs project ID 0 is reserved for the default project")
		}
		if oldID, ok := candidateReverse[volumeID]; ok && oldID != projID {
			return fmt.Errorf("xfs volume %q has conflicting project IDs %d and %d", volumeID, oldID, projID)
		}
		if oldVolume, ok := candidateActive[projID]; ok && oldVolume != volumeID {
			return fmt.Errorf("duplicate project ID %d: volumes %q and %q", projID, oldVolume, volumeID)
		}
		candidateActive[projID] = volumeID
		candidateReverse[volumeID] = projID
		return nil
	}
	entries, err := readXFSRootEntries(root)
	if err != nil {
		return fmt.Errorf("list xfs root for interrupted mutation scan: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	// Delete authority must be loaded before ordinary volumes: a prior partial
	// RemoveAll may have unlinked the inner project marker, and the exact typed
	// sibling is the only reason that otherwise-invalid volume is safe to resume.
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), xfsDeleteStagePrefix) {
			continue
		}
		stage, parseErr := parseXFSDeleteStageName(entry.Name())
		if parseErr != nil {
			return fmt.Errorf("reject malformed entry in reserved xfs delete-stage namespace: %w", parseErr)
		}
		if previous, duplicate := candidatePrivateTargets[stage.volumeID.value()]; duplicate {
			return fmt.Errorf("xfs private stages %q and %q target volume %q",
				previous, stage.value(), stage.volumeID.value())
		}
		if inspectErr := inspectXFSDeleteStage(root, stage); inspectErr != nil {
			return fmt.Errorf("attest interrupted xfs delete %q: %w", stage.value(), inspectErr)
		}
		if _, statErr := managedDirectoryExistsAtRoot(root, stage.volumeID); statErr != nil {
			return fmt.Errorf("inspect final volume for xfs delete-stage %q: %w", stage.value(), statErr)
		}
		if registerErr := registerCandidate(stage.volumeID.value(), stage.projID); registerErr != nil {
			return fmt.Errorf("register interrupted xfs delete %q: %w", stage.value(), registerErr)
		}
		candidatePrivateTargets[stage.volumeID.value()] = stage.value()
		candidateRecoveredDeletes[stage.volumeID.value()] = recoveredXFSDeleteStage{stage: stage}
	}

	sort.Strings(ids)
	for _, vid := range ids {
		volumeID, parseErr := parseManagedVolumeName(vid)
		if parseErr != nil {
			return fmt.Errorf("validate managed volume name %q during active ID scan: %w", vid, parseErr)
		}
		exists, statErr := managedDirectoryExistsAtRoot(root, volumeID)
		if statErr != nil {
			return fmt.Errorf("stat volume %s during active ID scan: %w", volumeID.hostPath(x.dataPath), statErr)
		}
		if !exists {
			return fmt.Errorf("volume %s disappeared during active ID scan", volumeID.hostPath(x.dataPath))
		}
		projID, readErr := readProjectIDFileAtRoot(root, volumeID)
		if recoveredDelete, deleting := candidateRecoveredDeletes[volumeID.value()]; deleting {
			switch {
			case readErr == nil && projID != recoveredDelete.stage.projID:
				return fmt.Errorf("xfs delete-stage %q encodes project ID %d but volume marker names %d",
					recoveredDelete.stage.value(), recoveredDelete.stage.projID, projID)
			case readErr == nil:
				continue // delete-stage scan already registered the exact authority
			case errors.Is(readErr, fs.ErrNotExist):
				continue // marker-first partial removal; typed sibling is authoritative
			default:
				return fmt.Errorf("read project ID marker for deleting volume %s: %w", volumeID.value(), readErr)
			}
		}
		if readErr != nil {
			return fmt.Errorf("read project ID marker for volume %s: %w", volumeID.value(), readErr)
		}
		if registerErr := registerCandidate(volumeID.value(), projID); registerErr != nil {
			return fmt.Errorf("register xfs volume %s during active ID scan: %w", volumeID.value(), registerErr)
		}
	}

	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), xfsStagePrefix) {
			continue
		}
		stage, parseErr := parseXFSStageName(entry.Name())
		if parseErr != nil {
			return fmt.Errorf("reject malformed entry in reserved xfs stage namespace: %w", parseErr)
		}
		if _, duplicate := candidateRecovered[stage.volumeID.value()]; duplicate {
			return fmt.Errorf("multiple xfs stages target volume %q", stage.volumeID.value())
		}
		if previous, duplicate := candidatePrivateTargets[stage.volumeID.value()]; duplicate {
			return fmt.Errorf("xfs private stages %q and %q target volume %q",
				previous, stage.value(), stage.volumeID.value())
		}
		finalExists, statErr := managedDirectoryExistsAtRoot(root, stage.volumeID)
		if statErr != nil {
			return fmt.Errorf("inspect final volume for xfs stage %q: %w", stage.value(), statErr)
		}
		if finalExists {
			return fmt.Errorf("xfs stage %q and final volume %q coexist",
				stage.value(), stage.volumeID.value())
		}
		if _, inspectErr := inspectXFSStageForCleanup(root, stage); inspectErr != nil {
			return fmt.Errorf("attest interrupted xfs create %q: %w", stage.value(), inspectErr)
		}
		if registerErr := registerCandidate(stage.volumeID.value(), stage.projID); registerErr != nil {
			return fmt.Errorf("register interrupted xfs create %q: %w", stage.value(), registerErr)
		}
		candidatePrivateTargets[stage.volumeID.value()] = stage.value()
		candidateRecovered[stage.volumeID.value()] = recoveredXFSStage{stage: stage}
	}

	x.mu.Lock()
	defer x.mu.Unlock()
	if len(x.durableStages) != 0 || len(x.durableDeleteStages) != 0 {
		return errors.New("refuse startup xfs scan while live durable stages are registered")
	}
	x.activeIDs = candidateActive
	x.volumeToID = candidateReverse
	x.recoveredStages = candidateRecovered
	x.recoveredDeleteStages = candidateRecoveredDeletes
	return nil
}

// RequireNoInterruptedVolumeMutations is a read-only publication gate. Validate has
// already converted every reserved stage pathname into typed, conflict-checked
// evidence; explicit adoption and preflight must reject that evidence rather
// than changing the filesystem they are proving.
func (x *xfsVolumeManager) RequireNoInterruptedVolumeMutations(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	x.mu.Lock()
	names := make([]string, 0,
		len(x.recoveredStages)+len(x.durableStages)+len(x.recoveredDeleteStages)+len(x.durableDeleteStages))
	for _, recovered := range x.recoveredStages {
		names = append(names, recovered.stage.value())
	}
	for _, stage := range x.durableStages {
		names = append(names, stage.value())
	}
	for _, recovered := range x.recoveredDeleteStages {
		names = append(names, recovered.stage.value())
	}
	for _, stage := range x.durableDeleteStages {
		names = append(names, stage.value())
	}
	x.mu.Unlock()
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)
	return fmt.Errorf("xfs volume root contains interrupted creates/deletes: %s", strings.Join(names, ", "))
}

// RecoverInterruptedVolumeMutations consumes startup-scanned create and delete
// stages but never publishes a create stage: its requested quota is not durable,
// and operation recovery settles rather than replaying the original provision
// request. Cleanup is exact and retains both filesystem evidence and project-ID
// reservations on failure.
func (x *xfsVolumeManager) RecoverInterruptedVolumeMutations(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	x.mu.Lock()
	stages := make([]xfsStageName, 0, len(x.recoveredStages))
	for _, recovered := range x.recoveredStages {
		stages = append(stages, recovered.stage)
	}
	x.mu.Unlock()
	sort.Slice(stages, func(i, j int) bool { return stages[i].value() < stages[j].value() })
	for _, stage := range stages {
		if err := ctx.Err(); err != nil {
			return err
		}
		x.mu.Lock()
		recovered, ok := x.recoveredStages[stage.volumeID.value()]
		x.mu.Unlock()
		if !ok || recovered.stage != stage {
			return fmt.Errorf("xfs recovered-stage authority changed for %q", stage.value())
		}
		if err := x.cleanupXFSStage(ctx, stage); err != nil {
			return fmt.Errorf("recover interrupted xfs create %q: %w", stage.value(), err)
		}
	}
	x.mu.Lock()
	deleteStages := make([]xfsDeleteStageName, 0, len(x.recoveredDeleteStages))
	for _, recovered := range x.recoveredDeleteStages {
		deleteStages = append(deleteStages, recovered.stage)
	}
	x.mu.Unlock()
	sort.Slice(deleteStages, func(i, j int) bool { return deleteStages[i].value() < deleteStages[j].value() })
	for _, stage := range deleteStages {
		if err := ctx.Err(); err != nil {
			return err
		}
		x.mu.Lock()
		recovered, ok := x.recoveredDeleteStages[stage.volumeID.value()]
		x.mu.Unlock()
		if !ok || recovered.stage != stage {
			return fmt.Errorf("xfs recovered delete-stage authority changed for %q", stage.value())
		}
		if err := x.cleanupXFSDeleteStage(ctx, stage); err != nil {
			return fmt.Errorf("recover interrupted xfs delete %q: %w", stage.value(), err)
		}
	}
	return nil
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
	return x.loadProjectIDs()
}
