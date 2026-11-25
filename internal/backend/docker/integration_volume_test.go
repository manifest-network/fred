//go:build integration

package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	fredtestutil "github.com/manifest-network/fred/internal/testutil"
)

// execInContainer runs a command inside a running container and returns stdout.
func execInContainer(t *testing.T, containerID string, cmd []string) string {
	t.Helper()
	args := append([]string{"exec", containerID}, cmd...)
	out, err := exec.Command("docker", args...).CombinedOutput()
	require.NoError(t, err, "docker exec %v: %s", cmd, out)
	return string(out)
}

// getContainerID returns the Docker container ID for a lease (exactly one expected).
func getContainerID(t *testing.T, leaseUUID string) string {
	t.Helper()
	docker, err := NewDockerClient(t.Context(), "", "")
	require.NoError(t, err)
	defer func() { _ = docker.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	containers, err := docker.client.ContainerList(ctx, container.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("label", LabelManaged+"=true"),
			filters.Arg("label", LabelLeaseUUID+"="+leaseUUID),
		),
	})
	require.NoError(t, err)
	require.Len(t, containers, 1, "expected exactly 1 container for lease %s", leaseUUID)
	return containers[0].ID
}

// containerHasBindMount checks if a container has a bind mount at the given destination.
func containerHasBindMount(t *testing.T, containerID, dest string) bool {
	t.Helper()
	docker, err := NewDockerClient(t.Context(), "", "")
	require.NoError(t, err)
	defer func() { _ = docker.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	inspect, err := docker.client.ContainerInspect(ctx, containerID)
	require.NoError(t, err)

	for _, m := range inspect.Mounts {
		if m.Destination == dest && m.Type == "bind" {
			return true
		}
	}
	return false
}

// TestIntegration_Docker_OrphanedVolumeCleanup verifies that an unclaimed
// managed volume is destroyed when a new backend starts. It seeds the durable
// state left after a failed provisioning rollback could not remove its volume:
// no live projection, operation intent, active release, retention record, or
// container names the bytes. Interrupted-operation recovery is covered
// separately; this test starts at the orphan collector's ownership boundary.

// writeIncompressibleMiB writes mib MiB of incompressible data to path and
// syncs. A compressing filesystem can store zero-filled test data (dd
// if=/dev/zero / make([]byte,N)) in far less space than its length, so it would
// not fill a quota. /dev/urandom is incompressible so the bytes are actually
// allocated; the sync makes the usage visible to the next quota read.
func writeIncompressibleMiB(t *testing.T, path string, mib int) {
	t.Helper()
	out, err := exec.Command("dd", "if=/dev/urandom", "of="+path, "bs=1M", fmt.Sprintf("count=%d", mib)).CombinedOutput()
	require.NoError(t, err, "dd urandom -> %s: %s", path, out)
	_ = exec.Command("sync").Run()
}

// writeIncompressibleExpectFail writes mib MiB of incompressible data and asserts
// the write is REJECTED (quota/space exceeded) — used to prove an enforced cap.
func writeIncompressibleExpectFail(t *testing.T, path string, mib int) {
	t.Helper()
	out, err := exec.Command("dd", "if=/dev/urandom", "of="+path, "bs=1M", fmt.Sprintf("count=%d", mib)).CombinedOutput()
	require.Error(t, err, "writing %d MiB should have been quota-rejected: %s", mib, out)
}

// backendForReconcileTest builds a Backend with one active stateful lease whose
// single instance of service svc uses sku (DiskMB=diskMB), wired to mgr — the
// minimal wiring reconcileVolumeQuotas needs to re-apply a real quota.
func backendForReconcileTest(t *testing.T, mgr volumeManager, dataPath, lease, svc, sku string, diskMB int64) *Backend {
	t.Helper()
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease,
			Items:     []backend.LeaseItem{{SKU: sku, Quantity: 1, ServiceName: svc}},
		}},
	})
	useVolumeManager(b, mgr)
	b.cfg.VolumeDataPath = dataPath
	b.cfg.SKUProfiles[sku] = SKUProfile{CPUCores: 1, MemoryMB: 256, DiskMB: diskMB}
	return b
}

// TestIntegration_Usage_XFS verifies that xfsVolumeManager.Usage returns the
// project's used bytes (1 KiB block granularity) within a metadata-overhead bound.
func TestIntegration_Usage_XFS(t *testing.T) {
	mountDir := fredtestutil.SetupXFSLoopback(t)
	ctx := context.Background()

	mgr, err := newVolumeManager(mountDir, "xfs", 1024, slog.Default())
	require.NoError(t, err)

	const leaseUUID = "550e8400-e29b-41d4-a716-446655440101"
	volName := canonicalVolumeName(leaseUUID, "app", 0)
	const capMiB = int64(100)

	hostPath, created, err := mgr.Create(ctx, volName, capMiB)
	require.NoError(t, err)
	require.True(t, created)
	t.Cleanup(func() { _ = volDestroyer(t, mgr).Destroy(ctx, volName) })

	const writeMiB = 10
	require.NoError(t, os.WriteFile(filepath.Join(hostPath, "data.bin"), make([]byte, writeMiB*1024*1024), 0600))

	got, err := mgr.Usage(ctx, volName)
	require.NoError(t, err)

	const wantMin = int64(writeMiB * 1024 * 1024)
	const wantMax = int64((writeMiB + 6) * 1024 * 1024)
	assert.GreaterOrEqual(t, got, wantMin, "Usage too low: got %d, want >= %d", got, wantMin)
	assert.LessOrEqual(t, got, wantMax, "Usage too high: got %d, want <= %d", got, wantMax)
}

// TestIntegration_XFS_SubdirDataPath_TagsMeasuresEnforces is the ENG-449
// regression. It runs the xfs manager over a volume_data_path that is a
// *subdirectory* of the XFS mount — the production layout (e.g.
// /data/fred/volumes under the /data/fred mount) — instead of the mount root
// that the other XFS tests use (which accidentally hid the bug: when
// dataPath == mountpoint the wrong argument still resolved).
//
// Under the pre-fix code, xfs_quota was run against the subdir, so:
//   - `project -s` silently no-op'd → the volume inode was never project-tagged
//     → Usage()'s `report -p` could not find the projid ("project id N not found");
//   - `limit -p` silently no-op'd → the disk_mb cap was never enforced.
func TestIntegration_XFS_SubdirDataPath_TagsMeasuresEnforces(t *testing.T) {
	mount := fredtestutil.SetupXFSLoopback(t)
	dataPath := filepath.Join(mount, "volumes") // subdir of the XFS mount
	require.NoError(t, os.MkdirAll(dataPath, 0700))

	ctx := context.Background()
	mgr, err := newVolumeManager(dataPath, "xfs", 1024, slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.Validate())

	const leaseUUID = "550e8400-e29b-41d4-a716-446655440102"
	volName := canonicalVolumeName(leaseUUID, "app", 0)
	const capMiB = int64(20)
	hostPath, created, err := mgr.Create(ctx, volName, capMiB)
	require.NoError(t, err)
	require.True(t, created)
	t.Cleanup(func() { _ = volDestroyer(t, mgr).Destroy(ctx, volName) })

	// (1) Measurement: a 10 MiB write is accounted, proving the inode is
	// project-tagged and `report -p` runs against the mount, not the subdir.
	const writeMiB = 10
	require.NoError(t, os.WriteFile(filepath.Join(hostPath, "data.bin"), make([]byte, writeMiB*1024*1024), 0600))
	got, err := mgr.Usage(ctx, volName)
	require.NoError(t, err, "Usage must succeed for a subdirectory volume_data_path (ENG-449)")
	assert.GreaterOrEqual(t, got, int64(writeMiB*1024*1024), "Usage under-reports: %d", got)
	assert.LessOrEqual(t, got, int64((writeMiB+6)*1024*1024), "Usage over-reports: %d", got)

	// (2) Enforcement: a write past the 20 MiB cap is rejected with EDQUOT/ENOSPC.
	err = os.WriteFile(filepath.Join(hostPath, "big.bin"), make([]byte, 25*1024*1024), 0600)
	require.Error(t, err, "writing past the disk_mb cap must be quota-enforced (ENG-449)")
	assert.True(t,
		errors.Is(err, syscall.EDQUOT) || errors.Is(err, syscall.ENOSPC) ||
			strings.Contains(err.Error(), "disk quota exceeded") ||
			strings.Contains(err.Error(), "no space left"),
		"expected a quota/space error, got: %v", err)
}

// xfsReportListsProject reports whether projID currently appears in
// `xfs_quota report -p` for mount — i.e. its dquot still carries a nonzero limit or
// usage (a modern GETNEXTQUOTA kernel skips fully-uninitialized dquots). This is the
// precise per-project signal ENG-459 asserts on, robust to any other (baseline or
// foreign) entries in the filesystem-global quota table. `-N` suppresses headers.
func xfsReportListsProject(t *testing.T, mount string, projID uint32) bool {
	t.Helper()
	out, err := exec.Command("xfs_quota", xfsQuotaArgs("report -p -n -N", mount)...).CombinedOutput()
	require.NoError(t, err, "xfs_quota report -p: %s", out)
	want := strconv.FormatUint(uint64(projID), 10)
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 1 && strings.TrimPrefix(fields[0], "#") == want {
			return true
		}
	}
	return false
}

func requireXFSProjectEventuallyAbsent(t *testing.T, mount string, projID uint32) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for xfsReportListsProject(t, mount, projID) {
		if time.Now().After(deadline) {
			t.Fatalf("project-quota entry %d remained visible after pending inode cleanup", projID)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestIntegration_XFS_InterruptedCreateStageRecoveryClearsQuota is the
// privileged crash-recovery regression for the private XFS create namespace.
// It seeds the exact durable state that exists after project tagging and quota
// setup but before the stage-to-final rename, then constructs a fresh manager
// as a restarted process would. Startup must discover (and initially refuse to
// publish past) the stage, and explicit recovery must remove both the directory
// evidence and its kernel dquot without ever publishing the final volume name.
func TestIntegration_XFS_InterruptedCreateStageRecoveryClearsQuota(t *testing.T) {
	mount := fredtestutil.SetupXFSLoopback(t)
	dataPath := filepath.Join(mount, "volumes") // subdir of the mount (production layout)
	require.NoError(t, os.MkdirAll(dataPath, 0700))
	ctx := context.Background()

	const leaseUUID = "550e8400-e29b-41d4-a716-44665544010a"
	finalName, err := parseManagedVolumeName(canonicalVolumeName(leaseUUID, "app", 0))
	require.NoError(t, err)
	const projID = uint32(424243)
	stage, err := newXFSStageName(projID, finalName)
	require.NoError(t, err)
	stagePath := stage.hostPath(dataPath)
	finalPath := finalName.hostPath(dataPath)

	// Reproduce Create's durable pre-publication boundary: a parent-synced
	// typed stage, a synced marker, a project tag, and nonzero block/inode
	// limits. No final directory is ever created by this fixture.
	require.NoError(t, os.Mkdir(stagePath, 0700))
	parent, err := os.Open(dataPath)
	require.NoError(t, err)
	require.NoError(t, parent.Sync())
	require.NoError(t, parent.Close())
	require.NoError(t, writeProjectIDFile(stagePath, projID))
	stageDir, err := os.Open(stagePath)
	require.NoError(t, err)
	require.NoError(t, stageDir.Sync())
	require.NoError(t, stageDir.Close())

	setupCmd := xfsProjectSetupCmd(dataPath, stage)
	out, err := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(setupCmd, mount)...).CombinedOutput()
	require.NoError(t, err, "xfs_quota project setup for interrupted stage: %s", out)
	limitCmd := xfsLimitCmd(projID, "20m", inodeHardLimit(20, 1024))
	out, err = exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(limitCmd, mount)...).CombinedOutput()
	require.NoError(t, err, "xfs_quota limit for interrupted stage: %s", out)
	require.True(t, xfsReportListsProject(t, mount, projID),
		"fixture project must have a live dquot before restart recovery")
	require.NoDirExists(t, finalPath, "fixture must stop before stage publication")

	// A new manager has no process-local stage capability. Validate reconstructs
	// cleanup-only authority exclusively from the parent-synced on-disk name.
	restarted, err := newVolumeManager(dataPath, "xfs", 1024, slog.Default())
	require.NoError(t, err)
	require.NoError(t, restarted.Validate())
	gateErr := restarted.RequireNoInterruptedVolumeMutations(ctx)
	require.Error(t, gateErr, "startup must not proceed while interrupted create evidence remains")
	assert.Contains(t, gateErr.Error(), stage.value())

	require.NoError(t, restarted.RecoverInterruptedVolumeMutations(ctx))
	require.NoError(t, restarted.RequireNoInterruptedVolumeMutations(ctx),
		"successful recovery must consume its reconstructed stage authority")
	require.NoDirExists(t, stagePath, "recovery must remove the private create stage")
	require.NoDirExists(t, finalPath, "recovery must never publish an interrupted create")
	// Clearing the limits precedes unlinking the typed stage so crash recovery
	// never loses its project-ID authority. XFS may retire that last unlinked
	// inode asynchronously; require eventual dquot disappearance rather than
	// racing the inodegc worker immediately after recovery returns.
	requireXFSProjectEventuallyAbsent(t, mount, projID)
}

// TestIntegration_XFS_DeleteStageRecoveryWaitsForOpenUnlinkedInode exercises
// the kernel behavior behind the durable delete protocol. An open descriptor
// keeps an unlinked project-tagged inode (and its usage) alive. Startup
// registers the replayed pre-reset tombstone as a hold without touching it;
// the hold executor's retry normalizes it to project 0, removes the namespace,
// refuses to clear the old dquot while that inode remains (a residual hold),
// and converges once the descriptor is closed, also across a restart.
func TestIntegration_XFS_DeleteStageRecoveryWaitsForOpenUnlinkedInode(t *testing.T) {
	mount := fredtestutil.SetupXFSLoopback(t)
	dataPath := filepath.Join(mount, "volumes")
	require.NoError(t, os.MkdirAll(dataPath, 0o700))
	ctx := context.Background()

	mgr, err := newVolumeManager(dataPath, "xfs", 1024, slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.Validate())
	const leaseUUID = "550e8400-e29b-41d4-a716-44665544010b"
	volumeName := canonicalVolumeName(leaseUUID, "app", 0)
	volumeID, err := parseManagedVolumeName(volumeName)
	require.NoError(t, err)
	volumePath, created, err := mgr.Create(ctx, volumeName, 20)
	require.NoError(t, err)
	require.True(t, created)
	projID, err := readProjectIDFile(volumePath)
	require.NoError(t, err)

	dataFile, err := os.OpenFile(filepath.Join(volumePath, "tenant-data"), os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataFile.Close() })
	require.NoError(t, dataFile.Truncate(1024*1024))
	require.NoError(t, dataFile.Sync())

	deleteStage, err := newXFSDeleteStageName(projID, volumeID)
	require.NoError(t, err)
	deleteStagePath := deleteStage.hostPath(dataPath)
	require.NoError(t, os.Mkdir(deleteStagePath, 0o700))
	// Model a power loss after mkdir but before prepare's project-0 reset by
	// deliberately charging the sibling itself to the retiring project.
	preResetRoot, err := os.OpenRoot(deleteStagePath)
	require.NoError(t, err)
	require.NoError(t, (linuxXFSProjectAttributes{}).SetProjectID(preResetRoot, projID))
	require.NoError(t, preResetRoot.Close())
	parent, err := os.Open(dataPath)
	require.NoError(t, err)
	require.NoError(t, parent.Sync())
	require.NoError(t, parent.Close())

	// Reproduce marker-first partial removal while tenant data and its open FD
	// remain. Only the typed sibling carries the collision-probed project ID.
	require.NoError(t, os.Remove(filepath.Join(volumePath, projectIDFile)))
	volumeDir, err := os.Open(volumePath)
	require.NoError(t, err)
	require.NoError(t, volumeDir.Sync())
	require.NoError(t, volumeDir.Close())

	restarted, err := newVolumeManager(dataPath, "xfs", 1024, slog.Default())
	require.NoError(t, err)
	require.NoError(t, restarted.Validate())
	require.ErrorContains(t, restarted.RequireNoInterruptedVolumeMutations(ctx), deleteStage.value(),
		"the exclusive commands still refuse a held deletion")
	require.NoError(t, restarted.RequireNoUnheldVolumeMutations(ctx), "Start accepts a held deletion")
	require.NoError(t, restarted.RecoverInterruptedVolumeMutations(ctx))
	assert.DirExists(t, volumePath, "Start never removes tenant data itself")

	retryCtx, cancelRetry := context.WithTimeout(ctx, 5*time.Second)
	err = restarted.RetryHeldVolumeDelete(retryCtx, volumeName)
	cancelRetry()
	require.ErrorIs(t, err, ErrVolumeDeleteHeld)
	require.ErrorContains(t, err, "refuse to clear xfs project quota")
	require.NotErrorIs(t, err, ErrVolumeMutationRecoveryPending)
	hold := restarted.VolumeDeleteHolds().holds[volumeName]
	assert.True(t, hold.phase == holdPhaseResidual, "the namespace is durably gone; only the project remains")
	assert.Equal(t, holdReasonUsageNonzero, hold.reason)
	assert.Positive(t, hold.footprintMB, "the residual hold accounts the project's hard limit")
	assert.NoDirExists(t, volumePath, "namespace deletion may finish while the FD remains open")
	assert.DirExists(t, deleteStagePath, "nonzero kernel usage must retain durable cleanup authority")
	assert.True(t, xfsReportListsProject(t, mount, projID), "nonzero open-inode usage must retain the dquot")

	// The retry must have repaired the replayed pre-reset sibling before its zero
	// proof. Read the typed kernel attribute rather than xfsprogs report prose.
	deleteStageRoot, err := os.OpenRoot(deleteStagePath)
	require.NoError(t, err)
	deleteStageAttr, readAttrErr := (linuxXFSProjectAttributes{}).ReadProjectAttributes(deleteStageRoot)
	closeRootErr := deleteStageRoot.Close()
	require.NoError(t, readAttrErr)
	require.NoError(t, closeRootErr)
	require.NoError(t, validateXFSDefaultProject(deleteStageAttr))

	require.NoError(t, dataFile.Close(), "closing the last reference releases the unlinked project inode")
	restartedAgain, err := newVolumeManager(dataPath, "xfs", 1024, slog.Default())
	require.NoError(t, err)
	require.NoError(t, restartedAgain.Validate())
	require.NoError(t, restartedAgain.RecoverInterruptedVolumeMutations(ctx))
	require.True(t, restartedAgain.VolumeDeleteHolds().holds[volumeName].phase == holdPhaseResidual,
		"Start sizes a recovered deletion whose final path is gone")
	retryCtx, cancelRetry = context.WithTimeout(ctx, 10*time.Second)
	require.NoError(t, restartedAgain.RetryHeldVolumeDelete(retryCtx, volumeName))
	cancelRetry()
	require.NoError(t, restartedAgain.RequireNoInterruptedVolumeMutations(ctx))
	assert.NoDirExists(t, deleteStagePath)
	assert.False(t, xfsReportListsProject(t, mount, projID),
		"the retry after the last close must clear the dquot and tombstone")
}

const xfsDeepChainChildEnv = "FRED_INTEGRATION_XFS_DEEP_CHAIN_CHILD"

// TestIntegration_XFS_DestroyDeletesChainDeeperThanFDLimit pins ENG-1117 on a
// real XFS project-quota volume: a tenant chain deeper than RLIMIT_NOFILE used
// to fail every deletion with EMFILE. The chain is built and the volume
// destroyed in a child process whose soft RLIMIT_NOFILE is 512.
func TestIntegration_XFS_DestroyDeletesChainDeeperThanFDLimit(t *testing.T) {
	if os.Getenv(xfsDeepChainChildEnv) == "1" {
		destroyDeepXFSChainUnderLowFDLimit(t)
		return
	}
	if os.Getuid() != 0 {
		t.Skip("xfs loopback requires root")
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), executable,
		"-test.run=^TestIntegration_XFS_DestroyDeletesChainDeeperThanFDLimit$", "-test.v")
	command.Env = append(os.Environ(), xfsDeepChainChildEnv+"=1")
	output, err := command.CombinedOutput()
	if strings.Contains(string(output), "--- SKIP: TestIntegration_XFS_DestroyDeletesChainDeeperThanFDLimit") {
		t.Skipf("child skipped:\n%s", output)
	}
	require.NoError(t, err, "%s", output)
	require.Contains(t, string(output), "--- PASS: TestIntegration_XFS_DestroyDeletesChainDeeperThanFDLimit (")
}

func destroyDeepXFSChainUnderLowFDLimit(t *testing.T) {
	const depth = 4096
	mount := fredtestutil.SetupXFSLoopback(t)
	dataPath := filepath.Join(mount, "volumes")
	require.NoError(t, os.MkdirAll(dataPath, 0o700))
	ctx := context.Background()
	mgr, err := newVolumeManager(dataPath, "xfs", 1024, slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.Validate())
	volumeName := canonicalVolumeName("550e8400-e29b-41d4-a716-44665544010c", "app", 0)
	volumePath, _, err := mgr.Create(ctx, volumeName, 64)
	require.NoError(t, err)
	projID, err := readProjectIDFile(volumePath)
	require.NoError(t, err)
	buildDirectoryChain(t, volumePath, writablePathSubdir, depth)
	controlPath := filepath.Join(mount, "control")
	require.NoError(t, os.Mkdir(controlPath, 0o700))
	buildDirectoryChain(t, controlPath, "chain", depth)

	var limit syscall.Rlimit
	require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit))
	require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{Cur: 512, Max: limit.Max}))
	t.Cleanup(func() { _ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit) })

	controlRoot, err := os.OpenRoot(controlPath)
	require.NoError(t, err)
	// Control: the unbounded remover ENG-1117 replaced.
	controlErr := controlRoot.RemoveAll("chain")
	require.NoError(t, controlRoot.Close())
	require.ErrorIs(t, controlErr, syscall.EMFILE, "control: Go's RemoveAll needs one descriptor per level")

	destroyCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	require.NoError(t, volDestroyer(t, mgr).Destroy(destroyCtx, volumeName))
	assert.NoDirExists(t, volumePath)
	assert.Empty(t, mgr.VolumeDeleteHolds().holds)
	requireXFSProjectEventuallyAbsent(t, mount, projID)
}

// setXFSFileProjectID tags one file with projID through the fsxattr ioctl, as
// a tenant could before ENG-1118: an inode outside the volume charged to it.
func setXFSFileProjectID(t *testing.T, path string, projID uint32) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()
	var attr linuxFSXAttr
	require.Zero(t, xfsAttributeIoctl(int(file.Fd()), linuxFSIOCFSGetXAttr, &attr))
	attr.ProjectID = projID
	require.Zero(t, xfsAttributeIoctl(int(file.Fd()), linuxFSIOCFSSetXAttr, &attr))
}

// TestIntegration_XFS_DestroyHoldsForeignTaggedInodeUsage pins the residual
// hold on the real kernel: an inode outside the volume charged to its project
// keeps the project's usage nonzero after the volume is gone. Destroy settles
// its caller, the hold keeps the stage, the project ID and its limits, the name
// cannot be recreated, and once the planted inode is deleted the retry clears
// everything.
func TestIntegration_XFS_DestroyHoldsForeignTaggedInodeUsage(t *testing.T) {
	mount := fredtestutil.SetupXFSLoopback(t)
	dataPath := filepath.Join(mount, "volumes")
	require.NoError(t, os.MkdirAll(dataPath, 0o700))
	ctx := context.Background()
	mgr, err := newVolumeManager(dataPath, "xfs", 1024, slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.Validate())
	volumeName := canonicalVolumeName("550e8400-e29b-41d4-a716-44665544010d", "app", 0)
	volumePath, _, err := mgr.Create(ctx, volumeName, 20)
	require.NoError(t, err)
	projID, err := readProjectIDFile(volumePath)
	require.NoError(t, err)
	stage := mustXFSDeleteStage(t, projID, volumeName)

	planted := filepath.Join(mount, "planted")
	writeIncompressibleMiB(t, planted, 1)
	setXFSFileProjectID(t, planted, projID)

	require.NoError(t, volDestroyer(t, mgr).Destroy(ctx, volumeName),
		"the volume's namespace is durably gone, so its caller settles")
	listed, err := mgr.ListForProof(ctx)
	require.NoError(t, err)
	assert.Contains(t, listed, volumeName, "until a publication counts it, proof listings keep the residual name")
	hold, held := mgr.VolumeDeleteHolds().holds[volumeName]
	require.True(t, held)
	assert.True(t, hold.phase == holdPhaseResidual)
	assert.Equal(t, holdReasonUsageNonzero, hold.reason)
	assert.Equal(t, int64(20), hold.footprintMB, "the hold accounts the project's 20 MiB hard limit")
	assert.NoDirExists(t, volumePath)
	assert.DirExists(t, stage.hostPath(dataPath))
	assert.True(t, xfsReportListsProject(t, mount, projID), "the limits stay while the project still charges usage")
	_, _, err = mgr.Create(ctx, volumeName, 20)
	require.ErrorIs(t, err, ErrVolumeDeleteHeld, "the held name cannot be recreated")

	require.NoError(t, os.Remove(planted))
	retryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	require.NoError(t, mgr.RetryHeldVolumeDelete(retryCtx, volumeName))
	assert.NoDirExists(t, stage.hostPath(dataPath))
	assert.Empty(t, mgr.VolumeDeleteHolds().holds)
	requireXFSProjectEventuallyAbsent(t, mount, projID)
}

// TestIntegration_XFS_DestroyCutsChainDeeperThanMaxDepth removes a chain deeper
// than fstree's ancestry bound (65,536 levels) from a PROJINHERIT volume on
// real XFS: fstree cuts the deeper subtree into the anchor, which the condemned
// volume first detaches from its project, and the deletion completes and clears
// the dquot. 70,000 levels fit the 262,144-inode floor.
func TestIntegration_XFS_DestroyCutsChainDeeperThanMaxDepth(t *testing.T) {
	mount := fredtestutil.SetupXFSLoopback(t)
	dataPath := filepath.Join(mount, "volumes")
	require.NoError(t, os.MkdirAll(dataPath, 0o700))
	ctx := context.Background()
	mgr, err := newVolumeManager(dataPath, "xfs", 1024, slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.Validate())
	volumeName := canonicalVolumeName("550e8400-e29b-41d4-a716-44665544010e", "app", 0)
	volumePath, _, err := mgr.Create(ctx, volumeName, 64)
	require.NoError(t, err)
	projID, err := readProjectIDFile(volumePath)
	require.NoError(t, err)
	buildDirectoryChain(t, volumePath, writablePathSubdir, 70_000)

	cuts := treeRemovalCutsTotal.WithLabelValues(treeRemovalSiteDeleteStage.String())
	before := testutil.ToFloat64(cuts)
	// A chain this deep can outlast the inline budget; the executor's retries
	// then finish it, exactly as in production.
	destroyErr := volDestroyer(t, mgr).Destroy(ctx, volumeName)
	for attempt := 0; errors.Is(destroyErr, ErrVolumeDeleteHeld) && attempt < 40; attempt++ {
		retryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		destroyErr = mgr.RetryHeldVolumeDelete(retryCtx, volumeName)
		cancel()
	}
	require.NoError(t, destroyErr)
	assert.Greater(t, testutil.ToFloat64(cuts), before, "a chain past the bound must be cut, not refused")
	assert.NoDirExists(t, volumePath)
	assert.Empty(t, mgr.VolumeDeleteHolds().holds)
	requireXFSProjectEventuallyAbsent(t, mount, projID)
}

// TestIntegration_XFS_QuotaSet_RequiresCapSysAdmin is the ENG-454 regression.
// It proves that SETTING an XFS project quota requires CAP_SYS_ADMIN, by running
// the exact privileged xfs_quota command both as root and as a dropped-privilege
// (non-root) subprocess. The pre-existing XFS integration tests all run as root
// (mounting a loopback fs requires it), so they never exercised the daemon's
// real, unprivileged capability set — which is why the missing capability (the
// docker-backend runs User=fred with only CAP_CHOWN,CAP_DAC_OVERRIDE) stayed
// invisible until ENG-449 unmasked it and it broke provisioning in prod.
// runWithoutCapSysAdmin runs `sh -c cmdline` as root but with CAP_SYS_ADMIN
// dropped (bounding+effective) via capsh. Unlike a setuid-to-nobody run — which
// fails at FILE ACCESS before ever reaching the privileged syscall (and on which
// xfs_quota misleadingly exits 0) — this keeps euid 0, so the mount is reachable
// and the quota syscall is actually reached with the capability missing,
// faithfully reproducing the daemon's failing condition. Skips if capsh
// (libcap2-bin) is unavailable.
func runWithoutCapSysAdmin(t *testing.T, cmdline string) ([]byte, error) {
	t.Helper()
	if _, err := exec.LookPath("capsh"); err != nil {
		t.Skip("capsh (libcap2-bin) not available; cannot drop CAP_SYS_ADMIN")
	}
	return exec.Command("capsh", "--drop=cap_sys_admin", "--", "-c", cmdline).CombinedOutput()
}

func TestIntegration_XFS_QuotaSet_RequiresCapSysAdmin(t *testing.T) {
	mount := fredtestutil.SetupXFSLoopback(t) // requires root
	ctx := context.Background()

	// Sanity: our own detector agrees the (root) test process can set quotas.
	canSet, err := daemonCanSetQuotas()
	require.NoError(t, err)
	require.True(t, canSet, "test must run as root to exercise this")

	// As root: setting the quota SUCCEEDS — proving the filesystem itself is fine
	// and it is purely the capability that gates the operation.
	rootOut, rootErr := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(xfsLimitCmd(987654321, "1m", 262144), mount)...).CombinedOutput()
	require.NoError(t, rootErr, "root should be able to set the quota: %s", rootOut)

	// With CAP_SYS_ADMIN dropped (euid still 0, so the mount is reachable and
	// quotactl(Q_XSETQLIM) is actually reached), the SAME command FAILS.
	dropOut, dropErr := runWithoutCapSysAdmin(t, fmt.Sprintf("xfs_quota -x -c '%s' %s", xfsLimitCmd(987654321, "1m", 262144), mount))
	require.Error(t, dropErr,
		"setting an XFS quota without CAP_SYS_ADMIN must fail: %s", dropOut)
	if len(dropOut) > 0 {
		assert.Contains(t, strings.ToLower(string(dropOut)), "not permitted",
			"expected an 'Operation not permitted' error, got: %s", dropOut)
	}
}

// TestIntegration_XFS_EnsureQuota_RepairsRootForNewWrites verifies that root-only
// repair restores inheritance and enforcement for new files. Historical untagged
// files remain outside the project; repairing them requires stopped-writer
// offline maintenance, never recursive work in startup or reuse.
func TestIntegration_XFS_EnsureQuota_RepairsRootForNewWrites(t *testing.T) {
	mount := fredtestutil.SetupXFSLoopback(t) // requires root
	dataPath := filepath.Join(mount, "volumes")
	require.NoError(t, os.MkdirAll(dataPath, 0700))
	ctx := context.Background()

	mgr, err := newVolumeManager(dataPath, "xfs", 1024, slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.Validate())

	const leaseUUID = "550e8400-e29b-41d4-a716-446655440105"
	volName := canonicalVolumeName(leaseUUID, "app", 0)
	const capMiB = int64(20)

	// Simulate a volume created by a pre-ENG-454 daemon: the directory and the
	// .fred-project-id marker exist, but the inode was never project-tagged and
	// no limit was set (the privileged xfs_quota calls silently no-op'd).
	dir := filepath.Join(dataPath, volName)
	require.NoError(t, os.MkdirAll(dir, 0700))
	require.NoError(t, writeProjectIDFile(dir, 424242))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.bin"), make([]byte, 10*1024*1024), 0600))

	// Pre-backfill: untagged → Usage cannot find its projid in the quota report.
	_, uerr := mgr.Usage(ctx, volName)
	require.Error(t, uerr, "an untagged volume must not be measurable before backfill")

	// Repair only the root via EnsureQuota (what reconcileVolumeQuotas invokes).
	require.NoError(t, ensureQuotaErr(mgr.EnsureQuota(ctx, volName, capMiB)))

	// Historical bytes must not be recursively charged to the repaired root.
	used, err := mgr.Usage(ctx, volName)
	require.NoError(t, err, "after root repair the project must be measurable")
	assert.Less(t, used, int64(10*1024*1024), "root repair must not retag historical data")

	// A file created after repair inherits the project and consumes its quota.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "new-data.bin"), make([]byte, 10*1024*1024), 0600))
	used, err = mgr.Usage(ctx, volName)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, used, int64(10*1024*1024), "new writes must be measured: %d", used)
	assert.Less(t, used, int64(20*1024*1024), "historical data must remain outside the project")

	// The new files exceed the repaired 20 MiB cap together and are rejected.
	werr := os.WriteFile(filepath.Join(dir, "big.bin"), make([]byte, 20*1024*1024), 0600)
	require.Error(t, werr, "new writes past the repaired cap must be quota-enforced")
	assert.True(t,
		errors.Is(werr, syscall.EDQUOT) || errors.Is(werr, syscall.ENOSPC) ||
			strings.Contains(werr.Error(), "disk quota exceeded") ||
			strings.Contains(werr.Error(), "no space left"),
		"expected a quota/space error, got: %v", werr)
}

// TestIntegration_XFS_ReconcileBackfill_EndToEnd exercises startup reconciliation
// against a normally created, project-tagged active volume. Reconciliation must
// preserve accounting and tighten the original cap to the lease's disk_mb. This
// composes enumeration (name derivation + existence gate + SKU sizing) with the
// real EnsureQuota, catching wiring bugs the mock-based unit test cannot.
func TestIntegration_XFS_ReconcileBackfill_EndToEnd(t *testing.T) {
	mount := fredtestutil.SetupXFSLoopback(t) // requires root
	dataPath := filepath.Join(mount, "volumes")
	require.NoError(t, os.MkdirAll(dataPath, 0700))
	ctx := context.Background()

	mgr, err := newVolumeManager(dataPath, "xfs", 1024, slog.Default())
	require.NoError(t, err)

	const lease = "550e8400-e29b-41d4-a716-446655440106"
	volName := canonicalVolumeName(lease, "app", 0)
	dir, created, err := mgr.Create(ctx, volName, 100)
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.bin"), make([]byte, 10*1024*1024), 0600))

	b := backendForReconcileTest(t, mgr, dataPath, lease, "app", "e2e-stateful", 20)

	used, err := mgr.Usage(ctx, volName)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, used, int64(10*1024*1024), "the fixture must already account for tenant data")

	require.NoError(t, b.reconcileVolumeQuotas(ctx))

	used, err = mgr.Usage(ctx, volName)
	require.NoError(t, err, "reconciliation must preserve project accounting")
	assert.GreaterOrEqual(t, used, int64(10*1024*1024), "Usage under-reports: %d", used)

	werr := os.WriteFile(filepath.Join(dir, "big.bin"), make([]byte, 20*1024*1024), 0600)
	require.Error(t, werr, "reconciliation must tighten the active volume's cap from 100 to 20 MiB")
	assert.True(t,
		errors.Is(werr, syscall.EDQUOT) || errors.Is(werr, syscall.ENOSPC) ||
			strings.Contains(werr.Error(), "disk quota exceeded") ||
			strings.Contains(werr.Error(), "no space left"),
		"expected a quota/space error, got: %v", werr)
}

// TestIntegration_XFS_ReconcileBackfill_RetainedVolume covers the retained-volume
// arm of startup reconciliation. A normally created volume is renamed into
// retention with its project tags intact, and reconciliation must apply the
// retained record's smaller cap while preserving accounting.
func TestIntegration_XFS_ReconcileBackfill_RetainedVolume(t *testing.T) {
	mount := fredtestutil.SetupXFSLoopback(t) // requires root
	dataPath := filepath.Join(mount, "volumes")
	require.NoError(t, os.MkdirAll(dataPath, 0700))
	ctx := context.Background()

	mgr, err := newVolumeManager(dataPath, "xfs", 1024, slog.Default())
	require.NoError(t, err)

	origLease := newIntegrationLeaseUUID()
	volName := canonicalVolumeName(origLease, "db", 0)
	retName := retainedName(volName)
	dir, created, err := mgr.Create(ctx, volName, 100)
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.bin"), make([]byte, 10*1024*1024), 0600))
	require.NoError(t, mgr.RenameVolume(ctx, volName, retName))
	dir = filepath.Join(dataPath, retName)

	b, rs := newBackendWithRetention(t)
	useVolumeManager(b, mgr)
	b.cfg.VolumeDataPath = dataPath
	b.cfg.SKUProfiles["ret-sku"] = SKUProfile{CPUCores: 1, MemoryMB: 256, DiskMB: 20}
	require.NoError(t, putRetentionForTest(t, rs, shared.RetentionEntry{
		OriginalLeaseUUID:   origLease,
		Tenant:              "t1",
		ProviderUUID:        testProviderUUID,
		Items:               []backend.LeaseItem{{SKU: "ret-sku", Quantity: 1, ServiceName: "db"}},
		RetainedVolumeNames: []string{retName},
		Status:              shared.RetentionStatusActive,
		CreatedAt:           time.Now(),
	}))

	used, err := mgr.Usage(ctx, retName)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, used, int64(10*1024*1024), "the retained fixture must already account for tenant data")

	require.NoError(t, b.reconcileVolumeQuotas(ctx))

	used, err = mgr.Usage(ctx, retName)
	require.NoError(t, err, "reconciliation must preserve retained project accounting")
	assert.GreaterOrEqual(t, used, int64(10*1024*1024), "Usage under-reports: %d", used)

	werr := os.WriteFile(filepath.Join(dir, "big.bin"), make([]byte, 20*1024*1024), 0600)
	require.Error(t, werr, "reconciliation must tighten the retained volume's cap from 100 to 20 MiB")
	assert.True(t,
		errors.Is(werr, syscall.EDQUOT) || errors.Is(werr, syscall.ENOSPC) ||
			strings.Contains(werr.Error(), "disk quota exceeded") ||
			strings.Contains(werr.Error(), "no space left"),
		"expected a quota/space error, got: %v", werr)
}

// TestIntegration_DemotePromote_XFS exercises the demote/promote quota flow on
// a real XFS loopback with project quotas (pquota): (a) the gate passes and
// the quota is lowered, (b) the gate refuses when data exceeds the smaller
// tier, (c) a promote raises the cap.
func TestIntegration_DemotePromote_XFS(t *testing.T) {
	mountDir := fredtestutil.SetupXFSLoopback(t)
	ctx := context.Background()

	const largeMiB, mediumMiB = int64(100), int64(20)

	mgr, err := newVolumeManager(mountDir, "xfs", 1024, slog.Default())
	require.NoError(t, err)

	b := newBackendForTest(&mockDockerClient{}, nil)
	useVolumeManager(b, mgr)
	b.cfg.SKUProfiles["test-large"] = SKUProfile{CPUCores: 0.5, MemoryMB: 512, DiskMB: largeMiB}
	b.cfg.SKUProfiles["test-medium"] = SKUProfile{CPUCores: 0.5, MemoryMB: 512, DiskMB: mediumMiB}
	mediumProfiles := map[string]SKUProfile{"test-medium": b.cfg.SKUProfiles["test-medium"]}

	// (a) fits: gate passes, XFS bhard lowered, write beyond medium fails.
	t.Run("fits_medium", func(t *testing.T) {
		origLease := newIntegrationLeaseUUID()
		canon := canonicalVolumeName(origLease, "app", 0)
		retained := retainedName(canon)

		hostPath, _, err := mgr.Create(ctx, canon, largeMiB)
		require.NoError(t, err)

		require.NoError(t, os.WriteFile(filepath.Join(hostPath, "data.bin"), make([]byte, 5*1024*1024), 0600))

		require.NoError(t, mgr.RenameVolume(context.Background(), canon, retained))
		t.Cleanup(func() { _ = volDestroyer(t, mgr).Destroy(ctx, retained) })

		rec := &shared.RetentionEntry{
			OriginalLeaseUUID: origLease,
			Items:             []backend.LeaseItem{{SKU: "test-large", ServiceName: "app", Quantity: 1}},
			ResourceProfiles: frozenRetainedProfilesForTest(t, b,
				[]backend.LeaseItem{{SKU: "test-large", ServiceName: "app", Quantity: 1}}),
			RetainedVolumeNames: []string{retained},
		}
		err = b.checkDemoteFit(ctx, rec,
			[]backend.LeaseItem{{SKU: "test-medium", ServiceName: "app", Quantity: 1}},
			mediumProfiles, b.logger)
		require.NoError(t, err, "5 MiB data fits 20 MiB medium: checkDemoteFit must pass")

		newLease := newIntegrationLeaseUUID()
		newCanon := canonicalVolumeName(newLease, "app", 0)
		require.NoError(t, mgr.RenameVolume(context.Background(), retained, newCanon))
		t.Cleanup(func() { _ = volDestroyer(t, mgr).Destroy(ctx, newCanon) })

		// Create at medium cap updates the XFS project bhard limit to 20 MiB.
		newHostPath, _, err := mgr.Create(ctx, newCanon, mediumMiB)
		require.NoError(t, err, "Create at medium cap must succeed")

		out, werr := exec.Command("dd", "if=/dev/zero",
			"of="+filepath.Join(newHostPath, "big.bin"), "bs=1M", "count=25").CombinedOutput()
		require.Error(t, werr,
			"writing 25 MiB to a 20 MiB-quota XFS volume must fail (EDQUOT); dd output: %s", out)
	})

	// (b) exceeds: gate refuses.
	t.Run("exceeds_medium", func(t *testing.T) {
		origLease := newIntegrationLeaseUUID()
		canon := canonicalVolumeName(origLease, "app", 0)
		retained := retainedName(canon)

		hostPath, _, err := mgr.Create(ctx, canon, largeMiB)
		require.NoError(t, err)
		t.Cleanup(func() { _ = volDestroyer(t, mgr).Destroy(ctx, canon) })

		out, werr := exec.Command("dd", "if=/dev/zero",
			"of="+filepath.Join(hostPath, "data.bin"), "bs=1M", "count=25").CombinedOutput()
		require.NoError(t, werr, "25 MiB must fit 100 MiB XFS quota; dd output: %s", out)

		require.NoError(t, mgr.RenameVolume(context.Background(), canon, retained))
		t.Cleanup(func() { _ = volDestroyer(t, mgr).Destroy(ctx, retained) })

		rec := &shared.RetentionEntry{
			OriginalLeaseUUID: origLease,
			Items:             []backend.LeaseItem{{SKU: "test-large", ServiceName: "app", Quantity: 1}},
			ResourceProfiles: frozenRetainedProfilesForTest(t, b,
				[]backend.LeaseItem{{SKU: "test-large", ServiceName: "app", Quantity: 1}}),
			RetainedVolumeNames: []string{retained},
		}
		err = b.checkDemoteFit(ctx, rec,
			[]backend.LeaseItem{{SKU: "test-medium", ServiceName: "app", Quantity: 1}},
			mediumProfiles, b.logger)
		require.ErrorIs(t, err, backend.ErrDemoteDataExceedsTier,
			"25 MiB data exceeds 20 MiB medium: gate must refuse")
	})

	// (c) promote: Create at large from a medium-capped project raises the XFS
	//     project bhard so writes beyond the medium cap now succeed.
	t.Run("promote_raises_cap", func(t *testing.T) {
		origLease := newIntegrationLeaseUUID()
		canon := canonicalVolumeName(origLease, "app", 0)

		// Start at medium cap (20 MiB).
		hostPath, _, err := mgr.Create(ctx, canon, mediumMiB)
		require.NoError(t, err)
		t.Cleanup(func() { _ = volDestroyer(t, mgr).Destroy(ctx, canon) })

		// Write 5 MiB so the project has some data.
		require.NoError(t, os.WriteFile(filepath.Join(hostPath, "data.bin"), make([]byte, 5*1024*1024), 0600))

		// Confirm 25 MiB does NOT fit under the medium bhard (partial write fails).
		out, werr := exec.Command("dd", "if=/dev/zero",
			"of="+filepath.Join(hostPath, "big.bin"), "bs=1M", "count=25").CombinedOutput()
		require.Error(t, werr,
			"25 MiB must not fit the 20 MiB bhard before promote; dd output: %s", out)
		// Remove any partial file so usage drops back toward the initial 5 MiB.
		_ = os.Remove(filepath.Join(hostPath, "big.bin"))

		// Promote: Create (idempotent) at large cap raises the project bhard to 100 MiB.
		_, _, err = mgr.Create(ctx, canon, largeMiB)
		require.NoError(t, err, "Create at large cap must succeed (bhard update)")

		// Writing 25 MiB must now succeed (bhard is 100 MiB; total at most ~30 MiB).
		out, werr = exec.Command("dd", "if=/dev/zero",
			"of="+filepath.Join(hostPath, "big2.bin"), "bs=1M", "count=25").CombinedOutput()
		require.NoError(t, werr,
			"25 MiB must fit the promoted 100 MiB bhard; dd output: %s", out)
	})
}
