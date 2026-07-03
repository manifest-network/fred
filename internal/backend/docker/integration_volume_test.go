//go:build integration

package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// setupBtrfsLoopback creates a btrfs filesystem on a loopback file and mounts it.
// Returns the mount path. Cleanup is registered via t.Cleanup.
// Skips the test if requirements (root, btrfs-progs) are not met.
func setupBtrfsLoopback(t *testing.T) string {
	t.Helper()

	// Must be root to mount filesystems
	if os.Getuid() != 0 {
		t.Skip("btrfs loopback requires root")
	}

	// Check btrfs tools
	if _, err := exec.LookPath("mkfs.btrfs"); err != nil {
		t.Skip("mkfs.btrfs not found:", err)
	}
	if _, err := exec.LookPath("btrfs"); err != nil {
		t.Skip("btrfs not found:", err)
	}

	tmpDir := t.TempDir()
	imgFile := filepath.Join(tmpDir, "btrfs.img")
	mountDir := filepath.Join(tmpDir, "mnt")
	require.NoError(t, os.MkdirAll(mountDir, 0755))

	// Create sparse file (256MB)
	out, err := exec.Command("truncate", "-s", "256M", imgFile).CombinedOutput()
	require.NoError(t, err, "truncate: %s", out)

	// Format as btrfs
	out, err = exec.Command("mkfs.btrfs", "-f", imgFile).CombinedOutput()
	require.NoError(t, err, "mkfs.btrfs: %s", out)

	// Mount with loop. Loop-device support is a prerequisite just like root and
	// btrfs-progs above, but it can't be probed reliably ahead of time. Some
	// environments (containers, restricted/hardened hosts, many CI runners) have
	// no usable loop devices even as root, where `mount -o loop` fails with a
	// "failed to set up loop device" error. Skip there rather than fail — only a
	// genuine (non-loop) mount error should fail the test.
	out, err = exec.Command("mount", "-o", "loop", imgFile, mountDir).CombinedOutput()
	if err != nil {
		if strings.Contains(string(out), "set up loop device") {
			t.Skipf("btrfs loopback unavailable (loop device setup failed): %s", out)
		}
		require.NoError(t, err, "mount: %s", out)
	}

	// Enable quotas
	out, err = exec.Command("btrfs", "quota", "enable", mountDir).CombinedOutput()
	require.NoError(t, err, "btrfs quota enable: %s", out)

	t.Cleanup(func() {
		// Unmount — ignore errors on cleanup
		_ = exec.Command("umount", "-l", mountDir).Run()
	})

	return mountDir
}

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
	docker, err := NewDockerClient("", "")
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
	docker, err := NewDockerClient("", "")
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

// containerHasTmpfsMount checks if a container has a tmpfs mount at the given destination.
func containerHasTmpfsMount(t *testing.T, containerID, dest string) bool {
	t.Helper()
	docker, err := NewDockerClient("", "")
	require.NoError(t, err)
	defer func() { _ = docker.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	inspect, err := docker.client.ContainerInspect(ctx, containerID)
	require.NoError(t, err)

	// Check Tmpfs map (used for explicit tmpfs mounts)
	if _, ok := inspect.HostConfig.Tmpfs[dest]; ok {
		return true
	}

	// Also check Mounts list for tmpfs type
	for _, m := range inspect.Mounts {
		if m.Destination == dest && m.Type == "tmpfs" {
			return true
		}
	}
	return false
}

// drainCallbacks consumes all pending callbacks from the channel without blocking.
func drainCallbacks(ch <-chan backend.CallbackPayload) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func TestIntegration_Docker_StatefulVolumeLifecycle(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.VolumeDataPath = mountPath
		cfg.VolumeFilesystem = "btrfs"
	})

	ctx := context.Background()
	leaseUUID := fmt.Sprintf("vol-lifecycle-%d", time.Now().UnixNano())

	// Redis declares VOLUME /data
	appManifest := manifest.Manifest{
		Image:   "redis:7",
		Command: []string{"redis-server", "--save", "1", "1"},
	}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)

	// 1. Provision redis with stateful SKU
	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	// 2. Wait for success callback
	select {
	case cb := <-callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(3 * time.Minute):
		t.Fatal("timeout waiting for provision callback")
	}

	// 3. Verify btrfs subvolume exists
	volumeID := fmt.Sprintf("fred-%s-%s-0", leaseUUID, manifest.DefaultServiceName)
	subvolPath := filepath.Join(mountPath, volumeID)
	out, err := exec.Command("btrfs", "subvolume", "show", subvolPath).CombinedOutput()
	require.NoError(t, err, "btrfs subvolume should exist: %s", out)

	// 4. Verify container has bind mount for /data
	containerID := getContainerID(t, leaseUUID)
	assert.True(t, containerHasBindMount(t, containerID, "/data"),
		"container should have bind mount at /data")

	// 5. Write data to redis
	execInContainer(t, containerID, []string{"redis-cli", "SET", "testkey", "testvalue"})
	execInContainer(t, containerID, []string{"redis-cli", "SAVE"})

	// 6. Verify data file exists on host volume
	dataDir := filepath.Join(subvolPath, "data")
	entries, err := os.ReadDir(dataDir)
	require.NoError(t, err, "should be able to read volume data directory")
	found := false
	for _, e := range entries {
		if e.Name() == "dump.rdb" {
			found = true
			break
		}
	}
	assert.True(t, found, "dump.rdb should exist in volume data directory")

	// 7. Deprovision
	err = b.Deprovision(ctx, leaseUUID)
	require.NoError(t, err)

	// 8. Verify btrfs subvolume destroyed
	_, err = exec.Command("btrfs", "subvolume", "show", subvolPath).CombinedOutput()
	assert.Error(t, err, "btrfs subvolume should be destroyed after deprovision")

	_, statErr := os.Stat(subvolPath)
	assert.True(t, errors.Is(statErr, fs.ErrNotExist), "volume directory should be gone after deprovision")
}

func TestIntegration_Docker_VolumePersistsAcrossReProvision(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.VolumeDataPath = mountPath
		cfg.VolumeFilesystem = "btrfs"
		cfg.ReconcileInterval = 2 * time.Second // fast reconciler for re-provision
	})

	ctx := context.Background()
	leaseUUID := fmt.Sprintf("vol-persist-%d", time.Now().UnixNano())

	appManifest := manifest.Manifest{
		Image:   "redis:7",
		Command: []string{"redis-server", "--save", "1", "1"},
	}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)

	// 1. Provision redis
	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	select {
	case cb := <-callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(3 * time.Minute):
		t.Fatal("timeout waiting for initial provision callback")
	}

	// 2. Write data
	containerID := getContainerID(t, leaseUUID)
	execInContainer(t, containerID, []string{"redis-cli", "SET", "persist_key", "persist_value"})
	execInContainer(t, containerID, []string{"redis-cli", "SAVE"})

	// Record subvolume path for later comparison
	volumeID := fmt.Sprintf("fred-%s-%s-0", leaseUUID, manifest.DefaultServiceName)
	subvolPath := filepath.Join(mountPath, volumeID)
	_, err = os.Stat(subvolPath)
	require.NoError(t, err, "subvolume should exist before kill")

	// 3. Kill the container to simulate crash
	killContainer(t, containerID)
	waitForContainerExited(t, containerID)

	// 4. Wait for reconciler to detect failure
	waitForProvisionStatus(t, b, leaseUUID, backend.ProvisionStatusFailed, 30*time.Second)

	// Drain any failure callbacks sent by the reconciler before re-provisioning.
	drainCallbacks(callbackCh)

	// 5. Re-provision same lease
	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	// 6. Wait for success callback.
	//    recoverState may fire a "failed" callback when it detects the killed
	//    container (race with the re-provision). Skip any "failed" callbacks
	//    until we receive "success".
	successTimeout := time.After(3 * time.Minute)
	gotSuccess := false
	for !gotSuccess {
		select {
		case cb := <-callbackCh:
			if cb.Status == backend.CallbackStatusSuccess {
				gotSuccess = true
			} else {
				t.Logf("skipping non-success callback (status=%s) while waiting for re-provision success", cb.Status)
			}
		case <-successTimeout:
			t.Fatal("timeout waiting for re-provision success callback")
		}
	}

	// 7. Verify data persisted
	newContainerID := getContainerID(t, leaseUUID)
	result := execInContainer(t, newContainerID, []string{"redis-cli", "GET", "persist_key"})
	assert.Contains(t, result, "persist_value",
		"data should persist across re-provision")

	// 8. Verify same subvolume reused (path still exists)
	_, err = os.Stat(subvolPath)
	assert.NoError(t, err, "same subvolume path should still exist after re-provision")

	// Cleanup
	err = b.Deprovision(ctx, leaseUUID)
	require.NoError(t, err)
}

func TestIntegration_Docker_EphemeralVolumeOverrideTmpfs(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)

	// Use only ephemeral SKUs (DiskMB=0) so volume_data_path is not required.
	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.SKUProfiles = map[string]SKUProfile{
			"docker-ephemeral": {
				CPUCores: 0.5,
				MemoryMB: 512,
				DiskMB:   0,
			},
		}
	})

	ctx := context.Background()
	leaseUUID := fmt.Sprintf("ephemeral-%d", time.Now().UnixNano())

	// Redis declares VOLUME /data
	appManifest := manifest.Manifest{
		Image:   "redis:7",
		Command: []string{"redis-server"},
	}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)

	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-ephemeral", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	select {
	case cb := <-callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(3 * time.Minute):
		t.Fatal("timeout waiting for provision callback")
	}

	// Verify container has tmpfs at /data (not a bind mount)
	containerID := getContainerID(t, leaseUUID)
	assert.True(t, containerHasTmpfsMount(t, containerID, "/data"),
		"ephemeral SKU should use tmpfs for /data")
	assert.False(t, containerHasBindMount(t, containerID, "/data"),
		"ephemeral SKU should NOT have bind mount for /data")

	// Cleanup
	err = b.Deprovision(ctx, leaseUUID)
	require.NoError(t, err)
}

// TestIntegration_Docker_MultiInstanceVolumeIsolation verifies that a
// multi-unit lease (Quantity=2) gets separate, isolated btrfs subvolumes
// per instance. Data written in one container must not be visible in the other.
func TestIntegration_Docker_MultiInstanceVolumeIsolation(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.VolumeDataPath = mountPath
		cfg.VolumeFilesystem = "btrfs"
	})

	ctx := context.Background()
	leaseUUID := fmt.Sprintf("vol-multi-%d", time.Now().UnixNano())

	// redis:7 declares VOLUME /data
	appManifest := manifest.Manifest{
		Image:   "redis:7",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)

	// Provision with Quantity=2 → two containers, two volumes
	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 2}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	select {
	case cb := <-callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(3 * time.Minute):
		t.Fatal("timeout waiting for provision callback")
	}

	// Verify two separate btrfs subvolumes exist
	subvol0 := filepath.Join(mountPath, fmt.Sprintf("fred-%s-%s-0", leaseUUID, manifest.DefaultServiceName))
	subvol1 := filepath.Join(mountPath, fmt.Sprintf("fred-%s-%s-1", leaseUUID, manifest.DefaultServiceName))

	out, err := exec.Command("btrfs", "subvolume", "show", subvol0).CombinedOutput()
	require.NoError(t, err, "subvolume 0 should be a btrfs subvolume: %s", out)
	out, err = exec.Command("btrfs", "subvolume", "show", subvol1).CombinedOutput()
	require.NoError(t, err, "subvolume 1 should be a btrfs subvolume: %s", out)

	// Get both containers
	containers := inspectProvisionContainers(t, leaseUUID)
	require.Len(t, containers, 2, "expected 2 containers")

	// Write a unique marker to /data in each container
	execInContainer(t, containers[0].ID, []string{"sh", "-c", "echo data-alpha > /data/marker.txt"})
	execInContainer(t, containers[1].ID, []string{"sh", "-c", "echo data-bravo > /data/marker.txt"})

	// Read back from each container and verify it only sees its own data
	out0 := execInContainer(t, containers[0].ID, []string{"cat", "/data/marker.txt"})
	out1 := execInContainer(t, containers[1].ID, []string{"cat", "/data/marker.txt"})
	assert.Contains(t, out0, "data-alpha", "container 0 should see its own marker")
	assert.Contains(t, out1, "data-bravo", "container 1 should see its own marker")
	assert.NotEqual(t, out0, out1, "containers should have different data (isolated volumes)")

	// Verify on the host filesystem: subvolumes contain different files
	hostData0, err := os.ReadFile(filepath.Join(subvol0, "data", "marker.txt"))
	require.NoError(t, err)
	hostData1, err := os.ReadFile(filepath.Join(subvol1, "data", "marker.txt"))
	require.NoError(t, err)
	assert.NotEqual(t, string(hostData0), string(hostData1),
		"host subvolumes should contain different data")

	// Deprovision and verify both subvolumes are destroyed
	err = b.Deprovision(ctx, leaseUUID)
	require.NoError(t, err)

	_, err = os.Stat(subvol0)
	assert.True(t, errors.Is(err, fs.ErrNotExist), "subvolume 0 should be destroyed after deprovision, got err: %v", err)
	_, err = os.Stat(subvol1)
	assert.True(t, errors.Is(err, fs.ErrNotExist), "subvolume 1 should be destroyed after deprovision, got err: %v", err)
}

// TestIntegration_Docker_OrphanedVolumeCleanup verifies that orphaned volumes
// (created but no matching provision) are destroyed when a new backend starts.
// This simulates a crash between container removal and volume destruction.
func TestIntegration_Docker_OrphanedVolumeCleanup(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	callbackServer, callbackCh := startCallbackServer(t)

	// Build config manually to control backend lifecycle (like ColdStartRecovery tests)
	cfg := DefaultConfig()
	cfg.SKUProfiles = defaultTestSKUProfiles()
	cfg.Name = fmt.Sprintf("test-orphan-vol-%d", time.Now().UnixNano())
	cfg.CallbackSecret = testCallbackSecret
	cfg.HostAddress = "127.0.0.1"
	cfg.StartupVerifyDuration = 1 * time.Second
	cfg.ReconcileInterval = 1 * time.Hour
	cfg.ProvisionTimeout = 2 * time.Minute
	cfg.NetworkIsolation = ptrBool(false)
	cfg.VolumeDataPath = mountPath
	cfg.VolumeFilesystem = "btrfs"
	tmpDir := t.TempDir()
	cfg.CallbackDBPath = filepath.Join(tmpDir, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(tmpDir, "diagnostics.db")

	logger := slog.Default()
	b1, err := New(cfg, logger)
	require.NoError(t, err)

	ctx := context.Background()
	err = b1.Start(ctx)
	require.NoError(t, err)
	b1Stopped := false
	t.Cleanup(func() {
		if !b1Stopped {
			_ = b1.Stop()
		}
	})

	dockerCli, err := NewDockerClient("", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupTestContainers(t, dockerCli, cfg.Name)
		cleanupTestNetworks(t, dockerCli, cfg.Name)
		_ = dockerCli.Close()
	})

	leaseUUID := fmt.Sprintf("orphan-vol-%d", time.Now().UnixNano())

	// redis:7 declares VOLUME /data → triggers btrfs subvolume creation
	appManifest := manifest.Manifest{
		Image:   "redis:7",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)

	err = b1.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	select {
	case cb := <-callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(3 * time.Minute):
		t.Fatal("timeout waiting for provision callback")
	}

	// Verify subvolume exists
	volumeID := fmt.Sprintf("fred-%s-%s-0", leaseUUID, manifest.DefaultServiceName)
	subvolPath := filepath.Join(mountPath, volumeID)
	_, err = os.Stat(subvolPath)
	require.NoError(t, err, "subvolume should exist after provision")

	// Stop the first backend
	err = b1.Stop()
	require.NoError(t, err)
	b1Stopped = true

	// Force-remove the container (simulates crash that removes container but
	// leaves the volume orphaned on disk)
	provContainers := inspectProvisionContainers(t, leaseUUID)
	require.Len(t, provContainers, 1)
	err = dockerCli.client.ContainerRemove(ctx, provContainers[0].ID, container.RemoveOptions{Force: true})
	require.NoError(t, err)

	// Volume should still exist on disk after container removal
	_, err = os.Stat(subvolPath)
	require.NoError(t, err, "volume should survive container removal")

	// Start a new backend with the same config.
	// Start() calls recoverState (no containers found) then cleanupOrphanedVolumes
	// (finds volume with no matching provision → destroys it).
	b2, err := New(cfg, logger)
	require.NoError(t, err)
	err = b2.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = b2.Stop() })

	// Orphaned volume should have been destroyed during startup
	_, statErr := os.Stat(subvolPath)
	assert.True(t, errors.Is(statErr, fs.ErrNotExist),
		"orphaned volume should be destroyed after backend restart, got err: %v", statErr)
}

// TestIntegration_Docker_BtrfsRenameVolume_PreservesSubvolID verifies
// the recover-time legacy→stack migration invariant on real btrfs:
// when a managed volume directory is renamed (legacy name →
// service-aware name), the underlying btrfs subvolume's UUID/ID
// must be preserved. Renaming a btrfs subvolume via os.Rename
// preserves the subvolume identity (Btrfs treats subvolumes as
// special directories; renaming the directory does NOT copy or
// reflink — it's a metadata-only operation that updates the parent
// directory entry).
//
// If this test fails, it means a future change to the rename path
// has accidentally substituted a copy-then-delete for the metadata
// rename, which would re-id the subvolume (losing snapshot lineage,
// quota assignment, and any subvol-id-keyed btrfs feature state).
func TestIntegration_Docker_BtrfsRenameVolume_PreservesSubvolID(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)

	mgr := &btrfsVolumeManager{dataPath: mountPath, logger: slog.Default()}

	const oldName = "fred-rename-legacy-0"
	const newName = "fred-rename-legacy-app-0"

	// Create a real btrfs subvolume at the legacy path.
	oldPath := filepath.Join(mountPath, oldName)
	out, err := exec.Command("btrfs", "subvolume", "create", oldPath).CombinedOutput()
	require.NoError(t, err, "btrfs subvolume create: %s", out)

	// Record original subvol id (line `Subvolume ID: NNN` in btrfs show output).
	originalID := extractSubvolID(t, oldPath)
	require.NotEmpty(t, originalID, "must extract subvol id before rename")

	// Rename via the manager.
	require.NoError(t, mgr.RenameVolume(oldName, newName))

	// New path exists, old path is gone.
	newPath := filepath.Join(mountPath, newName)
	_, err = os.Stat(newPath)
	require.NoError(t, err, "new path must exist after rename")
	_, err = os.Stat(oldPath)
	require.True(t, errors.Is(err, fs.ErrNotExist), "old path must be gone after rename")

	// Subvol id must be unchanged at the new path.
	renamedID := extractSubvolID(t, newPath)
	require.NotEmpty(t, renamedID, "must extract subvol id after rename")
	assert.Equal(t, originalID, renamedID,
		"btrfs subvolume ID must be preserved across rename (rename is a metadata-only op; "+
			"a re-id would indicate the rename was implemented as copy+delete, losing identity)")

	// Cleanup: delete the renamed subvolume.
	out, _ = exec.Command("btrfs", "subvolume", "delete", newPath).CombinedOutput()
	t.Logf("cleanup btrfs subvolume delete: %s", out)
}

// extractSubvolID parses `btrfs subvolume show <path>` output for the
// "Subvolume ID:" line and returns the numeric id as a string. The
// raw string form (not parsed int) is fine for equality assertions.
func extractSubvolID(t *testing.T, path string) string {
	t.Helper()
	out, err := exec.Command("btrfs", "subvolume", "show", path).CombinedOutput()
	require.NoError(t, err, "btrfs subvolume show: %s", out)
	for _, line := range strings.Split(string(out), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "Subvolume ID:") {
			return strings.TrimSpace(strings.TrimPrefix(trimmed, "Subvolume ID:"))
		}
	}
	return ""
}

// TestIntegration_Docker_VolumeQuotaEnforced verifies that btrfs quota
// enforcement actually prevents writes beyond the configured disk limit.
// A small write should succeed; a large write should fail with ENOSPC/EDQUOT.
func TestIntegration_Docker_VolumeQuotaEnforced(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.VolumeDataPath = mountPath
		cfg.VolumeFilesystem = "btrfs"
		// Custom SKU with a tiny 5MB disk quota
		cfg.SKUProfiles = map[string]SKUProfile{
			"docker-tiny": {
				CPUCores: 0.25,
				MemoryMB: 256,
				DiskMB:   5,
			},
		}
	})

	ctx := context.Background()
	leaseUUID := fmt.Sprintf("vol-quota-%d", time.Now().UnixNano())

	// redis:7 declares VOLUME /data → bind-mounted to 5MB btrfs subvolume.
	// Using "sleep" as command so redis-server doesn't start and consume space.
	appManifest := manifest.Manifest{
		Image:   "redis:7",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)

	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-tiny", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	select {
	case cb := <-callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(3 * time.Minute):
		t.Fatal("timeout waiting for provision callback")
	}

	containerID := getContainerID(t, leaseUUID)

	// Small write (1KB) should succeed — well within 5MB quota
	out, err := exec.Command("docker", "exec", containerID,
		"dd", "if=/dev/zero", "of=/data/small.bin", "bs=1K", "count=1",
	).CombinedOutput()
	require.NoError(t, err, "small write should succeed: %s", out)

	// Large write (50MB) should fail — far exceeds 5MB quota
	out, err = exec.Command("docker", "exec", containerID,
		"dd", "if=/dev/zero", "of=/data/big.bin", "bs=1M", "count=50",
	).CombinedOutput()
	require.Error(t, err, "writing 50MB to a 5MB-quota volume should fail (quota enforcement broken?)")
	outStr := string(out)
	require.True(t,
		strings.Contains(outStr, "No space left") ||
			strings.Contains(outStr, "Disk quota exceeded"),
		"error should indicate quota/space limit, got: %s", outStr)

	// Cleanup
	err = b.Deprovision(ctx, leaseUUID)
	require.NoError(t, err)
}

// setupXFSLoopback creates an XFS filesystem on a loopback file, mounted with
// the pquota option required for project-quota enforcement. Returns the mount
// path. Cleanup (umount) is registered via t.Cleanup.
// Skips if root, mkfs.xfs, xfs_quota, or a usable loop device are absent.
func setupXFSLoopback(t *testing.T) string {
	t.Helper()

	if os.Getuid() != 0 {
		t.Skip("xfs loopback requires root")
	}
	if _, err := exec.LookPath("mkfs.xfs"); err != nil {
		t.Skip("mkfs.xfs not found:", err)
	}
	if _, err := exec.LookPath("xfs_quota"); err != nil {
		t.Skip("xfs_quota not found:", err)
	}

	tmpDir := t.TempDir()
	imgFile := filepath.Join(tmpDir, "xfs.img")
	mountDir := filepath.Join(tmpDir, "mnt")
	require.NoError(t, os.MkdirAll(mountDir, 0755))

	// 512M: modern xfsprogs refuses to mkfs an XFS filesystem smaller than
	// 300MB ("Filesystem must be larger than 300MB"). The per-volume project
	// quotas (tens of MiB) are independent of this backing-image size.
	out, err := exec.Command("truncate", "-s", "512M", imgFile).CombinedOutput()
	require.NoError(t, err, "truncate: %s", out)

	out, err = exec.Command("mkfs.xfs", "-f", imgFile).CombinedOutput()
	require.NoError(t, err, "mkfs.xfs: %s", out)

	// pquota enables project-quota enforcement. Same loop-skip guard as btrfs.
	out, err = exec.Command("mount", "-o", "loop,pquota", imgFile, mountDir).CombinedOutput()
	if err != nil {
		if strings.Contains(string(out), "set up loop device") {
			t.Skipf("xfs loopback unavailable (loop device setup failed): %s", out)
		}
		require.NoError(t, err, "mount: %s", out)
	}

	t.Cleanup(func() {
		_ = exec.Command("umount", "-l", mountDir).Run()
	})

	return mountDir
}

// setupZFSPool creates a ZFS pool backed by a sparse loopback image and
// mounts it at a dedicated directory. Returns the mountpoint and pool name.
// Cleanup (zpool destroy) is registered via t.Cleanup.
// Skips if root, zfs, or zpool are absent, or if pool creation fails with a
// device-unavailable error (common in containers without ZFS kernel modules).
func setupZFSPool(t *testing.T) (mountPath, poolName string) {
	t.Helper()

	if os.Getuid() != 0 {
		t.Skip("zfs pool requires root")
	}
	if _, err := exec.LookPath("zfs"); err != nil {
		t.Skip("zfs not found:", err)
	}
	if _, err := exec.LookPath("zpool"); err != nil {
		t.Skip("zpool not found:", err)
	}

	tmpDir := t.TempDir()
	imgFile := filepath.Join(tmpDir, "zfs.img")
	// Pool names must be unique to avoid collisions when tests run in parallel.
	poolName = fmt.Sprintf("fredtest%d", time.Now().UnixNano())
	mountPath = filepath.Join(tmpDir, "mnt")
	require.NoError(t, os.MkdirAll(mountPath, 0755))

	// 512 MiB: ZFS needs more headroom than btrfs/xfs due to metadata.
	out, err := exec.Command("truncate", "-s", "512M", imgFile).CombinedOutput()
	require.NoError(t, err, "truncate: %s", out)

	out, err = exec.Command("zpool", "create", "-m", mountPath, poolName, imgFile).CombinedOutput()
	if err != nil {
		s := string(out)
		if strings.Contains(s, "Unable to open") ||
			strings.Contains(s, "no such device") ||
			strings.Contains(s, "not a block device") ||
			strings.Contains(s, "permission denied") {
			t.Skipf("zfs pool creation unavailable: %s", s)
		}
		require.NoError(t, err, "zpool create: %s", out)
	}

	t.Cleanup(func() {
		_ = exec.Command("zpool", "destroy", "-f", poolName).Run()
	})

	return mountPath, poolName
}

// writeIncompressibleMiB writes mib MiB of incompressible data to path and
// syncs. ZFS defaults to compression=on, and with any compression setting it
// stores all-zero blocks as holes — so zero-filled test data (dd if=/dev/zero
// / make([]byte,N)) would be elided and never consume space, leaving
// `referenced` at the empty baseline and the refquota unfilled. /dev/urandom is
// incompressible so the bytes are actually allocated; the sync flushes the txg
// so `referenced` reflects the write immediately (it updates at txg commit).
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
	b.volumes = mgr
	b.cfg.VolumeDataPath = dataPath
	b.cfg.SKUProfiles[sku] = SKUProfile{CPUCores: 1, MemoryMB: 256, DiskMB: diskMB}
	return b
}

// TestIntegration_Usage_Btrfs verifies that btrfsVolumeManager.Usage returns
// the subvolume's referenced bytes within a generous metadata-overhead bound.
func TestIntegration_Usage_Btrfs(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	ctx := context.Background()

	mgr := &btrfsVolumeManager{dataPath: mountPath, logger: slog.Default()}

	const volName = "fred-int-usage-btrfs-app-0"
	const capMiB = int64(100)

	hostPath, created, err := mgr.Create(ctx, volName, capMiB)
	require.NoError(t, err)
	require.True(t, created)
	t.Cleanup(func() { _ = mgr.Destroy(ctx, volName) })

	const writeMiB = 10
	require.NoError(t, os.WriteFile(filepath.Join(hostPath, "data.bin"), make([]byte, writeMiB*1024*1024), 0600))

	got, err := mgr.Usage(ctx, volName)
	require.NoError(t, err)

	// rfer includes filesystem metadata; allow a 6 MiB overhead ceiling.
	const wantMin = int64(writeMiB * 1024 * 1024)
	const wantMax = int64((writeMiB + 6) * 1024 * 1024)
	assert.GreaterOrEqual(t, got, wantMin, "Usage too low: got %d, want >= %d", got, wantMin)
	assert.LessOrEqual(t, got, wantMax, "Usage too high: got %d, want <= %d", got, wantMax)
}

// TestIntegration_Usage_XFS verifies that xfsVolumeManager.Usage returns the
// project's used bytes (1 KiB block granularity) within a metadata-overhead bound.
func TestIntegration_Usage_XFS(t *testing.T) {
	mountDir := setupXFSLoopback(t)
	ctx := context.Background()

	mgr, err := newVolumeManager(mountDir, "xfs", slog.Default())
	require.NoError(t, err)

	const volName = "fred-int-usage-xfs-app-0"
	const capMiB = int64(100)

	hostPath, created, err := mgr.Create(ctx, volName, capMiB)
	require.NoError(t, err)
	require.True(t, created)
	t.Cleanup(func() { _ = mgr.Destroy(ctx, volName) })

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
	mount := setupXFSLoopback(t)
	dataPath := filepath.Join(mount, "volumes") // subdir of the XFS mount
	require.NoError(t, os.MkdirAll(dataPath, 0700))

	ctx := context.Background()
	mgr, err := newVolumeManager(dataPath, "xfs", slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.Validate())

	const volName = "fred-int-xfs-subdir-app-0"
	const capMiB = int64(20)
	hostPath, created, err := mgr.Create(ctx, volName, capMiB)
	require.NoError(t, err)
	require.True(t, created)
	t.Cleanup(func() { _ = mgr.Destroy(ctx, volName) })

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

// countXfsProjectQuotaEntries returns the number of project-quota entries the
// kernel reports for the mount — one row per project dquot that `report -p` emits.
// On a modern (GETNEXTQUOTA) kernel the walk skips fully-uninitialized dquots
// (all limits AND all usage zero), so a project with a leftover bhard limit but 0
// usage still counts, while a properly-cleared one does not. This is exactly the
// quantity ENG-459 watches for a leak. `-N` suppresses the header rows.
func countXfsProjectQuotaEntries(t *testing.T, mount string) int {
	t.Helper()
	out, err := exec.Command("xfs_quota", xfsQuotaArgs("report -p -N", mount)...).CombinedOutput()
	require.NoError(t, err, "xfs_quota report -p: %s", out)
	n := 0
	for _, line := range strings.Split(string(out), "\n") {
		if len(strings.Fields(line)) >= 2 {
			n++
		}
	}
	return n
}

// TestIntegration_XFS_Destroy_ClearsQuotaEntry is the ENG-459 regression. Since
// ENG-449/454 made per-volume project quotas actually apply, Create sets a projid +
// bhard limit on every volume; the pre-fix Destroy removed the directory but left
// the on-disk limit set, so every closed lease leaked one project-quota entry and
// each entry taxed every subsequent xfs_quota scan (report -p in Usage/Validate),
// degrading provisioning latency cumulatively. This asserts the acceptance
// criterion: after a create→Destroy cycle the project-quota entry count returns to
// its pre-create baseline (no leak). It also empirically pins the "bhard=0 + dir
// removed hides the entry" kernel behavior on the CI kernel — the one claim the
// mechanism research could confirm by semantics but not by primary documentation.
func TestIntegration_XFS_Destroy_ClearsQuotaEntry(t *testing.T) {
	mount := setupXFSLoopback(t)
	dataPath := filepath.Join(mount, "volumes") // subdir of the mount (production layout)
	require.NoError(t, os.MkdirAll(dataPath, 0700))
	ctx := context.Background()

	mgr, err := newVolumeManager(dataPath, "xfs", slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.Validate())

	baseline := countXfsProjectQuotaEntries(t, mount)

	const n = 8
	const capMiB = int64(20)
	names := make([]string, n)
	for i := range n {
		name := fmt.Sprintf("fred-int-eng459-app-%d", i)
		names[i] = name
		_, created, err := mgr.Create(ctx, name, capMiB)
		require.NoError(t, err)
		require.True(t, created)
	}

	// Each Create must add exactly one project-quota entry (the leak's raw material).
	require.Equal(t, baseline+n, countXfsProjectQuotaEntries(t, mount),
		"each created volume must add one project-quota entry")

	for _, name := range names {
		require.NoError(t, mgr.Destroy(ctx, name))
	}

	// The fix: Destroy zeroes each project's limit and removes its dir, so every
	// dquot returns to uninitialized and GETNEXTQUOTA stops reporting it. Pre-fix
	// this stayed at baseline+n (limits left set) — the leak.
	assert.Equal(t, baseline, countXfsProjectQuotaEntries(t, mount),
		"ENG-459: after create→Destroy the project-quota entry count must return to baseline (no leak)")
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
	mount := setupXFSLoopback(t) // requires root
	ctx := context.Background()

	// Sanity: our own detector agrees the (root) test process can set quotas.
	canSet, err := daemonCanSetQuotas()
	require.NoError(t, err)
	require.True(t, canSet, "test must run as root to exercise this")

	// As root: setting the quota SUCCEEDS — proving the filesystem itself is fine
	// and it is purely the capability that gates the operation.
	rootOut, rootErr := exec.CommandContext(ctx, "xfs_quota", xfsQuotaArgs(xfsLimitCmd(987654321, "1m"), mount)...).CombinedOutput()
	require.NoError(t, rootErr, "root should be able to set the quota: %s", rootOut)

	// With CAP_SYS_ADMIN dropped (euid still 0, so the mount is reachable and
	// quotactl(Q_XSETQLIM) is actually reached), the SAME command FAILS.
	dropOut, dropErr := runWithoutCapSysAdmin(t, fmt.Sprintf("xfs_quota -x -c 'limit -p bhard=1m 987654321' %s", mount))
	require.Error(t, dropErr,
		"setting an XFS quota without CAP_SYS_ADMIN must fail: %s", dropOut)
	if len(dropOut) > 0 {
		assert.Contains(t, strings.ToLower(string(dropOut)), "not permitted",
			"expected an 'Operation not permitted' error, got: %s", dropOut)
	}
}

// TestIntegration_Btrfs_QuotaSet_RequiresCapSysAdmin is the btrfs analogue of
// the ENG-454 regression: btrfs `qgroup limit` is a privileged ioctl requiring
// CAP_SYS_ADMIN, and (unlike zfs) has no delegation alternative — so the daemon
// must hold the capability. Proven by running it as root (succeeds) vs a
// dropped-privilege subprocess (fails). Guards against the same "tests run as
// root, prod runs unprivileged" fidelity gap on btrfs hosts.
func TestIntegration_Btrfs_QuotaSet_RequiresCapSysAdmin(t *testing.T) {
	mount := setupBtrfsLoopback(t) // requires root; quota enabled
	ctx := context.Background()

	subvol := filepath.Join(mount, "fred-cap-probe-app-0")
	out, err := exec.CommandContext(ctx, "btrfs", "subvolume", "create", subvol).CombinedOutput()
	require.NoError(t, err, "root subvolume create: %s", out)

	// As root: setting the qgroup limit SUCCEEDS.
	rootOut, rootErr := exec.CommandContext(ctx, "btrfs", "qgroup", "limit", "10M", subvol).CombinedOutput()
	require.NoError(t, rootErr, "root should set the qgroup limit: %s", rootOut)

	// With CAP_SYS_ADMIN dropped (euid 0, so the qgroup ioctl is reached): FAILS.
	dropOut, dropErr := runWithoutCapSysAdmin(t, fmt.Sprintf("btrfs qgroup limit 5M %s", subvol))
	require.Error(t, dropErr,
		"setting a btrfs qgroup limit without CAP_SYS_ADMIN must fail: %s", dropOut)
	if len(dropOut) > 0 { // message shape is a bonus signal; require.Error is the guarantee
		assert.Contains(t, strings.ToLower(string(dropOut)), "not permitted",
			"expected a permission error, got: %s", dropOut)
	}
}

// TestIntegration_Zfs_QuotaSet_RequiresPrivilege documents that setting a zfs
// refquota requires privilege for a plain (non-delegated) non-root process —
// proven by root (succeeds) vs a dropped-privilege subprocess (fails). ZFS is
// the deliberate exception to the startup CAP_SYS_ADMIN guard: `zfs allow` can
// delegate create/quota to a non-root user WITHOUT the capability, which is why
// requireCapSysAdmin is not called for zfs. This test pins the underlying
// privilege requirement (absent delegation) so the exemption stays intentional.
func TestIntegration_Zfs_QuotaSet_RequiresPrivilege(t *testing.T) {
	_, pool := setupZFSPool(t) // requires root
	ctx := context.Background()

	dataset := pool + "/fred-cap-probe"
	out, err := exec.CommandContext(ctx, "zfs", "create", dataset).CombinedOutput()
	require.NoError(t, err, "root zfs create: %s", out)

	// As root: setting refquota SUCCEEDS.
	rootOut, rootErr := exec.CommandContext(ctx, "zfs", "set", "refquota=10M", dataset).CombinedOutput()
	require.NoError(t, rootErr, "root should set refquota: %s", rootOut)

	// As a dropped-privilege (nobody) child with no `zfs allow` delegation: FAILS.
	dropped := exec.CommandContext(ctx, "zfs", "set", "refquota=5M", dataset)
	dropped.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{Uid: 65534, Gid: 65534},
	}
	dropOut, dropErr := dropped.CombinedOutput()
	require.Error(t, dropErr,
		"a non-root process without zfs-allow delegation must NOT set refquota (got success: %s)", dropOut)
	if len(dropOut) > 0 { // message shape is a bonus signal; require.Error is the guarantee
		assert.Contains(t, strings.ToLower(string(dropOut)), "permission",
			"expected a permission error, got: %s", dropOut)
	}
}

// TestIntegration_XFS_Backfill_TagsUntaggedVolume proves the ENG-454 startup
// backfill mechanism: EnsureQuota (what reconcileVolumeQuotas invokes) re-tags +
// limits a volume that a pre-CAP_SYS_ADMIN daemon left untagged and unenforced —
// restoring measurement + enforcement with no re-provision or data move.
func TestIntegration_XFS_Backfill_TagsUntaggedVolume(t *testing.T) {
	mount := setupXFSLoopback(t) // requires root
	dataPath := filepath.Join(mount, "volumes")
	require.NoError(t, os.MkdirAll(dataPath, 0700))
	ctx := context.Background()

	mgr, err := newVolumeManager(dataPath, "xfs", slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.Validate())

	const volName = "fred-int-backfill-app-0"
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

	// Backfill via EnsureQuota (what reconcileVolumeQuotas invokes).
	require.NoError(t, mgr.EnsureQuota(ctx, volName, capMiB))

	// Post-backfill: measurable...
	used, err := mgr.Usage(ctx, volName)
	require.NoError(t, err, "after backfill the volume must be measurable")
	assert.GreaterOrEqual(t, used, int64(10*1024*1024), "Usage under-reports: %d", used)

	// ...and enforced: a write past the cap is EDQUOT'd.
	werr := os.WriteFile(filepath.Join(dir, "big.bin"), make([]byte, 20*1024*1024), 0600)
	require.Error(t, werr, "a write past the backfilled cap must be quota-enforced")
	assert.True(t,
		errors.Is(werr, syscall.EDQUOT) || errors.Is(werr, syscall.ENOSPC) ||
			strings.Contains(werr.Error(), "disk quota exceeded") ||
			strings.Contains(werr.Error(), "no space left"),
		"expected a quota/space error, got: %v", werr)
}

// TestIntegration_XFS_ReconcileBackfill_EndToEnd exercises the FULL startup
// backfill wired to a real xfs manager: an active lease whose on-disk volume was
// left untagged by a pre-CAP_SYS_ADMIN daemon is healed by reconcileVolumeQuotas
// — becoming measurable and enforced — with no re-provision. This composes the
// enumeration (name derivation + existence gate + SKU sizing) with the real
// EnsureQuota, catching wiring bugs the mock-based unit test cannot.
func TestIntegration_XFS_ReconcileBackfill_EndToEnd(t *testing.T) {
	mount := setupXFSLoopback(t) // requires root
	dataPath := filepath.Join(mount, "volumes")
	require.NoError(t, os.MkdirAll(dataPath, 0700))
	ctx := context.Background()

	mgr, err := newVolumeManager(dataPath, "xfs", slog.Default())
	require.NoError(t, err)

	const lease = "l-e2e-backfill"
	volName := canonicalVolumeName(lease, "app", 0)
	dir := filepath.Join(dataPath, volName)
	require.NoError(t, os.MkdirAll(dir, 0700))
	require.NoError(t, writeProjectIDFile(dir, 555001))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.bin"), make([]byte, 10*1024*1024), 0600))

	b := backendForReconcileTest(t, mgr, dataPath, lease, "app", "e2e-stateful", 20)

	_, uerr := mgr.Usage(ctx, volName)
	require.Error(t, uerr, "untagged volume must not be measurable before backfill")

	b.reconcileVolumeQuotas(ctx)

	used, err := mgr.Usage(ctx, volName)
	require.NoError(t, err, "reconcile backfill must make the volume measurable")
	assert.GreaterOrEqual(t, used, int64(10*1024*1024), "Usage under-reports: %d", used)

	werr := os.WriteFile(filepath.Join(dir, "big.bin"), make([]byte, 20*1024*1024), 0600)
	require.Error(t, werr, "a write past the backfilled cap must be quota-enforced")
	assert.True(t,
		errors.Is(werr, syscall.EDQUOT) || errors.Is(werr, syscall.ENOSPC) ||
			strings.Contains(werr.Error(), "disk quota exceeded") ||
			strings.Contains(werr.Error(), "no space left"),
		"expected a quota/space error, got: %v", werr)
}

// TestIntegration_XFS_ReconcileBackfill_RetainedVolume covers the retained-volume
// arm of the backfill end-to-end: a soft-deleted (fred-retained-) volume left
// untagged by a pre-CAP_SYS_ADMIN daemon is healed by reconcileVolumeQuotas via
// its active retention record — the population that dominated the prod backlog.
func TestIntegration_XFS_ReconcileBackfill_RetainedVolume(t *testing.T) {
	mount := setupXFSLoopback(t) // requires root
	dataPath := filepath.Join(mount, "volumes")
	require.NoError(t, os.MkdirAll(dataPath, 0700))
	ctx := context.Background()

	mgr, err := newVolumeManager(dataPath, "xfs", slog.Default())
	require.NoError(t, err)

	const origLease = "l-ret"
	retName := retainedName(canonicalVolumeName(origLease, "db", 0)) // fred-retained-l-ret-db-0
	dir := filepath.Join(dataPath, retName)
	require.NoError(t, os.MkdirAll(dir, 0700))
	require.NoError(t, writeProjectIDFile(dir, 556001))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data.bin"), make([]byte, 10*1024*1024), 0600))

	b, rs := newBackendWithRetention(t)
	b.volumes = mgr
	b.cfg.VolumeDataPath = dataPath
	b.cfg.SKUProfiles["ret-sku"] = SKUProfile{CPUCores: 1, MemoryMB: 256, DiskMB: 20}
	require.NoError(t, rs.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   origLease,
		Tenant:              "t1",
		ProviderUUID:        "p1",
		Items:               []backend.LeaseItem{{SKU: "ret-sku", Quantity: 1, ServiceName: "db"}},
		RetainedVolumeNames: []string{retName},
		Status:              shared.RetentionStatusActive,
		CreatedAt:           time.Now(),
	}))

	_, uerr := mgr.Usage(ctx, retName)
	require.Error(t, uerr, "untagged retained volume must not be measurable before backfill")

	b.reconcileVolumeQuotas(ctx)

	used, err := mgr.Usage(ctx, retName)
	require.NoError(t, err, "reconcile must backfill the retained volume")
	assert.GreaterOrEqual(t, used, int64(10*1024*1024), "Usage under-reports: %d", used)

	werr := os.WriteFile(filepath.Join(dir, "big.bin"), make([]byte, 20*1024*1024), 0600)
	require.Error(t, werr, "retained volume cap must be enforced after backfill")
	assert.True(t,
		errors.Is(werr, syscall.EDQUOT) || errors.Is(werr, syscall.ENOSPC) ||
			strings.Contains(werr.Error(), "disk quota exceeded") ||
			strings.Contains(werr.Error(), "no space left"),
		"expected a quota/space error, got: %v", werr)
}

// TestIntegration_Btrfs_ReconcileBackfill_EndToEnd exercises the full startup
// backfill wired to a real btrfs manager. btrfs has no "untagged" state (the
// qgroup limit is applied at create), so this proves the reconcile RE-APPLIES
// the current SKU cap: a volume created with a large cap is tightened by the
// reconcile to the lease's smaller disk_mb, and a write past it is rejected.
func TestIntegration_Btrfs_ReconcileBackfill_EndToEnd(t *testing.T) {
	mount := setupBtrfsLoopback(t) // requires root
	dataPath := filepath.Join(mount, "volumes")
	require.NoError(t, os.MkdirAll(dataPath, 0700))
	ctx := context.Background()
	mgr := &btrfsVolumeManager{dataPath: dataPath, logger: slog.Default()}

	const lease = "l-btrfs-e2e"
	volName := canonicalVolumeName(lease, "app", 0)
	hostPath, _, err := mgr.Create(ctx, volName, 100) // large initial cap
	require.NoError(t, err)
	writeIncompressibleMiB(t, filepath.Join(hostPath, "seed.bin"), 5) // under both caps

	b := backendForReconcileTest(t, mgr, dataPath, lease, "app", "btrfs-small", 20)
	b.reconcileVolumeQuotas(ctx)

	// The reconcile tightened the qgroup limit to 20 MiB → 30 MiB more is rejected.
	writeIncompressibleExpectFail(t, filepath.Join(hostPath, "big.bin"), 30)
}

// TestIntegration_Zfs_ReconcileBackfill_EndToEnd is the zfs analogue: the
// reconcile re-applies refquota to the lease's smaller disk_mb via the real
// manager, and a write past it is rejected. (refquota cannot be lowered below
// current usage, so the seed write stays under the smaller cap.)
func TestIntegration_Zfs_ReconcileBackfill_EndToEnd(t *testing.T) {
	mount, _ := setupZFSPool(t) // requires root
	ctx := context.Background()
	mgr, err := newVolumeManager(mount, "zfs", slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.Validate()) // resolves parentDataset

	const lease = "l-zfs-e2e"
	volName := canonicalVolumeName(lease, "app", 0)
	hostPath, _, err := mgr.Create(ctx, volName, 100) // large initial refquota
	require.NoError(t, err)
	writeIncompressibleMiB(t, filepath.Join(hostPath, "seed.bin"), 5) // under both caps

	b := backendForReconcileTest(t, mgr, mount, lease, "app", "zfs-small", 20)
	b.reconcileVolumeQuotas(ctx)

	// The reconcile tightened refquota to 20 MiB → 30 MiB more is rejected.
	writeIncompressibleExpectFail(t, filepath.Join(hostPath, "big.bin"), 30)
}

// TestIntegration_Btrfs_SubdirDataPath_Measures guards that the btrfs backend
// still measures correctly when volume_data_path is a subdirectory of the mount
// (ENG-449 "validate other fs too"). btrfs tools operate on any path within the
// filesystem, so — unlike XFS — a subdir needs no mountpoint resolution; this
// pins that so the shared refactor did not regress it.
func TestIntegration_Btrfs_SubdirDataPath_Measures(t *testing.T) {
	mount := setupBtrfsLoopback(t)
	dataPath := filepath.Join(mount, "volumes") // subdir of the btrfs mount
	require.NoError(t, os.MkdirAll(dataPath, 0700))

	ctx := context.Background()
	mgr := &btrfsVolumeManager{dataPath: dataPath, logger: slog.Default()}

	const volName = "fred-int-btrfs-subdir-app-0"
	hostPath, created, err := mgr.Create(ctx, volName, 100)
	require.NoError(t, err)
	require.True(t, created)
	t.Cleanup(func() { _ = mgr.Destroy(ctx, volName) })

	const writeMiB = 10
	require.NoError(t, os.WriteFile(filepath.Join(hostPath, "data.bin"), make([]byte, writeMiB*1024*1024), 0600))
	got, err := mgr.Usage(ctx, volName)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, got, int64(writeMiB*1024*1024), "Usage under-reports: %d", got)
	assert.LessOrEqual(t, got, int64((writeMiB+6)*1024*1024), "Usage over-reports: %d", got)
}

// TestIntegration_Usage_ZFS verifies that zfsVolumeManager.Usage returns the
// dataset's referenced bytes within a metadata-overhead bound.
func TestIntegration_Usage_ZFS(t *testing.T) {
	mountPath, _ := setupZFSPool(t)
	ctx := context.Background()

	mgr, err := newVolumeManager(mountPath, "zfs", slog.Default())
	require.NoError(t, err)
	require.NoError(t, mgr.Validate())

	const volName = "fred-int-usage-zfs-app-0"
	const capMiB = int64(100)

	hostPath, created, err := mgr.Create(ctx, volName, capMiB)
	require.NoError(t, err)
	require.True(t, created)
	t.Cleanup(func() { _ = mgr.Destroy(ctx, volName) })

	const writeMiB = 10
	writeIncompressibleMiB(t, filepath.Join(hostPath, "data.bin"), writeMiB)

	got, err := mgr.Usage(ctx, volName)
	require.NoError(t, err)

	const wantMin = int64(writeMiB * 1024 * 1024)
	const wantMax = int64((writeMiB + 6) * 1024 * 1024)
	assert.GreaterOrEqual(t, got, wantMin, "Usage too low: got %d, want >= %d", got, wantMin)
	assert.LessOrEqual(t, got, wantMax, "Usage too high: got %d, want <= %d", got, wantMax)
}

// TestIntegration_DemotePromote_Btrfs is the end-to-end demote/promote quota
// test on a real btrfs loopback. It verifies all three plan assertions:
//
//	(a) data fits medium → checkDemoteFit passes, Create at medium lowers the
//	    enforced quota, and a write beyond medium fails with EDQUOT/ENOSPC.
//	(b) data exceeds medium → checkDemoteFit returns ErrDemoteDataExceedsTier.
//	(c) promote Create at large raises the cap so writes beyond medium succeed.
//
// Uses custom 100 MiB / 20 MiB "large"/"medium" tiers to stay within the
// 256 MiB loopback image.
func TestIntegration_DemotePromote_Btrfs(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	ctx := context.Background()

	const largeMiB, mediumMiB = int64(100), int64(20)

	mgr := &btrfsVolumeManager{dataPath: mountPath, logger: slog.Default()}

	// Wire a Backend with real btrfs volumes and the custom small-SKU profiles.
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.volumes = mgr
	b.cfg.SKUProfiles["test-large"] = SKUProfile{CPUCores: 0.5, MemoryMB: 512, DiskMB: largeMiB}
	b.cfg.SKUProfiles["test-medium"] = SKUProfile{CPUCores: 0.5, MemoryMB: 512, DiskMB: mediumMiB}
	mediumProfiles := map[string]SKUProfile{"test-medium": b.cfg.SKUProfiles["test-medium"]}

	// (a) 5 MiB data fits 20 MiB medium: gate passes, quota lowered, write beyond fails.
	t.Run("fits_medium", func(t *testing.T) {
		const origLease = "int-btrfs-fits"
		canon := canonicalVolumeName(origLease, "app", 0)
		retained := retainedName(canon)

		hostPath, _, err := mgr.Create(ctx, canon, largeMiB)
		require.NoError(t, err)

		// 5 MiB — fits 20 MiB medium
		require.NoError(t, os.WriteFile(filepath.Join(hostPath, "data.bin"), make([]byte, 5*1024*1024), 0600))

		require.NoError(t, mgr.RenameVolume(canon, retained))
		t.Cleanup(func() { _ = mgr.Destroy(ctx, retained) })

		rec := &shared.RetentionEntry{
			OriginalLeaseUUID:   origLease,
			Items:               []backend.LeaseItem{{SKU: "test-large", ServiceName: "app", Quantity: 1}},
			RetainedVolumeNames: []string{retained},
		}
		err = b.checkDemoteFit(ctx, rec,
			[]backend.LeaseItem{{SKU: "test-medium", ServiceName: "app", Quantity: 1}},
			mediumProfiles, b.logger)
		require.NoError(t, err, "5 MiB data fits 20 MiB medium: checkDemoteFit must pass")

		// Simulate adoptRetainedVolumes: rename to a new lease's canonical name.
		const newLease = "int-btrfs-fits-new"
		newCanon := canonicalVolumeName(newLease, "app", 0)
		require.NoError(t, mgr.RenameVolume(retained, newCanon))
		t.Cleanup(func() { _ = mgr.Destroy(ctx, newCanon) })

		// Create at medium cap lowers the btrfs qgroup limit to 20 MiB.
		newHostPath, _, err := mgr.Create(ctx, newCanon, mediumMiB)
		require.NoError(t, err, "Create at medium cap must succeed")

		// Writing 25 MiB beyond the 20 MiB quota must fail (total ~30 MiB > 20 MiB).
		out, werr := exec.Command("dd", "if=/dev/zero",
			"of="+filepath.Join(newHostPath, "big.bin"), "bs=1M", "count=25").CombinedOutput()
		require.Error(t, werr,
			"writing 25 MiB to a 20 MiB-quota btrfs volume must fail (EDQUOT/ENOSPC); dd output: %s", out)
	})

	// (b) 25 MiB data exceeds 20 MiB medium: gate returns ErrDemoteDataExceedsTier.
	t.Run("exceeds_medium", func(t *testing.T) {
		const origLease = "int-btrfs-exceeds"
		canon := canonicalVolumeName(origLease, "app", 0)
		retained := retainedName(canon)

		hostPath, _, err := mgr.Create(ctx, canon, largeMiB)
		require.NoError(t, err)
		t.Cleanup(func() { _ = mgr.Destroy(ctx, canon) })

		// 25 MiB — exceeds 20 MiB medium cap; write must succeed under 100 MiB large cap.
		out, werr := exec.Command("dd", "if=/dev/zero",
			"of="+filepath.Join(hostPath, "data.bin"), "bs=1M", "count=25").CombinedOutput()
		require.NoError(t, werr, "writing 25 MiB to 100 MiB-quota volume must succeed; dd output: %s", out)

		require.NoError(t, mgr.RenameVolume(canon, retained))
		t.Cleanup(func() { _ = mgr.Destroy(ctx, retained) })

		rec := &shared.RetentionEntry{
			OriginalLeaseUUID:   origLease,
			Items:               []backend.LeaseItem{{SKU: "test-large", ServiceName: "app", Quantity: 1}},
			RetainedVolumeNames: []string{retained},
		}
		err = b.checkDemoteFit(ctx, rec,
			[]backend.LeaseItem{{SKU: "test-medium", ServiceName: "app", Quantity: 1}},
			mediumProfiles, b.logger)
		require.ErrorIs(t, err, backend.ErrDemoteDataExceedsTier,
			"25 MiB data exceeds 20 MiB medium: gate must refuse with ErrDemoteDataExceedsTier")
	})

	// (c) promote: Create at large from a medium-capped volume raises the cap so
	//     writes beyond medium now succeed.
	t.Run("promote_raises_cap", func(t *testing.T) {
		const origLease = "int-btrfs-promote"
		canon := canonicalVolumeName(origLease, "app", 0)

		// Start at medium cap (20 MiB).
		hostPath, _, err := mgr.Create(ctx, canon, mediumMiB)
		require.NoError(t, err)
		t.Cleanup(func() { _ = mgr.Destroy(ctx, canon) })

		// Write 5 MiB so the subvolume has some data.
		require.NoError(t, os.WriteFile(filepath.Join(hostPath, "data.bin"), make([]byte, 5*1024*1024), 0600))

		// Confirm 25 MiB does NOT fit under medium cap (partial write fails).
		out, werr := exec.Command("dd", "if=/dev/zero",
			"of="+filepath.Join(hostPath, "big.bin"), "bs=1M", "count=25").CombinedOutput()
		require.Error(t, werr,
			"25 MiB must not fit the 20 MiB cap before promote; dd output: %s", out)
		// Remove any partial file so rfer drops back toward the initial 5 MiB.
		_ = os.Remove(filepath.Join(hostPath, "big.bin"))

		// Promote: Create (idempotent) at large cap raises qgroup limit to 100 MiB.
		_, _, err = mgr.Create(ctx, canon, largeMiB)
		require.NoError(t, err, "Create at large cap must succeed (quota update)")

		// Writing 25 MiB must now succeed (cap is 100 MiB; total at most ~30 MiB).
		out, werr = exec.Command("dd", "if=/dev/zero",
			"of="+filepath.Join(hostPath, "big2.bin"), "bs=1M", "count=25").CombinedOutput()
		require.NoError(t, werr,
			"25 MiB must fit the promoted 100 MiB cap; dd output: %s", out)
	})
}

// TestIntegration_DemotePromote_XFS exercises the demote/promote quota flow on
// a real XFS loopback with project quotas (pquota). Covers the same gate
// assertions as the btrfs variant: (a) gate passes + quota lowered, (b) gate
// refuses when data exceeds the smaller tier.
func TestIntegration_DemotePromote_XFS(t *testing.T) {
	mountDir := setupXFSLoopback(t)
	ctx := context.Background()

	const largeMiB, mediumMiB = int64(100), int64(20)

	mgr, err := newVolumeManager(mountDir, "xfs", slog.Default())
	require.NoError(t, err)

	b := newBackendForTest(&mockDockerClient{}, nil)
	b.volumes = mgr
	b.cfg.SKUProfiles["test-large"] = SKUProfile{CPUCores: 0.5, MemoryMB: 512, DiskMB: largeMiB}
	b.cfg.SKUProfiles["test-medium"] = SKUProfile{CPUCores: 0.5, MemoryMB: 512, DiskMB: mediumMiB}
	mediumProfiles := map[string]SKUProfile{"test-medium": b.cfg.SKUProfiles["test-medium"]}

	// (a) fits: gate passes, XFS bhard lowered, write beyond medium fails.
	t.Run("fits_medium", func(t *testing.T) {
		const origLease = "int-xfs-fits"
		canon := canonicalVolumeName(origLease, "app", 0)
		retained := retainedName(canon)

		hostPath, _, err := mgr.Create(ctx, canon, largeMiB)
		require.NoError(t, err)

		require.NoError(t, os.WriteFile(filepath.Join(hostPath, "data.bin"), make([]byte, 5*1024*1024), 0600))

		require.NoError(t, mgr.RenameVolume(canon, retained))
		t.Cleanup(func() { _ = mgr.Destroy(ctx, retained) })

		rec := &shared.RetentionEntry{
			OriginalLeaseUUID:   origLease,
			Items:               []backend.LeaseItem{{SKU: "test-large", ServiceName: "app", Quantity: 1}},
			RetainedVolumeNames: []string{retained},
		}
		err = b.checkDemoteFit(ctx, rec,
			[]backend.LeaseItem{{SKU: "test-medium", ServiceName: "app", Quantity: 1}},
			mediumProfiles, b.logger)
		require.NoError(t, err, "5 MiB data fits 20 MiB medium: checkDemoteFit must pass")

		const newLease = "int-xfs-fits-new"
		newCanon := canonicalVolumeName(newLease, "app", 0)
		require.NoError(t, mgr.RenameVolume(retained, newCanon))
		t.Cleanup(func() { _ = mgr.Destroy(ctx, newCanon) })

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
		const origLease = "int-xfs-exceeds"
		canon := canonicalVolumeName(origLease, "app", 0)
		retained := retainedName(canon)

		hostPath, _, err := mgr.Create(ctx, canon, largeMiB)
		require.NoError(t, err)
		t.Cleanup(func() { _ = mgr.Destroy(ctx, canon) })

		out, werr := exec.Command("dd", "if=/dev/zero",
			"of="+filepath.Join(hostPath, "data.bin"), "bs=1M", "count=25").CombinedOutput()
		require.NoError(t, werr, "25 MiB must fit 100 MiB XFS quota; dd output: %s", out)

		require.NoError(t, mgr.RenameVolume(canon, retained))
		t.Cleanup(func() { _ = mgr.Destroy(ctx, retained) })

		rec := &shared.RetentionEntry{
			OriginalLeaseUUID:   origLease,
			Items:               []backend.LeaseItem{{SKU: "test-large", ServiceName: "app", Quantity: 1}},
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
		const origLease = "int-xfs-promote"
		canon := canonicalVolumeName(origLease, "app", 0)

		// Start at medium cap (20 MiB).
		hostPath, _, err := mgr.Create(ctx, canon, mediumMiB)
		require.NoError(t, err)
		t.Cleanup(func() { _ = mgr.Destroy(ctx, canon) })

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

// TestIntegration_DemotePromote_ZFS exercises the demote/promote quota flow on
// a real ZFS pool. The key ZFS-specific assertion is that the gate's precondition
// (usage ≤ new cap) prevents `zfs set refquota` from being called with a value
// below the dataset's `referenced`, which ZFS would reject with "cannot set
// refquota below referenced value".
func TestIntegration_DemotePromote_ZFS(t *testing.T) {
	mountPath, _ := setupZFSPool(t)
	ctx := context.Background()

	const largeMiB, mediumMiB = int64(100), int64(20)

	mgr, err := newVolumeManager(mountPath, "zfs", slog.Default())
	require.NoError(t, err)
	// Validate resolves and caches the parent dataset name required by all ZFS ops.
	require.NoError(t, mgr.Validate())

	b := newBackendForTest(&mockDockerClient{}, nil)
	b.volumes = mgr
	b.cfg.SKUProfiles["test-large"] = SKUProfile{CPUCores: 0.5, MemoryMB: 512, DiskMB: largeMiB}
	b.cfg.SKUProfiles["test-medium"] = SKUProfile{CPUCores: 0.5, MemoryMB: 512, DiskMB: mediumMiB}
	mediumProfiles := map[string]SKUProfile{"test-medium": b.cfg.SKUProfiles["test-medium"]}

	// (a) 5 MiB data fits 20 MiB medium: gate passes, `zfs set refquota` at
	//     medium succeeds because referenced (~5 MiB) < refquota (20 MiB).
	//     Write beyond medium then fails — confirming refquota is the enforcer.
	t.Run("fits_medium_refquota_succeeds", func(t *testing.T) {
		const origLease = "int-zfs-fits"
		canon := canonicalVolumeName(origLease, "app", 0)
		retained := retainedName(canon)

		hostPath, _, err := mgr.Create(ctx, canon, largeMiB)
		require.NoError(t, err)

		writeIncompressibleMiB(t, filepath.Join(hostPath, "data.bin"), 5)

		require.NoError(t, mgr.RenameVolume(canon, retained))
		t.Cleanup(func() { _ = mgr.Destroy(ctx, retained) })

		rec := &shared.RetentionEntry{
			OriginalLeaseUUID:   origLease,
			Items:               []backend.LeaseItem{{SKU: "test-large", ServiceName: "app", Quantity: 1}},
			RetainedVolumeNames: []string{retained},
		}
		err = b.checkDemoteFit(ctx, rec,
			[]backend.LeaseItem{{SKU: "test-medium", ServiceName: "app", Quantity: 1}},
			mediumProfiles, b.logger)
		require.NoError(t, err, "5 MiB fits 20 MiB medium: gate must pass")

		const newLease = "int-zfs-fits-new"
		newCanon := canonicalVolumeName(newLease, "app", 0)
		require.NoError(t, mgr.RenameVolume(retained, newCanon))
		t.Cleanup(func() { _ = mgr.Destroy(ctx, newCanon) })

		// Create at medium cap issues `zfs set refquota=20M <dataset>`.
		// Since referenced (~5 MiB) < refquota (20 MiB) the set must NOT fail with
		// "cannot set refquota below referenced value" — that ZFS error only arises
		// when the gate is bypassed and data > new cap reaches this call.
		_, _, err = mgr.Create(ctx, newCanon, mediumMiB)
		require.NoError(t, err,
			"zfs set refquota at 20 MiB must succeed when referenced is only ~5 MiB")

		// refquota is now 20 MiB; a write beyond it must fail. Use /dev/urandom:
		// zero-filled data would be stored as holes (ZFS compression=on default)
		// and never reach the refquota, so dd would spuriously succeed.
		newHostPath := mgr.HostPath(newCanon)
		out, werr := exec.Command("dd", "if=/dev/urandom",
			"of="+filepath.Join(newHostPath, "big.bin"), "bs=1M", "count=25").CombinedOutput()
		require.Error(t, werr,
			"writing 25 MiB beyond the 20 MiB ZFS refquota must fail; dd output: %s", out)
	})

	// (b) promote: Create at large cap on an existing medium-capped dataset must
	//     clear any stale quota= so refquota is the sole enforcer. Simulates the
	//     pre-ENG-438 state by explicitly setting quota= before the promote call,
	//     then asserts quota is none (0 in parsable output) afterwards. A stale
	//     smaller quota= would otherwise silently bind on promote and cap the lease
	//     at the old size even after `zfs set refquota=<larger>` succeeds.
	t.Run("promote_clears_legacy_quota", func(t *testing.T) {
		const lease = "int-zfs-promote"
		canon := canonicalVolumeName(lease, "app", 0)

		// Create at medium cap — hits the fresh-create path (no legacy quota).
		_, _, err := mgr.Create(ctx, canon, mediumMiB)
		require.NoError(t, err)
		t.Cleanup(func() { _ = mgr.Destroy(ctx, canon) })

		// Resolve the ZFS dataset name so we can inspect and set properties directly.
		dsOut, dsErr := exec.Command("zfs", "list", "-H", "-o", "name", filepath.Join(mountPath, canon)).CombinedOutput()
		require.NoError(t, dsErr, "resolve dataset name: %s", dsOut)
		dataset := strings.TrimSpace(string(dsOut))

		// Simulate a pre-ENG-438 dataset that had quota= set by the old code.
		// ZFS enforces the tighter of quota and refquota simultaneously; without
		// the fix, a promote would leave this stale smaller quota= in place.
		out, setErr := exec.Command("zfs", "set", "quota="+fmt.Sprintf("%dM", mediumMiB), dataset).CombinedOutput()
		require.NoError(t, setErr, "zfs set legacy quota: %s", out)

		// Promote: Create at large cap hits the idempotent-reuse path and must
		// issue `zfs set refquota=100M quota=none <dataset>` so the stale quota is cleared.
		_, _, err = mgr.Create(ctx, canon, largeMiB)
		require.NoError(t, err, "promote Create at large cap must succeed")

		// Assert that quota= is now none. With -Hp, quota=none is reported as "0".
		// A non-zero value here means the stale quota= was not cleared and would
		// silently cap writes at mediumMiB despite the refquota promotion.
		quotaOut, quotaErr := exec.Command("zfs", "get", "-Hp", "-o", "value", "quota", dataset).CombinedOutput()
		require.NoError(t, quotaErr, "zfs get quota: %s", quotaOut)
		require.Equal(t, "0", strings.TrimSpace(string(quotaOut)),
			"legacy quota= must be none (0) after promote so refquota is the sole cap")
	})

	// (c) 25 MiB data exceeds 20 MiB medium: gate refuses, preventing the
	//     `zfs set refquota < referenced` error that ZFS would return if we
	//     proceeded directly to Create at medium cap.
	t.Run("exceeds_medium_gate_refuses", func(t *testing.T) {
		const origLease = "int-zfs-exceeds"
		canon := canonicalVolumeName(origLease, "app", 0)
		retained := retainedName(canon)

		hostPath, _, err := mgr.Create(ctx, canon, largeMiB)
		require.NoError(t, err)
		t.Cleanup(func() { _ = mgr.Destroy(ctx, canon) })

		// 25 MiB of incompressible data so `referenced` actually reflects it
		// (ZFS compression=on default would elide zero-filled data as holes).
		writeIncompressibleMiB(t, filepath.Join(hostPath, "data.bin"), 25)

		require.NoError(t, mgr.RenameVolume(canon, retained))
		t.Cleanup(func() { _ = mgr.Destroy(ctx, retained) })

		rec := &shared.RetentionEntry{
			OriginalLeaseUUID:   origLease,
			Items:               []backend.LeaseItem{{SKU: "test-large", ServiceName: "app", Quantity: 1}},
			RetainedVolumeNames: []string{retained},
		}
		err = b.checkDemoteFit(ctx, rec,
			[]backend.LeaseItem{{SKU: "test-medium", ServiceName: "app", Quantity: 1}},
			mediumProfiles, b.logger)
		// Gate refuses — prevents `zfs set refquota=20M` on a ~25 MiB dataset,
		// which would fail: "cannot set refquota below referenced value."
		require.ErrorIs(t, err, backend.ErrDemoteDataExceedsTier,
			"gate must refuse 25 MiB data for 20 MiB medium, preventing ZFS refquota-below-referenced error")
	})
}
