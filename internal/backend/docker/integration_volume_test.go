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
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
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

	out, err := exec.Command("truncate", "-s", "256M", imgFile).CombinedOutput()
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
	require.NoError(t, os.WriteFile(filepath.Join(hostPath, "data.bin"), make([]byte, writeMiB*1024*1024), 0600))

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

		// refquota is now 20 MiB; a write beyond it must fail.
		newHostPath := mgr.HostPath(newCanon)
		out, werr := exec.Command("dd", "if=/dev/zero",
			"of="+filepath.Join(newHostPath, "big.bin"), "bs=1M", "count=25").CombinedOutput()
		require.Error(t, werr,
			"writing 25 MiB beyond the 20 MiB ZFS refquota must fail; dd output: %s", out)
	})

	// (b) 25 MiB data exceeds 20 MiB medium: gate refuses, preventing the
	//     `zfs set refquota < referenced` error that ZFS would return if we
	//     proceeded directly to Create at medium cap.
	t.Run("exceeds_medium_gate_refuses", func(t *testing.T) {
		const origLease = "int-zfs-exceeds"
		canon := canonicalVolumeName(origLease, "app", 0)
		retained := retainedName(canon)

		hostPath, _, err := mgr.Create(ctx, canon, largeMiB)
		require.NoError(t, err)
		t.Cleanup(func() { _ = mgr.Destroy(ctx, canon) })

		out, werr := exec.Command("dd", "if=/dev/zero",
			"of="+filepath.Join(hostPath, "data.bin"), "bs=1M", "count=25").CombinedOutput()
		require.NoError(t, werr, "25 MiB must fit 100 MiB ZFS refquota; dd output: %s", out)

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
