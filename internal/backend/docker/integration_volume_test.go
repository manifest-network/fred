//go:build integration

package docker

import (
	"context"
	"encoding/json"
	"fmt"
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

	// Mount with loop
	out, err = exec.Command("mount", "-o", "loop", imgFile, mountDir).CombinedOutput()
	require.NoError(t, err, "mount: %s", out)

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
	docker, err := NewDockerClient("")
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
	docker, err := NewDockerClient("")
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
	docker, err := NewDockerClient("")
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
	manifest := DockerManifest{
		Image:   "redis:7",
		Command: []string{"redis-server", "--save", "1", "1"},
	}
	payload, err := json.Marshal(manifest)
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
	volumeID := fmt.Sprintf("fred-%s-0", leaseUUID)
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
	assert.True(t, os.IsNotExist(statErr), "volume directory should be gone after deprovision")
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

	manifest := DockerManifest{
		Image:   "redis:7",
		Command: []string{"redis-server", "--save", "1", "1"},
	}
	payload, err := json.Marshal(manifest)
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
	volumeID := fmt.Sprintf("fred-%s-0", leaseUUID)
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
	manifest := DockerManifest{
		Image:   "redis:7",
		Command: []string{"redis-server"},
	}
	payload, err := json.Marshal(manifest)
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
	manifest := DockerManifest{
		Image:   "redis:7",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
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
	subvol0 := filepath.Join(mountPath, fmt.Sprintf("fred-%s-0", leaseUUID))
	subvol1 := filepath.Join(mountPath, fmt.Sprintf("fred-%s-1", leaseUUID))

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
	assert.True(t, os.IsNotExist(err), "subvolume 0 should be destroyed after deprovision, got err: %v", err)
	_, err = os.Stat(subvol1)
	assert.True(t, os.IsNotExist(err), "subvolume 1 should be destroyed after deprovision, got err: %v", err)
}

// TestIntegration_Docker_OrphanedVolumeCleanup verifies that orphaned volumes
// (created but no matching provision) are destroyed when a new backend starts.
// This simulates a crash between container removal and volume destruction.
func TestIntegration_Docker_OrphanedVolumeCleanup(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	callbackServer, callbackCh := startCallbackServer(t)

	// Build config manually to control backend lifecycle (like ColdStartRecovery tests)
	cfg := DefaultConfig()
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

	dockerCli, err := NewDockerClient("")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupTestContainers(t, dockerCli, cfg.Name)
		cleanupTestNetworks(t, dockerCli)
		_ = dockerCli.Close()
	})

	leaseUUID := fmt.Sprintf("orphan-vol-%d", time.Now().UnixNano())

	// redis:7 declares VOLUME /data → triggers btrfs subvolume creation
	manifest := DockerManifest{
		Image:   "redis:7",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
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
	volumeID := fmt.Sprintf("fred-%s-0", leaseUUID)
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
	assert.True(t, os.IsNotExist(statErr),
		"orphaned volume should be destroyed after backend restart, got err: %v", statErr)
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
	manifest := DockerManifest{
		Image:   "redis:7",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
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
