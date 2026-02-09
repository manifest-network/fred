//go:build integration

package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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

	// 6. Wait for success callback
	select {
	case cb := <-callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(3 * time.Minute):
		t.Fatal("timeout waiting for re-provision callback")
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
