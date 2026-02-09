//go:build integration

package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	networktypes "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
)

const testCallbackSecret = "integration-test-secret-at-least-32-chars!"

// testBackendWithRealDocker creates a Backend connected to the real Docker daemon.
// The backend is stopped and all test containers/networks are cleaned up via t.Cleanup.
func testBackendWithRealDocker(t *testing.T, cfgFn func(*Config)) *Backend {
	t.Helper()

	docker, err := NewDockerClient("")
	require.NoError(t, err)

	ctx := context.Background()
	if err := docker.Ping(ctx); err != nil {
		t.Skip("Docker not available:", err)
	}

	cfg := DefaultConfig()
	cfg.Name = fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())
	cfg.CallbackSecret = testCallbackSecret
	cfg.HostAddress = "127.0.0.1"
	cfg.StartupVerifyDuration = 1 * time.Second
	cfg.ReconcileInterval = 1 * time.Hour // disable during tests
	cfg.ProvisionTimeout = 2 * time.Minute
	// Isolate DB stores per test to avoid replaying stale callbacks from previous runs.
	tmpDir := t.TempDir()
	cfg.CallbackDBPath = filepath.Join(tmpDir, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(tmpDir, "diagnostics.db")

	if cfgFn != nil {
		cfgFn(&cfg)
	}

	// If the test didn't set VolumeDataPath, zero out DiskMB on all profiles
	// so config validation doesn't require a volume filesystem.
	if cfg.VolumeDataPath == "" {
		for name, p := range cfg.SKUProfiles {
			p.DiskMB = 0
			cfg.SKUProfiles[name] = p
		}
	}

	logger := slog.Default()
	b, err := New(cfg, logger)
	require.NoError(t, err)

	err = b.Start(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = b.Stop()
		cleanupTestContainers(t, docker, cfg.Name)
		cleanupTestNetworks(t, docker)
	})

	return b
}

// cleanupTestContainers removes all containers managed by the test backend.
func cleanupTestContainers(t *testing.T, docker *DockerClient, backendName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	containers, err := docker.client.ContainerList(ctx, container.ListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("label", LabelManaged+"=true"),
		),
	})
	if err != nil {
		t.Logf("cleanup: failed to list containers: %v", err)
		return
	}

	for _, c := range containers {
		if err := docker.client.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true}); err != nil {
			if !client.IsErrNotFound(err) {
				t.Logf("cleanup: failed to remove container %s: %v", c.ID[:12], err)
			}
		}
	}
}

// cleanupTestNetworks removes all networks managed by Fred.
func cleanupTestNetworks(t *testing.T, docker *DockerClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	networks, err := docker.client.NetworkList(ctx, networktypes.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("label", LabelManaged+"=true"),
		),
	})
	if err != nil {
		t.Logf("cleanup: failed to list networks: %v", err)
		return
	}

	for _, n := range networks {
		if err := docker.client.NetworkRemove(ctx, n.ID); err != nil {
			t.Logf("cleanup: failed to remove network %s: %v", n.Name, err)
		}
	}
}

// startCallbackServer creates an httptest server that receives HMAC-signed callbacks
// and sends them to the returned channel.
func startCallbackServer(t *testing.T) (*httptest.Server, <-chan backend.CallbackPayload) {
	t.Helper()
	ch := make(chan backend.CallbackPayload, 10)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Logf("callback: failed to read body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Verify HMAC signature
		sig := r.Header.Get(hmacauth.SignatureHeader)
		if sig == "" {
			t.Logf("callback: missing signature header")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if err := hmacauth.Verify(testCallbackSecret, body, sig, 5*time.Minute); err != nil {
			t.Logf("callback: invalid signature: %v", err)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		var payload backend.CallbackPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Logf("callback: failed to unmarshal: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		ch <- payload
		w.WriteHeader(http.StatusOK)
	}))

	t.Cleanup(server.Close)
	return server, ch
}

func TestIntegration_Docker_ProvisionLifecycle(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
	})

	ctx := context.Background()
	leaseUUID := fmt.Sprintf("lifecycle-%d", time.Now().UnixNano())

	manifest := DockerManifest{
		Image: "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	// Provision
	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	// Wait for callback
	select {
	case cb := <-callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for provision callback")
	}

	// GetInfo should return connection details
	info, err := b.GetInfo(ctx, leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "127.0.0.1", (*info)["host"])
	instances, ok := (*info)["instances"].([]map[string]any)
	require.True(t, ok, "expected instances array")
	require.Len(t, instances, 1)
	assert.Equal(t, "running", instances[0]["status"])

	// ListProvisions should include our lease
	provisions, err := b.ListProvisions(ctx)
	require.NoError(t, err)
	found := false
	for _, p := range provisions {
		if p.LeaseUUID == leaseUUID {
			found = true
			assert.Equal(t, backend.ProvisionStatusReady, p.Status)
		}
	}
	assert.True(t, found, "lease should appear in ListProvisions")

	// Deprovision
	err = b.Deprovision(ctx, leaseUUID)
	require.NoError(t, err)

	// GetInfo should return ErrNotProvisioned after deprovision
	_, err = b.GetInfo(ctx, leaseUUID)
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestIntegration_Docker_NetworkIsolation(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(true)
	})

	ctx := context.Background()
	tenant1 := fmt.Sprintf("tenant-a-%d", time.Now().UnixNano())
	tenant2 := fmt.Sprintf("tenant-b-%d", time.Now().UnixNano())
	leaseUUID1 := fmt.Sprintf("net-iso-1-%d", time.Now().UnixNano())
	leaseUUID2 := fmt.Sprintf("net-iso-2-%d", time.Now().UnixNano())

	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	// Provision for tenant 1
	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID1,
		Tenant:       tenant1,
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	// Provision for tenant 2
	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID2,
		Tenant:       tenant2,
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	// Wait for both callbacks
	received := 0
	for received < 2 {
		select {
		case cb := <-callbackCh:
			assert.Equal(t, backend.CallbackStatusSuccess, cb.Status)
			received++
		case <-time.After(2 * time.Minute):
			t.Fatalf("timeout waiting for callbacks, got %d/2", received)
		}
	}

	// Verify separate tenant networks were created
	docker, err := NewDockerClient("")
	require.NoError(t, err)
	defer func() { _ = docker.Close() }()

	net1Name := TenantNetworkName(tenant1)
	net2Name := TenantNetworkName(tenant2)
	assert.NotEqual(t, net1Name, net2Name, "tenants should have different network names")

	networks, err := docker.ListManagedNetworks(ctx)
	require.NoError(t, err)

	foundNet1, foundNet2 := false, false
	for _, n := range networks {
		if n.Name == net1Name {
			foundNet1 = true
		}
		if n.Name == net2Name {
			foundNet2 = true
		}
	}
	assert.True(t, foundNet1, "tenant 1 network should exist")
	assert.True(t, foundNet2, "tenant 2 network should exist")

	// Deprovision both
	err = b.Deprovision(ctx, leaseUUID1)
	require.NoError(t, err)
	err = b.Deprovision(ctx, leaseUUID2)
	require.NoError(t, err)

	// Networks should be cleaned up (eventually, after deprovision removes containers)
	// Give Docker a moment to process network disconnections
	time.Sleep(500 * time.Millisecond)

	networks, err = docker.ListManagedNetworks(ctx)
	require.NoError(t, err)

	for _, n := range networks {
		assert.NotEqual(t, net1Name, n.Name, "tenant 1 network should be removed")
		assert.NotEqual(t, net2Name, n.Name, "tenant 2 network should be removed")
	}
}

func TestIntegration_Docker_ContainerHardening(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)

	pidsLimit := int64(128)
	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.ContainerReadonlyRootfs = ptrBool(true)
		cfg.ContainerPidsLimit = &pidsLimit
	})

	ctx := context.Background()
	leaseUUID := fmt.Sprintf("hardening-%d", time.Now().UnixNano())

	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	// Wait for callback
	select {
	case cb := <-callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for provision callback")
	}

	// Inspect container via Docker API
	docker, err := NewDockerClient("")
	require.NoError(t, err)
	defer func() { _ = docker.Close() }()

	containers, err := docker.client.ContainerList(ctx, container.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("label", LabelManaged+"=true"),
			filters.Arg("label", LabelLeaseUUID+"="+leaseUUID),
		),
	})
	require.NoError(t, err)
	require.Len(t, containers, 1, "expected exactly one container for lease")

	inspect, err := docker.client.ContainerInspect(ctx, containers[0].ID)
	require.NoError(t, err)

	// Verify hardening settings
	assert.True(t, inspect.HostConfig.ReadonlyRootfs, "root filesystem should be read-only")

	assert.Contains(t, inspect.HostConfig.CapDrop, "ALL", "all capabilities should be dropped")

	assert.Contains(t, inspect.HostConfig.SecurityOpt, "no-new-privileges:true",
		"no-new-privileges should be set")

	require.NotNil(t, inspect.HostConfig.PidsLimit, "PidsLimit should be set")
	assert.Equal(t, pidsLimit, *inspect.HostConfig.PidsLimit)

	// Verify tmpfs mounts for /tmp and /run
	assert.Contains(t, inspect.HostConfig.Tmpfs, "/tmp", "tmpfs mount for /tmp expected")
	assert.Contains(t, inspect.HostConfig.Tmpfs, "/run", "tmpfs mount for /run expected")

	// Cleanup
	err = b.Deprovision(ctx, leaseUUID)
	require.NoError(t, err)
}

func TestIntegration_Docker_DeprovisionIdempotent(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
	})

	ctx := context.Background()
	leaseUUID := fmt.Sprintf("idempotent-%d", time.Now().UnixNano())

	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	select {
	case cb := <-callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for provision callback")
	}

	// First deprovision
	err = b.Deprovision(ctx, leaseUUID)
	require.NoError(t, err)

	// Second deprovision should also succeed (idempotent)
	err = b.Deprovision(ctx, leaseUUID)
	assert.NoError(t, err, "second deprovision should be idempotent")

	// Third for good measure
	err = b.Deprovision(ctx, leaseUUID)
	assert.NoError(t, err, "third deprovision should be idempotent")
}

// --- Helpers for edge-case tests ---

// waitForContainerExited polls until the container reaches "exited" state.
// ContainerKill is asynchronous — the SIGKILL is sent but the container needs
// time to transition to exited. This helper ensures the transition is complete.
func waitForContainerExited(t *testing.T, containerID string) {
	t.Helper()
	docker, err := NewDockerClient("")
	require.NoError(t, err)
	defer func() { _ = docker.Close() }()

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		info, err := docker.InspectContainer(ctx, containerID)
		if err != nil {
			return false
		}
		return info.Status == "exited"
	}, 10*time.Second, 100*time.Millisecond, "container %s did not reach exited state", containerID)
}

// killContainer sends SIGKILL to a container, leaving it in "exited" state.
// The container remains visible to Docker (and recoverState) unlike ContainerRemove.
func killContainer(t *testing.T, containerID string) {
	t.Helper()
	docker, err := NewDockerClient("")
	require.NoError(t, err)
	defer func() { _ = docker.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = docker.client.ContainerKill(ctx, containerID, "KILL")
	require.NoError(t, err, "failed to kill container %s", containerID)
}

// waitForProvisionStatus polls ListProvisions until the given lease reaches the expected status.
func waitForProvisionStatus(t *testing.T, b *Backend, leaseUUID string, expected backend.ProvisionStatus, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		provisions, err := b.ListProvisions(context.Background())
		if err != nil {
			return false
		}
		for _, p := range provisions {
			if p.LeaseUUID == leaseUUID && p.Status == expected {
				return true
			}
		}
		return false
	}, timeout, 100*time.Millisecond, "provision %s did not reach status %s", leaseUUID, expected)
}

// inspectProvisionContainers lists containers for a lease by label.
func inspectProvisionContainers(t *testing.T, leaseUUID string) []container.Summary {
	t.Helper()
	docker, err := NewDockerClient("")
	require.NoError(t, err)
	defer func() { _ = docker.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	containers, err := docker.client.ContainerList(ctx, container.ListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("label", LabelManaged+"=true"),
			filters.Arg("label", LabelLeaseUUID+"="+leaseUUID),
		),
	})
	require.NoError(t, err)
	return containers
}

// getProvisionInfo returns the ProvisionInfo for a specific lease, or fails.
func getProvisionInfo(t *testing.T, b *Backend, leaseUUID string) backend.ProvisionInfo {
	t.Helper()
	provisions, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	for _, p := range provisions {
		if p.LeaseUUID == leaseUUID {
			return p
		}
	}
	t.Fatalf("provision not found for lease %s", leaseUUID)
	return backend.ProvisionInfo{}
}

// --- Edge-case integration tests ---

func TestIntegration_Docker_MultiContainerProvision(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
	})

	ctx := context.Background()
	leaseUUID := fmt.Sprintf("multi-%d", time.Now().UnixNano())

	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	// Provision with Quantity: 2
	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	// Wait for success callback
	select {
	case cb := <-callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for provision callback")
	}

	// GetInfo → verify 2 instances in response
	info, err := b.GetInfo(ctx, leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, info)
	instances, ok := (*info)["instances"].([]map[string]any)
	require.True(t, ok, "expected instances array")
	assert.Len(t, instances, 2, "expected 2 instances")

	// ListProvisions → verify status=Ready
	prov := getProvisionInfo(t, b, leaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)

	// Verify 2 containers exist in Docker
	containers := inspectProvisionContainers(t, leaseUUID)
	assert.Len(t, containers, 2, "expected 2 Docker containers")

	// Deprovision → verify both containers removed
	err = b.Deprovision(ctx, leaseUUID)
	require.NoError(t, err)

	containers = inspectProvisionContainers(t, leaseUUID)
	assert.Empty(t, containers, "expected all containers removed after deprovision")
}

func TestIntegration_Docker_ContainerKilled_Detected(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.ReconcileInterval = 2 * time.Second // fast detection
	})

	ctx := context.Background()
	leaseUUID := fmt.Sprintf("killed-%d", time.Now().UnixNano())

	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	// Wait for success callback
	select {
	case cb := <-callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for success callback")
	}

	// Kill the container via Docker API
	containers := inspectProvisionContainers(t, leaseUUID)
	require.Len(t, containers, 1)
	killContainer(t, containers[0].ID)

	// Wait for failure callback from recoverState (reconcile loop)
	select {
	case cb := <-callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusFailed, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for failure callback after kill")
	}

	// ListProvisions → status=Failed, FailCount=1
	prov := getProvisionInfo(t, b, leaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	assert.Equal(t, 1, prov.FailCount)
}

func TestIntegration_Docker_MultiContainer_PartialKill(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.ReconcileInterval = 2 * time.Second // fast detection
	})

	ctx := context.Background()
	leaseUUID := fmt.Sprintf("partial-%d", time.Now().UnixNano())

	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	// Provision with Quantity: 2
	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	// Wait for success callback
	select {
	case cb := <-callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for success callback")
	}

	// Kill only ONE container
	containers := inspectProvisionContainers(t, leaseUUID)
	require.Len(t, containers, 2)
	killContainer(t, containers[0].ID)

	// Wait for failure callback from recoverState
	select {
	case cb := <-callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusFailed, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for failure callback after partial kill")
	}

	// Entire provision should be marked Failed (not just one instance)
	prov := getProvisionInfo(t, b, leaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
}

func TestIntegration_Docker_ImmediateExit(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.ContainerReadonlyRootfs = ptrBool(false) // busybox "false" needs no tmpfs
	})

	ctx := context.Background()
	leaseUUID := fmt.Sprintf("exit-%d", time.Now().UnixNano())

	// Command ["false"] exits immediately with code 1
	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"false"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	// Wait for failure callback (doProvision startup verify detects exit)
	select {
	case cb := <-callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusFailed, cb.Status)
		assert.Contains(t, cb.Error, "exited", "error should mention container exited")
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for failure callback")
	}
}

func TestIntegration_Docker_HealthCheckTimeout(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.ProvisionTimeout = 15 * time.Second // short, to avoid slow test
	})

	ctx := context.Background()
	leaseUUID := fmt.Sprintf("health-%d", time.Now().UnixNano())

	// Health check ["CMD", "false"] always fails
	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
		HealthCheck: &HealthCheckConfig{
			Test:     []string{"CMD", "false"},
			Interval: Duration(1 * time.Second),
			Timeout:  Duration(1 * time.Second),
			Retries:  1,
		},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	// Wait for failure callback
	select {
	case cb := <-callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusFailed, cb.Status)
		assert.True(t,
			strings.Contains(cb.Error, "unhealthy") || strings.Contains(cb.Error, "healthy"),
			"error should mention health: %s", cb.Error,
		)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for failure callback")
	}
}

func TestIntegration_Docker_ColdStartRecovery(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)

	cfg := DefaultConfig()
	cfg.Name = fmt.Sprintf("test-cold-%d", time.Now().UnixNano())
	cfg.CallbackSecret = testCallbackSecret
	cfg.HostAddress = "127.0.0.1"
	cfg.StartupVerifyDuration = 1 * time.Second
	cfg.ReconcileInterval = 1 * time.Hour
	cfg.ProvisionTimeout = 2 * time.Minute
	cfg.NetworkIsolation = ptrBool(false)
	tmpDir := t.TempDir()
	cfg.CallbackDBPath = filepath.Join(tmpDir, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(tmpDir, "diagnostics.db")
	for name, p := range cfg.SKUProfiles {
		p.DiskMB = 0
		cfg.SKUProfiles[name] = p
	}

	logger := slog.Default()
	b, err := New(cfg, logger)
	require.NoError(t, err)

	ctx := context.Background()
	err = b.Start(ctx)
	require.NoError(t, err)

	// Track Docker client for cleanup
	docker, err := NewDockerClient("")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupTestContainers(t, docker, cfg.Name)
		cleanupTestNetworks(t, docker)
		_ = docker.Close()
	})

	leaseUUID := fmt.Sprintf("cold-%d", time.Now().UnixNano())

	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	// Wait for success callback
	select {
	case cb := <-callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for success callback")
	}

	// Stop the backend
	err = b.Stop()
	require.NoError(t, err)

	// Create a NEW Backend instance with same config, pointing to same Docker
	b2, err := New(cfg, logger)
	require.NoError(t, err)

	err = b2.Start(ctx) // triggers recoverState
	require.NoError(t, err)
	t.Cleanup(func() { _ = b2.Stop() })

	// ListProvisions on new backend → provision exists, status=Ready
	prov := getProvisionInfo(t, b2, leaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)

	// GetInfo on new backend → returns connection details
	info, err := b2.GetInfo(ctx, leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "127.0.0.1", (*info)["host"])

	// Cleanup
	err = b2.Deprovision(ctx, leaseUUID)
	require.NoError(t, err)
}

func TestIntegration_Docker_ColdStartRecovery_DeadContainer(t *testing.T) {
	callbackServer1, callbackCh1 := startCallbackServer(t)

	cfg := DefaultConfig()
	cfg.Name = fmt.Sprintf("test-cold-dead-%d", time.Now().UnixNano())
	cfg.CallbackSecret = testCallbackSecret
	cfg.HostAddress = "127.0.0.1"
	cfg.StartupVerifyDuration = 1 * time.Second
	cfg.ReconcileInterval = 1 * time.Hour
	cfg.ProvisionTimeout = 2 * time.Minute
	cfg.NetworkIsolation = ptrBool(false)
	tmpDir := t.TempDir()
	cfg.CallbackDBPath = filepath.Join(tmpDir, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(tmpDir, "diagnostics.db")
	for name, p := range cfg.SKUProfiles {
		p.DiskMB = 0
		cfg.SKUProfiles[name] = p
	}

	logger := slog.Default()
	b, err := New(cfg, logger)
	require.NoError(t, err)

	ctx := context.Background()
	err = b.Start(ctx)
	require.NoError(t, err)

	docker, err := NewDockerClient("")
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupTestContainers(t, docker, cfg.Name)
		cleanupTestNetworks(t, docker)
		_ = docker.Close()
	})

	leaseUUID := fmt.Sprintf("cold-dead-%d", time.Now().UnixNano())

	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
		CallbackURL:  callbackServer1.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	select {
	case cb := <-callbackCh1:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for success callback")
	}

	// Kill the container and wait for it to reach "exited" state.
	// ContainerKill is async — the container needs time to transition.
	containers := inspectProvisionContainers(t, leaseUUID)
	require.Len(t, containers, 1)
	killContainer(t, containers[0].ID)
	waitForContainerExited(t, containers[0].ID)

	// Stop the first backend (don't wait for its reconciler to fire)
	err = b.Stop()
	require.NoError(t, err)

	// Create new backend (cold start — no prior in-memory state)
	b2, err := New(cfg, logger)
	require.NoError(t, err)

	err = b2.Start(ctx) // triggers recoverState — finds dead container
	require.NoError(t, err)
	t.Cleanup(func() { _ = b2.Stop() })

	// Cold-start: recoverState detects the dead container and increments FailCount.
	// Unlike ready→failed transitions, cold-start correction does NOT send a callback
	// (there's no prior in-memory state to detect a transition against).
	// Verify status=Failed, FailCount=1 on the new backend.
	prov := getProvisionInfo(t, b2, leaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	assert.Equal(t, 1, prov.FailCount)
}

func TestIntegration_Docker_PortConflict(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
	})

	// Start a TCP listener to occupy port 19876
	ln, err := net.Listen("tcp", "127.0.0.1:19876")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	ctx := context.Background()
	leaseUUID := fmt.Sprintf("port-%d", time.Now().UnixNano())

	manifest := DockerManifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
		Ports: map[string]PortConfig{
			"80/tcp": {HostPort: 19876},
		},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	err = b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "test-tenant",
		ProviderUUID: "test-provider",
		Items:        []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      payload,
	})
	require.NoError(t, err)

	// Wait for failure callback (port bind fails during container start)
	select {
	case cb := <-callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusFailed, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for failure callback")
	}
}

// TestIntegration_EnsureTenantNetwork_ConcurrentRace verifies that two
// concurrent calls to EnsureTenantNetwork for the same tenant both succeed,
// even when the second call races with the first's NetworkCreate.
func TestIntegration_EnsureTenantNetwork_ConcurrentRace(t *testing.T) {
	docker, err := NewDockerClient("")
	require.NoError(t, err)

	ctx := context.Background()
	if err := docker.Ping(ctx); err != nil {
		t.Skip("Docker not available:", err)
	}

	tenant := fmt.Sprintf("race-test-%d", time.Now().UnixNano())

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := docker.RemoveTenantNetworkIfEmpty(cleanupCtx, tenant); err != nil {
			t.Logf("cleanup: failed to remove test network for tenant %s: %v", tenant, err)
		}
	})

	const goroutines = 5
	results := make(chan string, goroutines)
	errs := make(chan error, goroutines)

	for range goroutines {
		go func() {
			id, err := docker.EnsureTenantNetwork(ctx, tenant)
			if err != nil {
				errs <- err
				return
			}
			results <- id
		}()
	}

	var networkIDs []string
	for range goroutines {
		select {
		case id := <-results:
			networkIDs = append(networkIDs, id)
		case err := <-errs:
			t.Fatalf("EnsureTenantNetwork failed: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("timeout waiting for goroutines")
		}
	}

	// All goroutines must return the same network ID.
	require.Len(t, networkIDs, goroutines)
	for _, id := range networkIDs[1:] {
		assert.Equal(t, networkIDs[0], id, "all goroutines should return the same network ID")
	}
}
