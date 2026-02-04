//go:build integration

package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

	if cfgFn != nil {
		cfgFn(&cfg)
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
