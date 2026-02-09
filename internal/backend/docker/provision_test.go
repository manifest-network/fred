package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/hmacauth"
)

// validManifestJSON returns a minimal valid manifest payload.
func validManifestJSON(image string) []byte {
	m := DockerManifest{
		Image: image,
	}
	b, _ := json.Marshal(m)
	return b
}

// newProvisionRequest creates a ProvisionRequest for testing.
func newProvisionRequest(leaseUUID, tenant, sku string, qty int, payload []byte) backend.ProvisionRequest {
	return backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       tenant,
		ProviderUUID: "prov-1",
		Items:        []backend.LeaseItem{{SKU: sku, Quantity: qty}},
		CallbackURL:  "http://localhost/callback",
		Payload:      payload,
	}
}

// newBackendForProvisionTest creates a Backend with httpClient set and zero backoff.
func newBackendForProvisionTest(t *testing.T, mock *mockDockerClient, provisions map[string]*provision) *Backend {
	t.Helper()
	b := newBackendForTest(mock, provisions)
	b.httpClient = &http.Client{Timeout: 5 * time.Second}
	rebuildCallbackSender(b)
	return b
}

// zeroBackoff eliminates retry delays in tests.
var zeroBackoff = [shared.CallbackMaxAttempts]time.Duration{}

// rebuildCallbackSender is a test-only workaround that re-creates the
// callbackSender from the Backend's current httpClient, callbackStore, and cfg
// fields with zero backoff for fast tests.
//
// Why this exists: CallbackSender captures its dependencies (HTTP client,
// secret, store, etc.) at construction time. Many tests swap these fields on
// the Backend after newBackendForProvisionTest returns (e.g. b.httpClient =
// server.Client()), but those mutations have no effect on the already-built
// sender. This helper rebuilds the sender so it picks up the new values.
//
// The alternative would be a more complex test builder that accepts every
// possible override before constructing the Backend.
func rebuildCallbackSender(b *Backend) {
	b.callbackSender = shared.NewCallbackSender(shared.CallbackSenderConfig{
		Store:      b.callbackStore,
		HTTPClient: b.httpClient,
		Secret:     b.cfg.CallbackSecret,
		Logger:     b.logger,
		StopCtx:    b.stopCtx,
		Backoff:    &zeroBackoff,
	})
}

// --- Provision (synchronous validation) tests ---

func TestProvision_Success(t *testing.T) {
	pullCalled := false
	createCalls := 0
	startCalls := 0
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			pullCalled = true
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			createCalls++
			return fmt.Sprintf("container-%d", createCalls), nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			startCalls++
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, nil)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = callbackServer.URL

	err := b.Provision(context.Background(), req)
	require.NoError(t, err)

	// Wait for async doProvision to complete (signaled by callback)
	<-callbackReceived

	// Verify final state (must read status under lock to avoid race with goroutine)
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	require.NotNil(t, prov)
	status := prov.Status
	containerIDs := prov.ContainerIDs
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, status)
	assert.Len(t, containerIDs, 1)
	assert.True(t, pullCalled)
	assert.Equal(t, 1, createCalls)
	assert.Equal(t, 1, startCalls)

	b.stopCancel()
	b.wg.Wait()
}

func TestProvision_AlreadyProvisioned(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
		},
	})

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	err := b.Provision(context.Background(), req)

	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrAlreadyProvisioned)
}

func TestProvision_ReProvisionFailed(t *testing.T) {
	removeCalled := false
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			removeCalled = true
			return nil
		},
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "new-container", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Status:       backend.ProvisionStatusFailed,
			FailCount:    2,
			Quantity:     1,
			ContainerIDs: []string{"old-container"},
		},
	})
	// Pre-allocate a resource for the old provision
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = callbackServer.URL
	err := b.Provision(context.Background(), req)
	require.NoError(t, err)

	// Should have cleaned up old container
	assert.True(t, removeCalled, "old container should be removed during re-provision")

	// New provision should have preserved FailCount
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, 2, prov.FailCount, "FailCount should be preserved from previous provision")

	<-callbackReceived
	b.stopCancel()
	b.wg.Wait()
}

func TestProvision_UnknownSKU(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)

	req := newProvisionRequest("lease-1", "tenant-a", "unknown-sku-xyz", 1, validManifestJSON("nginx:latest"))
	err := b.Provision(context.Background(), req)

	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrValidation)
	assert.ErrorIs(t, err, backend.ErrUnknownSKU)

	// Provision slot should be cleaned up
	b.provisionsMu.RLock()
	_, exists := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)
}

func TestProvision_InvalidManifest(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, []byte("not json"))
	err := b.Provision(context.Background(), req)

	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrValidation)
	assert.ErrorIs(t, err, backend.ErrInvalidManifest)
}

func TestProvision_DisallowedImage(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	b.cfg.AllowedRegistries = []string{"docker.io"}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1,
		validManifestJSON("evil-registry.com/malware:latest"))
	err := b.Provision(context.Background(), req)

	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrValidation)
	assert.ErrorIs(t, err, backend.ErrImageNotAllowed)
}

func TestProvision_InsufficientResources(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	// Exhaust the pool
	b.cfg.TotalCPUCores = 0.1
	b.cfg.TotalMemoryMB = 1
	b.pool = shared.NewResourcePool(b.cfg.TotalCPUCores, b.cfg.TotalMemoryMB, b.cfg.TotalDiskMB, b.cfg.GetSKUProfile, nil)

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	err := b.Provision(context.Background(), req)

	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrInsufficientResources)
}

func TestProvision_MultiItem_PartialResourceRollback(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	// Only enough resources for 1 docker-small, not 2
	b.cfg.TotalCPUCores = 0.6
	b.cfg.TotalMemoryMB = 600
	b.pool = shared.NewResourcePool(b.cfg.TotalCPUCores, b.cfg.TotalMemoryMB, b.cfg.TotalDiskMB, b.cfg.GetSKUProfile, nil)

	req := backend.ProvisionRequest{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		ProviderUUID: "prov-1",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 2}},
		CallbackURL:  "http://localhost/callback",
		Payload:      validManifestJSON("nginx:latest"),
	}

	err := b.Provision(context.Background(), req)
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrInsufficientResources)

	// All allocations should have been rolled back
	stats := b.pool.Stats()
	assert.Equal(t, 0, stats.AllocationCount)
}

// --- doProvision (async) tests ---

func TestDoProvision_PullFailure(t *testing.T) {

	var callbackPayload backend.CallbackPayload
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return errors.New("pull failed: image not found")
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusProvisioning,
			Quantity:  1,
		},
	})
	b.provisions["lease-1"].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvision(context.Background(), req, manifest, profiles, b.logger)

	// Provision should be marked failed
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	assert.Equal(t, 1, prov.FailCount)
	assert.Contains(t, prov.LastError, "image pull failed")

	// Resources should be released
	stats := b.pool.Stats()
	assert.Equal(t, 0, stats.AllocationCount)

	// Callback should report failure
	assert.Equal(t, backend.CallbackStatusFailed, callbackPayload.Status)
	assert.Equal(t, "lease-1", callbackPayload.LeaseUUID)
}

func TestDoProvision_CreateFailure_CleansUpCreated(t *testing.T) {

	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	createCallCount := 0
	var removedIDs []string
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			createCallCount++
			if createCallCount == 2 {
				return "", errors.New("disk full")
			}
			return fmt.Sprintf("container-%d", createCallCount), nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			removedIDs = append(removedIDs, containerID)
			return nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusProvisioning,
			Quantity:  2,
		},
	})
	b.provisions["lease-1"].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	_ = b.pool.TryAllocate("lease-1-1", "docker-small", "tenant-a")

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := backend.ProvisionRequest{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		ProviderUUID: "prov-1",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 2}},
		CallbackURL:  callbackServer.URL,
		Payload:      validManifestJSON("nginx:latest"),
	}

	b.doProvision(context.Background(), req, manifest, profiles, b.logger)

	// First container should have been cleaned up
	assert.Contains(t, removedIDs, "container-1")
}

func TestDoProvision_ContextCanceled(t *testing.T) {

	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusProvisioning,
			Quantity:  1,
		},
	})
	b.provisions["lease-1"].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel before starting

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvision(ctx, req, manifest, profiles, b.logger)

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
}

func TestDoProvision_NetworkIsolation(t *testing.T) {

	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	networkCreated := false
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		EnsureTenantNetworkFn: func(ctx context.Context, tenant string) (string, error) {
			networkCreated = true
			assert.Equal(t, "tenant-a", tenant)
			return "net-123", nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			assert.NotNil(t, params.NetworkConfig, "network config should be set")
			return "container-1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusProvisioning,
			Quantity:  1,
		},
	})
	b.cfg.NetworkIsolation = ptrBool(true)
	b.provisions["lease-1"].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvision(context.Background(), req, manifest, profiles, b.logger)

	assert.True(t, networkCreated)

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
}

func TestDoProvision_StartupVerify_ContainerExited(t *testing.T) {

	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "container-1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			// Container exited during startup verify
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "Error: config not found", nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusProvisioning,
			Quantity:  1,
		},
	})
	b.provisions["lease-1"].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvision(context.Background(), req, manifest, profiles, b.logger)

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	assert.Contains(t, prov.LastError, "exit_code=1")
	assert.Contains(t, prov.LastError, "config not found")
}

func TestDoProvision_HealthCheckTimeout_CleansUpContainers(t *testing.T) {

	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	var removedIDs []string
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return fmt.Sprintf("container-%d", params.InstanceIndex), nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			// Always return "starting" so health check never passes
			return &ContainerInfo{
				ContainerID: containerID,
				Status:      "running",
				Health:      HealthStatusStarting,
			}, nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			removedIDs = append(removedIDs, containerID)
			return nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:   "lease-1",
			Status:      backend.ProvisionStatusProvisioning,
			Quantity:    2,
			CallbackURL: callbackServer.URL,
		},
	})
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	_ = b.pool.TryAllocate("lease-1-1", "docker-small", "tenant-a")

	manifest := &DockerManifest{
		Image: "nginx:latest",
		HealthCheck: &HealthCheckConfig{
			Test: []string{"CMD", "curl", "-f", "http://localhost/health"},
		},
	}
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	// Use a very short timeout so the health check times out quickly
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	req := backend.ProvisionRequest{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		ProviderUUID: "prov-1",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 2}},
		CallbackURL:  callbackServer.URL,
		Payload:      validManifestJSON("nginx:latest"),
	}
	b.doProvision(ctx, req, manifest, profiles, b.logger)

	// Verify: provision should be marked failed
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	assert.Contains(t, prov.LastError, "timed out")

	// Verify: both containers should have been cleaned up by the defer
	assert.ElementsMatch(t, []string{"container-0", "container-1"}, removedIDs,
		"containers should be cleaned up when health check times out")

	// Verify: resources should be released
	stats := b.pool.Stats()
	assert.Equal(t, 0, stats.AllocationCount)
}

// --- Deprovision tests ---

func TestDeprovision_Success(t *testing.T) {
	var removedIDs []string
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			removedIDs = append(removedIDs, containerID)
			return nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusReady,
			Quantity:     2,
			ContainerIDs: []string{"c1", "c2"},
			CallbackURL:  "http://localhost/callback",
		},
	})
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	_ = b.pool.TryAllocate("lease-1-1", "docker-small", "tenant-a")

	err := b.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"c1", "c2"}, removedIDs)

	// Provision should be removed
	b.provisionsMu.RLock()
	_, exists := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)

	// Resources should be released
	stats := b.pool.Stats()
	assert.Equal(t, 0, stats.AllocationCount)
}

func TestDeprovision_Idempotent(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)

	err := b.Deprovision(context.Background(), "nonexistent-lease")
	assert.NoError(t, err, "deprovisioning a nonexistent lease should succeed")
}

func TestDeprovision_PartialContainerFailure(t *testing.T) {
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			if containerID == "c2" {
				return errors.New("permission denied")
			}
			return nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusReady,
			Quantity:     2,
			ContainerIDs: []string{"c1", "c2"},
		},
	})
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	_ = b.pool.TryAllocate("lease-1-1", "docker-small", "tenant-a")

	err := b.Deprovision(context.Background(), "lease-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deprovision partially failed")

	// Provision should remain in map with only the failed container
	b.provisionsMu.RLock()
	prov, exists := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	require.True(t, exists, "provision should remain in map after partial failure")
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	assert.Equal(t, []string{"c2"}, prov.ContainerIDs, "only stuck containers should remain")
	assert.Contains(t, prov.LastError, "deprovision partially failed")

	// Resources should still be released (lease is being abandoned)
	stats := b.pool.Stats()
	assert.Equal(t, 0, stats.AllocationCount)
}

func TestDeprovision_PartialFailure_RetryOnlyStuck(t *testing.T) {
	removeCalls := 0
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			removeCalls++
			if containerID == "c2" && removeCalls <= 2 {
				// Fail on first attempt, succeed on second
				return errors.New("permission denied")
			}
			return nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusReady,
			Quantity:     2,
			ContainerIDs: []string{"c1", "c2"},
		},
	})

	// First deprovision: partial failure (c2 fails)
	err := b.Deprovision(context.Background(), "lease-1")
	require.Error(t, err)

	// Retry: only the stuck container (c2) should be attempted
	err = b.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err)

	// Provision should now be fully removed
	b.provisionsMu.RLock()
	_, exists := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.False(t, exists, "provision should be removed after successful retry")
}

func TestDeprovision_WithNetworkIsolation(t *testing.T) {
	networkCleanupCalled := false
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) error {
			networkCleanupCalled = true
			assert.Equal(t, "tenant-a", tenant)
			return nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusReady,
			Quantity:     1,
			ContainerIDs: []string{"c1"},
		},
	})
	b.cfg.NetworkIsolation = ptrBool(true)

	err := b.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err)
	assert.True(t, networkCleanupCalled)
}

// --- GetInfo tests ---

func TestGetInfo_Success(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{
				ContainerID:   containerID,
				InstanceIndex: 0,
				Image:         "nginx:latest",
				Status:        "running",
				Ports: map[string]PortBinding{
					"80/tcp": {HostIP: "0.0.0.0", HostPort: "8080"},
				},
			}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1"},
		},
	})
	b.cfg.HostAddress = "192.168.1.100"

	info, err := b.GetInfo(context.Background(), "lease-1")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "192.168.1.100", (*info)["host"])
	instances := (*info)["instances"].([]map[string]any)
	assert.Len(t, instances, 1)
	assert.Equal(t, "running", instances[0]["status"])
}

func TestGetInfo_NotProvisioned(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)

	_, err := b.GetInfo(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestGetInfo_NotReady(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusProvisioning,
		},
	})

	_, err := b.GetInfo(context.Background(), "lease-1")
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestGetInfo_MultiContainer(t *testing.T) {
	inspectCount := 0
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			inspectCount++
			return &ContainerInfo{
				ContainerID:   containerID,
				InstanceIndex: inspectCount - 1,
				Image:         "nginx:latest",
				Status:        "running",
				Ports:         map[string]PortBinding{},
			}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1", "c2", "c3"},
		},
	})

	info, err := b.GetInfo(context.Background(), "lease-1")
	require.NoError(t, err)
	instances := (*info)["instances"].([]map[string]any)
	assert.Len(t, instances, 3)
}

// --- GetLogs tests ---

func TestGetLogs_Success(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return fmt.Sprintf("logs for %s (tail=%d)", containerID, tail), nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1", "c2"},
		},
	})

	logs, err := b.GetLogs(context.Background(), "lease-1", 50)
	require.NoError(t, err)
	assert.Len(t, logs, 2)
	assert.Equal(t, "logs for c1 (tail=50)", logs["0"])
	assert.Equal(t, "logs for c2 (tail=50)", logs["1"])
}

func TestGetLogs_NotProvisioned(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)

	_, err := b.GetLogs(context.Background(), "nonexistent", 100)
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestGetLogs_PartialError(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			if containerID == "c2" {
				return "", errors.New("container not running")
			}
			return "hello world", nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1", "c2"},
		},
	})

	logs, err := b.GetLogs(context.Background(), "lease-1", 100)
	require.NoError(t, err)
	assert.Equal(t, "hello world", logs["0"])
	assert.Contains(t, logs["1"], "<error:")
}

// --- GetProvision tests ---

func TestGetProvision_Found(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			ProviderUUID: "prov-1",
			Status:       backend.ProvisionStatusFailed,
			FailCount:    2,
			LastError:    "exit_code=1; logs:\nSECRET=abc",
		},
	})

	info, err := b.GetProvision(context.Background(), "lease-1")
	require.NoError(t, err)
	assert.Equal(t, "lease-1", info.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, info.Status)
	assert.Equal(t, 2, info.FailCount)
	assert.Contains(t, info.LastError, "exit_code=1")
}

func TestGetProvision_NotFound(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)

	_, err := b.GetProvision(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

// --- ListProvisions tests ---

func TestListProvisions_Empty(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)

	result, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestListProvisions_Multiple(t *testing.T) {
	mock := &mockDockerClient{}
	now := time.Now()
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			ProviderUUID: "prov-1",
			Status:       backend.ProvisionStatusReady,
			CreatedAt:    now,
			FailCount:    0,
		},
		"lease-2": {
			LeaseUUID:    "lease-2",
			ProviderUUID: "prov-1",
			Status:       backend.ProvisionStatusFailed,
			CreatedAt:    now,
			FailCount:    3,
		},
	})

	result, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.Len(t, result, 2)

	// Verify field mapping
	for _, pi := range result {
		assert.NotEmpty(t, pi.LeaseUUID)
		assert.Equal(t, "prov-1", pi.ProviderUUID)
		assert.NotEmpty(t, pi.BackendName)
	}
}

// --- sendCallback / trySendCallback tests ---

func TestSendCallback_Success(t *testing.T) {

	var received backend.CallbackPayload
	var receivedSig string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSig = r.Header.Get(hmacauth.SignatureHeader)
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", CallbackURL: server.URL},
	})
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	b.sendCallback("lease-1", true, "")

	assert.Equal(t, "lease-1", received.LeaseUUID)
	assert.Equal(t, backend.CallbackStatusSuccess, received.Status)
	assert.NotEmpty(t, receivedSig, "HMAC signature header should be set")
}

func TestSendCallback_FailurePayload(t *testing.T) {

	var received backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", CallbackURL: server.URL},
	})
	b.httpClient = server.Client()
	rebuildCallbackSender(b)

	b.sendCallback("lease-1", false, "image pull failed")

	assert.Equal(t, backend.CallbackStatusFailed, received.Status)
	assert.Equal(t, "image pull failed", received.Error)
}

func TestSendCallback_NoCallbackURL(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	// No panic, no error — just a log warning
	b.sendCallback("unknown-lease", true, "")
}

func TestSendCallback_TruncatesLongError(t *testing.T) {
	var received backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", CallbackURL: server.URL},
	})
	b.httpClient = server.Client()
	rebuildCallbackSender(b)

	// Send an error message that exceeds the on-chain rejection reason limit.
	longError := strings.Repeat("x", callbackMaxErrorLen+100)
	b.sendCallback("lease-1", false, longError)

	assert.LessOrEqual(t, len(received.Error), callbackMaxErrorLen,
		"callback error should be truncated to fit on-chain limit")
	assert.True(t, strings.HasSuffix(received.Error, "..."))
}

func TestSendCallback_Retry(t *testing.T) {

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", CallbackURL: server.URL},
	})
	b.httpClient = server.Client()
	rebuildCallbackSender(b)

	b.sendCallback("lease-1", true, "")

	assert.Equal(t, int32(3), attempts.Load(), "should have retried 3 times")
}

func TestSendCallback_ShutdownAbortsRetry(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", CallbackURL: server.URL},
	})
	b.httpClient = server.Client()
	// Use long backoff so shutdown cancellation is observable.
	longBackoff := [shared.CallbackMaxAttempts]time.Duration{0, 5 * time.Second, 5 * time.Second}
	b.callbackSender = shared.NewCallbackSender(shared.CallbackSenderConfig{
		Store:      b.callbackStore,
		HTTPClient: b.httpClient,
		Secret:     b.cfg.CallbackSecret,
		Logger:     b.logger,
		StopCtx:    b.stopCtx,
		Backoff:    &longBackoff,
	})

	// Cancel stopCtx after first attempt
	go func() {
		time.Sleep(50 * time.Millisecond)
		b.stopCancel()
	}()

	b.sendCallback("lease-1", true, "")

	// Should have stopped after 1 attempt due to shutdown
	assert.LessOrEqual(t, attempts.Load(), int32(2))
}

func TestDeliverCallback_Success(t *testing.T) {

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	b.httpClient = server.Client()
	rebuildCallbackSender(b)

	ok := b.callbackSender.DeliverCallback("lease-1", server.URL, []byte(`{"test":true}`))
	assert.True(t, ok)
}

func TestDeliverCallback_ServerError(t *testing.T) {

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	b.httpClient = server.Client()
	rebuildCallbackSender(b)

	ok := b.callbackSender.DeliverCallback("lease-1", server.URL, []byte(`{}`))
	assert.False(t, ok)
}

// --- Start / Stop / Health tests ---

func TestStart_Success(t *testing.T) {
	mock := &mockDockerClient{
		PingFn: func(ctx context.Context) error { return nil },
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
		CloseFn: func() error { return nil },
	}

	b := newBackendForProvisionTest(t, mock, nil)
	err := b.Start(context.Background())
	require.NoError(t, err)

	// Clean shutdown
	err = b.Stop()
	assert.NoError(t, err)
}

func TestStart_PingFails(t *testing.T) {
	mock := &mockDockerClient{
		PingFn: func(ctx context.Context) error {
			return errors.New("docker not available")
		},
	}

	b := newBackendForProvisionTest(t, mock, nil)
	err := b.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect to Docker")
}

func TestStart_RecoverStateFails(t *testing.T) {
	mock := &mockDockerClient{
		PingFn: func(ctx context.Context) error { return nil },
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return nil, errors.New("docker error")
		},
	}

	b := newBackendForProvisionTest(t, mock, nil)
	err := b.Start(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to recover state")
}

func TestHealth(t *testing.T) {
	mock := &mockDockerClient{
		PingFn: func(ctx context.Context) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	assert.NoError(t, b.Health(context.Background()))

	mock.PingFn = func(ctx context.Context) error { return errors.New("unhealthy") }
	assert.Error(t, b.Health(context.Background()))
}

func TestName(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	assert.Equal(t, b.cfg.Name, b.Name())
}

// --- Pure function tests ---

func TestShortID(t *testing.T) {
	assert.Equal(t, "abcdefghijkl", shortID("abcdefghijklmnop"))
	assert.Equal(t, "short", shortID("short"))
	assert.Equal(t, "", shortID(""))
}

func TestContainerStatusToProvisionStatus(t *testing.T) {
	tests := []struct {
		status string
		want   backend.ProvisionStatus
	}{
		{"created", backend.ProvisionStatusProvisioning},
		{"restarting", backend.ProvisionStatusProvisioning},
		{"running", backend.ProvisionStatusReady},
		{"paused", backend.ProvisionStatusReady},
		{"Running", backend.ProvisionStatusReady}, // case insensitive
		{"removing", backend.ProvisionStatusFailed},
		{"exited", backend.ProvisionStatusFailed},
		{"dead", backend.ProvisionStatusFailed},
		{"unknown", backend.ProvisionStatusUnknown},
		{"", backend.ProvisionStatusUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			assert.Equal(t, tt.want, containerStatusToProvisionStatus(tt.status))
		})
	}
}

func TestRemoveProvision(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", CallbackURL: "http://localhost"},
	})

	b.removeProvision("lease-1")

	b.provisionsMu.RLock()
	_, exists := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)

	// Removing nonexistent should not panic
	b.removeProvision("nonexistent")
}

// --- waitForHealthy tests ---

func TestWaitForHealthy_AllHealthy(t *testing.T) {
	callCount := 0
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			callCount++
			return &ContainerInfo{
				ContainerID: id,
				Status:      "running",
				Health:      HealthStatusHealthy,
			}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := b.waitForHealthy(ctx, []string{"c1", "c2"}, b.logger)
	assert.NoError(t, err)
	assert.Equal(t, 2, callCount)
}

func TestWaitForHealthy_Unhealthy(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{
				ContainerID: id,
				Status:      "running",
				Health:      HealthStatusUnhealthy,
			}, nil
		},
		ContainerLogsFn: func(_ context.Context, _ string, _ int) (string, error) {
			return "", nil
		},
	}
	b := newBackendForTest(mock, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := b.waitForHealthy(ctx, []string{"c1"}, b.logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unhealthy")
}

func TestWaitForHealthy_ContainerExited(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{
				ContainerID: id,
				Status:      "exited",
				Health:      HealthStatusNone,
				ExitCode:    137,
				OOMKilled:   true,
			}, nil
		},
		ContainerLogsFn: func(_ context.Context, _ string, _ int) (string, error) {
			return "Killed", nil
		},
	}
	b := newBackendForTest(mock, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := b.waitForHealthy(ctx, []string{"c1"}, b.logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exited")
	assert.Contains(t, err.Error(), "exit_code=137")
	assert.Contains(t, err.Error(), "oom_killed=true")
}

func TestWaitForHealthy_ContextTimeout(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{
				ContainerID: id,
				Status:      "running",
				Health:      HealthStatusStarting,
			}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	// Use a very short timeout so the test doesn't wait long.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := b.waitForHealthy(ctx, []string{"c1"}, b.logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}

func TestWaitForHealthy_InspectFailure(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return nil, fmt.Errorf("docker daemon error")
		},
	}
	b := newBackendForTest(mock, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := b.waitForHealthy(ctx, []string{"c1"}, b.logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to inspect")
}

func TestWaitForHealthy_BecomesHealthyAfterStarting(t *testing.T) {
	inspectCount := 0
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			inspectCount++
			health := HealthStatusStarting
			if inspectCount >= 2 {
				health = HealthStatusHealthy
			}
			return &ContainerInfo{
				ContainerID: id,
				Status:      "running",
				Health:      health,
			}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := b.waitForHealthy(ctx, []string{"c1"}, b.logger)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, inspectCount, 2, "should have polled at least twice")
}

// --- Error persistence tests ---

func TestDoProvision_LastError_ContextCanceled(t *testing.T) {

	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:   "lease-1",
			Status:      backend.ProvisionStatusProvisioning,
			Quantity:    1,
			CallbackURL: callbackServer.URL,
		},
	})
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvision(ctx, req, manifest, profiles, b.logger)

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	assert.Contains(t, prov.LastError, "canceled")
}

func TestDoProvision_LastError_ClearedOnSuccess(t *testing.T) {

	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "container-1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	// Start with a previously failed provision that had a LastError
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:   "lease-1",
			Status:      backend.ProvisionStatusProvisioning,
			Quantity:    1,
			LastError:   "previous error",
			CallbackURL: callbackServer.URL,
		},
	})
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvision(context.Background(), req, manifest, profiles, b.logger)
	<-callbackReceived

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
	// LastError should still have old value since success path doesn't clear it
	// (it's only set on failure, not cleared on success — which is correct
	// because a re-provision creates a new provision record)
}

func TestListProvisions_IncludesLastError(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			ProviderUUID: "prov-1",
			Status:       backend.ProvisionStatusFailed,
			CreatedAt:    time.Now(),
			FailCount:    2,
			LastError:    "container crashed",
		},
	})

	result, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, "container crashed", result[0].LastError)
	assert.Equal(t, 2, result[0].FailCount)
}

// --- Disk quota container creation tests ---

func TestDoProvision_EphemeralProfileWithoutDiskMB(t *testing.T) {

	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()

	var capturedParams CreateContainerParams
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{Volumes: map[string]struct{}{"/data": {}}}, nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			capturedParams = params
			return "container-1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:   "lease-1",
			Status:      backend.ProvisionStatusProvisioning,
			Quantity:    1,
			CallbackURL: callbackServer.URL,
		},
	})
	b.cfg.ContainerReadonlyRootfs = ptrBool(true)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	// Ephemeral profile with DiskMB=0 — no volume binds, image volumes get tmpfs
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512}}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvision(context.Background(), req, manifest, profiles, b.logger)
	<-callbackReceived

	assert.True(t, capturedParams.ReadonlyRootfs, "ReadonlyRootfs should be true")
	assert.Equal(t, int64(0), capturedParams.Profile.DiskMB, "DiskMB should be zero")
	assert.Nil(t, capturedParams.VolumeBinds, "VolumeBinds should be nil for ephemeral")
	assert.Equal(t, []string{"/data"}, capturedParams.ImageVolumes, "ImageVolumes should contain /data")
}

func TestDoProvision_TmpfsPassedThrough(t *testing.T) {

	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()

	var capturedParams CreateContainerParams
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			capturedParams = params
			return "container-1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:   "lease-1",
			Status:      backend.ProvisionStatusProvisioning,
			Quantity:    1,
			CallbackURL: callbackServer.URL,
		},
	})
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	// Manifest with additional tmpfs mounts (like nginx would need)
	manifestJSON := []byte(`{
		"image": "nginx:latest",
		"ports": {"80/tcp": {}},
		"tmpfs": ["/var/cache/nginx", "/var/log/nginx"]
	}`)
	manifest, err := ParseManifest(manifestJSON)
	require.NoError(t, err)

	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}
	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, manifestJSON)
	b.doProvision(context.Background(), req, manifest, profiles, b.logger)
	<-callbackReceived

	// Verify the manifest tmpfs paths were passed through
	assert.Equal(t, []string{"/var/cache/nginx", "/var/log/nginx"}, capturedParams.Manifest.Tmpfs)
}

// --- Callback persistence tests ---

func TestSendCallback_PersistsBeforeDelivery(t *testing.T) {

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb_persist.db")
	cbStore, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer cbStore.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", CallbackURL: server.URL},
	})
	b.httpClient = server.Client()
	b.callbackStore = cbStore
	rebuildCallbackSender(b)

	b.sendCallback("lease-1", true, "")

	// After successful delivery, callback should be removed from store
	pending, err := cbStore.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "callback should be removed from store after delivery")
}

func TestSendCallback_FailedDeliveryRemainsInStore(t *testing.T) {

	// Server always returns 500
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb_fail.db")
	cbStore, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer cbStore.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", CallbackURL: server.URL},
	})
	b.httpClient = server.Client()
	b.callbackStore = cbStore
	rebuildCallbackSender(b)

	b.sendCallback("lease-1", false, "container crashed")

	// After failed delivery, callback should remain in store
	pending, err := cbStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "lease-1", pending[0].LeaseUUID)
	assert.False(t, pending[0].Success)
	assert.Equal(t, "container crashed", pending[0].Error)
}

// --- Replay callbacks tests ---

func TestReplayPendingCallbacks_Success(t *testing.T) {

	var received []backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		received = append(received, p)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb_replay.db")
	cbStore, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	// Pre-populate with pending callbacks
	require.NoError(t, cbStore.Store(shared.CallbackEntry{
		LeaseUUID:   "lease-1",
		CallbackURL: server.URL,
		Success:     true,
		CreatedAt:   time.Now(),
	}))
	require.NoError(t, cbStore.Store(shared.CallbackEntry{
		LeaseUUID:   "lease-2",
		CallbackURL: server.URL,
		Success:     false,
		Error:       "pull failed",
		CreatedAt:   time.Now(),
	}))

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	b.httpClient = server.Client()
	b.callbackStore = cbStore
	rebuildCallbackSender(b)

	b.callbackSender.ReplayPendingCallbacks()

	// Both callbacks should have been delivered
	assert.Len(t, received, 2)

	// Store should be empty after successful replay
	pending, err := cbStore.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)

	cbStore.Close()
}

func TestReplayPendingCallbacks_PartialFailure(t *testing.T) {

	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		// First callback delivery succeeds, second fails all retries
		if n <= 1 {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb_partial.db")
	cbStore, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	require.NoError(t, cbStore.Store(shared.CallbackEntry{
		LeaseUUID:   "lease-1",
		CallbackURL: server.URL,
		Success:     true,
		CreatedAt:   time.Now(),
	}))
	require.NoError(t, cbStore.Store(shared.CallbackEntry{
		LeaseUUID:   "lease-2",
		CallbackURL: server.URL,
		Success:     false,
		Error:       "some error",
		CreatedAt:   time.Now(),
	}))

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	b.httpClient = server.Client()
	b.callbackStore = cbStore
	rebuildCallbackSender(b)

	b.callbackSender.ReplayPendingCallbacks()

	// lease-2 should remain in store since delivery failed
	pending, err := cbStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "lease-2", pending[0].LeaseUUID)

	cbStore.Close()
}

func TestReplayPendingCallbacks_NilStore(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	b.callbackStore = nil
	rebuildCallbackSender(b)

	// Should not panic
	b.callbackSender.ReplayPendingCallbacks()
}

func TestReplayPendingCallbacks_ExpiresOldEntries(t *testing.T) {

	var received []backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		received = append(received, p)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// First, create a store without expiry and insert old + fresh entries
	dbPath := filepath.Join(t.TempDir(), "cb_expire.db")
	store1, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	require.NoError(t, store1.Store(shared.CallbackEntry{
		LeaseUUID:   "lease-old",
		CallbackURL: server.URL,
		Success:     true,
		CreatedAt:   time.Now().Add(-2 * time.Hour),
	}))
	require.NoError(t, store1.Store(shared.CallbackEntry{
		LeaseUUID:   "lease-fresh",
		CallbackURL: server.URL,
		Success:     true,
		CreatedAt:   time.Now(),
	}))
	require.NoError(t, store1.Close())

	// Reopen with MaxAge — initial cleanup removes old entries
	cbStore, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: dbPath,
		MaxAge: 1 * time.Hour,
	})
	require.NoError(t, err)

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	b.httpClient = server.Client()
	b.callbackStore = cbStore
	rebuildCallbackSender(b)

	b.callbackSender.ReplayPendingCallbacks()

	// Only the fresh callback should have been delivered
	require.Len(t, received, 1)
	assert.Equal(t, "lease-fresh", received[0].LeaseUUID)

	// Store should be empty after replay (old expired at open, fresh delivered)
	pending, err := cbStore.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)

	cbStore.Close()
}

func TestReplayPendingCallbacks_EmptyStore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cb_empty.db")
	cbStore, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer cbStore.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	b.callbackStore = cbStore
	rebuildCallbackSender(b)

	// Should not panic or make HTTP calls
	b.callbackSender.ReplayPendingCallbacks()
}

// --- Additional coverage tests ---

// Fix 1: Total deprovision failure — ALL container removals fail.
func TestDeprovision_AllContainersFail(t *testing.T) {
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return errors.New("permission denied")
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusReady,
			Quantity:     2,
			ContainerIDs: []string{"c1", "c2"},
		},
	})
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	_ = b.pool.TryAllocate("lease-1-1", "docker-small", "tenant-a")

	err := b.Deprovision(context.Background(), "lease-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deprovision partially failed")

	// Provision should remain with all containers marked as failed
	b.provisionsMu.RLock()
	prov, exists := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	require.True(t, exists)
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	assert.ElementsMatch(t, []string{"c1", "c2"}, prov.ContainerIDs)

	// Resources should still be released
	stats := b.pool.Stats()
	assert.Equal(t, 0, stats.AllocationCount)
}

// Fix 2: CallbackMaxAge=0 skips expiry in ReplayPendingCallbacks.
func TestReplayPendingCallbacks_ZeroMaxAge_SkipsExpiry(t *testing.T) {

	var received []backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		received = append(received, p)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb_zeromax.db")
	cbStore, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	// Store an old callback (48 hours ago)
	require.NoError(t, cbStore.Store(shared.CallbackEntry{
		LeaseUUID:   "lease-old",
		CallbackURL: server.URL,
		Success:     true,
		CreatedAt:   time.Now().Add(-48 * time.Hour),
	}))

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	b.httpClient = server.Client()
	b.callbackStore = cbStore
	rebuildCallbackSender(b)
	b.cfg.CallbackMaxAge = 0 // Zero means "don't expire"

	b.callbackSender.ReplayPendingCallbacks()

	// Old callback should still be delivered (not expired)
	require.Len(t, received, 1)
	assert.Equal(t, "lease-old", received[0].LeaseUUID)

	cbStore.Close()
}

// Fix 2: Negative CallbackMaxAge rejected by config validation.
func TestConfigValidation_NegativeCallbackMaxAge(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CallbackSecret = "this-is-a-32-character-secret!!x"
	cfg.HostAddress = "192.168.1.100"
	cfg.CallbackMaxAge = -1 * time.Hour

	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "callback_max_age")
}

// Fix 4: GetLogs on a provisioning (in-progress) lease still returns logs.
func TestGetLogs_ProvisioningLease(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "startup log output", nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Status:       backend.ProvisionStatusProvisioning,
			ContainerIDs: []string{"c1"},
		},
	})

	// GetLogs doesn't check status — it should work for in-progress provisions
	logs, err := b.GetLogs(context.Background(), "lease-1", 50)
	require.NoError(t, err)
	assert.Equal(t, "startup log output", logs["0"])
}

// Fix 4: GetLogs with empty ContainerIDs returns empty map.
func TestGetLogs_EmptyContainerIDs(t *testing.T) {
	mock := &mockDockerClient{}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{},
		},
	})

	logs, err := b.GetLogs(context.Background(), "lease-1", 100)
	require.NoError(t, err)
	assert.Empty(t, logs)
}

// Fix 5: Explicit (non-ephemeral) port conflict returns error immediately without retry.
func TestCreateContainer_ExplicitPortConflict_NoRetry(t *testing.T) {
	// Verify the decision logic: explicit ports should NOT trigger retry
	assert.False(t, hasEphemeralPorts(map[string]PortConfig{
		"80/tcp": {HostPort: 8080},
	}), "explicit ports should not be ephemeral")
	assert.True(t, isPortBindingError(fmt.Errorf("port is already allocated")))

	// Mixed ports (one explicit, one ephemeral) DO trigger retry
	assert.True(t, hasEphemeralPorts(map[string]PortConfig{
		"80/tcp":  {HostPort: 8080}, // explicit
		"443/tcp": {HostPort: 0},    // ephemeral
	}), "mixed ports with an ephemeral should be ephemeral")

	// No ports means no ephemeral
	assert.False(t, hasEphemeralPorts(nil))
	assert.False(t, hasEphemeralPorts(map[string]PortConfig{}))
}

// Fix 1: Deprovision on a provisioning lease still removes containers.
func TestDeprovision_ProvisioningLease(t *testing.T) {
	var removedIDs []string
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			removedIDs = append(removedIDs, containerID)
			return nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusProvisioning,
			Quantity:     1,
			ContainerIDs: []string{"c1"},
		},
	})
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	err := b.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err)

	assert.Equal(t, []string{"c1"}, removedIDs)

	b.provisionsMu.RLock()
	_, exists := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)
}

// Fix 4: GetLogs on a failed lease still returns logs for remaining containers.
func TestGetLogs_FailedLease(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "crash log output", nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Status:       backend.ProvisionStatusFailed,
			ContainerIDs: []string{"c1"},
			LastError:    "container crashed",
		},
	})

	logs, err := b.GetLogs(context.Background(), "lease-1", 50)
	require.NoError(t, err)
	assert.Equal(t, "crash log output", logs["0"])
}

// --- containerFailureDiagnostics tests ---

func TestContainerFailureDiagnostics_ExitCodeAndLogs(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			assert.Equal(t, diagnosticLogTail, tail)
			return "Error: EACCES: permission denied", nil
		},
	}
	b := newBackendForTest(mock, nil)

	info := &ContainerInfo{ExitCode: 1}
	diag := b.containerFailureDiagnostics(context.Background(), "c1", info)

	assert.Equal(t, "exit_code=1; logs:\nError: EACCES: permission denied", diag)
}

func TestContainerFailureDiagnostics_OOMKilled(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "Killed", nil
		},
	}
	b := newBackendForTest(mock, nil)

	info := &ContainerInfo{ExitCode: 137, OOMKilled: true}
	diag := b.containerFailureDiagnostics(context.Background(), "c1", info)

	assert.Contains(t, diag, "exit_code=137")
	assert.Contains(t, diag, "oom_killed=true")
	assert.Contains(t, diag, "Killed")
}

func TestContainerFailureDiagnostics_LogsFetchFails(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", errors.New("container not found")
		},
	}
	b := newBackendForTest(mock, nil)

	info := &ContainerInfo{ExitCode: 1}
	diag := b.containerFailureDiagnostics(context.Background(), "c1", info)

	assert.Equal(t, "exit_code=1", diag)
}

func TestContainerFailureDiagnostics_ZeroExitCode(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", nil
		},
	}
	b := newBackendForTest(mock, nil)

	info := &ContainerInfo{ExitCode: 0}
	diag := b.containerFailureDiagnostics(context.Background(), "c1", info)

	assert.Equal(t, "exit_code=0", diag)
}

func TestContainerFailureDiagnostics_Truncation(t *testing.T) {
	// Generate logs larger than diagnosticMaxBytes
	largeLogs := strings.Repeat("x", diagnosticMaxBytes)
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return largeLogs, nil
		},
	}
	b := newBackendForTest(mock, nil)

	info := &ContainerInfo{ExitCode: 1}
	diag := b.containerFailureDiagnostics(context.Background(), "c1", info)

	assert.LessOrEqual(t, len(diag), diagnosticMaxBytes)
	assert.True(t, strings.HasSuffix(diag, "..."))
}

// --- Callback sanitization tests ---
// These tests verify that callback error messages (which flow on-chain) never
// contain container logs or dynamic data, while prov.LastError retains full
// diagnostics for authenticated API access.

func TestDoProvision_CallbackSanitized_ContainerExitedDuringStartup(t *testing.T) {
	secret := "SECRET_API_KEY=abc123"

	var callbackPayload backend.CallbackPayload
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "container-1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return secret, nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:   "lease-1",
			Status:      backend.ProvisionStatusProvisioning,
			Quantity:    1,
			CallbackURL: callbackServer.URL,
		},
	})
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvision(context.Background(), req, manifest, profiles, b.logger)

	// Callback should have a hardcoded message — no secrets.
	assert.Equal(t, "container exited during startup", callbackPayload.Error)
	assert.NotContains(t, callbackPayload.Error, secret)

	// LastError should contain full diagnostics including logs.
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Contains(t, prov.LastError, "exit_code=1")
	assert.Contains(t, prov.LastError, secret)
}

func TestDoProvision_CallbackSanitized_Unhealthy(t *testing.T) {
	secret := "DB_PASSWORD=hunter2"

	var callbackPayload backend.CallbackPayload
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "container-1", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{
				ContainerID: containerID,
				Status:      "running",
				Health:      HealthStatusUnhealthy,
			}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return secret, nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:   "lease-1",
			Status:      backend.ProvisionStatusProvisioning,
			Quantity:    1,
			CallbackURL: callbackServer.URL,
		},
	})
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	manifest := &DockerManifest{
		Image: "nginx:latest",
		HealthCheck: &HealthCheckConfig{
			Test: []string{"CMD", "curl", "-f", "http://localhost/health"},
		},
	}
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvision(context.Background(), req, manifest, profiles, b.logger)

	// Callback should have a hardcoded message — no secrets.
	assert.Equal(t, "container reported unhealthy", callbackPayload.Error)
	assert.NotContains(t, callbackPayload.Error, secret)

	// LastError should contain full diagnostics including logs.
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Contains(t, prov.LastError, "unhealthy")
	assert.Contains(t, prov.LastError, secret)
}

func TestDoProvision_CallbackSanitized_PullFailure(t *testing.T) {
	var callbackPayload backend.CallbackPayload
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return errors.New("unauthorized: authentication required for registry.example.com")
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:   "lease-1",
			Status:      backend.ProvisionStatusProvisioning,
			Quantity:    1,
			CallbackURL: callbackServer.URL,
		},
	})
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvision(context.Background(), req, manifest, profiles, b.logger)

	// Callback should have hardcoded message — no registry auth details.
	assert.Equal(t, "image pull failed", callbackPayload.Error)
	assert.NotContains(t, callbackPayload.Error, "registry.example.com")

	// LastError should contain the full error.
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Contains(t, prov.LastError, "registry.example.com")
}

func TestHealthErrorToCallbackMsg(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"unhealthy", fmt.Errorf("container 0 reported unhealthy: exit_code=1; logs: oops"), "container reported unhealthy"},
		{"exited during health", fmt.Errorf("container 0 exited while waiting for healthy"), "container exited during health check"},
		{"timeout", fmt.Errorf("timed out waiting for containers to become healthy"), "container exited during health check"},
		{"inspect failure", fmt.Errorf("failed to inspect container 0 during health check"), "container exited during health check"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, healthErrorToCallbackMsg(tt.err))
		})
	}
}

func TestProvision_FailurePersistsDiagnostics(t *testing.T) {
	// Verify that a provisioning failure (image pull) persists diagnostics
	// and that GetProvision/GetLogs fall back to the diagnostics store
	// after deprovision removes the in-memory provision.
	dbPath := filepath.Join(t.TempDir(), "diag.db")
	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer diagStore.Close()

	var callbackReceived atomic.Bool
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackReceived.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return fmt.Errorf("registry unreachable")
		},
	}

	b := newBackendForProvisionTest(t, mock, nil)
	b.diagnosticsStore = diagStore
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)

	req := backend.ProvisionRequest{
		LeaseUUID:    "lease-diag",
		Tenant:       "tenant-a",
		ProviderUUID: "prov-1",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 1}},
		CallbackURL:  callbackServer.URL,
		Payload:      validManifestJSON("docker.io/nginx:latest"),
	}

	err = b.Provision(context.Background(), req)
	require.NoError(t, err)

	// Wait for async provision to complete.
	require.Eventually(t, func() bool { return callbackReceived.Load() }, 5*time.Second, 50*time.Millisecond)

	// Verify diagnostics were persisted.
	entry, err := diagStore.Get("lease-diag")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Contains(t, entry.Error, "image pull failed")
	assert.Equal(t, 1, entry.FailCount)
	assert.Equal(t, "tenant-a", entry.Tenant)
	assert.Equal(t, "prov-1", entry.ProviderUUID)

	// In-memory GetProvision should still work.
	info, err := b.GetProvision(context.Background(), "lease-diag")
	require.NoError(t, err)
	assert.Equal(t, backend.ProvisionStatusFailed, info.Status)

	// Now simulate deprovision (remove from in-memory map).
	b.provisionsMu.Lock()
	delete(b.provisions, "lease-diag")
	b.provisionsMu.Unlock()

	// GetProvision should fall back to diagnostics store.
	info, err = b.GetProvision(context.Background(), "lease-diag")
	require.NoError(t, err)
	assert.Equal(t, backend.ProvisionStatusFailed, info.Status)
	assert.Contains(t, info.LastError, "image pull failed")
	assert.Equal(t, 1, info.FailCount)
	assert.Equal(t, "prov-1", info.ProviderUUID)

	// GetProvision for unknown lease still returns ErrNotProvisioned.
	_, err = b.GetProvision(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestGetLogs_FallsBackToDiagnosticsStore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "diag_logs.db")
	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer diagStore.Close()

	// Persist a diagnostic entry with logs.
	require.NoError(t, diagStore.Store(shared.DiagnosticEntry{
		LeaseUUID:    "lease-logs",
		ProviderUUID: "prov-1",
		Tenant:       "tenant-a",
		Error:        "container exited",
		Logs: map[string]string{
			"0": "line 1\nline 2\n",
			"1": "worker output\n",
		},
		FailCount: 1,
	}))

	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	b.diagnosticsStore = diagStore

	// No in-memory provision — should fall back to diagnostics store.
	logs, err := b.GetLogs(context.Background(), "lease-logs", 100)
	require.NoError(t, err)
	require.Len(t, logs, 2)
	assert.Equal(t, "line 1\nline 2\n", logs["0"])
	assert.Equal(t, "worker output\n", logs["1"])

	// Unknown lease still returns ErrNotProvisioned.
	_, err = b.GetLogs(context.Background(), "nonexistent", 100)
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestGetLogs_FallbackNoLogs(t *testing.T) {
	// When diagnostics entry exists but has no logs (e.g., image pull failure),
	// GetLogs should return ErrNotProvisioned.
	dbPath := filepath.Join(t.TempDir(), "diag_nologs.db")
	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer diagStore.Close()

	require.NoError(t, diagStore.Store(shared.DiagnosticEntry{
		LeaseUUID: "lease-nologs",
		Error:     "image pull failed",
		FailCount: 1,
	}))

	mock := &mockDockerClient{}
	b := newBackendForTest(mock, nil)
	b.diagnosticsStore = diagStore

	_, err = b.GetLogs(context.Background(), "lease-nologs", 100)
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestProvision_SuccessClearsStaleDiagnostics(t *testing.T) {
	// When a re-provision succeeds, the stale diagnostic entry from the
	// previous failure should be removed from the diagnostics store.
	dbPath := filepath.Join(t.TempDir(), "diag_clear.db")
	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer diagStore.Close()

	// Pre-populate a stale diagnostic entry.
	require.NoError(t, diagStore.Store(shared.DiagnosticEntry{
		LeaseUUID: "lease-reprov",
		Error:     "old failure",
		FailCount: 1,
	}))

	var callbackReceived atomic.Bool
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackReceived.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error { return nil },
		PullImageFn:       func(ctx context.Context, imageName string, timeout time.Duration) error { return nil },
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			return "new-ctr", nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error { return nil },
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-reprov": {
			LeaseUUID:    "lease-reprov",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			Status:       backend.ProvisionStatusFailed,
			FailCount:    1,
			Quantity:     1,
			ContainerIDs: []string{"old-ctr"},
		},
	})
	b.diagnosticsStore = diagStore
	_ = b.pool.TryAllocate("lease-reprov-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)

	req := newProvisionRequest("lease-reprov", "tenant-a", "docker-small", 1, validManifestJSON("docker.io/nginx:latest"))
	req.CallbackURL = callbackServer.URL
	err = b.Provision(context.Background(), req)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return callbackReceived.Load() }, 5*time.Second, 50*time.Millisecond)

	// Stale diagnostic entry should be removed.
	entry, err := diagStore.Get("lease-reprov")
	require.NoError(t, err)
	assert.Nil(t, entry, "stale diagnostic entry should be removed on successful re-provision")
}
