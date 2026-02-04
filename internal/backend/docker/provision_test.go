package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
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
	return b
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

	// Override callback backoff so tests run instantly
	origBackoff := callbackBackoff
	callbackBackoff = [callbackMaxAttempts]time.Duration{0, 0, 0}
	defer func() { callbackBackoff = origBackoff }()

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

	origBackoff := callbackBackoff
	callbackBackoff = [callbackMaxAttempts]time.Duration{0, 0, 0}
	defer func() { callbackBackoff = origBackoff }()

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
	_ = b.pool.TryAllocate("lease-1-0", "docker-small")
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
}

func TestProvision_InsufficientResources(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	// Exhaust the pool
	b.cfg.TotalCPUCores = 0.1
	b.cfg.TotalMemoryMB = 1
	b.pool = NewResourcePool(b.cfg.TotalCPUCores, b.cfg.TotalMemoryMB, b.cfg.TotalDiskMB, b.cfg.GetSKUProfile)

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
	b.pool = NewResourcePool(b.cfg.TotalCPUCores, b.cfg.TotalMemoryMB, b.cfg.TotalDiskMB, b.cfg.GetSKUProfile)

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
	origBackoff := callbackBackoff
	callbackBackoff = [callbackMaxAttempts]time.Duration{0, 0, 0}
	defer func() { callbackBackoff = origBackoff }()

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
	b.callbackURLs["lease-1"] = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small")

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

	// Resources should be released
	stats := b.pool.Stats()
	assert.Equal(t, 0, stats.AllocationCount)

	// Callback should report failure
	assert.Equal(t, backend.CallbackStatusFailed, callbackPayload.Status)
	assert.Equal(t, "lease-1", callbackPayload.LeaseUUID)
}

func TestDoProvision_CreateFailure_CleansUpCreated(t *testing.T) {
	origBackoff := callbackBackoff
	callbackBackoff = [callbackMaxAttempts]time.Duration{0, 0, 0}
	defer func() { callbackBackoff = origBackoff }()

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
	b.callbackURLs["lease-1"] = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small")
	_ = b.pool.TryAllocate("lease-1-1", "docker-small")

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
	origBackoff := callbackBackoff
	callbackBackoff = [callbackMaxAttempts]time.Duration{0, 0, 0}
	defer func() { callbackBackoff = origBackoff }()

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
	b.callbackURLs["lease-1"] = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small")

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
	origBackoff := callbackBackoff
	callbackBackoff = [callbackMaxAttempts]time.Duration{0, 0, 0}
	defer func() { callbackBackoff = origBackoff }()

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
	b.callbackURLs["lease-1"] = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small")
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
	origBackoff := callbackBackoff
	callbackBackoff = [callbackMaxAttempts]time.Duration{0, 0, 0}
	defer func() { callbackBackoff = origBackoff }()

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
			return &ContainerInfo{ContainerID: containerID, Status: "exited"}, nil
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
	b.callbackURLs["lease-1"] = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvision(context.Background(), req, manifest, profiles, b.logger)

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
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
		},
	})
	b.callbackURLs["lease-1"] = "http://localhost/callback"
	_ = b.pool.TryAllocate("lease-1-0", "docker-small")
	_ = b.pool.TryAllocate("lease-1-1", "docker-small")

	err := b.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"c1", "c2"}, removedIDs)

	// Provision should be removed
	b.provisionsMu.RLock()
	_, exists := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)

	// Callback URL should be cleaned up
	b.callbackURLsMu.Lock()
	_, hasCallback := b.callbackURLs["lease-1"]
	b.callbackURLsMu.Unlock()
	assert.False(t, hasCallback)

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

	err := b.Deprovision(context.Background(), "lease-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deprovision partially failed")
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
	origBackoff := callbackBackoff
	callbackBackoff = [callbackMaxAttempts]time.Duration{0, 0, 0}
	defer func() { callbackBackoff = origBackoff }()

	var received backend.CallbackPayload
	var receivedSig string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSig = r.Header.Get(hmacauth.SignatureHeader)
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	b.callbackURLs["lease-1"] = server.URL

	b.sendCallback("lease-1", true, "")

	assert.Equal(t, "lease-1", received.LeaseUUID)
	assert.Equal(t, backend.CallbackStatusSuccess, received.Status)
	assert.NotEmpty(t, receivedSig, "HMAC signature header should be set")
}

func TestSendCallback_FailurePayload(t *testing.T) {
	origBackoff := callbackBackoff
	callbackBackoff = [callbackMaxAttempts]time.Duration{0, 0, 0}
	defer func() { callbackBackoff = origBackoff }()

	var received backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	b.httpClient = server.Client()
	b.callbackURLs["lease-1"] = server.URL

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

func TestSendCallback_Retry(t *testing.T) {
	origBackoff := callbackBackoff
	callbackBackoff = [callbackMaxAttempts]time.Duration{0, 0, 0}
	defer func() { callbackBackoff = origBackoff }()

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
	b := newBackendForProvisionTest(t, mock, nil)
	b.httpClient = server.Client()
	b.callbackURLs["lease-1"] = server.URL

	b.sendCallback("lease-1", true, "")

	assert.Equal(t, int32(3), attempts.Load(), "should have retried 3 times")
}

func TestSendCallback_ShutdownAbortsRetry(t *testing.T) {
	origBackoff := callbackBackoff
	callbackBackoff = [callbackMaxAttempts]time.Duration{0, 5 * time.Second, 5 * time.Second}
	defer func() { callbackBackoff = origBackoff }()

	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	b.httpClient = server.Client()
	b.callbackURLs["lease-1"] = server.URL

	// Cancel stopCtx after first attempt
	go func() {
		time.Sleep(50 * time.Millisecond)
		b.stopCancel()
	}()

	b.sendCallback("lease-1", true, "")

	// Should have stopped after 1 attempt due to shutdown
	assert.LessOrEqual(t, attempts.Load(), int32(2))
}

func TestTrySendCallback_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	b.httpClient = server.Client()

	ok := b.trySendCallback("lease-1", server.URL, []byte(`{"test":true}`))
	assert.True(t, ok)
}

func TestTrySendCallback_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	b.httpClient = server.Client()

	ok := b.trySendCallback("lease-1", server.URL, []byte(`{}`))
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
		"lease-1": {LeaseUUID: "lease-1"},
	})
	b.callbackURLs["lease-1"] = "http://localhost"

	b.removeProvision("lease-1")

	b.provisionsMu.RLock()
	_, exists := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)

	b.callbackURLsMu.Lock()
	_, hasCallback := b.callbackURLs["lease-1"]
	b.callbackURLsMu.Unlock()
	assert.False(t, hasCallback)

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
			}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := b.waitForHealthy(ctx, []string{"c1"}, b.logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exited")
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
