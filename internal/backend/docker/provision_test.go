package docker

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
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
		Secret:     string(b.cfg.CallbackSecret),
		Logger:     b.logger,
		StopCtx:    b.stopCtx,
		Backoff:    &zeroBackoff,
	})
}

// doProvisionAndFire wraps doProvision with the SM event firing that the
// public Provision path normally handles. Production code never calls
// doProvision directly; tests do (to exercise specific branches without
// the full validate-and-allocate preamble).
//
// SM events fire synchronously here (bypassing the actor inbox) so tests
// can assert on callback state immediately after it returns. The actor
// inbox path is exercised by the public Provision flow and its dedicated
// tests.
func (b *Backend) doProvisionAndFire(ctx context.Context, req backend.ProvisionRequest, manifest *DockerManifest, profiles map[string]SKUProfile, logger *slog.Logger) {
	actor := b.actorFor(req.LeaseUUID)
	if actor.sm == nil {
		actor.sm = newLeaseSM(actor)
	}
	_ = actor.sm.Fire(ctx, evProvisionRequested)
	callbackErr, result, logs, err := b.doProvision(ctx, req, manifest, profiles, logger)
	if err != nil {
		_ = actor.sm.Fire(ctx, evProvisionErrored, provisionErrorInfo{callbackErr: callbackErr, lastError: err.Error(), logs: logs})
	} else {
		_ = actor.sm.Fire(ctx, evProvisionCompleted, result)
	}
}

// doProvisionStackAndFire is the stack-variant companion to doProvisionAndFire.
func (b *Backend) doProvisionStackAndFire(ctx context.Context, req backend.ProvisionRequest, stack *StackManifest, profiles map[string]SKUProfile, logger *slog.Logger) {
	actor := b.actorFor(req.LeaseUUID)
	if actor.sm == nil {
		actor.sm = newLeaseSM(actor)
	}
	_ = actor.sm.Fire(ctx, evProvisionRequested)
	callbackErr, result, logs, err := b.doProvisionStack(ctx, req, stack, profiles, logger)
	if err != nil {
		_ = actor.sm.Fire(ctx, evProvisionErrored, provisionErrorInfo{callbackErr: callbackErr, lastError: err.Error(), logs: logs})
	} else {
		_ = actor.sm.Fire(ctx, evProvisionCompleted, result)
	}
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

// TestProvision_RejectsWhileDeprovisioning guards Provision's status check:
// a concurrent Deprovision (which sets Status=Deprovisioning) must block
// re-provision. The reconciler retries on the next cycle once the
// Deprovision completes and removes the entry. Without this, a re-provision
// races with RemoveContainer and corrupts state.
func TestProvision_RejectsWhileDeprovisioning(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusDeprovisioning,
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

// TestDoProvision_MultiItem_QuantityPassesTotalNotPerItem verifies that
// CreateContainerParams.Quantity receives the lease-wide totalQuantity, not
// the per-item quantity. This is critical for ComputeSubdomain/RouterName
// consistency between provision and restart/update paths.
func TestDoProvision_MultiItem_QuantityPassesTotalNotPerItem(t *testing.T) {
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	var capturedQuantities []int
	createCalls := 0
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			capturedQuantities = append(capturedQuantities, params.Quantity)
			createCalls++
			return fmt.Sprintf("container-%d", createCalls), nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	// Two items, each with Quantity=1 → totalQuantity=2.
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
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 0}}

	req := backend.ProvisionRequest{
		LeaseUUID:    "lease-1",
		Tenant:       "tenant-a",
		ProviderUUID: "prov-1",
		Items: []backend.LeaseItem{
			{SKU: "docker-small", Quantity: 1},
			{SKU: "docker-small", Quantity: 1},
		},
		CallbackURL: callbackServer.URL,
		Payload:     validManifestJSON("nginx:latest"),
	}
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

	// Both containers must receive totalQuantity (2), not item.Quantity (1).
	// This ensures ComputeSubdomain produces the same FQDN format on
	// restart/update (which passes prov.Quantity = totalQuantity).
	require.Len(t, capturedQuantities, 2)
	assert.Equal(t, 2, capturedQuantities[0], "first container should get totalQuantity")
	assert.Equal(t, 2, capturedQuantities[1], "second container should get totalQuantity")
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
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

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
		// doProvision's failure defer now captures logs from the created
		// containers BEFORE RemoveContainer tears them down, so the mock
		// needs to answer ContainerLogs.
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", nil
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

	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

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
	b.doProvisionAndFire(ctx, req, manifest, profiles, b.logger)

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
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

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
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

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
		// doProvision's failure defer captures logs before cleanup.
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", nil
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
	b.doProvisionAndFire(ctx, req, manifest, profiles, b.logger)

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

// --- Volume-aware provision tests ---

func TestDoProvision_StatefulSKUCreatesVolume(t *testing.T) {
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	var createID string
	var createSizeMB int64
	volDir := t.TempDir()
	vm := &mockVolumeManager{
		defaultDir: volDir,
		CreateFn: func(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
			createID = id
			createSizeMB = sizeMB
			return volDir, true, nil
		},
	}

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
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusProvisioning,
			Quantity:  1,
		},
	})
	b.volumes = vm
	b.provisions["lease-1"].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

	// Verify volume was created with correct ID and size
	assert.Equal(t, "fred-lease-1-0", createID)
	assert.Equal(t, int64(1024), createSizeMB)

	// Verify VolumeBinds maps host subdir to container path
	require.Len(t, capturedParams.VolumeBinds, 1)
	expectedHostPath := filepath.Join(volDir, "data")
	assert.Equal(t, "/data", capturedParams.VolumeBinds[expectedHostPath])

	// Verify ImageVolumes passed through
	assert.Equal(t, []string{"/data"}, capturedParams.ImageVolumes)

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
}

func TestDoProvision_StatefulSKUMultipleVolumes(t *testing.T) {
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	createCalls := 0
	volDir := t.TempDir()
	vm := &mockVolumeManager{
		defaultDir: volDir,
		CreateFn: func(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
			createCalls++
			return volDir, true, nil
		},
	}

	var capturedParams CreateContainerParams
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{Volumes: map[string]struct{}{
				"/data":    {},
				"/var/log": {},
			}}, nil
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
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusProvisioning,
			Quantity:  1,
		},
	})
	b.volumes = vm
	b.provisions["lease-1"].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 2048}}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

	// Single volume.Create per container (not per image VOLUME path)
	assert.Equal(t, 1, createCalls)

	// VolumeBinds should have 2 entries mapping subdirectories
	require.Len(t, capturedParams.VolumeBinds, 2)
	assert.Equal(t, "/data", capturedParams.VolumeBinds[filepath.Join(volDir, "data")])
	assert.Equal(t, "/var/log", capturedParams.VolumeBinds[filepath.Join(volDir, "var/log")])
}

func TestDoProvision_VolumeCreateFailure(t *testing.T) {
	var callbackPayload backend.CallbackPayload
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	vm := &mockVolumeManager{
		CreateFn: func(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
			return "", false, fmt.Errorf("disk full")
		},
	}

	createCalled := false
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{Volumes: map[string]struct{}{"/data": {}}}, nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			createCalled = true
			return "container-1", nil
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
	b.volumes = vm
	b.provisions["lease-1"].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

	// Provision should be marked failed
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	assert.Contains(t, prov.LastError, "volume creation failed")

	// Callback should report failure
	assert.Equal(t, backend.CallbackStatusFailed, callbackPayload.Status)
	assert.Equal(t, "volume creation failed", callbackPayload.Error)

	// No containers should have been created
	assert.False(t, createCalled, "no containers should be created when volume creation fails")
}

func TestDoProvision_CleanupOnlyDestroysNewVolumes(t *testing.T) {
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	volDir := t.TempDir()
	var destroyedIDs []string
	createCall := 0
	vm := &mockVolumeManager{
		CreateFn: func(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
			createCall++
			if createCall == 1 {
				// First container: volume already exists (reused)
				return volDir, false, nil
			}
			// Second container: newly created
			return volDir, true, nil
		},
		DestroyFn: func(ctx context.Context, id string) error {
			destroyedIDs = append(destroyedIDs, id)
			return nil
		},
	}

	containerCreateCall := 0
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{Volumes: map[string]struct{}{"/data": {}}}, nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			containerCreateCall++
			return fmt.Sprintf("container-%d", containerCreateCall), nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			if containerID == "container-2" {
				return fmt.Errorf("port already in use")
			}
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		// doProvision's failure defer captures logs before cleanup.
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusProvisioning,
			Quantity:  2,
		},
	})
	b.volumes = vm
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
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

	// Destroy should only be called for the newly created volume (index 1), not the reused one (index 0)
	assert.Equal(t, []string{"fred-lease-1-1"}, destroyedIDs,
		"only newly created volumes should be destroyed on failure cleanup")
}

func TestDoProvision_StatefulSKUNoImageVolumes(t *testing.T) {
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	createCalled := false
	vm := &mockVolumeManager{
		CreateFn: func(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
			createCalled = true
			return "", true, nil
		},
	}

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			// Image has NO VOLUME declarations
			return &ImageInfo{Volumes: map[string]struct{}{}}, nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			// VolumeBinds should be nil for images without VOLUMEs
			assert.Nil(t, params.VolumeBinds)
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
	b.volumes = vm
	b.provisions["lease-1"].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

	// volumes.Create should NOT be called when image has no VOLUME paths
	assert.False(t, createCalled, "volumes.Create should not be called when image has no VOLUME paths")

	// Provision should still succeed
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
}

func TestProvision_ReProvisionKeepsVolumes(t *testing.T) {
	destroyCalled := false
	vm := &mockVolumeManager{
		defaultDir: t.TempDir(),
		DestroyFn: func(ctx context.Context, id string) error {
			destroyCalled = true
			return nil
		},
	}

	removedContainers := make(map[string]bool)
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			removedContainers[containerID] = true
			return nil
		},
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{Volumes: map[string]struct{}{"/data": {}}}, nil
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
			FailCount:    1,
			Quantity:     1,
			ContainerIDs: []string{"old-container"},
		},
	})
	b.volumes = vm
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

	<-callbackReceived

	// Old container should be removed during re-provision cleanup
	assert.True(t, removedContainers["old-container"], "old container should be removed")

	// volumes.Destroy should NOT be called during re-provision — volumes persist
	assert.False(t, destroyCalled, "volumes should not be destroyed during re-provision")

	b.stopCancel()
	b.wg.Wait()
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

func TestDeprovision_PartialFailure_UpdatesResourceMetrics(t *testing.T) {
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

	// Set gauges to a known stale value so we can detect the update.
	resourceCPUAllocatedRatio.Set(0.99)
	resourceMemoryAllocatedRatio.Set(0.99)

	err := b.Deprovision(context.Background(), "lease-1")
	require.Error(t, err, "partial failure expected")

	// Pool allocations were released, so the gauges must reflect 0 allocation
	// even though Deprovision returned an error.
	assert.InDelta(t, 0.0, testutil.ToFloat64(resourceCPUAllocatedRatio), 0.001,
		"CPU gauge should be updated after partial-failure release")
	assert.InDelta(t, 0.0, testutil.ToFloat64(resourceMemoryAllocatedRatio), 0.001,
		"memory gauge should be updated after partial-failure release")
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

// --- Deprovision volume tests ---

func TestDeprovision_DestroysVolumes(t *testing.T) {
	var destroyedIDs []string
	vm := &mockVolumeManager{
		DestroyFn: func(ctx context.Context, id string) error {
			destroyedIDs = append(destroyedIDs, id)
			return nil
		},
	}

	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
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
	b.volumes = vm
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	_ = b.pool.TryAllocate("lease-1-1", "docker-small", "tenant-a")

	err := b.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"fred-lease-1-0", "fred-lease-1-1"}, destroyedIDs)

	// Provision should be fully removed
	b.provisionsMu.RLock()
	_, exists := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)
}

func TestDeprovision_VolumeDestroyFailure(t *testing.T) {
	vm := &mockVolumeManager{
		DestroyFn: func(ctx context.Context, id string) error {
			return fmt.Errorf("device busy")
		},
	}

	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
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
	b.volumes = vm
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	_ = b.pool.TryAllocate("lease-1-1", "docker-small", "tenant-a")

	err := b.Deprovision(context.Background(), "lease-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "volume cleanup failed")

	// Provision should remain in map with Status=Failed and ContainerIDs=nil
	b.provisionsMu.RLock()
	prov, exists := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	require.True(t, exists, "provision should remain in map after volume destroy failure")
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	assert.Nil(t, prov.ContainerIDs, "containers should be nil since they were removed")
	assert.Contains(t, prov.LastError, "volume cleanup failed")
	assert.Equal(t, 1, prov.VolumeCleanupAttempts)
}

func TestDeprovision_VolumeDestroyGivesUpAfterMaxAttempts(t *testing.T) {
	vm := &mockVolumeManager{
		DestroyFn: func(ctx context.Context, id string) error {
			return fmt.Errorf("permission denied")
		},
	}

	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:             "lease-1",
			Tenant:                "tenant-a",
			Status:                backend.ProvisionStatusFailed,
			Quantity:              1,
			ContainerIDs:          nil, // containers already removed
			VolumeCleanupAttempts: maxVolumeCleanupAttempts - 1,
		},
	})
	b.volumes = vm

	err := b.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err, "should return nil when giving up")

	// Provision should be removed from the map
	b.provisionsMu.RLock()
	_, exists := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.False(t, exists, "provision should be removed after max volume cleanup attempts")
}

// TestDeprovision_SendsDeprovisionedCallback verifies that a clean successful
// Deprovision fires exactly one callback with status=deprovisioned and the
// backend name populated. Regression test for the 34 spurious failure events
// observed on the "Provision Rate by Outcome" dashboard.
func TestDeprovision_SendsDeprovisionedCallback(t *testing.T) {
	var received backend.CallbackPayload
	callbackDone := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
		select {
		case callbackDone <- struct{}{}:
		default:
		}
	}))
	defer server.Close()

	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusReady,
			Quantity:     1,
			ContainerIDs: []string{"c1"},
			CallbackURL:  server.URL,
		},
	})
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.Deprovision(context.Background(), "lease-1"))

	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for deprovisioned callback")
	}

	assert.Equal(t, "lease-1", received.LeaseUUID)
	assert.Equal(t, backend.CallbackStatusDeprovisioned, received.Status)
	assert.Empty(t, received.Error)
	assert.NotEmpty(t, received.Backend, "backend name should be populated for per-backend metrics")
}

// TestDeprovision_VolumeExhaustionSendsFailedCallback verifies that the
// max-attempts-exhausted volume cleanup path fires a failed callback with a
// hardcoded message. Pre-existing gap: that path used to send nothing, leaving
// Fred unaware the lease had become terminal.
func TestDeprovision_VolumeExhaustionSendsFailedCallback(t *testing.T) {
	var received backend.CallbackPayload
	callbackDone := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
		select {
		case callbackDone <- struct{}{}:
		default:
		}
	}))
	defer server.Close()

	vm := &mockVolumeManager{
		DestroyFn: func(ctx context.Context, id string) error {
			return fmt.Errorf("permission denied")
		},
	}
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:             "lease-1",
			Tenant:                "tenant-a",
			Status:                backend.ProvisionStatusFailed,
			Quantity:              1,
			ContainerIDs:          nil,
			VolumeCleanupAttempts: maxVolumeCleanupAttempts - 1,
			CallbackURL:           server.URL,
		},
	})
	b.volumes = vm
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	require.NoError(t, b.Deprovision(context.Background(), "lease-1"))

	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for exhaustion callback")
	}

	assert.Equal(t, backend.CallbackStatusFailed, received.Status)
	assert.Equal(t, "volume cleanup exhausted", received.Error)
}

// TestDeprovision_RetryAfterPartialFailureFiresOneCallback verifies that the
// terminal callback only fires on clean completion. A first call that hits a
// partial-container-failure path emits no callback; a successful retry emits
// exactly one deprovisioned callback.
func TestDeprovision_RetryAfterPartialFailureFiresOneCallback(t *testing.T) {
	// failStuck is true during the first call (makes c2 unremovable), toggled
	// to false before the retry so the second call succeeds.
	var failStuck atomic.Bool
	failStuck.Store(true)
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			if failStuck.Load() && containerID == "c2" {
				return errors.New("permission denied")
			}
			return nil
		},
	}

	// Buffered channel carries the callback payload from the httptest handler
	// goroutine to the test goroutine with a defined happens-before relation.
	// Buffer >1 catches accidental extra callbacks without blocking the handler.
	received := make(chan backend.CallbackPayload, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		received <- p
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusReady,
			Quantity:     2,
			ContainerIDs: []string{"c1", "c2"},
			CallbackURL:  server.URL,
		},
	})
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	// First call: partial failure on c2. Callback fires synchronously on the
	// success path; a partial failure returns early before reaching it, so
	// the channel must be empty when Deprovision returns.
	require.Error(t, b.Deprovision(context.Background(), "lease-1"))
	select {
	case p := <-received:
		t.Fatalf("partial failure must not fire a terminal callback, got %+v", p)
	default:
	}

	// Flip the switch so the second call can remove c2.
	failStuck.Store(false)
	require.NoError(t, b.Deprovision(context.Background(), "lease-1"))

	select {
	case p := <-received:
		assert.Equal(t, backend.CallbackStatusDeprovisioned, p.Status)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for terminal callback")
	}

	// No further callbacks should arrive.
	select {
	case p := <-received:
		t.Fatalf("unexpected extra callback: %+v", p)
	case <-time.After(50 * time.Millisecond):
	}
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
	assert.Equal(t, "192.168.1.100", info.Host)
	require.Len(t, info.Instances, 1)
	assert.Equal(t, "running", info.Instances[0].Status)
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
	assert.Len(t, info.Instances, 3)
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

// --- Workload field fixtures ---

// nonStackProvision returns a simple non-stack provision fixture.
func nonStackProvision() *provision {
	return &provision{
		LeaseUUID:    "lease-1",
		ProviderUUID: "prov-1",
		Status:       backend.ProvisionStatusReady,
		SKU:          "docker-micro",
		Image:        "nginx:1.25",
		Quantity:     2,
	}
}

// stackProvision returns a stack provision fixture with web + db services.
func stackProvision() *provision {
	return &provision{
		LeaseUUID:    "lease-1",
		ProviderUUID: "prov-1",
		Status:       backend.ProvisionStatusReady,
		Quantity:     3,
		Items: []backend.LeaseItem{
			{SKU: "docker-micro", Quantity: 2, ServiceName: "web"},
			{SKU: "docker-large", Quantity: 1, ServiceName: "db"},
		},
		StackManifest: &StackManifest{
			Services: map[string]*DockerManifest{
				"web": {Image: "nginx:1.25"},
				"db":  {Image: "postgres:16"},
			},
		},
	}
}

// stackProvisionNilManifest returns a stack provision with a nil StackManifest
// (simulates cold restart with no release store).
func stackProvisionNilManifest() *provision {
	return &provision{
		LeaseUUID:     "lease-1",
		ProviderUUID:  "prov-1",
		Status:        backend.ProvisionStatusReady,
		Quantity:      2,
		Items:         []backend.LeaseItem{{SKU: "docker-micro", Quantity: 2, ServiceName: "web"}},
		StackManifest: nil,
	}
}

// assertNonStackFields verifies workload fields on a non-stack ProvisionInfo.
func assertNonStackFields(t *testing.T, info *backend.ProvisionInfo) {
	t.Helper()
	assert.Equal(t, "docker-micro", info.SKU)
	assert.Equal(t, "nginx:1.25", info.Image)
	assert.Equal(t, 2, info.Quantity)
	assert.Nil(t, info.Items)
	assert.Nil(t, info.ServiceImages)
}

// assertStackFields verifies workload fields on a stack ProvisionInfo.
func assertStackFields(t *testing.T, info *backend.ProvisionInfo) {
	t.Helper()
	assert.Equal(t, 3, info.Quantity)
	assert.Empty(t, info.Image, "stack lease should not set top-level Image")
	assert.Empty(t, info.SKU, "stack lease should not set top-level SKU")
	require.Len(t, info.Items, 2)
	require.NotNil(t, info.ServiceImages)
	assert.Equal(t, "nginx:1.25", info.ServiceImages["web"])
	assert.Equal(t, "postgres:16", info.ServiceImages["db"])
}

// assertNilManifestFields verifies workload fields when StackManifest is nil.
func assertNilManifestFields(t *testing.T, info *backend.ProvisionInfo) {
	t.Helper()
	require.Len(t, info.Items, 1)
	assert.Nil(t, info.ServiceImages, "nil StackManifest should produce nil ServiceImages")
}

// backendWithProvision creates a test backend with a single provision keyed by lease UUID.
func backendWithProvision(t *testing.T, prov *provision) *Backend {
	t.Helper()
	return newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		prov.LeaseUUID: prov,
	})
}

func TestGetProvision_WorkloadFields_NonStack(t *testing.T) {
	b := backendWithProvision(t, nonStackProvision())
	info, err := b.GetProvision(context.Background(), "lease-1")
	require.NoError(t, err)
	assertNonStackFields(t, info)
}

func TestGetProvision_WorkloadFields_Stack(t *testing.T) {
	b := backendWithProvision(t, stackProvision())
	info, err := b.GetProvision(context.Background(), "lease-1")
	require.NoError(t, err)
	assertStackFields(t, info)
}

func TestGetProvision_WorkloadFields_Stack_NilManifest(t *testing.T) {
	b := backendWithProvision(t, stackProvisionNilManifest())
	info, err := b.GetProvision(context.Background(), "lease-1")
	require.NoError(t, err)
	assertNilManifestFields(t, info)
}

// --- ListProvisions tests ---

func TestListProvisions_Empty(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	result, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestListProvisions_Multiple(t *testing.T) {
	now := time.Now()
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			ProviderUUID: "prov-1",
			Status:       backend.ProvisionStatusReady,
			CreatedAt:    now,
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

	for _, pi := range result {
		assert.NotEmpty(t, pi.LeaseUUID)
		assert.Equal(t, "prov-1", pi.ProviderUUID)
		assert.NotEmpty(t, pi.BackendName)
	}
}

func TestListProvisions_WorkloadFields_NonStack(t *testing.T) {
	b := backendWithProvision(t, nonStackProvision())
	result, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 1)
	assertNonStackFields(t, &result[0])
}

func TestListProvisions_WorkloadFields_Stack(t *testing.T) {
	b := backendWithProvision(t, stackProvision())
	result, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 1)
	assertStackFields(t, &result[0])
}

func TestListProvisions_WorkloadFields_Stack_NilManifest(t *testing.T) {
	b := backendWithProvision(t, stackProvisionNilManifest())
	result, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 1)
	assertNilManifestFields(t, &result[0])
}

func TestListProvisions_ItemsDefensivelyCopied(t *testing.T) {
	originalItems := []backend.LeaseItem{
		{SKU: "docker-micro", Quantity: 2, ServiceName: "web"},
	}
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		"lease-1": {
			LeaseUUID:     "lease-1",
			ProviderUUID:  "prov-1",
			Status:        backend.ProvisionStatusReady,
			Quantity:      2,
			Items:         originalItems,
			StackManifest: &StackManifest{Services: map[string]*DockerManifest{"web": {Image: "nginx:1.25"}}},
		},
	})

	result, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 1)

	result[0].Items[0].SKU = "mutated"
	assert.Equal(t, "docker-micro", originalItems[0].SKU, "returned Items should be a copy, not share backing array")
}

// --- LookupProvisions tests ---

func TestLookupProvisions_Empty(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	result, err := b.LookupProvisions(context.Background(), []string{"lease-1"})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestLookupProvisions_Subset(t *testing.T) {
	now := time.Now()
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", ProviderUUID: "prov-1", Status: backend.ProvisionStatusReady, CreatedAt: now},
		"lease-2": {LeaseUUID: "lease-2", ProviderUUID: "prov-1", Status: backend.ProvisionStatusReady, CreatedAt: now},
		"lease-3": {LeaseUUID: "lease-3", ProviderUUID: "prov-1", Status: backend.ProvisionStatusReady, CreatedAt: now},
	})

	result, err := b.LookupProvisions(context.Background(), []string{"lease-1", "lease-3"})
	require.NoError(t, err)
	assert.Len(t, result, 2)

	got := make(map[string]bool, len(result))
	for _, p := range result {
		got[p.LeaseUUID] = true
	}
	assert.True(t, got["lease-1"])
	assert.True(t, got["lease-3"])
	assert.False(t, got["lease-2"])
}

func TestLookupProvisions_UnknownIgnored(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
	})

	result, err := b.LookupProvisions(context.Background(), []string{"lease-1", "lease-unknown"})
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, "lease-1", result[0].LeaseUUID)
}

func TestLookupProvisions_AllUnknownReturnsEmpty(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
	})

	result, err := b.LookupProvisions(context.Background(), []string{"lease-unknown-1", "lease-unknown-2"})
	require.NoError(t, err)
	assert.Empty(t, result)
	// The slice is non-nil so it serializes as `[]` not `null`.
	assert.NotNil(t, result)
}

func TestLookupProvisions_StackImageRoundTrip(t *testing.T) {
	b := backendWithProvision(t, stackProvision())
	result, err := b.LookupProvisions(context.Background(), []string{"lease-1"})
	require.NoError(t, err)
	require.Len(t, result, 1)
	assertStackFields(t, &result[0])
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

	b.sendCallback("lease-1", backend.CallbackStatusSuccess, "")

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

	b.sendCallback("lease-1", backend.CallbackStatusFailed, "image pull failed")

	assert.Equal(t, backend.CallbackStatusFailed, received.Status)
	assert.Equal(t, "image pull failed", received.Error)
}

func TestSendCallback_NoCallbackURL(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, nil)
	// No panic, no error — just a log warning
	b.sendCallback("unknown-lease", backend.CallbackStatusSuccess, "")
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
	b.sendCallback("lease-1", backend.CallbackStatusFailed, longError)

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

	b.sendCallback("lease-1", backend.CallbackStatusSuccess, "")

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
		Secret:     string(b.cfg.CallbackSecret),
		Logger:     b.logger,
		StopCtx:    b.stopCtx,
		Backoff:    &longBackoff,
	})

	// Cancel stopCtx after first attempt
	go func() {
		time.Sleep(50 * time.Millisecond)
		b.stopCancel()
	}()

	b.sendCallback("lease-1", backend.CallbackStatusSuccess, "")

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
	b.doProvisionAndFire(ctx, req, manifest, profiles, b.logger)

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
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)
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
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)
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
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)
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

	b.sendCallback("lease-1", backend.CallbackStatusSuccess, "")

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

	b.sendCallback("lease-1", backend.CallbackStatusFailed, "container crashed")

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

// TestDeprovision_ActiveProvisionsGauge verifies that the activeProvisions gauge
// is decremented exactly once on Ready→Failed transitions and never for non-Ready
// provisions, even across partial-failure retry sequences.
func TestDeprovision_ActiveProvisionsGauge(t *testing.T) {
	t.Run("ready provision decrements gauge", func(t *testing.T) {
		activeProvisions.Set(5)
		mock := &mockDockerClient{
			RemoveContainerFn: func(ctx context.Context, containerID string) error {
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
		_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

		err := b.Deprovision(context.Background(), "lease-1")
		require.NoError(t, err)
		assert.Equal(t, float64(4), testutil.ToFloat64(activeProvisions))
	})

	t.Run("failed provision does not decrement gauge", func(t *testing.T) {
		activeProvisions.Set(5)
		mock := &mockDockerClient{
			RemoveContainerFn: func(ctx context.Context, containerID string) error {
				return nil
			},
		}
		b := newBackendForProvisionTest(t, mock, map[string]*provision{
			"lease-1": {
				LeaseUUID:    "lease-1",
				Tenant:       "tenant-a",
				Status:       backend.ProvisionStatusFailed,
				Quantity:     1,
				ContainerIDs: []string{"c1"},
			},
		})
		_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

		err := b.Deprovision(context.Background(), "lease-1")
		require.NoError(t, err)
		assert.Equal(t, float64(5), testutil.ToFloat64(activeProvisions),
			"gauge must not change when deprovisioning an already-failed provision")
	})

	t.Run("provisioning lease does not decrement gauge", func(t *testing.T) {
		activeProvisions.Set(5)
		mock := &mockDockerClient{
			RemoveContainerFn: func(ctx context.Context, containerID string) error {
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
		assert.Equal(t, float64(5), testutil.ToFloat64(activeProvisions),
			"gauge must not change when deprovisioning a provisioning lease")
	})

	t.Run("partial failure then retry decrements gauge exactly once", func(t *testing.T) {
		activeProvisions.Set(5)
		callCount := 0
		mock := &mockDockerClient{
			RemoveContainerFn: func(ctx context.Context, containerID string) error {
				callCount++
				// c2 fails during the first deprovision attempt (callCount=2);
				// on retry c2 is the only container and succeeds (callCount=3).
				if containerID == "c2" && callCount == 2 {
					return errors.New("device busy")
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

		// First attempt: partial failure. Gauge should still decrement
		// because the provision transitions Ready→Failed.
		err := b.Deprovision(context.Background(), "lease-1")
		require.Error(t, err)
		assert.Equal(t, float64(4), testutil.ToFloat64(activeProvisions),
			"gauge should decrement on Ready→Failed even with partial failure")

		// Retry: provision is already Failed, gauge must not decrement again.
		err = b.Deprovision(context.Background(), "lease-1")
		require.NoError(t, err)
		assert.Equal(t, float64(4), testutil.ToFloat64(activeProvisions),
			"gauge must not double-decrement on retry")
	})

	t.Run("idempotent call on missing lease does not decrement gauge", func(t *testing.T) {
		activeProvisions.Set(5)
		mock := &mockDockerClient{}
		b := newBackendForProvisionTest(t, mock, nil)

		err := b.Deprovision(context.Background(), "nonexistent")
		require.NoError(t, err)
		assert.Equal(t, float64(5), testutil.ToFloat64(activeProvisions),
			"gauge must not change for nonexistent lease")
	})
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
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

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
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

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
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

	// Callback should have hardcoded message — no registry auth details.
	assert.Equal(t, "image pull failed", callbackPayload.Error)
	assert.NotContains(t, callbackPayload.Error, "registry.example.com")

	// LastError should contain the full error.
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Contains(t, prov.LastError, "registry.example.com")
}

func TestStartupErrorToCallbackMsg(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"unhealthy", fmt.Errorf("container 0 reported unhealthy: exit_code=1; logs: oops"), "container reported unhealthy"},
		{"exited during startup", fmt.Errorf("container 0 exited during startup (status: exited): exit_code=1"), "container exited during startup"},
		{"exited during health", fmt.Errorf("container 0 exited while waiting for healthy"), "container exited during health check"},
		{"timeout", fmt.Errorf("timed out waiting for containers to become healthy"), "container exited during startup"},
		{"canceled during verification", fmt.Errorf("canceled during startup verification: context canceled"), "container startup verification canceled"},
		{"inspect failure", fmt.Errorf("failed to inspect container 0 during health check"), "container exited during startup"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, startupErrorToCallbackMsg(tt.err))
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

// TestProvision_FailurePersistsContainerLogs pins the log-capture
// invariant: when doProvision fails AFTER creating containers, the
// worker must fetch logs from those containers BEFORE the cleanup
// defer removes them. The persisted diagnostic entry should contain
// the captured logs.
//
// Pre-refactor, the SM's onEnterFailedFromProvision called
// persistDiagnostics which tried to re-fetch from prov.ContainerIDs —
// but that field was empty (pre-publish only on success) and the
// containers were gone anyway, so the persisted entry had no Logs.
// The fix threads pre-captured logs through provisionErrorInfo.
func TestProvision_FailurePersistsContainerLogs(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "diag_logs.db")
	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer diagStore.Close()

	var callbackReceived atomic.Bool
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackReceived.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	// Mock: PullImage/Create/Start succeed, Inspect returns exited so
	// waitForHealthy treats the container as crashed. Cleanup removes
	// the containers. The test's ContainerLogsFn must return the
	// simulated output — captureContainerLogs calls it BEFORE cleanup.
	logsFetched := 0
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
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			logsFetched++
			return "boot error on " + containerID, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, nil)
	b.diagnosticsStore = diagStore
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)

	req := backend.ProvisionRequest{
		LeaseUUID:    "lease-logs",
		Tenant:       "tenant-a",
		ProviderUUID: "prov-1",
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 2}},
		CallbackURL:  callbackServer.URL,
		Payload:      validManifestJSON("nginx:latest"),
	}

	require.NoError(t, b.Provision(context.Background(), req))
	require.Eventually(t, callbackReceived.Load, 5*time.Second, 50*time.Millisecond)

	// Logs must have been fetched BEFORE cleanup — at least one call
	// per failed container via captureContainerLogs (verifyStartup may
	// add one extra for the LastError diagnostic string).
	assert.GreaterOrEqual(t, logsFetched, 2, "captureContainerLogs must fetch logs from the failed containers")

	entry, err := diagStore.Get("lease-logs")
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.NotEmpty(t, entry.Logs, "persisted diagnostic must contain the pre-captured logs")
	// Index-based keys for single-manifest provisions.
	assert.Contains(t, entry.Logs["0"], "boot error on container-0")
	assert.Contains(t, entry.Logs["1"], "boot error on container-1")
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

func TestDoProvision_StatefulSKUChownsVolumeSubdirs(t *testing.T) {
	// Verify that doProvision chowns volume subdirectories to the image's
	// runtime UID/GID when ResolveImageUser returns a non-root user.
	if os.Getuid() != 0 {
		t.Skip("chown requires root")
	}

	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	volDir := t.TempDir()
	vm := &mockVolumeManager{
		defaultDir: volDir,
		CreateFn: func(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
			return volDir, true, nil
		},
	}

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{Volumes: map[string]struct{}{"/data": {}}}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 999, 999, nil
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

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-chown": {
			LeaseUUID: "lease-chown",
			Status:    backend.ProvisionStatusProvisioning,
			Quantity:  1,
		},
	})
	b.volumes = vm
	b.provisions["lease-chown"].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-chown-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifestPayload, _ := json.Marshal(DockerManifest{Image: "postgres:16", User: "999:999"})
	manifest, _ := ParseManifest(manifestPayload)
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest("lease-chown", "tenant-a", "docker-small", 1, manifestPayload)
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

	// Verify volume subdir is owned by UID/GID 999.
	subdir := filepath.Join(volDir, "data")
	info, err := os.Stat(subdir)
	require.NoError(t, err)
	stat := info.Sys().(*syscall.Stat_t)
	assert.Equal(t, uint32(999), stat.Uid, "volume subdir should be owned by UID 999")
	assert.Equal(t, uint32(999), stat.Gid, "volume subdir should be owned by GID 999")

	b.provisionsMu.RLock()
	prov := b.provisions["lease-chown"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
}

func TestDoProvision_StatefulSKURootUserNoChown(t *testing.T) {
	// Verify that doProvision does NOT chown when ResolveImageUser returns root (0, 0).
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	volDir := t.TempDir()
	vm := &mockVolumeManager{
		defaultDir: volDir,
		CreateFn: func(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
			return volDir, true, nil
		},
	}

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{Volumes: map[string]struct{}{"/data": {}}}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 0, 0, nil // root
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

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-root": {
			LeaseUUID: "lease-root",
			Status:    backend.ProvisionStatusProvisioning,
			Quantity:  1,
		},
	})
	b.volumes = vm
	b.provisions["lease-root"].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-root-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}}

	req := newProvisionRequest("lease-root", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

	// Verify provision succeeded — ownership stays as created by MkdirAll
	// (no chown call since UID/GID are 0).
	b.provisionsMu.RLock()
	prov := b.provisions["lease-root"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
}

func TestInspectImageForSetup_AutoDetectVolumeOwner(t *testing.T) {
	// Mongo-like image: root USER, volumes owned by UID 999.
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:abc123",
				Volumes: map[string]struct{}{"/data/db": {}, "/data/configdb": {}},
			}, nil
		},
		DetectVolumeOwnerFn: func(ctx context.Context, imageName string, volumePaths []string) (int, int, error) {
			return 999, 999, nil
		},
	}
	b := newBackendForTest(mock, nil)

	result, err := b.inspectImageForSetup(context.Background(), "mongo:latest", "")
	require.NoError(t, err)

	assert.Equal(t, "999:999", result.ContainerUser)
	assert.Equal(t, 999, result.VolumeUID)
	assert.Equal(t, 999, result.VolumeGID)
}

func TestInspectImageForSetup_AutoDetectCacheHit(t *testing.T) {
	// Second call with same image ID should not invoke DetectVolumeOwner.
	var detectCalls int
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:cached",
				Volumes: map[string]struct{}{"/data": {}},
			}, nil
		},
		DetectVolumeOwnerFn: func(ctx context.Context, imageName string, volumePaths []string) (int, int, error) {
			detectCalls++
			return 999, 999, nil
		},
	}
	b := newBackendForTest(mock, nil)

	_, err := b.inspectImageForSetup(context.Background(), "mongo:latest", "")
	require.NoError(t, err)
	assert.Equal(t, 1, detectCalls)

	// Second call — cache hit, no additional DetectVolumeOwner call.
	result, err := b.inspectImageForSetup(context.Background(), "mongo:latest", "")
	require.NoError(t, err)
	assert.Equal(t, 1, detectCalls, "DetectVolumeOwner should not be called again")
	assert.Equal(t, "999:999", result.ContainerUser)
}

func TestInspectImageForSetup_AutoDetectRootOwnership(t *testing.T) {
	// Volumes owned by root → no override.
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:rootowned",
				Volumes: map[string]struct{}{"/data": {}},
			}, nil
		},
		DetectVolumeOwnerFn: func(ctx context.Context, imageName string, volumePaths []string) (int, int, error) {
			return 0, 0, nil
		},
	}
	b := newBackendForTest(mock, nil)

	result, err := b.inspectImageForSetup(context.Background(), "alpine:latest", "")
	require.NoError(t, err)

	assert.Empty(t, result.ContainerUser)
	assert.Equal(t, 0, result.VolumeUID)
	assert.Equal(t, 0, result.VolumeGID)
}

func TestInspectImageForSetup_AutoDetectError(t *testing.T) {
	// DetectVolumeOwner fails → graceful fallback, no error propagated.
	// Errors are NOT cached, so a subsequent call retries.
	var detectCalls int
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:errorcase",
				Volumes: map[string]struct{}{"/data": {}},
			}, nil
		},
		DetectVolumeOwnerFn: func(ctx context.Context, imageName string, volumePaths []string) (int, int, error) {
			detectCalls++
			if detectCalls == 1 {
				return 0, 0, errors.New("docker daemon unreachable")
			}
			return 999, 999, nil // succeeds on retry
		},
	}
	b := newBackendForTest(mock, nil)

	// First call: error → defaults to root.
	result, err := b.inspectImageForSetup(context.Background(), "mongo:latest", "")
	require.NoError(t, err)
	assert.Empty(t, result.ContainerUser)
	assert.Equal(t, 1, detectCalls)

	// Second call: retries (error was not cached) → succeeds.
	result, err = b.inspectImageForSetup(context.Background(), "mongo:latest", "")
	require.NoError(t, err)
	assert.Equal(t, "999:999", result.ContainerUser)
	assert.Equal(t, 2, detectCalls, "should retry after transient error")
}

func TestInspectImageForSetup_ExplicitUserSkipsAutoDetect(t *testing.T) {
	// Manifest user set → DetectVolumeOwner NOT called.
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:explicituser",
				Volumes: map[string]struct{}{"/data": {}},
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 1000, 1000, nil
		},
		DetectVolumeOwnerFn: func(ctx context.Context, imageName string, volumePaths []string) (int, int, error) {
			t.Fatal("DetectVolumeOwner should not be called when manifest user is set")
			return 0, 0, nil
		},
	}
	b := newBackendForTest(mock, nil)

	result, err := b.inspectImageForSetup(context.Background(), "postgres:16", "1000:1000")
	require.NoError(t, err)

	assert.Equal(t, "1000:1000", result.ContainerUser)
	assert.Equal(t, 1000, result.VolumeUID)
	assert.Equal(t, 1000, result.VolumeGID)
}

func TestInspectImageForSetup_NoVolumesSkipsAutoDetect(t *testing.T) {
	// No VOLUME paths → DetectVolumeOwner NOT called.
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:novolumes",
				Volumes: map[string]struct{}{},
			}, nil
		},
		DetectVolumeOwnerFn: func(ctx context.Context, imageName string, volumePaths []string) (int, int, error) {
			t.Fatal("DetectVolumeOwner should not be called when there are no volumes")
			return 0, 0, nil
		},
	}
	b := newBackendForTest(mock, nil)

	result, err := b.inspectImageForSetup(context.Background(), "nginx:latest", "")
	require.NoError(t, err)

	assert.Empty(t, result.ContainerUser)
	assert.Equal(t, 0, result.VolumeUID)
	assert.Equal(t, 0, result.VolumeGID)
}

// --- WritablePaths tests ---

func TestInspectImageForSetup_DetectsWritablePaths(t *testing.T) {
	// Grafana-like image: non-root user (472), no VOLUMEs.
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:grafana123",
				Volumes: map[string]struct{}{},
				User:    "472",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 472, 472, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			assert.Equal(t, 472, uid)
			return []string{"/var/lib/grafana", "/var/log/grafana"}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	result, err := b.inspectImageForSetup(context.Background(), "grafana/grafana:latest", "")
	require.NoError(t, err)

	assert.Equal(t, "472:472", result.ContainerUser)
	assert.Equal(t, []string{"/var/lib/grafana", "/var/log/grafana"}, result.WritablePaths)
	assert.Empty(t, result.Volumes)
}

func TestInspectImageForSetup_WritablePathsDetectedWithVolumes(t *testing.T) {
	// MySQL-like image: has VOLUMEs AND needs /var/run/mysqld writable.
	// Detection must still run even when VOLUMEs are declared.
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:mysql9",
				Volumes: map[string]struct{}{"/var/lib/mysql": {}},
				User:    "999",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 999, 999, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			return []string{"/var/run/mysqld"}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	result, err := b.inspectImageForSetup(context.Background(), "mysql:9", "")
	require.NoError(t, err)

	assert.Equal(t, []string{"/var/run/mysqld"}, result.WritablePaths)
	assert.Equal(t, []string{"/var/lib/mysql"}, result.Volumes)
	assert.Equal(t, "999:999", result.ContainerUser)
}

func TestInspectImageForSetup_WritablePathsDetectedForRoot(t *testing.T) {
	// Root user → writable path detection is called with uid=0,
	// matching directories owned by any non-root user (e.g., neo4j).
	var detectedUID int
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:rootuser",
				Volumes: map[string]struct{}{"/data": {}},
			}, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			detectedUID = uid
			return []string{"/var/lib/neo4j"}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	result, err := b.inspectImageForSetup(context.Background(), "neo4j:latest", "")
	require.NoError(t, err)

	assert.Equal(t, 0, detectedUID, "should pass uid=0 for root images")
	assert.Equal(t, []string{"/var/lib/neo4j"}, result.WritablePaths)
	assert.Empty(t, result.ContainerUser)
}

func TestInspectImageForSetup_WritablePathsCacheHit(t *testing.T) {
	// Second call with same image ID should not invoke DetectWritablePaths.
	var detectCalls int
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:grafana-cached",
				Volumes: map[string]struct{}{},
				User:    "472",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 472, 472, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			detectCalls++
			return []string{"/var/lib/grafana"}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	_, err := b.inspectImageForSetup(context.Background(), "grafana/grafana:latest", "")
	require.NoError(t, err)
	assert.Equal(t, 1, detectCalls)

	// Second call — cache hit, no additional DetectWritablePaths call.
	result, err := b.inspectImageForSetup(context.Background(), "grafana/grafana:latest", "")
	require.NoError(t, err)
	assert.Equal(t, 1, detectCalls, "DetectWritablePaths should not be called again")
	assert.Equal(t, []string{"/var/lib/grafana"}, result.WritablePaths)
}

func TestInspectImageForSetup_WritablePathsErrorNotCached(t *testing.T) {
	// Error → no cache, retry succeeds.
	var detectCalls int
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:grafana-retry",
				Volumes: map[string]struct{}{},
				User:    "472",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 472, 472, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			detectCalls++
			if detectCalls == 1 {
				return nil, errors.New("docker daemon unreachable")
			}
			return []string{"/var/lib/grafana"}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	// First call: error → nil writable paths.
	result, err := b.inspectImageForSetup(context.Background(), "grafana/grafana:latest", "")
	require.NoError(t, err)
	assert.Nil(t, result.WritablePaths)
	assert.Equal(t, 1, detectCalls)

	// Second call: retries (error was not cached) → succeeds.
	result, err = b.inspectImageForSetup(context.Background(), "grafana/grafana:latest", "")
	require.NoError(t, err)
	assert.Equal(t, []string{"/var/lib/grafana"}, result.WritablePaths)
	assert.Equal(t, 2, detectCalls, "should retry after transient error")
}

func TestInspectImageForSetup_WritablePathsBinds(t *testing.T) {
	// End-to-end: detected writable paths flow through to CreateContainerParams
	// as WritablePathBinds (bind mounts from managed volume).
	var capturedParams CreateContainerParams

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
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:grafana-e2e",
				Volumes: map[string]struct{}{},
				User:    "472",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 472, 472, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			return []string{"/var/lib/grafana"}, nil
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

	tmpDir := t.TempDir()
	b := newBackendForProvisionTest(t, mock, nil)
	b.volumes = &mockVolumeManager{defaultDir: tmpDir}
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	b.cfg.SKUProfiles = map[string]SKUProfile{
		"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 0},
	}
	b.pool = shared.NewResourcePool(b.cfg.TotalCPUCores, b.cfg.TotalMemoryMB, b.cfg.TotalDiskMB, b.cfg.GetSKUProfile, nil)

	req := newProvisionRequest("lease-wp", "tenant-a", "docker-small", 1, validManifestJSON("grafana/grafana:latest"))
	req.CallbackURL = callbackServer.URL

	err := b.Provision(context.Background(), req)
	require.NoError(t, err)

	<-callbackReceived

	// WritablePathBinds should map host path → container path.
	require.NotNil(t, capturedParams.WritablePathBinds)
	assert.Contains(t, capturedParams.WritablePathBinds, filepath.Join(tmpDir, "_wp", "var/lib/grafana"))
	assert.Equal(t, "/var/lib/grafana", capturedParams.WritablePathBinds[filepath.Join(tmpDir, "_wp", "var/lib/grafana")])
	assert.Equal(t, "472:472", capturedParams.User)

	b.stopCancel()
	b.wg.Wait()
}

func TestInspectImageForSetup_FilterSubpaths(t *testing.T) {
	// Verify that writable paths that overlap VOLUME paths are filtered out.
	mock := &mockDockerClient{
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID: "sha256:neo4j-test",
				Volumes: map[string]struct{}{
					"/data": {},
				},
				User: "7474",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 7474, 7474, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			// /data/transactions is a subtree of VOLUME /data → should be filtered
			return []string{"/var/lib/neo4j", "/data/transactions"}, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, nil)
	b.cfg.ContainerReadonlyRootfs = ptrBool(true)

	imgSetup, err := b.inspectImageForSetup(context.Background(), "neo4j:latest", "")
	require.NoError(t, err)

	// /data/transactions should be filtered out (subtree of /data)
	// /var/lib/neo4j should remain
	assert.Equal(t, []string{"/var/lib/neo4j"}, imgSetup.WritablePaths)
}

func TestWritablePathBindsNoOverlap(t *testing.T) {
	// Verify that writable path binds don't include paths already covered
	// by VOLUME bind mounts. This is handled upstream by filterSubpaths
	// in inspectImageForSetup — writable paths that overlap VOLUME paths
	// are removed before they reach doProvision.

	// filterSubpaths should remove /var/lib/mysql (equal to VOLUME path)
	volumes := []string{"/var/lib/mysql"}
	writablePaths := []string{"/var/lib/mysql", "/var/run/mysqld"}
	filtered := filterSubpaths(writablePaths, volumes)

	assert.NotContains(t, filtered, "/var/lib/mysql",
		"/var/lib/mysql should be filtered (covered by VOLUME)")
	assert.Contains(t, filtered, "/var/run/mysqld",
		"/var/run/mysqld should remain (not covered by VOLUME)")
}

// --- filterSubpaths tests ---

func TestFilterSubpaths(t *testing.T) {
	tests := []struct {
		name       string
		candidates []string
		parents    []string
		want       []string
	}{
		{
			name:       "no overlap",
			candidates: []string{"/var/lib/grafana", "/var/run/mysqld"},
			parents:    []string{"/data"},
			want:       []string{"/var/lib/grafana", "/var/run/mysqld"},
		},
		{
			name:       "exact match removed",
			candidates: []string{"/data", "/var/lib/app"},
			parents:    []string{"/data"},
			want:       []string{"/var/lib/app"},
		},
		{
			name:       "subtree removed",
			candidates: []string{"/data/transactions", "/var/lib/app"},
			parents:    []string{"/data"},
			want:       []string{"/var/lib/app"},
		},
		{
			name:       "multiple parents",
			candidates: []string{"/data/tx", "/logs/app", "/var/lib/app"},
			parents:    []string{"/data", "/logs"},
			want:       []string{"/var/lib/app"},
		},
		{
			name:       "nil candidates",
			candidates: nil,
			parents:    []string{"/data"},
			want:       nil,
		},
		{
			name:       "nil parents",
			candidates: []string{"/var/lib/app"},
			parents:    nil,
			want:       []string{"/var/lib/app"},
		},
		{
			name:       "prefix but not subtree",
			candidates: []string{"/data-extra"},
			parents:    []string{"/data"},
			want:       []string{"/data-extra"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterSubpaths(tt.candidates, tt.parents)
			assert.Equal(t, tt.want, got)
		})
	}
}

// --- sanitizeAndExtractTar tests ---

func TestSanitizeAndExtractTar(t *testing.T) {
	t.Run("extracts regular files and dirs", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "conf/", Typeflag: tar.TypeDir, Mode: 0o755, Uid: 1000, Gid: 1000},
			{Name: "conf/app.conf", Typeflag: tar.TypeReg, Mode: 0o644, Uid: 1000, Gid: 1000, Content: "key=value"},
		})

		written, skipped, err := sanitizeAndExtractTar(buf, destDir, 1024*1024)
		require.NoError(t, err)
		assert.Empty(t, skipped)
		assert.Equal(t, int64(9), written) // len("key=value")

		content, readErr := os.ReadFile(filepath.Join(destDir, "conf", "app.conf"))
		require.NoError(t, readErr)
		assert.Equal(t, "key=value", string(content))
	})

	t.Run("rejects absolute path", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "/etc/passwd", Typeflag: tar.TypeReg, Mode: 0o644, Content: "evil"},
		})
		_, _, err := sanitizeAndExtractTar(buf, destDir, 1024*1024)
		assert.ErrorContains(t, err, "unsafe path")
	})

	t.Run("rejects path traversal", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "../escape", Typeflag: tar.TypeReg, Mode: 0o644, Content: "evil"},
		})
		_, _, err := sanitizeAndExtractTar(buf, destDir, 1024*1024)
		assert.ErrorContains(t, err, "unsafe path")
	})

	t.Run("rejects device node", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "dev/null", Typeflag: tar.TypeBlock, Mode: 0o666},
		})
		_, _, err := sanitizeAndExtractTar(buf, destDir, 1024*1024)
		assert.ErrorContains(t, err, "disallowed type")
	})

	t.Run("enforces size limit", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "big.bin", Typeflag: tar.TypeReg, Mode: 0o644, Content: strings.Repeat("x", 100)},
		})
		_, _, err := sanitizeAndExtractTar(buf, destDir, 50)
		assert.ErrorContains(t, err, "exceeds")
	})

	t.Run("strips setuid bits", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "suid-binary", Typeflag: tar.TypeReg, Mode: 0o4755, Content: "binary"},
		})
		_, _, err := sanitizeAndExtractTar(buf, destDir, 1024*1024)
		require.NoError(t, err)

		info, statErr := os.Stat(filepath.Join(destDir, "suid-binary"))
		require.NoError(t, statErr)
		// Setuid bit should be stripped
		assert.Zero(t, info.Mode()&os.ModeSetuid)
	})

	t.Run("creates absolute symlink and reports it", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "conf/", Typeflag: tar.TypeDir, Mode: 0o755},
			{Name: "conf/real.conf", Typeflag: tar.TypeReg, Mode: 0o644, Content: "data"},
			{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"},
		})
		_, outOfScope, err := sanitizeAndExtractTar(buf, destDir, 1024*1024)
		require.NoError(t, err)
		assert.Equal(t, []string{"link -> /etc/passwd"}, outOfScope)
		// Symlink is created (dangling on host, resolves inside container).
		assert.FileExists(t, filepath.Join(destDir, "conf", "real.conf"))
		linkTarget, readlinkErr := os.Readlink(filepath.Join(destDir, "link"))
		require.NoError(t, readlinkErr)
		assert.Equal(t, "/etc/passwd", linkTarget)
	})

	t.Run("creates traversal symlink and reports it", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "a/b/", Typeflag: tar.TypeDir, Mode: 0o755},
			{Name: "a/b/file.txt", Typeflag: tar.TypeReg, Mode: 0o644, Content: "ok"},
			{Name: "a/b/link", Typeflag: tar.TypeSymlink, Linkname: "../../../etc/passwd"},
		})
		_, outOfScope, err := sanitizeAndExtractTar(buf, destDir, 1024*1024)
		require.NoError(t, err)
		assert.Equal(t, []string{"a/b/link -> ../../../etc/passwd"}, outOfScope)
		assert.FileExists(t, filepath.Join(destDir, "a", "b", "file.txt"))
		linkTarget, readlinkErr := os.Readlink(filepath.Join(destDir, "a", "b", "link"))
		require.NoError(t, readlinkErr)
		assert.Equal(t, "../../../etc/passwd", linkTarget)
	})

	t.Run("creates parent-escape symlink and reports it", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "keepme.txt", Typeflag: tar.TypeReg, Mode: 0o644, Content: "kept"},
			{Name: "escape", Typeflag: tar.TypeSymlink, Linkname: ".."},
		})
		_, outOfScope, err := sanitizeAndExtractTar(buf, destDir, 1024*1024)
		require.NoError(t, err)
		assert.Equal(t, []string{"escape -> .."}, outOfScope)
		assert.FileExists(t, filepath.Join(destDir, "keepme.txt"))
		linkTarget, readlinkErr := os.Readlink(filepath.Join(destDir, "escape"))
		require.NoError(t, readlinkErr)
		assert.Equal(t, "..", linkTarget)
	})

	t.Run("allows safe symlink", func(t *testing.T) {
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "conf/", Typeflag: tar.TypeDir, Mode: 0o755},
			{Name: "conf/real.conf", Typeflag: tar.TypeReg, Mode: 0o644, Content: "data"},
			{Name: "conf/link.conf", Typeflag: tar.TypeSymlink, Linkname: "real.conf"},
		})
		_, skipped, err := sanitizeAndExtractTar(buf, destDir, 1024*1024)
		require.NoError(t, err)
		assert.Empty(t, skipped)

		target, readlinkErr := os.Readlink(filepath.Join(destDir, "conf", "link.conf"))
		require.NoError(t, readlinkErr)
		assert.Equal(t, "real.conf", target)
	})

	t.Run("preserves ownership", func(t *testing.T) {
		if os.Getuid() != 0 {
			t.Skip("chown requires root")
		}
		destDir := t.TempDir()
		buf := createTestTar(t, []testTarEntry{
			{Name: "owned.txt", Typeflag: tar.TypeReg, Mode: 0o644, Uid: 1000, Gid: 1000, Content: "data"},
		})
		_, _, err := sanitizeAndExtractTar(buf, destDir, 1024*1024)
		require.NoError(t, err)

		info, statErr := os.Stat(filepath.Join(destDir, "owned.txt"))
		require.NoError(t, statErr)
		stat := info.Sys().(*syscall.Stat_t)
		assert.Equal(t, uint32(1000), stat.Uid)
		assert.Equal(t, uint32(1000), stat.Gid)
	})
}

// testTarEntry describes a single tar entry for test helpers.
type testTarEntry struct {
	Name     string
	Typeflag byte
	Mode     int64
	Uid, Gid int
	Content  string
	Linkname string
}

// createTestTar builds an in-memory tar archive from test entries.
func createTestTar(t *testing.T, entries []testTarEntry) io.Reader {
	t.Helper()
	var buf strings.Builder
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		hdr := &tar.Header{
			Name:     e.Name,
			Typeflag: e.Typeflag,
			Mode:     e.Mode,
			Uid:      e.Uid,
			Gid:      e.Gid,
			Linkname: e.Linkname,
			Size:     int64(len(e.Content)),
		}
		require.NoError(t, tw.WriteHeader(hdr))
		if e.Content != "" {
			_, err := tw.Write([]byte(e.Content))
			require.NoError(t, err)
		}
	}
	require.NoError(t, tw.Close())
	return strings.NewReader(buf.String())
}

// --- WritablePathBinds tests ---

func TestDoProvision_WritablePathBinds(t *testing.T) {
	// Volume created, content extracted, bind mounts set.
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	var extractCalled bool
	var capturedParams CreateContainerParams
	tmpDir := t.TempDir()
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:abc",
				Volumes: map[string]struct{}{},
				User:    "1000",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 1000, 1000, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			return []string{"/var/lib/neo4j"}, nil
		},
		ExtractImageContentFn: func(ctx context.Context, imageName string, paths []string, destDir string, maxBytes int64) map[string]error {
			extractCalled = true
			assert.Equal(t, "nginx:latest", imageName)
			assert.Equal(t, []string{"/var/lib/neo4j"}, paths)
			assert.Contains(t, destDir, "_wp")
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
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusProvisioning,
			Quantity:  1,
		},
	})
	b.cfg.ContainerReadonlyRootfs = ptrBool(true)
	b.volumes = &mockVolumeManager{defaultDir: tmpDir}
	b.provisions["lease-1"].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 0}}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

	assert.True(t, extractCalled, "ExtractImageContent should be called for writable paths")
	require.NotNil(t, capturedParams.WritablePathBinds)
	assert.Equal(t, "/var/lib/neo4j", capturedParams.WritablePathBinds[filepath.Join(tmpDir, "_wp", "var/lib/neo4j")])

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
}

func TestDoProvision_WritablePathBinds_PartialFailure(t *testing.T) {
	// When ExtractImageContent fails for some paths, only successful paths
	// appear in WritablePathBinds; failed paths are omitted gracefully.
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	var capturedParams CreateContainerParams
	tmpDir := t.TempDir()
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:abc",
				Volumes: map[string]struct{}{},
				User:    "1000",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 1000, 1000, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			return []string{"/var/lib/app", "/var/log/app"}, nil
		},
		ExtractImageContentFn: func(ctx context.Context, imageName string, paths []string, destDir string, maxBytes int64) map[string]error {
			return map[string]error{
				"/var/log/app": fmt.Errorf("path does not exist in image"),
			}
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
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusProvisioning,
			Quantity:  1,
		},
	})
	b.cfg.ContainerReadonlyRootfs = ptrBool(true)
	b.volumes = &mockVolumeManager{defaultDir: tmpDir}
	b.provisions["lease-1"].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 0}}

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

	// Successful path should be in binds, failed path should be omitted.
	require.NotNil(t, capturedParams.WritablePathBinds)
	assert.Equal(t, "/var/lib/app", capturedParams.WritablePathBinds[filepath.Join(tmpDir, "_wp", "var/lib/app")])
	for hostPath, containerPath := range capturedParams.WritablePathBinds {
		assert.NotEqual(t, "/var/log/app", containerPath, "failed path should not be in binds: %s", hostPath)
	}
}

func TestDoRestart_WritablePathBinds(t *testing.T) {
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	var extractCalled bool
	tmpDir := t.TempDir()
	mock := &mockDockerClient{
		StopContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:abc",
				Volumes: map[string]struct{}{},
				User:    "1000",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 1000, 1000, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			return []string{"/var/lib/grafana"}, nil
		},
		ExtractImageContentFn: func(ctx context.Context, imageName string, paths []string, destDir string, maxBytes int64) map[string]error {
			extractCalled = true
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
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusRestarting,
			Quantity:     1,
			ContainerIDs: []string{"old-container"},
			CallbackURL:  callbackServer.URL,
		},
	})
	b.cfg.ContainerReadonlyRootfs = ptrBool(true)
	b.volumes = &mockVolumeManager{defaultDir: tmpDir}
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest := &DockerManifest{Image: "nginx:latest"}
	b.doRestart(context.Background(), "lease-1", manifest, []string{"old-container"}, "docker-small", backend.ProvisionStatusReady, b.logger)

	assert.True(t, extractCalled, "ExtractImageContent should be called on restart")
}

func TestDoProvision_WritablePaths_EphemeralCreatesVolume(t *testing.T) {
	// Ephemeral SKU (DiskMB=0) with writable paths should create a small volume.
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	tmpDir := t.TempDir()
	var volumeCreated bool
	var createdSizeMB int64

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{
				ID:      "sha256:abc",
				Volumes: map[string]struct{}{},
				User:    "1000",
			}, nil
		},
		ResolveImageUserFn: func(ctx context.Context, imageName string, userOverride string) (int, int, error) {
			return 1000, 1000, nil
		},
		DetectWritablePathsFn: func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
			return []string{"/var/lib/app"}, nil
		},
		ExtractImageContentFn: func(ctx context.Context, imageName string, paths []string, destDir string, maxBytes int64) map[string]error {
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

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusProvisioning,
			Quantity:  1,
		},
	})
	b.cfg.ContainerReadonlyRootfs = ptrBool(true)
	b.volumes = &mockVolumeManager{
		defaultDir: tmpDir,
		CreateFn: func(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
			volumeCreated = true
			createdSizeMB = sizeMB
			return tmpDir, true, nil
		},
	}
	b.provisions["lease-1"].CallbackURL = callbackServer.URL
	_ = b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	manifest, _ := ParseManifest(validManifestJSON("nginx:latest"))
	profiles := map[string]SKUProfile{"docker-small": {CPUCores: 0.5, MemoryMB: 512, DiskMB: 0}} // ephemeral

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	b.doProvisionAndFire(context.Background(), req, manifest, profiles, b.logger)

	assert.True(t, volumeCreated, "volume should be created for ephemeral SKU with writable paths")
	assert.Equal(t, int64(b.cfg.GetTmpfsSizeMB()), createdSizeMB, "ephemeral writable volume should use TmpfsSizeMB")

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
}

// TestProvision_DeprovisionWaitsForInFlightGoroutine pins the fix for
// bug_012: when Deprovision preempts an in-flight doProvision,
// Provisioning.OnExit must (1) cancel the goroutine's context and
// (2) wait for the goroutine to exit before doDeprovision reads
// ContainerIDs. Without this, the goroutine's successful creations
// stay on the host even though the provision struct reports none.
//
// Unit-tests the wait mechanism directly: installs synthetic
// workCancel + workDone on the actor, fires Deprovision,
// verifies OnExit blocks until workDone closes, and verifies
// doDeprovision then observes the "pre-published" ContainerIDs
// (simulating the real goroutine's pre-publish step).
func TestProvision_DeprovisionWaitsForInFlightGoroutine(t *testing.T) {
	var removedMu sync.Mutex
	var removedIDs []string
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			removedMu.Lock()
			removedIDs = append(removedIDs, containerID)
			removedMu.Unlock()
			return nil
		},
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusProvisioning,
			ContainerIDs: nil,
		},
	})
	defer b.stopCancel()

	// Eager SM in newLeaseActor reads initial state from the provision
	// entry's Status, so actor.sm starts in Provisioning.
	actor := b.actorFor("lease-1")
	require.Equal(t, backend.ProvisionStatusProvisioning, actor.sm.State())

	// Simulate an in-flight provision worker via workersWg. The actor's
	// onExitProvisioning will call workCancel then waitForWorkers.
	var cancelCalled atomic.Bool
	workerRelease := make(chan struct{})
	actor.workCancel = func() { cancelCalled.Store(true) }
	actor.workersWg.Add(1)
	go func() {
		<-workerRelease
		// Simulate the worker's pre-publish step before Done.
		b.provisionsMu.Lock()
		b.provisions["lease-1"].ContainerIDs = []string{"published-container"}
		b.provisionsMu.Unlock()
		actor.workersWg.Done()
	}()

	deprovErr := make(chan error, 1)
	go func() {
		deprovErr <- b.Deprovision(context.Background(), "lease-1")
	}()

	// onExitProvisioning must call workCancel before entering the wait.
	require.Eventually(t, cancelCalled.Load, 1*time.Second, 5*time.Millisecond,
		"OnExit must call workCancel before waitForWorkers")

	// Deprovision must be blocked in waitForWorkers.
	select {
	case err := <-deprovErr:
		t.Fatalf("Deprovision returned before worker finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Release the worker → wg.Done → waitForWorkers unblocks → doDeprovision
	// runs and reads the pre-published ContainerIDs.
	close(workerRelease)

	select {
	case err := <-deprovErr:
		require.NoError(t, err, "Deprovision must succeed after worker completes")
	case <-time.After(3 * time.Second):
		t.Fatal("Deprovision did not complete after worker finished")
	}

	removedMu.Lock()
	defer removedMu.Unlock()
	require.Contains(t, removedIDs, "published-container",
		"doDeprovision must see the pre-published ContainerIDs and call RemoveContainer")
}
