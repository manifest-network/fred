package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

// validStackManifestJSON builds a minimal valid stack manifest payload.
func validStackManifestJSON(services map[string]string) []byte {
	svcMap := make(map[string]*DockerManifest, len(services))
	for name, image := range services {
		svcMap[name] = &DockerManifest{Image: image}
	}
	sm := StackManifest{Services: svcMap}
	b, _ := json.Marshal(sm)
	return b
}

// newStackProvisionRequest creates a ProvisionRequest for stack testing.
func newStackProvisionRequest(leaseUUID, tenant string, items []backend.LeaseItem, payload []byte) backend.ProvisionRequest {
	return backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       tenant,
		ProviderUUID: "prov-1",
		Items:        items,
		CallbackURL:  "http://localhost/callback",
		Payload:      payload,
	}
}

// --- Finding 8: stackContainerLogKeys / containerLogKeys ---

func TestStackContainerLogKeys(t *testing.T) {
	t.Run("nil map", func(t *testing.T) {
		assert.Nil(t, stackContainerLogKeys(nil))
	})

	t.Run("empty map", func(t *testing.T) {
		assert.Nil(t, stackContainerLogKeys(map[string][]string{}))
	})

	t.Run("single service two containers", func(t *testing.T) {
		sc := map[string][]string{
			"web": {"cid1", "cid2"},
		}
		keys := stackContainerLogKeys(sc)
		assert.Equal(t, map[string]string{
			"cid1": "web/0",
			"cid2": "web/1",
		}, keys)
	})

	t.Run("two services", func(t *testing.T) {
		sc := map[string][]string{
			"web": {"w1", "w2"},
			"db":  {"d1"},
		}
		keys := stackContainerLogKeys(sc)
		assert.Len(t, keys, 3)
		assert.Equal(t, "web/0", keys["w1"])
		assert.Equal(t, "web/1", keys["w2"])
		assert.Equal(t, "db/0", keys["d1"])
	})

	t.Run("containerLogKeys with stack provision", func(t *testing.T) {
		prov := &provision{
			StackManifest: &StackManifest{
				Services: map[string]*DockerManifest{
					"web": {Image: "nginx"},
				},
			},
			ServiceContainers: map[string][]string{
				"web": {"c1", "c2"},
			},
		}
		keys := containerLogKeys(prov)
		assert.Equal(t, map[string]string{
			"c1": "web/0",
			"c2": "web/1",
		}, keys)
	})

	t.Run("containerLogKeys with non-stack provision", func(t *testing.T) {
		prov := &provision{
			ContainerIDs: []string{"c1"},
		}
		assert.Nil(t, containerLogKeys(prov))
	})

	t.Run("containerLogKeys with nil provision", func(t *testing.T) {
		assert.Nil(t, containerLogKeys(nil))
	})
}

// --- Finding 1: volume IDs are service-aware ---

func TestStackProvision_VolumeIDsAreServiceAware(t *testing.T) {
	var mu sync.Mutex
	volumeIDs := []string{}

	volDir := t.TempDir()
	vm := &mockVolumeManager{
		defaultDir: volDir,
		CreateFn: func(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
			mu.Lock()
			volumeIDs = append(volumeIDs, id)
			mu.Unlock()
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
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	composeMock := &mockComposeExecutor{
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{
				{ID: "web-c1", Service: "web", State: "running"},
				{ID: "db-c1", Service: "db", State: "running"},
			}, nil
		},
	}

	items := []backend.LeaseItem{
		{SKU: "docker-small-disk", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small-disk", Quantity: 1, ServiceName: "db"},
	}
	payload := validStackManifestJSON(map[string]string{
		"web": "nginx:latest",
		"db":  "postgres:16",
	})

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

	b := newBackendForProvisionTest(t, mock, nil)
	b.compose = composeMock
	b.volumes = vm
	// Add a disk-enabled SKU profile.
	b.cfg.SKUProfiles["docker-small-disk"] = SKUProfile{CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	req := newStackProvisionRequest("lease-1", "tenant-a", items, payload)
	req.CallbackURL = callbackServer.URL

	err := b.Provision(context.Background(), req)
	require.NoError(t, err)

	<-callbackReceived

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	status := prov.Status
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, status)

	mu.Lock()
	slices.Sort(volumeIDs)
	mu.Unlock()
	assert.Equal(t, []string{"fred-lease-1-db-0", "fred-lease-1-web-0"}, volumeIDs)

	b.stopCancel()
	b.wg.Wait()
}

// --- Finding 2: per-service health check verification ---

func TestStackProvision_PerServiceHealthCheck(t *testing.T) {
	// "web" has a health check, "db" does not.
	// Both should succeed if per-service health check verification works correctly.
	payload, _ := json.Marshal(StackManifest{
		Services: map[string]*DockerManifest{
			"web": {
				Image: "nginx:latest",
				HealthCheck: &HealthCheckConfig{
					Test:     []string{"CMD-SHELL", "curl -f http://localhost/"},
					Interval: Duration(1 * time.Second),
					Timeout:  Duration(1 * time.Second),
					Retries:  2,
				},
			},
			"db": {
				Image: "postgres:16",
				// No health check — uses simple startup verification.
			},
		},
	})

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			info := &ContainerInfo{ContainerID: containerID, Status: "running"}
			if containerID == "web-c1" {
				// Health check container: report healthy so waitForHealthy succeeds.
				info.Health = HealthStatusHealthy
			}
			return info, nil
		},
	}

	composeMock := &mockComposeExecutor{
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{
				{ID: "web-c1", Service: "web", State: "running"},
				{ID: "db-c1", Service: "db", State: "running"},
			}, nil
		},
	}

	items := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}

	callbackReceived := make(chan struct{})
	var callbackPayload backend.CallbackPayload
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, nil)
	b.compose = composeMock
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	req := newStackProvisionRequest("lease-1", "tenant-a", items, payload)
	req.CallbackURL = callbackServer.URL

	err := b.Provision(context.Background(), req)
	require.NoError(t, err)

	<-callbackReceived

	assert.Equal(t, backend.CallbackStatusSuccess, callbackPayload.Status, "provision should succeed with per-service health checks")

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	status := prov.Status
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, status)

	b.stopCancel()
	b.wg.Wait()
}

// --- Finding 3: re-provision cleans up old stack allocations ---

func TestStackReProvision_CleansUpOldStackAllocations(t *testing.T) {
	removedContainers := map[string]bool{}
	var mu sync.Mutex
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			mu.Lock()
			removedContainers[containerID] = true
			mu.Unlock()
			return nil
		},
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	composeMock := &mockComposeExecutor{
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{
				{ID: "new-web-c1", Service: "web", State: "running"},
				{ID: "new-db-c1", Service: "db", State: "running"},
			}, nil
		},
	}

	oldItems := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Status:       backend.ProvisionStatusFailed,
			FailCount:    1,
			Quantity:     2,
			ContainerIDs: []string{"old-web-c1", "old-db-c1"},
			Items:        oldItems,
			ServiceContainers: map[string][]string{
				"web": {"old-web-c1"},
				"db":  {"old-db-c1"},
			},
			StackManifest: &StackManifest{
				Services: map[string]*DockerManifest{
					"web": {Image: "nginx:latest"},
					"db":  {Image: "postgres:16"},
				},
			},
		},
	})
	b.compose = composeMock

	// Pre-allocate old stack resources with service-aware IDs.
	_ = b.pool.TryAllocate("lease-1-web-0", "docker-small", "tenant-a")
	_ = b.pool.TryAllocate("lease-1-db-0", "docker-small", "tenant-a")
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

	newItems := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}
	payload := validStackManifestJSON(map[string]string{
		"web": "nginx:latest",
		"db":  "postgres:16",
	})

	req := newStackProvisionRequest("lease-1", "tenant-a", newItems, payload)
	req.CallbackURL = callbackServer.URL

	err := b.Provision(context.Background(), req)
	require.NoError(t, err)

	// Old containers should be removed during re-provision cleanup (synchronous phase).
	mu.Lock()
	assert.True(t, removedContainers["old-web-c1"], "old web container should be removed")
	assert.True(t, removedContainers["old-db-c1"], "old db container should be removed")
	mu.Unlock()

	<-callbackReceived

	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	status := prov.Status
	newCIDs := prov.ContainerIDs
	svcContainers := prov.ServiceContainers
	b.provisionsMu.RUnlock()

	assert.Equal(t, backend.ProvisionStatusReady, status)
	assert.Len(t, newCIDs, 2, "should have 2 new containers")
	assert.Len(t, svcContainers, 2, "should have 2 services")

	// Verify new resources were allocated (pool would reject if old ones weren't freed).
	stats := b.pool.Stats()
	assert.Greater(t, stats.AllocatedCPU, float64(0), "resources should be allocated for new provision")

	b.stopCancel()
	b.wg.Wait()
}

// --- Stack Restart tests ---

func TestStackRestart_Success(t *testing.T) {
	stackManifest := &StackManifest{
		Services: map[string]*DockerManifest{
			"web": {Image: "nginx:latest"},
			"db":  {Image: "postgres:16"},
		},
	}
	items := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}

	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:     "lease-1",
			Tenant:        "tenant-a",
			ProviderUUID:  "prov-1",
			SKU:           "docker-small",
			Status:        backend.ProvisionStatusReady,
			StackManifest: stackManifest,
			ContainerIDs:  []string{"old-web", "old-db"},
			ServiceContainers: map[string][]string{
				"web": {"old-web"},
				"db":  {"old-db"},
			},
			Items: items,
		},
	}

	var mu sync.Mutex
	var upForceRecreate bool

	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	composeMock := &mockComposeExecutor{
		UpFn: func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
			mu.Lock()
			upForceRecreate = opts.ForceRecreate
			mu.Unlock()
			return nil
		},
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{
				{ID: "new-web-c1", Service: "web", State: "running"},
				{ID: "new-db-c1", Service: "db", State: "running"},
			}, nil
		},
	}

	var callbackPayload backend.CallbackPayload
	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.compose = composeMock
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
	})
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	// Verify Compose Up was called with ForceRecreate for restart.
	mu.Lock()
	assert.True(t, upForceRecreate, "restart should use ForceRecreate")
	mu.Unlock()

	// Verify callback indicates success.
	assert.Equal(t, backend.CallbackStatusSuccess, callbackPayload.Status)

	// Verify final state: new containers, ready status.
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	status := prov.Status
	svcContainers := prov.ServiceContainers
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, status)
	assert.Len(t, svcContainers, 2)
	assert.Len(t, svcContainers["web"], 1)
	assert.Len(t, svcContainers["db"], 1)

	b.stopCancel()
	b.wg.Wait()
}

func TestStackRestart_FailureRollsBack(t *testing.T) {
	stackManifest := &StackManifest{
		Services: map[string]*DockerManifest{
			"web": {Image: "nginx:latest"},
			"db":  {Image: "postgres:16"},
		},
	}
	items := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}

	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:     "lease-1",
			Tenant:        "tenant-a",
			ProviderUUID:  "prov-1",
			SKU:           "docker-small",
			Status:        backend.ProvisionStatusReady,
			StackManifest: stackManifest,
			ContainerIDs:  []string{"old-web", "old-db"},
			ServiceContainers: map[string][]string{
				"web": {"old-web"},
				"db":  {"old-db"},
			},
			Items: items,
		},
	}

	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	var mu sync.Mutex
	upCallCount := 0
	composeMock := &mockComposeExecutor{
		UpFn: func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
			mu.Lock()
			upCallCount++
			call := upCallCount
			mu.Unlock()
			if call == 1 {
				// First Up (restart) fails.
				return fmt.Errorf("compose up failed")
			}
			// Second Up (rollback) succeeds.
			return nil
		},
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{
				{ID: "restored-web", Service: "web", State: "running"},
				{ID: "restored-db", Service: "db", State: "running"},
			}, nil
		},
	}

	var callbackPayload backend.CallbackPayload
	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.compose = composeMock
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
	})
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	// Even though rollback succeeded, the operation failed — callback should report failure.
	assert.Equal(t, backend.CallbackStatusFailed, callbackPayload.Status)

	// After rollback via Compose, provision should be back to Ready.
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	status := prov.Status
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, status)

	// Verify Up was called twice (restart + rollback).
	mu.Lock()
	assert.Equal(t, 2, upCallCount, "should call Up twice: restart + rollback")
	mu.Unlock()

	b.stopCancel()
	b.wg.Wait()
}

// --- Stack Update tests ---

func TestStackUpdate_Success(t *testing.T) {
	oldStack := &StackManifest{
		Services: map[string]*DockerManifest{
			"web": {Image: "nginx:1.24"},
			"db":  {Image: "postgres:15"},
		},
	}
	items := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}

	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:     "lease-1",
			Tenant:        "tenant-a",
			ProviderUUID:  "prov-1",
			SKU:           "docker-small",
			Status:        backend.ProvisionStatusReady,
			StackManifest: oldStack,
			ContainerIDs:  []string{"old-web", "old-db"},
			ServiceContainers: map[string][]string{
				"web": {"old-web"},
				"db":  {"old-db"},
			},
			Items: items,
		},
	}

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	composeMock := &mockComposeExecutor{
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{
				{ID: "new-web-c1", Service: "web", State: "running"},
				{ID: "new-db-c1", Service: "db", State: "running"},
			}, nil
		},
	}

	var callbackPayload backend.CallbackPayload
	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, provisions)
	b.compose = composeMock
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	newPayload := validStackManifestJSON(map[string]string{
		"web": "nginx:1.25",
		"db":  "postgres:16",
	})

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: callbackServer.URL,
		Payload:     newPayload,
	})
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	assert.Equal(t, backend.CallbackStatusSuccess, callbackPayload.Status)

	// Verify OnSuccess updated the StackManifest.
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	status := prov.Status
	updatedManifest := prov.StackManifest
	svcContainers := prov.ServiceContainers
	b.provisionsMu.RUnlock()

	assert.Equal(t, backend.ProvisionStatusReady, status)
	require.NotNil(t, updatedManifest)
	assert.Equal(t, "nginx:1.25", updatedManifest.Services["web"].Image)
	assert.Equal(t, "postgres:16", updatedManifest.Services["db"].Image)
	assert.Len(t, svcContainers, 2)

	b.stopCancel()
	b.wg.Wait()
}

func TestStackUpdate_PayloadTypeMismatch(t *testing.T) {
	t.Run("stack lease with single manifest payload", func(t *testing.T) {
		provisions := map[string]*provision{
			"lease-1": {
				LeaseUUID:     "lease-1",
				Status:        backend.ProvisionStatusReady,
				StackManifest: &StackManifest{Services: map[string]*DockerManifest{"web": {Image: "nginx"}}},
				Items:         []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "web"}},
			},
		}
		b := newBackendForProvisionTest(t, &mockDockerClient{}, provisions)

		err := b.Update(context.Background(), backend.UpdateRequest{
			LeaseUUID: "lease-1",
			Payload:   validManifestJSON("nginx:latest"), // single, not stack
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, backend.ErrInvalidManifest)
	})

	t.Run("non-stack lease with stack manifest payload", func(t *testing.T) {
		provisions := map[string]*provision{
			"lease-1": {
				LeaseUUID: "lease-1",
				Status:    backend.ProvisionStatusReady,
				Manifest:  &DockerManifest{Image: "nginx"},
				SKU:       "docker-small",
			},
		}
		b := newBackendForProvisionTest(t, &mockDockerClient{}, provisions)

		err := b.Update(context.Background(), backend.UpdateRequest{
			LeaseUUID: "lease-1",
			Payload:   validStackManifestJSON(map[string]string{"web": "nginx:latest"}),
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, backend.ErrInvalidManifest)
	})
}

// --- Stack GetInfo tests ---

func TestGetInfo_Stack(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			switch containerID {
			case "web-c1":
				return &ContainerInfo{
					ContainerID:   "web-c1",
					InstanceIndex: 0,
					Image:         "nginx:latest",
					Status:        "running",
					Ports: map[string]PortBinding{
						"80/tcp": {HostIP: "0.0.0.0", HostPort: "8080"},
					},
				}, nil
			case "db-c1":
				return &ContainerInfo{
					ContainerID:   "db-c1",
					InstanceIndex: 0,
					Image:         "postgres:16",
					Status:        "running",
					Ports:         map[string]PortBinding{},
				}, nil
			}
			return nil, fmt.Errorf("unknown container")
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
			StackManifest: &StackManifest{
				Services: map[string]*DockerManifest{
					"web": {Image: "nginx:latest"},
					"db":  {Image: "postgres:16"},
				},
			},
			ContainerIDs: []string{"web-c1", "db-c1"},
			ServiceContainers: map[string][]string{
				"web": {"web-c1"},
				"db":  {"db-c1"},
			},
		},
	})
	b.cfg.HostAddress = "10.0.0.1"

	info, err := b.GetInfo(context.Background(), "lease-1")
	require.NoError(t, err)
	require.NotNil(t, info)

	// Should have Host and Services (not Instances).
	assert.Equal(t, "10.0.0.1", info.Host)
	assert.Empty(t, info.Instances, "stack GetInfo should not have flat instances")

	require.Len(t, info.Services, 2)

	// Verify web service.
	webSvc, ok := info.Services["web"]
	require.True(t, ok)
	require.Len(t, webSvc.Instances, 1)
	assert.Equal(t, "running", webSvc.Instances[0].Status)
	assert.Equal(t, "nginx:latest", webSvc.Instances[0].Image)

	// Verify db service.
	dbSvc, ok := info.Services["db"]
	require.True(t, ok)
	require.Len(t, dbSvc.Instances, 1)
	assert.Equal(t, "postgres:16", dbSvc.Instances[0].Image)
}

// --- Stack GetLogs tests ---

func TestGetLogs_Stack(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return fmt.Sprintf("logs from %s", containerID), nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
			StackManifest: &StackManifest{
				Services: map[string]*DockerManifest{
					"web": {Image: "nginx"},
					"db":  {Image: "postgres"},
				},
			},
			ContainerIDs: []string{"web-c1", "db-c1"},
			ServiceContainers: map[string][]string{
				"web": {"web-c1"},
				"db":  {"db-c1"},
			},
		},
	})

	logs, err := b.GetLogs(context.Background(), "lease-1", 50)
	require.NoError(t, err)
	assert.Len(t, logs, 2)
	assert.Equal(t, "logs from web-c1", logs["web/0"])
	assert.Equal(t, "logs from db-c1", logs["db/0"])
}

func TestGetLogs_Stack_MultiInstance(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return containerID, nil
		},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Status:    backend.ProvisionStatusReady,
			StackManifest: &StackManifest{
				Services: map[string]*DockerManifest{"web": {Image: "nginx"}},
			},
			ContainerIDs: []string{"w1", "w2"},
			ServiceContainers: map[string][]string{
				"web": {"w1", "w2"},
			},
		},
	})

	logs, err := b.GetLogs(context.Background(), "lease-1", 50)
	require.NoError(t, err)
	assert.Equal(t, "w1", logs["web/0"])
	assert.Equal(t, "w2", logs["web/1"])
}

// --- Stack Deprovision tests ---

func TestDeprovision_Stack(t *testing.T) {
	var downCalled bool
	var destroyedVols []string
	mock := &mockDockerClient{}
	vm := &mockVolumeManager{
		DestroyFn: func(ctx context.Context, id string) error {
			destroyedVols = append(destroyedVols, id)
			return nil
		},
	}

	composeMock := &mockComposeExecutor{
		DownFn: func(ctx context.Context, projectName string, timeout time.Duration) error {
			downCalled = true
			assert.Equal(t, "fred-lease-1", projectName)
			return nil
		},
	}

	items := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID: "lease-1",
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusReady,
			Quantity:  2,
			StackManifest: &StackManifest{
				Services: map[string]*DockerManifest{
					"web": {Image: "nginx"},
					"db":  {Image: "postgres"},
				},
			},
			ContainerIDs: []string{"web-c1", "db-c1"},
			ServiceContainers: map[string][]string{
				"web": {"web-c1"},
				"db":  {"db-c1"},
			},
			Items: items,
		},
	})
	b.compose = composeMock
	b.volumes = vm
	// Pre-allocate with service-aware IDs.
	_ = b.pool.TryAllocate("lease-1-web-0", "docker-small", "tenant-a")
	_ = b.pool.TryAllocate("lease-1-db-0", "docker-small", "tenant-a")

	err := b.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err)

	// Compose Down was called instead of individual RemoveContainer.
	assert.True(t, downCalled, "compose down should be called for stack deprovision")

	// Service-aware volumes destroyed.
	slices.Sort(destroyedVols)
	assert.Equal(t, []string{"fred-lease-1-db-0", "fred-lease-1-web-0"}, destroyedVols)

	// Provision removed.
	b.provisionsMu.RLock()
	_, exists := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)

	// Resources freed.
	stats := b.pool.Stats()
	assert.Equal(t, 0, stats.AllocationCount)
}

// --- Compose failure tests ---

func TestStackProvision_ComposeUpFailure(t *testing.T) {
	var downCalled bool
	var mu sync.Mutex

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
	}

	composeMock := &mockComposeExecutor{
		UpFn: func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
			return fmt.Errorf("compose up failed: out of memory")
		},
		DownFn: func(ctx context.Context, projectName string, timeout time.Duration) error {
			mu.Lock()
			downCalled = true
			mu.Unlock()
			return nil
		},
	}

	items := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}
	payload := validStackManifestJSON(map[string]string{
		"web": "nginx:latest",
		"db":  "postgres:16",
	})

	var callbackPayload backend.CallbackPayload
	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, nil)
	b.compose = composeMock
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)

	req := newStackProvisionRequest("lease-1", "tenant-a", items, payload)
	req.CallbackURL = callbackServer.URL

	err := b.Provision(context.Background(), req)
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	// Callback should indicate failure.
	assert.Equal(t, backend.CallbackStatusFailed, callbackPayload.Status)

	// Compose Down should be called for cleanup.
	mu.Lock()
	assert.True(t, downCalled, "compose down should be called on up failure")
	mu.Unlock()

	// Provision status should be failed.
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	b.provisionsMu.RUnlock()

	// Resources should be released.
	stats := b.pool.Stats()
	assert.Equal(t, 0, stats.AllocationCount)

	b.stopCancel()
	b.wg.Wait()
}

func TestDeprovision_Stack_DownFallback(t *testing.T) {
	var mu sync.Mutex
	removedContainers := map[string]bool{}

	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			mu.Lock()
			removedContainers[containerID] = true
			mu.Unlock()
			return nil
		},
	}

	composeMock := &mockComposeExecutor{
		DownFn: func(ctx context.Context, projectName string, timeout time.Duration) error {
			return fmt.Errorf("compose down failed")
		},
	}

	vm := &mockVolumeManager{
		DestroyFn: func(ctx context.Context, id string) error {
			return nil
		},
	}

	items := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			StackManifest: &StackManifest{
				Services: map[string]*DockerManifest{
					"web": {Image: "nginx"},
					"db":  {Image: "postgres"},
				},
			},
			ContainerIDs: []string{"web-c1", "db-c1"},
			ServiceContainers: map[string][]string{
				"web": {"web-c1"},
				"db":  {"db-c1"},
			},
			Items: items,
		},
	})
	b.compose = composeMock
	b.volumes = vm
	_ = b.pool.TryAllocate("lease-1-web-0", "docker-small", "tenant-a")
	_ = b.pool.TryAllocate("lease-1-db-0", "docker-small", "tenant-a")

	err := b.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err)

	// Both containers should be removed individually as fallback.
	mu.Lock()
	assert.True(t, removedContainers["web-c1"], "web container should be removed individually")
	assert.True(t, removedContainers["db-c1"], "db container should be removed individually")
	mu.Unlock()

	// Provision removed.
	b.provisionsMu.RLock()
	_, exists := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)

	// Resources freed.
	stats := b.pool.Stats()
	assert.Equal(t, 0, stats.AllocationCount)
}

// --- Stack recoverState tests ---

func TestRecoverState_Stack(t *testing.T) {
	now := time.Now()

	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{
					ContainerID:   "web-c1",
					LeaseUUID:     "lease-1",
					Tenant:        "tenant-a",
					ProviderUUID:  "prov-1",
					SKU:           "docker-small",
					ServiceName:   "web",
					InstanceIndex: 0,
					Image:         "nginx:latest",
					Status:        "running",
					CreatedAt:     now,
				},
				{
					ContainerID:   "db-c1",
					LeaseUUID:     "lease-1",
					Tenant:        "tenant-a",
					ProviderUUID:  "prov-1",
					SKU:           "docker-small",
					ServiceName:   "db",
					InstanceIndex: 0,
					Image:         "postgres:16",
					Status:        "running",
					CreatedAt:     now,
				},
			}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	err := b.recoverState(context.Background())
	require.NoError(t, err)

	require.Len(t, b.provisions, 1)
	prov := b.provisions["lease-1"]
	require.NotNil(t, prov)
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
	assert.ElementsMatch(t, []string{"web-c1", "db-c1"}, prov.ContainerIDs)

	// ServiceContainers should be populated.
	require.Len(t, prov.ServiceContainers, 2)
	assert.Equal(t, []string{"web-c1"}, prov.ServiceContainers["web"])
	assert.Equal(t, []string{"db-c1"}, prov.ServiceContainers["db"])

	// Items should be rebuilt from container labels.
	require.Len(t, prov.Items, 2)
	itemMap := map[string]backend.LeaseItem{}
	for _, item := range prov.Items {
		itemMap[item.ServiceName] = item
	}
	assert.Equal(t, "docker-small", itemMap["web"].SKU)
	assert.Equal(t, 1, itemMap["web"].Quantity)
	assert.Equal(t, "docker-small", itemMap["db"].SKU)
	assert.Equal(t, 1, itemMap["db"].Quantity)

	// Resource allocations should use service-aware IDs.
	stats := b.pool.Stats()
	assert.Equal(t, 2, stats.AllocationCount)
}

func TestRecoverState_StackMultiInstance(t *testing.T) {
	now := time.Now()

	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{
					ContainerID:   "web-c0",
					LeaseUUID:     "lease-1",
					Tenant:        "tenant-a",
					ProviderUUID:  "prov-1",
					SKU:           "docker-small",
					ServiceName:   "web",
					InstanceIndex: 0,
					Image:         "nginx:latest",
					Status:        "running",
					CreatedAt:     now,
				},
				{
					ContainerID:   "web-c1",
					LeaseUUID:     "lease-1",
					Tenant:        "tenant-a",
					ProviderUUID:  "prov-1",
					SKU:           "docker-small",
					ServiceName:   "web",
					InstanceIndex: 1,
					Image:         "nginx:latest",
					Status:        "running",
					CreatedAt:     now,
				},
			}, nil
		},
	}
	b := newBackendForTest(mock, nil)

	err := b.recoverState(context.Background())
	require.NoError(t, err)

	prov := b.provisions["lease-1"]
	require.NotNil(t, prov)

	// Two containers for the same service should produce one item with quantity 2.
	require.Len(t, prov.Items, 1)
	assert.Equal(t, "web", prov.Items[0].ServiceName)
	assert.Equal(t, 2, prov.Items[0].Quantity)
	assert.Len(t, prov.ServiceContainers["web"], 2)
}
