package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

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

// --- Finding 9: rollbackStackContainers skips start after rename failure ---

func TestRollbackStackContainers_SkipsStartAfterRenameFail(t *testing.T) {
	var mu sync.Mutex
	startedContainers := map[string]bool{}

	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited"}, nil
		},
		RenameContainerFn: func(ctx context.Context, containerID string, newName string) error {
			if containerID == "db-c1" {
				return fmt.Errorf("rename conflict")
			}
			return nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			mu.Lock()
			startedContainers[containerID] = true
			mu.Unlock()
			return nil
		},
	}

	b := newBackendForTest(mock, nil)

	serviceContainers := map[string][]string{
		"web": {"web-c1"},
		"db":  {"db-c1"},
	}

	allRestored := b.rollbackStackContainers("lease-1", serviceContainers, b.logger)

	assert.False(t, allRestored, "should return false when rename fails for a container")

	mu.Lock()
	defer mu.Unlock()
	assert.True(t, startedContainers["web-c1"], "web container should be started after successful rename")
	assert.False(t, startedContainers["db-c1"], "db container should NOT be started after rename failure")
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

	containerN := 0
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectImageFn: func(ctx context.Context, imageName string) (*ImageInfo, error) {
			return &ImageInfo{Volumes: map[string]struct{}{"/data": {}}}, nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			mu.Lock()
			containerN++
			id := fmt.Sprintf("container-%d", containerN)
			mu.Unlock()
			return id, nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
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
	sort.Strings(volumeIDs)
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

	// Track which container IDs correspond to which service.
	var mu sync.Mutex
	containerToService := map[string]string{}
	containerN := 0

	inspectCount := 0
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			mu.Lock()
			containerN++
			id := fmt.Sprintf("c-%s-%d", params.ServiceName, containerN)
			containerToService[id] = params.ServiceName
			mu.Unlock()
			return id, nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			mu.Lock()
			svc := containerToService[containerID]
			inspectCount++
			mu.Unlock()

			info := &ContainerInfo{ContainerID: containerID, Status: "running"}
			if svc == "web" {
				// Health check container: report healthy so waitForHealthy succeeds.
				info.Health = HealthStatusHealthy
			}
			return info, nil
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
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
	containerN := 0
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
		CreateContainerFn: func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
			mu.Lock()
			containerN++
			id := fmt.Sprintf("new-%d", containerN)
			mu.Unlock()
			return id, nil
		},
		StartContainerFn: func(ctx context.Context, containerID string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
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
