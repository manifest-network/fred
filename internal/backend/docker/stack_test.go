package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

const (
	stackMaintenanceLeaseUUID = "11111111-1111-4111-8111-111111111111"
	stackFixtureLeaseUUID     = "33333333-3333-4333-8333-333333333333"
)

// installStackStrictCohortInventory makes the Docker inventory observed by a
// stack replacement come from the exact labels emitted by its last successful
// Compose Up. Restart/update settlement deliberately requires both Compose PS
// and a strict Docker inventory of the same generation, so returning a generic
// running container here would make these tests bypass the production safety
// boundary they are meant to exercise.
func installStackStrictCohortInventory(
	t *testing.T,
	dockerMock *mockDockerClient,
	composeMock *mockComposeExecutor,
) {
	t.Helper()
	originalList := dockerMock.ListManagedContainersFn

	type serviceSnapshot struct {
		image  string
		labels map[string]string
	}
	type projectSnapshot struct {
		name     string
		services map[string]serviceSnapshot
	}
	var (
		mu              sync.Mutex
		current         projectSnapshot
		strictPublished bool
	)
	originalUp := composeMock.UpFn
	originalInspect := dockerMock.InspectContainerFn
	composeMock.UpFn = func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
		if originalUp != nil {
			if err := originalUp(ctx, project, opts); err != nil {
				return err
			}
		}
		snapshot := projectSnapshot{
			name:     project.Name,
			services: make(map[string]serviceSnapshot, len(project.Services)),
		}
		for serviceName, service := range project.Services {
			snapshot.services[serviceName] = serviceSnapshot{
				image:  service.Image,
				labels: maps.Clone(service.Labels),
			}
		}
		mu.Lock()
		current = snapshot
		strictPublished = true
		mu.Unlock()
		return nil
	}
	originalDown := composeMock.DownFn
	composeMock.DownFn = func(ctx context.Context, projectName string, timeout time.Duration) error {
		if originalDown != nil {
			if err := originalDown(ctx, projectName, timeout); err != nil {
				return err
			}
		}
		mu.Lock()
		current = projectSnapshot{}
		strictPublished = true
		mu.Unlock()
		return nil
	}

	dockerMock.ListManagedContainersFn = func(ctx context.Context) ([]ContainerInfo, error) {
		mu.Lock()
		snapshot := projectSnapshot{
			name:     current.name,
			services: maps.Clone(current.services),
		}
		published := strictPublished
		mu.Unlock()
		if !published && originalList != nil {
			return originalList(ctx)
		}
		if snapshot.name == "" {
			return nil, nil
		}
		containers, err := composeMock.PS(ctx, snapshot.name)
		if err != nil {
			return nil, err
		}
		inventory := make([]ContainerInfo, 0, len(containers))
		for _, container := range containers {
			service, ok := snapshot.services[container.Service]
			if !ok {
				return nil, fmt.Errorf("compose service %q is absent from the successful project", container.Service)
			}
			instanceIndex, err := strconv.Atoi(service.labels[LabelInstanceIndex])
			if err != nil {
				return nil, fmt.Errorf("parse %s for service %q: %w", LabelInstanceIndex, container.Service, err)
			}
			createdAt, err := time.Parse(time.RFC3339, service.labels[LabelCreatedAt])
			if err != nil {
				return nil, fmt.Errorf("parse %s for service %q: %w", LabelCreatedAt, container.Service, err)
			}
			inventory = append(inventory, ContainerInfo{
				ContainerID:          container.ID,
				LeaseUUID:            service.labels[LabelLeaseUUID],
				Tenant:               service.labels[LabelTenant],
				ProviderUUID:         service.labels[LabelProviderUUID],
				BackendName:          service.labels[LabelBackendName],
				SKU:                  service.labels[LabelSKU],
				ServiceName:          service.labels[LabelServiceName],
				CallbackURL:          service.labels[LabelCallbackURL],
				LifecycleCallbackURL: service.labels[LabelLifecycleCallbackURL],
				MaintenanceID:        mustParseMaintenanceID(t, service.labels[LabelMaintenanceID]),
				Image:                service.image,
				Status:               container.State,
				Health:               HealthStatus(container.Health),
				CreatedAt:            createdAt,
				InstanceIndex:        instanceIndex,
				CustomDomain:         service.labels[LabelCustomDomain],
			})
		}
		return inventory, nil
	}
	dockerMock.InspectContainerFn = func(ctx context.Context, containerID string) (*ContainerInfo, error) {
		if originalInspect == nil {
			return nil, fmt.Errorf("strict cohort fixture has no container inspector")
		}
		inspected, err := originalInspect(ctx, containerID)
		if err != nil {
			return nil, err
		}
		inventory, err := dockerMock.ListManagedContainersFn(ctx)
		if err != nil {
			return nil, err
		}
		for _, listed := range inventory {
			if listed.ContainerID != containerID {
				continue
			}
			// Docker Inspect repeats the immutable labels returned by List and
			// adds a fresher runtime state. Keep the exact generated identity while
			// allowing individual tests to model a changing status/health result.
			if inspected != nil {
				if inspected.Status != "" {
					listed.Status = inspected.Status
				}
				listed.Health = inspected.Health
				listed.ExitCode = inspected.ExitCode
				listed.OOMKilled = inspected.OOMKilled
			}
			return &listed, nil
		}
		return nil, fmt.Errorf("container %q is absent from the exact fixture inventory", containerID)
	}
}

// seedStackMaintenanceAuthority attaches the same durable source release,
// maintenance journal, and callback replay loop required by production. The
// source authority is exact: callback identity, workload topology, resource
// sizing, tenant, provider, and canonical lease identity all agree.
func seedStackMaintenanceAuthority(
	t *testing.T,
	b *Backend,
	leaseUUID string,
	stack *manifest.StackManifest,
	items []backend.LeaseItem,
	operationID shared.OperationID,
	callbackURL, lifecycleCallbackURL string,
	client *http.Client,
) {
	t.Helper()
	require.True(t, backend.IsCanonicalLeaseUUID(leaseUUID))
	require.True(t, operationID.Valid())

	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	profiles := testResourceProfiles(t, items)
	attachBoundOperationHandoffStores(t, b)
	seedProvisionReleaseForBackendTest(t, b, leaseUUID, shared.Release{
		Manifest:         manifestBytes,
		Image:            "stack",
		OperationID:      operationID,
		Items:            slices.Clone(items),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(profiles),
		RuntimeAuthority: mustTestReleaseRuntimeAuthority(
			t, operationID, "tenant-a", nominalDockerProviderUUID,
			callbackURL, lifecycleCallbackURL,
		),
		Status:    "active",
		CreatedAt: time.Now(),
	})

	b.provisionsMu.Lock()
	provision := b.provisions[leaseUUID]
	if provision != nil {
		provision.LeaseUUID = leaseUUID
		provision.Tenant = "tenant-a"
		provision.ProviderUUID = nominalDockerProviderUUID
		provision.CallbackURL = callbackURL
		provision.LifecycleCallbackURL = lifecycleCallbackURL
		provision.ResourceProfiles = shared.CloneSKUResourceSnapshot(profiles)
	}
	b.provisionsMu.Unlock()
	require.NotNil(t, provision)

	rebuildCallbackSender(b, client)
	b.wg.Go(b.callbackSender.RunReplayLoop)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
}

// awaitStackMaintenanceCallback waits for both the HTTP observation and the
// sender's precise durable-row removal. Handler receipt alone is too early a
// shutdown barrier: canceling stopCtx while the client is still consuming the
// 2xx response correctly leaves the row for replay.
func awaitStackMaintenanceCallback(t *testing.T, b *Backend, received <-chan struct{}) {
	t.Helper()
	select {
	case <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}
	require.Eventually(t, func() bool {
		pending, err := b.callbackStore.ListPending()
		return err == nil && len(pending) == 0
	}, 5*time.Second, time.Millisecond, "callback delivery must remove its exact durable row")
}

// validStackManifestJSON builds a minimal valid stack manifest payload.
func validStackManifestJSON(services map[string]string) []byte {
	svcMap := make(map[string]*manifest.Manifest, len(services))
	for name, image := range services {
		svcMap[name] = &manifest.Manifest{Image: image}
	}
	sm := manifest.StackManifest{Services: svcMap}
	b, _ := json.Marshal(sm)
	return b
}

// newStackProvisionRequest creates a ProvisionRequest for stack testing.
func newStackProvisionRequest(leaseUUID, tenant string, items []backend.LeaseItem, payload []byte) backend.ProvisionRequest {
	return backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       tenant,
		ProviderUUID: nominalDockerProviderUUID,
		Items:        items,
		CallbackURL:  "http://localhost/callbacks/provision",
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
		prov := &provision{ProvisionState: leasesm.ProvisionState{StackManifest: &manifest.StackManifest{
			Services: map[string]*manifest.Manifest{
				"web": {Image: "nginx"},
			},
		},
			ServiceContainers: map[string][]string{
				"web": {"c1", "c2"},
			}},
		}
		keys := leasesm.ContainerLogKeys(&prov.ProvisionState)
		assert.Equal(t, map[string]string{
			"c1": "web/0",
			"c2": "web/1",
		}, keys)
	})

	t.Run("containerLogKeys with non-stack provision", func(t *testing.T) {
		prov := &provision{ProvisionState: leasesm.ProvisionState{ContainerIDs: []string{"c1"}}}
		assert.Nil(t, leasesm.ContainerLogKeys(&prov.ProvisionState))
	})

	t.Run("containerLogKeys with nil provision", func(t *testing.T) {
		assert.Nil(t, leasesm.ContainerLogKeys(nil))
	})
}

// --- Finding 1: volume IDs are service-aware ---

func TestStackProvision_VolumeIDsAreServiceAware(t *testing.T) {
	var mu sync.Mutex
	volumeIDs := []string{}

	volumeRoot := t.TempDir()
	vm := &mockVolumeManager{
		defaultDir: volumeRoot,
		CreateFn: func(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
			mu.Lock()
			volumeIDs = append(volumeIDs, id)
			mu.Unlock()
			path := filepath.Join(volumeRoot, id)
			if err := os.MkdirAll(path, 0o700); err != nil {
				return "", false, err
			}
			return path, true, nil
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
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()

	installStackStrictCohortInventory(t, mock, composeMock)
	b := newBackendForProvisionTest(t, mock, nil)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	b.compose = composeMock
	b.volumes = vm
	b.cfg.VolumeDataPath = volumeRoot
	// Add a disk-enabled SKU profile.
	b.cfg.SKUProfiles["docker-small-disk"] = SKUProfile{CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	req := newStackProvisionRequest(stackFixtureLeaseUUID, "tenant-a", items, payload)
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	rebuildCallbackSender(b, callbackServer.Client())
	startCallbackReplayForTest(b)

	err := b.Provision(context.Background(), req)
	require.NoError(t, err)

	waitForTestSignal(t, callbackReceived, "service-aware stack callback")

	b.provisionsMu.RLock()
	prov := b.provisions[stackFixtureLeaseUUID]
	status := prov.Status
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, status)

	mu.Lock()
	slices.Sort(volumeIDs)
	mu.Unlock()
	assert.Equal(t, []string{
		canonicalVolumeName(stackFixtureLeaseUUID, "db", 0),
		canonicalVolumeName(stackFixtureLeaseUUID, "web", 0),
	}, volumeIDs)

}

// --- Finding 2: per-service health check verification ---

func TestStackProvision_PerServiceHealthCheck(t *testing.T) {
	// "web" has a health check, "db" does not.
	// Both should succeed if per-service health check verification works correctly.
	payload, _ := json.Marshal(manifest.StackManifest{
		Services: map[string]*manifest.Manifest{
			"web": {
				Image: "nginx:latest",
				HealthCheck: &manifest.HealthCheckConfig{
					Test:     []string{"CMD-SHELL", "curl -f http://localhost/"},
					Interval: manifest.Duration(1 * time.Second),
					Timeout:  manifest.Duration(1 * time.Second),
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
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()

	installStackStrictCohortInventory(t, mock, composeMock)
	b := newBackendForProvisionTest(t, mock, nil)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	b.compose = composeMock
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	req := newStackProvisionRequest(stackFixtureLeaseUUID, "tenant-a", items, payload)
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)
	rebuildCallbackSender(b, callbackServer.Client())
	startCallbackReplayForTest(b)

	err := b.Provision(context.Background(), req)
	require.NoError(t, err)

	waitForTestSignal(t, callbackReceived, "per-service health callback")

	assert.Equal(t, backend.CallbackStatusSuccess, callbackPayload.Status, "provision should succeed with per-service health checks")

	b.provisionsMu.RLock()
	prov := b.provisions[stackFixtureLeaseUUID]
	status := prov.Status
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, status)

}

// --- Finding 3: re-provision cleans up old stack allocations ---

func TestStackReProvision_CleansUpOldStackAllocations(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	removedContainers := map[string]bool{}
	var oldContainers []ContainerInfo
	var mu sync.Mutex
	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			mu.Lock()
			defer mu.Unlock()
			removedContainers[containerID] = true
			oldContainers = slices.DeleteFunc(oldContainers, func(container ContainerInfo) bool {
				return container.ContainerID == containerID
			})
			return nil
		},
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
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
	oldPayload := validStackManifestJSON(map[string]string{
		"web": "nginx:latest",
		"db":  "postgres:16",
	})
	oldProfiles := testResourceProfiles(t, oldItems)
	oldOperationID := mustDockerOperationID("9a72fbc2-38c8-4f31-87f7-f689979b9324")
	oldCallbackURL := "https://old.example/callbacks/provision?operation_id=" + oldOperationID.String()
	oldLifecycleURL, err := backend.ResolveLifecycleCallbackURL(oldCallbackURL, "")
	require.NoError(t, err)
	oldAuthority, err := shared.NewReleaseRuntimeAuthority(
		oldOperationID, "tenant-a", nominalDockerProviderUUID, oldCallbackURL, oldLifecycleURL,
	)
	require.NoError(t, err)
	oldContainers = []ContainerInfo{
		{
			ContainerID: "old-web-c1", LeaseUUID: leaseUUID, Tenant: "tenant-a",
			ProviderUUID: nominalDockerProviderUUID, SKU: "docker-small", ServiceName: "web",
			BackendName:   DefaultConfig().Name,
			InstanceIndex: 0, CallbackURL: oldCallbackURL, LifecycleCallbackURL: oldLifecycleURL,
			Image: "nginx:latest", Status: "running",
		},
		{
			ContainerID: "old-db-c1", LeaseUUID: leaseUUID, Tenant: "tenant-a",
			ProviderUUID: nominalDockerProviderUUID, SKU: "docker-small", ServiceName: "db",
			BackendName:   DefaultConfig().Name,
			InstanceIndex: 0, CallbackURL: oldCallbackURL, LifecycleCallbackURL: oldLifecycleURL,
			Image: "postgres:16", Status: "running",
		},
	}
	mock.ListManagedContainersFn = func(context.Context) ([]ContainerInfo, error) {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(oldContainers), nil
	}
	mock.InspectContainerFn = func(_ context.Context, containerID string) (*ContainerInfo, error) {
		mu.Lock()
		defer mu.Unlock()
		for _, container := range oldContainers {
			if container.ContainerID == containerID {
				copy := container
				return &copy, nil
			}
		}
		return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
	}

	composeMock.DownFn = func(context.Context, string, time.Duration) error {
		return errors.New("force strict per-container predecessor teardown")
	}
	installStackStrictCohortInventory(t, mock, composeMock)
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: leaseUUID,
			Tenant:               "tenant-a",
			ProviderUUID:         nominalDockerProviderUUID,
			Status:               backend.ProvisionStatusFailed,
			FailCount:            1,
			Quantity:             2,
			ContainerIDs:         []string{"old-web-c1", "old-db-c1"},
			CallbackURL:          oldCallbackURL,
			LifecycleCallbackURL: oldLifecycleURL,
			Items:                oldItems,
			ResourceProfiles:     oldProfiles,
			ServiceContainers: map[string][]string{
				"web": {"old-web-c1"},
				"db":  {"old-db-c1"},
			},
			StackManifest: &manifest.StackManifest{
				Services: map[string]*manifest.Manifest{
					"web": {Image: "nginx:latest"},
					"db":  {Image: "postgres:16"},
				},
			}},
		},
	})
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	attachReleaseStore(t, b)
	seedProvisionReleaseForBackendTest(t, b, leaseUUID, shared.Release{
		Manifest:         oldPayload,
		Image:            "stack",
		OperationID:      oldOperationID,
		Items:            slices.Clone(oldItems),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(oldProfiles),
		RuntimeAuthority: &oldAuthority,
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	})
	b.compose = composeMock

	// Pre-allocate old stack resources with service-aware IDs.
	_ = b.pool.TryAllocate(leaseUUID+"-web-0", "docker-small", "tenant-a")
	_ = b.pool.TryAllocate(leaseUUID+"-db-0", "docker-small", "tenant-a")
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	callbackReceived := make(chan struct{})
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()
	rebuildCallbackSender(b, callbackServer.Client())

	newItems := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}
	payload := validStackManifestJSON(map[string]string{
		"web": "nginx:latest",
		"db":  "postgres:16",
	})

	req := newStackProvisionRequest(leaseUUID, "tenant-a", newItems, payload)
	candidateOperationID := mustDockerOperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
	req.CallbackURL = callbackServer.URL + "?operation_id=" + candidateOperationID.String()

	err = b.Provision(context.Background(), req)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		return b.provisions[leaseUUID] != nil &&
			b.provisions[leaseUUID].Status == backend.ProvisionStatusReady
	}, 5*time.Second, time.Millisecond)

	// The asynchronous replacement worker must remove the exact predecessor
	// generation before publishing the candidate generation.
	mu.Lock()
	assert.True(t, removedContainers["old-web-c1"], "old web container should be removed")
	assert.True(t, removedContainers["old-db-c1"], "old db container should be removed")
	mu.Unlock()

	b.provisionsMu.RLock()
	prov := b.provisions[leaseUUID]
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

}

// --- Stack Restart tests ---

func TestStackRestart_Success(t *testing.T) {
	stackManifest := &manifest.StackManifest{
		Services: map[string]*manifest.Manifest{
			"web": {Image: "nginx:latest"},
			"db":  {Image: "postgres:16"},
		},
	}
	items := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}

	provisions := map[string]*provision{
		stackMaintenanceLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: stackMaintenanceLeaseUUID,
			Tenant:        "tenant-a",
			ProviderUUID:  nominalDockerProviderUUID,
			SKU:           "docker-small",
			Status:        backend.ProvisionStatusReady,
			StackManifest: stackManifest,
			ContainerIDs:  []string{"old-web", "old-db"},
			ServiceContainers: map[string][]string{
				"web": {"old-web"},
				"db":  {"old-db"},
			},
			Items: items},
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
	installStackStrictCohortInventory(t, mock, composeMock)

	var callbackPayload backend.CallbackPayload
	var callbackRequestURI string
	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		callbackRequestURI = r.URL.RequestURI()
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()
	const lifecycleID = "550e8400-e29b-41d4-a716-446655440000"
	oldOperationURL := callbackServer.URL + "/old/callbacks/provision?operation_id=" + lifecycleID
	oldLifecycleURL := callbackServer.URL + "/old/callbacks/provision?lifecycle_id=" + lifecycleID
	newOperationURL := callbackServer.URL + "/new/callbacks/provision?operation_id=" + lifecycleID
	newLifecycleURL := callbackServer.URL + "/new/callbacks/provision?lifecycle_id=" + lifecycleID

	b := newBackendForProvisionTest(t, mock, provisions)
	b.compose = composeMock
	seedStackMaintenanceAuthority(
		t, b, stackMaintenanceLeaseUUID, stackManifest, items,
		mustDockerOperationID(lifecycleID), oldOperationURL, oldLifecycleURL,
		callbackServer.Client(),
	)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	err := b.Restart(context.Background(), backend.RestartRequest{MaintenanceID: newTestMaintenanceID(t),
		LeaseUUID:   stackMaintenanceLeaseUUID,
		CallbackURL: newLifecycleURL,
	})
	require.NoError(t, err)

	awaitStackMaintenanceCallback(t, b, callbackReceived)

	// Verify Compose Up was called with ForceRecreate for restart.
	mu.Lock()
	assert.True(t, upForceRecreate, "restart should use ForceRecreate")
	mu.Unlock()

	// Verify callback indicates success.
	assert.Equal(t, backend.CallbackStatusSuccess, callbackPayload.Status)
	assert.Equal(t, "/new/callbacks/provision?lifecycle_id="+lifecycleID, callbackRequestURI,
		"restart completion must use the same lifecycle capability at the current callback base")

	// Verify final state: new containers, ready status.
	b.provisionsMu.RLock()
	prov := b.provisions[stackMaintenanceLeaseUUID]
	status := prov.Status
	svcContainers := prov.ServiceContainers
	gotOperationURL := prov.CallbackURL
	gotLifecycleURL := prov.LifecycleCallbackURL
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, status)
	assert.Len(t, svcContainers, 2)
	assert.Len(t, svcContainers["web"], 1)
	assert.Len(t, svcContainers["db"], 1)
	assert.Equal(t, newOperationURL, gotOperationURL, "restart must persist the relocated callback pair")
	assert.Equal(t, newLifecycleURL, gotLifecycleURL, "restart must preserve lifecycle identity")

	b.stopCancel()
	b.wg.Wait()
}

func TestStackRestart_ComposeUpFailurePreservesRecoveryAuthority(t *testing.T) {
	stackManifest := &manifest.StackManifest{
		Services: map[string]*manifest.Manifest{
			"web": {Image: "nginx:latest"},
			"db":  {Image: "postgres:16"},
		},
	}
	items := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}

	provisions := map[string]*provision{
		stackMaintenanceLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: stackMaintenanceLeaseUUID,
			Tenant:        "tenant-a",
			ProviderUUID:  nominalDockerProviderUUID,
			SKU:           "docker-small",
			Status:        backend.ProvisionStatusReady,
			StackManifest: stackManifest,
			ContainerIDs:  []string{"old-web", "old-db"},
			ServiceContainers: map[string][]string{
				"web": {"old-web"},
				"db":  {"old-db"},
			},
			Items: items},
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
			return fmt.Errorf("compose up failed (attempt %d)", call)
		},
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{
				{ID: "restored-web", Service: "web", State: "running"},
				{ID: "restored-db", Service: "db", State: "running"},
			}, nil
		},
	}
	installStackStrictCohortInventory(t, mock, composeMock)

	var callbackPayload backend.CallbackPayload
	callbackReceived := make(chan struct{})
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	operationID := mustDockerOperationID("6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	operationURL := callbackServer.URL + "?operation_id=" + operationID.String()
	lifecycleURL := callbackServer.URL + "?lifecycle_id=" + operationID.String()
	seedStackMaintenanceAuthority(
		t, b, stackMaintenanceLeaseUUID, stackManifest, items,
		operationID, operationURL, lifecycleURL, callbackServer.Client(),
	)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	err := b.Restart(context.Background(), backend.RestartRequest{MaintenanceID: newTestMaintenanceID(t),
		LeaseUUID:   stackMaintenanceLeaseUUID,
		CallbackURL: lifecycleURL,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		intent, found, readErr := b.maintenanceSettlement.GetMaintenanceIntent(stackMaintenanceLeaseUUID)
		return readErr == nil && found &&
			intent.ExecutionPhase() == shared.MaintenanceExecutionStarted &&
			!b.actorOwnsMaintenance(stackMaintenanceLeaseUUID, intent.MaintenanceID())
	}, 5*time.Second, time.Millisecond,
		"the exact Started restart must hand off from the live worker to recovery")
	select {
	case <-callbackReceived:
		t.Fatal("an ambiguous Compose result must not emit a terminal callback")
	default:
	}

	// Compose Up crossed the external-effect boundary. Its error cannot prove
	// that Docker published nothing, so neither rollback nor a terminal actor
	// projection is safe until strict inventory recovery classifies the cohort.
	b.provisionsMu.RLock()
	prov := b.provisions[stackMaintenanceLeaseUUID]
	status := prov.Status
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusRestarting, status)

	// Recovery, not an immediate second mutation, owns the ambiguous outcome.
	mu.Lock()
	assert.Equal(t, 1, upCallCount, "ambiguous restart must not attempt an unsafe rollback")
	mu.Unlock()

	b.stopCancel()
	b.wg.Wait()
}

// --- Stack Update tests ---

func TestStackUpdate_Success(t *testing.T) {
	const lifecycleID = "550e8400-e29b-41d4-a716-446655440000"
	oldStack := &manifest.StackManifest{
		Services: map[string]*manifest.Manifest{
			"web": {Image: "nginx:1.24"},
			"db":  {Image: "postgres:15"},
		},
	}
	items := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}

	provisions := map[string]*provision{
		stackMaintenanceLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: stackMaintenanceLeaseUUID,
			Tenant:        "tenant-a",
			ProviderUUID:  nominalDockerProviderUUID,
			SKU:           "docker-small",
			Status:        backend.ProvisionStatusReady,
			StackManifest: oldStack,
			ContainerIDs:  []string{"old-web", "old-db"},
			ServiceContainers: map[string][]string{
				"web": {"old-web"},
				"db":  {"old-db"},
			},
			Items: items},
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
	installStackStrictCohortInventory(t, mock, composeMock)

	var callbackPayload backend.CallbackPayload
	var callbackRequestURI string
	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		callbackRequestURI = r.URL.RequestURI()
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
		}
	}))
	defer callbackServer.Close()
	oldOperationURL := callbackServer.URL + "/old/callbacks/provision?operation_id=" + lifecycleID
	oldLifecycleURL := callbackServer.URL + "/old/callbacks/provision?lifecycle_id=" + lifecycleID
	newOperationURL := callbackServer.URL + "/new/callbacks/provision?operation_id=" + lifecycleID
	newLifecycleURL := callbackServer.URL + "/new/callbacks/provision?lifecycle_id=" + lifecycleID

	b := newBackendForProvisionTest(t, mock, provisions)
	b.compose = composeMock
	seedStackMaintenanceAuthority(
		t, b, stackMaintenanceLeaseUUID, oldStack, items,
		mustDockerOperationID(lifecycleID), oldOperationURL, oldLifecycleURL,
		callbackServer.Client(),
	)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	newPayload := validStackManifestJSON(map[string]string{
		"web": "nginx:1.25",
		"db":  "postgres:16",
	})

	err := b.Update(context.Background(), backend.UpdateRequest{MaintenanceID: newTestMaintenanceID(t),
		LeaseUUID:   stackMaintenanceLeaseUUID,
		CallbackURL: newLifecycleURL,
		Payload:     newPayload,
	})
	require.NoError(t, err)

	awaitStackMaintenanceCallback(t, b, callbackReceived)

	assert.Equal(t, backend.CallbackStatusSuccess, callbackPayload.Status)
	assert.Equal(t, "/new/callbacks/provision?lifecycle_id="+lifecycleID, callbackRequestURI,
		"update completion must use the same lifecycle capability at the current callback base")

	// Verify OnSuccess updated the manifest.StackManifest.
	b.provisionsMu.RLock()
	prov := b.provisions[stackMaintenanceLeaseUUID]
	status := prov.Status
	updatedManifest := prov.StackManifest
	svcContainers := prov.ServiceContainers
	gotOperationURL := prov.CallbackURL
	gotLifecycleURL := prov.LifecycleCallbackURL
	b.provisionsMu.RUnlock()

	assert.Equal(t, backend.ProvisionStatusReady, status)
	require.NotNil(t, updatedManifest)
	assert.Equal(t, "nginx:1.25", updatedManifest.Services["web"].Image)
	assert.Equal(t, "postgres:16", updatedManifest.Services["db"].Image)
	assert.Len(t, svcContainers, 2)
	assert.Equal(t, newOperationURL, gotOperationURL, "update must persist the relocated callback pair")
	assert.Equal(t, newLifecycleURL, gotLifecycleURL, "update must preserve lifecycle identity")

	b.stopCancel()
	b.wg.Wait()
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
		stackFixtureLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: stackFixtureLeaseUUID,
			Status: backend.ProvisionStatusReady,
			StackManifest: &manifest.StackManifest{
				Services: map[string]*manifest.Manifest{
					"web": {Image: "nginx:latest"},
					"db":  {Image: "postgres:16"},
				},
			},
			ContainerIDs: []string{"web-c1", "db-c1"},
			ServiceContainers: map[string][]string{
				"web": {"web-c1"},
				"db":  {"db-c1"},
			}},
		},
	})
	b.cfg.HostAddress = "10.0.0.1"
	// newBackendForProvisionTest installs the same strict inventory boundary used
	// after Compose mutations. This fixture starts from an already-running stack,
	// so publish that pre-existing cohort as Docker would instead of relying on
	// point Inspect calls that are not membership evidence.
	mock.ListManagedContainersFn = func(context.Context) ([]ContainerInfo, error) {
		return []ContainerInfo{
			{
				ContainerID: "web-c1", Image: "nginx:latest", Status: "running",
				Ports: map[string]PortBinding{
					"80/tcp": {HostIP: "0.0.0.0", HostPort: "8080"},
				},
			},
			{ContainerID: "db-c1", Image: "postgres:16", Status: "running", Ports: map[string]PortBinding{}},
		}, nil
	}

	info, err := b.GetInfo(context.Background(), stackFixtureLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, info)

	// Should have Host, Services, AND a flattened Instances view.
	// Task 13 unified the LeaseInfo contract: Services is the primary
	// source of truth, Instances is a flattened convenience view in
	// deterministic service-name order (so "db" precedes "web" here).
	assert.Equal(t, "10.0.0.1", info.Host)

	require.Len(t, info.Services, 2)
	require.Len(t, info.Instances, 2, "flattened Instances view should match the total service-instance count")
	assert.Equal(t, "postgres:16", info.Instances[0].Image, "deterministic service-name order: db first")
	assert.Equal(t, "nginx:latest", info.Instances[1].Image, "deterministic service-name order: web second")

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
		stackFixtureLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: stackFixtureLeaseUUID,
			Status: backend.ProvisionStatusReady,
			StackManifest: &manifest.StackManifest{
				Services: map[string]*manifest.Manifest{
					"web": {Image: "nginx"},
					"db":  {Image: "postgres"},
				},
			},
			ContainerIDs: []string{"web-c1", "db-c1"},
			ServiceContainers: map[string][]string{
				"web": {"web-c1"},
				"db":  {"db-c1"},
			}},
		},
	})

	logs, err := b.GetLogs(context.Background(), stackFixtureLeaseUUID, 50)
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
		stackFixtureLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: stackFixtureLeaseUUID,
			Status: backend.ProvisionStatusReady,
			StackManifest: &manifest.StackManifest{
				Services: map[string]*manifest.Manifest{"web": {Image: "nginx"}},
			},
			ContainerIDs: []string{"w1", "w2"},
			ServiceContainers: map[string][]string{
				"web": {"w1", "w2"},
			}},
		},
	})

	logs, err := b.GetLogs(context.Background(), stackFixtureLeaseUUID, 50)
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
			assert.Equal(t, composeProjectName(stackFixtureLeaseUUID), projectName)
			return nil
		},
	}

	items := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
		{SKU: "docker-small", Quantity: 1, ServiceName: "db"},
	}

	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		stackFixtureLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: stackFixtureLeaseUUID,
			Tenant:   "tenant-a",
			Status:   backend.ProvisionStatusReady,
			Quantity: 2,
			StackManifest: &manifest.StackManifest{
				Services: map[string]*manifest.Manifest{
					"web": {Image: "nginx"},
					"db":  {Image: "postgres"},
				},
			},
			ContainerIDs: []string{"web-c1", "db-c1"},
			ServiceContainers: map[string][]string{
				"web": {"web-c1"},
				"db":  {"db-c1"},
			},
			Items: items},
		},
	})
	b.compose = composeMock
	b.volumes = vm
	installReadyRuntimeProofForTest(t, b, stackFixtureLeaseUUID)
	// Pre-allocate with service-aware IDs.
	_ = b.pool.TryAllocate(stackFixtureLeaseUUID+"-web-0", "docker-small", "tenant-a")
	_ = b.pool.TryAllocate(stackFixtureLeaseUUID+"-db-0", "docker-small", "tenant-a")

	err := b.Deprovision(context.Background(), stackFixtureLeaseUUID)
	require.NoError(t, err)

	// Compose Down was called instead of individual RemoveContainer.
	assert.True(t, downCalled, "compose down should be called for stack deprovision")

	// Service-aware volumes destroyed.
	slices.Sort(destroyedVols)
	assert.Equal(t, []string{
		canonicalVolumeName(stackFixtureLeaseUUID, "db", 0),
		canonicalVolumeName(stackFixtureLeaseUUID, "web", 0),
	}, destroyedVols)

	// Provision removed.
	b.provisionsMu.RLock()
	_, exists := b.provisions[stackFixtureLeaseUUID]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)

	// Resources freed.
	stats := b.pool.Stats()
	assert.Equal(t, 0, stats.AllocationCount)
}

// --- Compose failure tests ---

func TestStackProvision_ComposeUpFailurePreservesRecoveryAuthority(t *testing.T) {
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

	callbackReceived := make(chan struct{}, 1)
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		callbackReceived <- struct{}{}
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, nil)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	b.compose = composeMock

	req := newStackProvisionRequest(stackFixtureLeaseUUID, "tenant-a", items, payload)
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL)

	err := b.Provision(context.Background(), req)
	require.NoError(t, err)
	awaitProvisionWorkerQuiescence(t, b, stackFixtureLeaseUUID)
	select {
	case <-callbackReceived:
		t.Fatal("post-effect Compose Up error published a terminal callback")
	default:
	}

	// Compose Down should be called for cleanup.
	mu.Lock()
	assert.True(t, downCalled, "compose down should be called on up failure")
	mu.Unlock()

	// Compose Up crossed an external effect boundary. Its error cannot prove
	// that Docker published nothing, so exact recovery authority must survive.
	b.provisionsMu.RLock()
	prov := b.provisions[stackFixtureLeaseUUID]
	assert.Equal(t, backend.ProvisionStatusProvisioning, prov.Status)
	b.provisionsMu.RUnlock()
	intents, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Equal(t, shared.OperationExecutionStarted, intents[0].ExecutionPhase())

	// The Started operation continues to own its reserved capacity until
	// recovery proves Ready or exact absence.
	stats := b.pool.Stats()
	assert.Equal(t, 2, stats.AllocationCount)
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
		// The fallback re-discovers by label (ENG-647); an empty listing keeps the
		// recorded ContainerIDs as this test's subject.
		ListManagedContainersFn: func(_ context.Context) ([]ContainerInfo, error) { return nil, nil },
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
		stackFixtureLeaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: stackFixtureLeaseUUID,
			Tenant:       "tenant-a",
			ProviderUUID: nominalDockerProviderUUID,
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			StackManifest: &manifest.StackManifest{
				Services: map[string]*manifest.Manifest{
					"web": {Image: "nginx"},
					"db":  {Image: "postgres"},
				},
			},
			ContainerIDs: []string{"web-c1", "db-c1"},
			ServiceContainers: map[string][]string{
				"web": {"web-c1"},
				"db":  {"db-c1"},
			},
			Items: items},
		},
	})
	b.compose = composeMock
	b.volumes = vm
	installReadyRuntimeProofForTest(t, b, stackFixtureLeaseUUID)
	mock.InspectContainerFn = func(_ context.Context, containerID string) (*ContainerInfo, error) {
		b.provisionsMu.RLock()
		projection := b.provisions[stackFixtureLeaseUUID]
		info := &ContainerInfo{
			ContainerID: containerID, LeaseUUID: stackFixtureLeaseUUID,
			BackendName: b.cfg.Name, Tenant: projection.Tenant, ProviderUUID: projection.ProviderUUID,
			CallbackURL: projection.CallbackURL, LifecycleCallbackURL: projection.LifecycleCallbackURL,
		}
		b.provisionsMu.RUnlock()
		return info, nil
	}
	_ = b.pool.TryAllocate(stackFixtureLeaseUUID+"-web-0", "docker-small", "tenant-a")
	_ = b.pool.TryAllocate(stackFixtureLeaseUUID+"-db-0", "docker-small", "tenant-a")

	err := b.Deprovision(context.Background(), stackFixtureLeaseUUID)
	require.NoError(t, err)

	// Both containers should be removed individually as fallback.
	mu.Lock()
	assert.True(t, removedContainers["web-c1"], "web container should be removed individually")
	assert.True(t, removedContainers["db-c1"], "db container should be removed individually")
	mu.Unlock()

	// Provision removed.
	b.provisionsMu.RLock()
	_, exists := b.provisions[stackFixtureLeaseUUID]
	b.provisionsMu.RUnlock()
	assert.False(t, exists)

	// Resources freed.
	stats := b.pool.Stats()
	assert.Equal(t, 0, stats.AllocationCount)
}

// --- Stack recoverState tests ---

func TestRecoverState_StackUnprovenCohortDoesNotMintCapacityAuthority(t *testing.T) {
	now := time.Now()

	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{
					ContainerID:   "web-c1",
					LeaseUUID:     stackFixtureLeaseUUID,
					Tenant:        "tenant-a",
					ProviderUUID:  nominalDockerProviderUUID,
					SKU:           "docker-small",
					ServiceName:   "web",
					InstanceIndex: 0,
					Image:         "nginx:latest",
					Status:        "running",
					CreatedAt:     now,
					CustomDomain:  "foo.example.com",
				},
				{
					ContainerID:   "db-c1",
					LeaseUUID:     stackFixtureLeaseUUID,
					Tenant:        "tenant-a",
					ProviderUUID:  nominalDockerProviderUUID,
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
	prov := b.provisions[stackFixtureLeaseUUID]
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

	// Per-service CustomDomain must be restored from container labels.
	// This seeds the reconciler's downstream label-emission path; without
	// it a refactor to recover.go could silently break custom-domain
	// routing for tenants. The reconciler tests assume
	// prov.Items[*].CustomDomain is already populated, so they don't
	// catch this either.
	webItem := itemMap["web"]
	assert.Equal(t, "foo.example.com", webItem.CustomDomain,
		"recover.go must restore per-service CustomDomain from container labels")
	dbItem := itemMap["db"]
	assert.Equal(t, "", dbItem.CustomDomain,
		"services without a CustomDomain label must restore with an empty CustomDomain")

	// Container labels can rebuild the observational projection, but they do not
	// mint an immutable resource snapshot. A cold empty pool therefore omits the
	// unproven cohort; existing allocations are preserved on a live reset.
	stats := b.pool.Stats()
	assert.Equal(t, 0, stats.AllocationCount)
}

func TestRecoverState_StackMultiInstance(t *testing.T) {
	now := time.Now()

	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{
					ContainerID:   "web-c0",
					LeaseUUID:     stackFixtureLeaseUUID,
					Tenant:        "tenant-a",
					ProviderUUID:  nominalDockerProviderUUID,
					SKU:           "docker-small",
					ServiceName:   "web",
					InstanceIndex: 0,
					Image:         "nginx:latest",
					Status:        "running",
					CreatedAt:     now,
				},
				{
					ContainerID:   "web-c1",
					LeaseUUID:     stackFixtureLeaseUUID,
					Tenant:        "tenant-a",
					ProviderUUID:  nominalDockerProviderUUID,
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

	prov := b.provisions[stackFixtureLeaseUUID]
	require.NotNil(t, prov)

	// Two containers for the same service should produce one item with quantity 2.
	require.Len(t, prov.Items, 1)
	assert.Equal(t, "web", prov.Items[0].ServiceName)
	assert.Equal(t, 2, prov.Items[0].Quantity)
	assert.Len(t, prov.ServiceContainers["web"], 2)
}

// --- Coverage lifted from Task 16 bulk-delete of legacy-fixture tests ---
//
// The following tests preserve coverage that was unique to the deleted
// legacy-shape suite — i.e. semantic behaviour not already exercised by
// the existing stack_test.go suite or the post-Task-15 baseline tests.
// Each test is intentionally minimal: a 1-service stack fixture plus a
// focused assertion on the one behaviour we're preserving.

// TestStackRestart_PreservesCustomDomainInItems lifts a slice of the
// CustomDomain-threading coverage from the deleted
// TestRestart_LegacyPropagatesCustomDomainFromProvItems. The fixture uses the
// minimal real ingress path so strict inventory observes the same custom-domain
// label as Docker. The focused invariant remains that a Restart cycle must not
// lose the CustomDomain off prov.Items.
func TestStackRestart_PreservesCustomDomainInItems(t *testing.T) {
	const customDomain = "foo.example.com"
	stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
		manifest.DefaultServiceName: {
			Image: "nginx:latest",
			Ports: map[string]manifest.PortConfig{"80/tcp": {}},
		},
	}}
	items := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName, CustomDomain: customDomain},
	}

	provisions := map[string]*provision{
		stackMaintenanceLeaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:         stackMaintenanceLeaseUUID,
			Tenant:            "tenant-a",
			ProviderUUID:      nominalDockerProviderUUID,
			SKU:               "docker-small",
			Status:            backend.ProvisionStatusReady,
			Quantity:          1,
			Items:             items,
			StackManifest:     stack,
			ServiceContainers: map[string][]string{manifest.DefaultServiceName: {"old-c1"}},
			ContainerIDs:      []string{"old-c1"},
		}},
	}

	composeMock := &mockComposeExecutor{
		UpFn: func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
			return nil
		},
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{
				{ID: "new-c1", Service: manifest.DefaultServiceName, State: "running"},
			}, nil
		},
		DownFn: func(ctx context.Context, projectName string, timeout time.Duration) error {
			return nil
		},
	}
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, image string, t time.Duration) error { return nil },
		InspectContainerFn: func(ctx context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: id, Status: "running"}, nil
		},
		RemoveContainerFn: func(ctx context.Context, id string) error { return nil },
	}
	installStackStrictCohortInventory(t, mock, composeMock)

	callbackReceived := make(chan struct{})
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	b.cfg.Ingress = IngressConfig{
		Enabled: true, WildcardDomain: "barney0.manifest0.net", Entrypoint: "websecure",
	}
	operationID := mustDockerOperationID("123e4567-e89b-42d3-a456-426614174000")
	operationURL := callbackServer.URL + "?operation_id=" + operationID.String()
	lifecycleURL := callbackServer.URL + "?lifecycle_id=" + operationID.String()
	seedStackMaintenanceAuthority(
		t, b, stackMaintenanceLeaseUUID, stack, items,
		operationID, operationURL, lifecycleURL, callbackServer.Client(),
	)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	require.NoError(t, b.Restart(context.Background(), backend.RestartRequest{MaintenanceID: newTestMaintenanceID(t),
		LeaseUUID:   stackMaintenanceLeaseUUID,
		CallbackURL: lifecycleURL,
	}))
	awaitStackMaintenanceCallback(t, b, callbackReceived)

	b.provisionsMu.RLock()
	prov := b.provisions[stackMaintenanceLeaseUUID]
	require.NotNil(t, prov)
	preserved := prov.Items[0].CustomDomain
	b.provisionsMu.RUnlock()

	assert.Equal(t, customDomain, preserved,
		"Restart must preserve prov.Items[*].CustomDomain — downstream label-emission depends on it")
}

// TestStackUpdate_ComposeUpFailurePreservesRecoveryAuthority proves that a raw
// Compose error cannot authorize either rollback or terminal failure. Compose
// may have published some or all of the target cohort before returning the
// error, so only strict inventory recovery may settle the Started generation.
func TestStackUpdate_ComposeUpFailurePreservesRecoveryAuthority(t *testing.T) {
	payload := validStackManifestJSON(map[string]string{
		manifest.DefaultServiceName: "nginx:latest",
	})
	stack, err := manifest.ParsePayload(payload)
	require.NoError(t, err)

	provisions := map[string]*provision{
		stackMaintenanceLeaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:    stackMaintenanceLeaseUUID,
			Tenant:       "tenant-a",
			ProviderUUID: nominalDockerProviderUUID,
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			Quantity:     1,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName},
			},
			StackManifest:     stack,
			ServiceContainers: map[string][]string{manifest.DefaultServiceName: {"old-c1"}},
			ContainerIDs:      []string{"old-c1"},
		}},
	}

	upCalls := 0
	composeMock := &mockComposeExecutor{
		UpFn: func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
			upCalls++
			return fmt.Errorf("simulated compose up failure (attempt %d)", upCalls)
		},
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return nil, nil
		},
		DownFn: func(ctx context.Context, projectName string, timeout time.Duration) error {
			return nil
		},
	}
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, image string, t time.Duration) error { return nil },
		InspectContainerFn: func(ctx context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: id, Status: "running"}, nil
		},
		RemoveContainerFn: func(ctx context.Context, id string) error { return nil },
		ContainerLogsFn:   func(ctx context.Context, id string, tail int) (string, error) { return "", nil },
	}
	installStackStrictCohortInventory(t, mock, composeMock)

	var callbackPayload backend.CallbackPayload
	callbackReceived := make(chan struct{})
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&callbackPayload)
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
	operationID := mustDockerOperationID("6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	operationURL := callbackServer.URL + "?operation_id=" + operationID.String()
	lifecycleURL := callbackServer.URL + "?lifecycle_id=" + operationID.String()
	seedStackMaintenanceAuthority(
		t, b, stackMaintenanceLeaseUUID, stack, provisions[stackMaintenanceLeaseUUID].Items,
		operationID, operationURL, lifecycleURL, callbackServer.Client(),
	)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	require.NoError(t, b.Update(context.Background(), backend.UpdateRequest{MaintenanceID: newTestMaintenanceID(t),
		LeaseUUID:   stackMaintenanceLeaseUUID,
		CallbackURL: lifecycleURL,
		Payload:     payload,
	}))
	require.Eventually(t, func() bool {
		intent, found, err := b.maintenanceSettlement.GetMaintenanceIntent(stackMaintenanceLeaseUUID)
		return err == nil && found &&
			intent.ExecutionPhase() == shared.MaintenanceExecutionStarted &&
			!b.actorOwnsMaintenance(stackMaintenanceLeaseUUID, intent.MaintenanceID())
	}, 5*time.Second, time.Millisecond,
		"the exact Started update must hand off from the live worker to recovery")
	select {
	case <-callbackReceived:
		t.Fatal("an ambiguous Compose result must preserve callback settlement for recovery")
	default:
	}

	assert.Equal(t, 1, upCalls,
		"an ambiguous update must not perform a second, potentially destructive mutation")
	b.provisionsMu.RLock()
	prov := b.provisions[stackMaintenanceLeaseUUID]
	status := prov.Status
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusUpdating, status,
		"post-effect ambiguity must remain non-terminal until strict recovery settles it")
}

// TestStackRestart_AmbiguousFailureDoesNotManufactureDiagnostics proves that
// an unclassified Compose result cannot overwrite the last terminal evidence.
func TestStackRestart_AmbiguousFailureDoesNotManufactureDiagnostics(t *testing.T) {
	payload := validStackManifestJSON(map[string]string{
		manifest.DefaultServiceName: "nginx:latest",
	})
	stack, err := manifest.ParsePayload(payload)
	require.NoError(t, err)

	provisions := map[string]*provision{
		stackMaintenanceLeaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:    stackMaintenanceLeaseUUID,
			Tenant:       "tenant-a",
			ProviderUUID: nominalDockerProviderUUID,
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			Quantity:     1,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName},
			},
			StackManifest:     stack,
			ServiceContainers: map[string][]string{manifest.DefaultServiceName: {"old-c1"}},
			ContainerIDs:      []string{"old-c1"},
			LastError:         "previous terminal diagnostic",
			FailCount:         7,
		}},
	}

	upCalls := 0
	composeMock := &mockComposeExecutor{
		UpFn: func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
			upCalls++
			return fmt.Errorf("simulated compose up failure")
		},
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{
				{ID: "old-c1", Service: manifest.DefaultServiceName, State: "running"},
			}, nil
		},
		DownFn: func(ctx context.Context, projectName string, timeout time.Duration) error {
			return nil
		},
	}
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, image string, t time.Duration) error { return nil },
		InspectContainerFn: func(ctx context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: id, Status: "running"}, nil
		},
		RemoveContainerFn: func(ctx context.Context, id string) error { return nil },
	}
	installStackStrictCohortInventory(t, mock, composeMock)

	callbackReceived := make(chan struct{})
	callbackServer := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	operationID := mustDockerOperationID("123e4567-e89b-42d3-a456-426614174000")
	operationURL := callbackServer.URL + "?operation_id=" + operationID.String()
	lifecycleURL := callbackServer.URL + "?lifecycle_id=" + operationID.String()
	seedStackMaintenanceAuthority(
		t, b, stackMaintenanceLeaseUUID, stack, provisions[stackMaintenanceLeaseUUID].Items,
		operationID, operationURL, lifecycleURL, callbackServer.Client(),
	)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	require.NoError(t, b.Restart(context.Background(), backend.RestartRequest{MaintenanceID: newTestMaintenanceID(t),
		LeaseUUID:   stackMaintenanceLeaseUUID,
		CallbackURL: lifecycleURL,
	}))
	require.Eventually(t, func() bool {
		intent, found, readErr := b.maintenanceSettlement.GetMaintenanceIntent(stackMaintenanceLeaseUUID)
		return readErr == nil && found &&
			intent.ExecutionPhase() == shared.MaintenanceExecutionStarted &&
			!b.actorOwnsMaintenance(stackMaintenanceLeaseUUID, intent.MaintenanceID())
	}, 5*time.Second, time.Millisecond,
		"the exact Started restart must hand off from the live worker to recovery")
	select {
	case <-callbackReceived:
		t.Fatal("an ambiguous Compose result must not emit a terminal callback")
	default:
	}

	b.provisionsMu.RLock()
	prov := b.provisions[stackMaintenanceLeaseUUID]
	b.provisionsMu.RUnlock()

	assert.Equal(t, backend.ProvisionStatusRestarting, prov.Status,
		"post-effect ambiguity must remain non-terminal until strict recovery settles it")
	assert.Equal(t, "previous terminal diagnostic", prov.LastError,
		"ambiguity is not terminal evidence and must not replace the last exact diagnostic")
	assert.Equal(t, 7, prov.FailCount,
		"ambiguity is not a proven failure and must not increment the terminal failure count")
	assert.Equal(t, 1, upCalls, "ambiguous restart must not attempt an unsafe rollback")
}
