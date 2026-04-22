package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	networktypes "github.com/docker/docker/api/types/network"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// mockVolumeManager implements volumeManager for testing. Unlike mockDockerClient,
// methods default to sensible no-ops (not panics) since most tests don't care
// about volumes. Tests that need to observe volume calls set the Fn fields.
type mockVolumeManager struct {
	CreateFn   func(ctx context.Context, id string, sizeMB int64) (string, bool, error)
	DestroyFn  func(ctx context.Context, id string) error
	ListFn     func() ([]string, error)
	ValidateFn func() error

	// defaultDir is returned by Create when CreateFn is nil.
	// Set this to t.TempDir() in tests that need real paths.
	defaultDir string
}

func (m *mockVolumeManager) Create(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
	if m.CreateFn != nil {
		return m.CreateFn(ctx, id, sizeMB)
	}
	return m.defaultDir, true, nil
}

func (m *mockVolumeManager) Destroy(ctx context.Context, id string) error {
	if m.DestroyFn != nil {
		return m.DestroyFn(ctx, id)
	}
	return nil
}

func (m *mockVolumeManager) List() ([]string, error) {
	if m.ListFn != nil {
		return m.ListFn()
	}
	return nil, nil
}

func (m *mockVolumeManager) Validate() error {
	if m.ValidateFn != nil {
		return m.ValidateFn()
	}
	return nil
}

// mockDockerClient implements dockerClient for testing. Each method delegates to
// the corresponding Fn field; an unexpected call (nil Fn) panics so tests fail
// loudly rather than silently returning zero values.
type mockDockerClient struct {
	PingFn                       func(ctx context.Context) error
	DaemonInfoFn                 func(ctx context.Context) (DaemonSecurityInfo, error)
	CloseFn                      func() error
	PullImageFn                  func(ctx context.Context, imageName string, timeout time.Duration) error
	InspectImageFn               func(ctx context.Context, imageName string) (*ImageInfo, error)
	CreateContainerFn            func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error)
	StartContainerFn             func(ctx context.Context, containerID string, timeout time.Duration) error
	StopContainerFn              func(ctx context.Context, containerID string, timeout time.Duration) error
	RenameContainerFn            func(ctx context.Context, containerID string, newName string) error
	RemoveContainerFn            func(ctx context.Context, containerID string) error
	InspectContainerFn           func(ctx context.Context, containerID string) (*ContainerInfo, error)
	ContainerLogsFn              func(ctx context.Context, containerID string, tail int) (string, error)
	ListManagedContainersFn      func(ctx context.Context) ([]ContainerInfo, error)
	EnsureTenantNetworkFn        func(ctx context.Context, tenant string) (string, error)
	RemoveTenantNetworkIfEmptyFn func(ctx context.Context, tenant string) error
	ListManagedNetworksFn        func(ctx context.Context) ([]networktypes.Inspect, error)
	ResolveImageUserFn           func(ctx context.Context, imageName string, userOverride string) (int, int, error)
	DetectVolumeOwnerFn          func(ctx context.Context, imageName string, volumePaths []string) (int, int, error)
	DetectWritablePathsFn        func(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error)
	ExtractImageContentFn        func(ctx context.Context, imageName string, paths []string, destDir string, maxBytes int64) map[string]error
	ContainerEventsFn            func(ctx context.Context) (<-chan ContainerEvent, <-chan error)
}

func (m *mockDockerClient) Ping(ctx context.Context) error {
	if m.PingFn != nil {
		return m.PingFn(ctx)
	}
	panic("unexpected call to Ping")
}

func (m *mockDockerClient) DaemonInfo(ctx context.Context) (DaemonSecurityInfo, error) {
	if m.DaemonInfoFn != nil {
		return m.DaemonInfoFn(ctx)
	}
	// Default: return sensible values so existing tests don't break.
	return DaemonSecurityInfo{
		StorageDriver:     "overlay2",
		BackingFilesystem: "xfs",
		SecurityOptions:   []string{"name=seccomp,profile=default"},
		IPv4Forwarding:    true,
	}, nil
}

func (m *mockDockerClient) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	panic("unexpected call to Close")
}

func (m *mockDockerClient) PullImage(ctx context.Context, imageName string, timeout time.Duration) error {
	if m.PullImageFn != nil {
		return m.PullImageFn(ctx, imageName, timeout)
	}
	panic("unexpected call to PullImage")
}

func (m *mockDockerClient) InspectImage(ctx context.Context, imageName string) (*ImageInfo, error) {
	if m.InspectImageFn != nil {
		return m.InspectImageFn(ctx, imageName)
	}
	// Default: return empty volumes so existing tests don't break.
	return &ImageInfo{Volumes: map[string]struct{}{}}, nil
}

func (m *mockDockerClient) CreateContainer(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
	if m.CreateContainerFn != nil {
		return m.CreateContainerFn(ctx, params, timeout)
	}
	panic("unexpected call to CreateContainer")
}

func (m *mockDockerClient) StartContainer(ctx context.Context, containerID string, timeout time.Duration) error {
	if m.StartContainerFn != nil {
		return m.StartContainerFn(ctx, containerID, timeout)
	}
	panic("unexpected call to StartContainer")
}

func (m *mockDockerClient) StopContainer(ctx context.Context, containerID string, timeout time.Duration) error {
	if m.StopContainerFn != nil {
		return m.StopContainerFn(ctx, containerID, timeout)
	}
	return nil
}

func (m *mockDockerClient) RenameContainer(ctx context.Context, containerID string, newName string) error {
	if m.RenameContainerFn != nil {
		return m.RenameContainerFn(ctx, containerID, newName)
	}
	return nil // default: rename succeeds silently
}

func (m *mockDockerClient) RemoveContainer(ctx context.Context, containerID string) error {
	if m.RemoveContainerFn != nil {
		return m.RemoveContainerFn(ctx, containerID)
	}
	panic("unexpected call to RemoveContainer")
}

func (m *mockDockerClient) InspectContainer(ctx context.Context, containerID string) (*ContainerInfo, error) {
	if m.InspectContainerFn != nil {
		return m.InspectContainerFn(ctx, containerID)
	}
	panic("unexpected call to InspectContainer")
}

func (m *mockDockerClient) ContainerLogs(ctx context.Context, containerID string, tail int) (string, error) {
	if m.ContainerLogsFn != nil {
		return m.ContainerLogsFn(ctx, containerID, tail)
	}
	panic("unexpected call to ContainerLogs")
}

func (m *mockDockerClient) ListManagedContainers(ctx context.Context) ([]ContainerInfo, error) {
	if m.ListManagedContainersFn != nil {
		return m.ListManagedContainersFn(ctx)
	}
	panic("unexpected call to ListManagedContainers")
}

func (m *mockDockerClient) EnsureTenantNetwork(ctx context.Context, tenant string) (string, error) {
	if m.EnsureTenantNetworkFn != nil {
		return m.EnsureTenantNetworkFn(ctx, tenant)
	}
	panic("unexpected call to EnsureTenantNetwork")
}

func (m *mockDockerClient) RemoveTenantNetworkIfEmpty(ctx context.Context, tenant string) error {
	if m.RemoveTenantNetworkIfEmptyFn != nil {
		return m.RemoveTenantNetworkIfEmptyFn(ctx, tenant)
	}
	panic("unexpected call to RemoveTenantNetworkIfEmpty")
}

func (m *mockDockerClient) ResolveImageUser(ctx context.Context, imageName string, userOverride string) (int, int, error) {
	if m.ResolveImageUserFn != nil {
		return m.ResolveImageUserFn(ctx, imageName, userOverride)
	}
	return 0, 0, nil // default: root
}

func (m *mockDockerClient) ListManagedNetworks(ctx context.Context) ([]networktypes.Inspect, error) {
	if m.ListManagedNetworksFn != nil {
		return m.ListManagedNetworksFn(ctx)
	}
	panic("unexpected call to ListManagedNetworks")
}

func (m *mockDockerClient) DetectVolumeOwner(ctx context.Context, imageName string, volumePaths []string) (int, int, error) {
	if m.DetectVolumeOwnerFn != nil {
		return m.DetectVolumeOwnerFn(ctx, imageName, volumePaths)
	}
	return 0, 0, nil // default: root (auto-detect path entered but produces no override)
}

func (m *mockDockerClient) DetectWritablePaths(ctx context.Context, imageName string, uid int, candidateParents []string) ([]string, error) {
	if m.DetectWritablePathsFn != nil {
		return m.DetectWritablePathsFn(ctx, imageName, uid, candidateParents)
	}
	return nil, nil // default: no writable paths detected
}

func (m *mockDockerClient) ExtractImageContent(ctx context.Context, imageName string, paths []string, destDir string, maxBytes int64) map[string]error {
	if m.ExtractImageContentFn != nil {
		return m.ExtractImageContentFn(ctx, imageName, paths, destDir, maxBytes)
	}
	return nil // default: extraction succeeds silently
}

func (m *mockDockerClient) ContainerEvents(ctx context.Context) (<-chan ContainerEvent, <-chan error) {
	if m.ContainerEventsFn != nil {
		return m.ContainerEventsFn(ctx)
	}
	// Default: return channels that block until context is canceled (no-op).
	ch := make(chan ContainerEvent)
	errCh := make(chan error)
	go func() {
		<-ctx.Done()
		close(ch)
		close(errCh)
	}()
	return ch, errCh
}

// newBackendForTest creates a Backend suitable for unit testing. It wires up a
// mockDockerClient, a real ResourcePool, and pre-populates the provisions map.
// Network isolation is disabled by default so tests don't need to stub network
// methods unless they explicitly opt in.
func newBackendForTest(mock *mockDockerClient, provisions map[string]*provision) *Backend {
	cfg := DefaultConfig()
	cfg.NetworkIsolation = ptrBool(false)

	pool := shared.NewResourcePool(
		cfg.TotalCPUCores,
		cfg.TotalMemoryMB,
		cfg.TotalDiskMB,
		cfg.GetSKUProfile,
		nil,
	)

	provs := make(map[string]*provision)
	for k, v := range provisions {
		provs[k] = v
	}

	stopCtx, stopCancel := context.WithCancel(context.Background())

	b := &Backend{
		cfg:        cfg,
		docker:     mock,
		compose:    &mockComposeExecutor{},
		pool:       pool,
		volumes:    &noopVolumeManager{},
		logger:     slog.Default(),
		provisions: provs,
		actors:     make(map[string]*leaseActor),
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
	}
	b.callbackSender = shared.NewCallbackSender(shared.CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     b.logger,
		StopCtx:    b.stopCtx,
	})
	return b
}

func TestRecoverState(t *testing.T) {
	now := time.Now()

	t.Run("no containers no prior provisions", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return nil, nil
			},
		}
		b := newBackendForTest(mock, nil)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		assert.Len(t, b.provisions, 0)
	})

	t.Run("single running container", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:   "c1",
						LeaseUUID:     "lease-1",
						Tenant:        "tenant-a",
						ProviderUUID:  "prov-1",
						SKU:           "docker-small",
						InstanceIndex: 0,
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

		require.Len(t, b.provisions, 1)
		prov := b.provisions["lease-1"]
		require.NotNil(t, prov)
		assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
		assert.Equal(t, []string{"c1"}, prov.ContainerIDs)
		assert.Equal(t, "tenant-a", prov.Tenant)
		assert.Equal(t, "nginx:latest", prov.Image)
	})

	t.Run("multiple containers same lease UUID", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:   "c1",
						LeaseUUID:     "lease-1",
						Tenant:        "tenant-a",
						ProviderUUID:  "prov-1",
						SKU:           "docker-small",
						InstanceIndex: 0,
						Image:         "nginx:latest",
						Status:        "running",
						CreatedAt:     now,
					},
					{
						ContainerID:   "c2",
						LeaseUUID:     "lease-1",
						Tenant:        "tenant-a",
						ProviderUUID:  "prov-1",
						SKU:           "docker-small",
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

		require.Len(t, b.provisions, 1)
		prov := b.provisions["lease-1"]
		require.NotNil(t, prov)
		assert.Len(t, prov.ContainerIDs, 2)
		assert.Equal(t, 2, prov.Quantity)
	})

	t.Run("containers from different leases", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:   "c1",
						LeaseUUID:     "lease-1",
						Tenant:        "tenant-a",
						ProviderUUID:  "prov-1",
						SKU:           "docker-small",
						InstanceIndex: 0,
						Image:         "nginx:latest",
						Status:        "running",
						CreatedAt:     now,
					},
					{
						ContainerID:   "c2",
						LeaseUUID:     "lease-2",
						Tenant:        "tenant-b",
						ProviderUUID:  "prov-1",
						SKU:           "docker-medium",
						InstanceIndex: 0,
						Image:         "redis:7",
						Status:        "running",
						CreatedAt:     now,
					},
				}, nil
			},
		}
		b := newBackendForTest(mock, nil)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		require.Len(t, b.provisions, 2)
		assert.NotNil(t, b.provisions["lease-1"])
		assert.NotNil(t, b.provisions["lease-2"])
	})

	t.Run("exited container marks failed", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:   "c1",
						LeaseUUID:     "lease-1",
						Tenant:        "tenant-a",
						ProviderUUID:  "prov-1",
						SKU:           "docker-small",
						InstanceIndex: 0,
						Image:         "nginx:latest",
						Status:        "exited",
						CreatedAt:     now,
					},
				}, nil
			},
			InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
				return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
			},
			ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
				return "segfault", nil
			},
		}
		b := newBackendForTest(mock, nil)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		prov := b.provisions["lease-1"]
		require.NotNil(t, prov)
		assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
		// Cold-start: FailCount should be incremented from the label value (0 → 1).
		assert.Equal(t, 1, prov.FailCount)
		assert.Contains(t, prov.LastError, "container exited unexpectedly")
		assert.Contains(t, prov.LastError, "exit_code=1")
	})

	t.Run("in-flight provision preserved without containers", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return nil, nil
			},
		}
		existing := map[string]*provision{
			"lease-1": {
				LeaseUUID: "lease-1",
				Tenant:    "tenant-a",
				Status:    backend.ProvisionStatusProvisioning,
				CreatedAt: now,
			},
		}
		b := newBackendForTest(mock, existing)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		prov := b.provisions["lease-1"]
		require.NotNil(t, prov)
		assert.Equal(t, backend.ProvisionStatusProvisioning, prov.Status)
	})

	t.Run("failed provision preserved without containers", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return nil, nil
			},
		}
		existing := map[string]*provision{
			"lease-1": {
				LeaseUUID: "lease-1",
				Tenant:    "tenant-a",
				Status:    backend.ProvisionStatusFailed,
				FailCount: 2,
				CreatedAt: now,
			},
		}
		b := newBackendForTest(mock, existing)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		prov := b.provisions["lease-1"]
		require.NotNil(t, prov)
		assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
		assert.Equal(t, 2, prov.FailCount)
	})

	t.Run("ready provision without containers dropped", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return nil, nil
			},
		}
		existing := map[string]*provision{
			"lease-1": {
				LeaseUUID:    "lease-1",
				Tenant:       "tenant-a",
				Status:       backend.ProvisionStatusReady,
				ContainerIDs: []string{"c1"},
				CreatedAt:    now,
			},
		}
		b := newBackendForTest(mock, existing)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		assert.NotContains(t, b.provisions, "lease-1")
	})

	t.Run("ListContainers error", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return nil, fmt.Errorf("docker daemon unreachable")
			},
		}
		existing := map[string]*provision{
			"lease-1": {
				LeaseUUID: "lease-1",
				Status:    backend.ProvisionStatusReady,
			},
		}
		b := newBackendForTest(mock, existing)

		err := b.recoverState(context.Background())
		require.Error(t, err)

		// Provisions should be unchanged since recoverState errored out early.
		assert.NotNil(t, b.provisions["lease-1"])
	})

	t.Run("unknown SKU container skipped", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:   "c-good",
						LeaseUUID:     "lease-good",
						Tenant:        "tenant-a",
						ProviderUUID:  "prov-1",
						SKU:           "docker-small",
						InstanceIndex: 0,
						Image:         "nginx:latest",
						Status:        "running",
						CreatedAt:     now,
					},
					{
						ContainerID:   "c-bad",
						LeaseUUID:     "lease-bad",
						Tenant:        "tenant-b",
						ProviderUUID:  "prov-1",
						SKU:           "unknown-sku-xyz",
						InstanceIndex: 0,
						Image:         "redis:7",
						Status:        "running",
						CreatedAt:     now,
					},
				}, nil
			},
		}
		b := newBackendForTest(mock, nil)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		// The good container should be recovered; the bad one skipped.
		require.Len(t, b.provisions, 1)
		assert.NotNil(t, b.provisions["lease-good"])
		assert.Nil(t, b.provisions["lease-bad"])
	})

	t.Run("container with health check info", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:   "c1",
						LeaseUUID:     "lease-1",
						Tenant:        "tenant-a",
						ProviderUUID:  "prov-1",
						SKU:           "docker-small",
						InstanceIndex: 0,
						Image:         "nginx:latest",
						Status:        "running",
						Health:        HealthStatusHealthy,
						CreatedAt:     now,
					},
				}, nil
			},
		}
		b := newBackendForTest(mock, nil)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		// recoverState uses the container's Status field (not Health) to
		// determine provision status. A "running" container with health=healthy
		// should still be Ready.
		prov := b.provisions["lease-1"]
		require.NotNil(t, prov)
		assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
	})

	t.Run("resource pool rebuilt from containers", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:   "c1",
						LeaseUUID:     "lease-1",
						Tenant:        "tenant-a",
						ProviderUUID:  "prov-1",
						SKU:           "docker-small",
						InstanceIndex: 0,
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

		// docker-small profile: CPU=0.5, Memory=512, Disk=1024
		stats := b.pool.Stats()
		assert.Equal(t, 0.5, stats.AllocatedCPU)
		assert.Equal(t, int64(512), stats.AllocatedMemoryMB)
		assert.Equal(t, int64(1024), stats.AllocatedDiskMB)
		assert.Equal(t, 1, stats.AllocationCount)
	})

	t.Run("ready to failed transition sets LastError", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:   "c1",
						LeaseUUID:     "lease-1",
						Tenant:        "tenant-a",
						ProviderUUID:  "prov-1",
						SKU:           "docker-small",
						InstanceIndex: 0,
						Image:         "nginx:latest",
						Status:        "exited",
						CreatedAt:     now,
					},
				}, nil
			},
			InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
				return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 137, OOMKilled: true}, nil
			},
			ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
				return "Killed", nil
			},
		}
		// Pre-populate with a "ready" provision so recoverState detects a ready→failed transition.
		existing := map[string]*provision{
			"lease-1": {
				LeaseUUID:    "lease-1",
				Tenant:       "tenant-a",
				Status:       backend.ProvisionStatusReady,
				ContainerIDs: []string{"c1"},
				FailCount:    0,
				CreatedAt:    now,
			},
		}
		b := newBackendForTest(mock, existing)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		// recoverState now hands off the transition to the lease's actor;
		// the SM's Failing → Failed flow gathers diagnostics via an async
		// goroutine. Poll until the expected terminal state lands.
		require.Eventually(t, func() bool {
			b.provisionsMu.RLock()
			defer b.provisionsMu.RUnlock()
			prov := b.provisions["lease-1"]
			return prov != nil && prov.Status == backend.ProvisionStatusFailed &&
				prov.FailCount == 1 &&
				strings.Contains(prov.LastError, "exit_code=137") &&
				strings.Contains(prov.LastError, "oom_killed=true")
		}, 2*time.Second, 10*time.Millisecond)

		b.provisionsMu.RLock()
		prov := b.provisions["lease-1"]
		b.provisionsMu.RUnlock()
		require.NotNil(t, prov)
		assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
		assert.Equal(t, 1, prov.FailCount)
		assert.Contains(t, prov.LastError, "container exited unexpectedly")
		assert.Contains(t, prov.LastError, "exit_code=137")
		assert.Contains(t, prov.LastError, "oom_killed=true")
	})

	t.Run("diagnostics fallback when inspect fails in recovery", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:   "c1",
						LeaseUUID:     "lease-1",
						Tenant:        "tenant-a",
						ProviderUUID:  "prov-1",
						SKU:           "docker-small",
						InstanceIndex: 0,
						Image:         "nginx:latest",
						Status:        "exited",
						CreatedAt:     now,
					},
				}, nil
			},
			InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
				return nil, fmt.Errorf("docker daemon unreachable")
			},
		}
		b := newBackendForTest(mock, nil)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		prov := b.provisions["lease-1"]
		require.NotNil(t, prov)
		assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
		// When InspectContainer fails, LastError should remain the bare message
		// set inside the lock (no diagnostic enrichment).
		assert.Equal(t, "container exited unexpectedly", prov.LastError)
	})

	t.Run("container missing labels skipped", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID: "c-nolease",
						LeaseUUID:   "",
						SKU:         "docker-small",
						Status:      "running",
					},
					{
						ContainerID: "c-nosku",
						LeaseUUID:   "lease-1",
						SKU:         "",
						Status:      "running",
					},
				}, nil
			},
		}
		b := newBackendForTest(mock, nil)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		assert.Len(t, b.provisions, 0)
	})
}

func TestRecoverState_SetsActiveProvisionsGauge(t *testing.T) {
	now := time.Now()

	t.Run("cold start with running containers", func(t *testing.T) {
		// Reset gauge to a known value to detect the change.
		activeProvisions.Set(999)

		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:  "c1",
						LeaseUUID:    "lease-1",
						Tenant:       "tenant-a",
						ProviderUUID: "prov-1",
						SKU:          "docker-small",
						Status:       "running",
						CreatedAt:    now,
					},
					{
						ContainerID:  "c2",
						LeaseUUID:    "lease-2",
						Tenant:       "tenant-b",
						ProviderUUID: "prov-1",
						SKU:          "docker-small",
						Status:       "running",
						CreatedAt:    now,
					},
				}, nil
			},
		}
		b := newBackendForTest(mock, nil)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		assert.Equal(t, float64(2), testutil.ToFloat64(activeProvisions))
	})

	t.Run("mixed ready and failed provisions", func(t *testing.T) {
		activeProvisions.Set(999)

		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:  "c1",
						LeaseUUID:    "lease-1",
						Tenant:       "tenant-a",
						ProviderUUID: "prov-1",
						SKU:          "docker-small",
						Status:       "running",
						CreatedAt:    now,
					},
					{
						ContainerID:  "c2",
						LeaseUUID:    "lease-2",
						Tenant:       "tenant-b",
						ProviderUUID: "prov-1",
						SKU:          "docker-small",
						Status:       "exited",
						CreatedAt:    now,
					},
				}, nil
			},
			InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
				return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
			},
			ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
				return "", nil
			},
		}
		b := newBackendForTest(mock, nil)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		// Only lease-1 is ready; lease-2 is failed.
		assert.Equal(t, float64(1), testutil.ToFloat64(activeProvisions))
	})

	t.Run("no containers sets gauge to zero", func(t *testing.T) {
		activeProvisions.Set(999)

		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return nil, nil
			},
		}
		b := newBackendForTest(mock, nil)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		assert.Equal(t, float64(0), testutil.ToFloat64(activeProvisions))
	})

	t.Run("in-flight provisioning leases not counted", func(t *testing.T) {
		activeProvisions.Set(999)

		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return nil, nil
			},
		}
		existing := map[string]*provision{
			"lease-1": {
				LeaseUUID: "lease-1",
				Status:    backend.ProvisionStatusProvisioning,
				CreatedAt: now,
			},
		}
		b := newBackendForTest(mock, existing)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		// Provisioning leases are preserved but not counted as active.
		assert.Len(t, b.provisions, 1)
		assert.Equal(t, float64(0), testutil.ToFloat64(activeProvisions))
	})
}

func TestCleanupOrphanedNetworks(t *testing.T) {
	now := time.Now()

	t.Run("removes networks for tenants without active provisions", func(t *testing.T) {
		var removedTenants []string
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:  "c1",
						LeaseUUID:    "lease-1",
						Tenant:       "tenant-a",
						ProviderUUID: "prov-1",
						SKU:          "docker-small",
						Status:       "running",
						CreatedAt:    now,
					},
				}, nil
			},
			ListManagedNetworksFn: func(ctx context.Context) ([]networktypes.Inspect, error) {
				return []networktypes.Inspect{
					{
						Name:       "fred-tenant-aaaa",
						Labels:     map[string]string{LabelTenant: "tenant-a"},
						Containers: map[string]networktypes.EndpointResource{"c1": {}},
					},
					{
						Name:       "fred-tenant-bbbb",
						Labels:     map[string]string{LabelTenant: "tenant-b"},
						Containers: map[string]networktypes.EndpointResource{}, // No containers
					},
				}, nil
			},
			RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) error {
				removedTenants = append(removedTenants, tenant)
				return nil
			},
		}

		cfg := DefaultConfig()
		cfg.NetworkIsolation = ptrBool(true)
		pool := shared.NewResourcePool(cfg.TotalCPUCores, cfg.TotalMemoryMB, cfg.TotalDiskMB, cfg.GetSKUProfile, nil)
		stopCtx, stopCancel := context.WithCancel(context.Background())
		defer stopCancel()

		b := &Backend{
			cfg:        cfg,
			docker:     mock,
			pool:       pool,
			volumes:    &noopVolumeManager{},
			logger:     slog.Default(),
			provisions: make(map[string]*provision),
			stopCtx:    stopCtx,
			stopCancel: stopCancel,
		}
		b.callbackSender = shared.NewCallbackSender(shared.CallbackSenderConfig{
			HTTPClient: http.DefaultClient,
			Logger:     b.logger,
			StopCtx:    b.stopCtx,
		})

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		// tenant-b should be removed (no active provisions, no containers)
		assert.Equal(t, []string{"tenant-b"}, removedTenants)
	})

	t.Run("keeps networks with connected containers", func(t *testing.T) {
		var removedTenants []string
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return nil, nil // No managed containers
			},
			ListManagedNetworksFn: func(ctx context.Context) ([]networktypes.Inspect, error) {
				return []networktypes.Inspect{
					{
						Name:   "fred-tenant-cccc",
						Labels: map[string]string{LabelTenant: "tenant-c"},
						// Has containers still connected even though no provisions
						Containers: map[string]networktypes.EndpointResource{"some-container": {}},
					},
				}, nil
			},
			RemoveTenantNetworkIfEmptyFn: func(ctx context.Context, tenant string) error {
				removedTenants = append(removedTenants, tenant)
				return nil
			},
		}

		cfg := DefaultConfig()
		cfg.NetworkIsolation = ptrBool(true)
		pool := shared.NewResourcePool(cfg.TotalCPUCores, cfg.TotalMemoryMB, cfg.TotalDiskMB, cfg.GetSKUProfile, nil)
		stopCtx, stopCancel := context.WithCancel(context.Background())
		defer stopCancel()

		b := &Backend{
			cfg:        cfg,
			docker:     mock,
			pool:       pool,
			volumes:    &noopVolumeManager{},
			logger:     slog.Default(),
			provisions: make(map[string]*provision),
			stopCtx:    stopCtx,
			stopCancel: stopCancel,
		}
		b.callbackSender = shared.NewCallbackSender(shared.CallbackSenderConfig{
			HTTPClient: http.DefaultClient,
			Logger:     b.logger,
			StopCtx:    b.stopCtx,
		})

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		// Network with containers should NOT be removed
		assert.Empty(t, removedTenants)
	})

	t.Run("ListManagedNetworks error is non-fatal", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return nil, nil
			},
			ListManagedNetworksFn: func(ctx context.Context) ([]networktypes.Inspect, error) {
				return nil, fmt.Errorf("docker network list failed")
			},
		}

		cfg := DefaultConfig()
		cfg.NetworkIsolation = ptrBool(true)
		pool := shared.NewResourcePool(cfg.TotalCPUCores, cfg.TotalMemoryMB, cfg.TotalDiskMB, cfg.GetSKUProfile, nil)
		stopCtx, stopCancel := context.WithCancel(context.Background())
		defer stopCancel()

		b := &Backend{
			cfg:        cfg,
			docker:     mock,
			pool:       pool,
			volumes:    &noopVolumeManager{},
			logger:     slog.Default(),
			provisions: make(map[string]*provision),
			stopCtx:    stopCtx,
			stopCancel: stopCancel,
		}
		b.callbackSender = shared.NewCallbackSender(shared.CallbackSenderConfig{
			HTTPClient: http.DefaultClient,
			Logger:     b.logger,
			StopCtx:    b.stopCtx,
		})

		// Should not return error - network cleanup failure is logged, not propagated
		err := b.recoverState(context.Background())
		require.NoError(t, err)
	})
}

func TestRecoverState_Serialized(t *testing.T) {
	// Verify that concurrent recoverState calls are serialized by recoverMu.
	// Use an atomic counter to track the maximum number of concurrent calls
	// inside ListManagedContainers (called inside the critical section).
	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32

	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			n := concurrent.Add(1)
			defer concurrent.Add(-1)

			// Record the peak concurrency observed.
			for {
				cur := maxConcurrent.Load()
				if n <= cur || maxConcurrent.CompareAndSwap(cur, n) {
					break
				}
			}

			// Hold inside the lock long enough for the other goroutine to attempt entry.
			time.Sleep(50 * time.Millisecond)
			return nil, nil
		},
	}
	b := newBackendForTest(mock, nil)

	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			_ = b.recoverState(context.Background())
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(1), maxConcurrent.Load(),
		"recoverState calls should be serialized (max concurrency must be 1)")
}

func TestRecoverState_PersistsDiagnostics(t *testing.T) {
	// Verify that a ready→failed transition detected by recoverState persists
	// diagnostics including container logs.
	dbPath := filepath.Join(t.TempDir(), "recover_diag.db")
	diagStore, err := shared.NewDiagnosticsStore(shared.DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer diagStore.Close()

	now := time.Now()
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{
					ContainerID:   "c1",
					LeaseUUID:     "lease-1",
					Tenant:        "tenant-a",
					ProviderUUID:  "prov-1",
					SKU:           "docker-small",
					InstanceIndex: 0,
					Image:         "nginx:latest",
					Status:        "exited",
					CreatedAt:     now,
				},
			}, nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 137, OOMKilled: true}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "Killed by OOM\n", nil
		},
	}

	existing := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1"},
			FailCount:    0,
			CreatedAt:    now,
		},
	}
	b := newBackendForTest(mock, existing)
	b.diagnosticsStore = diagStore

	err = b.recoverState(context.Background())
	require.NoError(t, err)

	// The SM's Failing → Failed flow runs asynchronously after recoverState
	// hands off the transition. Poll for the diagnostics entry to land.
	require.Eventually(t, func() bool {
		entry, _ := diagStore.Get("lease-1")
		return entry != nil && entry.FailCount == 1 && entry.Logs != nil
	}, 2*time.Second, 10*time.Millisecond, "diagnostics should eventually be persisted")

	// Verify diagnostics were persisted.
	entry, err := diagStore.Get("lease-1")
	require.NoError(t, err)
	require.NotNil(t, entry, "diagnostics should be persisted for failed transition")
	assert.Contains(t, entry.Error, "container exited unexpectedly")
	assert.Contains(t, entry.Error, "exit_code=137")
	assert.Equal(t, 1, entry.FailCount)
	assert.Equal(t, "tenant-a", entry.Tenant)
	assert.Equal(t, "prov-1", entry.ProviderUUID)
	// Logs should be persisted
	require.NotNil(t, entry.Logs)
	assert.Contains(t, entry.Logs["0"], "Killed by OOM")
}

func TestRecoverState_CallbackSanitized(t *testing.T) {
	// Verify that recoverState failure callbacks never include container logs
	// or dynamic data — only the hardcoded errMsgContainerExited.
	secret := "AWS_SECRET_KEY=wJalrXUtnFEMI"
	now := time.Now()

	var payloadMu sync.Mutex
	var callbackPayload backend.CallbackPayload
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		payloadMu.Lock()
		callbackPayload = p
		payloadMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	readPayload := func() backend.CallbackPayload {
		payloadMu.Lock()
		defer payloadMu.Unlock()
		return callbackPayload
	}

	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{
					ContainerID:   "c1",
					LeaseUUID:     "lease-1",
					Tenant:        "tenant-a",
					ProviderUUID:  "prov-1",
					SKU:           "docker-small",
					InstanceIndex: 0,
					Image:         "nginx:latest",
					Status:        "exited",
					CreatedAt:     now,
					CallbackURL:   callbackServer.URL,
				},
			}, nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return secret, nil
		},
	}

	existing := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1"},
			FailCount:    0,
			CreatedAt:    now,
			CallbackURL:  callbackServer.URL,
		},
	}
	b := newBackendForTest(mock, existing)
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)

	err := b.recoverState(context.Background())
	require.NoError(t, err)

	// Callback is now emitted by the SM's Failed.OnEntryFrom(evDiagGathered)
	// entry action after the Failing goroutine completes. Poll for it.
	require.Eventually(t, func() bool {
		return readPayload().Error == errMsgContainerExited
	}, 2*time.Second, 10*time.Millisecond, "Failed callback should eventually arrive")

	// Callback should have hardcoded message only — no secrets.
	payload := readPayload()
	assert.Equal(t, errMsgContainerExited, payload.Error)
	assert.NotContains(t, payload.Error, secret)
	assert.NotContains(t, payload.Error, "exit_code")

	// LastError should contain full diagnostics.
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	require.NotNil(t, prov)
	assert.Contains(t, prov.LastError, "exit_code=1")
	assert.Contains(t, prov.LastError, secret)
}

// The recoverState-side cc62f3b simulation is obsolete: recoverState now
// hands Ready→Failed transitions off to the lease's actor via containerDiedMsg,
// and the SM's Failing.OnExit cancellation is the structural suppression of
// stale Failed callbacks on Deprovision preemption. The real-Deprovision
// test TestConcurrentDeprovisionAndContainerDeath_ExactlyOneCallback
// (lease_actor_test.go) covers the invariant end-to-end.

func TestRecoverState_InFlightReProvisionPreservesFailCount(t *testing.T) {
	// Regression test: recoverState must not overwrite an in-flight
	// PROVISIONING entry with stale container labels.
	//
	// Scenario: a re-provision is in-flight (Status=PROVISIONING, FailCount=2)
	// while old dead containers (FailCount=0 in labels) still exist.
	// recoverState should keep the PROVISIONING entry with its correct FailCount.
	now := time.Now()

	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{
					ContainerID:   "old-dead-container",
					LeaseUUID:     "lease-1",
					Tenant:        "tenant-a",
					ProviderUUID:  "prov-1",
					SKU:           "docker-small",
					InstanceIndex: 0,
					Image:         "nginx:latest",
					Status:        "exited",
					CreatedAt:     now,
					FailCount:     0, // stale: was written before failures accumulated
				},
			}, nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", nil
		},
	}

	// Existing in-memory state: in-flight re-provision with accumulated FailCount.
	existing := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusProvisioning,
			Quantity:     1,
			ContainerIDs: []string{}, // new provision hasn't created containers yet
			FailCount:    2,
			CreatedAt:    now,
		},
	}
	b := newBackendForTest(mock, existing)

	err := b.recoverState(context.Background())
	require.NoError(t, err)

	// The PROVISIONING entry should be preserved with the correct FailCount,
	// not overwritten by the stale container label value (0).
	b.provisionsMu.RLock()
	prov, ok := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()

	require.True(t, ok, "provision should still exist")
	assert.Equal(t, backend.ProvisionStatusProvisioning, prov.Status,
		"should preserve PROVISIONING status, not replace with Failed from dead container")
	assert.Equal(t, 2, prov.FailCount,
		"should preserve in-flight FailCount (2), not stale label value (0)")
}

func TestRecoverState_RepeatedCallPreservesFailCount(t *testing.T) {
	// Regression test: when recoverState runs multiple times against the same
	// dead container, the FailCount must not regress to the stale label value.
	//
	// Scenario: first recoverState detects ready→failed and increments FailCount
	// to 1. A second recoverState call (e.g., from RefreshState) re-reads the
	// container with FailCount=0 in labels. The in-memory value (1) must be
	// preserved.
	now := time.Now()

	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{
					ContainerID:   "dead-container",
					LeaseUUID:     "lease-1",
					Tenant:        "tenant-a",
					ProviderUUID:  "prov-1",
					SKU:           "docker-small",
					InstanceIndex: 0,
					Image:         "nginx:latest",
					Status:        "exited",
					CreatedAt:     now,
					FailCount:     0, // stale label from container creation
				},
			}, nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", nil
		},
	}

	// Existing in-memory state: recoverState already ran once and incremented
	// FailCount to 1. Provision has Status=Failed (transition already detected).
	existing := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusFailed,
			Quantity:     1,
			ContainerIDs: []string{"dead-container"},
			FailCount:    1,
			CreatedAt:    now,
		},
	}
	b := newBackendForTest(mock, existing)

	err := b.recoverState(context.Background())
	require.NoError(t, err)

	b.provisionsMu.RLock()
	prov, ok := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()

	require.True(t, ok, "provision should still exist")
	assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
	assert.Equal(t, 1, prov.FailCount,
		"should preserve in-memory FailCount (1), not regress to stale label value (0)")
}

func TestRecoverState_RestoresManifestFromReleaseStore(t *testing.T) {
	now := time.Now()

	manifest := DockerManifest{
		Image:   "nginx:latest",
		Command: []string{"nginx", "-g", "daemon off;"},
		Env:     map[string]string{"FOO": "bar"},
		Ports:   map[string]PortConfig{"80/tcp": {HostPort: 0}},
	}
	manifestBytes, err := json.Marshal(manifest)
	require.NoError(t, err)

	t.Run("manifest restored on cold start", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "releases.db")
		relStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
		require.NoError(t, err)
		defer relStore.Close()

		// Pre-seed the release store with a manifest for this lease.
		err = relStore.Append("lease-1", shared.Release{
			Manifest:  manifestBytes,
			Image:     manifest.Image,
			Status:    "active",
			CreatedAt: now,
		})
		require.NoError(t, err)

		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:   "c1",
						LeaseUUID:     "lease-1",
						Tenant:        "tenant-a",
						ProviderUUID:  "prov-1",
						SKU:           "docker-small",
						InstanceIndex: 0,
						Image:         "nginx:latest",
						Status:        "running",
						CreatedAt:     now,
					},
				}, nil
			},
		}
		b := newBackendForTest(mock, nil)
		b.releaseStore = relStore

		err = b.recoverState(context.Background())
		require.NoError(t, err)

		prov := b.provisions["lease-1"]
		require.NotNil(t, prov)
		assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
		require.NotNil(t, prov.Manifest, "manifest should be restored from release store")
		assert.Equal(t, "nginx:latest", prov.Manifest.Image)
		assert.Equal(t, []string{"nginx", "-g", "daemon off;"}, prov.Manifest.Command)
		assert.Equal(t, map[string]string{"FOO": "bar"}, prov.Manifest.Env)
	})

	t.Run("manifest nil when no release store", func(t *testing.T) {
		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:   "c1",
						LeaseUUID:     "lease-1",
						Tenant:        "tenant-a",
						ProviderUUID:  "prov-1",
						SKU:           "docker-small",
						InstanceIndex: 0,
						Image:         "nginx:latest",
						Status:        "running",
						CreatedAt:     now,
					},
				}, nil
			},
		}
		b := newBackendForTest(mock, nil)
		// b.releaseStore is nil (default from newBackendForTest)

		err := b.recoverState(context.Background())
		require.NoError(t, err)

		prov := b.provisions["lease-1"]
		require.NotNil(t, prov)
		assert.Nil(t, prov.Manifest, "manifest should be nil when no release store is configured")
	})

	t.Run("manifest restored from active release not failed", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "releases.db")
		relStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
		require.NoError(t, err)
		defer relStore.Close()

		// Simulate: provision with nginx, then failed update to alpine.
		// The active release is nginx; the latest release is the failed alpine.
		err = relStore.Append("lease-1", shared.Release{
			Manifest:  manifestBytes,
			Image:     "nginx:latest",
			Status:    "active",
			CreatedAt: now,
		})
		require.NoError(t, err)

		alpineManifest := DockerManifest{Image: "alpine:latest", Command: []string{"sleep", "3600"}}
		alpineBytes, _ := json.Marshal(alpineManifest)
		err = relStore.Append("lease-1", shared.Release{
			Manifest:  alpineBytes,
			Image:     "alpine:latest",
			Status:    "failed",
			CreatedAt: now.Add(time.Second),
		})
		require.NoError(t, err)

		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:   "c1",
						LeaseUUID:     "lease-1",
						Tenant:        "tenant-a",
						ProviderUUID:  "prov-1",
						SKU:           "docker-small",
						InstanceIndex: 0,
						Image:         "nginx:latest",
						Status:        "running",
						CreatedAt:     now,
					},
				}, nil
			},
		}
		b := newBackendForTest(mock, nil)
		b.releaseStore = relStore

		err = b.recoverState(context.Background())
		require.NoError(t, err)

		prov := b.provisions["lease-1"]
		require.NotNil(t, prov)
		require.NotNil(t, prov.Manifest, "manifest should be restored from active release")
		assert.Equal(t, "nginx:latest", prov.Manifest.Image,
			"manifest should come from the active release, not the failed one")
	})

	t.Run("manifest nil when no release exists for lease", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "releases.db")
		relStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
		require.NoError(t, err)
		defer relStore.Close()

		// No releases pre-seeded for lease-1.

		mock := &mockDockerClient{
			ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{
					{
						ContainerID:   "c1",
						LeaseUUID:     "lease-1",
						Tenant:        "tenant-a",
						ProviderUUID:  "prov-1",
						SKU:           "docker-small",
						InstanceIndex: 0,
						Image:         "nginx:latest",
						Status:        "running",
						CreatedAt:     now,
					},
				}, nil
			},
		}
		b := newBackendForTest(mock, nil)
		b.releaseStore = relStore

		err = b.recoverState(context.Background())
		require.NoError(t, err)

		prov := b.provisions["lease-1"]
		require.NotNil(t, prov)
		assert.Nil(t, prov.Manifest, "manifest should be nil when no release exists")
	})
}
