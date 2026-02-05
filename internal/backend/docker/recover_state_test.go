package docker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
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

// mockDockerClient implements dockerClient for testing. Each method delegates to
// the corresponding Fn field; an unexpected call (nil Fn) panics so tests fail
// loudly rather than silently returning zero values.
type mockDockerClient struct {
	PingFn                       func(ctx context.Context) error
	CloseFn                      func() error
	PullImageFn                  func(ctx context.Context, imageName string, timeout time.Duration) error
	CreateContainerFn            func(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error)
	StartContainerFn             func(ctx context.Context, containerID string, timeout time.Duration) error
	RemoveContainerFn            func(ctx context.Context, containerID string) error
	InspectContainerFn           func(ctx context.Context, containerID string) (*ContainerInfo, error)
	ContainerLogsFn              func(ctx context.Context, containerID string, tail int) (string, error)
	ListManagedContainersFn      func(ctx context.Context) ([]ContainerInfo, error)
	EnsureTenantNetworkFn        func(ctx context.Context, tenant string) (string, error)
	RemoveTenantNetworkIfEmptyFn func(ctx context.Context, tenant string) error
	ListManagedNetworksFn        func(ctx context.Context) ([]networktypes.Inspect, error)
}

func (m *mockDockerClient) Ping(ctx context.Context) error {
	if m.PingFn != nil {
		return m.PingFn(ctx)
	}
	panic("unexpected call to Ping")
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

func (m *mockDockerClient) ListManagedNetworks(ctx context.Context) ([]networktypes.Inspect, error) {
	if m.ListManagedNetworksFn != nil {
		return m.ListManagedNetworksFn(ctx)
	}
	panic("unexpected call to ListManagedNetworks")
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
		pool:       pool,
		logger:     slog.Default(),
		provisions: provs,
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

		prov := b.provisions["lease-1"]
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
