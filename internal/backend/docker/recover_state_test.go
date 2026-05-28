package docker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	networktypes "github.com/docker/docker/api/types/network"
	"github.com/stretchr/testify/assert"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
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

// RenameVolume is a no-op for tests that don't exercise migration; Task 9
// tests use fakeVolumeBackend (in testsupport_test.go) which captures
// rename calls.
func (m *mockVolumeManager) RenameVolume(oldName, newName string) error {
	return nil
}

func (m *mockVolumeManager) HostPath(name string) string {
	return filepath.Join(m.defaultDir, name)
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
	// Default: return "not found". The production failure-path code
	// (doProvision/doReplace* captureContainerLogs) routinely calls
	// ContainerLogs on containers that may be in the process of being
	// removed; panicking here forces every failure-path test to set a
	// stub. Match Docker's real behavior on removed containers instead.
	return "", fmt.Errorf("no such container: %s", leasesm.ShortID(containerID))
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
	cfg.SKUProfiles = defaultTestSKUProfiles()
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
		actors:     make(map[string]*leasesm.LeaseActor),
		stopCtx:    stopCtx,
		stopCancel: stopCancel,
	}
	b.callbackSender = shared.NewCallbackSender(shared.CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     b.logger,
		StopCtx:    b.stopCtx,
	})
	b.inspector = &dockerInstanceInspector{docker: b.docker}
	b.gatherer = &dockerDiagnosticsGatherer{backend: b}
	b.provisionStore = &backendProvisionStore{backend: b}
	return b
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

// Simulated-race tests on the recoverState path are obsolete: recoverState
// hands Ready→Failed transitions off to the lease's actor via
// containerDiedMsg, and the SM's Failing.OnExit cancellation is the
// structural suppression of stale Failed callbacks on Deprovision
// preemption. The real-Deprovision test
// TestConcurrentDeprovisionAndContainerDeath_ExactlyOneCallback
// (lease_actor_test.go) covers the invariant end-to-end.
