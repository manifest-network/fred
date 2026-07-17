package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	networktypes "github.com/docker/docker/api/types/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// mockVolumeManager implements volumeManager for testing. Unlike mockDockerClient,
// methods default to sensible no-ops (not panics) since most tests don't care
// about volumes. Tests that need to observe volume calls set the Fn fields.
type mockVolumeManager struct {
	CreateFn       func(ctx context.Context, id string, sizeMB int64) (string, bool, error)
	EnsureQuotaFn  func(ctx context.Context, id string, sizeMB int64) error
	DestroyFn      func(ctx context.Context, id string) error
	ListFn         func() ([]string, error)
	ValidateFn     func() error
	RenameVolumeFn func(oldName, newName string) error
	UsageFn        func(ctx context.Context, id string) (int64, error)

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

func (m *mockVolumeManager) EnsureQuota(ctx context.Context, id string, sizeMB int64) error {
	if m.EnsureQuotaFn != nil {
		return m.EnsureQuotaFn(ctx, id, sizeMB)
	}
	return nil
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

// RenameVolume delegates to RenameVolumeFn when set. Tests that don't exercise
// volume renaming get a no-op default; Task 9 tests and restore tests that need
// to observe rename calls set RenameVolumeFn.
func (m *mockVolumeManager) RenameVolume(oldName, newName string) error {
	if m.RenameVolumeFn != nil {
		return m.RenameVolumeFn(oldName, newName)
	}
	return nil
}

func (m *mockVolumeManager) HostPath(name string) string {
	return filepath.Join(m.defaultDir, name)
}

func (m *mockVolumeManager) Usage(ctx context.Context, id string) (int64, error) {
	if m.UsageFn != nil {
		return m.UsageFn(ctx, id)
	}
	return 0, errors.New("mockVolumeManager.Usage not configured")
}

func (m *mockVolumeManager) Kind() string { return "mock" }

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
	ExtractImageContentFn        func(ctx context.Context, imageName string, paths []string, destDir string, maxBytes, maxEntries int64) map[string]error
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

func (m *mockDockerClient) ExtractImageContent(ctx context.Context, imageName string, paths []string, destDir string, maxBytes, maxEntries int64) map[string]error {
	if m.ExtractImageContentFn != nil {
		return m.ExtractImageContentFn(ctx, imageName, paths, destDir, maxBytes, maxEntries)
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

// runRecover drives recoverState with a fixed set of managed containers and a
// pre-existing provisions map, returning the resulting b.provisions.
func runRecover(t *testing.T, existing map[string]*provision, containers []ContainerInfo) map[string]*provision {
	t.Helper()
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return containers, nil
		},
		// Cold-start recovery (a Failed-derived entry with no prior in-memory
		// state) runs a post-merge diagnostics-gathering pass that inspects each
		// recovered container. By the time recovery sees a Failed provision the
		// container is gone, so mirror Docker's real behavior on a removed
		// container: InspectInstance returns an error, the diag loop logs+skips,
		// and LastError stays the un-enriched ErrMsgContainerExited baseline the
		// merge set. This stubs the harness only — it does NOT alter the merge
		// result these tests assert on.
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return nil, fmt.Errorf("no such container: %s", leasesm.ShortID(containerID))
		},
	}
	b := newBackendForTest(mock, existing)
	require.NoError(t, b.recoverState(context.Background()))
	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()
	out := make(map[string]*provision, len(b.provisions))
	for k, v := range b.provisions {
		out[k] = v
	}
	return out
}

func TestRecoverState_ReadyFromRunningContainers(t *testing.T) {
	got := runRecover(t, nil, []ContainerInfo{
		{ContainerID: "c1", LeaseUUID: "L1", Tenant: "t", SKU: "docker-small", ServiceName: "app", Status: "running", CallbackURL: "http://cb"},
	})
	p, ok := got["L1"]
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusReady, p.Status)
	assert.Equal(t, []string{"c1"}, p.ContainerIDs)
	assert.Equal(t, 1, p.Quantity)
	assert.Equal(t, "http://cb", p.CallbackURL)
	assert.Equal(t, map[string][]string{"app": {"c1"}}, p.ServiceContainers,
		"ServiceContainers must be rebuilt from container labels")
	assert.Equal(t, []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}, p.Items,
		"Items must be rebuilt from container labels")
}

func TestRecoverState_ColdStartFailed_BumpsFailCountAndLastError(t *testing.T) {
	got := runRecover(t, nil, []ContainerInfo{
		{ContainerID: "c1", LeaseUUID: "L1", Tenant: "t", SKU: "docker-small", ServiceName: "app", Status: "exited", FailCount: 2},
	})
	p, ok := got["L1"]
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusFailed, p.Status)
	assert.Equal(t, 3, p.FailCount, "cold-start increments the label FailCount")
	assert.Equal(t, leasesm.ErrMsgContainerExited, p.LastError)
}

func TestRecoverState_InFlightProvisioning_PreservedNoContainers(t *testing.T) {
	existing := map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusProvisioning, FailCount: 4}},
	}
	got := runRecover(t, existing, nil) // no containers
	p, ok := got["L1"]
	require.True(t, ok, "in-flight provisioning entry must survive recovery")
	assert.Equal(t, backend.ProvisionStatusProvisioning, p.Status)
	assert.Equal(t, 4, p.FailCount)
	assert.Same(t, existing["L1"], p, "in-flight entry is preserved by pointer")
}

// TestRecoverState_InFlightProvisioning_PreservesPoolReservation is the ENG-546
// regression. A lease still Provisioning (e.g. mid image-pull) reserved its
// CPU/mem/disk synchronously via TryAllocate but has no containers yet, so its
// allocation is excluded from the container-derived rebuild list. Before the
// fix, recoverState's full-replace pool.Reset therefore DROPPED that reservation
// for the whole window, letting TryAllocate briefly see phantom free capacity
// and over-admit past physical capacity. The reservation must survive.
func TestRecoverState_InFlightProvisioning_PreservesPoolReservation(t *testing.T) {
	existing := map[string]*provision{
		"a1b2c3d4-0000-4000-8000-000000000001": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "a1b2c3d4-0000-4000-8000-000000000001",
			Tenant:    "t",
			Status:    backend.ProvisionStatusProvisioning,
			Items:     []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		}},
	}
	mock := &mockDockerClient{
		// Still pulling: no containers exist yet for the in-flight lease.
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	b := newBackendForTest(mock, existing)
	// The synchronous reservation Provision made before handing off to the pull.
	require.NoError(t, b.pool.TryAllocate("a1b2c3d4-0000-4000-8000-000000000001-app-0", "docker-small", "t"))
	before := b.pool.Stats()
	require.Equal(t, int64(1024), before.AllocatedDiskMB, "docker-small disk reserved up front")

	require.NoError(t, b.recoverState(context.Background()))

	after := b.pool.Stats()
	assert.Equal(t, before.AllocatedDiskMB, after.AllocatedDiskMB, "in-flight disk reservation must survive recoverState (ENG-546)")
	assert.Equal(t, before.AllocatedCPU, after.AllocatedCPU, "in-flight CPU reservation must survive recoverState")
	assert.Equal(t, before.AllocatedMemoryMB, after.AllocatedMemoryMB, "in-flight memory reservation must survive recoverState")
	assert.Equal(t, 1, after.AllocationCount, "reservation preserved exactly once")
}

// TestRecoverState_InFlightProvisioning_PreservesReservation_ItemsNotYetEnriched
// covers the window in Provision between TryAllocate and enrichReserved: the
// entry is already Provisioning with a live pool reservation, but its Items are
// not yet populated. The reservation must still survive — which is why the fix
// preserves it from the LIVE POOL (by lease-UUID prefix) rather than
// reconstructing the keys from the not-yet-populated Items.
func TestRecoverState_InFlightProvisioning_PreservesReservation_ItemsNotYetEnriched(t *testing.T) {
	existing := map[string]*provision{
		"a1b2c3d4-0000-4000-8000-000000000002": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "a1b2c3d4-0000-4000-8000-000000000002",
			Tenant:    "t",
			Status:    backend.ProvisionStatusProvisioning,
			Items:     nil, // enrichReserved has not run yet
		}},
	}
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	b := newBackendForTest(mock, existing)
	require.NoError(t, b.pool.TryAllocate("a1b2c3d4-0000-4000-8000-000000000002-app-0", "docker-small", "t"))
	before := b.pool.Stats()

	require.NoError(t, b.recoverState(context.Background()))

	assert.Equal(t, before.AllocatedDiskMB, b.pool.Stats().AllocatedDiskMB,
		"reservation must survive even before Items are enriched (preserved from the pool, not reconstructed from Items)")
}

// TestRecoverState_InFlightUpdate_PreservesReservationExactlyOnce verifies a
// lease mid-Update (containers running, reservation reused from Provision) keeps
// its reservation across recoverState AND is not double-counted: its
// container-derived allocation is excluded because the op may be mid-cleanup,
// while the live pool reservation is preserved, so the disk is counted once.
func TestRecoverState_InFlightUpdate_PreservesReservationExactlyOnce(t *testing.T) {
	const lease = "a1b2c3d4-0000-4000-8000-000000000003"
	existing := map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease,
			Tenant:    "t",
			Status:    backend.ProvisionStatusUpdating,
			Items:     []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		}},
	}
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{ContainerID: "c1", LeaseUUID: lease, Tenant: "t", SKU: "docker-small", ServiceName: "app", InstanceIndex: 0, Status: "running"},
			}, nil
		},
	}
	b := newBackendForTest(mock, existing)
	require.NoError(t, b.pool.TryAllocate(lease+"-app-0", "docker-small", "t"))

	require.NoError(t, b.recoverState(context.Background()))

	got := b.pool.Stats()
	assert.Equal(t, int64(1024), got.AllocatedDiskMB, "reservation preserved exactly once (not dropped, not doubled)")
	assert.Equal(t, 1, got.AllocationCount)
}

// TestRecoverState_ReadyLease_ReservationRebuiltFromContainers guards the
// boundary: a Ready lease is NOT in-flight, so its reservation is rebuilt purely
// from the container-derived list and a stale pre-existing pool entry is not
// additionally preserved — no double count, container-derived figure wins.
func TestRecoverState_ReadyLease_ReservationRebuiltFromContainers(t *testing.T) {
	const lease = "a1b2c3d4-0000-4000-8000-000000000004"
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{ContainerID: "c1", LeaseUUID: lease, Tenant: "t", SKU: "docker-small", ServiceName: "app", InstanceIndex: 0, Status: "running"},
			}, nil
		},
	}
	b := newBackendForTest(mock, nil)
	require.NoError(t, b.pool.TryAllocate(lease+"-app-0", "docker-small", "t"))

	require.NoError(t, b.recoverState(context.Background()))

	got := b.pool.Stats()
	assert.Equal(t, int64(1024), got.AllocatedDiskMB, "Ready lease counted once from containers")
	assert.Equal(t, 1, got.AllocationCount)
}

// TestRecoverState_InFlightProvisioning_PreservesAllInstanceReservations locks
// the crux of the fix: the keep closure preserves by {leaseUUID}- PREFIX, so
// EVERY instance key of an in-flight lease must survive — not just the first.
// A multi-instance / multi-service in-flight lease with no containers must carry
// all its reservations forward. Guards against a refactor to an exact-key set
// that would preserve only one instance and silently under-count (the ENG-546
// over-admission class).
func TestRecoverState_InFlightProvisioning_PreservesAllInstanceReservations(t *testing.T) {
	const lease = "a1b2c3d4-0000-4000-8000-000000000005"
	existing := map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease,
			Tenant:    "t",
			Status:    backend.ProvisionStatusProvisioning,
			Items: []backend.LeaseItem{
				{SKU: "docker-small", Quantity: 2, ServiceName: "app"},
				{SKU: "docker-small", Quantity: 1, ServiceName: "web"},
			},
		}},
	}
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	b := newBackendForTest(mock, existing)
	for _, key := range []string{lease + "-app-0", lease + "-app-1", lease + "-web-0"} {
		require.NoError(t, b.pool.TryAllocate(key, "docker-small", "t"))
	}
	before := b.pool.Stats()
	require.Equal(t, int64(3*1024), before.AllocatedDiskMB)
	require.Equal(t, 3, before.AllocationCount)

	require.NoError(t, b.recoverState(context.Background()))

	after := b.pool.Stats()
	assert.Equal(t, int64(3*1024), after.AllocatedDiskMB, "every instance key of the in-flight lease must survive (prefix keep)")
	assert.Equal(t, 3, after.AllocationCount, "all three instance reservations preserved, not just the first")
}

// TestRecoverState_FailedCrashGCd_PreservesReservation is the ENG-567 core case
// (the "4th sibling"). A Ready lease whose container crashed → Failed never
// releases its pool key (releases happen only on the Deprovision and doProvision-
// failure paths), and if the exited container is then removed out-of-band the
// lease is Failed with VolumeCleanupAttempts==0 and no container — but its volume
// (and XFS bhard quota) is still on disk. The pool is authoritative for tracked
// leases, so recoverState must PRESERVE the still-held reservation. (This
// repurposes the former TestRecoverState_FailedLease_StaleReservationDropped,
// whose premise — a genuinely-failed provision leaving a *stale* key — is
// unreachable: doProvision's failure defer releases the key AND destroys the
// volume in the same defer.)
func TestRecoverState_FailedCrashGCd_PreservesReservation(t *testing.T) {
	const lease = "a1b2c3d4-0000-4000-8000-000000000006"
	existing := map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease, Tenant: "t", Status: backend.ProvisionStatusFailed, FailCount: 1,
		}},
	}
	mock := &mockDockerClient{
		// Exited container removed out-of-band: no containers reported.
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	b := newBackendForTest(mock, existing)
	require.NoError(t, b.pool.TryAllocate(lease+"-app-0", "docker-small", "t"))
	require.Equal(t, int64(1024), b.pool.Stats().AllocatedDiskMB)

	require.NoError(t, b.recoverState(context.Background()))

	got := b.pool.Stats()
	assert.Equal(t, int64(1024), got.AllocatedDiskMB, "a tracked Failed lease's still-held reservation must be preserved (ENG-567)")
	assert.Equal(t, 1, got.AllocationCount)
}

// TestRecoverState_FailedCleanupRetry_PreservesPoolReservation is the ENG-563
// regression, and the counterpart to the test above. When doDeprovision removes
// a lease's containers but a volume Destroy/rename then FAILS (under the retry
// limit), it keeps the lease Status=Failed with VolumeCleanupAttempts>0 and
// deliberately does NOT call releaseLive() — the bytes are still on disk, so the
// reservation must stay counted for the retry. recoverState must preserve that
// reservation (dropping it would free phantom capacity while the volume's XFS
// quota still occupies disk → over-admit).
//
// Under the pool-authoritative rule (ENG-567) VolumeCleanupAttempts no longer
// gates preservation — every tracked lease that still holds a key is preserved —
// so this now exercises one specific instance of that rule (the cleanup-retry
// sub-state).
func TestRecoverState_FailedCleanupRetry_PreservesPoolReservation(t *testing.T) {
	const lease = "a1b2c3d4-0000-4000-8000-000000000009"
	existing := map[string]*provision{
		lease: {
			ProvisionState: leasesm.ProvisionState{
				LeaseUUID: lease,
				Tenant:    "t",
				Status:    backend.ProvisionStatusFailed,
				Items:     []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
			},
			// Volume cleanup failed at least once and is pending retry: the
			// reservation is still held (releaseLive not yet called).
			VolumeCleanupAttempts: 1,
		},
	}
	mock := &mockDockerClient{
		// Containers already removed by the successful compose.Down; only the
		// volume teardown failed.
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	b := newBackendForTest(mock, existing)
	require.NoError(t, b.pool.TryAllocate(lease+"-app-0", "docker-small", "t"))
	before := b.pool.Stats()
	require.Equal(t, int64(1024), before.AllocatedDiskMB)

	require.NoError(t, b.recoverState(context.Background()))

	after := b.pool.Stats()
	assert.Equal(t, before.AllocatedDiskMB, after.AllocatedDiskMB, "cleanup-retry reservation must survive until releaseLive() runs (ENG-563)")
	assert.Equal(t, 1, after.AllocationCount, "reservation preserved exactly once")
}

func TestRecoverState_FailingNormalizedToFailed(t *testing.T) {
	existing := map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusFailing, FailCount: 1, LastError: "x"}},
	}
	got := runRecover(t, existing, nil)
	p, ok := got["L1"]
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusFailed, p.Status, "Failing normalizes to Failed")
	assert.Equal(t, 1, p.FailCount)
	assert.Equal(t, "x", p.LastError)
}

func TestRecoverState_FailingNormalized_DeepClonesReferenceFields(t *testing.T) {
	// recover.go's Failing->Failed normalization builds a fresh entry via
	// recoveredFromProvision+materialize. Lock the deep-clone at its real call
	// site with NON-empty reference fields: the recovered entry must be a fresh
	// struct whose Items/ContainerIDs/ServiceContainers do not alias the
	// original existing entry's backing arrays.
	existing := map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:         "L1",
			Status:            backend.ProvisionStatusFailing,
			FailCount:         2,
			LastError:         "boom",
			Items:             []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app", CustomDomain: "x.example"}},
			ContainerIDs:      []string{"c1"},
			ServiceContainers: map[string][]string{"app": {"c1"}},
		}},
	}
	got := runRecover(t, existing, nil) // no containers -> Failing-normalize path
	p, ok := got["L1"]
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusFailed, p.Status, "Failing normalizes to Failed")
	assert.NotSame(t, existing["L1"], p, "must be a fresh materialized entry, not the mutated published struct")
	// Mutating the recovered entry's reference fields must NOT touch the original.
	p.Items[0].ServiceName = "mutated"
	p.ContainerIDs[0] = "mutated"
	p.ServiceContainers["app"][0] = "mutated"
	assert.Equal(t, "app", existing["L1"].Items[0].ServiceName, "Items must be deep-cloned")
	assert.Equal(t, "c1", existing["L1"].ContainerIDs[0], "ContainerIDs must be deep-cloned")
	assert.Equal(t, "c1", existing["L1"].ServiceContainers["app"][0], "ServiceContainers must be deep-cloned")
}

func TestRecoverState_FailedPreservedNoContainers(t *testing.T) {
	existing := map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusFailed, FailCount: 7}},
	}
	got := runRecover(t, existing, nil)
	p, ok := got["L1"]
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusFailed, p.Status)
	assert.Equal(t, 7, p.FailCount)
}

func TestRecoverState_FailCountAntiRegression(t *testing.T) {
	// Existing in-memory FailCount (5) must win over the stale container label (2).
	existing := map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusFailed, FailCount: 5}},
	}
	got := runRecover(t, existing, []ContainerInfo{
		{ContainerID: "c1", LeaseUUID: "L1", Tenant: "t", SKU: "docker-small", ServiceName: "app", Status: "running", FailCount: 2},
	})
	p, ok := got["L1"]
	require.True(t, ok)
	assert.Equal(t, 5, p.FailCount, "higher in-memory FailCount must not regress to the label value")
	assert.Equal(t, backend.ProvisionStatusReady, p.Status,
		"container-derived Ready status must not be suppressed by FailCount anti-regression")
}

func TestRecoverState_DeprovisioningPreserved_NoContainers(t *testing.T) {
	existing := map[string]*provision{
		// Seed a non-zero VolumeCleanupAttempts. ENG-285's volume-retry split
		// increments this docker-private counter in a span separate from the
		// ProvisionState writes and relies on this preserve-case keeping the live
		// *provision (and thus the counter) across recoverState's map swap.
		// Asserting it survives here is the DETERMINISTIC guard for that invariant
		// — the concurrent race test only hits the inter-span window
		// probabilistically; a rebuild-fresh regression would reset the counter
		// and is caught here every run.
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusDeprovisioning}, VolumeCleanupAttempts: 2},
	}
	got := runRecover(t, existing, nil) // containers already gone
	p, ok := got["L1"]
	require.True(t, ok, "a Deprovisioning lease must be preserved, not dropped")
	assert.Equal(t, backend.ProvisionStatusDeprovisioning, p.Status)
	assert.Same(t, existing["L1"], p, "preserved by pointer — the deprovision goroutine owns it")
	assert.Equal(t, 2, p.VolumeCleanupAttempts, "docker-private VolumeCleanupAttempts must survive the preserve-case (not reset by a rebuild-fresh)")
}

func TestRecoverState_DeprovisioningPreserved_SurvivingContainers(t *testing.T) {
	existing := map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusDeprovisioning}},
	}
	got := runRecover(t, existing, []ContainerInfo{
		{ContainerID: "c1", LeaseUUID: "L1", Tenant: "t", SKU: "docker-small", ServiceName: "app", Status: "running"},
	})
	p, ok := got["L1"]
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusDeprovisioning, p.Status, "must NOT be resurrected to a container-derived status")
	assert.Same(t, existing["L1"], p)
}

// TestRecoverState_Deprovisioning_PreservesPoolReservation is the ENG-562
// regression. doDeprovision removes containers BEFORE releasing the pool (it
// defers releaseLive() until after the volume-destroy / retention work, so the
// footprint is never momentarily uncounted while bytes persist on disk). So a
// lease can be Status=Deprovisioning with its containers already gone but its
// reservation still held. recoverState must keep that reservation across the
// rebuild — dropping it would let TryAllocate see phantom free capacity and
// over-admit before the deprovision goroutine's own Release runs.
func TestRecoverState_Deprovisioning_PreservesPoolReservation(t *testing.T) {
	const lease = "a1b2c3d4-0000-4000-8000-000000000007"
	existing := map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease,
			Tenant:    "t",
			Status:    backend.ProvisionStatusDeprovisioning,
			Items:     []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		}},
	}
	mock := &mockDockerClient{
		// Containers already removed; the deprovision goroutine has not yet run
		// releaseLive(), so the pool reservation is still held.
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	b := newBackendForTest(mock, existing)
	require.NoError(t, b.pool.TryAllocate(lease+"-app-0", "docker-small", "t"))
	before := b.pool.Stats()
	require.Equal(t, int64(1024), before.AllocatedDiskMB)

	require.NoError(t, b.recoverState(context.Background()))

	after := b.pool.Stats()
	assert.Equal(t, before.AllocatedDiskMB, after.AllocatedDiskMB, "Deprovisioning reservation must survive until releaseLive() runs (ENG-562)")
	assert.Equal(t, 1, after.AllocationCount, "reservation preserved exactly once")
}

// TestRecoverState_Deprovisioning_WithContainer_CountedOnce covers the brief
// window between the Deprovisioning mark and compose.Down: a container is still
// present AND the pool reservation is still held. The container-derived
// allocation (Deprovisioning is not excluded from the rebuild list) and the
// preserved pool reservation share a key, so it must be counted exactly once —
// ResetPreserving dedup makes the preserved entry win.
func TestRecoverState_Deprovisioning_WithContainer_CountedOnce(t *testing.T) {
	const lease = "a1b2c3d4-0000-4000-8000-000000000008"
	existing := map[string]*provision{
		lease: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: lease,
			Tenant:    "t",
			Status:    backend.ProvisionStatusDeprovisioning,
			Items:     []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}},
		}},
	}
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{ContainerID: "c1", LeaseUUID: lease, Tenant: "t", SKU: "docker-small", ServiceName: "app", InstanceIndex: 0, Status: "running"},
			}, nil
		},
	}
	b := newBackendForTest(mock, existing)
	require.NoError(t, b.pool.TryAllocate(lease+"-app-0", "docker-small", "t"))

	require.NoError(t, b.recoverState(context.Background()))

	got := b.pool.Stats()
	assert.Equal(t, int64(1024), got.AllocatedDiskMB, "counted exactly once (not doubled by container-derived + preserved)")
	assert.Equal(t, 1, got.AllocationCount)
}

// TestRecoverState_ConcurrentReaderDuringMerge runs recoverState while another
// goroutine continuously reads provision state through the store seam. The race
// detector must stay clean: the merge holds provisionsMu across the swap and the
// reader takes it via Get.
func TestRecoverState_ConcurrentReaderDuringMerge(t *testing.T) {
	existing := map[string]*provision{
		"L1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "L1", Status: backend.ProvisionStatusReady, ContainerIDs: []string{"c1"}}},
	}
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{ContainerID: "c1", LeaseUUID: "L1", Tenant: "t", SKU: "docker-small", ServiceName: "app", Status: "running"},
			}, nil
		},
	}
	b := newBackendForTest(mock, existing)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = b.provisionStore.Get("L1")
			}
		}
	}()

	for range 20 {
		require.NoError(t, b.recoverState(context.Background()))
	}
	close(stop)
	wg.Wait()
}

// TestRecoverState_ColdStartFailed_EnrichesLastError covers the cold-start
// diagnostics-enrichment path (ENG-193 review #4 — previously dark): the
// runRecover helper hardcodes an erroring InspectContainerFn so the gather
// branch never runs. Here InspectInstance SUCCEEDS (Status "exited" maps to
// PhaseExited), so failedDiagnostics is populated and the UpdateFn enrichment
// loop fires, rewriting LastError to ErrMsgContainerExited + ": " + diag.
func TestRecoverState_ColdStartFailed_EnrichesLastError(t *testing.T) {
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{ContainerID: "c1", LeaseUUID: "L1", Tenant: "t", SKU: "docker-small", ServiceName: "app", Status: "exited"},
			}, nil
		},
		// Succeeding inspector: InspectInstance -> PhaseExited reaches the gather
		// branch. ContainerLogs uses the mock's safe default ("no such container"
		// error), so the gatherer returns a benign non-empty "exit_code=0" diag.
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{Status: "exited", ExitCode: 1}, nil
		},
	}
	b := newBackendForTest(mock, nil)
	require.NoError(t, b.recoverState(context.Background()))

	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()
	p, ok := b.provisions["L1"]
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusFailed, p.Status)
	assert.True(t, strings.HasPrefix(p.LastError, leasesm.ErrMsgContainerExited+": "),
		"enrichment loop must rewrite LastError with the gathered diag prefix; got %q", p.LastError)
}

// TestRecoverState_ColdStartFailed_EnrichmentSkippedWhenInstanceReplaced is the
// ENG-193 review #1 guard: diagnostics are gathered during an I/O window AFTER
// provisionsMu is released. If a concurrent Provision-retry replaces the lease
// with a DIFFERENT Failed instance during that window, the stale diag must NOT
// clobber the new owner's LastError. The InspectContainerFn side effect
// simulates that re-provision mid-window; the CreatedAt guard must skip the
// stale write because the replacement carries a fresh CreatedAt.
func TestRecoverState_ColdStartFailed_EnrichmentSkippedWhenInstanceReplaced(t *testing.T) {
	// The recovered cold-start entry's CreatedAt comes from the container label
	// (ContainerInfo.CreatedAt). Use distinct explicit times so the guard sees a
	// mismatch between the snapshot identity and the replacement's identity.
	originalCreatedAt := time.Unix(1000, 0)
	replacementCreatedAt := time.Unix(2000, 0)

	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{ContainerID: "c1", LeaseUUID: "L1", Tenant: "t", SKU: "docker-small", ServiceName: "app", Status: "exited", CreatedAt: originalCreatedAt},
			}, nil
		},
	}
	b := newBackendForTest(mock, nil)
	// Set the inspector AFTER building b so the closure can capture it. The side
	// effect simulates a Provision-retry taking ownership during the diag I/O
	// window: replace L1 with a fresh Failed instance (different CreatedAt) under
	// the same provisionsMu, then return a terminal state to drive the gather.
	mock.InspectContainerFn = func(ctx context.Context, containerID string) (*ContainerInfo, error) {
		b.provisionsMu.Lock()
		b.provisions["L1"] = &provision{ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "L1",
			Status:    backend.ProvisionStatusFailed,
			LastError: "fresh-owner-error",
			CreatedAt: replacementCreatedAt,
		}}
		b.provisionsMu.Unlock()
		return &ContainerInfo{Status: "exited"}, nil
	}

	require.NoError(t, b.recoverState(context.Background()))

	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()
	p, ok := b.provisions["L1"]
	require.True(t, ok)
	assert.Equal(t, "fresh-owner-error", p.LastError,
		"CreatedAt guard must skip the stale enriched write onto a replacement instance; got %q", p.LastError)
}

// TestRecoverState_AgeReapedActiveRelease_StillRehydratesManifest is the ENG-440
// functional regression at the recover layer: a lease running stably >=90d has one old
// "active" release; after the age reaper runs (e.g. at the next backend restart)
// recoverState must STILL rehydrate prov.StackManifest from it. Before the keep-latest
// fix, RemoveOlderThan whole-key-deleted the record, leaving StackManifest nil ->
// routeReplaceRestart hard-fails ErrInvalidState "no stored manifest" and the
// custom-domain reconcile loops.
//
// This asserts the two NECESSARY conditions for the restart gate (restart_update.go:108-116):
// the recovered provision is Ready AND its StackManifest is non-nil. That a real Restart
// then SUCCEEDS end-to-end (the sufficient condition, through the actual constructor
// reaper-before-recover ordering) is proven by the integration e2e
// TestIntegration_Docker_AgeReapedReleaseStillRestartable.
func TestRecoverState_AgeReapedActiveRelease_StillRehydratesManifest(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recover_releases.db")
	relStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer relStore.Close()

	// One provision-time active release, older than the 90d cutoff.
	require.NoError(t, relStore.Append("L1", shared.Release{
		Manifest:  []byte(`{"image":"nginx:1.25"}`),
		Image:     "stack",
		Status:    "active",
		CreatedAt: time.Now().Add(-100 * 24 * time.Hour),
	}))

	// Simulate the startup/periodic age reap.
	_, err = relStore.RemoveOlderThan(90 * 24 * time.Hour)
	require.NoError(t, err)

	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{
				{ContainerID: "c1", LeaseUUID: "L1", Tenant: "t", SKU: "docker-small", ServiceName: "app", Status: "running", CallbackURL: "http://cb"},
			}, nil
		},
	}
	b := newBackendForTest(mock, nil)
	b.releaseStore = relStore
	require.NoError(t, b.recoverState(context.Background()))

	b.provisionsMu.RLock()
	p := b.provisions["L1"]
	b.provisionsMu.RUnlock()
	require.NotNil(t, p)
	require.NotNil(t, p.StackManifest,
		"after the keep-latest fix, an age-reaped lease still rehydrates its manifest (restart gate's StackManifest!=nil condition)")
	assert.Equal(t, backend.ProvisionStatusReady, p.Status,
		"the recovered lease is Ready (the restart gate's other condition)")
}
