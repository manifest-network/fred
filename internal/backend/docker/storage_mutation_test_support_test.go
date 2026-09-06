package docker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

var testStorageMutationOperations sync.Map // map[*Backend]storageMutationOperations

// installTestStorageMutationAdapters is the explicit test-only construction
// seam. Its proxies follow deliberate test replacement of Backend's read views;
// production has no equivalent raw-capability recovery path.
func installTestStorageMutationAdapters(b *Backend) {
	ops := newStorageMutationOperations(
		b,
		testDockerMutationProxy{backend: b},
		testComposeMutationProxy{backend: b},
		testVolumeMutationProxy{backend: b},
	)
	testStorageMutationOperations.Store(b, ops)
	coordinator, err := newBackgroundMaintenanceCoordinator(b, ops)
	if err != nil {
		panic(err)
	}
	b.backgroundMaintenance = coordinator
}

// testStorageMutationAdapter preserves the low-level identity-boundary tests'
// compact call shape without adding an unscoped mutation accessor to
// production. It deliberately exists only in a _test.go file. Operation and
// maintenance tests must use their Started-subject-bound executors instead.
type testStorageMutationAdapter struct{ ops storageMutationOperations }

func storageMutationOperationsForTest(b *Backend) (storageMutationOperations, error) {
	if b == nil {
		return storageMutationOperations{}, errors.New("test backend is nil")
	}
	value, ok := testStorageMutationOperations.Load(b)
	if !ok {
		return storageMutationOperations{}, errors.New("test backend has no constructed storage mutation operations")
	}
	ops, ok := value.(storageMutationOperations)
	if !ok || ops.backend != b {
		return storageMutationOperations{}, errors.New("test backend storage mutation operations are invalid")
	}
	return ops, nil
}

// testFullDockerClient and testFullVolumeManager rejoin the deliberately
// separated read/write projections only at construction-only test seams. The
// resulting aggregate is never retained by Backend.
type testFullDockerClient struct {
	dockerReadClient
	dockerMutationSink
}

type testFullVolumeManager struct {
	volumeReader
	volumeMutationSink
}

func fullStorageClientsForTest(b *Backend) (dockerClient, volumeManager) {
	ops, err := storageMutationOperationsForTest(b)
	if err != nil {
		panic(err)
	}
	return testFullDockerClient{
			dockerReadClient: b.docker, dockerMutationSink: ops.docker,
		}, testFullVolumeManager{
			volumeReader: b.volumes, volumeMutationSink: ops.volumes,
		}
}

func (b *Backend) mutationAdapter() *testStorageMutationAdapter {
	ops, err := storageMutationOperationsForTest(b)
	if err != nil {
		panic(err)
	}
	return &testStorageMutationAdapter{ops: ops}
}

func (m *testStorageMutationAdapter) perform(
	ctx context.Context,
	operation string,
	action func(context.Context) error,
) error {
	if m == nil || m.ops.backend == nil || action == nil {
		return errors.New("test substrate mutation adapter is unavailable")
	}
	result := substratemutation.RunStep(
		ctx,
		operation,
		m.ops.backend.authorizeStorageMutation,
		m.ops.backend.completeStorageMutation,
		action,
	)
	return m.ops.backend.resolveBackgroundStorageStep(operation, result)
}

func (m *testStorageMutationAdapter) composeUp(
	ctx context.Context,
	project *composetypes.Project,
	opts composeUpOpts,
) error {
	return m.perform(ctx, "test compose up", func(ctx context.Context) error {
		return m.ops.compose.Up(ctx, project, opts)
	})
}

func (m *testStorageMutationAdapter) composeDown(
	ctx context.Context,
	leaseUUID string,
	timeout time.Duration,
) error {
	return m.perform(ctx, "test compose down", func(ctx context.Context) error {
		return m.ops.compose.Down(ctx, composeProjectName(leaseUUID), timeout)
	})
}

func (m *testStorageMutationAdapter) createVolume(
	ctx context.Context,
	id string,
	sizeMB int64,
) (path string, created bool, err error) {
	err = m.perform(ctx, "test create volume", func(ctx context.Context) error {
		path, created, err = m.ops.volumes.Create(ctx, id, sizeMB)
		return err
	})
	return path, created, err
}

func (m *testStorageMutationAdapter) destroyVolume(
	ctx context.Context,
	_ volumeDestroyer,
	id string,
) error {
	return m.perform(ctx, "test destroy volume", func(ctx context.Context) error {
		return m.ops.volumes.Destroy(ctx, id)
	})
}

func (m *testStorageMutationAdapter) renameVolume(
	ctx context.Context,
	oldName, newName string,
) error {
	return m.perform(ctx, "test rename volume", func(ctx context.Context) error {
		return m.ops.volumes.RenameVolume(ctx, oldName, newName)
	})
}

func (m *testStorageMutationAdapter) ensureVolumeQuota(ctx context.Context, id string, sizeMB int64) error {
	return m.perform(ctx, "test ensure volume quota", func(ctx context.Context) error {
		return m.ops.volumes.EnsureQuota(ctx, id, sizeMB)
	})
}

func (m *testStorageMutationAdapter) removeContainer(ctx context.Context, id string) error {
	return m.perform(ctx, "test remove container", func(ctx context.Context) error {
		return m.ops.docker.RemoveContainer(ctx, id)
	})
}

func (m *testStorageMutationAdapter) removeTenantNetwork(ctx context.Context, tenant string) error {
	return m.perform(ctx, "test remove tenant network", func(ctx context.Context) error {
		return m.ops.docker.RemoveTenantNetworkIfEmpty(ctx, tenant)
	})
}

type testVolumeDestroyCapability struct{ adapter *testStorageMutationAdapter }

func (c testVolumeDestroyCapability) destroyVolume(ctx context.Context, id string) error {
	return c.adapter.destroyVolume(ctx, nil, id)
}

func (c testVolumeDestroyCapability) canDestroyVolumes() bool {
	return c.adapter != nil && c.adapter.ops.volumes != nil
}

func backgroundCapabilitiesForTest(
	b *Backend,
) (backgroundVolumeRename, teardownMutationCapability, volumeDestroyMutationCapability, backgroundVolumeQuota) {
	adapter := b.mutationAdapter()
	renameVolume := backgroundVolumeRename(adapter.renameVolume)
	teardown := backgroundTeardownCapability{
		downFn: adapter.composeDown, removeFn: adapter.removeContainer,
	}
	destroyVolumes := testVolumeDestroyCapability{adapter: adapter}
	ensureQuota := backgroundVolumeQuota(adapter.ensureVolumeQuota)
	return renameVolume, teardown, destroyVolumes, ensureQuota
}

func volumeDestroyCapabilityForTest(b *Backend) volumeDestroyMutationCapability {
	_, _, destroyVolumes, _ := backgroundCapabilitiesForTest(b)
	return destroyVolumes
}

// Targeted helpers below are deliberately test-only. Production can retain and
// invoke only complete background workflows through backgroundMaintenance.
func (b *Backend) reconcileRestoring(ctx context.Context, entry shared.RetentionEntry) error {
	renameVolume, teardown, destroyVolumes, ensureQuota := backgroundCapabilitiesForTest(b)
	return b.reconcileRestoringWithAuthorityUsing(
		ctx, entry, renameVolume, teardown, destroyVolumes, ensureQuota,
	)
}

func (b *Backend) reconcileRestoringWithAuthority(ctx context.Context, entry shared.RetentionEntry) error {
	return b.reconcileRestoring(ctx, entry)
}

func (b *Backend) retryReapingRecords(ctx context.Context) error {
	_, _, destroyVolumes, _ := backgroundCapabilitiesForTest(b)
	return b.retryReapingRecordsUsing(ctx, destroyVolumes)
}

func (b *Backend) destroyReapingVolumes(
	ctx context.Context,
	index *managedVolumeIndex,
	proof shared.ReapingRetentionProof,
) bool {
	_, _, destroyVolumes, _ := backgroundCapabilitiesForTest(b)
	return b.destroyReapingVolumesUsing(ctx, index, proof, destroyVolumes)
}

func (b *Backend) restoreRetainedVolumeQuotas(
	ctx context.Context,
	record *shared.RetentionEntry,
) ([]shared.SKUResourceSnapshot, error) {
	_, _, _, ensureQuota := backgroundCapabilitiesForTest(b)
	return b.restoreRetainedVolumeQuotasUsing(ctx, record, ensureQuota)
}

func (b *Backend) renameIfPresent(ctx context.Context, oldName, newName string) error {
	renameVolume, _, _, _ := backgroundCapabilitiesForTest(b)
	return b.renameIfPresentUsing(ctx, renameVolume, oldName, newName)
}

func (b *Backend) teardownLeaseContainers(
	ctx context.Context,
	leaseUUID string,
	recordedIDs []string,
	stopTimeout time.Duration,
	operation string,
	logger *slog.Logger,
) ([]string, error) {
	_, teardown, _, _ := backgroundCapabilitiesForTest(b)
	return b.teardownLeaseContainersUsing(
		teardown, ctx, leaseUUID, recordedIDs, stopTimeout, operation, logger,
	)
}

func (b *Backend) releaseTenantNetwork(ctx context.Context, tenant string) error {
	return b.removeOrphanedTenantNetworkUsing(
		ctx, tenant, backgroundTenantNetworkRemove(b.mutationAdapter().removeTenantNetwork),
	)
}

type testDockerMutationProxy struct{ backend *Backend }

func (p testDockerMutationProxy) sink() (dockerMutationSink, error) {
	sink, ok := p.backend.docker.(dockerMutationSink)
	if !ok {
		return nil, fmt.Errorf("test Docker mutation sink %T is unavailable", p.backend.docker)
	}
	return sink, nil
}

func (p testDockerMutationProxy) PullImage(ctx context.Context, image string, timeout time.Duration) error {
	sink, err := p.sink()
	if err != nil {
		return err
	}
	return sink.PullImage(ctx, image, timeout)
}

func (p testDockerMutationProxy) ResolveImageUser(ctx context.Context, image, user string) (int, int, error) {
	sink, err := p.sink()
	if err != nil {
		return 0, 0, err
	}
	return sink.ResolveImageUser(ctx, image, user)
}

func (p testDockerMutationProxy) CreateContainer(ctx context.Context, params CreateContainerParams, timeout time.Duration) (string, error) {
	sink, err := p.sink()
	if err != nil {
		return "", err
	}
	return sink.CreateContainer(ctx, params, timeout)
}

func (p testDockerMutationProxy) StartContainer(ctx context.Context, id string, timeout time.Duration) error {
	sink, err := p.sink()
	if err != nil {
		return err
	}
	return sink.StartContainer(ctx, id, timeout)
}

func (p testDockerMutationProxy) StopContainer(ctx context.Context, id string, timeout time.Duration) error {
	sink, err := p.sink()
	if err != nil {
		return err
	}
	return sink.StopContainer(ctx, id, timeout)
}

func (p testDockerMutationProxy) RenameContainer(ctx context.Context, id, name string) error {
	sink, err := p.sink()
	if err != nil {
		return err
	}
	return sink.RenameContainer(ctx, id, name)
}

func (p testDockerMutationProxy) RemoveContainer(ctx context.Context, id string) error {
	sink, err := p.sink()
	if err != nil {
		return err
	}
	return sink.RemoveContainer(ctx, id)
}

func (p testDockerMutationProxy) EnsureTenantNetwork(ctx context.Context, tenant string) (string, error) {
	sink, err := p.sink()
	if err != nil {
		return "", err
	}
	return sink.EnsureTenantNetwork(ctx, tenant)
}

func (p testDockerMutationProxy) RemoveTenantNetworkIfEmpty(ctx context.Context, tenant string) error {
	sink, err := p.sink()
	if err != nil {
		return err
	}
	return sink.RemoveTenantNetworkIfEmpty(ctx, tenant)
}

func (p testDockerMutationProxy) DetectVolumeOwner(ctx context.Context, image string, paths []string) (int, int, error) {
	sink, err := p.sink()
	if err != nil {
		return 0, 0, err
	}
	return sink.DetectVolumeOwner(ctx, image, paths)
}

func (p testDockerMutationProxy) DetectWritablePaths(ctx context.Context, image string, uid int, parents []string) ([]string, error) {
	sink, err := p.sink()
	if err != nil {
		return nil, err
	}
	return sink.DetectWritablePaths(ctx, image, uid, parents)
}

func (p testDockerMutationProxy) ExtractImageContent(
	ctx context.Context,
	image string,
	paths []string,
	destination string,
	maxBytes, maxEntries int64,
) map[string]error {
	sink, err := p.sink()
	if err != nil {
		return map[string]error{"": err}
	}
	return sink.ExtractImageContent(ctx, image, paths, destination, maxBytes, maxEntries)
}

type testComposeMutationProxy struct{ backend *Backend }

func (p testComposeMutationProxy) sink() (composeMutationSink, error) {
	sink, ok := p.backend.compose.(composeMutationSink)
	if !ok {
		return nil, fmt.Errorf("test Compose mutation sink %T is unavailable", p.backend.compose)
	}
	return sink, nil
}

func (p testComposeMutationProxy) Up(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
	sink, err := p.sink()
	if err != nil {
		return err
	}
	return sink.Up(ctx, project, opts)
}

func (p testComposeMutationProxy) Down(ctx context.Context, project string, timeout time.Duration) error {
	sink, err := p.sink()
	if err != nil {
		return err
	}
	return sink.Down(ctx, project, timeout)
}

type testVolumeMutationProxy struct{ backend *Backend }

func (p testVolumeMutationProxy) sink() (volumeMutationSink, error) {
	sink, ok := p.backend.volumes.(volumeMutationSink)
	if !ok {
		return nil, fmt.Errorf("test volume mutation sink %T is unavailable", p.backend.volumes)
	}
	return sink, nil
}

func (p testVolumeMutationProxy) Create(ctx context.Context, id string, sizeMB int64) (string, bool, error) {
	sink, err := p.sink()
	if err != nil {
		return "", false, err
	}
	return sink.Create(ctx, id, sizeMB)
}

func (p testVolumeMutationProxy) EnsureQuota(ctx context.Context, id string, sizeMB int64) error {
	sink, err := p.sink()
	if err != nil {
		return err
	}
	return sink.EnsureQuota(ctx, id, sizeMB)
}

func (p testVolumeMutationProxy) RecoverInterruptedVolumeMutations(ctx context.Context) error {
	sink, err := p.sink()
	if err != nil {
		return err
	}
	return sink.RecoverInterruptedVolumeMutations(ctx)
}

func (p testVolumeMutationProxy) RenameVolume(ctx context.Context, oldName, newName string) error {
	sink, err := p.sink()
	if err != nil {
		return err
	}
	return sink.RenameVolume(ctx, oldName, newName)
}

func (p testVolumeMutationProxy) Destroy(ctx context.Context, id string) error {
	sink, err := p.sink()
	if err != nil {
		return err
	}
	return sink.Destroy(ctx, id)
}
