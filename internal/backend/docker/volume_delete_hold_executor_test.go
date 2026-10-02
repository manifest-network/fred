package docker

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/metrics/background"
)

func dueHold(t *testing.T, name string, lastAttempt time.Time) volumeDeleteHoldView {
	t.Helper()
	parsed, err := parseManagedVolumeName(name)
	require.NoError(t, err)
	return volumeDeleteHoldView{volume: parsed, reason: holdReasonDeadline, lastAttempt: lastAttempt}
}

// One pass retries the due holds, least recently attempted first, one bounded
// slice each, and stops when its budget can no longer fit a slice.
func TestVolumeDeleteHoldPassRetriesDueHoldsInOrderWithinBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		names := make([]string, 6)
		holds := volumeDeleteHoldSnapshot{holds: map[string]volumeDeleteHoldView{}}
		for i := range names {
			names[i] = canonicalVolumeName("550e8400-e29b-41d4-a716-446655440000", "app", i)
			// Older holds were attempted longer ago; index 0 is the stalest.
			holds.holds[names[i]] = dueHold(t, names[i], now.Add(-time.Duration(len(names)-i)*time.Minute))
		}
		b := newBackendForTest(&mockDockerClient{}, nil)
		b.volumes = &mockVolumeManager{VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return holds }}

		var order []string
		var sliceBudgets []time.Duration
		retry := backgroundHeldVolumeDeleteRetry(func(ctx context.Context, name string) error {
			deadline, ok := ctx.Deadline()
			require.True(t, ok, "every attempt runs under a slice deadline")
			sliceBudgets = append(sliceBudgets, time.Until(deadline))
			order = append(order, name)
			time.Sleep(20 * time.Second) // each attempt uses (at most) its whole slice
			return nil
		})
		ctx, cancel := context.WithTimeout(t.Context(), volumeDeleteHoldPassBudget)
		defer cancel()
		report := b.runVolumeDeleteHoldPass(ctx, now, retry)

		assert.Equal(t, names[:report.attempted], order, "least recently attempted first")
		assert.Equal(t, 3, report.attempted, "a 60 s pass fits three 20 s attempts and stops")
		for _, budget := range sliceBudgets {
			assert.LessOrEqual(t, budget, volumeDeleteHoldSlice, "no attempt gets more than one slice")
		}
	})
}

// The executor's retry takes only its own lease's namespace, exclusively, and
// never the global recovery gate: another lease mutates freely while a slice
// runs, and the same lease waits.
func TestVolumeDeleteHoldPassTakesOnlyTheLeaseNamespace(t *testing.T) {
	const leaseA = "550e8400-e29b-41d4-a716-446655440001"
	const leaseB = "550e8400-e29b-41d4-a716-446655440002"
	held := canonicalVolumeName(leaseA, "app", 0)
	other := canonicalVolumeName(leaseB, "app", 0)
	b := newBackendForTest(&mockDockerClient{}, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	b.volumes = &mockVolumeManager{
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot {
			return volumeDeleteHoldSnapshot{holds: map[string]volumeDeleteHoldView{held: dueHold(t, held, time.Time{})}}
		},
		RetryHeldVolumeDeleteFn: func(ctx context.Context, name string) error {
			assert.Equal(t, held, name)
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil
		},
	}
	installTestStorageMutationAdapters(b)

	var wg sync.WaitGroup
	wg.Go(func() {
		ctx, cancel := context.WithTimeout(context.Background(), volumeDeleteHoldPassBudget)
		defer cancel()
		b.backgroundMaintenance.retryHeldVolumeDeletes(ctx)
	})
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the pass never reached the held deletion")
	}

	parse := func(name string) managedVolumeName {
		parsed, err := parseManagedVolumeName(name)
		require.NoError(t, err)
		return parsed
	}
	otherCtx, cancelOther := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelOther()
	require.NoError(t, b.volumeAccess.mutateNamespace(otherCtx, []managedVolumeName{parse(other)},
		func(context.Context) error { return nil }),
		"another lease must not wait on the executor's slice")

	sameCtx, cancelSame := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelSame()
	err := b.volumeAccess.mutateNamespace(sameCtx, []managedVolumeName{parse(held)},
		func(context.Context) error { return nil })
	require.ErrorIs(t, err, context.DeadlineExceeded, "the retry holds its own lease's namespace exclusively")

	close(release)
	wg.Wait()
}

// The executor's first pass runs as soon as it starts; it stops with the
// Backend's lifetime.
func TestVolumeDeleteHoldLoopRunsAtOnceAndStopsWithTheBackend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBackendForTest(&mockDockerClient{}, nil)
		var passes atomic.Int32
		b.backgroundMaintenance = &backgroundMaintenanceCoordinator{
			retryHeldVolumeDeletesFn: func(ctx context.Context) volumeDeleteHoldPassReport {
				passes.Add(1)
				_, hasDeadline := ctx.Deadline()
				assert.True(t, hasDeadline, "each pass runs under its budget")
				return volumeDeleteHoldPassReport{}
			},
		}
		b.wg.Go(b.volumeDeleteHoldLoop)
		synctest.Wait()
		require.Equal(t, int32(1), passes.Load(), "the first pass must not wait for the first tick")
		time.Sleep(volumeDeleteHoldInterval)
		synctest.Wait()
		require.Equal(t, int32(2), passes.Load())
		b.stopCancel()
		b.wg.Wait()
		time.Sleep(10 * volumeDeleteHoldInterval)
		require.Equal(t, int32(2), passes.Load(), "the loop ends with the Backend")
	})
}

// A panic in a pass is contained and counted, and the next pass still runs.
func TestVolumeDeleteHoldIterationCountsPanics(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	var calls atomic.Int32
	b.backgroundMaintenance = &backgroundMaintenanceCoordinator{
		retryHeldVolumeDeletesFn: func(context.Context) volumeDeleteHoldPassReport {
			if calls.Add(1) == 1 {
				panic("injected hold executor failure")
			}
			return volumeDeleteHoldPassReport{}
		},
	}
	panics := background.CleanupPanicsTotal.WithLabelValues(volumeDeleteHoldComponent)
	before := testutil.ToFloat64(panics)
	b.runVolumeDeleteHoldIteration()
	assert.Equal(t, before+1, testutil.ToFloat64(panics))
	b.runVolumeDeleteHoldIteration()
	assert.Equal(t, int32(2), calls.Load(), "a panic cannot kill the executor")
	assert.Equal(t, before+1, testutil.ToFloat64(panics))
}

// When a hold leaves the removal phase, the executor resumes its lease's close
// at once, without waiting for the next reconcile.
func TestVolumeDeleteHoldExecutorResumesTheCloseWhenAHoldLeavesRemoval(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{})
	seedCloseDeprovisionLease(t, b, stores)
	name := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0)
	var held atomic.Bool
	held.Store(true)
	snapshot := func() volumeDeleteHoldSnapshot {
		if held.Load() {
			return pendingDeletes(name)
		}
		return volumeDeleteHoldSnapshot{}
	}
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			if held.Load() {
				return []string{name}, nil
			}
			return nil, nil
		},
		VolumeDeleteHoldsFn: snapshot,
		DestroyFn: func(context.Context, string) error {
			if held.Load() {
				return heldDeleteErr(name)
			}
			return nil
		},
		RetryHeldVolumeDeleteFn: func(context.Context, string) error {
			held.Store(false) // the deletion completes
			return nil
		},
	}
	installTestStorageMutationAdapters(b)
	require.Error(t, b.doDeprovisionForTest(t, t.Context(), closeDeprovisionLeaseUUID))
	_, found, err := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.True(t, found, "the close is pending on the held deletion")

	b.runVolumeDeleteHoldIteration()
	_, found, err = stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	assert.False(t, found, "the executor resumed and completed the close")
	closeCloseRecoveryBackend(t, b, stores)
}

// A close retry and an HTTP Deprovision of a two-volume lease (one volume held,
// one already destroyed) answer within budget while the hold executor holds the
// lease's namespace: neither name needs the lock, and the answer is the
// observable 503 lifecycle_pending, never a breaker-counted failure.
func TestHeldLeaseDeprovisionAnswersWithinBudgetWhileTheExecutorHoldsItsNamespace(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{})
	seedCloseLeaseFixture(t, b, stores, closeDeprovisionLeaseUUID, "", 2)
	heldName := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0)
	goneName := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 1)
	var destroys atomic.Int32
	b.volumes = &mockVolumeManager{
		ListFn:              func() ([]string, error) { return []string{heldName}, nil },
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return pendingDeletes(heldName) },
		PrecheckDestroyFn: func(name managedVolumeName) (destroyPrecheckVerdict, error) {
			switch name.value() {
			case heldName:
				return destroyPrecheckHeld, heldDeleteErr(heldName)
			case goneName:
				return destroyPrecheckGone, nil
			}
			return destroyPrecheckNeedsLock, nil
		},
		DestroyFn: func(context.Context, string) error {
			destroys.Add(1)
			return nil
		},
	}
	installTestStorageMutationAdapters(b)

	// The executor's slice holds the lease's namespace for the whole test.
	parsed, err := parseManagedVolumeName(heldName)
	require.NoError(t, err)
	holding := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		_ = b.volumeAccess.mutateNamespace(context.Background(), []managedVolumeName{parsed},
			func(context.Context) error {
				close(holding)
				<-release
				return nil
			})
	})
	<-holding
	defer func() { close(release); wg.Wait() }()

	for _, caller := range []string{"close retry", "HTTP deprovision"} {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		started := time.Now()
		var err error
		if caller == "close retry" {
			err = b.doDeprovisionForTest(t, ctx, closeDeprovisionLeaseUUID)
		} else {
			err = b.Deprovision(ctx, closeDeprovisionLeaseUUID)
		}
		cancel()
		elapsed := time.Since(started)
		require.Error(t, err, caller)
		assert.True(t, shared.IsLifecyclePending(err), "%s answers 503 lifecycle_pending: %v", caller, err)
		assert.Less(t, elapsed, 5*time.Second, "%s must not wait behind the executor's slice", caller)
	}
	assert.Zero(t, destroys.Load(), "neither name needed the locked Destroy")
	require.NoError(t, b.terminalStorageAuthorityError())
	closeCloseRecoveryBackend(t, b, stores)
}

// A periodic reconcile skips, from memory, every close that waits only on held
// deletions: no Docker, disk or store I/O for them, so the tick's later
// stages keep their budget however many such closes pile up.
func TestReconcileTickSkipsClosesWaitingOnlyOnHeldDeletes(t *testing.T) {
	dir := t.TempDir()
	mock := &mockDockerClient{}
	b, stores := openCloseRecoveryBackend(t, dir, mock, &mockVolumeManager{})
	const closes = 50
	leases := make([]string, closes)
	names := make([]string, closes)
	for i := range closes {
		leases[i] = fmt.Sprintf("550e8400-e29b-41d4-a716-4466554%05d", 40000+i)
		names[i] = canonicalVolumeName(leases[i], "app", 0)
		seedCloseLeaseFixture(t, b, stores, leases[i], "", 1)
	}
	var destroys atomic.Int32
	b.volumes = &mockVolumeManager{
		ListFn:              func() ([]string, error) { return names, nil },
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return pendingDeletes(names...) },
		PrecheckDestroyFn: func(name managedVolumeName) (destroyPrecheckVerdict, error) {
			return destroyPrecheckHeld, heldDeleteErr(name.value())
		},
		DestroyFn: func(context.Context, string) error {
			destroys.Add(1)
			return nil
		},
	}
	installTestStorageMutationAdapters(b)
	for _, lease := range leases {
		require.Error(t, b.doDeprovisionForTest(t, t.Context(), lease))
	}
	generations := make(map[string]int, closes)
	for _, lease := range leases {
		claim, found, err := stores.callbacks.GetCloseIntent(lease)
		require.NoError(t, err)
		require.True(t, found)
		generations[lease] = claim.ExecutionGeneration().Number()
	}
	var downs atomic.Int32
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		downs.Add(1)
		return nil
	}}
	var volumeLists, proofLists, dockerLists atomic.Int32
	volumes := b.volumes.(*mockVolumeManager)
	volumes.ListFn = func() ([]string, error) { volumeLists.Add(1); return names, nil }
	volumes.ListForProofFn = func(context.Context) ([]string, error) { proofLists.Add(1); return names, nil }
	mock.ListManagedContainersFn = func(context.Context) ([]ContainerInfo, error) {
		dockerLists.Add(1)
		return nil, nil
	}

	ctx, cancel := b.periodicReconcileContext()
	defer cancel()
	started := time.Now()
	require.NoError(t, b.reconcileStateAndOperations(ctx), "every stage of the tick ran")
	elapsed := time.Since(started)
	assert.Less(t, elapsed, periodicReconcileTimeout/2, "the tick keeps its budget for later stages")
	require.NoError(t, ctx.Err(), "the tick finished within its budget")
	// A resumed close would classify its previous generation from a Docker
	// and a volume inventory read; a skipped one reads nothing.
	assert.Zero(t, proofLists.Load()+volumeLists.Load(), "no volume inventory read for a held close")
	assert.Less(t, dockerLists.Load(), int32(closes), "Docker reads do not scale with the held closes")
	assert.Zero(t, downs.Load(), "no held close was retried")
	for _, lease := range leases {
		claim, found, err := stores.callbacks.GetCloseIntent(lease)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, generations[lease], claim.ExecutionGeneration().Number(),
			"a skipped close keeps its durable generation")
	}
	assert.Zero(t, destroys.Load())
	closeCloseRecoveryBackend(t, b, stores)
}

// validatingXFSManager is a real XFS manager whose Validate runs only the
// startup scan: the CAP_SYS_ADMIN probe Validate also makes is unavailable
// to an unprivileged test process.
type validatingXFSManager struct{ *xfsVolumeManager }

func (m validatingXFSManager) Validate() error { return m.loadProjectIDs() }

// Acceptance (ENG-1117): Start serves with a recovered delete stage whose
// cleanup always fails. Start returns nil without any storage-authority
// latch, the hold executor holds the deletion, and Stop is clean.
func TestStartServesWithHeldDeleteStage(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root bypasses directory permissions, so the removal would not fail")
	}
	// Another lease provisions through the nominal mocked path.
	dockerClient := &mockDockerClient{
		PingFn:      func(context.Context) error { return nil },
		CloseFn:     func() error { return nil },
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}
	b := newBackendForProvisionTest(t, dockerClient, nil)
	bindTestStorageIdentity(t, b, dockerClient)
	t.Cleanup(b.stopCancel)
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok)
	b.operationSettlement = operations
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	dataPath := t.TempDir()
	stage := mustXFSDeleteStage(t, xfsDeleteTestProjectID, xfsStageTestVolume)
	require.NoError(t, os.Mkdir(stage.hostPath(dataPath), 0o700))
	locked := filepath.Join(stage.volumeID.hostPath(dataPath), writablePathSubdir, "locked")
	require.NoError(t, os.MkdirAll(locked, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(locked, "pinned"), []byte("x"), 0o600))
	require.NoError(t, os.Chmod(locked, 0o500))
	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
	// A second recovered deletion whose final directory is already gone: Start
	// sizes it as residual and the executor finishes it.
	gone := mustXFSDeleteStage(t, xfsDeleteTestProjectID+1, "fred-550e8400-e29b-41d4-a716-446655440001-app-0")
	require.NoError(t, os.Mkdir(gone.hostPath(dataPath), 0o700))
	installXFSQuotaFixture(t, "")
	mgr := newXfsManagerForTest(dataPath)
	mgr.inlineDeletes.Store(false) // a fresh process: Start enables inline deletes
	b.volumes = validatingXFSManager{mgr}
	installTestStorageMutationAdapters(b)
	bindRetentionOrphanPrunerForTest(t, b)
	holdsGauge := volumeDeleteHolds.WithLabelValues(volumeDeleteHoldPhaseRemoval)
	b.sampleVolumeDeleteHoldMetrics()
	gaugeBefore := testutil.ToFloat64(holdsGauge)
	// New's identity probe scans before Start's Validate does: the second scan
	// must accept the same stages without a duplicate name.
	require.NoError(t, mgr.loadProjectIDs())
	_, err := attestManagedVolumeInventory(t.Context(), mgr)
	require.NoError(t, err, "the probe's inventory proof accepts both deletions")

	stop := sync.OnceValue(b.Stop)
	t.Cleanup(func() { _ = stop() })
	require.NoError(t, b.Start(context.Background()))
	require.NoError(t, b.terminalStorageAuthorityError(), "a held deletion never latches")
	require.NoError(t, b.Health(t.Context()), "the Backend reports healthy")
	assert.True(t, mgr.inlineDeletes.Load(), "Start enables inline deletes once the executor runs")
	require.Eventually(t, func() bool {
		hold, held := mgr.VolumeDeleteHolds().holds[stage.volumeID.value()]
		return held && hold.attempts >= 1
	}, 10*time.Second, 20*time.Millisecond, "the executor's first pass retries the recovered hold at once")
	hold := mgr.VolumeDeleteHolds().holds[stage.volumeID.value()]
	assert.Equal(t, holdReasonUndeletable, hold.reason)
	b.sampleVolumeDeleteHoldMetrics()
	assert.Equal(t, gaugeBefore+1, testutil.ToFloat64(holdsGauge), "the gauge reports the held deletion")
	assert.DirExists(t, stage.hostPath(dataPath), "the stage keeps its authority")
	mgr.mu.Lock()
	reserved := mgr.volumeToID[stage.volumeID.value()]
	mgr.mu.Unlock()
	assert.Equal(t, stage.projID, reserved, "the project ID stays reserved")
	require.Eventually(t, func() bool {
		_, held := mgr.VolumeDeleteHolds().holds[gone.volumeID.value()]
		_, statErr := os.Lstat(gone.hostPath(dataPath))
		return !held && errors.Is(statErr, fs.ErrNotExist)
	}, 10*time.Second, 20*time.Millisecond, "the executor finishes the residual deletion")

	const otherLease = "550e8400-e29b-41d4-a716-446655449999"
	// The bound-identity fixture makes every profile stateless, so this
	// provision never reaches the held manager's namespace.
	request := newProvisionRequest(otherLease, "tenant-b", "docker-small", 1, validManifestJSON("nginx:latest"))
	require.NoError(t, b.Provision(t.Context(), request))
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		other := b.provisions[otherLease]
		return other != nil && other.Status == backend.ProvisionStatusReady
	}, provisionFlowTimeout, 20*time.Millisecond, "another lease provisions while the deletion is held")
	require.NoError(t, b.terminalStorageAuthorityError())
	require.NoError(t, stop(), "Stop is clean with a hold outstanding")
}

// Start never waits on a tenant tree: a deletion first requested during
// Start is handed to the executor without an attempt, so several closes whose
// volumes need long removals do not stretch Start.
func TestStartDefersFirstTimeDeletesToTheExecutor(t *testing.T) {
	dataPath := t.TempDir()
	mgr := newXfsManagerForTest(dataPath)
	mgr.inlineDeletes.Store(false)
	installLoggingXFSQuota(t)
	started := time.Now()
	for i := range 3 {
		name := canonicalVolumeName("550e8400-e29b-41d4-a716-446655440000", "app", i)
		volumePath := filepath.Join(dataPath, name)
		require.NoError(t, os.Mkdir(volumePath, 0o700))
		require.NoError(t, writeProjectIDFile(volumePath, xfsDeleteTestProjectID+uint32(i)))
		buildDirectoryChain(t, volumePath, writablePathSubdir, 64)
		require.ErrorIs(t, mgr.Destroy(t.Context(), name), ErrVolumeDeleteHeld)
		assert.DirExists(t, filepath.Join(volumePath, writablePathSubdir), "nothing is removed during Start")
	}
	assert.Less(t, time.Since(started), liveXFSDeleteBudget, "three deferrals take no removal time")
	removal, _ := mgr.VolumeDeleteHolds().phaseCounts()
	assert.Equal(t, 3, removal)
}

// HTTP Deprovision of a close waiting only on held deletions answers the
// observable pending without starting a new close generation or touching the
// substrate again.
func TestDeprovisionOfADeleteHeldCloseAnswersPendingWithoutRetrying(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{})
	seedCloseDeprovisionLease(t, b, stores)
	name := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0)
	var destroys atomic.Int32
	b.volumes = &mockVolumeManager{
		ListFn:              func() ([]string, error) { return []string{name}, nil },
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return pendingDeletes(name) },
		DestroyFn: func(context.Context, string) error {
			destroys.Add(1)
			return heldDeleteErr(name)
		},
	}
	installTestStorageMutationAdapters(b)
	require.Error(t, b.doDeprovisionForTest(t, t.Context(), closeDeprovisionLeaseUUID))
	before, found, err := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int32(1), destroys.Load())
	var downs atomic.Int32
	b.compose = &mockComposeExecutor{DownFn: func(context.Context, string, time.Duration) error {
		downs.Add(1)
		return nil
	}}

	err = b.Deprovision(t.Context(), closeDeprovisionLeaseUUID)
	require.Error(t, err)
	assert.True(t, shared.IsLifecyclePending(err), "the answer is the observable pending: %v", err)
	after, found, err := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, before.ExecutionGeneration().Number(), after.ExecutionGeneration().Number(),
		"no new close generation is started")
	assert.Equal(t, int32(1), destroys.Load(), "the held volume is not destroyed again")
	assert.Zero(t, downs.Load(), "the substrate is not touched again")
	closeCloseRecoveryBackend(t, b, stores)
}

// The close predicate answers from memory, and only when every volume slot is
// removal-held and a projection vouches that no container remains.
func TestCloseAwaitsHeldDeletesPredicate(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{})
	seedCloseLeaseFixture(t, b, stores, closeDeprovisionLeaseUUID, "", 2)
	first := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0)
	second := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 1)
	b.volumes = &mockVolumeManager{
		ListFn:    func() ([]string, error) { return []string{first, second}, nil },
		DestroyFn: func(_ context.Context, name string) error { return heldDeleteErr(name) },
	}
	installTestStorageMutationAdapters(b)
	require.Error(t, b.doDeprovisionForTest(t, t.Context(), closeDeprovisionLeaseUUID))
	claim, found, err := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.True(t, found)

	retained := pendingDeletes(retainedName(first))
	retained.holds[second] = pendingDeletes(second).holds[second]
	assert.True(t, b.closeAwaitsHeldDeletes(claim, pendingDeletes(first, second)))
	assert.True(t, b.closeAwaitsHeldDeletes(claim, retained), "a slot held under its retained name counts")
	assert.False(t, b.closeAwaitsHeldDeletes(claim, pendingDeletes(first)), "every slot must be held")
	assert.False(t, b.closeAwaitsHeldDeletes(claim, residualDeletes(10, first, second)),
		"a residual hold no longer keeps the close waiting")

	b.provisionsMu.Lock()
	projection := b.provisions[closeDeprovisionLeaseUUID]
	require.NotNil(t, projection)
	projection.ContainerIDs = []string{"remaining"}
	b.provisionsMu.Unlock()
	assert.False(t, b.closeAwaitsHeldDeletes(claim, pendingDeletes(first, second)),
		"a remaining container keeps the close running")

	b.provisionsMu.Lock()
	delete(b.provisions, closeDeprovisionLeaseUUID)
	b.provisionsMu.Unlock()
	assert.False(t, b.closeAwaitsHeldDeletes(claim, pendingDeletes(first, second)),
		"without a projection nothing vouches that no container remains")
	closeCloseRecoveryBackend(t, b, stores)
}

// The close-intent gauge counts closes waiting only on held deletions, so
// close-age alerting can exclude them.
func TestCloseIntentsDeleteHeldGauge(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{})
	seedCloseDeprovisionLease(t, b, stores)
	name := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0)
	b.volumes = &mockVolumeManager{
		ListFn:              func() ([]string, error) { return []string{name}, nil },
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return pendingDeletes(name) },
		DestroyFn:           func(context.Context, string) error { return heldDeleteErr(name) },
	}
	installTestStorageMutationAdapters(b)
	require.Error(t, b.doDeprovisionForTest(t, t.Context(), closeDeprovisionLeaseUUID))
	b.sampleCloseIntentMetrics(time.Now())
	assert.Equal(t, float64(1), testutil.ToFloat64(closeIntentsDeleteHeld))
	assert.Equal(t, float64(1), testutil.ToFloat64(pendingCloseIntents))

	b.volumes = &mockVolumeManager{ListFn: func() ([]string, error) { return []string{name}, nil }}
	b.sampleCloseIntentMetrics(time.Now())
	assert.Equal(t, float64(0), testutil.ToFloat64(closeIntentsDeleteHeld),
		"a close with an unheld volume is not excluded from close-age alerting")
	closeCloseRecoveryBackend(t, b, stores)
}

// A failed restore's rollback routes its destroy of a restore-created volume
// through the same pre-lock check: a held deletion answers at once, with no
// destroy and no lock wait, even while the executor holds the lease's
// namespace, and the rollback stays pending until the deletion finishes.
func TestRestoreRollbackAnswersAHeldCreatedVolumeWithoutTheLock(t *testing.T) {
	f := newInterruptedRestoreRecoveryFixture(
		t, 2, []string{"exited"}, backend.ProvisionStatusFailed, true,
	)
	volumes, ok := f.b.volumes.(*mockVolumeManager)
	require.True(t, ok)
	created := canonicalVolumeName(f.spec.LeaseUUID, f.spec.Items[0].ServiceName, 1)
	var held atomic.Bool
	held.Store(true)
	listed := volumes.ListForProofFn
	volumes.ListForProofFn = func(ctx context.Context) ([]string, error) {
		names, err := listed(ctx)
		if held.Load() {
			names = append(names, created)
		}
		return names, err
	}
	volumes.VolumeDeleteHoldsFn = func() volumeDeleteHoldSnapshot {
		if held.Load() {
			return pendingDeletes(created)
		}
		return volumeDeleteHoldSnapshot{}
	}
	volumes.PrecheckDestroyFn = func(name managedVolumeName) (destroyPrecheckVerdict, error) {
		if name.value() != created {
			return destroyPrecheckNeedsLock, nil
		}
		if held.Load() {
			return destroyPrecheckHeld, heldDeleteErr(created)
		}
		return destroyPrecheckGone, nil
	}
	volumes.DestroyFn = func(_ context.Context, name string) error {
		f.record("destroy:" + name)
		return nil
	}

	// First pass: the adopted source volume is re-quarantined, and the held
	// restore-created volume keeps the rollback pending.
	retries := operationIntentRecoveryCleanupRetriesTotal.WithLabelValues("restore")
	before := testutil.ToFloat64(retries)
	require.NoError(t, f.b.recoverOperationIntents(t.Context()))
	assert.Equal(t, before+1, testutil.ToFloat64(retries), "the rollback remains pending for the next pass")
	assert.Equal(t, []string{"remove:restore-container-0", "re-quarantine"}, f.snapshotEvents())
	require.NoError(t, f.b.terminalStorageAuthorityError(), "a held deletion never latches")
	intents, err := f.b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1, "the rollback stays pending")

	// Second pass while the executor holds the lease's namespace: the held
	// name answers without waiting for the lock.
	parsed, err := parseManagedVolumeName(created)
	require.NoError(t, err)
	holding := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		_ = f.b.volumeAccess.mutateNamespace(context.Background(), []managedVolumeName{parsed},
			func(context.Context) error {
				close(holding)
				<-release
				return nil
			})
	})
	<-holding
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	started := time.Now()
	err = f.b.recoverOperationIntents(ctx)
	cancel()
	assert.Less(t, time.Since(started), 5*time.Second, "the rollback must not wait behind the executor's slice")
	require.NoError(t, err)
	assert.Equal(t, before+2, testutil.ToFloat64(retries))
	close(release)
	wg.Wait()

	// The deletion finishes: the rollback completes without destroying anything.
	held.Store(false)
	require.NoError(t, f.b.recoverOperationIntents(t.Context()))
	require.NoError(t, f.b.reconcileRestoringWithAuthority(t.Context(), *f.source))
	assertInterruptedRestoreSettled(t, f)
	for _, event := range f.snapshotEvents() {
		assert.NotContains(t, event, "destroy:", "the held name was never destroyed by the rollback")
	}
}

// Start with three pending closes never waits on their volumes' trees: each
// close's deletion is handed to the executor without an attempt during Start,
// and once the executor runs it removes them and resumes the closes.
func TestStartWithPendingClosesDefersTheirDeletesToTheExecutor(t *testing.T) {
	dir := t.TempDir()
	mock := &mockDockerClient{
		PingFn:  func(context.Context) error { return nil },
		CloseFn: func() error { return nil },
	}
	b, stores := openCloseRecoveryBackend(t, dir, mock, &mockVolumeManager{})
	leases := []string{
		"550e8400-e29b-41d4-a716-446655440601",
		"550e8400-e29b-41d4-a716-446655440602",
		"550e8400-e29b-41d4-a716-446655440603",
	}
	names := make([]string, len(leases))
	for i, lease := range leases {
		names[i] = canonicalVolumeName(lease, "app", 0)
		seedCloseLeaseFixture(t, b, stores, lease, "", 1)
	}
	// Each close was interrupted before its volume deletion finished.
	b.volumes = &mockVolumeManager{
		ListFn:    func() ([]string, error) { return names, nil },
		DestroyFn: func(_ context.Context, name string) error { return heldDeleteErr(name) },
	}
	for _, lease := range leases {
		require.Error(t, b.doDeprovisionForTest(t, t.Context(), lease))
	}

	dataPath := t.TempDir()
	for i, name := range names {
		volumePath := filepath.Join(dataPath, name)
		require.NoError(t, os.Mkdir(volumePath, 0o700))
		require.NoError(t, writeProjectIDFile(volumePath, xfsDeleteTestProjectID+uint32(i)))
		buildDirectoryChain(t, volumePath, writablePathSubdir, 64)
	}
	installLoggingXFSQuota(t)
	mgr := newXfsManagerForTest(dataPath)
	mgr.inlineDeletes.Store(false) // a fresh process: Start enables inline deletes
	b.volumes = validatingXFSManager{mgr}
	bindRetentionOrphanPrunerForTest(t, b)
	// Hold the executor's first pass until the test has observed Start's work.
	pass := b.backgroundMaintenance.retryHeldVolumeDeletesFn
	gate := make(chan struct{})
	b.backgroundMaintenance.retryHeldVolumeDeletesFn = func(ctx context.Context) volumeDeleteHoldPassReport {
		select {
		case <-gate:
		case <-ctx.Done():
			return volumeDeleteHoldPassReport{}
		}
		return pass(ctx)
	}

	stop := sync.OnceValue(b.Stop)
	t.Cleanup(func() { _ = stop() })
	started := time.Now()
	require.NoError(t, b.Start(context.Background()))
	assert.Less(t, time.Since(started), liveXFSDeleteBudget, "Start does not wait on any tenant tree")
	holds := mgr.VolumeDeleteHolds()
	for _, name := range names {
		hold, held := holds.holds[name]
		require.True(t, held, "%s: the close's deletion is handed to the executor", name)
		assert.Equal(t, holdReasonDeadline, hold.reason)
		assert.Zero(t, hold.attempts, "%s: no removal is attempted during Start", name)
		assert.DirExists(t, filepath.Join(dataPath, name, writablePathSubdir), "%s: nothing is removed during Start", name)
	}
	for _, lease := range leases {
		_, found, err := stores.callbacks.GetCloseIntent(lease)
		require.NoError(t, err)
		assert.True(t, found, "the close stays pending on its held deletion")
	}

	close(gate)
	require.Eventually(t, func() bool {
		for _, lease := range leases {
			if _, found, err := stores.callbacks.GetCloseIntent(lease); err != nil || found {
				return false
			}
		}
		return true
	}, 30*time.Second, 50*time.Millisecond, "the executor removes the volumes and resumes the closes")
	for _, name := range names {
		assert.NoDirExists(t, filepath.Join(dataPath, name))
	}
	assert.Empty(t, mgr.VolumeDeleteHolds().holds)
	require.NoError(t, b.terminalStorageAuthorityError())
	require.NoError(t, stop())
}
