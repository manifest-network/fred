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

// One pass runs attempts of different leases in parallel, up to its worker
// bound, so one very large deletion cannot serialize every other lease's; two
// holds of the same lease never run at once.
func TestVolumeDeleteHoldPassRunsDifferentLeasesInParallel(t *testing.T) {
	const leaseA = "550e8400-e29b-41d4-a716-446655440011"
	const leaseB = "550e8400-e29b-41d4-a716-446655440012"
	a0, a1 := canonicalVolumeName(leaseA, "app", 0), canonicalVolumeName(leaseA, "app", 1)
	b0 := canonicalVolumeName(leaseB, "app", 0)
	base := time.Now().Add(-time.Hour)
	holds := volumeDeleteHoldSnapshot{holds: map[string]volumeDeleteHoldView{
		a0: dueHold(t, a0, base), a1: dueHold(t, a1, base.Add(time.Minute)), b0: dueHold(t, b0, base.Add(2*time.Minute)),
	}}
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.volumes = &mockVolumeManager{VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return holds }}

	var mu sync.Mutex
	active := map[string]int{}
	running, peak := 0, 0
	sameLeaseOverlap := false
	proceed := make(chan struct{})
	retry := backgroundHeldVolumeDeleteRetry(func(ctx context.Context, name string) error {
		parsed, err := parseManagedVolumeName(name)
		require.NoError(t, err)
		lease := managedVolumeLeaseUUID(parsed)
		mu.Lock()
		active[lease]++
		sameLeaseOverlap = sameLeaseOverlap || active[lease] > 1
		running++
		peak = max(peak, running)
		mu.Unlock()
		select {
		case <-proceed:
		case <-ctx.Done():
		}
		mu.Lock()
		active[lease]--
		running--
		mu.Unlock()
		return nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), volumeDeleteHoldPassBudget)
	defer cancel()
	reports := make(chan volumeDeleteHoldPassReport, 1)
	go func() { reports <- b.runVolumeDeleteHoldPass(ctx, time.Now(), retry) }()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return peak >= 2
	}, 10*time.Second, 5*time.Millisecond, "two leases' attempts run at once")
	close(proceed)
	report := <-reports
	assert.Equal(t, 3, report.attempted)
	assert.False(t, sameLeaseOverlap, "two holds of one lease never run at once")
	assert.Equal(t, 2, peak, "no more attempts run at once than the worker bound")
}

// A pass that moved a hold forward is followed at once by the next; a pass
// that did not waits for the executor's interval.
func TestVolumeDeleteHoldLoopStartsTheNextPassAtOnceAfterProgress(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := newBackendForTest(&mockDockerClient{}, nil)
		var passes atomic.Int32
		b.backgroundMaintenance = &backgroundMaintenanceCoordinator{
			retryHeldVolumeDeletesFn: func(context.Context) volumeDeleteHoldPassReport {
				return volumeDeleteHoldPassReport{progressed: passes.Add(1) <= 3}
			},
		}
		b.wg.Go(b.volumeDeleteHoldLoop)
		synctest.Wait()
		require.Equal(t, int32(4), passes.Load(), "three progressing passes are followed at once, the fourth waits")
		time.Sleep(volumeDeleteHoldInterval)
		synctest.Wait()
		require.Equal(t, int32(5), passes.Load())
		b.stopCancel()
		b.wg.Wait()
	})
}

// A hold whose attempts never reach the manager keeps its stale lastAttempt,
// yet still rotates behind the others: the executor orders by its own
// dispatch record too, so the holds a pass could not fit go first next time.
func TestVolumeDeleteHoldPassRotatesHoldsThatNeverReachTheManager(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		holds := volumeDeleteHoldSnapshot{holds: map[string]volumeDeleteHoldView{}}
		names := make([]string, 10)
		for i := range names {
			names[i] = canonicalVolumeName(fmt.Sprintf("550e8400-e29b-41d4-a716-4466554%05d", 41000+i), "app", 0)
			holds.holds[names[i]] = dueHold(t, names[i], time.Time{})
		}
		b := newBackendForTest(&mockDockerClient{}, nil)
		b.volumes = &mockVolumeManager{VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return holds }}
		var mu sync.Mutex
		var order []string
		retry := backgroundHeldVolumeDeleteRetry(func(ctx context.Context, name string) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			<-ctx.Done() // every attempt uses its whole slice and records nothing
			return ctx.Err()
		})
		pass := func() volumeDeleteHoldPassReport {
			ctx, cancel := context.WithTimeout(t.Context(), volumeDeleteHoldPassBudget)
			defer cancel()
			return b.runVolumeDeleteHoldPass(ctx, time.Now(), retry)
		}
		first := pass()
		require.Equal(t, 8, first.attempted, "a 60 s pass fits four rounds of two 15 s attempts")
		assert.False(t, first.progressed, "attempts that never reached the manager are no progress")
		assert.ElementsMatch(t, names[:8], order)
		order = nil
		second := pass()
		require.GreaterOrEqual(t, second.attempted, 2)
		assert.ElementsMatch(t, names[8:], order[:2], "the holds the first pass could not fit go first")
	})
}

// The executor classifies an attempt from the hold before and after it.
func TestClassifyHeldDeleteAttempt(t *testing.T) {
	t.Parallel()

	now := time.Now()
	parsed, err := parseManagedVolumeName(xfsStageTestVolume)
	require.NoError(t, err)
	before := volumeDeleteHoldView{volume: parsed, stage: "stage", reason: holdReasonRecovered, attempts: 1}
	with := func(edit func(*volumeDeleteHoldView)) volumeDeleteHoldView {
		view := before
		view.attempts = 2
		edit(&view)
		return view
	}
	for _, tc := range []struct {
		name      string
		after     volumeDeleteHoldView
		stillHeld bool
		want      heldDeleteAttempt
	}{
		{"completed", volumeDeleteHoldView{}, false, heldDeleteAttempt{progressed: true, releasedCaller: true}},
		{"never reached the manager", before, true, heldDeleteAttempt{}},
		{"slice ran out after removing content", with(func(v *volumeDeleteHoldView) {
			v.reason, v.nextAttempt, v.contentRemoved = holdReasonDeadline, now, true
		}), true, heldDeleteAttempt{progressed: true}},
		// A slice spent in a hung quota command (the zero-usage proof) ends in
		// the same reason but removed nothing: no immediate next pass.
		{"slice ran out without removing content", with(func(v *volumeDeleteHoldView) {
			v.reason, v.nextAttempt = holdReasonDeadline, now
		}), true, heldDeleteAttempt{}},
		{"stopped without removing content", with(func(v *volumeDeleteHoldView) {
			v.reason, v.nextAttempt = holdReasonStopped, now
		}), true, heldDeleteAttempt{}},
		{"refused and backing off", with(func(v *volumeDeleteHoldView) {
			v.reason, v.nextAttempt = holdReasonUndeletable, now.Add(time.Minute)
		}), true, heldDeleteAttempt{}},
		{"phase changed", with(func(v *volumeDeleteHoldView) {
			v.phase, v.reason, v.nextAttempt = holdPhaseUnsized, holdReasonUsageUnprovable, now
		}), true, heldDeleteAttempt{progressed: true}},
		{"left the removal phase", with(func(v *volumeDeleteHoldView) {
			v.phase, v.reason, v.nextAttempt = holdPhaseResidual, holdReasonUsageNonzero, now.Add(time.Minute)
		}), true, heldDeleteAttempt{progressed: true, releasedCaller: true}},
		{"a different stage", with(func(v *volumeDeleteHoldView) { v.stage = "other" }), true, heldDeleteAttempt{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, classifyHeldDeleteAttempt(before, tc.after, tc.stillHeld, now))
		})
	}
	unsized := before
	unsized.phase = holdPhaseUnsized
	stillUnsized := unsized
	stillUnsized.attempts, stillUnsized.reason, stillUnsized.nextAttempt = 2, holdReasonUsageUnprovable, now
	assert.Equal(t, heldDeleteAttempt{}, classifyHeldDeleteAttempt(unsized, stillUnsized, true, now),
		"an unsized hold that stays unsized is no progress, so a broken quota report cannot spin the executor")
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

// A close resume that finds its lease's command fence busy is owed, not
// dropped: the command holding the fence may have answered from the state
// before the hold ended, so the next pass resumes the close.
func TestVolumeDeleteHoldExecutorRetriesACloseResumeItFoundBusy(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{})
	seedCloseDeprovisionLease(t, b, stores)
	name := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0)
	var held atomic.Bool
	held.Store(true)
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			if held.Load() {
				return []string{name}, nil
			}
			return nil, nil
		},
		DestroyFn: func(context.Context, string) error {
			if held.Load() {
				return heldDeleteErr(name)
			}
			return nil
		},
	}
	installTestStorageMutationAdapters(b)
	require.Error(t, b.doDeprovisionForTest(t, t.Context(), closeDeprovisionLeaseUUID))
	parsed, err := parseManagedVolumeName(name)
	require.NoError(t, err)

	held.Store(false) // the hold executor finished the deletion
	unlock := b.commandFence.Lock(closeDeprovisionLeaseUUID)
	b.resumeClosesAfterHeldDeletes([]managedVolumeName{parsed})
	unlock()
	_, found, err := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.True(t, found, "a busy lease is not resumed")

	b.resumeClosesAfterHeldDeletes(nil) // the next pass
	_, found, err = stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	assert.False(t, found, "the owed resume completed the close on the next pass")
	assert.Empty(t, b.holdExecutor.takeResumes(), "a completed resume is no longer owed")
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
// stages keep their budget however many such closes pile up. Half the closes
// are mixed: one held volume plus a slot whose volume is already destroyed or
// never existed (a stateless service), which counts as done.
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
		seedCloseLeaseFixture(t, b, stores, leases[i], "", 1+i%2)
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

// A recovered deletion whose final path is gone, whose caller a previous
// process may already have settled (no close, reaping record or operation
// remains to count it), and whose footprint Start cannot read, fails disk
// admission closed: when Start returns, every disk-bearing allocation is
// refused while diskless ones and /health are unaffected. Once the executor
// sizes it, admission reopens with the footprint counted.
func TestStartWithholdsDiskAdmissionForAnUnsizedRecoveredDeletion(t *testing.T) {
	dockerClient := &mockDockerClient{
		PingFn:  func(context.Context) error { return nil },
		CloseFn: func() error { return nil },
	}
	b := newBackendForProvisionTest(t, dockerClient, nil)
	bindTestStorageIdentity(t, b, dockerClient)
	t.Cleanup(b.stopCancel)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	dataPath := t.TempDir()
	gone := mustXFSDeleteStage(t, xfsDeleteTestProjectID, "fred-550e8400-e29b-41d4-a716-446655440001-app-0")
	require.NoError(t, os.Mkdir(gone.hostPath(dataPath), 0o700))
	sizable := filepath.Join(t.TempDir(), "sizable")
	t.Setenv("FRED_TEST_XFS_SIZABLE", sizable)
	installXFSQuotaFixture(t, fmt.Sprintf(`case "$*" in
  *"report -p -b -n -N"*) [ -e "$FRED_TEST_XFS_SIZABLE" ] || exit 23; printf '#%d 5 0 2048 0\n' ;;
  *"report -p -i -n -N"*) printf '#%d 1 0 0 0\n' ;;
esac`, gone.projID, gone.projID))
	mgr := newXfsManagerForTest(dataPath)
	mgr.inlineDeletes.Store(false)
	b.volumes = validatingXFSManager{mgr}
	installTestStorageMutationAdapters(b)
	bindRetentionOrphanPrunerForTest(t, b)
	// The test drives the executor's passes itself.
	pass := b.backgroundMaintenance.retryHeldVolumeDeletesFn
	b.backgroundMaintenance.retryHeldVolumeDeletesFn = func(ctx context.Context) volumeDeleteHoldPassReport {
		<-ctx.Done()
		return volumeDeleteHoldPassReport{}
	}
	stop := sync.OnceValue(b.Stop)
	t.Cleanup(func() { _ = stop() })

	require.NoError(t, b.Start(context.Background()))
	require.Equal(t, holdPhaseUnsized, heldForTest(t, mgr, gone.volumeID.value()).phase)
	stats := b.pool.Stats()
	assert.True(t, stats.DiskAccountingHeld, "Start returns with disk admission withheld")
	assert.Zero(t, stats.AvailableDiskMB())
	disk := shared.SKUResourceSnapshot{SKU: "disk", CPUCores: 0.1, MemoryMB: 16, DiskMB: 64}
	diskless := shared.SKUResourceSnapshot{SKU: "diskless", CPUCores: 0.1, MemoryMB: 16}
	require.ErrorIs(t, b.pool.TryAllocateResolved("550e8400-e29b-41d4-a716-446655449998-app-0", "tenant-b", disk),
		shared.ErrDiskAccountingIncomplete)
	require.NoError(t, b.pool.TryAllocateResolved("550e8400-e29b-41d4-a716-446655449997-app-0", "tenant-b", diskless))
	require.NoError(t, b.Health(t.Context()), "an unsized hold withholds disk, not the backend")
	b.sampleVolumeDeleteHoldMetrics()
	assert.Equal(t, float64(1), testutil.ToFloat64(volumeDeleteHolds.WithLabelValues(volumeDeleteHoldPhaseUnsized)))

	// A short pass: the nonzero usage this fixture reports keeps the zero-usage
	// wait polling until the attempt's slice ends.
	runPass := func() {
		passCtx, cancelPass := context.WithTimeout(t.Context(), 2*volumeDeleteHoldMinSlice)
		defer cancelPass()
		pass(passCtx)
	}
	runPass()
	assert.Equal(t, holdPhaseUnsized, heldForTest(t, mgr, gone.volumeID.value()).phase)
	assert.True(t, b.pool.Stats().DiskAccountingHeld, "a pass that cannot size it keeps admission withheld")

	retainedBefore := b.pool.Stats().RetainedDiskMB
	require.NoError(t, os.WriteFile(sizable, []byte("x"), 0o600))
	runPass()
	assert.Equal(t, holdPhaseResidual, heldForTest(t, mgr, gone.volumeID.value()).phase)
	stats = b.pool.Stats()
	assert.False(t, stats.DiskAccountingHeld, "a sized hold releases the exclusion")
	assert.Equal(t, retainedBefore+2, stats.RetainedDiskMB, "and is counted at its block hard limit instead")
	require.NoError(t, b.pool.TryAllocateResolved("550e8400-e29b-41d4-a716-446655449998-app-0", "tenant-b", disk))
	require.NoError(t, b.terminalStorageAuthorityError())
	require.NoError(t, stop())
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
	removal := mgr.VolumeDeleteHolds().phaseCounts()[volumeDeleteHoldPhaseRemoval]
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

// A close mixing one held volume with a slot that has no volume left (already
// destroyed, or a stateless service that never had one) waits only on the
// hold: HTTP Deprovision answers the observable pending without a new close
// generation or any substrate call, and the close-age gauges exclude it.
func TestDeprovisionOfAMixedDeleteHeldCloseAnswersPendingWithoutRetrying(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{})
	seedCloseLeaseFixture(t, b, stores, closeDeprovisionLeaseUUID, "", 2)
	held := canonicalVolumeName(closeDeprovisionLeaseUUID, "app", 0)
	var destroys atomic.Int32
	b.volumes = &mockVolumeManager{
		ListFn:              func() ([]string, error) { return []string{held}, nil },
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return pendingDeletes(held) },
		DestroyFn: func(_ context.Context, name string) error {
			destroys.Add(1)
			if name == held {
				return heldDeleteErr(name)
			}
			return nil
		},
	}
	installTestStorageMutationAdapters(b)
	require.Error(t, b.doDeprovisionForTest(t, t.Context(), closeDeprovisionLeaseUUID))
	before, found, err := stores.callbacks.GetCloseIntent(closeDeprovisionLeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	destroysBefore := destroys.Load()
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
	assert.Equal(t, destroysBefore, destroys.Load(), "nothing is destroyed again")
	assert.Zero(t, downs.Load(), "the substrate is not touched again")

	b.sampleCloseIntentMetrics(time.Now())
	assert.Equal(t, float64(1), testutil.ToFloat64(closeIntentsDeleteHeld), "the mixed close counts as delete-held")
	closeCloseRecoveryBackend(t, b, stores)
}

// The close predicate answers from memory: every remaining volume slot must be
// held with its caller pending, at least one must be, and a projection must
// vouch that no container remains.
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

	inFlight := volumeDeleteHoldSnapshot{
		pending: map[string]struct{}{second: {}}, staged: map[string]struct{}{second: {}},
		mapped: map[string]struct{}{second: {}},
	}
	for _, tc := range []struct {
		name  string
		holds volumeDeleteHoldSnapshot
		want  bool
	}{
		{"every slot held", pendingDeletes(first, second), true},
		{"a slot held under its retained name counts", pendingDeletes(retainedName(first), second), true},
		{"an unsized hold keeps its caller pending", unsizedDeletes(first, second), true},
		{"held plus already destroyed", pendingDeletes(first), true},
		{"held plus a slot that never had a volume (a stateless service)", pendingDeletes(second), true},
		{"held plus residual: the residual slot is settled", pendingDeletes(first).merge(residualDeletes(10, second)), true},
		{"held plus a retained volume", pendingDeletes(first).withMapped(retainedName(second)), true},
		{"held plus an existing unheld volume", pendingDeletes(first).withMapped(second), false},
		{"held plus a deletion still in flight", pendingDeletes(first).merge(inFlight), false},
		{"every slot residual: nothing is held, the close can finish", residualDeletes(10, first, second), false},
		{"nothing held and nothing left: the close must run to finish", volumeDeleteHoldSnapshot{}, false},
	} {
		assert.Equal(t, tc.want, b.closeAwaitsHeldDeletes(claim, tc.holds), tc.name)
	}

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

// A cleanup-only close has no projection to vouch for its containers. Once
// this process's attempt of the close completed its container teardown and
// left only held deletions, periodic recovery skips it and close-age paging
// leaves it out, as for a projected close; before that, and after a failed
// teardown, it runs.
func TestCleanupOnlyCloseAwaitsHeldDeletesAfterItsTeardown(t *testing.T) {
	dir := t.TempDir()
	name := canonicalVolumeName(closeRecoveryLeaseUUID, "app", 0)
	var destroyCalls atomic.Int32
	volumes := &mockVolumeManager{
		ListFn:              func() ([]string, error) { return []string{name}, nil },
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return pendingDeletes(name) },
		DestroyFn: func(context.Context, string) error {
			destroyCalls.Add(1)
			return heldDeleteErr(name)
		},
	}
	docker := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	b, stores := openCloseRecoveryBackend(t, dir, docker, volumes)
	begun := beginCloseRecoveryIntent(t, b, stores, true, "")
	require.True(t, begun.CleanupOnly())
	require.False(t, b.closeAwaitsHeldDeletes(begun, pendingDeletes(name)),
		"no attempt of this close has torn its containers down yet")

	require.NoError(t, b.recoverState(t.Context()))
	claim, found, err := stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.True(t, found, "the held deletion keeps the close pending")
	require.Equal(t, 1, claim.ExecutionGeneration().Number())
	require.Equal(t, int32(1), destroyCalls.Load())
	assert.True(t, b.closeAwaitsHeldDeletes(claim, pendingDeletes(name)))

	require.NoError(t, b.recoverState(t.Context()))
	claim, found, err = stores.callbacks.GetCloseIntent(closeRecoveryLeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, 1, claim.ExecutionGeneration().Number(), "the skipped close advances no generation")
	assert.Equal(t, int32(1), destroyCalls.Load(), "the skipped close does no volume work")

	b.sampleCloseIntentMetrics(time.Now())
	assert.Equal(t, float64(1), testutil.ToFloat64(closeIntentsDeleteHeld))
	assert.Zero(t, testutil.ToFloat64(oldestUnheldCloseIntentAgeSeconds), "a held close does not page")

	b.closeTeardowns.record(claim, false)
	assert.False(t, b.closeAwaitsHeldDeletes(claim, pendingDeletes(name)),
		"a failed teardown forgets the evidence, so the close runs again")
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

// The unheld close-age gauge ages only the closes that do not wait on held
// deletions, so a paging rule on it neither fires for an old held close beside
// a young unheld one, nor is masked by it.
func TestOldestUnheldCloseIntentAgeExcludesHeldCloses(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, &mockVolumeManager{})
	const heldLease = "550e8400-e29b-41d4-a716-446655440701"
	const unheldLease = "550e8400-e29b-41d4-a716-446655440702"
	seedCloseLeaseFixture(t, b, stores, heldLease, "", 1)
	seedCloseLeaseFixture(t, b, stores, unheldLease, "", 1)
	heldName := canonicalVolumeName(heldLease, "app", 0)
	unheldName := canonicalVolumeName(unheldLease, "app", 0)
	var holds atomic.Value
	holds.Store(pendingDeletes(heldName).withMapped(unheldName))
	b.volumes = &mockVolumeManager{
		ListFn:              func() ([]string, error) { return []string{heldName, unheldName}, nil },
		VolumeDeleteHoldsFn: func() volumeDeleteHoldSnapshot { return holds.Load().(volumeDeleteHoldSnapshot) },
		DestroyFn: func(_ context.Context, name string) error {
			if name == heldName {
				return heldDeleteErr(name)
			}
			return errors.New("injected unheld destroy failure")
		},
	}
	installTestStorageMutationAdapters(b)
	require.Error(t, b.doDeprovisionForTest(t, t.Context(), heldLease))
	time.Sleep(20 * time.Millisecond) // the held close is the older one
	require.Error(t, b.doDeprovisionForTest(t, t.Context(), unheldLease))
	createdAt := func(lease string) time.Time {
		claim, found, err := stores.callbacks.GetCloseIntent(lease)
		require.NoError(t, err)
		require.True(t, found)
		return claim.CreatedAt()
	}
	now := createdAt(unheldLease).Add(time.Hour)

	b.sampleCloseIntentMetrics(now)
	assert.Equal(t, float64(2), testutil.ToFloat64(pendingCloseIntents))
	assert.Equal(t, float64(1), testutil.ToFloat64(closeIntentsDeleteHeld))
	assert.Equal(t, now.Sub(createdAt(heldLease)).Seconds(), testutil.ToFloat64(oldestCloseIntentAgeSeconds))
	assert.Equal(t, now.Sub(createdAt(unheldLease)).Seconds(), testutil.ToFloat64(oldestUnheldCloseIntentAgeSeconds),
		"the older held close does not age the unheld gauge")

	holds.Store(pendingDeletes(heldName, unheldName))
	b.sampleCloseIntentMetrics(now)
	assert.Equal(t, float64(2), testutil.ToFloat64(closeIntentsDeleteHeld))
	assert.Zero(t, testutil.ToFloat64(oldestUnheldCloseIntentAgeSeconds), "no unheld close: nothing to page on")

	holds.Store(volumeDeleteHoldSnapshot{}.withMapped(heldName, unheldName))
	b.sampleCloseIntentMetrics(now)
	assert.Zero(t, testutil.ToFloat64(closeIntentsDeleteHeld))
	assert.Equal(t, now.Sub(createdAt(heldLease)).Seconds(), testutil.ToFloat64(oldestUnheldCloseIntentAgeSeconds),
		"a close whose hold is gone ages like any other")
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

// A held restore-created volume no longer stalls the rollback's locked step:
// that step still runs (it also returns an interrupted source's volumes to
// retention) and destroys the other created volume, and only then does the
// held answer keep the rollback pending.
func TestRestoreRollbackHeldCreatedVolumeDoesNotStallTheLockedStep(t *testing.T) {
	f := newInterruptedRestoreRecoveryFixture(
		t, 3, []string{"exited"}, backend.ProvisionStatusFailed, true,
	)
	volumes, ok := f.b.volumes.(*mockVolumeManager)
	require.True(t, ok)
	dockerMock, ok := f.b.docker.(*mockDockerClient)
	require.True(t, ok)
	dockerMock.ListVolumeWritersFn = func(context.Context) ([]ContainerInfo, error) { return nil, nil }
	held := canonicalVolumeName(f.spec.LeaseUUID, f.spec.Items[0].ServiceName, 1)
	unheld := canonicalVolumeName(f.spec.LeaseUUID, f.spec.Items[0].ServiceName, 2)
	var unheldGone atomic.Bool
	listed := volumes.ListForProofFn
	volumes.ListForProofFn = func(ctx context.Context) ([]string, error) {
		names, err := listed(ctx)
		names = append(names, held)
		if !unheldGone.Load() {
			names = append(names, unheld)
		}
		return names, err
	}
	volumes.VolumeDeleteHoldsFn = func() volumeDeleteHoldSnapshot { return pendingDeletes(held) }
	volumes.PrecheckDestroyFn = func(name managedVolumeName) (destroyPrecheckVerdict, error) {
		if name.value() == held {
			return destroyPrecheckHeld, heldDeleteErr(held)
		}
		return destroyPrecheckNeedsLock, nil
	}
	volumes.DestroyFn = func(_ context.Context, name string) error {
		f.record("destroy:" + name)
		if name == unheld {
			unheldGone.Store(true)
		}
		return nil
	}

	retries := operationIntentRecoveryCleanupRetriesTotal.WithLabelValues("restore")
	before := testutil.ToFloat64(retries)
	require.NoError(t, f.b.recoverOperationIntents(t.Context()))
	assert.Contains(t, f.snapshotEvents(), "destroy:"+unheld,
		"the locked step ran despite the held created volume")
	assert.NotContains(t, f.snapshotEvents(), "destroy:"+held, "the held volume is answered, never destroyed")
	assert.Equal(t, before+1, testutil.ToFloat64(retries), "the held answer keeps the rollback pending")
	intents, err := f.b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	require.NoError(t, f.b.terminalStorageAuthorityError())
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
