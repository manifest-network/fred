package provisioner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

func TestDeferredCloseRetainsHintArrivingDuringAttempt(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "unknown failure"}[failed], func(t *testing.T) {
			var calls atomic.Int32
			var scheduler *deferredCloseScheduler
			var coordinator *placement.ProvisionCoordinator
			var observedHint placement.DeferredDeprovision
			var enqueueErr error
			manager, proofs, project := newDeferredCloseFixture(t, 1, func(ctx context.Context, lease string) error {
				if calls.Add(1) == 1 {
					// The first backend call owns the lease. This fresh event must
					// therefore receive a real lifecycle-busy scheduling proof.
					hint := coordinator.DeprovisionEvent(ctx, lease)
					observedHint = hint.Deferred()
					enqueueErr = scheduler.enqueue(observedHint)
					if enqueueErr != nil {
						return enqueueErr
					}
					if failed {
						return errors.New("backend reply was lost")
					}
				}
				return nil
			})
			coordinator = manager.handlers.events.orchestrator.coordinator
			manager.deferredCloses.stop()
			manager.deferredCloses.wg.Wait()
			project()
			synctest.Test(t, func(t *testing.T) {
				scheduler = newDeferredCloseScheduler()
				scheduler.start(t.Context())
				defer func() { scheduler.stop(); scheduler.wg.Wait() }()
				require.NoError(t, scheduler.enqueue(proofs[0]))
				time.Sleep(3 * time.Second)
				synctest.Wait()
				require.NoError(t, enqueueErr)
				require.Equal(t, placement.DeprovisionDeferredLifecycle, observedHint.Reason())
				require.EqualValues(t, 2, calls.Load(), "a completed older attempt cannot consume its newer hint")
				require.Empty(t, scheduler.entries)
			})
		})
	}
}

func TestDeferredCloseRetainsHintAfterExecutionBeforeAccounting(t *testing.T) {
	var calls atomic.Int32
	manager, proofs, project := newDeferredCloseFixture(t, 1, func(context.Context, string) error {
		calls.Add(1)
		return nil
	})
	manager.deferredCloses.stop()
	manager.deferredCloses.wg.Wait()
	project()
	synctest.Test(t, func(t *testing.T) {
		scheduler := newDeferredCloseScheduler()
		scheduler.start(t.Context())
		defer func() { scheduler.stop(); scheduler.wg.Wait() }()
		pendingEntries := func() int {
			scheduler.mu.Lock()
			defer scheduler.mu.Unlock()
			return len(scheduler.entries)
		}
		pendingBefore := promtestutil.ToFloat64(metrics.DeferredClosesPending)
		dispatched := metrics.DeferredClosesTotal.WithLabelValues("dispatched", string(proofs[0].Reason()))
		dispatchedBefore := promtestutil.ToFloat64(dispatched)
		require.NoError(t, scheduler.enqueue(proofs[0]))
		// Drive the production dispatch/execution/accounting phases in order.
		// No wall-clock race is needed to put the hint in the exact gap after
		// backend return and before completion reacquires the scheduler mutex.
		jobs := make(chan *deferredCloseAttempt, 1)
		scheduler.dispatch(time.Now().Add(deferredCloseInterval), jobs)
		attempt := <-jobs
		result, panicErr := executeDeferredClose(t.Context(), attempt.hint.proof)
		require.NoError(t, panicErr)
		require.Equal(t, placement.DeprovisionEventCompleted, result.Disposition())
		require.NoError(t, scheduler.enqueue(proofs[0]))
		scheduler.finishAttempt(attempt, result, panicErr)
		require.Equal(t, 1, pendingEntries())
		require.Equal(t, pendingBefore+1, promtestutil.ToFloat64(metrics.DeferredClosesPending))
		require.Equal(t, dispatchedBefore+1, promtestutil.ToFloat64(dispatched),
			"completed attempts count even when a newer hint remains pending")
		scheduler.finishAttempt(attempt, result, nil)
		require.Equal(t, 1, pendingEntries(), "a stale completion cannot consume the replacement hint")
		require.Equal(t, dispatchedBefore+1, promtestutil.ToFloat64(dispatched))
		time.Sleep(2 * time.Second)
		synctest.Wait()
		require.EqualValues(t, 2, calls.Load())
		require.Empty(t, scheduler.entries)
		require.Equal(t, pendingBefore, promtestutil.ToFloat64(metrics.DeferredClosesPending))
		require.Equal(t, dispatchedBefore+2, promtestutil.ToFloat64(dispatched))
	})
}

func TestDeferredCloseOldestAgeSurvivesCoalescingBackoffAndExecution(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "completion", true: "shutdown"}[shutdown], func(t *testing.T) {
			var active atomic.Int32
			releases := map[string]chan struct{}{}
			manager, proofs, project := newDeferredCloseFixture(t, 2, func(ctx context.Context, lease string) error {
				active.Add(1)
				defer active.Add(-1)
				select {
				case <-releases[lease]:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			manager.deferredCloses.stop()
			manager.deferredCloses.wg.Wait()
			synctest.Test(t, func(t *testing.T) {
				for _, proof := range proofs {
					releases[proof.LeaseUUID()] = make(chan struct{})
				}
				scheduler := newDeferredCloseScheduler()
				scheduler.start(t.Context())
				defer func() { scheduler.stop(); scheduler.wg.Wait() }()
				require.NoError(t, scheduler.enqueue(proofs[0]))
				time.Sleep(11 * time.Second)
				synctest.Wait()
				require.Zero(t, active.Load(), "pending inventory still forbids physical dispatch")
				require.InDelta(t, 11, promtestutil.ToFloat64(metrics.DeferredClosesOldestAge), 1)
				require.NoError(t, scheduler.enqueue(proofs[0]))
				require.NoError(t, scheduler.enqueue(proofs[1]))
				require.EqualValues(t, 11, promtestutil.ToFloat64(metrics.DeferredClosesOldestAge))
				project()
				time.Sleep(3 * time.Second)
				synctest.Wait()
				require.EqualValues(t, 2, active.Load())
				require.InDelta(t, 14, promtestutil.ToFloat64(metrics.DeferredClosesOldestAge), 1,
					"the dispatch ticker reports age while workers are occupied")
				if shutdown {
					scheduler.stop()
					scheduler.wg.Wait()
				} else {
					close(releases[proofs[0].LeaseUUID()])
					synctest.Wait()
					require.EqualValues(t, 3, promtestutil.ToFloat64(metrics.DeferredClosesOldestAge),
						"completion exposes the next oldest entry's original age")
					close(releases[proofs[1].LeaseUUID()])
					synctest.Wait()
				}
				require.Zero(t, active.Load())
				require.Empty(t, scheduler.entries)
				require.Zero(t, promtestutil.ToFloat64(metrics.DeferredClosesOldestAge))
			})
		})
	}
}
