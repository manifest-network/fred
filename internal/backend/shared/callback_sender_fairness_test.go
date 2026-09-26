package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

// Existing exact completion is healthy, but every available worker is handed
// newly committed work. All rows and admission counts fit the existing bounds.
func TestCallbackReplayLoopRetainedCompletionProgressUnderFreshCommits(t *testing.T) {
	const victim = "ffffffff-ffff-4fff-bfff-ffffffffffff"
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	seed := func(lease string) {
		require.NoError(t, store.storeValidTest(CallbackEntry{
			LeaseUUID: lease, CallbackURL: "https://fred.example/callbacks/provision?lifecycle_id=" + lease,
			DeliveryKind: CallbackDeliveryKindMaintenance, MaintenanceID: newTestMaintenanceID(t),
			Status: backend.CallbackStatusSuccess, CreatedAt: time.Now(),
		}))
	}
	for i := range callbackReplayWorkerLimit {
		seed(fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1))
	}
	seed(victim)
	dispatched := make(chan string, 256)
	release := make(chan struct{})
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		var body backend.CallbackPayload
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			return nil, err
		}
		dispatched <- body.LeaseUUID
		if body.LeaseUUID == victim {
			return callbackHTTPResponse(http.StatusNoContent), nil
		}
		select {
		case <-release:
			return callbackHTTPResponse(http.StatusNoContent), nil
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	})}
	stop, cancel := context.WithCancel(t.Context())
	zero := [CallbackMaxAttempts]time.Duration{}
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store, HTTPClient: client, Secret: "secret", Logger: slog.Default(), Backoff: &zero,
		ReplayInterval: time.Hour, DeliveryTimeout: time.Minute,
	}, callbackSenderTestLifetime(stop))
	done := make(chan struct{})
	go func() { defer close(done); sender.RunReplayLoop() }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("callback loop did not drain")
		}
	})
	receive := func() string {
		select {
		case lease := <-dispatched:
			return lease
		case <-time.After(time.Second):
			t.Fatal("callback dispatch stalled")
			return ""
		}
	}
	for range callbackReplayWorkerLimit {
		require.NotEqual(t, victim, receive())
	}
	const opportunities = 4 * callbackReplayWorkerLimit
	spent := 0
	victimServed := false
	for i := range opportunities {
		fresh := fmt.Sprintf("11111111-1111-4111-8111-%012d", i+1)
		seed(fresh)
		require.Eventually(t, func() bool { return sender.replayWake.pendingCount() == 0 }, time.Second, time.Millisecond)
		release <- struct{}{}
		spent++
		if next := receive(); next == victim {
			victimServed = true
			break
		} else {
			require.Equal(t, fresh, next)
		}
	}
	pending, err := store.listPending(victim)
	require.NoError(t, err)
	t.Logf("worker opportunities=%d, victim served=%v, protected rows remaining=%d", spent, victimServed, len(pending))
	require.True(t, victimServed, "healthy exact maintenance completion must receive a scheduling opportunity while fresh commits continue")
}

func TestCallbackReplayQueueWakesPreserveBothFIFOPositions(t *testing.T) {
	retainedOne, retainedTwo := testLeaseUUID("retained-one"), testLeaseUUID("retained-two")
	freshOne, freshTwo := testLeaseUUID("fresh-one"), testLeaseUUID("fresh-two")
	queue := newCallbackReplayQueue()
	queue.discover([]string{retainedOne, retainedTwo}, callbackReplayStartup)
	queue.wake(newCallbackReplayCommitWake(freshOne))
	queue.wake(newCallbackReplayCommitWake(freshTwo))
	// Neither repeated commit/handoff hints nor rediscovery can reset a
	// queued lease's position or move it into the other scheduling class.
	for range 20 {
		queue.wake(newCallbackReplayHandoffWake(retainedTwo))
		queue.wake(newCallbackReplayCommitWake(retainedOne))
		queue.wake(newCallbackReplayHandoffWake(freshTwo))
		queue.wake(newCallbackReplayCommitWake(freshOne))
		queue.discover([]string{freshTwo, retainedTwo, freshOne, retainedOne}, callbackReplayRequested)
	}
	for _, expected := range []string{freshOne, retainedOne, freshTwo, retainedTwo} {
		lease, ready := queue.next()
		require.True(t, ready)
		require.Equal(t, expected, lease)
		// Looking at the next opportunity cannot spend it. Only a dispatch
		// accepted by a worker advances the alternating service schedule.
		again, ready := queue.next()
		require.True(t, ready)
		require.Equal(t, lease, again)
		queue.dispatched(lease)
		queue.completed(callbackReplayCompletion{leaseUUID: lease, outcome: callbackReplayEmpty})
	}
}

func TestCallbackReplayQueueDirtySuffixAndDormantRetryRetainTheirLane(t *testing.T) {
	hot, old, fresh := testLeaseUUID("dirty-hot"), testLeaseUUID("dirty-old"), testLeaseUUID("dirty-fresh")
	queue := newCallbackReplayQueue()
	queue.discover([]string{hot, old}, callbackReplayStartup)
	queue.dispatched(hot)
	queue.wake(newCallbackReplayCommitWake(hot))
	queue.wake(newCallbackReplayCommitWake(fresh))
	queue.completed(callbackReplayCompletion{leaseUUID: hot, outcome: callbackReplayMore})
	for _, expected := range []string{fresh, old, hot} {
		lease, ready := queue.next()
		require.True(t, ready)
		require.Equal(t, expected, lease, "dirty successful suffix cannot overtake retained work")
		queue.dispatched(lease)
		queue.completed(callbackReplayCompletion{leaseUUID: lease, outcome: callbackReplayDeferred})
	}
	queue.wake(newCallbackReplayCommitWake(old))
	_, ready := queue.next()
	require.False(t, ready, "ordinary commits cannot revive a failed dormant head")
	queue.discover([]string{old, hot}, callbackReplayPeriodic)
	queue.wake(newCallbackReplayHandoffWake(fresh))
	for _, expected := range []string{fresh, old, hot} {
		lease, ready := queue.next()
		require.True(t, ready)
		require.Equal(t, expected, lease, "handoff and periodic retry each retain a service opportunity")
		queue.dispatched(lease)
		queue.completed(callbackReplayCompletion{leaseUUID: lease, outcome: callbackReplayEmpty})
	}
}

func TestCallbackReplayQueueFailedInFlightWorkDoesNotBorrowFreshAuthority(t *testing.T) {
	for _, observe := range []struct {
		name  string
		apply func(*callbackReplayQueue, string)
	}{
		{"periodic", func(q *callbackReplayQueue, lease string) { q.discover([]string{lease}, callbackReplayPeriodic) }},
		{"commit", func(q *callbackReplayQueue, lease string) { q.wake(newCallbackReplayCommitWake(lease)) }},
	} {
		t.Run(observe.name, func(t *testing.T) {
			failed, fresh := testLeaseUUID("in-flight-failed"), testLeaseUUID("later-fresh")
			queue := newCallbackReplayQueue()
			queue.discover([]string{failed}, callbackReplayStartup)
			queue.dispatched(failed)
			for range 10 {
				observe.apply(queue, failed)
			}
			queue.completed(callbackReplayCompletion{leaseUUID: failed, outcome: callbackReplayDeferred})
			queue.wake(newCallbackReplayCommitWake(fresh))
			next, ready := queue.next()
			require.True(t, ready)
			require.Equal(t, fresh, next, "an observation during a failed attempt cannot promote that head ahead of a later fresh commit")
			queue.dispatched(next)
			queue.completed(callbackReplayCompletion{leaseUUID: next, outcome: callbackReplayEmpty})
			_, ready = queue.next()
			require.False(t, ready, "a failed head waits for a retry requested after failure")
			require.Contains(t, queue.dormant, failed)
			queue.discover([]string{failed}, callbackReplayPeriodic)
			next, ready = queue.next()
			require.True(t, ready)
			require.Equal(t, failed, next, "the next periodic opportunity retries the durable head")
		})
	}
}

func TestCallbackReplayQueuePrioritizedDiscoveryServesNewWorkBeforeRetainedBacklog(t *testing.T) {
	oldOne, oldTwo := testLeaseUUID("discovery-old-one"), testLeaseUUID("discovery-old-two")
	newOne, newTwo := testLeaseUUID("discovery-new-one"), testLeaseUUID("discovery-new-two")
	queue := newCallbackReplayQueue()
	queue.discover([]string{oldOne, oldTwo}, callbackReplayStartup)
	queue.discover([]string{oldOne, oldTwo, newOne, newTwo}, callbackReplayRequested)
	for _, expected := range []string{newOne, oldOne, newTwo, oldTwo} {
		next, ready := queue.next()
		require.True(t, ready)
		require.Equal(t, expected, next, "explicit discovery prioritizes unseen work while preserving retained progress")
		queue.dispatched(next)
		queue.completed(callbackReplayCompletion{leaseUUID: next, outcome: callbackReplayEmpty})
	}
}

func TestCallbackReplayQueueExplicitInFlightRetryKeepsItsCause(t *testing.T) {
	for _, handoff := range []bool{false, true} {
		t.Run(fmt.Sprintf("handoff=%v", handoff), func(t *testing.T) {
			failed, fresh, retained := testLeaseUUID("explicit-failed"), testLeaseUUID("explicit-fresh"), testLeaseUUID("explicit-retained")
			queue := newCallbackReplayQueue()
			queue.discover([]string{failed, retained}, callbackReplayStartup)
			queue.dispatched(failed)
			if handoff {
				queue.wake(newCallbackReplayHandoffWake(failed))
			}
			// A later notification, commit, and periodic scan cannot erase a
			// handoff; neither can those weaker observations erase explicit retry.
			queue.discover([]string{failed, retained}, callbackReplayRequested)
			queue.wake(newCallbackReplayCommitWake(failed))
			queue.discover([]string{failed, retained}, callbackReplayPeriodic)
			queue.completed(callbackReplayCompletion{leaseUUID: failed, outcome: callbackReplayDeferred})
			queue.wake(newCallbackReplayCommitWake(fresh))
			expected := []string{fresh, retained, failed}
			if handoff {
				expected = []string{failed, retained, fresh}
			}
			for _, lease := range expected {
				next, ready := queue.next()
				require.True(t, ready)
				require.Equal(t, lease, next, "retry permission must preserve its scheduling cause")
				queue.dispatched(next)
				queue.completed(callbackReplayCompletion{leaseUUID: next, outcome: callbackReplayDeferred})
			}
			_, ready := queue.next()
			require.False(t, ready, "explicit permission is consumed by one dispatch, not every later failure")
		})
	}
}

func TestCallbackReplayQueuePeriodicObservationRechecksEmptyWithoutTakingFreshLane(t *testing.T) {
	old, fresh := testLeaseUUID("periodic-empty"), testLeaseUUID("periodic-empty-fresh")
	queue := newCallbackReplayQueue()
	queue.discover([]string{old}, callbackReplayStartup)
	queue.dispatched(old)
	queue.discover([]string{old}, callbackReplayPeriodic)
	queue.completed(callbackReplayCompletion{leaseUUID: old, outcome: callbackReplayEmpty})
	queue.wake(newCallbackReplayCommitWake(fresh))
	for _, expected := range []string{fresh, old} {
		next, ready := queue.next()
		require.True(t, ready)
		require.Equal(t, expected, next, "a periodic read racing the final empty check belongs to retained service")
		queue.dispatched(next)
		queue.completed(callbackReplayCompletion{leaseUUID: next, outcome: callbackReplayEmpty})
	}
}

func TestCallbackReplayLoopPeriodicTickCannotRestartActiveFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
		require.NoError(t, err)
		defer store.Close()
		failed, fresh := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
		seed := func(lease string) {
			require.NoError(t, store.storeValidTest(CallbackEntry{
				LeaseUUID: lease, CallbackURL: "https://fred.example/callbacks/provision?lifecycle_id=" + lease,
				DeliveryKind: CallbackDeliveryKindMaintenance, MaintenanceID: newTestMaintenanceID(t),
				Status: backend.CallbackStatusSuccess, CreatedAt: time.Now(),
			}))
		}
		seed(failed)
		release := make(chan struct{})
		var failures, successes atomic.Int32
		client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			var body backend.CallbackPayload
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				return nil, err
			}
			if body.LeaseUUID == fresh {
				successes.Add(1)
				return callbackHTTPResponse(http.StatusNoContent), nil
			}
			failures.Add(1)
			select {
			case <-release:
				return callbackHTTPResponse(http.StatusServiceUnavailable), nil
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		})}
		stop, cancel := context.WithCancel(t.Context())
		sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
			Store: store, HTTPClient: client, Secret: "secret", Logger: slog.Default(), Backoff: &zeroBackoff,
			ReplayInterval: 30 * time.Second, DeliveryTimeout: 2 * time.Minute,
		}, callbackSenderTestLifetime(stop))
		done := make(chan struct{})
		go func() { defer close(done); sender.RunReplayLoop() }()
		defer func() { cancel(); <-done }()
		synctest.Wait()
		require.EqualValues(t, 1, failures.Load())
		time.Sleep(sender.replayInterval)
		synctest.Wait() // Periodic discovery sees this lease in flight.
		close(release)
		synctest.Wait()
		require.EqualValues(t, CallbackMaxAttempts, failures.Load(), "a periodic observation cannot authorize a second failed delivery chain")
		seed(fresh)
		synctest.Wait()
		require.EqualValues(t, 1, successes.Load(), "a fresh completion is delivered while the failed head stays dormant")
		require.EqualValues(t, CallbackMaxAttempts, failures.Load())
		pending, err := store.listPending(failed)
		require.NoError(t, err)
		require.Len(t, pending, 1, "dormancy never removes durable work")
		time.Sleep(sender.replayInterval)
		synctest.Wait()
		require.EqualValues(t, 2*CallbackMaxAttempts, failures.Load(), "the next periodic opportunity retries the failed head")
	})
}
