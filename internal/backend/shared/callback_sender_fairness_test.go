package shared

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"testing"
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
	queue.discover([]string{retainedOne, retainedTwo}, false, false)
	queue.wake(newCallbackReplayCommitWake(freshOne))
	queue.wake(newCallbackReplayCommitWake(freshTwo))
	// Neither repeated commit/handoff hints nor rediscovery can reset a
	// queued lease's position or move it into the other scheduling class.
	for range 20 {
		queue.wake(newCallbackReplayHandoffWake(retainedTwo))
		queue.wake(newCallbackReplayCommitWake(retainedOne))
		queue.wake(newCallbackReplayHandoffWake(freshTwo))
		queue.wake(newCallbackReplayCommitWake(freshOne))
		queue.discover([]string{freshTwo, retainedTwo, freshOne, retainedOne}, true, true)
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
	queue.discover([]string{hot, old}, false, false)
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
	queue.discover([]string{old, hot}, true, false)
	queue.wake(newCallbackReplayHandoffWake(fresh))
	for _, expected := range []string{fresh, old, hot} {
		lease, ready := queue.next()
		require.True(t, ready)
		require.Equal(t, expected, lease, "handoff and periodic retry each retain a service opportunity")
		queue.dispatched(lease)
		queue.completed(callbackReplayCompletion{leaseUUID: lease, outcome: callbackReplayEmpty})
	}
}
