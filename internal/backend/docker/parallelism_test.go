package docker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

// TestCrossLeaseParallelism measures whether container-death events on
// distinct leases process in parallel under the actor model. The fire-and-
// forget event-loop dispatch + per-lease actor goroutine + async diag
// gather should mean N simultaneous deaths complete in ~diagDelay plus
// scheduling overhead, not N*diagDelay.
//
// Skipped under -short: this test runs for ~diagDelay seconds and is
// meaningful only with enough leases to stress the scheduler.
//
// If this test ever fails the parallelism assertion, something in the
// architecture has regressed toward serialization. The failure output
// includes elapsed time so the actual serialization factor is visible.
func TestCrossLeaseParallelism(t *testing.T) {
	if testing.Short() {
		t.Skip("parallelism load test — not run under -short")
	}

	const numLeases = 50
	const diagDelay = 200 * time.Millisecond

	var callbacksReceived atomic.Int32
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbacksReceived.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			// Simulate I/O latency — this is where parallelism pays off.
			// Real Docker log fetches are bounded by daemon throughput,
			// but each call is independent.
			select {
			case <-time.After(diagDelay):
				return "container exited with logs", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		},
	}

	provisions := make(map[string]*provision, numLeases)
	for i := 0; i < numLeases; i++ {
		uuid := fmt.Sprintf("lease-%d", i)
		provisions[uuid] = &provision{
			LeaseUUID:    uuid,
			Tenant:       "tenant-a",
			ContainerIDs: []string{fmt.Sprintf("c-%d", i)},
			Status:       backend.ProvisionStatusReady,
			CallbackURL:  callbackServer.URL,
		}
	}
	b := newBackendForTest(mock, provisions)
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	defer b.stopCancel()

	// Fire all container-death events as fast as possible from a single
	// goroutine — matches how the production event loop delivers them.
	start := time.Now()
	for i := 0; i < numLeases; i++ {
		cid := fmt.Sprintf("c-%d", i)
		// Use the fire-and-forget actor dispatch path (what the production
		// event loop uses), not the synchronous shim.
		if leaseUUID, found := b.findLeaseByContainerID(cid); found {
			b.actorFor(leaseUUID).send(containerDiedMsg{containerID: cid})
		}
	}
	dispatchElapsed := time.Since(start)

	require.Eventually(t, func() bool {
		return callbacksReceived.Load() == int32(numLeases)
	}, 10*time.Second, 10*time.Millisecond,
		"all %d Failed callbacks must arrive", numLeases)

	elapsed := time.Since(start)
	serialExpected := time.Duration(numLeases) * diagDelay
	t.Logf("N=%d leases, diagDelay=%v: dispatch took %v, all callbacks in %v (serial would be %v, speedup %.1fx)",
		numLeases, diagDelay, dispatchElapsed, elapsed, serialExpected,
		float64(serialExpected)/float64(elapsed))

	// The dispatch loop itself should be sub-millisecond — send()s to
	// bounded inboxes aren't blocking on anything but buffer space.
	assert.Less(t, dispatchElapsed, 100*time.Millisecond,
		"event loop dispatch should not block on processing")

	// Total time should be O(diagDelay), not O(N*diagDelay). Allow 5x
	// headroom for goroutine scheduling overhead, Docker SDK contention,
	// callback HTTP roundtrip latency, etc.
	maxExpected := 5 * diagDelay
	assert.Less(t, elapsed, maxExpected,
		"N simultaneous deaths should process in ~diagDelay, not N*diagDelay")
}

// TestCrossLeaseInboxBackpressure verifies that a crash storm on a *single*
// lease (many die events, multi-container) doesn't deadlock the event loop
// when the actor's inbox buffer fills. The event loop should block on
// send, backpressuring the event stream rather than dropping events or
// blocking forever.
func TestCrossLeaseInboxBackpressure(t *testing.T) {
	if testing.Short() {
		t.Skip("backpressure test — not run under -short")
	}

	// Stall all diag gathering so the actor's inbox accumulates.
	logsProceed := make(chan struct{})
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			select {
			case <-logsProceed:
				return "", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		},
	}

	// Single lease with many containers — each containerDiedMsg targets
	// this same actor. Inbox capacity is leaseActorInboxSize (16), so
	// pushing 50 msgs will backpressure at least 34 of them.
	containerIDs := make([]string, 50)
	for i := range containerIDs {
		containerIDs[i] = fmt.Sprintf("c-%d", i)
	}
	provisions := map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ContainerIDs: containerIDs,
			Status:       backend.ProvisionStatusReady,
		},
	}
	b := newBackendForTest(mock, provisions)
	defer b.stopCancel()

	// Send all 50 msgs. The first one will enter Failing and trigger the
	// goroutine (blocked on logsProceed). Subsequent msgs land in the
	// inbox or block on send once the buffer is full.
	dispatchDone := make(chan struct{})
	go func() {
		defer close(dispatchDone)
		for _, cid := range containerIDs {
			b.actorFor("lease-1").send(containerDiedMsg{containerID: cid})
		}
	}()

	// The dispatcher should make progress despite inbox fill — send blocks
	// briefly then continues as the actor drains. It should NOT deadlock.
	select {
	case <-dispatchDone:
		// good: dispatcher completed under backpressure
	case <-time.After(3 * time.Second):
		t.Fatal("dispatcher deadlocked — inbox backpressure not released")
	}

	// Release diag goroutine so the test exits cleanly.
	close(logsProceed)
}
