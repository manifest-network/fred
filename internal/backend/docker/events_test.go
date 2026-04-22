package docker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestContainerEventLoop_DetectsDeathAndFailsLease(t *testing.T) {
	eventCh := make(chan ContainerEvent, 1)
	errCh := make(chan error)

	var callbackPayload backend.CallbackPayload
	var callbackReceived atomic.Bool
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&callbackPayload)
		callbackReceived.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		ContainerEventsFn: func(ctx context.Context) (<-chan ContainerEvent, <-chan error) {
			return eventCh, errCh
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{
				ContainerID: "c1",
				Status:      "exited",
				ExitCode:    1,
			}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "some log output", nil
		},
	}

	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusReady,
			CallbackURL:  callbackServer.URL,
		},
	})
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	defer b.stopCancel()

	// Start the event loop in a goroutine.
	done := make(chan struct{})
	go func() {
		b.containerEventLoop()
		close(done)
	}()

	// Send a die event.
	eventCh <- ContainerEvent{ContainerID: "c1", Action: "die"}

	// Wait for the transition to be processed.
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		prov := b.provisions["lease-1"]
		return prov != nil && prov.Status == backend.ProvisionStatusFailed
	}, 2*time.Second, 10*time.Millisecond)

	// Verify fail count incremented.
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	assert.Equal(t, 1, prov.FailCount)
	assert.Contains(t, prov.LastError, errMsgContainerExited)
	b.provisionsMu.RUnlock()

	// Verify callback was sent.
	require.Eventually(t, func() bool {
		return callbackReceived.Load()
	}, 2*time.Second, 10*time.Millisecond)

	assert.Equal(t, backend.CallbackStatusFailed, callbackPayload.Status)
	assert.Equal(t, "lease-1", callbackPayload.LeaseUUID)

	// Clean up.
	b.stopCancel()
	<-done
}

func TestContainerEventLoop_IgnoresNonReadyLease(t *testing.T) {
	eventCh := make(chan ContainerEvent, 1)
	errCh := make(chan error)

	mock := &mockDockerClient{
		ContainerEventsFn: func(ctx context.Context) (<-chan ContainerEvent, <-chan error) {
			return eventCh, errCh
		},
		// InspectContainer should NOT be called since the lease is not Ready.
	}

	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			Tenant:       "tenant-a",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusRestarting,
		},
	})
	defer b.stopCancel()

	done := make(chan struct{})
	go func() {
		b.containerEventLoop()
		close(done)
	}()

	// Send a die event for a non-ready lease.
	eventCh <- ContainerEvent{ContainerID: "c1", Action: "die"}

	// Give it time to process.
	time.Sleep(100 * time.Millisecond)

	// Verify status did NOT change.
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	assert.Equal(t, backend.ProvisionStatusRestarting, prov.Status)
	b.provisionsMu.RUnlock()

	b.stopCancel()
	<-done
}

func TestContainerEventLoop_ReconnectsOnError(t *testing.T) {
	var callCount atomic.Int32

	mock := &mockDockerClient{
		ContainerEventsFn: func(ctx context.Context) (<-chan ContainerEvent, <-chan error) {
			count := callCount.Add(1)
			eventCh := make(chan ContainerEvent)
			errCh := make(chan error, 1)

			if count == 1 {
				// First call: return an error to trigger reconnect.
				go func() {
					errCh <- assert.AnError
				}()
			} else {
				// Subsequent calls: block until context cancellation.
				go func() {
					<-ctx.Done()
					close(eventCh)
					close(errCh)
				}()
			}
			return eventCh, errCh
		},
	}

	b := newBackendForTest(mock, nil)
	defer b.stopCancel()

	done := make(chan struct{})
	go func() {
		b.containerEventLoop()
		close(done)
	}()

	// Wait for reconnection (at least 2 calls to ContainerEvents).
	require.Eventually(t, func() bool {
		return callCount.Load() >= 2
	}, 5*time.Second, 50*time.Millisecond)

	b.stopCancel()
	<-done
}

func TestHandleContainerDeath_UnknownContainer(t *testing.T) {
	mock := &mockDockerClient{
		// InspectContainer should NOT be called for unknown containers.
	}

	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusReady,
		},
	})

	// Call with unknown container ID — should be a no-op.
	b.handleContainerDeath("unknown-container-id")

	// Verify nothing changed.
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	assert.Equal(t, backend.ProvisionStatusReady, prov.Status)
	assert.Equal(t, 0, prov.FailCount)
	b.provisionsMu.RUnlock()
}

// TestHandleContainerDeath_SkipsDeprovisioning guards the SM guard's Ready
// check: die events emitted by RemoveContainer during an in-flight Deprovision
// must NOT trigger the Ready→Failing transition (which would send a spurious
// failure callback and decrement the gauge twice). guardContainerActuallyDied
// returns false for non-Ready status, short-circuiting before InspectContainer.
func TestHandleContainerDeath_SkipsDeprovisioning(t *testing.T) {
	inspectCalled := false
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			inspectCalled = true
			return nil, nil
		},
	}

	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusDeprovisioning,
			FailCount:    0,
		},
	})

	b.handleContainerDeath("c1")

	assert.False(t, inspectCalled, "should short-circuit before InspectContainer when status is Deprovisioning")
	b.provisionsMu.RLock()
	prov := b.provisions["lease-1"]
	assert.Equal(t, backend.ProvisionStatusDeprovisioning, prov.Status, "status must not change")
	assert.Equal(t, 0, prov.FailCount, "fail count must not increment")
	b.provisionsMu.RUnlock()
}

// The stale-Failed-callback regression case is covered structurally by
// TestConcurrentDeprovisionAndContainerDeath_ExactlyOneCallback in
// lease_actor_test.go. That test exercises a real Deprovision call racing
// a container death; the SM's Failing.OnExit cancellation makes a stale
// Failed callback impossible by construction. Simulated-race tests that
// mutated provision.Status directly to fake a Deprovision are obsolete:
// the SM is authoritative, so a direct status mutation no longer
// triggers the suppression mechanism.

func TestFindLeaseByContainerID(t *testing.T) {
	b := newBackendForTest(nil, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			ContainerIDs: []string{"c1", "c2"},
		},
		"lease-2": {
			LeaseUUID:    "lease-2",
			ContainerIDs: []string{"c3"},
		},
	})

	uuid, found := b.findLeaseByContainerID("c2")
	assert.True(t, found)
	assert.Equal(t, "lease-1", uuid)

	uuid, found = b.findLeaseByContainerID("c3")
	assert.True(t, found)
	assert.Equal(t, "lease-2", uuid)

	_, found = b.findLeaseByContainerID("nonexistent")
	assert.False(t, found)
}
