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

// TestLeaseActor_DirectDispatch exercises the async actor path without the
// synchronous shim: send fire-and-forget, poll for completion. Catches async
// bugs (missed channel sends, dropped messages) that the shim would hide by
// blocking on a done channel.
func TestLeaseActor_DirectDispatch(t *testing.T) {
	var callbackHit atomic.Bool
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackHit.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", nil
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

	require.True(t, b.actorFor("lease-1").send(containerDiedMsg{containerID: "c1"}))

	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		prov := b.provisions["lease-1"]
		return prov != nil && prov.Status == backend.ProvisionStatusFailed
	}, 2*time.Second, 10*time.Millisecond, "actor must transition Ready→Failed asynchronously")

	require.Eventually(t, callbackHit.Load, 2*time.Second, 10*time.Millisecond,
		"actor must emit the Failed callback")
}

// TestConcurrentDeprovisionAndContainerDeath_ExactlyOneCallback pins the
// cc62f3b invariant structurally: when a container-death event and a
// Deprovision call race on the same lease, exactly ONE terminal callback
// fires (Deprovisioned), never a stale Failed that Fred already knows is
// being torn down.
//
// Mechanism: handleContainerDeath transitions the SM to Failing and
// spawns an async diag goroutine. When Deprovision arrives, the
// Failing→Deprovisioning transition's OnExit cancels the goroutine's
// context. The goroutine's ContainerLogs call respects ctx and returns
// early; no DiagGathered fires; the SM never enters Failed; no Failed
// callback is emitted. Defense-in-depth: if the goroutine races past the
// cancel signal and fires DiagGathered, Deprovisioning.Ignore drops it.
func TestConcurrentDeprovisionAndContainerDeath_ExactlyOneCallback(t *testing.T) {
	var callbackCount atomic.Int32
	var failedSeen atomic.Bool
	var deprovisionedSeen atomic.Bool

	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload backend.CallbackPayload
		_ = json.NewDecoder(r.Body).Decode(&payload)
		callbackCount.Add(1)
		switch payload.Status {
		case backend.CallbackStatusFailed:
			failedSeen.Store(true)
		case backend.CallbackStatusDeprovisioned:
			deprovisionedSeen.Store(true)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	inDiag := make(chan struct{})

	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
		// Mirror the production Docker client: block until ctx is cancelled
		// or a signal (none here) fires. Cancellation unblocks the goroutine
		// — that's what the Failing.OnExit path triggers.
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			close(inDiag)
			<-ctx.Done()
			return "", ctx.Err()
		},
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
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

	g1Done := make(chan struct{})
	go func() {
		defer close(g1Done)
		b.handleContainerDeath("c1")
	}()

	// Wait for the diag goroutine to reach ContainerLogs (SM is in Failing,
	// status is Failing, goroutine is blocked on ctx).
	<-inDiag

	g2Done := make(chan struct{})
	go func() {
		defer close(g2Done)
		if err := b.Deprovision(context.Background(), "lease-1"); err != nil {
			t.Errorf("Deprovision returned unexpected error: %v", err)
		}
	}()

	// Deprovision runs: actor picks up deprovisionMsg, Fire(evDeprovisionRequested)
	// triggers Failing.OnExit which cancels the goroutine's ctx. ContainerLogs
	// returns ctx.Err(); goroutine exits without firing DiagGathered. Then
	// doDeprovision removes containers and deletes the provision entry.
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		_, exists := b.provisions["lease-1"]
		return !exists
	}, 2*time.Second, 10*time.Millisecond, "Deprovision must complete (provision entry deleted)")

	<-g1Done
	<-g2Done

	require.Eventually(t, deprovisionedSeen.Load, 2*time.Second, 10*time.Millisecond,
		"Deprovisioned callback must fire")

	assert.Equal(t, int32(1), callbackCount.Load(),
		"exactly one terminal callback expected (Failed seen: %v, Deprovisioned seen: %v)",
		failedSeen.Load(), deprovisionedSeen.Load())
	assert.False(t, failedSeen.Load(),
		"stale Failed callback must not fire when Deprovision takes over mid-flight")
}

// TestDebugActors exercises the ops introspection path: after container
// events land for known leases, DebugActors reports each with its SM
// state and inbox depth. Shape-level test; the point is that this
// never blocks, never panics, and produces JSON-encodable data.
func TestDebugActors(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
		ContainerLogsFn: func(ctx context.Context, containerID string, tail int) (string, error) {
			return "", nil
		},
	}
	b := newBackendForTest(mock, map[string]*provision{
		"lease-a": {
			LeaseUUID:    "lease-a",
			Tenant:       "tenant-1",
			ContainerIDs: []string{"ca"},
			Status:       backend.ProvisionStatusReady,
		},
		"lease-b": {
			LeaseUUID:    "lease-b",
			Tenant:       "tenant-1",
			ContainerIDs: []string{"cb"},
			Status:       backend.ProvisionStatusReady,
		},
	})
	defer b.stopCancel()

	// Before any activity, no actors.
	require.Empty(t, b.DebugActors())

	// Touch one lease — actor is created.
	b.actorFor("lease-a")
	snaps := b.DebugActors()
	require.Len(t, snaps, 1)
	require.Equal(t, "lease-a", snaps[0].LeaseUUID)
	require.Equal(t, leaseActorInboxSize, snaps[0].InboxCap)
	// No messages sent, SM never created.
	require.Empty(t, snaps[0].SMState)

	// Touch the other — now two actors visible.
	b.actorFor("lease-b")
	snaps = b.DebugActors()
	require.Len(t, snaps, 2)
}

// TestHandleContainerDeath_ShutdownDoesNotHang guards the sync shim's
// stopCtx branch: once the backend is shutting down, the shim must return
// promptly instead of blocking on a done channel the actor will never close.
func TestHandleContainerDeath_ShutdownDoesNotHang(t *testing.T) {
	mock := &mockDockerClient{}

	b := newBackendForTest(mock, map[string]*provision{
		"lease-1": {
			LeaseUUID:    "lease-1",
			ContainerIDs: []string{"c1"},
			Status:       backend.ProvisionStatusReady,
		},
	})

	b.stopCancel()

	returned := make(chan struct{})
	go func() {
		b.handleContainerDeath("c1")
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handleContainerDeath did not return within 500ms after shutdown")
	}
}
