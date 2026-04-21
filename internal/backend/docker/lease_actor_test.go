package docker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
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
	// The SM is initialized eagerly — its initial state mirrors the
	// provision's current Status at actor creation time.
	require.Equal(t, string(backend.ProvisionStatusReady), snaps[0].SMState)

	// Touch the other — now two actors visible.
	b.actorFor("lease-b")
	snaps = b.DebugActors()
	require.Len(t, snaps, 2)
}

// TestLeaseActor_EagerSMInit pins the fix for the DebugActors data race:
// the SM must be constructed inside newLeaseActor, not lazily on first
// handler invocation. Lazy init raced with DebugActors readers under
// -race; the eager init makes the pointer publication synchronous with
// the actor's exposure via b.actors.
func TestLeaseActor_EagerSMInit(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
	})
	defer b.stopCancel()

	actor := b.actorFor("lease-1")
	require.NotNil(t, actor.sm,
		"sm must be initialized in newLeaseActor; lazy init races with DebugActors")
	require.Equal(t, backend.ProvisionStatusReady, actor.sm.State(),
		"sm initial state mirrors the provision's current Status at actor creation")
}

// TestLeaseActor_RegistryClearedAfterDeprovision pins the fix for the
// actor-registry leak and its downstream UUID-reuse wedge: once
// Deprovision removes the provision entry, the actor exits and removes
// itself from b.actors. Without this, every deprovisioned lease would
// leak a goroutine plus an SM stuck in Deprovisioning, and any
// subsequent Provision with the same UUID would be silently Ignored by
// that stale SM.
func TestLeaseActor_RegistryClearedAfterDeprovision(t *testing.T) {
	mock := &mockDockerClient{
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
		},
	})
	defer b.stopCancel()

	first := b.actorFor("lease-1")
	require.NoError(t, b.Deprovision(context.Background(), "lease-1"))

	// Actor deletes itself from b.actors and closes done on exit.
	select {
	case <-first.done:
	case <-time.After(2 * time.Second):
		t.Fatal("actor did not exit after successful deprovision")
	}
	_, exists := b.actors.Load("lease-1")
	assert.False(t, exists,
		"b.actors must not retain a reference to a deprovisioned lease")

	// A subsequent actorFor with the same UUID returns a *fresh* actor —
	// not the terminated one — whose SM can accept evProvisionRequested.
	second := b.actorFor("lease-1")
	require.NotSame(t, first, second,
		"UUID reuse after Deprovision must produce a fresh actor")
	require.Equal(t, backend.ProvisionStatusProvisioning, second.sm.State(),
		"fresh actor's SM starts in Provisioning (no provision entry exists)")
}

// TestLeaseActor_SurvivesHandlerPanic pins the defer-recover in handle():
// a panic in a handler path (SM guard, entry action, or downstream I/O)
// must be contained to a single message. Without recovery, the actor
// goroutine dies and senders block on a full inbox forever.
func TestLeaseActor_SurvivesHandlerPanic(t *testing.T) {
	var inspectCalls atomic.Int32
	mock := &mockDockerClient{
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			if inspectCalls.Add(1) == 1 {
				panic("simulated inspect failure")
			}
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
		},
	})
	defer b.stopCancel()

	actor := b.actorFor("lease-1")
	before := testutil.ToFloat64(leaseActorPanicsTotal)

	// First send: panics inside the SM guard (InspectContainer).
	require.True(t, actor.send(containerDiedMsg{containerID: "c1"}))
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(leaseActorPanicsTotal) > before
	}, 2*time.Second, 10*time.Millisecond,
		"leaseActorPanicsTotal must increment when a handler panics")

	// Second send: must be processed — the actor survived the panic.
	done := make(chan struct{})
	require.True(t, actor.send(containerDiedMsg{containerID: "c1", done: done}))
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("actor did not process a message after recovering from panic")
	}
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
