package leasesm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func validConstructionTestActorConfig(ctx context.Context, store LeaseProvisionStore) LeaseActorConfig {
	lineage := mustTestRecoveryLineage()
	return LeaseActorConfig{
		LeaseUUID:      testActorLeaseUUID,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		StopCtx:        ctx,
		WG:             &sync.WaitGroup{},
		CloseTimeout:   testActorCloseTimeout,
		CommandFence:   &shared.CommandFence{},
		Inspector:      &mockInstanceInspector{},
		Diag:           &mockDiagnosticsGatherer{},
		ProvisionStore: store,
		Metrics:        mockSMMetrics{},
		ProvisionWorkFn: func(context.Context, shared.ProvisionResourceExecution) ProvisionWorkOutcome {
			return nil
		},
		RestoreWorkFn: func(context.Context, shared.OperationIntentClaim) ReplaceWorkOutcome {
			return nil
		},
		MaintenanceWorkFn: func(shared.MaintenanceWorkerLifetime, shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
			return nil
		},
		OnTerminated: func(string, *LeaseActor) {},
		PersistDiagnosticsFn: func(shared.DiagnosticEntry, []string, map[string]string) {
		},
		SendOperationSuccessFn:   func(shared.OperationReleaseCommitted) {},
		SendOperationFailureFn:   func(shared.OperationReleaseUncommitted, string) {},
		SendLifecycleFailureFn:   func(shared.RuntimeGenerationProof, string) {},
		SendMaintenanceSuccessFn: func(shared.MaintenanceReleaseActive) {},
		SendMaintenanceFailureFn: func(shared.MaintenanceReleaseFailure, string) {},
		RecoveryLineage:          lineage,
		DoDeprovisionFn:          func(context.Context, ActorCloseScope) error { return nil },
	}
}

func TestNewLeaseActorRegistersBeforeCanceledRunCanRetire(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusReady})
	var registered atomic.Bool
	terminatedAfterRegistration := make(chan struct{})
	cfg := validConstructionTestActorConfig(ctx, store)
	cfg.OnTerminated = func(string, *LeaseActor) {
		if registered.Load() {
			close(terminatedAfterRegistration)
		}
	}
	actor, err := NewLeaseActor(cfg, func(*LeaseActor) {
		registered.Store(true)
	})
	require.NoError(t, err)
	select {
	case <-actor.Done():
	case <-time.After(time.Second):
		t.Fatal("canceled actor did not retire")
	}
	select {
	case <-terminatedAfterRegistration:
	default:
		t.Fatal("termination ran before atomic registry installation")
	}
}

func TestNewLeaseActorRejectsTypedNilCapabilityBeforeInstallation(t *testing.T) {
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusReady})
	cfg := validConstructionTestActorConfig(context.Background(), store)
	var nilInspector *mockInstanceInspector
	cfg.Inspector = nilInspector
	installed := false
	actor, err := NewLeaseActor(cfg, func(*LeaseActor) { installed = true })
	require.ErrorContains(t, err, "instance inspector")
	assert.Nil(t, actor)
	assert.False(t, installed, "invalid composition must not reach registry installation")
}

// A close owns the command fence its caller handed over. Releasing it early
// takes durable evidence of the close's head: a substrate's bare or forged
// claim is refused and the fence stays held until the attempt ends. (A real
// close head releases it before teardown: docker-backend's
// TestCloseKeepsTheCommandFenceUntilItsHeadCommits.)
func TestCloseKeepsItsCommandFenceWithoutDurableAdmission(t *testing.T) {
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusReady})
	tearingDown, finishTeardown := make(chan struct{}), make(chan struct{})
	var admissionErr error
	actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{
		ProvisionStore: store,
		DoDeprovisionFn: func(_ context.Context, scope ActorCloseScope) error {
			admissionErr = scope.CloseAdmitted(shared.CloseIntentClaim{})
			close(tearingDown)
			<-finishTeardown
			return errors.New("close head was not committed")
		},
	})
	command, reply := newTestDeprovisionCommand(t, actor)
	go actor.handle(command.envelope.message)
	<-tearingDown
	_, free := actor.cfg.CommandFence.TryLock(testActorLeaseUUID)
	require.False(t, free, "a close without a durable head keeps the fence")
	close(finishTeardown)
	require.Error(t, <-reply.Result())
	require.ErrorContains(t, admissionErr, "durable close head")
	hold, free := actor.cfg.CommandFence.TryLock(testActorLeaseUUID)
	require.True(t, free, "the attempt's end gives the fence back")
	hold.Release()
}

// A close command takes the caller's fence ownership: no copy the caller kept
// can release it or hand it over again, and the actor accepts only a hold of
// its own fence for its own lease.
func TestCloseCommandTakesOwnershipOfItsLeasesFence(t *testing.T) {
	_, _, err := NewDeprovisionCommand(shared.CommandFenceHold{})
	require.Error(t, err, "a close needs the fence its caller checked under")

	var teardown atomic.Int32
	actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{
		ProvisionStore: newMockProvisionStore(),
		DoDeprovisionFn: func(context.Context, ActorCloseScope) error {
			teardown.Add(1)
			return nil
		},
	})
	fenceFree := func() bool {
		hold, free := actor.cfg.CommandFence.TryLock(testActorLeaseUUID)
		hold.Release()
		return free
	}

	released := actorCloseFence(actor)
	released.Release()
	_, _, err = NewDeprovisionCommand(released)
	require.Error(t, err, "a released hold is no authority")

	kept := actorCloseFence(actor)
	command, reply, err := NewDeprovisionCommand(kept)
	require.NoError(t, err)
	kept.Release()
	require.False(t, fenceFree(), "the caller's copy cannot release the fence the command owns")
	_, _, err = NewDeprovisionCommand(kept)
	require.Error(t, err, "the caller's copy cannot hand the fence over twice")
	command.Abandon()
	require.True(t, fenceFree(), "an abandoned command gives the fence back")
	require.ErrorIs(t, <-reply.Result(), errCommandAbandoned)
	require.False(t, actor.TryEnqueueCommand(command), "an abandoned command is spent")

	var foreign shared.CommandFence
	for name, hold := range map[string]shared.CommandFenceHold{
		"another fence": foreign.Lock(testActorLeaseUUID),
		"another lease": actor.cfg.CommandFence.Lock("33333333-3333-4333-8333-333333333333"),
	} {
		command, reply, err := NewDeprovisionCommand(hold)
		require.NoError(t, err, name)
		actor.handle(command.envelope.message)
		require.ErrorContains(t, <-reply.Result(), "does not own this lease's command fence", name)
	}
	require.Zero(t, teardown.Load())
	retaken, free := foreign.TryLock(testActorLeaseUUID)
	require.True(t, free, "a refused close gives its fence back")
	retaken.Release()
}

// Abandoning a close while an enqueue of it is in flight is never lost: if no
// actor accepts it, that enqueue settles the abandonment and gives the fence
// back; if an actor accepts it, the actor owns it and abandonment does nothing.
func TestAbandonDuringEnqueueIsNeverLost(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(fmt.Sprintf("accepted=%t", accepted), func(t *testing.T) {
			actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{ProvisionStore: newMockProvisionStore()})
			command, reply := newTestDeprovisionCommand(t, actor)
			fenceFree := func() bool {
				hold, free := actor.cfg.CommandFence.TryLock(testActorLeaseUUID)
				hold.Release()
				return free
			}
			require.True(t, command.envelope.beginEnqueue())
			command.Abandon()
			require.False(t, fenceFree(), "abandonment waits for the enqueue in flight")
			command.envelope.finishEnqueue(accepted)
			if accepted {
				require.False(t, fenceFree(), "an accepted close owns its fence")
				require.Empty(t, reply.Result(), "an accepted close is not abandoned")
				return
			}
			require.True(t, fenceFree(), "the refused enqueue settles the abandonment")
			require.ErrorIs(t, <-reply.Result(), errCommandAbandoned)
			require.False(t, actor.TryEnqueueCommand(command), "an abandoned close is spent")
		})
	}
}

// Concurrent enqueue attempts against an actor that refuses them, racing an
// abandonment, always end with the close settled exactly once.
func TestAbandonRacingRefusedEnqueuesSettlesOnce(t *testing.T) {
	for range 200 {
		actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{ProvisionStore: newMockProvisionStore()})
		actor.stopAdmission()
		command, reply := newTestDeprovisionCommand(t, actor)
		var attempts sync.WaitGroup
		for range 4 {
			attempts.Go(func() { require.False(t, actor.TryEnqueueCommand(command)) })
		}
		attempts.Go(command.Abandon)
		attempts.Wait()
		hold, free := actor.cfg.CommandFence.TryLock(testActorLeaseUUID)
		require.True(t, free, "the abandoned close gave its fence back")
		hold.Release()
		require.ErrorIs(t, <-reply.Result(), errCommandAbandoned)
		require.Empty(t, reply.Result(), "abandonment settles once")
	}
}

func TestNewLeaseActorRequiresACloseTimeout(t *testing.T) {
	store := newMockProvisionStore()
	cfg := validConstructionTestActorConfig(context.Background(), store)
	cfg.CloseTimeout = 0
	actor, err := NewLeaseActor(cfg, func(*LeaseActor) { t.Fatal("invalid composition reached registry installation") })
	require.ErrorContains(t, err, "close timeout")
	assert.Nil(t, actor)
}

// A close runs on the actor's lifetime, bounded by CloseTimeout. The command
// carries no context, so no caller can cancel a teardown midway; only the
// backend stopping does.
func TestCloseRunsOnTheActorLifetime(t *testing.T) {
	stop, stopBackend := context.WithCancel(t.Context())
	defer stopBackend()
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusReady})
	const closeTimeout = time.Hour
	teardown := make(chan context.Context, 1)
	actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{
		StopCtx: stop, CloseTimeout: closeTimeout, ProvisionStore: store,
		DoDeprovisionFn: func(ctx context.Context, _ ActorCloseScope) error {
			teardown <- ctx
			<-ctx.Done()
			return ctx.Err()
		},
	})
	command, reply := newTestDeprovisionCommand(t, actor)
	started := time.Now()
	go actor.handle(command.envelope.message)
	var closeCtx context.Context
	select {
	case closeCtx = <-teardown:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not reach teardown")
	}
	deadline, bounded := closeCtx.Deadline()
	require.True(t, bounded, "a close attempt must be bounded")
	assert.WithinDuration(t, started.Add(closeTimeout), deadline, time.Minute)
	require.NoError(t, closeCtx.Err())

	stopBackend()
	select {
	case err := <-reply.Result():
		require.ErrorIs(t, err, context.Canceled, "the close ends with the backend's lifetime")
	case <-time.After(5 * time.Second):
		t.Fatal("close did not end with the backend's lifetime")
	}
}

func TestActorCloseScopeIsExactAndCallbackLifetime(t *testing.T) {
	_, _, releases := newTestOperationSuccess(t, testActorLeaseUUID, shared.OperationIntentProvision)
	runtime, err := releases.ProveRuntimeGeneration(testActorLeaseUUID)
	require.NoError(t, err)
	observation, err := NewContainerDiedObservation("closing-container", runtime)
	require.NoError(t, err)
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{
		LeaseUUID:            testActorLeaseUUID,
		Status:               backend.ProvisionStatusReady,
		ActiveReleaseVersion: runtime.Version(),
		ActiveOperationID:    runtime.OperationID(),
		ContainerIDs:         []string{"closing-container"},
	})
	var actor *LeaseActor
	var escaped ActorCloseScope
	actor = newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{
		ProvisionStore: store,
		DoDeprovisionFn: func(_ context.Context, scope ActorCloseScope) error {
			escaped = scope
			assert.Same(t, scope.state, actor.activeClose.Load())
			assert.True(t, actor.CloseOwnsObservation(observation))
			assert.False(t, actor.CloseOwnsObservation(ActorObservation{}))
			store.UpdateFn(testActorLeaseUUID, func(p *ProvisionState) { p.ActiveReleaseVersion++ })
			assert.False(t, actor.CloseOwnsObservation(observation), "close cannot suppress another runtime generation")
			store.UpdateFn(testActorLeaseUUID, func(p *ProvisionState) { p.ActiveReleaseVersion-- })
			require.NoError(t, releases.Close())
			assert.False(t, actor.CloseOwnsObservation(observation), "close cannot suppress unreadable runtime authority")
			assert.True(t, scope.Matches(actor, actor.cfg.RecoveryLineage))
			assert.False(t, scope.Matches(actor, mustTestRecoveryLineage()),
				"an actor-close scope must not cross coordinator lineages")
			assert.False(t, scope.Matches(&LeaseActor{}, actor.cfg.RecoveryLineage),
				"an actor-close scope must not cross actor generations")
			store.remove(scope.LeaseUUID())
			return nil
		},
	})

	require.NoError(t, actor.handleDeprovision(actorCloseFence(actor)))
	assert.Nil(t, actor.activeClose.Load(), "completed close must clear its observational scope")
	assert.False(t, actor.CloseOwnsObservation(observation))
	assert.Empty(t, escaped.LeaseUUID())
	assert.False(t, escaped.Matches(actor, actor.cfg.RecoveryLineage),
		"a copied actor-close scope must be revoked after its callback returns")
}

func TestActorCloseScopeIsRevokedWhenCallbackPanics(t *testing.T) {
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{
		LeaseUUID: testActorLeaseUUID,
		Status:    backend.ProvisionStatusReady,
	})
	var actor *LeaseActor
	var escaped ActorCloseScope
	actor = newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{
		ProvisionStore: store,
		DoDeprovisionFn: func(_ context.Context, scope ActorCloseScope) error {
			escaped = scope
			assert.Same(t, scope.state, actor.activeClose.Load())
			panic("boom")
		},
	})

	func() {
		defer func() { require.Equal(t, "boom", recover()) }()
		_ = actor.handleDeprovision(actorCloseFence(actor))
	}()
	assert.Nil(t, actor.activeClose.Load(), "panicking close must clear its observational scope")
	assert.Empty(t, escaped.LeaseUUID())
	assert.False(t, escaped.Matches(actor, actor.cfg.RecoveryLineage),
		"panic must not leak actor-close authority")
}

// TestLeaseActor_RetirementRejectsQueuedCommandsBeforeUnregister pins the
// actor-generation handoff. Once a deprovision terminates an actor, commands
// accepted behind that in-flight handler must be rejected before the actor is
// removed from the registry. Executing them during the shutdown drain could
// spawn work after the worker barrier; unregistering first could let a
// replacement actor mutate the same lease concurrently.
func TestLeaseActor_RetirementRejectsQueuedCommandsBeforeUnregister(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID: "lease-1",
		Tenant:    "tenant-a",
		Status:    backend.ProvisionStatusReady,
	})

	finalizerEntered := make(chan struct{})
	releaseFinalizer := make(chan struct{})
	var releaseFinalizerOnce sync.Once
	t.Cleanup(func() {
		releaseFinalizerOnce.Do(func() { close(releaseFinalizer) })
	})
	var finalizerCalls atomic.Int32

	queuedProvisionAck := make(chan error, 1)
	queuedDeprovisionReply := make(chan error, 1)
	// The first close still holds the actor's fence, so the queued close
	// carries a separate one to show that rejection gives it back.
	var queuedCloseFence shared.CommandFence
	unregisterEntered := make(chan struct{})
	releaseUnregister := make(chan struct{})
	var releaseUnregisterOnce sync.Once
	t.Cleanup(func() {
		releaseUnregisterOnce.Do(func() { close(releaseUnregister) })
	})
	var queueSettledBeforeUnregister atomic.Bool

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		DoDeprovisionFn: func(_ context.Context, scope ActorCloseScope) error {
			leaseUUID := scope.LeaseUUID()
			if finalizerCalls.Add(1) == 1 {
				close(finalizerEntered)
				<-releaseFinalizer
			}
			store.remove(leaseUUID)
			return nil
		},
		OnTerminated: func(string) {
			queueSettledBeforeUnregister.Store(
				len(queuedProvisionAck) == 1 && len(queuedDeprovisionReply) == 1,
			)
			close(unregisterEntered)
			<-releaseUnregister
		},
	})

	firstReply := make(chan error, 1)
	requireAccepted(t, func() bool {
		return actor.tryEnqueue(deprovisionMsg{
			Reply: firstReply,
			fence: actorCloseFence(actor),
		})
	})
	select {
	case <-finalizerEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first deprovision did not reach its finalizer")
	}

	var queuedWorkRan atomic.Bool
	requireAccepted(t, func() bool {
		return actor.tryEnqueue(provisionRequestedMsg{
			Ctx: context.Background(),
			Ack: queuedProvisionAck,
		})
	})
	requireAccepted(t, func() bool {
		return actor.tryEnqueue(deprovisionMsg{
			Reply: queuedDeprovisionReply,
			fence: queuedCloseFence.Lock(actor.leaseUUID),
		})
	})

	releaseFinalizerOnce.Do(func() { close(releaseFinalizer) })
	require.NoError(t, <-firstReply)
	select {
	case <-unregisterEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("actor did not reach registry removal")
	}

	assert.True(t, queueSettledBeforeUnregister.Load(),
		"accepted commands must be rejected before a replacement actor can be registered")
	assert.False(t, actor.tryEnqueue(containerDiedMsg{ContainerID: "late"}),
		"a retiring actor must refuse new external admission while still registered")
	releaseUnregisterOnce.Do(func() { close(releaseUnregister) })

	select {
	case <-actor.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("actor did not finish retirement")
	}

	require.ErrorIs(t, <-queuedProvisionAck, errActorTerminated)
	rejectedClose := <-queuedDeprovisionReply
	require.True(t, IsRejectedByRetiringActor(rejectedClose),
		"a close the retiring actor never handled can be routed to its replacement")
	require.False(t, IsRejectedByRetiringActor(fmt.Errorf("wrapped: %w", rejectedClose)),
		"only the reply itself proves the close never ran")
	fence, free := queuedCloseFence.TryLock(actor.leaseUUID)
	require.True(t, free, "a rejected close gives its command fence back")
	fence.Release()
	assert.False(t, queuedWorkRan.Load(),
		"retirement must not execute queued caller work after the worker barrier")
	assert.Equal(t, int32(1), finalizerCalls.Load(),
		"retirement must not execute a second queued destructive close")
}
