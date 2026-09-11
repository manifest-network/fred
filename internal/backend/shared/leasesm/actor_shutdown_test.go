package leasesm

import (
	"context"
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
		MaintenanceWorkFn: func(context.Context, shared.MaintenanceReleaseClaim) ReplaceWorkOutcome {
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

func TestActorCloseScopeIsExactAndCallbackLifetime(t *testing.T) {
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
			assert.True(t, scope.Matches(actor, actor.cfg.RecoveryLineage))
			assert.False(t, scope.Matches(actor, mustTestRecoveryLineage()),
				"an actor-close scope must not cross coordinator lineages")
			assert.False(t, scope.Matches(&LeaseActor{}, actor.cfg.RecoveryLineage),
				"an actor-close scope must not cross actor generations")
			store.remove(scope.LeaseUUID())
			return nil
		},
	})

	require.NoError(t, actor.handleDeprovision(context.Background()))
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
			panic("boom")
		},
	})

	func() {
		defer func() { require.Equal(t, "boom", recover()) }()
		_ = actor.handleDeprovision(context.Background())
	}()
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
	require.True(t, actor.tryEnqueue(deprovisionMsg{
		Ctx:   context.Background(),
		Reply: firstReply,
	}))
	select {
	case <-finalizerEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first deprovision did not reach its finalizer")
	}

	var queuedWorkRan atomic.Bool
	require.True(t, actor.tryEnqueue(provisionRequestedMsg{
		Ctx: context.Background(),
		Ack: queuedProvisionAck,
	}))
	require.True(t, actor.tryEnqueue(deprovisionMsg{
		Ctx:   context.Background(),
		Reply: queuedDeprovisionReply,
	}))

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
	require.ErrorIs(t, <-queuedDeprovisionReply, errActorTerminated)
	assert.False(t, queuedWorkRan.Load(),
		"retirement must not execute queued caller work after the worker barrier")
	assert.Equal(t, int32(1), finalizerCalls.Load(),
		"retirement must not execute a second queued destructive close")
}
