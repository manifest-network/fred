package leasesm

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

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
		DoDeprovisionFn: func(_ context.Context, leaseUUID string) error {
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
	require.True(t, actor.TryEnqueue(DeprovisionMsg{
		Ctx:   context.Background(),
		Reply: firstReply,
	}))
	select {
	case <-finalizerEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first deprovision did not reach its finalizer")
	}

	var queuedWorkRan atomic.Bool
	require.True(t, actor.TryEnqueue(ProvisionRequestedMsg{
		Cancel: func() {},
		Work: func() (string, backend.Reason, ProvisionSuccessResult, map[string]string, error) {
			queuedWorkRan.Store(true)
			return "", "", ProvisionSuccessResult{}, nil, nil
		},
		Ack: queuedProvisionAck,
	}))
	require.True(t, actor.TryEnqueue(DeprovisionMsg{
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
	assert.False(t, actor.TryEnqueue(ContainerDiedMsg{ContainerID: "late"}),
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
