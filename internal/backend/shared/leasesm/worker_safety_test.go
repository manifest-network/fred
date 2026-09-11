package leasesm

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestDeprovisionRefusesTeardownUntilMutationWorkerDrains(t *testing.T) {
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID:    "lease-1",
		Status:       backend.ProvisionStatusProvisioning,
		ContainerIDs: []string{"existing"},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var cancelCalled atomic.Bool
	var deprovisionCalls atomic.Int64
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:            ctx,
		ProvisionStore:     store,
		WorkerDrainTimeout: 25 * time.Millisecond,
		DoDeprovisionFn: func(_ context.Context, scope ActorCloseScope) error {
			leaseUUID := scope.LeaseUUID()
			deprovisionCalls.Add(1)
			store.remove(leaseUUID)
			return nil
		},
	})
	require.NoError(t, actor.sm.requestProvision(context.Background()),
		"test must model an admitted worker-owning Provisioning state, not a reservation")

	// Model a Docker mutation that ignores cancellation and remains capable of
	// publishing a later Compose Up. The barrier, not context cancellation, is
	// the authoritative proof that its effects have stopped.
	actor.workCancel = func() { cancelCalled.Store(true) }
	actor.workers.Add()

	reply := make(chan error, 1)
	require.True(t, actor.tryEnqueue(deprovisionMsg{Ctx: context.Background(), Reply: reply}))
	select {
	case err := <-reply:
		require.ErrorIs(t, err, ErrWorkerDrainTimeout)
	case <-time.After(2 * time.Second):
		t.Fatal("deprovision did not report the worker-drain timeout")
	}

	assert.True(t, cancelCalled.Load(), "preemption must still signal cancellation")
	assert.Equal(t, int64(0), deprovisionCalls.Load(),
		"substrate teardown must not run while the old mutation can still land")
	assert.Equal(t, backend.ProvisionStatusProvisioning, actor.State(),
		"a failed OnExit must leave the SM in its work-owning source state")
	_, exists := store.Get("lease-1")
	assert.True(t, exists, "the live provision must remain for safe retry/recovery")

	// Once the real worker exits, a retry may transition and tear down. This
	// proves the fail-closed timeout is recoverable rather than a permanent
	// Deprovisioning wedge.
	actor.workers.Done()
	retryReply := make(chan error, 1)
	require.True(t, actor.tryEnqueue(deprovisionMsg{Ctx: context.Background(), Reply: retryReply}))
	select {
	case err := <-retryReply:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("deprovision retry did not complete after the worker drained")
	}
	assert.Equal(t, int64(1), deprovisionCalls.Load())
	<-actor.Done()
}

func TestDeprovisionAbsentProjectionStillRunsSubstrateFinalizer(t *testing.T) {
	store := newMockProvisionStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var deprovisionCalls atomic.Int64
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		DoDeprovisionFn: func(_ context.Context, scope ActorCloseScope) error {
			leaseUUID := scope.LeaseUUID()
			assert.Equal(t, "lease-1", leaseUUID)
			deprovisionCalls.Add(1)
			return nil
		},
	})

	reply := make(chan error, 1)
	require.True(t, actor.tryEnqueue(deprovisionMsg{Ctx: context.Background(), Reply: reply}))
	select {
	case err := <-reply:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("absent-projection finalizer did not acknowledge completion")
	}
	assert.Equal(t, int64(1), deprovisionCalls.Load())
	<-actor.Done()
}

func TestCohortDivergenceFailsReadyLeaseIdempotently(t *testing.T) {
	leaseUUID := testActorLeaseUUID
	runtime := newTestRuntimeGenerationProof(t, leaseUUID)
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID:            leaseUUID,
		Status:               backend.ProvisionStatusReady,
		FailCount:            4,
		LifecycleCallbackURL: "https://fred.example/callbacks/lifecycle",
		ActiveReleaseVersion: runtime.Version(),
		ActiveOperationID:    runtime.OperationID(),
		ContainerIDs:         []string{"survivor-a", "survivor-b"},
	})

	ctx, cancel := context.WithCancel(context.Background())
	metrics := &countingMetrics{}
	var inspectCalls atomic.Int64
	var diagnosticCalls atomic.Int64
	var callbackCalls atomic.Int64
	var gotCallbackStatus backend.CallbackStatus
	var gotCallbackError string
	actor := newTestActor(t, leaseUUID, testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		Metrics:        metrics,
		Inspector: &mockInstanceInspector{InspectInstanceFn: func(context.Context, string) (*InstanceState, error) {
			inspectCalls.Add(1)
			return nil, errors.New("cohort divergence must not inspect a fabricated dead instance")
		}},
		PersistDiagnosticsFn: func(entry shared.DiagnosticEntry, ids []string, _ map[string]string) {
			diagnosticCalls.Add(1)
			assert.Equal(t, leaseUUID, entry.LeaseUUID)
			assert.ElementsMatch(t, []string{"survivor-a", "survivor-b"}, ids)
		},
		SendLifecycleFailureFn: func(_ shared.RuntimeGenerationProof, errMsg string) {
			callbackCalls.Add(1)
			gotCallbackStatus = backend.CallbackStatusFailed
			gotCallbackError = errMsg
		},
	})
	t.Cleanup(func() {
		cancel()
		<-actor.Done()
	})

	observation, reply, err := NewCohortDivergedObservation(context.Background(), runtime)
	require.NoError(t, err)
	require.True(t, actor.TryEnqueueObservation(observation))
	require.NoError(t, reply.Wait(context.Background()))

	got, exists := store.Get(leaseUUID)
	require.True(t, exists)
	assert.Equal(t, backend.ProvisionStatusFailed, got.Status)
	assert.Equal(t, 5, got.FailCount)
	assert.Equal(t, backend.ReasonInternal, got.Reason)
	assert.Equal(t, errMsgCohortDiverged, got.Message)
	assert.Equal(t, errMsgCohortDiverged, got.LastError)
	assert.Equal(t, []string{"survivor-a", "survivor-b"}, got.ContainerIDs,
		"recovery failure must describe, not fabricate or rewrite, instance identity")
	assert.Equal(t, int64(0), inspectCalls.Load())
	assert.Equal(t, int64(1), diagnosticCalls.Load())
	assert.Equal(t, int64(1), callbackCalls.Load())
	assert.Equal(t, backend.CallbackStatusFailed, gotCallbackStatus)
	assert.Equal(t, errMsgCohortDiverged, gotCallbackError)

	// A repeated recovery observation after the lease is already Failed is an
	// SM Ignore: no count inflation, duplicate callback, or gauge movement.
	second, secondReply, err := NewCohortDivergedObservation(context.Background(), runtime)
	require.NoError(t, err)
	require.True(t, actor.TryEnqueueObservation(second))
	require.NoError(t, secondReply.Wait(context.Background()))
	got, exists = store.Get(leaseUUID)
	require.True(t, exists)
	assert.Equal(t, 5, got.FailCount)
	assert.Equal(t, int64(1), diagnosticCalls.Load())
	assert.Equal(t, int64(1), callbackCalls.Load())
}

// TestCohortObservationCapturedBeforeInventoryCannotFailReplacementGeneration
// models the recovery ordering that matters: authority is captured first, an
// inventory read reports divergence for that old cohort, and a maintenance
// generation becomes active before the observation reaches the actor. The
// observation's durable re-attestation must supersede it even though
// maintenance deliberately preserves the original operation ID.
func TestCohortObservationCapturedBeforeInventoryCannotFailReplacementGeneration(t *testing.T) {
	leaseUUID := testActorLeaseUUID
	intent := newTestMaintenanceClaim(t, leaseUUID, shared.MaintenanceIntentUpdate)
	value, ok := maintenanceAuthorities.Load(intent.MaintenanceID())
	require.True(t, ok)
	authority := value.(testMaintenanceAuthority)

	// This proof is captured before the simulated inventory read.
	oldRuntime, err := authority.releases.ProveRuntimeGeneration(leaseUUID)
	require.NoError(t, err)
	observation, reply, err := NewCohortDivergedObservation(context.Background(), oldRuntime)
	require.NoError(t, err)

	// The inventory result now belongs to the old generation. Before routing it,
	// maintenance commits a replacement which keeps the same operation ID but
	// advances the exact Release version and digest.
	_ = testMaintenanceSuccess(t, intent, ReplaceSuccessProjection{})
	newRuntime, err := authority.releases.ProveRuntimeGeneration(leaseUUID)
	require.NoError(t, err)
	require.NotEqual(t, oldRuntime.Version(), newRuntime.Version())
	require.Equal(t, oldRuntime.OperationID(), newRuntime.OperationID(),
		"maintenance is intentionally within one operation lineage")

	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID:            leaseUUID,
		Status:               backend.ProvisionStatusReady,
		FailCount:            2,
		ContainerIDs:         []string{"replacement-container"},
		ActiveReleaseVersion: newRuntime.Version(),
		ActiveOperationID:    newRuntime.OperationID(),
	})
	var callbackCalls atomic.Int64
	actor := newTestActorNoSpawn(t, leaseUUID, testActorOpts{
		ProvisionStore: store,
		SendLifecycleFailureFn: func(shared.RuntimeGenerationProof, string) {
			callbackCalls.Add(1)
		},
	})

	assert.False(t, observation.Current(store),
		"routing must re-attest the pre-inventory proof after replacement")
	actor.handle(observation.message())
	require.NoError(t, reply.Wait(context.Background()))

	got, exists := store.Get(leaseUUID)
	require.True(t, exists)
	assert.Equal(t, backend.ProvisionStatusReady, got.Status)
	assert.Equal(t, 2, got.FailCount)
	assert.Equal(t, []string{"replacement-container"}, got.ContainerIDs)
	assert.Zero(t, callbackCalls.Load())
}
