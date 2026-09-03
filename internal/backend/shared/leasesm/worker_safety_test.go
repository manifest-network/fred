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
		DoDeprovisionFn: func(_ context.Context, leaseUUID string) error {
			deprovisionCalls.Add(1)
			store.remove(leaseUUID)
			return nil
		},
	})

	// Model a Docker mutation that ignores cancellation and remains capable of
	// publishing a later Compose Up. The barrier, not context cancellation, is
	// the authoritative proof that its effects have stopped.
	actor.workCancel = func() { cancelCalled.Store(true) }
	actor.workers.Add()

	reply := make(chan error, 1)
	require.True(t, actor.TryEnqueue(DeprovisionMsg{Ctx: context.Background(), Reply: reply}))
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
	require.True(t, actor.TryEnqueue(DeprovisionMsg{Ctx: context.Background(), Reply: retryReply}))
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
		DoDeprovisionFn: func(_ context.Context, leaseUUID string) error {
			assert.Equal(t, "lease-1", leaseUUID)
			deprovisionCalls.Add(1)
			return nil
		},
	})

	reply := make(chan error, 1)
	require.True(t, actor.TryEnqueue(DeprovisionMsg{Ctx: context.Background(), Reply: reply}))
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
	store := newMockProvisionStore()
	store.put("lease-1", &ProvisionState{
		LeaseUUID:            "lease-1",
		Status:               backend.ProvisionStatusReady,
		FailCount:            4,
		LifecycleCallbackURL: "https://fred.example/callbacks/lifecycle",
		ContainerIDs:         []string{"survivor-a", "survivor-b"},
	})

	ctx, cancel := context.WithCancel(context.Background())
	metrics := &countingMetrics{}
	var inspectCalls atomic.Int64
	var diagnosticCalls atomic.Int64
	var callbackCalls atomic.Int64
	var gotCallbackURL string
	var gotCallbackStatus backend.CallbackStatus
	var gotCallbackError string
	actor := newTestActor(t, "lease-1", testActorOpts{
		StopCtx:        ctx,
		ProvisionStore: store,
		Metrics:        metrics,
		Inspector: &mockInstanceInspector{InspectInstanceFn: func(context.Context, string) (*InstanceState, error) {
			inspectCalls.Add(1)
			return nil, errors.New("cohort divergence must not inspect a fabricated dead instance")
		}},
		PersistDiagnosticsFn: func(entry shared.DiagnosticEntry, ids []string, _ map[string]string) {
			diagnosticCalls.Add(1)
			assert.Equal(t, "lease-1", entry.LeaseUUID)
			assert.ElementsMatch(t, []string{"survivor-a", "survivor-b"}, ids)
		},
		SendLifecycleCallbackFn: func(_ string, callbackURL string, status backend.CallbackStatus, errMsg string) {
			callbackCalls.Add(1)
			gotCallbackURL = callbackURL
			gotCallbackStatus = status
			gotCallbackError = errMsg
		},
	})
	t.Cleanup(func() {
		cancel()
		<-actor.Done()
	})

	reply := make(chan error, 1)
	require.True(t, actor.TryEnqueue(CohortDivergedMsg{Ctx: context.Background(), Reply: reply}))
	select {
	case err := <-reply:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("cohort-divergence transition did not acknowledge completion")
	}

	got, exists := store.Get("lease-1")
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
	assert.Equal(t, int64(1), metrics.activeProvisionsDec.Load())
	assert.Equal(t, "https://fred.example/callbacks/lifecycle", gotCallbackURL)
	assert.Equal(t, backend.CallbackStatusFailed, gotCallbackStatus)
	assert.Equal(t, errMsgCohortDiverged, gotCallbackError)

	// A repeated recovery observation after the lease is already Failed is an
	// SM Ignore: no count inflation, duplicate callback, or gauge movement.
	secondReply := make(chan error, 1)
	require.True(t, actor.TryEnqueue(CohortDivergedMsg{Ctx: context.Background(), Reply: secondReply}))
	select {
	case err := <-secondReply:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("idempotent cohort-divergence observation did not acknowledge")
	}
	got, exists = store.Get("lease-1")
	require.True(t, exists)
	assert.Equal(t, 5, got.FailCount)
	assert.Equal(t, int64(1), diagnosticCalls.Load())
	assert.Equal(t, int64(1), callbackCalls.Load())
	assert.Equal(t, int64(1), metrics.activeProvisionsDec.Load())
}
