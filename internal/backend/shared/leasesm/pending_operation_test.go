package leasesm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// The projection awaits the exact accepted provision from Provisioning entry
// through Failed, and Ready clears it: live recovery matches a settled failure
// to its projection by this stamp (ENG-1125).
func TestPendingOperation_StampedAtProvisioningKeptThroughFailedClearedAtReady(t *testing.T) {
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{
		LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusProvisioning,
	})
	admission, failure := newTestProvisionFailure(t, testActorLeaseUUID)
	operationID := admission.Operation().OperationID()
	release := make(chan struct{})
	actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{
		ProvisionStore: store,
		ProvisionWorkFn: func(context.Context, shared.ProvisionResourceExecution) ProvisionWorkOutcome {
			<-release
			outcome, err := NewProvisionWorkFailure(
				errors.New("refused"), ErrMsgInternal, backend.ReasonInternal, failure,
			)
			require.NoError(t, err)
			return outcome
		},
	})
	ack := make(chan error, 1)
	actor.handleProvisionRequested(provisionRequestedMsg{Ctx: context.Background(), Ack: ack, Admission: admission})
	require.NoError(t, <-ack)

	state, ok := store.Get(testActorLeaseUUID)
	require.True(t, ok)
	assert.True(t, state.PendingOperation.Names(operationID), "Provisioning entry stamps the exact operation")
	assert.False(t, state.PendingOperation.Names(shared.OperationID{}), "a zero operation is never named")

	close(release)
	select {
	case terminal := <-actor.inbox:
		actor.handleAcceptedMessage(terminal)
	case <-time.After(5 * time.Second):
		t.Fatal("provision worker sent no terminal message")
	}
	state, _ = store.Get(testActorLeaseUUID)
	require.Equal(t, backend.ProvisionStatusFailed, state.Status)
	assert.True(t, state.PendingOperation.Names(operationID), "Failed keeps the stamp for recovery's proven-failure path")

	state.SetStatus(backend.ProvisionStatusReady, time.Now())
	assert.Equal(t, PendingOperation{}, state.PendingOperation, "a Ready lease awaits no operation")
}

// A restore entry awaits its exact operation, and a maintenance replacement
// awaits none, whatever the projection awaited before.
func TestPendingOperation_RestoreStampsAndMaintenanceClears(t *testing.T) {
	restore := newTestOperationFixture(t, testActorLeaseUUID, shared.OperationIntentRestore).claim
	provisionOperation := newTestOperationFixture(t, testActorLeaseUUID, shared.OperationIntentProvision).claim

	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusFailed})
	actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{ProvisionStore: store})

	require.NoError(t, actor.sm.applyReplaceEntry([]any{replaceEntryArgs{
		CallbackURL: restore.CallbackURL(), LifecycleCallbackURL: restore.LifecycleCallbackURL(),
		CallbackKind: replaceCallbackOperation, Operation: restore,
	}}, backend.ProvisionStatusRestarting))
	state, _ := store.Get(testActorLeaseUUID)
	assert.True(t, state.PendingOperation.Names(restore.OperationID()), "a restore awaits its exact operation")

	store.put(testActorLeaseUUID, &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusFailed})
	stamped, _ := store.Get(testActorLeaseUUID)
	require.True(t, stamped.AwaitOperation(provisionOperation))
	store.put(testActorLeaseUUID, stamped)
	maintenance := newTestMaintenanceClaim(t, "33333333-3333-4333-8333-333333333333", shared.MaintenanceIntentRestart)
	require.NoError(t, actor.sm.applyReplaceEntry([]any{replaceEntryArgs{
		CallbackKind: replaceCallbackLifecycle, Maintenance: maintenance,
	}}, backend.ProvisionStatusRestarting))
	state, _ = store.Get(testActorLeaseUUID)
	assert.Equal(t, PendingOperation{}, state.PendingOperation, "a maintenance replacement awaits no operation")
}

// A Provisioning or restore entry whose projection cannot await its operation
// is reported (log and metric), never skipped silently, and leaves no earlier
// operation's stamp behind: live recovery matches a failure only by the stamp.
func TestPendingOperation_UnstampableEntryIsReportedAndClearsTheOldStamp(t *testing.T) {
	earlier := newTestOperationFixture(t, testActorLeaseUUID, shared.OperationIntentProvision).claim
	for _, test := range []struct {
		name  string
		enter func(*testing.T, *LeaseActor)
	}{
		{"provision", func(t *testing.T, actor *LeaseActor) {
			require.NoError(t, actor.sm.requestProvision(context.Background(), shared.OperationIntentClaim{}))
		}},
		{"restore", func(t *testing.T, actor *LeaseActor) {
			require.NoError(t, actor.sm.applyReplaceEntry([]any{replaceEntryArgs{
				CallbackKind: replaceCallbackOperation, Operation: shared.OperationIntentClaim{},
			}}, backend.ProvisionStatusRestarting))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newMockProvisionStore()
			stale := &ProvisionState{LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusFailed}
			require.True(t, stale.AwaitOperation(earlier))
			store.put(testActorLeaseUUID, stale)
			metrics := &countingMetrics{}
			actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{ProvisionStore: store, Metrics: metrics})
			if test.name == "restore" {
				require.NoError(t, actor.sm.requestProvision(context.Background(), earlier))
				require.Equal(t, int64(0), metrics.unstamped.Load(), "a stampable entry is not reported")
			}

			test.enter(t, actor)

			assert.Equal(t, int64(1), metrics.unstamped.Load(), "the failed stamp is reported")
			state, _ := store.Get(testActorLeaseUUID)
			assert.Equal(t, PendingOperation{}, state.PendingOperation, "no earlier operation's stamp survives")
		})
	}
}

// A definite startup failure returns the projection to the lease's durable
// runtime inside the actor's own Provisioning -> Failed transition (ENG-1125):
// the worker writes nothing. Without a predecessor the durable runtime is the
// failed claim's own callback pair; no container is published either way.
func TestStartupFailureRestoresTheDurableRuntimeInTheActorTransition(t *testing.T) {
	fixture := newTestOperationFixture(t, testActorLeaseUUID, shared.OperationIntentProvision)
	runtime, err := NewDurableRuntime(fixture.claim, nil)
	require.NoError(t, err)
	startup, err := shared.NewOperationStartupFailure(shared.OperationStartupFailureTerms{
		Reason: backend.ReasonHealthCheckFailed, Message: backend.MsgContainerUnhealthy,
		InstanceID: "candidate-1", Service: "app",
	})
	require.NoError(t, err)

	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{
		LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusProvisioning,
		CallbackURL: "https://candidate.example/cb", LifecycleCallbackURL: "https://candidate.example/life",
		ContainerIDs: []string{"candidate-1"}, ServiceContainers: map[string][]string{"app": {"candidate-1"}},
	})
	actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{ProvisionStore: store})
	require.NoError(t, actor.sm.requestProvision(context.Background(), fixture.claim))
	_, failure := newTestProvisionFailure(t, testActorLeaseUUID)
	require.NoError(t, actor.sm.provisionErrored(context.Background(), provisionErrorInfo{
		callbackErr: startup.Message(), reason: startup.Reason(), lastError: startup.Detail(),
		operationFailure: failure, startup: startup, runtime: runtime,
	}))

	state, _ := store.Get(testActorLeaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, state.Status)
	assert.Equal(t, fixture.claim.CallbackURL(), state.CallbackURL)
	assert.Equal(t, fixture.claim.LifecycleCallbackURL(), state.LifecycleCallbackURL)
	assert.Empty(t, state.ContainerIDs)
	assert.Empty(t, state.ServiceContainers)
	assert.Equal(t, backend.ReasonHealthCheckFailed, state.Reason)
}

// The worker's startup-failure outcome carries a durable runtime derived for
// its own operation, and nothing else.
func TestProvisionWorkStartupFailureRequiresItsOwnDurableRuntime(t *testing.T) {
	own := newTestOperationFixture(t, testActorLeaseUUID, shared.OperationIntentProvision).claim
	runtime, err := NewDurableRuntime(own, nil)
	require.NoError(t, err)
	assert.True(t, runtime.Valid())
	assert.True(t, runtime.awaitedBy(own.OperationID()))
	assert.False(t, runtime.awaitedBy(shared.OperationID{}))
	assert.False(t, DurableRuntime{}.awaitedBy(own.OperationID()), "the zero runtime belongs to no operation")
	_, err = NewDurableRuntime(shared.OperationIntentClaim{}, nil)
	assert.Error(t, err, "an invalid claim derives no runtime")

	untouched := &ProvisionState{LeaseUUID: "22222222-2222-4222-8222-222222222222", CallbackURL: "kept"}
	runtime.Apply(untouched)
	assert.Equal(t, "kept", untouched.CallbackURL, "another lease's projection is left alone")
	DurableRuntime{}.Apply(untouched)
	assert.Equal(t, "kept", untouched.CallbackURL, "the zero runtime applies nothing")
}

// AwaitOperation stamps only from a valid claim for the projection's own lease.
func TestPendingOperation_AwaitRequiresAClaimForTheSameLease(t *testing.T) {
	claim := newTestOperationFixture(t, testActorLeaseUUID, shared.OperationIntentProvision).claim

	other := &ProvisionState{LeaseUUID: "22222222-2222-4222-8222-222222222222"}
	assert.False(t, other.AwaitOperation(claim), "another lease's claim cannot stamp the projection")
	assert.Equal(t, PendingOperation{}, other.PendingOperation)

	zero := &ProvisionState{LeaseUUID: testActorLeaseUUID}
	assert.False(t, zero.AwaitOperation(shared.OperationIntentClaim{}), "an invalid claim cannot stamp the projection")
	assert.Equal(t, PendingOperation{}, zero.PendingOperation)

	own := &ProvisionState{LeaseUUID: testActorLeaseUUID}
	require.True(t, own.AwaitOperation(claim))
	assert.True(t, own.PendingOperation.Names(claim.OperationID()))
}
