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
