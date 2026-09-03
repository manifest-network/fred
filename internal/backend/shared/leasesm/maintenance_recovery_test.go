package leasesm

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func maintenanceRecoveryActor(
	t *testing.T,
	status backend.ProvisionStatus,
	claim shared.MaintenanceIntentClaim,
) (*LeaseActor, *mockProvisionStore, *int) {
	t.Helper()
	store := newMockProvisionStore()
	store.put(testActorLeaseUUID, &ProvisionState{
		LeaseUUID:            testActorLeaseUUID,
		Tenant:               "tenant-a",
		ProviderUUID:         "22222222-2222-4222-8222-222222222222",
		Status:               status,
		CallbackURL:          "https://source.example/callbacks/provision?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8",
		LifecycleCallbackURL: "https://source.example/callbacks/provision?lifecycle_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8",
		Items:                []backend.LeaseItem{{SKU: "sku-a", ServiceName: "app", Quantity: 1}},
		StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
			"app": {Image: "source:1"},
		}},
	})
	deliveries := 0
	actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{
		ProvisionStore: store,
		SendMaintenanceCallbackFn: func(shared.MaintenanceIntentClaim, backend.CallbackStatus, string) {
			deliveries++
		},
		SendLifecycleCallbackFn: func(string, string, backend.CallbackStatus, string) {
			deliveries++
		},
	})
	actor.replaceCallbackKind = replaceCallbackLifecycle
	actor.pendingReplaceCallbackURL = claim.CallbackURL()
	actor.pendingReplaceLifecycleCallbackURL = claim.LifecycleCallbackURL()
	actor.pendingMaintenance = claim
	return actor, store, &deliveries
}

func targetMaintenanceRecoveryProjection(
	_ shared.MaintenanceIntentClaim,
) MaintenanceRecoveryProjection {
	return MaintenanceRecoveryProjection{}
}

func TestMaintenanceRecoveredSuccessPromotesExactProjectionWithoutDelivery(t *testing.T) {
	claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentUpdate)
	actor, store, deliveries := maintenanceRecoveryActor(
		t, backend.ProvisionStatusUpdating, claim,
	)
	target := claim.TargetRelease()
	targetStack, err := manifest.ParsePayload(target.Manifest)
	require.NoError(t, err)
	reply := make(chan error, 1)
	projection := targetMaintenanceRecoveryProjection(claim)
	projection.ContainerIDs = []string{"target-1"}
	projection.ServiceContainers = map[string][]string{"app": {"target-1"}}
	msg, err := NewMaintenanceRecoveredSuccessMsg(claim, projection, reply)
	require.NoError(t, err)
	actor.handle(msg)
	require.NoError(t, <-reply)

	assert.Equal(t, backend.ProvisionStatusReady, actor.State())
	state, found := store.Get(testActorLeaseUUID)
	require.True(t, found)
	assert.Equal(t, claim.CallbackURL(), state.CallbackURL)
	assert.Equal(t, claim.LifecycleCallbackURL(), state.LifecycleCallbackURL)
	assert.Equal(t, target.Items, state.Items)
	assert.Equal(t, targetStack, state.StackManifest)
	assert.Equal(t, []string{"target-1"}, state.ContainerIDs)
	assert.Equal(t, map[string][]string{"app": {"target-1"}}, state.ServiceContainers)
	assert.False(t, actor.pendingMaintenance.Valid())
	assert.Zero(t, *deliveries)
}

func TestMaintenanceRecoveredFailureProjectionIsTypedAndKeepsSourceRoute(t *testing.T) {
	for _, test := range []struct {
		name string
		new  func(shared.MaintenanceIntentClaim, MaintenanceRecoveryProjection, ReplaceFailureInfo, chan error) (LeaseMessage, error)
		want backend.ProvisionStatus
	}{
		{name: "exact source ready", new: NewMaintenanceRecoveredFailureReadyMsg, want: backend.ProvisionStatusReady},
		{name: "source not proven", new: NewMaintenanceRecoveredFailureFailedMsg, want: backend.ProvisionStatusFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentRestart)
			actor, store, deliveries := maintenanceRecoveryActor(
				t, backend.ProvisionStatusRestarting, claim,
			)
			reply := make(chan error, 1)
			msg, err := test.new(claim, MaintenanceRecoveryProjection{}, ReplaceFailureInfo{
				Operation: "restart", CallbackErr: "interrupted", LastError: "interrupted",
			}, reply)
			require.NoError(t, err)
			actor.handle(msg)
			require.NoError(t, <-reply)

			assert.Equal(t, test.want, actor.State())
			state, found := store.Get(testActorLeaseUUID)
			require.True(t, found)
			assert.Contains(t, state.CallbackURL, "source.example")
			assert.Contains(t, state.LifecycleCallbackURL, "source.example")
			assert.False(t, actor.pendingMaintenance.Valid())
			assert.Zero(t, *deliveries)
		})
	}
}

func TestMaintenanceRecoveredRejectsWrongIdentityAndActiveWorker(t *testing.T) {
	claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentRestart)
	other := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentRestart)
	actor, _, _ := maintenanceRecoveryActor(t, backend.ProvisionStatusRestarting, claim)

	reply := make(chan error, 1)
	wrong, err := NewMaintenanceRecoveredFailureFailedMsg(
		other, MaintenanceRecoveryProjection{}, ReplaceFailureInfo{}, reply,
	)
	require.NoError(t, err)
	actor.handle(wrong)
	assert.ErrorContains(t, <-reply, "differs from actor generation")
	assert.Equal(t, backend.ProvisionStatusRestarting, actor.State())

	actor.markMaintenanceWorker(claim.MaintenanceID())
	reply = make(chan error, 1)
	owned, err := NewMaintenanceRecoveredFailureFailedMsg(
		claim, MaintenanceRecoveryProjection{}, ReplaceFailureInfo{}, reply,
	)
	require.NoError(t, err)
	actor.handle(owned)
	assert.ErrorContains(t, <-reply, "worker remains active")
	assert.Equal(t, backend.ProvisionStatusRestarting, actor.State())
}

func TestMaintenanceRecoveredIsIdempotentAfterWorkerTerminalWins(t *testing.T) {
	claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentRestart)
	actor, _, deliveries := maintenanceRecoveryActor(t, backend.ProvisionStatusReady, claim)
	for range 2 {
		reply := make(chan error, 1)
		msg, err := NewMaintenanceRecoveredSuccessMsg(
			claim, targetMaintenanceRecoveryProjection(claim), reply,
		)
		require.NoError(t, err)
		actor.handle(msg)
		require.NoError(t, <-reply)
	}
	assert.Equal(t, backend.ProvisionStatusReady, actor.State())
	assert.False(t, actor.pendingMaintenance.Valid())
	assert.Zero(t, *deliveries)
}

func TestMaintenanceRecoveredCorrectsContradictoryTerminalProjection(t *testing.T) {
	t.Run("durable success corrects failed actor", func(t *testing.T) {
		claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentUpdate)
		actor, store, deliveries := maintenanceRecoveryActor(t, backend.ProvisionStatusFailed, claim)
		reply := make(chan error, 1)
		projection := targetMaintenanceRecoveryProjection(claim)
		projection.ContainerIDs = []string{"target"}
		msg, err := NewMaintenanceRecoveredSuccessMsg(claim, projection, reply)
		require.NoError(t, err)
		actor.handle(msg)
		require.NoError(t, <-reply)
		assert.Equal(t, backend.ProvisionStatusReady, actor.State())
		state, found := store.Get(testActorLeaseUUID)
		require.True(t, found)
		assert.Equal(t, []string{"target"}, state.ContainerIDs)
		assert.Equal(t, claim.TargetRelease().Items, state.Items)
		assert.Zero(t, *deliveries)
	})

	t.Run("durable failure corrects ready actor", func(t *testing.T) {
		claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentUpdate)
		actor, _, deliveries := maintenanceRecoveryActor(t, backend.ProvisionStatusReady, claim)
		reply := make(chan error, 1)
		msg, err := NewMaintenanceRecoveredFailureFailedMsg(
			claim,
			MaintenanceRecoveryProjection{},
			ReplaceFailureInfo{Operation: "update", CallbackErr: "interrupted", LastError: "interrupted"},
			reply,
		)
		require.NoError(t, err)
		actor.handle(msg)
		require.NoError(t, <-reply)
		assert.Equal(t, backend.ProvisionStatusFailed, actor.State())
		assert.Zero(t, *deliveries)
	})
}

func TestMaintenanceRecoveredRuntimeFailureIsCompoundAndIdempotent(t *testing.T) {
	claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentUpdate)
	actor, store, deliveries := maintenanceRecoveryActor(
		t, backend.ProvisionStatusUpdating, claim,
	)
	store.UpdateFn(testActorLeaseUUID, func(state *ProvisionState) {
		state.FailCount = 7
	})
	projection := MaintenanceRecoveryProjection{
		ContainerIDs:      []string{"target-1"},
		ServiceContainers: map[string][]string{"app": {"target-1"}},
	}

	for attempt := range 2 {
		if attempt == 1 {
			// Model an entry-action panic after the FSM transitioned: retry must
			// repair the stale store without replaying terminal side effects.
			store.UpdateFn(testActorLeaseUUID, func(state *ProvisionState) {
				state.Status = backend.ProvisionStatusUpdating
				state.ContainerIDs = []string{"stale-source"}
				state.CallbackURL = "https://source.example/callbacks/provision?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8"
			})
		}
		reply := make(chan error, 1)
		msg, err := NewMaintenanceRecoveredRuntimeFailureMsg(claim, projection, reply)
		require.NoError(t, err)
		actor.handle(msg)
		require.NoError(t, <-reply)
		assert.Equal(t, backend.ProvisionStatusFailed, actor.State())
		state, found := store.Get(testActorLeaseUUID)
		require.True(t, found)
		assert.Equal(t, backend.ProvisionStatusFailed, state.Status)
		assert.Equal(t, []string{"target-1"}, state.ContainerIDs)
		assert.Equal(t, claim.CallbackURL(), state.CallbackURL)
		assert.Equal(t, claim.LifecycleCallbackURL(), state.LifecycleCallbackURL)
		assert.Equal(t, claim.TargetRelease().Items, state.Items)
		assert.Equal(t, ErrMsgCohortDiverged, state.LastError)
		assert.Equal(t, 7, state.FailCount)
	}
	assert.Zero(t, *deliveries)
}

func TestMaintenanceRecoveredSameStateRepairsProvisionAfterEntryActionPanic(t *testing.T) {
	for _, test := range []struct {
		name    string
		state   backend.ProvisionStatus
		newMsg  func(shared.MaintenanceIntentClaim, MaintenanceRecoveryProjection, ReplaceFailureInfo, chan error) (LeaseMessage, error)
		failure ReplaceFailureInfo
	}{
		{
			name:    "ready source",
			state:   backend.ProvisionStatusReady,
			newMsg:  NewMaintenanceRecoveredFailureReadyMsg,
			failure: ReplaceFailureInfo{CallbackErr: "interrupted", LastError: "interrupted", Reason: backend.ReasonRestartFailed},
		},
		{
			name:    "failed source",
			state:   backend.ProvisionStatusFailed,
			newMsg:  NewMaintenanceRecoveredFailureFailedMsg,
			failure: ReplaceFailureInfo{CallbackErr: "interrupted", LastError: "interrupted", Reason: backend.ReasonUpdateFailed},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentUpdate)
			actor, store, deliveries := maintenanceRecoveryActor(t, test.state, claim)
			store.UpdateFn(testActorLeaseUUID, func(state *ProvisionState) {
				state.Status = backend.ProvisionStatusUpdating
				state.FailCount = 11
			})
			reply := make(chan error, 1)
			msg, err := test.newMsg(
				claim, MaintenanceRecoveryProjection{}, test.failure, reply,
			)
			require.NoError(t, err)
			actor.handle(msg)
			require.NoError(t, <-reply)
			projected, found := store.Get(testActorLeaseUUID)
			require.True(t, found)
			assert.Equal(t, test.state, projected.Status)
			assert.Equal(t, test.failure.CallbackErr, projected.Message)
			assert.Equal(t, test.failure.LastError, projected.LastError)
			assert.Equal(t, test.failure.Reason, projected.Reason)
			assert.Equal(t, 11, projected.FailCount)
			assert.Contains(t, projected.CallbackURL, "source.example")
			assert.Zero(t, *deliveries)
		})
	}
}

func TestRestartUpdateRejectMissingMaintenanceAuthorityBeforeTransition(t *testing.T) {
	for _, update := range []bool{false, true} {
		store := newMockProvisionStore()
		store.put(testActorLeaseUUID, &ProvisionState{
			LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusReady,
		})
		actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{ProvisionStore: store})
		ack := make(chan error, 1)
		work := func() ReplaceResult { return ReplaceResult{Err: errors.New("must not run")} }
		if update {
			actor.handleUpdateRequested(UpdateRequestedMsg{Work: work, Ack: ack})
		} else {
			actor.handleRestartRequested(RestartRequestedMsg{Work: work, Ack: ack})
		}
		assert.ErrorContains(t, <-ack, "valid maintenance intent claim")
		assert.Equal(t, backend.ProvisionStatusReady, actor.State())
	}
}
