package leasesm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func handleRecoveryCommand(t *testing.T, actor *LeaseActor, command RecoveryCommand) {
	t.Helper()
	require.True(t, actor.TryEnqueueRecovery(command))
	actor.handleAcceptedMessage(<-actor.inbox)
}

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
		SendLifecycleFailureFn: func(shared.RuntimeGenerationProof, string) {
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
	claim shared.MaintenanceIntentClaim,
) MaintenanceRecoveryProjection {
	projection := MaintenanceRecoveryProjection{
		ServiceContainers: make(map[string][]string),
	}
	for _, item := range claim.TargetRelease().Items {
		for instance := range item.Quantity {
			id := fmt.Sprintf("%s-%d", item.ServiceName, instance)
			projection.ContainerIDs = append(projection.ContainerIDs, id)
			projection.ServiceContainers[item.ServiceName] = append(
				projection.ServiceContainers[item.ServiceName], id,
			)
		}
	}
	return projection
}

func activeMaintenanceForRecovery(
	t *testing.T,
	claim shared.MaintenanceIntentClaim,
) shared.MaintenanceReleaseActive {
	t.Helper()
	result := testMaintenanceSuccess(t, claim, ReplaceSuccessProjection{})
	require.True(t, result.success.maintenanceRelease.Valid())
	return result.success.maintenanceRelease
}

func TestMaintenanceRecoveredSuccessPromotesExactProjectionWithoutDelivery(t *testing.T) {
	claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentUpdate)
	active := activeMaintenanceForRecovery(t, claim)
	actor, store, deliveries := maintenanceRecoveryActor(
		t, backend.ProvisionStatusUpdating, claim,
	)
	target := claim.TargetRelease()
	targetStack, err := manifest.ParsePayload(target.Manifest)
	require.NoError(t, err)
	projection := targetMaintenanceRecoveryProjection(claim)
	projection.ContainerIDs = []string{"target-1"}
	projection.ServiceContainers = map[string][]string{"app": {"target-1"}}
	msg, reply, err := NewMaintenanceRecoveredSuccessMsg(active, projection)
	require.NoError(t, err)
	handleRecoveryCommand(t, actor, msg)
	require.NoError(t, reply.Wait(t.Context()))

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
		new  func(shared.MaintenanceIntentClaim, MaintenanceRecoveryProjection, ReplaceFailureInfo) (RecoveryCommand, ActorReply, error)
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
			info := maintenanceRecoveryFailureInfo(t, claim, ReplaceFailureDetails{
				CallbackErr: "interrupted", LastError: "interrupted",
			})
			msg, reply, err := test.new(claim, MaintenanceRecoveryProjection{}, info)
			require.NoError(t, err)
			handleRecoveryCommand(t, actor, msg)
			require.NoError(t, reply.Wait(t.Context()))

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
	otherInfo := maintenanceRecoveryFailureInfo(t, other, ReplaceFailureDetails{})
	_, _, err := NewMaintenanceRecoveredFailureFailedMsg(
		claim, MaintenanceRecoveryProjection{}, otherInfo,
	)
	require.ErrorContains(t, err, "details belong to another intent")

	wrong, wrongReply, err := NewMaintenanceRecoveredFailureFailedMsg(
		other, MaintenanceRecoveryProjection{}, otherInfo,
	)
	require.NoError(t, err)
	handleRecoveryCommand(t, actor, wrong)
	assert.ErrorContains(t, wrongReply.Wait(t.Context()), "differs from actor generation")
	assert.Equal(t, backend.ProvisionStatusRestarting, actor.State())

	actor.markMaintenanceWorker(claim.MaintenanceID())
	claimInfo := maintenanceRecoveryFailureInfo(t, claim, ReplaceFailureDetails{})
	owned, ownedReply, err := NewMaintenanceRecoveredFailureFailedMsg(
		claim, MaintenanceRecoveryProjection{}, claimInfo,
	)
	require.NoError(t, err)
	handleRecoveryCommand(t, actor, owned)
	assert.ErrorContains(t, ownedReply.Wait(t.Context()), "worker remains active")
	assert.Equal(t, backend.ProvisionStatusRestarting, actor.State())
}

func TestMaintenanceRecoveredCannotComposeAcrossSettlementsWithSameIntent(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(*testing.T, shared.MaintenanceIntentClaim, *shared.MaintenanceSettlement) (RecoveryCommand, ActorReply)
	}{
		{
			name: "success",
			run: func(
				t *testing.T,
				foreign shared.MaintenanceIntentClaim,
				settlement *shared.MaintenanceSettlement,
			) (RecoveryCommand, ActorReply) {
				active, err := settlement.ProveMaintenanceActive(foreign)
				require.NoError(t, err)
				command, reply, err := NewMaintenanceRecoveredSuccessMsg(
					active, targetMaintenanceRecoveryProjection(foreign),
				)
				require.NoError(t, err)
				return command, reply
			},
		},
		{
			name: "failure",
			run: func(
				t *testing.T,
				foreign shared.MaintenanceIntentClaim,
				_ *shared.MaintenanceSettlement,
			) (RecoveryCommand, ActorReply) {
				info, err := NewMaintenanceRecoveryFailureInfo(
					foreign,
					ReplaceFailureDetails{
						CallbackErr: "definitive failure",
						Reason:      backend.ReasonInternal,
						LastError:   "definitive failure",
					},
				)
				require.NoError(t, err)
				command, reply, err := NewMaintenanceRecoveredFailureFailedMsg(
					foreign, MaintenanceRecoveryProjection{}, info,
				)
				require.NoError(t, err)
				return command, reply
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			claim := newTestMaintenanceClaim(
				t, testActorLeaseUUID, shared.MaintenanceIntentRestart,
			)
			if test.name == "success" {
				_ = activeMaintenanceForRecovery(t, claim)
			} else {
				_ = testMaintenanceFailure(
					t, claim, fmt.Errorf("definitive failure"), false, false,
					ReplaceFailureDetails{
						CallbackErr: "definitive failure",
						Reason:      backend.ReasonInternal,
						LastError:   "definitive failure",
					},
				)
			}

			value, ok := maintenanceAuthorities.Load(claim.MaintenanceID())
			require.True(t, ok)
			authority := value.(testMaintenanceAuthority)
			foreignSettlement, err := shared.NewMaintenanceSettlement(
				authority.callbacks, authority.releases,
			)
			require.NoError(t, err)
			foreign, found, err := foreignSettlement.GetMaintenanceIntent(claim.LeaseUUID())
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, claim.MaintenanceID(), foreign.MaintenanceID())
			assert.False(t, foreign.MatchesIntent(claim),
				"a separately minted settlement capability is not actor authority")

			actor, _, _ := maintenanceRecoveryActor(
				t, backend.ProvisionStatusRestarting, claim,
			)
			command, reply := test.run(t, foreign, foreignSettlement)
			handleRecoveryCommand(t, actor, command)
			assert.ErrorContains(t, reply.Wait(t.Context()), "differs from actor generation")
			assert.Equal(t, backend.ProvisionStatusRestarting, actor.State())
			assert.True(t, actor.pendingMaintenance.MatchesIntent(claim))
		})
	}
}

func TestMaintenanceRecoveredIsIdempotentAfterWorkerTerminalWins(t *testing.T) {
	claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentRestart)
	active := activeMaintenanceForRecovery(t, claim)
	actor, _, deliveries := maintenanceRecoveryActor(t, backend.ProvisionStatusReady, claim)
	for range 2 {
		msg, reply, err := NewMaintenanceRecoveredSuccessMsg(
			active, targetMaintenanceRecoveryProjection(claim),
		)
		require.NoError(t, err)
		handleRecoveryCommand(t, actor, msg)
		require.NoError(t, reply.Wait(t.Context()))
	}
	assert.Equal(t, backend.ProvisionStatusReady, actor.State())
	assert.False(t, actor.pendingMaintenance.Valid())
	assert.Zero(t, *deliveries)
}

func TestMaintenanceRecoveredCorrectsContradictoryTerminalProjection(t *testing.T) {
	t.Run("durable success corrects failed actor", func(t *testing.T) {
		claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentUpdate)
		active := activeMaintenanceForRecovery(t, claim)
		actor, store, deliveries := maintenanceRecoveryActor(t, backend.ProvisionStatusFailed, claim)
		projection := targetMaintenanceRecoveryProjection(claim)
		projection.ContainerIDs = []string{"target"}
		projection.ServiceContainers = map[string][]string{"app": {"target"}}
		msg, reply, err := NewMaintenanceRecoveredSuccessMsg(active, projection)
		require.NoError(t, err)
		handleRecoveryCommand(t, actor, msg)
		require.NoError(t, reply.Wait(t.Context()))
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
		info := maintenanceRecoveryFailureInfo(t, claim, ReplaceFailureDetails{
			CallbackErr: "interrupted", LastError: "interrupted",
		})
		msg, reply, err := NewMaintenanceRecoveredFailureFailedMsg(
			claim,
			MaintenanceRecoveryProjection{},
			info,
		)
		require.NoError(t, err)
		handleRecoveryCommand(t, actor, msg)
		require.NoError(t, reply.Wait(t.Context()))
		assert.Equal(t, backend.ProvisionStatusFailed, actor.State())
		assert.Zero(t, *deliveries)
	})
}

func TestMaintenanceRecoveredRuntimeFailureIsCompoundAndIdempotent(t *testing.T) {
	claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentUpdate)
	active := activeMaintenanceForRecovery(t, claim)
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
		msg, reply, err := NewMaintenanceRecoveredRuntimeFailureMsg(active, projection)
		require.NoError(t, err)
		handleRecoveryCommand(t, actor, msg)
		require.NoError(t, reply.Wait(t.Context()))
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
		newMsg  func(shared.MaintenanceIntentClaim, MaintenanceRecoveryProjection, ReplaceFailureInfo) (RecoveryCommand, ActorReply, error)
		details ReplaceFailureDetails
	}{
		{
			name:   "ready source",
			state:  backend.ProvisionStatusReady,
			newMsg: NewMaintenanceRecoveredFailureReadyMsg,
			details: ReplaceFailureDetails{
				CallbackErr: "interrupted", LastError: "interrupted", Reason: backend.ReasonRestartFailed,
			},
		},
		{
			name:   "failed source",
			state:  backend.ProvisionStatusFailed,
			newMsg: NewMaintenanceRecoveredFailureFailedMsg,
			details: ReplaceFailureDetails{
				CallbackErr: "interrupted", LastError: "interrupted", Reason: backend.ReasonUpdateFailed,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			claim := newTestMaintenanceClaim(t, testActorLeaseUUID, shared.MaintenanceIntentUpdate)
			actor, store, deliveries := maintenanceRecoveryActor(t, test.state, claim)
			failure := maintenanceRecoveryFailureInfo(t, claim, test.details)
			store.UpdateFn(testActorLeaseUUID, func(state *ProvisionState) {
				state.Status = backend.ProvisionStatusUpdating
				state.FailCount = 11
			})
			msg, reply, err := test.newMsg(
				claim, MaintenanceRecoveryProjection{}, failure,
			)
			require.NoError(t, err)
			handleRecoveryCommand(t, actor, msg)
			require.NoError(t, reply.Wait(t.Context()))
			projected, found := store.Get(testActorLeaseUUID)
			require.True(t, found)
			assert.Equal(t, test.state, projected.Status)
			assert.Equal(t, failure.callbackErr, projected.Message)
			assert.Equal(t, failure.lastError, projected.LastError)
			assert.Equal(t, failure.reason, projected.Reason)
			assert.Equal(t, 11, projected.FailCount)
			assert.Contains(t, projected.CallbackURL, "source.example")
			assert.Zero(t, *deliveries)
		})
	}
}

func maintenanceRecoveryFailureInfo(
	t *testing.T,
	claim shared.MaintenanceIntentClaim,
	details ReplaceFailureDetails,
) ReplaceFailureInfo {
	t.Helper()
	info, err := NewMaintenanceRecoveryFailureInfo(claim, details)
	require.NoError(t, err)
	return info
}

func TestRestartUpdateRejectMissingMaintenanceAuthorityBeforeTransition(t *testing.T) {
	for _, update := range []bool{false, true} {
		store := newMockProvisionStore()
		store.put(testActorLeaseUUID, &ProvisionState{
			LeaseUUID: testActorLeaseUUID, Status: backend.ProvisionStatusReady,
		})
		actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{ProvisionStore: store})
		ack := make(chan error, 1)
		if update {
			actor.handleUpdateRequested(updateRequestedMsg{Ctx: t.Context(), Ack: ack})
		} else {
			actor.handleRestartRequested(restartRequestedMsg{Ctx: t.Context(), Ack: ack})
		}
		assert.ErrorContains(t, <-ack, "valid maintenance intent claim")
		assert.Equal(t, backend.ProvisionStatusReady, actor.State())
	}
}
