package leasesm

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/maintenanceid"
)

func TestActorBoundaryDoesNotExportWritableMessageOrResultShapes(t *testing.T) {
	for _, value := range []any{
		ActorCloseScope{}, ActorReply{}, ActorCommand{}, ActorObservation{}, RecoveryCommand{}, ProvisionSuccessResult{},
		ReplaceSuccessResult{}, ReplaceFailureInfo{}, ReplaceResult{},
	} {
		typeOf := reflect.TypeOf(value)
		for index := range typeOf.NumField() {
			assert.Falsef(t, typeOf.Field(index).IsExported(), "%s.%s must not be caller-writable", typeOf, typeOf.Field(index).Name)
		}
	}

	actorReplyType := reflect.TypeFor[ActorReply]()
	for _, constructor := range []any{
		NewMaintenanceRecoveredSuccessMsg,
		NewMaintenanceRecoveredFailureReadyMsg,
		NewMaintenanceRecoveredFailureFailedMsg,
		NewMaintenanceRecoveredRuntimeFailureMsg,
	} {
		typeOf := reflect.TypeOf(constructor)
		for index := range typeOf.NumIn() {
			assert.NotEqualf(t, reflect.Chan, typeOf.In(index).Kind(), "%s must mint its own reply channel", typeOf)
		}
		require.GreaterOrEqual(t, typeOf.NumOut(), 2)
		assert.Equal(t, actorReplyType, typeOf.Out(1), "recovery constructor must return the receive-only reply capability")
	}
}

func TestActorCommandConstructorsRejectMissingOrWrongCapabilities(t *testing.T) {
	provision := newTestOperationFixture(t, "11111111-1111-4111-8111-111111111111", shared.OperationIntentProvision).admission

	_, _, err := NewProvisionCommand(nil, provision) //nolint:staticcheck // Deliberately verify nil-context rejection.
	require.Error(t, err)
	// Restore claims cannot construct a provision command: its distinct
	// admission type is issued only by the provision capacity boundary.
	_, _, err = NewProvisionCommand(context.Background(), shared.ProvisionAdmission{})
	require.Error(t, err)
	_, _, err = NewRestoreCommand(context.Background(), provision.Operation())
	require.Error(t, err)
	_, _, err = NewRestoreCommand(context.Background(), shared.OperationIntentClaim{})
	require.Error(t, err)
	_, _, err = NewDeprovisionCommand(nil) //nolint:staticcheck // Deliberately verify nil-context rejection.
	require.Error(t, err)
	_, err = NewContainerDiedObservation("", shared.RuntimeGenerationProof{})
	require.Error(t, err)
	_, _, err = NewCohortDivergedObservation(nil, shared.RuntimeGenerationProof{}) //nolint:staticcheck // Deliberately verify nil-context rejection.
	require.Error(t, err)
	_, err = NewContainerDiedObservation("container-a", shared.RuntimeGenerationProof{})
	require.Error(t, err)

	maintenance := newTestMaintenanceClaim(t, "33333333-3333-4333-8333-333333333333", shared.MaintenanceIntentRestart)
	target := testMaintenanceTarget(t, maintenance)
	_, _, err = NewUpdateCommand(context.Background(), target)
	require.Error(t, err, "restart authority must not construct an update command")
	_, _, err = NewCustomDomainCommand(context.Background(), target)
	require.Error(t, err, "restart authority must not construct a custom-domain command")
	customDomain := newTestMaintenanceClaim(t, "44444444-4444-4444-8444-444444444444", shared.MaintenanceIntentCustomDomain)
	customDomainTarget := testMaintenanceTarget(t, customDomain)
	_, _, err = NewRestartCommand(context.Background(), customDomainTarget)
	require.Error(t, err, "custom-domain authority must not construct a restart command")
	_, err = NewMaintenanceRecoveryFailureInfo(
		shared.MaintenanceIntentClaim{}, ReplaceFailureDetails{},
	)
	require.Error(t, err)
	_, _, err = NewMaintenanceRecoveredFailureReadyMsg(
		maintenance, MaintenanceRecoveryProjection{}, ReplaceFailureInfo{},
	)
	require.Error(t, err, "zero recovery details must not cross the actor boundary")
}

func TestSuccessProjectionConstructorsRejectInvalidShape(t *testing.T) {
	_, committed, _ := newTestOperationSuccess(t, "44444444-4444-4444-8444-444444444444", shared.OperationIntentProvision)
	tests := []ProvisionSuccessProjection{
		{ContainerIDs: []string{"c1", "c1"}, ServiceContainers: map[string][]string{"app": {"c1"}}},
		{ContainerIDs: []string{"c1"}, ServiceContainers: map[string][]string{"app": {"other"}}},
		{ContainerIDs: []string{"c1", "c2"}, ServiceContainers: map[string][]string{"app": {"c1"}}},
		{ContainerIDs: []string{"c1"}, ServiceContainers: map[string][]string{"wrong": {"c1"}}},
	}
	for _, projection := range tests {
		_, err := NewProvisionSuccessResult(projection, committed)
		require.Error(t, err)
	}
}

func TestSuccessProjectionIsDetachedAndDerivedFromCommittedRelease(t *testing.T) {
	_, committed, _ := newTestOperationSuccess(t, "45454545-4545-4454-8454-454545454545", shared.OperationIntentProvision)
	containerIDs := []string{"container-a"}
	serviceContainers := map[string][]string{"app": {"container-a"}}
	result, err := NewProvisionSuccessResult(ProvisionSuccessProjection{
		ContainerIDs:      containerIDs,
		ServiceContainers: serviceContainers,
	}, committed)
	require.NoError(t, err)

	containerIDs[0] = "caller-mutated"
	serviceContainers["app"][0] = "caller-mutated"
	serviceContainers["forged"] = []string{"caller-mutated"}

	assert.Equal(t, []string{"container-a"}, result.containerIDs)
	assert.Equal(t, map[string][]string{"app": {"container-a"}}, result.serviceContainers)
	require.NotNil(t, result.stackManifest)
	assert.Len(t, result.stackManifest.Services, 1)
	assert.Contains(t, result.stackManifest.Services, "app")
}

func TestRestoreSuccessCannotComposeAcrossJournalPairsWithSameOperationID(t *testing.T) {
	const leaseUUID = "46464646-4646-4464-8464-464646464646"
	left := newTestOperationFixture(t, leaseUUID, shared.OperationIntentRestore)
	right := newTestOperationFixture(t, leaseUUID, shared.OperationIntentRestore)
	require.Equal(t, left.claim.OperationID(), right.claim.OperationID(),
		"regression requires deliberately colliding public operation identities")

	bindLeaseSMOperationExecutor(t, right.settlement)
	execution, err := right.settlement.StartOperationExecution(right.candidate)
	require.NoError(t, err)
	physical := right.settlement.ExecuteOperation(t.Context(), execution)
	success, ok := physical.(shared.OperationExecutionSuccess)
	require.True(t, ok)
	committed, err := right.settlement.CommitOperationSuccess(success)
	require.NoError(t, err)

	assert.False(t, committed.MatchesIntent(left.claim),
		"equal UUIDs from different open journal pairs are not interchangeable authority")
	outcome, err := NewRestoreWorkSuccess(committed)
	require.NoError(t, err)
	err = validateReplaceWorkOutcome(outcome, shared.MaintenanceIntentClaim{}, left.claim)
	require.ErrorContains(t, err, "another authority")
}

func TestMaintenanceTerminalOutcomeCannotComposeAcrossJournalPairsWithSameID(t *testing.T) {
	const leaseUUID = "47474747-4747-4474-8474-474747474747"
	tests := []struct {
		name       string
		newOutcome func(*testing.T, shared.MaintenanceIntentClaim) ReplaceWorkOutcome
	}{
		{
			name: "success",
			newOutcome: func(t *testing.T, claim shared.MaintenanceIntentClaim) ReplaceWorkOutcome {
				result := testMaintenanceSuccess(t, claim, ReplaceSuccessProjection{})
				outcome, err := NewMaintenanceWorkSuccess(result.success.maintenanceRelease)
				require.NoError(t, err)
				return outcome
			},
		},
		{
			name: "failure",
			newOutcome: func(t *testing.T, claim shared.MaintenanceIntentClaim) ReplaceWorkOutcome {
				result := testMaintenanceFailure(
					t, claim, errors.New("definitive failure"), false, false,
					ReplaceFailureDetails{
						CallbackErr: "definitive failure",
						Reason:      backend.ReasonInternal,
						LastError:   "definitive failure",
					},
				)
				outcome, err := NewMaintenanceWorkResult(result)
				require.NoError(t, err)
				return outcome
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			maintenanceID, err := maintenanceid.New()
			require.NoError(t, err)
			left := newTestMaintenanceClaimWithID(
				t, leaseUUID, shared.MaintenanceIntentRestart, maintenanceID,
			)
			leftTarget := testMaintenanceTarget(t, left)
			right := newTestMaintenanceClaimWithID(
				t, leaseUUID, shared.MaintenanceIntentRestart, maintenanceID,
			)
			require.Equal(t, left.MaintenanceID(), right.MaintenanceID(),
				"regression requires deliberately colliding public maintenance identities")
			assert.False(t, right.MatchesIntent(left),
				"equal UUIDs from different open journal pairs are not interchangeable authority")

			outcome := test.newOutcome(t, right)
			require.NoError(t, validateReplaceWorkOutcome(
				outcome, right, shared.OperationIntentClaim{},
			), "control outcome must belong to its issuing maintenance generation")
			require.ErrorContains(t, validateReplaceWorkOutcome(
				outcome, left, shared.OperationIntentClaim{},
			), "another authority")

			store := newMockProvisionStore()
			store.put(leaseUUID, &ProvisionState{
				LeaseUUID: leaseUUID,
				Status:    backend.ProvisionStatusReady,
				ContainerIDs: []string{
					"source-container",
				},
				ServiceContainers: map[string][]string{
					"app": {"source-container"},
				},
			})
			actor := newTestActorNoSpawn(t, leaseUUID, testActorOpts{
				ProvisionStore: store,
				MaintenanceWorkFn: func(
					context.Context,
					shared.MaintenanceReleaseClaim,
				) ReplaceWorkOutcome {
					return outcome
				},
			})
			actor.pendingMaintenance = left
			actor.spawnMaintenanceWorker(t.Context(), leftTarget)
			require.NoError(t, actor.waitForWorkers())
			terminal := <-actor.inbox
			ambiguous, ok := terminal.(operationAmbiguousMsg)
			require.True(t, ok, "foreign proof must not cross the actor terminal boundary")
			assert.ErrorContains(t, ambiguous.err, "another authority")

			state, found := store.Get(leaseUUID)
			require.True(t, found)
			assert.Equal(t, []string{"source-container"}, state.ContainerIDs,
				"foreign success must not pre-publish its projection")
			assert.Equal(t, map[string][]string{"app": {"source-container"}}, state.ServiceContainers)
		})
	}
}

func TestLifecycleObservationCarriesExactActiveOperationGeneration(t *testing.T) {
	const leaseUUID = "55555555-5555-4555-8555-555555555555"
	runtime := newTestRuntimeGenerationProof(t, leaseUUID)
	store := newMockProvisionStore()
	store.put(leaseUUID, &ProvisionState{
		LeaseUUID: leaseUUID, Status: backend.ProvisionStatusReady,
		ActiveReleaseVersion: runtime.Version(), ActiveOperationID: runtime.OperationID(),
	})
	observed := make(chan shared.RuntimeGenerationProof, 1)
	stopCtx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	actor := newTestActor(t, leaseUUID, testActorOpts{
		StopCtx: stopCtx, ProvisionStore: store,
		SendLifecycleFailureFn: func(got shared.RuntimeGenerationProof, _ string) { observed <- got },
	})
	message, reply, err := NewCohortDivergedObservation(context.Background(), runtime)
	require.NoError(t, err)
	require.True(t, actor.TryEnqueueObservation(message))
	require.NoError(t, reply.Wait(context.Background()))
	select {
	case got := <-observed:
		assert.Equal(t, runtime.LeaseUUID(), got.LeaseUUID())
		assert.Equal(t, runtime.Version(), got.Version())
		assert.Equal(t, runtime.OperationID(), got.OperationID())
	case <-time.After(time.Second):
		t.Fatal("lifecycle failure did not carry its active operation generation")
	}
}

func TestOpaqueMessageCapabilitiesAreCopySafeAndAcceptedOnce(t *testing.T) {
	actor := newTestActorNoSpawn(t, testActorLeaseUUID, testActorOpts{})

	command, _, err := NewDeprovisionCommand(t.Context())
	require.NoError(t, err)
	copyOfCommand := command
	require.True(t, actor.TryEnqueueCommand(command))
	assert.False(t, actor.TryEnqueueCommand(copyOfCommand), "a copied command must share one-shot admission")
	assert.False(t, actor.TryEnqueueCommand(ActorCommand{}), "zero command must fail closed")

	runtime := newTestRuntimeGenerationProof(t, testActorLeaseUUID)
	observation, err := NewContainerDiedObservation("c1", runtime)
	require.NoError(t, err)
	copyOfObservation := observation
	require.True(t, actor.TryEnqueueObservation(observation))
	assert.False(t, actor.TryEnqueueObservation(copyOfObservation), "a copied observation must share one-shot admission")
	assert.False(t, actor.TryEnqueueObservation(ActorObservation{}), "zero observation must fail closed")
	assert.False(t, actor.TryEnqueueRecovery(RecoveryCommand{}), "zero recovery command must fail closed")
}
