package provisioner

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

func TestPlacementPortsExposeOnlyConsumerOwnedAuthority(t *testing.T) {
	provision := reflect.TypeOf((*placement.ProvisionCoordinator)(nil))
	for _, method := range []string{
		"BeginInventorySession", "ProjectInventory", "DeleteRecord",
	} {
		_, exposed := provision.MethodByName(method)
		assert.False(t, exposed, "provision port exposes reconciler method %s", method)
	}
	reconciler := reflect.TypeOf((*placement.ReconciliationCoordinator)(nil))
	for _, method := range []string{
		"BeginRestore", "ConfirmRestore", "ConfirmOperation", "RefuseOperation",
	} {
		_, exposed := reconciler.MethodByName(method)
		assert.False(t, exposed, "reconciler port exposes unrelated method %s", method)
	}
	projected := reflect.TypeOf((*placement.ProjectedReconciliationSweep)(nil))
	for _, method := range []string{"Fenced", "ScopeRecordlessAdmission"} {
		_, exposed := projected.MethodByName(method)
		assert.False(t, exposed, "projected sweep exposes raw projection method %s", method)
	}
	sweep := reflect.TypeOf((*placement.ReconciliationSweep)(nil))
	for _, method := range []string{"RecordProvision", "RecordRetention", "RecordUntrusted"} {
		_, exposed := sweep.MethodByName(method)
		assert.False(t, exposed,
			"reconciliation sweep exposes caller-manufactured inventory method %s", method)
	}
}

func TestPlacementStoreDoesNotExposeUnclaimedSettlement(t *testing.T) {
	store := reflect.TypeOf((*placement.Store)(nil))
	for _, method := range []string{
		"ConfirmAttempt", "RefuseAttempt", "ConfirmOperation", "RefuseOperation",
		"BeginInventorySession", "EndInventorySession", "BindInventoryProjector",
		"ProjectInventory",
	} {
		_, exposed := store.MethodByName(method)
		assert.False(t, exposed,
			"placement Store exposes settlement without an exact AttemptClaim: %s", method)
	}
}

func TestOperationCoordinatorDoesNotExposeCallerSelectedDispatchOutcomes(t *testing.T) {
	coordinator := reflect.TypeOf((*placement.OperationCoordinator)(nil))
	for _, method := range []string{
		"CompleteProvisionAccepted", "CompleteProvisionRefused", "CompleteProvisionAmbiguous",
		"CompleteRestoreAccepted", "CompleteRestoreRefused", "CompleteRestoreAmbiguous",
		"JoinProvisionDispatch", "BeginProvisionCall", "CompleteProvision", "AbortProvisionDispatch",
		"JoinRestoreDispatch", "BindRestoreBackend", "BeginRestoreCall", "CompleteRestore", "AbortRestoreDispatch",
		"TryClaimCallback", "ReleaseCallback", "FinishConfirmedCallback", "FinishRefusedCallback",
		"TryClaimRecoveryCallback", "ReleaseRecoveryCallback",
		"TryClaimTimeout", "ReleaseTimeout", "FinishTimeoutPreservingForRedelivery",
		"TryClaimDeprovision", "ReleaseDeprovision", "FinishDeprovision",
	} {
		_, exposed := coordinator.MethodByName(method)
		assert.False(t, exposed,
			"operation coordinator exposes raw authority method %s", method)
	}
	for _, method := range []string{
		"ProvisionCoordinator", "ProvisionCoordinatorWithPayloads", "RestoreCoordinator", "ReconciliationCoordinator",
		"MaintenanceCoordinator",
	} {
		_, exposed := coordinator.MethodByName(method)
		assert.False(t, exposed,
			"operation coordinator exposes backend-bound purpose facet %s", method)
	}
	for _, method := range []string{
		"AuthenticatedCallbackCoordinator", "TimeoutCoordinator",
	} {
		_, exposed := coordinator.MethodByName(method)
		assert.False(t, exposed,
			"unbound operation coordinator exposes executable settlement facet %s", method)
	}
	_, exposesBinder := coordinator.MethodByName("BindBackendRuntime")
	assert.True(t, exposesBinder,
		"operation coordinator must expose its one backend-runtime binding transition")

	execution := reflect.TypeOf((*placement.ExecutionCoordinator)(nil))
	_, exposesLegacyProvisionAlias := execution.MethodByName("ProvisionCoordinator")
	assert.False(t, exposesLegacyProvisionAlias, "execution coordinator exposes a test-only provision alias")
	for _, method := range []string{
		"ProvisionCoordinatorWithPayloads", "RestoreCoordinator", "ReconciliationCoordinator",
		"MaintenanceCoordinator",
	} {
		_, exposed := execution.MethodByName(method)
		assert.True(t, exposed, "execution coordinator does not mint purpose facet %s", method)
	}
	for _, method := range []string{
		"AuthenticatedCallbackCoordinator", "TimeoutCoordinator",
	} {
		_, exposed := execution.MethodByName(method)
		assert.True(t, exposed,
			"fully bound execution coordinator does not mint settlement facet %s", method)
	}
}

func TestReconcilerRetainsOnlyConsumerOwnedAuthorityPorts(t *testing.T) {
	reconcilerType := reflect.TypeOf(Reconciler{})
	for _, forbidden := range []string{
		"tracker", "placementView", "legacyPlacement", "operations",
		"placementAuthority", "dispatches", "inventoryProjector",
	} {
		_, retained := reconcilerType.FieldByName(forbidden)
		assert.False(t, retained, "Reconciler retains legacy field %s", forbidden)
	}

	coordinator, ok := reconcilerType.FieldByName("coordinator")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*placement.ReconciliationCoordinator)(nil)), coordinator.Type)
}

func TestHandlerAndTimeoutConfigurationExposeOnlyConsumerPorts(t *testing.T) {
	handlerDeps := reflect.TypeOf(HandlerDeps{})
	for _, forbidden := range []string{
		"Tracker", "Operations", "Acknowledger", "BackendRouter", "Placement",
	} {
		_, exposed := handlerDeps.FieldByName(forbidden)
		assert.False(t, exposed, "HandlerDeps exposes compatibility field %s", forbidden)
	}
	eventField, ok := handlerDeps.FieldByName("Events")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*HandlerEventCoordinator)(nil)), eventField.Type)
	callbackField, ok := handlerDeps.FieldByName("Callbacks")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*CallbackApplication)(nil)).Elem(), callbackField.Type)
	_, exposesOrchestrator := handlerDeps.FieldByName("Orchestrator")
	assert.False(t, exposesOrchestrator)
	payloadField, ok := handlerDeps.FieldByName("PayloadStore")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*HandlerPayloadStore)(nil)).Elem(), payloadField.Type)

	timeoutConfig := reflect.TypeOf(TimeoutCheckerConfig{})
	_, exposesTracker := timeoutConfig.FieldByName("Tracker")
	assert.False(t, exposesTracker)
	timeoutField, ok := timeoutConfig.FieldByName("Coordinator")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*placement.TimeoutCoordinator)(nil)), timeoutField.Type)

	handler := reflect.TypeOf(HandlerSet{})
	for _, forbidden := range []string{"tracker", "operations", "callbackInitErr"} {
		_, retained := handler.FieldByName(forbidden)
		assert.False(t, retained, "HandlerSet retains compatibility field %s", forbidden)
	}
	eventPort, ok := handler.FieldByName("events")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*HandlerEventCoordinator)(nil)), eventPort.Type)
	callbackPort, ok := handler.FieldByName("callbacks")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*CallbackApplication)(nil)).Elem(), callbackPort.Type)
}

func TestReconcilerConfigCannotSpliceStoreOrRegistry(t *testing.T) {
	config := reflect.TypeOf(ReconcilerConfig{})
	for _, forbidden := range []string{"Operations", "Registry", "Placement", "Store"} {
		_, exposed := config.FieldByName(forbidden)
		assert.False(t, exposed, "ReconcilerConfig exposes independently spliceable %s", forbidden)
	}
	field, ok := config.FieldByName("Coordinator")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*placement.ReconciliationCoordinator)(nil)), field.Type)
}

func TestManagerConfigCannotSpliceCallbackOrigin(t *testing.T) {
	config := reflect.TypeOf(ManagerConfig{})
	_, exposed := config.FieldByName("CallbackBaseURL")
	assert.False(t, exposed,
		"callback routes must come only from the factory bound to PlacementStore")
}

func TestCallbackCatalogCannotExposeBackendClients(t *testing.T) {
	catalog := reflect.TypeOf((*CallbackBackendCatalog)(nil)).Elem()
	require.Equal(t, 1, catalog.NumMethod())
	method := catalog.Method(0)
	assert.Equal(t, "HasBackend", method.Name)
	assert.Equal(t, reflect.TypeOf(false), method.Type.Out(0))
}

type typedNilCallbackApplication struct{}

func (*typedNilCallbackApplication) HandleCallback(context.Context, hmacauth.VerifiedRequest) error {
	return nil
}
