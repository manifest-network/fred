package provisioner

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/provisioner/operation"
)

var (
	_ EventOperations      = (*operation.Registry)(nil)
	_ CallbackOperations   = (*operation.Registry)(nil)
	_ TimeoutOperations    = (*operation.Registry)(nil)
	_ ReconcilerOperations = (*operation.Registry)(nil)
)

func TestPlacementPortsExcludeRawAndUnrelatedAuthority(t *testing.T) {
	provision := reflect.TypeOf((*ProvisionPlacement)(nil)).Elem()
	reconciler := reflect.TypeOf((*ReconcilerPlacement)(nil)).Elem()
	aggregate := reflect.TypeOf((*PlacementAuthorityStore)(nil)).Elem()

	require.True(t, aggregate.Implements(provision))
	require.True(t, aggregate.Implements(reconciler))

	for _, method := range []string{
		"SetAttempting", "SetAttemptingIfNotNewer", "Confirm",
		"ConfirmAttemptIfRevision", "ClearAttemptIfRevision",
		"SetBatchIfNotNewer", "DeleteIfRevision",
	} {
		_, provisionExposes := provision.MethodByName(method)
		_, reconcilerExposes := reconciler.MethodByName(method)
		assert.False(t, provisionExposes, "provision port exposes raw method %s", method)
		assert.False(t, reconcilerExposes, "reconciler port exposes raw method %s", method)
	}

	for _, method := range []string{
		"BeginInventorySession", "ProjectInventory", "MarkInventoryReady",
		"InvalidateInventoryAuthority", "DeleteRecord",
	} {
		_, exposed := provision.MethodByName(method)
		assert.False(t, exposed, "provision port exposes reconciler method %s", method)
	}
	for _, method := range []string{"BeginAttempt", "ConfirmOperation", "RefuseOperation"} {
		_, exposed := reconciler.MethodByName(method)
		assert.False(t, exposed, "reconciler port exposes unrelated method %s", method)
	}
}

func TestReconcilerRetainsOnlyConsumerOwnedAuthorityPorts(t *testing.T) {
	reconcilerType := reflect.TypeOf(Reconciler{})
	for _, forbidden := range []string{"tracker", "placementView", "legacyPlacement"} {
		_, retained := reconcilerType.FieldByName(forbidden)
		assert.False(t, retained, "Reconciler retains legacy field %s", forbidden)
	}

	operations, ok := reconcilerType.FieldByName("operations")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*ReconcilerOperations)(nil)).Elem(), operations.Type)

	placementAuthority, ok := reconcilerType.FieldByName("placementAuthority")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*ReconcilerPlacement)(nil)).Elem(), placementAuthority.Type)
}

func TestHandlerAndTimeoutConfigurationExposeOnlyConsumerPorts(t *testing.T) {
	handlerDeps := reflect.TypeOf(HandlerDeps{})
	for _, forbidden := range []string{
		"Tracker", "Operations", "Acknowledger", "BackendRouter", "Placement",
	} {
		_, exposed := handlerDeps.FieldByName(forbidden)
		assert.False(t, exposed, "HandlerDeps exposes compatibility field %s", forbidden)
	}
	eventField, ok := handlerDeps.FieldByName("EventOperations")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*EventOperations)(nil)).Elem(), eventField.Type)
	callbackField, ok := handlerDeps.FieldByName("Callbacks")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*CallbackApplication)(nil)).Elem(), callbackField.Type)
	provisionerField, ok := handlerDeps.FieldByName("Orchestrator")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*EventProvisioner)(nil)).Elem(), provisionerField.Type)
	payloadField, ok := handlerDeps.FieldByName("PayloadStore")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*HandlerPayloadStore)(nil)).Elem(), payloadField.Type)

	timeoutConfig := reflect.TypeOf(TimeoutCheckerConfig{})
	_, exposesTracker := timeoutConfig.FieldByName("Tracker")
	assert.False(t, exposesTracker)
	timeoutField, ok := timeoutConfig.FieldByName("Operations")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*TimeoutOperations)(nil)).Elem(), timeoutField.Type)

	handler := reflect.TypeOf(HandlerSet{})
	for _, forbidden := range []string{"tracker", "operations", "callbackInitErr"} {
		_, retained := handler.FieldByName(forbidden)
		assert.False(t, retained, "HandlerSet retains compatibility field %s", forbidden)
	}
	eventPort, ok := handler.FieldByName("eventOperations")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*EventOperations)(nil)).Elem(), eventPort.Type)
	callbackPort, ok := handler.FieldByName("callbacks")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*CallbackApplication)(nil)).Elem(), callbackPort.Type)
}

func TestReconcilerRuntimeExposesOnlyConsumerOwnedOperations(t *testing.T) {
	runtime := reflect.TypeOf((*ReconcilerRuntime)(nil)).Elem()
	_, exposesRegistry := runtime.MethodByName("Operations")
	assert.False(t, exposesRegistry, "ReconcilerRuntime exposes the concrete registry accessor")

	method, ok := runtime.MethodByName("ReconcilerOperations")
	require.True(t, ok)
	require.Equal(t, 1, method.Type.NumOut())
	assert.Equal(t, reflect.TypeOf((*ReconcilerOperations)(nil)).Elem(), method.Type.Out(0))
}

type typedNilCallbackApplication struct{}

func (*typedNilCallbackApplication) HandleCallback(context.Context, CallbackCommand) error {
	return nil
}
