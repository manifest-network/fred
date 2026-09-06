package placement

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

func TestLegacyProvisionErrorCannotClearDurableAttempt(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-legacy-error-ambiguous")
	client := &executionTestBackend{name: "backend-a", provision: func(
		context.Context,
		backend.ProvisionRequest,
	) error {
		return errors.Join(backend.ErrValidation, context.DeadlineExceeded)
	}}
	execution := bindExecutionForTest(
		t, fixture.coordinator, newExecutionTestRuntime(client),
	)

	result := executeProvision(
		t.Context(), fixture.coordinator, execution.backends, nil, fixture.dispatch, nil,
	)

	assert.Equal(t, DispatchPreserved, result.Disposition())
	assert.True(t, result.CallAmbiguous())
	assert.ErrorIs(t, result.CallErr(), backend.ErrValidation)
	remaining := fixture.store.Lookup("lease-legacy-error-ambiguous")
	assert.Equal(t, StateAttempting, remaining.State())
	assert.Equal(t, fixture.initiation.ID(), remaining.AttemptOperationID())
}

func TestLegacyValidationErrorCannotAuthorizeProvisionEventRejection(t *testing.T) {
	const leaseUUID = "61638ef8-1401-4f14-a355-1ae02afeb35b"
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, store, "backend-a")
	coordinator, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	client := &executionTestBackend{name: "backend-a", provision: func(
		context.Context,
		backend.ProvisionRequest,
	) error {
		return errors.Join(backend.ErrValidation, context.DeadlineExceeded)
	}}
	execution := bindExecutionForTest(
		t, coordinator, newExecutionTestRuntime(client),
	)
	authority, err := execution.ProvisionCoordinator(nil)
	require.NoError(t, err)
	request, err := NewProvisionEventRequest(leaseUUID, "tenant-test")
	require.NoError(t, err)

	result := authority.ExecuteCurrentLease(t.Context(), request)

	assert.Equal(t, ProvisionEventUncertain, result.Disposition())
	assert.ErrorIs(t, result.Err(), backend.ErrValidation)
	assert.Equal(t, StateAttempting, store.Lookup(leaseUUID).State())
}

func TestFreshNoDispatchClearsAttemptWithoutBecomingARefusal(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-known-unsent")
	client, err := backend.NewIdentityBoundHTTPClient(backend.HTTPClientConfig{
		Name: "backend-a", BaseURL: "https://backend.invalid",
		Secret: causalOutcomeTestSecret,
	}, causalOutcomeTestIdentity{})
	require.NoError(t, err)
	execution := bindExecutionForTest(
		t, fixture.coordinator, newExecutionTestRuntime(client),
	)

	result := executeProvision(
		t.Context(), fixture.coordinator, execution.backends, nil, fixture.dispatch, nil,
	)

	assert.True(t, result.Applied(), result.Err())
	assert.True(t, result.CallNotDispatched())
	assert.False(t, result.CallDefinitivelyRefused())
	assert.Equal(t, backend.ProvisionRefusalNone, result.ProvisionRefusal())
	assert.ErrorIs(t, result.CallErr(), backend.ErrBackendStorageIdentityUnbound)
	assert.Equal(t, StateAbsent, fixture.store.Lookup("lease-known-unsent").State())
}

func recoverProvisionAttemptForTest(
	t *testing.T,
	leaseUUID string,
	client backend.Backend,
) (*Store, AttemptRecoveryResult) {
	t.Helper()
	fixture := newCoordinatorFixture(t, leaseUUID)
	record := fixture.store.Lookup(leaseUUID)
	callback := fixture.coordinator.operations.TryClaimCallback(
		leaseUUID, fixture.initiation.ID(),
	)
	require.True(t, callback.Claimed())
	require.True(t, fixture.coordinator.operations.FinishCallback(callback.Claim()))
	lease := fixture.coordinator.operations.TryClaimLeaseNow(leaseUUID)
	require.True(t, lease.Acquired())
	t.Cleanup(func() { fixture.coordinator.operations.ReleaseLease(lease.Claim()) })

	runtime := newExecutionTestRuntime(client)
	execution := bindExecutionForTest(t, fixture.coordinator, runtime)
	setProviderControlPlaneForTest(t, execution, liveRecoveryReader())
	_, recovery, err := fixture.coordinator.bindReconciliation(
		execution.controlPlane, runtime, nil,
		inventoryProjectorForTest(t, fixture.store),
	)
	require.NoError(t, err)
	result := recovery.Recover(
		t.Context(), record.RecordRevision(), lease.Claim(), operation.LeaseClaim{},
	)
	return fixture.store, result
}

func TestAttemptRecoveryRequiresTransportMintedRefusal(t *testing.T) {
	t.Run("legacy error tree is preserved", func(t *testing.T) {
		client := &executionTestBackend{name: "backend-a", provision: func(
			context.Context,
			backend.ProvisionRequest,
		) error {
			return errors.Join(backend.ErrValidation, context.DeadlineExceeded)
		}}
		store, result := recoverProvisionAttemptForTest(
			t, "lease-recovery-legacy-error", client,
		)
		assert.True(t, result.Preserved())
		assert.ErrorIs(t, result.CallErr(), backend.ErrValidation)
		assert.Equal(t, StateAttempting,
			store.Lookup("lease-recovery-legacy-error").State())
	})

	t.Run("verified HTTP refusal clears exact attempt", func(t *testing.T) {
		client := causalOutcomeHTTPClient(
			t, http.StatusBadRequest, `{"error":"invalid request"}`,
		)
		store, result := recoverProvisionAttemptForTest(
			t, "lease-recovery-http-refusal", client,
		)
		assert.True(t, result.Refused(), result.Err())
		assert.ErrorIs(t, result.CallErr(), backend.ErrValidation)
		assert.Equal(t, StateAbsent,
			store.Lookup("lease-recovery-http-refusal").State())
	})

	t.Run("known no-dispatch preserves redelivery evidence", func(t *testing.T) {
		client, err := backend.NewIdentityBoundHTTPClient(backend.HTTPClientConfig{
			Name: "backend-a", BaseURL: "https://backend.invalid",
			Secret: causalOutcomeTestSecret,
		}, causalOutcomeTestIdentity{})
		require.NoError(t, err)
		store, result := recoverProvisionAttemptForTest(
			t, "lease-recovery-not-dispatched", client,
		)
		assert.True(t, result.Preserved())
		assert.ErrorIs(t, result.CallErr(), backend.ErrBackendStorageIdentityUnbound)
		assert.Equal(t, StateAttempting,
			store.Lookup("lease-recovery-not-dispatched").State())
	})
}
