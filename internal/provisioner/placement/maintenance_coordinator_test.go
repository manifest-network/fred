package placement

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/maintenanceid"
)

type maintenanceLeaseReaderFunc func(context.Context, string) (*billingtypes.Lease, error)

func (read maintenanceLeaseReaderFunc) GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	return read(ctx, leaseUUID)
}

func maintenanceActiveLeaseReader() maintenanceLeaseReaderFunc {
	return func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
		return &billingtypes.Lease{
			Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
			State: billingtypes.LEASE_STATE_ACTIVE,
		}, nil
	}
}

func newMaintenanceCoordinatorForTest(
	t *testing.T,
	reader maintenanceLeaseReaderFunc,
	client backend.Backend,
) (*MaintenanceCoordinator, *Store) {
	t.Helper()
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	prepareMaintenanceLease(t, store)
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(client))
	setProviderControlPlaneForTest(t, execution, reader)
	authority, err := execution.MaintenanceCoordinator(nil)
	require.NoError(t, err)
	return authority, store
}

func admitMaintenanceForTest(
	t *testing.T,
	authority *MaintenanceCoordinator,
) (MaintenanceCommandClaim, AuthorizedMaintenanceCommand) {
	t.Helper()
	id := mustMaintenanceID(t, maintenanceIDA)
	preparation := authority.prepareMaintenanceCommand(
		t.Context(), id, maintenanceLease, "tenant-test", MaintenanceCommandRestart, nil,
	)
	require.True(t, preparation.Authorized(), preparation.Err())
	admission, err := authority.beginMaintenanceCommand(preparation)
	require.NoError(t, err)
	require.True(t, admission.Pending())
	reauthorization := authority.reauthorizeMaintenanceCommand(t.Context(), admission.Claim())
	require.True(t, reauthorization.Authorized(), reauthorization.Err())
	return admission.Claim(), reauthorization.Authorization()
}

func TestMaintenanceCoordinatorIsFullyBoundAndZeroInvalid(t *testing.T) {
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	runtime := executionRuntime("backend-a")
	execution := bindExecutionForTest(t, base, runtime)
	setProviderControlPlaneForTest(t, execution, maintenanceActiveLeaseReader())
	authority, err := execution.MaintenanceCoordinator(nil)
	require.NoError(t, err)
	require.True(t, authority.Valid())

	otherStore := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	otherBase, err := otherStore.BindOperationCoordinator(nil)
	require.NoError(t, err)
	var typedNilControlPlane *testProviderControlPlane
	otherExecution, err := otherBase.BindBackendRuntime(runtime, typedNilControlPlane)
	require.Error(t, err)
	require.Nil(t, otherExecution)
	require.Nil(t, otherBase.execution)

	var zero MaintenanceCoordinator
	assert.False(t, zero.Valid())
	assert.False(t, zero.tryClaimLeaseNow("lease").Acquired())
	require.ErrorIs(t,
		zero.executeMaintenance(t.Context(), AuthorizedMaintenanceCommand{}).Err(),
		ErrInvalidMaintenanceCoordinator,
	)
}

func TestMaintenanceExecuteOwnsPhysicalOutcomeAndInvokesExactlyOnce(t *testing.T) {
	var calls atomic.Int32
	client := &executionTestBackend{name: "backend-a"}
	client.restart = func(context.Context, backend.RestartRequest) error {
		calls.Add(1)
		return nil
	}
	authority, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), client)
	_, authorization := admitMaintenanceForTest(t, authority)
	copyOfAuthorization := authorization

	completion := authority.executeMaintenance(t.Context(), authorization)
	require.True(t, completion.Settled(), completion.Err())
	assert.True(t, completion.BackendAccepted())
	assert.Equal(t, int32(1), calls.Load())

	stale := authority.executeMaintenance(t.Context(), copyOfAuthorization)
	require.ErrorIs(t, stale.Err(), ErrInvalidMaintenanceCoordinator)
	assert.Equal(t, int32(1), calls.Load(), "copied authority cannot invoke twice")
	record, found, err := store.LookupMaintenanceCommand(maintenanceLease, mustMaintenanceID(t, maintenanceIDA))
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, MaintenanceOutcomeAccepted, record.Outcome())
}

func TestMaintenanceExecutePanicIsAmbiguousAndPreservesPending(t *testing.T) {
	client := &executionTestBackend{name: "backend-a", restart: func(context.Context, backend.RestartRequest) error {
		panic("physical panic")
	}}
	authority, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), client)
	claim, authorization := admitMaintenanceForTest(t, authority)
	completion := authority.executeMaintenance(t.Context(), authorization)
	assert.False(t, completion.Settled())
	require.ErrorContains(t, completion.CallErr(), "panicked")
	record, found, err := store.LookupMaintenanceCommand(maintenanceLease, claim.Command().ID())
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, MaintenanceOutcomePending, record.Outcome())
}

func TestMaintenanceReauthorizationRequiresPositiveTerminalEvidence(t *testing.T) {
	client := &executionTestBackend{name: "backend-a"}
	authority, _ := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), client)
	claim, _ := admitMaintenanceForTest(t, authority)
	readers := map[string]maintenanceLeaseReaderFunc{
		"missing": func(context.Context, string) (*billingtypes.Lease, error) { return nil, nil },
		"unknown": func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
				State: billingtypes.LeaseState(127),
			}, nil
		},
	}
	for name, reader := range readers {
		t.Run(name, func(t *testing.T) {
			setProviderControlPlaneForTest(t, authority.coordinator.execution, reader)
			result := authority.reauthorizeMaintenanceCommand(t.Context(), claim)
			require.Error(t, result.Err())
			assert.False(t, result.Settled())
		})
	}
}

func TestMaintenancePreparationCannotCrossCoordinatorWithSameStore(t *testing.T) {
	client := &executionTestBackend{name: "backend-a"}
	first, _ := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), client)
	second, err := first.coordinator.execution.MaintenanceCoordinator(nil)
	require.NoError(t, err)
	preparation := first.prepareMaintenanceCommand(
		t.Context(), mustMaintenanceID(t, maintenanceIDA), maintenanceLease,
		"tenant-test", MaintenanceCommandRestart, nil,
	)
	_, err = second.beginMaintenanceCommand(preparation)
	require.ErrorIs(t, err, ErrInvalidMaintenanceCoordinator)
}

func TestMaintenanceLegacyErrorTreeCannotManufactureDefinitiveRefusal(t *testing.T) {
	client := &executionTestBackend{name: "backend-a", restart: func(context.Context, backend.RestartRequest) error {
		return errors.Join(errors.New("envelope"), backend.ErrValidation)
	}}
	authority, store := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), client)
	claim, authorization := admitMaintenanceForTest(t, authority)
	completion := authority.executeMaintenance(t.Context(), authorization)
	assert.False(t, completion.Settled())
	assert.ErrorIs(t, completion.CallErr(), backend.ErrValidation)
	record, found, err := store.LookupMaintenanceCommand(maintenanceLease, claim.Command().ID())
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, MaintenanceOutcomePending, record.Outcome())
}

func TestMaintenanceHTTPRefusalIsTransportClassified(t *testing.T) {
	client := causalOutcomeHTTPClient(
		t, http.StatusBadRequest, `{"error":"invalid request"}`,
	)
	authority, _ := newMaintenanceCoordinatorForTest(t, maintenanceActiveLeaseReader(), client)
	_, authorization := admitMaintenanceForTest(t, authority)
	completion := authority.executeMaintenance(t.Context(), authorization)
	require.True(t, completion.Settled(), completion.Err())
	assert.Equal(t, MaintenanceOutcomeValidationRejected, completion.Outcome())
}

func mustMaintenanceID(t testing.TB, raw string) maintenanceid.ID {
	t.Helper()
	id, err := maintenanceid.Parse(raw)
	require.NoError(t, err)
	return id
}
