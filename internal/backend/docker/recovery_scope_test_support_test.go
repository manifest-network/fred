package docker

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

func bindBackendRecoveryCoordinatorForTest(t *testing.T, b *Backend) {
	t.Helper()
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok, "test backend requires a concrete operation settlement")
	if b.recoveryCoordinator != nil && b.recoveryCoordinator.BoundTo(
		operations, b.maintenanceSettlement, b.closeSettlement,
	) {
		return
	}
	coordinator, err := shared.NewRecoveryCoordinator(shared.RecoveryCoordinatorConfig{
		Operations: operations, Maintenance: b.maintenanceSettlement, Close: b.closeSettlement,
		ExcludeLease: b.withRecoveryLeaseExclusion, ValidateActorClose: b.validateActorCloseScope,
	})
	require.NoError(t, err)
	b.recoveryCoordinator = coordinator
}

// doDeprovisionForTest exercises the substrate finalizer under the same exact
// lease exclusion and recovery scope as production, without manufacturing an
// actor-owned close scope in tests.
func (b *Backend) doDeprovisionForTest(
	t *testing.T,
	ctx context.Context,
	leaseUUID string,
) error {
	t.Helper()
	bindBackendRecoveryCoordinatorForTest(t, b)
	acquired, err := b.recoveryCoordinator.WithLease(
		ctx, leaseUUID,
		func(scope shared.LeaseRecoveryScope) error {
			return b.doDeprovisionScoped(ctx, scope, leaseUUID)
		},
	)
	if err != nil {
		return err
	}
	if !acquired {
		return errors.New("test could not acquire lease recovery scope")
	}
	return nil
}
