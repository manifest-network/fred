package shared

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPhysicalEvidenceZeroValuesFailClosed(t *testing.T) {
	t.Run("operation", func(t *testing.T) {
		require.Error(t, validateOperationPhysicalEvidence(
			OperationPhysicalSubject{}, OperationPhysicalEvidence{},
		))
	})
	t.Run("maintenance", func(t *testing.T) {
		require.Error(t, validateMaintenancePhysicalEvidence(
			MaintenancePhysicalSubject{}, MaintenancePhysicalEvidence{},
		))
	})
	t.Run("close", func(t *testing.T) {
		require.Error(t, validateClosePhysicalEvidence(
			ClosePhysicalSubject{}, ClosePhysicalEvidence{},
		))
	})
}

func TestZeroPhysicalEvidenceCannotMintTerminalAuthority(t *testing.T) {
	t.Run("operation", func(t *testing.T) {
		stores := openOperationHandoffStores(t, "zero-operation-evidence")
		claim := beginHandoffOperation(t, stores.settlement,
			testOperationIntentSpec(t, "zero-operation-evidence"))
		candidate, err := stores.settlement.PrepareOperationRelease(claim)
		require.NoError(t, err)
		bindTestOperationMutation(t, stores.settlement,
			func(OperationPhysicalSubject) (OperationPhysicalEvidence, error) {
				return OperationPhysicalEvidence{}, nil
			})
		execution, err := stores.settlement.StartOperationExecution(candidate)
		require.NoError(t, err)
		require.IsType(t, OperationExecutionAmbiguous{},
			stores.settlement.ExecuteOperation(context.Background(), execution))
	})

	t.Run("maintenance", func(t *testing.T) {
		fixture := beginBoundMaintenance(t, "zero-maintenance-evidence")
		target := fixture.appendAndBind(t)
		bindTestMaintenanceMutation(t, fixture.settlement,
			func(MaintenancePhysicalSubject) (MaintenancePhysicalEvidence, error) {
				return MaintenancePhysicalEvidence{}, nil
			})
		execution, err := fixture.settlement.StartMaintenanceExecution(target)
		require.NoError(t, err)
		require.IsType(t, MaintenanceExecutionAmbiguous{},
			fixture.settlement.ExecuteMaintenance(context.Background(), execution))
	})

	t.Run("close", func(t *testing.T) {
		stores := openOperationHandoffStores(t, "zero-close-evidence")
		settlement := newCloseSettlementForTest(t, stores)
		bindTestCloseMutation(t, settlement, nil,
			func(ClosePhysicalSubject) (ClosePhysicalEvidence, error) {
				return ClosePhysicalEvidence{}, nil
			})
		spec := seedCloseSettlementRelease(t, stores, "zero-close-evidence")
		claim := admitSettlementClose(t, settlement, spec.LeaseUUID, false)
		execution := startTestCloseExecution(t, settlement, claim)
		require.IsType(t, CloseExecutionPending{},
			settlement.ExecuteClose(context.Background(), execution))
	})
}
