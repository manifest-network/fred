package shared

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestRecoveryCoordinator(
	t *testing.T,
	operations *OperationSettlement,
	maintenance *MaintenanceSettlement,
	close *CloseSettlement,
) *RecoveryCoordinator {
	t.Helper()
	config := RecoveryCoordinatorConfig{
		Operations: operations, Maintenance: maintenance, Close: close,
		ExcludeLease: func(_ context.Context, _ string, run func() error) (bool, error) {
			return true, run()
		},
	}
	if close != nil {
		config.ValidateActorClose = func(ActorCloseScope, RecoveryLineage) (string, bool) {
			return "", false
		}
	}
	coordinator, err := NewRecoveryCoordinator(config)
	require.NoError(t, err)
	return coordinator
}
