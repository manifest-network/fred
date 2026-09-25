package shared

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMaintenanceHandoffCannotConvertToClaimedWorker(t *testing.T) {
	handoff := reflect.TypeFor[MaintenanceWorkerHandoff]()
	worker := reflect.TypeFor[MaintenanceWorkerLifetime]()
	require.False(t, handoff.ConvertibleTo(worker), "Go conversion must not bypass the one-shot ownership transfer")
	require.False(t, worker.ConvertibleTo(handoff), "a claimed owner must not recreate a handoff")
}

func testMaintenanceHandoff(t *testing.T, shutdown context.Context) MaintenanceWorkerHandoff {
	t.Helper()
	handoff, cancel := NewMaintenanceWorkerHandoff(shutdown, 10*time.Minute)
	t.Cleanup(cancel)
	return handoff
}

func testMaintenanceLifetime(t *testing.T, shutdown context.Context) MaintenanceWorkerLifetime {
	t.Helper()
	lifetime, err := testMaintenanceHandoff(t, shutdown).ClaimWorker()
	require.NoError(t, err)
	t.Cleanup(lifetime.Cancel)
	return lifetime
}
