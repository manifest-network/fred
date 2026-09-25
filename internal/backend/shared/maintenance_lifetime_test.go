package shared

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
