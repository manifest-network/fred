package docker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)

func testMaintenanceHandoff(t *testing.T, shutdown context.Context) shared.MaintenanceWorkerHandoff {
	t.Helper()
	handoff, cancel := shared.NewMaintenanceWorkerHandoff(shutdown, 10*time.Minute)
	t.Cleanup(cancel)
	return handoff
}

func testMaintenanceLifetime(t *testing.T, shutdown context.Context) shared.MaintenanceWorkerLifetime {
	t.Helper()
	lifetime, err := testMaintenanceHandoff(t, shutdown).ClaimWorker()
	require.NoError(t, err)
	t.Cleanup(lifetime.Cancel)
	return lifetime
}
