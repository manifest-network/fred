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

func TestMaintenanceWorkerHandoffUsesConfiguredProvisionTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{37 * time.Second, 19 * time.Minute} {
		t.Run(timeout.String(), func(t *testing.T) {
			shutdown, stop := context.WithCancel(t.Context())
			defer stop()
			b := &Backend{cfg: Config{ProvisionTimeout: timeout}, stopCtx: shutdown}
			start := time.Now()
			handoff, discard := b.maintenanceWorkerHandoff()
			defer discard()
			deadline, bounded := handoff.TargetContext().Deadline()
			require.True(t, bounded)
			require.WithinDuration(t, start.Add(timeout), deadline, time.Second)
			stop()
			require.ErrorIs(t, handoff.TargetContext().Err(), context.Canceled)
		})
	}
}
