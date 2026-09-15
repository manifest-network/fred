package docker

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestRoutingExcludesUnaccountedDockerFootprintWithoutFabricatingLoad(t *testing.T) {
	idle := newBackendForTest(&mockDockerClient{}, nil)
	idle.cfg.Name = "idle"
	t.Cleanup(idle.stopCancel)
	healthy := newBackendForTest(&mockDockerClient{}, nil)
	healthy.cfg.Name = "healthy"
	t.Cleanup(healthy.stopCancel)
	require.NoError(t, healthy.pool.TryAllocate("existing", "docker-micro", "tenant"))

	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{
		{Backend: idle, Match: backend.MatchCriteria{SKUs: []string{"docker-micro"}}, IsDefault: true},
		{Backend: healthy, Match: backend.MatchCriteria{SKUs: []string{"docker-micro"}}},
	}})
	require.NoError(t, err)
	require.Same(t, idle, router.RouteForProvision(t.Context(), "docker-micro", nil),
		"the empty ledger is genuinely least loaded before exclusion")

	closedHold := idle.pool.HoldUnaccountedFootprint()
	failedHold := idle.pool.HoldUnaccountedFootprint()
	for range 3 {
		load, loadErr := idle.GetLoadStats(t.Context())
		require.ErrorIs(t, loadErr, shared.ErrResourceAccountingIncomplete)
		require.Nil(t, load)
		require.Same(t, healthy, router.RouteForProvision(t.Context(), "docker-micro", nil),
			"a low known allocation must not attract work while the actual footprint is unknown")
	}
	known := idle.Stats()
	require.Zero(t, known.AllocatedCPU, "diagnostics must retain the exact known ledger")
	require.Zero(t, known.AllocationCount)
	require.True(t, known.AccountingHeld)

	closedHold.Release()
	require.Same(t, healthy, router.RouteForProvision(t.Context(), "docker-micro", nil),
		"one receipt family cannot make another family's held backend routable")
	failedHold.Release()
	require.Same(t, idle, router.RouteForProvision(t.Context(), "docker-micro", nil),
		"complete accounting restores routing without a restart")
}
