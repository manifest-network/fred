//go:build integration

package chain

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/test/e2e"
)

// TEMPORARY, not for merge. These measure how long a 0.14 backend's start is
// held by the health check of a workload it puts back after an interrupted
// restart. The gate is a file in the lease's volume, which the test creates
// on the host a minute after the backend is started again.
func TestExperiment_StartAfterKilledRestart(t *testing.T) {
	experimentStartAfterInterruptedRestart(t, func(b *e2e.Backend) { b.Kill() })
}

func TestExperiment_StartAfterStoppedRestart(t *testing.T) {
	experimentStartAfterInterruptedRestart(t, func(b *e2e.Backend) { b.Stop() })
}

func experimentStartAfterInterruptedRestart(t *testing.T, interrupt func(*e2e.Backend)) {
	const hold = time.Minute
	p := StartProviderWith(t, 1, heldSettings(nil))
	backend, tenant := p.Backends[0], p.NewTenant(t)
	manifest := map[string]any{
		"image": e2e.RedisImage, "command": []string{"sleep", "3600"},
		"health_check": map[string]any{"test": []string{"CMD", "test", "-f", "/data/gate"}, "interval": "1s", "start_period": "1h"},
	}
	lease := tenant.Deploy(t, p.SKUs.Stateful, manifest)
	first := requireHeld(t, backend, lease, "provisioning")
	gate := filepath.Join(dataDir(t, first), "gate")
	require.NoError(t, os.WriteFile(gate, nil, 0o644))
	requireActive(t, p.Chain, lease)
	requireStatus(t, backend, lease, "ready")
	describeOnFailure(t, backend, lease)

	require.NoError(t, os.Remove(gate))
	tenant.Restart(t, lease)
	requireHeld(t, backend, lease, "restarting")

	stopping := time.Now()
	interrupt(backend)
	t.Logf("EXPERIMENT: the backend was down %s after it was interrupted", time.Since(stopping).Round(time.Millisecond))

	launched := time.Now()
	opened := make(chan time.Time, 1)
	go func() {
		time.Sleep(hold)
		_ = os.WriteFile(gate, nil, 0o644)
		opened <- time.Now()
	}()
	backend.Start()
	served := time.Now()
	gateOpened := <-opened
	t.Logf("EXPERIMENT: launched %s; gate opened after %s; served after %s",
		launched.Format(time.RFC3339), gateOpened.Sub(launched).Round(time.Millisecond), served.Sub(launched).Round(time.Millisecond))
	require.True(t, served.After(gateOpened), "EXPERIMENT: the backend served %s after launch, before the gate opened at %s: its start was not held",
		served.Sub(launched).Round(time.Millisecond), gateOpened.Sub(launched).Round(time.Millisecond))
	requireStatus(t, backend, lease, "ready")
}
