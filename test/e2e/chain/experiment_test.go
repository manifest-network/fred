//go:build integration

package chain

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/test/e2e"
)

// EXPERIMENT ONLY, never to merge. These reproduce two 0.14 behaviours found
// while writing the crash tests, and log what happens.

// errorLines are the ERROR lines at the end of the backend's log.
func errorLines(backend *e2e.Backend) string {
	var lines []string
	for _, line := range strings.Split(backend.LogTail(), "\n") {
		if strings.Contains(line, "level=ERROR") {
			lines = append(lines, line)
		}
	}
	if len(lines) > 4 {
		lines = lines[len(lines)-4:]
	}
	return strings.Join(lines, "\n")
}

// Finding 2: interrupt the backend while a restart's target launch is being
// dispatched to Docker, then start it three times.
func TestExperiment_KillDuringDispatch(t *testing.T) {
	experimentInterruptDuringDispatch(t, "kill", func(b *e2e.Backend) { b.Kill() })
}

func TestExperiment_StopDuringDispatch(t *testing.T) {
	experimentInterruptDuringDispatch(t, "stop", func(b *e2e.Backend) { b.Stop() })
}

func experimentInterruptDuringDispatch(t *testing.T, label string, interrupt func(*e2e.Backend)) {
	p := StartProviderWith(t, 1, heldSettings(nil))
	backend, tenant := p.Backends[0], p.NewTenant(t)
	bystander := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	containerBystander := running(t, backend, bystander)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	first := running(t, backend, lease)
	describeOnFailure(t, backend, lease)

	asked := time.Now()
	tenant.Restart(t, lease)
	accepted := time.Now()
	// Interrupt as soon as Docker shows a container of the lease other than
	// the first: the launch is dispatched, and may not be recorded yet.
	seen := false
	for deadline := time.Now().Add(time.Minute); time.Now().Before(deadline) && !seen; {
		containers, err := backend.TryContainers(lease)
		if err != nil {
			continue
		}
		for _, one := range containers {
			seen = seen || one.ID != first.ID
		}
	}
	require.True(t, seen, "the restart never created a container")
	sawTarget := time.Now()
	interrupt(backend)
	t.Logf("EXPERIMENT %s: restart accepted after %s; target container seen %s later; interrupt took %s",
		label, accepted.Sub(asked).Round(time.Millisecond), sawTarget.Sub(accepted).Round(time.Millisecond), time.Since(sawTarget).Round(time.Millisecond))

	served := false
	for attempt := 1; attempt <= 3 && !served; attempt++ {
		launched := time.Now()
		backend.Launch()
		var exited bool
		var err error
		served, exited, err = backend.TryAwaitServing(45 * time.Second)
		t.Logf("EXPERIMENT %s: start %d after %s: served=%v exited=%v exit=%v", label, attempt, time.Since(launched).Round(time.Millisecond), served, exited, err)
		if !served {
			t.Logf("EXPERIMENT %s: start %d errors:\n%s", label, attempt, errorLines(backend))
		}
		if !served && !exited {
			backend.Kill()
		}
		if !served {
			time.Sleep(2 * time.Second)
		}
	}
	t.Logf("EXPERIMENT %s: backend serving at the end: %v", label, served)
	containers, err := backend.TryContainers(lease)
	t.Logf("EXPERIMENT %s: the restarted lease has %d containers (err %v)", label, len(containers), err)
	for _, one := range containers {
		health := "none"
		if one.State.Health != nil {
			health = one.State.Health.Status
		}
		t.Logf("EXPERIMENT %s:   %s %s health %s", label, one.ID[:12], one.State.Status, health)
	}
	requireUntouched(t, backend, bystander, containerBystander)
	if served {
		t.Logf("EXPERIMENT %s: pending launches %v", label, backend.Metric(t, "fred_docker_backend_volume_launches_pending"))
		reported, err := backend.Client.Get(t.Context(), lease)
		t.Logf("EXPERIMENT %s: the backend reports the lease %q (err %v)", label, reported.Status, err)
	}
}

// Finding 1: after an interrupted restart, hold the put-back workload's
// health check for two and a half minutes, longer than the two minutes a
// running backend gives the same step.
func TestExperiment_StartHeldBeyondTwoMinutes(t *testing.T) {
	const hold = 150 * time.Second
	p := StartProviderWith(t, 1, heldSettings(nil))
	backend, tenant := p.Backends[0], p.NewTenant(t)
	bystander := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
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
	backend.Stop()

	launched := time.Now()
	backend.Launch()
	served, exited, err := backend.TryAwaitServing(hold)
	t.Logf("EXPERIMENT held: %s after launch: served=%v exited=%v exit=%v", time.Since(launched).Round(time.Millisecond), served, exited, err)
	_, getErr := backend.Client.Get(t.Context(), bystander)
	t.Logf("EXPERIMENT held: asking the backend for the other lease while held: %v", getErr)
	require.NoError(t, os.WriteFile(gate, nil, 0o644))
	opened := time.Now()
	served, exited, err = backend.TryAwaitServing(time.Minute)
	t.Logf("EXPERIMENT held: %s after the gate opened: served=%v exited=%v exit=%v", time.Since(opened).Round(time.Millisecond), served, exited, err)
	if !served && !exited {
		backend.Kill()
	}
}
