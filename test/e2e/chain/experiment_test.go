//go:build integration

package chain

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/test/e2e"
)

// EXPERIMENT ONLY, never to merge. These reproduce two 0.14 behaviours found
// while writing the crash tests, and log what happens.

// errorLines are the last lines of the backend's log.
func errorLines(backend *e2e.Backend) string {
	lines := strings.Split(backend.LogTail(), "\n")
	if len(lines) > 12 {
		lines = lines[len(lines)-12:]
	}
	for i, line := range lines {
		if len(line) > 900 {
			lines[i] = line[:900]
		}
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
