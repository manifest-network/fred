//go:build integration

package chain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The tests in this file pin that a close, once asked for, runs to its end.

// TestClose_OutlivesItsRequest closes a lease whose workload ignores SIGTERM,
// on a backend whose stop grace is longer than providerd waits for an answer.
// The workload must get its whole grace, the close must still complete, and
// the backend must go on being given work.
func TestClose_OutlivesItsRequest(t *testing.T) {
	// providerd gives a backend request 30 seconds.
	const grace = 40 * time.Second
	p := StartProviderWith(t, 1, Options{BackendSettings: map[string]any{"container_stop_timeout": grace.String()}})
	backend, tenant := p.Backends[0], p.NewTenant(t)
	// sleep runs as the container's first process, which gets no default
	// signal handling: SIGTERM does nothing, and the stop lasts its grace.
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	beside := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	containerBeside := running(t, backend, beside)

	asked := time.Now()
	tenant.Close(t, lease)
	requireGone(t, backend, lease)
	require.GreaterOrEqual(t, time.Since(asked), grace, "the workload must have been given its whole stop grace")
	requireNothingLeft(t, tenant, backend, lease)

	requireUntouched(t, backend, beside, containerBeside)
	after := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	running(t, backend, after)
}

// TestClose_DuringAStartedRestartOrUpdate closes one lease in the middle of a
// restart and another in the middle of an update, each held at "started, not
// healthy". Both closes must complete and leave nothing behind.
func TestClose_DuringAStartedRestartOrUpdate(t *testing.T) {
	p := StartProviderWith(t, 1, heldSettings(nil))
	backend, tenant := p.Backends[0], p.NewTenant(t)

	restarting, first := holdCreate(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	openGate(t, backend, first)
	requireSettled(t, p, backend, restarting, first)
	tenant.Restart(t, restarting)
	requireHeld(t, backend, restarting, "restarting")

	updating := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	tenant.Update(t, updating, gated(sleeper("7200")))
	requireHeld(t, backend, updating, "updating")

	requireNothingLeft(t, tenant, backend, restarting)
	requireNothingLeft(t, tenant, backend, updating)
	p.AwaitSweeps(t, 2, SweepComplete)
}
