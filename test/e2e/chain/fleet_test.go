//go:build integration

package chain

import (
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/test/e2e"
)

// The tests in this file run a provider with two backends. They pin what
// providerd must not do when it cannot be sure: destroy a lease on missing
// evidence, or act on a backend that does not hold the lease.
//
// Both backends use the one Docker daemon, and a lease's network is named
// after the lease alone. A backend therefore refuses a lease the other one
// already runs, so "the other backend runs nothing for the lease" is weak
// evidence that providerd sent it nothing. Where that matters, the tests
// also read providerd's own count of the requests it sent to each backend.

// TestFleet_UncertainEvidenceDestroysNothing covers the ways providerd can
// lack evidence about a lease: the chain does not know it, the chain does not
// answer, or the lease's backend does not answer. In each, nothing of the
// lease may be removed. The controls show that cleanup still happens once the
// evidence exists: a closed lease is removed when its backend returns, and a
// close that providerd missed while it was down is acted on when it starts.
func TestFleet_UncertainEvidenceDestroysNothing(t *testing.T) {
	p := StartProvider(t, 2)
	a, b := p.Backends[0], p.Backends[1]
	tenant := p.NewTenant(t)
	onA := deployActive(t, p, tenant, a.SKUs.Pinned, sleeper("3600"))
	onB := deployActive(t, p, tenant, b.SKUs.Pinned, sleeper("3600"))
	containerA, containerB := running(t, a, onA), running(t, b, onB)

	// A lease of this provider that the chain has never heard of, running on
	// a backend. The chain keeps every lease it ever had, so this is not a
	// closed lease, and providerd must leave it. The counter shows that
	// providerd asked the chain about it and got that answer.
	unknown := e2e.Spec{LeaseUUID: uuid.NewString(), Tenant: tenant.Address(), SKU: p.SKUs.Stateless, Manifest: sleeper("3600")}
	require.NoError(t, a.Client.Apply(t.Context(), unknown))
	requireStatus(t, a, unknown.LeaseUUID, "ready")
	containerUnknown := running(t, a, unknown.LeaseUUID)
	p.AwaitMetric(t, 2, "fred_reconciler_cleanup_skips_total", `pass="orphan"`, `reason="chain_unknown"`)
	requireUntouched(t, a, unknown.LeaseUUID, containerUnknown)

	// The chain stops answering. No call to the chain may be made until it
	// resumes: it would block.
	p.Chain.Pause(t)
	p.AwaitSweeps(t, 1, SweepFailed)
	requireUntouched(t, a, onA, containerA)
	requireUntouched(t, b, onB, containerB)
	requireUntouched(t, a, unknown.LeaseUUID, containerUnknown)
	p.Chain.Resume(t)
	p.AwaitSweeps(t, 1, SweepComplete)
	require.Equal(t, LeaseActive, p.Chain.Lease(t, onA).State)
	require.Equal(t, LeaseActive, p.Chain.Lease(t, onB).State)
	requireUntouched(t, a, onA, containerA)
	requireUntouched(t, b, onB, containerB)

	// A lease is closed while its backend is unreachable. Nothing can be
	// removed, and nothing of it may appear elsewhere. Once the backend is
	// back, the close is carried out there.
	b.Stop()
	tenant.Close(t, onB)
	p.AwaitSweeps(t, 2, SweepDegraded)
	requireUntouched(t, b, onB, containerB)
	require.Empty(t, a.Containers(t, onB))
	b.Start()
	requireGone(t, b, onB)

	// A lease is closed while providerd is down, so it never sees the event.
	// The chain reports the lease closed, which is the evidence cleanup needs.
	p.Stop()
	tenant.Close(t, onA)
	p.Start()
	requireGone(t, a, onA)

	// Through all of it, the lease the chain does not know kept running.
	p.AwaitSweeps(t, 2, SweepComplete)
	requireUntouched(t, a, unknown.LeaseUUID, containerUnknown)
}

// TestFleet_CloseAndRestoreReachTheOwningBackend checks that a close and a
// restore go to the backend that holds the lease's data, also when the other
// backend would be the better choice for a new lease.
func TestFleet_CloseAndRestoreReachTheOwningBackend(t *testing.T) {
	p := StartProvider(t, 2)
	tenant := p.NewTenant(t)
	// The image declares VOLUME /data, so this lease has data to retain.
	stateful := map[string]any{"image": e2e.RedisImage, "command": []string{"sleep", "3600"}}
	source := deployActive(t, p, tenant, p.SKUs.Stateful, stateful)
	holder := p.Host(t, source)
	other := p.Backends[1-slices.Index(p.Backends, holder)]
	e2e.Exec(t, running(t, holder, source).ID, "sh", "-c", "echo kept > /data/sentinel")

	// One more lease on each backend, to witness that a close touches only
	// its own lease.
	beside := deployActive(t, p, tenant, holder.SKUs.Pinned, sleeper("3600"))
	elsewhere := deployActive(t, p, tenant, other.SKUs.Pinned, sleeper("3600"))
	containerBeside, containerElsewhere := running(t, holder, beside), running(t, other, elsewhere)

	tenant.Close(t, source)
	requireStatus(t, holder, source, "retained")
	require.Empty(t, e2e.LeaseContainers(t, source))
	requireUntouched(t, holder, beside, containerBeside)
	requireUntouched(t, other, elsewhere, containerElsewhere)
	require.Zero(t, p.Dispatched(t, other, "deprovision"), "the close must not have been sent to the other backend")

	// Restore into a lease of a SKU both backends serve. The other backend is
	// made idle first, so load alone would send a new lease there: the
	// restore must still run where the data is.
	tenant.Close(t, elsewhere)
	requireGone(t, other, elsewhere)
	provisionsElsewhere := p.Dispatched(t, other, "provision")
	target := tenant.Restore(t, p.SKUs.Stateful, source, stateful)
	requireActive(t, p.Chain, target)
	require.Same(t, holder, p.Host(t, target), "the restore must run on the backend that held the data")
	require.Equal(t, "kept\n", e2e.Exec(t, running(t, holder, target).ID, "cat", "/data/sentinel"))
	requireAbsent(t, other, target)
	require.Zero(t, p.Dispatched(t, other, "restore"), "the restore must not have been sent to the other backend")
	require.Equal(t, provisionsElsewhere, p.Dispatched(t, other, "provision"), "the target must not have been provisioned on the other backend")
	requireUntouched(t, holder, beside, containerBeside)
}

// TestFleet_LostLeaseReturnsToItsBackend removes a lease from the backend it
// was placed on, behind providerd's back. providerd must run it again on that
// backend. The backend is kept the busier of the two, so choosing afresh by
// load would send the lease to the other one, where it would start without
// the data its own backend holds.
func TestFleet_LostLeaseReturnsToItsBackend(t *testing.T) {
	p := StartProvider(t, 2)
	tenant := p.NewTenant(t)
	lost := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	home := p.Host(t, lost)
	away := p.Backends[1-slices.Index(p.Backends, home)]
	deployActive(t, p, tenant, home.SKUs.Pinned, sleeper("3600"))
	deployActive(t, p, tenant, home.SKUs.Pinned, sleeper("3600"))
	first := running(t, home, lost)
	provisionsAway := p.Dispatched(t, away, "provision")

	// A backend answers a close with a deferral while earlier work on the
	// lease settles, and expects the same close again.
	eventually(t, "backend "+home.Name+" accepts the close of lease "+lost, func() bool {
		return home.Client.Close(t.Context(), lost) == nil
	})
	replaced(t, home, lost, first)
	require.Equal(t, provisionsAway, p.Dispatched(t, away, "provision"), "the lease must not have been sent to the other backend")
	requireAbsent(t, away, lost)
	require.Equal(t, LeaseActive, p.Chain.Lease(t, lost).State)
}

// TestFleet_BackendOutageLeavesTheOtherServing stops one backend. The lease
// on the other backend keeps running and can still be restarted and updated.
// The lease on the stopped backend is not started anywhere else, although
// the other backend serves its SKU, and is intact when its backend returns.
func TestFleet_BackendOutageLeavesTheOtherServing(t *testing.T) {
	p := StartProvider(t, 2)
	a, b := p.Backends[0], p.Backends[1]
	tenant := p.NewTenant(t)
	// The first lease makes a the busier backend, so the second, of a SKU
	// both serve, goes to b.
	served := deployActive(t, p, tenant, a.SKUs.Pinned, sleeper("3600"))
	stranded := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	require.Same(t, b, p.Host(t, stranded), "the second lease must have gone to the idle backend")
	containerStranded := running(t, b, stranded)
	provisionsOnA := p.Dispatched(t, a, "provision")

	b.Stop()
	p.AwaitSweeps(t, 2, SweepDegraded)
	requireAbsent(t, a, stranded)

	containerServed := running(t, a, served)
	tenant.Restart(t, served)
	restarted := replaced(t, a, served, containerServed)
	tenant.Update(t, served, sleeper("7200"))
	updated := replaced(t, a, served, restarted)
	require.Contains(t, updated.Args, "7200", "the update must run the new manifest")

	p.AwaitSweeps(t, 2, SweepDegraded)
	requireAbsent(t, a, stranded)
	require.Equal(t, provisionsOnA, p.Dispatched(t, a, "provision"), "nothing may have been provisioned on the remaining backend")
	b.Start()
	requireStatus(t, b, stranded, "ready")
	requireUntouched(t, b, stranded, containerStranded)
	require.Equal(t, LeaseActive, p.Chain.Lease(t, stranded).State)
	require.Equal(t, LeaseActive, p.Chain.Lease(t, served).State)
}

// TestFleet_ProviderdRestartKeepsRouting restarts providerd under leases on
// both backends. Afterwards nothing was started again, and work on each lease
// still reaches the backend that holds it.
func TestFleet_ProviderdRestartKeepsRouting(t *testing.T) {
	p := StartProvider(t, 2)
	a, b := p.Backends[0], p.Backends[1]
	tenant := p.NewTenant(t)
	// As above: the second lease, of a SKU both backends serve, goes to b.
	// Only providerd's record says so afterwards.
	onA := deployActive(t, p, tenant, a.SKUs.Pinned, sleeper("3600"))
	onB := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	require.Same(t, b, p.Host(t, onB), "the second lease must have gone to the idle backend")
	containerA, containerB := running(t, a, onA), running(t, b, onB)

	// The counts below start again from zero with the new process.
	p.Stop()
	p.Start()
	p.AwaitSweeps(t, 2, SweepComplete)
	requireUntouched(t, a, onA, containerA)
	requireUntouched(t, b, onB, containerB)
	for _, backend := range p.Backends {
		require.Zero(t, p.Dispatched(t, backend, "provision"), "nothing may be provisioned again on %s", backend.Name)
	}

	tenant.Restart(t, onB)
	replaced(t, b, onB, containerB)
	require.Zero(t, p.Dispatched(t, a, "restart"), "the restart must not have been sent to the other backend")

	tenant.Close(t, onB)
	requireGone(t, b, onB)
	requireUntouched(t, a, onA, containerA)
	require.Zero(t, p.Dispatched(t, a, "deprovision"), "the close must not have been sent to the other backend")
	tenant.Close(t, onA)
	requireGone(t, a, onA)
}
