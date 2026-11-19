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
	onA := deployActive(t, p, tenant, p.Pinned[0], sleeper("3600"))
	onB := deployActive(t, p, tenant, p.Pinned[1], sleeper("3600"))
	containerA, containerB := running(t, a, onA), running(t, b, onB)

	// A lease the chain has never heard of, running on a backend. The chain
	// keeps every lease it ever had, so this is not a closed lease, and
	// providerd must leave it.
	unknown := e2e.Spec{LeaseUUID: uuid.NewString(), Tenant: tenant.Address(), SKU: p.SKUs.Stateless, Manifest: sleeper("3600")}
	require.NoError(t, a.Client.Apply(t.Context(), unknown))
	requireStatus(t, a, unknown.LeaseUUID, "ready")
	containerUnknown := running(t, a, unknown.LeaseUUID)
	p.AwaitMetric(t, 2, "fred_reconciler_cleanup_skips_total", `pass="orphan"`, `reason="chain_unknown"`)
	requireUntouched(t, a, unknown.LeaseUUID, containerUnknown)

	// The chain stops answering. No call to the chain may be made until it
	// resumes: it would block.
	failedSweeps := func() float64 {
		return p.Metric(t, "fred_reconciler_runs_total") - p.Metric(t, "fred_reconciler_runs_total", `outcome="success"`)
	}
	p.Chain.Pause(t)
	failedBefore := failedSweeps()
	eventually(t, "a reconciliation pass fails while the chain is paused", func() bool { return failedSweeps() > failedBefore })
	requireUntouched(t, a, onA, containerA)
	requireUntouched(t, b, onB, containerB)
	requireUntouched(t, a, unknown.LeaseUUID, containerUnknown)
	p.Chain.Resume(t)
	p.AwaitMetric(t, 1, "fred_reconciler_runs_total", `outcome="success"`)
	require.Equal(t, LeaseActive, p.Chain.Lease(t, onA).State)
	require.Equal(t, LeaseActive, p.Chain.Lease(t, onB).State)
	requireUntouched(t, a, onA, containerA)
	requireUntouched(t, b, onB, containerB)

	// A lease is closed while its backend is unreachable. Nothing can be
	// removed, and nothing of it may appear elsewhere. Once the backend is
	// back, the close is carried out there.
	b.Stop()
	tenant.Close(t, onB)
	p.AwaitSweeps(t, 2)
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
	p.AwaitSweeps(t, 2)
	requireUntouched(t, a, unknown.LeaseUUID, containerUnknown)
}

// TestFleet_CloseAndRestoreReachTheOwningBackend checks that work on a lease
// goes to the backend that holds it, also when another backend would be the
// better choice for a new lease, and that a disagreement between backends
// about who holds a lease destroys nothing.
func TestFleet_CloseAndRestoreReachTheOwningBackend(t *testing.T) {
	p := StartProvider(t, 2)
	tenant := p.NewTenant(t)
	// The image declares VOLUME /data, so this lease has data to retain.
	stateful := map[string]any{"image": e2e.RedisImage, "command": []string{"sleep", "3600"}}
	source := deployActive(t, p, tenant, p.SKUs.Stateful, stateful)
	holder := p.Host(t, source)
	holderIndex := slices.Index(p.Backends, holder)
	other := p.Backends[1-holderIndex]
	e2e.Exec(t, running(t, holder, source).ID, "sh", "-c", "echo kept > /data/sentinel")

	// One more lease on each backend, to witness that a close touches only
	// its own lease.
	beside := deployActive(t, p, tenant, p.Pinned[holderIndex], sleeper("3600"))
	elsewhere := deployActive(t, p, tenant, p.Pinned[1-holderIndex], sleeper("3600"))
	containerBeside, containerElsewhere := running(t, holder, beside), running(t, other, elsewhere)

	tenant.Close(t, source)
	requireStatus(t, holder, source, "retained")
	require.Empty(t, e2e.LeaseContainers(t, source))
	requireUntouched(t, holder, beside, containerBeside)
	requireUntouched(t, other, elsewhere, containerElsewhere)

	// Restore into a lease of a SKU both backends serve. The other backend is
	// made idle first, so load alone would send a new lease there: the
	// restore must still run where the data is.
	tenant.Close(t, elsewhere)
	requireGone(t, other, elsewhere)
	target := tenant.Restore(t, p.SKUs.Stateful, source, stateful)
	requireActive(t, p.Chain, target)
	require.Same(t, holder, p.Host(t, target), "the restore must run on the backend that held the data")
	require.Equal(t, "kept\n", e2e.Exec(t, running(t, holder, target).ID, "cat", "/data/sentinel"))
	requireAbsent(t, other, target)

	// A lease disappears from the backend it was placed on. providerd runs it
	// again on that backend and never on another one, where it would start
	// without the lease's data.
	moved := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	home := p.Host(t, moved)
	away := p.Backends[1-slices.Index(p.Backends, home)]
	first := running(t, home, moved)
	// A backend answers a close with a deferral while earlier work on the
	// lease settles, and expects the same close again.
	eventually(t, "backend "+home.Name+" accepts the close of lease "+moved, func() bool {
		return home.Client.Close(t.Context(), moved) == nil
	})
	replaced(t, home, moved, first)
	requireAbsent(t, away, moved)
	require.Equal(t, LeaseActive, p.Chain.Lease(t, moved).State)

	// Two backends report the same lease. providerd cannot tell which copy
	// is the lease, so it leaves both alone.
	require.NoError(t, other.Client.Apply(t.Context(), e2e.Spec{
		LeaseUUID: beside, Tenant: tenant.Address(), SKU: p.SKUs.Stateless, Manifest: sleeper("3600"),
	}))
	requireStatus(t, other, beside, "ready")
	duplicate := running(t, other, beside)
	p.AwaitMetric(t, 2, "fred_provisioner_reconciler_deferred_leases_total")
	p.AwaitSweeps(t, 2)
	requireUntouched(t, holder, beside, containerBeside)
	requireUntouched(t, other, beside, duplicate)
	require.Equal(t, LeaseActive, p.Chain.Lease(t, beside).State)
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
	served := deployActive(t, p, tenant, p.Pinned[0], sleeper("3600"))
	stranded := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	require.Same(t, b, p.Host(t, stranded), "the second lease must have gone to the idle backend")
	containerStranded := running(t, b, stranded)

	b.Stop()
	p.AwaitMetric(t, 2, "fred_reconciler_runs_total", `outcome="degraded"`)
	requireAbsent(t, a, stranded)

	containerServed := running(t, a, served)
	tenant.Restart(t, served)
	restarted := replaced(t, a, served, containerServed)
	tenant.Update(t, served, sleeper("7200"))
	updated := replaced(t, a, served, restarted)
	require.Contains(t, updated.Args, "7200", "the update must run the new manifest")

	p.AwaitSweeps(t, 2)
	requireAbsent(t, a, stranded)
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
	onA := deployActive(t, p, tenant, p.Pinned[0], sleeper("3600"))
	onB := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	require.Same(t, b, p.Host(t, onB), "the second lease must have gone to the idle backend")
	containerA, containerB := running(t, a, onA), running(t, b, onB)

	p.Stop()
	p.Start()
	p.AwaitSweeps(t, 2)
	requireUntouched(t, a, onA, containerA)
	requireUntouched(t, b, onB, containerB)

	tenant.Restart(t, onB)
	replaced(t, b, onB, containerB)
	requireAbsent(t, a, onB)

	tenant.Close(t, onB)
	requireGone(t, b, onB)
	requireUntouched(t, a, onA, containerA)
	tenant.Close(t, onA)
	requireGone(t, a, onA)
	require.Empty(t, e2e.LeaseContainers(t, onB))
}
