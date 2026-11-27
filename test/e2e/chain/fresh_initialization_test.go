//go:build integration

package chain

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/test/e2e"
)

// The test in this file pins when a placement database may be created from
// nothing. The database records which backend holds each lease. Creating an
// empty one for a provider that has served leases would strand them: the
// provider would no longer know where a lease runs or where its data is kept.

// TestFreshInitialization_RefusesAProviderThatHasServedLeases loses the
// placement database of a provider three times, each time in a different
// state, and asks placement-preflight for a new one as an operator would. It
// must refuse while a backend runs a lease, while the chain records a lease
// the provider once served, and while a backend keeps the data of a closed
// lease. A refusal must create nothing.
func TestFreshInitialization_RefusesAProviderThatHasServedLeases(t *testing.T) {
	p := StartProvider(t, 1)
	backend, tenant := p.Backends[0], p.NewTenant(t)

	live := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	requireFreshInitializationRefused(t, p, backend.Name, "is not empty (1 provisions, 0 retentions)")

	// The backend is empty again, and only the chain remembers the lease.
	requireNothingLeft(t, tenant, backend, live)
	requireFreshInitializationRefused(t, p, "provider has 1 leases in chain history")

	kept := deployActive(t, p, tenant, p.SKUs.Stateful, runs(e2e.RedisImage))
	tenant.Close(t, kept)
	requireStatus(t, backend, kept, "retained")
	requireFreshInitializationRefused(t, p, backend.Name, "is not empty (0 provisions, 1 retentions)")

	// The provider still runs on the database it had.
	after := deployActive(t, p, tenant, p.SKUs.Stateless, sleeper("3600"))
	running(t, backend, after)
}

// requireFreshInitializationRefused stops providerd, takes its placement
// database away, and asks for a new one. The tool must refuse, say each of
// the given parts of its reason, and create no database. The provider then
// gets its own database back and starts again.
func requireFreshInitializationRefused(t *testing.T, p *Provider, why ...string) {
	t.Helper()
	p.Stop()
	lost := p.PlacementDB + ".lost"
	require.NoError(t, os.Rename(p.PlacementDB, lost))
	out, err := p.InitializePlacements(t)
	_, created := os.Lstat(p.PlacementDB)
	require.NoError(t, os.Rename(lost, p.PlacementDB))
	p.Start()

	require.Error(t, err, "placement-preflight created a database for a provider that has served leases: %s", out)
	for _, part := range why {
		require.Contains(t, out, part)
	}
	require.ErrorIs(t, created, os.ErrNotExist, "a refused initialization creates no database")
}
