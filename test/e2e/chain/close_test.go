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

// TestClose_LeaseTheProviderNeverPlacedKeepsItsData closes a lease that runs
// on a backend the provider has no record of sending it to. For a lease it
// deployed, the provider knows which backend to ask. For this one it knows
// nothing, so the close must still reach the backend that holds the lease,
// and that backend must keep the data as it does for any closed lease.
//
// No lease gets into that state through the provider. The test puts the
// workload on the backend itself and acknowledges the lease on the chain with
// the provider's key, as its operator could.
//
// A pass of the provider that found the workload on the backend would record
// where it is, and the close would go the ordinary way. So the test lets the
// pass at startup finish before the workload exists, puts the next one an
// hour away, and checks at the end that none has run in between.
func TestClose_LeaseTheProviderNeverPlacedKeepsItsData(t *testing.T) {
	p := StartProviderWith(t, 1, Options{ProviderSettings: map[string]any{"reconciliation_interval": "1h"}})
	backend, tenant := p.Backends[0], p.NewTenant(t)
	// The provider serves before its pass at startup is over.
	const passes = "fred_reconciler_runs_total"
	eventually(t, "providerd finishes its pass at startup", func() bool {
		return p.Metric(t, passes, `outcome="`+string(SweepComplete)+`"`) >= 1
	})
	passesBefore := p.Metric(t, passes)

	manifest := runs(e2e.RedisImage)
	// No manifest is uploaded, so the provider deploys nothing.
	lease := tenant.CreateLease(t, manifest, p.SKUs.Stateful+":1")
	describeOnFailure(t, backend, lease)
	require.NoError(t, backend.Client.Apply(t.Context(), e2e.Spec{
		LeaseUUID: lease, Tenant: tenant.Address(), SKU: p.SKUs.Stateful, Manifest: manifest,
	}))
	requireStatus(t, backend, lease, "ready")
	workload := running(t, backend, lease)
	backend.Exec(t, workload.ID, "sh", "-c", "echo kept > /data/sentinel")
	// The one directory on the backend's volume path that carries the
	// lease's name, under its live name and later under its retained one.
	volumeRoot := func() string {
		roots, err := filepath.Glob(filepath.Join(backend.VolumePath, "*"+lease+"*"))
		require.NoError(t, err)
		require.Len(t, roots, 1, "lease %s must hold exactly one volume directory", lease)
		return roots[0]
	}
	data, err := filepath.Rel(volumeRoot(), dataDir(t, workload))
	require.NoError(t, err)
	kept := inode(t, dataDir(t, workload))

	p.Chain.Tx(t, providerKey, "billing", "acknowledge-lease", lease)
	require.Equal(t, LeaseActive, p.Chain.Lease(t, lease).State)
	require.Zero(t, p.Dispatched(t, backend, "provision"), "the provider must not have deployed the lease itself")

	tenant.Close(t, lease)
	requireStatus(t, backend, lease, "retained")
	require.Equal(t, passesBefore, p.Metric(t, passes), "no pass of the provider may have seen the workload on the backend before the close reached it")
	require.Empty(t, backend.Containers(t, lease))
	retained := filepath.Join(volumeRoot(), data)
	require.Equal(t, kept, inode(t, retained), "the retained data must be the volume's own directory")
	sentinel, err := os.ReadFile(filepath.Join(retained, "sentinel"))
	require.NoError(t, err)
	require.Equal(t, "kept\n", string(sentinel))
	require.Equal(t, LeaseClosed, p.Chain.Lease(t, lease).State)
}
