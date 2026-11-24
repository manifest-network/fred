//go:build integration

package chain

import (
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/test/e2e"
)

// The tests in this file are about what the provider does when nobody asks
// it for anything. It compares the chain with its backends on every pass and
// puts right what it finds: a lease whose workload died is run again, as it
// was (G18, G19), and a lease that ended while the provider was not looking
// is removed from its backend.

// eventsOnly are the settings of a backend that learns of a container's
// death from the daemon's events alone. Its own periodic pass would find a
// dead container too, and a death found that way is never counted against
// the lease: the backend cannot tell what ended the container.
var eventsOnly = Options{BackendSettings: map[string]any{"reconcile_interval": "1h"}}

// reported reads a lease from its backend.
func reported(t *testing.T, backend *e2e.Backend, leaseUUID string) e2e.Lease {
	t.Helper()
	lease, err := backend.Client.Get(t.Context(), leaseUUID)
	require.NoError(t, err)
	return lease
}

// requireStaysActive waits out two complete passes of the provider and
// checks that the lease is still active on the chain and still running in
// the given container: the provider found nothing to put right.
func requireStaysActive(t *testing.T, p *Provider, backend *e2e.Backend, leaseUUID string, workload container.InspectResponse) {
	t.Helper()
	p.AwaitSweeps(t, 2, SweepComplete)
	requireUntouched(t, backend, leaseUUID, workload)
	onChain := p.Chain.Lease(t, leaseUUID)
	require.Equal(t, LeaseActive, onChain.State, "the provider closed the lease: %q", onChain.ClosureReason)
}

// TestReconcile_DeadWorkloadIsRunAgain ends an active lease's container
// twice: it is killed, and then removed outright, as a host that lost it
// would have none. Each time the provider must run the lease again in a new
// container and keep it active.
func TestReconcile_DeadWorkloadIsRunAgain(t *testing.T) {
	p := StartProvider(t, 1)
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, runs(e2e.BusyboxImage))
	describeOnFailure(t, backend, lease)
	first := running(t, backend, lease)

	backend.KillContainers(t, lease)
	second := replaced(t, backend, lease, first)
	requireImage(t, second, e2e.BusyboxImage)
	requireReachable(t, tenant, backend, lease)

	backend.LoseContainers(t, lease)
	third := replaced(t, backend, lease, second)
	require.NotEqual(t, first.ID, third.ID)
	requireReachable(t, tenant, backend, lease)
	requireStaysActive(t, p, backend, lease, third)
}

// TestReconcile_CrashLoopIsCountedAndRunAgain runs a workload that exits by
// itself twenty seconds after every start, long enough for the provider to
// have acknowledged the lease before the first exit. Each exit counts
// towards the failures that can close a lease (ENG-799). Three of them
// within a few minutes are not enough: a lease is closed only for failures
// that span half an hour. So the provider must run the lease again every
// time, and the lease must stay active.
func TestReconcile_CrashLoopIsCountedAndRunAgain(t *testing.T) {
	p := StartProviderWith(t, 1, eventsOnly)
	backend, tenant := p.Backends[0], p.NewTenant(t)
	deployed := time.Now()
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, map[string]any{
		"image": e2e.BusyboxImage, "command": []string{"sh", "-c", "sleep 20; exit 3"},
	})
	describeOnFailure(t, backend, lease)

	// Every container of the lease exits by itself, and the provider starts
	// the next one only once the backend has reported the lease failed. So
	// at any moment the failures counted are one for each container started,
	// but for the last one while it still runs. The containers are counted
	// from the daemon's events, not from what the backend says, and read
	// first: a container cannot start and exit between the two reads.
	const crashes = 3
	starts := 0
	eventually(t, "lease "+lease+" has crashed and been run again three times", func() bool {
		starts = len(backend.Started(t, lease, deployed))
		counted := reported(t, backend, lease)
		require.GreaterOrEqual(t, counted.ConsecutiveFailures, starts-1, "an exit was not counted: %d containers were started", starts)
		require.LessOrEqual(t, counted.ConsecutiveFailures, starts, "an exit was counted twice: %d containers were started", starts)
		require.Equal(t, e2e.VerdictRetry, counted.BudgetVerdict, "%d failures are within the first half hour", counted.ConsecutiveFailures)
		return starts > crashes
	})
	require.Equal(t, crashes+1, starts, "one container for the deploy, and one for each time the lease was run again")
	require.Equal(t, crashes, reported(t, backend, lease).ConsecutiveFailures, "each exit counts once")
	onChain := p.Chain.Lease(t, lease)
	require.Equal(t, LeaseActive, onChain.State, "a young crash loop must not close the lease: %q", onChain.ClosureReason)
}

// TestReconcile_KillsFromOutsideAreNeverCounted kills an active lease's
// container three times from outside, and then removes it. A workload that
// somebody else killed has not failed, so none of the kills may count against
// the lease, however many there are. The provider runs it again each time.
func TestReconcile_KillsFromOutsideAreNeverCounted(t *testing.T) {
	p := StartProviderWith(t, 1, eventsOnly)
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, runs(e2e.BusyboxImage))
	describeOnFailure(t, backend, lease)
	workload := running(t, backend, lease)

	for kill := 1; kill <= 3; kill++ {
		backend.KillContainers(t, lease)
		workload = replaced(t, backend, lease, workload)
		after := reported(t, backend, lease)
		require.Zero(t, after.ConsecutiveFailures, "kill %d must not count", kill)
		require.Equal(t, e2e.VerdictRetry, after.BudgetVerdict, "after kill %d the lease may be run again", kill)
	}

	// A container removed outright leaves the same trace, an event, and no
	// container to look at afterwards. The backend's periodic pass is an
	// hour away, so the event alone must be enough to have the lease run
	// again.
	backend.LoseContainers(t, lease)
	workload = replaced(t, backend, lease, workload)
	requireStaysActive(t, p, backend, lease, workload)
}

// TestReconcile_LeaseThatLostOneInstanceRunsThemAllAgain kills one of the two
// containers of an active lease. The lease is whole or it has failed: the
// provider must bring it back to two running containers.
func TestReconcile_LeaseThatLostOneInstanceRunsThemAllAgain(t *testing.T) {
	p := StartProvider(t, 1)
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease := tenant.DeployItems(t, runs(e2e.BusyboxImage), p.SKUs.Stateless+":2")
	requireActive(t, p.Chain, lease)
	describeOnFailure(t, backend, lease)
	requireStatus(t, backend, lease, "ready")
	before := backend.Containers(t, lease)
	require.Len(t, before, 2)

	backend.KillContainer(t, before[0].ID)
	eventually(t, "lease "+lease+" runs in two containers again", func() bool {
		now := backend.Containers(t, lease)
		if len(now) != 2 {
			return false
		}
		for _, one := range now {
			if one.ID == before[0].ID || !one.State.Running {
				return false
			}
		}
		return reported(t, backend, lease).Status == "ready"
	})
	require.Equal(t, LeaseActive, p.Chain.Lease(t, lease).State)
}

// TestReconcile_LeaseClosedWhileTheProviderWasDownIsRemoved closes a lease on
// the chain while providerd is not running, so providerd never sees the
// chain's event. When it starts it finds a workload whose lease is closed,
// and must have the backend remove it.
func TestReconcile_LeaseClosedWhileTheProviderWasDownIsRemoved(t *testing.T) {
	p := StartProvider(t, 1)
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, runs(e2e.BusyboxImage))
	workload := running(t, backend, lease)

	p.Stop()
	tenant.Close(t, lease)
	require.Equal(t, LeaseClosed, p.Chain.Lease(t, lease).State)
	requireUntouched(t, backend, lease, workload)

	p.Start()
	requireGone(t, backend, lease)
	eventually(t, "backend "+backend.Name+" holds nothing of lease "+lease, func() bool {
		return len(backend.Leftovers(lease)) == 0
	})
}

// TestReconcile_DataOfALeaseClosedWhileTheProviderWasDownIsRestorable does
// the same with a lease that has a volume. The backend must keep the data as
// it does for a close the provider saw, the tenant must be able to read from
// the provider that it is kept, and a restore must bring it back.
func TestReconcile_DataOfALeaseClosedWhileTheProviderWasDownIsRestorable(t *testing.T) {
	p := StartProvider(t, 1)
	backend, tenant := p.Backends[0], p.NewTenant(t)
	manifest := runs(e2e.RedisImage)
	source := deployActive(t, p, tenant, p.SKUs.Stateful, manifest)
	workload := running(t, backend, source)
	backend.Exec(t, workload.ID, "sh", "-c", "echo kept > /data/sentinel")
	kept := inode(t, dataDir(t, workload))

	p.Stop()
	tenant.Close(t, source)
	p.Start()
	requireStatus(t, backend, source, "retained")
	require.Empty(t, backend.Containers(t, source))
	// What was leased is told with it: a tenant builds the lease it restores
	// into from that.
	var provision struct {
		Status string `json:"status"`
		Items  []struct {
			SKU      string `json:"sku"`
			Quantity int    `json:"quantity"`
		} `json:"items"`
	}
	eventually(t, "the provider tells the tenant that lease "+source+" is retained", func() bool {
		return tenant.Read(t, source, "provision", &provision) == 200 && provision.Status == "retained"
	})
	require.Len(t, provision.Items, 1, "the items of the retained lease: %+v", provision)
	require.Equal(t, p.SKUs.Stateful, provision.Items[0].SKU)
	require.Equal(t, 1, provision.Items[0].Quantity)

	target := tenant.Restore(t, p.SKUs.Stateful, source, manifest)
	requireActive(t, p.Chain, target)
	requireStatus(t, backend, target, "ready")
	restored := running(t, backend, target)
	require.Equal(t, "kept\n", backend.Exec(t, restored.ID, "cat", "/data/sentinel"))
	require.Equal(t, kept, inode(t, dataDir(t, restored)), "the restored volume must be the source's own directory")
	requireGone(t, backend, source)
}

// TestReconcile_RestoredLeaseIsRunAgainOnItsData ends the container of a
// lease that was restored. The tenant never uploaded a manifest for that
// lease: the backend ran the source's. The provider must still be able to
// run the lease again, on the same data (ENG-1442).
func TestReconcile_RestoredLeaseIsRunAgainOnItsData(t *testing.T) {
	p := StartProvider(t, 1)
	backend, tenant := p.Backends[0], p.NewTenant(t)
	manifest := runs(e2e.RedisImage)
	source := deployActive(t, p, tenant, p.SKUs.Stateful, manifest)
	backend.Exec(t, running(t, backend, source).ID, "sh", "-c", "echo kept > /data/sentinel")
	tenant.Close(t, source)
	requireStatus(t, backend, source, "retained")

	target := tenant.Restore(t, p.SKUs.Stateful, source, manifest)
	requireActive(t, p.Chain, target)
	describeOnFailure(t, backend, target)
	requireStatus(t, backend, target, "ready")
	restored := running(t, backend, target)
	kept := inode(t, dataDir(t, restored))
	// A pass of the provider with the restored lease healthy, as it is for
	// most of its life.
	p.AwaitSweeps(t, 2, SweepComplete)

	backend.LoseContainers(t, target)
	again := replaced(t, backend, target, restored)
	requireImage(t, again, e2e.RedisImage)
	require.Equal(t, "kept\n", backend.Exec(t, again.ID, "cat", "/data/sentinel"))
	require.Equal(t, kept, inode(t, dataDir(t, again)), "the lease must get its own volume back")
	requireStaysActive(t, p, backend, target, again)
}

// TestReconcile_UpdatedLeaseIsRunAgainOnItsNewImage ends the container of a
// lease its tenant has updated, after providerd has restarted. The lease
// must come back on the image of the update, not on the one it was created
// with (ENG-619). The chain still records the hash of the first manifest,
// and the provider must not take the difference for a corrupt manifest and
// close the lease.
func TestReconcile_UpdatedLeaseIsRunAgainOnItsNewImage(t *testing.T) {
	p := StartProvider(t, 1)
	backend, tenant := p.Backends[0], p.NewTenant(t)
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, runs(e2e.BusyboxImage))
	describeOnFailure(t, backend, lease)
	created := running(t, backend, lease)

	tenant.Update(t, lease, runs(e2e.RedisImage))
	updated := replaced(t, backend, lease, created)
	requireImage(t, updated, e2e.RedisImage)

	// What providerd runs a lease from must have outlived the process.
	p.Stop()
	p.Start()
	p.AwaitSweeps(t, 2, SweepComplete)
	requireUntouched(t, backend, lease, updated)

	backend.LoseContainers(t, lease)
	again := replaced(t, backend, lease, updated)
	requireImage(t, again, e2e.RedisImage)
	requireStaysActive(t, p, backend, lease, again)
}
