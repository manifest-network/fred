//go:build integration

package chain

import (
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/test/e2e"
)

// The tests in this file restore data from a lease that ended without ever
// becoming active. A workload can be running, and have written to its volume,
// while its lease is still pending: the provider acknowledges a lease only
// once the workload is healthy. If the lease ends first, because the tenant
// withdrew it or the chain expired it, the backend keeps the volume as it
// does for a closed lease, and the tenant must be able to restore it
// (ENG-2130). The other tests of this suite restore from closed leases.
//
// Each test also interrupts the restore: providerd's request to the backend
// is lost on the way, and providerd is killed. The restarted providerd finds
// a restore it recorded and cannot know the fate of, and reads the source
// lease again before it sends the restore a second time. So the tests cover
// both places where a source that was never active used to be turned away:
// when the restore is asked for, and when it is taken up again.
//
// It has to be the request that is lost. Once the backend reports the lease
// being restored, providerd has nothing to take up: it waits for the backend
// to finish. And it has to be every request until providerd is dead: a
// running providerd sends a restore it got no answer to again on its next
// pass, a few seconds later, which would leave nothing for the restarted one.

// retainUnacknowledged runs a gated workload with a volume, writes to the
// volume, and lets end finish the lease on the chain while it is still
// pending. The workload then turns healthy with no lease to serve, and the
// provider has the backend remove it, which retains its data.
func retainUnacknowledged(t *testing.T, p *Provider, tenant *Tenant, end func(leaseUUID string)) restoreSource {
	t.Helper()
	backend := p.Backends[0]
	source := restoreSource{manifest: map[string]any{"image": e2e.RedisImage, "command": []string{"sleep", "3600"}}}
	var held container.InspectResponse
	source.leaseUUID, held = holdCreate(t, p, tenant, p.SKUs.Stateful, source.manifest)
	backend.Exec(t, held.ID, "sh", "-c", "echo kept > /data/sentinel")
	source.inode, source.project = inode(t, dataDir(t, held)), e2e.ProjectID(t, dataDir(t, held))
	require.NotZero(t, source.project, "a volume belongs to a project")

	end(source.leaseUUID)
	requireStillHeld(t, backend, source.leaseUUID, held)
	openGate(t, backend, held)
	requireStatus(t, backend, source.leaseUUID, "retained")
	return source
}

// requireRestoredAcrossAnInterruption restores the source into a new lease,
// loses every restore providerd sends the backend, and kills providerd. The
// restarted providerd must send the restore again, and the new lease must
// become active on the source's data in one container, started once.
func requireRestoredAcrossAnInterruption(t *testing.T, p *Provider, tenant *Tenant, source restoreSource) {
	t.Helper()
	backend, link := p.Backends[0], p.Links[0]
	lost := link.LoseRequests("restore")
	asked := time.Now()
	target := tenant.RestoreUnanswered(t, p.SKUs.Stateful, source.leaseUUID, source.manifest, lost)
	_, err := backend.Client.Get(t.Context(), target)
	require.ErrorIs(t, err, e2e.ErrNotFound, "the backend must not have received the restore")
	requireStatus(t, backend, source.leaseUUID, "retained")
	require.Equal(t, LeasePending, p.Chain.Lease(t, target).State)

	p.Kill()
	lost.Deliver()
	p.Start()
	held := requireHeld(t, backend, target, "restarting")
	require.Equal(t, LeasePending, p.Chain.Lease(t, target).State, "a lease whose workload is not healthy is not acknowledged")
	openGate(t, backend, held)
	requireRestored(t, backend, source, requireSettled(t, p, backend, target, held))
	require.Equal(t, []string{held.ID}, backend.Started(t, target, asked), "the restore must have started one container, once")
}

// TestRestore_FromALeaseTheTenantWithdrew restores the data of a lease its
// tenant cancelled before the provider acknowledged it. The chain records
// such a lease as rejected.
func TestRestore_FromALeaseTheTenantWithdrew(t *testing.T) {
	options := heldSettings(nil)
	options.Links = true
	p := StartProviderWith(t, 1, options)
	tenant := p.NewTenant(t)

	source := retainUnacknowledged(t, p, tenant, func(leaseUUID string) {
		tenant.Cancel(t, leaseUUID)
		require.Equal(t, LeaseRejected, p.Chain.Lease(t, leaseUUID).State)
	})
	requireRestoredAcrossAnInterruption(t, p, tenant, source)
}

// TestRestore_FromALeaseThatExpired restores the data of a lease the chain
// expired because the provider did not acknowledge it in time.
func TestRestore_FromALeaseThatExpired(t *testing.T) {
	options := heldSettings(nil)
	options.Links = true
	// The chain's minimum. The restored lease is pending while its workload
	// is held, and must be acknowledged within the same minute.
	options.PendingTimeout = time.Minute
	p := StartProviderWith(t, 1, options)
	tenant := p.NewTenant(t)

	source := retainUnacknowledged(t, p, tenant, func(leaseUUID string) {
		eventually(t, "the chain expires lease "+leaseUUID, func() bool {
			onChain := p.Chain.Lease(t, leaseUUID)
			if onChain.State == LeaseExpired {
				return true
			}
			require.Equal(t, LeasePending, onChain.State, "the lease left pending without expiring: %+v", onChain)
			return false
		})
	})
	requireRestoredAcrossAnInterruption(t, p, tenant, source)
}
