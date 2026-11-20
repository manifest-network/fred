//go:build integration

package chain

import (
	"context"
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
// Each test also interrupts the restore: the backend's answer to it is lost,
// and providerd is killed while the restored workload is not yet healthy. The
// restarted providerd finds a restore it never heard back about, and reads
// the source lease again before it sends the restore a second time. So the
// tests cover both places where a source that was never active used to be
// turned away: when the restore is asked for, and when it is taken up again.
//
// The restart is needed. A providerd that stays up waits for the backend to
// report on the restore it asked for, and sends nothing again.

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

// oneHeld reports whether exactly one lease on the backend is in the given
// status with one container that has started and not passed its health check,
// and records that container in held. It never fails the test. A test uses it
// for a lease that does not exist when the response to lose is named.
func oneHeld(backend *e2e.Backend, status string, held *container.InspectResponse) bool {
	leases, err := backend.Client.List(context.Background())
	if err != nil {
		return false
	}
	var found []string
	for _, lease := range leases {
		if lease.Status == status {
			found = append(found, lease.UUID)
		}
	}
	if len(found) != 1 {
		return false
	}
	containers, err := backend.TryContainers(found[0])
	if err != nil || len(containers) != 1 {
		return false
	}
	one := containers[0]
	if !one.State.Running || one.State.Health == nil || one.State.Health.Status != container.Starting {
		return false
	}
	*held = one
	return true
}

// requireRestoredAcrossAnInterruption restores the source into a new lease,
// loses the backend's answer while the restored workload is started and not
// yet healthy, and then kills providerd. The restarted providerd must send
// the restore again, the backend must not run it twice, and the new lease
// must become active on the source's data.
func requireRestoredAcrossAnInterruption(t *testing.T, p *Provider, tenant *Tenant, source restoreSource) {
	t.Helper()
	backend, link := p.Backends[0], p.Links[0]
	var held container.InspectResponse
	lost := link.LoseResponse("restore", func() bool { return oneHeld(backend, "restarting", &held) })
	asked := time.Now()
	target := tenant.RestoreUnanswered(t, p.SKUs.Stateful, source.leaseUUID, source.manifest)
	lost.Require(t)
	require.Equal(t, LeasePending, p.Chain.Lease(t, target).State, "a lease whose workload is not healthy is not acknowledged")

	p.Kill()
	p.Start()
	eventually(t, "the restarted providerd sends the restore of lease "+target+" again", func() bool {
		// The count starts with the process.
		return p.Dispatched(t, backend, "restore") >= 1
	})
	requireStillHeld(t, backend, target, held)
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
