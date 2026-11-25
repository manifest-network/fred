//go:build integration

package e2e

import (
	"net/http"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file follow a lease without a volume through what a
// backend does with it: how its containers are run, what the backend refuses
// to run, and what it reports when a container dies. A lease's ordinary path
// from Apply to Close is in smoke_test.go, and its restart and update, which
// a tenant asks the provider for, in the chain tier.

// stateless is a new lease without a volume.
func stateless(tenant string) Spec {
	return Spec{LeaseUUID: uuid.NewString(), Tenant: tenant, SKU: StatelessSKU, Manifest: sleeper(BusyboxImage)}
}

// requireRunningContainers returns the lease's containers, which must be the
// given number and all running.
func requireRunningContainers(t *testing.T, b *Backend, leaseUUID string, count int) []container.InspectResponse {
	t.Helper()
	containers := b.Containers(t, leaseUUID)
	require.Len(t, containers, count, "containers of lease %s", leaseUUID)
	for _, one := range containers {
		require.True(t, one.State.Running, "container %s is %s", one.Name, one.State.Status)
	}
	return containers
}

// requireFailed waits until the backend reports the lease failed, and returns
// what it reports.
func requireFailed(t *testing.T, b *Backend, leaseUUID string) Lease {
	t.Helper()
	var lease Lease
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var err error
		lease, err = b.Client.Get(t.Context(), leaseUUID)
		if assert.NoError(collect, err) {
			assert.Equal(collect, statusFailed, lease.Status)
		}
	}, 3*time.Minute, 500*time.Millisecond)
	return lease
}

// requireListedFailed asserts that the backend still lists a failed lease, as
// failed. Asking for a lease by its UUID is not enough to know that: the
// backend answers for a lease it no longer lists from what it recorded when
// the lease failed (TestBackend_FailedDeployStaysExplained).
func requireListedFailed(t *testing.T, b *Backend, leaseUUID string) {
	t.Helper()
	listed, err := b.Client.List(t.Context())
	require.NoError(t, err)
	for _, lease := range listed {
		if lease.UUID == leaseUUID {
			require.Equal(t, statusFailed, lease.Status, "lease %s as the backend lists it", leaseUUID)
			return
		}
	}
	require.Failf(t, "lease not listed", "the backend must still list failed lease %s: %+v", leaseUUID, listed)
}

// requireNotListed asserts that the backend lists none of the leases.
func requireNotListed(t *testing.T, b *Backend, leaseUUIDs ...string) {
	t.Helper()
	listed, err := b.Client.List(t.Context())
	require.NoError(t, err)
	for _, lease := range listed {
		require.NotContains(t, leaseUUIDs, lease.UUID, "the backend lists lease %s as %s", lease.UUID, lease.Status)
	}
}

// requireRefused asserts that the backend refused a lease with the given
// status and code, and started nothing for it.
func requireRefused(t *testing.T, b *Backend, lease Spec, err error, status int, code string) *StatusError {
	t.Helper()
	var refused *StatusError
	require.ErrorAs(t, err, &refused, "lease %s must be refused", lease.LeaseUUID)
	require.Equal(t, status, refused.Status, "refusal: %v", err)
	require.Equal(t, code, refused.Code, "refusal: %v", err)
	require.Empty(t, b.Containers(t, lease.LeaseUUID), "a refused lease must start nothing")
	return refused
}

// TestBackend_ContainerIsHardened checks the limits a tenant's container runs
// under, as Docker reports them and as the workload meets them: no
// capabilities, no way to gain privileges, a process limit, and a root
// filesystem it cannot write, with /tmp and /run as the writable exceptions.
func TestBackend_ContainerIsHardened(t *testing.T) {
	// Not the default, so the limit Docker reports is the configured one.
	b := StartBackendWithConfig(t, map[string]any{"container_pids_limit": 128})
	lease := stateless("tenant-a")
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	workload := requireOneRunningContainer(t, b, lease.LeaseUUID)

	host := workload.HostConfig
	require.True(t, host.ReadonlyRootfs, "the root filesystem must be read-only")
	require.Contains(t, []string(host.CapDrop), "ALL", "every capability must be dropped")
	require.Empty(t, host.CapAdd, "no capability may be added back")
	require.Contains(t, host.SecurityOpt, "no-new-privileges:true")
	require.NotNil(t, host.PidsLimit)
	require.EqualValues(t, 128, *host.PidsLimit)
	writable := map[string]bool{}
	for _, mount := range workload.Mounts {
		if string(mount.Type) == "tmpfs" {
			writable[mount.Destination] = true
		}
	}
	require.True(t, writable["/tmp"], "/tmp must be a tmpfs: %+v", workload.Mounts)
	require.True(t, writable["/run"], "/run must be a tmpfs: %+v", workload.Mounts)

	// The same, as the workload meets it.
	exitCode, out := b.TryExec(t, workload.ID, "touch", "/written-by-the-tenant")
	require.NotZero(t, exitCode, "the root filesystem must refuse a write: %s", out)
	b.Exec(t, workload.ID, "touch", "/tmp/written-by-the-tenant", "/run/written-by-the-tenant")
}

// TestBackend_CloseIsRepeatable closes a lease three times. A caller that
// lost the answer to a close sends it again, so every close of a lease that
// is already gone must succeed.
func TestBackend_CloseIsRepeatable(t *testing.T) {
	b := StartBackend(t)
	lease := stateless("tenant-a")
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)

	closeLease(t, b, lease.LeaseUUID)
	require.Empty(t, b.Containers(t, lease.LeaseUUID))
	require.NoError(t, b.Client.Close(t.Context(), lease.LeaseUUID), "a second close")
	require.NoError(t, b.Client.Close(t.Context(), lease.LeaseUUID), "a third close")
	requireGone(t, b, lease.LeaseUUID)
	require.Empty(t, b.Containers(t, lease.LeaseUUID))
}

// TestBackend_EveryInstanceOfALeaseRuns applies a lease for two units of a
// SKU. Both instances must run, and a close must remove both before it
// answers.
func TestBackend_EveryInstanceOfALeaseRuns(t *testing.T) {
	b := StartBackend(t)
	lease := stateless("tenant-a")
	lease.Quantity = map[string]int{"": 2}
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	instances := requireRunningContainers(t, b, lease.LeaseUUID, 2)
	require.NotEqual(t, instances[0].ID, instances[1].ID)

	closeLease(t, b, lease.LeaseUUID)
	require.Empty(t, b.Containers(t, lease.LeaseUUID), "an accepted close has removed every container")
	requireGone(t, b, lease.LeaseUUID)
}

// TestBackend_KilledContainerFailsTheLease kills a running lease's container
// from outside. The backend must notice, report the lease failed, and count
// one failure.
func TestBackend_KilledContainerFailsTheLease(t *testing.T) {
	b := StartBackend(t)
	lease := stateless("tenant-a")
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	requireOneRunningContainer(t, b, lease.LeaseUUID)

	b.KillContainers(t, lease.LeaseUUID)
	failed := requireFailed(t, b, lease.LeaseUUID)
	require.Equal(t, 1, failed.FailCount, "one death is one failure")
	requireListedFailed(t, b, lease.LeaseUUID)
}

// TestBackend_OneKilledInstanceFailsTheLease kills one of a lease's two
// instances. A lease that paid for two and runs one is not running: the
// backend must report the whole lease failed.
func TestBackend_OneKilledInstanceFailsTheLease(t *testing.T) {
	b := StartBackend(t)
	lease := stateless("tenant-a")
	lease.Quantity = map[string]int{"": 2}
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	instances := requireRunningContainers(t, b, lease.LeaseUUID, 2)

	b.KillContainer(t, instances[0].ID)
	requireFailed(t, b, lease.LeaseUUID)
	requireListedFailed(t, b, lease.LeaseUUID)
}

// TestBackend_ContainerThatDiedWhileTheBackendWasDownFailsTheLease kills a
// lease's container while docker-backend is stopped. The backend was not
// there to see the death, so at its next start it must find the lease's
// container dead, report the lease failed, and count one failure. It must do
// so at its start: the periodic pass is an hour away.
func TestBackend_ContainerThatDiedWhileTheBackendWasDownFailsTheLease(t *testing.T) {
	b := StartBackendWithConfig(t, withoutPeriodicPass)
	lease := stateless("tenant-a")
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	workload := requireOneRunningContainer(t, b, lease.LeaseUUID)

	b.Stop()
	b.KillContainer(t, workload.ID)
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		containers, err := b.TryContainers(lease.LeaseUUID)
		if assert.NoError(collect, err) && assert.Len(collect, containers, 1) {
			assert.Equal(collect, "exited", containers[0].State.Status)
		}
	}, 30*time.Second, 200*time.Millisecond)

	b.Start()
	failed := requireFailed(t, b, lease.LeaseUUID)
	require.Equal(t, 1, failed.FailCount, "one death is one failure")
	requireListedFailed(t, b, lease.LeaseUUID)
}

// TestBackend_RefusesALeaseItCannotRun sends leases a backend can tell at
// once it cannot run. Each must be refused in the answer to the request, with
// what is wrong with it, and leave nothing behind: no container, and no lease
// in the backend's list.
func TestBackend_RefusesALeaseItCannotRun(t *testing.T) {
	b := StartBackend(t)
	lease := func(change func(*Spec)) Spec {
		spec := stateless("tenant-a")
		change(&spec)
		return spec
	}
	cases := []struct {
		name string
		spec Spec
		code string
	}{
		{"a SKU the backend does not serve", lease(func(s *Spec) { s.SKU = uuid.NewString() }), CodeUnknownSKU},
		{"a manifest without an image", lease(func(s *Spec) { s.Manifest["image"] = "" }), CodeInvalidManifest},
		{"a manifest of the wrong shape", lease(func(s *Spec) { s.Manifest["image"] = 42 }), CodeInvalidManifest},
		// Never pulled: the registry is not on the backend's list.
		{"an image from a registry that is not allowed", lease(func(s *Spec) { s.Manifest["image"] = "registry.example.com/workload:1.0" }), CodeImageNotAllowed},
	}
	refused := make([]string, 0, len(cases))
	for _, c := range cases {
		err := b.Client.Apply(t.Context(), c.spec)
		require.Error(t, err, c.name)
		requireRefused(t, b, c.spec, err, http.StatusBadRequest, c.code)
		refused = append(refused, c.spec.LeaseUUID)
	}
	requireNotListed(t, b, refused...)
}

// TestBackend_RefusesALeaseBeyondItsCapacity gives a backend the CPU of one
// lease. A second lease must be refused for lack of capacity and start
// nothing. Once the first is closed, the same second lease must be accepted:
// a close has released the lease's capacity by the time it answers.
func TestBackend_RefusesALeaseBeyondItsCapacity(t *testing.T) {
	// The fixture's SKUs take a quarter of a core each.
	b := StartBackendWithConfig(t, map[string]any{"total_cpu_cores": 0.25})
	first, second := stateless("tenant-a"), stateless("tenant-a")
	require.NoError(t, b.Client.Apply(t.Context(), first))
	requireStatus(t, b, first.LeaseUUID, statusReady)

	err := b.Client.Apply(t.Context(), second)
	require.Error(t, err, "a lease that does not fit must be refused")
	requireRefused(t, b, second, err, http.StatusServiceUnavailable, CodeInsufficientResources)
	requireNotListed(t, b, second.LeaseUUID)
	requireOneRunningContainer(t, b, first.LeaseUUID)

	closeLease(t, b, first.LeaseUUID)
	// The refused lease, sent again as its caller would send it. While the
	// backend settles the refusal it asks for the lease to be sent later.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NoError(collect, b.Client.Apply(t.Context(), second))
	}, time.Minute, time.Second)
	requireStatus(t, b, second.LeaseUUID, statusReady)
	requireOneRunningContainer(t, b, second.LeaseUUID)
}
