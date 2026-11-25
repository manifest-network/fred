//go:build integration

package e2e

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	statusReady    = "ready"
	statusRetained = "retained"
)

// sleeper is a manifest whose workload does nothing, so a lease is ready as
// soon as its container runs.
func sleeper(image string) map[string]any {
	return map[string]any{"image": image, "command": []string{"sleep", "3600"}}
}

// requireStatus waits until the backend reports the lease in the wanted
// status. The budget covers a first image admission.
func requireStatus(t *testing.T, b *Backend, leaseUUID, want string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		lease, err := b.Client.Get(t.Context(), leaseUUID)
		if assert.NoError(collect, err) {
			assert.Equal(collect, want, lease.Status)
		}
	}, 3*time.Minute, 500*time.Millisecond)
}

// requireGone waits until the backend no longer knows the lease.
func requireGone(t *testing.T, b *Backend, leaseUUID string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		_, err := b.Client.Get(t.Context(), leaseUUID)
		assert.ErrorIs(collect, err, ErrNotFound)
	}, time.Minute, 500*time.Millisecond)
}

// closeLease closes a lease the way providerd does. While earlier work on the
// lease settles, 0.14 answers close_deferred or lifecycle_pending and expects
// the same close again, so the close is sent until it is accepted.
func closeLease(t *testing.T, b *Backend, leaseUUID string) {
	t.Helper()
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NoError(collect, b.Client.Close(t.Context(), leaseUUID))
	}, time.Minute, time.Second)
}

// requireOneRunningContainer returns the lease's only container, which must
// be running.
func requireOneRunningContainer(t *testing.T, b *Backend, leaseUUID string) container.InspectResponse {
	t.Helper()
	containers := b.Containers(t, leaseUUID)
	require.Len(t, containers, 1)
	require.True(t, containers[0].State.Running, "container state: %s", containers[0].State.Status)
	return containers[0]
}

// TestBackend_Lifecycle runs a stateless lease from Apply to Close and checks
// each step against Docker as well as against the backend's answers.
func TestBackend_Lifecycle(t *testing.T) {
	b := StartBackend(t)
	ctx := t.Context()
	lease := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatelessSKU, Manifest: sleeper(BusyboxImage)}

	require.NoError(t, b.Client.Apply(ctx, lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	requireOneRunningContainer(t, b, lease.LeaseUUID)

	leases, err := b.Client.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []Lease{{UUID: lease.LeaseUUID, Status: statusReady, Tenant: lease.Tenant, BudgetVerdict: VerdictRetry}}, leases)

	closeLease(t, b, lease.LeaseUUID)
	requireGone(t, b, lease.LeaseUUID)
	require.Empty(t, b.Containers(t, lease.LeaseUUID))
	leases, err = b.Client.List(ctx)
	require.NoError(t, err)
	require.Empty(t, leases)
}

// TestBackend_RetainAndRestore closes a stateful lease and restores its data
// into a new one. The data must survive the close as retained, reach the new
// lease, and belong to that lease alone afterwards.
func TestBackend_RetainAndRestore(t *testing.T) {
	b := StartBackend(t)
	ctx := t.Context()
	// The image declares VOLUME /data, which a stateful SKU backs with a
	// quota-capped directory on the backend's XFS volume path.
	source := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatefulSKU, Manifest: sleeper(RedisImage)}

	require.NoError(t, b.Client.Apply(ctx, source))
	requireStatus(t, b, source.LeaseUUID, statusReady)
	sourceContainer := requireOneRunningContainer(t, b, source.LeaseUUID)
	requireVolumeBind(t, b, sourceContainer, "/data")
	b.Exec(t, sourceContainer.ID, "sh", "-c", "echo kept > /data/sentinel")

	closeLease(t, b, source.LeaseUUID)
	requireStatus(t, b, source.LeaseUUID, statusRetained)
	require.Empty(t, b.Containers(t, source.LeaseUUID))

	target := Spec{LeaseUUID: uuid.NewString(), Tenant: source.Tenant, SKU: StatefulSKU}
	require.NoError(t, b.Client.Restore(ctx, target, source.LeaseUUID))
	requireStatus(t, b, target.LeaseUUID, statusReady)
	targetContainer := requireOneRunningContainer(t, b, target.LeaseUUID)
	requireVolumeBind(t, b, targetContainer, "/data")
	require.Equal(t, "kept\n", b.Exec(t, targetContainer.ID, "cat", "/data/sentinel"))
	requireGone(t, b, source.LeaseUUID)

	closeLease(t, b, target.LeaseUUID)
	requireStatus(t, b, target.LeaseUUID, statusRetained)
}

// withoutPeriodicPass puts a backend's periodic pass an hour away, for a test
// of what the backend does at its start or on a Docker event: with the
// fixture's ten seconds, a pass would put the same things right a moment
// later and the test could not tell which did.
var withoutPeriodicPass = map[string]any{"reconcile_interval": "1h"}

// TestBackend_GracefulRestartKeepsLease stops and starts docker-backend under
// a running lease. The container must outlive the process, and the restarted
// backend must report the same lease on the same container.
//
// The backend's periodic pass is an hour away, so it is what the backend
// finds at its start that makes the lease ready again, and not a later pass.
func TestBackend_GracefulRestartKeepsLease(t *testing.T) {
	b := StartBackendWithConfig(t, withoutPeriodicPass)
	ctx := t.Context()
	lease := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatelessSKU, Manifest: sleeper(BusyboxImage)}

	require.NoError(t, b.Client.Apply(ctx, lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	before := requireOneRunningContainer(t, b, lease.LeaseUUID)

	b.Stop()
	require.Equal(t, before.ID, requireOneRunningContainer(t, b, lease.LeaseUUID).ID)

	b.Start()
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	after := requireOneRunningContainer(t, b, lease.LeaseUUID)
	require.Equal(t, before.ID, after.ID)
	require.Equal(t, before.State.StartedAt, after.State.StartedAt, "the container must not have been restarted")

	closeLease(t, b, lease.LeaseUUID)
	requireGone(t, b, lease.LeaseUUID)
	require.Empty(t, b.Containers(t, lease.LeaseUUID))
}

// requireVolumeBind asserts the container mounts destination from a directory
// under the backend's volume path, not from a Docker volume or a tmpfs.
func requireVolumeBind(t *testing.T, b *Backend, c container.InspectResponse, destination string) {
	t.Helper()
	for _, mount := range c.Mounts {
		if mount.Destination == destination {
			require.Equal(t, "bind", string(mount.Type))
			require.True(t, strings.HasPrefix(mount.Source, b.VolumePath+"/"),
				"%s is mounted from %s, outside %s", destination, mount.Source, b.VolumePath)
			return
		}
	}
	require.Failf(t, "mount missing", "container %s has no mount at %s", c.ID, destination)
}

// TestBackend_SecondApplyChangesNothing sends a lease's command a second
// time under a new operation, as a caller that lost track of the first would.
// Whether the backend refuses it or accepts it, the lease must keep running
// in its one container, never restarted.
//
// The test does not yet assert what the backend then reports for the lease.
// 0.14 refuses the second command, and at its next periodic pass reports the
// lease failed although its container runs untouched: the refused command
// became the lease's last operation, and a lease whose last operation failed
// is left out of the pass's inventory (ENG-2195). The status assertion belongs
// here once that is fixed.
func TestBackend_SecondApplyChangesNothing(t *testing.T) {
	b := StartBackend(t)
	lease := Spec{LeaseUUID: uuid.NewString(), Tenant: "tenant-a", SKU: StatelessSKU, Manifest: sleeper(BusyboxImage)}
	require.NoError(t, b.Client.Apply(t.Context(), lease))
	requireStatus(t, b, lease.LeaseUUID, statusReady)
	first := requireOneRunningContainer(t, b, lease.LeaseUUID)

	applied := time.Now()
	if err := b.Client.Apply(t.Context(), lease); err != nil {
		// 0.14 refuses a lease it already runs.
		var refused *StatusError
		require.ErrorAs(t, err, &refused)
		require.Equal(t, http.StatusConflict, refused.Status, "second apply: %v", err)
	}
	// Two of the backend's periodic passes, in case it acts on the command
	// after answering it.
	time.Sleep(20 * time.Second)
	require.Empty(t, b.Started(t, lease.LeaseUUID, applied), "the second apply must not start a container")
	now := requireOneRunningContainer(t, b, lease.LeaseUUID)
	require.Equal(t, first.ID, now.ID)
	require.Equal(t, first.State.StartedAt, now.State.StartedAt)
	reported, err := b.Client.Get(t.Context(), lease.LeaseUUID)
	require.NoError(t, err)
	t.Logf("after the second apply the backend reports the lease %q", reported.Status)
}
