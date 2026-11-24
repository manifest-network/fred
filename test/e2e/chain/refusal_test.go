//go:build integration

package chain

import (
	"net/http"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/test/e2e"
)

// The tests in this file make the provider turn a tenant's request away, and
// check what the tenant is told (G24, G25). A refusal must say why, in a
// reason a program can read. It must change nothing, so that the tenant can
// send the request again. And where the cause passes, the same request must
// then go through.

// requireRefused asserts that the provider refused a request with the given
// status and reason.
func requireRefused(t *testing.T, answer Answer, status int, reason, what string) {
	t.Helper()
	require.Equal(t, status, answer.Status, "%s: %s", what, answer.Body)
	require.Equal(t, reason, answer.Reason, "%s: %s", what, answer.Body)
	require.NotEmpty(t, answer.Error, "%s: a refusal says what is wrong: %s", what, answer.Body)
}

// passHoldsLease is what the provider answers a restart or an update with
// while one of its own passes is at work on the lease: 409, "lease is already
// undergoing a lifecycle operation". A pass holds each lease for a moment, so
// a request can meet one at any time, and most likely just after a pass that
// was held open, when the next one starts at once. Nothing has been sent to
// the backend, and the same request goes through a moment later.
const passHoldsLease = http.StatusConflict

// accepted sends a request until the provider accepts it, and fails the test
// when the provider answers with anything but an acceptance or one of the
// statuses a tenant is told to retry.
func accepted(t *testing.T, what string, ask func() Answer, retried ...int) {
	t.Helper()
	eventually(t, "the provider accepts "+what, func() bool {
		answer := ask()
		if answer.Status == http.StatusAccepted {
			return true
		}
		require.Contains(t, retried, answer.Status, "%s: %s", what, answer.Body)
		return false
	})
}

// TestRefusal_WhileTheProviderTakesStock sends a restart, an update and a
// restore while the provider is in the middle of a pass: it has heard from
// the backend that holds the leases and is still waiting for another one.
// Until it has heard from all of them it cannot tell what each backend
// holds, and it refuses to change a lease it is taking stock of. Each
// request must be refused as that, with a Retry-After, and nothing may come
// of it. Sent again after the pass, with the same idempotency key, each must
// be done, once.
//
// The pass is held open by a second backend that holds nothing: its answer
// is kept back on the way to providerd.
func TestRefusal_WhileTheProviderTakesStock(t *testing.T) {
	const reconciling = "inventory_reconciling"
	p := StartProviderWith(t, 2, Options{Links: true})
	tenant := p.NewTenant(t)
	stored := runs(e2e.RedisImage)
	source := deployActive(t, p, tenant, p.SKUs.Stateful, stored)
	home := p.Host(t, source)
	idle := p.Links[1-slices.Index(p.Backends, home)]
	home.Exec(t, running(t, home, source).ID, "sh", "-c", "echo kept > /data/sentinel")
	tenant.Close(t, source)
	requireStatus(t, home, source, "retained")
	lease := deployActive(t, p, tenant, home.SKUs.Pinned, runs(e2e.BusyboxImage))
	describeOnFailure(t, home, lease)
	workload := running(t, home, lease)
	target := tenant.CreateLease(t, stored, p.SKUs.Stateful+":1")

	// One idempotency key per command, so a repeated request names the
	// command it repeats.
	restartKey, updateKey := uuid.NewString(), uuid.NewString()
	restart := func() Answer {
		return tenant.Ask(t, http.MethodPost, lease, "restart", map[string]string{"Idempotency-Key": restartKey}, nil)
	}
	update := func() Answer {
		return tenant.Ask(t, http.MethodPost, lease, "update", map[string]string{"Idempotency-Key": updateKey}, UpdateTo(t, runs(e2e.RedisImage)))
	}
	restore := func() Answer {
		return tenant.Ask(t, http.MethodPost, target, "restore", nil, RestoreFrom(t, source))
	}

	// The provider asks every backend for its retained leases last. The idle
	// backend's answer to that is held, so the pass stays open with both
	// the running lease and the retained one already heard of.
	var held, release atomic.Bool
	lost := idle.LoseResponse("retentions", func() bool {
		held.Store(true)
		return release.Load()
	})
	eventually(t, "a pass of the provider waits for its idle backend", held.Load)
	// The backends are asked at the same time: by now the other one, which
	// answers at once, has been heard.
	time.Sleep(2 * time.Second)
	sent := time.Now()
	for name, ask := range map[string]func() Answer{"restart": restart, "update": update, "restore": restore} {
		for _, attempt := range []string{"first", "repeated"} {
			answer := ask()
			requireRefused(t, answer, http.StatusServiceUnavailable, reconciling, attempt+" "+name)
			require.Equal(t, "1", answer.RetryAfter, "%s %s must say when to come back", attempt, name)
		}
	}
	requireUntouched(t, home, lease, workload)
	requireStatus(t, home, source, "retained")
	_, err := home.Client.Get(t.Context(), target)
	require.ErrorIs(t, err, e2e.ErrNotFound, "the refused restore must not have reached the backend")
	for _, operation := range []string{"restart", "update", "restore"} {
		require.Zero(t, p.Dispatched(t, home, operation), "a refused %s was sent to the backend", operation)
	}
	release.Store(true)
	lost.Require(t)

	// The same requests, after the pass.
	accepted(t, "the restart", restart, http.StatusServiceUnavailable, passHoldsLease)
	restarted := replaced(t, home, lease, workload)
	requireImage(t, restarted, e2e.BusyboxImage)
	accepted(t, "the update", update, http.StatusServiceUnavailable, passHoldsLease)
	updated := replaced(t, home, lease, restarted)
	requireImage(t, updated, e2e.RedisImage)
	require.Equal(t, []string{restarted.ID, updated.ID}, home.Started(t, lease, sent), "the restart and the update must each have run once")

	accepted(t, "the restore", restore, http.StatusServiceUnavailable)
	requireActive(t, p.Chain, target)
	requireStatus(t, home, target, "ready")
	require.Equal(t, "kept\n", home.Exec(t, running(t, home, target).ID, "cat", "/data/sentinel"))
}

// TestRefusal_RestoreConflictsSayWhy walks one restore through every state
// in which the provider turns it away. While the source's close is still
// stopping its workload, the source is busy. While the restore is under way,
// a second one into the same lease finds the target busy, and an upload into
// it is told that a restore owns the lease. Once the lease is active it is no
// longer one that can be restored into. Each refusal names its reason, sends
// nothing to the backend, and leaves the restore that is under way alone.
func TestRefusal_RestoreConflictsSayWhy(t *testing.T) {
	// The source's workload ignores SIGTERM, so its close lasts the stop
	// grace. It is gated, so a restore of it stays under way until the test
	// opens the gate.
	p := StartProviderWith(t, 1, heldSettings(map[string]any{"container_stop_timeout": stopGrace}))
	backend, tenant := p.Backends[0], p.NewTenant(t)
	stored := runs(e2e.RedisImage)
	source, first := holdCreate(t, p, tenant, p.SKUs.Stateful, stored)
	openGate(t, backend, first)
	requireSettled(t, p, backend, source, first)
	backend.Exec(t, first.ID, "sh", "-c", "echo kept > /data/sentinel")
	target := tenant.CreateLease(t, stored, p.SKUs.Stateful+":1")
	restore := func() Answer {
		return tenant.Ask(t, http.MethodPost, target, "restore", nil, RestoreFrom(t, source))
	}
	twice := func(status int, reason, what string) {
		t.Helper()
		for _, attempt := range []string{"first", "repeated"} {
			requireRefused(t, restore(), status, reason, attempt+" restore "+what)
		}
	}

	tenant.Close(t, source)
	eventually(t, "the close of lease "+source+" is held while its workload stops", func() bool {
		return reported(t, backend, source).Status == "deprovisioning"
	})
	twice(http.StatusConflict, "source_busy", "while the source closes")
	require.Zero(t, p.Dispatched(t, backend, "restore"), "a refused restore was sent to the backend")
	require.Equal(t, LeasePending, p.Chain.Lease(t, target).State)

	// The same request, once the source is closed.
	requireStatus(t, backend, source, "retained")
	asked := time.Now()
	accepted(t, "the restore", restore, http.StatusConflict)
	held := requireHeld(t, backend, target, "restarting")
	deployed := p.Dispatched(t, backend, "provision")
	twice(http.StatusConflict, "target_busy", "into a lease being restored")
	for _, attempt := range []string{"first", "repeated"} {
		requireRefused(t, tenant.Upload(t, target, stored), http.StatusConflict, "restore_in_progress", attempt+" upload into a lease being restored")
	}
	requireStillHeld(t, backend, target, held)
	require.Equal(t, deployed, p.Dispatched(t, backend, "provision"), "a refused upload must not deploy the lease")

	openGate(t, backend, held)
	settled := requireSettled(t, p, backend, target, held)
	require.Equal(t, "kept\n", backend.Exec(t, settled.ID, "cat", "/data/sentinel"))
	twice(http.StatusConflict, "target_not_pending", "into an active lease")
	requireUntouched(t, backend, target, settled)
	require.Equal(t, []string{settled.ID}, backend.Started(t, target, asked), "the restore must have started one container, once")
}

// TestBillingOnly_LeaseIsAcknowledgedAndNeverDeployed leases a SKU the
// provider bills for and does not run. The provider must acknowledge the
// lease without a manifest and send nothing to a backend. An upload or a
// restore into the lease is refused as billing-only whatever state the lease
// is in: right after it was created, once it is active, and after it is
// closed.
func TestBillingOnly_LeaseIsAcknowledgedAndNeverDeployed(t *testing.T) {
	const billingOnly = "billing_only"
	p := StartProviderWith(t, 1, Options{BillingOnly: true})
	backend, tenant := p.Backends[0], p.NewTenant(t)
	started := time.Now()
	manifest := runs(e2e.BusyboxImage)
	lease := tenant.CreateLease(t, manifest, p.BillingOnlySKU+":1")
	refused := func(when string) {
		t.Helper()
		requireRefused(t, tenant.Upload(t, lease, manifest), http.StatusBadRequest, billingOnly, "upload "+when)
		answer := tenant.Ask(t, http.MethodPost, lease, "restore", nil, RestoreFrom(t, uuid.NewString()))
		requireRefused(t, answer, http.StatusBadRequest, billingOnly, "restore "+when)
	}

	// The provider acknowledges the lease within a block or two, so this
	// usually finds it still pending. The answer is the same either way.
	refused("right after the lease was created")
	t.Logf("the first refusals were sent with the lease %s or just before", p.Chain.Lease(t, lease).State)

	requireActive(t, p.Chain, lease)
	refused("with the lease active")
	p.AwaitSweeps(t, 2, SweepComplete)
	require.Equal(t, LeaseActive, p.Chain.Lease(t, lease).State)

	tenant.Close(t, lease)
	require.Equal(t, LeaseClosed, p.Chain.Lease(t, lease).State)
	refused("with the lease closed")
	p.AwaitSweeps(t, 2, SweepComplete)

	for _, operation := range []string{"provision", "restore"} {
		require.Zero(t, p.Dispatched(t, backend, operation), "the provider sent a %s for a billing-only lease", operation)
	}
	listed, err := backend.Client.List(t.Context())
	require.NoError(t, err)
	require.Empty(t, listed, "the backend must hold no lease")
	require.Empty(t, backend.Created(t, started), "no container may be created for a billing-only lease")
}
