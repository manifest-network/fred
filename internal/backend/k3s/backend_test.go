package k3s

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/hmacauth"
)

// zeroBackoff eliminates retry delays so tests don't pay 1s/5s waits on
// failed callback deliveries. Mirrors docker/provision_test.go:64.
var zeroBackoff = [shared.CallbackMaxAttempts]time.Duration{}

// testCallbackSecret is the HMAC secret the fake Fred receiver uses to
// verify inbound callbacks. 32 chars to satisfy Config.Validate's floor.
const testCallbackSecret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

// newBackendForTest constructs a Backend with t.TempDir-backed bbolt
// stores, a zero-backoff callback sender, and an HTTP client targeting
// the caller-supplied fake Fred. Stop is registered in t.Cleanup so the
// stores close and the wait group drains on teardown.
//
// fredURL is unused for callback routing (callback URL is per-request
// via ProvisionRequest.CallbackURL); the parameter exists for symmetry
// with future tests that may want to route through a single Fred URL.
//
// KubeconfigPath is left empty — these tests never invoke Health, so the
// client-go resolver is never exercised. T7c covers Health in isolation.
func newBackendForTest(t *testing.T, fredURL string) *Backend {
	t.Helper()
	_ = fredURL

	cfg := validConfig()
	dir := t.TempDir()
	cfg.CallbackDBPath = filepath.Join(dir, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(dir, "diagnostics.db")
	cfg.ReleasesDBPath = filepath.Join(dir, "releases.db")
	cfg.CallbackSecret = config.Secret(testCallbackSecret)

	b, err := New(cfg, slog.Default())
	require.NoError(t, err)

	rebuildCallbackSender(b)
	t.Cleanup(func() { _ = b.Stop() })
	return b
}

// rebuildCallbackSender swaps b.callbackSender for one configured with
// zeroBackoff and a short-timeout HTTP client. Same-package access lets
// us replace unexported fields without touching production code.
// Mirrors docker/provision_test.go:78.
func rebuildCallbackSender(b *Backend) {
	b.httpClient = &http.Client{Timeout: 5 * time.Second}
	b.callbackSender = shared.NewCallbackSender(shared.CallbackSenderConfig{
		Store:      b.callbackStore,
		HTTPClient: b.httpClient,
		Secret:     string(b.cfg.CallbackSecret),
		Logger:     b.logger,
		StopCtx:    b.stopCtx,
		Backoff:    &zeroBackoff,
	})
}

// startFakeFred returns an httptest.Server that plays Fred for callback
// delivery tests: it HMAC-verifies inbound requests with testCallbackSecret
// (failing the test on mismatch), unmarshals the body to a
// backend.CallbackPayload, and pushes it to a buffered channel for the
// test to await with a timeout. Closes automatically via t.Cleanup.
func startFakeFred(t *testing.T) (*httptest.Server, <-chan backend.CallbackPayload) {
	t.Helper()
	ch := make(chan backend.CallbackPayload, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("fake Fred: read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		sig := r.Header.Get(hmacauth.SignatureHeader)
		if err := hmacauth.Verify(testCallbackSecret, body, sig, 5*time.Minute); err != nil {
			t.Errorf("fake Fred: HMAC verify failed: %v (sig=%q)", err, sig)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var p backend.CallbackPayload
		if err := json.Unmarshal(body, &p); err != nil {
			t.Errorf("fake Fred: unmarshal payload: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ch <- p
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server, ch
}

// awaitCallback reads from ch with a 3-second deadline, failing the test
// on timeout. Returns the received payload.
func awaitCallback(t *testing.T, ch <-chan backend.CallbackPayload) backend.CallbackPayload {
	t.Helper()
	select {
	case p := <-ch:
		return p
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for callback delivery")
		return backend.CallbackPayload{}
	}
}

// newProvisionRequest is a convenience constructor for tests.
func newProvisionRequest(leaseUUID, callbackURL string) backend.ProvisionRequest {
	return backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "manifest1test",
		ProviderUUID: "prov-1",
		Items:        []backend.LeaseItem{{SKU: "k3s-small", Quantity: 1}},
		CallbackURL:  callbackURL,
	}
}

// --- Backend lifecycle ----------------------------------------------------

func TestBackend_New_RejectsInvalidConfig(t *testing.T) {
	// DefaultConfig is intentionally missing required CallbackSecret +
	// HostAddress so the user is forced to fill them; New must surface
	// the validation failure.
	cfg := DefaultConfig()
	b, err := New(cfg, slog.Default())
	require.Error(t, err)
	assert.Nil(t, b)
	assert.ErrorContains(t, err, "invalid config")
}

func TestBackend_New_Success_FieldsPopulated(t *testing.T) {
	fred, _ := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)
	assert.Equal(t, "k3s", b.Name())
	assert.NotNil(t, b.callbackStore)
	assert.NotNil(t, b.diagnosticsStore)
	assert.NotNil(t, b.releaseStore)
	assert.NotNil(t, b.callbackSender)
	assert.NotNil(t, b.pool)
	require.NotNil(t, b.provisions)
	assert.Empty(t, b.provisions)
}

func TestBackend_Start_Succeeds(t *testing.T) {
	fred, _ := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)
	require.NoError(t, b.Start(context.Background()))
}

func TestBackend_Name_ReturnsCfgName(t *testing.T) {
	fred, _ := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)
	assert.Equal(t, "k3s", b.Name())
}

// --- Stub provisioner: validation ----------------------------------------

func TestProvision_RejectsInvalidRequests(t *testing.T) {
	fred, _ := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	tests := []struct {
		name    string
		req     backend.ProvisionRequest
		wantMsg string
	}{
		{
			name:    "empty lease_uuid",
			req:     backend.ProvisionRequest{CallbackURL: fred.URL, Items: []backend.LeaseItem{{SKU: "k3s-small", Quantity: 1}}},
			wantMsg: "lease_uuid is required",
		},
		{
			name:    "empty callback_url",
			req:     backend.ProvisionRequest{LeaseUUID: "lease-1", Items: []backend.LeaseItem{{SKU: "k3s-small", Quantity: 1}}},
			wantMsg: "callback_url is required",
		},
		{
			name:    "empty items",
			req:     backend.ProvisionRequest{LeaseUUID: "lease-1", CallbackURL: fred.URL},
			wantMsg: "items is required",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := b.Provision(context.Background(), tt.req)
			require.Error(t, err)
			assert.ErrorIs(t, err, backend.ErrValidation,
				"Provision must wrap backend.ErrValidation so the HTTP handler maps to 400+ValidationCode")
			assert.ErrorContains(t, err, tt.wantMsg)
		})
	}
}

func TestProvision_RejectsActiveDuplicate(t *testing.T) {
	// Duplicate Provision on a lease whose entry is in a non-failed status
	// (Provisioning, or — once ENG-134+ ships real lifecycle — Ready/
	// Restarting/etc.) must return ErrAlreadyProvisioned. Only failed entries
	// are eligible for replacement (covered by TestProvision_AllowsRetryAfterFailure).
	//
	// We seed the in-memory map directly with a Provisioning entry instead
	// of racing against the stub goroutine — the goroutine flips to Failed
	// quickly and would defeat the in-progress check non-deterministically.
	fred, _ := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	b.provisionsMu.Lock()
	b.provisions["lease-active"] = &provision{
		LeaseUUID: "lease-active",
		Status:    backend.ProvisionStatusProvisioning,
		CreatedAt: time.Now(),
	}
	b.provisionsMu.Unlock()

	err := b.Provision(context.Background(), newProvisionRequest("lease-active", fred.URL))
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrAlreadyProvisioned)
}

func TestProvision_AllowsRetryAfterFailure(t *testing.T) {
	// Regression test for the round-4 Copilot finding: a provision in
	// status=Failed must be replaceable by a subsequent Provision call so
	// Fred's reconciler can retry failed-active leases until FailCount
	// reaches the configured retry ceiling. Mirrors docker-backend's
	// status-aware check.
	//
	// Verifies: (a) the second Provision succeeds (not ErrAlreadyProvisioned),
	// (b) FailCount carries forward across the replacement (1 -> 2 after
	// the second stub failure), (c) the map entry is replaced (not
	// duplicated).
	fred, ch := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("lease-1", fred.URL)))
	_ = awaitCallback(t, ch)

	info, err := b.GetProvision(context.Background(), "lease-1")
	require.NoError(t, err)
	require.Equal(t, backend.ProvisionStatusFailed, info.Status)
	require.Equal(t, 1, info.FailCount)

	// Retry. Should succeed and inherit the prior FailCount.
	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("lease-1", fred.URL)))
	_ = awaitCallback(t, ch)

	info, err = b.GetProvision(context.Background(), "lease-1")
	require.NoError(t, err)
	require.Equal(t, backend.ProvisionStatusFailed, info.Status)
	assert.Equal(t, 2, info.FailCount,
		"FailCount must carry forward across retry-after-failure cycles for reconciler ceiling enforcement")

	list, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.Len(t, list, 1, "lease must not be duplicated in the map after replacement")
}

// --- Stub provisioner: happy path (ENG-133 AC3) --------------------------

func TestProvision_HappyPath_PostsFailedCallback(t *testing.T) {
	// AC3 contract: POST /provision returns 202 and the stub provisioner
	// posts a signed callback with status=failed, error="not implemented",
	// backend=<cfg.Name>, lease_uuid=<request>.
	//
	// HMAC verification happens inside startFakeFred's handler; if the
	// callback's signature is invalid the test fails there.
	fred, ch := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("lease-1", fred.URL)))

	p := awaitCallback(t, ch)
	assert.Equal(t, "lease-1", p.LeaseUUID)
	assert.Equal(t, backend.CallbackStatusFailed, p.Status)
	assert.Equal(t, "not implemented", p.Error)
	assert.Equal(t, "k3s", p.Backend)
}

// --- Deprovision ---------------------------------------------------------

func TestDeprovision_Idempotent_NonexistentLease(t *testing.T) {
	fred, _ := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)
	// Two consecutive deprovisions on a lease that was never provisioned.
	// Both must return nil per BACKEND_GUIDE.md's idempotency contract.
	require.NoError(t, b.Deprovision(context.Background(), "ghost"))
	require.NoError(t, b.Deprovision(context.Background(), "ghost"))
}

func TestDeprovision_RemovesFromMap_AfterProvision(t *testing.T) {
	fred, ch := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("lease-1", fred.URL)))
	_ = awaitCallback(t, ch)

	list, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "lease-1", list[0].LeaseUUID)

	require.NoError(t, b.Deprovision(context.Background(), "lease-1"))

	list, err = b.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.Empty(t, list)
}

// TestDeprovision_RemovesPendingCallback pins the regression guard for
// Copilot id 3237313147. shared.CallbackSender persists every callback
// to bbolt BEFORE delivery and Removes only on success. Pre-fix,
// Deprovision deleted the in-memory record but left the bbolt entry,
// so a failed-delivery → Deprovision → restart sequence would let
// ReplayPendingCallbacks fire a stale status=failed for a torn-down
// lease. Asserts that Deprovision now clears the pending entry too.
func TestDeprovision_RemovesPendingCallback(t *testing.T) {
	fred, _ := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	// Seed the callback store directly to simulate "callback persisted,
	// delivery hasn't succeeded yet". Bypasses Provision so the goroutine-
	// timing path stays out of the test.
	entry := shared.CallbackEntry{
		LeaseUUID:   "lease-1",
		CallbackURL: fred.URL + "/callbacks/provision",
		Status:      backend.CallbackStatusFailed,
		Backend:     b.cfg.Name,
		Error:       "not implemented",
		CreatedAt:   time.Now(),
	}
	require.NoError(t, b.callbackStore.Store(entry))

	pending, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "precondition: callback store has the seeded entry")

	require.NoError(t, b.Deprovision(context.Background(), "lease-1"))

	pending, err = b.callbackStore.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "Deprovision must drop the pending callback so ReplayPendingCallbacks won't fire a stale status=failed")
}

// --- GetProvision contract (architect's required test set) ---------------

func TestGetProvision_FromMap_AfterStubFailure(t *testing.T) {
	fred, ch := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("lease-1", fred.URL)))
	_ = awaitCallback(t, ch) // signals goroutine has flipped state + fired callback

	info, err := b.GetProvision(context.Background(), "lease-1")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "lease-1", info.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, info.Status)
	assert.Equal(t, "not implemented", info.LastError)
	// Option 2 patch: in-memory record carries FailCount=1 alongside Status.
	// Pre-patch this would have returned 0 from the map path; the assertion
	// guards against regression.
	assert.Equal(t, 1, info.FailCount)
}

func TestGetProvision_FromDiagnostics_AfterDeprovision(t *testing.T) {
	fred, ch := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("lease-1", fred.URL)))
	_ = awaitCallback(t, ch)
	require.NoError(t, b.Deprovision(context.Background(), "lease-1"))

	// Diagnostics survive Deprovision (cfg.DiagnosticsMaxAge handles
	// eventual cleanup) so post-teardown queries can still surface the
	// failure cause.
	info, err := b.GetProvision(context.Background(), "lease-1")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "lease-1", info.LeaseUUID)
	assert.Equal(t, "prov-1", info.ProviderUUID)
	// Fallback synthesizes Status=Failed because shared.DiagnosticEntry
	// is failure-only by construction (only the runStubProvisioner failure
	// path calls diagnosticsStore.Store).
	assert.Equal(t, backend.ProvisionStatusFailed, info.Status)
	assert.Equal(t, "not implemented", info.LastError)
	assert.Equal(t, 1, info.FailCount)
}

func TestGetProvision_NotFound(t *testing.T) {
	fred, _ := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	_, err := b.GetProvision(context.Background(), "ghost")
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestGetProvision_MapAndDiagnostics_AgreeOnWire(t *testing.T) {
	// Architect-required invariant: for the same lease, the in-memory map
	// path and the diagnostics-fallback path must serialize to the same
	// wire shape. Asserts that Option 2's FailCount fix didn't accidentally
	// reintroduce divergence on any other field.
	//
	// CreatedAt is excluded from the byte-equal invariant: map path returns
	// the lease-intake time (p.CreatedAt), diagnostics-fallback returns the
	// diagnostic-record write time (diag.CreatedAt, which is also the TTL
	// cleanup key for shared.DiagnosticsStore — see shared/diagnostics.go
	// RemoveOlderThan). The shared schema collapses both meanings into a
	// single JSON field; resolving that (likely via a LeaseCreatedAt field
	// on DiagnosticEntry) is tracked separately and intentionally out of
	// scope for ENG-133.
	fred, ch := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("lease-1", fred.URL)))
	_ = awaitCallback(t, ch)

	mapInfo, err := b.GetProvision(context.Background(), "lease-1")
	require.NoError(t, err)
	mapJSON, err := json.Marshal(mapInfo)
	require.NoError(t, err)

	require.NoError(t, b.Deprovision(context.Background(), "lease-1"))

	diagInfo, err := b.GetProvision(context.Background(), "lease-1")
	require.NoError(t, err)
	diagJSON, err := json.Marshal(diagInfo)
	require.NoError(t, err)

	var mapDoc, diagDoc map[string]any
	require.NoError(t, json.Unmarshal(mapJSON, &mapDoc))
	require.NoError(t, json.Unmarshal(diagJSON, &diagDoc))
	delete(mapDoc, "created_at")
	delete(diagDoc, "created_at")

	assert.Equal(t, mapDoc, diagDoc,
		"map path and diagnostics fallback must agree on every wire field except created_at "+
			"(see test comment for rationale)")
}

// --- List / Lookup -------------------------------------------------------

func TestListProvisions_EmptyReturnsEmptyNotNil(t *testing.T) {
	fred, _ := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	list, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, list, "ListProvisions must return non-nil empty slice so JSON serializes as [] not null")
	assert.Empty(t, list)
}

func TestListProvisions_ReflectsProvisions(t *testing.T) {
	fred, ch := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("lease-1", fred.URL)))
	_ = awaitCallback(t, ch)
	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("lease-2", fred.URL)))
	_ = awaitCallback(t, ch)

	list, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, list, 2)
	uuids := []string{list[0].LeaseUUID, list[1].LeaseUUID}
	assert.ElementsMatch(t, []string{"lease-1", "lease-2"}, uuids)
}

func TestLookupProvisions_FiltersToRequested(t *testing.T) {
	fred, ch := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("lease-1", fred.URL)))
	_ = awaitCallback(t, ch)
	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("lease-2", fred.URL)))
	_ = awaitCallback(t, ch)

	// "lease-3" is not provisioned — silently omitted from the result per
	// the handler's "200 with empty slice vs 404" contract.
	list, err := b.LookupProvisions(context.Background(), []string{"lease-1", "lease-3"})
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "lease-1", list[0].LeaseUUID)
}

// --- Sentinel-error stubs ------------------------------------------------

func TestStubMethods_ReturnErrNotProvisioned(t *testing.T) {
	// Every read/write path beyond Provision/Deprovision/GetProvision/
	// List/Lookup/Stats returns backend.ErrNotProvisioned in the ENG-133
	// scaffold. Each is mapped to 404 by the HTTP layer. ENG-134+ wires
	// real K8s flows.
	fred, _ := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)
	ctx := context.Background()

	t.Run("GetInfo", func(t *testing.T) {
		info, err := b.GetInfo(ctx, "lease-1")
		assert.Nil(t, info)
		assert.ErrorIs(t, err, backend.ErrNotProvisioned)
	})

	t.Run("GetLogs", func(t *testing.T) {
		logs, err := b.GetLogs(ctx, "lease-1", 100)
		assert.Nil(t, logs)
		assert.ErrorIs(t, err, backend.ErrNotProvisioned)
	})

	t.Run("Restart", func(t *testing.T) {
		err := b.Restart(ctx, backend.RestartRequest{LeaseUUID: "lease-1", CallbackURL: fred.URL})
		assert.ErrorIs(t, err, backend.ErrNotProvisioned)
	})

	t.Run("Update", func(t *testing.T) {
		err := b.Update(ctx, backend.UpdateRequest{
			LeaseUUID:   "lease-1",
			CallbackURL: fred.URL,
			Payload:     []byte("eyJpbWFnZSI6Im5naW54In0="),
		})
		assert.ErrorIs(t, err, backend.ErrNotProvisioned)
	})

	// ReconcileCustomDomain is intentionally NOT in this group: it returns
	// nil (no-op) rather than ErrNotProvisioned to match docker-backend's
	// contract for unhandled / ingress-disabled leases. See
	// TestReconcileCustomDomain_NoOpForUnhandledLease for the positive
	// assertion.

	t.Run("GetReleases", func(t *testing.T) {
		releases, err := b.GetReleases(ctx, "lease-1")
		assert.Nil(t, releases)
		assert.ErrorIs(t, err, backend.ErrNotProvisioned)
	})
}

func TestRunStubProvisioner_SuppressesCallbackAfterDeprovision(t *testing.T) {
	// Regression test for the round-6 Copilot finding: runStubProvisioner
	// must not fire a stale status=failed callback (or persist a stale
	// diagnostic) when a concurrent Deprovision has already removed the
	// entry from b.provisions.
	//
	// Race scenario:
	//   Provision()   -> entry inserted, goroutine spawned but unscheduled.
	//   Deprovision() -> entry deleted from map.
	//   <worker runs> -> p pointer is still alive but map no longer holds
	//                    it; without the suppression check it would mutate
	//                    and send a failed callback for a lease Fred just
	//                    tore down.
	//
	// We seed an entry directly into the map, delete it (simulating the
	// fast Deprovision), then invoke runStubProvisioner synchronously so
	// the test is deterministic instead of racing the Go scheduler.
	fred, callbacks := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	p := &provision{
		LeaseUUID:    "lease-deleted",
		Tenant:       "manifest1test",
		ProviderUUID: "prov-1",
		Status:       backend.ProvisionStatusProvisioning,
		CallbackURL:  fred.URL,
		CreatedAt:    time.Now(),
	}
	b.provisionsMu.Lock()
	b.provisions[p.LeaseUUID] = p
	b.provisionsMu.Unlock()
	b.wg.Add(1) // runStubProvisioner's defer wg.Done() needs a paired Add.

	// Deprovision the entry BEFORE the worker runs.
	require.NoError(t, b.Deprovision(context.Background(), p.LeaseUUID))

	// Run the worker synchronously. With the suppression check it must
	// see the missing map entry and exit silently.
	b.runStubProvisioner(p)

	// Assert no callback was sent.
	select {
	case got := <-callbacks:
		t.Fatalf("expected no callback after deprovision, got: %+v", got)
	case <-time.After(200 * time.Millisecond):
		// Expected: callback channel stays empty.
	}

	// Assert no diagnostic was persisted either.
	diag, err := b.diagnosticsStore.Get(p.LeaseUUID)
	require.NoError(t, err)
	assert.Nil(t, diag, "no diagnostic should be persisted for a deprovisioned lease")
}

func TestReconcileCustomDomain_NoOpForUnhandledLease(t *testing.T) {
	// Per the backendService contract, ReconcileCustomDomain must be a
	// no-op (return nil) for leases the backend doesn't manage and for
	// scenarios where ingress is disabled. Docker-backend follows this
	// (internal/backend/docker/reconcile_custom_domain.go early-returns
	// nil for missing / non-ready provisions and ingress.Enabled=false).
	//
	// In the ENG-133 scaffold ingress is rejected at config time so EVERY
	// call should return nil. Verifies the contract so Fred's reconciler
	// doesn't see 404 on every tick per active lease.
	fred, callbacks := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)
	ctx := context.Background()
	items := []backend.LeaseItem{
		{SKU: "k3s-small", Quantity: 1, ServiceName: "web", CustomDomain: "foo.example.com"},
	}

	// Lease not present in the map.
	err := b.ReconcileCustomDomain(ctx, "ghost-lease", items)
	assert.NoError(t, err, "ReconcileCustomDomain must no-op (nil) for unhandled leases")

	// Lease present (provisioned, then flipped to failed by the stub).
	require.NoError(t, b.Provision(ctx, newProvisionRequest("lease-rcd", fred.URL)))
	_ = awaitCallback(t, callbacks)
	err = b.ReconcileCustomDomain(ctx, "lease-rcd", items)
	assert.NoError(t, err, "ReconcileCustomDomain must no-op (nil) even for present leases while ingress is disabled")
}

// --- Stats ---------------------------------------------------------------

func TestStats_ReturnsPoolSnapshot(t *testing.T) {
	// ENG-133 stub never allocates — pool stays at totals, AllocationCount
	// is 0, Available* == Total*. validConfig() carries the defaults:
	// 8.0 CPU cores, 16384 MB memory, 102400 MB disk.
	fred, _ := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	stats := b.Stats()
	assert.InDelta(t, 8.0, stats.TotalCPU, 0.001)
	assert.Equal(t, int64(16384), stats.TotalMemoryMB)
	assert.Equal(t, int64(102400), stats.TotalDiskMB)
	assert.Equal(t, 0, stats.AllocationCount)
	assert.InDelta(t, 0.0, stats.AllocatedCPU, 0.001)
	assert.Equal(t, int64(0), stats.AllocatedMemoryMB)
	assert.Equal(t, int64(0), stats.AllocatedDiskMB)
	assert.InDelta(t, 8.0, stats.AvailableCPU(), 0.001)
	assert.Equal(t, int64(16384), stats.AvailableMemoryMB())
	assert.Equal(t, int64(102400), stats.AvailableDiskMB())
}

// --- Task #19 regression tests --------------------------------------------

func TestGetProvision_NoRace_UnderConcurrentProvision(t *testing.T) {
	// Task #19 Fix 2 regression guard. The pre-fix GetProvision dropped
	// the RLock before reading p.Status / p.FailCount / p.LastError —
	// which runStubProvisioner mutates under the write lock. This test
	// hammers the same lease UUID from a Provision writer and a
	// GetProvision reader concurrently, with NO channel-sync between
	// the pair (intentional: T7b's existing GetProvision tests use
	// awaitCallback's channel-sync happens-before, which masks the race
	// because the reader never observes the writer mid-flight).
	//
	// Pre-fix: race detector trips on the unsynchronized field reads.
	// Post-fix: reads occur under RLock — race detector clean.
	//
	// N=50 pairs run for up to 2 seconds — empirically reliable for
	// triggering the race window pre-fix while keeping wall clock under
	// 3s post-fix.

	// Drain-only Fred handler: outbound callbacks from runStubProvisioner
	// must not block on a buffered channel. HMAC verification isn't
	// relevant to the race fix (the race is in-process map access, not
	// callback delivery), so we skip it here to keep the test focused.
	fred := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(fred.Close)

	b := newBackendForTest(t, fred.URL)

	const N = 50
	var wg sync.WaitGroup
	wg.Add(2 * N)

	for i := 0; i < N; i++ {
		uuid := fmt.Sprintf("lease-race-%d", i)

		go func(uuid string) {
			defer wg.Done()
			_ = b.Provision(context.Background(), newProvisionRequest(uuid, fred.URL))
		}(uuid)

		go func(uuid string) {
			defer wg.Done()
			// Tight loop until deadline. Each iteration potentially
			// observes a different snapshot of the writer's state —
			// pre-fix, any iteration could trip the race detector.
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				_, _ = b.GetProvision(context.Background(), uuid)
			}
		}(uuid)
	}

	wg.Wait()

	// Sanity check: by the time wg.Wait returns, the writer goroutine
	// for lease-race-0 has at least called Provision, which inserts the
	// in-memory entry. GetProvision should surface it (whether the stub
	// goroutine has finished mutating or not — both states are valid map
	// hits). Doesn't verify the race fix itself (that's the race
	// detector's job); just ensures the test setup actually exercised
	// the contended code path and didn't no-op.
	info, err := b.GetProvision(context.Background(), "lease-race-0")
	require.NoError(t, err)
	require.NotNil(t, info)
}

func TestListProvisions_PopulatesFailCount(t *testing.T) {
	// Task #19 Fix 3 regression guard. Pre-fix ListProvisions omitted
	// FailCount from the returned ProvisionInfo struct literal,
	// surfacing fail_count=0 on the wire for failed provisions —
	// contradicting GetProvision's map path which carries FailCount=1.
	// Post-fix: ListProvisions populates FailCount from p.FailCount
	// alongside the other fields, agreeing with GetProvision on the
	// wire shape.
	fred, ch := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("lease-1", fred.URL)))
	_ = awaitCallback(t, ch)

	list, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, 1, list[0].FailCount,
		"ListProvisions must populate FailCount from the in-memory record")
}

func TestLookupProvisions_PopulatesFailCount(t *testing.T) {
	// Task #19 Fix 3 regression guard for the filtered list path.
	// Same shape as TestListProvisions_PopulatesFailCount: pre-fix
	// LookupProvisions omitted FailCount in its struct literal;
	// post-fix it carries FailCount: p.FailCount.
	fred, ch := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("lease-1", fred.URL)))
	_ = awaitCallback(t, ch)

	list, err := b.LookupProvisions(context.Background(), []string{"lease-1"})
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, 1, list[0].FailCount,
		"LookupProvisions must populate FailCount from the in-memory record")
}
