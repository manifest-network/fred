package k3s

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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

func TestProvision_RejectsAlreadyProvisioned(t *testing.T) {
	fred, ch := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)

	// First Provision succeeds and the stub goroutine fires the failure
	// callback. We await delivery so the in-memory entry is in its final
	// state before attempting the duplicate — though the duplicate check
	// fires on any in-memory entry regardless of status.
	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("lease-1", fred.URL)))
	_ = awaitCallback(t, ch)

	err := b.Provision(context.Background(), newProvisionRequest("lease-1", fred.URL))
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrAlreadyProvisioned)
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
	// is failure-only by construction (provision_stub.go:150-156).
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

	t.Run("ReconcileCustomDomain", func(t *testing.T) {
		err := b.ReconcileCustomDomain(ctx, "lease-1", []backend.LeaseItem{
			{SKU: "k3s-small", Quantity: 1, ServiceName: "web", CustomDomain: "foo.example.com"},
		})
		assert.ErrorIs(t, err, backend.ErrNotProvisioned)
	})

	t.Run("GetReleases", func(t *testing.T) {
		releases, err := b.GetReleases(ctx, "lease-1")
		assert.Nil(t, releases)
		assert.ErrorIs(t, err, backend.ErrNotProvisioned)
	})
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
