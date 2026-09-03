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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/callbackurl"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/hmacauth"
)

// zeroBackoff eliminates retry delays so tests don't pay 1s/5s waits on
// failed callback deliveries. Mirrors docker/provision_test.go:64.
var zeroBackoff = [shared.CallbackMaxAttempts]time.Duration{}

const testCallbackDeliveryTimeout = 5 * time.Second

// testCallbackSecret is the HMAC secret the fake Fred receiver uses to
// verify inbound callbacks. 32 chars to satisfy Config.Validate's floor.
const testCallbackSecret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

// testK3sProviderUUID is a canonical provider identity for fixtures that pass
// through the real durable operation-intent journal. Tests that seed only an
// in-memory provision may still use memorable strings when identity is not the
// behavior under test.
const testK3sProviderUUID = "bc19c267-ddbd-47c8-84ca-c944b9a9c74f"

func TestBackendConstructionRejectsMissingDependencies(t *testing.T) {
	_, err := newBackend(context.Background(), Config{}, nil, testK3sStorageIdentity{})
	require.ErrorContains(t, err, "logger")

	_, err = newBackend(context.Background(), Config{}, slog.Default(), nil)
	require.ErrorContains(t, err, "identity resolver")
}

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

	b, err := newBackendWithTestIdentity(cfg, slog.Default())
	require.NoError(t, err)
	bindK3sTestStorageIdentity(t, b)

	rebuildCallbackSender(b)
	t.Cleanup(func() { _ = b.Stop() })
	return b
}

// rebuildCallbackSender swaps b.callbackSender for one configured with
// zeroBackoff and a scaled per-request attempt timeout. Same-package access lets
// us replace a production field without production carrying a seam for
// it; the client is a local here, because a *Backend field only tests
// read would be test scaffolding in a production struct (ENG-765).
func rebuildCallbackSender(b *Backend) {
	httpClient := &http.Client{}
	b.callbackSender = shared.MustNewCallbackSender(shared.CallbackSenderConfig{
		Store:           b.callbackStore,
		HTTPClient:      httpClient,
		Secret:          string(b.cfg.CallbackSecret),
		StorageIdentity: b.storageIdentity,
		BeforeDelivery:  b.VerifyStorageIdentity,
		BeforeReplay:    b.VerifyStorageIdentity,
		Logger:          b.logger,
		StopCtx:         b.stopCtx,
		Backoff:         &zeroBackoff,
		DeliveryTimeout: testCallbackDeliveryTimeout,
	})
}

// startK3sCallbackReplayForTest opts a fixture into the same tracked delivery
// lifecycle production Start uses. Most state-only tests deliberately leave
// replay stopped so they can inspect the durable outbox without racing HTTP.
func startK3sCallbackReplayForTest(b *Backend) {
	b.wg.Go(b.callbackSender.RunReplayLoop)
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
		if err := hmacauth.Verify(testCallbackSecret, r.Method, r.URL.RequestURI(), body, sig, 5*time.Minute); err != nil {
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

func testProvisionCallbackURL(baseURL string) string {
	return baseURL + callbackurl.ProvisionPath
}

// newProvisionRequest is a convenience constructor for tests. Callers supply
// a fake Fred base URL; the helper builds the same canonical callback endpoint
// that providerd supplies in production.
func newProvisionRequest(leaseUUID, callbackBaseURL string) backend.ProvisionRequest {
	return backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "manifest1test",
		ProviderUUID: testK3sProviderUUID,
		Items:        []backend.LeaseItem{{SKU: "k3s-small", Quantity: 1}},
		Payload:      []byte(`{"image":"example.invalid/app:1"}`),
		CallbackURL: testProvisionCallbackURL(callbackBaseURL) +
			"?operation_id=" + uuid.NewString(),
	}
}

// newTestProvision builds a provision struct with ctx/cancel populated
// against b.stopCtx, matching the invariant that production Provision
// maintains. Tests that seed b.provisions directly must use this so
// Deprovision's existing.cancel() and runStubProvisioner's ctx.Err()
// checks behave correctly.
func newTestProvision(b *Backend, leaseUUID, callbackURL string) *provision {
	ctx, cancel := context.WithCancel(b.stopCtx)
	return &provision{
		LeaseUUID:    leaseUUID,
		Tenant:       "manifest1test",
		ProviderUUID: "prov-1",
		Status:       backend.ProvisionStatusProvisioning,
		CallbackURL:  callbackURL,
		CreatedAt:    time.Now(),
		ctx:          ctx,
		cancel:       cancel,
	}
}

func seedK3sProvisionIntentForTest(
	t *testing.T, b *Backend, leaseUUID, callbackBaseURL string,
) string {
	t.Helper()
	bindK3sTestStorageIdentity(t, b)
	callbackURL := testProvisionCallbackURL(callbackBaseURL) +
		"?operation_id=6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	items := []backend.LeaseItem{{SKU: "k3s-small", ServiceName: "app", Quantity: 1}}
	_, err = b.callbackStore.BeginOperationIntent(shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentProvision,
		LeaseUUID:            leaseUUID,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleURL,
		Backend:              b.cfg.Name,
		BackendStorageID:     b.storageIdentity,
		Tenant:               "manifest1test",
		ProviderUUID:         testK3sProviderUUID,
		Items:                items,
		ResourceProfiles:     testK3sResourceProfiles(t, b, items),
		Manifest:             []byte(`{"services":{"app":{"image":"example.invalid/app:1"}}}`),
	})
	require.NoError(t, err)
	return callbackURL
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

// TestNewCallbackHTTPClient covers the callback client's construction,
// including the CallbackInsecureSkipVerify branch — which had no test at
// all: k3s/config_test.go covers the *validation* rule that rejects the
// flag in production mode, never the transport it wires up.
//
// The client is a local inside New (ENG-765 removed the Backend field
// that used to shadow it), so this function is the seam.
func TestNewCallbackHTTPClient(t *testing.T) {
	t.Run("verification on by default", func(t *testing.T) {
		c := newCallbackHTTPClient(validConfig(), slog.Default())
		require.NotNil(t, c)
		assert.Zero(t, c.Timeout,
			"CallbackSender's per-request context must be the sole timeout authority")
		assert.Nil(t, c.Transport,
			"default client must use the stdlib transport, which verifies TLS")
	})

	t.Run("insecure skip verify wires an unverifying transport", func(t *testing.T) {
		cfg := validConfig()
		cfg.CallbackInsecureSkipVerify = true

		c := newCallbackHTTPClient(cfg, slog.Default())
		require.NotNil(t, c)
		assert.Zero(t, c.Timeout,
			"CallbackSender's per-request context must be the sole timeout authority")
		tr, ok := c.Transport.(*http.Transport)
		require.True(t, ok, "transport must be an *http.Transport")
		require.NotNil(t, tr.TLSClientConfig)
		assert.True(t, tr.TLSClientConfig.InsecureSkipVerify)
	})
}

func TestBackend_Start_Succeeds(t *testing.T) {
	fred, _ := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)
	bindK3sTestStorageIdentity(t, b)
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
	const (
		callbackID = "550e8400-e29b-41d4-a716-446655440000"
		otherID    = "123e4567-e89b-42d3-a456-426614174000"
	)

	tests := []struct {
		name    string
		req     backend.ProvisionRequest
		wantMsg string
	}{
		{
			name:    "empty lease_uuid",
			req:     backend.ProvisionRequest{CallbackURL: testProvisionCallbackURL(fred.URL), Items: []backend.LeaseItem{{SKU: "k3s-small", Quantity: 1}}},
			wantMsg: "lease_uuid is required",
		},
		{
			name:    "empty callback_url",
			req:     backend.ProvisionRequest{LeaseUUID: "550e8400-e29b-41d4-a716-446655440000", Items: []backend.LeaseItem{{SKU: "k3s-small", Quantity: 1}}},
			wantMsg: "callback_url is required",
		},
		{
			name: "lifecycle authority in operation callback_url",
			req: backend.ProvisionRequest{
				LeaseUUID:   "550e8400-e29b-41d4-a716-446655440000",
				CallbackURL: testProvisionCallbackURL(fred.URL) + "?lifecycle_id=" + callbackID,
				Items:       []backend.LeaseItem{{SKU: "k3s-small", Quantity: 1}},
			},
			wantMsg: "invalid lifecycle callback URL",
		},
		{
			name: "mismatched explicit lifecycle callback_url",
			req: backend.ProvisionRequest{
				LeaseUUID:            "550e8400-e29b-41d4-a716-446655440000",
				CallbackURL:          testProvisionCallbackURL(fred.URL) + "?operation_id=" + callbackID,
				LifecycleCallbackURL: testProvisionCallbackURL(fred.URL) + "?lifecycle_id=" + otherID,
				Items:                []backend.LeaseItem{{SKU: "k3s-small", Quantity: 1}},
			},
			wantMsg: "invalid lifecycle callback URL",
		},
		{
			name:    "empty items",
			req:     backend.ProvisionRequest{LeaseUUID: "550e8400-e29b-41d4-a716-446655440000", CallbackURL: testProvisionCallbackURL(fred.URL)},
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

func TestProvisionPersistsAndReportsLifecycleGeneration(t *testing.T) {
	const id = "550e8400-e29b-41d4-a716-446655440000"
	b := newBackendForTest(t, "")
	operationURL := "https://fred.example" + callbackurl.ProvisionPath + "?operation_id=" + id
	lifecycleURL := "https://fred.example" + callbackurl.ProvisionPath + "?lifecycle_id=" + id

	require.NoError(t, b.Provision(t.Context(), backend.ProvisionRequest{
		LeaseUUID:            id,
		Tenant:               "tenant-1",
		ProviderUUID:         testK3sProviderUUID,
		CallbackURL:          operationURL,
		LifecycleCallbackURL: lifecycleURL,
		Items:                []backend.LeaseItem{{SKU: "k3s-small", Quantity: 1}},
		Payload:              []byte(`{"image":"example.invalid/app:1"}`),
	}))

	b.provisionsMu.RLock()
	stored := b.provisions[id]
	require.NotNil(t, stored)
	assert.Equal(t, operationURL, stored.CallbackURL)
	assert.Equal(t, lifecycleURL, stored.LifecycleCallbackURL)
	b.provisionsMu.RUnlock()

	info, err := b.GetProvision(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, &backend.LifecycleGenerationObservation{
		Kind: backend.LifecycleGenerationTyped,
		ID:   id,
	}, info.LifecycleGeneration)
}

func TestProvisionToInfoMissingCallbackHalfIsUnknown(t *testing.T) {
	info := provisionToInfo(&provision{
		LeaseUUID:   "legacy-record",
		Tenant:      "tenant-a",
		CallbackURL: "https://fred.example/callbacks/provision",
	})
	assert.Equal(t, "tenant-a", info.Tenant)
	assert.Equal(t, &backend.LifecycleGenerationObservation{
		Kind: backend.LifecycleGenerationUnknown,
	}, info.LifecycleGeneration)
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
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"

	b.provisionsMu.Lock()
	b.provisions[leaseUUID] = newTestProvision(b, leaseUUID, testProvisionCallbackURL(fred.URL))
	b.provisionsMu.Unlock()

	err := b.Provision(context.Background(), newProvisionRequest(leaseUUID, fred.URL))
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
	startK3sCallbackReplayForTest(b)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("550e8400-e29b-41d4-a716-446655440000", fred.URL)))
	_ = awaitCallback(t, ch)
	require.Eventually(t, func() bool {
		pending, pendingErr := b.callbackStore.ListPending()
		intents, intentErr := b.callbackStore.ListOperationIntents()
		return pendingErr == nil && intentErr == nil && len(pending) == 0 && len(intents) == 0
	}, time.Second, time.Millisecond,
		"the first synchronous 2xx must precisely remove its completion before a new generation")

	info, err := b.GetProvision(context.Background(), "550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	require.Equal(t, backend.ProvisionStatusFailed, info.Status)
	require.Equal(t, 1, info.FailCount)

	// Retry. Should succeed and inherit the prior FailCount.
	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("550e8400-e29b-41d4-a716-446655440000", fred.URL)))
	_ = awaitCallback(t, ch)

	info, err = b.GetProvision(context.Background(), "550e8400-e29b-41d4-a716-446655440000")
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
	startK3sCallbackReplayForTest(b)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("550e8400-e29b-41d4-a716-446655440000", fred.URL)))

	p := awaitCallback(t, ch)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", p.LeaseUUID)
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
	const missingLease = "550e8400-e29b-41d4-a716-446655440000"
	require.NoError(t, b.Deprovision(context.Background(), missingLease))
	require.NoError(t, b.Deprovision(context.Background(), missingLease))
}

func TestDeprovision_RemovesFromMap_AfterProvision(t *testing.T) {
	fred, ch := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)
	startK3sCallbackReplayForTest(b)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("550e8400-e29b-41d4-a716-446655440000", fred.URL)))
	_ = awaitCallback(t, ch)

	list, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", list[0].LeaseUUID)

	require.NoError(t, b.Deprovision(context.Background(), "550e8400-e29b-41d4-a716-446655440000"))

	list, err = b.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.Empty(t, list)
}

// An exact operation completion is causal evidence for Fred's durable
// write-ahead placement Attempt. Deprovision must not erase it merely because
// the backend's in-memory lease record is already gone.
func TestDeprovision_PreservesPendingExactCallback(t *testing.T) {
	fred, _ := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"

	// Resolve a real write-ahead intent to simulate "exact callback persisted,
	// delivery hasn't succeeded yet". Bypasses Provision so goroutine timing
	// stays out of the test without manufacturing causal evidence directly.
	callbackURL := seedK3sProvisionIntentForTest(t, b, leaseUUID, fred.URL)
	intents, err := b.callbackStore.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	entry, err := b.callbackStore.ResolveOperationIntent(
		intents[0], backend.CallbackStatusFailed, "not implemented",
	)
	require.NoError(t, err)
	require.Equal(t, callbackURL, entry.CallbackURL)

	pending, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "precondition: callback store has the seeded entry")

	require.NoError(t, b.Deprovision(context.Background(), leaseUUID))

	pending, err = b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, entry.LeaseUUID, pending[0].LeaseUUID)
	assert.Equal(t, shared.CallbackDeliveryKindOperation, pending[0].DeliveryKind)
}

func TestDeprovision_DoesNotWaitForInFlightCallbackDelivery(t *testing.T) {
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCallback) }) }
	defer release()
	var once sync.Once
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		once.Do(func() {
			close(callbackStarted)
			<-releaseCallback
		})
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(callbackServer.Close)
	b := newBackendForTest(t, callbackServer.URL)
	startK3sCallbackReplayForTest(b)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("550e8400-e29b-41d4-a716-446655440000", callbackServer.URL)))
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("stub provision callback did not begin delivery")
	}

	deprovisionDone := make(chan error, 1)
	go func() { deprovisionDone <- b.Deprovision(context.Background(), "550e8400-e29b-41d4-a716-446655440000") }()
	select {
	case err := <-deprovisionDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("deprovision waited for unrelated callback HTTP delivery")
	}

	pending, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1,
		"deprovision must preserve the original completion without manufacturing a duplicate")
	for _, entry := range pending {
		assert.Equal(t, shared.CallbackDeliveryKindOperation, entry.DeliveryKind)
		assert.Equal(t, backend.CallbackStatusFailed, entry.Status)
	}
	provisions, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	assert.Empty(t, provisions)
	release()
}

// --- GetProvision contract (architect's required test set) ---------------

func TestGetProvision_FromMap_AfterStubFailure(t *testing.T) {
	fred, ch := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)
	startK3sCallbackReplayForTest(b)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("550e8400-e29b-41d4-a716-446655440000", fred.URL)))
	_ = awaitCallback(t, ch) // signals goroutine has flipped state + fired callback

	info, err := b.GetProvision(context.Background(), "550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", info.LeaseUUID)
	assert.Equal(t, backend.ProvisionStatusFailed, info.Status)
	// ENG-508: the map path surfaces the curated tenant-safe failure signal
	// (Reason/Message); the verbose operator LastError is no longer on the wire.
	assert.Equal(t, backend.ReasonInternal, info.Reason)
	assert.Equal(t, "not implemented", info.Message)
	// Option 2 patch: in-memory record carries FailCount=1 alongside Status.
	// Pre-patch this would have returned 0 from the map path; the assertion
	// guards against regression.
	assert.Equal(t, 1, info.FailCount)
}

func TestGetProvision_FromDiagnostics_AfterDeprovision(t *testing.T) {
	fred, ch := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)
	startK3sCallbackReplayForTest(b)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("550e8400-e29b-41d4-a716-446655440000", fred.URL)))
	_ = awaitCallback(t, ch)
	require.NoError(t, b.Deprovision(context.Background(), "550e8400-e29b-41d4-a716-446655440000"))

	// Diagnostics survive Deprovision (cfg.DiagnosticsMaxAge handles
	// eventual cleanup) so post-teardown queries can still surface the
	// failure cause.
	info, err := b.GetProvision(context.Background(), "550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", info.LeaseUUID)
	assert.Equal(t, testK3sProviderUUID, info.ProviderUUID)
	// Fallback synthesizes Status=Failed because shared.DiagnosticEntry
	// is failure-only by construction (only the runStubProvisioner failure
	// path calls diagnosticsStore.Store).
	assert.Equal(t, backend.ProvisionStatusFailed, info.Status)
	// ENG-508: the diagnostics fallback surfaces the same curated Reason/Message
	// as the live map path, so both read boundaries agree on the wire shape;
	// the verbose operator LastError is no longer on the wire.
	assert.Equal(t, backend.ReasonInternal, info.Reason)
	assert.Equal(t, "not implemented", info.Message)
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
	startK3sCallbackReplayForTest(b)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("550e8400-e29b-41d4-a716-446655440000", fred.URL)))
	_ = awaitCallback(t, ch)

	mapInfo, err := b.GetProvision(context.Background(), "550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	mapJSON, err := json.Marshal(mapInfo)
	require.NoError(t, err)

	require.NoError(t, b.Deprovision(context.Background(), "550e8400-e29b-41d4-a716-446655440000"))

	diagInfo, err := b.GetProvision(context.Background(), "550e8400-e29b-41d4-a716-446655440000")
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
	startK3sCallbackReplayForTest(b)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("550e8400-e29b-41d4-a716-446655440000", fred.URL)))
	_ = awaitCallback(t, ch)
	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("6ba7b811-9dad-41d1-80b4-00c04fd430c8", fred.URL)))
	_ = awaitCallback(t, ch)

	list, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	require.Len(t, list, 2)
	uuids := []string{list[0].LeaseUUID, list[1].LeaseUUID}
	assert.ElementsMatch(t, []string{"550e8400-e29b-41d4-a716-446655440000", "6ba7b811-9dad-41d1-80b4-00c04fd430c8"}, uuids)
}

func TestLookupProvisions_FiltersToRequested(t *testing.T) {
	fred, ch := startFakeFred(t)
	b := newBackendForTest(t, fred.URL)
	startK3sCallbackReplayForTest(b)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("550e8400-e29b-41d4-a716-446655440000", fred.URL)))
	_ = awaitCallback(t, ch)
	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("6ba7b811-9dad-41d1-80b4-00c04fd430c8", fred.URL)))
	_ = awaitCallback(t, ch)

	// "lease-3" is not provisioned — silently omitted from the result per
	// the handler's "200 with empty slice vs 404" contract.
	list, err := b.LookupProvisions(context.Background(), []string{"550e8400-e29b-41d4-a716-446655440000", "lease-3"})
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", list[0].LeaseUUID)
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
		info, err := b.GetInfo(ctx, "550e8400-e29b-41d4-a716-446655440000")
		assert.Nil(t, info)
		assert.ErrorIs(t, err, backend.ErrNotProvisioned)
	})

	t.Run("GetLogs", func(t *testing.T) {
		logs, err := b.GetLogs(ctx, "550e8400-e29b-41d4-a716-446655440000", 100)
		assert.Nil(t, logs)
		assert.ErrorIs(t, err, backend.ErrNotProvisioned)
	})

	t.Run("Restart", func(t *testing.T) {
		err := b.Restart(ctx, backend.RestartRequest{LeaseUUID: "550e8400-e29b-41d4-a716-446655440000", CallbackURL: testProvisionCallbackURL(fred.URL)})
		assert.ErrorIs(t, err, backend.ErrNotProvisioned)
	})

	t.Run("Update", func(t *testing.T) {
		err := b.Update(ctx, backend.UpdateRequest{
			LeaseUUID:   "550e8400-e29b-41d4-a716-446655440000",
			CallbackURL: testProvisionCallbackURL(fred.URL),
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
		releases, err := b.GetReleases(ctx, "550e8400-e29b-41d4-a716-446655440000")
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
	b := newBackendForTest(t, "")

	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	callbackURL := seedK3sProvisionIntentForTest(t, b, leaseUUID, "https://fred.example")
	p := newTestProvision(b, leaseUUID, callbackURL)
	b.provisionsMu.Lock()
	b.provisions[p.LeaseUUID] = p
	b.provisionsMu.Unlock()
	b.wg.Add(1) // runStubProvisioner's defer wg.Done() needs a paired Add.

	// Deprovision the entry BEFORE the worker runs.
	require.NoError(t, b.Deprovision(context.Background(), p.LeaseUUID))

	// Run the worker synchronously. With the suppression check it must
	// see the missing map entry and exit silently.
	b.runStubProvisioner(p)

	// The ownership check, specifically. Both side-effect assertions below
	// are also satisfied by the two ctx.Err() checkpoints — Deprovision
	// cancels before it deletes, so a worker that skipped the ownership
	// check would still be stopped one phase later, and this test passed
	// with that check deleted. The status flip is the one observable the
	// checkpoints do NOT guard: it happens under provisionsMu, before
	// either of them. Measured on origin/main before ENG-765: deleting the
	// ownership check left this test green without this assertion.
	assert.Equal(t, backend.ProvisionStatusProvisioning, p.Status,
		"a worker that no longer owns the record must not flip its status")

	pending, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, p.CallbackURL, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, "provision canceled by deprovision", pending[0].Error)

	// Assert no diagnostic was persisted either.
	diag, err := b.diagnosticsStore.Get(p.LeaseUUID)
	require.NoError(t, err)
	assert.Nil(t, diag, "no diagnostic should be persisted for a deprovisioned lease")
}

// TestRunStubProvisioner_SuppressesCallback_PostUnlockPreDiagnostic
// covers ENG-189 case (b): a Deprovision that wins provisionsMu
// BETWEEN the worker's unlock and the diagnostic store call must
// cancel the per-lease ctx, and the worker's checkpoint-1 ctx.Err()
// check must observe the cancellation and abort before persisting
// the diagnostic OR sending the callback.
//
// Determinism: runStubProvisioner's phases are driven in order on this
// goroutine, with the real Deprovision run at exactly the interleaving
// point under test. No worker goroutine, no pause channels, and no
// test-only hook field on Backend (ENG-765) — the interleaving is
// expressed by WHERE Deprovision is called, which is what the hook
// fields used to simulate.
func TestRunStubProvisioner_SuppressesCallback_PostUnlockPreDiagnostic(t *testing.T) {
	b := newBackendForTest(t, "")

	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	callbackURL := seedK3sProvisionIntentForTest(t, b, leaseUUID, "https://fred.example")
	p := newTestProvision(b, leaseUUID, callbackURL)
	b.provisionsMu.Lock()
	b.provisions[p.LeaseUUID] = p
	b.provisionsMu.Unlock()

	// Phase 1 runs to completion: the worker owns the record and has
	// released provisionsMu.
	f, ok := b.claimStubFailure(p)
	require.True(t, ok, "worker must claim a record it still owns")

	// Deprovision wins the lock here — after the unlock, before the
	// diagnostic write. It cancels the lease ctx captured in f.
	require.NoError(t, b.Deprovision(context.Background(), p.LeaseUUID))
	require.Error(t, f.leaseCtx.Err(), "Deprovision must cancel the lease ctx")

	// Checkpoint 1 must observe the cancellation and skip the write; the
	// callback phase re-checks and must skip too.
	require.True(t, b.persistStubDiagnostic(f),
		"checkpoint 1 must report the lease as canceled")
	b.sendStubFailureCallback(f)

	// The canceled worker must not persist diagnostics or emit its stale
	// not-implemented callback. Deprovision persists one exact cancellation
	// outcome for replay so Fred can refuse the durable attempt.
	diag, err := b.diagnosticsStore.Get(p.LeaseUUID)
	require.NoError(t, err)
	assert.Nil(t, diag,
		"diagnostic must not be persisted when ctx is canceled before the diagnostic write")

	pending, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, p.CallbackURL, pending[0].CallbackURL)
	assert.Equal(t, "provision canceled by deprovision", pending[0].Error)
}

// TestRunStubProvisioner_SuppressesCallback_PostDiagnosticPreCallback
// covers ENG-189 case (c): a Deprovision that wins the lock between
// the diagnostic store call and the callback send must cancel the
// per-lease ctx; the worker's checkpoint-2 ctx.Err() check must
// observe the cancellation and skip SendOperationCallback (and therefore
// also skip the bbolt persist that SendOperationCallback would otherwise do
// before delivery).
//
// The diagnostic IS allowed to persist — it was written before the
// cancel arrived. Asserting that explicitly proves the suppression
// is at checkpoint 2, not at the earlier checkpoint 1.
//
// Like its sibling above, this drives runStubProvisioner's phases in
// order and places the real Deprovision at the interleaving point under
// test, so no test-only hook field is needed (ENG-765).
func TestRunStubProvisioner_SuppressesCallback_PostDiagnosticPreCallback(t *testing.T) {
	b := newBackendForTest(t, "")

	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	callbackURL := seedK3sProvisionIntentForTest(t, b, leaseUUID, "https://fred.example")
	p := newTestProvision(b, leaseUUID, callbackURL)
	b.provisionsMu.Lock()
	b.provisions[p.LeaseUUID] = p
	b.provisionsMu.Unlock()

	f, ok := b.claimStubFailure(p)
	require.True(t, ok, "worker must claim a record it still owns")

	// The diagnostic write happens BEFORE the cancel arrives.
	require.False(t, b.persistStubDiagnostic(f),
		"checkpoint 1 must not report cancellation before Deprovision runs")

	// Deprovision wins the lock here — after the diagnostic write,
	// before the callback send.
	require.NoError(t, b.Deprovision(context.Background(), p.LeaseUUID))
	require.Error(t, f.leaseCtx.Err(), "Deprovision must cancel the lease ctx")

	b.sendStubFailureCallback(f)

	// The diagnostic was written before the cancel arrived, so it survives.
	// This proves the suppression is at checkpoint 2 (callback), not at
	// checkpoint 1 (diagnostic).
	diag, err := b.diagnosticsStore.Get(p.LeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, diag, "diagnostic written before ctx cancel must survive")
	assert.Equal(t, stubProvisionerErrMsg, diag.Error)

	// The worker callback was guarded by ctx.Err() at checkpoint 2. Deprovision
	// leaves the single exact cancellation outcome queued for durable replay.
	pending, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1,
		"callbackStore must contain only the exact cancellation for the canceled lease")
	assert.Equal(t, p.CallbackURL, pending[0].CallbackURL)
	assert.Equal(t, "provision canceled by deprovision", pending[0].Error)
}

// TestRunStubProvisioner_PersistsDiagnosticBeforeCallback pins the ORDER
// of runStubProvisioner's two post-lock phases: the diagnostic write
// completes before the callback is sent.
//
// Why this needs its own test. The two ENG-189 tests above drive the
// phases in an order the TEST chooses, so they prove each checkpoint
// guards what it claims but say nothing about how the production
// composition sequences them. TestProvision_HappyPath asserts only the
// callback payload, so a reorder leaves it green.
//
// The rest of the incumbent coverage is timing-dependent rather than
// absent, which is a weaker claim than "nothing covers it" and the
// accurate one. Measured against a swapped composition:
// TestGetProvision_FromDiagnostics_AfterDeprovision fails 30/30 and
// TestGetProvision_MapAndDiagnostics_AgreeOnWire 29/30. Both race their
// own Deprovision against the worker — they catch a reorder by winning
// that race, not by construction, and the second already loses it once
// in thirty. This test makes the same check deterministic.
//
// Why the order is load-bearing. The callback is what makes Fred
// deprovision. If the send came first, the resulting Deprovision would
// cancel the lease ctx before the diagnostic write, checkpoint 1 would
// suppress it, and GetProvision's diagnostics fallback — a documented
// tenant-facing contract (BACKEND_GUIDE.md, README.md) — would have
// nothing to surface for a failed lease.
//
// The assertion runs INSIDE the handler, synchronously with the POST.
// Checking after awaitCallback returns is exactly what makes the tests
// above timing-dependent: the worker may have written the diagnostic by
// then under either order.
// httptest.NewUnstartedServer lets the Backend be assigned before the
// server goroutine exists, which orders that write ahead of any handler
// read without a mutex.
func TestRunStubProvisioner_PersistsDiagnosticBeforeCallback(t *testing.T) {
	const leaseUUID = "123e4567-e89b-42d3-a456-426614174000"

	var b *Backend
	diagSeen := make(chan bool, 1)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		diag, err := b.diagnosticsStore.Get(leaseUUID)
		select {
		case diagSeen <- err == nil && diag != nil:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	b = newBackendForTest(t, "")
	startK3sCallbackReplayForTest(b)
	srv.Start()

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest(leaseUUID, srv.URL)))

	select {
	case ok := <-diagSeen:
		assert.True(t, ok,
			"the failure diagnostic must already be persisted when the callback is delivered; "+
				"if this fails, the diagnostic write and the callback send have been reordered")
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for callback delivery")
	}
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
	startK3sCallbackReplayForTest(b)
	ctx := context.Background()
	items := []backend.LeaseItem{
		{SKU: "k3s-small", Quantity: 1, ServiceName: "web", CustomDomain: "foo.example.com"},
	}

	// Lease not present in the map.
	err := b.ReconcileCustomDomain(ctx, "ghost-lease", items)
	assert.NoError(t, err, "ReconcileCustomDomain must no-op (nil) for unhandled leases")

	// Lease present (provisioned, then flipped to failed by the stub).
	require.NoError(t, b.Provision(ctx, newProvisionRequest("9c858901-8a57-4791-81fe-4c455b099bc9", fred.URL)))
	_ = awaitCallback(t, callbacks)
	err = b.ReconcileCustomDomain(ctx, "9c858901-8a57-4791-81fe-4c455b099bc9", items)
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
	// hammers each lease from the worker's claimStubFailure writer and a
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

	// Exercise the actual worker's map-mutation phase directly. Provision now
	// commits a durable operation intent, whose serialized bbolt writes and
	// storage-identity checks are unrelated to the provisionsMu race this test
	// pins. Seeding the in-memory records keeps the regression focused and avoids
	// turning a lock-race test into a journal-throughput benchmark.
	b := newBackendForTest(t, "")

	const N = 50
	provisions := make([]*provision, 0, N)
	b.provisionsMu.Lock()
	for i := range N {
		leaseUUID := fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1)
		p := newTestProvision(
			b,
			leaseUUID,
			"https://fred.example"+callbackurl.ProvisionPath+"?operation_id="+leaseUUID,
		)
		b.provisions[leaseUUID] = p
		provisions = append(provisions, p)
	}
	b.provisionsMu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2 * N)

	for _, p := range provisions {
		go func(p *provision) {
			defer wg.Done()
			_, _ = b.claimStubFailure(p)
		}(p)

		go func(leaseUUID string) {
			defer wg.Done()
			// Tight loop until deadline. Each iteration potentially
			// observes a different snapshot of the writer's state —
			// pre-fix, any iteration could trip the race detector.
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				_, _ = b.GetProvision(context.Background(), leaseUUID)
			}
		}(p.LeaseUUID)
	}

	wg.Wait()

	// Sanity check: the worker-side writer actually transitioned the record.
	// The race detector remains the assertion for synchronized field access.
	info, err := b.GetProvision(context.Background(), "00000000-0000-4000-8000-000000000001")
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, backend.ProvisionStatusFailed, info.Status)
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
	startK3sCallbackReplayForTest(b)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("550e8400-e29b-41d4-a716-446655440000", fred.URL)))
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
	startK3sCallbackReplayForTest(b)

	require.NoError(t, b.Provision(context.Background(), newProvisionRequest("550e8400-e29b-41d4-a716-446655440000", fred.URL)))
	_ = awaitCallback(t, ch)

	list, err := b.LookupProvisions(context.Background(), []string{"550e8400-e29b-41d4-a716-446655440000"})
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, 1, list[0].FailCount,
		"LookupProvisions must populate FailCount from the in-memory record")
}
