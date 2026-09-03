package shared

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/callbackurl"
	"github.com/manifest-network/fred/internal/hmacauth"
)

func callbackStorageID(t *testing.T, value string) backendidentity.ID {
	t.Helper()
	id, err := backendidentity.Parse(value)
	require.NoError(t, err)
	return id
}

func beginCallbackSenderOperationIntent(
	t *testing.T,
	store *CallbackStore,
	leaseUUID string,
	callbackURL string,
	backendName string,
	storageID backendidentity.ID,
) OperationIntentAdmission {
	t.Helper()
	if !storageID.Valid() {
		storageID = callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000")
	}
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	spec := testOperationIntentSpec(t, "callback-sender")
	spec.LeaseUUID = leaseUUID
	spec.CallbackURL = callbackURL
	spec.LifecycleCallbackURL = lifecycleURL
	spec.Backend = backendName
	spec.BackendStorageID = storageID
	admission, err := store.BeginOperationIntent(spec)
	require.NoError(t, err)
	return admission
}

func TestCallbackSenderBindsHMACCoveredPayloadToStorageIdentity(t *testing.T) {
	t.Parallel()

	id := callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000")
	const secret = "callback-storage-identity-test-secret"
	var received backend.CallbackPayload
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		require.NoError(t, hmacauth.Verify(
			secret, req.Method, req.URL.RequestURI(), body,
			req.Header.Get(hmacauth.SignatureHeader), 5*time.Minute,
		))
		require.NoError(t, json.Unmarshal(body, &received))
		return callbackHTTPResponse(http.StatusOK), nil
	})}
	sender := MustNewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient:      client,
		Secret:          secret,
		Logger:          slog.Default(),
		StopCtx:         context.Background(),
		Backoff:         &zeroBackoff,
		StorageIdentity: id,
		BeforeDelivery:  func(context.Context) error { return nil },
	})
	sender.SendOperationCallback(
		testLeaseUUID("storage-bound"),
		"https://fred.example/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000",
		"docker-a", backend.CallbackStatusSuccess, "",
	)
	assert.Equal(t, id.String(), received.BackendStorageID)

	// A v0.13 provider decoding its old DTO ignores the additive JSON field.
	var oldProvider struct {
		LeaseUUID string                 `json:"lease_uuid"`
		Status    backend.CallbackStatus `json:"status"`
	}
	body, err := json.Marshal(received)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(body, &oldProvider))
	assert.Equal(t, received.LeaseUUID, oldProvider.LeaseUUID)
	assert.Equal(t, received.Status, oldProvider.Status)
}

func TestCallbackSenderCopiedOutboxCannotReplayUnderDifferentStorageIdentity(t *testing.T) {
	t.Parallel()

	idA := callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000")
	idB := callbackStorageID(t, "6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	store, err := NewCallbackStore(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	failingClient := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return callbackHTTPResponse(http.StatusServiceUnavailable), nil
	})}
	senderA := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store, HTTPClient: failingClient, Secret: "secret", Logger: slog.Default(),
		StopCtx: context.Background(), Backoff: &zeroBackoff,
		StorageIdentity: idA, BeforeDelivery: func(context.Context) error { return nil },
	})
	leaseUUID := testLeaseUUID("copied-outbox")
	callbackURL := "https://fred.example/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000"
	beginCallbackSenderOperationIntent(t, store, leaseUUID, callbackURL, "docker-a", idA)
	senderA.SendOperationCallback(
		leaseUUID, callbackURL,
		"docker-a", backend.CallbackStatusSuccess, "",
	)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, idA.String(), pending[0].BackendStorageID)

	var requests atomic.Int32
	senderB := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return callbackHTTPResponse(http.StatusOK), nil
		})},
		Secret: "secret", Logger: slog.Default(), StopCtx: context.Background(),
		Backoff: &zeroBackoff, StorageIdentity: idB,
		BeforeReplay:   func(context.Context) error { return nil },
		BeforeDelivery: func(context.Context) error { return nil },
	})
	senderB.ReplayPendingCallbacks()
	assert.Zero(t, requests.Load(), "mismatched durable evidence must not reach HTTP")
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Len(t, pending, 1, "mismatched durable evidence must remain quarantined")
}

func TestCallbackSenderBlockingIdentityProbeIsBoundedAndPersistsBeforeDeferring(t *testing.T) {
	t.Parallel()

	id := callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000")
	store, err := NewCallbackStore(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	var requests atomic.Int32
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return callbackHTTPResponse(http.StatusOK), nil
		})},
		Secret: "secret", Logger: slog.Default(), StopCtx: context.Background(),
		Backoff: &zeroBackoff, StorageIdentity: id,
		IdentityVerificationTimeout: 20 * time.Millisecond,
		BeforeDelivery: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	leaseUUID := testLeaseUUID("blocked-identity")
	callbackURL := "https://fred.example/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000"
	beginCallbackSenderOperationIntent(t, store, leaseUUID, callbackURL, "docker-a", id)
	started := time.Now()
	sender.SendOperationCallback(
		leaseUUID, callbackURL,
		"docker-a", backend.CallbackStatusSuccess, "",
	)
	assert.Less(t, time.Since(started), 500*time.Millisecond)
	assert.Zero(t, requests.Load())
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, id.String(), pending[0].BackendStorageID)
}

func TestReplayPendingCallbacks_BlockingIdentityProbeIsBounded(t *testing.T) {
	t.Parallel()

	store, err := NewCallbackStore(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	probeFinished := make(chan error, 1)
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store, HTTPClient: http.DefaultClient, Secret: "secret", Logger: slog.Default(),
		StopCtx: context.Background(), Backoff: &zeroBackoff,
		IdentityVerificationTimeout: 20 * time.Millisecond,
		BeforeReplay: func(ctx context.Context) error {
			<-ctx.Done()
			probeFinished <- ctx.Err()
			return ctx.Err()
		},
	})

	started := time.Now()
	sender.ReplayPendingCallbacks()
	assert.Less(t, time.Since(started), 500*time.Millisecond)
	assert.ErrorIs(t, <-probeFinished, context.DeadlineExceeded)
}

func TestCallbackSenderPermanentDriftSuppressesEnqueue(t *testing.T) {
	t.Parallel()

	store, err := NewCallbackStore(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testOperationIntentSpec(t, "permanent-drift")
	_, err = store.BeginOperationIntent(spec)
	require.NoError(t, err)
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store, HTTPClient: http.DefaultClient, Secret: "secret", Logger: slog.Default(),
		StopCtx: context.Background(), Backoff: &zeroBackoff,
		StorageIdentity: spec.BackendStorageID,
		BeforeDelivery: func(context.Context) error {
			return fmt.Errorf("%w: marker mismatch", backendidentity.ErrIdentityDrift)
		},
	})
	sender.SendOperationCallback(
		spec.LeaseUUID, spec.CallbackURL, spec.Backend, backend.CallbackStatusSuccess, "",
	)
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	assert.Len(t, intents, 1, "identity drift must leave the write-ahead intent for recovery")
}

func TestCallbackSenderRechecksIdentityAfterWaitingForLeaseFIFO(t *testing.T) {
	t.Parallel()

	const leaseUUID = "018f47a2-8b1c-7def-8123-456789abcdef"
	store, err := NewCallbackStore(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	var requests atomic.Int32
	firstVerified := make(chan struct{})
	var probes atomic.Int32
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		})},
		Secret:  "secret",
		Logger:  slog.Default(),
		StopCtx: context.Background(),
		Backoff: &zeroBackoff,
		StorageIdentity: callbackStorageID(
			t, "550e8400-e29b-41d4-a716-446655440000",
		),
		BeforeDelivery: func(context.Context) error {
			if probes.Add(1) == 1 {
				close(firstVerified)
				return nil
			}
			return fmt.Errorf("%w: volume root changed", backendidentity.ErrIdentityDrift)
		},
	})
	callbackURL := "https://fred.example/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000"
	beginCallbackSenderOperationIntent(
		t, store, leaseUUID, callbackURL, "docker-a", sender.storageIdentity,
	)

	// Model an older journal mutation holding this lease's FIFO while the
	// physical root changes. The sender may pass its first probe, but it must not
	// persist using that stale proof after it eventually acquires the keyed lock.
	unlockOlder := sender.lockLease(leaseUUID)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sender.SendOperationCallback(
			leaseUUID, callbackURL,
			"docker-a", backend.CallbackStatusSuccess, "",
		)
	}()
	select {
	case <-firstVerified:
	case <-time.After(time.Second):
		t.Fatal("callback did not complete its pre-FIFO identity probe")
	}
	require.Eventually(t, func() bool {
		sender.deliveryLocksMu.Lock()
		defer sender.deliveryLocksMu.Unlock()
		lock := sender.deliveryLocks[leaseUUID]
		return lock != nil && lock.refs == 2
	}, time.Second, time.Millisecond, "callback did not join the contended journal mutation")
	unlockOlder()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("callback did not return after the FIFO was released")
	}

	assert.Equal(t, int32(2), probes.Load(), "identity must be independently re-attested after lock wait")
	assert.Zero(t, requests.Load(), "permanent post-wait drift must suppress network delivery")
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "permanent post-wait drift must suppress durable enqueue")
}

func TestCallbackSenderCancellationDuringPostLockIdentityProbeSuppressesEnqueue(t *testing.T) {
	t.Parallel()

	store, err := NewCallbackStore(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	const leaseUUID = "018f47a2-8b1c-7def-8123-456789abcdee"
	const callbackURL = "https://fred.example/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000"
	var requests atomic.Int32
	secondProbeStarted := make(chan struct{})
	var probes atomic.Int32
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		})},
		Secret:  "secret",
		Logger:  slog.Default(),
		StopCtx: context.Background(),
		Backoff: &zeroBackoff,
		StorageIdentity: callbackStorageID(
			t, "550e8400-e29b-41d4-a716-446655440000",
		),
		BeforeDelivery: func(ctx context.Context) error {
			switch probes.Add(1) {
			case 1:
				return nil
			case 2:
				close(secondProbeStarted)
				<-ctx.Done()
				return ctx.Err()
			default:
				panic("unexpected identity probe")
			}
		},
	})
	beginCallbackSenderOperationIntent(
		t, store, leaseUUID, callbackURL, "docker-a", sender.storageIdentity,
	)

	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	t.Cleanup(cancelOwner)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sender.SendOperationCallbackContext(
			ownerCtx, leaseUUID, callbackURL, "docker-a",
			backend.CallbackStatusSuccess, "",
		)
	}()
	select {
	case <-secondProbeStarted:
	case <-time.After(time.Second):
		t.Fatal("callback did not enter its post-lock identity probe")
	}
	cancelOwner()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("callback did not return after owner cancellation")
	}

	assert.Equal(t, int32(2), probes.Load())
	assert.Zero(t, requests.Load(), "canceled owner must never reach callback HTTP")
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "canceled owner must not persist a stale completion")
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	assert.Len(t, intents, 1, "cancellation must preserve the write-ahead intent for recovery")
}

// zeroBackoff is used in tests to eliminate retry delays.
var zeroBackoff = [CallbackMaxAttempts]time.Duration{}

// newTestSender creates a CallbackSender with zero backoff for fast tests.
func newTestSender(t *testing.T, store *CallbackStore, httpClient *http.Client, secret string) *CallbackSender {
	t.Helper()
	cfg := CallbackSenderConfig{
		Store:      store,
		HTTPClient: httpClient,
		Secret:     secret,
		Logger:     slog.Default(),
		StopCtx:    context.Background(),
		Backoff:    &zeroBackoff,
	}
	if store == nil {
		return MustNewEphemeralCallbackSender(cfg)
	}
	return mustNewDurableCallbackSender(t, cfg)
}

func mustNewDurableCallbackSender(t *testing.T, cfg CallbackSenderConfig) *CallbackSender {
	t.Helper()
	if len(cfg.Secret) < hmacauth.MinSecretLength {
		cfg.Secret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	}
	if !cfg.StorageIdentity.Valid() {
		cfg.StorageIdentity = callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000")
	}
	if cfg.BeforeDelivery == nil {
		cfg.BeforeDelivery = func(context.Context) error { return nil }
	}
	if cfg.BeforeReplay == nil {
		cfg.BeforeReplay = func(context.Context) error { return nil }
	}
	return MustNewCallbackSender(cfg)
}

type callbackRoundTripFunc func(*http.Request) (*http.Response, error)

func (f callbackRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func callbackHTTPResponse(status int) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       http.NoBody,
	}
}

func TestNewCallbackSender_ErrorsOnNilHTTPClient(t *testing.T) {
	_, err := NewEphemeralCallbackSender(CallbackSenderConfig{
		Logger:  slog.Default(),
		StopCtx: context.Background(),
	})
	require.ErrorContains(t, err, "HTTP client")
}

func TestNewCallbackSender_ErrorsOnNilLogger(t *testing.T) {
	_, err := NewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		StopCtx:    context.Background(),
	})
	require.ErrorContains(t, err, "logger")
}

func TestNewCallbackSender_ErrorsOnNilStopCtx(t *testing.T) {
	_, err := NewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     slog.Default(),
	})
	require.ErrorContains(t, err, "stop context")
}

func TestNewCallbackSender_RequiresDurableAuthority(t *testing.T) {
	valid := CallbackSenderConfig{
		Store:           new(CallbackStore),
		HTTPClient:      http.DefaultClient,
		Secret:          "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		StorageIdentity: callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
		BeforeDelivery:  func(context.Context) error { return nil },
		BeforeReplay:    func(context.Context) error { return nil },
		Logger:          slog.Default(),
		StopCtx:         context.Background(),
	}

	missingStore := valid
	missingStore.Store = nil
	_, err := NewCallbackSender(missingStore)
	require.ErrorContains(t, err, "durable store")

	missingSecret := valid
	missingSecret.Secret = ""
	_, err = NewCallbackSender(missingSecret)
	require.ErrorContains(t, err, "HMAC secret")

	weakSecret := valid
	weakSecret.Secret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	_, err = NewCallbackSender(weakSecret)
	require.ErrorContains(t, err, "at least 32 bytes")

	missingIdentity := valid
	missingIdentity.StorageIdentity = backendidentity.ID{}
	_, err = NewCallbackSender(missingIdentity)
	require.ErrorContains(t, err, "storage identity")

	missingDeliveryAuthority := valid
	missingDeliveryAuthority.BeforeDelivery = nil
	_, err = NewCallbackSender(missingDeliveryAuthority)
	require.ErrorContains(t, err, "delivery storage re-attestation")

	missingReplayAuthority := valid
	missingReplayAuthority.BeforeReplay = nil
	_, err = NewCallbackSender(missingReplayAuthority)
	require.ErrorContains(t, err, "replay storage re-attestation")

	assert.Panics(t, func() { MustNewCallbackSender(missingStore) },
		"only the explicitly named Must constructor may panic on invalid wiring")
}

func TestNewCallbackSender_DefaultBackoff(t *testing.T) {
	s := MustNewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     slog.Default(),
		StopCtx:    context.Background(),
	})
	assert.Equal(t, defaultCallbackBackoff, s.backoff)
	assert.Equal(t, 2*time.Minute, backend.DefaultCallbackApplicationTimeout)
	assert.Equal(t, 2*time.Minute+15*time.Second, backend.DefaultCallbackDeliveryTimeout)
	assert.Greater(t, backend.DefaultCallbackDeliveryTimeout, backend.DefaultCallbackApplicationTimeout)
	assert.Equal(t, backend.DefaultCallbackDeliveryTimeout, s.deliveryTimeout)
	assert.Equal(t, DefaultCallbackReplayInterval, s.replayInterval)
}

func TestNewCallbackSender_CustomBackoff(t *testing.T) {
	custom := [CallbackMaxAttempts]time.Duration{0, 100 * time.Millisecond, 200 * time.Millisecond}
	s := MustNewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     slog.Default(),
		StopCtx:    context.Background(),
		Backoff:    &custom,
	})
	assert.Equal(t, custom, s.backoff)
}

func TestNewCallbackSender_ErrorsOnNegativeReplayInterval(t *testing.T) {
	_, err := NewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient:     http.DefaultClient,
		Logger:         slog.Default(),
		StopCtx:        context.Background(),
		ReplayInterval: -time.Second,
	})
	require.ErrorContains(t, err, "replay interval")
}

func TestNewCallbackSender_ErrorsOnNegativeDeliveryTimeout(t *testing.T) {
	_, err := NewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient:      http.DefaultClient,
		Logger:          slog.Default(),
		StopCtx:         context.Background(),
		DeliveryTimeout: -time.Nanosecond,
	})
	require.ErrorContains(t, err, "delivery timeout")
}

func TestSendCallback_EmptyURL(t *testing.T) {
	s := newTestSender(t, nil, http.DefaultClient, "secret")
	// Should not panic, just log a warning
	s.SendOperationCallback(testLeaseUUID("lease-1"), "", "test-backend", backend.CallbackStatusSuccess, "")
}

func TestCallbackSender_TransportErrorNeverLogsCallbackCapability(t *testing.T) {
	const capability = "550e8400-e29b-41d4-a716-446655440000"
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + capability
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, nil))
	client := &http.Client{Transport: callbackRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("transport failed for %s", request.URL.String())
	})}
	sender := MustNewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient: client,
		Secret:     "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		Logger:     logger,
		StopCtx:    context.Background(),
		Backoff:    &zeroBackoff,
	})

	sender.SendOperationCallback(testLeaseUUID("log-redaction"), callbackURL, "docker", backend.CallbackStatusFailed, "failed")
	sender.SendOperationCallback(
		testLeaseUUID("log-redaction-invalid"),
		"https://fred.example/\x7f/callbacks/provision?operation_id="+capability,
		"docker", backend.CallbackStatusFailed, "failed",
	)

	assert.NotContains(t, output.String(), capability)
	assert.NotContains(t, output.String(), callbackURL)
}

func TestSendCallback_SuccessDelivery(t *testing.T) {
	const secret = "test-secret-32-chars-long-enough"
	var received backend.CallbackPayload
	var capturedBody []byte
	var capturedSig string
	var capturedMethod, capturedURI string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		capturedMethod = r.Method
		capturedURI = r.URL.RequestURI()
		capturedBody, _ = io.ReadAll(r.Body)
		json.Unmarshal(capturedBody, &received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := newTestSender(t, nil, server.Client(), secret)
	s.SendOperationCallback(testLeaseUUID("lease-1"), server.URL+callbackurl.ProvisionPath, "test-backend", backend.CallbackStatusSuccess, "")

	assert.Equal(t, testLeaseUUID("lease-1"), received.LeaseUUID)
	assert.Equal(t, backend.CallbackStatusSuccess, received.Status)
	assert.Equal(t, "test-backend", received.Backend)

	// Verify HMAC signature is present and valid
	assert.NotEmpty(t, capturedSig, "HMAC signature header must be set")
	assert.NoError(t, hmacauth.Verify(secret, capturedMethod, capturedURI, capturedBody, capturedSig, time.Minute))
}

func TestSendCallback_FailurePayload(t *testing.T) {
	var received backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := newTestSender(t, nil, server.Client(), "secret")
	s.SendOperationCallback(testLeaseUUID("lease-1"), server.URL+callbackurl.ProvisionPath, "test-backend", backend.CallbackStatusFailed, "image pull failed")

	assert.Equal(t, backend.CallbackStatusFailed, received.Status)
	assert.Equal(t, "image pull failed", received.Error)
}

func TestSendCallback_DurableSenderPersistsThenReplayRemoves(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	s := newTestSender(t, store, server.Client(), "secret")
	leaseUUID := testLeaseUUID("lease-1")
	callbackURL := server.URL + callbackurl.ProvisionPath
	beginCallbackSenderOperationIntent(t, store, leaseUUID, callbackURL, "test-backend", s.storageIdentity)
	s.SendOperationCallback(leaseUUID, callbackURL, "test-backend", backend.CallbackStatusSuccess, "")

	assert.Zero(t, requests.Load(), "durable command paths must not perform callback HTTP inline")
	assert.Len(t, s.replayWake, 1, "durable publication must wake its replay owner")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "the outbox row must commit before replay owns delivery")

	s.ReplayPendingCallbacks()
	assert.Equal(t, int32(1), requests.Load())
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendCallback_DurableFailureCompletionRemainsPendingUntilReplay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	s := newTestSender(t, store, server.Client(), "secret")
	leaseUUID := testLeaseUUID("lease-1")
	callbackURL := server.URL + callbackurl.ProvisionPath
	beginCallbackSenderOperationIntent(t, store, leaseUUID, callbackURL, "test-backend", s.storageIdentity)
	s.SendOperationCallback(leaseUUID, callbackURL, "test-backend", backend.CallbackStatusFailed, "error")

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, testLeaseUUID("lease-1"), pending[0].LeaseUUID)
}

func TestSendLifecycleCallback_CoalescesOlderPendingLifecycle(t *testing.T) {
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return callbackHTTPResponse(http.StatusInternalServerError), nil
	})}
	for _, tc := range []struct {
		name         string
		firstStatus  backend.CallbackStatus
		latestStatus backend.CallbackStatus
	}{
		{"failed to success", backend.CallbackStatusFailed, backend.CallbackStatusSuccess},
		{"success to failed", backend.CallbackStatusSuccess, backend.CallbackStatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
			require.NoError(t, err)
			defer store.Close()
			s := newTestSender(t, store, client, "secret")

			s.SendLifecycleCallback(testLeaseUUID("lease-1"), "https://fred.example/callbacks/provision", "docker", tc.firstStatus, "first", false)
			first, err := store.ListPending()
			require.NoError(t, err)
			require.Len(t, first, 1)

			s.SendLifecycleCallback(testLeaseUUID("lease-1"), "https://fred.example/callbacks/provision", "docker", tc.latestStatus, "latest", false)
			pending, err := store.ListPending()
			require.NoError(t, err)
			require.Len(t, pending, 1)
			assert.NotEqual(t, first[0].DeliveryID, pending[0].DeliveryID)
			assert.Greater(t, pending[0].Sequence, first[0].Sequence)
			assert.Equal(t, CallbackDeliveryKindLifecycle, pending[0].DeliveryKind)
			assert.Equal(t, tc.latestStatus, pending[0].Status)
			assert.Equal(t, tc.latestStatus != backend.CallbackStatusFailed, pending[0].Success)
		})
	}
}

func TestSendLifecycleCallback_DropsLateObservationBehindTerminal(t *testing.T) {
	var terminalRequests atomic.Int32
	var lateRequests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/terminal" + callbackurl.ProvisionPath:
			terminalRequests.Add(1)
		case "/late" + callbackurl.ProvisionPath:
			lateRequests.Add(1)
		}
		return callbackHTTPResponse(http.StatusServiceUnavailable), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	sender := newTestSender(t, store, client, "secret")
	leaseUUID := testLeaseUUID("terminal-sender")

	sender.SendLifecycleCallback(
		leaseUUID, "https://fred.example/terminal/callbacks/provision", "docker",
		backend.CallbackStatusDeprovisioned, "", false,
	)
	sender.SendLifecycleCallback(
		leaseUUID, "https://fred.example/late/callbacks/provision", "docker",
		backend.CallbackStatusFailed, "delayed runtime observation", false,
	)

	assert.Zero(t, terminalRequests.Load(),
		"durable lifecycle publishers must never retry the terminal head inline")
	assert.Zero(t, lateRequests.Load(), "a runtime observation must never overtake or follow terminal retirement")
	pending, err := store.listPending(leaseUUID)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusDeprovisioned, pending[0].Status)
}

func TestSendLifecycleCallback_RejectsInvalidURL(t *testing.T) {
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	s := newTestSender(t, store, client, "secret")

	const id = "550e8400-e29b-41d4-a716-446655440000"
	for index, callbackURL := range []string{
		"https://fred.example/callbacks/provision?trace=keep&operation%5fid=" + id,
		"https://fred.example/callbacks/provision?trace=%ZZ&lifecycle_id=" + id,
		"https://fred.example/callbacks/provision?trace=x;y&lifecycle_id=" + id,
	} {
		s.SendLifecycleCallback(
			fmt.Sprintf("lease-%d", index), callbackURL, "docker",
			backend.CallbackStatusFailed, "container exited", false,
		)
	}

	assert.Zero(t, requests.Load())
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendOperationCallback_RejectsInvalidURL(t *testing.T) {
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	s := newTestSender(t, store, client, "secret")
	const id = "550e8400-e29b-41d4-a716-446655440000"

	for index, callbackURL := range []string{
		"https://fred.example/callbacks/provision?trace=keep&lifecycle%5fid=" + id,
		"https://fred.example/callbacks/provision?operation_id=" + id + "&lifecycle_id=" + id,
		"https://fred.example/callbacks/provision?operation_id=" + id + "&operation_id=" + id,
		"https://fred.example/callbacks/provision?trace=%ZZ&operation_id=" + id,
		"https://fred.example/callbacks/provision?trace=x;y&operation_id=" + id,
	} {
		s.SendOperationCallback(
			fmt.Sprintf("lease-%d", index), callbackURL, "docker",
			backend.CallbackStatusSuccess, "",
		)
	}

	assert.Zero(t, requests.Load())
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendOperationCallback_AcceptsTypedAndLegacyURLs(t *testing.T) {
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	s := newTestSender(t, store, client, "secret")
	typedLeaseUUID := testLeaseUUID("typed")
	typedURL := "https://fred.example/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000"
	legacyLeaseUUID := testLeaseUUID("legacy")
	legacyURL := "https://fred.example/callbacks/provision?trace=keep"
	beginCallbackSenderOperationIntent(t, store, typedLeaseUUID, typedURL, "docker", s.storageIdentity)
	beginCallbackSenderOperationIntent(t, store, legacyLeaseUUID, legacyURL, "docker", s.storageIdentity)

	s.SendOperationCallback(
		typedLeaseUUID, typedURL,
		"docker", backend.CallbackStatusSuccess, "",
	)
	s.SendOperationCallback(
		legacyLeaseUUID, legacyURL,
		"docker", backend.CallbackStatusSuccess, "",
	)

	assert.Zero(t, requests.Load(), "accepted durable callbacks must only publish outbox facts")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)

	s.ReplayPendingCallbacks()
	assert.Equal(t, int32(2), requests.Load())
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendLifecycleCallback_AcceptsTypedLifecycleURL(t *testing.T) {
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	s := newTestSender(t, store, client, "secret")

	s.SendLifecycleCallback(
		testLeaseUUID("lease-1"),
		"https://fred.example/callbacks/provision?trace=keep&lifecycle_id=550e8400-e29b-41d4-a716-446655440000",
		"docker",
		backend.CallbackStatusFailed,
		"container exited",
		false,
	)

	assert.Zero(t, requests.Load(), "accepted durable callbacks must only publish outbox facts")
	assert.Len(t, s.replayWake, 1, "durable publication must wake its replay owner")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)

	s.ReplayPendingCallbacks()
	assert.Equal(t, int32(1), requests.Load())
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackSender_RejectsStatusOutsideDeliveryKind(t *testing.T) {
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	s := newTestSender(t, store, client, "secret")

	s.SendOperationCallback(
		"lease-operation",
		"https://fred.example/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000",
		"docker",
		backend.CallbackStatusDeprovisioned,
		"",
	)
	s.SendOperationCallback(
		"lease-operation-unknown",
		"https://fred.example/callbacks/provision",
		"docker",
		backend.CallbackStatus("unknown"),
		"",
	)
	s.SendLifecycleCallback(
		"lease-lifecycle-unknown",
		"https://fred.example/callbacks/provision",
		"docker",
		backend.CallbackStatus("unknown"),
		"",
		false,
	)
	s.SendLifecycleCallback(
		"lease-retained",
		"https://fred.example/callbacks/provision",
		"docker",
		backend.CallbackStatusSuccess,
		"",
		true,
	)

	assert.Zero(t, requests.Load())
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendOperationCallback_MissingIntentReportsStoreErrorAndSuppressesHTTP(t *testing.T) {
	var requests atomic.Int32
	var storeErrors atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store, HTTPClient: client, Secret: "secret", Logger: slog.Default(),
		StopCtx: context.Background(), Backoff: &zeroBackoff,
		OnStoreError: func() { storeErrors.Add(1) },
	})

	s.SendOperationCallback(
		testLeaseUUID("missing-intent"), "https://fred.example/callbacks/provision",
		"docker", backend.CallbackStatusSuccess, "",
	)

	assert.Zero(t, requests.Load(), "missing causal authority must suppress HTTP delivery")
	assert.Equal(t, int32(1), storeErrors.Load())
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendOperationCallback_EphemeralSenderDoesNotRequireDurableIntent(t *testing.T) {
	var requests atomic.Int32
	s := MustNewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		})},
		Secret: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", Logger: slog.Default(),
		StopCtx: context.Background(), Backoff: &zeroBackoff,
	})

	s.SendOperationCallback(
		testLeaseUUID("ephemeral"), "https://fred.example/callbacks/provision",
		"docker", backend.CallbackStatusSuccess, "",
	)

	assert.Equal(t, int32(1), requests.Load())
}

func TestSendOperationCallback_StoreFailureSuppressesDirectDelivery(t *testing.T) {
	var requests atomic.Int32
	var storeErrors atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	leaseUUID := testLeaseUUID("lease-1")
	callbackURL := "https://fred.example/callbacks/provision"
	beginCallbackSenderOperationIntent(
		t, store, leaseUUID, callbackURL, "docker",
		callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	)
	require.NoError(t, store.Close())
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:        store,
		HTTPClient:   client,
		Secret:       "secret",
		Logger:       slog.Default(),
		StopCtx:      context.Background(),
		Backoff:      &zeroBackoff,
		OnStoreError: func() { storeErrors.Add(1) },
	})

	s.SendOperationCallback(leaseUUID, callbackURL, "docker", backend.CallbackStatusSuccess, "")

	assert.Zero(t, requests.Load(), "configured persistence failure must fail closed past unknown older entries")
	assert.Equal(t, int32(1), storeErrors.Load())
	s.deliveryLocksMu.Lock()
	assert.Empty(t, s.deliveryLocks, "lease locks must be released and retired after a failed enqueue")
	s.deliveryLocksMu.Unlock()
}

func TestReplayPendingCallbacks_ListFailureSuppressesDelivery(t *testing.T) {
	var requests atomic.Int32
	var storeErrors atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	require.NoError(t, store.Close())
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:        store,
		HTTPClient:   client,
		Secret:       "secret",
		Logger:       slog.Default(),
		StopCtx:      context.Background(),
		Backoff:      &zeroBackoff,
		OnStoreError: func() { storeErrors.Add(1) },
	})

	s.ReplayPendingCallbacks()

	assert.Zero(t, requests.Load(), "a failed durable listing must not guess that no older entry exists")
	assert.Equal(t, int32(1), storeErrors.Load())
}

func TestReplayPendingCallbacks_CorruptLeaseDoesNotBlockHealthyLease(t *testing.T) {
	var healthyRequests atomic.Int32
	var corruptRequests atomic.Int32
	var storeErrors atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/healthy" + callbackurl.ProvisionPath:
			healthyRequests.Add(1)
		case "/corrupt" + callbackurl.ProvisionPath:
			corruptRequests.Add(1)
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("healthy-lease"),
		CallbackURL:  "https://fred.example/healthy/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("corrupt-lease"),
		CallbackURL:  "https://fred.example/corrupt/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		leaseBucket := tx.Bucket(callbackV2BucketName).Bucket([]byte(testLeaseUUID("corrupt-lease")))
		require.NotNil(t, leaseBucket)
		return leaseBucket.Put(
			[]byte("123e4567-e89b-42d3-a456-426614174099"), []byte("{"))
	}))

	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:        store,
		HTTPClient:   client,
		Secret:       "secret",
		Logger:       slog.Default(),
		StopCtx:      context.Background(),
		Backoff:      &zeroBackoff,
		OnStoreError: func() { storeErrors.Add(1) },
	})
	s.ReplayPendingCallbacks()

	assert.Equal(t, int32(1), healthyRequests.Load(),
		"corruption in another identifiable lease must not poison replay")
	assert.Zero(t, corruptRequests.Load())
	assert.Equal(t, int32(1), storeErrors.Load(), "the quarantined lease must remain observable")
	healthyPending, err := store.listPending(testLeaseUUID("healthy-lease"))
	require.NoError(t, err)
	assert.Empty(t, healthyPending)
	_, err = store.listPending(testLeaseUUID("corrupt-lease"))
	require.ErrorContains(t, err, "failed to decode callback entry")
	require.ErrorContains(t, store.Healthy(), "callback queue unhealthy",
		"per-lease delivery isolation must not hide corruption from health")
}

func TestReplayPendingCallbacks_SemanticPoisonNeverReachesTransport(t *testing.T) {
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	leaseUUID := testLeaseUUID("semantic-ssrf-poison")
	entry := CallbackEntry{
		DeliveryID:   "550e8400-e29b-41d4-a716-446655440000",
		LeaseUUID:    leaseUUID,
		CallbackURL:  "http://169.254.169.254/latest/meta-data/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000",
		DeliveryKind: CallbackDeliveryKindOperation,
		Sequence:     1,
		Success:      true,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}
	data, err := json.Marshal(entry)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.Bucket(callbackV2BucketName).CreateBucket([]byte(leaseUUID))
		if err != nil {
			return err
		}
		return bucket.Put(callbackSequenceKey(1), data)
	}))
	var storeErrors atomic.Int32
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: store, HTTPClient: client, Secret: "secret", Logger: slog.Default(),
		StopCtx: context.Background(), Backoff: &zeroBackoff,
		OnStoreError: func() { storeErrors.Add(1) },
	})

	sender.ReplayPendingCallbacks()
	assert.Zero(t, requests.Load(), "semantic corruption must be rejected before any outbound request")
	assert.Positive(t, storeErrors.Load())
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		assert.NotNil(t, tx.Bucket(callbackV2BucketName).Bucket([]byte(leaseUUID)).Get(callbackSequenceKey(1)))
		return nil
	}))
}

func TestReplayPendingCallbacks_StructuralDiscoveryErrorStillDrainsHealthyLease(t *testing.T) {
	var healthyRequests atomic.Int32
	var storeErrors atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/healthy"+callbackurl.ProvisionPath {
			healthyRequests.Add(1)
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("healthy-lease"),
		CallbackURL:  "https://fred.example/healthy/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		// V2 root values are invalid: every valid top-level key must own a
		// nested per-lease bucket. Preserve this value as corruption evidence.
		return tx.Bucket(callbackV2BucketName).
			Put([]byte("structural-corrupt-lease"), []byte("{"))
	}))

	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:        store,
		HTTPClient:   client,
		Secret:       "secret",
		Logger:       slog.Default(),
		StopCtx:      context.Background(),
		Backoff:      &zeroBackoff,
		OnStoreError: func() { storeErrors.Add(1) },
	})
	s.ReplayPendingCallbacks()

	assert.Equal(t, int32(1), healthyRequests.Load(),
		"structural discovery errors must be reported without discarding valid lease jobs")
	assert.GreaterOrEqual(t, storeErrors.Load(), int32(1))
	healthyPending, err := store.listPending(testLeaseUUID("healthy-lease"))
	require.NoError(t, err)
	assert.Empty(t, healthyPending)
	require.ErrorContains(t, store.Healthy(), "is not a nested bucket")
}

func TestCallbackSender_DifferentLeasesDoNotShareDeliveryLock(t *testing.T) {
	blockedStarted := make(chan struct{})
	releaseBlocked := make(chan struct{})
	otherDelivered := make(chan struct{})
	var startOnce sync.Once
	var otherOnce sync.Once
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/blocked" + callbackurl.ProvisionPath:
			startOnce.Do(func() { close(blockedStarted) })
			<-releaseBlocked
		case "/other" + callbackurl.ProvisionPath:
			otherOnce.Do(func() { close(otherDelivered) })
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	s := newTestSender(t, nil, client, "secret")
	blockedDone := make(chan struct{})
	go func() {
		defer close(blockedDone)
		s.SendOperationCallback(testLeaseUUID("blocked-lease"), "https://fred.example/blocked/callbacks/provision", "docker", backend.CallbackStatusSuccess, "")
	}()
	<-blockedStarted

	otherDone := make(chan struct{})
	go func() {
		defer close(otherDone)
		s.SendOperationCallback(testLeaseUUID("other-lease"), "https://fred.example/other/callbacks/provision", "docker", backend.CallbackStatusSuccess, "")
	}()
	select {
	case <-otherDelivered:
	case <-time.After(time.Second):
		t.Fatal("an unrelated lease was head-of-line blocked")
	}
	<-otherDone
	close(releaseBlocked)
	<-blockedDone

	s.deliveryLocksMu.Lock()
	assert.Empty(t, s.deliveryLocks, "reference-counted keyed locks must not leak lease IDs")
	s.deliveryLocksMu.Unlock()
}

func TestCallbackSender_ReplayLoopDeliversPublishedCompletionWithoutRestart(t *testing.T) {
	var available atomic.Bool
	var attempts atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts.Add(1)
		if !available.Load() {
			return callbackHTTPResponse(http.StatusServiceUnavailable), nil
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	stopCtx, cancel := context.WithCancel(context.Background())
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:          store,
		HTTPClient:     client,
		Secret:         "secret",
		Logger:         slog.Default(),
		StopCtx:        stopCtx,
		Backoff:        &zeroBackoff,
		ReplayInterval: 5 * time.Millisecond,
	})

	leaseUUID := testLeaseUUID("lease-1")
	callbackURL := "https://fred.example/callbacks/provision"
	beginCallbackSenderOperationIntent(t, store, leaseUUID, callbackURL, "docker", s.storageIdentity)
	s.SendOperationCallback(leaseUUID, callbackURL, "docker", backend.CallbackStatusSuccess, "")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "the exact completion must be durable before replay")
	assert.Zero(t, attempts.Load(), "durable settlement must not perform HTTP inline")

	available.Store(true)
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		s.RunReplayLoop()
	}()
	require.Eventually(t, func() bool {
		pending, listErr := store.ListPending()
		return listErr == nil && len(pending) == 0
	}, time.Second, 5*time.Millisecond, "periodic replay must deliver without a backend restart")
	cancel()
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("periodic replay loop did not stop with sender context")
	}
	assert.Positive(t, attempts.Load())
}

func TestCallbackSender_NotificationWakesTrackedReplayLoop(t *testing.T) {
	var attempts atomic.Int32
	initialExhausted := make(chan struct{})
	var exhaustedOnce sync.Once
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		attempt := attempts.Add(1)
		if attempt <= CallbackMaxAttempts {
			if attempt == CallbackMaxAttempts {
				exhaustedOnce.Do(func() { close(initialExhausted) })
			}
			return callbackHTTPResponse(http.StatusServiceUnavailable), nil
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("wake-lease"),
		CallbackURL:  "https://fred.example/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	stopCtx, cancel := context.WithCancel(context.Background())
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:          store,
		HTTPClient:     client,
		Secret:         "secret",
		Logger:         slog.Default(),
		StopCtx:        stopCtx,
		Backoff:        &zeroBackoff,
		ReplayInterval: time.Hour,
	})

	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		s.RunReplayLoop()
	}()
	select {
	case <-initialExhausted:
	case <-time.After(time.Second):
		t.Fatal("initial replay did not exhaust its delivery attempts")
	}
	s.NotifyPendingCallbacks()
	require.Eventually(t, func() bool {
		pending, listErr := store.ListPending()
		return listErr == nil && len(pending) == 0
	}, time.Second, time.Millisecond, "outbox notification must not wait for the periodic interval")
	cancel()
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("notified replay loop did not stop with sender context")
	}
}

func TestCallbackSender_DirectIntentSettlementWakesTrackedReplayLoop(t *testing.T) {
	var attempts atomic.Int32
	delivered := make(chan struct{})
	var deliveredOnce sync.Once
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts.Add(1)
		deliveredOnce.Do(func() { close(delivered) })
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	stopCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	initialReplayEntered := make(chan struct{})
	releaseInitialReplay := make(chan struct{})
	var replayCalls atomic.Int32
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:          store,
		HTTPClient:     client,
		Secret:         "secret",
		Logger:         slog.Default(),
		StopCtx:        stopCtx,
		Backoff:        &zeroBackoff,
		ReplayInterval: time.Hour,
		BeforeReplay: func(ctx context.Context) error {
			if replayCalls.Add(1) != 1 {
				return nil
			}
			close(initialReplayEntered)
			select {
			case <-releaseInitialReplay:
				return fmt.Errorf("suppress initial replay for wakeup test")
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})

	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		s.RunReplayLoop()
	}()
	select {
	case <-initialReplayEntered:
	case <-time.After(time.Second):
		t.Fatal("tracked replay loop did not enter its initial pass")
	}

	leaseUUID := testLeaseUUID("direct-settlement-wake")
	callbackURL := "https://fred.example/callbacks/provision"
	admission := beginCallbackSenderOperationIntent(
		t, store, leaseUUID, callbackURL, "docker", s.storageIdentity,
	)
	_, err = store.ResolveOperationIntent(
		admission.Claim, backend.CallbackStatusFailed, "definitively refused",
	)
	require.NoError(t, err)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "direct settlement must commit before replay")
	assert.Zero(t, attempts.Load(), "direct settlement must not perform HTTP inline")

	// The first replay is deliberately suppressed after the completion is
	// durable. Only the store's post-commit notification can trigger delivery
	// before the one-hour periodic interval.
	close(releaseInitialReplay)
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("direct intent settlement did not wake the tracked replay loop")
	}
	require.Eventually(t, func() bool {
		pending, listErr := store.ListPending()
		return listErr == nil && len(pending) == 0
	}, time.Second, time.Millisecond)
	assert.Equal(t, int32(1), attempts.Load())
	assert.GreaterOrEqual(t, replayCalls.Load(), int32(2))

	cancel()
	select {
	case <-loopDone:
	case <-time.After(time.Second):
		t.Fatal("direct-settlement replay loop did not stop with sender context")
	}
	store.replaySubscribersMu.Lock()
	subscriberCount := len(store.replaySubscribers)
	store.replaySubscribersMu.Unlock()
	assert.Zero(t, subscriberCount, "a stopped replay loop must unregister its wake channel")
}

func TestCallbackSender_CanceledDrainerHandsOffToTrackedPeer(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	leaseUUID := testLeaseUUID("canceled-drainer-handoff")
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    leaseUUID,
		CallbackURL:  "https://fred.example/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)

	ownerStarted := make(chan struct{})
	var ownerStartedOnce sync.Once
	ownerClient := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		ownerStartedOnce.Do(func() { close(ownerStarted) })
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	owner := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:          store,
		HTTPClient:     ownerClient,
		Secret:         "secret",
		Logger:         slog.Default(),
		StopCtx:        ownerCtx,
		Backoff:        &zeroBackoff,
		ReplayInterval: time.Hour,
	})
	ownerDone := make(chan struct{})
	go func() {
		defer close(ownerDone)
		owner.RunReplayLoop()
	}()
	select {
	case <-ownerStarted:
	case <-time.After(time.Second):
		t.Fatal("first replay loop did not acquire callback drain ownership")
	}

	peerDelivered := make(chan struct{})
	var peerDeliveredOnce sync.Once
	peerClient := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		peerDeliveredOnce.Do(func() { close(peerDelivered) })
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	peerCtx, cancelPeer := context.WithCancel(context.Background())
	secondReplayEntered := make(chan struct{})
	releaseSecondReplay := make(chan struct{})
	var releaseSecondOnce sync.Once
	var peerReplayCalls atomic.Int32
	peer := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:          store,
		HTTPClient:     peerClient,
		Secret:         "secret",
		Logger:         slog.Default(),
		StopCtx:        peerCtx,
		Backoff:        &zeroBackoff,
		ReplayInterval: time.Hour,
		BeforeReplay: func(ctx context.Context) error {
			if peerReplayCalls.Add(1) != 2 {
				return nil
			}
			close(secondReplayEntered)
			select {
			case <-releaseSecondReplay:
				return fmt.Errorf("suppress marker replay for cancellation handoff test")
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		peer.RunReplayLoop()
	}()
	t.Cleanup(func() {
		cancelOwner()
		cancelPeer()
		releaseSecondOnce.Do(func() { close(releaseSecondReplay) })
		for name, done := range map[string]<-chan struct{}{
			"owner": ownerDone,
			"peer":  peerDone,
		} {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Errorf("%s replay loop did not stop during cleanup", name)
			}
		}
	})

	// A marker can be consumed only after the peer's initial replay has
	// completed and lost drain election to owner. Hold its second pass in the
	// identity hook so no unrelated replay can race the cancellation handoff.
	peer.NotifyPendingCallbacks()
	select {
	case <-secondReplayEntered:
	case <-time.After(time.Second):
		t.Fatal("peer replay loop did not finish its initial lost election")
	}

	cancelOwner()
	select {
	case <-ownerDone:
	case <-time.After(time.Second):
		t.Fatal("canceled drain owner did not stop")
	}
	assert.Len(t, peer.replayWake, 1,
		"canceled owner must publish one coalesced handoff edge to its peer")
	releaseSecondOnce.Do(func() { close(releaseSecondReplay) })
	select {
	case <-peerDelivered:
	case <-time.After(time.Second):
		t.Fatal("peer did not take over callback delivery before the one-hour interval")
	}

	cancelPeer()
	select {
	case <-peerDone:
	case <-time.After(time.Second):
		t.Fatal("peer replay loop did not stop")
	}
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendCallback_ExactCompletionBlocksNewerLifecycleUntilFIFOCanDrain(t *testing.T) {
	var exactAvailable atomic.Bool
	var lifecycleAttempts atomic.Int32
	var deliveredMu sync.Mutex
	var deliveredOrder []backend.CallbackStatus
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload backend.CallbackPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.URL.Path == "/exact"+callbackurl.ProvisionPath && !exactAvailable.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.URL.Path == "/lifecycle"+callbackurl.ProvisionPath {
			lifecycleAttempts.Add(1)
		}
		deliveredMu.Lock()
		deliveredOrder = append(deliveredOrder, payload.Status)
		deliveredMu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	s := newTestSender(t, store, http.DefaultClient, "secret")
	leaseUUID := testLeaseUUID("lease-1")
	exactURL := server.URL + "/exact" + callbackurl.ProvisionPath
	beginCallbackSenderOperationIntent(t, store, leaseUUID, exactURL, "docker", s.storageIdentity)
	s.SendOperationCallback(leaseUUID, exactURL, "docker", backend.CallbackStatusSuccess, "")
	s.SendLifecycleCallback(leaseUUID, server.URL+"/lifecycle"+callbackurl.ProvisionPath, "docker", backend.CallbackStatusFailed, "container exited", false)

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, server.URL+"/exact"+callbackurl.ProvisionPath, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	assert.Equal(t, server.URL+"/lifecycle"+callbackurl.ProvisionPath, pending[1].CallbackURL)
	assert.Equal(t, backend.CallbackStatusFailed, pending[1].Status)
	assert.Less(t, pending[0].Sequence, pending[1].Sequence)
	assert.Zero(t, lifecycleAttempts.Load(), "new lifecycle callback must not overtake the exact completion")

	exactAvailable.Store(true)
	s.ReplayPendingCallbacks()
	deliveredMu.Lock()
	assert.Equal(t, []backend.CallbackStatus{
		backend.CallbackStatusSuccess,
		backend.CallbackStatusFailed,
	}, deliveredOrder)
	deliveredMu.Unlock()
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackSender_ConcurrentReplayAndLiveEnqueueRemainFIFO(t *testing.T) {
	exactStarted := make(chan struct{})
	releaseExact := make(chan struct{})
	var exactOnce sync.Once
	var deliveredMu sync.Mutex
	var delivered []string
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		deliveredMu.Lock()
		delivered = append(delivered, req.URL.Path)
		deliveredMu.Unlock()
		if req.URL.Path == "/exact"+callbackurl.ProvisionPath {
			exactOnce.Do(func() { close(exactStarted) })
			<-releaseExact
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "https://fred.example/exact/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	s := newTestSender(t, store, client, "secret")

	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		s.ReplayPendingCallbacks()
	}()
	select {
	case <-exactStarted:
	case <-time.After(time.Second):
		t.Fatal("replay did not start the exact completion")
	}

	liveDone := make(chan struct{})
	go func() {
		defer close(liveDone)
		s.SendLifecycleCallback(
			testLeaseUUID("lease-1"),
			"https://fred.example/lifecycle/callbacks/provision",
			"docker",
			backend.CallbackStatusFailed,
			"container exited",
			false,
		)
	}()
	select {
	case <-liveDone:
	case <-time.After(time.Second):
		t.Fatal("live enqueue waited behind callback HTTP")
	}
	pending, err := store.listPending(testLeaseUUID("lease-1"))
	require.NoError(t, err)
	require.Len(t, pending, 2, "live enqueue must durably append while replay delivers the head")
	assert.Equal(t, CallbackDeliveryKindOperation, pending[0].DeliveryKind)
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[1].DeliveryKind)
	deliveredMu.Lock()
	assert.Equal(t, []string{"/exact" + callbackurl.ProvisionPath}, delivered,
		"durable enqueue must not perform HTTP inline")
	deliveredMu.Unlock()

	close(releaseExact)
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("replay did not finish")
	}
	deliveredMu.Lock()
	assert.Equal(t, []string{"/exact" + callbackurl.ProvisionPath, "/lifecycle" + callbackurl.ProvisionPath}, delivered,
		"the same drainer must re-list and preserve wire FIFO")
	deliveredMu.Unlock()
	pending, err = store.listPending(testLeaseUUID("lease-1"))
	require.NoError(t, err)
	assert.Empty(t, pending)
	s.deliveryLocksMu.Lock()
	assert.Empty(t, s.deliveryLocks, "concurrent replay/send must retire its mutation lock")
	s.deliveryLocksMu.Unlock()
	s.drainLocksMu.Lock()
	assert.Empty(t, s.drainLocks, "completed replay must retire its drainer lock")
	s.drainLocksMu.Unlock()
}

func TestCallbackSender_ConcurrentReplaysShareOneStoreDrainer(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		startedOnce.Do(func() { close(started) })
		<-release
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	leaseUUID := testLeaseUUID("shared-drainer")
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    leaseUUID,
		CallbackURL:  "https://fred.example/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	senderA := newTestSender(t, store, client, "secret")
	senderB := newTestSender(t, store, client, "secret")

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		senderA.ReplayPendingCallbacks()
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first sender did not begin callback delivery")
	}

	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		senderB.ReplayPendingCallbacks()
	}()
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second replay waited behind the elected wire drainer")
	}
	assert.Equal(t, int32(1), requests.Load(), "shared store must elect exactly one wire drainer per lease")

	releaseOnce.Do(func() { close(release) })
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first sender did not finish after callback release")
	}
	pending, err := store.listPending(leaseUUID)
	require.NoError(t, err)
	assert.Empty(t, pending)
	store.drainLocksMu.Lock()
	assert.Empty(t, store.drainLocks, "shared drain ownership must retire after delivery")
	store.drainLocksMu.Unlock()
}

func TestCallbackSender_InFlightLifecycleReplacementSurvivesPreciseRemoval(t *testing.T) {
	oldStarted := make(chan struct{})
	releaseOld := make(chan struct{})
	var oldStartedOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseOld) }) })
	var deliveredMu sync.Mutex
	var delivered []string
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		deliveredMu.Lock()
		delivered = append(delivered, req.URL.Path)
		deliveredMu.Unlock()
		if req.URL.Path == "/old"+callbackurl.ProvisionPath {
			oldStartedOnce.Do(func() { close(oldStarted) })
			<-releaseOld
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	leaseUUID := testLeaseUUID("in-flight-lifecycle-replacement")
	oldEntry, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    leaseUUID,
		CallbackURL:  "https://fred.example/old/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       backend.CallbackStatusFailed,
		Error:        "old observation",
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	sender := newTestSender(t, store, client, "secret")

	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		sender.ReplayPendingCallbacks()
	}()
	select {
	case <-oldStarted:
	case <-time.After(time.Second):
		t.Fatal("old lifecycle observation did not begin delivery")
	}

	enqueueDone := make(chan struct{})
	go func() {
		defer close(enqueueDone)
		sender.SendLifecycleCallback(
			leaseUUID,
			"https://fred.example/new/callbacks/provision",
			"docker",
			backend.CallbackStatusFailed,
			"new observation",
			false,
		)
	}()
	select {
	case <-enqueueDone:
	case <-time.After(time.Second):
		t.Fatal("lifecycle replacement waited behind callback HTTP")
	}
	pending, err := store.listPending(leaseUUID)
	require.NoError(t, err)
	require.Len(t, pending, 1, "new lifecycle observation must coalesce the in-flight durable row")
	assert.NotEqual(t, oldEntry.DeliveryID, pending[0].DeliveryID)
	assert.Equal(t, "https://fred.example/new/callbacks/provision", pending[0].CallbackURL)

	releaseOnce.Do(func() { close(releaseOld) })
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("drainer did not re-list the lifecycle replacement")
	}
	deliveredMu.Lock()
	assert.Equal(t, []string{
		"/old" + callbackurl.ProvisionPath,
		"/new" + callbackurl.ProvisionPath,
	}, delivered, "old precise removal must not delete its replacement")
	deliveredMu.Unlock()
	pending, err = store.listPending(leaseUUID)
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackSender_CanceledDrainerLeavesRowForReplacementSender(t *testing.T) {
	started := make(chan struct{})
	var startedOnce sync.Once
	clientA := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		startedOnce.Do(func() { close(started) })
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	leaseUUID := testLeaseUUID("replace-canceled-drainer")
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    leaseUUID,
		CallbackURL:  "https://fred.example/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	stopCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	senderA := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      store,
		HTTPClient: clientA,
		Secret:     "secret",
		Logger:     slog.Default(),
		StopCtx:    stopCtx,
		Backoff:    &zeroBackoff,
	})

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		senderA.ReplayPendingCallbacks()
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first sender did not begin callback delivery")
	}
	cancel()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("canceled sender did not release callback drain ownership")
	}
	pending, err := store.listPending(leaseUUID)
	require.NoError(t, err)
	require.Len(t, pending, 1, "cancellation must preserve the durable head")
	store.drainLocksMu.Lock()
	assert.Empty(t, store.drainLocks, "cancellation must retire drain ownership")
	store.drainLocksMu.Unlock()

	var replacementRequests atomic.Int32
	senderB := newTestSender(t, store, &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		replacementRequests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}, "secret")
	senderB.ReplayPendingCallbacks()
	assert.Equal(t, int32(1), replacementRequests.Load())
	pending, err = store.listPending(leaseUUID)
	require.NoError(t, err)
	assert.Empty(t, pending, "replacement sender must consume the preserved head")
}

func TestCallbackSender_ExpirySkipsBusyLeaseWithoutMutatingDrain(t *testing.T) {
	exactStarted := make(chan struct{})
	releaseExact := make(chan struct{})
	var exactOnce sync.Once
	var lifecycleRequests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/exact" + callbackurl.ProvisionPath:
			exactOnce.Do(func() {
				close(exactStarted)
				<-releaseExact
			})
			return callbackHTTPResponse(http.StatusServiceUnavailable), nil
		case "/lifecycle" + callbackurl.ProvisionPath:
			lifecycleRequests.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		default:
			return callbackHTTPResponse(http.StatusNotFound), nil
		}
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "https://fred.example/exact/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	})
	require.NoError(t, err)
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "https://fred.example/lifecycle/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       backend.CallbackStatusFailed,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	})
	require.NoError(t, err)
	s := newTestSender(t, store, client, "secret")

	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		s.ReplayPendingCallbacks()
	}()
	select {
	case <-exactStarted:
	case <-time.After(time.Second):
		t.Fatal("replay did not begin exact delivery")
	}

	cleanupDone := make(chan struct{})
	var removed int
	var cleanupErr error
	go func() {
		defer close(cleanupDone)
		removed, cleanupErr = store.RemoveOlderThan(24 * time.Hour)
	}()
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("cleanup blocked behind an in-flight lease drain")
	}
	require.NoError(t, cleanupErr)
	assert.Zero(t, removed, "cleanup must skip a lease whose drain lock is busy")
	pending, err := store.listPending(testLeaseUUID("lease-1"))
	require.NoError(t, err)
	require.Len(t, pending, 2, "cleanup must not mutate an in-flight drain snapshot")

	close(releaseExact)
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("replay did not stop after exact delivery failure")
	}
	assert.Zero(t, lifecycleRequests.Load(), "lifecycle suffix must not pass the failed exact head")
	pending, err = store.listPending(testLeaseUUID("lease-1"))
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, CallbackDeliveryKindOperation, pending[0].DeliveryKind)
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[1].DeliveryKind)

	removed, err = store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Zero(t, removed, "exact operation evidence remains non-expiring after delivery releases the lock")
	pending, err = store.listPending(testLeaseUUID("lease-1"))
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, CallbackDeliveryKindOperation, pending[0].DeliveryKind)
}

func TestDeliverCallback_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := newTestSender(t, nil, server.Client(), "secret")
	ok := s.DeliverCallback(testLeaseUUID("lease-1"), server.URL+callbackurl.ProvisionPath, []byte(`{"test":true}`))
	assert.True(t, ok)
}

func TestDeliverCallback_RetriesOnServerError(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := attempts.Add(1)
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := newTestSender(t, nil, server.Client(), "secret")
	ok := s.DeliverCallback(testLeaseUUID("lease-1"), server.URL+callbackurl.ProvisionPath, []byte(`{}`))
	assert.True(t, ok)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestDeliverCallback_AllRetriesFail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	s := newTestSender(t, nil, server.Client(), "secret")
	ok := s.DeliverCallback(testLeaseUUID("lease-1"), server.URL+callbackurl.ProvisionPath, []byte(`{}`))
	assert.False(t, ok)
}

func TestCallbackSenderRejectsRedirectEvenWhenSuppliedClientFollows(t *testing.T) {
	var redirectedRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectedRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	var redirectResponses atomic.Int32
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectResponses.Add(1)
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	client := redirect.Client()
	require.Nil(t, client.CheckRedirect, "test client must follow redirects by default")
	s := MustNewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient: client,
		Secret:     "secret",
		Logger:     slog.Default(),
		StopCtx:    context.Background(),
		Backoff:    &zeroBackoff,
	})

	delivered := s.DeliverCallback(
		testLeaseUUID("redirect"), redirect.URL, []byte(`{"authority":"signed"}`),
	)

	assert.False(t, delivered)
	assert.Equal(t, int32(CallbackMaxAttempts), redirectResponses.Load())
	assert.Zero(t, redirectedRequests.Load(),
		"an HMAC-signed request must never be forwarded to a redirect target")
	assert.Nil(t, client.CheckRedirect, "constructor must not mutate the caller's client")
}

func TestCallbackSenderStripsAmbientCookieJarWithoutMutatingClient(t *testing.T) {
	cookies := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookies <- r.Header.Get("Cookie")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	callbackRequest, err := http.NewRequest(http.MethodPost, server.URL+callbackurl.ProvisionPath, nil)
	require.NoError(t, err)
	jar.SetCookies(callbackRequest.URL, []*http.Cookie{{Name: "ambient", Value: "credential"}})
	client := server.Client()
	client.Jar = jar
	require.NotEmpty(t, client.Jar.Cookies(callbackRequest.URL))

	s := MustNewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient: client,
		Secret:     "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		Logger:     slog.Default(),
		StopCtx:    context.Background(),
		Backoff:    &zeroBackoff,
	})
	delivered := s.DeliverCallback(
		testLeaseUUID("ambient-cookie"), server.URL+callbackurl.ProvisionPath, []byte(`{"authority":"signed"}`),
	)

	assert.True(t, delivered)
	assert.Empty(t, <-cookies, "callback delivery must not carry ambient cookies")
	assert.Same(t, jar, client.Jar, "constructor must not mutate the caller's client")
	assert.NotEmpty(t, client.Jar.Cookies(callbackRequest.URL),
		"stripping sender authority must not clear the caller's cookie jar")
}

func TestDeliverCallback_ShutdownAbortsRetry(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	longBackoff := [CallbackMaxAttempts]time.Duration{0, 5 * time.Second, 5 * time.Second}
	s := MustNewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient: server.Client(),
		Logger:     slog.Default(),
		StopCtx:    ctx,
		Backoff:    &longBackoff,
	})

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	ok := s.DeliverCallback(testLeaseUUID("lease-1"), server.URL+callbackurl.ProvisionPath, []byte(`{}`))
	assert.False(t, ok)
	assert.LessOrEqual(t, attempts.Load(), int32(2))
}

func TestDeliverCallback_ConfiguredDeliveryTimeoutOutlivesFormerCaps(t *testing.T) {
	// Scale the former 10-second sender and 30-second client caps down to
	// milliseconds. A synchronous Fred application that finishes after both
	// boundaries must still succeed when the sender owns the request deadline.
	const (
		formerSenderCap  = 10 * time.Millisecond
		formerClientCap  = 30 * time.Millisecond
		applicationDelay = 50 * time.Millisecond
		deliveryTimeout  = 200 * time.Millisecond
	)
	require.Greater(t, formerClientCap, formerSenderCap)
	require.Greater(t, applicationDelay, formerClientCap)
	require.Greater(t, deliveryTimeout, applicationDelay)

	var attempts atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts.Add(1)
		timer := time.NewTimer(applicationDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
			return callbackHTTPResponse(http.StatusNoContent), nil
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	})}
	assert.Zero(t, client.Timeout)
	s := MustNewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient:      client,
		Logger:          slog.Default(),
		StopCtx:         context.Background(),
		Backoff:         &zeroBackoff,
		DeliveryTimeout: deliveryTimeout,
	})

	started := time.Now()
	ok := s.DeliverCallback(testLeaseUUID("lease-1"), "https://fred.example/callbacks/provision", []byte(`{}`))

	assert.True(t, ok)
	assert.Equal(t, int32(1), attempts.Load())
	assert.GreaterOrEqual(t, time.Since(started), applicationDelay)
}

func TestDeliverCallback_AttemptDeadlineDefersRemainingRetries(t *testing.T) {
	const deliveryTimeout = 25 * time.Millisecond
	var attempts atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts.Add(1)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	s := MustNewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient:      client,
		Logger:          slog.Default(),
		StopCtx:         context.Background(),
		Backoff:         &zeroBackoff,
		DeliveryTimeout: deliveryTimeout,
	})

	delivered := s.DeliverCallback(
		testLeaseUUID("lease-1"), "https://fred.example/callbacks/provision", []byte(`{}`),
	)

	assert.False(t, delivered)
	assert.Equal(t, int32(1), attempts.Load(),
		"one exhausted request budget must defer to periodic replay instead of starting two more full-budget attempts")
}

func TestDeliverCallback_HTTPFailureRetrySharesInlineDeadline(t *testing.T) {
	const deliveryTimeout = time.Minute
	stopCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	firstAttemptStarted := make(chan struct{})
	releaseFirstAttempt := make(chan struct{})
	secondAttemptStarted := make(chan struct{})
	var attempts atomic.Int32
	deadlines := make(chan time.Time, 2)
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		deadline, ok := req.Context().Deadline()
		if !ok {
			return nil, fmt.Errorf("callback request has no deadline")
		}
		deadlines <- deadline
		attempt := attempts.Add(1)
		switch attempt {
		case 1:
			close(firstAttemptStarted)
			select {
			case <-releaseFirstAttempt:
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
			return callbackHTTPResponse(http.StatusServiceUnavailable), nil
		case 2:
			close(secondAttemptStarted)
			<-req.Context().Done()
			return nil, req.Context().Err()
		default:
			return nil, fmt.Errorf("unexpected callback attempt %d", attempt)
		}
	})}
	s := MustNewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient:      client,
		Logger:          slog.Default(),
		StopCtx:         stopCtx,
		Backoff:         &zeroBackoff,
		DeliveryTimeout: deliveryTimeout,
	})

	delivered := make(chan bool, 1)
	go func() {
		delivered <- s.DeliverCallback(
			testLeaseUUID("lease-1"), "https://fred.example/callbacks/provision", []byte(`{}`),
		)
	}()
	select {
	case <-firstAttemptStarted:
	case <-time.After(time.Second):
		t.Fatal("first callback attempt did not start")
	}
	// Keep the first request parked until the test deliberately returns its
	// 503, avoiding sleeps and elapsed-time assertions for synchronization.
	close(releaseFirstAttempt)
	select {
	case <-secondAttemptStarted:
	case <-time.After(time.Second):
		t.Fatal("second callback attempt did not start")
	}
	cancel()
	select {
	case ok := <-delivered:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("callback delivery did not stop after sender cancellation")
	}

	assert.Equal(t, int32(2), attempts.Load(),
		"a failed response may use the remaining budget once but must not mint another full timeout")
	firstDeadline := <-deadlines
	secondDeadline := <-deadlines
	assert.True(t, firstDeadline.Equal(secondDeadline),
		"all retry attempts must inherit the exact same delivery deadline")
}

func TestDeliverCallback_StopContextCancelsInFlightRequest(t *testing.T) {
	stopCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	requestStarted := make(chan struct{})
	requestCanceled := make(chan error, 1)
	var attempts atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		attempts.Add(1)
		close(requestStarted)
		<-req.Context().Done()
		requestCanceled <- req.Context().Err()
		return nil, req.Context().Err()
	})}
	s := MustNewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient:      client,
		Logger:          slog.Default(),
		StopCtx:         stopCtx,
		DeliveryTimeout: time.Minute,
	})

	delivered := make(chan bool, 1)
	go func() {
		delivered <- s.DeliverCallback(
			testLeaseUUID("lease-1"), "https://fred.example/callbacks/provision", []byte(`{}`),
		)
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("callback request did not start")
	}
	cancel()

	select {
	case err := <-requestCanceled:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("request context did not inherit sender cancellation")
	}
	select {
	case ok := <-delivered:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("callback delivery did not stop after sender cancellation")
	}
	assert.Equal(t, int32(1), attempts.Load())
}

func TestReplayPendingCallbacks_NilStore(t *testing.T) {
	s := newTestSender(t, nil, http.DefaultClient, "secret")
	// Should not panic
	s.ReplayPendingCallbacks()
}

func TestReplayPendingCallbacks_EmptyStore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	s := newTestSender(t, store, http.DefaultClient, "secret")
	s.ReplayPendingCallbacks()
}

func TestReplayPendingCallbacks_DeliversAndRemoves(t *testing.T) {
	var receivedMu sync.Mutex
	var received []backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		receivedMu.Lock()
		received = append(received, p)
		receivedMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  server.URL + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-2"),
		CallbackURL:  server.URL + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      false,
		Error:        "pull failed",
		CreatedAt:    time.Now(),
	}))

	s := newTestSender(t, store, server.Client(), "secret")
	s.ReplayPendingCallbacks()

	receivedMu.Lock()
	assert.Len(t, received, 2)
	receivedMu.Unlock()

	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestReplayPendingCallbacks_PartialFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/success"+callbackurl.ProvisionPath {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  server.URL + "/success" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-2"),
		CallbackURL:  server.URL + "/failure" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      false,
		Error:        "error",
		CreatedAt:    time.Now(),
	}))

	s := newTestSender(t, store, server.Client(), "secret")
	s.ReplayPendingCallbacks()

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, testLeaseUUID("lease-2"), pending[0].LeaseUUID)
}

func TestReplayPendingCallbacks_FailureBlocksOnlyItsLease(t *testing.T) {
	var blockedAttempts atomic.Int32
	var overtakingLifecycleAttempts atomic.Int32
	var otherLeaseAttempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/blocked-exact" + callbackurl.ProvisionPath:
			blockedAttempts.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		case "/same-lease-lifecycle" + callbackurl.ProvisionPath:
			overtakingLifecycleAttempts.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case "/other-lease" + callbackurl.ProvisionPath:
			otherLeaseAttempts.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("blocked-lease"),
		CallbackURL:  server.URL + "/blocked-exact" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("blocked-lease"),
		CallbackURL:  server.URL + "/same-lease-lifecycle" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       backend.CallbackStatusFailed,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("healthy-lease"),
		CallbackURL:  server.URL + "/other-lease" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))

	newTestSender(t, store, server.Client(), "secret").ReplayPendingCallbacks()

	assert.Equal(t, int32(CallbackMaxAttempts), blockedAttempts.Load())
	assert.Zero(t, overtakingLifecycleAttempts.Load(),
		"a newer lifecycle observation must not overtake the failed exact completion")
	assert.Equal(t, int32(1), otherLeaseAttempts.Load(),
		"one lease's delivery failure must not stop another lease's drain")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, testLeaseUUID("blocked-lease"), pending[0].LeaseUUID)
	assert.Equal(t, testLeaseUUID("blocked-lease"), pending[1].LeaseUUID)
}

func TestReplayPendingCallbacks_BlockedLeaseDoesNotDelayAnotherLease(t *testing.T) {
	blockedStarted := make(chan struct{})
	releaseBlocked := make(chan struct{})
	otherDelivered := make(chan struct{})
	var blockedOnce sync.Once
	var otherOnce sync.Once
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/blocked" + callbackurl.ProvisionPath:
			blockedOnce.Do(func() { close(blockedStarted) })
			<-releaseBlocked
		case "/other" + callbackurl.ProvisionPath:
			otherOnce.Do(func() { close(otherDelivered) })
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	for _, entry := range []CallbackEntry{
		{
			LeaseUUID:    testLeaseUUID("blocked-lease"),
			CallbackURL:  "https://fred.example/blocked/callbacks/provision",
			DeliveryKind: CallbackDeliveryKindOperation,
			Status:       backend.CallbackStatusSuccess,
			CreatedAt:    time.Now(),
		},
		{
			LeaseUUID:    testLeaseUUID("other-lease"),
			CallbackURL:  "https://fred.example/other/callbacks/provision",
			DeliveryKind: CallbackDeliveryKindOperation,
			Status:       backend.CallbackStatusSuccess,
			CreatedAt:    time.Now(),
		},
	} {
		require.NoError(t, store.storeValidTest(entry))
	}
	s := newTestSender(t, store, client, "secret")
	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		s.ReplayPendingCallbacks()
	}()

	select {
	case <-blockedStarted:
	case <-time.After(time.Second):
		t.Fatal("blocked lease did not begin delivery")
	}
	select {
	case <-otherDelivered:
	case <-time.After(time.Second):
		t.Fatal("another lease was delayed behind the blocked replay")
	}
	close(releaseBlocked)
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("replay did not finish after blocked lease was released")
	}
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestReplayPendingCallbacks_BoundsFanout(t *testing.T) {
	release := make(chan struct{})
	var active atomic.Int32
	var maxActive atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		current := active.Add(1)
		for {
			previous := maxActive.Load()
			if current <= previous || maxActive.CompareAndSwap(previous, current) {
				break
			}
		}
		<-release
		active.Add(-1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	for i := range callbackReplayWorkerLimit + 1 {
		require.NoError(t, store.storeValidTest(CallbackEntry{
			LeaseUUID:    testLeaseUUID(fmt.Sprintf("lease-%02d", i)),
			CallbackURL:  fmt.Sprintf("https://fred.example/%02d/callbacks/provision", i),
			DeliveryKind: CallbackDeliveryKindOperation,
			Status:       backend.CallbackStatusSuccess,
			CreatedAt:    time.Now(),
		}))
	}
	s := newTestSender(t, store, client, "secret")
	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		s.ReplayPendingCallbacks()
	}()

	require.Eventually(t, func() bool {
		return active.Load() == int32(callbackReplayWorkerLimit)
	}, time.Second, time.Millisecond)
	assert.Equal(t, int32(callbackReplayWorkerLimit), maxActive.Load(),
		"replay must not create one simultaneous retry chain per queued lease")
	close(release)
	select {
	case <-replayDone:
	case <-time.After(5 * time.Second):
		t.Fatal("bounded replay did not finish")
	}
	assert.LessOrEqual(t, maxActive.Load(), int32(callbackReplayWorkerLimit))
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestReplayPendingCallbacks_RecoversPerLeasePanicAndContinuesWorker(t *testing.T) {
	var healthyDelivered atomic.Int32
	var replayPanics atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/healthy"+callbackurl.ProvisionPath {
			healthyDelivered.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		}
		panic("synthetic callback transport panic")
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	for i := range callbackReplayWorkerLimit {
		require.NoError(t, store.storeValidTest(CallbackEntry{
			LeaseUUID:    testLeaseUUID(fmt.Sprintf("panic-lease-%02d", i)),
			CallbackURL:  fmt.Sprintf("https://fred.example/panic-%02d/callbacks/provision", i),
			DeliveryKind: CallbackDeliveryKindOperation,
			Status:       backend.CallbackStatusSuccess,
			CreatedAt:    time.Now(),
		}))
	}
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("healthy-lease"),
		CallbackURL:  "https://fred.example/healthy/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:         store,
		HTTPClient:    client,
		Secret:        "secret",
		Logger:        slog.Default(),
		StopCtx:       context.Background(),
		Backoff:       &zeroBackoff,
		OnReplayPanic: func(any) { replayPanics.Add(1) },
	})

	assert.NotPanics(t, s.ReplayPendingCallbacks)
	assert.Equal(t, int32(callbackReplayWorkerLimit), replayPanics.Load())
	assert.Equal(t, int32(1), healthyDelivered.Load(),
		"workers must continue consuming unrelated lease jobs after a recovered panic")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, callbackReplayWorkerLimit)
	for _, entry := range pending {
		assert.NotEqual(t, testLeaseUUID("healthy-lease"), entry.LeaseUUID)
	}
	s.deliveryLocksMu.Lock()
	assert.Empty(t, s.deliveryLocks, "panic recovery must release journal-mutation locks")
	s.deliveryLocksMu.Unlock()
	s.drainLocksMu.Lock()
	assert.Empty(t, s.drainLocks, "panic recovery must release wire-drain ownership")
	s.drainLocksMu.Unlock()
}

func TestReplayPendingCallbacks_CanceledSenderDoesNotWalkQueue(t *testing.T) {
	var requests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	for i := range callbackReplayWorkerLimit * 2 {
		require.NoError(t, store.storeValidTest(CallbackEntry{
			LeaseUUID:    testLeaseUUID(fmt.Sprintf("lease-%02d", i)),
			CallbackURL:  fmt.Sprintf("https://fred.example/%02d/callbacks/provision", i),
			DeliveryKind: CallbackDeliveryKindOperation,
			Status:       backend.CallbackStatusSuccess,
			CreatedAt:    time.Now(),
		}))
	}
	stopCtx, cancel := context.WithCancel(context.Background())
	cancel()
	s := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store:      store,
		HTTPClient: client,
		Secret:     "secret",
		Logger:     slog.Default(),
		StopCtx:    stopCtx,
		Backoff:    &zeroBackoff,
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.ReplayPendingCallbacks()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("canceled replay blocked while feeding the bounded worker pool")
	}
	assert.Zero(t, requests.Load())
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Len(t, pending, callbackReplayWorkerLimit*2)
}

func TestReplayPendingCallbacks_StopsLeaseAfterFirstFailureAfterReopen(t *testing.T) {
	var lifecycleAttempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/exact"+callbackurl.ProvisionPath {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		lifecycleAttempts.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	createdAt := time.Now()
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  server.URL + "/exact" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		Status:       backend.CallbackStatusSuccess,
		Backend:      "docker",
		CreatedAt:    createdAt,
	}))
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  server.URL + "/lifecycle" + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Success:      false,
		Status:       backend.CallbackStatusFailed,
		Backend:      "docker",
		Error:        "container exited",
		CreatedAt:    createdAt.Add(time.Second),
	}))
	require.NoError(t, store.Close())

	store, err = NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()
	newTestSender(t, store, server.Client(), "secret").ReplayPendingCallbacks()

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, server.URL+"/exact"+callbackurl.ProvisionPath, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	assert.Equal(t, server.URL+"/lifecycle"+callbackurl.ProvisionPath, pending[1].CallbackURL)
	assert.Zero(t, lifecycleAttempts.Load(), "the failed exact completion is a per-lease FIFO barrier")
}

func TestReplayPendingCallbacks_LegacyV013EntryRemainsQuarantined(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	storeLegacyCallback(t, store, CallbackEntry{
		LeaseUUID:   testLeaseUUID("lease-v013"),
		CallbackURL: server.URL + callbackurl.ProvisionPath,
		Success:     true,
		CreatedAt:   time.Now(),
	})
	require.ErrorIs(t, store.Healthy(), errLegacyCallbackOutboxNotDrained)
	newTestSender(t, store, server.Client(), "secret").ReplayPendingCallbacks()

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, callbackStorageLegacy, pending[0].storageVersion)
	assert.Zero(t, requests.Load(),
		"a current sender must not restamp a pre-identity row with the mounted lineage")
	require.NoError(t, store.Close())

	reopened, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	assert.Nil(t, reopened)
	require.ErrorIs(t, err, errLegacyCallbackOutboxNotDrained,
		"current startup must reject an undeliverable pre-identity queue")
}

func TestReplayPendingCallbacks_FailedLegacyHeadExpiresThenFreshV2Drains(t *testing.T) {
	var legacyAttempts atomic.Int32
	var typedAttempts atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/legacy" + callbackurl.ProvisionPath:
			legacyAttempts.Add(1)
			return callbackHTTPResponse(http.StatusServiceUnavailable), nil
		case "/typed" + callbackurl.ProvisionPath:
			typedAttempts.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		default:
			return callbackHTTPResponse(http.StatusNotFound), nil
		}
	})}

	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	storeLegacyCallback(t, store, CallbackEntry{
		LeaseUUID:   testLeaseUUID("lease-1"),
		CallbackURL: "https://fred.example/legacy/callbacks/provision",
		CreatedAt:   time.Now().Add(-48 * time.Hour),
	})
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "https://fred.example/typed/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)

	sender := newTestSender(t, store, client, "secret")
	sender.ReplayPendingCallbacks()
	assert.Zero(t, legacyAttempts.Load(),
		"a pre-identity legacy row is an operator-repair barrier, not deliverable authority")
	assert.Zero(t, typedAttempts.Load(), "a live legacy head remains a strict FIFO barrier")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 1, removed)
	pending, err = store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "https://fred.example/typed/callbacks/provision", pending[0].CallbackURL)

	sender.ReplayPendingCallbacks()
	assert.Equal(t, int32(1), typedAttempts.Load())
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

// TestReplayPendingCallbacks_PreservesStatusAndBackend verifies that current
// identity-bearing entries preserve both status classes and optional backend
// metadata across durable replay.
func TestReplayPendingCallbacks_PreservesStatusAndBackend(t *testing.T) {
	var receivedMu sync.Mutex
	var received []backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		receivedMu.Lock()
		received = append(received, p)
		receivedMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	// New-writer entry: Status and Backend present. Success encodes "not failed"
	// so a pre-Status binary rolling back replays this as 'success' rather than
	// 'failed' (avoiding spurious dashboard failures).
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-new"),
		CallbackURL:  server.URL + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Success:      true,
		Status:       backend.CallbackStatusDeprovisioned,
		Backend:      "docker",
		CreatedAt:    time.Now(),
	}))
	// Current exact failure without optional backend metadata.
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-legacy"),
		CallbackURL:  server.URL + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      false,
		Error:        "image pull failed",
		CreatedAt:    time.Now(),
	}))

	s := newTestSender(t, store, server.Client(), "secret")
	s.ReplayPendingCallbacks()

	receivedMu.Lock()
	defer receivedMu.Unlock()
	require.Len(t, received, 2)
	byID := map[string]backend.CallbackPayload{}
	for _, p := range received {
		byID[p.LeaseUUID] = p
	}
	assert.Equal(t, backend.CallbackStatusDeprovisioned, byID[testLeaseUUID("lease-new")].Status)
	assert.Equal(t, "docker", byID[testLeaseUUID("lease-new")].Backend)
	assert.Equal(t, backend.CallbackStatusFailed, byID[testLeaseUUID("lease-legacy")].Status)
	assert.Empty(t, byID[testLeaseUUID("lease-legacy")].Backend)
}

// TestSendCallback_ThreadsRetainedFlag verifies (ENG-329 #7) that the lifecycle
// callback's retained argument is threaded into the wire payload AND persisted on the
// CallbackEntry, so a restart-replay keeps the flag.
func TestSendCallback_ThreadsRetainedFlag(t *testing.T) {
	var received backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		// Fail delivery so the entry stays in the store for the assertions below.
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	s := newTestSender(t, store, server.Client(), "secret")
	s.SendLifecycleCallback(testLeaseUUID("lease-r"), server.URL+callbackurl.ProvisionPath, "docker", backend.CallbackStatusDeprovisioned, "", true)

	// The publisher only commits the row. Replay owns wire delivery.
	s.ReplayPendingCallbacks()

	// Wire payload carried the flag.
	assert.True(t, received.Retained, "retained flag must be threaded into the wire payload")

	// Persisted entry retained the flag (delivery failed → entry remains).
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.True(t, pending[0].Retained, "retained flag must be persisted on the CallbackEntry")
}

// TestReplayPendingCallbacks_PreservesRetained verifies the replay path re-sends
// the persisted Retained flag (so a callback delivered only after restart still
// tells providerd the data was retained).
func TestReplayPendingCallbacks_PreservesRetained(t *testing.T) {
	var received backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-r"),
		CallbackURL:  server.URL + callbackurl.ProvisionPath,
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Success:      true,
		Status:       backend.CallbackStatusDeprovisioned,
		Backend:      "docker",
		Retained:     true,
		CreatedAt:    time.Now(),
	}))

	s := newTestSender(t, store, server.Client(), "secret")
	s.ReplayPendingCallbacks()

	assert.Equal(t, testLeaseUUID("lease-r"), received.LeaseUUID)
	assert.True(t, received.Retained, "replayed callback must preserve the retained flag")
}

// TestSendCallback_LegacySuccessFieldEncodesNotFailed verifies that when a
// new binary writes a deprovisioned (or success) callback, the legacy Success
// field is true so a rollback to a pre-Status binary replays as 'success' on
// the wire rather than 'failed', preventing the spurious failure events this
// PR was designed to eliminate.
func TestSendCallback_LegacySuccessFieldEncodesNotFailed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError) // force persistence; never delivers
	}))
	defer server.Close()

	cases := []struct {
		name        string
		status      backend.CallbackStatus
		wantSuccess bool
	}{
		{"success", backend.CallbackStatusSuccess, true},
		{"failed", backend.CallbackStatusFailed, false},
		{"deprovisioned rolls back as success", backend.CallbackStatusDeprovisioned, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "cb.db")
			store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
			require.NoError(t, err)
			defer store.Close()

			s := newTestSender(t, store, server.Client(), "secret")
			if tc.status == backend.CallbackStatusDeprovisioned {
				s.SendLifecycleCallback(testLeaseUUID("lease-1"), server.URL+callbackurl.ProvisionPath, "docker", tc.status, "", false)
			} else {
				leaseUUID := testLeaseUUID("lease-1")
				callbackURL := server.URL + callbackurl.ProvisionPath
				beginCallbackSenderOperationIntent(t, store, leaseUUID, callbackURL, "docker", s.storageIdentity)
				s.SendOperationCallback(leaseUUID, callbackURL, "docker", tc.status, "")
			}

			pending, err := store.ListPending()
			require.NoError(t, err)
			require.Len(t, pending, 1)
			assert.Equal(t, tc.wantSuccess, pending[0].Success)
			assert.Equal(t, tc.status, pending[0].Status)
		})
	}
}

func TestReportDelivery_NilHook(t *testing.T) {
	s := newTestSender(t, nil, http.DefaultClient, "secret")
	// Should not panic
	s.reportDelivery("success")
	s.reportDelivery("failure")
}

func TestReportDelivery_WithHook(t *testing.T) {
	var outcomes []string
	s := MustNewEphemeralCallbackSender(CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     slog.Default(),
		StopCtx:    context.Background(),
		OnDelivery: func(outcome string) { outcomes = append(outcomes, outcome) },
	})

	s.reportDelivery("success")
	s.reportDelivery("failure")

	assert.Equal(t, []string{"success", "failure"}, outcomes)
}
