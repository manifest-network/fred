package shared

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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
)

// zeroBackoff is used in tests to eliminate retry delays.
var zeroBackoff = [CallbackMaxAttempts]time.Duration{}

// newTestSender creates a CallbackSender with zero backoff for fast tests.
func newTestSender(t *testing.T, store *CallbackStore, httpClient *http.Client, secret string) *CallbackSender {
	t.Helper()
	return NewCallbackSender(CallbackSenderConfig{
		Store:      store,
		HTTPClient: httpClient,
		Secret:     secret,
		Logger:     slog.Default(),
		StopCtx:    context.Background(),
		Backoff:    &zeroBackoff,
	})
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

func TestNewCallbackSender_PanicsOnNilHTTPClient(t *testing.T) {
	assert.Panics(t, func() {
		NewCallbackSender(CallbackSenderConfig{
			Logger:  slog.Default(),
			StopCtx: context.Background(),
		})
	})
}

func TestNewCallbackSender_PanicsOnNilLogger(t *testing.T) {
	assert.Panics(t, func() {
		NewCallbackSender(CallbackSenderConfig{
			HTTPClient: http.DefaultClient,
			StopCtx:    context.Background(),
		})
	})
}

func TestNewCallbackSender_PanicsOnNilStopCtx(t *testing.T) {
	assert.Panics(t, func() {
		NewCallbackSender(CallbackSenderConfig{
			HTTPClient: http.DefaultClient,
			Logger:     slog.Default(),
		})
	})
}

func TestNewCallbackSender_DefaultBackoff(t *testing.T) {
	s := NewCallbackSender(CallbackSenderConfig{
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
	s := NewCallbackSender(CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     slog.Default(),
		StopCtx:    context.Background(),
		Backoff:    &custom,
	})
	assert.Equal(t, custom, s.backoff)
}

func TestNewCallbackSender_PanicsOnNegativeReplayInterval(t *testing.T) {
	assert.Panics(t, func() {
		NewCallbackSender(CallbackSenderConfig{
			HTTPClient:     http.DefaultClient,
			Logger:         slog.Default(),
			StopCtx:        context.Background(),
			ReplayInterval: -time.Second,
		})
	})
}

func TestNewCallbackSender_PanicsOnNegativeDeliveryTimeout(t *testing.T) {
	assert.Panics(t, func() {
		NewCallbackSender(CallbackSenderConfig{
			HTTPClient:      http.DefaultClient,
			Logger:          slog.Default(),
			StopCtx:         context.Background(),
			DeliveryTimeout: -time.Nanosecond,
		})
	})
}

func TestSendCallback_EmptyURL(t *testing.T) {
	s := newTestSender(t, nil, http.DefaultClient, "secret")
	// Should not panic, just log a warning
	s.SendOperationCallback("lease-1", "", "test-backend", backend.CallbackStatusSuccess, "")
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
	s.SendOperationCallback("lease-1", server.URL, "test-backend", backend.CallbackStatusSuccess, "")

	assert.Equal(t, "lease-1", received.LeaseUUID)
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
	s.SendOperationCallback("lease-1", server.URL, "test-backend", backend.CallbackStatusFailed, "image pull failed")

	assert.Equal(t, backend.CallbackStatusFailed, received.Status)
	assert.Equal(t, "image pull failed", received.Error)
}

func TestSendCallback_PersistsAndRemoves(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	s := newTestSender(t, store, server.Client(), "secret")
	s.SendOperationCallback("lease-1", server.URL, "test-backend", backend.CallbackStatusSuccess, "")

	// After successful delivery, store should be empty
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendCallback_FailedDeliveryRemainsInStore(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	s := newTestSender(t, store, server.Client(), "secret")
	s.SendOperationCallback("lease-1", server.URL, "test-backend", backend.CallbackStatusFailed, "error")

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "lease-1", pending[0].LeaseUUID)
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

			s.SendLifecycleCallback("lease-1", "https://fred.example/callback", "docker", tc.firstStatus, "first", false)
			first, err := store.ListPending()
			require.NoError(t, err)
			require.Len(t, first, 1)

			s.SendLifecycleCallback("lease-1", "https://fred.example/callback", "docker", tc.latestStatus, "latest", false)
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
		"https://fred.example/callback?trace=keep&operation%5fid=" + id,
		"https://fred.example/callback?trace=%ZZ&lifecycle_id=" + id,
		"https://fred.example/callback?trace=x;y&lifecycle_id=" + id,
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
		"https://fred.example/callback?trace=keep&lifecycle%5fid=" + id,
		"https://fred.example/callback?operation_id=" + id + "&lifecycle_id=" + id,
		"https://fred.example/callback?operation_id=" + id + "&operation_id=" + id,
		"https://fred.example/callback?trace=%ZZ&operation_id=" + id,
		"https://fred.example/callback?trace=x;y&operation_id=" + id,
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

	s.SendOperationCallback(
		"typed", "https://fred.example/callback?operation_id=550e8400-e29b-41d4-a716-446655440000",
		"docker", backend.CallbackStatusSuccess, "",
	)
	s.SendOperationCallback(
		"legacy", "https://fred.example/callback?trace=keep",
		"docker", backend.CallbackStatusSuccess, "",
	)

	assert.Equal(t, int32(2), requests.Load())
	pending, err := store.ListPending()
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
		"lease-1",
		"https://fred.example/callback?trace=keep&lifecycle_id=550e8400-e29b-41d4-a716-446655440000",
		"docker",
		backend.CallbackStatusFailed,
		"container exited",
		false,
	)

	assert.Equal(t, int32(1), requests.Load())
	pending, err := store.ListPending()
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
		"https://fred.example/callback?operation_id=550e8400-e29b-41d4-a716-446655440000",
		"docker",
		backend.CallbackStatusDeprovisioned,
		"",
	)
	s.SendOperationCallback(
		"lease-operation-unknown",
		"https://fred.example/callback",
		"docker",
		backend.CallbackStatus("unknown"),
		"",
	)
	s.SendLifecycleCallback(
		"lease-lifecycle-unknown",
		"https://fred.example/callback",
		"docker",
		backend.CallbackStatus("unknown"),
		"",
		false,
	)
	s.SendLifecycleCallback(
		"lease-retained",
		"https://fred.example/callback",
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

func TestSendOperationCallback_StoreFailureSuppressesDirectDelivery(t *testing.T) {
	var requests atomic.Int32
	var storeErrors atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	require.NoError(t, store.Close())
	s := NewCallbackSender(CallbackSenderConfig{
		Store:        store,
		HTTPClient:   client,
		Secret:       "secret",
		Logger:       slog.Default(),
		StopCtx:      context.Background(),
		Backoff:      &zeroBackoff,
		OnStoreError: func() { storeErrors.Add(1) },
	})

	s.SendOperationCallback("lease-1", "https://fred.example/callback", "docker", backend.CallbackStatusSuccess, "")

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
	s := NewCallbackSender(CallbackSenderConfig{
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

func TestCallbackSender_DifferentLeasesDoNotShareDeliveryLock(t *testing.T) {
	blockedStarted := make(chan struct{})
	releaseBlocked := make(chan struct{})
	otherDelivered := make(chan struct{})
	var startOnce sync.Once
	var otherOnce sync.Once
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/blocked":
			startOnce.Do(func() { close(blockedStarted) })
			<-releaseBlocked
		case "/other":
			otherOnce.Do(func() { close(otherDelivered) })
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	s := newTestSender(t, nil, client, "secret")
	blockedDone := make(chan struct{})
	go func() {
		defer close(blockedDone)
		s.SendOperationCallback("blocked-lease", "https://fred.example/blocked", "docker", backend.CallbackStatusSuccess, "")
	}()
	<-blockedStarted

	otherDone := make(chan struct{})
	go func() {
		defer close(otherDone)
		s.SendOperationCallback("other-lease", "https://fred.example/other", "docker", backend.CallbackStatusSuccess, "")
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

func TestCallbackSender_PeriodicReplayRecoversFromTransientOutage(t *testing.T) {
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
	s := NewCallbackSender(CallbackSenderConfig{
		Store:          store,
		HTTPClient:     client,
		Secret:         "secret",
		Logger:         slog.Default(),
		StopCtx:        stopCtx,
		Backoff:        &zeroBackoff,
		ReplayInterval: 5 * time.Millisecond,
	})

	s.SendOperationCallback("lease-1", "https://fred.example/callback", "docker", backend.CallbackStatusSuccess, "")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "initial retries must leave the exact completion durable")
	assert.Equal(t, int32(CallbackMaxAttempts), attempts.Load())

	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		s.RunReplayLoop()
	}()
	available.Store(true)
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
	assert.Greater(t, attempts.Load(), int32(CallbackMaxAttempts))
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
		if r.URL.Path == "/exact" && !exactAvailable.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.URL.Path == "/lifecycle" {
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
	s.SendOperationCallback("lease-1", server.URL+"/exact", "docker", backend.CallbackStatusSuccess, "")
	s.SendLifecycleCallback("lease-1", server.URL+"/lifecycle", "docker", backend.CallbackStatusFailed, "container exited", false)

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, server.URL+"/exact", pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	assert.Equal(t, server.URL+"/lifecycle", pending[1].CallbackURL)
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
		if req.URL.Path == "/exact" {
			exactOnce.Do(func() { close(exactStarted) })
			<-releaseExact
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "https://fred.example/exact",
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
			"lease-1",
			"https://fred.example/lifecycle",
			"docker",
			backend.CallbackStatusFailed,
			"container exited",
			false,
		)
	}()
	require.Eventually(t, func() bool {
		s.deliveryLocksMu.Lock()
		defer s.deliveryLocksMu.Unlock()
		lock := s.deliveryLocks["lease-1"]
		return lock != nil && lock.refs == 2
	}, time.Second, time.Millisecond, "live enqueue must join the same lease lock")
	pending, err := store.listPending("lease-1")
	require.NoError(t, err)
	require.Len(t, pending, 1, "the live enqueue must not mutate the FIFO while replay is delivering its head")
	assert.Equal(t, CallbackDeliveryKindOperation, pending[0].DeliveryKind)

	close(releaseExact)
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("replay did not finish")
	}
	select {
	case <-liveDone:
	case <-time.After(time.Second):
		t.Fatal("live enqueue did not finish")
	}

	deliveredMu.Lock()
	assert.Equal(t, []string{"/exact", "/lifecycle"}, delivered)
	deliveredMu.Unlock()
	pending, err = store.listPending("lease-1")
	require.NoError(t, err)
	assert.Empty(t, pending)
	s.deliveryLocksMu.Lock()
	assert.Empty(t, s.deliveryLocks, "concurrent replay/send must retire its keyed lock")
	s.deliveryLocksMu.Unlock()
}

func TestCallbackSender_ExpirySkipsBusyLeaseWithoutMutatingDrain(t *testing.T) {
	exactStarted := make(chan struct{})
	releaseExact := make(chan struct{})
	var exactOnce sync.Once
	var lifecycleRequests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/exact":
			exactOnce.Do(func() {
				close(exactStarted)
				<-releaseExact
			})
			return callbackHTTPResponse(http.StatusServiceUnavailable), nil
		case "/lifecycle":
			lifecycleRequests.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		default:
			return callbackHTTPResponse(http.StatusNotFound), nil
		}
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	_, err = store.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "https://fred.example/exact",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	})
	require.NoError(t, err)
	_, err = store.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "https://fred.example/lifecycle",
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
	pending, err := store.listPending("lease-1")
	require.NoError(t, err)
	require.Len(t, pending, 2, "cleanup must not mutate an in-flight drain snapshot")

	close(releaseExact)
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("replay did not stop after exact delivery failure")
	}
	assert.Zero(t, lifecycleRequests.Load(), "lifecycle suffix must not pass the failed exact head")
	pending, err = store.listPending("lease-1")
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, CallbackDeliveryKindOperation, pending[0].DeliveryKind)
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[1].DeliveryKind)

	removed, err = store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 2, removed, "a later pass may atomically expire the idle typed queue")
	pending, err = store.listPending("lease-1")
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackSender_CancelSerializesReplayAndSuppressesWaitingOwnedEnqueue(t *testing.T) {
	exactStarted := make(chan struct{})
	releaseExact := make(chan struct{})
	var exactOnce sync.Once
	var newRequests atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/old":
			exactOnce.Do(func() {
				close(exactStarted)
				<-releaseExact
			})
			return callbackHTTPResponse(http.StatusServiceUnavailable), nil
		case "/new":
			newRequests.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		default:
			return callbackHTTPResponse(http.StatusNotFound), nil
		}
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "https://fred.example/old",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusFailed,
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
		t.Fatal("replay did not begin exact delivery")
	}

	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	enqueueDone := make(chan struct{})
	go func() {
		defer close(enqueueDone)
		s.SendOperationCallbackContext(
			ownerCtx, "lease-1", "https://fred.example/new", "k3s",
			backend.CallbackStatusFailed, "stale worker",
		)
	}()
	require.Eventually(t, func() bool {
		s.deliveryLocksMu.Lock()
		defer s.deliveryLocksMu.Unlock()
		lock := s.deliveryLocks["lease-1"]
		return lock != nil && lock.refs >= 2
	}, time.Second, time.Millisecond)
	cancelOwner()

	cancelDone := make(chan error, 1)
	go func() { cancelDone <- s.CancelLeaseCallbacks("lease-1") }()
	select {
	case err := <-cancelDone:
		t.Fatalf("cancellation bypassed the in-flight lease drain: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseExact)
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("replay did not finish")
	}
	select {
	case <-enqueueDone:
	case <-time.After(time.Second):
		t.Fatal("owned enqueue did not observe cancellation")
	}
	select {
	case err := <-cancelDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("serialized callback cancellation did not finish")
	}

	assert.Zero(t, newRequests.Load(), "canceled worker must not deliver or persist after teardown")
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
	s.deliveryLocksMu.Lock()
	assert.Empty(t, s.deliveryLocks, "cancel/enqueue/replay must retire their keyed lock")
	s.deliveryLocksMu.Unlock()
}

func TestDeliverCallback_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := newTestSender(t, nil, server.Client(), "secret")
	ok := s.DeliverCallback("lease-1", server.URL, []byte(`{"test":true}`))
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
	ok := s.DeliverCallback("lease-1", server.URL, []byte(`{}`))
	assert.True(t, ok)
	assert.Equal(t, int32(3), attempts.Load())
}

func TestDeliverCallback_AllRetriesFail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	s := newTestSender(t, nil, server.Client(), "secret")
	ok := s.DeliverCallback("lease-1", server.URL, []byte(`{}`))
	assert.False(t, ok)
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
	s := NewCallbackSender(CallbackSenderConfig{
		HTTPClient: server.Client(),
		Logger:     slog.Default(),
		StopCtx:    ctx,
		Backoff:    &longBackoff,
	})

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	ok := s.DeliverCallback("lease-1", server.URL, []byte(`{}`))
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
	s := NewCallbackSender(CallbackSenderConfig{
		HTTPClient:      client,
		Logger:          slog.Default(),
		StopCtx:         context.Background(),
		Backoff:         &zeroBackoff,
		DeliveryTimeout: deliveryTimeout,
	})

	started := time.Now()
	ok := s.DeliverCallback("lease-1", "https://fred.example/callback", []byte(`{}`))

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
	s := NewCallbackSender(CallbackSenderConfig{
		HTTPClient:      client,
		Logger:          slog.Default(),
		StopCtx:         context.Background(),
		Backoff:         &zeroBackoff,
		DeliveryTimeout: deliveryTimeout,
	})

	delivered := s.DeliverCallback(
		"lease-1", "https://fred.example/callback", []byte(`{}`),
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
	s := NewCallbackSender(CallbackSenderConfig{
		HTTPClient:      client,
		Logger:          slog.Default(),
		StopCtx:         stopCtx,
		Backoff:         &zeroBackoff,
		DeliveryTimeout: deliveryTimeout,
	})

	delivered := make(chan bool, 1)
	go func() {
		delivered <- s.DeliverCallback(
			"lease-1", "https://fred.example/callback", []byte(`{}`),
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
		"all inline attempts must inherit the exact same delivery deadline")
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
	s := NewCallbackSender(CallbackSenderConfig{
		HTTPClient:      client,
		Logger:          slog.Default(),
		StopCtx:         stopCtx,
		DeliveryTimeout: time.Minute,
	})

	delivered := make(chan bool, 1)
	go func() {
		delivered <- s.DeliverCallback(
			"lease-1", "https://fred.example/callback", []byte(`{}`),
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

	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  server.URL,
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "lease-2",
		CallbackURL:  server.URL,
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
		if r.URL.Path == "/success" {
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

	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  server.URL + "/success",
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "lease-2",
		CallbackURL:  server.URL + "/failure",
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
	assert.Equal(t, "lease-2", pending[0].LeaseUUID)
}

func TestReplayPendingCallbacks_FailureBlocksOnlyItsLease(t *testing.T) {
	var blockedAttempts atomic.Int32
	var overtakingLifecycleAttempts atomic.Int32
	var otherLeaseAttempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/blocked-exact":
			blockedAttempts.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		case "/same-lease-lifecycle":
			overtakingLifecycleAttempts.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case "/other-lease":
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
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "blocked-lease",
		CallbackURL:  server.URL + "/blocked-exact",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "blocked-lease",
		CallbackURL:  server.URL + "/same-lease-lifecycle",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       backend.CallbackStatusFailed,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "healthy-lease",
		CallbackURL:  server.URL + "/other-lease",
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
	assert.Equal(t, "blocked-lease", pending[0].LeaseUUID)
	assert.Equal(t, "blocked-lease", pending[1].LeaseUUID)
}

func TestReplayPendingCallbacks_BlockedLeaseDoesNotDelayAnotherLease(t *testing.T) {
	blockedStarted := make(chan struct{})
	releaseBlocked := make(chan struct{})
	otherDelivered := make(chan struct{})
	var blockedOnce sync.Once
	var otherOnce sync.Once
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/blocked":
			blockedOnce.Do(func() { close(blockedStarted) })
			<-releaseBlocked
		case "/other":
			otherOnce.Do(func() { close(otherDelivered) })
		}
		return callbackHTTPResponse(http.StatusNoContent), nil
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	for _, entry := range []CallbackEntry{
		{
			LeaseUUID:    "blocked-lease",
			CallbackURL:  "https://fred.example/blocked",
			DeliveryKind: CallbackDeliveryKindOperation,
			Status:       backend.CallbackStatusSuccess,
			CreatedAt:    time.Now(),
		},
		{
			LeaseUUID:    "other-lease",
			CallbackURL:  "https://fred.example/other",
			DeliveryKind: CallbackDeliveryKindOperation,
			Status:       backend.CallbackStatusSuccess,
			CreatedAt:    time.Now(),
		},
	} {
		require.NoError(t, store.Store(entry))
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
		require.NoError(t, store.Store(CallbackEntry{
			LeaseUUID:    fmt.Sprintf("lease-%02d", i),
			CallbackURL:  fmt.Sprintf("https://fred.example/%02d", i),
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
	case <-time.After(time.Second):
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
		if req.URL.Path == "/healthy" {
			healthyDelivered.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		}
		panic("synthetic callback transport panic")
	})}
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	for i := range callbackReplayWorkerLimit {
		require.NoError(t, store.Store(CallbackEntry{
			LeaseUUID:    fmt.Sprintf("panic-lease-%02d", i),
			CallbackURL:  fmt.Sprintf("https://fred.example/panic-%02d", i),
			DeliveryKind: CallbackDeliveryKindOperation,
			Status:       backend.CallbackStatusSuccess,
			CreatedAt:    time.Now(),
		}))
	}
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "healthy-lease",
		CallbackURL:  "https://fred.example/healthy",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	}))
	s := NewCallbackSender(CallbackSenderConfig{
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
		assert.NotEqual(t, "healthy-lease", entry.LeaseUUID)
	}
	s.deliveryLocksMu.Lock()
	assert.Empty(t, s.deliveryLocks, "panic recovery must still release and retire keyed locks")
	s.deliveryLocksMu.Unlock()
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
		require.NoError(t, store.Store(CallbackEntry{
			LeaseUUID:    fmt.Sprintf("lease-%02d", i),
			CallbackURL:  fmt.Sprintf("https://fred.example/%02d", i),
			DeliveryKind: CallbackDeliveryKindOperation,
			Status:       backend.CallbackStatusSuccess,
			CreatedAt:    time.Now(),
		}))
	}
	stopCtx, cancel := context.WithCancel(context.Background())
	cancel()
	s := NewCallbackSender(CallbackSenderConfig{
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
		if r.URL.Path == "/exact" {
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
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  server.URL + "/exact",
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		Status:       backend.CallbackStatusSuccess,
		Backend:      "docker",
		CreatedAt:    createdAt,
	}))
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  server.URL + "/lifecycle",
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
	assert.Equal(t, server.URL+"/exact", pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	assert.Equal(t, server.URL+"/lifecycle", pending[1].CallbackURL)
	assert.Zero(t, lifecycleAttempts.Load(), "the failed exact completion is a per-lease FIFO barrier")
}

func TestReplayPendingCallbacks_LegacyV013Entry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	storeLegacyCallback(t, store, CallbackEntry{
		LeaseUUID:   "lease-v013",
		CallbackURL: server.URL,
		Success:     true,
		CreatedAt:   time.Now(),
	})
	require.NoError(t, store.Close())

	store, err = NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()
	newTestSender(t, store, server.Client(), "secret").ReplayPendingCallbacks()

	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestReplayPendingCallbacks_FailedLegacyHeadExpiresThenFreshV2Drains(t *testing.T) {
	var legacyAttempts atomic.Int32
	var typedAttempts atomic.Int32
	client := &http.Client{Transport: callbackRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/legacy":
			legacyAttempts.Add(1)
			return callbackHTTPResponse(http.StatusServiceUnavailable), nil
		case "/typed":
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
		LeaseUUID:   "lease-1",
		CallbackURL: "https://fred.example/legacy",
		CreatedAt:   time.Now().Add(-48 * time.Hour),
	})
	_, err = store.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "https://fred.example/typed",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusSuccess,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)

	sender := newTestSender(t, store, client, "secret")
	sender.ReplayPendingCallbacks()
	assert.Equal(t, int32(CallbackMaxAttempts), legacyAttempts.Load())
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
	assert.Equal(t, "https://fred.example/typed", pending[0].CallbackURL)

	sender.ReplayPendingCallbacks()
	assert.Equal(t, int32(1), typedAttempts.Load())
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

// TestReplayPendingCallbacks_PreservesStatusAndBackend verifies that entries
// written by the new writer (with Status and Backend populated) replay with
// those fields intact, while legacy entries (only Success bool) still replay
// correctly via the bool fallback. Regression guard for the fix: without the
// fallback, every persisted deprovisioned entry would replay as "failed".
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
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "lease-new",
		CallbackURL:  server.URL,
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Success:      true,
		Status:       backend.CallbackStatusDeprovisioned,
		Backend:      "docker",
		CreatedAt:    time.Now(),
	}))
	// Legacy entry: only Success, simulating a pre-upgrade write.
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "lease-legacy",
		CallbackURL:  server.URL,
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
	assert.Equal(t, backend.CallbackStatusDeprovisioned, byID["lease-new"].Status)
	assert.Equal(t, "docker", byID["lease-new"].Backend)
	assert.Equal(t, backend.CallbackStatusFailed, byID["lease-legacy"].Status)
	assert.Empty(t, byID["lease-legacy"].Backend)
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
	s.SendLifecycleCallback("lease-r", server.URL, "docker", backend.CallbackStatusDeprovisioned, "", true)

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

	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "lease-r",
		CallbackURL:  server.URL,
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Success:      true,
		Status:       backend.CallbackStatusDeprovisioned,
		Backend:      "docker",
		Retained:     true,
		CreatedAt:    time.Now(),
	}))

	s := newTestSender(t, store, server.Client(), "secret")
	s.ReplayPendingCallbacks()

	assert.Equal(t, "lease-r", received.LeaseUUID)
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
				s.SendLifecycleCallback("lease-1", server.URL, "docker", tc.status, "", false)
			} else {
				s.SendOperationCallback("lease-1", server.URL, "docker", tc.status, "")
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
	s := NewCallbackSender(CallbackSenderConfig{
		HTTPClient: http.DefaultClient,
		Logger:     slog.Default(),
		StopCtx:    context.Background(),
		OnDelivery: func(outcome string) { outcomes = append(outcomes, outcome) },
	})

	s.reportDelivery("success")
	s.reportDelivery("failure")

	assert.Equal(t, []string{"success", "failure"}, outcomes)
}
