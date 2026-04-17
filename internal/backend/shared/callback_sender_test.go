package shared

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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

func TestSendCallback_EmptyURL(t *testing.T) {
	s := newTestSender(t, nil, http.DefaultClient, "secret")
	// Should not panic, just log a warning
	s.SendCallback("lease-1", "", "test-backend", backend.CallbackStatusSuccess, "")
}

func TestSendCallback_SuccessDelivery(t *testing.T) {
	const secret = "test-secret-32-chars-long-enough"
	var received backend.CallbackPayload
	var capturedBody []byte
	var capturedSig string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedSig = r.Header.Get(hmacauth.SignatureHeader)
		capturedBody, _ = io.ReadAll(r.Body)
		json.Unmarshal(capturedBody, &received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := newTestSender(t, nil, server.Client(), secret)
	s.SendCallback("lease-1", server.URL, "test-backend", backend.CallbackStatusSuccess, "")

	assert.Equal(t, "lease-1", received.LeaseUUID)
	assert.Equal(t, backend.CallbackStatusSuccess, received.Status)
	assert.Equal(t, "test-backend", received.Backend)

	// Verify HMAC signature is present and valid
	assert.NotEmpty(t, capturedSig, "HMAC signature header must be set")
	assert.NoError(t, hmacauth.Verify(secret, capturedBody, capturedSig, time.Minute))
}

func TestSendCallback_FailurePayload(t *testing.T) {
	var received backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := newTestSender(t, nil, server.Client(), "secret")
	s.SendCallback("lease-1", server.URL, "test-backend", backend.CallbackStatusFailed, "image pull failed")

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
	s.SendCallback("lease-1", server.URL, "test-backend", backend.CallbackStatusSuccess, "")

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
	s.SendCallback("lease-1", server.URL, "test-backend", backend.CallbackStatusFailed, "error")

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "lease-1", pending[0].LeaseUUID)
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
	var received []backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		received = append(received, p)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	dbPath := filepath.Join(t.TempDir(), "cb.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:   "lease-1",
		CallbackURL: server.URL,
		Success:     true,
		CreatedAt:   time.Now(),
	}))
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:   "lease-2",
		CallbackURL: server.URL,
		Success:     false,
		Error:       "pull failed",
		CreatedAt:   time.Now(),
	}))

	s := newTestSender(t, store, server.Client(), "secret")
	s.ReplayPendingCallbacks()

	assert.Len(t, received, 2)

	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestReplayPendingCallbacks_PartialFailure(t *testing.T) {
	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n <= 1 {
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
		LeaseUUID:   "lease-1",
		CallbackURL: server.URL,
		Success:     true,
		CreatedAt:   time.Now(),
	}))
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:   "lease-2",
		CallbackURL: server.URL,
		Success:     false,
		Error:       "error",
		CreatedAt:   time.Now(),
	}))

	s := newTestSender(t, store, server.Client(), "secret")
	s.ReplayPendingCallbacks()

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "lease-2", pending[0].LeaseUUID)
}

// TestReplayPendingCallbacks_PreservesStatusAndBackend verifies that entries
// written by the new writer (with Status and Backend populated) replay with
// those fields intact, while legacy entries (only Success bool) still replay
// correctly via the bool fallback. Regression guard for the fix: without the
// fallback, every persisted deprovisioned entry would replay as "failed".
func TestReplayPendingCallbacks_PreservesStatusAndBackend(t *testing.T) {
	var received []backend.CallbackPayload
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		received = append(received, p)
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
		LeaseUUID:   "lease-new",
		CallbackURL: server.URL,
		Success:     true,
		Status:      backend.CallbackStatusDeprovisioned,
		Backend:     "docker",
		CreatedAt:   time.Now(),
	}))
	// Legacy entry: only Success, simulating a pre-upgrade write.
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:   "lease-legacy",
		CallbackURL: server.URL,
		Success:     false,
		Error:       "image pull failed",
		CreatedAt:   time.Now(),
	}))

	s := newTestSender(t, store, server.Client(), "secret")
	s.ReplayPendingCallbacks()

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
			s.SendCallback("lease-1", server.URL, "docker", tc.status, "")

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
