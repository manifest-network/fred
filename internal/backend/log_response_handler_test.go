package backend

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogsHandlerTimeoutRetainsAdmissionUntilWorkerExits(t *testing.T) {
	entered, finish, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	h := NewLogsHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			close(entered)
			<-r.Context().Done()
			<-finish // Cancellation does not imply that a backend has unwound.
			defer close(exited)
		}
		_, _ = io.WriteString(w, `{}`)
	}), 20*time.Millisecond)
	first := httptest.NewRecorder()
	h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/logs/tenant-one", nil))
	<-entered
	require.Equal(t, http.StatusServiceUnavailable, first.Code)
	require.Contains(t, first.Body.String(), "request timeout")
	assertLogCapacityExhausted(t, h)
	require.EqualValues(t, 1, calls.Load())
	close(finish)
	<-exited
	require.Eventually(t, func() bool {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/logs/tenant-two", nil))
		return r.Code == http.StatusOK
	}, time.Second, time.Millisecond)
}

func assertLogCapacityExhausted(t *testing.T, h http.Handler) {
	t.Helper()
	r := httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/logs/another-tenant", nil))
	require.Equal(t, http.StatusServiceUnavailable, r.Code)
	require.Contains(t, r.Body.String(), "capacity exhausted")
	require.Equal(t, "1", r.Header().Get("Retry-After"))
}

type blockedLogResponseWriter struct {
	*httptest.ResponseRecorder
	entered chan struct{}
	release chan struct{}
}

func (w *blockedLogResponseWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	return w.ResponseRecorder.Write(p)
}

func TestLogsHandlerRetainsAdmissionThroughFinalClientWrite(t *testing.T) {
	var calls atomic.Int32
	h := NewLogsHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, `{"live/0":"done"}`)
	}), time.Second)
	w := &blockedLogResponseWriter{httptest.NewRecorder(), make(chan struct{}), make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/logs/one", nil))
	}()
	<-w.entered // Worker exited; TimeoutHandler still owns its escaped buffer.
	assertLogCapacityExhausted(t, h)
	require.EqualValues(t, 1, calls.Load())
	close(w.release)
	<-done
	second := httptest.NewRecorder()
	h.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/logs/two", nil))
	require.Equal(t, http.StatusOK, second.Code)
	require.EqualValues(t, 2, calls.Load())
}

func TestLogsHandlerReleasesAdmissionAfterPanic(t *testing.T) {
	var calls atomic.Int32
	h := NewLogsHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			panic("backend panic")
		}
		_, _ = io.WriteString(w, `{}`)
	}), time.Second)
	require.Panics(t, func() { h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/logs/one", nil)) })
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/logs/two", nil))
	require.Equal(t, http.StatusOK, w.Code)
}

func TestLogsHandlerCarriesFullEscapedBudgetAndBothNamespaces(t *testing.T) {
	// Keep the wire fixture modest in the short race suite while exercising
	// escaped fragments and both namespaces through the real timeout wrapper.
	logs := map[string]string{"web/0": strings.Repeat("\x00", 3<<20), "failed/web/0": "failed"}
	response, err := NewLogResponse(logs)
	require.NoError(t, err)
	h := NewLogsHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = response.WriteJSON(w)
	}), 10*time.Second)
	server := httptest.NewServer(h)
	defer server.Close()
	client := newUnboundHTTPClientForTest(HTTPClientConfig{Name: "bounded-logs", BaseURL: server.URL, Timeout: 10 * time.Second})
	got, err := client.GetLogs(t.Context(), "lease", 100)
	require.NoError(t, err)
	require.Equal(t, logs, got)
}
