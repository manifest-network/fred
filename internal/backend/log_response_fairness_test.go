package backend

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogsHandlerTimedOutWorkerStillOwnsItsQueueSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		finish := make(chan struct{})
		release := sync.OnceFunc(func() { close(finish) })
		defer release()
		var calls atomic.Int32
		h := NewLogsHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) == 1 {
				<-r.Context().Done()
				<-finish
			}
			_, _ = io.WriteString(w, `{}`)
		}), time.Second, time.Second)
		first := httptest.NewRecorder()
		h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/logs/first", nil))
		require.Equal(t, http.StatusServiceUnavailable, first.Code)
		require.Contains(t, first.Body.String(), "request timeout")

		var waiting sync.WaitGroup
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		for range maxQueuedLogResponses {
			waiting.Go(func() {
				h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(ctx, http.MethodGet, "/logs/waiting", nil))
			})
			synctest.Wait()
		}
		start := time.Now()
		overflow := httptest.NewRecorder()
		h.ServeHTTP(overflow, httptest.NewRequest(http.MethodGet, "/logs/overflow", nil))
		require.Equal(t, time.Duration(0), time.Since(start), "a worker surviving timeout still owns one of the nine slots")
		require.Equal(t, http.StatusServiceUnavailable, overflow.Code)
		require.Equal(t, "1", overflow.Header().Get("Retry-After"))
		require.EqualValues(t, 1, calls.Load(), "queued requests cannot materialize another response")
		cancel()
		waiting.Wait()
		release()
		synctest.Wait()
		next := httptest.NewRecorder()
		h.ServeHTTP(next, httptest.NewRequest(http.MethodGet, "/logs/next", nil))
		require.Equal(t, http.StatusOK, next.Code, "canceled waiters must not leak slots or the materialization permit")
	})
}

func TestLogsHandlerQueueWaitDoesNotRenewRequestBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		finish := make(chan struct{})
		release := sync.OnceFunc(func() { close(finish) })
		defer release()
		var calls atomic.Int32
		observed := make(chan time.Time, 1)
		h := NewLogsHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) == 1 {
				<-finish
			} else {
				deadline, _ := r.Context().Deadline()
				observed <- deadline
			}
			_, _ = io.WriteString(w, `{}`)
		}), time.Second, time.Second)
		var requests sync.WaitGroup
		requests.Go(func() { h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/logs/first", nil)) })
		synctest.Wait()
		start := time.Now()
		second := httptest.NewRecorder()
		requests.Go(func() { h.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/logs/second", nil)) })
		synctest.Wait()
		time.Sleep(250 * time.Millisecond)
		release()
		requests.Wait()
		require.Equal(t, http.StatusOK, second.Code)
		require.Equal(t, start.Add(time.Second), <-observed)
	})
}

type observedSocketLogWriter struct {
	http.ResponseWriter
	started chan struct{}
	once    sync.Once
}

func (w *observedSocketLogWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *observedSocketLogWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	return w.ResponseWriter.Write(p)
}

func TestLogsHandlerStalledSocketReleasesToWaitingReader(t *testing.T) {
	for _, tc := range []struct {
		name string
		new  func(http.Handler, time.Duration, time.Duration) http.Handler
	}{
		{name: "backend", new: NewLogsHandler},
		{name: "prepared_tenant", new: preparedLogsTestHandler},
	} {
		t.Run(tc.name, func(t *testing.T) { testLogsStalledSocket(t, tc.new) })
	}
}

func testLogsStalledSocket(t *testing.T, constructor func(http.Handler, time.Duration, time.Duration) http.Handler) {
	t.Helper()
	// Real TCP flow control, not a fake writer: the first client never reads,
	// and its receive window is smaller than the materialized response.
	writeStarted := make(chan struct{})
	firstDone := make(chan struct{})
	var calls atomic.Int32
	h := constructor(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/stalled" {
			_, _ = io.WriteString(w, strings.Repeat("x", 8<<20))
			return
		}
		_, _ = io.WriteString(w, "healthy")
	}), 10*time.Second, 250*time.Millisecond)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stalled" {
			defer close(firstDone)
			w = &observedSocketLogWriter{ResponseWriter: w, started: writeStarted}
		}
		h.ServeHTTP(w, r)
	}))
	server.Config.WriteTimeout = time.Minute
	server.Start()
	defer server.Close()
	conn, err := net.DialTimeout("tcp", server.Listener.Addr().String(), time.Second)
	require.NoError(t, err)
	defer conn.Close()
	tcp, ok := conn.(*net.TCPConn)
	require.True(t, ok)
	require.NoError(t, tcp.SetReadBuffer(1024))
	_, err = fmt.Fprintf(conn, "GET /stalled HTTP/1.1\r\nHost: %s\r\n\r\n", server.Listener.Addr())
	require.NoError(t, err)
	select {
	case <-writeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first response did not reach its socket write")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/healthy", nil)
	require.NoError(t, err)
	response, err := server.Client().Do(req)
	require.NoError(t, err, "waiting reader must progress before the unrelated one-minute server timeout")
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, "healthy", string(body))
	require.EqualValues(t, 2, calls.Load())
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("stalled socket retained the response permit after its write deadline")
	}
}
