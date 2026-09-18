package backend

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPreparedLogsTimedOutPreparationsKeepBoundedRequestOwnership(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		finish := make(chan struct{})
		release := sync.OnceFunc(func() { close(finish) })
		defer release()
		var preparations, reads atomic.Int32
		h := NewPreparedTenantLogsHandler(func(_ http.ResponseWriter, r *http.Request) http.Handler {
			preparations.Add(1)
			<-r.Context().Done()
			<-finish
			return http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reads.Add(1) })
		}, time.Second, time.Second)
		var requests sync.WaitGroup
		for range 1 + maxQueuedLogResponses {
			requests.Go(func() {
				w := httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/logs", nil))
				require.Equal(t, http.StatusServiceUnavailable, w.Code)
			})
		}
		synctest.Wait()
		require.EqualValues(t, 1+maxQueuedLogResponses, preparations.Load())
		time.Sleep(time.Second)
		requests.Wait() // Each response timed out, but its worker is still alive.
		w := httptest.NewRecorder()
		start := time.Now()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/logs", nil))
		require.Equal(t, start, time.Now(), "all request slots remain owned despite response timeout")
		require.Equal(t, http.StatusServiceUnavailable, w.Code)
		require.Equal(t, "1", w.Header().Get("Retry-After"))
		require.Contains(t, w.Body.String(), "capacity exhausted")
		require.EqualValues(t, 1+maxQueuedLogResponses, preparations.Load())
		release()
		synctest.Wait()
		require.Zero(t, reads.Load(), "expired preparation cannot acquire materialization")
	})
}

// preparedLogsTestHandler adapts tests of the response lifetime to the provider's
// preparation boundary without adding preparation work of its own.
func preparedLogsTestHandler(next http.Handler, timeout, writeTimeout time.Duration) http.Handler {
	return NewPreparedTenantLogsHandler(func(http.ResponseWriter, *http.Request) http.Handler {
		return next
	}, timeout, writeTimeout)
}

func TestPreparedLogsKeepSingleMaterializationThroughFinalWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		h := preparedLogsTestHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			_, _ = io.WriteString(w, "{}")
		}), time.Second, time.Second)
		w := &blockedLogResponseWriter{httptest.NewRecorder(), make(chan struct{}), make(chan struct{})}
		release := sync.OnceFunc(func() { close(w.release) })
		defer release()
		done := make(chan struct{})
		go func() { defer close(done); h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/logs", nil)) }()
		<-w.entered
		second := httptest.NewRecorder()
		waiting := make(chan struct{})
		go func() { defer close(waiting); h.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/logs", nil)) }()
		synctest.Wait()
		require.EqualValues(t, 1, calls.Load(), "a live waiter cannot retrieve while the previous response still writes")
		time.Sleep(time.Second)
		<-waiting
		require.Equal(t, http.StatusServiceUnavailable, second.Code, "expired wait never becomes an empty 200")
		require.JSONEq(t, `{"error":"request timeout","code":503}`, second.Body.String())
		require.Empty(t, second.Header().Get("Retry-After"), "deadline expiry is distinct from a full queue")
		require.EqualValues(t, 1, calls.Load())
		release()
		<-done
		next := httptest.NewRecorder()
		h.ServeHTTP(next, httptest.NewRequest(http.MethodGet, "/logs", nil))
		require.Equal(t, http.StatusOK, next.Code)
		require.EqualValues(t, 2, calls.Load())
	})
}

func TestPreparedLogsReleaseOwnershipAfterPreparationEnds(t *testing.T) {
	for _, stage := range []string{"denied", "preparation_panic", "read_panic"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				finish := make(chan struct{})
				release := sync.OnceFunc(func() { close(finish) })
				defer release()
				var calls atomic.Int32
				h := NewPreparedTenantLogsHandler(func(w http.ResponseWriter, _ *http.Request) http.Handler {
					if calls.Add(1) == 1 {
						switch stage {
						case "denied":
							w.WriteHeader(http.StatusForbidden)
							return nil
						case "preparation_panic":
							panic("preparation failed")
						case "read_panic":
							return http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("read failed") })
						}
					}
					<-finish
					return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "{}") })
				}, time.Second, time.Second)
				first := httptest.NewRecorder()
				request := func() { h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/logs", nil)) }
				if stage == "denied" {
					request()
					require.Equal(t, http.StatusForbidden, first.Code)
				} else {
					require.Panics(t, request)
				}
				var workers sync.WaitGroup
				responses := make([]*httptest.ResponseRecorder, 1+maxQueuedLogResponses)
				for i := range responses {
					responses[i] = httptest.NewRecorder()
					workers.Go(func() { h.ServeHTTP(responses[i], httptest.NewRequest(http.MethodGet, "/logs", nil)) })
				}
				synctest.Wait()
				require.EqualValues(t, 2+maxQueuedLogResponses, calls.Load(), "all nine request slots must be reusable")
				release()
				workers.Wait()
				for _, w := range responses {
					require.Equal(t, http.StatusOK, w.Code, "the materialization slot must also be reusable")
				}
			})
		})
	}
}

func TestPreparedLogsCanceledWaiterDoesNotTakeNextResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		finish := make(chan struct{})
		release := sync.OnceFunc(func() { close(finish) })
		defer release()
		var calls atomic.Int32
		h := preparedLogsTestHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if calls.Add(1) == 1 {
				<-finish
			}
			_, _ = io.WriteString(w, "{}")
		}), time.Second, time.Second)
		var workers sync.WaitGroup
		workers.Go(func() { h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/logs", nil)) })
		synctest.Wait()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		second := httptest.NewRecorder()
		workers.Go(func() { h.ServeHTTP(second, httptest.NewRequestWithContext(ctx, http.MethodGet, "/logs", nil)) })
		synctest.Wait()
		cancel()
		synctest.Wait()
		release()
		workers.Wait()
		require.Equal(t, http.StatusServiceUnavailable, second.Code)
		require.EqualValues(t, 1, calls.Load(), "canceled waiter cannot begin a read")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/logs", nil))
		require.Equal(t, http.StatusOK, w.Code)
		require.EqualValues(t, 2, calls.Load())
	})
}

func TestPreparedLogsPreparationAndQueueShareDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var deadline time.Time
		h := NewPreparedTenantLogsHandler(func(http.ResponseWriter, *http.Request) http.Handler {
			time.Sleep(400 * time.Millisecond)
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				deadline, _ = r.Context().Deadline()
				_, _ = io.WriteString(w, "{}")
			})
		}, time.Second, time.Second)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/logs", nil))
		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, start.Add(time.Second), deadline)
	})
}
