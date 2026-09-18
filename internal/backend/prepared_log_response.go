package backend

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

// NewPreparedTenantLogsHandler separates small request preparation (identity,
// authorization and routing) from materializing logs. prepare returns a handler
// bound to that authorized request, or nil after writing a bounded error. It
// must not fetch logs. At most nine requests prepare or wait; only one returned
// handler may own a log response. Both limits cover worker and socket lifetime.
// The preparation and materialization queue share the original request deadline.
func NewPreparedTenantLogsHandler(
	prepare func(http.ResponseWriter, *http.Request) http.Handler,
	timeout, writeTimeout time.Duration,
) http.Handler {
	if writeTimeout <= 0 {
		writeTimeout = timeout
	}
	responses := semaphore.NewWeighted(1)
	requests := semaphore.NewWeighted(1 + maxQueuedLogResponses)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		writer := &logResponseWriter{ResponseWriter: w, timeout: writeTimeout}
		deadline, _ := ctx.Deadline()
		writer.setDeadline(deadline.Add(writeTimeout))
		writer.Header().Set("Content-Type", "application/json")
		refuse := func(reason string) {
			slog.Warn("tenant log response admission deferred", "reason", reason)
			writer.Header().Set("Retry-After", "1")
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(writer, `{"error":"log response capacity exhausted","code":503}`)
		}
		if ctx.Err() != nil {
			refuse("request_canceled")
			return
		}
		if !requests.TryAcquire(1) {
			refuse("request_queue_full")
			return
		}
		permit := &preparedLogResponsePermit{requests: requests, responses: responses}
		permit.owners.Store(2)
		defer permit.release()
		worker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer permit.release()
			next := prepare(w, r)
			if next == nil {
				return
			}
			if err := responses.Acquire(r.Context(), 1); err != nil {
				slog.Warn("tenant log response admission deferred", "reason", "response_wait_expired")
				// Worker completion can race TimeoutHandler's deadline arm. Both
				// branches must publish the same failure, never an empty 200.
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, `{"error":"request timeout","code":503}`)
				return
			}
			permit.materialized.Store(true)
			next.ServeHTTP(w, r)
		})
		http.TimeoutHandler(worker, timeout, `{"error":"request timeout","code":503}`).ServeHTTP(writer, r.WithContext(ctx))
	})
}

// Only the constructor's worker can acquire materialization. The socket owner
// cannot release either permit while a timed-out worker still owns its request;
// equally, worker completion cannot release a buffered response still writing.
type preparedLogResponsePermit struct {
	requests     *semaphore.Weighted
	responses    *semaphore.Weighted
	owners       atomic.Int32
	materialized atomic.Bool
}

func (p *preparedLogResponsePermit) release() {
	if p.owners.Add(-1) != 0 {
		return
	}
	if p.materialized.Load() {
		p.responses.Release(1)
	}
	p.requests.Release(1)
}
