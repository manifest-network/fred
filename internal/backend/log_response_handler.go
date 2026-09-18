package backend

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

const maxQueuedLogResponses = 8

// NewLogsHandler admits one materialized backend log response per server.
// Up to eight requests wait in FIFO order within their request deadline.
// Saturation returns the backend's string-code capacity envelope. A timeout
// remains a backend failure with no capacity code. Authentication middleware
// should wrap this handler.
//
// The permit covers BOTH the worker and the buffered timeout response's final
// write. A timeout or a slow client cannot release it while either still owns
// memory. Keep this wrapper in place of (not inside) another TimeoutHandler.
// The final socket write has its own finite budget; a disabled write timeout
// uses the request timeout. ResponseWriter wrappers must preserve
// ResponseController support through Unwrap.
func NewLogsHandler(next http.Handler, timeout, writeTimeout time.Duration) http.Handler {
	return newLogsHandler(next, timeout, writeTimeout, logResponseProjection{
		capacity: `{"error":"log response capacity exhausted","code":"` + CodeInsufficientResources + `"}`,
		timeout:  `{"error":"request timeout"}`,
	})
}

// NewTenantLogsHandler applies the same response ownership to the provider's
// tenant route, whose error envelopes carry numeric HTTP status codes.
func NewTenantLogsHandler(next http.Handler, timeout, writeTimeout time.Duration) http.Handler {
	return newLogsHandler(next, timeout, writeTimeout, logResponseProjection{
		capacity: `{"error":"log response capacity exhausted","code":503}`,
		timeout:  `{"error":"request timeout","code":503}`,
	})
}

type logResponseProjection struct {
	capacity string
	timeout  string
}

func newLogsHandler(next http.Handler, timeout, writeTimeout time.Duration, projection logResponseProjection) http.Handler {
	if writeTimeout <= 0 {
		writeTimeout = timeout
	}
	gate := semaphore.NewWeighted(1)
	// Waiting requests own only a request, never a materialized log response.
	// Bound that queue separately; semaphore.Acquire prevents new arrivals from
	// overtaking an existing waiter when the previous response releases memory.
	slots := semaphore.NewWeighted(1 + maxQueuedLogResponses)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Queueing and retrieval share one budget; admission does not restart
		// the caller's timeout when a previous client finishes its transfer.
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		writer := &logResponseWriter{ResponseWriter: w, timeout: writeTimeout}
		deadline, _ := ctx.Deadline()
		// Set this before queueing. In particular, an HTTP/2 stream whose old
		// server write deadline has fired cannot be revived at the final write.
		writer.setDeadline(deadline.Add(writeTimeout))
		w = writer
		w.Header().Set("Content-Type", "application/json")
		refuse := func() {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, projection.capacity)
		}
		permit := acquireLogResponse(ctx, gate, slots)
		if permit == nil {
			refuse()
			return
		}
		defer permit.release()
		worker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer permit.release()
			next.ServeHTTP(w, r)
		})
		http.TimeoutHandler(worker, timeout, projection.timeout).ServeHTTP(w, r.WithContext(ctx))
	})
}

// The timeout worker never sees this writer. Only the outer response owner can
// tighten the socket deadline after retrieval/encoding completes (or times out).
// It covers headers and the complete buffered write, rather than renewing for
// each fragment. net/http resets the connection deadline for the next request.
type logResponseWriter struct {
	http.ResponseWriter
	started bool
	timeout time.Duration
}

func (w *logResponseWriter) start() {
	if w.started {
		return
	}
	w.started = true
	w.setDeadline(time.Now().Add(w.timeout))
}

func (w *logResponseWriter) setDeadline(deadline time.Time) {
	err := http.NewResponseController(w.ResponseWriter).SetWriteDeadline(deadline)
	// In-memory recorders do not implement deadlines. Production's HTTP/1 and
	// HTTP/2 writers do, including through this server's middleware wrappers.
	if err != nil && !errors.Is(err, http.ErrNotSupported) {
		panic(http.ErrAbortHandler)
	}
}

func (w *logResponseWriter) WriteHeader(status int) {
	w.start()
	w.ResponseWriter.WriteHeader(status)
}

func (w *logResponseWriter) Write(p []byte) (int, error) {
	w.start()
	return w.ResponseWriter.Write(p)
}

// Only newLogsHandler owns these two references. Neither the handler nor its
// caller receives a release capability, so cancellation cannot shorten the
// materialized response's lifetime.
type logResponsePermit struct {
	gate   *semaphore.Weighted
	slots  *semaphore.Weighted
	owners atomic.Int32
}

func acquireLogResponse(ctx context.Context, gate, slots *semaphore.Weighted) *logResponsePermit {
	if !slots.TryAcquire(1) {
		return nil
	}
	if err := gate.Acquire(ctx, 1); err != nil {
		slots.Release(1)
		return nil
	}
	p := &logResponsePermit{gate: gate, slots: slots}
	p.owners.Store(2)
	return p
}

func (p *logResponsePermit) release() {
	if p.owners.Add(-1) == 0 {
		p.gate.Release(1)
		p.slots.Release(1)
	}
}
