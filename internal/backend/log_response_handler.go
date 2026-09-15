package backend

import (
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

// NewLogsHandler admits one materialized backend log response per server.
// Saturation returns the backend's string-code capacity envelope. A timeout
// remains a backend failure with no capacity code. Authentication middleware
// should wrap this handler.
//
// The permit covers BOTH the worker and the buffered timeout response's final
// write. A timeout or a slow client cannot release it while either still owns
// memory. Keep this wrapper in place of (not inside) another TimeoutHandler.
func NewLogsHandler(next http.Handler, timeout time.Duration) http.Handler {
	return newLogsHandler(next, timeout, logResponseProjection{
		capacity: `{"error":"log response capacity exhausted","code":"` + CodeInsufficientResources + `"}`,
		timeout:  `{"error":"request timeout"}`,
	})
}

// NewTenantLogsHandler applies the same response ownership to the provider's
// tenant route, whose error envelopes carry numeric HTTP status codes.
func NewTenantLogsHandler(next http.Handler, timeout time.Duration) http.Handler {
	return newLogsHandler(next, timeout, logResponseProjection{
		capacity: `{"error":"log response capacity exhausted","code":503}`,
		timeout:  `{"error":"request timeout","code":503}`,
	})
}

type logResponseProjection struct {
	capacity string
	timeout  string
}

func newLogsHandler(next http.Handler, timeout time.Duration, projection logResponseProjection) http.Handler {
	gate := semaphore.NewWeighted(1)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !gate.TryAcquire(1) {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, projection.capacity)
			return
		}
		permit := logResponsePermit{gate: gate}
		permit.owners.Store(2)
		defer permit.release()
		worker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer permit.release()
			next.ServeHTTP(w, r)
		})
		http.TimeoutHandler(worker, timeout, projection.timeout).ServeHTTP(w, r)
	})
}

// Only newLogsHandler owns these two references. Neither the handler nor its
// caller receives a release capability, so cancellation cannot shorten the
// materialized response's lifetime.
type logResponsePermit struct {
	gate   *semaphore.Weighted
	owners atomic.Int32
}

func (p *logResponsePermit) release() {
	if p.owners.Add(-1) == 0 {
		p.gate.Release(1)
	}
}
