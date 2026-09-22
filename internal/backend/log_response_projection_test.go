package backend

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogsHandlerWireProjectionsKeepTimeoutDistinctFromCapacity(t *testing.T) {
	for _, tc := range []struct {
		name        string
		constructor func(http.Handler, time.Duration, time.Duration) http.Handler
		capacity    string
		timeout     string
	}{
		{
			name: "backend", constructor: NewLogsHandler,
			capacity: `{"error":"log response capacity exhausted","code":"insufficient_resources"}`,
			timeout:  `{"error":"request timeout"}`,
		},
		{
			name: "tenant", constructor: NewTenantLogsHandler,
			capacity: `{"error":"log response capacity exhausted","code":503}`,
			timeout:  `{"error":"request timeout","code":503}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			finish := make(chan struct{})
			defer close(finish)
			h := tc.constructor(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				<-r.Context().Done()
				<-finish // A timed-out worker still owns its response allocation.
			}), 20*time.Millisecond, time.Second)
			first := httptest.NewRecorder()
			h.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/logs/lease", nil))
			require.Equal(t, http.StatusServiceUnavailable, first.Code)
			require.JSONEq(t, tc.timeout, first.Body.String())
			require.Empty(t, first.Header().Get("Retry-After"))
			busy := httptest.NewRecorder()
			h.ServeHTTP(busy, httptest.NewRequest(http.MethodGet, "/logs/another", nil))
			require.Equal(t, http.StatusServiceUnavailable, busy.Code)
			require.Equal(t, "application/json", busy.Header().Get("Content-Type"))
			require.Equal(t, "1", busy.Header().Get("Retry-After"))
			require.JSONEq(t, tc.capacity, busy.Body.String())
		})
	}
}
