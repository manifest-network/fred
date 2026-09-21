package main

import (
	"net/http"

	"github.com/felixge/httpsnoop"

	"github.com/manifest-network/fred/internal/healthprobe"
)

// observeHealth surrounds identity admission as well as the actual probe. The
// diagnostic context changes neither storage authority nor request lifetime.
func (s *Server) observeHealth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, observation := healthprobe.Start(r.Context(), r.Header.Values(healthprobe.Header)...)
		w.Header().Set(healthprobe.Header, observation.ID())
		result := httpsnoop.CaptureMetrics(next, w, r.WithContext(ctx))
		observation.Finish(s.logger, healthprobe.Server, result.Code, ctx.Err())
	})
}
