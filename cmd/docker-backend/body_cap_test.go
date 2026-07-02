package main

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/docker"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/hmacauth"
)

// TestHMACMiddleware_EnforcesConfiguredBodyCap is the F42 regression: the
// configured cap is what the middleware actually enforces. The middleware
// io.ReadAll's the body behind a MaxBytesReader, so an over-cap body trips it
// and returns 413; an under-cap body passes the size gate and then fails auth on
// the (present but invalid) signature with 401 — proving it was NOT rejected for
// size. The key case is a body above providerd's 1 MiB cap but below the 2 MiB
// backend default: it must be accepted for size (the headroom the fix adds).
func TestHMACMiddleware_EnforcesConfiguredBodyCap(t *testing.T) {
	send := func(t *testing.T, capBytes, bodyLen int64) int {
		t.Helper()
		mw := hmacAuthMiddleware(testSecret, slog.Default(), capBytes)
		h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
		req := httptest.NewRequest(http.MethodPost, "/provision", bytes.NewReader(make([]byte, bodyLen)))
		req.Header.Set(hmacauth.SignatureHeader, "present-but-invalid") // non-empty → reaches the body read
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	cases := []struct {
		name              string
		capBytes, bodyLen int64
		want              int
	}{
		{"under small cap → size ok, auth fails", 100, 50, http.StatusUnauthorized},
		{"over small cap → 413", 100, 200, http.StatusRequestEntityTooLarge},
		{"over providerd cap, under backend default → size ok", docker.DefaultMaxRequestBodySize, config.DefaultMaxRequestBodySize + 512*1024, http.StatusUnauthorized},
		{"over backend default → 413", docker.DefaultMaxRequestBodySize, docker.DefaultMaxRequestBodySize + 1, http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, send(t, tc.capBytes, tc.bodyLen))
		})
	}
}

// TestNewServer_NonPositiveBodyCapDefaults: a zero/negative cap falls back to
// the package default rather than disabling the limit.
func TestNewServer_NonPositiveBodyCapDefaults(t *testing.T) {
	require.Equal(t, docker.DefaultMaxRequestBodySize, NewServer(nil, testSecret, slog.Default(), 0).maxRequestBodySize)
	require.Equal(t, docker.DefaultMaxRequestBodySize, NewServer(nil, testSecret, slog.Default(), -1).maxRequestBodySize)
}
