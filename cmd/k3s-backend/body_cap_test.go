package main

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/k3s"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/hmacauth"
)

// TestHMACMiddleware_EnforcesConfiguredBodyCap mirrors the docker-backend F42
// regression for the k3s-backend's (duplicated) middleware: the configured cap
// is what MaxBytesReader enforces — over-cap → 413, under-cap → passes the size
// gate (then 401 on the invalid signature). Includes the headroom case: a body
// above providerd's 1 MiB cap but below the 2 MiB backend default is accepted.
func TestHMACMiddleware_EnforcesConfiguredBodyCap(t *testing.T) {
	send := func(t *testing.T, capBytes, bodyLen int64) int {
		t.Helper()
		mw := hmacAuthMiddleware(testSecret, slog.Default(), capBytes)
		h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
		req := httptest.NewRequest(http.MethodPost, "/provision", bytes.NewReader(make([]byte, bodyLen)))
		req.Header.Set(hmacauth.SignatureHeader, "present-but-invalid")
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
		{"over providerd cap, under backend default → size ok", k3s.DefaultMaxRequestBodySize, config.DefaultMaxRequestBodySize + 512*1024, http.StatusUnauthorized},
		{"over backend default → 413", k3s.DefaultMaxRequestBodySize, k3s.DefaultMaxRequestBodySize + 1, http.StatusRequestEntityTooLarge},
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
	require.Equal(t, k3s.DefaultMaxRequestBodySize, NewServer(nil, testSecret, slog.Default(), 0).maxRequestBodySize)
	require.Equal(t, k3s.DefaultMaxRequestBodySize, NewServer(nil, testSecret, slog.Default(), -1).maxRequestBodySize)
}
