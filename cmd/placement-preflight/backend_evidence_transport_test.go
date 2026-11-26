package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun_InitializeFreshRequiresAuthenticatedBackendEvidence(t *testing.T) {
	for _, transport := range []string{"HTTP", "TLS verification disabled"} {
		t.Run(transport, func(t *testing.T) {
			var requests atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
			})
			server := httptest.NewUnstartedServer(handler)
			if transport == "HTTP" {
				server.Start()
			} else {
				server.StartTLS()
			}
			defer server.Close()
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			configPath := writePreflightConfig(t, dbPath, server.URL, "backend-a")
			if transport != "HTTP" {
				contents, err := os.ReadFile(configPath)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(configPath, []byte(strings.Replace(string(contents), "    default: true", "    tls_skip_verify: true\n    default: true", 1)), 0o600))
			}
			var output bytes.Buffer
			err := runWithDependencies(
				t.Context(), freshInitializationArgs(t, configPath), &output, &bytes.Buffer{},
				defaultCommandDependencies(),
			)
			require.ErrorContains(t, err, "authenticated backend evidence requires certificate-verified HTTPS")
			require.Empty(t, output.String())
			require.Zero(t, requests.Load())
			require.NoFileExists(t, dbPath)
		})
	}
}
