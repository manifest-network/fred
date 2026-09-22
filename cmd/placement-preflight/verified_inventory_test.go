package main

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

var verifiedInventoryTrust sync.Map

// Each fixture trusts the actual test server certificate. Mutation tests use
// the same verified TLS handshake as operators; no skip-verification shortcut.
func newVerifiedInventoryServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	caFile := filepath.Join(t.TempDir(), "inventory-ca.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600))
	verifiedInventoryTrust.Store(server.URL, caFile)
	t.Cleanup(func() { server.Close(); verifiedInventoryTrust.Delete(server.URL) })
	return server
}

func verifiedInventoryCAFile(origin string) string {
	file, _ := verifiedInventoryTrust.Load(origin)
	if path, ok := file.(string); ok {
		return path
	}
	return ""
}
