package placementprobe

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/config"
)

func TestAuthenticatedFleetOwnsVerifiedTransportAndExactIdentityPins(t *testing.T) {
	id := probeStorageID("550e8400-e29b-41d4-a716-446655440000")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(backendidentity.ResponseHeader, id.String())
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/provisions" {
			_ = json.NewEncoder(w).Encode(backend.ListProvisionsResponse{Provisions: []backend.ProvisionInfo{}})
		} else {
			_ = json.NewEncoder(w).Encode(backend.ListRetentionsResponse{Retentions: []backend.RetainedLease{}})
		}
	}))
	defer server.Close()
	caFile := filepath.Join(t.TempDir(), "backend-ca.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600))
	cfg := &config.Config{Backends: []config.BackendConfig{{Name: "backend-a", URL: server.URL, HMACSecret: "backend-secret-0123456789abcdef0123456789", TLSCAFile: caFile}}}
	fleet, err := NewAuthenticatedFleet(cfg)
	require.NoError(t, err)
	// The capability owns the parsed trust roots and origin, not this mutable
	// configuration or the certificate pathname.
	cfg.Backends[0].URL = "http://127.0.0.1:1"
	cfg.Backends[0].TLSSkipVerify = true
	require.NoError(t, os.Remove(caFile))
	clients, err := fleet.NewClients()
	require.NoError(t, err)
	inventory, err := Collect(t.Context(), clients)
	require.NoError(t, err)
	require.Equal(t, id, inventory["backend-a"].StorageIdentity)
	bound, err := fleet.NewIdentityBoundClients(fixedIdentityResolver{"backend-a": id})
	require.NoError(t, err)
	_, err = Collect(t.Context(), bound)
	require.NoError(t, err)
	wrong, err := fleet.NewIdentityBoundClients(fixedIdentityResolver{"backend-a": probeStorageID("6ba7b811-9dad-41d1-80b4-00c04fd430c8")})
	require.NoError(t, err)
	_, err = Collect(t.Context(), wrong)
	require.ErrorIs(t, err, backend.ErrBackendStorageIdentityMismatch)
}

func TestAuthenticatedFleetRejectsZeroAndUnauthenticatedMembers(t *testing.T) {
	_, err := (AuthenticatedFleet{}).NewClients()
	require.Error(t, err)
	_, err = (AuthenticatedFleet{}).NewIdentityBoundClients(nil)
	require.Error(t, err)
	for _, entry := range []config.BackendConfig{
		{Name: "plain", URL: "http://backend.invalid", HMACSecret: "backend-secret-0123456789abcdef0123456789"},
		{Name: "unverified", URL: "https://backend.invalid", TLSSkipVerify: true, HMACSecret: "backend-secret-0123456789abcdef0123456789"},
	} {
		fleet, err := NewAuthenticatedFleet(&config.Config{Backends: []config.BackendConfig{entry}})
		require.ErrorContains(t, err, "certificate-verified HTTPS")
		_, err = fleet.NewClients()
		require.Error(t, err)
	}
}
