package placementprobe

import (
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
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
	pins := fixedIdentityResolver{"backend-a": id}
	bound, err := fleet.NewIdentityBoundClients(pins)
	require.NoError(t, err)
	// Final repair probes run after the store's admission lock is held. They
	// must own the captured pins, independent of resolver changes or closure.
	delete(pins, "backend-a")
	_, err = Collect(t.Context(), bound)
	require.NoError(t, err)
	wrong, err := fleet.NewIdentityBoundClients(fixedIdentityResolver{"backend-a": probeStorageID("6ba7b811-9dad-41d1-80b4-00c04fd430c8")})
	require.NoError(t, err)
	_, err = Collect(t.Context(), wrong)
	require.ErrorIs(t, err, backend.ErrBackendStorageIdentityMismatch)
}

func TestAuthenticatedFleetRejectsIncompleteConfigurationAtomically(t *testing.T) {
	const secret = "backend-secret-0123456789abcdef0123456789"
	for _, scenario := range []struct {
		name string
		cfg  *config.Config
	}{
		{name: "missing config"},
		{name: "empty fleet", cfg: &config.Config{}},
		{name: "invalid second secret", cfg: &config.Config{Backends: []config.BackendConfig{
			{Name: "first", URL: "https://first.invalid", HMACSecret: secret},
			{Name: "second", URL: "https://second.invalid", HMACSecret: "short"},
		}}},
		{name: "unreadable second trust root", cfg: &config.Config{Backends: []config.BackendConfig{
			{Name: "first", URL: "https://first.invalid", HMACSecret: secret},
			{Name: "second", URL: "https://second.invalid", HMACSecret: secret, TLSCAFile: filepath.Join(t.TempDir(), "absent-ca.pem")},
		}}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			fleet, err := NewAuthenticatedFleet(scenario.cfg)
			require.Error(t, err)
			clients, err := fleet.NewClients()
			require.Error(t, err, "no successfully parsed prefix of a refused fleet may authorize observations")
			require.Nil(t, clients)
			clients, err = fleet.NewIdentityBoundClients(fixedIdentityResolver{"first": probeStorageID("550e8400-e29b-41d4-a716-446655440000")})
			require.Error(t, err)
			require.Nil(t, clients)
		})
	}
}

func TestAuthenticatedFleetRequiresCompleteDistinctPinsBeforeNetwork(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	caFile := filepath.Join(t.TempDir(), "backend-ca.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600))
	cfg := &config.Config{Backends: []config.BackendConfig{
		{Name: "first", URL: server.URL, HMACSecret: "backend-secret-0123456789abcdef0123456789", TLSCAFile: caFile},
		{Name: "second", URL: server.URL, HMACSecret: "backend-secret-0123456789abcdef0123456789", TLSCAFile: caFile},
	}}
	fleet, err := NewAuthenticatedFleet(cfg)
	require.NoError(t, err)
	id := probeStorageID("550e8400-e29b-41d4-a716-446655440000")
	var typedNil fixedIdentityResolver
	for _, scenario := range []struct {
		name     string
		resolver backend.BackendStorageIdentityResolver
		want     string
	}{
		{name: "nil resolver", want: "resolver is required"},
		{name: "typed nil resolver", resolver: typedNil, want: "resolver is required"},
		{name: "unbound second backend", resolver: fixedIdentityResolver{"first": id}, want: "no durable storage identity"},
		{name: "invalid second identity", resolver: fixedIdentityResolver{"first": id, "second": {}}, want: "no durable storage identity"},
		{name: "aliased backend identities", resolver: fixedIdentityResolver{"first": id, "second": id}, want: "share storage identity"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			clients, err := fleet.NewIdentityBoundClients(scenario.resolver)
			require.ErrorContains(t, err, scenario.want)
			require.Nil(t, clients, "a partially pinned fleet must not escape construction")
			require.Zero(t, requests.Load(), "unusable pins must fail before evidence collection")
		})
	}
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
