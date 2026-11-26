package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

func TestIntegrationCommandFreshInitializeMultiBackendAndRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	storageA := mustPreflightStorageID(t, "c0a8012e-b4ee-4f4d-9c31-7e6623928311")
	storageB := mustPreflightStorageID(t, "1b72ca05-90e1-4eb8-a781-5d33430a5167")
	serverA := newSwitchableInventoryServer(t, storageA, nil, nil, nil)
	serverB := newSwitchableInventoryServer(t, storageB, nil, nil, nil)
	configPath := writeMultiBackendPreflightConfig(
		t, dbPath, serverA.URL, serverB.URL,
	)
	dependencies := defaultCommandDependencies()
	dependencies.newFreshChainClient = func(
		chain.ReadOnlyClientConfig,
	) (freshChainClient, error) {
		return &preflightFreshChainClient{snapshot: preflightFreshChainSnapshot{
			providerUUID: "550e8400-e29b-41d4-a716-446655440000",
			height:       914,
		}}, nil
	}

	var stdout bytes.Buffer
	require.NoError(t, runWithDependencies(
		t.Context(), freshInitializationArgs(t, configPath),
		&stdout, &bytes.Buffer{}, dependencies,
	))
	assert.Contains(t, stdout.String(),
		freshSuccessVerdict+": fresh placement database initialized; chain_height=914 total_leases=0 backends=2")

	for range 2 {
		store, err := placement.OpenStore(
			dbPath, "550e8400-e29b-41d4-a716-446655440000",
		)
		require.NoError(t, err)
		require.NoError(t, store.VerifyBackendTopology([]string{"backend-a", "backend-b"}))
		require.NoError(t, store.Close())
	}
}

func newSwitchableInventoryServer(
	t *testing.T,
	storageID backendidentity.ID,
	down *atomic.Bool,
	provisions []backend.ProvisionInfo,
	retentions []backend.RetainedLease,
) *httptest.Server {
	t.Helper()
	if provisions == nil {
		provisions = []backend.ProvisionInfo{}
	}
	if retentions == nil {
		retentions = []backend.RetainedLease{}
	}
	server := newVerifiedInventoryServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NotEmpty(t, r.Header.Get(hmacauth.SignatureHeader))
		if down != nil && down.Load() {
			http.Error(w, "transient backend outage", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set(backendidentity.ResponseHeader, storageID.String())
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/provisions":
			assert.NoError(t, json.NewEncoder(w).Encode(backend.ListProvisionsResponse{
				Provisions: provisions,
			}))
		case "/retentions":
			assert.NoError(t, json.NewEncoder(w).Encode(backend.ListRetentionsResponse{
				Retentions: retentions,
			}))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func writeMultiBackendPreflightConfig(
	t *testing.T,
	dbPath, backendAURL, backendBURL string,
) string {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "provider.yaml")
	contents := fmt.Sprintf(`provider_uuid: "550e8400-e29b-41d4-a716-446655440000"
provider_address: "manifest1provider"
keyring_dir: %q
key_name: "provider"
grpc_tls_enabled: true
callback_base_url: "http://fred.example.test"
callback_secret: "0123456789abcdef0123456789abcdef"
placement_store_db_path: %q
backends:
  - name: "backend-a"
    url: %q
    tls_ca_file: %q
    default: true
  - name: "backend-b"
    url: %q
    tls_ca_file: %q
`, t.TempDir(), dbPath, backendAURL, verifiedInventoryCAFile(backendAURL), backendBURL, verifiedInventoryCAFile(backendBURL))
	require.NoError(t, os.WriteFile(configPath, []byte(contents), 0o600))
	return configPath
}

func mustPreflightStorageID(t *testing.T, value string) backendidentity.ID {
	t.Helper()
	storageID, err := backendidentity.Parse(value)
	require.NoError(t, err)
	return storageID
}
