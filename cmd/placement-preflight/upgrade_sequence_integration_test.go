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

// TestIntegrationCommandV013InspectPrepareRestartWithTransientBackend exercises
// the production command runner and its real config, HMAC inventory, bbolt,
// backup, preparation, and reopen boundaries. Only the signer-free chain query
// is substituted, avoiding a brittle external gRPC dependency.
func TestIntegrationCommandV013InspectPrepareRestartWithTransientBackend(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "placements.db")
	backupPath := filepath.Join(tempDir, "placements.v0.13.bak")
	writeLegacyPlacementDB(t, dbPath, map[string][]byte{
		preflightCommandProvisionLease: []byte(
			`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`,
		),
		preflightCommandRetainedLease: []byte("backend-b"),
	})
	original, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	storageA := mustPreflightStorageID(t, "c0a8012e-b4ee-4f4d-9c31-7e6623928311")
	storageB := mustPreflightStorageID(t, "1b72ca05-90e1-4eb8-a781-5d33430a5167")
	serverA := newSwitchableInventoryServer(t, storageA, nil,
		[]backend.ProvisionInfo{{
			LeaseUUID: preflightCommandProvisionLease,
			Items:     preflightSnapshotItems([]string{preflightCommandProvisionLease})[preflightCommandProvisionLease],
		}}, nil)
	var backendBDown atomic.Bool
	backendBDown.Store(true)
	serverB := newSwitchableInventoryServer(t, storageB, &backendBDown,
		nil, []backend.RetainedLease{{LeaseUUID: preflightCommandRetainedLease}})
	configPath := writeMultiBackendPreflightConfig(
		t, dbPath, serverA.URL, serverB.URL,
	)
	dependencies := legacyPreflightDependencies(
		preflightCommandProvisionLease, preflightCommandRetainedLease,
	)

	var unavailableOut bytes.Buffer
	err = runWithDependencies(t.Context(), []string{
		"-config", configPath, "-proof-timeout", "5s",
	}, &unavailableOut, &bytes.Buffer{}, dependencies)
	require.Error(t, err)
	assert.Empty(t, unavailableOut.String())
	afterFailedProof, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, original, afterFailedProof,
		"a transient inventory outage must leave the v0.13 authority byte-exact")
	_, statErr := os.Stat(backupPath)
	require.ErrorIs(t, statErr, os.ErrNotExist)

	backendBDown.Store(false)
	var inspectOut bytes.Buffer
	require.NoError(t, runWithDependencies(t.Context(), []string{
		"-config", configPath, "-proof-timeout", "5s",
	}, &inspectOut, &bytes.Buffer{}, dependencies))
	assert.Contains(t, inspectOut.String(),
		inspectSuccessVerdict+": v0.13 placement preflight verified 2 rows against 2 leases on 2 backends")
	afterInspect, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, original, afterInspect)

	var prepareOut bytes.Buffer
	require.NoError(t, runWithDependencies(t.Context(), []string{
		"-config", configPath,
		"-proof-timeout", "5s",
		"-prepare",
		"-backup", backupPath,
		"-attest-drained", placement.LegacyPreparationDrainAttestation,
	}, &prepareOut, &bytes.Buffer{}, dependencies))
	assert.Contains(t, prepareOut.String(), prepareSuccessVerdict+":")
	assert.Contains(t, prepareOut.String(), "database prepared")
	backup, readErr := os.ReadFile(backupPath)
	require.NoError(t, readErr)
	assert.Equal(t, original, backup, "rollback backup must be the exact v0.13 bytes")

	for range 2 {
		store, openErr := placement.OpenStore(
			dbPath, "550e8400-e29b-41d4-a716-446655440000",
		)
		require.NoError(t, openErr)
		require.NoError(t, store.VerifyBackendTopology([]string{"backend-a", "backend-b"}))
		gotA, ok := store.ExpectedBackendStorageIdentity("backend-a")
		require.True(t, ok)
		assert.Equal(t, storageA, gotA)
		gotB, ok := store.ExpectedBackendStorageIdentity("backend-b")
		require.True(t, ok)
		assert.Equal(t, storageB, gotB)
		require.NoError(t, store.Close())
	}
}

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
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
    default: true
  - name: "backend-b"
    url: %q
`, t.TempDir(), dbPath, backendAURL, backendBURL)
	require.NoError(t, os.WriteFile(configPath, []byte(contents), 0o600))
	return configPath
}

func mustPreflightStorageID(t *testing.T, value string) backendidentity.ID {
	t.Helper()
	storageID, err := backendidentity.Parse(value)
	require.NoError(t, err)
	return storageID
}
