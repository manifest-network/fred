package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// TestIntegrationPreparedMultiBackendRestartsWithTransientNode crosses the
// production providerd startup boundary twice with an unchanged, identity-bound
// two-backend topology while one node is unreachable. It proves ordinary
// restart availability without relaxing the positive-identity-contradiction
// checks exercised by the focused startup tests.
func TestIntegrationPreparedMultiBackendRestartsWithTransientNode(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	storageA := startupStorageID(t, "253b5115-e341-40ee-8686-bb56f1d795d4")
	storageB := startupStorageID(t, "f547a804-17f8-4977-99c8-1154a939d899")
	initializePreparedMultiBackendStore(t, dbPath, storageA, storageB)

	var availableRequests atomic.Int32
	available := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		availableRequests.Add(1)
		assert.Equal(t, "/health", r.URL.Path)
		assert.Equal(t, []string{storageA.String()},
			r.URL.Query()[backendidentity.QueryParameter])
		w.Header().Set(backendidentity.ResponseHeader, storageA.String())
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(available.Close)

	cfg := &config.Config{
		PlacementStoreDBPath: dbPath,
		ProviderUUID:         startupChainSnapshot{}.ProviderUUID(),
		CallbackSecret:       config.Secret("0123456789abcdef0123456789abcdef"),
		Backends: []config.BackendConfig{
			{
				Name: "backend-a", URL: available.URL, Timeout: time.Second,
				SKUs: []string{"sku-a"}, IsDefault: true,
			},
			{
				Name: "backend-b", URL: "http://127.0.0.1:1", Timeout: 20 * time.Millisecond,
				SKUs: []string{"sku-a"},
			},
		},
	}

	for range 2 {
		store, entries, err := preparePlacementBackends(t.Context(), cfg)
		require.NoError(t, err)
		require.Len(t, entries, 2)
		require.NoError(t, store.VerifyBackendTopology([]string{"backend-a", "backend-b"}))
		require.NoError(t, store.Close())
	}
	assert.Equal(t, int32(2), availableRequests.Load(),
		"the available node must still be positively attested on every restart")
}

func initializePreparedMultiBackendStore(
	t *testing.T,
	dbPath string,
	storageA, storageB backendidentity.ID,
) {
	t.Helper()
	backendNames := []string{"backend-a", "backend-b"}
	chainProof, err := placement.NewFreshChainProof(startupChainSnapshot{})
	require.NoError(t, err)
	backendProof, err := placement.NewFreshBackendProof(
		backendNames,
		map[string]placement.BackendInventory{
			"backend-a": {
				StorageIdentity:        storageA,
				Provisions:             []string{},
				ProvisionProviderUUIDs: map[string]string{},
				Retentions:             []string{},
			},
			"backend-b": {
				StorageIdentity:        storageB,
				Provisions:             []string{},
				ProvisionProviderUUIDs: map[string]string{},
				Retentions:             []string{},
			},
		},
	)
	require.NoError(t, err)
	target, err := placement.NewFreshInitializationTarget(
		dbPath, startupChainSnapshot{}.ProviderUUID(), backendNames,
	)
	require.NoError(t, err)
	quiescence, err := placement.ConfirmFreshQuiescence(target, target.Confirmation())
	require.NoError(t, err)
	proofCtx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	plan, err := placement.NewFreshInitializationPlan(
		proofCtx, target, chainProof, backendProof, quiescence,
	)
	require.NoError(t, err)
	require.NoError(t, placement.InitializeFreshStoreContext(t.Context(), plan))
}
