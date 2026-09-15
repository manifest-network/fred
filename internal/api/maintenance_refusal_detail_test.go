package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
	maintenanceapp "github.com/manifest-network/fred/internal/provisioner/maintenance"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
	"github.com/manifest-network/fred/internal/testutil"
)

type maintenanceDetailPayloadStore struct{ store *payload.Store }

func (persister maintenanceDetailPayloadStore) OverwritePayload(leaseUUID string, value []byte) error {
	return persister.store.Put(leaseUUID, value)
}

func TestMaintenanceLeaseReturnsCuratedBackendRefusalDetailAndReplaysReceipt(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"restart", "update"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			const (
				backendName   = "backend-a"
				secret        = "api-maintenance-detail-test-secret-at-least-32-bytes"
				generation    = "253b5115-e341-40ee-8686-bb56f1d795d4"
				requestID     = "550e8400-e29b-41d4-a716-446655440000"
				backendDetail = "services.web.image:\nregistry is not allowed\x1b"
				wantDetail    = "services.web.image: registry is not allowed"
			)
			leaseUUID, providerUUID := testutil.ValidUUID1, testutil.ValidUUID2
			keyPair := testutil.NewTestKeyPair("maintenance-refusal-detail")
			storageID := testAPIBackendStorageID(backendName)
			var maintenanceCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if !assert.NoError(t, err) {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				if err := hmacauth.VerifyRequest(secret, r, body, r.Header.Get(hmacauth.SignatureHeader), time.Minute); err != nil {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				w.Header().Set(backendidentity.ResponseHeader, storageID.String())
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/provisions":
					assert.NoError(t, json.NewEncoder(w).Encode(backend.ListProvisionsResponse{Provisions: []backend.ProvisionInfo{{
						LeaseUUID: leaseUUID, ProviderUUID: providerUUID, Tenant: keyPair.Address,
						LifecycleGeneration: &backend.LifecycleGenerationObservation{
							Kind: backend.LifecycleGenerationTyped, ID: generation,
						},
					}}}))
				case "/retentions":
					assert.NoError(t, json.NewEncoder(w).Encode(backend.ListRetentionsResponse{Retentions: []backend.RetainedLease{}}))
				case backendidentity.BoundPathPrefix + storageID.String() + "/" + operation:
					maintenanceCalls.Add(1)
					assert.Equal(t, http.MethodPost, r.Method)
					switch operation {
					case "restart":
						var restart backend.RestartRequest
						if !assert.NoError(t, json.Unmarshal(body, &restart)) {
							w.WriteHeader(http.StatusInternalServerError)
							return
						}
						assert.Equal(t, leaseUUID, restart.LeaseUUID)
						assert.Equal(t, requestID, restart.MaintenanceID.String())
					case "update":
						var update backend.UpdateRequest
						if !assert.NoError(t, json.Unmarshal(body, &update)) {
							w.WriteHeader(http.StatusInternalServerError)
							return
						}
						assert.Equal(t, leaseUUID, update.LeaseUUID)
						assert.Equal(t, requestID, update.MaintenanceID.String())
						assert.Equal(t, []byte("manifest"), update.Payload)
					}
					w.WriteHeader(http.StatusBadRequest)
					assert.NoError(t, json.NewEncoder(w).Encode(map[string]string{
						"error": backendDetail, "validation_code": "image_not_allowed",
					}))
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)
			store, err := placementstore.NewStoreForProvider(filepath.Join(t.TempDir(), "placements.db"), providerUUID)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			configureAPIPlacementTopology(t, store, []string{backendName})
			policy, err := backend.NewConnectionPolicy(backend.ConnectionConfig{
				Name: backendName, BaseURL: server.URL, Secret: secret, Timeout: time.Second,
			})
			require.NoError(t, err)
			client, err := backend.NewIdentityBoundHTTPClient(policy, backend.HTTPClientOptions{}, store)
			require.NoError(t, err)
			router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}}})
			require.NoError(t, err)
			chain := &mockChainClient{getLeaseFunc: func(_ context.Context, requested string) (*billingtypes.Lease, error) {
				require.Equal(t, leaseUUID, requested)
				return &billingtypes.Lease{
					Uuid: leaseUUID, ProviderUuid: providerUUID, Tenant: keyPair.Address,
					State: billingtypes.LEASE_STATE_ACTIVE,
					Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1, ServiceName: "app"}},
				}, nil
			}}
			coordinator, err := store.BindOperationCoordinator(nil)
			require.NoError(t, err)
			execution, err := coordinator.BindBackendRuntime(router, apiProviderControlPlane{
				ReconciliationChain: apiReconciliationChain{PruneLeaseReader: chain},
			})
			require.NoError(t, err)
			reconciliation, err := execution.ReconciliationCoordinator(nil, nil)
			require.NoError(t, err)
			sweep, err := reconciliation.BeginSweep()
			require.NoError(t, err)
			t.Cleanup(sweep.End)
			provisions, err := sweep.CollectProvisionInventory(t.Context(), backendName)
			require.NoError(t, err)
			retentions, err := sweep.CollectRetentionInventory(t.Context(), backendName)
			require.NoError(t, err)
			disposition, err := sweep.RecordBackendInventory(provisions, retentions)
			require.NoError(t, err)
			require.Equal(t, placement.BackendInventoryAuthoritative, disposition)
			require.NoError(t, sweep.SealInventory())
			_, err = sweep.Project(placement.ReconciliationProjection{Placements: map[string]string{leaseUUID: backendName}})
			require.NoError(t, err)
			sweep.End()
			payloadStore, err := payload.NewStore(payload.StoreConfig{DBPath: filepath.Join(t.TempDir(), "payloads.db")})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, payloadStore.Close()) })
			require.NoError(t, payloadStore.Put(leaseUUID, []byte("previously accepted manifest")))
			maintenanceCoordinator, err := execution.MaintenanceCoordinator(maintenanceDetailPayloadStore{store: payloadStore})
			require.NoError(t, err)
			service, err := maintenanceapp.NewService(maintenanceapp.Config{Coordinator: maintenanceCoordinator})
			require.NoError(t, err)
			handlers := NewHandlers(HandlersConfig{
				MaintenanceService: service, ProviderUUID: providerUUID, Bech32Prefix: "manifest",
			})
			mux := http.NewServeMux()
			mux.HandleFunc("POST /v1/leases/{lease_uuid}/restart", handlers.RestartLease)
			mux.HandleFunc("POST /v1/leases/{lease_uuid}/update", handlers.UpdateLease)
			for attempt := range 2 {
				var body io.Reader
				if operation == "update" {
					body = strings.NewReader(`{"payload":"bWFuaWZlc3Q="}`)
				}
				request := httptest.NewRequest(http.MethodPost, "/v1/leases/"+leaseUUID+"/"+operation, body)
				request.Header.Set("Authorization", "Bearer "+testutil.CreateTestToken(keyPair, leaseUUID, time.Now().Add(time.Duration(attempt)*time.Second)))
				request.Header.Set(idempotencyKeyHeader, requestID)
				response := httptest.NewRecorder()
				mux.ServeHTTP(response, request)
				require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
				var result ErrorResponse
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
				assert.Equal(t, wantDetail, result.Error, "both first response and receipt replay must retain the curated diagnostic")
				assert.Equal(t, http.StatusBadRequest, result.Code)
			}
			assert.Equal(t, int32(1), maintenanceCalls.Load(), "the real service must replay the durable refusal without another backend call")
			storedPayload, err := payloadStore.Get(leaseUUID)
			require.NoError(t, err)
			assert.Equal(t, []byte("previously accepted manifest"), storedPayload, "a refused maintenance command must preserve the accepted manifest")
		})
	}
}
