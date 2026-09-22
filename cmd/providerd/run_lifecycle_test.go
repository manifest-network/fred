package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	cryptocodec "github.com/cosmos/cosmos-sdk/crypto/codec"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/gorilla/websocket"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// The real composition root must retain the same cancellation and shutdown
// behavior as its smaller startup helpers. These fixtures replace only external
// servers and persisted operator inputs; run, the manager, HTTP ingress, and all
// background workers are the production implementations.
type lifecycleChainServer struct {
	billingtypes.UnimplementedQueryServer
	tx.UnimplementedServiceServer
	blockWithdrawal bool
	withdrawStarted chan struct{}
	withdrawStopped chan struct{}
	queries         atomic.Int32
	broadcasts      atomic.Int32
}

func (s *lifecycleChainServer) ProviderWithdrawable(ctx context.Context, _ *billingtypes.QueryProviderWithdrawableRequest) (*billingtypes.QueryProviderWithdrawableResponse, error) {
	if s.queries.Add(1) == 1 {
		close(s.withdrawStarted)
	}
	if s.blockWithdrawal {
		<-ctx.Done()
		close(s.withdrawStopped)
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &billingtypes.QueryProviderWithdrawableResponse{}, nil
}

func (*lifecycleChainServer) LeasesByProvider(context.Context, *billingtypes.QueryLeasesByProviderRequest) (*billingtypes.QueryLeasesByProviderResponse, error) {
	return &billingtypes.QueryLeasesByProviderResponse{}, nil
}

func (s *lifecycleChainServer) BroadcastTx(context.Context, *tx.BroadcastTxRequest) (*tx.BroadcastTxResponse, error) {
	s.broadcasts.Add(1)
	return nil, status.Error(codes.Internal, "empty fleet must not broadcast")
}

func TestRunLifecycleCancellationJoinsRealRuntime(t *testing.T) {
	for _, phase := range []string{"initial withdrawal", "running"} {
		t.Run(phase, func(t *testing.T) {
			// run installs process-global logger/collector instances. Restore them
			// after its joined shutdown; these tests deliberately are not parallel.
			oldLogger := slog.Default()
			oldRegisterer, oldGatherer := prometheus.DefaultRegisterer, prometheus.DefaultGatherer
			registry := prometheus.NewRegistry()
			prometheus.DefaultRegisterer, prometheus.DefaultGatherer = registry, registry
			t.Cleanup(func() {
				slog.SetDefault(oldLogger)
				prometheus.DefaultRegisterer, prometheus.DefaultGatherer = oldRegisterer, oldGatherer
			})

			chainServer := &lifecycleChainServer{
				blockWithdrawal: phase == "initial withdrawal",
				withdrawStarted: make(chan struct{}), withdrawStopped: make(chan struct{}),
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			grpcServer := grpc.NewServer()
			billingtypes.RegisterQueryServer(grpcServer, chainServer)
			tx.RegisterServiceServer(grpcServer, chainServer)
			grpcDone := make(chan error, 1)
			go func() { grpcDone <- grpcServer.Serve(listener) }()
			t.Cleanup(func() { grpcServer.Stop(); <-grpcDone })

			subscribed, websocketClosed := make(chan struct{}), make(chan struct{})
			var subscriptions atomic.Int32
			wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				defer close(websocketClosed)
				for {
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
					if subscriptions.Add(1) == 4 {
						close(subscribed)
					}
				}
			}))
			t.Cleanup(wsServer.Close)

			storageID := startupStorageID(t, "253b5115-e341-40ee-8686-bb56f1d795d4")
			var inventories atomic.Int32
			backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set(backendidentity.ResponseHeader, storageID.String())
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/health":
					w.WriteHeader(http.StatusOK)
				case "/provisions":
					inventories.Add(1)
					_ = json.NewEncoder(w).Encode(backend.ListProvisionsResponse{Provisions: []backend.ProvisionInfo{}})
				case "/retentions":
					_ = json.NewEncoder(w).Encode(backend.ListRetentionsResponse{Retentions: []backend.RetainedLease{}})
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(backendServer.Close)
			cfg := startupProviderConfig(t, backendServer.URL, storageID)
			keyDir := t.TempDir()
			interfaces := codectypes.NewInterfaceRegistry()
			cryptocodec.RegisterInterfaces(interfaces)
			keys, err := keyring.New("manifest", "test", keyDir, nil, codec.NewProtoCodec(interfaces))
			require.NoError(t, err)
			algorithms, _ := keys.SupportedAlgorithms()
			record, _, err := keys.NewMnemonic("provider", keyring.English, sdk.FullFundraiserPath, keyring.DefaultBIP39Passphrase, algorithms[0])
			require.NoError(t, err)
			address, err := record.GetAddress()
			require.NoError(t, err)
			apiListener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			apiAddress := apiListener.Addr().String()
			require.NoError(t, apiListener.Close())
			values := map[string]any{
				"provider_uuid": cfg.ProviderUUID, "provider_address": address.String(),
				"key_name": "provider", "keyring_backend": "test", "keyring_dir": keyDir,
				"grpc_endpoint": listener.Addr().String(), "grpc_tls_enabled": false,
				"websocket_url":   "ws" + strings.TrimPrefix(wsServer.URL, "http"),
				"api_listen_addr": apiAddress, "callback_base_url": "http://" + apiAddress,
				"callback_secret": string(cfg.CallbackSecret), "placement_store_db_path": cfg.PlacementStoreDBPath,
				"token_tracker_db_path": filepath.Join(t.TempDir(), "tokens.db"),
				"payload_store_db_path": filepath.Join(t.TempDir(), "payloads.db"),
				"shutdown_timeout":      "2s", "withdraw_interval": "1h", "reconciliation_interval": "1h",
				"backends": []any{map[string]any{
					"name": "backend-a", "url": backendServer.URL, "default": true,
				}},
			}
			encoded, err := json.Marshal(values)
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "provider.json")
			require.NoError(t, os.WriteFile(path, encoded, 0o600))
			oldConfigFile := configFile
			configFile = path
			t.Cleanup(func() { configFile = oldConfigFile })

			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			cmd := &cobra.Command{}
			cmd.SetContext(ctx)
			done := make(chan struct{})
			var runErr error
			go func() {
				runErr = run(cmd, nil)
				close(done)
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("provider runtime did not join after cancellation")
				}
			})
			select {
			case <-chainServer.withdrawStarted:
			case <-done:
				t.Fatalf("provider exited before initial withdrawal: %v", runErr)
			case <-time.After(5 * time.Second):
				t.Fatal("initial withdrawal never reached the chain server")
			}
			if phase == "running" {
				select {
				case <-subscribed:
				case <-time.After(5 * time.Second):
					t.Fatal("event subscriber did not start after reconciliation")
				}
				require.Positive(t, inventories.Load(), "startup must reconcile before event subscription")
			}
			httpClient := &http.Client{Timeout: time.Second}
			response, err := httpClient.Get("http://" + apiAddress + "/health")
			require.NoError(t, err, "callback ingress must be live during initial chain work")
			require.NoError(t, response.Body.Close())
			cancel()
			select {
			case <-done:
				require.NoError(t, runErr)
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown did not interrupt startup/running work")
			}
			if phase == "initial withdrawal" {
				select {
				case <-chainServer.withdrawStopped:
				case <-time.After(time.Second):
					t.Fatal("initial chain RPC did not receive cancellation")
				}
				require.Zero(t, inventories.Load(), "canceled withdrawal must not start reconciliation")
				require.Zero(t, subscriptions.Load(), "canceled startup must not start event ingestion")
			} else {
				select {
				case <-websocketClosed:
				case <-time.After(time.Second):
					t.Fatal("shutdown left the event socket open")
				}
			}
			require.Zero(t, chainServer.broadcasts.Load(), "an empty fleet must never produce chain writes")
			_, err = httpClient.Get("http://" + apiAddress + "/health")
			require.Error(t, err, "joined shutdown must close tenant/callback ingress")
			store, err := placement.OpenStore(cfg.PlacementStoreDBPath, cfg.ProviderUUID)
			require.NoError(t, err, "joined shutdown must release placement authority")
			require.NoError(t, store.Close())
		})
	}
}
