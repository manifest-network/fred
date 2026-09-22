package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	sdksecp "github.com/cosmos/cosmos-sdk/crypto/keys/secp256k1"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/api"
	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner/callbackwire"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

type fixtureChain map[string]*billingtypes.Lease

func (f fixtureChain) GetLease(_ context.Context, id string) (*billingtypes.Lease, error) {
	return f[id], nil
}
func (f fixtureChain) GetActiveLease(ctx context.Context, id string) (*billingtypes.Lease, error) {
	return f.GetLease(ctx, id)
}
func (f fixtureChain) Ping(context.Context) error { return nil }

type fixtureIdentity struct{ id backendidentity.ID }

func (f fixtureIdentity) ExpectedBackendStorageIdentity(string) (backendidentity.ID, bool) {
	return f.id, true
}

type fixturePublisher struct {
	mu        sync.Mutex
	data      []byte
	events    []payload.Event
	callbacks []hmacauth.VerifiedRequest
}

func (f *fixturePublisher) StorePayload(_ string, data []byte) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = bytes.Clone(data)
	return true
}
func (f *fixturePublisher) DeletePayload(string) {}
func (f *fixturePublisher) PublishPayload(event payload.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
	return nil
}
func (f *fixturePublisher) PublishCallback(_ context.Context, proof hmacauth.VerifiedRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callbacks = append(f.callbacks, proof)
	return nil
}

func fixtureKeyFile(t *testing.T) string {
	t.Helper()
	key := sdksecp.GenPrivKeyFromSecret([]byte("loadtest-request-regression"))
	path := filepath.Join(t.TempDir(), "tenant.hex")
	require.NoError(t, os.WriteFile(path, []byte(hex.EncodeToString(key.Bytes())), 0o600))
	return path
}

// Exercise the real HTTP API, its ADR-036/payload validation, replay database,
// backend HTTP client, and routed HMAC verifier. The collectors replace only
// external chain state and final application effects; no handler/auth hook exists.
func TestAuthenticatedFixturesReachRealAPI(t *testing.T) {
	const (
		leaseID    = "6ba7b811-9dad-41d1-80b4-00c04fd430c8"
		providerID = "7b1b8908-3e56-481a-917e-4e9586642323"
		storage    = "550e8400-e29b-41d4-a716-446655440000"
		secret     = "loadtest-fixture-hmac-secret-at-least-32-bytes"
	)
	keyFile := fixtureKeyFile(t)
	signer, err := loadTenantSigner(keyFile, "manifest")
	require.NoError(t, err)
	storageID, err := backendidentity.Parse(storage)
	require.NoError(t, err)
	backendServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, hmacauth.VerifyRequest(secret, r, nil, r.Header.Get(hmacauth.SignatureHeader), time.Minute))
		w.Header().Set(backendidentity.ResponseHeader, storage)
		require.NoError(t, json.NewEncoder(w).Encode(backend.LeaseInfo{Host: "192.0.2.1"}))
	}))
	defer backendServer.Close()
	policy, err := backend.NewConnectionPolicy(backend.ConnectionConfig{Name: "fixture", BaseURL: backendServer.URL, Secret: secret})
	require.NoError(t, err)
	client, err := backend.NewIdentityBoundHTTPClient(policy, backend.HTTPClientOptions{}, fixtureIdentity{id: storageID})
	require.NoError(t, err)
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}}})
	require.NoError(t, err)
	manifest := []byte(`{"services":{"app":{"image":"nginx:alpine"}}}`)
	digest := sha256.Sum256(manifest)
	lease := &billingtypes.Lease{Uuid: leaseID, Tenant: signer.tenant, ProviderUuid: providerID,
		State: billingtypes.LEASE_STATE_PENDING, MetaHash: digest[:]}
	publisher := &fixturePublisher{}
	// Follow the API's existing startup tests: reserve a local address, release
	// it, then use the public StartBackground readiness contract.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	proofVerifier, _ := hmacauth.NewCallbackProofBoundary()
	server, err := api.NewServer(api.ServerConfig{
		Addr: address, ProviderUUID: providerID, Bech32Prefix: "manifest",
		RateLimitRPS: 100, RateLimitBurst: 100, MaxRequestBodySize: 1 << 20,
		TokenTrackerDBPath:  filepath.Join(t.TempDir(), "tokens.db"),
		CallbackHMACSecrets: map[backendidentity.ID]string{storageID: secret},
	}, api.ServerDeps{ChainClient: fixtureChain{leaseID: lease}, BackendRouter: router,
		PayloadPublisher: publisher, CallbackPublisher: publisher, CallbackProofVerifier: proofVerifier})
	require.NoError(t, err)
	_, err = server.StartBackground()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Shutdown(context.Background())) })
	const callbackURI = "/callbacks/provision?trace=a%2Fb&operation_id=550e8400-e29b-41d4-a716-446655440000"
	body := []byte("\n\t" + `{"lease_uuid":"` + leaseID + `","backend":"fixture","backend_storage_id":"` + storage + `","status":"success"}` + "\n")
	fixtures := fixtureFile{Leases: []leaseFixture{{LeaseUUID: leaseID, Payload: manifest}},
		Callbacks: []callbackFixture{{RequestURI: callbackURI, Body: body}}}
	encoded, err := json.Marshal(fixtures)
	require.NoError(t, err)
	fixturePath := filepath.Join(t.TempDir(), "fixtures.json")
	require.NoError(t, os.WriteFile(fixturePath, encoded, 0o600))
	cfg := workloadConfig{target: "http://" + address, traffic: "authenticated", fixtures: fixturePath,
		keyFile: keyFile, prefix: "manifest", callbackSecret: secret}
	for _, scenario := range []string{"payload", "connection", "callback"} {
		t.Run(scenario, func(t *testing.T) {
			cfg.scenario = scenario
			work, err := loadWorkload(cfg)
			require.NoError(t, err)
			if scenario == "connection" {
				lease.State = billingtypes.LEASE_STATE_ACTIVE
			}
			req, err := work.requests[0](t.Context())
			require.NoError(t, err)
			results := NewResults()
			runner := LoadTester{client: &http.Client{Timeout: time.Second}}
			runner.execute(req, results)
			require.EqualValues(t, 1, results.SuccessCount, "statuses=%v errors=%v", results.StatusCodes, results.Errors)
			if scenario == "connection" {
				// Replay the exact token through the real database: this proves
				// our positive path did not disable replay protection.
				runner.execute(req.Clone(t.Context()), results)
				require.EqualValues(t, 1, results.StatusCodes[http.StatusUnauthorized])
			}
		})
	}
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	require.Equal(t, manifest, publisher.data)
	require.Len(t, publisher.events, 1)
	require.Len(t, publisher.callbacks, 1)
	require.Equal(t, callbackURI, publisher.callbacks[0].URI())
	require.Equal(t, body, publisher.callbacks[0].Body(), "preserve exact recorded body bytes, including outer whitespace")
	observation, err := callbackwire.DecodeVerified(publisher.callbacks[0])
	require.NoError(t, err)
	require.Equal(t, storageID, observation.StorageID())
	require.Equal(t, callbackwire.SelectorOperation, observation.Selector())
}

func TestConnectionSigningPacesDistinctTokensAndCancels(t *testing.T) {
	signer, err := loadTenantSigner(fixtureKeyFile(t), "manifest")
	require.NoError(t, err)
	synctest.Test(t, func(t *testing.T) {
		first, err := signer.nextConnectionTimestamp(t.Context(), "lease")
		require.NoError(t, err)
		second, err := signer.nextConnectionTimestamp(t.Context(), "lease")
		require.NoError(t, err)
		require.Equal(t, first+1, second)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = signer.nextConnectionTimestamp(ctx, "lease")
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestWorkloadRejectsUnconfiguredOrInventedAuthority(t *testing.T) {
	_, err := loadWorkload(workloadConfig{target: "http://localhost:8080", traffic: "authenticated", scenario: "mixed"})
	require.ErrorContains(t, err, "requires -fixtures")
	_, err = loadWorkload(workloadConfig{target: "http://localhost:8080", traffic: "rejection", scenario: "connection", keyFile: "secret"})
	require.ErrorContains(t, err, "must not receive")
	for _, uri := range []string{"https://other.example/callbacks/provision", "/callbacks/provision", "/callbacks/provision?operation_id=invalid"} {
		_, err = recordedCallbackFactory("http://localhost:8080", strings.Repeat("x", 32), []callbackFixture{{
			RequestURI: uri, Body: json.RawMessage(`{"lease_uuid":"6ba7b811-9dad-41d1-80b4-00c04fd430c8","backend":"fixture","backend_storage_id":"550e8400-e29b-41d4-a716-446655440000","status":"success"}`),
		}})
		require.Error(t, err)
	}
	keyFile := filepath.Join(t.TempDir(), "zero.hex")
	require.NoError(t, os.WriteFile(keyFile, []byte(strings.Repeat("0", 64)), 0o600))
	_, err = loadTenantSigner(keyFile, "manifest")
	require.ErrorContains(t, err, "scalar range")
}
