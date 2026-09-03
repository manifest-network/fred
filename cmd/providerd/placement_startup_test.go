package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/config"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

type startupChainSnapshot struct{}

func (startupChainSnapshot) Valid() bool             { return true }
func (startupChainSnapshot) ProviderUUID() string    { return "de5b8be8-76bb-4d43-b473-757430e6c574" }
func (startupChainSnapshot) BlockHeight() int64      { return 93 }
func (startupChainSnapshot) TotalLeases() int        { return 0 }
func (startupChainSnapshot) BlockingLeaseCount() int { return 0 }

type startupHealthBackend struct {
	backend.Backend
	name      string
	healthErr error
	health    func(context.Context) error
	called    chan<- string
}

func (candidate *startupHealthBackend) Name() string { return candidate.name }

func (candidate *startupHealthBackend) Health(ctx context.Context) error {
	if candidate.called != nil {
		candidate.called <- candidate.name
	}
	if candidate.health != nil {
		return candidate.health(ctx)
	}
	return candidate.healthErr
}

func TestAttestPinnedBackendIdentitiesContinuesAroundTransientOutage(t *testing.T) {
	t.Parallel()

	called := make(chan string, 2)
	entries := []backend.BackendEntry{
		{Backend: &startupHealthBackend{name: "available", called: called}},
		{Backend: &startupHealthBackend{
			name: "down", healthErr: errors.New("dial tcp: connection refused"), called: called,
		}},
	}
	require.NoError(t, attestPinnedBackendIdentities(t.Context(), entries))
	seen := map[string]bool{<-called: true, <-called: true}
	assert.Equal(t, map[string]bool{"available": true, "down": true}, seen)
}

func TestAttestPinnedBackendIdentitiesFailsOnPositiveIdentityContradiction(t *testing.T) {
	t.Parallel()

	for _, sentinel := range []error{
		backend.ErrBackendStorageIdentityUnbound,
		backend.ErrBackendStorageIdentityMissing,
		backend.ErrBackendStorageIdentityMismatch,
		backend.ErrBackendUpgradeRequired,
		backendidentity.ErrIdentityDrift,
	} {
		sentinel := sentinel
		t.Run(sentinel.Error(), func(t *testing.T) {
			t.Parallel()
			err := attestPinnedBackendIdentities(t.Context(), []backend.BackendEntry{{
				Backend: &startupHealthBackend{name: "contradiction", healthErr: sentinel},
			}})
			require.ErrorIs(t, err, sentinel)
			require.ErrorContains(t, err, "contradicted its durable storage identity")
		})
	}
}

func TestAttestPinnedBackendIdentitiesHonorsStartupCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := attestPinnedBackendIdentities(ctx, []backend.BackendEntry{{
		Backend: &startupHealthBackend{name: "backend-a", healthErr: context.Canceled},
	}})
	require.ErrorIs(t, err, context.Canceled)
}

func TestAttestPinnedBackendIdentitiesHonorsCancellationDuringProbe(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- attestPinnedBackendIdentitiesWithin(
			ctx,
			[]backend.BackendEntry{{Backend: &startupHealthBackend{
				name: "slow-backend",
				health: func(ctx context.Context) error {
					close(started)
					<-ctx.Done()
					return ctx.Err()
				},
			}}},
			time.Minute,
		)
	}()
	<-started
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
}

func TestAttestPinnedBackendIdentitiesToleratesInternalProbeDeadline(t *testing.T) {
	t.Parallel()

	healthResult := make(chan error, 1)
	err := attestPinnedBackendIdentitiesWithin(
		t.Context(),
		[]backend.BackendEntry{{Backend: &startupHealthBackend{
			name: "slow-backend",
			health: func(ctx context.Context) error {
				<-ctx.Done()
				healthResult <- ctx.Err()
				return ctx.Err()
			},
		}}},
		20*time.Millisecond,
	)
	require.NoError(t, err)
	assert.ErrorIs(t, <-healthResult, context.DeadlineExceeded)
}

func TestPreparePlacementBackendsReattestsExactTopology(t *testing.T) {
	t.Parallel()

	expected := startupStorageID(t, "253b5115-e341-40ee-8686-bb56f1d795d4")
	other := startupStorageID(t, "f547a804-17f8-4977-99c8-1154a939d899")
	tests := []struct {
		name        string
		headers     []string
		status      int
		wantErr     error
		wantSuccess bool
	}{
		{name: "matching", headers: []string{expected.String()}, status: http.StatusOK, wantSuccess: true},
		{name: "unhealthy but matching", headers: []string{expected.String()}, status: http.StatusServiceUnavailable, wantSuccess: true},
		{name: "unhealthy and unable to attest", status: http.StatusServiceUnavailable, wantSuccess: true},
		{name: "missing identity on success", status: http.StatusOK, wantErr: backend.ErrBackendStorageIdentityMissing},
		{name: "empty identity while unhealthy", headers: []string{""}, status: http.StatusServiceUnavailable, wantErr: backend.ErrBackendStorageIdentityMissing},
		{name: "duplicate identity while unhealthy", headers: []string{expected.String(), expected.String()}, status: http.StatusServiceUnavailable, wantErr: backend.ErrBackendStorageIdentityMissing},
		{name: "malformed identity while unhealthy", headers: []string{"not-a-uuid"}, status: http.StatusServiceUnavailable, wantErr: backend.ErrBackendStorageIdentityMismatch},
		{name: "mismatched identity", headers: []string{other.String()}, status: http.StatusOK, wantErr: backend.ErrBackendStorageIdentityMismatch},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			// The production HTTP client may retry an unhealthy response within its
			// bounded request budget. Never let instrumentation back-pressure the
			// server and deadlock the startup call under test.
			requests := make(chan struct{}, 16)
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				requests <- struct{}{}
				assert.Equal(t, "/health", request.URL.Path)
				assert.Equal(t, []string{expected.String()}, request.URL.Query()[backendidentity.QueryParameter])
				for _, header := range test.headers {
					response.Header().Add(backendidentity.ResponseHeader, header)
				}
				response.WriteHeader(test.status)
			}))
			defer server.Close()

			cfg := startupProviderConfig(t, server.URL, expected)
			store, entries, err := preparePlacementBackends(t.Context(), cfg)
			<-requests
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				assert.Nil(t, store)
				assert.Nil(t, entries)
				return
			}
			require.True(t, test.wantSuccess)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			require.NoError(t, store.Close())
		})
	}
}

func TestPreparePlacementBackendsAllowsExactTopologyNodeOutage(t *testing.T) {
	t.Parallel()

	expected := startupStorageID(t, "253b5115-e341-40ee-8686-bb56f1d795d4")
	cfg := startupProviderConfig(t, "http://127.0.0.1:1", expected)
	cfg.Backends[0].Timeout = 20 * time.Millisecond

	store, entries, err := preparePlacementBackends(t.Context(), cfg)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.NoError(t, store.Close())
}

func TestPreparePlacementBackendsRejectsMissingAuthorityBeforeBackendIO(t *testing.T) {
	t.Parallel()

	requests := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests <- struct{}{}
	}))
	defer server.Close()
	cfg := &config.Config{
		PlacementStoreDBPath: filepath.Join(t.TempDir(), "absent.db"),
		ProviderUUID:         startupChainSnapshot{}.ProviderUUID(),
		CallbackSecret:       config.Secret("0123456789abcdef0123456789abcdef"),
		Backends: []config.BackendConfig{{
			Name: "backend-a", URL: server.URL, SKUs: []string{"sku-a"}, IsDefault: true,
		}},
	}

	store, entries, err := preparePlacementBackends(t.Context(), cfg)
	require.ErrorIs(t, err, os.ErrNotExist)
	assert.Nil(t, store)
	assert.Nil(t, entries)
	select {
	case <-requests:
		t.Fatal("backend was contacted before placement authority opened")
	default:
	}
}

func TestPreparePlacementBackendsRejectsWrongProviderBeforeBackendConstruction(t *testing.T) {
	t.Parallel()

	expected := startupStorageID(t, "253b5115-e341-40ee-8686-bb56f1d795d4")
	cfg := startupProviderConfig(t, "http://127.0.0.1:1", expected)
	cfg.ProviderUUID = "e58ed763-928c-4e03-bfac-67a92a99de90"

	store, entries, err := preparePlacementBackends(t.Context(), cfg)
	require.ErrorIs(t, err, placement.ErrProviderAuthorityMismatch)
	assert.Nil(t, store)
	assert.Nil(t, entries)
}

func TestPreparePlacementBackendsEmptyAdditionCanBeRevertedAfterLaterStartupFailure(t *testing.T) {
	t.Parallel()

	idA := startupStorageID(t, "253b5115-e341-40ee-8686-bb56f1d795d4")
	idB := startupStorageID(t, "f547a804-17f8-4977-99c8-1154a939d899")
	serverA := startupInventoryServer(t, idA, []backend.ProvisionInfo{}, []backend.RetainedLease{})
	serverB := startupInventoryServer(t, idB, []backend.ProvisionInfo{}, []backend.RetainedLease{})
	cfg := startupProviderConfig(t, serverA.URL, idA)
	cfg.Backends = append(cfg.Backends, config.BackendConfig{
		Name: "backend-b", URL: serverB.URL, Timeout: time.Second, SKUs: []string{"sku-b"},
	})

	store, _, err := preparePlacementBackends(t.Context(), cfg)
	require.NoError(t, err)
	assert.False(t, store.CurrentAdmissionBaseline().Valid(),
		"the topology probe must not mint admission authority")
	require.NoError(t, store.Close())

	// Simulate a later startup phase failing after the topology transaction, then
	// an operator reverting the deployment config before any reconciler sweep.
	cfg.Backends = cfg.Backends[:1]
	reverted, entries, err := preparePlacementBackends(t.Context(), cfg)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.NoError(t, reverted.VerifyBackendTopology([]string{"backend-a"}))
	assert.False(t, reverted.CurrentAdmissionBaseline().Valid())
	require.NoError(t, reverted.Close())
}

func TestPreparePlacementBackendsNonemptyAdditionCannotBeRemovedByConfigRevert(t *testing.T) {
	t.Parallel()

	idA := startupStorageID(t, "253b5115-e341-40ee-8686-bb56f1d795d4")
	idB := startupStorageID(t, "f547a804-17f8-4977-99c8-1154a939d899")
	serverA := startupInventoryServer(t, idA, []backend.ProvisionInfo{}, []backend.RetainedLease{})
	serverB := startupInventoryServer(t, idB, []backend.ProvisionInfo{{
		LeaseUUID: "d144291f-a36f-47a4-8ccf-48afe590e29d",
	}}, []backend.RetainedLease{})
	cfg := startupProviderConfig(t, serverA.URL, idA)
	cfg.Backends = append(cfg.Backends, config.BackendConfig{
		Name: "backend-b", URL: serverB.URL, Timeout: time.Second, SKUs: []string{"sku-b"},
	})

	store, _, err := preparePlacementBackends(t.Context(), cfg)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	cfg.Backends = cfg.Backends[:1]
	reverted, entries, err := preparePlacementBackends(t.Context(), cfg)
	require.ErrorIs(t, err, placement.ErrBackendTopologyInUse)
	require.ErrorContains(t, err, `latest complete inventory did not prove backend "backend-b" empty`)
	assert.Nil(t, reverted)
	assert.Nil(t, entries)

	inspector, inspectErr := placement.OpenStore(cfg.PlacementStoreDBPath, cfg.ProviderUUID)
	require.NoError(t, inspectErr)
	t.Cleanup(func() { _ = inspector.Close() })
	require.NoError(t, inspector.VerifyBackendTopology([]string{"backend-a", "backend-b"}),
		"a refused revert must leave the committed topology intact")
}

func startupInventoryServer(
	t *testing.T,
	storageID backendidentity.ID,
	provisions []backend.ProvisionInfo,
	retentions []backend.RetainedLease,
) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(backendidentity.ResponseHeader, storageID.String())
		response.Header().Set("Content-Type", "application/json")
		var payload any
		switch request.URL.Path {
		case "/provisions":
			payload = backend.ListProvisionsResponse{Provisions: provisions}
		case "/retentions":
			payload = backend.ListRetentionsResponse{Retentions: retentions}
		default:
			http.NotFound(response, request)
			return
		}
		assert.NoError(t, json.NewEncoder(response).Encode(payload))
	}))
	t.Cleanup(server.Close)
	return server
}

func startupProviderConfig(
	t *testing.T,
	backendURL string,
	storageID backendidentity.ID,
) *config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "placements.db")
	chainProof, err := placement.NewFreshChainProof(startupChainSnapshot{})
	require.NoError(t, err)
	backendProof, err := placement.NewFreshBackendProof(
		[]string{"backend-a"},
		map[string]placement.BackendInventory{
			"backend-a": {
				StorageIdentity:        storageID,
				Provisions:             []string{},
				ProvisionProviderUUIDs: map[string]string{},
				Retentions:             []string{},
			},
		},
	)
	require.NoError(t, err)
	target, err := placement.NewFreshInitializationTarget(
		path, startupChainSnapshot{}.ProviderUUID(), []string{"backend-a"},
	)
	require.NoError(t, err)
	quiescence, err := placement.ConfirmFreshQuiescence(target, target.Confirmation())
	require.NoError(t, err)
	proofCtx, cancelProof := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancelProof)
	plan, err := placement.NewFreshInitializationPlan(
		proofCtx, target, chainProof, backendProof, quiescence,
	)
	require.NoError(t, err)
	require.NoError(t, placement.InitializeFreshStoreContext(t.Context(), plan))
	return &config.Config{
		PlacementStoreDBPath: path,
		ProviderUUID:         startupChainSnapshot{}.ProviderUUID(),
		CallbackSecret:       config.Secret("0123456789abcdef0123456789abcdef"),
		Backends: []config.BackendConfig{{
			Name: "backend-a", URL: backendURL, Timeout: time.Second,
			SKUs: []string{"sku-a"}, IsDefault: true,
		}},
	}
}

func startupStorageID(t *testing.T, text string) backendidentity.ID {
	t.Helper()
	id, err := backendidentity.Parse(text)
	require.NoError(t, err)
	return id
}

type startupIdentityResolver map[string]backendidentity.ID

func (resolver startupIdentityResolver) ExpectedBackendStorageIdentity(
	backendName string,
) (backendidentity.ID, bool) {
	id, exists := resolver[backendName]
	return id, exists && id.Valid()
}

func TestCallbackHMACSecretsBindKeysToPreparedStorageIdentities(t *testing.T) {
	t.Parallel()
	idA := startupStorageID(t, "253b5115-e341-40ee-8686-bb56f1d795d4")
	idB := startupStorageID(t, "f547a804-17f8-4977-99c8-1154a939d899")
	const (
		secretA = "backend-a-secret-0123456789abcdef"
		secretB = "backend-b-secret-0123456789abcdef"
	)
	cfg := &config.Config{Backends: []config.BackendConfig{
		{Name: "backend-a", HMACSecret: secretA},
		{Name: "backend-b", HMACSecret: secretB},
	}}

	keyring, err := callbackHMACSecrets(cfg, startupIdentityResolver{
		"backend-a": idA,
		"backend-b": idB,
	})
	require.NoError(t, err)
	assert.Equal(t, map[backendidentity.ID]string{idA: secretA, idB: secretB}, keyring)

	_, err = callbackHMACSecrets(cfg, startupIdentityResolver{"backend-a": idA})
	require.ErrorContains(t, err, `backend "backend-b" has no prepared storage identity`)
	_, err = callbackHMACSecrets(cfg, startupIdentityResolver{
		"backend-a": idA,
		"backend-b": idA,
	})
	require.ErrorContains(t, err, "reuses callback storage identity")
}

func TestNewProductionBackendClientSignsWithBackendSpecificKey(t *testing.T) {
	t.Parallel()
	id := startupStorageID(t, "253b5115-e341-40ee-8686-bb56f1d795d4")
	const secret = "backend-a-secret-0123456789abcdef"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set(backendidentity.ResponseHeader, id.String())
		assert.Equal(t, []string{id.String()}, request.URL.Query()[backendidentity.QueryParameter])
		signature := request.Header.Get(hmacauth.SignatureHeader)
		if err := hmacauth.VerifyRequest(secret, request, nil, signature, 5*time.Minute); err != nil {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		response.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	configuredBackend := config.BackendConfig{
		Name: "backend-a", URL: server.URL, Timeout: time.Second, HMACSecret: secret,
	}
	cfg := &config.Config{Backends: []config.BackendConfig{configuredBackend}}
	client, err := newProductionBackendClient(
		configuredBackend, cfg, startupIdentityResolver{"backend-a": id},
	)
	require.NoError(t, err)
	require.NoError(t, client.Health(t.Context()))
}
