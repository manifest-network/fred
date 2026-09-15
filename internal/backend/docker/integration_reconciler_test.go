//go:build integration

package docker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/callbackurl"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner"
	providermaintenance "github.com/manifest-network/fred/internal/provisioner/maintenance"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

// integrationAcknowledger wraps a chain client as an Acknowledger for integration tests.
type integrationChainClient interface {
	provisioner.ReconcilerChainClient
	AcknowledgeLeases(context.Context, []string) (uint64, []string, error)
}

type integrationAcknowledger struct {
	chainClient integrationChainClient
}

func (a *integrationAcknowledger) Acknowledge(ctx context.Context, leaseUUID string) (bool, string, error) {
	n, hashes, err := a.chainClient.AcknowledgeLeases(ctx, []string{leaseUUID})
	if err != nil {
		return false, "", err
	}
	var hash string
	if len(hashes) > 0 {
		hash = hashes[0]
	}
	return n > 0, hash, nil
}

type integrationProviderControlPlane struct {
	provisioner.ReconcilerChainClient
	placement.CallbackAcknowledger
}

// testReconcilerTracker adapts the shared operation registry and payload store
// to the reconciler's narrow production runtime port.
type testReconcilerTracker struct {
	callbacks        *placement.AuthenticatedCallbackCoordinator
	callbackVerifier hmacauth.CallbackProofVerifier
	callbackPath     string
	backendStorageID backendidentity.ID
	store            *payload.Store
}

// integrationIdentityBackend models the production HTTP client's
// identity-bearing inventory contract while keeping these tests' direct,
// in-process Docker transport seam.
type integrationIdentityBackend struct {
	*Backend
}

// integrationPayloadPersister is the narrow production Manager.OverwritePayload
// boundary used by the maintenance service. Keeping the adapter here ensures
// the integration test cannot manually reorder backend acceptance and payload
// durability.
type integrationPayloadPersister struct {
	store *payload.Store
}

func (persister integrationPayloadPersister) OverwritePayload(
	leaseUUID string,
	value []byte,
) error {
	return persister.store.Put(leaseUUID, value)
}

// loseAcceptedUpdateResponseBackend models the provider losing the HTTP
// response after docker-backend has durably accepted the command. The real
// backend, including its aggregate WAL and worker, remains underneath.
type loseAcceptedUpdateResponseBackend struct {
	integrationIdentityBackend
	mu       sync.Mutex
	loseNext bool
	calls    int
}

func (wrapped *loseAcceptedUpdateResponseBackend) Update(
	ctx context.Context,
	request backend.UpdateRequest,
) error {
	if err := wrapped.integrationIdentityBackend.Update(ctx, request); err != nil {
		return err
	}
	wrapped.mu.Lock()
	defer wrapped.mu.Unlock()
	wrapped.calls++
	if wrapped.loseNext {
		wrapped.loseNext = false
		return errors.New("injected lost backend response after durable update acceptance")
	}
	return nil
}

func (wrapped *loseAcceptedUpdateResponseBackend) updateCount() int {
	wrapped.mu.Lock()
	defer wrapped.mu.Unlock()
	return wrapped.calls
}

func (b integrationIdentityBackend) ListProvisionsWithIdentity(
	ctx context.Context,
) ([]backend.ProvisionInfo, backendidentity.ID, error) {
	provisions, err := b.ListProvisions(ctx)
	return provisions, b.StorageIdentity(), err
}

func (b integrationIdentityBackend) ListRetentionsWithIdentity(
	ctx context.Context,
) ([]backend.RetainedLease, backendidentity.ID, error) {
	retentions, err := b.ListRetentions(ctx)
	return retentions, b.StorageIdentity(), err
}

func (t *testReconcilerTracker) HasPayload(leaseUUID string) (bool, error) {
	return t.store.Has(leaseUUID)
}

func (t *testReconcilerTracker) PayloadStore() *payload.Store {
	return t.store
}

// finishProvisionCallback settles the typed operation from the exact signed
// request received by this integration harness. Production performs the same
// verification in its HTTP callback handler; accepting a lease ID and
// reconstructing a fresh operation URL here would erase the route/generation
// authority that these tests are meant to preserve.
func (t *testReconcilerTracker) finishProvisionCallback(delivery integrationCallbackDelivery) bool {
	if t == nil || t.callbacks == nil || !t.callbackVerifier.Valid() ||
		t.callbackPath == "" ||
		delivery.LeaseUUID == "" || delivery.method == "" || delivery.requestURI == "" ||
		len(delivery.body) == 0 || delivery.signature == "" {
		return false
	}
	now := time.Now()
	proof, err := t.callbackVerifier.VerifyRoutedWithTime(
		testCallbackSecret,
		delivery.method,
		delivery.requestURI,
		delivery.body,
		delivery.signature,
		t.backendStorageID.String(),
		t.callbackPath,
		5*time.Minute, time.Minute, now,
	)
	if err != nil {
		return false
	}
	result, err := t.callbacks.Apply(context.Background(), proof)
	return err == nil && result.OperationOutcome() == placement.CallbackOperationSucceeded
}

func configuredIntegrationCallbackPath(t *testing.T, rawBase string) string {
	t.Helper()
	base, err := callbackurl.ParseBase(rawBase)
	require.NoError(t, err)
	endpoint, err := base.ProvisionURL()
	require.NoError(t, err)
	return endpoint.EscapedPath()
}

func waitForCallbackDelivery(
	t *testing.T,
	ch <-chan integrationCallbackDelivery,
	leaseUUID string,
	timeout time.Duration,
) integrationCallbackDelivery {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case delivery := <-ch:
			if delivery.LeaseUUID == leaseUUID {
				return delivery
			}
			t.Logf("skipping callback for lease %s (status=%s, waiting for %s)",
				delivery.LeaseUUID, delivery.Status, leaseUUID)
		case <-deadline.C:
			t.Fatalf("timeout waiting for callback for lease %s", leaseUUID)
		}
	}
}

// reconcilerTestEnv holds all components for a full-stack reconciler integration test.
type reconcilerTestEnv struct {
	backend        *Backend
	router         *backend.Router
	execution      *placement.ExecutionCoordinator
	reconciler     *provisioner.Reconciler
	tracker        *testReconcilerTracker
	placementStore *placement.Store
	chainClient    *chaintest.MockClient
	callbackCh     <-chan integrationCallbackDelivery
	callbackURL    string
	providerUUID   string
	payloadPath    string
	placementPath  string
}

// installExactLeaseLookupFallback makes the integration chain double obey the
// same list/get contract as the real ledger. Typed reconciliation re-reads a
// lease under its lifecycle claim immediately before dispatch; returning nil
// there means "the lease disappeared" and must safely defer the action. Tests
// that need a different terminal-state answer install their own GetLeaseFunc,
// which this helper deliberately preserves.
func installExactLeaseLookupFallback(chainClient *chaintest.MockClient, providerUUID string) {
	if chainClient.GetLeaseFunc != nil {
		return
	}
	chainClient.GetLeaseFunc = func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
		pending, err := chainClient.GetPendingLeases(ctx, providerUUID)
		if err != nil {
			return nil, err
		}
		for i := range pending {
			if pending[i].Uuid == leaseUUID {
				lease := pending[i]
				return &lease, nil
			}
		}

		active, err := chainClient.GetActiveLeasesByProvider(ctx, providerUUID)
		if err != nil {
			return nil, err
		}
		for i := range active {
			if active[i].Uuid == leaseUUID {
				lease := active[i]
				return &lease, nil
			}
		}
		return nil, nil
	}
}

// configureEmptyPlacement projects the actual backend's complete empty
// inventory after configureBackendTopologyForTest has bound its identity.
func configureEmptyPlacement(
	t *testing.T,
	reconciliation *placement.ReconciliationCoordinator,
	b *Backend,
) {
	t.Helper()
	sweep, err := reconciliation.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	provisionReceipt, err := sweep.CollectProvisionInventory(t.Context(), b.Name())
	require.NoError(t, err)
	retentionReceipt, err := sweep.CollectRetentionInventory(t.Context(), b.Name())
	require.NoError(t, err)
	disposition, err := sweep.RecordBackendInventory(provisionReceipt, retentionReceipt)
	require.NoError(t, err)
	require.Equal(t, placement.BackendInventoryAuthoritative, disposition)
	require.NoError(t, sweep.SealInventory())
	_, err = sweep.Project(placement.ReconciliationProjection{})
	require.NoError(t, err)
}

// configureBackendTopologyForTest obtains the same complete inventory
// observation that production startup requires before binding runtime
// components. This replaces the removable seed topology in a fixture store
// atomically with the identity and inventory of the actual backend.
func configureBackendTopologyForTest(t *testing.T, store *placement.Store, b *Backend) {
	t.Helper()
	ctx := t.Context()
	provisions, err := b.ListProvisions(ctx)
	require.NoError(t, err)
	retentions, err := b.ListRetentions(ctx)
	require.NoError(t, err)
	observation, err := placement.NewCompleteBackendObservation(
		b.StorageIdentity(), provisions, retentions,
	)
	require.NoError(t, err)
	require.NoError(t, store.ConfigureBackendTopologyWithCompleteObservations(
		[]string{b.Name()}, map[string]placement.CompleteBackendObservation{
			b.Name(): observation,
		},
	))
}

// testReconcilerSetup creates a full-stack test environment:
// real docker backend + reconciler + mock chain + tracker + payload store.
func testReconcilerSetup(t *testing.T, chainClient *chaintest.MockClient, extraCfg ...func(*Config)) *reconcilerTestEnv {
	return testReconcilerSetupWithRuntime(t, chainClient, nil, extraCfg...)
}

// testReconcilerSetupWithRuntime binds an optional transport fixture before
// constructing the placement execution aggregate. This mirrors production's
// one-time runtime binding: tests may alter transport behavior, but cannot
// swap the router underneath an already-issued coordinator.
func testReconcilerSetupWithRuntime(
	t *testing.T,
	chainClient *chaintest.MockClient,
	wrap func(integrationIdentityBackend) backend.Backend,
	extraCfg ...func(*Config),
) *reconcilerTestEnv {
	t.Helper()
	const providerUUID = testProviderUUID
	installExactLeaseLookupFallback(chainClient, providerUUID)

	callbackServer, callbackCh := startCallbackDeliveryServer(t)

	// Create a backend with fast reconcile for detection
	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.ReconcileInterval = 2 * time.Second
		for _, fn := range extraCfg {
			fn(cfg)
		}
	})

	// Create the exact runtime that every purpose facet will share.
	identityBackend := integrationIdentityBackend{Backend: b}
	runtimeBackend := backend.Backend(identityBackend)
	if wrap != nil {
		runtimeBackend = wrap(identityBackend)
		require.NotNil(t, runtimeBackend)
	}
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{
			Backend: runtimeBackend, IsDefault: true,
		}},
	})
	require.NoError(t, err)

	// Create payload store in temp directory
	payloadPath := filepath.Join(t.TempDir(), "payloads.db")
	store, err := payload.NewStore(payload.StoreConfig{DBPath: payloadPath})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// Create in-flight tracker + reconciler tracker adapter
	tracker := &testReconcilerTracker{store: store}
	placementPath := filepath.Join(t.TempDir(), "placements.db")
	callbackRoutes, err := placement.NewCallbackRouteFactory(callbackServer.URL)
	require.NoError(t, err)
	placementStore, err := placementstore.NewStoreForProvider(
		placementPath, providerUUID, placement.WithCallbackRouteFactory(callbackRoutes),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = placementStore.Close() })
	// BindBackendRuntime verifies the durable topology during construction, so
	// replace the fixture seed with the actual backend identity first.
	configureBackendTopologyForTest(t, placementStore, b)
	// Create a simple acknowledger that delegates to chainClient for integration tests
	integrationAck := &integrationAcknowledger{chainClient: chainClient}
	coordinator, err := placementStore.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution, err := coordinator.BindBackendRuntime(router, integrationProviderControlPlane{
		ReconcilerChainClient: chainClient,
		CallbackAcknowledger:  integrationAck,
	})
	require.NoError(t, err)
	callbackVerifier, callbackConsumer := hmacauth.NewCallbackProofBoundary()
	callbackCoordinator, err := execution.AuthenticatedCallbackCoordinator(callbackConsumer)
	require.NoError(t, err)
	tracker.callbacks = callbackCoordinator
	tracker.callbackVerifier = callbackVerifier
	tracker.callbackPath = configuredIntegrationCallbackPath(t, callbackServer.URL)
	tracker.backendStorageID = b.StorageIdentity()
	reconciliation, err := execution.ReconciliationCoordinator(tracker.store, nil)
	require.NoError(t, err)
	configureEmptyPlacement(t, reconciliation, b)

	reconciler, err := provisioner.NewReconciler(
		provisioner.ReconcilerConfig{
			Interval:               1 * time.Hour, // manual RunOnce only
			MaxReprovisionAttempts: 3,
			Coordinator:            reconciliation,
		},
		tracker,
	)
	require.NoError(t, err)

	return &reconcilerTestEnv{
		backend:        b,
		router:         router,
		execution:      execution,
		reconciler:     reconciler,
		tracker:        tracker,
		placementStore: placementStore,
		chainClient:    chainClient,
		callbackCh:     callbackCh,
		callbackURL:    callbackServer.URL,
		providerUUID:   providerUUID,
		payloadPath:    payloadPath,
		placementPath:  placementPath,
	}
}

func newIntegrationMaintenanceService(
	t *testing.T,
	env *reconcilerTestEnv,
	execution *placement.ExecutionCoordinator,
) *providermaintenance.Service {
	t.Helper()
	maintenanceCoordinator, err := execution.MaintenanceCoordinator(
		integrationPayloadPersister{store: env.tracker.store},
	)
	require.NoError(t, err)
	service, err := providermaintenance.NewService(providermaintenance.Config{
		Coordinator: maintenanceCoordinator,
	})
	require.NoError(t, err)
	return service
}

func newIntegrationExecutionAfterProviderRestart(
	t *testing.T,
	env *reconcilerTestEnv,
	authority *placement.Store,
	router *backend.Router,
) *placement.ExecutionCoordinator {
	t.Helper()
	coordinator, err := authority.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution, err := coordinator.BindBackendRuntime(router, integrationProviderControlPlane{
		ReconcilerChainClient: env.chainClient,
		CallbackAcknowledger:  &integrationAcknowledger{chainClient: env.chainClient},
	})
	require.NoError(t, err)
	return execution
}

func newIntegrationReconcilerAfterProviderRestart(
	t *testing.T,
	execution *placement.ExecutionCoordinator,
	tracker *testReconcilerTracker,
) *provisioner.Reconciler {
	t.Helper()
	reconciliation, err := execution.ReconciliationCoordinator(tracker.store, nil)
	require.NoError(t, err)
	reconciler, err := provisioner.NewReconciler(
		provisioner.ReconcilerConfig{
			Interval:               time.Hour,
			MaxReprovisionAttempts: 3,
			Coordinator:            reconciliation,
		},
		tracker,
	)
	require.NoError(t, err)
	return reconciler
}

// makeLease creates a billingtypes.Lease with a payload MetaHash.
func makeLease(leaseUUID, tenant, providerUUID, sku string, quantity uint64, metaHash []byte) billingtypes.Lease {
	return billingtypes.Lease{
		Uuid:         leaseUUID,
		Tenant:       tenant,
		ProviderUuid: providerUUID,
		Items: []billingtypes.LeaseItem{
			{
				SkuUuid:  sku,
				Quantity: quantity,
			},
		},
		MetaHash: metaHash,
	}
}

func countMaintenanceRelease(
	releases []shared.Release,
	id shared.MaintenanceID,
) int {
	count := 0
	for _, release := range releases {
		if release.MaintenanceID == id {
			count++
		}
	}
	return count
}

func TestIntegration_Reconciler_ContainerDied_ReProvisions(t *testing.T) {
	leaseUUID := newIntegrationLeaseUUID()
	tenant := "test-tenant"
	sku := "docker-micro"

	// Prepare the manifest payload
	manifest := manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	// Set up mock chain state - starts as PENDING
	var mu sync.Mutex
	leaseState := billingtypes.LEASE_STATE_PENDING

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_PENDING {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				lease.State = billingtypes.LEASE_STATE_PENDING
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_ACTIVE {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				lease.State = billingtypes.LEASE_STATE_ACTIVE
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 1, []string{"txhash1"}, nil
		},
	}

	env := testReconcilerSetup(t, mockChain)

	// Store payload in PayloadStore
	stored := env.tracker.store.Store(leaseUUID, payload)
	require.True(t, stored)

	ctx := context.Background()

	// RunOnce → should see PENDING + not provisioned → start provisioning
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Wait for success callback.
	initialDelivery := waitForCallbackDelivery(t, env.callbackCh, leaseUUID, 2*time.Minute)
	assert.Equal(t, backend.CallbackStatusSuccess, initialDelivery.Status)

	// Untrack in-flight (simulates what the handler would do on callback)
	require.True(t, env.tracker.finishProvisionCallback(initialDelivery))

	// Transition lease to ACTIVE
	mu.Lock()
	leaseState = billingtypes.LEASE_STATE_ACTIVE
	mu.Unlock()

	// RunOnce to acknowledge (PENDING+Ready → acknowledge)
	// But we just set it to ACTIVE, so it should be ACTIVE+Ready → no action (healthy)
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Kill the container
	containers := inspectProvisionContainers(t, leaseUUID)
	require.NotEmpty(t, containers)
	killContainer(t, containers[0].ID)

	// Wait for the docker backend's recoverState to detect failure
	waitForProvisionStatus(t, env.backend, leaseUUID, backend.ProvisionStatusFailed, 30*time.Second)

	// Drain the failure callback from recoverState before triggering re-provision
	select {
	case cb := <-env.callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusFailed, cb.Status, "should receive failure callback from recoverState")
	case <-time.After(10 * time.Second):
		// recoverState may have already fired it; continue
	}

	// RunOnce → sees ACTIVE + Failed → re-provisions
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Wait for new success callback from re-provision
	select {
	case cb := <-env.callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for re-provision success callback")
	}

	// Verify new container is running
	info, err := env.backend.GetInfo(ctx, leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, info)
}

func TestIntegration_Reconciler_CrashLoop_ClosesLease(t *testing.T) {
	leaseUUID := newIntegrationLeaseUUID()
	tenant := "test-tenant"
	sku := "docker-micro"

	manifest := manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	leaseState := billingtypes.LEASE_STATE_PENDING
	var closedLeases []string

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_PENDING {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				lease.State = billingtypes.LEASE_STATE_PENDING
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_ACTIVE {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				lease.State = billingtypes.LEASE_STATE_ACTIVE
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 1, []string{"txhash1"}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			closedLeases = append(closedLeases, leaseUUIDs...)
			mu.Unlock()
			return 1, []string{"txhash-close"}, nil
		},
	}

	env := testReconcilerSetup(t, mockChain)

	stored := env.tracker.store.Store(leaseUUID, payload)
	require.True(t, stored)

	ctx := context.Background()

	// Initial provision
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	initialDelivery := waitForCallbackDelivery(t, env.callbackCh, leaseUUID, 2*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, initialDelivery.Status)
	require.True(t, env.tracker.finishProvisionCallback(initialDelivery))

	// Transition to ACTIVE
	mu.Lock()
	leaseState = billingtypes.LEASE_STATE_ACTIVE
	mu.Unlock()

	// Kill → FailCount=1 → re-provision → Kill → FailCount=2 → re-provision → Kill → FailCount=3 → close
	for i := 1; i <= 3; i++ {
		// Kill the container
		containers := inspectProvisionContainers(t, leaseUUID)
		require.NotEmpty(t, containers, "expected running container for iteration %d", i)
		killContainer(t, containers[0].ID)

		// Wait for backend to detect failure
		waitForProvisionStatus(t, env.backend, leaseUUID, backend.ProvisionStatusFailed, 30*time.Second)

		// Drain the failure callback from recoverState
		select {
		case <-env.callbackCh:
		case <-time.After(10 * time.Second):
			// may not always get it if backend already detected
		}

		// RunOnce → reconciler sees ACTIVE + Failed
		err = env.reconciler.RunOnce(ctx)
		require.NoError(t, err)

		if i < 3 {
			// Should re-provision (FailCount < 3)
			delivery := waitForCallbackDelivery(t, env.callbackCh, leaseUUID, 2*time.Minute)
			assert.Equal(t, backend.CallbackStatusSuccess, delivery.Status,
				"re-provision %d should succeed", i)
			require.True(t, env.tracker.finishProvisionCallback(delivery))
		}
	}

	// After the 3rd kill, reconciler should have called CloseLeases
	mu.Lock()
	closed := closedLeases
	mu.Unlock()
	assert.Contains(t, closed, leaseUUID, "CloseLeases should have been called with the lease UUID")
}

func TestIntegration_Reconciler_OrphanCleanup(t *testing.T) {
	leaseUUID := newIntegrationLeaseUUID()
	tenant := "test-tenant"
	sku := "docker-micro"

	manifest := manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	leaseVisible := true
	var deprovisionedViaChain bool

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseVisible {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				lease.State = billingtypes.LEASE_STATE_PENDING
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
		// A per-lease lookup still finds the lease once it leaves the PENDING
		// list — CLOSED, not absent, which is how the real chain ends a lease
		// (x/billing never deletes one) and the positive evidence the orphan
		// pass now requires before deprovisioning (ENG-654).
		GetLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			lease := makeLease(uuid, tenant, testProviderUUID, sku, 1, hash[:])
			lease.State = billingtypes.LEASE_STATE_CLOSED
			if leaseVisible {
				lease.State = billingtypes.LEASE_STATE_PENDING
			}
			return &lease, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 1, []string{"txhash1"}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			mu.Lock()
			deprovisionedViaChain = true
			mu.Unlock()
			return 1, []string{"txhash-close"}, nil
		},
	}

	env := testReconcilerSetup(t, mockChain)

	stored := env.tracker.store.Store(leaseUUID, payload)
	require.True(t, stored)

	ctx := context.Background()

	// Provision via reconciler
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	initialDelivery := waitForCallbackDelivery(t, env.callbackCh, leaseUUID, 2*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, initialDelivery.Status)
	require.True(t, env.tracker.finishProvisionCallback(initialDelivery))

	// Verify container exists
	containers := inspectProvisionContainers(t, leaseUUID)
	require.NotEmpty(t, containers)

	// Make the lease disappear from chain (simulating close/expiry)
	mu.Lock()
	leaseVisible = false
	mu.Unlock()

	// RunOnce → orphan detection should deprovision
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Verify container is removed
	require.Eventually(t, func() bool {
		c := inspectProvisionContainers(t, leaseUUID)
		return len(c) == 0
	}, 30*time.Second, 500*time.Millisecond, "orphaned container should be removed")

	// Verify ListProvisions is empty for this lease
	provisions, err := env.backend.ListProvisions(ctx)
	require.NoError(t, err)
	for _, p := range provisions {
		assert.NotEqual(t, leaseUUID, p.LeaseUUID, "orphaned provision should be removed")
	}

	_ = deprovisionedViaChain // tracked but not asserted (deprovision is on the backend, not close on chain)
}

func TestIntegration_Reconciler_MultiContainer_PartialKill_Recovers(t *testing.T) {
	leaseUUID := newIntegrationLeaseUUID()
	tenant := "test-tenant"
	sku := "docker-micro"

	manifest := manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	leaseState := billingtypes.LEASE_STATE_PENDING

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_PENDING {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 2, hash[:])
				lease.State = billingtypes.LEASE_STATE_PENDING
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_ACTIVE {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 2, hash[:])
				lease.State = billingtypes.LEASE_STATE_ACTIVE
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 1, []string{"txhash1"}, nil
		},
	}

	env := testReconcilerSetup(t, mockChain)

	stored := env.tracker.store.Store(leaseUUID, payload)
	require.True(t, stored)

	ctx := context.Background()

	// RunOnce → PENDING + not provisioned → start provisioning (quantity=2)
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Wait for success callback.
	initialDelivery := waitForCallbackDelivery(t, env.callbackCh, leaseUUID, 2*time.Minute)
	assert.Equal(t, backend.CallbackStatusSuccess, initialDelivery.Status)
	require.True(t, env.tracker.finishProvisionCallback(initialDelivery))

	// Verify 2 containers are running
	containers := inspectProvisionContainers(t, leaseUUID)
	require.Len(t, containers, 2, "should have 2 containers after initial provision")

	// Transition lease to ACTIVE
	mu.Lock()
	leaseState = billingtypes.LEASE_STATE_ACTIVE
	mu.Unlock()

	// RunOnce → ACTIVE + Ready → no action (healthy)
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Kill one of the two containers
	killContainer(t, containers[0].ID)

	// Wait for backend to detect failure
	waitForProvisionStatus(t, env.backend, leaseUUID, backend.ProvisionStatusFailed, 30*time.Second)

	// Drain the failure callback from recoverState
	select {
	case cb := <-env.callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusFailed, cb.Status, "should receive failure callback from recoverState")
	case <-time.After(10 * time.Second):
		// recoverState may have already fired it; continue
	}

	// RunOnce → sees ACTIVE + Failed → re-provisions
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Wait for success callback from re-provision
	select {
	case cb := <-env.callbackCh:
		assert.Equal(t, leaseUUID, cb.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for re-provision success callback")
	}

	// Verify 2 containers are running again after recovery
	newContainers := inspectProvisionContainers(t, leaseUUID)
	require.Len(t, newContainers, 2, "should have 2 running containers after re-provision")

	// Verify provision is back to Ready
	info := getProvisionInfo(t, env.backend, leaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, info.Status)
}

func TestIntegration_Reconciler_PendingReady_Acknowledges(t *testing.T) {
	leaseUUID := newIntegrationLeaseUUID()
	tenant := "test-tenant"
	sku := "docker-micro"

	manifest := manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	var acknowledgedLeases []string

	// Chain always returns PENDING (simulating missed ack)
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
			lease.State = billingtypes.LEASE_STATE_PENDING
			return []billingtypes.Lease{lease}, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return nil, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			mu.Lock()
			acknowledgedLeases = append(acknowledgedLeases, leaseUUIDs...)
			mu.Unlock()
			return 1, []string{"txhash-ack"}, nil
		},
	}

	env := testReconcilerSetup(t, mockChain)

	stored := env.tracker.store.Store(leaseUUID, payload)
	require.True(t, stored)

	ctx := context.Background()

	// First RunOnce → PENDING + not provisioned → start provisioning
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Wait for success callback (container ready).
	initialDelivery := waitForCallbackDelivery(t, env.callbackCh, leaseUUID, 2*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, initialDelivery.Status)
	require.True(t, env.tracker.finishProvisionCallback(initialDelivery))

	// Second RunOnce → chain still returns PENDING but backend has it Ready
	// → should acknowledge
	err = env.reconciler.RunOnce(ctx)
	require.NoError(t, err)

	mu.Lock()
	acked := acknowledgedLeases
	mu.Unlock()

	assert.Contains(t, acked, leaseUUID, "AcknowledgeLeases should have been called with the lease UUID")

	// Cleanup
	err = env.backend.Deprovision(ctx, leaseUUID)
	require.NoError(t, err)
}

// TestIntegration_Reconciler_DetectsFailureWithoutRecoverState is a regression
// test for the reconciler ↔ backend state refresh race condition.
//
// Without RefreshState, the reconciler reads stale in-memory state from
// ListProvisions and misses container failures that happened between
// recoverState cycles. With RefreshState called before ListProvisions, every
// reconciler tick re-lists live Docker state and hands a crashed container to
// the lease SM — so the failure is detected and re-provisioned even with the
// background recoverState loop disabled. Detection and re-provision span more
// than one tick: recoverState keeps the provision Ready and fires
// containerDiedMsg, but the SM's Ready→Failing→Failed transition is async
// (diagnostics are gathered off-actor), so the ListProvisions in that same tick
// can still observe Ready and skip re-provision. The assertion below therefore
// drives RunOnce on an interval — mirroring the production reconciler, which
// self-heals on its next tick — instead of betting on a single tick.
func TestIntegration_Reconciler_DetectsFailureWithoutRecoverState(t *testing.T) {
	leaseUUID := newIntegrationLeaseUUID()
	tenant := "test-tenant"
	sku := "docker-micro"

	manifest := manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	manifestPayload, err := json.Marshal(manifest)
	require.NoError(t, err)

	hash := sha256.Sum256(manifestPayload)

	var mu sync.Mutex
	leaseState := billingtypes.LEASE_STATE_PENDING

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_PENDING {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				lease.State = billingtypes.LEASE_STATE_PENDING
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_ACTIVE {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				lease.State = billingtypes.LEASE_STATE_ACTIVE
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 1, []string{"txhash1"}, nil
		},
	}

	// Use a backend with ReconcileInterval=1h so the background recoverState
	// loop effectively never runs during this test. The only way the reconciler
	// can see fresh state is through RefreshState being called inline.
	callbackServer, callbackCh := startCallbackDeliveryServer(t)
	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		cfg.ReconcileInterval = 1 * time.Hour // disable background recoverState
	})

	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{
			Backend: integrationIdentityBackend{Backend: b}, IsDefault: true,
		}},
	})
	require.NoError(t, err)

	store, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(t.TempDir(), "payloads.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	tracker := &testReconcilerTracker{
		store: store,
	}
	providerUUID := testProviderUUID
	callbackRoutes, err := placement.NewCallbackRouteFactory(callbackServer.URL)
	require.NoError(t, err)
	placementStore, err := placementstore.NewStoreForProvider(
		filepath.Join(t.TempDir(), "placements.db"), providerUUID,
		placement.WithCallbackRouteFactory(callbackRoutes),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = placementStore.Close() })
	configureBackendTopologyForTest(t, placementStore, b)

	installExactLeaseLookupFallback(mockChain, providerUUID)
	integrationAck2 := &integrationAcknowledger{chainClient: mockChain}
	coordinator, err := placementStore.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution, err := coordinator.BindBackendRuntime(router, integrationProviderControlPlane{
		ReconcilerChainClient: mockChain,
		CallbackAcknowledger:  integrationAck2,
	})
	require.NoError(t, err)
	callbackVerifier, callbackConsumer := hmacauth.NewCallbackProofBoundary()
	callbackCoordinator, err := execution.AuthenticatedCallbackCoordinator(callbackConsumer)
	require.NoError(t, err)
	tracker.callbacks = callbackCoordinator
	tracker.callbackVerifier = callbackVerifier
	tracker.callbackPath = configuredIntegrationCallbackPath(t, callbackServer.URL)
	tracker.backendStorageID = b.StorageIdentity()
	reconciliation, err := execution.ReconciliationCoordinator(tracker.store, nil)
	require.NoError(t, err)
	configureEmptyPlacement(t, reconciliation, b)
	reconciler, err := provisioner.NewReconciler(
		provisioner.ReconcilerConfig{
			Interval:               1 * time.Hour,
			MaxReprovisionAttempts: 3,
			Coordinator:            reconciliation,
		},
		tracker,
	)
	require.NoError(t, err)

	// Store payload
	stored := store.Store(leaseUUID, manifestPayload)
	require.True(t, stored)

	ctx := context.Background()

	// 1. RunOnce → PENDING + not provisioned → start provisioning
	err = reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// Wait for success callback.
	initialDelivery := waitForCallbackDelivery(t, callbackCh, leaseUUID, 2*time.Minute)
	assert.Equal(t, backend.CallbackStatusSuccess, initialDelivery.Status)
	require.True(t, tracker.finishProvisionCallback(initialDelivery))

	// 2. Transition lease to ACTIVE
	mu.Lock()
	leaseState = billingtypes.LEASE_STATE_ACTIVE
	mu.Unlock()

	// 3. Kill the container via Docker API
	containers := inspectProvisionContainers(t, leaseUUID)
	require.NotEmpty(t, containers)
	killContainer(t, containers[0].ID)
	waitForContainerExited(t, containers[0].ID)

	// 4. Do NOT wait for the background recoverState loop (ReconcileInterval=1h,
	//    so it never fires). Drive the reconciler manually instead. Each RunOnce
	//    calls RefreshState (→ recoverState) before ListProvisions, so it re-lists
	//    live Docker state and hands the dead container to the lease SM.

	// Drain any failure callback that RefreshState might send
	drainCallbacks := func() {
		for {
			select {
			case <-callbackCh:
			case <-time.After(1 * time.Second):
				return
			}
		}
	}

	err = reconciler.RunOnce(ctx)
	require.NoError(t, err)

	// 5. The reconciler re-provisions the crashed container; wait for the success
	//    callback. Detection and re-provision are NOT atomic within one RunOnce:
	//    RefreshState fires containerDiedMsg, but the SM's Ready→Failing→Failed
	//    transition is asynchronous, so the ListProvisions in that same tick can
	//    still observe a Ready provision and skip re-provision. In production the
	//    reconciler runs on an interval and self-heals on the next tick, so model
	//    that here by ticking RunOnce until the provision is re-provisioned —
	//    rather than betting on a single tick winning the race (which flaked on
	//    CI). Failed callbacks emitted while the SM settles are skipped.
	successTimeout := time.After(2 * time.Minute)
	retick := time.NewTicker(2 * time.Second)
	defer retick.Stop()
	gotSuccess := false
	for !gotSuccess {
		select {
		case cb := <-callbackCh:
			if cb.Status == backend.CallbackStatusSuccess {
				assert.Equal(t, leaseUUID, cb.LeaseUUID)
				gotSuccess = true
			}
		case <-retick.C:
			require.NoError(t, reconciler.RunOnce(ctx))
		case <-successTimeout:
			t.Fatal("timeout waiting for re-provision success callback after repeated reconciler ticks")
		}
	}

	drainCallbacks()

	// 6. Verify the new container is running
	newContainers := inspectProvisionContainers(t, leaseUUID)
	require.NotEmpty(t, newContainers, "should have running container after re-provision")

	info := getProvisionInfo(t, b, leaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, info.Status)
}

func TestIntegration_Reconciler_RetainRestoreLifecycle(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	leaseUUID := newIntegrationLeaseUUID()
	tenant := "test-tenant"
	const sku = "docker-small" // stateful: DiskMB=1024

	appManifest := manifest.Manifest{Image: "redis:7", Command: []string{"sleep", "3600"}}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)
	hash := sha256.Sum256(payload)

	var mu sync.Mutex
	leaseVisible := true // flip false to simulate the on-chain close/auto-close
	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(_ context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseVisible {
				l := makeLease(leaseUUID, tenant, providerUUID, sku, 1, hash[:])
				l.State = billingtypes.LEASE_STATE_PENDING
				return []billingtypes.Lease{l}, nil
			}
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(_ context.Context, _ string) ([]billingtypes.Lease, error) { return nil, nil },
		AcknowledgeLeasesFunc:         func(_ context.Context, _ []string) (uint64, []string, error) { return 1, []string{"tx"}, nil },
		// The auto-close this test simulates leaves the lease CLOSED on chain, not
		// erased from it — x/billing never deletes a lease. That distinction is
		// what authorises the orphan sweep below to soft-delete (ENG-654); a lease
		// the chain denied all knowledge of would be kept instead.
		GetLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			l := makeLease(uuid, tenant, testProviderUUID, sku, 1, hash[:])
			l.State = billingtypes.LEASE_STATE_CLOSED
			if leaseVisible {
				l.State = billingtypes.LEASE_STATE_PENDING
			}
			return &l, nil
		},
	}

	env := testReconcilerSetup(t, mockChain, func(cfg *Config) {
		cfg.VolumeDataPath = mountPath
		cfg.VolumeMountPath = mountPath
		cfg.VolumeFilesystem = "btrfs"
		cfg.RetainOnClose = true
		cfg.RetentionDBPath = filepath.Join(t.TempDir(), "retention.db")
		cfg.RetentionMaxAge = 0 // reaper OFF: assert soft-delete persists
		cfg.RetentionReapInterval = 0
	})
	require.True(t, env.tracker.store.Store(leaseUUID, payload))
	ctx := context.Background()

	// 1. Chain-driven provision.
	require.NoError(t, env.reconciler.RunOnce(ctx))
	initialDelivery := waitForCallbackDelivery(t, env.callbackCh, leaseUUID, 3*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, initialDelivery.Status)
	require.True(t, env.tracker.finishProvisionCallback(initialDelivery))

	// 2. Sentinel into the managed volume.
	cid := getContainerID(t, leaseUUID)
	require.True(t, containerHasBindMount(t, cid, "/data"))
	execInContainer(t, cid, []string{"sh", "-c", "echo recon-restore-sentinel > /data/sentinel.txt"})

	// 3. Lease vanishes from chain (auto-close / credit exhaustion) → orphan cleanup soft-deletes.
	mu.Lock()
	leaseVisible = false
	mu.Unlock()
	require.NoError(t, env.reconciler.RunOnce(ctx))

	retainedPath := filepath.Join(mountPath, retainedName(canonicalVolumeName(leaseUUID, manifest.DefaultServiceName, 0)))
	require.Eventually(t, func() bool {
		rec, _ := env.backend.retentionStore.Get(leaseUUID)
		if rec == nil {
			return false
		}
		_, statErr := os.Stat(retainedPath)
		return statErr == nil
	}, 60*time.Second, 500*time.Millisecond, "reconciler orphan cleanup must soft-delete (retained volume + record)")
	data, err := os.ReadFile(filepath.Join(retainedPath, "data", "sentinel.txt"))
	require.NoError(t, err)
	assert.Contains(t, string(data), "recon-restore-sentinel")

	// 3b. ENG-329 Part A on the REAL auto-close path: after the reconciler orphan
	// sweep soft-deletes (the actual ENG-329 trigger — lease vanished from chain),
	// GetProvision must surface the queryable retention status so the offline
	// tenant can self-serve. This is the key new behavior exercised end-to-end.
	rec0, err := env.backend.retentionStore.Get(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, rec0)
	info, err := env.backend.GetProvision(ctx, leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, info)
	assert.Equal(t, backend.ProvisionStatusRetained, info.Status, "auto-closed + retained lease must report retained via GetProvision")
	assert.False(t, info.RetainedUntil.IsZero(), "retained provision must carry a RetainedUntil deadline")
	assert.Equal(t, rec0.CreatedAt.Add(env.backend.cfg.RetentionMaxAge), info.RetainedUntil, "RetainedUntil = CreatedAt + RetentionMaxAge")
	assert.Equal(t, tenant, info.Tenant, "Tenant must be populated for the closed-lease authz fallback")
	require.NotEmpty(t, info.Items, "retained provision must carry the restore-shape Items")

	// 4. Restore into a NEW lease.
	newLease := newIntegrationLeaseUUID()
	restoreCallbacks := newIntegrationCallbackAuthority(t, env.callbackURL)
	require.NoError(t, env.backend.Restore(ctx, backend.RestoreRequest{
		LeaseUUID: newLease, FromLeaseUUID: leaseUUID, Tenant: tenant, ProviderUUID: env.providerUUID,
		Items:                []backend.LeaseItem{{SKU: sku, Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		CallbackURL:          restoreCallbacks.operationURL,
		LifecycleCallbackURL: restoreCallbacks.lifecycleURL,
	}))
	require.Equal(t, backend.CallbackStatusSuccess,
		waitForCallbackDelivery(t, env.callbackCh, newLease, 3*time.Minute).Status)
	// Callback settlement and source-finalizer deletion are separate durable
	// crash boundaries. Drive the level-triggered retention reconciler explicitly
	// instead of waiting for the production sweep cadence.
	finalizeRestoreRetentionForTest(t, env.backend, env.backend.retentionStore, leaseUUID)

	// 5. Sentinel survived; record gone.
	out := execInContainer(t, getContainerID(t, newLease), []string{"cat", "/data/sentinel.txt"})
	assert.Contains(t, out, "recon-restore-sentinel")
	rec, err := env.backend.retentionStore.Get(leaseUUID)
	require.NoError(t, err)
	assert.Nil(t, rec)

	env.backend.cfg.RetainOnClose = false
	require.NoError(t, env.backend.Deprovision(ctx, newLease))
}

// testManagerSetup wires a real provisioner.Manager to the real docker backend +
// mock chain, for exercising the event-driven close/expiry deprovision path.
//
// NOTE: this deliberately collapses the production transport seam — prod wires the
// Manager to backends via backend.NewHTTPClient against the docker-backend process
// (cmd/providerd/main.go), whereas this injects the in-process *docker.Backend
// directly. It exercises the provisioner→backend LIFECYCLE logic, not the HTTP wire
// or HMAC auth (those are unit-tested separately).
func testManagerSetup(t *testing.T, mockChain *chaintest.MockClient, extraCfg ...func(*Config)) (*provisioner.Manager, *Backend, <-chan backend.CallbackPayload, string) {
	t.Helper()
	callbackServer, callbackCh := startCallbackServer(t)
	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
		for _, fn := range extraCfg {
			fn(cfg)
		}
	})
	router, err := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{
			Backend: integrationIdentityBackend{Backend: b}, IsDefault: true,
		}},
	})
	require.NoError(t, err)
	placementStore, err := placementstore.NewStoreForProvider(
		filepath.Join(t.TempDir(), "manager-placements.db"), testProviderUUID,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, placementStore.Close()) })
	configureBackendTopologyForTest(t, placementStore, b)
	_, callbackProofConsumer := hmacauth.NewCallbackProofBoundary()
	mgr, err := provisioner.NewManager(provisioner.ManagerConfig{
		ProviderUUID:          testProviderUUID,
		PlacementStore:        placementStore,
		CallbackProofConsumer: callbackProofConsumer,
	}, router, mockChain)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); _ = mgr.Close() })
	go func() { _ = mgr.Start(ctx) }()
	select {
	case <-mgr.Running():
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for manager to start")
	}
	return mgr, b, callbackCh, callbackServer.URL
}

func TestIntegration_Manager_CloseEvent_RealSoftDelete(t *testing.T) {
	mountPath := setupBtrfsLoopback(t)
	leaseUUID := newIntegrationLeaseUUID()
	tenant := "test-tenant"
	const sku = "docker-small"
	const providerUUID = testProviderUUID

	mockChain := &chaintest.MockClient{
		GetLeaseFunc: func(_ context.Context, uuid string) (*billingtypes.Lease, error) {
			return chaintest.NewMockLeaseWithSKU(uuid, tenant, providerUUID, billingtypes.LEASE_STATE_ACTIVE, sku), nil
		},
	}
	mgr, b, callbackCh, callbackURL := testManagerSetup(t, mockChain, func(cfg *Config) {
		cfg.VolumeDataPath = mountPath // REQUIRED: empty would zero docker-small's DiskMB → stateless → nothing to retain
		cfg.VolumeMountPath = mountPath
		cfg.VolumeFilesystem = "btrfs"
		cfg.RetainOnClose = true
		cfg.RetentionDBPath = filepath.Join(t.TempDir(), "retention.db")
		cfg.RetentionMaxAge = 0
		cfg.RetentionReapInterval = 0
	})
	ctx := context.Background()

	// Provision directly on the backend. This exercises the SKU-hint / default-route
	// close path (orchestrator Case 2): the lease is NOT Manager-tracked and has no
	// placement, mirroring a post-restart close of an already-active lease.
	payload, err := json.Marshal(manifest.Manifest{Image: "redis:7", Command: []string{"sleep", "3600"}})
	require.NoError(t, err)
	callbacks := newIntegrationCallbackAuthority(t, callbackURL)
	require.NoError(t, b.Provision(ctx, backend.ProvisionRequest{
		LeaseUUID: leaseUUID, Tenant: tenant, ProviderUUID: providerUUID,
		Items: []backend.LeaseItem{{SKU: sku, Quantity: 1}}, CallbackURL: callbacks.operationURL,
		LifecycleCallbackURL: callbacks.lifecycleURL, Payload: payload,
	}))
	require.Equal(t, backend.CallbackStatusSuccess, waitForCallback(t, callbackCh, leaseUUID, 3*time.Minute).Status)

	execInContainer(t, getContainerID(t, leaseUUID), []string{"sh", "-c", "echo mgr-event-sentinel > /data/sentinel.txt"})

	// Drive the on-chain close as an event through the real Manager. LeaseExpired is
	// intentionally not a second btrfs run — HandleLeaseExpired calls processLeaseClose
	// verbatim (handler_set.go:126-136), so this also covers the expiry route.
	require.NoError(t, mgr.PublishLeaseEvent(chain.LeaseEvent{
		Type: chain.LeaseClosed, LeaseUUID: leaseUUID, ProviderUUID: providerUUID, Tenant: tenant,
	}))

	// The KEY assertion: the event route reached a REAL soft-delete (mock backends can't).
	retainedPath := filepath.Join(mountPath, retainedName(canonicalVolumeName(leaseUUID, manifest.DefaultServiceName, 0)))
	require.Eventually(t, func() bool {
		rec, _ := b.retentionStore.Get(leaseUUID)
		if rec == nil {
			return false
		}
		_, statErr := os.Stat(retainedPath)
		return statErr == nil
	}, 60*time.Second, 500*time.Millisecond, "LeaseClosed event must reach a real soft-delete (retained volume + record)")
	data, err := os.ReadFile(filepath.Join(retainedPath, "data", "sentinel.txt"))
	require.NoError(t, err)
	assert.Contains(t, string(data), "mgr-event-sentinel")
	// Retained volume + bbolt DB live under the loopback mount / t.TempDir → torn down automatically.
}

// TestIntegration_Reconciler_UpdatedPayload_ReprovisionsUpdatedImage is the
// mainnet scenario from ENG-619, end to end against real Docker: a tenant
// updates a running deployment, the host later loses the container, and the
// reconciler brings the lease back.
//
// Before the fix the lease came back on its as-created image, because /update
// wrote only to the backend and the reconciler replays from the payload store.
// The naive fix was worse: the updated payload no longer matches the lease's
// immutable on-chain MetaHash, so the reprovision path deleted it as corrupt
// and then closed the ACTIVE lease on-chain. This test pins both — the lease
// must come back on the UPDATED image, and the payload must survive.
func TestIntegration_Reconciler_UpdatedPayload_ReprovisionsUpdatedImage(t *testing.T) {
	const (
		originalImage = "busybox:latest"
		updatedImage  = "alpine:latest"
	)
	leaseUUID := newIntegrationLeaseUUID()
	tenant := "test-tenant"
	sku := "docker-micro"

	payloadV1, err := json.Marshal(manifest.Manifest{
		Image:   originalImage,
		Command: []string{"sleep", "3600"},
	})
	require.NoError(t, err)
	payloadV2, err := json.Marshal(manifest.Manifest{
		Image:   updatedImage,
		Command: []string{"sleep", "3600"},
	})
	require.NoError(t, err)

	// MetaHash names the CREATE-time manifest and never changes: the chain has
	// no message to update it (that is ENG-643).
	metaHash := sha256.Sum256(payloadV1)

	var mu sync.Mutex
	leaseState := billingtypes.LEASE_STATE_PENDING

	mockChain := &chaintest.MockClient{
		GetPendingLeasesFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_PENDING {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, metaHash[:])
				lease.State = billingtypes.LEASE_STATE_PENDING
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			mu.Lock()
			defer mu.Unlock()
			if leaseState == billingtypes.LEASE_STATE_ACTIVE {
				lease := makeLease(leaseUUID, tenant, providerUUID, sku, 1, metaHash[:])
				lease.State = billingtypes.LEASE_STATE_ACTIVE
				return []billingtypes.Lease{lease}, nil
			}
			return nil, nil
		},
		AcknowledgeLeasesFunc: func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
			return 1, []string{"txhash1"}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			t.Errorf("leases %v were closed on-chain (%q) — an updated payload must never be read as corruption", leaseUUIDs, reason)
			return 0, nil, nil
		},
	}

	var lostResponseBackend *loseAcceptedUpdateResponseBackend
	env := testReconcilerSetupWithRuntime(
		t,
		mockChain,
		func(base integrationIdentityBackend) backend.Backend {
			lostResponseBackend = &loseAcceptedUpdateResponseBackend{
				integrationIdentityBackend: base,
				loseNext:                   true,
			}
			return lostResponseBackend
		},
	)
	ctx := context.Background()

	// --- create path: store the original payload and provision ---
	require.True(t, env.tracker.store.Store(leaseUUID, payloadV1))
	require.NoError(t, env.reconciler.RunOnce(ctx))

	initialDelivery := waitForCallbackDelivery(t, env.callbackCh, leaseUUID, 2*time.Minute)
	require.Equal(t, backend.CallbackStatusSuccess, initialDelivery.Status)
	require.True(t, env.tracker.finishProvisionCallback(initialDelivery))

	containers := inspectProvisionContainers(t, leaseUUID)
	require.NotEmpty(t, containers)
	requireProvisionContainerImage(t, containers[0], originalImage)

	mu.Lock()
	leaseState = billingtypes.LEASE_STATE_ACTIVE
	mu.Unlock()
	require.NoError(t, env.reconciler.RunOnce(ctx))

	// --- tenant /update through the provider application boundary ---
	lifecycleAuthorization := env.placementStore.CurrentLifecycle(leaseUUID)
	require.Equal(t, placement.LifecycleVerdictAuthorized, lifecycleAuthorization.Verdict())
	callbackRoutes, err := placement.NewCallbackRouteFactory(env.callbackURL)
	require.NoError(t, err)
	maintenanceOperationID, err := operation.ParseID(lifecycleAuthorization.ID().String())
	require.NoError(t, err)
	maintenanceCallbacks, err := callbackRoutes.ForOperation(maintenanceOperationID)
	require.NoError(t, err)
	maintenanceCallbackURL := maintenanceCallbacks.LifecycleURL()
	maintenanceID := newTestMaintenanceID(t)
	maintenanceService := newIntegrationMaintenanceService(t, env, env.execution)
	maintenanceCommand := providermaintenance.Command{
		ID: maintenanceID, LeaseUUID: leaseUUID, Tenant: tenant,
		Kind: providermaintenance.KindUpdate, Payload: payloadV2,
	}

	firstUpdate := maintenanceService.Execute(ctx, maintenanceCommand)
	require.Equal(t, providermaintenance.OutcomeServiceUnavailable, firstUpdate.Outcome(),
		"a lost response is ambiguous even though docker-backend accepted the command")
	assert.Equal(t, 1, lostResponseBackend.updateCount())
	storedPayload, err := env.tracker.store.Get(leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, payloadV1, storedPayload,
		"the provider must not overwrite desired state until acceptance is recoverable")
	providerRecord, found, err := env.placementStore.LookupMaintenanceCommand(
		leaseUUID, maintenanceID,
	)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, placement.MaintenanceOutcomePending, providerRecord.Outcome())
	select {
	case cb := <-env.callbackCh:
		require.Equal(t, leaseUUID, cb.LeaseUUID)
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status, "update should succeed")
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for update success callback")
	}

	waitForProvisionStatus(t, env.backend, leaseUUID, backend.ProvisionStatusReady, 60*time.Second)
	requestAuthority, err := env.backend.maintenanceSettlement.NewMaintenanceRequestAuthority(
		maintenanceID, shared.MaintenanceIntentUpdate, leaseUUID,
		maintenanceCallbackURL, payloadV2,
	)
	require.NoError(t, err)
	disposition, err := env.backend.maintenanceSettlement.ProbeMaintenanceIntent(requestAuthority)
	require.NoError(t, err)
	assert.Equal(t, shared.MaintenanceIntentAdmissionCompleted, disposition,
		"the real backend WAL must recognize the completed exact command")
	releases, err := env.backend.releaseStore.List(leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, 1, countMaintenanceRelease(releases, maintenanceID),
		"one accepted command must append exactly one replacement generation")

	// Simulate a provider process restart: close and independently reopen both
	// durable stores, rebuild callback routes from configuration, and construct a
	// fresh volatile registry and proof boundary. Constructor rehydration must
	// recover the Pending command without any authority from the old process.
	backendStorageID := env.tracker.backendStorageID
	require.NoError(t, env.placementStore.Close())
	require.NoError(t, env.tracker.store.Close())
	restartedRoutes, err := placement.NewCallbackRouteFactory(env.callbackURL)
	require.NoError(t, err)
	reopenedPlacement, err := placement.OpenStore(
		env.placementPath,
		env.providerUUID,
		placement.WithCallbackRouteFactory(restartedRoutes),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedPlacement.Close() })
	reopenedPayload, err := payload.NewStore(payload.StoreConfig{DBPath: env.payloadPath})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopenedPayload.Close() })
	restartedExecution := newIntegrationExecutionAfterProviderRestart(
		t, env, reopenedPlacement, env.router,
	)
	restartedVerifier, restartedConsumer := hmacauth.NewCallbackProofBoundary()
	restartedCallbacks, err := restartedExecution.AuthenticatedCallbackCoordinator(restartedConsumer)
	require.NoError(t, err)
	restartedTracker := &testReconcilerTracker{
		callbacks:        restartedCallbacks,
		callbackVerifier: restartedVerifier,
		callbackPath:     configuredIntegrationCallbackPath(t, env.callbackURL),
		backendStorageID: backendStorageID,
		store:            reopenedPayload,
	}
	env.placementStore = reopenedPlacement
	env.execution = restartedExecution
	env.tracker = restartedTracker
	env.reconciler = newIntegrationReconcilerAfterProviderRestart(
		t, restartedExecution, restartedTracker,
	)
	recoveredMaintenance := newIntegrationMaintenanceService(t, env, restartedExecution)
	require.NoError(t, recoveredMaintenance.RecoverPending(ctx))
	assert.Equal(t, 2, lostResponseBackend.updateCount(),
		"restart recovery must replay the exact command through the backend boundary")
	storedPayload, err = env.tracker.store.Get(leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, payloadV2, storedPayload,
		"exact backend replay must authorize the provider payload commit")
	providerRecord, found, err = reopenedPlacement.LookupMaintenanceCommand(
		leaseUUID, maintenanceID,
	)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, placement.MaintenanceOutcomeAccepted, providerRecord.Outcome())
	releases, err = env.backend.releaseStore.List(leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, 1, countMaintenanceRelease(releases, maintenanceID),
		"recovery must consume the backend receipt without another logical mutation")
	assert.Equal(t, providermaintenance.OutcomeAccepted,
		recoveredMaintenance.Execute(ctx, maintenanceCommand).Outcome(),
		"the API-facing exact retry must replay the provider's terminal receipt")
	releases, err = env.backend.releaseStore.List(leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, 1, countMaintenanceRelease(releases, maintenanceID))
	select {
	case callback := <-env.callbackCh:
		t.Fatalf("terminal provider replay emitted a backend callback without a worker: %+v", callback)
	case <-time.After(250 * time.Millisecond):
	}

	containers = inspectProvisionContainers(t, leaseUUID)
	require.NotEmpty(t, containers)
	requireProvisionContainerImage(t, containers[0], updatedImage)

	// --- the reboot: lose the container, let the reconciler bring it back ---
	killContainer(t, containers[0].ID)
	waitForProvisionStatus(t, env.backend, leaseUUID, backend.ProvisionStatusFailed, 30*time.Second)

	select {
	case cb := <-env.callbackCh:
		assert.Equal(t, backend.CallbackStatusFailed, cb.Status)
	case <-time.After(10 * time.Second):
		// recoverState may have already fired it; continue
	}

	require.NoError(t, env.reconciler.RunOnce(ctx))

	callbackDeadline := time.Now().Add(2 * time.Minute)
	var reprovisionDelivery integrationCallbackDelivery
	for {
		reprovisionDelivery = waitForCallbackDelivery(
			t, env.callbackCh, leaseUUID, time.Until(callbackDeadline),
		)
		if reprovisionDelivery.Status == backend.CallbackStatusSuccess {
			break
		}
		t.Logf("skipping pre-reprovision lifecycle callback with status %s",
			reprovisionDelivery.Status)
	}
	require.True(t, env.tracker.finishProvisionCallback(reprovisionDelivery),
		"the fresh process must settle the exact signed re-provision operation")
	confirmed := env.placementStore.Lookup(leaseUUID)
	require.Equal(t, placement.StateConfirmed, confirmed.State())
	require.Equal(t, env.backend.cfg.Name, confirmed.Backend)
	require.False(t, confirmed.AttemptOperationID().Valid(),
		"successful settlement must consume the durable operation attempt")

	// The assertion this whole ticket exists for.
	containers = inspectProvisionContainers(t, leaseUUID)
	require.NotEmpty(t, containers)
	requireProvisionContainerImage(t, containers[0], updatedImage)

	// And the payload must still be there: deleting it as "corrupt" is what
	// would close the lease on the following sweep.
	has, err := env.tracker.store.Has(leaseUUID)
	require.NoError(t, err)
	assert.True(t, has, "the updated payload must survive hash verification")
}
