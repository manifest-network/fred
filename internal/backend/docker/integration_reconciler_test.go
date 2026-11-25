//go:build integration

package docker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
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
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner"
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
	result, err := t.applyCallback(delivery)
	return err == nil && result.OperationOutcome() == placement.CallbackOperationSucceeded
}

// applyCallback preserves the captured request through either operation
// settlement or exact maintenance completion, including across provider reopen.
func (t *testReconcilerTracker) applyCallback(delivery integrationCallbackDelivery) (placement.CallbackResult, error) {
	if t == nil || t.callbacks == nil || !t.callbackVerifier.Valid() ||
		t.callbackPath == "" ||
		delivery.LeaseUUID == "" || delivery.method == "" || delivery.requestURI == "" ||
		len(delivery.body) == 0 || delivery.signature == "" {
		return placement.CallbackResult{}, errors.New("invalid captured integration callback")
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
		return placement.CallbackResult{}, err
	}
	return t.callbacks.Apply(context.Background(), proof)
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
	require.Equal(t, placement.BackendInventoryAuthoritative, disposition.Disposition())
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
	}, placement.NoBillingOnlySKUs())
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
			Interval:    1 * time.Hour,
			Coordinator: reconciliation,
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
