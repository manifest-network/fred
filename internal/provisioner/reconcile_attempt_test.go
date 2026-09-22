package provisioner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

const (
	redeliveryTarget = "11111111-1111-4111-8111-111111111111"
	redeliverySource = "22222222-2222-4222-8222-222222222222"
	redeliveryTenant = "tenant-redelivery"
)

func redeliveryLease(uuid string, state billingtypes.LeaseState) billingtypes.Lease {
	return billingtypes.Lease{
		Uuid:         uuid,
		ProviderUuid: placementstore.ProviderUUID,
		Tenant:       redeliveryTenant,
		State:        state,
		Items: []billingtypes.LeaseItem{{
			SkuUuid: "sku-redelivery", Quantity: 2, ServiceName: "app",
		}},
	}
}

func redeliveryRequestSnapshot(
	t testing.TB,
	store *placement.Store,
	lease billingtypes.Lease,
) placement.BackendRequestSnapshot {
	t.Helper()
	snapshot, err := store.MintBackendRequestSnapshot(lease.Tenant, ExtractLeaseItems(&lease))
	require.NoError(t, err)
	return snapshot
}

var redeliveryExecutions sync.Map
var redeliveryCoordinators sync.Map

func persistAmbiguousProvisionOnStore(
	t testing.TB,
	store *placement.Store,
	registry *operation.Registry,
	router *backend.Router,
	backendClient *mockReconcilerBackend,
	target billingtypes.Lease,
	payloadBytes []byte,
) operation.OperationID {
	t.Helper()
	var coordinator *placement.OperationCoordinator
	if existing, ok := redeliveryCoordinators.Load(store); ok {
		coordinator = existing.(*placement.OperationCoordinator)
	} else {
		coordinator = bindTestOperationCoordinator(t, store)
		redeliveryCoordinators.Store(store, coordinator)
		t.Cleanup(func() { redeliveryCoordinators.Delete(store) })
	}
	var execution *placement.ExecutionCoordinator
	if existing, ok := redeliveryExecutions.Load(store); ok {
		execution = existing.(*placement.ExecutionCoordinator)
	} else {
		execution = bindTestBackendRuntime(t, coordinator, router)
	}
	var err error
	chain := redeliveryChain([]billingtypes.Lease{target}, nil)
	bindTestReconciliationCoordinator(t, store, execution, chain, nil, nil)
	backendClient.mu.Lock()
	previousErr := backendClient.provisionErr
	backendClient.provisionErr = errors.New("connection reset after provision dispatch")
	backendClient.mu.Unlock()
	if target.State == billingtypes.LEASE_STATE_PENDING {
		provision, bindErr := execution.ProvisionCoordinatorWithPayloads(nil, nil)
		require.NoError(t, bindErr)
		var request placement.ProvisionEventRequest
		if payloadBytes == nil {
			request, err = placement.NewProvisionEventRequest(target.Uuid, target.Tenant)
		} else {
			request, err = placement.NewPayloadProvisionEventRequest(
				target.Uuid, target.Tenant,
				func() ([]byte, error) { return append([]byte(nil), payloadBytes...), nil },
			)
		}
		require.NoError(t, err)
		result := provision.ExecuteCurrentLease(context.Background(), request)
		require.Error(t, result.Err())
	} else {
		reconciliation := testReconciliationCoordinator(t, store)
		sweep, sweepErr := reconciliation.BeginSweep()
		require.NoError(t, sweepErr)
		defer sweep.End()
		backendNames, namesErr := reconciliation.BackendNames()
		require.NoError(t, namesErr)
		for _, backendName := range backendNames {
			storageID := testBackendStorageID(backendName)
			disposition := collectTestBackendInventory(
				t, reconciliation, sweep, backendName, storageID, nil, nil,
			)
			require.Equal(t, placement.BackendInventoryAuthoritative, disposition)
		}
		require.NoError(t, sweep.SealInventory())
		projected, projectErr := sweep.Project(placement.ReconciliationProjection{})
		require.NoError(t, projectErr)
		action, disposition, observeErr := projected.ObserveLiveAction(
			context.Background(), target.Uuid,
		)
		require.NoError(t, observeErr)
		require.Equal(t, placement.ReconciliationObservationReady, disposition)
		fingerprint := placement.PayloadFingerprint{}
		if payloadBytes != nil {
			digest := sha256.Sum256(payloadBytes)
			fingerprint, err = placement.NewPayloadFingerprint(digest[:])
			require.NoError(t, err)
		}
		result := reconciliation.Provision(context.Background(), action, payloadBytes, fingerprint)
		require.NoError(t, result.Err())
		require.Error(t, result.Dispatch().CallErr())
	}
	record := store.Lookup(target.Uuid)
	if record.Backend == "" {
		require.Equal(t, placement.StateAttempting, record.State())
	} else {
		require.Equal(t, placement.StateConfirmed, record.State())
	}
	require.NotEmpty(t, record.Attempt)
	operationID := record.AttemptOperationID()
	require.True(t, operationID.Valid())
	backendClient.mu.Lock()
	backendClient.provisionErr = previousErr
	backendClient.provisionCalls = nil
	backendClient.mu.Unlock()
	redeliveryExecutions.Store(store, execution)
	t.Cleanup(func() { redeliveryExecutions.Delete(store) })
	return operationID
}

// persistAmbiguousProvisionOnHTTPStore creates a pending write-ahead attempt
// through the exact identity-bound transport. The server first returns an
// indeterminate 500, then is left ready for the caller to select the recovery
// response. This keeps refusal tests on the only boundary authorized to mint
// causal evidence.
func persistAmbiguousProvisionOnHTTPStore(
	t testing.TB,
	store *placement.Store,
	router *backend.Router,
	server *fakeBackendServer,
	target billingtypes.Lease,
) operation.OperationID {
	t.Helper()
	server.setProvisionResponse(http.StatusInternalServerError, `{"error":"uncertain"}`)
	coordinator := bindTestOperationCoordinator(t, store)
	redeliveryCoordinators.Store(store, coordinator)
	t.Cleanup(func() { redeliveryCoordinators.Delete(store) })
	execution := bindTestBackendRuntime(t, coordinator, router)
	chain := redeliveryChain([]billingtypes.Lease{target}, nil)
	bindTestReconciliationCoordinator(t, store, execution, chain, nil, nil)
	provision, err := execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)
	request, err := placement.NewProvisionEventRequest(target.Uuid, target.Tenant)
	require.NoError(t, err)
	result := provision.ExecuteCurrentLease(context.Background(), request)
	require.Error(t, result.Err())
	record := store.Lookup(target.Uuid)
	require.Equal(t, placement.StateAttempting, record.State())
	operationID := record.AttemptOperationID()
	require.True(t, operationID.Valid())
	server.mu.Lock()
	server.provisionCalls = make(map[string]int)
	server.provisionRequests = make(map[string]backend.ProvisionRequest)
	server.mu.Unlock()
	redeliveryExecutions.Store(store, execution)
	t.Cleanup(func() { redeliveryExecutions.Delete(store) })
	return operationID
}

func redeliveryChain(
	live []billingtypes.Lease,
	extra map[string]billingtypes.Lease,
) *chaintest.MockClient {
	byID := make(map[string]billingtypes.Lease, len(live)+len(extra))
	var pending, active []billingtypes.Lease
	for _, lease := range live {
		byID[lease.Uuid] = lease
		switch lease.State {
		case billingtypes.LEASE_STATE_PENDING:
			pending = append(pending, lease)
		case billingtypes.LEASE_STATE_ACTIVE:
			active = append(active, lease)
		}
	}
	for uuid, lease := range extra {
		byID[uuid] = lease
	}
	return &chaintest.MockClient{
		GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return append([]billingtypes.Lease(nil), pending...), nil
		},
		GetActiveLeasesByProviderFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			return append([]billingtypes.Lease(nil), active...), nil
		},
		GetLeaseFunc: func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			lease, ok := byID[leaseUUID]
			if !ok {
				return nil, nil
			}
			copy := lease
			return &copy, nil
		},
	}
}

func redeliveryRouter(
	t *testing.T,
	backends ...backend.Backend,
) *backend.Router {
	t.Helper()
	entries := make([]backend.BackendEntry, len(backends))
	for index, backendClient := range backends {
		entries[index] = backend.BackendEntry{
			Backend: backendClient,
			Match:   backend.MatchCriteria{SKUs: []string{"sku-redelivery"}},
		}
	}
	entries[len(entries)-1].IsDefault = true
	router, err := backend.NewRouter(backend.RouterConfig{Backends: entries})
	require.NoError(t, err)
	return router
}

func redeliveryReconciler(
	t *testing.T,
	store *placement.Store,
	router *backend.Router,
	chainClient ReconcilerChainClient,
	registry *operation.Registry,
) *Reconciler {
	return redeliveryReconcilerWithPayload(t, store, router, chainClient, registry, nil)
}

func redeliveryReconcilerWithPayload(
	t *testing.T,
	store *placement.Store,
	router *backend.Router,
	chainClient ReconcilerChainClient,
	registry *operation.Registry,
	payloadStore *payload.Store,
) *Reconciler {
	t.Helper()
	if bound, ok := redeliveryExecutions.Load(store); ok {
		execution := bound.(*placement.ExecutionCoordinator)
		coordinator := bindTestReconciliationCoordinator(
			t, store, execution, chainClient, payloadStore, nil,
		)
		reconciler, err := NewReconciler(
			ReconcilerConfig{Coordinator: coordinator}, newMockInFlightTracker(payloadStore),
		)
		require.NoError(t, err)
		return reconciler
	}
	coordinator := bindTestOperationCoordinator(t, store)
	redeliveryCoordinators.Store(store, coordinator)
	t.Cleanup(func() { redeliveryCoordinators.Delete(store) })
	execution := bindTestBackendRuntime(t, coordinator, router)
	reconciliation := bindTestReconciliationCoordinator(
		t, store, execution, chainClient, payloadStore, nil,
	)
	reconciler, err := NewReconciler(
		ReconcilerConfig{Coordinator: reconciliation}, newMockInFlightTracker(payloadStore),
	)
	require.NoError(t, err)
	return reconciler
}

func redeliveryRuntime(t testing.TB, store *placement.Store) operation.RuntimeController {
	t.Helper()
	value, ok := redeliveryCoordinators.Load(store)
	require.True(t, ok, "redelivery fixture must retain its bound operation coordinator")
	return value.(*placement.OperationCoordinator).RuntimeController()
}

func openRedeliveryStore(t testing.TB, dbPath string) *placement.Store {
	t.Helper()
	routes, err := placement.NewCallbackRouteFactory("https://provider.example/callbacks/provision")
	require.NoError(t, err)
	store, err := placementstore.NewStore(dbPath, placement.WithCallbackRouteFactory(routes))
	require.NoError(t, err)
	return store
}

// persistAmbiguousRestoreAttempt creates the write-ahead fact only through the
// production restore application, then closes the process-local authority.
// Reopening the Store is mandatory: recovery must not inherit the initiating
// Registry, backend runtime, or chain reader.
func persistAmbiguousRestoreAttempt(
	t testing.TB,
	dbPath string,
	router *backend.Router,
	backendClient *mockReconcilerBackend,
	target billingtypes.Lease,
) operation.OperationID {
	t.Helper()
	store := openRedeliveryStore(t, dbPath)
	_, execution := seedTestTypedConfirmedPlacementsWithExecution(
		t, store, router, map[string]string{
			redeliverySource: backendClient.Name(),
		},
	)
	source := redeliveryLease(redeliverySource, billingtypes.LEASE_STATE_CLOSED)
	chain := redeliveryChain([]billingtypes.Lease{target}, map[string]billingtypes.Lease{
		redeliverySource: source,
	})
	setTestProviderControlPlane(t, execution, chain, nil)
	restore, err := execution.RestoreCoordinator(nil)
	require.NoError(t, err)
	request, err := placement.NewRestoreApplicationRequest(
		target.Uuid, target.Tenant, redeliverySource,
	)
	require.NoError(t, err)
	backendClient.restoreErr = errors.New("connection reset after restore dispatch")
	result := restore.ExecuteApplication(context.Background(), request)
	require.Error(t, result.CallErr())
	record := store.Lookup(target.Uuid)
	require.Equal(t, placement.StateAttempting, record.State())
	operationID := record.AttemptOperationID()
	require.True(t, operationID.Valid())
	backendClient.restoreErr = nil
	backendClient.restoreCalls = nil
	require.NoError(t, store.Close())
	return operationID
}

func persistAmbiguousProvisionAttempt(
	t testing.TB,
	dbPath string,
	router *backend.Router,
	backendClient *mockReconcilerBackend,
	lease billingtypes.Lease,
) {
	t.Helper()
	store := openRedeliveryStore(t, dbPath)
	_ = persistAmbiguousProvisionOnStore(
		t, store, operation.NewRegistry(), router, backendClient, lease, nil,
	)
	require.True(t, store.Lookup(lease.Uuid).AttemptOperationID().Valid())
	require.NoError(t, store.Close())
}

func requireCallbackOperationID(t *testing.T, callbackURL string) operation.OperationID {
	t.Helper()
	parsed, err := url.Parse(callbackURL)
	require.NoError(t, err)
	id, present, err := operation.ParseQuery(parsed.Query())
	require.NoError(t, err)
	require.True(t, present)
	return id
}

func TestReconciler_RedeliversNeverReceivedProvisionWithExactIdentity(t *testing.T) {
	backendA := &mockReconcilerBackend{
		name:         "backend-a",
		provisionErr: errors.New("connection reset before request reached backend"),
	}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	registry := operation.NewRegistry()
	operationID := persistAmbiguousProvisionOnStore(
		t, store, registry, router, backendA,
		redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING), nil,
	)
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain([]billingtypes.Lease{
			redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING),
		}, nil),
		registry,
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	require.Len(t, backendA.provisionCalls, 1)
	firstRequest := backendA.provisionCalls[0]
	assert.Equal(t, operationID, requireCallbackOperationID(t, firstRequest.CallbackURL))
	assert.Equal(t, placement.StateAttempting, store.Lookup(redeliveryTarget).State())
	assert.Zero(t, redeliveryRuntime(t, store).Count())

	backendA.provisionErr = nil
	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	require.Len(t, backendA.provisionCalls, 2)
	secondRequest := backendA.provisionCalls[1]
	assert.Equal(t, firstRequest, secondRequest,
		"every retry must reproduce the complete request, not merely its backend")
	assert.Equal(t, operationID, requireCallbackOperationID(t, secondRequest.CallbackURL))
	assert.Equal(t, placement.StateConfirmed, store.Lookup(redeliveryTarget).State())
	assert.True(t, redeliveryRuntime(t, store).Contains(redeliveryTarget))

	coordinatorValue, ok := redeliveryCoordinators.Load(store)
	require.True(t, ok)
	applied, err := confirmPlacementOperationForTest(
		coordinatorValue.(*placement.OperationCoordinator),
		redeliveryTarget, backendA.Name(), operationID,
	)
	require.NoError(t, err)
	require.True(t, applied,
		"the backend callback must settle through the recovered registry record")
}

func TestReconciler_RedeliversProvisionAfterProviderRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	store1 := openRedeliveryStore(t, dbPath)
	armTestPlacementTopology(t, store1, backendTopologyNames(router))
	originalTarget := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
	originalTarget.Items[0].CustomDomain = "original.example.test"
	operationID := persistAmbiguousProvisionOnStore(
		t, store1, operation.NewRegistry(), router, backendA, originalTarget, nil,
	)
	require.NoError(t, store1.Close())

	store2 := openRedeliveryStore(t, dbPath)
	t.Cleanup(func() { require.NoError(t, store2.Close()) })
	reopened := store2.Lookup(redeliveryTarget)
	require.True(t, reopened.AttemptMetadata().Valid())
	assert.Equal(t, operation.KindProvision, reopened.AttemptMetadata().Kind())
	assert.Equal(t, operationID, reopened.AttemptMetadata().OperationID())
	durableCallbacks := reopened.AttemptMetadata().CallbackPair()
	require.NotEmpty(t, durableCallbacks.OperationURL())
	assert.Contains(t, durableCallbacks.OperationURL(), "provider.example",
		"the Store-bound callback origin must survive restart unchanged")
	registry := operation.NewRegistry()
	mutatedTarget := originalTarget
	mutatedTarget.Items = append([]billingtypes.LeaseItem(nil), originalTarget.Items...)
	mutatedTarget.Items[0].CustomDomain = "changed-after-dispatch.example.test"
	reconciler := redeliveryReconciler(
		t, store2, router,
		redeliveryChain([]billingtypes.Lease{mutatedTarget}, nil),
		registry,
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	require.Len(t, backendA.provisionCalls, 1)
	assert.Equal(t, operationID,
		requireCallbackOperationID(t, backendA.provisionCalls[0].CallbackURL))
	assert.Equal(t, durableCallbacks.OperationURL(), backendA.provisionCalls[0].CallbackURL)
	assert.Equal(t, durableCallbacks.LifecycleURL(), backendA.provisionCalls[0].LifecycleCallbackURL)
	require.Len(t, backendA.provisionCalls[0].Items, 1)
	assert.Equal(t, "original.example.test", backendA.provisionCalls[0].Items[0].CustomDomain,
		"mutable chain fields cannot rewrite an already-authorized exact request")
	assert.True(t, redeliveryRuntime(t, store2).Contains(redeliveryTarget))
}

func TestFleet_ProviderRestartRedeliversPersistedAttemptAcrossHTTPBoundary(t *testing.T) {
	fleet := newFleet(t, fleetOptions{backendCount: 2})
	induceFleetAmbiguousProvision(t, fleet, redeliveryTarget, "sku-redelivery", 1)
	leaseUUID := fleetLeaseUUID(redeliveryTarget)
	operationID := fleet.placement.Lookup(leaseUUID).AttemptOperationID()
	require.True(t, operationID.Valid())
	require.Zero(t, fleet.coordinator.RuntimeController().Count(),
		"the simulated process exits in the write-ahead window before registration")

	require.NoError(t, fleet.placement.Close())
	callbackRoutes, err := placement.NewCallbackRouteFactory("http://fred.invalid")
	require.NoError(t, err)
	reopened, err := placementstore.NewStore(
		fleet.placementPath,
		placement.WithClock(func() time.Time { return time.Now().Add(-fleet.placementAge) }),
		placement.WithCallbackRouteFactory(callbackRoutes),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	fleet.placement = reopened
	fleet.tracker = &fleetReconcilerTracker{
		testOperationRegistry: newTestOperationRegistry(),
		payloads:              fleet.payloads,
	}
	fleet.coordinator, err = fleet.tracker.bindPlacementStore(reopened)
	require.NoError(t, err)
	fleet.execution = bindTestBackendRuntime(t, fleet.coordinator, fleet.router)
	fleet.reconcilerCfg.Coordinator = bindTestReconciliationCoordinator(
		t, fleet.placement, fleet.execution, fleet.chain, fleet.payloads, nil,
	)
	fleet.reconciler, err = newTestReconciler(t,
		fleet.reconcilerCfg,
		fleet.chain,
		fleet.acknowledger,
		fleet.router,
		fleet.tracker,
		fleet.placement,
	)
	require.NoError(t, err)

	require.NoError(t, fleet.sweep())
	request, exists := fleet.backendAt(1).provisionRequest(leaseUUID)
	require.True(t, exists)
	assert.Equal(t, operationID, requireCallbackOperationID(t, request.CallbackURL))
	assert.Equal(t, leaseUUID, request.LeaseUUID)
	assert.Equal(t, fleet.providerUUID, request.ProviderUUID)
	assert.Zero(t, fleet.backendAt(2).provisionCount(leaseUUID),
		"redelivery cannot load-balance to a different HTTP backend")
	assert.True(t, fleet.coordinator.RuntimeController().Contains(leaseUUID))
	assert.Equal(t, placement.StateConfirmed, fleet.placement.Lookup(leaseUUID).State())
}

func TestReconciler_AcceptedOldAttemptGetsFreshCallbackTimeoutWindow(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	callbackRoutes, err := placement.NewCallbackRouteFactory("https://provider.example/callbacks/provision")
	require.NoError(t, err)
	store, err := placementstore.NewStore(
		filepath.Join(t.TempDir(), "placements.db"),
		placement.WithClock(func() time.Time { return time.Now().Add(-24 * time.Hour) }),
		placement.WithCallbackRouteFactory(callbackRoutes),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	registry := operation.NewRegistry()
	_ = persistAmbiguousProvisionOnStore(
		t, store, registry, router, backendA,
		redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING), nil,
	)
	oldAttempt := store.Lookup(redeliveryTarget)
	require.Less(t, oldAttempt.SetAt, time.Now().Add(-23*time.Hour))

	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain([]billingtypes.Lease{
			redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING),
		}, nil),
		registry,
	)
	require.NoError(t, reconciler.ReconcileAll(t.Context()))

	coordinatorValue, ok := redeliveryCoordinators.Load(store)
	require.True(t, ok)
	coordinator := coordinatorValue.(*placement.OperationCoordinator)
	rejecter := &mockRejecter{rejectFn: func(
		context.Context, []string, string,
	) (uint64, []string, error) {
		t.Fatal("freshly recovered operation must not be rejected as timed out")
		return 0, nil, nil
	}}
	checker, err := NewTimeoutChecker(TimeoutCheckerConfig{
		Coordinator: func() *placement.TimeoutCoordinator {
			executionValue, exists := redeliveryExecutions.Load(store)
			require.True(t, exists)
			execution := executionValue.(*placement.ExecutionCoordinator)
			setTestProviderRejecter(t, execution, rejecter)
			timeouts, bindErr := execution.TimeoutCoordinator()
			require.NoError(t, bindErr)
			return timeouts
		}(),
		Timeout:       time.Hour,
		CheckInterval: time.Hour,
	})
	require.NoError(t, err)
	checker.CheckOnce(t.Context())
	assert.True(t, coordinator.RuntimeController().Contains(redeliveryTarget))
}

func TestReconciler_RedeliveryRebuildsExactPersistedPayload(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store := openRedeliveryStore(t, dbPath)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(t.TempDir(), "payloads.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, payloadStore.Close()) })
	payloadBytes := []byte(`{"services":{"app":{"image":"example.invalid/app:1"}}}`)
	require.True(t, payloadStore.Store(redeliveryTarget, payloadBytes))
	payloadHash := sha256.Sum256(payloadBytes)
	registry := operation.NewRegistry()
	target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
	target.MetaHash = payloadHash[:]
	operationID := persistAmbiguousProvisionOnStore(
		t, store, registry, router, backendA, target, payloadBytes,
	)
	reconciler := redeliveryReconcilerWithPayload(
		t, store, router,
		redeliveryChain([]billingtypes.Lease{target}, nil),
		registry, payloadStore,
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	require.Len(t, backendA.provisionCalls, 1)
	request := backendA.provisionCalls[0]
	assert.Equal(t, payloadBytes, request.Payload)
	assert.Equal(t, hex.EncodeToString(payloadHash[:]), request.PayloadHash)
	assert.Equal(t, operationID, requireCallbackOperationID(t, request.CallbackURL))
}

func TestReconciler_RedeliveryRejectsPayloadDifferentFromDurableAttempt(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	authorizedPayload := []byte(`{"version":"attempt-authorized"}`)
	authorizedHash := sha256.Sum256(authorizedPayload)
	registry := operation.NewRegistry()
	target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
	target.MetaHash = authorizedHash[:]
	_ = persistAmbiguousProvisionOnStore(
		t, store, registry, router, backendA, target, authorizedPayload,
	)
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(t.TempDir(), "payloads.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, payloadStore.Close()) })
	require.True(t, payloadStore.Store(redeliveryTarget, []byte(`{"version":"persisted"}`)))
	reconciler := redeliveryReconcilerWithPayload(
		t, store, router,
		redeliveryChain([]billingtypes.Lease{target}, nil),
		registry, payloadStore,
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.Empty(t, backendA.provisionCalls,
		"payload-store state cannot override the exact durable attempt fingerprint")
	assert.Equal(t, placement.StateAttempting, store.Lookup(redeliveryTarget).State())
}

func TestReconciler_RedeliveryUsesUpdatedActivePayloadFingerprint(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store := openRedeliveryStore(t, dbPath)
	registry := operation.NewRegistry()
	seededCoordinator, seededExecution := seedTestTypedConfirmedPlacementsWithExecution(t, store, router, map[string]string{
		redeliveryTarget: backendA.Name(),
	})
	redeliveryCoordinators.Store(store, seededCoordinator)
	t.Cleanup(func() { redeliveryCoordinators.Delete(store) })
	redeliveryExecutions.Store(store, seededExecution)
	t.Cleanup(func() { redeliveryExecutions.Delete(store) })
	payloadStore, err := payload.NewStore(payload.StoreConfig{
		DBPath: filepath.Join(t.TempDir(), "payloads.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, payloadStore.Close()) })
	updatedPayload := []byte(`{"version":"updated-after-create"}`)
	require.NoError(t, payloadStore.Put(redeliveryTarget, updatedPayload))
	updatedHash := sha256.Sum256(updatedPayload)
	target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_ACTIVE)
	createPayloadHash := sha256.Sum256([]byte(`{"version":"original-create"}`))
	target.MetaHash = createPayloadHash[:]
	_ = persistAmbiguousProvisionOnStore(
		t, store, registry, router, backendA, target, updatedPayload,
	)
	require.NoError(t, store.Close())
	store = openRedeliveryStore(t, dbPath)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	reconciler := redeliveryReconcilerWithPayload(
		t, store, router, redeliveryChain([]billingtypes.Lease{target}, nil),
		operation.NewRegistry(), payloadStore,
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	require.Len(t, backendA.provisionCalls, 1)
	request := backendA.provisionCalls[0]
	assert.Equal(t, updatedPayload, request.Payload)
	assert.Equal(t, hex.EncodeToString(updatedHash[:]), request.PayloadHash)
	assert.NotEqual(t, hex.EncodeToString(createPayloadHash[:]), request.PayloadHash)
}

func TestReconciler_RedeliveryRefusalAndLocalTransportGatesSettleConservatively(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		status    int
		body      string
		wantState placement.State
	}{
		{
			name: "validation refusal", err: backend.ErrValidation,
			status: http.StatusBadRequest, body: `{"error":"bad request"}`,
			wantState: placement.StateAbsent,
		},
		{
			name: "coded capacity refusal", err: backend.ErrCapacityRefused,
			status:    http.StatusServiceUnavailable,
			body:      `{"error":"full","code":"insufficient_resources"}`,
			wantState: placement.StateAbsent,
		},
		{name: "circuit open", err: backend.ErrCircuitOpen, wantState: placement.StateAttempting},
		{name: "identity unbound", err: backend.ErrBackendStorageIdentityUnbound, wantState: placement.StateAttempting},
		{name: "identity mismatch", err: backend.ErrBackendStorageIdentityMismatch, wantState: placement.StateAttempting},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newTestPlacementAuthority(t)
			registry := operation.NewRegistry()
			target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
			var router *backend.Router
			if test.status != 0 {
				server, backendClient := provisionResponseBackendForTest(
					t, "backend-a", http.StatusInternalServerError, `{"error":"uncertain"}`,
				)
				router = redeliveryRouter(t, backendClient)
				armTestPlacementTopology(t, store, backendTopologyNames(router))
				_ = persistAmbiguousProvisionOnHTTPStore(t, store, router, server, target)
				server.setProvisionResponse(test.status, test.body)
			} else {
				backendBase := &mockReconcilerBackend{name: "backend-a"}
				router = redeliveryRouter(t, backendBase)
				armTestPlacementTopology(t, store, backendTopologyNames(router))
				_ = persistAmbiguousProvisionOnStore(
					t, store, registry, router, backendBase, target, nil,
				)
				backendBase.provisionErr = test.err
			}
			reconciler := redeliveryReconciler(
				t, store, router,
				redeliveryChain([]billingtypes.Lease{target}, nil),
				registry,
			)

			require.NoError(t, reconciler.ReconcileAll(t.Context()))
			assert.Equal(t, test.wantState, store.Lookup(redeliveryTarget).State())
		})
	}
}

func TestReconciler_RedeliveryRequiresExactTargetProvider(t *testing.T) {
	for _, test := range []struct {
		name         string
		providerUUID string
	}{
		{name: "different provider", providerUUID: "a6d6790d-d04b-48bd-ad91-675cb7a4b2ed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			backendA := &mockReconcilerBackend{name: "backend-a"}
			router := redeliveryRouter(t, backendA)
			store := newTestPlacementAuthority(t)
			armTestPlacementTopology(t, store, backendTopologyNames(router))
			registry := operation.NewRegistry()
			original := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
			_ = persistAmbiguousProvisionOnStore(
				t, store, registry, router, backendA, original, nil,
			)
			target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
			target.ProviderUuid = test.providerUUID
			reconciler := redeliveryReconciler(
				t, store, router, redeliveryChain(
					[]billingtypes.Lease{original},
					map[string]billingtypes.Lease{redeliveryTarget: target},
				), registry,
			)
			require.NoError(t, reconciler.ReconcileAll(t.Context()))
			assert.Empty(t, backendA.provisionCalls)
			assert.Equal(t, placement.StateAttempting,
				store.Lookup(redeliveryTarget).State())
			assert.Zero(t, redeliveryRuntime(t, store).Count())
		})
	}
}

func TestReconciler_RedeliversRestoreWithDurableSourceAndRecoveredKind(t *testing.T) {
	backendA := &mockReconcilerBackend{
		name:          "backend-a",
		restoreAccept: true,
		retentions:    []backend.RetainedLease{{LeaseUUID: redeliverySource}},
	}
	router := redeliveryRouter(t, backendA)
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	originalTarget := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
	originalTarget.Items[0].CustomDomain = "restore-original.example.test"
	operationID := persistAmbiguousRestoreAttempt(t, dbPath, router, backendA, originalTarget)

	store := openRedeliveryStore(t, dbPath)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	metadata := store.Lookup(redeliveryTarget).AttemptMetadata()
	require.True(t, metadata.Valid())
	assert.Equal(t, operation.KindRestore, metadata.Kind())
	assert.Equal(t, redeliverySource, metadata.RestoreSourceLeaseUUID())

	target := originalTarget
	target.Items = append([]billingtypes.LeaseItem(nil), originalTarget.Items...)
	target.Items[0].CustomDomain = "restore-changed-after-dispatch.example.test"
	source := redeliveryLease(redeliverySource, billingtypes.LEASE_STATE_CLOSED)
	registry := operation.NewRegistry()
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain([]billingtypes.Lease{target}, map[string]billingtypes.Lease{
			redeliverySource: source,
		}),
		registry,
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	require.Len(t, backendA.restoreCalls, 1)
	request := backendA.restoreCalls[0]
	assert.Equal(t, redeliveryTarget, request.LeaseUUID)
	assert.Equal(t, redeliverySource, request.FromLeaseUUID)
	require.Len(t, request.Items, 1)
	assert.Equal(t, "restore-original.example.test", request.Items[0].CustomDomain)
	assert.Equal(t, operationID, requireCallbackOperationID(t, request.CallbackURL))
	assert.True(t, redeliveryRuntime(t, store).Contains(redeliveryTarget))
	assert.Equal(t, placement.StateConfirmed, store.Lookup(redeliveryTarget).State())
}

func TestReconciler_PreservesRestoreAttemptWhenSourceIsUnknownToChain(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a", restoreAccept: true}
	router := redeliveryRouter(t, backendA)
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
	_ = persistAmbiguousRestoreAttempt(t, dbPath, router, backendA, target)
	store := openRedeliveryStore(t, dbPath)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	registry := operation.NewRegistry()
	reconciler := redeliveryReconciler(
		t, store, router, redeliveryChain([]billingtypes.Lease{target}, nil), registry,
	)
	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.Empty(t, backendA.restoreCalls)
	assert.Equal(t, placement.StateAttempting, store.Lookup(redeliveryTarget).State())
	assert.Zero(t, redeliveryRuntime(t, store).Count())
}

func TestReconciler_RestoreRedeliveryRejectsChangedSourceAuthority(t *testing.T) {
	tests := []struct {
		name                      string
		sourceHasPendingOperation bool
		chainSource               billingtypes.Lease
		wantErrorMatch            string
	}{
		{
			name: "wrong tenant",
			chainSource: func() billingtypes.Lease {
				lease := redeliveryLease(redeliverySource, billingtypes.LEASE_STATE_CLOSED)
				lease.Tenant = "another-tenant"
				return lease
			}(),
			wantErrorMatch: "tenant and provider",
		},
		{
			name: "wrong provider",
			chainSource: func() billingtypes.Lease {
				lease := redeliveryLease(redeliverySource, billingtypes.LEASE_STATE_CLOSED)
				lease.ProviderUuid = "b40f1a67-e1aa-4711-84d1-398dbca14c2f"
				return lease
			}(),
			wantErrorMatch: "tenant and provider",
		},
		{
			name:                      "source has pending operation",
			sourceHasPendingOperation: true,
			chainSource:               redeliveryLease(redeliverySource, billingtypes.LEASE_STATE_CLOSED),
			wantErrorMatch:            "source placement",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backendA := &mockReconcilerBackend{name: "backend-a", restoreAccept: true}
			router := redeliveryRouter(t, backendA)
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
			_ = persistAmbiguousRestoreAttempt(t, dbPath, router, backendA, target)
			if test.sourceHasPendingOperation {
				source := redeliveryLease(redeliverySource, billingtypes.LEASE_STATE_ACTIVE)
				persistAmbiguousProvisionAttempt(t, dbPath, router, backendA, source)
			}
			store := openRedeliveryStore(t, dbPath)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			registry := operation.NewRegistry()
			reconciler := redeliveryReconciler(
				t, store, router,
				redeliveryChain([]billingtypes.Lease{target}, map[string]billingtypes.Lease{
					redeliverySource: test.chainSource,
				}),
				registry,
			)
			require.NoError(t, reconciler.ReconcileAll(t.Context()))
			assert.Empty(t, backendA.restoreCalls)
			assert.Equal(t, placement.StateAttempting,
				store.Lookup(redeliveryTarget).State())
			assert.Zero(t, redeliveryRuntime(t, store).Count())
		})
	}
}

func TestReconciler_MalformedAttemptMetadataIsQuarantinedWithoutDispatch(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	storeBeforeCorruption := openRedeliveryStore(t, dbPath)
	armTestPlacementTopology(t, storeBeforeCorruption, backendTopologyNames(router))
	_ = persistAmbiguousProvisionOnStore(
		t, storeBeforeCorruption, operation.NewRegistry(), router, backendA,
		redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING), nil,
	)
	require.NoError(t, storeBeforeCorruption.Close())

	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("placements")).Put(
			[]byte(redeliveryTarget),
			[]byte(`{"schema":1,"attempt":"backend-a","set_at":"2026-08-25T15:00:00Z","revision":99}`),
		)
	}))
	require.NoError(t, db.Close())
	routes, err := placement.NewCallbackRouteFactory("https://provider.example/callbacks/provision")
	require.NoError(t, err)
	store, err := placement.OpenStore(
		dbPath, placementstore.ProviderUUID, placement.WithCallbackRouteFactory(routes),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.Equal(t, placement.StateUnusable, store.Lookup(redeliveryTarget).State())

	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain([]billingtypes.Lease{
			redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING),
		}, nil),
		operation.NewRegistry(),
	)
	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.Empty(t, backendA.provisionCalls)
	assert.Empty(t, backendA.restoreCalls)
	assert.Equal(t, placement.StateUnusable, store.Lookup(redeliveryTarget).State())
}

func TestReconciler_CallbackContendingWithRedeliveryRetriesThenSettles(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	backendA := &mockReconcilerBackend{name: "backend-a"}
	backendA.onProvision = func() {
		entered <- struct{}{}
		<-release
	}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	backendA.onProvision = nil
	operationID := persistAmbiguousProvisionOnStore(
		t, store, operation.NewRegistry(), router, backendA,
		redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING), nil,
	)
	backendA.onProvision = func() { entered <- struct{}{}; <-release }
	target := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING)
	registry := operation.NewRegistry()
	reconciler := redeliveryReconciler(
		t, store, router, redeliveryChain([]billingtypes.Lease{target}, nil), registry,
	)

	done := make(chan error, 1)
	go func() { done <- reconciler.ReconcileAll(context.Background()) }()
	<-entered
	coordinatorValue, ok := redeliveryCoordinators.Load(store)
	require.True(t, ok)
	coordinator := coordinatorValue.(*placement.OperationCoordinator)
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinator,
		Acknowledger: callbackAcknowledgerFunc(func(
			context.Context, string,
		) (bool, string, error) {
			return true, "tx-ack", nil
		}),
	})
	require.NoError(t, err)
	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   redeliveryTarget,
		Status:      backend.CallbackStatusSuccess,
		OperationID: operationID.String(),
	})
	firstErr := service.HandleCallback(t.Context(), command)
	require.ErrorIs(t, firstErr, errCallbackRecoveryLeaseBusy,
		"inline callback contention must remain retryable")

	close(release)
	released = true
	require.NoError(t, <-done)
	require.True(t, coordinator.RuntimeController().Contains(redeliveryTarget))
	require.NoError(t, service.HandleCallback(t.Context(), command))
	assert.False(t, coordinator.RuntimeController().Contains(redeliveryTarget))
	assert.Equal(t, placement.StateConfirmed, store.Lookup(redeliveryTarget).State())
}

func TestReconciler_DownAttemptBackendDoesNotPauseHealthyBackendAdmission(t *testing.T) {
	backendA := &mockReconcilerBackend{
		name:         "backend-a",
		listErr:      errors.New("backend-a inventory outage"),
		provisionErr: backend.ErrCircuitOpen,
	}
	backendB := &mockReconcilerBackend{name: "backend-b"}
	router := redeliveryRouter(t, backendA, backendB)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	_ = persistAmbiguousProvisionOnStore(
		t, store, operation.NewRegistry(), router, backendA,
		redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING), nil,
	)
	healthyLease := redeliveryLease("33333333-3333-4333-8333-333333333333", billingtypes.LEASE_STATE_PENDING)
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain([]billingtypes.Lease{
			redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING),
			healthyLease,
		}, nil),
		operation.NewRegistry(),
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.Equal(t, placement.StateAttempting, store.Lookup(redeliveryTarget).State())
	require.Len(t, backendA.provisionCalls, 1)
	require.Len(t, backendB.provisionCalls, 1)
	assert.Equal(t, healthyLease.Uuid, backendB.provisionCalls[0].LeaseUUID)
	assert.NotEqual(t, healthyLease.Uuid, backendA.provisionCalls[0].LeaseUUID,
		"the exact attempt backend can never receive another lease through fallback")
}

func TestReconciler_TerminalAttemptConvergesByExactTeardown(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store := openRedeliveryStore(t, dbPath)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	operationID := persistAmbiguousProvisionOnStore(
		t, store, operation.NewRegistry(), router, backendA,
		redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING), nil,
	)
	terminal := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_CLOSED)
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain(nil, map[string]billingtypes.Lease{redeliveryTarget: terminal}),
		operation.NewRegistry(),
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.Equal(t, []string{redeliveryTarget}, backendA.deprovisionCalls)
	record := store.Lookup(redeliveryTarget)
	assert.Equal(t, placement.StateConfirmed, record.State())
	assert.Equal(t, backendA.Name(), record.Backend,
		"successful teardown conservatively preserves possible retained-data affinity")
	assert.Empty(t, record.Attempt)
	assert.Equal(t, operationID.String(), store.CurrentLifecycle(redeliveryTarget).ID().String(),
		"queued operation/lifecycle callbacks retain their exact promoted generation")
}

func TestReconciler_NotFoundAttemptRemainsPreservedAfterProviderRestart(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store1 := openRedeliveryStore(t, dbPath)
	armTestPlacementTopology(t, store1, backendTopologyNames(router))
	persistAmbiguousProvisionOnStore(
		t, store1, operation.NewRegistry(), router, backendA,
		redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING), nil,
	)
	require.NoError(t, store1.Close())

	store2 := openRedeliveryStore(t, dbPath)
	t.Cleanup(func() { require.NoError(t, store2.Close()) })
	chainClient := redeliveryChain(nil, nil)
	chainClient.GetLeaseFunc = func(context.Context, string) (*billingtypes.Lease, error) {
		return nil, billingtypes.ErrLeaseNotFound
	}
	reconciler := redeliveryReconciler(
		t, store2, router, chainClient, operation.NewRegistry(),
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.Empty(t, backendA.deprovisionCalls)
	record := store2.Lookup(redeliveryTarget)
	assert.Equal(t, placement.StateAttempting, record.State())
	assert.Equal(t, backendA.Name(), record.Attempt)
	assert.True(t, record.AttemptOperationID().Valid())
	assert.False(t, store2.CurrentLifecycle(redeliveryTarget).ID().Valid(),
		"no-record cannot mint or retire lifecycle authority")
	assert.Zero(t, redeliveryRuntime(t, store2).Count())
}

func TestReconciler_TerminalAttemptAmbiguousTeardownRetainsAuthority(t *testing.T) {
	backendA := &mockReconcilerBackend{
		name:           "backend-a",
		deprovisionErr: errors.New("connection reset after teardown request"),
	}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	operationID := persistAmbiguousProvisionOnStore(
		t, store, operation.NewRegistry(), router, backendA,
		redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING), nil,
	)
	terminal := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_REJECTED)
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain(nil, map[string]billingtypes.Lease{redeliveryTarget: terminal}),
		operation.NewRegistry(),
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.Equal(t, []string{redeliveryTarget}, backendA.deprovisionCalls)
	record := store.Lookup(redeliveryTarget)
	assert.Equal(t, placement.StateAttempting, record.State())
	assert.Equal(t, operationID, record.AttemptMetadata().OperationID())
}

func TestReconciler_TerminalAttemptRequiresExactTargetSnapshot(t *testing.T) {
	for index, test := range []struct {
		name   string
		mutate func(*billingtypes.Lease)
	}{
		{
			name: "tenant changed",
			mutate: func(lease *billingtypes.Lease) {
				lease.Tenant = "another-tenant"
			},
		},
		{
			name: "provider changed",
			mutate: func(lease *billingtypes.Lease) {
				lease.ProviderUuid = "a6d6790d-d04b-48bd-ad91-675cb7a4b2ed"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			backendA := &mockReconcilerBackend{name: "backend-a"}
			router := redeliveryRouter(t, backendA)
			store := newTestPlacementAuthority(t)
			armTestPlacementTopology(t, store, backendTopologyNames(router))
			_ = index
			_ = persistAmbiguousProvisionOnStore(
				t, store, operation.NewRegistry(), router, backendA,
				redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING), nil,
			)
			terminal := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_CLOSED)
			test.mutate(&terminal)
			reconciler := redeliveryReconciler(
				t, store, router,
				redeliveryChain(nil, map[string]billingtypes.Lease{redeliveryTarget: terminal}),
				operation.NewRegistry(),
			)

			require.NoError(t, reconciler.ReconcileAll(t.Context()))
			assert.Empty(t, backendA.deprovisionCalls,
				"mismatched chain identity cannot authorize destructive teardown")
			assert.Empty(t, backendA.provisionCalls)
			assert.Equal(t, placement.StateAttempting, store.Lookup(redeliveryTarget).State())
		})
	}
}

func TestReconciler_DownTerminalAttemptBackendDoesNotBlockHealthyAdmission(t *testing.T) {
	backendA := &mockReconcilerBackend{
		name:           "backend-a",
		listErr:        errors.New("backend-a inventory outage"),
		deprovisionErr: backend.ErrCircuitOpen,
	}
	backendB := &mockReconcilerBackend{name: "backend-b"}
	router := redeliveryRouter(t, backendA, backendB)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	_ = persistAmbiguousProvisionOnStore(
		t, store, operation.NewRegistry(), router, backendA,
		redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING), nil,
	)
	healthy := redeliveryLease("44444444-4444-4444-8444-444444444444", billingtypes.LEASE_STATE_PENDING)
	terminal := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_EXPIRED)
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain(
			[]billingtypes.Lease{healthy},
			map[string]billingtypes.Lease{redeliveryTarget: terminal},
		),
		operation.NewRegistry(),
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	require.Len(t, backendB.provisionCalls, 1)
	assert.Equal(t, healthy.Uuid, backendB.provisionCalls[0].LeaseUUID)
	assert.Equal(t, []string{redeliveryTarget}, backendA.deprovisionCalls)
	assert.Equal(t, placement.StateAttempting, store.Lookup(redeliveryTarget).State())
}

func TestReconciler_TerminalAttemptPreservesConfirmedOwnerAffinity(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a"}
	router := redeliveryRouter(t, backendA)
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store := openRedeliveryStore(t, dbPath)
	registry := operation.NewRegistry()
	seededCoordinator, seededExecution := seedTestTypedConfirmedPlacementsWithExecution(t, store, router, map[string]string{
		redeliveryTarget: backendA.Name(),
	})
	redeliveryCoordinators.Store(store, seededCoordinator)
	t.Cleanup(func() { redeliveryCoordinators.Delete(store) })
	redeliveryExecutions.Store(store, seededExecution)
	t.Cleanup(func() { redeliveryExecutions.Delete(store) })
	operationID := persistAmbiguousProvisionOnStore(
		t, store, registry, router, backendA,
		redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_ACTIVE), nil,
	)
	require.NoError(t, store.Close())
	store = openRedeliveryStore(t, dbPath)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	terminal := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_CLOSED)
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain(nil, map[string]billingtypes.Lease{redeliveryTarget: terminal}),
		operation.NewRegistry(),
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	record := store.Lookup(redeliveryTarget)
	assert.Equal(t, placement.StateConfirmed, record.State())
	assert.Equal(t, backendA.Name(), record.Backend)
	assert.Empty(t, record.Attempt)
	assert.Equal(t, operationID.String(), store.CurrentLifecycle(redeliveryTarget).ID().String())
}

func TestReconciler_TerminalAttemptClaimsFenceCallbackAndInventory(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	backendA := &mockReconcilerBackend{
		name: "backend-a",
		onDeprovision: func() {
			entered <- struct{}{}
			<-release
		},
	}
	router := redeliveryRouter(t, backendA)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	operationID := persistAmbiguousProvisionOnStore(
		t, store, operation.NewRegistry(), router, backendA,
		redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING), nil,
	)
	terminal := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_CLOSED)
	chainClient := redeliveryChain(
		nil, map[string]billingtypes.Lease{redeliveryTarget: terminal},
	)
	registry := operation.NewRegistry()
	reconciler := redeliveryReconciler(t, store, router, chainClient, registry)

	done := make(chan error, 1)
	go func() { done <- reconciler.ReconcileAll(context.Background()) }()
	<-entered
	coordinatorValue, ok := redeliveryCoordinators.Load(store)
	require.True(t, ok)
	service, err := newCallbackServiceForTest(callbackServiceTestConfig{
		Coordinator: coordinatorValue.(*placement.OperationCoordinator),
		Chain:       chainClient,
	})
	require.NoError(t, err)
	command := callbackCommand(t, backend.CallbackPayload{
		LeaseUUID:   redeliveryTarget,
		Status:      backend.CallbackStatusFailed,
		Error:       "operation preempted by deprovision",
		OperationID: operationID.String(),
	})
	require.ErrorIs(t, service.HandleCallback(t.Context(), command), errCallbackRecoveryLeaseBusy)

	sweep, err := reconciler.coordinator.BeginSweep()
	require.NoError(t, err)
	for _, backendName := range backendTopologyNames(router) {
		disposition := collectTestBackendInventory(
			t, reconciler.coordinator, sweep, backendName,
			testBackendStorageID(backendName), []backend.ProvisionInfo{{
				LeaseUUID: redeliveryTarget, BackendName: backendA.Name(),
			}}, nil,
		)
		require.Equal(t, placement.BackendInventoryAuthoritative, disposition)
	}
	require.NoError(t, sweep.SealInventory())
	_, err = sweep.Project(placement.ReconciliationProjection{
		Placements: map[string]string{redeliveryTarget: backendA.Name()},
	})
	sweep.End()
	require.NoError(t, err)
	assert.Equal(t, placement.StateAttempting, store.Lookup(redeliveryTarget).State())

	close(release)
	released = true
	require.NoError(t, <-done)
	assert.Equal(t, placement.StateConfirmed, store.Lookup(redeliveryTarget).State())
	require.NoError(t, service.HandleCallback(t.Context(), command),
		"the bundled backend's queued exact failure must drain after claim release")
	assert.Equal(t, placement.StateConfirmed, store.Lookup(redeliveryTarget).State(),
		"terminal callback cannot discard conservative retained-data affinity")
}

func TestReconciler_ConflictedAttemptCannotAuthorizeBackendCall(t *testing.T) {
	backendA := &mockReconcilerBackend{name: "backend-a"}
	backendB := &mockReconcilerBackend{name: "backend-b"}
	router := redeliveryRouter(t, backendA, backendB)
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, backendTopologyNames(router))
	_ = persistAmbiguousProvisionOnStore(
		t, store, operation.NewRegistry(), router, backendA,
		redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_PENDING), nil,
	)
	projectTestPlacementInventory(t, testReconciliationCoordinator(t, store), backendTopologyNames(router), placement.ReconciliationProjection{
		Conflicts: map[string][]string{
			redeliveryTarget: {backendA.Name(), backendB.Name()},
		},
	})
	conflicted := store.Lookup(redeliveryTarget)
	require.Equal(t, placement.StateUnusable, conflicted.State())
	assert.False(t, conflicted.AttemptMetadata().Valid(),
		"unusable ownership must not expose an executable operation capability")
	terminal := redeliveryLease(redeliveryTarget, billingtypes.LEASE_STATE_CLOSED)
	reconciler := redeliveryReconciler(
		t, store, router,
		redeliveryChain(nil, map[string]billingtypes.Lease{redeliveryTarget: terminal}),
		operation.NewRegistry(),
	)

	require.NoError(t, reconciler.ReconcileAll(t.Context()))
	assert.Empty(t, backendA.provisionCalls)
	assert.Empty(t, backendB.provisionCalls)
	assert.Empty(t, backendA.deprovisionCalls)
	assert.Empty(t, backendB.deprovisionCalls)
	assert.Equal(t, placement.StateUnusable, store.Lookup(redeliveryTarget).State())
}
