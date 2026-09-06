package placement

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/inventory"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

const reconciliationSweepLease = "018f47a2-8b1c-7def-8123-456789abcdef"

const (
	fencedAvailabilityOwner   = "00000000-0000-4000-8000-000000000211"
	fencedAvailabilitySibling = "00000000-0000-4000-8000-000000000212"
)

type reconciliationSweepReader struct {
	mu           sync.Mutex
	lease        *billingtypes.Lease
	pending      []billingtypes.Lease
	active       []billingtypes.Lease
	err          error
	pendingReads int
	activeReads  int
	exactReads   int
	rejected     []string
	closed       []string
}

type fixedRouteExecutionRuntime struct {
	*executionTestRuntime
	routeName string
}

type unrecordedPositiveInventoryBackend struct {
	*executionTestBackend
	provisions []backend.ProvisionInfo
	retentions []backend.RetainedLease
	storageID  backendidentity.ID
}

func (client *unrecordedPositiveInventoryBackend) ListProvisionsWithIdentity(
	context.Context,
) ([]backend.ProvisionInfo, backendidentity.ID, error) {
	return append([]backend.ProvisionInfo(nil), client.provisions...), client.storageID, nil
}

func (client *unrecordedPositiveInventoryBackend) ListRetentionsWithIdentity(
	context.Context,
) ([]backend.RetainedLease, backendidentity.ID, error) {
	return append([]backend.RetainedLease(nil), client.retentions...), client.storageID, nil
}

type fencedAvailabilityFixture struct {
	store       *Store
	baseline    AdmissionBaseline
	lifecycleID lifecycle.ID
	coordinator *ReconciliationCoordinator
	backendB    *executionTestBackend
}

func newFencedAvailabilityFixture(t *testing.T) fencedAvailabilityFixture {
	t.Helper()
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	baseline := requireAdmissionBaseline(t, store, "backend-a", "backend-b", "backend-c")
	scope := requireAdmissionScope(t, store, baseline, "backend-a")
	seedID := requireOperationID(t, "2111")
	request, err := newBackendRequestSnapshot(
		"tenant-test", freshTestProviderUUID,
		[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
	)
	require.NoError(t, err)
	attempt, applied, err := store.beginNewAttempt(
		scope, fencedAvailabilityOwner, "backend-a", seedID, PayloadFingerprint{},
		request, testCallbackPair(seedID),
	)
	require.NoError(t, err)
	require.True(t, applied)
	confirmed, err := confirmAttemptForTest(store, attempt)
	require.NoError(t, err)
	require.True(t, confirmed)
	lifecycleID := store.CurrentLifecycle(fencedAvailabilityOwner).ID()
	require.True(t, lifecycleID.Valid())

	reader := &reconciliationSweepReader{lease: &billingtypes.Lease{
		Uuid: fencedAvailabilitySibling, Tenant: "tenant-test",
		ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_PENDING,
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
	}}
	backendA := &executionTestBackend{name: "backend-a"}
	backendB := &executionTestBackend{name: "backend-b"}
	backendC := &executionTestBackend{name: "backend-c"}
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	runtime := &fixedRouteExecutionRuntime{
		executionTestRuntime: newExecutionTestRuntime(backendA, backendB, backendC),
		routeName:            "backend-b",
	}
	execution := bindExecutionForTest(t, base, runtime)
	coordinator, err := reconciliationCoordinatorWithReaderForTest(t, execution, reader)
	require.NoError(t, err)
	return fencedAvailabilityFixture{
		store: store, baseline: baseline, lifecycleID: lifecycleID,
		coordinator: coordinator, backendB: backendB,
	}
}

func beginFencedAvailabilitySweep(
	t *testing.T,
	fixture fencedAvailabilityFixture,
) *ReconciliationSweep {
	t.Helper()
	sweep, err := fixture.coordinator.BeginSweep()
	require.NoError(t, err)
	operationID := requireOperationID(t, "2112")
	request, err := newBackendRequestSnapshot(
		"tenant-test", freshTestProviderUUID,
		[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
	)
	require.NoError(t, err)
	_, applied, err := fixture.store.beginOwnedAttempt(
		fixture.baseline, fixture.store.Lookup(fencedAvailabilityOwner).RecordRevision(),
		"backend-a", operationID, PayloadFingerprint{}, request, testCallbackPair(operationID),
	)
	require.NoError(t, err)
	require.True(t, applied)
	return sweep
}

func (runtime *fixedRouteExecutionRuntime) Route(string) backend.Backend {
	return runtime.GetBackendByName(runtime.routeName)
}

func (runtime *fixedRouteExecutionRuntime) RouteForProvision(
	context.Context,
	string,
	map[string]int,
) backend.Backend {
	return runtime.GetBackendByName(runtime.routeName)
}

func (runtime *fixedRouteExecutionRuntime) RouteForProvisionAmong(
	_ context.Context,
	_ string,
	eligible map[string]struct{},
	_ map[string]int,
) backend.Backend {
	if _, allowed := eligible[runtime.routeName]; !allowed {
		return nil
	}
	return runtime.GetBackendByName(runtime.routeName)
}

func (reader *reconciliationSweepReader) GetLease(
	_ context.Context,
	leaseUUID string,
) (*billingtypes.Lease, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.exactReads++
	if reader.err != nil || reader.lease == nil {
		return nil, reader.err
	}
	copy := cloneReconciliationLease(reader.lease)
	if copy.Uuid == "" {
		copy.Uuid = leaseUUID
	}
	return &copy, nil
}

func (reader *reconciliationSweepReader) GetPendingLeases(
	_ context.Context,
	_ string,
) ([]billingtypes.Lease, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.pendingReads++
	return append([]billingtypes.Lease(nil), reader.pending...), reader.err
}

func (reader *reconciliationSweepReader) GetActiveLeasesByProvider(
	_ context.Context,
	_ string,
) ([]billingtypes.Lease, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.activeReads++
	return append([]billingtypes.Lease(nil), reader.active...), reader.err
}

func TestReconciliationCoordinatorOwnsInventoryAndExactReads(t *testing.T) {
	lease := billingtypes.Lease{
		Uuid: reconciliationSweepLease, Tenant: "tenant-test",
		ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_PENDING,
	}
	reader := &reconciliationSweepReader{lease: &lease, pending: []billingtypes.Lease{lease}}
	_, _, coordinator := newReconciliationSweepFixture(
		t, &executionTestBackend{name: "backend-a"}, reader,
	)

	pending, active, err := coordinator.CollectChainInventory(t.Context(), time.Second)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Empty(t, active)
	sweep, projected := completeEmptyReconciliationSweep(t, coordinator)
	defer sweep.End()
	action, disposition, err := projected.ObserveLiveAction(t.Context(), reconciliationSweepLease)
	require.NoError(t, err)
	require.Equal(t, ReconciliationObservationReady, disposition)
	require.True(t, action.Valid())
	require.True(t, coordinator.ReleaseAction(action))

	reader.mu.Lock()
	defer reader.mu.Unlock()
	assert.Equal(t, 1, reader.pendingReads)
	assert.Equal(t, 1, reader.activeReads)
	assert.Equal(t, 1, reader.exactReads,
		"the chain capability that listed the lease must also authorize its exact read")
}

func TestReconciliationChainTerminalWritesRequireObservedState(t *testing.T) {
	pendingLease := billingtypes.Lease{
		Uuid: reconciliationSweepLease, Tenant: "tenant-test",
		ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_PENDING,
	}
	reader := &reconciliationSweepReader{lease: &pendingLease}
	_, _, coordinator := newReconciliationSweepFixture(
		t, &executionTestBackend{name: "backend-a"}, reader,
	)
	sweep, projected := completeEmptyReconciliationSweep(t, coordinator)
	defer sweep.End()
	action, disposition, err := projected.ObserveLiveAction(t.Context(), reconciliationSweepLease)
	require.NoError(t, err)
	require.Equal(t, ReconciliationObservationReady, disposition)
	defer coordinator.ReleaseAction(action)

	_, _, err = coordinator.CloseObserved(t.Context(), action, "wrong state")
	require.Error(t, err)
	_, _, err = coordinator.RejectObserved(t.Context(), action, "invalid request")
	require.NoError(t, err)

	reader.mu.Lock()
	defer reader.mu.Unlock()
	assert.Empty(t, reader.closed)
	assert.Equal(t, []string{reconciliationSweepLease}, reader.rejected)
}

func (reader *reconciliationSweepReader) RejectLeases(
	_ context.Context,
	leaseUUIDs []string,
	_ string,
) (uint64, []string, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.rejected = append(reader.rejected, leaseUUIDs...)
	return uint64(len(leaseUUIDs)), []string{"tx-reject"}, nil
}

func (reader *reconciliationSweepReader) CloseLeases(
	_ context.Context,
	leaseUUIDs []string,
	_ string,
) (uint64, []string, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.closed = append(reader.closed, leaseUUIDs...)
	return uint64(len(leaseUUIDs)), []string{"tx-close"}, nil
}

func newReconciliationSweepFixture(
	t *testing.T,
	client *executionTestBackend,
	reader *reconciliationSweepReader,
) (*Store, *operation.Registry, *ReconciliationCoordinator) {
	t.Helper()
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, configureBackendTopologyForTest(store, []string{"backend-a"}))
	registry := operation.NewRegistry()
	base, err := newOperationCoordinator(store, registry)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(client))
	coordinator, err := reconciliationCoordinatorWithReaderForTest(t, execution, reader)
	require.NoError(t, err)
	return store, registry, coordinator
}

func completeEmptyReconciliationSweep(
	t *testing.T,
	coordinator *ReconciliationCoordinator,
) (*ReconciliationSweep, *ProjectedReconciliationSweep) {
	t.Helper()
	sweep, err := coordinator.BeginSweep()
	require.NoError(t, err)
	storageID := testBackendStorageID("backend-a")
	require.NoError(t, sweep.RecordProvision("backend-a", storageID, nil))
	require.NoError(t, sweep.RecordRetention("backend-a", storageID, nil))
	require.NoError(t, sweep.SealInventory())
	projected, err := sweep.Project(ReconciliationProjection{})
	require.NoError(t, err)
	return sweep, projected
}

func TestEndSweepMarkerClearFailureIsObservableAndRetainsRecoveryRequirement(t *testing.T) {
	store, err := newStoreForTest(filepath.Join(t.TempDir(), "placements.db"))
	require.NoError(t, err)
	require.NoError(t, configureBackendTopologyForTest(store, []string{"backend-a"}))
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(
		t, base, newExecutionTestRuntime(&executionTestBackend{name: "backend-a"}),
	)
	coordinator, err := reconciliationCoordinatorWithReaderForTest(
		t, execution, &reconciliationSweepReader{},
	)
	require.NoError(t, err)
	sweep, err := coordinator.BeginSweep()
	require.NoError(t, err)
	pendingSweepID := store.pendingInventorySweepID
	require.NotZero(t, pendingSweepID)
	require.NoError(t, sweep.RecordProvision(
		"backend-a", testBackendStorageID("backend-a"), nil,
	))
	require.NoError(t, sweep.RecordRetention(
		"backend-a", testBackendStorageID("backend-a"), nil,
	))
	require.NoError(t, sweep.SealInventory())

	before := promtestutil.ToFloat64(metrics.PlacementWriteFailuresTotal)
	require.NoError(t, store.db.Close())
	sweep.End()
	assert.Equal(t, before+1, promtestutil.ToFloat64(metrics.PlacementWriteFailuresTotal),
		"failing to clear a safety marker must increment the placement write-failure signal")
	assert.Equal(t, pendingSweepID, store.pendingInventorySweepID)
	assert.True(t, store.inventoryRecoveryRequired,
		"an uncommitted marker clear must retain fail-closed recovery state")
	_ = store.Close()
}

func TestUnsealedSweepEndRetainsDurableRecoveryRequirement(t *testing.T) {
	const leaseUUID = "00000000-0000-4000-8000-000000000200"
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	routes := testCallbackRoutes(t)
	store, err := newStoreForTest(dbPath, WithCallbackRouteFactory(routes))
	require.NoError(t, err)
	require.NoError(t, configureBackendTopologyForTest(store, []string{"backend-a"}))
	storageID := testBackendStorageID("backend-a")
	client := &unrecordedPositiveInventoryBackend{
		executionTestBackend: &executionTestBackend{name: "backend-a"},
		provisions: []backend.ProvisionInfo{{
			LeaseUUID: leaseUUID, BackendName: "backend-a",
			ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
		}},
		storageID: storageID,
	}
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(client))
	coordinator, err := reconciliationCoordinatorWithReaderForTest(
		t, execution, &reconciliationSweepReader{},
	)
	require.NoError(t, err)

	baseline, err := coordinator.BeginSweep()
	require.NoError(t, err)
	require.NoError(t, baseline.RecordProvision("backend-a", storageID, nil))
	require.NoError(t, baseline.RecordRetention("backend-a", storageID, nil))
	require.NoError(t, baseline.SealInventory())
	_, err = baseline.Project(ReconciliationProjection{})
	require.NoError(t, err)
	baseline.End()
	require.True(t, store.CurrentAdmissionBaseline().Valid())

	abandoned, err := coordinator.BeginSweep()
	require.NoError(t, err)
	pendingSweepID := store.pendingInventorySweepID
	require.NotZero(t, pendingSweepID)
	raw, err := abandoned.CollectProvisionInventory(t.Context(), "backend-a")
	require.NoError(t, err)
	require.Len(t, raw.Provisions(), 1,
		"model an error after a positive backend response but before RecordProvision")

	// Without sealed session evidence, End cannot prove that the backend read
	// was empty. The pre-read durable marker must therefore survive restart.
	abandoned.End()
	assert.Equal(t, pendingSweepID, store.pendingInventorySweepID)
	assert.True(t, store.inventoryRecoveryRequired)
	assert.False(t, store.CurrentAdmissionBaseline().Valid())
	require.NoError(t, store.Close())

	reopened, err := OpenStore(
		dbPath, freshTestProviderUUID, WithCallbackRouteFactory(routes),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assert.Equal(t, pendingSweepID, reopened.pendingInventorySweepID)
	assert.True(t, reopened.inventoryRecoveryRequired)
	assert.False(t, reopened.CurrentAdmissionBaseline().Valid(),
		"an unsealed sweep cannot restore absence authority after restart")
}

func newCollectedInventorySweep(
	t *testing.T,
	provisions []backend.ProvisionInfo,
	retentions []backend.RetainedLease,
) (*Store, *ReconciliationCoordinator, *ReconciliationSweep) {
	t.Helper()
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, configureBackendTopologyForTest(store, []string{"backend-a"}))
	client := &unrecordedPositiveInventoryBackend{
		executionTestBackend: &executionTestBackend{name: "backend-a"},
		provisions:           provisions,
		retentions:           retentions,
		storageID:            testBackendStorageID("backend-a"),
	}
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(client))
	coordinator, err := reconciliationCoordinatorWithReaderForTest(
		t, execution, &reconciliationSweepReader{},
	)
	require.NoError(t, err)
	sweep, err := coordinator.BeginSweep()
	require.NoError(t, err)
	return store, coordinator, sweep
}

func TestCollectedInventoryReceiptsAreExactOneShotAndMandatory(t *testing.T) {
	const leaseUUID = "00000000-0000-4000-8000-000000000204"
	provision := backend.ProvisionInfo{
		LeaseUUID: leaseUUID, BackendName: "backend-a",
		ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
		Items:         []backend.LeaseItem{{SKU: "sku-test", Quantity: 1}},
		ServiceImages: map[string]string{"app": "example.invalid/app:1"},
		LifecycleGeneration: &backend.LifecycleGenerationObservation{
			Kind: backend.LifecycleGenerationTyped,
			ID:   requireLifecycleID(t, "2204").String(),
		},
	}

	t.Run("outstanding response prevents seal", func(t *testing.T) {
		_, _, sweep := newCollectedInventorySweep(t, []backend.ProvisionInfo{provision}, nil)
		defer sweep.End()
		_, err := sweep.CollectProvisionInventory(t.Context(), "backend-a")
		require.NoError(t, err)
		require.ErrorIs(t, sweep.SealInventory(), inventory.ErrInvalidSession)
	})

	t.Run("canonical response is isolated and consumed once", func(t *testing.T) {
		_, _, sweep := newCollectedInventorySweep(t, []backend.ProvisionInfo{provision}, nil)
		defer sweep.End()
		provisions, err := sweep.CollectProvisionInventory(t.Context(), "backend-a")
		require.NoError(t, err)
		retentions, err := sweep.CollectRetentionInventory(t.Context(), "backend-a")
		require.NoError(t, err)
		copyOfProvisions := provisions
		escaped := provisions.Provisions()
		escaped[0].Items[0].SKU = "mutated"
		escaped[0].ServiceImages["app"] = "mutated"
		escaped[0].LifecycleGeneration.ID = requireLifecycleID(t, "2299").String()
		canonical := provisions.Provisions()[0]
		assert.Equal(t, "sku-test", canonical.Items[0].SKU)
		assert.Equal(t, "example.invalid/app:1", canonical.ServiceImages["app"])
		assert.Equal(t, provision.LifecycleGeneration.ID, canonical.LifecycleGeneration.ID)

		disposition, err := sweep.RecordBackendInventory(provisions, retentions)
		require.NoError(t, err)
		require.Equal(t, BackendInventoryAuthoritative, disposition)
		disposition, err = sweep.RecordBackendInventory(copyOfProvisions, retentions)
		require.ErrorIs(t, err, inventory.ErrInvalidSession)
		require.Equal(t, BackendInventoryInvalid, disposition)
		require.NoError(t, sweep.SealInventory())
	})

	t.Run("cross-sweep copy is rejected", func(t *testing.T) {
		_, coordinator, first := newCollectedInventorySweep(t, []backend.ProvisionInfo{provision}, nil)
		defer first.End()
		provisions, err := first.CollectProvisionInventory(t.Context(), "backend-a")
		require.NoError(t, err)
		retentions, err := first.CollectRetentionInventory(t.Context(), "backend-a")
		require.NoError(t, err)
		second, err := coordinator.BeginSweep()
		require.NoError(t, err)
		defer second.End()
		disposition, err := second.RecordBackendInventory(provisions, retentions)
		require.ErrorIs(t, err, inventory.ErrInvalidSession)
		require.Equal(t, BackendInventoryInvalid, disposition)
	})

	t.Run("same-sweep cross-backend receipts cannot be spliced", func(t *testing.T) {
		store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
		require.NoError(t, configureBackendTopologyForTest(
			store, []string{"backend-a", "backend-b"},
		))
		clientA := &unrecordedPositiveInventoryBackend{
			executionTestBackend: &executionTestBackend{name: "backend-a"},
			storageID:            testBackendStorageID("backend-a"),
		}
		clientB := &unrecordedPositiveInventoryBackend{
			executionTestBackend: &executionTestBackend{name: "backend-b"},
			storageID:            testBackendStorageID("backend-b"),
		}
		base, err := store.BindOperationCoordinator(nil)
		require.NoError(t, err)
		execution := bindExecutionForTest(
			t, base, newExecutionTestRuntime(clientA, clientB),
		)
		coordinator, err := reconciliationCoordinatorWithReaderForTest(
			t, execution, &reconciliationSweepReader{},
		)
		require.NoError(t, err)
		sweep, err := coordinator.BeginSweep()
		require.NoError(t, err)
		defer sweep.End()

		provisionsA, err := sweep.CollectProvisionInventory(t.Context(), "backend-a")
		require.NoError(t, err)
		retentionsB, err := sweep.CollectRetentionInventory(t.Context(), "backend-b")
		require.NoError(t, err)
		disposition, err := sweep.RecordBackendInventory(provisionsA, retentionsB)
		require.ErrorIs(t, err, inventory.ErrInvalidSession)
		require.Equal(t, BackendInventoryInvalid, disposition)
		// A failed splice consumes nothing. Each exact receipt still has to be
		// disposed through its typed rejected-half transition before sealing.
		require.NoError(t, sweep.RejectProvisionInventory(provisionsA))
		require.NoError(t, sweep.RejectRetentionInventory(retentionsB))
		require.NoError(t, sweep.SealInventory())
		require.False(t, sweep.InventoryComplete())
	})

	for _, test := range []struct {
		name    string
		collect func(*testing.T, *ReconciliationSweep) error
	}{
		{
			name: "provision-only receipt",
			collect: func(t *testing.T, sweep *ReconciliationSweep) error {
				response, err := sweep.CollectProvisionInventory(t.Context(), "backend-a")
				require.NoError(t, err)
				require.ErrorIs(t, sweep.SealInventory(), inventory.ErrInvalidSession)
				return sweep.RejectProvisionInventory(response)
			},
		},
		{
			name: "retention-only receipt",
			collect: func(t *testing.T, sweep *ReconciliationSweep) error {
				response, err := sweep.CollectRetentionInventory(t.Context(), "backend-a")
				require.NoError(t, err)
				require.ErrorIs(t, sweep.SealInventory(), inventory.ErrInvalidSession)
				return sweep.RejectRetentionInventory(response)
			},
		},
	} {
		t.Run(test.name+" grants no endpoint or absence authority", func(t *testing.T) {
			_, _, sweep := newCollectedInventorySweep(t, nil, nil)
			defer sweep.End()
			require.NoError(t, test.collect(t, sweep))
			require.NoError(t, sweep.SealInventory())
			require.False(t, sweep.InventoryComplete())
			projected, err := sweep.Project(ReconciliationProjection{})
			require.NoError(t, err)
			require.False(t, projected.Complete())
			require.Empty(t, projected.EligibleBackends())
			require.False(t, projected.AdmissionBaseline().Valid())
			require.False(t, projected.HasPruneAbsence(leaseUUID))
		})
	}

	t.Run("cross-endpoint contradiction can only become quarantine", func(t *testing.T) {
		store, _, sweep := newCollectedInventorySweep(
			t,
			[]backend.ProvisionInfo{provision},
			[]backend.RetainedLease{{LeaseUUID: leaseUUID}},
		)
		defer sweep.End()
		provisions, err := sweep.CollectProvisionInventory(t.Context(), "backend-a")
		require.NoError(t, err)
		retentions, err := sweep.CollectRetentionInventory(t.Context(), "backend-a")
		require.NoError(t, err)
		disposition, err := sweep.RecordBackendInventory(provisions, retentions)
		require.NoError(t, err)
		require.Equal(t, BackendInventoryUntrusted, disposition)
		require.NoError(t, sweep.SealInventory())
		_, err = sweep.Project(ReconciliationProjection{
			UntrustedPositives: map[string][]string{leaseUUID: {"backend-a"}},
		})
		require.NoError(t, err)
		require.Equal(t, StateUnusable, store.Lookup(leaseUUID).State())
	})
}

func TestCollectedPositiveBlocksAdmissionBeforeInventoryDisposition(t *testing.T) {
	const leaseUUID = "00000000-0000-4000-8000-000000000205"
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	baseline := requireAdmissionBaseline(t, store, "backend-a", "backend-b")
	clientA := &unrecordedPositiveInventoryBackend{
		executionTestBackend: &executionTestBackend{name: "backend-a"},
		provisions: []backend.ProvisionInfo{{
			LeaseUUID: leaseUUID, BackendName: "backend-a",
			ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
		}},
		storageID: testBackendStorageID("backend-a"),
	}
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(
		clientA, &executionTestBackend{name: "backend-b"},
	))
	coordinator, err := reconciliationCoordinatorWithReaderForTest(
		t, execution, &reconciliationSweepReader{},
	)
	require.NoError(t, err)
	sweep, err := coordinator.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	response, err := sweep.CollectProvisionInventory(t.Context(), "backend-a")
	require.NoError(t, err)
	require.Len(t, response.Provisions(), 1)

	scope := requireAdmissionScope(t, store, baseline, "backend-b")
	operationID := requireOperationID(t, "2205")
	request, err := newBackendRequestSnapshot(
		"tenant-test", freshTestProviderUUID,
		[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
	)
	require.NoError(t, err)
	_, applied, err := store.beginNewAttempt(
		scope, leaseUUID, "backend-b", operationID, PayloadFingerprint{},
		request, testCallbackPair(operationID),
	)
	require.ErrorIs(t, err, ErrUnprojectedInventoryPositive)
	require.False(t, applied)
}

func TestNewSweepRevokesOlderProjectedActionMintingAndFencesHeldActions(t *testing.T) {
	const leaseUUID = "00000000-0000-4000-8000-000000000206"
	newFixture := func(t *testing.T) (*ReconciliationCoordinator, *ProjectedReconciliationSweep) {
		t.Helper()
		store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
		requireAdmissionBaseline(t, store, "backend-a")
		reader := &reconciliationSweepReader{lease: &billingtypes.Lease{
			Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
			State: billingtypes.LEASE_STATE_PENDING,
			Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
		}}
		base, err := store.BindOperationCoordinator(nil)
		require.NoError(t, err)
		execution := bindExecutionForTest(
			t, base, newExecutionTestRuntime(&executionTestBackend{name: "backend-a"}),
		)
		coordinator, err := reconciliationCoordinatorWithReaderForTest(t, execution, reader)
		require.NoError(t, err)
		sweep, err := coordinator.BeginSweep()
		require.NoError(t, err)
		t.Cleanup(sweep.End)
		storageID := testBackendStorageID("backend-a")
		require.NoError(t, sweep.RecordProvision("backend-a", storageID, nil))
		require.NoError(t, sweep.RecordRetention("backend-a", storageID, nil))
		require.NoError(t, sweep.SealInventory())
		projected, err := sweep.Project(ReconciliationProjection{})
		require.NoError(t, err)
		require.True(t, projected.Valid())
		return coordinator, projected
	}

	t.Run("new sweep before claim prevents old action minting", func(t *testing.T) {
		coordinator, projected := newFixture(t)
		newer, err := coordinator.BeginSweep()
		require.NoError(t, err)
		defer newer.End()
		assert.False(t, projected.Valid())

		action, disposition, err := projected.ObserveLiveAction(t.Context(), leaseUUID)
		require.Error(t, err)
		assert.Equal(t, ReconciliationObservationInvalid, disposition)
		assert.False(t, action.Valid())
	})

	t.Run("claim before new sweep is captured and old action cannot dispatch later", func(t *testing.T) {
		coordinator, projected := newFixture(t)
		action, disposition, err := projected.ObserveLiveAction(t.Context(), leaseUUID)
		require.NoError(t, err)
		require.Equal(t, ReconciliationObservationReady, disposition)
		require.True(t, action.Valid())

		newer, err := coordinator.BeginSweep()
		require.NoError(t, err)
		defer newer.End()
		assert.True(t, newer.WasInFlight(leaseUUID),
			"a newer boundary must exclude an action whose lease claim was already held")
		assert.False(t, projected.Valid())
		assert.False(t, action.Valid())
		result := coordinator.Provision(t.Context(), action, nil, PayloadFingerprint{})
		assert.ErrorIs(t, result.Err(), ErrReconciliationBoundaryStale)
		assert.True(t, coordinator.ReleaseAction(action),
			"epoch revocation must not strand the exact held lease claim")
	})
}

func TestReconciliationPlacementViewsDoNotAliasConflictCandidates(t *testing.T) {
	const leaseUUID = "00000000-0000-4000-8000-000000000207"
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, store, "backend-a", "backend-b")
	reader := &reconciliationSweepReader{lease: &billingtypes.Lease{
		Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
		State: billingtypes.LEASE_STATE_ACTIVE,
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
	}}
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(
		&executionTestBackend{name: "backend-a"},
		&executionTestBackend{name: "backend-b"},
	))
	coordinator, err := reconciliationCoordinatorWithReaderForTest(t, execution, reader)
	require.NoError(t, err)

	recordConflict := func(t *testing.T, sweep *ReconciliationSweep) {
		t.Helper()
		for _, backendName := range []string{"backend-a", "backend-b"} {
			storageID := testBackendStorageID(backendName)
			require.NoError(t, sweep.RecordProvision(
				backendName, storageID, []backend.ProvisionInfo{{
					LeaseUUID: leaseUUID, BackendName: backendName,
				}},
			))
			require.NoError(t, sweep.RecordRetention(backendName, storageID, nil))
		}
		require.NoError(t, sweep.SealInventory())
	}

	seed, err := coordinator.BeginSweep()
	require.NoError(t, err)
	recordConflict(t, seed)
	_, err = seed.Project(ReconciliationProjection{
		Conflicts: map[string][]string{leaseUUID: {"backend-a", "backend-b"}},
	})
	require.NoError(t, err)
	seed.End()

	sweep, err := coordinator.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	initial := sweep.InitialRecord(leaseUUID)
	require.Len(t, initial.ConflictBackends, 2)
	initial.ConflictBackends[0] = "mutated"
	assert.ElementsMatch(t, []string{"backend-a", "backend-b"},
		sweep.InitialRecord(leaseUUID).ConflictBackends)
	initialRecords := sweep.InitialRecords()
	mutated := initialRecords[leaseUUID]
	mutated.ConflictBackends[0] = "mutated"
	initialRecords[leaseUUID] = mutated
	assert.ElementsMatch(t, []string{"backend-a", "backend-b"},
		sweep.InitialRecords()[leaseUUID].ConflictBackends)

	recordConflict(t, sweep)
	projected, err := sweep.Project(ReconciliationProjection{
		Conflicts: map[string][]string{leaseUUID: {"backend-a", "backend-b"}},
	})
	require.NoError(t, err)
	projectedRecord := projected.Record(leaseUUID)
	projectedRecord.ConflictBackends[0] = "mutated"
	assert.ElementsMatch(t, []string{"backend-a", "backend-b"},
		projected.Record(leaseUUID).ConflictBackends)
	projectedRecords := projected.Records()
	mutated = projectedRecords[leaseUUID]
	mutated.ConflictBackends[0] = "mutated"
	projectedRecords[leaseUUID] = mutated
	assert.ElementsMatch(t, []string{"backend-a", "backend-b"},
		projected.Records()[leaseUUID].ConflictBackends)

	action, disposition, err := projected.ObserveLiveAction(t.Context(), leaseUUID)
	require.NoError(t, err)
	require.Equal(t, ReconciliationObservationReady, disposition)
	actionRecord := action.Placement()
	actionRecord.ConflictBackends[0] = "mutated"
	assert.ElementsMatch(t, []string{"backend-a", "backend-b"},
		action.Placement().ConflictBackends)
	require.True(t, coordinator.ReleaseAction(action))
}

func TestInterruptedProjectionSurvivesRestartAndWithdrawsDegradedRecordlessAdmission(t *testing.T) {
	const leaseUUID = "00000000-0000-4000-8000-000000000201"
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	routes := testCallbackRoutes(t)
	store, err := newStoreForTest(dbPath, WithCallbackRouteFactory(routes))
	require.NoError(t, err)
	require.NoError(t, configureBackendTopologyForTest(store, []string{"backend-a", "backend-b"}))
	reader := &reconciliationSweepReader{lease: &billingtypes.Lease{
		Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
		State: billingtypes.LEASE_STATE_PENDING,
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
	}}
	bind := func(t *testing.T, current *Store, backendA, backendB *executionTestBackend) *ReconciliationCoordinator {
		t.Helper()
		base, bindErr := current.BindOperationCoordinator(nil)
		require.NoError(t, bindErr)
		execution := bindExecutionForTest(t, base, newExecutionTestRuntime(backendA, backendB))
		coordinator, coordinatorErr := reconciliationCoordinatorWithReaderForTest(t, execution, reader)
		require.NoError(t, coordinatorErr)
		return coordinator
	}

	backendA := &executionTestBackend{name: "backend-a"}
	backendB := &executionTestBackend{name: "backend-b"}
	coordinator := bind(t, store, backendA, backendB)
	initial, err := coordinator.BeginSweep()
	require.NoError(t, err)
	for _, backendName := range []string{"backend-a", "backend-b"} {
		storageID := testBackendStorageID(backendName)
		require.NoError(t, initial.RecordProvision(backendName, storageID, nil))
		require.NoError(t, initial.RecordRetention(backendName, storageID, nil))
	}
	require.NoError(t, initial.SealInventory())
	_, err = initial.Project(ReconciliationProjection{})
	require.NoError(t, err)
	initial.End()
	require.True(t, store.CurrentAdmissionBaseline().Valid())

	// The marker commits before this sweep can observe backend A. Closing the DB
	// before projection models a process failure/write failure after the positive
	// was collected but before any placement row could commit.
	interrupted, err := coordinator.BeginSweep()
	require.NoError(t, err)
	storageA := testBackendStorageID("backend-a")
	require.NoError(t, interrupted.RecordProvision("backend-a", storageA, []backend.ProvisionInfo{{
		LeaseUUID: leaseUUID, BackendName: "backend-a",
		ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
	}}))
	require.NoError(t, interrupted.RecordRetention("backend-a", storageA, nil))
	storageB := testBackendStorageID("backend-b")
	require.NoError(t, interrupted.RecordProvision("backend-b", storageB, nil))
	require.NoError(t, interrupted.RecordRetention("backend-b", storageB, nil))
	require.NoError(t, interrupted.SealInventory())
	require.NoError(t, store.db.Close())
	_, err = interrupted.Project(ReconciliationProjection{
		Placements: map[string]string{leaseUUID: "backend-a"},
	})
	require.Error(t, err)
	interrupted.End()
	_ = store.Close()

	reopened, err := OpenStore(
		dbPath, freshTestProviderUUID, WithCallbackRouteFactory(routes),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.False(t, reopened.CurrentAdmissionBaseline().Valid(),
		"an interrupted sweep must withdraw the otherwise-current durable baseline after restart")

	var backendBProvisions int
	restartedA := &executionTestBackend{name: "backend-a"}
	restartedB := &executionTestBackend{
		name: "backend-b",
		provision: func(context.Context, backend.ProvisionRequest) error {
			backendBProvisions++
			return nil
		},
	}
	restarted := bind(t, reopened, restartedA, restartedB)
	degraded, err := restarted.BeginSweep()
	require.NoError(t, err)
	require.NoError(t, degraded.RecordProvision("backend-b", storageB, nil))
	require.NoError(t, degraded.RecordRetention("backend-b", storageB, nil))
	require.NoError(t, degraded.SealInventory())
	projected, err := degraded.Project(ReconciliationProjection{})
	require.NoError(t, err)
	defer degraded.End()
	require.False(t, projected.Complete())
	require.False(t, projected.AdmissionBaseline().Valid(),
		"one answering peer cannot clear interrupted-sweep uncertainty")

	action, disposition, err := projected.ObserveLiveAction(t.Context(), leaseUUID)
	require.NoError(t, err)
	require.Equal(t, ReconciliationObservationStale, disposition)
	require.False(t, action.Valid())
	assert.Zero(t, backendBProvisions,
		"the recordless lease observed on silent backend A must not be duplicated on backend B")
	assert.Equal(t, StateAbsent, reopened.Lookup(leaseUUID).State())
}

func TestFencedPositiveProjectionSurvivesRestartAndWithdrawsDegradedAdmission(t *testing.T) {
	const leaseUUID = "00000000-0000-4000-8000-000000000202"
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	routes := testCallbackRoutes(t)
	store, err := newStoreForTest(dbPath, WithCallbackRouteFactory(routes))
	require.NoError(t, err)
	require.NoError(t, configureBackendTopologyForTest(store, []string{"backend-a", "backend-b"}))
	reader := &reconciliationSweepReader{lease: &billingtypes.Lease{
		Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
		State: billingtypes.LEASE_STATE_PENDING,
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
	}}
	bind := func(t *testing.T, current *Store, backendA, backendB *executionTestBackend) *ReconciliationCoordinator {
		t.Helper()
		base, bindErr := current.BindOperationCoordinator(nil)
		require.NoError(t, bindErr)
		execution := bindExecutionForTest(t, base, newExecutionTestRuntime(backendA, backendB))
		coordinator, coordinatorErr := reconciliationCoordinatorWithReaderForTest(t, execution, reader)
		require.NoError(t, coordinatorErr)
		return coordinator
	}
	recordComplete := func(t *testing.T, sweep *ReconciliationSweep, backendAPositive bool) {
		t.Helper()
		for _, backendName := range []string{"backend-a", "backend-b"} {
			storageID := testBackendStorageID(backendName)
			var provisions []backend.ProvisionInfo
			if backendAPositive && backendName == "backend-a" {
				provisions = []backend.ProvisionInfo{{
					LeaseUUID: leaseUUID, BackendName: backendName,
					ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
				}}
			}
			require.NoError(t, sweep.RecordProvision(backendName, storageID, provisions))
			require.NoError(t, sweep.RecordRetention(backendName, storageID, nil))
		}
		require.NoError(t, sweep.SealInventory())
	}

	coordinator := bind(
		t, store,
		&executionTestBackend{name: "backend-a"},
		&executionTestBackend{name: "backend-b"},
	)
	initial, err := coordinator.BeginSweep()
	require.NoError(t, err)
	recordComplete(t, initial, true)
	_, err = initial.Project(ReconciliationProjection{
		Placements: map[string]string{leaseUUID: "backend-a"},
	})
	require.NoError(t, err)
	initial.End()
	require.Equal(t, StateConfirmed, store.Lookup(leaseUUID).State())
	require.True(t, store.CurrentAdmissionBaseline().Valid())

	// A newer exact deletion fences the collected positive before projection.
	// The projection succeeds without recreating the row, so its durable marker
	// must retain that unrepresented backend affinity across process restart.
	fenced, err := coordinator.BeginSweep()
	require.NoError(t, err)
	recordComplete(t, fenced, true)
	requireDeleteRecord(t, store, leaseUUID)
	projected, err := fenced.Project(ReconciliationProjection{
		Placements: map[string]string{leaseUUID: "backend-a"},
	})
	require.NoError(t, err)
	require.True(t, projected.fenced(leaseUUID))
	require.False(t, projected.AdmissionBaseline().Valid())
	fenced.End()
	require.NoError(t, store.Close())

	reopened, err := OpenStore(
		dbPath, freshTestProviderUUID, WithCallbackRouteFactory(routes),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	require.False(t, reopened.CurrentAdmissionBaseline().Valid())

	var backendBProvisions int
	restarted := bind(
		t, reopened,
		&executionTestBackend{name: "backend-a"},
		&executionTestBackend{
			name: "backend-b",
			provision: func(context.Context, backend.ProvisionRequest) error {
				backendBProvisions++
				return nil
			},
		},
	)
	degraded, err := restarted.BeginSweep()
	require.NoError(t, err)
	storageB := testBackendStorageID("backend-b")
	require.NoError(t, degraded.RecordProvision("backend-b", storageB, nil))
	require.NoError(t, degraded.RecordRetention("backend-b", storageB, nil))
	require.NoError(t, degraded.SealInventory())
	degradedProjection, err := degraded.Project(ReconciliationProjection{})
	require.NoError(t, err)
	defer degraded.End()
	require.False(t, degradedProjection.Complete())
	require.False(t, degradedProjection.AdmissionBaseline().Valid())

	action, disposition, err := degradedProjection.ObserveLiveAction(t.Context(), leaseUUID)
	require.NoError(t, err)
	require.Equal(t, ReconciliationObservationStale, disposition)
	require.False(t, action.Valid())
	assert.Zero(t, backendBProvisions)
	assert.Equal(t, StateAbsent, reopened.Lookup(leaseUUID).State())
}

func TestRepresentedFencedProvisionKeepsHealthySiblingAdmission(t *testing.T) {
	fixture := newFencedAvailabilityFixture(t)
	var backendBProvisions int
	fixture.backendB.provision = func(context.Context, backend.ProvisionRequest) error {
		backendBProvisions++
		return nil
	}
	sweep := beginFencedAvailabilitySweep(t, fixture)
	defer sweep.End()
	storageA := testBackendStorageID("backend-a")
	require.NoError(t, sweep.RecordProvision(
		"backend-a", storageA, []backend.ProvisionInfo{{
			LeaseUUID: fencedAvailabilityOwner, BackendName: "backend-a",
			ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
			LifecycleGeneration: &backend.LifecycleGenerationObservation{
				Kind: backend.LifecycleGenerationTyped,
				ID:   fixture.lifecycleID.String(),
			},
		}},
	))
	require.NoError(t, sweep.RecordRetention("backend-a", storageA, nil))
	storageB := testBackendStorageID("backend-b")
	require.NoError(t, sweep.RecordProvision("backend-b", storageB, nil))
	require.NoError(t, sweep.RecordRetention("backend-b", storageB, nil))
	require.NoError(t, sweep.SealInventory())
	projected, err := sweep.Project(ReconciliationProjection{
		Placements: map[string]string{fencedAvailabilityOwner: "backend-a"},
	})
	require.NoError(t, err)
	require.False(t, projected.Complete(), "backend-c is intentionally unavailable")
	require.True(t, projected.fenced(fencedAvailabilityOwner))
	require.False(t, fixture.store.inventoryRecoveryRequired,
		"the exact durable owner/attempt generation already represents this positive")
	require.True(t, projected.AdmissionBaseline().Valid())
	require.False(t, fixture.coordinator.AbsenceUntrusted(fencedAvailabilityOwner))
	require.NoError(t, fixture.store.leaseSideEffectError(fencedAvailabilityOwner))

	action, disposition, err := projected.ObserveLiveAction(
		t.Context(), fencedAvailabilitySibling,
	)
	require.NoError(t, err)
	require.Equal(t, ReconciliationObservationReady, disposition)
	require.True(t, action.Valid())
	defer fixture.coordinator.ReleaseAction(action)
	result := fixture.coordinator.Provision(t.Context(), action, nil, PayloadFingerprint{})
	require.NoError(t, result.Err())
	assert.Equal(t, "backend-b", result.BackendName())
	assert.Equal(t, 1, backendBProvisions,
		"routine same-generation fencing must not pause an unrelated healthy-node admission")
}

func TestUnrepresentedFencedPositiveWithdrawsHealthySiblingAdmission(t *testing.T) {
	for _, test := range []struct {
		name       string
		record     func(*testing.T, *ReconciliationSweep, fencedAvailabilityFixture)
		projection ReconciliationProjection
	}{
		{
			name: "retention is not a live provision",
			record: func(t *testing.T, sweep *ReconciliationSweep, _ fencedAvailabilityFixture) {
				storageA := testBackendStorageID("backend-a")
				require.NoError(t, sweep.RecordProvision("backend-a", storageA, nil))
				require.NoError(t, sweep.RecordRetention(
					"backend-a", storageA, []string{fencedAvailabilityOwner},
				))
				storageB := testBackendStorageID("backend-b")
				require.NoError(t, sweep.RecordProvision("backend-b", storageB, nil))
				require.NoError(t, sweep.RecordRetention("backend-b", storageB, nil))
			},
			projection: ReconciliationProjection{
				Placements: map[string]string{fencedAvailabilityOwner: "backend-a"},
			},
		},
		{
			name: "untrusted identity requires quarantine",
			record: func(t *testing.T, sweep *ReconciliationSweep, _ fencedAvailabilityFixture) {
				storageA := testBackendStorageID("backend-a")
				require.NoError(t, sweep.RecordProvision("backend-a", storageA, nil))
				require.NoError(t, sweep.RecordRetention("backend-a", storageA, nil))
				require.NoError(t, sweep.RecordUntrusted(
					"backend-a", []string{fencedAvailabilityOwner},
				))
				storageB := testBackendStorageID("backend-b")
				require.NoError(t, sweep.RecordProvision("backend-b", storageB, nil))
				require.NoError(t, sweep.RecordRetention("backend-b", storageB, nil))
			},
			projection: ReconciliationProjection{
				UntrustedPositives: map[string][]string{
					fencedAvailabilityOwner: {"backend-a"},
				},
			},
		},
		{
			name: "novel reporter is not the durable owner",
			record: func(t *testing.T, sweep *ReconciliationSweep, fixture fencedAvailabilityFixture) {
				storageA := testBackendStorageID("backend-a")
				require.NoError(t, sweep.RecordProvision("backend-a", storageA, nil))
				require.NoError(t, sweep.RecordRetention("backend-a", storageA, nil))
				storageB := testBackendStorageID("backend-b")
				require.NoError(t, sweep.RecordProvision(
					"backend-b", storageB, []backend.ProvisionInfo{{
						LeaseUUID: fencedAvailabilityOwner, BackendName: "backend-b",
						ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
						LifecycleGeneration: &backend.LifecycleGenerationObservation{
							Kind: backend.LifecycleGenerationTyped,
							ID:   fixture.lifecycleID.String(),
						},
					}},
				))
				require.NoError(t, sweep.RecordRetention("backend-b", storageB, nil))
			},
			projection: ReconciliationProjection{
				Conflicts: map[string][]string{
					fencedAvailabilityOwner: {"backend-a", "backend-b"},
				},
			},
		},
		{
			name: "same generation reports another provider",
			record: func(t *testing.T, sweep *ReconciliationSweep, fixture fencedAvailabilityFixture) {
				storageA := testBackendStorageID("backend-a")
				require.NoError(t, sweep.RecordProvision(
					"backend-a", storageA, []backend.ProvisionInfo{{
						LeaseUUID: fencedAvailabilityOwner, BackendName: "backend-a",
						ProviderUUID: "00000000-0000-4000-8000-000000000299",
						Tenant:       "tenant-test",
						LifecycleGeneration: &backend.LifecycleGenerationObservation{
							Kind: backend.LifecycleGenerationTyped,
							ID:   fixture.lifecycleID.String(),
						},
					}},
				))
				require.NoError(t, sweep.RecordRetention("backend-a", storageA, nil))
				storageB := testBackendStorageID("backend-b")
				require.NoError(t, sweep.RecordProvision("backend-b", storageB, nil))
				require.NoError(t, sweep.RecordRetention("backend-b", storageB, nil))
			},
			projection: ReconciliationProjection{
				Placements: map[string]string{fencedAvailabilityOwner: "backend-a"},
			},
		},
		{
			name: "same generation reports another tenant",
			record: func(t *testing.T, sweep *ReconciliationSweep, fixture fencedAvailabilityFixture) {
				storageA := testBackendStorageID("backend-a")
				require.NoError(t, sweep.RecordProvision(
					"backend-a", storageA, []backend.ProvisionInfo{{
						LeaseUUID: fencedAvailabilityOwner, BackendName: "backend-a",
						ProviderUUID: freshTestProviderUUID,
						Tenant:       "another-tenant",
						LifecycleGeneration: &backend.LifecycleGenerationObservation{
							Kind: backend.LifecycleGenerationTyped,
							ID:   fixture.lifecycleID.String(),
						},
					}},
				))
				require.NoError(t, sweep.RecordRetention("backend-a", storageA, nil))
				storageB := testBackendStorageID("backend-b")
				require.NoError(t, sweep.RecordProvision("backend-b", storageB, nil))
				require.NoError(t, sweep.RecordRetention("backend-b", storageB, nil))
			},
			projection: ReconciliationProjection{
				Placements: map[string]string{fencedAvailabilityOwner: "backend-a"},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFencedAvailabilityFixture(t)
			sweep := beginFencedAvailabilitySweep(t, fixture)
			defer sweep.End()
			test.record(t, sweep, fixture)
			require.NoError(t, sweep.SealInventory())
			projected, err := sweep.Project(test.projection)
			require.NoError(t, err)
			require.True(t, projected.fenced(fencedAvailabilityOwner))
			require.True(t, fixture.store.inventoryRecoveryRequired)
			require.False(t, projected.AdmissionBaseline().Valid())
			require.True(t, fixture.coordinator.AbsenceUntrusted(fencedAvailabilityOwner))

			action, disposition, err := projected.ObserveLiveAction(
				t.Context(), fencedAvailabilitySibling,
			)
			require.NoError(t, err)
			require.Equal(t, ReconciliationObservationStale, disposition)
			require.False(t, action.Valid())
		})
	}
}

func TestCommittedCausalQuarantineKeepsHealthySiblingAdmission(t *testing.T) {
	fixture := newFencedAvailabilityFixture(t)
	claim := fixture.coordinator.coordinator.operations.TryClaimLeaseNow(
		fencedAvailabilityOwner,
	)
	require.True(t, claim.Acquired())
	initiation := fixture.coordinator.coordinator.operations.TryInitiateProvisionClaimed(
		claim.Claim(),
		testProvisionInitiation(
			t, fencedAvailabilityOwner, "tenant-test", "backend-a",
		),
	)
	require.True(t, initiation.Started())

	sweep, err := fixture.coordinator.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	require.True(t, sweep.WasInFlight(fencedAvailabilityOwner))
	storageA := testBackendStorageID("backend-a")
	require.NoError(t, sweep.RecordProvision("backend-a", storageA, nil))
	require.NoError(t, sweep.RecordRetention("backend-a", storageA, nil))
	require.NoError(t, sweep.RecordUntrusted(
		"backend-a", []string{fencedAvailabilityOwner},
	))
	storageB := testBackendStorageID("backend-b")
	require.NoError(t, sweep.RecordProvision("backend-b", storageB, nil))
	require.NoError(t, sweep.RecordRetention("backend-b", storageB, nil))
	require.NoError(t, sweep.SealInventory())
	require.Equal(t, operation.InitiationAborted,
		fixture.coordinator.coordinator.operations.AbortInitiation(initiation.Capability()))
	require.True(t, fixture.coordinator.coordinator.operations.ReleaseLease(claim.Claim()))

	projected, err := sweep.Project(ReconciliationProjection{
		UntrustedPositives: map[string][]string{
			fencedAvailabilityOwner: {"backend-a"},
		},
	})
	require.NoError(t, err)
	require.False(t, projected.Complete(), "backend-c is intentionally unavailable")
	require.Equal(t, StateUnusable, fixture.store.Lookup(fencedAvailabilityOwner).State())
	require.False(t, fixture.store.inventoryRecoveryRequired,
		"the exact quarantine and marker clear commit atomically")
	require.True(t, projected.AdmissionBaseline().Valid())
	require.False(t, fixture.coordinator.AbsenceUntrusted(fencedAvailabilityOwner))

	action, disposition, err := projected.ObserveLiveAction(
		t.Context(), fencedAvailabilitySibling,
	)
	require.NoError(t, err)
	require.Equal(t, ReconciliationObservationReady, disposition)
	require.True(t, action.Valid())
	require.True(t, fixture.coordinator.ReleaseAction(action))
	sweep.End()

	// The same rejected fact on a later causally fenced partial sweep is
	// already represented by the durable unusable candidate set. Repeating it
	// must not turn an ordinary sibling outage into a global admission pause.
	repeatClaim := fixture.coordinator.coordinator.operations.TryClaimLeaseNow(
		fencedAvailabilityOwner,
	)
	require.True(t, repeatClaim.Acquired())
	repeatInitiation := fixture.coordinator.coordinator.operations.TryInitiateProvisionClaimed(
		repeatClaim.Claim(),
		testProvisionInitiation(
			t, fencedAvailabilityOwner, "tenant-test", "backend-a",
		),
	)
	require.True(t, repeatInitiation.Started())
	repeat, err := fixture.coordinator.BeginSweep()
	require.NoError(t, err)
	defer repeat.End()
	require.True(t, repeat.WasInFlight(fencedAvailabilityOwner))
	require.NoError(t, repeat.RecordProvision("backend-a", storageA, nil))
	require.NoError(t, repeat.RecordRetention("backend-a", storageA, nil))
	require.NoError(t, repeat.RecordUntrusted(
		"backend-a", []string{fencedAvailabilityOwner},
	))
	require.NoError(t, repeat.RecordProvision("backend-b", storageB, nil))
	require.NoError(t, repeat.RecordRetention("backend-b", storageB, nil))
	require.NoError(t, repeat.SealInventory())
	require.Equal(t, operation.InitiationAborted,
		fixture.coordinator.coordinator.operations.AbortInitiation(repeatInitiation.Capability()))
	require.True(t,
		fixture.coordinator.coordinator.operations.ReleaseLease(repeatClaim.Claim()))
	repeatedProjection, err := repeat.Project(ReconciliationProjection{
		UntrustedPositives: map[string][]string{
			fencedAvailabilityOwner: {"backend-a"},
		},
	})
	require.NoError(t, err)
	require.True(t, repeatedProjection.AdmissionBaseline().Valid())
	require.False(t, fixture.store.inventoryRecoveryRequired)
}

func TestIncompleteRetentionIsConstructionBoundQuarantine(t *testing.T) {
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	baseline := requireAdmissionBaseline(t, store, "backend-a", "backend-b")
	reader := &reconciliationSweepReader{lease: &billingtypes.Lease{
		Uuid: fencedAvailabilitySibling, Tenant: "tenant-test",
		ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_PENDING,
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
	}}
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, &fixedRouteExecutionRuntime{
		executionTestRuntime: newExecutionTestRuntime(
			&executionTestBackend{name: "backend-a"},
			&executionTestBackend{name: "backend-b"},
		),
		routeName: "backend-a",
	})
	coordinator, err := reconciliationCoordinatorWithReaderForTest(t, execution, reader)
	require.NoError(t, err)

	sweep, err := coordinator.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	storageID := testBackendStorageID("backend-a")
	require.NoError(t, sweep.RecordProvision("backend-a", storageID, nil))
	require.NoError(t, sweep.RecordRetention(
		"backend-a", storageID, []string{fencedAvailabilityOwner},
	))
	require.NoError(t, sweep.SealInventory())

	// No caller-supplied projection class is needed: the sealed snapshot owns
	// the fact that this is a retention positive observed during a partial fleet
	// sweep, and the placement layer can only turn it into quarantine.
	projected, err := sweep.Project(ReconciliationProjection{})
	require.NoError(t, err)
	require.False(t, projected.Complete())
	require.Equal(t, baseline, projected.AdmissionBaseline())
	record := store.Lookup(fencedAvailabilityOwner)
	require.Equal(t, StateUnusable, record.State())
	require.Equal(t, []string{"backend-a"}, record.ConflictBackends)

	action, disposition, err := projected.ObserveLiveAction(
		t.Context(), fencedAvailabilitySibling,
	)
	require.NoError(t, err)
	require.Equal(t, ReconciliationObservationReady, disposition,
		"the lease-local quarantine must not pause healthy-backend admission")
	require.True(t, action.Valid())
	require.True(t, coordinator.ReleaseAction(action))
}

func TestInheritedInventoryRecoveryBlocksFreshSideEffectsButAllowsExactMaintenanceRecovery(
	t *testing.T,
) {
	const (
		ownerUUID  = "00000000-0000-4000-8000-000000000208"
		orphanUUID = "00000000-0000-4000-8000-000000000209"
	)
	for _, test := range []struct {
		name      string
		interrupt func(*testing.T, *ReconciliationSweep)
	}{
		{
			name: "same-backend retention",
			interrupt: func(t *testing.T, sweep *ReconciliationSweep) {
				require.NoError(t, sweep.RecordRetention(
					"backend-a", testBackendStorageID("backend-a"), []string{ownerUUID},
				))
			},
		},
		{
			name: "same-backend untrusted provision",
			interrupt: func(t *testing.T, sweep *ReconciliationSweep) {
				require.NoError(t, sweep.RecordUntrusted("backend-a", []string{ownerUUID}))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "placements.db")
			routes := testCallbackRoutes(t)
			store, err := newStoreForTest(dbPath, WithCallbackRouteFactory(routes))
			require.NoError(t, err)
			baseline := requireAdmissionBaseline(t, store, "backend-a", "backend-b")
			scope := requireAdmissionScope(t, store, baseline, "backend-a")
			seedID := requireOperationID(t, "208")
			request, err := newBackendRequestSnapshot(
				"tenant-test", freshTestProviderUUID,
				[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
			)
			require.NoError(t, err)
			attempt, applied, err := store.beginNewAttempt(
				scope, ownerUUID, "backend-a", seedID, PayloadFingerprint{}, request,
				testCallbackPair(seedID),
			)
			require.NoError(t, err)
			require.True(t, applied)
			confirmed, err := confirmAttemptForTest(store, attempt)
			require.NoError(t, err)
			require.True(t, confirmed)
			lifecycleID := store.CurrentLifecycle(ownerUUID).ID()
			require.True(t, lifecycleID.Valid())

			maintenanceID, err := maintenanceid.Parse(maintenanceIDA)
			require.NoError(t, err)
			prepared, err := store.prepareMaintenanceCommand(
				maintenanceID, ownerUUID, MaintenanceCommandRestart, nil,
			)
			require.NoError(t, err)
			admission, err := store.beginMaintenanceCommand(prepared)
			require.NoError(t, err)
			require.True(t, admission.Pending())

			reader := &reconciliationSweepReader{lease: &billingtypes.Lease{
				Uuid: ownerUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
				State: billingtypes.LEASE_STATE_ACTIVE,
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
			}}
			bind := func(
				t *testing.T,
				current *Store,
				backendA, backendB *executionTestBackend,
			) (*ExecutionCoordinator, *ReconciliationCoordinator) {
				t.Helper()
				base, bindErr := current.BindOperationCoordinator(nil)
				require.NoError(t, bindErr)
				execution := bindExecutionForTest(
					t, base, newExecutionTestRuntime(backendA, backendB),
				)
				reconciliation, bindErr := reconciliationCoordinatorWithReaderForTest(t, execution, reader)
				require.NoError(t, bindErr)
				return execution, reconciliation
			}

			_, reconciliation := bind(
				t, store,
				&executionTestBackend{name: "backend-a"},
				&executionTestBackend{name: "backend-b"},
			)
			interrupted, err := reconciliation.BeginSweep()
			require.NoError(t, err)
			test.interrupt(t, interrupted)
			// Model process death after the positive observation but before a
			// semantic projection could represent it durably.
			require.NoError(t, store.db.Close())
			interrupted.End()
			_ = store.Close()

			reopened, err := OpenStore(
				dbPath, freshTestProviderUUID, WithCallbackRouteFactory(routes),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = reopened.Close() })
			require.True(t, reopened.inventoryRecoveryRequired)
			require.False(t, reopened.CurrentAdmissionBaseline().Valid())

			var restartCalls, deprovisionCalls int
			backendA := &executionTestBackend{
				name: "backend-a",
				restart: func(context.Context, backend.RestartRequest) error {
					restartCalls++
					return nil
				},
				deprovision: func(context.Context, string) error {
					deprovisionCalls++
					return nil
				},
			}
			backendB := &executionTestBackend{name: "backend-b"}
			execution, restarted := bind(t, reopened, backendA, backendB)

			maintenance, err := maintenanceCoordinatorWithReaderForTest(t, execution, reader)
			require.NoError(t, err)
			pending, err := maintenance.pendingMaintenanceCommands()
			require.NoError(t, err)
			require.Len(t, pending, 1)
			reauthorization := maintenance.reauthorizeMaintenanceCommand(t.Context(), pending[0])
			require.NoError(t, reauthorization.Err())
			require.True(t, reauthorization.Authorized(),
				"an exact durable command remains recoverable while fresh authority is withheld")
			completion := maintenance.executeMaintenance(
				t.Context(), reauthorization.Authorization(),
			)
			require.NoError(t, completion.Err())
			require.True(t, completion.Settled())
			assert.Equal(t, 1, restartCalls)

			partial, err := restarted.BeginSweep()
			require.NoError(t, err)
			storageA := testBackendStorageID("backend-a")
			require.NoError(t, partial.RecordProvision(
				"backend-a", storageA, []backend.ProvisionInfo{{
					LeaseUUID: orphanUUID, BackendName: "backend-a",
					ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
				}},
			))
			require.NoError(t, partial.RecordRetention("backend-a", storageA, nil))
			require.NoError(t, partial.SealInventory())
			projected, err := partial.Project(ReconciliationProjection{
				Placements: map[string]string{orphanUUID: "backend-a"},
			})
			require.NoError(t, err)
			require.False(t, projected.Complete())
			require.False(t, projected.AdmissionBaseline().Valid())
			require.False(t, projected.HasPruneAbsence(ownerUUID),
				"inherited uncertainty must not mint a destructive absence capability")

			live, disposition, err := projected.ObserveLiveAction(t.Context(), ownerUUID)
			require.NoError(t, err)
			require.Equal(t, ReconciliationObservationStale, disposition)
			require.False(t, live.Valid())
			orphan, disposition, err := projected.ObserveTerminalOrphan(t.Context(), orphanUUID)
			require.NoError(t, err)
			require.Equal(t, ReconciliationObservationStale, disposition)
			require.False(t, orphan.Valid())

			_, err = reopened.prepareMaintenanceCommand(
				maintenanceID, ownerUUID, MaintenanceCommandRestart, nil,
			)
			require.ErrorIs(t, err, ErrUnprojectedInventoryPositive)
			provision, err := provisionCoordinatorWithReaderForTest(t, execution, reader)
			require.NoError(t, err)
			err = provision.Deprovision(t.Context(), ownerUUID)
			require.ErrorIs(t, err, ErrUnprojectedInventoryPositive)
			assert.Zero(t, deprovisionCalls)
			partial.End()

			complete, err := restarted.BeginSweep()
			require.NoError(t, err)
			ownerRow := backend.ProvisionInfo{
				LeaseUUID: ownerUUID, BackendName: "backend-a",
				ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
				LifecycleGeneration: &backend.LifecycleGenerationObservation{
					Kind: backend.LifecycleGenerationTyped,
					ID:   lifecycleID.String(),
				},
			}
			orphanRow := backend.ProvisionInfo{
				LeaseUUID: orphanUUID, BackendName: "backend-a",
				ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
			}
			require.NoError(t, complete.RecordProvision(
				"backend-a", storageA, []backend.ProvisionInfo{ownerRow, orphanRow},
			))
			require.NoError(t, complete.RecordRetention("backend-a", storageA, nil))
			storageB := testBackendStorageID("backend-b")
			require.NoError(t, complete.RecordProvision("backend-b", storageB, nil))
			require.NoError(t, complete.RecordRetention("backend-b", storageB, nil))
			require.NoError(t, complete.SealInventory())
			resolved, err := complete.Project(ReconciliationProjection{
				Placements: map[string]string{
					ownerUUID:  "backend-a",
					orphanUUID: "backend-a",
				},
			})
			require.NoError(t, err)
			defer complete.End()
			require.True(t, resolved.Complete())
			require.True(t, resolved.AdmissionBaseline().Valid())
			require.False(t, reopened.inventoryRecoveryRequired)

			live, disposition, err = resolved.ObserveLiveAction(t.Context(), ownerUUID)
			require.NoError(t, err)
			require.Equal(t, ReconciliationObservationReady, disposition)
			require.True(t, live.Valid())
			require.True(t, restarted.ReleaseAction(live))
			_, err = reopened.prepareMaintenanceCommand(
				maintenanceID, ownerUUID, MaintenanceCommandRestart, nil,
			)
			require.NoError(t, err,
				"a complete semantic projection must restore fresh maintenance authority")
		})
	}
}

func TestRecordedPositiveBlocksConcurrentRecordlessProvisionBeforeProjection(t *testing.T) {
	const (
		leaseUUID  = "00000000-0000-4000-8000-000000000203"
		sourceUUID = "00000000-0000-4000-8000-000000000204"
	)
	for _, test := range []struct {
		name   string
		record func(*testing.T, *ReconciliationSweep)
	}{
		{
			name: "provision inventory",
			record: func(t *testing.T, sweep *ReconciliationSweep) {
				require.NoError(t, sweep.RecordProvision(
					"backend-a", testBackendStorageID("backend-a"),
					[]backend.ProvisionInfo{{
						LeaseUUID: leaseUUID, BackendName: "backend-a",
						ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
					}},
				))
			},
		},
		{
			name: "retention inventory",
			record: func(t *testing.T, sweep *ReconciliationSweep) {
				require.NoError(t, sweep.RecordRetention(
					"backend-a", testBackendStorageID("backend-a"), []string{leaseUUID},
				))
			},
		},
		{
			name: "untrusted inventory",
			record: func(t *testing.T, sweep *ReconciliationSweep) {
				require.NoError(t, sweep.RecordUntrusted("backend-a", []string{leaseUUID}))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := &reconciliationSweepReader{lease: &billingtypes.Lease{
				Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
				State: billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
			}}
			store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
			requireAdmissionBaseline(t, store, "backend-a", "backend-b")
			requireConfirmedPlacement(t, store, sourceUUID, "backend-b")
			var backendBProvisions int
			backendA := &executionTestBackend{name: "backend-a"}
			backendB := &executionTestBackend{
				name: "backend-b",
				provision: func(context.Context, backend.ProvisionRequest) error {
					backendBProvisions++
					return nil
				},
			}
			base, err := store.BindOperationCoordinator(nil)
			require.NoError(t, err)
			runtime := &fixedRouteExecutionRuntime{
				executionTestRuntime: newExecutionTestRuntime(backendA, backendB),
				routeName:            "backend-b",
			}
			execution := bindExecutionForTest(t, base, runtime)
			provision, err := provisionCoordinatorWithReaderForTest(t, execution, reader)
			require.NoError(t, err)
			reconciliation, err := reconciliationCoordinatorWithReaderForTest(t, execution, reader)
			require.NoError(t, err)

			sweep, err := reconciliation.BeginSweep()
			require.NoError(t, err)
			defer sweep.End()
			test.record(t, sweep)

			event, err := NewProvisionEventRequest(leaseUUID, "tenant-test")
			require.NoError(t, err)
			result := provision.ExecuteCurrentLease(t.Context(), event)
			require.Equal(t, ProvisionEventUncertain, result.Disposition())
			require.ErrorIs(t, result.Err(), ErrUnprojectedInventoryPositive)
			assert.Zero(t, backendBProvisions,
				"a recorded positive must close the observation-to-projection dispatch race")
			assert.Equal(t, StateAbsent, store.Lookup(leaseUUID).State())

			operationID := requireOperationID(t, "204")
			_, err = store.beginAuthorizedRestore(
				store.CurrentAdmissionBaseline(),
				store.Lookup(sourceUUID).RecordRevision(),
				leaseUUID,
				operationID,
				testBackendRequestSnapshot(t),
				testCallbackPair(operationID),
			)
			require.ErrorIs(t, err, ErrUnprojectedInventoryPositive)
			assert.Equal(t, StateAbsent, store.Lookup(leaseUUID).State(),
				"restore must not create a target attempt across the same inventory race")
		})
	}
}

func TestProjectionCannotOmitSameBackendUntrustedPositive(t *testing.T) {
	const leaseUUID = "00000000-0000-4000-8000-000000000205"
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, store, "backend-b")
	requireConfirmedPlacement(t, store, leaseUUID, "backend-b")
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(
		t, base, newExecutionTestRuntime(&executionTestBackend{name: "backend-b"}),
	)
	reader := &reconciliationSweepReader{}
	reconciliation, err := reconciliationCoordinatorWithReaderForTest(t, execution, reader)
	require.NoError(t, err)
	sweep, err := reconciliation.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	storageID := testBackendStorageID("backend-b")
	require.NoError(t, sweep.RecordProvision("backend-b", storageID, nil))
	require.NoError(t, sweep.RecordRetention("backend-b", storageID, nil))
	require.NoError(t, sweep.RecordUntrusted("backend-b", []string{leaseUUID}))
	require.NoError(t, sweep.SealInventory())

	_, err = sweep.Project(ReconciliationProjection{})
	require.ErrorIs(t, err, ErrInvalidInventoryEvidence)
	require.ErrorContains(t, err, "untrusted reporter")
	assert.Equal(t, StateConfirmed, store.Lookup(leaseUUID).State())
}

func TestProjectionCannotOmitSameBackendRetentionPositive(t *testing.T) {
	const leaseUUID = "00000000-0000-4000-8000-000000000210"
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, store, "backend-b")
	requireConfirmedPlacement(t, store, leaseUUID, "backend-b")
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(
		t, base, newExecutionTestRuntime(&executionTestBackend{name: "backend-b"}),
	)
	reconciliation, err := reconciliationCoordinatorWithReaderForTest(
		t, execution, &reconciliationSweepReader{},
	)
	require.NoError(t, err)
	sweep, err := reconciliation.BeginSweep()
	require.NoError(t, err)
	defer sweep.End()
	storageID := testBackendStorageID("backend-b")
	require.NoError(t, sweep.RecordProvision("backend-b", storageID, nil))
	require.NoError(t, sweep.RecordRetention("backend-b", storageID, []string{leaseUUID}))
	require.NoError(t, sweep.SealInventory())

	_, err = sweep.Project(ReconciliationProjection{})
	require.ErrorIs(t, err, ErrInvalidInventoryEvidence)
	require.ErrorContains(t, err, "omitted outside its causal exclusion")
	assert.Equal(t, StateConfirmed, store.Lookup(leaseUUID).State())
}

func TestUnprojectedPositiveBlocksOwnerAffineSideEffects(t *testing.T) {
	const (
		leaseUUID  = "00000000-0000-4000-8000-000000000206"
		targetUUID = "00000000-0000-4000-8000-000000000207"
	)
	for _, test := range []struct {
		name   string
		record func(*testing.T, *ReconciliationSweep)
	}{
		{
			name: "foreign trusted provision",
			record: func(t *testing.T, sweep *ReconciliationSweep) {
				require.NoError(t, sweep.RecordProvision(
					"backend-b", testBackendStorageID("backend-b"),
					[]backend.ProvisionInfo{{
						LeaseUUID: leaseUUID, BackendName: "backend-b",
						ProviderUUID: freshTestProviderUUID, Tenant: "tenant-test",
					}},
				))
			},
		},
		{
			name: "same backend retention",
			record: func(t *testing.T, sweep *ReconciliationSweep) {
				require.NoError(t, sweep.RecordRetention(
					"backend-a", testBackendStorageID("backend-a"), []string{leaseUUID},
				))
			},
		},
		{
			name: "same backend untrusted",
			record: func(t *testing.T, sweep *ReconciliationSweep) {
				require.NoError(t, sweep.RecordUntrusted("backend-a", []string{leaseUUID}))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
			baseline := requireAdmissionBaseline(t, store, "backend-a", "backend-b")
			scope := requireAdmissionScope(t, store, baseline, "backend-a")
			seedID := requireOperationID(t, "206")
			requestSnapshot := testBackendRequestSnapshot(t)
			attempt, applied, err := store.beginNewAttempt(
				scope, leaseUUID, "backend-a", seedID, PayloadFingerprint{},
				requestSnapshot, testCallbackPair(seedID),
			)
			require.NoError(t, err)
			require.True(t, applied)
			confirmed, err := confirmAttemptForTest(store, attempt)
			require.NoError(t, err)
			require.True(t, confirmed)

			reader := &reconciliationSweepReader{lease: &billingtypes.Lease{
				Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
				State: billingtypes.LEASE_STATE_PENDING,
				Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
			}}
			var provisionCalls, deprovisionCalls int
			backendA := &executionTestBackend{
				name: "backend-a",
				provision: func(context.Context, backend.ProvisionRequest) error {
					provisionCalls++
					return nil
				},
				deprovision: func(context.Context, string) error {
					deprovisionCalls++
					return nil
				},
			}
			backendB := &executionTestBackend{
				name: "backend-b",
				deprovision: func(context.Context, string) error {
					deprovisionCalls++
					return nil
				},
			}
			base, err := store.BindOperationCoordinator(nil)
			require.NoError(t, err)
			execution := bindExecutionForTest(
				t, base, newExecutionTestRuntime(backendA, backendB),
			)
			provision, err := provisionCoordinatorWithReaderForTest(t, execution, reader)
			require.NoError(t, err)
			reconciliation, err := reconciliationCoordinatorWithReaderForTest(t, execution, reader)
			require.NoError(t, err)
			sweep, err := reconciliation.BeginSweep()
			require.NoError(t, err)
			defer sweep.End()
			test.record(t, sweep)

			event, err := NewProvisionEventRequest(leaseUUID, "tenant-test")
			require.NoError(t, err)
			provisionResult := provision.ExecuteCurrentLease(t.Context(), event)
			require.Equal(t, ProvisionEventUncertain, provisionResult.Disposition())
			require.ErrorIs(t, provisionResult.Err(), ErrUnprojectedInventoryPositive)
			assert.Zero(t, provisionCalls)

			maintenanceID, err := maintenanceid.Parse(maintenanceIDA)
			require.NoError(t, err)
			_, err = store.prepareMaintenanceCommand(
				maintenanceID, leaseUUID, MaintenanceCommandRestart, nil,
			)
			require.ErrorIs(t, err, ErrUnprojectedInventoryPositive)

			restoreID := requireOperationID(t, "207")
			_, err = store.beginAuthorizedRestore(
				store.CurrentAdmissionBaseline(), store.Lookup(leaseUUID).RecordRevision(),
				targetUUID, restoreID, requestSnapshot, testCallbackPair(restoreID),
			)
			require.ErrorIs(t, err, ErrUnprojectedInventoryPositive)
			assert.Equal(t, StateAbsent, store.Lookup(targetUUID).State())

			err = provision.Deprovision(t.Context(), leaseUUID)
			require.ErrorIs(t, err, ErrUnprojectedInventoryPositive)
			assert.Zero(t, deprovisionCalls)
			assert.Equal(t, StateConfirmed, store.Lookup(leaseUUID).State())
		})
	}
}

func TestReconciliationSweepProvisionDerivesProviderTargetAndRoutesFromOneCapability(t *testing.T) {
	reader := &reconciliationSweepReader{lease: &billingtypes.Lease{
		Uuid: reconciliationSweepLease, Tenant: "tenant-test",
		ProviderUuid: freshTestProviderUUID,
		State:        billingtypes.LEASE_STATE_PENDING,
		Items: []billingtypes.LeaseItem{{
			SkuUuid: "sku-test", Quantity: 2, ServiceName: "app",
		}},
	}}
	var got backend.ProvisionRequest
	client := &executionTestBackend{name: "backend-a", provision: func(
		_ context.Context,
		request backend.ProvisionRequest,
	) error {
		got = request
		return nil
	}}
	store, _, coordinator := newReconciliationSweepFixture(t, client, reader)
	sweep, projected := completeEmptyReconciliationSweep(t, coordinator)
	defer sweep.End()

	action, disposition, err := projected.ObserveLiveAction(t.Context(), reconciliationSweepLease)
	require.NoError(t, err)
	require.Equal(t, ReconciliationObservationReady, disposition)
	require.True(t, action.Valid())
	defer coordinator.ReleaseAction(action)

	result := coordinator.Provision(t.Context(), action, nil, PayloadFingerprint{})
	require.NoError(t, result.Err())
	assert.True(t, result.Dispatch().CallAccepted())
	assert.Equal(t, "backend-a", result.BackendName())
	assert.Equal(t, reconciliationSweepLease, got.LeaseUUID)
	assert.Equal(t, freshTestProviderUUID, got.ProviderUUID)
	assert.Equal(t, "tenant-test", got.Tenant)
	assert.Len(t, got.Items, 1)
	assert.Contains(t, got.CallbackURL, "https://provider.test/proxy")
	assert.Contains(t, got.CallbackURL, "operation_id=")
	assert.Contains(t, got.LifecycleCallbackURL, "lifecycle_id=")
	assert.Equal(t, StateConfirmed, store.Lookup(reconciliationSweepLease).State())
}

func TestReconciliationSweepRejectsOperationThatCrossedInventoryBoundary(t *testing.T) {
	reader := &reconciliationSweepReader{lease: &billingtypes.Lease{
		Uuid: reconciliationSweepLease, Tenant: "tenant-test",
		ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_PENDING,
	}}
	_, _, coordinator := newReconciliationSweepFixture(
		t, &executionTestBackend{name: "backend-a"}, reader,
	)
	sweep, projected := completeEmptyReconciliationSweep(t, coordinator)
	defer sweep.End()

	claim := coordinator.coordinator.operations.TryClaimLeaseNow(reconciliationSweepLease)
	require.True(t, claim.Acquired())
	require.True(t, coordinator.coordinator.operations.ReleaseLease(claim.Claim()))

	action, disposition, err := projected.ObserveLiveAction(t.Context(), reconciliationSweepLease)
	require.NoError(t, err)
	assert.False(t, action.Valid())
	assert.Equal(t, ReconciliationObservationStale, disposition)
}

func TestReconciliationSweepRejectsForeignProviderAndForeignAction(t *testing.T) {
	readerA := &reconciliationSweepReader{lease: &billingtypes.Lease{
		Uuid: reconciliationSweepLease, Tenant: "tenant-test",
		ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_PENDING,
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
	}}
	_, _, coordinatorA := newReconciliationSweepFixture(
		t, &executionTestBackend{name: "backend-a"}, readerA,
	)
	sweepA, projectedA := completeEmptyReconciliationSweep(t, coordinatorA)
	defer sweepA.End()
	actionA, disposition, err := projectedA.ObserveLiveAction(t.Context(), reconciliationSweepLease)
	require.NoError(t, err)
	require.Equal(t, ReconciliationObservationReady, disposition)

	readerB := &reconciliationSweepReader{lease: readerA.lease}
	_, _, coordinatorB := newReconciliationSweepFixture(
		t, &executionTestBackend{name: "backend-a"}, readerB,
	)
	foreign := coordinatorB.Provision(t.Context(), actionA, nil, PayloadFingerprint{})
	assert.ErrorIs(t, foreign.Err(), ErrReconciliationBoundaryStale)
	require.True(t, coordinatorA.ReleaseAction(actionA))

	readerA.mu.Lock()
	readerA.lease.ProviderUuid = "a6d6790d-d04b-48bd-ad91-675cb7a4b2ed"
	readerA.mu.Unlock()
	newSweep, newProjected := completeEmptyReconciliationSweep(t, coordinatorA)
	defer newSweep.End()
	action, disposition, err := newProjected.ObserveLiveAction(t.Context(), reconciliationSweepLease)
	require.NoError(t, err)
	assert.False(t, action.Valid())
	assert.Equal(t, ReconciliationObservationChainChanged, disposition)
}

func TestReconciliationSweepNewEpochInvalidatesOlderProjection(t *testing.T) {
	reader := &reconciliationSweepReader{lease: &billingtypes.Lease{
		Uuid: reconciliationSweepLease, Tenant: "tenant-test",
		ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_PENDING,
	}}
	_, _, coordinator := newReconciliationSweepFixture(
		t, &executionTestBackend{name: "backend-a"}, reader,
	)
	old, err := coordinator.BeginSweep()
	require.NoError(t, err)
	defer old.End()
	storageID := testBackendStorageID("backend-a")
	require.NoError(t, old.RecordProvision("backend-a", storageID, nil))
	require.NoError(t, old.RecordRetention("backend-a", storageID, nil))
	require.NoError(t, old.SealInventory())

	newer, err := coordinator.BeginSweep()
	require.NoError(t, err)
	defer newer.End()
	_, err = old.Project(ReconciliationProjection{})
	assert.Error(t, err)
}

func TestReconciliationSweepCarriesExcludedPositiveAcrossPartialOutage(t *testing.T) {
	reader := &reconciliationSweepReader{lease: &billingtypes.Lease{
		Uuid: reconciliationSweepLease, Tenant: "tenant-test",
		ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_PENDING,
	}}
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	require.NoError(t, configureBackendTopologyForTest(store, []string{"backend-a", "backend-b"}))
	registry := operation.NewRegistry()
	base, err := newOperationCoordinator(store, registry)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, executionRuntime("backend-a", "backend-b"))
	coordinator, err := reconciliationCoordinatorWithReaderForTest(t, execution, reader)
	require.NoError(t, err)

	// First establish the durable topology baseline with a genuinely empty,
	// complete observation.
	baselineSweep, err := coordinator.BeginSweep()
	require.NoError(t, err)
	for _, backendName := range []string{"backend-a", "backend-b"} {
		storageID := testBackendStorageID(backendName)
		require.NoError(t, baselineSweep.RecordProvision(backendName, storageID, nil))
		require.NoError(t, baselineSweep.RecordRetention(backendName, storageID, nil))
	}
	require.NoError(t, baselineSweep.SealInventory())
	_, err = baselineSweep.Project(ReconciliationProjection{})
	require.NoError(t, err)
	baselineSweep.End()

	// A later positive that cannot cross the projection boundary is remembered
	// by the coordinator even though the stale fact cannot be written to Store.
	excluded, err := coordinator.BeginSweep()
	require.NoError(t, err)
	backendAID := testBackendStorageID("backend-a")
	backendBID := testBackendStorageID("backend-b")
	require.NoError(t, excluded.RecordProvision("backend-a", backendAID, []backend.ProvisionInfo{{
		LeaseUUID: reconciliationSweepLease, BackendName: "backend-a",
		Tenant: "tenant-test", ProviderUUID: freshTestProviderUUID,
	}}))
	require.NoError(t, excluded.RecordRetention("backend-a", backendAID, nil))
	require.NoError(t, excluded.RecordProvision("backend-b", backendBID, nil))
	require.NoError(t, excluded.RecordRetention("backend-b", backendBID, nil))
	require.NoError(t, excluded.SealInventory())
	_, err = excluded.Project(ReconciliationProjection{})
	require.Error(t, err, "omitting the sealed positive must fail projection")
	excluded.End()
	require.True(t, coordinator.AbsenceUntrusted(reconciliationSweepLease))

	// Backend A is transiently down. A valid partial projection from B cannot
	// erase A's excluded positive and therefore cannot authorize recordless work.
	partial, err := coordinator.BeginSweep()
	require.NoError(t, err)
	require.NoError(t, partial.RecordProvision("backend-b", backendBID, nil))
	require.NoError(t, partial.RecordRetention("backend-b", backendBID, nil))
	require.NoError(t, partial.SealInventory())
	partialProjection, err := partial.Project(ReconciliationProjection{})
	require.NoError(t, err)
	assert.False(t, partialProjection.Complete())
	assert.True(t, coordinator.AbsenceUntrusted(reconciliationSweepLease))
	partial.End()

	// Only a fresh, paired positive from the same backend, durably projected to
	// the exact owner, retires the marker.
	resolved, err := coordinator.BeginSweep()
	require.NoError(t, err)
	require.NoError(t, resolved.RecordProvision("backend-a", backendAID, []backend.ProvisionInfo{{
		LeaseUUID: reconciliationSweepLease, BackendName: "backend-a",
		Tenant: "tenant-test", ProviderUUID: freshTestProviderUUID,
	}}))
	require.NoError(t, resolved.RecordRetention("backend-a", backendAID, nil))
	require.NoError(t, resolved.RecordProvision("backend-b", backendBID, nil))
	require.NoError(t, resolved.RecordRetention("backend-b", backendBID, nil))
	require.NoError(t, resolved.SealInventory())
	_, err = resolved.Project(ReconciliationProjection{
		Placements: map[string]string{reconciliationSweepLease: "backend-a"},
	})
	require.NoError(t, err)
	assert.False(t, coordinator.AbsenceUntrusted(reconciliationSweepLease))
	resolved.End()
}
