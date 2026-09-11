package placement

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/inventory"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

func TestDispatchCallClassifiersKeepOnlyTypedRefusalsTerminal(t *testing.T) {
	assert.Equal(t, dispatchCallAccepted, classifyProvisionCall(
		backend.ConservativeProvisionCallOutcome(nil),
	))
	assert.Equal(t, dispatchCallAmbiguous, classifyProvisionCall(
		backend.ConservativeProvisionCallOutcome(errors.Join(
			backend.ErrValidation, context.DeadlineExceeded,
		)),
	), "an arbitrary error tree cannot manufacture refusal evidence")
	assert.Equal(t, dispatchCallRefused,
		classifyProvisionCall(refusedProvisionOutcomeForTest(t)))

	assert.Equal(t, dispatchCallAccepted, classifyRestoreCall(
		backend.ConservativeRestoreCallOutcome(nil),
	))
	assert.Equal(t, dispatchCallAmbiguous, classifyRestoreCall(
		backend.ConservativeRestoreCallOutcome(errors.Join(
			backend.ErrNotRetained, context.DeadlineExceeded,
		)),
	), "an arbitrary error tree cannot manufacture refusal evidence")
	assert.Equal(t, dispatchCallRefused,
		classifyRestoreCall(refusedRestoreOutcomeForTest(t)))
}

type coordinatorFixture struct {
	store       *Store
	coordinator *OperationCoordinator
	initiation  operation.Initiation
}

type timeoutRejecterFunc func(context.Context, []string, string) (uint64, []string, error)

type forgedNotPendingMutationError struct{}

func (forgedNotPendingMutationError) Error() string { return "forged not pending" }
func (forgedNotPendingMutationError) Is(target error) bool {
	return target == billingtypes.ErrLeaseNotPending
}

func (reject timeoutRejecterFunc) RejectLeases(
	ctx context.Context,
	leaseUUIDs []string,
	reason string,
) (uint64, []string, error) {
	return reject(ctx, leaseUUIDs, reason)
}

func timeoutCoordinatorForTest(
	t *testing.T,
	coordinator *OperationCoordinator,
	control any,
) *TimeoutCoordinator {
	t.Helper()
	execution := bindExecutionForTest(t, coordinator, executionRuntime("backend-a"))
	setProviderControlPlaneForTest(t, execution, control)
	timeouts, err := execution.TimeoutCoordinator()
	require.NoError(t, err)
	return timeouts
}

type timeoutControlPlane struct {
	timeoutRejecterFunc
	lease *billingtypes.Lease
}

func (control timeoutControlPlane) GetLease(
	context.Context, string,
) (*billingtypes.Lease, error) {
	if control.lease == nil {
		return nil, nil
	}
	copy := *control.lease
	return &copy, nil
}

type provisionDispatchFixture struct {
	store       *Store
	coordinator *OperationCoordinator
	initiation  operation.Initiation
	dispatch    ProvisionDispatch
	leaseClaim  operation.LeaseClaim
}

type restoreDispatchFixture struct {
	store       *Store
	coordinator *OperationCoordinator
	initiation  operation.Initiation
	dispatch    RestoreDispatch
	bound       restoreBoundDispatch
}

func requireProvisionCall(
	t *testing.T,
	coordinator *OperationCoordinator,
	dispatch ProvisionDispatch,
) provisionCall {
	t.Helper()
	call, began := coordinator.beginProvisionCall(dispatch)
	require.True(t, began)
	require.True(t, call.Valid())
	return call
}

func requireRestoreCall(
	t *testing.T,
	coordinator *OperationCoordinator,
	bound restoreBoundDispatch,
) restoreCall {
	t.Helper()
	call, began := coordinator.beginRestoreCall(bound)
	require.True(t, began)
	require.True(t, call.Valid())
	return call
}

func testProvisionInitiation(
	t *testing.T,
	leaseUUID, tenant, backendName string,
) operation.ProvisionInitiation {
	t.Helper()
	initiation, err := operation.NewProvisionInitiation(
		leaseUUID, tenant,
		[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
		backendName,
	)
	require.NoError(t, err)
	return initiation
}

func testRestoreInitiation(
	t *testing.T,
	leaseUUID, tenant string,
) operation.RestoreInitiation {
	t.Helper()
	initiation, err := operation.NewRestoreInitiation(
		leaseUUID, tenant,
		[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
	)
	require.NoError(t, err)
	return initiation
}

func newProvisionDispatchFixture(t *testing.T, leaseUUID string) provisionDispatchFixture {
	t.Helper()
	store := newTestStore(t)
	requireAdmissionBaseline(t, store, "backend-a")
	registry := operation.NewRegistry()
	coordinator, err := NewOperationCoordinator(store, registry)
	require.NoError(t, err)
	lease := coordinator.operations.TryClaimLeaseNow(leaseUUID)
	require.True(t, lease.Acquired())
	initiated := coordinator.operations.TryInitiateProvisionClaimed(
		lease.Claim(), testProvisionInitiation(t, leaseUUID, "tenant-test", "backend-a"),
	)
	require.True(t, initiated.Started())
	initiation := initiated.Capability()
	attempt := requireTypedAttempt(t, store, leaseUUID, "backend-a", initiation.ID())
	dispatch, err := coordinator.joinProvisionDispatch(initiation, attempt)
	require.NoError(t, err)
	return provisionDispatchFixture{
		store: store, coordinator: coordinator,
		initiation: initiation, dispatch: dispatch, leaseClaim: lease.Claim(),
	}
}

func newRestoreDispatchFixture(t *testing.T, sourceLeaseUUID, targetLeaseUUID string) restoreDispatchFixture {
	t.Helper()
	store := newTestStore(t)
	requireAdmissionBaseline(t, store, "backend-a")
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{sourceLeaseUUID: "backend-a"},
	})
	registry := operation.NewRegistry()
	coordinator, err := NewOperationCoordinator(store, registry)
	require.NoError(t, err)
	lease := coordinator.operations.TryClaimLeaseNow(targetLeaseUUID)
	require.True(t, lease.Acquired())
	initiated := coordinator.operations.TryInitiateRestoreClaimed(
		lease.Claim(), testRestoreInitiation(t, targetLeaseUUID, "tenant-test"),
	)
	require.True(t, initiated.Started())
	initiation := initiated.Capability()
	claim, err := store.beginAuthorizedRestore(
		store.CurrentAdmissionBaseline(), store.Lookup(sourceLeaseUUID).RecordRevision(),
		targetLeaseUUID, initiation.ID(), testBackendRequestSnapshot(t),
		testCallbackPair(initiation.ID()),
	)
	require.NoError(t, err)
	dispatch, err := coordinator.joinRestoreDispatch(initiation, claim)
	require.NoError(t, err)
	bound, boundOK := coordinator.bindRestoreBackend(dispatch)
	require.True(t, boundOK)
	return restoreDispatchFixture{
		store: store, coordinator: coordinator,
		initiation: initiation, dispatch: dispatch, bound: bound,
	}
}

func TestJoinProvisionDispatchDisposesAdmissionInvalidatedByInventory(t *testing.T) {
	const leaseUUID = "lease-provision-join-projection"
	store := newTestStore(t)
	scope := requireAdmissionScope(
		t, store, requireAdmissionBaseline(t, store, "backend-a"), "backend-a",
	)
	registry := operation.NewRegistry()
	coordinator, err := NewOperationCoordinator(store, registry)
	require.NoError(t, err)
	leaseClaim := coordinator.operations.TryClaimLeaseNow(leaseUUID)
	require.True(t, leaseClaim.Acquired())
	defer coordinator.operations.ReleaseLease(leaseClaim.Claim())
	initiated := coordinator.operations.TryInitiateProvisionClaimed(
		leaseClaim.Claim(),
		testProvisionInitiation(t, leaseUUID, "tenant-test", "backend-a"),
	)
	require.True(t, initiated.Started())
	initiation := initiated.Capability()
	attempt, applied, err := store.beginNewAttempt(
		scope, leaseUUID, "backend-a", initiation.ID(), PayloadFingerprint{},
		testBackendRequestSnapshot(t), testCallbackPair(initiation.ID()),
	)
	require.NoError(t, err)
	require.True(t, applied)

	// Model the exact admissible interleaving: inventory positively confirms
	// the operation after its write-ahead Attempt but before the Registry/Store
	// join. The old Attempt token is stale, but the positive successor must win.
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{leaseUUID: "backend-a"},
		lifecycles: map[string]LifecycleObservation{
			leaseUUID: {
				Kind: LifecycleObservationTyped,
				ID:   lifecycleIDFromOperation(t, initiation.ID()),
			},
		},
	})

	dispatch, err := coordinator.joinProvisionDispatch(initiation, attempt)
	require.ErrorIs(t, err, ErrOperationSettlementGenerationUnavailable)
	assert.False(t, dispatch.Valid())
	assert.False(t, coordinator.operations.Contains(leaseUUID),
		"a failed join must not leak a preparing Registry operation")
	placement := store.Lookup(leaseUUID)
	assert.Equal(t, StateConfirmed, placement.State())
	assert.Equal(t, "backend-a", placement.Backend)
	assert.Empty(t, placement.Attempt,
		"disposing the stale admission must preserve its exact positive successor")
}

func TestJoinRestoreDispatchReleasesSourceWhenInventoryInvalidatesTarget(t *testing.T) {
	const (
		sourceLeaseUUID = "lease-restore-join-source"
		targetLeaseUUID = "lease-restore-join-target"
	)
	store := newTestStore(t)
	requireAdmissionBaseline(t, store, "backend-a")
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{sourceLeaseUUID: "backend-a"},
	})
	registry := operation.NewRegistry()
	coordinator, err := NewOperationCoordinator(store, registry)
	require.NoError(t, err)
	leaseClaim := coordinator.operations.TryClaimLeaseNow(targetLeaseUUID)
	require.True(t, leaseClaim.Acquired())
	defer coordinator.operations.ReleaseLease(leaseClaim.Claim())
	initiated := coordinator.operations.TryInitiateRestoreClaimed(
		leaseClaim.Claim(),
		testRestoreInitiation(t, targetLeaseUUID, "tenant-test"),
	)
	require.True(t, initiated.Started())
	initiation := initiated.Capability()
	claim, err := store.beginAuthorizedRestore(
		store.CurrentAdmissionBaseline(), store.Lookup(sourceLeaseUUID).RecordRevision(),
		targetLeaseUUID, initiation.ID(), testBackendRequestSnapshot(t),
		testCallbackPair(initiation.ID()),
	)
	require.NoError(t, err)
	require.Contains(t, store.restoreClaims, sourceLeaseUUID)

	// A matching positive inventory result can promote the target between the
	// atomic restore admission and volatile join. Cleanup must retain that
	// stronger evidence while releasing both ephemeral ownership halves.
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{
			sourceLeaseUUID: "backend-a",
			targetLeaseUUID: "backend-a",
		},
		lifecycles: map[string]LifecycleObservation{
			targetLeaseUUID: {
				Kind: LifecycleObservationTyped,
				ID:   lifecycleIDFromOperation(t, initiation.ID()),
			},
		},
	})

	dispatch, err := coordinator.joinRestoreDispatch(initiation, claim)
	require.ErrorIs(t, err, ErrOperationSettlementGenerationUnavailable)
	assert.False(t, dispatch.Valid())
	assert.False(t, coordinator.operations.Contains(targetLeaseUUID),
		"a failed join must not leak a preparing Registry operation")
	assert.NotContains(t, store.restoreClaims, sourceLeaseUUID,
		"a failed join must release the process-local source reservation")
	target := store.Lookup(targetLeaseUUID)
	assert.Equal(t, StateConfirmed, target.State())
	assert.Equal(t, "backend-a", target.Backend)
	assert.Empty(t, target.Attempt)
}

func newCoordinatorFixture(t *testing.T, leaseUUID string) coordinatorFixture {
	t.Helper()
	store := newTestStore(t)
	requireAdmissionBaseline(t, store, "backend-a")
	registry := operation.NewRegistry()
	coordinator, err := NewOperationCoordinator(store, registry)
	require.NoError(t, err)
	leaseClaim := coordinator.operations.TryClaimLeaseNow(leaseUUID)
	require.True(t, leaseClaim.Acquired())
	requested, err := operation.NewProvisionInitiation(
		leaseUUID, "tenant-test",
		[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
		"backend-a",
	)
	require.NoError(t, err)
	initiationResult := coordinator.operations.TryInitiateProvisionClaimed(
		leaseClaim.Claim(), requested,
	)
	require.True(t, initiationResult.Started())
	initiation := initiationResult.Capability()
	attempt := requireTypedAttempt(t, store, leaseUUID, "backend-a", initiation.ID())
	dispatch, err := coordinator.joinProvisionDispatch(initiation, attempt)
	require.NoError(t, err)
	call := requireProvisionCall(t, coordinator, dispatch)
	// Model the production preservation branch after an accepted backend call
	// whose durable confirmation could not be completed. A synthetic exact
	// callback contender makes the coordinator preserve the Attempt while it
	// releases the call barrier; no raw Registry transition is needed.
	contended := coordinator.tryClaimCallback(leaseUUID, initiation.ID())
	require.True(t, contended.Claimed())
	require.True(t, coordinator.completeProvision(
		call, backend.ConservativeProvisionCallOutcome(nil),
	).Superseded())
	require.True(t, coordinator.releaseCallback(contended.Claim()))
	require.True(t, coordinator.operations.ReleaseLease(leaseClaim.Claim()))
	return coordinatorFixture{
		store: store, coordinator: coordinator, initiation: initiation,
	}
}

func TestOperationCoordinatorBindsEachAuthorityOnce(t *testing.T) {
	store := newTestStore(t)
	registry := operation.NewRegistry()
	coordinator, err := NewOperationCoordinator(store, registry)
	require.NoError(t, err)
	require.True(t, coordinator.Valid())
	reused, err := NewOperationCoordinator(store, registry)
	require.NoError(t, err)
	assert.Same(t, coordinator, reused,
		"the exact composition is idempotent; neither half is rebound")

	_, err = NewOperationCoordinator(store, operation.NewRegistry())
	require.Error(t, err)
	_, err = NewOperationCoordinator(newTestStore(t), registry)
	require.Error(t, err)
}

func TestStoreOwnsOperationRegistryAtBinding(t *testing.T) {
	store := newTestStore(t)
	var counts []int
	coordinator, err := store.BindOperationCoordinator(func(count int) {
		counts = append(counts, count)
	})
	require.NoError(t, err)
	require.True(t, coordinator.Valid())

	lease := coordinator.operations.TryClaimLeaseNow("lease-store-owned-registry")
	require.True(t, lease.Acquired())
	initiated := coordinator.operations.TryInitiateProvisionClaimed(
		lease.Claim(),
		testProvisionInitiation(
			t, "lease-store-owned-registry", "tenant-test", "backend-a",
		),
	)
	require.True(t, initiated.Started())
	assert.Equal(t, []int{1}, counts)
	require.Equal(t, operation.InitiationAborted,
		coordinator.operations.AbortInitiation(initiated.Capability()))
	require.True(t, coordinator.operations.ReleaseLease(lease.Claim()))
	assert.Equal(t, []int{1, 0}, counts)

	_, err = store.BindOperationCoordinator(nil)
	require.ErrorContains(t, err, "already bound")
}

func TestBoundAggregateDoesNotExposeRawOperationAuthority(t *testing.T) {
	observerType := reflect.TypeFor[func(int)]()
	bind, exposed := reflect.TypeFor[*Store]().MethodByName("BindOperationCoordinator")
	require.True(t, exposed)
	require.Equal(t, 2, bind.Type.NumIn())
	assert.Equal(t, observerType, bind.Type.In(1),
		"Store binding must accept configuration, never a caller-owned Registry")

	forbidden := map[reflect.Type]string{
		reflect.TypeFor[*operation.Registry]():           "Registry",
		reflect.TypeFor[operation.SettlementAuthority](): "SettlementAuthority",
		reflect.TypeFor[operation.DispatchOperation]():   "DispatchOperation",
		reflect.TypeFor[operation.Initiation]():          "Initiation",
	}
	for _, aggregate := range []reflect.Type{
		reflect.TypeFor[*Store](),
		reflect.TypeFor[*OperationCoordinator](),
		reflect.TypeFor[*ExecutionCoordinator](),
	} {
		for methodIndex := range aggregate.NumMethod() {
			method := aggregate.Method(methodIndex)
			for input := 1; input < method.Type.NumIn(); input++ {
				if name, found := forbidden[method.Type.In(input)]; found {
					t.Errorf("%s.%s accepts raw operation %s", aggregate, method.Name, name)
				}
			}
			for output := range method.Type.NumOut() {
				if name, found := forbidden[method.Type.Out(output)]; found {
					t.Errorf("%s.%s returns raw operation %s", aggregate, method.Name, name)
				}
			}
		}
	}
}

func TestAttemptRecoveryIsStoreBoundAndOutcomeOwnedByCoordinator(t *testing.T) {
	prepare := func(t *testing.T, leaseUUID string) (coordinatorFixture, operation.LeaseClaim) {
		t.Helper()
		fixture := newCoordinatorFixture(t, leaseUUID)
		claimed := fixture.coordinator.operations.TryClaimCallback(leaseUUID, fixture.initiation.ID())
		require.True(t, claimed.Claimed())
		require.True(t, fixture.coordinator.operations.FinishCallback(claimed.Claim()))
		lease := fixture.coordinator.operations.TryClaimLeaseNow(leaseUUID)
		require.True(t, lease.Acquired())
		t.Cleanup(func() { fixture.coordinator.operations.ReleaseLease(lease.Claim()) })
		return fixture, lease.Claim()
	}
	redeliveryFixture, redeliveryLease := prepare(t, "lease-recovery-facet")
	record := redeliveryFixture.store.Lookup("lease-recovery-facet")
	redeliveryRuntime := executionRuntime("backend-a")
	redeliveryExecution := bindExecutionForTest(t, redeliveryFixture.coordinator, redeliveryRuntime)
	setProviderControlPlaneForTest(t, redeliveryExecution, liveRecoveryReader())
	_, redeliveryRecovery, err := redeliveryFixture.coordinator.bindReconciliation(
		redeliveryExecution.controlPlane, redeliveryRuntime, nil,
		inventoryProjectorForTest(t, redeliveryFixture.store),
	)
	require.NoError(t, err)
	redelivery := redeliveryRecovery.Recover(
		t.Context(), record.RecordRevision(), redeliveryLease, operation.LeaseClaim{},
	)
	require.True(t, redelivery.Accepted(), redelivery.Err())
	assert.Equal(t, StateConfirmed, redeliveryFixture.store.Lookup("lease-recovery-facet").State())

	foreignFixture, foreignLease := prepare(t, "lease-recovery-facet")
	foreignRuntime := executionRuntime("backend-a")
	foreignExecution := bindExecutionForTest(t, foreignFixture.coordinator, foreignRuntime)
	setProviderControlPlaneForTest(t, foreignExecution, liveRecoveryReader())
	_, foreignRecovery, err := foreignFixture.coordinator.bindReconciliation(
		foreignExecution.controlPlane, foreignRuntime, nil,
		inventoryProjectorForTest(t, foreignFixture.store),
	)
	require.NoError(t, err)
	foreign := foreignRecovery.Recover(
		t.Context(), record.RecordRevision(), foreignLease, operation.LeaseClaim{},
	)
	assert.False(t, foreign.Accepted(), "a revision from another store cannot authorize recovery")

	terminalFixture, terminalLease := prepare(t, "lease-terminal-facet")
	terminalRecord := terminalFixture.store.Lookup("lease-terminal-facet")
	terminalRuntime := executionRuntime("backend-a")
	terminalExecution := bindExecutionForTest(t, terminalFixture.coordinator, terminalRuntime)
	setProviderControlPlaneForTest(t, terminalExecution, terminalRecoveryReader())
	_, terminalRecovery, err := terminalFixture.coordinator.bindReconciliation(
		terminalExecution.controlPlane, terminalRuntime, nil,
		inventoryProjectorForTest(t, terminalFixture.store),
	)
	require.NoError(t, err)
	terminal := terminalRecovery.Recover(
		t.Context(), terminalRecord.RecordRevision(), terminalLease, operation.LeaseClaim{},
	)
	require.True(t, terminal.Accepted(), terminal.Err())
	assert.Equal(t, StateConfirmed, terminalFixture.store.Lookup("lease-terminal-facet").State())
}

type pruneLeaseReaderFunc func(context.Context, string) (*billingtypes.Lease, error)

func (read pruneLeaseReaderFunc) GetLease(
	ctx context.Context,
	leaseUUID string,
) (*billingtypes.Lease, error) {
	return read(ctx, leaseUUID)
}

type mutablePruneLeaseReader struct{ read pruneLeaseReaderFunc }

func (reader *mutablePruneLeaseReader) GetLease(
	ctx context.Context,
	leaseUUID string,
) (*billingtypes.Lease, error) {
	return reader.read(ctx, leaseUUID)
}

func liveRecoveryReader() pruneLeaseReaderFunc {
	return func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
		return &billingtypes.Lease{
			Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
			State: billingtypes.LEASE_STATE_PENDING,
		}, nil
	}
}

func terminalRecoveryReader() pruneLeaseReaderFunc {
	return func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
		return &billingtypes.Lease{
			Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
			State: billingtypes.LEASE_STATE_CLOSED,
		}, nil
	}
}

func TestAttemptTargetObservationTreatsEveryAbsenceAsUnknown(t *testing.T) {
	t.Parallel()
	request := testBackendRequestSnapshot(t)
	pending := &billingtypes.Lease{
		Uuid: "lease-observation", Tenant: request.Tenant(), ProviderUuid: request.ProviderUUID(),
		State: billingtypes.LEASE_STATE_PENDING,
	}
	closed := *pending
	closed.State = billingtypes.LEASE_STATE_CLOSED
	wrongTenant := *pending
	wrongTenant.Tenant = "another-tenant"

	tests := []struct {
		name    string
		lease   *billingtypes.Lease
		err     error
		want    any
		wantErr error
	}{
		{
			name:    "typed not found is unknown because ledger history is retained",
			err:     fmt.Errorf("wrapped: %w", billingtypes.ErrLeaseNotFound),
			want:    unknownAttemptTarget{},
			wantErr: billingtypes.ErrLeaseNotFound,
		},
		{
			name:    "joined not found and transient error is unknown",
			err:     errors.Join(billingtypes.ErrLeaseNotFound, context.DeadlineExceeded),
			want:    unknownAttemptTarget{},
			wantErr: context.DeadlineExceeded,
		},
		{
			name:    "non nil lease with not found is unknown",
			lease:   pending,
			err:     billingtypes.ErrLeaseNotFound,
			want:    unknownAttemptTarget{},
			wantErr: billingtypes.ErrLeaseNotFound,
		},
		{
			name: "nil without an error is unknown",
			want: unknownAttemptTarget{},
		},
		{
			name:    "other read error is unknown",
			err:     context.DeadlineExceeded,
			want:    unknownAttemptTarget{},
			wantErr: context.DeadlineExceeded,
		},
		{
			name:  "wrong identity is unknown",
			lease: &wrongTenant,
			want:  unknownAttemptTarget{},
		},
		{
			name:  "pending exact lease is live",
			lease: pending,
			want:  liveAttemptTarget{},
		},
		{
			name:  "closed exact lease is terminal",
			lease: &closed,
			want:  terminalAttemptTarget{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			observation := observeAttemptTarget(
				test.lease, test.err, "lease-observation", request,
			)
			assert.IsType(t, test.want, observation)
			if unknown, ok := observation.(unknownAttemptTarget); ok {
				if test.wantErr != nil {
					assert.ErrorIs(t, unknown.err, test.wantErr)
				} else {
					assert.Error(t, unknown.err)
				}
			}
		})
	}
}

func bindTerminalPrunerForTest(
	t *testing.T,
	coordinator *OperationCoordinator,
	reader PruneLeaseReader,
	backendNames ...string,
) (*TerminalPruner, *inventory.Collector) {
	t.Helper()
	if len(backendNames) == 0 {
		backendNames = []string{"backend-a"}
	}
	projector := inventoryProjectorForTest(t, coordinator.store)
	runtime := executionRuntime(backendNames...)
	execution := bindExecutionForTest(t, coordinator, runtime)
	setProviderControlPlaneForTest(t, execution, reader)
	pruner, _, err := coordinator.bindReconciliation(
		execution.controlPlane, runtime, nil, projector,
	)
	require.NoError(t, err)
	return pruner, projector.collector
}

func newInventoryCollectorForTest(
	t *testing.T,
	backendNames ...string,
) *inventory.Collector {
	t.Helper()
	collector, err := inventory.NewCollector(backendNames)
	require.NoError(t, err)
	return collector
}

func inventoryEvidenceForTest(
	t *testing.T,
	collector *inventory.Collector,
	provisionAnswered []string,
	retentionAnswered []string,
	present []string,
) inventory.Snapshot {
	t.Helper()
	ids := testBackendStorageIDs(append(
		slices.Clone(provisionAnswered), retentionAnswered...,
	)...)
	session := collector.Begin()
	for _, backendName := range provisionAnswered {
		reported := []string(nil)
		if backendName == "backend-a" {
			reported = present
		}
		require.NoError(t, session.RecordProvision(
			backendName, ids[backendName],
			inventoryProvisionRowsForTest(backendName, reported, InventoryProjection{}),
		))
	}
	for _, backendName := range retentionAnswered {
		require.NoError(t, session.RecordRetention(backendName, ids[backendName], nil))
	}
	snapshot, err := session.Seal()
	require.NoError(t, err)
	return snapshot
}

func mintPruneAbsenceForTest(
	t *testing.T,
	store *Store,
	collector *inventory.Collector,
	leaseUUID string,
) (InventoryFence, PruneAbsenceProof) {
	t.Helper()
	fence := store.BeginInventorySession()
	result, err := inventoryProjectorForTest(t, store).Project(fence, InventoryProjection{
		backendStorageIdentities: testBackendStorageIDs("backend-a"),
		AbsenceEvidence: inventoryEvidenceForTest(
			t, collector, []string{"backend-a"}, []string{"backend-a"}, nil,
		),
	})
	require.NoError(t, err)
	proof, ok := result.PruneAbsence(leaseUUID)
	require.True(t, ok)
	return fence, proof
}

func terminalPruneReader() pruneLeaseReaderFunc {
	return func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
		return &billingtypes.Lease{
			Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
			State: billingtypes.LEASE_STATE_CLOSED,
		}, nil
	}
}

func TestProjectInventoryMintsPruneAbsenceOnlyFromDualEndpointEvidence(t *testing.T) {
	for _, test := range []struct {
		name              string
		provisionAnswered []string
		retentionAnswered []string
		present           []string
		wantProof         bool
		wantProjectionErr bool
	}{
		{
			name:              "owner answered both while peer is down",
			provisionAnswered: []string{"backend-a"},
			retentionAnswered: []string{"backend-a"},
			wantProof:         true,
		},
		{
			name:              "provision endpoint only",
			provisionAnswered: []string{"backend-a"},
		},
		{
			name:              "retention endpoint only",
			retentionAnswered: []string{"backend-a"},
		},
		{
			name:              "raw positive omission is rejected",
			provisionAnswered: []string{"backend-a"},
			retentionAnswered: []string{"backend-a"},
			present:           []string{"lease-prune-evidence"},
			wantProjectionErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t)
			requireAdmissionBaseline(t, store, "backend-a", "backend-b")
			projectInventoryForTest(t, store, InventoryProjection{
				Placements: map[string]string{"lease-prune-evidence": "backend-a"},
			})
			coordinator, err := NewOperationCoordinator(store, operation.NewRegistry())
			require.NoError(t, err)
			_, collector := bindTerminalPrunerForTest(
				t, coordinator, terminalPruneReader(), "backend-a", "backend-b",
			)
			fence := store.BeginInventorySession()
			defer store.EndInventorySession(fence)
			result, err := inventoryProjectorForTest(t, store).Project(fence, InventoryProjection{
				backendStorageIdentities: testBackendStorageIDs("backend-a"),
				AbsenceEvidence: inventoryEvidenceForTest(
					t, collector, test.provisionAnswered, test.retentionAnswered, test.present,
				),
			})
			if test.wantProjectionErr {
				require.ErrorIs(t, err, ErrInvalidInventoryEvidence)
				return
			}
			require.NoError(t, err)
			_, proved := result.PruneAbsence("lease-prune-evidence")
			assert.Equal(t, test.wantProof, proved)
		})
	}
}

func TestProjectInventoryRejectsForeignAndStaleInventoryEvidence(t *testing.T) {
	store := newTestStore(t)
	requireAdmissionBaseline(t, store, "backend-a")
	coordinator, err := NewOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	_, collector := bindTerminalPrunerForTest(t, coordinator, terminalPruneReader())
	foreign := newInventoryCollectorForTest(t, "backend-a")
	fence := store.BeginInventorySession()
	defer store.EndInventorySession(fence)

	foreignEvidence := inventoryEvidenceForTest(
		t, foreign, []string{"backend-a"}, []string{"backend-a"}, nil,
	)
	_, err = inventoryProjectorForTest(t, store).Project(fence, InventoryProjection{
		backendStorageIdentities: testBackendStorageIDs("backend-a"),
		AbsenceEvidence:          foreignEvidence,
	})
	require.ErrorIs(t, err, ErrInvalidInventoryEvidence)

	staleEvidence := inventoryEvidenceForTest(
		t, collector, []string{"backend-a"}, []string{"backend-a"}, nil,
	)
	collector.Begin()
	_, err = inventoryProjectorForTest(t, store).Project(fence, InventoryProjection{
		backendStorageIdentities: testBackendStorageIDs("backend-a"),
		AbsenceEvidence:          staleEvidence,
	})
	require.ErrorIs(t, err, ErrInvalidInventoryEvidence)
}

func TestBindReconciliationRejectsForeignInventoryProjector(t *testing.T) {
	store := newTestStore(t)
	requireAdmissionBaseline(t, store, "backend-a", "backend-b")
	coordinator, err := NewOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	runtime := executionRuntime("backend-a", "backend-b")
	execution := bindExecutionForTest(t, coordinator, runtime)
	setProviderControlPlaneForTest(t, execution, terminalPruneReader())
	foreignStore := newTestStore(t)
	requireAdmissionBaseline(t, foreignStore, "backend-a", "backend-b")
	_, _, err = coordinator.bindReconciliation(
		execution.controlPlane, runtime, nil,
		inventoryProjectorForTest(t, foreignStore),
	)
	require.ErrorContains(t, err, "inventory projector")
	_, _, err = coordinator.bindReconciliation(
		execution.controlPlane, runtime, nil,
		inventoryProjectorForTest(t, store),
	)
	require.NoError(t, err, "failed binding must not leave a partial authority")
}

func TestPruneTerminalAbsenceRequiresProjectionAndRegistryProofs(t *testing.T) {
	store := newTestStore(t)
	requireAdmissionBaseline(t, store, "backend-a")
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{"lease-prune-facet": "backend-a"},
	})
	registry := operation.NewRegistry()
	coordinator, err := NewOperationCoordinator(store, registry)
	require.NoError(t, err)
	reader := &mutablePruneLeaseReader{read: terminalPruneReader()}
	pruner, collector := bindTerminalPrunerForTest(t, coordinator, reader)
	fence, proof := mintPruneAbsenceForTest(t, store, collector, "lease-prune-facet")
	defer store.EndInventorySession(fence)
	foreignRegistry := operation.NewRegistry()
	foreignAuthority, err := foreignRegistry.BindSettlementAuthority()
	require.NoError(t, err)
	foreignClaim := foreignAuthority.TryClaimLeaseNow("lease-prune-facet")
	require.True(t, foreignClaim.Acquired())
	foreign := pruner.PruneTerminalAbsence(t.Context(), proof, foreignClaim.Claim())
	assert.Equal(t, PruneDispositionInvalid, foreign.Disposition())
	assert.Equal(t, StateConfirmed, store.Lookup("lease-prune-facet").State())

	claim := coordinator.operations.TryClaimLeaseNow("lease-prune-facet")
	require.True(t, claim.Acquired())
	reader.read = func(_ context.Context, _ string) (*billingtypes.Lease, error) {
		return &billingtypes.Lease{
			Uuid: "another-lease", State: billingtypes.LEASE_STATE_CLOSED,
		}, nil
	}
	mismatched := pruner.PruneTerminalAbsence(t.Context(), proof, claim.Claim())
	assert.Equal(t, PruneDispositionChainError, mismatched.Disposition())
	assert.Error(t, mismatched.Err())
	assert.Equal(t, StateConfirmed, store.Lookup("lease-prune-facet").State())

	reader.read = terminalPruneReader()
	result := pruner.PruneTerminalAbsence(t.Context(), proof, claim.Claim())
	require.True(t, result.Deleted(), result.Err())
	assert.Equal(t, StateAbsent, store.Lookup("lease-prune-facet").State())
}

func TestPruneTerminalAbsenceInvalidatesInventoryAndRecordRaces(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *Store, InventoryFence)
	}{
		{
			name: "ended inventory session",
			mutate: func(_ *testing.T, store *Store, fence InventoryFence) {
				store.EndInventorySession(fence)
			},
		},
		{
			name: "new inventory session",
			mutate: func(t *testing.T, store *Store, _ InventoryFence) {
				newer := store.BeginInventorySession()
				t.Cleanup(func() { store.EndInventorySession(newer) })
			},
		},
		{
			name: "record mutation",
			mutate: func(t *testing.T, store *Store, _ InventoryFence) {
				operationID := requireOperationID(t, "9911")
				_, applied, err := store.beginOwnedAttempt(
					store.CurrentAdmissionBaseline(),
					store.Lookup("lease-prune-race").RecordRevision(),
					"backend-a", operationID, PayloadFingerprint{},
					testBackendRequestSnapshot(t), testCallbackPair(operationID),
				)
				require.NoError(t, err)
				require.True(t, applied)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t)
			requireAdmissionBaseline(t, store, "backend-a")
			projectInventoryForTest(t, store, InventoryProjection{
				Placements: map[string]string{"lease-prune-race": "backend-a"},
			})
			registry := operation.NewRegistry()
			coordinator, err := NewOperationCoordinator(store, registry)
			require.NoError(t, err)
			claim := coordinator.operations.TryClaimLeaseNow("lease-prune-race")
			require.True(t, claim.Acquired())
			var (
				fence InventoryFence
				proof PruneAbsenceProof
			)
			reader := pruneLeaseReaderFunc(func(
				_ context.Context, leaseUUID string,
			) (*billingtypes.Lease, error) {
				// Cross the authority boundary while the coordinator is between
				// its exact chain read and final Store CAS.
				test.mutate(t, store, fence)
				return &billingtypes.Lease{
					Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
					State: billingtypes.LEASE_STATE_CLOSED,
				}, nil
			})
			pruner, collector := bindTerminalPrunerForTest(t, coordinator, reader)
			fence, proof = mintPruneAbsenceForTest(
				t, store, collector, "lease-prune-race",
			)
			defer store.EndInventorySession(fence)
			result := pruner.PruneTerminalAbsence(t.Context(), proof, claim.Claim())
			assert.Equal(t, PruneDispositionEvidenceStale, result.Disposition())
			assert.NotEqual(t, StateAbsent, store.Lookup("lease-prune-race").State())
		})
	}
}

func TestPruneTerminalAbsenceRejectsEvidenceAfterCollectorAdvances(t *testing.T) {
	store := newTestStore(t)
	requireAdmissionBaseline(t, store, "backend-a")
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{"lease-prune-collector-stale": "backend-a"},
	})
	registry := operation.NewRegistry()
	coordinator, err := NewOperationCoordinator(store, registry)
	require.NoError(t, err)
	pruner, collector := bindTerminalPrunerForTest(t, coordinator, terminalPruneReader())
	fence, proof := mintPruneAbsenceForTest(
		t, store, collector, "lease-prune-collector-stale",
	)
	defer store.EndInventorySession(fence)
	claim := coordinator.operations.TryClaimLeaseNow("lease-prune-collector-stale")
	require.True(t, claim.Acquired())

	collector.Begin()
	result := pruner.PruneTerminalAbsence(t.Context(), proof, claim.Claim())
	assert.Equal(t, PruneDispositionEvidenceStale, result.Disposition())
	assert.Equal(t, StateConfirmed, store.Lookup("lease-prune-collector-stale").State())
}

func TestPruneAbsenceProofCannotCrossStoreReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "placement.db")
	store, err := newStoreForTest(path)
	require.NoError(t, err)
	requireAdmissionBaseline(t, store, "backend-a")
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{"lease-prune-reopen": "backend-a"},
	})
	coordinator, err := NewOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	_, collector := bindTerminalPrunerForTest(t, coordinator, terminalPruneReader())
	fence, proof := mintPruneAbsenceForTest(t, store, collector, "lease-prune-reopen")
	store.EndInventorySession(fence)
	require.NoError(t, store.Close())

	reopened, err := newStoreForTest(path)
	require.NoError(t, err)
	defer reopened.Close()
	registry := operation.NewRegistry()
	reopenedCoordinator, err := NewOperationCoordinator(reopened, registry)
	require.NoError(t, err)
	pruner, _ := bindTerminalPrunerForTest(t, reopenedCoordinator, terminalPruneReader())
	claim := reopenedCoordinator.operations.TryClaimLeaseNow("lease-prune-reopen")
	require.True(t, claim.Acquired())
	result := pruner.PruneTerminalAbsence(t.Context(), proof, claim.Claim())
	assert.Equal(t, PruneDispositionInvalid, result.Disposition())
	assert.Equal(t, StateConfirmed, reopened.Lookup("lease-prune-reopen").State())
	_ = coordinator // keep the original binding live until the Store closes.
}

func TestOperationCoordinatorRejectsCrossSourceMetadata(t *testing.T) {
	fixture := newCoordinatorFixture(t, "lease-mismatch")

	// The durable attempt remains exact, but replace the volatile record with a
	// different tenant under the same operation ID to model an accidental splice.
	claimed := fixture.coordinator.operations.TryClaimCallback(
		"lease-mismatch", fixture.initiation.ID(),
	)
	require.True(t, claimed.Claimed())
	require.True(t, fixture.coordinator.operations.FinishCallback(claimed.Claim()))
	recoveryLease := fixture.coordinator.operations.TryClaimLeaseNow("lease-mismatch")
	require.True(t, recoveryLease.Acquired())
	recoveredOperation, err := operation.NewRecoveredProvision(
		"lease-mismatch", "other-tenant",
		[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
		"backend-a",
	)
	require.NoError(t, err)
	recovered := fixture.coordinator.operations.RecoverClaimed(
		recoveryLease.Claim(), fixture.initiation.ID(), recoveredOperation,
	)
	require.Equal(t, operation.RecoveryInstalled, recovered)
	require.True(t, fixture.coordinator.operations.ReleaseLease(recoveryLease.Claim()))

	admission := fixture.coordinator.tryClaimCallback(
		"lease-mismatch", fixture.initiation.ID(),
	)
	assert.Equal(t, operation.SettlementInvalid, admission.Outcome())
	require.ErrorIs(t, admission.Err(), ErrOperationSettlementGenerationUnavailable)
	current, exists := fixture.coordinator.Lookup("lease-mismatch")
	require.True(t, exists)
	assert.Equal(t, operation.SettlementUnclaimed, current.Settlement())
}

func TestConfirmedCallbackRetryUsesDurableConfirmedGeneration(t *testing.T) {
	fixture := newCoordinatorFixture(t, "lease-confirm")
	first := fixture.coordinator.tryClaimCallback("lease-confirm", fixture.initiation.ID())
	require.True(t, first.Claimed())
	require.Equal(t, OperationGenerationAttempt, first.Claim().Generation())

	// Simulate a crash window after the placement commit but before Registry
	// finish: the live claims disappear, while the exact confirmed generation
	// remains durable.
	applied, err := fixture.store.confirmClaimedAttempt(first.Claim().placement)
	require.NoError(t, err)
	require.True(t, applied)
	require.True(t, fixture.coordinator.operations.ReleaseCallback(first.Claim().registry))

	retry := fixture.coordinator.tryClaimCallback("lease-confirm", fixture.initiation.ID())
	require.True(t, retry.Claimed())
	require.Equal(t, OperationGenerationConfirmed, retry.Claim().Generation())
	require.NoError(t, fixture.coordinator.finishConfirmedCallback(retry.Claim()))
	assert.False(t, fixture.coordinator.RuntimeController().Contains("lease-confirm"))
	assert.Equal(t, StateConfirmed, fixture.store.Lookup("lease-confirm").State())
}

func TestRefusalCrashWindowRecoversFromDurableAttempt(t *testing.T) {
	fixture := newCoordinatorFixture(t, "lease-refuse")
	claimed := fixture.coordinator.tryClaimCallback("lease-refuse", fixture.initiation.ID())
	require.True(t, claimed.Claimed())

	// The safe refusal order retires volatile state before clearing durable
	// evidence. Model a crash at that exact boundary by dropping the process-local
	// Store claim while leaving the Attempt intact.
	require.True(t, fixture.coordinator.operations.FinishCallback(claimed.Claim().registry))
	require.True(t, fixture.store.releaseAttemptClaim(claimed.Claim().placement))
	assert.False(t, fixture.coordinator.RuntimeController().Contains("lease-refuse"))
	assert.Equal(t, StateAttempting, fixture.store.Lookup("lease-refuse").State())

	recovery := fixture.coordinator.tryClaimRecoveryCallback(
		"lease-refuse", fixture.initiation.ID(),
	)
	require.True(t, recovery.Claimed())
	require.NoError(t, fixture.coordinator.finishRefusedRecoveryCallback(recovery.Claim()))
	assert.Equal(t, StateAbsent, fixture.store.Lookup("lease-refuse").State())
}

func TestTimeoutCanOnlyPreserveAmbiguousAttempt(t *testing.T) {
	fixture := newCoordinatorFixture(t, "lease-timeout")
	timeouts := timeoutCoordinatorForTest(t, fixture.coordinator, timeoutRejecterFunc(func(
		_ context.Context, leaseUUIDs []string, reason string,
	) (uint64, []string, error) {
		require.Equal(t, []string{"lease-timeout"}, leaseUUIDs)
		require.Equal(t, "callback timeout", reason)
		return 1, []string{"tx-timeout"}, nil
	}))
	candidates := timeouts.TimedOut(-time.Nanosecond)
	require.Len(t, candidates, 1)
	result := timeouts.Settle(t.Context(), candidates[0])
	require.Equal(t, TimeoutRejected, result.Disposition())
	require.NoError(t, result.Err())
	require.Equal(t, uint64(1), result.Rejected())
	require.Equal(t, []string{"tx-timeout"}, result.TxHashes())
	assert.False(t, fixture.coordinator.RuntimeController().Contains("lease-timeout"))
	remaining := fixture.store.Lookup("lease-timeout")
	assert.Equal(t, StateAttempting, remaining.State())
	assert.Equal(t, fixture.initiation.ID(), remaining.AttemptMetadata().OperationID())
}

func TestTimeoutNotFoundPreservesAttemptForRetry(t *testing.T) {
	const leaseUUID = "lease-timeout-terminal-absence"
	fixture := newCoordinatorFixture(t, leaseUUID)
	rejecter := timeoutRejecterFunc(func(
		_ context.Context, leaseUUIDs []string, reason string,
	) (uint64, []string, error) {
		require.Equal(t, []string{leaseUUID}, leaseUUIDs)
		require.Equal(t, "callback timeout", reason)
		return 0, nil, billingtypes.ErrLeaseNotFound
	})
	timeouts := timeoutCoordinatorForTest(t, fixture.coordinator, rejecter)
	candidates := timeouts.TimedOut(-time.Nanosecond)
	require.Len(t, candidates, 1)
	result := timeouts.Settle(t.Context(), candidates[0])
	require.Equal(t, TimeoutRetry, result.Disposition())
	require.ErrorIs(t, result.Err(), billingtypes.ErrLeaseNotFound)
	assert.True(t, fixture.coordinator.RuntimeController().Contains(leaseUUID))
	assert.Equal(t, StateAttempting, fixture.store.Lookup(leaseUUID).State(),
		"no-record cannot authorize evidence retirement on a wrong, reset, or lagging endpoint")
}

func TestTimeoutRetainsAlreadyConfirmedOwner(t *testing.T) {
	fixture := newCoordinatorFixture(t, "lease-timeout-confirmed")
	placementClaim, claimed, err := fixture.store.claimAttempt(
		"lease-timeout-confirmed", fixture.initiation.ID(),
	)
	require.NoError(t, err)
	require.True(t, claimed)
	confirmed, err := fixture.store.confirmClaimedAttempt(placementClaim)
	require.NoError(t, err)
	require.True(t, confirmed)

	timeouts := timeoutCoordinatorForTest(t, fixture.coordinator, timeoutControlPlane{
		timeoutRejecterFunc: timeoutRejecterFunc(func(
			context.Context, []string, string,
		) (uint64, []string, error) {
			return 0, nil, billingtypes.ErrLeaseNotPending
		}),
		lease: &billingtypes.Lease{
			Uuid: "lease-timeout-confirmed", Tenant: "tenant-test",
			ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_ACTIVE,
		},
	})
	candidates := timeouts.TimedOut(-time.Nanosecond)
	require.Len(t, candidates, 1)
	result := timeouts.Settle(t.Context(), candidates[0])
	require.Equal(t, TimeoutLeaseTerminal, result.Disposition())
	require.ErrorIs(t, result.Err(), billingtypes.ErrLeaseNotPending)
	assert.False(t, fixture.coordinator.RuntimeController().Contains("lease-timeout-confirmed"))
	assert.Equal(t, StateConfirmed, fixture.store.Lookup("lease-timeout-confirmed").State())
}

func TestTimeoutJoinedTerminalAndTransientErrorPreservesAttemptForRetry(t *testing.T) {
	fixture := newCoordinatorFixture(t, "lease-timeout-joined-error")
	timeouts := timeoutCoordinatorForTest(t, fixture.coordinator, timeoutRejecterFunc(func(
		context.Context, []string, string,
	) (uint64, []string, error) {
		return 0, nil, errors.Join(
			billingtypes.ErrLeaseNotPending,
			context.DeadlineExceeded,
		)
	}))
	candidates := timeouts.TimedOut(-time.Nanosecond)
	require.Len(t, candidates, 1)
	result := timeouts.Settle(t.Context(), candidates[0])
	require.Equal(t, TimeoutRetry, result.Disposition())
	require.ErrorIs(t, result.Err(), context.DeadlineExceeded)
	assert.True(t, fixture.coordinator.RuntimeController().Contains("lease-timeout-joined-error"))
	assert.Equal(t, StateAttempting, fixture.store.Lookup("lease-timeout-joined-error").State())
}

func TestTimeoutCustomIsMutationErrorCannotSettleWithoutPositiveObservation(t *testing.T) {
	const leaseUUID = "lease-timeout-forged-error"
	fixture := newCoordinatorFixture(t, leaseUUID)
	timeouts := timeoutCoordinatorForTest(t, fixture.coordinator, timeoutRejecterFunc(func(
		context.Context, []string, string,
	) (uint64, []string, error) {
		return 0, nil, forgedNotPendingMutationError{}
	}))
	candidates := timeouts.TimedOut(-time.Nanosecond)
	require.Len(t, candidates, 1)
	result := timeouts.Settle(t.Context(), candidates[0])
	require.Equal(t, TimeoutRetry, result.Disposition())
	require.ErrorIs(t, result.Err(), billingtypes.ErrLeaseNotPending)
	assert.True(t, fixture.coordinator.RuntimeController().Contains(leaseUUID))
	assert.Equal(t, StateAttempting, fixture.store.Lookup(leaseUUID).State(),
		"an error-controlled Is method cannot manufacture settlement authority")
}

func TestTimeoutPositiveObservationConvergesRegardlessOfMutationErrorShape(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "plain", err: errors.New("ambiguous mutation result")},
		{name: "custom Is", err: forgedNotPendingMutationError{}},
		{name: "joined terminal and transient", err: errors.Join(
			billingtypes.ErrLeaseNotPending, context.DeadlineExceeded,
		)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			leaseUUID := "lease-timeout-positive-" + test.name
			fixture := newCoordinatorFixture(t, leaseUUID)
			timeouts := timeoutCoordinatorForTest(t, fixture.coordinator, timeoutControlPlane{
				timeoutRejecterFunc: func(
					context.Context, []string, string,
				) (uint64, []string, error) {
					return 0, nil, test.err
				},
				lease: &billingtypes.Lease{
					Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
					State: billingtypes.LEASE_STATE_CLOSED,
				},
			})
			candidates := timeouts.TimedOut(-time.Nanosecond)
			require.Len(t, candidates, 1)
			result := timeouts.Settle(t.Context(), candidates[0])
			require.Equal(t, TimeoutLeaseTerminal, result.Disposition(), result.Err())
			require.ErrorIs(t, result.Err(), test.err)
			assert.False(t, fixture.coordinator.RuntimeController().Contains(leaseUUID))
			assert.Equal(t, StateAttempting, fixture.store.Lookup(leaseUUID).State(),
				"timeout convergence must retain ambiguous physical attempt evidence")
		})
	}
}

func TestTimeoutCandidateCannotCrossCoordinator(t *testing.T) {
	first := newCoordinatorFixture(t, "lease-first")
	second := newCoordinatorFixture(t, "lease-second")
	firstTimeouts := timeoutCoordinatorForTest(t, first.coordinator, timeoutRejecterFunc(func(
		context.Context, []string, string,
	) (uint64, []string, error) {
		return 1, nil, nil
	}))
	var foreignCalls int
	secondTimeouts := timeoutCoordinatorForTest(t, second.coordinator, timeoutRejecterFunc(func(
		context.Context, []string, string,
	) (uint64, []string, error) {
		foreignCalls++
		return 1, nil, nil
	}))
	candidate := firstTimeouts.TimedOut(-time.Nanosecond)[0]
	result := secondTimeouts.Settle(t.Context(), candidate)
	assert.Equal(t, TimeoutSkipped, result.Disposition())
	assert.Zero(t, foreignCalls, "a foreign candidate cannot select this coordinator's rejecter")
}

func TestTimeoutCoordinatorBindsRejecterOnceAndPanicReleasesExactClaim(t *testing.T) {
	fixture := newCoordinatorFixture(t, "lease-timeout-panic")
	panics := true
	rejecter := timeoutRejecterFunc(func(
		context.Context, []string, string,
	) (uint64, []string, error) {
		if panics {
			panic("chain panic")
		}
		return 1, nil, nil
	})
	execution := bindExecutionForTest(t, fixture.coordinator, executionRuntime("backend-a"))
	setProviderControlPlaneForTest(t, execution, rejecter)
	timeouts, err := execution.TimeoutCoordinator()
	require.NoError(t, err)
	_, err = execution.TimeoutCoordinator()
	require.Error(t, err)

	candidate := timeouts.TimedOut(-time.Nanosecond)[0]
	result := timeouts.Settle(t.Context(), candidate)
	require.Equal(t, TimeoutRetry, result.Disposition())
	require.ErrorContains(t, result.Err(), "chain panic")
	assert.True(t, fixture.coordinator.RuntimeController().Contains("lease-timeout-panic"))

	panics = false
	result = timeouts.Settle(t.Context(), candidate)
	require.Equal(t, TimeoutRejected, result.Disposition(), result.Err())
	assert.False(t, fixture.coordinator.RuntimeController().Contains("lease-timeout-panic"),
		"panic unwinding must release both exact claims for retry")
}

func TestProvisionDispatchDefinitiveRefusalIsDurableBeforeRegistryRetirement(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-dispatch-refuse")
	call := requireProvisionCall(t, fixture.coordinator, fixture.dispatch)

	result := fixture.coordinator.completeProvision(call, refusedProvisionOutcomeForTest(t))
	require.True(t, result.Applied(), result.Err())
	assert.Equal(t, StateAbsent, fixture.store.Lookup("lease-dispatch-refuse").State())
	assert.False(t, fixture.coordinator.RuntimeController().Contains("lease-dispatch-refuse"))
}

func TestAbortProvisionDispatchPreservesAttemptAndRetiresPreparingRegistryOnWriteFailure(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-unsent-write-failure")
	require.NoError(t, fixture.store.db.Close())

	result := fixture.coordinator.abortProvisionDispatch(fixture.dispatch)
	require.Error(t, result.Err())
	assert.Equal(t, DispatchPreserved, result.Disposition())
	assert.False(t, fixture.coordinator.RuntimeController().Contains("lease-unsent-write-failure"),
		"a failed durable pre-call cleanup must not wedge an ephemeral Preparing row")
	remaining := fixture.store.Lookup("lease-unsent-write-failure")
	assert.Equal(t, StateAttempting, remaining.State())
	assert.Equal(t, fixture.initiation.ID(), remaining.AttemptMetadata().OperationID())
	assert.True(t, fixture.coordinator.operations.ReleaseLease(fixture.leaseClaim))
}

func TestProvisionDispatchTypestateSeparatesPreCallAbortFromCallCompletion(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-dispatch-typestate")
	call := requireProvisionCall(t, fixture.coordinator, fixture.dispatch)

	abort := fixture.coordinator.abortProvisionDispatch(fixture.dispatch)
	require.Error(t, abort.Err())
	assert.Equal(t, DispatchInvalid, abort.Disposition())
	record, exists := fixture.coordinator.Lookup("lease-dispatch-typestate")
	require.True(t, exists)
	assert.Equal(t, operation.PhaseCalling, record.Phase())

	completed := fixture.coordinator.completeProvision(
		call, backend.ConservativeProvisionCallOutcome(nil),
	)
	require.True(t, completed.Applied(), completed.Err())
	assert.Equal(t, StateConfirmed, fixture.store.Lookup("lease-dispatch-typestate").State())
}

func TestProvisionDispatchDurableRefusalSurvivesRestart(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-dispatch-crash")
	call := requireProvisionCall(t, fixture.coordinator, fixture.dispatch)
	dbPath := fixture.store.db.Path()
	result := fixture.coordinator.completeProvision(call, refusedProvisionOutcomeForTest(t))
	require.True(t, result.Applied(), result.Err())
	assert.False(t, fixture.coordinator.RuntimeController().Contains("lease-dispatch-crash"))
	assert.Equal(t, StateAbsent, fixture.store.Lookup("lease-dispatch-crash").State())
	require.NoError(t, fixture.store.Close())

	restarted, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = restarted.Close() })
	assert.Equal(t, StateAbsent, restarted.Lookup("lease-dispatch-crash").State())
	_, claimed, err := restarted.claimAttempt("lease-dispatch-crash", fixture.initiation.ID())
	require.NoError(t, err)
	assert.False(t, claimed, "restart must not recover an attempt known to be unsent/refused")
}

func TestProvisionDispatchDurableConfirmationRecoversAcrossRestart(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-dispatch-confirm-crash")
	call := requireProvisionCall(t, fixture.coordinator, fixture.dispatch)
	dbPath := fixture.store.db.Path()
	result := fixture.coordinator.completeProvision(
		call, backend.ConservativeProvisionCallOutcome(nil),
	)
	require.True(t, result.Applied(), result.Err())
	assert.Equal(t, StateConfirmed, fixture.store.Lookup("lease-dispatch-confirm-crash").State())
	assert.True(t, fixture.coordinator.RuntimeController().Contains("lease-dispatch-confirm-crash"))
	require.NoError(t, fixture.store.Close())

	restarted, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = restarted.Close() })
	restartedCoordinator, err := NewOperationCoordinator(restarted, operation.NewRegistry())
	require.NoError(t, err)
	recovery := restartedCoordinator.tryClaimRecoveryCallback(
		"lease-dispatch-confirm-crash", fixture.initiation.ID(),
	)
	require.True(t, recovery.Claimed())
	require.Equal(t, OperationGenerationConfirmed, recovery.Claim().Generation())
	require.NoError(t, restartedCoordinator.finishConfirmedRecoveryCallback(recovery.Claim()))
	assert.Equal(t, StateConfirmed, restarted.Lookup("lease-dispatch-confirm-crash").State())
}

func TestProvisionDispatchAmbiguousPreservesAttemptForRedelivery(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-dispatch-ambiguous")
	call := requireProvisionCall(t, fixture.coordinator, fixture.dispatch)

	result := fixture.coordinator.completeProvision(
		call, backend.ConservativeProvisionCallOutcome(backend.ErrMalformedErrorBody),
	)
	require.Equal(t, DispatchPreserved, result.Disposition())
	require.NoError(t, result.Err())
	assert.False(t, fixture.coordinator.RuntimeController().Contains("lease-dispatch-ambiguous"))
	remaining := fixture.store.Lookup("lease-dispatch-ambiguous")
	assert.Equal(t, StateAttempting, remaining.State())
	assert.Equal(t, fixture.initiation.ID(), remaining.AttemptMetadata().OperationID())
}

func TestProvisionDispatchInlineCallbackSupersedesSynchronousRefusal(t *testing.T) {
	fixture := newProvisionDispatchFixture(t, "lease-dispatch-inline")
	call := requireProvisionCall(t, fixture.coordinator, fixture.dispatch)
	callback := fixture.coordinator.tryClaimCallback(
		"lease-dispatch-inline", fixture.initiation.ID(),
	)
	require.True(t, callback.Claimed())
	require.NoError(t, fixture.coordinator.finishConfirmedCallback(callback.Claim()))

	result := fixture.coordinator.completeProvision(call, refusedProvisionOutcomeForTest(t))
	require.True(t, result.Superseded(), result.Err())
	assert.Equal(t, StateConfirmed, fixture.store.Lookup("lease-dispatch-inline").State())
	assert.False(t, fixture.coordinator.RuntimeController().Contains("lease-dispatch-inline"))
}

func TestRestoreDispatchDurableRefusalSurvivesRestart(t *testing.T) {
	fixture := newRestoreDispatchFixture(t, "restore-source", "restore-target-crash")
	call := requireRestoreCall(t, fixture.coordinator, fixture.bound)
	dbPath := fixture.store.db.Path()
	result := fixture.coordinator.completeRestore(call, refusedRestoreOutcomeForTest(t))
	require.True(t, result.Applied(), result.Err())
	assert.False(t, fixture.coordinator.RuntimeController().Contains("restore-target-crash"))
	assert.Equal(t, StateAbsent, fixture.store.Lookup("restore-target-crash").State())
	assert.Equal(t, StateConfirmed, fixture.store.Lookup("restore-source").State())
	require.NoError(t, fixture.store.Close())

	restarted, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = restarted.Close() })
	assert.Equal(t, StateAbsent, restarted.Lookup("restore-target-crash").State())
	_, claimed, err := restarted.claimAttempt(
		"restore-target-crash", fixture.initiation.ID(),
	)
	require.NoError(t, err)
	assert.False(t, claimed, "restart must not recover a restore known to be unsent/refused")
}

func TestAbortRestoreDispatchPreservesAttemptAndRetiresPreparingRegistryOnWriteFailure(t *testing.T) {
	fixture := newRestoreDispatchFixture(t, "restore-source-unsent", "restore-target-unsent")
	require.NoError(t, fixture.store.db.Close())

	result := fixture.coordinator.abortRestoreDispatch(fixture.dispatch)
	require.Error(t, result.Err())
	assert.Equal(t, DispatchPreserved, result.Disposition())
	assert.False(t, fixture.coordinator.RuntimeController().Contains("restore-target-unsent"),
		"a failed durable pre-call cleanup must not wedge an ephemeral Preparing row")
	remaining := fixture.store.Lookup("restore-target-unsent")
	assert.Equal(t, StateAttempting, remaining.State())
	assert.Equal(t, fixture.initiation.ID(), remaining.AttemptMetadata().OperationID())
}

func TestRestoreDispatchDurableConfirmationRecoversAcrossRestart(t *testing.T) {
	fixture := newRestoreDispatchFixture(t, "restore-source-confirm", "restore-target-confirm-crash")
	call := requireRestoreCall(t, fixture.coordinator, fixture.bound)
	dbPath := fixture.store.db.Path()
	result := fixture.coordinator.completeRestore(
		call, backend.ConservativeRestoreCallOutcome(nil),
	)
	require.True(t, result.Applied(), result.Err())
	assert.Equal(t, StateConfirmed, fixture.store.Lookup("restore-target-confirm-crash").State())
	assert.Equal(t, StateConfirmed, fixture.store.Lookup("restore-source-confirm").State())
	require.NoError(t, fixture.store.Close())

	restarted, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = restarted.Close() })
	restartedCoordinator, err := NewOperationCoordinator(restarted, operation.NewRegistry())
	require.NoError(t, err)
	recovery := restartedCoordinator.tryClaimRecoveryCallback(
		"restore-target-confirm-crash", fixture.initiation.ID(),
	)
	require.True(t, recovery.Claimed())
	require.Equal(t, OperationGenerationConfirmed, recovery.Claim().Generation())
	require.NoError(t, restartedCoordinator.finishConfirmedRecoveryCallback(recovery.Claim()))
	assert.Equal(t, StateConfirmed, restarted.Lookup("restore-target-confirm-crash").State())
}

func TestRestoreDispatchAmbiguousPreservesAttemptAndReleasesSource(t *testing.T) {
	fixture := newRestoreDispatchFixture(t, "restore-source-ambiguous", "restore-target-ambiguous")
	call := requireRestoreCall(t, fixture.coordinator, fixture.bound)

	result := fixture.coordinator.completeRestore(
		call, backend.ConservativeRestoreCallOutcome(backend.ErrMalformedErrorBody),
	)
	require.Equal(t, DispatchPreserved, result.Disposition())
	require.NoError(t, result.Err())
	assert.False(t, fixture.coordinator.RuntimeController().Contains("restore-target-ambiguous"))
	remaining := fixture.store.Lookup("restore-target-ambiguous")
	assert.Equal(t, StateAttempting, remaining.State())
	assert.Equal(t, fixture.initiation.ID(), remaining.AttemptMetadata().OperationID())

	second, err := fixture.store.beginAuthorizedRestore(
		fixture.store.CurrentAdmissionBaseline(),
		fixture.store.Lookup("restore-source-ambiguous").RecordRevision(),
		"restore-target-after-ambiguous", requireOperationID(t, "998"),
		testBackendRequestSnapshot(t), testCallbackPair(requireOperationID(t, "998")),
	)
	require.NoError(t, err)
	assert.True(t, second.Valid(), "ambiguous dispatch must release only its source reservation")
}

func TestRestoreDispatchInlineCallbackSupersedesSynchronousRefusal(t *testing.T) {
	fixture := newRestoreDispatchFixture(t, "restore-source", "restore-target-inline")
	call := requireRestoreCall(t, fixture.coordinator, fixture.bound)
	callback := fixture.coordinator.tryClaimCallback(
		"restore-target-inline", fixture.initiation.ID(),
	)
	require.True(t, callback.Claimed())
	require.NoError(t, fixture.coordinator.finishConfirmedCallback(callback.Claim()))

	result := fixture.coordinator.completeRestore(call, refusedRestoreOutcomeForTest(t))
	require.True(t, result.Superseded(), result.Err())
	assert.Equal(t, StateConfirmed, fixture.store.Lookup("restore-target-inline").State())
	assert.False(t, fixture.coordinator.RuntimeController().Contains("restore-target-inline"))
	// Callback ownership must not leak the source-side synchronous exclusion.
	second, err := fixture.store.beginAuthorizedRestore(
		fixture.store.CurrentAdmissionBaseline(),
		fixture.store.Lookup("restore-source").RecordRevision(),
		"restore-target-next", requireOperationID(t, "999"),
		testBackendRequestSnapshot(t), testCallbackPair(requireOperationID(t, "999")),
	)
	require.NoError(t, err)
	assert.True(t, second.Valid())
}
