package shared

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

type blockingScopeRecoveryMutation func(context.Context) error

func failedOperationReceiptForRecoveryScopeTest(
	t *testing.T,
	settlement *OperationSettlement,
) FailedOperationReceipt {
	t.Helper()
	claim := beginHandoffOperation(
		t, settlement, testOperationIntentSpec(t, "recovery-scope-in-flight"),
	)
	uncommitted := commitHandoffRefusal(t, settlement, claim)
	_, err := settlement.resolveOperationFailure(uncommitted, "definitive refusal")
	require.NoError(t, err)
	receipts, err := settlement.ListFailedOperationReceipts()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	return receipts[0]
}

func bindBlockingScopeRecoveryMutation(
	t *testing.T,
	settlement *OperationSettlement,
	entered chan<- struct{},
	proceed <-chan struct{},
) {
	t.Helper()
	err := BindOperationSubstrateExecutor(
		settlement,
		func(ctx context.Context, _ string) (context.Context, func(), error) {
			return ctx, func() {}, nil
		},
		func(context.Context, string, error) error { return nil },
		func(
			runner substratemutation.Runner,
			_ OperationPhysicalSubject,
		) blockingScopeRecoveryMutation {
			return func(ctx context.Context) error {
				return runner.Step(ctx, "blocking recovery scope mutation", func(ctx context.Context) error {
					entered <- struct{}{}
					select {
					case <-proceed:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
			}
		},
		func(
			ctx context.Context,
			mutation blockingScopeRecoveryMutation,
			_ OperationPhysicalSubject,
		) error {
			return mutation(ctx)
		},
		func(
			_ context.Context,
			subject OperationPhysicalSubject,
		) (OperationPhysicalEvidence, error) {
			return NewOperationFailedReceiptAbsent(subject)
		},
	)
	require.NoError(t, err)
}

func newObservedRecoveryCoordinator(
	t *testing.T,
	settlement *OperationSettlement,
	released chan<- struct{},
) *RecoveryCoordinator {
	t.Helper()
	coordinator, err := NewRecoveryCoordinator(RecoveryCoordinatorConfig{
		Operations: settlement,
		ExcludeLease: func(_ context.Context, _ string, run func() error) (bool, error) {
			err := run()
			close(released)
			return true, err
		},
	})
	require.NoError(t, err)
	return coordinator
}

func TestLeaseRecoveryScopeIsExactAndCallbackLifetime(t *testing.T) {
	leaseUUID := testLeaseUUID("recovery-scope-lifetime")
	coordinator := newTestRecoveryCoordinator(t, nil, nil, nil)
	var escaped LeaseRecoveryScope

	acquired, err := coordinator.WithLease(
		context.Background(), leaseUUID,
		func(scope LeaseRecoveryScope) error {
			escaped = scope
			assert.True(t, scope.validFor(coordinator, leaseUUID))
			assert.False(t, scope.validFor(coordinator, testLeaseUUID("another-recovery-lease")))
			return nil
		},
	)
	require.NoError(t, err)
	require.True(t, acquired)
	assert.False(t, escaped.validFor(coordinator, leaseUUID),
		"a copied recovery scope must be revoked when its callback returns")
}

func TestLeaseRecoveryScopeIsRevokedWhenCallbackPanics(t *testing.T) {
	leaseUUID := testLeaseUUID("recovery-scope-panic")
	coordinator := newTestRecoveryCoordinator(t, nil, nil, nil)
	var escaped LeaseRecoveryScope

	func() {
		defer func() { require.Equal(t, "boom", recover()) }()
		_, _ = coordinator.WithLease(
			context.Background(), leaseUUID,
			func(scope LeaseRecoveryScope) error {
				escaped = scope
				panic("boom")
			},
		)
	}()
	assert.False(t, escaped.validFor(coordinator, leaseUUID),
		"panic must not leak recovery authority")
}

func TestLeaseRecoveryScopeJoinsInFlightAuthorizedConsumerBeforeExclusionRelease(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	receipt := failedOperationReceiptForRecoveryScopeTest(t, stores.settlement)
	entered := make(chan struct{}, 1)
	proceed := make(chan struct{})
	bindBlockingScopeRecoveryMutation(t, stores.settlement, entered, proceed)
	exclusionReleased := make(chan struct{})
	coordinator := newObservedRecoveryCoordinator(
		t, stores.settlement, exclusionReleased,
	)

	var escaped LeaseRecoveryScope
	callbackReturned := make(chan struct{})
	consumerDone := make(chan error, 1)
	withLeaseDone := make(chan error, 1)
	go func() {
		_, err := coordinator.WithLease(
			context.Background(), receipt.LeaseUUID(),
			func(scope LeaseRecoveryScope) error {
				escaped = scope
				defer close(callbackReturned)
				go func() {
					consumerDone <- stores.settlement.CleanupFailedOperationReceipt(
						context.Background(), scope, receipt,
					)
				}()
				<-entered
				return nil
			},
		)
		withLeaseDone <- err
	}()
	<-callbackReturned
	require.Eventually(t, func() bool {
		escaped.state.mu.Lock()
		defer escaped.state.mu.Unlock()
		return !escaped.state.active && escaped.state.inFlight == 1
	}, time.Second, time.Millisecond)
	select {
	case <-exclusionReleased:
		t.Fatal("live-work exclusion released while an authorized recovery mutation was in flight")
	default:
	}

	close(proceed)
	require.NoError(t, <-consumerDone)
	require.NoError(t, <-withLeaseDone)
	<-exclusionReleased

	err := stores.settlement.CleanupFailedOperationReceipt(
		context.Background(), escaped, receipt,
	)
	require.ErrorContains(t, err, "exact lease-quiescence authority",
		"a copied scope must stay revoked after its in-flight consumer drains")
}

func TestLeaseRecoveryScopeCancellationDrainsConsumerBeforeExclusionRelease(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	receipt := failedOperationReceiptForRecoveryScopeTest(t, stores.settlement)
	entered := make(chan struct{}, 1)
	bindBlockingScopeRecoveryMutation(t, stores.settlement, entered, nil)
	exclusionReleased := make(chan struct{})
	coordinator := newObservedRecoveryCoordinator(
		t, stores.settlement, exclusionReleased,
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var escaped LeaseRecoveryScope
	callbackReturned := make(chan struct{})
	consumerDone := make(chan error, 1)
	withLeaseDone := make(chan error, 1)
	go func() {
		_, err := coordinator.WithLease(
			ctx, receipt.LeaseUUID(),
			func(scope LeaseRecoveryScope) error {
				escaped = scope
				defer close(callbackReturned)
				go func() {
					consumerDone <- stores.settlement.CleanupFailedOperationReceipt(
						ctx, scope, receipt,
					)
				}()
				<-entered
				return nil
			},
		)
		withLeaseDone <- err
	}()
	<-callbackReturned
	require.Eventually(t, func() bool {
		escaped.state.mu.Lock()
		defer escaped.state.mu.Unlock()
		return !escaped.state.active && escaped.state.inFlight == 1
	}, time.Second, time.Millisecond)
	select {
	case <-exclusionReleased:
		t.Fatal("cancellation test released exclusion before the context was canceled")
	default:
	}

	cancel()
	require.ErrorIs(t, <-consumerDone, context.Canceled)
	require.NoError(t, <-withLeaseDone)
	<-exclusionReleased
}

func TestOperationRecoveryRejectsCrossCoordinatorCrossLeaseAndExpiredScopes(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlementA := stores.settlement
	settlementB, err := NewOperationSettlement(stores.callbacks, stores.releases)
	require.NoError(t, err)
	bindTestOperationMutation(t, settlementA, nil)
	bindTestOperationMutation(t, settlementB, nil)
	coordinatorA := newTestRecoveryCoordinator(t, settlementA, nil, nil)
	coordinatorB := newTestRecoveryCoordinator(t, settlementB, nil, nil)
	claim := beginHandoffOperation(
		t, settlementA, testOperationIntentSpec(t, "recovery-scope-lineage"),
	)

	_, err = coordinatorB.WithLease(
		context.Background(), claim.LeaseUUID(),
		func(scope LeaseRecoveryScope) error {
			_, recoverErr := settlementA.RecoverOperationExecution(
				context.Background(), scope, claim,
			)
			return recoverErr
		},
	)
	require.ErrorContains(t, err, "exact lease-quiescence authority")

	_, err = coordinatorA.WithLease(
		context.Background(), testLeaseUUID("wrong-recovery-scope-lease"),
		func(scope LeaseRecoveryScope) error {
			_, recoverErr := settlementA.RecoverOperationExecution(
				context.Background(), scope, claim,
			)
			return recoverErr
		},
	)
	require.ErrorContains(t, err, "exact lease-quiescence authority")

	var expired LeaseRecoveryScope
	acquired, err := coordinatorA.WithLease(
		context.Background(), claim.LeaseUUID(),
		func(scope LeaseRecoveryScope) error {
			expired = scope
			return nil
		},
	)
	require.NoError(t, err)
	require.True(t, acquired)
	_, err = settlementA.RecoverOperationExecution(context.Background(), expired, claim)
	require.ErrorContains(t, err, "exact lease-quiescence authority")
}

func TestOperationRecoveryRejectsZeroOpaqueAuthorities(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	bindTestOperationMutation(t, stores.settlement, nil)
	coordinator := newTestRecoveryCoordinator(t, stores.settlement, nil, nil)
	leaseUUID := testLeaseUUID("zero-operation-recovery-authority")

	acquired, err := coordinator.WithLease(
		context.Background(), leaseUUID,
		func(scope LeaseRecoveryScope) error {
			_, recoverErr := stores.settlement.RecoverOperationExecution(
				context.Background(), scope, OperationIntentClaim{},
			)
			require.ErrorContains(t, recoverErr, "exact lease-quiescence authority")
			return stores.settlement.CleanupFailedOperationReceipt(
				context.Background(), scope, FailedOperationReceipt{},
			)
		},
	)
	require.True(t, acquired)
	require.ErrorContains(t, err, "exact lease-quiescence authority")
	assert.False(t, (OperationIntentClaim{}).Valid())
	assert.False(t, (FailedOperationReceipt{}).Valid())
}

func TestRecoveryConsumersRejectClosedJournalsWithActiveScope(t *testing.T) {
	t.Run("operation", func(t *testing.T) {
		stores := openOperationHandoffStores(t, "docker-a")
		claim := beginHandoffOperation(
			t, stores.settlement, testOperationIntentSpec(t, "closed-operation-recovery"),
		)
		bindTestOperationMutation(t, stores.settlement, nil)
		coordinator := newTestRecoveryCoordinator(t, stores.settlement, nil, nil)

		acquired, err := coordinator.WithLease(
			context.Background(), claim.LeaseUUID(),
			func(scope LeaseRecoveryScope) error {
				require.True(t, scope.validFor(coordinator, claim.LeaseUUID()))
				require.NoError(t, stores.callbacks.Close())
				_, recoverErr := stores.settlement.RecoverOperationExecution(
					context.Background(), scope, claim,
				)
				return recoverErr
			},
		)
		require.True(t, acquired)
		require.ErrorContains(t, err, "invalid or closed")
	})

	t.Run("maintenance", func(t *testing.T) {
		fixture := beginBoundMaintenance(t, "closed-maintenance-recovery")
		bindTestMaintenanceMutation(t, fixture.settlement, nil)
		coordinator := newTestRecoveryCoordinator(t, nil, fixture.settlement, nil)

		acquired, err := coordinator.WithLease(
			context.Background(), fixture.intent.LeaseUUID(),
			func(scope LeaseRecoveryScope) error {
				require.True(t, scope.validFor(coordinator, fixture.intent.LeaseUUID()))
				require.NoError(t, fixture.stores.callbacks.Close())
				_, recoverErr := fixture.settlement.RecoverMaintenanceExecution(
					context.Background(), scope, fixture.intent,
				)
				return recoverErr
			},
		)
		require.True(t, acquired)
		require.ErrorContains(t, err, "maintenance settlement is invalid")
	})
}

func TestRecoveryCoordinatorRejectsMixedJournalPairs(t *testing.T) {
	storesA := openOperationHandoffStores(t, "docker-a")
	storesB := openOperationHandoffStores(t, "docker-b")
	maintenanceB, err := NewMaintenanceSettlement(storesB.callbacks, storesB.releases)
	require.NoError(t, err)

	_, err = NewRecoveryCoordinator(RecoveryCoordinatorConfig{
		Operations:  storesA.settlement,
		Maintenance: maintenanceB,
		ExcludeLease: func(_ context.Context, _ string, run func() error) (bool, error) {
			return true, run()
		},
	})
	require.ErrorContains(t, err, "different journal pairs")
}

func TestRecoveryCoordinatorRequiresCloseAndActorValidatorTogether(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	closeSettlement := newCloseSettlementForTest(t, stores)
	exclude := func(_ context.Context, _ string, run func() error) (bool, error) {
		return true, run()
	}
	validator := func(ActorCloseScope, RecoveryLineage) (string, bool) {
		return "", false
	}

	_, err := NewRecoveryCoordinator(RecoveryCoordinatorConfig{
		Close: closeSettlement, ExcludeLease: exclude,
	})
	require.ErrorContains(t, err, "configured together")

	_, err = NewRecoveryCoordinator(RecoveryCoordinatorConfig{
		ExcludeLease: exclude, ValidateActorClose: validator,
	})
	require.ErrorContains(t, err, "configured together")
}

func TestRecoveryCoordinatorRejectsPreCloseSettlementAfterRetentionClose(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	closeSettlement := newCloseSettlementForTest(t, stores)
	require.NoError(t, stores.retentions.Close())

	_, err := NewRecoveryCoordinator(RecoveryCoordinatorConfig{
		Close: closeSettlement,
		ExcludeLease: func(_ context.Context, _ string, run func() error) (bool, error) {
			return true, run()
		},
		ValidateActorClose: func(ActorCloseScope, RecoveryLineage) (string, bool) {
			return "", false
		},
	})
	require.ErrorContains(t, err, "open close settlement")
}
