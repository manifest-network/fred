package shared

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

type testCloseMutation struct {
	run func(context.Context) error
}

func bindTestCloseMutation(
	t *testing.T,
	settlement *CloseSettlement,
	workflow func(substratemutation.Runner, context.Context, ClosePhysicalSubject) error,
	classify func(ClosePhysicalSubject) (ClosePhysicalEvidence, error),
) {
	t.Helper()
	if workflow == nil {
		workflow = func(runner substratemutation.Runner, ctx context.Context, _ ClosePhysicalSubject) error {
			return runner.Step(ctx, "test close mutation", func(context.Context) error { return nil })
		}
	}
	if classify == nil {
		classify = func(subject ClosePhysicalSubject) (ClosePhysicalEvidence, error) {
			return NewCloseDestroyed(subject)
		}
	}
	require.NoError(t, BindCloseSubstrateExecutor(
		settlement,
		func(ctx context.Context, _ string) (context.Context, func(), error) {
			return ctx, func() {}, nil
		},
		func(context.Context, string, error) error { return nil },
		func(runner substratemutation.Runner, subject ClosePhysicalSubject) testCloseMutation {
			return testCloseMutation{run: func(ctx context.Context) error {
				return workflow(runner, ctx, subject)
			}}
		},
		func(ctx context.Context, mutation testCloseMutation, _ ClosePhysicalSubject) error {
			return mutation.run(ctx)
		},
		func(_ context.Context, subject ClosePhysicalSubject) (ClosePhysicalEvidence, error) {
			return classify(subject)
		},
	))
}

func startTestCloseExecution(
	t *testing.T,
	settlement *CloseSettlement,
	claim CloseIntentClaim,
) CloseExecutionClaim {
	t.Helper()
	execution, err := settlement.StartCloseExecution(claim)
	require.NoError(t, err)
	return execution
}

func TestCloseExecutionDestroyedCrossesDurableGenerationAndSettlesOnce(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	bindTestCloseMutation(t, settlement, nil, nil)
	spec := seedCloseSettlementRelease(t, stores, "close-execution-destroyed")
	initial := admitSettlementClose(t, settlement, spec.LeaseUUID, false)

	execution := startTestCloseExecution(t, settlement, initial)
	assert.Equal(t, 1, execution.subject.ExecutionGeneration().Number())
	current, found, err := settlement.GetCloseIntent(spec.LeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, 1, current.ExecutionGeneration().Number())
	_, err = settlement.StartCloseExecution(initial)
	require.ErrorContains(t, err, "changed before precise mutation")

	outcome := settlement.ExecuteClose(context.Background(), execution)
	destroyed, ok := outcome.(CloseExecutionDestroyed)
	require.True(t, ok)
	entry, err := settlement.CompleteClose(destroyed)
	require.NoError(t, err)
	assert.Equal(t, "deprovisioned", string(entry.Status))
	assert.False(t, entry.Retained)
	_, err = settlement.CompleteClose(destroyed)
	require.Error(t, err, "a copied terminal result must not settle twice")
}

func TestCloseExecutionAmbiguityAndIncompleteInventoryRemainDurable(t *testing.T) {
	tests := []struct {
		name      string
		workflow  func(substratemutation.Runner, context.Context, ClosePhysicalSubject) error
		classify  func(ClosePhysicalSubject) (ClosePhysicalEvidence, error)
		retryable bool
	}{
		{
			name: "ambiguous physical step",
			workflow: func(runner substratemutation.Runner, ctx context.Context, _ ClosePhysicalSubject) error {
				return runner.Step(ctx, "uncertain destroy", func(context.Context) error {
					return errors.New("transport lost after dispatch")
				})
			},
		},
		{
			name: "strict inventory remains incomplete",
			classify: func(subject ClosePhysicalSubject) (ClosePhysicalEvidence, error) {
				return NewCloseIncomplete(subject)
			},
			retryable: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stores := openOperationHandoffStores(t, "docker-a")
			settlement := newCloseSettlementForTest(t, stores)
			bindTestCloseMutation(t, settlement, test.workflow, test.classify)
			spec := seedCloseSettlementRelease(t, stores, "close-pending-"+test.name)
			claim := admitSettlementClose(t, settlement, spec.LeaseUUID, false)
			execution := startTestCloseExecution(t, settlement, claim)

			outcome := settlement.ExecuteClose(context.Background(), execution)
			pending, ok := outcome.(CloseExecutionPending)
			require.True(t, ok)
			require.Error(t, pending.Cause())
			assert.Equal(t, test.retryable, pending.RetryableNow())
			if test.retryable {
				_, err := settlement.RetryCloseExecution(pending)
				require.NoError(t, err)
			} else {
				_, err := settlement.RetryCloseExecution(pending)
				require.ErrorContains(t, err, "retryable evidence")
			}
			_, found, err := settlement.GetCloseIntent(spec.LeaseUUID)
			require.NoError(t, err)
			assert.True(t, found)
			releases, err := stores.releases.List(spec.LeaseUUID)
			require.NoError(t, err)
			assert.NotEmpty(t, releases)
		})
	}
}

func TestCloseExecutionRetainedRequiresExactActiveRetention(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	bindTestCloseMutation(t, settlement, nil, func(subject ClosePhysicalSubject) (ClosePhysicalEvidence, error) {
		proof, err := settlement.ProveRetention(subject.Intent())
		if err != nil {
			return ClosePhysicalEvidence{}, err
		}
		return NewCloseRetained(subject, proof)
	})
	spec := seedCloseSettlementRelease(t, stores, "close-execution-retained")
	claim := admitSettlementClose(t, settlement, spec.LeaseUUID, true)
	ok, err := settlement.RecordRetention(
		claim, "partition-a", []string{"fred-retained-" + spec.LeaseUUID + "-app-0"},
	)
	require.NoError(t, err)
	require.True(t, ok)

	execution := startTestCloseExecution(t, settlement, claim)
	outcome := settlement.ExecuteClose(context.Background(), execution)
	retained, ok := outcome.(CloseExecutionRetained)
	require.True(t, ok)
	entry, err := settlement.CompleteClose(retained)
	require.NoError(t, err)
	assert.True(t, entry.Retained)
}

func TestCloseExecutionStaleGenerationCannotRetireRelease(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	incomplete := false
	bindTestCloseMutation(t, settlement, nil, func(subject ClosePhysicalSubject) (ClosePhysicalEvidence, error) {
		if incomplete {
			return NewCloseIncomplete(subject)
		}
		return NewCloseDestroyed(subject)
	})
	spec := seedCloseSettlementRelease(t, stores, "close-stale-execution")
	claim := admitSettlementClose(t, settlement, spec.LeaseUUID, false)
	first := startTestCloseExecution(t, settlement, claim)
	current, found, err := settlement.GetCloseIntent(spec.LeaseUUID)
	require.NoError(t, err)
	require.True(t, found)

	firstOutcome, ok := settlement.ExecuteClose(context.Background(), first).(CloseExecutionDestroyed)
	require.True(t, ok)
	incomplete = true
	coordinator := newTestRecoveryCoordinator(t, nil, nil, settlement)
	var recovered CloseExecutionOutcome
	acquired, err := coordinator.WithLease(
		context.Background(), current.LeaseUUID(),
		func(scope LeaseRecoveryScope) error {
			recovered, err = settlement.RecoverCloseExecution(context.Background(), scope, current)
			return err
		},
	)
	require.NoError(t, err)
	require.True(t, acquired)
	pending, ok := recovered.(CloseExecutionPending)
	require.True(t, ok)
	second, err := settlement.RetryCloseExecution(pending)
	require.NoError(t, err)
	incomplete = false
	_, err = settlement.CompleteClose(firstOutcome)
	require.ErrorContains(t, err, "changed before precise mutation")
	releases, err := stores.releases.List(spec.LeaseUUID)
	require.NoError(t, err)
	assert.NotEmpty(t, releases, "stale generation must be rejected before release retirement")

	secondOutcome, ok := settlement.ExecuteClose(context.Background(), second).(CloseExecutionDestroyed)
	require.True(t, ok)
	_, err = settlement.CompleteClose(secondOutcome)
	require.NoError(t, err)
}

func TestCloseExecutionRecoveryAfterStartedCrash(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	firstSettlement := newCloseSettlementForTest(t, stores)
	bindTestCloseMutation(t, firstSettlement, nil, nil)
	spec := seedCloseSettlementRelease(t, stores, "close-started-crash")
	claim := admitSettlementClose(t, firstSettlement, spec.LeaseUUID, false)
	staleExecution := startTestCloseExecution(t, firstSettlement, claim)
	staleDestroyed, ok := firstSettlement.ExecuteClose(
		context.Background(), staleExecution,
	).(CloseExecutionDestroyed)
	require.True(t, ok)
	// Model a crash after the substrate was destroyed but before the terminal
	// journal transition consumed this first-instance outcome.

	require.NoError(t, stores.callbacks.Close())
	require.NoError(t, stores.releases.Close())
	require.NoError(t, stores.retentions.Close())
	callbacks, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	releases, err := OpenIdentityBoundReleaseStore(
		ReleaseStoreConfig{DBPath: stores.releasePath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	retentions, err := OpenIdentityBoundRetentionStore(
		RetentionStoreConfig{DBPath: stores.retentionPath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	secondSettlement, err := NewCloseSettlement(callbacks, releases, retentions)
	require.NoError(t, err)
	bindTestCloseMutation(t, secondSettlement, nil, nil)
	t.Cleanup(func() {
		_ = callbacks.Close()
		_ = releases.Close()
		_ = retentions.Close()
	})

	recoveredClaim, found, err := secondSettlement.GetCloseIntent(spec.LeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, 1, recoveredClaim.ExecutionGeneration().Number())
	coordinator := newTestRecoveryCoordinator(t, nil, nil, secondSettlement)
	var recovered CloseExecutionOutcome
	acquired, err := coordinator.WithLease(
		context.Background(), recoveredClaim.LeaseUUID(),
		func(scope LeaseRecoveryScope) error {
			recovered, err = secondSettlement.RecoverCloseExecution(
				context.Background(), scope, recoveredClaim,
			)
			return err
		},
	)
	require.NoError(t, err)
	require.True(t, acquired)
	destroyed, ok := recovered.(CloseExecutionDestroyed)
	require.True(t, ok)
	_, err = secondSettlement.CompleteClose(destroyed)
	require.NoError(t, err)

	_, err = secondSettlement.CompleteClose(staleDestroyed)
	require.ErrorContains(t, err, "another settlement")
}

func TestCloseExecutionCopiedTerminalOutcomeSettlesAtMostOnce(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	bindTestCloseMutation(t, settlement, nil, nil)
	spec := seedCloseSettlementRelease(t, stores, "close-execution-race")
	claim := admitSettlementClose(t, settlement, spec.LeaseUUID, false)
	execution := startTestCloseExecution(t, settlement, claim)
	destroyed, ok := settlement.ExecuteClose(context.Background(), execution).(CloseExecutionDestroyed)
	require.True(t, ok)

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func(copy CloseExecutionDestroyed) {
			defer wg.Done()
			<-start
			_, err := settlement.CompleteClose(copy)
			errs <- err
		}(destroyed)
	}
	close(start)
	wg.Wait()
	close(errs)
	var successes, failures int
	for err := range errs {
		if err == nil {
			successes++
		} else {
			failures++
		}
	}
	assert.Equal(t, 1, successes)
	assert.Equal(t, 1, failures)
}
