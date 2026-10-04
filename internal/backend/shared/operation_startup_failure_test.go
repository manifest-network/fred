package shared

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

func exitedStartupTerms(instanceID string) OperationStartupFailureTerms {
	return OperationStartupFailureTerms{
		Reason: backend.ReasonContainerExited, Message: backend.MsgContainerExitedDuringStartup,
		Detail: "exit_code=3", InstanceID: instanceID, Service: "app",
		Termination: failurecause.Exited(), ExitCode: 3,
	}
}

func liveProvenanceFor(instanceID string) failurecause.Provenance {
	session := failurecause.NewEventSession()
	session.ObserveStart(instanceID)
	return session.ObserveExit(instanceID)
}

// The startup failure's terms are validated as a whole: an exit needs the
// substrate's Exited termination (an exit code alone proves nothing), an
// unhealthy container has neither a termination nor a death, and a provenance
// must have been minted for this very container.
func TestOperationStartupFailureTermsAreValidatedAsAWhole(t *testing.T) {
	unhealthy := OperationStartupFailureTerms{
		Reason: backend.ReasonHealthCheckFailed, Message: backend.MsgContainerUnhealthy,
		InstanceID: "c1", Service: "app",
	}
	refused := OperationStartupFailureTerms{
		Reason: backend.ReasonContainerStartFailed, Message: backend.MsgContainerStartRefused,
		InstanceID: "c1", Service: "app",
	}
	tests := []struct {
		name  string
		edit  func(*OperationStartupFailureTerms)
		base  OperationStartupFailureTerms
		valid bool
		kind  OperationStartupFailureKind
	}{
		{"exit", func(*OperationStartupFailureTerms) {}, exitedStartupTerms("c1"), true, OperationStartupExited},
		{"exit with its own provenance", func(terms *OperationStartupFailureTerms) {
			terms.Provenance = liveProvenanceFor("c1")
		}, exitedStartupTerms("c1"), true, OperationStartupExited},
		{"unhealthy", func(*OperationStartupFailureTerms) {}, unhealthy, true, OperationStartupUnhealthy},
		{"refused start", func(*OperationStartupFailureTerms) {}, refused, true, OperationStartupRefused},
		{"refused start with a termination", func(terms *OperationStartupFailureTerms) {
			terms.Termination = failurecause.Exited()
		}, refused, false, 0},
		{"refused start with a death", func(terms *OperationStartupFailureTerms) {
			terms.Provenance = liveProvenanceFor("c1")
		}, refused, false, 0},
		{"refused start with an exit status", func(terms *OperationStartupFailureTerms) {
			terms.ExitCode = 127
		}, refused, false, 0},
		{"no container", func(terms *OperationStartupFailureTerms) { terms.InstanceID = "" }, exitedStartupTerms("c1"), false, 0},
		{"no message", func(terms *OperationStartupFailureTerms) { terms.Message = "" }, exitedStartupTerms("c1"), false, 0},
		{"exit code without an observed exit", func(terms *OperationStartupFailureTerms) {
			terms.Termination = failurecause.Termination{}
		}, exitedStartupTerms("c1"), false, 0},
		{"gone is not an exit", func(terms *OperationStartupFailureTerms) {
			terms.Termination = failurecause.Gone()
		}, exitedStartupTerms("c1"), false, 0},
		{"provenance of another container", func(terms *OperationStartupFailureTerms) {
			terms.Provenance = liveProvenanceFor("c2")
		}, exitedStartupTerms("c1"), false, 0},
		{"unhealthy with a termination", func(terms *OperationStartupFailureTerms) {
			terms.Termination = failurecause.Exited()
		}, unhealthy, false, 0},
		{"unhealthy with a death", func(terms *OperationStartupFailureTerms) {
			terms.Provenance = liveProvenanceFor("c1")
		}, unhealthy, false, 0},
		{"any other reason", func(terms *OperationStartupFailureTerms) {
			terms.Reason = backend.ReasonInternal
		}, exitedStartupTerms("c1"), false, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			terms := test.base
			test.edit(&terms)
			failure, err := NewOperationStartupFailure(terms)
			if !test.valid {
				require.Error(t, err)
				assert.False(t, failure.Valid())
				return
			}
			require.NoError(t, err)
			assert.True(t, failure.Valid())
			assert.Equal(t, test.kind, failure.Kind())
			assert.Equal(t, terms.Reason, failure.Reason())
			assert.Equal(t, terms.Termination, failure.Termination())
			assert.Equal(t, terms.Provenance, failure.Provenance())
		})
	}
	var zero OperationStartupFailure
	assert.False(t, zero.Valid(), "the zero startup failure is invalid")
	assert.Zero(t, zero.Kind())
	_, _, exited := zero.ExitStatus()
	assert.False(t, exited)
}

// startupFindingCapability is a test facade: one rollback Step and the
// session's Accept.
type startupFindingCapability struct {
	rollback func(context.Context) error
	accept   func(OperationStartupFailure) (substratemutation.Accepted[OperationStartupFailure], error)
}

func newStartupFindingCapability(runner substratemutation.Runner, _ OperationPhysicalSubject) startupFindingCapability {
	return startupFindingCapability{
		rollback: func(ctx context.Context) error {
			return runner.Step(ctx, "remove failed cohort", func(context.Context) error { return nil })
		},
		accept: func(finding OperationStartupFailure) (substratemutation.Accepted[OperationStartupFailure], error) {
			return substratemutation.Accept(runner, finding)
		},
	}
}

// bindStartupFindingMutation binds a workflow that accepts a valid finding
// when its session allows it (a recovery session never does), then enters one
// clean rollback Step, with a classifier that mints StartupFailed evidence
// exactly when it receives an accepted finding. A hostile classifier mints it
// whatever it receives, to prove the evidence's own validity rules.
func bindStartupFindingMutation(
	t *testing.T,
	settlement *OperationSettlement,
	finding OperationStartupFailure,
	hostile bool,
) {
	t.Helper()
	require.NoError(t, BindOperationFindingExecutor(
		settlement,
		func(ctx context.Context, _ string) (context.Context, func(), error) { return ctx, func() {}, nil },
		func(context.Context, string, error) error { return nil },
		newStartupFindingCapability,
		func(ctx context.Context, capability startupFindingCapability, _ OperationPhysicalSubject) (
			substratemutation.Accepted[OperationStartupFailure], error,
		) {
			var accepted substratemutation.Accepted[OperationStartupFailure]
			if finding.Valid() {
				// A refusal leaves the finding absent; the hostile classifier
				// below is then the only barrier left to prove.
				accepted, _ = capability.accept(finding)
			}
			return accepted, capability.rollback(ctx)
		},
		func(_ context.Context, subject OperationPhysicalSubject, accepted substratemutation.Accepted[OperationStartupFailure]) (OperationPhysicalEvidence, error) {
			if hostile {
				return NewOperationStartupFailed(subject, finding)
			}
			if observed, present := accepted.Finding(); present {
				return NewOperationStartupFailed(subject, observed)
			}
			return NewOperationExactAbsent(subject)
		},
	))
}

// A live provision whose startup failed definitely settles as a failure that
// carries its sealed account, and the release-absence proof can be committed.
func TestStartupFailedEvidenceSettlesALiveProvisionDefinitely(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	claim := beginHandoffOperation(t, stores.settlement, testOperationIntentSpec(t, "startup-failed"))
	failure, err := NewOperationStartupFailure(exitedStartupTerms("c1"))
	require.NoError(t, err)
	bindStartupFindingMutation(t, stores.settlement, failure, false)

	candidate, err := stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	execution, err := stores.settlement.StartOperationExecution(candidate)
	require.NoError(t, err)
	outcome := stores.settlement.ExecuteOperation(context.Background(), execution)
	settled, ok := outcome.(OperationExecutionFailure)
	require.True(t, ok, "outcome = %T", outcome)
	require.True(t, settled.Valid())
	carried, ok := settled.StartupFailure()
	require.True(t, ok)
	assert.Equal(t, failure, carried)
	assert.Equal(t, claim.OperationID(), settled.OperationID())
	uncommitted, err := stores.settlement.CommitOperationFailure(settled)
	require.NoError(t, err)
	assert.True(t, uncommitted.MatchesIntent(claim))
}

// Other failures carry no startup account.
func TestOtherFailuresCarryNoStartupFailure(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	claim := beginHandoffOperation(t, stores.settlement, testOperationIntentSpec(t, "absent"))
	bindStartupFindingMutation(t, stores.settlement, OperationStartupFailure{}, false)
	candidate, err := stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	execution, err := stores.settlement.StartOperationExecution(candidate)
	require.NoError(t, err)
	settled, ok := stores.settlement.ExecuteOperation(context.Background(), execution).(OperationExecutionFailure)
	require.True(t, ok)
	_, carried := settled.StartupFailure()
	assert.False(t, carried, "an exact-absence failure carries no startup account")
	refused, err := stores.settlement.RefuseOperationExecution(candidate)
	require.Error(t, err, "a Started candidate can no longer be refused")
	_, carried = refused.StartupFailure()
	assert.False(t, carried)
}

// Startup failure evidence is live-provision only: a restore cannot mint it,
// and neither recovery entry point accepts it, even from a classifier that
// mints it regardless of what it was given.
func TestStartupFailedEvidenceIsLiveProvisionOnly(t *testing.T) {
	failure, err := NewOperationStartupFailure(exitedStartupTerms("c1"))
	require.NoError(t, err)

	t.Run("restore", func(t *testing.T) {
		stores := openOperationHandoffStores(t, "docker-a")
		_, claim, _ := restoreHandoffFixture(t, stores, "startup-restore")
		var minted error
		require.NoError(t, BindOperationFindingExecutor(
			stores.settlement,
			func(ctx context.Context, _ string) (context.Context, func(), error) { return ctx, func() {}, nil },
			func(context.Context, string, error) error { return nil },
			newStartupFindingCapability,
			func(ctx context.Context, capability startupFindingCapability, _ OperationPhysicalSubject) (
				substratemutation.Accepted[OperationStartupFailure], error,
			) {
				accepted, err := capability.accept(failure)
				if err != nil {
					return accepted, err
				}
				return accepted, capability.rollback(ctx)
			},
			func(_ context.Context, subject OperationPhysicalSubject, accepted substratemutation.Accepted[OperationStartupFailure]) (OperationPhysicalEvidence, error) {
				observed, _ := accepted.Finding()
				evidence, err := NewOperationStartupFailed(subject, observed)
				minted = err
				return evidence, err
			},
		))
		candidate, err := stores.settlement.PrepareOperationRelease(claim)
		require.NoError(t, err)
		execution, err := stores.settlement.StartOperationExecution(candidate)
		require.NoError(t, err)
		_, ambiguous := stores.settlement.ExecuteOperation(context.Background(), execution).(OperationExecutionAmbiguous)
		assert.True(t, ambiguous)
		require.ErrorContains(t, minted, "live provision")
	})

	t.Run("recovery", func(t *testing.T) {
		stores := openOperationHandoffStores(t, "docker-a")
		claim := beginHandoffOperation(t, stores.settlement, testOperationIntentSpec(t, "startup-recovery"))
		bindStartupFindingMutation(t, stores.settlement, failure, true)
		candidate, err := stores.settlement.PrepareOperationRelease(claim)
		require.NoError(t, err)
		_, err = stores.settlement.StartOperationExecution(candidate)
		require.NoError(t, err)
		claims, err := stores.settlement.ListOperationIntents()
		require.NoError(t, err)
		require.Len(t, claims, 1)
		coordinator := newTestRecoveryCoordinator(t, stores.settlement, nil, nil)
		acquired, err := coordinator.WithLease(t.Context(), claim.LeaseUUID(), func(scope LeaseRecoveryScope) error {
			_, recoverErr := stores.settlement.RecoverOperationExecution(context.Background(), scope, claims[0])
			require.ErrorContains(t, recoverErr, "live-only")
			cleaned, cleanupErr := stores.settlement.CleanupRecoveredOperation(context.Background(), scope, claims[0])
			require.NoError(t, cleanupErr)
			ambiguous, ok := cleaned.(OperationExecutionAmbiguous)
			require.True(t, ok, "cleanup = %T", cleaned)
			assert.ErrorContains(t, ambiguous.Cause(), "live provision",
				"a recovery-cleanup subject cannot mint startup evidence")
			return nil
		})
		require.NoError(t, err)
		require.True(t, acquired)
	})
}
