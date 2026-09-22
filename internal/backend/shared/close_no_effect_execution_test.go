package shared

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

func TestCloseEffectFreeExecutionRequiresExactTerminalObservation(t *testing.T) {
	for _, mode := range []string{
		"destroyed", "retained", "unknown inventory", "incomplete cohort",
		"foreign evidence", "empty evidence", "stale generation", "ambiguous dispatch",
	} {
		t.Run(mode, func(t *testing.T) {
			stores := openOperationHandoffStores(t, "docker-a")
			settlement := newCloseSettlementForTest(t, stores)
			var foreign ClosePhysicalSubject
			classifications := 0
			bindTestCloseMutation(t, settlement,
				func(runner substratemutation.Runner, ctx context.Context, subject ClosePhysicalSubject) error {
					switch mode {
					case "stale generation":
						// Model a concurrent durable owner replacing the observed
						// generation before the effect-free result is consumed.
						_, err := settlement.startCloseGeneration(subject.state.claim)
						return err
					case "ambiguous dispatch":
						return runner.Step(ctx, "uncertain close dispatch", func(context.Context) error {
							return errors.New("remote close response lost")
						})
					default:
						return nil // An empty exact cohort requires no mutation.
					}
				},
				func(subject ClosePhysicalSubject) (ClosePhysicalEvidence, error) {
					classifications++
					switch mode {
					case "unknown inventory":
						return ClosePhysicalEvidence{}, errors.New("strict inventory unavailable")
					case "incomplete cohort":
						return NewCloseIncomplete(subject)
					case "foreign evidence":
						return NewCloseDestroyed(foreign)
					case "empty evidence":
						return ClosePhysicalEvidence{}, nil
					case "retained":
						proof, err := settlement.ProveRetention(subject.Intent())
						if err != nil {
							return ClosePhysicalEvidence{}, err
						}
						return NewCloseRetained(subject, proof)
					default:
						return NewCloseDestroyed(subject)
					}
				})
			spec := seedCloseSettlementRelease(t, stores, "effect-free-"+mode)
			claim := admitSettlementClose(t, settlement, spec.LeaseUUID, mode == "retained")
			if mode == "retained" {
				recorded, err := settlement.RecordRetention(claim, "partition-a",
					[]string{"fred-retained-" + spec.LeaseUUID + "-app-0"})
				require.NoError(t, err)
				require.True(t, recorded)
			}
			if mode == "foreign evidence" {
				other := seedCloseSettlementRelease(t, stores, "effect-free-other")
				otherClaim := admitSettlementClose(t, settlement, other.LeaseUUID, false)
				foreign = startTestCloseExecution(t, settlement, otherClaim).subject
			}
			execution := startTestCloseExecution(t, settlement, claim)
			outcome := settlement.ExecuteClose(t.Context(), execution)
			if mode == "stale generation" {
				require.Zero(t, classifications, "a stale generation cannot acquire observation authority")
			} else {
				require.Equal(t, 1, classifications,
					"effect-free work observes once; ambiguous work gets only its original guard classification")
			}
			switch mode {
			case "destroyed", "retained":
				terminal, ok := outcome.(CloseTerminalOutcome)
				require.True(t, ok, "exact empty/retained cohort must settle on the first call: %T", outcome)
				replayed, ok := settlement.ExecuteClose(t.Context(), execution).(CloseExecutionPending)
				require.True(t, ok)
				require.Error(t, replayed.Cause())
				require.Equal(t, 1, classifications, "a consumed live execution cannot request another observation")
				entry, err := settlement.CompleteClose(terminal)
				require.NoError(t, err)
				require.Equal(t, mode == "retained", entry.Retained)
				_, err = settlement.CompleteClose(terminal)
				require.Error(t, err, "a copied observation cannot settle twice")
			default:
				pending, ok := outcome.(CloseExecutionPending)
				require.True(t, ok, "uncertainty cannot terminalize close: %T", outcome)
				require.Error(t, pending.Cause())
				require.Equal(t, mode == "incomplete cohort", pending.RetryableNow())
				_, found, err := settlement.GetCloseIntent(spec.LeaseUUID)
				require.NoError(t, err)
				require.True(t, found)
				active, err := stores.releases.LatestActive(spec.LeaseUUID)
				require.NoError(t, err)
				require.NotNil(t, active, "failed observation preserves the exact durable release owner")
			}
		})
	}
}

func TestCloseEffectFreeObservationRejectsForeignExecutionResult(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	classifications, workflows := 0, 0
	bindTestCloseMutation(t, settlement,
		func(substratemutation.Runner, context.Context, ClosePhysicalSubject) error {
			workflows++
			return nil
		},
		func(subject ClosePhysicalSubject) (ClosePhysicalEvidence, error) {
			classifications++
			return NewCloseDestroyed(subject)
		})
	first := seedCloseSettlementRelease(t, stores, "effect-free-first")
	second := seedCloseSettlementRelease(t, stores, "effect-free-second")
	zero, ok := settlement.ExecuteClose(t.Context(), CloseExecutionClaim{}).(CloseExecutionPending)
	require.True(t, ok)
	require.ErrorContains(t, zero.Cause(), "boundary is invalid")
	require.Zero(t, workflows, "an unstarted execution cannot enter the workflow")
	require.Zero(t, classifications, "an unstarted execution cannot acquire observation authority")
	firstExecution := startTestCloseExecution(t, settlement, admitSettlementClose(t, settlement, first.LeaseUUID, false))
	secondExecution := startTestCloseExecution(t, settlement, admitSettlementClose(t, settlement, second.LeaseUUID, false))
	foreign := settlement.execute(t.Context(), firstExecution.started)
	require.Equal(t, substratemutation.Refused, foreign.Kind())
	settlement.execute = func(context.Context, substratemutation.LiveExecution[ClosePhysicalSubject]) substratemutation.Result[ClosePhysicalSubject, ClosePhysicalEvidence] {
		return foreign
	}
	pending, ok := settlement.ExecuteClose(t.Context(), secondExecution).(CloseExecutionPending)
	require.True(t, ok)
	require.ErrorContains(t, pending.Cause(), "belongs to another execution")
	require.Zero(t, classifications, "a foreign Refused result cannot acquire observation authority")
}
