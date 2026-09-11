package shared

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

type compensationTestState struct {
	targetErr       error
	beforeTargetErr error
	auxiliaryErr    error
	beforeSourceErr error
	sourceErr       error
	sourceReady     bool
	sourceUnready   bool
	launches        int
	sourceSubject   MaintenanceCompensationSubject
	journal         *VolumeLaunchJournal
	targetReturned  func()
}

func bindCompensationTest(t *testing.T, s *MaintenanceSettlement, state *compensationTestState) {
	t.Helper()
	journal, err := NewVolumeLaunchJournal(s.callbacks)
	require.NoError(t, err)
	state.journal = journal
	authorize := func(ctx context.Context, _ string) (context.Context, func(), error) { return ctx, func() {}, ctx.Err() }
	complete := func(context.Context, string, error) error { return nil }
	require.NoError(t, BindMaintenanceCompensationExecutor(s, t.Context(), authorize, complete,
		func(context.Context, MaintenancePhysicalSubject) (MaintenanceSourceCapture, error) {
			return CapturedMaintenanceSource([]byte(`{"version":1,"image":"immutable-source"}`))
		},
		func(_ MaintenancePhysicalSubject, plan []byte) error {
			if string(plan) != `{"version":1,"image":"immutable-source"}` {
				return errors.New("source plan changed")
			}
			return nil
		},
		func(runner substratemutation.Runner, subject MaintenanceCompensationSubject) func(context.Context) error {
			return func(ctx context.Context) error {
				state.sourceSubject = subject
				if !subject.SourceLaunchRequired() {
					return nil
				}
				if state.beforeSourceErr != nil {
					return state.beforeSourceErr
				}
				debt, err := journal.Begin(VolumeLaunchForCompensation(subject), nil)
				if err != nil {
					return err
				}
				assertCompensationJournalPhase(t, s, subject.Intent(), compensationSourceDispatching, 1)
				receipt, err := runner.StepCompleted(ctx, MaintenanceSourceLaunchStep, func(context.Context) error { state.launches++; return state.sourceErr })
				if err != nil {
					return err
				}
				if err := journal.Complete(debt, receipt); err != nil {
					return err
				}
				assertCompensationJournalPhase(t, s, subject.Intent(), compensationSourceSettled, 0)
				require.Error(t, journal.Complete(debt, receipt), "copied completion cannot settle twice")
				state.sourceReady = !state.sourceUnready
				return nil
			}
		},
		func(ctx context.Context, run func(context.Context) error, _ MaintenanceCompensationSubject) error {
			return run(ctx)
		},
		func(_ context.Context, subject MaintenanceCompensationSubject) (MaintenancePhysicalEvidence, error) {
			if !state.sourceReady {
				if state.sourceUnready {
					source, _ := subject.SourceRelease()
					ids, services := testPhysicalProjection(source)
					return NewMaintenanceCompensationSourceFailed(subject, ids, services)
				}
				return MaintenancePhysicalEvidence{}, errors.New("source not ready")
			}
			source, _ := subject.SourceRelease()
			ids, services := testPhysicalProjection(source)
			return NewMaintenanceSourceReady(subject.FailedTarget(), ids, services)
		},
	))
	require.NoError(t, BindMaintenanceSubstrateExecutor(s, authorize, complete,
		func(runner substratemutation.Runner, subject MaintenancePhysicalSubject) func(context.Context) error {
			return func(ctx context.Context) error {
				if state.auxiliaryErr != nil {
					return runner.Prepare(ctx, "pull replacement image", func(context.Context) error { return state.auxiliaryErr })
				}
				if state.beforeTargetErr != nil {
					return runner.Step(ctx, "prepare target mount sources", func(context.Context) error { return state.beforeTargetErr })
				}
				debt, err := journal.Begin(VolumeLaunchForMaintenance(subject), nil)
				if err != nil {
					return err
				}
				receipt, err := runner.StepCompleted(ctx, MaintenanceTargetLaunchStep, func(context.Context) error { return state.targetErr })
				if err != nil {
					return err
				}
				if err := journal.Complete(debt, receipt); err != nil {
					return err
				}
				require.Error(t, journal.Complete(debt, receipt), "copied target completion cannot settle twice")
				if state.targetReturned != nil {
					state.targetReturned()
				}
				return errors.New("startup rejected known failed target")
			}
		},
		func(ctx context.Context, run func(context.Context) error, _ MaintenancePhysicalSubject) error {
			return run(ctx)
		},
		func(_ context.Context, subject MaintenancePhysicalSubject) (MaintenancePhysicalEvidence, error) {
			return NewMaintenanceTargetAbsent(subject)
		},
	))
}

func TestMaintenanceCompensationClassifiesSourceAfterAuxiliaryTargetRefusal(t *testing.T) {
	f := beginBoundMaintenance(t, "compensation-image-refusal")
	target := f.appendAndBind(t)
	cause := errors.New("replacement image could not be pulled")
	state := &compensationTestState{auxiliaryErr: cause}
	bindCompensationTest(t, f.settlement, state)
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	outcome := f.settlement.ExecuteMaintenance(t.Context(), execution)
	failure, ok := outcome.(MaintenanceExecutionFailure)
	require.True(t, ok, "%T: %v", outcome, outcome)
	require.True(t, failure.SourceRecovered())
	require.ErrorIs(t, failure.Cause(), cause)
	proof, err := f.settlement.FailMaintenance(failure, backend.ReasonImagePullFailed, "image unavailable; original source ready")
	require.NoError(t, err)
	require.True(t, proof.Valid())
}

func TestMaintenanceCompensationRestoresSourceAfterCompletedFailedTarget(t *testing.T) {
	f := beginBoundMaintenance(t, "compensation-live")
	target := f.appendAndBind(t)
	state := new(compensationTestState)
	bindCompensationTest(t, f.settlement, state)
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	outcome := f.settlement.ExecuteMaintenance(t.Context(), execution)
	failed, ok := outcome.(MaintenanceExecutionFailure)
	require.True(t, ok, "%T: %v", outcome, outcome)
	require.True(t, failed.SourceRecovered())
	require.Equal(t, 1, state.launches)
	proof, err := f.settlement.FailMaintenance(failed, backend.ReasonInternal, "startup failed; source restored")
	require.NoError(t, err)
	require.True(t, proof.Valid())
	_, err = state.journal.Begin(VolumeLaunchForCompensation(MaintenanceCompensationSubject{}), nil)
	require.Error(t, err)
}

func TestMaintenanceCompensationNeverUsesAmbiguousTargetOrZeroReceipt(t *testing.T) {
	f := beginBoundMaintenance(t, "compensation-ambiguous")
	target := f.appendAndBind(t)
	state := &compensationTestState{targetErr: errors.New("daemon reply lost")}
	bindCompensationTest(t, f.settlement, state)
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	require.Error(t, state.journal.Complete(VolumeLaunchDebt{}, substratemutation.CompletedStep{}))
	outcome := f.settlement.ExecuteMaintenance(t.Context(), execution)
	require.IsType(t, MaintenanceExecutionAmbiguous{}, outcome)
	require.Zero(t, state.launches)
	assertCompensationJournalPhase(t, f.settlement, execution.subject.Intent(), compensationTargetDispatching, 1)
	coordinator := newTestRecoveryCoordinator(t, nil, f.settlement, nil)
	_, err = coordinator.WithLease(t.Context(), target.LeaseUUID(), func(scope LeaseRecoveryScope) error {
		_, err := f.settlement.RecoverMaintenanceCompensation(t.Context(), scope, execution.subject.Intent())
		return err
	})
	require.ErrorContains(t, err, "not durably settled")
	require.Zero(t, state.launches)
	require.IsType(t, MaintenanceExecutionAmbiguous{}, f.settlement.ExecuteMaintenance(t.Context(), execution), "copied original worker cannot recapture or relaunch")
}

func TestMaintenanceCompensationPrelaunchFailureHasUndispatchedSourceAuthority(t *testing.T) {
	f := beginBoundMaintenance(t, "compensation-prelaunch")
	target := f.appendAndBind(t)
	state := &compensationTestState{beforeTargetErr: errors.New("source mount preparation failed before target launch")}
	bindCompensationTest(t, f.settlement, state)
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	outcome := f.settlement.ExecuteMaintenance(t.Context(), execution)
	failed, ok := outcome.(MaintenanceExecutionFailure)
	require.True(t, ok, "%T: %v", outcome, outcome)
	require.True(t, failed.SourceRecovered())
	require.Equal(t, 1, state.launches)
	assertCompensationJournalPhase(t, f.settlement, execution.subject.Intent(), compensationSourceReady, 0)
}

func TestMaintenanceCompensationDoesNotReissueAmbiguousSourceLaunch(t *testing.T) {
	f := beginBoundMaintenance(t, "compensation-source-ambiguous")
	target := f.appendAndBind(t)
	state := &compensationTestState{sourceErr: errors.New("source create response lost")}
	bindCompensationTest(t, f.settlement, state)
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	require.IsType(t, MaintenanceExecutionAmbiguous{}, f.settlement.ExecuteMaintenance(t.Context(), execution))
	require.Equal(t, 1, state.launches)
	coordinator := newTestRecoveryCoordinator(t, nil, f.settlement, nil)
	var outcome MaintenanceExecutionOutcome
	_, err = coordinator.WithLease(t.Context(), target.LeaseUUID(), func(scope LeaseRecoveryScope) error {
		outcome, err = f.settlement.RecoverMaintenanceCompensation(t.Context(), scope, execution.subject.Intent())
		return err
	})
	require.NoError(t, err)
	require.IsType(t, MaintenanceExecutionAmbiguous{}, outcome)
	require.Equal(t, 1, state.launches)
	state.sourceReady = true // the original request became observable; never redispatch
	_, err = coordinator.WithLease(t.Context(), target.LeaseUUID(), func(scope LeaseRecoveryScope) error {
		outcome, err = f.settlement.RecoverMaintenanceCompensation(t.Context(), scope, execution.subject.Intent())
		return err
	})
	require.NoError(t, err)
	require.IsType(t, MaintenanceExecutionFailure{}, outcome)
	require.True(t, outcome.(MaintenanceExecutionFailure).SourceRecovered())
	require.Equal(t, 1, state.launches)
}

func TestMaintenanceCompensationResumesPreparedSourceAfterStoreReopen(t *testing.T) {
	f := beginBoundMaintenance(t, "compensation-reopen")
	target := f.appendAndBind(t)
	state := &compensationTestState{beforeSourceErr: errors.New("process stopped before source launch")}
	bindCompensationTest(t, f.settlement, state)
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	require.IsType(t, MaintenanceExecutionAmbiguous{}, f.settlement.ExecuteMaintenance(t.Context(), execution))
	require.Zero(t, state.launches)
	oldSubject := state.sourceSubject
	reopened := reopenCompensationSettlement(t, f)
	state.beforeSourceErr = nil
	bindCompensationTest(t, reopened, state)
	_, err = state.journal.Begin(VolumeLaunchForCompensation(oldSubject), nil)
	require.Error(t, err, "old open-store capability cannot launch after reopen")
	intent, found, err := reopened.GetMaintenanceIntent(target.LeaseUUID())
	require.NoError(t, err)
	require.True(t, found)
	coordinator := newTestRecoveryCoordinator(t, nil, reopened, nil)
	var outcome MaintenanceExecutionOutcome
	_, err = coordinator.WithLease(t.Context(), target.LeaseUUID(), func(scope LeaseRecoveryScope) error {
		outcome, err = reopened.RecoverMaintenanceCompensation(t.Context(), scope, intent)
		return err
	})
	require.NoError(t, err)
	require.IsType(t, MaintenanceExecutionFailure{}, outcome)
	require.True(t, outcome.(MaintenanceExecutionFailure).SourceRecovered())
	require.Equal(t, 1, state.launches)
	require.NoError(t, reopened.callbacks.Healthy())
}

func TestMaintenanceCompensationCapacityFailurePrecedesTargetEffects(t *testing.T) {
	f := beginBoundMaintenance(t, "compensation-capacity")
	target := f.appendAndBind(t)
	state := new(compensationTestState)
	bindCompensationTest(t, f.settlement, state)
	f.settlement.compensation.prepare = func(context.Context, MaintenancePhysicalSubject) (MaintenanceSourceCapture, error) {
		return CapturedMaintenanceSource([]byte(`{"data":"` + strings.Repeat("a", maxMaintenanceIntentEntryBytes) + `"}`))
	}
	f.settlement.compensation.validate = func(MaintenancePhysicalSubject, []byte) error { return nil }
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	outcome := f.settlement.ExecuteMaintenance(t.Context(), execution)
	require.IsType(t, MaintenanceExecutionFailure{}, outcome)
	require.ErrorContains(t, outcome.(MaintenanceExecutionFailure).Cause(), "capacity")
	require.Zero(t, state.launches)
	pending, err := f.settlement.CompensationPending(execution.subject.Intent())
	require.NoError(t, err)
	require.False(t, pending, "failed durable source write must not leave a partial record")
}

func TestMaintenanceCompensationCannotReverseActivatedTarget(t *testing.T) {
	f := beginBoundMaintenance(t, "compensation-activation")
	target := f.appendAndBind(t)
	state := new(compensationTestState)
	bindCompensationTest(t, f.settlement, state)
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	require.NoError(t, f.settlement.prepareCompensation(t.Context(), execution))
	release, _ := execution.subject.TargetRelease()
	ids, services := testPhysicalProjection(release)
	evidence, err := NewMaintenanceTargetReady(execution.subject, ids, services)
	require.NoError(t, err)
	active, err := f.settlement.ActivateMaintenance(MaintenanceExecutionSuccess{settlement: f.settlement, execution: execution, ready: evidence.targetReady})
	require.NoError(t, err)
	require.True(t, active.Valid())
	_, err = f.settlement.beginCompensation(execution.target)
	require.Error(t, err)
	require.Zero(t, state.launches)
}

func TestMaintenanceCompensationUnavailableSourceDoesNotBlockRestart(t *testing.T) {
	f := beginBoundMaintenance(t, "compensation-unavailable")
	target := f.appendAndBind(t)
	state := new(compensationTestState)
	bindCompensationTest(t, f.settlement, state)
	f.settlement.compensation.prepare = func(context.Context, MaintenancePhysicalSubject) (MaintenanceSourceCapture, error) {
		return UnavailableMaintenanceSource(), nil
	}
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	outcome := f.settlement.ExecuteMaintenance(t.Context(), execution)
	require.IsType(t, MaintenanceExecutionAmbiguous{}, outcome)
	require.Zero(t, state.launches)
	pending, err := f.settlement.CompensationPending(execution.subject.Intent())
	require.NoError(t, err)
	require.False(t, pending)
	require.NoError(t, f.stores.callbacks.Healthy())
}

func TestMaintenanceCompensationCannotMintFromWrongStoredGeneration(t *testing.T) {
	f := beginBoundMaintenance(t, "compensation-record-fence")
	target := f.appendAndBind(t)
	state := &compensationTestState{beforeSourceErr: errors.New("not launched")}
	bindCompensationTest(t, f.settlement, state)
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	require.IsType(t, MaintenanceExecutionAmbiguous{}, f.settlement.ExecuteMaintenance(t.Context(), execution))
	require.NoError(t, f.stores.callbacks.update(func(tx *bolt.Tx) error {
		record, err := readCompensationTx(tx, execution.subject.Intent())
		if err != nil {
			return err
		}
		record.SourceDigest = strings.Repeat("1", 64)
		return writeCompensationTx(tx, *record)
	}))
	_, err = f.settlement.beginCompensation(execution.target)
	require.ErrorContains(t, err, "differs from exact intent")
	require.Zero(t, state.launches)
}

func assertCompensationJournalPhase(t *testing.T, s *MaintenanceSettlement, intent MaintenanceIntentClaim, phase compensationPhase, debts int) {
	t.Helper()
	require.NoError(t, s.callbacks.view(func(tx *bolt.Tx) error {
		record, err := readCompensationTx(tx, intent)
		if err != nil {
			return err
		}
		require.NotNil(t, record)
		require.Equal(t, phase, record.Phase)
		require.Equal(t, debts, tx.Bucket(volumeLaunchDebtBucketName).Stats().KeyN)
		return nil
	}))
}

func reopenCompensationSettlement(t *testing.T, f boundMaintenanceFixture) *MaintenanceSettlement {
	t.Helper()
	require.NoError(t, f.stores.callbacks.Close())
	require.NoError(t, f.stores.releases.Close())
	callbacks, err := OpenIdentityBoundCallbackStore(CallbackStoreConfig{DBPath: f.stores.callbackPath}, f.stores.storage, f.stores.gate)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbacks.Close()) })
	releases, err := OpenIdentityBoundReleaseStore(ReleaseStoreConfig{DBPath: f.stores.releasePath}, f.stores.storage, f.stores.gate)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, releases.Close()) })
	reopened, err := NewMaintenanceSettlement(callbacks, releases)
	require.NoError(t, err)
	return reopened
}

func TestMaintenanceCompensationSourceEffectsSettleBeforeUnhealthyTerminal(t *testing.T) {
	f := beginBoundMaintenance(t, "compensation-unhealthy")
	target := f.appendAndBind(t)
	state := &compensationTestState{sourceUnready: true}
	bindCompensationTest(t, f.settlement, state)
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	outcome := f.settlement.ExecuteMaintenance(t.Context(), execution)
	failed, ok := outcome.(MaintenanceExecutionFailure)
	require.True(t, ok, "%T: %v", outcome, outcome)
	require.False(t, failed.SourceRecovered())
	assertCompensationJournalPhase(t, f.settlement, execution.subject.Intent(), compensationSourceFailed, 0)
	proof, err := f.settlement.FailMaintenance(failed, backend.ReasonInternal, "source launch settled but source unhealthy")
	require.NoError(t, err)
	require.True(t, proof.Valid())
}

func TestMaintenanceCompensationAmbiguousSourceDebtSurvivesReopen(t *testing.T) {
	f := beginBoundMaintenance(t, "compensation-ambiguous-reopen")
	target := f.appendAndBind(t)
	state := &compensationTestState{sourceErr: errors.New("Start reply lost")}
	bindCompensationTest(t, f.settlement, state)
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	require.IsType(t, MaintenanceExecutionAmbiguous{}, f.settlement.ExecuteMaintenance(t.Context(), execution))
	assertCompensationJournalPhase(t, f.settlement, execution.subject.Intent(), compensationSourceDispatching, 1)
	reopened := reopenCompensationSettlement(t, f)
	bindCompensationTest(t, reopened, state)
	intent, found, err := reopened.GetMaintenanceIntent(target.LeaseUUID())
	require.NoError(t, err)
	require.True(t, found)
	require.ErrorIs(t, state.journal.CheckNamespace(target.LeaseUUID()), ErrVolumeLaunchUnsettled)
	coordinator := newTestRecoveryCoordinator(t, nil, reopened, nil)
	_, err = coordinator.WithLease(t.Context(), target.LeaseUUID(), func(scope LeaseRecoveryScope) error {
		outcome, err := reopened.RecoverMaintenanceCompensation(t.Context(), scope, intent)
		require.NoError(t, err)
		require.IsType(t, MaintenanceExecutionAmbiguous{}, outcome)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, state.launches)
	assertCompensationJournalPhase(t, reopened, intent, compensationSourceDispatching, 1)
	require.NoError(t, reopened.callbacks.Healthy())
}

func TestMaintenanceCompensationCloseRevokesSourcePlanAndRetainsAmbiguousDebt(t *testing.T) {
	f := beginBoundMaintenance(t, "compensation-close")
	target := f.appendAndBind(t)
	state := &compensationTestState{sourceErr: errors.New("Start reply lost")}
	bindCompensationTest(t, f.settlement, state)
	execution, err := f.settlement.StartMaintenanceExecution(target)
	require.NoError(t, err)
	require.IsType(t, MaintenanceExecutionAmbiguous{}, f.settlement.ExecuteMaintenance(t.Context(), execution))
	closeSettlement := newCloseSettlementForTest(t, f.stores)
	_ = admitSettlementClose(t, closeSettlement, target.LeaseUUID(), false)
	require.NoError(t, f.stores.callbacks.view(func(tx *bolt.Tx) error {
		require.Nil(t, tx.Bucket(maintenanceCompensationBucket).Get([]byte(target.MaintenanceID().String())))
		return nil
	}))
	_, err = f.settlement.beginCompensation(execution.target)
	require.Error(t, err, "closed head must revoke copied compensation authority")
	require.ErrorIs(t, state.journal.CheckNamespace(target.LeaseUUID()), ErrVolumeLaunchUnsettled, "close cannot erase a possible outstanding daemon request")
	require.NoError(t, f.stores.callbacks.Healthy())
}

func TestMaintenanceCompensationTargetDeadlineAllowsSourceButCancellationDoesNot(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout bool
	}{{"deadline", true}, {"close-cancellation", false}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := beginBoundMaintenance(t, "compensation-context")
				target := f.appendAndBind(t)
				targetCtx, cancel := context.WithTimeout(t.Context(), time.Hour)
				defer cancel()
				state := &compensationTestState{targetReturned: func() {
					if tc.timeout {
						time.Sleep(time.Hour)
						<-targetCtx.Done()
					} else {
						cancel()
					}
				}}
				bindCompensationTest(t, f.settlement, state)
				execution, err := f.settlement.StartMaintenanceExecution(target)
				require.NoError(t, err)
				outcome := f.settlement.ExecuteMaintenance(targetCtx, execution)
				if tc.timeout {
					require.ErrorIs(t, targetCtx.Err(), context.DeadlineExceeded)
					require.IsType(t, MaintenanceExecutionFailure{}, outcome)
					require.Equal(t, 1, state.launches, "expired target startup deadline must allow bounded source recovery")
				} else {
					require.IsType(t, MaintenanceExecutionAmbiguous{}, outcome)
					require.Zero(t, state.launches, "close cancellation cannot dispatch source effects")
				}
			})
		})
	}
}

func TestMaintenanceCompensationExecutionLifetimeIgnoresOnlyTargetDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lifetime, shutdown := context.WithCancel(t.Context())
		defer shutdown()
		target, cancelTarget := context.WithTimeout(t.Context(), time.Second)
		defer cancelTarget()
		time.Sleep(time.Second)
		<-target.Done()
		require.ErrorIs(t, target.Err(), context.DeadlineExceeded)
		source, cancelSource := compensationExecutionContext(target, lifetime)
		defer cancelSource()
		require.NoError(t, source.Err(), "expired target timeout must not consume source recovery budget")
		deadline, ok := source.Deadline()
		require.True(t, ok)
		require.Equal(t, 2*time.Minute, time.Until(deadline))
		shutdown()
		require.ErrorIs(t, source.Err(), context.Canceled, "backend shutdown cancels source recovery")
	})
}
