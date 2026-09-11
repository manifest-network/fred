package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
	"github.com/manifest-network/fred/internal/backendidentity"
)

type testOperationMutation struct {
	run func(context.Context) error
}

func bindTestOperationMutation(
	t *testing.T,
	settlement *OperationSettlement,
	classify func(OperationPhysicalSubject) (OperationPhysicalEvidence, error),
) {
	t.Helper()
	err := newTestOperationMutation(settlement, classify)
	require.NoError(t, err)
}

func bindRefusingTestOperationMutation(t *testing.T, settlement *OperationSettlement) {
	t.Helper()
	err := BindOperationSubstrateExecutor(
		settlement,
		func(ctx context.Context, _ string) (context.Context, func(), error) {
			return ctx, func() {}, nil
		},
		func(context.Context, string, error) error { return nil },
		func(substratemutation.Runner, OperationPhysicalSubject) struct{} { return struct{}{} },
		func(context.Context, struct{}, OperationPhysicalSubject) error {
			return errors.New("refused before entering a tenant substrate step")
		},
		func(context.Context, OperationPhysicalSubject) (OperationPhysicalEvidence, error) {
			return OperationPhysicalEvidence{}, errors.New("a refused execution must not reach classification")
		},
	)
	require.NoError(t, err)
}

func newTestOperationMutation(
	settlement *OperationSettlement,
	classify func(OperationPhysicalSubject) (OperationPhysicalEvidence, error),
) error {
	if classify == nil {
		classify = func(subject OperationPhysicalSubject) (OperationPhysicalEvidence, error) {
			release, ok := subject.ExpectedRelease()
			if !ok {
				return OperationPhysicalEvidence{}, errors.New("missing expected release")
			}
			ids, services := testPhysicalProjection(release)
			return NewOperationTargetReady(subject, ids, services)
		}
	}
	return BindOperationSubstrateExecutor(
		settlement,
		func(ctx context.Context, _ string) (context.Context, func(), error) {
			return ctx, func() {}, nil
		},
		func(context.Context, string, error) error { return nil },
		func(runner substratemutation.Runner, _ OperationPhysicalSubject) testOperationMutation {
			return testOperationMutation{run: func(ctx context.Context) error {
				return runner.Step(ctx, "test operation mutation", func(context.Context) error { return nil })
			}}
		},
		func(ctx context.Context, mutation testOperationMutation, _ OperationPhysicalSubject) error {
			return mutation.run(ctx)
		},
		func(_ context.Context, subject OperationPhysicalSubject) (OperationPhysicalEvidence, error) {
			return classify(subject)
		},
	)
}

func testPhysicalProjection(release Release) ([]string, map[string][]string) {
	var ids []string
	services := make(map[string][]string, len(release.Items))
	for _, item := range release.Items {
		for replica := range item.Quantity {
			id := fmt.Sprintf("%s-%d", item.ServiceName, replica)
			ids = append(ids, id)
			services[item.ServiceName] = append(services[item.ServiceName], id)
		}
	}
	return ids, services
}

type operationHandoffStores struct {
	callbacks            *CallbackStore
	releases             *ReleaseStore
	settlement           *OperationSettlement
	restore              *RestoreSettlement
	retentions           *RetentionStore
	callbackPath         string
	releasePath          string
	alternateReleasePath string
	retentionPath        string
	storage              backendidentity.VerifiedStorage
	gate                 *backendidentity.StorageAuthorityGate
}

func openOperationHandoffStores(t *testing.T, backendName string) operationHandoffStores {
	t.Helper()
	dir := t.TempDir()
	callbackPath := filepath.Join(dir, "callbacks.db")
	releasePath := filepath.Join(dir, "releases.db")
	alternateReleasePath := filepath.Join(dir, "alternate-releases.db")
	retentionPath := filepath.Join(dir, "retention.db")
	markerPath := filepath.Join(dir, "storage.json")
	anchorPath := filepath.Join(dir, "storage-anchor.json")

	pair, err := backendidentity.BindMarkerPair(markerPath, anchorPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pair.Close()) })
	callbacksBound, err := BindAuthoritativeStorePath(callbackPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, callbacksBound.Close()) }()
	releasesBound, err := BindAuthoritativeStorePath(releasePath)
	require.NoError(t, err)
	defer func() { require.NoError(t, releasesBound.Close()) }()
	alternateReleasesBound, err := BindAuthoritativeStorePath(alternateReleasePath)
	require.NoError(t, err)
	defer func() { require.NoError(t, alternateReleasesBound.Close()) }()
	retentionsBound, err := BindAuthoritativeStorePath(retentionPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, retentionsBound.Close()) }()

	hooks := backendidentity.MarkerPairStoreHooks{
		Profile: backendidentity.InitializationProfileFresh,
		Prepare: func(
			storage backendidentity.PendingStorage,
			profile backendidentity.InitializationProfile,
		) error {
			if err := PrepareBoundCallbackStoreStorage(callbacksBound, storage, profile); err != nil {
				return err
			}
			if err := PrepareBoundReleaseStoreStorage(releasesBound, storage, profile); err != nil {
				return err
			}
			if err := PrepareBoundReleaseStoreStorage(alternateReleasesBound, storage, profile); err != nil {
				return err
			}
			return PrepareBoundRetentionStoreStorage(retentionsBound, storage, profile)
		},
		Check: func(storage backendidentity.PendingStorage) error {
			if err := CheckBoundCallbackStoreStorage(callbacksBound, storage); err != nil {
				return err
			}
			if err := CheckBoundReleaseStoreStorage(releasesBound, storage); err != nil {
				return err
			}
			if err := CheckBoundReleaseStoreStorage(alternateReleasesBound, storage); err != nil {
				return err
			}
			return CheckBoundRetentionStoreStorage(retentionsBound, storage)
		},
		Verify: func(storage backendidentity.VerifiedStorage) error {
			if err := VerifyBoundCallbackStoreStorage(callbacksBound, storage); err != nil {
				return err
			}
			if err := VerifyBoundReleaseStoreStorage(releasesBound, storage); err != nil {
				return err
			}
			if err := VerifyBoundReleaseStoreStorage(alternateReleasesBound, storage); err != nil {
				return err
			}
			return VerifyBoundRetentionStoreStorage(retentionsBound, storage)
		},
	}
	storage, err := pair.InitializeWithStores(backendName, "test-substrate", hooks)
	require.NoError(t, err)
	gate := newTestStorageAuthorityGate(t)
	callbacks, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: callbackPath}, storage, gate,
	)
	require.NoError(t, err)
	releases, err := OpenIdentityBoundReleaseStore(
		ReleaseStoreConfig{DBPath: releasePath}, storage, gate,
	)
	require.NoError(t, err)
	retentions, err := OpenIdentityBoundRetentionStore(
		RetentionStoreConfig{DBPath: retentionPath}, storage, gate,
	)
	require.NoError(t, err)
	settlement, err := NewOperationSettlement(callbacks, releases)
	require.NoError(t, err)
	restore, err := NewRestoreSettlement(settlement, retentions)
	require.NoError(t, err)
	stores := operationHandoffStores{
		callbacks: callbacks, releases: releases, settlement: settlement,
		restore: restore, retentions: retentions,
		callbackPath: callbackPath, releasePath: releasePath,
		alternateReleasePath: alternateReleasePath, retentionPath: retentionPath,
		storage: storage, gate: gate,
	}
	t.Cleanup(func() {
		if stores.callbacks != nil {
			require.NoError(t, stores.callbacks.Close())
		}
		if stores.releases != nil {
			require.NoError(t, stores.releases.Close())
		}
		if stores.retentions != nil {
			require.NoError(t, stores.retentions.Close())
		}
	})
	return stores
}

func beginHandoffOperation(
	t *testing.T,
	settlement *OperationSettlement,
	spec OperationIntentSpec,
) OperationIntentClaim {
	t.Helper()
	candidate, err := settlement.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	admission, err := settlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	return createdOperationClaim(t, admission)
}

func commitHandoffOperation(
	t *testing.T,
	settlement *OperationSettlement,
	candidate OperationReleaseCandidate,
) OperationReleaseCommitted {
	t.Helper()
	if settlement.execute == nil {
		bindTestOperationMutation(t, settlement, nil)
	}
	execution, err := settlement.StartOperationExecution(candidate)
	require.NoError(t, err)
	outcome := settlement.ExecuteOperation(context.Background(), execution)
	success, ok := outcome.(OperationExecutionSuccess)
	require.True(t, ok, "physical result = %T, want exact target ready", outcome)
	committed, err := settlement.CommitOperationSuccess(success)
	require.NoError(t, err)
	return committed
}

func commitHandoffRefusal(
	t *testing.T,
	settlement *OperationSettlement,
	claim OperationIntentClaim,
) OperationReleaseUncommitted {
	t.Helper()
	candidate, err := settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	failure, err := settlement.RefuseOperationExecution(candidate)
	require.NoError(t, err)
	uncommitted, err := settlement.CommitOperationFailure(failure)
	require.NoError(t, err)
	return uncommitted
}

func TestOperationReleaseCandidateRejectsZeroCrossStoreAndCrossLineage(t *testing.T) {
	storesA := openOperationHandoffStores(t, "docker-a")
	storesB := openOperationHandoffStores(t, "docker-b")
	claim := beginHandoffOperation(
		t, storesA.settlement, testOperationIntentSpec(t, "handoff-lineage"),
	)

	_, err := storesB.settlement.PrepareOperationRelease(claim)
	require.Error(t, err)
	require.Error(t, storesA.settlement.CheckOperationReleaseCapacity(OperationReleaseCandidate{}))
	_, err = storesA.settlement.appendOperationRelease(OperationReleaseCandidate{})
	require.Error(t, err)

	candidate, err := storesA.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	_, err = storesB.settlement.appendOperationRelease(candidate)
	require.ErrorContains(t, err, "another journal pair")
	active, err := storesB.releases.LatestActive(claim.LeaseUUID())
	require.NoError(t, err)
	assert.Nil(t, active)
}

func TestOperationReleaseCandidateDetachesSourceClaim(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	specA := testOperationIntentSpec(t, "handoff-a")
	claimA := beginHandoffOperation(t, stores.settlement, specA)
	candidateA, err := stores.settlement.PrepareOperationRelease(claimA)
	require.NoError(t, err)

	// Mutating the source claim after handoff cannot change the detached release.
	durableA := claimA
	durableA.entry.Items[0].SKU = "caller-mutated"
	durableA.entry.ResourceProfiles[0].MemoryMB++
	durableA.entry.Manifest[0] ^= 0xff
	commitHandoffOperation(t, stores.settlement, candidateA)
	active, err := stores.releases.LatestActive(specA.LeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, "small", active.Items[0].SKU)
	assert.Equal(t, int64(512), active.ResourceProfiles[0].MemoryMB)
	assert.Equal(t, specA.Manifest, active.Manifest)
}

func TestOperationReleaseCandidateAppendIsDurablyIdempotent(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	spec := testOperationIntentSpec(t, "handoff-idempotent")
	claim := beginHandoffOperation(t, stores.settlement, spec)
	candidate, err := stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	bindTestOperationMutation(t, stores.settlement, nil)
	execution, err := stores.settlement.StartOperationExecution(candidate)
	require.NoError(t, err)
	outcome := stores.settlement.ExecuteOperation(context.Background(), execution)
	success, ok := outcome.(OperationExecutionSuccess)
	require.True(t, ok)
	copied := success
	_, err = stores.settlement.CommitOperationSuccess(success)
	require.NoError(t, err)
	_, err = stores.settlement.CommitOperationSuccess(success)
	require.NoError(t, err)
	_, err = stores.settlement.CommitOperationSuccess(copied)
	require.NoError(t, err)
	history, err := stores.releases.List(spec.LeaseUUID)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, 1, history[0].Version)
	assert.Equal(t, claim.OperationID(), history[0].OperationID)
}

func TestOperationReleaseCandidateCannotReactivateSupersededOperation(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	specA := testOperationIntentSpec(t, "handoff-stale-a")
	claimA := beginHandoffOperation(t, stores.settlement, specA)
	candidateA, err := stores.settlement.PrepareOperationRelease(claimA)
	require.NoError(t, err)
	committedA := commitHandoffOperation(t, stores.settlement, candidateA)
	_, err = stores.settlement.resolveOperationSuccess(committedA)
	require.NoError(t, err)
	pending, err := stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.NoError(t, stores.callbacks.removeEntry(pending[0]))

	specB := testOperationIntentSpec(t, "handoff-stale-b")
	specB.LeaseUUID = specA.LeaseUUID
	specB.Manifest = []byte(`{"services":{"app":{"image":"example.invalid/app:2"}}}`)
	claimB := beginHandoffOperation(t, stores.settlement, specB)
	candidateB, err := stores.settlement.PrepareOperationRelease(claimB)
	require.NoError(t, err)
	commitHandoffOperation(t, stores.settlement, candidateB)

	_, err = stores.settlement.appendOperationRelease(candidateA)
	require.ErrorIs(t, err, ErrOperationIntentConflict)
	history, err := stores.releases.List(specA.LeaseUUID)
	require.NoError(t, err)
	require.Len(t, history, 2)
	assert.Equal(t, "superseded", history[0].Status)
	assert.Equal(t, claimA.OperationID(), history[0].OperationID)
	assert.Equal(t, "active", history[1].Status)
	assert.Equal(t, claimB.OperationID(), history[1].OperationID)
}

func TestOperationFailureProofCannotSettleSuccessorGeneration(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	specA := testOperationIntentSpec(t, "handoff-failure-aba-a")
	claimA := beginHandoffOperation(t, stores.settlement, specA)
	proofA := commitHandoffRefusal(t, stores.settlement, claimA)
	require.True(t, proofA.Valid())

	_, err := stores.settlement.resolveOperationFailure(proofA, "first operation failed")
	require.NoError(t, err)
	pending, err := stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.NoError(t, stores.callbacks.removeEntry(pending[0]))

	specB := testOperationIntentSpec(t, "handoff-failure-aba-b")
	specB.LeaseUUID = specA.LeaseUUID
	claimB := beginHandoffOperation(t, stores.settlement, specB)

	_, err = stores.settlement.resolveOperationFailure(proofA, "stale worker failure")
	require.ErrorIs(t, err, ErrOperationIntentConflict)
	intents, err := stores.settlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Equal(t, claimB.OperationID(), intents[0].OperationID())
}

func TestOperationReleaseCandidateRejectsChangedExistingGeneration(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	spec := testOperationIntentSpec(t, "handoff-changed")
	claim := beginHandoffOperation(t, stores.settlement, spec)
	candidate, err := stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	bindTestOperationMutation(t, stores.settlement, nil)
	execution, err := stores.settlement.StartOperationExecution(candidate)
	require.NoError(t, err)
	outcome := stores.settlement.ExecuteOperation(context.Background(), execution)
	success, ok := outcome.(OperationExecutionSuccess)
	require.True(t, ok)
	_, err = stores.settlement.CommitOperationSuccess(success)
	require.NoError(t, err)
	require.NoError(t, stores.releases.updateLatestStatus(
		spec.LeaseUUID,
		"failed",
		backend.ReasonInternal,
		"changed after commit",
	))

	_, err = stores.settlement.CommitOperationSuccess(success)
	require.ErrorIs(t, err, ErrOperationReleaseConflict)
	history, err := stores.releases.List(spec.LeaseUUID)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, "failed", history[0].Status)
}

func TestOperationReleaseCandidateMustBeRemintedAfterRestart(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	spec := testOperationIntentSpec(t, "handoff-restart")
	claim := beginHandoffOperation(t, stores.settlement, spec)
	probe, err := stores.settlement.NewOperationIntentProbe(spec.LeaseUUID, spec.CallbackURL)
	require.NoError(t, err)
	candidate, err := stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)

	require.NoError(t, stores.releases.Close())
	stores.releases = nil
	reopenedRelease, err := OpenIdentityBoundReleaseStore(
		ReleaseStoreConfig{DBPath: stores.releasePath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	stores.releases = reopenedRelease
	reopenedSettlement, err := NewOperationSettlement(stores.callbacks, reopenedRelease)
	require.NoError(t, err)
	_, err = reopenedSettlement.StartOperationExecution(candidate)
	require.ErrorContains(t, err, "another journal pair")

	require.NoError(t, stores.callbacks.Close())
	stores.callbacks = nil
	reopenedCallbacks, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	stores.callbacks = reopenedCallbacks
	reopenedSettlement, err = NewOperationSettlement(reopenedCallbacks, reopenedRelease)
	require.NoError(t, err)
	_, err = reopenedSettlement.PrepareOperationRelease(claim)
	require.ErrorContains(t, err, "another journal pair")
	_, err = reopenedSettlement.ProbeOperationIntent(probe)
	require.ErrorContains(t, err, "another journal pair")
	states, err := reopenedSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, states, 1)
	recovered := states[0]
	reminted, err := reopenedSettlement.PrepareOperationRelease(recovered)
	require.NoError(t, err)
	require.NoError(t, reopenedSettlement.CheckOperationReleaseCapacity(reminted))
	commitHandoffOperation(t, reopenedSettlement, reminted)
}

func TestOperationExecutionStartInvalidatesEveryPreEffectCandidateCopy(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	claim := beginHandoffOperation(
		t, stores.settlement, testOperationIntentSpec(t, "execution-start-copy"),
	)
	candidate, err := stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	bindTestOperationMutation(t, stores.settlement, nil)
	copied := candidate

	execution, err := stores.settlement.StartOperationExecution(candidate)
	require.NoError(t, err)
	require.NotNil(t, execution.settlement)
	_, err = stores.settlement.StartOperationExecution(copied)
	require.Error(t, err)
	_, err = stores.settlement.RefuseOperationExecution(copied)
	require.Error(t, err)

	claims, err := stores.settlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	durable := claims[0]
	assert.False(t, durable.entry.EffectNotStarted)
}

func TestStartedOperationRefusedBeforeFirstSubstrateStepCanCommitFailure(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	claim := beginHandoffOperation(
		t, stores.settlement, testOperationIntentSpec(t, "started-refusal"),
	)
	candidate, err := stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	bindRefusingTestOperationMutation(t, stores.settlement)
	execution, err := stores.settlement.StartOperationExecution(candidate)
	require.NoError(t, err)

	outcome := stores.settlement.ExecuteOperation(t.Context(), execution)
	failure, ok := outcome.(OperationExecutionFailure)
	require.True(t, ok, "outcome = %T, want definitive guarded refusal", outcome)
	require.True(t, failure.Valid())
	uncommitted, err := stores.settlement.CommitOperationFailure(failure)
	require.NoError(t, err)
	require.True(t, uncommitted.Valid())

	var zero OperationExecutionFailure
	assert.False(t, zero.Valid())
	_, err = stores.settlement.CommitOperationFailure(zero)
	require.Error(t, err)

	wrongPhase := failure
	wrongPhase.kind = operationExecutionRefusedBeforeStart
	assert.False(t, wrongPhase.Valid())
	_, err = stores.settlement.CommitOperationFailure(wrongPhase)
	require.Error(t, err)
}

func TestOperationExecutionPhaseMissingBitFailsSafeAndUnknownFieldIsRejected(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	claim := beginHandoffOperation(
		t, stores.settlement, testOperationIntentSpec(t, "execution-phase-schema"),
	)
	require.True(t, claim.entry.EffectNotStarted)

	legacy := *cloneOperationAuthority(claim.operationAuthority).entry
	legacy.EffectNotStarted = false
	data, err := json.Marshal(legacy)
	require.NoError(t, err)
	decoded, err := decodeOperationIntent([]byte(legacy.LeaseUUID), data)
	require.NoError(t, err)
	assert.False(t, decoded.entry.EffectNotStarted,
		"a missing legacy bit must be treated as potentially started")

	var object map[string]any
	require.NoError(t, json.Unmarshal(data, &object))
	object["unexpected_execution_phase"] = true
	data, err = json.Marshal(object)
	require.NoError(t, err)
	_, err = decodeOperationIntent([]byte(legacy.LeaseUUID), data)
	require.Error(t, err)
}

func TestOperationStartedPhaseRecoversTypedPhysicalOutcomeAfterReopen(t *testing.T) {
	for _, test := range []struct {
		name    string
		success bool
	}{
		{name: "absent becomes failure", success: false},
		{name: "present becomes success", success: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			stores := openOperationHandoffStores(t, "docker-a")
			claim := beginHandoffOperation(
				t, stores.settlement, testOperationIntentSpec(t, "started-reopen-"+test.name),
			)
			candidate, err := stores.settlement.PrepareOperationRelease(claim)
			require.NoError(t, err)
			bindTestOperationMutation(t, stores.settlement, nil)
			_, err = stores.settlement.StartOperationExecution(candidate)
			require.NoError(t, err)

			require.NoError(t, stores.callbacks.Close())
			stores.callbacks = nil
			require.NoError(t, stores.releases.Close())
			stores.releases = nil
			callbacks, err := OpenIdentityBoundCallbackStore(
				CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, stores.gate,
			)
			require.NoError(t, err)
			stores.callbacks = callbacks
			releases, err := OpenIdentityBoundReleaseStore(
				ReleaseStoreConfig{DBPath: stores.releasePath}, stores.storage, stores.gate,
			)
			require.NoError(t, err)
			stores.releases = releases
			reopened, err := NewOperationSettlement(callbacks, releases)
			require.NoError(t, err)
			inventoryChecked := false
			bindTestOperationMutation(t, reopened, func(subject OperationPhysicalSubject) (OperationPhysicalEvidence, error) {
				inventoryChecked = true
				if !test.success {
					return NewOperationExactAbsent(subject)
				}
				release, _ := subject.ExpectedRelease()
				ids, services := testPhysicalProjection(release)
				return NewOperationTargetReady(subject, ids, services)
			})
			claims, err := reopened.ListOperationIntents()
			require.NoError(t, err)
			require.Len(t, claims, 1)
			recovered := claims[0]
			assert.False(t, recovered.entry.EffectNotStarted)

			coordinator := newTestRecoveryCoordinator(t, reopened, nil, nil)
			var outcome OperationExecutionOutcome
			acquired, err := coordinator.WithLease(
				context.Background(), recovered.LeaseUUID(),
				func(scope LeaseRecoveryScope) error {
					outcome, err = reopened.RecoverOperationExecution(
						context.Background(), scope, recovered,
					)
					return err
				},
			)
			require.NoError(t, err)
			require.True(t, acquired)
			if test.success {
				success, ok := outcome.(OperationExecutionSuccess)
				require.True(t, ok)
				committed, err := reopened.CommitOperationSuccess(success)
				require.NoError(t, err)
				require.True(t, committed.Valid())
			} else {
				failure, ok := outcome.(OperationExecutionFailure)
				require.True(t, ok)
				uncommitted, err := reopened.CommitOperationFailure(failure)
				require.NoError(t, err)
				require.True(t, uncommitted.Valid())
			}
			assert.True(t, inventoryChecked)
		})
	}
}

func TestOperationExecutionRejectsOutcomeFromAnotherStartedGeneration(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	bindTestOperationMutation(t, stores.settlement, nil)

	claimA := beginHandoffOperation(
		t, stores.settlement, testOperationIntentSpec(t, "execution-lineage-a"),
	)
	claimB := beginHandoffOperation(
		t, stores.settlement, testOperationIntentSpec(t, "execution-lineage-b"),
	)
	candidateA, err := stores.settlement.PrepareOperationRelease(claimA)
	require.NoError(t, err)
	candidateB, err := stores.settlement.PrepareOperationRelease(claimB)
	require.NoError(t, err)
	executionA, err := stores.settlement.StartOperationExecution(candidateA)
	require.NoError(t, err)
	executionB, err := stores.settlement.StartOperationExecution(candidateB)
	require.NoError(t, err)

	acceptedA := stores.settlement.ExecuteOperation(context.Background(), executionA)
	require.IsType(t, OperationExecutionSuccess{}, acceptedA)
	forgedB := executionB
	forgedB.started = executionA.started
	rejectedB := stores.settlement.ExecuteOperation(context.Background(), forgedB)
	require.IsType(t, OperationExecutionAmbiguous{}, rejectedB)
}

func TestOperationSuccessWithoutCommittedReleaseIsUnrepresentableAcrossRestart(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	spec := testOperationIntentSpec(t, "handoff-succeeded")
	beginHandoffOperation(t, stores.settlement, spec)
	_, err := stores.settlement.resolveOperationSuccess(OperationReleaseCommitted{})
	require.Error(t, err)

	require.NoError(t, stores.callbacks.Close())
	stores.callbacks = nil
	reopened, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	stores.callbacks = reopened
	reopenedSettlement, err := NewOperationSettlement(reopened, stores.releases)
	require.NoError(t, err)
	probe, err := reopenedSettlement.NewOperationIntentProbe(spec.LeaseUUID, spec.CallbackURL)
	require.NoError(t, err)
	state, err := reopenedSettlement.LookupOperationRecovery(probe)
	require.NoError(t, err)
	recovered, ok := state.(OperationIntentClaim)
	require.True(t, ok)
	candidate, err := reopenedSettlement.PrepareOperationRelease(recovered)
	require.NoError(t, err)
	committed := commitHandoffOperation(t, reopenedSettlement, candidate)
	_, err = reopenedSettlement.resolveOperationSuccess(committed)
	require.NoError(t, err)
	active, err := stores.releases.LatestActive(spec.LeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, recovered.OperationID(), active.OperationID)
}

func TestOperationFailureProofIsInvalidatedByReleaseCommit(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	spec := testOperationIntentSpec(t, "failure-proof-stale")
	claim := beginHandoffOperation(t, stores.settlement, spec)
	uncommitted := commitHandoffRefusal(t, stores.settlement, claim)
	candidate, err := stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	commitHandoffOperation(t, stores.settlement, candidate)

	_, err = stores.settlement.resolveOperationFailure(uncommitted, "late failure")
	require.ErrorIs(t, err, ErrOperationIntentConflict)
	states, err := stores.settlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, states, 1)
	assert.NotEqual(t, OperationIntentClaim{}, states[0],
		"stale failure authority must leave the exact intent pending")
}

func TestOperationSuccessProofIsInvalidatedByReleaseDeletion(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	spec := testOperationIntentSpec(t, "success-proof-delete")
	claim := beginHandoffOperation(t, stores.settlement, spec)
	candidate, err := stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	committed := commitHandoffOperation(t, stores.settlement, candidate)
	require.NoError(t, stores.releases.delete(spec.LeaseUUID))

	_, err = stores.settlement.resolveOperationSuccess(committed)
	require.Error(t, err)
}

func TestOperationSuccessProofIsInvalidatedBySupersession(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	spec := testOperationIntentSpec(t, "success-proof-supersede")
	claim := beginHandoffOperation(t, stores.settlement, spec)
	candidate, err := stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	committed := commitHandoffOperation(t, stores.settlement, candidate)
	require.NoError(t, stores.releases.updateLatestStatus(
		spec.LeaseUUID, "superseded", backend.ReasonUnknown, "",
	))

	_, err = stores.settlement.resolveOperationSuccess(committed)
	require.ErrorContains(t, err, "no longer the active generation")
}

func TestOperationSuccessProofIsBoundToExactOpenStoreInstances(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	spec := testOperationIntentSpec(t, "success-proof-reopen")
	claim := beginHandoffOperation(t, stores.settlement, spec)
	candidate, err := stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	committed := commitHandoffOperation(t, stores.settlement, candidate)

	require.NoError(t, stores.releases.Close())
	stores.releases = nil
	reopened, err := OpenIdentityBoundReleaseStore(
		ReleaseStoreConfig{DBPath: stores.releasePath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	stores.releases = reopened
	reopenedSettlement, err := NewOperationSettlement(stores.callbacks, reopened)
	require.NoError(t, err)
	_, err = reopenedSettlement.resolveOperationSuccess(committed)
	require.ErrorContains(t, err, "another journal pair")
}

func TestOperationSuccessProofReattestsReleasePath(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	spec := testOperationIntentSpec(t, "success-proof-path")
	claim := beginHandoffOperation(t, stores.settlement, spec)
	candidate, err := stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	committed := commitHandoffOperation(t, stores.settlement, candidate)

	require.NoError(t, os.Rename(stores.releasePath, stores.releasePath+".withdrawn"))
	_, err = stores.settlement.resolveOperationSuccess(committed)
	require.Error(t, err, "path withdrawal must invalidate an otherwise immutable proof")
}

func restoreHandoffFixture(
	t *testing.T,
	stores operationHandoffStores,
	name string,
) (OperationIntentSpec, OperationIntentClaim, RetentionEntry) {
	t.Helper()
	spec := testOperationIntentSpec(t, name)
	spec.Kind = OperationIntentRestore
	spec.SourceLeaseUUID = testLeaseUUID(name + "-source")
	spec.SourceGeneration = 1
	stack, err := manifest.ParsePayload(spec.Manifest)
	require.NoError(t, err)
	source := RetentionEntry{
		OriginalLeaseUUID:   spec.SourceLeaseUUID,
		Tenant:              spec.Tenant,
		ProviderUUID:        spec.ProviderUUID,
		Items:               slices.Clone(spec.Items),
		ResourceProfiles:    CloneSKUResourceSnapshot(spec.ResourceProfiles),
		StackManifest:       stack,
		CallbackURL:         "https://fred.example/callbacks/provision",
		RetainedVolumeNames: []string{"retained-app-0"},
		Status:              RetentionStatusActive,
		CreatedAt:           time.Now(),
	}
	require.NoError(t, stores.retentions.putForTest(source))
	claim := beginHandoffOperation(t, stores.settlement, spec)
	return spec, claim, source
}

func TestRestoreClaimCandidateIsDetachedStoreBoundAndGenerationExact(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	other := openOperationHandoffStores(t, "docker-b")
	spec, claim, source := restoreHandoffFixture(t, stores, "restore-handoff")

	_, err := other.restore.PrepareRestoreClaim(claim)
	require.Error(t, err)
	_, err = stores.restore.ClaimForRestore(RestoreClaimCandidate{}, 0)
	require.Error(t, err)
	candidate, err := stores.restore.PrepareRestoreClaim(claim)
	require.NoError(t, err)
	_, err = other.restore.ClaimForRestore(candidate, 0)
	require.Error(t, err)

	// Mutations to the original claim cannot alter the already-minted transition.
	durable := claim
	durable.entry.SourceLeaseUUID = testLeaseUUID("mutated-source")
	durable.entry.EffectiveItems[0].SKU = "mutated"
	claimed, err := stores.restore.ClaimForRestore(candidate, 0)
	require.NoError(t, err)
	claimedEntry := claimed.Entry()
	assert.Equal(t, source.OriginalLeaseUUID, claimedEntry.OriginalLeaseUUID)
	assert.Equal(t, spec.LeaseUUID, claimedEntry.NewLeaseUUID)
	assert.Equal(t, candidate.authority.OperationID(), claimedEntry.DestinationOperationID)
	assert.Equal(t, "small", claimedEntry.DestinationItems[0].SKU)
	assert.Equal(t, spec.SourceGeneration, claimedEntry.Generation)
}

func TestRestoreClaimCandidateRejectsStaleGenerationWithoutMutation(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	spec, claim, source := restoreHandoffFixture(t, stores, "restore-stale")
	candidate, err := stores.restore.PrepareRestoreClaim(claim)
	require.NoError(t, err)

	current, err := stores.retentions.Get(source.OriginalLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, current)
	current.Generation = spec.SourceGeneration
	require.NoError(t, stores.retentions.putForTest(*current))
	before, err := stores.retentions.getRaw(source.OriginalLeaseUUID)
	require.NoError(t, err)
	_, err = stores.restore.ClaimForRestore(candidate, 0)
	require.ErrorIs(t, err, ErrNotRestorable)
	after, readErr := stores.retentions.getRaw(source.OriginalLeaseUUID)
	require.NoError(t, readErr)
	assert.Equal(t, before, after)
}

func TestRestoreClaimCandidateMustBeRemintedAfterRestart(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	_, claim, _ := restoreHandoffFixture(t, stores, "restore-restart")
	candidate, err := stores.restore.PrepareRestoreClaim(claim)
	require.NoError(t, err)
	require.NoError(t, stores.retentions.Close())
	stores.retentions = nil
	reopened, err := OpenIdentityBoundRetentionStore(
		RetentionStoreConfig{DBPath: stores.retentionPath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	stores.retentions = reopened
	reopenedRestore, err := NewRestoreSettlement(stores.settlement, reopened)
	require.NoError(t, err)
	_, err = reopenedRestore.ClaimForRestore(candidate, 0)
	require.ErrorContains(t, err, "another journal set")
	reminted, err := reopenedRestore.PrepareRestoreClaim(claim)
	require.NoError(t, err)
	_, err = reopenedRestore.ClaimForRestore(reminted, 0)
	require.NoError(t, err)
}

func TestRestoreSettlementRejectsClosedRetentionStoreBeforeUse(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	_, claim, _ := restoreHandoffFixture(t, stores, "restore-closed-retention")
	require.NoError(t, stores.retentions.Close())

	_, err := NewRestoreSettlement(stores.settlement, stores.retentions)
	require.ErrorContains(t, err, "exact identity-bound operation and retention journals")
	_, err = stores.restore.PrepareRestoreClaim(claim)
	require.ErrorContains(t, err, "restore settlement is invalid")
}

func TestRestoreClaimCandidateRejectsProvisionAuthority(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	claim := beginHandoffOperation(
		t, stores.settlement, testOperationIntentSpec(t, "not-restore"),
	)
	_, err := stores.restore.PrepareRestoreClaim(claim)
	require.ErrorContains(t, err, "requires a restore operation")
}

func TestRestoreClaimCandidateIsInvalidAfterOperationFailure(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	_, claim, source := restoreHandoffFixture(t, stores, "restore-stale-failure")
	candidate, err := stores.restore.PrepareRestoreClaim(claim)
	require.NoError(t, err)
	uncommitted := commitHandoffRefusal(t, stores.settlement, claim)
	_, err = stores.settlement.resolveOperationFailure(uncommitted, "definitive refusal")
	require.NoError(t, err)

	_, err = stores.restore.ClaimForRestore(candidate, 0)
	require.ErrorIs(t, err, ErrOperationIntentConflict)
	current, err := stores.retentions.Get(source.OriginalLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, current)
	assert.Equal(t, RetentionStatusActive, current.Status)
}

func TestRestoreClaimAndFailureSettlementLinearizeOnDestinationGate(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	_, claim, source := restoreHandoffFixture(t, stores, "restore-claim-failure-race")
	candidate, err := stores.restore.PrepareRestoreClaim(claim)
	require.NoError(t, err)
	uncommitted := commitHandoffRefusal(t, stores.settlement, claim)

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	var claimErr, failureErr error
	go func() {
		defer wg.Done()
		<-start
		_, claimErr = stores.restore.ClaimForRestore(candidate, 0)
	}()
	go func() {
		defer wg.Done()
		<-start
		_, failureErr = stores.settlement.resolveOperationFailure(
			uncommitted, "concurrent definitive refusal",
		)
	}()
	close(start)
	wg.Wait()

	require.NoError(t, failureErr)
	current, err := stores.retentions.Get(source.OriginalLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, current)
	if claimErr == nil {
		assert.Equal(t, RetentionStatusRestoring, current.Status,
			"a claim linearized before failure remains rollback authority")
		return
	}
	require.True(t,
		errors.Is(claimErr, ErrOperationIntentConflict) ||
			errors.Is(claimErr, ErrOperationIntentMissing),
		"a claim linearized after failure must be rejected by the pending-head check: %v", claimErr,
	)
	assert.Equal(t, RetentionStatusActive, current.Status)
}
