package shared

import (
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

func newCloseSettlementForTest(
	t *testing.T,
	stores operationHandoffStores,
) *CloseSettlement {
	t.Helper()
	settlement, err := NewCloseSettlement(stores.callbacks, stores.releases, stores.retentions)
	require.NoError(t, err)
	return settlement
}

func seedCloseSettlementRelease(
	t *testing.T,
	stores operationHandoffStores,
	name string,
) OperationIntentSpec {
	t.Helper()
	spec := testOperationIntentSpec(t, name)
	operationID, err := parseOperationCallbackID(spec.CallbackURL)
	require.NoError(t, err)
	authority, err := NewReleaseRuntimeAuthority(
		operationID,
		spec.Tenant,
		spec.ProviderUUID,
		spec.CallbackURL,
		spec.LifecycleCallbackURL,
	)
	require.NoError(t, err)
	require.NoError(t, stores.releases.appendActive(spec.LeaseUUID, Release{
		Manifest:         spec.Manifest,
		Image:            "stack",
		OperationID:      operationID,
		Items:            spec.Items,
		ResourceProfiles: spec.ResourceProfiles,
		RuntimeAuthority: &authority,
	}))
	return spec
}

func admitSettlementClose(
	t *testing.T,
	settlement *CloseSettlement,
	leaseUUID string,
	retain bool,
) CloseIntentClaim {
	t.Helper()
	request, err := settlement.NewCloseRequest(leaseUUID, retain)
	require.NoError(t, err)
	admission, err := settlement.BeginClose(request)
	require.NoError(t, err)
	return admission.Claim()
}

func completeDestroyedForTest(
	settlement *CloseSettlement,
	claim CloseIntentClaim,
) (CallbackEntry, error) {
	return settlement.completeTerminal(claim, closeCompletionDestroyed, ActiveRetentionProof{})
}

func completeRetainedForTest(
	settlement *CloseSettlement,
	claim CloseIntentClaim,
	proof ActiveRetentionProof,
) (CallbackEntry, error) {
	return settlement.completeTerminal(claim, closeCompletionRetained, proof)
}

func advanceCloseGenerationForTest(
	settlement *CloseSettlement,
	claim CloseIntentClaim,
) (CloseIntentClaim, error) {
	unlock := settlement.lockLease(claim.LeaseUUID())
	defer unlock()
	refreshed, err := settlement.callbacks.advanceCloseExecutionGenerationLocked(claim)
	if err == nil {
		refreshed.settlement = settlement
	}
	return refreshed, err
}

func settleActivePredecessorThenFailedSuccessorForClose(
	t *testing.T,
	stores operationHandoffStores,
	name string,
) (OperationIntentSpec, ReleaseClaim, OperationIntentClaim) {
	t.Helper()
	predecessorSpec := testOperationIntentSpec(t, name+"-predecessor")
	predecessor := beginHandoffOperation(t, stores.settlement, predecessorSpec)
	candidate, err := stores.settlement.PrepareOperationRelease(predecessor)
	require.NoError(t, err)
	committed := commitHandoffOperation(t, stores.settlement, candidate)
	completion, err := stores.settlement.resolveOperationSuccess(committed)
	require.NoError(t, err)
	require.NoError(t, stores.callbacks.removeEntry(completion))
	_, active, err := stores.releases.claimLatestActive(predecessorSpec.LeaseUUID)
	require.NoError(t, err)

	successorSpec := predecessorSpec
	successorSpec.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	successorSpec.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(
		successorSpec.CallbackURL, "",
	)
	require.NoError(t, err)
	successorSpec.Manifest = []byte(`{"services":{"app":{"image":"example.invalid/app:2"}}}`)
	successor := beginHandoffOperation(t, stores.settlement, successorSpec)
	uncommitted := commitHandoffRefusal(t, stores.settlement, successor)
	failure, err := stores.settlement.resolveOperationFailure(
		uncommitted, "replacement definitively refused",
	)
	require.NoError(t, err)
	require.NoError(t, stores.callbacks.removeEntry(failure))

	var failed OperationIntentClaim
	require.NoError(t, stores.callbacks.view(func(tx *bolt.Tx) error {
		head, present, readErr := getLeaseMutationHeadTx(tx, predecessorSpec.LeaseUUID)
		if readErr != nil {
			return readErr
		}
		operation, ok := head.(operationLeaseMutationHead)
		if !present || !ok {
			return errors.New("failed successor operation head is missing")
		}
		failed = operation.claim
		failed.settlement = stores.settlement
		return nil
	}))
	require.Equal(t, operationIntentFailed, failed.entry.State)
	require.Equal(t, operationFailurePredecessorActive, failed.entry.FailurePredecessor.Kind)
	require.Equal(t, active.Version(), failed.entry.FailurePredecessor.ReleaseVersion)
	require.Equal(t,
		encodeOperationFailurePredecessorDigest(active.Digest()),
		failed.entry.FailurePredecessor.ReleaseDigest,
	)
	return predecessorSpec, active, failed
}

func TestCloseSettlementConsumesFailedSuccessorOverExactActivePredecessor(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	spec, active, failed := settleActivePredecessorThenFailedSuccessorForClose(
		t, stores, "close-failed-successor",
	)

	request, err := settlement.NewCloseRequest(spec.LeaseUUID, false)
	require.NoError(t, err)
	admission, err := settlement.BeginClose(request)
	require.NoError(t, err)
	require.Equal(t, CloseIntentAdmissionCreated, admission.Disposition())
	assert.False(t, admission.OperationPreempted())
	assert.Equal(t, active.Version(), admission.Claim().ActiveReleaseVersion())
	assert.Equal(t, active.Digest(), admission.Claim().ActiveReleaseDigest())
	assert.NotEqual(t, failed.OperationID(), admission.Claim().ActiveReleaseOperationID())

	receipts, err := stores.callbacks.ListFailedOperationReceipts()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	assert.Equal(t, failed.OperationID(), receipts[0].OperationID())
}

func TestCloseSettlementConsumesFailedSuccessorOverAdoptedV013Predecessor(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	leaseUUID := testLeaseUUID("close-failed-successor-v013")
	require.NoError(t, stores.releases.appendActive(
		leaseUUID, legacyMaintenanceRelease(t, "https://legacy.example"),
	))
	_, active, err := stores.releases.claimLatestActive(leaseUUID)
	require.NoError(t, err)

	successorSpec := testOperationIntentSpec(t, "close-failed-successor-v013-operation")
	successorSpec.LeaseUUID = leaseUUID
	successor := beginHandoffOperation(t, stores.settlement, successorSpec)
	uncommitted := commitHandoffRefusal(t, stores.settlement, successor)
	failure, err := stores.settlement.resolveOperationFailure(
		uncommitted, "replacement definitively refused",
	)
	require.NoError(t, err)
	require.NoError(t, stores.callbacks.removeEntry(failure))

	request, err := settlement.NewCloseRequest(leaseUUID, false)
	require.NoError(t, err)
	admission, err := settlement.BeginClose(request)
	require.NoError(t, err)
	assert.Equal(t, active.Version(), admission.Claim().ActiveReleaseVersion())
	assert.Equal(t, active.Digest(), admission.Claim().ActiveReleaseDigest())
	assert.True(t, admission.Claim().ActiveReleaseOperationID().IsZero())
}

func TestCloseSettlementRejectsFailedSuccessorWhenPredecessorChanged(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	spec, _, _ := settleActivePredecessorThenFailedSuccessorForClose(
		t, stores, "close-failed-successor-changed",
	)
	require.NoError(t, stores.releases.updateLatestStatus(
		spec.LeaseUUID, "superseded", backend.ReasonUnknown, "",
	))

	request, err := settlement.NewCloseRequest(spec.LeaseUUID, false)
	require.NoError(t, err)
	_, err = settlement.BeginClose(request)
	require.ErrorContains(t, err, "exact active predecessor")
	_, found, getErr := settlement.GetCloseIntent(spec.LeaseUUID)
	require.NoError(t, getErr)
	assert.False(t, found)
}

func TestCloseSettlementRejectsReleaseAppearingAfterFailedOperationWithoutPredecessor(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	successorSpec := testOperationIntentSpec(t, "close-failed-without-predecessor")
	successor := beginHandoffOperation(t, stores.settlement, successorSpec)
	uncommitted := commitHandoffRefusal(t, stores.settlement, successor)
	failure, err := stores.settlement.resolveOperationFailure(uncommitted, "initial refusal")
	require.NoError(t, err)
	require.NoError(t, stores.callbacks.removeEntry(failure))
	require.NoError(t, stores.releases.appendActive(
		successorSpec.LeaseUUID, validRuntimeAuthorityRelease(),
	))

	request, err := settlement.NewCloseRequest(successorSpec.LeaseUUID, false)
	require.NoError(t, err)
	_, err = settlement.BeginClose(request)
	require.ErrorContains(t, err, "exact active predecessor")

	cleanup, err := settlement.NewCleanupCloseRequest(successorSpec.LeaseUUID)
	require.NoError(t, err)
	_, err = settlement.BeginCleanupClose(cleanup)
	require.ErrorContains(t, err, "exact active predecessor")
}

func TestCloseSettlementCleanupConsumesFailedOperationWithExactReleaseAbsence(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	spec := testOperationIntentSpec(t, "close-initial-failure-cleanup")
	operation := beginHandoffOperation(t, stores.settlement, spec)
	uncommitted := commitHandoffRefusal(t, stores.settlement, operation)
	failure, err := stores.settlement.resolveOperationFailure(
		uncommitted, "initial operation definitively refused",
	)
	require.NoError(t, err)
	require.NoError(t, stores.callbacks.removeEntry(failure))

	projected, err := settlement.NewCloseRequest(spec.LeaseUUID, false)
	require.NoError(t, err)
	_, err = settlement.BeginClose(projected)
	require.ErrorIs(t, err, ErrCloseAuthorityMissing)

	request, err := settlement.NewCleanupCloseRequest(spec.LeaseUUID)
	require.NoError(t, err)
	admission, err := settlement.BeginCleanupClose(request)
	require.NoError(t, err)
	require.Equal(t, CloseIntentAdmissionCreated, admission.Disposition())
	assert.False(t, admission.OperationPreempted())
	claim := admission.Claim()
	assert.True(t, claim.CleanupOnly())
	assert.False(t, claim.RetainOnClose())
	assert.Empty(t, claim.Tenant())
	assert.Empty(t, claim.ProviderUUID())
	assert.Empty(t, claim.CallbackURL())
	assert.Empty(t, claim.LifecycleCallbackURL())
	assert.Zero(t, claim.ActiveReleaseVersion())
	assert.Zero(t, claim.ActiveReleaseDigest())
	assert.True(t, claim.ActiveReleaseOperationID().IsZero())
	assert.Equal(t, operation.EffectiveItems(), claim.Items())
	assert.Equal(t, operation.ResourceProfiles(), claim.ResourceProfiles())
	assert.Equal(t, operation.Manifest(), claim.Manifest())

	receipts, err := stores.callbacks.ListFailedOperationReceipts()
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	assert.Equal(t, operation.OperationID(), receipts[0].OperationID())

	callback, err := completeDestroyedForTest(settlement, claim)
	require.NoError(t, err)
	assert.Empty(t, callback.DeliveryID, "cleanup-only close must stay callbackless")
	closed, err := stores.callbacks.LookupClosedLeaseReceipts([]string{spec.LeaseUUID})
	require.NoError(t, err)
	require.Len(t, closed, 1)
	assert.Equal(t, ClosedLeaseAuthorityOrphan, closed[0].AuthorityKind())
	assert.True(t, closed[0].CleanupOnly())
}

func TestCloseSettlementRemintsFailedWithoutReleaseCleanupAuthorityAfterReopen(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	spec := testOperationIntentSpec(t, "close-initial-failure-reopen")
	operation := beginHandoffOperation(t, stores.settlement, spec)
	uncommitted := commitHandoffRefusal(t, stores.settlement, operation)
	_, err := stores.settlement.resolveOperationFailure(
		uncommitted, "initial operation definitively refused",
	)
	require.NoError(t, err)

	require.NoError(t, stores.callbacks.Close())
	require.NoError(t, stores.releases.Close())
	require.NoError(t, stores.retentions.Close())
	stores.callbacks = nil
	stores.releases = nil
	stores.retentions = nil

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
	retentions, err := OpenIdentityBoundRetentionStore(
		RetentionStoreConfig{DBPath: stores.retentionPath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	stores.retentions = retentions
	settlement := newCloseSettlementForTest(t, stores)

	request, err := settlement.NewCleanupCloseRequest(spec.LeaseUUID)
	require.NoError(t, err)
	admission, err := settlement.BeginCleanupClose(request)
	require.NoError(t, err)
	assert.True(t, admission.Claim().CleanupOnly())
	assert.Zero(t, admission.Claim().ActiveReleaseVersion())
}

func TestCloseSettlementBindsRequestsClaimsAndRetentionProofsToExactJournalSet(t *testing.T) {
	storesA := openOperationHandoffStores(t, "docker-a")
	storesB := openOperationHandoffStores(t, "docker-b")
	closeA := newCloseSettlementForTest(t, storesA)
	closeB := newCloseSettlementForTest(t, storesB)
	spec := seedCloseSettlementRelease(t, storesA, "close-pair")

	request, err := closeA.NewCloseRequest(spec.LeaseUUID, true)
	require.NoError(t, err)
	_, err = closeB.BeginClose(request)
	require.ErrorContains(t, err, "another settlement")
	admission, err := closeA.BeginClose(request)
	require.NoError(t, err)
	claim := admission.Claim()
	_, err = completeDestroyedForTest(closeB, claim)
	require.ErrorContains(t, err, "another journal set")
	_, err = closeB.RecordRetention(
		claim, "", []string{"fred-retained-" + spec.LeaseUUID + "-app-0"},
	)
	require.ErrorContains(t, err, "another journal set")

	ok, err := closeA.RecordRetention(
		claim, "", []string{"fred-retained-" + spec.LeaseUUID + "-app-0"},
	)
	require.NoError(t, err)
	require.True(t, ok)
	proof, err := closeA.ProveRetention(claim)
	require.NoError(t, err)
	require.True(t, proof.Valid())
	_, err = completeRetainedForTest(closeB, claim, proof)
	require.ErrorContains(t, err, "another journal set")
	_, err = completeRetainedForTest(closeA, claim, ActiveRetentionProof{})
	require.Error(t, err)
}

func TestNewCloseSettlementRequiresOneExactJournalLineage(t *testing.T) {
	storesA := openOperationHandoffStores(t, "docker-a")
	storesB := openOperationHandoffStores(t, "docker-b")

	_, err := NewCloseSettlement(storesA.callbacks, storesB.releases, storesA.retentions)
	require.ErrorContains(t, err, "exact identity-bound")
	_, err = NewCloseSettlement(storesA.callbacks, storesA.releases, storesB.retentions)
	require.ErrorContains(t, err, "exact identity-bound retention")
	_, err = NewCloseSettlement(nil, storesA.releases, storesA.retentions)
	require.Error(t, err)
}

func TestCloseSettlementRejectsClosedRetentionStoreBeforeUse(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	require.NoError(t, stores.retentions.Close())

	_, err := NewCloseSettlement(stores.callbacks, stores.releases, stores.retentions)
	require.ErrorContains(t, err, "exact identity-bound retention")
	_, err = settlement.NewCloseRequest(testLeaseUUID("closed-retention-close"), false)
	require.ErrorContains(t, err, "close settlement is invalid")
}

func TestCloseSettlementDerivesAuthorityAndCompletesRetained(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	close := newCloseSettlementForTest(t, stores)
	spec := seedCloseSettlementRelease(t, stores, "close-retained")
	claim := admitSettlementClose(t, close, spec.LeaseUUID, true)

	require.Equal(t, spec.Tenant, claim.Tenant())
	require.Equal(t, spec.ProviderUUID, claim.ProviderUUID())
	require.Equal(t, spec.Items, claim.Items())
	require.Equal(t, spec.ResourceProfiles, claim.ResourceProfiles())
	require.Equal(t, spec.Manifest, claim.Manifest())
	require.Equal(t, spec.CallbackURL, claim.CallbackURL())
	require.Positive(t, claim.ActiveReleaseVersion())

	ok, err := close.RecordRetention(
		claim, "partition-a", []string{"fred-retained-" + spec.LeaseUUID + "-app-0"},
	)
	require.NoError(t, err)
	require.True(t, ok)
	proof, err := close.ProveRetention(claim)
	require.NoError(t, err)
	entry, err := completeRetainedForTest(close, claim, proof)
	require.NoError(t, err)
	require.Equal(t, "deprovisioned", string(entry.Status))
	require.True(t, entry.Retained)

	releases, err := stores.releases.List(spec.LeaseUUID)
	require.NoError(t, err)
	require.Empty(t, releases)
	_, found, err := stores.callbacks.GetCloseIntent(spec.LeaseUUID)
	require.NoError(t, err)
	require.False(t, found)
	receipts, err := stores.callbacks.LookupClosedLeaseReceipts([]string{spec.LeaseUUID})
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	require.Equal(t, spec.Tenant, receipts[0].Tenant())
}

func TestCloseSettlementRejectsStaleRetentionAndReleaseAuthority(t *testing.T) {
	t.Run("retention generation", func(t *testing.T) {
		stores := openOperationHandoffStores(t, "docker-a")
		close := newCloseSettlementForTest(t, stores)
		spec := seedCloseSettlementRelease(t, stores, "stale-retention")
		claim := admitSettlementClose(t, close, spec.LeaseUUID, true)
		ok, err := close.RecordRetention(
			claim, "", []string{"fred-retained-" + spec.LeaseUUID + "-app-0"},
		)
		require.NoError(t, err)
		require.True(t, ok)
		proof, err := close.ProveRetention(claim)
		require.NoError(t, err)

		record, err := stores.retentions.Get(spec.LeaseUUID)
		require.NoError(t, err)
		require.NotNil(t, record)
		record.Partition = "changed"
		require.NoError(t, stores.retentions.putForTest(*record))
		_, err = completeRetainedForTest(close, claim, proof)
		require.ErrorContains(t, err, "changed before close settlement")
	})

	t.Run("release generation", func(t *testing.T) {
		stores := openOperationHandoffStores(t, "docker-a")
		close := newCloseSettlementForTest(t, stores)
		spec := seedCloseSettlementRelease(t, stores, "stale-release")
		claim := admitSettlementClose(t, close, spec.LeaseUUID, false)
		current, err := stores.releases.LatestActive(spec.LeaseUUID)
		require.NoError(t, err)
		require.NotNil(t, current)
		current.Manifest = []byte(`{"services":{"app":{"image":"example.invalid/app:2"}}}`)
		require.NoError(t, stores.releases.appendActive(spec.LeaseUUID, *current))
		_, err = completeDestroyedForTest(close, claim)
		require.ErrorContains(t, err, "changed after close admission")
	})
}

func TestCloseSettlementDestroyedRequiresRetentionAbsence(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	close := newCloseSettlementForTest(t, stores)
	spec := seedCloseSettlementRelease(t, stores, "destroy-retention")
	claim := admitSettlementClose(t, close, spec.LeaseUUID, true)
	ok, err := close.RecordRetention(
		claim, "", []string{"fred-retained-" + spec.LeaseUUID + "-app-0"},
	)
	require.NoError(t, err)
	require.True(t, ok)

	_, err = completeDestroyedForTest(close, claim)
	require.ErrorContains(t, err, "requires retention absence")
	releases, err := stores.releases.List(spec.LeaseUUID)
	require.NoError(t, err)
	require.NotEmpty(t, releases)
}

func TestCloseSettlementRecordRetentionRequiresRetainedProjectedClose(t *testing.T) {
	for _, test := range []struct {
		name    string
		cleanup bool
	}{
		{name: "non-retaining projected close"},
		{name: "cleanup-only close", cleanup: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			stores := openOperationHandoffStores(t, "docker-a")
			settlement := newCloseSettlementForTest(t, stores)
			spec := seedCloseSettlementRelease(t, stores, "record-forbidden-"+test.name)

			var claim CloseIntentClaim
			if test.cleanup {
				request, err := settlement.NewCleanupCloseRequest(spec.LeaseUUID)
				require.NoError(t, err)
				admission, err := settlement.BeginCleanupClose(request)
				require.NoError(t, err)
				claim = admission.Claim()
			} else {
				claim = admitSettlementClose(t, settlement, spec.LeaseUUID, false)
			}

			ok, err := settlement.RecordRetention(
				claim, "", []string{"fred-retained-" + spec.LeaseUUID + "-app-0"},
			)
			require.ErrorContains(t, err, "requires a retained projected close")
			assert.False(t, ok)
			record, readErr := stores.retentions.Get(spec.LeaseUUID)
			require.NoError(t, readErr)
			assert.Nil(t, record, "rejected phase must not mutate retention state")
		})
	}
}

func TestCloseSettlementCleanupAdmissionRequiresDurableAuthority(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	leaseUUID := testLeaseUUID("close-no-authority")

	request, err := settlement.NewCleanupCloseRequest(leaseUUID)
	require.NoError(t, err)
	_, err = settlement.BeginCleanupClose(request)
	require.ErrorIs(t, err, ErrCloseAuthorityMissing)
	_, found, err := settlement.GetCloseIntent(leaseUUID)
	require.NoError(t, err)
	assert.False(t, found)

	projected, err := settlement.NewCloseRequest(leaseUUID, false)
	require.NoError(t, err)
	_, err = settlement.BeginClose(projected)
	require.ErrorIs(t, err, ErrCloseAuthorityMissing)
}

func TestCloseSettlementRefreshedClaimInvalidatesStaleCopy(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	spec := seedCloseSettlementRelease(t, stores, "close-stale-claim")
	stale := admitSettlementClose(t, settlement, spec.LeaseUUID, false)
	current, err := advanceCloseGenerationForTest(settlement, stale)
	require.NoError(t, err)

	_, err = completeDestroyedForTest(settlement, stale)
	require.ErrorContains(t, err, "changed before precise mutation")
	_, err = completeDestroyedForTest(settlement, current)
	require.NoError(t, err)
}

func TestCloseSettlementStaleClaimCannotRecordRetention(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	spec := seedCloseSettlementRelease(t, stores, "close-stale-retention-write")
	stale := admitSettlementClose(t, settlement, spec.LeaseUUID, true)
	_, err := advanceCloseGenerationForTest(settlement, stale)
	require.NoError(t, err)

	ok, err := settlement.RecordRetention(
		stale, "", []string{"fred-retained-" + spec.LeaseUUID + "-app-0"},
	)
	require.ErrorContains(t, err, "changed before precise mutation")
	assert.False(t, ok)
	record, readErr := stores.retentions.Get(spec.LeaseUUID)
	require.NoError(t, readErr)
	assert.Nil(t, record)
}

func TestCloseSettlementCleanupAdmissionCanConsumeExactPendingOperation(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	spec := testOperationIntentSpec(t, "cleanup-pending-operation")
	beginHandoffOperation(t, stores.settlement, spec)

	request, err := settlement.NewCleanupCloseRequest(spec.LeaseUUID)
	require.NoError(t, err)
	admission, err := settlement.BeginCleanupClose(request)
	require.NoError(t, err)
	claim := admission.Claim()
	assert.True(t, claim.CleanupOnly())
	assert.Empty(t, claim.Tenant())
	assert.Empty(t, claim.ProviderUUID())
	assert.Empty(t, claim.CallbackURL())
	assert.Zero(t, claim.ActiveReleaseVersion())

	_, err = completeDestroyedForTest(settlement, claim)
	require.NoError(t, err)
	pending, err := stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, spec.CallbackURL, pending[0].CallbackURL)
	assert.Equal(t, "failed", string(pending[0].Status))
}

func TestCloseSettlementCopiedClaimSettlesAtMostOnce(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	settlement := newCloseSettlementForTest(t, stores)
	spec := seedCloseSettlementRelease(t, stores, "close-race")
	claim := admitSettlementClose(t, settlement, spec.LeaseUUID, false)

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func(copy CloseIntentClaim) {
			defer wg.Done()
			<-start
			_, err := completeDestroyedForTest(settlement, copy)
			errs <- err
		}(claim)
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

func TestCloseSettlementRejectsClaimAcrossCoordinatorReconstruction(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	first := newCloseSettlementForTest(t, stores)
	spec := seedCloseSettlementRelease(t, stores, "close-reconstruction")
	claim := admitSettlementClose(t, first, spec.LeaseUUID, false)
	second := newCloseSettlementForTest(t, stores)

	_, err := completeDestroyedForTest(second, claim)
	require.ErrorContains(t, err, "another journal set")
	current, found, err := second.GetCloseIntent(spec.LeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	_, err = completeDestroyedForTest(second, current)
	require.NoError(t, err)
}

func TestCloseSettlementRemintsRecoveryAuthorityAfterJournalReopen(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	first := newCloseSettlementForTest(t, stores)
	spec := seedCloseSettlementRelease(t, stores, "close-reopen")
	stale := admitSettlementClose(t, first, spec.LeaseUUID, false)

	require.NoError(t, stores.callbacks.Close())
	require.NoError(t, stores.releases.Close())
	require.NoError(t, stores.retentions.Close())
	stores.callbacks = nil
	stores.releases = nil
	stores.retentions = nil

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
	retentions, err := OpenIdentityBoundRetentionStore(
		RetentionStoreConfig{DBPath: stores.retentionPath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	stores.retentions = retentions
	second := newCloseSettlementForTest(t, stores)

	_, err = completeDestroyedForTest(second, stale)
	require.ErrorContains(t, err, "another journal set")
	recovered, found, err := second.GetCloseIntent(spec.LeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	_, err = completeDestroyedForTest(second, recovered)
	require.NoError(t, err)
}
