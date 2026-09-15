package shared

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func activeRetentionCandidateForTest(
	t *testing.T,
	store *RetentionStore,
	leaseUUID string,
) ActiveRetentionCandidate {
	t.Helper()
	candidates, err := store.ListActiveCandidates()
	require.NoError(t, err)
	for _, candidate := range candidates {
		if candidate.Entry().OriginalLeaseUUID == leaseUUID {
			return candidate
		}
	}
	t.Fatalf("active retention candidate %q not found", leaseUUID)
	return ActiveRetentionCandidate{}
}

func TestRetentionProofsRejectCrossStoreAndReopen(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	other := openOperationHandoffStores(t, "docker-b")
	_, _, source := restoreHandoffFixture(t, stores, "retention-cross-store")

	active := activeRetentionCandidateForTest(t, stores.retentions, source.OriginalLeaseUUID)
	_, _, err := other.retentions.BeginReaping(active)
	require.ErrorContains(t, err, "another journal lineage")
	reaping, ok, err := stores.retentions.BeginReaping(active)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, reaping.Valid())

	_, err = other.retentions.DeleteReaped(reaping)
	require.ErrorContains(t, err, "another journal lineage")

	require.NoError(t, stores.retentions.Close())
	stores.retentions = nil
	reopened, err := OpenIdentityBoundRetentionStore(
		RetentionStoreConfig{DBPath: stores.retentionPath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	_, err = reopened.DeleteReaped(reaping)
	require.ErrorContains(t, err, "another journal lineage")
	_, _, err = reopened.BeginReaping(active)
	require.ErrorContains(t, err, "another journal lineage")
}

func TestCopiedRestoringProofCannotConsumeNewGeneration(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	_, claim, source := restoreHandoffFixture(t, stores, "retention-aba")
	candidate, err := stores.restore.PrepareRestoreClaim(claim)
	require.NoError(t, err)
	first, err := stores.restore.ClaimForRestore(candidate, 0)
	require.NoError(t, err)

	_, err = stores.retentions.RollbackRestoring(
		first, first.Entry().ResourceProfiles,
	)
	require.NoError(t, err)

	// Recreate a later Restoring generation using a distinct operation. The
	// copied first-generation proof must not delete it even though the source
	// and destination identities are unchanged.
	secondSpec := testOperationIntentSpec(t, "retention-aba-second")
	secondSpec.Kind = OperationIntentRestore
	secondSpec.SourceLeaseUUID = source.OriginalLeaseUUID
	active, err := stores.retentions.Get(source.OriginalLeaseUUID)
	require.NoError(t, err)
	secondSpec.SourceGeneration = active.Generation + 1
	secondClaim := beginHandoffOperation(t, stores.settlement, secondSpec)
	secondCandidate, err := stores.restore.PrepareRestoreClaim(secondClaim)
	require.NoError(t, err)
	second, err := stores.restore.ClaimForRestore(secondCandidate, 0)
	require.NoError(t, err)
	require.NotEqual(t, first.Entry().Generation, second.Entry().Generation)

	_, err = stores.retentions.DeleteRestoring(first)
	require.ErrorContains(t, err, "generation changed")
	current, err := stores.retentions.Get(source.OriginalLeaseUUID)
	require.NoError(t, err)
	require.Equal(t, RetentionStatusRestoring, current.Status)
	require.Equal(t, second.Entry().Generation, current.Generation)
}

func TestCopiedActiveCandidateCannotConsumeABAGeneration(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	_, claim, source := restoreHandoffFixture(t, stores, "retention-active-aba")
	stale := activeRetentionCandidateForTest(t, stores.retentions, source.OriginalLeaseUUID)

	restoreCandidate, err := stores.restore.PrepareRestoreClaim(claim)
	require.NoError(t, err)
	restoring, err := stores.restore.ClaimForRestore(restoreCandidate, 0)
	require.NoError(t, err)
	_, err = stores.retentions.RollbackRestoring(
		restoring, restoring.Entry().ResourceProfiles,
	)
	require.NoError(t, err)

	_, reaped, err := stores.retentions.BeginReaping(stale)
	require.NoError(t, err)
	require.False(t, reaped, "copied pre-ABA candidate must not consume a later Active row")
	current, err := stores.retentions.Get(source.OriginalLeaseUUID)
	require.NoError(t, err)
	require.Equal(t, RetentionStatusActive, current.Status)
	require.Greater(t, current.Generation, stale.Entry().Generation)
}

func TestTenantRetentionSelectionSeparatesInspectionFromActiveAuthority(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	_, claim, restoringSource := restoreHandoffFixture(t, stores, "retention-tenant-restoring")
	_, _, activeSource := restoreHandoffFixture(t, stores, "retention-tenant-active")
	restoreCandidate, err := stores.restore.PrepareRestoreClaim(claim)
	require.NoError(t, err)
	_, err = stores.restore.ClaimForRestore(restoreCandidate, 0)
	require.NoError(t, err)

	entries, candidates, err := stores.retentions.ListTenantRetentionCandidates(
		activeSource.Tenant,
	)
	require.NoError(t, err)
	require.Len(t, entries, 2, "partition inspection retains Active and Restoring rows")
	require.Len(t, candidates, 1, "only Active rows carry destructive authority")
	require.Equal(t, activeSource.OriginalLeaseUUID, candidates[0].Entry().OriginalLeaseUUID)
	require.NotEqual(t, restoringSource.OriginalLeaseUUID, candidates[0].Entry().OriginalLeaseUUID)
}

func TestRetentionProofsRejectWrongStateAndRouteSplice(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	_, claim, source := restoreHandoffFixture(t, stores, "retention-route-splice")
	activeCandidate := activeRetentionCandidateForTest(
		t, stores.retentions, source.OriginalLeaseUUID,
	)

	reaping, err := stores.retentions.ListReapingProofs()
	require.NoError(t, err)
	require.Empty(t, reaping, "an Active DTO cannot be upgraded to Reaping authority")
	candidate, err := stores.restore.PrepareRestoreClaim(claim)
	require.NoError(t, err)
	restoring, err := stores.restore.ClaimForRestore(candidate, 0)
	require.NoError(t, err)

	spliced := restoring.Entry()
	spliced.DestinationCallbackURL = "https://attacker.invalid/callback"
	_, err = stores.retentions.ProveRestoringSnapshot(spliced)
	require.ErrorContains(t, err, "stale")

	_, ok, err := stores.retentions.BeginReaping(activeCandidate)
	require.NoError(t, err)
	require.False(t, ok, "Restoring is structurally outside Active -> Reaping")
}

func TestRestoreClaimAndReapingAdmissionAreMutuallyExclusive(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	_, claim, source := restoreHandoffFixture(t, stores, "retention-race")
	restoreCandidate, err := stores.restore.PrepareRestoreClaim(claim)
	require.NoError(t, err)
	activeCandidate := activeRetentionCandidateForTest(
		t, stores.retentions, source.OriginalLeaseUUID,
	)

	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	var restoreProof RestoringRetentionProof
	var restoreErr error
	var reapingProof ReapingRetentionProof
	var reapingOK bool
	var reapingDeleted bool
	var reapingErr error
	go func() {
		defer wait.Done()
		<-start
		restoreProof, restoreErr = stores.restore.ClaimForRestore(restoreCandidate, 0)
	}()
	go func() {
		defer wait.Done()
		<-start
		reapingProof, reapingOK, reapingErr = stores.retentions.BeginReaping(activeCandidate)
		if reapingErr == nil && reapingOK {
			reapingDeleted, reapingErr = stores.retentions.DeleteReaped(reapingProof)
		}
	}()
	close(start)
	wait.Wait()

	require.NoError(t, reapingErr)
	switch {
	case restoreErr == nil:
		require.True(t, restoreProof.Valid())
		require.False(t, reapingOK)
	case reapingOK:
		require.True(t, reapingProof.Valid())
		require.True(t, reapingDeleted)
		require.True(t,
			errors.Is(restoreErr, ErrNotRestorable) || errors.Is(restoreErr, ErrNoRetention),
			"restore loser must observe reaping or its exact deletion, got %v", restoreErr,
		)
	default:
		t.Fatalf("neither exact retention transition won: restore=%v reaping=%v", restoreErr, reapingErr)
	}
	current, err := stores.retentions.Get(source.OriginalLeaseUUID)
	require.NoError(t, err)
	if restoreErr == nil {
		require.Equal(t, RetentionStatusRestoring, current.Status)
	} else {
		require.Nil(t, current)
	}
}
