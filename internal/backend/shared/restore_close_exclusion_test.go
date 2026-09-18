package shared

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestInterruptedRestoreSourceProofIsExactAndReattested(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	closeSettlement := newCloseSettlementForTest(t, stores)
	source := seedCloseSettlementRelease(t, stores, "interrupted-source-proof")
	closeClaim := admitSettlementClose(t, closeSettlement, source.LeaseUUID, true)
	ok, err := closeSettlement.RecordRetention(closeClaim, "", []string{"retained-app-0"})
	require.NoError(t, err)
	require.True(t, ok)
	spec := testOperationIntentSpec(t, "interrupted-source-target")
	spec.Kind, spec.SourceLeaseUUID, spec.SourceGeneration = OperationIntentRestore, source.LeaseUUID, 1
	claim := beginHandoffOperation(t, stores.settlement, spec)
	// Reproduce the old journal transition directly in this package's storage
	// fixture; the production claim boundary now correctly refuses this overlap.
	_, err = stores.retentions.claimForRestoreWithAuthorityAtUnsafe(source.LeaseUUID, spec.LeaseUUID, 0,
		spec.Items, spec.ResourceProfiles, claim.OperationID(), spec.CallbackURL, spec.LifecycleCallbackURL, claim.CreatedAt())
	require.NoError(t, err)
	var subject OperationPhysicalSubject
	bindTestOperationMutation(t, stores.settlement, func(observed OperationPhysicalSubject) (OperationPhysicalEvidence, error) {
		subject = observed
		return NewOperationExactAbsent(observed)
	})
	candidate, err := stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	live, err := stores.settlement.StartOperationExecution(candidate)
	require.NoError(t, err)
	_, err = stores.restore.ProveInterruptedSourceClose(live.subject)
	require.Error(t, err, "ordinary execution cannot acquire recovery-only source authority")
	claims, err := stores.settlement.ListOperationIntents()
	require.NoError(t, err)
	coordinator := newTestRecoveryCoordinator(t, stores.settlement, nil, nil)
	acquired, err := coordinator.WithLease(t.Context(), spec.LeaseUUID, func(scope LeaseRecoveryScope) error {
		_, recoverErr := stores.settlement.CleanupRecoveredOperation(context.Background(), scope, claims[0])
		return recoverErr
	})
	require.NoError(t, err)
	require.True(t, acquired)
	proof, err := stores.restore.ProveInterruptedSourceClose(subject)
	require.NoError(t, err)
	require.NoError(t, proof.ReattestFor(subject))
	var zero InterruptedRestoreSource
	require.Error(t, zero.ReattestFor(subject))
	require.Error(t, proof.ReattestFor(live.subject), "the same operation's live subject is a different phase")
	other := openOperationHandoffStores(t, "docker-b")
	_, err = other.restore.ProveInterruptedSourceClose(subject)
	require.Error(t, err, "another journal set cannot borrow source authority")
	names := proof.RetainedVolumeNames()
	names[0] = "foreign-volume"
	require.Equal(t, []string{"retained-app-0"}, proof.RetainedVolumeNames())
	_, err = advanceCloseGenerationForTest(closeSettlement, closeClaim)
	require.NoError(t, err)
	require.Error(t, proof.ReattestFor(subject), "a copied source proof cannot survive a close retry generation")
	current, err := stores.restore.ProveInterruptedSourceClose(subject)
	require.NoError(t, err)
	require.NoError(t, current.ReattestFor(subject))
	require.NoError(t, stores.retentions.Close())
	require.Error(t, current.ReattestFor(subject), "closed journal instances revoke source authority")
}

func TestRestoreClaimAndSourceCloseAreMutuallyExclusive(t *testing.T) {
	for _, order := range []string{"close first", "restore first", "concurrent"} {
		t.Run(order, func(t *testing.T) {
			stores := openOperationHandoffStores(t, "docker-a")
			closeSettlement := newCloseSettlementForTest(t, stores)
			source := seedCloseSettlementRelease(t, stores, "restore-close-source")
			stack, err := manifest.ParsePayload(source.Manifest)
			require.NoError(t, err)
			require.NoError(t, stores.retentions.putForTest(RetentionEntry{
				OriginalLeaseUUID: source.LeaseUUID, Tenant: source.Tenant, ProviderUUID: source.ProviderUUID,
				Items: source.Items, ResourceProfiles: source.ResourceProfiles, StackManifest: stack,
				CallbackURL: source.CallbackURL, RetainedVolumeNames: []string{"retained-app-0"},
				Status: RetentionStatusActive, CreatedAt: time.Now(),
			}))
			spec := testOperationIntentSpec(t, "restore-close-destination")
			spec.Kind, spec.SourceLeaseUUID, spec.SourceGeneration = OperationIntentRestore, source.LeaseUUID, 1
			claim := beginHandoffOperation(t, stores.settlement, spec)
			candidate, err := stores.restore.PrepareRestoreClaim(claim)
			require.NoError(t, err)
			request, err := closeSettlement.NewCloseRequest(source.LeaseUUID, true)
			require.NoError(t, err)
			var closeErr, restoreErr error
			beginClose := func() { _, closeErr = closeSettlement.BeginClose(request) }
			claimRestore := func() { _, restoreErr = stores.restore.ClaimForRestore(candidate, 0) }
			switch order {
			case "close first":
				beginClose()
				claimRestore()
			case "restore first":
				claimRestore()
				beginClose()
			case "concurrent":
				start := make(chan struct{})
				var workers sync.WaitGroup
				workers.Go(func() { <-start; beginClose() })
				workers.Go(func() { <-start; claimRestore() })
				close(start)
				workers.Wait()
			}
			require.NotEqual(t, closeErr == nil, restoreErr == nil, "exactly one source owner must win")
			entry, err := stores.retentions.Get(source.LeaseUUID)
			require.NoError(t, err)
			require.NotNil(t, entry)
			_, pendingClose, err := closeSettlement.GetCloseIntent(source.LeaseUUID)
			require.NoError(t, err)
			if closeErr == nil {
				require.ErrorIs(t, restoreErr, ErrNotRestorable)
				require.True(t, pendingClose)
				require.Equal(t, RetentionStatusActive, entry.Status)
			} else {
				require.True(t, errors.Is(closeErr, ErrCloseIntentConflict), "%v", closeErr)
				require.False(t, pendingClose)
				require.Equal(t, RetentionStatusRestoring, entry.Status)
			}
		})
	}
}
