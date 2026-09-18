package docker

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestRestoreFinalizerRetriesAfterOperationSettlement(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		name := "live"
		if reopen {
			name = "reopened"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newVolumeWriterLaunchFixture(t, func(context.Context) ([]ContainerInfo, error) {
					return nil, errors.New("unresolved foreign writer inventory")
				})
				f.start(t, "restore")
				synctest.Wait()
				volumes, ok := f.b.volumes.(*mockVolumeManager)
				require.True(t, ok)
				var quotaUnavailable atomic.Bool
				quotaUnavailable.Store(true)
				volumes.EnsureQuotaFn = func(context.Context, string, int64) error {
					if quotaUnavailable.Load() {
						return errors.New("source quota temporarily unavailable")
					}
					return nil
				}
				time.Sleep(f.b.cfg.ProvisionTimeout + time.Second)
				require.ErrorContains(t, f.b.reconcileStateAndOperations(t.Context()), "cannot restore immutable source quota")
				pending, err := f.b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Empty(t, pending, "the operation has settled; only its restoring finalizer owns remaining handback")
				callbacks, err := f.b.callbackStore.ListPending()
				require.NoError(t, err)
				require.Len(t, callbacks, 1)
				require.Equal(t, backend.CallbackStatusFailed, callbacks[0].Status)
				source, err := f.b.retentionStore.Get(f.source)
				require.NoError(t, err)
				require.NotNil(t, source)
				require.Equal(t, shared.RetentionStatusRestoring, source.Status)
				if reopen {
					f.reopen(t)
				}
				quotaUnavailable.Store(false)
				require.NoError(t, f.b.reconcileStateAndOperations(t.Context()))
				source, err = f.b.retentionStore.Get(f.source)
				require.NoError(t, err)
				require.NotNil(t, source)
				require.Equal(t, shared.RetentionStatusActive, source.Status,
					"a finalizer must remain scheduled after the operation journal becomes empty")
				require.Zero(t, f.b.pool.Stats().AllocationCount)
				require.EqualValues(t, 2048, f.b.pool.Stats().RetainedDiskMB)
				for _, name := range source.RetainedVolumeNames {
					require.FileExists(t, filepath.Join(f.root, name, "sentinel"))
				}
				after, err := f.b.callbackStore.ListPending()
				require.NoError(t, err)
				require.Equal(t, callbacks, after)
				// The independent retention worker uses this same recovery stage.
				// Its later sweep is idempotent after the normal cadence completes.
				require.NoError(t, f.b.runRetentionSweep(t.Context()))
			})
		})
	}
}

func TestRestoreFinalizerProgressesDespiteUnrelatedOperationError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newVolumeWriterLaunchFixture(t, func(context.Context) ([]ContainerInfo, error) {
			return nil, errors.New("unresolved foreign writer inventory")
		})
		f.start(t, "restore")
		synctest.Wait()
		time.Sleep(f.b.cfg.ProvisionTimeout + time.Second)
		// Model the durable window after operation settlement and before its
		// independent source finalizer, as after an interrupted earlier pass.
		require.NoError(t, f.b.recoverLiveOperationIntents(t.Context()))
		source, err := f.b.retentionStore.Get(f.source)
		require.NoError(t, err)
		require.NotNil(t, source)
		require.Equal(t, shared.RetentionStatusRestoring, source.Status)
		spec := dockerOperationIntentSpec(t, f.b.storageIdentity)
		spec.LeaseUUID = "33333333-3333-4333-8333-333333333173"
		candidate, err := f.b.operationSettlement.NewOperationIntentCandidate(spec)
		require.NoError(t, err)
		admission, err := f.b.operationSettlement.BeginOperationIntent(candidate)
		require.NoError(t, err)
		_, created := admission.CreatedClaim()
		require.True(t, created)
		cause := errors.New("unrelated operation cleanup transaction unavailable")
		f.b.operationSettlement = restoreSiblingCleanupFailure{
			operationSettlementService: f.b.operationSettlement,
			leaseUUID:                  spec.LeaseUUID, cause: cause,
		}
		require.ErrorIs(t, f.b.reconcileStateAndOperations(t.Context()), cause)
		source, err = f.b.retentionStore.Get(f.source)
		require.NoError(t, err)
		require.NotNil(t, source)
		require.Equal(t, shared.RetentionStatusActive, source.Status,
			"an independently guarded finalizer must progress while its sibling operation reports an error")
		pending, err := f.b.operationSettlement.ListOperationIntents()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.Equal(t, spec.LeaseUUID, pending[0].LeaseUUID())
		for _, name := range source.RetainedVolumeNames {
			require.FileExists(t, filepath.Join(f.root, name, "sentinel"))
		}
	})
}

type restoreSiblingCleanupFailure struct {
	operationSettlementService
	leaseUUID string
	cause     error
}

func (s restoreSiblingCleanupFailure) CleanupRecoveredOperation(
	ctx context.Context,
	scope shared.LeaseRecoveryScope,
	claim shared.OperationIntentClaim,
) (shared.OperationExecutionOutcome, error) {
	if claim.LeaseUUID() == s.leaseUUID {
		return nil, s.cause
	}
	return s.operationSettlementService.CleanupRecoveredOperation(ctx, scope, claim)
}

func TestLaunchPreparationFailureReplaysAcrossJournalReopen(t *testing.T) {
	for _, kind := range []string{"provision", "restore"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newVolumeWriterLaunchFixture(t, func(context.Context) ([]ContainerInfo, error) {
					return nil, errors.New("unresolved foreign writer inventory")
				})
				f.start(t, kind)
				synctest.Wait()
				before, err := f.b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Len(t, before, 1)
				require.Equal(t, shared.OperationExecutionStarted, before[0].ExecutionPhase())
				f.reopen(t)
				after, err := f.b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Len(t, after, 1)
				require.Equal(t, before[0].OperationID(), after[0].OperationID())
				require.Equal(t, shared.OperationExecutionStarted, after[0].ExecutionPhase())
				debt, err := f.b.volumeLaunches.pendingCount()
				require.NoError(t, err)
				require.Zero(t, debt)

				time.Sleep(f.b.cfg.ProvisionTimeout + time.Second)
				require.NoError(t, f.b.reconcileStateAndOperations(t.Context()))
				pending, err := f.b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Empty(t, pending)
				callbacks, err := f.b.callbackStore.ListPending()
				require.NoError(t, err)
				require.Len(t, callbacks, 1)
				require.Equal(t, backend.CallbackStatusFailed, callbacks[0].Status)
				if kind == "restore" {
					source, err := f.b.retentionStore.Get(f.source)
					require.NoError(t, err)
					require.NotNil(t, source)
					require.Equal(t, shared.RetentionStatusActive, source.Status)
					for _, name := range source.RetainedVolumeNames {
						require.FileExists(t, filepath.Join(f.root, name, "sentinel"))
					}
				}
				// Replaying both recovery owners must preserve the same durable
				// terminal receipt, rather than issuing a duplicate callback.
				require.NoError(t, f.b.reconcileStateAndOperations(t.Context()))
				replayed, err := f.b.callbackStore.ListPending()
				require.NoError(t, err)
				require.Equal(t, callbacks, replayed)
			})
		})
	}
}

func TestPublicLaunchUnknownReplyPreservesDurableVolumeFence(t *testing.T) {
	for _, kind := range []string{"provision", "restore"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newVolumeWriterLaunchFixture(t, func(context.Context) ([]ContainerInfo, error) { return nil, nil })
				compose, ok := f.b.compose.(*mockComposeExecutor)
				require.True(t, ok)
				compose.UpFn = func(context.Context, *composetypes.Project, composeUpOpts) error {
					return errors.New("daemon reply lost after launch admission")
				}
				f.start(t, kind)
				synctest.Wait()
				before, err := f.b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Len(t, before, 1)
				debt, err := f.b.volumeLaunches.pendingCount()
				require.NoError(t, err)
				require.Equal(t, 1, debt)
				f.reopen(t)
				time.Sleep(f.b.cfg.ProvisionTimeout + time.Second)
				require.NoError(t, f.b.reconcileStateAndOperations(t.Context()))
				pending, err := f.b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Len(t, pending, 1, "time and empty inventory cannot settle an unknown daemon launch")
				require.Equal(t, before[0].OperationID(), pending[0].OperationID())
				debt, err = f.b.volumeLaunches.pendingCount()
				require.NoError(t, err)
				require.Equal(t, 1, debt)
				callbacks, err := f.b.callbackStore.ListPending()
				require.NoError(t, err)
				require.Empty(t, callbacks)
				for _, item := range f.items {
					path := filepath.Join(f.root, canonicalVolumeName(f.target, item.ServiceName, 0))
					require.DirExists(t, path, "unknown effects retain their protected bytes")
					if kind == "restore" {
						require.FileExists(t, filepath.Join(path, "sentinel"))
					}
				}
				if kind == "restore" {
					source, err := f.b.retentionStore.Get(f.source)
					require.NoError(t, err)
					require.NotNil(t, source)
					require.Equal(t, shared.RetentionStatusRestoring, source.Status)
					require.NoError(t, f.b.runRetentionSweep(t.Context()))
					stillOwned, err := f.b.retentionStore.Get(f.source)
					require.NoError(t, err)
					require.Equal(t, source, stillOwned)
				}
			})
		})
	}
}

// Reopen all authoritative journals into a new backend lifetime, without a
// surviving actor or provision projection. The underlying Docker and volume
// observations persist just as they do across a real process restart.
func (f *volumeWriterLaunchFixture) reopen(t *testing.T) {
	t.Helper()
	previous := f.b
	previous.stopCancel()
	previous.wg.Wait()
	require.NoError(t, previous.callbackStore.Close())
	require.NoError(t, previous.releaseStore.Close())
	require.NoError(t, previous.retentionStore.Close())
	mock, ok := previous.docker.(*mockDockerClient)
	require.True(t, ok)
	b := newBackendForTest(mock, nil)
	b.cfg, b.compose, b.volumes = previous.cfg, previous.compose, previous.volumes
	b.storageAuthority, b.storageIdentity = previous.storageAuthority, previous.storageIdentity
	var err error
	b.callbackStore, err = shared.OpenIdentityBoundCallbackStore(
		shared.CallbackStoreConfig{DBPath: b.cfg.CallbackDBPath}, b.storageAuthority, b.storeAuthorityGate,
	)
	require.NoError(t, err)
	b.releaseStore, err = shared.OpenIdentityBoundReleaseStore(
		shared.ReleaseStoreConfig{DBPath: b.cfg.ReleasesDBPath}, b.storageAuthority, b.storeAuthorityGate,
	)
	require.NoError(t, err)
	b.retentionStore, err = shared.OpenIdentityBoundRetentionStore(
		shared.RetentionStoreConfig{DBPath: b.cfg.RetentionDBPath}, b.storageAuthority, b.storeAuthorityGate,
	)
	require.NoError(t, err)
	b.operationSettlement = nil
	attachBoundOperationHandoffStores(t, b)
	bindRetentionOrphanPrunerForTest(t, b)
	rebuildCallbackSender(b, testCallbackClient)
	f.b = b
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
		require.NoError(t, b.callbackStore.Close())
		require.NoError(t, b.releaseStore.Close())
		require.NoError(t, b.retentionStore.Close())
	})
}
