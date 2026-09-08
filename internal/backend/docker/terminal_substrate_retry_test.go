package docker

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/docker/docker/errdefs"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestProvisionUnaccountedFootprintRefusesBeforeEffects(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	callbacks := attachCapacityAdmissionCallbackStore(t, b)
	hold := b.pool.HoldUnaccountedFootprint()
	defer hold.Release()
	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	err := b.Provision(t.Context(), backend.ProvisionRequest{
		LeaseUUID: spec.LeaseUUID, Tenant: spec.Tenant, ProviderUUID: spec.ProviderUUID,
		Items: spec.Items, CallbackURL: spec.CallbackURL,
		LifecycleCallbackURL: spec.LifecycleCallbackURL, Payload: spec.Manifest,
	})
	require.ErrorIs(t, err, backend.ErrInsufficientResources)
	require.ErrorIs(t, err, shared.ErrResourceAccountingIncomplete)
	assertCapacityRefusalSettled(t, callbacks)
	require.Empty(t, b.pool.ListAllocations())
}

func TestFailedProjectionCannotCertifyYoungOperationAbsence(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	projection := readyIntentProjection(spec)
	projection[spec.LeaseUUID].Status = backend.ProvisionStatusFailed
	b := newOperationIntentRecoveryBackend(t, store, storageID, nil, projection)
	b.cfg.ProvisionTimeout = time.Hour
	claim := startPendingOperationForRecoveryTest(t, b)
	require.True(t, projectionMatchesOperationIntent(projection[spec.LeaseUUID], claim))
	acquired, err := b.recoveryCoordinator.WithLease(t.Context(), spec.LeaseUUID,
		func(scope shared.LeaseRecoveryScope) error {
			outcome, recoverErr := b.operationSettlement.RecoverOperationExecution(t.Context(), scope, claim)
			require.NoError(t, recoverErr)
			ambiguous, ok := outcome.(shared.OperationExecutionAmbiguous)
			require.True(t, ok, "a volatile Failed projection is not terminal operation evidence")
			require.ErrorContains(t, ambiguous.Cause(), "absence is not yet causally stable")
			return nil
		})
	require.NoError(t, err)
	require.True(t, acquired)
	claims, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
}

func TestClosedLeaseCleanupCancellationReattestsWithoutLatching(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	visible := false
	late := ContainerInfo{
		ContainerID: "late", LeaseUUID: closeRecoveryLeaseUUID,
		Tenant: "tenant-a", ProviderUUID: closeRecoveryProviderUUID,
		ServiceName: "app", SKU: "docker-small",
	}
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			if visible {
				return []ContainerInfo{late}, nil
			}
			return nil, nil
		},
		RemoveContainerFn: func(ctx context.Context, _ string) error {
			cancel()
			return ctx.Err()
		},
	}
	b, stores := openCloseRecoveryBackend(t, t.TempDir(), mock, nil)
	t.Cleanup(func() { closeCloseRecoveryBackend(t, b, stores) })
	claim := beginCloseRecoveryIntent(t, b, stores, false, "")
	completeDestroyedCloseForTest(t, b, stores.close, claim)
	// A real identity verifier honors context cancellation. Its postcheck must
	// still receive a live, bounded context after the removal call times out.
	mock.DaemonInfoFn = func(ctx context.Context) (DaemonSecurityInfo, error) {
		return DaemonSecurityInfo{SystemID: "close-recovery-daemon"}, ctx.Err()
	}
	visible = true
	_, err := b.recoverClosedLeaseSubstrate(ctx)
	require.NoError(t, err)
	require.NoError(t, b.terminalStorageAuthorityError())
	require.NoError(t, b.stopCtx.Err())
	require.True(t, b.pool.Stats().AccountingHeld)
	receipts, err := stores.callbacks.LookupClosedLeaseReceipts([]string{closeRecoveryLeaseUUID})
	require.NoError(t, err)
	require.Len(t, receipts, 1)
}

type terminalRecoveryOmittedOrdinaryInventory struct{ *mockDockerClient }

func (terminalRecoveryOmittedOrdinaryInventory) ListManagedContainers(context.Context) ([]ContainerInfo, error) {
	return nil, nil
}

func TestFailedReceiptCleanupDefersPreEffectCancellationAndTransientRemoval(t *testing.T) {
	for _, mode := range []string{"before_scope", "remove", "ordinary_omission"} {
		t.Run(mode, func(t *testing.T) {
			storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
			require.NoError(t, err)
			store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := dockerOperationIntentSpec(t, storageID)
			admission, err := beginDockerTestOperationIntent(t, store, spec, storageID)
			require.NoError(t, err)
			_, settlement := operationSettlementForCallbackTest(t, store)
			claim := createdDockerOperationClaim(t, admission)
			uncommitted := commitPreEffectOperationFailureForTest(t, settlement, claim)
			require.NoError(t, callbackPublisherForCallbackTest(t, store).PublishOperationFailureContext(
				t.Context(), uncommitted, interruptedOperationFailure))
			late := dockerIntentContainer(spec, "late-failed", spec.Items[0].SKU, 0)
			b := newOperationIntentRecoveryBackend(t, store, storageID, []ContainerInfo{late}, nil)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			blocked := true
			visible := true
			inventoryUnavailable := false
			removes := 0
			mock := b.docker.(*mockDockerClient)
			mock.ListManagedContainersFn = func(context.Context) ([]ContainerInfo, error) {
				if inventoryUnavailable {
					return nil, errors.New("strict inventory unavailable")
				}
				if mode == "before_scope" && blocked {
					cancel()
				}
				if visible {
					return []ContainerInfo{late}, nil
				}
				return nil, nil
			}
			mock.RemoveContainerFn = func(context.Context, string) error {
				removes++
				if blocked {
					return errors.New("device busy")
				}
				visible = false
				return nil
			}
			fence, err := b.recoverFailedOperationSubstrate(ctx)
			require.NoError(t, err)
			targets, err := fence.targets([]ContainerInfo{late})
			require.NoError(t, err)
			require.Len(t, targets, 1)
			require.Equal(t, late.ContainerID, targets[0].containerID)
			require.NoError(t, b.terminalStorageAuthorityError())
			require.NoError(t, b.stopCtx.Err())
			require.True(t, b.pool.Stats().AccountingHeld)
			if mode == "ordinary_omission" {
				b.docker = terminalRecoveryOmittedOrdinaryInventory{mock}
				require.NoError(t, b.recoverState(t.Context()))
				require.True(t, b.pool.Stats().AccountingHeld,
					"an ordinary omitted positive cannot release a hold retained by strict cleanup")
			}
			if mode == "before_scope" {
				require.Zero(t, removes)
			}
			inventoryUnavailable = true
			_, err = b.recoverFailedOperationSubstrate(t.Context())
			require.ErrorContains(t, err, "strict inventory unavailable")
			require.True(t, b.pool.Stats().AccountingHeld,
				"an unsuccessful strict observation must preserve the capacity hold")
			inventoryUnavailable = false
			blocked = false
			_, err = b.recoverFailedOperationSubstrate(t.Context())
			require.NoError(t, err)
			require.False(t, b.pool.Stats().AccountingHeld)
			require.False(t, visible)
		})
	}
}

func TestFailedSubstrateHoldSurvivesCloseReceiptSupersessionUntilStrictAbsence(t *testing.T) {
	for _, releaseOrder := range []string{"closed_first", "failed_first"} {
		t.Run(releaseOrder, func(t *testing.T) {
			var containers []ContainerInfo
			blocked := false
			inventoryUnavailable := false
			removes := 0
			mock := &mockDockerClient{
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
					if inventoryUnavailable {
						return nil, errors.New("strict inventory unavailable")
					}
					return slices.Clone(containers), nil
				},
				InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
					for _, container := range containers {
						if container.ContainerID == id {
							return &container, nil
						}
					}
					return nil, errdefs.NotFound(errors.New("container absent"))
				},
				RemoveContainerFn: func(_ context.Context, id string) error {
					removes++
					if blocked {
						return errors.New("device busy")
					}
					containers = slices.DeleteFunc(containers, func(c ContainerInfo) bool { return c.ContainerID == id })
					return nil
				},
			}
			b, stores := openCloseRecoveryBackend(t, t.TempDir(), mock, nil)
			t.Cleanup(func() { closeCloseRecoveryBackend(t, b, stores) })
			b.compose.(*mockComposeExecutor).DownFn = func(context.Context, string, time.Duration) error {
				if blocked {
					return errors.New("device busy")
				}
				containers = nil
				return nil
			}
			spec := dockerOperationIntentSpec(t, b.storageIdentity)
			_, err := beginDockerTestOperationIntent(t, stores.callbacks, spec, b.storageIdentity)
			require.NoError(t, err)
			startPendingOperationForRecoveryTest(t, b)
			b.cfg.ProvisionTimeout = time.Nanosecond
			require.NoError(t, b.recoverOperationIntents(t.Context()),
				"recover a genuinely Started operation through exact physical cleanup")

			late := dockerIntentContainer(spec, "late-failed-before-close", spec.Items[0].SKU, 0)
			containers = []ContainerInfo{late}
			blocked = true
			_, err = b.recoverFailedOperationSubstrate(t.Context())
			require.NoError(t, err)
			require.NotNil(t, b.failedSubstrateCapacityHold)

			// Complete a genuine typed close while the accounting observer is not
			// running. Its permanent closed receipt atomically retires per-operation
			// history, but it cannot consume the observer's independent pool hold.
			blocked = false
			request, err := stores.close.NewCleanupCloseRequest(spec.LeaseUUID)
			require.NoError(t, err)
			closeAdmission, err := stores.close.BeginCleanupClose(request)
			require.NoError(t, err)
			completeDestroyedCloseForTest(t, b, stores.close, closeAdmission.Claim())
			receipts, err := stores.operations.ListFailedOperationReceipts()
			require.NoError(t, err)
			require.Empty(t, receipts, "the stronger closed receipt supersedes operation history")
			require.True(t, b.pool.Stats().AccountingHeld)

			// A still-later daemon Create has the same immutable callback identity.
			// Receipt pruning is not evidence that this physical cohort is absent.
			late.ContainerID = "late-failed-after-close"
			containers = []ContainerInfo{late}
			blocked = true
			before := removes
			_, err = b.recoverFailedOperationSubstrate(t.Context())
			require.NoError(t, err)
			require.Equal(t, before, removes, "retained observation scope must not issue stale destructive authority")
			require.NotNil(t, b.failedSubstrateCapacityHold)
			require.Equal(t, float64(1), testutil.ToFloat64(terminalSubstratePendingContainers.WithLabelValues("failed_operation")))

			// Even one surviving identity must withhold capacity. The observation
			// scope is deliberately more conservative than a mutation capability.
			containers[0].LifecycleCallbackURL = ""
			_, err = b.recoverFailedOperationSubstrate(t.Context())
			require.NoError(t, err)
			require.NotNil(t, b.failedSubstrateCapacityHold)
			containers[0] = late
			_, err = b.recoverClosedLeaseSubstrate(t.Context())
			require.NoError(t, err)
			require.NotNil(t, b.closedSubstrateCapacityHold)

			inventoryUnavailable = true
			_, err = b.recoverFailedOperationSubstrate(t.Context())
			require.ErrorContains(t, err, "strict inventory unavailable")
			require.NotNil(t, b.failedSubstrateCapacityHold)
			inventoryUnavailable = false
			if releaseOrder == "failed_first" {
				// A different generation remains retired by the closed UUID but
				// is outside the failed operation's exact observation scope.
				containers[0].CallbackURL = ""
				containers[0].LifecycleCallbackURL = ""
				_, err = b.recoverFailedOperationSubstrate(t.Context())
				require.NoError(t, err)
				require.Nil(t, b.failedSubstrateCapacityHold)
				require.NotNil(t, b.closedSubstrateCapacityHold)
				require.True(t, b.pool.Stats().AccountingHeld, "failed-family absence cannot release closed-family capacity")
			}
			blocked = false
			_, err = b.recoverClosedLeaseSubstrate(t.Context())
			require.NoError(t, err)
			require.Nil(t, b.closedSubstrateCapacityHold)
			if releaseOrder == "closed_first" {
				require.True(t, b.pool.Stats().AccountingHeld, "closed-family absence cannot release failed-family capacity")
			}
			_, err = b.recoverFailedOperationSubstrate(t.Context())
			require.NoError(t, err)
			require.Nil(t, b.failedSubstrateCapacityHold)
			require.False(t, b.pool.Stats().AccountingHeld)
			require.Equal(t, float64(0), testutil.ToFloat64(terminalSubstratePendingContainers.WithLabelValues("failed_operation")))
		})
	}
}

func TestOperationRecoveryCleanupRetainsIntentAndReservationForNextPass(t *testing.T) {
	storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
	require.NoError(t, err)
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := dockerOperationIntentSpec(t, storageID)
	_, err = beginDockerTestOperationIntent(t, store, spec, storageID)
	require.NoError(t, err)
	container := dockerIntentContainer(spec, "interrupted", spec.Items[0].SKU, 0)
	container.Status = "exited"
	b := newOperationIntentRecoveryBackend(t, store, storageID,
		[]ContainerInfo{container}, readyIntentProjection(spec, container.ContainerID))
	b.cfg.ProvisionTimeout = time.Nanosecond
	require.NoError(t, b.pool.TryAllocate(spec.LeaseUUID, spec.Items[0].SKU, spec.Tenant))
	before := b.pool.ListAllocations()
	blocked := true
	mock := b.docker.(*mockDockerClient)
	mock.RemoveContainerFn = func(context.Context, string) error { return errors.New("device busy") }
	compose := b.compose.(*mockComposeExecutor)
	originalDown := compose.DownFn
	compose.DownFn = func(ctx context.Context, lease string, timeout time.Duration) error {
		if blocked {
			return errors.New("compose unavailable")
		}
		return originalDown(ctx, lease, timeout)
	}
	metric := operationIntentRecoveryCleanupRetriesTotal.WithLabelValues("provision")
	metricBefore := testutil.ToFloat64(metric)
	require.NoError(t, b.recoverLiveOperationIntents(t.Context()))
	require.NoError(t, b.stopCtx.Err())
	require.NoError(t, b.terminalStorageAuthorityError())
	claims, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	require.Equal(t, before, b.pool.ListAllocations())
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Empty(t, pending)
	require.Equal(t, metricBefore+1, testutil.ToFloat64(metric))
	blocked = false
	require.NoError(t, b.recoverLiveOperationIntents(t.Context()))
	claims, err = b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Empty(t, claims)
	pending, err = store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
}
