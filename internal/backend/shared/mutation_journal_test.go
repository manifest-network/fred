package shared

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

func TestLeaseMutationTransitionConstructorsRejectPhaseSkips(t *testing.T) {
	t.Run("operation settlement cannot change immutable authority", func(t *testing.T) {
		store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
			DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })

		spec := testOperationIntentSpec(t, "transition-operation")
		admission, err := beginTestOperationIntent(t, store, spec)
		require.NoError(t, err)
		pending, err := validateOperationIntentClaim(createdOperationClaim(t, admission))
		require.NoError(t, err)

		terminal := cloneOperationMutationClaim(pending)
		terminal.entry.State = operationIntentSucceeded
		terminal.entry.SettledAt = time.Now()
		terminal.entry.Tenant = "different-tenant"

		_, err = newSettleOperationLeaseMutation(pending, terminal)
		require.ErrorContains(t, err, "changes immutable authority")
	})

	t.Run("maintenance target cannot bind before append starts", func(t *testing.T) {
		_, callbacks, source, target := maintenanceFixture(t, "transition-bind")
		candidate := newMaintenanceIntentSpec(
			t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target,
			"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
		)
		admission, err := callbacks.BeginMaintenanceIntent(candidate)
		require.NoError(t, err)
		initial := admission.intent

		entry := cloneMaintenanceIntentEntry(initial.entry)
		entry.AppendStarted = true
		entry.TargetReleaseVersion = entry.SourceReleaseVersion + 1
		boundRelease := cloneRelease(entry.TargetRelease)
		boundRelease.Version = entry.TargetReleaseVersion
		digest, err := maintenanceReleaseDigest(boundRelease)
		require.NoError(t, err)
		entry.TargetReleaseDigest = encodeMaintenanceDigest(digest)
		data, err := marshalMaintenanceIntent(entry)
		require.NoError(t, err)
		bound, err := decodeMaintenanceIntent([]byte(entry.LeaseUUID), data)
		require.NoError(t, err)

		_, err = newBindMaintenanceTargetLeaseMutation(initial, bound)
		require.ErrorContains(t, err, "requires started unbound")
	})

	t.Run("close advancement cannot skip execution generations", func(t *testing.T) {
		store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		admission, err := beginUnboundCloseIntent(t, store, testCloseIntentSpec(t, "transition-close"))
		require.NoError(t, err)
		current := admission.Claim()

		entry := cloneCloseIntentEntry(current.entry)
		entry.ExecutionGeneration += 2
		data, err := marshalCloseIntent(entry)
		require.NoError(t, err)
		skipped, err := decodeCloseIntent([]byte(entry.LeaseUUID), data)
		require.NoError(t, err)

		_, err = newAdvanceCloseLeaseMutation(current, skipped)
		require.ErrorContains(t, err, "increment only the execution generation")
	})
}

func TestApplyLeaseMutationRejectsForgedTypedPredecessor(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	spec := testOperationIntentSpec(t, "transition-forged-predecessor")
	admission, err := beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	pending, err := validateOperationIntentClaim(createdOperationClaim(t, admission))
	require.NoError(t, err)

	var before []byte
	require.NoError(t, store.view(func(tx *bolt.Tx) error {
		before = bytes.Clone(tx.Bucket(callbackLeaseMutationHeadBucketName).Get([]byte(spec.LeaseUUID)))
		return nil
	}))

	forged := cloneOperationMutationClaim(pending)
	forged.entry.Tenant = "different-tenant"
	terminal := cloneOperationMutationClaim(forged)
	terminal.entry.State = operationIntentFailed
	terminal.entry.SettledAt = time.Now()
	terminal.entry.SettlementError = "forged"
	transition := settleOperationLeaseMutation{previous: forged, next: terminal}

	err = store.update(func(tx *bolt.Tx) error {
		_, applyErr := applyLeaseMutationTx(tx, transition)
		return applyErr
	})
	require.ErrorContains(t, err, "diverges from stored authority")

	require.NoError(t, store.view(func(tx *bolt.Tx) error {
		after := tx.Bucket(callbackLeaseMutationHeadBucketName).Get([]byte(spec.LeaseUUID))
		require.Equal(t, before, after)
		return nil
	}))
	disposition, err := store.ProbeOperationIntent(testOperationIntentProbe(t, store, spec))
	require.NoError(t, err)
	require.Equal(t, OperationIntentAdmissionExisting, disposition)
}

func TestLeaseMutationTransitionVariantsCarryConcretePayloads(t *testing.T) {
	variants := []leaseMutationTransition{
		publishOperationLeaseMutation{},
		replaceOperationLeaseMutation{},
		settleOperationLeaseMutation{},
		publishMaintenanceLeaseMutation{},
		replaceOperationWithMaintenanceLeaseMutation{},
		startMaintenanceAppendLeaseMutation{},
		bindMaintenanceTargetLeaseMutation{},
		cancelMaintenanceLeaseMutation{},
		resolveMaintenanceLeaseMutation{},
		publishCloseLeaseMutation{},
		replaceOperationWithCloseLeaseMutation{},
		replaceMaintenanceWithCloseLeaseMutation{},
		advanceCloseLeaseMutation{},
		completeCloseLeaseMutation{},
	}
	require.Len(t, variants, 14)

	for _, variant := range variants {
		_, err := sealLeaseMutationTransition(variant)
		require.Error(t, err, "the zero value of %T must never carry authority", variant)
	}
}

func TestCallbackReceiptReservationsSurviveReopenAcrossIntentKinds(t *testing.T) {
	dir := t.TempDir()
	callbackPath := filepath.Join(dir, "callbacks.db")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)

	operationSpec := testOperationIntentSpec(t, "shared-capacity-reopen-operation")
	_, err = beginTestOperationIntent(t, store, operationSpec)
	require.NoError(t, err)
	maintenance, maintenanceReleases := beginPendingMaintenanceForCallbackStore(
		t, store, filepath.Join(dir, "releases.db"), "shared-capacity-reopen-maintenance",
	)
	require.Equal(t, uint64(2), callbackReceiptReservationsForTest(t, store))
	capacity, err := store.CallbackReceiptCapacity()
	require.NoError(t, err)
	require.Equal(t, CallbackReceiptCapacity{
		Reserved: 2,
		Limit:    maxCallbackReceiptReservationsGlobal,
	}, capacity)
	require.Equal(t, maxCallbackReceiptReservationsGlobal-2, capacity.Remaining())
	require.NoError(t, store.Healthy())
	require.NoError(t, store.Close())

	reopened, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	require.Equal(t, uint64(2), callbackReceiptReservationsForTest(t, reopened))
	reopenedCapacity, err := reopened.CallbackReceiptCapacity()
	require.NoError(t, err)
	require.Equal(t, capacity, reopenedCapacity)
	require.NoError(t, reopened.Healthy())

	recovered, found, err := maintenancePairForTest(
		t, reopened, maintenanceReleases,
	).GetMaintenanceIntent(maintenance.LeaseUUID())
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, recovered.Valid(), "reopen must recover settlement authority")
	require.Equal(t, uint64(2), callbackReceiptReservationsForTest(t, reopened),
		"recovery authority must not be convertible back into pre-dispatch cancellation authority")
	require.NoError(t, reopened.Healthy())
}

func TestCallbackReceiptCapacityRejectsImpossibleStoredCounter(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackLeaseMutationHeadBucketName).
			SetSequence(maxCallbackReceiptReservationsGlobal + 1)
	}))

	_, err = store.CallbackReceiptCapacity()
	require.ErrorContains(t, err, "global callback receipt capacity exceeded")

	saturated := CallbackReceiptCapacity{Reserved: 2, Limit: 1}
	require.Zero(t, saturated.Remaining())
}

func TestCallbackReceiptReservationRollsBackWithTransaction(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	_, err = beginTestOperationIntent(t, store, testOperationIntentSpec(t, "shared-capacity-rollback"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), callbackReceiptReservationsForTest(t, store))

	forcedRollback := errors.New("force shared receipt reservation rollback")
	err = store.db.Update(func(tx *bolt.Tx) error {
		reserved, reserveErr := reserveCallbackReceiptReservationWithinLimitTx(
			tx, maxCallbackReceiptReservationsGlobal,
		)
		require.NoError(t, reserveErr)
		require.True(t, reserved)
		return forcedRollback
	})
	require.ErrorIs(t, err, forcedRollback)
	require.Equal(t, uint64(1), callbackReceiptReservationsForTest(t, store))
	require.NoError(t, store.Healthy())
}

func TestGlobalCallbackReceiptCapacityRefusesMaintenanceBeforePublishing(t *testing.T) {
	dir := t.TempDir()
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
		DBPath: filepath.Join(dir, "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	releases, source, target := maintenanceAuthorityFixtureForCallbackStore(
		t, filepath.Join(dir, "releases.db"), "shared-capacity-boundary",
	)
	t.Cleanup(func() { require.NoError(t, releases.Close()) })
	candidate := newMaintenanceIntentSpec(
		t, store, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target,
		"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackLeaseMutationHeadBucketName).
			SetSequence(maxCallbackReceiptReservationsGlobal)
	}))

	_, err = store.BeginMaintenanceIntent(candidate)
	require.ErrorIs(t, err, ErrMaintenanceReceiptCapacity)
	require.ErrorIs(t, err, backend.ErrCapacityRefused)
	var capacityErr *MaintenanceReceiptCapacityError
	require.ErrorAs(t, err, &capacityErr)
	require.Empty(t, capacityErr.LeaseUUID)
	require.Equal(t, maxCallbackReceiptReservationsGlobal, capacityErr.Limit)

	_, found, lookupErr := store.getMaintenanceIntent(source.LeaseUUID())
	require.NoError(t, lookupErr)
	require.False(t, found)
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		slots := tx.Bucket(callbackLeaseMutationUUIDSlotBucketName)
		require.Nil(t, slots.Get([]byte(source.LeaseUUID())))
		require.Zero(t, slots.Sequence())
		return nil
	}))
}

func beginPendingMaintenanceForCallbackStore(
	t *testing.T,
	store *CallbackStore,
	releasePath string,
	name string,
) (MaintenanceIntentAdmission, *ReleaseStore) {
	t.Helper()
	releases, source, target := maintenanceAuthorityFixtureForCallbackStore(t, releasePath, name)
	t.Cleanup(func() { require.NoError(t, releases.Close()) })
	candidate := newMaintenanceIntentSpec(
		t, store, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target,
		"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	)
	admission, err := store.BeginMaintenanceIntent(candidate)
	require.NoError(t, err)
	return admission, releases
}

func maintenanceAuthorityFixtureForCallbackStore(
	t *testing.T,
	releasePath string,
	name string,
) (*ReleaseStore, ReleaseClaim, Release) {
	t.Helper()
	releases, err := newUnboundReleaseStoreForTest(ReleaseStoreConfig{DBPath: releasePath})
	require.NoError(t, err)
	leaseUUID := testLeaseUUID("maintenance-" + name)
	require.NoError(t, releases.appendActive(leaseUUID, validRuntimeAuthorityRelease()))
	active, source, err := releases.claimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	return releases, source, target
}

func callbackReceiptReservationsForTest(t *testing.T, store *CallbackStore) uint64 {
	t.Helper()
	var count uint64
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		var err error
		count, err = callbackReceiptReservationCountTx(tx)
		return err
	}))
	return count
}
