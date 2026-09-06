package shared

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

func operationSpecForClosedLease(t *testing.T, spec closeIntentSpec) OperationIntentSpec {
	t.Helper()
	return OperationIntentSpec{
		Kind:                 OperationIntentProvision,
		LeaseUUID:            spec.LeaseUUID,
		CallbackURL:          spec.CallbackURL,
		LifecycleCallbackURL: spec.LifecycleCallbackURL,
		Tenant:               spec.Tenant,
		ProviderUUID:         spec.ProviderUUID,
		Items:                slices.Clone(spec.Items),
		ResourceProfiles:     CloneSKUResourceSnapshot(spec.ResourceProfiles),
		EffectiveItems:       slices.Clone(spec.Items),
		Manifest:             bytes.Clone(spec.Manifest),
	}
}

func TestClosedLeaseTombstoneRecognizesExactRedeliveryAndRejectsResurrection(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store := newCloseIntentTestStore(t, dbPath)
	closeSpec := testCloseIntentSpec(t, "tombstone-redelivery")
	closeAdmission, err := beginUnboundCloseIntent(t, store, closeSpec)
	require.NoError(t, err)
	completion, err := store.ResolveCloseIntent(
		closeAdmission.Claim(), backend.CallbackStatusDeprovisioned, "", true,
	)
	require.NoError(t, err)
	require.NoError(t, store.Close())

	store = newCloseIntentTestStore(t, dbPath)
	exactProbe, err := newOperationIntentProbe(
		store,
		closeSpec.LeaseUUID, closeSpec.CallbackURL,
		testCloseIntentBackend, testCloseIntentStorageID(t),
	)
	require.NoError(t, err)
	disposition, err := store.ProbeOperationIntent(exactProbe)
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionCompleted, disposition)
	receipts, err := store.LookupClosedLeaseReceipts([]string{closeSpec.LeaseUUID})
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	assert.Equal(t, closeSpec.LeaseUUID, receipts[0].LeaseUUID())
	assert.Equal(t, testCloseIntentBackend, receipts[0].Backend())
	assert.Equal(t, testCloseIntentStorageID(t), receipts[0].BackendStorageID())
	assert.Equal(t, closeSpec.Tenant, receipts[0].Tenant())
	assert.Equal(t, closeSpec.ProviderUUID, receipts[0].ProviderUUID())
	assert.False(t, receipts[0].CleanupOnly())
	assert.Equal(t, ClosedLeaseAuthorityPrincipal, receipts[0].AuthorityKind())
	assert.False(t, receipts[0].ClosedAt().IsZero())
	capacity, err := store.LeaseMutationUUIDCapacity()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), capacity.Reserved)
	assert.Equal(t, maxLeaseMutationUUIDSlotsGlobal, capacity.Limit)
	assert.Equal(t, maxLeaseMutationUUIDSlotsGlobal-1, capacity.Remaining())
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		slots := tx.Bucket(callbackLeaseMutationUUIDSlotBucketName)
		assert.Equal(t, leaseMutationUUIDSlotValue, slots.Get([]byte(closeSpec.LeaseUUID)))
		assert.Equal(t, uint64(1), slots.Sequence())
		return nil
	}))
	require.NoError(t, store.removeEntry(completion))
	removed, err := store.removeOlderThan(time.Nanosecond)
	require.NoError(t, err)
	assert.Zero(t, removed, "TTL cleanup must never reclaim a closed UUID or its reserved slot")

	differentCallbackURL := "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	differentToken, err := newOperationIntentProbe(
		store,
		closeSpec.LeaseUUID, differentCallbackURL,
		testCloseIntentBackend, testCloseIntentStorageID(t),
	)
	require.NoError(t, err)
	_, err = store.ProbeOperationIntent(differentToken)
	require.ErrorIs(t, err, ErrOperationIntentConflict)

	operation := operationSpecForClosedLease(t, closeSpec)
	_, err = beginTestOperationIntent(t, store, operation)
	require.ErrorIs(t, err, ErrOperationIntentConflict,
		"even an exact old token is recognition-only after close")
	operation.CallbackURL = differentToken.CallbackURL()
	operation.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(operation.CallbackURL, "")
	require.NoError(t, err)
	_, err = beginTestOperationIntent(t, store, operation)
	require.ErrorIs(t, err, ErrOperationIntentConflict)
	_, err = beginUnboundCloseIntent(t, store, closeSpec)
	require.ErrorIs(t, err, ErrCloseIntentConflict,
		"a completed close cannot recreate an unresolvable close intent")

	require.NoError(t, store.Healthy())
	require.NoError(t, store.Close())
	inspection, err := InspectCallbackStoreReadOnly(dbPath)
	require.NoError(t, err)
	assert.True(t, inspection.UpgradedSchema)
	assert.Zero(t, inspection.Pending, "an immutable tombstone is history, not pending work")
	assert.Equal(t, uint64(1), inspection.LeaseMutationUUIDSlots)
	assert.Equal(t, maxLeaseMutationUUIDSlotsGlobal, inspection.LeaseMutationUUIDSlotLimit)
	assert.Zero(t, inspection.CallbackReceiptReservations,
		"the closed lease fence supersedes and releases its operation/maintenance receipts")
	assert.Equal(t, maxCallbackReceiptReservationsGlobal, inspection.CallbackReceiptReservationLimit)
}

func TestLeaseMutationUUIDCapacityRefusesBeforeCloseAdmission(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackLeaseMutationUUIDSlotBucketName).
			SetSequence(maxLeaseMutationUUIDSlotsGlobal)
	}))

	spec := testCloseIntentSpec(t, "uuid-capacity")
	_, err := beginUnboundCloseIntent(t, store, spec)
	require.ErrorIs(t, err, ErrLeaseMutationCapacity)
	require.ErrorIs(t, err, backend.ErrCapacityRefused)
	require.ErrorIs(t, err, backend.ErrInsufficientResources)
	var capacityErr *LeaseMutationCapacityError
	require.ErrorAs(t, err, &capacityErr)
	assert.Equal(t, maxLeaseMutationUUIDSlotsGlobal, capacityErr.Limit)

	_, found, getErr := store.GetCloseIntent(spec.LeaseUUID)
	require.NoError(t, getErr)
	assert.False(t, found, "capacity refusal must precede aggregate-head publication")
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		slots := tx.Bucket(callbackLeaseMutationUUIDSlotBucketName)
		assert.Nil(t, slots.Get([]byte(spec.LeaseUUID)),
			"capacity refusal must not manufacture an unowned UUID reservation")
		assert.Equal(t, maxLeaseMutationUUIDSlotsGlobal, slots.Sequence())
		return nil
	}))
}

func TestReservedLeaseMutationUUIDCanCompleteCloseAtCapacity(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testCloseIntentSpec(t, "reserved-close-capacity")
	admission, err := beginUnboundCloseIntent(t, store, spec)
	require.NoError(t, err)

	// Model every aggregate UUID slot becoming occupied after this close was
	// admitted. Completion must consume no fresh capacity: teardown is allowed
	// only because BeginCloseIntent already made the permanent retirement slot
	// inevitable in the same transaction as the close authority.
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackLeaseMutationUUIDSlotBucketName).
			SetSequence(maxLeaseMutationUUIDSlotsGlobal)
	}))
	_, err = store.ResolveCloseIntent(
		admission.Claim(), backend.CallbackStatusDeprovisioned, "", true,
	)
	require.NoError(t, err)
	receipts, err := store.LookupClosedLeaseReceipts([]string{spec.LeaseUUID})
	require.NoError(t, err)
	require.Len(t, receipts, 1)

	next := testCloseIntentSpec(t, "unreserved-close-capacity")
	_, err = beginUnboundCloseIntent(t, store, next)
	require.ErrorIs(t, err, ErrLeaseMutationCapacity)
}

func TestLeaseMutationUUIDSlotCorruptionFailsHealthAndInspection(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store := newCloseIntentTestStore(t, dbPath)
	spec := testCloseIntentSpec(t, "missing-slot")
	_, err := beginUnboundCloseIntent(t, store, spec)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		slots := tx.Bucket(callbackLeaseMutationUUIDSlotBucketName)
		if err := slots.Delete([]byte(spec.LeaseUUID)); err != nil {
			return err
		}
		return slots.SetSequence(0)
	}))

	require.ErrorContains(t, store.Healthy(), "no exact UUID slot")
	require.NoError(t, store.Close())
	_, err = InspectCallbackStoreReadOnly(dbPath)
	require.ErrorContains(t, err, "no exact UUID slot")
}

func TestClosedLeaseTombstoneAndLifecycleCallbackCommitAtomically(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	closeSpec := testCloseIntentSpec(t, "tombstone-atomic")
	admission, err := beginUnboundCloseIntent(t, store, closeSpec)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackV2BucketName).Put([]byte(closeSpec.LeaseUUID), []byte("poison"))
	}))

	_, err = store.ResolveCloseIntent(
		admission.Claim(), backend.CallbackStatusDeprovisioned, "", true,
	)
	require.ErrorContains(t, err, "is not a nested bucket")
	current, found, err := store.GetCloseIntent(closeSpec.LeaseUUID)
	require.NoError(t, err)
	require.True(t, found, "failed callback enqueue must preserve cleanup authority")
	assert.Equal(t, admission.Claim().IntentID(), current.IntentID())
	receipts, err := store.LookupClosedLeaseReceipts([]string{closeSpec.LeaseUUID})
	require.NoError(t, err)
	assert.Empty(t, receipts, "callback failure must roll back the tombstone in the same transaction")
}

func TestLookupClosedLeaseReceiptsScopesPermanentHistoryToRequestedLeases(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	first := testCloseIntentSpec(t, "lookup-first")
	second := testCloseIntentSpec(t, "lookup-second")
	for _, spec := range []closeIntentSpec{first, second} {
		admission, err := beginUnboundCloseIntent(t, store, spec)
		require.NoError(t, err)
		_, err = store.ResolveCloseIntent(
			admission.Claim(), backend.CallbackStatusDeprovisioned, "", true,
		)
		require.NoError(t, err)
	}

	receipts, err := store.LookupClosedLeaseReceipts([]string{
		second.LeaseUUID,
		second.LeaseUUID,
		"77777777-7777-4777-8777-777777777777",
	})
	require.NoError(t, err)
	require.Len(t, receipts, 1)
	assert.Equal(t, second.LeaseUUID, receipts[0].LeaseUUID())
	assert.Equal(t, ClosedLeaseAuthorityPrincipal, receipts[0].AuthorityKind())
}

func TestClosedLeaseReceiptZeroValueIsInvalid(t *testing.T) {
	receipt := ClosedLeaseReceipt{}
	assert.False(t, receipt.Valid())
	assert.Empty(t, receipt.LeaseUUID())
	assert.False(t, receipt.BackendStorageID().Valid())
}

func TestClosedLeaseTombstoneRejectsMaintenanceAdmission(t *testing.T) {
	releases, callbacks, source, target := maintenanceFixture(t, "closed-tombstone")
	closeSpec := testCloseIntentSpec(t, "maintenance-tombstone")
	closeSpec.LeaseUUID = source.LeaseUUID()
	closeSpec.ActiveReleaseVersion = source.Version()
	closeSpec.ActiveReleaseDigest = source.Digest()
	admission, err := beginUnboundCloseIntent(t, callbacks, closeSpec)
	require.NoError(t, err)
	_, err = callbacks.ResolveCloseIntent(
		admission.Claim(), backend.CallbackStatusDeprovisioned, "", true,
	)
	require.NoError(t, err)

	_, err = callbacks.BeginMaintenanceIntent(newMaintenanceIntentSpec(
		t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target,
		testCloseIntentBackend, testCloseIntentStorageID(t),
	))
	require.ErrorIs(t, err, ErrMaintenanceIntentConflict)
	_, _, err = releases.claimLatestActive(closeSpec.LeaseUUID)
	require.NoError(t, err, "the fixture remains a valid maintenance request absent the tombstone")
}

func TestClosedLeaseTombstoneCorruptionFailsHealthAndInspection(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store := newCloseIntentTestStore(t, dbPath)
	closeSpec := testCloseIntentSpec(t, "corrupt-tombstone")
	admission, err := beginUnboundCloseIntent(t, store, closeSpec)
	require.NoError(t, err)
	_, err = store.ResolveCloseIntent(
		admission.Claim(), backend.CallbackStatusDeprovisioned, "", true,
	)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
		key := []byte(closeSpec.LeaseUUID)
		var head storedLeaseMutationHead
		if err := json.Unmarshal(bucket.Get(key), &head); err != nil {
			return err
		}
		head.Closed.BackendStorageID = "not-a-storage-id"
		data, err := json.Marshal(head)
		if err != nil {
			return err
		}
		return bucket.Put(key, data)
	}))
	require.ErrorContains(t, store.Healthy(), "storage identity")
	require.NoError(t, store.Close())
	_, err = InspectCallbackStoreReadOnly(dbPath)
	require.ErrorContains(t, err, "storage identity")
}

func TestCleanupOnlyClosedLeaseRejectsPartialPrincipalAuthority(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store := newCloseIntentTestStore(t, dbPath)
	closeSpec := testCloseIntentSpec(t, "partial-cleanup-principal")
	closeSpec.CleanupOnly = true
	closeSpec.Tenant = ""
	closeSpec.ProviderUUID = ""
	closeSpec.CallbackURL = ""
	closeSpec.LifecycleCallbackURL = ""
	closeSpec.RetainOnClose = false
	admission, err := beginUnboundCloseIntent(t, store, closeSpec)
	require.NoError(t, err)
	_, err = store.ResolveCloseIntent(
		admission.Claim(), backend.CallbackStatusDeprovisioned, "", false,
	)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
		key := []byte(closeSpec.LeaseUUID)
		var head storedLeaseMutationHead
		if err := json.Unmarshal(bucket.Get(key), &head); err != nil {
			return err
		}
		head.Closed.Tenant = "tenant-without-provider"
		data, err := json.Marshal(head)
		if err != nil {
			return err
		}
		return bucket.Put(key, data)
	}))

	require.ErrorContains(t, store.Healthy(), "principal must be wholly absent or wholly present")
	require.NoError(t, store.Close())
	_, err = InspectCallbackStoreReadOnly(dbPath)
	require.ErrorContains(t, err, "principal must be wholly absent or wholly present")
}
