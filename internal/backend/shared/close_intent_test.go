package shared

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
)

const testCloseIntentBackend = "docker-a"

func testCloseIntentStorageID(t *testing.T) backendidentity.ID {
	t.Helper()
	return callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000")
}

func testCloseIntentSpec(t *testing.T, name string) closeIntentSpec {
	t.Helper()
	operationURL := "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(operationURL, "")
	require.NoError(t, err)
	operationID, err := parseOperationCallbackID(operationURL)
	require.NoError(t, err)
	return closeIntentSpec{
		LeaseUUID:                testLeaseUUID("close-" + name),
		Tenant:                   "tenant-a",
		ProviderUUID:             "22222222-2222-4222-8222-222222222222",
		Items:                    []backend.LeaseItem{{SKU: "small", Quantity: 2, ServiceName: "app"}},
		ResourceProfiles:         []SKUResourceSnapshot{{SKU: "small", CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}},
		Manifest:                 []byte(`{"services":{"app":{"image":"example.invalid/app:1"}}}`),
		CallbackURL:              operationURL,
		LifecycleCallbackURL:     lifecycleURL,
		RetainOnClose:            true,
		ActiveReleaseVersion:     7,
		ActiveReleaseDigest:      sha256.Sum256([]byte("active-release-" + name)),
		ActiveReleaseOperationID: operationID,
	}
}

func newUnboundCloseCandidate(
	t *testing.T,
	spec closeIntentSpec,
) (closeIntentCandidate, error) {
	t.Helper()
	return newCloseIntentCandidate(
		nil, spec, testCloseIntentBackend, testCloseIntentStorageID(t),
	)
}

func newUnboundCloseCandidateWithLineage(
	t *testing.T,
	spec closeIntentSpec,
	backendName string,
	storageID backendidentity.ID,
) (closeIntentCandidate, error) {
	t.Helper()
	return newCloseIntentCandidate(nil, spec, backendName, storageID)
}

func beginUnboundCloseIntent(
	t *testing.T,
	store *CallbackStore,
	spec closeIntentSpec,
) (CloseIntentAdmission, error) {
	t.Helper()
	candidate, err := newUnboundCloseCandidate(t, spec)
	if err != nil {
		return CloseIntentAdmission{}, err
	}
	return store.BeginCloseIntent(candidate)
}

func beginUnboundCloseIntentWithLineage(
	t *testing.T,
	store *CallbackStore,
	spec closeIntentSpec,
	backendName string,
	storageID backendidentity.ID,
) (CloseIntentAdmission, error) {
	t.Helper()
	candidate, err := newUnboundCloseCandidateWithLineage(
		t, spec, backendName, storageID,
	)
	if err != nil {
		return CloseIntentAdmission{}, err
	}
	return store.BeginCloseIntent(candidate)
}

func newCloseIntentTestStore(t *testing.T, dbPath string) *CallbackStore {
	t.Helper()
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	return store
}

func openBoundCloseIntentTestStore(
	t *testing.T,
) (*CallbackStore, backendidentity.VerifiedStorage) {
	t.Helper()
	dbPath, storage := initializeBoundCallbackStore(t)
	store, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: dbPath}, storage, newTestStorageAuthorityGate(t),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store, storage
}

func TestCloseIntentCandidateIsBoundToItsIssuingJournal(t *testing.T) {
	issuer, issuerStorage := openBoundCloseIntentTestStore(t)
	other, _ := openBoundCloseIntentTestStore(t)
	spec := testCloseIntentSpec(t, "wrong-store")
	candidate, err := issuer.NewCloseIntentCandidate(spec)
	require.NoError(t, err)

	_, err = other.BeginCloseIntent(candidate)
	require.ErrorContains(t, err, "not minted by this callback journal")
	otherCloses, listErr := other.ListCloseIntents()
	require.NoError(t, listErr)
	assert.Empty(t, otherCloses)

	admission, err := issuer.BeginCloseIntent(candidate)
	require.NoError(t, err)
	assert.Equal(t, issuerStorage.BackendName(), admission.Claim().Backend())
	assert.Equal(t, issuerStorage.ID(), admission.Claim().BackendStorageID())
}

func TestCloseIntentCandidateDetachesCallerOwnedInputBeforeAdmission(t *testing.T) {
	store, _ := openBoundCloseIntentTestStore(t)
	spec := testCloseIntentSpec(t, "immutable-candidate")
	candidate, err := store.NewCloseIntentCandidate(spec)
	require.NoError(t, err)

	spec.Items[0].SKU = "mutated"
	spec.ResourceProfiles[0].MemoryMB = 1
	spec.Manifest[0] = 'x'

	admission, err := store.BeginCloseIntent(candidate)
	require.NoError(t, err)
	assert.Equal(t, "small", admission.Claim().Items()[0].SKU)
	assert.EqualValues(t, 512, admission.Claim().ResourceProfiles()[0].MemoryMB)
	assert.Equal(t, byte('{'), admission.Claim().Manifest()[0])
}

func TestBeginCloseIntentRejectsZeroCandidate(t *testing.T) {
	store, _ := openBoundCloseIntentTestStore(t)
	_, err := store.BeginCloseIntent(closeIntentCandidate{})
	require.Error(t, err)
	closes, listErr := store.ListCloseIntents()
	require.NoError(t, listErr)
	assert.Empty(t, closes)
}

func TestCloseIntentSurvivesRestartAndResolvesExactlyOnce(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store := newCloseIntentTestStore(t, dbPath)
	spec := testCloseIntentSpec(t, "restart")
	admission, err := beginUnboundCloseIntent(t, store, spec)
	require.NoError(t, err)
	require.Equal(t, CloseIntentAdmissionCreated, admission.Disposition())
	require.NotEmpty(t, admission.Claim().IntentID())
	require.Zero(t, admission.Claim().ExecutionGeneration().Number())

	// Neither caller-owned input nor accessor output can mutate the claim.
	spec.Items[0].SKU = "mutated"
	spec.ResourceProfiles[0].MemoryMB = 1
	spec.Manifest[0] = 'x'
	items := admission.Claim().Items()
	profiles := admission.Claim().ResourceProfiles()
	manifestBytes := admission.Claim().Manifest()
	items[0].SKU = "also-mutated"
	profiles[0].MemoryMB = 2
	manifestBytes[0] = 'x'
	assert.Equal(t, "small", admission.Claim().Items()[0].SKU)
	assert.EqualValues(t, 512, admission.Claim().ResourceProfiles()[0].MemoryMB)
	assert.Equal(t, byte('{'), admission.Claim().Manifest()[0])

	require.NoError(t, store.Close())
	store = newCloseIntentTestStore(t, dbPath)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	claims, err := store.ListCloseIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	assert.Equal(t, admission.Claim().IntentID(), claims[0].IntentID())

	staleCopy := claims[0]
	refreshed, err := store.AdvanceCloseExecutionGeneration(claims[0])
	require.NoError(t, err)
	assert.Equal(t, 1, refreshed.ExecutionGeneration().Number())
	_, err = store.ResolveCloseIntent(staleCopy, backend.CallbackStatusDeprovisioned, "", true)
	require.ErrorContains(t, err, "changed before precise mutation")

	require.NoError(t, store.Close())
	store = newCloseIntentTestStore(t, dbPath)
	claims, err = store.ListCloseIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	assert.Equal(t, 1, claims[0].ExecutionGeneration().Number(), "restart must preserve the execution generation")

	copyForReplay := claims[0]
	entry, err := store.ResolveCloseIntent(
		claims[0], backend.CallbackStatusDeprovisioned, "", true,
	)
	require.NoError(t, err)
	assert.Equal(t, CallbackDeliveryKindLifecycle, entry.DeliveryKind)
	assert.Equal(t, spec.LifecycleCallbackURL, entry.CallbackURL)
	assert.True(t, entry.Retained)
	_, err = store.ResolveCloseIntent(copyForReplay, backend.CallbackStatusDeprovisioned, "", true)
	require.ErrorContains(t, err, "replaced by \"closed\"")

	claims, err = store.ListCloseIntents()
	require.NoError(t, err)
	assert.Empty(t, claims)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusDeprovisioned, pending[0].Status)
}

func TestCloseIntentRecoverySurvivesWallClockRollback(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store := newCloseIntentTestStore(t, dbPath)
	spec := testCloseIntentSpec(t, "future-after-clock-rollback")
	_, err := beginUnboundCloseIntent(t, store, spec)
	require.NoError(t, err)
	futureCreatedAt := time.Now().Add(24 * time.Hour)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
		key := []byte(spec.LeaseUUID)
		var head storedLeaseMutationHead
		if unmarshalErr := json.Unmarshal(bucket.Get(key), &head); unmarshalErr != nil {
			return unmarshalErr
		}
		head.Close.CreatedAt = futureCreatedAt
		data, marshalErr := json.Marshal(head)
		if marshalErr != nil {
			return marshalErr
		}
		return bucket.Put(key, data)
	}))
	require.NoError(t, store.Close())

	store = newCloseIntentTestStore(t, dbPath)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	claims, err := store.ListCloseIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	assert.Equal(t, futureCreatedAt.UnixNano(), claims[0].CreatedAt().UnixNano())
	current, found, err := store.GetCloseIntent(spec.LeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, claims[0].IntentID(), current.IntentID())
	require.NoError(t, store.Healthy())

	_, err = store.ResolveCloseIntent(
		current, backend.CallbackStatusDeprovisioned, "", true,
	)
	require.NoError(t, err)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusDeprovisioned, pending[0].Status)
}

func TestBeginCloseIntentAtomicallyPreemptsOperationIntent(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	wake := newCallbackReplayMailbox()
	unsubscribe := store.subscribeReplayWake(wake)
	t.Cleanup(unsubscribe)
	closeSpec := testCloseIntentSpec(t, "preempt")
	operationSpec := OperationIntentSpec{
		Kind:                 OperationIntentProvision,
		LeaseUUID:            closeSpec.LeaseUUID,
		CallbackURL:          closeSpec.CallbackURL,
		LifecycleCallbackURL: closeSpec.LifecycleCallbackURL,
		Tenant:               closeSpec.Tenant,
		ProviderUUID:         closeSpec.ProviderUUID,
		Items:                slices.Clone(closeSpec.Items),
		ResourceProfiles:     CloneSKUResourceSnapshot(closeSpec.ResourceProfiles),
		Manifest:             bytes.Clone(closeSpec.Manifest),
	}
	operation, err := beginTestOperationIntent(t, store, operationSpec, operationIntentTestIdentity{
		backend: testCloseIntentBackend, storageID: testCloseIntentStorageID(t),
	})
	require.NoError(t, err)
	require.Equal(t, OperationIntentAdmissionCreated, operation.Disposition())
	operationClaim, created := operation.CreatedClaim()
	require.True(t, created)
	select {
	case <-wake.ready:
		t.Fatal("an intent-only write must not wake callback replay")
	default:
	}

	closeAdmission, err := beginUnboundCloseIntent(t, store, closeSpec)
	require.NoError(t, err)
	assert.True(t, closeAdmission.OperationPreempted())
	select {
	case <-wake.ready:
		wakes := wake.take()
		require.Len(t, wakes, 1)
		assert.Equal(t, closeSpec.LeaseUUID, wakes[0].leaseUUID)
		assert.Equal(t, callbackReplayWakeCommit, wakes[0].kind)
	case <-time.After(time.Second):
		t.Fatal("atomic operation preemption did not wake callback replay")
	}
	operations, err := store.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, operations)
	closes, err := store.ListCloseIntents()
	require.NoError(t, err)
	require.Len(t, closes, 1)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, CallbackDeliveryKindOperation, pending[0].DeliveryKind)
	assert.Equal(t, closeSpec.CallbackURL, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, closeIntentPreemptedOperation, pending[0].Error)
	assert.False(t, operationClaim.OperationID().IsZero(), "fixture must exercise typed operation authority")

	// The preempted worker may finish after close admission. Its old success
	// cannot replace or append to the exact failure already published by the
	// atomic preemption.
	err = store.settleOperationCallbackLocked(
		callbackEntryForOperationSpec(operationSpec, backend.CallbackStatusSuccess),
	)
	require.ErrorIs(t, err, ErrOperationIntentConflict)
	pending, err = store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, closeIntentPreemptedOperation, pending[0].Error)
}

func TestBeginCloseIntentRollsBackCloseAndOperationPreemptionTogether(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	closeSpec := testCloseIntentSpec(t, "atomic-rollback")
	operationSpec := testOperationIntentSpec(t, "atomic-rollback")
	operationSpec.LeaseUUID = closeSpec.LeaseUUID
	operationSpec.CallbackURL = closeSpec.CallbackURL
	operationSpec.LifecycleCallbackURL = closeSpec.LifecycleCallbackURL
	operationSpec.Tenant = closeSpec.Tenant
	operationSpec.ProviderUUID = closeSpec.ProviderUUID
	operationSpec.Items = slices.Clone(closeSpec.Items)
	operationSpec.Manifest = bytes.Clone(closeSpec.Manifest)
	_, err := beginTestOperationIntent(t, store, operationSpec, operationIntentTestIdentity{
		backend: testCloseIntentBackend, storageID: testCloseIntentStorageID(t),
	})
	require.NoError(t, err)

	// A corrupt non-bucket v2 lease key makes callback publication fail after
	// BeginCloseIntent has tentatively put the close row in the same transaction.
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackV2BucketName).Put([]byte(closeSpec.LeaseUUID), []byte("poison"))
	}))
	_, err = beginUnboundCloseIntent(t, store, closeSpec)
	require.ErrorContains(t, err, "is not a nested bucket")

	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		head, present, err := getLeaseMutationHeadTx(tx, closeSpec.LeaseUUID)
		require.NoError(t, err)
		require.True(t, present)
		_, operation := head.(operationLeaseMutationHead)
		assert.True(t, operation, "failed close admission must preserve the sole operation head")
		return nil
	}))
}

func TestResolveCloseIntentRollsBackRemovalWhenCallbackEnqueueFails(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testCloseIntentSpec(t, "resolve-rollback")
	admission, err := beginUnboundCloseIntent(t, store, spec)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackV2BucketName).Put([]byte(spec.LeaseUUID), []byte("poison"))
	}))

	_, err = store.ResolveCloseIntent(
		admission.Claim(), backend.CallbackStatusDeprovisioned, "", false,
	)
	require.ErrorContains(t, err, "is not a nested bucket")
	current, found, getErr := store.GetCloseIntent(spec.LeaseUUID)
	require.NoError(t, getErr)
	require.True(t, found, "failed callback enqueue must roll back close-intent removal")
	assert.Equal(t, admission.Claim().IntentID(), current.IntentID())
}

func TestBeginCloseIntentRejectsMismatchedOperationStorageAuthority(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	closeSpec := testCloseIntentSpec(t, "storage-mismatch")
	operationSpec := testOperationIntentSpec(t, "storage-mismatch")
	operationSpec.LeaseUUID = closeSpec.LeaseUUID
	operationSpec.CallbackURL = closeSpec.CallbackURL
	operationSpec.LifecycleCallbackURL = closeSpec.LifecycleCallbackURL
	_, err := beginTestOperationIntent(t, store, operationSpec, operationIntentTestIdentity{
		backend:   testCloseIntentBackend,
		storageID: callbackStorageID(t, "6ba7b810-9dad-41d1-80b4-00c04fd430c8"),
	})
	require.NoError(t, err)

	_, err = beginUnboundCloseIntent(t, store, closeSpec)
	require.ErrorContains(t, err, "different backend storage authority")
	closes, listErr := store.ListCloseIntents()
	require.NoError(t, listErr)
	assert.Empty(t, closes)
	operations, listErr := store.ListOperationIntents()
	require.NoError(t, listErr)
	require.Len(t, operations, 1)
}

func TestCloseIntentBlocksNewOperationAdmissionAndProbe(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	closeSpec := testCloseIntentSpec(t, "blocks-operation")
	closeAdmission, err := beginUnboundCloseIntent(t, store, closeSpec)
	require.NoError(t, err)
	operationSpec := testOperationIntentSpec(t, "blocked-operation")
	operationSpec.LeaseUUID = closeSpec.LeaseUUID
	operationSpec.CallbackURL = closeSpec.CallbackURL
	operationSpec.LifecycleCallbackURL = closeSpec.LifecycleCallbackURL
	operationSpec.Tenant = closeSpec.Tenant
	operationSpec.ProviderUUID = closeSpec.ProviderUUID
	operationSpec.Items = slices.Clone(closeSpec.Items)
	operationSpec.Manifest = bytes.Clone(closeSpec.Manifest)

	operationIdentity := operationIntentTestIdentity{
		backend: testCloseIntentBackend, storageID: testCloseIntentStorageID(t),
	}
	_, err = beginTestOperationIntent(t, store, operationSpec, operationIdentity)
	require.ErrorIs(t, err, ErrOperationIntentConflict)
	probe := testOperationIntentProbe(t, store, operationSpec, operationIdentity)
	_, err = store.ProbeOperationIntent(probe)
	require.ErrorIs(t, err, ErrOperationIntentConflict)
	lateCompletion := CallbackEntry{
		LeaseUUID:        operationSpec.LeaseUUID,
		CallbackURL:      operationSpec.CallbackURL,
		DeliveryKind:     CallbackDeliveryKindOperation,
		Status:           backend.CallbackStatusSuccess,
		Backend:          testCloseIntentBackend,
		BackendStorageID: operationIdentity.storageID.String(),
		CreatedAt:        time.Now(),
	}
	_, err = store.storeEntry(lateCompletion)
	require.ErrorIs(t, err, ErrCallbackIntentRequired)
	err = store.settleOperationCallbackLocked(lateCompletion)
	require.ErrorIs(t, err, ErrOperationIntentConflict)
	operations, err := store.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, operations)
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "a late operation completion cannot cross a durable close barrier")
	current, found, err := store.GetCloseIntent(closeSpec.LeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, closeAdmission.Claim().IntentID(), current.IntentID())
}

func TestCallbackStoreHealthRejectsAggregateWithMultipleStates(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	closeSpec := testCloseIntentSpec(t, "journal-overlap")
	operationSpec := testOperationIntentSpec(t, "journal-overlap")
	operationSpec.LeaseUUID = closeSpec.LeaseUUID
	operationSpec.CallbackURL = closeSpec.CallbackURL
	operationSpec.LifecycleCallbackURL = closeSpec.LifecycleCallbackURL
	operationSpec.Tenant = closeSpec.Tenant
	operationSpec.ProviderUUID = closeSpec.ProviderUUID
	operationSpec.Items = slices.Clone(closeSpec.Items)
	operationSpec.Manifest = bytes.Clone(closeSpec.Manifest)
	_, err := beginTestOperationIntent(t, store, operationSpec, operationIntentTestIdentity{
		backend: testCloseIntentBackend, storageID: testCloseIntentStorageID(t),
	})
	require.NoError(t, err)
	var operationHead storedLeaseMutationHead
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		return json.Unmarshal(
			tx.Bucket(callbackLeaseMutationHeadBucketName).Get([]byte(closeSpec.LeaseUUID)),
			&operationHead,
		)
	}))
	_, err = beginUnboundCloseIntent(t, store, closeSpec)
	require.NoError(t, err)

	// The persisted tagged union cannot encode this through its mutation API.
	// Inject both payloads as corruption and ensure strict decoding fails closed.
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
		key := []byte(closeSpec.LeaseUUID)
		var closeHead storedLeaseMutationHead
		require.NoError(t, json.Unmarshal(bucket.Get(key), &closeHead))
		closeHead.Operation = operationHead.Operation
		data, marshalErr := json.Marshal(closeHead)
		require.NoError(t, marshalErr)
		return bucket.Put(key, data)
	}))
	require.ErrorContains(t, store.Healthy(), "exactly one state")
}

func TestCloseIntentAdmissionIsExactIdempotentAndDeterministic(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	left := testCloseIntentSpec(t, "z-last")
	right := testCloseIntentSpec(t, "a-first")
	first, err := beginUnboundCloseIntent(t, store, left)
	require.NoError(t, err)
	refreshed, err := store.AdvanceCloseExecutionGeneration(first.Claim())
	require.NoError(t, err)
	retry, err := beginUnboundCloseIntent(t, store, left)
	require.NoError(t, err)
	assert.Equal(t, CloseIntentAdmissionExisting, retry.Disposition())
	assert.Equal(t, refreshed.IntentID(), retry.Claim().IntentID())
	assert.Equal(t, 1, retry.Claim().ExecutionGeneration().Number())

	conflicts := []closeIntentSpec{left, left, left, left}
	conflicts[0].RetainOnClose = !left.RetainOnClose
	conflicts[1].ActiveReleaseVersion++
	conflicts[2].ActiveReleaseDigest = sha256.Sum256([]byte("different-release"))
	conflicts[3].ResourceProfiles = CloneSKUResourceSnapshot(left.ResourceProfiles)
	conflicts[3].ResourceProfiles[0].MemoryMB++
	for _, conflict := range conflicts {
		_, err := beginUnboundCloseIntent(t, store, conflict)
		require.ErrorIs(t, err, ErrCloseIntentConflict)
	}
	_, err = beginUnboundCloseIntentWithLineage(
		t, store, left, "docker-b", testCloseIntentStorageID(t),
	)
	require.ErrorIs(t, err, ErrCloseIntentConflict)
	_, err = beginUnboundCloseIntent(t, store, right)
	require.NoError(t, err)
	claims, err := store.ListCloseIntents()
	require.NoError(t, err)
	require.Len(t, claims, 2)
	assert.Less(t, claims[0].LeaseUUID(), claims[1].LeaseUUID())

	got, found, err := store.GetCloseIntent(left.LeaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, refreshed.IntentID(), got.IntentID())
	_, found, err = store.GetCloseIntent(testLeaseUUID("absent-close"))
	require.NoError(t, err)
	assert.False(t, found)
}

func TestCloseIntentCallbacklessLegacyResolveOnlyDeletesIntent(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testCloseIntentSpec(t, "callbackless")
	spec.CallbackURL = ""
	spec.LifecycleCallbackURL = ""
	admission, err := beginUnboundCloseIntent(t, store, spec)
	require.NoError(t, err)
	entry, err := store.ResolveCloseIntent(
		admission.Claim(), backend.CallbackStatusDeprovisioned, "", false,
	)
	require.NoError(t, err)
	assert.Zero(t, entry)
	claims, err := store.ListCloseIntents()
	require.NoError(t, err)
	assert.Empty(t, claims)
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCloseIntentCleanupOnlyAndAbsentReleaseFenceSemantics(t *testing.T) {
	t.Run("cleanup-only wire form cannot carry principal authority", func(t *testing.T) {
		store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		spec := testCloseIntentSpec(t, "cleanup-only")
		spec.CleanupOnly = true
		spec.Tenant = ""
		spec.ProviderUUID = ""
		spec.CallbackURL = ""
		spec.LifecycleCallbackURL = ""
		spec.RetainOnClose = false
		admission, err := beginUnboundCloseIntent(t, store, spec)
		require.NoError(t, err)
		assert.True(t, admission.Claim().CleanupOnly())
		assert.Empty(t, admission.Claim().Tenant())
		assert.Empty(t, admission.Claim().ProviderUUID())

		withPrincipal := spec
		withPrincipal.LeaseUUID = testLeaseUUID("cleanup-principal")
		withPrincipal.Tenant = "tenant-a"
		withPrincipal.ProviderUUID = "22222222-2222-4222-8222-222222222222"
		_, err = beginUnboundCloseIntent(t, store, withPrincipal)
		require.ErrorContains(t, err, "cannot carry principal authority")

		partialPrincipal := spec
		partialPrincipal.LeaseUUID = testLeaseUUID("cleanup-partial-principal")
		partialPrincipal.Tenant = "tenant-a"
		_, err = beginUnboundCloseIntent(t, store, partialPrincipal)
		require.ErrorContains(t, err, "cannot carry principal authority")

		withCallback := spec
		withCallback.CallbackURL = testCloseIntentSpec(t, "cleanup-callback").CallbackURL
		withCallback.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(withCallback.CallbackURL, "")
		require.NoError(t, err)
		withCallback.LeaseUUID = testLeaseUUID("cleanup-callback")
		_, err = beginUnboundCloseIntent(t, store, withCallback)
		require.ErrorContains(t, err, "cleanup-only callback close intent cannot carry")

		withRetention := spec
		withRetention.LeaseUUID = testLeaseUUID("cleanup-retention")
		withRetention.RetainOnClose = true
		_, err = beginUnboundCloseIntent(t, store, withRetention)
		require.ErrorContains(t, err, "cleanup-only callback close intent cannot retain")

		// The encoded close form permits no release fence because the production
		// coordinator can atomically replace an exact pending operation. The
		// coordinator—not this low-level journal test shim—proves that source.
		withoutRelease := spec
		withoutRelease.LeaseUUID = testLeaseUUID("cleanup-no-release")
		withoutRelease.ActiveReleaseVersion = 0
		withoutRelease.ActiveReleaseDigest = [sha256.Size]byte{}
		withoutRelease.ActiveReleaseOperationID = OperationID{}
		_, err = beginUnboundCloseIntent(t, store, withoutRelease)
		require.NoError(t, err)
	})

	t.Run("failed pre-release close may have no release fence", func(t *testing.T) {
		store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		spec := testCloseIntentSpec(t, "no-release")
		spec.ActiveReleaseVersion = 0
		spec.ActiveReleaseDigest = [sha256.Size]byte{}
		spec.ActiveReleaseOperationID = OperationID{}
		admission, err := beginUnboundCloseIntent(t, store, spec)
		require.NoError(t, err)
		assert.Zero(t, admission.Claim().ActiveReleaseVersion())
		assert.Equal(t, [sha256.Size]byte{}, admission.Claim().ActiveReleaseDigest())
	})
}

func TestCloseIntentRejectsInvalidIdentityTopologyQuantityAndAuthority(t *testing.T) {
	base := testCloseIntentSpec(t, "validation")
	for _, test := range []struct {
		name        string
		backendName string
		storageID   backendidentity.ID
		want        string
	}{
		{
			name: "empty backend", backendName: " ",
			storageID: testCloseIntentStorageID(t), want: "backend name",
		},
		{
			name: "invalid storage", backendName: testCloseIntentBackend,
			storageID: backendidentity.ID{}, want: "storage identity",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := newCloseIntentCandidate(
				nil, base, test.backendName, test.storageID,
			)
			require.ErrorContains(t, err, test.want)
		})
	}
	tests := []struct {
		name   string
		mutate func(*closeIntentSpec)
		want   string
	}{
		{"noncanonical lease", func(s *closeIntentSpec) { s.LeaseUUID = "not-a-uuid" }, "canonical"},
		{"empty tenant", func(s *closeIntentSpec) { s.Tenant = "" }, "requires tenant"},
		{"empty provider", func(s *closeIntentSpec) { s.ProviderUUID = "" }, "requires provider"},
		{"no items", func(s *closeIntentSpec) { s.Items = nil }, "requires lease items"},
		{"no resource profiles", func(s *closeIntentSpec) { s.ResourceProfiles = nil }, "want exactly 1"},
		{"resource profile missing item SKU", func(s *closeIntentSpec) {
			s.ResourceProfiles[0].SKU = "large"
		}, "unreferenced SKU"},
		{"invalid resource profile", func(s *closeIntentSpec) {
			s.ResourceProfiles[0].MemoryMB = 0
		}, "memory_mb must be positive"},
		{"zero quantity", func(s *closeIntentSpec) { s.Items[0].Quantity = 0 }, "out of range"},
		{"unbounded quantity", func(s *closeIntentSpec) { s.Items[0].Quantity = math.MaxInt }, "out of range"},
		{"empty sku", func(s *closeIntentSpec) { s.Items[0].SKU = "" }, "requires item 0 SKU"},
		{"empty service", func(s *closeIntentSpec) { s.Items[0].ServiceName = "" }, "requires item 0 service name"},
		{"duplicate service", func(s *closeIntentSpec) {
			s.Items = append(s.Items, backend.LeaseItem{SKU: "medium", Quantity: 1, ServiceName: "app"})
		}, "duplicated"},
		{"manifest mismatch", func(s *closeIntentSpec) {
			s.Manifest = []byte(`{"services":{"worker":{"image":"example.invalid/worker:1"}}}`)
		}, "topology"},
		{"half callback pair", func(s *closeIntentSpec) { s.LifecycleCallbackURL = "" }, "both present or both empty"},
		{"different callback pair", func(s *closeIntentSpec) {
			s.LifecycleCallbackURL = strings.Replace(s.LifecycleCallbackURL, "fred.example", "other.example", 1)
		}, "invalid callback pair"},
		{"negative release version", func(s *closeIntentSpec) { s.ActiveReleaseVersion = -1 }, "cannot be negative"},
		{"version without release digest", func(s *closeIntentSpec) { s.ActiveReleaseDigest = [sha256.Size]byte{} }, "wholly absent or wholly present"},
		{"digest without release version", func(s *closeIntentSpec) { s.ActiveReleaseVersion = 0 }, "wholly absent or wholly present"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := base
			spec.Items = slices.Clone(base.Items)
			spec.ResourceProfiles = CloneSKUResourceSnapshot(base.ResourceProfiles)
			spec.Manifest = bytes.Clone(base.Manifest)
			test.mutate(&spec)
			_, err := beginUnboundCloseIntent(t, store, spec)
			require.ErrorContains(t, err, test.want)
			claims, listErr := store.ListCloseIntents()
			require.NoError(t, listErr)
			assert.Empty(t, claims)
		})
	}
}

func TestCloseIntentResolutionValidatesTerminalOutcome(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testCloseIntentSpec(t, "outcome")
	spec.RetainOnClose = false
	admission, err := beginUnboundCloseIntent(t, store, spec)
	require.NoError(t, err)
	for _, test := range []struct {
		status   backend.CallbackStatus
		retained bool
		want     string
	}{
		{backend.CallbackStatusSuccess, false, "invalid completion status"},
		{backend.CallbackStatusFailed, true, "invalid completion status"},
		{backend.CallbackStatusDeprovisioned, true, "cannot retain an unretained close"},
	} {
		_, err := store.ResolveCloseIntent(admission.Claim(), test.status, "error", test.retained)
		require.ErrorContains(t, err, test.want)
	}
	claims, err := store.ListCloseIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1, "invalid outcomes must preserve durable cleanup authority")
	entry, err := store.ResolveCloseIntent(
		claims[0], backend.CallbackStatusDeprovisioned, "", false,
	)
	require.NoError(t, err)
	assert.Equal(t, backend.CallbackStatusDeprovisioned, entry.Status)
	assert.Empty(t, entry.Error)
}

func TestCloseIntentCorruptionIsQuarantinedAndHealthFails(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*closeIntentEntry)
		want   string
	}{
		{"intent ID", func(entry *closeIntentEntry) { entry.IntentID = uuid.NewSHA1(uuid.Nil, []byte("v5")).String() }, "UUIDv4"},
		{"storage identity", func(entry *closeIntentEntry) { entry.BackendStorageID = uuid.NewSHA1(uuid.Nil, []byte("v5")).String() }, "storage identity"},
		{"release digest", func(entry *closeIntentEntry) { entry.ActiveReleaseDigest = "ABC" }, "canonical SHA-256"},
		{"negative generation", func(entry *closeIntentEntry) { entry.ExecutionGeneration = -1 }, "cannot be negative"},
		{"unbounded quantity", func(entry *closeIntentEntry) { entry.Items[0].Quantity = math.MaxInt }, "out of range"},
		{"missing resource profiles", func(entry *closeIntentEntry) { entry.ResourceProfiles = nil }, "want exactly 1"},
		{"resource profile mismatch", func(entry *closeIntentEntry) { entry.ResourceProfiles[0].SKU = "other" }, "unreferenced SKU"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := testCloseIntentSpec(t, "corrupt-"+test.name)
			_, err := beginUnboundCloseIntent(t, store, spec)
			require.NoError(t, err)

			var corrupt []byte
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
				key := []byte(spec.LeaseUUID)
				var head storedLeaseMutationHead
				require.NoError(t, json.Unmarshal(bucket.Get(key), &head))
				test.mutate(head.Close)
				corrupt, err = json.Marshal(head)
				require.NoError(t, err)
				return bucket.Put(key, corrupt)
			}))

			_, err = store.ListCloseIntents()
			require.ErrorContains(t, err, test.want)
			require.ErrorContains(t, store.Healthy(), test.want)
			require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
				assert.Equal(t, corrupt, tx.Bucket(callbackLeaseMutationHeadBucketName).Get([]byte(spec.LeaseUUID)))
				return nil
			}))
		})
	}
}

func TestCloseIntentRejectsDuplicateNestedJSONFields(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testCloseIntentSpec(t, "duplicate-json")
	_, err := beginUnboundCloseIntent(t, store, spec)
	require.NoError(t, err)
	var corrupt []byte
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
		key := []byte(spec.LeaseUUID)
		raw := bucket.Get(key)
		corrupt = bytes.Replace(raw, []byte(`"items":[{"sku":"small"`),
			[]byte(`"items":[{"sku":"small","sku":"large"`), 1)
		require.NotEqual(t, raw, corrupt)
		return bucket.Put(key, corrupt)
	}))
	_, err = store.ListCloseIntents()
	require.ErrorContains(t, err, `duplicate field "sku"`)
	require.ErrorContains(t, store.Healthy(), `duplicate field "sku"`)
}

func TestCloseIntentsNeverExpireAndReadOnlyInspectionCountsThem(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store := newCloseIntentTestStore(t, dbPath)
	spec := testCloseIntentSpec(t, "never-expire")
	_, err := beginUnboundCloseIntent(t, store, spec)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
		key := []byte(spec.LeaseUUID)
		var head storedLeaseMutationHead
		require.NoError(t, json.Unmarshal(bucket.Get(key), &head))
		head.Close.CreatedAt = time.Unix(1, 0).UTC()
		data, marshalErr := json.Marshal(head)
		require.NoError(t, marshalErr)
		return bucket.Put(key, data)
	}))
	removed, err := store.removeOlderThan(time.Nanosecond)
	require.NoError(t, err)
	assert.Zero(t, removed)
	claims, err := store.ListCloseIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	require.NoError(t, store.Close())

	inspection, err := InspectCallbackStoreReadOnly(dbPath)
	require.NoError(t, err)
	assert.True(t, inspection.UpgradedSchema)
	assert.Equal(t, 1, inspection.Pending)
}

func TestCloseIntentNestedBucketCorruptionFailsHealth(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	leaseUUID := testLeaseUUID("nested-close")
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.Bucket(callbackLeaseMutationHeadBucketName).CreateBucket([]byte(leaseUUID))
		return err
	}))
	_, err := store.ListCloseIntents()
	require.ErrorContains(t, err, "nested bucket")
	require.ErrorContains(t, store.Healthy(), "nested bucket")
}
