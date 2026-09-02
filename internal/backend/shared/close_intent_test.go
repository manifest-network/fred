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

func testCloseIntentSpec(t *testing.T, name string) CloseIntentSpec {
	t.Helper()
	operationURL := "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(operationURL, "")
	require.NoError(t, err)
	return CloseIntentSpec{
		LeaseUUID:            testLeaseUUID("close-" + name),
		Backend:              "docker-a",
		BackendStorageID:     callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
		Tenant:               "tenant-a",
		ProviderUUID:         "22222222-2222-4222-8222-222222222222",
		Items:                []backend.LeaseItem{{SKU: "small", Quantity: 2, ServiceName: "app"}},
		ResourceProfiles:     []SKUResourceSnapshot{{SKU: "small", CPUCores: 0.5, MemoryMB: 512, DiskMB: 1024}},
		Manifest:             []byte(`{"services":{"app":{"image":"example.invalid/app:1"}}}`),
		CallbackURL:          operationURL,
		LifecycleCallbackURL: lifecycleURL,
		RetainOnClose:        true,
		ActiveReleaseVersion: 7,
		ActiveReleaseDigest:  sha256.Sum256([]byte("active-release-" + name)),
		LegacyRollbackTargets: []CloseLegacyRollbackTarget{{
			ContainerID: "sha256:immutable-container-a",
			Name:        "fred-" + testLeaseUUID("close-"+name) + "-app-0-prev",
		}},
	}
}

func newCloseIntentTestStore(t *testing.T, dbPath string) *CallbackStore {
	t.Helper()
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	return store
}

func TestCloseIntentSurvivesRestartAndResolvesExactlyOnce(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store := newCloseIntentTestStore(t, dbPath)
	spec := testCloseIntentSpec(t, "restart")
	admission, err := store.BeginCloseIntent(spec)
	require.NoError(t, err)
	require.Equal(t, CloseIntentAdmissionCreated, admission.Disposition)
	require.NotEmpty(t, admission.Claim.IntentID())
	require.Zero(t, admission.Claim.CleanupAttempts())

	// Neither caller-owned input nor accessor output can mutate the claim.
	spec.Items[0].SKU = "mutated"
	spec.ResourceProfiles[0].MemoryMB = 1
	spec.Manifest[0] = 'x'
	spec.LegacyRollbackTargets[0].ContainerID = "mutated"
	items := admission.Claim.Items()
	profiles := admission.Claim.ResourceProfiles()
	manifestBytes := admission.Claim.Manifest()
	targets := admission.Claim.LegacyRollbackTargets()
	items[0].SKU = "also-mutated"
	profiles[0].MemoryMB = 2
	manifestBytes[0] = 'x'
	targets[0].ContainerID = "also-mutated"
	assert.Equal(t, "small", admission.Claim.Items()[0].SKU)
	assert.EqualValues(t, 512, admission.Claim.ResourceProfiles()[0].MemoryMB)
	assert.Equal(t, byte('{'), admission.Claim.Manifest()[0])
	assert.Equal(t, "sha256:immutable-container-a", admission.Claim.LegacyRollbackTargets()[0].ContainerID)

	require.NoError(t, store.Close())
	store = newCloseIntentTestStore(t, dbPath)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	claims, err := store.ListCloseIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	assert.Equal(t, admission.Claim.IntentID(), claims[0].IntentID())

	staleCopy := claims[0]
	refreshed, err := store.IncrementCloseCleanupAttempts(claims[0])
	require.NoError(t, err)
	assert.Equal(t, 1, refreshed.CleanupAttempts())
	_, err = store.ResolveCloseIntent(staleCopy, backend.CallbackStatusDeprovisioned, "", true)
	require.ErrorContains(t, err, "changed before precise mutation")

	require.NoError(t, store.Close())
	store = newCloseIntentTestStore(t, dbPath)
	claims, err = store.ListCloseIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	assert.Equal(t, 1, claims[0].CleanupAttempts(), "restart must not reset the cleanup retry budget")

	copyForReplay := claims[0]
	entry, err := store.ResolveCloseIntent(
		claims[0], backend.CallbackStatusDeprovisioned, "", true,
	)
	require.NoError(t, err)
	assert.Equal(t, CallbackDeliveryKindLifecycle, entry.DeliveryKind)
	assert.Equal(t, spec.LifecycleCallbackURL, entry.CallbackURL)
	assert.True(t, entry.Retained)
	_, err = store.ResolveCloseIntent(copyForReplay, backend.CallbackStatusDeprovisioned, "", true)
	require.ErrorContains(t, err, "no longer exists")

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
	_, err := store.BeginCloseIntent(spec)
	require.NoError(t, err)
	futureCreatedAt := time.Now().Add(24 * time.Hour)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackCloseIntentBucketName)
		key := []byte(spec.LeaseUUID)
		var entry closeIntentEntry
		if unmarshalErr := json.Unmarshal(bucket.Get(key), &entry); unmarshalErr != nil {
			return unmarshalErr
		}
		entry.CreatedAt = futureCreatedAt
		data, marshalErr := json.Marshal(entry)
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
	wake := make(chan struct{}, 1)
	unsubscribe := store.subscribeReplayWake(wake)
	t.Cleanup(unsubscribe)
	closeSpec := testCloseIntentSpec(t, "preempt")
	operationSpec := OperationIntentSpec{
		Kind:                 OperationIntentProvision,
		LeaseUUID:            closeSpec.LeaseUUID,
		CallbackURL:          closeSpec.CallbackURL,
		LifecycleCallbackURL: closeSpec.LifecycleCallbackURL,
		Backend:              closeSpec.Backend,
		BackendStorageID:     closeSpec.BackendStorageID,
		Tenant:               closeSpec.Tenant,
		ProviderUUID:         closeSpec.ProviderUUID,
		Items:                slices.Clone(closeSpec.Items),
		ResourceProfiles:     CloneSKUResourceSnapshot(closeSpec.ResourceProfiles),
		Manifest:             bytes.Clone(closeSpec.Manifest),
	}
	operation, err := store.BeginOperationIntent(operationSpec)
	require.NoError(t, err)
	require.Equal(t, OperationIntentAdmissionCreated, operation.Disposition)
	select {
	case <-wake:
		t.Fatal("an intent-only write must not wake callback replay")
	default:
	}

	closeAdmission, err := store.BeginCloseIntent(closeSpec)
	require.NoError(t, err)
	assert.True(t, closeAdmission.OperationPreempted)
	select {
	case <-wake:
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
	assert.NotEmpty(t, operation.Claim.OperationID(), "fixture must exercise typed operation authority")

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
	operationSpec.Backend = closeSpec.Backend
	operationSpec.BackendStorageID = closeSpec.BackendStorageID
	operationSpec.Tenant = closeSpec.Tenant
	operationSpec.ProviderUUID = closeSpec.ProviderUUID
	operationSpec.Items = slices.Clone(closeSpec.Items)
	operationSpec.Manifest = bytes.Clone(closeSpec.Manifest)
	_, err := store.BeginOperationIntent(operationSpec)
	require.NoError(t, err)

	// A corrupt non-bucket v2 lease key makes callback publication fail after
	// BeginCloseIntent has tentatively put the close row in the same transaction.
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackV2BucketName).Put([]byte(closeSpec.LeaseUUID), []byte("poison"))
	}))
	_, err = store.BeginCloseIntent(closeSpec)
	require.ErrorContains(t, err, "is not a nested bucket")

	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		assert.Nil(t, tx.Bucket(callbackCloseIntentBucketName).Get([]byte(closeSpec.LeaseUUID)),
			"failed operation callback publication must roll back the close row")
		assert.NotNil(t, tx.Bucket(callbackOperationIntentBucketName).Get([]byte(closeSpec.LeaseUUID)),
			"failed close admission must preserve the operation intent")
		return nil
	}))
}

func TestResolveCloseIntentRollsBackRemovalWhenCallbackEnqueueFails(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testCloseIntentSpec(t, "resolve-rollback")
	admission, err := store.BeginCloseIntent(spec)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackV2BucketName).Put([]byte(spec.LeaseUUID), []byte("poison"))
	}))

	_, err = store.ResolveCloseIntent(
		admission.Claim, backend.CallbackStatusFailed, "cleanup failed", false,
	)
	require.ErrorContains(t, err, "is not a nested bucket")
	current, found, getErr := store.GetCloseIntent(spec.LeaseUUID)
	require.NoError(t, getErr)
	require.True(t, found, "failed callback enqueue must roll back close-intent removal")
	assert.Equal(t, admission.Claim.IntentID(), current.IntentID())
}

func TestBeginCloseIntentRejectsMismatchedOperationStorageAuthority(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	closeSpec := testCloseIntentSpec(t, "storage-mismatch")
	operationSpec := testOperationIntentSpec(t, "storage-mismatch")
	operationSpec.LeaseUUID = closeSpec.LeaseUUID
	operationSpec.CallbackURL = closeSpec.CallbackURL
	operationSpec.LifecycleCallbackURL = closeSpec.LifecycleCallbackURL
	operationSpec.Backend = closeSpec.Backend
	operationSpec.BackendStorageID = callbackStorageID(t, "6ba7b810-9dad-41d1-80b4-00c04fd430c8")
	_, err := store.BeginOperationIntent(operationSpec)
	require.NoError(t, err)

	_, err = store.BeginCloseIntent(closeSpec)
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
	closeAdmission, err := store.BeginCloseIntent(closeSpec)
	require.NoError(t, err)
	operationSpec := testOperationIntentSpec(t, "blocked-operation")
	operationSpec.LeaseUUID = closeSpec.LeaseUUID
	operationSpec.CallbackURL = closeSpec.CallbackURL
	operationSpec.LifecycleCallbackURL = closeSpec.LifecycleCallbackURL
	operationSpec.Backend = closeSpec.Backend
	operationSpec.BackendStorageID = closeSpec.BackendStorageID
	operationSpec.Tenant = closeSpec.Tenant
	operationSpec.ProviderUUID = closeSpec.ProviderUUID
	operationSpec.Items = slices.Clone(closeSpec.Items)
	operationSpec.Manifest = bytes.Clone(closeSpec.Manifest)

	_, err = store.BeginOperationIntent(operationSpec)
	require.ErrorIs(t, err, ErrOperationIntentConflict)
	_, err = store.ProbeOperationIntent(OperationIntentProbe{
		LeaseUUID:        operationSpec.LeaseUUID,
		CallbackURL:      operationSpec.CallbackURL,
		Backend:          operationSpec.Backend,
		BackendStorageID: operationSpec.BackendStorageID,
	})
	require.ErrorIs(t, err, ErrOperationIntentConflict)
	lateCompletion := CallbackEntry{
		LeaseUUID:        operationSpec.LeaseUUID,
		CallbackURL:      operationSpec.CallbackURL,
		DeliveryKind:     CallbackDeliveryKindOperation,
		Success:          true,
		Status:           backend.CallbackStatusSuccess,
		Backend:          operationSpec.Backend,
		BackendStorageID: operationSpec.BackendStorageID.String(),
		CreatedAt:        time.Now(),
	}
	_, err = store.StoreEntry(lateCompletion)
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
	assert.Equal(t, closeAdmission.Claim.IntentID(), current.IntentID())
}

func TestCallbackStoreHealthRejectsSimultaneousOperationAndCloseIntents(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	closeSpec := testCloseIntentSpec(t, "journal-overlap")
	operationSpec := testOperationIntentSpec(t, "journal-overlap")
	operationSpec.LeaseUUID = closeSpec.LeaseUUID
	operationSpec.CallbackURL = closeSpec.CallbackURL
	operationSpec.LifecycleCallbackURL = closeSpec.LifecycleCallbackURL
	operationSpec.Backend = closeSpec.Backend
	operationSpec.BackendStorageID = closeSpec.BackendStorageID
	operationSpec.Tenant = closeSpec.Tenant
	operationSpec.ProviderUUID = closeSpec.ProviderUUID
	operationSpec.Items = slices.Clone(closeSpec.Items)
	operationSpec.Manifest = bytes.Clone(closeSpec.Manifest)
	_, err := store.BeginOperationIntent(operationSpec)
	require.NoError(t, err)
	var operationBytes []byte
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		operationBytes = bytes.Clone(
			tx.Bucket(callbackOperationIntentBucketName).Get([]byte(closeSpec.LeaseUUID)),
		)
		return nil
	}))
	_, err = store.BeginCloseIntent(closeSpec)
	require.NoError(t, err)

	// Reintroduce the preempted row as if on-disk corruption violated the
	// transaction invariant. Each row is valid alone; health must still reject
	// the impossible overlap so startup cannot choose one authority silently.
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackOperationIntentBucketName).Put(
			[]byte(closeSpec.LeaseUUID), operationBytes,
		)
	}))
	require.ErrorContains(t, store.Healthy(), "simultaneous operation and close intents")
}

func TestCloseIntentAdmissionIsExactIdempotentAndDeterministic(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	left := testCloseIntentSpec(t, "z-last")
	right := testCloseIntentSpec(t, "a-first")
	first, err := store.BeginCloseIntent(left)
	require.NoError(t, err)
	refreshed, err := store.IncrementCloseCleanupAttempts(first.Claim)
	require.NoError(t, err)
	retry, err := store.BeginCloseIntent(left)
	require.NoError(t, err)
	assert.Equal(t, CloseIntentAdmissionExisting, retry.Disposition)
	assert.Equal(t, refreshed.IntentID(), retry.Claim.IntentID())
	assert.Equal(t, 1, retry.Claim.CleanupAttempts())

	conflicts := []CloseIntentSpec{left, left, left, left, left}
	conflicts[0].RetainOnClose = !left.RetainOnClose
	conflicts[1].ActiveReleaseVersion++
	conflicts[2].ActiveReleaseDigest = sha256.Sum256([]byte("different-release"))
	conflicts[3].Backend = "docker-b"
	conflicts[4].ResourceProfiles = CloneSKUResourceSnapshot(left.ResourceProfiles)
	conflicts[4].ResourceProfiles[0].MemoryMB++
	for _, conflict := range conflicts {
		_, err := store.BeginCloseIntent(conflict)
		require.ErrorIs(t, err, ErrCloseIntentConflict)
	}
	_, err = store.BeginCloseIntent(right)
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
	admission, err := store.BeginCloseIntent(spec)
	require.NoError(t, err)
	entry, err := store.ResolveCloseIntent(
		admission.Claim, backend.CallbackStatusDeprovisioned, "", false,
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
	t.Run("cleanup only requires no tenant callback or retention authority", func(t *testing.T) {
		store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		spec := testCloseIntentSpec(t, "cleanup-only")
		spec.CleanupOnly = true
		spec.Tenant = ""
		spec.ProviderUUID = ""
		spec.CallbackURL = ""
		spec.LifecycleCallbackURL = ""
		spec.RetainOnClose = false
		admission, err := store.BeginCloseIntent(spec)
		require.NoError(t, err)
		assert.True(t, admission.Claim.CleanupOnly())
		assert.Empty(t, admission.Claim.Tenant())
		assert.Empty(t, admission.Claim.ProviderUUID())

		withCallback := spec
		withCallback.CallbackURL = testCloseIntentSpec(t, "cleanup-callback").CallbackURL
		withCallback.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(withCallback.CallbackURL, "")
		require.NoError(t, err)
		withCallback.LeaseUUID = testLeaseUUID("cleanup-callback")
		_, err = store.BeginCloseIntent(withCallback)
		require.ErrorContains(t, err, "cleanup-only callback close intent cannot carry")

		withRetention := spec
		withRetention.LeaseUUID = testLeaseUUID("cleanup-retention")
		withRetention.RetainOnClose = true
		_, err = store.BeginCloseIntent(withRetention)
		require.ErrorContains(t, err, "cleanup-only callback close intent cannot retain")

		withoutRelease := spec
		withoutRelease.LeaseUUID = testLeaseUUID("cleanup-no-release")
		withoutRelease.ActiveReleaseVersion = 0
		withoutRelease.ActiveReleaseDigest = [sha256.Size]byte{}
		withoutRelease.LegacyRollbackTargets = nil
		_, err = store.BeginCloseIntent(withoutRelease)
		require.ErrorContains(t, err, "cleanup-only callback close intent requires an active release fence")
	})

	t.Run("failed pre-release close may have no release fence", func(t *testing.T) {
		store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		spec := testCloseIntentSpec(t, "no-release")
		spec.ActiveReleaseVersion = 0
		spec.ActiveReleaseDigest = [sha256.Size]byte{}
		spec.LegacyRollbackTargets = nil
		admission, err := store.BeginCloseIntent(spec)
		require.NoError(t, err)
		assert.Zero(t, admission.Claim.ActiveReleaseVersion())
		assert.Equal(t, [sha256.Size]byte{}, admission.Claim.ActiveReleaseDigest())

		withRollback := spec
		withRollback.LeaseUUID = testLeaseUUID("no-release-with-rollback")
		withRollback.LegacyRollbackTargets = []CloseLegacyRollbackTarget{{
			ContainerID: "unexpected-container", Name: "unexpected-prev",
		}}
		_, err = store.BeginCloseIntent(withRollback)
		require.ErrorContains(t, err, "rollback targets require an active release fence")
	})

	t.Run("historical rollback targets are independently bounded", func(t *testing.T) {
		store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		spec := testCloseIntentSpec(t, "scaled-down")
		spec.Items[0].Quantity = 1
		spec.LegacyRollbackTargets = append(spec.LegacyRollbackTargets, CloseLegacyRollbackTarget{
			ContainerID: "sha256:historical-container-b",
			Name:        "fred-historical-app-1-prev",
		})
		_, err := store.BeginCloseIntent(spec)
		require.NoError(t, err)
	})
}

func TestCloseIntentRejectsInvalidIdentityTopologyQuantityAndAuthority(t *testing.T) {
	base := testCloseIntentSpec(t, "validation")
	tests := []struct {
		name   string
		mutate func(*CloseIntentSpec)
		want   string
	}{
		{"noncanonical lease", func(s *CloseIntentSpec) { s.LeaseUUID = "not-a-uuid" }, "canonical"},
		{"invalid storage", func(s *CloseIntentSpec) { s.BackendStorageID = backendidentity.ID{} }, "storage identity"},
		{"empty backend", func(s *CloseIntentSpec) { s.Backend = " " }, "backend name"},
		{"empty tenant", func(s *CloseIntentSpec) { s.Tenant = "" }, "requires tenant"},
		{"empty provider", func(s *CloseIntentSpec) { s.ProviderUUID = "" }, "requires provider"},
		{"no items", func(s *CloseIntentSpec) { s.Items = nil }, "requires lease items"},
		{"no resource profiles", func(s *CloseIntentSpec) { s.ResourceProfiles = nil }, "want exactly 1"},
		{"resource profile missing item SKU", func(s *CloseIntentSpec) {
			s.ResourceProfiles[0].SKU = "large"
		}, "unreferenced SKU"},
		{"invalid resource profile", func(s *CloseIntentSpec) {
			s.ResourceProfiles[0].MemoryMB = 0
		}, "memory_mb must be positive"},
		{"zero quantity", func(s *CloseIntentSpec) { s.Items[0].Quantity = 0 }, "out of range"},
		{"unbounded quantity", func(s *CloseIntentSpec) { s.Items[0].Quantity = math.MaxInt }, "out of range"},
		{"empty sku", func(s *CloseIntentSpec) { s.Items[0].SKU = "" }, "requires item 0 SKU"},
		{"empty service", func(s *CloseIntentSpec) { s.Items[0].ServiceName = "" }, "requires item 0 service name"},
		{"duplicate service", func(s *CloseIntentSpec) {
			s.Items = append(s.Items, backend.LeaseItem{SKU: "medium", Quantity: 1, ServiceName: "app"})
		}, "duplicated"},
		{"manifest mismatch", func(s *CloseIntentSpec) {
			s.Manifest = []byte(`{"services":{"worker":{"image":"example.invalid/worker:1"}}}`)
		}, "topology"},
		{"half callback pair", func(s *CloseIntentSpec) { s.LifecycleCallbackURL = "" }, "both present or both empty"},
		{"different callback pair", func(s *CloseIntentSpec) {
			s.LifecycleCallbackURL = strings.Replace(s.LifecycleCallbackURL, "fred.example", "other.example", 1)
		}, "invalid callback pair"},
		{"negative release version", func(s *CloseIntentSpec) { s.ActiveReleaseVersion = -1 }, "cannot be negative"},
		{"version without release digest", func(s *CloseIntentSpec) { s.ActiveReleaseDigest = [sha256.Size]byte{} }, "wholly absent or wholly present"},
		{"digest without release version", func(s *CloseIntentSpec) { s.ActiveReleaseVersion = 0 }, "wholly absent or wholly present"},
		{"missing rollback ID", func(s *CloseIntentSpec) { s.LegacyRollbackTargets[0].ContainerID = "" }, "requires container ID"},
		{"missing rollback name", func(s *CloseIntentSpec) { s.LegacyRollbackTargets[0].Name = "" }, "requires name"},
		{"duplicate rollback ID", func(s *CloseIntentSpec) {
			s.LegacyRollbackTargets = append(s.LegacyRollbackTargets, CloseLegacyRollbackTarget{
				ContainerID: s.LegacyRollbackTargets[0].ContainerID, Name: "other-prev",
			})
		}, "container ID"},
		{"duplicate rollback name", func(s *CloseIntentSpec) {
			s.LegacyRollbackTargets = append(s.LegacyRollbackTargets, CloseLegacyRollbackTarget{
				ContainerID: "other-container", Name: s.LegacyRollbackTargets[0].Name,
			})
		}, "rollback name"},
		{"too many rollback targets", func(s *CloseIntentSpec) {
			s.LegacyRollbackTargets = make([]CloseLegacyRollbackTarget, backend.MaxOperationQuantity+1)
			for i := range s.LegacyRollbackTargets {
				s.LegacyRollbackTargets[i] = CloseLegacyRollbackTarget{
					ContainerID: "container-" + strings.Repeat("x", i%7) + uuid.NewString(),
					Name:        "rollback-" + uuid.NewString(),
				}
			}
		}, "rollback targets; maximum"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := base
			spec.Items = slices.Clone(base.Items)
			spec.ResourceProfiles = CloneSKUResourceSnapshot(base.ResourceProfiles)
			spec.Manifest = bytes.Clone(base.Manifest)
			spec.LegacyRollbackTargets = slices.Clone(base.LegacyRollbackTargets)
			test.mutate(&spec)
			_, err := store.BeginCloseIntent(spec)
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
	admission, err := store.BeginCloseIntent(spec)
	require.NoError(t, err)
	for _, test := range []struct {
		status   backend.CallbackStatus
		retained bool
		want     string
	}{
		{backend.CallbackStatusSuccess, false, "invalid completion status"},
		{backend.CallbackStatusFailed, true, "failed close callback cannot be retained"},
		{backend.CallbackStatusDeprovisioned, true, "cannot retain an unretained close"},
	} {
		_, err := store.ResolveCloseIntent(admission.Claim, test.status, "error", test.retained)
		require.ErrorContains(t, err, test.want)
	}
	claims, err := store.ListCloseIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1, "invalid outcomes must preserve durable cleanup authority")
	entry, err := store.ResolveCloseIntent(
		claims[0], backend.CallbackStatusFailed, "cleanup failed", false,
	)
	require.NoError(t, err)
	assert.Equal(t, backend.CallbackStatusFailed, entry.Status)
	assert.Equal(t, "cleanup failed", entry.Error)
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
		{"negative attempts", func(entry *closeIntentEntry) { entry.CleanupAttempts = -1 }, "cannot be negative"},
		{"unbounded quantity", func(entry *closeIntentEntry) { entry.Items[0].Quantity = math.MaxInt }, "out of range"},
		{"missing resource profiles", func(entry *closeIntentEntry) { entry.ResourceProfiles = nil }, "want exactly 1"},
		{"resource profile mismatch", func(entry *closeIntentEntry) { entry.ResourceProfiles[0].SKU = "other" }, "unreferenced SKU"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := testCloseIntentSpec(t, "corrupt-"+test.name)
			_, err := store.BeginCloseIntent(spec)
			require.NoError(t, err)

			var corrupt []byte
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(callbackCloseIntentBucketName)
				key := []byte(spec.LeaseUUID)
				var entry closeIntentEntry
				require.NoError(t, json.Unmarshal(bucket.Get(key), &entry))
				test.mutate(&entry)
				corrupt, err = json.Marshal(entry)
				require.NoError(t, err)
				return bucket.Put(key, corrupt)
			}))

			_, err = store.ListCloseIntents()
			require.ErrorContains(t, err, test.want)
			require.ErrorContains(t, store.Healthy(), test.want)
			require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
				assert.Equal(t, corrupt, tx.Bucket(callbackCloseIntentBucketName).Get([]byte(spec.LeaseUUID)))
				return nil
			}))
		})
	}
}

func TestCloseIntentRejectsDuplicateNestedJSONFields(t *testing.T) {
	store := newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testCloseIntentSpec(t, "duplicate-json")
	_, err := store.BeginCloseIntent(spec)
	require.NoError(t, err)
	var corrupt []byte
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackCloseIntentBucketName)
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
	_, err := store.BeginCloseIntent(spec)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackCloseIntentBucketName)
		key := []byte(spec.LeaseUUID)
		var entry closeIntentEntry
		require.NoError(t, json.Unmarshal(bucket.Get(key), &entry))
		entry.CreatedAt = time.Unix(1, 0).UTC()
		data, marshalErr := json.Marshal(entry)
		require.NoError(t, marshalErr)
		return bucket.Put(key, data)
	}))
	removed, err := store.RemoveOlderThan(time.Nanosecond)
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
		_, err := tx.Bucket(callbackCloseIntentBucketName).CreateBucket([]byte(leaseUUID))
		return err
	}))
	_, err := store.ListCloseIntents()
	require.ErrorContains(t, err, "nested bucket")
	require.ErrorContains(t, store.Healthy(), "nested bucket")
}
