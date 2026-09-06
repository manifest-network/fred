package shared

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/maintenanceid"
)

func maintenanceFixture(
	t *testing.T,
	name string,
) (*ReleaseStore, *CallbackStore, ReleaseClaim, Release) {
	t.Helper()
	dir := t.TempDir()
	releases, err := newUnboundReleaseStoreForTest(ReleaseStoreConfig{DBPath: filepath.Join(dir, "releases.db")})
	require.NoError(t, err)
	callbacks, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(dir, "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})
	leaseUUID := testLeaseUUID("maintenance-" + name)
	source := validRuntimeAuthorityRelease()
	require.NoError(t, releases.appendActive(leaseUUID, source))
	active, sourceClaim, err := releases.claimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	return releases, callbacks, sourceClaim, target
}

func beginMaintenanceFixture(
	t *testing.T,
	name string,
) (*ReleaseStore, *CallbackStore, MaintenanceIntentClaim, MaintenanceAppendClaim) {
	t.Helper()
	releases, callbacks, source, target := maintenanceFixture(t, name)
	spec := newMaintenanceIntentSpec(
		t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target,
		"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	)
	admission, err := callbacks.BeginMaintenanceIntent(spec)
	require.NoError(t, err)
	appendClaim, err := callbacks.StartMaintenanceAppend(
		createdMaintenanceDispatch(t, admission),
	)
	require.NoError(t, err)
	return releases, callbacks, appendClaim.Intent(), appendClaim
}

func maintenanceSpecFromLatestActive(
	t *testing.T,
	callbacks *CallbackStore,
	releases *ReleaseStore,
	leaseUUID string,
	kind MaintenanceIntentKind,
) MaintenanceIntentCandidate {
	t.Helper()
	active, source, err := releases.claimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.MaintenanceID = MaintenanceID{}
	target.CreatedAt = time.Now()
	return newMaintenanceIntentSpec(
		t, callbacks, newTestMaintenanceID(t), kind, source, target, "docker-a",
		callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	)
}

func newTestMaintenanceID(t *testing.T) MaintenanceID {
	t.Helper()
	id, err := maintenanceid.New()
	require.NoError(t, err)
	return id
}

func newMaintenanceIntentSpec(
	t *testing.T,
	callbacks *CallbackStore,
	id MaintenanceID,
	kind MaintenanceIntentKind,
	source ReleaseClaim,
	target Release,
	backendName string,
	storageID backendidentity.ID,
) MaintenanceIntentCandidate {
	t.Helper()
	identity, ok := target.RuntimeIdentity()
	require.True(t, ok)
	payload := []byte(nil)
	if kind != MaintenanceIntentRestart {
		payload = target.Manifest
	}
	request, err := newMaintenanceRequestAuthority(
		callbacks, id, kind, source.LeaseUUID(), identity.LifecycleCallbackURL(), payload,
		backendName, storageID,
	)
	require.NoError(t, err)
	candidate, err := callbacks.NewMaintenanceIntentCandidate(request, source, target)
	require.NoError(t, err)
	return candidate
}

func createdMaintenanceDispatch(
	t *testing.T,
	admission MaintenanceIntentAdmission,
) MaintenanceIntentDispatch {
	t.Helper()
	dispatch, ok := admission.CreatedDispatch()
	require.True(t, ok, "maintenance admission must carry first-dispatch authority")
	return dispatch
}

func reissueMaintenanceCandidate(
	t *testing.T,
	callbacks *CallbackStore,
	candidate MaintenanceIntentCandidate,
) MaintenanceIntentCandidate {
	t.Helper()
	payload := []byte(nil)
	if candidate.request.Kind() != MaintenanceIntentRestart {
		payload = candidate.targetRelease.Manifest
	}
	request, err := newMaintenanceRequestAuthority(
		callbacks,
		candidate.request.MaintenanceID(),
		candidate.request.Kind(),
		candidate.request.LeaseUUID(),
		candidate.request.CallbackURL(),
		payload,
		candidate.request.Backend(),
		candidate.request.BackendStorageID(),
	)
	require.NoError(t, err)
	reissued, err := callbacks.NewMaintenanceIntentCandidate(
		request, candidate.sourceRelease, candidate.targetRelease,
	)
	require.NoError(t, err)
	return reissued
}

func TestMaintenanceIntentCannotSplitReplayAndDeliveryAuthorities(t *testing.T) {
	_, callbacks, source, target := maintenanceFixture(t, "split-callback-authority")
	differentOperationURL := "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	differentLifecycleURL, err := backend.ResolveLifecycleCallbackURL(differentOperationURL, "")
	require.NoError(t, err)
	request, err := newMaintenanceRequestAuthority(
		callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source.LeaseUUID(),
		differentLifecycleURL, nil, "docker-a",
		callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	)
	require.NoError(t, err)

	_, err = callbacks.NewMaintenanceIntentCandidate(request, source, target)
	require.ErrorContains(t, err, "callback differs")
	intents, listErr := callbacks.listMaintenanceIntents()
	require.NoError(t, listErr)
	assert.Empty(t, intents, "a split replay/delivery identity must not publish a durable head")
}

func TestMaintenanceRequestAndDispatchCapabilitiesAreExactStoreBound(t *testing.T) {
	_, _, source, target := maintenanceFixture(t, "exact-store-capabilities")

	pathA, storageA := initializeBoundCallbackStore(t)
	storeA, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: pathA}, storageA, newTestStorageAuthorityGate(t),
	)
	require.NoError(t, err)
	pathB, storageB := initializeBoundCallbackStore(t)
	storeB, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: pathB}, storageB, newTestStorageAuthorityGate(t),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, storeB.Close()) })

	request, err := storeA.NewMaintenanceRequestAuthority(
		newTestMaintenanceID(t), MaintenanceIntentRestart, source.LeaseUUID(),
		target.RuntimeAuthority.LifecycleCallbackURL(), nil,
	)
	require.NoError(t, err)
	require.True(t, request.Valid())
	assert.Equal(t, storageA.ID(), request.BackendStorageID())

	assert.False(t, (MaintenanceRequestAuthority{}).Valid())
	assert.False(t, (MaintenanceIntentDispatch{}).Valid())
	_, err = storeA.NewMaintenanceIntentCandidate(MaintenanceRequestAuthority{}, source, target)
	require.ErrorContains(t, err, "not minted by this journal pair")
	_, err = storeA.ProbeMaintenanceIntent(MaintenanceRequestAuthority{})
	require.ErrorContains(t, err, "not minted by this callback journal")
	_, err = storeA.StartMaintenanceAppend(MaintenanceIntentDispatch{})
	require.ErrorContains(t, err, "not minted by this journal pair")
	require.ErrorContains(t, storeA.CancelMaintenanceIntent(MaintenanceIntentDispatch{}),
		"not minted by this journal pair")

	_, err = storeB.NewMaintenanceIntentCandidate(request, source, target)
	require.ErrorContains(t, err, "not minted by this journal pair")
	_, err = storeB.ProbeMaintenanceIntent(request)
	require.ErrorContains(t, err, "not minted by this callback journal")

	candidate, err := storeA.NewMaintenanceIntentCandidate(request, source, target)
	require.NoError(t, err)
	admission, err := storeA.BeginMaintenanceIntent(candidate)
	require.NoError(t, err)
	dispatch := createdMaintenanceDispatch(t, admission)
	_, err = storeB.StartMaintenanceAppend(dispatch)
	require.ErrorContains(t, err, "not minted by this journal pair")
	require.ErrorContains(t, storeB.CancelMaintenanceIntent(dispatch),
		"not minted by this journal pair")

	replay, err := storeA.BeginMaintenanceIntent(candidate)
	require.NoError(t, err)
	require.Equal(t, MaintenanceIntentAdmissionExisting, replay.Disposition())
	_, ok := replay.CreatedDispatch()
	require.False(t, ok, "an exact replay must carry no first-dispatch capability")

	require.NoError(t, storeA.Close())
	reopened, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: pathA}, storageA, newTestStorageAuthorityGate(t),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	_, err = reopened.NewMaintenanceIntentCandidate(request, source, target)
	require.ErrorContains(t, err, "not minted by this journal pair")
	_, err = reopened.ProbeMaintenanceIntent(request)
	require.ErrorContains(t, err, "not minted by this callback journal")
	_, err = reopened.BeginMaintenanceIntent(candidate)
	require.ErrorContains(t, err, "not minted by this journal pair")
	_, err = reopened.StartMaintenanceAppend(dispatch)
	require.ErrorContains(t, err, "not minted by this journal pair")
	require.ErrorContains(t, reopened.CancelMaintenanceIntent(dispatch),
		"not minted by this journal pair")

	freshRequest, err := reopened.NewMaintenanceRequestAuthority(
		request.MaintenanceID(), request.Kind(), request.LeaseUUID(),
		request.CallbackURL(), nil,
	)
	require.NoError(t, err)
	disposition, err := reopened.ProbeMaintenanceIntent(freshRequest)
	require.NoError(t, err)
	require.Equal(t, MaintenanceIntentAdmissionExisting, disposition,
		"a reopen must recover durable replay classification through freshly issued authority")
	freshCandidate, err := reopened.NewMaintenanceIntentCandidate(freshRequest, source, target)
	require.NoError(t, err)
	freshReplay, err := reopened.BeginMaintenanceIntent(freshCandidate)
	require.NoError(t, err)
	require.Equal(t, MaintenanceIntentAdmissionExisting, freshReplay.Disposition())
	_, ok = freshReplay.CreatedDispatch()
	require.False(t, ok, "reopen recovery must not recreate first-dispatch authority")
}

func maintenanceUpdateSpecFromLatestActive(
	t *testing.T,
	callbacks *CallbackStore,
	releases *ReleaseStore,
	leaseUUID string,
	payload []byte,
) MaintenanceIntentCandidate {
	t.Helper()
	active, source, err := releases.claimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.MaintenanceID = MaintenanceID{}
	target.Manifest = append([]byte(nil), payload...)
	target.CreatedAt = time.Now()
	return newMaintenanceIntentSpec(
		t, callbacks, newTestMaintenanceID(t), MaintenanceIntentUpdate, source, target,
		"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	)
}

func completeMaintenanceForTest(
	t *testing.T,
	releases *ReleaseStore,
	callbacks *CallbackStore,
	spec MaintenanceIntentCandidate,
) {
	t.Helper()
	admission, err := callbacks.BeginMaintenanceIntent(spec)
	require.NoError(t, err)
	require.Equal(t, MaintenanceIntentAdmissionCreated, admission.Disposition())
	appendClaim, err := callbacks.StartMaintenanceAppend(
		createdMaintenanceDispatch(t, admission),
	)
	require.NoError(t, err)
	target, err := appendClaim.settlement.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	target, err = callbacks.BindMaintenanceIntentTarget(appendClaim.Intent(), target)
	require.NoError(t, err)
	intent := target.Intent()
	require.NoError(t, activateMaintenanceForTest(t, callbacks, releases, intent, target))
	completion, err := resolveMaintenanceForTest(t, callbacks, releases,
		intent, backend.CallbackStatusSuccess, "",
	)
	require.NoError(t, err)
	require.NoError(t, callbacks.removeEntry(completion))
}

func TestMaintenanceIntentExactReplaySurvivesOutboxRemovalAndProviderTTL(t *testing.T) {
	releases, callbacks, source, target := maintenanceFixture(t, "exact-replay")
	maintenanceID := newTestMaintenanceID(t)
	identity, ok := target.RuntimeIdentity()
	require.True(t, ok)
	request, err := newMaintenanceRequestAuthority(
		callbacks, maintenanceID, MaintenanceIntentRestart, source.LeaseUUID(),
		identity.LifecycleCallbackURL(), nil, "docker-a",
		callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	)
	require.NoError(t, err)
	spec, err := callbacks.NewMaintenanceIntentCandidate(request, source, target)
	require.NoError(t, err)

	admission, err := callbacks.BeginMaintenanceIntent(spec)
	require.NoError(t, err)
	assert.Equal(t, MaintenanceIntentAdmissionCreated, admission.Disposition())

	retry := spec
	retry.targetRelease.CreatedAt = time.Now()
	disposition, err := callbacks.ProbeMaintenanceIntent(retry.request)
	require.NoError(t, err)
	assert.Equal(t, MaintenanceIntentAdmissionExisting, disposition)
	replayed, err := callbacks.BeginMaintenanceIntent(retry)
	require.NoError(t, err)
	assert.Equal(t, MaintenanceIntentAdmissionExisting, replayed.Disposition())
	_, replayDispatch := replayed.CreatedDispatch()
	assert.False(t, replayDispatch, "an exact retry is acknowledgement-only mutation authority")

	completion, err := resolveMaintenanceForTest(t, callbacks, releases,
		admission.intent, backend.CallbackStatusFailed, "refused before append",
	)
	require.NoError(t, err)
	require.NoError(t, callbacks.removeEntry(completion))
	require.NoError(t, callbacks.db.View(func(tx *bolt.Tx) error {
		slots := tx.Bucket(callbackLeaseMutationUUIDSlotBucketName)
		assert.Equal(t, leaseMutationUUIDSlotValue, slots.Get([]byte(source.LeaseUUID())))
		assert.Equal(t, uint64(1), slots.Sequence(),
			"settling an idle live lease must retain its pre-reserved close slot")
		return nil
	}))
	require.NoError(t, callbacks.Healthy())

	// Terminal receipts on both sides are lifetime-scoped. Age the backend
	// receipt by a year: it remains the final dedupe fence for an arbitrarily
	// late provider replay.
	require.NoError(t, callbacks.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(callbackMaintenanceHistoryBucketName)
		leaseBucket := root.Bucket([]byte(source.LeaseUUID()))
		key := []byte(maintenanceID.String())
		record, decodeErr := decodeMaintenanceCompletionRecord(
			[]byte(source.LeaseUUID()), key, leaseBucket.Get(key),
		)
		if decodeErr != nil {
			return decodeErr
		}
		record.SettledAt = time.Now().Add(-365 * 24 * time.Hour)
		data, marshalErr := marshalMaintenanceCompletionRecord(record)
		if marshalErr != nil {
			return marshalErr
		}
		return leaseBucket.Put(key, data)
	}))

	disposition, err = callbacks.ProbeMaintenanceIntent(retry.request)
	require.NoError(t, err)
	assert.Equal(t, MaintenanceIntentAdmissionCompleted, disposition)
	completedReplay, err := callbacks.BeginMaintenanceIntent(retry)
	require.NoError(t, err)
	require.Equal(t, MaintenanceIntentAdmissionCompleted, completedReplay.Disposition())
	_, ok = completedReplay.CreatedDispatch()
	require.False(t, ok, "a completed replay must carry no first-dispatch capability")

	divergentRequest, err := newMaintenanceRequestAuthority(
		callbacks, maintenanceID, MaintenanceIntentUpdate, source.LeaseUUID(),
		identity.LifecycleCallbackURL(), target.Manifest, "docker-a",
		callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	)
	require.NoError(t, err)
	_, err = callbacks.ProbeMaintenanceIntent(divergentRequest)
	require.ErrorIs(t, err, ErrMaintenanceIntentConflict)

	requestB, err := newMaintenanceRequestAuthority(
		callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source.LeaseUUID(),
		identity.LifecycleCallbackURL(), nil, "docker-a",
		callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	)
	require.NoError(t, err)
	candidateB, err := callbacks.NewMaintenanceIntentCandidate(requestB, source, target)
	require.NoError(t, err)
	maintenanceB, err := callbacks.BeginMaintenanceIntent(candidateB)
	require.NoError(t, err)
	assert.Equal(t, MaintenanceIntentAdmissionCreated, maintenanceB.Disposition())
	disposition, err = callbacks.ProbeMaintenanceIntent(request)
	require.NoError(t, err)
	assert.Equal(t, MaintenanceIntentAdmissionCompleted, disposition,
		"a newer active maintenance head must not erase exact replay of A")
	require.NoError(t, callbacks.CancelMaintenanceIntent(
		createdMaintenanceDispatch(t, maintenanceB),
	))
	require.NoError(t, callbacks.Healthy())
}

func TestCompletedUpdateReplayIsSupersededByLaterUpdateGeneration(t *testing.T) {
	releases, callbacks, _, _ := maintenanceFixture(t, "superseded-update")
	leaseUUID := testLeaseUUID("maintenance-superseded-update")

	first := maintenanceUpdateSpecFromLatestActive(
		t, callbacks, releases, leaseUUID, []byte(`{"image":"payload-a"}`),
	)
	completeMaintenanceForTest(t, releases, callbacks, first)
	second := maintenanceUpdateSpecFromLatestActive(
		t, callbacks, releases, leaseUUID, []byte(`{"image":"payload-b"}`),
	)
	completeMaintenanceForTest(t, releases, callbacks, second)

	disposition, err := callbacks.ProbeMaintenanceIntent(first.request)
	require.NoError(t, err)
	assert.Equal(t, MaintenanceIntentAdmissionCompletedSuperseded, disposition,
		"a permanent receipt must prevent a late provider retry from reinstalling an older payload")
	disposition, err = callbacks.ProbeMaintenanceIntent(second.request)
	require.NoError(t, err)
	assert.Equal(t, MaintenanceIntentAdmissionCompleted, disposition,
		"the newest completed update remains recoverable after provider receipt loss")
}

func TestCompletedUpdateReplayIsSupersededByLaterPendingUpdate(t *testing.T) {
	releases, callbacks, _, _ := maintenanceFixture(t, "superseded-update")
	leaseUUID := testLeaseUUID("maintenance-superseded-update")

	first := maintenanceUpdateSpecFromLatestActive(
		t, callbacks, releases, leaseUUID, []byte(`{"image":"payload-a"}`),
	)
	completeMaintenanceForTest(t, releases, callbacks, first)
	second := maintenanceUpdateSpecFromLatestActive(
		t, callbacks, releases, leaseUUID, []byte(`{"image":"payload-b"}`),
	)
	admission, err := callbacks.BeginMaintenanceIntent(second)
	require.NoError(t, err)
	require.Equal(t, MaintenanceIntentAdmissionCreated, admission.Disposition())
	t.Cleanup(func() {
		require.NoError(t, callbacks.CancelMaintenanceIntent(
			createdMaintenanceDispatch(t, admission),
		))
	})

	disposition, err := callbacks.ProbeMaintenanceIntent(first.request)
	require.NoError(t, err)
	assert.Equal(t, MaintenanceIntentAdmissionCompletedSuperseded, disposition)
}

func TestMaintenanceReceiptCapacityRefusesBeforePublishingHead(t *testing.T) {
	_, callbacks, source, target := maintenanceFixture(t, "receipt-capacity")
	identity, ok := target.RuntimeIdentity()
	require.True(t, ok)
	storageID := callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000")

	require.NoError(t, callbacks.db.Update(func(tx *bolt.Tx) error {
		for index := range maxMaintenanceReceiptsPerLease {
			id := newTestMaintenanceID(t)
			digest := sha256.Sum256([]byte(fmt.Sprintf("maintenance-receipt-%d", index)))
			record := maintenanceCompletionRecord{
				Version:            maintenanceCompletionRecordV1,
				MaintenanceID:      id,
				Kind:               MaintenanceIntentRestart,
				LeaseUUID:          source.LeaseUUID(),
				RequestDigest:      encodeMaintenanceDigest(digest),
				CompletionSequence: uint64(index + 1),
				Backend:            "docker-a",
				BackendStorageID:   storageID.String(),
				Tenant:             identity.Tenant(),
				ProviderUUID:       identity.ProviderUUID(),
				Status:             backend.CallbackStatusSuccess,
				SettledAt:          time.Now(),
			}
			if err := archiveMaintenanceCompletionTx(tx, record); err != nil {
				return err
			}
		}
		return tx.Bucket(callbackLeaseMutationHeadBucketName).
			SetSequence(uint64(maxMaintenanceReceiptsPerLease))
	}))
	require.NoError(t, callbacks.Healthy())

	request, err := newMaintenanceRequestAuthority(
		callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source.LeaseUUID(),
		identity.LifecycleCallbackURL(), nil, "docker-a", storageID,
	)
	require.NoError(t, err)
	candidate, err := callbacks.NewMaintenanceIntentCandidate(request, source, target)
	require.NoError(t, err)
	_, err = callbacks.BeginMaintenanceIntent(candidate)
	require.ErrorIs(t, err, ErrMaintenanceReceiptCapacity)
	require.ErrorIs(t, err, backend.ErrCapacityRefused)
	require.ErrorIs(t, err, backend.ErrInsufficientResources)
	var capacityErr *MaintenanceReceiptCapacityError
	require.ErrorAs(t, err, &capacityErr)
	assert.Equal(t, source.LeaseUUID(), capacityErr.LeaseUUID)
	assert.Equal(t, uint64(maxMaintenanceReceiptsPerLease), capacityErr.Limit)

	intents, listErr := callbacks.listMaintenanceIntents()
	require.NoError(t, listErr)
	assert.Empty(t, intents, "capacity refusal must precede aggregate-head publication")
}

func TestLeaseMutationUUIDCapacityRefusesMaintenanceAdmission(t *testing.T) {
	_, callbacks, source, target := maintenanceFixture(t, "uuid-capacity")
	require.NoError(t, callbacks.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackLeaseMutationUUIDSlotBucketName).
			SetSequence(maxLeaseMutationUUIDSlotsGlobal)
	}))

	spec := newMaintenanceIntentSpec(
		t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target,
		"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	)
	_, err := callbacks.BeginMaintenanceIntent(spec)
	require.ErrorIs(t, err, ErrLeaseMutationCapacity)
	require.ErrorIs(t, err, backend.ErrCapacityRefused)
	require.ErrorIs(t, err, backend.ErrInsufficientResources)

	intents, listErr := callbacks.listMaintenanceIntents()
	require.NoError(t, listErr)
	assert.Empty(t, intents, "capacity refusal must not publish maintenance authority")
	require.NoError(t, callbacks.db.View(func(tx *bolt.Tx) error {
		assert.Nil(t, tx.Bucket(callbackLeaseMutationUUIDSlotBucketName).
			Get([]byte(source.LeaseUUID())))
		return nil
	}))
}

func TestMaintenanceIntentSurvivesEveryCommitBoundaryAndSettlesExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	releasePath := filepath.Join(dir, "releases.db")
	callbackPath := filepath.Join(dir, "callbacks.db")
	releases, err := newUnboundReleaseStoreForTest(ReleaseStoreConfig{DBPath: releasePath})
	require.NoError(t, err)
	callbacks, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	leaseUUID := testLeaseUUID("maintenance-restart-boundaries")
	require.NoError(t, releases.appendActive(leaseUUID, validRuntimeAuthorityRelease()))
	active, source, err := releases.claimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()

	admission, err := callbacks.BeginMaintenanceIntent(newMaintenanceIntentSpec(
		t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target,
		"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	))
	require.NoError(t, err)
	require.True(t, admission.MaintenanceID().Valid())
	assert.Equal(t, admission.MaintenanceID(), admission.TargetRelease().MaintenanceID)
	dispatch := createdMaintenanceDispatch(t, admission)
	require.NoError(t, dispatch.settlement.CheckAppendMaintenanceCapacity(dispatch))
	appendClaim, err := callbacks.StartMaintenanceAppend(dispatch)
	require.NoError(t, err)
	require.NoError(t, callbacks.Close())

	callbacks, err = newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	settlement := maintenancePairForTest(t, callbacks, releases)
	intents, err := settlement.ListMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	_, err = settlement.AppendMaintenance(appendClaim)
	require.ErrorContains(t, err, "another journal pair")
	appendClaim, err = settlement.RecoverMaintenanceAppend(intents[0])
	require.NoError(t, err)
	targetClaim, err := settlement.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	require.NoError(t, releases.Close())
	require.NoError(t, callbacks.Close())

	releases, err = newUnboundReleaseStoreForTest(ReleaseStoreConfig{DBPath: releasePath})
	require.NoError(t, err)
	callbacks, err = newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})
	settlement = maintenancePairForTest(t, callbacks, releases)
	intents, err = settlement.ListMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	intent := intents[0]
	_, recoveredTargetClaim, found, err := settlement.FindMaintenanceRelease(
		leaseUUID, intent.MaintenanceID(),
	)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, targetClaim.Digest(), recoveredTargetClaim.Digest())
	recoveredTargetClaim, err = callbacks.BindMaintenanceIntentTarget(intent, recoveredTargetClaim)
	require.NoError(t, err)
	intent = recoveredTargetClaim.Intent()
	activeProof := activateMaintenanceOutcomeForTest(t, settlement, recoveredTargetClaim)

	entry, err := resolveMaintenanceSuccessForTest(settlement, activeProof)
	require.NoError(t, err)
	assert.Equal(t, CallbackDeliveryKindMaintenance, entry.DeliveryKind)
	assert.Equal(t, intent.LifecycleCallbackURL(), entry.CallbackURL)
	_, err = resolveMaintenanceForTest(t, callbacks, releases, intent, backend.CallbackStatusSuccess, "")
	require.ErrorContains(t, err, "no longer exists")
	intents, err = callbacks.listMaintenanceIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	assert.Equal(t, CallbackDeliveryKindMaintenance, pending[0].DeliveryKind)
	activeAfter, err := releases.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, activeAfter)
	assert.Equal(t, intent.MaintenanceID(), activeAfter.MaintenanceID)
}

func TestBeginMaintenanceIntentWaitsForPriorCompletionAcrossReopenAndPreciseRemoval(t *testing.T) {
	dir := t.TempDir()
	releasePath := filepath.Join(dir, "releases.db")
	callbackPath := filepath.Join(dir, "callbacks.db")
	releases, err := newUnboundReleaseStoreForTest(ReleaseStoreConfig{DBPath: releasePath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, releases.Close()) })
	callbacks, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)

	leaseUUID := testLeaseUUID("maintenance-completion-admission-fence")
	require.NoError(t, releases.appendActive(leaseUUID, validRuntimeAuthorityRelease()))
	spec := maintenanceSpecFromLatestActive(t, callbacks, releases, leaseUUID, MaintenanceIntentRestart)

	// A non-maintenance observation may be older in the FIFO. The admission
	// guard must find the exact maintenance completion anywhere in the lease
	// queue rather than inspecting only its head.
	olderLifecycle, err := callbacks.storeEntry(CallbackEntry{
		LeaseUUID:        leaseUUID,
		CallbackURL:      spec.targetRelease.RuntimeAuthority.LifecycleCallbackURL(),
		DeliveryKind:     CallbackDeliveryKindLifecycle,
		Status:           backend.CallbackStatusFailed,
		Backend:          spec.request.Backend(),
		BackendStorageID: spec.request.BackendStorageID().String(),
		Error:            "older runtime observation",
		CreatedAt:        time.Now(),
	})
	require.NoError(t, err)

	admission, err := callbacks.BeginMaintenanceIntent(spec)
	require.NoError(t, err, "an ordinary lifecycle observation is not an exact maintenance completion")
	appendClaim, err := callbacks.StartMaintenanceAppend(
		createdMaintenanceDispatch(t, admission),
	)
	require.NoError(t, err)
	target, err := appendClaim.settlement.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	target, err = callbacks.BindMaintenanceIntentTarget(appendClaim.Intent(), target)
	require.NoError(t, err)
	intent := target.Intent()
	require.NoError(t, activateMaintenanceForTest(t, callbacks, releases, intent, target))
	completion, err := resolveMaintenanceForTest(t, callbacks, releases,
		intent, backend.CallbackStatusSuccess, "",
	)
	require.NoError(t, err)

	next := maintenanceSpecFromLatestActive(t, callbacks, releases, leaseUUID, MaintenanceIntentUpdate)
	_, err = callbacks.BeginMaintenanceIntent(next)
	require.ErrorIs(t, err, backend.ErrInvalidState)
	require.ErrorContains(t, err, "pending delivery")

	// Removing another FIFO member cannot release the generation fence.
	require.NoError(t, callbacks.removeEntry(olderLifecycle))
	_, err = callbacks.BeginMaintenanceIntent(next)
	require.ErrorIs(t, err, backend.ErrInvalidState)

	require.NoError(t, callbacks.Close())
	callbacks, err = newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbacks.Close()) })
	next = reissueMaintenanceCandidate(t, callbacks, next)
	_, err = callbacks.BeginMaintenanceIntent(next)
	require.ErrorIs(t, err, backend.ErrInvalidState,
		"the undelivered generation fence must survive backend restart")

	pending, err := callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, completion.DeliveryID, pending[0].DeliveryID)
	require.NoError(t, callbacks.removeEntry(pending[0]))

	nextAdmission, err := callbacks.BeginMaintenanceIntent(next)
	require.NoError(t, err, "precise removal after synchronous application releases admission")
	require.NoError(t, callbacks.CancelMaintenanceIntent(
		createdMaintenanceDispatch(t, nextAdmission),
	))
}

func TestResolveMaintenanceIntentAndNextBeginHaveNoAdmissionGap(t *testing.T) {
	releases, callbacks, _, _ := maintenanceFixture(t, "resolve-next-begin-race")
	const iterations = 32

	for i := range iterations {
		leaseUUID := testLeaseUUID(fmt.Sprintf("maintenance-generation-race-%d", i))
		require.NoError(t, releases.appendActive(leaseUUID, validRuntimeAuthorityRelease()))
		firstSpec := maintenanceSpecFromLatestActive(t, callbacks, releases, leaseUUID, MaintenanceIntentRestart)
		firstAdmission, err := callbacks.BeginMaintenanceIntent(firstSpec)
		require.NoError(t, err)
		appendClaim, err := callbacks.StartMaintenanceAppend(
			createdMaintenanceDispatch(t, firstAdmission),
		)
		require.NoError(t, err)
		target, err := appendClaim.settlement.AppendMaintenance(appendClaim)
		require.NoError(t, err)
		target, err = callbacks.BindMaintenanceIntentTarget(appendClaim.Intent(), target)
		require.NoError(t, err)
		intent := target.Intent()
		require.NoError(t, activateMaintenanceForTest(t, callbacks, releases, intent, target))
		next := maintenanceSpecFromLatestActive(t, callbacks, releases, leaseUUID, MaintenanceIntentUpdate)

		start := make(chan struct{})
		resolveErr := make(chan error, 1)
		beginErr := make(chan error, 1)
		go func() {
			<-start
			_, resolve := resolveMaintenanceForTest(t, callbacks, releases,
				intent, backend.CallbackStatusSuccess, "",
			)
			resolveErr <- resolve
		}()
		go func() {
			<-start
			_, begin := callbacks.BeginMaintenanceIntent(next)
			beginErr <- begin
		}()
		close(start)

		require.NoError(t, <-resolveErr)
		err = <-beginErr
		require.Error(t, err)
		assert.True(t,
			errors.Is(err, ErrMaintenanceIntentConflict) || errors.Is(err, backend.ErrInvalidState),
			"next admission must see either the unresolved intent or its atomic completion: %v", err,
		)
		intents, listErr := callbacks.listMaintenanceIntents()
		require.NoError(t, listErr)
		assert.Empty(t, intents)
		pending, listErr := callbacks.ListPending()
		require.NoError(t, listErr)
		var exact CallbackEntry
		for _, entry := range pending {
			if entry.LeaseUUID == leaseUUID && entry.DeliveryKind == CallbackDeliveryKindMaintenance {
				exact = entry
				break
			}
		}
		require.NotEmpty(t, exact.DeliveryID)
		require.NoError(t, callbacks.removeEntry(exact))
	}
}

func TestSendMaintenanceCallback_DurableSettlementOnlyNotifiesReplay(t *testing.T) {
	fixture := beginBoundMaintenance(t, "enqueue-only")
	target := fixture.appendAndBind(t)
	active := activateMaintenanceOutcomeForTest(t, fixture.settlement, target)
	callbacks := fixture.stores.callbacks
	intent := fixture.intent

	var requests atomic.Int32
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: callbacks,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			requests.Add(1)
			return callbackHTTPResponse(http.StatusNoContent), nil
		})},
		Secret: "secret",
		Logger: slog.Default(),

		Backoff: &zeroBackoff,
	})
	publisher := mustNewCallbackPublisherForTest(t, CallbackPublisherConfig{
		OperationSettlement:   fixture.stores.settlement,
		MaintenanceSettlement: fixture.settlement,
		StorageAttestor:       sender.attestor,
		Logger:                sender.logger,
	})

	require.NoError(t, publisher.PublishMaintenanceSuccessContext(context.Background(), active))
	assert.Zero(t, requests.Load(),
		"maintenance settlement must not perform callback HTTP inline")
	assert.Zero(t, sender.replayWake.pendingCount(),
		"maintenance settlement must not address a sender before its replay loop subscribes")
	_, found, err := callbacks.getMaintenanceIntent(intent.LeaseUUID())
	require.NoError(t, err)
	assert.False(t, found, "intent removal and outbox publication must settle atomically")
	pending, err := callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, CallbackDeliveryKindMaintenance, pending[0].DeliveryKind)

	sender.replayPendingCallbacks()
	assert.Equal(t, int32(1), requests.Load())
	pending, err = callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestSendMaintenanceCallback_DoesNotWaitForSlowReplay(t *testing.T) {
	fixture := beginBoundMaintenance(t, "slow-replay")
	target := fixture.appendAndBind(t)
	active := activateMaintenanceOutcomeForTest(t, fixture.settlement, target)
	callbacks := fixture.stores.callbacks
	intent := fixture.intent

	_, err := callbacks.storeValidTestEntry(CallbackEntry{
		LeaseUUID:        intent.LeaseUUID(),
		CallbackURL:      intent.LifecycleCallbackURL(),
		DeliveryKind:     CallbackDeliveryKindLifecycle,
		Status:           backend.CallbackStatusFailed,
		Backend:          "docker-a",
		BackendStorageID: intent.BackendStorageID().String(),
		CreatedAt:        time.Now(),
	})
	require.NoError(t, err)

	headStarted := make(chan struct{})
	releaseHead := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseHead) }) })
	var requests atomic.Int32
	sender := mustNewDurableCallbackSender(t, CallbackSenderConfig{
		Store: callbacks,
		HTTPClient: &http.Client{Transport: callbackRoundTripFunc(func(*http.Request) (*http.Response, error) {
			if requests.Add(1) == 1 {
				close(headStarted)
				<-releaseHead
			}
			return callbackHTTPResponse(http.StatusNoContent), nil
		})},
		Secret: "secret",
		Logger: slog.Default(),

		Backoff: &zeroBackoff,
	})
	publisher := mustNewCallbackPublisherForTest(t, CallbackPublisherConfig{
		OperationSettlement:   fixture.stores.settlement,
		MaintenanceSettlement: fixture.settlement,
		StorageAttestor:       sender.attestor,
		Logger:                sender.logger,
	})

	replayDone := make(chan struct{})
	go func() {
		defer close(replayDone)
		sender.replayPendingCallbacks()
	}()
	select {
	case <-headStarted:
	case <-time.After(time.Second):
		t.Fatal("replay did not begin the older lifecycle callback")
	}

	settled := make(chan error, 1)
	go func() {
		settled <- publisher.PublishMaintenanceSuccessContext(context.Background(), active)
	}()
	select {
	case err := <-settled:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("maintenance settlement waited behind callback HTTP")
	}
	_, found, err := callbacks.getMaintenanceIntent(intent.LeaseUUID())
	require.NoError(t, err)
	assert.False(t, found, "settlement must consume the intent before HTTP completes")
	pending, err := callbacks.listPending(intent.LeaseUUID())
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[0].DeliveryKind)
	assert.Equal(t, CallbackDeliveryKindMaintenance, pending[1].DeliveryKind)

	releaseOnce.Do(func() { close(releaseHead) })
	select {
	case <-replayDone:
	case <-time.After(time.Second):
		t.Fatal("same replay did not drain the appended maintenance completion")
	}
	assert.Equal(t, int32(2), requests.Load())
	pending, err = callbacks.listPending(intent.LeaseUUID())
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestMaintenanceIntentUnboundFailureAndBoundCancellationFence(t *testing.T) {
	t.Run("before append start can be canceled", func(t *testing.T) {
		_, callbacks, source, target := maintenanceFixture(t, "cancelable-admission")
		admission, err := callbacks.BeginMaintenanceIntent(newMaintenanceIntentSpec(
			t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target,
			"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
		))
		require.NoError(t, err)
		dispatch := createdMaintenanceDispatch(t, admission)
		require.NoError(t, callbacks.CancelMaintenanceIntent(dispatch))
		_, found, err := callbacks.getMaintenanceIntent(admission.LeaseUUID())
		require.NoError(t, err)
		require.False(t, found)
		_, err = callbacks.StartMaintenanceAppend(dispatch)
		require.ErrorContains(t, err, "no longer exists")
	})

	t.Run("before target append resolves failure", func(t *testing.T) {
		releases, callbacks, intent, _ := beginMaintenanceFixture(t, "unbound-failure")
		_, err := resolveMaintenanceForTest(t, callbacks, releases, intent, backend.CallbackStatusSuccess, "")
		require.ErrorContains(t, err, "unbound")
		_, err = resolveMaintenanceForTest(t, callbacks, releases, intent, backend.CallbackStatusFailed, "restart interrupted")
		require.NoError(t, err)
	})

	t.Run("append-started admission cannot be canceled", func(t *testing.T) {
		releases, callbacks, source, targetTemplate := maintenanceFixture(t, "bound-cancel")
		admission, err := callbacks.BeginMaintenanceIntent(newMaintenanceIntentSpec(
			t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, targetTemplate,
			"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
		))
		require.NoError(t, err)
		dispatch := createdMaintenanceDispatch(t, admission)
		appendClaim, err := callbacks.StartMaintenanceAppend(dispatch)
		require.NoError(t, err)
		// This is the cross-store window: callback WAL advancement committed,
		// but releases.db has not. A copied pre-start admission must already be
		// powerless to orphan the only recovery index.
		require.ErrorContains(t, callbacks.CancelMaintenanceIntent(dispatch), "changed")
		_, _, found, err := maintenancePairForTest(t, callbacks, releases).FindMaintenanceRelease(
			admission.LeaseUUID(), admission.MaintenanceID(),
		)
		require.NoError(t, err)
		require.False(t, found)
		target, err := appendClaim.settlement.AppendMaintenance(appendClaim)
		require.NoError(t, err)
		bound, err := callbacks.BindMaintenanceIntentTarget(appendClaim.Intent(), target)
		require.NoError(t, err)
		require.True(t, bound.Valid())
		claims, err := callbacks.listMaintenanceIntents()
		require.NoError(t, err)
		require.Len(t, claims, 1)
	})
}

func TestTryResolveMaintenanceIntentDefersBehindConcurrentMutation(t *testing.T) {
	_, callbacks, intent, appendClaim := beginMaintenanceFixture(t, "try-resolve-busy")
	settlement := appendClaim.settlement
	target, err := appendClaim.settlement.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	target, err = callbacks.BindMaintenanceIntentTarget(intent, target)
	require.NoError(t, err)
	intent = target.Intent()
	active := activateMaintenanceOutcomeForTest(t, settlement, target)

	unlock := callbacks.lockDeliveryLease(intent.LeaseUUID())
	_, acquired, err := tryResolveMaintenanceSuccessForTest(settlement, active)
	require.NoError(t, err)
	require.False(t, acquired)
	claims, err := callbacks.listMaintenanceIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	pending, err := callbacks.ListPending()
	require.NoError(t, err)
	require.Empty(t, pending)
	unlock()

	_, acquired, err = tryResolveMaintenanceSuccessForTest(settlement, active)
	require.NoError(t, err)
	require.True(t, acquired)
	pending, err = callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
}

func TestTryBindMaintenanceIntentTargetDefersBehindConcurrentMutation(t *testing.T) {
	_, callbacks, intent, appendClaim := beginMaintenanceFixture(t, "try-bind-busy")
	settlement := appendClaim.settlement
	target, err := settlement.AppendMaintenance(appendClaim)
	require.NoError(t, err)

	unlock := callbacks.lockDeliveryLease(intent.LeaseUUID())
	bound, acquired, err := callbacks.TryBindMaintenanceIntentTarget(intent, target)
	require.NoError(t, err)
	require.False(t, acquired)
	require.False(t, bound.Valid())
	stored, found, err := callbacks.getMaintenanceIntent(intent.LeaseUUID())
	require.NoError(t, err)
	require.True(t, found)
	_, targetBound := settlement.targetReleaseClaim(stored)
	assert.False(t, targetBound, "busy binding must leave the exact intent untouched")
	unlock()

	bound, acquired, err = callbacks.TryBindMaintenanceIntentTarget(intent, target)
	require.NoError(t, err)
	require.True(t, acquired)
	require.True(t, bound.Valid())
	assert.Equal(t, target.Version(), bound.Version())
	assert.Equal(t, target.Digest(), bound.Digest())
}

func TestTryResolveMaintenanceIntentWithRuntimeFailureIsAtomicAndOrdered(t *testing.T) {
	dir := t.TempDir()
	releasePath := filepath.Join(dir, "releases.db")
	callbackPath := filepath.Join(dir, "callbacks.db")
	releases, err := newUnboundReleaseStoreForTest(ReleaseStoreConfig{DBPath: releasePath})
	require.NoError(t, err)
	callbacks, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})

	leaseUUID := testLeaseUUID("maintenance-runtime-divergence")
	require.NoError(t, releases.appendActive(leaseUUID, validRuntimeAuthorityRelease()))
	active, source, err := releases.claimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := cloneRelease(active)
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	admission, err := callbacks.BeginMaintenanceIntent(newMaintenanceIntentSpec(
		t, callbacks, newTestMaintenanceID(t), MaintenanceIntentUpdate, source, target,
		"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	))
	require.NoError(t, err)
	appendClaim, err := callbacks.StartMaintenanceAppend(
		createdMaintenanceDispatch(t, admission),
	)
	require.NoError(t, err)
	intent := appendClaim.Intent()
	targetClaim, err := appendClaim.settlement.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	targetClaim, err = callbacks.BindMaintenanceIntentTarget(intent, targetClaim)
	require.NoError(t, err)
	intent = targetClaim.Intent()
	activeProof := activateMaintenanceOutcomeForTest(t, intent.settlement, targetClaim)
	intent = activeProof.Intent()
	settlement := intent.settlement

	// A concurrent journal mutation owns the same lease lock. The try form must
	// leave both journals byte-for-byte in their pre-settlement state.
	unlock := callbacks.lockDeliveryLease(leaseUUID)
	acquired, err := tryResolveMaintenanceRuntimeFailureForTest(settlement,
		activeProof, "committed runtime cohort is missing")

	require.NoError(t, err)
	require.False(t, acquired)
	_, found, err := callbacks.getMaintenanceIntent(leaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	pending, err := callbacks.ListPending()
	require.NoError(t, err)
	require.Empty(t, pending)
	unlock()

	acquired, err = tryResolveMaintenanceRuntimeFailureForTest(settlement,
		activeProof, "committed runtime cohort is missing")

	require.NoError(t, err)
	require.True(t, acquired)

	// Reopen the database to prove that intent removal and both ordered facts
	// share one durable transaction rather than merely one process snapshot.
	require.NoError(t, callbacks.Close())
	callbacks, err = newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: callbackPath})
	require.NoError(t, err)
	_, found, err = callbacks.getMaintenanceIntent(leaseUUID)
	require.NoError(t, err)
	require.False(t, found)
	pending, err = callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, CallbackDeliveryKindMaintenance, pending[0].DeliveryKind)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	assert.Equal(t, CallbackDeliveryKindMaintenance, pending[1].DeliveryKind)
	assert.Equal(t, backend.CallbackStatusFailed, pending[1].Status)
	assert.Equal(t, "committed runtime cohort is missing", pending[1].Error)
	assert.Less(t, pending[0].Sequence, pending[1].Sequence)

	// Delivering only the Success head must not admit a newer maintenance
	// generation ahead of its paired runtime-failure fact. Both rows are exact
	// barriers even though the second carries lifecycle Failed on the wire.
	require.NoError(t, callbacks.removeEntry(pending[0]))
	latest, nextSource, err := releases.claimLatestActive(leaseUUID)
	require.NoError(t, err)
	nextTarget := cloneRelease(latest)
	nextTarget.Version = 0
	nextTarget.MaintenanceID = MaintenanceID{}
	nextTarget.Status = "deploying"
	nextTarget.CreatedAt = time.Now()
	_, err = callbacks.BeginMaintenanceIntent(newMaintenanceIntentSpec(
		t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, nextSource, nextTarget,
		"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	))
	require.ErrorIs(t, err, backend.ErrInvalidState)
	require.NoError(t, callbacks.removeEntry(pending[1]))
	nextAdmission, err := callbacks.BeginMaintenanceIntent(newMaintenanceIntentSpec(
		t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, nextSource, nextTarget,
		"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	))
	require.NoError(t, err)
	require.NoError(t, callbacks.CancelMaintenanceIntent(
		createdMaintenanceDispatch(t, nextAdmission),
	))
}

func TestMaintenanceReleaseExactMutationNeverTargetsLatest(t *testing.T) {
	releases, callbacks, firstIntent, firstAppend := beginMaintenanceFixture(t, "not-latest")
	firstTarget, err := firstAppend.settlement.AppendMaintenance(firstAppend)
	require.NoError(t, err)
	firstTarget, err = callbacks.BindMaintenanceIntentTarget(firstIntent, firstTarget)
	require.NoError(t, err)
	firstIntent = firstTarget.Intent()
	settlement := firstTarget.settlement
	activeProof := activateMaintenanceOutcomeForTest(t, settlement, firstTarget)
	completion, err := resolveMaintenanceSuccessForTest(settlement, activeProof)
	require.NoError(t, err)
	// A protected exact completion deliberately blocks a later maintenance
	// admission until delivery consumes that precise row. Model the successful
	// delivery before exercising the next generation.
	require.NoError(t, callbacks.removeEntry(completion))

	active, secondSource, err := releases.claimLatestActive(firstIntent.LeaseUUID())
	require.NoError(t, err)
	secondTemplate := cloneRelease(active)
	secondTemplate.Version = 0
	secondTemplate.MaintenanceID = MaintenanceID{}
	secondTemplate.Status = "deploying"
	secondTemplate.CreatedAt = time.Now().Add(time.Second)
	secondAdmission, err := callbacks.BeginMaintenanceIntent(newMaintenanceIntentSpec(
		t, callbacks, newTestMaintenanceID(t), MaintenanceIntentUpdate, secondSource, secondTemplate,
		"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	))
	require.NoError(t, err)
	secondAppend, err := callbacks.StartMaintenanceAppend(
		createdMaintenanceDispatch(t, secondAdmission),
	)
	require.NoError(t, err)
	secondTarget, err := secondAppend.settlement.AppendMaintenance(secondAppend)
	require.NoError(t, err)

	// Once the first intent is settled, its copied release claim cannot cross a
	// later intent's pair gate. The stale authority is rejected before either
	// exact row can change.
	require.Error(t, activateMaintenanceForTest(t, callbacks, releases, firstIntent, firstTarget))
	list, err := releases.List(firstIntent.LeaseUUID())
	require.NoError(t, err)
	require.Len(t, list, 3)
	assert.Equal(t, "active", list[1].Status)
	assert.Equal(t, firstIntent.MaintenanceID(), list[1].MaintenanceID)
	assert.Equal(t, "deploying", list[2].Status)
	assert.Equal(t, secondTarget.MaintenanceID(), list[2].MaintenanceID)
}

func TestMaintenanceReleaseRejectsRawMutationBypasses(t *testing.T) {
	t.Run("append APIs require typed admission", func(t *testing.T) {
		releases, _, intent, _ := beginMaintenanceFixture(t, "raw-append")
		target := intent.TargetRelease()
		require.ErrorIs(t, releases.append(intent.LeaseUUID(), target), ErrMaintenanceReleaseClaimRequired)
		target.Status = "active"
		require.ErrorIs(t, releases.checkAppendActiveCapacity(intent.LeaseUUID(), target), ErrMaintenanceReleaseClaimRequired)
		require.ErrorIs(t, releases.appendActive(intent.LeaseUUID(), target), ErrMaintenanceReleaseClaimRequired)
		history, err := releases.List(intent.LeaseUUID())
		require.NoError(t, err)
		require.Len(t, history, 1)
	})

	t.Run("latest and raw delete APIs cannot mutate a typed target", func(t *testing.T) {
		releases, callbacks, intent, appendClaim := beginMaintenanceFixture(t, "raw-terminal")
		target, err := appendClaim.settlement.AppendMaintenance(appendClaim)
		require.NoError(t, err)
		target, err = callbacks.BindMaintenanceIntentTarget(intent, target)
		require.NoError(t, err)
		intent = target.Intent()
		raw := validRuntimeAuthorityRelease()
		raw.Status = "failed"
		raw.CreatedAt = time.Now().Add(time.Second)
		require.ErrorIs(t, releases.append(intent.LeaseUUID(), raw), ErrMaintenanceReleaseClaimRequired)
		raw.Status = "active"
		require.ErrorIs(t,
			releases.checkAppendActiveCapacity(intent.LeaseUUID(), raw),
			ErrMaintenanceReleaseClaimRequired,
		)
		require.ErrorIs(t, releases.appendActive(intent.LeaseUUID(), raw), ErrMaintenanceReleaseClaimRequired)
		require.ErrorIs(t,
			releases.updateLatestStatus(intent.LeaseUUID(), "failed", backend.ReasonUpdateFailed, "bypass"),
			ErrMaintenanceReleaseClaimRequired,
		)
		require.ErrorIs(t, releases.activateLatest(intent.LeaseUUID()), ErrMaintenanceReleaseClaimRequired)
		targetRelease := intent.TargetRelease()
		require.ErrorIs(t, releases.backfillActiveResourceProfiles(
			intent.LeaseUUID(),
			intent.SourceRelease().Version(),
			targetRelease.Items,
			targetRelease.ResourceProfiles,
		), ErrMaintenanceReleaseClaimRequired)
		require.ErrorIs(t, releases.delete(intent.LeaseUUID()), ErrMaintenanceReleaseClaimRequired)

		release, _, found, err := maintenancePairForTest(t, callbacks, releases).FindMaintenanceRelease(
			intent.LeaseUUID(), intent.MaintenanceID(),
		)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "deploying", release.Status)
		require.NoError(t, activateMaintenanceForTest(t, callbacks, releases, intent, target))
	})
}

func TestMaintenanceReleaseTerminalHistoryDoesNotBlockLaterGeneration(t *testing.T) {
	for _, terminal := range []string{"active", "failed"} {
		t.Run(terminal, func(t *testing.T) {
			releases, callbacks, intent, appendClaim := beginMaintenanceFixture(t, "terminal-followed-by-"+terminal)
			target, err := appendClaim.settlement.AppendMaintenance(appendClaim)
			require.NoError(t, err)
			target, err = callbacks.BindMaintenanceIntentTarget(intent, target)
			require.NoError(t, err)
			intent = target.Intent()
			if terminal == "active" {
				require.NoError(t, activateMaintenanceForTest(t, callbacks, releases, intent, target))
			} else {
				require.NoError(t, failMaintenanceForTest(
					t, callbacks, releases, intent, target,
					backend.ReasonUpdateFailed, "maintenance failed",
				))
			}

			next := validRuntimeAuthorityRelease()
			next.CreatedAt = time.Now().Add(time.Second)
			require.NoError(t, releases.checkAppendActiveCapacity(intent.LeaseUUID(), next))
			require.NoError(t, releases.appendActive(intent.LeaseUUID(), next))

			latest, err := releases.LatestActive(intent.LeaseUUID())
			require.NoError(t, err)
			require.NotNil(t, latest)
			assert.Empty(t, latest.MaintenanceID)
			assert.Equal(t, next.OperationID, latest.OperationID)
		})
	}
}

func TestMaintenanceAppendPreservesRuntimeAuthorityIdentity(t *testing.T) {
	t.Run("trusted callback route base may rotate", func(t *testing.T) {
		_, callbacks, source, target := maintenanceFixture(t, "rotated-route-base")
		operationID := target.OperationID
		authority, err := NewReleaseRuntimeAuthority(
			operationID,
			target.RuntimeAuthority.Tenant(),
			target.RuntimeAuthority.ProviderUUID(),
			"https://rotated.example/callbacks/provision?operation_id="+operationID.String(),
			"https://rotated.example/callbacks/provision?lifecycle_id="+operationID.String(),
		)
		require.NoError(t, err)
		target.RuntimeAuthority = &authority
		intent, err := callbacks.BeginMaintenanceIntent(newMaintenanceIntentSpec(
			t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target,
			"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
		))
		require.NoError(t, err)
		dispatch := createdMaintenanceDispatch(t, intent)
		require.NoError(t, dispatch.settlement.CheckAppendMaintenanceCapacity(dispatch))
	})

	for _, test := range []struct {
		name      string
		wantError string
		mutate    func(*testing.T, *Release)
	}{
		{
			name:      "tenant",
			wantError: "changes tenant authority",
			mutate: func(t *testing.T, target *Release) {
				authority, err := NewReleaseRuntimeAuthority(
					target.OperationID,
					"tenant-b",
					target.RuntimeAuthority.ProviderUUID(),
					target.RuntimeAuthority.CallbackURL(),
					target.RuntimeAuthority.LifecycleCallbackURL(),
				)
				require.NoError(t, err)
				target.RuntimeAuthority = &authority
			},
		},
		{
			name:      "provider",
			wantError: "changes provider authority",
			mutate: func(t *testing.T, target *Release) {
				authority, err := NewReleaseRuntimeAuthority(
					target.OperationID,
					target.RuntimeAuthority.Tenant(),
					"33333333-3333-4333-8333-333333333333",
					target.RuntimeAuthority.CallbackURL(),
					target.RuntimeAuthority.LifecycleCallbackURL(),
				)
				require.NoError(t, err)
				target.RuntimeAuthority = &authority
			},
		},
		{
			name:      "operation ID",
			wantError: "changes operation lineage",
			mutate: func(t *testing.T, target *Release) {
				operationID := mustSharedOperationID(uuid.NewString())
				authority, err := NewReleaseRuntimeAuthority(
					operationID,
					target.RuntimeAuthority.Tenant(),
					target.RuntimeAuthority.ProviderUUID(),
					"https://fred.example/callbacks/provision?operation_id="+operationID.String(),
					"https://fred.example/callbacks/provision?lifecycle_id="+operationID.String(),
				)
				require.NoError(t, err)
				target.OperationID = operationID
				target.RuntimeAuthority = &authority
			},
		},
	} {
		t.Run("rejects "+test.name+" divergence", func(t *testing.T) {
			_, callbacks, source, target := maintenanceFixture(t, "authority-"+test.name)
			test.mutate(t, &target)
			intent, err := callbacks.BeginMaintenanceIntent(newMaintenanceIntentSpec(
				t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target,
				"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
			))
			require.NoError(t, err)
			dispatch := createdMaintenanceDispatch(t, intent)
			require.ErrorContains(t, dispatch.settlement.CheckAppendMaintenanceCapacity(dispatch), test.wantError)
		})
	}

	t.Run("constructor rejects malformed mixed callback pair", func(t *testing.T) {
		operationID := mustSharedOperationID(uuid.NewString())
		_, err := NewReleaseRuntimeAuthority(
			operationID,
			"tenant-a",
			"22222222-2222-4222-8222-222222222222",
			"https://fred.example/callbacks/provision?operation_id="+operationID.String(),
			"https://fred.example/callbacks/provision?lifecycle_id="+uuid.NewString(),
		)
		require.Error(t, err)
	})
}

func TestMaintenanceIntentRejectsOperationAndCloseOverlap(t *testing.T) {
	t.Run("operation already owns lease", func(t *testing.T) {
		releases, callbacks, source, target := maintenanceFixture(t, "operation-first")
		_ = releases
		op := testOperationIntentSpec(t, "maintenance-operation-first")
		op.LeaseUUID = source.LeaseUUID()
		_, err := beginTestOperationIntent(t, callbacks, op)
		require.NoError(t, err)
		_, err = callbacks.BeginMaintenanceIntent(newMaintenanceIntentSpec(
			t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target,
			"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
		))
		require.ErrorIs(t, err, ErrMaintenanceIntentConflict)
	})

	t.Run("maintenance already owns lease", func(t *testing.T) {
		_, callbacks, intent, _ := beginMaintenanceFixture(t, "maintenance-first")
		op := testOperationIntentSpec(t, "maintenance-second-operation")
		op.LeaseUUID = intent.LeaseUUID()
		_, err := beginTestOperationIntent(t, callbacks, op, operationIntentTestIdentity{
			backend: intent.Backend(), storageID: intent.BackendStorageID(),
		})
		require.ErrorIs(t, err, ErrOperationIntentConflict)
	})
}

func TestMaintenanceIntentPreservesTerminalOperationOnlyWithinSameAuthority(t *testing.T) {
	t.Run("matching successor preserves terminal history", func(t *testing.T) {
		_, callbacks, source, target := maintenanceFixture(t, "terminal-operation-matching-authority")
		op := testOperationIntentSpec(t, "maintenance-terminal-matching")
		op.LeaseUUID = source.LeaseUUID()
		identity, ok := target.RuntimeIdentity()
		require.True(t, ok)
		op.CallbackURL = identity.CallbackURL()
		op.LifecycleCallbackURL = identity.LifecycleCallbackURL()
		op.Tenant = identity.Tenant()
		op.ProviderUUID = identity.ProviderUUID()
		admission, err := beginTestOperationIntent(t, callbacks, op)
		require.NoError(t, err)
		claim, created := admission.CreatedClaim()
		require.True(t, created)
		claim = startCallbackOperationForTest(t, callbacks, claim)
		_, err = callbacks.ResolveOperationIntent(claim, backend.CallbackStatusSuccess, "")
		require.NoError(t, err)

		maintenance, err := callbacks.BeginMaintenanceIntent(newMaintenanceIntentSpec(
			t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target,
			"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
		))
		require.NoError(t, err)
		assert.True(t, maintenance.MaintenanceID().Valid())

		disposition, err := callbacks.ProbeOperationIntent(testOperationIntentProbe(t, callbacks, op))
		require.NoError(t, err)
		assert.Equal(t, OperationIntentAdmissionCompleted, disposition,
			"maintenance must preserve terminal operation replay authority")
		require.NoError(t, callbacks.Healthy())
		pending, err := callbacks.ListPending()
		require.NoError(t, err)
		require.Len(t, pending, 1, "maintenance must not consume the operation callback")
		assert.Equal(t, op.CallbackURL, pending[0].CallbackURL)
	})

	for _, test := range []struct {
		name   string
		mutate func(*OperationIntentSpec, *operationIntentTestIdentity)
	}{
		{
			name: "backend",
			mutate: func(_ *OperationIntentSpec, identity *operationIntentTestIdentity) {
				identity.backend = "docker-b"
			},
		},
		{
			name: "storage",
			mutate: func(_ *OperationIntentSpec, identity *operationIntentTestIdentity) {
				identity.storageID = callbackStorageID(t, "660e8400-e29b-41d4-a716-446655440000")
			},
		},
		{
			name: "tenant",
			mutate: func(spec *OperationIntentSpec, _ *operationIntentTestIdentity) {
				spec.Tenant = "tenant-b"
			},
		},
		{
			name: "provider",
			mutate: func(spec *OperationIntentSpec, _ *operationIntentTestIdentity) {
				spec.ProviderUUID = "33333333-3333-4333-8333-333333333333"
			},
		},
		{
			name: "operation",
			mutate: func(spec *OperationIntentSpec, _ *operationIntentTestIdentity) {
				spec.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
				var err error
				spec.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(spec.CallbackURL, "")
				require.NoError(t, err)
			},
		},
	} {
		t.Run("rejects different "+test.name, func(t *testing.T) {
			_, callbacks, source, target := maintenanceFixture(t, "terminal-operation-different-"+test.name)
			op := testOperationIntentSpec(t, "maintenance-terminal-different-"+test.name)
			op.LeaseUUID = source.LeaseUUID()
			identity, ok := target.RuntimeIdentity()
			require.True(t, ok)
			op.CallbackURL = identity.CallbackURL()
			op.LifecycleCallbackURL = identity.LifecycleCallbackURL()
			op.Tenant = identity.Tenant()
			op.ProviderUUID = identity.ProviderUUID()
			opIdentity := operationIntentTestIdentity{
				backend:   "docker-a",
				storageID: callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
			}
			test.mutate(&op, &opIdentity)
			admission, err := beginTestOperationIntent(t, callbacks, op, opIdentity)
			require.NoError(t, err)
			claim, created := admission.CreatedClaim()
			require.True(t, created)
			claim = startCallbackOperationForTest(t, callbacks, claim)
			_, err = callbacks.ResolveOperationIntent(claim, backend.CallbackStatusSuccess, "")
			require.NoError(t, err)

			_, err = callbacks.BeginMaintenanceIntent(newMaintenanceIntentSpec(
				t, callbacks, newTestMaintenanceID(t), MaintenanceIntentRestart, source, target,
				"docker-a", callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
			))
			require.ErrorIs(t, err, ErrMaintenanceIntentConflict)
			require.ErrorContains(t, err, "different backend storage, principal, or operation authority")

			state, lookupErr := callbacks.LookupOperationRecovery(
				testOperationIntentProbe(t, callbacks, op, opIdentity),
			)
			require.NoError(t, lookupErr)
			_, ok = state.(OperationSucceeded)
			assert.True(t, ok, "rejected maintenance must preserve predecessor outcome")
			intents, listErr := callbacks.listMaintenanceIntents()
			require.NoError(t, listErr)
			assert.Empty(t, intents)
		})
	}
}

func TestBeginCloseIntentAtomicallyPreemptsMaintenanceBeforeCloseDelivery(t *testing.T) {
	releases, callbacks, intent, _ := beginMaintenanceFixture(t, "close-preempt")
	_ = releases
	closeSpec := testCloseIntentSpec(t, "maintenance-preempt")
	closeSpec.LeaseUUID = intent.LeaseUUID()
	closeSpec.Tenant = intent.Tenant()
	closeSpec.ProviderUUID = intent.ProviderUUID()
	closeSpec.CallbackURL = intent.CallbackURL()
	closeSpec.LifecycleCallbackURL = intent.LifecycleCallbackURL()
	closeSpec.ActiveReleaseVersion = intent.SourceRelease().Version()
	closeSpec.ActiveReleaseDigest = intent.SourceRelease().Digest()
	closeSpec.ActiveReleaseOperationID = intent.TargetRelease().OperationID

	admission, err := beginUnboundCloseIntentWithLineage(
		t, callbacks, closeSpec, intent.Backend(), intent.BackendStorageID(),
	)
	require.NoError(t, err)
	assert.True(t, admission.MaintenancePreempted())
	intents, err := callbacks.listMaintenanceIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Contains(t, pending[0].Error, "preempted")
	require.NoError(t, callbacks.db.View(func(tx *bolt.Tx) error {
		receipts, receiptErr := listMaintenanceReceiptsTx(tx, intent.LeaseUUID())
		require.NoError(t, receiptErr)
		require.Len(t, receipts, 1)
		assert.Equal(t, intent.MaintenanceID(), receipts[0].MaintenanceID)
		assert.Equal(t, backend.CallbackStatusFailed, receipts[0].Status)
		return nil
	}))

	_, err = callbacks.ResolveCloseIntent(admission.Claim(), backend.CallbackStatusDeprovisioned, "", false)
	require.NoError(t, err)
	pending, err = callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Less(t, pending[0].Sequence, pending[1].Sequence)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, backend.CallbackStatusDeprovisioned, pending[1].Status)
	require.NoError(t, callbacks.db.View(func(tx *bolt.Tx) error {
		receipts, receiptErr := listMaintenanceReceiptsTx(tx, intent.LeaseUUID())
		require.NoError(t, receiptErr)
		assert.Empty(t, receipts,
			"the permanent closed head must replace live-lease maintenance receipts")
		return nil
	}))
}

func TestBeginCloseIntentRejectsMaintenanceWithoutExactSourceFence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*closeIntentSpec)
	}{
		{name: "zero fence", mutate: func(spec *closeIntentSpec) {
			spec.ActiveReleaseVersion = 0
			spec.ActiveReleaseDigest = [sha256.Size]byte{}
			spec.ActiveReleaseOperationID = OperationID{}
		}},
		{name: "stale version", mutate: func(spec *closeIntentSpec) {
			spec.ActiveReleaseVersion++
		}},
		{name: "stale digest", mutate: func(spec *closeIntentSpec) {
			spec.ActiveReleaseDigest = sha256.Sum256([]byte("stale"))
		}},
		{name: "cleanup only", mutate: func(spec *closeIntentSpec) {
			spec.CleanupOnly = true
			spec.Tenant = ""
			spec.ProviderUUID = ""
			spec.CallbackURL = ""
			spec.LifecycleCallbackURL = ""
			spec.RetainOnClose = false
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, callbacks, intent, _ := beginMaintenanceFixture(t, "close-fence-"+test.name)
			spec := testCloseIntentSpec(t, "maintenance-fence-"+test.name)
			spec.LeaseUUID = intent.LeaseUUID()
			spec.Tenant = intent.Tenant()
			spec.ProviderUUID = intent.ProviderUUID()
			spec.CallbackURL = intent.CallbackURL()
			spec.LifecycleCallbackURL = intent.LifecycleCallbackURL()
			spec.ActiveReleaseVersion = intent.SourceRelease().Version()
			spec.ActiveReleaseDigest = intent.SourceRelease().Digest()
			spec.ActiveReleaseOperationID = intent.TargetRelease().OperationID
			test.mutate(&spec)
			_, err := beginUnboundCloseIntentWithLineage(
				t, callbacks, spec, intent.Backend(), intent.BackendStorageID(),
			)
			require.ErrorContains(t, err, "does not fence the maintenance source release")
			claims, listErr := callbacks.listMaintenanceIntents()
			require.NoError(t, listErr)
			require.Len(t, claims, 1)
			closes, listErr := callbacks.ListCloseIntents()
			require.NoError(t, listErr)
			assert.Empty(t, closes)
		})
	}

	t.Run("already active target is not fabricated as failure", func(t *testing.T) {
		releases, callbacks, intent, appendClaim := beginMaintenanceFixture(t, "close-active-target")
		target, err := appendClaim.settlement.AppendMaintenance(appendClaim)
		require.NoError(t, err)
		target, err = callbacks.BindMaintenanceIntentTarget(intent, target)
		require.NoError(t, err)
		intent = target.Intent()
		require.NoError(t, activateMaintenanceForTest(t, callbacks, releases, intent, target))
		activeRelease, active, err := releases.claimLatestActive(intent.LeaseUUID())
		require.NoError(t, err)

		spec := testCloseIntentSpec(t, "maintenance-active-target")
		spec.LeaseUUID = intent.LeaseUUID()
		spec.Tenant = intent.Tenant()
		spec.ProviderUUID = intent.ProviderUUID()
		spec.CallbackURL = intent.CallbackURL()
		spec.LifecycleCallbackURL = intent.LifecycleCallbackURL()
		spec.ActiveReleaseVersion = active.Version()
		spec.ActiveReleaseDigest = active.Digest()
		spec.ActiveReleaseOperationID = activeRelease.OperationID
		_, err = beginUnboundCloseIntentWithLineage(
			t, callbacks, spec, intent.Backend(), intent.BackendStorageID(),
		)
		require.ErrorContains(t, err, "does not fence the maintenance source release")
		pending, listErr := callbacks.ListPending()
		require.NoError(t, listErr)
		assert.Empty(t, pending)
	})
}

func TestMaintenanceIntentCorruptTargetDigestFailsClosed(t *testing.T) {
	_, callbacks, intent, _ := beginMaintenanceFixture(t, "corrupt-digest")
	require.NoError(t, callbacks.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
		value := bucket.Get([]byte(intent.LeaseUUID()))
		var head storedLeaseMutationHead
		if err := json.Unmarshal(value, &head); err != nil {
			return err
		}
		head.Maintenance.TargetReleaseVersion = 2
		head.Maintenance.TargetReleaseDigest = encodeMaintenanceDigest(sha256.Sum256([]byte("wrong")))
		data, err := json.Marshal(head)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(intent.LeaseUUID()), data)
	}))
	_, err := callbacks.listMaintenanceIntents()
	require.ErrorContains(t, err, "does not match target template")
	require.Error(t, callbacks.Healthy())
}
