package shared

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"net/http"
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

type operationIntentTestIdentity struct {
	backend   string
	storageID backendidentity.ID
}

func beginTestOperationIntent(
	t *testing.T,
	store *CallbackStore,
	spec OperationIntentSpec,
	identityOverride ...operationIntentTestIdentity,
) (OperationIntentAdmission, error) {
	t.Helper()
	if store != nil && store.boltStore != nil && store.binding != nil {
		candidate, err := store.NewOperationIntentCandidate(spec)
		if err != nil {
			return OperationIntentAdmission{}, err
		}
		return store.BeginOperationIntent(candidate)
	}
	identity := operationIntentTestIdentity{
		backend: "docker-a",
		storageID: callbackStorageID(t,
			"550e8400-e29b-41d4-a716-446655440000"),
	}
	if len(identityOverride) > 0 {
		identity = identityOverride[0]
	}
	candidate, err := newOperationIntentCandidate(
		store, spec, identity.backend, identity.storageID,
	)
	if err != nil {
		return OperationIntentAdmission{}, err
	}
	return store.BeginOperationIntent(candidate)
}

func testOperationIntentProbe(
	t *testing.T,
	store *CallbackStore,
	spec OperationIntentSpec,
	identityOverride ...operationIntentTestIdentity,
) OperationIntentProbe {
	t.Helper()
	identity := operationIntentTestIdentity{
		backend: "docker-a",
		storageID: callbackStorageID(t,
			"550e8400-e29b-41d4-a716-446655440000"),
	}
	if len(identityOverride) > 0 {
		identity = identityOverride[0]
	}
	probe, err := newOperationIntentProbe(
		store, spec.LeaseUUID, spec.CallbackURL, identity.backend, identity.storageID,
	)
	require.NoError(t, err)
	return probe
}

func createdOperationClaim(
	t *testing.T,
	admission OperationIntentAdmission,
) OperationIntentClaim {
	t.Helper()
	claim, created := admission.CreatedClaim()
	require.True(t, created, "operation admission must carry first-dispatch authority")
	return claim
}

// startCallbackOperationForTest crosses the same durable not-started ->
// started lease-head transition as OperationSettlement.StartOperationExecution.
// These CallbackStore tests intentionally exercise the journal in isolation,
// so they cannot construct a release settlement or a substrate executor; they
// must nevertheless never manufacture Success from pre-effect authority.
func startCallbackOperationForTest(
	t *testing.T,
	store *CallbackStore,
	claim OperationIntentClaim,
) OperationIntentClaim {
	t.Helper()
	durable := claim
	unlock := store.lockDeliveryLease(durable.LeaseUUID())
	defer unlock()
	var refreshed OperationIntentClaim
	require.NoError(t, store.update(func(tx *bolt.Tx) error {
		if err := verifyOperationIntentTx(tx, durable); err != nil {
			return err
		}
		entry := *cloneOperationAuthority(durable.operationAuthority).entry
		entry.EffectNotStarted = false
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		next, err := decodeOperationIntent([]byte(entry.LeaseUUID), data)
		if err != nil {
			return err
		}
		transition, err := newStartOperationExecutionLeaseMutation(durable, next)
		if err != nil {
			return err
		}
		written, err := applyLeaseMutationTx(tx, transition)
		if err != nil {
			return err
		}
		refreshed = written.(operationLeaseMutationHead).claim
		return nil
	}))
	return refreshed
}

func testOperationIntentSpec(t *testing.T, name string) OperationIntentSpec {
	t.Helper()
	operationID := uuid.NewString()
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	return OperationIntentSpec{
		Kind:                 OperationIntentProvision,
		LeaseUUID:            testLeaseUUID("intent-" + name),
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleURL,
		Tenant:               "tenant-a",
		ProviderUUID:         "22222222-2222-4222-8222-222222222222",
		Items:                []backend.LeaseItem{{SKU: "small", ServiceName: "app", Quantity: 1}},
		ResourceProfiles: []SKUResourceSnapshot{{
			SKU: "small", CPUCores: 1, MemoryMB: 512, DiskMB: 1024,
		}},
		Manifest: []byte(`{"services":{"app":{"image":"example.invalid/app:1"}}}`),
	}
}

func callbackEntryForOperationSpec(
	spec OperationIntentSpec,
	status backend.CallbackStatus,
) CallbackEntry {
	return CallbackEntry{
		LeaseUUID:        spec.LeaseUUID,
		CallbackURL:      spec.CallbackURL,
		DeliveryKind:     CallbackDeliveryKindOperation,
		Status:           status,
		Backend:          "docker-a",
		BackendStorageID: "550e8400-e29b-41d4-a716-446655440000",
		CreatedAt:        time.Now(),
	}
}

func TestOperationIDValid(t *testing.T) {
	t.Parallel()
	legacy, err := parseOperationCallbackID("https://fred.example/callbacks/provision")
	require.NoError(t, err)
	assert.True(t, legacy.IsZero(), "only an absent query token denotes v0.13 compatibility")

	tests := []struct {
		name string
		wire string
		want bool
	}{
		{
			name: "canonical RFC 4122 UUIDv4",
			wire: "11111111-1111-4111-8111-111111111111",
			want: true,
		},
		{
			name: "empty legacy value",
		},
		{
			name: "malformed",
			wire: "not-a-uuid",
		},
		{
			name: "noncanonical uppercase",
			wire: "11111111-1111-4111-8111-11111111111A",
		},
		{
			name: "wrong version",
			wire: "11111111-1111-3111-8111-111111111111",
		},
		{
			name: "v4 bits with non-RFC variant",
			wire: "11111111-1111-4111-c111-111111111111",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			id, err := parseOperationCallbackID(
				"https://fred.example/callbacks/provision?operation_id=" + test.wire,
			)
			assert.Equal(t, test.want, err == nil && id.Valid())
		})
	}
}

func TestOperationOutcomeSurvivesCallbackDeliveryAndRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	spec := testOperationIntentSpec(t, "restart")
	admission, err := beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	require.Equal(t, OperationIntentAdmissionCreated, admission.Disposition())
	require.NoError(t, store.Close())

	store, err = newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	started := startCallbackOperationForTest(t, store, intents[0])
	completion, err := store.ResolveOperationIntent(started, backend.CallbackStatusSuccess, "")
	require.NoError(t, err)
	intents, err = store.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, spec.CallbackURL, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	state, err := store.LookupOperationRecovery(testOperationIntentProbe(t, store, spec))
	require.NoError(t, err)
	succeeded, ok := state.(OperationSucceeded)
	require.True(t, ok)
	assert.False(t, succeeded.SettledAt().IsZero())

	require.NoError(t, store.removeEntry(completion))
	require.NoError(t, store.Close())
	store, err = newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	state, err = store.LookupOperationRecovery(testOperationIntentProbe(t, store, spec))
	require.NoError(t, err)
	_, ok = state.(OperationSucceeded)
	assert.True(t, ok, "terminal success must outlive outbox delivery and process restart")
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "delivered callback must leave the FIFO while its outcome remains")
	disposition, err := store.ProbeOperationIntent(testOperationIntentProbe(t, store, spec))
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionCompleted, disposition)
}

func TestOperationIntentCandidateIsStoreBoundAndDetached(t *testing.T) {
	dbPath, storage := initializeBoundCallbackStore(t)
	store, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: dbPath}, storage, newTestStorageAuthorityGate(t),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	spec := testOperationIntentSpec(t, "candidate-detached")
	spec.EffectiveItems = slices.Clone(spec.Items)
	spec.HealthCheckServices = []string{"app"}
	wantItems := slices.Clone(spec.Items)
	wantProfiles := CloneSKUResourceSnapshot(spec.ResourceProfiles)
	wantEffective := slices.Clone(spec.EffectiveItems)
	wantHealth := slices.Clone(spec.HealthCheckServices)
	wantManifest := bytes.Clone(spec.Manifest)
	candidate, err := store.NewOperationIntentCandidate(spec)
	require.NoError(t, err)

	spec.Items[0].SKU = "mutated"
	spec.ResourceProfiles[0].MemoryMB++
	spec.EffectiveItems[0].ServiceName = "mutated"
	spec.HealthCheckServices[0] = "mutated"
	spec.Manifest[0] = '['

	admission, err := store.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim := createdOperationClaim(t, admission)
	assert.Equal(t, wantItems, claim.Items())
	assert.Equal(t, wantProfiles, claim.ResourceProfiles())
	assert.Equal(t, wantEffective, claim.EffectiveItems())
	assert.Equal(t, wantHealth, claim.HealthCheckServices())
	assert.Equal(t, wantManifest, claim.Manifest())
	assert.Equal(t, storage.BackendName(), claim.Backend())
	assert.Equal(t, storage.ID(), claim.BackendStorageID())
}

func TestOperationIntentCapabilitiesRejectZeroAndCrossStoreUse(t *testing.T) {
	unbound, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "unbound-callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, unbound.Close()) })
	unboundSpec := testOperationIntentSpec(t, "unbound-store")
	_, err = unbound.NewOperationIntentCandidate(unboundSpec)
	require.ErrorContains(t, err, "identity-bound callback journal")
	_, err = unbound.NewOperationIntentProbe(unboundSpec.LeaseUUID, unboundSpec.CallbackURL)
	require.ErrorContains(t, err, "identity-bound callback journal")

	pathA, storageA := initializeBoundCallbackStore(t)
	storeA, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: pathA}, storageA, newTestStorageAuthorityGate(t),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, storeA.Close()) })
	pathB, storageB := initializeBoundCallbackStore(t)
	storeB, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: pathB}, storageB, newTestStorageAuthorityGate(t),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, storeB.Close()) })

	_, err = storeA.BeginOperationIntent(OperationIntentCandidate{})
	require.ErrorContains(t, err, "was not minted")
	_, err = storeA.ProbeOperationIntent(OperationIntentProbe{})
	require.ErrorContains(t, err, "was not minted")
	_, err = storeA.LookupOperationRecovery(OperationIntentProbe{})
	require.ErrorContains(t, err, "was not minted")

	spec := testOperationIntentSpec(t, "candidate-cross-store")
	candidate, err := storeA.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	_, err = storeB.BeginOperationIntent(candidate)
	require.ErrorContains(t, err, "was not minted by this callback journal")
	probe, err := storeA.NewOperationIntentProbe(spec.LeaseUUID, spec.CallbackURL)
	require.NoError(t, err)
	_, err = storeB.ProbeOperationIntent(probe)
	require.ErrorContains(t, err, "was not minted by this callback journal")
	_, err = storeB.LookupOperationRecovery(probe)
	require.ErrorContains(t, err, "was not minted by this callback journal")

	intents, err := storeA.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	intents, err = storeB.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
}

func TestOperationIntentCapabilitiesDoNotSurviveStoreReopen(t *testing.T) {
	dbPath, storage := initializeBoundCallbackStore(t)
	issuer, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: dbPath}, storage, newTestStorageAuthorityGate(t),
	)
	require.NoError(t, err)
	spec := testOperationIntentSpec(t, "reopened-store")
	candidate, err := issuer.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	probe, err := issuer.NewOperationIntentProbe(spec.LeaseUUID, spec.CallbackURL)
	require.NoError(t, err)
	require.NoError(t, issuer.Close())

	reopened, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: dbPath}, storage, newTestStorageAuthorityGate(t),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	_, err = reopened.BeginOperationIntent(candidate)
	require.ErrorContains(t, err, "was not minted by this callback journal")
	_, err = reopened.ProbeOperationIntent(probe)
	require.ErrorContains(t, err, "was not minted by this callback journal")
	_, err = reopened.LookupOperationRecovery(probe)
	require.ErrorContains(t, err, "was not minted by this callback journal")
}

func TestOperationIntentReplayAdmissionsCarryNoDispatchAuthority(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testOperationIntentSpec(t, "replay-capability-free")
	created, err := beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	claim := createdOperationClaim(t, created)

	existing, err := beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	require.Equal(t, OperationIntentAdmissionExisting, existing.Disposition())
	_, hasDispatchAuthority := existing.CreatedClaim()
	assert.False(t, hasDispatchAuthority)

	claim = startCallbackOperationForTest(t, store, claim)
	_, err = store.ResolveOperationIntent(claim, backend.CallbackStatusSuccess, "")
	require.NoError(t, err)
	completed, err := beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	require.Equal(t, OperationIntentAdmissionCompleted, completed.Disposition())
	_, hasDispatchAuthority = completed.CreatedClaim()
	assert.False(t, hasDispatchAuthority)
}

func TestFailedOperationOutcomeIsTypedAndDurable(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testOperationIntentSpec(t, "durable-failure")
	admission, err := beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	_, err = store.ResolveOperationIntent(
		createdOperationClaim(t, admission), backend.CallbackStatusFailed, "substrate refused",
	)
	require.NoError(t, err)

	state, err := store.LookupOperationRecovery(testOperationIntentProbe(t, store, spec))
	require.NoError(t, err)
	failed, ok := state.(OperationFailed)
	require.True(t, ok)
	assert.Equal(t, "substrate refused", failed.Error())
	assert.False(t, failed.SettledAt().IsZero())
}

func TestOperationIntentRecoverySurvivesWallClockRollback(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	spec := testOperationIntentSpec(t, "future-after-clock-rollback")
	_, err = beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	futureCreatedAt := time.Now().Add(24 * time.Hour)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
		key := []byte(spec.LeaseUUID)
		var head storedLeaseMutationHead
		if unmarshalErr := json.Unmarshal(bucket.Get(key), &head); unmarshalErr != nil {
			return unmarshalErr
		}
		head.Operation.CreatedAt = futureCreatedAt
		data, marshalErr := json.Marshal(head)
		if marshalErr != nil {
			return marshalErr
		}
		return bucket.Put(key, data)
	}))
	require.NoError(t, store.Close())

	store, err = newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Equal(t, futureCreatedAt.UnixNano(), intents[0].CreatedAt().UnixNano())
	disposition, err := store.ProbeOperationIntent(testOperationIntentProbe(t, store, spec))
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionExisting, disposition)
	require.NoError(t, store.Healthy())

	started := startCallbackOperationForTest(t, store, intents[0])
	_, err = store.ResolveOperationIntent(started, backend.CallbackStatusSuccess, "")
	require.NoError(t, err)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, spec.CallbackURL, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
}

func TestOperationIntentAdmissionIsIdempotentAndConflictsByExactAuthority(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testOperationIntentSpec(t, "idempotent")
	first, err := beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	require.Equal(t, OperationIntentAdmissionCreated, first.Disposition())
	second, err := beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionExisting, second.Disposition())

	conflict := spec
	conflict.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	conflict.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(conflict.CallbackURL, "")
	require.NoError(t, err)
	_, err = beginTestOperationIntent(t, store, conflict)
	require.ErrorIs(t, err, ErrOperationIntentConflict)
}

func TestProbeOperationIntentRecognizesExactAcceptedAndCompletedRedelivery(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testOperationIntentSpec(t, "probe")
	probe := testOperationIntentProbe(t, store, spec)

	disposition, err := store.ProbeOperationIntent(probe)
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionNone, disposition)
	admission, err := beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	disposition, err = store.ProbeOperationIntent(probe)
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionExisting, disposition)

	conflictSpec := spec
	conflictSpec.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	conflict := testOperationIntentProbe(t, store, conflictSpec)
	_, err = store.ProbeOperationIntent(conflict)
	require.ErrorIs(t, err, ErrOperationIntentConflict)

	_, err = store.ResolveOperationIntent(createdOperationClaim(t, admission), backend.CallbackStatusFailed, "refused")
	require.NoError(t, err)
	disposition, err = store.ProbeOperationIntent(probe)
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionCompleted, disposition)
}

func TestOperationIntentTerminalSuccessorWaitsForCallbackDelivery(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	predecessor := testOperationIntentSpec(t, "terminal-successor")
	admission, err := beginTestOperationIntent(t, store, predecessor)
	require.NoError(t, err)
	completion, err := store.ResolveOperationIntent(
		createdOperationClaim(t, admission), backend.CallbackStatusFailed, "predecessor failed",
	)
	require.NoError(t, err)

	successor := predecessor
	successor.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	successor.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(successor.CallbackURL, "")
	require.NoError(t, err)
	successorProbe := testOperationIntentProbe(t, store, successor)

	_, err = store.ProbeOperationIntent(successorProbe)
	require.ErrorIs(t, err, ErrOperationIntentConflict)
	_, err = beginTestOperationIntent(t, store, successor)
	require.ErrorIs(t, err, ErrOperationIntentConflict)

	require.NoError(t, store.removeEntry(completion))
	disposition, err := store.ProbeOperationIntent(successorProbe)
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionNone, disposition)
	next, err := beginTestOperationIntent(t, store, successor)
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionCreated, next.Disposition())
	assert.Equal(t, successor.CallbackURL, createdOperationClaim(t, next).CallbackURL())
}

func TestOperationIntentTerminalSuccessorRequiresSameDurableAuthority(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	predecessor := testOperationIntentSpec(t, "terminal-authority")
	admission, err := beginTestOperationIntent(t, store, predecessor)
	require.NoError(t, err)
	started := startCallbackOperationForTest(t, store, createdOperationClaim(t, admission))
	completion, err := store.ResolveOperationIntent(started, backend.CallbackStatusSuccess, "")
	require.NoError(t, err)
	require.NoError(t, store.removeEntry(completion))

	differentStorage := predecessor
	differentStorage.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	differentStorage.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(differentStorage.CallbackURL, "")
	require.NoError(t, err)
	foreignIdentity := operationIntentTestIdentity{
		backend:   "docker-a",
		storageID: callbackStorageID(t, "660e8400-e29b-41d4-a716-446655440000"),
	}
	_, err = store.ProbeOperationIntent(testOperationIntentProbe(t, store, differentStorage, foreignIdentity))
	require.ErrorIs(t, err, ErrOperationIntentConflict)
	_, err = beginTestOperationIntent(t, store, differentStorage, foreignIdentity)
	require.ErrorIs(t, err, ErrOperationIntentConflict)

	differentPrincipal := predecessor
	differentPrincipal.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	differentPrincipal.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(differentPrincipal.CallbackURL, "")
	require.NoError(t, err)
	differentPrincipal.Tenant = "tenant-b"
	_, err = beginTestOperationIntent(t, store, differentPrincipal)
	require.ErrorIs(t, err, ErrOperationIntentConflict)
}

func TestOperationCompletionHistoryPreservesAThroughBRestartAndClose(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	operationA := testOperationIntentSpec(t, "history-a-b")
	admissionA, err := beginTestOperationIntent(t, store, operationA)
	require.NoError(t, err)
	completionA, err := store.ResolveOperationIntent(
		createdOperationClaim(t, admissionA), backend.CallbackStatusFailed, "operation A failed",
	)
	require.NoError(t, err)
	require.NoError(t, store.removeEntry(completionA))

	operationB := operationA
	operationB.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	operationB.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(operationB.CallbackURL, "")
	require.NoError(t, err)
	admissionB, err := beginTestOperationIntent(t, store, operationB)
	require.NoError(t, err)
	require.Equal(t, OperationIntentAdmissionCreated, admissionB.Disposition())
	startedB := startCallbackOperationForTest(t, store, createdOperationClaim(t, admissionB))
	completionB, err := store.ResolveOperationIntent(startedB, backend.CallbackStatusSuccess, "")
	require.NoError(t, err)
	require.NoError(t, store.removeEntry(completionB))
	require.NoError(t, store.Close())

	store, err = newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	failedReceipts, err := store.ListFailedOperationReceipts()
	require.NoError(t, err)
	require.Len(t, failedReceipts, 1)
	assert.Equal(t, operationA.LeaseUUID, failedReceipts[0].LeaseUUID())
	assert.Equal(t, operationA.CallbackURL, failedReceipts[0].CallbackURL())
	assert.Equal(t, "operation A failed", failedReceipts[0].Error())
	probeA := testOperationIntentProbe(t, store, operationA)
	disposition, err := store.ProbeOperationIntent(probeA)
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionCompleted, disposition)
	replayedA, err := beginTestOperationIntent(t, store, operationA)
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionCompleted, replayedA.Disposition())
	_, replayAuthority := replayedA.CreatedClaim()
	assert.False(t, replayAuthority, "historical completion must not mint mutation authority")

	operationAID, err := parseOperationCallbackID(operationA.CallbackURL)
	require.NoError(t, err)
	divergentA := operationA
	divergentA.CallbackURL = "https://rotated.example/callbacks/provision?operation_id=" + operationAID.String()
	divergentA.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(divergentA.CallbackURL, "")
	require.NoError(t, err)
	_, err = store.ProbeOperationIntent(testOperationIntentProbe(t, store, divergentA))
	require.ErrorIs(t, err, ErrOperationIntentConflict)
	_, err = beginTestOperationIntent(t, store, divergentA)
	require.ErrorIs(t, err, ErrOperationIntentConflict)

	operationBID, err := parseOperationCallbackID(operationB.CallbackURL)
	require.NoError(t, err)
	closeSpec := testCloseIntentSpec(t, "history-a-b")
	closeSpec.LeaseUUID = operationB.LeaseUUID
	closeSpec.Tenant = operationB.Tenant
	closeSpec.ProviderUUID = operationB.ProviderUUID
	closeSpec.Items = slices.Clone(operationB.Items)
	closeSpec.ResourceProfiles = CloneSKUResourceSnapshot(operationB.ResourceProfiles)
	closeSpec.Manifest = bytes.Clone(operationB.Manifest)
	closeSpec.CallbackURL = operationB.CallbackURL
	closeSpec.LifecycleCallbackURL = operationB.LifecycleCallbackURL
	closeSpec.ActiveReleaseOperationID = operationBID
	closeAdmission, err := beginUnboundCloseIntentWithLineage(
		t, store, closeSpec, "docker-a",
		callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
	)
	require.NoError(t, err)
	_, err = store.ResolveCloseIntent(
		closeAdmission.Claim(), backend.CallbackStatusDeprovisioned, "", false,
	)
	require.NoError(t, err)

	_, err = store.ProbeOperationIntent(probeA)
	require.ErrorIs(t, err, ErrOperationIntentConflict,
		"the permanent closed head replaces per-operation replay history")
	_, err = beginTestOperationIntent(t, store, operationA)
	require.ErrorIs(t, err, ErrOperationIntentConflict,
		"a closed lease must never regain mutation authority from historical completion")
	failedReceipts, err = store.ListFailedOperationReceipts()
	require.NoError(t, err)
	assert.Empty(t, failedReceipts,
		"successful close must release per-operation capacity behind the stronger closed fence")
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		count, countErr := callbackReceiptReservationCountTx(tx)
		require.NoError(t, countErr)
		assert.Zero(t, count)
		return nil
	}))
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
}

func TestOperationCompletionHistoryRejectsUnsupportedSchema(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, []byte) []byte
		want   string
	}{
		{
			name: "missing version",
			mutate: func(t *testing.T, raw []byte) []byte {
				t.Helper()
				mutated := bytes.Replace(raw, []byte(`"version":1,`), nil, 1)
				require.NotEqual(t, raw, mutated)
				return mutated
			},
			want: "unsupported version 0",
		},
		{
			name: "future version",
			mutate: func(t *testing.T, raw []byte) []byte {
				t.Helper()
				mutated := bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":2`), 1)
				require.NotEqual(t, raw, mutated)
				return mutated
			},
			want: "unsupported version 2",
		},
		{
			name: "unknown field",
			mutate: func(t *testing.T, raw []byte) []byte {
				t.Helper()
				return append(bytes.TrimSuffix(bytes.Clone(raw), []byte("}")),
					[]byte(`,"future_authority":true}`)...)
			},
			want: "unknown field",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })

			predecessor := testOperationIntentSpec(t, "receipt-schema-"+test.name)
			admission, err := beginTestOperationIntent(t, store, predecessor)
			require.NoError(t, err)
			completion, err := store.ResolveOperationIntent(
				createdOperationClaim(t, admission),
				backend.CallbackStatusFailed,
				"operation failed",
			)
			require.NoError(t, err)
			require.NoError(t, store.removeEntry(completion))

			successor := predecessor
			successor.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
			successor.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(successor.CallbackURL, "")
			require.NoError(t, err)
			_, err = beginTestOperationIntent(t, store, successor)
			require.NoError(t, err, "the successor transition must archive the predecessor")

			operationID, err := parseOperationCallbackID(predecessor.CallbackURL)
			require.NoError(t, err)
			historyKey := operationHistoryKey(operationID, predecessor.CallbackURL)
			var poisoned []byte
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				leaseBucket := tx.Bucket(callbackOperationHistoryBucketName).
					Bucket([]byte(predecessor.LeaseUUID))
				require.NotNil(t, leaseBucket)
				raw := bytes.Clone(leaseBucket.Get(historyKey[:]))
				require.NotEmpty(t, raw)
				var persisted operationCompletionRecord
				require.NoError(t, json.Unmarshal(raw, &persisted))
				assert.Equal(t, operationCompletionRecordVersion, persisted.Version)
				poisoned = test.mutate(t, raw)
				return leaseBucket.Put(historyKey[:], poisoned)
			}))

			_, err = store.ListFailedOperationReceipts()
			require.ErrorContains(t, err, test.want)
			require.ErrorContains(t, store.Healthy(), test.want)
			require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
				stored := tx.Bucket(callbackOperationHistoryBucketName).
					Bucket([]byte(predecessor.LeaseUUID)).Get(historyKey[:])
				assert.Equal(t, poisoned, stored,
					"unsupported completion evidence must remain quarantined")
				return nil
			}))
		})
	}
}

func TestOperationIntentRejectsOversizeBeforePersisting(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testOperationIntentSpec(t, "oversize")
	spec.Manifest = []byte(strings.Repeat("x", maxOperationIntentEntryBytes))
	_, err = beginTestOperationIntent(t, store, spec)
	require.ErrorContains(t, err, "exceeds")
	intents, listErr := store.ListOperationIntents()
	require.NoError(t, listErr)
	assert.Empty(t, intents)
}

func TestOperationIntentAdmissionReservesTerminalSettlementHeadroom(t *testing.T) {
	t.Run("near-budget intent can settle with near-limit failure", func(t *testing.T) {
		store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })

		spec := testOperationIntentSpec(t, "terminal-headroom")
		spec.Manifest = append(bytes.Clone(spec.Manifest), bytes.Repeat([]byte(" "), 2_350_000)...)
		admission, err := beginTestOperationIntent(t, store, spec)
		require.NoError(t, err)
		_, err = store.ResolveOperationIntent(
			createdOperationClaim(t, admission),
			backend.CallbackStatusFailed,
			strings.Repeat("e", maxCallbackEntryBytes-2_048),
		)
		require.NoError(t, err)
	})

	t.Run("row that consumes settlement reserve is rejected", func(t *testing.T) {
		store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })

		spec := testOperationIntentSpec(t, "terminal-headroom-rejected")
		spec.Manifest = append(bytes.Clone(spec.Manifest), bytes.Repeat([]byte(" "), 2_370_000)...)
		_, err = beginTestOperationIntent(t, store, spec)
		require.ErrorContains(t, err, "reserves terminal settlement headroom")
		intents, listErr := store.ListOperationIntents()
		require.NoError(t, listErr)
		assert.Empty(t, intents)
	})
}

func TestOperationReceiptCapacityIsReservedExactlyOnceAndRollsBack(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	spec := testOperationIntentSpec(t, "receipt-reservation")
	admission, err := beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	require.Equal(t, OperationIntentAdmissionCreated, admission.Disposition())
	claim := createdOperationClaim(t, admission)

	retry, err := beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionExisting, retry.Disposition())
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		count, countErr := callbackReceiptReservationCountTx(tx)
		require.NoError(t, countErr)
		assert.Equal(t, uint64(1), count,
			"an exact retry must reuse rather than reserve another receipt")
		return nil
	}))
	require.NoError(t, store.Healthy())

	err = store.db.Update(func(tx *bolt.Tx) error {
		return reserveOperationReceiptWithinLimitsTx(
			tx, *claim.entry, 0, maxCallbackReceiptReservationsGlobal,
		)
	})
	require.ErrorIs(t, err, ErrOperationReceiptCapacity)
	var leaseCapacityErr *OperationReceiptCapacityError
	require.ErrorAs(t, err, &leaseCapacityErr)
	assert.Equal(t, spec.LeaseUUID, leaseCapacityErr.LeaseUUID)
	assert.Zero(t, leaseCapacityErr.Limit)

	forcedRollback := errors.New("force receipt reservation rollback")
	err = store.db.Update(func(tx *bolt.Tx) error {
		require.NoError(t, reserveOperationReceiptWithinLimitsTx(
			tx, *claim.entry, maxOperationReceiptsPerLease, maxCallbackReceiptReservationsGlobal,
		))
		return forcedRollback
	})
	require.ErrorIs(t, err, forcedRollback)
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		count, countErr := callbackReceiptReservationCountTx(tx)
		require.NoError(t, countErr)
		assert.Equal(t, uint64(1), count,
			"bbolt rollback must undo the reservation with the rejected head write")
		return nil
	}))
}

func TestOperationReceiptCapacityRefusesBeforePublishingHead(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackLeaseMutationHeadBucketName).
			SetSequence(maxCallbackReceiptReservationsGlobal)
	}))

	spec := testOperationIntentSpec(t, "receipt-capacity")
	_, err = beginTestOperationIntent(t, store, spec)
	require.ErrorIs(t, err, ErrOperationReceiptCapacity)
	require.ErrorIs(t, err, backend.ErrInsufficientResources,
		"pre-side-effect journal exhaustion must use the coded capacity protocol")
	var capacityErr *OperationReceiptCapacityError
	require.ErrorAs(t, err, &capacityErr)
	assert.Empty(t, capacityErr.LeaseUUID)
	assert.Equal(t, maxCallbackReceiptReservationsGlobal, capacityErr.Limit)

	intents, listErr := store.ListOperationIntents()
	require.NoError(t, listErr)
	assert.Empty(t, intents, "capacity refusal must not publish mutation authority")
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		slots := tx.Bucket(callbackLeaseMutationUUIDSlotBucketName)
		assert.Nil(t, slots.Get([]byte(spec.LeaseUUID)),
			"a later receipt-capacity refusal must roll back the first UUID reservation")
		assert.Zero(t, slots.Sequence())
		count, countErr := callbackReceiptReservationCountTx(tx)
		require.NoError(t, countErr)
		assert.Equal(t, maxCallbackReceiptReservationsGlobal, count)
		return nil
	}))
}

func TestLeaseMutationUUIDCapacityRefusesOperationAdmission(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackLeaseMutationUUIDSlotBucketName).
			SetSequence(maxLeaseMutationUUIDSlotsGlobal)
	}))

	spec := testOperationIntentSpec(t, "uuid-capacity")
	_, err = beginTestOperationIntent(t, store, spec)
	require.ErrorIs(t, err, ErrLeaseMutationCapacity)
	require.ErrorIs(t, err, backend.ErrCapacityRefused)
	require.ErrorIs(t, err, backend.ErrInsufficientResources)

	intents, listErr := store.ListOperationIntents()
	require.NoError(t, listErr)
	assert.Empty(t, intents, "capacity refusal must not publish operation authority")
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		slots := tx.Bucket(callbackLeaseMutationUUIDSlotBucketName)
		assert.Nil(t, slots.Get([]byte(spec.LeaseUUID)))
		return nil
	}))
}

func TestResolveOperationIntentRejectsZeroClaim(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	_, err = store.ResolveOperationIntent(OperationIntentClaim{}, backend.CallbackStatusSuccess, "")
	require.ErrorContains(t, err, "has no durable capability")

	spec := testOperationIntentSpec(t, "wrapped-claim")
	_, err = beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	intents, listErr := store.ListOperationIntents()
	require.NoError(t, listErr)
	require.Len(t, intents, 1, "rejecting a zero claim must preserve the precise pending claim")
}

func TestOperationIntentRejectsNonCanonicalProviderBeforePersisting(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	spec := testOperationIntentSpec(t, "noncanonical-provider")
	spec.ProviderUUID = "not-a-provider-uuid"
	_, err = beginTestOperationIntent(t, store, spec)
	require.ErrorContains(t, err, "provider UUID is not canonical")

	intents, listErr := store.ListOperationIntents()
	require.NoError(t, listErr)
	assert.Empty(t, intents, "invalid provider authority must not be persisted")
}

func TestOperationIntentRecoveryRejectsPersistedNonCanonicalProvider(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	spec := testOperationIntentSpec(t, "persisted-noncanonical-provider")
	_, err = beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)

	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
		key := []byte(spec.LeaseUUID)
		var head storedLeaseMutationHead
		require.NoError(t, json.Unmarshal(bucket.Get(key), &head))
		head.Operation.ProviderUUID = "not-a-provider-uuid"
		data, marshalErr := json.Marshal(head)
		require.NoError(t, marshalErr)
		return bucket.Put(key, data)
	}))

	_, err = store.ListOperationIntents()
	require.ErrorContains(t, err, "provider UUID is not canonical")
	require.ErrorContains(t, store.Healthy(), "provider UUID is not canonical")
}

func TestOperationIntentRecoveryRejectsPersistedNonCanonicalOperationID(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	spec := testOperationIntentSpec(t, "persisted-noncanonical-operation")
	_, err = beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	operationID, err := parseOperationCallbackID(spec.CallbackURL)
	require.NoError(t, err)

	var corrupt []byte
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
		key := []byte(spec.LeaseUUID)
		raw := bucket.Get(key)
		corrupt = bytes.Replace(
			raw,
			[]byte(`"operation_id":"`+operationID.String()+`"`),
			[]byte(`"operation_id":"`+strings.ToUpper(operationID.String())+`"`),
			1,
		)
		require.NotEqual(t, raw, corrupt, "fixture must replace the durable operation ID")
		return bucket.Put(key, corrupt)
	}))

	_, err = store.ListOperationIntents()
	require.ErrorContains(t, err, "canonical UUIDv4")
	require.ErrorContains(t, store.Healthy(), "canonical UUIDv4")
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		stored := tx.Bucket(callbackLeaseMutationHeadBucketName).Get([]byte(spec.LeaseUUID))
		assert.Equal(t, corrupt, stored, "corrupt authority must remain quarantined")
		return nil
	}))
}

func TestOperationIntentOversizedCompletionPreservesAtomicIntent(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testOperationIntentSpec(t, "oversized-completion")
	admission, err := beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)

	_, err = store.ResolveOperationIntent(
		createdOperationClaim(t, admission),
		backend.CallbackStatusFailed,
		strings.Repeat("x", maxCallbackEntryBytes),
	)
	require.ErrorContains(t, err, "callback entry exceeds")
	intents, listErr := store.ListOperationIntents()
	require.NoError(t, listErr)
	require.Len(t, intents, 1, "failed callback insertion must not consume recovery authority")
	admittedClaim := createdOperationClaim(t, admission)
	recoveredClaim := intents[0]
	assert.Equal(t, admittedClaim.entry.IntentID, recoveredClaim.entry.IntentID)
	assert.Equal(t, admittedClaim.digest, recoveredClaim.digest)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending, "the oversized callback and terminal transition must roll back together")

	_, err = store.ResolveOperationIntent(
		intents[0], backend.CallbackStatusFailed, "bounded failure",
	)
	require.NoError(t, err)
	intents, listErr = store.ListOperationIntents()
	require.NoError(t, listErr)
	assert.Empty(t, intents)
	pending, listErr = store.ListPending()
	require.NoError(t, listErr)
	require.Len(t, pending, 1)
	assert.Equal(t, "bounded failure", pending[0].Error)
}

func TestOperationIntentRejectsInvalidManifestAuthorityBeforePersisting(t *testing.T) {
	tests := []struct {
		name     string
		manifest []byte
		wantErr  string
	}{
		{
			name:     "malformed",
			manifest: []byte(`{"services":`),
			wantErr:  "callback operation intent manifest",
		},
		{
			name:     "topology mismatch",
			manifest: []byte(`{"services":{"worker":{"image":"example.invalid/worker:1"}}}`),
			wantErr:  "callback operation intent manifest topology",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })

			spec := testOperationIntentSpec(t, "invalid-manifest-"+test.name)
			spec.Manifest = test.manifest
			_, err = beginTestOperationIntent(t, store, spec)
			require.ErrorContains(t, err, test.wantErr)

			intents, listErr := store.ListOperationIntents()
			require.NoError(t, listErr)
			assert.Empty(t, intents, "invalid recovery authority must not be persisted")
		})
	}
}

func TestOperationIntentHealthAndRecoveryRejectPersistedManifestAuthority(t *testing.T) {
	tests := []struct {
		name     string
		manifest []byte
		wantErr  string
	}{
		{
			name:     "malformed",
			manifest: []byte(`{"services":`),
			wantErr:  "callback operation intent manifest",
		},
		{
			name:     "topology mismatch",
			manifest: []byte(`{"services":{"worker":{"image":"example.invalid/worker:1"}}}`),
			wantErr:  "callback operation intent manifest topology",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })

			spec := testOperationIntentSpec(t, "persisted-invalid-manifest-"+test.name)
			_, err = beginTestOperationIntent(t, store, spec)
			require.NoError(t, err)

			var corrupt []byte
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
				key := []byte(spec.LeaseUUID)
				var head storedLeaseMutationHead
				if err := json.Unmarshal(bucket.Get(key), &head); err != nil {
					return err
				}
				head.Operation.Manifest = test.manifest
				corrupt, err = json.Marshal(head)
				if err != nil {
					return err
				}
				return bucket.Put(key, corrupt)
			}))

			_, err = store.ListOperationIntents()
			require.ErrorContains(t, err, test.wantErr)
			require.ErrorContains(t, store.Healthy(), test.wantErr)
			require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
				stored := tx.Bucket(callbackLeaseMutationHeadBucketName).Get([]byte(spec.LeaseUUID))
				assert.Equal(t, corrupt, stored, "invalid causal evidence must remain quarantined")
				return nil
			}))
		})
	}
}

func TestOperationIntentRejectsNestedDuplicateAuthorityFields(t *testing.T) {
	for _, test := range []struct {
		name   string
		prefix string
	}{
		{name: "desired item", prefix: `"items":[{"sku":"small"`},
		{name: "effective item", prefix: `"effective_items":[{"sku":"small"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := testOperationIntentSpec(t, "nested-duplicate-"+test.name)
			_, err = beginTestOperationIntent(t, store, spec)
			require.NoError(t, err)

			var corrupt []byte
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
				key := []byte(spec.LeaseUUID)
				raw := bucket.Get(key)
				corrupt = bytes.Replace(raw, []byte(test.prefix),
					[]byte(test.prefix+`,"sku":"large"`), 1)
				require.NotEqual(t, raw, corrupt, "fixture must target the nested item object")
				return bucket.Put(key, corrupt)
			}))

			_, err = store.ListOperationIntents()
			require.ErrorContains(t, err, `duplicate field "sku"`)
			require.ErrorContains(t, store.Healthy(), `duplicate field "sku"`)
			require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
				stored := tx.Bucket(callbackLeaseMutationHeadBucketName).Get([]byte(spec.LeaseUUID))
				assert.Equal(t, corrupt, stored, "corrupt causal evidence must remain quarantined")
				return nil
			}))
		})
	}
}

func TestOperationIntentRejectsUnboundedPersistedQuantitiesBeforeRecovery(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*operationIntentEntry)
		want   string
	}{
		{
			name: "single item max int",
			mutate: func(entry *operationIntentEntry) {
				entry.Items[0].Quantity = math.MaxInt
				entry.EffectiveItems[0].Quantity = math.MaxInt
			},
			want: "out of range",
		},
		{
			name: "aggregate above recovery bound",
			mutate: func(entry *operationIntentEntry) {
				entry.Items = []backend.LeaseItem{
					{SKU: "small", ServiceName: "app", Quantity: 600},
					{SKU: "small", ServiceName: "worker", Quantity: 600},
				}
				entry.EffectiveItems = append([]backend.LeaseItem(nil), entry.Items...)
			},
			want: "total quantity exceeds",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := testOperationIntentSpec(t, "unbounded-quantity-"+test.name)
			_, err = beginTestOperationIntent(t, store, spec)
			require.NoError(t, err)

			var corrupt []byte
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
				key := []byte(spec.LeaseUUID)
				var head storedLeaseMutationHead
				require.NoError(t, json.Unmarshal(bucket.Get(key), &head))
				test.mutate(head.Operation)
				corrupt, err = json.Marshal(head)
				require.NoError(t, err)
				return bucket.Put(key, corrupt)
			}))

			_, err = store.ListOperationIntents()
			require.ErrorContains(t, err, test.want)
			require.ErrorContains(t, store.Healthy(), test.want)
			require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
				stored := tx.Bucket(callbackLeaseMutationHeadBucketName).Get([]byte(spec.LeaseUUID))
				assert.Equal(t, corrupt, stored,
					"unsafe causal evidence must remain quarantined, not be iterated or discarded")
				return nil
			}))
		})
	}
}

func TestOperationIntentRejectsUnsafePersistedCallbackDestination(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*operationIntentEntry)
		want   string
	}{
		{
			name: "port-only authority",
			mutate: func(entry *operationIntentEntry) {
				entry.CallbackURL = strings.Replace(entry.CallbackURL, "fred.example", ":443", 1)
				entry.LifecycleCallbackURL = strings.Replace(entry.LifecycleCallbackURL, "fred.example", ":443", 1)
			},
			want: "non-empty, non-dot hostname",
		},
		{
			name: "dot path segment",
			mutate: func(entry *operationIntentEntry) {
				entry.CallbackURL = strings.Replace(entry.CallbackURL, "/callbacks/provision", "/api/../callbacks/provision", 1)
				entry.LifecycleCallbackURL = strings.Replace(entry.LifecycleCallbackURL, "/callbacks/provision", "/api/../callbacks/provision", 1)
			},
			want: "dot, parent",
		},
		{
			name: "same-origin wrong route",
			mutate: func(entry *operationIntentEntry) {
				entry.CallbackURL = strings.Replace(entry.CallbackURL, "/callbacks/provision", "/callbacks/other", 1)
				entry.LifecycleCallbackURL = strings.Replace(entry.LifecycleCallbackURL, "/callbacks/provision", "/callbacks/other", 1)
			},
			want: "path must end",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := testOperationIntentSpec(t, "unsafe-callback-"+test.name)
			_, err = beginTestOperationIntent(t, store, spec)
			require.NoError(t, err)

			var corrupt []byte
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
				key := []byte(spec.LeaseUUID)
				var head storedLeaseMutationHead
				require.NoError(t, json.Unmarshal(bucket.Get(key), &head))
				test.mutate(head.Operation)
				corrupt, err = json.Marshal(head)
				require.NoError(t, err)
				return bucket.Put(key, corrupt)
			}))

			_, err = store.ListOperationIntents()
			require.ErrorContains(t, err, test.want)
			require.ErrorContains(t, store.Healthy(), test.want)
			require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
				stored := tx.Bucket(callbackLeaseMutationHeadBucketName).Get([]byte(spec.LeaseUUID))
				assert.Equal(t, corrupt, stored,
					"unsafe causal evidence must remain quarantined, not be discarded")
				return nil
			}))
		})
	}
}

func TestOperationIntentRejectsUnknownAggregateFields(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testOperationIntentSpec(t, "unknown-nested")
	_, err = beginTestOperationIntent(t, store, spec)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
		key := []byte(spec.LeaseUUID)
		raw := bucket.Get(key)
		extended := append(bytes.TrimSuffix(bytes.Clone(raw), []byte("}")),
			[]byte(`,"future":{"nested":[{"value":1}]}}`)...)
		return bucket.Put(key, extended)
	}))

	_, err = store.ListOperationIntents()
	require.ErrorContains(t, err, "unknown field")
	require.ErrorContains(t, store.Healthy(), "unknown field")
}

func TestOperationIntentPendingCompletionScanChecksEveryOperationRow(t *testing.T) {
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testOperationIntentSpec(t, "pending-scan")
	for _, callbackURL := range []string{
		spec.CallbackURL,
		"https://fred.example/callbacks/provision?operation_id=" + uuid.NewString(),
	} {
		_, err := store.storeRawTestEntry(CallbackEntry{
			LeaseUUID:        spec.LeaseUUID,
			CallbackURL:      callbackURL,
			DeliveryKind:     CallbackDeliveryKindOperation,
			Status:           backend.CallbackStatusFailed,
			Backend:          "docker-a",
			BackendStorageID: "550e8400-e29b-41d4-a716-446655440000",
			CreatedAt:        time.Now(),
		})
		require.NoError(t, err)
	}
	_, err = beginTestOperationIntent(t, store, spec)
	require.True(t, errors.Is(err, ErrOperationIntentConflict), "got %v", err)
}

func TestTypedOperationRefusalSettlesExactOperationBeforeLifecycle(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	store := stores.callbacks
	spec := testOperationIntentSpec(t, "deprovision-preemption")
	claim := beginHandoffOperation(t, stores.settlement, spec)

	proof := commitHandoffRefusal(t, stores.settlement, claim)
	_, err := stores.settlement.resolveOperationFailure(proof, "operation preempted by deprovision")
	require.NoError(t, err)
	intents, err := stores.settlement.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents, "the typed refusal must consume the exact operation intent")

	_, err = store.storeEntry(CallbackEntry{
		LeaseUUID:        spec.LeaseUUID,
		CallbackURL:      spec.LifecycleCallbackURL,
		DeliveryKind:     CallbackDeliveryKindLifecycle,
		Status:           backend.CallbackStatusDeprovisioned,
		Backend:          "docker-a",
		BackendStorageID: "550e8400-e29b-41d4-a716-446655440000",
		CreatedAt:        time.Now(),
	})
	require.NoError(t, err)

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, CallbackDeliveryKindOperation, pending[0].DeliveryKind)
	assert.Equal(t, spec.CallbackURL, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[1].DeliveryKind)
	assert.Equal(t, spec.LifecycleCallbackURL, pending[1].CallbackURL)
}

func TestRejectCallbackRedirectNeverFollowsSignedRequest(t *testing.T) {
	err := RejectCallbackRedirect(nil, nil)
	require.ErrorIs(t, err, http.ErrUseLastResponse)
}
