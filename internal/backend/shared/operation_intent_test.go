package shared

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

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
		Backend:              "docker-a",
		BackendStorageID:     callbackStorageID(t, "550e8400-e29b-41d4-a716-446655440000"),
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
		Success:          status != backend.CallbackStatusFailed,
		Status:           status,
		Backend:          spec.Backend,
		BackendStorageID: spec.BackendStorageID.String(),
		CreatedAt:        time.Now(),
	}
}

func TestOperationIDValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   OperationID
		want bool
	}{
		{
			name: "canonical RFC 4122 UUIDv4",
			id:   "11111111-1111-4111-8111-111111111111",
			want: true,
		},
		{
			name: "empty legacy value",
		},
		{
			name: "malformed",
			id:   "not-a-uuid",
		},
		{
			name: "noncanonical uppercase",
			id:   "11111111-1111-4111-8111-11111111111A",
		},
		{
			name: "wrong version",
			id:   "11111111-1111-3111-8111-111111111111",
		},
		{
			name: "v4 bits with non-RFC variant",
			id:   "11111111-1111-4111-c111-111111111111",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, test.want, test.id.Valid())
		})
	}
}

func TestOperationIntentSurvivesRestartAndAtomicallyBecomesOutboxEntry(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	spec := testOperationIntentSpec(t, "restart")
	admission, err := store.BeginOperationIntent(spec)
	require.NoError(t, err)
	require.Equal(t, OperationIntentAdmissionCreated, admission.Disposition)
	require.NoError(t, store.Close())

	store, err = NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	_, err = store.ResolveOperationIntent(intents[0], backend.CallbackStatusSuccess, "")
	require.NoError(t, err)
	intents, err = store.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, spec.CallbackURL, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
}

func TestSettleOperationCallbackRequiresAndAtomicallyConsumesExactIntent(t *testing.T) {
	t.Run("missing intent cannot manufacture completion", func(t *testing.T) {
		store, err := NewCallbackStore(CallbackStoreConfig{
			DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		spec := testOperationIntentSpec(t, "missing-settlement")

		err = store.settleOperationCallbackLocked(
			callbackEntryForOperationSpec(spec, backend.CallbackStatusSuccess),
		)
		require.ErrorIs(t, err, ErrOperationIntentMissing)
		pending, listErr := store.ListPending()
		require.NoError(t, listErr)
		assert.Empty(t, pending)
	})

	t.Run("mismatch preserves intent", func(t *testing.T) {
		store, err := NewCallbackStore(CallbackStoreConfig{
			DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		spec := testOperationIntentSpec(t, "mismatched-settlement")
		_, err = store.BeginOperationIntent(spec)
		require.NoError(t, err)
		entry := callbackEntryForOperationSpec(spec, backend.CallbackStatusFailed)
		entry.Backend = "different-backend"

		err = store.settleOperationCallbackLocked(entry)
		require.ErrorContains(t, err, "does not match durable intent")
		intents, listErr := store.ListOperationIntents()
		require.NoError(t, listErr)
		require.Len(t, intents, 1)
		assert.Equal(t, spec.CallbackURL, intents[0].CallbackURL())
		pending, listErr := store.ListPending()
		require.NoError(t, listErr)
		assert.Empty(t, pending)
	})

	t.Run("exact intent becomes exactly one completion", func(t *testing.T) {
		store, err := NewCallbackStore(CallbackStoreConfig{
			DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })
		spec := testOperationIntentSpec(t, "exact-settlement")
		_, err = store.BeginOperationIntent(spec)
		require.NoError(t, err)
		entry := callbackEntryForOperationSpec(spec, backend.CallbackStatusSuccess)

		require.NoError(t, store.settleOperationCallbackLocked(entry))
		intents, listErr := store.ListOperationIntents()
		require.NoError(t, listErr)
		assert.Empty(t, intents)
		pending, listErr := store.ListPending()
		require.NoError(t, listErr)
		require.Len(t, pending, 1)
		assert.Equal(t, spec.CallbackURL, pending[0].CallbackURL)

		err = store.settleOperationCallbackLocked(entry)
		require.ErrorIs(t, err, ErrOperationIntentMissing,
			"an already-completed worker cannot append a duplicate")
		pending, listErr = store.ListPending()
		require.NoError(t, listErr)
		require.Len(t, pending, 1)
	})
}

func TestOperationIntentRecoverySurvivesWallClockRollback(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	spec := testOperationIntentSpec(t, "future-after-clock-rollback")
	_, err = store.BeginOperationIntent(spec)
	require.NoError(t, err)
	futureCreatedAt := time.Now().Add(24 * time.Hour)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackOperationIntentBucketName)
		key := []byte(spec.LeaseUUID)
		var entry operationIntentEntry
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

	store, err = NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Equal(t, futureCreatedAt.UnixNano(), intents[0].CreatedAt().UnixNano())
	disposition, err := store.ProbeOperationIntent(OperationIntentProbe{
		LeaseUUID: spec.LeaseUUID, CallbackURL: spec.CallbackURL, Backend: spec.Backend,
		BackendStorageID: spec.BackendStorageID,
	})
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionExisting, disposition)
	require.NoError(t, store.Healthy())

	_, err = store.ResolveOperationIntent(intents[0], backend.CallbackStatusSuccess, "")
	require.NoError(t, err)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, spec.CallbackURL, pending[0].CallbackURL)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
}

func TestOperationIntentAdmissionIsIdempotentAndConflictsByExactAuthority(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testOperationIntentSpec(t, "idempotent")
	first, err := store.BeginOperationIntent(spec)
	require.NoError(t, err)
	require.Equal(t, OperationIntentAdmissionCreated, first.Disposition)
	second, err := store.BeginOperationIntent(spec)
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionExisting, second.Disposition)

	conflict := spec
	conflict.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	conflict.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(conflict.CallbackURL, "")
	require.NoError(t, err)
	_, err = store.BeginOperationIntent(conflict)
	require.ErrorIs(t, err, ErrOperationIntentConflict)
}

func TestProbeOperationIntentRecognizesExactAcceptedAndCompletedRedelivery(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testOperationIntentSpec(t, "probe")
	probe := OperationIntentProbe{
		LeaseUUID: spec.LeaseUUID, CallbackURL: spec.CallbackURL, Backend: spec.Backend,
		BackendStorageID: spec.BackendStorageID,
	}

	disposition, err := store.ProbeOperationIntent(probe)
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionNone, disposition)
	admission, err := store.BeginOperationIntent(spec)
	require.NoError(t, err)
	disposition, err = store.ProbeOperationIntent(probe)
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionExisting, disposition)

	conflict := probe
	conflict.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	_, err = store.ProbeOperationIntent(conflict)
	require.ErrorIs(t, err, ErrOperationIntentConflict)

	_, err = store.ResolveOperationIntent(admission.Claim, backend.CallbackStatusFailed, "refused")
	require.NoError(t, err)
	disposition, err = store.ProbeOperationIntent(probe)
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionCompleted, disposition)
}

func TestOperationIntentRejectsOversizeBeforePersisting(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testOperationIntentSpec(t, "oversize")
	spec.Manifest = []byte(strings.Repeat("x", maxOperationIntentEntryBytes))
	_, err = store.BeginOperationIntent(spec)
	require.ErrorContains(t, err, "exceeds")
	intents, listErr := store.ListOperationIntents()
	require.NoError(t, listErr)
	assert.Empty(t, intents)
}

func TestOperationIntentRejectsNonCanonicalProviderBeforePersisting(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	spec := testOperationIntentSpec(t, "noncanonical-provider")
	spec.ProviderUUID = "not-a-provider-uuid"
	_, err = store.BeginOperationIntent(spec)
	require.ErrorContains(t, err, "provider UUID is not canonical")

	intents, listErr := store.ListOperationIntents()
	require.NoError(t, listErr)
	assert.Empty(t, intents, "invalid provider authority must not be persisted")
}

func TestOperationIntentRecoveryRejectsPersistedNonCanonicalProvider(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	spec := testOperationIntentSpec(t, "persisted-noncanonical-provider")
	_, err = store.BeginOperationIntent(spec)
	require.NoError(t, err)

	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackOperationIntentBucketName)
		key := []byte(spec.LeaseUUID)
		var entry operationIntentEntry
		require.NoError(t, json.Unmarshal(bucket.Get(key), &entry))
		entry.ProviderUUID = "not-a-provider-uuid"
		data, marshalErr := json.Marshal(entry)
		require.NoError(t, marshalErr)
		return bucket.Put(key, data)
	}))

	_, err = store.ListOperationIntents()
	require.ErrorContains(t, err, "provider UUID is not canonical")
	require.ErrorContains(t, store.Healthy(), "provider UUID is not canonical")
}

func TestOperationIntentOversizedCompletionPreservesAtomicIntent(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testOperationIntentSpec(t, "oversized-completion")
	admission, err := store.BeginOperationIntent(spec)
	require.NoError(t, err)

	_, err = store.ResolveOperationIntent(
		admission.Claim,
		backend.CallbackStatusFailed,
		strings.Repeat("x", maxCallbackEntryBytes),
	)
	require.ErrorContains(t, err, "callback entry exceeds")
	intents, listErr := store.ListOperationIntents()
	require.NoError(t, listErr)
	require.Len(t, intents, 1, "failed callback insertion must not consume recovery authority")
	assert.Equal(t, admission.Claim.entry.IntentID, intents[0].entry.IntentID)
	assert.Equal(t, admission.Claim.digest, intents[0].digest)
	pending, listErr := store.ListPending()
	require.NoError(t, listErr)
	assert.Empty(t, pending, "the oversized callback and intent removal must roll back together")

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
			store, err := NewCallbackStore(CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })

			spec := testOperationIntentSpec(t, "invalid-manifest-"+test.name)
			spec.Manifest = test.manifest
			_, err = store.BeginOperationIntent(spec)
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
			store, err := NewCallbackStore(CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })

			spec := testOperationIntentSpec(t, "persisted-invalid-manifest-"+test.name)
			_, err = store.BeginOperationIntent(spec)
			require.NoError(t, err)

			var corrupt []byte
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(callbackOperationIntentBucketName)
				key := []byte(spec.LeaseUUID)
				var entry operationIntentEntry
				if err := json.Unmarshal(bucket.Get(key), &entry); err != nil {
					return err
				}
				entry.Manifest = test.manifest
				corrupt, err = json.Marshal(entry)
				if err != nil {
					return err
				}
				return bucket.Put(key, corrupt)
			}))

			_, err = store.ListOperationIntents()
			require.ErrorContains(t, err, test.wantErr)
			require.ErrorContains(t, store.Healthy(), test.wantErr)
			require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
				stored := tx.Bucket(callbackOperationIntentBucketName).Get([]byte(spec.LeaseUUID))
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
			store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := testOperationIntentSpec(t, "nested-duplicate-"+test.name)
			_, err = store.BeginOperationIntent(spec)
			require.NoError(t, err)

			var corrupt []byte
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(callbackOperationIntentBucketName)
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
				stored := tx.Bucket(callbackOperationIntentBucketName).Get([]byte(spec.LeaseUUID))
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
			store, err := NewCallbackStore(CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := testOperationIntentSpec(t, "unbounded-quantity-"+test.name)
			_, err = store.BeginOperationIntent(spec)
			require.NoError(t, err)

			var corrupt []byte
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(callbackOperationIntentBucketName)
				key := []byte(spec.LeaseUUID)
				var entry operationIntentEntry
				require.NoError(t, json.Unmarshal(bucket.Get(key), &entry))
				test.mutate(&entry)
				corrupt, err = json.Marshal(entry)
				require.NoError(t, err)
				return bucket.Put(key, corrupt)
			}))

			_, err = store.ListOperationIntents()
			require.ErrorContains(t, err, test.want)
			require.ErrorContains(t, store.Healthy(), test.want)
			require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
				stored := tx.Bucket(callbackOperationIntentBucketName).Get([]byte(spec.LeaseUUID))
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
			store, err := NewCallbackStore(CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := testOperationIntentSpec(t, "unsafe-callback-"+test.name)
			_, err = store.BeginOperationIntent(spec)
			require.NoError(t, err)

			var corrupt []byte
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket(callbackOperationIntentBucketName)
				key := []byte(spec.LeaseUUID)
				var entry operationIntentEntry
				require.NoError(t, json.Unmarshal(bucket.Get(key), &entry))
				test.mutate(&entry)
				corrupt, err = json.Marshal(entry)
				require.NoError(t, err)
				return bucket.Put(key, corrupt)
			}))

			_, err = store.ListOperationIntents()
			require.ErrorContains(t, err, test.want)
			require.ErrorContains(t, store.Healthy(), test.want)
			require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
				stored := tx.Bucket(callbackOperationIntentBucketName).Get([]byte(spec.LeaseUUID))
				assert.Equal(t, corrupt, stored,
					"unsafe causal evidence must remain quarantined, not be discarded")
				return nil
			}))
		})
	}
}

func TestOperationIntentAllowsUnknownNestedFields(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testOperationIntentSpec(t, "unknown-nested")
	_, err = store.BeginOperationIntent(spec)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackOperationIntentBucketName)
		key := []byte(spec.LeaseUUID)
		raw := bucket.Get(key)
		extended := append(bytes.TrimSuffix(bytes.Clone(raw), []byte("}")),
			[]byte(`,"future":{"nested":[{"value":1}]}}`)...)
		return bucket.Put(key, extended)
	}))

	intents, err := store.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Equal(t, spec.LeaseUUID, intents[0].LeaseUUID())
}

func TestOperationIntentPendingCompletionScanChecksEveryOperationRow(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
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
			Success:          false,
			Status:           backend.CallbackStatusFailed,
			Backend:          spec.Backend,
			BackendStorageID: spec.BackendStorageID.String(),
			CreatedAt:        time.Now(),
		})
		require.NoError(t, err)
	}
	_, err = store.BeginOperationIntent(spec)
	require.True(t, errors.Is(err, ErrOperationIntentConflict), "got %v", err)
}

func TestFailOperationIntentIfPresentSettlesExactOperationBeforeLifecycle(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	spec := testOperationIntentSpec(t, "deprovision-preemption")
	_, err = store.BeginOperationIntent(spec)
	require.NoError(t, err)

	settled, err := store.FailOperationIntentIfPresent(spec.LeaseUUID, "operation preempted by deprovision")
	require.NoError(t, err)
	require.True(t, settled)
	settled, err = store.FailOperationIntentIfPresent(spec.LeaseUUID, "duplicate")
	require.NoError(t, err)
	assert.False(t, settled, "an already-settled operation must not manufacture a duplicate callback")

	_, err = store.StoreEntry(CallbackEntry{
		LeaseUUID:        spec.LeaseUUID,
		CallbackURL:      spec.LifecycleCallbackURL,
		DeliveryKind:     CallbackDeliveryKindLifecycle,
		Success:          true,
		Status:           backend.CallbackStatusDeprovisioned,
		Backend:          spec.Backend,
		BackendStorageID: spec.BackendStorageID.String(),
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
