package shared

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

func TestCallbackHealthValidationMatchesOriginalTraversal(t *testing.T) {
	const lease = "00000000-0000-4000-8000-000000000001"
	type validationCase struct {
		name   string
		valid  bool
		mutate func(*testing.T, *CallbackStore, *bolt.Tx)
	}
	cases := []validationCase{
		{name: "closed head", valid: true},
		{name: "empty histories beside head", valid: true, mutate: func(t *testing.T, _ *CallbackStore, tx *bolt.Tx) {
			_, err := tx.Bucket(callbackOperationHistoryBucketName).CreateBucket([]byte(lease))
			require.NoError(t, err)
			_, err = tx.Bucket(callbackMaintenanceHistoryBucketName).CreateBucket([]byte(lease))
			require.NoError(t, err)
		}},
		{name: "history without head", valid: true, mutate: func(t *testing.T, s *CallbackStore, tx *bolt.Tx) {
			seedCallbackHealthOperationHistory(t, s, tx, lease, "docker-a")
			require.NoError(t, tx.Bucket(callbackLeaseMutationHeadBucketName).Delete([]byte(lease)))
		}},
		{name: "history matching closed storage", valid: true, mutate: func(t *testing.T, s *CallbackStore, tx *bolt.Tx) {
			seedCallbackHealthOperationHistory(t, s, tx, lease, "docker-a")
		}},
		{name: "history crosses storage", mutate: func(t *testing.T, s *CallbackStore, tx *bolt.Tx) {
			seedCallbackHealthOperationHistory(t, s, tx, lease, "docker-b")
		}},
		{name: "reservation count", mutate: func(t *testing.T, _ *CallbackStore, tx *bolt.Tx) {
			require.NoError(t, tx.Bucket(callbackLeaseMutationHeadBucketName).SetSequence(1))
		}},
		{name: "missing uuid marker", mutate: func(t *testing.T, _ *CallbackStore, tx *bolt.Tx) {
			require.NoError(t, tx.Bucket(callbackLeaseMutationUUIDSlotBucketName).Delete([]byte(lease)))
		}},
		{name: "uuid marker sequence", mutate: func(t *testing.T, _ *CallbackStore, tx *bolt.Tx) {
			require.NoError(t, tx.Bucket(callbackLeaseMutationUUIDSlotBucketName).SetSequence(2))
		}},
		{name: "uuid marker value", mutate: func(t *testing.T, _ *CallbackStore, tx *bolt.Tx) {
			require.NoError(t, tx.Bucket(callbackLeaseMutationUUIDSlotBucketName).Put([]byte(lease), []byte{2}))
		}},
		{name: "nested head", mutate: func(t *testing.T, _ *CallbackStore, tx *bolt.Tx) {
			heads := tx.Bucket(callbackLeaseMutationHeadBucketName)
			require.NoError(t, heads.Delete([]byte(lease)))
			_, err := heads.CreateBucket([]byte(lease))
			require.NoError(t, err)
		}},
		{name: "missing history root", mutate: func(t *testing.T, _ *CallbackStore, tx *bolt.Tx) {
			require.NoError(t, tx.DeleteBucket(callbackMaintenanceHistoryBucketName))
		}},
	}
	for _, historyRoot := range [][]byte{callbackOperationHistoryBucketName, callbackMaintenanceHistoryBucketName} {
		for _, hasHead := range []bool{false, true} {
			cases = append(cases, validationCase{
				name: fmt.Sprintf("%s malformed history has_head=%t", historyRoot, hasHead),
				mutate: func(t *testing.T, _ *CallbackStore, tx *bolt.Tx) {
					if !hasHead {
						require.NoError(t, tx.Bucket(callbackLeaseMutationHeadBucketName).Delete([]byte(lease)))
					}
					bucket, err := tx.Bucket(historyRoot).CreateBucket([]byte(lease))
					require.NoError(t, err)
					require.NoError(t, bucket.Put([]byte("malformed-record"), []byte(`{"version":1,"version":2}`)))
				},
			})
		}
		cases = append(cases, validationCase{
			name: string(historyRoot) + " value instead of lease bucket",
			mutate: func(t *testing.T, _ *CallbackStore, tx *bolt.Tx) {
				require.NoError(t, tx.Bucket(historyRoot).Put([]byte(lease), []byte("value")))
			},
		})
	}
	for _, corrupt := range []struct {
		name   string
		change func([]byte) []byte
	}{
		{"malformed JSON", func([]byte) []byte { return []byte("{") }},
		{"duplicate envelope field", func(value []byte) []byte { return append([]byte(`{"version":1,`), value[1:]...) }},
		{"unknown envelope field", func(value []byte) []byte { return append([]byte(`{"unexpected":true,`), value[1:]...) }},
		{"case-folded envelope field", func(value []byte) []byte { return bytes.Replace(value, []byte(`"version"`), []byte(`"Version"`), 1) }},
		{"invalid variant", func(value []byte) []byte {
			return bytes.Replace(value, []byte(`"kind":"closed"`), []byte(`"kind":"close"`), 1)
		}},
		{"missing principal", func(value []byte) []byte {
			return bytes.Replace(value, []byte(`"tenant":"health-benchmark-tenant"`), []byte(`"tenant":""`), 1)
		}},
		{"oversized head", func(value []byte) []byte {
			return append(value, bytes.Repeat([]byte(" "), maxLeaseMutationHeadBytes)...)
		}},
	} {
		cases = append(cases, validationCase{
			name: corrupt.name,
			mutate: func(t *testing.T, _ *CallbackStore, tx *bolt.Tx) {
				heads := tx.Bucket(callbackLeaseMutationHeadBucketName)
				value := bytes.Clone(heads.Get([]byte(lease)))
				require.NoError(t, heads.Put([]byte(lease), corrupt.change(value)))
			},
		})
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			store := newBoundCallbackHealthStore(t, 1)
			if test.mutate != nil {
				require.NoError(t, store.db.Update(func(tx *bolt.Tx) error { test.mutate(t, store, tx); return nil }))
			}
			oldErr, newErr := referenceCallbackHealth(store), store.Healthy()
			require.Equal(t, oldErr == nil, newErr == nil, "reference=%v optimized=%v", oldErr, newErr)
			if test.valid {
				require.NoError(t, newErr)
			} else {
				require.Error(t, newErr)
			}
		})
	}
}

func seedCallbackHealthOperationHistory(t *testing.T, store *CallbackStore, tx *bolt.Tx, leaseUUID, backendName string) {
	t.Helper()
	_, storage := store.journalBackendIdentity("")
	callback := "https://fred.example/callbacks/provision"
	lifecycle, err := backend.ResolveLifecycleCallbackURL(callback, "")
	require.NoError(t, err)
	record := operationCompletionRecord{
		Version: operationCompletionRecordVersion, IntentID: "00000000-0000-4000-8000-000000000002",
		Kind: OperationIntentProvision, LeaseUUID: leaseUUID, CallbackURL: callback, LifecycleCallbackURL: lifecycle,
		Backend: backendName, BackendStorageID: storage.String(), Tenant: "health-benchmark-tenant",
		ProviderUUID: "33333333-3333-4333-8333-333333333333", CreatedAt: time.Unix(1, 0).UTC(),
		SettledAt: time.Unix(2, 0).UTC(), State: operationIntentSucceeded,
	}
	require.NoError(t, validateOperationCompletionRecord(record, leaseUUID))
	data, err := json.Marshal(record)
	require.NoError(t, err)
	history, err := tx.Bucket(callbackOperationHistoryBucketName).CreateBucketIfNotExists([]byte(leaseUUID))
	require.NoError(t, err)
	key := operationHistoryKey(record.OperationID, record.CallbackURL)
	require.NoError(t, history.Put(key[:], data))
	require.NoError(t, tx.Bucket(callbackLeaseMutationHeadBucketName).SetSequence(1))
}

func TestCallbackHealthValidationIsTransactionScoped(t *testing.T) {
	store := newBoundCallbackHealthStore(t, 1)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		_, err := newCallbackReceiptValidation(tx)
		require.ErrorContains(t, err, "immutable read transaction")
		return nil
	}))
	require.NoError(t, store.Healthy())
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackLeaseMutationHeadBucketName).Put([]byte("00000000-0000-4000-8000-000000000001"), []byte("corrupt"))
	}))
	require.Error(t, store.Healthy(), "a successful prior read cannot hide committed corruption")
	require.Error(t, referenceCallbackHealth(store))
}

func TestCallbackHealthValidationCoversEveryMutationVariant(t *testing.T) {
	for _, variant := range []string{"operation", "maintenance", "close", "closed"} {
		t.Run(variant, func(t *testing.T) {
			var store *CallbackStore
			switch variant {
			case "operation":
				store = newBoundCallbackHealthStore(t, 0)
				_, err := beginTestOperationIntent(t, store, testOperationIntentSpec(t, "health-validation"))
				require.NoError(t, err)
			case "maintenance":
				_, store, _, _ = beginMaintenanceFixture(t, "health-validation")
			case "close":
				store = newCloseIntentTestStore(t, filepath.Join(t.TempDir(), "callbacks.db"))
				t.Cleanup(func() { require.NoError(t, store.Close()) })
				_, err := beginUnboundCloseIntent(t, store, testCloseIntentSpec(t, "health-validation"))
				require.NoError(t, err)
			case "closed":
				store = newBoundCallbackHealthStore(t, 1)
			}
			require.NoError(t, referenceCallbackHealth(store))
			require.NoError(t, store.Healthy())
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				heads := tx.Bucket(callbackLeaseMutationHeadBucketName)
				return heads.SetSequence(heads.Sequence() + 1)
			}))
			require.Error(t, referenceCallbackHealth(store))
			require.Error(t, store.Healthy(), "every variant contributes its exact durable reservation count")
		})
	}
}
