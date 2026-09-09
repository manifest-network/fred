package shared

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

func TestOperationIntentRequiresExplicitSettleableCallbackPair(t *testing.T) {
	for _, kind := range []OperationIntentKind{OperationIntentProvision, OperationIntentRestore} {
		t.Run(string(kind), func(t *testing.T) {
			for _, pair := range []string{"explicit", "missing lifecycle", "divergent lifecycle"} {
				t.Run(pair, func(t *testing.T) {
					fixture := newCallbackPublisherFixture(t, "docker-a")
					spec := testOperationIntentSpec(t, "callback-admission")
					spec.Kind = kind
					if kind == OperationIntentRestore {
						spec.SourceLeaseUUID = testLeaseUUID("callback-admission-source")
						spec.SourceGeneration = 1
					}
					switch pair {
					case "missing lifecycle":
						spec.LifecycleCallbackURL = ""
					case "divergent lifecycle":
						spec.LifecycleCallbackURL += "&different=authority"
					}
					before, err := os.ReadFile(fixture.stores.callbackPath)
					require.NoError(t, err)
					candidate, err := fixture.stores.settlement.NewOperationIntentCandidate(spec)
					if pair != "explicit" {
						require.ErrorContains(t, err, "callback")
						after, readErr := os.ReadFile(fixture.stores.callbackPath)
						require.NoError(t, readErr)
						assert.Equal(t, before, after, "invalid admission must not persist a Pending row")
						claims, readErr := fixture.stores.settlement.ListOperationIntents()
						require.NoError(t, readErr)
						assert.Empty(t, claims)
						return
					}
					require.NoError(t, err)
					admission, err := fixture.stores.settlement.BeginOperationIntent(candidate)
					require.NoError(t, err)
					claim := createdOperationClaim(t, admission)
					// Exercise the actual Release and terminal-receipt constructors,
					// not just a second copy of the admission predicate.
					proof := commitHandoffRefusal(t, fixture.stores.settlement, claim)
					require.NoError(t, fixture.publisher.PublishOperationFailureContext(t.Context(), proof, "pre-effect refusal"))
					claims, err := fixture.stores.settlement.ListOperationIntents()
					require.NoError(t, err)
					assert.Empty(t, claims, "a canonical callback pair remains settleable")
					pending, err := fixture.stores.callbacks.ListPending()
					require.NoError(t, err)
					require.Len(t, pending, 1)
					assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
				})
			}
		})
	}
}

func TestOperationIntentDecodeRejectsMissingDurableLifecycleCallback(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	spec := testOperationIntentSpec(t, "missing-durable-lifecycle")
	beginHandoffOperation(t, stores.settlement, spec)
	require.NoError(t, stores.callbacks.Close())
	db, err := bolt.Open(stores.callbackPath, 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	var corrupt []byte
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
		var head storedLeaseMutationHead
		if err := json.Unmarshal(bucket.Get([]byte(spec.LeaseUUID)), &head); err != nil {
			return err
		}
		head.Operation.LifecycleCallbackURL = ""
		corrupt, err = json.Marshal(head)
		if err != nil {
			return err
		}
		return bucket.Put([]byte(spec.LeaseUUID), corrupt)
	}))
	require.NoError(t, db.Close())
	callbacks, openErr := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, newTestStorageAuthorityGate(t),
	)
	if openErr == nil {
		_, err = callbacks.ListOperationIntents()
		require.ErrorContains(t, err, "explicit lifecycle callback")
		require.ErrorContains(t, callbacks.Healthy(), "explicit lifecycle callback")
		require.NoError(t, callbacks.Close())
	} else {
		require.ErrorContains(t, openErr, "explicit lifecycle callback")
	}
	db, err = bolt.Open(stores.callbackPath, 0o600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		assert.Equal(t, corrupt, tx.Bucket(callbackLeaseMutationHeadBucketName).Get([]byte(spec.LeaseUUID)),
			"decoding cannot silently invent durable callback authority")
		return nil
	}))
}
