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
			for _, pair := range []string{"explicit", "missing lifecycle", "divergent lifecycle", "tokenless"} {
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
					case "tokenless":
						spec.CallbackURL = "https://fred.example/callbacks/provision"
						spec.LifecycleCallbackURL = spec.CallbackURL
					}
					before, err := os.ReadFile(fixture.stores.callbackPath)
					require.NoError(t, err)
					candidate, err := fixture.stores.settlement.NewOperationIntentCandidate(spec)
					if pair != "explicit" {
						require.ErrorContains(t, err, "callback")
						require.ErrorIs(t, err, backend.ErrValidation)
						// Ignoring construction failure cannot turn its zero result
						// into durable Pending admission or first-dispatch authority.
						_, beginErr := fixture.stores.settlement.BeginOperationIntent(candidate)
						require.Error(t, beginErr)
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
					assert.Equal(t, candidate.runtime, claim.runtime,
						"admission must retain the validated runtime capability")
					prepared, err := fixture.stores.settlement.PrepareOperationRelease(claim)
					require.NoError(t, err)
					assert.Equal(t, candidate.runtime, prepared.authority.runtime)
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

func TestOperationRuntimeAuthorityRehydratesWithoutPromotingTokenlessRows(t *testing.T) {
	for _, kind := range []OperationIntentKind{OperationIntentProvision, OperationIntentRestore} {
		t.Run(string(kind), func(t *testing.T) {
			for _, encoding := range []string{"typed", "historical tokenless"} {
				t.Run(encoding, func(t *testing.T) {
					stores := openOperationHandoffStores(t, "docker-a")
					spec := testOperationIntentSpec(t, "runtime-rehydration")
					spec.Kind = kind
					if kind == OperationIntentRestore {
						spec.SourceLeaseUUID = testLeaseUUID("runtime-rehydration-source")
						spec.SourceGeneration = 1
					}
					claim := beginHandoffOperation(t, stores.settlement, spec)
					require.NoError(t, stores.callbacks.Close())
					var historicalRow []byte
					if encoding == "historical tokenless" {
						// Model an already stored compatibility row, not a new
						// request: current admission must never mint this shape.
						db, err := bolt.Open(stores.callbackPath, 0o600, &bolt.Options{Timeout: time.Second})
						require.NoError(t, err)
						require.NoError(t, db.Update(func(tx *bolt.Tx) error {
							bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
							var head storedLeaseMutationHead
							if err := json.Unmarshal(bucket.Get([]byte(spec.LeaseUUID)), &head); err != nil {
								return err
							}
							head.Operation.OperationID = OperationID{}
							head.Operation.CallbackURL = "https://fred.example/callbacks/provision"
							head.Operation.LifecycleCallbackURL = head.Operation.CallbackURL
							data, err := json.Marshal(head)
							if err != nil {
								return err
							}
							historicalRow = data
							return bucket.Put([]byte(spec.LeaseUUID), data)
						}))
						require.NoError(t, db.Close())
					}
					callbacks, err := OpenIdentityBoundCallbackStore(
						CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, stores.gate,
					)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, callbacks.Close()) })
					settlement, err := NewOperationSettlement(callbacks, stores.releases)
					require.NoError(t, err)
					before, err := os.ReadFile(stores.callbackPath)
					require.NoError(t, err)
					claims, err := settlement.ListOperationIntents()
					require.NoError(t, err)
					require.Len(t, claims, 1)
					recovered := claims[0]
					require.NoError(t, callbacks.Healthy())
					prepared, err := settlement.PrepareOperationRelease(recovered)
					if encoding == "typed" {
						require.NoError(t, err)
						assert.Equal(t, claim.runtime, recovered.runtime)
						assert.Equal(t, recovered.runtime, prepared.authority.runtime)
						failure, refuseErr := settlement.RefuseOperationExecution(prepared)
						require.NoError(t, refuseErr)
						_, commitErr := settlement.CommitOperationFailure(failure)
						require.NoError(t, commitErr)
						return
					}
					require.ErrorContains(t, err, "requires typed runtime authority")
					assert.True(t, recovered.OperationID().IsZero())
					assert.False(t, recovered.runtime.valid, "read compatibility cannot mint typed settlement authority")
					probe, err := settlement.NewOperationIntentProbe(recovered.LeaseUUID(), recovered.CallbackURL())
					require.NoError(t, err)
					disposition, err := settlement.ProbeOperationIntent(probe)
					require.NoError(t, err)
					assert.Equal(t, OperationIntentAdmissionExisting, disposition)
					spec.CallbackURL = recovered.CallbackURL()
					spec.LifecycleCallbackURL = recovered.LifecycleCallbackURL()
					_, err = settlement.NewOperationIntentCandidate(spec)
					require.ErrorIs(t, err, backend.ErrValidation,
						"readable historical state must not authorize another tokenless admission")
					require.NoError(t, callbacks.view(func(tx *bolt.Tx) error {
						assert.Equal(t, historicalRow, tx.Bucket(callbackLeaseMutationHeadBucketName).Get([]byte(spec.LeaseUUID)),
							"reopening cannot rewrite historical operation authority")
						return nil
					}))
					after, err := os.ReadFile(stores.callbackPath)
					require.NoError(t, err)
					assert.Equal(t, before, after, "compatibility reads cannot rewrite stored authority")
				})
			}
		})
	}
}
