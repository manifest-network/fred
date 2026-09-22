package shared

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

func TestOperationCompletionPendingDiagnosticSurvivesReopenWithoutGrantingAdmission(t *testing.T) {
	path := filepath.Join(t.TempDir(), "callbacks.db")
	store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: path})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	predecessor := testOperationIntentSpec(t, "fifo-restart")
	admission, err := beginTestOperationIntent(t, store, predecessor)
	require.NoError(t, err)
	successor := predecessor
	successor.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
	successor.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(successor.CallbackURL, "")
	require.NoError(t, err)
	_, err = store.ProbeOperationIntent(testOperationIntentProbe(t, store, successor))
	require.ErrorIs(t, err, ErrOperationIntentConflict)
	require.False(t, IsOperationCompletionPending(err), "unresolved operation head is not an observed completion FIFO")
	require.False(t, IsOperationCompletionPending(fmt.Errorf("%w: an earlier operation completion is pending", ErrOperationIntentConflict)), "matching prose and broad sentinel cannot mint the diagnostic")
	completion, err := store.ResolveOperationIntent(createdOperationClaim(t, admission), backend.CallbackStatusFailed, "capacity")
	require.NoError(t, err)
	require.NoError(t, store.Close())
	store, err = newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: path})
	require.NoError(t, err)

	disposition, err := store.ProbeOperationIntent(testOperationIntentProbe(t, store, successor))
	require.True(t, IsOperationCompletionPending(err))
	require.Equal(t, OperationIntentAdmissionNone, disposition)
	rejected, err := beginTestOperationIntent(t, store, successor)
	require.True(t, IsOperationCompletionPending(err))
	_, created := rejected.CreatedClaim()
	require.False(t, created)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, completion.DeliveryID, pending[0].DeliveryID)

	require.NoError(t, store.removeEntry(completion))
	accepted, err := beginTestOperationIntent(t, store, successor)
	require.NoError(t, err)
	_, created = accepted.CreatedClaim()
	require.True(t, created, "only exact callback acknowledgement releases FIFO admission")
}

func TestOperationCompletionPendingRejectsForeignOutboxAuthority(t *testing.T) {
	for _, field := range []string{"backend", "storage"} {
		t.Run(field, func(t *testing.T) {
			store, err := newUnboundCallbackStoreForTest(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			spec := testOperationIntentSpec(t, "foreign-outbox")
			admission, err := beginTestOperationIntent(t, store, spec)
			require.NoError(t, err)
			completion, err := store.ResolveOperationIntent(createdOperationClaim(t, admission), backend.CallbackStatusFailed, "capacity")
			require.NoError(t, err)
			corrupted := completion
			if field == "backend" {
				corrupted.Backend = "foreign-backend"
			} else {
				corrupted.BackendStorageID = uuid.NewString()
			}
			encoded, err := json.Marshal(corrupted)
			require.NoError(t, err)
			require.NoError(t, store.update(func(tx *bolt.Tx) error {
				return tx.Bucket(callbackV2BucketName).Bucket([]byte(spec.LeaseUUID)).Put(callbackSequenceKey(completion.Sequence), encoded)
			}))
			spec.CallbackURL = "https://fred.example/callbacks/provision?operation_id=" + uuid.NewString()
			spec.LifecycleCallbackURL, err = backend.ResolveLifecycleCallbackURL(spec.CallbackURL, "")
			require.NoError(t, err)
			_, err = store.ProbeOperationIntent(testOperationIntentProbe(t, store, spec))
			require.Error(t, err)
			require.False(t, IsOperationCompletionPending(err), "foreign outbox authority is not normal tenant contention")
			_, err = beginTestOperationIntent(t, store, spec)
			require.Error(t, err)
			require.False(t, IsOperationCompletionPending(err))
		})
	}
}
