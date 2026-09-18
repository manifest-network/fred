package shared

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// BenchmarkCallbackHealth measures full validation after completed closes.
// Permanent heads remain after delivery and receipt history are drained; this
// idle fixture models their growing validation cost, not deployment latency.
func BenchmarkCallbackHealth(b *testing.B) {
	for _, rows := range []int{0, 1024, 4096} {
		b.Run(fmt.Sprintf("closed=%d", rows), func(b *testing.B) {
			store := newBoundCallbackHealthStore(b, rows)
			b.ReportAllocs()
			for b.Loop() {
				if err := store.Healthy(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func newBoundCallbackHealthStore(t testing.TB, closedRows int) *CallbackStore {
	t.Helper()
	path, storage := initializeBoundCallbackStore(t)
	store, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: path}, storage, newTestStorageAuthorityGate(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		heads := tx.Bucket(callbackLeaseMutationHeadBucketName)
		for index := range closedRows {
			leaseUUID := fmt.Sprintf("00000000-0000-4000-8000-%012x", index+1)
			data, err := marshalLeaseMutationHead(closedLeaseMutationHead{entry: closedLeaseTombstone{
				CloseIntentID: fmt.Sprintf("00000000-0000-4000-8000-%012x", index+1),
				LeaseUUID:     leaseUUID, Backend: "docker-a", BackendStorageID: storage.ID().String(),
				Tenant: "health-benchmark-tenant", ProviderUUID: "33333333-3333-4333-8333-333333333333",
				ClosedAt: time.Unix(1, 0).UTC(),
			}})
			if err != nil {
				return err
			}
			if err := reserveLeaseMutationUUIDSlotTx(tx, leaseUUID); err != nil {
				return err
			}
			if err := heads.Put([]byte(leaseUUID), data); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, store.Healthy())
	return store
}
