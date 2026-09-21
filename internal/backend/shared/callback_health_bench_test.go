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

// Histories can survive without a current head. This fixture exercises both
// receipt roots and increasing per-lease depth independently of closed heads,
// whose successful close normally compacts both histories into a tombstone.
func BenchmarkCallbackHealthHistories(b *testing.B) {
	for _, depth := range []int{1, 16, 128} {
		b.Run(fmt.Sprintf("leases=64/depth=%d", depth), func(b *testing.B) {
			store := newBoundCallbackHealthStore(b, 64)
			require.NoError(b, store.db.Update(func(tx *bolt.Tx) error {
				for index := range 64 {
					lease := fmt.Sprintf("00000000-0000-4000-8000-%012x", index+1)
					seedCallbackHealthOperationHistory(b, store, tx, lease, "docker-a")
					seedCallbackHealthMaintenanceHistory(b, store, tx, lease, depth)
					require.NoError(b, tx.Bucket(callbackLeaseMutationHeadBucketName).Delete([]byte(lease)))
				}
				return tx.Bucket(callbackLeaseMutationHeadBucketName).SetSequence(uint64(64 * (depth + 1)))
			}))
			require.NoError(b, store.Healthy())
			b.ReportAllocs()
			for b.Loop() {
				if err := store.HealthyContext(b.Context()); err != nil {
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
