package shared

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestRetentionPageSnapshotSurvivesConcurrentRestoreFinalization(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-page")
	_, claim, source := restoreHandoffFixture(t, stores, "page-source")
	candidate, err := stores.restore.PrepareRestoreClaim(claim)
	require.NoError(t, err)
	proof, err := stores.restore.ClaimForRestore(candidate, 0)
	require.NoError(t, err)
	// Give the fixture free mapped pages before deliberately holding a reader
	// across a writer commit. Otherwise a tiny new database can require mmap
	// growth, which correctly waits for that reader and defeats the test barrier.
	allocationBucket := []byte("test-page-allocation")
	require.NoError(t, stores.retentions.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket(allocationBucket)
		if err != nil {
			return err
		}
		return bucket.Put([]byte("capacity"), make([]byte, 1<<20))
	}))
	require.NoError(t, stores.retentions.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(allocationBucket)
	}))
	admitted := make(chan struct{})
	deleted := make(chan error, 1)
	go func() {
		<-admitted
		removed, err := stores.retentions.DeleteRestoring(proof)
		if err == nil && !removed {
			err = errors.New("exact restoring row was not deleted")
		}
		deleted <- err
	}()
	var page []RetentionEntry
	err = stores.retentions.view(func(tx *bolt.Tx) error {
		// Admit the MVCC snapshot, then allow the real typed finalizer to
		// commit before the page reads any cursor values. No production hook
		// or unbounded read transaction escapes the store-owned page function.
		close(admitted)
		if err := <-deleted; err != nil {
			return err
		}
		var err error
		page, _, err = stores.retentions.retentionPageTx(tx, "", 1)
		return err
	})
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, source.OriginalLeaseUUID, page[0].OriginalLeaseUUID)
	require.Equal(t, proof.Entry(), page[0], "the admitted snapshot retains the complete exact preimage")
	current, next, err := stores.retentions.ListPage("", 1)
	require.NoError(t, err)
	require.NotNil(t, current)
	require.Empty(t, current, "a later page observes committed finalization")
	require.Empty(t, next)
	page[0].Items[0].SKU = "caller mutation"
	require.NotEqual(t, "caller mutation", proof.Entry().Items[0].SKU, "page data is detached from authority")
}

func TestRetentionPageRejectsInvalidSnapshotWithoutPartialRows(t *testing.T) {
	for _, mode := range []string{"missing bucket", "nested bucket", "malformed record", "unsupported schema", "key mismatch", "invalid principal", "invalid source", "invalid profiles", "foreign storage", "withdrawn storage", "closed store"} {
		t.Run(mode, func(t *testing.T) {
			stores := openOperationHandoffStores(t, "docker-page-invalid")
			_, _, source := restoreHandoffFixture(t, stores, "page-invalid")
			valid := source
			valid.OriginalLeaseUUID = "00000000-0000-4000-8000-000000000001"
			require.Less(t, valid.OriginalLeaseUUID, source.OriginalLeaseUUID)
			require.NoError(t, stores.retentions.putForTest(valid))
			prefix, next, err := stores.retentions.ListPage("", 1)
			require.NoError(t, err)
			require.Len(t, prefix, 1)
			require.Equal(t, valid.OriginalLeaseUUID, next, "a valid row is decoded before the invalid row")
			switch mode {
			case "foreign storage":
				other := openOperationHandoffStores(t, "docker-other-page")
				require.NoError(t, stores.retentions.db.Update(func(tx *bolt.Tx) error {
					return tx.Bucket(storeIdentityBucketName).Put(storeIdentityStorageIDKey, []byte(other.storage.ID().String()))
				}))
			case "withdrawn storage":
				_ = stores.gate.Latch(backendidentity.ErrIdentityDrift)
			case "closed store":
				require.NoError(t, stores.retentions.Close())
			default:
				require.NoError(t, stores.retentions.db.Update(func(tx *bolt.Tx) error {
					bucket := tx.Bucket(retentionBucketName)
					key := []byte(source.OriginalLeaseUUID)
					switch mode {
					case "missing bucket":
						return tx.DeleteBucket(retentionBucketName)
					case "nested bucket":
						if err := bucket.Delete(key); err != nil {
							return err
						}
						_, err := bucket.CreateBucket(key)
						return err
					case "malformed record":
						return bucket.Put(key, []byte(`{"broken":true}`))
					case "key mismatch":
						source.OriginalLeaseUUID = testLeaseUUID("foreign-page-key")
					case "invalid principal":
						source.ProviderUUID = ""
					case "invalid source":
						source.Tenant = ""
					case "invalid profiles":
						source.ResourceProfiles[0].CPUCores = -1
					}
					data, err := marshalRetentionEntry(source)
					if err != nil {
						return err
					}
					if mode == "unsupported schema" {
						data = bytes.Replace(data, []byte(`"schema_version":1`), []byte(`"schema_version":99`), 1)
					}
					return bucket.Put(key, data)
				}))
			}
			page, next, err := stores.retentions.ListPage("", 10)
			require.Error(t, err)
			require.Nil(t, page)
			require.Empty(t, next)
		})
	}
}

func TestRetentionPageReturnsDetachedNestedValues(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-page-detached")
	_, _, source := restoreHandoffFixture(t, stores, "detached-page")
	source.CreatedAt = source.CreatedAt.Round(0) // JSON persistence omits the monotonic clock reading.
	page, _, err := stores.retentions.ListPage("", 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	page[0].Items[0].SKU = "caller mutation"
	page[0].ResourceProfiles[0].MemoryMB++
	page[0].RetainedVolumeNames[0] = "caller mutation"
	for _, service := range page[0].StackManifest.Services {
		service.Image = "caller mutation"
	}
	current, _, err := stores.retentions.ListPage("", 1)
	require.NoError(t, err)
	require.Equal(t, []RetentionEntry{source}, current)
}

func TestRetentionPageDecodesOnlyRequestedRows(t *testing.T) {
	s := newTestRetentionStore(t)
	require.NoError(t, s.putForTest(sampleEntry("a")))
	require.NoError(t, s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(retentionBucketName).Put([]byte("z"), []byte("invalid"))
	}))
	page, next, err := s.ListPage("", 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.Equal(t, "a", next, "continuation comes from the same bounded snapshot")
	page, next, err = s.ListPage(next, 1)
	require.Error(t, err)
	require.Nil(t, page)
	require.Empty(t, next)
}
