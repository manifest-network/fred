package shared

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
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

func testLeaseUUID(name string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(name)).String()
}

func validTestCallbackEntry(entry CallbackEntry) CallbackEntry {
	if !entry.DeliveryKind.known() {
		return entry
	}
	if entry.CallbackURL == "" {
		entry.CallbackURL = "https://fred.example/callbacks/provision"
	}
	if entry.Status == "" {
		entry.Status = "success"
		if entry.Error != "" {
			entry.Status = "failed"
		}
	}
	entry.Success = entry.Status != "failed"
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	if entry.BackendStorageID == "" {
		entry.BackendStorageID = "550e8400-e29b-41d4-a716-446655440000"
	}
	return entry
}

func (s *CallbackStore) storeValidTest(entry CallbackEntry) error {
	_, err := s.storeRawTestEntry(validTestCallbackEntry(entry))
	return err
}

func (s *CallbackStore) storeValidTestEntry(entry CallbackEntry) (CallbackEntry, error) {
	return s.storeRawTestEntry(validTestCallbackEntry(entry))
}

// storeRawTestEntry is an explicitly unsafe fixture seam for exercising queue
// decoding, ordering, and corruption behavior. Production callers must use the
// public lifecycle-only Store/StoreEntry API or atomically resolve an operation
// intent; tests that need a pre-existing exact completion may seed that durable
// post-settlement state directly.
func (s *CallbackStore) storeRawTestEntry(entry CallbackEntry) (CallbackEntry, error) {
	unlock := s.lockDeliveryLease(entry.LeaseUUID)
	defer unlock()
	return s.storeEntryLocked(entry)
}

func TestCallbackStorePublicAPIRejectsRawCausalCompletion(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	for _, kind := range []CallbackDeliveryKind{
		CallbackDeliveryKindOperation,
		CallbackDeliveryKindMaintenance,
	} {
		entry := validTestCallbackEntry(CallbackEntry{
			LeaseUUID:    testLeaseUUID("raw-" + string(kind)),
			CallbackURL:  "https://fred.example/callbacks/provision",
			DeliveryKind: kind,
		})

		require.ErrorIs(t, store.Store(entry), ErrCallbackIntentRequired)
		_, err = store.StoreEntry(entry)
		require.ErrorIs(t, err, ErrCallbackIntentRequired)
	}
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackStore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_callbacks.db")

	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	t.Run("store and list", func(t *testing.T) {
		entry := CallbackEntry{
			LeaseUUID:    testLeaseUUID("lease-1"),
			CallbackURL:  "http://localhost/cb/callbacks/provision",
			DeliveryKind: CallbackDeliveryKindOperation,
			Success:      true,
			CreatedAt:    time.Now(),
		}
		err := store.storeValidTest(entry)
		require.NoError(t, err)

		pending, err := store.ListPending()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		assert.Equal(t, testLeaseUUID("lease-1"), pending[0].LeaseUUID)
		assert.Equal(t, "http://localhost/cb/callbacks/provision", pending[0].CallbackURL)
		assert.True(t, pending[0].Success)
	})

	t.Run("remove after delivery", func(t *testing.T) {
		pending, err := store.ListPending()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.NoError(t, store.RemoveEntry(pending[0]))

		pending, err = store.ListPending()
		require.NoError(t, err)
		assert.Len(t, pending, 0)
	})

	t.Run("store failure entry", func(t *testing.T) {
		entry := CallbackEntry{
			LeaseUUID:    testLeaseUUID("lease-2"),
			CallbackURL:  "http://localhost/cb/callbacks/provision",
			DeliveryKind: CallbackDeliveryKindOperation,
			Success:      false,
			Error:        "container crashed",
			CreatedAt:    time.Now(),
		}
		err := store.storeValidTest(entry)
		require.NoError(t, err)

		pending, err := store.ListPending()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		assert.False(t, pending[0].Success)
		assert.Equal(t, "container crashed", pending[0].Error)
	})

	t.Run("append another delivery for the same lease", func(t *testing.T) {
		entry := CallbackEntry{
			LeaseUUID:    testLeaseUUID("lease-2"),
			CallbackURL:  "http://localhost/cb2/callbacks/provision",
			DeliveryKind: CallbackDeliveryKindOperation,
			Success:      true,
			CreatedAt:    time.Now(),
		}
		err := store.storeValidTest(entry)
		require.NoError(t, err)

		pending, err := store.ListPending()
		require.NoError(t, err)
		require.Len(t, pending, 2)
		assert.NotEqual(t, pending[0].DeliveryID, pending[1].DeliveryID)
		byURL := map[string]bool{}
		for _, callback := range pending {
			byURL[callback.CallbackURL] = callback.Success
		}
		assert.Equal(t, map[string]bool{
			"http://localhost/cb/callbacks/provision":  false,
			"http://localhost/cb2/callbacks/provision": true,
		}, byURL)

		for _, callback := range pending {
			require.NoError(t, store.RemoveEntry(callback))
		}
	})
}

func TestCallbackStore_Persistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "persist_callbacks.db")

	// Write an entry
	store1, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	err = store1.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-persist"),
		CallbackURL:  "http://localhost/persist/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      false,
		Error:        "some error",
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, store1.Close())

	// Reopen and verify entry survived
	store2, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store2.Close()

	pending, err := store2.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, testLeaseUUID("lease-persist"), pending[0].LeaseUUID)
	assert.Equal(t, "http://localhost/persist/callbacks/provision", pending[0].CallbackURL)
	assert.Equal(t, "some error", pending[0].Error)
	assert.NotZero(t, pending[0].Sequence)

	next, err := store2.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-next"),
		CallbackURL:  "http://localhost/next/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	assert.Greater(t, next.Sequence, pending[0].Sequence,
		"bbolt sequence must remain monotonic across process reopen")
}

func TestCallbackStoreRejectsEntryLargerThanDurableReaderLimit(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "bounded-callback.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("oversized-callback"),
		CallbackURL:  "https://fred.example/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Error:        strings.Repeat("x", maxCallbackEntryBytes),
		CreatedAt:    time.Now(),
	})
	require.ErrorContains(t, err, "callback entry exceeds")

	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending, "a row the durable reader would reject must not be committed")

	stored, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("bounded-callback"),
		CallbackURL:  "https://fred.example/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	assert.Positive(t, stored.Sequence,
		"an oversized rollback must not poison later sequence allocation")
	pending, err = store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, stored.DeliveryID, pending[0].DeliveryID)
}

func TestCallbackStore_V2SchemaNestsDeliveriesByLease(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "nested.db")})
	require.NoError(t, err)
	defer store.Close()

	first, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-a"),
		CallbackURL:  "https://fred.example/a/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	second, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-b"),
		CallbackURL:  "https://fred.example/b/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)

	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		legacy := tx.Bucket(callbackBucketName)
		require.NotNil(t, legacy)
		legacyKey, _ := legacy.Cursor().First()
		assert.Nil(t, legacyKey, "new writes must leave the v0.13 rollback bucket untouched")

		root := tx.Bucket(callbackV2BucketName)
		require.NotNil(t, root)
		assert.Nil(t, root.Get(callbackSequenceKey(first.Sequence)),
			"deliveries must not be flat top-level keys")
		leaseA := root.Bucket([]byte(testLeaseUUID("lease-a")))
		leaseB := root.Bucket([]byte(testLeaseUUID("lease-b")))
		require.NotNil(t, leaseA)
		require.NotNil(t, leaseB)
		assert.NotNil(t, leaseA.Get(callbackSequenceKey(first.Sequence)))
		assert.NotNil(t, leaseB.Get(callbackSequenceKey(second.Sequence)))
		return nil
	}))
}

func TestCallbackStore_EmptyPath(t *testing.T) {
	_, err := NewCallbackStore(CallbackStoreConfig{})
	assert.Error(t, err)
}

func TestCallbackStore_Healthy(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "healthy_callbacks.db")

	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	err = store.Healthy()
	require.NoError(t, err)
}

func TestCallbackStore_HealthyReportsDurableQueueCorruption(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "healthy_callbacks.db")})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		leaseBucket, err := tx.Bucket(callbackV2BucketName).CreateBucket([]byte("corrupt-lease"))
		if err != nil {
			return err
		}
		return leaseBucket.Put([]byte("123e4567-e89b-42d3-a456-426614174099"), []byte("{"))
	}))

	err = store.Healthy()
	require.ErrorContains(t, err, "callback queue unhealthy")
	require.ErrorContains(t, err, "failed to decode callback entry")
}

func TestCallbackStore_CloseIdempotent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "idempotent_callbacks.db")

	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	// Close twice — should not panic
	require.NoError(t, store.Close())
	require.NoError(t, store.Close())
}

func TestCallbackStore_InitialCleanup(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "initial_cleanup.db")

	// Create store without expiry, insert old entries
	store1, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	require.NoError(t, store1.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-old"),
		CallbackURL:  "http://example.com/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Success:      true,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	}))
	require.NoError(t, store1.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-fresh"),
		CallbackURL:  "http://example.com/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Success:      true,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store1.Close())

	// Reopen WITH expiry — initial cleanup should remove the old entry
	store2, err := NewCallbackStore(CallbackStoreConfig{
		DBPath: dbPath,
		MaxAge: 24 * time.Hour,
	})
	require.NoError(t, err)
	defer store2.Close()

	pending, err := store2.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, testLeaseUUID("lease-fresh"), pending[0].LeaseUUID)
}

func TestCallbackStore_RemoveOlderThan(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cb_ttl.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	// Store entries at different ages
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("old-1"),
		CallbackURL:  "http://example.com/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Success:      true,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	}))
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("old-2"),
		CallbackURL:  "http://example.com/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Success:      false,
		Error:        "some error",
		CreatedAt:    time.Now().Add(-25 * time.Hour),
	}))
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("fresh"),
		CallbackURL:  "http://example.com/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		CreatedAt:    time.Now(),
	}))

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 2, removed)

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, testLeaseUUID("fresh"), pending[0].LeaseUUID)
}

func TestCallbackStore_RemoveOlderThan_EmptyStore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cb_empty_ttl.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 0, removed)
}

func TestCallbackStore_RemoveOlderThan_AllFresh(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cb_allfresh.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "http://example.com/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-2"),
		CallbackURL:  "http://example.com/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		CreatedAt:    time.Now().Add(-1 * time.Hour),
	}))

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 0, removed)

	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Len(t, pending, 2)
}

func TestCallbackStore_RemoveOlderThan_ExpiresOldHeadAndKeepsFreshSuffix(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	storeLegacyCallback(t, store, CallbackEntry{
		LeaseUUID:   testLeaseUUID("lease-1"),
		CallbackURL: "https://fred.example/legacy-old/callbacks/provision",
		CreatedAt:   time.Now().Add(-48 * time.Hour),
	})
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "https://fred.example/lifecycle/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 1, removed)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[0].DeliveryKind)
}

func TestCallbackStore_RemoveOlderThan_OperationCompletionIsPermanentBarrier(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	for _, kind := range []CallbackDeliveryKind{
		CallbackDeliveryKindOperation,
		CallbackDeliveryKindLifecycle,
	} {
		_, err = store.storeValidTestEntry(CallbackEntry{
			LeaseUUID:    testLeaseUUID("lease-1"),
			CallbackURL:  "https://fred.example/callbacks/provision",
			DeliveryKind: kind,
			CreatedAt:    time.Now().Add(-48 * time.Hour),
		})
		require.NoError(t, err)
	}

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Zero(t, removed,
		"TTL must not discard the only exact evidence that can settle a durable placement attempt")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, CallbackDeliveryKindOperation, pending[0].DeliveryKind)
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[1].DeliveryKind,
		"FIFO suffix cannot overtake the non-expiring operation head")
}

func TestCallbackStore_RemoveOlderThan_ExpiresLegacyHead(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	storeLegacyCallback(t, store, CallbackEntry{
		LeaseUUID:   testLeaseUUID("legacy-lease"),
		CallbackURL: "https://fred.example/legacy/callbacks/provision",
		CreatedAt:   time.Now().Add(-48 * time.Hour),
	})
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("legacy-lease"),
		CallbackURL:  "https://fred.example/typed-after-legacy/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 1, removed)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "https://fred.example/typed-after-legacy/callbacks/provision", pending[0].CallbackURL)
}

func TestCallbackStore_RemoveOlderThan_ExpiresAgedLegacyAndV2SameLease(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	createdAt := time.Now().Add(-48 * time.Hour)
	storeLegacyCallback(t, store, CallbackEntry{
		LeaseUUID:   testLeaseUUID("lease-1"),
		CallbackURL: "https://fred.example/legacy/callbacks/provision",
		CreatedAt:   createdAt,
	})
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "https://fred.example/typed/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		CreatedAt:    createdAt,
	})
	require.NoError(t, err)

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 2, removed)
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackStore_RemoveOlderThan_QuarantinesMalformedLeaseWithoutBlockingOthers(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("valid-lease"),
		CallbackURL:  "https://fred.example/lifecycle/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	})
	require.NoError(t, err)
	quarantinedStored, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("corrupt-lease"),
		CallbackURL:  "https://fred.example/quarantined/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	})
	require.NoError(t, err)
	corruptDeliveryID := "123e4567-e89b-42d3-a456-426614174099"
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackV2BucketName).Bucket([]byte(testLeaseUUID("corrupt-lease"))).
			Put([]byte(corruptDeliveryID), []byte("{"))
	}))

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.ErrorContains(t, err, "failed to decode callback entry")
	assert.Equal(t, 1, removed, "an unrelated valid lease must still expire")
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket(callbackV2BucketName)
		corruptLease := root.Bucket([]byte(testLeaseUUID("corrupt-lease")))
		require.NotNil(t, corruptLease)
		assert.NotNil(t, corruptLease.Get([]byte(corruptDeliveryID)),
			"cleanup must retain malformed data for operator recovery")
		assert.NotNil(t, corruptLease.Get(callbackSequenceKey(quarantinedStored.Sequence)),
			"cleanup must not partially advance the quarantined lease")
		assert.Nil(t, root.Bucket([]byte(testLeaseUUID("valid-lease"))),
			"removing the final delivery should remove the now-empty lease bucket")
		return nil
	}))
}

func TestCallbackStore_DistinctDeliveriesForSameLease(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cb_distinct_deliveries.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	first, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "http://example.com/v1/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)

	second, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "http://example.com/v2/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      false,
		Error:        "updated error",
		CreatedAt:    time.Now().Add(time.Second),
	})
	require.NoError(t, err)
	require.NotEmpty(t, first.DeliveryID)
	require.NotEmpty(t, second.DeliveryID)
	require.NotEqual(t, first.DeliveryID, second.DeliveryID)

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2, "independent deliveries must not overwrite each other")
	assert.Equal(t, first.DeliveryID, pending[0].DeliveryID)
	assert.Equal(t, second.DeliveryID, pending[1].DeliveryID)

	require.NoError(t, store.RemoveEntry(pending[0]))
	pending, err = store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, second.DeliveryID, pending[0].DeliveryID)

	require.NoError(t, store.RemoveEntry(pending[0]))
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackStore_LifecycleEnqueueCoalescesOnlyOlderTypedLifecycle(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	baseTime := time.Now().Add(-4 * time.Hour)
	exact, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "http://example.com/exact/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       "success",
		CreatedAt:    baseTime,
	})
	require.NoError(t, err)
	staleLifecycle, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "http://example.com/stale-lifecycle/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       "failed",
		CreatedAt:    baseTime.Add(time.Hour),
	})
	require.NoError(t, err)
	storeLegacyCallback(t, store, CallbackEntry{
		LeaseUUID:   testLeaseUUID("lease-1"),
		CallbackURL: "http://example.com/protected-legacy/callbacks/provision",
		Success:     true,
		Status:      "success",
		CreatedAt:   baseTime.Add(2 * time.Hour),
	})

	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-2"),
		CallbackURL:  "http://example.com/other-lease/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       "failed",
		CreatedAt:    time.Now(),
	}))

	latestLifecycle, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "http://example.com/latest-lifecycle/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       "deprovisioned",
		CreatedAt:    time.Now().Add(-72 * time.Hour),
	})
	require.NoError(t, err)

	pending, err := store.listPending(testLeaseUUID("lease-1"))
	require.NoError(t, err)
	require.Len(t, pending, 3)
	assert.Empty(t, pending[0].DeliveryID, "legacy entries remain protected from lifecycle coalescing")
	assert.Equal(t, exact.DeliveryID, pending[1].DeliveryID,
		"an exact operation completion must never be coalesced")
	assert.Equal(t, latestLifecycle.DeliveryID, pending[2].DeliveryID)
	assert.Less(t, exact.Sequence, latestLifecycle.Sequence,
		"durable sequence, not CreatedAt, defines FIFO order")
	for _, entry := range pending {
		assert.NotEqual(t, staleLifecycle.DeliveryID, entry.DeliveryID)
	}

	allPending, err := store.ListPending()
	require.NoError(t, err)
	assert.Len(t, allPending, 4, "another lease's lifecycle observation must remain independent")
}

func TestCallbackStore_LifecycleEnqueueCoalescesAdjacentKeys(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	const (
		staleLifecycle1 = "00000000-0000-4000-8000-000000000001"
		staleLifecycle2 = "00000000-0000-4000-8000-000000000002"
		staleLifecycle3 = "00000000-0000-4000-8000-000000000003"
		exactDelivery   = "00000000-0000-4000-8000-000000000004"
		otherLease      = "00000000-0000-4000-8000-000000000005"
		latestLifecycle = "00000000-0000-4000-8000-000000000006"
	)
	seed := []CallbackEntry{
		{DeliveryID: staleLifecycle1, LeaseUUID: testLeaseUUID("lease-1"), DeliveryKind: CallbackDeliveryKindLifecycle, Sequence: 1},
		{DeliveryID: staleLifecycle2, LeaseUUID: testLeaseUUID("lease-1"), DeliveryKind: CallbackDeliveryKindLifecycle, Sequence: 2},
		{DeliveryID: staleLifecycle3, LeaseUUID: testLeaseUUID("lease-1"), DeliveryKind: CallbackDeliveryKindLifecycle, Sequence: 3},
		{DeliveryID: exactDelivery, LeaseUUID: testLeaseUUID("lease-1"), DeliveryKind: CallbackDeliveryKindOperation, Sequence: 4},
		{DeliveryID: otherLease, LeaseUUID: testLeaseUUID("lease-2"), DeliveryKind: CallbackDeliveryKindLifecycle, Sequence: 5},
	}
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(callbackV2BucketName)
		for _, entry := range seed {
			entry = validTestCallbackEntry(entry)
			b, bucketErr := root.CreateBucketIfNotExists([]byte(entry.LeaseUUID))
			if bucketErr != nil {
				return bucketErr
			}
			data, marshalErr := json.Marshal(entry)
			if marshalErr != nil {
				return marshalErr
			}
			if putErr := b.Put(callbackSequenceKey(entry.Sequence), data); putErr != nil {
				return putErr
			}
		}
		return root.SetSequence(5)
	}))

	stored, err := store.storeValidTestEntry(CallbackEntry{
		DeliveryID:   latestLifecycle,
		LeaseUUID:    testLeaseUUID("lease-1"),
		DeliveryKind: CallbackDeliveryKindLifecycle,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(6), stored.Sequence)

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 3)
	assert.Equal(t, exactDelivery, pending[0].DeliveryID)
	assert.Equal(t, otherLease, pending[1].DeliveryID)
	assert.Equal(t, latestLifecycle, pending[2].DeliveryID)
	for _, entry := range pending {
		assert.NotContains(t, []string{staleLifecycle1, staleLifecycle2, staleLifecycle3}, entry.DeliveryID)
	}
}

func TestCallbackStore_RequiresTypedKindAndAssignsSequence(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	_, err = store.storeValidTestEntry(CallbackEntry{LeaseUUID: testLeaseUUID("lease-1")})
	require.ErrorContains(t, err, "invalid callback delivery kind")
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		DeliveryKind: CallbackDeliveryKindOperation,
		Sequence:     42,
	})
	require.ErrorContains(t, err, "store-assigned")

	first, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		DeliveryKind: CallbackDeliveryKindOperation,
	})
	require.NoError(t, err)
	second, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		DeliveryKind: CallbackDeliveryKindOperation,
	})
	require.NoError(t, err)
	assert.NotZero(t, first.Sequence)
	assert.Greater(t, second.Sequence, first.Sequence)
}

func TestCallbackStore_SequenceDefinesFIFOWithEqualOrReversedCreatedAt(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name            string
		firstCreatedAt  time.Time
		secondCreatedAt time.Time
	}{
		{
			name:            "equal timestamps",
			firstCreatedAt:  now.Add(-2 * time.Hour),
			secondCreatedAt: now.Add(-2 * time.Hour),
		},
		{
			name:            "timestamps run backwards",
			firstCreatedAt:  now.Add(-time.Hour),
			secondCreatedAt: now.Add(-2 * time.Hour),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
			require.NoError(t, err)
			defer store.Close()

			first, err := store.storeValidTestEntry(CallbackEntry{
				LeaseUUID:    testLeaseUUID("lease-1"),
				CallbackURL:  "http://example.com/first/callbacks/provision",
				DeliveryKind: CallbackDeliveryKindOperation,
				CreatedAt:    tc.firstCreatedAt,
			})
			require.NoError(t, err)
			second, err := store.storeValidTestEntry(CallbackEntry{
				LeaseUUID:    testLeaseUUID("lease-1"),
				CallbackURL:  "http://example.com/second/callbacks/provision",
				DeliveryKind: CallbackDeliveryKindOperation,
				CreatedAt:    tc.secondCreatedAt,
			})
			require.NoError(t, err)

			pending, err := store.listPending(testLeaseUUID("lease-1"))
			require.NoError(t, err)
			require.Len(t, pending, 2)
			assert.Equal(t, first.DeliveryID, pending[0].DeliveryID)
			assert.Equal(t, second.DeliveryID, pending[1].DeliveryID)
			assert.Less(t, pending[0].Sequence, pending[1].Sequence)
		})
	}
}

func TestCallbackStore_SequenceExhaustionRollsBackLifecycleReplacement(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	previous, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "http://example.com/previous/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       "failed",
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackV2BucketName).SetSequence(math.MaxUint64)
	}))

	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "http://example.com/replacement/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       "success",
		CreatedAt:    time.Now().Add(time.Second),
	})
	require.ErrorContains(t, err, "callback sequence exhausted")

	pending, err := store.listPending(testLeaseUUID("lease-1"))
	require.NoError(t, err)
	require.Len(t, pending, 1,
		"failed allocation must not persist the replacement or coalesce the previous lifecycle record")
	assert.Equal(t, previous.DeliveryID, pending[0].DeliveryID)
}

func TestCallbackStore_ReadsLegacyBucketAndRemovesPrecisely(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cb_legacy_and_v2.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	createdAt := time.Now()
	storeLegacyCallback(t, store, CallbackEntry{
		LeaseUUID:   testLeaseUUID("lease-1"),
		CallbackURL: "http://example.com/legacy/callbacks/provision",
		Success:     false,
		CreatedAt:   createdAt,
	})
	require.NoError(t, store.storeValidTest(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "http://example.com/v2/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		CreatedAt:    createdAt.Add(time.Second),
	}))
	require.ErrorIs(t, store.Healthy(), errLegacyCallbackOutboxNotDrained)

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Empty(t, pending[0].DeliveryID, "v0.13 entries do not carry a delivery ID")
	assert.NotEmpty(t, pending[1].DeliveryID)

	require.NoError(t, store.RemoveEntry(pending[0]))
	require.NoError(t, store.Healthy(), "draining the old bucket restores current health")
	require.NoError(t, store.Close())

	store, err = NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err, "a drained legacy schema upgrades while preserving v2 rows")
	defer store.Close()
	pending, err = store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "http://example.com/v2/callbacks/provision", pending[0].CallbackURL)
}

func TestCallbackStore_RejectsDuplicateDeliveryIDWithinLease(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	const deliveryID = "550e8400-e29b-41d4-a716-446655440000"
	first, err := store.storeValidTestEntry(CallbackEntry{
		DeliveryID:   deliveryID,
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "http://example.com/first/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	_, err = store.storeValidTestEntry(CallbackEntry{
		DeliveryID:   deliveryID,
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "http://example.com/second/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now(),
	})
	require.ErrorContains(t, err, "already exists")
	require.ErrorContains(t, store.RemoveEntry(CallbackEntry{DeliveryID: deliveryID}), "no durable lease capability",
		"a public identity without StoreEntry/ListPending's storage capability must not authorize deletion")

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, first.LeaseUUID, pending[0].LeaseUUID)
	assert.Equal(t, first.CallbackURL, pending[0].CallbackURL)
}

func TestCallbackStore_SameDeliveryIDAcrossLeasesRemovesPrecisely(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	const deliveryID = "550e8400-e29b-41d4-a716-446655440000"
	first, err := store.storeValidTestEntry(CallbackEntry{
		DeliveryID:   deliveryID,
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "http://example.com/first/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	second, err := store.storeValidTestEntry(CallbackEntry{
		DeliveryID:   deliveryID,
		LeaseUUID:    testLeaseUUID("lease-2"),
		CallbackURL:  "http://example.com/second/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now().Add(time.Second),
	})
	require.NoError(t, err)

	require.NoError(t, store.RemoveEntry(first))
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, second.LeaseUUID, pending[0].LeaseUUID)
	assert.Equal(t, second.CallbackURL, pending[0].CallbackURL)
	assert.Equal(t, deliveryID, pending[0].DeliveryID)
}

func TestCallbackStore_RemoveEntryRejectsStaleCapabilityAfterValueReplacement(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	const deliveryID = "550e8400-e29b-41d4-a716-446655440000"
	first, err := store.storeValidTestEntry(CallbackEntry{
		DeliveryID:   deliveryID,
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "http://example.com/first/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	replacement := first
	replacement.CallbackURL = "http://example.com/replaced/callbacks/provision"
	replacementData, err := json.Marshal(replacement)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackV2BucketName).Bucket([]byte(first.LeaseUUID)).
			Put(callbackSequenceKey(first.Sequence), replacementData)
	}))

	require.ErrorContains(t, store.RemoveEntry(first), "changed before precise removal",
		"a stale storage capability must not delete changed durable bytes at the same path")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, testLeaseUUID("lease-1"), pending[0].LeaseUUID)
	assert.Equal(t, replacement.CallbackURL, pending[0].CallbackURL)
}

func TestCallbackStore_RemoveEntryRejectsMutatedDeliveryIdentity(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	stored, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("lease-1"),
		CallbackURL:  "http://example.com/callbacks/provision",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	mutatedID := "550e8400-e29b-41d4-a716-446655440000"
	if stored.DeliveryID == mutatedID {
		mutatedID = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	}
	stored.DeliveryID = mutatedID

	require.ErrorContains(t, store.RemoveEntry(stored), "does not match durable identity")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "mutating the public identity must not retain deletion authority")
}

func TestCallbackStore_ListPendingRejectsMismatchedV2Sequence(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	entry := CallbackEntry{
		DeliveryID:       "6ba7b810-9dad-41d1-80b4-00c04fd430c8",
		LeaseUUID:        testLeaseUUID("lease-1"),
		CallbackURL:      "http://example.com/callbacks/provision",
		DeliveryKind:     CallbackDeliveryKindOperation,
		Sequence:         1,
		Success:          true,
		Status:           "success",
		BackendStorageID: "550e8400-e29b-41d4-a716-446655440000",
		CreatedAt:        time.Now(),
	}
	data, err := json.Marshal(entry)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		leaseBucket, createErr := tx.Bucket(callbackV2BucketName).CreateBucket([]byte(testLeaseUUID("lease-1")))
		if createErr != nil {
			return createErr
		}
		return leaseBucket.Put(callbackSequenceKey(2), data)
	}))

	pending, err := store.ListPending()
	require.ErrorContains(t, err, "callback sequence mismatch")
	assert.Empty(t, pending, "an invalid durable identity must fail closed instead of being skipped")
}

func TestCallbackStore_RejectsInvalidV2SemanticsBeforeWrite(t *testing.T) {
	const callbackID = "550e8400-e29b-41d4-a716-446655440000"
	now := time.Now()
	valid := CallbackEntry{
		LeaseUUID:        testLeaseUUID("semantic-validation"),
		CallbackURL:      "https://fred.example/callbacks/provision?operation_id=" + callbackID,
		DeliveryKind:     CallbackDeliveryKindOperation,
		Success:          true,
		Status:           "success",
		BackendStorageID: callbackID,
		CreatedAt:        now,
	}
	for _, test := range []struct {
		name   string
		mutate func(*CallbackEntry)
		want   string
	}{
		{"noncanonical lease", func(entry *CallbackEntry) { entry.LeaseUUID = "lease-1" }, "canonical non-nil UUID"},
		{"nil lease UUID", func(entry *CallbackEntry) { entry.LeaseUUID = uuid.Nil.String() }, "canonical non-nil UUID"},
		{"missing storage identity", func(entry *CallbackEntry) { entry.BackendStorageID = "" }, "storage identity is required"},
		{"invalid storage identity", func(entry *CallbackEntry) { entry.BackendStorageID = "not-a-uuid" }, "invalid callback backend storage identity"},
		{"relative destination", func(entry *CallbackEntry) { entry.CallbackURL = "/callbacks/provision" }, "scheme must be http or https"},
		{"userinfo destination", func(entry *CallbackEntry) { entry.CallbackURL = "https://user@fred.example/callbacks/provision" }, "user info"},
		{"port-only destination", func(entry *CallbackEntry) { entry.CallbackURL = "https://:443/callbacks/provision" }, "non-empty, non-dot hostname"},
		{"invalid-port destination", func(entry *CallbackEntry) { entry.CallbackURL = "https://fred.example:65536/callbacks/provision" }, "port must be between 1 and 65535"},
		{"empty-fragment destination", func(entry *CallbackEntry) {
			entry.CallbackURL = "https://fred.example/callbacks/provision#"
		}, "fragment"},
		{"dot-segment destination", func(entry *CallbackEntry) { entry.CallbackURL = "https://fred.example/api/../callbacks/provision" }, "dot, parent"},
		{"encoded-separator destination", func(entry *CallbackEntry) {
			entry.CallbackURL = "https://fred.example/api%2Fcallback/callbacks/provision"
		}, "canonical percent-encoding"},
		{"link-local destination", func(entry *CallbackEntry) { entry.CallbackURL = "http://169.254.169.254/callbacks/provision" }, "routable unicast"},
		{"operation carrying lifecycle authority", func(entry *CallbackEntry) {
			entry.CallbackURL = "https://fred.example/callbacks/provision?lifecycle_id=" + callbackID
		}, "operation URL must not contain lifecycle_id"},
		{"lifecycle carrying operation authority", func(entry *CallbackEntry) {
			entry.DeliveryKind = CallbackDeliveryKindLifecycle
		}, "lifecycle URL must not contain operation_id"},
		{"operation deprovisioned status", func(entry *CallbackEntry) { entry.Status = "deprovisioned" }, "invalid status"},
		{"retained operation", func(entry *CallbackEntry) { entry.Retained = true }, "cannot be retained"},
		{"success flag conflicts with status", func(entry *CallbackEntry) { entry.Success = false }, "conflicts with status"},
		{"zero creation time", func(entry *CallbackEntry) { entry.CreatedAt = time.Time{} }, "Unix epoch"},
		{"pre-epoch creation time", func(entry *CallbackEntry) { entry.CreatedAt = time.Unix(-1, 0) }, "Unix epoch"},
		{"future creation time", func(entry *CallbackEntry) {
			entry.CreatedAt = time.Now().Add(callbackCreatedAtFutureSkew + time.Minute)
		}, "future clock-skew allowance"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
			require.NoError(t, err)
			defer store.Close()
			entry := valid
			test.mutate(&entry)
			_, err = store.storeRawTestEntry(entry)
			require.ErrorContains(t, err, test.want)
			pending, listErr := store.ListPending()
			require.NoError(t, listErr)
			assert.Empty(t, pending, "invalid input must not allocate a durable row")
		})
	}

	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "skew.db")})
	require.NoError(t, err)
	defer store.Close()
	valid.CreatedAt = time.Now().Add(callbackCreatedAtFutureSkew / 2)
	_, err = store.storeRawTestEntry(valid)
	require.NoError(t, err, "modest rolling-deployment clock skew remains valid")
}

func TestCallbackStore_V2AcceptsTokenlessURLForMigratedV013Workload(t *testing.T) {
	const callbackID = "550e8400-e29b-41d4-a716-446655440000"
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()
	leaseUUID := testLeaseUUID("tokenless-v2-rollout")
	for _, entry := range []CallbackEntry{
		{
			LeaseUUID: leaseUUID, CallbackURL: "https://fred.example/callbacks/provision",
			DeliveryKind: CallbackDeliveryKindOperation,
			Success:      true, Status: backend.CallbackStatusSuccess,
			BackendStorageID: callbackID, CreatedAt: time.Now(),
		},
		{
			LeaseUUID: leaseUUID, CallbackURL: "https://fred.example/callbacks/provision",
			DeliveryKind: CallbackDeliveryKindLifecycle,
			Success:      false, Status: backend.CallbackStatusFailed,
			BackendStorageID: callbackID, CreatedAt: time.Now(),
		},
	} {
		_, err = store.storeRawTestEntry(entry)
		require.NoError(t, err)
	}

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, callbackStorageV2, pending[0].storageVersion)
	assert.Equal(t, CallbackDeliveryKindOperation, pending[0].DeliveryKind)
	assert.Equal(t, callbackStorageV2, pending[1].storageVersion)
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[1].DeliveryKind)
	require.NoError(t, store.Healthy())
}

func TestCallbackStore_DurableSemanticPoisonFailsClosed(t *testing.T) {
	const deliveryID = "550e8400-e29b-41d4-a716-446655440000"
	leaseUUID := testLeaseUUID("semantic-poison")
	base := CallbackEntry{
		DeliveryID:       deliveryID,
		LeaseUUID:        leaseUUID,
		CallbackURL:      "https://fred.example/callbacks/provision?operation_id=" + deliveryID,
		DeliveryKind:     CallbackDeliveryKindOperation,
		Sequence:         1,
		Success:          true,
		Status:           "success",
		BackendStorageID: deliveryID,
		CreatedAt:        time.Now(),
	}
	for _, test := range []struct {
		name string
		raw  func() []byte
		want string
	}{
		{"link-local destination", func() []byte {
			entry := base
			entry.CallbackURL = "http://169.254.169.254/latest/meta-data/callbacks/provision?operation_id=" + deliveryID
			data, err := json.Marshal(entry)
			require.NoError(t, err)
			return data
		}, "routable unicast"},
		{"wrong callback class", func() []byte {
			entry := base
			entry.CallbackURL = "https://fred.example/callbacks/provision?lifecycle_id=" + deliveryID
			data, err := json.Marshal(entry)
			require.NoError(t, err)
			return data
		}, "operation URL must not contain lifecycle_id"},
		{"port-only destination", func() []byte {
			entry := base
			entry.CallbackURL = "https://:443/callbacks/provision?operation_id=" + deliveryID
			data, err := json.Marshal(entry)
			require.NoError(t, err)
			return data
		}, "non-empty, non-dot hostname"},
		{"unstable path destination", func() []byte {
			entry := base
			entry.CallbackURL = "https://fred.example/api/../callbacks/provision?operation_id=" + deliveryID
			data, err := json.Marshal(entry)
			require.NoError(t, err)
			return data
		}, "dot, parent"},
		{"missing storage identity", func() []byte {
			entry := base
			entry.BackendStorageID = ""
			data, err := json.Marshal(entry)
			require.NoError(t, err)
			return data
		}, "storage identity is required"},
		{"duplicate authority field", func() []byte {
			return []byte(fmt.Sprintf(
				`{"delivery_id":%q,"lease_uuid":%q,"callback_url":"https://fred.example/callbacks/provision","callback_url":"http://169.254.169.254/latest/meta-data/callbacks/provision","delivery_kind":"operation","sequence":1,"success":true,"status":"success","created_at":%q}`,
				deliveryID, leaseUUID, base.CreatedAt.Format(time.RFC3339Nano),
			))
		}, `duplicate field "callback_url"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
			require.NoError(t, err)
			defer store.Close()
			raw := test.raw()
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				bucket, err := tx.Bucket(callbackV2BucketName).CreateBucket([]byte(leaseUUID))
				if err != nil {
					return err
				}
				return bucket.Put(callbackSequenceKey(1), raw)
			}))

			_, err = store.ListPending()
			require.ErrorContains(t, err, test.want)
			require.ErrorContains(t, store.Healthy(), test.want)
			require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
				stored := tx.Bucket(callbackV2BucketName).Bucket([]byte(leaseUUID)).Get(callbackSequenceKey(1))
				assert.Equal(t, raw, stored, "semantic poison must remain quarantined for operator recovery")
				return nil
			}))
		})
	}
}

func TestCallbackStore_DurableRowsSurviveWallClockRollback(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	const operationID = "550e8400-e29b-41d4-a716-446655440000"
	v2LeaseUUID := testLeaseUUID("future-v2-after-clock-rollback")
	stored, err := store.storeRawTestEntry(CallbackEntry{
		LeaseUUID:        v2LeaseUUID,
		CallbackURL:      "https://fred.example/callbacks/provision?operation_id=" + operationID,
		DeliveryKind:     CallbackDeliveryKindOperation,
		Success:          true,
		Status:           backend.CallbackStatusSuccess,
		Backend:          "docker-a",
		BackendStorageID: "550e8400-e29b-41d4-a716-446655440000",
		CreatedAt:        time.Now(),
	})
	require.NoError(t, err)

	futureCreatedAt := time.Now().Add(24 * time.Hour)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackV2BucketName).Bucket([]byte(v2LeaseUUID))
		key := callbackSequenceKey(stored.Sequence)
		var entry CallbackEntry
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
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	for _, entry := range pending {
		assert.Equal(t, futureCreatedAt.UnixNano(), entry.CreatedAt.UnixNano())
	}
	require.NoError(t, store.Healthy())
}

func TestCallbackStore_ReadsLegitimateV013RowsWithSeparateCompatibilityRules(t *testing.T) {
	const callbackID = "550e8400-e29b-41d4-a716-446655440000"
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "legacy.db")})
	require.NoError(t, err)
	defer store.Close()
	legacyRows := []CallbackEntry{
		{LeaseUUID: "legacy-arbitrary-id", CallbackURL: "http://fred.internal/callbacks/provision", CreatedAt: time.Now()},
		{
			LeaseUUID:   "legacy-operation-route",
			CallbackURL: "http://fred.internal/callbacks/provision?operation_id=" + callbackID,
			Success:     true,
			Status:      "success",
			CreatedAt:   time.Now(),
		},
		{
			LeaseUUID:   "legacy-lifecycle-route",
			CallbackURL: "http://fred.internal/callbacks/provision?lifecycle_id=" + callbackID,
			Success:     true,
			Status:      "deprovisioned",
			Retained:    true,
			CreatedAt:   time.Now(),
		},
	}
	for _, entry := range legacyRows {
		storeLegacyCallback(t, store, entry)
	}
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, len(legacyRows))
	for _, entry := range pending {
		assert.Equal(t, callbackStorageLegacy, entry.storageVersion)
	}
}

func TestCallbackStore_RemoveOlderThan_PreservesOldSuffixBehindFreshHead(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "ttl-prefix.db")})
	require.NoError(t, err)
	defer store.Close()
	leaseUUID := testLeaseUUID("clock-regression")
	storeLegacyCallback(t, store, CallbackEntry{
		LeaseUUID: leaseUUID, CallbackURL: "https://fred.example/legacy-fresh/callbacks/provision", CreatedAt: time.Now(),
	})
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID: leaseUUID, DeliveryKind: CallbackDeliveryKindLifecycle, CreatedAt: time.Now().Add(-48 * time.Hour),
	})
	require.NoError(t, err)

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Zero(t, removed, "wall-clock age must not delete a FIFO suffix behind a live head")
	pending, err := store.listPending(leaseUUID)
	require.NoError(t, err)
	assert.Len(t, pending, 2)
}

func TestCallbackStore_RemoveOlderThan_LegacyHeadParticipatesInContiguousPrefix(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "legacy-prefix.db")})
	require.NoError(t, err)
	defer store.Close()
	leaseUUID := testLeaseUUID("legacy-prefix")
	storeLegacyCallback(t, store, CallbackEntry{
		LeaseUUID: leaseUUID, CallbackURL: "https://fred.example/legacy/callbacks/provision", CreatedAt: time.Now().Add(-48 * time.Hour),
	})
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID: leaseUUID, DeliveryKind: CallbackDeliveryKindOperation, CreatedAt: time.Now(),
	})
	require.NoError(t, err)
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID: leaseUUID, DeliveryKind: CallbackDeliveryKindLifecycle, CreatedAt: time.Now().Add(-48 * time.Hour),
	})
	require.NoError(t, err)

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 1, removed, "only the expired legacy head is a removable FIFO prefix")
	pending, err := store.listPending(leaseUUID)
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.False(t, pending[0].CreatedAt.Before(time.Now().Add(-24*time.Hour)))
	assert.True(t, pending[1].CreatedAt.Before(time.Now().Add(-24*time.Hour)),
		"expired suffix stays behind the fresh sequenced head")
}

func TestCallbackStore_RemoveOlderThan_HoldsOnlyCurrentLeaseLock(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "ttl-locks.db")})
	require.NoError(t, err)
	defer store.Close()
	const (
		firstLease  = "00000000-0000-4000-8000-000000000001"
		secondLease = "00000000-0000-4000-8000-000000000002"
	)
	for _, leaseUUID := range []string{firstLease, secondLease} {
		_, err = store.storeValidTestEntry(CallbackEntry{
			LeaseUUID: leaseUUID, DeliveryKind: CallbackDeliveryKindLifecycle, CreatedAt: time.Now().Add(-48 * time.Hour),
		})
		require.NoError(t, err)
	}
	writer, err := store.db.Begin(true)
	require.NoError(t, err)
	cleanupDone := make(chan struct{})
	var removed int
	var cleanupErr error
	go func() {
		defer close(cleanupDone)
		removed, cleanupErr = store.RemoveOlderThan(24 * time.Hour)
	}()
	require.Eventually(t, func() bool {
		store.deliveryLocksMu.Lock()
		_, firstMutationHeld := store.deliveryLocks[firstLease]
		_, secondMutationHeld := store.deliveryLocks[secondLease]
		store.deliveryLocksMu.Unlock()
		store.drainLocksMu.Lock()
		defer store.drainLocksMu.Unlock()
		_, firstDrainHeld := store.drainLocks[firstLease]
		_, secondDrainHeld := store.drainLocks[secondLease]
		return firstMutationHeld && firstDrainHeld && !secondMutationHeld && !secondDrainHeld
	}, time.Second, time.Millisecond, "cleanup must reach the first lease without pre-locking the second")
	unlockSecond, acquired := store.tryLockDeliveryLease(secondLease)
	require.True(t, acquired, "an unrelated lease remains available while the first transaction waits")
	unlockSecond()
	require.NoError(t, writer.Rollback())
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish after releasing the bbolt writer")
	}
	require.NoError(t, cleanupErr)
	assert.Equal(t, 2, removed)
}

func TestCallbackStore_RemoveOlderThanRenotifiesAfterLostDrainElection(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "ttl-renotify.db")})
	require.NoError(t, err)
	defer store.Close()
	leaseUUID := testLeaseUUID("ttl-renotify")
	wake := make(chan struct{}, 1)
	unsubscribe := store.subscribeReplayWake(wake)
	defer unsubscribe()

	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    leaseUUID,
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       backend.CallbackStatusFailed,
		Error:        "expired observation",
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	})
	require.NoError(t, err)
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("old callback publication did not notify replay")
	}

	// Hold the mutation lock so cleanup can acquire drain ownership but cannot
	// inspect the FIFO yet. A live append can then commit under the same modeled
	// ownership and publish the wake that a real replay loop loses to cleanup.
	unlockMutation := store.lockDeliveryLease(leaseUUID)
	mutationReleased := false
	defer func() {
		if !mutationReleased {
			unlockMutation()
		}
	}()
	cleanupDone := make(chan struct{})
	var removed int
	var cleanupErr error
	go func() {
		defer close(cleanupDone)
		removed, cleanupErr = store.RemoveOlderThan(24 * time.Hour)
	}()
	require.Eventually(t, func() bool {
		store.drainLocksMu.Lock()
		defer store.drainLocksMu.Unlock()
		lock := store.drainLocks[leaseUUID]
		return lock != nil && lock.refs > 0
	}, time.Second, time.Millisecond, "cleanup did not acquire drain ownership")

	_, err = store.storeEntryLocked(validTestCallbackEntry(CallbackEntry{
		LeaseUUID:    leaseUUID,
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       backend.CallbackStatusFailed,
		Error:        "fresh observation",
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, err)
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("fresh callback commit did not notify replay")
	}
	_, acquired := store.tryLockDrainLease(leaseUUID)
	require.False(t, acquired, "replay must lose drain election while TTL cleanup owns it")

	unlockMutation()
	mutationReleased = true
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish after mutation ownership was released")
	}
	require.NoError(t, cleanupErr)
	assert.Equal(t, 1, removed)

	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("cleanup handoff did not re-notify replay after releasing drain ownership")
	}
	unlockDrain, acquired := store.tryLockDrainLease(leaseUUID)
	require.True(t, acquired, "re-notified replay must be able to acquire drain ownership")
	unlockDrain()
	pending, err := store.listPending(leaseUUID)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "fresh observation", pending[0].Error)
}

func TestRunCallbackLeaseCleanup_UnlocksAfterPanic(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "ttl-panic.db")})
	require.NoError(t, err)
	defer store.Close()
	leaseUUID := testLeaseUUID("ttl-panic")

	unlockDrain := lockCallbackLease(store.drainLocksMu, store.drainLocks, leaseUUID)
	unlockMutation := store.lockDeliveryLease(leaseUUID)
	func() {
		defer func() {
			require.Equal(t, "cleanup panic", recover())
		}()
		_, _ = runCallbackLeaseCleanup(
			unlockMutation,
			unlockDrain,
			func() (int, error) { panic("cleanup panic") },
			nil,
		)
	}()

	reacquiredDrain, ok := store.tryLockDrainLease(leaseUUID)
	require.True(t, ok, "a recovered cleanup panic must not strand drain ownership")
	reacquiredDrain()
	reacquiredMutation, ok := store.tryLockDeliveryLease(leaseUUID)
	require.True(t, ok, "a recovered cleanup panic must not strand the journal-mutation lock")
	reacquiredMutation()
}

func TestCallbackStore_TerminalLifecycleIsSticky(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "terminal.db")})
	require.NoError(t, err)
	defer store.Close()
	leaseUUID := testLeaseUUID("terminal")
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID: leaseUUID, DeliveryKind: CallbackDeliveryKindLifecycle, Status: "failed",
	})
	require.NoError(t, err)
	terminal, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID: leaseUUID, DeliveryKind: CallbackDeliveryKindLifecycle, Status: "deprovisioned",
	})
	require.NoError(t, err)
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID: leaseUUID, DeliveryKind: CallbackDeliveryKindLifecycle, Status: "failed",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, errTerminalLifecyclePending)
	pending, err := store.listPending(leaseUUID)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, terminal.DeliveryID, pending[0].DeliveryID)
	assert.Equal(t, backend.CallbackStatusDeprovisioned, pending[0].Status)

	replacement, err := store.storeValidTestEntry(CallbackEntry{
		LeaseUUID: leaseUUID, DeliveryKind: CallbackDeliveryKindLifecycle, Status: "deprovisioned", Retained: true,
	})
	require.NoError(t, err, "a newer terminal observation may supersede the queued terminal row")
	pending, err = store.listPending(leaseUUID)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, replacement.DeliveryID, pending[0].DeliveryID)
}

func TestCallbackStore_LegacyTerminalLifecycleIsSticky(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "legacy-terminal.db")})
	require.NoError(t, err)
	defer store.Close()
	leaseUUID := testLeaseUUID("legacy-terminal")
	storeLegacyCallback(t, store, CallbackEntry{
		LeaseUUID: leaseUUID, CallbackURL: "https://fred.example/legacy/callbacks/provision", Success: true,
		Status: "deprovisioned", CreatedAt: time.Now(),
	})
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID: leaseUUID, DeliveryKind: CallbackDeliveryKindLifecycle, Status: "failed",
	})
	require.ErrorIs(t, err, errTerminalLifecyclePending)
	pending, err := store.listPending(leaseUUID)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, callbackStorageLegacy, pending[0].storageVersion)
}

func TestCallbackStore_ListPendingFailsClosedOnMalformedRecord(t *testing.T) {
	for _, version := range []callbackStorageVersion{callbackStorageLegacy, callbackStorageV2} {
		t.Run(fmt.Sprintf("version-%d", version), func(t *testing.T) {
			store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
			require.NoError(t, err)
			defer store.Close()
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				if version == callbackStorageLegacy {
					return tx.Bucket(callbackBucketName).Put([]byte("corrupt-lease"), []byte("{"))
				}
				leaseBucket, createErr := tx.Bucket(callbackV2BucketName).
					CreateBucket([]byte("corrupt-lease"))
				if createErr != nil {
					return createErr
				}
				return leaseBucket.Put(
					[]byte("123e4567-e89b-42d3-a456-426614174099"), []byte("{"))
			}))

			pending, err := store.ListPending()
			require.ErrorContains(t, err, "failed to decode callback entry")
			assert.Empty(t, pending, "corruption must be a delivery barrier, not a skipped record")
		})
	}
}

func TestInspectPendingCallbacksReadOnlyCountsLegacyAndTypedRowsWithoutMutation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	storeLegacyCallback(t, store, CallbackEntry{
		LeaseUUID: testLeaseUUID("inspect-legacy"),
		CreatedAt: time.Now(),
	})
	_, err = store.storeValidTestEntry(CallbackEntry{
		LeaseUUID:    testLeaseUUID("inspect-typed"),
		DeliveryKind: CallbackDeliveryKindOperation,
	})
	require.NoError(t, err)
	require.NoError(t, store.Close())

	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	inspection, err := InspectCallbackStoreReadOnly(dbPath)
	require.NoError(t, err)
	assert.True(t, inspection.Exists)
	assert.True(t, inspection.LegacySchema)
	assert.True(t, inspection.UpgradedSchema)
	assert.Equal(t, 2, inspection.Pending)
	after, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestInspectPendingCallbacksReadOnlyDoesNotCreateMissingDatabase(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "missing.db")
	inspection, err := InspectCallbackStoreReadOnly(dbPath)
	require.NoError(t, err)
	assert.Equal(t, CallbackStoreInspection{}, inspection)
	_, err = os.Stat(dbPath)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestInspectCallbackStoreReadOnlyDistinguishesDrainedV013Schema(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.CreateBucket(callbackBucketName)
		return createErr
	}))
	require.NoError(t, db.Close())

	inspection, err := InspectCallbackStoreReadOnly(dbPath)
	require.NoError(t, err)
	assert.Equal(t, CallbackStoreInspection{
		Exists:       true,
		LegacySchema: true,
	}, inspection)
}

func TestInspectCallbackStoreReadOnlyRejectsEveryPartialCurrentSchema(t *testing.T) {
	for _, missingBucket := range callbackCurrentSchemaBuckets() {
		missingBucket := append([]byte(nil), missingBucket...)
		t.Run(string(missingBucket), func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "callbacks.db")
			store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
			require.NoError(t, err)
			require.NoError(t, store.Close())
			db, err := bolt.Open(dbPath, 0o600, nil)
			require.NoError(t, err)
			require.NoError(t, db.Update(func(tx *bolt.Tx) error {
				return tx.DeleteBucket(missingBucket)
			}))
			require.NoError(t, db.Close())
			before, err := os.ReadFile(dbPath)
			require.NoError(t, err)

			inspection, err := InspectCallbackStoreReadOnly(dbPath)
			assert.ErrorContains(t, err, "partial upgraded schema")
			assert.Equal(t, CallbackStoreInspection{}, inspection)
			after, readErr := os.ReadFile(dbPath)
			require.NoError(t, readErr)
			assert.Equal(t, before, after)
		})
	}
}

func TestCallbackStore_UpgradesDrainedV013SchemaWithoutInventingRows(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "callbacks.db")
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.CreateBucket(callbackBucketName)
		return createErr
	}))
	require.NoError(t, db.Close())

	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
	require.NoError(t, store.Healthy())
	require.NoError(t, store.Close())

	inspection, err := InspectCallbackStoreReadOnly(dbPath)
	require.NoError(t, err)
	assert.Equal(t, CallbackStoreInspection{
		Exists:         true,
		LegacySchema:   true,
		UpgradedSchema: true,
	}, inspection)
}

func storeLegacyCallback(t *testing.T, store *CallbackStore, entry CallbackEntry) {
	t.Helper()
	data, err := json.Marshal(entry)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackBucketName).Put([]byte(entry.LeaseUUID), data)
	}))
}
