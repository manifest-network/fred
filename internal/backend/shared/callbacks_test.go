package shared

import (
	"encoding/json"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestCallbackStore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_callbacks.db")

	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	t.Run("store and list", func(t *testing.T) {
		entry := CallbackEntry{
			LeaseUUID:    "lease-1",
			CallbackURL:  "http://localhost/cb",
			DeliveryKind: CallbackDeliveryKindOperation,
			Success:      true,
			CreatedAt:    time.Now(),
		}
		err := store.Store(entry)
		require.NoError(t, err)

		pending, err := store.ListPending()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		assert.Equal(t, "lease-1", pending[0].LeaseUUID)
		assert.Equal(t, "http://localhost/cb", pending[0].CallbackURL)
		assert.True(t, pending[0].Success)
	})

	t.Run("remove after delivery", func(t *testing.T) {
		err := store.Remove("lease-1")
		require.NoError(t, err)

		pending, err := store.ListPending()
		require.NoError(t, err)
		assert.Len(t, pending, 0)
	})

	t.Run("store failure entry", func(t *testing.T) {
		entry := CallbackEntry{
			LeaseUUID:    "lease-2",
			CallbackURL:  "http://localhost/cb",
			DeliveryKind: CallbackDeliveryKindOperation,
			Success:      false,
			Error:        "container crashed",
			CreatedAt:    time.Now(),
		}
		err := store.Store(entry)
		require.NoError(t, err)

		pending, err := store.ListPending()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		assert.False(t, pending[0].Success)
		assert.Equal(t, "container crashed", pending[0].Error)
	})

	t.Run("append another delivery for the same lease", func(t *testing.T) {
		entry := CallbackEntry{
			LeaseUUID:    "lease-2",
			CallbackURL:  "http://localhost/cb2",
			DeliveryKind: CallbackDeliveryKindOperation,
			Success:      true,
			CreatedAt:    time.Now(),
		}
		err := store.Store(entry)
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
			"http://localhost/cb":  false,
			"http://localhost/cb2": true,
		}, byURL)

		require.NoError(t, store.Remove("lease-2"))
	})

	t.Run("remove nonexistent is noop", func(t *testing.T) {
		err := store.Remove("nonexistent")
		require.NoError(t, err)
	})
}

func TestCallbackStore_Persistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "persist_callbacks.db")

	// Write an entry
	store1, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	err = store1.Store(CallbackEntry{
		LeaseUUID:    "lease-persist",
		CallbackURL:  "http://localhost/persist",
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
	assert.Equal(t, "lease-persist", pending[0].LeaseUUID)
	assert.Equal(t, "http://localhost/persist", pending[0].CallbackURL)
	assert.Equal(t, "some error", pending[0].Error)
	assert.NotZero(t, pending[0].Sequence)

	next, err := store2.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-next",
		CallbackURL:  "http://localhost/next",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	assert.Greater(t, next.Sequence, pending[0].Sequence,
		"bbolt sequence must remain monotonic across process reopen")
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
		return tx.Bucket(callbackV2BucketName).Put([]byte("corrupt-key"), []byte("{"))
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

	require.NoError(t, store1.Store(CallbackEntry{
		LeaseUUID:    "lease-old",
		CallbackURL:  "http://example.com",
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	}))
	require.NoError(t, store1.Store(CallbackEntry{
		LeaseUUID:    "lease-fresh",
		CallbackURL:  "http://example.com",
		DeliveryKind: CallbackDeliveryKindOperation,
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
	assert.Equal(t, "lease-fresh", pending[0].LeaseUUID)
}

func TestCallbackStore_RemoveOlderThan(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cb_ttl.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	// Store entries at different ages
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "old-1",
		CallbackURL:  "http://example.com",
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	}))
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "old-2",
		CallbackURL:  "http://example.com",
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      false,
		Error:        "some error",
		CreatedAt:    time.Now().Add(-25 * time.Hour),
	}))
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "fresh",
		CallbackURL:  "http://example.com",
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
	assert.Equal(t, "fresh", pending[0].LeaseUUID)
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

	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "http://example.com",
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		CreatedAt:    time.Now(),
	}))
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "lease-2",
		CallbackURL:  "http://example.com",
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

func TestCallbackStore_RemoveOlderThan_DoesNotAdvanceFreshSuffixPastExpiredExact(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	_, err = store.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "https://fred.example/exact",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	})
	require.NoError(t, err)
	_, err = store.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "https://fred.example/lifecycle",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Zero(t, removed)
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Equal(t, CallbackDeliveryKindOperation, pending[0].DeliveryKind)
	assert.Equal(t, CallbackDeliveryKindLifecycle, pending[1].DeliveryKind)
}

func TestCallbackStore_RemoveOlderThan_ExpiresWholeTypedLeaseQueueAtomically(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	for _, kind := range []CallbackDeliveryKind{
		CallbackDeliveryKindOperation,
		CallbackDeliveryKindLifecycle,
	} {
		_, err = store.StoreEntry(CallbackEntry{
			LeaseUUID:    "lease-1",
			CallbackURL:  "https://fred.example/callback",
			DeliveryKind: kind,
			CreatedAt:    time.Now().Add(-48 * time.Hour),
		})
		require.NoError(t, err)
	}

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 2, removed)
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackStore_RemoveOlderThan_ProtectsLegacyAndUnknownBarriers(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	storeLegacyCallback(t, store, CallbackEntry{
		LeaseUUID:   "legacy-lease",
		CallbackURL: "https://fred.example/legacy",
		CreatedAt:   time.Now().Add(-48 * time.Hour),
	})
	_, err = store.StoreEntry(CallbackEntry{
		LeaseUUID:    "legacy-lease",
		CallbackURL:  "https://fred.example/typed-after-legacy",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	})
	require.NoError(t, err)
	storePreKindV2Callback(t, store, CallbackEntry{
		DeliveryID:  "6ba7b810-9dad-41d1-80b4-00c04fd430c8",
		LeaseUUID:   "unknown-lease",
		CallbackURL: "https://fred.example/unknown",
		CreatedAt:   time.Now().Add(-48 * time.Hour),
	})
	_, err = store.StoreEntry(CallbackEntry{
		LeaseUUID:    "unknown-lease",
		CallbackURL:  "https://fred.example/typed-after-unknown",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	})
	require.NoError(t, err)

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Zero(t, removed)
	pending, err := store.ListPending()
	require.NoError(t, err)
	assert.Len(t, pending, 4,
		"protected legacy/unknown heads must retain every newer typed suffix entry")
}

func TestCallbackStore_RemoveOlderThan_FailsClosedWithoutDeletingMalformedData(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	stored, err := store.StoreEntry(CallbackEntry{
		LeaseUUID:    "valid-lease",
		CallbackURL:  "https://fred.example/exact",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
	})
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackBucketName).Put([]byte("corrupt-lease"), []byte("{"))
	}))

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.ErrorContains(t, err, "failed to decode callback entry")
	assert.Zero(t, removed)
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		assert.NotNil(t, tx.Bucket(callbackBucketName).Get([]byte("corrupt-lease")),
			"cleanup must retain malformed data as a poison barrier")
		assert.NotNil(t, tx.Bucket(callbackV2BucketName).Get([]byte(stored.DeliveryID)),
			"global validation must happen before cleanup deletes any other lease")
		return nil
	}))
}

func TestCallbackStore_DistinctDeliveriesForSameLease(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "cb_distinct_deliveries.db")
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	first, err := store.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "http://example.com/v1",
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)

	second, err := store.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "http://example.com/v2",
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

	// Remove remains deliberately lease-wide for deprovision cleanup.
	require.NoError(t, store.Remove("lease-1"))
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackStore_LifecycleEnqueueCoalescesOnlyOlderTypedLifecycle(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	exact, err := store.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "http://example.com/exact",
		DeliveryKind: CallbackDeliveryKindOperation,
		Status:       "success",
		CreatedAt:    time.Now().Add(24 * time.Hour),
	})
	require.NoError(t, err)
	staleLifecycle, err := store.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "http://example.com/stale-lifecycle",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       "failed",
		CreatedAt:    time.Now().Add(48 * time.Hour),
	})
	require.NoError(t, err)
	storeLegacyCallback(t, store, CallbackEntry{
		LeaseUUID:   "lease-1",
		CallbackURL: "http://example.com/protected-legacy",
		Status:      "success",
		CreatedAt:   time.Now().Add(60 * time.Hour),
	})

	const unknownID = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
	storePreKindV2Callback(t, store, CallbackEntry{
		DeliveryID:  unknownID,
		LeaseUUID:   "lease-1",
		CallbackURL: "http://example.com/protected-unknown",
		Status:      "success",
		CreatedAt:   time.Now().Add(72 * time.Hour),
	})
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "lease-2",
		CallbackURL:  "http://example.com/other-lease",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       "failed",
		CreatedAt:    time.Now(),
	}))

	latestLifecycle, err := store.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "http://example.com/latest-lifecycle",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       "deprovisioned",
		CreatedAt:    time.Now().Add(-72 * time.Hour),
	})
	require.NoError(t, err)

	pending, err := store.listPending("lease-1")
	require.NoError(t, err)
	require.Len(t, pending, 4)
	assert.Empty(t, pending[0].DeliveryID, "legacy entries remain protected from lifecycle coalescing")
	assert.Equal(t, unknownID, pending[1].DeliveryID,
		"pre-kind/pre-sequence v2 entries are protected unknowns and sort first")
	assert.Zero(t, pending[1].Sequence)
	assert.Empty(t, pending[1].DeliveryKind)
	assert.Equal(t, exact.DeliveryID, pending[2].DeliveryID,
		"an exact operation completion must never be coalesced")
	assert.Equal(t, latestLifecycle.DeliveryID, pending[3].DeliveryID)
	assert.Less(t, exact.Sequence, latestLifecycle.Sequence,
		"durable sequence, not CreatedAt, defines FIFO order")
	for _, entry := range pending {
		assert.NotEqual(t, staleLifecycle.DeliveryID, entry.DeliveryID)
	}

	allPending, err := store.ListPending()
	require.NoError(t, err)
	assert.Len(t, allPending, 5, "another lease's lifecycle observation must remain independent")
}

func TestCallbackStore_RequiresTypedKindAndAssignsSequence(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	_, err = store.StoreEntry(CallbackEntry{LeaseUUID: "lease-1"})
	require.ErrorContains(t, err, "invalid callback delivery kind")
	_, err = store.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-1",
		DeliveryKind: CallbackDeliveryKindOperation,
		Sequence:     42,
	})
	require.ErrorContains(t, err, "store-assigned")

	first, err := store.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-1",
		DeliveryKind: CallbackDeliveryKindOperation,
	})
	require.NoError(t, err)
	second, err := store.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-1",
		DeliveryKind: CallbackDeliveryKindOperation,
	})
	require.NoError(t, err)
	assert.NotZero(t, first.Sequence)
	assert.Greater(t, second.Sequence, first.Sequence)
}

func TestCallbackStore_SequenceDefinesFIFOWithEqualOrReversedCreatedAt(t *testing.T) {
	for _, tc := range []struct {
		name            string
		firstCreatedAt  time.Time
		secondCreatedAt time.Time
	}{
		{
			name:            "equal timestamps",
			firstCreatedAt:  time.Unix(1_700_000_000, 0),
			secondCreatedAt: time.Unix(1_700_000_000, 0),
		},
		{
			name:            "timestamps run backwards",
			firstCreatedAt:  time.Unix(1_800_000_000, 0),
			secondCreatedAt: time.Unix(1_600_000_000, 0),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
			require.NoError(t, err)
			defer store.Close()

			first, err := store.StoreEntry(CallbackEntry{
				LeaseUUID:    "lease-1",
				CallbackURL:  "http://example.com/first",
				DeliveryKind: CallbackDeliveryKindOperation,
				CreatedAt:    tc.firstCreatedAt,
			})
			require.NoError(t, err)
			second, err := store.StoreEntry(CallbackEntry{
				LeaseUUID:    "lease-1",
				CallbackURL:  "http://example.com/second",
				DeliveryKind: CallbackDeliveryKindOperation,
				CreatedAt:    tc.secondCreatedAt,
			})
			require.NoError(t, err)

			pending, err := store.listPending("lease-1")
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

	previous, err := store.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "http://example.com/previous",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       "failed",
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackV2BucketName).SetSequence(math.MaxUint64)
	}))

	_, err = store.StoreEntry(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "http://example.com/replacement",
		DeliveryKind: CallbackDeliveryKindLifecycle,
		Status:       "success",
		CreatedAt:    time.Now().Add(time.Second),
	})
	require.ErrorContains(t, err, "callback sequence exhausted")

	pending, err := store.listPending("lease-1")
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
		LeaseUUID:   "lease-1",
		CallbackURL: "http://example.com/legacy",
		Success:     false,
		CreatedAt:   createdAt,
	})
	require.NoError(t, store.Store(CallbackEntry{
		LeaseUUID:    "lease-1",
		CallbackURL:  "http://example.com/v2",
		DeliveryKind: CallbackDeliveryKindOperation,
		Success:      true,
		CreatedAt:    createdAt.Add(time.Second),
	}))
	require.NoError(t, store.Close())

	store, err = NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	assert.Empty(t, pending[0].DeliveryID, "v0.13 entries do not carry a delivery ID")
	assert.NotEmpty(t, pending[1].DeliveryID)

	require.NoError(t, store.RemoveEntry(pending[0]))
	pending, err = store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "http://example.com/v2", pending[0].CallbackURL)
}

func TestCallbackStore_RejectsDuplicateDeliveryID(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	const deliveryID = "550e8400-e29b-41d4-a716-446655440000"
	first, err := store.StoreEntry(CallbackEntry{
		DeliveryID:   deliveryID,
		LeaseUUID:    "lease-1",
		CallbackURL:  "http://example.com/first",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	_, err = store.StoreEntry(CallbackEntry{
		DeliveryID:   deliveryID,
		LeaseUUID:    "lease-2",
		CallbackURL:  "http://example.com/second",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now(),
	})
	require.ErrorContains(t, err, "already exists")
	require.ErrorContains(t, store.RemoveEntry(CallbackEntry{DeliveryID: deliveryID}), "no durable storage capability",
		"a public identity without StoreEntry/ListPending's storage capability must not authorize deletion")

	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, first.LeaseUUID, pending[0].LeaseUUID)
	assert.Equal(t, first.CallbackURL, pending[0].CallbackURL)
}

func TestCallbackStore_RemoveLeaseDoesNotMatchV2DeliveryID(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	const deliveryID = "550e8400-e29b-41d4-a716-446655440000"
	require.NoError(t, store.Store(CallbackEntry{
		DeliveryID:   deliveryID,
		LeaseUUID:    "actual-lease-owner",
		CallbackURL:  "http://example.com/callback",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now(),
	}))

	// Lease-wide removal must inspect the v2 value, never confuse its UUID key
	// with an unrelated lease that happens to have the same UUID.
	require.NoError(t, store.Remove(deliveryID))
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "actual-lease-owner", pending[0].LeaseUUID)

	require.NoError(t, store.Remove("actual-lease-owner"))
	pending, err = store.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestCallbackStore_RemoveEntryRejectsStaleCapabilityAfterIDReuse(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	const deliveryID = "550e8400-e29b-41d4-a716-446655440000"
	first, err := store.StoreEntry(CallbackEntry{
		DeliveryID:   deliveryID,
		LeaseUUID:    "lease-1",
		CallbackURL:  "http://example.com/first",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, store.RemoveEntry(first))
	_, err = store.StoreEntry(CallbackEntry{
		DeliveryID:   deliveryID,
		LeaseUUID:    "lease-2",
		CallbackURL:  "http://example.com/reused",
		DeliveryKind: CallbackDeliveryKindOperation,
		CreatedAt:    time.Now().Add(time.Second),
	})
	require.NoError(t, err)

	require.ErrorContains(t, store.RemoveEntry(first), "changed before precise removal",
		"a stale storage capability must not delete a different delivery that reused its ID")
	pending, err := store.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "lease-2", pending[0].LeaseUUID)
}

func TestCallbackStore_ListPendingRejectsMismatchedV2Identity(t *testing.T) {
	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
	require.NoError(t, err)
	defer store.Close()

	entry := CallbackEntry{
		DeliveryID:  "6ba7b810-9dad-41d1-80b4-00c04fd430c8",
		LeaseUUID:   "lease-1",
		CallbackURL: "http://example.com/callback",
		CreatedAt:   time.Now(),
	}
	data, err := json.Marshal(entry)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackV2BucketName).Put(
			[]byte("6ba7b811-9dad-41d1-80b4-00c04fd430c8"), data,
		)
	}))

	pending, err := store.ListPending()
	require.ErrorContains(t, err, "callback delivery identity mismatch")
	assert.Empty(t, pending, "an invalid durable identity must fail closed instead of being skipped")
}

func TestCallbackStore_ListPendingFailsClosedOnMalformedRecord(t *testing.T) {
	for _, bucketName := range [][]byte{callbackBucketName, callbackV2BucketName} {
		t.Run(string(bucketName), func(t *testing.T) {
			store, err := NewCallbackStore(CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "cb.db")})
			require.NoError(t, err)
			defer store.Close()
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(bucketName).Put([]byte("corrupt-key"), []byte("{"))
			}))

			pending, err := store.ListPending()
			require.ErrorContains(t, err, "failed to decode callback entry")
			assert.Empty(t, pending, "corruption must be a delivery barrier, not a skipped record")
		})
	}
}

func storeLegacyCallback(t *testing.T, store *CallbackStore, entry CallbackEntry) {
	t.Helper()
	data, err := json.Marshal(entry)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackBucketName).Put([]byte(entry.LeaseUUID), data)
	}))
}

func storePreKindV2Callback(t *testing.T, store *CallbackStore, entry CallbackEntry) {
	t.Helper()
	require.NoError(t, validateCallbackDeliveryID(entry.DeliveryID))
	data, err := json.Marshal(entry)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackV2BucketName).Put([]byte(entry.DeliveryID), data)
	}))
}
