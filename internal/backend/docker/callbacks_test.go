package docker

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCallbackStore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_callbacks.db")

	store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	t.Run("store and list", func(t *testing.T) {
		entry := CallbackEntry{
			LeaseUUID:   "lease-1",
			CallbackURL: "http://localhost/cb",
			Success:     true,
			CreatedAt:   time.Now(),
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
			LeaseUUID:   "lease-2",
			CallbackURL: "http://localhost/cb",
			Success:     false,
			Error:       "container crashed",
			CreatedAt:   time.Now(),
		}
		err := store.Store(entry)
		require.NoError(t, err)

		pending, err := store.ListPending()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		assert.False(t, pending[0].Success)
		assert.Equal(t, "container crashed", pending[0].Error)
	})

	t.Run("overwrite existing entry", func(t *testing.T) {
		entry := CallbackEntry{
			LeaseUUID:   "lease-2",
			CallbackURL: "http://localhost/cb2",
			Success:     true,
			CreatedAt:   time.Now(),
		}
		err := store.Store(entry)
		require.NoError(t, err)

		pending, err := store.ListPending()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		assert.Equal(t, "http://localhost/cb2", pending[0].CallbackURL)
		assert.True(t, pending[0].Success)
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
		LeaseUUID:   "lease-persist",
		CallbackURL: "http://localhost/persist",
		Success:     false,
		Error:       "some error",
		CreatedAt:   time.Now(),
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
		LeaseUUID:   "lease-old",
		CallbackURL: "http://example.com",
		Success:     true,
		CreatedAt:   time.Now().Add(-48 * time.Hour),
	}))
	require.NoError(t, store1.Store(CallbackEntry{
		LeaseUUID:   "lease-fresh",
		CallbackURL: "http://example.com",
		Success:     true,
		CreatedAt:   time.Now(),
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
