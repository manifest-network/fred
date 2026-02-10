package shared

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReleaseStore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_releases.db")

	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	t.Run("append and list", func(t *testing.T) {
		err := store.Append("lease-1", Release{
			Manifest:  []byte(`{"image":"nginx:1.0"}`),
			Image:     "nginx:1.0",
			Status:    "active",
			CreatedAt: time.Now(),
		})
		require.NoError(t, err)

		releases, err := store.List("lease-1")
		require.NoError(t, err)
		require.Len(t, releases, 1)
		assert.Equal(t, 1, releases[0].Version)
		assert.Equal(t, "nginx:1.0", releases[0].Image)
		assert.Equal(t, "active", releases[0].Status)
	})

	t.Run("append auto-increments version", func(t *testing.T) {
		err := store.Append("lease-1", Release{
			Manifest:  []byte(`{"image":"nginx:2.0"}`),
			Image:     "nginx:2.0",
			Status:    "active",
			CreatedAt: time.Now(),
		})
		require.NoError(t, err)

		releases, err := store.List("lease-1")
		require.NoError(t, err)
		require.Len(t, releases, 2)
		assert.Equal(t, 1, releases[0].Version)
		assert.Equal(t, 2, releases[1].Version)
	})

	t.Run("latest", func(t *testing.T) {
		latest, err := store.Latest("lease-1")
		require.NoError(t, err)
		require.NotNil(t, latest)
		assert.Equal(t, 2, latest.Version)
		assert.Equal(t, "nginx:2.0", latest.Image)
	})

	t.Run("latest not found", func(t *testing.T) {
		latest, err := store.Latest("nonexistent")
		require.NoError(t, err)
		assert.Nil(t, latest)
	})

	t.Run("list not found", func(t *testing.T) {
		releases, err := store.List("nonexistent")
		require.NoError(t, err)
		assert.Nil(t, releases)
	})

	t.Run("update latest status", func(t *testing.T) {
		err := store.UpdateLatestStatus("lease-1", "superseded", "")
		require.NoError(t, err)

		latest, err := store.Latest("lease-1")
		require.NoError(t, err)
		assert.Equal(t, "superseded", latest.Status)
	})

	t.Run("update latest status with error", func(t *testing.T) {
		err := store.Append("lease-1", Release{
			Manifest:  []byte(`{"image":"nginx:3.0"}`),
			Image:     "nginx:3.0",
			Status:    "deploying",
			CreatedAt: time.Now(),
		})
		require.NoError(t, err)

		err = store.UpdateLatestStatus("lease-1", "failed", "image pull failed")
		require.NoError(t, err)

		latest, err := store.Latest("lease-1")
		require.NoError(t, err)
		assert.Equal(t, "failed", latest.Status)
		assert.Equal(t, "image pull failed", latest.Error)
	})

	t.Run("update latest on nonexistent is no-op", func(t *testing.T) {
		err := store.UpdateLatestStatus("nonexistent", "active", "")
		require.NoError(t, err)
	})

	t.Run("delete", func(t *testing.T) {
		err := store.Delete("lease-1")
		require.NoError(t, err)

		releases, err := store.List("lease-1")
		require.NoError(t, err)
		assert.Nil(t, releases)
	})

	t.Run("delete nonexistent is no-op", func(t *testing.T) {
		err := store.Delete("nonexistent")
		require.NoError(t, err)
	})
}

func TestReleaseStore_ActivateLatest(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "activate_releases.db")

	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	// Create 3 releases: v1 active, v2 active, v3 deploying
	require.NoError(t, store.Append("lease-1", Release{
		Image:     "nginx:1.0",
		Status:    "active",
		CreatedAt: time.Now(),
	}))
	require.NoError(t, store.Append("lease-1", Release{
		Image:     "nginx:2.0",
		Status:    "active",
		CreatedAt: time.Now(),
	}))
	require.NoError(t, store.Append("lease-1", Release{
		Image:     "nginx:3.0",
		Status:    "deploying",
		CreatedAt: time.Now(),
	}))

	// Activate latest: v1,v2 should become superseded, v3 should become active
	err = store.ActivateLatest("lease-1")
	require.NoError(t, err)

	releases, err := store.List("lease-1")
	require.NoError(t, err)
	require.Len(t, releases, 3)
	assert.Equal(t, "superseded", releases[0].Status, "v1 should be superseded")
	assert.Equal(t, "superseded", releases[1].Status, "v2 should be superseded")
	assert.Equal(t, "active", releases[2].Status, "v3 should be active")
	assert.Empty(t, releases[2].Error, "v3 should have no error")

	t.Run("nonexistent is no-op", func(t *testing.T) {
		err := store.ActivateLatest("nonexistent")
		require.NoError(t, err)
	})

	t.Run("single release", func(t *testing.T) {
		require.NoError(t, store.Append("lease-single", Release{
			Image:     "redis:7",
			Status:    "deploying",
			CreatedAt: time.Now(),
		}))

		err := store.ActivateLatest("lease-single")
		require.NoError(t, err)

		releases, err := store.List("lease-single")
		require.NoError(t, err)
		require.Len(t, releases, 1)
		assert.Equal(t, "active", releases[0].Status)
	})

	t.Run("failed releases stay failed", func(t *testing.T) {
		require.NoError(t, store.Append("lease-mixed", Release{
			Image:     "app:1.0",
			Status:    "active",
			CreatedAt: time.Now(),
		}))
		require.NoError(t, store.Append("lease-mixed", Release{
			Image:     "app:2.0",
			Status:    "failed",
			Error:     "crash",
			CreatedAt: time.Now(),
		}))
		require.NoError(t, store.Append("lease-mixed", Release{
			Image:     "app:3.0",
			Status:    "deploying",
			CreatedAt: time.Now(),
		}))

		err := store.ActivateLatest("lease-mixed")
		require.NoError(t, err)

		releases, err := store.List("lease-mixed")
		require.NoError(t, err)
		require.Len(t, releases, 3)
		assert.Equal(t, "superseded", releases[0].Status, "v1 active->superseded")
		assert.Equal(t, "failed", releases[1].Status, "v2 stays failed")
		assert.Equal(t, "active", releases[2].Status, "v3 deploying->active")
	})
}

func TestReleaseStore_Persistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "persist_releases.db")

	store1, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	err = store1.Append("lease-persist", Release{
		Manifest:  []byte(`{"image":"redis:7"}`),
		Image:     "redis:7",
		Status:    "active",
		CreatedAt: time.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, store1.Close())

	store2, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store2.Close()

	releases, err := store2.List("lease-persist")
	require.NoError(t, err)
	require.Len(t, releases, 1)
	assert.Equal(t, "redis:7", releases[0].Image)
}

func TestReleaseStore_EmptyPath(t *testing.T) {
	_, err := NewReleaseStore(ReleaseStoreConfig{})
	assert.Error(t, err)
}

func TestReleaseStore_Healthy(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "healthy_releases.db")

	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	err = store.Healthy()
	require.NoError(t, err)
}

func TestReleaseStore_CloseIdempotent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "idempotent_releases.db")

	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	require.NoError(t, store.Close())
	require.NoError(t, store.Close())
}

func TestReleaseStore_RemoveOlderThan(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "releases_ttl.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	// Old lease - all releases are old
	require.NoError(t, store.Append("old-lease", Release{
		Image:     "nginx:old",
		Status:    "active",
		CreatedAt: time.Now().Add(-48 * time.Hour),
	}))

	// Fresh lease
	require.NoError(t, store.Append("fresh-lease", Release{
		Image:     "nginx:fresh",
		Status:    "active",
		CreatedAt: time.Now(),
	}))

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 1, removed)

	releases, err := store.List("fresh-lease")
	require.NoError(t, err)
	assert.Len(t, releases, 1)

	releases, err = store.List("old-lease")
	require.NoError(t, err)
	assert.Nil(t, releases)
}

func TestReleaseStore_InitialCleanup(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "initial_cleanup_releases.db")

	store1, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	require.NoError(t, store1.Append("old-lease", Release{
		Image:     "nginx:old",
		Status:    "active",
		CreatedAt: time.Now().Add(-48 * time.Hour),
	}))
	require.NoError(t, store1.Append("fresh-lease", Release{
		Image:     "nginx:fresh",
		Status:    "active",
		CreatedAt: time.Now(),
	}))
	require.NoError(t, store1.Close())

	store2, err := NewReleaseStore(ReleaseStoreConfig{
		DBPath: dbPath,
		MaxAge: 24 * time.Hour,
	})
	require.NoError(t, err)
	defer store2.Close()

	old, err := store2.List("old-lease")
	require.NoError(t, err)
	assert.Nil(t, old)

	fresh, err := store2.List("fresh-lease")
	require.NoError(t, err)
	assert.Len(t, fresh, 1)
}
