package shared

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
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

func TestReleaseStore_LatestActive(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "latest_active.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	t.Run("returns active release skipping failed", func(t *testing.T) {
		require.NoError(t, store.Append("lease-1", Release{
			Image:     "postgres:17",
			Manifest:  []byte(`{"image":"postgres:17"}`),
			Status:    "active",
			CreatedAt: time.Now(),
		}))
		require.NoError(t, store.Append("lease-1", Release{
			Image:     "postgres:18",
			Manifest:  []byte(`{"image":"postgres:18"}`),
			Status:    "failed",
			CreatedAt: time.Now(),
		}))

		rel, err := store.LatestActive("lease-1")
		require.NoError(t, err)
		require.NotNil(t, rel)
		assert.Equal(t, "postgres:17", rel.Image)
		assert.Equal(t, "active", rel.Status)
	})

	t.Run("returns nil when no active release", func(t *testing.T) {
		require.NoError(t, store.Append("lease-2", Release{
			Image:  "app:1.0",
			Status: "failed",
		}))

		rel, err := store.LatestActive("lease-2")
		require.NoError(t, err)
		assert.Nil(t, rel)
	})

	t.Run("returns nil for nonexistent lease", func(t *testing.T) {
		rel, err := store.LatestActive("nonexistent")
		require.NoError(t, err)
		assert.Nil(t, rel)
	})

	t.Run("returns most recent active when multiple exist", func(t *testing.T) {
		require.NoError(t, store.Append("lease-3", Release{
			Image:  "app:1.0",
			Status: "active",
		}))
		require.NoError(t, store.Append("lease-3", Release{
			Image:  "app:2.0",
			Status: "active",
		}))

		rel, err := store.LatestActive("lease-3")
		require.NoError(t, err)
		require.NotNil(t, rel)
		assert.Equal(t, "app:2.0", rel.Image)
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
	// ENG-440: the old lease's lone "active" release is load-bearing (recoverState
	// rehydrates the manifest from it), so it is now RETAINED, not reaped.
	assert.Equal(t, 0, removed, "no prunable non-protected entries")

	releases, err := store.List("fresh-lease")
	require.NoError(t, err)
	assert.Len(t, releases, 1)

	releases, err = store.List("old-lease")
	require.NoError(t, err)
	assert.Len(t, releases, 1, "the lone old active release is protected and retained")
}

func TestReleaseStore_AppendCorruptedData(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "corrupt_releases.db")

	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	// Write corrupted (non-JSON) data directly into the bucket.
	err = store.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(releasesBucketName)
		return b.Put([]byte("corrupt-lease"), []byte("not valid json"))
	})
	require.NoError(t, err)

	// Append should detect the corruption and return an error instead of
	// silently discarding existing data.
	err = store.Append("corrupt-lease", Release{
		Image:     "nginx:latest",
		Status:    "deploying",
		CreatedAt: time.Now(),
	})
	require.Error(t, err, "Append should fail when existing data is corrupted")
	assert.Contains(t, err.Error(), "corrupted release data")

	// List should also return an error for the corrupted key.
	_, err = store.List("corrupt-lease")
	require.Error(t, err, "List should fail when data is corrupted")
}

func TestReleaseStore_InitialCleanup(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "initial_cleanup_releases.db")

	store1, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	require.NoError(t, store1.Append("old-lease", Release{
		Image:     "nginx:superseded",
		Status:    "superseded",
		CreatedAt: time.Now().Add(-48 * time.Hour),
	}))
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

	// ENG-440: the startup cleanup prunes the old superseded entry but RETAINS the
	// load-bearing active one (keep-latest guard) — proving the initial sweep still
	// runs and prunes, without destroying the manifest source.
	old, err := store2.List("old-lease")
	require.NoError(t, err)
	require.Len(t, old, 1, "old superseded entry pruned, active retained")
	assert.Equal(t, "nginx:old", old[0].Image)
	assert.Equal(t, "active", old[0].Status)

	fresh, err := store2.List("fresh-lease")
	require.NoError(t, err)
	assert.Len(t, fresh, 1)
}

// TestReleaseStore_Append_VersionDerivedFromMax verifies Append assigns the next
// version from max(existing.Version)+1, not len(releases)+1. This matters once
// RemoveOlderThan's keep-latest guard prunes entries from within a key, leaving a
// shorter slice whose surviving entry carries a higher version than its length
// (ENG-440). Seed such a slice directly so len(1) < maxVersion(5).
func TestReleaseStore_Append_VersionDerivedFromMax(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "version_max.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	seeded, err := json.Marshal([]Release{
		{Version: 5, Image: "nginx:5", Status: "active", CreatedAt: time.Now()},
	})
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(releasesBucketName).Put([]byte("lease-1"), seeded)
	}))

	require.NoError(t, store.Append("lease-1", Release{
		Image: "nginx:6", Status: "deploying", CreatedAt: time.Now(),
	}))

	latest, err := store.Latest("lease-1")
	require.NoError(t, err)
	assert.Equal(t, 6, latest.Version,
		"next version must be max(existing)+1 (6), never the len-derived 2")
}

// TestReleaseStore_RemoveOlderThan_KeepsLoneOldActive is the core ENG-440 guard:
// a lease running stably >=90d has a single provision-time "active" release, now
// older than the cutoff. The reaper must NOT delete it — recoverState rehydrates
// the StackManifest from this record, so reaping it makes the live lease
// un-Restartable.
func TestReleaseStore_RemoveOlderThan_KeepsLoneOldActive(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "keep_lone_active.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.Append("lease-1", Release{
		Manifest:  []byte(`{"image":"nginx:1.25"}`),
		Image:     "nginx:1.25",
		Status:    "active",
		CreatedAt: time.Now().Add(-100 * 24 * time.Hour),
	}))

	removed, err := store.RemoveOlderThan(90 * 24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 0, removed, "the lone active release is load-bearing and must be retained")

	active, err := store.LatestActive("lease-1")
	require.NoError(t, err)
	require.NotNil(t, active, "RemoveOlderThan must not destroy the manifest-rehydration source")
	assert.Equal(t, "nginx:1.25", active.Image)
}

// TestReleaseStore_RemoveOlderThan_PrunesOldSupersededTail confirms the reaper still
// does useful work: it prunes old superseded history while retaining the latest-active
// and index-latest entries.
func TestReleaseStore_RemoveOlderThan_PrunesOldSupersededTail(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "prune_tail.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	old := time.Now().Add(-100 * 24 * time.Hour)
	require.NoError(t, store.Append("lease-1", Release{Image: "v1", Status: "superseded", CreatedAt: old}))
	require.NoError(t, store.Append("lease-1", Release{Image: "v2", Status: "superseded", CreatedAt: old}))
	require.NoError(t, store.Append("lease-1", Release{Image: "v3", Status: "active", CreatedAt: old}))

	removed, err := store.RemoveOlderThan(90 * 24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 2, removed, "the two old superseded entries are pruned")

	releases, err := store.List("lease-1")
	require.NoError(t, err)
	require.Len(t, releases, 1, "only the protected active/index-latest entry remains")
	assert.Equal(t, "v3", releases[0].Image)
	assert.Equal(t, 3, releases[0].Version, "surviving entry keeps its original version")
}

// TestReleaseStore_RemoveOlderThan_KeepsOldActiveWhenNewestIsFailed locks the
// version-monotonicity coupling: when the newest entry is a FAILED release appended
// after the active one, latest-active and index-latest differ. Both are protected, so
// the global-max version (on the failed index-latest) is never pruned, keeping Append's
// max+1 collision-free.
func TestReleaseStore_RemoveOlderThan_KeepsOldActiveWhenNewestIsFailed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "active_then_failed.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	old := time.Now().Add(-100 * 24 * time.Hour)
	require.NoError(t, store.Append("lease-1", Release{Image: "v1", Status: "active", CreatedAt: old}))
	require.NoError(t, store.Append("lease-1", Release{Image: "v2", Status: "failed", CreatedAt: old}))

	removed, err := store.RemoveOlderThan(90 * 24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 0, removed, "latest-active (v1) and index-latest (v2) are both protected")

	active, err := store.LatestActive("lease-1")
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, "v1", active.Image, "the older active release is the rehydration source and must survive")

	// And a subsequent Append must not reuse v2 (the retained global max).
	require.NoError(t, store.Append("lease-1", Release{Image: "v3", Status: "deploying", CreatedAt: time.Now()}))
	latest, err := store.Latest("lease-1")
	require.NoError(t, err)
	assert.Equal(t, 3, latest.Version, "next version is max(2)+1=3, never a reused 2")
}

// TestReleaseStore_RemoveOlderThan_AppendAfterPruneNoVersionReuse is the end-to-end
// coupling capstone: prune actually removes entries, then Append must still issue a
// fresh version (proves Change 1 + Change 2 together).
func TestReleaseStore_RemoveOlderThan_AppendAfterPruneNoVersionReuse(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "prune_then_append.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	old := time.Now().Add(-100 * 24 * time.Hour)
	require.NoError(t, store.Append("lease-1", Release{Image: "v1", Status: "superseded", CreatedAt: old}))
	require.NoError(t, store.Append("lease-1", Release{Image: "v2", Status: "superseded", CreatedAt: old}))
	require.NoError(t, store.Append("lease-1", Release{Image: "v3", Status: "active", CreatedAt: old}))

	_, err = store.RemoveOlderThan(90 * 24 * time.Hour) // prunes v1, v2 -> [v3]
	require.NoError(t, err)

	require.NoError(t, store.Append("lease-1", Release{Image: "v4", Status: "deploying", CreatedAt: time.Now()}))
	latest, err := store.Latest("lease-1")
	require.NoError(t, err)
	assert.Equal(t, 4, latest.Version, "after pruning to [v3], next version is max(3)+1=4, not len-derived 2")
}

// TestReleaseStore_RemoveOlderThan_EmptyValueRemoved guards the len(releases)==0 path:
// a null/[] value must be removed like a corrupt key, never panic the sweep.
func TestReleaseStore_RemoveOlderThan_EmptyValueRemoved(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "empty_value.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(releasesBucketName).Put([]byte("empty-lease"), []byte("[]"))
	}))

	removed, err := store.RemoveOlderThan(90 * 24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 1, removed, "an empty value is removed like a corrupt key")

	releases, err := store.List("empty-lease")
	require.NoError(t, err)
	assert.Nil(t, releases)
}

// TestReleaseStore_RemoveOlderThan_PrunesOldTailKeepsFreshActive exercises the age
// predicate on a MIXED-age key: an old superseded entry plus a FRESH active entry.
// Only the old non-protected entry is pruned; the fresh protected one is kept. (Today
// the all-or-nothing reaper keeps the whole key because not every entry is old, so this
// is a genuine RED->GREEN test of the new within-key pruning.)
func TestReleaseStore_RemoveOlderThan_PrunesOldTailKeepsFreshActive(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mixed_age.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.Append("lease-1", Release{Image: "v1", Status: "superseded", CreatedAt: time.Now().Add(-100 * 24 * time.Hour)}))
	require.NoError(t, store.Append("lease-1", Release{Image: "v2", Status: "active", CreatedAt: time.Now()})) // fresh

	removed, err := store.RemoveOlderThan(90 * 24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 1, removed, "only the old superseded entry is pruned")

	releases, err := store.List("lease-1")
	require.NoError(t, err)
	require.Len(t, releases, 1)
	assert.Equal(t, "v2", releases[0].Image)
}

// TestReleaseStore_RemoveOlderThan_CorruptValueRemoved is a regression-lock (green
// before and after the fix) for the unparseable-value branch: a corrupt value is
// removed whole-key.
func TestReleaseStore_RemoveOlderThan_CorruptValueRemoved(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "corrupt_reap.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(releasesBucketName).Put([]byte("corrupt-lease"), []byte("not valid json"))
	}))

	removed, err := store.RemoveOlderThan(90 * 24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 1, removed, "an unparseable value is removed whole-key")

	releases, err := store.List("corrupt-lease")
	require.NoError(t, err)
	assert.Nil(t, releases)
}

// TestReleaseStore_RemoveOlderThan_KeepsLoneOldNonActive covers the keepActive == -1
// branch: a lease that only ever failed to deploy (no "active" release) and whose sole
// entry is older than the cutoff. The index-latest guard alone must retain it — the
// reaper never empties a non-corrupt key — so the record is preserved (cosmetic-only,
// but a regression anchor for the no-active path).
func TestReleaseStore_RemoveOlderThan_KeepsLoneOldNonActive(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "lone_failed.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.Append("lease-1", Release{
		Image:     "nginx:failed",
		Status:    "failed",
		CreatedAt: time.Now().Add(-100 * 24 * time.Hour),
	}))

	removed, err := store.RemoveOlderThan(90 * 24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 0, removed, "the index-latest entry is protected even with no active release")

	releases, err := store.List("lease-1")
	require.NoError(t, err)
	require.Len(t, releases, 1, "a non-corrupt key is never emptied")
	assert.Equal(t, "failed", releases[0].Status)
}
