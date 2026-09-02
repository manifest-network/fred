package shared

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
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
			Status:    "deploying",
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
		err := store.UpdateLatestStatus("lease-1", "superseded", "", "")
		require.NoError(t, err)

		latest, err := store.Latest("lease-1")
		require.NoError(t, err)
		assert.Equal(t, "superseded", latest.Status)
	})

	t.Run("update latest status with reason and message", func(t *testing.T) {
		err := store.Append("lease-1", Release{
			Manifest:  []byte(`{"image":"nginx:3.0"}`),
			Image:     "nginx:3.0",
			Status:    "deploying",
			CreatedAt: time.Now(),
		})
		require.NoError(t, err)

		err = store.UpdateLatestStatus("lease-1", "failed", backend.ReasonImagePullFailed, "image pull failed")
		require.NoError(t, err)

		latest, err := store.Latest("lease-1")
		require.NoError(t, err)
		assert.Equal(t, "failed", latest.Status)
		assert.Equal(t, backend.ReasonImagePullFailed, latest.Reason)
		assert.Equal(t, "image pull failed", latest.Message)
	})

	t.Run("update latest on nonexistent is no-op", func(t *testing.T) {
		err := store.UpdateLatestStatus("nonexistent", "active", "", "")
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

func TestUpdateLatestStatus_PersistsReasonMessage(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "reason_message.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.Append("l1", Release{
		Image:     "nginx:1.0",
		Status:    "deploying",
		CreatedAt: time.Now(),
	}))

	require.NoError(t, store.UpdateLatestStatus("l1", "failed", backend.ReasonUpdateFailed, "update failed"))

	rels, err := store.List("l1")
	require.NoError(t, err)
	require.NotEmpty(t, rels)
	last := rels[len(rels)-1]
	assert.Equal(t, "failed", last.Status)
	assert.Equal(t, backend.ReasonUpdateFailed, last.Reason)
	assert.Equal(t, "update failed", last.Message)
}

func TestReleaseStore_ActivateLatest(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "activate_releases.db")

	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	// Create 3 releases: v1 active, v2 superseded, v3 deploying.
	require.NoError(t, store.Append("lease-1", Release{
		Image:     "nginx:1.0",
		Status:    "active",
		CreatedAt: time.Now(),
	}))
	require.NoError(t, store.Append("lease-1", Release{
		Image:     "nginx:2.0",
		Status:    "superseded",
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

	t.Run("rejects a second active release without changing the first", func(t *testing.T) {
		require.NoError(t, store.Append("lease-3", Release{
			Image:  "app:1.0",
			Status: "active",
		}))
		err := store.Append("lease-3", Release{
			Image:  "app:2.0",
			Status: "active",
		})
		require.ErrorContains(t, err, "active records")

		rel, err := store.LatestActive("lease-3")
		require.NoError(t, err)
		require.NotNil(t, rel)
		assert.Equal(t, "app:1.0", rel.Image)
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

func TestReleaseStore_RuntimeInspectionStreamsPastOfflineAggregateLimits(t *testing.T) {
	store, err := NewReleaseStore(ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "runtime_streaming_releases.db"),
	})
	require.NoError(t, err)
	defer store.Close()

	for _, leaseUUID := range []string{
		"00000000-0000-4000-8000-000000000001",
		"00000000-0000-4000-8000-000000000002",
	} {
		require.NoError(t, store.AppendActive(leaseUUID, Release{
			Manifest:  []byte{},
			Status:    "active",
			CreatedAt: time.Now(),
		}))
	}

	// Runtime open/health validates every record independently and therefore
	// remains healthy when legitimate aggregate growth exceeds an offline
	// collection budget.
	require.NoError(t, store.Healthy())

	t.Run("stopped row ceiling", func(t *testing.T) {
		err := store.view(func(tx *bolt.Tx) error {
			budget := newAuthoritativeInspectionBudget(authoritativeInspectionLimits{
				maxRows:        1,
				maxRecordBytes: 1 << 20,
				maxTotalBytes:  1 << 20,
			})
			_, inspectErr := inspectReleaseBucketWithObserver(
				tx, nil, nil, nil, false, budget.observe,
			)
			return inspectErr
		})
		require.ErrorContains(t, err, "1-record cutover ceiling")
		require.ErrorContains(t, err, "matching predecessor")
	})

	t.Run("stopped byte ceiling", func(t *testing.T) {
		err := store.view(func(tx *bolt.Tx) error {
			budget := newAuthoritativeInspectionBudget(authoritativeInspectionLimits{
				maxRows:        10,
				maxRecordBytes: 1 << 20,
				maxTotalBytes:  1,
			})
			_, inspectErr := inspectReleaseBucketWithObserver(
				tx, nil, nil, nil, false, budget.observe,
			)
			return inspectErr
		})
		require.ErrorContains(t, err, "1-byte logical cutover ceiling")
		require.ErrorContains(t, err, "matching predecessor")
	})
}

func TestReleaseProjectionResponseCeilingCoversWorstLegacyExpansion(t *testing.T) {
	stored := Release{
		Version:  1,
		Manifest: []byte{},
		Status:   "failed",
	}
	require.NoError(t, validateStoredRelease(stored))

	storedElement, err := json.Marshal(stored)
	require.NoError(t, err)
	projectedElement, err := json.Marshal(backend.ReleaseInfo{
		Version:   stored.Version,
		Image:     stored.Image,
		Status:    stored.Status,
		CreatedAt: stored.CreatedAt,
		Reason:    backend.ReasonUnknown,
		Message:   stored.Message,
		Manifest:  stored.Manifest,
	})
	require.NoError(t, err)
	require.Greater(t, len(projectedElement), len(storedElement))

	// This is the smallest valid failed legacy row: one-digit version, empty
	// image, empty non-nil manifest, and zero timestamp. Every additional array
	// element costs at least its encoded length plus one comma. The only
	// read-boundary expansion is the synthesized ReasonUnknown field.
	perEntryStoredBytes := int64(len(storedElement) + 1)
	maxEntries := (int64(backend.MaxStoredReleaseHistoryBytes) - 1) / perEntryStoredBytes
	perEntryExpansion := int64(len(projectedElement) - len(storedElement))
	worstProjectedBytes := int64(backend.MaxStoredReleaseHistoryBytes) +
		maxEntries*perEntryExpansion

	require.Greater(t, worstProjectedBytes, int64(backend.MaxStoredReleaseHistoryBytes))
	require.LessOrEqual(
		t,
		worstProjectedBytes,
		int64(backend.MaxProjectedReleasesResponseBytes),
	)
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

// A restart/update can supersede the migration release before its background
// rollback-window cleanup fires. The old row is then the only durable fact that
// names the exact `-prev` cohort, so TTL pruning must retain it alongside the
// current active release and the index-latest version holder.
func TestReleaseStore_RemoveOlderThan_KeepsSupersededMigrationAuthority(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "keep_migration_authority.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	old := time.Now().Add(-100 * 24 * time.Hour)
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}}
	resourceProfiles := []SKUResourceSnapshot{{
		SKU: "docker-small", CPUCores: 1, MemoryMB: 512, DiskMB: 1024,
	}}
	manifestAuthority := []byte(`{"services":{"app":{"image":"example.invalid/app:1"}}}`)
	require.NoError(t, store.Append("lease-1", Release{
		Image: "migrated", Manifest: manifestAuthority, Items: items,
		ResourceProfiles: resourceProfiles, LegacyMigration: true,
		Status: "superseded", CreatedAt: old,
	}))
	require.NoError(t, store.Append("lease-1", Release{
		Image: "current", Manifest: manifestAuthority, Items: items,
		ResourceProfiles: resourceProfiles, Status: "active", CreatedAt: old,
	}))
	require.NoError(t, store.Append("lease-1", Release{
		Image: "failed-candidate", Manifest: manifestAuthority, Items: items,
		ResourceProfiles: resourceProfiles, Status: "failed", CreatedAt: old,
	}))

	removed, err := store.RemoveOlderThan(90 * 24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 0, removed,
		"migration authority, current active release, and index-latest version must all survive")

	releases, err := store.List("lease-1")
	require.NoError(t, err)
	require.Len(t, releases, 3)
	assert.True(t, releases[0].LegacyMigration)
	assert.Equal(t, items, releases[0].Items)
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

// Empty release history is ambiguous authority, not disposable history. The
// reaper must preserve its bytes and fail closed so an operator can repair it.
func TestReleaseStore_RemoveOlderThan_EmptyValueFailsClosed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "empty_value.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(releasesBucketName).Put([]byte("empty-lease"), []byte("[]"))
	}))

	removed, err := store.RemoveOlderThan(90 * 24 * time.Hour)
	require.ErrorContains(t, err, "release history is empty")
	assert.Equal(t, 0, removed)

	_, err = store.List("empty-lease")
	require.ErrorContains(t, err, "release history is empty")
	keys, err := store.LeaseUUIDs()
	require.NoError(t, err)
	assert.Contains(t, keys, "empty-lease")
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

// A corrupt exact release must never be erased by TTL cleanup: doing so would
// let recovery infer a smaller desired cohort from partial survivors.
func TestReleaseStore_RemoveOlderThan_CorruptValueFailsClosed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "corrupt_reap.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(releasesBucketName).Put([]byte("corrupt-lease"), []byte("not valid json"))
	}))

	removed, err := store.RemoveOlderThan(90 * 24 * time.Hour)
	require.ErrorContains(t, err, "corrupted release history")
	assert.Equal(t, 0, removed)

	_, err = store.List("corrupt-lease")
	require.Error(t, err, "the original corrupt bytes must remain visible")
	keys, err := store.LeaseUUIDs()
	require.NoError(t, err)
	assert.Contains(t, keys, "corrupt-lease")
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

func TestReleaseStore_RecordMigrationBackfillsDesiredItemsIdempotently(t *testing.T) {
	store, err := NewReleaseStore(ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "migration_releases.db"),
	})
	require.NoError(t, err)
	defer store.Close()

	manifest := []byte(`{"image":"nginx:1.25"}`)
	require.NoError(t, store.Append("lease-1", Release{
		Manifest: manifest, Image: "nginx:1.25", Status: "active", CreatedAt: time.Now(),
	}))
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 2, ServiceName: "app"}}
	profiles, err := BuildSKUResourceSnapshot(items, func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: 1024}, nil
	})
	require.NoError(t, err)
	require.NoError(t, store.RecordMigration("lease-1", manifest, items, profiles))
	require.NoError(t, store.RecordMigration("lease-1", manifest, items, profiles))

	releases, err := store.List("lease-1")
	require.NoError(t, err)
	require.Len(t, releases, 1, "backfill and exact replay must not inflate release history")
	assert.Equal(t, items, releases[0].Items)
	assert.True(t, releases[0].LegacyMigration)

	items[0].Quantity = 9
	assert.Equal(t, 2, releases[0].Items[0].Quantity, "stored desired topology must not alias caller memory")
}

func TestReleaseStore_RecordMigrationRejectsDivergentDesiredItems(t *testing.T) {
	store, err := NewReleaseStore(ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "migration_divergence.db"),
	})
	require.NoError(t, err)
	defer store.Close()

	manifest := []byte(`{"image":"nginx:1.25"}`)
	firstItems := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: "app",
	}}
	firstProfiles, err := BuildSKUResourceSnapshot(firstItems, func(string) (SKUProfile, error) {
		return SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: 1024}, nil
	})
	require.NoError(t, err)
	require.NoError(t, store.RecordMigration("lease-1", manifest, firstItems, firstProfiles))
	secondItems := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 2, ServiceName: "app",
	}}
	err = store.RecordMigration("lease-1", manifest, secondItems, firstProfiles)
	require.ErrorContains(t, err, "divergent desired items")
}

func TestReleaseStore_BackfillLegacyActiveAuthorityFreezesMultiSKUProfile(t *testing.T) {
	store, err := NewReleaseStore(ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "legacy_authority.db"),
	})
	require.NoError(t, err)
	defer store.Close()

	legacy := Release{
		Manifest: []byte(`{"services":{"web":{"image":"nginx:1.25"},"db":{"image":"postgres:16"}}}`),
		Image:    "stack", Status: "active", CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, store.Append("lease-1", legacy))
	stored, err := store.LatestActive("lease-1")
	require.NoError(t, err)
	require.NotNil(t, stored)
	items := []backend.LeaseItem{
		{SKU: "docker-small", Quantity: 2, ServiceName: "web", CustomDomain: "web.example"},
		{SKU: "docker-large", Quantity: 1, ServiceName: "db"},
	}
	profiles, err := BuildSKUResourceSnapshot(items, func(sku string) (SKUProfile, error) {
		switch sku {
		case "docker-small":
			return SKUProfile{CPUCores: 1, MemoryMB: 512, DiskMB: 1024}, nil
		case "docker-large":
			return SKUProfile{CPUCores: 4, MemoryMB: 4096, DiskMB: 8192}, nil
		default:
			return SKUProfile{}, errors.New("unexpected SKU")
		}
	})
	require.NoError(t, err)
	err = store.BackfillLegacyActiveAuthority("lease-1", *stored, items, profiles, 0)
	require.ErrorContains(t, err, "authority class is invalid")
	require.NoError(t, store.BackfillLegacyActiveAuthority(
		"lease-1", *stored, items, profiles, LegacyActiveAuthorityWorkload,
	))
	require.NoError(t, store.BackfillLegacyActiveAuthority(
		"lease-1", *stored, items, profiles, LegacyActiveAuthorityWorkload,
	),
		"an exact retry after an uncertain commit must be idempotent")

	backfilled, err := store.LatestActive("lease-1")
	require.NoError(t, err)
	require.NotNil(t, backfilled)
	assert.Equal(t, items, backfilled.Items)
	assert.Equal(t, profiles, backfilled.ResourceProfiles)
	items[0].Quantity = 9
	profiles[0].MemoryMB = 1
	assert.Equal(t, 2, backfilled.Items[0].Quantity)
	small, found := LookupSKUResourceSnapshotRow(backfilled.ResourceProfiles, "docker-small")
	require.True(t, found)
	assert.Equal(t, int64(512), small.MemoryMB)

	changedFence := *stored
	changedFence.Image = "manually-rewritten"
	err = store.BackfillLegacyActiveAuthority(
		"lease-1", changedFence, backfilled.Items, backfilled.ResourceProfiles,
		LegacyActiveAuthorityWorkload,
	)
	require.ErrorContains(t, err, "changed before legacy authority backfill")
}

func TestReleaseStore_DeleteCloseHistory_ExactAndIdempotent(t *testing.T) {
	store, err := NewReleaseStore(ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "close_fence.db"),
	})
	require.NoError(t, err)
	defer store.Close()

	const leaseUUID = "lease-close"
	require.NoError(t, store.Append(leaseUUID, Release{
		Manifest: []byte(`{"services":{"app":{"image":"nginx:1.0"}}}`),
		Image:    "nginx:1.0", Status: "active", CreatedAt: time.Now(),
	}))
	require.NoError(t, store.Append(leaseUUID, Release{
		Manifest: []byte(`{"services":{"app":{"image":"nginx:2.0"}}}`),
		Image:    "nginx:2.0", Status: "deploying", CreatedAt: time.Now(),
	}))
	releases, err := store.List(leaseUUID)
	require.NoError(t, err)
	selected := releaseForCloseFence(releases)
	require.NotNil(t, selected)
	encoded, err := json.Marshal(selected)
	require.NoError(t, err)
	digest := sha256.Sum256(encoded)

	require.NoError(t, store.DeleteCloseHistory(leaseUUID, selected.Version, digest),
		"a close may retire its exact active history, including a preempted pending tail")
	require.NoError(t, store.DeleteCloseHistory(leaseUUID, selected.Version, digest),
		"replay after the delete commit must be idempotent")
	releases, err = store.List(leaseUUID)
	require.NoError(t, err)
	assert.Nil(t, releases)
}

func TestReleaseStore_DeleteCloseHistory_RejectsChangedOrUnexpectedHistory(t *testing.T) {
	store, err := NewReleaseStore(ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "close_fence_conflict.db"),
	})
	require.NoError(t, err)
	defer store.Close()

	const leaseUUID = "lease-close"
	require.NoError(t, store.Append(leaseUUID, Release{
		Image: "nginx:1.0", Status: "active", CreatedAt: time.Now(),
	}))
	releases, err := store.List(leaseUUID)
	require.NoError(t, err)
	encoded, err := json.Marshal(releaseForCloseFence(releases))
	require.NoError(t, err)
	digest := sha256.Sum256(encoded)

	require.NoError(t, store.UpdateLatestStatus(
		leaseUUID, "failed", backend.ReasonUpdateFailed, "changed after close admission",
	))
	err = store.DeleteCloseHistory(leaseUUID, releases[0].Version, digest)
	require.ErrorContains(t, err, "changed after close admission")
	remaining, err := store.List(leaseUUID)
	require.NoError(t, err)
	require.Len(t, remaining, 1, "a mismatched fence must preserve the evidence")

	const unexpectedLease = "lease-unexpected"
	require.NoError(t, store.Append(unexpectedLease, Release{
		Image: "nginx:1.0", Status: "active", CreatedAt: time.Now(),
	}))
	err = store.DeleteCloseHistory(unexpectedLease, 0, [sha256.Size]byte{})
	require.ErrorContains(t, err, "appeared after close admission")
	remaining, err = store.List(unexpectedLease)
	require.NoError(t, err)
	require.Len(t, remaining, 1)
}

func putRawReleaseHistory(t *testing.T, store *ReleaseStore, leaseUUID string, releases []Release) []byte {
	t.Helper()
	encoded, err := json.Marshal(releases)
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(releasesBucketName).Put([]byte(leaseUUID), encoded)
	}))
	return encoded
}

func rawReleaseHistory(t *testing.T, store *ReleaseStore, leaseUUID string) []byte {
	t.Helper()
	var encoded []byte
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		encoded = append(encoded, tx.Bucket(releasesBucketName).Get([]byte(leaseUUID))...)
		return nil
	}))
	return encoded
}

func TestReleaseStore_CorruptAuthorityFailsEveryDecodeMutationClosed(t *testing.T) {
	store, err := NewReleaseStore(ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "corrupt_authority.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	const leaseUUID = "lease-corrupt"
	items := []backend.LeaseItem{{SKU: "small", Quantity: 1, ServiceName: "app"}}
	profiles := []SKUResourceSnapshot{{
		SKU: "small", CPUCores: 1, MemoryMB: 512, DiskMB: 1024,
	}}
	corrupt := Release{
		Version:          1,
		Manifest:         []byte(`{"services":{"app":{"image":"nginx:1.0"}}}`),
		Image:            "nginx:1.0",
		OperationID:      "not-a-canonical-uuid-v4",
		Items:            items,
		ResourceProfiles: profiles,
		Status:           "active",
		CreatedAt:        time.Now().Add(-time.Hour),
	}
	wantRaw := putRawReleaseHistory(t, store, leaseUUID, []Release{corrupt})
	selected, err := json.Marshal(corrupt)
	require.NoError(t, err)
	digest := sha256.Sum256(selected)

	assertInvalid := func(name string, err error) {
		t.Helper()
		require.ErrorContains(t, err, "operation ID", name)
		assert.Equal(t, wantRaw, rawReleaseHistory(t, store, leaseUUID), name)
	}
	_, err = store.List(leaseUUID)
	assertInvalid("List", err)
	assertInvalid("Append", store.Append(leaseUUID, Release{
		Image: "nginx:2.0", Status: "deploying", CreatedAt: time.Now(),
	}))
	assertInvalid("BackfillActiveResourceProfiles", store.BackfillActiveResourceProfiles(
		leaseUUID, 1, items, profiles,
	))
	assertInvalid("UpdateLatestStatus", store.UpdateLatestStatus(
		leaseUUID, "failed", backend.ReasonUpdateFailed, "failed",
	))
	assertInvalid("ActivateLatest", store.ActivateLatest(leaseUUID))
	assertInvalid("RecordMigration", store.RecordMigration(
		leaseUUID, corrupt.Manifest, items, profiles,
	))
	assertInvalid("DeleteCloseHistory", store.DeleteCloseHistory(leaseUUID, 1, digest))
	_, err = store.RemoveOlderThan(time.Minute)
	assertInvalid("RemoveOlderThan", err)
}

func TestReleaseStore_ListRejectsCorruptItemsAndResourceProfiles(t *testing.T) {
	tests := map[string]Release{
		"invalid quantity": {
			Version: 1,
			Items:   []backend.LeaseItem{{SKU: "small", Quantity: 0, ServiceName: "app"}},
		},
		"profiles without items": {
			Version: 1,
			ResourceProfiles: []SKUResourceSnapshot{{
				SKU: "small", CPUCores: 1, MemoryMB: 512, DiskMB: 1024,
			}},
		},
		"profile coverage mismatch": {
			Version: 1,
			Items:   []backend.LeaseItem{{SKU: "small", Quantity: 1, ServiceName: "app"}},
			ResourceProfiles: []SKUResourceSnapshot{{
				SKU: "large", CPUCores: 2, MemoryMB: 1024, DiskMB: 2048,
			}},
		},
	}
	for name, corrupt := range tests {
		t.Run(name, func(t *testing.T) {
			store, err := NewReleaseStore(ReleaseStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "corrupt_resources.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })
			putRawReleaseHistory(t, store, "lease-corrupt", []Release{corrupt})

			_, err = store.List("lease-corrupt")
			require.Error(t, err)
		})
	}
}

func TestReleaseStore_RejectsInvalidManifestAuthorityBeforePersisting(t *testing.T) {
	tests := []struct {
		name     string
		manifest []byte
		wantErr  string
	}{
		{
			name:     "malformed",
			manifest: []byte(`{"services":`),
			wantErr:  "release manifest",
		},
		{
			name:     "topology mismatch",
			manifest: []byte(`{"services":{"worker":{"image":"example.invalid/worker:1"}}}`),
			wantErr:  "release manifest topology",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := NewReleaseStore(ReleaseStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "releases.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })

			const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
			err = store.AppendActive(leaseUUID, Release{
				Manifest: test.manifest,
				Items: []backend.LeaseItem{{
					SKU: "small", Quantity: 1, ServiceName: "app",
				}},
				ResourceProfiles: []SKUResourceSnapshot{{
					SKU: "small", CPUCores: 1, MemoryMB: 512, DiskMB: 1024,
				}},
				CreatedAt: time.Now(),
			})
			require.ErrorContains(t, err, test.wantErr)

			releases, listErr := store.List(leaseUUID)
			require.NoError(t, listErr)
			assert.Empty(t, releases, "invalid recovery authority must not be persisted")
		})
	}
}

func TestReleaseStore_HealthAndRecoveryRejectPersistedManifestAuthority(t *testing.T) {
	tests := []struct {
		name     string
		manifest []byte
		wantErr  string
	}{
		{
			name:     "malformed",
			manifest: []byte(`{"services":`),
			wantErr:  "release manifest",
		},
		{
			name:     "topology mismatch",
			manifest: []byte(`{"services":{"worker":{"image":"example.invalid/worker:1"}}}`),
			wantErr:  "release manifest topology",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := NewReleaseStore(ReleaseStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "releases.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })

			const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
			corrupt := Release{
				Version:  1,
				Manifest: test.manifest,
				Items: []backend.LeaseItem{{
					SKU: "small", Quantity: 1, ServiceName: "app",
				}},
				ResourceProfiles: []SKUResourceSnapshot{{
					SKU: "small", CPUCores: 1, MemoryMB: 512, DiskMB: 1024,
				}},
				Status:    "active",
				CreatedAt: time.Now(),
			}
			wantRaw := putRawReleaseHistory(t, store, leaseUUID, []Release{corrupt})

			_, err = store.List(leaseUUID)
			require.ErrorContains(t, err, test.wantErr)
			require.ErrorContains(t, store.Healthy(), test.wantErr)
			assert.Equal(t, wantRaw, rawReleaseHistory(t, store, leaseUUID),
				"invalid recovery authority must remain quarantined")
		})
	}
}

func TestReleaseStore_AppendRejectsNonCanonicalOperationID(t *testing.T) {
	store, err := NewReleaseStore(ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "invalid_operation_id.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	err = store.Append("lease", Release{
		OperationID: "11111111-1111-4111-8111-11111111111A",
		Status:      "active",
		CreatedAt:   time.Now(),
	})
	require.ErrorContains(t, err, "canonical UUIDv4")
}

func TestReleaseStore_AssignsVersionBeforeValidationAndRollsBackDuplicateActive(t *testing.T) {
	store, err := NewReleaseStore(ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "release_invariants.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	const leaseUUID = "lease-1"
	require.NoError(t, store.Append(leaseUUID, Release{
		Version: 0, Status: "active", CreatedAt: time.Now(),
	}))
	releases, err := store.List(leaseUUID)
	require.NoError(t, err)
	require.Len(t, releases, 1)
	assert.Equal(t, 1, releases[0].Version)

	wantRaw := rawReleaseHistory(t, store, leaseUUID)
	err = store.Append(leaseUUID, Release{
		Version: 0, Status: "active", CreatedAt: time.Now(),
	})
	require.ErrorContains(t, err, "active records")
	assert.Equal(t, wantRaw, rawReleaseHistory(t, store, leaseUUID))

	require.NoError(t, store.Append(leaseUUID, Release{
		Version: 0, Status: "deploying", CreatedAt: time.Now(),
	}))
	wantRaw = rawReleaseHistory(t, store, leaseUUID)
	err = store.UpdateLatestStatus(leaseUUID, "active", "", "")
	require.ErrorContains(t, err, "active records")
	assert.Equal(t, wantRaw, rawReleaseHistory(t, store, leaseUUID))

	releases, err = store.List(leaseUUID)
	require.NoError(t, err)
	require.Len(t, releases, 2)
	assert.Equal(t, []int{1, 2}, []int{releases[0].Version, releases[1].Version})
	assert.Equal(t, []string{"active", "deploying"}, []string{releases[0].Status, releases[1].Status})
}

func TestReleaseStore_AppendActiveAtomicallySupersedesAndAssignsVersion(t *testing.T) {
	store, err := NewReleaseStore(ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "append_active.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	const leaseUUID = "lease-1"
	require.NoError(t, store.Append(leaseUUID, Release{
		Status: "active", CreatedAt: time.Now(),
	}))
	require.NoError(t, store.AppendActive(leaseUUID, Release{
		CreatedAt: time.Now(),
	}))

	releases, err := store.List(leaseUUID)
	require.NoError(t, err)
	require.Len(t, releases, 2)
	assert.Equal(t, []int{1, 2}, []int{releases[0].Version, releases[1].Version})
	assert.Equal(t, []string{"superseded", "active"},
		[]string{releases[0].Status, releases[1].Status})
}

func TestReleaseStore_AppendActiveCapacityCompactsDeterministically(t *testing.T) {
	store, err := NewReleaseStore(ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "append_active_capacity.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	store.maxAge = 24 * time.Hour

	const leaseUUID = "lease-capacity"
	now := time.Now()
	seed := []Release{
		{Version: 1, Image: "expired", Status: "superseded", CreatedAt: now.Add(-48 * time.Hour), Error: "expired-history"},
		{Version: 2, Image: "oldest-fresh", Status: "superseded", CreatedAt: now.Add(-3 * time.Hour), Error: "fresh-history"},
		{Version: 3, Image: "newer-fresh", Status: "superseded", CreatedAt: now.Add(-2 * time.Hour), Error: "fresh-history"},
		{Version: 4, Image: "migration", Status: "superseded", CreatedAt: now.Add(-time.Hour), LegacyMigration: true},
		{Version: 5, Image: "old-active", Status: "active", CreatedAt: now.Add(-30 * time.Minute)},
	}
	raw := putRawReleaseHistory(t, store, leaseUUID, seed)
	candidate := Release{Image: "new-active", Status: "active", CreatedAt: now}
	full, _, err := planAppendedReleaseHistory(
		raw,
		leaseUUID,
		candidate,
		true,
		now.Add(-24*time.Hour),
		1<<20,
	)
	require.NoError(t, err)
	require.Len(t, full, 6)
	// Force exactly two removals. The expired v1 must go before the oldest
	// still-fresh v2; newer v3 and the now-superseded v5 remain audit history.
	want := []Release{full[2], full[3], full[4], full[5]}
	wantBytes, err := json.Marshal(want)
	require.NoError(t, err)

	require.NoError(t, store.appendWithinLimit(leaseUUID, candidate, true, len(wantBytes)))
	assert.Equal(t, wantBytes, rawReleaseHistory(t, store, leaseUUID))
	got, err := store.List(leaseUUID)
	require.NoError(t, err)
	require.Len(t, got, 4)
	assert.Equal(t, []int{3, 4, 5, 6}, []int{
		got[0].Version, got[1].Version, got[2].Version, got[3].Version,
	})
	assert.True(t, got[1].LegacyMigration, "latest migration cleanup authority must survive")
	assert.Equal(t, "active", got[3].Status)

	// The retained index-latest v6 holds the pre-prune maximum, so the next
	// append issues v7 rather than reusing a deleted version.
	require.NoError(t, store.appendWithinLimit(
		leaseUUID,
		Release{Image: "after-compaction", Status: "failed", CreatedAt: now.Add(time.Minute)},
		false,
		1<<20,
	))
	latest, err := store.Latest(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.Equal(t, 7, latest.Version)
}

func TestReleaseStore_AppendActiveCapacityErrorRollsBackByteIdentically(t *testing.T) {
	store, err := NewReleaseStore(ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "append_active_capacity_rollback.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	const leaseUUID = "lease-capacity-rollback"
	now := time.Now()
	wantRaw := putRawReleaseHistory(t, store, leaseUUID, []Release{
		{Version: 1, Image: "migration", Status: "superseded", CreatedAt: now.Add(-time.Hour), LegacyMigration: true, Error: "protected-migration-authority"},
		{Version: 2, Image: "old-active", Status: "active", CreatedAt: now},
	})
	candidate := Release{Image: "new-active", Status: "active", CreatedAt: now.Add(time.Minute)}
	full, _, err := planAppendedReleaseHistory(
		wantRaw,
		leaseUUID,
		candidate,
		true,
		time.Time{},
		1<<20,
	)
	require.NoError(t, err)
	protected := []Release{full[0], full[2]}
	protectedBytes, err := json.Marshal(protected)
	require.NoError(t, err)

	err = store.appendWithinLimit(leaseUUID, candidate, true, len(protectedBytes)-1)
	require.ErrorIs(t, err, ErrReleaseHistoryCapacity)
	var capacityErr *ReleaseHistoryCapacityError
	require.ErrorAs(t, err, &capacityErr)
	assert.Equal(t, len(protectedBytes)-1, capacityErr.LimitBytes)
	assert.Equal(t, len(protectedBytes), capacityErr.RequiredBytes)
	assert.Equal(t, wantRaw, rawReleaseHistory(t, store, leaseUUID))
}

func TestReleaseStore_AppendActiveCapacityProofConvergesAfterColdReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "append_active_capacity_cold_reopen.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	const leaseUUID = "lease-capacity-cold-reopen"
	now := time.Now()
	seedRaw := putRawReleaseHistory(t, store, leaseUUID, []Release{
		{Version: 1, Image: "discardable", Status: "superseded", CreatedAt: now.Add(-2 * time.Hour)},
		{Version: 2, Image: "migration", Status: "superseded", CreatedAt: now.Add(-time.Hour), LegacyMigration: true},
		{Version: 3, Image: "old-active", Status: "active", CreatedAt: now.Add(-time.Minute)},
	})
	candidate := Release{Image: "recovered-active", Status: "active", CreatedAt: now}
	full, _, err := planAppendedReleaseHistory(
		seedRaw,
		leaseUUID,
		candidate,
		true,
		time.Time{},
		1<<20,
	)
	require.NoError(t, err)
	// Only the most-recent migration and new active row are load-bearing after
	// the success boundary. Use their exact encoded size as a tiny injected
	// ceiling so both disposable rows must be compacted.
	want := []Release{full[1], full[3]}
	wantRaw, err := json.Marshal(want)
	require.NoError(t, err)

	require.NoError(t, store.checkAppendCapacityWithinLimit(
		leaseUUID,
		candidate,
		true,
		len(wantRaw),
	))
	assert.Equal(t, seedRaw, rawReleaseHistory(t, store, leaseUUID),
		"pre-admission proof must remain read-only")
	require.NoError(t, store.Close())

	store, err = NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	require.NoError(t, store.appendWithinLimit(leaseUUID, candidate, true, len(wantRaw)))
	require.NoError(t, store.Close())

	store, err = NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	assert.Equal(t, wantRaw, rawReleaseHistory(t, store, leaseUUID))
	latest, err := store.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.Equal(t, 4, latest.Version,
		"cold recovery must preserve version monotonicity while compacting")
}

func TestReleaseStore_DeployingAppendReservesActivationAndFailureCapacity(t *testing.T) {
	t.Run("activation_refused_before_append_when_only_migration_is_protected", func(t *testing.T) {
		store, err := NewReleaseStore(ReleaseStoreConfig{
			DBPath: filepath.Join(t.TempDir(), "activation_preflight.db"),
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })

		const leaseUUID = "lease-activation"
		now := time.Now()
		wantRaw := putRawReleaseHistory(t, store, leaseUUID, []Release{{
			Version: 1, Image: "migration", Status: "active", CreatedAt: now, LegacyMigration: true,
		}})
		candidate := Release{Image: "deploying", Status: "deploying", CreatedAt: now.Add(time.Minute)}
		currentShape, _, err := planAppendedReleaseHistory(
			wantRaw,
			leaseUUID,
			candidate,
			false,
			time.Time{},
			1<<20,
		)
		require.NoError(t, err)
		currentBytes, err := json.Marshal(currentShape)
		require.NoError(t, err)

		err = store.appendWithinLimit(leaseUUID, candidate, false, len(currentBytes))
		require.ErrorIs(t, err, ErrReleaseHistoryCapacity,
			"the one-byte-larger terminal activation must be refused before Compose")
		assert.Equal(t, wantRaw, rawReleaseHistory(t, store, leaseUUID))
	})

	t.Run("failed_status_drops_optional_metadata_instead_of_losing_terminal_state", func(t *testing.T) {
		store, err := NewReleaseStore(ReleaseStoreConfig{
			DBPath: filepath.Join(t.TempDir(), "failed_status_capacity.db"),
		})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, store.Close()) })

		const leaseUUID = "lease-failed-status"
		now := time.Now()
		raw := putRawReleaseHistory(t, store, leaseUUID, []Release{
			{Version: 1, Image: "migration", Status: "active", CreatedAt: now, LegacyMigration: true},
			{Version: 2, Image: "candidate", Status: "deploying", CreatedAt: now.Add(time.Minute)},
		})
		require.NoError(t, store.updateLatestStatusWithinLimit(
			leaseUUID,
			"failed",
			backend.ReasonUpdateFailed,
			"optional detail that cannot fit",
			len(raw),
		))
		got, err := store.List(leaseUUID)
		require.NoError(t, err)
		require.Len(t, got, 2)
		assert.Equal(t, "failed", got[1].Status)
		assert.Empty(t, got[1].Reason)
		assert.Empty(t, got[1].Message)
	})
}

func TestReleaseStoreInspection_ProvesLegacyBackfillCapacityBeforeMutation(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
	manifestBytes := []byte(`{"image":"example.invalid/app:1"}`)
	items := []backend.LeaseItem{{SKU: "small", Quantity: 1, ServiceName: "app"}}
	profiles := []SKUResourceSnapshot{{SKU: "small", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}}
	now := time.Now()
	expected := Release{
		Version: 2, Manifest: manifestBytes, Image: "example.invalid/app:1",
		Status: "active", CreatedAt: now,
	}
	candidate := cloneRelease(expected)
	candidate.Items = slices.Clone(items)
	candidate.ResourceProfiles = CloneSKUResourceSnapshot(profiles)
	candidate.LegacyMigration = true
	candidateBytes, err := json.Marshal([]Release{candidate})
	require.NoError(t, err)

	dbPath := filepath.Join(t.TempDir(), "legacy_backfill_capacity.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	putRawReleaseHistory(t, store, leaseUUID, []Release{
		{Version: 1, Image: "discardable", Status: "superseded", CreatedAt: now.Add(-time.Hour), Error: "historical detail"},
		expected,
	})
	require.NoError(t, store.Close())

	inspection, err := InspectReleaseStoreReadOnly(dbPath)
	require.NoError(t, err)
	require.NoError(t, inspection.checkLegacyActiveAuthorityCapacityWithinLimit(
		leaseUUID,
		expected,
		items,
		profiles,
		LegacyActiveAuthorityMigration,
		len(candidateBytes),
	))

	store, err = NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.backfillLegacyActiveAuthorityWithinLimit(
		leaseUUID,
		expected,
		items,
		profiles,
		LegacyActiveAuthorityMigration,
		len(candidateBytes),
	))
	assert.Equal(t, candidateBytes, rawReleaseHistory(t, store, leaseUUID))
}

func TestReleaseStoreInspection_RejectsIrreducibleProfileBackfillAtomically(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440001"
	items := []backend.LeaseItem{{SKU: "small", Quantity: 1, ServiceName: "app"}}
	profiles := []SKUResourceSnapshot{{SKU: "small", CPUCores: 1, MemoryMB: 512, DiskMB: 1024}}
	expected := Release{
		Version:  1,
		Manifest: []byte(`{"services":{"app":{"image":"example.invalid/app:1"}}}`),
		Image:    "stack", Items: items, Status: "active", CreatedAt: time.Now(),
	}
	dbPath := filepath.Join(t.TempDir(), "profile_backfill_capacity.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	wantRaw := putRawReleaseHistory(t, store, leaseUUID, []Release{expected})
	require.NoError(t, store.Close())

	inspection, err := InspectReleaseStoreReadOnly(dbPath)
	require.NoError(t, err)
	err = inspection.checkActiveResourceProfilesCapacityWithinLimit(
		leaseUUID,
		expected,
		profiles,
		len(wantRaw),
	)
	require.ErrorIs(t, err, ErrReleaseHistoryCapacity)

	store, err = NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	err = store.backfillActiveResourceProfilesWithinLimit(
		leaseUUID,
		expected.Version,
		expected.Items,
		profiles,
		len(wantRaw),
	)
	require.ErrorIs(t, err, ErrReleaseHistoryCapacity)
	assert.Equal(t, wantRaw, rawReleaseHistory(t, store, leaseUUID))
}
