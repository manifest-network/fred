package shared

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticsStore(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_diagnostics.db")

	store, err := NewDiagnosticsStore(DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	t.Run("store and get", func(t *testing.T) {
		entry := DiagnosticEntry{
			LeaseUUID:    "lease-1",
			ProviderUUID: "prov-1",
			Tenant:       "tenant-a",
			Error:        "container exited unexpectedly: exit_code=1",
			FailCount:    1,
			CreatedAt:    time.Now(),
		}
		err := store.Store(entry)
		require.NoError(t, err)

		got, err := store.Get("lease-1")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "lease-1", got.LeaseUUID)
		assert.Equal(t, "prov-1", got.ProviderUUID)
		assert.Equal(t, "tenant-a", got.Tenant)
		assert.Equal(t, "container exited unexpectedly: exit_code=1", got.Error)
		assert.Equal(t, 1, got.FailCount)
	})

	t.Run("get with logs", func(t *testing.T) {
		entry := DiagnosticEntry{
			LeaseUUID:    "lease-2",
			ProviderUUID: "prov-1",
			Tenant:       "tenant-a",
			Error:        "container exited",
			Logs: map[string]string{
				"0": "line 1\nline 2\n",
				"1": "worker log\n",
			},
			FailCount: 2,
			CreatedAt: time.Now(),
		}
		require.NoError(t, store.Store(entry))

		got, err := store.Get("lease-2")
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Len(t, got.Logs, 2)
		assert.Equal(t, "line 1\nline 2\n", got.Logs["0"])
		assert.Equal(t, "worker log\n", got.Logs["1"])
	})

	t.Run("not found", func(t *testing.T) {
		got, err := store.Get("nonexistent")
		require.NoError(t, err)
		assert.Nil(t, got)
	})
}

func TestDiagnosticsStore_Persistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "persist_diagnostics.db")

	// Write an entry with logs
	store1, err := NewDiagnosticsStore(DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	err = store1.Store(DiagnosticEntry{
		LeaseUUID:    "lease-persist",
		ProviderUUID: "prov-1",
		Tenant:       "tenant-a",
		Error:        "some error",
		Logs:         map[string]string{"0": "persisted logs\n"},
		FailCount:    3,
		CreatedAt:    time.Now(),
	})
	require.NoError(t, err)
	require.NoError(t, store1.Close())

	// Reopen and verify entry survived
	store2, err := NewDiagnosticsStore(DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store2.Close()

	got, err := store2.Get("lease-persist")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "lease-persist", got.LeaseUUID)
	assert.Equal(t, "some error", got.Error)
	assert.Equal(t, 3, got.FailCount)
	require.Len(t, got.Logs, 1)
	assert.Equal(t, "persisted logs\n", got.Logs["0"])
}

func TestDiagnosticsStore_EmptyPath(t *testing.T) {
	_, err := NewDiagnosticsStore(DiagnosticsStoreConfig{})
	assert.Error(t, err)
}

func TestDiagnosticsStore_Healthy(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "healthy_diagnostics.db")

	store, err := NewDiagnosticsStore(DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	err = store.Healthy()
	require.NoError(t, err)
}

func TestDiagnosticsStore_CloseIdempotent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "idempotent_diagnostics.db")

	store, err := NewDiagnosticsStore(DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	// Close twice — should not panic
	require.NoError(t, store.Close())
	require.NoError(t, store.Close())
}

func TestDiagnosticsStore_InitialCleanup(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "initial_cleanup.db")

	// Create store without expiry, insert old entries
	store1, err := NewDiagnosticsStore(DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)

	require.NoError(t, store1.Store(DiagnosticEntry{
		LeaseUUID: "lease-old",
		Error:     "old error",
		FailCount: 1,
		CreatedAt: time.Now().Add(-48 * time.Hour),
	}))
	require.NoError(t, store1.Store(DiagnosticEntry{
		LeaseUUID: "lease-fresh",
		Error:     "fresh error",
		FailCount: 1,
		CreatedAt: time.Now(),
	}))
	require.NoError(t, store1.Close())

	// Reopen WITH expiry — initial cleanup should remove the old entry
	store2, err := NewDiagnosticsStore(DiagnosticsStoreConfig{
		DBPath: dbPath,
		MaxAge: 24 * time.Hour,
	})
	require.NoError(t, err)
	defer store2.Close()

	old, err := store2.Get("lease-old")
	require.NoError(t, err)
	assert.Nil(t, old)

	fresh, err := store2.Get("lease-fresh")
	require.NoError(t, err)
	assert.NotNil(t, fresh)
}

func TestDiagnosticsStore_RemoveOlderThan(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "diag_ttl.db")
	store, err := NewDiagnosticsStore(DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	// Store entries at different ages
	require.NoError(t, store.Store(DiagnosticEntry{
		LeaseUUID: "old-1",
		Error:     "old error 1",
		FailCount: 1,
		CreatedAt: time.Now().Add(-48 * time.Hour),
	}))
	require.NoError(t, store.Store(DiagnosticEntry{
		LeaseUUID: "old-2",
		Error:     "old error 2",
		FailCount: 2,
		CreatedAt: time.Now().Add(-25 * time.Hour),
	}))
	require.NoError(t, store.Store(DiagnosticEntry{
		LeaseUUID: "fresh",
		Error:     "fresh error",
		FailCount: 1,
		CreatedAt: time.Now(),
	}))

	removed, err := store.RemoveOlderThan(24 * time.Hour)
	require.NoError(t, err)
	assert.Equal(t, 2, removed)

	got, err := store.Get("fresh")
	require.NoError(t, err)
	assert.NotNil(t, got)

	got, err = store.Get("old-1")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestDiagnosticsStore_Delete(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "diag_delete.db")
	store, err := NewDiagnosticsStore(DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.Store(DiagnosticEntry{
		LeaseUUID: "lease-1",
		Error:     "some error",
		FailCount: 1,
	}))

	// Delete existing entry
	require.NoError(t, store.Delete("lease-1"))

	got, err := store.Get("lease-1")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Delete nonexistent is a no-op
	require.NoError(t, store.Delete("nonexistent"))
}

func TestDiagnosticsStore_StoreOverwrites(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "diag_overwrite.db")
	store, err := NewDiagnosticsStore(DiagnosticsStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store.Close()

	// Store initial entry
	require.NoError(t, store.Store(DiagnosticEntry{
		LeaseUUID: "lease-1",
		Error:     "first error",
		FailCount: 1,
		CreatedAt: time.Now(),
	}))

	// Overwrite with new entry (same LeaseUUID)
	require.NoError(t, store.Store(DiagnosticEntry{
		LeaseUUID: "lease-1",
		Error:     "updated error",
		FailCount: 2,
		Logs:      map[string]string{"0": "new logs"},
		CreatedAt: time.Now(),
	}))

	got, err := store.Get("lease-1")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "updated error", got.Error)
	assert.Equal(t, 2, got.FailCount)
	assert.Equal(t, "new logs", got.Logs["0"])
}
