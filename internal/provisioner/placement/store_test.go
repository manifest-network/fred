package placement

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func TestStore_GetSetDelete(t *testing.T) {
	s := newTestStore(t)

	// Get on empty store returns ""
	assert.Equal(t, "", s.Get("lease-1"))

	// Set and Get
	require.NoError(t, s.Set("lease-1", "backend-a"))
	assert.Equal(t, "backend-a", s.Get("lease-1"))

	// Overwrite
	require.NoError(t, s.Set("lease-1", "backend-b"))
	assert.Equal(t, "backend-b", s.Get("lease-1"))

	// Delete
	s.Delete("lease-1")
	assert.Equal(t, "", s.Get("lease-1"))

	// Delete non-existent key is a no-op
	s.Delete("nonexistent")
}

func TestStore_SetBatch(t *testing.T) {
	s := newTestStore(t)

	placements := map[string]string{
		"lease-1": "backend-a",
		"lease-2": "backend-b",
		"lease-3": "backend-a",
	}

	require.NoError(t, s.SetBatch(placements))

	assert.Equal(t, "backend-a", s.Get("lease-1"))
	assert.Equal(t, "backend-b", s.Get("lease-2"))
	assert.Equal(t, "backend-a", s.Get("lease-3"))

	// Empty batch is a no-op
	require.NoError(t, s.SetBatch(nil))
	require.NoError(t, s.SetBatch(map[string]string{}))
}

func TestStore_PersistenceAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")

	// Write some data
	s1, err := NewStore(dbPath)
	require.NoError(t, err)
	require.NoError(t, s1.Set("lease-1", "backend-a"))
	require.NoError(t, s1.Set("lease-2", "backend-b"))
	require.NoError(t, s1.Close())

	// Reopen and verify data persisted
	s2, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	assert.Equal(t, "backend-a", s2.Get("lease-1"))
	assert.Equal(t, "backend-b", s2.Get("lease-2"))
	assert.Equal(t, "", s2.Get("nonexistent"))
}

func TestStore_ConcurrentAccess(t *testing.T) {
	s := newTestStore(t)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			key := "lease-concurrent"
			_ = s.Set(key, "backend")
			_ = s.Get(key)
			s.Delete(key)
			_ = s.Set(key, "backend-2")
			_ = s.Get(key)
		}(i)
	}

	wg.Wait()
}

func TestStore_CloseIdempotent(t *testing.T) {
	s := newTestStore(t)

	require.NoError(t, s.Close())
	require.NoError(t, s.Close()) // second close should not error
}

func TestStore_Healthy(t *testing.T) {
	s := newTestStore(t)
	assert.NoError(t, s.Healthy())
}

func TestStore_Healthy_AfterClose(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, s.Close())
	assert.Error(t, s.Healthy())
}

func TestNewStore_EmptyPath(t *testing.T) {
	_, err := NewStore("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "placement db path is required")
}

func TestStore_Count(t *testing.T) {
	s := newTestStore(t)

	// Empty store
	assert.Equal(t, 0, s.Count())

	// After one Set
	require.NoError(t, s.Set("lease-1", "backend-a"))
	assert.Equal(t, 1, s.Count())

	// After second Set (different key)
	require.NoError(t, s.Set("lease-2", "backend-b"))
	assert.Equal(t, 2, s.Count())

	// Overwrite existing key does not change count
	require.NoError(t, s.Set("lease-1", "backend-c"))
	assert.Equal(t, 2, s.Count())

	// After Delete
	s.Delete("lease-1")
	assert.Equal(t, 1, s.Count())

	// After SetBatch
	require.NoError(t, s.SetBatch(map[string]string{
		"lease-3": "backend-a",
		"lease-4": "backend-b",
		"lease-5": "backend-c",
	}))
	assert.Equal(t, 4, s.Count())
}

func TestStore_List(t *testing.T) {
	s := newTestStore(t)

	// Empty store
	assert.Empty(t, s.List())

	// After Sets
	require.NoError(t, s.Set("lease-1", "backend-a"))
	require.NoError(t, s.Set("lease-2", "backend-b"))
	require.NoError(t, s.Set("lease-3", "backend-c"))

	got := s.List()
	sort.Strings(got)
	assert.Equal(t, []string{"lease-1", "lease-2", "lease-3"}, got)

	// After Delete
	s.Delete("lease-2")
	got = s.List()
	sort.Strings(got)
	assert.Equal(t, []string{"lease-1", "lease-3"}, got)
}

func TestStore_List_PersistenceAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")

	s1, err := NewStore(dbPath)
	require.NoError(t, err)
	require.NoError(t, s1.Set("lease-1", "backend-a"))
	require.NoError(t, s1.Set("lease-2", "backend-b"))
	require.NoError(t, s1.Close())

	s2, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	got := s2.List()
	sort.Strings(got)
	assert.Equal(t, []string{"lease-1", "lease-2"}, got)
	assert.Equal(t, 2, s2.Count())
}

func TestStore_SetBatch_Overwrite(t *testing.T) {
	s := newTestStore(t)

	require.NoError(t, s.Set("lease-1", "backend-a"))
	require.NoError(t, s.Set("lease-2", "backend-b"))

	// SetBatch overwrites lease-1 and adds lease-3
	require.NoError(t, s.SetBatch(map[string]string{
		"lease-1": "backend-x",
		"lease-3": "backend-y",
	}))

	assert.Equal(t, "backend-x", s.Get("lease-1"))
	assert.Equal(t, "backend-b", s.Get("lease-2"))
	assert.Equal(t, "backend-y", s.Get("lease-3"))
	assert.Equal(t, 3, s.Count())
}

func TestStore_SetBatch_PersistenceAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")

	s1, err := NewStore(dbPath)
	require.NoError(t, err)
	require.NoError(t, s1.SetBatch(map[string]string{
		"lease-1": "backend-a",
		"lease-2": "backend-b",
	}))
	require.NoError(t, s1.Close())

	s2, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	assert.Equal(t, "backend-a", s2.Get("lease-1"))
	assert.Equal(t, "backend-b", s2.Get("lease-2"))
	assert.Equal(t, 2, s2.Count())
}

func TestStore_DeletePersistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")

	s1, err := NewStore(dbPath)
	require.NoError(t, err)
	require.NoError(t, s1.Set("lease-1", "backend-a"))
	require.NoError(t, s1.Set("lease-2", "backend-b"))
	s1.Delete("lease-1")
	require.NoError(t, s1.Close())

	s2, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	assert.Equal(t, "", s2.Get("lease-1"))
	assert.Equal(t, "backend-b", s2.Get("lease-2"))
	assert.Equal(t, 1, s2.Count())
}

func TestStore_ConcurrentMixedOps(t *testing.T) {
	s := newTestStore(t)

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("lease-%d", id)
			_ = s.Set(key, "backend")
			_ = s.Get(key)
			_ = s.Count()
			_ = s.List()
			s.Delete(key)
			_ = s.Count()
		}(i)
	}

	wg.Wait()

	// After all goroutines complete, every key was deleted
	assert.Equal(t, 0, s.Count())
	assert.Empty(t, s.List())
}
