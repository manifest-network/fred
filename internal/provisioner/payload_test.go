package provisioner

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/manifest-network/fred/internal/testutil"
)

// newTestPayloadStore creates a PayloadStore for testing with a temp database.
func newTestPayloadStore(t *testing.T) *PayloadStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")
	store, err := NewPayloadStore(PayloadStoreConfig{
		DBPath: dbPath,
		TTL:    1 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestPayloadStore_Store_Success(t *testing.T) {
	store := newTestPayloadStore(t)
	payload := []byte("test payload")

	ok := store.Store(testutil.ValidUUID1, payload)
	if !ok {
		t.Error("Store() = false, want true for new payload")
	}

	if !store.Has(testutil.ValidUUID1) {
		t.Error("Has() = false after Store()")
	}
}

func TestPayloadStore_Store_Conflict(t *testing.T) {
	store := newTestPayloadStore(t)
	payload1 := []byte("test payload 1")
	payload2 := []byte("test payload 2")

	store.Store(testutil.ValidUUID1, payload1)

	// Second store should fail
	ok := store.Store(testutil.ValidUUID1, payload2)
	if ok {
		t.Error("Store() = true, want false for duplicate")
	}

	// Original payload should be unchanged
	got := store.Get(testutil.ValidUUID1)
	if string(got) != string(payload1) {
		t.Errorf("Get() = %q, want %q", got, payload1)
	}
}

func TestPayloadStore_Get(t *testing.T) {
	store := newTestPayloadStore(t)
	payload := []byte("test payload")

	store.Store(testutil.ValidUUID1, payload)

	got := store.Get(testutil.ValidUUID1)
	if string(got) != string(payload) {
		t.Errorf("Get() = %q, want %q", got, payload)
	}

	// Get should not remove the payload
	if !store.Has(testutil.ValidUUID1) {
		t.Error("Has() = false after Get()")
	}
}

func TestPayloadStore_Get_NotFound(t *testing.T) {
	store := newTestPayloadStore(t)

	got := store.Get(testutil.ValidUUID1)
	if got != nil {
		t.Errorf("Get() = %v, want nil for non-existent", got)
	}
}

func TestPayloadStore_Pop(t *testing.T) {
	store := newTestPayloadStore(t)
	payload := []byte("test payload")

	store.Store(testutil.ValidUUID1, payload)

	got := store.Pop(testutil.ValidUUID1)
	if string(got) != string(payload) {
		t.Errorf("Pop() = %q, want %q", got, payload)
	}

	// Pop should remove the payload
	if store.Has(testutil.ValidUUID1) {
		t.Error("Has() = true after Pop()")
	}
}

func TestPayloadStore_Pop_NotFound(t *testing.T) {
	store := newTestPayloadStore(t)

	got := store.Pop(testutil.ValidUUID1)
	if got != nil {
		t.Errorf("Pop() = %v, want nil for non-existent", got)
	}
}

func TestPayloadStore_Has(t *testing.T) {
	store := newTestPayloadStore(t)

	if store.Has(testutil.ValidUUID1) {
		t.Error("Has() = true for empty store")
	}

	store.Store(testutil.ValidUUID1, []byte("test"))

	if !store.Has(testutil.ValidUUID1) {
		t.Error("Has() = false after Store()")
	}
}

func TestPayloadStore_Delete(t *testing.T) {
	store := newTestPayloadStore(t)
	payload := []byte("test payload")

	store.Store(testutil.ValidUUID1, payload)
	store.Delete(testutil.ValidUUID1)

	if store.Has(testutil.ValidUUID1) {
		t.Error("Has() = true after Delete()")
	}
}

func TestPayloadStore_Delete_NotFound(t *testing.T) {
	store := newTestPayloadStore(t)

	// Should not panic for non-existent key
	store.Delete(testutil.ValidUUID1)
}

func TestPayloadStore_Count(t *testing.T) {
	store := newTestPayloadStore(t)

	if count := store.Count(); count != 0 {
		t.Errorf("Count() = %d, want 0 for empty store", count)
	}

	store.Store(testutil.ValidUUID1, []byte("test1"))
	if count := store.Count(); count != 1 {
		t.Errorf("Count() = %d, want 1", count)
	}

	store.Store(testutil.ValidUUID2, []byte("test2"))
	if count := store.Count(); count != 2 {
		t.Errorf("Count() = %d, want 2", count)
	}

	store.Pop(testutil.ValidUUID1)
	if count := store.Count(); count != 1 {
		t.Errorf("Count() = %d, want 1 after Pop()", count)
	}
}

func TestPayloadStore_PayloadIsCopied(t *testing.T) {
	store := newTestPayloadStore(t)
	original := []byte("test payload")

	store.Store(testutil.ValidUUID1, original)

	// Modify original - should not affect stored value
	original[0] = 'X'

	got := store.Get(testutil.ValidUUID1)
	if got[0] == 'X' {
		t.Error("Store() did not copy payload, original mutation affected stored value")
	}

	// Modify returned value - should not affect stored value
	got[0] = 'Y'
	got2 := store.Get(testutil.ValidUUID1)
	if got2[0] == 'Y' {
		t.Error("Get() did not copy payload, returned mutation affected stored value")
	}
}

func TestPayloadStore_ConcurrentAccess(t *testing.T) {
	store := newTestPayloadStore(t)
	const numGoroutines = 100
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			uuid := testutil.ValidUUID1
			payload := []byte("payload")

			for j := 0; j < numOperations; j++ {
				switch j % 5 {
				case 0:
					store.Store(uuid, payload)
				case 1:
					store.Get(uuid)
				case 2:
					store.Has(uuid)
				case 3:
					store.Pop(uuid)
				case 4:
					store.Delete(uuid)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestPayloadStore_Persistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	// Create store and add data
	store1, err := NewPayloadStore(PayloadStoreConfig{
		DBPath: dbPath,
		TTL:    1 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}

	leaseUUID := "persistent-lease"
	payload := []byte("persistent payload data")
	store1.Store(leaseUUID, payload)
	store1.Close()

	// Reopen store and verify data persisted
	store2, err := NewPayloadStore(PayloadStoreConfig{
		DBPath: dbPath,
		TTL:    1 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() reopen error = %v", err)
	}
	defer store2.Close()

	got := store2.Get(leaseUUID)
	if string(got) != string(payload) {
		t.Errorf("After reopen, Get() = %q, want %q", got, payload)
	}
}

func TestPayloadStore_RequiresDBPath(t *testing.T) {
	_, err := NewPayloadStore(PayloadStoreConfig{})
	if err == nil {
		t.Error("NewPayloadStore() with empty DBPath should return error")
	}
}

func TestPayloadStore_CleanupExpiredPayloads(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")
	store, err := NewPayloadStore(PayloadStoreConfig{
		DBPath:          dbPath,
		TTL:             50 * time.Millisecond, // Very short TTL for testing
		CleanupInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}
	defer store.Close()

	// Store some data
	store.Store("lease-1", []byte("data1"))

	if store.Count() != 1 {
		t.Errorf("Count() = %d, want 1", store.Count())
	}

	// Wait for TTL + cleanup interval
	time.Sleep(150 * time.Millisecond)

	// Data should be cleaned up
	if store.Count() != 0 {
		t.Errorf("After TTL, Count() = %d, want 0", store.Count())
	}

	if store.Has("lease-1") {
		t.Error("After TTL, Has() returned true, want false")
	}
}

func TestNewPayloadStore_AppliesDefaults(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	// Create with zero values for TTL and CleanupInterval
	store, err := NewPayloadStore(PayloadStoreConfig{
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}
	defer store.Close()

	// Verify defaults were applied
	if store.ttl != DefaultPayloadTTL {
		t.Errorf("ttl = %v, want %v", store.ttl, DefaultPayloadTTL)
	}
	if store.cleanupInterval != DefaultPayloadCleanupInterval {
		t.Errorf("cleanupInterval = %v, want %v", store.cleanupInterval, DefaultPayloadCleanupInterval)
	}
}

func TestPayloadStore_CanReuseAfterDelete(t *testing.T) {
	store := newTestPayloadStore(t)
	leaseUUID := "reusable-lease"

	// Store, delete, then store again
	store.Store(leaseUUID, []byte("first"))
	store.Delete(leaseUUID)

	// Should be able to store again
	if !store.Store(leaseUUID, []byte("second")) {
		t.Error("Store() after Delete returned false, want true")
	}

	got := store.Get(leaseUUID)
	if string(got) != "second" {
		t.Errorf("Get() = %q, want %q", got, "second")
	}
}

func TestPayloadStore_CanReuseAfterPop(t *testing.T) {
	store := newTestPayloadStore(t)
	leaseUUID := "reusable-lease"

	// Store, pop, then store again
	store.Store(leaseUUID, []byte("first"))
	store.Pop(leaseUUID)

	// Should be able to store again
	if !store.Store(leaseUUID, []byte("second")) {
		t.Error("Store() after Pop returned false, want true")
	}

	got := store.Get(leaseUUID)
	if string(got) != "second" {
		t.Errorf("Get() = %q, want %q", got, "second")
	}
}

func TestPayloadStore_FilePermissions(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")
	store, err := NewPayloadStore(PayloadStoreConfig{
		DBPath: dbPath,
		TTL:    1 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}
	store.Close()

	// Check file permissions (0600 = owner read/write only)
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("os.Stat() error = %v", err)
	}

	// On Unix systems, check permissions
	perm := info.Mode().Perm()
	if perm&0077 != 0 {
		t.Errorf("DB file has unexpected permissions: %o (should not be readable/writable by group/other)", perm)
	}
}

func TestPayloadStore_InvalidDBPath(t *testing.T) {
	// Try to create store in a path that doesn't exist and can't be created
	_, err := NewPayloadStore(PayloadStoreConfig{
		DBPath: "/nonexistent/path/that/cannot/be/created/test.db",
		TTL:    1 * time.Hour,
	})
	if err == nil {
		t.Error("NewPayloadStore() with invalid path should return error")
	}
}

func TestPayloadStore_BatchingDefaults(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	store, err := NewPayloadStore(PayloadStoreConfig{
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}
	defer store.Close()

	// Verify batching defaults were applied
	if store.batchSize != DefaultBatchSize {
		t.Errorf("batchSize = %d, want %d", store.batchSize, DefaultBatchSize)
	}
	if store.flushInterval != DefaultFlushInterval {
		t.Errorf("flushInterval = %v, want %v", store.flushInterval, DefaultFlushInterval)
	}
}

func TestPayloadStore_BatchingCustomConfig(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	customBatchSize := 25
	customFlushInterval := 100 * time.Millisecond

	store, err := NewPayloadStore(PayloadStoreConfig{
		DBPath:        dbPath,
		BatchSize:     customBatchSize,
		FlushInterval: customFlushInterval,
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}
	defer store.Close()

	if store.batchSize != customBatchSize {
		t.Errorf("batchSize = %d, want %d", store.batchSize, customBatchSize)
	}
	if store.flushInterval != customFlushInterval {
		t.Errorf("flushInterval = %v, want %v", store.flushInterval, customFlushInterval)
	}
}

func TestPayloadStore_FlushOnClose(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	// Create store with short flush interval
	store, err := NewPayloadStore(PayloadStoreConfig{
		DBPath:        dbPath,
		TTL:           1 * time.Hour,
		FlushInterval: 10 * time.Millisecond,
		BatchSize:     50,
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}

	// Store multiple items
	for i := 0; i < 20; i++ {
		key := string(rune('a' + i))
		store.Store(key, []byte("data"))
	}

	// Close should wait for all operations to complete and flush
	store.Close()

	// Reopen and verify all data was persisted
	store2, err := NewPayloadStore(PayloadStoreConfig{
		DBPath: dbPath,
		TTL:    1 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() reopen error = %v", err)
	}
	defer store2.Close()

	count := store2.Count()
	if count != 20 {
		t.Errorf("After reopen, Count() = %d, want 20", count)
	}
}

func TestPayloadStore_BatchingConcurrentWrites(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	store, err := NewPayloadStore(PayloadStoreConfig{
		DBPath:        dbPath,
		TTL:           1 * time.Hour,
		BatchSize:     10,
		FlushInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}
	defer store.Close()

	const numWrites = 100
	var wg sync.WaitGroup
	wg.Add(numWrites)

	// Concurrent writes with unique keys
	for i := 0; i < numWrites; i++ {
		go func(id int) {
			defer wg.Done()
			// Generate unique key using format: "batch-NNN"
			key := "batch-" + string(rune('0'+id/100)) + string(rune('0'+(id/10)%10)) + string(rune('0'+id%10))
			store.Store(key, []byte("data"))
		}(i)
	}

	wg.Wait()

	// Verify all writes completed
	count := store.Count()
	if count != numWrites {
		t.Errorf("Count() = %d, want %d", count, numWrites)
	}
}

func TestPayloadStore_BatchingMixedOperations(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	store, err := NewPayloadStore(PayloadStoreConfig{
		DBPath:        dbPath,
		TTL:           1 * time.Hour,
		BatchSize:     5,
		FlushInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}
	defer store.Close()

	// Store multiple items
	for i := 0; i < 10; i++ {
		key := string(rune('a' + i))
		store.Store(key, []byte("initial"))
	}

	// Mix of operations
	var wg sync.WaitGroup
	wg.Add(30)

	// 10 stores
	for i := 10; i < 20; i++ {
		go func(id int) {
			defer wg.Done()
			key := string(rune('a' + id))
			store.Store(key, []byte("new"))
		}(i)
	}

	// 10 pops
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer wg.Done()
			key := string(rune('a' + id))
			store.Pop(key)
		}(i)
	}

	// 10 deletes (of non-existent keys - should be safe)
	for i := 20; i < 30; i++ {
		go func(id int) {
			defer wg.Done()
			key := string(rune('a' + id))
			store.Delete(key)
		}(i)
	}

	wg.Wait()

	// Verify: 10 new stores - 10 pops = 10 remaining
	count := store.Count()
	if count != 10 {
		t.Errorf("Count() = %d, want 10", count)
	}
}

func TestPayloadStore_FlushIntervalTriggersWrite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	store, err := NewPayloadStore(PayloadStoreConfig{
		DBPath:        dbPath,
		TTL:           1 * time.Hour,
		BatchSize:     1000,                   // Large batch so it won't trigger by size
		FlushInterval: 25 * time.Millisecond, // Short interval
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}
	defer store.Close()

	// Store a single item (won't trigger batch size)
	store.Store("interval-test", []byte("data"))

	// Wait for flush interval
	time.Sleep(100 * time.Millisecond)

	// Data should be written
	if !store.Has("interval-test") {
		t.Error("Has() = false after flush interval")
	}
}
