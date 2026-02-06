package payload

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/testutil"
)

// mustHas is a test helper that calls Has() and fails the test on error.
func mustHas(t *testing.T, store *Store, leaseUUID string) bool {
	t.Helper()
	has, err := store.Has(leaseUUID)
	require.NoError(t, err)
	return has
}

// newTestStore creates a Store for testing with a temp database.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")
	store, err := NewStore(StoreConfig{
		DBPath: dbPath,
	})
	require.NoError(t, err, "NewStore() error")
	t.Cleanup(func() { store.Close() })
	return store
}

func TestStore_Store_Success(t *testing.T) {
	store := newTestStore(t)
	payload := []byte("test payload")

	ok := store.Store(testutil.ValidUUID1, payload)
	assert.True(t, ok, "Store() = false, want true for new payload")

	assert.True(t, mustHas(t, store, testutil.ValidUUID1), "Has() = false after Store()")
}

func TestStore_Store_Conflict(t *testing.T) {
	store := newTestStore(t)
	payload1 := []byte("test payload 1")
	payload2 := []byte("test payload 2")

	store.Store(testutil.ValidUUID1, payload1)

	// Second store should fail
	ok := store.Store(testutil.ValidUUID1, payload2)
	assert.False(t, ok, "Store() = true, want false for duplicate")

	// Original payload should be unchanged
	got, err := store.Get(testutil.ValidUUID1)
	require.NoError(t, err)
	assert.Equal(t, string(payload1), string(got))
}

func TestStore_Get(t *testing.T) {
	store := newTestStore(t)
	payload := []byte("test payload")

	store.Store(testutil.ValidUUID1, payload)

	got, err := store.Get(testutil.ValidUUID1)
	require.NoError(t, err)
	assert.Equal(t, string(payload), string(got))

	// Get should not remove the payload
	assert.True(t, mustHas(t, store, testutil.ValidUUID1), "Has() = false after Get()")
}

func TestStore_Get_NotFound(t *testing.T) {
	store := newTestStore(t)

	got, err := store.Get(testutil.ValidUUID1)
	require.NoError(t, err)
	assert.Nil(t, got, "Get() should return nil for non-existent")
}

func TestStore_Pop(t *testing.T) {
	store := newTestStore(t)
	payload := []byte("test payload")

	store.Store(testutil.ValidUUID1, payload)

	got := store.Pop(testutil.ValidUUID1)
	assert.Equal(t, string(payload), string(got))

	// Pop should remove the payload
	assert.False(t, mustHas(t, store, testutil.ValidUUID1), "Has() = true after Pop()")
}

func TestStore_Pop_NotFound(t *testing.T) {
	store := newTestStore(t)

	got := store.Pop(testutil.ValidUUID1)
	assert.Nil(t, got, "Pop() should return nil for non-existent")
}

func TestStore_Has(t *testing.T) {
	store := newTestStore(t)

	assert.False(t, mustHas(t, store, testutil.ValidUUID1), "Has() = true for empty store")

	store.Store(testutil.ValidUUID1, []byte("test"))

	assert.True(t, mustHas(t, store, testutil.ValidUUID1), "Has() = false after Store()")
}

func TestStore_Delete(t *testing.T) {
	store := newTestStore(t)
	payload := []byte("test payload")

	store.Store(testutil.ValidUUID1, payload)
	store.Delete(testutil.ValidUUID1)

	assert.False(t, mustHas(t, store, testutil.ValidUUID1), "Has() = true after Delete()")
}

func TestStore_Delete_NotFound(t *testing.T) {
	store := newTestStore(t)

	// Should not panic for non-existent key
	store.Delete(testutil.ValidUUID1)
}

func TestStore_Count(t *testing.T) {
	store := newTestStore(t)

	assert.Equal(t, 0, store.Count(), "Count() should be 0 for empty store")

	store.Store(testutil.ValidUUID1, []byte("test1"))
	assert.Equal(t, 1, store.Count())

	store.Store(testutil.ValidUUID2, []byte("test2"))
	assert.Equal(t, 2, store.Count())

	store.Pop(testutil.ValidUUID1)
	assert.Equal(t, 1, store.Count(), "Count() should be 1 after Pop()")
}

func TestStore_PayloadIsCopied(t *testing.T) {
	store := newTestStore(t)
	original := []byte("test payload")

	store.Store(testutil.ValidUUID1, original)

	// Modify original - should not affect stored value
	original[0] = 'X'

	got, err := store.Get(testutil.ValidUUID1)
	require.NoError(t, err)
	assert.NotEqual(t, byte('X'), got[0], "Store() did not copy payload, original mutation affected stored value")

	// Modify returned value - should not affect stored value
	got[0] = 'Y'
	got2, err := store.Get(testutil.ValidUUID1)
	require.NoError(t, err)
	assert.NotEqual(t, byte('Y'), got2[0], "Get() did not copy payload, returned mutation affected stored value")
}

func TestStore_ConcurrentAccess(t *testing.T) {
	store := newTestStore(t)
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
					_, _ = store.Get(uuid)
				case 2:
					_, _ = store.Has(uuid)
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

func TestStore_Persistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	// Create store and add data
	store1, err := NewStore(StoreConfig{
		DBPath: dbPath,
	})
	require.NoError(t, err, "NewStore() error")

	leaseUUID := "persistent-lease"
	payload := []byte("persistent payload data")
	store1.Store(leaseUUID, payload)
	store1.Close()

	// Reopen store and verify data persisted
	store2, err := NewStore(StoreConfig{
		DBPath: dbPath,
	})
	require.NoError(t, err, "NewStore() reopen error")
	defer store2.Close()

	got, err := store2.Get(leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, string(payload), string(got), "After reopen, Get() returned wrong value")
}

func TestStore_RequiresDBPath(t *testing.T) {
	_, err := NewStore(StoreConfig{})
	require.Error(t, err, "NewStore() with empty DBPath should return error")
}

func TestNewStore_AppliesDefaults(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	store, err := NewStore(StoreConfig{
		DBPath: dbPath,
	})
	require.NoError(t, err, "NewStore() error")
	defer store.Close()

	// Verify batch defaults were applied
	assert.Equal(t, DefaultBatchSize, store.batchSize)
	assert.Equal(t, DefaultFlushInterval, store.flushInterval)
}

func TestStore_CanReuseAfterDelete(t *testing.T) {
	store := newTestStore(t)
	leaseUUID := "reusable-lease"

	// Store, delete, then store again
	store.Store(leaseUUID, []byte("first"))
	store.Delete(leaseUUID)

	// Should be able to store again
	assert.True(t, store.Store(leaseUUID, []byte("second")), "Store() after Delete returned false, want true")

	got, err := store.Get(leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, "second", string(got))
}

func TestStore_CanReuseAfterPop(t *testing.T) {
	store := newTestStore(t)
	leaseUUID := "reusable-lease"

	// Store, pop, then store again
	store.Store(leaseUUID, []byte("first"))
	store.Pop(leaseUUID)

	// Should be able to store again
	assert.True(t, store.Store(leaseUUID, []byte("second")), "Store() after Pop returned false, want true")

	got, err := store.Get(leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, "second", string(got))
}

func TestStore_FilePermissions(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")
	store, err := NewStore(StoreConfig{
		DBPath: dbPath,
	})
	require.NoError(t, err, "NewStore() error")
	store.Close()

	// Check file permissions (0600 = owner read/write only)
	info, err := os.Stat(dbPath)
	require.NoError(t, err, "os.Stat() error")

	// On Unix systems, check permissions
	perm := info.Mode().Perm()
	assert.Equal(t, os.FileMode(0), perm&0077, "DB file has unexpected permissions: %o (should not be readable/writable by group/other)", perm)
}

func TestStore_InvalidDBPath(t *testing.T) {
	// Try to create store in a path that doesn't exist and can't be created
	_, err := NewStore(StoreConfig{
		DBPath: "/nonexistent/path/that/cannot/be/created/test.db",
	})
	require.Error(t, err, "NewStore() with invalid path should return error")
}

func TestStore_BatchingDefaults(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	store, err := NewStore(StoreConfig{
		DBPath: dbPath,
	})
	require.NoError(t, err, "NewStore() error")
	defer store.Close()

	// Verify batching defaults were applied
	assert.Equal(t, DefaultBatchSize, store.batchSize)
	assert.Equal(t, DefaultFlushInterval, store.flushInterval)
}

func TestStore_BatchingCustomConfig(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	customBatchSize := 25
	customFlushInterval := 100 * time.Millisecond

	store, err := NewStore(StoreConfig{
		DBPath:        dbPath,
		BatchSize:     customBatchSize,
		FlushInterval: customFlushInterval,
	})
	require.NoError(t, err, "NewStore() error")
	defer store.Close()

	assert.Equal(t, customBatchSize, store.batchSize)
	assert.Equal(t, customFlushInterval, store.flushInterval)
}

func TestStore_FlushOnClose(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	// Create store with short flush interval
	store, err := NewStore(StoreConfig{
		DBPath: dbPath,

		FlushInterval: 10 * time.Millisecond,
		BatchSize:     50,
	})
	require.NoError(t, err, "NewStore() error")

	// Store multiple items
	for i := 0; i < 20; i++ {
		key := string(rune('a' + i))
		store.Store(key, []byte("data"))
	}

	// Close should wait for all operations to complete and flush
	store.Close()

	// Reopen and verify all data was persisted
	store2, err := NewStore(StoreConfig{
		DBPath: dbPath,
	})
	require.NoError(t, err, "NewStore() reopen error")
	defer store2.Close()

	assert.Equal(t, 20, store2.Count(), "After reopen, Count() should be 20")
}

func TestStore_BatchingConcurrentWrites(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	store, err := NewStore(StoreConfig{
		DBPath: dbPath,

		BatchSize:     10,
		FlushInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err, "NewStore() error")
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
	assert.Equal(t, numWrites, store.Count())
}

func TestStore_BatchingMixedOperations(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	store, err := NewStore(StoreConfig{
		DBPath: dbPath,

		BatchSize:     5,
		FlushInterval: 10 * time.Millisecond,
	})
	require.NoError(t, err, "NewStore() error")
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
	assert.Equal(t, 10, store.Count())
}

func TestStore_FlushIntervalTriggersWrite(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	store, err := NewStore(StoreConfig{
		DBPath: dbPath,

		BatchSize:     1000,                  // Large batch so it won't trigger by size
		FlushInterval: 25 * time.Millisecond, // Short interval
	})
	require.NoError(t, err, "NewStore() error")
	defer store.Close()

	// Store a single item (won't trigger batch size)
	store.Store("interval-test", []byte("data"))

	// Poll until flush interval writes the data
	deadline := time.After(5 * time.Second)
	for !mustHas(t, store, "interval-test") {
		select {
		case <-deadline:
			require.Fail(t, "timed out waiting for flush")
		default:
			runtime.Gosched()
		}
	}

	// Data should be written
	assert.True(t, mustHas(t, store, "interval-test"), "Has() = false after flush interval")
}

// Tests for VerifyHash and VerifyHashHex

func TestVerifyHash_ValidHash(t *testing.T) {
	payload := []byte("test payload data")
	// Compute SHA-256 of payload
	expectedHash := sha256.Sum256(payload)

	err := VerifyHash(payload, expectedHash[:])
	assert.NoError(t, err)
}

func TestVerifyHash_Mismatch(t *testing.T) {
	payload := []byte("test payload data")
	// Wrong hash (all zeros)
	wrongHash := make([]byte, 32)

	err := VerifyHash(payload, wrongHash)
	require.Error(t, err, "VerifyHash() error = nil, want HashMismatchError")

	var mismatchErr *HashMismatchError
	assert.True(t, errors.As(err, &mismatchErr), "VerifyHash() error type = %T, want *HashMismatchError", err)
}

func TestVerifyHash_EmptyExpectedHash(t *testing.T) {
	payload := []byte("test payload data")

	err := VerifyHash(payload, []byte{})
	require.Error(t, err, "VerifyHash() error = nil, want error for empty hash")
	assert.Equal(t, "expected hash is empty", err.Error())
}

func TestVerifyHash_NilExpectedHash(t *testing.T) {
	payload := []byte("test payload data")

	err := VerifyHash(payload, nil)
	require.Error(t, err, "VerifyHash() error = nil, want error for nil hash")
	assert.Equal(t, "expected hash is empty", err.Error())
}

func TestVerifyHash_EmptyPayload(t *testing.T) {
	payload := []byte{}
	// SHA-256 of empty string: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
	expectedHash := []byte{
		0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14,
		0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
		0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c,
		0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
	}

	err := VerifyHash(payload, expectedHash)
	assert.NoError(t, err, "VerifyHash() should succeed for empty payload")
}

func TestVerifyHash_NilPayload(t *testing.T) {
	// SHA-256 of empty/nil is same as empty string
	expectedHash := []byte{
		0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14,
		0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
		0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c,
		0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
	}

	err := VerifyHash(nil, expectedHash)
	assert.NoError(t, err, "VerifyHash() should succeed for nil payload")
}

func TestVerifyHash_WrongLengthHash(t *testing.T) {
	payload := []byte("test payload data")
	// Hash with wrong length (not 32 bytes)
	wrongLengthHash := []byte{0x01, 0x02, 0x03}

	err := VerifyHash(payload, wrongLengthHash)
	require.Error(t, err, "VerifyHash() error = nil, want HashMismatchError for wrong length hash")

	var mismatchErr *HashMismatchError
	assert.True(t, errors.As(err, &mismatchErr), "VerifyHash() error type = %T, want *HashMismatchError", err)
}

func TestVerifyHashHex_ValidHash(t *testing.T) {
	payload := []byte("hello world")
	// SHA-256("hello world") = b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
	expectedHashHex := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

	err := VerifyHashHex(payload, expectedHashHex)
	assert.NoError(t, err)
}

func TestVerifyHashHex_Mismatch(t *testing.T) {
	payload := []byte("hello world")
	// Wrong hash (all zeros in hex)
	wrongHashHex := "0000000000000000000000000000000000000000000000000000000000000000"

	err := VerifyHashHex(payload, wrongHashHex)
	require.Error(t, err, "VerifyHashHex() error = nil, want HashMismatchError")

	var mismatchErr *HashMismatchError
	assert.True(t, errors.As(err, &mismatchErr), "VerifyHashHex() error type = %T, want *HashMismatchError", err)
}

func TestVerifyHashHex_InvalidHex(t *testing.T) {
	payload := []byte("hello world")

	testCases := []struct {
		name    string
		hexStr  string
		wantErr string
	}{
		{
			name:    "invalid characters",
			hexStr:  "xyz123notvalidhex!@#$%^&*()",
			wantErr: "invalid expected hash hex",
		},
		{
			name:    "odd length hex",
			hexStr:  "abc",
			wantErr: "invalid expected hash hex",
		},
		{
			name:    "spaces in hex",
			hexStr:  "b94d 27b9 934d",
			wantErr: "invalid expected hash hex",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := VerifyHashHex(payload, tc.hexStr)
			require.Error(t, err, "VerifyHashHex() error = nil, want error")
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestVerifyHashHex_EmptyHex(t *testing.T) {
	payload := []byte("hello world")

	err := VerifyHashHex(payload, "")
	require.Error(t, err, "VerifyHashHex() error = nil, want error for empty hex")
	// Empty hex decodes to empty []byte, which triggers "expected hash is empty"
	assert.Equal(t, "expected hash is empty", err.Error())
}

func TestVerifyHashHex_EmptyPayload(t *testing.T) {
	payload := []byte{}
	// SHA-256 of empty string in hex
	expectedHashHex := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	err := VerifyHashHex(payload, expectedHashHex)
	assert.NoError(t, err, "VerifyHashHex() should succeed for empty payload")
}

func TestVerifyHashHex_CaseInsensitive(t *testing.T) {
	payload := []byte("hello world")
	// SHA-256("hello world") in uppercase
	expectedHashHexUpper := "B94D27B9934D3E08A52E52D7DA7DABFAC484EFE37A5380EE9088F7ACE2EFCDE9"

	err := VerifyHashHex(payload, expectedHashHexUpper)
	assert.NoError(t, err, "hex should be case-insensitive")
}

func TestHashMismatchError_Error(t *testing.T) {
	err := &HashMismatchError{
		Expected: []byte{0x01, 0x02, 0x03},
		Actual:   []byte{0x04, 0x05, 0x06},
	}

	msg := err.Error()
	assert.Contains(t, msg, "010203", "HashMismatchError.Error() should contain expected hash hex")
	assert.Contains(t, msg, "040506", "HashMismatchError.Error() should contain actual hash hex")
	assert.Contains(t, msg, "payload hash mismatch", "HashMismatchError.Error() should contain 'payload hash mismatch'")
}

func TestVerifyHash_LargePayload(t *testing.T) {
	// Test with a larger payload to ensure no issues with size
	payload := make([]byte, 1024*1024) // 1MB
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	// Compute expected hash
	hash := sha256.Sum256(payload)

	err := VerifyHash(payload, hash[:])
	assert.NoError(t, err, "VerifyHash() should succeed for large payload")
}

func TestVerifyHash_BinaryPayload(t *testing.T) {
	// Test with binary payload containing all byte values
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}

	// Compute expected hash
	hash := sha256.Sum256(payload)

	err := VerifyHash(payload, hash[:])
	assert.NoError(t, err, "VerifyHash() should succeed for binary payload")
}

func TestStore_Healthy(t *testing.T) {
	store := newTestStore(t)

	err := store.Healthy()
	require.NoError(t, err)
}

func TestStore_CloseIdempotent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_idempotent.db")
	store, err := NewStore(StoreConfig{
		DBPath: dbPath,
	})
	require.NoError(t, err)

	// Close twice — should not panic
	require.NoError(t, store.Close())
	require.NoError(t, store.Close())
}

// TestStore_CloseDrainsPendingWrites tests that Close() properly drains
// pending write operations so callers don't block forever.
func TestStore_CloseDrainsPendingWrites(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_drain.db")
	store, err := NewStore(StoreConfig{
		DBPath: dbPath,

		FlushInterval: 1 * time.Second, // Slow flush to ensure operations queue up
		BatchSize:     100,             // Large batch size
	})
	require.NoError(t, err, "NewStore() error")

	// Start many concurrent writers
	const numWriters = 50
	var wg sync.WaitGroup
	wg.Add(numWriters)

	// Start writers that will queue up operations
	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer wg.Done()
			leaseUUID := strings.Repeat("a", 8) + "-" + string(rune('0'+id%10))
			store.Store(leaseUUID, []byte("payload"))
		}(i)
	}

	// Give writers a moment to start queueing
	time.Sleep(10 * time.Millisecond)

	// Close the store while writes are pending
	// This should drain the queue and allow all writers to complete
	closeDone := make(chan struct{})
	go func() {
		store.Close()
		close(closeDone)
	}()

	// Wait for all writers with a timeout
	writersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(writersDone)
	}()

	select {
	case <-writersDone:
		// Good - all writers completed
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for writers to complete - Close() did not drain pending operations")
	}

	select {
	case <-closeDone:
		// Good - Close completed
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for Close() to complete")
	}
}
