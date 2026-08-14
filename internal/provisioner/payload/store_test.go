package payload

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/testutil"
)

// mustHas is a test helper that calls Has() and fails the test on error.
func mustHas(t *testing.T, store *Store, leaseUUID string) bool {
	t.Helper()
	has, err := store.Has(leaseUUID)
	require.NoError(t, err)
	return has
}

// dropHashBucket deletes the payload_hashes bucket from a CLOSED store's
// database, producing the on-disk shape a pre-ENG-619 build would have written.
// bbolt is single-writer per file, so the store must be closed first.
func dropHashBucket(dbPath string) error {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(payloadHashBucketName)
	})
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

	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			uuid := testutil.ValidUUID1
			payload := []byte("payload")

			for j := range numOperations {
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
	for i := range 20 {
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
	for i := range numWrites {
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
	for i := range 10 {
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
	for i := range 10 {
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
	for i := range numWriters {
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

// --- ENG-619: overwrite-capable Put and the recorded-hash bucket ---

func TestStore_Store_RecordsPayloadHash(t *testing.T) {
	store := newTestStore(t)
	data := []byte("test payload")

	require.True(t, store.Store(testutil.ValidUUID1, data))

	_, got, err := store.GetWithHash(testutil.ValidUUID1)
	require.NoError(t, err)
	want := sha256.Sum256(data)
	assert.Equal(t, want[:], got, "Store() must record the payload's own hash")
}

func TestStore_GetWithHash_AbsentReturnsNilWithoutError(t *testing.T) {
	store := newTestStore(t)

	// Absence must be distinguishable from a read failure: callers fall back to
	// the on-chain MetaHash on nil, but abort the provision on an error.
	_, got, err := store.GetWithHash("no-such-lease")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestStore_Put_OverwritesPayloadAndHash(t *testing.T) {
	store := newTestStore(t)
	original := []byte("original manifest")
	updated := []byte("updated manifest with a new image")

	require.True(t, store.Store(testutil.ValidUUID1, original))
	require.NoError(t, store.Put(testutil.ValidUUID1, updated))

	got, err := store.Get(testutil.ValidUUID1)
	require.NoError(t, err)
	assert.Equal(t, updated, got, "Put() must replace the stored payload")

	_, gotHash, err := store.GetWithHash(testutil.ValidUUID1)
	require.NoError(t, err)
	want := sha256.Sum256(updated)
	assert.Equal(t, want[:], gotHash, "Put() must replace the recorded hash too")
}

func TestStore_Put_OnAbsentKeyStores(t *testing.T) {
	store := newTestStore(t)
	data := []byte("first write via Put")

	require.NoError(t, store.Put(testutil.ValidUUID1, data))

	assert.True(t, mustHas(t, store, testutil.ValidUUID1))
	got, err := store.Get(testutil.ValidUUID1)
	require.NoError(t, err)
	assert.Equal(t, data, got)
}

func TestStore_Store_DoesNotOverwriteWhatPutWrote(t *testing.T) {
	store := newTestStore(t)
	updated := []byte("updated manifest")

	require.NoError(t, store.Put(testutil.ValidUUID1, updated))

	// Store keeps its conflict semantics: the create path must never clobber a
	// payload it did not write, including one an update just installed.
	assert.False(t, store.Store(testutil.ValidUUID1, []byte("create-path payload")))

	got, err := store.Get(testutil.ValidUUID1)
	require.NoError(t, err)
	assert.Equal(t, updated, got)
}

func TestStore_Delete_ClearsRecordedHash(t *testing.T) {
	store := newTestStore(t)
	require.True(t, store.Store(testutil.ValidUUID1, []byte("payload")))

	store.Delete(testutil.ValidUUID1)

	// A leaked hash would make a later payload reused under the same key verify
	// against a stale reference.
	_, got, err := store.GetWithHash(testutil.ValidUUID1)
	require.NoError(t, err)
	assert.Nil(t, got, "Delete() must clear the recorded hash")
}

func TestStore_Pop_ClearsRecordedHash(t *testing.T) {
	store := newTestStore(t)
	data := []byte("payload")
	require.True(t, store.Store(testutil.ValidUUID1, data))

	assert.Equal(t, data, store.Pop(testutil.ValidUUID1))

	_, got, err := store.GetWithHash(testutil.ValidUUID1)
	require.NoError(t, err)
	assert.Nil(t, got, "Pop() must clear the recorded hash")
}

func TestStore_RecordedHashSurvivesReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")
	leaseUUID := "persistent-lease"
	updated := []byte("updated manifest data")

	store1, err := NewStore(StoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	require.True(t, store1.Store(leaseUUID, []byte("original")))
	require.NoError(t, store1.Put(leaseUUID, updated))
	require.NoError(t, store1.Close())

	// The whole point of the fix is that a REBOOT replays the updated payload,
	// so the hash has to survive the process, not just the transaction.
	store2, err := NewStore(StoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store2.Close()

	got, err := store2.Get(leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, updated, got)

	_, gotHash, err := store2.GetWithHash(leaseUUID)
	require.NoError(t, err)
	want := sha256.Sum256(updated)
	assert.Equal(t, want[:], gotHash)
}

func TestStore_OpeningPreENG619DatabaseAddsHashBucket(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	store1, err := NewStore(StoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	leaseUUID := "legacy-lease"
	require.True(t, store1.Store(leaseUUID, []byte("legacy payload")))
	require.NoError(t, store1.Close())

	// Simulate a database written before the hash bucket existed: drop the
	// bucket entirely, then reopen.
	require.NoError(t, dropHashBucket(dbPath))

	store2, err := NewStore(StoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer store2.Close()

	// The bucket is recreated (Healthy passes) but the legacy payload has no
	// recorded hash, so readers fall back to the on-chain MetaHash.
	require.NoError(t, store2.Healthy())
	_, gotHash, err := store2.GetWithHash(leaseUUID)
	require.NoError(t, err)
	assert.Nil(t, gotHash, "a pre-ENG-619 payload must have no recorded hash")

	got, err := store2.Get(leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, []byte("legacy payload"), got, "the legacy payload itself must be untouched")
}

func TestStore_Put_AfterCloseReturnsError(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, store.Close())

	// Unlike Store's bool, Put's caller has already applied the update to a
	// backend, so a closed store must surface as an error rather than silence.
	err := store.Put(testutil.ValidUUID1, []byte("payload"))
	require.Error(t, err)
}

// writeRawHash writes an arbitrary value into the payload_hashes bucket of a
// CLOSED store's database, producing a malformed entry the store's own API
// cannot create. bbolt is single-writer per file, so the store must be closed.
func writeRawHash(dbPath, leaseUUID string, value []byte) error {
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(payloadHashBucketName).Put([]byte(leaseUUID), value)
	})
}

func TestStore_GetWithHash_MalformedEntryIsAnError(t *testing.T) {
	// A recorded hash that is present but not a SHA-256 must NOT read as
	// "absent". Absence sends the caller to the on-chain MetaHash, and for a
	// payload an update legitimately changed that means deleting a good
	// manifest and closing a live lease. Corruption of the checksum is a failed
	// read, so the caller retries instead.
	for _, tc := range []struct {
		name  string
		value []byte
	}{
		{"empty", []byte{}},
		{"too short", []byte{0x01, 0x02, 0x03}},
		{"too long", make([]byte, HashSize+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "test_payloads.db")
			store, err := NewStore(StoreConfig{DBPath: dbPath})
			require.NoError(t, err)
			require.True(t, store.Store(testutil.ValidUUID1, []byte("payload")))
			require.NoError(t, store.Close())

			require.NoError(t, writeRawHash(dbPath, testutil.ValidUUID1, tc.value))

			store2, err := NewStore(StoreConfig{DBPath: dbPath})
			require.NoError(t, err)
			defer store2.Close()

			_, got, err := store2.GetWithHash(testutil.ValidUUID1)
			require.Error(t, err, "a malformed recorded hash must be an error, not (nil, nil)")
			assert.Nil(t, got)
			assert.Contains(t, err.Error(), "want 32")
		})
	}
}

func TestStore_GetWithHash_ExactSizeIsAccepted(t *testing.T) {
	// The boundary the malformed check keys on: a real 32-byte hash still reads
	// back cleanly.
	store := newTestStore(t)
	data := []byte("payload")
	require.True(t, store.Store(testutil.ValidUUID1, data))

	_, got, err := store.GetWithHash(testutil.ValidUUID1)
	require.NoError(t, err)
	require.Len(t, got, HashSize)
	want := sha256.Sum256(data)
	assert.Equal(t, want[:], got)
}

func TestStore_GetWithHash_ConcurrentPutIsRaceFree(t *testing.T) {
	// Exercises GetWithHash against a concurrent writer under -race, which is
	// what it is for: catching a data race in the copy-out of the two buckets.
	//
	// It deliberately does NOT claim to prove the single-snapshot property. The
	// torn-read window is a few microseconds wide and the writer here manages
	// only ~20 Puts/sec (each blocks on the 50ms batch flush), so this test was
	// measured NOT to fail when GetWithHash is mutated back into two separate
	// transactions. The structural guarantee is enforced deterministically by
	// TestStore_GetWithHash_UsesExactlyOneReadTransaction instead — that one
	// does fail under the mutation. Consistency is still asserted below as a
	// cheap sanity check, not as the guard.
	store := newTestStore(t)
	const leaseUUID = "race-lease"
	require.True(t, store.Store(leaseUUID, []byte("payload-v0")))

	const readsPerReader = 500
	const readers = 4

	// Two WaitGroups on purpose: the writer runs until the readers are finished,
	// so waiting on one combined group before closing stop would deadlock.
	var readersWg, writerWg sync.WaitGroup
	stop := make(chan struct{})

	writerWg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			// Each write is a distinct payload, so a torn pair cannot coincide.
			_ = store.Put(leaseUUID, fmt.Appendf(nil, "payload-v%d", i))
		}
	})

	for range readers {
		readersWg.Go(func() {
			for range readsPerReader {
				payload, hash, err := store.GetWithHash(leaseUUID)
				if err != nil {
					t.Errorf("GetWithHash returned an error: %v", err)
					return
				}
				if payload == nil || len(hash) == 0 {
					continue // never written yet — not a tear
				}
				if err := VerifyHash(payload, hash); err != nil {
					t.Errorf("torn pair: payload and recorded hash came from different commits: %v", err)
					return
				}
			}
		})
	}

	readersWg.Wait()
	close(stop)
	writerWg.Wait()
}

func TestStore_GetWithHash_UsesExactlyOneReadTransaction(t *testing.T) {
	// The guard that actually has teeth. The torn-read defect is a ~microsecond
	// window, so a probabilistic concurrency test does not reliably catch it —
	// this asserts the structural property instead: the payload and its hash
	// must come from ONE bbolt snapshot. bbolt counts started read transactions
	// in DB.Stats().TxN, so a second View here shows up as a second txn.
	//
	// Reading them in two transactions lets an /update Put commit between them,
	// producing (old payload, new hash); the reconciler reads that as corruption
	// and deletes the freshly persisted update, closing the lease a sweep later.
	store := newTestStore(t)
	require.True(t, store.Store(testutil.ValidUUID1, []byte("payload")))

	before := store.db.Stats().TxN
	payload, hash, err := store.GetWithHash(testutil.ValidUUID1)
	after := store.db.Stats().TxN

	require.NoError(t, err)
	require.NotNil(t, payload)
	require.Len(t, hash, HashSize)
	assert.Equal(t, 1, after-before,
		"GetWithHash must read payload and hash from a single snapshot; %d transactions means the pair can tear", after-before)
}
