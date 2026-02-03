package provisioner

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

	"github.com/manifest-network/fred/internal/testutil"
)

// newTestPayloadStore creates a PayloadStore for testing with a temp database.
func newTestPayloadStore(t *testing.T) *PayloadStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")
	store, err := NewPayloadStore(PayloadStoreConfig{
		DBPath: dbPath,
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

func TestNewPayloadStore_AppliesDefaults(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_payloads.db")

	store, err := NewPayloadStore(PayloadStoreConfig{
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}
	defer store.Close()

	// Verify batch defaults were applied
	if store.batchSize != DefaultBatchSize {
		t.Errorf("batchSize = %v, want %v", store.batchSize, DefaultBatchSize)
	}
	if store.flushInterval != DefaultFlushInterval {
		t.Errorf("flushInterval = %v, want %v", store.flushInterval, DefaultFlushInterval)
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

		BatchSize:     1000,                  // Large batch so it won't trigger by size
		FlushInterval: 25 * time.Millisecond, // Short interval
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}
	defer store.Close()

	// Store a single item (won't trigger batch size)
	store.Store("interval-test", []byte("data"))

	// Poll until flush interval writes the data
	deadline := time.After(5 * time.Second)
	for !store.Has("interval-test") {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for flush")
		default:
			runtime.Gosched()
		}
	}

	// Data should be written
	if !store.Has("interval-test") {
		t.Error("Has() = false after flush interval")
	}
}

// Tests for VerifyPayloadHash and VerifyPayloadHashHex

func TestVerifyPayloadHash_ValidHash(t *testing.T) {
	payload := []byte("test payload data")
	// Compute SHA-256 of payload
	expectedHash := sha256.Sum256(payload)

	err := VerifyPayloadHash(payload, expectedHash[:])
	if err != nil {
		t.Errorf("VerifyPayloadHash() error = %v, want nil", err)
	}
}

func TestVerifyPayloadHash_Mismatch(t *testing.T) {
	payload := []byte("test payload data")
	// Wrong hash (all zeros)
	wrongHash := make([]byte, 32)

	err := VerifyPayloadHash(payload, wrongHash)
	if err == nil {
		t.Fatal("VerifyPayloadHash() error = nil, want HashMismatchError")
	}

	var mismatchErr *HashMismatchError
	if !errors.As(err, &mismatchErr) {
		t.Errorf("VerifyPayloadHash() error type = %T, want *HashMismatchError", err)
	}
}

func TestVerifyPayloadHash_EmptyExpectedHash(t *testing.T) {
	payload := []byte("test payload data")

	err := VerifyPayloadHash(payload, []byte{})
	if err == nil {
		t.Fatal("VerifyPayloadHash() error = nil, want error for empty hash")
	}
	if err.Error() != "expected hash is empty" {
		t.Errorf("VerifyPayloadHash() error = %q, want %q", err.Error(), "expected hash is empty")
	}
}

func TestVerifyPayloadHash_NilExpectedHash(t *testing.T) {
	payload := []byte("test payload data")

	err := VerifyPayloadHash(payload, nil)
	if err == nil {
		t.Fatal("VerifyPayloadHash() error = nil, want error for nil hash")
	}
	if err.Error() != "expected hash is empty" {
		t.Errorf("VerifyPayloadHash() error = %q, want %q", err.Error(), "expected hash is empty")
	}
}

func TestVerifyPayloadHash_EmptyPayload(t *testing.T) {
	payload := []byte{}
	// SHA-256 of empty string: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
	expectedHash := []byte{
		0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14,
		0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
		0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c,
		0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
	}

	err := VerifyPayloadHash(payload, expectedHash)
	if err != nil {
		t.Errorf("VerifyPayloadHash() error = %v, want nil for empty payload", err)
	}
}

func TestVerifyPayloadHash_NilPayload(t *testing.T) {
	// SHA-256 of empty/nil is same as empty string
	expectedHash := []byte{
		0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14,
		0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
		0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c,
		0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
	}

	err := VerifyPayloadHash(nil, expectedHash)
	if err != nil {
		t.Errorf("VerifyPayloadHash() error = %v, want nil for nil payload", err)
	}
}

func TestVerifyPayloadHash_WrongLengthHash(t *testing.T) {
	payload := []byte("test payload data")
	// Hash with wrong length (not 32 bytes)
	wrongLengthHash := []byte{0x01, 0x02, 0x03}

	err := VerifyPayloadHash(payload, wrongLengthHash)
	if err == nil {
		t.Fatal("VerifyPayloadHash() error = nil, want HashMismatchError for wrong length hash")
	}

	var mismatchErr *HashMismatchError
	if !errors.As(err, &mismatchErr) {
		t.Errorf("VerifyPayloadHash() error type = %T, want *HashMismatchError", err)
	}
}

func TestVerifyPayloadHashHex_ValidHash(t *testing.T) {
	payload := []byte("hello world")
	// SHA-256("hello world") = b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
	expectedHashHex := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

	err := VerifyPayloadHashHex(payload, expectedHashHex)
	if err != nil {
		t.Errorf("VerifyPayloadHashHex() error = %v, want nil", err)
	}
}

func TestVerifyPayloadHashHex_Mismatch(t *testing.T) {
	payload := []byte("hello world")
	// Wrong hash (all zeros in hex)
	wrongHashHex := "0000000000000000000000000000000000000000000000000000000000000000"

	err := VerifyPayloadHashHex(payload, wrongHashHex)
	if err == nil {
		t.Fatal("VerifyPayloadHashHex() error = nil, want HashMismatchError")
	}

	var mismatchErr *HashMismatchError
	if !errors.As(err, &mismatchErr) {
		t.Errorf("VerifyPayloadHashHex() error type = %T, want *HashMismatchError", err)
	}
}

func TestVerifyPayloadHashHex_InvalidHex(t *testing.T) {
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
			err := VerifyPayloadHashHex(payload, tc.hexStr)
			if err == nil {
				t.Fatal("VerifyPayloadHashHex() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("VerifyPayloadHashHex() error = %q, want to contain %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestVerifyPayloadHashHex_EmptyHex(t *testing.T) {
	payload := []byte("hello world")

	err := VerifyPayloadHashHex(payload, "")
	if err == nil {
		t.Fatal("VerifyPayloadHashHex() error = nil, want error for empty hex")
	}
	// Empty hex decodes to empty []byte, which triggers "expected hash is empty"
	if err.Error() != "expected hash is empty" {
		t.Errorf("VerifyPayloadHashHex() error = %q, want %q", err.Error(), "expected hash is empty")
	}
}

func TestVerifyPayloadHashHex_EmptyPayload(t *testing.T) {
	payload := []byte{}
	// SHA-256 of empty string in hex
	expectedHashHex := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

	err := VerifyPayloadHashHex(payload, expectedHashHex)
	if err != nil {
		t.Errorf("VerifyPayloadHashHex() error = %v, want nil for empty payload", err)
	}
}

func TestVerifyPayloadHashHex_CaseInsensitive(t *testing.T) {
	payload := []byte("hello world")
	// SHA-256("hello world") in uppercase
	expectedHashHexUpper := "B94D27B9934D3E08A52E52D7DA7DABFAC484EFE37A5380EE9088F7ACE2EFCDE9"

	err := VerifyPayloadHashHex(payload, expectedHashHexUpper)
	if err != nil {
		t.Errorf("VerifyPayloadHashHex() error = %v, want nil (hex should be case-insensitive)", err)
	}
}

func TestHashMismatchError_Error(t *testing.T) {
	err := &HashMismatchError{
		Expected: []byte{0x01, 0x02, 0x03},
		Actual:   []byte{0x04, 0x05, 0x06},
	}

	msg := err.Error()
	if !strings.Contains(msg, "010203") {
		t.Errorf("HashMismatchError.Error() = %q, want to contain expected hash hex", msg)
	}
	if !strings.Contains(msg, "040506") {
		t.Errorf("HashMismatchError.Error() = %q, want to contain actual hash hex", msg)
	}
	if !strings.Contains(msg, "payload hash mismatch") {
		t.Errorf("HashMismatchError.Error() = %q, want to contain 'payload hash mismatch'", msg)
	}
}

func TestVerifyPayloadHash_LargePayload(t *testing.T) {
	// Test with a larger payload to ensure no issues with size
	payload := make([]byte, 1024*1024) // 1MB
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	// Compute expected hash
	hash := sha256.Sum256(payload)

	err := VerifyPayloadHash(payload, hash[:])
	if err != nil {
		t.Errorf("VerifyPayloadHash() error = %v, want nil for large payload", err)
	}
}

func TestVerifyPayloadHash_BinaryPayload(t *testing.T) {
	// Test with binary payload containing all byte values
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}

	// Compute expected hash
	hash := sha256.Sum256(payload)

	err := VerifyPayloadHash(payload, hash[:])
	if err != nil {
		t.Errorf("VerifyPayloadHash() error = %v, want nil for binary payload", err)
	}
}

// TestPayloadStore_CloseDrainsPendingWrites tests that Close() properly drains
// pending write operations so callers don't block forever.
func TestPayloadStore_CloseDrainsPendingWrites(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test_drain.db")
	store, err := NewPayloadStore(PayloadStoreConfig{
		DBPath:        dbPath,

		FlushInterval: 1 * time.Second, // Slow flush to ensure operations queue up
		BatchSize:     100,             // Large batch size
	})
	if err != nil {
		t.Fatalf("NewPayloadStore() error = %v", err)
	}

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
		t.Fatal("timeout waiting for writers to complete - Close() did not drain pending operations")
	}

	select {
	case <-closeDone:
		// Good - Close completed
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for Close() to complete")
	}
}
