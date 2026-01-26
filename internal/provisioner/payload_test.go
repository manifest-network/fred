package provisioner

import (
	"sync"
	"testing"

	"github.com/manifest-network/fred/internal/testutil"
)

func TestPayloadStore_Store_Success(t *testing.T) {
	store := NewPayloadStore()
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
	store := NewPayloadStore()
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
	store := NewPayloadStore()
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
	store := NewPayloadStore()

	got := store.Get(testutil.ValidUUID1)
	if got != nil {
		t.Errorf("Get() = %v, want nil for non-existent", got)
	}
}

func TestPayloadStore_Pop(t *testing.T) {
	store := NewPayloadStore()
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
	store := NewPayloadStore()

	got := store.Pop(testutil.ValidUUID1)
	if got != nil {
		t.Errorf("Pop() = %v, want nil for non-existent", got)
	}
}

func TestPayloadStore_Has(t *testing.T) {
	store := NewPayloadStore()

	if store.Has(testutil.ValidUUID1) {
		t.Error("Has() = true for empty store")
	}

	store.Store(testutil.ValidUUID1, []byte("test"))

	if !store.Has(testutil.ValidUUID1) {
		t.Error("Has() = false after Store()")
	}
}

func TestPayloadStore_Delete(t *testing.T) {
	store := NewPayloadStore()
	payload := []byte("test payload")

	store.Store(testutil.ValidUUID1, payload)
	store.Delete(testutil.ValidUUID1)

	if store.Has(testutil.ValidUUID1) {
		t.Error("Has() = true after Delete()")
	}
}

func TestPayloadStore_Delete_NotFound(t *testing.T) {
	store := NewPayloadStore()

	// Should not panic for non-existent key
	store.Delete(testutil.ValidUUID1)
}

func TestPayloadStore_Count(t *testing.T) {
	store := NewPayloadStore()

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
	store := NewPayloadStore()
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
	store := NewPayloadStore()
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
