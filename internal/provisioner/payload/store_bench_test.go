package payload

import (
	"crypto/rand"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitForFlush polls until the specified key is visible in the store.
// This is more reliable than time.Sleep for synchronization.
func waitForFlush(t testing.TB, store *Store, key string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, _ := store.Get(key)
		if data != nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting for key %q to be flushed", key)
}

// BenchmarkStore_Write benchmarks single payload writes.
func BenchmarkStore_Write(b *testing.B) {
	dir := b.TempDir()
	store, err := NewStore(StoreConfig{
		DBPath: filepath.Join(dir, "payload.db"),

		FlushInterval: time.Millisecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })

	payload := make([]byte, 1024)
	rand.Read(payload)

	b.ResetTimer()
	// Use b.Loop() for Go 1.24+ - faster and more accurate benchmarking
	i := 0
	for b.Loop() {
		leaseUUID := fmt.Sprintf("lease-%d", i)
		store.Store(leaseUUID, payload)
		i++
	}
	b.StopTimer()
}

// BenchmarkStore_Write_Parallel benchmarks concurrent payload writes.
func BenchmarkStore_Write_Parallel(b *testing.B) {
	dir := b.TempDir()
	store, err := NewStore(StoreConfig{
		DBPath: filepath.Join(dir, "payload.db"),

		BatchSize:     100,
		FlushInterval: time.Millisecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })

	var counter atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		payload := make([]byte, 1024)
		rand.Read(payload)
		for pb.Next() {
			id := counter.Add(1)
			leaseUUID := fmt.Sprintf("lease-%d", id)
			store.Store(leaseUUID, payload)
		}
	})
	b.StopTimer()
}

// BenchmarkStore_Read benchmarks payload reads.
func BenchmarkStore_Read(b *testing.B) {
	dir := b.TempDir()
	store, err := NewStore(StoreConfig{
		DBPath: filepath.Join(dir, "payload.db"),

		FlushInterval: time.Millisecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })

	// Pre-populate store
	const numEntries = 1000
	payload := make([]byte, 1024)
	rand.Read(payload)
	lastKey := ""
	for i := 0; i < numEntries; i++ {
		lastKey = fmt.Sprintf("lease-%d", i)
		store.Store(lastKey, payload)
	}
	waitForFlush(b, store, lastKey, time.Second)

	b.ResetTimer()
	// Use b.Loop() for Go 1.24+ - faster and more accurate benchmarking
	i := 0
	for b.Loop() {
		leaseUUID := fmt.Sprintf("lease-%d", i%numEntries)
		_, _ = store.Get(leaseUUID)
		i++
	}
	b.StopTimer()
}

// BenchmarkStore_Read_Parallel benchmarks concurrent payload reads.
func BenchmarkStore_Read_Parallel(b *testing.B) {
	dir := b.TempDir()
	store, err := NewStore(StoreConfig{
		DBPath: filepath.Join(dir, "payload.db"),

		FlushInterval: time.Millisecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })

	// Pre-populate store
	const numEntries = 1000
	payload := make([]byte, 1024)
	rand.Read(payload)
	lastKey := ""
	for i := 0; i < numEntries; i++ {
		lastKey = fmt.Sprintf("lease-%d", i)
		store.Store(lastKey, payload)
	}
	waitForFlush(b, store, lastKey, time.Second)

	var counter atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := counter.Add(1)
			leaseUUID := fmt.Sprintf("lease-%d", id%numEntries)
			_, _ = store.Get(leaseUUID)
		}
	})
	b.StopTimer()
}

// BenchmarkStore_BatchEfficiency compares different batch sizes.
func BenchmarkStore_BatchEfficiency(b *testing.B) {
	batchSizes := []int{1, 10, 50, 100, 200}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			dir := b.TempDir()
			store, err := NewStore(StoreConfig{
				DBPath: filepath.Join(dir, "payload.db"),

				BatchSize:     batchSize,
				FlushInterval: time.Millisecond,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { store.Close() })

			var counter atomic.Int64
			payload := make([]byte, 1024)
			rand.Read(payload)

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := counter.Add(1)
					store.Store(fmt.Sprintf("lease-%d", id), payload)
				}
			})
			b.StopTimer()
		})
	}
}

// BenchmarkStore_LargePayload benchmarks large payload handling.
func BenchmarkStore_LargePayload(b *testing.B) {
	sizes := []struct {
		name string
		size int
	}{
		{"1KB", 1 << 10},
		{"10KB", 10 << 10},
		{"100KB", 100 << 10},
		{"1MB", 1 << 20},
	}

	for _, tc := range sizes {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			store, err := NewStore(StoreConfig{
				DBPath: filepath.Join(dir, "payload.db"),

				FlushInterval: time.Millisecond,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { store.Close() })

			payload := make([]byte, tc.size)
			rand.Read(payload)

			b.ResetTimer()
			b.SetBytes(int64(tc.size))
			// Use b.Loop() for Go 1.24+ - faster and more accurate benchmarking
			i := 0
			for b.Loop() {
				leaseUUID := fmt.Sprintf("lease-%d", i)
				store.Store(leaseUUID, payload)
				i++
			}
			b.StopTimer()
		})
	}
}

// BenchmarkStore_MixedWorkload simulates realistic read/write mix.
func BenchmarkStore_MixedWorkload(b *testing.B) {
	dir := b.TempDir()
	store, err := NewStore(StoreConfig{
		DBPath: filepath.Join(dir, "payload.db"),

		BatchSize:     50,
		FlushInterval: time.Millisecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })

	// Pre-populate with some data
	const numEntries = 500
	payload := make([]byte, 1024)
	rand.Read(payload)
	lastKey := ""
	for i := 0; i < numEntries; i++ {
		lastKey = fmt.Sprintf("lease-%d", i)
		store.Store(lastKey, payload)
	}
	waitForFlush(b, store, lastKey, time.Second)

	var writeCounter atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localPayload := make([]byte, 1024)
		rand.Read(localPayload)
		opCount := 0
		for pb.Next() {
			opCount++
			// 20% writes, 80% reads
			if opCount%5 == 0 {
				id := writeCounter.Add(1) + numEntries
				store.Store(fmt.Sprintf("lease-%d", id), localPayload)
			} else {
				_, _ = store.Get(fmt.Sprintf("lease-%d", opCount%numEntries))
			}
		}
	})
	b.StopTimer()
}

// TestStore_StressTest performs a stress test with concurrent operations.
// Use -short to skip this test.
func TestStore_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	dir := t.TempDir()
	store, err := NewStore(StoreConfig{
		DBPath: filepath.Join(dir, "payload.db"),

		BatchSize:     100,
		FlushInterval: time.Millisecond,
	})
	require.NoError(t, err)
	defer store.Close()

	// Reduced from 100 goroutines x 1000 ops to avoid long test times
	const (
		numGoroutines = 50
		opsPerRoutine = 200
	)

	var wg sync.WaitGroup
	var writeOps, readOps, deleteOps atomic.Int64
	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			payload := make([]byte, 1024)
			rand.Read(payload)

			for i := 0; i < opsPerRoutine; i++ {
				leaseUUID := fmt.Sprintf("lease-%d-%d", gid, i%50) // Reuse some UUIDs

				switch i % 10 {
				case 0, 1, 2: // 30% writes
					store.Store(leaseUUID, payload)
					writeOps.Add(1)
				case 3: // 10% deletes
					store.Delete(leaseUUID)
					deleteOps.Add(1)
				default: // 60% reads
					_, _ = store.Get(leaseUUID)
					readOps.Add(1)
				}
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalOps := writeOps.Load() + readOps.Load() + deleteOps.Load()
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	t.Logf("Stress test completed in %v", elapsed)
	t.Logf("Total operations: %d (%.0f ops/sec)", totalOps, opsPerSec)
	t.Logf("  Writes: %d", writeOps.Load())
	t.Logf("  Reads: %d", readOps.Load())
	t.Logf("  Deletes: %d", deleteOps.Load())
}

// TestStore_HighConcurrencyWrites tests behavior under write-heavy load.
// Use -short to skip this test.
func TestStore_HighConcurrencyWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high concurrency test in short mode")
	}

	dir := t.TempDir()
	store, err := NewStore(StoreConfig{
		DBPath: filepath.Join(dir, "payload.db"),

		BatchSize:     200,
		FlushInterval: time.Millisecond,
	})
	require.NoError(t, err)
	defer store.Close()

	// Reduced from 200 goroutines x 500 writes
	const (
		numGoroutines    = 100
		writesPerRoutine = 100
	)

	var wg sync.WaitGroup
	var successCount, failCount atomic.Int64
	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			payload := make([]byte, 2048) // 2KB payloads
			rand.Read(payload)

			for i := 0; i < writesPerRoutine; i++ {
				leaseUUID := fmt.Sprintf("lease-%d-%d", gid, i)
				if store.Store(leaseUUID, payload) {
					successCount.Add(1)
				} else {
					failCount.Add(1)
				}
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalWrites := successCount.Load() + failCount.Load()
	writesPerSec := float64(totalWrites) / elapsed.Seconds()

	t.Logf("High concurrency write test completed in %v", elapsed)
	t.Logf("Total writes: %d (%.0f writes/sec)", totalWrites, writesPerSec)
	t.Logf("  Successful: %d", successCount.Load())
	t.Logf("  Failed (duplicates): %d", failCount.Load())

	// Verify data integrity - sample check
	for g := 0; g < 10; g++ {
		leaseUUID := fmt.Sprintf("lease-%d-0", g)
		got, err := store.Get(leaseUUID)
		assert.NoError(t, err, "Get error for lease %s", leaseUUID)
		assert.NotNil(t, got, "failed to retrieve lease %s", leaseUUID)
	}
}
