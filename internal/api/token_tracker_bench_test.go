package api

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkTokenTracker_TryUse benchmarks single token use operations.
func BenchmarkTokenTracker_TryUse(b *testing.B) {
	dir := b.TempDir()
	tracker, err := NewTokenTracker(TokenTrackerConfig{
		DBPath:          filepath.Join(dir, "tokens.db"),
		MaxAge:          time.Hour,
		CleanupInterval: time.Hour,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer tracker.Close()

	// Pre-generate unique tokens
	tokens := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		tokenBytes := make([]byte, 32)
		rand.Read(tokenBytes)
		tokens[i] = hex.EncodeToString(tokenBytes)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := tracker.TryUse(tokens[i]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTokenTracker_TryUse_Parallel benchmarks concurrent token use.
func BenchmarkTokenTracker_TryUse_Parallel(b *testing.B) {
	dir := b.TempDir()
	tracker, err := NewTokenTracker(TokenTrackerConfig{
		DBPath:          filepath.Join(dir, "tokens.db"),
		MaxAge:          time.Hour,
		CleanupInterval: time.Hour,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer tracker.Close()

	var counter atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := counter.Add(1)
			token := fmt.Sprintf("token-%d", id)
			if err := tracker.TryUse(token); err != nil {
				b.Error(err)
			}
		}
	})
}

// BenchmarkTokenTracker_ReplayDetection benchmarks detecting already-used tokens.
func BenchmarkTokenTracker_ReplayDetection(b *testing.B) {
	dir := b.TempDir()
	tracker, err := NewTokenTracker(TokenTrackerConfig{
		DBPath:          filepath.Join(dir, "tokens.db"),
		MaxAge:          time.Hour,
		CleanupInterval: time.Hour,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer tracker.Close()

	// Pre-populate with tokens
	const numTokens = 1000
	tokens := make([]string, numTokens)
	for i := 0; i < numTokens; i++ {
		tokens[i] = fmt.Sprintf("preload-token-%d", i)
		if err := tracker.TryUse(tokens[i]); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Try to reuse an existing token - should return ErrTokenAlreadyUsed
		err := tracker.TryUse(tokens[i%numTokens])
		if err != ErrTokenAlreadyUsed {
			b.Fatalf("expected ErrTokenAlreadyUsed, got %v", err)
		}
	}
}

// BenchmarkTokenTracker_MixedWorkload simulates realistic token checking.
func BenchmarkTokenTracker_MixedWorkload(b *testing.B) {
	dir := b.TempDir()
	tracker, err := NewTokenTracker(TokenTrackerConfig{
		DBPath:          filepath.Join(dir, "tokens.db"),
		MaxAge:          time.Hour,
		CleanupInterval: time.Hour,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer tracker.Close()

	// Pre-populate with some tokens (simulating replay attempts)
	const numExistingTokens = 100
	existingTokens := make([]string, numExistingTokens)
	for i := 0; i < numExistingTokens; i++ {
		existingTokens[i] = fmt.Sprintf("existing-token-%d", i)
		tracker.TryUse(existingTokens[i])
	}

	var newTokenCounter atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localCounter := 0
		for pb.Next() {
			localCounter++
			// 90% new tokens, 10% replay attempts
			if localCounter%10 == 0 {
				tracker.TryUse(existingTokens[localCounter%numExistingTokens])
			} else {
				id := newTokenCounter.Add(1)
				token := fmt.Sprintf("new-token-%d", id)
				tracker.TryUse(token)
			}
		}
	})
}

// TestTokenTracker_StressTest performs a stress test with many concurrent operations.
func TestTokenTracker_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	dir := t.TempDir()
	tracker, err := NewTokenTracker(TokenTrackerConfig{
		DBPath:          filepath.Join(dir, "tokens.db"),
		MaxAge:          time.Hour,
		CleanupInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()

	const (
		numGoroutines = 100
		opsPerRoutine = 1000
	)

	var wg sync.WaitGroup
	var newTokens, replays, errors atomic.Int64
	start := time.Now()

	// Shared tokens for replay testing
	sharedTokens := make([]string, 50)
	for i := range sharedTokens {
		sharedTokens[i] = fmt.Sprintf("shared-token-%d", i)
	}

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()

			for i := 0; i < opsPerRoutine; i++ {
				var token string
				if i%20 == 0 {
					// 5% use shared tokens (high contention)
					token = sharedTokens[i%len(sharedTokens)]
				} else {
					// 95% unique tokens
					token = fmt.Sprintf("token-%d-%d", gid, i)
				}

				err := tracker.TryUse(token)
				switch err {
				case nil:
					newTokens.Add(1)
				case ErrTokenAlreadyUsed:
					replays.Add(1)
				default:
					errors.Add(1)
				}
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalOps := newTokens.Load() + replays.Load() + errors.Load()
	opsPerSec := float64(totalOps) / elapsed.Seconds()

	t.Logf("Token tracker stress test completed in %v", elapsed)
	t.Logf("Total operations: %d (%.0f ops/sec)", totalOps, opsPerSec)
	t.Logf("  New tokens: %d", newTokens.Load())
	t.Logf("  Replay detections: %d", replays.Load())
	t.Logf("  Errors: %d", errors.Load())

	if errors.Load() > 0 {
		t.Errorf("unexpected errors during stress test: %d", errors.Load())
	}
}

// TestTokenTracker_HighContention tests behavior under high contention.
func TestTokenTracker_HighContention(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high contention test in short mode")
	}

	dir := t.TempDir()
	tracker, err := NewTokenTracker(TokenTrackerConfig{
		DBPath:          filepath.Join(dir, "tokens.db"),
		MaxAge:          time.Hour,
		CleanupInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.Close()

	const (
		numGoroutines   = 200
		opsPerRoutine   = 500
		numSharedTokens = 10 // Very few tokens = high contention
	)

	sharedTokens := make([]string, numSharedTokens)
	for i := range sharedTokens {
		sharedTokens[i] = fmt.Sprintf("contended-token-%d", i)
	}

	var wg sync.WaitGroup
	var firstUse, replayDetected, errors atomic.Int64
	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < opsPerRoutine; i++ {
				token := sharedTokens[i%numSharedTokens]
				err := tracker.TryUse(token)
				switch err {
				case nil:
					firstUse.Add(1)
				case ErrTokenAlreadyUsed:
					replayDetected.Add(1)
				default:
					errors.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	t.Logf("High contention test completed in %v", elapsed)
	t.Logf("  First use wins: %d", firstUse.Load())
	t.Logf("  Replay detected: %d", replayDetected.Load())
	t.Logf("  Errors: %d", errors.Load())

	// Only numSharedTokens should succeed (one per token)
	if firstUse.Load() != numSharedTokens {
		t.Errorf("expected %d first uses, got %d", numSharedTokens, firstUse.Load())
	}

	if errors.Load() > 0 {
		t.Errorf("unexpected errors: %d", errors.Load())
	}
}
