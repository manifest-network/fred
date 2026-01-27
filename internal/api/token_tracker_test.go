package api

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewTokenTracker(t *testing.T) {
	t.Run("requires_db_path", func(t *testing.T) {
		_, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: "",
		})
		if err == nil {
			t.Error("NewTokenTracker() should fail with empty DBPath")
		}
	})

	t.Run("creates_db_file", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")

		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
		})
		if err != nil {
			t.Fatalf("NewTokenTracker() error = %v", err)
		}
		defer tracker.Close()

		// Check file exists
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			t.Error("database file was not created")
		}
	})

	t.Run("uses_default_max_age", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")

		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			// MaxAge not set - should default to MaxTokenAge
		})
		if err != nil {
			t.Fatalf("NewTokenTracker() error = %v", err)
		}
		defer tracker.Close()

		if tracker.maxAge != MaxTokenAge {
			t.Errorf("maxAge = %v, want %v", tracker.maxAge, MaxTokenAge)
		}
	})
}

func TestTokenTracker_TryUse(t *testing.T) {
	t.Run("first_use_succeeds", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			MaxAge: 1 * time.Minute,
		})
		if err != nil {
			t.Fatalf("NewTokenTracker() error = %v", err)
		}
		defer tracker.Close()

		err = tracker.TryUse("test-token-123")
		if err != nil {
			t.Errorf("TryUse() first use error = %v", err)
		}
	})

	t.Run("second_use_fails", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			MaxAge: 1 * time.Minute,
		})
		if err != nil {
			t.Fatalf("NewTokenTracker() error = %v", err)
		}
		defer tracker.Close()

		// First use
		err = tracker.TryUse("test-token-456")
		if err != nil {
			t.Fatalf("TryUse() first use error = %v", err)
		}

		// Second use should fail
		err = tracker.TryUse("test-token-456")
		if err != ErrTokenAlreadyUsed {
			t.Errorf("TryUse() second use error = %v, want ErrTokenAlreadyUsed", err)
		}
	})

	t.Run("different_tokens_independent", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			MaxAge: 1 * time.Minute,
		})
		if err != nil {
			t.Fatalf("NewTokenTracker() error = %v", err)
		}
		defer tracker.Close()

		// Use token A
		err = tracker.TryUse("token-A")
		if err != nil {
			t.Fatalf("TryUse(A) first error = %v", err)
		}

		// Use token B should succeed
		err = tracker.TryUse("token-B")
		if err != nil {
			t.Errorf("TryUse(B) error = %v, want nil", err)
		}

		// Use token A again should fail
		err = tracker.TryUse("token-A")
		if err != ErrTokenAlreadyUsed {
			t.Errorf("TryUse(A) second error = %v, want ErrTokenAlreadyUsed", err)
		}
	})

	t.Run("expired_token_can_be_reused", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath:          dbPath,
			MaxAge:          50 * time.Millisecond, // Very short for testing
			CleanupInterval: 1 * time.Hour,         // Don't auto-cleanup during test
		})
		if err != nil {
			t.Fatalf("NewTokenTracker() error = %v", err)
		}
		defer tracker.Close()

		// First use
		err = tracker.TryUse("expiring-token")
		if err != nil {
			t.Fatalf("TryUse() first use error = %v", err)
		}

		// Wait for expiry
		time.Sleep(60 * time.Millisecond)

		// Should be able to reuse after expiry
		err = tracker.TryUse("expiring-token")
		if err != nil {
			t.Errorf("TryUse() after expiry error = %v, want nil", err)
		}
	})
}

func TestTokenTracker_Stats(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tokens.db")
	tracker, err := NewTokenTracker(TokenTrackerConfig{
		DBPath: dbPath,
		MaxAge: 1 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewTokenTracker() error = %v", err)
	}
	defer tracker.Close()

	// Initially empty
	count, err := tracker.Stats()
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if count != 0 {
		t.Errorf("Stats() initial count = %d, want 0", count)
	}

	// Add some tokens
	tracker.TryUse("token-1")
	tracker.TryUse("token-2")
	tracker.TryUse("token-3")

	count, err = tracker.Stats()
	if err != nil {
		t.Fatalf("Stats() error = %v", err)
	}
	if count != 3 {
		t.Errorf("Stats() count = %d, want 3", count)
	}
}

func TestTokenTracker_Cleanup(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tokens.db")
	tracker, err := NewTokenTracker(TokenTrackerConfig{
		DBPath:          dbPath,
		MaxAge:          50 * time.Millisecond, // Very short for testing
		CleanupInterval: 1 * time.Hour,         // Don't auto-cleanup
	})
	if err != nil {
		t.Fatalf("NewTokenTracker() error = %v", err)
	}
	defer tracker.Close()

	// Add tokens
	tracker.TryUse("token-1")
	tracker.TryUse("token-2")

	count, _ := tracker.Stats()
	if count != 2 {
		t.Fatalf("Stats() before cleanup = %d, want 2", count)
	}

	// Wait for expiry
	time.Sleep(60 * time.Millisecond)

	// Manual cleanup
	err = tracker.cleanup()
	if err != nil {
		t.Fatalf("cleanup() error = %v", err)
	}

	// Should be empty now
	count, _ = tracker.Stats()
	if count != 0 {
		t.Errorf("Stats() after cleanup = %d, want 0", count)
	}
}

func TestTokenTracker_Persistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tokens.db")

	// Create tracker and add tokens
	tracker1, err := NewTokenTracker(TokenTrackerConfig{
		DBPath: dbPath,
		MaxAge: 1 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewTokenTracker() error = %v", err)
	}

	err = tracker1.TryUse("persistent-token")
	if err != nil {
		t.Fatalf("TryUse() error = %v", err)
	}

	// Close first tracker
	tracker1.Close()

	// Open new tracker with same DB
	tracker2, err := NewTokenTracker(TokenTrackerConfig{
		DBPath: dbPath,
		MaxAge: 1 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewTokenTracker() second error = %v", err)
	}
	defer tracker2.Close()

	// Token should still be marked as used
	err = tracker2.TryUse("persistent-token")
	if err != ErrTokenAlreadyUsed {
		t.Errorf("TryUse() after restart error = %v, want ErrTokenAlreadyUsed", err)
	}
}

func TestTokenTracker_Close(t *testing.T) {
	t.Run("closes_without_error", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			MaxAge: 1 * time.Minute,
		})
		if err != nil {
			t.Fatalf("NewTokenTracker() error = %v", err)
		}

		// Close should not error
		err = tracker.Close()
		if err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	t.Run("close_is_idempotent", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			MaxAge: 1 * time.Minute,
		})
		if err != nil {
			t.Fatalf("NewTokenTracker() error = %v", err)
		}

		// First close
		err = tracker.Close()
		if err != nil {
			t.Errorf("Close() first call error = %v", err)
		}

		// Second close should not error (idempotent)
		err = tracker.Close()
		if err != nil {
			t.Errorf("Close() second call error = %v", err)
		}

		// Third close should also be fine
		err = tracker.Close()
		if err != nil {
			t.Errorf("Close() third call error = %v", err)
		}
	})
}

func TestTimeConversion(t *testing.T) {
	// Test round-trip conversion
	original := time.Now()
	bytes := timeToBytes(original)
	restored := bytesToTime(bytes)

	// UnixNano precision
	if original.UnixNano() != restored.UnixNano() {
		t.Errorf("time round-trip failed: original=%v, restored=%v", original.UnixNano(), restored.UnixNano())
	}
}

func TestBytesToTime_InvalidInput(t *testing.T) {
	// Invalid length should return zero time
	result := bytesToTime([]byte{1, 2, 3}) // Only 3 bytes, need 8
	if !result.IsZero() {
		t.Errorf("bytesToTime(short) = %v, want zero time", result)
	}
}
