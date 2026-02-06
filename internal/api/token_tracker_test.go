package api

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/util"
)

func TestNewTokenTracker(t *testing.T) {
	t.Run("requires_db_path", func(t *testing.T) {
		_, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: "",
		})
		assert.Error(t, err, "NewTokenTracker() should fail with empty DBPath")
	})

	t.Run("creates_db_file", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")

		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
		})
		require.NoError(t, err)
		defer tracker.Close()

		// Check file exists
		_, err = os.Stat(dbPath)
		assert.False(t, os.IsNotExist(err), "database file was not created")
	})

	t.Run("uses_default_max_age", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")

		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			// MaxAge not set - should default to MaxTokenAge
		})
		require.NoError(t, err)
		defer tracker.Close()

		assert.Equal(t, MaxTokenAge, tracker.maxAge)
	})
}

func TestTokenTracker_TryUse(t *testing.T) {
	t.Run("first_use_succeeds", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			MaxAge: 1 * time.Minute,
		})
		require.NoError(t, err)
		defer tracker.Close()

		err = tracker.TryUse("test-token-123")
		assert.NoError(t, err)
	})

	t.Run("second_use_fails", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			MaxAge: 1 * time.Minute,
		})
		require.NoError(t, err)
		defer tracker.Close()

		// First use
		err = tracker.TryUse("test-token-456")
		require.NoError(t, err)

		// Second use should fail
		err = tracker.TryUse("test-token-456")
		assert.Equal(t, ErrTokenAlreadyUsed, err)
	})

	t.Run("different_tokens_independent", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			MaxAge: 1 * time.Minute,
		})
		require.NoError(t, err)
		defer tracker.Close()

		// Use token A
		err = tracker.TryUse("token-A")
		require.NoError(t, err)

		// Use token B should succeed
		err = tracker.TryUse("token-B")
		assert.NoError(t, err)

		// Use token A again should fail
		err = tracker.TryUse("token-A")
		assert.Equal(t, ErrTokenAlreadyUsed, err)
	})

	t.Run("expired_token_can_be_reused", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath:          dbPath,
			MaxAge:          50 * time.Millisecond, // Very short for testing
			CleanupInterval: 1 * time.Hour,         // Don't auto-cleanup during test
		})
		require.NoError(t, err)
		defer tracker.Close()

		// First use
		err = tracker.TryUse("expiring-token")
		require.NoError(t, err)

		// Wait for expiry
		time.Sleep(60 * time.Millisecond)

		// Should be able to reuse after expiry
		err = tracker.TryUse("expiring-token")
		assert.NoError(t, err)
	})
}

func TestTokenTracker_Cleanup(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tokens.db")
	tracker, err := NewTokenTracker(TokenTrackerConfig{
		DBPath:          dbPath,
		MaxAge:          50 * time.Millisecond, // Very short for testing
		CleanupInterval: 1 * time.Hour,         // Don't auto-cleanup
	})
	require.NoError(t, err)
	defer tracker.Close()

	// Add tokens
	tracker.TryUse("token-1")
	tracker.TryUse("token-2")

	// Verify tokens are tracked (replay should fail)
	err = tracker.TryUse("token-1")
	require.Equal(t, ErrTokenAlreadyUsed, err, "TryUse() before expiry should return ErrTokenAlreadyUsed")

	// Wait for expiry
	time.Sleep(60 * time.Millisecond)

	// Manual cleanup
	err = tracker.cleanup()
	require.NoError(t, err)

	// After cleanup, tokens should be reusable (they were cleaned up)
	assert.NoError(t, tracker.TryUse("token-1"), "token should be reusable")
	assert.NoError(t, tracker.TryUse("token-2"), "token should be reusable")
}

func TestTokenTracker_Persistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tokens.db")

	// Create tracker and add tokens
	tracker1, err := NewTokenTracker(TokenTrackerConfig{
		DBPath: dbPath,
		MaxAge: 1 * time.Minute,
	})
	require.NoError(t, err)

	err = tracker1.TryUse("persistent-token")
	require.NoError(t, err)

	// Close first tracker
	tracker1.Close()

	// Open new tracker with same DB
	tracker2, err := NewTokenTracker(TokenTrackerConfig{
		DBPath: dbPath,
		MaxAge: 1 * time.Minute,
	})
	require.NoError(t, err)
	defer tracker2.Close()

	// Token should still be marked as used
	err = tracker2.TryUse("persistent-token")
	assert.Equal(t, ErrTokenAlreadyUsed, err)
}

func TestTokenTracker_Close(t *testing.T) {
	t.Run("closes_without_error", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			MaxAge: 1 * time.Minute,
		})
		require.NoError(t, err)

		// Close should not error
		err = tracker.Close()
		assert.NoError(t, err)
	})

	t.Run("close_is_idempotent", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			MaxAge: 1 * time.Minute,
		})
		require.NoError(t, err)

		// First close
		err = tracker.Close()
		assert.NoError(t, err)

		// Second close should not error (idempotent)
		err = tracker.Close()
		assert.NoError(t, err)

		// Third close should also be fine
		err = tracker.Close()
		assert.NoError(t, err)
	})
}

func TestTimeConversion(t *testing.T) {
	// Test round-trip conversion
	original := time.Now()
	bytes := util.TimeToBytes(original)
	restored := util.BytesToTime(bytes)

	// UnixNano precision
	assert.Equal(t, original.UnixNano(), restored.UnixNano(), "time round-trip failed")
}

func TestBytesToTime_InvalidInput(t *testing.T) {
	// Invalid length should return zero time
	result := util.BytesToTime([]byte{1, 2, 3}) // Only 3 bytes, need 8
	assert.True(t, result.IsZero())
}
