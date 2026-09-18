package api

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

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
		assert.False(t, errors.Is(err, fs.ErrNotExist), "database file was not created")
	})

	t.Run("uses_default_cleanup_interval", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")

		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
			// Cleanup defaults to the token validity window.
		})
		require.NoError(t, err)
		defer tracker.Close()

		assert.Equal(t, MaxTokenAge, tracker.cleanupInterval)
	})
}

func TestTokenTracker_TryUse(t *testing.T) {
	t.Run("first_use_succeeds", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
		})
		require.NoError(t, err)
		defer tracker.Close()

		err = tracker.TryUse(replayClaimForTest("test-token-123"))
		assert.NoError(t, err)
	})

	t.Run("second_use_fails", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
		})
		require.NoError(t, err)
		defer tracker.Close()

		// First use
		err = tracker.TryUse(replayClaimForTest("test-token-456"))
		require.NoError(t, err)

		// Second use should fail
		err = tracker.TryUse(replayClaimForTest("test-token-456"))
		assert.Equal(t, ErrTokenAlreadyUsed, err)
	})

	t.Run("different_tokens_independent", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
		})
		require.NoError(t, err)
		defer tracker.Close()

		// Use token A
		err = tracker.TryUse(replayClaimForTest("token-A"))
		require.NoError(t, err)

		// Use token B should succeed
		err = tracker.TryUse(replayClaimForTest("token-B"))
		assert.NoError(t, err)

		// Use token A again should fail
		err = tracker.TryUse(replayClaimForTest("token-A"))
		assert.Equal(t, ErrTokenAlreadyUsed, err)
	})

	t.Run("expired_claim_is_rejected", func(t *testing.T) {
		tracker, err := NewTokenTracker(TokenTrackerConfig{DBPath: filepath.Join(t.TempDir(), "tokens.db")})
		require.NoError(t, err)
		defer tracker.Close()
		assert.ErrorIs(t, tracker.TryUse(TokenReplayClaim{signature: "expired", expiresAt: time.Now()}), ErrInvalidReplayClaim)
		assert.ErrorIs(t, tracker.TryUse(TokenReplayClaim{}), ErrInvalidReplayClaim)
	})
}

func TestTokenTracker_Cleanup(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tokens.db")
	tracker, err := NewTokenTracker(TokenTrackerConfig{
		DBPath:          dbPath,
		CleanupInterval: 1 * time.Hour, // Don't auto-cleanup
	})
	require.NoError(t, err)
	defer tracker.Close()

	// Add tokens
	tracker.TryUse(replayClaimForTest("token-1"))
	tracker.TryUse(replayClaimForTest("token-2"))

	// Verify tokens are tracked (replay should fail)
	err = tracker.TryUse(replayClaimForTest("token-1"))
	require.Equal(t, ErrTokenAlreadyUsed, err, "TryUse() before expiry should return ErrTokenAlreadyUsed")

	// Expire both records deterministically. A sub-100ms wall-clock window makes
	// the pre-expiry assertion flaky when the full repository suite is under CPU
	// contention, and this test is about cleanup semantics rather than timers.
	err = tracker.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		expired := util.TimeToBytes(time.Now().Add(-MaxFutureClockSkew - time.Second))
		if err := b.Put([]byte("token-1"), expired); err != nil {
			return err
		}
		return b.Put([]byte("token-2"), expired)
	})
	require.NoError(t, err)

	// Manual cleanup
	err = tracker.cleanup()
	require.NoError(t, err)

	// Cleanup removes persisted expired rows, without authorizing an expired claim.
	require.NoError(t, tracker.db.View(func(tx *bolt.Tx) error {
		assert.Nil(t, tx.Bucket(bucketName).Get([]byte("token-1")))
		assert.Nil(t, tx.Bucket(bucketName).Get([]byte("token-2")))
		return nil
	}))
}

func TestTokenTracker_Persistence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "tokens.db")

	// Create tracker and add tokens
	tracker1, err := NewTokenTracker(TokenTrackerConfig{
		DBPath: dbPath,
	})
	require.NoError(t, err)

	err = tracker1.TryUse(replayClaimForTest("persistent-token"))
	require.NoError(t, err)

	// Close first tracker
	tracker1.Close()

	// Open new tracker with same DB
	tracker2, err := NewTokenTracker(TokenTrackerConfig{
		DBPath: dbPath,
	})
	require.NoError(t, err)
	defer tracker2.Close()

	// Token should still be marked as used
	err = tracker2.TryUse(replayClaimForTest("persistent-token"))
	assert.Equal(t, ErrTokenAlreadyUsed, err)
}

func TestTokenTracker_Close(t *testing.T) {
	t.Run("closes_without_error", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{
			DBPath: dbPath,
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

// Cache mechanics fixtures do not bypass cryptographic validation in production.
func replayClaimForTest(signature string) TokenReplayClaim {
	return TokenReplayClaim{signature: signature, expiresAt: time.Now().Add(time.Hour)}
}
