package api

import (
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/testutil"
	"github.com/manifest-network/fred/internal/util"
)

func TestFutureDatedTokenReplayRemainsConsumedAcrossRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{DBPath: path})
		require.NoError(t, err)
		key := testutil.NewTestKeyPair("future-replay")
		encoded := testutil.CreateTestToken(key, testutil.ValidUUID1, time.Now().Add(MaxFutureClockSkew))
		token, err := ParseAuthToken(encoded)
		require.NoError(t, err)
		require.NoError(t, token.Validate("manifest"))
		require.Equal(t, time.Unix(token.Timestamp, 0).Add(MaxTokenAge), token.replay.expiresAt)
		require.NoError(t, tracker.TryUse(token.replay))
		time.Sleep(MaxTokenAge + time.Second)
		require.NoError(t, tracker.Close())
		tracker, err = NewTokenTracker(TokenTrackerConfig{DBPath: path})
		require.NoError(t, err)
		defer tracker.Close()
		replayed, err := ParseAuthToken(encoded)
		require.NoError(t, err)
		require.NoError(t, replayed.Validate("manifest"), "token is still inside its signed validity window")
		require.ErrorIs(t, tracker.TryUse(replayed.replay), ErrTokenAlreadyUsed)
		time.Sleep(time.Until(token.replay.expiresAt))
		require.ErrorIs(t, tracker.TryUse(token.replay), ErrInvalidReplayClaim)
		require.Error(t, replayed.Validate("manifest"))
		require.Equal(t, TokenReplayClaim{}, replayed.replay, "failed revalidation revokes the cached claim")
	})
}

func TestReplayTrackerRetainsLegacyRowsThroughFutureSkew(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "tokens.db")
		tracker, err := NewTokenTracker(TokenTrackerConfig{DBPath: path})
		require.NoError(t, err)
		claim := TokenReplayClaim{signature: "legacy-canonical-signature", expiresAt: time.Now().Add(MaxTokenAge + MaxFutureClockSkew)}
		require.NoError(t, tracker.db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket(bucketName).Put([]byte(claim.signature), util.TimeToBytes(time.Now().Add(MaxTokenAge)))
		}))
		require.NoError(t, tracker.Close())
		time.Sleep(MaxTokenAge + time.Second)
		tracker, err = NewTokenTracker(TokenTrackerConfig{DBPath: path})
		require.NoError(t, err)
		defer tracker.Close()
		require.ErrorIs(t, tracker.TryUse(claim), ErrTokenAlreadyUsed)
		time.Sleep(MaxFutureClockSkew)
		require.NoError(t, tracker.cleanup())
		require.NoError(t, tracker.db.View(func(tx *bolt.Tx) error {
			require.Nil(t, tx.Bucket(bucketName).Get([]byte(claim.signature)))
			return nil
		}))
	})
}
