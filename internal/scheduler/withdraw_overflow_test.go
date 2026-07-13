package scheduler

import (
	"testing"
	"time"

	sdkmath "cosmossdk.io/math"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWithdrawScheduler_EstimateDepletionTime_ClampsOverflow verifies that a
// whale balance with a minimal per-interval burn does not panic or produce a
// wrapped time. secondsRemaining = curr * elapsedSeconds / burned = 3e15 * 3600
// / 1 = 1.08e19, which exceeds int64 max (~9.22e18); the previous code called
// LegacyDec.TruncateInt64() (panic "Int64() out of bound") on it (ENG-500). The
// horizon must clamp to a finite, far-future value instead.
func TestWithdrawScheduler_EstimateDepletionTime_ClampsOverflow(t *testing.T) {
	s := NewWithdrawScheduler(&mockChainClient{}, WithdrawSchedulerConfig{
		ProviderUUID: "test-uuid",
		Interval:     time.Minute,
	})
	now := time.Now()

	prev := sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(3_000_000_000_000_001)))
	curr := sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(3_000_000_000_000_000)))

	var depletion time.Time
	require.NotPanics(t, func() {
		depletion = s.estimateDepletionTime(prev, curr, time.Hour, now)
	})

	assert.True(t, depletion.After(now),
		"clamped depletion must be in the future, got %v", depletion)
	assert.Greater(t, depletion.Sub(now), 100*365*24*time.Hour,
		"an enormous balance should map to a far-future horizon, got %v", depletion.Sub(now))
}

// TestWithdrawScheduler_EstimateDepletionTime_ClampsDurationOverflow covers the
// milder band: secondsRemaining = 1e13 * 3600 = 3.6e16 fits int64 (no
// TruncateInt64 panic) but time.Duration(3.6e16)*time.Second overflows int64
// nanoseconds and wraps to a bogus (possibly past) time. The clamp must yield a
// sane future horizon (ENG-500).
func TestWithdrawScheduler_EstimateDepletionTime_ClampsDurationOverflow(t *testing.T) {
	s := NewWithdrawScheduler(&mockChainClient{}, WithdrawSchedulerConfig{
		ProviderUUID: "test-uuid",
		Interval:     time.Minute,
	})
	now := time.Now()

	prev := sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(10_000_000_000_001)))
	curr := sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(10_000_000_000_000)))

	depletion := s.estimateDepletionTime(prev, curr, time.Hour, now)

	assert.True(t, depletion.After(now),
		"duration-overflow horizon must clamp to a future time, got %v", depletion)
	assert.Greater(t, depletion.Sub(now), 100*365*24*time.Hour,
		"the clamped horizon should be far in the future, got %v", depletion.Sub(now))
}
