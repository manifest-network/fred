package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	sdkmath "cosmossdk.io/math"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/metrics"
)

// mockChainClient implements ChainClient interface for testing.
type mockChainClient struct {
	GetProviderWithdrawableFunc   func(ctx context.Context, providerUUID string) (sdktypes.Coins, error)
	WithdrawByProviderFunc        func(ctx context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error)
	GetActiveLeasesByProviderFunc func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
	GetCreditAccountFunc          func(ctx context.Context, tenant string) (*billingtypes.CreditAccount, sdktypes.Coins, error)
	CloseLeasesFunc               func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
}

func (m *mockChainClient) GetProviderWithdrawable(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
	if m.GetProviderWithdrawableFunc != nil {
		return m.GetProviderWithdrawableFunc(ctx, providerUUID)
	}
	return nil, nil
}

func (m *mockChainClient) WithdrawByProvider(ctx context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
	if m.WithdrawByProviderFunc != nil {
		return m.WithdrawByProviderFunc(ctx, providerUUID, key)
	}
	return "", &billingtypes.MsgWithdrawResponse{}, nil
}

func (m *mockChainClient) GetActiveLeasesByProvider(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
	if m.GetActiveLeasesByProviderFunc != nil {
		return m.GetActiveLeasesByProviderFunc(ctx, providerUUID)
	}
	return nil, nil
}

func (m *mockChainClient) GetCreditAccount(ctx context.Context, tenant string) (*billingtypes.CreditAccount, sdktypes.Coins, error) {
	if m.GetCreditAccountFunc != nil {
		return m.GetCreditAccountFunc(ctx, tenant)
	}
	return nil, nil, nil
}

func (m *mockChainClient) CloseLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
	if m.CloseLeasesFunc != nil {
		return m.CloseLeasesFunc(ctx, leaseUUIDs, reason)
	}
	return 0, nil, nil
}

func TestNewWithdrawScheduler(t *testing.T) {
	tests := []struct {
		name               string
		cfg                WithdrawSchedulerConfig
		wantMaxIterations  int
		wantErrorThreshold int
		wantRetryInterval  time.Duration
	}{
		{
			name: "with defaults",
			cfg: WithdrawSchedulerConfig{
				ProviderUUID:     "test-uuid",
				WithdrawInterval: time.Minute,
			},
			wantMaxIterations:  100,
			wantErrorThreshold: 3,
			wantRetryInterval:  30 * time.Second,
		},
		{
			name: "with custom values",
			cfg: WithdrawSchedulerConfig{
				ProviderUUID:              "test-uuid",
				WithdrawInterval:          5 * time.Minute,
				MaxWithdrawIterations:     50,
				CreditCheckErrorThreshold: 5,
				CreditCheckRetryInterval:  time.Minute,
			},
			wantMaxIterations:  50,
			wantErrorThreshold: 5,
			wantRetryInterval:  time.Minute,
		},
		{
			name: "zero values get defaults",
			cfg: WithdrawSchedulerConfig{
				ProviderUUID:              "test-uuid",
				WithdrawInterval:          time.Minute,
				MaxWithdrawIterations:     0,
				CreditCheckErrorThreshold: 0,
				CreditCheckRetryInterval:  0,
			},
			wantMaxIterations:  100,
			wantErrorThreshold: 3,
			wantRetryInterval:  30 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockChainClient{}
			s := NewWithdrawScheduler(client, tt.cfg)

			require.NotNil(t, s, "NewWithdrawScheduler() returned nil")
			assert.Equal(t, tt.cfg.ProviderUUID, s.providerUUID)
			assert.Equal(t, tt.cfg.WithdrawInterval, s.withdrawInterval)
			assert.Equal(t, tt.wantMaxIterations, s.maxWithdrawIterations)
			assert.Equal(t, tt.wantErrorThreshold, s.creditCheckErrorThreshold)
			assert.Equal(t, tt.wantRetryInterval, s.creditCheckRetryInterval)
			assert.NotNil(t, s.tenants, "tenants map is nil")
		})
	}
}

func TestWithdrawScheduler_Withdraw_NothingToWithdraw(t *testing.T) {
	var withdrawCalled bool

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.Coins{}, nil // Empty coins
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
			withdrawCalled = true
			return "", &billingtypes.MsgWithdrawResponse{}, nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	s.withdraw(context.Background())

	assert.False(t, withdrawCalled, "WithdrawByProvider should not be called when nothing to withdraw")
}

func TestWithdrawScheduler_Withdraw_Success(t *testing.T) {
	var withdrawCalls int

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
			withdrawCalls++
			return "txhash123", &billingtypes.MsgWithdrawResponse{}, nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	s.withdraw(context.Background())

	assert.Equal(t, 1, withdrawCalls, "WithdrawByProvider called unexpected number of times")
}

func TestWithdrawScheduler_Withdraw_Error(t *testing.T) {
	var withdrawCalls int

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
			withdrawCalls++
			return "", nil, errors.New("withdrawal failed")
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	// Should not panic on error
	s.withdraw(context.Background())

	assert.Equal(t, 1, withdrawCalls, "WithdrawByProvider called unexpected number of times")
}

func TestWithdrawScheduler_Withdraw_RetryOnError(t *testing.T) {
	var getCalls int32

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			call := atomic.AddInt32(&getCalls, 1)
			if call < 3 {
				return nil, errors.New("transient error")
			}
			return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
			return "txhash", &billingtypes.MsgWithdrawResponse{}, nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.withdraw(ctx)

	assert.Equal(t, int32(3), atomic.LoadInt32(&getCalls), "GetProviderWithdrawable called unexpected number of times (want 2 retries + 1 success)")
}

// TestWithdrawScheduler_Withdraw_PaginatesUntilCursorEmpty verifies that
// withdraw() pages through the provider's leases, echoing each returned
// next_key back as the next request cursor, and stops when the cursor is empty.
func TestWithdrawScheduler_Withdraw_PaginatesUntilCursorEmpty(t *testing.T) {
	var gotKeys [][]byte

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
			gotKeys = append(gotKeys, key)
			page := len(gotKeys)
			if page < 3 {
				return fmt.Sprintf("tx%d", page), &billingtypes.MsgWithdrawResponse{
					NextKey: []byte(fmt.Sprintf("cursor-%d", page)),
					HasMore: true,
				}, nil
			}
			return "tx3", &billingtypes.MsgWithdrawResponse{}, nil // drained
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	err := s.withdraw(context.Background())
	require.NoError(t, err)

	require.Len(t, gotKeys, 3, "should page until the cursor is empty")
	assert.Nil(t, gotKeys[0], "first page must start from an empty cursor")
	assert.Equal(t, []byte("cursor-1"), gotKeys[1], "second page must echo the first next_key")
	assert.Equal(t, []byte("cursor-2"), gotKeys[2], "third page must echo the second next_key")
}

// TestWithdrawScheduler_Withdraw_StopsAtIterationBoundAndReportsIncomplete
// verifies the safety bound: if the cursor never empties, withdraw() stops
// after maxWithdrawIterations pages and increments the under-collection metric.
func TestWithdrawScheduler_Withdraw_StopsAtIterationBoundAndReportsIncomplete(t *testing.T) {
	var calls int

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
			calls++
			// Never drains: always returns a non-empty cursor.
			return "tx", &billingtypes.MsgWithdrawResponse{NextKey: []byte("more"), HasMore: true}, nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:          "test-uuid",
		WithdrawInterval:      time.Minute,
		MaxWithdrawIterations: 5,
	})

	before := promtestutil.ToFloat64(metrics.WithdrawIncompleteCyclesTotal)

	err := s.withdraw(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 5, calls, "must stop at the iteration bound")
	assert.Equal(t, before+1, promtestutil.ToFloat64(metrics.WithdrawIncompleteCyclesTotal),
		"incomplete-cycle metric must increment when the bound is hit with a non-empty cursor")
}

// TestWithdrawScheduler_Withdraw_ErrorMidPagination verifies a page error is
// fatal for the cycle: withdraw() returns the wrapped error and stops.
func TestWithdrawScheduler_Withdraw_ErrorMidPagination(t *testing.T) {
	var calls int

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
			calls++
			if calls == 2 {
				return "", nil, errors.New("page 2 boom")
			}
			return "tx1", &billingtypes.MsgWithdrawResponse{NextKey: []byte("cursor-1"), HasMore: true}, nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	err := s.withdraw(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "page 2")
	assert.Equal(t, 2, calls, "must stop paginating on the failing page")
}

// TestWithdrawScheduler_Withdraw_ContextCanceledMidPagination verifies the loop
// aborts promptly when the context is canceled between pages.
func TestWithdrawScheduler_Withdraw_ContextCanceledMidPagination(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls int

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
		},
		WithdrawByProviderFunc: func(_ context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
			calls++
			cancel() // cancel mid-cycle; a non-empty cursor would otherwise continue
			return "tx", &billingtypes.MsgWithdrawResponse{NextKey: []byte("more"), HasMore: true}, nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	err := s.withdraw(ctx)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls, "must stop paginating once the context is canceled")
}

// TestWithdrawScheduler_Withdraw_NilResponseIsError covers the defensive guard
// against a client returning (hash, nil, nil): the cycle must error rather than
// nil-deref the response.
func TestWithdrawScheduler_Withdraw_NilResponseIsError(t *testing.T) {
	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
			return "tx", nil, nil // misbehaving client: no error, no response
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	err := s.withdraw(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nil response")
}

func TestWithdrawScheduler_CheckCreditsAndClose_NoLeases(t *testing.T) {
	client := &mockChainClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{}, nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	nextCheck := s.checkCreditsAndClose(context.Background())

	// Should return default interval
	expectedMin := time.Now().Add(time.Minute - time.Second)
	expectedMax := time.Now().Add(time.Minute + time.Second)
	assert.True(t, !nextCheck.Before(expectedMin) && !nextCheck.After(expectedMax),
		"nextCheck = %v, want approximately now + 1 minute", nextCheck)
}

func TestWithdrawScheduler_CheckCreditsAndClose_DepletedCredit(t *testing.T) {
	var closeCalled bool
	var closedLeases []string

	// Generate a valid bech32 address
	tenant := generateTestAddress(t, "manifest")

	client := &mockChainClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: tenant, ProviderUuid: "test-uuid"},
				{Uuid: "lease-2", Tenant: tenant, ProviderUuid: "test-uuid"},
			}, nil
		},
		GetCreditAccountFunc: func(ctx context.Context, t string) (*billingtypes.CreditAccount, sdktypes.Coins, error) {
			// Return zero balance (depleted)
			return &billingtypes.CreditAccount{}, sdktypes.Coins{}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			closeCalled = true
			closedLeases = leaseUUIDs
			assert.Equal(t, "credit exhausted", reason)
			return uint64(len(leaseUUIDs)), []string{"txhash"}, nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	// ENG-591: a single zero-balance read must NOT close leases — it starts the
	// grace window (tolerationSeconds analogue) and defers. A brief chain-node lag
	// behind a tenant top-up would otherwise wrongfully soft-delete paying data.
	s.checkCreditsAndClose(context.Background())
	assert.False(t, closeCalled, "CloseLeases must not fire on a single unconfirmed zero read")

	s.mu.Lock()
	state := s.tenants[tenant]
	s.mu.Unlock()
	require.NotNil(t, state, "tenant state should record the first zero observation")
	require.False(t, state.firstZeroAt.IsZero(), "firstZeroAt must be stamped on the first zero read")

	// Once the zero balance has persisted beyond the grace period, closure fires.
	// Backdate the pending observation past the (default 5m) grace window.
	s.mu.Lock()
	state.firstZeroAt = time.Now().Add(-6 * time.Minute)
	s.mu.Unlock()

	s.checkCreditsAndClose(context.Background())
	assert.True(t, closeCalled, "CloseLeases must fire once zero persists past the grace period")
	assert.Len(t, closedLeases, 2)
}

// TestWithdrawScheduler_ZeroBalance_GraceDefersThenCloses drives applyCreditResults
// directly with a controlled clock to prove the grace-window boundary: a zero read
// inside the window defers (no close), a zero read at/after the window closes.
func TestWithdrawScheduler_ZeroBalance_GraceDefersThenCloses(t *testing.T) {
	tenant := generateTestAddress(t, "manifest")
	s := NewWithdrawScheduler(&mockChainClient{}, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})
	require.Equal(t, 5*time.Minute, s.creditCheckZeroGracePeriod, "default grace period should be 5m")

	tenantLeases := map[string][]string{tenant: {"lease-1", "lease-2"}}
	zero := map[string]creditResult{tenant: {balances: sdktypes.Coins{}}}

	t0 := time.Now()

	// First zero read: stamps firstZeroAt, defers.
	_, toClose := s.applyCreditResults(tenantLeases, zero, t0)
	assert.Empty(t, toClose, "first zero read must defer, not close")
	s.mu.Lock()
	require.NotNil(t, s.tenants[tenant])
	assert.True(t, s.tenants[tenant].firstZeroAt.Equal(t0), "firstZeroAt stamped at first observation")
	s.mu.Unlock()

	// Second zero read still inside the grace window: still defers, firstZeroAt unchanged.
	_, toClose = s.applyCreditResults(tenantLeases, zero, t0.Add(4*time.Minute))
	assert.Empty(t, toClose, "zero read within grace window must keep deferring")
	s.mu.Lock()
	assert.True(t, s.tenants[tenant].firstZeroAt.Equal(t0), "firstZeroAt must not advance while pending")
	s.mu.Unlock()

	// Zero read at/after the grace boundary: closes both leases and clears state.
	_, toClose = s.applyCreditResults(tenantLeases, zero, t0.Add(5*time.Minute))
	assert.ElementsMatch(t, []string{"lease-1", "lease-2"}, toClose, "sustained zero past grace must close")
	s.mu.Lock()
	_, stillTracked := s.tenants[tenant]
	s.mu.Unlock()
	assert.False(t, stillTracked, "closed tenant state must be cleared")
}

// TestWithdrawScheduler_ZeroBalance_RecoveryResetsGrace verifies hysteresis: a
// non-zero read during the grace window clears the pending zero, so a later zero
// starts a fresh window rather than inheriting the stale one.
func TestWithdrawScheduler_ZeroBalance_RecoveryResetsGrace(t *testing.T) {
	tenant := generateTestAddress(t, "manifest")
	s := NewWithdrawScheduler(&mockChainClient{}, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	tenantLeases := map[string][]string{tenant: {"lease-1"}}
	nonZero := map[string]creditResult{tenant: {balances: sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000)))}}
	zero := map[string]creditResult{tenant: {balances: sdktypes.Coins{}}}

	t0 := time.Now()

	// Zero read stamps the pending window.
	s.applyCreditResults(tenantLeases, zero, t0)
	s.mu.Lock()
	require.False(t, s.tenants[tenant].firstZeroAt.IsZero())
	s.mu.Unlock()

	// A non-zero read (top-up observed / node caught up) must clear the pending zero.
	s.applyCreditResults(tenantLeases, nonZero, t0.Add(1*time.Minute))
	s.mu.Lock()
	require.NotNil(t, s.tenants[tenant])
	assert.True(t, s.tenants[tenant].firstZeroAt.IsZero(), "non-zero read must reset the grace window")
	s.mu.Unlock()

	// A later zero read starts a FRESH window — must not close even though the
	// original observation was >grace ago.
	_, toClose := s.applyCreditResults(tenantLeases, zero, t0.Add(6*time.Minute))
	assert.Empty(t, toClose, "post-recovery zero must start a fresh grace window, not inherit the old one")
}

// TestWithdrawScheduler_ZeroBalance_RecoveryResetsBurnBaseline verifies that a
// non-zero read recovering from a pending zero window does NOT compute a burn rate
// across the zero plateau (which would be diluted/misleading and could schedule the
// next check too late). Recovery resets the baseline and skips estimation for that
// cycle. (ENG-591 / Copilot review.)
func TestWithdrawScheduler_ZeroBalance_RecoveryResetsBurnBaseline(t *testing.T) {
	tenant := generateTestAddress(t, "manifest")
	s := NewWithdrawScheduler(&mockChainClient{}, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})
	tenantLeases := map[string][]string{tenant: {"lease-1"}}
	high := map[string]creditResult{tenant: {balances: sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1_000_000)))}}
	zero := map[string]creditResult{tenant: {balances: sdktypes.Coins{}}}
	low := map[string]creditResult{tenant: {balances: sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(10)))}}

	t0 := time.Now()

	// Establish a normal non-zero baseline (X, at t0).
	s.applyCreditResults(tenantLeases, high, t0)
	// Dip to zero at t0+1m — burn-rate baseline (lastBalance/lastCheckTime) is NOT
	// updated by the zero path; the pending window opens.
	s.applyCreditResults(tenantLeases, zero, t0.Add(time.Minute))
	// Recover to a LOWER non-zero balance (Y<X) at t0+2m. A naive burn rate over
	// (t0 .. t0+2m) would be diluted by the zero gap.
	earliest, _ := s.applyCreditResults(tenantLeases, low, t0.Add(2*time.Minute))

	// No depletion estimate is scheduled from the recovery cycle (baseline reset).
	assert.True(t, earliest.IsZero(), "recovery from a zero window must not schedule a diluted depletion estimate")

	s.mu.Lock()
	state := s.tenants[tenant]
	s.mu.Unlock()
	require.NotNil(t, state)
	assert.True(t, state.firstZeroAt.IsZero(), "recovery clears the pending zero window")
	assert.True(t, state.lastCheckTime.Equal(t0.Add(2*time.Minute)), "recovery resets the burn baseline timestamp to now")
	assert.Equal(t, low[tenant].balances, state.lastBalance, "recovery resets the burn baseline balance to the recovered value")
}

// TestWithdrawScheduler_ZeroBalance_Integration_ClosesAfterGrace drives the full
// running scheduler (Start loop → withdrawAndCheckCredits → checkCreditsAndClose →
// CloseLeases) under synctest virtual time. With a sustained empty balance and the
// default 5m grace, closure must NOT fire during the grace window (the checks are
// deferred and re-scheduled) and must fire exactly once after it elapses. (ENG-591)
func TestWithdrawScheduler_ZeroBalance_Integration_ClosesAfterGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tenant := generateTestAddress(t, "manifest")
		var closeCalls int32
		var mu sync.Mutex
		var closedLeases []string
		var leasesClosed atomic.Bool // once closed, the chain no longer lists them

		client := &mockChainClient{
			// Nothing to withdraw — keep the cycle focused on the credit check.
			GetProviderWithdrawableFunc: func(_ context.Context, _ string) (sdktypes.Coins, error) {
				return sdktypes.Coins{}, nil
			},
			GetActiveLeasesByProviderFunc: func(_ context.Context, _ string) ([]billingtypes.Lease, error) {
				if leasesClosed.Load() {
					return []billingtypes.Lease{}, nil
				}
				return []billingtypes.Lease{
					{Uuid: "lease-1", Tenant: tenant, ProviderUuid: "test-uuid"},
					{Uuid: "lease-2", Tenant: tenant, ProviderUuid: "test-uuid"},
				}, nil
			},
			GetCreditAccountFunc: func(_ context.Context, _ string) (*billingtypes.CreditAccount, sdktypes.Coins, error) {
				return &billingtypes.CreditAccount{}, sdktypes.Coins{}, nil // always empty
			},
			CloseLeasesFunc: func(_ context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
				atomic.AddInt32(&closeCalls, 1)
				assert.Equal(t, "credit exhausted", reason)
				mu.Lock()
				closedLeases = append([]string(nil), leaseUUIDs...)
				mu.Unlock()
				leasesClosed.Store(true)
				return uint64(len(leaseUUIDs)), []string{"txhash"}, nil
			},
		}

		// Credit checks every 1m; default 5m zero-grace. Many checks land inside the
		// window, all of which must defer rather than close.
		s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
			ProviderUUID:        "test-uuid",
			WithdrawInterval:    time.Hour,
			CreditCheckInterval: time.Minute,
		})
		require.Equal(t, 5*time.Minute, s.creditCheckZeroGracePeriod)

		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = s.Start(ctx) }()

		// Within the grace window (first zero observed ~t0+1m, grace ends ~t0+6m):
		// closure must not have fired despite repeated empty reads.
		time.Sleep(4 * time.Minute)
		synctest.Wait()
		assert.Equal(t, int32(0), atomic.LoadInt32(&closeCalls),
			"a sustained-but-recent empty balance must not close leases inside the grace window")

		// Past the grace window: closure fires exactly once, for both leases.
		time.Sleep(4 * time.Minute) // total ~8m
		synctest.Wait()
		cancel()
		synctest.Wait()

		assert.Equal(t, int32(1), atomic.LoadInt32(&closeCalls),
			"closure must fire exactly once after the empty balance persists past the grace window")
		mu.Lock()
		assert.ElementsMatch(t, []string{"lease-1", "lease-2"}, closedLeases)
		mu.Unlock()
	})
}

// TestWithdrawScheduler_ZeroBalance_Integration_RecoveryPreventsClose proves the
// hysteresis end-to-end: a tenant reads empty, then a top-up is observed mid-window
// (the chain node caught up). Closure must never fire, even well past what would
// have been the original grace deadline, because the non-zero read reset the window.
func TestWithdrawScheduler_ZeroBalance_Integration_RecoveryPreventsClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tenant := generateTestAddress(t, "manifest")
		var closeCalls int32
		var recovered atomic.Bool

		client := &mockChainClient{
			GetProviderWithdrawableFunc: func(_ context.Context, _ string) (sdktypes.Coins, error) {
				return sdktypes.Coins{}, nil
			},
			GetActiveLeasesByProviderFunc: func(_ context.Context, _ string) ([]billingtypes.Lease, error) {
				return []billingtypes.Lease{{Uuid: "lease-1", Tenant: tenant, ProviderUuid: "test-uuid"}}, nil
			},
			GetCreditAccountFunc: func(_ context.Context, _ string) (*billingtypes.CreditAccount, sdktypes.Coins, error) {
				if recovered.Load() {
					// Node caught up / tenant topped up — healthy balance again.
					return &billingtypes.CreditAccount{}, sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1_000_000))), nil
				}
				return &billingtypes.CreditAccount{}, sdktypes.Coins{}, nil
			},
			CloseLeasesFunc: func(_ context.Context, leaseUUIDs []string, _ string) (uint64, []string, error) {
				atomic.AddInt32(&closeCalls, 1)
				return uint64(len(leaseUUIDs)), []string{"txhash"}, nil
			},
		}

		s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
			ProviderUUID:        "test-uuid",
			WithdrawInterval:    time.Hour,
			CreditCheckInterval: time.Minute,
		})

		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = s.Start(ctx) }()

		// Let a couple of empty reads stamp the pending window, then recover before
		// the grace elapses.
		time.Sleep(3 * time.Minute)
		synctest.Wait()
		recovered.Store(true)

		// Run well past the original grace deadline; closure must never fire.
		time.Sleep(10 * time.Minute)
		synctest.Wait()
		cancel()
		synctest.Wait()

		assert.Equal(t, int32(0), atomic.LoadInt32(&closeCalls),
			"a mid-window recovery must reset the grace window so closure never fires")
	})
}

func TestWithdrawScheduler_CheckCreditsAndClose_HealthyCredit(t *testing.T) {
	var closeCalled bool

	tenant := generateTestAddress(t, "manifest")

	client := &mockChainClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: tenant, ProviderUuid: "test-uuid"},
			}, nil
		},
		GetCreditAccountFunc: func(ctx context.Context, t string) (*billingtypes.CreditAccount, sdktypes.Coins, error) {
			// Return healthy balance
			return &billingtypes.CreditAccount{}, sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000000))), nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			closeCalled = true
			return 0, nil, nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	s.checkCreditsAndClose(context.Background())

	assert.False(t, closeCalled, "CloseLeases should not be called for healthy credit")
}

func TestWithdrawScheduler_EstimateDepletionTime(t *testing.T) {
	s := NewWithdrawScheduler(&mockChainClient{}, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	now := time.Now()

	tests := []struct {
		name         string
		prevBalance  sdktypes.Coins
		currBalance  sdktypes.Coins
		elapsed      time.Duration
		wantZero     bool
		minRemaining time.Duration
		maxRemaining time.Duration
	}{
		{
			name:        "no burn - balance same",
			prevBalance: sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))),
			currBalance: sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))),
			elapsed:     time.Minute,
			wantZero:    true,
		},
		{
			name:        "balance increased - deposit",
			prevBalance: sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))),
			currBalance: sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(2000))),
			elapsed:     time.Minute,
			wantZero:    true,
		},
		{
			name:         "steady burn rate",
			prevBalance:  sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))),
			currBalance:  sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(900))),
			elapsed:      time.Minute,
			wantZero:     false,
			minRemaining: 8 * time.Minute, // 900 / (100/60s) = 540s = 9min, give some buffer
			maxRemaining: 10 * time.Minute,
		},
		{
			name:        "zero elapsed time",
			prevBalance: sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))),
			currBalance: sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(900))),
			elapsed:     0,
			wantZero:    true,
		},
		// Multi-denom test cases
		{
			name: "multi-denom - umfx depletes first",
			prevBalance: sdktypes.NewCoins(
				sdktypes.NewCoin("umfx", sdkmath.NewInt(600)),   // Burns 100/min -> 6 min remaining
				sdktypes.NewCoin("uatom", sdkmath.NewInt(1000)), // Burns 50/min -> 20 min remaining
			),
			currBalance: sdktypes.NewCoins(
				sdktypes.NewCoin("umfx", sdkmath.NewInt(500)),  // Burned 100 in 1 min
				sdktypes.NewCoin("uatom", sdkmath.NewInt(950)), // Burned 50 in 1 min
			),
			elapsed:      time.Minute,
			wantZero:     false,
			minRemaining: 4 * time.Minute, // umfx: 500 / (100/60s) = 300s = 5min
			maxRemaining: 6 * time.Minute,
		},
		{
			name: "multi-denom - uatom depletes first",
			prevBalance: sdktypes.NewCoins(
				sdktypes.NewCoin("umfx", sdkmath.NewInt(10000)), // Burns 100/min -> 100 min remaining
				sdktypes.NewCoin("uatom", sdkmath.NewInt(500)),  // Burns 100/min -> 5 min remaining
			),
			currBalance: sdktypes.NewCoins(
				sdktypes.NewCoin("umfx", sdkmath.NewInt(9900)), // Burned 100 in 1 min
				sdktypes.NewCoin("uatom", sdkmath.NewInt(400)), // Burned 100 in 1 min
			),
			elapsed:      time.Minute,
			wantZero:     false,
			minRemaining: 3 * time.Minute, // uatom: 400 / (100/60s) = 240s = 4min
			maxRemaining: 5 * time.Minute,
		},
		{
			name: "multi-denom - one increases (deposit) one decreases",
			prevBalance: sdktypes.NewCoins(
				sdktypes.NewCoin("umfx", sdkmath.NewInt(500)),   // Will increase (deposit)
				sdktypes.NewCoin("uatom", sdkmath.NewInt(1000)), // Burns normally
			),
			currBalance: sdktypes.NewCoins(
				sdktypes.NewCoin("umfx", sdkmath.NewInt(1000)), // Increased - should be ignored
				sdktypes.NewCoin("uatom", sdkmath.NewInt(900)), // Burned 100 in 1 min
			),
			elapsed:      time.Minute,
			wantZero:     false,
			minRemaining: 8 * time.Minute, // uatom: 900 / (100/60s) = 540s = 9min
			maxRemaining: 10 * time.Minute,
		},
		{
			name: "multi-denom - both increase (deposits)",
			prevBalance: sdktypes.NewCoins(
				sdktypes.NewCoin("umfx", sdkmath.NewInt(500)),
				sdktypes.NewCoin("uatom", sdkmath.NewInt(500)),
			),
			currBalance: sdktypes.NewCoins(
				sdktypes.NewCoin("umfx", sdkmath.NewInt(1000)),
				sdktypes.NewCoin("uatom", sdkmath.NewInt(1000)),
			),
			elapsed:  time.Minute,
			wantZero: true, // Both increased, no depletion estimate
		},
		{
			name: "multi-denom - one empty, one burning",
			prevBalance: sdktypes.NewCoins(
				sdktypes.NewCoin("umfx", sdkmath.NewInt(0)),     // Empty - should be skipped
				sdktypes.NewCoin("uatom", sdkmath.NewInt(1000)), // Burns normally
			),
			currBalance: sdktypes.NewCoins(
				sdktypes.NewCoin("umfx", sdkmath.NewInt(0)),    // Still empty
				sdktypes.NewCoin("uatom", sdkmath.NewInt(900)), // Burned 100 in 1 min
			),
			elapsed:      time.Minute,
			wantZero:     false,
			minRemaining: 8 * time.Minute, // uatom: 900 / (100/60s) = 540s = 9min
			maxRemaining: 10 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			depletion := s.estimateDepletionTime(tt.prevBalance, tt.currBalance, tt.elapsed, now)

			if tt.wantZero {
				assert.True(t, depletion.IsZero(), "estimateDepletionTime() = %v, want zero time", depletion)
				return
			}

			require.False(t, depletion.IsZero(), "estimateDepletionTime() returned zero time, want non-zero")

			remaining := depletion.Sub(now)
			assert.True(t, remaining >= tt.minRemaining && remaining <= tt.maxRemaining,
				"estimated remaining = %v, want between %v and %v", remaining, tt.minRemaining, tt.maxRemaining)
		})
	}
}

func TestWithdrawScheduler_TriggerWithdraw(t *testing.T) {
	var withdrawCalled int32
	var mu sync.Mutex
	var wg sync.WaitGroup

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
			mu.Lock()
			atomic.AddInt32(&withdrawCalled, 1)
			mu.Unlock()
			wg.Done()
			return "txhash", &billingtypes.MsgWithdrawResponse{}, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{}, nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	// Set context (normally done in Start)
	s.ctx = context.Background()

	wg.Add(1)
	s.TriggerWithdraw()

	// Wait for the goroutine to complete
	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&withdrawCalled))
}

func TestWithdrawScheduler_WithdrawOnce(t *testing.T) {
	var withdrawCalled bool
	var creditCheckCalled bool

	tenant := generateTestAddress(t, "manifest")

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
			withdrawCalled = true
			return "txhash", &billingtypes.MsgWithdrawResponse{}, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			creditCheckCalled = true
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: tenant, ProviderUuid: "test-uuid"},
			}, nil
		},
		GetCreditAccountFunc: func(ctx context.Context, t string) (*billingtypes.CreditAccount, sdktypes.Coins, error) {
			return &billingtypes.CreditAccount{}, sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000000))), nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	s.WithdrawOnce(context.Background())

	assert.True(t, withdrawCalled, "WithdrawByProvider was not called")
	assert.True(t, creditCheckCalled, "GetActiveLeasesByProvider was not called (credit check)")
}

func TestWithdrawScheduler_Start_ContextCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &mockChainClient{
			GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
				return sdktypes.Coins{}, nil
			},
			GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
				return []billingtypes.Lease{}, nil
			},
		}

		s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
			ProviderUUID:     "test-uuid",
			WithdrawInterval: 100 * time.Millisecond,
		})

		ctx, cancel := context.WithCancel(context.Background())

		errCh := make(chan error, 1)
		go func() {
			errCh <- s.Start(ctx)
		}()

		// Cancel context after a short delay (virtualized time advances instantly)
		time.Sleep(50 * time.Millisecond)
		cancel()

		// Wait for goroutines to process the cancellation
		synctest.Wait()

		// Should return context.Canceled
		select {
		case err := <-errCh:
			assert.ErrorIs(t, err, context.Canceled)
		default:
			t.Error("Start() did not return after context cancellation")
		}
	})
}

// TestWithdrawScheduler_GuardThrottlesWithdraw verifies the decoupled-cadence
// guard: with CreditCheckInterval < WithdrawInterval, the scheduler wakes
// every creditCheckInterval to run a credit check, but only issues a paid
// withdraw once per withdrawInterval.
func TestWithdrawScheduler_GuardThrottlesWithdraw(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var withdrawCalls, creditChecks int32
		client := &mockChainClient{
			GetProviderWithdrawableFunc: func(ctx context.Context, _ string) (sdktypes.Coins, error) {
				return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
			},
			WithdrawByProviderFunc: func(ctx context.Context, _ string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
				atomic.AddInt32(&withdrawCalls, 1)
				return "txhash", &billingtypes.MsgWithdrawResponse{}, nil // empty NextKey => full drain
			},
			GetActiveLeasesByProviderFunc: func(ctx context.Context, _ string) ([]billingtypes.Lease, error) {
				atomic.AddInt32(&creditChecks, 1) // credit check runs on EVERY wake
				return []billingtypes.Lease{}, nil
			},
		}
		// Deterministic under synctest: the bubble clock starts at 2000-01-01 and
		// lastWithdrawTime is the zero value, so the first wake (t0+1h) withdraws
		// and stamps; every subsequent 1h wake sees time.Since(last) < 24h and skips.
		s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
			ProviderUUID:        "test-uuid",
			WithdrawInterval:    24 * time.Hour,
			CreditCheckInterval: 1 * time.Hour,
		})
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = s.Start(ctx) }()

		time.Sleep(24*time.Hour + time.Minute) // ~24 credit-check wakes of virtual time
		synctest.Wait()
		cancel()
		synctest.Wait()

		assert.Equal(t, int32(1), atomic.LoadInt32(&withdrawCalls),
			"expected exactly one paid withdraw across 24h of 1h wakes")
		assert.GreaterOrEqual(t, atomic.LoadInt32(&creditChecks), int32(24),
			"credit check must run on every wake, not just when the withdraw fires")
	})
}

// TestWithdrawScheduler_GuardInertByDefault verifies backward compatibility:
// when CreditCheckInterval is left unset, it defaults to WithdrawInterval, so
// the two cadences are equal and the guard is inert (guardActive == false).
// Every wake performs a paid withdraw, matching pre-ENG-524 behavior.
func TestWithdrawScheduler_GuardInertByDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var withdrawCalls int32
		client := &mockChainClient{
			GetProviderWithdrawableFunc: func(ctx context.Context, _ string) (sdktypes.Coins, error) {
				return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
			},
			WithdrawByProviderFunc: func(ctx context.Context, _ string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
				atomic.AddInt32(&withdrawCalls, 1)
				return "txhash", &billingtypes.MsgWithdrawResponse{}, nil
			},
			GetActiveLeasesByProviderFunc: func(ctx context.Context, _ string) ([]billingtypes.Lease, error) {
				return []billingtypes.Lease{}, nil
			},
		}
		// CreditCheckInterval unset => equals WithdrawInterval => guard inert.
		s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
			ProviderUUID:     "test-uuid",
			WithdrawInterval: 1 * time.Hour,
		})
		require.False(t, s.guardActive)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = s.Start(ctx) }()
		time.Sleep(3*time.Hour + time.Minute)
		synctest.Wait()
		cancel()
		synctest.Wait()
		// Every wake withdraws (today's behavior): ~3 over 3h.
		assert.GreaterOrEqual(t, atomic.LoadInt32(&withdrawCalls), int32(3))
	})
}

// TestWithdrawScheduler_IdleProviderThrottled pins the IsZero stamp in
// withdraw(): even when there is nothing to withdraw, withdraw() stamps
// lastWithdrawTime so the guard still throttles the free withdrawable-check
// to once per withdrawInterval, instead of re-probing on every credit-check
// wake.
func TestWithdrawScheduler_IdleProviderThrottled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var getCalls int32
		client := &mockChainClient{
			GetProviderWithdrawableFunc: func(ctx context.Context, _ string) (sdktypes.Coins, error) {
				atomic.AddInt32(&getCalls, 1)
				return sdktypes.Coins{}, nil // nothing to withdraw
			},
			GetActiveLeasesByProviderFunc: func(ctx context.Context, _ string) ([]billingtypes.Lease, error) {
				return []billingtypes.Lease{}, nil
			},
		}
		s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
			ProviderUUID:        "test-uuid",
			WithdrawInterval:    24 * time.Hour,
			CreditCheckInterval: 1 * time.Hour,
		})
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = s.Start(ctx) }()
		time.Sleep(24*time.Hour + time.Minute)
		synctest.Wait()
		cancel()
		synctest.Wait()
		// Without the IsZero stamp this would be ~24 (a free query every tick).
		assert.Equal(t, int32(1), atomic.LoadInt32(&getCalls),
			"idle provider must be throttled to one withdrawable-check per withdraw_interval")
	})
}

// TestWithdrawScheduler_GuardReArmsNextWindow proves the guard's stamp resets
// each window rather than latching forever: a first withdraw fires and stamps
// at t0+1h, then the guard suppresses every 1h wake until t0+25h, when
// time.Since(lastWithdrawTime) == withdrawInterval (24h) and a second withdraw
// fires.
func TestWithdrawScheduler_GuardReArmsNextWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var withdrawCalls int32
		client := &mockChainClient{
			GetProviderWithdrawableFunc: func(ctx context.Context, _ string) (sdktypes.Coins, error) {
				return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
			},
			WithdrawByProviderFunc: func(ctx context.Context, _ string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
				atomic.AddInt32(&withdrawCalls, 1)
				return "txhash", &billingtypes.MsgWithdrawResponse{}, nil
			},
			GetActiveLeasesByProviderFunc: func(ctx context.Context, _ string) ([]billingtypes.Lease, error) {
				return []billingtypes.Lease{}, nil
			},
		}
		s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
			ProviderUUID:        "test-uuid",
			WithdrawInterval:    24 * time.Hour,
			CreditCheckInterval: 1 * time.Hour,
		})
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = s.Start(ctx) }()
		// First withdraw at t0+1h (stamp), second at t0+25h (since==24h). Sleep to t0+25h+.
		time.Sleep(25*time.Hour + time.Minute)
		synctest.Wait()
		cancel()
		synctest.Wait()
		assert.Equal(t, int32(2), atomic.LoadInt32(&withdrawCalls),
			"guard must re-arm: a second withdraw fires one withdraw_interval after the first")
	})
}

// TestWithdrawScheduler_TriggerWithdrawBypassesGuard verifies that the force
// path (TriggerWithdraw) always withdraws, even when the cadence guard is
// active and lastWithdrawTime was just stamped — the whole point of an
// on-demand trigger is an immediate withdraw. Uses the real-time
// WaitGroup-signalled pattern (no synctest bubble), mirroring
// TestWithdrawScheduler_TriggerWithdraw.
func TestWithdrawScheduler_TriggerWithdrawBypassesGuard(t *testing.T) {
	var withdrawCalled int32
	var mu sync.Mutex
	var wg sync.WaitGroup

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
			mu.Lock()
			atomic.AddInt32(&withdrawCalled, 1)
			mu.Unlock()
			wg.Done()
			return "txhash", &billingtypes.MsgWithdrawResponse{}, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{}, nil
		},
	}

	// Guard active: CreditCheckInterval < WithdrawInterval.
	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:        "test-uuid",
		WithdrawInterval:    time.Hour,
		CreditCheckInterval: time.Minute,
	})
	require.True(t, s.guardActive, "guard must be active for this test to be meaningful")

	// Set context (normally done in Start)
	s.ctx = context.Background()

	// Seed a fresh stamp: under the periodic path this would suppress a
	// withdraw for the next hour. The force path must ignore it.
	s.mu.Lock()
	s.lastWithdrawTime = time.Now()
	s.mu.Unlock()

	wg.Add(1)
	s.TriggerWithdraw()

	// Wait for the goroutine to complete
	wg.Wait()

	assert.Equal(t, int32(1), atomic.LoadInt32(&withdrawCalled),
		"force path must withdraw despite a fresh lastWithdrawTime under an active guard")
}

// TestWithdrawScheduler_GuardConcurrentWriters exercises the periodic loop's
// lastWithdrawTime writer (Start's ticks) racing against the force path's
// writer (TriggerWithdraw) on the same field, guarded by s.mu. Run under
// `go test -race`: the point of this test is that the race detector reports
// nothing and the run does not deadlock, not any particular call count.
func TestWithdrawScheduler_GuardConcurrentWriters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &mockChainClient{
			GetProviderWithdrawableFunc: func(ctx context.Context, _ string) (sdktypes.Coins, error) {
				return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
			},
			WithdrawByProviderFunc: func(ctx context.Context, _ string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
				return "txhash", &billingtypes.MsgWithdrawResponse{}, nil
			},
			GetActiveLeasesByProviderFunc: func(ctx context.Context, _ string) ([]billingtypes.Lease, error) {
				return []billingtypes.Lease{}, nil
			},
		}
		s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
			ProviderUUID:        "test-uuid",
			WithdrawInterval:    1 * time.Hour,
			CreditCheckInterval: 10 * time.Minute,
		})
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = s.Start(ctx) }() // periodic loop stamps lastWithdrawTime on full drain
		// Fire the force path repeatedly across the run so a TriggerWithdraw
		// goroutine's stamp genuinely overlaps the periodic loop's stamps.
		for i := 0; i < 5; i++ {
			s.TriggerWithdraw()
			time.Sleep(30 * time.Minute)
		}
		synctest.Wait()
		cancel()
		synctest.Wait()
		// No functional assertion: the point is that `go test -race` reports no data
		// race on lastWithdrawTime and the run does not deadlock.
	})
}

func TestWithdrawScheduler_ConsecutiveErrors(t *testing.T) {
	errorCount := 0
	tenant := generateTestAddress(t, "manifest")

	client := &mockChainClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: tenant, ProviderUuid: "test-uuid"},
			}, nil
		},
		GetCreditAccountFunc: func(ctx context.Context, t string) (*billingtypes.CreditAccount, sdktypes.Coins, error) {
			errorCount++
			return nil, nil, errors.New("network error")
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:              "test-uuid",
		WithdrawInterval:          time.Minute,
		CreditCheckErrorThreshold: 2,
		CreditCheckRetryInterval:  10 * time.Second,
	})

	// First check - 1 error
	s.checkCreditsAndClose(context.Background())

	s.mu.Lock()
	state := s.tenants[tenant]
	s.mu.Unlock()

	require.NotNil(t, state, "tenant state not created")
	assert.Equal(t, 1, state.consecutiveErrs)

	// Second check - 2 errors, exceeds threshold
	nextCheck := s.checkCreditsAndClose(context.Background())

	s.mu.Lock()
	state = s.tenants[tenant]
	s.mu.Unlock()

	assert.Equal(t, 2, state.consecutiveErrs)

	// Next check should be scheduled earlier due to errors
	expectedEarliest := time.Now().Add(10 * time.Second)
	assert.False(t, nextCheck.After(expectedEarliest.Add(time.Second)),
		"nextCheck = %v, should be before %v due to error threshold", nextCheck, expectedEarliest)
}

// generateTestAddress creates a valid bech32 address for testing.
func generateTestAddress(t *testing.T, prefix string) string {
	t.Helper()
	// Use a fixed 20-byte value for deterministic tests
	addrBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	addr, err := bech32.ConvertAndEncode(prefix, addrBytes)
	require.NoError(t, err, "failed to generate test address")
	return addr
}

// generateTestAddressN creates a unique valid bech32 address for testing.
func generateTestAddressN(t *testing.T, prefix string, n int) string {
	t.Helper()
	addrBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, byte(n)}
	addr, err := bech32.ConvertAndEncode(prefix, addrBytes)
	require.NoError(t, err, "failed to generate test address")
	return addr
}

func TestWithdrawScheduler_CheckCreditsAndClose_ContextCancellation(t *testing.T) {
	// Test that checkCreditsAndClose respects context cancellation during iteration
	ctx, cancel := context.WithCancel(context.Background())
	var getCreditCallCount int32

	// Generate multiple test tenants
	tenant1 := generateTestAddressN(t, "manifest", 1)
	tenant2 := generateTestAddressN(t, "manifest", 2)
	tenant3 := generateTestAddressN(t, "manifest", 3)

	client := &mockChainClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: tenant1, ProviderUuid: "test-uuid"},
				{Uuid: "lease-2", Tenant: tenant2, ProviderUuid: "test-uuid"},
				{Uuid: "lease-3", Tenant: tenant3, ProviderUuid: "test-uuid"},
			}, nil
		},
		GetCreditAccountFunc: func(ctx context.Context, tenant string) (*billingtypes.CreditAccount, sdktypes.Coins, error) {
			count := atomic.AddInt32(&getCreditCallCount, 1)
			// Cancel after first credit check
			if count == 1 {
				cancel()
			}
			// Check if context is cancelled
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			return &billingtypes.CreditAccount{}, sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000000))), nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	// Should return early when context is cancelled
	s.checkCreditsAndClose(ctx)

	// Verify we stopped iterating when context was cancelled
	// Due to map iteration order being non-deterministic, we may get 1-2 calls
	finalCount := atomic.LoadInt32(&getCreditCallCount)
	assert.LessOrEqual(t, finalCount, int32(2),
		"expected at most 2 GetCreditAccount calls before cancellation, got %d", finalCount)
}

func TestWithdrawScheduler_CheckCreditsAndClose_CloseLeasesContextCheck(t *testing.T) {
	// Test that CloseLeases is not called when context is cancelled
	ctx, cancel := context.WithCancel(context.Background())
	var closeCalled bool

	tenant := generateTestAddress(t, "manifest")

	client := &mockChainClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{
				{Uuid: "lease-1", Tenant: tenant, ProviderUuid: "test-uuid"},
			}, nil
		},
		GetCreditAccountFunc: func(ctx context.Context, t string) (*billingtypes.CreditAccount, sdktypes.Coins, error) {
			// Cancel context after checking credit
			cancel()
			// Return depleted balance (normally would trigger CloseLeases)
			return &billingtypes.CreditAccount{}, sdktypes.Coins{}, nil
		},
		CloseLeasesFunc: func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
			closeCalled = true
			return uint64(len(leaseUUIDs)), []string{"txhash"}, nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	s.checkCreditsAndClose(ctx)

	// CloseLeases should NOT be called because context was cancelled
	assert.False(t, closeCalled, "CloseLeases should not be called when context is cancelled")
}

func TestWithdrawScheduler_EstimateDepletionTime_HighPrecision(t *testing.T) {
	// Test that burn rate calculation handles various time intervals correctly
	s := NewWithdrawScheduler(&mockChainClient{}, WithdrawSchedulerConfig{
		ProviderUUID:     "test-uuid",
		WithdrawInterval: time.Minute,
	})

	now := time.Now()

	tests := []struct {
		name         string
		prevBalance  sdktypes.Coins
		currBalance  sdktypes.Coins
		elapsed      time.Duration
		wantZero     bool
		minRemaining time.Duration
		maxRemaining time.Duration
	}{
		{
			name:         "short interval - 10 seconds",
			prevBalance:  sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))),
			currBalance:  sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(990))),
			elapsed:      10 * time.Second,
			wantZero:     false,
			minRemaining: 15 * time.Minute, // 990 / (10/10s) = 990s = ~16.5min
			maxRemaining: 18 * time.Minute,
		},
		{
			name:         "long interval - 10 minutes",
			prevBalance:  sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(10000))),
			currBalance:  sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(9000))),
			elapsed:      10 * time.Minute,
			wantZero:     false,
			minRemaining: 85 * time.Minute, // 9000 / (1000/600s) = 5400s = 90min
			maxRemaining: 95 * time.Minute,
		},
		{
			name:        "very small burn - micro amount",
			prevBalance: sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000000))),
			currBalance: sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(999999))), // Burned 1 in 1 min
			elapsed:     time.Minute,
			wantZero:    false,
			// burn rate = 1 per 60 seconds = 1/60 per second
			// time remaining = 999999 / (1/60) seconds = 999999 * 60 seconds = ~694 days
			minRemaining: 690 * 24 * time.Hour,
			maxRemaining: 700 * 24 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			depletion := s.estimateDepletionTime(tt.prevBalance, tt.currBalance, tt.elapsed, now)

			if tt.wantZero {
				assert.True(t, depletion.IsZero(), "estimateDepletionTime() = %v, want zero time", depletion)
				return
			}

			require.False(t, depletion.IsZero(), "estimateDepletionTime() returned zero time, want non-zero")

			remaining := depletion.Sub(now)
			assert.True(t, remaining >= tt.minRemaining && remaining <= tt.maxRemaining,
				"estimated remaining = %v, want between %v and %v", remaining, tt.minRemaining, tt.maxRemaining)
		})
	}
}

// TestTriggerWithdraw_PanicDoesNotCrashFred pins the panic-recovery
// invariant for the TriggerWithdraw-spawned goroutine. A panic inside
// a chain RPC (here, WithdrawByProvider) must be recovered instead of
// propagating up and killing the fred process. Subsequent triggers
// must still work.
func TestTriggerWithdraw_PanicDoesNotCrashFred(t *testing.T) {
	var callCount atomic.Int32
	client := &mockChainClient{
		// Report a non-zero withdrawable so withdraw() proceeds past
		// the "nothing to withdraw" short-circuit and reaches the
		// panicking WithdrawByProvider call.
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.NewCoins(sdktypes.NewCoin("utoken", sdkmath.NewInt(100))), nil
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error) {
			count := callCount.Add(1)
			if count == 1 {
				panic("synthetic chain client panic")
			}
			return "tx-hash", &billingtypes.MsgWithdrawResponse{}, nil
		},
	}

	scheduler := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID:     "test-provider",
		WithdrawInterval: time.Hour, // long so only triggers fire
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start must be running for TriggerWithdraw to spawn goroutines.
	startDone := make(chan struct{})
	go func() {
		_ = scheduler.Start(ctx)
		close(startDone)
	}()
	t.Cleanup(func() {
		scheduler.Stop()
		<-startDone
	})

	before := promtestutil.ToFloat64(metrics.GoroutinePanicsTotal.WithLabelValues("withdraw_scheduler"))

	// First trigger — will panic inside WithdrawByProvider. Must not
	// crash fred. The spawned goroutine recovers, logs, bumps metric.
	scheduler.TriggerWithdraw()

	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(metrics.GoroutinePanicsTotal.WithLabelValues("withdraw_scheduler")) == before+1
	}, 2*time.Second, 10*time.Millisecond,
		"GoroutinePanicsTotal{withdraw_scheduler} must increment after panic recovery")

	// Second trigger — must still work. The scheduler itself isn't
	// poisoned by the recovered panic.
	scheduler.TriggerWithdraw()

	require.Eventually(t, func() bool {
		return callCount.Load() >= 2
	}, 2*time.Second, 10*time.Millisecond,
		"second TriggerWithdraw must proceed to WithdrawByProvider — scheduler must still be functional")
}
