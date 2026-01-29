package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	sdkmath "cosmossdk.io/math"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/bech32"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

// mockChainClient implements ChainClient interface for testing.
type mockChainClient struct {
	GetProviderWithdrawableFunc   func(ctx context.Context, providerUUID string) (sdktypes.Coins, error)
	WithdrawByProviderFunc        func(ctx context.Context, providerUUID string) (string, error)
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

func (m *mockChainClient) WithdrawByProvider(ctx context.Context, providerUUID string) (string, error) {
	if m.WithdrawByProviderFunc != nil {
		return m.WithdrawByProviderFunc(ctx, providerUUID)
	}
	return "", nil
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
				ProviderUUID: "test-uuid",
				Interval:     time.Minute,
			},
			wantMaxIterations:  100,
			wantErrorThreshold: 3,
			wantRetryInterval:  30 * time.Second,
		},
		{
			name: "with custom values",
			cfg: WithdrawSchedulerConfig{
				ProviderUUID:              "test-uuid",
				Interval:                  5 * time.Minute,
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
				Interval:                  time.Minute,
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

			if s == nil {
				t.Fatal("NewWithdrawScheduler() returned nil")
			}
			if s.providerUUID != tt.cfg.ProviderUUID {
				t.Errorf("providerUUID = %q, want %q", s.providerUUID, tt.cfg.ProviderUUID)
			}
			if s.interval != tt.cfg.Interval {
				t.Errorf("interval = %v, want %v", s.interval, tt.cfg.Interval)
			}
			if s.maxWithdrawIterations != tt.wantMaxIterations {
				t.Errorf("maxWithdrawIterations = %d, want %d", s.maxWithdrawIterations, tt.wantMaxIterations)
			}
			if s.creditCheckErrorThreshold != tt.wantErrorThreshold {
				t.Errorf("creditCheckErrorThreshold = %d, want %d", s.creditCheckErrorThreshold, tt.wantErrorThreshold)
			}
			if s.creditCheckRetryInterval != tt.wantRetryInterval {
				t.Errorf("creditCheckRetryInterval = %v, want %v", s.creditCheckRetryInterval, tt.wantRetryInterval)
			}
			if s.tenants == nil {
				t.Error("tenants map is nil")
			}
		})
	}
}

func TestWithdrawScheduler_Withdraw_NothingToWithdraw(t *testing.T) {
	var withdrawCalled bool

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.Coins{}, nil // Empty coins
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string) (string, error) {
			withdrawCalled = true
			return "", nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID: "test-uuid",
		Interval:     time.Minute,
	})

	s.withdraw(context.Background())

	if withdrawCalled {
		t.Error("WithdrawByProvider should not be called when nothing to withdraw")
	}
}

func TestWithdrawScheduler_Withdraw_Success(t *testing.T) {
	var withdrawCalls int

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string) (string, error) {
			withdrawCalls++
			return "txhash123", nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID: "test-uuid",
		Interval:     time.Minute,
	})

	s.withdraw(context.Background())

	if withdrawCalls != 1 {
		t.Errorf("WithdrawByProvider called %d times, want 1", withdrawCalls)
	}
}

func TestWithdrawScheduler_Withdraw_Error(t *testing.T) {
	var withdrawCalls int

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string) (string, error) {
			withdrawCalls++
			return "", errors.New("withdrawal failed")
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID: "test-uuid",
		Interval:     time.Minute,
	})

	// Should not panic on error
	s.withdraw(context.Background())

	if withdrawCalls != 1 {
		t.Errorf("WithdrawByProvider called %d times, want 1", withdrawCalls)
	}
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
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string) (string, error) {
			return "txhash", nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID: "test-uuid",
		Interval:     time.Minute,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.withdraw(ctx)

	if atomic.LoadInt32(&getCalls) != 3 {
		t.Errorf("GetProviderWithdrawable called %d times, want 3 (2 retries + 1 success)", getCalls)
	}
}

func TestWithdrawScheduler_CheckCreditsAndClose_NoLeases(t *testing.T) {
	client := &mockChainClient{
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{}, nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID: "test-uuid",
		Interval:     time.Minute,
	})

	nextCheck := s.checkCreditsAndClose(context.Background())

	// Should return default interval
	expectedMin := time.Now().Add(time.Minute - time.Second)
	expectedMax := time.Now().Add(time.Minute + time.Second)
	if nextCheck.Before(expectedMin) || nextCheck.After(expectedMax) {
		t.Errorf("nextCheck = %v, want approximately now + 1 minute", nextCheck)
	}
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
			if reason != "credit exhausted" {
				t.Errorf("close reason = %q, want %q", reason, "credit exhausted")
			}
			return uint64(len(leaseUUIDs)), []string{"txhash"}, nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID: "test-uuid",
		Interval:     time.Minute,
	})

	s.checkCreditsAndClose(context.Background())

	if !closeCalled {
		t.Error("CloseLeases was not called for depleted credit")
	}
	if len(closedLeases) != 2 {
		t.Errorf("closedLeases = %d, want 2", len(closedLeases))
	}
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
		ProviderUUID: "test-uuid",
		Interval:     time.Minute,
	})

	s.checkCreditsAndClose(context.Background())

	if closeCalled {
		t.Error("CloseLeases should not be called for healthy credit")
	}
}

func TestWithdrawScheduler_EstimateDepletionTime(t *testing.T) {
	s := NewWithdrawScheduler(&mockChainClient{}, WithdrawSchedulerConfig{
		ProviderUUID: "test-uuid",
		Interval:     time.Minute,
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
				if !depletion.IsZero() {
					t.Errorf("estimateDepletionTime() = %v, want zero time", depletion)
				}
				return
			}

			if depletion.IsZero() {
				t.Fatal("estimateDepletionTime() returned zero time, want non-zero")
			}

			remaining := depletion.Sub(now)
			if remaining < tt.minRemaining || remaining > tt.maxRemaining {
				t.Errorf("estimated remaining = %v, want between %v and %v", remaining, tt.minRemaining, tt.maxRemaining)
			}
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
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string) (string, error) {
			mu.Lock()
			atomic.AddInt32(&withdrawCalled, 1)
			mu.Unlock()
			wg.Done()
			return "txhash", nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{}, nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID: "test-uuid",
		Interval:     time.Minute,
	})

	// Set context (normally done in Start)
	s.ctx = context.Background()

	wg.Add(1)
	s.TriggerWithdraw()

	// Wait for the goroutine to complete
	wg.Wait()

	if atomic.LoadInt32(&withdrawCalled) != 1 {
		t.Errorf("WithdrawByProvider called %d times, want 1", withdrawCalled)
	}
}

func TestWithdrawScheduler_WithdrawOnce(t *testing.T) {
	var withdrawCalled bool
	var creditCheckCalled bool

	tenant := generateTestAddress(t, "manifest")

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			return sdktypes.NewCoins(sdktypes.NewCoin("umfx", sdkmath.NewInt(1000))), nil
		},
		WithdrawByProviderFunc: func(ctx context.Context, providerUUID string) (string, error) {
			withdrawCalled = true
			return "txhash", nil
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
		ProviderUUID: "test-uuid",
		Interval:     time.Minute,
	})

	s.WithdrawOnce(context.Background())

	if !withdrawCalled {
		t.Error("WithdrawByProvider was not called")
	}
	if !creditCheckCalled {
		t.Error("GetActiveLeasesByProvider was not called (credit check)")
	}
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
			ProviderUUID: "test-uuid",
			Interval:     100 * time.Millisecond,
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
			if err != context.Canceled {
				t.Errorf("Start() returned %v, want context.Canceled", err)
			}
		default:
			t.Error("Start() did not return after context cancellation")
		}
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
		Interval:                  time.Minute,
		CreditCheckErrorThreshold: 2,
		CreditCheckRetryInterval:  10 * time.Second,
	})

	// First check - 1 error
	s.checkCreditsAndClose(context.Background())

	s.mu.Lock()
	state := s.tenants[tenant]
	s.mu.Unlock()

	if state == nil {
		t.Fatal("tenant state not created")
	}
	if state.consecutiveErrs != 1 {
		t.Errorf("consecutiveErrs = %d, want 1", state.consecutiveErrs)
	}

	// Second check - 2 errors, exceeds threshold
	nextCheck := s.checkCreditsAndClose(context.Background())

	s.mu.Lock()
	state = s.tenants[tenant]
	s.mu.Unlock()

	if state.consecutiveErrs != 2 {
		t.Errorf("consecutiveErrs = %d, want 2", state.consecutiveErrs)
	}

	// Next check should be scheduled earlier due to errors
	expectedEarliest := time.Now().Add(10 * time.Second)
	if nextCheck.After(expectedEarliest.Add(time.Second)) {
		t.Errorf("nextCheck = %v, should be before %v due to error threshold", nextCheck, expectedEarliest)
	}
}

// generateTestAddress creates a valid bech32 address for testing.
func generateTestAddress(t *testing.T, prefix string) string {
	t.Helper()
	// Use a fixed 20-byte value for deterministic tests
	addrBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	addr, err := bech32.ConvertAndEncode(prefix, addrBytes)
	if err != nil {
		t.Fatalf("failed to generate test address: %v", err)
	}
	return addr
}

// generateTestAddressN creates a unique valid bech32 address for testing.
func generateTestAddressN(t *testing.T, prefix string, n int) string {
	t.Helper()
	addrBytes := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, byte(n)}
	addr, err := bech32.ConvertAndEncode(prefix, addrBytes)
	if err != nil {
		t.Fatalf("failed to generate test address: %v", err)
	}
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
		ProviderUUID: "test-uuid",
		Interval:     time.Minute,
	})

	// Should return early when context is cancelled
	s.checkCreditsAndClose(ctx)

	// Verify we stopped iterating when context was cancelled
	// Due to map iteration order being non-deterministic, we may get 1-2 calls
	finalCount := atomic.LoadInt32(&getCreditCallCount)
	if finalCount > 2 {
		t.Errorf("expected at most 2 GetCreditAccount calls before cancellation, got %d", finalCount)
	}
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
		ProviderUUID: "test-uuid",
		Interval:     time.Minute,
	})

	s.checkCreditsAndClose(ctx)

	// CloseLeases should NOT be called because context was cancelled
	if closeCalled {
		t.Error("CloseLeases should not be called when context is cancelled")
	}
}

func TestWithdrawScheduler_EstimateDepletionTime_HighPrecision(t *testing.T) {
	// Test that burn rate calculation handles various time intervals correctly
	s := NewWithdrawScheduler(&mockChainClient{}, WithdrawSchedulerConfig{
		ProviderUUID: "test-uuid",
		Interval:     time.Minute,
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
				if !depletion.IsZero() {
					t.Errorf("estimateDepletionTime() = %v, want zero time", depletion)
				}
				return
			}

			if depletion.IsZero() {
				t.Fatal("estimateDepletionTime() returned zero time, want non-zero")
			}

			remaining := depletion.Sub(now)
			if remaining < tt.minRemaining || remaining > tt.maxRemaining {
				t.Errorf("estimated remaining = %v, want between %v and %v", remaining, tt.minRemaining, tt.maxRemaining)
			}
		})
	}
}
