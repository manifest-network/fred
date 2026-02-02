package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	sdktypes "github.com/cosmos/cosmos-sdk/types"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

func TestWithdrawScheduler_StopStopsStart(t *testing.T) {
	var withdrawCalled int32

	client := &mockChainClient{
		GetProviderWithdrawableFunc: func(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
			atomic.AddInt32(&withdrawCalled, 1)
			return sdktypes.Coins{}, nil
		},
		GetActiveLeasesByProviderFunc: func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
			return []billingtypes.Lease{}, nil
		},
	}

	s := NewWithdrawScheduler(client, WithdrawSchedulerConfig{
		ProviderUUID: "test-uuid",
		Interval:     50 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start(ctx)
	}()

	// Wait for at least one cycle
	time.Sleep(100 * time.Millisecond)

	// Call Stop() - should stop Start()
	s.Stop()

	// Wait for Start() to return
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Start() returned error: %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start() did not return after Stop() - timeout")
	}

	// Verify no more withdrawals happen after Stop()
	callsAfterStop := atomic.LoadInt32(&withdrawCalled)
	time.Sleep(100 * time.Millisecond)
	callsLater := atomic.LoadInt32(&withdrawCalled)

	if callsLater != callsAfterStop {
		t.Errorf("withdrawals continued after Stop(): before=%d, after=%d", callsAfterStop, callsLater)
	}
}
