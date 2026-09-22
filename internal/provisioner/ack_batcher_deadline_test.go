package provisioner

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"
)

func TestAckBatcher_FlushBudgetIncludesQueryAndAllowsNextBatch(t *testing.T) {
	var queries, broadcasts atomic.Int32
	client := &mockAckChainClient{
		getPendingLeasesFunc: func(ctx context.Context, _ string) ([]billingtypes.Lease, error) {
			if queries.Add(1) == 1 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return []billingtypes.Lease{{Uuid: "lease"}}, nil
		},
		acknowledgeFunc: func(context.Context, []string) (uint64, []string, error) {
			broadcasts.Add(1)
			return 1, []string{"tx"}, nil
		},
	}
	batcher := NewAckBatcher(client, AckBatcherConfig{BatchSize: 1, FlushTimeout: 25 * time.Millisecond})
	batcher.Start(t.Context())
	defer batcher.Stop()
	acked, _, err := batcher.Acknowledge(t.Context(), "lease")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, acked)
	require.Zero(t, broadcasts.Load(), "an expired query must not start a write")
	acked, _, err = batcher.Acknowledge(t.Context(), "lease")
	require.NoError(t, err)
	require.True(t, acked)
	require.Equal(t, int32(1), broadcasts.Load())
}

func TestAckBatcher_CanceledCallerDoesNotCancelSharedFlush(t *testing.T) {
	queryEntered := make(chan context.Context, 1)
	resume := make(chan struct{})
	var writes atomic.Int32
	client := &mockAckChainClient{
		getPendingLeasesFunc: func(ctx context.Context, _ string) ([]billingtypes.Lease, error) {
			queryEntered <- ctx
			select {
			case <-resume:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return []billingtypes.Lease{{Uuid: "first"}, {Uuid: "second"}}, nil
		},
		acknowledgeFunc: func(ctx context.Context, ids []string) (uint64, []string, error) {
			if ctx.Err() != nil {
				return 0, nil, ctx.Err()
			}
			writes.Add(1)
			return uint64(len(ids)), []string{"tx"}, nil
		},
	}
	batcher := NewAckBatcher(client, AckBatcherConfig{BatchSize: 2, BatchInterval: time.Hour, FlushTimeout: time.Second})
	batcher.Start(t.Context())
	defer batcher.Stop()
	firstCtx, cancelFirst := context.WithCancel(t.Context())
	defer cancelFirst()
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	go func() { _, _, err := batcher.Acknowledge(firstCtx, "first"); firstDone <- err }()
	go func() { _, _, err := batcher.Acknowledge(t.Context(), "second"); secondDone <- err }()
	var ownerCtx context.Context
	select {
	case ownerCtx = <-queryEntered:
	case <-time.After(time.Second):
		t.Fatal("flush did not start")
	}
	cancelFirst()
	require.ErrorIs(t, <-firstDone, context.Canceled)
	require.NoError(t, ownerCtx.Err(), "one caller does not own the shared batch lifetime")
	close(resume)
	require.NoError(t, <-secondDone)
	require.Equal(t, int32(1), writes.Load())
}

func TestAckBatcher_FlushBudgetBoundsBroadcast(t *testing.T) {
	var calls atomic.Int32
	client := &mockAckChainClient{
		pendingLeases: []string{"lease"},
		acknowledgeFunc: func(ctx context.Context, _ []string) (uint64, []string, error) {
			calls.Add(1)
			<-ctx.Done()
			return 0, nil, ctx.Err()
		},
	}
	batcher := NewAckBatcher(client, AckBatcherConfig{BatchSize: 1, FlushTimeout: 25 * time.Millisecond})
	batcher.Start(t.Context())
	defer batcher.Stop()
	_, _, err := batcher.Acknowledge(t.Context(), "lease")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, int32(1), calls.Load())
}
