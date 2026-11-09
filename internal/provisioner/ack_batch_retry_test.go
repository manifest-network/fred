package provisioner

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/chain"
)

// Queue the complete flush before starting its owner so request ordering and
// duplicate membership do not depend on goroutine scheduling or timer sleeps.
func runAckRetryBatch(t *testing.T, client *mockAckChainClient, ids []string, timeout time.Duration) []ackResult {
	t.Helper()
	batcher := NewAckBatcher(client, AckBatcherConfig{
		ProviderUUID: testProviderUUID, BatchSize: len(ids), BatchInterval: time.Hour, FlushTimeout: timeout,
	})
	channels := make([]chan ackResult, len(ids))
	for i, id := range ids {
		channels[i] = make(chan ackResult, 1)
		batcher.lanes[0].requests <- ackRequest{leaseUUID: id, resultCh: channels[i]}
	}
	batcher.Start(t.Context())
	t.Cleanup(batcher.Stop)
	results := make([]ackResult, len(ids))
	for i, ch := range channels {
		select {
		case results[i] = <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("flush left a caller without a result")
		}
	}
	batcher.Stop() // Join the lane before inspecting its recorded calls.
	return results
}

func TestAckBatcherRetriesFilteredBatchAfterPendingRace(t *testing.T) {
	for _, state := range []billingtypes.LeaseState{
		billingtypes.LEASE_STATE_CLOSED, billingtypes.LEASE_STATE_EXPIRED, billingtypes.LEASE_STATE_ACTIVE,
	} {
		t.Run(state.String(), func(t *testing.T) {
			ids := []string{"a", "b", "c", "d", "e", "f", "g", "changed", "a", "changed"}
			var calls [][]string
			client := &mockAckChainClient{
				pendingLeases: ids,
				getLeaseFunc: func(_ context.Context, id string) (*billingtypes.Lease, error) {
					observed := billingtypes.LEASE_STATE_PENDING
					if id == "changed" {
						observed = state
					}
					return &billingtypes.Lease{Uuid: id, ProviderUuid: testProviderUUID, State: observed}, nil
				},
				acknowledgeFunc: func(_ context.Context, ids []string) (uint64, []string, error) {
					calls = append(calls, append([]string(nil), ids...))
					if len(calls) == 1 {
						return 0, nil, fmt.Errorf("broadcast: %w", ackNotPendingError())
					}
					return uint64(len(ids)), []string{"retry-tx"}, nil
				},
			}
			results := runAckRetryBatch(t, client, ids, time.Second)
			require.Equal(t, [][]string{{"a", "b", "c", "d", "e", "f", "g", "changed"}, {"a", "b", "c", "d", "e", "f", "g"}}, calls)
			for i, result := range results {
				if ids[i] == "changed" {
					require.Empty(t, result.txHash)
					require.Equal(t, state == billingtypes.LEASE_STATE_ACTIVE, result.acknowledged)
					if state != billingtypes.LEASE_STATE_ACTIVE {
						require.Error(t, result.err)
					} else {
						require.NoError(t, result.err)
					}
				} else {
					require.NoError(t, result.err)
					require.True(t, result.acknowledged)
					require.Equal(t, "retry-tx", result.txHash)
				}
			}
		})
	}
}

func TestAckBatcherFilteredRetryHandlesAnotherRace(t *testing.T) {
	var calls [][]string
	client := &mockAckChainClient{
		pendingLeases: []string{"a", "b", "c", "d"},
		getLeaseFunc: func(_ context.Context, id string) (*billingtypes.Lease, error) {
			state := billingtypes.LEASE_STATE_PENDING
			if id == "d" || (id == "c" && len(calls) >= 2) {
				state = billingtypes.LEASE_STATE_CLOSED
			}
			return &billingtypes.Lease{Uuid: id, ProviderUuid: testProviderUUID, State: state}, nil
		},
		acknowledgeFunc: func(_ context.Context, ids []string) (uint64, []string, error) {
			calls = append(calls, append([]string(nil), ids...))
			if len(calls) <= 2 {
				return 0, nil, ackNotPendingError()
			}
			return uint64(len(ids)), []string{"tx"}, nil
		},
	}
	results := runAckRetryBatch(t, client, []string{"a", "b", "c", "d", "c", "a"}, time.Second)
	require.Equal(t, [][]string{{"a", "b", "c", "d"}, {"a", "b", "c"}, {"a", "b"}}, calls)
	for _, i := range []int{0, 1, 5} {
		require.NoError(t, results[i].err)
		require.True(t, results[i].acknowledged)
	}
	for _, i := range []int{2, 3, 4} {
		require.Error(t, results[i].err)
		require.False(t, results[i].acknowledged)
	}
}

func TestAckBatcherFilteredRetryRejectsUnboundOrUnknownEvidence(t *testing.T) {
	for _, evidence := range []string{"foreign provider", "wrong UUID", "missing", "query error", "unknown"} {
		t.Run(evidence, func(t *testing.T) {
			var calls [][]string
			client := &mockAckChainClient{
				pendingLeases: []string{"a", "b", "bad"},
				getLeaseFunc: func(_ context.Context, id string) (*billingtypes.Lease, error) {
					lease := &billingtypes.Lease{Uuid: id, ProviderUuid: testProviderUUID, State: billingtypes.LEASE_STATE_PENDING}
					if id == "bad" {
						switch evidence {
						case "foreign provider":
							lease.ProviderUuid = "other"
						case "wrong UUID":
							lease.Uuid = "other"
						case "missing":
							return nil, nil
						case "query error":
							return nil, errors.New("query failed")
						case "unknown":
							lease.State = billingtypes.LEASE_STATE_UNSPECIFIED
						}
					}
					return lease, nil
				},
				acknowledgeFunc: func(_ context.Context, ids []string) (uint64, []string, error) {
					calls = append(calls, append([]string(nil), ids...))
					if len(calls) == 1 {
						return 0, nil, ackNotPendingError()
					}
					return uint64(len(ids)), nil, nil
				},
			}
			results := runAckRetryBatch(t, client, []string{"a", "b", "bad", "bad"}, time.Second)
			require.Equal(t, [][]string{{"a", "b", "bad"}, {"a", "b"}}, calls)
			for _, result := range results[:2] {
				require.NoError(t, result.err)
				require.True(t, result.acknowledged)
			}
			for _, result := range results[2:] {
				require.Error(t, result.err)
				require.False(t, result.acknowledged)
			}
		})
	}
}

func TestAckBatcherFilteredRetryFailurePolicy(t *testing.T) {
	for _, failure := range []string{"fee", "gas", "unrelated", "no progress"} {
		t.Run(failure, func(t *testing.T) {
			var calls [][]string
			client := &mockAckChainClient{
				pendingLeases: []string{"a", "b", "gone"},
				getLeaseFunc: func(_ context.Context, id string) (*billingtypes.Lease, error) {
					state := billingtypes.LEASE_STATE_PENDING
					if id == "gone" {
						state = billingtypes.LEASE_STATE_CLOSED
					}
					return &billingtypes.Lease{Uuid: id, ProviderUuid: testProviderUUID, State: state}, nil
				},
				acknowledgeFunc: func(_ context.Context, ids []string) (uint64, []string, error) {
					calls = append(calls, append([]string(nil), ids...))
					if len(calls) == 1 {
						return 0, nil, ackNotPendingError()
					}
					if len(calls) == 2 {
						switch failure {
						case "fee":
							return 0, nil, &chain.ChainTxError{Codespace: "sdk", Code: 13}
						case "gas":
							return 0, nil, &chain.ChainTxError{Codespace: "sdk", Code: 11}
						case "unrelated":
							return 0, nil, errors.New("transport failed")
						default:
							return 0, nil, ackNotPendingError()
						}
					}
					return uint64(len(ids)), nil, nil
				},
			}
			results := runAckRetryBatch(t, client, []string{"a", "b", "gone", "a"}, time.Second)
			feeGas := failure == "fee" || failure == "gas"
			if feeGas {
				require.Len(t, calls, 2, "fee/gas failures never amplify into individual retries")
			} else {
				require.Equal(t, [][]string{{"a", "b", "gone"}, {"a", "b"}, {"a"}, {"b"}}, calls)
			}
			for _, i := range []int{0, 1, 3} {
				require.Equal(t, !feeGas, results[i].acknowledged)
				if feeGas {
					require.Error(t, results[i].err)
				} else {
					require.NoError(t, results[i].err)
				}
			}
			require.Error(t, results[2].err)
		})
	}
}

func TestAckBatcherFilteredRetrySharesDeadline(t *testing.T) {
	for _, phase := range []string{"observation", "broadcast"} {
		t.Run(phase, func(t *testing.T) {
			var calls int
			var flushDeadline time.Time
			client := &mockAckChainClient{
				pendingLeases: []string{"a", "b", "gone"},
				getLeaseFunc: func(ctx context.Context, id string) (*billingtypes.Lease, error) {
					deadline, _ := ctx.Deadline()
					require.Equal(t, flushDeadline, deadline)
					state := billingtypes.LEASE_STATE_PENDING
					if id == "gone" {
						state = billingtypes.LEASE_STATE_CLOSED
						if phase == "observation" {
							<-ctx.Done() // Earlier PENDING observations must not authorize a late write.
						}
					}
					return &billingtypes.Lease{Uuid: id, ProviderUuid: testProviderUUID, State: state}, nil
				},
				acknowledgeFunc: func(ctx context.Context, ids []string) (uint64, []string, error) {
					calls++
					deadline, _ := ctx.Deadline()
					if calls == 1 {
						flushDeadline = deadline
						return 0, nil, ackNotPendingError()
					}
					require.Equal(t, flushDeadline, deadline)
					<-ctx.Done()
					return 0, nil, ctx.Err()
				},
			}
			results := runAckRetryBatch(t, client, []string{"a", "b", "gone", "a"}, 25*time.Millisecond)
			for _, i := range []int{0, 1, 3} {
				require.ErrorIs(t, results[i].err, context.DeadlineExceeded)
				require.False(t, results[i].acknowledged)
			}
			require.Error(t, results[2].err)
			if phase == "observation" {
				require.Equal(t, 1, calls)
			} else {
				require.Equal(t, 2, calls)
			}
		})
	}
}
