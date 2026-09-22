package provisioner

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

func ackNotPendingError() error {
	return &chain.ChainTxError{Code: 22, Codespace: "billing", RawLog: "lease not in PENDING state"}
}

func TestAckBatcherRefreshesAfterBatchFailureAndFansOutExactSuccess(t *testing.T) {
	var broadcasts, exactReads atomic.Int32
	client := &mockAckChainClient{
		pendingLeases: []string{"already-active", "still-pending"},
		getLeaseFunc: func(_ context.Context, id string) (*billingtypes.Lease, error) {
			exactReads.Add(1)
			state := billingtypes.LEASE_STATE_PENDING
			if id == "already-active" {
				state = billingtypes.LEASE_STATE_ACTIVE
			}
			return &billingtypes.Lease{Uuid: id, ProviderUuid: testProviderUUID, State: state}, nil
		},
		acknowledgeFunc: func(_ context.Context, ids []string) (uint64, []string, error) {
			if broadcasts.Add(1) == 1 {
				return 0, nil, ackNotPendingError()
			}
			if len(ids) != 1 || ids[0] != "still-pending" {
				return 0, nil, ackNotPendingError()
			}
			return 1, []string{"sibling-tx"}, nil
		},
	}
	batcher := NewAckBatcher(client, AckBatcherConfig{ProviderUUID: testProviderUUID, BatchSize: 3, BatchInterval: time.Hour})
	batcher.Start(t.Context())
	t.Cleanup(batcher.Stop)
	ids := []string{"already-active", "still-pending", "already-active"}
	results := make([]ackResult, len(ids))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var requests sync.WaitGroup
	for i, id := range ids {
		requests.Go(func() { results[i].acknowledged, results[i].txHash, results[i].err = batcher.Acknowledge(ctx, id) })
	}
	requests.Wait()
	for i, result := range results {
		require.NoError(t, result.err)
		require.True(t, result.acknowledged)
		if ids[i] == "already-active" {
			require.Empty(t, result.txHash, "exact state does not identify the winning transaction")
		} else {
			require.Equal(t, "sibling-tx", result.txHash)
		}
	}
	require.Equal(t, int32(2), broadcasts.Load(), "one failed batch and one still-pending sibling retry")
	require.Equal(t, int32(2), exactReads.Load(), "duplicate callers share one exact recovery observation")
}

func TestAckBatcherExactObservationControlsSuccess(t *testing.T) {
	for _, phase := range []string{"pending-list omission", "batch failure", "individual failure"} {
		for _, evidence := range []string{"active", "closed", "rejected", "expired", "unknown state", "missing", "wrong UUID", "foreign provider", "query error"} {
			t.Run(phase+"/"+evidence, func(t *testing.T) {
				var broadcasts atomic.Int32
				readFailure := errors.New("exact chain read failed")
				client := &mockAckChainClient{
					getLeaseFunc: func(_ context.Context, id string) (*billingtypes.Lease, error) {
						lease := &billingtypes.Lease{Uuid: id, ProviderUuid: testProviderUUID, State: billingtypes.LEASE_STATE_ACTIVE}
						if phase == "individual failure" && broadcasts.Load() == 1 {
							lease.State = billingtypes.LEASE_STATE_PENDING
							return lease, nil
						}
						switch evidence {
						case "closed":
							lease.State = billingtypes.LEASE_STATE_CLOSED
						case "rejected":
							lease.State = billingtypes.LEASE_STATE_REJECTED
						case "expired":
							lease.State = billingtypes.LEASE_STATE_EXPIRED
						case "unknown state":
							lease.State = billingtypes.LEASE_STATE_UNSPECIFIED
						case "missing":
							return nil, nil
						case "wrong UUID":
							lease.Uuid = "different-lease"
						case "foreign provider":
							lease.ProviderUuid = "different-provider"
						case "query error":
							return lease, readFailure
						}
						return lease, nil
					},
					acknowledgeFunc: func(context.Context, []string) (uint64, []string, error) {
						broadcasts.Add(1)
						return 0, []string{"not-a-successful-tx"}, ackNotPendingError()
					},
				}
				if phase != "pending-list omission" {
					client.pendingLeases = []string{"lease"}
				}
				batcher := NewAckBatcher(client, AckBatcherConfig{ProviderUUID: testProviderUUID, BatchSize: 1})
				batcher.Start(t.Context())
				t.Cleanup(batcher.Stop)
				acked, tx, err := batcher.Acknowledge(t.Context(), "lease")
				require.Empty(t, tx)
				if evidence == "active" {
					require.NoError(t, err)
					require.True(t, acked)
				} else {
					require.Error(t, err)
					require.False(t, acked)
					if evidence == "query error" {
						require.ErrorIs(t, err, readFailure)
					}
				}
				want := map[string]int32{"pending-list omission": 0, "batch failure": 1, "individual failure": 2}[phase]
				require.Equal(t, want, broadcasts.Load(), "unknown or terminal state cannot authorize another retry")
			})
		}
	}
}

func TestAckBatcherExactReadSharesFlushDeadline(t *testing.T) {
	var queries, broadcasts atomic.Int32
	client := &mockAckChainClient{
		getPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) {
			if queries.Add(1) == 1 {
				return nil, nil
			}
			return []billingtypes.Lease{{Uuid: "lease"}}, nil
		},
		getLeaseFunc: func(ctx context.Context, id string) (*billingtypes.Lease, error) {
			<-ctx.Done()
			return &billingtypes.Lease{Uuid: id, ProviderUuid: testProviderUUID, State: billingtypes.LEASE_STATE_ACTIVE}, nil
		},
		acknowledgeFunc: func(context.Context, []string) (uint64, []string, error) { broadcasts.Add(1); return 1, nil, nil },
	}
	batcher := NewAckBatcher(client, AckBatcherConfig{ProviderUUID: testProviderUUID, BatchSize: 1, FlushTimeout: 25 * time.Millisecond})
	batcher.Start(t.Context())
	t.Cleanup(batcher.Stop)
	acked, _, err := batcher.Acknowledge(t.Context(), "lease")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, acked, "a late ACTIVE response after the flush deadline cannot settle")
	acked, _, err = batcher.Acknowledge(t.Context(), "lease")
	require.NoError(t, err)
	require.True(t, acked)
	require.Equal(t, int32(1), broadcasts.Load())
}

func TestAckBatcherFreshPendingStateRequiresSuccessfulTransaction(t *testing.T) {
	for _, fails := range []bool{false, true} {
		name := "pending lease omitted by inventory"
		if fails {
			name = "still pending after failed individual transaction"
		}
		t.Run(name, func(t *testing.T) {
			var broadcasts atomic.Int32
			txFailure := errors.New("broadcast failed without changing lease state")
			client := &mockAckChainClient{
				getLeaseFunc: func(_ context.Context, id string) (*billingtypes.Lease, error) {
					return &billingtypes.Lease{Uuid: id, ProviderUuid: testProviderUUID, State: billingtypes.LEASE_STATE_PENDING}, nil
				},
				acknowledgeFunc: func(context.Context, []string) (uint64, []string, error) {
					broadcasts.Add(1)
					if fails {
						return 0, nil, txFailure
					}
					return 1, []string{"ack-tx"}, nil
				},
			}
			batcher := NewAckBatcher(client, AckBatcherConfig{ProviderUUID: testProviderUUID, BatchSize: 1})
			batcher.Start(t.Context())
			t.Cleanup(batcher.Stop)
			acked, tx, err := batcher.Acknowledge(t.Context(), "lease")
			if fails {
				require.ErrorIs(t, err, txFailure)
				require.False(t, acked)
				require.Empty(t, tx)
				require.Equal(t, int32(2), broadcasts.Load())
			} else {
				require.NoError(t, err)
				require.True(t, acked)
				require.Equal(t, "ack-tx", tx)
				require.Equal(t, int32(1), broadcasts.Load())
			}
		})
	}
}

func TestReconcilerAckRaceRequiresExactActiveProof(t *testing.T) {
	for _, state := range []billingtypes.LeaseState{billingtypes.LEASE_STATE_ACTIVE, billingtypes.LEASE_STATE_CLOSED} {
		t.Run(state.String(), func(t *testing.T) {
			const target = "00000000-0000-4000-8000-000000001031"
			const sibling = "00000000-0000-4000-8000-000000001032"
			leases := overlapPendingLeases(target, sibling)
			var batchFailed atomic.Bool
			var targetRetries, siblingRetries atomic.Int32
			client := &chaintest.MockClient{
				GetPendingLeasesFunc: func(context.Context, string) ([]billingtypes.Lease, error) { return leases, nil },
				GetLeaseFunc: func(_ context.Context, id string) (*billingtypes.Lease, error) {
					for _, lease := range leases {
						if lease.Uuid == id {
							if id == target && batchFailed.Load() {
								lease.State = state
							}
							return &lease, nil
						}
					}
					return nil, nil
				},
				AcknowledgeLeasesFunc: func(_ context.Context, ids []string) (uint64, []string, error) {
					if !batchFailed.Swap(true) {
						return 0, nil, ackNotPendingError()
					}
					if len(ids) == 1 && ids[0] == sibling {
						siblingRetries.Add(1)
						return 1, []string{"sibling-tx"}, nil
					}
					targetRetries.Add(1)
					return 0, nil, ackNotPendingError()
				},
			}
			batcher := NewAckBatcher(client, AckBatcherConfig{ProviderUUID: placementstore.ProviderUUID, BatchSize: 2, BatchInterval: time.Hour})
			batcher.Start(t.Context())
			t.Cleanup(batcher.Stop)
			owner := &mockReconcilerBackend{name: "backend-a", provisions: []backend.ProvisionInfo{
				{LeaseUUID: target, Status: backend.ProvisionStatusReady}, {LeaseUUID: sibling, Status: backend.ProvisionStatusReady},
			}}
			router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{Backend: owner, IsDefault: true}}})
			require.NoError(t, err)
			store, err := placementstore.NewStore(filepath.Join(t.TempDir(), "placements.db"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			reconciler, err := newTestReconciler(t, ReconcilerConfig{}, client, batcher, router, nil, store)
			require.NoError(t, err)
			errorsMetric := metrics.ReconciliationActions.WithLabelValues(metrics.ActionLeaseError)
			before := promtestutil.ToFloat64(errorsMetric)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			require.NoError(t, reconciler.ReconcileAll(ctx))
			wantErrors := float64(0)
			if state != billingtypes.LEASE_STATE_ACTIVE {
				wantErrors = 1
			}
			require.Equal(t, wantErrors, promtestutil.ToFloat64(errorsMetric)-before)
			require.Zero(t, targetRetries.Load(), "fresh terminal/active evidence prevents repeated stale ACK")
			require.Equal(t, int32(1), siblingRetries.Load(), "the still-PENDING sibling completes")
			owner.mu.Lock()
			defer owner.mu.Unlock()
			require.Empty(t, owner.provisionCalls)
			require.Empty(t, owner.deprovisionCalls)
		})
	}
}
