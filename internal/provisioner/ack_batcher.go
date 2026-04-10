package provisioner

import (
	"cmp"
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Acknowledger defines the interface for acknowledging leases on chain.
// This is used by handlers to acknowledge successful provisions.
type Acknowledger interface {
	// Acknowledge queues a lease for acknowledgment and waits for the result.
	// Returns true if the lease is in an acknowledged state (either by this call
	// or already acknowledged), along with the transaction hash (empty if no tx
	// was needed). This method blocks until the acknowledgment is processed.
	Acknowledge(ctx context.Context, leaseUUID string) (acknowledged bool, txHash string, err error)
}

// Compile-time check that AckBatcher implements Acknowledger.
var _ Acknowledger = (*AckBatcher)(nil)

const (
	// DefaultAckBatchInterval is the maximum time to wait before flushing a batch.
	// Short interval to minimize latency while still allowing batching.
	DefaultAckBatchInterval = 500 * time.Millisecond

	// DefaultAckBatchSize is the maximum number of acks to batch before flushing.
	// With authz sub-signers, each lane can handle a large batch per block.
	DefaultAckBatchSize = 50
)

// AckBatcherConfig configures the acknowledgment batcher.
type AckBatcherConfig struct {
	// ProviderUUID is required for querying pending leases.
	ProviderUUID string

	// BatchInterval is the maximum time to wait before flushing a batch.
	// Default: 500ms
	BatchInterval time.Duration

	// BatchSize is the maximum number of acks to collect before flushing.
	// Default: DefaultAckBatchSize (50)
	BatchSize int

	// LaneCount is the number of parallel lanes. Defaults to 1 (single signer).
	// With N sub-signers, set to N.
	LaneCount int
}

// ackRequest represents a pending acknowledgment request.
type ackRequest struct {
	leaseUUID string
	resultCh  chan<- ackResult
}

// ackResult contains the result of an acknowledgment attempt.
type ackResult struct {
	acknowledged bool
	txHash       string
	err          error
}

// ackLane is an independent worker that runs its own batchLoop.
// Each lane has its own channel, timer, and flush goroutine.
type ackLane struct {
	chainClient   ChainClient
	providerUUID  string
	batchInterval time.Duration
	batchSize     int
	requests      chan ackRequest
	done          chan struct{}
}

// AckBatcher distributes lease acknowledgment requests across N independent lanes.
// Each lane runs its own sequential batchLoop, enabling parallel broadcasting
// when backed by multiple signers via authz.
type AckBatcher struct {
	lanes  []*ackLane
	next   atomic.Uint64
	cancel context.CancelFunc
	wg     *sync.WaitGroup
}

// NewAckBatcher creates a new acknowledgment batcher with N lanes.
func NewAckBatcher(chainClient ChainClient, cfg AckBatcherConfig) *AckBatcher {
	interval := cmp.Or(cfg.BatchInterval, DefaultAckBatchInterval)
	size := cmp.Or(cfg.BatchSize, DefaultAckBatchSize)
	laneCount := cmp.Or(max(cfg.LaneCount, 0), 1)

	lanes := make([]*ackLane, laneCount)
	for i := range laneCount {
		lanes[i] = &ackLane{
			chainClient:   chainClient,
			providerUUID:  cfg.ProviderUUID,
			batchInterval: interval,
			batchSize:     size,
			requests:      make(chan ackRequest, size*2),
			done:          make(chan struct{}),
		}
	}

	return &AckBatcher{
		lanes: lanes,
		wg:    &sync.WaitGroup{},
	}
}

// Start begins the batching loop for all lanes.
func (b *AckBatcher) Start(ctx context.Context) {
	ctx, b.cancel = context.WithCancel(ctx)

	for i, lane := range b.lanes {
		b.wg.Go(func() { lane.batchLoop(ctx, i) })
	}

	slog.Info("ack batcher started",
		"lanes", len(b.lanes),
		"batch_interval", b.lanes[0].batchInterval,
		"batch_size", b.lanes[0].batchSize,
	)
}

// Stop gracefully shuts down all lanes, flushing pending requests.
func (b *AckBatcher) Stop() {
	if b.cancel != nil {
		b.cancel()
	}
	b.wg.Wait()
	slog.Info("ack batcher stopped")
}

// Acknowledge queues a lease for acknowledgment and waits for the result.
// Requests are distributed across lanes via round-robin.
func (b *AckBatcher) Acknowledge(ctx context.Context, leaseUUID string) (bool, string, error) {
	resultCh := make(chan ackResult, 1)

	// Round-robin lane selection with fallback for stopped lanes
	startIdx := b.next.Add(1) - 1
	n := uint64(len(b.lanes))

	for attempt := range n {
		lane := b.lanes[(startIdx+attempt)%n]

		select {
		case lane.requests <- ackRequest{leaseUUID: leaseUUID, resultCh: resultCh}:
			// Sent — wait for result from this lane
			select {
			case result := <-resultCh:
				return result.acknowledged, result.txHash, result.err
			case <-lane.done:
				return false, "", context.Canceled
			case <-ctx.Done():
				return false, "", ctx.Err()
			}
		case <-lane.done:
			// Lane stopped, try next
			continue
		case <-ctx.Done():
			return false, "", ctx.Err()
		}
	}

	// All lanes stopped
	return false, "", context.Canceled
}

// batchLoop collects requests and flushes them periodically or when batch is full.
func (l *ackLane) batchLoop(ctx context.Context, laneIdx int) {
	defer close(l.done)

	var pending []ackRequest
	timer := time.NewTimer(l.batchInterval)
	defer timer.Stop()

	flush := func() {
		if len(pending) == 0 {
			return
		}

		slog.Debug("flushing ack batch", "lane", laneIdx, "count", len(pending))

		chainPendingLeases, err := l.chainClient.GetPendingLeases(ctx, l.providerUUID)

		// nil map = query failed, attempt all; non-nil = filter by pending
		var pendingOnChain map[string]struct{}
		if err != nil {
			slog.Warn("failed to query pending leases, will attempt ack for all",
				"lane", laneIdx, "error", err,
			)
		} else {
			pendingOnChain = make(map[string]struct{}, len(chainPendingLeases))
			for _, lease := range chainPendingLeases {
				pendingOnChain[lease.Uuid] = struct{}{}
			}
		}

		var pendingLeases []ackRequest
		for _, req := range pending {
			if pendingOnChain == nil {
				// Query failed — include all leases (conservative approach)
				pendingLeases = append(pendingLeases, req)
				continue
			}

			if _, isPending := pendingOnChain[req.leaseUUID]; isPending {
				pendingLeases = append(pendingLeases, req)
			} else {
				slog.Debug("lease not pending, skipping acknowledgment",
					"lease_uuid", req.leaseUUID, "lane", laneIdx,
				)
				select {
				case req.resultCh <- ackResult{acknowledged: true, txHash: ""}:
				default:
				}
			}
		}

		pending = pending[:0]

		if len(pendingLeases) == 0 {
			slog.Debug("all leases already acknowledged, no tx needed", "lane", laneIdx)
			return
		}

		leaseUUIDs := make([]string, len(pendingLeases))
		for i, req := range pendingLeases {
			leaseUUIDs[i] = req.leaseUUID
		}

		acknowledged, txHashes, err := l.chainClient.AcknowledgeLeases(ctx, leaseUUIDs)

		if err == nil {
			var txHash string
			if len(txHashes) > 0 {
				txHash = txHashes[0]
			}

			for _, req := range pendingLeases {
				select {
				case req.resultCh <- ackResult{acknowledged: true, txHash: txHash}:
				default:
				}
			}

			slog.Info("batch acknowledgment completed",
				"lane", laneIdx, "count", acknowledged, "tx_hashes", txHashes,
			)
		} else {
			slog.Warn("batch acknowledgment failed, falling back to individual acks",
				"lane", laneIdx, "count", len(leaseUUIDs), "error", err,
			)
			l.acknowledgeIndividually(ctx, pendingLeases, pendingOnChain, laneIdx)
		}
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			for {
				select {
				case req := <-l.requests:
					select {
					case req.resultCh <- ackResult{err: context.Canceled}:
					default:
					}
				default:
					return
				}
			}

		case req := <-l.requests:
			pending = append(pending, req)

			if len(pending) >= l.batchSize {
				flush()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(l.batchInterval)
			}

		case <-timer.C:
			flush()
			timer.Reset(l.batchInterval)
		}
	}
}

// acknowledgeIndividually processes each request one at a time.
// Reuses the pendingOnChain map from the caller's flush to avoid a redundant RPC.
func (l *ackLane) acknowledgeIndividually(ctx context.Context, requests []ackRequest, pendingOnChain map[string]struct{}, laneIdx int) {
	for _, req := range requests {
		if ctx.Err() != nil {
			select {
			case req.resultCh <- ackResult{err: ctx.Err()}:
			default:
			}
			continue
		}

		if pendingOnChain != nil {
			if _, isPending := pendingOnChain[req.leaseUUID]; !isPending {
				slog.Debug("lease not pending during individual ack, skipping",
					"lease_uuid", req.leaseUUID, "lane", laneIdx,
				)
				select {
				case req.resultCh <- ackResult{acknowledged: true, txHash: ""}:
				default:
				}
				continue
			}
		}

		acknowledged, txHashes, ackErr := l.chainClient.AcknowledgeLeases(ctx, []string{req.leaseUUID})

		var txHash string
		if len(txHashes) > 0 {
			txHash = txHashes[0]
		}

		result := ackResult{
			acknowledged: ackErr == nil && acknowledged > 0,
			txHash:       txHash,
			err:          ackErr,
		}

		select {
		case req.resultCh <- result:
		default:
		}

		if ackErr != nil {
			slog.Error("individual acknowledgment failed",
				"lease_uuid", req.leaseUUID, "lane", laneIdx, "error", ackErr,
			)
		} else {
			slog.Debug("individual acknowledgment succeeded",
				"lease_uuid", req.leaseUUID, "lane", laneIdx, "tx_hash", txHash,
			)
		}
	}
}
