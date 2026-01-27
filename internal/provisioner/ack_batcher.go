package provisioner

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const (
	// DefaultAckBatchInterval is the maximum time to wait before flushing a batch.
	// Short interval to minimize latency while still allowing batching.
	DefaultAckBatchInterval = 500 * time.Millisecond

	// DefaultAckBatchSize is the maximum number of acks to batch before flushing.
	// Keep small to limit blast radius when one lease fails (atomic tx = all or nothing).
	// If batch fails, we fall back to individual acks, so smaller batches are better.
	DefaultAckBatchSize = 10
)

// AckBatcherConfig configures the acknowledgment batcher.
type AckBatcherConfig struct {
	// BatchInterval is the maximum time to wait before flushing a batch.
	// Default: 500ms
	BatchInterval time.Duration

	// BatchSize is the maximum number of acks to collect before flushing.
	// Default: 50
	BatchSize int
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

// AckBatcher batches lease acknowledgment requests to avoid sequence mismatch errors.
// Instead of sending individual transactions for each lease, it collects requests
// and sends them in a single multi-lease transaction.
type AckBatcher struct {
	chainClient ChainClient

	batchInterval time.Duration
	batchSize     int

	// Channel for incoming ack requests
	requests chan ackRequest

	// For graceful shutdown
	cancel context.CancelFunc
	wg     *sync.WaitGroup
}

// NewAckBatcher creates a new acknowledgment batcher.
func NewAckBatcher(chainClient ChainClient, cfg AckBatcherConfig) *AckBatcher {
	// Apply defaults
	interval := cfg.BatchInterval
	if interval == 0 {
		interval = DefaultAckBatchInterval
	}
	size := cfg.BatchSize
	if size == 0 {
		size = DefaultAckBatchSize
	}

	return &AckBatcher{
		chainClient:   chainClient,
		batchInterval: interval,
		batchSize:     size,
		requests:      make(chan ackRequest, size*2), // Buffer to prevent blocking
		wg:            &sync.WaitGroup{},
	}
}

// Start begins the batching loop. Call this before submitting requests.
func (b *AckBatcher) Start(ctx context.Context) {
	ctx, b.cancel = context.WithCancel(ctx)

	b.wg.Add(1)
	go b.batchLoop(ctx)

	slog.Info("ack batcher started",
		"batch_interval", b.batchInterval,
		"batch_size", b.batchSize,
	)
}

// Stop gracefully shuts down the batcher, flushing any pending requests.
func (b *AckBatcher) Stop() {
	if b.cancel != nil {
		b.cancel()
	}
	b.wg.Wait()
	slog.Info("ack batcher stopped")
}

// Acknowledge queues a lease for acknowledgment and waits for the result.
// Returns true if the lease was acknowledged, along with the transaction hash.
// This method blocks until the acknowledgment is processed (batched with others).
func (b *AckBatcher) Acknowledge(ctx context.Context, leaseUUID string) (bool, string, error) {
	resultCh := make(chan ackResult, 1)

	select {
	case b.requests <- ackRequest{leaseUUID: leaseUUID, resultCh: resultCh}:
	case <-ctx.Done():
		return false, "", ctx.Err()
	}

	select {
	case result := <-resultCh:
		return result.acknowledged, result.txHash, result.err
	case <-ctx.Done():
		return false, "", ctx.Err()
	}
}

// batchLoop collects requests and flushes them periodically or when batch is full.
func (b *AckBatcher) batchLoop(ctx context.Context) {
	defer b.wg.Done()

	var pending []ackRequest
	timer := time.NewTimer(b.batchInterval)
	defer timer.Stop()

	flush := func() {
		if len(pending) == 0 {
			return
		}

		// Collect lease UUIDs
		leaseUUIDs := make([]string, len(pending))
		for i, req := range pending {
			leaseUUIDs[i] = req.leaseUUID
		}

		slog.Debug("flushing ack batch", "count", len(leaseUUIDs))

		// Try batched acknowledgment first
		acknowledged, txHashes, err := b.chainClient.AcknowledgeLeases(ctx, leaseUUIDs)

		if err == nil {
			// Batch succeeded - notify all waiters with success
			var txHash string
			if len(txHashes) > 0 {
				txHash = txHashes[0]
			}

			for _, req := range pending {
				select {
				case req.resultCh <- ackResult{acknowledged: true, txHash: txHash}:
				default:
				}
			}

			slog.Info("batch acknowledgment completed",
				"count", acknowledged,
				"tx_hashes", txHashes,
			)
		} else {
			// Batch failed - fall back to individual acknowledgments
			// This handles atomic transaction failures (one bad lease fails the whole batch)
			slog.Warn("batch acknowledgment failed, falling back to individual acks",
				"count", len(leaseUUIDs),
				"error", err,
			)
			b.acknowledgeIndividually(ctx, pending)
		}

		// Clear the batch
		pending = pending[:0]
	}

	for {
		select {
		case <-ctx.Done():
			// Flush remaining requests before shutdown
			flush()
			// Send cancellation error to any remaining requests in the channel
			close(b.requests)
			for req := range b.requests {
				select {
				case req.resultCh <- ackResult{err: context.Canceled}:
				default:
				}
			}
			return

		case req := <-b.requests:
			pending = append(pending, req)

			// Flush if batch is full
			if len(pending) >= b.batchSize {
				flush()
				// Reset timer since we just flushed
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(b.batchInterval)
			}

		case <-timer.C:
			// Flush on interval timeout
			flush()
			timer.Reset(b.batchInterval)
		}
	}
}

// acknowledgeIndividually processes each request one at a time.
// This is the fallback when batch acknowledgment fails.
func (b *AckBatcher) acknowledgeIndividually(ctx context.Context, requests []ackRequest) {
	for _, req := range requests {
		if ctx.Err() != nil {
			select {
			case req.resultCh <- ackResult{err: ctx.Err()}:
			default:
			}
			continue
		}

		acknowledged, txHashes, err := b.chainClient.AcknowledgeLeases(ctx, []string{req.leaseUUID})

		var txHash string
		if len(txHashes) > 0 {
			txHash = txHashes[0]
		}

		result := ackResult{
			acknowledged: err == nil && acknowledged > 0,
			txHash:       txHash,
			err:          err,
		}

		select {
		case req.resultCh <- result:
		default:
		}

		if err != nil {
			slog.Error("individual acknowledgment failed",
				"lease_uuid", req.leaseUUID,
				"error", err,
			)
		} else {
			slog.Debug("individual acknowledgment succeeded",
				"lease_uuid", req.leaseUUID,
				"tx_hash", txHash,
			)
		}
	}
}
