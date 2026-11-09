package provisioner

import (
	"context"
	"errors"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

// observedAckBatch binds exact provider-owned PENDING observations to their
// reader and waiting callers. Only observeAckBatch constructs it; broadcast
// takes no replacement identities or client. This is point-in-time evidence,
// not a reservation: the chain still checks every member atomically.
type observedAckBatch struct {
	lane     *ackLane
	requests []ackRequest
}

func (l *ackLane) observeAckBatch(ctx context.Context, requests []ackRequest, duplicates map[string][]chan<- ackResult) observedAckBatch {
	batch := observedAckBatch{lane: l}
	for _, req := range requests {
		switch observed := l.observeAcknowledgment(ctx, req.leaseUUID).(type) {
		case ackLeasePending:
			batch.requests = append(batch.requests, req)
		case ackLeaseActive:
			deliverAckResult(req, duplicates, ackResult{acknowledged: true})
		case ackLeaseUnresolved:
			deliverAckResult(req, duplicates, ackResult{err: observed.err})
		default:
			deliverAckResult(req, duplicates, ackResult{err: errors.New("acknowledgment observation is invalid")})
		}
	}
	return batch
}

func (b observedAckBatch) broadcast(ctx context.Context) (uint64, []string, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	if len(b.requests) == 0 {
		return 0, nil, nil // Includes the zero value: it cannot authorize a write.
	}
	ids := make([]string, len(b.requests))
	for i, req := range b.requests {
		ids[i] = req.leaseUUID
	}
	return b.lane.chainClient.AcknowledgeLeases(ctx, ids)
}

// retryChangedAckBatch resolves callers with exact ACTIVE or ineligible state
// and retries the remaining PENDING set together. Every retry must shrink the
// set, bounding broadcasts by the original number of distinct requests even
// when a stale reader keeps returning PENDING. Unrelated errors and no-progress
// observations return to the ordinary fee/gas or individual fallback paths.
// All observations and writes share the original flush deadline.
func (l *ackLane) retryChangedAckBatch(ctx context.Context, requests []ackRequest, duplicates map[string][]chan<- ackResult, err error) ([]ackRequest, uint64, []string, error) {
	var acknowledged uint64
	var hashes []string
	for errors.Is(err, billingtypes.ErrLeaseNotPending) {
		batch := l.observeAckBatch(ctx, requests, duplicates)
		if len(batch.requests) == len(requests) {
			break
		}
		requests = batch.requests
		if len(requests) == 0 {
			return nil, 0, nil, nil
		}
		acknowledged, hashes, err = batch.broadcast(ctx)
	}
	return requests, acknowledged, hashes, err
}

func deliverAckResult(req ackRequest, duplicates map[string][]chan<- ackResult, result ackResult) {
	select {
	case req.resultCh <- result:
	default:
	}
	for _, ch := range duplicates[req.leaseUUID] {
		select {
		case ch <- result:
		default:
		}
	}
}
