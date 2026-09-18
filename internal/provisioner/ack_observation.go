package provisioner

import (
	"context"
	"errors"
	"fmt"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

// ackLeaseObservation is issued only by the lane's construction-bound exact
// chain reader. A missing entry in a PENDING inventory is never acknowledgment
// evidence: only the exact provider-owned ACTIVE lease can settle without a tx.
// The lane does not receive tenant metadata; its callers retain the separate
// placement/operation claim which owns that immutable lease identity.
type ackLeaseObservation interface{ ackLeaseObservation() }

type ackLeasePending struct{}
type ackLeaseActive struct{}
type ackLeaseUnresolved struct{ err error }

func (ackLeasePending) ackLeaseObservation()    {}
func (ackLeaseActive) ackLeaseObservation()     {}
func (ackLeaseUnresolved) ackLeaseObservation() {}

func (l *ackLane) observeAcknowledgment(ctx context.Context, leaseUUID string) ackLeaseObservation {
	if err := ctx.Err(); err != nil {
		return ackLeaseUnresolved{err: err}
	}
	if leaseUUID == "" || l.providerUUID == "" {
		return ackLeaseUnresolved{err: errors.New("exact acknowledgment requires lease and provider identities")}
	}
	lease, err := l.chainClient.GetLease(ctx, leaseUUID)
	if err != nil {
		return ackLeaseUnresolved{err: fmt.Errorf("read acknowledgment state for %s: %w", leaseUUID, err)}
	}
	if err := ctx.Err(); err != nil {
		return ackLeaseUnresolved{err: err}
	}
	if lease == nil {
		return ackLeaseUnresolved{err: fmt.Errorf("acknowledgment state for %s is absent", leaseUUID)}
	}
	if lease.Uuid != leaseUUID || lease.ProviderUuid != l.providerUUID {
		return ackLeaseUnresolved{err: fmt.Errorf("acknowledgment state for %s differs from bound lease/provider", leaseUUID)}
	}
	switch lease.State {
	case billingtypes.LEASE_STATE_PENDING:
		return ackLeasePending{}
	case billingtypes.LEASE_STATE_ACTIVE:
		return ackLeaseActive{}
	default:
		return ackLeaseUnresolved{err: fmt.Errorf("lease %s is %s, not acknowledged ACTIVE", leaseUUID, lease.State)}
	}
}

// acknowledgeCurrent owns the whole individual retry, including the race
// between its point read and broadcast. An errored transaction supplies no
// success authority; a later exact ACTIVE observation can independently prove
// the desired state, without attributing it to any particular transaction.
func (l *ackLane) acknowledgeCurrent(ctx context.Context, leaseUUID string) ackResult {
	switch observed := l.observeAcknowledgment(ctx, leaseUUID).(type) {
	case ackLeaseActive:
		return ackResult{acknowledged: true}
	case ackLeaseUnresolved:
		return ackResult{err: observed.err}
	case ackLeasePending:
		// Only this fresh observation reaches the retry below.
	default:
		return ackResult{err: errors.New("acknowledgment observation is invalid")}
	}
	acknowledged, txHashes, err := l.chainClient.AcknowledgeLeases(ctx, []string{leaseUUID})
	if err != nil {
		switch observed := l.observeAcknowledgment(ctx, leaseUUID).(type) {
		case ackLeaseActive:
			return ackResult{acknowledged: true}
		case ackLeaseUnresolved:
			return ackResult{err: errors.Join(err, observed.err)}
		case ackLeasePending:
			return ackResult{err: err}
		default:
			return ackResult{err: err}
		}
	}
	var txHash string
	if len(txHashes) > 0 {
		txHash = txHashes[0]
	}
	return ackResult{acknowledged: acknowledged > 0, txHash: txHash}
}
