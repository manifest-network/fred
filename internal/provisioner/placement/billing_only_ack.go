package placement

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/provisioner/operation"
)

// BillingOnlyAckOutcome reports what AcknowledgeBillingOnly did. The zero
// value is a failure, which a later sweep retries.
type BillingOnlyAckOutcome uint8

const (
	BillingOnlyAckFailed BillingOnlyAckOutcome = iota
	// BillingOnlyAckBusy: another lifecycle action holds the lease's claim.
	BillingOnlyAckBusy
	// BillingOnlyAckSettled: the lease is no longer PENDING, so there is
	// nothing to acknowledge.
	BillingOnlyAckSettled
	// BillingOnlyAcknowledged: this call's acknowledgment committed.
	BillingOnlyAcknowledged
)

// AcknowledgeBillingOnly acknowledges a PENDING lease the bound billing policy
// classifies as billing-only, so its billing starts. Such a lease is never
// deployed, so nothing a backend reports, or fails to report, bears on it:
// unlike the deployment actions, this one reads no backend inventory, passes
// no inventory gate, and leaves any placement or attempt evidence as it is. It
// holds the lease's lifecycle claim across an authoritative chain read and
// acknowledges only a lease that read shows PENDING and billing-only, so it
// cannot acknowledge a deployable lease ahead of its workload.
func (authority *ReconciliationCoordinator) AcknowledgeBillingOnly(
	ctx context.Context,
	leaseUUID string,
) (BillingOnlyAckOutcome, error) {
	if !authority.Valid() || ctx == nil || leaseUUID == "" {
		return BillingOnlyAckFailed, errors.New("invalid billing-only acknowledgment")
	}
	claimed := authority.coordinator.operations.TryClaimLeaseNow(leaseUUID)
	if !claimed.Acquired() {
		return BillingOnlyAckBusy, nil
	}
	claim := claimed.Claim()
	defer authority.coordinator.operations.ReleaseLease(claim)
	observation := authority.controlPlane.observeLeaseBounded(ctx, leaseUUID, "")
	lease, exact := exactLeaseFromObservation(observation)
	if !exact {
		return BillingOnlyAckFailed, fmt.Errorf("read billing-only lease %s: %w",
			leaseUUID, exactLeaseObservationError(observation))
	}
	if !authority.coordinator.classify(&lease).BillingOnly() {
		return BillingOnlyAckFailed, fmt.Errorf("lease %s is not billing-only on an authoritative read", leaseUUID)
	}
	if lease.State != billingtypes.LEASE_STATE_PENDING {
		return BillingOnlyAckSettled, nil
	}
	_, acknowledged, err := authority.coordinator.acknowledgeBillingOnly(ctx, claim, lease)
	switch {
	case acknowledged:
		return BillingOnlyAcknowledged, nil
	case err == nil:
		return BillingOnlyAckSettled, nil
	default:
		return BillingOnlyAckFailed, err
	}
}

// acknowledgeBillingOnly acknowledges lease, an authoritative read of a
// PENDING billing-only lease, under the caller's lifecycle claim. It passes no
// inventory gate, since no backend report bears on a lease that is never
// deployed; only the placement runtime authority must still hold, as for every
// side effect. It returns the lease as the chain last showed it and whether it
// is now ACTIVE. A lease that ended meanwhile reports false without an error.
func (coordinator *OperationCoordinator) acknowledgeBillingOnly(
	ctx context.Context,
	claim operation.LeaseClaim,
	lease billingtypes.Lease,
) (billingtypes.Lease, bool, error) {
	if !coordinator.Valid() || coordinator.execution == nil ||
		!coordinator.operations.HoldsLeaseClaim(claim, lease.Uuid) {
		return lease, false, errors.New("a claimed billing-only acknowledgment is required")
	}
	if lease.State != billingtypes.LEASE_STATE_PENDING || !coordinator.classify(&lease).BillingOnly() {
		return lease, false, fmt.Errorf("lease %s is not a PENDING billing-only lease", lease.Uuid)
	}
	if err := coordinator.store.reattestRuntimeAuthority(); err != nil {
		return lease, false, err
	}
	control := coordinator.execution.controlPlane
	_, txHash, ackErr := control.acknowledgeLease(ctx, lease.Uuid)
	if ackErr == nil {
		slog.InfoContext(ctx, "acknowledged billing-only lease; it is billed but never deployed",
			"lease_uuid", lease.Uuid, "tenant", lease.Tenant, "tx_hash", txHash)
		lease.State = billingtypes.LEASE_STATE_ACTIVE
		return lease, true, nil
	}
	// An RPC error can follow a committed acknowledgment, so only a fresh read
	// decides.
	if current, ok := exactLeaseFromObservation(
		control.observeLeaseBounded(ctx, lease.Uuid, lease.Tenant),
	); ok {
		switch current.State {
		case billingtypes.LEASE_STATE_ACTIVE:
			return current, true, nil
		case billingtypes.LEASE_STATE_CLOSED, billingtypes.LEASE_STATE_REJECTED, billingtypes.LEASE_STATE_EXPIRED:
			return current, false, nil
		}
	}
	return lease, false, fmt.Errorf("acknowledge billing-only lease %s: %w", lease.Uuid, ackErr)
}
