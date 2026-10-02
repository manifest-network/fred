package provisioner

import (
	"context"
	"fmt"
	"log/slog"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/provisioner/terminalverdict"
)

// Closing an ACTIVE lease on chain cannot be undone and stops a paying
// tenant. closeActiveLeaseOnChain, the raw chain sink, is called only from
// this file: a forbidigo rule (`\.closeActiveLeaseOnChain$`) excludes this
// file alone. Each wrapper below is one closing reason with its own evidence,
// so a new code path cannot close a lease with an arbitrary reason or without
// that evidence. The failure-budget close takes the sealed terminalverdict
// proof end to end (ENG-799).

// closeActiveLeaseOnChain closes an ACTIVE lease on chain with a reason. Only
// the typed wrappers in this file may call it.
func (r *Reconciler) closeActiveLeaseOnChain(
	ctx context.Context,
	action placement.ObservedReconciliationAction,
	reason string,
) error {
	leaseUUID := action.Lease().Uuid
	closed, txHashes, err := r.coordinator.CloseObserved(ctx, action, reason)
	if err != nil {
		return err
	}

	r.cleanupTerminalLease(leaseUUID)

	slog.Info("reconcile: closed lease",
		"lease_uuid", leaseUUID,
		"closed", closed,
		"tx_hashes", txHashes,
		"reason", reason,
	)

	return nil
}

// lostBackendChainReason is the fixed on-chain reason for a lease whose data
// lived on a retired backend's lost storage. It names no backend.
const lostBackendChainReason = "backend storage lost"

// failureBudgetChainReason is the fixed on-chain reason for closing a lease
// whose own workload failed consecutively (ENG-799). It names no backend and
// embeds no count, so nothing backend-authored reaches the chain.
const failureBudgetChainReason = "workload failed repeatedly"

// closeExhaustedLease is the failure-budget close. It accepts only the sealed
// proof that complete inventory reported an exhausted verdict for this exact
// lease; the planner mints none from FailCount or any other count. The proof is
// re-checked here as defense in depth before the irreversible close.
func (r *Reconciler) closeExhaustedLease(
	ctx context.Context,
	action placement.ObservedReconciliationAction,
	exhaustion terminalverdict.Exhaustion,
) error {
	leaseUUID := action.Lease().Uuid
	if !exhaustion.Valid() || exhaustion.LeaseUUID() != leaseUUID ||
		action.Lease().State != billingtypes.LEASE_STATE_ACTIVE {
		return fmt.Errorf("refusing failure-budget close of lease %q without its exhausted verdict", leaseUUID)
	}
	return r.closeActiveLeaseOnChain(ctx, action, failureBudgetChainReason)
}

// closeLostLease closes an ACTIVE lease whose placement was lost with its
// retired backend's storage, with the fixed reason. The caller holds the
// action it observed under the lost-lease lifecycle claim.
func (r *Reconciler) closeLostLease(
	ctx context.Context,
	action placement.ObservedReconciliationAction,
) error {
	return r.closeActiveLeaseOnChain(ctx, action, lostBackendChainReason)
}

// closeRefusedLease closes an ACTIVE lease whose re-provision the backend
// refused with a sealed validation error, with the reason derived from that
// refusal. ENG-799 leaves this path unchanged; giving it a typed proof of its
// own is ENG-800's scope.
func (r *Reconciler) closeRefusedLease(
	ctx context.Context,
	action placement.ObservedReconciliationAction,
	reason string,
) error {
	return r.closeActiveLeaseOnChain(ctx, action, reason)
}
