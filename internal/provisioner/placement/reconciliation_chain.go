package placement

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

// ReconciliationChain is the single chain capability bound to one
// ReconciliationCoordinator. Both the non-atomic fleet inventory and every
// exact read or terminal write are therefore issued through the same client;
// NewReconciler cannot accidentally splice inventory from one node with exact
// authority from another.
type ReconciliationChain interface {
	PruneLeaseReader
	GetPendingLeases(context.Context, string) ([]billingtypes.Lease, error)
	GetActiveLeasesByProvider(context.Context, string) ([]billingtypes.Lease, error)
	RejectLeases(context.Context, []string, string) (uint64, []string, error)
	CloseLeases(context.Context, []string, string) (uint64, []string, error)
}

// CollectChainInventory returns one all-or-nothing PENDING/ACTIVE inventory.
// The provider identity is derived from the Store and both list operations get
// independent budgets, so one slow endpoint cannot consume the other's chance
// to answer.
func (authority *ReconciliationCoordinator) CollectChainInventory(
	ctx context.Context,
	budget time.Duration,
) (pending, active []billingtypes.Lease, err error) {
	if !authority.Valid() || ctx == nil || budget <= 0 {
		return nil, nil, errors.New("valid reconciliation coordinator, context, and budget are required")
	}
	pendingCtx, cancelPending := context.WithTimeout(ctx, budget)
	activeCtx, cancelActive := context.WithTimeout(ctx, budget)
	var pendingErr, activeErr error
	var reads sync.WaitGroup
	reads.Go(func() {
		defer cancelPending()
		pending, pendingErr = authority.controlPlane.inventoryLeases(
			pendingCtx, billingtypes.LEASE_STATE_PENDING,
		)
	})
	reads.Go(func() {
		defer cancelActive()
		active, activeErr = authority.controlPlane.inventoryLeases(
			activeCtx, billingtypes.LEASE_STATE_ACTIVE,
		)
	})
	reads.Wait()

	var inventoryErrors []error
	if pendingErr != nil {
		inventoryErrors = append(inventoryErrors,
			fmt.Errorf("failed to get pending leases: %w", pendingErr))
	}
	if activeErr != nil {
		inventoryErrors = append(inventoryErrors,
			fmt.Errorf("failed to get active leases: %w", activeErr))
	}
	if len(inventoryErrors) > 0 {
		return nil, nil, errors.Join(inventoryErrors...)
	}
	pendingIDs := make(map[string]struct{}, len(pending))
	for _, lease := range pending {
		pendingIDs[lease.Uuid] = struct{}{}
	}
	for _, lease := range active {
		if _, duplicate := pendingIDs[lease.Uuid]; duplicate {
			return nil, nil, fmt.Errorf(
				"lease %s appears in both PENDING and ACTIVE provider inventory", lease.Uuid,
			)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	return pending, active, nil
}

// ReadLease performs an observational exact read through the same chain
// capability that produced the sweep inventory. Callers remain responsible for
// applying their operation-specific timeout.
func (authority *ReconciliationCoordinator) ReadLease(
	ctx context.Context,
	leaseUUID string,
) (*billingtypes.Lease, error) {
	if !authority.Valid() || ctx == nil || leaseUUID == "" {
		return nil, errors.New("valid reconciliation coordinator, context, and lease UUID are required")
	}
	observation := authority.controlPlane.observeLease(ctx, leaseUUID, "")
	lease, exact := exactLeaseFromObservation(observation)
	if !exact {
		return nil, exactLeaseObservationError(observation)
	}
	return &lease, nil
}

// RejectObserved rejects the exact PENDING lease carried by action. A caller
// cannot use reconciliation authority to reject an arbitrary UUID or an ACTIVE
// lease.
func (authority *ReconciliationCoordinator) RejectObserved(
	ctx context.Context,
	action ObservedReconciliationAction,
	reason string,
) (uint64, []string, error) {
	if !authority.Valid() || ctx == nil || !action.validFor(authority) ||
		action.lease.State != billingtypes.LEASE_STATE_PENDING {
		return 0, nil, errors.New("a current observed PENDING reconciliation action is required")
	}
	if err := authority.coordinator.store.leaseSideEffectError(action.lease.Uuid); err != nil {
		return 0, nil, err
	}
	return authority.controlPlane.rejectLease(ctx, action.lease.Uuid, reason)
}

// CloseObserved closes the exact ACTIVE lease carried by action. A caller
// cannot use reconciliation authority to close an arbitrary UUID or a PENDING
// lease.
func (authority *ReconciliationCoordinator) CloseObserved(
	ctx context.Context,
	action ObservedReconciliationAction,
	reason string,
) (uint64, []string, error) {
	if !authority.Valid() || ctx == nil || !action.validFor(authority) ||
		action.lease.State != billingtypes.LEASE_STATE_ACTIVE {
		return 0, nil, errors.New("a current observed ACTIVE reconciliation action is required")
	}
	if err := authority.coordinator.store.leaseSideEffectError(action.lease.Uuid); err != nil {
		return 0, nil, err
	}
	return authority.controlPlane.closeLease(ctx, action.lease.Uuid, reason)
}

// AcknowledgeObserved acknowledges only the exact PENDING action minted by the
// current projected sweep through the construction-bound control plane.
func (authority *ReconciliationCoordinator) AcknowledgeObserved(
	ctx context.Context,
	action ObservedReconciliationAction,
) (bool, string, error) {
	if !authority.Valid() || ctx == nil || !action.validFor(authority) ||
		action.lease.State != billingtypes.LEASE_STATE_PENDING {
		return false, "", errors.New("a current observed PENDING reconciliation action is required")
	}
	if err := authority.coordinator.store.leaseSideEffectError(action.lease.Uuid); err != nil {
		return false, "", err
	}
	return authority.controlPlane.acknowledgeLease(ctx, action.lease.Uuid)
}
