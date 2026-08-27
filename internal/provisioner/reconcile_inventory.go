package provisioner

import (
	"context"
	"fmt"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

// reconcileInventory is an immutable collection result. Collection reports
// which backend inventories answered; it does not decide whether those facts
// are authoritative or mutate placement state.
type reconcileInventory struct {
	chainLeases map[string]billingtypes.Lease
	pending     int
	active      int

	fleet                       fleetSnapshot
	retentions                  map[string]string
	retentionsAnswered          answeredSet
	retentionsReportedByBackend map[string]map[string]struct{}
}

func (inventory reconcileInventory) complete() bool {
	return inventory.fleet.complete &&
		(inventory.retentionsAnswered == nil || inventory.retentionsAnswered.complete())
}

// collectInventory performs only external reads. Keeping it separate from the
// projector makes the durability boundary visible: no collected observation
// becomes lifecycle authority until the placement store accepts the complete
// projection.
func (r *Reconciler) collectInventory(ctx context.Context) (reconcileInventory, error) {
	pendingLeases, err := r.chainClient.GetPendingLeases(ctx, r.providerUUID)
	if err != nil {
		return reconcileInventory{}, fmt.Errorf("failed to get pending leases: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return reconcileInventory{}, err
	}

	activeLeases, err := r.chainClient.GetActiveLeasesByProvider(ctx, r.providerUUID)
	if err != nil {
		return reconcileInventory{}, fmt.Errorf("failed to get active leases: %w", err)
	}

	chainLeases := make(map[string]billingtypes.Lease, len(pendingLeases)+len(activeLeases))
	for _, lease := range pendingLeases {
		chainLeases[lease.Uuid] = lease
	}
	for _, lease := range activeLeases {
		chainLeases[lease.Uuid] = lease
	}

	inventory := reconcileInventory{
		chainLeases: chainLeases,
		pending:     len(pendingLeases),
		active:      len(activeLeases),
		fleet:       r.fetchFleetSnapshot(ctx),
	}
	if r.placementView != nil {
		inventory.retentions,
			inventory.retentionsAnswered,
			inventory.retentionsReportedByBackend = r.fetchAllRetentions(ctx)
	}
	return inventory, nil
}
