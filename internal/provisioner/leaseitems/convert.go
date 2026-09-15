// Package leaseitems converts authoritative chain lease items into the
// backend protocol representation. Keeping the conversion in a leaf package
// lets both provisioning and restore use the same contract without creating a
// package cycle.
package leaseitems

import (
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
)

// FromLease returns a detached backend representation of every lease item.
func FromLease(lease *billingtypes.Lease) []backend.LeaseItem {
	if lease == nil || len(lease.Items) == 0 {
		return nil
	}
	items := make([]backend.LeaseItem, len(lease.Items))
	for index, item := range lease.Items {
		items[index] = backend.LeaseItem{
			SKU:          item.SkuUuid,
			Quantity:     int(item.Quantity),
			ServiceName:  item.ServiceName,
			CustomDomain: item.CustomDomain,
		}
	}
	return items
}
