package backend

import "fmt"

// MaxOperationQuantity is the maximum aggregate number of substrate instances
// an asynchronously accepted provision or restore operation may describe.
// It bounds all pre-admission allocations and recovery loops that are derived
// from tenant-controlled lease items.
const MaxOperationQuantity = 1024

// ValidateOperationQuantities validates and sums lease-item quantities without
// integer overflow. Keeping the aggregate proof beside the wire type lets every
// backend and the durable operation journal enforce the same bound before any
// allocation, substrate loop, or asynchronous acceptance.
func ValidateOperationQuantities(items []LeaseItem) (int, error) {
	if len(items) == 0 {
		return 0, fmt.Errorf("%w: operation has no lease items", ErrValidation)
	}
	total := 0
	for i, item := range items {
		if item.Quantity <= 0 || item.Quantity > MaxOperationQuantity {
			return 0, fmt.Errorf(
				"%w: item %d (SKU %q, service %q) quantity %d out of range [1, %d]",
				ErrValidation, i, item.SKU, item.ServiceName, item.Quantity, MaxOperationQuantity,
			)
		}
		// Subtraction is safe because item.Quantity is already in [1, max].
		// This check cannot overflow even if a corrupt/requested item slice is
		// large enough that a naive sum would wrap int.
		if total > MaxOperationQuantity-item.Quantity {
			return 0, fmt.Errorf(
				"%w: total quantity exceeds maximum %d",
				ErrValidation, MaxOperationQuantity,
			)
		}
		total += item.Quantity
	}
	return total, nil
}
