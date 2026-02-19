package provisioner

import (
	"errors"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
)

// maxRejectReasonLen is the maximum length for on-chain lease rejection reasons.
// The billing module enforces a 256-character limit.
const maxRejectReasonLen = 256

// truncateRejectReason truncates a rejection reason to fit the on-chain limit.
func truncateRejectReason(reason string) string {
	if len(reason) > maxRejectReasonLen {
		return reason[:maxRejectReasonLen-3] + "..."
	}
	return reason
}

// Deterministic rejection reasons for on-chain lease rejection.
// These hardcoded strings prevent dynamic data (SKU names, image registries,
// file paths, JSON bodies) from leaking on-chain.
const (
	rejectReasonInvalidSKU       = "invalid SKU"
	rejectReasonInvalidManifest  = "invalid manifest"
	rejectReasonImageNotAllowed  = "image not allowed"
	rejectReasonValidationError  = "validation error"
	rejectReasonPayloadCorrupted = "payload corrupted"
)

// validationErrorToRejectReason maps a validation error to a hardcoded
// rejection reason safe for on-chain surfacing.
func validationErrorToRejectReason(err error) string {
	switch {
	case errors.Is(err, backend.ErrUnknownSKU):
		return rejectReasonInvalidSKU
	case errors.Is(err, backend.ErrInvalidManifest):
		return rejectReasonInvalidManifest
	case errors.Is(err, backend.ErrImageNotAllowed):
		return rejectReasonImageNotAllowed
	default:
		return rejectReasonValidationError
	}
}

// isTerminalAcknowledgeError returns true if the error indicates the lease
// cannot be acknowledged and retrying won't help. This includes cases where
// the lease is already acknowledged (not in PENDING state).
func isTerminalAcknowledgeError(err error) bool {
	if err == nil {
		return false
	}
	// Check against specific billing module errors using errors.Is().
	// ErrLeaseNotPending: lease is already ACTIVE or in another terminal state.
	// ErrLeaseNotFound: lease doesn't exist (may have been deleted).
	return errors.Is(err, billingtypes.ErrLeaseNotPending) ||
		errors.Is(err, billingtypes.ErrLeaseNotFound)
}

// ExtractRoutingSKU returns a SKU UUID from the lease for backend routing.
//
// Why this exists: A lease may contain multiple items with different SKUs,
// but all items are guaranteed to belong to the same provider (enforced by
// the chain). Therefore, any SKU can be used to determine which backend
// should handle the request. We use the first item's SKU by convention.
//
// This should NOT be used for resource allocation - use ExtractLeaseItems()
// to get the full list of items with their quantities.
func ExtractRoutingSKU(lease *billingtypes.Lease) string {
	if lease == nil || len(lease.Items) == 0 {
		return ""
	}
	return lease.Items[0].SkuUuid
}

// ExtractLeaseItems converts chain lease items to backend lease items.
func ExtractLeaseItems(lease *billingtypes.Lease) []backend.LeaseItem {
	if lease == nil || len(lease.Items) == 0 {
		return nil
	}
	items := make([]backend.LeaseItem, len(lease.Items))
	for i, item := range lease.Items {
		items[i] = backend.LeaseItem{
			SKU:         item.SkuUuid,
			Quantity:    int(item.Quantity),
			ServiceName: item.ServiceName,
		}
	}
	return items
}

// TotalLeaseQuantity returns the total quantity across all lease items.
func TotalLeaseQuantity(lease *billingtypes.Lease) int {
	if lease == nil {
		return 0
	}
	total := 0
	for _, item := range lease.Items {
		total += int(item.Quantity)
	}
	return total
}
