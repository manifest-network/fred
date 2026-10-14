package provisioner

import (
	"errors"
	"unicode/utf8"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
)

// maxRejectReasonLen is the maximum byte length for on-chain lease rejection
// reasons. The billing module enforces len(reason) <= 256 in ValidateBasic.
const maxRejectReasonLen = 256

// truncateRejectReason truncates a rejection reason to fit the on-chain byte
// limit while avoiding splitting multi-byte UTF-8 runes.
func truncateRejectReason(reason string) string {
	if len(reason) <= maxRejectReasonLen {
		return reason
	}
	// Leave room for the "..." suffix.
	limit := maxRejectReasonLen - 3
	// Back up to a rune boundary so we don't split a multi-byte sequence.
	for limit > 0 && !utf8.RuneStart(reason[limit]) {
		limit--
	}
	return reason[:limit] + "..."
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

// BillingOnlySKUs is the set of on-chain SKU UUIDs the provider bills for but
// never deploys (config billing_only_skus). The service behind such a SKU is
// delivered outside Fred, so a lease that uses only these SKUs is acknowledged
// as soon as providerd sees it, which starts its billing, and is never
// provisioned, rejected, or closed for a provisioning reason. Billing applies
// to it like any other lease: provider-wide withdrawals collect what it
// accrues, and the credit check closes it once its tenant's credit is
// exhausted.
//
// The nil set is empty: no lease is billing-only.
type BillingOnlySKUs map[string]struct{}

// NewBillingOnlySKUs builds the set from the validated config list.
func NewBillingOnlySKUs(skuUUIDs []string) BillingOnlySKUs {
	if len(skuUUIDs) == 0 {
		return nil
	}
	set := make(BillingOnlySKUs, len(skuUUIDs))
	for _, skuUUID := range skuUUIDs {
		set[skuUUID] = struct{}{}
	}
	return set
}

// Covers reports whether every item of the lease uses a billing-only SKU. A
// lease with no items is not billing-only. A lease that mixes billing-only and
// deployable SKUs is not billing-only either: it takes the normal provisioning
// path, where the backend refuses the SKU it cannot serve.
func (s BillingOnlySKUs) Covers(lease *billingtypes.Lease) bool {
	if len(s) == 0 || lease == nil || len(lease.Items) == 0 {
		return false
	}
	for _, item := range lease.Items {
		if _, ok := s[item.SkuUuid]; !ok {
			return false
		}
	}
	return true
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
			SKU:          item.SkuUuid,
			Quantity:     int(item.Quantity),
			ServiceName:  item.ServiceName,
			CustomDomain: item.CustomDomain,
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
