package provisioner

import (
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/ThreeDotsLabs/watermill/message"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
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
			SKU:      item.SkuUuid,
			Quantity: int(item.Quantity),
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

// recordWatermillMetrics records the outcome of a Watermill message handler.
func recordWatermillMetrics(topic string, err error) {
	outcome := metrics.OutcomeSuccess
	if err != nil {
		outcome = metrics.OutcomeError
	}
	metrics.WatermillMessagesTotal.WithLabelValues(topic, outcome).Inc()
}

// unmarshalMessagePayload unmarshals a Watermill message payload into the given type.
// On failure it records a malformed message metric, logs the error, and returns false.
// Callers should return nil (skip retry) when ok is false.
func unmarshalMessagePayload[T any](msg *message.Message, topic string) (T, bool) {
	var v T
	if err := json.Unmarshal(msg.Payload, &v); err != nil {
		metrics.MalformedMessagesTotal.WithLabelValues(topic).Inc()
		slog.Error("failed to unmarshal message",
			"topic", topic,
			"error", err,
			"error_type", ErrMalformedMessage,
		)
		return v, false
	}
	return v, true
}

// The following methods delegate to the HandlerSet for backwards compatibility
// with existing tests and code that calls handlers via the Manager.

// handleLeaseCreated delegates to the handler set.
func (m *Manager) handleLeaseCreated(msg *message.Message) error {
	return m.handlers.HandleLeaseCreated(msg)
}

// handleLeaseClosed delegates to the handler set.
func (m *Manager) handleLeaseClosed(msg *message.Message) error {
	return m.handlers.HandleLeaseClosed(msg)
}

// handleLeaseExpired delegates to the handler set.
func (m *Manager) handleLeaseExpired(msg *message.Message) error {
	return m.handlers.HandleLeaseExpired(msg)
}

// handleBackendCallback delegates to the handler set.
func (m *Manager) handleBackendCallback(msg *message.Message) error {
	return m.handlers.HandleBackendCallback(msg)
}

// handlePayloadReceived delegates to the handler set.
func (m *Manager) handlePayloadReceived(msg *message.Message) error {
	return m.handlers.HandlePayloadReceived(msg)
}
