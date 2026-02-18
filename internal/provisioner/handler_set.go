package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

// HandlerDeps contains the dependencies needed by the handler set.
type HandlerDeps struct {
	ChainClient  ChainClient
	Orchestrator *ProvisionOrchestrator
	Tracker      InFlightTracker
	Acknowledger Acknowledger
	PayloadStore *payload.Store
	Publisher    message.Publisher // For publishing to TopicLeaseEvent (optional)
}

// HandlerSet contains the Watermill message handlers for the provisioner.
// It encapsulates all handler methods and their dependencies.
type HandlerSet struct {
	deps HandlerDeps
}

// NewHandlerSet creates a new HandlerSet with the given dependencies.
func NewHandlerSet(deps HandlerDeps) *HandlerSet {
	return &HandlerSet{deps: deps}
}

// rejectOnValidationError rejects a lease on chain after a validation error.
// Returns nil on success, or an error to trigger Watermill retry on rejection failure.
func (h *HandlerSet) rejectOnValidationError(ctx context.Context, lease *billingtypes.Lease, err error) error {
	slog.Warn("provisioning failed with validation error, rejecting lease",
		"lease_uuid", lease.Uuid,
		"tenant", lease.Tenant,
		"error", err,
	)
	reason := validationErrorToRejectReason(err)
	_, _, rejectErr := h.deps.ChainClient.RejectLeases(ctx, []string{lease.Uuid}, reason)
	if rejectErr != nil {
		slog.Error("failed to reject lease after validation error",
			"lease_uuid", lease.Uuid,
			"error", rejectErr,
		)
		return fmt.Errorf("failed to reject lease %s after validation error: %w", lease.Uuid, rejectErr)
	}
	h.publishLeaseEvent(lease.Uuid, backend.ProvisionStatusFailed, reason)
	return nil
}

// HandleLeaseCreated processes new lease events.
func (h *HandlerSet) HandleLeaseCreated(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicLeaseCreated, err) }()

	event, ok := unmarshalMessagePayload[chain.LeaseEvent](msg, TopicLeaseCreated)
	if !ok {
		return nil
	}

	// Fetch lease details from chain to get SKU for routing
	lease, err := h.deps.ChainClient.GetLease(msg.Context(), event.LeaseUUID)
	if err != nil {
		slog.Error("failed to fetch lease details",
			"lease_uuid", event.LeaseUUID,
			"error", err,
		)
		return fmt.Errorf("failed to fetch lease %s: %w", event.LeaseUUID, err)
	}
	if lease == nil {
		slog.Warn("lease not found, skipping",
			"lease_uuid", event.LeaseUUID,
			"tenant", event.Tenant,
		)
		return nil
	}

	// Check if lease requires a payload (has MetaHash)
	// If so, skip immediate provisioning - wait for payload upload
	if len(lease.MetaHash) > 0 {
		metrics.LeasesAwaitingPayloadTotal.Inc()
		slog.Info("lease requires payload, awaiting upload",
			"lease_uuid", event.LeaseUUID,
			"tenant", event.Tenant,
			"meta_hash_hex", fmt.Sprintf("%x", lease.MetaHash),
		)
		return nil // Don't provision yet - wait for payload
	}

	// Start provisioning without payload
	err = h.deps.Orchestrator.StartProvisioning(msg.Context(), lease, ProvisionOpts{})
	if err != nil {
		if errors.Is(err, backend.ErrValidation) {
			return h.rejectOnValidationError(msg.Context(), lease, err)
		}
		return err
	}

	h.publishLeaseEvent(event.LeaseUUID, backend.ProvisionStatusProvisioning, "")
	return nil
}

// HandleLeaseClosed processes lease closure events.
func (h *HandlerSet) HandleLeaseClosed(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicLeaseClosed, err) }()
	return h.processLeaseClose(msg, TopicLeaseClosed)
}

// HandleLeaseExpired processes lease expiration events.
// Same logic as HandleLeaseClosed but records metrics under the correct topic.
func (h *HandlerSet) HandleLeaseExpired(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicLeaseExpired, err) }()
	return h.processLeaseClose(msg, TopicLeaseExpired)
}

// processLeaseClose is the shared implementation for HandleLeaseClosed and HandleLeaseExpired.
func (h *HandlerSet) processLeaseClose(msg *message.Message, topic string) error {
	event, ok := unmarshalMessagePayload[chain.LeaseEvent](msg, topic)
	if !ok {
		return nil
	}

	slog.Info("processing lease close", "lease_uuid", event.LeaseUUID, "tenant", event.Tenant, "topic", topic)

	// Clean up any stored payload for this lease.
	// This handles the case where a tenant uploaded a payload but canceled the lease
	// before provisioning started, or any other scenario where payload exists but
	// the lease is no longer valid.
	if h.deps.PayloadStore != nil {
		if exists, err := h.deps.PayloadStore.Has(event.LeaseUUID); err != nil {
			slog.Warn("failed to check payload store during lease close",
				"lease_uuid", event.LeaseUUID,
				"error", err,
			)
		} else if exists {
			h.deps.PayloadStore.Delete(event.LeaseUUID)
			slog.Info("cleaned up stored payload for closed lease",
				"lease_uuid", event.LeaseUUID,
				"tenant", event.Tenant,
			)
		}
	}

	// Get SKU hint from chain for routing if lease is not in-flight.
	// The orchestrator will try in-flight tracking first, then fall back to SKU hint.
	var skuHint string
	lease, err := h.deps.ChainClient.GetLease(msg.Context(), event.LeaseUUID)
	if err != nil {
		slog.Warn("failed to fetch lease details for deprovision routing",
			"lease_uuid", event.LeaseUUID,
			"error", err,
		)
		// Continue without SKU hint - orchestrator will try all backends
	} else if lease != nil {
		skuHint = ExtractRoutingSKU(lease)
	}

	// Delegate to orchestrator for deprovisioning
	return h.deps.Orchestrator.Deprovision(msg.Context(), event.LeaseUUID, skuHint)
}

// HandleBackendCallback processes callbacks from backends.
func (h *HandlerSet) HandleBackendCallback(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicBackendCallback, err) }()

	callback, ok := unmarshalMessagePayload[backend.CallbackPayload](msg, TopicBackendCallback)
	if !ok {
		return nil
	}

	// Check if this lease is in-flight (idempotency check).
	// Non-in-flight callbacks are expected for restart/update operations, which
	// don't register in the in-flight tracker (the lease is already ACTIVE).
	// For these, we still publish the status event so WebSocket clients see
	// the ready/failed transition, but skip chain operations.
	provision, exists := h.deps.Tracker.GetInFlight(callback.LeaseUUID)
	if !exists {
		switch callback.Status {
		case backend.CallbackStatusSuccess:
			h.publishLeaseEvent(callback.LeaseUUID, backend.ProvisionStatusReady, "")
		case backend.CallbackStatusFailed:
			h.publishLeaseEvent(callback.LeaseUUID, backend.ProvisionStatusFailed, callback.Error)
		default:
			slog.Warn("unexpected callback status for non-in-flight lease",
				"lease_uuid", callback.LeaseUUID,
				"status", callback.Status,
			)
			return nil
		}
		slog.Info("published event for non-in-flight callback (restart/update)",
			"lease_uuid", callback.LeaseUUID,
			"status", callback.Status,
		)
		return nil
	}

	slog.Info("processing backend callback",
		"lease_uuid", callback.LeaseUUID,
		"tenant", provision.Tenant,
		"status", callback.Status,
		"backend", provision.Backend,
	)

	// Record provisioning duration if we have the start time
	recordDuration := func() {
		if !provision.StartTime.IsZero() {
			duration := time.Since(provision.StartTime).Seconds()
			metrics.ProvisioningDuration.WithLabelValues(provision.Backend).Observe(duration)
		}
	}

	switch callback.Status {
	case backend.CallbackStatusSuccess:
		// Acknowledge the lease on chain via batcher to avoid sequence mismatch errors
		acknowledged, txHash, err := h.deps.Acknowledger.Acknowledge(msg.Context(), callback.LeaseUUID)
		if err != nil {
			// Check if this is a terminal error (e.g., lease already acknowledged)
			if isTerminalAcknowledgeError(err) {
				// Lease is already in a non-PENDING state (likely already ACTIVE).
				// This can happen if we received a duplicate callback or the reconciler
				// already acknowledged it. Treat as success - the lease is active.
				h.deps.Tracker.UntrackInFlight(callback.LeaseUUID)
				recordDuration()
				metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeSuccess, provision.Backend).Inc()
				slog.Info("lease already acknowledged, skipping",
					"lease_uuid", callback.LeaseUUID,
					"tenant", provision.Tenant,
				)
				return nil
			}

			slog.Error("failed to acknowledge lease",
				"lease_uuid", callback.LeaseUUID,
				"tenant", provision.Tenant,
				"error", err,
			)
			// Keep in-flight tracking for retry - Watermill will retry this message
			return fmt.Errorf("%w: lease %s: %w", ErrAcknowledgeFailed, callback.LeaseUUID, err)
		}

		// Only remove from in-flight after successful acknowledgment
		h.deps.Tracker.UntrackInFlight(callback.LeaseUUID)
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeSuccess, provision.Backend).Inc()

		// Payload is intentionally NOT deleted here. The lease is now ACTIVE
		// but the container could crash later, requiring re-provisioning with
		// the same manifest. Payload cleanup happens when the lease is closed
		// (HandleLeaseClosed) or rejected (failure callback above).

		h.publishLeaseEvent(callback.LeaseUUID, backend.ProvisionStatusReady, "")

		slog.Info("lease acknowledged after provisioning",
			"lease_uuid", callback.LeaseUUID,
			"tenant", provision.Tenant,
			"acknowledged", acknowledged,
			"tx_hash", txHash,
		)

	case backend.CallbackStatusFailed:
		reason := callback.Error
		if reason == "" {
			reason = "provisioning failed"
		}

		// Check if this is a re-provision of an ACTIVE lease. Rejecting only
		// applies to PENDING leases. For ACTIVE leases, just untrack and let
		// the reconciler handle it (it will retry or reject based on FailCount).
		lease, err := h.deps.ChainClient.GetLease(msg.Context(), callback.LeaseUUID)
		if err != nil {
			slog.Error("failed to fetch lease state for failure callback, keeping in-flight",
				"lease_uuid", callback.LeaseUUID,
				"error", err,
			)
			return fmt.Errorf("failed to fetch lease %s: %w", callback.LeaseUUID, err)
		}
		if lease != nil && lease.State == billingtypes.LEASE_STATE_ACTIVE {
			// Lease is ACTIVE — this was a re-provision attempt. Untrack and
			// let the reconciler detect the still-failed backend state.
			h.deps.Tracker.UntrackInFlight(callback.LeaseUUID)
			recordDuration()
			metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeFailed, provision.Backend).Inc()

			h.publishLeaseEvent(callback.LeaseUUID, backend.ProvisionStatusFailed, reason)

			slog.Warn("re-provision failed for active lease, deferring to reconciler",
				"lease_uuid", callback.LeaseUUID,
				"tenant", provision.Tenant,
				"reason", reason,
			)
			return nil
		}

		// PENDING lease — reject on chain FIRST, before untracking.
		// This prevents a race where the reconciler sees a PENDING lease that's
		// not in-flight and tries to provision it again.
		rejected, txHashes, err := h.deps.ChainClient.RejectLeases(msg.Context(), []string{callback.LeaseUUID}, truncateRejectReason(reason))
		if err != nil {
			// Keep in-flight so reconciler doesn't try to re-provision.
			// The timeout checker or next reconciliation will retry.
			slog.Error("failed to reject lease after provisioning failure, keeping in-flight",
				"lease_uuid", callback.LeaseUUID,
				"tenant", provision.Tenant,
				"error", err,
			)
			// Return error to trigger Watermill retry
			return fmt.Errorf("failed to reject lease %s: %w", callback.LeaseUUID, err)
		}

		// Only untrack AFTER successful rejection
		h.deps.Tracker.UntrackInFlight(callback.LeaseUUID)
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeFailed, provision.Backend).Inc()

		// Clean up payload and placement after successful rejection.
		// Placement was recorded when the backend accepted the provision
		// request, but rejected leases don't emit a close event, so
		// Deprovision (which normally cleans up placement) is never called.
		if h.deps.PayloadStore != nil {
			h.deps.PayloadStore.Delete(callback.LeaseUUID)
		}
		h.deps.Orchestrator.DeletePlacement(callback.LeaseUUID)

		h.publishLeaseEvent(callback.LeaseUUID, backend.ProvisionStatusFailed, reason)

		slog.Info("lease rejected after provisioning failure",
			"lease_uuid", callback.LeaseUUID,
			"tenant", provision.Tenant,
			"rejected", rejected,
			"tx_hashes", txHashes,
			"reason", reason,
		)

	default:
		// Unknown status is treated as terminal to prevent leases from being stuck
		// in the in-flight map indefinitely. The reconciler will pick up the lease
		// and handle it based on its actual chain/backend state.
		h.deps.Tracker.UntrackInFlight(callback.LeaseUUID)
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeError, provision.Backend).Inc()

		slog.Warn("unknown callback status, treating as terminal",
			"lease_uuid", callback.LeaseUUID,
			"tenant", provision.Tenant,
			"status", callback.Status,
		)
	}

	return nil
}

// HandlePayloadReceived processes payload upload events.
// This triggers provisioning for leases that were waiting for a payload.
func (h *HandlerSet) HandlePayloadReceived(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicPayloadReceived, err) }()

	// Guard against nil payloadStore - this shouldn't happen in normal operation
	// since payload events are only published after successful storage, but
	// handle it gracefully for robustness.
	if h.deps.PayloadStore == nil {
		slog.Error("payload store not configured, cannot process payload event")
		return nil // Don't retry - configuration issue
	}

	event, ok := unmarshalMessagePayload[payload.Event](msg, TopicPayloadReceived)
	if !ok {
		return nil
	}

	slog.Info("processing payload received",
		"lease_uuid", event.LeaseUUID,
		"tenant", event.Tenant,
	)

	// Fetch lease details from chain to get SKU for routing
	lease, err := h.deps.ChainClient.GetLease(msg.Context(), event.LeaseUUID)
	if err != nil {
		slog.Error("failed to fetch lease details",
			"lease_uuid", event.LeaseUUID,
			"error", err,
		)
		return fmt.Errorf("failed to fetch lease %s: %w", event.LeaseUUID, err)
	}
	if lease == nil {
		slog.Warn("lease not found, cleaning up payload",
			"lease_uuid", event.LeaseUUID,
			"tenant", event.Tenant,
		)
		h.deps.PayloadStore.Delete(event.LeaseUUID)
		return nil
	}

	// Verify lease is still pending
	if lease.State != billingtypes.LEASE_STATE_PENDING {
		slog.Warn("lease is no longer pending, skipping provisioning",
			"lease_uuid", event.LeaseUUID,
			"tenant", event.Tenant,
			"state", lease.State.String(),
		)
		// Clean up the stored payload
		h.deps.PayloadStore.Delete(event.LeaseUUID)
		return nil
	}

	// Get the payload from the store WITHOUT removing it yet.
	// We only delete after Provision() succeeds to allow retries.
	// Note: Payload is NOT deleted here. It will be deleted by HandleBackendCallback
	// after the backend reports success or failure. This ensures the payload remains
	// available for retry if the backend fails or crashes before sending a callback.
	payloadData, err := h.deps.PayloadStore.Get(event.LeaseUUID)
	if err != nil {
		slog.Error("failed to read payload from store",
			"lease_uuid", event.LeaseUUID,
			"error", err,
		)
		return fmt.Errorf("payload store read error: %w", err)
	}
	if payloadData == nil {
		// This shouldn't happen in normal operation since payload is stored
		// before publishing the event, but handle it gracefully
		slog.Warn("payload not found in store, proceeding without payload",
			"lease_uuid", event.LeaseUUID,
			"tenant", event.Tenant,
		)
	} else if event.MetaHashHex != "" {
		// Re-verify payload hash before provisioning to catch any corruption.
		// The payload was validated on upload, but disk corruption could occur.
		if err := payload.VerifyHashHex(payloadData, event.MetaHashHex); err != nil {
			h.deps.PayloadStore.Delete(event.LeaseUUID)
			slog.Error("payload hash mismatch - possible corruption",
				"lease_uuid", event.LeaseUUID,
				"error", err,
			)
			return err
		}
	}

	// Start provisioning with payload
	err = h.deps.Orchestrator.StartProvisioning(msg.Context(), lease, ProvisionOpts{
		Payload:     payloadData,
		PayloadHash: event.MetaHashHex,
	})
	if err != nil {
		if errors.Is(err, backend.ErrValidation) {
			h.deps.PayloadStore.Delete(event.LeaseUUID)
			return h.rejectOnValidationError(msg.Context(), lease, err)
		}
		return err
	}
	return nil
}

// publishLeaseEvent publishes a LeaseStatusEvent to TopicLeaseEvent for real-time delivery.
// Best-effort: errors are logged but do not affect the handler's return value.
func (h *HandlerSet) publishLeaseEvent(leaseUUID string, status backend.ProvisionStatus, errMsg string) {
	if h.deps.Publisher == nil {
		return
	}

	event := backend.LeaseStatusEvent{
		LeaseUUID: leaseUUID,
		Status:    status,
		Error:     errMsg,
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(event)
	if err != nil {
		slog.Warn("failed to marshal lease event", "lease_uuid", leaseUUID, "error", err)
		return
	}

	msg := message.NewMessage(watermill.NewUUID(), data)
	if err := h.deps.Publisher.Publish(TopicLeaseEvent, msg); err != nil {
		slog.Warn("failed to publish lease event", "lease_uuid", leaseUUID, "error", err)
	}
}
