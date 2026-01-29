package provisioner

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/metrics"
)

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

// ExtractPrimarySKU returns the SKU UUID from the first lease item.
// For multi-SKU leases, we route based on the first SKU (all SKUs in a lease
// must belong to the same provider, so routing by the first is sufficient).
func ExtractPrimarySKU(lease *billingtypes.Lease) string {
	if lease == nil || len(lease.Items) == 0 {
		return ""
	}
	return lease.Items[0].SkuUuid
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

// handleLeaseCreated processes new lease events.
func (m *Manager) handleLeaseCreated(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicLeaseCreated, err) }()

	event, ok := unmarshalMessagePayload[chain.LeaseEvent](msg, TopicLeaseCreated)
	if !ok {
		return nil
	}

	// Fetch lease details from chain to get SKU for routing
	lease, err := m.chainClient.GetLease(msg.Context(), event.LeaseUUID)
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
		)
		return nil
	}

	// Log lease details for debugging
	hasMetaHash := len(lease.MetaHash) > 0
	slog.Info("processing lease created",
		"lease_uuid", event.LeaseUUID,
		"tenant", event.Tenant,
		"has_meta_hash", hasMetaHash,
		"meta_hash_len", len(lease.MetaHash),
	)

	// Check if lease requires a payload (has MetaHash)
	// If so, skip immediate provisioning - wait for payload upload
	if hasMetaHash {
		metrics.LeasesAwaitingPayloadTotal.Inc()
		slog.Info("lease requires payload, awaiting upload",
			"lease_uuid", event.LeaseUUID,
			"tenant", event.Tenant,
			"meta_hash_hex", fmt.Sprintf("%x", lease.MetaHash),
		)
		return nil // Don't provision yet - wait for payload
	}

	// Extract primary SKU from lease items for routing
	sku := ExtractPrimarySKU(lease)

	// Route to appropriate backend based on SKU (Route already falls back to default)
	backendClient := m.router.Route(sku)
	if backendClient == nil {
		slog.Error("no backend available for provisioning",
			"lease_uuid", event.LeaseUUID,
			"sku", sku,
		)
		return fmt.Errorf("%w: lease %s", ErrNoBackendAvailable, event.LeaseUUID)
	}

	// Atomically track in-flight BEFORE calling Provision to prevent:
	// 1. Race with reconciler (TOCTOU between IsInFlight check and TrackInFlight)
	// 2. Race with fast backend response (callback arriving before tracking)
	if !m.TryTrackInFlight(event.LeaseUUID, event.Tenant, sku, backendClient.Name()) {
		slog.Debug("lease already in-flight, skipping",
			"lease_uuid", event.LeaseUUID,
		)
		return nil
	}

	// Start provisioning (async - backend will call back)
	err = backendClient.Provision(msg.Context(), backend.ProvisionRequest{
		LeaseUUID:    event.LeaseUUID,
		Tenant:       event.Tenant,
		ProviderUUID: m.providerUUID,
		SKU:          sku,
		CallbackURL:  BuildCallbackURL(m.callbackBaseURL),
	})
	if err != nil {
		// Clean up in-flight tracking on failure
		m.UntrackInFlight(event.LeaseUUID)

		slog.Error("failed to start provisioning",
			"lease_uuid", event.LeaseUUID,
			"sku", sku,
			"backend", backendClient.Name(),
			"error", err,
		)
		return fmt.Errorf("%w: %w", ErrProvisioningFailed, err)
	}

	slog.Info("provisioning started",
		"lease_uuid", event.LeaseUUID,
		"sku", sku,
		"backend", backendClient.Name(),
	)

	return nil
}

// handleLeaseClosed processes lease closure events.
func (m *Manager) handleLeaseClosed(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicLeaseClosed, err) }()

	event, ok := unmarshalMessagePayload[chain.LeaseEvent](msg, TopicLeaseClosed)
	if !ok {
		return nil
	}

	slog.Info("processing lease closed", "lease_uuid", event.LeaseUUID)

	// Clean up any stored payload for this lease.
	// This handles the case where a tenant uploaded a payload but canceled the lease
	// before provisioning started, or any other scenario where payload exists but
	// the lease is no longer valid.
	if m.payloadStore != nil {
		if m.payloadStore.Has(event.LeaseUUID) {
			m.payloadStore.Delete(event.LeaseUUID)
			slog.Info("cleaned up stored payload for closed lease",
				"lease_uuid", event.LeaseUUID,
			)
		}
	}

	// Remove from in-flight if present (atomic check-and-delete)
	provision, wasInFlight := m.PopInFlight(event.LeaseUUID)

	// Determine the backend for deprovisioning.
	// Priority:
	// 1. From in-flight tracking (most reliable - we know exactly where it's provisioned)
	// 2. Route by SKU (consistent with provisioning path)
	// 3. Fallback: deprovision from all backends (ensures cleanup even if routing differs)
	var backendClient backend.Backend

	if wasInFlight && provision.Backend != "" {
		// Case 1: Was in-flight - use the tracked backend
		backendClient = m.router.GetBackendByName(provision.Backend)
		if backendClient == nil {
			slog.Warn("backend not found by name, will route by SKU",
				"lease_uuid", event.LeaseUUID,
				"backend_name", provision.Backend,
			)
		}
	}

	if backendClient == nil {
		// Case 2: Try to route by SKU (fetch from chain if we have SKU, or query lease)
		var sku string
		if wasInFlight && provision.SKU != "" {
			sku = provision.SKU
		} else {
			// Fetch lease details from chain to get SKU for routing
			// This ensures consistency with the provisioning path
			lease, err := m.chainClient.GetLease(msg.Context(), event.LeaseUUID)
			if err != nil {
				slog.Warn("failed to fetch lease details for deprovision routing",
					"lease_uuid", event.LeaseUUID,
					"error", err,
				)
				// Fall through to deprovision from all backends
			} else if lease != nil {
				sku = ExtractPrimarySKU(lease)
			}
		}

		if sku != "" {
			backendClient = m.router.Route(sku)
			slog.Debug("routing deprovision by SKU",
				"lease_uuid", event.LeaseUUID,
				"sku", sku,
				"backend", backendClient.Name(),
			)
		}
	}

	if backendClient != nil {
		// Deprovision from the determined backend
		if err := backendClient.Deprovision(msg.Context(), event.LeaseUUID); err != nil {
			slog.Error("failed to deprovision",
				"lease_uuid", event.LeaseUUID,
				"backend", backendClient.Name(),
				"error", err,
			)
			return fmt.Errorf("%w: lease %s: %w", ErrDeprovisionFailed, event.LeaseUUID, err)
		}

		slog.Info("deprovisioned successfully",
			"lease_uuid", event.LeaseUUID,
			"backend", backendClient.Name(),
		)
	} else {
		// Case 3: Fallback - deprovision from all backends
		// This ensures cleanup even if we can't determine the correct backend
		slog.Warn("could not determine backend for deprovision, trying all backends",
			"lease_uuid", event.LeaseUUID,
		)

		var lastErr error
		deprovisioned := false
		for _, b := range m.router.Backends() {
			if err := b.Deprovision(msg.Context(), event.LeaseUUID); err != nil {
				slog.Debug("deprovision from backend returned error",
					"lease_uuid", event.LeaseUUID,
					"backend", b.Name(),
					"error", err,
				)
				lastErr = err
			} else {
				slog.Info("deprovisioned successfully",
					"lease_uuid", event.LeaseUUID,
					"backend", b.Name(),
				)
				deprovisioned = true
			}
		}

		if !deprovisioned && lastErr != nil {
			return fmt.Errorf("%w: lease %s: %w", ErrDeprovisionFailed, event.LeaseUUID, lastErr)
		}
	}

	return nil
}

// handleLeaseExpired processes lease expiration events.
func (m *Manager) handleLeaseExpired(msg *message.Message) error {
	// Same handling as closed - deprovision the resource
	return m.handleLeaseClosed(msg)
}

// handleBackendCallback processes callbacks from backends.
func (m *Manager) handleBackendCallback(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicBackendCallback, err) }()

	callback, ok := unmarshalMessagePayload[backend.CallbackPayload](msg, TopicBackendCallback)
	if !ok {
		return nil
	}

	// Check if this lease is in-flight (idempotency check)
	// Ignore callbacks for unknown/already-processed leases to prevent:
	// - Duplicate on-chain transactions from replay attacks
	// - Processing misrouted callbacks from other providers
	provision, exists := m.GetInFlight(callback.LeaseUUID)
	if !exists {
		slog.Warn("ignoring callback for unknown or already-processed lease",
			"lease_uuid", callback.LeaseUUID,
			"status", callback.Status,
		)
		return nil // Don't retry - this is not an error
	}

	slog.Info("processing backend callback",
		"lease_uuid", callback.LeaseUUID,
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
		acknowledged, txHash, err := m.ackBatcher.Acknowledge(msg.Context(), callback.LeaseUUID)
		if err != nil {
			// Check if this is a terminal error (e.g., lease already acknowledged)
			if isTerminalAcknowledgeError(err) {
				// Lease is already in a non-PENDING state (likely already ACTIVE).
				// This can happen if we received a duplicate callback or the reconciler
				// already acknowledged it. Treat as success - the lease is active.
				m.UntrackInFlight(callback.LeaseUUID)
				recordDuration()
				metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeSuccess, provision.Backend).Inc()
				slog.Info("lease already acknowledged, skipping",
					"lease_uuid", callback.LeaseUUID,
				)
				return nil
			}

			slog.Error("failed to acknowledge lease",
				"lease_uuid", callback.LeaseUUID,
				"error", err,
			)
			// Keep in-flight tracking for retry - Watermill will retry this message
			return fmt.Errorf("%w: lease %s: %w", ErrAcknowledgeFailed, callback.LeaseUUID, err)
		}

		// Only remove from in-flight after successful acknowledgment
		m.UntrackInFlight(callback.LeaseUUID)
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeSuccess, provision.Backend).Inc()

		slog.Info("lease acknowledged after provisioning",
			"lease_uuid", callback.LeaseUUID,
			"acknowledged", acknowledged,
			"tx_hash", txHash,
		)

	case backend.CallbackStatusFailed:
		// Remove from in-flight - this is a terminal state, no retry needed
		m.UntrackInFlight(callback.LeaseUUID)
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeFailed, provision.Backend).Inc()

		// Reject the lease on chain so tenant's credit is released immediately
		reason := callback.Error
		if reason == "" {
			reason = "provisioning failed"
		}
		rejected, txHashes, err := m.chainClient.RejectLeases(msg.Context(), []string{callback.LeaseUUID}, reason)
		if err != nil {
			// Log but don't retry - the lease will eventually expire if rejection fails
			slog.Error("failed to reject lease after provisioning failure",
				"lease_uuid", callback.LeaseUUID,
				"error", err,
			)
		} else {
			slog.Info("lease rejected after provisioning failure",
				"lease_uuid", callback.LeaseUUID,
				"rejected", rejected,
				"tx_hashes", txHashes,
				"reason", reason,
			)
		}

	default:
		// Unknown status is treated as terminal to prevent leases from being stuck
		// in the in-flight map indefinitely. The reconciler will pick up the lease
		// and handle it based on its actual chain/backend state.
		m.UntrackInFlight(callback.LeaseUUID)
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeError, provision.Backend).Inc()

		slog.Warn("unknown callback status, treating as terminal",
			"lease_uuid", callback.LeaseUUID,
			"status", callback.Status,
		)
	}

	return nil
}

// handlePayloadReceived processes payload upload events.
// This triggers provisioning for leases that were waiting for a payload.
func (m *Manager) handlePayloadReceived(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicPayloadReceived, err) }()

	// Guard against nil payloadStore - this shouldn't happen in normal operation
	// since payload events are only published after successful storage, but
	// handle it gracefully for robustness.
	if m.payloadStore == nil {
		slog.Error("payload store not configured, cannot process payload event")
		return nil // Don't retry - configuration issue
	}

	event, ok := unmarshalMessagePayload[PayloadEvent](msg, TopicPayloadReceived)
	if !ok {
		return nil
	}

	slog.Info("processing payload received",
		"lease_uuid", event.LeaseUUID,
		"tenant", event.Tenant,
	)

	// Fetch lease details from chain to get SKU for routing
	lease, err := m.chainClient.GetLease(msg.Context(), event.LeaseUUID)
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
		)
		m.payloadStore.Delete(event.LeaseUUID)
		return nil
	}

	// Verify lease is still pending
	if lease.State != billingtypes.LEASE_STATE_PENDING {
		slog.Warn("lease is no longer pending, skipping provisioning",
			"lease_uuid", event.LeaseUUID,
			"state", lease.State.String(),
		)
		// Clean up the stored payload
		m.payloadStore.Delete(event.LeaseUUID)
		return nil
	}

	// Extract primary SKU for routing
	sku := ExtractPrimarySKU(lease)

	// Route to appropriate backend based on SKU
	backendClient := m.router.Route(sku)
	if backendClient == nil {
		slog.Error("no backend available for provisioning",
			"lease_uuid", event.LeaseUUID,
			"sku", sku,
		)
		return fmt.Errorf("%w: lease %s", ErrNoBackendAvailable, event.LeaseUUID)
	}

	// Atomically track in-flight BEFORE calling Provision
	if !m.TryTrackInFlight(event.LeaseUUID, event.Tenant, sku, backendClient.Name()) {
		slog.Debug("lease already in-flight, skipping",
			"lease_uuid", event.LeaseUUID,
		)
		return nil
	}

	// Get the payload from the store WITHOUT removing it yet.
	// We only delete after Provision() succeeds to allow retries.
	payload := m.payloadStore.Get(event.LeaseUUID)
	if payload == nil {
		// This shouldn't happen in normal operation since payload is stored
		// before publishing the event, but handle it gracefully
		slog.Warn("payload not found in store, proceeding without payload",
			"lease_uuid", event.LeaseUUID,
		)
	} else if event.MetaHashHex != "" {
		// Re-verify payload hash before provisioning to catch any corruption.
		// The payload was validated on upload, but disk corruption could occur.
		if err := VerifyPayloadHashHex(payload, event.MetaHashHex); err != nil {
			// Payload is corrupted - delete it and fail
			m.payloadStore.Delete(event.LeaseUUID)
			m.UntrackInFlight(event.LeaseUUID)
			slog.Error("payload hash mismatch - possible corruption",
				"lease_uuid", event.LeaseUUID,
				"error", err,
			)
			return err
		}
	}

	// Build provision request - only include PayloadHash when we have the actual payload.
	// This ensures backends never receive a hash without the corresponding data.
	req := backend.ProvisionRequest{
		LeaseUUID:    event.LeaseUUID,
		Tenant:       event.Tenant,
		ProviderUUID: m.providerUUID,
		SKU:          sku,
		CallbackURL:  BuildCallbackURL(m.callbackBaseURL),
		Payload:      payload,
	}
	if payload != nil && event.MetaHashHex != "" {
		req.PayloadHash = event.MetaHashHex
	}

	// Start provisioning with payload
	err = backendClient.Provision(msg.Context(), req)
	if err != nil {
		// Clean up in-flight tracking on failure.
		// Keep payload in store so reconciliation can retry with it.
		m.UntrackInFlight(event.LeaseUUID)

		slog.Error("failed to start provisioning",
			"lease_uuid", event.LeaseUUID,
			"sku", sku,
			"backend", backendClient.Name(),
			"error", err,
		)
		return fmt.Errorf("%w: %w", ErrProvisioningFailed, err)
	}

	// Provision succeeded - now safe to delete the payload from store
	if payload != nil {
		m.payloadStore.Delete(event.LeaseUUID)
	}

	slog.Info("provisioning started with payload",
		"lease_uuid", event.LeaseUUID,
		"sku", sku,
		"backend", backendClient.Name(),
		"payload_size", len(payload),
	)

	return nil
}
