package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/util"
)

const (
	callbackSettlementClaimPollInterval = 25 * time.Millisecond
	callbackSettlementClaimMaxWait      = 30 * time.Second
)

// HandlerDeps contains the dependencies needed by the handler set.
type HandlerDeps struct {
	ChainClient     ChainClient
	Orchestrator    EventProvisioner
	EventOperations EventOperations
	PayloadStore    HandlerPayloadStore
	Publisher       message.Publisher // For publishing to TopicLeaseEvent (optional)
	Callbacks       CallbackApplication
}

// EventProvisioner is the application capability consumed by event adapters.
// Handlers can request provision/deprovision workflows, but cannot reach the
// orchestrator's placement, routing, or operation-registry dependencies.
type EventProvisioner interface {
	StartProvisioningClaimed(
		context.Context,
		operation.LeaseClaim,
		*billingtypes.Lease,
		ProvisionOpts,
	) error
	Deprovision(context.Context, string) error
}

// HandlerPayloadStore is the lease-payload surface needed by message handlers.
// Hash verification remains a pure payload-package function; persistence and
// lifecycle coordination stay behind separate ports.
type HandlerPayloadStore interface {
	Get(string) ([]byte, error)
	Has(string) (bool, error)
	Delete(string)
}

// EventOperations is the exact lifecycle capability needed by lease and
// payload event adapters. It can fence one lease while chain state is re-read,
// but cannot inspect, start, or settle an operation.
type EventOperations interface {
	TryClaimLeaseNow(string) operation.LeaseClaimResult
	ReleaseLease(operation.LeaseClaim) bool
	Contains(string) bool
}

// HandlerSet contains message adapters for the provisioner. Chain and payload
// adapters are registered with Watermill; Manager invokes the callback adapter
// synchronously to preserve backend outbox ordering.
type HandlerSet struct {
	deps            HandlerDeps
	provisioner     EventProvisioner
	payloads        HandlerPayloadStore
	callbacks       CallbackApplication
	eventOperations EventOperations
	awaitingMu      sync.Mutex
	awaitingPayload map[string]struct{} // tracks lease UUIDs awaiting payload for gauge accuracy
}

// NewHandlerSet creates a new HandlerSet with the given dependencies.
func NewHandlerSet(deps HandlerDeps) *HandlerSet {
	provisioner := deps.Orchestrator
	if util.IsNilInterface(provisioner) {
		provisioner = nil
	}
	payloads := deps.PayloadStore
	if util.IsNilInterface(payloads) {
		payloads = nil
	}
	eventOperations := deps.EventOperations
	if util.IsNilInterface(eventOperations) {
		eventOperations = nil
	}
	callbacks := deps.Callbacks
	if util.IsNilInterface(callbacks) {
		callbacks = nil
	}
	// The ports are retained in dedicated narrow fields, not duplicated in the
	// handler dependency bag. Normalizing typed nils here keeps optional payload
	// storage safe after changing the dependency from a concrete pointer to an
	// interface.
	deps.Orchestrator = nil
	deps.PayloadStore = nil
	deps.EventOperations = nil
	deps.Callbacks = nil
	handler := &HandlerSet{
		deps:            deps,
		provisioner:     provisioner,
		payloads:        payloads,
		callbacks:       callbacks,
		eventOperations: eventOperations,
		awaitingPayload: make(map[string]struct{}),
	}
	return handler
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
	claim, proceed, err := h.claimEventLease(event.LeaseUUID)
	if err != nil || !proceed {
		return err
	}
	defer h.releaseEventLease(claim, event.LeaseUUID)

	// Re-read chain state while the exact lifecycle claim excludes close and
	// reconciliation. Delayed create events must never dispatch from an older
	// PENDING observation after the lease has become terminal.
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
	if lease.State != billingtypes.LEASE_STATE_PENDING {
		slog.Info("ignoring delayed create event for non-pending lease",
			"lease_uuid", event.LeaseUUID,
			"state", lease.State.String(),
		)
		return nil
	}

	// Check if lease requires a payload (has MetaHash)
	// If so, skip immediate provisioning - wait for payload upload
	if len(lease.MetaHash) > 0 {
		h.awaitingMu.Lock()
		h.awaitingPayload[event.LeaseUUID] = struct{}{}
		metrics.LeasesAwaitingPayload.Set(float64(len(h.awaitingPayload)))
		h.awaitingMu.Unlock()
		slog.Info("lease requires payload, awaiting upload",
			"lease_uuid", event.LeaseUUID,
			"tenant", event.Tenant,
			"meta_hash_hex", fmt.Sprintf("%x", lease.MetaHash),
		)
		return nil // Don't provision yet - wait for payload
	}

	// Start provisioning without payload
	err = h.provisioner.StartProvisioningClaimed(msg.Context(), claim, lease, ProvisionOpts{})
	if err != nil {
		if errors.Is(err, backend.ErrValidation) {
			return h.rejectOnValidationError(msg.Context(), lease, err)
		}
		return err
	}

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

	// If the lease was still awaiting payload, update the gauge.
	h.awaitingMu.Lock()
	delete(h.awaitingPayload, event.LeaseUUID)
	metrics.LeasesAwaitingPayload.Set(float64(len(h.awaitingPayload)))
	h.awaitingMu.Unlock()

	// Clean up any stored payload for this lease.
	// This handles the case where a tenant uploaded a payload but canceled the lease
	// before provisioning started, or any other scenario where payload exists but
	// the lease is no longer valid.
	if h.payloads != nil {
		if exists, err := h.payloads.Has(event.LeaseUUID); err != nil {
			slog.Warn("failed to check payload store during lease close",
				"lease_uuid", event.LeaseUUID,
				"error", err,
			)
		} else if exists {
			h.payloads.Delete(event.LeaseUUID)
			slog.Info("cleaned up stored payload for closed lease",
				"lease_uuid", event.LeaseUUID,
				"tenant", event.Tenant,
			)
		}
	}

	// ENG-329: the retained notice is NOT emitted here (on close intent). At
	// close time providerd cannot know whether the backend actually retained,
	// so the former optimistic emit fired regardless of outcome. The notice now
	// fires on observed ground truth from the deprovision callback (Retained=true)
	// in HandleBackendCallback, and the durable backstop is the queryable
	// retention status (GET /status, GET /provision).

	// Delegate to orchestrator for deprovisioning
	return h.provisioner.Deprovision(msg.Context(), event.LeaseUUID)
}

// HandleBackendCallbackPayload is the synchronous, typed transport adapter
// used by Manager. It returns only after CallbackService reaches a terminal
// application result, preserving the backend's per-lease delivery order.
func (h *HandlerSet) HandleBackendCallbackPayload(
	ctx context.Context,
	callback backend.CallbackPayload,
) (err error) {
	defer func() { recordWatermillMetrics(TopicBackendCallback, err) }()
	return h.handleBackendCallbackPayload(ctx, callback)
}

func (h *HandlerSet) handleBackendCallbackPayload(
	ctx context.Context,
	callback backend.CallbackPayload,
) error {
	command, err := NewCallbackCommand(callback)
	if err != nil {
		return fmt.Errorf("decode backend callback operation identity: %w", err)
	}
	if h.callbacks == nil {
		return errCallbackOperationsUnavailable
	}
	return h.callbacks.HandleCallback(ctx, command)
}

// HandlePayloadReceived processes payload upload events.
// This triggers provisioning for leases that were waiting for a payload.
func (h *HandlerSet) HandlePayloadReceived(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicPayloadReceived, err) }()

	// Guard against nil payloadStore - this shouldn't happen in normal operation
	// since payload events are only published after successful storage, but
	// handle it gracefully for robustness.
	if h.payloads == nil {
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

	// Lease is no longer awaiting payload — update gauge.
	h.awaitingMu.Lock()
	delete(h.awaitingPayload, event.LeaseUUID)
	metrics.LeasesAwaitingPayload.Set(float64(len(h.awaitingPayload)))
	h.awaitingMu.Unlock()

	claim, proceed, err := h.claimEventLease(event.LeaseUUID)
	if err != nil || !proceed {
		return err
	}
	defer h.releaseEventLease(claim, event.LeaseUUID)

	// Fetch lease details from chain to get SKU for routing. The subscriber's
	// context may live for the whole process, so bound this point read: a stalled
	// RPC must release the lease claim and let Watermill retry.
	leaseCtx, cancelLease := context.WithTimeout(msg.Context(), chainConfirmTimeout)
	lease, err := h.deps.ChainClient.GetLease(leaseCtx, event.LeaseUUID)
	cancelLease()
	if err != nil {
		slog.Error("failed to fetch lease details",
			"lease_uuid", event.LeaseUUID,
			"error", err,
		)
		return fmt.Errorf("failed to fetch lease %s: %w", event.LeaseUUID, err)
	}
	liveness, reason := classifyLease(lease, nil)
	switch liveness {
	case leaseTerminal:
		slog.Info("payload event observed terminal lease; deleting payload",
			"lease_uuid", event.LeaseUUID,
			"tenant", event.Tenant,
			"state", leaseState(lease),
		)
		h.payloads.Delete(event.LeaseUUID)
		return nil
	case leaseUnknown:
		// Absence and unknown/future states are not terminal evidence. A lagging or
		// reset RPC node must never delete the only manifest needed to provision or
		// recover a live lease.
		slog.Warn("payload event cannot confirm lease state; preserving payload for retry",
			"lease_uuid", event.LeaseUUID,
			"tenant", event.Tenant,
			"state", leaseState(lease),
			"reason", reason,
		)
		return fmt.Errorf("cannot confirm payload lease %s state", event.LeaseUUID)
	case leaseLive:
		if lease.State == billingtypes.LEASE_STATE_ACTIVE {
			// A delayed duplicate payload event can arrive after a successful callback.
			// ACTIVE leases intentionally retain their manifest for crash recovery and
			// reprovision, so acknowledge the duplicate without touching the payload.
			slog.Debug("payload event arrived after lease became active; preserving payload",
				"lease_uuid", event.LeaseUUID,
				"tenant", event.Tenant,
			)
			return nil
		}
	}

	// Get the payload from the store WITHOUT removing it yet.
	// Payload deletion happens later: when the lease is closed
	// (HandleLeaseClosed) or when a PENDING-failure callback rejects the
	// lease. Successful and ACTIVE-re-provision-failure paths intentionally
	// keep the payload so a subsequent re-provision can reuse the same
	// manifest. This also ensures the payload remains available for retry
	// if the backend fails or crashes before sending a callback.
	payloadData, err := h.payloads.Get(event.LeaseUUID)
	if err != nil {
		slog.Error("failed to read payload from store",
			"lease_uuid", event.LeaseUUID,
			"error", err,
		)
		return fmt.Errorf("payload store read error: %w", err)
	}
	if payloadData == nil {
		// The event may outlive or race the durable payload write. Retrying is
		// safe; dispatching without bytes is not, because the chain still names a
		// payload-bearing request and no exact fingerprint could be persisted.
		h.awaitingMu.Lock()
		h.awaitingPayload[event.LeaseUUID] = struct{}{}
		metrics.LeasesAwaitingPayload.Set(float64(len(h.awaitingPayload)))
		h.awaitingMu.Unlock()
		slog.Warn("payload not found in store, deferring payload event",
			"lease_uuid", event.LeaseUUID,
			"tenant", event.Tenant,
		)
		return fmt.Errorf("%w: lease %s", errPayloadNotAvailable, event.LeaseUUID)
	} else if event.MetaHashHex != "" {
		// Re-verify payload hash before provisioning to catch any corruption.
		// The payload was validated on upload, but disk corruption could occur.
		if err := payload.VerifyHashHex(payloadData, event.MetaHashHex); err != nil {
			slog.Error("payload hash mismatch - possible corruption, rejecting lease",
				"lease_uuid", event.LeaseUUID,
				"error", err,
			)
			// Reject the lease on-chain: the payload is irrecoverably corrupted
			// and cannot be provisioned. If rejection fails, we return the error
			// so Watermill retries — the payload is still in the store, so the
			// hash mismatch will fire again and re-attempt rejection.
			_, _, rejectErr := h.deps.ChainClient.RejectLeases(
				msg.Context(), []string{event.LeaseUUID}, rejectReasonPayloadCorrupted,
			)
			if rejectErr != nil {
				return fmt.Errorf("failed to reject lease %s after payload corruption: %w",
					event.LeaseUUID, rejectErr)
			}
			h.payloads.Delete(event.LeaseUUID)
			h.publishLeaseEvent(event.LeaseUUID, backend.ProvisionStatusFailed, rejectReasonPayloadCorrupted)
			return nil
		}
	}

	// Start provisioning with payload
	err = h.provisioner.StartProvisioningClaimed(msg.Context(), claim, lease, ProvisionOpts{
		Payload:     payloadData,
		PayloadHash: event.MetaHashHex,
	})
	if err != nil {
		if errors.Is(err, backend.ErrValidation) {
			h.payloads.Delete(event.LeaseUUID)
			return h.rejectOnValidationError(msg.Context(), lease, err)
		}
		return err
	}

	return nil
}

func (h *HandlerSet) claimEventLease(leaseUUID string) (operation.LeaseClaim, bool, error) {
	if h.eventOperations == nil {
		return operation.LeaseClaim{}, false, errors.New("event lifecycle operation registry is unavailable")
	}
	result := h.eventOperations.TryClaimLeaseNow(leaseUUID)
	if result.Acquired() {
		return result.Claim(), true, nil
	}
	if h.eventOperations.Contains(leaseUUID) {
		// A duplicate create/payload delivery for the current operation is
		// idempotent. There is no stale read to retry.
		return operation.LeaseClaim{}, false, nil
	}
	return operation.LeaseClaim{}, false, fmt.Errorf(
		"lease %s lifecycle claim is busy; retry event", leaseUUID,
	)
}

func (h *HandlerSet) releaseEventLease(claim operation.LeaseClaim, leaseUUID string) {
	if !h.eventOperations.ReleaseLease(claim) {
		slog.Error("failed to release exact event lifecycle claim", "lease_uuid", leaseUUID)
	}
}

// publishLeaseEvent publishes a LeaseStatusEvent to TopicLeaseEvent for real-time delivery.
// Best-effort: errors are logged but do not affect the handler's return value.
func (h *HandlerSet) publishLeaseEvent(leaseUUID string, status backend.ProvisionStatus, errMsg string) {
	publishLeaseStatusEvent(h.deps.Publisher, leaseUUID, status, errMsg)
}

func publishLeaseStatusEvent(
	publisher message.Publisher,
	leaseUUID string,
	status backend.ProvisionStatus,
	errMsg string,
) {
	if publisher == nil {
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
	if err := publisher.Publish(TopicLeaseEvent, msg); err != nil {
		slog.Warn("failed to publish lease event", "lease_uuid", leaseUUID, "error", err)
	}
}

// Label sentinels for sanitized Prom label values.
const (
	labelBackendUnknown = "unknown"
	labelBackendInvalid = "invalid"
)
