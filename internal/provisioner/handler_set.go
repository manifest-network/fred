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

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/util"
)

// HandlerDeps contains the dependencies needed by the handler set.
type HandlerDeps struct {
	Events       *HandlerEventCoordinator
	PayloadStore HandlerPayloadStore
	Publisher    message.Publisher // For publishing to TopicLeaseEvent (optional)
	Callbacks    CallbackApplication
}

// HandlerPayloadStore is the lease-payload surface needed by message handlers.
// Hash verification remains a pure payload-package function; persistence and
// lifecycle coordination stay behind separate ports.
type HandlerPayloadStore interface {
	Get(string) ([]byte, error)
	Has(string) (bool, error)
	Delete(string)
}

// HandlerSet contains message adapters for the provisioner. Chain and payload
// adapters are registered with Watermill; Manager invokes the callback adapter
// synchronously to preserve backend outbox ordering.
type HandlerSet struct {
	deps            HandlerDeps
	events          *HandlerEventCoordinator
	payloads        HandlerPayloadStore
	callbacks       CallbackApplication
	awaitingMu      sync.Mutex
	awaitingPayload map[string]struct{} // tracks lease UUIDs awaiting payload for gauge accuracy
}

// NewHandlerSet creates a fully wired HandlerSet. The event capability joins
// lease exclusion with the exact provisioner once at construction; a handler
// with independently spliceable lifecycle and side-effect ports cannot exist.
func NewHandlerSet(deps HandlerDeps) (*HandlerSet, error) {
	if deps.Events == nil || !deps.Events.Valid() {
		return nil, errors.New("handler event coordinator is required")
	}
	if util.IsNilInterface(deps.Callbacks) {
		return nil, errors.New("handler callback application is required")
	}
	events := deps.Events
	payloads := deps.PayloadStore
	if util.IsNilInterface(payloads) {
		payloads = nil
	}
	callbacks := deps.Callbacks
	// The ports are retained in dedicated narrow fields, not duplicated in the
	// handler dependency bag. Normalizing typed nils here keeps optional payload
	// storage safe after changing the dependency from a concrete pointer to an
	// interface.
	deps.Events = nil
	deps.PayloadStore = nil
	deps.Callbacks = nil
	handler := &HandlerSet{
		deps:            deps,
		events:          events,
		payloads:        payloads,
		callbacks:       callbacks,
		awaitingPayload: make(map[string]struct{}),
	}
	return handler, nil
}

// HandleLeaseCreated processes new lease events.
func (h *HandlerSet) HandleLeaseCreated(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicLeaseCreated, err) }()

	event, ok := unmarshalMessagePayload[chain.LeaseEvent](msg, TopicLeaseCreated)
	if !ok {
		return nil
	}
	request, err := placement.NewProvisionEventRequest(event.LeaseUUID, event.Tenant)
	if err != nil {
		return err
	}
	result := h.events.startFromCurrentLease(msg.Context(), request)
	switch result.Disposition() {
	case placement.ProvisionEventStarted, placement.ProvisionEventDuplicate,
		placement.ProvisionEventLeaseActive,
		placement.ProvisionEventLeaseTerminal:
		return nil
	case placement.ProvisionEventAwaitingPayload:
		hash, ok := result.MetaHashHex()
		if !ok {
			return errors.New("awaiting-payload result omitted authoritative hash")
		}
		h.awaitingMu.Lock()
		h.awaitingPayload[event.LeaseUUID] = struct{}{}
		metrics.LeasesAwaitingPayload.Set(float64(len(h.awaitingPayload)))
		h.awaitingMu.Unlock()
		slog.Info("lease requires payload, awaiting upload",
			"lease_uuid", event.LeaseUUID,
			"tenant", event.Tenant,
			"meta_hash_hex", hash,
		)
		return nil
	case placement.ProvisionEventRejected:
		h.publishLeaseEvent(event.LeaseUUID, backend.ProvisionStatusFailed, result.RejectionReason())
		return result.Err()
	default:
		if result.Err() != nil {
			return result.Err()
		}
		return fmt.Errorf("provision event ended without a successful disposition: %d",
			result.Disposition())
	}
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
	return h.events.Deprovision(msg.Context(), event.LeaseUUID)
}

// HandleBackendCallbackEvidence is the synchronous authenticated transport adapter
// used by Manager. It returns only after CallbackService reaches a terminal
// application result, preserving the backend's per-lease delivery order.
func (h *HandlerSet) HandleBackendCallbackEvidence(
	ctx context.Context,
	request hmacauth.VerifiedRequest,
) (err error) {
	defer func() { recordWatermillMetrics(TopicBackendCallback, err) }()
	if h.callbacks == nil {
		return errCallbackOperationsUnavailable
	}
	return h.callbacks.HandleCallback(ctx, request)
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

	request, err := placement.NewPayloadProvisionEventRequest(
		event.LeaseUUID, event.Tenant,
		func() ([]byte, error) {
			return h.payloads.Get(event.LeaseUUID)
		},
	)
	if err != nil {
		return err
	}
	result := h.events.startFromCurrentLease(msg.Context(), request)
	switch result.Disposition() {
	case placement.ProvisionEventStarted, placement.ProvisionEventDuplicate,
		placement.ProvisionEventLeaseActive:
		return nil
	case placement.ProvisionEventLeaseTerminal:
		state, _ := result.LeaseState()
		slog.Info("payload event observed terminal lease; deleting payload",
			"lease_uuid", event.LeaseUUID,
			"tenant", event.Tenant,
			"state", state.String(),
		)
		return nil
	case placement.ProvisionEventUncertain, placement.ProvisionEventInvalid:
		// Absence and unknown/future states are not terminal evidence. A lagging or
		// reset RPC node must never delete the only manifest needed to provision or
		// recover a live lease.
		slog.Warn("payload event cannot confirm lease state; preserving payload for retry",
			"lease_uuid", event.LeaseUUID,
			"tenant", event.Tenant,
			"error", result.Err(),
		)
		if result.Err() != nil {
			return result.Err()
		}
		return fmt.Errorf("cannot confirm payload lease %s state", event.LeaseUUID)
	case placement.ProvisionEventPayloadUnavailable:
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
		return result.Err()
	case placement.ProvisionEventRejected:
		h.publishLeaseEvent(event.LeaseUUID, backend.ProvisionStatusFailed, result.RejectionReason())
		return result.Err()
	default:
		if result.Err() != nil {
			return result.Err()
		}
		return fmt.Errorf("payload provision event ended without a successful disposition: %d",
			result.Disposition())
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
