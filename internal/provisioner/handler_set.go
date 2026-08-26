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
	"github.com/manifest-network/fred/internal/provisioner/payload"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const (
	callbackSettlementClaimPollInterval = 25 * time.Millisecond
	callbackSettlementClaimMaxWait      = 30 * time.Second
)

var errCallbackSettlementClaimTimeout = errors.New("timed out waiting for callback settlement claim")

// HandlerDeps contains the dependencies needed by the handler set.
type HandlerDeps struct {
	ChainClient   ChainClient
	Orchestrator  *ProvisionOrchestrator
	Tracker       InFlightTracker
	Acknowledger  Acknowledger
	PayloadStore  *payload.Store
	Publisher     message.Publisher // For publishing to TopicLeaseEvent (optional)
	BackendRouter BackendRouter     // Used to allowlist backend names on non-in-flight callback metrics
}

// HandlerSet contains the Watermill message handlers for the provisioner.
// It encapsulates all handler methods and their dependencies.
type HandlerSet struct {
	deps            HandlerDeps
	awaitingMu      sync.Mutex
	awaitingPayload map[string]struct{} // tracks lease UUIDs awaiting payload for gauge accuracy
}

// isPermanentPlacementVerdict reports semantic placement decisions that the
// same callback can never repair by retrying. They are deliberately distinct
// from storage/I/O failures: an authenticated success callback is still
// positive backend evidence, while retrying a semantic conflict until the
// poison queue drops it can leave a live lease to be rejected by the timeout
// checker.
func isPermanentPlacementVerdict(err error) bool {
	return errors.Is(err, placement.ErrInvalidPlacement) ||
		errors.Is(err, placement.ErrAttemptConflict) ||
		errors.Is(err, placement.ErrBackendConflict) ||
		errors.Is(err, placement.ErrUnusablePlacement) ||
		errors.Is(err, placement.ErrAttemptMismatch)
}

// waitForCallbackSettlementClaim waits for ownership of one exact in-flight
// generation. It returns ownsClaim=false when another actor completed or
// replaced that generation while we waited, and reports a deprovision-owned
// claim separately so backend status can be observed without settling the
// provision operation. The hard deadline bounds a leaked/slow foreign claim
// without stealing it: forced reclamation would let two concurrent chain
// operations believe they both own terminal settlement.
func waitForCallbackSettlementClaim(
	ctx context.Context,
	tracker InFlightTracker,
	provision InFlightProvision,
	maxWait time.Duration,
) (claimed InFlightProvision, ownsClaim, deprovisionOwned bool, err error) {
	if claimed, ok := tracker.TryClaimInFlight(provision.LeaseUUID, provision.Generation); ok {
		return claimed, true, false, nil
	}

	started := time.Now()
	timedOut := func() error {
		waited := time.Since(started)
		metrics.CallbackSettlementClaimWaitTimeoutsTotal.Inc()
		slog.Error("timed out waiting for callback settlement claim",
			"lease_uuid", provision.LeaseUUID,
			"backend", provision.Backend,
			"generation", provision.Generation,
			"waited", waited,
		)
		return fmt.Errorf("%w after %s", errCallbackSettlementClaimTimeout, waited)
	}
	if maxWait <= 0 {
		return InFlightProvision{}, false, false, timedOut()
	}
	ticker := time.NewTicker(callbackSettlementClaimPollInterval)
	defer ticker.Stop()
	timer := time.NewTimer(maxWait)
	defer timer.Stop()

	slog.Warn("callback settlement claim is contended; waiting for exact generation",
		"lease_uuid", provision.LeaseUUID,
		"backend", provision.Backend,
		"generation", provision.Generation,
		"max_wait", maxWait,
	)

	for {
		// The timer case below wakes the loop efficiently. This elapsed check
		// makes the bound strict even if the ticker and timer become ready in the
		// same select and the runtime chooses the ticker first.
		if time.Since(started) >= maxWait {
			return InFlightProvision{}, false, false, timedOut()
		}
		current, stillExists := tracker.GetInFlight(provision.LeaseUUID)
		if !stillExists {
			slog.Info("callback generation settled while waiting for claim",
				"lease_uuid", provision.LeaseUUID,
				"backend", provision.Backend,
				"generation", provision.Generation,
				"waited", time.Since(started),
			)
			return InFlightProvision{}, false, false, nil
		}
		if current.Generation != provision.Generation {
			slog.Info("callback generation was replaced while waiting for claim",
				"lease_uuid", provision.LeaseUUID,
				"backend", provision.Backend,
				"generation", provision.Generation,
				"replacement_generation", current.Generation,
				"waited", time.Since(started),
			)
			return InFlightProvision{}, false, false, nil
		}
		if current.settlementOwner == inFlightSettlementDeprovision {
			return current, false, true, nil
		}

		select {
		case <-ctx.Done():
			return InFlightProvision{}, false, false, ctx.Err()
		case <-timer.C:
			return InFlightProvision{}, false, false, timedOut()
		case <-ticker.C:
			// Both timer and ticker can become ready at the deadline. Recheck
			// elapsed time before accepting a newly available claim so select's
			// pseudo-random choice cannot extend the hard bound.
			if time.Since(started) >= maxWait {
				return InFlightProvision{}, false, false, timedOut()
			}
			claimed, ok := tracker.TryClaimInFlight(provision.LeaseUUID, provision.Generation)
			if !ok {
				continue
			}
			slog.Info("acquired callback settlement claim after contention",
				"lease_uuid", provision.LeaseUUID,
				"backend", provision.Backend,
				"generation", provision.Generation,
				"waited", time.Since(started),
			)
			return claimed, true, false, nil
		}
	}
}

func (h *HandlerSet) publishRetainedLeaseNotice(leaseUUID string) {
	h.publishLeaseEvent(leaseUUID, backend.ProvisionStatusRetained,
		"your lease data was retained and can be restored within the grace window: create a fresh PENDING lease of matching shape, then POST /v1/leases/{new_lease_uuid}/restore with from_lease_uuid set to this lease's UUID")
}

func (h *HandlerSet) handleDeprovisionOwnedCallback(
	callback backend.CallbackPayload,
	provision InFlightProvision,
) {
	switch callback.Status {
	case backend.CallbackStatusDeprovisioned:
		if h.deps.Orchestrator != nil {
			// The in-flight tracker is the operation identity. Legacy callbacks may
			// omit Backend, so retire the candidate bound to the owned generation
			// rather than trusting the optional payload field.
			h.deps.Orchestrator.forgetDeprovisionCandidate(callback.LeaseUUID, provision.Backend)
		}
		if callback.Retained {
			h.publishRetainedLeaseNotice(callback.LeaseUUID)
		}
	case backend.CallbackStatusFailed:
		h.publishLeaseEvent(callback.LeaseUUID, backend.ProvisionStatusFailed, callback.Error)
	case backend.CallbackStatusSuccess:
		metrics.CallbackDeprovisionOwnedSuccessTotal.Inc()
		slog.Warn("ignoring success callback emitted while deprovision owns the operation",
			"lease_uuid", callback.LeaseUUID,
			"backend", provision.Backend,
			"generation", provision.Generation,
		)
	}
	slog.Info("observed callback for deprovision-owned in-flight operation",
		"lease_uuid", callback.LeaseUUID,
		"backend", provision.Backend,
		"generation", provision.Generation,
		"status", callback.Status,
		"retained", callback.Retained,
	)
}

// NewHandlerSet creates a new HandlerSet with the given dependencies.
func NewHandlerSet(deps HandlerDeps) *HandlerSet {
	return &HandlerSet{
		deps:            deps,
		awaitingPayload: make(map[string]struct{}),
	}
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

	// If the lease was still awaiting payload, update the gauge.
	h.awaitingMu.Lock()
	delete(h.awaitingPayload, event.LeaseUUID)
	metrics.LeasesAwaitingPayload.Set(float64(len(h.awaitingPayload)))
	h.awaitingMu.Unlock()

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

	// ENG-329: the retained notice is NOT emitted here (on close intent). At
	// close time providerd cannot know whether the backend actually retained,
	// so the former optimistic emit fired regardless of outcome. The notice now
	// fires on observed ground truth from the deprovision callback (Retained=true)
	// in HandleBackendCallback, and the durable backstop is the queryable
	// retention status (GET /status, GET /provision).

	// Delegate to orchestrator for deprovisioning
	return h.deps.Orchestrator.Deprovision(msg.Context(), event.LeaseUUID)
}

// HandleBackendCallback processes callbacks from backends.
func (h *HandlerSet) HandleBackendCallback(msg *message.Message) (err error) {
	defer func() { recordWatermillMetrics(TopicBackendCallback, err) }()

	callback, ok := unmarshalMessagePayload[backend.CallbackPayload](msg, TopicBackendCallback)
	if !ok {
		return nil
	}

	// Check if this lease is in-flight (idempotency check). An operation generation
	// is authoritative only while its exact tracker entry exists: backends treat
	// callback_url as an opaque, durable address and may reuse the provision URL
	// for later ACTIVE-lease redeploys or autonomous status notifications. Once the
	// tracker entry is gone, no callback may settle chain or placement state; both
	// generated and generation-less callbacks are best-effort status notifications.
	provision, exists := h.deps.Tracker.GetInFlight(callback.LeaseUUID)
	if !exists {
		backendLabel := h.sanitizeBackendName(callback.Backend)
		statusLabel := sanitizeCallbackStatus(callback.Status)
		if backendLabel == labelBackendInvalid || statusLabel == labelStatusOther {
			slog.Warn("sanitized callback label to bounded value",
				"lease_uuid", callback.LeaseUUID,
				"received_backend", callback.Backend,
				"received_status", callback.Status,
			)
		}
		metrics.NonInFlightCallbacksTotal.WithLabelValues(backendLabel, statusLabel).Inc()
		// Without an in-flight entry, the callback cannot be tied to the operation
		// that created the current placement Attempt. This is true even when the
		// backend name matches: a delayed restart/update callback may predate a
		// newer provision attempt. Publish only the best-effort status event;
		// authoritative inventory repairs placement.

		switch callback.Status {
		case backend.CallbackStatusSuccess:
			h.publishLeaseEvent(callback.LeaseUUID, backend.ProvisionStatusReady, "")
		case backend.CallbackStatusFailed:
			h.publishLeaseEvent(callback.LeaseUUID, backend.ProvisionStatusFailed, callback.Error)
		case backend.CallbackStatusDeprovisioned:
			if h.deps.Orchestrator != nil {
				h.deps.Orchestrator.forgetDeprovisionCandidate(callback.LeaseUUID, callback.Backend)
			}
			// No chain action: the backend tore down a lease that was not
			// in-flight here. Chain state is unchanged. ENG-329: if the backend
			// reports it actually retained the data, emit the retained notice on
			// observed ground truth (best-effort/fire-and-forget — the queryable
			// retention status is the durable backstop, so there is no marker or
			// reaper here). A non-retain deprovision emits nothing.
			if callback.Retained {
				h.publishRetainedLeaseNotice(callback.LeaseUUID)
			}
		default:
			slog.Warn("unexpected callback status for non-in-flight lease",
				"lease_uuid", callback.LeaseUUID,
				"status", callback.Status,
			)
			return nil
		}
		slog.Info("published status event for non-in-flight callback",
			"lease_uuid", callback.LeaseUUID,
			"generation", callback.OperationGeneration,
			"status", callback.Status,
		)
		return nil
	}
	// The tracker is the local operation identity. A callback naming a different
	// backend belongs to an older or otherwise unrelated operation for this lease;
	// applying it to the currently tracked backend could acknowledge/reject the
	// wrong incarnation and erase its durable placement transition. Empty is
	// accepted for backwards compatibility with backends that predate the field.
	if callback.Backend != "" && callback.Backend != provision.Backend {
		slog.Warn("ignoring callback whose backend does not match the in-flight operation",
			"lease_uuid", callback.LeaseUUID,
			"callback_backend", callback.Backend,
			"in_flight_backend", provision.Backend,
			"generation", provision.Generation,
		)
		return nil
	}
	if callback.OperationGeneration != provision.Generation &&
		(provision.GenerationRequired || callback.OperationGeneration != 0) {
		slog.Warn("ignoring callback whose operation token does not match the in-flight operation",
			"lease_uuid", callback.LeaseUUID,
			"callback_generation", callback.OperationGeneration,
			"in_flight_generation", provision.Generation,
			"token_required", provision.GenerationRequired,
		)
		return nil
	}
	// A callback emitted by Backend.Deprovision is evidence about the close RPC,
	// not a provisioning result to settle on chain. The close path deliberately
	// owns the exact generation across every candidate RPC so a later candidate
	// failure remains retryable. Consume the status notification without waiting
	// for or finishing that claim; Deprovision will finish or release it. The
	// owner tag is essential: an autonomous deprovision callback with no active
	// close still follows ordinary terminal settlement below. If this close later
	// fails, the orchestrator moves its positive candidates out of the ordinary
	// provisioning tracker before returning so timeout/load accounting cannot leak.
	if provision.settlementOwner == inFlightSettlementDeprovision {
		h.handleDeprovisionOwnedCallback(callback, provision)
		return nil
	}

	claimed, ownsClaim, deprovisionOwned, err := waitForCallbackSettlementClaim(
		msg.Context(), h.deps.Tracker, provision, callbackSettlementClaimMaxWait,
	)
	if err != nil {
		return fmt.Errorf("wait to settle callback for lease %s: %w", callback.LeaseUUID, err)
	}
	if deprovisionOwned {
		h.handleDeprovisionOwnedCallback(callback, claimed)
		return nil
	}
	if !ownsClaim {
		// The claimant completed or replaced this generation. Never fall through to
		// the general non-in-flight restart/update path or touch a replacement.
		return nil
	}
	provision = claimed
	defer h.deps.Tracker.ReleaseInFlightClaim(callback.LeaseUUID, provision.Generation)

	slog.Info("processing backend callback",
		"lease_uuid", callback.LeaseUUID,
		"tenant", provision.Tenant,
		"status", callback.Status,
		"backend", provision.Backend,
	)

	// Record provisioning duration if we have the start time. The operation label
	// (provision|restore) keeps restore latency separable from fresh provisions (ENG-358).
	operation := provision.Kind.operationLabel()
	recordDuration := func() {
		if !provision.StartTime.IsZero() {
			duration := time.Since(provision.StartTime).Seconds()
			metrics.ProvisioningDuration.WithLabelValues(provision.Backend, operation).Observe(duration)
		}
	}

	switch callback.Status {
	case backend.CallbackStatusSuccess:
		// Repair any Attempt left behind by a failed synchronous Confirm before
		// moving the lease on chain. The authenticated backend/generation pair is
		// positive evidence that the lease is live even when placement persistence
		// fails. In every error case the durable write-ahead Attempt or conflict is
		// left untouched for inventory/operator repair; blocking acknowledgement
		// would instead let the timeout path reject a lease that is already live.
		if err := h.deps.Orchestrator.ConfirmPlacement(callback.LeaseUUID, provision.Backend); err != nil {
			permanentVerdict := isPermanentPlacementVerdict(err)
			if permanentVerdict {
				metrics.CallbackPlacementSemanticConflictsTotal.Inc()
			}
			slog.Error("failed to confirm placement from authenticated success callback; continuing chain acknowledgement",
				"lease_uuid", callback.LeaseUUID,
				"backend", provision.Backend,
				"generation", provision.Generation,
				"permanent_semantic_verdict", permanentVerdict,
				"error", err,
			)
		}

		// Acknowledge the lease on chain via batcher to avoid sequence mismatch errors
		acknowledged, txHash, err := h.deps.Acknowledger.Acknowledge(msg.Context(), callback.LeaseUUID)
		if err != nil {
			// Check if this is a terminal error (e.g., lease already acknowledged)
			if isTerminalAcknowledgeError(err) {
				// Lease is already in a non-PENDING state (likely already ACTIVE).
				// This can happen if we received a duplicate callback or the reconciler
				// already acknowledged it. Treat as success - the lease is active.
				h.deps.Tracker.FinishClaimedInFlight(callback.LeaseUUID, provision.Generation)
				recordDuration()
				metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeSuccess, provision.Backend, operation).Inc()
				h.publishLeaseEvent(callback.LeaseUUID, backend.ProvisionStatusReady, "")
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
		h.deps.Tracker.FinishClaimedInFlight(callback.LeaseUUID, provision.Generation)
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeSuccess, provision.Backend, operation).Inc()

		// Payload is intentionally NOT deleted here. The lease is now ACTIVE
		// but the container could crash later, requiring re-provisioning with
		// the same manifest. Payload cleanup happens when the lease is closed
		// (HandleLeaseClosed) or when the PENDING-failure path below rejects
		// the lease and deletes the payload. ACTIVE re-provision failures
		// also keep the payload — the reconciler may retry from it.

		h.publishLeaseEvent(callback.LeaseUUID, backend.ProvisionStatusReady, "")

		slog.Info("lease acknowledged after provisioning",
			"lease_uuid", callback.LeaseUUID,
			"tenant", provision.Tenant,
			"operation", operation,
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
			// A definitive async failure settles only this operation's Attempt.
			// Any established Backend pin belongs to the ACTIVE lease and survives.
			if err := h.deps.Orchestrator.ClearPlacementAttempt(callback.LeaseUUID, provision.Backend); err != nil {
				slog.Error("failed to clear placement attempt after active reprovision failure, keeping in-flight",
					"lease_uuid", callback.LeaseUUID,
					"backend", provision.Backend,
					"error", err,
				)
				return fmt.Errorf("clear placement attempt for lease %s: %w", callback.LeaseUUID, err)
			}
			// Lease is ACTIVE — this was a re-provision attempt. Untrack and
			// let the reconciler detect the still-failed backend state.
			h.deps.Tracker.FinishClaimedInFlight(callback.LeaseUUID, provision.Generation)
			recordDuration()
			metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeFailed, provision.Backend, operation).Inc()

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

		// Clean up payload and placement after successful rejection, while the
		// in-flight entry still excludes a concurrent provision. Placement
		// deletion is revision-checked and backend-scoped so a stale callback can
		// never erase newer ownership.
		if h.deps.PayloadStore != nil {
			h.deps.PayloadStore.Delete(callback.LeaseUUID)
		}
		if err := h.deps.Orchestrator.DeletePlacementIfOwned(callback.LeaseUUID, provision.Backend); err != nil {
			slog.Warn("failed to conditionally clean up rejected lease placement",
				"lease_uuid", callback.LeaseUUID,
				"backend", provision.Backend,
				"error", err,
			)
		}

		// Only untrack AFTER successful rejection and conditional cleanup.
		h.deps.Tracker.FinishClaimedInFlight(callback.LeaseUUID, provision.Generation)
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeFailed, provision.Backend, operation).Inc()

		h.publishLeaseEvent(callback.LeaseUUID, backend.ProvisionStatusFailed, reason)

		slog.Info("lease rejected after provisioning failure",
			"lease_uuid", callback.LeaseUUID,
			"tenant", provision.Tenant,
			"rejected", rejected,
			"tx_hashes", txHashes,
			"reason", reason,
		)

	case backend.CallbackStatusDeprovisioned:
		// Without a close-owned claim this is an autonomous terminal callback.
		// Preserve the historical behavior of releasing the stuck provisioning
		// entry, while still surfacing retained-data ground truth.
		h.deps.Tracker.FinishClaimedInFlight(callback.LeaseUUID, provision.Generation)
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeError, provision.Backend, operation).Inc()
		if callback.Retained {
			h.publishRetainedLeaseNotice(callback.LeaseUUID)
		}
		slog.Warn("deprovision callback settled an in-flight provisioning operation",
			"lease_uuid", callback.LeaseUUID,
			"tenant", provision.Tenant,
			"backend", provision.Backend,
			"generation", provision.Generation,
			"retained", callback.Retained,
		)

	default:
		// Unknown status is treated as terminal to prevent leases from being stuck
		// in the in-flight map indefinitely. The reconciler will pick up the lease
		// and handle it based on its actual chain/backend state.
		h.deps.Tracker.FinishClaimedInFlight(callback.LeaseUUID, provision.Generation)
		recordDuration()
		metrics.ProvisioningTotal.WithLabelValues(metrics.OutcomeError, provision.Backend, operation).Inc()

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

	// Lease is no longer awaiting payload — update gauge.
	h.awaitingMu.Lock()
	delete(h.awaitingPayload, event.LeaseUUID)
	metrics.LeasesAwaitingPayload.Set(float64(len(h.awaitingPayload)))
	h.awaitingMu.Unlock()

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
	// Payload deletion happens later: when the lease is closed
	// (HandleLeaseClosed) or when a PENDING-failure callback rejects the
	// lease. Successful and ACTIVE-re-provision-failure paths intentionally
	// keep the payload so a subsequent re-provision can reuse the same
	// manifest. This also ensures the payload remains available for retry
	// if the backend fails or crashes before sending a callback.
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
			h.deps.PayloadStore.Delete(event.LeaseUUID)
			h.publishLeaseEvent(event.LeaseUUID, backend.ProvisionStatusFailed, rejectReasonPayloadCorrupted)
			return nil
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

	h.publishLeaseEvent(event.LeaseUUID, backend.ProvisionStatusProvisioning, "")
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

// Label sentinels for sanitized Prom label values.
const (
	labelBackendUnknown = "unknown"
	labelBackendInvalid = "invalid"
	labelStatusOther    = "other"
)

// Keep in sync with CallbackStatus* constants in internal/backend/client.go.
func sanitizeCallbackStatus(s backend.CallbackStatus) string {
	switch s {
	case backend.CallbackStatusSuccess, backend.CallbackStatusFailed, backend.CallbackStatusDeprovisioned:
		return string(s)
	default:
		return labelStatusOther
	}
}

// sanitizeBackendName bounds Prometheus label cardinality by collapsing any
// name outside the configured router's allowlist to "invalid". Empty values
// (pre-upgrade backends that don't populate CallbackPayload.Backend) map to
// "unknown". Returning the raw name was insufficient: a misbehaving sender
// could emit arbitrarily many distinct regex-valid values.
func (h *HandlerSet) sanitizeBackendName(name string) string {
	if name == "" {
		return labelBackendUnknown
	}
	if h.deps.BackendRouter == nil || h.deps.BackendRouter.GetBackendByName(name) == nil {
		return labelBackendInvalid
	}
	return name
}
