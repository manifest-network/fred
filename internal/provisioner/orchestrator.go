package provisioner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// ProvisionOpts contains optional parameters for provisioning.
type ProvisionOpts struct {
	Payload     []byte // Optional deployment payload
	PayloadHash string // Optional hex-encoded SHA-256 hash of payload
}

// provisionOutcome is the durable-placement consequence of a synchronous
// Provision result. Keep this classifier shared with the reconciler: the event
// and sweep paths must never disagree about whether it is safe to forget an
// attempted backend.
type provisionOutcome uint8

const (
	provisionOutcomeAccepted provisionOutcome = iota
	provisionOutcomeAlreadyExists
	provisionOutcomeDefinitiveFailure
	provisionOutcomeAmbiguous
)

// classifyProvisionOutcome classifies whether Provision definitely did or did
// not take effect. Unknown transport results and off-contract responses remain
// ambiguous: clearing their attempt could route a retry to a second backend
// even though the first backend accepted the request.
func classifyProvisionOutcome(err error) provisionOutcome {
	switch {
	case err == nil:
		return provisionOutcomeAccepted
	case errors.Is(err, backend.ErrAlreadyProvisioned):
		// A 409 is positive proof that this backend owns the lease.
		return provisionOutcomeAlreadyExists
	case errors.Is(err, backend.ErrValidation),
		errors.Is(err, backend.ErrInsufficientResources),
		errors.Is(err, backend.ErrCircuitOpen):
		// These errors are explicit refusals. In particular, an open circuit
		// means the request was never sent.
		return provisionOutcomeDefinitiveFailure
	default:
		return provisionOutcomeAmbiguous
	}
}

// ProvisionOrchestrator coordinates the provisioning flow.
// It routes to the appropriate backend, tracks the provision in-flight,
// and initiates the async provisioning call.
type ProvisionOrchestrator struct {
	providerUUID    string
	callbackBaseURL string
	router          BackendRouter
	tracker         InFlightTracker
	placementStore  PlacementStore
}

// ErrProvisionAttemptPending means a prior backend call still has an unknown
// outcome. Callers must not send another operation until reconciliation settles
// the durable attempt.
var ErrProvisionAttemptPending = errors.New("lease already has an unresolved durable provision attempt")

// ErrPlacementStoreUnavailable means a placement-dependent write path was
// invoked without durable placement storage. Such paths must fail closed before
// contacting a backend.
var ErrPlacementStoreUnavailable = errors.New("placement store is unavailable")

// setProvisionAttempt writes the durable intent immediately before the
// external call. An existing attempt is a benign idempotency result: regardless
// of which backend routing selected on this invocation, a previous call may
// already have taken effect and must be reconciled before another one is sent.
func setProvisionAttempt(store PlacementStore, leaseUUID, backendName string) (uint64, error) {
	if store == nil {
		return 0, nil
	}
	revision, err := store.SetAttempting(leaseUUID, backendName)
	if err != nil {
		if errors.Is(err, placement.ErrAttemptConflict) {
			return 0, ErrProvisionAttemptPending
		}
		return 0, err
	}
	return revision, nil
}

// settleProvisionAttempt applies the placement transition implied by a
// synchronous Provision result. Ambiguous outcomes intentionally do nothing.
// The caller decides how to surface persistence failures after the external
// call; it can no longer safely roll the backend operation back.
func settleProvisionAttempt(
	store PlacementStore,
	leaseUUID, backendName string,
	attemptRevision uint64,
	outcome provisionOutcome,
) error {
	if store == nil {
		return nil
	}
	switch outcome {
	case provisionOutcomeAccepted, provisionOutcomeAlreadyExists:
		_, err := store.ConfirmAttemptIfRevision(leaseUUID, backendName, attemptRevision)
		return err
	case provisionOutcomeDefinitiveFailure:
		_, err := store.ClearAttemptIfRevision(leaseUUID, backendName, attemptRevision)
		return err
	case provisionOutcomeAmbiguous:
		return nil
	default:
		return fmt.Errorf("unknown provision outcome %d", outcome)
	}
}

// NewProvisionOrchestrator creates a new ProvisionOrchestrator.
func NewProvisionOrchestrator(providerUUID, callbackBaseURL string, router BackendRouter, tracker InFlightTracker, placementStore PlacementStore) *ProvisionOrchestrator {
	return &ProvisionOrchestrator{
		providerUUID:    providerUUID,
		callbackBaseURL: callbackBaseURL,
		router:          router,
		tracker:         tracker,
		placementStore:  placementStore,
	}
}

// StartProvisioning handles the common provisioning flow for both lease creation
// and payload-triggered provisioning. It routes to the appropriate backend,
// tracks the provision in-flight, and initiates the async provisioning call.
//
// Returns nil if provisioning was started successfully or the lease is already in-flight.
// Returns an error if routing fails or the backend call fails.
func (o *ProvisionOrchestrator) StartProvisioning(ctx context.Context, lease *billingtypes.Lease, opts ProvisionOpts) error {
	// Extract lease items and primary SKU for routing
	items := ExtractLeaseItems(lease)
	sku := ExtractRoutingSKU(lease)
	totalQuantity := TotalLeaseQuantity(lease)

	// Route to appropriate backend, honoring existing placement for restored/placed leases (ENG-333)
	backendClient, err := routeForProvisionHonoringPlacement(ctx, o.router, o.placementStore, lease.Uuid, sku, o.tracker.InFlightCountsByBackend())
	if err != nil {
		// A placement naming an unknown backend is refused, never re-routed
		// (ENG-635). Logged at ERROR because it needs operator action — the
		// recorded backend is missing from config — and because the alternative
		// (silently provisioning elsewhere) destroys tenant data.
		slog.Error("refusing to provision: lease is placed on a backend the router does not know",
			"lease_uuid", lease.Uuid,
			"sku", sku,
			"error", err,
		)
		return err
	}
	if backendClient == nil {
		slog.Error("no backend available for provisioning",
			"lease_uuid", lease.Uuid,
			"sku", sku,
		)
		return fmt.Errorf("%w: lease %s", ErrNoBackendAvailable, lease.Uuid)
	}

	// Atomically track in-flight BEFORE calling Provision to prevent:
	// 1. Race with reconciler (both use TryTrackInFlightWithGeneration)
	// 2. Race with fast backend response (callback arriving before tracking)
	inFlightGeneration, tracked := o.tracker.TryTrackInFlightWithGeneration(
		lease.Uuid, lease.Tenant, items, backendClient.Name(),
	)
	if !tracked {
		slog.Debug("lease already in-flight, skipping",
			"lease_uuid", lease.Uuid,
		)
		return nil
	}

	// Build provision request
	req := backend.ProvisionRequest{
		LeaseUUID:    lease.Uuid,
		Tenant:       lease.Tenant,
		ProviderUUID: o.providerUUID,
		Items:        items,
		CallbackURL:  BuildCallbackURLForGeneration(o.callbackBaseURL, inFlightGeneration),
		Payload:      opts.Payload,
	}
	// Only include PayloadHash when we have the actual payload
	if opts.Payload != nil && opts.PayloadHash != "" {
		req.PayloadHash = opts.PayloadHash
	}

	// Persist intent BEFORE the external call. A failure here is fail-closed:
	// no backend is contacted. A pre-existing attempt is a benign duplicate and
	// is left for the fleet snapshot to confirm or clear.
	attemptRevision, err := setProvisionAttempt(o.placementStore, lease.Uuid, backendClient.Name())
	if err != nil {
		o.tracker.UntrackInFlightIfGeneration(lease.Uuid, inFlightGeneration)
		if errors.Is(err, ErrProvisionAttemptPending) {
			slog.Debug("lease already has an unresolved durable attempt, skipping backend call",
				"lease_uuid", lease.Uuid,
				"routed_backend", backendClient.Name(),
			)
			return nil
		}
		slog.Error("failed to record provision attempt; refusing backend call",
			"lease_uuid", lease.Uuid,
			"backend", backendClient.Name(),
			"error", err,
		)
		return fmt.Errorf("%w: record placement attempt: %w", ErrProvisioningFailed, err)
	}

	// Start provisioning (async - backend will call back), then settle only the
	// attempt field. Confirmed ownership is never cleared by a failed retry.
	provisionErr := backendClient.Provision(ctx, req)
	outcome := classifyProvisionOutcome(provisionErr)
	if errors.Is(provisionErr, backend.ErrInsufficientResources) {
		metrics.BackendInsufficientResourcesTotal.WithLabelValues(backendClient.Name()).Inc()
	}
	if err := settleProvisionAttempt(
		o.placementStore, lease.Uuid, backendClient.Name(), attemptRevision, outcome,
	); err != nil {
		// The write-ahead Attempt remains the conservative truth on a failed
		// settlement. The backend has already answered, so retrying the external
		// call here would be less safe than letting callback/snapshot repair it.
		slog.Warn("failed to settle provision placement",
			"lease_uuid", lease.Uuid,
			"backend", backendClient.Name(),
			"outcome", outcome,
			"error", err,
		)
	}

	switch outcome {
	case provisionOutcomeAccepted:
		// Keep in-flight until the async callback owns chain acknowledgement.
	case provisionOutcomeAlreadyExists:
		// A duplicate is positive ownership evidence, but it may not emit a new
		// callback. Untrack and let the queryable reconciler state acknowledge it.
		o.tracker.UntrackInFlightIfGeneration(lease.Uuid, inFlightGeneration)
		slog.Info("backend already owns lease; confirmed placement and deferred to reconciliation",
			"lease_uuid", lease.Uuid,
			"backend", backendClient.Name(),
		)
		return nil
	case provisionOutcomeDefinitiveFailure, provisionOutcomeAmbiguous:
		o.tracker.UntrackInFlightIfGeneration(lease.Uuid, inFlightGeneration)
		slog.Error("failed to start provisioning",
			"lease_uuid", lease.Uuid,
			"sku", sku,
			"total_quantity", totalQuantity,
			"backend", backendClient.Name(),
			"outcome", outcome,
			"error", provisionErr,
		)
		return fmt.Errorf("%w: %w", ErrProvisioningFailed, provisionErr)
	}

	// Log success with appropriate detail level
	if opts.Payload != nil {
		slog.Info("provisioning started with payload",
			"lease_uuid", lease.Uuid,
			"tenant", lease.Tenant,
			"sku", sku,
			"total_quantity", totalQuantity,
			"backend", backendClient.Name(),
			"payload_size", len(opts.Payload),
		)
	} else {
		slog.Info("provisioning started",
			"lease_uuid", lease.Uuid,
			"tenant", lease.Tenant,
			"sku", sku,
			"total_quantity", totalQuantity,
			"backend", backendClient.Name(),
		)
	}

	return nil
}

// routeForProvisionHonoringPlacement returns the backend that already holds the
// lease's data (from placement) when one is recorded, keeping a restored or
// already-placed lease pinned to the backend with its volumes (ENG-333).
//
// A lease with NO placement record routes freely by least-loaded selection —
// unchanged behavior, and the path every new lease takes.
//
// A lease WHOSE RECORD DOES NOT RESOLVE returns ErrPlacementUnresolvable rather
// than routing somewhere else (ENG-635). fred never substitutes a backend: the
// recorded machine holds the lease's data, so provisioning on a peer creates a
// brand-new empty volume while the real data sits untouched on the machine that
// is merely absent from the router — which is what happens when a backend is
// removed, renamed or paused. That failure fires on a timer, for every affected
// lease at once, and reports success to its caller. Refusing is the safe
// direction: the lease stops making progress until an operator restores the
// backend, and nothing is destroyed meanwhile.
func routeForProvisionHonoringPlacement(
	ctx context.Context,
	router BackendRouter,
	placementStore PlacementStore,
	leaseUUID, sku string,
	inFlightByBackend map[string]int,
) (backend.Backend, error) {
	if placementStore != nil {
		p := placementStore.Lookup(leaseUUID)
		switch p.State() {
		case placement.StateUnusable:
			return nil, fmt.Errorf("%w: lease %s has an unusable placement record",
				ErrPlacementUnresolvable, leaseUUID)
		case placement.StateConfirmed:
			b := router.GetBackendByName(p.Backend)
			if b == nil {
				return nil, fmt.Errorf("%w: lease %s is placed on %q",
					ErrPlacementUnresolvable, leaseUUID, p.Backend)
			}
			return b, nil
		case placement.StateAbsent, placement.StateAttempting:
			// An unresolved attempt is safety evidence for reconciliation and
			// deprovision, but never an ownership pin for provision routing.
		}
	}
	return router.RouteForProvision(ctx, sku, inFlightByBackend), nil
}

// SetPlacementAttempting exposes the same fail-closed write-ahead primitive to
// restore. The API must call it before backend.Restore.
func (o *ProvisionOrchestrator) SetPlacementAttempting(leaseUUID, backendName string) (uint64, error) {
	if o.placementStore == nil {
		return 0, ErrPlacementStoreUnavailable
	}
	return setProvisionAttempt(o.placementStore, leaseUUID, backendName)
}

// ConfirmPlacementIfRevision promotes only the exact restore attempt created by
// the caller. A false result means a callback or newer operation superseded it.
func (o *ProvisionOrchestrator) ConfirmPlacementIfRevision(leaseUUID, backendName string, revision uint64) (bool, error) {
	if o.placementStore == nil {
		return false, ErrPlacementStoreUnavailable
	}
	return o.placementStore.ConfirmAttemptIfRevision(leaseUUID, backendName, revision)
}

// ClearPlacementAttemptIfRevision clears only the exact refused restore
// attempt, preserving a callback/newer operation that won the race.
func (o *ProvisionOrchestrator) ClearPlacementAttemptIfRevision(leaseUUID, backendName string, revision uint64) (bool, error) {
	if o.placementStore == nil {
		return false, ErrPlacementStoreUnavailable
	}
	return o.placementStore.ClearAttemptIfRevision(leaseUUID, backendName, revision)
}

// ConfirmPlacement promotes a matching attempt to confirmed ownership. It is
// idempotent when that backend is already confirmed.
func (o *ProvisionOrchestrator) ConfirmPlacement(leaseUUID, backendName string) error {
	if o.placementStore == nil {
		return nil
	}
	return o.placementStore.Confirm(leaseUUID, backendName)
}

// ClearPlacementAttempt clears only the matching attempt, preserving any
// previously confirmed owner. Absence is treated as an idempotent no-op so a
// callback arriving after synchronous settlement does not create log noise.
func (o *ProvisionOrchestrator) ClearPlacementAttempt(leaseUUID, backendName string) error {
	if o.placementStore == nil || backendName == "" {
		return nil
	}
	p := o.placementStore.Lookup(leaseUUID)
	if p.Attempt == "" {
		return nil
	}
	if p.Attempt != backendName {
		return fmt.Errorf("placement attempt for lease %s is %q, not callback backend %q",
			leaseUUID, p.Attempt, backendName)
	}
	_, err := o.placementStore.ClearAttemptIfRevision(leaseUUID, backendName, p.Revision())
	return err
}

// DeletePlacementIfOwned removes terminal PENDING placement only when every
// backend named by the current record belongs to this operation. Delete uses
// the observed revision so a concurrent sync/attempt wins rather than being
// erased by a stale callback.
func (o *ProvisionOrchestrator) DeletePlacementIfOwned(leaseUUID, backendName string) error {
	if o.placementStore == nil {
		return nil
	}
	p := o.placementStore.Lookup(leaseUUID)
	if p.State() == placement.StateAbsent {
		return nil
	}
	if p.State() == placement.StateUnusable {
		return fmt.Errorf("refusing to delete unusable placement for lease %s", leaseUUID)
	}
	if (p.Backend != "" && p.Backend != backendName) ||
		(p.Attempt != "" && p.Attempt != backendName) {
		return fmt.Errorf("refusing to delete placement for lease %s owned by backend=%q attempt=%q from callback backend %q",
			leaseUUID, p.Backend, p.Attempt, backendName)
	}
	deleted, err := o.placementStore.DeleteIfRevision(leaseUUID, p.Revision())
	if err != nil {
		return err
	}
	if !deleted {
		slog.Debug("placement changed during terminal callback cleanup; preserving newer record",
			"lease_uuid", leaseUUID,
			"backend", backendName,
			"revision", p.Revision(),
		)
	}
	return nil
}

// Deprovision tears down a lease's backend resources. The backend is resolved
// POSITIVELY — from the placement record, then the in-flight tracker. It never
// guesses a default backend from the SKU: in a multi-backend pool a SKU is not
// pinned to one backend, so a guessed deprovision is a phantom no-op that
// reports success while stranding the real volume on another backend (ENG-335).
// When the backend cannot be positively resolved, all backends are swept;
// deprovision is idempotent, so the real holder is torn down and the rest are
// harmless no-ops.
//
// Returns nil only when every positively resolved candidate succeeds, or when
// an unresolved fallback sweep reaches every configured backend without error
// and no positively named candidate lies outside the current configuration.
func (o *ProvisionOrchestrator) Deprovision(ctx context.Context, leaseUUID string) error {
	var provision InFlightProvision
	wasInFlight := false
	claimFinished := false
	leaseActionClaimed := false
	if observed, exists := o.tracker.GetInFlight(leaseUUID); exists {
		claimed, ok := o.tracker.TryClaimInFlightForDeprovision(leaseUUID, observed.Generation)
		if !ok {
			// The observed generation either changed or is already owned by a
			// callback/timeout settlement actor. Do not proceed without its backend
			// candidate and do not steal its claim; the close event can retry after
			// that exact generation settles or releases ownership.
			return fmt.Errorf("%w: lease %s: in-flight generation %d is already being settled",
				ErrDeprovisionFailed, leaseUUID, observed.Generation)
		}
		provision = claimed
		wasInFlight = true
		defer func() {
			// Every fallible backend/routing path, and a backend panic unwinding the
			// ordinary call stack, leaves this exact generation retryable.
			if !claimFinished && !o.tracker.ReleaseInFlightClaim(leaseUUID, provision.Generation) {
				slog.Error("failed to release deprovision settlement claim",
					"lease_uuid", leaseUUID,
					"backend", provision.Backend,
					"generation", provision.Generation,
				)
			}
		}()
	} else {
		// A close without an in-flight provision still races reconciliation. Hold
		// the same lease action fence workers use so a stale chain/backend snapshot
		// cannot provision while teardown is in progress (or vice versa).
		if !o.tracker.TryClaimLeaseAction(leaseUUID) {
			return fmt.Errorf("%w: lease %s: another lifecycle action owns the lease",
				ErrDeprovisionFailed, leaseUUID)
		}
		leaseActionClaimed = true
		defer func() {
			if leaseActionClaimed && !o.tracker.ReleaseLeaseAction(leaseUUID) {
				slog.Error("failed to release deprovision lease action claim",
					"lease_uuid", leaseUUID)
			}
		}()
	}
	finishInFlight := func() error {
		if !wasInFlight {
			return nil
		}
		if !o.tracker.FinishClaimedInFlight(leaseUUID, provision.Generation) {
			return fmt.Errorf("%w: lease %s: lost claim for in-flight generation %d",
				ErrDeprovisionFailed, leaseUUID, provision.Generation)
		}
		claimFinished = true
		return nil
	}

	// Every positively named backend is a possible holder. Backend and Attempt
	// are independent facts, and the claimed in-flight entry may predate either;
	// deprovision all distinct candidates rather than allowing one to overwrite
	// another.
	candidateNames := make([]string, 0, 3)
	seenNames := make(map[string]struct{}, 3)
	addCandidateName := func(name string) {
		if name == "" {
			return
		}
		if _, exists := seenNames[name]; exists {
			return
		}
		seenNames[name] = struct{}{}
		candidateNames = append(candidateNames, name)
	}

	unresolved := false
	unaccountablePlacement := false
	if o.placementStore != nil {
		p := o.placementStore.Lookup(leaseUUID)
		addCandidateName(p.Backend)
		addCandidateName(p.Attempt)
		if p.State() == placement.StateUnusable {
			for _, backendName := range p.ConflictBackends {
				addCandidateName(backendName)
			}
			// A conflict with a complete durable candidate set can be settled by
			// contacting every candidate. Legacy conflicts and malformed records do
			// not identify the full historical owner set, so even a successful sweep
			// of today's router cannot be reported as terminal success.
			if !p.Conflict || p.ConflictOwnersUnknown || len(p.ConflictBackends) < 2 {
				unresolved = true
				unaccountablePlacement = true
				slog.Warn("placement ownership is not fully accountable, will sweep all backends and fail closed",
					"lease_uuid", leaseUUID,
					"conflict", p.Conflict,
					"conflict_backends", p.ConflictBackends,
					"conflict_owners_unknown", p.ConflictOwnersUnknown,
				)
			}
		}
	}
	if wasInFlight {
		addCandidateName(provision.Backend)
	}

	candidates := make([]backend.Backend, 0, len(candidateNames))
	unreachedCandidates := make([]string, 0)
	for _, name := range candidateNames {
		b := o.router.GetBackendByName(name)
		if b == nil {
			unresolved = true
			unreachedCandidates = append(unreachedCandidates, name)
			slog.Warn("candidate backend not found, will sweep all backends",
				"lease_uuid", leaseUUID, "backend_name", name)
			continue
		}
		candidates = append(candidates, b)
	}

	if !unresolved && len(candidates) > 0 {
		var candidateErrs []error
		for _, b := range candidates {
			if err := b.Deprovision(ctx, leaseUUID); err != nil {
				slog.Error("failed to deprovision candidate backend",
					"lease_uuid", leaseUUID, "backend", b.Name(), "error", err)
				candidateErrs = append(candidateErrs, fmt.Errorf("backend %s: %w", b.Name(), err))
				continue
			}
			slog.Info("deprovisioned successfully",
				"lease_uuid", leaseUUID, "backend", b.Name())
		}
		if len(candidateErrs) > 0 {
			return fmt.Errorf("%w: lease %s: %w", ErrDeprovisionFailed, leaseUUID, errors.Join(candidateErrs...))
		}
		// Placement is intentionally NOT deleted here (ENG-333). It is a derived
		// index of where the lease's data lives; if the backend retained the
		// volumes, the placement must survive close so a restore can route to it.
		// The reconciler is the sole pruner (cleanupOrphanedPlacements).
		return finishInFlight()
	}

	// Fallback: backend could not be positively resolved → sweep all backends.
	// Idempotent, so the holder is torn down and the rest no-op. We deliberately
	// do NOT emit a per-backend "deprovisioned successfully" here — that
	// phantom-success line (against a backend that never held the lease) is what
	// made ENG-335 hard to diagnose. One summary line names the outcome instead.
	backends := o.router.Backends()
	var sweepErrs []error
	swept := make([]string, 0, len(backends))
	failed := make([]string, 0)
	for _, b := range backends {
		if err := b.Deprovision(ctx, leaseUUID); err != nil {
			failed = append(failed, b.Name())
			sweepErrs = append(sweepErrs, fmt.Errorf("backend %s: %w", b.Name(), err))
		} else {
			swept = append(swept, b.Name())
		}
	}
	// Log level depends on whether an unresolved placement is expected. With the
	// placement store disabled, the sweep is the normal resolution path for any
	// not-in-flight close, so it is not anomalous — reserve WARN for actual
	// backend failures and for the ENG-335 case where placement IS enabled but
	// could not resolve the backend.
	logArgs := []any{
		"lease_uuid", leaseUUID,
		"swept_ok_or_noop", swept,
		"failed", failed,
		"positively_named_but_unconfigured", unreachedCandidates,
	}
	switch {
	case len(failed) > 0:
		slog.Warn("deprovision swept all backends with failures", logArgs...)
	case o.placementStore == nil:
		slog.Info("deprovision swept all backends (placement store disabled)", logArgs...)
	default:
		slog.Warn("deprovision swept all backends (placement unresolved, ENG-335)", logArgs...)
	}
	// Placement is intentionally NOT deleted here (ENG-333); see resolved path.
	// With no authoritative holder, every configured backend remains a possible
	// holder. A no-op success from one backend therefore cannot prove that a
	// failing peer did not retain the lease.
	for _, name := range unreachedCandidates {
		sweepErrs = append(sweepErrs, fmt.Errorf("%w: positively identified backend %q is not configured",
			ErrPlacementUnresolvable, name))
	}
	if unaccountablePlacement {
		sweepErrs = append(sweepErrs, fmt.Errorf("%w: durable placement ownership is unusable or incomplete",
			ErrPlacementUnresolvable))
	}
	if len(sweepErrs) > 0 {
		return fmt.Errorf("%w: lease %s: %w", ErrDeprovisionFailed, leaseUUID, errors.Join(sweepErrs...))
	}
	return finishInFlight()
}
