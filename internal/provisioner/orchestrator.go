package provisioner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/util"
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
		// HTTPClient maps every 409 response to this sentinel without validating
		// a backend-authored error code. An intermediary-generated 409 therefore
		// cannot prove that the selected backend owns the lease. Keep the durable
		// Attempt until matching positive inventory confirms it or an operator
		// supplies a remote cancellation/refusal proof.
		return provisionOutcomeAmbiguous
	case errors.Is(err, backend.ErrValidation),
		errors.Is(err, backend.ErrCapacityRefused),
		errors.Is(err, backend.ErrCircuitOpen):
		// Validation and coded capacity errors carry parsed backend-authored
		// verdicts, while an open circuit means the request was never sent. The base
		// ErrInsufficientResources is not included: legacy and intermediary 503s map
		// to that sentinel without proving that the backend refused the request.
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
	operations      ProvisionOperations
	placementStore  ProvisionPlacement
	startEvents     ProvisionStartEventSink

	deprovisionCandidatesMu sync.Mutex
	deprovisionCandidates   map[string]map[string]struct{}
}

// ProvisionStartEventSink is the one best-effort event hook needed by backend
// initiation. Keeping it status-specific prevents the orchestrator from
// acquiring the handler or broker APIs merely to publish one ordered fact.
type ProvisionStartEventSink interface {
	PublishProvisionStarting(leaseUUID string)
}

type provisionStartEventSinkFunc func(string)

func (publish provisionStartEventSinkFunc) PublishProvisionStarting(leaseUUID string) {
	publish(leaseUUID)
}

// ErrProvisionAttemptPending is retained for source compatibility with callers
// that mapped the former raw restore bridge. New typed placement consumers use
// placement.ErrAttemptConflict directly; this sentinel grants no authority.
var ErrProvisionAttemptPending = errors.New("lease already has an unresolved durable provision attempt")

// ErrPlacementStoreUnavailable means a placement-dependent write path was
// invoked without durable placement storage. Such paths must fail closed before
// contacting a backend.
var ErrPlacementStoreUnavailable = errors.New("placement store is unavailable")

// NewProvisionOrchestrator creates a capability-safe ProvisionOrchestrator.
// Durable placement authority and one typed operation registry are mandatory:
// a provisioning coordinator without either dependency could contact a second
// backend or emit an unscoped callback.
func NewProvisionOrchestrator(
	providerUUID, callbackBaseURL string,
	router BackendRouter,
	operations ProvisionOperations,
	placementStore ProvisionPlacement,
	startEvents ProvisionStartEventSink,
) (*ProvisionOrchestrator, error) {
	if util.IsNilInterface(operations) {
		return nil, errors.New("operation registry is required")
	}
	if util.IsNilInterface(placementStore) {
		return nil, ErrPlacementStoreUnavailable
	}
	if util.IsNilInterface(startEvents) {
		startEvents = nil
	}
	return &ProvisionOrchestrator{
		providerUUID:          providerUUID,
		callbackBaseURL:       callbackBaseURL,
		router:                router,
		operations:            operations,
		placementStore:        placementStore,
		startEvents:           startEvents,
		deprovisionCandidates: make(map[string]map[string]struct{}),
	}, nil
}

func isNilPlacementAuthorityStore(store PlacementAuthorityStore) bool {
	return util.IsNilInterface(store)
}

// rememberedDeprovisionCandidates returns process-local positive candidates
// retained from a failed close after its provisioning tracker entry was
// released. They do not participate in load balancing, callback timeouts, or
// provision admission, but let later close/orphan retries remain fail-closed.
func (o *ProvisionOrchestrator) rememberedDeprovisionCandidates(leaseUUID string) []string {
	o.deprovisionCandidatesMu.Lock()
	defer o.deprovisionCandidatesMu.Unlock()

	names := make([]string, 0, len(o.deprovisionCandidates[leaseUUID]))
	for name := range o.deprovisionCandidates[leaseUUID] {
		names = append(names, name)
	}
	return names
}

func (o *ProvisionOrchestrator) rememberDeprovisionCandidates(leaseUUID string, names []string) {
	if len(names) == 0 {
		return
	}
	o.deprovisionCandidatesMu.Lock()
	defer o.deprovisionCandidatesMu.Unlock()

	if o.deprovisionCandidates == nil {
		o.deprovisionCandidates = make(map[string]map[string]struct{})
	}
	remembered := o.deprovisionCandidates[leaseUUID]
	if remembered == nil {
		remembered = make(map[string]struct{}, len(names))
		o.deprovisionCandidates[leaseUUID] = remembered
	}
	for _, name := range names {
		if name != "" {
			remembered[name] = struct{}{}
		}
	}
}

func (o *ProvisionOrchestrator) forgetDeprovisionCandidates(leaseUUID string) {
	o.deprovisionCandidatesMu.Lock()
	defer o.deprovisionCandidatesMu.Unlock()
	delete(o.deprovisionCandidates, leaseUUID)
}

// forgetDeprovisionCandidate retires one outstanding teardown candidate after
// a positive deprovisioned callback. This also lets a later reconciler/orphan
// teardown clean up retry state left by a poisoned close message.
func (o *ProvisionOrchestrator) forgetDeprovisionCandidate(leaseUUID, backendName string) {
	if backendName == "" {
		return
	}
	o.deprovisionCandidatesMu.Lock()
	defer o.deprovisionCandidatesMu.Unlock()

	remembered := o.deprovisionCandidates[leaseUUID]
	delete(remembered, backendName)
	if len(remembered) == 0 {
		delete(o.deprovisionCandidates, leaseUUID)
	}
}

// StartProvisioningClaimed starts an event-driven provision under the exact
// lease lifecycle claim held across the caller's authoritative chain re-read.
func (o *ProvisionOrchestrator) StartProvisioningClaimed(
	ctx context.Context,
	claim operation.LeaseClaim,
	lease *billingtypes.Lease,
	opts ProvisionOpts,
) error {
	if !claim.Valid() {
		return fmt.Errorf("%w: invalid lease initiation claim", ErrProvisioningFailed)
	}
	if lease == nil {
		return fmt.Errorf("%w: lease is required", ErrProvisioningFailed)
	}
	if lease.State != billingtypes.LEASE_STATE_PENDING {
		return fmt.Errorf("%w: lease %s is %s, not pending",
			ErrProvisioningFailed, lease.Uuid, lease.State)
	}
	return o.startProvisioning(ctx, claim, lease, opts)
}

func (o *ProvisionOrchestrator) startProvisioning(
	ctx context.Context,
	claim operation.LeaseClaim,
	lease *billingtypes.Lease,
	opts ProvisionOpts,
) error {
	if !claim.Valid() {
		return fmt.Errorf("%w: invalid lease initiation claim", ErrProvisioningFailed)
	}
	// Extract lease items and primary SKU for routing
	items := ExtractLeaseItems(lease)
	sku := ExtractRoutingSKU(lease)
	totalQuantity := TotalLeaseQuantity(lease)

	// Route to appropriate backend, honoring existing placement for restored/placed leases (ENG-333)
	backendClient, err := routeForProvisionHonoringPlacement(
		ctx, o.router, o.placementStore, lease.Uuid, sku, o.operations.CountsByBackend(),
	)
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
	baseline := o.placementStore.CurrentAdmissionBaseline()
	placementRecord := o.placementStore.Lookup(lease.Uuid)
	var recordlessScope placement.AdmissionScope
	if placementRecord.State() == placement.StateAbsent {
		recordlessScope, err = o.placementStore.ScopeAdmission(
			baseline, backendTopologyNames(o.router),
		)
		if err != nil {
			return fmt.Errorf("%w: scope placement admission: %w", ErrProvisioningFailed, err)
		}
		if !recordlessScope.Allows(backendClient.Name()) {
			return fmt.Errorf("%w: %w: router selected %q",
				ErrProvisioningFailed, placement.ErrBackendOutsideAdmissionScope,
				backendClient.Name())
		}
	}

	// Atomically register the operation BEFORE recording placement or calling
	// Provision. The returned capability owns exact abort and callback identity;
	// opaque operation IDs cannot be confused with placement revisions.
	spec := operation.TrackSpec{
		LeaseUUID:     lease.Uuid,
		Tenant:        lease.Tenant,
		Items:         items,
		Backend:       backendClient.Name(),
		Kind:          operation.KindProvision,
		TokenRequired: true,
	}
	initiationResult := o.operations.TryInitiateClaimed(claim, spec)
	if !initiationResult.Started() {
		if initiationResult.Outcome() != operation.TrackBusy {
			return fmt.Errorf("%w: register operation: outcome %d",
				ErrProvisioningFailed, initiationResult.Outcome())
		}
		slog.Debug("lease already in-flight, skipping",
			"lease_uuid", lease.Uuid,
		)
		return nil
	}
	initiation := initiationResult.Capability()
	abortOperation := func(reason string) {
		completion := o.operations.AbortInitiation(initiation)
		if completion != operation.InitiationAborted &&
			completion != operation.InitiationFinished &&
			completion != operation.InitiationSettling {
			slog.Error("failed to abort exact provision operation",
				"lease_uuid", lease.Uuid,
				"backend", backendClient.Name(),
				"reason", reason,
			)
		}
	}

	callbackURL, err := BuildCallbackURLForOperation(o.callbackBaseURL, initiation.ID())
	if err != nil {
		abortOperation("callback URL construction failed")
		return fmt.Errorf("%w: build callback URL: %w", ErrProvisioningFailed, err)
	}

	// Build provision request
	req := backend.ProvisionRequest{
		LeaseUUID:    lease.Uuid,
		Tenant:       lease.Tenant,
		ProviderUUID: o.providerUUID,
		Items:        items,
		CallbackURL:  callbackURL,
		Payload:      opts.Payload,
	}
	// Only include PayloadHash when we have the actual payload
	if opts.Payload != nil && opts.PayloadHash != "" {
		req.PayloadHash = opts.PayloadHash
	}

	// Persist intent BEFORE the external call under the durable topology
	// baseline. The state-specific methods make rerouting impossible by
	// construction: a recordless lease is insert-if-absent, while an existing
	// placement can only attempt its exact confirmed owner revision.
	var (
		attemptToken placement.AttemptToken
		attemptSet   bool
	)
	switch placementRecord.State() {
	case placement.StateAbsent:
		attemptToken, attemptSet, err = o.placementStore.BeginNewAttempt(
			recordlessScope, lease.Uuid, backendClient.Name(), initiation.ID(),
		)
	case placement.StateConfirmed:
		attemptToken, attemptSet, err = o.placementStore.BeginOwnedAttempt(
			baseline, placementRecord.RecordRevision(), backendClient.Name(), initiation.ID(),
		)
	case placement.StateAttempting:
		err = placement.ErrAttemptConflict
	case placement.StateUnusable:
		err = placement.ErrUnusablePlacement
	default:
		err = placement.ErrUnusablePlacement
	}
	if err != nil {
		abortOperation("placement attempt refused")
		if errors.Is(err, placement.ErrAttemptConflict) {
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
	if !attemptSet {
		abortOperation("placement changed before attempt")
		current := o.placementStore.Lookup(lease.Uuid)
		if current.Attempt != "" {
			slog.Debug("lease acquired an unresolved durable attempt before dispatch; skipping duplicate",
				"lease_uuid", lease.Uuid,
				"attempted_backend", current.Attempt,
			)
			return nil
		}
		return fmt.Errorf("%w: placement changed before write-ahead attempt", ErrProvisioningFailed)
	}

	if !o.operations.BeginCall(initiation) {
		refused, refuseErr := o.placementStore.RefuseAttempt(attemptToken)
		abortOperation("operation did not enter calling phase")
		if refuseErr != nil {
			return fmt.Errorf("%w: enter backend call phase; refuse unsent placement attempt: %w",
				ErrProvisioningFailed, refuseErr)
		}
		if !refused {
			return fmt.Errorf("%w: enter backend call phase; unsent placement attempt was superseded",
				ErrProvisioningFailed)
		}
		return fmt.Errorf("%w: enter backend call phase", ErrProvisioningFailed)
	}
	// The durable attempt and call phase are visible before the status event,
	// and that event is visible before the backend can deliver a terminal
	// callback. This prevents both a start event for a failed phase transition
	// and a stale post-return Provisioning event.
	publishProvisionStartingBestEffort(o.startEvents, lease.Uuid, backendClient.Name())

	// Start provisioning (async - backend will call back), then settle only the
	// attempt field. Confirmed ownership is never cleared by a failed retry.
	provisionErr := invokeBackendProvision(ctx, backendClient, req)
	outcome := classifyProvisionOutcome(provisionErr)
	if errors.Is(provisionErr, backend.ErrInsufficientResources) {
		metrics.BackendInsufficientResourcesTotal.WithLabelValues(backendClient.Name()).Inc()
	}
	var completion operation.InitiationCompletion
	if outcome == provisionOutcomeAccepted {
		completion = o.operations.Activate(initiation)
	} else {
		completion = o.operations.AbortInitiation(initiation)
	}
	switch completion {
	case operation.InitiationSettling, operation.InitiationFinished:
		// An authenticated inline callback claimed or finished the exact
		// operation while Provision was on the stack. Its terminal verdict is
		// stronger than the synchronous return and owns placement/chain/event
		// settlement, including when that return is an error.
		slog.Info("inline provision callback superseded synchronous backend result",
			"lease_uuid", lease.Uuid,
			"backend", backendClient.Name(),
			"outcome", outcome,
		)
		return nil
	case operation.InitiationActivated:
		if outcome != provisionOutcomeAccepted {
			return fmt.Errorf("%w: invalid accepted completion for failed backend call", ErrProvisioningFailed)
		}
	case operation.InitiationAborted:
		if outcome == provisionOutcomeAccepted {
			return fmt.Errorf("%w: accepted backend call was aborted", ErrProvisioningFailed)
		}
	default:
		return fmt.Errorf("%w: complete backend call phase", ErrProvisioningFailed)
	}

	var settleErr error
	switch outcome {
	case provisionOutcomeAccepted:
		_, settleErr = o.placementStore.ConfirmAttempt(attemptToken)
	case provisionOutcomeDefinitiveFailure:
		_, settleErr = o.placementStore.RefuseAttempt(attemptToken)
	case provisionOutcomeAmbiguous:
		// Preserve the typed durable attempt. Matching positive inventory may
		// confirm it; silence cannot prove the remote effect will never commit.
	default:
		settleErr = fmt.Errorf("unknown provision outcome %d", outcome)
	}
	if settleErr != nil {
		// The write-ahead Attempt remains the conservative truth on a failed
		// settlement. The backend has already answered, so retrying the external
		// call here would be less safe than leaving it for callback, matching
		// positive inventory, or operator repair.
		slog.Warn("failed to settle provision placement",
			"lease_uuid", lease.Uuid,
			"backend", backendClient.Name(),
			"outcome", outcome,
			"error", settleErr,
		)
	}

	switch outcome {
	case provisionOutcomeAccepted:
	case provisionOutcomeDefinitiveFailure, provisionOutcomeAmbiguous:
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
	placementStore PlacementView,
	leaseUUID, sku string,
	inFlightByBackend map[string]int,
) (backend.Backend, error) {
	return routeForProvisionHonoringPlacementAmong(
		ctx, router, placementStore, leaseUUID, sku, nil, inFlightByBackend,
	)
}

// routeForProvisionHonoringPlacementAmong preserves exact durable affinity and
// applies eligibleBackends only to a recordless placement. A non-nil empty set
// therefore means "no new-placement candidate", while nil retains unrestricted
// event-path routing. Confirmed owners never move because of a health filter.
func routeForProvisionHonoringPlacementAmong(
	ctx context.Context,
	router BackendRouter,
	placementStore PlacementView,
	leaseUUID, sku string,
	eligibleBackends map[string]struct{},
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
	if eligibleBackends != nil {
		return router.RouteForProvisionAmong(ctx, sku, eligibleBackends, inFlightByBackend), nil
	}
	return router.RouteForProvision(ctx, sku, inFlightByBackend), nil
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
	var provision operation.Record
	var settlementClaim operation.SettlementClaim
	var leaseClaim operation.LeaseClaim
	wasInFlight := false
	claimFinished := false
	leaseActionClaimed := false
	if observed, exists := o.operations.Lookup(leaseUUID); exists {
		claimResult := o.operations.TryClaimDeprovision(leaseUUID, observed.ID)
		if !claimResult.Claimed() {
			// The observed operation either changed or is already owned by a
			// callback/timeout settlement actor. Do not proceed without its backend
			// candidate and do not steal its claim.
			return fmt.Errorf("%w: lease %s: operation is already being settled (outcome %d)",
				ErrDeprovisionFailed, leaseUUID, claimResult.Outcome())
		}
		provision = claimResult.Record()
		settlementClaim = claimResult.Claim()
		wasInFlight = true
		defer func() {
			// Every fallible backend/routing path, and a backend panic, leaves this
			// exact operation retryable without releasing a newer claim.
			if !claimFinished && !o.operations.ReleaseSettlement(settlementClaim) {
				slog.Error("failed to release deprovision settlement claim",
					"lease_uuid", leaseUUID,
					"backend", provision.Backend,
				)
			}
		}()
	} else {
		// A close without an in-flight provision still races reconciliation. Hold
		// the same lease action fence workers use so a stale chain/backend snapshot
		// cannot provision while teardown is in progress (or vice versa).
		claimResult := o.operations.TryClaimLeaseNow(leaseUUID)
		if !claimResult.Acquired() {
			return fmt.Errorf("%w: lease %s: another lifecycle action owns the lease",
				ErrDeprovisionFailed, leaseUUID)
		}
		leaseClaim = claimResult.Claim()
		leaseActionClaimed = true
		defer func() {
			if leaseActionClaimed && !o.operations.ReleaseLease(leaseClaim) {
				slog.Error("failed to release deprovision lease action claim",
					"lease_uuid", leaseUUID)
			}
		}()
	}
	finishInFlight := func() error {
		if !wasInFlight {
			return nil
		}
		if !o.operations.FinishSettlement(settlementClaim) {
			return fmt.Errorf("%w: lease %s: lost exact operation settlement claim",
				ErrDeprovisionFailed, leaseUUID)
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
	for _, name := range o.rememberedDeprovisionCandidates(leaseUUID) {
		addCandidateName(name)
	}

	unresolved := false
	unaccountablePlacement := false
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
	if wasInFlight {
		addCandidateName(provision.Backend)
	}
	finishWithError := func(deprovisionErr error, retryCandidates []string) error {
		// Once this invocation has captured every positive candidate, the ordinary
		// provisioning tracker must not survive a poisoned close message: it feeds
		// load balancing and callback timeouts. Retain only candidates whose teardown
		// failed or could not be attempted; successful candidates need no retry and
		// a later deprovisioned callback can retire an outstanding entry. Then finish
		// only the exact claimed operation. Durable placement remains untouched.
		o.rememberDeprovisionCandidates(leaseUUID, retryCandidates)
		if finishErr := finishInFlight(); finishErr != nil {
			return errors.Join(deprovisionErr, finishErr)
		}
		return deprovisionErr
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
		failedCandidates := make([]string, 0)
		for _, b := range candidates {
			if err := b.Deprovision(ctx, leaseUUID); err != nil {
				slog.Error("failed to deprovision candidate backend",
					"lease_uuid", leaseUUID, "backend", b.Name(), "error", err)
				candidateErrs = append(candidateErrs, fmt.Errorf("backend %s: %w", b.Name(), err))
				failedCandidates = append(failedCandidates, b.Name())
				continue
			}
			o.forgetDeprovisionCandidate(leaseUUID, b.Name())
			slog.Info("deprovisioned successfully",
				"lease_uuid", leaseUUID, "backend", b.Name())
		}
		if len(candidateErrs) > 0 {
			return finishWithError(fmt.Errorf("%w: lease %s: %w",
				ErrDeprovisionFailed, leaseUUID, errors.Join(candidateErrs...)), failedCandidates)
		}
		// Placement is intentionally NOT deleted here (ENG-333). It is a derived
		// index of where the lease's data lives; if the backend retained the
		// volumes, the placement must survive close so a restore can route to it.
		// The reconciler is the sole pruner (cleanupOrphanedPlacements).
		o.forgetDeprovisionCandidates(leaseUUID)
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
			o.forgetDeprovisionCandidate(leaseUUID, b.Name())
		}
	}
	// Placement authority is mandatory, so falling back to a fleet sweep always
	// means ownership was unresolved. Reserve ERROR handling for the returned
	// aggregate while keeping this per-sweep diagnostic at WARN.
	logArgs := []any{
		"lease_uuid", leaseUUID,
		"swept_ok_or_noop", swept,
		"failed", failed,
		"positively_named_but_unconfigured", unreachedCandidates,
	}
	switch {
	case len(failed) > 0:
		slog.Warn("deprovision swept all backends with failures", logArgs...)
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
		return finishWithError(fmt.Errorf("%w: lease %s: %w",
			ErrDeprovisionFailed, leaseUUID, errors.Join(sweepErrs...)),
			append(append([]string(nil), failed...), unreachedCandidates...))
	}
	o.forgetDeprovisionCandidates(leaseUUID)
	return finishInFlight()
}
