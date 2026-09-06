package placement

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/util"
)

var ErrOperationSettlementGenerationUnavailable = errors.New(
	"exact durable placement generation is unavailable for operation settlement",
)

type operationCoordinatorMarker struct{ _ byte }

// OperationCoordinator is the one-to-one construction binding between an
// operation Registry's settlement lane and an exact placement Store. It is the
// only production authority that may finish a tracked callback or timeout: a
// Registry claim alone is not enough until the matching durable placement
// generation has been joined and disposed through a purpose-specific method.
// The zero value is invalid.
type OperationCoordinator struct {
	store      *Store
	operations operation.SettlementAuthority
	marker     *operationCoordinatorMarker

	// dispatchMu closes the only cross-authority race in the synchronous call
	// protocol. A callback may acquire its Registry claim while the backend call
	// is on the stack, but acquiring the matching Store generation and disposing
	// a joined dispatch are serialized. Consequently neither side can consume
	// one half while the other side is still deciding from an independent view.
	dispatchMu sync.Mutex

	bindMu          sync.Mutex
	terminalPruner  *TerminalPruner
	attemptRecovery *AttemptRecoveryCoordinator
	execution       *ExecutionCoordinator
	timeout         *TimeoutCoordinator
}

// BindOperationCoordinator constructs and consumes a private Registry's
// one-shot settlement facet, permanently binding it to this Store before
// either authority can be shared with workers. The caller receives only the
// joined coordinator; it can never retain the raw Registry or settlement
// authority and therefore cannot bypass placement settlement.
func (s *Store) BindOperationCoordinator(
	countObserver func(int),
) (*OperationCoordinator, error) {
	return newOperationCoordinator(
		s, operation.NewRegistryWithCountObserver(countObserver),
	)
}

// newOperationCoordinator accepts a Registry only for package-internal
// construction and white-box tests. Production construction always enters
// through Store.BindOperationCoordinator, which creates the Registry itself.
func newOperationCoordinator(
	store *Store,
	registry *operation.Registry,
) (*OperationCoordinator, error) {
	if store == nil {
		return nil, errors.New("placement store is required")
	}
	if registry == nil {
		return nil, errors.New("operation registry is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.operationCoordinator != nil {
		if store.boundOperationCoordinator != nil &&
			store.boundOperationCoordinator.operations.MatchesRegistry(registry) {
			return store.boundOperationCoordinator, nil
		}
		return nil, errors.New("placement store operation coordinator is already bound to another Registry")
	}
	authority, err := registry.BindSettlementAuthority()
	if err != nil {
		return nil, fmt.Errorf("bind operation registry settlement authority: %w", err)
	}
	marker := &operationCoordinatorMarker{}
	coordinator := &OperationCoordinator{
		store: store, operations: authority, marker: marker,
	}
	store.operationCoordinator = marker
	store.boundOperationCoordinator = coordinator
	return coordinator, nil
}

func (coordinator *OperationCoordinator) Valid() bool {
	return coordinator != nil && coordinator.store != nil && coordinator.marker != nil &&
		coordinator.store.operationCoordinator == coordinator.marker
}

func (coordinator *OperationCoordinator) RuntimeController() operation.RuntimeController {
	if !coordinator.Valid() {
		return operation.RuntimeController{}
	}
	return coordinator.operations.RuntimeController()
}

func (coordinator *OperationCoordinator) claimExactRecoveryAttempt(
	expected RecordRevision,
) (AttemptClaim, bool, error) {
	if !coordinator.Valid() || !expected.Valid() {
		return AttemptClaim{}, false, ErrInvalidAttemptToken
	}
	current := coordinator.store.Lookup(expected.leaseUUID)
	metadata := current.AttemptMetadata()
	if current.RecordRevision() != expected || current.Attempt == "" ||
		!metadata.Valid() || current.Conflict ||
		current.State() == StateUnusable {
		return AttemptClaim{}, false, nil
	}
	claim, claimed, err := coordinator.store.claimAttempt(
		expected.leaseUUID, metadata.OperationID(),
	)
	if err != nil || !claimed {
		return AttemptClaim{}, claimed, err
	}
	if claim.Backend() != current.Attempt || claim.Metadata() != metadata {
		coordinator.store.releaseAttemptClaim(claim)
		return AttemptClaim{}, false, nil
	}
	return claim, true, nil
}

type attemptRecoveryOutcome uint8

const (
	attemptRecoveryInvalid attemptRecoveryOutcome = iota
	attemptRecoveryPreserved
	attemptRecoveryAccepted
	attemptRecoveryRefused
)

// AttemptRecoveryResult is an observation of one construction-bound recovery
// operation. It exposes no claim or settlement method: callers can observe the
// coordinator's decision but cannot choose a durable outcome themselves.
type AttemptRecoveryResult struct {
	outcome attemptRecoveryOutcome
	callErr error
	err     error
}

func (result AttemptRecoveryResult) Accepted() bool {
	return result.outcome == attemptRecoveryAccepted && result.err == nil
}

func (result AttemptRecoveryResult) Refused() bool {
	return result.outcome == attemptRecoveryRefused && result.err == nil
}

func (result AttemptRecoveryResult) Preserved() bool {
	return result.outcome == attemptRecoveryPreserved
}

func (result AttemptRecoveryResult) CallErr() error { return result.callErr }
func (result AttemptRecoveryResult) Err() error     { return result.err }

type attemptRecoveryCoordinatorMarker struct{ _ byte }

// AttemptRecoveryCoordinator binds exact chain observation, backend execution,
// Registry claims, and placement settlement into one high-level operation.
type AttemptRecoveryCoordinator struct {
	coordinator  *OperationCoordinator
	controlPlane *boundProviderControlPlane
	backends     backendRuntime
	payloads     AttemptPayloadReader
	issuer       *attemptRecoveryCoordinatorMarker
}

func (recovery *AttemptRecoveryCoordinator) Valid() bool {
	return recovery != nil && recovery.coordinator != nil && recovery.coordinator.Valid() &&
		recovery.controlPlane != nil && recovery.controlPlane.validFor(recovery.coordinator.execution) &&
		!util.IsNilInterface(recovery.backends) &&
		recovery.issuer != nil && recovery.coordinator.attemptRecovery == recovery
}

// attemptTargetObservation is a sealed sum of the only three conclusions an
// exact chain read may authorize during recovery. Only a positive observation
// of a terminal state authorizes teardown. Absence and a nil lease with no
// error are both unknown because ledger history is not deleted; neither can
// authorize durable-evidence retirement.
type attemptTargetObservation interface {
	attemptTargetObservation()
}

type liveAttemptTarget struct{ lease billingtypes.Lease }
type terminalAttemptTarget struct{}
type unknownAttemptTarget struct{ err error }

func (liveAttemptTarget) attemptTargetObservation()     {}
func (terminalAttemptTarget) attemptTargetObservation() {}
func (unknownAttemptTarget) attemptTargetObservation()  {}

func observeAttemptTarget(
	lease *billingtypes.Lease,
	readErr error,
	leaseUUID string,
	request BackendRequestSnapshot,
) attemptTargetObservation {
	if readErr != nil {
		return unknownAttemptTarget{err: readErr}
	}
	if lease == nil || lease.Uuid != leaseUUID ||
		lease.Tenant != request.Tenant() || lease.ProviderUuid != request.ProviderUUID() {
		return unknownAttemptTarget{
			err: errors.New("attempt target lacks exact provider chain authority"),
		}
	}
	switch lease.State {
	case billingtypes.LEASE_STATE_PENDING, billingtypes.LEASE_STATE_ACTIVE:
		return liveAttemptTarget{lease: *lease}
	case billingtypes.LEASE_STATE_CLOSED, billingtypes.LEASE_STATE_REJECTED,
		billingtypes.LEASE_STATE_EXPIRED:
		return terminalAttemptTarget{}
	default:
		return unknownAttemptTarget{
			err: fmt.Errorf("attempt target state %s is not actionable", lease.State.String()),
		}
	}
}

// Recover derives live redelivery versus terminal teardown from the exact chain
// lease while holding the exact Registry and Store claims. The caller supplies
// only opaque record and lease capabilities and cannot choose a backend action
// or settlement outcome.
func (recovery *AttemptRecoveryCoordinator) Recover(
	ctx context.Context,
	expected RecordRevision,
	targetClaim operation.LeaseClaim,
	sourceClaim operation.LeaseClaim,
) AttemptRecoveryResult {
	if !recovery.Valid() || !expected.Valid() ||
		!recovery.coordinator.operations.HoldsLeaseClaim(targetClaim, expected.leaseUUID) {
		return AttemptRecoveryResult{outcome: attemptRecoveryInvalid, err: ErrInvalidAttemptToken}
	}
	coordinator := recovery.coordinator
	current := coordinator.store.Lookup(expected.leaseUUID)
	claim, claimed, err := coordinator.claimExactRecoveryAttempt(expected)
	if err != nil || !claimed {
		if err == nil {
			err = ErrOperationSettlementGenerationUnavailable
		}
		return AttemptRecoveryResult{outcome: attemptRecoveryPreserved, err: err}
	}
	defer coordinator.store.releaseAttemptClaim(claim)
	metadata := claim.Metadata()
	request := metadata.RequestSnapshot()
	if !request.Valid() {
		return AttemptRecoveryResult{outcome: attemptRecoveryInvalid, err: ErrInvalidAttemptToken}
	}
	sourceLeaseUUID := metadata.RestoreSourceLeaseUUID()
	if sourceLeaseUUID == "" {
		if sourceClaim.Valid() {
			return AttemptRecoveryResult{outcome: attemptRecoveryInvalid, err: ErrInvalidAttemptToken}
		}
	} else if sourceLeaseUUID == expected.leaseUUID ||
		!coordinator.operations.HoldsLeaseClaim(sourceClaim, sourceLeaseUUID) {
		return AttemptRecoveryResult{outcome: attemptRecoveryInvalid, err: ErrInvalidAttemptToken}
	}

	queryCtx, cancel := context.WithTimeout(ctx, pruneConfirmationTimeout)
	observation := recovery.controlPlane.observeLease(
		queryCtx, expected.leaseUUID, request.Tenant(),
	)
	cancel()
	var target attemptTargetObservation
	switch observed := observation.(type) {
	case observedExactLease:
		target = observeAttemptTarget(&observed.lease, nil, expected.leaseUUID, request)
	case observedLeaseUnauthorized:
		target = unknownAttemptTarget{
			err: errors.New("attempt target lacks exact provider chain authority"),
		}
	case observedLeaseUnknown:
		target = unknownAttemptTarget(observed)
	default:
		target = unknownAttemptTarget{err: ErrInvalidAttemptToken}
	}

	backendNames := []string{claim.Backend()}
	terminal := false
	var (
		callErr      error
		callAccepted bool
		callRefused  bool
	)
	switch target := target.(type) {
	case liveAttemptTarget:
		switch metadata.Kind() {
		case operation.KindProvision:
			outcome := recovery.redeliverProvision(
				ctx, expected.leaseUUID, claim.Backend(), metadata, len(target.lease.MetaHash) != 0,
			)
			callErr, callAccepted, callRefused = outcome.Err(), outcome.Accepted(), outcome.Refused()
		case operation.KindRestore:
			if target.lease.State != billingtypes.LEASE_STATE_PENDING {
				return AttemptRecoveryResult{
					outcome: attemptRecoveryPreserved,
					err:     errors.New("restore redelivery target is no longer pending"),
				}
			}
			if err := recovery.validateRestoreSource(
				ctx, &target.lease, claim.Backend(), metadata,
			); err != nil {
				return AttemptRecoveryResult{outcome: attemptRecoveryPreserved, err: err}
			}
			outcome := recovery.redeliverRestore(
				ctx, expected.leaseUUID, claim.Backend(), metadata,
			)
			callErr, callAccepted, callRefused = outcome.Err(), outcome.Accepted(), outcome.Refused()
		default:
			return AttemptRecoveryResult{outcome: attemptRecoveryInvalid, err: operation.ErrInvalidKind}
		}
	case terminalAttemptTarget:
		terminal = true
		if current.Backend != "" && current.Backend != claim.Backend() {
			backendNames = append(backendNames, current.Backend)
		}
		callErr = recovery.teardownTerminal(
			ctx, expected.leaseUUID, slices.Clone(backendNames),
		)
	case unknownAttemptTarget:
		return AttemptRecoveryResult{
			outcome: attemptRecoveryPreserved,
			err:     target.err,
		}
	default:
		return AttemptRecoveryResult{
			outcome: attemptRecoveryInvalid,
			err:     ErrInvalidAttemptToken,
		}
	}

	if !terminal && callRefused {
		settled, settleErr := coordinator.store.refuseClaimedAttempt(claim)
		if settleErr != nil || !settled {
			if settleErr == nil {
				settleErr = ErrOperationSettlementGenerationUnavailable
			}
			return AttemptRecoveryResult{
				outcome: attemptRecoveryPreserved, callErr: callErr, err: settleErr,
			}
		}
		return AttemptRecoveryResult{outcome: attemptRecoveryRefused, callErr: callErr}
	}
	if callErr != nil || (!terminal && !callAccepted) {
		message := "exact backend redelivery remains ambiguous"
		if terminal {
			message = "terminal exact teardown remains ambiguous"
		}
		return AttemptRecoveryResult{
			outcome: attemptRecoveryPreserved, callErr: callErr,
			err: fmt.Errorf("%s: %w", message, callErr),
		}
	}

	if !terminal {
		var (
			recovered  operation.RecoveredOperation
			recoverErr error
		)
		switch metadata.Kind() {
		case operation.KindProvision:
			recovered, recoverErr = operation.NewRecoveredProvision(
				expected.leaseUUID, request.Tenant(), request.Items(), claim.Backend(),
			)
		case operation.KindRestore:
			recovered, recoverErr = operation.NewRecoveredRestore(
				expected.leaseUUID, request.Tenant(), request.Items(), claim.Backend(),
			)
		default:
			recoverErr = operation.ErrInvalidKind
		}
		if recoverErr != nil {
			return AttemptRecoveryResult{
				outcome: attemptRecoveryInvalid,
				err:     fmt.Errorf("construct recovered operation: %w", recoverErr),
			}
		}
		if !coordinator.operations.RecoverClaimed(
			targetClaim, metadata.OperationID(), recovered,
		).Recovered() {
			return AttemptRecoveryResult{
				outcome: attemptRecoveryPreserved,
				err:     errors.New("recover accepted operation registry gate"),
			}
		}
	}
	confirmed, confirmErr := coordinator.store.confirmClaimedAttempt(claim)
	if confirmErr != nil || !confirmed {
		if confirmErr == nil {
			confirmErr = ErrOperationSettlementGenerationUnavailable
		}
		return AttemptRecoveryResult{outcome: attemptRecoveryPreserved, err: confirmErr}
	}
	return AttemptRecoveryResult{outcome: attemptRecoveryAccepted}
}

func (recovery *AttemptRecoveryCoordinator) validateRestoreSource(
	ctx context.Context,
	target *billingtypes.Lease,
	backendName string,
	metadata AttemptMetadata,
) error {
	sourceLeaseUUID := metadata.RestoreSourceLeaseUUID()
	sourcePlacement := recovery.coordinator.store.Lookup(sourceLeaseUUID)
	switch sourcePlacement.State() {
	case StateConfirmed:
		if sourcePlacement.Backend != backendName || sourcePlacement.Attempt != "" {
			return errors.New("restore source placement no longer matches attempted backend")
		}
	case StateAbsent:
		// Accepted restore may already have consumed the source; the exact durable
		// operation generation still authorizes an idempotent same-ID retry.
	default:
		return errors.New("restore source placement is ambiguous")
	}
	queryCtx, cancel := context.WithTimeout(ctx, pruneConfirmationTimeout)
	defer cancel()
	observation := recovery.controlPlane.observeLease(
		queryCtx, sourceLeaseUUID, target.Tenant,
	)
	source, exact := exactLeaseFromObservation(observation)
	if !exact {
		return fmt.Errorf("re-read restore recovery source: %w",
			exactLeaseObservationError(observation))
	}
	if source.State != billingtypes.LEASE_STATE_CLOSED {
		return errors.New("restore recovery source is not a closed retained lease")
	}
	return nil
}

// TerminalPruner is the one-time construction binding between the joined
// placement/Registry coordinator and the exact chain reader used to establish
// terminality. Its execution method accepts no per-call behavior or target.
type TerminalPruner struct {
	coordinator  *OperationCoordinator
	controlPlane *boundProviderControlPlane
	issuer       *operationCoordinatorMarker
}

// BindReconciliation atomically joins the exact chain reader and backend
// recovery executor to this Store/Registry pair. Neither a pruner without a
// recovery path nor a recovery path using a different reader can be observed.
func (coordinator *OperationCoordinator) bindReconciliation(
	controlPlane *boundProviderControlPlane,
	backends backendRuntime,
	payloads AttemptPayloadReader,
	projector *inventoryProjector,
) (*TerminalPruner, *AttemptRecoveryCoordinator, error) {
	if !coordinator.Valid() || controlPlane == nil ||
		!controlPlane.validFor(coordinator.execution) || util.IsNilInterface(backends) ||
		projector == nil || !projector.matchesStore(coordinator.store) {
		return nil, nil, errors.New(
			"valid operation coordinator, chain reader, recovery executor, and inventory projector are required",
		)
	}
	coordinator.bindMu.Lock()
	defer coordinator.bindMu.Unlock()
	if coordinator.terminalPruner != nil || coordinator.attemptRecovery != nil {
		return nil, nil, errors.New("reconciliation authority is already bound")
	}
	pruner := &TerminalPruner{
		coordinator: coordinator, controlPlane: controlPlane, issuer: coordinator.marker,
	}
	recovery := &AttemptRecoveryCoordinator{
		coordinator: coordinator, controlPlane: controlPlane,
		backends: backends, payloads: payloads,
		issuer: &attemptRecoveryCoordinatorMarker{},
	}
	coordinator.terminalPruner = pruner
	coordinator.attemptRecovery = recovery
	return pruner, recovery, nil
}

func (pruner *TerminalPruner) Valid() bool {
	return pruner != nil && pruner.coordinator != nil &&
		pruner.coordinator.Valid() && pruner.controlPlane != nil &&
		pruner.controlPlane.validFor(pruner.coordinator.execution) &&
		pruner.issuer == pruner.coordinator.marker
}

// PruneDisposition is the closed observational result of one high-level prune
// operation. It is output only: callers cannot submit it as authority.
type PruneDisposition uint8

const (
	PruneDispositionInvalid PruneDisposition = iota
	PruneDispositionChainError
	PruneDispositionChainUnknown
	PruneDispositionChainLive
	PruneDispositionChainUnknownState
	PruneDispositionEvidenceStale
	PruneDispositionDeleted
)

// PruneResult exposes why a record was preserved without exposing the proof's
// lease or revision identity. The zero value is an invalid, non-deleting result.
type PruneResult struct {
	disposition PruneDisposition
	err         error
	leaseState  billingtypes.LeaseState
}

func (result PruneResult) Disposition() PruneDisposition { return result.disposition }
func (result PruneResult) Deleted() bool {
	return result.disposition == PruneDispositionDeleted
}
func (result PruneResult) Err() error                          { return result.err }
func (result PruneResult) LeaseState() billingtypes.LeaseState { return result.leaseState }

const pruneConfirmationTimeout = 10 * time.Second

// PruneTerminalAbsence owns the entire destructive transition. The target is
// derived from projection-minted proof, terminality is classified from an exact
// bounded chain reread, and the final Store mutation requires the matching live
// Registry lease claim. No caller-selected target, revision, or outcome crosses
// this boundary.
func (pruner *TerminalPruner) PruneTerminalAbsence(
	ctx context.Context,
	proof PruneAbsenceProof,
	leaseClaim operation.LeaseClaim,
) PruneResult {
	if !pruner.Valid() {
		return PruneResult{disposition: PruneDispositionInvalid, err: ErrInvalidRecordRevision}
	}
	coordinator := pruner.coordinator
	if !proof.Valid() ||
		proof.store != coordinator.store || proof.coordinator != coordinator.marker {
		return PruneResult{disposition: PruneDispositionInvalid, err: ErrInvalidRecordRevision}
	}
	leaseUUID := proof.record.leaseUUID
	if !coordinator.operations.HoldsLeaseClaim(leaseClaim, leaseUUID) {
		return PruneResult{disposition: PruneDispositionInvalid, err: ErrInvalidRecordRevision}
	}

	queryCtx, cancel := context.WithTimeout(ctx, pruneConfirmationTimeout)
	defer cancel()
	observation := pruner.controlPlane.observeLease(queryCtx, leaseUUID, "")
	switch observed := observation.(type) {
	case observedLeaseUnauthorized:
		return PruneResult{disposition: PruneDispositionChainUnknown}
	case observedLeaseUnknown:
		return PruneResult{disposition: PruneDispositionChainError, err: observed.err}
	case observedExactLease:
		lease := observed.lease
		switch lease.State {
		case billingtypes.LEASE_STATE_PENDING, billingtypes.LEASE_STATE_ACTIVE:
			return PruneResult{
				disposition: PruneDispositionChainLive, leaseState: lease.State,
			}
		case billingtypes.LEASE_STATE_CLOSED, billingtypes.LEASE_STATE_REJECTED,
			billingtypes.LEASE_STATE_EXPIRED:
			deleted, deleteErr := coordinator.store.consumePruneAbsence(proof)
			if deleteErr != nil {
				return PruneResult{
					disposition: PruneDispositionEvidenceStale, err: deleteErr,
					leaseState: lease.State,
				}
			}
			if !deleted {
				return PruneResult{
					disposition: PruneDispositionEvidenceStale, leaseState: lease.State,
				}
			}
			return PruneResult{
				disposition: PruneDispositionDeleted, leaseState: lease.State,
			}
		default:
			return PruneResult{
				disposition: PruneDispositionChainUnknownState, leaseState: lease.State,
			}
		}
	default:
		return PruneResult{disposition: PruneDispositionInvalid, err: ErrInvalidRecordRevision}
	}
}

// Lookup returns detached operation metadata and never grants settlement
// authority.
func (coordinator *OperationCoordinator) Lookup(
	leaseUUID string,
) (operation.SettlementMetadata, bool) {
	if !coordinator.Valid() {
		return operation.SettlementMetadata{}, false
	}
	return coordinator.operations.Lookup(leaseUUID)
}

// TryClaimDeprovision derives exact close ownership from the live operation
// inside the construction-bound Registry facet. The caller supplies only a
// lease UUID, so an independently observed or replacement operation ID can
// never be spliced into close settlement.
func (coordinator *OperationCoordinator) tryClaimDeprovision(
	leaseUUID string,
) operation.DeprovisionClaimResult {
	if !coordinator.Valid() {
		return operation.DeprovisionClaimResult{}
	}
	return coordinator.operations.TryClaimDeprovision(leaseUUID)
}

func (coordinator *OperationCoordinator) releaseDeprovision(
	claim operation.DeprovisionClaim,
) bool {
	return coordinator.Valid() && coordinator.operations.ReleaseDeprovision(claim)
}

func (coordinator *OperationCoordinator) finishDeprovision(
	claim operation.DeprovisionClaim,
) bool {
	return coordinator.Valid() && coordinator.operations.FinishDeprovision(claim)
}

// Callback lifecycle and storage identity are deliberately derived through
// this exact Store. CallbackService cannot mix a coordinator from store A with
// observational authority from store B.
func (coordinator *OperationCoordinator) ExpectedBackendStorageIdentity(
	backendName string,
) (backendidentity.ID, bool) {
	if !coordinator.Valid() {
		return backendidentity.ID{}, false
	}
	return coordinator.store.ExpectedBackendStorageIdentity(backendName)
}

func (coordinator *OperationCoordinator) authorizeLifecycle(
	leaseUUID string,
	id lifecycle.ID,
) LifecycleAuthorization {
	if !coordinator.Valid() {
		return LifecycleAuthorization{}
	}
	return coordinator.store.authorizeLifecycle(leaseUUID, id)
}

func (coordinator *OperationCoordinator) retireLifecycle(
	leaseUUID string,
	id lifecycle.ID,
) (LifecycleAuthorization, error) {
	if !coordinator.Valid() {
		return LifecycleAuthorization{}, errors.New("operation coordinator is invalid")
	}
	return coordinator.store.retireLifecycle(leaseUUID, id)
}

// OperationGeneration is the closed durable state joined to a Registry claim.
// The zero value is invalid and always fails closed.
type OperationGeneration uint8

const (
	OperationGenerationInvalid OperationGeneration = iota
	OperationGenerationAttempt
	OperationGenerationConfirmed
)

func generationOf(claim AttemptClaim) OperationGeneration {
	if !claim.Valid() {
		return OperationGenerationInvalid
	}
	switch claim.kind {
	case attemptClaimUnresolved:
		return OperationGenerationAttempt
	case attemptClaimConfirmedGeneration:
		return OperationGenerationConfirmed
	default:
		return OperationGenerationInvalid
	}
}

// Generation returns the closed durable-state arm represented by this claim.
// It exposes no Store mutation authority and lets application-boundary test
// adapters preserve the same exhaustive settlement policy as the production
// coordinator.
func (claim AttemptClaim) Generation() OperationGeneration {
	return generationOf(claim)
}

func (coordinator *OperationCoordinator) claimMatchingAttempt(
	metadata operation.SettlementMetadata,
) (AttemptClaim, bool, error) {
	if !coordinator.Valid() || !metadata.Valid() {
		return AttemptClaim{}, false, ErrOperationSettlementGenerationUnavailable
	}
	claim, claimed, err := coordinator.store.claimAttempt(metadata.LeaseUUID(), metadata.ID())
	if err != nil || !claimed {
		return AttemptClaim{}, claimed, err
	}
	attempt := claim.Metadata()
	request := attempt.RequestSnapshot()
	if claim.Backend() != metadata.Backend() ||
		attempt.OperationID() != metadata.ID() ||
		attempt.Kind() != metadata.Kind() ||
		request.Tenant() != metadata.Tenant() ||
		!slices.Equal(request.Items(), metadata.Items()) {
		coordinator.store.releaseAttemptClaim(claim)
		return AttemptClaim{}, false, fmt.Errorf(
			"%w: Registry and Store generations diverge",
			ErrOperationSettlementGenerationUnavailable,
		)
	}
	return claim, true, nil
}

func (coordinator *OperationCoordinator) claimAttemptByID(
	leaseUUID string,
	id operation.OperationID,
) (AttemptClaim, bool, error) {
	if !coordinator.Valid() || leaseUUID == "" || !id.Valid() {
		return AttemptClaim{}, false, ErrOperationSettlementGenerationUnavailable
	}
	return coordinator.store.claimAttempt(leaseUUID, id)
}

// DispatchDisposition is the exhaustive result of disposing one joined
// synchronous dispatch. Its zero value is invalid and therefore never permits
// a caller to infer that placement or Registry state was settled.
type DispatchDisposition uint8

const (
	DispatchInvalid DispatchDisposition = iota
	DispatchApplied
	DispatchPreserved
	DispatchSuperseded
)

// DispatchResult reports the construction-safe consequence of a synchronous
// return. Preserved means the durable write-ahead generation remains the
// recovery authority; Superseded means an exact callback (or exact confirmed
// generation) supplied stronger evidence.
type DispatchResult struct {
	disposition      DispatchDisposition
	callOutcome      dispatchCallOutcome
	provisionRefusal backend.ProvisionRefusal
	restoreRefusal   backend.RestoreRefusal
	callErr          error
	err              error
}

func (result DispatchResult) Disposition() DispatchDisposition { return result.disposition }
func (result DispatchResult) Err() error                       { return result.err }
func (result DispatchResult) CallErr() error                   { return result.callErr }
func (result DispatchResult) Applied() bool {
	return result.disposition == DispatchApplied && result.err == nil
}
func (result DispatchResult) Superseded() bool {
	return result.disposition == DispatchSuperseded
}

// CallAccepted, CallDefinitivelyRefused, and CallAmbiguous expose only the
// coordinator's classification. They grant no settlement authority and keep
// response/logging policy from growing a second copy of the safety decision.
func (result DispatchResult) CallAccepted() bool {
	return result.callOutcome == dispatchCallAccepted
}

func (result DispatchResult) CallDefinitivelyRefused() bool {
	return result.callOutcome == dispatchCallRefused
}

func (result DispatchResult) CallNotDispatched() bool {
	return result.callOutcome == dispatchCallNotDispatched
}

func (result DispatchResult) CallAmbiguous() bool {
	return result.callOutcome == dispatchCallAmbiguous
}

func (result DispatchResult) ProvisionRefusal() backend.ProvisionRefusal {
	if !result.CallDefinitivelyRefused() {
		return backend.ProvisionRefusalNone
	}
	return result.provisionRefusal
}

func (result DispatchResult) RestoreRefusal() backend.RestoreRefusal {
	if !result.CallDefinitivelyRefused() {
		return backend.RestoreRefusalNone
	}
	return result.restoreRefusal
}

type dispatchCallOutcome uint8

const (
	dispatchCallInvalid dispatchCallOutcome = iota
	dispatchCallAccepted
	dispatchCallRefused
	dispatchCallNotDispatched
	dispatchCallAmbiguous
)

func classifyProvisionCall(outcome backend.ProvisionCallOutcome) dispatchCallOutcome {
	switch {
	case outcome.Accepted():
		return dispatchCallAccepted
	case outcome.Refused():
		return dispatchCallRefused
	case outcome.NotDispatched():
		return dispatchCallNotDispatched
	default:
		return dispatchCallAmbiguous
	}
}

func classifyRestoreCall(outcome backend.RestoreCallOutcome) dispatchCallOutcome {
	switch {
	case outcome.Accepted():
		return dispatchCallAccepted
	case outcome.Refused():
		return dispatchCallRefused
	case outcome.NotDispatched():
		return dispatchCallNotDispatched
	default:
		return dispatchCallAmbiguous
	}
}

func withCallOutcome(
	result DispatchResult,
	outcome dispatchCallOutcome,
	callErr error,
) DispatchResult {
	result.callOutcome = outcome
	result.callErr = callErr
	return result
}

// ProvisionDispatch is the indivisible Registry+Store authority for one
// provision call. Its components are private, so production callers cannot
// independently clear durable evidence and retire volatile state in the unsafe
// order. The zero value is invalid.
type ProvisionDispatch struct {
	issuer    *operationCoordinatorMarker
	operation operation.DispatchOperation
	attempt   AttemptToken
}

// provisionCall proves that the exact joined provision dispatch crossed its
// Registry call barrier. Its zero value is invalid.
type provisionCall struct {
	issuer   *operationCoordinatorMarker
	dispatch ProvisionDispatch
}

func (call provisionCall) Valid() bool {
	return call.issuer != nil && call.dispatch.Valid() && call.issuer == call.dispatch.issuer
}

func (dispatch ProvisionDispatch) Valid() bool {
	return dispatch.issuer != nil && dispatch.operation.Valid() && dispatch.attempt.Valid()
}

func (dispatch ProvisionDispatch) Metadata() operation.SettlementMetadata {
	if !dispatch.Valid() {
		return operation.SettlementMetadata{}
	}
	return dispatch.operation.Metadata()
}

// admitNewProvisionDispatch owns the complete transition from a preparing
// Registry operation to a joined new-placement dispatch. Once called, every
// non-success path retires the preparing operation; if durable admission
// succeeded, joinProvisionDispatch also disposes that exact Attempt.
func (coordinator *OperationCoordinator) admitNewProvisionDispatch(
	initiation operation.Initiation,
	scope AdmissionScope,
	leaseUUID, backendName string,
	payload PayloadFingerprint,
	request BackendRequestSnapshot,
	callbacks CallbackPair,
) (ProvisionDispatch, bool, error) {
	if !coordinator.Valid() || !initiation.Valid() || !initiation.ID().Valid() {
		return ProvisionDispatch{}, false, errors.New("valid provision admission authority is required")
	}
	attempt, applied, err := coordinator.store.beginNewAttempt(
		scope, leaseUUID, backendName, initiation.ID(), payload, request, callbacks,
	)
	return coordinator.finishProvisionAdmission(initiation, attempt, applied, err)
}

// admitOwnedProvisionDispatch is the exact-owner counterpart to
// admitNewProvisionDispatch. Keeping admission and join behind one method
// prevents a caller from leaking either half after the durable write succeeds.
func (coordinator *OperationCoordinator) admitOwnedProvisionDispatch(
	initiation operation.Initiation,
	baseline AdmissionBaseline,
	revision RecordRevision,
	backendName string,
	payload PayloadFingerprint,
	request BackendRequestSnapshot,
	callbacks CallbackPair,
) (ProvisionDispatch, bool, error) {
	if !coordinator.Valid() || !initiation.Valid() || !initiation.ID().Valid() {
		return ProvisionDispatch{}, false, errors.New("valid provision admission authority is required")
	}
	attempt, applied, err := coordinator.store.beginOwnedAttempt(
		baseline, revision, backendName, initiation.ID(), payload, request, callbacks,
	)
	return coordinator.finishProvisionAdmission(initiation, attempt, applied, err)
}

func (coordinator *OperationCoordinator) finishProvisionAdmission(
	initiation operation.Initiation,
	attempt AttemptToken,
	applied bool,
	admissionErr error,
) (ProvisionDispatch, bool, error) {
	if admissionErr != nil || !applied {
		completion := coordinator.operations.AbortInitiation(initiation)
		if completion != operation.InitiationAborted && completion != operation.InitiationFinished {
			admissionErr = errors.Join(admissionErr, fmt.Errorf(
				"abort provision initiation after admission refusal: outcome %d", completion,
			))
		}
		return ProvisionDispatch{}, false, admissionErr
	}
	dispatch, err := coordinator.joinProvisionDispatch(initiation, attempt)
	if err != nil {
		return ProvisionDispatch{}, false, err
	}
	return dispatch, true, nil
}

// joinProvisionDispatch proves that the exact preparing Registry operation
// and durable Attempt describe the same request before a backend call can
// become callback-visible. It takes ownership of a valid same-operation
// Attempt: every join failure either refuses that exact unresolved generation
// (or preserves an exact positive successor) and retires the Registry
// initiation, or reports why conservative durable evidence had to remain.
func (coordinator *OperationCoordinator) joinProvisionDispatch(
	initiation operation.Initiation,
	attempt AttemptToken,
) (ProvisionDispatch, error) {
	if !coordinator.Valid() || !attempt.Valid() || attempt.issuer != coordinator.store ||
		attempt.operationKind != operation.KindProvision || attempt.restoreSourceLeaseUUID != "" ||
		!initiation.Valid() || initiation.ID() != attempt.operationID {
		return ProvisionDispatch{}, errors.New("valid provision attempt from this coordinator is required")
	}
	dispatch, joined := coordinator.operations.JoinDispatch(initiation)
	if !joined {
		joinErr := errors.New("exact preparing operation cannot join provision dispatch")
		completion := coordinator.operations.AbortInitiation(initiation)
		if completion == operation.InitiationInvalid {
			// A foreign initiation must not consume a local durable capability.
			return ProvisionDispatch{}, joinErr
		}
		return ProvisionDispatch{}, errors.Join(
			joinErr, coordinator.disposeUnjoinedProvision(initiation, attempt, completion),
		)
	}
	metadata := dispatch.Metadata()
	request := attempt.requestSnapshot
	if metadata.Kind() != operation.KindProvision ||
		metadata.LeaseUUID() != attempt.leaseUUID ||
		metadata.Backend() != attempt.backendName ||
		metadata.ID() != attempt.operationID ||
		metadata.Tenant() != request.Tenant() ||
		!slices.Equal(metadata.Items(), request.Items()) ||
		!attempt.callbackPair.ValidFor(metadata.ID()) {
		joinErr := fmt.Errorf(
			"%w: Registry and provision Attempt describe different operations",
			ErrOperationSettlementGenerationUnavailable,
		)
		return ProvisionDispatch{}, errors.Join(
			joinErr, coordinator.disposeUnjoinedProvision(
				initiation, attempt, operation.InitiationInvalid,
			),
		)
	}
	coordinator.store.mu.RLock()
	_, current := coordinator.store.matchAttemptTokenLocked(attempt)
	coordinator.store.mu.RUnlock()
	if !current {
		return ProvisionDispatch{}, errors.Join(
			ErrOperationSettlementGenerationUnavailable,
			coordinator.disposeUnjoinedProvision(
				initiation, attempt, operation.InitiationInvalid,
			),
		)
	}
	return ProvisionDispatch{
		issuer: coordinator.marker, operation: dispatch, attempt: attempt,
	}, nil
}

func (coordinator *OperationCoordinator) disposeUnjoinedProvision(
	initiation operation.Initiation,
	attempt AttemptToken,
	registryCompletion operation.InitiationCompletion,
) error {
	var cleanupErr error
	claim, claimed, err := coordinator.store.claimAttempt(attempt.leaseUUID, attempt.operationID)
	if err != nil {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("claim unjoined provision attempt: %w", err))
	} else if claimed {
		exact := claim.Backend() == attempt.backendName &&
			claim.Metadata() == (AttemptMetadata{
				operationID:            attempt.operationID,
				operationKind:          attempt.operationKind,
				restoreSourceLeaseUUID: attempt.restoreSourceLeaseUUID,
				payloadFingerprint:     attempt.payloadFingerprint,
				requestSnapshot:        attempt.requestSnapshot,
				callbackPair:           attempt.callbackPair,
			})
		if !exact {
			if !coordinator.store.releaseAttemptClaim(claim) {
				cleanupErr = errors.Join(cleanupErr, errors.New("release mismatched unjoined provision claim"))
			}
			cleanupErr = errors.Join(cleanupErr, ErrOperationSettlementGenerationUnavailable)
		} else {
			applied, refuseErr := coordinator.store.refuseClaimedAttempt(claim)
			if refuseErr != nil || !applied {
				if refuseErr == nil {
					refuseErr = ErrOperationSettlementGenerationUnavailable
				}
				cleanupErr = errors.Join(cleanupErr,
					fmt.Errorf("dispose unjoined provision attempt: %w", refuseErr))
			}
		}
	}
	if registryCompletion == operation.InitiationInvalid {
		registryCompletion = coordinator.operations.AbortInitiation(initiation)
	}
	if registryCompletion != operation.InitiationAborted &&
		registryCompletion != operation.InitiationFinished {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf(
			"abort unjoined provision initiation: outcome %d", registryCompletion,
		))
	}
	return cleanupErr
}

func (coordinator *OperationCoordinator) validProvisionDispatch(
	dispatch ProvisionDispatch,
) bool {
	return coordinator.Valid() && dispatch.Valid() && dispatch.issuer == coordinator.marker &&
		dispatch.attempt.issuer == coordinator.store
}

// BeginProvisionCall is the only way a joined provision becomes callback
// visible. Callback claim admission shares dispatchMu with every terminal
// method below.
func (coordinator *OperationCoordinator) beginProvisionCall(
	dispatch ProvisionDispatch,
) (provisionCall, bool) {
	if !coordinator.validProvisionDispatch(dispatch) {
		return provisionCall{}, false
	}
	coordinator.dispatchMu.Lock()
	defer coordinator.dispatchMu.Unlock()
	if !coordinator.operations.BeginDispatchCall(dispatch.operation) {
		return provisionCall{}, false
	}
	return provisionCall{issuer: coordinator.marker, dispatch: dispatch}, true
}

func (coordinator *OperationCoordinator) callbackOwnsDispatch(
	dispatch operation.DispatchOperation,
) bool {
	metadata := dispatch.Metadata()
	current, exists := coordinator.operations.Lookup(metadata.LeaseUUID())
	return !exists || (current.ID() == metadata.ID() &&
		current.Settlement() == operation.SettlementTerminal)
}

func (coordinator *OperationCoordinator) completeCallbackOwnedDispatch(
	dispatch operation.DispatchOperation,
	accepted bool,
) DispatchResult {
	var completion operation.InitiationCompletion
	if accepted {
		completion = coordinator.operations.ActivateDispatch(dispatch)
	} else {
		completion = coordinator.operations.AbortDispatch(dispatch)
	}
	if completion == operation.InitiationSettling || completion == operation.InitiationFinished {
		return DispatchResult{disposition: DispatchSuperseded}
	}
	return DispatchResult{
		disposition: DispatchInvalid,
		err:         fmt.Errorf("callback-owned dispatch completed with Registry outcome %d", completion),
	}
}

// completeProvisionAccepted persists/verifies the positive placement first,
// then activates the Registry operation. A placement write failure leaves the
// Attempt durable and still releases the synchronous call barrier by keeping
// the operation active; exact callback/timeout recovery can therefore retry.
func (coordinator *OperationCoordinator) completeProvisionAccepted(
	dispatch ProvisionDispatch,
) DispatchResult {
	if !coordinator.validProvisionDispatch(dispatch) {
		return DispatchResult{disposition: DispatchInvalid, err: errors.New("invalid provision dispatch")}
	}
	coordinator.dispatchMu.Lock()
	defer coordinator.dispatchMu.Unlock()
	if coordinator.callbackOwnsDispatch(dispatch.operation) {
		return coordinator.completeCallbackOwnedDispatch(dispatch.operation, true)
	}
	claim, claimed, err := coordinator.claimMatchingAttempt(dispatch.Metadata())
	if err != nil || !claimed {
		// The accepted remote call must leave Calling even when durable storage is
		// temporarily unavailable. Keeping the exact Registry operation active and
		// the write-ahead Attempt untouched preserves both recovery routes.
		completion := coordinator.operations.ActivateDispatch(dispatch.operation)
		if err == nil {
			err = ErrOperationSettlementGenerationUnavailable
		}
		if completion != operation.InitiationActivated {
			return DispatchResult{disposition: DispatchInvalid, err: fmt.Errorf(
				"activate provision after placement claim failure: outcome %d: %w", completion, err,
			)}
		}
		return DispatchResult{disposition: DispatchPreserved, err: err}
	}
	applied, settleErr := coordinator.store.confirmClaimedAttempt(claim)
	if settleErr != nil || !applied {
		completion := coordinator.operations.ActivateDispatch(dispatch.operation)
		if settleErr == nil {
			settleErr = ErrOperationSettlementGenerationUnavailable
		}
		if completion != operation.InitiationActivated {
			return DispatchResult{disposition: DispatchInvalid, err: fmt.Errorf(
				"activate provision after placement confirmation failure: outcome %d: %w",
				completion, settleErr,
			)}
		}
		return DispatchResult{disposition: DispatchPreserved, err: settleErr}
	}
	completion := coordinator.operations.ActivateDispatch(dispatch.operation)
	if completion != operation.InitiationActivated {
		return DispatchResult{disposition: DispatchInvalid, err: fmt.Errorf(
			"activate confirmed provision: Registry outcome %d", completion,
		)}
	}
	return DispatchResult{disposition: DispatchApplied}
}

// completeProvisionRefused clears the exact unresolved Attempt before it
// retires the volatile initiation. This order is fixed inside the coordinator:
// a crash can leave an already-cleared Attempt with an ephemeral Registry row,
// but can never leave an Attempt eligible for redelivery after fred forgot a
// definitive no-call/refusal. An exact positive generation wins instead.
func (coordinator *OperationCoordinator) completeProvisionRefused(
	dispatch ProvisionDispatch,
) DispatchResult {
	if !coordinator.validProvisionDispatch(dispatch) {
		return DispatchResult{disposition: DispatchInvalid, err: errors.New("invalid provision dispatch")}
	}
	coordinator.dispatchMu.Lock()
	defer coordinator.dispatchMu.Unlock()
	if coordinator.callbackOwnsDispatch(dispatch.operation) {
		return coordinator.completeCallbackOwnedDispatch(dispatch.operation, false)
	}
	claim, claimed, err := coordinator.claimMatchingAttempt(dispatch.Metadata())
	if err != nil || !claimed {
		if err == nil {
			err = ErrOperationSettlementGenerationUnavailable
		}
		// Do not retire the Registry half without first disposing durable evidence.
		// Activate only releases the call barrier and makes later exact settlement
		// possible; it does not forget the operation.
		completion := coordinator.operations.ActivateDispatch(dispatch.operation)
		if completion != operation.InitiationActivated {
			return DispatchResult{disposition: DispatchInvalid, err: fmt.Errorf(
				"preserve provision after refusal claim failure: outcome %d: %w", completion, err,
			)}
		}
		return DispatchResult{disposition: DispatchPreserved, err: err}
	}
	if generationOf(claim) == OperationGenerationConfirmed {
		applied, confirmErr := coordinator.store.confirmClaimedAttempt(claim)
		if confirmErr != nil || !applied {
			if confirmErr == nil {
				confirmErr = ErrOperationSettlementGenerationUnavailable
			}
			completion := coordinator.operations.ActivateDispatch(dispatch.operation)
			if completion != operation.InitiationActivated {
				return DispatchResult{disposition: DispatchInvalid, err: confirmErr}
			}
			return DispatchResult{disposition: DispatchPreserved, err: confirmErr}
		}
		completion := coordinator.operations.ActivateDispatch(dispatch.operation)
		if completion != operation.InitiationActivated {
			return DispatchResult{disposition: DispatchInvalid, err: fmt.Errorf(
				"activate positively confirmed provision: Registry outcome %d", completion,
			)}
		}
		return DispatchResult{disposition: DispatchSuperseded}
	}
	applied, refuseErr := coordinator.store.refuseClaimedAttempt(claim)
	if refuseErr != nil || !applied {
		if refuseErr == nil {
			refuseErr = ErrOperationSettlementGenerationUnavailable
		}
		completion := coordinator.operations.ActivateDispatch(dispatch.operation)
		if completion != operation.InitiationActivated {
			return DispatchResult{disposition: DispatchInvalid, err: refuseErr}
		}
		return DispatchResult{disposition: DispatchPreserved, err: refuseErr}
	}
	completion := coordinator.operations.AbortDispatch(dispatch.operation)
	if completion != operation.InitiationAborted {
		return DispatchResult{disposition: DispatchInvalid, err: fmt.Errorf(
			"abort durably refused provision: Registry outcome %d", completion,
		)}
	}
	return DispatchResult{disposition: DispatchApplied}
}

// completeProvisionAmbiguous verifies and releases the exact durable
// generation unchanged, then retires only the volatile initiation. It can
// never call a refusal or confirmation mutation.
func (coordinator *OperationCoordinator) completeProvisionAmbiguous(
	dispatch ProvisionDispatch,
) DispatchResult {
	if !coordinator.validProvisionDispatch(dispatch) {
		return DispatchResult{disposition: DispatchInvalid, err: errors.New("invalid provision dispatch")}
	}
	coordinator.dispatchMu.Lock()
	defer coordinator.dispatchMu.Unlock()
	if coordinator.callbackOwnsDispatch(dispatch.operation) {
		return coordinator.completeCallbackOwnedDispatch(dispatch.operation, false)
	}
	claim, claimed, err := coordinator.claimMatchingAttempt(dispatch.Metadata())
	if err != nil || !claimed {
		if err == nil {
			err = ErrOperationSettlementGenerationUnavailable
		}
		completion := coordinator.operations.ActivateDispatch(dispatch.operation)
		if completion != operation.InitiationActivated {
			return DispatchResult{disposition: DispatchInvalid, err: err}
		}
		return DispatchResult{disposition: DispatchPreserved, err: err}
	}
	if !coordinator.store.releaseAttemptClaim(claim) {
		completion := coordinator.operations.ActivateDispatch(dispatch.operation)
		if completion != operation.InitiationActivated {
			return DispatchResult{disposition: DispatchInvalid, err: errors.New(
				"lost durable provision generation and Registry call barrier",
			)}
		}
		return DispatchResult{disposition: DispatchPreserved, err: errors.New(
			"lost exact durable provision generation claim",
		)}
	}
	completion := coordinator.operations.AbortDispatch(dispatch.operation)
	if completion != operation.InitiationAborted {
		return DispatchResult{disposition: DispatchInvalid, err: fmt.Errorf(
			"abort ambiguous provision: Registry outcome %d", completion,
		)}
	}
	return DispatchResult{disposition: DispatchPreserved}
}

// CompleteProvision is the sole synchronous provision settlement boundary.
// The caller supplies the transport result, never a placement outcome; this
// coordinator owns the exhaustive safety classification and consumes the joined
// dispatch through exactly one matching transition.
func (coordinator *OperationCoordinator) completeProvision(
	call provisionCall,
	callOutcome backend.ProvisionCallOutcome,
) DispatchResult {
	callErr := callOutcome.Err()
	if !call.Valid() || call.issuer != coordinator.marker ||
		!coordinator.validProvisionDispatch(call.dispatch) {
		return DispatchResult{disposition: DispatchInvalid, err: errors.New("invalid provision call")}
	}
	dispatch := call.dispatch
	outcome := classifyProvisionCall(callOutcome)
	var result DispatchResult
	switch outcome {
	case dispatchCallAccepted:
		result = coordinator.completeProvisionAccepted(dispatch)
	case dispatchCallRefused, dispatchCallNotDispatched:
		result = coordinator.completeProvisionRefused(dispatch)
	case dispatchCallAmbiguous:
		result = coordinator.completeProvisionAmbiguous(dispatch)
	default:
		result = DispatchResult{
			disposition: DispatchInvalid,
			err:         errors.New("invalid provision call outcome"),
		}
	}
	result = withCallOutcome(result, outcome, callErr)
	result.provisionRefusal = callOutcome.Refusal()
	return result
}

// AbortProvisionDispatch disposes a joined provision dispatch only while it
// is still provably pre-call. Completing an unsent call is therefore not
// representable as a synthetic transport outcome.
func (coordinator *OperationCoordinator) abortProvisionDispatch(
	dispatch ProvisionDispatch,
) DispatchResult {
	if !coordinator.validProvisionDispatch(dispatch) {
		return DispatchResult{disposition: DispatchInvalid, err: errors.New("invalid provision dispatch")}
	}
	coordinator.dispatchMu.Lock()
	defer coordinator.dispatchMu.Unlock()
	current, exists := coordinator.operations.Lookup(dispatch.Metadata().LeaseUUID())
	if !exists || current.ID() != dispatch.Metadata().ID() ||
		current.Phase() != operation.PhasePreparing {
		return DispatchResult{disposition: DispatchInvalid, err: errors.New(
			"provision dispatch is not awaiting its backend call",
		)}
	}
	claim, claimed, err := coordinator.claimMatchingAttempt(dispatch.Metadata())
	if err == nil && !claimed {
		err = ErrOperationSettlementGenerationUnavailable
	}
	disposition := DispatchPreserved
	if claimed {
		var applied bool
		if generationOf(claim) == OperationGenerationConfirmed {
			applied, err = coordinator.store.confirmClaimedAttempt(claim)
			disposition = DispatchSuperseded
		} else {
			applied, err = coordinator.store.refuseClaimedAttempt(claim)
			disposition = DispatchApplied
		}
		if err == nil && !applied {
			err = ErrOperationSettlementGenerationUnavailable
		}
	}
	completion := coordinator.operations.AbortDispatch(dispatch.operation)
	if completion != operation.InitiationAborted && completion != operation.InitiationFinished {
		return DispatchResult{disposition: DispatchInvalid, err: errors.Join(
			err, fmt.Errorf("abort unsent provision: Registry outcome %d", completion),
		)}
	}
	if err != nil {
		disposition = DispatchPreserved
	}
	return withCallOutcome(
		DispatchResult{disposition: disposition, err: err},
		dispatchCallNotDispatched,
		nil,
	)
}

// RestoreDispatch is the indivisible Registry+Store authority for one restore
// call. It retains the process-local source reservation until exactly one
// terminal method consumes it. The zero value is invalid.
type RestoreDispatch struct {
	issuer    *operationCoordinatorMarker
	operation operation.DispatchOperation
	restore   RestoreClaim
}

// restoreCall proves that the exact joined restore dispatch crossed its
// Registry call barrier. Its zero value is invalid.
type restoreCall struct {
	issuer   *operationCoordinatorMarker
	dispatch RestoreDispatch
}

// restoreBoundDispatch proves that the Store-selected restore backend was
// bound into the exact Registry initiation. beginRestoreCall accepts only this
// stage, making a call on an unbound restore dispatch unrepresentable.
type restoreBoundDispatch struct {
	issuer   *operationCoordinatorMarker
	dispatch RestoreDispatch
}

func (bound restoreBoundDispatch) Valid() bool {
	return bound.issuer != nil && bound.dispatch.Valid() && bound.issuer == bound.dispatch.issuer
}

func (call restoreCall) Valid() bool {
	return call.issuer != nil && call.dispatch.Valid() && call.issuer == call.dispatch.issuer
}

func (dispatch RestoreDispatch) Valid() bool {
	return dispatch.issuer != nil && dispatch.operation.Valid() && dispatch.restore.Valid()
}

func (dispatch RestoreDispatch) Metadata() operation.SettlementMetadata {
	if !dispatch.Valid() {
		return operation.SettlementMetadata{}
	}
	return dispatch.operation.Metadata()
}

// admitRestoreDispatch owns durable source/target admission and the Registry
// join as one construction step. A caller can receive either a complete
// RestoreDispatch or no mutation capability at all; every failure disposes the
// exact source reservation and retires the preparing Registry operation.
func (coordinator *OperationCoordinator) admitRestoreDispatch(
	initiation operation.Initiation,
	baseline AdmissionBaseline,
	sourceRevision RecordRevision,
	targetLeaseUUID string,
	request BackendRequestSnapshot,
	callbacks CallbackPair,
) (RestoreDispatch, string, error) {
	if !coordinator.Valid() || !initiation.Valid() || !initiation.ID().Valid() {
		return RestoreDispatch{}, "", errors.New("valid restore admission authority is required")
	}
	claim, err := coordinator.store.beginAuthorizedRestore(
		baseline, sourceRevision, targetLeaseUUID, initiation.ID(), request, callbacks,
	)
	if err != nil {
		completion := coordinator.operations.AbortInitiation(initiation)
		if completion != operation.InitiationAborted && completion != operation.InitiationFinished {
			err = errors.Join(err, fmt.Errorf(
				"abort restore initiation after admission refusal: outcome %d", completion,
			))
		}
		return RestoreDispatch{}, "", err
	}
	dispatch, err := coordinator.joinRestoreDispatch(initiation, claim)
	if err != nil {
		return RestoreDispatch{}, claim.backendName, err
	}
	return dispatch, claim.backendName, nil
}

// joinRestoreDispatch proves that the preparing Registry operation and the
// Store's atomic source-reservation/target-Attempt pair describe one restore.
// A valid same-operation claim is owned on entry: a failed join consumes its
// process-local source reservation, preserves any exact positive target
// successor, and retires the preparing Registry operation.
func (coordinator *OperationCoordinator) joinRestoreDispatch(
	initiation operation.Initiation,
	claim RestoreClaim,
) (RestoreDispatch, error) {
	if !coordinator.Valid() || !claim.Valid() || claim.issuer != coordinator.store ||
		!initiation.Valid() || initiation.ID() != claim.operationID {
		return RestoreDispatch{}, errors.New("valid restore claim from this coordinator is required")
	}
	dispatch, joined := coordinator.operations.JoinDispatch(initiation)
	if !joined {
		joinErr := errors.New("exact preparing operation cannot join restore dispatch")
		completion := coordinator.operations.AbortInitiation(initiation)
		if completion == operation.InitiationInvalid {
			// A foreign initiation must not consume a local durable capability.
			return RestoreDispatch{}, joinErr
		}
		return RestoreDispatch{}, errors.Join(
			joinErr, coordinator.disposeUnjoinedRestore(initiation, claim, completion),
		)
	}
	metadata := dispatch.Metadata()
	if metadata.Kind() != operation.KindRestore || metadata.Backend() != "" ||
		metadata.LeaseUUID() != claim.targetLeaseUUID ||
		metadata.ID() != claim.operationID ||
		metadata.Tenant() != claim.requestSnapshot.Tenant() ||
		!slices.Equal(metadata.Items(), claim.requestSnapshot.Items()) ||
		!claim.callbackPair.ValidFor(metadata.ID()) {
		joinErr := fmt.Errorf(
			"%w: Registry and restore claim describe different operations",
			ErrOperationSettlementGenerationUnavailable,
		)
		return RestoreDispatch{}, errors.Join(
			joinErr, coordinator.disposeUnjoinedRestore(
				initiation, claim, operation.InitiationInvalid,
			),
		)
	}
	coordinator.store.mu.RLock()
	live, reserved := coordinator.store.restoreClaims[claim.sourceLeaseUUID]
	target := AttemptToken{
		issuer:                 coordinator.store,
		leaseUUID:              claim.targetLeaseUUID,
		backendName:            claim.backendName,
		operationID:            claim.operationID,
		operationKind:          operation.KindRestore,
		restoreSourceLeaseUUID: claim.sourceLeaseUUID,
		requestSnapshot:        claim.requestSnapshot,
		callbackPair:           claim.callbackPair,
		revision:               claim.targetRevision,
	}
	_, targetCurrent := coordinator.store.matchAttemptTokenLocked(target)
	coordinator.store.mu.RUnlock()
	if !reserved || live != claim || !targetCurrent {
		return RestoreDispatch{}, errors.Join(
			ErrOperationSettlementGenerationUnavailable,
			coordinator.disposeUnjoinedRestore(
				initiation, claim, operation.InitiationInvalid,
			),
		)
	}
	return RestoreDispatch{
		issuer: coordinator.marker, operation: dispatch, restore: claim,
	}, nil
}

func (coordinator *OperationCoordinator) disposeUnjoinedRestore(
	initiation operation.Initiation,
	claim RestoreClaim,
	registryCompletion operation.InitiationCompletion,
) error {
	settled, err := coordinator.store.refuseRestore(claim)
	if err == nil && !settled {
		err = ErrOperationSettlementGenerationUnavailable
	}
	if registryCompletion == operation.InitiationInvalid {
		registryCompletion = coordinator.operations.AbortInitiation(initiation)
	}
	if registryCompletion != operation.InitiationAborted &&
		registryCompletion != operation.InitiationFinished {
		err = errors.Join(err, fmt.Errorf(
			"abort unjoined restore initiation: outcome %d", registryCompletion,
		))
	}
	return err
}

func (coordinator *OperationCoordinator) validRestoreDispatch(dispatch RestoreDispatch) bool {
	return coordinator.Valid() && dispatch.Valid() && dispatch.issuer == coordinator.marker &&
		dispatch.restore.issuer == coordinator.store
}

// BindRestoreBackend performs the one-shot Registry bind from the backend
// already selected by the opaque Store claim.
func (coordinator *OperationCoordinator) bindRestoreBackend(
	dispatch RestoreDispatch,
) (restoreBoundDispatch, bool) {
	if !coordinator.validRestoreDispatch(dispatch) {
		return restoreBoundDispatch{}, false
	}
	coordinator.dispatchMu.Lock()
	defer coordinator.dispatchMu.Unlock()
	if !coordinator.operations.BindDispatchBackend(dispatch.operation, dispatch.restore.backendName) {
		return restoreBoundDispatch{}, false
	}
	return restoreBoundDispatch{issuer: coordinator.marker, dispatch: dispatch}, true
}

func (coordinator *OperationCoordinator) beginRestoreCall(
	bound restoreBoundDispatch,
) (restoreCall, bool) {
	dispatch := bound.dispatch
	if !coordinator.validRestoreDispatch(dispatch) {
		return restoreCall{}, false
	}
	if !bound.Valid() || bound.issuer != coordinator.marker {
		return restoreCall{}, false
	}
	coordinator.dispatchMu.Lock()
	defer coordinator.dispatchMu.Unlock()
	if !coordinator.operations.BeginDispatchCall(dispatch.operation) {
		return restoreCall{}, false
	}
	return restoreCall{issuer: coordinator.marker, dispatch: dispatch}, true
}

func (coordinator *OperationCoordinator) completeCallbackOwnedRestore(
	dispatch RestoreDispatch,
	accepted bool,
) DispatchResult {
	result := coordinator.completeCallbackOwnedDispatch(dispatch.operation, accepted)
	if !result.Superseded() {
		return result
	}
	settled, err := coordinator.store.abandonRestore(dispatch.restore)
	if err != nil {
		return DispatchResult{disposition: DispatchSuperseded, err: err}
	}
	if !settled {
		return DispatchResult{disposition: DispatchSuperseded, err: errors.New(
			"inline callback won but restore source reservation was already consumed",
		)}
	}
	return result
}

// completeRestoreAccepted promotes the target Attempt before activating the
// Registry operation. On a write failure the target Attempt remains durable,
// the source reservation is consumed, and the active Registry operation keeps
// callback/timeout settlement available.
func (coordinator *OperationCoordinator) completeRestoreAccepted(
	dispatch RestoreDispatch,
) DispatchResult {
	if !coordinator.validRestoreDispatch(dispatch) {
		return DispatchResult{disposition: DispatchInvalid, err: errors.New("invalid restore dispatch")}
	}
	coordinator.dispatchMu.Lock()
	defer coordinator.dispatchMu.Unlock()
	if coordinator.callbackOwnsDispatch(dispatch.operation) {
		return coordinator.completeCallbackOwnedRestore(dispatch, true)
	}
	settled, err := coordinator.store.confirmRestore(dispatch.restore)
	if err != nil || !settled {
		completion := coordinator.operations.ActivateDispatch(dispatch.operation)
		if err == nil {
			err = ErrOperationSettlementGenerationUnavailable
		}
		if completion != operation.InitiationActivated {
			return DispatchResult{disposition: DispatchInvalid, err: fmt.Errorf(
				"activate restore after placement confirmation failure: outcome %d: %w",
				completion, err,
			)}
		}
		return DispatchResult{disposition: DispatchPreserved, err: err}
	}
	completion := coordinator.operations.ActivateDispatch(dispatch.operation)
	if completion != operation.InitiationActivated {
		return DispatchResult{disposition: DispatchInvalid, err: fmt.Errorf(
			"activate confirmed restore: Registry outcome %d", completion,
		)}
	}
	return DispatchResult{disposition: DispatchApplied}
}

func (coordinator *OperationCoordinator) restoreTargetPositivelyConfirmed(
	dispatch RestoreDispatch,
) bool {
	current := coordinator.store.Lookup(dispatch.restore.targetLeaseUUID)
	if current.State() != StateConfirmed || current.Backend != dispatch.restore.backendName ||
		current.Attempt != "" {
		return false
	}
	id, err := lifecycleIDForOperation(dispatch.restore.operationID)
	if err != nil {
		return false
	}
	authorization := coordinator.store.authorizeLifecycle(dispatch.restore.targetLeaseUUID, id)
	return authorization.Authorized() && authorization.Backend() == dispatch.restore.backendName
}

// completeRestoreRefused consumes the Store's source reservation and clears
// the target Attempt before retiring the volatile initiation. A crash after
// the durable commit therefore cannot resurrect a known-unsent restore.
func (coordinator *OperationCoordinator) completeRestoreRefused(
	dispatch RestoreDispatch,
) DispatchResult {
	if !coordinator.validRestoreDispatch(dispatch) {
		return DispatchResult{disposition: DispatchInvalid, err: errors.New("invalid restore dispatch")}
	}
	coordinator.dispatchMu.Lock()
	defer coordinator.dispatchMu.Unlock()
	if coordinator.callbackOwnsDispatch(dispatch.operation) {
		return coordinator.completeCallbackOwnedRestore(dispatch, false)
	}
	settled, err := coordinator.store.refuseRestore(dispatch.restore)
	if err != nil || !settled {
		if err == nil {
			err = ErrOperationSettlementGenerationUnavailable
		}
		completion := coordinator.operations.ActivateDispatch(dispatch.operation)
		if completion != operation.InitiationActivated {
			return DispatchResult{disposition: DispatchInvalid, err: fmt.Errorf(
				"preserve restore after placement refusal failure: outcome %d: %w", completion, err,
			)}
		}
		return DispatchResult{disposition: DispatchPreserved, err: err}
	}
	if coordinator.restoreTargetPositivelyConfirmed(dispatch) {
		completion := coordinator.operations.ActivateDispatch(dispatch.operation)
		if completion != operation.InitiationActivated {
			return DispatchResult{disposition: DispatchInvalid, err: fmt.Errorf(
				"activate positively confirmed restore: Registry outcome %d", completion,
			)}
		}
		return DispatchResult{disposition: DispatchSuperseded}
	}
	completion := coordinator.operations.AbortDispatch(dispatch.operation)
	if completion != operation.InitiationAborted {
		return DispatchResult{disposition: DispatchInvalid, err: fmt.Errorf(
			"abort durably refused restore: Registry outcome %d", completion,
		)}
	}
	return DispatchResult{disposition: DispatchApplied}
}

// completeRestoreAmbiguous consumes only the process-local source reservation;
// it leaves the target Attempt byte-for-byte unchanged before retiring the
// volatile initiation.
func (coordinator *OperationCoordinator) completeRestoreAmbiguous(
	dispatch RestoreDispatch,
) DispatchResult {
	if !coordinator.validRestoreDispatch(dispatch) {
		return DispatchResult{disposition: DispatchInvalid, err: errors.New("invalid restore dispatch")}
	}
	coordinator.dispatchMu.Lock()
	defer coordinator.dispatchMu.Unlock()
	if coordinator.callbackOwnsDispatch(dispatch.operation) {
		return coordinator.completeCallbackOwnedRestore(dispatch, false)
	}
	settled, err := coordinator.store.abandonRestore(dispatch.restore)
	if err != nil || !settled {
		if err == nil {
			err = ErrOperationSettlementGenerationUnavailable
		}
		completion := coordinator.operations.ActivateDispatch(dispatch.operation)
		if completion != operation.InitiationActivated {
			return DispatchResult{disposition: DispatchInvalid, err: err}
		}
		return DispatchResult{disposition: DispatchPreserved, err: err}
	}
	completion := coordinator.operations.AbortDispatch(dispatch.operation)
	if completion != operation.InitiationAborted {
		return DispatchResult{disposition: DispatchInvalid, err: fmt.Errorf(
			"abort ambiguous restore: Registry outcome %d", completion,
		)}
	}
	return DispatchResult{disposition: DispatchPreserved}
}

// CompleteRestore is the sole synchronous restore settlement boundary. Restore
// has a different definitive-refusal vocabulary from provision; keeping both
// classifiers beside the joined mutations makes that distinction exhaustive
// and prevents application callers from selecting a terminal branch directly.
func (coordinator *OperationCoordinator) completeRestore(
	call restoreCall,
	callOutcome backend.RestoreCallOutcome,
) DispatchResult {
	callErr := callOutcome.Err()
	if !call.Valid() || call.issuer != coordinator.marker ||
		!coordinator.validRestoreDispatch(call.dispatch) {
		return DispatchResult{disposition: DispatchInvalid, err: errors.New("invalid restore call")}
	}
	dispatch := call.dispatch
	outcome := classifyRestoreCall(callOutcome)
	var result DispatchResult
	switch outcome {
	case dispatchCallAccepted:
		result = coordinator.completeRestoreAccepted(dispatch)
	case dispatchCallRefused, dispatchCallNotDispatched:
		result = coordinator.completeRestoreRefused(dispatch)
	case dispatchCallAmbiguous:
		result = coordinator.completeRestoreAmbiguous(dispatch)
	default:
		result = DispatchResult{
			disposition: DispatchInvalid,
			err:         errors.New("invalid restore call outcome"),
		}
	}
	result = withCallOutcome(result, outcome, callErr)
	result.restoreRefusal = callOutcome.Refusal()
	return result
}

// AbortRestoreDispatch disposes a joined restore dispatch only while it is
// still provably pre-call.
func (coordinator *OperationCoordinator) abortRestoreDispatch(
	dispatch RestoreDispatch,
) DispatchResult {
	if !coordinator.validRestoreDispatch(dispatch) {
		return DispatchResult{disposition: DispatchInvalid, err: errors.New("invalid restore dispatch")}
	}
	coordinator.dispatchMu.Lock()
	defer coordinator.dispatchMu.Unlock()
	current, exists := coordinator.operations.Lookup(dispatch.Metadata().LeaseUUID())
	if !exists || current.ID() != dispatch.Metadata().ID() ||
		current.Phase() != operation.PhasePreparing {
		return DispatchResult{disposition: DispatchInvalid, err: errors.New(
			"restore dispatch is not awaiting its backend call",
		)}
	}
	settled, err := coordinator.store.refuseRestore(dispatch.restore)
	if err == nil && !settled {
		err = ErrOperationSettlementGenerationUnavailable
	}
	disposition := DispatchApplied
	if err != nil {
		disposition = DispatchPreserved
	} else if coordinator.restoreTargetPositivelyConfirmed(dispatch) {
		disposition = DispatchSuperseded
	}
	completion := coordinator.operations.AbortDispatch(dispatch.operation)
	if completion != operation.InitiationAborted && completion != operation.InitiationFinished {
		return DispatchResult{disposition: DispatchInvalid, err: errors.Join(
			err, fmt.Errorf("abort unsent restore: Registry outcome %d", completion),
		)}
	}
	return withCallOutcome(
		DispatchResult{disposition: disposition, err: err},
		dispatchCallNotDispatched,
		nil,
	)
}

// CallbackSettlementClaim joins one exact Registry callback claim to the
// matching Store-issued durable generation sum (unresolved Attempt or exact
// confirmed owner). Neither half is exposed.
type CallbackSettlementClaim struct {
	issuer    *operationCoordinatorMarker
	registry  operation.CallbackClaim
	placement AttemptClaim
}

func (claim CallbackSettlementClaim) Valid() bool {
	return claim.issuer != nil && claim.registry.Valid() && claim.placement.Valid()
}
func (claim CallbackSettlementClaim) Metadata() operation.SettlementMetadata {
	if !claim.Valid() {
		return operation.SettlementMetadata{}
	}
	return claim.registry.Metadata()
}
func (claim CallbackSettlementClaim) Generation() OperationGeneration {
	if !claim.Valid() {
		return OperationGenerationInvalid
	}
	return generationOf(claim.placement)
}

type CallbackSettlementAdmission struct {
	claim   CallbackSettlementClaim
	outcome operation.SettlementOutcome
	err     error
}

func (admission CallbackSettlementAdmission) Outcome() operation.SettlementOutcome {
	return admission.outcome
}
func (admission CallbackSettlementAdmission) Err() error { return admission.err }
func (admission CallbackSettlementAdmission) Claimed() bool {
	return admission.outcome == operation.SettlementClaimed && admission.err == nil &&
		admission.claim.Valid()
}
func (admission CallbackSettlementAdmission) Claim() CallbackSettlementClaim {
	if !admission.Claimed() {
		return CallbackSettlementClaim{}
	}
	return admission.claim
}

// TryClaimCallback independently derives then joins the exact Registry and
// Store generations. Store contention is reported as busy so a durable backend
// outbox retries instead of losing evidence.
func (coordinator *OperationCoordinator) tryClaimCallback(
	leaseUUID string,
	id operation.OperationID,
) CallbackSettlementAdmission {
	if !coordinator.Valid() {
		return CallbackSettlementAdmission{outcome: operation.SettlementInvalid}
	}
	coordinator.dispatchMu.Lock()
	defer coordinator.dispatchMu.Unlock()
	registryResult := coordinator.operations.TryClaimCallback(leaseUUID, id)
	if !registryResult.Claimed() {
		return CallbackSettlementAdmission{outcome: registryResult.Outcome()}
	}
	registryClaim := registryResult.Claim()
	placementClaim, claimed, err := coordinator.claimMatchingAttempt(registryClaim.Metadata())
	if err != nil || !claimed {
		coordinator.operations.ReleaseCallback(registryClaim)
		if errors.Is(err, ErrAttemptClaimed) {
			return CallbackSettlementAdmission{outcome: operation.SettlementBusy, err: err}
		}
		if err == nil {
			err = ErrOperationSettlementGenerationUnavailable
		}
		return CallbackSettlementAdmission{outcome: operation.SettlementInvalid, err: err}
	}
	return CallbackSettlementAdmission{
		claim: CallbackSettlementClaim{
			issuer: coordinator.marker, registry: registryClaim, placement: placementClaim,
		},
		outcome: operation.SettlementClaimed,
	}
}

func (coordinator *OperationCoordinator) validCallbackClaim(
	claim CallbackSettlementClaim,
) bool {
	return coordinator.Valid() && claim.Valid() && claim.issuer == coordinator.marker &&
		claim.registry.Metadata().LeaseUUID() == claim.placement.leaseUUID &&
		claim.registry.Metadata().ID() == claim.placement.operationID
}

// ReleaseCallback releases both unconsumed claims. It is the only valid action
// after retryable chain work fails.
func (coordinator *OperationCoordinator) releaseCallback(claim CallbackSettlementClaim) bool {
	if !coordinator.validCallbackClaim(claim) {
		return false
	}
	placementReleased := coordinator.store.releaseAttemptClaim(claim.placement)
	registryReleased := coordinator.operations.ReleaseCallback(claim.registry)
	return placementReleased && registryReleased
}

// FinishConfirmedCallback first persists/verifies the exact confirmed owner,
// then retires the Registry record. If the second step fails, ClaimAttempt can
// reissue the confirmed-generation arm on retry; no volatile-only evidence is
// required.
func (coordinator *OperationCoordinator) finishConfirmedCallback(
	claim CallbackSettlementClaim,
) error {
	if !coordinator.validCallbackClaim(claim) {
		return errors.New("callback settlement claim belongs to another coordinator")
	}
	applied, err := coordinator.store.confirmClaimedAttempt(claim.placement)
	if err != nil {
		coordinator.operations.ReleaseCallback(claim.registry)
		return err
	}
	if !applied {
		coordinator.operations.ReleaseCallback(claim.registry)
		return ErrOperationSettlementGenerationUnavailable
	}
	if !coordinator.operations.FinishCallback(claim.registry) {
		coordinator.operations.ReleaseCallback(claim.registry)
		return errors.New("lost exact Registry callback claim after placement confirmation")
	}
	return nil
}

// FinishRefusedCallback consumes an authenticated backend failure only after
// its caller has established the required chain outcome (none for an already
// ACTIVE lease, or a positively terminal/rejected PENDING lease). It retires
// the volatile Registry first and only then clears the exact Attempt. A crash
// in that window leaves the durable Attempt for callback recovery; the unsafe
// inverse ordering is not representable through this coordinator.
func (coordinator *OperationCoordinator) finishRefusedCallback(
	claim CallbackSettlementClaim,
) error {
	if !coordinator.validCallbackClaim(claim) ||
		generationOf(claim.placement) != OperationGenerationAttempt {
		return errors.New("exact unresolved callback attempt is required")
	}
	matched, err := coordinator.store.attestClaimedAttempt(claim.placement, attemptClaimUnresolved)
	if err != nil || !matched {
		coordinator.releaseCallback(claim)
		if err != nil {
			return err
		}
		return ErrOperationSettlementGenerationUnavailable
	}
	if !coordinator.operations.FinishCallback(claim.registry) {
		coordinator.releaseCallback(claim)
		return errors.New("lost exact Registry callback claim before placement refusal")
	}
	applied, err := coordinator.store.refuseClaimedAttempt(claim.placement)
	if err != nil {
		return err
	}
	if !applied {
		coordinator.store.releaseAttemptClaim(claim.placement)
		return ErrOperationSettlementGenerationUnavailable
	}
	return nil
}

// FinishCallbackRetainingConfirmed verifies the exact confirmed-generation arm
// without demoting its owner, then retires the Registry operation.
func (coordinator *OperationCoordinator) finishCallbackRetainingConfirmed(
	claim CallbackSettlementClaim,
) error {
	if !coordinator.validCallbackClaim(claim) ||
		generationOf(claim.placement) != OperationGenerationConfirmed {
		return errors.New("exact confirmed callback generation is required")
	}
	return coordinator.finishConfirmedCallback(claim)
}

// TimeoutCandidate is an issuer-bound snapshot. A copied candidate still must
// win both exact live claims, and a candidate from another coordinator is inert.
type TimeoutCandidate struct {
	issuer    *operationCoordinatorMarker
	operation operation.TimeoutCandidate
}

func (candidate TimeoutCandidate) Valid() bool {
	return candidate.issuer != nil && candidate.operation.Valid()
}
func (candidate TimeoutCandidate) Metadata() operation.SettlementMetadata {
	if !candidate.Valid() {
		return operation.SettlementMetadata{}
	}
	return candidate.operation.Metadata()
}

func (coordinator *OperationCoordinator) timedOut(timeout time.Duration) []TimeoutCandidate {
	if !coordinator.Valid() {
		return nil
	}
	operations := coordinator.operations.TimedOut(timeout)
	candidates := make([]TimeoutCandidate, 0, len(operations))
	for _, candidate := range operations {
		candidates = append(candidates, TimeoutCandidate{
			issuer: coordinator.marker, operation: candidate,
		})
	}
	return candidates
}

type TimeoutSettlementClaim struct {
	issuer    *operationCoordinatorMarker
	registry  operation.TimeoutClaim
	placement AttemptClaim
}

func (claim TimeoutSettlementClaim) Valid() bool {
	return claim.issuer != nil && claim.registry.Valid() && claim.placement.Valid()
}
func (claim TimeoutSettlementClaim) Metadata() operation.SettlementMetadata {
	if !claim.Valid() {
		return operation.SettlementMetadata{}
	}
	return claim.registry.Metadata()
}
func (claim TimeoutSettlementClaim) Generation() OperationGeneration {
	if !claim.Valid() {
		return OperationGenerationInvalid
	}
	return generationOf(claim.placement)
}

type TimeoutSettlementAdmission struct {
	claim   TimeoutSettlementClaim
	outcome operation.SettlementOutcome
	err     error
}

func (admission TimeoutSettlementAdmission) Outcome() operation.SettlementOutcome {
	return admission.outcome
}
func (admission TimeoutSettlementAdmission) Err() error { return admission.err }
func (admission TimeoutSettlementAdmission) Claimed() bool {
	return admission.outcome == operation.SettlementClaimed && admission.err == nil &&
		admission.claim.Valid()
}
func (admission TimeoutSettlementAdmission) Claim() TimeoutSettlementClaim {
	if !admission.Claimed() {
		return TimeoutSettlementClaim{}
	}
	return admission.claim
}

func (coordinator *OperationCoordinator) tryClaimTimeout(
	candidate TimeoutCandidate,
) TimeoutSettlementAdmission {
	if !coordinator.Valid() || !candidate.Valid() || candidate.issuer != coordinator.marker {
		return TimeoutSettlementAdmission{outcome: operation.SettlementInvalid}
	}
	registryResult := coordinator.operations.TryClaimTimeout(candidate.operation)
	if !registryResult.Claimed() {
		return TimeoutSettlementAdmission{outcome: registryResult.Outcome()}
	}
	registryClaim := registryResult.Claim()
	placementClaim, claimed, err := coordinator.claimMatchingAttempt(registryClaim.Metadata())
	if err != nil || !claimed {
		coordinator.operations.ReleaseTimeout(registryClaim)
		if errors.Is(err, ErrAttemptClaimed) {
			return TimeoutSettlementAdmission{outcome: operation.SettlementBusy, err: err}
		}
		if err == nil {
			err = ErrOperationSettlementGenerationUnavailable
		}
		return TimeoutSettlementAdmission{outcome: operation.SettlementInvalid, err: err}
	}
	return TimeoutSettlementAdmission{
		claim: TimeoutSettlementClaim{
			issuer: coordinator.marker, registry: registryClaim, placement: placementClaim,
		},
		outcome: operation.SettlementClaimed,
	}
}

func (coordinator *OperationCoordinator) validTimeoutClaim(
	claim TimeoutSettlementClaim,
) bool {
	return coordinator.Valid() && claim.Valid() && claim.issuer == coordinator.marker &&
		claim.registry.Metadata().LeaseUUID() == claim.placement.leaseUUID &&
		claim.registry.Metadata().ID() == claim.placement.operationID
}

func (coordinator *OperationCoordinator) releaseTimeout(claim TimeoutSettlementClaim) {
	if !coordinator.validTimeoutClaim(claim) {
		return
	}
	coordinator.store.releaseAttemptClaim(claim.placement)
	coordinator.operations.ReleaseTimeout(claim.registry)
}

// FinishTimeoutPreservingForRedelivery can neither clear nor promote an
// unresolved Attempt. Accepted-without-callback is physically ambiguous even
// after an on-chain reject, so the exact durable candidate survives for
// reconciler redelivery or terminal teardown.
func (coordinator *OperationCoordinator) finishTimeoutPreservingForRedelivery(
	claim TimeoutSettlementClaim,
) error {
	if !coordinator.validTimeoutClaim(claim) ||
		generationOf(claim.placement) != OperationGenerationAttempt {
		return errors.New("exact unresolved timeout attempt is required")
	}
	matched, err := coordinator.store.attestClaimedAttempt(claim.placement, attemptClaimUnresolved)
	if err != nil || !matched {
		coordinator.releaseTimeout(claim)
		if err != nil {
			return err
		}
		return ErrOperationSettlementGenerationUnavailable
	}
	if !coordinator.operations.FinishTimeout(claim.registry) {
		coordinator.releaseTimeout(claim)
		return errors.New("lost exact Registry timeout claim before durable preservation")
	}
	if !coordinator.store.releaseAttemptClaim(claim.placement) {
		return errors.New("lost exact placement claim after Registry timeout finish")
	}
	return nil
}

// FinishTimeoutRetainingConfirmed handles the idempotent case where inventory
// already promoted the same operation generation. It preserves that positive
// owner and removes only the timed-out Registry record.
func (coordinator *OperationCoordinator) finishTimeoutRetainingConfirmed(
	claim TimeoutSettlementClaim,
) error {
	if !coordinator.validTimeoutClaim(claim) ||
		generationOf(claim.placement) != OperationGenerationConfirmed {
		return errors.New("exact confirmed timeout generation is required")
	}
	applied, err := coordinator.store.confirmClaimedAttempt(claim.placement)
	if err != nil {
		coordinator.operations.ReleaseTimeout(claim.registry)
		return err
	}
	if !applied {
		coordinator.operations.ReleaseTimeout(claim.registry)
		return ErrOperationSettlementGenerationUnavailable
	}
	if !coordinator.operations.FinishTimeout(claim.registry) {
		coordinator.operations.ReleaseTimeout(claim.registry)
		return errors.New("lost exact Registry timeout claim after owner verification")
	}
	return nil
}

// RecoveryCallbackClaim joins the callback-only Registry lease fence to an
// exact durable placement generation after the volatile operation disappeared.
type RecoveryCallbackClaim struct {
	issuer    *operationCoordinatorMarker
	registry  operation.RecoveryLeaseClaim
	placement AttemptClaim
}

func (claim RecoveryCallbackClaim) Valid() bool {
	return claim.issuer != nil && claim.registry.Valid() && claim.placement.Valid()
}
func (claim RecoveryCallbackClaim) LeaseUUID() string {
	if !claim.Valid() {
		return ""
	}
	return claim.placement.leaseUUID
}
func (claim RecoveryCallbackClaim) Backend() string {
	if !claim.Valid() {
		return ""
	}
	return claim.placement.Backend()
}
func (claim RecoveryCallbackClaim) Metadata() AttemptMetadata {
	if !claim.Valid() {
		return AttemptMetadata{}
	}
	return claim.placement.Metadata()
}
func (claim RecoveryCallbackClaim) Generation() OperationGeneration {
	if !claim.Valid() {
		return OperationGenerationInvalid
	}
	return generationOf(claim.placement)
}
func (claim RecoveryCallbackClaim) HasSameBackendOwner() bool {
	return claim.Valid() && claim.placement.HasSameBackendOwner()
}

type RecoveryCallbackOutcome uint8

const (
	RecoveryCallbackInvalid RecoveryCallbackOutcome = iota
	RecoveryCallbackClaimed
	RecoveryCallbackBusy
	RecoveryCallbackNotFound
)

type RecoveryCallbackAdmission struct {
	claim   RecoveryCallbackClaim
	outcome RecoveryCallbackOutcome
	err     error
}

func (admission RecoveryCallbackAdmission) Outcome() RecoveryCallbackOutcome {
	return admission.outcome
}
func (admission RecoveryCallbackAdmission) Err() error { return admission.err }
func (admission RecoveryCallbackAdmission) Claimed() bool {
	return admission.outcome == RecoveryCallbackClaimed && admission.err == nil &&
		admission.claim.Valid()
}
func (admission RecoveryCallbackAdmission) Claim() RecoveryCallbackClaim {
	if !admission.Claimed() {
		return RecoveryCallbackClaim{}
	}
	return admission.claim
}

func (coordinator *OperationCoordinator) tryClaimRecoveryCallback(
	leaseUUID string,
	id operation.OperationID,
) RecoveryCallbackAdmission {
	if !coordinator.Valid() || leaseUUID == "" || !id.Valid() {
		return RecoveryCallbackAdmission{outcome: RecoveryCallbackInvalid}
	}
	registryResult := coordinator.operations.TryClaimCallbackRecoveryLease(leaseUUID)
	if !registryResult.Acquired() {
		outcome := RecoveryCallbackInvalid
		if registryResult.Outcome() == operation.LeaseClaimBusy {
			outcome = RecoveryCallbackBusy
		}
		return RecoveryCallbackAdmission{outcome: outcome}
	}
	registryClaim := registryResult.Claim()
	placementClaim, claimed, err := coordinator.claimAttemptByID(leaseUUID, id)
	if err != nil || !claimed {
		coordinator.operations.ReleaseRecoveryLease(registryClaim)
		if errors.Is(err, ErrAttemptClaimed) {
			return RecoveryCallbackAdmission{outcome: RecoveryCallbackBusy, err: err}
		}
		if err != nil {
			return RecoveryCallbackAdmission{outcome: RecoveryCallbackInvalid, err: err}
		}
		return RecoveryCallbackAdmission{outcome: RecoveryCallbackNotFound}
	}
	return RecoveryCallbackAdmission{
		claim: RecoveryCallbackClaim{
			issuer: coordinator.marker, registry: registryClaim, placement: placementClaim,
		},
		outcome: RecoveryCallbackClaimed,
	}
}

func (coordinator *OperationCoordinator) validRecoveryClaim(
	claim RecoveryCallbackClaim,
) bool {
	return coordinator.Valid() && claim.Valid() && claim.issuer == coordinator.marker
}

func (coordinator *OperationCoordinator) releaseRecoveryCallback(
	claim RecoveryCallbackClaim,
) bool {
	if !coordinator.validRecoveryClaim(claim) {
		return false
	}
	placementReleased := coordinator.store.releaseAttemptClaim(claim.placement)
	registryReleased := coordinator.operations.ReleaseRecoveryLease(claim.registry)
	return placementReleased && registryReleased
}

func (coordinator *OperationCoordinator) finishConfirmedRecoveryCallback(
	claim RecoveryCallbackClaim,
) error {
	if !coordinator.validRecoveryClaim(claim) {
		return errors.New("recovery callback claim belongs to another coordinator")
	}
	applied, err := coordinator.store.confirmClaimedAttempt(claim.placement)
	if err != nil {
		coordinator.operations.ReleaseRecoveryLease(claim.registry)
		return err
	}
	if !applied {
		coordinator.operations.ReleaseRecoveryLease(claim.registry)
		return ErrOperationSettlementGenerationUnavailable
	}
	if !coordinator.operations.ReleaseRecoveryLease(claim.registry) {
		return errors.New("lost callback recovery lease after placement confirmation")
	}
	return nil
}

func (coordinator *OperationCoordinator) finishRefusedRecoveryCallback(
	claim RecoveryCallbackClaim,
) error {
	if !coordinator.validRecoveryClaim(claim) ||
		generationOf(claim.placement) != OperationGenerationAttempt {
		return errors.New("exact unresolved recovery attempt is required")
	}
	applied, err := coordinator.store.refuseClaimedAttempt(claim.placement)
	if err != nil {
		coordinator.operations.ReleaseRecoveryLease(claim.registry)
		return err
	}
	if !applied {
		coordinator.operations.ReleaseRecoveryLease(claim.registry)
		return ErrOperationSettlementGenerationUnavailable
	}
	if !coordinator.operations.ReleaseRecoveryLease(claim.registry) {
		return errors.New("lost callback recovery lease after placement refusal")
	}
	return nil
}

func (coordinator *OperationCoordinator) finishRecoveryRetainingConfirmed(
	claim RecoveryCallbackClaim,
) error {
	if !coordinator.validRecoveryClaim(claim) ||
		generationOf(claim.placement) != OperationGenerationConfirmed {
		return errors.New("exact confirmed recovery generation is required")
	}
	return coordinator.finishConfirmedRecoveryCallback(claim)
}

// DeprovisionOwnedCallbackClaim proves both that close currently owns the
// exact Registry operation and that the same durable generation is reserved in
// the Store. It cannot finish or release close's Registry claim.
type DeprovisionOwnedCallbackClaim struct {
	issuer    *operationCoordinatorMarker
	operation operation.DeprovisionObservation
	placement AttemptClaim
}

func (claim DeprovisionOwnedCallbackClaim) Valid() bool {
	return claim.issuer != nil && claim.operation.Valid() && claim.placement.Valid()
}
func (claim DeprovisionOwnedCallbackClaim) Metadata() operation.SettlementMetadata {
	if !claim.Valid() {
		return operation.SettlementMetadata{}
	}
	return claim.operation.Metadata()
}
func (claim DeprovisionOwnedCallbackClaim) Generation() OperationGeneration {
	if !claim.Valid() {
		return OperationGenerationInvalid
	}
	return generationOf(claim.placement)
}

func (coordinator *OperationCoordinator) tryClaimDeprovisionOwnedCallback(
	leaseUUID string,
	id operation.OperationID,
) (DeprovisionOwnedCallbackClaim, bool, error) {
	if !coordinator.Valid() {
		return DeprovisionOwnedCallbackClaim{}, false, errors.New("operation coordinator is invalid")
	}
	observation, observed := coordinator.operations.ObserveDeprovisionOwned(leaseUUID, id)
	if !observed {
		return DeprovisionOwnedCallbackClaim{}, false, nil
	}
	placementClaim, claimed, err := coordinator.claimMatchingAttempt(observation.Metadata())
	if err != nil || !claimed {
		if err == nil {
			err = ErrOperationSettlementGenerationUnavailable
		}
		return DeprovisionOwnedCallbackClaim{}, false, err
	}
	return DeprovisionOwnedCallbackClaim{
		issuer: coordinator.marker, operation: observation, placement: placementClaim,
	}, true, nil
}

func (coordinator *OperationCoordinator) releaseDeprovisionOwnedCallback(
	claim DeprovisionOwnedCallbackClaim,
) bool {
	return coordinator.Valid() && claim.Valid() && claim.issuer == coordinator.marker &&
		coordinator.store.releaseAttemptClaim(claim.placement)
}

func (coordinator *OperationCoordinator) confirmDeprovisionOwnedCallback(
	claim DeprovisionOwnedCallbackClaim,
) (bool, error) {
	if !coordinator.Valid() || !claim.Valid() || claim.issuer != coordinator.marker {
		return false, errors.New("deprovision-owned callback claim belongs to another coordinator")
	}
	return coordinator.store.confirmClaimedAttempt(claim.placement)
}

func (coordinator *OperationCoordinator) refuseDeprovisionOwnedCallback(
	claim DeprovisionOwnedCallbackClaim,
) (bool, error) {
	if !coordinator.Valid() || !claim.Valid() || claim.issuer != coordinator.marker ||
		generationOf(claim.placement) != OperationGenerationAttempt {
		return false, errors.New("exact unresolved deprovision-owned callback claim is required")
	}
	return coordinator.store.refuseClaimedAttempt(claim.placement)
}

// attestClaimedAttempt verifies a live process claim still matches the exact
// durable generation without consuming either. A caller can therefore order a
// volatile Registry transition before a durable no-op/refusal while knowing a
// crash retains the Attempt for recovery.
func (s *Store) attestClaimedAttempt(
	claim AttemptClaim,
	kind attemptClaimKind,
) (bool, error) {
	if !claim.Valid() || claim.issuer != s || claim.kind != kind {
		return false, ErrInvalidAttemptToken
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.matchAttemptClaimLocked(claim), nil
}
