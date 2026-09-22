package operation

import (
	"errors"
	"time"

	"github.com/manifest-network/fred/internal/backend"
)

// settlementAuthorityMarker is non-zero-sized so distinct Registry bindings
// cannot compare equal under Go's zero-sized pointer rules.
type settlementAuthorityMarker struct{ _ byte }

// SettlementAuthority is the one-shot facet through which one placement
// coordinator may claim and finish Registry settlement work. Its zero value is
// invalid. Production components should receive the bound coordinator, never
// this Registry-level facet.
type SettlementAuthority struct {
	registry *Registry
	marker   *settlementAuthorityMarker
}

// Lookup returns a detached observation of the current operation. It carries
// no settlement capability; callers must still acquire one of the
// purpose-specific claims below before doing terminal work.
func (authority SettlementAuthority) Lookup(leaseUUID string) (SettlementMetadata, bool) {
	if !authority.valid() || leaseUUID == "" {
		return SettlementMetadata{}, false
	}
	record, exists := authority.registry.lookup(leaseUUID)
	if !exists {
		return SettlementMetadata{}, false
	}
	metadata := newSettlementMetadata(record)
	return metadata, metadata.Valid()
}

// TryClaimLeaseNow and ReleaseLease expose ordinary process-local exclusion to
// coordinator-minted purpose facets. They do not settle an operation and
// cannot be obtained independently from the Registry's one bound authority.
func (authority SettlementAuthority) TryClaimLeaseNow(leaseUUID string) LeaseClaimResult {
	if !authority.valid() {
		return LeaseClaimResult{}
	}
	return authority.registry.tryClaimLeaseNow(leaseUUID)
}

func (authority SettlementAuthority) ReleaseLease(claim LeaseClaim) bool {
	return authority.valid() && authority.registry.releaseLease(claim)
}

// TryInitiateProvisionClaimed installs a preparing provision only from a
// validated provision arm under the exact held lease claim.
func (authority SettlementAuthority) TryInitiateProvisionClaimed(
	claim LeaseClaim,
	initiation ProvisionInitiation,
) InitiationResult {
	if !authority.valid() || !initiation.valid() {
		return InitiationResult{}
	}
	return authority.registry.tryInitiateClaimed(claim, initiation.spec)
}

// TryInitiateRestoreClaimed installs a preparing, deliberately backend-unbound
// restore only from a validated restore arm under the exact held lease claim.
func (authority SettlementAuthority) TryInitiateRestoreClaimed(
	claim LeaseClaim,
	initiation RestoreInitiation,
) InitiationResult {
	if !authority.valid() || !initiation.valid() {
		return InitiationResult{}
	}
	return authority.registry.tryInitiateClaimed(claim, initiation.spec)
}

func (authority SettlementAuthority) AbortInitiation(
	initiation Initiation,
) InitiationCompletion {
	if !authority.valid() {
		return InitiationInvalid
	}
	return authority.registry.abortInitiation(initiation)
}

func (authority SettlementAuthority) CountsByBackend() map[string]int {
	if !authority.valid() {
		return nil
	}
	return authority.registry.countsByBackend()
}

func (authority SettlementAuthority) Contains(leaseUUID string) bool {
	return authority.valid() && authority.registry.contains(leaseUUID)
}

// CaptureReconciliationBoundary atomically joins the causal operation
// revision and active lease set for one inventory sweep. The returned opaque
// boundary is consumable only through this exact SettlementAuthority.
func (authority SettlementAuthority) CaptureReconciliationBoundary() ReconciliationBoundary {
	if !authority.valid() {
		return ReconciliationBoundary{}
	}
	return authority.registry.captureReconciliationBoundary()
}

// TryClaimReconciliationLease consumes an opaque boundary from this exact
// Registry. The application never receives the raw TrackerSnapshot that backs
// the claim decision.
func (authority SettlementAuthority) TryClaimReconciliationLease(
	leaseUUID string,
	boundary ReconciliationBoundary,
) LeaseClaimResult {
	if !authority.valid() || !boundary.Valid() ||
		boundary.snapshot.registry != authority.registry.identity {
		return LeaseClaimResult{}
	}
	return authority.registry.tryClaimLease(leaseUUID, boundary.snapshot)
}

func (authority SettlementAuthority) valid() bool {
	return authority.registry != nil && authority.marker != nil &&
		authority.registry.settlementAuthority == authority.marker
}

// MatchesRegistry lets a composition boundary idempotently recover an already
// constructed joined coordinator without exposing Registry mutation authority.
func (authority SettlementAuthority) MatchesRegistry(registry *Registry) bool {
	return authority.valid() && registry != nil && authority.registry == registry
}

// DispatchOperation is the Registry half of one synchronous backend dispatch.
// It can be issued only for the exact live preparing Initiation and is useful
// only through the SettlementAuthority that issued it. Placement joins it to
// durable write-ahead authority before exposing any call/terminal transition.
type DispatchOperation struct {
	issuer     *settlementAuthorityMarker
	registry   *Registry
	initiation Initiation
	metadata   SettlementMetadata
}

func (dispatch DispatchOperation) Valid() bool {
	return dispatch.issuer != nil && dispatch.registry != nil &&
		dispatch.initiation.Valid() && dispatch.metadata.Valid() &&
		dispatch.registry.settlementAuthority == dispatch.issuer &&
		dispatch.initiation.ID() == dispatch.metadata.ID()
}

// Metadata is a detached construction-time snapshot. Registry transitions
// still validate the live Initiation capability on every use.
func (dispatch DispatchOperation) Metadata() SettlementMetadata {
	if !dispatch.Valid() {
		return SettlementMetadata{}
	}
	return dispatch.metadata
}

// JoinDispatch converts a live preparing Initiation into the Registry half of
// a joined dispatch. The raw Initiation is deliberately not exposed again.
func (authority SettlementAuthority) JoinDispatch(
	initiation Initiation,
) (DispatchOperation, bool) {
	if !authority.valid() || !initiation.Valid() ||
		initiation.token.registry != authority.registry.identity {
		return DispatchOperation{}, false
	}
	authority.registry.mu.RLock()
	defer authority.registry.mu.RUnlock()
	tracked, exists := authority.registry.operations[initiation.token.leaseUUID]
	if !exists || tracked.initiation != initiation || tracked.token != initiation.token ||
		tracked.record.Phase != PhasePreparing || tracked.claim.Valid() ||
		tracked.terminalFinished {
		return DispatchOperation{}, false
	}
	metadata := newSettlementMetadata(tracked.record)
	if !metadata.Valid() {
		return DispatchOperation{}, false
	}
	return DispatchOperation{
		issuer: authority.marker, registry: authority.registry,
		initiation: initiation, metadata: metadata,
	}, true
}

func (authority SettlementAuthority) validDispatch(dispatch DispatchOperation) bool {
	return authority.valid() && dispatch.Valid() && dispatch.issuer == authority.marker &&
		dispatch.registry == authority.registry
}

// BindDispatchBackend performs restore's one-shot authoritative backend bind.
func (authority SettlementAuthority) BindDispatchBackend(
	dispatch DispatchOperation,
	backendName string,
) bool {
	return authority.validDispatch(dispatch) &&
		authority.registry.bindBackend(dispatch.initiation, backendName)
}

// BeginDispatchCall opens the callback-visible call phase only for a dispatch
// already joined to durable placement authority.
func (authority SettlementAuthority) BeginDispatchCall(dispatch DispatchOperation) bool {
	return authority.validDispatch(dispatch) &&
		authority.registry.beginCall(dispatch.initiation)
}

// ActivateDispatch completes an accepted synchronous return.
func (authority SettlementAuthority) ActivateDispatch(
	dispatch DispatchOperation,
) InitiationCompletion {
	if !authority.validDispatch(dispatch) {
		return InitiationInvalid
	}
	return authority.registry.activate(dispatch.initiation)
}

// AbortDispatch completes a definitive or ambiguous synchronous return after
// the joined placement coordinator has performed the appropriate durable step.
func (authority SettlementAuthority) AbortDispatch(
	dispatch DispatchOperation,
) InitiationCompletion {
	if !authority.validDispatch(dispatch) {
		return InitiationInvalid
	}
	return authority.registry.abortInitiation(dispatch.initiation)
}

// BindSettlementAuthority permanently binds this Registry's settlement lane
// to one coordinator construction. A second binding is refused.
func (registry *Registry) BindSettlementAuthority() (SettlementAuthority, error) {
	if registry == nil {
		return SettlementAuthority{}, errors.New("operation registry is required")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.settlementAuthority != nil {
		return SettlementAuthority{}, errors.New("operation registry settlement authority is already bound")
	}
	marker := &settlementAuthorityMarker{}
	registry.settlementAuthority = marker
	return SettlementAuthority{registry: registry, marker: marker}, nil
}

// SettlementMetadata is a detached read view of an exact claimed operation.
// It deliberately exposes no Registry claim or token.
type SettlementMetadata struct{ record Record }

func newSettlementMetadata(record Record) SettlementMetadata {
	if !record.Valid() {
		return SettlementMetadata{}
	}
	return SettlementMetadata{record: record.clone()}
}

func (metadata SettlementMetadata) Valid() bool                { return metadata.record.Valid() }
func (metadata SettlementMetadata) LeaseUUID() string          { return metadata.record.LeaseUUID }
func (metadata SettlementMetadata) Tenant() string             { return metadata.record.Tenant }
func (metadata SettlementMetadata) Backend() string            { return metadata.record.Backend }
func (metadata SettlementMetadata) ID() OperationID            { return metadata.record.ID }
func (metadata SettlementMetadata) Kind() Kind                 { return metadata.record.Kind }
func (metadata SettlementMetadata) Phase() Phase               { return metadata.record.Phase }
func (metadata SettlementMetadata) StartedAt() time.Time       { return metadata.record.StartedAt }
func (metadata SettlementMetadata) Items() []backend.LeaseItem { return metadata.record.clone().Items }
func (metadata SettlementMetadata) RoutingSKU() string         { return metadata.record.RoutingSKU() }
func (metadata SettlementMetadata) Settlement() SettlementKind { return metadata.record.Settlement }

type authorityClaim struct {
	issuer   *settlementAuthorityMarker
	registry *Registry
	metadata SettlementMetadata
	claim    SettlementClaim
}

func (claim authorityClaim) valid(actor settlementActor) bool {
	return claim.issuer != nil && claim.registry != nil && claim.metadata.Valid() &&
		claim.claim.Valid() && claim.claim.actor == actor &&
		claim.claim.token.registry == claim.registry.identity &&
		claim.claim.token.leaseUUID == claim.metadata.LeaseUUID() &&
		claim.claim.token.id == claim.metadata.ID() &&
		claim.registry.settlementAuthority == claim.issuer
}

// CallbackClaim is exact terminal ownership acquired for an authenticated
// operation callback. Its fields are inaccessible outside this package.
type CallbackClaim struct{ core authorityClaim }

func (claim CallbackClaim) Valid() bool { return claim.core.valid(settlementCallback) }
func (claim CallbackClaim) Metadata() SettlementMetadata {
	if !claim.Valid() {
		return SettlementMetadata{}
	}
	return claim.core.metadata
}

type CallbackClaimResult struct {
	claim   CallbackClaim
	outcome SettlementOutcome
}

func (result CallbackClaimResult) Outcome() SettlementOutcome { return result.outcome }
func (result CallbackClaimResult) Claimed() bool {
	return result.outcome == SettlementClaimed && result.claim.Valid()
}
func (result CallbackClaimResult) Claim() CallbackClaim {
	if !result.Claimed() {
		return CallbackClaim{}
	}
	return result.claim
}

func (authority SettlementAuthority) TryClaimCallback(
	leaseUUID string,
	id OperationID,
) CallbackClaimResult {
	if !authority.valid() {
		return CallbackClaimResult{outcome: SettlementInvalid}
	}
	result := authority.registry.tryClaimSettlement(
		leaseUUID, id, SettlementTerminal, settlementCallback,
	)
	if !result.Claimed() {
		return CallbackClaimResult{outcome: result.Outcome()}
	}
	return CallbackClaimResult{
		claim: CallbackClaim{core: authorityClaim{
			issuer: authority.marker, registry: authority.registry,
			metadata: newSettlementMetadata(result.record), claim: result.claim,
		}},
		outcome: SettlementClaimed,
	}
}

func (authority SettlementAuthority) ReleaseCallback(claim CallbackClaim) bool {
	return authority.valid() && claim.Valid() && claim.core.issuer == authority.marker &&
		authority.registry.releaseSettlement(claim.core.claim)
}

func (authority SettlementAuthority) FinishCallback(claim CallbackClaim) bool {
	return authority.valid() && claim.Valid() && claim.core.issuer == authority.marker &&
		authority.registry.finishSettlement(claim.core.claim)
}

// TimeoutCandidate is an issuer-bound snapshot; TryClaimTimeout must still
// win the exact live claim, so copied or stale candidates grant no authority.
type TimeoutCandidate struct {
	issuer   *settlementAuthorityMarker
	registry *Registry
	metadata SettlementMetadata
}

func (candidate TimeoutCandidate) Valid() bool {
	return candidate.issuer != nil && candidate.registry != nil && candidate.metadata.Valid() &&
		candidate.registry.settlementAuthority == candidate.issuer
}
func (candidate TimeoutCandidate) Metadata() SettlementMetadata {
	if !candidate.Valid() {
		return SettlementMetadata{}
	}
	return candidate.metadata
}

func (authority SettlementAuthority) TimedOut(timeout time.Duration) []TimeoutCandidate {
	if !authority.valid() {
		return nil
	}
	records := authority.registry.timedOut(timeout)
	candidates := make([]TimeoutCandidate, 0, len(records))
	for _, record := range records {
		candidates = append(candidates, TimeoutCandidate{
			issuer: authority.marker, registry: authority.registry,
			metadata: newSettlementMetadata(record),
		})
	}
	return candidates
}

type TimeoutClaim struct{ core authorityClaim }

func (claim TimeoutClaim) Valid() bool { return claim.core.valid(settlementTimeout) }
func (claim TimeoutClaim) Metadata() SettlementMetadata {
	if !claim.Valid() {
		return SettlementMetadata{}
	}
	return claim.core.metadata
}

type TimeoutClaimResult struct {
	claim   TimeoutClaim
	outcome SettlementOutcome
}

func (result TimeoutClaimResult) Outcome() SettlementOutcome { return result.outcome }
func (result TimeoutClaimResult) Claimed() bool {
	return result.outcome == SettlementClaimed && result.claim.Valid()
}
func (result TimeoutClaimResult) Claim() TimeoutClaim {
	if !result.Claimed() {
		return TimeoutClaim{}
	}
	return result.claim
}

func (authority SettlementAuthority) TryClaimTimeout(candidate TimeoutCandidate) TimeoutClaimResult {
	if !authority.valid() || !candidate.Valid() || candidate.issuer != authority.marker {
		return TimeoutClaimResult{outcome: SettlementInvalid}
	}
	metadata := candidate.metadata
	result := authority.registry.tryClaimSettlement(
		metadata.LeaseUUID(), metadata.ID(), SettlementTerminal, settlementTimeout,
	)
	if !result.Claimed() {
		return TimeoutClaimResult{outcome: result.Outcome()}
	}
	return TimeoutClaimResult{
		claim: TimeoutClaim{core: authorityClaim{
			issuer: authority.marker, registry: authority.registry,
			metadata: newSettlementMetadata(result.record), claim: result.claim,
		}},
		outcome: SettlementClaimed,
	}
}

func (authority SettlementAuthority) ReleaseTimeout(claim TimeoutClaim) bool {
	return authority.valid() && claim.Valid() && claim.core.issuer == authority.marker &&
		authority.registry.releaseSettlement(claim.core.claim)
}

func (authority SettlementAuthority) FinishTimeout(claim TimeoutClaim) bool {
	return authority.valid() && claim.Valid() && claim.core.issuer == authority.marker &&
		authority.registry.finishSettlement(claim.core.claim)
}

// RecoveryLeaseClaim is the callback-only lease fence used when the volatile
// operation record was lost but the exact placement generation survived a
// restart. It is intentionally distinct from ordinary LeaseClaim so a
// coordinator cannot accidentally use callback recovery authority to initiate
// new backend work.
type RecoveryLeaseClaim struct {
	issuer   *settlementAuthorityMarker
	registry *Registry
	claim    LeaseClaim
}

func (claim RecoveryLeaseClaim) Valid() bool {
	return claim.issuer != nil && claim.registry != nil && claim.claim.Valid() &&
		claim.claim.registry == claim.registry.identity &&
		claim.registry.settlementAuthority == claim.issuer
}

type RecoveryLeaseClaimResult struct {
	claim   RecoveryLeaseClaim
	outcome LeaseClaimOutcome
}

func (result RecoveryLeaseClaimResult) Outcome() LeaseClaimOutcome { return result.outcome }
func (result RecoveryLeaseClaimResult) Acquired() bool {
	return result.outcome == LeaseClaimAcquired && result.claim.Valid()
}
func (result RecoveryLeaseClaimResult) Claim() RecoveryLeaseClaim {
	if !result.Acquired() {
		return RecoveryLeaseClaim{}
	}
	return result.claim
}

// TryClaimCallbackRecoveryLease acquires an issuer-bound callback-recovery
// fence. The lease identity is subsequently joined to the placement store's
// exact durable operation generation by placement.OperationCoordinator.
func (authority SettlementAuthority) TryClaimCallbackRecoveryLease(
	leaseUUID string,
) RecoveryLeaseClaimResult {
	if !authority.valid() || leaseUUID == "" {
		return RecoveryLeaseClaimResult{outcome: LeaseClaimInvalid}
	}
	result := authority.registry.tryClaimCallbackRecoveryLease(leaseUUID)
	if !result.Acquired() {
		return RecoveryLeaseClaimResult{outcome: result.Outcome()}
	}
	return RecoveryLeaseClaimResult{
		claim: RecoveryLeaseClaim{
			issuer: authority.marker, registry: authority.registry, claim: result.Claim(),
		},
		outcome: LeaseClaimAcquired,
	}
}

func (authority SettlementAuthority) ReleaseRecoveryLease(claim RecoveryLeaseClaim) bool {
	return authority.valid() && claim.Valid() && claim.issuer == authority.marker &&
		authority.registry.releaseLease(claim.claim)
}

// HoldsLeaseClaim reports whether claim is the exact live ordinary lease fence
// issued by this authority's Registry. It exposes no claim identity and performs
// no mutation; joined coordinators use it to reject cross-Registry recovery and
// pruning inputs before an external side effect.
func (authority SettlementAuthority) HoldsLeaseClaim(claim LeaseClaim, leaseUUID string) bool {
	if !authority.valid() || !claim.Valid() || leaseUUID == "" ||
		claim.registry != authority.registry.identity || claim.leaseUUID != leaseUUID {
		return false
	}
	authority.registry.mu.RLock()
	defer authority.registry.mu.RUnlock()
	return authority.registry.leaseClaims[leaseUUID] == claim
}

// RecoverClaimed installs one exact durable operation while its ordinary lease
// fence is held. Keeping this transition on the one-shot settlement facet lets
// placement.OperationCoordinator own accepted redelivery end to end.
func (authority SettlementAuthority) RecoverClaimed(
	claim LeaseClaim,
	id OperationID,
	recovered RecoveredOperation,
) RecoveryResult {
	if !authority.valid() || !recovered.valid() {
		return RecoveryInvalid
	}
	return authority.registry.recoverClaimed(claim, id, recovered)
}

// DeprovisionClaim is exclusive close ownership for the exact operation found
// by lease. Callers never supply a separately observed operation ID.
type DeprovisionClaim struct{ core authorityClaim }

func (claim DeprovisionClaim) Valid() bool { return claim.core.valid(settlementDeprovision) }
func (claim DeprovisionClaim) Metadata() SettlementMetadata {
	if !claim.Valid() {
		return SettlementMetadata{}
	}
	return claim.core.metadata
}

type DeprovisionClaimResult struct {
	claim   DeprovisionClaim
	outcome SettlementOutcome
}

func (result DeprovisionClaimResult) Outcome() SettlementOutcome { return result.outcome }
func (result DeprovisionClaimResult) Claimed() bool {
	return result.outcome == SettlementClaimed && result.claim.Valid()
}
func (result DeprovisionClaimResult) Claim() DeprovisionClaim {
	if !result.Claimed() {
		return DeprovisionClaim{}
	}
	return result.claim
}

func (authority SettlementAuthority) TryClaimDeprovision(leaseUUID string) DeprovisionClaimResult {
	if !authority.valid() || leaseUUID == "" {
		return DeprovisionClaimResult{outcome: SettlementInvalid}
	}
	record, exists := authority.registry.lookup(leaseUUID)
	if !exists {
		return DeprovisionClaimResult{outcome: SettlementNotFound}
	}
	result := authority.registry.tryClaimSettlement(
		leaseUUID, record.ID, SettlementDeprovision, settlementDeprovision,
	)
	if !result.Claimed() {
		return DeprovisionClaimResult{outcome: result.Outcome()}
	}
	return DeprovisionClaimResult{
		claim: DeprovisionClaim{core: authorityClaim{
			issuer: authority.marker, registry: authority.registry,
			metadata: newSettlementMetadata(result.record), claim: result.claim,
		}},
		outcome: SettlementClaimed,
	}
}

func (authority SettlementAuthority) ReleaseDeprovision(claim DeprovisionClaim) bool {
	return authority.valid() && claim.Valid() && claim.core.issuer == authority.marker &&
		authority.registry.releaseSettlement(claim.core.claim)
}

func (authority SettlementAuthority) FinishDeprovision(claim DeprovisionClaim) bool {
	return authority.valid() && claim.Valid() && claim.core.issuer == authority.marker &&
		authority.registry.finishSettlement(claim.core.claim)
}

// DeprovisionObservation proves that the exact operation is currently owned
// by a live deprovision settlement claim. It grants no Registry mutation; its
// sole purpose is to let the joined placement coordinator consume an exact
// provision/restore callback that arrived while close owns the operation.
type DeprovisionObservation struct {
	issuer   *settlementAuthorityMarker
	registry *Registry
	metadata SettlementMetadata
}

func (observation DeprovisionObservation) Valid() bool {
	return observation.issuer != nil && observation.registry != nil &&
		observation.metadata.Valid() &&
		observation.registry.settlementAuthority == observation.issuer
}

func (observation DeprovisionObservation) Metadata() SettlementMetadata {
	if !observation.Valid() {
		return SettlementMetadata{}
	}
	return observation.metadata
}

func (authority SettlementAuthority) ObserveDeprovisionOwned(
	leaseUUID string,
	id OperationID,
) (DeprovisionObservation, bool) {
	if !authority.valid() || leaseUUID == "" || !id.Valid() {
		return DeprovisionObservation{}, false
	}
	authority.registry.mu.RLock()
	defer authority.registry.mu.RUnlock()
	tracked, exists := authority.registry.operations[leaseUUID]
	if !exists || tracked.record.ID != id || !tracked.claim.Valid() ||
		tracked.claim.actor != settlementDeprovision ||
		tracked.record.Settlement != SettlementDeprovision {
		return DeprovisionObservation{}, false
	}
	metadata := newSettlementMetadata(tracked.record)
	if !metadata.Valid() {
		return DeprovisionObservation{}, false
	}
	return DeprovisionObservation{
		issuer: authority.marker, registry: authority.registry, metadata: metadata,
	}, true
}
