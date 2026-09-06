package shared

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

// ErrOperationReleaseConflict means an operation token already appears in
// release history but no longer denotes the exact active generation this
// candidate would publish. Replaying it must never reactivate or duplicate an
// older operation.
var ErrOperationReleaseConflict = errors.New("operation release conflicts with durable history")

// OperationReleaseCandidate is a store-issued capability to publish the active
// Release of one exact provision or restore operation. Its fields are private:
// callers can carry the capability across the substrate mutation, but cannot
// replace its operation ID, callback routes, principal, topology, sizing, or
// admission timestamp before the durable success commit.
//
// A candidate is deliberately bound to one open ReleaseStore. After restart a
// caller must load the operation claim/outcome again and ask the reopened store
// to mint a fresh candidate. The zero value is invalid.
type OperationReleaseCandidate struct {
	settlement *OperationSettlement
	callbacks  *CallbackStore
	releases   *ReleaseStore
	authority  operationAuthority
}

// OperationExecutionClaim is the irreversible-effect capability returned only
// after the callback WAL durably advances NotStarted -> Started. Copies of the
// pre-effect candidate become stale at that transaction.
type OperationExecutionClaim struct {
	settlement *OperationSettlement
	candidate  OperationReleaseCandidate
	subject    OperationPhysicalSubject
	started    substratemutation.LiveExecution[OperationPhysicalSubject]
}

// OperationExecutionSuccess is evidence that the guarded operation worker
// returned successfully for one exact release candidate. It is deliberately a
// different type from journal-phase authority: only ExecuteOperation or the
// strict-inventory recovery minter can create it, and only this value may cross
// the active-release commit boundary.
type OperationExecutionSuccess struct {
	settlement *OperationSettlement
	execution  OperationExecutionClaim
	ready      OperationTargetReady
}

// OperationExecutionFailure is evidence that the guarded operation worker
// returned a definitive failure for one exact release candidate. It does not
// itself assert release absence; CommitOperationFailure rechecks that durable
// fact while consuming the proof.
type OperationExecutionFailure struct {
	settlement *OperationSettlement
	authority  OperationReleaseCandidate
	subject    OperationPhysicalSubject
	absent     OperationExactAbsent
	kind       operationExecutionFailureKind
	cause      error
}

// operationExecutionFailureKind keeps the three causally different failure
// proofs disjoint even though callback settlement intentionally consumes one
// exported capability type. Every value is minted inside this package:
// callers cannot turn a post-Started failure into a pre-effect refusal or
// replace strict absence evidence with a boolean.
type operationExecutionFailureKind uint8

const (
	operationExecutionFailureInvalid operationExecutionFailureKind = iota
	operationExecutionRefusedBeforeStart
	operationExecutionRefusedAfterStart
	operationExecutionAttestedAbsent
)

// OperationExecutionAmbiguous records that a guarded worker panicked or
// reported an ambiguous mutation outcome. It intentionally grants no terminal
// journal authority. The pending intent remains the recovery fence.
type OperationExecutionAmbiguous struct {
	settlement *OperationSettlement
	execution  OperationExecutionClaim
	cause      error
}

func (outcome OperationExecutionSuccess) Valid() bool {
	return outcome.settlement != nil && outcome.execution.candidate.callbacks != nil &&
		outcome.execution.candidate.releases != nil && outcome.execution.candidate.authority.entry != nil &&
		outcome.execution.subject.validFor(outcome.settlement) && outcome.ready.Valid() &&
		outcome.ready.validForOperation(outcome.execution.subject)
}

func (outcome OperationExecutionFailure) Valid() bool {
	if outcome.settlement == nil || !outcome.settlement.ownsOperationCandidate(outcome.authority) {
		return false
	}
	switch outcome.kind {
	case operationExecutionRefusedBeforeStart:
		return outcome.authority.authority.entry.EffectNotStarted &&
			!outcome.subject.Valid() && !outcome.absent.Valid()
	case operationExecutionRefusedAfterStart:
		return !outcome.authority.authority.entry.EffectNotStarted &&
			outcome.subject.validFor(outcome.settlement) &&
			outcome.subject.matchesCandidate(outcome.authority) &&
			!outcome.absent.Valid()
	case operationExecutionAttestedAbsent:
		return !outcome.authority.authority.entry.EffectNotStarted &&
			outcome.subject.validFor(outcome.settlement) &&
			outcome.subject.matchesCandidate(outcome.authority) &&
			outcome.absent.Valid() && outcome.absent.validForOperation(outcome.subject)
	default:
		return false
	}
}

func (outcome OperationExecutionAmbiguous) Valid() bool {
	return outcome.settlement != nil && outcome.execution.candidate.callbacks != nil &&
		outcome.execution.candidate.releases != nil &&
		outcome.execution.candidate.authority.entry != nil &&
		outcome.cause != nil
}

func (outcome OperationExecutionAmbiguous) Error() string {
	if outcome.cause == nil {
		return "ambiguous operation execution"
	}
	return outcome.cause.Error()
}

func (outcome OperationExecutionFailure) Cause() error   { return outcome.cause }
func (outcome OperationExecutionAmbiguous) Cause() error { return outcome.cause }

// OperationExecutionOutcome is the sealed result of one guarded physical
// execution. Sibling packages can inspect the concrete outcome but cannot
// implement or construct another variant.
type OperationExecutionOutcome interface {
	operationExecutionOutcome()
}

func (OperationExecutionSuccess) operationExecutionOutcome()   {}
func (OperationExecutionFailure) operationExecutionOutcome()   {}
func (OperationExecutionAmbiguous) operationExecutionOutcome() {}

// OperationReleaseCommitted is durable proof that the exact active Release
// derived from one pending operation has crossed its ReleaseStore commit
// boundary. Its zero value is invalid and every field is private: callers may
// carry the proof to the callback journal, but cannot splice a different
// operation, release version, or release digest into it.
//
// The proof is intentionally copyable and idempotent. Linearity is enforced by
// CallbackStore's exact pending-claim CAS, while the durable release version and
// digest make every copy name the same already-committed generation.
type OperationReleaseCommitted struct {
	settlement *OperationSettlement
	callbacks  *CallbackStore
	releases   *ReleaseStore
	authority  operationAuthority
	release    ReleaseClaim
	ready      OperationTargetReady
}

// OperationReleaseUncommitted is a store-issued, short-lived proof that the
// exact operation has no Release in this journal. Failure settlement rechecks
// this fact at consumption, so a copied proof cannot race a later successful
// commit into the contradictory Failed-plus-active state.
type OperationReleaseUncommitted struct {
	settlement *OperationSettlement
	callbacks  *CallbackStore
	releases   *ReleaseStore
	authority  operationAuthority
}

// OperationSettlement is the construction-bound handoff between the callback
// write-ahead journal and the release journal. Every operation release commit
// and terminal settlement crosses this pair so the exact pending claim and
// release fact are checked while holding one shared per-lease transition gate.
// The zero value is invalid.
type OperationSettlement struct {
	journalPair
	recoveryCoordinator *RecoveryCoordinator
	mutation            *substratemutation.Protocol[OperationPhysicalSubject]
	recovery            *substratemutation.RecoveryAttestor[OperationPhysicalSubject, OperationPhysicalEvidence]
	execute             func(context.Context, substratemutation.LiveExecution[OperationPhysicalSubject]) substratemutation.Result[OperationPhysicalSubject, OperationPhysicalEvidence]
	executeRecovery     func(context.Context, substratemutation.RecoveryExecution[OperationPhysicalSubject]) substratemutation.Result[OperationPhysicalSubject, OperationPhysicalEvidence]
}

// NewOperationSettlement binds the two identity-bound journals which jointly
// implement an asynchronous provision/restore operation. Aliased or merely
// lineage-compatible stores are insufficient: proofs name these exact open
// store instances and are invalid after either instance is replaced.
func NewOperationSettlement(
	callbacks *CallbackStore,
	releases *ReleaseStore,
) (*OperationSettlement, error) {
	pair, err := newJournalPair(callbacks, releases)
	if err != nil {
		return nil, fmt.Errorf("operation %w", err)
	}
	return &OperationSettlement{
		journalPair: pair,
		mutation:    substratemutation.NewProtocol[OperationPhysicalSubject](),
	}, nil
}

// BindOperationSubstrateExecutor atomically binds one narrow physical facade
// and the construction-fixed exhaustive classifier to this settlement. The
// paired recovery attestor remains private; callers cannot choose a verdict at
// recovery time.
func BindOperationSubstrateExecutor[T any](
	s *OperationSettlement,
	authorize substratemutation.Authorize,
	complete substratemutation.Complete,
	build func(substratemutation.Runner, OperationPhysicalSubject) T,
	run func(context.Context, T, OperationPhysicalSubject) error,
	classify func(context.Context, OperationPhysicalSubject) (OperationPhysicalEvidence, error),
) error {
	if s == nil || s.mutation == nil {
		return errors.New("operation settlement is invalid")
	}
	binding, err := s.mutation.NewGuardBinding()
	if err != nil {
		return err
	}
	guard, recovery, err := substratemutation.NewExecutor(
		binding, authorize, complete, build, run, classify,
	)
	if err != nil {
		return err
	}
	s.recovery = recovery
	s.execute = func(
		ctx context.Context,
		execution substratemutation.LiveExecution[OperationPhysicalSubject],
	) substratemutation.Result[OperationPhysicalSubject, OperationPhysicalEvidence] {
		return guard.Execute(execution, ctx)
	}
	s.executeRecovery = func(
		ctx context.Context,
		execution substratemutation.RecoveryExecution[OperationPhysicalSubject],
	) substratemutation.Result[OperationPhysicalSubject, OperationPhysicalEvidence] {
		return guard.ExecuteRecovery(execution, ctx)
	}
	return nil
}

// NewOperationIntentProbe binds an exact redelivery lookup to both journals
// that own operation settlement. Keeping admission and recovery on the same
// coordinator as terminal settlement makes a mismatched callback/release pair
// unrepresentable to backend callers.
func (s *OperationSettlement) NewOperationIntentProbe(
	leaseUUID, callbackURL string,
) (OperationIntentProbe, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return OperationIntentProbe{}, errors.New("operation settlement is invalid")
	}
	probe, err := s.callbacks.newOperationIntentProbe(leaseUUID, callbackURL)
	if err != nil {
		return OperationIntentProbe{}, err
	}
	probe.settlement = s
	return probe, nil
}

// ProbeOperationIntent classifies an exact redelivery without granting new
// mutation authority.
func (s *OperationSettlement) ProbeOperationIntent(
	probe OperationIntentProbe,
) (OperationIntentAdmissionDisposition, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return OperationIntentAdmissionNone, errors.New("operation settlement is invalid")
	}
	if probe.settlement != s {
		return OperationIntentAdmissionNone, errors.New(
			"operation intent probe belongs to another journal pair",
		)
	}
	return s.callbacks.probeOperationIntent(probe)
}

// NewOperationIntentCandidate freezes one operation request against the exact
// callback/release pair that will later publish its success or failure.
func (s *OperationSettlement) NewOperationIntentCandidate(
	spec OperationIntentSpec,
) (OperationIntentCandidate, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return OperationIntentCandidate{}, errors.New("operation settlement is invalid")
	}
	candidate, err := s.callbacks.newOperationIntentCandidate(spec)
	if err != nil {
		return OperationIntentCandidate{}, err
	}
	candidate.settlement = s
	return candidate, nil
}

// BeginOperationIntent commits the write-ahead record which grants first
// dispatch authority for this exact settlement pair.
func (s *OperationSettlement) BeginOperationIntent(
	candidate OperationIntentCandidate,
) (OperationIntentAdmission, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return OperationIntentAdmission{}, errors.New("operation settlement is invalid")
	}
	if candidate.settlement != s {
		return OperationIntentAdmission{}, errors.New(
			"operation intent candidate belongs to another journal pair",
		)
	}
	admission, err := s.callbacks.beginOperationIntent(candidate)
	if err != nil {
		return OperationIntentAdmission{}, err
	}
	admission.claim.settlement = s
	return admission, nil
}

// ListOperationIntents returns pending recovery claims from this exact
// settlement pair.
func (s *OperationSettlement) ListOperationIntents() ([]OperationIntentClaim, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return nil, errors.New("operation settlement is invalid")
	}
	claims, err := s.callbacks.listOperationIntents()
	if err != nil {
		return nil, err
	}
	for index, claim := range claims {
		durable, err := validateOperationIntentClaim(claim)
		if err != nil {
			return nil, err
		}
		durable.settlement = s
		claims[index] = durable
	}
	return claims, nil
}

// ListOperationRecoveryStates returns every rich operation head, including
// durable terminal heads that still fence late substrate creation.
func (s *OperationSettlement) ListOperationRecoveryStates() ([]OperationRecoveryState, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return nil, errors.New("operation settlement is invalid")
	}
	states, err := s.callbacks.listOperationRecoveryStates()
	if err != nil {
		return nil, err
	}
	for index, state := range states {
		if claim, ok := state.(OperationIntentClaim); ok {
			claim.settlement = s
			states[index] = claim
		}
	}
	return states, nil
}

// ListFailedOperationReceipts returns the permanent exact cleanup fences for
// operations that settled as failures.
func (s *OperationSettlement) ListFailedOperationReceipts() ([]FailedOperationReceipt, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return nil, errors.New("operation settlement is invalid")
	}
	return s.callbacks.listFailedOperationReceipts()
}

// LookupOperationRecovery returns the sealed durable state of one exact
// operation from the coordinator that owns its possible settlement.
func (s *OperationSettlement) LookupOperationRecovery(
	probe OperationIntentProbe,
) (OperationRecoveryState, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return nil, errors.New("operation settlement is invalid")
	}
	if probe.settlement != s {
		return nil, errors.New("operation intent probe belongs to another journal pair")
	}
	state, err := s.callbacks.lookupOperationRecovery(probe)
	if err != nil {
		return nil, err
	}
	if claim, ok := state.(OperationIntentClaim); ok {
		claim.settlement = s
		state = claim
	}
	return state, nil
}

// Valid reports whether the proof has a complete store-issued shape. The
// callback journal additionally checks the exact operation authority before it
// may consume the proof.
func (committed OperationReleaseCommitted) Valid() bool {
	return committed.settlement != nil &&
		committed.callbacks == committed.settlement.callbacks &&
		committed.releases == committed.settlement.releases &&
		committed.authority.entry != nil &&
		committed.authority.digest != ([sha256.Size]byte{}) && committed.release.valid()
}

// Valid reports whether the uncommitted proof has a complete store-issued
// shape. Durable absence is always rechecked by the consumer.
func (uncommitted OperationReleaseUncommitted) Valid() bool {
	return uncommitted.settlement != nil &&
		uncommitted.callbacks == uncommitted.settlement.callbacks &&
		uncommitted.releases == uncommitted.settlement.releases &&
		uncommitted.authority.entry != nil &&
		uncommitted.authority.digest != ([sha256.Size]byte{})
}

// LeaseUUID and OperationID expose only the immutable identity sealed into
// the proof. They let downstream typed adapters reject a proof for another
// actor generation without exposing any caller-writable authority fields.
func (committed OperationReleaseCommitted) LeaseUUID() string {
	return committed.authority.LeaseUUID()
}

func (committed OperationReleaseCommitted) OperationID() OperationID {
	return committed.authority.OperationID()
}

// MatchesIntent reports whether proof and claim name the same immutable
// operation in the same open journal pair. The execution-phase bit and digest
// are deliberately not compared: StartOperationExecution advances them while
// preserving the operation identity this terminal proof completes.
func (committed OperationReleaseCommitted) MatchesIntent(claim OperationIntentClaim) bool {
	durable := claim
	return durable.entry != nil && committed.Valid() && committed.settlement == durable.settlement &&
		sameImmutableOperation(committed.authority, durable.operationAuthority)
}

func (committed OperationReleaseCommitted) Release() (Release, bool) {
	if !committed.Valid() {
		return Release{}, false
	}
	release, err := releaseForOperationAuthority(committed.authority)
	if err == nil {
		release.Version = committed.release.Version()
	}
	return release, err == nil
}

// TargetRelease is the exact persisted operation generation. Unlike the
// pre-commit subject expectation, its Version is the ReleaseStore-assigned
// positive version sealed by the committed ReleaseClaim.
func (committed OperationReleaseCommitted) TargetRelease() (Release, bool) {
	return committed.Release()
}

// TargetReady returns the strict cohort sealed by the physical execution which
// produced this commit. Reconstructed callback-only proofs intentionally omit
// it; callers needing a runtime projection must recover strict inventory.
func (committed OperationReleaseCommitted) TargetReady() (OperationTargetReady, bool) {
	if !committed.Valid() || !committed.ready.Valid() ||
		committed.ready.LeaseUUID() != committed.LeaseUUID() ||
		committed.ready.OperationID() != committed.OperationID() {
		return OperationTargetReady{}, false
	}
	return committed.ready, true
}

func (uncommitted OperationReleaseUncommitted) LeaseUUID() string {
	return uncommitted.authority.LeaseUUID()
}

func (uncommitted OperationReleaseUncommitted) OperationID() OperationID {
	return uncommitted.authority.OperationID()
}

func (uncommitted OperationReleaseUncommitted) MatchesIntent(claim OperationIntentClaim) bool {
	durable := claim
	return durable.entry != nil && uncommitted.Valid() && uncommitted.settlement == durable.settlement &&
		sameImmutableOperation(uncommitted.authority, durable.operationAuthority)
}

func sameImmutableOperation(left, right operationAuthority) bool {
	if left.entry == nil || right.entry == nil {
		return false
	}
	a, b := *left.entry, *right.entry
	a.EffectNotStarted = false
	b.EffectNotStarted = false
	return operationIntentEntriesEqual(a, b)
}

func (uncommitted OperationReleaseUncommitted) Kind() OperationIntentKind {
	return uncommitted.authority.Kind()
}

func (uncommitted OperationReleaseUncommitted) CallbackURL() string {
	return uncommitted.authority.CallbackURL()
}

func (uncommitted OperationReleaseUncommitted) LifecycleCallbackURL() string {
	return uncommitted.authority.LifecycleCallbackURL()
}

// Intent returns the immutable operation claim sealed into this candidate. It
// is observation and routing authority only; it cannot settle either terminal
// outcome.
func (candidate OperationReleaseCandidate) Intent() OperationIntentClaim {
	if candidate.authority.entry == nil {
		return OperationIntentClaim{}
	}
	return OperationIntentClaim{
		operationAuthority: cloneOperationAuthority(candidate.authority),
		settlement:         candidate.settlement,
	}
}

// StartOperationExecution durably crosses the irreversible-effect boundary.
// The callback-row CAS invalidates every copy of the pre-effect candidate
// before a Started capability can reach a raw substrate mutator.
func (s *OperationSettlement) StartOperationExecution(
	candidate OperationReleaseCandidate,
) (OperationExecutionClaim, error) {
	if !s.ownsOperationCandidate(candidate) {
		return OperationExecutionClaim{}, errors.New(
			"operation candidate belongs to another journal pair",
		)
	}
	if candidate.authority.entry == nil || !candidate.authority.entry.EffectNotStarted {
		return OperationExecutionClaim{}, errors.New("operation execution has already started")
	}
	var nextCandidate OperationReleaseCandidate
	var subject OperationPhysicalSubject
	var predecessor *Release
	started, err := s.mutation.BeginAfter(func() (OperationPhysicalSubject, error) {
		unlock := s.callbacks.lockDeliveryLease(candidate.authority.LeaseUUID())
		defer unlock()
		err := s.callbacks.update(func(tx *bolt.Tx) error {
			claim := OperationIntentClaim{
				operationAuthority: cloneOperationAuthority(candidate.authority),
				settlement:         s,
			}
			if err := verifyOperationIntentTx(tx, claim); err != nil {
				return err
			}
			entry := *cloneOperationAuthority(claim.operationAuthority).entry
			entry.EffectNotStarted = false
			data, err := json.Marshal(entry)
			if err != nil {
				return err
			}
			next, err := decodeOperationIntent([]byte(entry.LeaseUUID), data)
			if err != nil {
				return err
			}
			transition, err := newStartOperationExecutionLeaseMutation(claim, next)
			if err != nil {
				return err
			}
			written, err := applyLeaseMutationTx(tx, transition)
			if err != nil {
				return err
			}
			refreshed := written.(operationLeaseMutationHead).claim
			refreshed.settlement = s
			nextCandidate, err = s.releases.prepareOperationRelease(s, refreshed.operationAuthority)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return OperationPhysicalSubject{}, err
		}
		if nextCandidate.Intent().Kind() == OperationIntentProvision {
			predecessor, err = s.releases.LatestActive(nextCandidate.authority.LeaseUUID())
			if err != nil {
				return OperationPhysicalSubject{}, fmt.Errorf("snapshot operation predecessor: %w", err)
			}
		}
		subject = newOperationPhysicalSubject(s, nextCandidate, predecessor)
		return subject, nil
	})
	if err != nil {
		return OperationExecutionClaim{}, err
	}
	return OperationExecutionClaim{
		settlement: s, candidate: nextCandidate, subject: subject, started: started,
	}, nil
}

// RefuseOperationExecution is the only normal-path failure minter before the
// irreversible boundary. Once StartOperationExecution commits, the candidate's
// exact row digest is stale and this method fails closed.
func (s *OperationSettlement) RefuseOperationExecution(
	candidate OperationReleaseCandidate,
) (OperationExecutionFailure, error) {
	if !s.ownsOperationCandidate(candidate) || candidate.authority.entry == nil ||
		!candidate.authority.entry.EffectNotStarted {
		return OperationExecutionFailure{}, errors.New("operation is not in the pre-effect phase")
	}
	unlock := s.callbacks.lockDeliveryLease(candidate.authority.LeaseUUID())
	defer unlock()
	if err := s.callbacks.requireCurrentOperationAuthority(candidate.authority); err != nil {
		return OperationExecutionFailure{}, err
	}
	return OperationExecutionFailure{
		settlement: s, authority: candidate, kind: operationExecutionRefusedBeforeStart,
	}, nil
}

// ExecuteOperation is the normal-path physical boundary after Started is
// durable. Until the substrate guard supplies its opaque tri-state outcome,
// every non-successful return is deliberately ambiguous; a plain error can
// never mint terminal failure authority.
func (s *OperationSettlement) ExecuteOperation(
	ctx context.Context,
	execution OperationExecutionClaim,
) (outcome OperationExecutionOutcome) {
	if s == nil || s.execute == nil || ctx == nil || execution.settlement != s ||
		!s.ownsOperationCandidate(execution.candidate) ||
		execution.candidate.authority.entry.EffectNotStarted ||
		!execution.subject.validFor(s) {
		return OperationExecutionAmbiguous{
			settlement: s,
			execution:  execution,
			cause:      errors.New("operation execution boundary is invalid"),
		}
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			outcome = OperationExecutionAmbiguous{
				settlement: s,
				execution:  execution,
				cause:      fmt.Errorf("operation worker panic: %v\n%s", recovered, debug.Stack()),
			}
		}
	}()
	physical := s.execute(ctx, execution.started)
	if err := substratemutation.ValidateLiveResult(s.mutation, execution.started, physical); err != nil {
		return OperationExecutionAmbiguous{settlement: s, execution: execution, cause: err}
	}
	switch physical.Kind() {
	case substratemutation.Attested:
		evidence, ok := physical.Evidence()
		if !ok {
			return OperationExecutionAmbiguous{settlement: s, execution: execution,
				cause: errors.New("attested operation result has no evidence")}
		}
		if err := validateOperationPhysicalEvidence(execution.subject, evidence); err != nil {
			return OperationExecutionAmbiguous{settlement: s, execution: execution, cause: err}
		}
		switch evidence.kind {
		case operationPhysicalEvidenceTargetReady:
			return OperationExecutionSuccess{
				settlement: s, execution: execution, ready: evidence.targetReady,
			}
		case operationPhysicalEvidenceExactAbsent:
			return OperationExecutionFailure{
				settlement: s, authority: execution.candidate,
				subject: execution.subject, absent: evidence.exactAbsent,
				kind: operationExecutionAttestedAbsent,
			}
		default:
			return OperationExecutionAmbiguous{settlement: s, execution: execution,
				cause: errors.New("unknown operation evidence")}
		}
	case substratemutation.Refused:
		return OperationExecutionFailure{
			settlement: s, authority: execution.candidate,
			subject: execution.subject, kind: operationExecutionRefusedAfterStart,
			cause: physical.Err(),
		}
	case substratemutation.Ambiguous:
		cause := physical.Err()
		if cause == nil {
			cause = errors.New("operation mutation outcome is ambiguous")
		}
		return OperationExecutionAmbiguous{settlement: s, execution: execution, cause: cause}
	default:
		return OperationExecutionAmbiguous{
			settlement: s, execution: execution,
			cause: fmt.Errorf("invalid operation mutation outcome %s", physical.Kind()),
		}
	}
}

// RecoverOperationExecution is the only post-restart physical outcome minter.
// Its classifier was bound at construction; the caller supplies only context
// and an exact durable claim and therefore cannot select success or failure.
func (s *OperationSettlement) RecoverOperationExecution(
	ctx context.Context,
	scope LeaseRecoveryScope,
	claim OperationIntentClaim,
) (OperationExecutionOutcome, error) {
	if s == nil {
		return nil, errors.New("operation recovery requires exact lease-quiescence authority")
	}
	releaseScope, valid := scope.enter(s.recoveryCoordinator, claim.LeaseUUID())
	if !valid {
		return nil, errors.New("operation recovery requires exact lease-quiescence authority")
	}
	defer releaseScope()
	if s.recovery == nil {
		return nil, errors.New("operation substrate recovery attestor is not bound")
	}
	durable, err := requireOperationSettlementClaim(s, claim)
	if err != nil {
		return nil, err
	}
	if durable.entry.EffectNotStarted {
		candidate, err := s.PrepareOperationRelease(claim)
		if err != nil {
			return nil, err
		}
		return s.RefuseOperationExecution(candidate)
	}
	var candidate OperationReleaseCandidate
	var subject OperationPhysicalSubject
	var predecessor *Release
	recovered, err := s.mutation.RecoverAfter(func() (OperationPhysicalSubject, error) {
		unlock := s.callbacks.lockDeliveryLease(durable.LeaseUUID())
		defer unlock()
		if err := s.callbacks.requireCurrentOperationClaim(durable); err != nil {
			return OperationPhysicalSubject{}, err
		}
		if durable.entry.EffectNotStarted {
			return OperationPhysicalSubject{}, errors.New(
				"operation has not crossed the durable execution boundary",
			)
		}
		candidate, err = s.releases.prepareOperationRelease(s, durable.operationAuthority)
		if err != nil {
			return OperationPhysicalSubject{}, err
		}
		if durable.Kind() == OperationIntentProvision {
			predecessor, err = s.releases.LatestActive(durable.LeaseUUID())
			if err != nil {
				return OperationPhysicalSubject{}, fmt.Errorf("snapshot recovered operation predecessor: %w", err)
			}
		}
		subject = newOperationPhysicalSubject(s, candidate, predecessor)
		return subject, nil
	})
	if err != nil {
		return nil, err
	}
	result := s.recovery.Inspect(recovered, ctx)
	if err := substratemutation.ValidateRecoveryResult(s.mutation, recovered, result); err != nil {
		return nil, err
	}
	execution := OperationExecutionClaim{
		settlement: s, candidate: candidate,
		subject: subject,
	}
	switch result.Kind() {
	case substratemutation.Attested:
		evidence, ok := result.Evidence()
		if !ok {
			return nil, errors.New("attested operation recovery has no evidence")
		}
		if err := validateOperationPhysicalEvidence(execution.subject, evidence); err != nil {
			return nil, err
		}
		switch evidence.kind {
		case operationPhysicalEvidenceTargetReady:
			return OperationExecutionSuccess{
				settlement: s, execution: execution, ready: evidence.targetReady,
			}, nil
		case operationPhysicalEvidenceExactAbsent:
			return OperationExecutionFailure{
				settlement: s, authority: candidate,
				subject: subject, absent: evidence.exactAbsent,
				kind: operationExecutionAttestedAbsent,
			}, nil
		default:
			return nil, errors.New("unknown operation recovery evidence")
		}
	case substratemutation.Ambiguous:
		cause := result.Err()
		if cause == nil {
			cause = errors.New("operation recovery remained ambiguous")
		}
		return OperationExecutionAmbiguous{
			settlement: s, execution: execution, cause: cause,
		}, nil
	default:
		return nil, fmt.Errorf("invalid operation recovery result %s: %w", result.Kind(), result.Err())
	}
}

// CleanupRecoveredOperation runs the construction-bound destructive recovery
// handler for one exact current Started intent, then returns only evidence from
// the same executor's exhaustive classifier. The caller supplies no workflow,
// target IDs, project name, or volume names; all such authority is derived from
// the opaque subject minted by the durable re-read below.
func (s *OperationSettlement) CleanupRecoveredOperation(
	ctx context.Context,
	scope LeaseRecoveryScope,
	claim OperationIntentClaim,
) (OperationExecutionOutcome, error) {
	if s == nil {
		return nil, errors.New("operation cleanup requires exact lease-quiescence authority")
	}
	releaseScope, valid := scope.enter(s.recoveryCoordinator, claim.LeaseUUID())
	if !valid {
		return nil, errors.New("operation cleanup requires exact lease-quiescence authority")
	}
	defer releaseScope()
	if s.executeRecovery == nil {
		return nil, errors.New("operation substrate recovery executor is not bound")
	}
	durable, err := requireOperationSettlementClaim(s, claim)
	if err != nil {
		return nil, err
	}
	if durable.entry.EffectNotStarted {
		candidate, err := s.PrepareOperationRelease(durable)
		if err != nil {
			return nil, err
		}
		return s.RefuseOperationExecution(candidate)
	}
	var candidate OperationReleaseCandidate
	var subject OperationPhysicalSubject
	var predecessor *Release
	execution, err := s.mutation.RecoverAfter(func() (OperationPhysicalSubject, error) {
		unlock := s.callbacks.lockDeliveryLease(durable.LeaseUUID())
		defer unlock()
		if err := s.callbacks.requireCurrentOperationClaim(durable); err != nil {
			return OperationPhysicalSubject{}, err
		}
		if durable.entry.EffectNotStarted {
			return OperationPhysicalSubject{}, errors.New(
				"operation has not crossed the durable execution boundary",
			)
		}
		candidate, err = s.releases.prepareOperationRelease(s, durable.operationAuthority)
		if err != nil {
			return OperationPhysicalSubject{}, err
		}
		if durable.Kind() == OperationIntentProvision {
			predecessor, err = s.releases.LatestActive(durable.LeaseUUID())
			if err != nil {
				return OperationPhysicalSubject{}, fmt.Errorf("snapshot recovered operation predecessor: %w", err)
			}
		}
		subject = newOperationPhysicalSubjectForMode(
			s, candidate, predecessor, operationPhysicalRecoveryCleanup,
		)
		return subject, nil
	})
	if err != nil {
		return nil, err
	}
	result := s.executeRecovery(ctx, execution)
	if err := substratemutation.ValidateRecoveryResult(s.mutation, execution, result); err != nil {
		return nil, err
	}
	claimResult := OperationExecutionClaim{settlement: s, candidate: candidate, subject: subject}
	switch result.Kind() {
	case substratemutation.Attested:
		evidence, ok := result.Evidence()
		if !ok {
			return nil, errors.New("attested operation cleanup has no evidence")
		}
		if err := validateOperationPhysicalEvidence(subject, evidence); err != nil {
			return nil, err
		}
		switch evidence.kind {
		case operationPhysicalEvidenceTargetReady:
			return OperationExecutionSuccess{settlement: s, execution: claimResult, ready: evidence.targetReady}, nil
		case operationPhysicalEvidenceExactAbsent:
			return OperationExecutionFailure{
				settlement: s, authority: candidate, subject: subject, absent: evidence.exactAbsent,
				kind: operationExecutionAttestedAbsent,
			}, nil
		default:
			return nil, errors.New("unknown operation cleanup evidence")
		}
	case substratemutation.Refused, substratemutation.Ambiguous:
		cause := result.Err()
		if cause == nil {
			cause = errors.New("operation recovery cleanup remained ambiguous")
		}
		return OperationExecutionAmbiguous{settlement: s, execution: claimResult, cause: cause}, nil
	default:
		return nil, fmt.Errorf("invalid operation cleanup result %s: %w", result.Kind(), result.Err())
	}
}

// CleanupFailedOperationReceipt executes the same construction-bound physical
// handler in permanent late-arrival mode. The receipt itself is store-issued
// and bound to this exact callback journal; a fresh durable re-read confirms it
// still exists before any mutation capability is minted.
func (s *OperationSettlement) CleanupFailedOperationReceipt(
	ctx context.Context,
	scope LeaseRecoveryScope,
	receipt FailedOperationReceipt,
) error {
	if s == nil {
		return errors.New("failed-operation cleanup requires exact lease-quiescence authority")
	}
	releaseScope, valid := scope.enter(s.recoveryCoordinator, receipt.LeaseUUID())
	if !valid {
		return errors.New("failed-operation cleanup requires exact lease-quiescence authority")
	}
	defer releaseScope()
	if s.executeRecovery == nil || !s.ownsFailedOperationReceipt(receipt) {
		return errors.New("failed-operation cleanup receipt belongs to another journal pair")
	}
	var subject OperationPhysicalSubject
	execution, err := s.mutation.RecoverAfter(func() (OperationPhysicalSubject, error) {
		current, err := s.callbacks.listFailedOperationReceipts()
		if err != nil {
			return OperationPhysicalSubject{}, err
		}
		found := false
		for _, candidate := range current {
			if sameFailedOperationReceipt(candidate, receipt) {
				found = true
				break
			}
		}
		if !found {
			return OperationPhysicalSubject{}, errors.New("failed-operation cleanup receipt is no longer durable")
		}
		subject = newFailedOperationCleanupSubject(s, receipt)
		return subject, nil
	})
	if err != nil {
		return err
	}
	result := s.executeRecovery(ctx, execution)
	if err := substratemutation.ValidateRecoveryResult(s.mutation, execution, result); err != nil {
		return err
	}
	if result.Kind() != substratemutation.Attested {
		return fmt.Errorf("failed-operation cleanup is %s: %w", result.Kind(), result.Err())
	}
	evidence, ok := result.Evidence()
	if !ok {
		return errors.New("attested failed-operation cleanup has no evidence")
	}
	if err := validateOperationPhysicalEvidence(subject, evidence); err != nil {
		return err
	}
	if evidence.kind != operationPhysicalEvidenceFailedReceiptAbsent {
		return errors.New("failed-operation cleanup has wrong evidence")
	}
	return nil
}

func sameFailedOperationReceipt(left, right FailedOperationReceipt) bool {
	return left.issuer != nil && right.issuer != nil &&
		left.OperationID() == right.OperationID() &&
		left.LeaseUUID() == right.LeaseUUID() &&
		left.Kind() == right.Kind() &&
		left.CallbackURL() == right.CallbackURL() &&
		left.LifecycleCallbackURL() == right.LifecycleCallbackURL() &&
		left.Backend() == right.Backend() &&
		left.BackendStorageID() == right.BackendStorageID() &&
		left.Tenant() == right.Tenant() &&
		left.ProviderUUID() == right.ProviderUUID() &&
		left.SettledAt().Equal(right.SettledAt()) &&
		left.Error() == right.Error()
}

func (s *OperationSettlement) ownsOperationCandidate(candidate OperationReleaseCandidate) bool {
	return s != nil && s.callbacks != nil && s.releases != nil &&
		candidate.settlement == s &&
		candidate.callbacks == s.callbacks && candidate.releases == s.releases &&
		candidate.authority.entry != nil
}

func (s *OperationSettlement) ownsFailedOperationReceipt(receipt FailedOperationReceipt) bool {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return false
	}
	return receipt.issuer == s.callbacks && receipt.storageID.Valid() &&
		receipt.record.OperationID.Valid() && receipt.record.LeaseUUID != ""
}

// PrepareOperationRelease validates and detaches the complete active-release
// authority of a pending operation. The returned capability can be used for a
// pre-side-effect capacity proof and then crossed through StartOperationExecution.
func (s *OperationSettlement) PrepareOperationRelease(
	claim OperationIntentClaim,
) (OperationReleaseCandidate, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return OperationReleaseCandidate{}, errors.New("operation settlement is invalid")
	}
	durable, err := requireOperationSettlementClaim(s, claim)
	if err != nil {
		return OperationReleaseCandidate{}, err
	}
	unlock := s.callbacks.lockDeliveryLease(durable.LeaseUUID())
	defer unlock()
	if err := s.callbacks.requireCurrentOperationClaim(durable); err != nil {
		return OperationReleaseCandidate{}, err
	}
	return s.releases.prepareOperationRelease(s, durable.operationAuthority)
}

func (s *ReleaseStore) prepareOperationRelease(
	settlement *OperationSettlement,
	authority operationAuthority,
) (OperationReleaseCandidate, error) {
	if settlement == nil || settlement.releases != s || settlement.callbacks == nil {
		return OperationReleaseCandidate{}, errors.New("operation settlement is invalid")
	}
	if err := requireOperationStoreLineage(s, authority); err != nil {
		return OperationReleaseCandidate{}, err
	}
	authority = cloneOperationAuthority(authority)
	if _, err := releaseForOperationAuthority(authority); err != nil {
		return OperationReleaseCandidate{}, err
	}
	return OperationReleaseCandidate{
		settlement: settlement,
		callbacks:  settlement.callbacks,
		releases:   s,
		authority:  authority,
	}, nil
}

// CheckOperationReleaseCapacity proves that the exact candidate currently fits
// without writing it. CommitOperationSuccess repeats both proof validation and
// capacity planning inside its write transaction; this method is advisory and
// is useful only before the first external side effect.
func (s *OperationSettlement) CheckOperationReleaseCapacity(
	candidate OperationReleaseCandidate,
) error {
	if !s.ownsOperationCandidate(candidate) {
		return errors.New("operation release candidate belongs to another journal pair")
	}
	unlock := s.callbacks.lockDeliveryLease(candidate.authority.LeaseUUID())
	defer unlock()
	if err := s.callbacks.requireCurrentOperationAuthority(candidate.authority); err != nil {
		return err
	}
	return s.releases.view(func(tx *bolt.Tx) error {
		leaseUUID, release, err := validateOperationReleaseCandidate(s.releases, candidate)
		if err != nil {
			return err
		}
		current := tx.Bucket(releasesBucketName).Get([]byte(leaseUUID))
		idempotent, err := classifyOperationReleaseReplay(current, leaseUUID, release)
		if err != nil || idempotent {
			return err
		}
		_, _, err = planAppendedReleaseHistory(
			current,
			leaseUUID,
			release,
			true,
			releaseHistoryCapacityCutoff(s.releases.maxAge, time.Now()),
			backend.MaxStoredReleaseHistoryBytes,
		)
		return err
	})
}

// CommitOperationSuccess consumes guarded physical-success evidence and
// atomically publishes the exact operation-derived active Release.
func (s *OperationSettlement) CommitOperationSuccess(
	outcome OperationExecutionSuccess,
) (OperationReleaseCommitted, error) {
	if outcome.settlement != s || !outcome.Valid() {
		return OperationReleaseCommitted{}, errors.New(
			"operation success outcome belongs to another execution boundary",
		)
	}
	committed, err := s.appendOperationRelease(outcome.execution.candidate)
	if err != nil {
		return OperationReleaseCommitted{}, err
	}
	release, ok := committed.Release()
	if !ok {
		return OperationReleaseCommitted{}, errors.New("committed operation release cannot be reconstructed")
	}
	committed.ready = outcome.ready.withCommittedRelease(release)
	if !committed.ready.Valid() {
		return OperationReleaseCommitted{}, errors.New("committed operation ready evidence cannot be rebound")
	}
	return committed, nil
}

func (s *OperationSettlement) appendOperationRelease(
	candidate OperationReleaseCandidate,
) (OperationReleaseCommitted, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return OperationReleaseCommitted{}, errors.New("operation settlement is invalid")
	}
	if !s.ownsOperationCandidate(candidate) {
		return OperationReleaseCommitted{}, errors.New(
			"operation release candidate belongs to another journal pair",
		)
	}
	unlock := s.callbacks.lockDeliveryLease(candidate.authority.LeaseUUID())
	defer unlock()
	if err := s.callbacks.requireCurrentOperationAuthority(candidate.authority); err != nil {
		return OperationReleaseCommitted{}, err
	}
	var committed OperationReleaseCommitted
	err := s.releases.update(func(tx *bolt.Tx) error {
		leaseUUID, release, err := validateOperationReleaseCandidate(s.releases, candidate)
		if err != nil {
			return err
		}
		bucket := tx.Bucket(releasesBucketName)
		current := bucket.Get([]byte(leaseUUID))
		idempotent, err := classifyOperationReleaseReplay(current, leaseUUID, release)
		if err != nil {
			return err
		}
		if idempotent {
			history, decodeErr := decodeReleaseHistory(current)
			if decodeErr != nil {
				return decodeErr
			}
			committed, err = operationReleaseCommittedFromHistory(
				s, s.callbacks, s.releases, candidate.authority, history,
			)
			return err
		}
		releases, _, err := planAppendedReleaseHistory(
			current,
			leaseUUID,
			release,
			true,
			releaseHistoryCapacityCutoff(s.releases.maxAge, time.Now()),
			backend.MaxStoredReleaseHistoryBytes,
		)
		if err != nil {
			return err
		}
		encoded, err := encodeReleaseHistoryWithinLimit(
			releases,
			backend.MaxStoredReleaseHistoryBytes,
		)
		if err != nil {
			return fmt.Errorf("failed to marshal releases: %w", err)
		}
		if err := bucket.Put([]byte(leaseUUID), encoded); err != nil {
			return err
		}
		committed, err = operationReleaseCommittedFromHistory(
			s, s.callbacks, s.releases, candidate.authority, releases,
		)
		return err
	})
	if err != nil {
		return OperationReleaseCommitted{}, err
	}
	return committed, nil
}

// CommitOperationFailure consumes guarded physical-failure evidence and mints
// the exact release-absence proof accepted by callback settlement. Absence is
// rechecked at consumption, so a copied or delayed outcome cannot contradict a
// concurrently committed success.
func (s *OperationSettlement) CommitOperationFailure(
	outcome OperationExecutionFailure,
) (OperationReleaseUncommitted, error) {
	if outcome.settlement != s || !outcome.Valid() {
		return OperationReleaseUncommitted{}, errors.New(
			"operation failure outcome belongs to another execution boundary",
		)
	}
	candidate := outcome.authority
	if !s.ownsOperationCandidate(candidate) {
		return OperationReleaseUncommitted{}, errors.New(
			"operation failure outcome belongs to another journal pair",
		)
	}
	durable := OperationIntentClaim{
		operationAuthority: cloneOperationAuthority(candidate.authority),
		settlement:         s,
	}
	unlock := s.callbacks.lockDeliveryLease(durable.LeaseUUID())
	defer unlock()
	return s.proveUncommittedOperationLocked(durable)
}

// ProveCommittedOperation reconstructs a success-settlement proof after a
// crash or an ambiguous callback-journal write. Only the exact currently active
// Release derived from claim is admissible; an older, superseded, divergent, or
// absent generation grants no success authority.
func (s *OperationSettlement) ProveCommittedOperation(
	claim OperationIntentClaim,
) (OperationReleaseCommitted, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return OperationReleaseCommitted{}, errors.New("operation settlement is invalid")
	}
	durable, err := requireOperationSettlementClaim(s, claim)
	if err != nil {
		return OperationReleaseCommitted{}, err
	}
	unlock := s.callbacks.lockDeliveryLease(durable.LeaseUUID())
	defer unlock()
	if err := s.callbacks.requireCurrentOperationClaim(durable); err != nil {
		return OperationReleaseCommitted{}, err
	}
	if err := requireOperationStoreLineage(s.releases, durable.operationAuthority); err != nil {
		return OperationReleaseCommitted{}, err
	}
	var committed OperationReleaseCommitted
	err = s.releases.view(func(tx *bolt.Tx) error {
		history, err := readReleaseHistoryTx(tx, durable.LeaseUUID())
		if err != nil {
			return err
		}
		committed, err = operationReleaseCommittedFromHistory(
			s, s.callbacks, s.releases, durable.operationAuthority, history,
		)
		return err
	})
	if err != nil {
		return OperationReleaseCommitted{}, err
	}
	return committed, nil
}

func (s *OperationSettlement) proveUncommittedOperationLocked(
	durable OperationIntentClaim,
) (OperationReleaseUncommitted, error) {
	if err := s.callbacks.requireCurrentOperationClaim(durable); err != nil {
		return OperationReleaseUncommitted{}, err
	}
	if err := requireOperationStoreLineage(s.releases, durable.operationAuthority); err != nil {
		return OperationReleaseUncommitted{}, err
	}
	if err := s.releases.requireOperationReleaseAbsent(durable.operationAuthority); err != nil {
		return OperationReleaseUncommitted{}, err
	}
	return OperationReleaseUncommitted{
		settlement: s, callbacks: s.callbacks, releases: s.releases,
		authority: cloneOperationAuthority(durable.operationAuthority),
	}, nil
}

func (s *ReleaseStore) requireOperationReleaseAbsent(authority operationAuthority) error {
	return s.view(func(tx *bolt.Tx) error {
		data := tx.Bucket(releasesBucketName).Get([]byte(authority.LeaseUUID()))
		if data == nil {
			return nil
		}
		history, err := decodeReleaseHistory(data)
		if err != nil {
			return fmt.Errorf("decode operation release history: %w", err)
		}
		if err := validateReleaseHistory(history); err != nil {
			return fmt.Errorf("validate operation release history: %w", err)
		}
		for _, release := range history {
			if release.OperationID == authority.OperationID() {
				return fmt.Errorf(
					"%w: operation %s already has release version %d with status %q",
					ErrOperationReleaseConflict, authority.OperationID().Fingerprint(), release.Version, release.Status,
				)
			}
		}
		return nil
	})
}

func operationReleaseCommittedFromHistory(
	settlement *OperationSettlement,
	callbacks *CallbackStore,
	releases *ReleaseStore,
	authority operationAuthority,
	history []Release,
) (OperationReleaseCommitted, error) {
	derived, err := releaseForOperationAuthority(authority)
	if err != nil {
		return OperationReleaseCommitted{}, err
	}
	for index := range history {
		existing := history[index]
		if existing.OperationID != derived.OperationID || existing.Status != "active" {
			continue
		}
		equal, err := equivalentOperationRelease(existing, derived)
		if err != nil {
			return OperationReleaseCommitted{}, err
		}
		if !equal {
			return OperationReleaseCommitted{}, fmt.Errorf(
				"%w: active operation release differs from exact authority",
				ErrOperationReleaseConflict,
			)
		}
		encoded, err := json.Marshal(existing)
		if err != nil {
			return OperationReleaseCommitted{}, fmt.Errorf("encode committed operation release: %w", err)
		}
		return OperationReleaseCommitted{
			settlement: settlement,
			callbacks:  callbacks,
			releases:   releases,
			authority:  cloneOperationAuthority(authority),
			release: ReleaseClaim{
				issuer:    releases,
				leaseUUID: authority.LeaseUUID(),
				version:   existing.Version,
				digest:    sha256.Sum256(encoded),
			},
		}, nil
	}
	return OperationReleaseCommitted{}, fmt.Errorf(
		"%w: operation %s has no exact active release",
		ErrOperationReleaseConflict, authority.OperationID().Fingerprint(),
	)
}

func classifyOperationReleaseReplay(
	data []byte,
	leaseUUID string,
	candidate Release,
) (bool, error) {
	if data == nil {
		return false, nil
	}
	history, err := decodeReleaseHistory(data)
	if err != nil {
		return false, fmt.Errorf("corrupted release data for %s: %w", leaseUUID, err)
	}
	if err := validateReleaseHistory(history); err != nil {
		return false, fmt.Errorf("invalid release data for %s: %w", leaseUUID, err)
	}
	idempotent := false
	for _, existing := range history {
		if existing.OperationID != candidate.OperationID {
			continue
		}
		equal, err := equivalentOperationRelease(existing, candidate)
		if err != nil {
			return false, err
		}
		if existing.Status != "active" || !equal || idempotent {
			return false, fmt.Errorf(
				"%w: operation %s is already present as release version %d with status %q",
				ErrOperationReleaseConflict,
				candidate.OperationID.Fingerprint(),
				existing.Version,
				existing.Status,
			)
		}
		idempotent = true
	}
	return idempotent, nil
}

func equivalentOperationRelease(existing, candidate Release) (bool, error) {
	existing.Version = 0
	candidate.Version = 0
	existingJSON, err := json.Marshal(existing)
	if err != nil {
		return false, fmt.Errorf("encode existing operation release: %w", err)
	}
	candidateJSON, err := json.Marshal(candidate)
	if err != nil {
		return false, fmt.Errorf("encode candidate operation release: %w", err)
	}
	return bytes.Equal(existingJSON, candidateJSON), nil
}

func validateOperationReleaseCandidate(
	store *ReleaseStore,
	candidate OperationReleaseCandidate,
) (string, Release, error) {
	if store == nil || candidate.callbacks == nil || candidate.releases != store {
		return "", Release{}, errors.New(
			"operation release candidate was not minted by this release journal",
		)
	}
	if err := requireOperationStoreLineage(store, candidate.authority); err != nil {
		return "", Release{}, err
	}
	if _, err := validateOperationIntentClaim(OperationIntentClaim{
		operationAuthority: candidate.authority,
	}); err != nil {
		return "", Release{}, err
	}
	derived, err := releaseForOperationAuthority(candidate.authority)
	if err != nil {
		return "", Release{}, err
	}
	return candidate.authority.LeaseUUID(), derived, nil
}

func validateOperationReleaseCommit(
	callbackStore *CallbackStore,
	claim OperationIntentClaim,
	committed OperationReleaseCommitted,
) error {
	if callbackStore == nil || callbackStore.boltStore == nil ||
		!committed.Valid() || committed.releases == nil ||
		committed.authority.entry == nil || claim.entry == nil {
		return errors.New("operation success requires a committed release proof")
	}
	if committed.callbacks != callbackStore || callbackStore.binding == nil ||
		committed.releases.binding == nil ||
		callbackStore.backendAuthorityGate == nil ||
		callbackStore.backendAuthorityGate != committed.releases.backendAuthorityGate ||
		callbackStore.binding.backendName != committed.releases.binding.backendName ||
		callbackStore.binding.storageID != committed.releases.binding.storageID {
		return errors.New("committed release proof belongs to another journal authority")
	}
	if err := requireOperationStoreLineage(committed.releases, committed.authority); err != nil {
		return err
	}
	if committed.authority.digest != claim.digest ||
		!operationIntentEntriesEqual(*committed.authority.entry, *claim.entry) {
		return errors.New("committed release proof belongs to another operation authority")
	}
	if committed.release.leaseUUID != claim.LeaseUUID() {
		return errors.New("committed release proof belongs to another lease")
	}
	derived, err := releaseForOperationAuthority(committed.authority)
	if err != nil {
		return err
	}
	derived.Version = committed.release.version
	encoded, err := json.Marshal(derived)
	if err != nil {
		return fmt.Errorf("encode committed release proof: %w", err)
	}
	if sha256.Sum256(encoded) != committed.release.digest {
		return errors.New("committed release proof digest does not match operation authority")
	}
	// Proofs are immutable and copyable, so validate durable liveness rather than
	// pretending Go can make them linear. This read occurs before the callback
	// journal's write transaction: release-store view -> callback-store update is
	// the sole cross-journal order, and no release mutation holds its transaction
	// while acquiring a callback delivery lock.
	return committed.releases.view(func(tx *bolt.Tx) error {
		history, err := readReleaseHistoryTx(tx, claim.LeaseUUID())
		if err != nil {
			return err
		}
		for index := range history {
			release := history[index]
			if release.Version != committed.release.version {
				continue
			}
			if release.Status != "active" || release.OperationID != claim.OperationID() {
				return errors.New("committed operation release is no longer the active generation")
			}
			stored, err := json.Marshal(release)
			if err != nil {
				return fmt.Errorf("encode current operation release proof: %w", err)
			}
			if sha256.Sum256(stored) != committed.release.digest {
				return errors.New("committed operation release changed after proof issuance")
			}
			return nil
		}
		return errors.New("committed operation release no longer exists")
	})
}

func validateOperationReleaseUncommitted(
	callbackStore *CallbackStore,
	claim OperationIntentClaim,
	uncommitted OperationReleaseUncommitted,
) error {
	if callbackStore == nil || callbackStore.boltStore == nil ||
		!uncommitted.Valid() || uncommitted.authority.entry == nil || claim.entry == nil {
		return errors.New("operation failure requires an uncommitted release proof")
	}
	if uncommitted.callbacks != callbackStore || callbackStore.binding == nil ||
		uncommitted.releases == nil || uncommitted.releases.binding == nil ||
		callbackStore.backendAuthorityGate == nil ||
		callbackStore.backendAuthorityGate != uncommitted.releases.backendAuthorityGate ||
		callbackStore.binding.backendName != uncommitted.releases.binding.backendName ||
		callbackStore.binding.storageID != uncommitted.releases.binding.storageID {
		return errors.New("uncommitted release proof belongs to another journal authority")
	}
	if err := requireOperationStoreLineage(uncommitted.releases, uncommitted.authority); err != nil {
		return err
	}
	if uncommitted.authority.digest != claim.digest ||
		!operationIntentEntriesEqual(*uncommitted.authority.entry, *claim.entry) {
		return errors.New("uncommitted release proof belongs to another operation authority")
	}
	return uncommitted.releases.requireOperationReleaseAbsent(uncommitted.authority)
}

func requireOperationStoreLineage(store *ReleaseStore, authority operationAuthority) error {
	if store == nil || store.boltStore == nil || store.binding == nil {
		return errors.New("operation release requires an identity-bound release journal")
	}
	if authority.entry == nil || !authority.storageID.Valid() {
		return errors.New("operation release authority is incomplete")
	}
	if authority.Backend() != store.binding.backendName ||
		authority.BackendStorageID() != store.binding.storageID {
		return errors.New("operation release authority belongs to another backend storage lineage")
	}
	return nil
}

func releaseForOperationAuthority(authority operationAuthority) (Release, error) {
	if authority.Kind() != OperationIntentProvision && authority.Kind() != OperationIntentRestore {
		return Release{}, fmt.Errorf("unsupported operation release kind %q", authority.Kind())
	}
	if authority.CreatedAt().IsZero() {
		return Release{}, errors.New("operation release requires a durable admission timestamp")
	}
	runtimeAuthority, err := NewReleaseRuntimeAuthority(
		authority.OperationID(),
		authority.Tenant(),
		authority.ProviderUUID(),
		authority.CallbackURL(),
		authority.LifecycleCallbackURL(),
	)
	if err != nil {
		return Release{}, fmt.Errorf("construct operation release runtime authority: %w", err)
	}
	release := Release{
		Manifest:         authority.Manifest(),
		Image:            "stack",
		OperationID:      authority.OperationID(),
		Items:            authority.EffectiveItems(),
		ResourceProfiles: authority.ResourceProfiles(),
		RuntimeAuthority: &runtimeAuthority,
		Status:           "active",
		CreatedAt:        authority.CreatedAt(),
	}
	if err := validateAppendRelease(release); err != nil {
		return Release{}, err
	}
	return cloneRelease(release), nil
}

func cloneOperationAuthority(authority operationAuthority) operationAuthority {
	if authority.entry == nil {
		return operationAuthority{}
	}
	entry := *authority.entry
	entry.Items = slices.Clone(entry.Items)
	entry.ResourceProfiles = CloneSKUResourceSnapshot(entry.ResourceProfiles)
	entry.EffectiveItems = slices.Clone(entry.EffectiveItems)
	entry.HealthCheckServices = slices.Clone(entry.HealthCheckServices)
	entry.Manifest = bytes.Clone(entry.Manifest)
	return operationAuthority{
		entry:     &entry,
		storageID: authority.storageID,
		digest:    authority.digest,
	}
}

// RestoreClaimCandidate is an exact, RetentionStore-bound capability for the
// Active -> Restoring transition of one pending restore. The security and
// causal fields are detached from caller-owned request buffers and cannot be
// replaced before the store consumes the candidate. Its zero value is invalid.
type RestoreClaimCandidate struct {
	callbacks  *CallbackStore
	releases   *ReleaseStore
	retentions *RetentionStore
	authority  operationAuthority
}

// RestoreSettlement is the exact three-journal boundary for claiming retained
// source state on behalf of a pending restore operation. It shares the
// destination operation gate with OperationSettlement while the retention
// store's transaction provides the independent source-generation CAS.
type RestoreSettlement struct {
	operations *OperationSettlement
	retentions *RetentionStore
}

// NewRestoreSettlement binds one operation pair to the exact retention journal
// in the same backend-storage lineage. The zero value is invalid.
func NewRestoreSettlement(
	operations *OperationSettlement,
	retentions *RetentionStore,
) (*RestoreSettlement, error) {
	if operations == nil || !operations.valid() ||
		!retentionStoreIsOpen(retentions) ||
		retentions.backendAuthorityGate != operations.callbacks.backendAuthorityGate ||
		retentions.binding.backendName != operations.callbacks.binding.backendName ||
		retentions.binding.storageID != operations.callbacks.binding.storageID {
		return nil, errors.New(
			"restore settlement requires exact identity-bound operation and retention journals",
		)
	}
	return &RestoreSettlement{operations: operations, retentions: retentions}, nil
}

func (s *RestoreSettlement) valid() bool {
	return s != nil && s.operations != nil && s.operations.valid() &&
		retentionStoreIsOpen(s.retentions) &&
		s.retentions.backendAuthorityGate == s.operations.callbacks.backendAuthorityGate &&
		s.retentions.binding.backendName == s.operations.callbacks.binding.backendName &&
		s.retentions.binding.storageID == s.operations.callbacks.binding.storageID
}

// PrepareRestoreClaim derives the complete retention transition from a sealed
// pending restore operation. It accepts no caller-spliced lease, token,
// callback, topology, sizing, or timestamp fields.
func (s *RestoreSettlement) PrepareRestoreClaim(
	claim OperationIntentClaim,
) (RestoreClaimCandidate, error) {
	if !s.valid() {
		return RestoreClaimCandidate{}, errors.New("restore settlement is invalid")
	}
	durable, err := requireOperationSettlementClaim(s.operations, claim)
	if err != nil {
		return RestoreClaimCandidate{}, err
	}
	if durable.Kind() != OperationIntentRestore {
		return RestoreClaimCandidate{}, errors.New("retention claim requires a restore operation")
	}
	unlock := s.operations.lockLease(durable.LeaseUUID())
	defer unlock()
	if err := s.operations.callbacks.requireCurrentOperationClaim(durable); err != nil {
		return RestoreClaimCandidate{}, err
	}
	if durable.Backend() != s.retentions.binding.backendName ||
		durable.BackendStorageID() != s.retentions.binding.storageID {
		return RestoreClaimCandidate{}, errors.New(
			"restore claim authority belongs to another backend storage lineage",
		)
	}
	if durable.SourceLeaseUUID() == "" || durable.LeaseUUID() == "" ||
		durable.SourceLeaseUUID() == durable.LeaseUUID() || durable.SourceGeneration() <= 0 {
		return RestoreClaimCandidate{}, errors.New("restore operation has invalid source authority")
	}
	return RestoreClaimCandidate{
		callbacks:  s.operations.callbacks,
		releases:   s.operations.releases,
		retentions: s.retentions,
		authority:  cloneOperationAuthority(durable.operationAuthority),
	}, nil
}

func validateRestoreClaimCandidate(
	settlement *RestoreSettlement,
	candidate RestoreClaimCandidate,
) error {
	if !settlement.valid() ||
		candidate.callbacks != settlement.operations.callbacks ||
		candidate.releases != settlement.operations.releases ||
		candidate.retentions != settlement.retentions {
		return errors.New("restore claim candidate belongs to another journal set")
	}
	durable, err := validateOperationIntentClaim(OperationIntentClaim{
		operationAuthority: candidate.authority,
	})
	if err != nil {
		return err
	}
	if durable.Kind() != OperationIntentRestore {
		return errors.New("retention claim requires a restore operation")
	}
	if settlement.retentions.binding == nil ||
		durable.Backend() != settlement.retentions.binding.backendName ||
		durable.BackendStorageID() != settlement.retentions.binding.storageID {
		return errors.New("restore claim authority belongs to another backend storage lineage")
	}
	return nil
}
