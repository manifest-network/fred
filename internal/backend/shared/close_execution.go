package shared

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

// CloseExecutionClaim is the irreversible-effect capability returned only
// after the close journal durably advances its monotonic execution generation.
// Copies of the earlier CloseIntentClaim become stale in that transaction.
type CloseExecutionClaim struct {
	settlement *CloseSettlement
	subject    ClosePhysicalSubject
	started    substratemutation.LiveExecution[ClosePhysicalSubject]
}

// CloseExecutionDestroyed and CloseExecutionRetained are the only terminal
// close outcomes. Their private fields can be minted only from an exact
// executor result; callers cannot choose retained/destroyed with a boolean.
type CloseExecutionDestroyed struct {
	settlement *CloseSettlement
	subject    ClosePhysicalSubject
	evidence   CloseDestroyed
}

type CloseExecutionRetained struct {
	settlement *CloseSettlement
	subject    ClosePhysicalSubject
	evidence   CloseRetained
}

// CloseExecutionPending deliberately grants no terminal authority. Refusal,
// ambiguous effects, incomplete strict inventory, and interrupted recovery all
// leave the durable close head as the sole retry owner.
type CloseExecutionPending struct {
	settlement *CloseSettlement
	subject    ClosePhysicalSubject
	cause      error
	retryable  bool
}

func (outcome CloseExecutionDestroyed) Valid() bool {
	return outcome.settlement != nil && outcome.subject.validFor(outcome.settlement) &&
		outcome.evidence.Valid() && outcome.evidence.validForClose(outcome.subject)
}

func (outcome CloseExecutionRetained) Valid() bool {
	return outcome.settlement != nil && outcome.subject.validFor(outcome.settlement) &&
		outcome.evidence.Valid() && outcome.evidence.validForClose(outcome.subject)
}

func (outcome CloseExecutionPending) Valid() bool {
	return outcome.settlement != nil && outcome.subject.validFor(outcome.settlement)
}

func (outcome CloseExecutionPending) Cause() error { return outcome.cause }

// RetryableNow reports that strict classification proved the current cohort
// incomplete (or the guard refused before entering a Step), so a new durable
// generation may safely retry work immediately. Ambiguous results return false
// and must wait for a later independent recovery classification.
func (outcome CloseExecutionPending) RetryableNow() bool { return outcome.retryable }
func (outcome CloseExecutionPending) Error() string {
	if outcome.cause == nil {
		return "close execution remains pending"
	}
	return outcome.cause.Error()
}

// CloseExecutionOutcome is a sealed sum. Only the two terminal variants also
// implement CloseTerminalOutcome and can cross CompleteClose.
type CloseExecutionOutcome interface{ closeExecutionOutcome() }

func (CloseExecutionDestroyed) closeExecutionOutcome() {}
func (CloseExecutionRetained) closeExecutionOutcome()  {}
func (CloseExecutionPending) closeExecutionOutcome()   {}

type CloseTerminalOutcome interface {
	CloseExecutionOutcome
	closeTerminalOutcome()
	LeaseUUID() string
	Tenant() string
	Items() []backend.LeaseItem
}

func (CloseExecutionDestroyed) closeTerminalOutcome() {}
func (CloseExecutionRetained) closeTerminalOutcome()  {}

func (outcome CloseExecutionDestroyed) LeaseUUID() string {
	return outcome.subject.Intent().LeaseUUID()
}
func (outcome CloseExecutionDestroyed) Tenant() string {
	return outcome.subject.Intent().Tenant()
}
func (outcome CloseExecutionDestroyed) Items() []backend.LeaseItem {
	return outcome.subject.Intent().Items()
}
func (outcome CloseExecutionRetained) LeaseUUID() string {
	return outcome.subject.Intent().LeaseUUID()
}
func (outcome CloseExecutionRetained) Tenant() string {
	return outcome.subject.Intent().Tenant()
}
func (outcome CloseExecutionRetained) Items() []backend.LeaseItem {
	return outcome.subject.Intent().Items()
}

// StartCloseExecution is the only transition into live physical close work.
// Incrementing the persisted generation is the durable Started boundary: no
// tenant Step can be entered until the exact refreshed claim and subject exist.
func (s *CloseSettlement) StartCloseExecution(
	claim CloseIntentClaim,
) (CloseExecutionClaim, error) {
	if err := s.requireClaim(claim); err != nil {
		return CloseExecutionClaim{}, err
	}
	if claim.ExecutionGeneration().Valid() {
		return CloseExecutionClaim{}, errors.New(
			"started close requires an independently attested retry capability",
		)
	}
	return s.startCloseGeneration(claim)
}

// RetryCloseExecution is the only transition from one Started generation to
// the next. Its input is minted only when the fixed classifier proves the
// previous generation incomplete, or when the previous live executor was
// refused before any tenant Step. An ambiguous live result therefore cannot
// be replayed in the same process merely by re-reading its durable claim.
func (s *CloseSettlement) RetryCloseExecution(
	pending CloseExecutionPending,
) (CloseExecutionClaim, error) {
	if !s.valid() || pending.settlement != s || !pending.Valid() || !pending.retryable {
		return CloseExecutionClaim{}, errors.New(
			"close retry requires executor-attested retryable evidence",
		)
	}
	return s.startCloseGeneration(pending.subject.state.claim)
}

func (s *CloseSettlement) startCloseGeneration(
	claim CloseIntentClaim,
) (CloseExecutionClaim, error) {
	if err := s.requireClaim(claim); err != nil {
		return CloseExecutionClaim{}, err
	}
	var subject ClosePhysicalSubject
	started, err := s.mutation.BeginAfter(func() (ClosePhysicalSubject, error) {
		unlock := s.lockLease(claim.LeaseUUID())
		defer unlock()
		refreshed, err := s.callbacks.advanceCloseExecutionGenerationLocked(claim)
		if err != nil {
			return ClosePhysicalSubject{}, err
		}
		refreshed.settlement = s
		subject = newClosePhysicalSubject(s, refreshed)
		return subject, nil
	})
	if err != nil {
		return CloseExecutionClaim{}, err
	}
	return CloseExecutionClaim{settlement: s, subject: subject, started: started}, nil
}

// ExecuteClose runs only the construction-bound physical workflow. A plain
// error, panic, or incomplete observation cannot terminalize the close.
func (s *CloseSettlement) ExecuteClose(
	ctx context.Context,
	execution CloseExecutionClaim,
) (outcome CloseExecutionOutcome) {
	if !s.valid() || s.execute == nil || ctx == nil || execution.settlement != s ||
		!execution.subject.validFor(s) {
		return CloseExecutionPending{
			settlement: s, subject: execution.subject,
			cause: errors.New("close execution boundary is invalid"),
		}
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			outcome = CloseExecutionPending{
				settlement: s, subject: execution.subject,
				cause: fmt.Errorf("close worker panic: %v\n%s", recovered, debug.Stack()),
			}
		}
	}()
	physical := s.execute(ctx, execution.started)
	if err := substratemutation.ValidateLiveResult(s.mutation, execution.started, physical); err != nil {
		return CloseExecutionPending{settlement: s, subject: execution.subject, cause: err}
	}
	return s.closeOutcomeForResult(execution.subject, physical)
}

func (s *CloseSettlement) closeOutcomeForResult(
	subject ClosePhysicalSubject,
	physical substratemutation.Result[ClosePhysicalSubject, ClosePhysicalEvidence],
) CloseExecutionOutcome {
	switch physical.Kind() {
	case substratemutation.Attested:
		evidence, ok := physical.Evidence()
		if !ok {
			return CloseExecutionPending{settlement: s, subject: subject,
				cause: errors.New("attested close result has no evidence")}
		}
		if err := validateClosePhysicalEvidence(subject, evidence); err != nil {
			return CloseExecutionPending{settlement: s, subject: subject, cause: err}
		}
		switch evidence.kind {
		case closePhysicalEvidenceDestroyed:
			return CloseExecutionDestroyed{settlement: s, subject: subject, evidence: evidence.destroyed}
		case closePhysicalEvidenceRetained:
			return CloseExecutionRetained{settlement: s, subject: subject, evidence: evidence.retained}
		case closePhysicalEvidenceIncomplete:
			return CloseExecutionPending{settlement: s, subject: subject,
				cause: errors.New("close substrate cleanup is incomplete"), retryable: true}
		default:
			return CloseExecutionPending{settlement: s, subject: subject,
				cause: errors.New("unknown close evidence")}
		}
	case substratemutation.Refused, substratemutation.Ambiguous:
		cause := physical.Err()
		if cause == nil {
			cause = errors.New("close mutation remains pending")
		}
		return CloseExecutionPending{
			settlement: s, subject: subject, cause: cause,
			retryable: physical.Kind() == substratemutation.Refused,
		}
	default:
		return CloseExecutionPending{settlement: s, subject: subject,
			cause: fmt.Errorf("invalid close mutation outcome %s", physical.Kind())}
	}
}

// RecoverCloseExecution performs a read-only strict classification of one
// exact persisted Started generation. It cannot replay live work and cannot
// choose its classifier. A close which never started must cross
// StartCloseExecution instead.
func (s *CloseSettlement) RecoverCloseExecution(
	ctx context.Context,
	scope LeaseRecoveryScope,
	claim CloseIntentClaim,
) (CloseExecutionOutcome, error) {
	if !s.valid() {
		return nil, errors.New("close recovery requires exact actor or lease-quiescence authority")
	}
	releaseScope, valid := scope.enter(s.recoveryCoordinator, claim.LeaseUUID())
	if !valid {
		return nil, errors.New("close recovery requires exact actor or lease-quiescence authority")
	}
	defer releaseScope()
	if s.recovery == nil {
		return nil, errors.New("close substrate recovery attestor is not bound")
	}
	if err := s.requireClaim(claim); err != nil {
		return nil, err
	}
	if !claim.ExecutionGeneration().Valid() {
		return nil, errors.New("close has not crossed the durable execution boundary")
	}
	var subject ClosePhysicalSubject
	recovered, err := s.mutation.RecoverAfter(func() (ClosePhysicalSubject, error) {
		unlock := s.lockLease(claim.LeaseUUID())
		defer unlock()
		if err := s.callbacks.requireCloseIntent(claim); err != nil {
			return ClosePhysicalSubject{}, err
		}
		subject = newClosePhysicalSubject(s, claim)
		return subject, nil
	})
	if err != nil {
		return nil, err
	}
	result := s.recovery.Inspect(recovered, ctx)
	if err := substratemutation.ValidateRecoveryResult(s.mutation, recovered, result); err != nil {
		return nil, err
	}
	return s.closeOutcomeForResult(subject, result), nil
}

// CompleteClose accepts only executor-minted terminal variants. It re-attests
// the exact close generation before touching release history, then validates
// the corresponding durable retention fact under the per-lease gate.
func (s *CloseSettlement) CompleteClose(
	outcome CloseTerminalOutcome,
) (CallbackEntry, error) {
	if s == nil || outcome == nil {
		return CallbackEntry{}, errors.New("close terminal outcome is invalid")
	}
	var subject ClosePhysicalSubject
	var completion closeCompletion
	var retention ActiveRetentionProof
	switch typed := outcome.(type) {
	case CloseExecutionDestroyed:
		if typed.settlement != s || !typed.Valid() {
			return CallbackEntry{}, errors.New("destroyed close outcome belongs to another settlement")
		}
		subject = typed.subject
		completion = closeCompletionDestroyed
	case CloseExecutionRetained:
		if typed.settlement != s || !typed.Valid() {
			return CallbackEntry{}, errors.New("retained close outcome belongs to another settlement")
		}
		subject = typed.subject
		completion = closeCompletionRetained
		retention = typed.evidence.state.retention
	default:
		return CallbackEntry{}, fmt.Errorf("unknown close terminal outcome %T", outcome)
	}
	return s.completeTerminal(subject.state.claim, completion, retention)
}

func (s *CloseSettlement) completeTerminal(
	claim CloseIntentClaim,
	outcome closeCompletion,
	retention ActiveRetentionProof,
) (CallbackEntry, error) {
	if err := s.requireClaim(claim); err != nil {
		return CallbackEntry{}, err
	}
	unlock := s.lockLease(claim.LeaseUUID())
	defer unlock()

	// This check precedes release retirement. A stale execution generation must
	// never delete the still-live release and fail only at the later callback CAS.
	if err := s.callbacks.requireCloseIntent(claim); err != nil {
		return CallbackEntry{}, err
	}

	s.retentions.mu.RLock()
	defer s.retentions.mu.RUnlock()
	switch outcome {
	case closeCompletionRetained:
		if !claim.RetainOnClose() || claim.CleanupOnly() {
			return CallbackEntry{}, errors.New("retained completion requires a retained projected close")
		}
		if err := s.retentions.requireActiveProofLocked(retention); err != nil {
			return CallbackEntry{}, err
		}
		if err := retentionMatchesClose(retention, claim); err != nil {
			return CallbackEntry{}, err
		}
	case closeCompletionDestroyed:
		if err := s.requireNoActiveRetentionLocked(claim.LeaseUUID()); err != nil {
			return CallbackEntry{}, err
		}
	default:
		return CallbackEntry{}, errors.New("invalid private close terminal completion")
	}

	if err := s.releases.deleteCloseHistory(claim); err != nil {
		return CallbackEntry{}, err
	}
	return s.callbacks.resolveCloseIntentLocked(claim, outcome, "")
}
