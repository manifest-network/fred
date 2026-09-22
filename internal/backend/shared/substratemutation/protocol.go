package substratemutation

import (
	"errors"
	"fmt"
	"sync/atomic"
)

type Kind uint8

const (
	Invalid Kind = iota
	Refused      // no physical Step was entered
	Attested
	Ambiguous
)

func (k Kind) String() string {
	switch k {
	case Refused:
		return "refused"
	case Attested:
		return "attested"
	case Ambiguous:
		return "ambiguous"
	default:
		return "invalid"
	}
}

// Non-zero-sized lineages have stable distinct addresses; pointers to distinct
// zero-sized allocations are permitted to compare equal in Go.
type lineage struct{ _ byte }
type executorLineage struct{ _ byte }
type executionLineage struct {
	_        byte
	consumed atomic.Bool
}
type liveExecutionMarker struct{ _ byte }
type recoveryExecutionMarker struct{ _ byte }

// Protocol owns one durable settlement coordinator's exact subject type and
// physical executor lineage.
type Protocol[Subject comparable] struct {
	lineage  *lineage
	executor atomic.Pointer[executorLineage]
}

func NewProtocol[Subject comparable]() *Protocol[Subject] {
	return &Protocol[Subject]{lineage: &lineage{}}
}

// GuardBinding is an opaque, one-shot request to bind the exact live executor
// and recovery attestor as one construction transaction.
type GuardBinding[Subject comparable] struct{ protocol *Protocol[Subject] }

func (p *Protocol[Subject]) NewGuardBinding() (GuardBinding[Subject], error) {
	if p == nil || p.lineage == nil {
		return GuardBinding[Subject]{}, errors.New("substrate mutation protocol is not initialized")
	}
	if p.executor.Load() != nil {
		return GuardBinding[Subject]{}, errors.New("substrate mutation protocol already has a bound executor")
	}
	return GuardBinding[Subject]{protocol: p}, nil
}

type executionProof struct {
	lineage   *lineage
	executor  *executorLineage
	execution *executionLineage
}

// LiveExecution combines exact durable Started authority with the Subject
// returned by that same CAS callback. Guard.Execute takes no independent
// Subject, making A-authority/B-subject mutation unrepresentable.
type LiveExecution[Subject comparable] struct {
	proof   executionProof
	live    *liveExecutionMarker
	subject Subject
}

// RecoveryExecution combines recovered Started authority with the Subject
// returned by that same exact durable re-read callback.
type RecoveryExecution[Subject comparable] struct {
	proof    executionProof
	recovery *recoveryExecutionMarker
	subject  Subject
}

// Result is unconstructable outside this package. An Attested result carries
// exhaustive domain evidence and the exact execution-bound Subject.
type Result[Subject comparable, Evidence any] struct {
	lineage   *lineage
	executor  *executorLineage
	execution *executionLineage
	subject   Subject
	kind      Kind
	evidence  Evidence
	hasProof  bool
	err       error
}

func (r Result[Subject, Evidence]) Kind() Kind { return r.kind }
func (r Result[Subject, Evidence]) Err() error { return r.err }
func (r Result[Subject, Evidence]) Evidence() (Evidence, bool) {
	return r.evidence, r.hasProof
}

func (p executionProof) valid() bool {
	return p.lineage != nil && p.executor != nil && p.execution != nil
}

func (p executionProof) consume() error {
	if !p.valid() {
		return errors.New("physical execution authority is invalid")
	}
	if !p.execution.consumed.CompareAndSwap(false, true) {
		return errors.New("physical execution authority was already consumed")
	}
	return nil
}

func resultFor[Subject comparable, Evidence any](
	proof executionProof,
	subject Subject,
	kind Kind,
	evidence Evidence,
	hasProof bool,
	err error,
) Result[Subject, Evidence] {
	return Result[Subject, Evidence]{
		lineage: proof.lineage, executor: proof.executor, execution: proof.execution,
		subject: subject, kind: kind, evidence: evidence, hasProof: hasProof, err: err,
	}
}

func refusedResult[Subject comparable, Evidence any](proof executionProof, subject Subject, err error) Result[Subject, Evidence] {
	return resultFor(proof, subject, Refused, *new(Evidence), false, err)
}

func attestedResult[Subject comparable, Evidence any](proof executionProof, subject Subject, evidence Evidence) Result[Subject, Evidence] {
	return resultFor(proof, subject, Attested, evidence, true, nil)
}

func ambiguousResult[Subject comparable, Evidence any](proof executionProof, subject Subject, err error) Result[Subject, Evidence] {
	if err == nil {
		err = errors.New("physical mutation outcome is ambiguous")
	}
	return resultFor(proof, subject, Ambiguous, *new(Evidence), false, err)
}

func invalidResult[Subject comparable, Evidence any](err error) Result[Subject, Evidence] {
	return Result[Subject, Evidence]{kind: Invalid, err: err}
}

// BeginAfter mints a LiveExecution only from the exact Subject returned by the
// durable NotStarted-to-Started CAS callback.
func (p *Protocol[Subject]) BeginAfter(
	commitStartedCAS func() (Subject, error),
) (LiveExecution[Subject], error) {
	if p == nil || p.lineage == nil || p.executor.Load() == nil {
		return LiveExecution[Subject]{}, errors.New("substrate mutation protocol is not bound")
	}
	if commitStartedCAS == nil {
		return LiveExecution[Subject]{}, errors.New("durable Started transition is required")
	}
	subject, err := commitStartedCAS()
	if err != nil {
		return LiveExecution[Subject]{}, err
	}
	return LiveExecution[Subject]{
		proof: executionProof{
			lineage: p.lineage, executor: p.executor.Load(), execution: &executionLineage{},
		},
		live: &liveExecutionMarker{}, subject: subject,
	}, nil
}

// RecoverAfter mints a RecoveryExecution only from the exact Subject returned
// by the durable Started re-read callback.
func (p *Protocol[Subject]) RecoverAfter(
	validateDurableStarted func() (Subject, error),
) (RecoveryExecution[Subject], error) {
	if p == nil || p.lineage == nil || p.executor.Load() == nil {
		return RecoveryExecution[Subject]{}, errors.New("substrate mutation protocol is not bound")
	}
	if validateDurableStarted == nil {
		return RecoveryExecution[Subject]{}, errors.New("durable Started validation is required")
	}
	subject, err := validateDurableStarted()
	if err != nil {
		return RecoveryExecution[Subject]{}, err
	}
	return RecoveryExecution[Subject]{
		proof: executionProof{
			lineage: p.lineage, executor: p.executor.Load(), execution: &executionLineage{},
		},
		recovery: &recoveryExecutionMarker{}, subject: subject,
	}, nil
}

func validateResultFor[Subject comparable, Evidence any](
	p *Protocol[Subject],
	proof executionProof,
	subject Subject,
	result Result[Subject, Evidence],
) error {
	if p == nil || p.lineage == nil {
		return errors.New("substrate mutation protocol is not bound")
	}
	if !proof.valid() || proof.lineage != p.lineage || proof.executor != p.executor.Load() {
		return errors.New("physical execution authority belongs to another protocol")
	}
	if result.lineage == nil || result.lineage != p.lineage ||
		result.executor == nil || result.executor != p.executor.Load() ||
		result.execution == nil || result.execution != proof.execution ||
		result.subject != subject {
		return errors.New("substrate mutation result belongs to another execution")
	}
	switch result.kind {
	case Refused, Ambiguous:
		if result.hasProof {
			return errors.New("non-attested substrate result carries evidence")
		}
		return nil
	case Attested:
		if !result.hasProof {
			return errors.New("attested substrate result has no evidence")
		}
		return nil
	default:
		return fmt.Errorf("invalid substrate mutation result kind %d", result.kind)
	}
}

func ValidateLiveResult[Subject comparable, Evidence any](
	p *Protocol[Subject],
	execution LiveExecution[Subject],
	result Result[Subject, Evidence],
) error {
	return validateResultFor(p, execution.proof, execution.subject, result)
}

func ValidateRecoveryResult[Subject comparable, Evidence any](
	p *Protocol[Subject],
	execution RecoveryExecution[Subject],
	result Result[Subject, Evidence],
) error {
	return validateResultFor(p, execution.proof, execution.subject, result)
}
