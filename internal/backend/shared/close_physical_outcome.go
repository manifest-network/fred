package shared

import "errors"

// ClosePhysicalSubject is the immutable physical identity returned by the
// same durable CAS which advances a close execution generation. Its pointer
// representation is comparable while all authority-bearing state remains
// private. The zero value is invalid.
type ClosePhysicalSubject struct {
	state *closePhysicalSubjectState
}

type closePhysicalSubjectState struct {
	settlement *CloseSettlement
	claim      CloseIntentClaim
}

func newClosePhysicalSubject(
	settlement *CloseSettlement,
	claim CloseIntentClaim,
) ClosePhysicalSubject {
	return ClosePhysicalSubject{state: &closePhysicalSubjectState{
		settlement: settlement,
		claim:      cloneCloseMutationClaim(claim),
	}}
}

func (subject ClosePhysicalSubject) validFor(settlement *CloseSettlement) bool {
	return subject.state != nil && subject.state.settlement == settlement &&
		settlement != nil && subject.state.claim.settlement == settlement &&
		subject.state.claim.ExecutionGeneration().Valid() &&
		validateCloseIntentClaim(subject.state.claim) == nil
}

// Valid reports whether this value has complete journal-minted authority. It
// does not re-read durable state; Start/Recover and terminal settlement perform
// the exact generation re-attestation at their transition boundaries.
func (subject ClosePhysicalSubject) Valid() bool {
	return subject.state != nil && subject.validFor(subject.state.settlement)
}

func (subject ClosePhysicalSubject) LeaseUUID() string {
	if !subject.Valid() {
		return ""
	}
	return subject.state.claim.LeaseUUID()
}

func (subject ClosePhysicalSubject) IntentID() string {
	if !subject.Valid() {
		return ""
	}
	return subject.state.claim.IntentID()
}

// ExecutionGeneration is the exact monotonic generation which crossed the
// durable Started boundary before this subject was minted.
func (subject ClosePhysicalSubject) ExecutionGeneration() CloseExecutionGeneration {
	if !subject.Valid() {
		return CloseExecutionGeneration{}
	}
	return subject.state.claim.ExecutionGeneration()
}

// Intent returns detached observation and routing authority for the exact
// close. It cannot itself start, execute, or settle physical work.
func (subject ClosePhysicalSubject) Intent() CloseIntentClaim {
	if !subject.Valid() {
		return CloseIntentClaim{}
	}
	return cloneCloseMutationClaim(subject.state.claim)
}

// ClosePhysicalEvidence is the closed exhaustive physical classification for
// one exact durable close execution. Only the construction-bound classifier can
// place one of these values inside an executor-minted result accepted by close
// settlement.
type closePhysicalEvidenceKind uint8

const (
	closePhysicalEvidenceInvalid closePhysicalEvidenceKind = iota
	closePhysicalEvidenceDestroyed
	closePhysicalEvidenceRetained
	closePhysicalEvidenceIncomplete
)

// ClosePhysicalEvidence is an opaque closed value algebra. Its private tag and
// concrete payloads make mixed and typed-nil classifications unrepresentable.
// The zero value is invalid.
type ClosePhysicalEvidence struct {
	kind       closePhysicalEvidenceKind
	destroyed  CloseDestroyed
	retained   CloseRetained
	incomplete CloseIncomplete
}

type closeDestroyedState struct{ subject ClosePhysicalSubject }

// CloseDestroyed attests that the exact close cohort, its canonical volumes,
// and any retained-volume cohort are absent. Durable retention absence is
// independently re-attested by terminal settlement.
type CloseDestroyed struct{ state *closeDestroyedState }

func (e CloseDestroyed) validForClose(subject ClosePhysicalSubject) bool {
	return e.state != nil && e.state.subject == subject && subject.Valid()
}
func (e CloseDestroyed) Valid() bool {
	return e.state != nil && e.validForClose(e.state.subject)
}

func NewCloseDestroyed(subject ClosePhysicalSubject) (ClosePhysicalEvidence, error) {
	if !subject.Valid() {
		return ClosePhysicalEvidence{}, errors.New("close physical subject is invalid")
	}
	destroyed := CloseDestroyed{state: &closeDestroyedState{subject: subject}}
	return ClosePhysicalEvidence{
		kind: closePhysicalEvidenceDestroyed, destroyed: destroyed,
	}, nil
}

type closeRetainedState struct {
	subject   ClosePhysicalSubject
	retention ActiveRetentionProof
}

// CloseRetained attests that the exact workload and canonical volumes are
// absent and every volume named by the exact Active retention generation is
// present. Terminal settlement re-reads that durable retention generation.
type CloseRetained struct{ state *closeRetainedState }

func (e CloseRetained) validForClose(subject ClosePhysicalSubject) bool {
	return e.state != nil && e.state.subject == subject && subject.Valid() &&
		e.state.retention.Valid() &&
		retentionMatchesClose(e.state.retention, subject.state.claim) == nil
}
func (e CloseRetained) Valid() bool {
	return e.state != nil && e.validForClose(e.state.subject)
}

func NewCloseRetained(
	subject ClosePhysicalSubject,
	retention ActiveRetentionProof,
) (ClosePhysicalEvidence, error) {
	if !subject.Valid() || !subject.state.claim.RetainOnClose() ||
		subject.state.claim.CleanupOnly() {
		return ClosePhysicalEvidence{}, errors.New(
			"retained close evidence requires a retaining projected close",
		)
	}
	if !retention.Valid() {
		return ClosePhysicalEvidence{}, errors.New("retained close evidence requires active retention authority")
	}
	if err := retentionMatchesClose(retention, subject.state.claim); err != nil {
		return ClosePhysicalEvidence{}, err
	}
	retained := CloseRetained{state: &closeRetainedState{
		subject: subject, retention: retention,
	}}
	return ClosePhysicalEvidence{
		kind: closePhysicalEvidenceRetained, retained: retained,
	}, nil
}

type closeIncompleteState struct{ subject ClosePhysicalSubject }

// CloseIncomplete is the nonterminal exhaustive classification: some exact
// cleanup target still survives, or a retaining close does not yet have a
// complete retained cohort. It carries no settlement authority.
type CloseIncomplete struct{ state *closeIncompleteState }

func (e CloseIncomplete) validForClose(subject ClosePhysicalSubject) bool {
	return e.state != nil && e.state.subject == subject && subject.Valid()
}
func (e CloseIncomplete) Valid() bool {
	return e.state != nil && e.validForClose(e.state.subject)
}

func NewCloseIncomplete(subject ClosePhysicalSubject) (ClosePhysicalEvidence, error) {
	if !subject.Valid() {
		return ClosePhysicalEvidence{}, errors.New("close physical subject is invalid")
	}
	incomplete := CloseIncomplete{state: &closeIncompleteState{subject: subject}}
	return ClosePhysicalEvidence{
		kind: closePhysicalEvidenceIncomplete, incomplete: incomplete,
	}, nil
}

func validateClosePhysicalEvidence(
	subject ClosePhysicalSubject,
	evidence ClosePhysicalEvidence,
) error {
	switch evidence.kind {
	case closePhysicalEvidenceDestroyed:
		if evidence.destroyed.validForClose(subject) {
			return nil
		}
	case closePhysicalEvidenceRetained:
		if evidence.retained.validForClose(subject) {
			return nil
		}
	case closePhysicalEvidenceIncomplete:
		if evidence.incomplete.validForClose(subject) {
			return nil
		}
	default:
		return errors.New("unknown close physical evidence")
	}
	return errors.New("close physical evidence does not match its durable subject")
}
