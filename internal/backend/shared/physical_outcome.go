package shared

import (
	"errors"
	"slices"
)

// OperationPhysicalSubject is the immutable substrate identity returned by the
// same durable transaction which advances an operation to Started. Its pointer
// backed representation makes it comparable without exposing caller-writable
// authority. The zero value is invalid.
type OperationPhysicalSubject struct {
	state *operationPhysicalSubjectState
}

type operationPhysicalSubjectMode uint8

const (
	operationPhysicalExecution operationPhysicalSubjectMode = iota + 1
	operationPhysicalRecoveryCleanup
	operationPhysicalFailedReceiptCleanup
)

type operationPhysicalSubjectState struct {
	settlement  *OperationSettlement
	candidate   OperationReleaseCandidate
	predecessor *Release
	receipt     FailedOperationReceipt
	mode        operationPhysicalSubjectMode
}

func newOperationPhysicalSubject(
	settlement *OperationSettlement,
	candidate OperationReleaseCandidate,
	predecessor *Release,
) OperationPhysicalSubject {
	return newOperationPhysicalSubjectForMode(
		settlement, candidate, predecessor, operationPhysicalExecution,
	)
}

func newOperationPhysicalSubjectForMode(
	settlement *OperationSettlement,
	candidate OperationReleaseCandidate,
	predecessor *Release,
	mode operationPhysicalSubjectMode,
) OperationPhysicalSubject {
	var previous *Release
	if predecessor != nil {
		cloned := cloneRelease(*predecessor)
		previous = &cloned
	}
	return OperationPhysicalSubject{state: &operationPhysicalSubjectState{
		settlement:  settlement,
		candidate:   candidate,
		predecessor: previous,
		mode:        mode,
	}}
}

func newFailedOperationCleanupSubject(
	settlement *OperationSettlement,
	receipt FailedOperationReceipt,
) OperationPhysicalSubject {
	return OperationPhysicalSubject{state: &operationPhysicalSubjectState{
		settlement: settlement,
		receipt:    receipt,
		mode:       operationPhysicalFailedReceiptCleanup,
	}}
}

func (subject OperationPhysicalSubject) validFor(settlement *OperationSettlement) bool {
	if subject.state == nil || subject.state.settlement != settlement {
		return false
	}
	if subject.state.mode == operationPhysicalFailedReceiptCleanup {
		return settlement.ownsFailedOperationReceipt(subject.state.receipt)
	}
	return (subject.state.mode == operationPhysicalExecution ||
		subject.state.mode == operationPhysicalRecoveryCleanup) &&
		settlement.ownsOperationCandidate(subject.state.candidate) &&
		subject.state.candidate.authority.entry != nil &&
		!subject.state.candidate.authority.entry.EffectNotStarted
}

func (subject OperationPhysicalSubject) matchesCandidate(candidate OperationReleaseCandidate) bool {
	return subject.Valid() && subject.state.candidate.settlement == candidate.settlement &&
		subject.state.candidate.callbacks == candidate.callbacks &&
		subject.state.candidate.releases == candidate.releases &&
		subject.state.candidate.authority.digest == candidate.authority.digest &&
		operationIntentEntriesEqual(
			*subject.state.candidate.authority.entry,
			*candidate.authority.entry,
		)
}

func (subject OperationPhysicalSubject) Valid() bool {
	return subject.state != nil && subject.validFor(subject.state.settlement)
}

func (subject OperationPhysicalSubject) LeaseUUID() string {
	if !subject.Valid() {
		return ""
	}
	if subject.state.mode == operationPhysicalFailedReceiptCleanup {
		return subject.state.receipt.LeaseUUID()
	}
	return subject.state.candidate.authority.LeaseUUID()
}

func (subject OperationPhysicalSubject) OperationID() OperationID {
	if !subject.Valid() {
		return OperationID{}
	}
	if subject.state.mode == operationPhysicalFailedReceiptCleanup {
		return subject.state.receipt.OperationID()
	}
	return subject.state.candidate.authority.OperationID()
}

func (subject OperationPhysicalSubject) Intent() OperationIntentClaim {
	if !subject.Valid() || subject.state.mode == operationPhysicalFailedReceiptCleanup {
		return OperationIntentClaim{}
	}
	return subject.state.candidate.Intent()
}

// RecoveryCleanup reports whether this unforgeable subject was minted for the
// construction-bound destructive recovery workflow rather than ordinary live
// execution or read-only recovery classification. Callers cannot toggle the
// bit or replace its candidate.
func (subject OperationPhysicalSubject) RecoveryCleanup() bool {
	return subject.Valid() && subject.state.mode == operationPhysicalRecoveryCleanup
}

// FailedReceiptCleanup identifies the permanent late-arrival cleanup mode and
// returns its exact sealed receipt. It cannot be set on a live operation
// subject or substituted after construction.
func (subject OperationPhysicalSubject) FailedReceiptCleanup() (FailedOperationReceipt, bool) {
	if !subject.Valid() || subject.state.mode != operationPhysicalFailedReceiptCleanup {
		return FailedOperationReceipt{}, false
	}
	return subject.state.receipt, true
}

// ExpectedRelease returns the detached release topology which strict inventory
// must classify. It is observation only and carries no commit authority.
func (subject OperationPhysicalSubject) ExpectedRelease() (Release, bool) {
	if !subject.Valid() || subject.state.mode == operationPhysicalFailedReceiptCleanup {
		return Release{}, false
	}
	release, err := releaseForOperationAuthority(subject.state.candidate.authority)
	return release, err == nil
}

// PredecessorRelease returns the exact active generation observed while the
// operation crossed Started. It is present only for an identity-preserving
// provision repair and lets the subject-bound facade accept that predecessor's
// containers without accepting an arbitrary same-lease generation.
func (subject OperationPhysicalSubject) PredecessorRelease() (Release, bool) {
	if !subject.Valid() || subject.state.mode == operationPhysicalFailedReceiptCleanup ||
		subject.state.predecessor == nil {
		return Release{}, false
	}
	return cloneRelease(*subject.state.predecessor), true
}

// OperationPhysicalEvidence is the closed exhaustive inventory result for one
// exact operation subject. Only the construction-bound classifier can place it
// inside a settlement-accepted substrate result.
type operationPhysicalEvidenceKind uint8

const (
	operationPhysicalEvidenceInvalid operationPhysicalEvidenceKind = iota
	operationPhysicalEvidenceTargetReady
	operationPhysicalEvidenceExactAbsent
	operationPhysicalEvidenceFailedReceiptAbsent
)

// OperationPhysicalEvidence is an opaque closed value algebra. Keeping its tag
// and every payload private makes invalid combinations, interface injection,
// and typed-nil variants unconstructable while the public payload types remain
// available on terminal projections. The zero value is invalid.
type OperationPhysicalEvidence struct {
	kind                operationPhysicalEvidenceKind
	targetReady         OperationTargetReady
	exactAbsent         OperationExactAbsent
	failedReceiptAbsent OperationFailedReceiptAbsent
}

type operationTargetReadyState struct {
	subject           OperationPhysicalSubject
	containerIDs      []string
	serviceContainers map[string][]string
	committedRelease  *Release
}

// OperationTargetReady is an exact, complete target-cohort observation.
type OperationTargetReady struct{ state *operationTargetReadyState }

func (e OperationTargetReady) validForOperation(subject OperationPhysicalSubject) bool {
	if e.state == nil || e.state.subject != subject || !subject.Valid() {
		return false
	}
	release, ok := subject.ExpectedRelease()
	return ok && validReadyProjectionForRelease(
		release, e.state.containerIDs, e.state.serviceContainers,
	)
}
func (e OperationTargetReady) Valid() bool {
	return e.state != nil && e.validForOperation(e.state.subject)
}
func (e OperationTargetReady) LeaseUUID() string {
	if !e.Valid() {
		return ""
	}
	return e.state.subject.LeaseUUID()
}
func (e OperationTargetReady) OperationID() OperationID {
	if !e.Valid() {
		return OperationID{}
	}
	return e.state.subject.OperationID()
}
func (e OperationTargetReady) Projection() (Release, []string, map[string][]string) {
	if !e.Valid() {
		return Release{}, nil, nil
	}
	release, _ := e.state.subject.ExpectedRelease()
	if e.state.committedRelease != nil {
		release = cloneRelease(*e.state.committedRelease)
	}
	return release, slices.Clone(e.state.containerIDs), clonePhysicalProjection(e.state.serviceContainers)
}

func (e OperationTargetReady) withCommittedRelease(release Release) OperationTargetReady {
	if !e.Valid() || release.Version <= 0 {
		return OperationTargetReady{}
	}
	copy := cloneRelease(release)
	return OperationTargetReady{state: &operationTargetReadyState{
		subject: e.state.subject, containerIDs: slices.Clone(e.state.containerIDs),
		serviceContainers: clonePhysicalProjection(e.state.serviceContainers),
		committedRelease:  &copy,
	}}
}

// NewOperationTargetReady seals a complete projection for the exact subject.
// Exhaustive physical classification remains construction-bound to the Guard;
// this constructor only makes the resulting value immutable and lineage-bound.
func NewOperationTargetReady(
	subject OperationPhysicalSubject,
	containerIDs []string,
	serviceContainers map[string][]string,
) (OperationPhysicalEvidence, error) {
	if !subject.Valid() {
		return OperationPhysicalEvidence{}, errors.New("operation physical subject is invalid")
	}
	release, ok := subject.ExpectedRelease()
	if !ok || !validReadyProjectionForRelease(release, containerIDs, serviceContainers) {
		return OperationPhysicalEvidence{}, errors.New("operation target projection is incomplete")
	}
	ready := OperationTargetReady{state: &operationTargetReadyState{
		subject: subject, containerIDs: slices.Clone(containerIDs),
		serviceContainers: clonePhysicalProjection(serviceContainers),
	}}
	return OperationPhysicalEvidence{
		kind: operationPhysicalEvidenceTargetReady, targetReady: ready,
	}, nil
}

type operationExactAbsentState struct{ subject OperationPhysicalSubject }

// OperationExactAbsent is strict evidence that the exact operation subject has
// no surviving substrate cohort. It deliberately exposes no projection.
type OperationExactAbsent struct{ state *operationExactAbsentState }

func (e OperationExactAbsent) validForOperation(subject OperationPhysicalSubject) bool {
	_, historical := subject.FailedReceiptCleanup()
	return !historical && e.state != nil && e.state.subject == subject && subject.Valid()
}

type operationFailedReceiptAbsentState struct{ subject OperationPhysicalSubject }

// OperationFailedReceiptAbsent is the terminal postcondition for removing a
// late container owned by one permanent failed-operation receipt. It is
// deliberately distinct from OperationExactAbsent: it cannot fail or settle a
// current intent and says nothing about a legitimate successor on the lease.
type OperationFailedReceiptAbsent struct {
	state *operationFailedReceiptAbsentState
}

func (e OperationFailedReceiptAbsent) validForOperation(subject OperationPhysicalSubject) bool {
	_, historical := subject.FailedReceiptCleanup()
	return historical && e.state != nil && e.state.subject == subject && subject.Valid()
}
func (e OperationFailedReceiptAbsent) Valid() bool {
	return e.state != nil && e.validForOperation(e.state.subject)
}

func NewOperationFailedReceiptAbsent(
	subject OperationPhysicalSubject,
) (OperationPhysicalEvidence, error) {
	if _, ok := subject.FailedReceiptCleanup(); !ok {
		return OperationPhysicalEvidence{}, errors.New("failed-operation cleanup subject is invalid")
	}
	absent := OperationFailedReceiptAbsent{
		state: &operationFailedReceiptAbsentState{subject: subject},
	}
	return OperationPhysicalEvidence{
		kind: operationPhysicalEvidenceFailedReceiptAbsent, failedReceiptAbsent: absent,
	}, nil
}
func (e OperationExactAbsent) Valid() bool {
	return e.state != nil && e.validForOperation(e.state.subject)
}
func (e OperationExactAbsent) LeaseUUID() string {
	if !e.Valid() {
		return ""
	}
	return e.state.subject.LeaseUUID()
}
func (e OperationExactAbsent) OperationID() OperationID {
	if !e.Valid() {
		return OperationID{}
	}
	return e.state.subject.OperationID()
}

func NewOperationExactAbsent(subject OperationPhysicalSubject) (OperationPhysicalEvidence, error) {
	if !subject.Valid() {
		return OperationPhysicalEvidence{}, errors.New("operation physical subject is invalid")
	}
	absent := OperationExactAbsent{state: &operationExactAbsentState{subject: subject}}
	return OperationPhysicalEvidence{
		kind: operationPhysicalEvidenceExactAbsent, exactAbsent: absent,
	}, nil
}

// MaintenancePhysicalSubject is the exact target and source authority returned
// by the transaction which advances one maintenance intent to Started.
type MaintenancePhysicalSubject struct {
	state *maintenancePhysicalSubjectState
}

type maintenancePhysicalSubjectMode uint8

const (
	maintenancePhysicalExecution maintenancePhysicalSubjectMode = iota + 1
	maintenancePhysicalRecoveryCleanup
	maintenancePhysicalFailedReceiptCleanup
)

type maintenancePhysicalSubjectState struct {
	settlement *MaintenanceSettlement
	target     MaintenanceReleaseClaim
	source     MaintenanceSourceSnapshot
	receipt    FailedMaintenanceReceipt
	mode       maintenancePhysicalSubjectMode
}

func newMaintenancePhysicalSubject(
	settlement *MaintenanceSettlement,
	target MaintenanceReleaseClaim,
	source MaintenanceSourceSnapshot,
) MaintenancePhysicalSubject {
	return newMaintenancePhysicalSubjectForMode(settlement, target, source, false)
}

func newMaintenancePhysicalSubjectForMode(
	settlement *MaintenanceSettlement,
	target MaintenanceReleaseClaim,
	source MaintenanceSourceSnapshot,
	recoveryCleanup bool,
) MaintenancePhysicalSubject {
	return MaintenancePhysicalSubject{state: &maintenancePhysicalSubjectState{
		settlement: settlement, target: target, source: source,
		mode: func() maintenancePhysicalSubjectMode {
			if recoveryCleanup {
				return maintenancePhysicalRecoveryCleanup
			}
			return maintenancePhysicalExecution
		}(),
	}}
}

func newFailedMaintenanceCleanupSubject(
	settlement *MaintenanceSettlement,
	receipt FailedMaintenanceReceipt,
) MaintenancePhysicalSubject {
	return MaintenancePhysicalSubject{state: &maintenancePhysicalSubjectState{
		settlement: settlement, receipt: receipt,
		mode: maintenancePhysicalFailedReceiptCleanup,
	}}
}

func (subject MaintenancePhysicalSubject) validFor(settlement *MaintenanceSettlement) bool {
	if subject.state == nil || subject.state.settlement != settlement {
		return false
	}
	if subject.state.mode == maintenancePhysicalFailedReceiptCleanup {
		return settlement.ownsFailedMaintenanceReceipt(subject.state.receipt)
	}
	return (subject.state.mode == maintenancePhysicalExecution ||
		subject.state.mode == maintenancePhysicalRecoveryCleanup) &&
		subject.state.target.validFor(settlement) &&
		subject.state.source.Valid() &&
		subject.state.source.settlement == settlement &&
		subject.state.target.LeaseUUID() == subject.state.source.LeaseUUID() &&
		subject.state.target.MaintenanceID() == subject.state.source.MaintenanceID() &&
		!subject.state.target.intent.entry.EffectNotStarted
}

func (subject MaintenancePhysicalSubject) Valid() bool {
	return subject.state != nil && subject.validFor(subject.state.settlement)
}
func (subject MaintenancePhysicalSubject) LeaseUUID() string {
	if !subject.Valid() {
		return ""
	}
	if subject.state.mode == maintenancePhysicalFailedReceiptCleanup {
		return subject.state.receipt.LeaseUUID()
	}
	return subject.state.target.LeaseUUID()
}
func (subject MaintenancePhysicalSubject) MaintenanceID() MaintenanceID {
	if !subject.Valid() {
		return MaintenanceID{}
	}
	if subject.state.mode == maintenancePhysicalFailedReceiptCleanup {
		return subject.state.receipt.MaintenanceID()
	}
	return subject.state.target.MaintenanceID()
}
func (subject MaintenancePhysicalSubject) Intent() MaintenanceIntentClaim {
	if !subject.Valid() || subject.state.mode == maintenancePhysicalFailedReceiptCleanup {
		return MaintenanceIntentClaim{}
	}
	return cloneMaintenanceIntentClaim(subject.state.target.intent)
}

// RecoveryCleanup distinguishes the construction-bound destructive recovery
// handler from ordinary maintenance execution and read-only recovery.
func (subject MaintenancePhysicalSubject) RecoveryCleanup() bool {
	return subject.Valid() && subject.state.mode == maintenancePhysicalRecoveryCleanup
}

func (subject MaintenancePhysicalSubject) FailedReceiptCleanup() (FailedMaintenanceReceipt, bool) {
	if !subject.Valid() || subject.state.mode != maintenancePhysicalFailedReceiptCleanup {
		return FailedMaintenanceReceipt{}, false
	}
	return subject.state.receipt, true
}
func (subject MaintenancePhysicalSubject) TargetRelease() (Release, bool) {
	if !subject.Valid() {
		return Release{}, false
	}
	if subject.state.mode == maintenancePhysicalFailedReceiptCleanup {
		return subject.state.receipt.TargetRelease()
	}
	return cloneRelease(subject.state.target.intent.TargetRelease()), true
}
func (subject MaintenancePhysicalSubject) SourceRelease() (Release, bool) {
	if !subject.Valid() || subject.state.mode == maintenancePhysicalFailedReceiptCleanup {
		return Release{}, false
	}
	return subject.state.source.Release(), true
}

// MaintenancePhysicalEvidence is a closed exhaustive classification of the
// source and target cohorts for one maintenance generation.
type maintenancePhysicalEvidenceKind uint8

const (
	maintenancePhysicalEvidenceInvalid maintenancePhysicalEvidenceKind = iota
	maintenancePhysicalEvidenceTargetReady
	maintenancePhysicalEvidenceSourceReady
	maintenancePhysicalEvidenceTargetDivergent
	maintenancePhysicalEvidenceTargetAbsent
	maintenancePhysicalEvidenceFailedReceiptAbsent
	maintenancePhysicalEvidenceSourceFailed
)

// MaintenancePhysicalEvidence is an opaque closed value algebra. Its private
// tag selects exactly one concrete payload, so callers cannot forge a mixed or
// typed-nil result. The zero value is invalid.
type MaintenancePhysicalEvidence struct {
	kind            maintenancePhysicalEvidenceKind
	targetReady     MaintenanceTargetReady
	sourceReady     MaintenanceSourceReady
	targetDivergent MaintenanceTargetDivergent
	targetAbsent    MaintenanceTargetAbsent
	failedAbsent    MaintenanceFailedReceiptAbsent
	sourceFailed    MaintenanceSourceFailed
}

type maintenanceProjectionState struct {
	subject           MaintenancePhysicalSubject
	containerIDs      []string
	serviceContainers map[string][]string
	committedRelease  *Release
}

type MaintenanceTargetReady struct{ state *maintenanceProjectionState }
type MaintenanceSourceReady struct{ state *maintenanceProjectionState }
type MaintenanceSourceFailed struct{ state *maintenanceProjectionState }
type MaintenanceTargetDivergent struct{ state *maintenanceProjectionState }
type MaintenanceTargetAbsent struct{ state *maintenanceAbsentState }
type MaintenanceFailedReceiptAbsent struct{ state *maintenanceAbsentState }
type maintenanceAbsentState struct{ subject MaintenancePhysicalSubject }

func (e MaintenanceTargetReady) validForMaintenance(subject MaintenancePhysicalSubject) bool {
	if !validMaintenanceProjection(e.state, subject, true) {
		return false
	}
	release, ok := subject.TargetRelease()
	return ok && validReadyProjectionForRelease(release, e.state.containerIDs, e.state.serviceContainers)
}
func (e MaintenanceSourceReady) validForMaintenance(subject MaintenancePhysicalSubject) bool {
	if !validMaintenanceProjection(e.state, subject, true) {
		return false
	}
	release, ok := subject.SourceRelease()
	return ok && validReadyProjectionForRelease(release, e.state.containerIDs, e.state.serviceContainers)
}

func (e MaintenanceSourceFailed) validForMaintenance(subject MaintenancePhysicalSubject) bool {
	if !validMaintenanceProjection(e.state, subject, true) {
		return false
	}
	release, ok := subject.SourceRelease()
	return ok && validReadyProjectionForRelease(release, e.state.containerIDs, e.state.serviceContainers)
}
func (e MaintenanceTargetDivergent) validForMaintenance(subject MaintenancePhysicalSubject) bool {
	if !validMaintenanceProjection(e.state, subject, false) {
		return false
	}
	target, targetOK := subject.TargetRelease()
	source, sourceOK := subject.SourceRelease()
	if !targetOK || !sourceOK {
		return false
	}
	return !validReadyProjectionForRelease(
		target, e.state.containerIDs, e.state.serviceContainers,
	) && !validReadyProjectionForRelease(
		source, e.state.containerIDs, e.state.serviceContainers,
	)
}
func (e MaintenanceTargetAbsent) validForMaintenance(subject MaintenancePhysicalSubject) bool {
	return e.state != nil && e.state.subject == subject && subject.Valid() &&
		subject.state.mode != maintenancePhysicalFailedReceiptCleanup
}
func (e MaintenanceFailedReceiptAbsent) validForMaintenance(subject MaintenancePhysicalSubject) bool {
	return e.state != nil && e.state.subject == subject && subject.Valid() &&
		subject.state.mode == maintenancePhysicalFailedReceiptCleanup
}

func validMaintenanceProjection(
	state *maintenanceProjectionState,
	subject MaintenancePhysicalSubject,
	requireComplete bool,
) bool {
	if state == nil || state.subject != subject || !subject.Valid() {
		return false
	}
	if requireComplete {
		return validPhysicalProjection(state.containerIDs, state.serviceContainers)
	}
	return validObservedPhysicalProjection(state.containerIDs, state.serviceContainers)
}

func (e MaintenanceTargetReady) Valid() bool {
	return e.state != nil && e.validForMaintenance(e.state.subject)
}
func (e MaintenanceSourceReady) Valid() bool {
	return e.state != nil && e.validForMaintenance(e.state.subject)
}
func (e MaintenanceTargetDivergent) Valid() bool {
	return e.state != nil && e.validForMaintenance(e.state.subject)
}
func (e MaintenanceTargetAbsent) Valid() bool {
	return e.state != nil && e.validForMaintenance(e.state.subject)
}
func (e MaintenanceFailedReceiptAbsent) Valid() bool {
	return e.state != nil && e.validForMaintenance(e.state.subject)
}

func (e MaintenanceTargetReady) LeaseUUID() string     { return maintenanceEvidenceLease(e.state) }
func (e MaintenanceSourceReady) LeaseUUID() string     { return maintenanceEvidenceLease(e.state) }
func (e MaintenanceTargetDivergent) LeaseUUID() string { return maintenanceEvidenceLease(e.state) }
func (e MaintenanceTargetAbsent) LeaseUUID() string {
	if !e.Valid() {
		return ""
	}
	return e.state.subject.LeaseUUID()
}
func (e MaintenanceTargetReady) MaintenanceID() MaintenanceID {
	return maintenanceEvidenceID(e.state)
}
func (e MaintenanceSourceReady) MaintenanceID() MaintenanceID {
	return maintenanceEvidenceID(e.state)
}
func (e MaintenanceTargetDivergent) MaintenanceID() MaintenanceID {
	return maintenanceEvidenceID(e.state)
}
func (e MaintenanceTargetAbsent) MaintenanceID() MaintenanceID {
	if !e.Valid() {
		return MaintenanceID{}
	}
	return e.state.subject.MaintenanceID()
}
func (e MaintenanceFailedReceiptAbsent) LeaseUUID() string {
	if !e.Valid() {
		return ""
	}
	return e.state.subject.LeaseUUID()
}
func (e MaintenanceFailedReceiptAbsent) MaintenanceID() MaintenanceID {
	if !e.Valid() {
		return MaintenanceID{}
	}
	return e.state.subject.MaintenanceID()
}

func maintenanceEvidenceLease(state *maintenanceProjectionState) string {
	if state == nil || !state.subject.Valid() {
		return ""
	}
	return state.subject.LeaseUUID()
}
func maintenanceEvidenceID(state *maintenanceProjectionState) MaintenanceID {
	if state == nil || !state.subject.Valid() {
		return MaintenanceID{}
	}
	return state.subject.MaintenanceID()
}

func (e MaintenanceTargetReady) Projection() (Release, []string, map[string][]string) {
	if !e.Valid() {
		return Release{}, nil, nil
	}
	release, _ := e.state.subject.TargetRelease()
	return maintenanceProjection(release, e.state)
}
func (e MaintenanceSourceReady) Projection() (Release, []string, map[string][]string) {
	if !e.Valid() {
		return Release{}, nil, nil
	}
	release, _ := e.state.subject.SourceRelease()
	return maintenanceProjection(release, e.state)
}
func (e MaintenanceTargetDivergent) Projection() (Release, []string, map[string][]string) {
	if !e.Valid() {
		return Release{}, nil, nil
	}
	release, _ := e.state.subject.TargetRelease()
	return maintenanceProjection(release, e.state)
}
func maintenanceProjection(
	release Release,
	state *maintenanceProjectionState,
) (Release, []string, map[string][]string) {
	if state.committedRelease != nil {
		release = cloneRelease(*state.committedRelease)
	}
	return cloneRelease(release), slices.Clone(state.containerIDs), clonePhysicalProjection(state.serviceContainers)
}

func (e MaintenanceTargetReady) withCommittedRelease(release Release) MaintenanceTargetReady {
	if !e.Valid() || release.Version <= 0 {
		return MaintenanceTargetReady{}
	}
	copy := cloneRelease(release)
	return MaintenanceTargetReady{state: &maintenanceProjectionState{
		subject: e.state.subject, containerIDs: slices.Clone(e.state.containerIDs),
		serviceContainers: clonePhysicalProjection(e.state.serviceContainers),
		committedRelease:  &copy,
	}}
}

func NewMaintenanceTargetReady(
	subject MaintenancePhysicalSubject,
	containerIDs []string,
	serviceContainers map[string][]string,
) (MaintenancePhysicalEvidence, error) {
	state, err := newMaintenanceProjection(subject, containerIDs, serviceContainers, true)
	evidence := MaintenanceTargetReady{state: state}
	if err == nil && !evidence.Valid() {
		err = errors.New("maintenance target projection differs from exact target release")
	}
	if err != nil {
		return MaintenancePhysicalEvidence{}, err
	}
	return MaintenancePhysicalEvidence{
		kind: maintenancePhysicalEvidenceTargetReady, targetReady: evidence,
	}, nil
}
func NewMaintenanceSourceReady(
	subject MaintenancePhysicalSubject,
	containerIDs []string,
	serviceContainers map[string][]string,
) (MaintenancePhysicalEvidence, error) {
	state, err := newMaintenanceProjection(subject, containerIDs, serviceContainers, true)
	evidence := MaintenanceSourceReady{state: state}
	if err == nil && !evidence.Valid() {
		err = errors.New("maintenance source projection differs from exact source release")
	}
	if err != nil {
		return MaintenancePhysicalEvidence{}, err
	}
	return MaintenancePhysicalEvidence{
		kind: maintenancePhysicalEvidenceSourceReady, sourceReady: evidence,
	}, nil
}
func NewMaintenanceTargetDivergent(
	subject MaintenancePhysicalSubject,
	containerIDs []string,
	serviceContainers map[string][]string,
) (MaintenancePhysicalEvidence, error) {
	state, err := newMaintenanceProjection(subject, containerIDs, serviceContainers, false)
	evidence := MaintenanceTargetDivergent{state: state}
	if err == nil && !evidence.Valid() {
		err = errors.New("maintenance divergent target projection is invalid")
	}
	if err != nil {
		return MaintenancePhysicalEvidence{}, err
	}
	return MaintenancePhysicalEvidence{
		kind: maintenancePhysicalEvidenceTargetDivergent, targetDivergent: evidence,
	}, nil
}
func NewMaintenanceTargetAbsent(subject MaintenancePhysicalSubject) (MaintenancePhysicalEvidence, error) {
	if !subject.Valid() || subject.state.mode == maintenancePhysicalFailedReceiptCleanup {
		return MaintenancePhysicalEvidence{}, errors.New("maintenance physical subject is invalid")
	}
	absent := MaintenanceTargetAbsent{state: &maintenanceAbsentState{subject: subject}}
	return MaintenancePhysicalEvidence{
		kind: maintenancePhysicalEvidenceTargetAbsent, targetAbsent: absent,
	}, nil
}

func NewMaintenanceFailedReceiptAbsent(
	subject MaintenancePhysicalSubject,
) (MaintenancePhysicalEvidence, error) {
	if !subject.Valid() || subject.state.mode != maintenancePhysicalFailedReceiptCleanup {
		return MaintenancePhysicalEvidence{}, errors.New("maintenance failed-receipt subject is invalid")
	}
	absent := MaintenanceFailedReceiptAbsent{state: &maintenanceAbsentState{subject: subject}}
	return MaintenancePhysicalEvidence{
		kind: maintenancePhysicalEvidenceFailedReceiptAbsent, failedAbsent: absent,
	}, nil
}

func newMaintenanceProjection(
	subject MaintenancePhysicalSubject,
	containerIDs []string,
	serviceContainers map[string][]string,
	requireComplete bool,
) (*maintenanceProjectionState, error) {
	if !subject.Valid() {
		return nil, errors.New("maintenance physical subject is invalid")
	}
	valid := validObservedPhysicalProjection(containerIDs, serviceContainers)
	if requireComplete {
		valid = validPhysicalProjection(containerIDs, serviceContainers)
	} else {
		valid = len(containerIDs) > 0 && valid
	}
	if !valid {
		return nil, errors.New("maintenance substrate projection is invalid")
	}
	return &maintenanceProjectionState{
		subject: subject, containerIDs: slices.Clone(containerIDs),
		serviceContainers: clonePhysicalProjection(serviceContainers),
	}, nil
}

func validPhysicalProjection(containerIDs []string, serviceContainers map[string][]string) bool {
	return len(containerIDs) > 0 && len(serviceContainers) > 0 &&
		validObservedPhysicalProjection(containerIDs, serviceContainers)
}

func validReadyProjectionForRelease(
	release Release,
	containerIDs []string,
	serviceContainers map[string][]string,
) bool {
	if !validPhysicalProjection(containerIDs, serviceContainers) || len(release.Items) == 0 {
		return false
	}
	wantCount := 0
	wantServices := make(map[string]int, len(release.Items))
	for _, item := range release.Items {
		if item.ServiceName == "" || item.Quantity <= 0 {
			return false
		}
		if _, duplicate := wantServices[item.ServiceName]; duplicate {
			return false
		}
		wantServices[item.ServiceName] = item.Quantity
		wantCount += item.Quantity
	}
	if len(containerIDs) != wantCount || len(serviceContainers) != len(wantServices) {
		return false
	}
	for service, quantity := range wantServices {
		if len(serviceContainers[service]) != quantity {
			return false
		}
	}
	return true
}

func validObservedPhysicalProjection(
	containerIDs []string,
	serviceContainers map[string][]string,
) bool {
	seen := make(map[string]struct{}, len(containerIDs))
	for _, id := range containerIDs {
		if id == "" {
			return false
		}
		if _, duplicate := seen[id]; duplicate {
			return false
		}
		seen[id] = struct{}{}
	}
	projected := make(map[string]struct{}, len(containerIDs))
	for service, ids := range serviceContainers {
		if service == "" || len(ids) == 0 {
			return false
		}
		for _, id := range ids {
			if _, ok := seen[id]; !ok {
				return false
			}
			if _, duplicate := projected[id]; duplicate {
				return false
			}
			projected[id] = struct{}{}
		}
	}
	return len(projected) == len(seen)
}

func clonePhysicalProjection(in map[string][]string) map[string][]string {
	if in == nil {
		return nil
	}
	out := make(map[string][]string, len(in))
	for service, ids := range in {
		out[service] = slices.Clone(ids)
	}
	return out
}

func validateOperationPhysicalEvidence(
	subject OperationPhysicalSubject,
	evidence OperationPhysicalEvidence,
) error {
	switch evidence.kind {
	case operationPhysicalEvidenceTargetReady:
		if evidence.targetReady.validForOperation(subject) {
			return nil
		}
	case operationPhysicalEvidenceExactAbsent:
		if evidence.exactAbsent.validForOperation(subject) {
			return nil
		}
	case operationPhysicalEvidenceFailedReceiptAbsent:
		if evidence.failedReceiptAbsent.validForOperation(subject) {
			return nil
		}
	default:
		return errors.New("unknown operation substrate evidence")
	}
	return errors.New("operation substrate evidence belongs to another execution subject")
}

func validateMaintenancePhysicalEvidence(
	subject MaintenancePhysicalSubject,
	evidence MaintenancePhysicalEvidence,
) error {
	switch evidence.kind {
	case maintenancePhysicalEvidenceTargetReady:
		if evidence.targetReady.validForMaintenance(subject) {
			return nil
		}
	case maintenancePhysicalEvidenceSourceReady:
		if evidence.sourceReady.validForMaintenance(subject) {
			return nil
		}
	case maintenancePhysicalEvidenceSourceFailed:
		if evidence.sourceFailed.validForMaintenance(subject) {
			return nil
		}
	case maintenancePhysicalEvidenceTargetDivergent:
		if evidence.targetDivergent.validForMaintenance(subject) {
			return nil
		}
	case maintenancePhysicalEvidenceTargetAbsent:
		if evidence.targetAbsent.validForMaintenance(subject) {
			return nil
		}
	case maintenancePhysicalEvidenceFailedReceiptAbsent:
		if evidence.failedAbsent.validForMaintenance(subject) {
			return nil
		}
	default:
		return errors.New("unknown maintenance substrate evidence")
	}
	return errors.New("maintenance substrate evidence belongs to another execution subject")
}
