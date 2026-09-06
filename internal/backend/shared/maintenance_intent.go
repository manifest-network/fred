package shared

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/backendname"
	"github.com/manifest-network/fred/internal/maintenanceid"
)

const maxMaintenanceIntentEntryBytes = 4 << 20

// MaintenanceID is the opaque caller-issued replacement-generation identity
// shared by the write-ahead intent, target release, and every replacement
// container.
type MaintenanceID = maintenanceid.ID

// MaintenanceIntentKind identifies the replacement command being journaled.
type MaintenanceIntentKind string

const (
	MaintenanceIntentRestart      MaintenanceIntentKind = "restart"
	MaintenanceIntentUpdate       MaintenanceIntentKind = "update"
	MaintenanceIntentCustomDomain MaintenanceIntentKind = "custom_domain"
)

var ErrMaintenanceIntentConflict = errors.New("unresolved callback maintenance intent")

// MaintenanceExecutionPhase is the durable causal phase of one replacement.
// The closed values prevent Docker recovery from turning a missing legacy bool
// into caller-selected evidence that no physical effect could have occurred.
type MaintenanceExecutionPhase uint8

const (
	maintenanceExecutionPhaseInvalid MaintenanceExecutionPhase = iota
	MaintenanceExecutionBeforeEffects
	MaintenanceExecutionStarted
)

// MaintenanceIntentCandidate is a store-minted, immutable admission
// capability. Its fields and issuer are private so sibling packages cannot
// combine a request replay identity with a different target callback or pass a
// candidate to a journal from another storage lineage. The zero value is
// invalid.
type MaintenanceIntentCandidate struct {
	settlement    *MaintenanceSettlement
	issuer        *CallbackStore
	releases      *ReleaseStore
	request       MaintenanceRequestAuthority
	sourceRelease ReleaseClaim
	targetRelease Release
}

func (candidate MaintenanceIntentCandidate) Request() MaintenanceRequestAuthority {
	return candidate.request
}

// NewMaintenanceIntentCandidate validates and detaches the complete
// maintenance transition before BeginMaintenanceIntent may publish it. Exact
// redelivery is intentionally handled by ProbeMaintenanceIntent, so a replay
// never needs to manufacture a partial candidate from mutable release state.
func (s *MaintenanceSettlement) NewMaintenanceIntentCandidate(
	request MaintenanceRequestAuthority,
	source MaintenanceSourceClaim,
	targetRelease Release,
) (MaintenanceIntentCandidate, error) {
	if s == nil || s.callbacks == nil || s.releases == nil || request.issuer != s.callbacks ||
		(request.settlement != nil && request.settlement != s) {
		return MaintenanceIntentCandidate{}, errors.New(
			"maintenance request authority was not minted by this journal pair",
		)
	}
	if !source.Valid() || source.settlement != s || source.releases != s.releases {
		return MaintenanceIntentCandidate{}, errors.New(
			"maintenance source release was not claimed by this journal pair",
		)
	}
	candidate := MaintenanceIntentCandidate{
		settlement: s, issuer: s.callbacks, releases: s.releases,
		request: request, sourceRelease: source.claim,
		targetRelease: cloneRelease(targetRelease),
	}
	if err := validateMaintenanceIntentCandidate(candidate); err != nil {
		return MaintenanceIntentCandidate{}, err
	}
	return candidate, nil
}

// MaintenanceIntentClaim is an opaque precise recovery and settlement
// capability for one journal row. It deliberately cannot append a Release or
// cancel admission: those phase-specific authorities have distinct types.
type MaintenanceIntentClaim struct {
	settlement    *MaintenanceSettlement
	callbacks     *CallbackStore
	releases      *ReleaseStore
	entry         maintenanceIntentEntry
	maintenanceID MaintenanceID
	storageID     backendidentity.ID
	sourceDigest  [sha256.Size]byte
	targetDigest  [sha256.Size]byte
	digest        [sha256.Size]byte
}

// MaintenanceIntentAdmissionDisposition distinguishes new authority from
// exact request replay. Every non-Created replay disposition is capability-free:
// the caller must not start another substrate mutation. CompletedSuperseded is
// additionally a refusal to reinstall an older update payload.
type MaintenanceIntentAdmissionDisposition uint8

const (
	MaintenanceIntentAdmissionNone MaintenanceIntentAdmissionDisposition = iota
	MaintenanceIntentAdmissionCreated
	MaintenanceIntentAdmissionExisting
	MaintenanceIntentAdmissionCompleted
	// MaintenanceIntentAdmissionCompletedSuperseded means this exact update
	// completed, but a later update generation is already durable. The backend
	// must acknowledge that it will not repeat substrate work while refusing to
	// let the provider reinstall this older payload as current desired state.
	MaintenanceIntentAdmissionCompletedSuperseded
)

// MaintenanceIntentAdmission classifies one request as newly created, existing,
// or completed. Only Created carries a MaintenanceIntentDispatch; replay
// classifications are mutation-capability-free by construction.
type MaintenanceIntentAdmission struct {
	intent      MaintenanceIntentClaim
	disposition MaintenanceIntentAdmissionDisposition
	dispatch    *MaintenanceIntentDispatch
}

// Disposition reports the journal-issued replay classification. It is
// intentionally read-only: sibling packages cannot relabel an Existing replay
// as Created and turn its precise intent snapshot back into append authority.
func (a MaintenanceIntentAdmission) Disposition() MaintenanceIntentAdmissionDisposition {
	return a.disposition
}

func (a MaintenanceIntentAdmission) MaintenanceID() MaintenanceID {
	return a.intent.MaintenanceID()
}
func (a MaintenanceIntentAdmission) LeaseUUID() string { return a.intent.LeaseUUID() }
func (a MaintenanceIntentAdmission) TargetRelease() Release {
	return a.intent.TargetRelease()
}

// MaintenanceIntentDispatch is the opaque, store-issued authority to perform
// the first external work for a newly-created maintenance intent. Exact replay
// admissions deliberately carry no value of this type. Copies become stale as
// soon as StartMaintenanceAppend advances the durable phase.
type MaintenanceIntentDispatch struct {
	settlement *MaintenanceSettlement
	issuer     *CallbackStore
	releases   *ReleaseStore
	intent     MaintenanceIntentClaim
}

// CreatedDispatch returns first-dispatch authority only for the transaction
// that created the durable maintenance intent. Existing and completed replay
// classifications cannot be converted into mutation authority.
func (a MaintenanceIntentAdmission) CreatedDispatch() (MaintenanceIntentDispatch, bool) {
	if a.disposition != MaintenanceIntentAdmissionCreated || a.dispatch == nil {
		return MaintenanceIntentDispatch{}, false
	}
	return *a.dispatch, true
}

func (d MaintenanceIntentDispatch) Valid() bool {
	return validateMaintenanceIntentDispatch(d) == nil
}

func (d MaintenanceIntentDispatch) MaintenanceID() MaintenanceID {
	return d.intent.MaintenanceID()
}

func (d MaintenanceIntentDispatch) LeaseUUID() string { return d.intent.LeaseUUID() }
func (d MaintenanceIntentDispatch) TargetRelease() Release {
	return d.intent.TargetRelease()
}

// MaintenanceAppendClaim is the only authority ReleaseStore accepts for a new
// maintenance generation. It can be constructed only after the callback WAL
// durably records that cancellation is no longer legal.
type MaintenanceAppendClaim struct {
	settlement *MaintenanceSettlement
	issuer     *CallbackStore
	releases   *ReleaseStore
	intent     MaintenanceIntentClaim
}

func (c MaintenanceAppendClaim) Valid() bool {
	return validateMaintenanceAppendClaim(c) == nil
}
func (c MaintenanceAppendClaim) Intent() MaintenanceIntentClaim { return c.intent }

func (c MaintenanceIntentClaim) Valid() bool {
	return c.settlement != nil && c.callbacks == c.settlement.callbacks &&
		c.releases == c.settlement.releases && validateMaintenanceIntentClaim(c) == nil
}

// MatchesIntent reports whether both claims name the same immutable
// maintenance generation in the same exact open journal pair. The execution
// phase and row digest are deliberately normalized: StartMaintenanceExecution
// advances that one bit before a terminal proof is minted, while preserving
// every identity, request, source, and bound-target field.
func (c MaintenanceIntentClaim) MatchesIntent(other MaintenanceIntentClaim) bool {
	if !c.Valid() || !other.Valid() {
		return false
	}
	left := cloneMaintenanceIntentClaim(c)
	right := cloneMaintenanceIntentClaim(other)
	left.entry.EffectNotStarted = false
	right.entry.EffectNotStarted = false
	leftData, err := marshalMaintenanceIntent(left.entry)
	if err != nil {
		return false
	}
	rightData, err := marshalMaintenanceIntent(right.entry)
	if err != nil {
		return false
	}
	left.digest = sha256.Sum256(leftData)
	right.digest = sha256.Sum256(rightData)
	return maintenanceIntentClaimsEqual(left, right)
}

func (c MaintenanceIntentClaim) ExecutionPhase() MaintenanceExecutionPhase {
	if !c.Valid() {
		return maintenanceExecutionPhaseInvalid
	}
	if c.entry.EffectNotStarted {
		return MaintenanceExecutionBeforeEffects
	}
	return MaintenanceExecutionStarted
}
func (c MaintenanceIntentClaim) MaintenanceID() MaintenanceID         { return c.maintenanceID }
func (c MaintenanceIntentClaim) Kind() MaintenanceIntentKind          { return c.entry.Kind }
func (c MaintenanceIntentClaim) LeaseUUID() string                    { return c.entry.LeaseUUID }
func (c MaintenanceIntentClaim) Backend() string                      { return c.entry.Backend }
func (c MaintenanceIntentClaim) BackendStorageID() backendidentity.ID { return c.storageID }
func (c MaintenanceIntentClaim) CreatedAt() time.Time                 { return c.entry.CreatedAt }
func (c MaintenanceIntentClaim) SourceRelease() ReleaseClaim {
	return ReleaseClaim{
		issuer:    c.releases,
		leaseUUID: c.entry.LeaseUUID,
		version:   c.entry.SourceReleaseVersion,
		digest:    c.sourceDigest,
	}
}
func (c MaintenanceIntentClaim) TargetRelease() Release {
	return cloneRelease(c.entry.TargetRelease)
}
func (s *MaintenanceSettlement) targetReleaseClaim(
	c MaintenanceIntentClaim,
) (MaintenanceReleaseClaim, bool) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return MaintenanceReleaseClaim{}, false
	}
	if c.entry.TargetReleaseVersion == 0 {
		return MaintenanceReleaseClaim{}, false
	}
	return MaintenanceReleaseClaim{
		settlement: s,
		callbacks:  s.callbacks,
		releases:   s.releases,
		intent:     cloneMaintenanceIntentClaim(c),
		releaseClaim: ReleaseClaim{
			issuer:    s.releases,
			leaseUUID: c.entry.LeaseUUID,
			version:   c.entry.TargetReleaseVersion,
			digest:    c.targetDigest,
		},
		maintenanceID:   c.maintenanceID,
		immutableDigest: c.targetDigest,
	}, true
}
func (c MaintenanceIntentClaim) Tenant() string {
	authority, ok := releaseRuntimeIdentityFor(c.entry.TargetRelease)
	if !ok {
		return ""
	}
	return authority.tenant
}
func (c MaintenanceIntentClaim) ProviderUUID() string {
	authority, ok := releaseRuntimeIdentityFor(c.entry.TargetRelease)
	if !ok {
		return ""
	}
	return authority.providerUUID
}
func (c MaintenanceIntentClaim) CallbackURL() string {
	authority, ok := releaseRuntimeIdentityFor(c.entry.TargetRelease)
	if !ok {
		return ""
	}
	return authority.callbackURL
}
func (c MaintenanceIntentClaim) LifecycleCallbackURL() string {
	authority, ok := releaseRuntimeIdentityFor(c.entry.TargetRelease)
	if !ok {
		return ""
	}
	return authority.lifecycleCallbackURL
}

type maintenanceIntentEntry struct {
	MaintenanceID        MaintenanceID         `json:"maintenance_id"`
	Kind                 MaintenanceIntentKind `json:"kind"`
	LeaseUUID            string                `json:"lease_uuid"`
	Backend              string                `json:"backend"`
	BackendStorageID     string                `json:"backend_storage_id"`
	SourceReleaseVersion int                   `json:"source_release_version"`
	SourceReleaseDigest  string                `json:"source_release_digest"`
	TargetRelease        Release               `json:"target_release"`
	AppendStarted        bool                  `json:"append_started,omitempty"`
	TargetReleaseVersion int                   `json:"target_release_version,omitempty"`
	TargetReleaseDigest  string                `json:"target_release_digest,omitempty"`
	// EffectNotStarted is cleared immediately before the first external
	// mutation. Missing values from an older process decode false and therefore
	// recover conservatively as already started.
	EffectNotStarted     bool      `json:"effect_not_started,omitempty"`
	RequestDigest        string    `json:"request_digest"`
	RequestCallbackURL   string    `json:"request_callback_url"`
	RequestPayloadDigest string    `json:"request_payload_digest"`
	CreatedAt            time.Time `json:"created_at"`
}

// ProbeMaintenanceIntent classifies an exact request without minting mutation
// authority. It uses the same canonical request digest and live-lease terminal
// receipt fence as BeginMaintenanceIntent.
func (s *MaintenanceSettlement) ProbeMaintenanceIntent(
	request MaintenanceRequestAuthority,
) (MaintenanceIntentAdmissionDisposition, error) {
	if s == nil || s.callbacks == nil || s.releases == nil || request.issuer != s.callbacks ||
		request.settlement != s {
		return MaintenanceIntentAdmissionNone, errors.New(
			"maintenance request authority was not minted by this journal pair",
		)
	}
	if !request.Valid() {
		return MaintenanceIntentAdmissionNone, errors.New("maintenance probe requires exact request authority")
	}
	var disposition MaintenanceIntentAdmissionDisposition
	err := s.callbacks.view(func(tx *bolt.Tx) error {
		var classifyErr error
		disposition, classifyErr = classifyMaintenanceReplayTx(
			tx, request.LeaseUUID(), request.MaintenanceID(),
			encodeMaintenanceDigest(request.digest),
		)
		return classifyErr
	})
	return disposition, err
}

// BeginMaintenanceIntent publishes the durable barrier before a target release
// or replacement container can exist.
// maintenanceAdmissionAuthority is the sealed result of inspecting the
// callback/release pair before entering the callback write transaction. This
// preserves the sole cross-journal order (Release view before Callback update)
// and makes a Failed successor's predecessor a distinct transition input.
type maintenanceAdmissionAuthority interface {
	isMaintenanceAdmissionAuthority()
}

type directMaintenanceAdmissionAuthority struct{}

func (directMaintenanceAdmissionAuthority) isMaintenanceAdmissionAuthority() {}

type failedSuccessorMaintenanceAdmissionAuthority struct {
	predecessor failedOperationOverRelease
}

func (failedSuccessorMaintenanceAdmissionAuthority) isMaintenanceAdmissionAuthority() {}

func (s *MaintenanceSettlement) deriveMaintenanceAdmissionAuthorityLocked(
	leaseUUID string,
	source ReleaseClaim,
) (maintenanceAdmissionAuthority, error) {
	var failed operationLeaseMutationHead
	found := false
	if err := s.callbacks.view(func(tx *bolt.Tx) error {
		head, present, err := getLeaseMutationHeadTx(tx, leaseUUID)
		if err != nil || !present {
			return err
		}
		operation, ok := head.(operationLeaseMutationHead)
		if ok && operation.claim.entry.State == operationIntentFailed {
			failed = operation
			found = true
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if !found {
		return directMaintenanceAdmissionAuthority{}, nil
	}
	predecessor, err := bindFailedOperationOverRelease(
		s.callbacks, s.releases, failed, source,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"%w for lease %q: %w",
			ErrMaintenanceIntentConflict, leaseUUID, err,
		)
	}
	return failedSuccessorMaintenanceAdmissionAuthority{predecessor: predecessor}, nil
}

func (s *MaintenanceSettlement) BeginMaintenanceIntent(
	candidate MaintenanceIntentCandidate,
) (MaintenanceIntentAdmission, error) {
	if s == nil || s.callbacks == nil || s.releases == nil || candidate.settlement != s ||
		candidate.issuer != s.callbacks ||
		candidate.releases != s.releases {
		return MaintenanceIntentAdmission{}, errors.New(
			"maintenance intent candidate was not minted by this journal pair",
		)
	}
	if err := validateMaintenanceIntentCandidate(candidate); err != nil {
		return MaintenanceIntentAdmission{}, err
	}
	target := cloneRelease(candidate.targetRelease)
	target.MaintenanceID = candidate.request.MaintenanceID()
	if err := validateMaintenanceAppendInput(candidate.sourceRelease, target); err != nil {
		return MaintenanceIntentAdmission{}, err
	}
	entry := maintenanceIntentEntry{
		MaintenanceID:        candidate.request.MaintenanceID(),
		Kind:                 candidate.request.Kind(),
		LeaseUUID:            candidate.sourceRelease.LeaseUUID(),
		Backend:              candidate.request.Backend(),
		BackendStorageID:     candidate.request.BackendStorageID().String(),
		SourceReleaseVersion: candidate.sourceRelease.Version(),
		SourceReleaseDigest:  encodeMaintenanceDigest(candidate.sourceRelease.Digest()),
		TargetRelease:        target,
		EffectNotStarted:     true,
		RequestDigest:        encodeMaintenanceDigest(candidate.request.digest),
		RequestCallbackURL:   candidate.request.CallbackURL(),
		RequestPayloadDigest: encodeMaintenanceDigest(candidate.request.payloadDigest),
		CreatedAt:            time.Now(),
	}
	data, err := marshalMaintenanceIntent(entry)
	if err != nil {
		return MaintenanceIntentAdmission{}, err
	}

	unlock := s.lockLease(entry.LeaseUUID)
	defer unlock()
	admissionAuthority, err := s.deriveMaintenanceAdmissionAuthorityLocked(
		entry.LeaseUUID, candidate.sourceRelease,
	)
	if err != nil {
		return MaintenanceIntentAdmission{}, err
	}
	var admission MaintenanceIntentAdmission
	err = s.callbacks.update(func(tx *bolt.Tx) error {
		disposition, err := classifyMaintenanceReplayTx(
			tx, entry.LeaseUUID, entry.MaintenanceID, entry.RequestDigest,
		)
		if err != nil {
			return err
		}
		if disposition == MaintenanceIntentAdmissionCompleted ||
			disposition == MaintenanceIntentAdmissionCompletedSuperseded {
			admission.disposition = disposition
			return nil
		}
		head, present, err := getLeaseMutationHeadTx(tx, entry.LeaseUUID)
		if err != nil {
			return err
		}
		if current, ok := head.(maintenanceLeaseMutationHead); ok &&
			current.claim.MaintenanceID() == entry.MaintenanceID {
			if current.claim.entry.RequestDigest != entry.RequestDigest {
				return fmt.Errorf("%w for lease %q: maintenance ID has divergent request authority",
					ErrMaintenanceIntentConflict, entry.LeaseUUID)
			}
			intent, err := s.mintMaintenanceIntentClaim(current.claim)
			if err != nil {
				return err
			}
			admission = MaintenanceIntentAdmission{
				intent: intent, disposition: MaintenanceIntentAdmissionExisting,
			}
			return nil
		}
		if err := rejectPendingMaintenanceCompletionTx(tx, entry.LeaseUUID); err != nil {
			return err
		}
		if present {
			switch state := head.(type) {
			case operationLeaseMutationHead:
				if state.claim.entry.State == operationIntentPending {
					return fmt.Errorf("%w for lease %q: operation is already admitted",
						ErrMaintenanceIntentConflict, entry.LeaseUUID)
				}
			case maintenanceLeaseMutationHead:
				return fmt.Errorf("%w for lease %q: maintenance is already admitted",
					ErrMaintenanceIntentConflict, entry.LeaseUUID)
			case closeLeaseMutationHead:
				return fmt.Errorf("%w for lease %q: close is already admitted",
					ErrMaintenanceIntentConflict, entry.LeaseUUID)
			case closedLeaseMutationHead:
				return fmt.Errorf("%w for lease %q: lease is permanently closed",
					ErrMaintenanceIntentConflict, entry.LeaseUUID)
			}
		}
		decoded, err := decodeMaintenanceIntent([]byte(entry.LeaseUUID), data)
		if err != nil {
			return err
		}
		var transition leaseMutationTransition
		var transitionErr error
		if operation, ok := head.(operationLeaseMutationHead); ok {
			if operation.claim.entry.State == operationIntentFailed {
				failed, valid := admissionAuthority.(failedSuccessorMaintenanceAdmissionAuthority)
				if !valid {
					return fmt.Errorf(
						"%w for lease %q: failed successor no longer matches the admitted predecessor",
						ErrMaintenanceIntentConflict, entry.LeaseUUID,
					)
				}
				transition, transitionErr = newReplaceFailedOperationWithMaintenanceLeaseMutation(
					operation.claim, failed.predecessor, decoded,
				)
			} else {
				if _, failed := admissionAuthority.(failedSuccessorMaintenanceAdmissionAuthority); failed {
					return fmt.Errorf(
						"%w for lease %q: failed successor no longer matches the callback head",
						ErrMaintenanceIntentConflict, entry.LeaseUUID,
					)
				}
				successorIdentity, ok := releaseRuntimeIdentityFor(entry.TargetRelease)
				if !ok || operation.claim.Backend() != entry.Backend ||
					operation.claim.BackendStorageID().String() != entry.BackendStorageID ||
					operation.claim.Tenant() != successorIdentity.Tenant() ||
					operation.claim.ProviderUUID() != successorIdentity.ProviderUUID() ||
					operation.claim.OperationID() != successorIdentity.OperationID() {
					return fmt.Errorf(
						"%w for lease %q: terminal operation has different backend storage, principal, or operation authority",
						ErrMaintenanceIntentConflict, entry.LeaseUUID,
					)
				}
				transition, transitionErr = newReplaceOperationWithMaintenanceLeaseMutation(
					operation.claim, decoded,
				)
			}
		} else {
			if _, failed := admissionAuthority.(failedSuccessorMaintenanceAdmissionAuthority); failed {
				return fmt.Errorf(
					"%w for lease %q: failed successor no longer matches the callback head",
					ErrMaintenanceIntentConflict, entry.LeaseUUID,
				)
			}
			transition, transitionErr = newPublishMaintenanceLeaseMutation(decoded)
		}
		if transitionErr != nil {
			return transitionErr
		}
		written, err := applyLeaseMutationTx(tx, transition)
		if err != nil {
			return err
		}
		intent, err := s.mintMaintenanceIntentClaim(written.(maintenanceLeaseMutationHead).claim)
		if err != nil {
			return err
		}
		admission = MaintenanceIntentAdmission{
			intent:      intent,
			disposition: MaintenanceIntentAdmissionCreated,
		}
		admission.dispatch = &MaintenanceIntentDispatch{
			settlement: s, issuer: s.callbacks,
			releases: s.releases, intent: admission.intent,
		}
		return nil
	})
	if err != nil {
		return MaintenanceIntentAdmission{}, err
	}
	return admission, nil
}

// StartMaintenanceAppend irreversibly consumes first-dispatch authority and
// advances its maintenance intent to the
// append-started phase before ReleaseStore can create a target generation. A
// crash after this transition but before the release append is classified as
// an interrupted failure by recovery; cancellation authority is never
// recreated.
func (s *MaintenanceSettlement) StartMaintenanceAppend(
	dispatch MaintenanceIntentDispatch,
) (MaintenanceAppendClaim, error) {
	if s == nil || s.callbacks == nil || s.releases == nil || dispatch.settlement != s ||
		dispatch.issuer != s.callbacks ||
		dispatch.releases != s.releases {
		return MaintenanceAppendClaim{}, errors.New(
			"maintenance dispatch was not minted by this journal pair",
		)
	}
	if err := validateMaintenanceIntentDispatch(dispatch); err != nil {
		return MaintenanceAppendClaim{}, err
	}
	claim := dispatch.intent

	unlock := s.lockLease(claim.LeaseUUID())
	defer unlock()
	var started MaintenanceIntentClaim
	err := s.callbacks.update(func(tx *bolt.Tx) error {
		if err := verifyMaintenanceIntentTx(tx, claim); err != nil {
			return err
		}
		entry := cloneMaintenanceIntentEntry(claim.entry)
		entry.AppendStarted = true
		data, err := marshalMaintenanceIntent(entry)
		if err != nil {
			return err
		}
		candidate, err := decodeMaintenanceIntent([]byte(entry.LeaseUUID), data)
		if err != nil {
			return err
		}
		transition, err := newStartMaintenanceAppendLeaseMutation(claim, candidate)
		if err != nil {
			return err
		}
		written, err := applyLeaseMutationTx(tx, transition)
		if err != nil {
			return err
		}
		started, err = s.mintMaintenanceIntentClaim(written.(maintenanceLeaseMutationHead).claim)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return MaintenanceAppendClaim{}, err
	}
	appendClaim := MaintenanceAppendClaim{
		settlement: s, issuer: s.callbacks, releases: s.releases, intent: started,
	}
	if err := validateMaintenanceAppendClaim(appendClaim); err != nil {
		return MaintenanceAppendClaim{}, err
	}
	return appendClaim, nil
}

// RecoverMaintenanceAppend reissues pair-bound append authority from the
// exact current durable append-started phase after process restart. It never
// recreates first-dispatch or cancellation authority.
func (s *MaintenanceSettlement) RecoverMaintenanceAppend(
	intent MaintenanceIntentClaim,
) (MaintenanceAppendClaim, error) {
	if err := s.validateIntent(intent); err != nil {
		return MaintenanceAppendClaim{}, err
	}
	if !intent.entry.AppendStarted {
		return MaintenanceAppendClaim{}, errors.New("maintenance append has not started")
	}
	if intent.entry.TargetReleaseVersion != 0 {
		return MaintenanceAppendClaim{}, errors.New("maintenance target is already bound")
	}
	unlock := s.lockLease(intent.LeaseUUID())
	defer unlock()
	if err := s.callbacks.requireCurrentMaintenanceClaim(intent); err != nil {
		return MaintenanceAppendClaim{}, err
	}
	return MaintenanceAppendClaim{
		settlement: s, issuer: s.callbacks, releases: s.releases,
		intent: cloneMaintenanceIntentClaim(intent),
	}, nil
}

// BindMaintenanceIntentTarget records the exact store-assigned target version
// and immutable release digest after AppendMaintenance. It returns the
// refreshed target capability, not a detached intent plus the now-stale input
// target: anything allowed to cross the physical-effect boundary therefore
// carries the bound journal generation by construction. A crash before this
// bind is recoverable by searching the release history for MaintenanceID.
func (s *MaintenanceSettlement) BindMaintenanceIntentTarget(
	target MaintenanceReleaseClaim,
) (MaintenanceReleaseClaim, error) {
	if !target.validFor(s) {
		return MaintenanceReleaseClaim{}, errors.New(
			"maintenance target was not issued by this journal pair",
		)
	}
	claim := target.intent
	if err := validateMaintenanceIntentTargetBinding(claim, target); err != nil {
		return MaintenanceReleaseClaim{}, err
	}

	unlock := s.lockLease(claim.LeaseUUID())
	defer unlock()
	refreshed, err := s.callbacks.bindMaintenanceIntentTargetLocked(claim, target)
	if err != nil {
		return MaintenanceReleaseClaim{}, err
	}
	intent, err := s.mintMaintenanceIntentClaim(refreshed)
	if err != nil {
		return MaintenanceReleaseClaim{}, err
	}
	bound, ok := s.targetReleaseClaim(intent)
	if !ok {
		return MaintenanceReleaseClaim{}, errors.New("bound maintenance intent lost its exact target")
	}
	return bound, nil
}

// TryBindMaintenanceIntentTarget is the recovery-safe form of
// BindMaintenanceIntentTarget. It never waits behind another journal mutation
// for this lease; acquired=false leaves the exact unbound intent untouched for
// the next level-triggered sweep. Callback HTTP owns a separate drain lock and
// cannot delay this mutation. Live admission uses the blocking form because it
// does not hold the fleet-wide recovery mutex.
func (s *MaintenanceSettlement) TryBindMaintenanceIntentTarget(
	target MaintenanceReleaseClaim,
) (bound MaintenanceReleaseClaim, acquired bool, err error) {
	if !target.validFor(s) {
		return MaintenanceReleaseClaim{}, false, errors.New(
			"maintenance target was not issued by this journal pair",
		)
	}
	claim := target.intent
	if err := validateMaintenanceIntentTargetBinding(claim, target); err != nil {
		return MaintenanceReleaseClaim{}, false, err
	}
	unlock, acquired := s.tryLockLease(claim.LeaseUUID())
	if !acquired {
		return MaintenanceReleaseClaim{}, false, nil
	}
	defer unlock()
	refreshed, err := s.callbacks.bindMaintenanceIntentTargetLocked(claim, target)
	if err != nil {
		return MaintenanceReleaseClaim{}, true, err
	}
	intent, err := s.mintMaintenanceIntentClaim(refreshed)
	if err != nil {
		return MaintenanceReleaseClaim{}, true, err
	}
	bound, ok := s.targetReleaseClaim(intent)
	if !ok {
		return MaintenanceReleaseClaim{}, true, errors.New("bound maintenance intent lost its exact target")
	}
	return bound, true, nil
}

func validateMaintenanceIntentTargetBinding(
	claim MaintenanceIntentClaim,
	target MaintenanceReleaseClaim,
) error {
	if err := validateMaintenanceIntentClaim(claim); err != nil {
		return err
	}
	if !claim.entry.AppendStarted {
		return errors.New("maintenance append has not started")
	}
	if !target.valid() || target.LeaseUUID() != claim.LeaseUUID() ||
		target.MaintenanceID() != claim.MaintenanceID() {
		return errors.New("maintenance target claim does not match intent")
	}
	expected := claim.TargetRelease()
	expected.Version = target.Version()
	expectedDigest, err := maintenanceReleaseDigest(expected)
	if err != nil {
		return err
	}
	if expectedDigest != target.Digest() {
		return errors.New("maintenance target release differs from durable intent")
	}
	return nil
}

// bindMaintenanceIntentTargetLocked requires ownership of the per-lease
// journal-mutation lock. The transaction's exact-claim check remains the
// linearization point against settlement, close preemption, and another
// recovery pass.
func (s *CallbackStore) bindMaintenanceIntentTargetLocked(
	claim MaintenanceIntentClaim,
	target MaintenanceReleaseClaim,
) (MaintenanceIntentClaim, error) {
	var refreshed MaintenanceIntentClaim
	err := s.update(func(tx *bolt.Tx) error {
		if err := verifyMaintenanceIntentTx(tx, claim); err != nil {
			return err
		}
		entry := cloneMaintenanceIntentEntry(claim.entry)
		if entry.TargetReleaseVersion != 0 {
			if entry.TargetReleaseVersion != target.Version() ||
				entry.TargetReleaseDigest != encodeMaintenanceDigest(target.Digest()) {
				return errors.New("maintenance intent target is already bound to another release")
			}
			refreshed = cloneMaintenanceIntentClaim(claim)
			return nil
		}
		entry.TargetReleaseVersion = target.Version()
		entry.TargetReleaseDigest = encodeMaintenanceDigest(target.Digest())
		data, err := marshalMaintenanceIntent(entry)
		if err != nil {
			return err
		}
		candidate, err := decodeMaintenanceIntent([]byte(entry.LeaseUUID), data)
		if err != nil {
			return err
		}
		transition, err := newBindMaintenanceTargetLeaseMutation(claim, candidate)
		if err != nil {
			return err
		}
		written, err := applyLeaseMutationTx(tx, transition)
		if err != nil {
			return err
		}
		refreshed = written.(maintenanceLeaseMutationHead).claim
		return nil
	})
	return refreshed, err
}

// CancelMaintenanceIntent removes only the exact pre-append intent when no
// target release or substrate mutation was accepted. StartMaintenanceAppend
// rewrites the row, so every copied dispatch becomes a stale CAS capability.
func (s *MaintenanceSettlement) CancelMaintenanceIntent(dispatch MaintenanceIntentDispatch) error {
	if s == nil || s.callbacks == nil || s.releases == nil || dispatch.settlement != s ||
		dispatch.issuer != s.callbacks ||
		dispatch.releases != s.releases {
		return errors.New("maintenance dispatch was not minted by this journal pair")
	}
	if err := validateMaintenanceIntentDispatch(dispatch); err != nil {
		return err
	}
	claim := dispatch.intent
	unlock := s.lockLease(claim.LeaseUUID())
	defer unlock()
	return s.callbacks.update(func(tx *bolt.Tx) error {
		if err := verifyMaintenanceIntentTx(tx, claim); err != nil {
			return err
		}
		transition, err := newCancelMaintenanceLeaseMutation(claim)
		if err != nil {
			return err
		}
		_, err = applyLeaseMutationTx(tx, transition)
		return err
	})
}

func (s *CallbackStore) getMaintenanceIntent(
	leaseUUID string,
) (MaintenanceIntentClaim, bool, error) {
	if err := validateCanonicalLeaseUUID(leaseUUID); err != nil {
		return MaintenanceIntentClaim{}, false, err
	}
	var claim MaintenanceIntentClaim
	var found bool
	err := s.view(func(tx *bolt.Tx) error {
		head, present, err := getLeaseMutationHeadTx(tx, leaseUUID)
		if err != nil || !present {
			return err
		}
		maintenance, ok := head.(maintenanceLeaseMutationHead)
		if !ok {
			return nil
		}
		claim = maintenance.claim
		found = true
		return nil
	})
	return claim, found, err
}

func (s *CallbackStore) listMaintenanceIntents() ([]MaintenanceIntentClaim, error) {
	var claims []MaintenanceIntentClaim
	err := s.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
		if bucket == nil {
			return errors.New("callback lease mutation head bucket missing")
		}
		return bucket.ForEach(func(key, value []byte) error {
			if value == nil {
				return fmt.Errorf("callback lease mutation head %q is a nested bucket", key)
			}
			head, err := decodeLeaseMutationHead(key, value)
			if err != nil {
				return err
			}
			if maintenance, ok := head.(maintenanceLeaseMutationHead); ok {
				claims = append(claims, maintenance.claim)
			}
			return nil
		})
	})
	return claims, err
}

// GetMaintenanceIntent reads one exact maintenance claim through the
// construction-bound callback/release pair used for subsequent recovery.
func (s *MaintenanceSettlement) GetMaintenanceIntent(
	leaseUUID string,
) (MaintenanceIntentClaim, bool, error) {
	if s == nil || !s.valid() {
		return MaintenanceIntentClaim{}, false, errors.New("maintenance settlement is invalid")
	}
	claim, found, err := s.callbacks.getMaintenanceIntent(leaseUUID)
	if err != nil || !found {
		return MaintenanceIntentClaim{}, found, err
	}
	claim, err = s.mintMaintenanceIntentClaim(claim)
	return claim, err == nil, err
}

// ListMaintenanceIntents reads recovery claims only through their exact
// callback/release pair, preventing a claim from one callback store from being
// combined with another release journal after reopen or mis-wiring.
func (s *MaintenanceSettlement) ListMaintenanceIntents() ([]MaintenanceIntentClaim, error) {
	if s == nil || !s.valid() {
		return nil, errors.New("maintenance settlement is invalid")
	}
	claims, err := s.callbacks.listMaintenanceIntents()
	if err != nil {
		return nil, err
	}
	for i := range claims {
		claims[i], err = s.mintMaintenanceIntentClaim(claims[i])
		if err != nil {
			return nil, err
		}
	}
	return claims, nil
}

// mintMaintenanceIntentClaim turns a claim decoded from this pair's current
// callback snapshot into exact process-local authority. It deliberately never
// rebinds a claim already issued by another open pair, so closing and reopening
// either journal invalidates every previously held capability.
func (s *MaintenanceSettlement) mintMaintenanceIntentClaim(
	claim MaintenanceIntentClaim,
) (MaintenanceIntentClaim, error) {
	if s == nil || !s.valid() {
		return MaintenanceIntentClaim{}, errors.New("maintenance settlement is invalid")
	}
	if err := validateMaintenanceIntentClaim(claim); err != nil {
		return MaintenanceIntentClaim{}, err
	}
	if claim.settlement != nil || claim.callbacks != nil || claim.releases != nil {
		if claim.settlement != s || claim.callbacks != s.callbacks || claim.releases != s.releases {
			return MaintenanceIntentClaim{}, errors.New(
				"maintenance intent was minted by another journal pair",
			)
		}
		return cloneMaintenanceIntentClaim(claim), nil
	}
	claim.settlement = s
	claim.callbacks = s.callbacks
	claim.releases = s.releases
	return cloneMaintenanceIntentClaim(claim), nil
}

func (s *MaintenanceSettlement) resolveSuccessLocked(
	claim MaintenanceIntentClaim,
	active MaintenanceReleaseActive,
) (CallbackEntry, error) {
	if err := s.callbacks.requireCurrentMaintenanceClaim(claim); err != nil {
		return CallbackEntry{}, err
	}
	if err := s.validateActiveProofLocked(claim, active); err != nil {
		return CallbackEntry{}, err
	}
	entry, err := prepareMaintenanceIntentCompletion(
		claim, backend.CallbackStatusSuccess, "",
	)
	if err != nil {
		return CallbackEntry{}, err
	}
	return s.callbacks.resolveMaintenanceIntentLocked(claim, entry)
}

func (s *MaintenanceSettlement) resolveFailureLocked(
	claim MaintenanceIntentClaim,
	failed MaintenanceReleaseFailure,
	errMsg string,
) (CallbackEntry, error) {
	if err := s.callbacks.requireCurrentMaintenanceClaim(claim); err != nil {
		return CallbackEntry{}, err
	}
	if err := s.validateFailureProofLocked(claim, failed); err != nil {
		return CallbackEntry{}, err
	}
	entry, err := prepareMaintenanceIntentCompletion(
		claim, backend.CallbackStatusFailed, errMsg,
	)
	if err != nil {
		return CallbackEntry{}, err
	}
	return s.callbacks.resolveMaintenanceIntentLocked(claim, entry)
}

func prepareDivergedMaintenanceCompletions(
	claim MaintenanceIntentClaim,
	errMsg string,
) (CallbackEntry, CallbackEntry, error) {
	maintenance, err := prepareMaintenanceIntentCompletion(
		claim, backend.CallbackStatusSuccess, "",
	)
	if err != nil {
		return CallbackEntry{}, CallbackEntry{}, err
	}
	deliveryID, err := uuid.NewRandom()
	if err != nil {
		return CallbackEntry{}, CallbackEntry{},
			fmt.Errorf("allocate runtime-failure callback delivery ID: %w", err)
	}
	runtimeFailure := CallbackEntry{
		DeliveryID:  deliveryID.String(),
		LeaseUUID:   claim.LeaseUUID(),
		CallbackURL: claim.LifecycleCallbackURL(),
		// This is a lifecycle status on the wire, but it is generated from and
		// ordered with one exact maintenance settlement. Classify it as
		// maintenance-derived so coalescing and later maintenance admission cannot
		// erase or overtake the second half of that atomic fact pair.
		DeliveryKind:     CallbackDeliveryKindMaintenance,
		Status:           backend.CallbackStatusFailed,
		Backend:          claim.Backend(),
		BackendStorageID: claim.BackendStorageID().String(),
		Error:            errMsg,
		CreatedAt:        time.Now(),
	}
	if err := validateNewCallbackEntry(runtimeFailure, time.Now()); err != nil {
		return CallbackEntry{}, CallbackEntry{}, err
	}
	return maintenance, runtimeFailure, nil
}

func prepareMaintenanceIntentCompletion(
	claim MaintenanceIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
) (CallbackEntry, error) {
	if err := validateMaintenanceIntentClaim(claim); err != nil {
		return CallbackEntry{}, err
	}
	if status != backend.CallbackStatusSuccess && status != backend.CallbackStatusFailed {
		return CallbackEntry{}, fmt.Errorf("maintenance intent has invalid completion status %q", status)
	}
	if status == backend.CallbackStatusSuccess && claim.entry.TargetReleaseVersion == 0 {
		return CallbackEntry{}, errors.New("unbound maintenance intent cannot resolve success")
	}
	deliveryID, err := uuid.NewRandom()
	if err != nil {
		return CallbackEntry{}, fmt.Errorf("allocate maintenance callback delivery ID: %w", err)
	}
	entry := callbackEntryForMaintenanceIntent(claim.entry, deliveryID.String(), status, errMsg)
	if err := validateNewCallbackEntry(entry, time.Now()); err != nil {
		return CallbackEntry{}, err
	}
	return entry, nil
}

func (s *CallbackStore) resolveMaintenanceIntentLocked(
	claim MaintenanceIntentClaim,
	entry CallbackEntry,
) (CallbackEntry, error) {
	entries, err := s.resolveMaintenanceIntentEntriesLocked(claim, []CallbackEntry{entry})
	if err != nil {
		return CallbackEntry{}, err
	}
	return entries[0], nil
}

func (s *CallbackStore) resolveMaintenanceIntentEntriesLocked(
	claim MaintenanceIntentClaim,
	entries []CallbackEntry,
) ([]CallbackEntry, error) {
	if len(entries) == 0 {
		return nil, errors.New("maintenance settlement requires at least one callback")
	}
	data := make([][]byte, len(entries))
	err := s.update(func(tx *bolt.Tx) error {
		if err := verifyMaintenanceIntentTx(tx, claim); err != nil {
			return err
		}
		for i := range entries {
			var putErr error
			entries[i], data[i], putErr = putCallbackEntryTx(tx, entries[i])
			if putErr != nil {
				return putErr
			}
		}
		receipt := maintenanceCompletionRecordFor(
			claim, entries[0].Status, entries[0].Error, entries[0].CreatedAt,
			entries[0].Sequence,
		)
		transition, err := newResolveMaintenanceLeaseMutation(claim, receipt)
		if err != nil {
			return err
		}
		_, err = applyLeaseMutationTx(tx, transition)
		return err
	})
	if err != nil {
		return nil, err
	}
	for i := range entries {
		entries[i].storageVersion = callbackStorageV2
		entries[i].storageLease = entries[i].LeaseUUID
		entries[i].storageDeliveryID = entries[i].DeliveryID
		entries[i].storageKey = string(callbackSequenceKey(entries[i].Sequence))
		entries[i].storageDigest = sha256.Sum256(data[i])
	}
	s.notifyReplayCommit(claim.LeaseUUID())
	return entries, nil
}

func callbackEntryForMaintenanceIntent(
	intent maintenanceIntentEntry,
	deliveryID string,
	status backend.CallbackStatus,
	errMsg string,
) CallbackEntry {
	authority, _ := releaseRuntimeIdentityFor(intent.TargetRelease)
	return CallbackEntry{
		DeliveryID:       deliveryID,
		LeaseUUID:        intent.LeaseUUID,
		CallbackURL:      authority.lifecycleCallbackURL,
		DeliveryKind:     CallbackDeliveryKindMaintenance,
		Status:           status,
		Backend:          intent.Backend,
		BackendStorageID: intent.BackendStorageID,
		Error:            errMsg,
		CreatedAt:        time.Now(),
	}
}

func validateMaintenanceIntentCandidate(candidate MaintenanceIntentCandidate) error {
	if candidate.settlement == nil || candidate.issuer != candidate.settlement.callbacks ||
		candidate.releases != candidate.settlement.releases ||
		candidate.request.issuer != candidate.issuer {
		return errors.New("maintenance intent candidate has no issuing journal pair")
	}
	if candidate.request.settlement != nil && candidate.request.settlement != candidate.settlement {
		return errors.New("maintenance request authority belongs to another settlement")
	}
	if !candidate.request.Valid() {
		return errors.New("maintenance intent requires exact wire-request authority")
	}
	if !candidate.sourceRelease.valid() {
		return errors.New("maintenance intent requires an exact source release claim")
	}
	if candidate.sourceRelease.LeaseUUID() != candidate.request.LeaseUUID() {
		return errors.New("maintenance request and source release identify different leases")
	}
	if !candidate.targetRelease.MaintenanceID.IsZero() {
		return errors.New("maintenance target template must not carry an identity")
	}
	if candidate.targetRelease.Version != 0 || candidate.targetRelease.Status != "deploying" {
		return errors.New("maintenance target must be a version-zero deploying template")
	}
	targetAuthority, ok := releaseRuntimeIdentityFor(candidate.targetRelease)
	if !ok {
		return errors.New("maintenance target requires durable runtime authority")
	}
	if candidate.request.CallbackURL() != targetAuthority.lifecycleCallbackURL {
		return errors.New("maintenance request callback differs from target release lifecycle authority")
	}
	if err := validateStoredCallbackCreatedAt(candidate.targetRelease.CreatedAt); err != nil {
		return fmt.Errorf("maintenance target: %w", err)
	}
	if err := validateNewCallbackCreatedAt(candidate.targetRelease.CreatedAt, time.Now()); err != nil {
		return fmt.Errorf("maintenance target: %w", err)
	}
	return nil
}

func validateMaintenanceIntentEntry(entry maintenanceIntentEntry, leaseUUID string) error {
	if !entry.MaintenanceID.Valid() {
		return fmt.Errorf("maintenance ID must be a canonical UUIDv4: %q", entry.MaintenanceID)
	}
	if entry.LeaseUUID != leaseUUID {
		return fmt.Errorf("maintenance intent lease mismatch: key %q contains %q", leaseUUID, entry.LeaseUUID)
	}
	storageID, err := backendidentity.Parse(entry.BackendStorageID)
	if err != nil {
		return fmt.Errorf("invalid maintenance intent storage identity: %w", err)
	}
	sourceDigest, err := parseMaintenanceDigest(entry.SourceReleaseDigest, false)
	if err != nil {
		return fmt.Errorf("invalid maintenance source release digest: %w", err)
	}
	source := ReleaseClaim{leaseUUID: entry.LeaseUUID, version: entry.SourceReleaseVersion, digest: sourceDigest}
	template := cloneRelease(entry.TargetRelease)
	if template.MaintenanceID != entry.MaintenanceID {
		return errors.New("maintenance target release has a different maintenance ID")
	}
	if err := validateMaintenanceAppendInput(source, template); err != nil {
		return err
	}
	if err := backendname.Validate(entry.Backend); err != nil {
		return fmt.Errorf("maintenance intent backend: %w", err)
	}
	if !storageID.Valid() {
		return errors.New("maintenance intent storage identity is invalid")
	}
	if entry.Kind != MaintenanceIntentRestart && entry.Kind != MaintenanceIntentUpdate &&
		entry.Kind != MaintenanceIntentCustomDomain {
		return fmt.Errorf("invalid maintenance intent kind %q", entry.Kind)
	}
	if err := validateStoredCallbackCreatedAt(entry.CreatedAt); err != nil {
		return err
	}
	targetDigest, err := parseMaintenanceDigest(entry.TargetReleaseDigest, true)
	if err != nil {
		return fmt.Errorf("invalid maintenance target release digest: %w", err)
	}
	switch {
	case !entry.AppendStarted && entry.TargetReleaseVersion != 0:
		return errors.New("maintenance target cannot be bound before append starts")
	case entry.TargetReleaseVersion == 0 && targetDigest != ([sha256.Size]byte{}):
		return errors.New("maintenance target fence must be wholly absent or wholly present")
	case entry.TargetReleaseVersion < 0:
		return errors.New("maintenance target release version cannot be negative")
	case entry.TargetReleaseVersion > 0 && targetDigest == ([sha256.Size]byte{}):
		return errors.New("maintenance target fence must be wholly absent or wholly present")
	case entry.TargetReleaseVersion > 0:
		expected := cloneRelease(template)
		expected.Version = entry.TargetReleaseVersion
		digest, err := maintenanceReleaseDigest(expected)
		if err != nil {
			return err
		}
		if digest != targetDigest {
			return errors.New("maintenance target digest does not match target template")
		}
	}
	requestDigest, err := parseMaintenanceDigest(entry.RequestDigest, false)
	if err != nil {
		return fmt.Errorf("invalid maintenance request digest: %w", err)
	}
	wantRequestDigest, err := maintenanceEntryRequestDigest(entry)
	if err != nil {
		return err
	}
	if requestDigest != wantRequestDigest {
		return errors.New("maintenance request digest does not match immutable intent authority")
	}
	if err := validateCallbackDestination(entry.RequestCallbackURL); err != nil {
		return fmt.Errorf("maintenance request callback: %w", err)
	}
	return nil
}

func validateMaintenanceIntentClaim(claim MaintenanceIntentClaim) error {
	if claim.digest == ([sha256.Size]byte{}) || !claim.maintenanceID.Valid() || !claim.storageID.Valid() {
		return errors.New("maintenance intent claim has no durable capability")
	}
	if claim.maintenanceID != claim.entry.MaintenanceID ||
		claim.storageID.String() != claim.entry.BackendStorageID ||
		encodeMaintenanceDigest(claim.sourceDigest) != claim.entry.SourceReleaseDigest ||
		encodeMaintenanceDigest(claim.targetDigest) != claim.entry.TargetReleaseDigest {
		return errors.New("maintenance intent claim has divergent authority")
	}
	return validateMaintenanceIntentEntry(claim.entry, claim.entry.LeaseUUID)
}

func validateMaintenanceIntentDispatch(dispatch MaintenanceIntentDispatch) error {
	if dispatch.settlement == nil || dispatch.issuer != dispatch.settlement.callbacks ||
		dispatch.releases != dispatch.settlement.releases {
		return errors.New("maintenance dispatch has no issuing journal pair")
	}
	if dispatch.issuer.boltStore == nil {
		return errors.New("maintenance dispatch has invalid callback journal lineage")
	}
	if err := validateMaintenanceIntentClaim(dispatch.intent); err != nil {
		return err
	}
	if dispatch.intent.callbacks != dispatch.issuer || dispatch.intent.releases != dispatch.releases {
		return errors.New("maintenance dispatch intent belongs to another journal pair")
	}
	if dispatch.intent.settlement != dispatch.settlement {
		return errors.New("maintenance dispatch intent belongs to another settlement")
	}
	if binding := dispatch.issuer.binding; binding != nil &&
		(binding.backendName != dispatch.intent.Backend() ||
			binding.storageID != dispatch.intent.BackendStorageID()) {
		return errors.New("maintenance dispatch differs from issuing callback journal lineage")
	}
	if dispatch.intent.entry.AppendStarted {
		return errors.New("maintenance intent dispatch is no longer cancelable")
	}
	return nil
}

func validateMaintenanceAppendClaim(claim MaintenanceAppendClaim) error {
	if claim.settlement == nil || claim.issuer != claim.settlement.callbacks ||
		claim.releases != claim.settlement.releases {
		return errors.New("maintenance append has no issuing journal pair")
	}
	if err := validateMaintenanceIntentClaim(claim.intent); err != nil {
		return err
	}
	if claim.intent.callbacks != claim.issuer || claim.intent.releases != claim.releases {
		return errors.New("maintenance append intent belongs to another journal pair")
	}
	if claim.intent.settlement != claim.settlement {
		return errors.New("maintenance append intent belongs to another settlement")
	}
	if !claim.intent.entry.AppendStarted {
		return errors.New("maintenance append has not started")
	}
	return nil
}

func marshalMaintenanceIntent(entry maintenanceIntentEntry) ([]byte, error) {
	data, err := json.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("marshal maintenance intent: %w", err)
	}
	if len(data) > maxMaintenanceIntentEntryBytes {
		return nil, fmt.Errorf("maintenance intent exceeds %d bytes", maxMaintenanceIntentEntryBytes)
	}
	return data, nil
}

func decodeMaintenanceIntent(key, value []byte) (MaintenanceIntentClaim, error) {
	var entry maintenanceIntentEntry
	if err := decodeStrictAuthoritativeObject(value, maxMaintenanceIntentEntryBytes, &entry); err != nil {
		return MaintenanceIntentClaim{}, fmt.Errorf("decode maintenance intent %q: %w", key, err)
	}
	if err := validateMaintenanceIntentEntry(entry, string(key)); err != nil {
		return MaintenanceIntentClaim{}, fmt.Errorf("invalid maintenance intent %q: %w", key, err)
	}
	storageID, _ := backendidentity.Parse(entry.BackendStorageID)
	sourceDigest, _ := parseMaintenanceDigest(entry.SourceReleaseDigest, false)
	targetDigest, _ := parseMaintenanceDigest(entry.TargetReleaseDigest, true)
	entry = cloneMaintenanceIntentEntry(entry)
	return MaintenanceIntentClaim{
		entry:         entry,
		maintenanceID: entry.MaintenanceID,
		storageID:     storageID,
		sourceDigest:  sourceDigest,
		targetDigest:  targetDigest,
		digest:        sha256.Sum256(value),
	}, nil
}

func cloneMaintenanceIntentEntry(entry maintenanceIntentEntry) maintenanceIntentEntry {
	entry.TargetRelease = cloneRelease(entry.TargetRelease)
	return entry
}

func verifyMaintenanceIntentTx(tx *bolt.Tx, claim MaintenanceIntentClaim) error {
	head, present, err := getLeaseMutationHeadTx(tx, claim.LeaseUUID())
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("maintenance intent no longer exists for lease %q", claim.LeaseUUID())
	}
	maintenance, ok := head.(maintenanceLeaseMutationHead)
	if !ok {
		return fmt.Errorf("maintenance intent for lease %q was replaced by %q",
			claim.LeaseUUID(), head.headKind())
	}
	if maintenance.claim.digest != claim.digest {
		return errors.New("maintenance intent changed before precise mutation")
	}
	return nil
}

func (s *CallbackStore) requireCurrentMaintenanceClaim(claim MaintenanceIntentClaim) error {
	if s == nil || s.boltStore == nil {
		return errors.New("maintenance intent requires an identity-bound callback journal")
	}
	if err := validateMaintenanceIntentClaim(claim); err != nil {
		return err
	}
	if s.binding != nil && (claim.Backend() != s.binding.backendName ||
		claim.BackendStorageID() != s.binding.storageID) {
		return errors.New("maintenance intent belongs to another callback journal")
	}
	return s.view(func(tx *bolt.Tx) error {
		return verifyMaintenanceIntentTx(tx, claim)
	})
}

// rejectPendingMaintenanceCompletionTx prevents a newer replacement from
// overtaking the subscriber-visible result of an older one. Maintenance
// completions use the lease's stable lifecycle route, so the provider cannot
// distinguish generations from the wire payload. Requiring the older durable
// completion to receive a synchronous 2xx and be precisely removed before a
// successor is admitted preserves subscriber order without changing the
// callback protocol.
//
// BeginMaintenanceIntent calls this in the same bbolt transaction and under
// the same per-lease journal-mutation lock as intent publication. Resolution of
// the previous intent atomically enqueues its completion, so there is no
// delete-before-check window in which a newer generation can slip through.
func rejectPendingMaintenanceCompletionTx(tx *bolt.Tx, leaseUUID string) error {
	entries, err := listPendingCallbackEntriesTx(tx, leaseUUID)
	if err != nil {
		return fmt.Errorf("inspect callback FIFO before maintenance admission: %w", err)
	}
	for _, entry := range entries {
		if entry.DeliveryKind == CallbackDeliveryKindMaintenance {
			return fmt.Errorf(
				"%w: previous maintenance completion for lease %q is still pending delivery",
				backend.ErrInvalidState, leaseUUID,
			)
		}
	}
	return nil
}

func parseMaintenanceDigest(value string, allowEmpty bool) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if value == "" && allowEmpty {
		return digest, nil
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != value {
		return digest, errors.New("digest must be canonical SHA-256")
	}
	copy(digest[:], decoded)
	if digest == ([sha256.Size]byte{}) {
		return digest, errors.New("zero digest is not authority")
	}
	return digest, nil
}

func encodeMaintenanceDigest(digest [sha256.Size]byte) string {
	if digest == ([sha256.Size]byte{}) {
		return ""
	}
	return hex.EncodeToString(digest[:])
}
