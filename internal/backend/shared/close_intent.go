package shared

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/backendname"
)

const (
	maxCloseIntentEntryBytes      = 4 << 20
	maxCloseIntentIdentityBytes   = 4 << 10
	maxClosePhysicalNameBytes     = 4 << 10
	closeIntentPreemptedOperation = "operation preempted by lease close"
)

// ErrCloseIntentConflict means a lease already has an unresolved close whose
// immutable input differs from the requested close. The existing intent is
// preserved; callers must not perform cleanup for the conflicting request.
var ErrCloseIntentConflict = errors.New("unresolved callback close intent")

// CloseIntentAdmissionDisposition tells a backend whether it created the
// durable close barrier or recovered an exact retry of one already accepted.
type CloseIntentAdmissionDisposition uint8

const (
	CloseIntentAdmissionNone CloseIntentAdmissionDisposition = iota
	CloseIntentAdmissionCreated
	CloseIntentAdmissionExisting
)

// closeIntentSpec is the coordinator-derived description of destructive lease
// cleanup. newCloseIntentCandidate validates and detaches it before admission,
// so restart recovery never reconstructs authority from partial survivors.
type closeIntentSpec struct {
	LeaseUUID        string
	Tenant           string
	ProviderUUID     string
	Items            []backend.LeaseItem
	ResourceProfiles []SKUResourceSnapshot
	Manifest         []byte

	// CallbackURL and LifecycleCallbackURL are the exact durable pair captured
	// by the active provision. Both may be empty only for a callbackless legacy
	// provision; otherwise both halves must be present and match exactly.
	CallbackURL          string
	LifecycleCallbackURL string

	RetainOnClose bool
	CleanupOnly   bool

	// ActiveReleaseVersion and ActiveReleaseDigest fence cleanup to the exact
	// release selected by CloseSettlement under the shared per-lease gate. The
	// callback journal binds this internal snapshot but does not reinterpret the
	// release store's canonical encoding.
	ActiveReleaseVersion     int
	ActiveReleaseDigest      [sha256.Size]byte
	ActiveReleaseOperationID OperationID
}

// closeIntentCandidate is a store-minted, immutable admission capability. Its
// private issuer and storage lineage prevent a close assembled for one backend
// journal from authorizing destructive work through another. The zero value is
// invalid.
type closeIntentCandidate struct {
	issuer    *CallbackStore
	spec      closeIntentSpec
	backend   string
	storageID backendidentity.ID
}

func newCloseIntentCandidate(
	issuer *CallbackStore,
	spec closeIntentSpec,
	backendName string,
	storageID backendidentity.ID,
) (closeIntentCandidate, error) {
	candidate := closeIntentCandidate{
		issuer: issuer, spec: cloneCloseIntentSpec(spec),
		backend: backendName, storageID: storageID,
	}
	if err := validateCloseIntentCandidate(candidate); err != nil {
		return closeIntentCandidate{}, err
	}
	return candidate, nil
}

func cloneCloseIntentSpec(spec closeIntentSpec) closeIntentSpec {
	spec.Items = slices.Clone(spec.Items)
	spec.ResourceProfiles = CloneSKUResourceSnapshot(spec.ResourceProfiles)
	spec.Manifest = bytes.Clone(spec.Manifest)
	return spec
}

// CloseIntentAdmission is returned only after the close barrier is durably
// committed. Existing is an exact idempotent retry and returns the original
// capability, including its persisted physical-execution generation.
type CloseIntentAdmission struct {
	claim       CloseIntentClaim
	disposition CloseIntentAdmissionDisposition
	// OperationPreempted reports that this transaction replaced an unresolved
	// operation intent with its exact failed operation callback.
	operationPreempted bool
	// MaintenancePreempted reports that this same transaction replaced an
	// unresolved maintenance intent with its failed lifecycle completion.
	maintenancePreempted bool
}

// Claim returns the journal-issued cleanup authority. Both a newly-created
// close and its exact replay carry the same durable authority, allowing cleanup
// to resume after a crash without letting callers assemble an admission.
func (admission CloseIntentAdmission) Claim() CloseIntentClaim { return admission.claim }

// Disposition reports whether this call created the durable close barrier or
// replayed the exact existing barrier.
func (admission CloseIntentAdmission) Disposition() CloseIntentAdmissionDisposition {
	return admission.disposition
}

// OperationPreempted reports whether admitting this close terminalized an
// unresolved operation in the same transaction.
func (admission CloseIntentAdmission) OperationPreempted() bool {
	return admission.operationPreempted
}

// MaintenancePreempted reports whether admitting this close terminalized an
// unresolved maintenance replacement in the same transaction.
func (admission CloseIntentAdmission) MaintenancePreempted() bool {
	return admission.maintenancePreempted
}

// CloseIntentClaim is an opaque, copy-safe capability for one exact durable
// close. It contains no caller-settable authority. Every mutation verifies the
// lease key and SHA-256 digest against bbolt, so using two copies cannot replay
// a resolve or overwrite a refreshed physical-execution generation.
type CloseIntentClaim struct {
	settlement          *CloseSettlement
	entry               closeIntentEntry
	intentID            uuid.UUID
	storageID           backendidentity.ID
	activeReleaseDigest [sha256.Size]byte
	digest              [sha256.Size]byte
}

func (c CloseIntentClaim) IntentID() string {
	if c.intentID == uuid.Nil {
		return ""
	}
	return c.intentID.String()
}

func (c CloseIntentClaim) LeaseUUID() string { return c.entry.LeaseUUID }
func (c CloseIntentClaim) Backend() string   { return c.entry.Backend }

func (c CloseIntentClaim) BackendStorageID() backendidentity.ID { return c.storageID }
func (c CloseIntentClaim) Tenant() string                       { return c.entry.Tenant }
func (c CloseIntentClaim) ProviderUUID() string                 { return c.entry.ProviderUUID }

func (c CloseIntentClaim) Items() []backend.LeaseItem {
	return slices.Clone(c.entry.Items)
}

func (c CloseIntentClaim) ResourceProfiles() []SKUResourceSnapshot {
	return CloneSKUResourceSnapshot(c.entry.ResourceProfiles)
}

func (c CloseIntentClaim) Manifest() []byte { return bytes.Clone(c.entry.Manifest) }

func (c CloseIntentClaim) CallbackURL() string { return c.entry.CallbackURL }

func (c CloseIntentClaim) LifecycleCallbackURL() string {
	return c.entry.LifecycleCallbackURL
}

func (c CloseIntentClaim) RetainOnClose() bool { return c.entry.RetainOnClose }
func (c CloseIntentClaim) CleanupOnly() bool   { return c.entry.CleanupOnly }

func (c CloseIntentClaim) ActiveReleaseVersion() int {
	return c.entry.ActiveReleaseVersion
}

func (c CloseIntentClaim) ActiveReleaseDigest() [sha256.Size]byte {
	return c.activeReleaseDigest
}

func (c CloseIntentClaim) ActiveReleaseOperationID() OperationID {
	return c.entry.ActiveReleaseOperationID
}

// CloseExecutionGeneration is an opaque monotonic durable generation. Zero is
// the not-started phase and cannot authorize physical work.
type CloseExecutionGeneration struct{ value int }

func (generation CloseExecutionGeneration) Valid() bool { return generation.value > 0 }
func (generation CloseExecutionGeneration) Number() int { return generation.value }

func (c CloseIntentClaim) ExecutionGeneration() CloseExecutionGeneration {
	return CloseExecutionGeneration{value: c.entry.ExecutionGeneration}
}
func (c CloseIntentClaim) CreatedAt() time.Time { return c.entry.CreatedAt }

// Interrupted attempt identities are authored only while consuming the exact
// previous journal head. They survive restart and never select by timestamp.
func (c CloseIntentClaim) InterruptedOperationID() OperationID { return c.entry.InterruptedOperationID }
func (c CloseIntentClaim) InterruptedMaintenanceID() MaintenanceID {
	return c.entry.InterruptedMaintenanceID
}

type closeIntentEntry struct {
	IntentID                 string                `json:"intent_id"`
	InterruptedOperationID   OperationID           `json:"interrupted_operation_id,omitzero"`
	InterruptedMaintenanceID MaintenanceID         `json:"interrupted_maintenance_id,omitzero"`
	LeaseUUID                string                `json:"lease_uuid"`
	Backend                  string                `json:"backend"`
	BackendStorageID         string                `json:"backend_storage_id"`
	Tenant                   string                `json:"tenant"`
	ProviderUUID             string                `json:"provider_uuid"`
	Items                    []backend.LeaseItem   `json:"items"`
	ResourceProfiles         []SKUResourceSnapshot `json:"resource_profiles"`
	Manifest                 []byte                `json:"manifest"`
	CallbackURL              string                `json:"callback_url,omitempty"`
	LifecycleCallbackURL     string                `json:"lifecycle_callback_url,omitempty"`
	RetainOnClose            bool                  `json:"retain_on_close"`
	CleanupOnly              bool                  `json:"cleanup_only"`
	ActiveReleaseVersion     int                   `json:"active_release_version"`
	ActiveReleaseDigest      string                `json:"active_release_digest"`
	ActiveReleaseOperationID OperationID           `json:"active_release_operation_id,omitzero"`
	ExecutionGeneration      int                   `json:"cleanup_attempts"`
	CreatedAt                time.Time             `json:"created_at"`
}

// closeIntentAdmissionAuthority is a sealed description of the callback-head
// transition that may publish a close. Failed-over-active and
// Failed-without-Release authority are intentionally unrepresentable as an
// ordinary terminal-operation transition.
type closeIntentAdmissionAuthority interface {
	isCloseIntentAdmissionAuthority()
}

type directCloseIntentAdmissionAuthority struct{}

func (directCloseIntentAdmissionAuthority) isCloseIntentAdmissionAuthority() {}

type failedSuccessorCloseIntentAdmissionAuthority struct {
	predecessor failedOperationOverRelease
}

func (failedSuccessorCloseIntentAdmissionAuthority) isCloseIntentAdmissionAuthority() {}

type failedWithoutReleaseCleanupCloseIntentAdmissionAuthority struct {
	absence failedOperationWithoutRelease
}

func (failedWithoutReleaseCleanupCloseIntentAdmissionAuthority) isCloseIntentAdmissionAuthority() {}

// beginCloseIntentLocked durably publishes the ordinary close barrier before
// destructive work. A terminal Failed operation cannot enter through this
// path; it requires one of the distinct pair-bound witnesses below.
func (s *CallbackStore) beginCloseIntentLocked(
	candidate closeIntentCandidate,
) (CloseIntentAdmission, error) {
	return s.beginCloseIntentWithAuthorityLocked(
		candidate, directCloseIntentAdmissionAuthority{},
	)
}

// beginCloseIntentAfterFailedSuccessorLocked consumes the store-bound proof
// that the current Failed operation succeeded one exact still-active Release.
// This keeps the exceptional lineage in its own transition type instead of
// weakening the generic operation/close identity rule.
func (s *CallbackStore) beginCloseIntentAfterFailedSuccessorLocked(
	candidate closeIntentCandidate,
	predecessor failedOperationOverRelease,
) (CloseIntentAdmission, error) {
	if predecessor.callbacks != s || predecessor.releases == nil {
		return CloseIntentAdmission{}, errors.New(
			"failed-successor close authority belongs to another journal pair",
		)
	}
	return s.beginCloseIntentWithAuthorityLocked(
		candidate,
		failedSuccessorCloseIntentAdmissionAuthority{predecessor: predecessor},
	)
}

// beginCleanupCloseAfterFailedOperationLocked consumes exact Failed-head plus
// Release-absence authority. The distinct admission type cannot be routed into
// a projected close, retention, or a terminal-operation lineage transition.
func (s *CallbackStore) beginCleanupCloseAfterFailedOperationLocked(
	candidate closeIntentCandidate,
	absence failedOperationWithoutRelease,
) (CloseIntentAdmission, error) {
	if absence.callbacks != s || absence.releases == nil {
		return CloseIntentAdmission{}, errors.New(
			"failed-operation cleanup authority belongs to another journal pair",
		)
	}
	return s.beginCloseIntentWithAuthorityLocked(
		candidate,
		failedWithoutReleaseCleanupCloseIntentAdmissionAuthority{absence: absence},
	)
}

func (s *CallbackStore) beginCloseIntentWithAuthorityLocked(
	candidate closeIntentCandidate,
	authority closeIntentAdmissionAuthority,
) (CloseIntentAdmission, error) {
	if candidate.issuer != s && (candidate.issuer != nil || s == nil ||
		s.boltStore == nil || s.binding != nil) {
		return CloseIntentAdmission{}, errors.New(
			"close intent candidate was not minted by this callback journal",
		)
	}
	if err := validateCloseIntentCandidate(candidate); err != nil {
		return CloseIntentAdmission{}, err
	}
	spec := candidate.spec
	entry := closeIntentEntry{
		LeaseUUID:                spec.LeaseUUID,
		Backend:                  candidate.backend,
		BackendStorageID:         candidate.storageID.String(),
		Tenant:                   spec.Tenant,
		ProviderUUID:             spec.ProviderUUID,
		Items:                    slices.Clone(spec.Items),
		ResourceProfiles:         CloneSKUResourceSnapshot(spec.ResourceProfiles),
		Manifest:                 bytes.Clone(spec.Manifest),
		CallbackURL:              spec.CallbackURL,
		LifecycleCallbackURL:     spec.LifecycleCallbackURL,
		RetainOnClose:            spec.RetainOnClose,
		CleanupOnly:              spec.CleanupOnly,
		ActiveReleaseVersion:     spec.ActiveReleaseVersion,
		ActiveReleaseDigest:      encodeCloseReleaseDigest(spec.ActiveReleaseDigest),
		ActiveReleaseOperationID: spec.ActiveReleaseOperationID,
	}

	admission := CloseIntentAdmission{}
	err := s.update(func(tx *bolt.Tx) error {
		head, present, err := getLeaseMutationHeadTx(tx, entry.LeaseUUID)
		if err != nil {
			return err
		}
		if current, ok := head.(closeLeaseMutationHead); ok {
			if !closeIntentEntryMatchesSpec(current.claim.entry, entry) {
				return fmt.Errorf("%w for lease %q", ErrCloseIntentConflict, entry.LeaseUUID)
			}
			admission = CloseIntentAdmission{
				claim: current.claim, disposition: CloseIntentAdmissionExisting,
			}
			return nil
		}
		if _, closed := head.(closedLeaseMutationHead); closed {
			return fmt.Errorf("%w for lease %q: lease is permanently closed",
				ErrCloseIntentConflict, entry.LeaseUUID)
		}

		intentID, err := uuid.NewRandom()
		if err != nil {
			return fmt.Errorf("allocate callback close intent ID: %w", err)
		}
		entry.IntentID = intentID.String()
		entry.CreatedAt = time.Now()
		switch current := head.(type) {
		case operationLeaseMutationHead:
			if current.claim.entry.State != operationIntentSucceeded {
				entry.InterruptedOperationID = current.claim.OperationID()
			}
		case maintenanceLeaseMutationHead:
			entry.InterruptedMaintenanceID = current.claim.MaintenanceID()
		}
		data, err := marshalCloseIntent(entry)
		if err != nil {
			return err
		}

		claim, decodeErr := decodeCloseIntent([]byte(entry.LeaseUUID), data)
		if decodeErr != nil {
			return decodeErr
		}
		var transition leaseMutationTransition
		var transitionErr error
		switch current := head.(type) {
		case operationLeaseMutationHead:
			operationClaim := current.claim
			if operationClaim.Backend() != entry.Backend ||
				operationClaim.BackendStorageID().String() != entry.BackendStorageID {
				return fmt.Errorf(
					"operation and close intents have different backend storage authority for lease %q",
					entry.LeaseUUID,
				)
			}
			var operationReceipt operationIntentEntry
			switch admissionAuthority := authority.(type) {
			case directCloseIntentAdmissionAuthority:
				if operationClaim.entry.State != operationIntentPending {
					if operationClaim.entry.State != operationIntentSucceeded ||
						entry.ActiveReleaseVersion == 0 ||
						operationClaim.OperationID() != entry.ActiveReleaseOperationID ||
						(!entry.CleanupOnly && (operationClaim.Tenant() != entry.Tenant ||
							operationClaim.ProviderUUID() != entry.ProviderUUID)) {
						return fmt.Errorf(
							"%w for lease %q: terminal operation does not match the close release lineage",
							ErrCloseIntentConflict, entry.LeaseUUID,
						)
					}
					operationReceipt = *operationClaim.entry
					transition, transitionErr = newReplaceOperationWithCloseLeaseMutation(
						operationClaim, operationReceipt, claim,
					)
					break
				}
				if !entry.CleanupOnly && (operationClaim.Tenant() != entry.Tenant ||
					operationClaim.ProviderUUID() != entry.ProviderUUID) {
					return fmt.Errorf(
						"%w for lease %q: pending operation and close have different principal authority",
						ErrCloseIntentConflict, entry.LeaseUUID,
					)
				}
				callback := operationFailureCallbackEntry(
					*operationClaim.entry, closeIntentPreemptedOperation,
				)
				preemptedDeliveryID, idErr := uuid.NewRandom()
				if idErr != nil {
					return fmt.Errorf("allocate preempted operation callback delivery ID: %w", idErr)
				}
				callback.DeliveryID = preemptedDeliveryID.String()
				if err := operationIntentMatchesCallback(*operationClaim.entry, callback); err != nil {
					return err
				}
				if _, _, err := putCallbackEntryTx(tx, callback); err != nil {
					return err
				}
				terminal := *operationClaim.entry
				terminal.State = operationIntentFailed
				terminal.SettledAt = callback.CreatedAt
				terminal.SettlementError = callback.Error
				// This terminal value exists only as the receipt atomically
				// consumed by the close transition. It never becomes a Failed
				// operation head, so it cannot mint predecessor authority.
				terminal.FailurePredecessor = operationFailurePredecessorRecord{
					Kind: operationFailurePredecessorAbsent,
				}
				operationReceipt = terminal
				admission.operationPreempted = true
				transition, transitionErr = newReplaceOperationWithCloseLeaseMutation(
					operationClaim, operationReceipt, claim,
				)

			case failedSuccessorCloseIntentAdmissionAuthority:
				if operationClaim.entry.State != operationIntentFailed {
					return fmt.Errorf(
						"%w for lease %q: failed-successor authority does not match the current operation",
						ErrCloseIntentConflict, entry.LeaseUUID,
					)
				}
				operationReceipt = *operationClaim.entry
				predecessor := ReleaseClaim{
					issuer:    admissionAuthority.predecessor.releases,
					leaseUUID: entry.LeaseUUID, version: entry.ActiveReleaseVersion,
					digest: candidate.spec.ActiveReleaseDigest,
				}
				if !admissionAuthority.predecessor.validForHeadAndRelease(
					s, admissionAuthority.predecessor.releases,
					operationClaim, predecessor,
				) {
					return fmt.Errorf(
						"%w for lease %q: failed operation does not seal the close release",
						ErrCloseIntentConflict, entry.LeaseUUID,
					)
				}
				transition, transitionErr = newReplaceFailedOperationWithCloseLeaseMutation(
					operationClaim, admissionAuthority.predecessor,
					operationReceipt, claim,
				)

			case failedWithoutReleaseCleanupCloseIntentAdmissionAuthority:
				if operationClaim.entry.State != operationIntentFailed ||
					!entry.CleanupOnly || entry.ActiveReleaseVersion != 0 ||
					entry.ActiveReleaseDigest != "" ||
					!entry.ActiveReleaseOperationID.IsZero() ||
					!admissionAuthority.absence.validForHead(
						s, admissionAuthority.absence.releases, operationClaim,
					) {
					return fmt.Errorf(
						"%w for lease %q: failed-operation absence does not authorize this cleanup",
						ErrCloseIntentConflict, entry.LeaseUUID,
					)
				}
				operationReceipt = *operationClaim.entry
				transition, transitionErr = newReplaceFailedOperationWithCloseLeaseMutation(
					operationClaim, admissionAuthority.absence,
					operationReceipt, claim,
				)

			default:
				return fmt.Errorf("unsupported close admission authority %T", authority)
			}
		case maintenanceLeaseMutationHead:
			if _, direct := authority.(directCloseIntentAdmissionAuthority); !direct {
				return fmt.Errorf(
					"%w for lease %q: derived operation authority no longer matches the callback head",
					ErrCloseIntentConflict, entry.LeaseUUID,
				)
			}
			maintenanceClaim := current.claim
			if maintenanceClaim.Backend() != entry.Backend ||
				maintenanceClaim.BackendStorageID().String() != entry.BackendStorageID {
				return fmt.Errorf(
					"maintenance and close intents have different backend storage authority for lease %q",
					entry.LeaseUUID,
				)
			}
			if entry.CleanupOnly || entry.ActiveReleaseVersion == 0 ||
				entry.ActiveReleaseVersion != maintenanceClaim.SourceRelease().Version() ||
				entry.ActiveReleaseDigest != maintenanceClaim.entry.SourceReleaseDigest {
				return fmt.Errorf(
					"close intent does not fence the maintenance source release for lease %q",
					entry.LeaseUUID,
				)
			}
			if (!entry.CleanupOnly && (entry.Tenant != maintenanceClaim.Tenant() ||
				entry.ProviderUUID != maintenanceClaim.ProviderUUID())) ||
				entry.ActiveReleaseOperationID != maintenanceClaim.TargetRelease().OperationID {
				return fmt.Errorf(
					"maintenance and close intents have different release lineage or principal authority for lease %q",
					entry.LeaseUUID,
				)
			}
			deliveryID, idErr := uuid.NewRandom()
			if idErr != nil {
				return fmt.Errorf("allocate preempted maintenance callback delivery ID: %w", idErr)
			}
			callback := callbackEntryForMaintenanceIntent(
				maintenanceClaim.entry,
				deliveryID.String(),
				backend.CallbackStatusFailed,
				"maintenance preempted by lease close",
			)
			if err := validateNewCallbackEntry(callback, time.Now()); err != nil {
				return err
			}
			storedCallback, _, err := putCallbackEntryTx(tx, callback)
			if err != nil {
				return err
			}
			receipt := maintenanceCompletionRecordFor(
				maintenanceClaim, callback.Status, callback.Error, callback.CreatedAt,
				storedCallback.Sequence,
			)
			transition, transitionErr = newReplaceMaintenanceWithCloseLeaseMutation(
				maintenanceClaim, receipt, claim,
			)
			admission.maintenancePreempted = true
		default:
			if _, direct := authority.(directCloseIntentAdmissionAuthority); !direct {
				return fmt.Errorf(
					"%w for lease %q: derived operation authority no longer matches the callback head",
					ErrCloseIntentConflict, entry.LeaseUUID,
				)
			}
			if present {
				return fmt.Errorf("unsupported callback lease mutation head %T", head)
			}
			transition, transitionErr = newPublishCloseLeaseMutation(claim)
		}
		if transitionErr != nil {
			return transitionErr
		}
		written, transitionErr := applyLeaseMutationTx(tx, transition)
		if transitionErr != nil {
			return transitionErr
		}
		admission.claim = written.(closeLeaseMutationHead).claim
		admission.disposition = CloseIntentAdmissionCreated
		return nil
	})
	if err != nil {
		return CloseIntentAdmission{}, err
	}
	if admission.operationPreempted || admission.maintenancePreempted {
		s.notifyReplayCommit(candidate.spec.LeaseUUID)
	}
	return admission, nil
}

// GetCloseIntent returns the current exact close capability for leaseUUID.
// Absence is reported as (zero, false, nil). The returned digest is a snapshot;
// a concurrent/refreshed mutation makes it safely stale.
func (s *CallbackStore) getCloseIntentLocked(leaseUUID string) (CloseIntentClaim, bool, error) {
	if err := validateCanonicalLeaseUUID(leaseUUID); err != nil {
		return CloseIntentClaim{}, false, err
	}
	var claim CloseIntentClaim
	found := false
	err := s.view(func(tx *bolt.Tx) error {
		head, present, err := getLeaseMutationHeadTx(tx, leaseUUID)
		if err != nil || !present {
			return err
		}
		close, ok := head.(closeLeaseMutationHead)
		if !ok {
			return nil
		}
		claim = close.claim
		found = true
		return nil
	})
	return claim, found, err
}

// GetCloseIntent is a read-only diagnostic snapshot. The returned value is
// deliberately not bound to a CloseSettlement and therefore cannot authorize
// cleanup progress, release retirement, or terminal settlement.
func (s *CallbackStore) GetCloseIntent(leaseUUID string) (CloseIntentClaim, bool, error) {
	unlock := s.lockDeliveryLease(leaseUUID)
	defer unlock()
	return s.getCloseIntentLocked(leaseUUID)
}

// ListCloseIntents returns durable recovery capabilities in deterministic
// canonical lease order. Close intents never expire: they are the sole causal
// authority for destructive cleanup after a crash.
func (s *CallbackStore) listCloseIntents() ([]CloseIntentClaim, error) {
	var claims []CloseIntentClaim
	err := s.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
		if bucket == nil {
			return fmt.Errorf("callback lease mutation head bucket missing")
		}
		return bucket.ForEach(func(key, value []byte) error {
			if value == nil {
				return fmt.Errorf("callback lease mutation head %q is a nested bucket", key)
			}
			head, err := decodeLeaseMutationHead(key, value)
			if err != nil {
				return err
			}
			if close, ok := head.(closeLeaseMutationHead); ok {
				claims = append(claims, close.claim)
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(claims, func(left, right CloseIntentClaim) int {
		return strings.Compare(left.LeaseUUID(), right.LeaseUUID())
	})
	return claims, nil
}

// ListCloseIntents returns read-only diagnostic snapshots. Recovery code that
// needs usable capabilities must call CloseSettlement.ListCloseIntents.
func (s *CallbackStore) ListCloseIntents() ([]CloseIntentClaim, error) {
	return s.listCloseIntents()
}

// advanceCloseExecutionGenerationLocked atomically persists the next Started
// generation and returns the only claim current enough to execute or settle it.
// A stale copied claim cannot overwrite this monotonic causal boundary.
func (s *CallbackStore) advanceCloseExecutionGenerationLocked(
	claim CloseIntentClaim,
) (CloseIntentClaim, error) {
	if err := validateCloseIntentClaim(claim); err != nil {
		return CloseIntentClaim{}, err
	}
	var refreshed CloseIntentClaim
	err := s.update(func(tx *bolt.Tx) error {
		if err := verifyCloseIntentTx(tx, claim); err != nil {
			return err
		}
		if claim.entry.ExecutionGeneration == math.MaxInt {
			return fmt.Errorf("callback close intent execution-generation counter exhausted")
		}
		entry := cloneCloseIntentEntry(claim.entry)
		entry.ExecutionGeneration++
		data, err := marshalCloseIntent(entry)
		if err != nil {
			return err
		}
		candidate, err := decodeCloseIntent([]byte(entry.LeaseUUID), data)
		if err != nil {
			return err
		}
		transition, err := newAdvanceCloseLeaseMutation(claim, candidate)
		if err != nil {
			return err
		}
		written, err := applyLeaseMutationTx(tx, transition)
		if err != nil {
			return err
		}
		refreshed = written.(closeLeaseMutationHead).claim
		return nil
	})
	if err != nil {
		return CloseIntentClaim{}, err
	}
	return refreshed, nil
}

// resolveCloseIntentLocked atomically advances one precise close and enqueues its
// lifecycle observation. The exact destroyed or retained outcome replaces
// cleanup authority with an immutable, indefinite closed-lease head.
// Incomplete or ambiguous work never reaches this function, so it necessarily
// retains the close head. Callbackless closes omit only the callback, never the
// fence.
func (s *CallbackStore) resolveCloseIntentLocked(
	claim CloseIntentClaim,
	outcome closeCompletion,
	errMsg string,
) (CallbackEntry, error) {
	if err := validateCloseIntentClaim(claim); err != nil {
		return CallbackEntry{}, err
	}
	var status backend.CallbackStatus
	retained := false
	switch outcome {
	case closeCompletionDestroyed:
		status = backend.CallbackStatusDeprovisioned
	case closeCompletionRetained:
		status = backend.CallbackStatusDeprovisioned
		retained = true
	default:
		return CallbackEntry{}, errors.New("close intent has invalid private completion")
	}

	callbackless := claim.CallbackURL() == "" && claim.LifecycleCallbackURL() == ""
	settledAt := time.Now()
	var entry CallbackEntry
	if !callbackless {
		deliveryID, err := uuid.NewRandom()
		if err != nil {
			return CallbackEntry{}, fmt.Errorf("allocate close callback delivery ID: %w", err)
		}
		entry = CallbackEntry{
			DeliveryID:       deliveryID.String(),
			LeaseUUID:        claim.LeaseUUID(),
			CallbackURL:      claim.LifecycleCallbackURL(),
			DeliveryKind:     CallbackDeliveryKindLifecycle,
			Status:           status,
			Backend:          claim.Backend(),
			BackendStorageID: claim.BackendStorageID().String(),
			Error:            errMsg,
			Retained:         retained,
			CreatedAt:        settledAt,
		}
		if err := validateNewCallbackEntry(entry, settledAt); err != nil {
			return CallbackEntry{}, err
		}
	}

	var data []byte
	err := s.update(func(tx *bolt.Tx) error {
		if err := verifyCloseIntentTx(tx, claim); err != nil {
			return err
		}
		if !callbackless {
			var err error
			entry, data, err = putCallbackEntryTx(tx, entry)
			if err != nil {
				return err
			}
		}
		var transition leaseMutationTransition
		var transitionErr error
		closed, err := newClosedLeaseMutationHead(claim, settledAt)
		if err != nil {
			return err
		}
		transition, transitionErr = newCompleteCloseLeaseMutation(claim, closed)
		if transitionErr != nil {
			return transitionErr
		}
		_, err = applyLeaseMutationTx(tx, transition)
		return err
	})
	if err != nil {
		return CallbackEntry{}, err
	}
	if callbackless {
		return CallbackEntry{}, nil
	}
	entry.storageVersion = callbackStorageV2
	entry.storageLease = entry.LeaseUUID
	entry.storageDeliveryID = entry.DeliveryID
	entry.storageKey = string(callbackSequenceKey(entry.Sequence))
	entry.storageDigest = sha256.Sum256(data)
	s.notifyReplayCommit(entry.LeaseUUID)
	return entry, nil
}

func validateCloseIntentCandidate(candidate closeIntentCandidate) error {
	if err := backendname.Validate(candidate.backend); err != nil {
		return fmt.Errorf("callback close intent backend: %w", err)
	}
	if !candidate.storageID.Valid() {
		return fmt.Errorf("callback close intent requires a valid backend storage identity")
	}
	return validateCloseIntentSpec(candidate.spec)
}

func validateCloseIntentSpec(spec closeIntentSpec) error {
	if err := validateCanonicalLeaseUUID(spec.LeaseUUID); err != nil {
		return err
	}
	if spec.CleanupOnly {
		if spec.Tenant != "" || spec.ProviderUUID != "" {
			return fmt.Errorf("cleanup-only callback close intent cannot carry principal authority")
		}
		if spec.CallbackURL != "" || spec.LifecycleCallbackURL != "" {
			return fmt.Errorf("cleanup-only callback close intent cannot carry a callback pair")
		}
		if spec.RetainOnClose {
			return fmt.Errorf("cleanup-only callback close intent cannot retain volumes")
		}
	} else {
		if err := validateCloseIntentIdentity("tenant", spec.Tenant); err != nil {
			return err
		}
		if err := validateCloseIntentIdentity("provider", spec.ProviderUUID); err != nil {
			return err
		}
	}
	if len(spec.Items) == 0 {
		return fmt.Errorf("callback close intent requires lease items")
	}
	_, err := backend.ValidateOperationQuantities(spec.Items)
	if err != nil {
		return fmt.Errorf("callback close intent quantities: %w", err)
	}
	seenServices := make(map[string]struct{}, len(spec.Items))
	for i, item := range spec.Items {
		if err := validateCloseIntentIdentity(fmt.Sprintf("item %d SKU", i), item.SKU); err != nil {
			return err
		}
		if err := validateCloseIntentIdentity(fmt.Sprintf("item %d service name", i), item.ServiceName); err != nil {
			return err
		}
		if _, exists := seenServices[item.ServiceName]; exists {
			return fmt.Errorf("callback close intent service name %q is duplicated", item.ServiceName)
		}
		seenServices[item.ServiceName] = struct{}{}
	}
	if err := ValidateSKUResourceSnapshot(spec.Items, spec.ResourceProfiles); err != nil {
		return fmt.Errorf("callback close intent resource profiles: %w", err)
	}
	if len(spec.Manifest) == 0 {
		return fmt.Errorf("callback close intent requires its manifest")
	}
	stack, err := manifest.ParsePayload(spec.Manifest)
	if err != nil {
		return fmt.Errorf("callback close intent manifest: %w", err)
	}
	if err := manifest.ValidateStackAgainstItems(stack, spec.Items); err != nil {
		return fmt.Errorf("callback close intent topology: %w", err)
	}

	switch {
	case spec.CallbackURL == "" && spec.LifecycleCallbackURL == "":
		// Explicit callbackless legacy close.
	case spec.CallbackURL == "" || spec.LifecycleCallbackURL == "":
		return fmt.Errorf("callback close intent callback pair must be both present or both empty")
	default:
		if err := validateCallbackDestination(spec.CallbackURL); err != nil {
			return err
		}
		if err := validateCallbackDestination(spec.LifecycleCallbackURL); err != nil {
			return err
		}
		if err := backend.ValidateOperationCallbackURL(spec.CallbackURL); err != nil {
			return fmt.Errorf("callback close intent has invalid operation callback: %w", err)
		}
		resolved, err := backend.ResolveLifecycleCallbackURL(
			spec.CallbackURL, spec.LifecycleCallbackURL,
		)
		if err != nil {
			return fmt.Errorf("callback close intent has invalid callback pair: %w", err)
		}
		if resolved != spec.LifecycleCallbackURL {
			return fmt.Errorf("callback close intent lifecycle callback does not match its operation callback")
		}
	}

	switch {
	case spec.ActiveReleaseVersion < 0:
		return fmt.Errorf("callback close intent active release version cannot be negative")
	case spec.ActiveReleaseVersion == 0 && spec.ActiveReleaseDigest != ([sha256.Size]byte{}):
		return fmt.Errorf("callback close intent release fence must be wholly absent or wholly present")
	case spec.ActiveReleaseVersion > 0 && spec.ActiveReleaseDigest == ([sha256.Size]byte{}):
		return fmt.Errorf("callback close intent release fence must be wholly absent or wholly present")
	}
	if !spec.ActiveReleaseOperationID.IsZero() && !spec.ActiveReleaseOperationID.Valid() {
		return errors.New("callback close intent active release operation ID must be a canonical UUIDv4")
	}
	if spec.ActiveReleaseVersion == 0 && !spec.ActiveReleaseOperationID.IsZero() {
		return errors.New("callback close intent operation lineage requires an active release fence")
	}
	if spec.ActiveReleaseVersion > 0 && spec.CallbackURL != "" &&
		!spec.ActiveReleaseOperationID.IsZero() {
		operationID, err := parseOperationCallbackID(spec.CallbackURL)
		if err != nil {
			return err
		}
		if operationID != spec.ActiveReleaseOperationID {
			return errors.New("callback close intent callback does not match active release operation lineage")
		}
	}
	return nil
}

func validateCloseIntentEntry(entry closeIntentEntry, leaseUUID string) error {
	if !entry.InterruptedOperationID.IsZero() && !entry.InterruptedMaintenanceID.IsZero() {
		return errors.New("close cannot interrupt both an operation and maintenance")
	}
	if _, err := parseCloseIntentID(entry.IntentID); err != nil {
		return err
	}
	if entry.LeaseUUID != leaseUUID {
		return fmt.Errorf("callback close intent lease mismatch: key %q contains %q", leaseUUID, entry.LeaseUUID)
	}
	storageID, err := backendidentity.Parse(entry.BackendStorageID)
	if err != nil {
		return fmt.Errorf("invalid callback close intent storage identity: %w", err)
	}
	activeDigest, err := parseCloseReleaseDigest(entry.ActiveReleaseDigest)
	if err != nil {
		return err
	}
	if entry.ExecutionGeneration < 0 {
		return fmt.Errorf("callback close intent execution generation cannot be negative")
	}
	if err := validateCloseIntentCandidate(closeIntentCandidate{
		spec: closeIntentSpec{
			LeaseUUID:                entry.LeaseUUID,
			Tenant:                   entry.Tenant,
			ProviderUUID:             entry.ProviderUUID,
			Items:                    entry.Items,
			ResourceProfiles:         entry.ResourceProfiles,
			Manifest:                 entry.Manifest,
			CallbackURL:              entry.CallbackURL,
			LifecycleCallbackURL:     entry.LifecycleCallbackURL,
			RetainOnClose:            entry.RetainOnClose,
			CleanupOnly:              entry.CleanupOnly,
			ActiveReleaseVersion:     entry.ActiveReleaseVersion,
			ActiveReleaseDigest:      activeDigest,
			ActiveReleaseOperationID: entry.ActiveReleaseOperationID,
		},
		backend:   entry.Backend,
		storageID: storageID,
	}); err != nil {
		return err
	}
	return validateStoredCallbackCreatedAt(entry.CreatedAt)
}

func validateCloseIntentClaim(claim CloseIntentClaim) error {
	if claim.digest == ([sha256.Size]byte{}) || claim.intentID == uuid.Nil {
		return fmt.Errorf("callback close intent claim has no durable capability")
	}
	if !claim.storageID.Valid() || claim.storageID.String() != claim.entry.BackendStorageID {
		return fmt.Errorf("callback close intent claim has invalid storage authority")
	}
	if claim.intentID.String() != claim.entry.IntentID {
		return fmt.Errorf("callback close intent claim has invalid intent authority")
	}
	if encodeCloseReleaseDigest(claim.activeReleaseDigest) != claim.entry.ActiveReleaseDigest {
		return fmt.Errorf("callback close intent claim has invalid release authority")
	}
	return validateCloseIntentEntry(claim.entry, claim.entry.LeaseUUID)
}

func decodeCloseIntent(key, value []byte) (CloseIntentClaim, error) {
	var entry closeIntentEntry
	if err := decodeStrictAuthoritativeObject(value, maxCloseIntentEntryBytes, &entry); err != nil {
		return CloseIntentClaim{}, fmt.Errorf("decode callback close intent %q: %w", key, err)
	}
	if entry.ActiveReleaseOperationID.IsZero() && entry.ActiveReleaseVersion > 0 && entry.CallbackURL != "" {
		// Compatibility with close rows written before the active release's
		// operation lineage was stored separately from its callback pair.
		operationID, err := parseOperationCallbackID(entry.CallbackURL)
		if err != nil {
			return CloseIntentClaim{}, fmt.Errorf("decode callback close intent %q lineage: %w", key, err)
		}
		entry.ActiveReleaseOperationID = operationID
	}
	if err := validateCloseIntentEntry(entry, string(key)); err != nil {
		return CloseIntentClaim{}, fmt.Errorf("invalid callback close intent %q: %w", key, err)
	}
	intentID, _ := parseCloseIntentID(entry.IntentID)
	storageID, _ := backendidentity.Parse(entry.BackendStorageID)
	activeDigest, _ := parseCloseReleaseDigest(entry.ActiveReleaseDigest)
	entry = cloneCloseIntentEntry(entry)
	return CloseIntentClaim{
		entry:               entry,
		intentID:            intentID,
		storageID:           storageID,
		activeReleaseDigest: activeDigest,
		digest:              sha256.Sum256(value),
	}, nil
}

func marshalCloseIntent(entry closeIntentEntry) ([]byte, error) {
	data, err := json.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("marshal callback close intent: %w", err)
	}
	if len(data) > maxCloseIntentEntryBytes {
		return nil, fmt.Errorf("callback close intent exceeds %d bytes", maxCloseIntentEntryBytes)
	}
	return data, nil
}

func verifyCloseIntentTx(tx *bolt.Tx, claim CloseIntentClaim) error {
	head, present, err := getLeaseMutationHeadTx(tx, claim.LeaseUUID())
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("callback close intent no longer exists for lease %q", claim.LeaseUUID())
	}
	close, ok := head.(closeLeaseMutationHead)
	if !ok {
		return fmt.Errorf("callback close intent for lease %q was replaced by %q",
			claim.LeaseUUID(), head.headKind())
	}
	if close.claim.digest != claim.digest {
		return fmt.Errorf("callback close intent changed before precise mutation")
	}
	return nil
}

// requireCloseIntent re-attests one exact close while its settlement owns the
// per-lease transition gate.
func (s *CallbackStore) requireCloseIntent(claim CloseIntentClaim) error {
	return s.view(func(tx *bolt.Tx) error { return verifyCloseIntentTx(tx, claim) })
}

func closeIntentEntryMatchesSpec(left, right closeIntentEntry) bool {
	return left.LeaseUUID == right.LeaseUUID &&
		left.Backend == right.Backend &&
		left.BackendStorageID == right.BackendStorageID &&
		left.Tenant == right.Tenant &&
		left.ProviderUUID == right.ProviderUUID &&
		slices.Equal(left.Items, right.Items) &&
		slices.Equal(left.ResourceProfiles, right.ResourceProfiles) &&
		bytes.Equal(left.Manifest, right.Manifest) &&
		left.CallbackURL == right.CallbackURL &&
		left.LifecycleCallbackURL == right.LifecycleCallbackURL &&
		left.RetainOnClose == right.RetainOnClose &&
		left.CleanupOnly == right.CleanupOnly &&
		left.ActiveReleaseVersion == right.ActiveReleaseVersion &&
		left.ActiveReleaseDigest == right.ActiveReleaseDigest &&
		left.ActiveReleaseOperationID == right.ActiveReleaseOperationID
}

func cloneCloseIntentEntry(entry closeIntentEntry) closeIntentEntry {
	entry.Items = slices.Clone(entry.Items)
	entry.ResourceProfiles = CloneSKUResourceSnapshot(entry.ResourceProfiles)
	entry.Manifest = bytes.Clone(entry.Manifest)
	return entry
}

func parseCloseIntentID(value string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil || id.String() != value || id.Version() != uuid.Version(4) || id.Variant() != uuid.RFC4122 {
		return uuid.Nil, fmt.Errorf("callback close intent ID must be a canonical UUIDv4: %q", value)
	}
	return id, nil
}

func parseCloseReleaseDigest(value string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if value == "" {
		return digest, nil
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != value {
		return digest, fmt.Errorf("callback close intent active release digest must be canonical SHA-256")
	}
	copy(digest[:], decoded)
	if digest == ([sha256.Size]byte{}) {
		return digest, fmt.Errorf("callback close intent absent release digest must use an empty encoding")
	}
	return digest, nil
}

func encodeCloseReleaseDigest(digest [sha256.Size]byte) string {
	if digest == ([sha256.Size]byte{}) {
		return ""
	}
	return hex.EncodeToString(digest[:])
}

func validateCloseIntentIdentity(label, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("callback close intent requires %s", label)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("callback close intent %s must not contain surrounding whitespace", label)
	}
	if len(value) > maxCloseIntentIdentityBytes || !utf8.ValidString(value) {
		return fmt.Errorf("callback close intent %s is invalid or exceeds %d bytes", label, maxCloseIntentIdentityBytes)
	}
	for _, character := range value {
		if !unicode.IsPrint(character) {
			return fmt.Errorf("callback close intent %s contains a non-printable character", label)
		}
	}
	return nil
}

func validateClosePhysicalName(index int, label, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("callback close intent rollback target %d requires %s", index, label)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("callback close intent rollback target %d %s has surrounding whitespace", index, label)
	}
	if len(value) > maxClosePhysicalNameBytes || !utf8.ValidString(value) {
		return fmt.Errorf(
			"callback close intent rollback target %d %s is invalid or exceeds %d bytes",
			index, label, maxClosePhysicalNameBytes,
		)
	}
	for _, character := range value {
		if !unicode.IsPrint(character) {
			return fmt.Errorf("callback close intent rollback target %d %s contains a non-printable character", index, label)
		}
	}
	return nil
}
