package shared

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/backendname"
	"github.com/manifest-network/fred/internal/operationid"
)

const (
	maxOperationIntentEntryBytes = 4 << 20
	// Completed-operation replay fences are deliberately permanent. They retain
	// only exact identity/outcome metadata rather than the potentially large
	// manifest and resource snapshot from the live recovery row.
	maxOperationHistoryEntryBytes = maxOperationIntentEntryBytes + 4<<10
	// A pending row must leave room for every terminal outcome that the
	// callback outbox itself accepts. The error's JSON token is bounded by the
	// complete callback-entry ceiling; the small fixed allowance covers the
	// state/timestamp and settlement_error key growth. Without this reservation,
	// an admitted near-limit intent could be impossible to settle and would keep
	// the lease permanently fenced.
	maxOperationIntentTerminalFixedGrowthBytes = 256
	maxOperationIntentPendingEntryBytes        = maxOperationIntentEntryBytes -
		maxCallbackEntryBytes - maxOperationIntentTerminalFixedGrowthBytes
	maxOperationReceiptsPerLease           = 4_096
	operationCompletionRecordVersion uint8 = 1
)

var callbackOperationHistoryBucketName = []byte("completed_callback_operation_history")

// OperationIntentKind identifies the asynchronous operation whose exact
// completion must survive a backend crash.
type OperationIntentKind string

// OperationID is the opaque operation authority shared by provider and
// backend layers. Its zero value is the explicit tokenless compatibility value
// accepted only where v0.13 records or requests are deliberately supported.
type OperationID = operationid.ID

const (
	OperationIntentProvision OperationIntentKind = "provision"
	OperationIntentRestore   OperationIntentKind = "restore"
)

// ErrOperationIntentConflict means a lease already has an unresolved durable
// operation. The existing intent is preserved; callers must not mutate the
// substrate for the conflicting request.
var ErrOperationIntentConflict = errors.New("unresolved callback operation intent")

// ErrOperationIntentMissing means an exact operation has no durable journal
// state. Manufacturing completion evidence after its write-ahead authority
// disappeared would let a stale worker settle a newer or already-closed lease
// generation, so absence never implies success or failure.
var ErrOperationIntentMissing = errors.New("callback operation intent missing")

// ErrOperationReceiptCapacity identifies a new asynchronous operation that
// cannot reserve its permanent replay fence. Admission fails before any
// substrate mutation, so backend HTTP handlers may safely return the coded
// insufficient-resources verdict that providerd treats as a definitive
// refusal of this exact attempt.
var ErrOperationReceiptCapacity = errors.New("operation receipt capacity exhausted")

// OperationReceiptCapacityError preserves whether the per-lease or global
// journal bound refused admission. It matches both
// [ErrOperationReceiptCapacity] and [backend.ErrInsufficientResources]: the
// first gives operators and tests a precise diagnosis, while the second uses
// the existing backend protocol's coded, pre-side-effect capacity response.
type OperationReceiptCapacityError struct {
	LeaseUUID string
	Limit     uint64
}

func (e *OperationReceiptCapacityError) Error() string {
	if e == nil {
		return ErrOperationReceiptCapacity.Error()
	}
	if e.LeaseUUID != "" {
		return fmt.Sprintf("%s for lease %q (limit %d)",
			ErrOperationReceiptCapacity, e.LeaseUUID, e.Limit)
	}
	return fmt.Sprintf("global %s (limit %d)", ErrOperationReceiptCapacity, e.Limit)
}

func (e *OperationReceiptCapacityError) Unwrap() []error {
	return []error{ErrOperationReceiptCapacity, backend.ErrInsufficientResources}
}

// OperationIntentAdmissionDisposition tells a backend whether it owns the
// newly-created intent and may start work, or whether an exact retry was
// already durably accepted/completed and should receive an idempotent success.
type OperationIntentAdmissionDisposition uint8

const (
	OperationIntentAdmissionNone OperationIntentAdmissionDisposition = iota
	OperationIntentAdmissionCreated
	OperationIntentAdmissionExisting
	OperationIntentAdmissionCompleted
)

// OperationIntentProbe is store-minted exact authority to recognize a
// redelivery before mutable SKU, manifest, or restore-source validation. Its
// storage lineage and issuer are private: callers can ask the journal that
// owns the lineage to mint a probe, but cannot restamp a probe for another
// backend or journal. A positive result authorizes only an idempotent
// acknowledgement, never a new substrate mutation. The zero value is invalid.
type OperationIntentProbe struct {
	issuer      *CallbackStore
	settlement  *OperationSettlement
	leaseUUID   string
	callbackURL string
	backend     string
	storageID   backendidentity.ID
}

func (probe OperationIntentProbe) LeaseUUID() string   { return probe.leaseUUID }
func (probe OperationIntentProbe) CallbackURL() string { return probe.callbackURL }

// newOperationIntentProbe binds an exact replay lookup to this journal's
// verified backend storage lineage. Production callers enter through the
// pair-bound OperationSettlement.
func (s *CallbackStore) newOperationIntentProbe(
	leaseUUID, callbackURL string,
) (OperationIntentProbe, error) {
	backendName, storageID := s.journalBackendIdentity("")
	if backendName == "" || !storageID.Valid() {
		return OperationIntentProbe{}, errors.New(
			"operation intent probe requires an identity-bound callback journal",
		)
	}
	return newOperationIntentProbe(s, leaseUUID, callbackURL, backendName, storageID)
}

func newOperationIntentProbe(
	issuer *CallbackStore,
	leaseUUID, callbackURL, backendName string,
	storageID backendidentity.ID,
) (OperationIntentProbe, error) {
	probe := OperationIntentProbe{
		issuer: issuer, leaseUUID: leaseUUID, callbackURL: callbackURL,
		backend: backendName, storageID: storageID,
	}
	if err := validateOperationIntentProbe(probe); err != nil {
		return OperationIntentProbe{}, err
	}
	return probe, nil
}

type OperationIntentAdmission struct {
	claim       OperationIntentClaim
	disposition OperationIntentAdmissionDisposition
}

// Disposition reports whether this exact operation was newly admitted or is
// an idempotent replay. It is read-only so callers cannot relabel a replay as
// newly-created authority.
func (admission OperationIntentAdmission) Disposition() OperationIntentAdmissionDisposition {
	return admission.disposition
}

// CreatedClaim returns first-dispatch authority only for a newly-created
// intent. Exact pending and completed replays are capability-free; restart
// recovery obtains their durable claims through ListOperationIntents.
func (admission OperationIntentAdmission) CreatedClaim() (OperationIntentClaim, bool) {
	if admission.disposition != OperationIntentAdmissionCreated || admission.claim.entry == nil {
		return OperationIntentClaim{}, false
	}
	return admission.claim, true
}

// OperationRecoveryState is the sealed durable state of one exact asynchronous
// operation. CallbackStore returns the opaque OperationIntentClaim value or
// one of the two private terminal implementations exposed through
// OperationSucceeded and OperationFailed. Private representation fields
// prevent callers from constructing valid proof values or converting one state
// representation into another.
type OperationRecoveryState interface {
	Kind() OperationIntentKind
	OperationID() OperationID
	LeaseUUID() string
	CallbackURL() string
	LifecycleCallbackURL() string
	Backend() string
	BackendStorageID() backendidentity.ID
	Tenant() string
	ProviderUUID() string
	Items() []backend.LeaseItem
	ResourceProfiles() []SKUResourceSnapshot
	EffectiveItems() []backend.LeaseItem
	HealthCheckServices() []string
	Manifest() []byte
	SourceLeaseUUID() string
	SourceGeneration() int
	CreatedAt() time.Time
	operationRecoveryState()
}

// OperationIntentSpec is the immutable evidence needed to classify an
// accepted asynchronous operation after restart.
type OperationIntentSpec struct {
	Kind                 OperationIntentKind
	LeaseUUID            string
	CallbackURL          string
	LifecycleCallbackURL string
	Tenant               string
	ProviderUUID         string
	Items                []backend.LeaseItem
	// ResourceProfiles freezes the resource definition used for admission and
	// substrate creation before either can happen. Operation intents are new in
	// v0.14, so unlike Release there is no deployed legacy format without this
	// authority.
	ResourceProfiles []SKUResourceSnapshot
	// EffectiveItems is the exact item metadata emitted to substrate labels.
	// It may differ from desired Items only where a custom domain was deferred
	// by the DNS-readiness gate.
	EffectiveItems      []backend.LeaseItem
	HealthCheckServices []string
	Manifest            []byte
	SourceLeaseUUID     string
	SourceGeneration    int
}

// OperationIntentCandidate is a store-minted, immutable admission capability.
// The semantic spec is detached from caller-owned buffers, while the private
// issuer and lineage prevent a candidate assembled for one callback journal
// from authorizing mutation through another. The zero value is invalid.
type OperationIntentCandidate struct {
	issuer     *CallbackStore
	settlement *OperationSettlement
	spec       OperationIntentSpec
	backend    string
	storageID  backendidentity.ID
}

// newOperationIntentCandidate validates and detaches the complete operation
// transition, then binds it to this journal's verified backend storage lineage.
// Production callers enter through the pair-bound OperationSettlement.
func (s *CallbackStore) newOperationIntentCandidate(
	spec OperationIntentSpec,
) (OperationIntentCandidate, error) {
	backendName, storageID := s.journalBackendIdentity("")
	if backendName == "" || !storageID.Valid() {
		return OperationIntentCandidate{}, errors.New(
			"operation intent candidate requires an identity-bound callback journal",
		)
	}
	return newOperationIntentCandidate(s, spec, backendName, storageID)
}

func newOperationIntentCandidate(
	issuer *CallbackStore,
	spec OperationIntentSpec,
	backendName string,
	storageID backendidentity.ID,
) (OperationIntentCandidate, error) {
	spec = cloneOperationIntentSpec(spec)
	if len(spec.EffectiveItems) == 0 {
		spec.EffectiveItems = slices.Clone(spec.Items)
	}
	candidate := OperationIntentCandidate{
		issuer: issuer, spec: spec, backend: backendName, storageID: storageID,
	}
	if err := validateOperationIntentCandidate(candidate); err != nil {
		return OperationIntentCandidate{}, err
	}
	return candidate, nil
}

func cloneOperationIntentSpec(spec OperationIntentSpec) OperationIntentSpec {
	spec.Items = slices.Clone(spec.Items)
	spec.ResourceProfiles = CloneSKUResourceSnapshot(spec.ResourceProfiles)
	spec.EffectiveItems = slices.Clone(spec.EffectiveItems)
	spec.HealthCheckServices = slices.Clone(spec.HealthCheckServices)
	spec.Manifest = bytes.Clone(spec.Manifest)
	return spec
}

// operationAuthority is the immutable identity shared by pending and terminal
// states. Private implementations embed it to share the read-only authority API.
type operationAuthority struct {
	entry     *operationIntentEntry
	storageID backendidentity.ID
	digest    [sha256.Size]byte
}

// OperationIntentClaim is an opaque, precise capability for resolving one
// pending durable intent. Its fields are private and its zero value is invalid,
// eliminating nil and typed-nil interface states while preventing callers from
// relabelling a terminal outcome as pending authority.
type OperationIntentClaim struct {
	operationAuthority
	settlement *OperationSettlement
	_          operationIntentClaimState
}

// OperationExecutionPhase is the durable side-effect boundary of a pending
// operation. The journal, rather than a caller-supplied boolean, is the only
// source of this fact. Recovery may refuse an operation in BeforeEffects
// without inspecting Docker because no substrate capability was ever issued;
// Started must be treated conservatively until exact substrate evidence is
// recovered.
type OperationExecutionPhase uint8

const (
	operationExecutionPhaseInvalid OperationExecutionPhase = iota
	OperationExecutionStarted
	OperationExecutionBeforeEffects
)

// OperationSucceeded is durable proof that an exact operation settled
// successfully. It remains queryable after its callback leaves the outbox.
type OperationSucceeded interface {
	OperationRecoveryState
	SettledAt() time.Time
	operationSucceeded()
}

// OperationFailed is durable proof that an exact operation settled as failed.
// It remains queryable after its callback leaves the outbox.
type OperationFailed interface {
	OperationRecoveryState
	SettledAt() time.Time
	Error() string
	operationFailed()
}

// FailedOperationReceipt is the opaque, immutable teardown identity of an
// operation that settled Failed. Unlike a live recovery state it intentionally
// carries no manifest or resource payload: a later generation may own the
// lease, so consumers may remove only substrate objects whose complete labels
// match this exact historical authority. Its zero value is invalid, and its
// private fields eliminate nil-interface authority states.
type FailedOperationReceipt struct {
	record    operationCompletionRecord
	storageID backendidentity.ID
	issuer    *CallbackStore
}

func (r FailedOperationReceipt) Kind() OperationIntentKind { return r.record.Kind }
func (r FailedOperationReceipt) OperationID() OperationID  { return r.record.OperationID }
func (r FailedOperationReceipt) LeaseUUID() string         { return r.record.LeaseUUID }
func (r FailedOperationReceipt) CallbackURL() string       { return r.record.CallbackURL }
func (r FailedOperationReceipt) LifecycleCallbackURL() string {
	return r.record.LifecycleCallbackURL
}
func (r FailedOperationReceipt) Backend() string                      { return r.record.Backend }
func (r FailedOperationReceipt) BackendStorageID() backendidentity.ID { return r.storageID }
func (r FailedOperationReceipt) Tenant() string                       { return r.record.Tenant }
func (r FailedOperationReceipt) ProviderUUID() string                 { return r.record.ProviderUUID }
func (r FailedOperationReceipt) SettledAt() time.Time                 { return r.record.SettledAt }
func (r FailedOperationReceipt) Error() string                        { return r.record.SettlementError }

// Distinct private marker types keep the concrete representations themselves
// non-convertible in addition to keeping their names out of the exported API.
// Blank zero-sized fields make the distinction structural without adding
// mutable state or a field that production code can read.
type operationIntentClaimState struct{}
type operationSucceededState struct{}
type operationFailedState struct{}

type operationSucceeded struct {
	operationAuthority
	_ operationSucceededState
}
type operationFailed struct {
	operationAuthority
	_ operationFailedState
}

func (OperationIntentClaim) operationRecoveryState() {}
func (operationSucceeded) operationRecoveryState()   {}
func (operationSucceeded) operationSucceeded()       {}
func (operationFailed) operationRecoveryState()      {}
func (operationFailed) operationFailed()             {}

var (
	_ OperationRecoveryState = OperationIntentClaim{}
	_ OperationSucceeded     = operationSucceeded{}
	_ OperationFailed        = operationFailed{}
)

func (c OperationIntentClaim) ExecutionPhase() OperationExecutionPhase {
	if c.entry == nil {
		return operationExecutionPhaseInvalid
	}
	// The inverted durable encoding makes an omitted field from an older row
	// conservatively Started. Only a current journal transition may persist the
	// explicit pre-effect state.
	if c.entry.EffectNotStarted {
		return OperationExecutionBeforeEffects
	}
	return OperationExecutionStarted
}

func (c OperationIntentClaim) Valid() bool {
	return c.entry != nil && c.settlement != nil &&
		c.digest != ([sha256.Size]byte{}) && c.storageID.Valid()
}

func (r FailedOperationReceipt) Valid() bool {
	return r.issuer != nil && r.record.OperationID.Valid() &&
		r.record.LeaseUUID != "" && r.storageID.Valid()
}

func (c operationAuthority) Kind() OperationIntentKind {
	if c.entry == nil {
		return ""
	}
	return c.entry.Kind
}
func (c operationAuthority) OperationID() OperationID {
	if c.entry == nil {
		return OperationID{}
	}
	return c.entry.OperationID
}
func (c operationAuthority) LeaseUUID() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.LeaseUUID
}
func (c operationAuthority) CallbackURL() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.CallbackURL
}
func (c operationAuthority) LifecycleCallbackURL() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.LifecycleCallbackURL
}
func (c operationAuthority) Backend() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.Backend
}
func (c operationAuthority) BackendStorageID() backendidentity.ID {
	return c.storageID
}
func (c operationAuthority) Tenant() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.Tenant
}
func (c operationAuthority) ProviderUUID() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.ProviderUUID
}
func (c operationAuthority) Items() []backend.LeaseItem {
	if c.entry == nil {
		return nil
	}
	return slices.Clone(c.entry.Items)
}
func (c operationAuthority) ResourceProfiles() []SKUResourceSnapshot {
	if c.entry == nil {
		return nil
	}
	return CloneSKUResourceSnapshot(c.entry.ResourceProfiles)
}
func (c operationAuthority) EffectiveItems() []backend.LeaseItem {
	if c.entry == nil {
		return nil
	}
	return slices.Clone(c.entry.EffectiveItems)
}
func (c operationAuthority) HealthCheckServices() []string {
	if c.entry == nil {
		return nil
	}
	return slices.Clone(c.entry.HealthCheckServices)
}
func (c operationAuthority) Manifest() []byte {
	if c.entry == nil {
		return nil
	}
	return slices.Clone(c.entry.Manifest)
}
func (c operationAuthority) SourceLeaseUUID() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.SourceLeaseUUID
}
func (c operationAuthority) SourceGeneration() int {
	if c.entry == nil {
		return 0
	}
	return c.entry.SourceGeneration
}
func (c operationAuthority) CreatedAt() time.Time {
	if c.entry == nil {
		return time.Time{}
	}
	return c.entry.CreatedAt
}

// SettledAt is the durable settlement timestamp. It is non-zero for both
// terminal state types.
func (c operationSucceeded) SettledAt() time.Time {
	if c.entry == nil {
		return time.Time{}
	}
	return c.entry.SettledAt
}
func (c operationFailed) SettledAt() time.Time {
	if c.entry == nil {
		return time.Time{}
	}
	return c.entry.SettledAt
}

// Error is the exact durable failure attached to the operation callback.
func (c operationFailed) Error() string {
	if c.entry == nil {
		return ""
	}
	return c.entry.SettlementError
}

type operationIntentState string

const (
	operationIntentPending   operationIntentState = "pending"
	operationIntentSucceeded operationIntentState = "succeeded"
	operationIntentFailed    operationIntentState = "failed"
)

type operationIntentEntry struct {
	IntentID             string                `json:"intent_id"`
	OperationID          OperationID           `json:"operation_id,omitzero"`
	Kind                 OperationIntentKind   `json:"kind"`
	LeaseUUID            string                `json:"lease_uuid"`
	CallbackURL          string                `json:"callback_url"`
	LifecycleCallbackURL string                `json:"lifecycle_callback_url"`
	Backend              string                `json:"backend"`
	BackendStorageID     string                `json:"backend_storage_id"`
	Tenant               string                `json:"tenant"`
	ProviderUUID         string                `json:"provider_uuid"`
	Items                []backend.LeaseItem   `json:"items"`
	ResourceProfiles     []SKUResourceSnapshot `json:"resource_profiles"`
	EffectiveItems       []backend.LeaseItem   `json:"effective_items,omitempty"`
	HealthCheckServices  []string              `json:"health_check_services,omitempty"`
	Manifest             []byte                `json:"manifest,omitempty"`
	SourceLeaseUUID      string                `json:"source_lease_uuid,omitempty"`
	SourceGeneration     int                   `json:"source_generation,omitempty"`
	// EffectNotStarted is true only between durable admission and the
	// irreversible execution-start transition. The inverted encoding is
	// intentional: operation rows written by an older process omit the field
	// and are therefore recovered conservatively as already started.
	EffectNotStarted   bool                              `json:"effect_not_started,omitempty"`
	CreatedAt          time.Time                         `json:"created_at"`
	State              operationIntentState              `json:"state,omitempty"`
	SettledAt          time.Time                         `json:"settled_at,omitempty"`
	SettlementError    string                            `json:"settlement_error,omitempty"`
	FailurePredecessor operationFailurePredecessorRecord `json:"failure_predecessor,omitzero"`
}

// operationCompletionRecord is the compact, permanent replay identity archived
// when a later durable transition supersedes the rich terminal recovery row.
// Callback delivery never removes it and it is never subject to TTL cleanup.
type operationCompletionRecord struct {
	Version              uint8                `json:"version"`
	IntentID             string               `json:"intent_id"`
	OperationID          OperationID          `json:"operation_id,omitzero"`
	Kind                 OperationIntentKind  `json:"kind"`
	LeaseUUID            string               `json:"lease_uuid"`
	CallbackURL          string               `json:"callback_url"`
	LifecycleCallbackURL string               `json:"lifecycle_callback_url"`
	Backend              string               `json:"backend"`
	BackendStorageID     string               `json:"backend_storage_id"`
	Tenant               string               `json:"tenant"`
	ProviderUUID         string               `json:"provider_uuid"`
	CreatedAt            time.Time            `json:"created_at"`
	State                operationIntentState `json:"state"`
	SettledAt            time.Time            `json:"settled_at"`
	SettlementError      string               `json:"settlement_error,omitempty"`
}

// probeOperationIntent recognizes an already-accepted or already-completed
// exact operation without requiring the original semantic inputs to remain
// valid. This is what makes provider-side redelivery safe after a SKU was
// removed or a completed restore deleted its source retention record.
func (s *CallbackStore) probeOperationIntent(
	probe OperationIntentProbe,
) (OperationIntentAdmissionDisposition, error) {
	if probe.issuer != s || s == nil {
		return OperationIntentAdmissionNone, errors.New(
			"operation intent probe was not minted by this callback journal",
		)
	}
	if err := validateOperationIntentProbe(probe); err != nil {
		return OperationIntentAdmissionNone, err
	}
	probeOperationID, err := parseOperationCallbackID(probe.callbackURL)
	if err != nil {
		return OperationIntentAdmissionNone, err
	}

	unlock := s.lockDeliveryLease(probe.leaseUUID)
	defer unlock()
	disposition := OperationIntentAdmissionNone
	err = s.view(func(tx *bolt.Tx) error {
		head, present, err := getLeaseMutationHeadTx(tx, probe.leaseUUID)
		if err != nil {
			return err
		}
		var current *OperationIntentClaim
		if operation, ok := head.(operationLeaseMutationHead); ok {
			claim := operation.claim
			current = &claim
		}
		history, err := listOperationHistoryTx(tx, probe.leaseUUID)
		if err != nil {
			return err
		}
		if current != nil && len(history) != 0 &&
			!operationHistoryMatchesAuthority(history[0], *current.entry) {
			return fmt.Errorf("operation state for lease %q crosses completed-history authority", probe.leaseUUID)
		}
		for _, completed := range history {
			sameOperation := probeOperationID.Valid() && completed.OperationID == probeOperationID
			sameCallback := completed.CallbackURL == probe.callbackURL
			if !sameOperation && !sameCallback {
				continue
			}
			if (!sameOperation && probeOperationID.Valid()) || !sameCallback {
				return fmt.Errorf("%w for lease %q: completed operation identity diverges",
					ErrOperationIntentConflict, probe.leaseUUID)
			}
			if completed.Backend != probe.backend ||
				completed.BackendStorageID != probe.storageID.String() {
				return fmt.Errorf("%w for lease %q", ErrOperationIntentConflict, probe.leaseUUID)
			}
			disposition = OperationIntentAdmissionCompleted
			return nil
		}
		if current != nil {
			claim := *current
			if claim.Backend() != probe.backend ||
				claim.BackendStorageID() != probe.storageID {
				return fmt.Errorf("%w for lease %q", ErrOperationIntentConflict, probe.leaseUUID)
			}
			if claim.CallbackURL() == probe.callbackURL && claim.entry.State != operationIntentPending {
				disposition = OperationIntentAdmissionCompleted
				return nil
			}
			if claim.CallbackURL() != probe.callbackURL &&
				probeOperationID.Valid() && claim.OperationID() == probeOperationID {
				return fmt.Errorf("%w for lease %q: current operation identity diverges",
					ErrOperationIntentConflict, probe.leaseUUID)
			}
		}

		if present {
			switch state := head.(type) {
			case closedLeaseMutationHead:
				if closedLeaseTombstoneMatchesProbe(state.entry, probe) {
					disposition = OperationIntentAdmissionCompleted
					return nil
				}
				return fmt.Errorf("%w for lease %q: lease is permanently closed",
					ErrOperationIntentConflict, probe.leaseUUID)
			case closeLeaseMutationHead:
				return fmt.Errorf("%w for lease %q: close is already admitted",
					ErrOperationIntentConflict, probe.leaseUUID)
			case maintenanceLeaseMutationHead:
				return fmt.Errorf("%w for lease %q: maintenance is already admitted",
					ErrOperationIntentConflict, probe.leaseUUID)
			}
		}
		if current != nil {
			claim := *current
			if claim.CallbackURL() == probe.callbackURL {
				disposition = OperationIntentAdmissionExisting
				return nil
			}
			if claim.entry.State == operationIntentPending {
				return fmt.Errorf("%w for lease %q", ErrOperationIntentConflict, probe.leaseUUID)
			}
			// A different callback may represent a successor to terminal history.
			// Continue through the FIFO check: admission is safe only after every
			// earlier exact completion has left the outbox.
		}
		if len(history) != 0 && (history[0].Backend != probe.backend ||
			history[0].BackendStorageID != probe.storageID.String()) {
			return fmt.Errorf("%w for lease %q", ErrOperationIntentConflict, probe.leaseUUID)
		}

		pending, err := listPendingCallbackEntriesTx(tx, probe.leaseUUID)
		if err != nil {
			return err
		}
		for _, callback := range pending {
			if callback.DeliveryKind == CallbackDeliveryKindLifecycle {
				continue
			}
			if callback.CallbackURL != probe.callbackURL ||
				callback.Backend != probe.backend ||
				callback.BackendStorageID != probe.storageID.String() {
				return fmt.Errorf("%w for lease %q: an earlier operation completion is pending",
					ErrOperationIntentConflict, probe.leaseUUID)
			}
			disposition = OperationIntentAdmissionCompleted
		}
		return nil
	})
	return disposition, err
}

// beginOperationIntent durably records a store-minted operation before its
// first external side effect. bbolt's default synchronous commit is the
// acceptance barrier.
func (s *CallbackStore) beginOperationIntent(
	candidate OperationIntentCandidate,
) (OperationIntentAdmission, error) {
	if candidate.issuer != s || s == nil {
		return OperationIntentAdmission{}, errors.New(
			"operation intent candidate was not minted by this callback journal",
		)
	}
	if err := validateOperationIntentCandidate(candidate); err != nil {
		return OperationIntentAdmission{}, err
	}
	spec := cloneOperationIntentSpec(candidate.spec)
	operationID, err := parseOperationCallbackID(spec.CallbackURL)
	if err != nil {
		return OperationIntentAdmission{}, err
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return OperationIntentAdmission{}, fmt.Errorf("allocate callback operation intent ID: %w", err)
	}
	entry := operationIntentEntry{
		IntentID:             id.String(),
		OperationID:          operationID,
		Kind:                 spec.Kind,
		LeaseUUID:            spec.LeaseUUID,
		CallbackURL:          spec.CallbackURL,
		LifecycleCallbackURL: spec.LifecycleCallbackURL,
		Backend:              candidate.backend,
		BackendStorageID:     candidate.storageID.String(),
		Tenant:               spec.Tenant,
		ProviderUUID:         spec.ProviderUUID,
		Items:                slices.Clone(spec.Items),
		ResourceProfiles:     CloneSKUResourceSnapshot(spec.ResourceProfiles),
		EffectiveItems:       slices.Clone(spec.EffectiveItems),
		HealthCheckServices:  slices.Clone(spec.HealthCheckServices),
		Manifest:             slices.Clone(spec.Manifest),
		SourceLeaseUUID:      spec.SourceLeaseUUID,
		SourceGeneration:     spec.SourceGeneration,
		EffectNotStarted:     true,
		CreatedAt:            time.Now(),
		State:                operationIntentPending,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return OperationIntentAdmission{}, fmt.Errorf("marshal callback operation intent: %w", err)
	}

	unlock := s.lockDeliveryLease(entry.LeaseUUID)
	defer unlock()
	admission := OperationIntentAdmission{}
	err = s.update(func(tx *bolt.Tx) error {
		head, present, err := getLeaseMutationHeadTx(tx, entry.LeaseUUID)
		if err != nil {
			return err
		}
		history, historyErr := listOperationHistoryTx(tx, entry.LeaseUUID)
		if historyErr != nil {
			return historyErr
		}
		var currentClaim *OperationIntentClaim
		if operation, ok := head.(operationLeaseMutationHead); ok {
			claim := operation.claim
			currentClaim = &claim
			if len(history) != 0 && !operationHistoryMatchesAuthority(history[0], *claim.entry) {
				return fmt.Errorf("operation state for lease %q crosses completed-history authority", entry.LeaseUUID)
			}
			if operationIntentEntriesEqual(*claim.entry, entry) {
				if claim.entry.State != operationIntentPending {
					admission = OperationIntentAdmission{disposition: OperationIntentAdmissionCompleted}
					return nil
				}
			}
		}
		// Begin is mutation authority, not a replay acknowledgement. Once close
		// has committed its permanent tombstone, every Begin must fail even when
		// an exact historical completion would otherwise be recognizable. Probe
		// remains the only API that acknowledges exact closed-lease redelivery.
		if present {
			switch head.(type) {
			case closedLeaseMutationHead:
				return fmt.Errorf("%w for lease %q: lease is permanently closed",
					ErrOperationIntentConflict, entry.LeaseUUID)
			case closeLeaseMutationHead:
				return fmt.Errorf("%w for lease %q: close is already admitted",
					ErrOperationIntentConflict, entry.LeaseUUID)
			case maintenanceLeaseMutationHead:
				return fmt.Errorf("%w for lease %q: maintenance is already admitted",
					ErrOperationIntentConflict, entry.LeaseUUID)
			}
		}
		for _, completed := range history {
			sameOperation := entry.OperationID.Valid() && completed.OperationID == entry.OperationID
			sameCallback := completed.CallbackURL == entry.CallbackURL
			if !sameOperation && !sameCallback {
				continue
			}
			if (!sameOperation && entry.OperationID.Valid()) || !sameCallback {
				return fmt.Errorf("%w for lease %q: completed operation identity diverges",
					ErrOperationIntentConflict, entry.LeaseUUID)
			}
			if !operationHistoryMatchesEntry(completed, entry) {
				return fmt.Errorf("%w for lease %q: completed operation has different authority",
					ErrOperationIntentConflict, entry.LeaseUUID)
			}
			admission = OperationIntentAdmission{disposition: OperationIntentAdmissionCompleted}
			return nil
		}
		if len(history) != 0 && !operationHistoryMatchesAuthority(history[0], entry) {
			return fmt.Errorf("%w for lease %q: completed history has different backend storage or principal authority",
				ErrOperationIntentConflict, entry.LeaseUUID)
		}

		if currentClaim != nil {
			claim := *currentClaim
			if operationIntentEntriesEqual(*claim.entry, entry) {
				admission = OperationIntentAdmission{disposition: OperationIntentAdmissionExisting}
				return nil
			}
			if claim.entry.State == operationIntentPending || claim.CallbackURL() == entry.CallbackURL {
				return fmt.Errorf("%w for lease %q", ErrOperationIntentConflict, entry.LeaseUUID)
			}
			if entry.OperationID.Valid() && claim.OperationID() == entry.OperationID {
				return fmt.Errorf("%w for lease %q: current operation identity diverges",
					ErrOperationIntentConflict, entry.LeaseUUID)
			}
			if claim.Backend() != entry.Backend ||
				claim.BackendStorageID().String() != entry.BackendStorageID ||
				claim.Tenant() != entry.Tenant ||
				claim.ProviderUUID() != entry.ProviderUUID {
				return fmt.Errorf(
					"%w for lease %q: terminal predecessor has different backend storage or principal authority",
					ErrOperationIntentConflict, entry.LeaseUUID,
				)
			}
			// A different operation may supersede terminal history only after
			// the earlier operation callback has left the outbox and only within
			// the same backend-storage and tenant/provider lineage. The archive and
			// Put below commit atomically, so the older replay identity is never lost.
		}
		pending, pendingErr := listPendingCallbackEntriesTx(tx, entry.LeaseUUID)
		if pendingErr != nil {
			return pendingErr
		}
		exactCompletion := false
		for _, callback := range pending {
			if callback.DeliveryKind == CallbackDeliveryKindLifecycle {
				continue
			}
			if callback.CallbackURL == entry.CallbackURL &&
				callback.Backend == entry.Backend &&
				callback.BackendStorageID == entry.BackendStorageID {
				exactCompletion = true
				continue
			}
			return fmt.Errorf("%w for lease %q: an earlier operation completion is pending",
				ErrOperationIntentConflict, entry.LeaseUUID)
		}
		if exactCompletion {
			admission = OperationIntentAdmission{disposition: OperationIntentAdmissionCompleted}
			return nil
		}
		if len(data) > maxOperationIntentPendingEntryBytes {
			return fmt.Errorf(
				"callback operation intent exceeds %d-byte pending admission budget (reserves terminal settlement headroom)",
				maxOperationIntentPendingEntryBytes,
			)
		}
		claimCandidate := OperationIntentClaim{operationAuthority: operationAuthority{
			entry: &entry, storageID: candidate.storageID, digest: sha256.Sum256(data),
		}}
		var transition leaseMutationTransition
		var transitionErr error
		if currentClaim != nil {
			transition, transitionErr = newReplaceOperationLeaseMutation(*currentClaim, claimCandidate)
		} else {
			transition, transitionErr = newPublishOperationLeaseMutation(claimCandidate)
		}
		if transitionErr != nil {
			return transitionErr
		}
		written, err := applyLeaseMutationTx(tx, transition)
		if err != nil {
			return err
		}
		admission = OperationIntentAdmission{
			claim:       written.(operationLeaseMutationHead).claim,
			disposition: OperationIntentAdmissionCreated,
		}
		return nil
	})
	if err != nil {
		return OperationIntentAdmission{}, err
	}
	return admission, nil
}

// listOperationIntents returns pending recovery capabilities in deterministic
// lease order. Terminal outcomes remain durable but are deliberately a
// different type and therefore cannot be accidentally resolved again.
func (s *CallbackStore) listOperationIntents() ([]OperationIntentClaim, error) {
	var claims []OperationIntentClaim
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
			operation, ok := head.(operationLeaseMutationHead)
			if !ok {
				return nil
			}
			claim := operation.claim
			if claim.entry.State != operationIntentPending {
				return nil
			}
			claims = append(claims, claim)
			return nil
		})
	})
	return claims, err
}

// listOperationRecoveryStates returns every rich operation head, including
// terminal failures that still fence daemon-side late substrate creation. A
// successor transition may retire a terminal head only after its immutable
// replay receipt is committed by the aggregate state machine.
func (s *CallbackStore) listOperationRecoveryStates() ([]OperationRecoveryState, error) {
	var states []OperationRecoveryState
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
			operation, ok := head.(operationLeaseMutationHead)
			if !ok {
				return nil
			}
			switch operation.claim.entry.State {
			case operationIntentPending:
				states = append(states, operation.claim)
			case operationIntentSucceeded:
				states = append(states, operationSucceeded{operationAuthority: operation.claim.operationAuthority})
			case operationIntentFailed:
				states = append(states, operationFailed{operationAuthority: operation.claim.operationAuthority})
			default:
				return fmt.Errorf("operation intent for lease %q has invalid state %q",
					key, operation.claim.entry.State)
			}
			return nil
		})
	})
	return states, err
}

// listFailedOperationReceipts returns permanent failure fences in canonical
// lease and receipt-key order. The receipt written while a Failed head is
// current is the same record retained after any successor transition, so each
// failed operation appears exactly once.
func (s *CallbackStore) listFailedOperationReceipts() ([]FailedOperationReceipt, error) {
	var receipts []FailedOperationReceipt
	err := s.view(func(tx *bolt.Tx) error {
		root := tx.Bucket(callbackOperationHistoryBucketName)
		if root == nil {
			return errors.New("completed operation history bucket missing")
		}
		return root.ForEach(func(leaseKey, value []byte) error {
			if value != nil {
				return fmt.Errorf("completed operation history %q is not a nested bucket", leaseKey)
			}
			history, err := listOperationHistoryTx(tx, string(leaseKey))
			if err != nil {
				return err
			}
			for _, record := range history {
				if record.State != operationIntentFailed {
					continue
				}
				storageID, err := backendidentity.Parse(record.BackendStorageID)
				if err != nil {
					return err
				}
				receipts = append(receipts, FailedOperationReceipt{
					record: record, storageID: storageID, issuer: s,
				})
			}
			return nil
		})
	})
	return receipts, err
}

// lookupOperationRecovery returns the exact sealed durable state for a lease.
// Missing evidence is an error rather than an implicit outcome: recovery must
// never turn journal absence into permission to commit or destroy substrate.
func (s *CallbackStore) lookupOperationRecovery(
	probe OperationIntentProbe,
) (OperationRecoveryState, error) {
	if probe.issuer != s || s == nil {
		return nil, errors.New(
			"operation intent probe was not minted by this callback journal",
		)
	}
	if err := validateOperationIntentProbe(probe); err != nil {
		return nil, err
	}

	unlock := s.lockDeliveryLease(probe.leaseUUID)
	defer unlock()
	var state OperationRecoveryState
	err := s.view(func(tx *bolt.Tx) error {
		head, present, err := getLeaseMutationHeadTx(tx, probe.leaseUUID)
		if err != nil {
			return err
		}
		if !present {
			return fmt.Errorf("%w for lease %q", ErrOperationIntentMissing, probe.leaseUUID)
		}
		operation, ok := head.(operationLeaseMutationHead)
		if !ok {
			return fmt.Errorf("%w for lease %q: aggregate is %q",
				ErrOperationIntentMissing, probe.leaseUUID, head.headKind())
		}
		claim := operation.claim
		if claim.CallbackURL() != probe.callbackURL || claim.Backend() != probe.backend ||
			claim.BackendStorageID() != probe.storageID {
			return fmt.Errorf("%w for lease %q", ErrOperationIntentConflict, probe.leaseUUID)
		}
		switch claim.entry.State {
		case operationIntentPending:
			state = claim
		case operationIntentSucceeded:
			state = operationSucceeded{operationAuthority: claim.operationAuthority}
		case operationIntentFailed:
			state = operationFailed{operationAuthority: claim.operationAuthority}
		default:
			return fmt.Errorf("operation intent for lease %q has invalid state %q",
				probe.leaseUUID, claim.entry.State)
		}
		return nil
	})
	return state, err
}

// resolveOperationSuccess atomically transitions one precise pending intent to
// success and appends its exact callback. Success is representable only after
// the caller presents the opaque proof returned by the paired release commit
// (or reconstructed from that exact active release after restart). Production
// semantic publication crosses CallbackPublisher, which performs storage
// re-attestation and notifies the transport after this transaction commits.
func (s *OperationSettlement) resolveOperationSuccess(
	committed OperationReleaseCommitted,
) (CallbackEntry, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return CallbackEntry{}, errors.New("operation settlement is invalid")
	}
	if committed.settlement != s || committed.callbacks != s.callbacks || committed.releases != s.releases {
		return CallbackEntry{}, errors.New("committed release proof belongs to another journal pair")
	}
	if !committed.Valid() {
		return CallbackEntry{}, errors.New("operation success requires a committed release proof")
	}
	durableClaim := OperationIntentClaim{operationAuthority: cloneOperationAuthority(committed.authority)}
	unlock := s.callbacks.lockDeliveryLease(durableClaim.LeaseUUID())
	defer unlock()
	return s.resolveOperationSuccessLocked(durableClaim, committed)
}

func (s *OperationSettlement) resolveOperationSuccessLocked(
	claim OperationIntentClaim,
	committed OperationReleaseCommitted,
) (CallbackEntry, error) {
	if committed.settlement != s || committed.callbacks != s.callbacks || committed.releases != s.releases {
		return CallbackEntry{}, errors.New("committed release proof belongs to another journal pair")
	}
	if err := s.callbacks.requireCurrentOperationClaim(claim); err != nil {
		return CallbackEntry{}, err
	}
	if err := validateOperationReleaseCommit(s.callbacks, claim, committed); err != nil {
		return CallbackEntry{}, err
	}
	entry := operationSuccessCallbackEntry(*claim.entry)
	return s.callbacks.resolveOperationIntentLocked(
		claim, entry, operationFailurePredecessorRecord{},
	)
}

// resolveOperationFailure atomically transitions one precise pending intent to
// failure and appends its exact callback. It is deliberately separate from the
// success API so a caller-selected status enum cannot bypass the committed
// release proof required by successful settlement. Production semantic
// publication crosses CallbackPublisher.
func (s *OperationSettlement) resolveOperationFailure(
	uncommitted OperationReleaseUncommitted,
	errMsg string,
) (CallbackEntry, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return CallbackEntry{}, errors.New("operation settlement is invalid")
	}
	if uncommitted.settlement != s || uncommitted.callbacks != s.callbacks || uncommitted.releases != s.releases {
		return CallbackEntry{}, errors.New("uncommitted release proof belongs to another journal pair")
	}
	if !uncommitted.Valid() {
		return CallbackEntry{}, errors.New("operation failure requires an uncommitted release proof")
	}
	durableClaim := OperationIntentClaim{
		operationAuthority: cloneOperationAuthority(uncommitted.authority),
		settlement:         s,
	}
	unlock := s.callbacks.lockDeliveryLease(durableClaim.LeaseUUID())
	defer unlock()
	return s.resolveOperationFailureLocked(durableClaim, uncommitted, errMsg)
}

func (s *OperationSettlement) resolveOperationFailureLocked(
	claim OperationIntentClaim,
	uncommitted OperationReleaseUncommitted,
	errMsg string,
) (CallbackEntry, error) {
	if uncommitted.settlement != s || uncommitted.callbacks != s.callbacks || uncommitted.releases != s.releases {
		return CallbackEntry{}, errors.New("uncommitted release proof belongs to another journal pair")
	}
	if err := s.callbacks.requireCurrentOperationClaim(claim); err != nil {
		return CallbackEntry{}, err
	}
	if err := validateOperationReleaseUncommitted(s.callbacks, claim, uncommitted); err != nil {
		return CallbackEntry{}, err
	}
	predecessor, err := s.captureOperationFailurePredecessorLocked(claim)
	if err != nil {
		return CallbackEntry{}, err
	}
	if !predecessor.validFor(s, claim) {
		return CallbackEntry{}, errors.New("failed operation produced invalid predecessor authority")
	}
	entry := operationFailureCallbackEntry(*claim.entry, errMsg)
	return s.callbacks.resolveOperationIntentLocked(claim, entry, predecessor.record())
}

// requireCurrentOperationClaim re-attests that claim is the exact pending head
// of this callback journal. Its caller holds the journal pair's per-lease gate.
func (s *CallbackStore) requireCurrentOperationClaim(claim OperationIntentClaim) error {
	return s.requireCurrentOperationAuthority(claim.operationAuthority)
}

func (s *CallbackStore) requireCurrentOperationAuthority(authority operationAuthority) error {
	if s == nil || authority.entry == nil {
		return errors.New("operation authority is invalid")
	}
	return s.view(func(tx *bolt.Tx) error {
		head, present, err := getLeaseMutationHeadTx(tx, authority.LeaseUUID())
		if err != nil {
			return err
		}
		if !present {
			return fmt.Errorf("%w for lease %q", ErrOperationIntentMissing, authority.LeaseUUID())
		}
		operation, ok := head.(operationLeaseMutationHead)
		if !ok || operation.claim.entry.State != operationIntentPending {
			return fmt.Errorf("%w for lease %q", ErrOperationIntentConflict, authority.LeaseUUID())
		}
		if operation.claim.digest != authority.digest ||
			!operationIntentEntriesEqual(*operation.claim.entry, *authority.entry) {
			return fmt.Errorf("%w for lease %q: pending authority changed",
				ErrOperationIntentConflict, authority.LeaseUUID())
		}
		return nil
	})
}

func (s *CallbackStore) resolveOperationIntentLocked(
	claim OperationIntentClaim,
	entry CallbackEntry,
	failurePredecessor operationFailurePredecessorRecord,
) (CallbackEntry, error) {
	if entry.DeliveryID == "" {
		id, err := uuid.NewRandom()
		if err != nil {
			return CallbackEntry{}, fmt.Errorf("allocate callback delivery ID: %w", err)
		}
		entry.DeliveryID = id.String()
	}
	if err := validateNewCallbackEntry(entry, time.Now()); err != nil {
		return CallbackEntry{}, err
	}
	if err := operationIntentMatchesCallback(*claim.entry, entry); err != nil {
		return CallbackEntry{}, err
	}
	terminal := *claim.entry
	terminal.SettledAt = entry.CreatedAt
	terminal.SettlementError = entry.Error
	if entry.Status == backend.CallbackStatusSuccess {
		if !failurePredecessor.IsZero() {
			return CallbackEntry{}, errors.New("successful operation cannot carry failure predecessor authority")
		}
		terminal.State = operationIntentSucceeded
	} else {
		if err := validateOperationFailurePredecessorRecord(failurePredecessor); err != nil {
			return CallbackEntry{}, err
		}
		terminal.State = operationIntentFailed
		terminal.FailurePredecessor = failurePredecessor
	}
	terminalData, err := marshalOperationIntent(terminal)
	if err != nil {
		return CallbackEntry{}, err
	}
	var data []byte
	err = s.update(func(tx *bolt.Tx) error {
		if err := verifyOperationIntentTx(tx, claim); err != nil {
			return err
		}
		var err error
		entry, data, err = putCallbackEntryTx(tx, entry)
		if err != nil {
			return err
		}
		terminalClaim := OperationIntentClaim{operationAuthority: operationAuthority{
			entry: &terminal, storageID: claim.storageID, digest: sha256.Sum256(terminalData),
		}}
		transition, err := newSettleOperationLeaseMutation(claim, terminalClaim)
		if err != nil {
			return err
		}
		_, err = applyLeaseMutationTx(tx, transition)
		return err
	})
	if err != nil {
		return CallbackEntry{}, err
	}
	entry.storageVersion = callbackStorageV2
	entry.storageLease = entry.LeaseUUID
	entry.storageDeliveryID = entry.DeliveryID
	entry.storageKey = string(callbackSequenceKey(entry.Sequence))
	entry.storageDigest = sha256.Sum256(data)
	s.notifyReplayCommit(entry.LeaseUUID)
	return entry, nil
}

// settleOperationSuccessCallbackLocked and
// settleOperationFailureCallbackLocked are deliberately disjoint. A caller
// with an uncommitted-release proof cannot represent Success, and a caller
// with a committed-release proof cannot attach a failure payload.
func (s *OperationSettlement) settleOperationSuccessCallbackLocked(
	committed OperationReleaseCommitted,
) error {
	if committed.settlement != s || committed.callbacks != s.callbacks ||
		committed.releases != s.releases {
		return errors.New("committed release proof belongs to another journal pair")
	}
	claim := OperationIntentClaim{
		operationAuthority: cloneOperationAuthority(committed.authority),
		settlement:         s,
	}
	if err := s.callbacks.requireCurrentOperationClaim(claim); err != nil {
		return err
	}
	if err := validateOperationReleaseCommit(s.callbacks, claim, committed); err != nil {
		return err
	}
	_, err := s.callbacks.resolveOperationIntentLocked(
		claim, operationSuccessCallbackEntry(*claim.entry), operationFailurePredecessorRecord{},
	)
	return err
}

func (s *OperationSettlement) settleOperationFailureCallbackLocked(
	uncommitted OperationReleaseUncommitted,
	errMsg string,
) error {
	if uncommitted.settlement != s || uncommitted.callbacks != s.callbacks ||
		uncommitted.releases != s.releases {
		return errors.New("uncommitted release proof belongs to another journal pair")
	}
	claim := OperationIntentClaim{
		operationAuthority: cloneOperationAuthority(uncommitted.authority),
		settlement:         s,
	}
	if err := s.callbacks.requireCurrentOperationClaim(claim); err != nil {
		return err
	}
	if err := validateOperationReleaseUncommitted(s.callbacks, claim, uncommitted); err != nil {
		return err
	}
	predecessor, err := s.captureOperationFailurePredecessorLocked(claim)
	if err != nil {
		return err
	}
	if !predecessor.validFor(s, claim) {
		return errors.New("failed operation produced invalid predecessor authority")
	}
	_, err = s.callbacks.resolveOperationIntentLocked(
		claim, operationFailureCallbackEntry(*claim.entry, errMsg), predecessor.record(),
	)
	return err
}

func operationCallbackEntry(intent operationIntentEntry) CallbackEntry {
	return CallbackEntry{
		LeaseUUID:        intent.LeaseUUID,
		CallbackURL:      intent.CallbackURL,
		DeliveryKind:     CallbackDeliveryKindOperation,
		Backend:          intent.Backend,
		BackendStorageID: intent.BackendStorageID,
		CreatedAt:        time.Now(),
	}
}

func operationSuccessCallbackEntry(intent operationIntentEntry) CallbackEntry {
	entry := operationCallbackEntry(intent)
	entry.Status = backend.CallbackStatusSuccess
	return entry
}

func operationFailureCallbackEntry(intent operationIntentEntry, errMsg string) CallbackEntry {
	entry := operationCallbackEntry(intent)
	entry.Status = backend.CallbackStatusFailed
	entry.Error = errMsg
	return entry
}

func validateOperationIntentSpec(spec OperationIntentSpec) error {
	// Manifest is only one field in the encoded row, so a manifest this large
	// necessarily makes the durable intent exceed its total entry budget. Reject
	// it before doing an expensive semantic parse.
	if len(spec.Manifest) >= maxOperationIntentEntryBytes {
		return fmt.Errorf("callback operation intent exceeds %d bytes", maxOperationIntentEntryBytes)
	}
	if err := validateCanonicalLeaseUUID(spec.LeaseUUID); err != nil {
		return err
	}
	if spec.Kind != OperationIntentProvision && spec.Kind != OperationIntentRestore {
		return fmt.Errorf("invalid callback operation intent kind %q", spec.Kind)
	}
	if spec.Tenant == "" || spec.ProviderUUID == "" {
		return fmt.Errorf("callback operation intent requires tenant and provider identities")
	}
	if !backend.IsCanonicalLeaseUUID(spec.ProviderUUID) {
		return fmt.Errorf("callback operation intent provider UUID is not canonical")
	}
	if len(spec.Items) == 0 {
		return fmt.Errorf("callback operation intent requires lease items")
	}
	if err := ValidateSKUResourceSnapshot(spec.Items, spec.ResourceProfiles); err != nil {
		return fmt.Errorf("callback operation intent resource profiles: %w", err)
	}
	if _, err := backend.ValidateOperationQuantities(spec.Items); err != nil {
		return fmt.Errorf("callback operation intent quantities: %w", err)
	}
	if len(spec.Manifest) == 0 {
		return fmt.Errorf("callback operation intent requires its manifest")
	}
	stack, err := manifest.ParsePayload(spec.Manifest)
	if err != nil {
		return fmt.Errorf("callback operation intent manifest: %w", err)
	}
	if err := manifest.ValidateStackAgainstItems(stack, spec.Items); err != nil {
		return fmt.Errorf("callback operation intent manifest topology: %w", err)
	}
	for i, item := range spec.Items {
		if item.SKU == "" || item.ServiceName == "" {
			return fmt.Errorf("callback operation intent item %d requires SKU and service name", i)
		}
	}
	if len(spec.EffectiveItems) != len(spec.Items) {
		return fmt.Errorf("callback operation intent effective items must match desired item count")
	}
	for i, effective := range spec.EffectiveItems {
		desired := spec.Items[i]
		if effective.SKU != desired.SKU || effective.ServiceName != desired.ServiceName ||
			effective.Quantity != desired.Quantity ||
			(effective.CustomDomain != desired.CustomDomain && effective.CustomDomain != "") {
			return fmt.Errorf("callback operation intent effective item %d diverges from desired item", i)
		}
	}
	seenHealthServices := make(map[string]struct{}, len(spec.HealthCheckServices))
	for _, service := range spec.HealthCheckServices {
		if service == "" {
			return fmt.Errorf("callback operation intent health-check service is empty")
		}
		if _, exists := seenHealthServices[service]; exists {
			return fmt.Errorf("callback operation intent health-check service %q is duplicated", service)
		}
		seenHealthServices[service] = struct{}{}
	}
	switch spec.Kind {
	case OperationIntentProvision:
		if spec.SourceLeaseUUID != "" || spec.SourceGeneration != 0 {
			return fmt.Errorf("provision operation intent cannot carry restore source authority")
		}
	case OperationIntentRestore:
		if err := validateCanonicalLeaseUUID(spec.SourceLeaseUUID); err != nil {
			return fmt.Errorf("restore source lease: %w", err)
		}
		if spec.SourceLeaseUUID == spec.LeaseUUID || spec.SourceGeneration <= 0 {
			return fmt.Errorf("restore operation intent requires a distinct source and positive generation")
		}
	}
	if err := validateCallbackDestination(spec.CallbackURL); err != nil {
		return err
	}
	if _, err := backend.ResolveLifecycleCallbackURL(spec.CallbackURL, spec.LifecycleCallbackURL); err != nil {
		return fmt.Errorf("callback operation intent has invalid callback pair: %w", err)
	}
	return nil
}

func validateOperationIntentCandidate(candidate OperationIntentCandidate) error {
	if err := backendname.Validate(candidate.backend); err != nil {
		return fmt.Errorf("callback operation intent candidate backend: %w", err)
	}
	if !candidate.storageID.Valid() {
		return errors.New("callback operation intent candidate has no backend storage authority")
	}
	return validateOperationIntentSpec(candidate.spec)
}

func validateOperationIntentProbe(probe OperationIntentProbe) error {
	if err := validateCanonicalLeaseUUID(probe.leaseUUID); err != nil {
		return err
	}
	if err := backendname.Validate(probe.backend); err != nil {
		return fmt.Errorf("operation intent probe backend: %w", err)
	}
	if !probe.storageID.Valid() {
		return errors.New("operation intent probe has no backend storage authority")
	}
	if err := backend.ValidateOperationCallbackURL(probe.callbackURL); err != nil {
		return err
	}
	return nil
}

func validateOperationIntentClaim(claim OperationIntentClaim) (OperationIntentClaim, error) {
	durableClaim := claim
	if durableClaim.digest == ([sha256.Size]byte{}) ||
		durableClaim.entry == nil || durableClaim.entry.IntentID == "" {
		return OperationIntentClaim{}, fmt.Errorf("callback operation intent claim has no durable capability")
	}
	if !durableClaim.storageID.Valid() ||
		durableClaim.storageID.String() != durableClaim.entry.BackendStorageID {
		return OperationIntentClaim{}, fmt.Errorf("callback operation intent claim has invalid storage authority")
	}
	if err := validateOperationIntentEntry(*durableClaim.entry, durableClaim.entry.LeaseUUID); err != nil {
		return OperationIntentClaim{}, err
	}
	if durableClaim.entry.State != operationIntentPending {
		return OperationIntentClaim{}, fmt.Errorf("callback operation intent claim is terminal, not pending")
	}
	return durableClaim, nil
}

// requireOperationSettlementClaim narrows a structurally valid callback-row
// claim to the exact callback/release coordinator that issued it. Callback
// store decoding deliberately leaves settlement nil; only OperationSettlement
// may bind a recovered row to a currently-open journal pair.
func requireOperationSettlementClaim(
	settlement *OperationSettlement,
	claim OperationIntentClaim,
) (OperationIntentClaim, error) {
	durable, err := validateOperationIntentClaim(claim)
	if err != nil {
		return OperationIntentClaim{}, err
	}
	if settlement == nil || !settlement.valid() {
		return OperationIntentClaim{}, errors.New(
			"callback operation settlement is invalid or closed",
		)
	}
	if durable.settlement != settlement {
		return OperationIntentClaim{}, errors.New(
			"callback operation intent claim belongs to another journal pair",
		)
	}
	return durable, nil
}

func validateOperationIntentEntry(entry operationIntentEntry, leaseUUID string) error {
	id, err := uuid.Parse(entry.IntentID)
	if err != nil || id.String() != entry.IntentID || id.Version() != uuid.Version(4) || id.Variant() != uuid.RFC4122 {
		return fmt.Errorf("callback operation intent ID must be a canonical UUIDv4: %q", entry.IntentID)
	}
	storageID, err := backendidentity.Parse(entry.BackendStorageID)
	if err != nil {
		return fmt.Errorf("invalid callback operation intent storage identity: %w", err)
	}
	if err := validateOperationIntentSpec(OperationIntentSpec{
		Kind:                 entry.Kind,
		LeaseUUID:            entry.LeaseUUID,
		CallbackURL:          entry.CallbackURL,
		LifecycleCallbackURL: entry.LifecycleCallbackURL,
		Tenant:               entry.Tenant,
		ProviderUUID:         entry.ProviderUUID,
		Items:                entry.Items,
		ResourceProfiles:     entry.ResourceProfiles,
		EffectiveItems:       entry.EffectiveItems,
		HealthCheckServices:  entry.HealthCheckServices,
		Manifest:             entry.Manifest,
		SourceLeaseUUID:      entry.SourceLeaseUUID,
		SourceGeneration:     entry.SourceGeneration,
	}); err != nil {
		return err
	}
	if err := backendname.Validate(entry.Backend); err != nil {
		return fmt.Errorf("callback operation intent backend: %w", err)
	}
	if !storageID.Valid() {
		return errors.New("callback operation intent requires backend storage authority")
	}
	wantOperationID, err := parseOperationCallbackID(entry.CallbackURL)
	if err != nil {
		return err
	}
	if entry.OperationID != wantOperationID {
		return fmt.Errorf("callback operation intent ID %q does not match callback authority %q",
			entry.OperationID.Fingerprint(), wantOperationID.Fingerprint())
	}
	if entry.LeaseUUID != leaseUUID {
		return fmt.Errorf("callback operation intent lease mismatch: key %q contains %q", leaseUUID, entry.LeaseUUID)
	}
	switch entry.State {
	case operationIntentPending:
		if !entry.SettledAt.IsZero() || entry.SettlementError != "" ||
			!entry.FailurePredecessor.IsZero() {
			return errors.New("pending callback operation intent carries terminal outcome")
		}
	case operationIntentSucceeded:
		if entry.SettledAt.IsZero() || entry.SettlementError != "" ||
			!entry.FailurePredecessor.IsZero() {
			return errors.New("successful callback operation intent has invalid terminal outcome")
		}
	case operationIntentFailed:
		if entry.SettledAt.IsZero() {
			return errors.New("failed callback operation intent has invalid terminal outcome")
		}
		if err := validateOperationFailurePredecessorRecord(entry.FailurePredecessor); err != nil {
			return fmt.Errorf("failed callback operation intent predecessor: %w", err)
		}
	default:
		return fmt.Errorf("invalid callback operation intent state %q", entry.State)
	}
	return validateStoredCallbackCreatedAt(entry.CreatedAt)
}

func parseOperationCallbackID(callbackURL string) (OperationID, error) {
	parsed, err := url.Parse(callbackURL)
	if err != nil {
		return OperationID{}, fmt.Errorf("parse operation callback authority: %w", err)
	}
	values, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return OperationID{}, fmt.Errorf("parse operation callback authority: %w", err)
	}
	ids := values[backend.CallbackOperationIDQueryParameter]
	if len(ids) == 0 {
		// Tokenless compatibility is intentional at the request boundary, so an
		// explicitly recorded tokenless intent remains readable and comparable.
		// It does not authorize completion when the durable intent is absent.
		return OperationID{}, nil
	}
	if len(ids) != 1 {
		return OperationID{}, fmt.Errorf("operation callback authority occurs %d times", len(ids))
	}
	operationID, err := operationid.Parse(ids[0])
	if err != nil {
		return OperationID{}, fmt.Errorf("parse operation callback authority: %w", err)
	}
	return operationID, nil
}

func operationIntentEntriesEqual(left, right operationIntentEntry) bool {
	return left.Kind == right.Kind &&
		left.OperationID == right.OperationID &&
		left.LeaseUUID == right.LeaseUUID &&
		left.CallbackURL == right.CallbackURL &&
		left.LifecycleCallbackURL == right.LifecycleCallbackURL &&
		left.Backend == right.Backend &&
		left.BackendStorageID == right.BackendStorageID &&
		left.Tenant == right.Tenant &&
		left.ProviderUUID == right.ProviderUUID &&
		slices.Equal(left.Items, right.Items) &&
		slices.Equal(left.ResourceProfiles, right.ResourceProfiles) &&
		slices.Equal(left.EffectiveItems, right.EffectiveItems) &&
		slices.Equal(left.HealthCheckServices, right.HealthCheckServices) &&
		bytes.Equal(left.Manifest, right.Manifest) &&
		left.SourceLeaseUUID == right.SourceLeaseUUID &&
		left.SourceGeneration == right.SourceGeneration
}

func decodeOperationIntent(key, value []byte) (OperationIntentClaim, error) {
	var entry operationIntentEntry
	if err := decodeStrictAuthoritativeObject(value, maxOperationIntentEntryBytes, &entry); err != nil {
		return OperationIntentClaim{}, fmt.Errorf("decode callback operation intent %q: %w", key, err)
	}
	if len(entry.EffectiveItems) == 0 {
		// Compatibility with intent rows written before effective custom-domain
		// labels were journaled separately from desired lease items.
		entry.EffectiveItems = slices.Clone(entry.Items)
	}
	if entry.State == "" {
		// Rows written by the first v0.14 release-candidate format predate
		// explicit outcomes; every such row was an unresolved intent.
		entry.State = operationIntentPending
	}
	if err := validateOperationIntentEntry(entry, string(key)); err != nil {
		return OperationIntentClaim{}, fmt.Errorf("invalid callback operation intent %q: %w", key, err)
	}
	storageID, err := backendidentity.Parse(entry.BackendStorageID)
	if err != nil {
		return OperationIntentClaim{}, fmt.Errorf("decode callback operation intent %q storage identity: %w", key, err)
	}
	return OperationIntentClaim{operationAuthority: operationAuthority{
		entry: &entry, storageID: storageID, digest: sha256.Sum256(value),
	}}, nil
}

func operationCompletionRecordFor(entry operationIntentEntry) operationCompletionRecord {
	return operationCompletionRecord{
		Version:              operationCompletionRecordVersion,
		IntentID:             entry.IntentID,
		OperationID:          entry.OperationID,
		Kind:                 entry.Kind,
		LeaseUUID:            entry.LeaseUUID,
		CallbackURL:          entry.CallbackURL,
		LifecycleCallbackURL: entry.LifecycleCallbackURL,
		Backend:              entry.Backend,
		BackendStorageID:     entry.BackendStorageID,
		Tenant:               entry.Tenant,
		ProviderUUID:         entry.ProviderUUID,
		CreatedAt:            entry.CreatedAt,
		State:                entry.State,
		SettledAt:            entry.SettledAt,
		SettlementError:      entry.SettlementError,
	}
}

func operationHistoryKey(operationID OperationID, callbackURL string) [sha256.Size]byte {
	return sha256.Sum256([]byte(operationID.String() + "\x00" + callbackURL))
}

func validateOperationCompletionRecord(record operationCompletionRecord, leaseUUID string) error {
	if record.Version != operationCompletionRecordVersion {
		return fmt.Errorf("completed operation has unsupported version %d", record.Version)
	}
	id, err := uuid.Parse(record.IntentID)
	if err != nil || id.String() != record.IntentID || id.Version() != uuid.Version(4) || id.Variant() != uuid.RFC4122 {
		return fmt.Errorf("completed operation intent ID must be a canonical UUIDv4: %q", record.IntentID)
	}
	if record.LeaseUUID != leaseUUID {
		return fmt.Errorf("completed operation lease mismatch: key %q contains %q", leaseUUID, record.LeaseUUID)
	}
	if record.Kind != OperationIntentProvision && record.Kind != OperationIntentRestore {
		return fmt.Errorf("invalid completed operation intent kind %q", record.Kind)
	}
	if err := backendname.Validate(record.Backend); err != nil {
		return fmt.Errorf("completed operation backend: %w", err)
	}
	if record.Tenant == "" || record.ProviderUUID == "" {
		return errors.New("completed operation requires tenant and provider identities")
	}
	if !backend.IsCanonicalLeaseUUID(record.ProviderUUID) {
		return errors.New("completed operation provider UUID is not canonical")
	}
	if _, err := backendidentity.Parse(record.BackendStorageID); err != nil {
		return fmt.Errorf("invalid completed operation storage identity: %w", err)
	}
	if err := validateCallbackDestination(record.CallbackURL); err != nil {
		return err
	}
	operationID, err := parseOperationCallbackID(record.CallbackURL)
	if err != nil {
		return err
	}
	if operationID != record.OperationID {
		return fmt.Errorf("completed operation ID %q does not match callback authority %q",
			record.OperationID.Fingerprint(), operationID.Fingerprint())
	}
	resolvedLifecycle, err := backend.ResolveLifecycleCallbackURL(
		record.CallbackURL, record.LifecycleCallbackURL,
	)
	if err != nil || resolvedLifecycle != record.LifecycleCallbackURL {
		return errors.New("completed operation has invalid callback pair")
	}
	switch record.State {
	case operationIntentSucceeded:
		if record.SettlementError != "" {
			return errors.New("successful completed operation carries a failure")
		}
	case operationIntentFailed:
	default:
		return fmt.Errorf("completed operation has non-terminal state %q", record.State)
	}
	if err := validateStoredCallbackCreatedAt(record.CreatedAt); err != nil {
		return err
	}
	return validateStoredCallbackCreatedAt(record.SettledAt)
}

func decodeOperationCompletionRecord(
	leaseKey, historyKey, value []byte,
) (operationCompletionRecord, error) {
	var record operationCompletionRecord
	if err := decodeStrictAuthoritativeObject(
		value, maxOperationHistoryEntryBytes, &record,
	); err != nil {
		return operationCompletionRecord{}, fmt.Errorf("decode completed operation for lease %q: %w", leaseKey, err)
	}
	if err := validateOperationCompletionRecord(record, string(leaseKey)); err != nil {
		return operationCompletionRecord{}, fmt.Errorf("invalid completed operation for lease %q: %w", leaseKey, err)
	}
	wantKey := operationHistoryKey(record.OperationID, record.CallbackURL)
	if !bytes.Equal(historyKey, wantKey[:]) {
		return operationCompletionRecord{}, fmt.Errorf("completed operation for lease %q has mismatched replay key", leaseKey)
	}
	return record, nil
}

func operationHistorySameAuthority(left, right operationCompletionRecord) bool {
	return left.Backend == right.Backend &&
		left.BackendStorageID == right.BackendStorageID &&
		left.Tenant == right.Tenant &&
		left.ProviderUUID == right.ProviderUUID
}

func operationHistoryMatchesAuthority(record operationCompletionRecord, entry operationIntentEntry) bool {
	return record.Backend == entry.Backend &&
		record.BackendStorageID == entry.BackendStorageID &&
		record.Tenant == entry.Tenant &&
		record.ProviderUUID == entry.ProviderUUID
}

func operationHistoryMatchesEntry(record operationCompletionRecord, entry operationIntentEntry) bool {
	return record.OperationID == entry.OperationID &&
		record.CallbackURL == entry.CallbackURL &&
		record.Backend == entry.Backend &&
		record.BackendStorageID == entry.BackendStorageID &&
		record.Tenant == entry.Tenant &&
		record.ProviderUUID == entry.ProviderUUID
}

// reserveOperationReceiptTx accounts for the immutable terminal replay fence
// before a pending operation can be admitted. Settlement consumes no further
// capacity, so an accepted mutation can always write its required receipt.
func reserveOperationReceiptTx(tx *bolt.Tx, entry operationIntentEntry) error {
	return reserveOperationReceiptWithinLimitsTx(
		tx, entry, maxOperationReceiptsPerLease, maxCallbackReceiptReservationsGlobal,
	)
}

func reserveOperationReceiptWithinLimitsTx(
	tx *bolt.Tx,
	entry operationIntentEntry,
	perLeaseLimit int,
	globalLimit uint64,
) error {
	history, err := listOperationHistoryTx(tx, entry.LeaseUUID)
	if err != nil {
		return err
	}
	if len(history) >= perLeaseLimit {
		return &OperationReceiptCapacityError{
			LeaseUUID: entry.LeaseUUID,
			Limit:     uint64(perLeaseLimit),
		}
	}
	reserved, err := reserveCallbackReceiptReservationWithinLimitTx(tx, globalLimit)
	if err != nil {
		return err
	}
	if !reserved {
		return &OperationReceiptCapacityError{Limit: globalLimit}
	}
	return nil
}

func releaseClosedLeaseOperationReceiptsTx(tx *bolt.Tx, leaseUUID string) error {
	root := tx.Bucket(callbackOperationHistoryBucketName)
	if root == nil {
		return errors.New("completed operation history bucket missing")
	}
	if root.Bucket([]byte(leaseUUID)) == nil {
		return nil
	}
	if err := root.DeleteBucket([]byte(leaseUUID)); err != nil {
		return fmt.Errorf("delete completed operation history for closed lease %q: %w", leaseUUID, err)
	}
	return nil
}

func listOperationHistoryTx(tx *bolt.Tx, leaseUUID string) ([]operationCompletionRecord, error) {
	root := tx.Bucket(callbackOperationHistoryBucketName)
	if root == nil {
		return nil, errors.New("completed operation history bucket missing")
	}
	leaseKey := []byte(leaseUUID)
	if root.Get(leaseKey) != nil {
		return nil, fmt.Errorf("completed operation history %q is not a nested bucket", leaseUUID)
	}
	leaseBucket := root.Bucket(leaseKey)
	if leaseBucket == nil {
		return nil, nil
	}
	var records []operationCompletionRecord
	var first operationCompletionRecord
	operationCallbacks := make(map[OperationID]string)
	callbackOperations := make(map[string]OperationID)
	err := leaseBucket.ForEach(func(key, value []byte) error {
		if value == nil {
			return fmt.Errorf("completed operation history %q contains a nested bucket", leaseUUID)
		}
		record, err := decodeOperationCompletionRecord(leaseKey, key, value)
		if err != nil {
			return err
		}
		if len(records) != 0 && !operationHistorySameAuthority(first, record) {
			return fmt.Errorf("completed operation history %q crosses backend storage or principal authority", leaseUUID)
		}
		if len(records) == 0 {
			first = record
		}
		if record.OperationID.Valid() {
			if callback, exists := operationCallbacks[record.OperationID]; exists && callback != record.CallbackURL {
				return fmt.Errorf("completed operation history %q reuses operation ID with divergent callback authority",
					leaseUUID)
			}
			operationCallbacks[record.OperationID] = record.CallbackURL
		}
		if operationID, exists := callbackOperations[record.CallbackURL]; exists && operationID != record.OperationID {
			return fmt.Errorf("completed operation history %q reuses callback with divergent operation authority",
				leaseUUID)
		}
		callbackOperations[record.CallbackURL] = record.OperationID
		records = append(records, record)
		return nil
	})
	return records, err
}

func validateOperationHistoryTx(tx *bolt.Tx) (uint64, error) {
	root := tx.Bucket(callbackOperationHistoryBucketName)
	if root == nil {
		return 0, errors.New("completed operation history bucket missing")
	}
	var completedCount uint64
	if err := root.ForEach(func(leaseKey, value []byte) error {
		if value != nil {
			return fmt.Errorf("completed operation history %q is not a nested bucket", leaseKey)
		}
		if err := validateCanonicalLeaseUUID(string(leaseKey)); err != nil {
			return fmt.Errorf("completed operation history has invalid lease key: %w", err)
		}
		history, err := listOperationHistoryTx(tx, string(leaseKey))
		completedCount += uint64(len(history))
		return err
	}); err != nil {
		return 0, err
	}
	heads := tx.Bucket(callbackLeaseMutationHeadBucketName)
	if heads == nil {
		return 0, errors.New("callback lease mutation head bucket missing")
	}
	var pendingReservations uint64
	if err := heads.ForEach(func(leaseKey, value []byte) error {
		if value == nil {
			return fmt.Errorf("callback lease mutation head %q is a nested bucket", leaseKey)
		}
		head, err := decodeLeaseMutationHead(leaseKey, value)
		if err != nil {
			return err
		}
		if operation, ok := head.(operationLeaseMutationHead); ok &&
			operation.claim.entry.State == operationIntentPending {
			pendingReservations++
		}
		history, err := listOperationHistoryTx(tx, string(leaseKey))
		if err != nil {
			return err
		}
		if len(history) == 0 {
			return nil
		}
		completed := history[0]
		switch state := head.(type) {
		case operationLeaseMutationHead:
			if !operationHistoryMatchesAuthority(completed, *state.claim.entry) {
				return fmt.Errorf("operation state for lease %q crosses completed-history authority", leaseKey)
			}
		case maintenanceLeaseMutationHead:
			if completed.Backend != state.claim.Backend() ||
				completed.BackendStorageID != state.claim.BackendStorageID().String() ||
				completed.Tenant != state.claim.Tenant() ||
				completed.ProviderUUID != state.claim.ProviderUUID() {
				return fmt.Errorf("maintenance state for lease %q crosses completed-history authority", leaseKey)
			}
		case closeLeaseMutationHead:
			if completed.Backend != state.claim.Backend() ||
				completed.BackendStorageID != state.claim.BackendStorageID().String() ||
				(!state.claim.CleanupOnly() && (completed.Tenant != state.claim.Tenant() ||
					completed.ProviderUUID != state.claim.ProviderUUID())) {
				return fmt.Errorf("close state for lease %q crosses completed-history authority", leaseKey)
			}
		case closedLeaseMutationHead:
			if completed.Backend != state.entry.Backend ||
				completed.BackendStorageID != state.entry.BackendStorageID {
				return fmt.Errorf("closed state for lease %q crosses completed-history authority", leaseKey)
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return completedCount + pendingReservations, nil
}

func archiveOperationCompletionTx(tx *bolt.Tx, entry operationIntentEntry) error {
	record := operationCompletionRecordFor(entry)
	if err := validateOperationCompletionRecord(record, record.LeaseUUID); err != nil {
		return err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal completed operation: %w", err)
	}
	if len(data) > maxOperationHistoryEntryBytes {
		return fmt.Errorf("completed operation exceeds %d bytes", maxOperationHistoryEntryBytes)
	}
	history, err := listOperationHistoryTx(tx, record.LeaseUUID)
	if err != nil {
		return err
	}
	for _, completed := range history {
		if !operationHistorySameAuthority(completed, record) {
			return fmt.Errorf("completed operation history %q crosses backend storage or principal authority",
				record.LeaseUUID)
		}
		sameOperation := record.OperationID.Valid() && completed.OperationID == record.OperationID
		sameCallback := completed.CallbackURL == record.CallbackURL
		if (sameOperation || sameCallback) && completed != record {
			return fmt.Errorf("completed operation replay identity conflicts for lease %q", record.LeaseUUID)
		}
	}
	root := tx.Bucket(callbackOperationHistoryBucketName)
	if root == nil {
		return errors.New("completed operation history bucket missing")
	}
	leaseKey := []byte(record.LeaseUUID)
	if root.Get(leaseKey) != nil {
		return fmt.Errorf("completed operation history %q is not a nested bucket", record.LeaseUUID)
	}
	leaseBucket, err := root.CreateBucketIfNotExists(leaseKey)
	if err != nil {
		return fmt.Errorf("create completed operation history for lease %q: %w", record.LeaseUUID, err)
	}
	historyKey := operationHistoryKey(record.OperationID, record.CallbackURL)
	if leaseBucket.Bucket(historyKey[:]) != nil {
		return fmt.Errorf("completed operation replay key for lease %q is a nested bucket", record.LeaseUUID)
	}
	if current := leaseBucket.Get(historyKey[:]); current != nil {
		existing, decodeErr := decodeOperationCompletionRecord(leaseKey, historyKey[:], current)
		if decodeErr != nil {
			return decodeErr
		}
		if existing != record {
			return fmt.Errorf("completed operation replay identity conflicts for lease %q", record.LeaseUUID)
		}
		return nil
	}
	return leaseBucket.Put(historyKey[:], data)
}

func operationIntentMatchesCallback(intent operationIntentEntry, entry CallbackEntry) error {
	if entry.DeliveryKind != CallbackDeliveryKindOperation ||
		entry.LeaseUUID != intent.LeaseUUID ||
		entry.CallbackURL != intent.CallbackURL ||
		entry.Backend != intent.Backend ||
		entry.BackendStorageID != intent.BackendStorageID {
		return fmt.Errorf("operation callback does not match durable intent for lease %q", intent.LeaseUUID)
	}
	return nil
}

func marshalOperationIntent(entry operationIntentEntry) ([]byte, error) {
	if err := validateOperationIntentEntry(entry, entry.LeaseUUID); err != nil {
		return nil, err
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("marshal callback operation intent: %w", err)
	}
	if len(data) > maxOperationIntentEntryBytes {
		return nil, fmt.Errorf("callback operation intent exceeds %d bytes", maxOperationIntentEntryBytes)
	}
	return data, nil
}

func verifyOperationIntentTx(tx *bolt.Tx, claim OperationIntentClaim) error {
	head, present, err := getLeaseMutationHeadTx(tx, claim.entry.LeaseUUID)
	if err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("callback operation intent no longer exists for lease %q", claim.entry.LeaseUUID)
	}
	operation, ok := head.(operationLeaseMutationHead)
	if !ok {
		return fmt.Errorf("callback operation intent for lease %q was replaced by %q",
			claim.entry.LeaseUUID, head.headKind())
	}
	if operation.claim.digest != claim.digest {
		return fmt.Errorf("callback operation intent changed before precise mutation")
	}
	return nil
}
