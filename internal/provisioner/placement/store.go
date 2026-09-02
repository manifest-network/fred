package placement

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/callbackurl"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/storeauthority"
)

var bucketName = []byte("placements")

// recordIssuerSequence provides process-local identities for opaque record
// capabilities. RecordRevision is never serialized, so reopening a Store
// deliberately mints a different issuer.
var recordIssuerSequence atomic.Uint64

// State describes what is durably known about one lease placement.
type State uint8

const (
	// StateAbsent means no record exists for the lease.
	StateAbsent State = iota
	// StateAttempting means a backend call may have happened but has not yet
	// been confirmed or authoritatively disproved.
	StateAttempting
	// StateConfirmed means Backend is the last positively observed owner. A
	// confirmed placement may also carry an unresolved Attempt.
	StateConfirmed
	// StateUnusable means a record exists but cannot safely be interpreted.
	StateUnusable
)

// String returns the stable log representation of a placement state.
func (s State) String() string {
	switch s {
	case StateAbsent:
		return "absent"
	case StateAttempting:
		return "attempting"
	case StateConfirmed:
		return "confirmed"
	case StateUnusable:
		return "unusable"
	default:
		return fmt.Sprintf("State(%d)", s)
	}
}

var (
	// ErrInvalidPlacement means a required lease or backend identifier is empty.
	ErrInvalidPlacement = errors.New("invalid placement")
	// ErrAttemptConflict means an unresolved attempt already exists. Retrying
	// even the same target is unsafe because the first backend outcome is unknown.
	ErrAttemptConflict = errors.New("placement attempt already exists")
	// ErrBackendConflict means an operation targeted a backend other than the
	// lease's confirmed owner.
	ErrBackendConflict = errors.New("placement backend conflicts with target")
	// ErrUnusablePlacement means an on-disk record exists but cannot safely be
	// used as either affinity or an attempt gate.
	ErrUnusablePlacement = errors.New("placement record is unusable")
	// ErrAttemptMismatch means an attempt settlement did not name the current
	// unresolved attempt.
	ErrAttemptMismatch = errors.New("placement attempt does not match target")
	// ErrAttemptClaimed means an exact durable generation is already reserved for
	// callback recovery in this process. Callers must retry instead of treating
	// contention as a terminal stale-callback verdict.
	ErrAttemptClaimed = errors.New("placement attempt is already claimed")
	// ErrInvalidAttemptToken means an attempt settlement capability was not
	// issued by this store or does not contain all required placement evidence.
	ErrInvalidAttemptToken = errors.New("invalid placement attempt token")
	// ErrInvalidInventoryFence means a typed inventory fence was not issued by
	// this store or was invalidated after it was issued.
	ErrInvalidInventoryFence = errors.New("invalid placement inventory fence")
	// ErrInvalidRecordRevision means a typed record mutation was attempted with
	// the invalid zero revision, including an unupgraded legacy record.
	ErrInvalidRecordRevision = errors.New("invalid placement record revision")
	// ErrInvalidRestoreClaim means a restore settlement capability was not
	// issued by this store or is missing facts needed for exact settlement.
	ErrInvalidRestoreClaim = errors.New("invalid placement restore claim")
	// ErrRestoreSourceNotFound means no durable placement identifies the
	// retained source requested by a restore.
	ErrRestoreSourceNotFound = errors.New("restore source placement not found")
	// ErrRestoreSourceUnavailable means the requested source is not one
	// unambiguous, confirmed, revisioned placement with no unresolved attempt.
	ErrRestoreSourceUnavailable = errors.New("restore source placement is unavailable")
	// ErrRestoreSourceClaimed means another synchronous restore dispatch already
	// holds the exclusive process-local claim for this source.
	ErrRestoreSourceClaimed = errors.New("restore source placement is claimed")
	// ErrRestoreTargetUnavailable means the target already has durable placement
	// evidence. Restore admission requires a truly absent target and never
	// adopts, retries, or overwrites an existing placement.
	ErrRestoreTargetUnavailable = errors.New("restore target placement is unavailable")
)

// RecordRevision is the opaque identity of one durable placement record
// version. Its private fields bind the capability to the issuing store, lease,
// and exact revision. The zero value is invalid. It deliberately has no
// numeric or lease accessor: callers can only present it back to its issuer.
type RecordRevision struct {
	issuer    uint64
	leaseUUID string
	value     uint64
}

// Valid reports whether the revision identifies a durably written record.
func (revision RecordRevision) Valid() bool {
	return revision.issuer != 0 && revision.leaseUUID != "" && revision.value != 0
}

func (s *Store) newRecordRevision(leaseUUID string, value uint64) RecordRevision {
	if s == nil || s.recordIssuer == 0 || leaseUUID == "" || value == 0 {
		return RecordRevision{}
	}
	return RecordRevision{issuer: s.recordIssuer, leaseUUID: leaseUUID, value: value}
}

// InventoryFence is a causal placement-store boundary. Unlike RecordRevision,
// an explicitly issued fence may represent revision zero. The private issuer
// and authority epoch prevent mixing stores or reusing evidence after a newer
// inventory session supersedes the collection that minted it.
type InventoryFence struct {
	issuer   *Store
	revision uint64
	epoch    uint64
}

// Valid reports whether the fence was explicitly issued by a placement store.
// A store still revalidates the issuer and authority epoch when consuming it.
func (fence InventoryFence) Valid() bool {
	return fence.issuer != nil && fence.epoch != 0
}

// LifecycleObservationKind classifies the non-secret callback generation a
// backend reports from its own persisted live-provision record. Its zero value
// is Unknown and grants no authority.
type LifecycleObservationKind uint8

const (
	LifecycleObservationUnknown LifecycleObservationKind = iota
	LifecycleObservationLegacy
	LifecycleObservationTyped
	LifecycleObservationUnusable
)

// LifecycleObservation is placement-owned inventory evidence. Typed requires
// one valid lifecycle UUID; every other kind requires the zero ID. Projection
// validates the value before consulting durable authority.
type LifecycleObservation struct {
	Kind LifecycleObservationKind
	ID   lifecycle.ID
}

// InventoryProjection is one fleet observation to be applied at a single
// causal boundary. Complete must be true only when every backend in the
// configured topology was authoritatively observed. A complete projection
// establishes the durable admission baseline in the same transaction as its
// placement mutations. Placements contains unique positive owners. A positive
// observation from the exact attempted backend may confirm that attempt only
// when its paired lifecycle generation is durably intact; otherwise the owner
// is recorded while the exact attempt remains available for callback recovery.
// Conflicts contains leases reported by at least two authoritative backends.
// UntrustedPositives contains positive membership reported by an endpoint whose
// response was rejected as placement authority (for example, because its two
// inventory endpoints disagreed). Those facts become a durable quarantine:
// they cannot establish an owner, but they also cannot be discarded as absence.
// Inventory silence is deliberately not representable: it cannot clear an
// attempt or a durable quarantine because an earlier backend request may commit
// after the inventory response.
//
// Lifecycles contains observations for active provisions only and may omit
// retention-derived placements. Unknown observations are explicit zero values;
// old backends may omit the wire fact, but that absence never establishes
// lifecycle authority. ProjectInventory defensively copies all maps and slices
// before use.
type InventoryProjection struct {
	Complete                 bool
	BackendStorageIdentities map[string]backendidentity.ID
	// EmptyBackends is raw collection evidence: each listed backend returned
	// concrete empty provision and retention inventories in this complete
	// snapshot. It is deliberately supplied separately from Placements because
	// causal fencing may remove positive lease observations before projection.
	// Complete projections must provide a non-nil slice; partial projections
	// must leave it nil.
	EmptyBackends []string
	Placements    map[string]string
	Lifecycles    map[string]LifecycleObservation
	Conflicts     map[string][]string
	// UntrustedPositives requires one or more configured backend names per
	// lease. It is distinct from Conflicts so a single rejected reporter cannot
	// be accidentally promoted to an authoritative owner by projection.
	UntrustedPositives map[string][]string
}

// ProjectionResult reports leases left unchanged because their durable
// evidence was newer than the input inventory or was exclusively claimed by a
// restore source or callback recovery.
type ProjectionResult struct {
	// Fenced contains the lease UUIDs whose submitted observations were not
	// applied at this inventory boundary.
	Fenced map[string]struct{}
}

// AttemptToken is the exclusive capability for settling one durable
// write-ahead attempt. Its private fields bind the settlement to the issuing
// store, lease, backend, operation, and exact record revision. The zero value
// is invalid.
type AttemptToken struct {
	issuer                 *Store
	leaseUUID              string
	backendName            string
	operationID            operation.OperationID
	operationKind          operation.Kind
	restoreSourceLeaseUUID string
	payloadFingerprint     PayloadFingerprint
	requestSnapshot        BackendRequestSnapshot
	callbackPair           CallbackPair
	revision               RecordRevision
}

// Valid reports whether the token contains every fact required to identify a
// typed write-ahead attempt. The issuing store still performs an exact CAS.
func (token AttemptToken) Valid() bool {
	return token.issuer != nil && token.leaseUUID != "" && token.backendName != "" &&
		validAttemptMetadata(
			token.leaseUUID, token.operationID, token.operationKind,
			token.restoreSourceLeaseUUID, token.payloadFingerprint,
			token.requestSnapshot, token.callbackPair,
		) && token.revision.Valid() &&
		token.revision.issuer == token.issuer.recordIssuer &&
		token.revision.leaseUUID == token.leaseUUID
}

// PayloadFingerprint is the exact SHA-256 identity of a payload-bearing
// provision request. Private storage makes malformed lengths unconstructable;
// the zero value explicitly means the provision request carries no payload.
// Restore admission has a distinct API and rejects a nonzero fingerprint.
type PayloadFingerprint struct {
	sha256  [sha256.Size]byte
	present bool
}

// BackendRequestSnapshot is the immutable portion of the exact backend
// request that remains authoritative across provider restart. Tenant,
// provider, and lease items are reconstructed from this snapshot rather than
// from mutable chain projection (notably LeaseItem.CustomDomain). The private
// canonical representation keeps the type comparable for exact CAS tokens;
// the zero value is invalid.
type BackendRequestSnapshot struct {
	tenant       string
	providerUUID string
	itemsJSON    string
}

// NewBackendRequestSnapshot validates and detaches the request facts persisted
// before backend dispatch. The canonical JSON representation is deliberately
// private so callers cannot manufacture a partially decoded snapshot.
func NewBackendRequestSnapshot(
	tenant string,
	providerUUID string,
	items []backend.LeaseItem,
) (BackendRequestSnapshot, error) {
	if !utf8.ValidString(tenant) {
		return BackendRequestSnapshot{}, errors.New("backend request tenant is not valid UTF-8")
	}
	if strings.TrimSpace(tenant) == "" {
		return BackendRequestSnapshot{}, errors.New("backend request tenant is required")
	}
	if !utf8.ValidString(providerUUID) {
		return BackendRequestSnapshot{}, errors.New("backend request provider UUID is not valid UTF-8")
	}
	if strings.TrimSpace(providerUUID) == "" {
		return BackendRequestSnapshot{}, errors.New("backend request provider UUID is required")
	}
	if len(items) == 0 {
		return BackendRequestSnapshot{}, errors.New("backend request items are required")
	}
	for index, item := range items {
		for _, field := range [...]struct {
			name  string
			value string
		}{
			{name: "SKU", value: item.SKU},
			{name: "service name", value: item.ServiceName},
			{name: "custom domain", value: item.CustomDomain},
		} {
			if !utf8.ValidString(field.value) {
				return BackendRequestSnapshot{}, fmt.Errorf(
					"backend request item %d %s is not valid UTF-8", index, field.name,
				)
			}
		}
		if strings.TrimSpace(item.SKU) == "" {
			return BackendRequestSnapshot{}, fmt.Errorf("backend request item %d has no SKU", index)
		}
	}
	if _, err := backend.ValidateOperationQuantities(items); err != nil {
		return BackendRequestSnapshot{}, fmt.Errorf("backend request quantities: %w", err)
	}
	encoded, err := json.Marshal(items)
	if err != nil {
		return BackendRequestSnapshot{}, fmt.Errorf("encode backend request items: %w", err)
	}
	return BackendRequestSnapshot{
		tenant: tenant, providerUUID: providerUUID, itemsJSON: string(encoded),
	}, nil
}

// Valid reports whether the snapshot contains one complete exact request.
func (snapshot BackendRequestSnapshot) Valid() bool {
	return snapshot.tenant != "" && snapshot.providerUUID != "" && snapshot.itemsJSON != ""
}

// Tenant returns the exact tenant sent to the backend.
func (snapshot BackendRequestSnapshot) Tenant() string {
	if !snapshot.Valid() {
		return ""
	}
	return snapshot.tenant
}

// ProviderUUID returns the exact provider identity sent to the backend.
func (snapshot BackendRequestSnapshot) ProviderUUID() string {
	if !snapshot.Valid() {
		return ""
	}
	return snapshot.providerUUID
}

// Items returns a detached copy of the exact ordered backend item list.
func (snapshot BackendRequestSnapshot) Items() []backend.LeaseItem {
	if !snapshot.Valid() {
		return nil
	}
	var items []backend.LeaseItem
	if err := json.Unmarshal([]byte(snapshot.itemsJSON), &items); err != nil {
		// Only the constructor and strict durable decoder can populate itemsJSON.
		// Preserve the safe-zero contract if an internal invariant is ever broken.
		return nil
	}
	return items
}

// NewPayloadFingerprint constructs an exact request fingerprint. Empty input
// is not accepted: callers represent a payloadless request with the explicit
// zero PayloadFingerprint value.
func NewPayloadFingerprint(hash []byte) (PayloadFingerprint, error) {
	if len(hash) != sha256.Size {
		return PayloadFingerprint{}, fmt.Errorf(
			"payload fingerprint must be %d bytes", sha256.Size,
		)
	}
	var fingerprint PayloadFingerprint
	copy(fingerprint.sha256[:], hash)
	fingerprint.present = true
	return fingerprint, nil
}

// Valid reports whether this fingerprint identifies a payload-bearing request.
// The zero value deliberately reports false and means payloadless.
func (fingerprint PayloadFingerprint) Valid() bool {
	return fingerprint.present
}

// Bytes returns a detached hash value, or nil for a payloadless fingerprint.
func (fingerprint PayloadFingerprint) Bytes() []byte {
	if !fingerprint.Valid() {
		return nil
	}
	return append([]byte(nil), fingerprint.sha256[:]...)
}

// String returns the canonical lowercase wire form, or empty for payloadless.
func (fingerprint PayloadFingerprint) String() string {
	if !fingerprint.Valid() {
		return ""
	}
	return hex.EncodeToString(fingerprint.sha256[:])
}

// CallbackPair is the exact operation/lifecycle destination persisted before a
// backend call. The private fields are constructible only after both URLs prove
// they carry the expected, mutually exclusive typed generation. The zero value
// is invalid; callback-base changes after restart cannot rewrite an Attempt.
type CallbackPair struct {
	operationID  operation.OperationID
	operationURL string
	lifecycleURL string
}

// NewCallbackPair validates and binds the exact callback destinations for id.
func NewCallbackPair(
	id operation.OperationID,
	operationURL string,
	lifecycleURL string,
) (CallbackPair, error) {
	if !id.Valid() {
		return CallbackPair{}, operation.ErrInvalidID
	}
	wantLifecycle, err := lifecycle.FromOperationID(id)
	if err != nil {
		return CallbackPair{}, err
	}
	if err := validateCallbackDestination(operationURL, id, lifecycle.ID{}); err != nil {
		return CallbackPair{}, fmt.Errorf("operation callback destination: %w", err)
	}
	if err := validateCallbackDestination(
		lifecycleURL, operation.OperationID{}, wantLifecycle,
	); err != nil {
		return CallbackPair{}, fmt.Errorf("lifecycle callback destination: %w", err)
	}
	if _, err := backend.ResolveLifecycleCallbackURL(operationURL, lifecycleURL); err != nil {
		return CallbackPair{}, fmt.Errorf("callback pair is not an exact derivation: %w", err)
	}
	return CallbackPair{
		operationID: id, operationURL: operationURL, lifecycleURL: lifecycleURL,
	}, nil
}

func validateCallbackDestination(
	raw string,
	wantOperation operation.OperationID,
	wantLifecycle lifecycle.ID,
) error {
	if raw == "" {
		return errors.New("URL is empty")
	}
	endpoint, err := callbackurl.ParseEndpoint(raw)
	if err != nil {
		return err
	}
	values, err := url.ParseQuery(endpoint.RawQuery())
	if err != nil {
		return fmt.Errorf("parse query: %w", err)
	}
	operationID, operationPresent, err := operation.ParseQuery(values)
	if err != nil {
		return err
	}
	lifecycleID, lifecyclePresent, err := lifecycle.ParseQuery(values)
	if err != nil {
		return err
	}
	if wantOperation.Valid() {
		if !operationPresent || operationID != wantOperation || lifecyclePresent {
			return errors.New("URL does not carry only the exact operation ID")
		}
		return nil
	}
	if !wantLifecycle.Valid() || !lifecyclePresent || lifecycleID != wantLifecycle || operationPresent {
		return errors.New("URL does not carry only the exact lifecycle ID")
	}
	return nil
}

// Valid reports whether the pair contains two exact destinations for id.
func (pair CallbackPair) ValidFor(id operation.OperationID) bool {
	return id.Valid() && pair.operationID == id && pair.operationURL != "" && pair.lifecycleURL != ""
}

// OperationURL returns the exact operation-scoped destination, or empty for an
// invalid pair.
func (pair CallbackPair) OperationURL() string {
	if !pair.ValidFor(pair.operationID) {
		return ""
	}
	return pair.operationURL
}

// LifecycleURL returns the exact observation-scoped destination, or empty for
// an invalid pair.
func (pair CallbackPair) LifecycleURL() string {
	if !pair.ValidFor(pair.operationID) {
		return ""
	}
	return pair.lifecycleURL
}

// AttemptMetadata is the immutable identity needed to reconstruct one exact
// backend request after process restart. It contains capability-bearing
// callback URLs and belongs to the placement database's sensitive operational
// state. Its fields are private so a caller cannot manufacture a restore
// without a source or attach a source to a provision. The zero value is invalid.
type AttemptMetadata struct {
	operationID            operation.OperationID
	operationKind          operation.Kind
	restoreSourceLeaseUUID string
	payloadFingerprint     PayloadFingerprint
	requestSnapshot        BackendRequestSnapshot
	callbackPair           CallbackPair
}

// Valid reports whether metadata describes one complete provision or restore
// operation. Target-specific invariants are rechecked by the issuing Placement
// or AttemptClaim before metadata is exposed.
func (metadata AttemptMetadata) Valid() bool {
	return metadata.operationID.Valid() && metadata.operationKind.Valid() &&
		metadata.requestSnapshot.Valid() &&
		metadata.callbackPair.ValidFor(metadata.operationID) &&
		((metadata.operationKind == operation.KindProvision &&
			metadata.restoreSourceLeaseUUID == "") ||
			(metadata.operationKind == operation.KindRestore &&
				metadata.restoreSourceLeaseUUID != "" &&
				!metadata.payloadFingerprint.Valid()))
}

// RequestSnapshot returns the exact durable backend request facts. Invalid
// metadata returns the invalid safe-zero snapshot.
func (metadata AttemptMetadata) RequestSnapshot() BackendRequestSnapshot {
	if !metadata.Valid() {
		return BackendRequestSnapshot{}
	}
	return metadata.requestSnapshot
}

// CallbackPair returns the exact durable callback destinations. Invalid
// metadata returns the invalid zero pair.
func (metadata AttemptMetadata) CallbackPair() CallbackPair {
	if !metadata.Valid() {
		return CallbackPair{}
	}
	return metadata.callbackPair
}

// PayloadFingerprint returns the exact payload identity for a valid provision
// attempt. Payloadless provisions, restores, and invalid metadata return zero.
func (metadata AttemptMetadata) PayloadFingerprint() PayloadFingerprint {
	if !metadata.Valid() || metadata.operationKind != operation.KindProvision {
		return PayloadFingerprint{}
	}
	return metadata.payloadFingerprint
}

// OperationID returns the exact callback operation identity. Invalid metadata
// returns the safe zero ID.
func (metadata AttemptMetadata) OperationID() operation.OperationID {
	if !metadata.Valid() {
		return operation.OperationID{}
	}
	return metadata.operationID
}

// Kind returns the exact operation protocol. Invalid metadata returns the
// safe-zero KindInvalid value.
func (metadata AttemptMetadata) Kind() operation.Kind {
	if !metadata.Valid() {
		return operation.KindInvalid
	}
	return metadata.operationKind
}

// RestoreSourceLeaseUUID returns the source only for valid restore metadata.
// Provision and invalid metadata return an empty string.
func (metadata AttemptMetadata) RestoreSourceLeaseUUID() string {
	if !metadata.Valid() || metadata.operationKind != operation.KindRestore {
		return ""
	}
	return metadata.restoreSourceLeaseUUID
}

func validAttemptMetadata(
	targetLeaseUUID string,
	operationID operation.OperationID,
	kind operation.Kind,
	restoreSourceLeaseUUID string,
	payloadFingerprint PayloadFingerprint,
	requestSnapshot BackendRequestSnapshot,
	callbackPair CallbackPair,
) bool {
	metadata := AttemptMetadata{
		operationID:            operationID,
		operationKind:          kind,
		restoreSourceLeaseUUID: restoreSourceLeaseUUID,
		payloadFingerprint:     payloadFingerprint,
		requestSnapshot:        requestSnapshot,
		callbackPair:           callbackPair,
	}
	return targetLeaseUUID != "" && metadata.Valid() &&
		(kind != operation.KindRestore || restoreSourceLeaseUUID != targetLeaseUUID)
}

// AttemptClaim is the exclusive process-local capability for settling one
// durable operation generation after its registry record has been lost. The
// authority is either the unresolved attempt or the exact lifecycle generation
// produced by matching paired-generation inventory. The claim fences callback
// recovery and inventory while chain settlement is in progress. Its private
// fields bind it to the issuing store and exact record revision, and its nonce
// prevents ABA reuse after release. The zero value is invalid.
type AttemptClaim struct {
	issuer                 *Store
	leaseUUID              string
	backendName            string
	operationID            operation.OperationID
	operationKind          operation.Kind
	restoreSourceLeaseUUID string
	payloadFingerprint     PayloadFingerprint
	requestSnapshot        BackendRequestSnapshot
	callbackPair           CallbackPair
	revision               RecordRevision
	nonce                  uint64
	kind                   attemptClaimKind
	// sameBackendOwner records that placement contains positive ownership
	// evidence for backendName alongside this operation generation. It does not
	// prove that the attempted generation succeeded or that the chain is ACTIVE.
	sameBackendOwner bool
}

type attemptClaimKind uint8

const (
	attemptClaimInvalid attemptClaimKind = iota
	attemptClaimUnresolved
	attemptClaimConfirmedGeneration
)

func (kind attemptClaimKind) valid() bool {
	return kind == attemptClaimUnresolved || kind == attemptClaimConfirmedGeneration
}

// Valid reports whether the claim carries every fact needed to identify one
// exact durable operation generation. The issuing store checks its reservation
// and performs a record-revision CAS when consuming it.
func (claim AttemptClaim) Valid() bool {
	return claim.issuer != nil && claim.leaseUUID != "" && claim.backendName != "" &&
		validAttemptMetadata(
			claim.leaseUUID, claim.operationID, claim.operationKind,
			claim.restoreSourceLeaseUUID, claim.payloadFingerprint,
			claim.requestSnapshot, claim.callbackPair,
		) && claim.revision.Valid() && claim.nonce != 0 &&
		claim.kind.valid() &&
		(claim.kind != attemptClaimConfirmedGeneration || claim.sameBackendOwner) &&
		claim.revision.issuer == claim.issuer.recordIssuer &&
		claim.revision.leaseUUID == claim.leaseUUID
}

// Metadata returns the exact durable operation identity bound to the claim.
// An invalid claim returns an invalid safe-zero value.
func (claim AttemptClaim) Metadata() AttemptMetadata {
	if !claim.Valid() {
		return AttemptMetadata{}
	}
	return AttemptMetadata{
		operationID:            claim.operationID,
		operationKind:          claim.operationKind,
		restoreSourceLeaseUUID: claim.restoreSourceLeaseUUID,
		payloadFingerprint:     claim.payloadFingerprint,
		requestSnapshot:        claim.requestSnapshot,
		callbackPair:           claim.callbackPair,
	}
}

// Backend returns the store-owned backend bound to the durable generation. An
// invalid claim returns an empty backend.
func (claim AttemptClaim) Backend() string {
	if !claim.Valid() {
		return ""
	}
	return claim.backendName
}

// HasSameBackendOwner reports positive placement evidence for the claimed
// backend. It does not prove that the attempted generation succeeded or that
// the chain lease is ACTIVE; callback recovery must re-read chain state before
// treating a failure as stale.
func (claim AttemptClaim) HasSameBackendOwner() bool {
	return claim.Valid() && claim.sameBackendOwner
}

// RestoreClaim is the exclusive capability for settling one synchronous
// restore dispatch. Its private fields bind it to the issuing store, exact
// source record, target attempt, backend, operation, and process-local nonce.
// The zero value is invalid.
//
// The source claim is intentionally process-local and short-lived: it fences
// source mutation only until the backend synchronously accepts, refuses, or
// returns an ambiguous outcome. The target attempt remains durable across
// process restarts and is the authority after dispatch returns.
type RestoreClaim struct {
	issuer          *Store
	sourceLeaseUUID string
	targetLeaseUUID string
	backendName     string
	operationID     operation.OperationID
	requestSnapshot BackendRequestSnapshot
	callbackPair    CallbackPair
	sourceRevision  RecordRevision
	targetRevision  RecordRevision
	nonce           uint64
}

// Valid reports whether the claim carries every fact required to identify an
// exact restore dispatch. The issuing store still checks its live reservation
// before consuming the claim.
func (claim RestoreClaim) Valid() bool {
	return claim.issuer != nil && claim.sourceLeaseUUID != "" &&
		claim.targetLeaseUUID != "" && claim.sourceLeaseUUID != claim.targetLeaseUUID &&
		claim.backendName != "" && claim.operationID.Valid() &&
		claim.requestSnapshot.Valid() &&
		claim.callbackPair.ValidFor(claim.operationID) &&
		claim.sourceRevision.Valid() && claim.targetRevision.Valid() && claim.nonce != 0 &&
		claim.sourceRevision.issuer == claim.issuer.recordIssuer &&
		claim.sourceRevision.leaseUUID == claim.sourceLeaseUUID &&
		claim.targetRevision.issuer == claim.issuer.recordIssuer &&
		claim.targetRevision.leaseUUID == claim.targetLeaseUUID
}

// Backend returns the confirmed source owner bound to the claim. An invalid
// claim returns an empty backend.
func (claim RestoreClaim) Backend() string {
	if !claim.Valid() {
		return ""
	}
	return claim.backendName
}

// Placement is an immutable snapshot of one cached placement record. Backend
// is the last positively observed owner. Attempt is a write-ahead record of a
// backend call whose outcome is not yet known. SetAt is the record's first-seen
// time and gates orphan pruning.
//
// State and revision deliberately remain opaque. Callers use State and
// Revision to make decisions without being able to mutate store-owned metadata.
type Placement struct {
	Backend string
	Attempt string
	SetAt   time.Time
	// Conflict is a durable quarantine marker: more than one backend reported
	// positive ownership, so no individual backend/status is authoritative.
	Conflict bool
	// ConflictBackends is the sorted durable set of backends that have been
	// positively identified as possible owners while Conflict is true. Keeping
	// the names is essential across restart/reconfiguration: a backend removed
	// from the current router must remain an unresolved deprovision candidate,
	// rather than disappearing from the evidence simply because it is offline.
	ConflictBackends []string
	// ConflictOwnersUnknown marks a legacy or malformed conflict whose complete
	// candidate set was not persisted. Such a record remains fail-closed until an
	// operator resolves it; a complete view of only the current configuration
	// cannot prove that a former owner is gone.
	ConflictOwnersUnknown bool
	// untrustedPositive distinguishes a complete set of rejected positive
	// reporters from a legacy conflict whose owner set is unknown. It remains
	// private so callers cannot manufacture a resolvable quarantine.
	untrustedPositive bool

	unusable bool
	revision uint64
	// recordRevision is attached only to immutable snapshots returned by a Store.
	// It is neither persisted nor trusted independently by another store.
	recordRevision RecordRevision
	// attemptOperationID is present only for attempts begun through the typed
	// API. Every typed attempt also carries an exact kind and, for restore, the
	// authorized source lease UUID. Incomplete combinations decode unusably.
	attemptOperationID            operation.OperationID
	attemptOperationKind          operation.Kind
	attemptRestoreSourceLeaseUUID string
	attemptPayloadFingerprint     PayloadFingerprint
	attemptRequestSnapshot        BackendRequestSnapshot
	attemptCallbackPair           CallbackPair
}

// State returns the placement's derived state. The zero Placement is Absent.
func (p Placement) State() State {
	if p.unusable || p.Conflict {
		return StateUnusable
	}
	if p.Backend != "" {
		return StateConfirmed
	}
	if p.Attempt != "" {
		return StateAttempting
	}
	// Only the completely empty public snapshot is Absent. A timestamp without
	// either placement fact is structurally present but cannot be interpreted.
	if !p.SetAt.IsZero() || p.revision != 0 {
		return StateUnusable
	}
	return StateAbsent
}

// CanResolveUntrustedPositive reports whether a later authoritative positive
// from backendName is the sole fact needed to replace a rejected-observation
// quarantine. The caller must additionally prove that its inventory is complete
// and identity-valid; this value alone grants no mutation authority.
func (p Placement) CanResolveUntrustedPositive(backendName string) bool {
	return backendName != "" && p.Conflict && p.untrustedPositive &&
		!p.ConflictOwnersUnknown && len(p.ConflictBackends) == 1 &&
		p.ConflictBackends[0] == backendName
}

// Revision returns the opaque per-record revision used by conditional writes.
func (p Placement) Revision() uint64 { return p.revision }

// RecordRevision returns the opaque identity of this exact durable snapshot.
// The zero Placement and legacy unrevisioned records return an invalid value.
func (p Placement) RecordRevision() RecordRevision {
	if !p.recordRevision.Valid() || p.recordRevision.value != p.revision {
		return RecordRevision{}
	}
	return p.recordRevision
}

// AttemptOperationID returns the operation identity associated with the
// unresolved attempt. Legacy attempts return an invalid ID.
func (p Placement) AttemptOperationID() operation.OperationID {
	metadata := p.AttemptMetadata()
	if !metadata.Valid() {
		return operation.OperationID{}
	}
	return metadata.OperationID()
}

// AttemptMetadata returns the complete exact-operation identity carried by an
// unresolved attempt. Missing, malformed, and settled attempts return the
// invalid safe-zero value.
func (p Placement) AttemptMetadata() AttemptMetadata {
	if p.Attempt == "" || p.State() == StateUnusable || !validAttemptMetadata(
		p.recordRevision.leaseUUID,
		p.attemptOperationID,
		p.attemptOperationKind,
		p.attemptRestoreSourceLeaseUUID,
		p.attemptPayloadFingerprint,
		p.attemptRequestSnapshot,
		p.attemptCallbackPair,
	) {
		return AttemptMetadata{}
	}
	return AttemptMetadata{
		operationID:            p.attemptOperationID,
		operationKind:          p.attemptOperationKind,
		restoreSourceLeaseUUID: p.attemptRestoreSourceLeaseUUID,
		payloadFingerprint:     p.attemptPayloadFingerprint,
		requestSnapshot:        p.attemptRequestSnapshot,
		callbackPair:           p.attemptCallbackPair,
	}
}

// record is the version-tolerant bbolt representation. The backend and set_at
// field names retain compatibility with the ENG-335 JSON format. Before that
// change, values were raw backend names; decodeRecord still accepts them.
type record struct {
	Backend                string              `json:"backend"`
	Attempt                string              `json:"attempt,omitempty"`
	OperationID            string              `json:"operation_id,omitempty"`
	OperationKind          string              `json:"operation_kind,omitempty"`
	RestoreSourceLeaseUUID string              `json:"restore_source_lease_uuid,omitempty"`
	PayloadHash            string              `json:"payload_hash,omitempty"`
	Tenant                 string              `json:"tenant,omitempty"`
	ProviderUUID           string              `json:"provider_uuid,omitempty"`
	RequestItems           []backend.LeaseItem `json:"request_items,omitempty"`
	CallbackURL            string              `json:"callback_url,omitempty"`
	LifecycleCallbackURL   string              `json:"lifecycle_callback_url,omitempty"`
	SetAt                  time.Time           `json:"set_at"`
	Revision               uint64              `json:"revision,omitempty"`
	Conflict               bool                `json:"conflict,omitempty"`
	ConflictBackends       []string            `json:"conflict_backends,omitempty"`
	ConflictOwnersUnknown  bool                `json:"conflict_owners_unknown,omitempty"`
	UntrustedPositive      bool                `json:"untrusted_positive,omitempty"`
}

// Store is a bbolt-backed placement store with an in-memory read cache. All
// writes commit to bbolt before the cache or revision clock is changed.
type Store struct {
	db             *bolt.DB
	cache          map[string]Placement
	lifecycleCache map[string]lifecycleCapability
	// deleteRevisions fences stale inventory from recreating an exact key that
	// was deleted after its snapshot began. Entries exist only while at least one
	// registered inventory snapshot could still need them, so unrelated keys do
	// not share a global fence and tombstones do not grow for the process lifetime.
	deleteRevisions map[string]uint64
	// activeSnapshots counts registered inventory cutoffs. More than one caller
	// may begin at the same revision, so a refcount is required before tombstones
	// at that cutoff can be pruned.
	activeSnapshots map[uint64]uint64
	now             func() time.Time
	revision        uint64
	authorityEpoch  uint64
	// Backend topology and its admission baseline are loaded from the metadata
	// bucket. The baseline remains durable across process restart, but is usable
	// only while it exactly matches the current topology identity.
	providerUUID        string
	backendTopology     []string
	backendTopologySet  map[string]struct{}
	knownBackendNames   map[string]struct{}
	backendStorageIDs   map[string]backendidentity.ID
	topologyFingerprint string
	topologyID          uint64
	baselineFingerprint string
	baselineTopologyID  uint64
	// emptyInventoryBackends is the latest complete inventory's proof that a
	// configured backend reported neither provisions nor retentions. It is
	// usable for removal only while inventoryTopologyID equals topologyID.
	inventoryTopologyID    uint64
	emptyInventoryBackends map[string]struct{}
	// restoreClaims are process-local source reservations held only across a
	// synchronous backend Restore call. The durable target attempt is the
	// authority after dispatch returns or this process restarts.
	restoreClaims map[string]RestoreClaim
	restoreNonce  uint64
	// attemptClaims are process-local reservations issued only when an
	// authenticated callback names an exact unresolved attempt or confirmed
	// lifecycle generation but its ephemeral Registry record is gone.
	// ProjectInventory observes these claims and fences all submitted evidence
	// until chain and placement settlement finish.
	attemptClaims map[string]AttemptClaim
	attemptNonce  uint64
	recordIssuer  uint64
	// runtimeAuthorityFile retains the exact database inode opened by bbolt.
	// Every authority-bearing read and write re-attests runtimeAuthorityPath
	// against it; runtimeAuthorityGate permanently withdraws authority after a
	// pathname mismatch or an indeterminate commit outcome and linearizes that
	// withdrawal with already-admitted write boundaries.
	runtimeAuthorityPath   string
	runtimeAuthorityFile   *os.File
	runtimeAuthorityGate   *storeauthority.Gate
	runtimeAuthorityMu     sync.RWMutex
	runtimeAuthorityClosed bool
	mu                     sync.RWMutex
	closeOnce              sync.Once
	closeErr               error
}

// Option configures a Store at construction.
type Option func(*Store)

// WithClock injects the clock used to stamp SetAt. Defaults to time.Now.
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// OpenStore opens an existing, fully prepared placement authority without
// creating a file, bucket, schema, or migration write. Production startup uses
// this boundary: an absent/empty database or a v0.13 database that has not been
// sealed by placement-preflight fails before any durable byte can change.
func OpenStore(dbPath, providerUUID string, opts ...Option) (*Store, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("placement db path is required")
	}
	if !canonicalLeaseUUID(providerUUID) {
		return nil, fmt.Errorf("%w: configured provider UUID %q is not canonical",
			ErrProviderAuthorityMismatch, providerUUID)
	}
	info, err := os.Lstat(dbPath)
	if err != nil {
		return nil, fmt.Errorf("stat placement db: %w", err)
	}
	if err := validatePlacementAuthorityFile(dbPath, info); err != nil {
		return nil, err
	}
	if info.Size() == 0 {
		return nil, fmt.Errorf("%w: placement database is empty", ErrPreparationRequired)
	}

	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{
		Timeout: 5 * time.Second,
		OpenFile: func(name string, flag int, mode os.FileMode) (*os.File, error) {
			// #nosec G304 -- dbPath is an absolute clean operator-supplied path;
			// the database is opened without create and checked against the inode
			// obtained before bbolt was invoked.
			if name != dbPath {
				return nil, errors.New("bbolt requested an unexpected placement database path")
			}
			file, openErr := openPlacementAuthorityFileNoFollow(name, flag&^os.O_CREATE, mode)
			if openErr != nil {
				return nil, openErr
			}
			openedInfo, statErr := file.Stat()
			validationErr := validatePlacementAuthorityFile(name, openedInfo)
			if statErr != nil || validationErr != nil || !os.SameFile(info, openedInfo) {
				_ = file.Close()
				if statErr != nil {
					return nil, fmt.Errorf("stat opened placement db: %w", statErr)
				}
				if validationErr != nil {
					return nil, validationErr
				}
				return nil, errors.New("placement db changed between validation and open")
			}
			return file, nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("open existing placement db: %w", err)
	}
	if err := db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(metadataBucketName) == nil {
			return fmt.Errorf(
				"%w: run placement-preflight --prepare against the stopped v0.13 database",
				ErrPreparationRequired,
			)
		}
		return verifyAuthorityBuckets(tx)
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("verify prepared placement db: %w", err)
	}
	store, err := loadStoreWithExpectedAuthority(db, info, opts...)
	if err != nil {
		return nil, err
	}
	if err := store.VerifyProviderUUID(providerUUID); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

// loadStore builds the in-memory authority from an already-open, initialized
// database without performing any schema or migration writes. It takes
// ownership of db and closes it on failure. Offline repair openers can use this
// only after their own strict existing-schema checks.
func loadStore(db *bolt.DB, opts ...Option) (*Store, error) {
	return loadStoreWithExpectedAuthority(db, nil, opts...)
}

func loadStoreWithExpectedAuthority(
	db *bolt.DB,
	expected os.FileInfo,
	opts ...Option,
) (*Store, error) {
	if db == nil {
		return nil, errors.New("placement db is required")
	}
	cache := make(map[string]Placement)
	var lifecycleCache map[string]lifecycleCapability
	var revision uint64
	var metadata topologyMetadata
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return errors.New("placements bucket missing")
		}
		if err := b.ForEach(func(k, v []byte) error {
			p := decodeRecord(string(k), v)
			cache[string(k)] = p
			if p.revision > revision {
				revision = p.revision
			}
			return nil
		}); err != nil {
			return err
		}
		var err error
		lifecycleCache, err = loadLifecycleCapabilities(tx)
		if err != nil {
			return err
		}
		quarantineLifecycleBindings(cache, lifecycleCache)
		metadata, err = loadTopologyMetadata(tx)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to load placement store: %w", err)
	}
	backendStorageIDs := make(map[string]backendidentity.ID, len(metadata.KnownBackendStorageIDs))
	for backendName, encodedID := range metadata.KnownBackendStorageIDs {
		id, err := backendidentity.Parse(encodedID)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf(
				"failed to load backend storage identity for %q: %w", backendName, err,
			)
		}
		backendStorageIDs[backendName] = id
	}

	recordIssuer := recordIssuerSequence.Add(1)
	if recordIssuer == 0 {
		_ = db.Close()
		return nil, errors.New("placement record capability issuer exhausted")
	}
	authorityPath, authorityFile, err := openRuntimeAuthorityIdentity(db, expected)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("bind placement runtime authority: %w", err)
	}
	authorityGate, err := storeauthority.New(
		func(err error) bool {
			return errors.Is(err, ErrRuntimeAuthorityUnavailable) ||
				errors.Is(err, errBoltCommitOutcomeUnknown)
		},
		func(failure error) {
			slog.Error("placement runtime authority withdrawn", "error", failure)
		},
	)
	if err != nil {
		_ = authorityFile.Close()
		_ = db.Close()
		return nil, fmt.Errorf("construct placement runtime authority gate: %w", err)
	}
	s := &Store{
		db:              db,
		cache:           cache,
		lifecycleCache:  lifecycleCache,
		deleteRevisions: make(map[string]uint64),
		activeSnapshots: make(map[uint64]uint64),
		now:             time.Now,
		revision:        revision,
		authorityEpoch:  1,
		providerUUID:    metadata.ProviderUUID,
		backendTopology: slices.Clone(metadata.Topology),
		backendTopologySet: func() map[string]struct{} {
			set := make(map[string]struct{}, len(metadata.Topology))
			for _, backendName := range metadata.Topology {
				set[backendName] = struct{}{}
			}
			return set
		}(),
		knownBackendNames: func() map[string]struct{} {
			set := make(map[string]struct{}, len(metadata.KnownBackends))
			for _, backendName := range metadata.KnownBackends {
				set[backendName] = struct{}{}
			}
			return set
		}(),
		backendStorageIDs:   backendStorageIDs,
		topologyFingerprint: metadata.TopologyFingerprint,
		topologyID:          metadata.TopologyID,
		baselineFingerprint: metadata.BaselineFingerprint,
		baselineTopologyID:  metadata.BaselineTopologyID,
		inventoryTopologyID: metadata.InventoryTopologyID,
		emptyInventoryBackends: func() map[string]struct{} {
			set := make(map[string]struct{}, len(metadata.EmptyInventoryBackends))
			for _, backendName := range metadata.EmptyInventoryBackends {
				set[backendName] = struct{}{}
			}
			return set
		}(),
		restoreClaims:        make(map[string]RestoreClaim),
		attemptClaims:        make(map[string]AttemptClaim),
		recordIssuer:         recordIssuer,
		runtimeAuthorityPath: authorityPath,
		runtimeAuthorityFile: authorityFile,
		runtimeAuthorityGate: authorityGate,
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

// migrateLegacyConfirmedRevisions upgrades the unambiguous confirmed-owner
// records written by v0.13 and earlier onto the revisioned schema. All records
// are examined before any write so new revisions start strictly above every
// existing durable revision. The caller's bbolt update transaction makes the
// entire migration atomic, including revision exhaustion or a failed Put.
//
// Attempts, conflicts, and unusable records deliberately remain byte-for-byte
// untouched. None of them is safe to turn into an exact confirmed-owner
// capability merely because it predates record revisions.
func migrateLegacyConfirmedRevisions(tx *bolt.Tx) error {
	b := tx.Bucket(bucketName)
	if b == nil {
		return errors.New("placements bucket missing")
	}

	type migration struct {
		leaseUUID string
		placement Placement
	}
	var migrations []migration
	var revision uint64
	if err := b.ForEach(func(k, v []byte) error {
		leaseUUID := string(k)
		// v0.13 selected the object decoder only when the first byte was
		// '{'. Consequently printable raw backend names such as "null",
		// "true", "123", and "[]" were valid even though they are also
		// valid non-object JSON roots. The current decoder deliberately rejects
		// those roots, so the one-time migration must reuse the strict historical
		// decoder after the preflight/epoch proof instead of silently leaving a
		// previously confirmed owner unusable. Object rows continue through the
		// current structural decoder so their historical SetAt value is retained.
		p := Placement{}
		if len(v) > 0 && v[0] != '{' {
			if backendName, legacyErr := decodeV013PreflightPlacement(v); legacyErr == nil {
				p = Placement{Backend: backendName}
			} else {
				p = decodeRecord(leaseUUID, v)
			}
		} else {
			p = decodeRecord(leaseUUID, v)
		}
		if p.revision > revision {
			revision = p.revision
		}
		if leaseUUID != "" && p.revision == 0 && p.State() == StateConfirmed &&
			p.Attempt == "" && !p.Conflict {
			migrations = append(migrations, migration{
				leaseUUID: leaseUUID,
				placement: p,
			})
		}
		return nil
	}); err != nil {
		return fmt.Errorf("scan legacy placement revisions: %w", err)
	}

	for _, migration := range migrations {
		if revision == math.MaxUint64 {
			return errors.New("placement revision exhausted during legacy migration")
		}
		revision++
		migration.placement.revision = revision
		encoded, err := encodePlacement(migration.placement)
		if err != nil {
			return fmt.Errorf("encode migrated placement %q: %w", migration.leaseUUID, err)
		}
		if err := b.Put([]byte(migration.leaseUUID), encoded); err != nil {
			return fmt.Errorf("write migrated placement %q: %w", migration.leaseUUID, err)
		}
	}
	return nil
}

// decodeRecord parses the current JSON representation or the pre-ENG-335 raw
// backend-name representation. An empty raw value, malformed JSON object, or
// JSON object with neither Backend nor Attempt remains present but Unusable.
func decodeRecord(leaseUUID string, v []byte) Placement {
	if len(v) > 0 && v[0] != '{' {
		if json.Valid(v) {
			return unusableRecord(leaseUUID, errors.New("placement JSON root is not an object"))
		}
		if !validLegacyBackendName(v) {
			return unusableRecord(leaseUUID, errors.New("legacy backend name is not printable UTF-8"))
		}
		return Placement{Backend: string(v)}
	}

	var r record
	if len(v) == 0 {
		return unusableRecord(leaseUUID, errors.New("empty value"))
	}
	fields, err := decodeUniqueJSONObject(v)
	if err != nil {
		return unusableRecord(leaseUUID, fmt.Errorf("invalid placement JSON object: %w", err))
	}
	if err := json.Unmarshal(v, &r); err != nil {
		return unusableRecord(leaseUUID, err)
	}

	operationID, operationErr := decodeOperationID(r.OperationID)
	operationKind, kindErr := decodeOperationKind(r.OperationKind)
	payloadFingerprint, fingerprintErr := decodePayloadFingerprint(r.PayloadHash)
	requestItemsRaw, requestItemsPresent := fields["request_items"]
	requestSnapshot, requestErr := decodeBackendRequestSnapshot(
		r.Tenant, r.ProviderUUID, r.RequestItems, requestItemsRaw, requestItemsPresent,
	)
	callbackPair, callbackErr := decodeCallbackPair(
		operationID, r.CallbackURL, r.LifecycleCallbackURL,
	)
	p := Placement{
		Backend:                       r.Backend,
		Attempt:                       r.Attempt,
		SetAt:                         r.SetAt,
		Conflict:                      r.Conflict,
		ConflictBackends:              normalizeBackendNames(r.ConflictBackends),
		ConflictOwnersUnknown:         r.ConflictOwnersUnknown,
		untrustedPositive:             r.UntrustedPositive,
		revision:                      r.Revision,
		attemptOperationID:            operationID,
		attemptOperationKind:          operationKind,
		attemptRestoreSourceLeaseUUID: r.RestoreSourceLeaseUUID,
		attemptPayloadFingerprint:     payloadFingerprint,
		attemptRequestSnapshot:        requestSnapshot,
		attemptCallbackPair:           callbackPair,
	}
	if operationErr != nil {
		return unusableRecord(leaseUUID, operationErr)
	}
	if kindErr != nil {
		return unusableRecord(leaseUUID, kindErr)
	}
	if fingerprintErr != nil {
		return unusableRecord(leaseUUID, fingerprintErr)
	}
	if requestErr != nil {
		return unusableRecord(leaseUUID, requestErr)
	}
	if callbackErr != nil {
		return unusableRecord(leaseUUID, callbackErr)
	}
	if p.Conflict {
		// Records written before conflict candidates were introduced contain only
		// conflict=true. Preserve them, but never mistake the missing owner set for
		// proof that the current router represents the whole historical fleet.
		if p.untrustedPositive && len(p.ConflictBackends) == 0 {
			return unusableRecord(leaseUUID, errors.New(
				"untrusted positive quarantine has no backend candidates",
			))
		}
		if !p.untrustedPositive && len(p.ConflictBackends) < 2 {
			p.ConflictOwnersUnknown = true
		}
	} else {
		// Candidate metadata has meaning only while the quarantine is active.
		p.ConflictBackends = nil
		p.ConflictOwnersUnknown = false
		p.untrustedPositive = false
	}
	if p.Attempt == "" {
		if p.attemptOperationID.Valid() || p.attemptOperationKind.Valid() ||
			p.attemptRestoreSourceLeaseUUID != "" || p.attemptPayloadFingerprint.Valid() ||
			p.attemptRequestSnapshot.Valid() ||
			p.attemptCallbackPair.ValidFor(p.attemptOperationID) {
			if p.Backend == "" || !validAttemptMetadata(
				leaseUUID,
				p.attemptOperationID,
				p.attemptOperationKind,
				p.attemptRestoreSourceLeaseUUID,
				p.attemptPayloadFingerprint,
				p.attemptRequestSnapshot,
				p.attemptCallbackPair,
			) {
				return unusableRecord(leaseUUID, errors.New("confirmed operation metadata is incomplete"))
			}
		}
	} else {
		if !validAttemptMetadata(
			leaseUUID,
			p.attemptOperationID,
			p.attemptOperationKind,
			p.attemptRestoreSourceLeaseUUID,
			p.attemptPayloadFingerprint,
			p.attemptRequestSnapshot,
			p.attemptCallbackPair,
		) {
			return unusableRecord(leaseUUID, errors.New("attempt is missing exact operation metadata"))
		}
	}
	if p.Backend == "" && p.Attempt == "" && !p.Conflict {
		p.unusable = true
		slog.Warn("placement: loaded record with no backend or attempt",
			"lease_uuid", leaseUUID)
	}
	return p
}

func decodeOperationKind(value string) (operation.Kind, error) {
	if value == "" {
		return operation.KindInvalid, nil
	}
	kind, err := operation.ParseKind(value)
	if err != nil {
		return operation.KindInvalid, fmt.Errorf("invalid persisted operation kind: %w", err)
	}
	return kind, nil
}

func decodePayloadFingerprint(value string) (PayloadFingerprint, error) {
	if value == "" {
		return PayloadFingerprint{}, nil
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return PayloadFingerprint{}, fmt.Errorf("invalid persisted payload fingerprint: %w", err)
	}
	fingerprint, err := NewPayloadFingerprint(decoded)
	if err != nil || !fingerprint.Valid() || fingerprint.String() != value {
		return PayloadFingerprint{}, errors.New(
			"invalid persisted payload fingerprint: expected canonical SHA-256",
		)
	}
	return fingerprint, nil
}

func decodeBackendRequestSnapshot(
	tenant string,
	providerUUID string,
	items []backend.LeaseItem,
	rawItems json.RawMessage,
	itemsPresent bool,
) (BackendRequestSnapshot, error) {
	if tenant == "" && providerUUID == "" && !itemsPresent {
		return BackendRequestSnapshot{}, nil
	}
	if !itemsPresent {
		return BackendRequestSnapshot{}, errors.New(
			"invalid persisted backend request snapshot: request_items is missing",
		)
	}
	snapshot, err := NewBackendRequestSnapshot(tenant, providerUUID, items)
	if err != nil {
		return BackendRequestSnapshot{}, fmt.Errorf(
			"invalid persisted backend request snapshot: %w", err,
		)
	}
	// The writer emits one canonical nested representation. Requiring that exact
	// form rejects duplicate item keys, invalid UTF-8, unknown fields, and
	// alternate structures that encoding/json would otherwise normalize away.
	if !bytes.Equal(rawItems, []byte(snapshot.itemsJSON)) {
		return BackendRequestSnapshot{}, errors.New(
			"invalid persisted backend request snapshot: request_items is not canonical",
		)
	}
	return snapshot, nil
}

func decodeCallbackPair(
	operationID operation.OperationID,
	operationURL string,
	lifecycleURL string,
) (CallbackPair, error) {
	if operationURL == "" && lifecycleURL == "" && !operationID.Valid() {
		return CallbackPair{}, nil
	}
	pair, err := NewCallbackPair(operationID, operationURL, lifecycleURL)
	if err != nil {
		return CallbackPair{}, fmt.Errorf("invalid persisted callback pair: %w", err)
	}
	return pair, nil
}

func decodeOperationID(value string) (operation.OperationID, error) {
	if value == "" {
		return operation.OperationID{}, nil
	}
	id, err := operation.ParseID(value)
	if err != nil {
		return operation.OperationID{}, fmt.Errorf("invalid persisted operation identity: %w", err)
	}
	return id, nil
}

func validLegacyBackendName(v []byte) bool {
	if !utf8.Valid(v) {
		return false
	}
	for _, r := range string(v) {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func unusableRecord(leaseUUID string, err error) Placement {
	slog.Warn("placement: loaded unparseable record",
		"lease_uuid", leaseUUID, "error", err)
	return Placement{unusable: true}
}

func encodePlacement(p Placement) ([]byte, error) {
	operationID := ""
	operationKind := ""
	if p.attemptOperationID.Valid() {
		operationID = p.attemptOperationID.String()
	}
	if p.attemptOperationKind.Valid() {
		operationKind = p.attemptOperationKind.String()
	}
	return json.Marshal(record{
		Backend:                p.Backend,
		Attempt:                p.Attempt,
		OperationID:            operationID,
		OperationKind:          operationKind,
		RestoreSourceLeaseUUID: p.attemptRestoreSourceLeaseUUID,
		PayloadHash:            p.attemptPayloadFingerprint.String(),
		Tenant:                 p.attemptRequestSnapshot.Tenant(),
		ProviderUUID:           p.attemptRequestSnapshot.ProviderUUID(),
		RequestItems:           p.attemptRequestSnapshot.Items(),
		CallbackURL:            p.attemptCallbackPair.OperationURL(),
		LifecycleCallbackURL:   p.attemptCallbackPair.LifecycleURL(),
		SetAt:                  p.SetAt,
		Revision:               p.revision,
		Conflict:               p.Conflict,
		ConflictBackends:       normalizeBackendNames(p.ConflictBackends),
		ConflictOwnersUnknown:  p.ConflictOwnersUnknown,
		UntrustedPositive:      p.untrustedPositive,
	})
}

func normalizeBackendNames(names []string) []string {
	unique := make(map[string]struct{}, len(names))
	for _, name := range names {
		if name != "" {
			unique[name] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(unique))
}

func equalPlacementIgnoringRevision(a, b Placement) bool {
	return a.Backend == b.Backend &&
		a.Attempt == b.Attempt &&
		a.SetAt.Equal(b.SetAt) &&
		a.Conflict == b.Conflict &&
		slices.Equal(a.ConflictBackends, b.ConflictBackends) &&
		a.ConflictOwnersUnknown == b.ConflictOwnersUnknown &&
		a.untrustedPositive == b.untrustedPositive &&
		a.attemptOperationID == b.attemptOperationID &&
		a.attemptOperationKind == b.attemptOperationKind &&
		a.attemptRestoreSourceLeaseUUID == b.attemptRestoreSourceLeaseUUID &&
		a.attemptPayloadFingerprint == b.attemptPayloadFingerprint &&
		a.attemptRequestSnapshot == b.attemptRequestSnapshot &&
		a.attemptCallbackPair == b.attemptCallbackPair &&
		a.unusable == b.unusable
}

// Lookup returns an immutable placement snapshot. A missing key returns the
// zero Placement (StateAbsent).
func (s *Store) Lookup(leaseUUID string) Placement {
	if err := s.reattestRuntimeAuthority(); err != nil {
		return unavailablePlacement()
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	p := s.cache[leaseUUID]
	p.ConflictBackends = slices.Clone(p.ConflictBackends)
	p.recordRevision = s.newRecordRevision(leaseUUID, p.revision)
	return p
}

// List returns a point-in-time copy of every cached placement, including
// StateUnusable records. Mutating the returned map cannot affect the store.
func (s *Store) List() map[string]Placement {
	if err := s.reattestRuntimeAuthority(); err != nil {
		s.mu.RLock()
		defer s.mu.RUnlock()
		out := make(map[string]Placement, len(s.cache))
		for leaseUUID := range s.cache {
			out[leaseUUID] = unavailablePlacement()
		}
		return out
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := maps.Clone(s.cache)
	for leaseUUID, p := range out {
		p.ConflictBackends = slices.Clone(p.ConflictBackends)
		p.recordRevision = s.newRecordRevision(leaseUUID, p.revision)
		out[leaseUUID] = p
	}
	return out
}

// BeginInventorySession registers and returns a typed inventory boundary.
// Callers must pair it with EndInventorySession even when collection fails.
func (s *Store) BeginInventorySession() InventoryFence {
	if err := s.reattestRuntimeAuthority(); err != nil {
		return InventoryFence{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// A new collection supersedes every process-local inventory fence issued
	// from an older collection. The durable admission baseline is deliberately
	// independent and remains usable while the topology is unchanged.
	s.advanceAuthorityEpochLocked()
	fence := s.inventoryFenceLocked()
	s.activeSnapshots[fence.revision]++
	return fence
}

// EndInventorySession releases a typed boundary returned by
// BeginInventorySession. Invalid or foreign fences are harmless no-ops. An
// authority-invalidated fence still releases its registered snapshot.
func (s *Store) EndInventorySession(fence InventoryFence) {
	if !fence.Valid() || fence.issuer != s {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endInventorySnapshotLocked(fence.revision)
}

// InventoryBootstrapped reports whether a complete fleet inventory was
// durably committed for the currently configured backend topology. It remains
// true across process restart and ordinary inventory sessions. A topology
// change makes the prior baseline inapplicable until a complete projection
// commits for the new topology.
func (s *Store) InventoryBootstrapped() bool {
	if err := s.reattestRuntimeAuthority(); err != nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hasCurrentAdmissionBaselineLocked()
}

// Caller holds s.mu.
func (s *Store) advanceAuthorityEpochLocked() {
	s.authorityEpoch++
	if s.authorityEpoch == 0 {
		// Epoch wrap is not operationally reachable, but zero is reserved for an
		// invalid fence and must never be issued.
		s.authorityEpoch = 1
	}
}

// Caller holds at least s.mu.RLock.
func (s *Store) inventoryFenceLocked() InventoryFence {
	return InventoryFence{issuer: s, revision: s.revision, epoch: s.authorityEpoch}
}

// Caller holds s.mu.
func (s *Store) endInventorySnapshotLocked(revision uint64) {

	count := s.activeSnapshots[revision]
	switch count {
	case 0:
		return
	case 1:
		delete(s.activeSnapshots, revision)
	default:
		s.activeSnapshots[revision] = count - 1
	}
	s.pruneDeleteRevisionsLocked()
}

func validateTypedAttempt(
	leaseUUID, backendName string,
	operationID operation.OperationID,
) error {
	if err := validateIDs(leaseUUID, backendName); err != nil {
		return err
	}
	if !operationID.Valid() {
		return operation.ErrInvalidID
	}
	return nil
}

// Caller holds at least s.mu.RLock.
func (s *Store) newAttemptToken(
	leaseUUID, backendName string,
	operationID operation.OperationID,
	operationKind operation.Kind,
	restoreSourceLeaseUUID string,
	payloadFingerprint PayloadFingerprint,
	requestSnapshot BackendRequestSnapshot,
	callbackPair CallbackPair,
	revision uint64,
) AttemptToken {
	token := AttemptToken{
		issuer:                 s,
		leaseUUID:              leaseUUID,
		backendName:            backendName,
		operationID:            operationID,
		operationKind:          operationKind,
		restoreSourceLeaseUUID: restoreSourceLeaseUUID,
		payloadFingerprint:     payloadFingerprint,
		requestSnapshot:        requestSnapshot,
		callbackPair:           callbackPair,
		revision:               s.newRecordRevision(leaseUUID, revision),
	}
	if !token.Valid() {
		return AttemptToken{}
	}
	return token
}

// ClaimAttempt reserves the exact typed durable generation named by operationID
// for callback recovery. The generation may still be an unresolved Attempt, or
// it may have been promoted by matching paired-generation inventory while the
// Registry was empty. In the latter case the exact lifecycle capability
// preserves the operation identity needed to finish chain acknowledgement
// safely. Backend is always derived from the store rather than body-supplied
// metadata. Missing,
// legacy, unusable, and mismatched generations return claimed=false.
// Contention returns ErrAttemptClaimed so a durable callback outbox retries.
func (s *Store) ClaimAttempt(
	leaseUUID string,
	operationID operation.OperationID,
) (claim AttemptClaim, claimed bool, err error) {
	if leaseUUID == "" {
		return AttemptClaim{}, false, fmt.Errorf("%w: lease UUID is required", ErrInvalidPlacement)
	}
	if !operationID.Valid() {
		return AttemptClaim{}, false, operation.ErrInvalidID
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return AttemptClaim{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	p, exists := s.cache[leaseUUID]
	if !exists || p.State() == StateUnusable || p.revision == 0 {
		return AttemptClaim{}, false, nil
	}

	var (
		backendName      string
		kind             attemptClaimKind
		sameBackendOwner bool
	)
	switch {
	case p.Attempt != "":
		if p.attemptOperationID != operationID ||
			(p.Backend != "" && p.Backend != p.Attempt) {
			return AttemptClaim{}, false, nil
		}
		backendName = p.Attempt
		kind = attemptClaimUnresolved
		sameBackendOwner = p.Backend == p.Attempt

	case p.State() == StateConfirmed:
		id, idErr := lifecycleIDForOperation(operationID)
		if idErr != nil {
			return AttemptClaim{}, false, nil
		}
		capability, capabilityExists := s.lifecycleCache[leaseUUID]
		authorization := authorizeLifecycleCapability(capability, id)
		if !capabilityExists || !authorization.Authorized() ||
			authorization.Backend() != p.Backend {
			return AttemptClaim{}, false, nil
		}
		backendName = p.Backend
		kind = attemptClaimConfirmedGeneration
		sameBackendOwner = true

	default:
		return AttemptClaim{}, false, nil
	}
	if !validAttemptMetadata(
		leaseUUID,
		p.attemptOperationID,
		p.attemptOperationKind,
		p.attemptRestoreSourceLeaseUUID,
		p.attemptPayloadFingerprint,
		p.attemptRequestSnapshot,
		p.attemptCallbackPair,
	) {
		return AttemptClaim{}, false, nil
	}
	if s.attemptClaimedLocked(leaseUUID) {
		return AttemptClaim{}, false, fmt.Errorf("%w: lease %q", ErrAttemptClaimed, leaseUUID)
	}
	if s.attemptNonce == math.MaxUint64 {
		return AttemptClaim{}, false, errors.New("placement attempt claim nonce exhausted")
	}
	s.attemptNonce++
	claim = AttemptClaim{
		issuer:                 s,
		leaseUUID:              leaseUUID,
		backendName:            backendName,
		operationID:            operationID,
		operationKind:          p.attemptOperationKind,
		restoreSourceLeaseUUID: p.attemptRestoreSourceLeaseUUID,
		payloadFingerprint:     p.attemptPayloadFingerprint,
		requestSnapshot:        p.attemptRequestSnapshot,
		callbackPair:           p.attemptCallbackPair,
		revision:               s.newRecordRevision(leaseUUID, p.revision),
		nonce:                  s.attemptNonce,
		kind:                   kind,
		sameBackendOwner:       sameBackendOwner,
	}
	if !claim.Valid() {
		return AttemptClaim{}, false, ErrInvalidAttemptToken
	}
	s.attemptClaims[leaseUUID] = claim
	return claim, true, nil
}

// ReleaseAttemptClaim releases only the exact live recovery claim. A stale,
// foreign, or already-consumed claim returns false without changing state.
func (s *Store) ReleaseAttemptClaim(claim AttemptClaim) bool {
	if !claim.Valid() || claim.issuer != s {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.attemptClaims[claim.leaseUUID]
	if !exists || current != claim {
		return false
	}
	delete(s.attemptClaims, claim.leaseUUID)
	return true
}

// ConfirmClaimedAttempt promotes an unresolved attempt or idempotently verifies
// the exact confirmed generation reserved by claim. The live claim is consumed
// for every valid settlement attempt; a failed durable write leaves unresolved
// authority intact so a later callback can claim and retry it.
func (s *Store) ConfirmClaimedAttempt(claim AttemptClaim) (bool, error) {
	if !claim.Valid() || claim.issuer != s {
		return false, ErrInvalidAttemptToken
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		s.ReleaseAttemptClaim(claim)
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.matchAttemptClaimLocked(claim) {
		return false, nil
	}
	defer delete(s.attemptClaims, claim.leaseUUID)
	if claim.kind == attemptClaimConfirmedGeneration {
		return true, nil
	}
	return s.confirmAttemptLocked(claim.attemptToken())
}

// RefuseClaimedAttempt clears only the exact unresolved attempt reserved by
// claim. An already-confirmed generation is verified without demoting it. The
// live claim is consumed for every valid settlement attempt; a failed durable
// write leaves unresolved authority intact for callback retry.
func (s *Store) RefuseClaimedAttempt(claim AttemptClaim) (bool, error) {
	if !claim.Valid() || claim.issuer != s {
		return false, ErrInvalidAttemptToken
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		s.ReleaseAttemptClaim(claim)
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.matchAttemptClaimLocked(claim) {
		return false, nil
	}
	defer delete(s.attemptClaims, claim.leaseUUID)
	if claim.kind == attemptClaimConfirmedGeneration {
		return true, nil
	}
	return s.refuseAttemptLocked(claim.attemptToken())
}

func (claim AttemptClaim) attemptToken() AttemptToken {
	if !claim.Valid() || claim.kind != attemptClaimUnresolved {
		return AttemptToken{}
	}
	return AttemptToken{
		issuer:                 claim.issuer,
		leaseUUID:              claim.leaseUUID,
		backendName:            claim.backendName,
		operationID:            claim.operationID,
		operationKind:          claim.operationKind,
		restoreSourceLeaseUUID: claim.restoreSourceLeaseUUID,
		payloadFingerprint:     claim.payloadFingerprint,
		requestSnapshot:        claim.requestSnapshot,
		callbackPair:           claim.callbackPair,
		revision:               claim.revision,
	}
}

// Caller holds s.mu.
func (s *Store) matchAttemptClaimLocked(claim AttemptClaim) bool {
	current, exists := s.attemptClaims[claim.leaseUUID]
	if !exists || current != claim {
		return false
	}
	if claim.kind == attemptClaimUnresolved {
		_, matches := s.matchAttemptTokenLocked(claim.attemptToken())
		return matches
	}
	p, exists := s.cache[claim.leaseUUID]
	if !exists || p.revision != claim.revision.value || p.State() != StateConfirmed ||
		p.Backend != claim.backendName || p.Attempt != "" {
		return false
	}
	id, err := lifecycleIDForOperation(claim.operationID)
	if err != nil {
		return false
	}
	capability, capabilityExists := s.lifecycleCache[claim.leaseUUID]
	authorization := authorizeLifecycleCapability(capability, id)
	return capabilityExists && authorization.Authorized() &&
		authorization.Backend() == claim.backendName
}

// Caller holds at least s.mu.RLock.
func (s *Store) attemptClaimedLocked(leaseUUID string) bool {
	_, claimed := s.attemptClaims[leaseUUID]
	return claimed
}

// beginRestore is the package-private unchecked source-revision variant used
// by placement tests. Production restore admission must use
// BeginAuthorizedRestore so tenant authorization remains bound to the exact
// source owner and revision that is claimed.
func (s *Store) beginRestore(
	baseline AdmissionBaseline,
	sourceLeaseUUID, targetLeaseUUID string,
	operationID operation.OperationID,
	requestSnapshot BackendRequestSnapshot,
	callbackPair CallbackPair,
) (RestoreClaim, error) {
	return s.beginRestoreWithSourceRevision(
		baseline, RecordRevision{}, sourceLeaseUUID, targetLeaseUUID, operationID,
		requestSnapshot, callbackPair,
	)
}

// BeginAuthorizedRestore atomically claims one previously authorized source
// revision for synchronous restore dispatch and durably records the target
// attempt on that same source backend. Restore authorization presents this
// opaque revision so a concurrent placement owner change cannot redirect the
// authorized command to a different backend.
func (s *Store) BeginAuthorizedRestore(
	baseline AdmissionBaseline,
	sourceRevision RecordRevision,
	targetLeaseUUID string,
	operationID operation.OperationID,
	requestSnapshot BackendRequestSnapshot,
	callbackPair CallbackPair,
) (RestoreClaim, error) {
	if !sourceRevision.Valid() || sourceRevision.issuer != s.recordIssuer {
		return RestoreClaim{}, ErrInvalidRecordRevision
	}
	return s.beginRestoreWithSourceRevision(
		baseline, sourceRevision, sourceRevision.leaseUUID, targetLeaseUUID, operationID,
		requestSnapshot, callbackPair,
	)
}

func (s *Store) beginRestoreWithSourceRevision(
	baseline AdmissionBaseline,
	expectedSource RecordRevision,
	sourceLeaseUUID, targetLeaseUUID string,
	operationID operation.OperationID,
	requestSnapshot BackendRequestSnapshot,
	callbackPair CallbackPair,
) (RestoreClaim, error) {
	if sourceLeaseUUID == "" {
		return RestoreClaim{}, fmt.Errorf("%w: source lease UUID is required", ErrInvalidPlacement)
	}
	if targetLeaseUUID == "" {
		return RestoreClaim{}, fmt.Errorf("%w: target lease UUID is required", ErrInvalidPlacement)
	}
	if sourceLeaseUUID == targetLeaseUUID {
		return RestoreClaim{}, fmt.Errorf("%w: source and target lease UUIDs must differ", ErrInvalidPlacement)
	}
	if !operationID.Valid() {
		return RestoreClaim{}, operation.ErrInvalidID
	}
	if !requestSnapshot.Valid() {
		return RestoreClaim{}, ErrInvalidRestoreClaim
	}
	if !callbackPair.ValidFor(operationID) {
		return RestoreClaim{}, ErrInvalidRestoreClaim
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return RestoreClaim{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateAdmissionBaselineLocked(baseline); err != nil {
		return RestoreClaim{}, err
	}
	if s.restoreSourceClaimedLocked(sourceLeaseUUID) {
		return RestoreClaim{}, fmt.Errorf("%w: lease %q", ErrRestoreSourceClaimed, sourceLeaseUUID)
	}
	// A source-reserved lease cannot simultaneously become another restore's
	// target. This is the restore counterpart to the typed attempt-admission
	// fence and keeps the source immutable for the holder of the first claim.
	if s.restoreSourceClaimedLocked(targetLeaseUUID) {
		return RestoreClaim{}, fmt.Errorf("%w: lease %q", ErrRestoreSourceClaimed, targetLeaseUUID)
	}

	source, exists := s.cache[sourceLeaseUUID]
	if !exists || source.State() == StateAbsent {
		return RestoreClaim{}, fmt.Errorf("%w: lease %q", ErrRestoreSourceNotFound, sourceLeaseUUID)
	}
	sourceRevision := s.newRecordRevision(sourceLeaseUUID, source.revision)
	if source.State() != StateConfirmed || source.Attempt != "" ||
		!sourceRevision.Valid() {
		return RestoreClaim{}, fmt.Errorf("%w: lease %q", ErrRestoreSourceUnavailable, sourceLeaseUUID)
	}
	if expectedSource.Valid() && expectedSource != sourceRevision {
		return RestoreClaim{}, fmt.Errorf(
			"%w: lease %q changed after authorization",
			ErrRestoreSourceUnavailable, sourceLeaseUUID,
		)
	}
	if err := s.validateConfiguredBackendLocked(source.Backend); err != nil {
		return RestoreClaim{}, err
	}
	target, targetExists := s.cache[targetLeaseUUID]
	if targetExists || target.State() != StateAbsent {
		return RestoreClaim{}, fmt.Errorf("%w: lease %q is %s",
			ErrRestoreTargetUnavailable, targetLeaseUUID, target.State())
	}
	if s.restoreNonce == math.MaxUint64 {
		return RestoreClaim{}, errors.New("placement restore nonce exhausted")
	}
	nonce := s.restoreNonce + 1

	targetRevision, err := s.setAttemptingLocked(
		targetLeaseUUID, source.Backend, operationID,
		operation.KindRestore, sourceLeaseUUID, PayloadFingerprint{},
		requestSnapshot, callbackPair,
	)
	if err != nil {
		return RestoreClaim{}, err
	}

	claim := RestoreClaim{
		issuer:          s,
		sourceLeaseUUID: sourceLeaseUUID,
		targetLeaseUUID: targetLeaseUUID,
		backendName:     source.Backend,
		operationID:     operationID,
		requestSnapshot: requestSnapshot,
		callbackPair:    callbackPair,
		sourceRevision:  sourceRevision,
		targetRevision:  s.newRecordRevision(targetLeaseUUID, targetRevision),
		nonce:           nonce,
	}
	if !claim.Valid() {
		// This is unreachable after the validation above. Returning an error is
		// still safer than exposing a durable attempt with no settlement claim.
		return RestoreClaim{}, ErrInvalidRestoreClaim
	}
	s.restoreNonce = nonce
	s.restoreClaims[sourceLeaseUUID] = claim
	return claim, nil
}

// ConfirmRestore consumes the exact live source claim and promotes the target
// attempt after synchronous backend acceptance. If an exact fast callback has
// already settled the target, it leaves that result untouched and still
// consumes the source claim successfully.
func (s *Store) ConfirmRestore(claim RestoreClaim) (bool, error) {
	return s.settleRestore(claim, restoreSettlementConfirm)
}

// RefuseRestore consumes the exact live source claim and clears the target
// attempt after a definitive synchronous refusal. An exact fast callback wins:
// its already-settled target is never deleted or rewritten.
func (s *Store) RefuseRestore(claim RestoreClaim) (bool, error) {
	return s.settleRestore(claim, restoreSettlementRefuse)
}

// AbandonRestore consumes the exact live source claim without changing the
// durable target attempt. Call it for an ambiguous synchronous outcome: the
// exact backend callback or a later matching paired-generation inventory
// observation owns settlement after dispatch.
func (s *Store) AbandonRestore(claim RestoreClaim) (bool, error) {
	return s.settleRestore(claim, restoreSettlementAbandon)
}

type restoreSettlement uint8

const (
	restoreSettlementConfirm restoreSettlement = iota + 1
	restoreSettlementRefuse
	restoreSettlementAbandon
)

func (s *Store) settleRestore(claim RestoreClaim, settlement restoreSettlement) (bool, error) {
	if !claim.Valid() || claim.issuer != s {
		return false, ErrInvalidRestoreClaim
	}
	authorityErr := s.reattestRuntimeAuthority()

	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.restoreClaims[claim.sourceLeaseUUID]
	if !exists || current != claim {
		return false, nil
	}
	// Source reservations must never leak beyond synchronous dispatch, even if
	// the target settlement write itself fails. The unresolved durable target
	// attempt is the conservative recovery state in that case.
	defer delete(s.restoreClaims, claim.sourceLeaseUUID)
	if authorityErr != nil {
		return true, authorityErr
	}

	if settlement == restoreSettlementAbandon {
		return true, nil
	}
	if s.attemptClaimedLocked(claim.targetLeaseUUID) {
		return true, fmt.Errorf("%w: lease %q", ErrAttemptClaimed, claim.targetLeaseUUID)
	}

	targetToken := AttemptToken{
		issuer:                 s,
		leaseUUID:              claim.targetLeaseUUID,
		backendName:            claim.backendName,
		operationID:            claim.operationID,
		operationKind:          operation.KindRestore,
		restoreSourceLeaseUUID: claim.sourceLeaseUUID,
		payloadFingerprint:     PayloadFingerprint{},
		requestSnapshot:        claim.requestSnapshot,
		callbackPair:           claim.callbackPair,
		revision:               claim.targetRevision,
	}
	target, matches := s.matchAttemptTokenLocked(targetToken)
	if !matches {
		// Exact callback settlement (or a later authoritative mutation) wins. A
		// synchronous response must never recreate or delete that result.
		return true, nil
	}
	if target.State() == StateUnusable {
		return true, fmt.Errorf("%w: lease %q", ErrUnusablePlacement, claim.targetLeaseUUID)
	}

	switch settlement {
	case restoreSettlementConfirm:
		_, err := s.promoteExactAttemptLocked(attemptPromotionGuard{
			leaseUUID:        claim.targetLeaseUUID,
			backendName:      claim.backendName,
			operationID:      claim.operationID,
			expectedRevision: claim.targetRevision,
			mutation:         "confirm restore placement",
		})
		return true, err

	case restoreSettlementRefuse:
		if target.Backend == "" {
			if err := s.deleteLocked(claim.targetLeaseUUID, "refuse restore placement"); err != nil {
				return true, err
			}
			return true, nil
		}
		next, err := s.nextRevision()
		if err != nil {
			return true, err
		}
		target.Attempt = ""
		clearOperationMetadata(&target)
		target.revision = next
		capability := clearAttemptLifecycle(
			s.lifecycleCache[claim.targetLeaseUUID], claim.backendName, claim.operationID,
		)
		if err := s.putPlacementWithLifecycleLocked(
			claim.targetLeaseUUID, target, capability, "refuse restore placement",
		); err != nil {
			return true, err
		}
		s.revision = next
		return true, nil

	default:
		return true, ErrInvalidRestoreClaim
	}
}

// Caller holds at least s.mu.RLock.
func (s *Store) restoreSourceClaimedLocked(leaseUUID string) bool {
	_, claimed := s.restoreClaims[leaseUUID]
	return claimed
}

// Caller holds s.mu.
func (s *Store) setAttemptingLocked(
	leaseUUID, backendName string,
	operationID operation.OperationID,
	operationKind operation.Kind,
	restoreSourceLeaseUUID string,
	payloadFingerprint PayloadFingerprint,
	requestSnapshot BackendRequestSnapshot,
	callbackPair CallbackPair,
) (uint64, error) {
	if !validAttemptMetadata(
		leaseUUID, operationID, operationKind, restoreSourceLeaseUUID,
		payloadFingerprint, requestSnapshot, callbackPair,
	) {
		return 0, operation.ErrInvalidKind
	}
	existing, exists := s.cache[leaseUUID]
	if exists && existing.State() == StateUnusable {
		return 0, fmt.Errorf("%w: lease %q", ErrUnusablePlacement, leaseUUID)
	}
	if existing.Attempt != "" {
		return 0, fmt.Errorf("%w: lease %q targets %q", ErrAttemptConflict, leaseUUID, existing.Attempt)
	}
	if existing.Backend != "" && existing.Backend != backendName {
		return 0, fmt.Errorf("%w: lease %q is confirmed on %q, not %q",
			ErrBackendConflict, leaseUUID, existing.Backend, backendName)
	}

	revision, err := s.nextRevision()
	if err != nil {
		return 0, err
	}
	p := existing
	if !exists {
		p.SetAt = s.now().UTC()
	}
	capability, err := s.lifecycleWithAttemptLocked(leaseUUID, backendName, operationID)
	if err != nil {
		return 0, err
	}
	p.Attempt = backendName
	p.attemptOperationID = operationID
	p.attemptOperationKind = operationKind
	p.attemptRestoreSourceLeaseUUID = restoreSourceLeaseUUID
	p.attemptPayloadFingerprint = payloadFingerprint
	p.attemptRequestSnapshot = requestSnapshot
	p.attemptCallbackPair = callbackPair
	p.revision = revision
	if err := s.putPlacementWithLifecycleLocked(
		leaseUUID, p, capability, "set attempting placement",
	); err != nil {
		return 0, err
	}
	s.revision = revision
	return revision, nil
}

func clearOperationMetadata(p *Placement) {
	if p == nil {
		return
	}
	p.attemptOperationID = operation.OperationID{}
	p.attemptOperationKind = operation.KindInvalid
	p.attemptRestoreSourceLeaseUUID = ""
	p.attemptPayloadFingerprint = PayloadFingerprint{}
	p.attemptRequestSnapshot = BackendRequestSnapshot{}
	p.attemptCallbackPair = CallbackPair{}
}

// attemptPromotionGuard names every durable fact that must still match before
// an attempt can become current ownership. Callers may differ in how they
// obtained the guard, but all confirmation paths share this one transaction
// body and therefore the same lifecycle-generation promotion semantics.
type attemptPromotionGuard struct {
	leaseUUID        string
	backendName      string
	operationID      operation.OperationID
	expectedRevision RecordRevision
	mutation         string
}

// Caller holds s.mu.
func (s *Store) promoteExactAttemptLocked(
	guard attemptPromotionGuard,
) (bool, error) {
	if err := validateTypedAttempt(
		guard.leaseUUID, guard.backendName, guard.operationID,
	); err != nil {
		return false, err
	}
	if !guard.expectedRevision.Valid() ||
		guard.expectedRevision.issuer != s.recordIssuer ||
		guard.expectedRevision.leaseUUID != guard.leaseUUID ||
		guard.mutation == "" {
		return false, ErrInvalidAttemptToken
	}
	p, exists := s.cache[guard.leaseUUID]
	if !exists || p.revision != guard.expectedRevision.value ||
		p.Attempt != guard.backendName || p.attemptOperationID != guard.operationID {
		return false, nil
	}
	if p.State() == StateUnusable {
		return false, fmt.Errorf("%w: lease %q", ErrUnusablePlacement, guard.leaseUUID)
	}
	if p.Backend != "" && p.Backend != guard.backendName {
		return false, fmt.Errorf("%w: lease %q is confirmed on %q, not %q",
			ErrBackendConflict, guard.leaseUUID, p.Backend, guard.backendName)
	}

	next, err := s.nextRevision()
	if err != nil {
		return false, err
	}
	p.Backend = guard.backendName
	p.Attempt = ""
	// Retain the exact operation metadata alongside the promoted lifecycle
	// generation. Callback recovery after restart needs the kind/source even
	// when inventory confirmed ownership before the callback arrived.
	p.revision = next
	capability := promoteAttemptLifecycle(guard.backendName, guard.operationID)
	if err := s.putPlacementWithLifecycleLocked(
		guard.leaseUUID, p, capability, guard.mutation,
	); err != nil {
		return false, err
	}
	s.revision = next
	return true, nil
}

// ConfirmAttempt promotes only the exact typed write-ahead attempt represented
// by token. A stale token returns false without writing; it can never confirm a
// later attempt even when that attempt targets the same backend.
func (s *Store) ConfirmAttempt(token AttemptToken) (bool, error) {
	if err := s.validateAttemptToken(token); err != nil {
		return false, err
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attemptClaimedLocked(token.leaseUUID) {
		return false, fmt.Errorf("%w: lease %q", ErrAttemptClaimed, token.leaseUUID)
	}
	return s.confirmAttemptLocked(token)
}

// Caller holds s.mu.
func (s *Store) confirmAttemptLocked(token AttemptToken) (bool, error) {
	return s.promoteExactAttemptLocked(attemptPromotionGuard{
		leaseUUID:        token.leaseUUID,
		backendName:      token.backendName,
		operationID:      token.operationID,
		expectedRevision: token.revision,
		mutation:         "confirm typed placement attempt",
	})
}

// RefuseAttempt clears only the exact typed write-ahead attempt represented by
// token after a definitive synchronous refusal. Ambiguous outcomes must not
// call this method. A stale token returns false without writing.
func (s *Store) RefuseAttempt(token AttemptToken) (bool, error) {
	if err := s.validateAttemptToken(token); err != nil {
		return false, err
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attemptClaimedLocked(token.leaseUUID) {
		return false, fmt.Errorf("%w: lease %q", ErrAttemptClaimed, token.leaseUUID)
	}
	return s.refuseAttemptLocked(token)
}

// Caller holds s.mu.
func (s *Store) refuseAttemptLocked(token AttemptToken) (bool, error) {
	p, matches := s.matchAttemptTokenLocked(token)
	if !matches {
		return false, nil
	}
	if p.State() == StateUnusable {
		return false, fmt.Errorf("%w: lease %q", ErrUnusablePlacement, token.leaseUUID)
	}

	if p.Backend == "" {
		if err := s.deleteLocked(token.leaseUUID, "refuse typed placement attempt"); err != nil {
			return false, err
		}
		return true, nil
	}

	next, err := s.nextRevision()
	if err != nil {
		return false, err
	}
	p.Attempt = ""
	clearOperationMetadata(&p)
	p.revision = next
	capability := clearAttemptLifecycle(
		s.lifecycleCache[token.leaseUUID], token.backendName, token.operationID,
	)
	if err := s.putPlacementWithLifecycleLocked(
		token.leaseUUID, p, capability, "refuse typed placement attempt",
	); err != nil {
		return false, err
	}
	s.revision = next
	return true, nil
}

// ConfirmOperation promotes a typed attempt identified by durable operation
// identity. It is the callback-safe counterpart to ConfirmAttempt: callbacks
// need not retain the process-local AttemptToken, but still cannot settle a
// legacy, mismatched, or newer same-backend operation. An already-confirmed
// record with no attempt is idempotent only when its current lifecycle
// generation exactly matches operationID; caller input can never rotate
// lifecycle authority.
func (s *Store) ConfirmOperation(
	leaseUUID, backendName string,
	operationID operation.OperationID,
) (bool, error) {
	if err := validateTypedAttempt(leaseUUID, backendName, operationID); err != nil {
		return false, err
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attemptClaimedLocked(leaseUUID) {
		return false, fmt.Errorf("%w: lease %q", ErrAttemptClaimed, leaseUUID)
	}
	p, exists := s.cache[leaseUUID]
	if !exists {
		return false, nil
	}
	if p.State() == StateUnusable {
		return false, fmt.Errorf("%w: lease %q", ErrUnusablePlacement, leaseUUID)
	}
	if p.Attempt == "" {
		if p.Backend != backendName {
			return false, nil
		}
		id, err := lifecycleIDForOperation(operationID)
		if err != nil {
			return false, err
		}
		capability, capabilityExists := s.lifecycleCache[leaseUUID]
		authorization := authorizeLifecycleCapability(capability, id)
		return capabilityExists && authorization.Authorized() &&
			authorization.Backend() == backendName, nil
	}
	if p.Attempt != backendName || p.attemptOperationID != operationID {
		return false, nil
	}
	if p.Backend != "" && p.Backend != backendName {
		return false, fmt.Errorf("%w: lease %q is confirmed on %q, not %q",
			ErrBackendConflict, leaseUUID, p.Backend, backendName)
	}

	return s.promoteExactAttemptLocked(attemptPromotionGuard{
		leaseUUID:        leaseUUID,
		backendName:      backendName,
		operationID:      operationID,
		expectedRevision: s.newRecordRevision(leaseUUID, p.revision),
		mutation:         "confirm placement operation",
	})
}

// RefuseOperation clears only the typed attempt whose persisted operation
// identity exactly matches. It never removes an already-confirmed owner.
func (s *Store) RefuseOperation(
	leaseUUID, backendName string,
	operationID operation.OperationID,
) (bool, error) {
	if err := validateTypedAttempt(leaseUUID, backendName, operationID); err != nil {
		return false, err
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attemptClaimedLocked(leaseUUID) {
		return false, fmt.Errorf("%w: lease %q", ErrAttemptClaimed, leaseUUID)
	}
	p, exists := s.cache[leaseUUID]
	if !exists || p.Attempt != backendName || p.attemptOperationID != operationID {
		return false, nil
	}
	if p.State() == StateUnusable {
		return false, fmt.Errorf("%w: lease %q", ErrUnusablePlacement, leaseUUID)
	}
	if p.Backend == "" {
		if err := s.deleteLocked(leaseUUID, "refuse placement operation"); err != nil {
			return false, err
		}
		return true, nil
	}

	next, err := s.nextRevision()
	if err != nil {
		return false, err
	}
	p.Attempt = ""
	clearOperationMetadata(&p)
	p.revision = next
	capability := clearAttemptLifecycle(
		s.lifecycleCache[leaseUUID], backendName, operationID,
	)
	if err := s.putPlacementWithLifecycleLocked(
		leaseUUID, p, capability, "refuse placement operation",
	); err != nil {
		return false, err
	}
	s.revision = next
	return true, nil
}

func (s *Store) validateAttemptToken(token AttemptToken) error {
	if !token.Valid() || token.issuer != s {
		return ErrInvalidAttemptToken
	}
	return nil
}

// matchAttemptTokenLocked checks every durable token component in one critical
// section. Caller holds s.mu.
func (s *Store) matchAttemptTokenLocked(token AttemptToken) (Placement, bool) {
	p, exists := s.cache[token.leaseUUID]
	if !exists || p.revision != token.revision.value ||
		p.Attempt != token.backendName || p.attemptOperationID != token.operationID ||
		p.attemptOperationKind != token.operationKind ||
		p.attemptRestoreSourceLeaseUUID != token.restoreSourceLeaseUUID ||
		p.attemptPayloadFingerprint != token.payloadFingerprint ||
		p.attemptRequestSnapshot != token.requestSnapshot ||
		p.attemptCallbackPair != token.callbackPair {
		return Placement{}, false
	}
	return p, true
}

// DeleteRecord removes only the exact store- and lease-bound placement record
// represented by revision. The target is derived from the capability itself;
// callers cannot transplant a numerically equal revision to another lease or
// store. Invalid and foreign revisions are rejected.
func (s *Store) DeleteRecord(revision RecordRevision) (bool, error) {
	if !revision.Valid() || revision.issuer != s.recordIssuer {
		return false, ErrInvalidRecordRevision
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return false, err
	}
	leaseUUID := revision.leaseUUID

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.restoreSourceClaimedLocked(leaseUUID) {
		return false, fmt.Errorf("%w: lease %q", ErrRestoreSourceClaimed, leaseUUID)
	}
	if s.attemptClaimedLocked(leaseUUID) {
		return false, fmt.Errorf("%w: lease %q", ErrAttemptClaimed, leaseUUID)
	}
	p, exists := s.cache[leaseUUID]
	if !exists || p.revision != revision.value {
		return false, nil
	}
	if err := s.deleteLocked(leaseUUID, "delete typed placement record"); err != nil {
		return false, err
	}
	return true, nil
}

type projectionMutation struct {
	placement Placement
	encoded   []byte
	revision  uint64
}

type projectionLifecycleMutation struct {
	capability lifecycleCapability
	encoded    []byte
	persist    bool
}

// ProjectInventory computes a placement projection against fence and persists
// every material write in one bbolt transaction. When Complete is true, that
// same transaction also establishes the durable admission baseline for the
// configured topology, including for empty or idempotent projections. No
// cache, baseline, or revision-clock change is visible unless the transaction
// commits. A partial projection never erases an existing matching baseline.
func (s *Store) ProjectInventory(
	fence InventoryFence,
	input InventoryProjection,
) (ProjectionResult, error) {
	if err := s.reattestRuntimeAuthority(); err != nil {
		return ProjectionResult{}, err
	}
	projection, err := normalizeInventoryProjection(input)
	if err != nil {
		return ProjectionResult{}, err
	}
	if !fence.Valid() || fence.issuer != s {
		return ProjectionResult{}, ErrInvalidInventoryFence
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if fence.epoch != s.authorityEpoch {
		return ProjectionResult{}, ErrInvalidInventoryFence
	}
	if err := s.validateProjectionBackendsLocked(projection); err != nil {
		return ProjectionResult{}, err
	}

	result := ProjectionResult{Fenced: make(map[string]struct{})}
	keySet := make(map[string]struct{},
		len(projection.Placements)+len(projection.Conflicts)+len(projection.UntrustedPositives))
	for leaseUUID := range projection.Placements {
		keySet[leaseUUID] = struct{}{}
	}
	for leaseUUID := range projection.Conflicts {
		keySet[leaseUUID] = struct{}{}
	}
	for leaseUUID := range projection.UntrustedPositives {
		keySet[leaseUUID] = struct{}{}
	}
	keys := slices.Sorted(maps.Keys(keySet))

	now := s.now().UTC()
	nextRevision := s.revision
	mutations := make(map[string]projectionMutation, len(keys))
	lifecycleMutations := make(map[string]projectionLifecycleMutation, len(keys))
	for _, leaseUUID := range keys {
		if s.restoreSourceClaimedLocked(leaseUUID) || s.attemptClaimedLocked(leaseUUID) {
			result.markFenced(leaseUUID)
			continue
		}
		if s.mutationRevisionLocked(leaseUUID) > fence.revision {
			result.markFenced(leaseUUID)
			continue
		}

		existing, exists := s.cache[leaseUUID]
		var candidate Placement
		acceptedPositive := false
		switch {
		case projection.Conflicts[leaseUUID] != nil:
			candidate = projectConflict(existing, exists, projection.Conflicts[leaseUUID], now)

		case projection.UntrustedPositives[leaseUUID] != nil:
			candidate = projectUntrustedPositive(
				existing, exists, projection.UntrustedPositives[leaseUUID], now,
			)

		case projection.Placements[leaseUUID] != "":
			backendName := projection.Placements[leaseUUID]
			if exists && existing.Conflict &&
				(!projection.Complete || !existing.CanResolveUntrustedPositive(backendName)) {
				// Inventory is not an operator conflict-resolution capability. Preserve
				// and enlarge an ordinary or multi-candidate quarantine rather than
				// allowing one later positive to erase historical evidence.
				candidate = projectConflict(existing, exists, []string{backendName}, now)
			} else {
				candidate = projectPositivePlacement(existing, exists, backendName, now)
				acceptedPositive = true
			}
		}

		var lifecycleMutation projectionLifecycleMutation
		lifecycleWrite := false
		if backendName := projection.Placements[leaseUUID]; acceptedPositive {
			currentCapability, capabilityExists := s.lifecycleCache[leaseUUID]
			observation, observationPresent := projection.Lifecycles[leaseUUID]
			capability, persist, settlesAttempt := projectPositiveLifecycle(
				currentCapability, capabilityExists, existing, exists, backendName,
				observation, observationPresent,
			)
			if exists && existing.Attempt == backendName && !settlesAttempt {
				// Positive inventory may record the owner, but it cannot discard the
				// only exact operation identity when lifecycle evidence is missing or
				// corrupt. A later authenticated callback must remain able to claim and
				// repair that generation while settling the chain.
				candidate.Attempt = existing.Attempt
				candidate.attemptOperationID = existing.attemptOperationID
				candidate.attemptOperationKind = existing.attemptOperationKind
				candidate.attemptRestoreSourceLeaseUUID = existing.attemptRestoreSourceLeaseUUID
				candidate.attemptPayloadFingerprint = existing.attemptPayloadFingerprint
				candidate.attemptRequestSnapshot = existing.attemptRequestSnapshot
				candidate.attemptCallbackPair = existing.attemptCallbackPair
			}
			lifecycleWrite = persist &&
				(!capabilityExists || capability != currentCapability ||
					capability.needsPersistence)
			if lifecycleWrite {
				capability.needsPersistence = false
			}
			var capabilityEncoded []byte
			if lifecycleWrite {
				var capabilityErr error
				capabilityEncoded, capabilityErr = encodeLifecycleCapability(capability)
				if capabilityErr != nil {
					return ProjectionResult{}, mutationFailure(
						"encode inventory lifecycle projection", capabilityErr,
					)
				}
			}
			lifecycleMutation = projectionLifecycleMutation{
				capability: capability,
				encoded:    capabilityEncoded,
				persist:    lifecycleWrite,
			}
		} else if backendName != "" {
			// A placement conflict prevents this observation from changing
			// lifecycle authority. It may still durably seal a decodable quarantine
			// discovered while loading the two authoritative rows, so the invalid
			// cross-row binding remains explicit rather than being rediscovered only
			// in process memory after every restart.
			capability, capabilityExists := s.lifecycleCache[leaseUUID]
			if capabilityExists && capability.unusable && capability.needsPersistence {
				capability.needsPersistence = false
				capabilityEncoded, capabilityErr := encodeLifecycleCapability(capability)
				if capabilityErr != nil {
					return ProjectionResult{}, mutationFailure(
						"encode quarantined inventory lifecycle", capabilityErr,
					)
				}
				lifecycleWrite = true
				lifecycleMutation = projectionLifecycleMutation{
					capability: capability,
					encoded:    capabilityEncoded,
					persist:    true,
				}
			}
		}
		mutate := !exists || existing.revision == 0 ||
			!equalPlacementIgnoringRevision(candidate, existing) || lifecycleWrite
		if !mutate {
			continue
		}
		if nextRevision == math.MaxUint64 {
			return ProjectionResult{}, fmt.Errorf("placement revision exhausted")
		}
		nextRevision++
		candidate.revision = nextRevision
		if lifecycleWrite {
			lifecycleMutations[leaseUUID] = lifecycleMutation
		}
		encoded, err := encodePlacement(candidate)
		if err != nil {
			return ProjectionResult{}, mutationFailure("encode inventory projection", err)
		}
		mutations[leaseUUID] = projectionMutation{
			placement: candidate,
			encoded:   encoded,
			revision:  nextRevision,
		}
	}

	if len(mutations) == 0 && !projection.Complete {
		if err := s.verifyBucket(); err != nil {
			return ProjectionResult{}, mutationFailure("verify inventory projection", err)
		}
		return result, nil
	}

	mutationKeys := slices.Sorted(maps.Keys(mutations))
	nextMetadata := s.topologyMetadataLocked()
	if projection.Complete {
		if nextMetadata.KnownBackendStorageIDs == nil {
			nextMetadata.KnownBackendStorageIDs = make(map[string]string, len(projection.BackendStorageIdentities))
		}
		for backendName, id := range projection.BackendStorageIdentities {
			nextMetadata.KnownBackendStorageIDs[backendName] = id.String()
		}
		nextMetadata.BaselineFingerprint = s.topologyFingerprint
		nextMetadata.BaselineTopologyID = s.topologyID
		nextMetadata.InventoryTopologyID = s.topologyID
		nextMetadata.EmptyInventoryBackends = slices.Clone(projection.EmptyBackends)
	}
	if err := s.updateRuntimeAuthority(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		capabilities := tx.Bucket(lifecycleCapabilityBucketName)
		if b == nil || capabilities == nil {
			return errors.New("placement lifecycle buckets missing")
		}
		for _, leaseUUID := range mutationKeys {
			mutation := mutations[leaseUUID]
			if err := b.Put([]byte(leaseUUID), mutation.encoded); err != nil {
				return err
			}
			if lifecycleMutation, ok := lifecycleMutations[leaseUUID]; ok &&
				lifecycleMutation.persist {
				if err := capabilities.Put(
					[]byte(leaseUUID), lifecycleMutation.encoded,
				); err != nil {
					return err
				}
			}
		}
		if projection.Complete {
			if err := putTopologyMetadata(tx, nextMetadata); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return ProjectionResult{}, mutationFailure("project placement inventory", err)
	}

	for _, leaseUUID := range mutationKeys {
		mutation := mutations[leaseUUID]
		s.cache[leaseUUID] = mutation.placement
		if lifecycleMutation, ok := lifecycleMutations[leaseUUID]; ok {
			s.lifecycleCache[leaseUUID] = lifecycleMutation.capability
		}
		delete(s.deleteRevisions, leaseUUID)
	}
	s.revision = nextRevision
	if projection.Complete {
		for backendName, id := range projection.BackendStorageIdentities {
			s.backendStorageIDs[backendName] = id
		}
		s.baselineFingerprint = nextMetadata.BaselineFingerprint
		s.baselineTopologyID = nextMetadata.BaselineTopologyID
		s.inventoryTopologyID = nextMetadata.InventoryTopologyID
		s.emptyInventoryBackends = make(map[string]struct{}, len(nextMetadata.EmptyInventoryBackends))
		for _, backendName := range nextMetadata.EmptyInventoryBackends {
			s.emptyInventoryBackends[backendName] = struct{}{}
		}
	}
	return result, nil
}

func (result *ProjectionResult) markFenced(leaseUUID string) {
	result.Fenced[leaseUUID] = struct{}{}
}

func normalizeInventoryProjection(input InventoryProjection) (InventoryProjection, error) {
	projection := InventoryProjection{
		Complete:                 input.Complete,
		BackendStorageIdentities: maps.Clone(input.BackendStorageIdentities),
		EmptyBackends:            slices.Clone(input.EmptyBackends),
		Placements:               maps.Clone(input.Placements),
		Lifecycles:               maps.Clone(input.Lifecycles),
		Conflicts:                make(map[string][]string, len(input.Conflicts)),
		UntrustedPositives:       make(map[string][]string, len(input.UntrustedPositives)),
	}
	if projection.Complete {
		if input.EmptyBackends == nil {
			return InventoryProjection{}, fmt.Errorf(
				"%w: complete projection has no raw empty-backend evidence",
				ErrInvalidPlacement,
			)
		}
		canonicalEmpty, err := canonicalOptionalBackendNames(projection.EmptyBackends)
		if err != nil {
			return InventoryProjection{}, err
		}
		projection.EmptyBackends = canonicalEmpty
	} else if input.EmptyBackends != nil {
		return InventoryProjection{}, fmt.Errorf(
			"%w: partial projection carries empty-backend evidence",
			ErrInvalidPlacement,
		)
	}
	for backendName, id := range projection.BackendStorageIdentities {
		if backendName == "" || !id.Valid() {
			return InventoryProjection{}, fmt.Errorf(
				"%w: backend %q", ErrBackendStorageIdentityUnbound, backendName,
			)
		}
	}
	for leaseUUID, backendName := range projection.Placements {
		if err := validateIDs(leaseUUID, backendName); err != nil {
			return InventoryProjection{}, err
		}
	}
	for leaseUUID, observation := range projection.Lifecycles {
		if _, active := projection.Placements[leaseUUID]; !active {
			return InventoryProjection{}, fmt.Errorf(
				"%w: lifecycle observation for lease %q has no positive placement",
				ErrInvalidPlacement, leaseUUID,
			)
		}
		if err := validateLifecycleObservation(observation); err != nil {
			return InventoryProjection{}, fmt.Errorf(
				"%w: lifecycle observation for lease %q: %w",
				ErrInvalidPlacement, leaseUUID, err,
			)
		}
	}
	for leaseUUID, backendNames := range input.Conflicts {
		if leaseUUID == "" {
			return InventoryProjection{}, fmt.Errorf("%w: lease UUID is required", ErrInvalidPlacement)
		}
		normalized := normalizeBackendNames(backendNames)
		if len(normalized) < 2 {
			return InventoryProjection{}, fmt.Errorf(
				"%w: conflict for lease %q requires at least two backends",
				ErrInvalidPlacement, leaseUUID,
			)
		}
		projection.Conflicts[leaseUUID] = normalized
	}
	for leaseUUID, backendNames := range input.UntrustedPositives {
		if leaseUUID == "" {
			return InventoryProjection{}, fmt.Errorf("%w: lease UUID is required", ErrInvalidPlacement)
		}
		normalized := normalizeBackendNames(backendNames)
		if len(normalized) == 0 {
			return InventoryProjection{}, fmt.Errorf(
				"%w: untrusted positive for lease %q requires at least one backend",
				ErrInvalidPlacement, leaseUUID,
			)
		}
		projection.UntrustedPositives[leaseUUID] = normalized
	}
	for leaseUUID := range projection.Conflicts {
		if _, overlaps := projection.Placements[leaseUUID]; overlaps {
			return InventoryProjection{}, projectionOverlapError(leaseUUID)
		}
		if _, overlaps := projection.UntrustedPositives[leaseUUID]; overlaps {
			return InventoryProjection{}, projectionOverlapError(leaseUUID)
		}
	}
	for leaseUUID := range projection.UntrustedPositives {
		if _, overlaps := projection.Placements[leaseUUID]; overlaps {
			return InventoryProjection{}, projectionOverlapError(leaseUUID)
		}
	}
	if projection.Complete {
		empty := make(map[string]struct{}, len(projection.EmptyBackends))
		for _, backendName := range projection.EmptyBackends {
			empty[backendName] = struct{}{}
		}
		for leaseUUID, backendName := range projection.Placements {
			if _, contradiction := empty[backendName]; contradiction {
				return InventoryProjection{}, fmt.Errorf(
					"%w: backend %q is both empty and owner of lease %q",
					ErrInvalidPlacement,
					backendName,
					leaseUUID,
				)
			}
		}
		for leaseUUID, backendNames := range projection.Conflicts {
			for _, backendName := range backendNames {
				if _, contradiction := empty[backendName]; contradiction {
					return InventoryProjection{}, fmt.Errorf(
						"%w: backend %q is both empty and a conflict reporter for lease %q",
						ErrInvalidPlacement,
						backendName,
						leaseUUID,
					)
				}
			}
		}
		for leaseUUID, backendNames := range projection.UntrustedPositives {
			for _, backendName := range backendNames {
				if _, contradiction := empty[backendName]; contradiction {
					return InventoryProjection{}, fmt.Errorf(
						"%w: backend %q is both empty and an untrusted positive reporter for lease %q",
						ErrInvalidPlacement,
						backendName,
						leaseUUID,
					)
				}
			}
		}
	}
	return projection, nil
}

func canonicalOptionalBackendNames(names []string) ([]string, error) {
	canonical := slices.Clone(names)
	seen := make(map[string]struct{}, len(canonical))
	for _, name := range canonical {
		if strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("%w: empty-backend name is blank", ErrInvalidPlacement)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf(
				"%w: duplicate empty-backend name %q",
				ErrInvalidPlacement,
				name,
			)
		}
		seen[name] = struct{}{}
	}
	slices.Sort(canonical)
	return canonical, nil
}

func validateLifecycleObservation(observation LifecycleObservation) error {
	switch observation.Kind {
	case LifecycleObservationUnknown, LifecycleObservationLegacy,
		LifecycleObservationUnusable:
		if observation.ID.Valid() {
			return errors.New("non-typed lifecycle observation carries an ID")
		}
		return nil
	case LifecycleObservationTyped:
		if !observation.ID.Valid() {
			return errors.New("typed lifecycle observation has no valid ID")
		}
		return nil
	default:
		return fmt.Errorf("unknown lifecycle observation kind %d", observation.Kind)
	}
}

func projectionOverlapError(leaseUUID string) error {
	return fmt.Errorf("%w: projection contains contradictory outcomes for lease %q",
		ErrInvalidPlacement, leaseUUID)
}

func projectConflict(
	existing Placement,
	exists bool,
	backendNames []string,
	now time.Time,
) Placement {
	setAt := existing.SetAt
	if setAt.IsZero() {
		setAt = now
	}
	candidateSet := make(map[string]struct{}, len(backendNames)+len(existing.ConflictBackends)+2)
	for _, backendName := range backendNames {
		candidateSet[backendName] = struct{}{}
	}
	for _, backendName := range existing.ConflictBackends {
		candidateSet[backendName] = struct{}{}
	}
	if existing.Backend != "" {
		candidateSet[existing.Backend] = struct{}{}
	}
	if existing.Attempt != "" {
		candidateSet[existing.Attempt] = struct{}{}
	}
	unknownOwners := existing.ConflictOwnersUnknown ||
		(existing.Conflict && len(existing.ConflictBackends) < 2 && !existing.untrustedPositive) ||
		(exists && existing.State() == StateUnusable && !existing.Conflict)
	return Placement{
		Backend:                       existing.Backend,
		Attempt:                       existing.Attempt,
		SetAt:                         setAt,
		Conflict:                      true,
		ConflictBackends:              slices.Sorted(maps.Keys(candidateSet)),
		ConflictOwnersUnknown:         unknownOwners,
		untrustedPositive:             false,
		attemptOperationID:            existing.attemptOperationID,
		attemptOperationKind:          existing.attemptOperationKind,
		attemptRestoreSourceLeaseUUID: existing.attemptRestoreSourceLeaseUUID,
		attemptPayloadFingerprint:     existing.attemptPayloadFingerprint,
		attemptRequestSnapshot:        existing.attemptRequestSnapshot,
		attemptCallbackPair:           existing.attemptCallbackPair,
	}
}

func projectUntrustedPositive(
	existing Placement,
	exists bool,
	backendNames []string,
	now time.Time,
) Placement {
	candidate := projectConflict(existing, exists, backendNames, now)
	// The candidate names are complete for this rejected observation, unlike a
	// legacy conflict. A later complete, identity-valid inventory may therefore
	// resolve a sole matching reporter, while multiple candidates and inherited
	// unknown-owner records remain operator-only quarantines.
	candidate.untrustedPositive = true
	return candidate
}

func projectPositivePlacement(
	existing Placement,
	exists bool,
	backendName string,
	now time.Time,
) Placement {
	p := existing
	if !exists || p.unusable {
		p = Placement{SetAt: now}
	}
	p.Backend = backendName
	if p.Attempt == backendName {
		p.Attempt = ""
		// The attempt was promoted by exact paired-generation evidence. Preserve
		// its complete metadata for callback recovery after process restart.
	}
	p.Conflict = false
	p.ConflictBackends = nil
	p.ConflictOwnersUnknown = false
	p.untrustedPositive = false
	p.unusable = false
	return p
}

// Healthy checks that every bbolt bucket carrying placement authority is
// accessible and that the durable topology/storage-identity metadata remains
// structurally valid. Admission depends on that metadata just as directly as
// it depends on the placement and lifecycle rows, so silently ignoring a
// missing or malformed metadata record would report a dangerously incomplete
// store as healthy.
func (s *Store) Healthy() error {
	return s.viewRuntimeAuthority(verifyAuthorityBuckets)
}

// Close closes the bbolt database and retained inode identity. It is safe to
// call multiple times.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.runtimeAuthorityMu.Lock()
		s.runtimeAuthorityClosed = true
		identityFile := s.runtimeAuthorityFile
		s.runtimeAuthorityMu.Unlock()
		dbErr := s.db.Close()
		var identityErr error
		if identityFile != nil {
			identityErr = identityFile.Close()
		}
		s.closeErr = errors.Join(dbErr, identityErr)
	})
	return s.closeErr
}

func validateIDs(leaseUUID, backendName string) error {
	if leaseUUID == "" {
		return fmt.Errorf("%w: lease UUID is required", ErrInvalidPlacement)
	}
	if backendName == "" {
		return fmt.Errorf("%w: backend name is required", ErrInvalidPlacement)
	}
	return nil
}

func (s *Store) nextRevision() (uint64, error) {
	if s.revision == math.MaxUint64 {
		return 0, fmt.Errorf("placement revision exhausted")
	}
	return s.revision + 1, nil
}

// mutationRevisionLocked returns the per-record revision for a present key or
// the exact key's deletion revision while an older registered inventory remains
// active. Unrelated deletions never fence this lease.
// Caller holds at least s.mu.RLock.
func (s *Store) mutationRevisionLocked(leaseUUID string) uint64 {
	if p, exists := s.cache[leaseUUID]; exists {
		return p.revision
	}
	return s.deleteRevisions[leaseUUID]
}

// verifyBucket proves the durable store is readable even when an idempotent or
// fully fenced synchronization has no mutation to commit.
// Caller holds at least s.mu.RLock.
func (s *Store) verifyBucket() error {
	return s.viewRuntimeAuthority(verifyAuthorityBuckets)
}

func verifyAuthorityBuckets(tx *bolt.Tx) error {
	if tx.Bucket(bucketName) == nil {
		return errors.New("placements bucket missing")
	}
	if tx.Bucket(lifecycleCapabilityBucketName) == nil {
		return errors.New("placement lifecycle capability bucket missing")
	}
	if _, err := loadTopologyMetadata(tx); err != nil {
		return fmt.Errorf("placement topology metadata: %w", err)
	}
	return nil
}

// deleteLocked durably removes one key and retains its deletion revision only
// while a registered inventory snapshot could otherwise recreate it.
// Caller holds s.mu.
func (s *Store) deleteLocked(leaseUUID, operation string) error {
	revision, err := s.nextRevision()
	if err != nil {
		return err
	}
	if err := s.deleteDurable(leaseUUID, operation); err != nil {
		return err
	}
	delete(s.cache, leaseUUID)
	if len(s.activeSnapshots) > 0 {
		s.deleteRevisions[leaseUUID] = revision
	}
	s.revision = revision
	return nil
}

// pruneDeleteRevisionsLocked drops tombstones that are no newer than every
// active inventory cutoff. Such inventories already observed the key absent.
// Caller holds s.mu.
func (s *Store) pruneDeleteRevisionsLocked() {
	if len(s.activeSnapshots) == 0 {
		clear(s.deleteRevisions)
		return
	}
	oldest := uint64(math.MaxUint64)
	for revision := range s.activeSnapshots {
		if revision < oldest {
			oldest = revision
		}
	}
	for leaseUUID, revision := range s.deleteRevisions {
		if revision <= oldest {
			delete(s.deleteRevisions, leaseUUID)
		}
	}
}

// deleteDurable removes placement while retaining its current lifecycle
// capability for delayed teardown callbacks. Any unresolved attempt marker is
// cleared in the same transaction. Caller holds s.mu and performs the placement
// cache delete after this succeeds.
func (s *Store) deleteDurable(leaseUUID, operation string) error {
	capability, capabilityExists := s.lifecycleCache[leaseUUID]
	capability, retainCapability := lifecycleAfterPlacementDelete(
		capability, s.cache[leaseUUID],
	)
	var capabilityEncoded []byte
	var err error
	if capabilityExists && retainCapability {
		capabilityEncoded, err = encodeLifecycleCapability(capability)
		if err != nil {
			return mutationFailure("encode lifecycle capability for "+operation, err)
		}
	}
	if err := s.updateRuntimeAuthority(func(tx *bolt.Tx) error {
		placements := tx.Bucket(bucketName)
		capabilities := tx.Bucket(lifecycleCapabilityBucketName)
		if placements == nil || capabilities == nil {
			return errors.New("placement lifecycle buckets missing")
		}
		if err := placements.Delete([]byte(leaseUUID)); err != nil {
			return err
		}
		if capabilityExists && retainCapability {
			return capabilities.Put([]byte(leaseUUID), capabilityEncoded)
		}
		return capabilities.Delete([]byte(leaseUUID))
	}); err != nil {
		return mutationFailure(operation, err)
	}
	if capabilityExists && retainCapability {
		s.lifecycleCache[leaseUUID] = capability
	} else {
		delete(s.lifecycleCache, leaseUUID)
	}
	return nil
}

func mutationFailure(operation string, err error) error {
	metrics.PlacementWriteFailuresTotal.Inc()
	return fmt.Errorf("failed to %s: %w", operation, err)
}
