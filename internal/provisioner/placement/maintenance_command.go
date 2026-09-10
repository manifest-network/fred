package placement

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/backendname"
	"github.com/manifest-network/fred/internal/callbackurl"
	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/strictjson"
)

var (
	maintenanceCommandBucketName = []byte("maintenance_commands")
	maintenancePendingBucketName = []byte("pending")
	maintenanceRecordsBucketName = []byte("commands")

	ErrInvalidMaintenanceCommand    = errors.New("invalid maintenance command")
	ErrMaintenanceCommandConflict   = errors.New("maintenance command conflicts with durable history")
	ErrMaintenanceCommandNotPending = errors.New("maintenance command is not pending")
	ErrMaintenanceJournalCorrupt    = errors.New("maintenance command journal is corrupt")
	ErrMaintenanceHistoryFull       = errors.New("maintenance command history is full")
)

const maintenanceCommandSchema = 1

const (
	// The API's entire encoded request is capped at 1 MiB. Apply the same hard
	// bound to direct callers so the placement DB is never an unbounded payload
	// storage surface.
	maxMaintenancePayloadBytes      = 1 << 20
	maxMaintenanceCommandValueBytes = 2 << 20
	// JSON escapes one input byte to at most six output bytes. Reserve the
	// complete refusal detail and fixed terminal metadata before admission.
	maxMaintenanceRefusalDetailBytes = 4 << 10
	// Retain the old pending limit as the baseline for fresh admission. Phase
	// transitions use the full record limit so legacy rows remain recoverable.
	maxMaintenanceCommandPendingBytes   = maxMaintenanceCommandValueBytes - 512
	maxMaintenanceCommandAdmissionBytes = maxMaintenanceCommandPendingBytes - 6*maxMaintenanceRefusalDetailBytes
	maxMaintenanceCommandsPerLease      = 1024
)

// MaintenanceCommandKind is the closed set of tenant maintenance effects.
// The invalid zero value cannot be persisted or dispatched.
type MaintenanceCommandKind uint8

const (
	MaintenanceCommandInvalid MaintenanceCommandKind = iota
	MaintenanceCommandRestart
	MaintenanceCommandUpdate
)

func (kind MaintenanceCommandKind) String() string {
	switch kind {
	case MaintenanceCommandRestart:
		return "restart"
	case MaintenanceCommandUpdate:
		return "update"
	default:
		return "invalid"
	}
}

func parseMaintenanceCommandKind(value string) MaintenanceCommandKind {
	switch value {
	case "restart":
		return MaintenanceCommandRestart
	case "update":
		return MaintenanceCommandUpdate
	default:
		return MaintenanceCommandInvalid
	}
}

// MaintenanceCommandOutcome is a durable terminal receipt. Pending is
// represented by the explicit zero value; all nonzero outcomes are immutable.
type MaintenanceCommandOutcome uint8

const (
	MaintenanceOutcomePending MaintenanceCommandOutcome = iota
	MaintenanceOutcomeAccepted
	MaintenanceOutcomeNotProvisioned
	MaintenanceOutcomeInvalidState
	MaintenanceOutcomeValidationRejected
	MaintenanceOutcomeLeaseEnded
	MaintenanceOutcomeAuthorityRevoked
	MaintenanceOutcomeCapacityRefused
	MaintenanceOutcomeBackendUnavailable
)

func (outcome MaintenanceCommandOutcome) String() string {
	switch outcome {
	case MaintenanceOutcomePending:
		return "pending"
	case MaintenanceOutcomeAccepted:
		return "accepted"
	case MaintenanceOutcomeNotProvisioned:
		return "not_provisioned"
	case MaintenanceOutcomeInvalidState:
		return "invalid_state"
	case MaintenanceOutcomeValidationRejected:
		return "validation_rejected"
	case MaintenanceOutcomeLeaseEnded:
		return "lease_ended"
	case MaintenanceOutcomeAuthorityRevoked:
		return "authority_revoked"
	case MaintenanceOutcomeCapacityRefused:
		return "capacity_refused"
	case MaintenanceOutcomeBackendUnavailable:
		return "backend_unavailable"
	default:
		return "invalid"
	}
}

func parseMaintenanceCommandOutcome(value string) (MaintenanceCommandOutcome, bool) {
	switch value {
	case "pending":
		return MaintenanceOutcomePending, true
	case "accepted":
		return MaintenanceOutcomeAccepted, true
	case "not_provisioned":
		return MaintenanceOutcomeNotProvisioned, true
	case "invalid_state":
		return MaintenanceOutcomeInvalidState, true
	case "validation_rejected":
		return MaintenanceOutcomeValidationRejected, true
	case "lease_ended":
		return MaintenanceOutcomeLeaseEnded, true
	case "authority_revoked":
		return MaintenanceOutcomeAuthorityRevoked, true
	case "capacity_refused":
		return MaintenanceOutcomeCapacityRefused, true
	case "backend_unavailable":
		return MaintenanceOutcomeBackendUnavailable, true
	default:
		return MaintenanceOutcomePending, false
	}
}

// MaintenanceCommand is the immutable, exact request written before dispatch.
// Private fields make partial commands unconstructable outside this package.
type MaintenanceCommand struct {
	id                maintenanceid.ID
	leaseUUID         string
	tenant            string
	providerUUID      string
	placementRevision uint64
	kind              MaintenanceCommandKind
	payload           []byte
	payloadHash       [sha256.Size]byte
	backendName       string
	backendStorageID  backendidentity.ID
	lifecycleLegacy   bool
	lifecycleID       lifecycle.ID
	callbackURL       string
	terminal          bool
	phase             maintenanceJournalPhase
}

// newMaintenanceCommand validates and detaches every fact needed to safely
// repeat a restart or update after a provider crash. It is intentionally
// package-private: production commands are minted only by Store from current
// durable authority and its construction-bound callback route factory.
func newMaintenanceCommand(
	id maintenanceid.ID,
	leaseUUID string,
	principal runtimePrincipal,
	revision RecordRevision,
	kind MaintenanceCommandKind,
	payload []byte,
	backendName string,
	backendStorageID backendidentity.ID,
	route lifecycle.Route,
) (MaintenanceCommand, error) {
	if !revision.Valid() || revision.leaseUUID != leaseUUID {
		return MaintenanceCommand{}, fmt.Errorf(
			"%w: exact placement revision is required",
			ErrInvalidMaintenanceCommand,
		)
	}
	if !route.Valid() {
		return MaintenanceCommand{}, fmt.Errorf(
			"%w: lifecycle route is required",
			ErrInvalidMaintenanceCommand,
		)
	}
	command := MaintenanceCommand{
		phase: maintenanceDeliveryOutstanding,
		id:    id, leaseUUID: leaseUUID,
		tenant: principal.tenant, providerUUID: principal.providerUUID,
		placementRevision: revision.value,
		kind:              kind, payload: append([]byte(nil), payload...), backendName: backendName,
		backendStorageID: backendStorageID, lifecycleLegacy: route.IsLegacy(),
		lifecycleID: route.ID(), callbackURL: route.URL(),
	}
	command.payloadHash = sha256.Sum256(command.payload)
	if err := validateMaintenanceCommand(command); err != nil {
		return MaintenanceCommand{}, err
	}
	return command, nil
}

// PreparedMaintenanceCommand is the only capability accepted by Begin. It is
// minted from a live Store aggregate and binds the immutable command data to
// the exact issuing store and placement revision. Decoded records and terminal
// observations are MaintenanceCommand values, so they cannot accidentally be
// presented for admission or local reauthorization.
type PreparedMaintenanceCommand struct {
	issuer   *Store
	revision RecordRevision
	command  MaintenanceCommand
}

func (prepared PreparedMaintenanceCommand) Valid() bool {
	return prepared.issuer != nil && prepared.revision.Valid() &&
		prepared.command.Dispatchable() &&
		prepared.revision.issuer == prepared.issuer.recordIssuer &&
		prepared.revision.leaseUUID == prepared.command.leaseUUID &&
		prepared.revision.value == prepared.command.placementRevision
}

// Command exposes immutable request data but not the capability needed to
// reconstruct another PreparedMaintenanceCommand.
func (prepared PreparedMaintenanceCommand) Command() MaintenanceCommand {
	if !prepared.Valid() {
		return MaintenanceCommand{}
	}
	return prepared.command
}

func validateMaintenanceCommand(command MaintenanceCommand) error {
	if !command.phase.validFor(command.kind, command.terminal) {
		return fmt.Errorf("%w: invalid maintenance journal phase", ErrInvalidMaintenanceCommand)
	}
	if !command.id.Valid() {
		return fmt.Errorf("%w: request ID is required", ErrInvalidMaintenanceCommand)
	}
	if !canonicalLeaseUUID(command.leaseUUID) {
		return fmt.Errorf("%w: lease UUID is not canonical", ErrInvalidMaintenanceCommand)
	}
	if !canonicalLeaseUUID(command.providerUUID) {
		return fmt.Errorf("%w: provider UUID is not canonical", ErrInvalidMaintenanceCommand)
	}
	if command.placementRevision == 0 {
		return fmt.Errorf("%w: placement revision is required", ErrInvalidMaintenanceCommand)
	}
	if command.tenant == "" || !utf8.ValidString(command.tenant) {
		return fmt.Errorf("%w: tenant is required and must be UTF-8", ErrInvalidMaintenanceCommand)
	}
	if err := backendname.Validate(command.backendName); err != nil {
		return fmt.Errorf("%w: backend name: %w", ErrInvalidMaintenanceCommand, err)
	}
	if !command.backendStorageID.Valid() {
		return fmt.Errorf("%w: backend storage identity is required", ErrInvalidMaintenanceCommand)
	}
	if command.lifecycleLegacy == command.lifecycleID.Valid() {
		return fmt.Errorf("%w: lifecycle generation is invalid", ErrInvalidMaintenanceCommand)
	}
	switch command.kind {
	case MaintenanceCommandRestart:
		if len(command.payload) != 0 {
			return fmt.Errorf("%w: restart cannot carry a payload", ErrInvalidMaintenanceCommand)
		}
	case MaintenanceCommandUpdate:
		if len(command.payload) == 0 && !command.terminal {
			return fmt.Errorf("%w: update payload is required", ErrInvalidMaintenanceCommand)
		}
		if len(command.payload) != 0 && command.terminal {
			return fmt.Errorf("%w: terminal update receipt retains a payload", ErrInvalidMaintenanceCommand)
		}
	default:
		return fmt.Errorf("%w: command kind is required", ErrInvalidMaintenanceCommand)
	}
	if len(command.payload) > maxMaintenancePayloadBytes {
		return fmt.Errorf(
			"%w: payload exceeds %d bytes",
			ErrInvalidMaintenanceCommand,
			maxMaintenancePayloadBytes,
		)
	}
	if !command.terminal && command.payloadHash != sha256.Sum256(command.payload) {
		return fmt.Errorf("%w: payload hash mismatch", ErrInvalidMaintenanceCommand)
	}
	if err := validateMaintenanceCallbackURL(
		command.callbackURL, command.lifecycleLegacy, command.lifecycleID,
	); err != nil {
		return fmt.Errorf("%w: callback URL: %w", ErrInvalidMaintenanceCommand, err)
	}
	return nil
}

func validateMaintenanceCallbackURL(raw string, legacy bool, want lifecycle.ID) error {
	endpoint, err := callbackurl.ParseEndpoint(raw)
	if err != nil {
		return err
	}
	values, err := url.ParseQuery(endpoint.RawQuery())
	if err != nil {
		return err
	}
	got, present, err := lifecycle.ParseQuery(values)
	if err != nil {
		return errors.New("has invalid lifecycle identity")
	}
	if legacy {
		if present {
			return errors.New("legacy route must be tokenless")
		}
	} else if !present || got != want {
		return errors.New("does not carry the exact lifecycle identity")
	}
	_, operationPresent, err := operation.ParseQuery(values)
	if err != nil || operationPresent {
		return errors.New("must not carry operation authority")
	}
	return nil
}

func (command MaintenanceCommand) Valid() bool {
	return validateMaintenanceCommand(command) == nil
}
func (command MaintenanceCommand) Dispatchable() bool {
	return command.Valid() && command.phase == maintenanceDeliveryOutstanding
}
func (command MaintenanceCommand) ID() maintenanceid.ID         { return command.id }
func (command MaintenanceCommand) LeaseUUID() string            { return command.leaseUUID }
func (command MaintenanceCommand) Tenant() string               { return command.tenant }
func (command MaintenanceCommand) ProviderUUID() string         { return command.providerUUID }
func (command MaintenanceCommand) PlacementRevision() uint64    { return command.placementRevision }
func (command MaintenanceCommand) Kind() MaintenanceCommandKind { return command.kind }
func (command MaintenanceCommand) Payload() []byte              { return append([]byte(nil), command.payload...) }
func (command MaintenanceCommand) PayloadHash() string {
	if !command.Valid() {
		return ""
	}
	return hex.EncodeToString(command.payloadHash[:])
}
func (command MaintenanceCommand) BackendName() string { return command.backendName }
func (command MaintenanceCommand) BackendStorageID() backendidentity.ID {
	return command.backendStorageID
}
func (command MaintenanceCommand) LifecycleID() lifecycle.ID { return command.lifecycleID }
func (command MaintenanceCommand) CallbackURL() string       { return command.callbackURL }

func (command MaintenanceCommand) equal(other MaintenanceCommand) bool {
	return command.id == other.id && command.leaseUUID == other.leaseUUID &&
		command.tenant == other.tenant && command.providerUUID == other.providerUUID &&
		command.placementRevision == other.placementRevision &&
		command.kind == other.kind && command.payloadHash == other.payloadHash &&
		(command.terminal || other.terminal || bytes.Equal(command.payload, other.payload)) &&
		command.backendName == other.backendName &&
		command.backendStorageID == other.backendStorageID &&
		command.lifecycleLegacy == other.lifecycleLegacy &&
		command.lifecycleID == other.lifecycleID && command.callbackURL == other.callbackURL
}

// MaintenanceCommandClaim is the only capability that can settle a pending
// command. It binds settlement to the issuing store and exact immutable value.
type MaintenanceCommandClaim struct {
	issuer  *Store
	command MaintenanceCommand
}

func (claim MaintenanceCommandClaim) Valid() bool {
	return claim.issuer != nil && claim.command.Valid() && !claim.command.terminal
}
func (claim MaintenanceCommandClaim) Command() MaintenanceCommand {
	if !claim.Valid() {
		return MaintenanceCommand{}
	}
	return claim.command
}

// MaintenanceCommandAdmission exhaustively describes begin/replay. Pending
// carries a settlement claim; terminal outcomes never do.
type MaintenanceCommandAdmission struct {
	claim   MaintenanceCommandClaim
	command MaintenanceCommand
	outcome MaintenanceCommandOutcome
	detail  string
}

func (result MaintenanceCommandAdmission) Pending() bool {
	return result.outcome == MaintenanceOutcomePending && result.claim.Valid()
}
func (result MaintenanceCommandAdmission) Claim() MaintenanceCommandClaim {
	if !result.Pending() {
		return MaintenanceCommandClaim{}
	}
	return result.claim
}
func (result MaintenanceCommandAdmission) Outcome() MaintenanceCommandOutcome { return result.outcome }
func (result MaintenanceCommandAdmission) Detail() string                     { return result.detail }

// MaintenanceCommandRecord is a read-only journal observation. It grants no
// settlement authority; only Begin/Pending can issue a claim.
type MaintenanceCommandRecord struct {
	command MaintenanceCommand
	outcome MaintenanceCommandOutcome
	detail  string
}

func (record MaintenanceCommandRecord) Valid() bool { return record.command.Valid() }
func (record MaintenanceCommandRecord) Command() MaintenanceCommand {
	if !record.Valid() {
		return MaintenanceCommand{}
	}
	return record.command
}
func (record MaintenanceCommandRecord) Outcome() MaintenanceCommandOutcome {
	if !record.Valid() {
		return MaintenanceOutcomePending
	}
	return record.outcome
}

func (record MaintenanceCommandRecord) Detail() string { return record.detail }

// maintenanceSettlement couples the closed verdict and its diagnostic at the
// classifier. Detail is observation only and never authorizes an effect.
type maintenanceSettlement struct {
	outcome MaintenanceCommandOutcome
	detail  string
}

type persistedMaintenanceCommand struct {
	Schema            int       `json:"schema"`
	ID                string    `json:"id"`
	LeaseUUID         string    `json:"lease_uuid"`
	Tenant            string    `json:"tenant"`
	ProviderUUID      string    `json:"provider_uuid"`
	PlacementRevision uint64    `json:"placement_revision"`
	Kind              string    `json:"kind"`
	Payload           []byte    `json:"payload,omitempty"`
	PayloadHash       string    `json:"payload_hash"`
	BackendName       string    `json:"backend_name"`
	BackendStorageID  string    `json:"backend_storage_id"`
	LifecycleKind     string    `json:"lifecycle_kind"`
	LifecycleID       string    `json:"lifecycle_id,omitempty"`
	CallbackURL       string    `json:"callback_url"`
	Outcome           string    `json:"outcome"`
	Phase             string    `json:"phase,omitempty"`
	Detail            string    `json:"detail,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	SettledAt         time.Time `json:"settled_at,omitempty"`
}

func initializeMaintenanceCommandJournal(tx *bolt.Tx) error {
	metadata := tx.Bucket(metadataBucketName)
	if metadata == nil {
		return errors.New("placement metadata bucket missing")
	}
	journal, err := metadata.CreateBucketIfNotExists(maintenanceCommandBucketName)
	if err != nil {
		return fmt.Errorf("create maintenance command journal: %w", err)
	}
	if _, err := journal.CreateBucketIfNotExists(maintenancePendingBucketName); err != nil {
		return fmt.Errorf("create pending maintenance command bucket: %w", err)
	}
	if _, err := journal.CreateBucketIfNotExists(maintenanceRecordsBucketName); err != nil {
		return fmt.Errorf("create maintenance command records bucket: %w", err)
	}
	return nil
}

func maintenanceCommandBuckets(tx *bolt.Tx) (*bolt.Bucket, *bolt.Bucket, error) {
	metadata := tx.Bucket(metadataBucketName)
	if metadata == nil {
		return nil, nil, fmt.Errorf("%w: metadata bucket missing", ErrMaintenanceJournalCorrupt)
	}
	journal := metadata.Bucket(maintenanceCommandBucketName)
	if journal == nil {
		return nil, nil, fmt.Errorf("%w: journal bucket missing", ErrMaintenanceJournalCorrupt)
	}
	pending := journal.Bucket(maintenancePendingBucketName)
	records := journal.Bucket(maintenanceRecordsBucketName)
	if pending == nil || records == nil {
		return nil, nil, fmt.Errorf("%w: pending or command bucket missing", ErrMaintenanceJournalCorrupt)
	}
	return pending, records, nil
}

func maintenanceReceiptKey(leaseUUID string, id maintenanceid.ID) []byte {
	return []byte(leaseUUID + "\x00" + id.String())
}

// rejectPendingMaintenanceTx is the placement mutation choke point. Once a
// maintenance command is durable, no placement/lifecycle transition may
// overtake it until settlement atomically removes this head.
func rejectPendingMaintenanceTx(tx *bolt.Tx, leaseUUID string) error {
	pending, _, err := maintenanceCommandBuckets(tx)
	if err != nil {
		return err
	}
	if pending.Get([]byte(leaseUUID)) != nil {
		return fmt.Errorf(
			"%w: lease %q has a pending maintenance command",
			ErrMaintenanceCommandConflict, leaseUUID,
		)
	}
	return nil
}

func pendingMaintenanceLeasesTx(tx *bolt.Tx) (map[string]struct{}, error) {
	pending, _, err := maintenanceCommandBuckets(tx)
	if err != nil {
		return nil, err
	}
	result := make(map[string]struct{})
	if err := pending.ForEach(func(leaseUUID, id []byte) error {
		if id == nil || !canonicalLeaseUUID(string(leaseUUID)) {
			return fmt.Errorf("%w: invalid pending maintenance head", ErrMaintenanceJournalCorrupt)
		}
		result[string(leaseUUID)] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// LookupMaintenanceCommand reads one lease-scoped ID. The same UUID may be
// used for a different lease without collision; within a lease it names one
// immutable command forever.
func (s *Store) LookupMaintenanceCommand(
	leaseUUID string,
	id maintenanceid.ID,
) (MaintenanceCommandRecord, bool, error) {
	if s == nil || !canonicalLeaseUUID(leaseUUID) || !id.Valid() {
		return MaintenanceCommandRecord{}, false, ErrInvalidMaintenanceCommand
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result MaintenanceCommandRecord
	found := false
	err := s.viewRuntimeAuthority(func(tx *bolt.Tx) error {
		_, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		encoded := records.Get(maintenanceReceiptKey(leaseUUID, id))
		if encoded == nil {
			return nil
		}
		command, outcome, _, _, detail, err := decodeMaintenanceCommand(encoded)
		if err != nil {
			return fmt.Errorf("%w: decode command record: %w", ErrMaintenanceJournalCorrupt, err)
		}
		if command.leaseUUID != leaseUUID || command.id != id {
			return fmt.Errorf("%w: command record key mismatch", ErrMaintenanceJournalCorrupt)
		}
		result = MaintenanceCommandRecord{command: command, outcome: outcome, detail: detail}
		found = true
		return nil
	})
	return result, found, err
}

func encodeMaintenanceCommand(command MaintenanceCommand, outcome MaintenanceCommandOutcome, createdAt, settledAt time.Time) ([]byte, error) {
	encoded, _, err := encodeMaintenanceSettlement(command, maintenanceSettlement{outcome: outcome}, createdAt, settledAt)
	return encoded, err
}

func encodeMaintenanceSettlement(command MaintenanceCommand, settlement maintenanceSettlement, createdAt, settledAt time.Time) ([]byte, string, error) {
	outcome := settlement.outcome
	if err := validateMaintenanceDetail(settlement.detail, outcome); err != nil {
		return nil, "", err
	}
	if !command.Valid() {
		return nil, "", ErrInvalidMaintenanceCommand
	}
	if outcome > MaintenanceOutcomeBackendUnavailable {
		return nil, "", ErrInvalidMaintenanceCommand
	}
	persistedPayload := append([]byte(nil), command.payload...)
	if outcome != MaintenanceOutcomePending {
		persistedPayload = nil
	}
	record := persistedMaintenanceCommand{
		Schema: maintenanceCommandSchema, ID: command.id.String(), LeaseUUID: command.leaseUUID,
		Tenant: command.tenant, ProviderUUID: command.providerUUID,
		PlacementRevision: command.placementRevision, Kind: command.kind.String(),
		Payload: persistedPayload, PayloadHash: command.PayloadHash(),
		BackendName: command.backendName, BackendStorageID: command.backendStorageID.String(),
		LifecycleKind: func() string {
			if command.lifecycleLegacy {
				return "legacy"
			}
			return "typed"
		}(),
		CallbackURL: command.callbackURL,
		Outcome:     outcome.String(), CreatedAt: createdAt.UTC(), SettledAt: settledAt.UTC(),
		Detail: settlement.detail,
	}
	if outcome == MaintenanceOutcomePending {
		record.Phase = command.phase.String()
	}
	if command.lifecycleID.Valid() {
		record.LifecycleID = command.lifecycleID.String()
	}
	// The tighter fresh-admission budget is enforced only by Begin. A legacy
	// pending row may acquire an explicit phase for the first time on recovery;
	// that durable transition must retain the full record budget.
	return encodeBoundedMaintenanceRecord(record, maxMaintenanceCommandValueBytes)
}

// encodeBoundedMaintenanceRecord preserves diagnostic text within the exact
// JSON receipt budget. New admission reserves its worst-case size; older
// pending rows may have less space. Observational detail must never prevent
// their authoritative settlement. Return the committed prefix so the first
// response and replay expose exactly the same diagnostic.
func encodeBoundedMaintenanceRecord(record persistedMaintenanceCommand, limit int) ([]byte, string, error) {
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, "", err
	}
	if len(encoded) <= limit {
		return encoded, record.Detail, nil
	}
	if record.Detail != "" {
		runes := []rune(record.Detail)
		low, high := 0, len(runes)
		for low < high {
			middle := low + (high-low+1)/2
			record.Detail = string(runes[:middle])
			candidate, err := json.Marshal(record)
			if err != nil {
				return nil, "", err
			}
			if len(candidate) <= limit {
				low = middle
			} else {
				high = middle - 1
			}
		}
		record.Detail = string(runes[:low])
		encoded, err = json.Marshal(record)
		if err != nil {
			return nil, "", err
		}
	}
	if len(encoded) > limit {
		return nil, "", fmt.Errorf(
			"%w: encoded command exceeds %d-byte %s budget",
			ErrInvalidMaintenanceCommand, limit, record.Outcome,
		)
	}
	return encoded, record.Detail, nil
}

func decodeMaintenanceCommand(encoded []byte) (
	MaintenanceCommand,
	MaintenanceCommandOutcome,
	time.Time,
	time.Time,
	string,
	error,
) {
	if len(encoded) == 0 || len(encoded) > maxMaintenanceCommandValueBytes {
		return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "",
			errors.New("maintenance command has an invalid encoded size")
	}
	var record persistedMaintenanceCommand
	if err := strictjson.DecodeObject(
		encoded, maxMaintenanceCommandValueBytes, &record,
	); err != nil {
		return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "", err
	}
	if record.Schema != maintenanceCommandSchema || record.CreatedAt.IsZero() {
		return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "",
			errors.New("invalid maintenance command schema or timestamp")
	}
	id, err := maintenanceid.Parse(record.ID)
	if err != nil {
		return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "", err
	}
	storageID, err := backendidentity.Parse(record.BackendStorageID)
	if err != nil {
		return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "", err
	}
	var lifecycleID lifecycle.ID
	lifecycleLegacy := false
	switch record.LifecycleKind {
	case "legacy":
		if record.LifecycleID != "" {
			return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "",
				errors.New("legacy maintenance command carries a lifecycle ID")
		}
		lifecycleLegacy = true
	case "typed":
		lifecycleID, err = lifecycle.ParseID(record.LifecycleID)
		if err != nil {
			return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "", err
		}
	default:
		return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "",
			errors.New("invalid maintenance lifecycle kind")
	}
	outcome, ok := parseMaintenanceCommandOutcome(record.Outcome)
	if !ok {
		return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "",
			errors.New("invalid maintenance outcome")
	}
	if err := validateMaintenanceDetail(record.Detail, outcome); err != nil {
		return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "", err
	}
	phase, err := decodeMaintenancePhase(record.Phase, outcome)
	if err != nil {
		return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "", err
	}
	if outcome == MaintenanceOutcomePending && !record.SettledAt.IsZero() ||
		outcome != MaintenanceOutcomePending && record.SettledAt.IsZero() {
		return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "",
			errors.New("maintenance settlement timestamp disagrees with outcome")
	}
	if !record.SettledAt.IsZero() && record.SettledAt.Before(record.CreatedAt) {
		return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "",
			errors.New("maintenance settlement predates admission")
	}
	command := MaintenanceCommand{
		phase: phase,
		id:    id, leaseUUID: record.LeaseUUID, tenant: record.Tenant,
		providerUUID: record.ProviderUUID, placementRevision: record.PlacementRevision,
		kind: parseMaintenanceCommandKind(record.Kind), payload: append([]byte(nil), record.Payload...),
		backendName: record.BackendName, backendStorageID: storageID,
		lifecycleLegacy: lifecycleLegacy,
		lifecycleID:     lifecycleID, callbackURL: record.CallbackURL,
	}
	command.payloadHash = sha256.Sum256(command.payload)
	err = validateMaintenanceCommand(command)
	if outcome != MaintenanceOutcomePending {
		if len(record.Payload) != 0 {
			return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "",
				errors.New("terminal maintenance receipt retains payload bytes")
		}
		digest, hashErr := hex.DecodeString(record.PayloadHash)
		if hashErr != nil || len(digest) != sha256.Size ||
			record.PayloadHash != strings.ToLower(record.PayloadHash) {
			return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "",
				errors.New("terminal maintenance receipt has an invalid payload fingerprint")
		}
		command = MaintenanceCommand{
			phase: maintenanceCompleted,
			id:    id, leaseUUID: record.LeaseUUID, tenant: record.Tenant,
			providerUUID: record.ProviderUUID, placementRevision: record.PlacementRevision,
			kind:        parseMaintenanceCommandKind(record.Kind),
			backendName: record.BackendName, backendStorageID: storageID,
			lifecycleLegacy: lifecycleLegacy,
			lifecycleID:     lifecycleID, callbackURL: record.CallbackURL, terminal: true,
		}
		copy(command.payloadHash[:], digest)
		err = validateMaintenanceCommand(command)
	}
	if err != nil {
		return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "", err
	}
	if record.PayloadHash != command.PayloadHash() {
		return MaintenanceCommand{}, 0, time.Time{}, time.Time{}, "",
			errors.New("maintenance payload fingerprint mismatch")
	}
	return command, outcome, record.CreatedAt, record.SettledAt, record.Detail, nil
}

// A diagnostic is bounded independently of the payload and can only accompany
// the validation-refusal receipt that admitted it. Older receipts omit it.
func validateMaintenanceDetail(detail string, outcome MaintenanceCommandOutcome) error {
	if len(detail) > maxMaintenanceRefusalDetailBytes || !utf8.ValidString(detail) ||
		(detail != "" && outcome != MaintenanceOutcomeValidationRejected) {
		return fmt.Errorf("%w: invalid maintenance refusal detail", ErrInvalidMaintenanceCommand)
	}
	return nil
}

// prepareMaintenanceCommand mints one immutable command exclusively from the
// current store-owned placement, storage, lifecycle, runtime principal, and
// construction-bound callback route. Its arguments contain only the tenant's
// requested effect; no caller can splice routing or identity authority into
// the durable journal.
func (s *Store) prepareMaintenanceCommand(
	id maintenanceid.ID,
	leaseUUID string,
	kind MaintenanceCommandKind,
	payload []byte,
) (PreparedMaintenanceCommand, error) {
	if s == nil || s.callbackRoutes == nil || !s.callbackRoutes.Valid() {
		return PreparedMaintenanceCommand{}, ErrInvalidMaintenanceCommand
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return PreparedMaintenanceCommand{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.unprojectedPositiveErrorLocked(leaseUUID); err != nil {
		return PreparedMaintenanceCommand{}, err
	}
	placementRecord, exists := s.cache[leaseUUID]
	capability, lifecycleExists := s.lifecycleCache[leaseUUID]
	if !exists || !lifecycleExists ||
		!maintenanceAuthorityAvailable(placementRecord, capability, s.providerUUID) {
		return PreparedMaintenanceCommand{}, ErrMaintenanceCommandConflict
	}
	storageID, storageBound := s.backendStorageIDs[placementRecord.Backend]
	if !storageBound || !storageID.Valid() {
		return PreparedMaintenanceCommand{}, ErrMaintenanceCommandConflict
	}
	var (
		route lifecycle.Route
		err   error
	)
	if capability.id.Valid() {
		route, err = s.callbackRoutes.lifecycleRoutes.For(capability.id)
	} else {
		route, err = s.callbackRoutes.lifecycleRoutes.Legacy()
	}
	if err != nil {
		return PreparedMaintenanceCommand{}, fmt.Errorf("mint maintenance callback route: %w", err)
	}
	revision := s.newRecordRevision(leaseUUID, placementRecord.revision)
	command, err := newMaintenanceCommand(
		id,
		leaseUUID,
		capability.principal,
		revision,
		kind,
		payload,
		placementRecord.Backend,
		storageID,
		route,
	)
	if err != nil {
		return PreparedMaintenanceCommand{}, err
	}
	return PreparedMaintenanceCommand{issuer: s, revision: revision, command: command}, nil
}

func maintenanceAuthorityAvailable(
	placementRecord Placement,
	capability lifecycleCapability,
	providerUUID string,
) bool {
	return placementRecord.State() == StateConfirmed && placementRecord.Attempt == "" &&
		!placementRecord.Conflict && placementRecord.revision != 0 &&
		capability.principal.valid() && capability.principal.providerUUID == providerUUID &&
		!capability.unusable && !capability.retired && capability.attemptBackend == "" &&
		capability.backend == placementRecord.Backend
}

func lifecycleMatchesMaintenanceCommand(
	capability lifecycleCapability,
	command MaintenanceCommand,
) bool {
	if command.lifecycleLegacy {
		return !capability.id.Valid() && !command.lifecycleID.Valid()
	}
	return capability.id.Valid() && capability.id == command.lifecycleID
}

// reauthorizeMaintenanceCommand verifies the exact pending claim against the
// current local aggregate. A decoded command or terminal record has no claim
// type and therefore cannot reach this authority boundary.
func (s *Store) reauthorizeMaintenanceCommand(claim MaintenanceCommandClaim) error {
	if s == nil || !claim.Valid() || claim.issuer != s {
		return ErrInvalidMaintenanceCommand
	}
	command := claim.command
	if err := s.reattestRuntimeAuthority(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.requireMaintenancePhaseLocked(claim, maintenanceDeliveryOutstanding); err != nil {
		return err
	}
	current, exists := s.cache[command.leaseUUID]
	capability, lifecycleExists := s.lifecycleCache[command.leaseUUID]
	storageID, storageBound := s.backendStorageIDs[command.backendName]
	if !exists || !lifecycleExists || !storageBound ||
		!maintenanceAuthorityAvailable(current, capability, s.providerUUID) ||
		current.revision != command.placementRevision || current.Backend != command.backendName ||
		storageID != command.backendStorageID || capability.principal.tenant != command.tenant ||
		capability.principal.providerUUID != command.providerUUID ||
		!lifecycleMatchesMaintenanceCommand(capability, command) {
		return ErrMaintenanceCommandConflict
	}
	return nil
}

// beginMaintenanceCommand writes the exact command before any backend side
// effect. Exact terminal receipts are idempotent; an exact pending replay gets
// the same settlement claim. No different ID can overtake a pending head.
func (s *Store) beginMaintenanceCommand(
	prepared PreparedMaintenanceCommand,
) (MaintenanceCommandAdmission, error) {
	if s == nil || !prepared.Valid() || prepared.issuer != s {
		return MaintenanceCommandAdmission{}, ErrInvalidMaintenanceCommand
	}
	command := prepared.command
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.cache[command.leaseUUID]
	capability, lifecycleExists := s.lifecycleCache[command.leaseUUID]
	if !exists || !maintenanceAuthorityAvailable(current, capability, s.providerUUID) ||
		current.revision != command.placementRevision ||
		current.Backend != command.backendName ||
		s.backendStorageIDs[command.backendName] != command.backendStorageID ||
		!lifecycleExists || capability.principal.tenant != command.tenant ||
		capability.principal.providerUUID != command.providerUUID ||
		!lifecycleMatchesMaintenanceCommand(capability, command) {
		return MaintenanceCommandAdmission{}, ErrMaintenanceCommandConflict
	}
	var result MaintenanceCommandAdmission
	err := s.updateRuntimeAuthority(func(tx *bolt.Tx) error {
		if err := validateMaintenanceAdmissionTx(tx, command); err != nil {
			return err
		}
		pending, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		count, err := countMaintenanceReceipts(records, command.leaseUUID)
		if err != nil {
			return err
		}
		key := maintenanceReceiptKey(command.leaseUUID, command.id)
		if encoded := records.Get(key); encoded != nil {
			stored, outcome, _, _, detail, decodeErr := decodeMaintenanceCommand(encoded)
			if decodeErr != nil {
				return fmt.Errorf("%w: decode receipt: %w", ErrMaintenanceJournalCorrupt, decodeErr)
			}
			if !stored.equal(command) {
				return ErrMaintenanceCommandConflict
			}
			result = MaintenanceCommandAdmission{command: stored, outcome: outcome, detail: detail}
			if outcome == MaintenanceOutcomePending {
				head := pending.Get([]byte(command.leaseUUID))
				if string(head) != command.id.String() {
					return fmt.Errorf("%w: pending head mismatch", ErrMaintenanceJournalCorrupt)
				}
				result.claim = MaintenanceCommandClaim{issuer: s, command: stored}
			}
			return nil
		}
		// An exact durable receipt above is safe to recover or replay while an
		// interrupted inventory sweep is unresolved. Minting a new command is
		// not: the missing observation may change the lease's owner, lifecycle,
		// or retained/live mode.
		if err := s.unprojectedPositiveErrorLocked(command.leaseUUID); err != nil {
			return err
		}
		if head := pending.Get([]byte(command.leaseUUID)); head != nil {
			return ErrMaintenanceCommandConflict
		}
		if count >= maxMaintenanceCommandsPerLease {
			return ErrMaintenanceHistoryFull
		}
		createdAt := s.now().UTC()
		encoded, err := encodeMaintenanceCommand(command, MaintenanceOutcomePending, createdAt, time.Time{})
		if err != nil {
			return err
		}
		if len(encoded) > maxMaintenanceCommandAdmissionBytes {
			return fmt.Errorf("%w: command exceeds %d-byte admission budget", ErrInvalidMaintenanceCommand, maxMaintenanceCommandAdmissionBytes)
		}
		if err := records.Put(key, encoded); err != nil {
			return err
		}
		if err := pending.Put([]byte(command.leaseUUID), []byte(command.id.String())); err != nil {
			return err
		}
		claim := MaintenanceCommandClaim{issuer: s, command: command}
		result = MaintenanceCommandAdmission{claim: claim, command: command, outcome: MaintenanceOutcomePending}
		return nil
	})
	return result, err
}

// countMaintenanceReceipts reserves the lease's bounded, lifetime idempotency
// history before dispatch. No live-lease receipt may age out: the provider and
// backend must agree forever whether a UUID names completed work, otherwise an
// exact late retry could be mistaken for a new asynchronous mutation. History
// is reclaimed only after both placement and lifecycle authority are gone.
func countMaintenanceReceipts(
	records *bolt.Bucket,
	leaseUUID string,
) (int, error) {
	prefix := []byte(leaseUUID + "\x00")
	cursor := records.Cursor()
	retained := 0
	for key, value := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
		command, _, _, _, _, err := decodeMaintenanceCommand(value)
		if err != nil {
			return 0, fmt.Errorf("%w: decode receipt for capacity: %w", ErrMaintenanceJournalCorrupt, err)
		}
		if command.leaseUUID != leaseUUID || !bytes.Equal(
			key, maintenanceReceiptKey(command.leaseUUID, command.id),
		) {
			return 0, fmt.Errorf("%w: command record key mismatch", ErrMaintenanceJournalCorrupt)
		}
		retained++
	}
	return retained, nil
}

func validateMaintenanceAdmissionTx(tx *bolt.Tx, command MaintenanceCommand) error {
	placements := tx.Bucket(bucketName)
	lifecycles := tx.Bucket(lifecycleCapabilityBucketName)
	if placements == nil || lifecycles == nil {
		return ErrMaintenanceJournalCorrupt
	}
	placementRecord := decodeRecord(command.leaseUUID, placements.Get([]byte(command.leaseUUID)))
	if placementRecord.State() != StateConfirmed || placementRecord.Attempt != "" ||
		placementRecord.Conflict || placementRecord.revision != command.placementRevision ||
		placementRecord.Backend != command.backendName {
		return ErrMaintenanceCommandConflict
	}
	capability, err := decodeLifecycleCapability(lifecycles.Get([]byte(command.leaseUUID)))
	if err != nil || !maintenanceAuthorityAvailable(placementRecord, capability, command.providerUUID) ||
		capability.principal.tenant != command.tenant ||
		!lifecycleMatchesMaintenanceCommand(capability, command) {
		return ErrMaintenanceCommandConflict
	}
	metadata, err := loadTopologyMetadata(tx)
	if err != nil || metadata.ProviderUUID != command.providerUUID ||
		metadata.KnownBackendStorageIDs[command.backendName] != command.backendStorageID.String() {
		return ErrMaintenanceCommandConflict
	}
	return nil
}

// settleMaintenanceCommand is the sole raw-enum mutation boundary. Production
// callers reach it only after MaintenanceCoordinator's closed chain/backend
// classifiers derive the outcome; no exported method accepts a caller-selected
// persisted enum.
func (s *Store) settleMaintenanceCommand(claim MaintenanceCommandClaim, outcome MaintenanceCommandOutcome) error {
	_, err := s.settleMaintenanceDelivery(claim, maintenanceSettlement{outcome: outcome})
	return err
}

func (s *Store) settleMaintenanceDelivery(claim MaintenanceCommandClaim, settlement maintenanceSettlement) (MaintenanceCommandRecord, error) {
	// Update acceptance requires a payload-commit capability. A generic
	// transport or chain classifier cannot retire its recovery bytes.
	if settlement.outcome == MaintenanceOutcomeAccepted && claim.command.kind == MaintenanceCommandUpdate {
		return MaintenanceCommandRecord{}, ErrMaintenanceCommandNotPending
	}
	return s.settleMaintenancePhaseReceipt(claim, settlement, maintenanceDeliveryOutstanding)
}

func (s *Store) settleMaintenancePhase(claim MaintenanceCommandClaim, settlement maintenanceSettlement, phase maintenanceJournalPhase) error {
	_, err := s.settleMaintenancePhaseReceipt(claim, settlement, phase)
	return err
}

func (s *Store) settleMaintenancePhaseReceipt(claim MaintenanceCommandClaim, settlement maintenanceSettlement, phase maintenanceJournalPhase) (MaintenanceCommandRecord, error) {
	outcome := settlement.outcome
	if s == nil || !claim.Valid() || claim.issuer != s || outcome == MaintenanceOutcomePending ||
		outcome > MaintenanceOutcomeBackendUnavailable {
		return MaintenanceCommandRecord{}, ErrMaintenanceCommandNotPending
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var receipt MaintenanceCommandRecord
	err := s.updateRuntimeAuthority(func(tx *bolt.Tx) error {
		pending, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		command := claim.command
		head := pending.Get([]byte(command.leaseUUID))
		if string(head) != command.id.String() {
			return ErrMaintenanceCommandNotPending
		}
		key := maintenanceReceiptKey(command.leaseUUID, command.id)
		encoded := records.Get(key)
		stored, currentOutcome, createdAt, _, _, err := decodeMaintenanceCommand(encoded)
		if err != nil {
			return fmt.Errorf("%w: decode pending receipt: %w", ErrMaintenanceJournalCorrupt, err)
		}
		if currentOutcome != MaintenanceOutcomePending || stored.phase != phase || !stored.equal(command) {
			return ErrMaintenanceCommandNotPending
		}
		settledAt := s.now().UTC()
		if settledAt.Before(createdAt) {
			// Wall clocks can move backward. Never write a receipt that our strict
			// decoder would immediately reject; lifetime retention is keyed to
			// durable lease authority rather than elapsed wall time.
			settledAt = createdAt
		}
		settled, detail, err := encodeMaintenanceSettlement(stored, settlement, createdAt, settledAt)
		if err != nil {
			return err
		}
		if err := records.Put(key, settled); err != nil {
			return err
		}
		if err := pending.Delete([]byte(command.leaseUUID)); err != nil {
			return err
		}
		stored.terminal, stored.phase, stored.payload = true, maintenanceCompleted, nil
		receipt = MaintenanceCommandRecord{command: stored, outcome: outcome, detail: detail}
		return nil
	})
	if err != nil {
		return MaintenanceCommandRecord{}, err
	}
	return receipt, nil
}

// reclaimDetachedMaintenanceCommandsForLeaseTx removes one lease's terminal
// history only after the same write transaction proves that both placement and
// lifecycle authority are absent. Ordinary authority deletion calls this at
// the transition itself, so reclamation has no crash window and recovery never
// needs a fleet-wide receipt scan.
func reclaimDetachedMaintenanceCommandsForLeaseTx(
	tx *bolt.Tx,
	leaseUUID string,
) error {
	if !canonicalLeaseUUID(leaseUUID) {
		// Maintenance admission only accepts canonical lease UUIDs. Placement
		// still has to retire pre-journal/legacy opaque keys, for which no valid
		// maintenance receipt can exist.
		return nil
	}
	pending, records, err := maintenanceCommandBuckets(tx)
	if err != nil {
		return err
	}
	placements := tx.Bucket(bucketName)
	lifecycles := tx.Bucket(lifecycleCapabilityBucketName)
	if placements == nil || lifecycles == nil {
		return fmt.Errorf("%w: placement or lifecycle bucket missing", ErrMaintenanceJournalCorrupt)
	}
	leaseKey := []byte(leaseUUID)
	if placements.Get(leaseKey) != nil || lifecycles.Get(leaseKey) != nil {
		return nil
	}
	if pending.Get(leaseKey) != nil {
		return fmt.Errorf(
			"%w: detached lease %q retains a pending command",
			ErrMaintenanceJournalCorrupt,
			leaseUUID,
		)
	}
	prefix := []byte(leaseUUID + "\x00")
	cursor := records.Cursor()
	for key, value := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
		command, outcome, _, _, _, decodeErr := decodeMaintenanceCommand(value)
		if decodeErr != nil {
			return fmt.Errorf("%w: decode detached receipt: %w", ErrMaintenanceJournalCorrupt, decodeErr)
		}
		if outcome == MaintenanceOutcomePending || command.leaseUUID != leaseUUID ||
			!bytes.Equal(key, maintenanceReceiptKey(command.leaseUUID, command.id)) {
			return fmt.Errorf("%w: invalid detached command record", ErrMaintenanceJournalCorrupt)
		}
		if err := cursor.Delete(); err != nil {
			return err
		}
	}
	return nil
}

// reclaimDetachedMaintenanceCommands repairs databases created before
// authority deletion reclaimed receipts atomically. It is intentionally
// package-private: no shipped repair command invokes it, and exposing an
// unscoped journal mutation would bypass MaintenanceCoordinator.
// Runtime maintenance recovery does not call it: normal writes use the
// lease-scoped helper above and therefore do work proportional only to the
// authority being removed.
func (s *Store) reclaimDetachedMaintenanceCommands() (int, error) {
	if s == nil {
		return 0, ErrMaintenanceJournalCorrupt
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	err := s.updateRuntimeAuthority(func(tx *bolt.Tx) error {
		_, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		placements := tx.Bucket(bucketName)
		lifecycles := tx.Bucket(lifecycleCapabilityBucketName)
		if placements == nil || lifecycles == nil {
			return fmt.Errorf("%w: placement or lifecycle bucket missing", ErrMaintenanceJournalCorrupt)
		}
		cursor := records.Cursor()
		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			command, outcome, _, _, _, decodeErr := decodeMaintenanceCommand(value)
			if decodeErr != nil {
				return fmt.Errorf("%w: decode receipt during reclamation: %w", ErrMaintenanceJournalCorrupt, decodeErr)
			}
			if !bytes.Equal(key, maintenanceReceiptKey(command.leaseUUID, command.id)) {
				return fmt.Errorf("%w: command record key mismatch", ErrMaintenanceJournalCorrupt)
			}
			leaseKey := []byte(command.leaseUUID)
			if outcome == MaintenanceOutcomePending || placements.Get(leaseKey) != nil ||
				lifecycles.Get(leaseKey) != nil {
				continue
			}
			if err := cursor.Delete(); err != nil {
				return err
			}
			removed++
		}
		return nil
	})
	return removed, err
}

// pendingMaintenanceCommands returns exact replay capabilities for startup and
// periodic recovery. Any structural mismatch fails the complete snapshot.
func (s *Store) pendingMaintenanceCommands() ([]MaintenanceCommandClaim, error) {
	if s == nil {
		return nil, ErrMaintenanceJournalCorrupt
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var claims []MaintenanceCommandClaim
	err := s.viewRuntimeAuthority(func(tx *bolt.Tx) error {
		pending, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		return pending.ForEach(func(leaseKey, value []byte) error {
			if value == nil {
				return fmt.Errorf("%w: nested pending row", ErrMaintenanceJournalCorrupt)
			}
			id, err := maintenanceid.Parse(string(value))
			if err != nil {
				return fmt.Errorf("%w: invalid pending ID", ErrMaintenanceJournalCorrupt)
			}
			encoded := records.Get(maintenanceReceiptKey(string(leaseKey), id))
			command, outcome, _, _, _, err := decodeMaintenanceCommand(encoded)
			if err != nil || outcome != MaintenanceOutcomePending || command.leaseUUID != string(leaseKey) {
				return fmt.Errorf("%w: invalid pending receipt for %q", ErrMaintenanceJournalCorrupt, leaseKey)
			}
			claims = append(claims, MaintenanceCommandClaim{issuer: s, command: command})
			return nil
		})
	})
	return claims, err
}

// verifyMaintenanceCommandJournal proves both directions of the journal's
// invariant: every pending command is the sole head for its lease, every head
// names that exact pending command, and terminal history is never a head.
func verifyMaintenanceCommandJournal(tx *bolt.Tx) error {
	pending, records, err := maintenanceCommandBuckets(tx)
	if err != nil {
		return err
	}
	if err := verifyMaintenanceCommandBuckets(pending, records); err != nil {
		return err
	}
	// A pending command and its routing authority form one cross-domain fact.
	// Prove that fact while the same read transaction pins every source: a
	// process must never rehydrate a lease claim for a command whose placement,
	// lifecycle generation, provider, or storage identity has already moved.
	return pending.ForEach(func(leaseKey, encodedID []byte) error {
		id, parseErr := maintenanceid.Parse(string(encodedID))
		if parseErr != nil {
			return fmt.Errorf("%w: invalid pending ID", ErrMaintenanceJournalCorrupt)
		}
		encoded := records.Get(maintenanceReceiptKey(string(leaseKey), id))
		command, outcome, _, _, _, decodeErr := decodeMaintenanceCommand(encoded)
		if decodeErr != nil || outcome != MaintenanceOutcomePending {
			return fmt.Errorf(
				"%w: invalid pending command authority for lease %q",
				ErrMaintenanceJournalCorrupt, leaseKey,
			)
		}
		if validationErr := validateMaintenanceAdmissionTx(tx, command); validationErr != nil {
			return fmt.Errorf(
				"%w: pending command authority for lease %q: %w",
				ErrMaintenanceJournalCorrupt, leaseKey, validationErr,
			)
		}
		return nil
	})
}

func verifyMaintenanceCommandBuckets(pending, records *bolt.Bucket) error {
	if pending == nil || records == nil {
		return fmt.Errorf("%w: pending or command bucket missing", ErrMaintenanceJournalCorrupt)
	}
	heads := make(map[string]maintenanceid.ID)
	if err := pending.ForEach(func(key, value []byte) error {
		if value == nil || !canonicalLeaseUUID(string(key)) {
			return fmt.Errorf("%w: invalid pending head key", ErrMaintenanceJournalCorrupt)
		}
		id, parseErr := maintenanceid.Parse(string(value))
		if parseErr != nil {
			return fmt.Errorf("%w: invalid pending head ID", ErrMaintenanceJournalCorrupt)
		}
		heads[string(key)] = id
		return nil
	}); err != nil {
		return err
	}

	pendingRecords := make(map[string]maintenanceid.ID)
	recordCounts := make(map[string]int)
	if err := records.ForEach(func(key, value []byte) error {
		if value == nil {
			return fmt.Errorf("%w: nested command record", ErrMaintenanceJournalCorrupt)
		}
		command, outcome, _, _, _, decodeErr := decodeMaintenanceCommand(value)
		if decodeErr != nil {
			return fmt.Errorf("%w: decode command record: %w", ErrMaintenanceJournalCorrupt, decodeErr)
		}
		if !bytes.Equal(key, maintenanceReceiptKey(command.leaseUUID, command.id)) {
			return fmt.Errorf("%w: command record key mismatch", ErrMaintenanceJournalCorrupt)
		}
		recordCounts[command.leaseUUID]++
		if recordCounts[command.leaseUUID] > maxMaintenanceCommandsPerLease {
			return fmt.Errorf("%w: command history exceeds per-lease capacity", ErrMaintenanceJournalCorrupt)
		}
		head, isHead := heads[command.leaseUUID]
		if outcome == MaintenanceOutcomePending {
			if previous, duplicate := pendingRecords[command.leaseUUID]; duplicate &&
				previous != command.id {
				return fmt.Errorf("%w: multiple pending commands for one lease", ErrMaintenanceJournalCorrupt)
			}
			pendingRecords[command.leaseUUID] = command.id
			if !isHead || head != command.id {
				return fmt.Errorf("%w: pending command has no exact head", ErrMaintenanceJournalCorrupt)
			}
		} else if isHead && head == command.id {
			return fmt.Errorf("%w: terminal command remains a pending head", ErrMaintenanceJournalCorrupt)
		}
		return nil
	}); err != nil {
		return err
	}
	for leaseUUID, id := range heads {
		if pendingRecords[leaseUUID] != id {
			return fmt.Errorf("%w: pending head has no exact command", ErrMaintenanceJournalCorrupt)
		}
	}
	return nil
}

func verifyMaintenanceCommandJournalInMetadata(metadata *bolt.Bucket) error {
	if metadata == nil {
		return fmt.Errorf("%w: metadata bucket missing", ErrMaintenanceJournalCorrupt)
	}
	journal := metadata.Bucket(maintenanceCommandBucketName)
	if journal == nil {
		return fmt.Errorf("%w: journal bucket missing", ErrMaintenanceJournalCorrupt)
	}
	pending := journal.Bucket(maintenancePendingBucketName)
	records := journal.Bucket(maintenanceRecordsBucketName)
	if pending == nil || records == nil {
		return fmt.Errorf("%w: pending or command bucket missing", ErrMaintenanceJournalCorrupt)
	}
	return verifyMaintenanceCommandBuckets(pending, records)
}
