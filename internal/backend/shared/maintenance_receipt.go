package shared

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/backendname"
)

const (
	maxMaintenanceReceiptsPerLease  = 1_024
	maxMaintenanceReceiptEntryBytes = 16 << 10
	maintenanceCompletionRecordV1   = 1
)

var callbackMaintenanceHistoryBucketName = []byte("completed_callback_maintenance_history")

// ErrMaintenanceReceiptCapacity is a pre-side-effect refusal: the backend
// cannot accept another maintenance generation while preserving every retry
// identity for the live lease. Receipts are reclaimed only after successful
// close installs the stronger permanent closed-lease fence.
var ErrMaintenanceReceiptCapacity = errors.New("maintenance receipt capacity exhausted")

// MaintenanceReceiptCapacityError preserves whether the lease-local or shared
// journal-wide receipt budget refused admission. It matches the coded capacity
// protocol so the refusal is definitive before substrate mutation.
type MaintenanceReceiptCapacityError struct {
	LeaseUUID string
	Limit     uint64
}

func (e *MaintenanceReceiptCapacityError) Error() string {
	if e == nil {
		return ErrMaintenanceReceiptCapacity.Error()
	}
	if e.LeaseUUID != "" {
		return fmt.Sprintf("%s for live lease %q (limit %d)",
			ErrMaintenanceReceiptCapacity, e.LeaseUUID, e.Limit)
	}
	return fmt.Sprintf("global callback receipt capacity exhausted (limit %d)", e.Limit)
}

func (e *MaintenanceReceiptCapacityError) Unwrap() []error {
	return []error{
		ErrMaintenanceReceiptCapacity,
		backend.ErrCapacityRefused,
		backend.ErrInsufficientResources,
	}
}

// MaintenanceRequestAuthority is the opaque, immutable identity of the wire
// request. It is valid only for the exact CallbackStore instance that issued
// it; reopening the same file requires minting fresh process-local authority.
// It can be constructed before any mutable provision/release read, so a
// completed retry never depends on reconstructing its now-superseded source
// generation. Payload bytes are reduced to SHA-256 at construction.
type MaintenanceRequestAuthority struct {
	settlement    *MaintenanceSettlement
	issuer        *CallbackStore
	id            MaintenanceID
	kind          MaintenanceIntentKind
	leaseUUID     string
	callbackURL   string
	backend       string
	storageID     backendidentity.ID
	payloadDigest [sha256.Size]byte
	digest        [sha256.Size]byte
}

type storedMaintenanceRequestAuthority struct {
	Version          uint8                 `json:"version"`
	MaintenanceID    MaintenanceID         `json:"maintenance_id"`
	Kind             MaintenanceIntentKind `json:"kind"`
	LeaseUUID        string                `json:"lease_uuid"`
	CallbackURL      string                `json:"callback_url"`
	Backend          string                `json:"backend"`
	BackendStorageID string                `json:"backend_storage_id"`
	PayloadDigest    string                `json:"payload_digest"`
}

func newMaintenanceRequestAuthority(
	issuer *CallbackStore,
	maintenanceID MaintenanceID,
	kind MaintenanceIntentKind,
	leaseUUID, callbackURL string,
	payload []byte,
	backendName string,
	storageID backendidentity.ID,
) (MaintenanceRequestAuthority, error) {
	authority := MaintenanceRequestAuthority{
		issuer: issuer,
		id:     maintenanceID, kind: kind, leaseUUID: leaseUUID,
		callbackURL: callbackURL, backend: backendName, storageID: storageID,
		payloadDigest: sha256.Sum256(payload),
	}
	if err := validateMaintenanceRequestAuthority(authority); err != nil {
		return MaintenanceRequestAuthority{}, err
	}
	digest, err := digestMaintenanceRequestAuthority(authority)
	if err != nil {
		return MaintenanceRequestAuthority{}, err
	}
	authority.digest = digest
	return authority, nil
}

// NewMaintenanceRequestAuthority mints request replay authority from this
// exact journal pair's verified backend lineage. Production callers therefore
// supply only wire facts; they cannot pair a request with caller-selected
// storage or release authority.
func (s *MaintenanceSettlement) NewMaintenanceRequestAuthority(
	maintenanceID MaintenanceID,
	kind MaintenanceIntentKind,
	leaseUUID, callbackURL string,
	payload []byte,
) (MaintenanceRequestAuthority, error) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return MaintenanceRequestAuthority{}, errors.New("maintenance settlement is invalid")
	}
	backendName, storageID := s.callbacks.journalBackendIdentity("")
	if backendName == "" || !storageID.Valid() {
		return MaintenanceRequestAuthority{}, errors.New(
			"maintenance request authority requires an identity-bound callback journal",
		)
	}
	authority, err := newMaintenanceRequestAuthority(
		s.callbacks, maintenanceID, kind, leaseUUID, callbackURL, payload, backendName, storageID,
	)
	if err != nil {
		return MaintenanceRequestAuthority{}, err
	}
	authority.settlement = s
	return authority, nil
}

func (a MaintenanceRequestAuthority) Valid() bool {
	if validateMaintenanceRequestAuthority(a) != nil || a.digest == ([sha256.Size]byte{}) {
		return false
	}
	digest, err := digestMaintenanceRequestAuthority(a)
	return err == nil && digest == a.digest
}
func (a MaintenanceRequestAuthority) MaintenanceID() MaintenanceID { return a.id }
func (a MaintenanceRequestAuthority) Kind() MaintenanceIntentKind  { return a.kind }
func (a MaintenanceRequestAuthority) LeaseUUID() string            { return a.leaseUUID }
func (a MaintenanceRequestAuthority) CallbackURL() string          { return a.callbackURL }
func (a MaintenanceRequestAuthority) Backend() string              { return a.backend }
func (a MaintenanceRequestAuthority) BackendStorageID() backendidentity.ID {
	return a.storageID
}

func validateMaintenanceRequestAuthority(authority MaintenanceRequestAuthority) error {
	if authority.issuer == nil {
		return errors.New("maintenance request authority has no issuing callback journal")
	}
	if authority.issuer.boltStore == nil {
		return errors.New("maintenance request authority has invalid callback journal lineage")
	}
	if binding := authority.issuer.binding; binding != nil &&
		(binding.backendName != authority.backend || binding.storageID != authority.storageID) {
		return errors.New("maintenance request authority differs from issuing callback journal lineage")
	}
	if !authority.id.Valid() {
		return errors.New("maintenance request authority requires a UUIDv4 identity")
	}
	if authority.kind != MaintenanceIntentRestart && authority.kind != MaintenanceIntentUpdate &&
		authority.kind != MaintenanceIntentCustomDomain {
		return fmt.Errorf("maintenance request authority has invalid kind %q", authority.kind)
	}
	if err := validateCanonicalLeaseUUID(authority.leaseUUID); err != nil {
		return err
	}
	if err := validateCallbackDestination(authority.callbackURL); err != nil {
		return fmt.Errorf("maintenance request callback: %w", err)
	}
	if err := backendname.Validate(authority.backend); err != nil {
		return fmt.Errorf("maintenance request backend: %w", err)
	}
	if !authority.storageID.Valid() {
		return errors.New("maintenance request authority requires backend storage identity")
	}
	return nil
}

func digestMaintenanceRequestAuthority(
	authority MaintenanceRequestAuthority,
) ([sha256.Size]byte, error) {
	data, err := json.Marshal(storedMaintenanceRequestAuthority{
		Version:          1,
		MaintenanceID:    authority.id,
		Kind:             authority.kind,
		LeaseUUID:        authority.leaseUUID,
		CallbackURL:      authority.callbackURL,
		Backend:          authority.backend,
		BackendStorageID: authority.storageID.String(),
		PayloadDigest:    encodeMaintenanceDigest(authority.payloadDigest),
	})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("marshal maintenance request authority: %w", err)
	}
	return sha256.Sum256(data), nil
}

type maintenanceCompletionRecord struct {
	Version            uint8                  `json:"version"`
	MaintenanceID      MaintenanceID          `json:"maintenance_id"`
	Kind               MaintenanceIntentKind  `json:"kind"`
	LeaseUUID          string                 `json:"lease_uuid"`
	RequestDigest      string                 `json:"request_digest"`
	CompletionSequence uint64                 `json:"completion_sequence"`
	Backend            string                 `json:"backend"`
	BackendStorageID   string                 `json:"backend_storage_id"`
	Tenant             string                 `json:"tenant"`
	ProviderUUID       string                 `json:"provider_uuid"`
	Status             backend.CallbackStatus `json:"status"`
	Error              string                 `json:"error,omitempty"`
	// EffectStarted and the exact target release identity preserve permanent
	// late-arrival cleanup authority after the live intent is consumed. Older
	// receipts omit these fields and therefore cannot authorize destruction.
	EffectStarted        bool      `json:"effect_started,omitempty"`
	TargetReleaseVersion int       `json:"target_release_version,omitempty"`
	TargetReleaseDigest  string    `json:"target_release_digest,omitempty"`
	SettledAt            time.Time `json:"settled_at"`
}

// FailedMaintenanceReceipt is permanent, store-issued authority to remove a
// replacement generation that becomes visible after its failed terminal
// settlement. It is minted only for an intent which durably crossed the
// physical-effect boundary and binds the exact immutable target release row.
// Pre-effect failures and older receipts deliberately cannot produce one.
type FailedMaintenanceReceipt struct {
	settlement *MaintenanceSettlement
	callbacks  *CallbackStore
	releases   *ReleaseStore
	record     maintenanceCompletionRecord
	storageID  backendidentity.ID
	target     ReleaseClaim
	release    Release
}

func (r FailedMaintenanceReceipt) Valid() bool {
	return r.settlement != nil && r.callbacks == r.settlement.callbacks &&
		r.releases == r.settlement.releases && r.record.Status == backend.CallbackStatusFailed &&
		r.record.EffectStarted && r.record.TargetReleaseVersion == r.target.Version() &&
		r.target.issuer == r.releases && r.target.valid() && r.storageID.Valid() &&
		r.record.BackendStorageID == r.storageID.String() && r.record.MaintenanceID.Valid()
}

func (r FailedMaintenanceReceipt) MaintenanceID() MaintenanceID { return r.record.MaintenanceID }
func (r FailedMaintenanceReceipt) LeaseUUID() string            { return r.record.LeaseUUID }
func (r FailedMaintenanceReceipt) Backend() string              { return r.record.Backend }
func (r FailedMaintenanceReceipt) BackendStorageID() backendidentity.ID {
	return r.storageID
}
func (r FailedMaintenanceReceipt) Tenant() string       { return r.record.Tenant }
func (r FailedMaintenanceReceipt) ProviderUUID() string { return r.record.ProviderUUID }
func (r FailedMaintenanceReceipt) SettledAt() time.Time { return r.record.SettledAt }
func (r FailedMaintenanceReceipt) TargetRelease() (Release, bool) {
	if !r.Valid() {
		return Release{}, false
	}
	return cloneRelease(r.release), true
}

func maintenanceEntryRequestDigest(entry maintenanceIntentEntry) ([sha256.Size]byte, error) {
	storageID, err := backendidentity.Parse(entry.BackendStorageID)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	payloadDigest, err := parseMaintenanceDigest(entry.RequestPayloadDigest, false)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	authority := MaintenanceRequestAuthority{
		id: entry.MaintenanceID, kind: entry.Kind, leaseUUID: entry.LeaseUUID,
		callbackURL: entry.RequestCallbackURL, backend: entry.Backend,
		storageID: storageID, payloadDigest: payloadDigest,
	}
	return digestMaintenanceRequestAuthority(authority)
}

func maintenanceCompletionRecordFor(
	claim MaintenanceIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
	settledAt time.Time,
	completionSequence uint64,
) maintenanceCompletionRecord {
	authority, _ := releaseRuntimeIdentityFor(claim.entry.TargetRelease)
	return maintenanceCompletionRecord{
		Version:              maintenanceCompletionRecordV1,
		MaintenanceID:        claim.MaintenanceID(),
		Kind:                 claim.Kind(),
		LeaseUUID:            claim.LeaseUUID(),
		RequestDigest:        claim.entry.RequestDigest,
		CompletionSequence:   completionSequence,
		Backend:              claim.Backend(),
		BackendStorageID:     claim.BackendStorageID().String(),
		Tenant:               authority.tenant,
		ProviderUUID:         authority.providerUUID,
		Status:               status,
		Error:                errMsg,
		EffectStarted:        !claim.entry.EffectNotStarted,
		TargetReleaseVersion: claim.entry.TargetReleaseVersion,
		TargetReleaseDigest:  claim.entry.TargetReleaseDigest,
		SettledAt:            settledAt,
	}
}

func validateMaintenanceCompletionRecord(record maintenanceCompletionRecord, leaseUUID string) error {
	if record.Version != maintenanceCompletionRecordV1 {
		return fmt.Errorf("maintenance receipt has unsupported version %d", record.Version)
	}
	if !record.MaintenanceID.Valid() {
		return errors.New("maintenance receipt has invalid maintenance ID")
	}
	if err := validateCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	if record.LeaseUUID != leaseUUID {
		return fmt.Errorf("maintenance receipt lease mismatch: key %q contains %q", leaseUUID, record.LeaseUUID)
	}
	if record.Kind != MaintenanceIntentRestart && record.Kind != MaintenanceIntentUpdate &&
		record.Kind != MaintenanceIntentCustomDomain {
		return fmt.Errorf("maintenance receipt has invalid kind %q", record.Kind)
	}
	if _, err := parseMaintenanceDigest(record.RequestDigest, false); err != nil {
		return fmt.Errorf("maintenance receipt request digest: %w", err)
	}
	if record.CompletionSequence == 0 {
		return errors.New("maintenance receipt completion sequence is required")
	}
	if err := backendname.Validate(record.Backend); err != nil {
		return fmt.Errorf("maintenance receipt backend: %w", err)
	}
	if _, err := backendidentity.Parse(record.BackendStorageID); err != nil {
		return fmt.Errorf("maintenance receipt storage identity: %w", err)
	}
	if err := validateCloseIntentIdentity("tenant", record.Tenant); err != nil {
		return err
	}
	if err := validateCloseIntentIdentity("provider", record.ProviderUUID); err != nil {
		return err
	}
	if record.TargetReleaseVersion < 0 {
		return errors.New("maintenance receipt target release version is invalid")
	}
	if record.TargetReleaseVersion == 0 {
		if record.TargetReleaseDigest != "" || record.EffectStarted {
			return errors.New("maintenance receipt has physical authority without a target release")
		}
	} else if _, err := parseMaintenanceDigest(record.TargetReleaseDigest, false); err != nil {
		return fmt.Errorf("maintenance receipt target release digest: %w", err)
	}
	switch record.Status {
	case backend.CallbackStatusSuccess:
		if record.Error != "" {
			return errors.New("successful maintenance receipt carries an error")
		}
	case backend.CallbackStatusFailed:
	default:
		return fmt.Errorf("maintenance receipt has invalid status %q", record.Status)
	}
	return validateStoredCallbackCreatedAt(record.SettledAt)
}

func marshalMaintenanceCompletionRecord(record maintenanceCompletionRecord) ([]byte, error) {
	if err := validateMaintenanceCompletionRecord(record, record.LeaseUUID); err != nil {
		return nil, err
	}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("marshal maintenance receipt: %w", err)
	}
	if len(data) > maxMaintenanceReceiptEntryBytes {
		return nil, fmt.Errorf("maintenance receipt exceeds %d bytes", maxMaintenanceReceiptEntryBytes)
	}
	return data, nil
}

func decodeMaintenanceCompletionRecord(
	leaseKey, receiptKey, value []byte,
) (maintenanceCompletionRecord, error) {
	var record maintenanceCompletionRecord
	if err := decodeStrictAuthoritativeObject(
		value, maxMaintenanceReceiptEntryBytes, &record,
	); err != nil {
		return maintenanceCompletionRecord{}, fmt.Errorf("decode maintenance receipt for lease %q: %w", leaseKey, err)
	}
	if err := validateMaintenanceCompletionRecord(record, string(leaseKey)); err != nil {
		return maintenanceCompletionRecord{}, fmt.Errorf("invalid maintenance receipt for lease %q: %w", leaseKey, err)
	}
	if string(receiptKey) != record.MaintenanceID.String() {
		return maintenanceCompletionRecord{}, fmt.Errorf("maintenance receipt for lease %q has mismatched ID key", leaseKey)
	}
	return record, nil
}

func listMaintenanceReceiptsTx(
	tx *bolt.Tx,
	leaseUUID string,
) ([]maintenanceCompletionRecord, error) {
	root := tx.Bucket(callbackMaintenanceHistoryBucketName)
	if root == nil {
		return nil, errors.New("completed maintenance history bucket missing")
	}
	leaseKey := []byte(leaseUUID)
	if root.Get(leaseKey) != nil {
		return nil, fmt.Errorf("completed maintenance history %q is not a nested bucket", leaseUUID)
	}
	leaseBucket := root.Bucket(leaseKey)
	if leaseBucket == nil {
		return nil, nil
	}
	var records []maintenanceCompletionRecord
	err := leaseBucket.ForEach(func(key, value []byte) error {
		if value == nil {
			return fmt.Errorf("completed maintenance history %q contains nested receipt %q", leaseUUID, key)
		}
		record, err := decodeMaintenanceCompletionRecord(leaseKey, key, value)
		if err != nil {
			return err
		}
		records = append(records, record)
		return nil
	})
	return records, err
}

func (s *CallbackStore) listFailedMaintenanceCompletionRecords() (
	[]maintenanceCompletionRecord,
	error,
) {
	var records []maintenanceCompletionRecord
	err := s.view(func(tx *bolt.Tx) error {
		root := tx.Bucket(callbackMaintenanceHistoryBucketName)
		if root == nil {
			return errors.New("completed maintenance history bucket missing")
		}
		return root.ForEach(func(leaseKey, value []byte) error {
			if value != nil {
				return fmt.Errorf("completed maintenance history %q is not a nested bucket", leaseKey)
			}
			leaseRecords, err := listMaintenanceReceiptsTx(tx, string(leaseKey))
			if err != nil {
				return err
			}
			for _, record := range leaseRecords {
				if record.Status == backend.CallbackStatusFailed && record.EffectStarted {
					records = append(records, record)
				}
			}
			return nil
		})
	})
	return records, err
}

// ListFailedMaintenanceReceipts returns only permanent failure records which
// prove the exact maintenance generation crossed the effect boundary. Each
// result is joined to its immutable target release under the journal pair's
// lease lock; a missing or divergent target fails closed instead of turning a
// receipt into caller-selected cleanup authority.
func (s *MaintenanceSettlement) ListFailedMaintenanceReceipts() (
	[]FailedMaintenanceReceipt,
	error,
) {
	if s == nil || s.callbacks == nil || s.releases == nil {
		return nil, errors.New("maintenance settlement is invalid")
	}
	records, err := s.callbacks.listFailedMaintenanceCompletionRecords()
	if err != nil {
		return nil, err
	}
	receipts := make([]FailedMaintenanceReceipt, 0, len(records))
	for _, snapshot := range records {
		unlock := s.lockLease(snapshot.LeaseUUID)
		var (
			current maintenanceCompletionRecord
			found   bool
			release Release
		)
		err = s.callbacks.view(func(tx *bolt.Tx) error {
			var readErr error
			current, found, readErr = findMaintenanceReceiptTx(
				tx, snapshot.LeaseUUID, snapshot.MaintenanceID,
			)
			return readErr
		})
		if err == nil && found && current == snapshot {
			err = s.releases.view(func(tx *bolt.Tx) error {
				history, readErr := readReleaseHistoryTx(tx, current.LeaseUUID)
				if readErr != nil {
					return readErr
				}
				for _, candidate := range history {
					if candidate.Version == current.TargetReleaseVersion &&
						candidate.MaintenanceID == current.MaintenanceID {
						release = cloneRelease(candidate)
						return nil
					}
				}
				return errors.New("failed maintenance receipt target release is missing")
			})
		}
		unlock()
		if err != nil {
			return nil, err
		}
		if !found || current != snapshot {
			continue
		}
		wantDigest, err := parseMaintenanceDigest(current.TargetReleaseDigest, false)
		if err != nil {
			return nil, err
		}
		actualDigest, err := maintenanceReleaseDigest(release)
		if err != nil {
			return nil, err
		}
		if actualDigest != wantDigest {
			return nil, errors.New("failed maintenance receipt target release is divergent")
		}
		identity, ok := releaseRuntimeIdentityFor(release)
		if !ok || identity.Tenant() != current.Tenant ||
			identity.ProviderUUID() != current.ProviderUUID {
			return nil, errors.New("failed maintenance receipt target authority is divergent")
		}
		storageID, err := backendidentity.Parse(current.BackendStorageID)
		if err != nil {
			return nil, err
		}
		receipt := FailedMaintenanceReceipt{
			settlement: s, callbacks: s.callbacks, releases: s.releases,
			record: current, storageID: storageID,
			target: ReleaseClaim{
				issuer: s.releases, leaseUUID: current.LeaseUUID,
				version: current.TargetReleaseVersion, digest: wantDigest,
			},
			release: release,
		}
		if !receipt.Valid() {
			return nil, errors.New("failed maintenance receipt could not be sealed")
		}
		receipts = append(receipts, receipt)
	}
	return receipts, nil
}

func reserveMaintenanceReceiptTx(tx *bolt.Tx, entry maintenanceIntentEntry) error {
	return reserveMaintenanceReceiptWithinLimitsTx(
		tx, entry, maxMaintenanceReceiptsPerLease, maxCallbackReceiptReservationsGlobal,
	)
}

func reserveMaintenanceReceiptWithinLimitsTx(
	tx *bolt.Tx,
	entry maintenanceIntentEntry,
	perLeaseLimit int,
	globalLimit uint64,
) error {
	records, err := listMaintenanceReceiptsTx(tx, entry.LeaseUUID)
	if err != nil {
		return err
	}
	for _, record := range records {
		if !maintenanceReceiptMatchesEntryAuthority(record, entry) {
			return fmt.Errorf(
				"maintenance history for lease %q crosses backend storage or principal authority",
				entry.LeaseUUID,
			)
		}
	}
	if len(records) >= perLeaseLimit {
		return &MaintenanceReceiptCapacityError{
			LeaseUUID: entry.LeaseUUID,
			Limit:     uint64(perLeaseLimit),
		}
	}
	reserved, err := reserveCallbackReceiptReservationWithinLimitTx(tx, globalLimit)
	if err != nil {
		return err
	}
	if !reserved {
		return &MaintenanceReceiptCapacityError{Limit: globalLimit}
	}
	return nil
}

func maintenanceReceiptMatchesEntryAuthority(
	record maintenanceCompletionRecord,
	entry maintenanceIntentEntry,
) bool {
	identity, ok := releaseRuntimeIdentityFor(entry.TargetRelease)
	return ok && record.Backend == entry.Backend &&
		record.BackendStorageID == entry.BackendStorageID &&
		record.Tenant == identity.Tenant() &&
		record.ProviderUUID == identity.ProviderUUID()
}

func archiveMaintenanceCompletionTx(tx *bolt.Tx, record maintenanceCompletionRecord) error {
	data, err := marshalMaintenanceCompletionRecord(record)
	if err != nil {
		return err
	}
	root := tx.Bucket(callbackMaintenanceHistoryBucketName)
	if root == nil {
		return errors.New("completed maintenance history bucket missing")
	}
	leaseKey := []byte(record.LeaseUUID)
	if root.Get(leaseKey) != nil {
		return fmt.Errorf("completed maintenance history %q is not a nested bucket", record.LeaseUUID)
	}
	leaseBucket, err := root.CreateBucketIfNotExists(leaseKey)
	if err != nil {
		return err
	}
	key := []byte(record.MaintenanceID.String())
	if leaseBucket.Bucket(key) != nil {
		return fmt.Errorf("maintenance receipt %q is a nested bucket", record.MaintenanceID)
	}
	if current := leaseBucket.Get(key); current != nil {
		existing, err := decodeMaintenanceCompletionRecord(leaseKey, key, current)
		if err != nil {
			return err
		}
		if existing != record {
			return fmt.Errorf("maintenance receipt %q has divergent terminal authority", record.MaintenanceID)
		}
		return nil
	}
	records, err := listMaintenanceReceiptsTx(tx, record.LeaseUUID)
	if err != nil {
		return err
	}
	for _, existing := range records {
		if existing.CompletionSequence >= record.CompletionSequence {
			return fmt.Errorf(
				"maintenance receipt %q does not advance completion sequence after %d",
				record.MaintenanceID, existing.CompletionSequence,
			)
		}
	}
	return leaseBucket.Put(key, data)
}

func findMaintenanceReceiptTx(
	tx *bolt.Tx,
	leaseUUID string,
	maintenanceID MaintenanceID,
) (maintenanceCompletionRecord, bool, error) {
	records, err := listMaintenanceReceiptsTx(tx, leaseUUID)
	if err != nil {
		return maintenanceCompletionRecord{}, false, err
	}
	for _, record := range records {
		if record.MaintenanceID != maintenanceID {
			continue
		}
		return record, true, nil
	}
	return maintenanceCompletionRecord{}, false, nil
}

func classifyMaintenanceReplayTx(
	tx *bolt.Tx,
	leaseUUID string,
	maintenanceID MaintenanceID,
	requestDigest string,
) (MaintenanceIntentAdmissionDisposition, error) {
	head, present, err := getLeaseMutationHeadTx(tx, leaseUUID)
	if err != nil {
		return MaintenanceIntentAdmissionNone, err
	}
	if present {
		if maintenance, ok := head.(maintenanceLeaseMutationHead); ok &&
			maintenance.claim.MaintenanceID() == maintenanceID {
			if maintenance.claim.entry.RequestDigest != requestDigest {
				return MaintenanceIntentAdmissionNone, fmt.Errorf(
					"%w for lease %q: maintenance ID has divergent request authority",
					ErrMaintenanceIntentConflict, leaseUUID,
				)
			}
			return MaintenanceIntentAdmissionExisting, nil
		}
	}
	receipt, found, err := findMaintenanceReceiptTx(tx, leaseUUID, maintenanceID)
	if err != nil {
		return MaintenanceIntentAdmissionNone, err
	}
	if found {
		if receipt.RequestDigest != requestDigest {
			return MaintenanceIntentAdmissionNone, fmt.Errorf(
				"%w for lease %q: completed maintenance ID has divergent request authority",
				ErrMaintenanceIntentConflict, leaseUUID,
			)
		}
		superseded, err := maintenanceReceiptSupersededTx(tx, receipt, head, present)
		if err != nil {
			return MaintenanceIntentAdmissionNone, err
		}
		if superseded {
			return MaintenanceIntentAdmissionCompletedSuperseded, nil
		}
		return MaintenanceIntentAdmissionCompleted, nil
	}
	if present {
		switch state := head.(type) {
		case maintenanceLeaseMutationHead:
			return MaintenanceIntentAdmissionNone, fmt.Errorf(
				"%w for lease %q: maintenance %q is already admitted",
				ErrMaintenanceIntentConflict, leaseUUID, state.claim.MaintenanceID(),
			)
		case closeLeaseMutationHead:
			return MaintenanceIntentAdmissionNone, fmt.Errorf(
				"%w for lease %q: close is already admitted",
				ErrMaintenanceIntentConflict, leaseUUID,
			)
		case closedLeaseMutationHead:
			return MaintenanceIntentAdmissionNone, fmt.Errorf(
				"%w for lease %q: lease is permanently closed",
				ErrMaintenanceIntentConflict, leaseUUID,
			)
		case operationLeaseMutationHead:
			if state.claim.entry.State == operationIntentPending {
				return MaintenanceIntentAdmissionNone, fmt.Errorf(
					"%w for lease %q: operation is already admitted",
					ErrMaintenanceIntentConflict, leaseUUID,
				)
			}
		}
	}
	return MaintenanceIntentAdmissionNone, nil
}

// maintenanceReceiptSupersededTx decides whether replaying a completed update
// could move the provider's desired payload backward. Callback sequence is a
// store-assigned monotonic fact committed in the same transaction as receipt
// creation; unlike UUIDv4 or wall time, it provides a trustworthy ordering.
// Restarts never change desired payload and therefore remain ordinary exact
// completions even when a later update exists.
func maintenanceReceiptSupersededTx(
	tx *bolt.Tx,
	receipt maintenanceCompletionRecord,
	head leaseMutationHead,
	headPresent bool,
) (bool, error) {
	if receipt.Kind != MaintenanceIntentUpdate {
		return false, nil
	}
	if headPresent {
		if maintenance, ok := head.(maintenanceLeaseMutationHead); ok &&
			maintenance.claim.MaintenanceID() != receipt.MaintenanceID &&
			maintenance.claim.Kind() == MaintenanceIntentUpdate {
			// Only one mutation head may exist. It was published after every
			// archived receipt, so this update is necessarily newer even before
			// its target release has been bound.
			return true, nil
		}
	}
	records, err := listMaintenanceReceiptsTx(tx, receipt.LeaseUUID)
	if err != nil {
		return false, err
	}
	for _, candidate := range records {
		if candidate.MaintenanceID == receipt.MaintenanceID ||
			candidate.Kind != MaintenanceIntentUpdate {
			continue
		}
		if candidate.CompletionSequence > receipt.CompletionSequence {
			return true, nil
		}
	}
	return false, nil
}

func releaseClosedLeaseMaintenanceReceiptsTx(tx *bolt.Tx, leaseUUID string) error {
	root := tx.Bucket(callbackMaintenanceHistoryBucketName)
	if root == nil {
		return errors.New("completed maintenance history bucket missing")
	}
	key := []byte(leaseUUID)
	if root.Bucket(key) == nil {
		return nil
	}
	return root.DeleteBucket(key)
}

func validateMaintenanceHistoryTx(tx *bolt.Tx) (uint64, error) {
	root := tx.Bucket(callbackMaintenanceHistoryBucketName)
	if root == nil {
		return 0, errors.New("completed maintenance history bucket missing")
	}
	var completedCount uint64
	if err := root.ForEach(func(leaseKey, value []byte) error {
		if value != nil {
			return fmt.Errorf("completed maintenance history %q is not a nested bucket", leaseKey)
		}
		if err := validateCanonicalLeaseUUID(string(leaseKey)); err != nil {
			return err
		}
		records, err := listMaintenanceReceiptsTx(tx, string(leaseKey))
		if err != nil {
			return err
		}
		if len(records) > maxMaintenanceReceiptsPerLease {
			return fmt.Errorf("maintenance receipt capacity exceeded for lease %q", leaseKey)
		}
		completedCount += uint64(len(records))
		sequences := make(map[uint64]struct{}, len(records))
		for index := range records {
			if _, duplicate := sequences[records[index].CompletionSequence]; duplicate {
				return fmt.Errorf(
					"maintenance history for lease %q repeats completion sequence %d",
					leaseKey, records[index].CompletionSequence,
				)
			}
			sequences[records[index].CompletionSequence] = struct{}{}
			if index > 0 &&
				(records[index].Backend != records[0].Backend ||
					records[index].BackendStorageID != records[0].BackendStorageID ||
					records[index].Tenant != records[0].Tenant ||
					records[index].ProviderUUID != records[0].ProviderUUID) {
				return fmt.Errorf(
					"maintenance history for lease %q crosses backend storage or principal authority",
					leaseKey,
				)
			}
		}
		return nil
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
		maintenance, ok := head.(maintenanceLeaseMutationHead)
		if !ok {
			return nil
		}
		pendingReservations++
		records, err := listMaintenanceReceiptsTx(tx, string(leaseKey))
		if err != nil {
			return err
		}
		if len(records) >= maxMaintenanceReceiptsPerLease {
			return fmt.Errorf("maintenance head for lease %q has no reserved receipt capacity", leaseKey)
		}
		for _, record := range records {
			if !maintenanceReceiptMatchesEntryAuthority(record, maintenance.claim.entry) {
				return fmt.Errorf(
					"maintenance head for lease %q crosses completed-history authority",
					leaseKey,
				)
			}
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return completedCount + pendingReservations, nil
}
