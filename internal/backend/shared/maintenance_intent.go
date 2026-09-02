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
)

const maxMaintenanceIntentEntryBytes = 4 << 20

// MaintenanceID is the exact replacement-generation identity shared by the
// write-ahead intent, target release, and every replacement container.
type MaintenanceID string

func (id MaintenanceID) String() string { return string(id) }

func (id MaintenanceID) Valid() bool {
	parsed, err := uuid.Parse(string(id))
	return err == nil && parsed.Version() == uuid.Version(4) &&
		parsed.Variant() == uuid.RFC4122 && parsed.String() == string(id)
}

// MaintenanceIntentKind identifies the replacement command being journaled.
type MaintenanceIntentKind string

const (
	MaintenanceIntentRestart      MaintenanceIntentKind = "restart"
	MaintenanceIntentUpdate       MaintenanceIntentKind = "update"
	MaintenanceIntentCustomDomain MaintenanceIntentKind = "custom_domain"
)

var ErrMaintenanceIntentConflict = errors.New("unresolved callback maintenance intent")

// MaintenanceIntentSpec is the immutable admission input. TargetRelease must
// be a version-zero deploying template without a MaintenanceID; Begin allocates
// the UUIDv4 and returns the only durable capability carrying the completed
// target.
type MaintenanceIntentSpec struct {
	Kind             MaintenanceIntentKind
	SourceRelease    ReleaseClaim
	TargetRelease    Release
	Backend          string
	BackendStorageID backendidentity.ID
}

// MaintenanceIntentClaim is an opaque precise recovery and settlement
// capability for one journal row. It deliberately cannot append a Release or
// cancel admission: those phase-specific authorities have distinct types.
type MaintenanceIntentClaim struct {
	entry         maintenanceIntentEntry
	maintenanceID MaintenanceID
	storageID     backendidentity.ID
	sourceDigest  [sha256.Size]byte
	targetDigest  [sha256.Size]byte
	digest        [sha256.Size]byte
}

// MaintenanceIntentAdmission is the cancelable pre-append phase returned by
// BeginMaintenanceIntent. StartMaintenanceAppend consumes this exact snapshot;
// after that durable phase transition, every copy of the admission is stale and
// CancelMaintenanceIntent fails its compare-and-swap check.
type MaintenanceIntentAdmission struct {
	intent MaintenanceIntentClaim
}

func (a MaintenanceIntentAdmission) Valid() bool {
	return validateMaintenanceIntentAdmission(a) == nil
}
func (a MaintenanceIntentAdmission) MaintenanceID() MaintenanceID {
	return a.intent.MaintenanceID()
}
func (a MaintenanceIntentAdmission) LeaseUUID() string { return a.intent.LeaseUUID() }
func (a MaintenanceIntentAdmission) TargetRelease() Release {
	return a.intent.TargetRelease()
}

// MaintenanceAppendClaim is the only authority ReleaseStore accepts for a new
// maintenance generation. It can be constructed only after the callback WAL
// durably records that cancellation is no longer legal.
type MaintenanceAppendClaim struct {
	intent MaintenanceIntentClaim
}

func (c MaintenanceAppendClaim) Valid() bool {
	return validateMaintenanceAppendClaim(c) == nil
}
func (c MaintenanceAppendClaim) Intent() MaintenanceIntentClaim { return c.intent }

func (c MaintenanceIntentClaim) Valid() bool {
	return validateMaintenanceIntentClaim(c) == nil
}
func (c MaintenanceIntentClaim) MaintenanceID() MaintenanceID         { return c.maintenanceID }
func (c MaintenanceIntentClaim) Kind() MaintenanceIntentKind          { return c.entry.Kind }
func (c MaintenanceIntentClaim) LeaseUUID() string                    { return c.entry.LeaseUUID }
func (c MaintenanceIntentClaim) Backend() string                      { return c.entry.Backend }
func (c MaintenanceIntentClaim) BackendStorageID() backendidentity.ID { return c.storageID }
func (c MaintenanceIntentClaim) CreatedAt() time.Time                 { return c.entry.CreatedAt }
func (c MaintenanceIntentClaim) SourceRelease() ReleaseClaim {
	return ReleaseClaim{
		leaseUUID: c.entry.LeaseUUID,
		version:   c.entry.SourceReleaseVersion,
		digest:    c.sourceDigest,
	}
}
func (c MaintenanceIntentClaim) TargetRelease() Release {
	return cloneRelease(c.entry.TargetRelease)
}
func (c MaintenanceIntentClaim) TargetReleaseClaim() (MaintenanceReleaseClaim, bool) {
	if c.entry.TargetReleaseVersion == 0 {
		return MaintenanceReleaseClaim{}, false
	}
	return MaintenanceReleaseClaim{
		releaseClaim: ReleaseClaim{
			leaseUUID: c.entry.LeaseUUID,
			version:   c.entry.TargetReleaseVersion,
			digest:    c.targetDigest,
		},
		maintenanceID:   c.maintenanceID,
		immutableDigest: c.targetDigest,
	}, true
}
func (c MaintenanceIntentClaim) Tenant() string {
	if c.entry.TargetRelease.RuntimeAuthority == nil {
		return ""
	}
	return c.entry.TargetRelease.RuntimeAuthority.Tenant()
}
func (c MaintenanceIntentClaim) ProviderUUID() string {
	if c.entry.TargetRelease.RuntimeAuthority == nil {
		return ""
	}
	return c.entry.TargetRelease.RuntimeAuthority.ProviderUUID()
}
func (c MaintenanceIntentClaim) CallbackURL() string {
	if c.entry.TargetRelease.RuntimeAuthority == nil {
		return ""
	}
	return c.entry.TargetRelease.RuntimeAuthority.CallbackURL()
}
func (c MaintenanceIntentClaim) LifecycleCallbackURL() string {
	if c.entry.TargetRelease.RuntimeAuthority == nil {
		return ""
	}
	return c.entry.TargetRelease.RuntimeAuthority.LifecycleCallbackURL()
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
	CreatedAt            time.Time             `json:"created_at"`
}

// BeginMaintenanceIntent publishes the durable barrier before a target release
// or replacement container can exist.
func (s *CallbackStore) BeginMaintenanceIntent(
	spec MaintenanceIntentSpec,
) (MaintenanceIntentAdmission, error) {
	if err := validateMaintenanceIntentSpec(spec); err != nil {
		return MaintenanceIntentAdmission{}, err
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return MaintenanceIntentAdmission{}, fmt.Errorf("allocate maintenance ID: %w", err)
	}
	target := cloneRelease(spec.TargetRelease)
	target.MaintenanceID = MaintenanceID(id.String())
	if err := validateMaintenanceAppendInput(spec.SourceRelease, target); err != nil {
		return MaintenanceIntentAdmission{}, err
	}
	entry := maintenanceIntentEntry{
		MaintenanceID:        MaintenanceID(id.String()),
		Kind:                 spec.Kind,
		LeaseUUID:            spec.SourceRelease.LeaseUUID(),
		Backend:              spec.Backend,
		BackendStorageID:     spec.BackendStorageID.String(),
		SourceReleaseVersion: spec.SourceRelease.Version(),
		SourceReleaseDigest:  encodeMaintenanceDigest(spec.SourceRelease.Digest()),
		TargetRelease:        target,
		CreatedAt:            time.Now(),
	}
	data, err := marshalMaintenanceIntent(entry)
	if err != nil {
		return MaintenanceIntentAdmission{}, err
	}

	unlock := s.lockDeliveryLease(entry.LeaseUUID)
	defer unlock()
	err = s.update(func(tx *bolt.Tx) error {
		if err := rejectMaintenanceOverlapTx(tx, entry.LeaseUUID); err != nil {
			return err
		}
		if err := rejectPendingMaintenanceCompletionTx(tx, entry.LeaseUUID); err != nil {
			return err
		}
		bucket := tx.Bucket(callbackMaintenanceIntentBucketName)
		if bucket == nil {
			return errors.New("callback maintenance intent bucket missing")
		}
		key := []byte(entry.LeaseUUID)
		if bucket.Bucket(key) != nil {
			return fmt.Errorf("callback maintenance intent %q is a nested bucket", entry.LeaseUUID)
		}
		if bucket.Get(key) != nil {
			return fmt.Errorf("%w for lease %q", ErrMaintenanceIntentConflict, entry.LeaseUUID)
		}
		return bucket.Put(key, data)
	})
	if err != nil {
		return MaintenanceIntentAdmission{}, err
	}
	claim, err := decodeMaintenanceIntent([]byte(entry.LeaseUUID), data)
	if err != nil {
		return MaintenanceIntentAdmission{}, err
	}
	return MaintenanceIntentAdmission{intent: claim}, nil
}

// StartMaintenanceAppend irreversibly advances a cancelable admission to the
// append-started phase before ReleaseStore can create a target generation. A
// crash after this transition but before the release append is classified as
// an interrupted failure by recovery; cancellation authority is never
// recreated.
func (s *CallbackStore) StartMaintenanceAppend(
	admission MaintenanceIntentAdmission,
) (MaintenanceAppendClaim, error) {
	if err := validateMaintenanceIntentAdmission(admission); err != nil {
		return MaintenanceAppendClaim{}, err
	}
	claim := admission.intent

	unlock := s.lockDeliveryLease(claim.LeaseUUID())
	defer unlock()
	var started MaintenanceIntentClaim
	err := s.update(func(tx *bolt.Tx) error {
		if err := verifyMaintenanceIntentTx(tx, claim); err != nil {
			return err
		}
		entry := cloneMaintenanceIntentEntry(claim.entry)
		entry.AppendStarted = true
		data, err := marshalMaintenanceIntent(entry)
		if err != nil {
			return err
		}
		if err := tx.Bucket(callbackMaintenanceIntentBucketName).Put(
			[]byte(entry.LeaseUUID), data,
		); err != nil {
			return err
		}
		started, err = decodeMaintenanceIntent([]byte(entry.LeaseUUID), data)
		return err
	})
	if err != nil {
		return MaintenanceAppendClaim{}, err
	}
	appendClaim := MaintenanceAppendClaim{intent: started}
	if err := validateMaintenanceAppendClaim(appendClaim); err != nil {
		return MaintenanceAppendClaim{}, err
	}
	return appendClaim, nil
}

// BindMaintenanceIntentTarget records the exact store-assigned target version
// and immutable release digest after AppendMaintenance. A crash before this
// bind is recoverable by searching the release history for MaintenanceID.
func (s *CallbackStore) BindMaintenanceIntentTarget(
	claim MaintenanceIntentClaim,
	target MaintenanceReleaseClaim,
) (MaintenanceIntentClaim, error) {
	if err := validateMaintenanceIntentTargetBinding(claim, target); err != nil {
		return MaintenanceIntentClaim{}, err
	}

	unlock := s.lockDeliveryLease(claim.LeaseUUID())
	defer unlock()
	return s.bindMaintenanceIntentTargetLocked(claim, target)
}

// TryBindMaintenanceIntentTarget is the recovery-safe form of
// BindMaintenanceIntentTarget. It never waits behind another journal mutation
// for this lease; acquired=false leaves the exact unbound intent untouched for
// the next level-triggered sweep. Callback HTTP owns a separate drain lock and
// cannot delay this mutation. Live admission uses the blocking form because it
// does not hold the fleet-wide recovery mutex.
func (s *CallbackStore) TryBindMaintenanceIntentTarget(
	claim MaintenanceIntentClaim,
	target MaintenanceReleaseClaim,
) (refreshed MaintenanceIntentClaim, acquired bool, err error) {
	if err := validateMaintenanceIntentTargetBinding(claim, target); err != nil {
		return MaintenanceIntentClaim{}, false, err
	}
	unlock, acquired := s.tryLockDeliveryLease(claim.LeaseUUID())
	if !acquired {
		return MaintenanceIntentClaim{}, false, nil
	}
	defer unlock()
	refreshed, err = s.bindMaintenanceIntentTargetLocked(claim, target)
	return refreshed, true, err
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
			return errors.New("maintenance intent target is already bound")
		}
		entry.TargetReleaseVersion = target.Version()
		entry.TargetReleaseDigest = encodeMaintenanceDigest(target.Digest())
		data, err := marshalMaintenanceIntent(entry)
		if err != nil {
			return err
		}
		if err := tx.Bucket(callbackMaintenanceIntentBucketName).Put(
			[]byte(entry.LeaseUUID), data,
		); err != nil {
			return err
		}
		refreshed, err = decodeMaintenanceIntent([]byte(entry.LeaseUUID), data)
		return err
	})
	return refreshed, err
}

// CancelMaintenanceIntent removes only the exact pre-append admission when no
// target release or substrate mutation was accepted. StartMaintenanceAppend
// rewrites the row, so every copied admission becomes a stale CAS capability.
func (s *CallbackStore) CancelMaintenanceIntent(admission MaintenanceIntentAdmission) error {
	if err := validateMaintenanceIntentAdmission(admission); err != nil {
		return err
	}
	claim := admission.intent
	unlock := s.lockDeliveryLease(claim.LeaseUUID())
	defer unlock()
	return s.update(func(tx *bolt.Tx) error {
		if err := verifyMaintenanceIntentTx(tx, claim); err != nil {
			return err
		}
		return tx.Bucket(callbackMaintenanceIntentBucketName).Delete([]byte(claim.LeaseUUID()))
	})
}

func (s *CallbackStore) GetMaintenanceIntent(
	leaseUUID string,
) (MaintenanceIntentClaim, bool, error) {
	if err := validateCanonicalLeaseUUID(leaseUUID); err != nil {
		return MaintenanceIntentClaim{}, false, err
	}
	var claim MaintenanceIntentClaim
	var found bool
	err := s.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackMaintenanceIntentBucketName)
		if bucket == nil {
			return errors.New("callback maintenance intent bucket missing")
		}
		key := []byte(leaseUUID)
		if bucket.Bucket(key) != nil {
			return fmt.Errorf("callback maintenance intent %q is a nested bucket", leaseUUID)
		}
		value := bucket.Get(key)
		if value == nil {
			return nil
		}
		var err error
		claim, err = decodeMaintenanceIntent(key, value)
		found = err == nil
		return err
	})
	return claim, found, err
}

func (s *CallbackStore) ListMaintenanceIntents() ([]MaintenanceIntentClaim, error) {
	var claims []MaintenanceIntentClaim
	err := s.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackMaintenanceIntentBucketName)
		if bucket == nil {
			return errors.New("callback maintenance intent bucket missing")
		}
		return bucket.ForEach(func(key, value []byte) error {
			if value == nil {
				return fmt.Errorf("callback maintenance intent %q is a nested bucket", key)
			}
			claim, err := decodeMaintenanceIntent(key, value)
			if err != nil {
				return err
			}
			claims = append(claims, claim)
			return nil
		})
	})
	return claims, err
}

// ResolveMaintenanceIntent atomically replaces one exact intent with its
// lifecycle completion. Success requires a bound target; failure may settle a
// pre-append intent during recovery or close preemption.
func (s *CallbackStore) ResolveMaintenanceIntent(
	claim MaintenanceIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
) (CallbackEntry, error) {
	entry, err := prepareMaintenanceIntentCompletion(claim, status, errMsg)
	if err != nil {
		return CallbackEntry{}, err
	}

	unlock := s.lockDeliveryLease(claim.LeaseUUID())
	defer unlock()
	return s.resolveMaintenanceIntentLocked(claim, entry)
}

// TryResolveMaintenanceIntent is the recovery-safe form of
// ResolveMaintenanceIntent. It never waits behind another journal mutation for
// this lease; acquired=false leaves the exact intent untouched for the next
// level-triggered sweep. Callback HTTP owns a separate drain lock and cannot
// hold Docker's fleet-wide recovery mutex or per-lease command fence.
func (s *CallbackStore) TryResolveMaintenanceIntent(
	claim MaintenanceIntentClaim,
	status backend.CallbackStatus,
	errMsg string,
) (entry CallbackEntry, acquired bool, err error) {
	if err := validateMaintenanceIntentClaim(claim); err != nil {
		return CallbackEntry{}, false, err
	}
	if status != backend.CallbackStatusSuccess && status != backend.CallbackStatusFailed {
		return CallbackEntry{}, false, fmt.Errorf("maintenance intent has invalid completion status %q", status)
	}
	if status == backend.CallbackStatusSuccess && claim.entry.TargetReleaseVersion == 0 {
		return CallbackEntry{}, false, errors.New("unbound maintenance intent cannot resolve success")
	}
	unlock, acquired := s.tryLockDeliveryLease(claim.LeaseUUID())
	if !acquired {
		return CallbackEntry{}, false, nil
	}
	defer unlock()
	entry, err = prepareMaintenanceIntentCompletion(claim, status, errMsg)
	if err != nil {
		return CallbackEntry{}, true, err
	}
	entry, err = s.resolveMaintenanceIntentLocked(claim, entry)
	return entry, true, err
}

// TryResolveMaintenanceIntentWithRuntimeFailure is the recovery-safe terminal
// transition for a maintenance generation whose Release committed active but
// whose exact runtime cohort is already definitively lost. It atomically
// preserves both facts, in causal order, before consuming the intent:
//
//  1. the requested maintenance operation succeeded durably; and
//  2. the resulting runtime subsequently failed.
//
// acquired=false means another journal mutation currently owns this lease. No
// row or intent is changed in that case, so the caller must retry the whole
// recovery classification rather than publishing ordinary inventory.
func (s *CallbackStore) TryResolveMaintenanceIntentWithRuntimeFailure(
	claim MaintenanceIntentClaim,
	errMsg string,
) (acquired bool, err error) {
	maintenance, runtimeFailure, err := prepareDivergedMaintenanceCompletions(claim, errMsg)
	if err != nil {
		return false, err
	}
	unlock, acquired := s.tryLockDeliveryLease(claim.LeaseUUID())
	if !acquired {
		return false, nil
	}
	defer unlock()
	_, err = s.resolveMaintenanceIntentEntriesLocked(
		claim, []CallbackEntry{maintenance, runtimeFailure},
	)
	return true, err
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
		Success:          false,
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
		return tx.Bucket(callbackMaintenanceIntentBucketName).Delete([]byte(claim.LeaseUUID()))
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
	s.notifyReplaySubscribers()
	return entries, nil
}

func callbackEntryForMaintenanceIntent(
	intent maintenanceIntentEntry,
	deliveryID string,
	status backend.CallbackStatus,
	errMsg string,
) CallbackEntry {
	return CallbackEntry{
		DeliveryID:       deliveryID,
		LeaseUUID:        intent.LeaseUUID,
		CallbackURL:      intent.TargetRelease.RuntimeAuthority.LifecycleCallbackURL(),
		DeliveryKind:     CallbackDeliveryKindMaintenance,
		Success:          status != backend.CallbackStatusFailed,
		Status:           status,
		Backend:          intent.Backend,
		BackendStorageID: intent.BackendStorageID,
		Error:            errMsg,
		CreatedAt:        time.Now(),
	}
}

func validateMaintenanceIntentSpec(spec MaintenanceIntentSpec) error {
	if !spec.SourceRelease.valid() {
		return errors.New("maintenance intent requires an exact source release claim")
	}
	if spec.Kind != MaintenanceIntentRestart && spec.Kind != MaintenanceIntentUpdate &&
		spec.Kind != MaintenanceIntentCustomDomain {
		return fmt.Errorf("invalid maintenance intent kind %q", spec.Kind)
	}
	if err := backendname.Validate(spec.Backend); err != nil {
		return fmt.Errorf("maintenance intent backend: %w", err)
	}
	if !spec.BackendStorageID.Valid() {
		return errors.New("maintenance intent requires a valid backend storage identity")
	}
	if spec.TargetRelease.MaintenanceID != "" {
		return errors.New("maintenance target ID is store-assigned")
	}
	if spec.TargetRelease.Version != 0 || spec.TargetRelease.Status != "deploying" {
		return errors.New("maintenance target must be a version-zero deploying template")
	}
	if !spec.TargetRelease.OperationID.Valid() || spec.TargetRelease.RuntimeAuthority == nil {
		return errors.New("maintenance target requires typed runtime authority")
	}
	if err := validateStoredCallbackCreatedAt(spec.TargetRelease.CreatedAt); err != nil {
		return fmt.Errorf("maintenance target: %w", err)
	}
	if err := validateNewCallbackCreatedAt(spec.TargetRelease.CreatedAt, time.Now()); err != nil {
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

func validateMaintenanceIntentAdmission(admission MaintenanceIntentAdmission) error {
	if err := validateMaintenanceIntentClaim(admission.intent); err != nil {
		return err
	}
	if admission.intent.entry.AppendStarted {
		return errors.New("maintenance intent admission is no longer cancelable")
	}
	return nil
}

func validateMaintenanceAppendClaim(claim MaintenanceAppendClaim) error {
	if err := validateMaintenanceIntentClaim(claim.intent); err != nil {
		return err
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
	if err := validateUniqueJSONObject(value, maxMaintenanceIntentEntryBytes); err != nil {
		return MaintenanceIntentClaim{}, fmt.Errorf("decode maintenance intent %q: %w", key, err)
	}
	var entry maintenanceIntentEntry
	if err := json.Unmarshal(value, &entry); err != nil {
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
	bucket := tx.Bucket(callbackMaintenanceIntentBucketName)
	if bucket == nil {
		return errors.New("callback maintenance intent bucket missing")
	}
	key := []byte(claim.LeaseUUID())
	if bucket.Bucket(key) != nil {
		return fmt.Errorf("callback maintenance intent %q is a nested bucket", claim.LeaseUUID())
	}
	current := bucket.Get(key)
	if current == nil {
		return fmt.Errorf("maintenance intent no longer exists for lease %q", claim.LeaseUUID())
	}
	if sha256.Sum256(current) != claim.digest {
		return errors.New("maintenance intent changed before precise mutation")
	}
	return nil
}

// rejectPendingMaintenanceCompletionTx prevents a newer replacement from
// overtaking the subscriber-visible result of an older one. Maintenance
// completions use the lease's stable lifecycle route, so the provider cannot
// distinguish generations from the wire payload. Requiring the older durable
// completion to receive a synchronous 2xx and be precisely removed before a
// new intent commits keeps accepted-start and terminal events causally ordered
// without changing the callback protocol.
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

func rejectMaintenanceOverlapTx(tx *bolt.Tx, leaseUUID string) error {
	key := []byte(leaseUUID)
	for _, journal := range []struct {
		name   string
		bucket []byte
	}{
		{name: "operation", bucket: callbackOperationIntentBucketName},
		{name: "close", bucket: callbackCloseIntentBucketName},
	} {
		bucket := tx.Bucket(journal.bucket)
		if bucket == nil {
			return fmt.Errorf("callback %s intent bucket missing", journal.name)
		}
		if bucket.Bucket(key) != nil {
			return fmt.Errorf("callback %s intent %q is a nested bucket", journal.name, leaseUUID)
		}
		if bucket.Get(key) != nil {
			return fmt.Errorf("%w for lease %q: %s is already admitted",
				ErrMaintenanceIntentConflict, leaseUUID, journal.name)
		}
	}
	return nil
}

func rejectOperationWhileMaintainingTx(tx *bolt.Tx, leaseUUID string) error {
	bucket := tx.Bucket(callbackMaintenanceIntentBucketName)
	if bucket == nil {
		return errors.New("callback maintenance intent bucket missing")
	}
	key := []byte(leaseUUID)
	if bucket.Bucket(key) != nil {
		return fmt.Errorf("callback maintenance intent %q is a nested bucket", leaseUUID)
	}
	if bucket.Get(key) != nil {
		return fmt.Errorf("%w for lease %q: maintenance is already admitted",
			ErrOperationIntentConflict, leaseUUID)
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
