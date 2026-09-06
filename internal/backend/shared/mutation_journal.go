package shared

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

const (
	maxLeaseMutationHeadBytes = maxOperationIntentEntryBytes + 4<<10
	// Use the repository's established authoritative-inspection scale as the
	// permanent identity ceiling. This is intentionally a safety guardrail, not
	// a target: each reserved UUID can also retain a bounded aggregate head, and
	// startup/health must remain able to validate the complete set.
	maxLeaseMutationUUIDSlotsGlobal = uint64(maxAuthoritativeInspectionRows)
	// Operation and maintenance admissions share one durable reservation
	// budget. A reservation is acquired before substrate work and becomes the
	// corresponding permanent receipt without another capacity decision.
	maxCallbackReceiptReservationsGlobal = uint64(maxAuthoritativeInspectionRows)
)

// callbackLeaseMutationHeadBucketName is the sole mutable authority for a
// lease's callback-producing work. A lease has exactly one tagged head, so an
// operation, maintenance replacement, close, and closed tombstone cannot be
// represented simultaneously.
var callbackLeaseMutationHeadBucketName = []byte("callback_lease_mutation_heads")

// callbackLeaseMutationUUIDSlotBucketName is the permanent cardinality fence
// for aggregate lease identities. The first operation, maintenance, or close
// admission reserves one UUID marker in the same bbolt transaction as its
// head. Markers are never deleted: a live lease may temporarily have no head,
// while a successfully closed UUID must retain its tombstone indefinitely.
// Reserving at first admission guarantees that completing a close never needs
// fresh capacity after teardown has begun.
var callbackLeaseMutationUUIDSlotBucketName = []byte("callback_lease_mutation_uuid_slots")

var leaseMutationUUIDSlotValue = []byte{1}

// ErrLeaseMutationCapacity is a definitive pre-side-effect refusal. The
// journal cannot safely accept another never-reusable lease UUID while keeping
// every closed-lease tombstone permanent.
var ErrLeaseMutationCapacity = errors.New("callback lease mutation UUID capacity exhausted")

// LeaseMutationCapacityError carries the global UUID bound and participates in
// the backend's existing coded capacity protocol.
type LeaseMutationCapacityError struct {
	Limit uint64
}

func (e *LeaseMutationCapacityError) Error() string {
	if e == nil {
		return ErrLeaseMutationCapacity.Error()
	}
	return fmt.Sprintf("global %s (limit %d)", ErrLeaseMutationCapacity, e.Limit)
}

func (e *LeaseMutationCapacityError) Unwrap() []error {
	return []error{
		ErrLeaseMutationCapacity,
		backend.ErrCapacityRefused,
		backend.ErrInsufficientResources,
	}
}

// LeaseMutationUUIDCapacity is a point-in-time view of the callback journal's
// permanent lease-identity budget. Reserved never decreases: maintenance and
// operation heads may settle, but their lease UUID remains reserved so a later
// successful close can install its non-expiring retirement fence without
// allocating after destructive work has begun.
type LeaseMutationUUIDCapacity struct {
	Reserved uint64
	Limit    uint64
}

// Remaining returns the number of never-before-seen lease UUIDs the journal
// can still admit. It saturates at zero so even a manually assembled diagnostic
// value cannot underflow.
func (capacity LeaseMutationUUIDCapacity) Remaining() uint64 {
	if capacity.Reserved >= capacity.Limit {
		return 0
	}
	return capacity.Limit - capacity.Reserved
}

// CallbackReceiptCapacity is a point-in-time view of the shared durable
// operation/maintenance receipt budget. Unlike the permanent lease-UUID
// budget, successful close can reclaim these reservations after installing
// the stronger lease-wide closed receipt.
type CallbackReceiptCapacity struct {
	Reserved uint64
	Limit    uint64
}

// Remaining returns the number of operation or maintenance receipts that can
// still be reserved. It saturates at zero for diagnostic values assembled
// outside the store.
func (capacity CallbackReceiptCapacity) Remaining() uint64 {
	if capacity.Reserved >= capacity.Limit {
		return 0
	}
	return capacity.Limit - capacity.Reserved
}

type leaseMutationHeadKind string

const (
	leaseMutationHeadOperation   leaseMutationHeadKind = "operation"
	leaseMutationHeadMaintenance leaseMutationHeadKind = "maintenance"
	leaseMutationHeadClose       leaseMutationHeadKind = "close"
	leaseMutationHeadClosed      leaseMutationHeadKind = "closed"
)

// leaseMutationHead is sealed to this package. Decoding produces exactly one
// of the private variants below; callers can never manufacture an overlapping
// aggregate state by combining independently decoded rows.
type leaseMutationHead interface {
	leaseUUID() string
	headDigest() [sha256.Size]byte
	headKind() leaseMutationHeadKind
	isLeaseMutationHead()
}

type operationLeaseMutationHead struct {
	claim OperationIntentClaim
}

func (h operationLeaseMutationHead) leaseUUID() string { return h.claim.LeaseUUID() }
func (h operationLeaseMutationHead) headDigest() [sha256.Size]byte {
	return h.claim.digest
}
func (operationLeaseMutationHead) headKind() leaseMutationHeadKind {
	return leaseMutationHeadOperation
}
func (operationLeaseMutationHead) isLeaseMutationHead() {}

type maintenanceLeaseMutationHead struct {
	claim MaintenanceIntentClaim
}

func (h maintenanceLeaseMutationHead) leaseUUID() string { return h.claim.LeaseUUID() }
func (h maintenanceLeaseMutationHead) headDigest() [sha256.Size]byte {
	return h.claim.digest
}
func (maintenanceLeaseMutationHead) headKind() leaseMutationHeadKind {
	return leaseMutationHeadMaintenance
}
func (maintenanceLeaseMutationHead) isLeaseMutationHead() {}

type closeLeaseMutationHead struct {
	claim CloseIntentClaim
}

func (h closeLeaseMutationHead) leaseUUID() string { return h.claim.LeaseUUID() }
func (h closeLeaseMutationHead) headDigest() [sha256.Size]byte {
	return h.claim.digest
}
func (closeLeaseMutationHead) headKind() leaseMutationHeadKind { return leaseMutationHeadClose }
func (closeLeaseMutationHead) isLeaseMutationHead()            {}

type closedLeaseMutationHead struct {
	entry  closedLeaseTombstone
	digest [sha256.Size]byte
}

func (h closedLeaseMutationHead) leaseUUID() string             { return h.entry.LeaseUUID }
func (h closedLeaseMutationHead) headDigest() [sha256.Size]byte { return h.digest }
func (closedLeaseMutationHead) headKind() leaseMutationHeadKind { return leaseMutationHeadClosed }
func (closedLeaseMutationHead) isLeaseMutationHead()            {}

// storedLeaseMutationHead is only a wire envelope. The pointer fields are
// deliberately private implementation details and validation requires exactly
// one field matching Kind. Runtime code works exclusively with the sealed
// variants above.
type storedLeaseMutationHead struct {
	Version     uint8                   `json:"version"`
	Kind        leaseMutationHeadKind   `json:"kind"`
	Operation   *operationIntentEntry   `json:"operation,omitempty"`
	Maintenance *maintenanceIntentEntry `json:"maintenance,omitempty"`
	Close       *closeIntentEntry       `json:"close,omitempty"`
	Closed      *closedLeaseTombstone   `json:"closed,omitempty"`
}

func getLeaseMutationHeadTx(
	tx *bolt.Tx,
	leaseUUID string,
) (leaseMutationHead, bool, error) {
	bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
	if bucket == nil {
		return nil, false, errors.New("callback lease mutation head bucket missing")
	}
	key := []byte(leaseUUID)
	if bucket.Bucket(key) != nil {
		return nil, false, fmt.Errorf("callback lease mutation head %q is a nested bucket", leaseUUID)
	}
	value := bucket.Get(key)
	if value == nil {
		return nil, false, nil
	}
	head, err := decodeLeaseMutationHead(key, value)
	return head, err == nil, err
}

func decodeLeaseMutationHead(key, value []byte) (leaseMutationHead, error) {
	var stored storedLeaseMutationHead
	if err := decodeStrictAuthoritativeObject(value, maxLeaseMutationHeadBytes, &stored); err != nil {
		return nil, fmt.Errorf("decode callback lease mutation head %q: %w", key, err)
	}
	if stored.Version != 1 {
		return nil, fmt.Errorf("callback lease mutation head %q has unsupported version %d", key, stored.Version)
	}
	present := 0
	for _, exists := range []bool{
		stored.Operation != nil,
		stored.Maintenance != nil,
		stored.Close != nil,
		stored.Closed != nil,
	} {
		if exists {
			present++
		}
	}
	if present != 1 {
		return nil, fmt.Errorf("callback lease mutation head %q must contain exactly one state", key)
	}
	digest := sha256.Sum256(value)
	switch stored.Kind {
	case leaseMutationHeadOperation:
		if stored.Operation == nil {
			return nil, fmt.Errorf("callback lease mutation head %q operation tag has different payload", key)
		}
		payload, err := json.Marshal(stored.Operation)
		if err != nil {
			return nil, fmt.Errorf("remarshal callback operation head %q: %w", key, err)
		}
		claim, err := decodeOperationIntent(key, payload)
		if err != nil {
			return nil, err
		}
		claim.digest = digest
		return operationLeaseMutationHead{claim: claim}, nil
	case leaseMutationHeadMaintenance:
		if stored.Maintenance == nil {
			return nil, fmt.Errorf("callback lease mutation head %q maintenance tag has different payload", key)
		}
		payload, err := json.Marshal(stored.Maintenance)
		if err != nil {
			return nil, fmt.Errorf("remarshal callback maintenance head %q: %w", key, err)
		}
		claim, err := decodeMaintenanceIntent(key, payload)
		if err != nil {
			return nil, err
		}
		claim.digest = digest
		return maintenanceLeaseMutationHead{claim: claim}, nil
	case leaseMutationHeadClose:
		if stored.Close == nil {
			return nil, fmt.Errorf("callback lease mutation head %q close tag has different payload", key)
		}
		payload, err := json.Marshal(stored.Close)
		if err != nil {
			return nil, fmt.Errorf("remarshal callback close head %q: %w", key, err)
		}
		claim, err := decodeCloseIntent(key, payload)
		if err != nil {
			return nil, err
		}
		claim.digest = digest
		return closeLeaseMutationHead{claim: claim}, nil
	case leaseMutationHeadClosed:
		if stored.Closed == nil {
			return nil, fmt.Errorf("callback lease mutation head %q closed tag has different payload", key)
		}
		payload, err := json.Marshal(stored.Closed)
		if err != nil {
			return nil, fmt.Errorf("remarshal callback closed head %q: %w", key, err)
		}
		entry, err := decodeClosedLeaseTombstone(key, payload)
		if err != nil {
			return nil, err
		}
		return closedLeaseMutationHead{entry: entry, digest: digest}, nil
	default:
		return nil, fmt.Errorf("callback lease mutation head %q has invalid kind %q", key, stored.Kind)
	}
}

func marshalLeaseMutationHead(head leaseMutationHead) ([]byte, error) {
	if head == nil {
		return nil, errors.New("callback lease mutation head is nil")
	}
	var stored storedLeaseMutationHead
	switch current := head.(type) {
	case operationLeaseMutationHead:
		if _, err := marshalOperationIntent(*current.claim.entry); err != nil {
			return nil, err
		}
		entry := *current.claim.entry
		stored = storedLeaseMutationHead{Version: 1, Kind: leaseMutationHeadOperation, Operation: &entry}
	case maintenanceLeaseMutationHead:
		if _, err := marshalMaintenanceIntent(current.claim.entry); err != nil {
			return nil, err
		}
		entry := cloneMaintenanceIntentEntry(current.claim.entry)
		stored = storedLeaseMutationHead{Version: 1, Kind: leaseMutationHeadMaintenance, Maintenance: &entry}
	case closeLeaseMutationHead:
		if _, err := marshalCloseIntent(current.claim.entry); err != nil {
			return nil, err
		}
		entry := cloneCloseIntentEntry(current.claim.entry)
		stored = storedLeaseMutationHead{Version: 1, Kind: leaseMutationHeadClose, Close: &entry}
	case closedLeaseMutationHead:
		if _, err := marshalClosedLeaseTombstone(current.entry); err != nil {
			return nil, err
		}
		entry := current.entry
		stored = storedLeaseMutationHead{Version: 1, Kind: leaseMutationHeadClosed, Closed: &entry}
	default:
		return nil, fmt.Errorf("unsupported callback lease mutation head %T", head)
	}
	data, err := json.Marshal(stored)
	if err != nil {
		return nil, fmt.Errorf("marshal callback lease mutation head: %w", err)
	}
	if len(data) > maxLeaseMutationHeadBytes {
		return nil, fmt.Errorf("callback lease mutation head exceeds %d bytes", maxLeaseMutationHeadBytes)
	}
	return data, nil
}

// leaseMutationTransition is a sealed sum type. Each valid source/target phase
// has a distinct representation, so a caller cannot express an action with a
// missing receipt, an unrelated target kind, or a nullable predecessor. The
// private constructors below also detach every claim before sealing it.
type leaseMutationTransition interface {
	isLeaseMutationTransition()
}

type publishOperationLeaseMutation struct {
	next OperationIntentClaim
}

type replaceOperationLeaseMutation struct {
	previous OperationIntentClaim
	next     OperationIntentClaim
}

type settleOperationLeaseMutation struct {
	previous OperationIntentClaim
	next     OperationIntentClaim
}

type startOperationExecutionLeaseMutation struct {
	previous OperationIntentClaim
	next     OperationIntentClaim
}

type publishMaintenanceLeaseMutation struct {
	next MaintenanceIntentClaim
}

type replaceOperationWithMaintenanceLeaseMutation struct {
	previous OperationIntentClaim
	next     MaintenanceIntentClaim
}

// replaceFailedOperationWithMaintenanceLeaseMutation is distinct from the
// ordinary terminal-operation path: the current Failed operation is a
// successor of the maintenance source rather than the source generation
// itself. The sealed relation is the only bridge between those identities.
type replaceFailedOperationWithMaintenanceLeaseMutation struct {
	previous    OperationIntentClaim
	predecessor failedOperationOverRelease
	next        MaintenanceIntentClaim
}

type startMaintenanceAppendLeaseMutation struct {
	previous MaintenanceIntentClaim
	next     MaintenanceIntentClaim
}

type bindMaintenanceTargetLeaseMutation struct {
	previous MaintenanceIntentClaim
	next     MaintenanceIntentClaim
}

type startMaintenanceExecutionLeaseMutation struct {
	previous MaintenanceIntentClaim
	next     MaintenanceIntentClaim
}

type cancelMaintenanceLeaseMutation struct {
	previous MaintenanceIntentClaim
}

type resolveMaintenanceLeaseMutation struct {
	previous MaintenanceIntentClaim
	receipt  maintenanceCompletionRecord
}

type publishCloseLeaseMutation struct {
	next CloseIntentClaim
}

type replaceOperationWithCloseLeaseMutation struct {
	previous OperationIntentClaim
	receipt  operationIntentEntry
	next     CloseIntentClaim
}

// replaceFailedOperationWithCloseLeaseMutation consumes one variant of the
// sealed Failed-successor relation. An exact active predecessor and explicit
// Release absence remain non-interchangeable capabilities; neither weakens the
// generic operation/close identity transition.
type replaceFailedOperationWithCloseLeaseMutation struct {
	previous  OperationIntentClaim
	authority failedOperationCloseAuthority
	receipt   operationIntentEntry
	next      CloseIntentClaim
}

type replaceMaintenanceWithCloseLeaseMutation struct {
	previous MaintenanceIntentClaim
	receipt  maintenanceCompletionRecord
	next     CloseIntentClaim
}

type advanceCloseLeaseMutation struct {
	previous CloseIntentClaim
	next     CloseIntentClaim
}

type completeCloseLeaseMutation struct {
	previous CloseIntentClaim
	next     closedLeaseMutationHead
}

func (publishOperationLeaseMutation) isLeaseMutationTransition()        {}
func (replaceOperationLeaseMutation) isLeaseMutationTransition()        {}
func (settleOperationLeaseMutation) isLeaseMutationTransition()         {}
func (startOperationExecutionLeaseMutation) isLeaseMutationTransition() {}
func (publishMaintenanceLeaseMutation) isLeaseMutationTransition()      {}
func (replaceOperationWithMaintenanceLeaseMutation) isLeaseMutationTransition() {
}
func (replaceFailedOperationWithMaintenanceLeaseMutation) isLeaseMutationTransition() {
}
func (startMaintenanceAppendLeaseMutation) isLeaseMutationTransition()    {}
func (bindMaintenanceTargetLeaseMutation) isLeaseMutationTransition()     {}
func (startMaintenanceExecutionLeaseMutation) isLeaseMutationTransition() {}
func (cancelMaintenanceLeaseMutation) isLeaseMutationTransition()         {}
func (resolveMaintenanceLeaseMutation) isLeaseMutationTransition()        {}
func (publishCloseLeaseMutation) isLeaseMutationTransition()              {}
func (replaceOperationWithCloseLeaseMutation) isLeaseMutationTransition() {}
func (replaceFailedOperationWithCloseLeaseMutation) isLeaseMutationTransition() {
}
func (replaceMaintenanceWithCloseLeaseMutation) isLeaseMutationTransition() {}
func (advanceCloseLeaseMutation) isLeaseMutationTransition()                {}
func (completeCloseLeaseMutation) isLeaseMutationTransition()               {}

func newPublishOperationLeaseMutation(
	next OperationIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(publishOperationLeaseMutation{
		next: cloneOperationMutationClaim(next),
	})
}

func newReplaceOperationLeaseMutation(
	previous, next OperationIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(replaceOperationLeaseMutation{
		previous: cloneOperationMutationClaim(previous),
		next:     cloneOperationMutationClaim(next),
	})
}

func newSettleOperationLeaseMutation(
	previous, next OperationIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(settleOperationLeaseMutation{
		previous: cloneOperationMutationClaim(previous),
		next:     cloneOperationMutationClaim(next),
	})
}

func newStartOperationExecutionLeaseMutation(
	previous, next OperationIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(startOperationExecutionLeaseMutation{
		previous: cloneOperationMutationClaim(previous),
		next:     cloneOperationMutationClaim(next),
	})
}

func newPublishMaintenanceLeaseMutation(
	next MaintenanceIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(publishMaintenanceLeaseMutation{
		next: cloneMaintenanceMutationClaim(next),
	})
}

func newReplaceOperationWithMaintenanceLeaseMutation(
	previous OperationIntentClaim,
	next MaintenanceIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(replaceOperationWithMaintenanceLeaseMutation{
		previous: cloneOperationMutationClaim(previous),
		next:     cloneMaintenanceMutationClaim(next),
	})
}

func newReplaceFailedOperationWithMaintenanceLeaseMutation(
	previous OperationIntentClaim,
	predecessor failedOperationOverRelease,
	next MaintenanceIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(replaceFailedOperationWithMaintenanceLeaseMutation{
		previous:    cloneOperationMutationClaim(previous),
		predecessor: predecessor,
		next:        cloneMaintenanceMutationClaim(next),
	})
}

func newStartMaintenanceAppendLeaseMutation(
	previous, next MaintenanceIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(startMaintenanceAppendLeaseMutation{
		previous: cloneMaintenanceMutationClaim(previous),
		next:     cloneMaintenanceMutationClaim(next),
	})
}

func newBindMaintenanceTargetLeaseMutation(
	previous, next MaintenanceIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(bindMaintenanceTargetLeaseMutation{
		previous: cloneMaintenanceMutationClaim(previous),
		next:     cloneMaintenanceMutationClaim(next),
	})
}

func newStartMaintenanceExecutionLeaseMutation(
	previous, next MaintenanceIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(startMaintenanceExecutionLeaseMutation{
		previous: cloneMaintenanceMutationClaim(previous),
		next:     cloneMaintenanceMutationClaim(next),
	})
}

func newCancelMaintenanceLeaseMutation(
	previous MaintenanceIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(cancelMaintenanceLeaseMutation{
		previous: cloneMaintenanceMutationClaim(previous),
	})
}

func newResolveMaintenanceLeaseMutation(
	previous MaintenanceIntentClaim,
	receipt maintenanceCompletionRecord,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(resolveMaintenanceLeaseMutation{
		previous: cloneMaintenanceMutationClaim(previous),
		receipt:  receipt,
	})
}

func newPublishCloseLeaseMutation(
	next CloseIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(publishCloseLeaseMutation{
		next: cloneCloseMutationClaim(next),
	})
}

func newReplaceOperationWithCloseLeaseMutation(
	previous OperationIntentClaim,
	receipt operationIntentEntry,
	next CloseIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(replaceOperationWithCloseLeaseMutation{
		previous: cloneOperationMutationClaim(previous),
		receipt:  cloneOperationMutationEntry(receipt),
		next:     cloneCloseMutationClaim(next),
	})
}

func newReplaceFailedOperationWithCloseLeaseMutation(
	previous OperationIntentClaim,
	authority failedOperationCloseAuthority,
	receipt operationIntentEntry,
	next CloseIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(replaceFailedOperationWithCloseLeaseMutation{
		previous:  cloneOperationMutationClaim(previous),
		authority: authority,
		receipt:   cloneOperationMutationEntry(receipt),
		next:      cloneCloseMutationClaim(next),
	})
}

func newReplaceMaintenanceWithCloseLeaseMutation(
	previous MaintenanceIntentClaim,
	receipt maintenanceCompletionRecord,
	next CloseIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(replaceMaintenanceWithCloseLeaseMutation{
		previous: cloneMaintenanceMutationClaim(previous),
		receipt:  receipt,
		next:     cloneCloseMutationClaim(next),
	})
}

func newAdvanceCloseLeaseMutation(
	previous, next CloseIntentClaim,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(advanceCloseLeaseMutation{
		previous: cloneCloseMutationClaim(previous),
		next:     cloneCloseMutationClaim(next),
	})
}

func newCompleteCloseLeaseMutation(
	previous CloseIntentClaim,
	next closedLeaseMutationHead,
) (leaseMutationTransition, error) {
	return sealLeaseMutationTransition(completeCloseLeaseMutation{
		previous: cloneCloseMutationClaim(previous),
		next:     next,
	})
}

func sealLeaseMutationTransition(
	transition leaseMutationTransition,
) (leaseMutationTransition, error) {
	current, present, err := declaredLeaseMutationCurrent(transition)
	if err != nil {
		return nil, err
	}
	if _, err := validateLeaseMutationTransition(current, present, transition); err != nil {
		return nil, err
	}
	return transition, nil
}

func cloneOperationMutationEntry(entry operationIntentEntry) operationIntentEntry {
	entry.Items = slices.Clone(entry.Items)
	entry.ResourceProfiles = CloneSKUResourceSnapshot(entry.ResourceProfiles)
	entry.EffectiveItems = slices.Clone(entry.EffectiveItems)
	entry.HealthCheckServices = slices.Clone(entry.HealthCheckServices)
	entry.Manifest = bytes.Clone(entry.Manifest)
	return entry
}

func cloneOperationMutationClaim(claim OperationIntentClaim) OperationIntentClaim {
	cloned := claim
	if claim.entry != nil {
		entry := cloneOperationMutationEntry(*claim.entry)
		cloned.entry = &entry
	}
	return cloned
}

func cloneMaintenanceMutationClaim(claim MaintenanceIntentClaim) MaintenanceIntentClaim {
	claim.entry = cloneMaintenanceIntentEntry(claim.entry)
	return claim
}

func cloneCloseMutationClaim(claim CloseIntentClaim) CloseIntentClaim {
	claim.entry = cloneCloseIntentEntry(claim.entry)
	return claim
}

func callbackReceiptReservationCountTx(tx *bolt.Tx) (uint64, error) {
	bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
	if bucket == nil {
		return 0, errors.New("callback lease mutation head bucket missing")
	}
	return bucket.Sequence(), nil
}

func callbackReceiptCapacityTx(tx *bolt.Tx) (CallbackReceiptCapacity, error) {
	reserved, err := callbackReceiptReservationCountTx(tx)
	if err != nil {
		return CallbackReceiptCapacity{}, err
	}
	if reserved > maxCallbackReceiptReservationsGlobal {
		return CallbackReceiptCapacity{}, fmt.Errorf(
			"global callback receipt capacity exceeded: %d > %d",
			reserved, maxCallbackReceiptReservationsGlobal,
		)
	}
	return CallbackReceiptCapacity{
		Reserved: reserved,
		Limit:    maxCallbackReceiptReservationsGlobal,
	}, nil
}

// CallbackReceiptCapacity returns the O(1) durable capacity counter shared by
// operation and maintenance receipt reservations. Healthy and stopped
// inspection additionally prove the counter equals the bounded history rows.
func (s *CallbackStore) CallbackReceiptCapacity() (CallbackReceiptCapacity, error) {
	if s == nil {
		return CallbackReceiptCapacity{}, errors.New("callback store is nil")
	}
	var capacity CallbackReceiptCapacity
	err := s.view(func(tx *bolt.Tx) error {
		var err error
		capacity, err = callbackReceiptCapacityTx(tx)
		return err
	})
	return capacity, err
}

func reserveCallbackReceiptReservationWithinLimitTx(
	tx *bolt.Tx,
	limit uint64,
) (bool, error) {
	bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
	if bucket == nil {
		return false, errors.New("callback lease mutation head bucket missing")
	}
	reserved := bucket.Sequence()
	if reserved >= limit {
		return false, nil
	}
	if err := bucket.SetSequence(reserved + 1); err != nil {
		return false, fmt.Errorf("advance callback receipt reservation count: %w", err)
	}
	return true, nil
}

func releaseCallbackReceiptReservationsTx(tx *bolt.Tx, released uint64) error {
	bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
	if bucket == nil {
		return errors.New("callback lease mutation head bucket missing")
	}
	reserved := bucket.Sequence()
	if reserved < released {
		return fmt.Errorf(
			"callback receipt reservation count underflow: stored=%d released=%d",
			reserved, released,
		)
	}
	if err := bucket.SetSequence(reserved - released); err != nil {
		return fmt.Errorf("release callback receipt reservations: %w", err)
	}
	return nil
}

func validateCallbackReceiptStateTx(tx *bolt.Tx) error {
	operationReservations, err := validateOperationHistoryTx(tx)
	if err != nil {
		return err
	}
	maintenanceReservations, err := validateMaintenanceHistoryTx(tx)
	if err != nil {
		return err
	}
	stored, err := callbackReceiptReservationCountTx(tx)
	if err != nil {
		return err
	}
	want := operationReservations + maintenanceReservations
	if stored != want {
		return fmt.Errorf(
			"callback receipt reservation count mismatch: stored=%d operation=%d maintenance=%d",
			stored, operationReservations, maintenanceReservations,
		)
	}
	if stored > maxCallbackReceiptReservationsGlobal {
		return fmt.Errorf(
			"global callback receipt capacity exceeded: %d > %d",
			stored, maxCallbackReceiptReservationsGlobal,
		)
	}
	return nil
}

func reserveLeaseMutationUUIDSlotTx(tx *bolt.Tx, leaseUUID string) error {
	if err := validateCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	bucket := tx.Bucket(callbackLeaseMutationUUIDSlotBucketName)
	if bucket == nil {
		return errors.New("callback lease mutation UUID slot bucket missing")
	}
	key := []byte(leaseUUID)
	if bucket.Bucket(key) != nil {
		return fmt.Errorf("callback lease mutation UUID slot %q is a nested bucket", leaseUUID)
	}
	if value := bucket.Get(key); value != nil {
		if !bytes.Equal(value, leaseMutationUUIDSlotValue) {
			return fmt.Errorf("callback lease mutation UUID slot %q has invalid marker", leaseUUID)
		}
		return nil
	}
	reserved := bucket.Sequence()
	if reserved >= maxLeaseMutationUUIDSlotsGlobal {
		return &LeaseMutationCapacityError{Limit: maxLeaseMutationUUIDSlotsGlobal}
	}
	if err := bucket.Put(key, leaseMutationUUIDSlotValue); err != nil {
		return fmt.Errorf("reserve callback lease mutation UUID slot %q: %w", leaseUUID, err)
	}
	if err := bucket.SetSequence(reserved + 1); err != nil {
		return fmt.Errorf("advance callback lease mutation UUID slot count: %w", err)
	}
	return nil
}

func leaseMutationUUIDCapacityTx(tx *bolt.Tx) (LeaseMutationUUIDCapacity, error) {
	bucket := tx.Bucket(callbackLeaseMutationUUIDSlotBucketName)
	if bucket == nil {
		return LeaseMutationUUIDCapacity{}, errors.New("callback lease mutation UUID slot bucket missing")
	}
	reserved := bucket.Sequence()
	if reserved > maxLeaseMutationUUIDSlotsGlobal {
		return LeaseMutationUUIDCapacity{}, fmt.Errorf(
			"callback lease mutation UUID capacity exceeded: %d > %d",
			reserved, maxLeaseMutationUUIDSlotsGlobal,
		)
	}
	return LeaseMutationUUIDCapacity{
		Reserved: reserved,
		Limit:    maxLeaseMutationUUIDSlotsGlobal,
	}, nil
}

// LeaseMutationUUIDCapacity returns the O(1) durable capacity counter used by
// runtime observability. Healthy and stopped inspection additionally walk the
// bounded marker set and prove this metadata equals its actual cardinality.
func (s *CallbackStore) LeaseMutationUUIDCapacity() (LeaseMutationUUIDCapacity, error) {
	if s == nil {
		return LeaseMutationUUIDCapacity{}, errors.New("callback store is nil")
	}
	var capacity LeaseMutationUUIDCapacity
	err := s.view(func(tx *bolt.Tx) error {
		var err error
		capacity, err = leaseMutationUUIDCapacityTx(tx)
		return err
	})
	return capacity, err
}

func validateLeaseMutationUUIDSlotsTx(tx *bolt.Tx) error {
	slots := tx.Bucket(callbackLeaseMutationUUIDSlotBucketName)
	if slots == nil {
		return errors.New("callback lease mutation UUID slot bucket missing")
	}
	var count uint64
	if err := slots.ForEach(func(key, value []byte) error {
		if value == nil {
			return fmt.Errorf("callback lease mutation UUID slot %q is a nested bucket", key)
		}
		if err := validateCanonicalLeaseUUID(string(key)); err != nil {
			return fmt.Errorf("callback lease mutation UUID slot has invalid key: %w", err)
		}
		if !bytes.Equal(value, leaseMutationUUIDSlotValue) {
			return fmt.Errorf("callback lease mutation UUID slot %q has invalid marker", key)
		}
		count++
		if count > maxLeaseMutationUUIDSlotsGlobal {
			return fmt.Errorf(
				"callback lease mutation UUID capacity exceeded: %d > %d",
				count, maxLeaseMutationUUIDSlotsGlobal,
			)
		}
		return nil
	}); err != nil {
		return err
	}
	if count != slots.Sequence() {
		return fmt.Errorf(
			"callback lease mutation UUID slot count mismatch: stored=%d actual=%d",
			slots.Sequence(), count,
		)
	}
	heads := tx.Bucket(callbackLeaseMutationHeadBucketName)
	if heads == nil {
		return errors.New("callback lease mutation head bucket missing")
	}
	return heads.ForEach(func(key, value []byte) error {
		if value == nil {
			return fmt.Errorf("callback lease mutation head %q is a nested bucket", key)
		}
		if !bytes.Equal(slots.Get(key), leaseMutationUUIDSlotValue) {
			return fmt.Errorf("callback lease mutation head %q has no exact UUID slot", key)
		}
		return nil
	})
}

// applyLeaseMutationTx is the only writer for the aggregate head. Its closed
// transition matrix is the structural state machine; business APIs may further
// narrow authority, but cannot encode an overlap or skip a required phase.
func applyLeaseMutationTx(
	tx *bolt.Tx,
	transition leaseMutationTransition,
) (leaseMutationHead, error) {
	leaseUUID, err := leaseMutationTransitionLeaseUUID(transition)
	if err != nil {
		return nil, err
	}
	if err := validateCanonicalLeaseUUID(leaseUUID); err != nil {
		return nil, err
	}
	current, present, err := getLeaseMutationHeadTx(tx, leaseUUID)
	if err != nil {
		return nil, err
	}
	next, err := validateLeaseMutationTransition(current, present, transition)
	if err != nil {
		return nil, err
	}
	// Every transition revalidates the permanent UUID reservation. On first
	// admission the marker and head commit atomically; later transitions reuse
	// it, so terminal close never needs to allocate after destructive work.
	if err := reserveLeaseMutationUUIDSlotTx(tx, leaseUUID); err != nil {
		return nil, err
	}
	if err := applyLeaseMutationTransitionEffectsTx(tx, transition); err != nil {
		return nil, err
	}
	// A successful close permanently rejects every future mutation for this
	// UUID and leaves its own cleanup receipt below. Historical operation IDs no
	// longer need individual replay fences: the closed head is the stronger
	// lease-wide fence, and releasing their reserved slots prevents dead leases
	// from monotonically exhausting the global admission budget.
	if _, complete := transition.(completeCloseLeaseMutation); complete {
		if err := releaseClosedLeaseReceiptReservationsTx(tx, leaseUUID); err != nil {
			return nil, err
		}
	}
	bucket := tx.Bucket(callbackLeaseMutationHeadBucketName)
	key := []byte(leaseUUID)
	if next == nil {
		if err := bucket.Delete(key); err != nil {
			return nil, err
		}
		return nil, nil
	}
	data, err := marshalLeaseMutationHead(next)
	if err != nil {
		return nil, err
	}
	if err := bucket.Put(key, data); err != nil {
		return nil, err
	}
	return decodeLeaseMutationHead(key, data)
}

func applyLeaseMutationTransitionEffectsTx(
	tx *bolt.Tx,
	transition leaseMutationTransition,
) error {
	switch typed := transition.(type) {
	case publishOperationLeaseMutation:
		return reserveOperationReceiptTx(tx, *typed.next.entry)
	case replaceOperationLeaseMutation:
		if err := reserveOperationReceiptTx(tx, *typed.next.entry); err != nil {
			return err
		}
		return archiveOperationCompletionTx(tx, *typed.previous.entry)
	case settleOperationLeaseMutation:
		return archiveOperationCompletionTx(tx, *typed.next.entry)
	case startOperationExecutionLeaseMutation:
		return nil
	case publishMaintenanceLeaseMutation:
		return reserveMaintenanceReceiptTx(tx, typed.next.entry)
	case replaceOperationWithMaintenanceLeaseMutation:
		if err := reserveMaintenanceReceiptTx(tx, typed.next.entry); err != nil {
			return err
		}
		return archiveOperationCompletionTx(tx, *typed.previous.entry)
	case replaceFailedOperationWithMaintenanceLeaseMutation:
		if err := reserveMaintenanceReceiptTx(tx, typed.next.entry); err != nil {
			return err
		}
		return archiveOperationCompletionTx(tx, *typed.previous.entry)
	case resolveMaintenanceLeaseMutation:
		return archiveMaintenanceCompletionTx(tx, typed.receipt)
	case replaceOperationWithCloseLeaseMutation:
		return archiveOperationCompletionTx(tx, typed.receipt)
	case replaceFailedOperationWithCloseLeaseMutation:
		return archiveOperationCompletionTx(tx, typed.receipt)
	case replaceMaintenanceWithCloseLeaseMutation:
		return archiveMaintenanceCompletionTx(tx, typed.receipt)
	case startMaintenanceAppendLeaseMutation,
		bindMaintenanceTargetLeaseMutation,
		startMaintenanceExecutionLeaseMutation,
		publishCloseLeaseMutation,
		advanceCloseLeaseMutation,
		completeCloseLeaseMutation:
		return nil
	case cancelMaintenanceLeaseMutation:
		return releaseCallbackReceiptReservationsTx(tx, 1)
	default:
		return fmt.Errorf("unsupported callback lease mutation transition %T", transition)
	}
}

func releaseClosedLeaseReceiptReservationsTx(tx *bolt.Tx, leaseUUID string) error {
	operationReceipts, err := listOperationHistoryTx(tx, leaseUUID)
	if err != nil {
		return err
	}
	maintenanceReceipts, err := listMaintenanceReceiptsTx(tx, leaseUUID)
	if err != nil {
		return err
	}
	released := uint64(len(operationReceipts) + len(maintenanceReceipts))
	if err := releaseCallbackReceiptReservationsTx(tx, released); err != nil {
		return err
	}
	if err := releaseClosedLeaseOperationReceiptsTx(tx, leaseUUID); err != nil {
		return err
	}
	return releaseClosedLeaseMaintenanceReceiptsTx(tx, leaseUUID)
}

func validateLeaseMutationTransition(
	current leaseMutationHead,
	present bool,
	transition leaseMutationTransition,
) (leaseMutationHead, error) {
	switch typed := transition.(type) {
	case publishOperationLeaseMutation:
		if err := requireAbsentLeaseMutationHead(current, present); err != nil {
			return nil, err
		}
		if err := validateMutationOperationClaim(typed.next); err != nil {
			return nil, err
		}
		if typed.next.entry.State != operationIntentPending || !typed.next.entry.EffectNotStarted {
			return nil, errors.New("new operation mutation must be pending")
		}
		return operationLeaseMutationHead{claim: typed.next}, nil

	case replaceOperationLeaseMutation:
		if err := requireCurrentOperationMutation(current, present, typed.previous); err != nil {
			return nil, err
		}
		if err := validateMutationOperationClaim(typed.next); err != nil {
			return nil, err
		}
		if typed.previous.entry.State == operationIntentPending ||
			typed.next.entry.State != operationIntentPending || !typed.next.entry.EffectNotStarted {
			return nil, errors.New("operation replacement requires terminal -> pending")
		}
		if !operationMutationAuthorityEqual(*typed.previous.entry, *typed.next.entry) {
			return nil, errors.New("operation replacement crosses backend storage or principal authority")
		}
		if typed.previous.entry.IntentID == typed.next.entry.IntentID ||
			typed.previous.entry.CallbackURL == typed.next.entry.CallbackURL ||
			(typed.next.entry.OperationID.Valid() &&
				typed.previous.entry.OperationID == typed.next.entry.OperationID) {
			return nil, errors.New("operation replacement reuses predecessor identity")
		}
		return operationLeaseMutationHead{claim: typed.next}, nil

	case settleOperationLeaseMutation:
		if err := requireCurrentOperationMutation(current, present, typed.previous); err != nil {
			return nil, err
		}
		if err := validateMutationOperationClaim(typed.next); err != nil {
			return nil, err
		}
		if typed.previous.entry.State != operationIntentPending ||
			typed.next.entry.State == operationIntentPending {
			return nil, errors.New("operation settlement requires pending -> terminal")
		}
		if !operationMutationIdentityEqual(*typed.previous.entry, *typed.next.entry) {
			return nil, errors.New("operation settlement changes immutable authority")
		}
		if typed.previous.entry.EffectNotStarted != typed.next.entry.EffectNotStarted {
			return nil, errors.New("operation settlement changes execution phase")
		}
		if typed.next.entry.State == operationIntentSucceeded &&
			typed.previous.entry.EffectNotStarted {
			return nil, errors.New("unstarted operation cannot settle successfully")
		}
		return operationLeaseMutationHead{claim: typed.next}, nil

	case startOperationExecutionLeaseMutation:
		if err := requireCurrentOperationMutation(current, present, typed.previous); err != nil {
			return nil, err
		}
		if err := validateMutationOperationClaim(typed.next); err != nil {
			return nil, err
		}
		if typed.previous.entry.State != operationIntentPending ||
			typed.next.entry.State != operationIntentPending ||
			!typed.previous.entry.EffectNotStarted || typed.next.entry.EffectNotStarted {
			return nil, errors.New("operation execution transition must change only not-started -> started")
		}
		if !operationMutationIdentityEqual(*typed.previous.entry, *typed.next.entry) {
			return nil, errors.New("operation execution transition changes immutable authority")
		}
		return operationLeaseMutationHead{claim: typed.next}, nil

	case publishMaintenanceLeaseMutation:
		if err := requireAbsentLeaseMutationHead(current, present); err != nil {
			return nil, err
		}
		if err := validateInitialMaintenanceMutation(typed.next); err != nil {
			return nil, err
		}
		return maintenanceLeaseMutationHead{claim: typed.next}, nil

	case replaceFailedOperationWithMaintenanceLeaseMutation:
		if err := requireCurrentOperationMutation(current, present, typed.previous); err != nil {
			return nil, err
		}
		if err := validateInitialMaintenanceMutation(typed.next); err != nil {
			return nil, err
		}
		sourceDigest, err := parseMaintenanceDigest(typed.next.entry.SourceReleaseDigest, false)
		if err != nil {
			return nil, err
		}
		source := ReleaseClaim{
			issuer: typed.predecessor.releases, leaseUUID: typed.next.LeaseUUID(),
			version: typed.next.entry.SourceReleaseVersion, digest: sourceDigest,
		}
		if typed.previous.entry.State != operationIntentFailed ||
			!typed.predecessor.validForHeadAndRelease(
				typed.predecessor.callbacks, typed.predecessor.releases,
				typed.previous, source,
			) {
			return nil, errors.New(
				"maintenance replacement lacks the failed operation's exact predecessor release",
			)
		}
		return maintenanceLeaseMutationHead{claim: typed.next}, nil

	case replaceOperationWithMaintenanceLeaseMutation:
		if err := requireCurrentOperationMutation(current, present, typed.previous); err != nil {
			return nil, err
		}
		if typed.previous.entry.State == operationIntentPending {
			return nil, errors.New("maintenance cannot replace a pending operation")
		}
		if err := validateInitialMaintenanceMutation(typed.next); err != nil {
			return nil, err
		}
		identity, ok := releaseRuntimeIdentityFor(typed.next.entry.TargetRelease)
		if !ok || typed.previous.Backend() != typed.next.Backend() ||
			typed.previous.BackendStorageID() != typed.next.BackendStorageID() ||
			typed.previous.Tenant() != identity.Tenant() ||
			typed.previous.ProviderUUID() != identity.ProviderUUID() ||
			typed.previous.OperationID() != identity.OperationID() {
			return nil, errors.New("maintenance replacement crosses terminal operation authority")
		}
		return maintenanceLeaseMutationHead{claim: typed.next}, nil

	case startMaintenanceAppendLeaseMutation:
		if err := requireCurrentMaintenanceMutation(current, present, typed.previous); err != nil {
			return nil, err
		}
		if err := validateMaintenanceIntentClaim(typed.next); err != nil {
			return nil, err
		}
		if typed.previous.entry.AppendStarted || maintenanceTargetBound(typed.previous.entry) ||
			!typed.next.entry.AppendStarted || maintenanceTargetBound(typed.next.entry) ||
			!typed.previous.entry.EffectNotStarted || !typed.next.entry.EffectNotStarted {
			return nil, errors.New("maintenance append transition must change only not-started -> started")
		}
		equal, err := maintenanceMutationIdentityEqual(typed.previous.entry, typed.next.entry)
		if err != nil {
			return nil, err
		}
		if !equal {
			return nil, errors.New("maintenance append transition changes immutable authority")
		}
		return maintenanceLeaseMutationHead{claim: typed.next}, nil

	case bindMaintenanceTargetLeaseMutation:
		if err := requireCurrentMaintenanceMutation(current, present, typed.previous); err != nil {
			return nil, err
		}
		if err := validateMaintenanceIntentClaim(typed.next); err != nil {
			return nil, err
		}
		if !typed.previous.entry.AppendStarted || maintenanceTargetBound(typed.previous.entry) ||
			!typed.next.entry.AppendStarted || !maintenanceTargetBound(typed.next.entry) ||
			!typed.previous.entry.EffectNotStarted || !typed.next.entry.EffectNotStarted {
			return nil, errors.New("maintenance target bind requires started unbound -> started bound")
		}
		equal, err := maintenanceMutationIdentityEqual(typed.previous.entry, typed.next.entry)
		if err != nil {
			return nil, err
		}
		if !equal {
			return nil, errors.New("maintenance target bind changes immutable authority")
		}
		return maintenanceLeaseMutationHead{claim: typed.next}, nil

	case startMaintenanceExecutionLeaseMutation:
		if err := requireCurrentMaintenanceMutation(current, present, typed.previous); err != nil {
			return nil, err
		}
		if err := validateMaintenanceIntentClaim(typed.next); err != nil {
			return nil, err
		}
		if !maintenanceTargetBound(typed.previous.entry) ||
			!maintenanceTargetBound(typed.next.entry) ||
			!typed.previous.entry.EffectNotStarted || typed.next.entry.EffectNotStarted {
			return nil, errors.New("maintenance execution transition must change only bound not-started -> started")
		}
		equal, err := maintenanceMutationIdentityEqual(typed.previous.entry, typed.next.entry)
		if err != nil {
			return nil, err
		}
		if !equal {
			return nil, errors.New("maintenance execution transition changes immutable authority")
		}
		return maintenanceLeaseMutationHead{claim: typed.next}, nil

	case cancelMaintenanceLeaseMutation:
		if err := requireCurrentMaintenanceMutation(current, present, typed.previous); err != nil {
			return nil, err
		}
		if typed.previous.entry.AppendStarted || maintenanceTargetBound(typed.previous.entry) {
			return nil, errors.New("maintenance cancellation requires an unstarted, unbound intent")
		}
		return nil, nil

	case resolveMaintenanceLeaseMutation:
		if err := requireCurrentMaintenanceMutation(current, present, typed.previous); err != nil {
			return nil, err
		}
		if err := validateMaintenanceCompletionRecord(typed.receipt, typed.previous.LeaseUUID()); err != nil {
			return nil, err
		}
		want := maintenanceCompletionRecordFor(
			typed.previous, typed.receipt.Status, typed.receipt.Error,
			typed.receipt.SettledAt, typed.receipt.CompletionSequence,
		)
		if typed.receipt != want {
			return nil, errors.New("maintenance resolution carries a divergent receipt")
		}
		return nil, nil

	case publishCloseLeaseMutation:
		if err := requireAbsentLeaseMutationHead(current, present); err != nil {
			return nil, err
		}
		if err := validateInitialCloseMutation(typed.next); err != nil {
			return nil, err
		}
		return closeLeaseMutationHead{claim: typed.next}, nil

	case replaceOperationWithCloseLeaseMutation:
		if err := requireCurrentOperationMutation(current, present, typed.previous); err != nil {
			return nil, err
		}
		if err := validateInitialCloseMutation(typed.next); err != nil {
			return nil, err
		}
		if typed.previous.Backend() != typed.next.Backend() ||
			typed.previous.BackendStorageID() != typed.next.BackendStorageID() {
			return nil, errors.New("close replacement crosses operation storage authority")
		}
		if err := validateOperationMutationReceipt(typed.previous, typed.receipt); err != nil {
			return nil, err
		}
		if typed.previous.entry.State == operationIntentPending {
			if !typed.next.CleanupOnly() && (typed.previous.Tenant() != typed.next.Tenant() ||
				typed.previous.ProviderUUID() != typed.next.ProviderUUID()) {
				return nil, errors.New("close replacement crosses pending operation principal authority")
			}
		} else if typed.next.ActiveReleaseVersion() == 0 ||
			(typed.next.ActiveReleaseVersion() > 0 &&
				typed.previous.OperationID() != typed.next.ActiveReleaseOperationID()) ||
			(!typed.next.CleanupOnly() && (typed.previous.Tenant() != typed.next.Tenant() ||
				typed.previous.ProviderUUID() != typed.next.ProviderUUID())) {
			return nil, errors.New("close replacement does not fence terminal operation lineage")
		}
		return closeLeaseMutationHead{claim: typed.next}, nil

	case replaceFailedOperationWithCloseLeaseMutation:
		if err := requireCurrentOperationMutation(current, present, typed.previous); err != nil {
			return nil, err
		}
		if err := validateInitialCloseMutation(typed.next); err != nil {
			return nil, err
		}
		if err := validateOperationMutationReceipt(typed.previous, typed.receipt); err != nil {
			return nil, err
		}
		if typed.previous.entry.State != operationIntentFailed ||
			typed.authority == nil ||
			!typed.authority.validForClose(typed.previous, typed.next) {
			return nil, errors.New(
				"close replacement lacks exact failed-operation cleanup authority",
			)
		}
		return closeLeaseMutationHead{claim: typed.next}, nil

	case replaceMaintenanceWithCloseLeaseMutation:
		if err := requireCurrentMaintenanceMutation(current, present, typed.previous); err != nil {
			return nil, err
		}
		if err := validateInitialCloseMutation(typed.next); err != nil {
			return nil, err
		}
		if err := validateMaintenanceCompletionRecord(typed.receipt, typed.previous.LeaseUUID()); err != nil {
			return nil, err
		}
		want := maintenanceCompletionRecordFor(
			typed.previous, typed.receipt.Status, typed.receipt.Error,
			typed.receipt.SettledAt, typed.receipt.CompletionSequence,
		)
		if typed.receipt != want {
			return nil, errors.New("close replacement carries a divergent maintenance receipt")
		}
		if typed.previous.Backend() != typed.next.Backend() ||
			typed.previous.BackendStorageID() != typed.next.BackendStorageID() ||
			typed.next.CleanupOnly() || typed.next.ActiveReleaseVersion() == 0 ||
			typed.next.ActiveReleaseVersion() != typed.previous.SourceRelease().Version() ||
			typed.next.entry.ActiveReleaseDigest != typed.previous.entry.SourceReleaseDigest ||
			typed.next.Tenant() != typed.previous.Tenant() ||
			typed.next.ProviderUUID() != typed.previous.ProviderUUID() ||
			typed.next.ActiveReleaseOperationID() != typed.previous.TargetRelease().OperationID {
			return nil, errors.New("close replacement does not fence maintenance source authority")
		}
		return closeLeaseMutationHead{claim: typed.next}, nil

	case advanceCloseLeaseMutation:
		if err := requireCurrentCloseMutation(current, present, typed.previous); err != nil {
			return nil, err
		}
		if err := validateCloseIntentClaim(typed.next); err != nil {
			return nil, err
		}
		if !closeMutationIdentityEqual(typed.previous.entry, typed.next.entry) ||
			typed.previous.ExecutionGeneration().Number() == math.MaxInt ||
			typed.next.ExecutionGeneration().Number() != typed.previous.ExecutionGeneration().Number()+1 {
			return nil, errors.New("close advancement must increment only the execution generation")
		}
		return closeLeaseMutationHead{claim: typed.next}, nil

	case completeCloseLeaseMutation:
		if err := requireCurrentCloseMutation(current, present, typed.previous); err != nil {
			return nil, err
		}
		if err := validateClosedLeaseTombstone(typed.next.entry, typed.previous.LeaseUUID()); err != nil {
			return nil, err
		}
		want, err := newClosedLeaseMutationHead(typed.previous, typed.next.entry.ClosedAt)
		if err != nil {
			return nil, err
		}
		if typed.next.entry != want.entry {
			return nil, errors.New("completed close carries a divergent tombstone")
		}
		return typed.next, nil

	default:
		return nil, fmt.Errorf("unsupported callback lease mutation transition %T", transition)
	}
}

func leaseMutationTransitionLeaseUUID(transition leaseMutationTransition) (string, error) {
	switch typed := transition.(type) {
	case publishOperationLeaseMutation:
		return typed.next.LeaseUUID(), nil
	case replaceOperationLeaseMutation:
		return typed.previous.LeaseUUID(), nil
	case settleOperationLeaseMutation:
		return typed.previous.LeaseUUID(), nil
	case startOperationExecutionLeaseMutation:
		return typed.previous.LeaseUUID(), nil
	case publishMaintenanceLeaseMutation:
		return typed.next.LeaseUUID(), nil
	case replaceOperationWithMaintenanceLeaseMutation:
		return typed.previous.LeaseUUID(), nil
	case replaceFailedOperationWithMaintenanceLeaseMutation:
		return typed.previous.LeaseUUID(), nil
	case startMaintenanceAppendLeaseMutation:
		return typed.previous.LeaseUUID(), nil
	case bindMaintenanceTargetLeaseMutation:
		return typed.previous.LeaseUUID(), nil
	case startMaintenanceExecutionLeaseMutation:
		return typed.previous.LeaseUUID(), nil
	case cancelMaintenanceLeaseMutation:
		return typed.previous.LeaseUUID(), nil
	case resolveMaintenanceLeaseMutation:
		return typed.previous.LeaseUUID(), nil
	case publishCloseLeaseMutation:
		return typed.next.LeaseUUID(), nil
	case replaceOperationWithCloseLeaseMutation:
		return typed.previous.LeaseUUID(), nil
	case replaceFailedOperationWithCloseLeaseMutation:
		return typed.previous.LeaseUUID(), nil
	case replaceMaintenanceWithCloseLeaseMutation:
		return typed.previous.LeaseUUID(), nil
	case advanceCloseLeaseMutation:
		return typed.previous.LeaseUUID(), nil
	case completeCloseLeaseMutation:
		return typed.previous.LeaseUUID(), nil
	default:
		return "", fmt.Errorf("unsupported callback lease mutation transition %T", transition)
	}
}

func declaredLeaseMutationCurrent(
	transition leaseMutationTransition,
) (leaseMutationHead, bool, error) {
	switch typed := transition.(type) {
	case publishOperationLeaseMutation, publishMaintenanceLeaseMutation, publishCloseLeaseMutation:
		return nil, false, nil
	case replaceOperationLeaseMutation:
		return operationLeaseMutationHead{claim: typed.previous}, true, nil
	case settleOperationLeaseMutation:
		return operationLeaseMutationHead{claim: typed.previous}, true, nil
	case startOperationExecutionLeaseMutation:
		return operationLeaseMutationHead{claim: typed.previous}, true, nil
	case replaceOperationWithMaintenanceLeaseMutation:
		return operationLeaseMutationHead{claim: typed.previous}, true, nil
	case replaceFailedOperationWithMaintenanceLeaseMutation:
		return operationLeaseMutationHead{claim: typed.previous}, true, nil
	case startMaintenanceAppendLeaseMutation:
		return maintenanceLeaseMutationHead{claim: typed.previous}, true, nil
	case bindMaintenanceTargetLeaseMutation:
		return maintenanceLeaseMutationHead{claim: typed.previous}, true, nil
	case startMaintenanceExecutionLeaseMutation:
		return maintenanceLeaseMutationHead{claim: typed.previous}, true, nil
	case cancelMaintenanceLeaseMutation:
		return maintenanceLeaseMutationHead{claim: typed.previous}, true, nil
	case resolveMaintenanceLeaseMutation:
		return maintenanceLeaseMutationHead{claim: typed.previous}, true, nil
	case replaceOperationWithCloseLeaseMutation:
		return operationLeaseMutationHead{claim: typed.previous}, true, nil
	case replaceFailedOperationWithCloseLeaseMutation:
		return operationLeaseMutationHead{claim: typed.previous}, true, nil
	case replaceMaintenanceWithCloseLeaseMutation:
		return maintenanceLeaseMutationHead{claim: typed.previous}, true, nil
	case advanceCloseLeaseMutation:
		return closeLeaseMutationHead{claim: typed.previous}, true, nil
	case completeCloseLeaseMutation:
		return closeLeaseMutationHead{claim: typed.previous}, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported callback lease mutation transition %T", transition)
	}
}

func requireAbsentLeaseMutationHead(current leaseMutationHead, present bool) error {
	if present || current != nil {
		return errors.New("callback lease mutation head already exists")
	}
	return nil
}

func requireCurrentOperationMutation(
	current leaseMutationHead,
	present bool,
	expected OperationIntentClaim,
) error {
	if err := validateMutationOperationClaim(expected); err != nil {
		return err
	}
	actual, ok := current.(operationLeaseMutationHead)
	if !present || !ok {
		return errors.New("callback operation mutation head no longer exists")
	}
	if actual.claim.digest != expected.digest {
		return errors.New("callback operation mutation changed before precise transition")
	}
	if actual.claim.storageID != expected.storageID ||
		!operationMutationEntriesExactlyEqual(*actual.claim.entry, *expected.entry) {
		return errors.New("callback operation mutation claim diverges from stored authority")
	}
	return nil
}

func requireCurrentMaintenanceMutation(
	current leaseMutationHead,
	present bool,
	expected MaintenanceIntentClaim,
) error {
	if err := validateMaintenanceIntentClaim(expected); err != nil {
		return err
	}
	actual, ok := current.(maintenanceLeaseMutationHead)
	if !present || !ok {
		return errors.New("callback maintenance mutation head no longer exists")
	}
	if actual.claim.digest != expected.digest {
		return errors.New("callback maintenance mutation changed before precise transition")
	}
	equal, err := maintenanceMutationEntriesExactlyEqual(actual.claim.entry, expected.entry)
	if err != nil {
		return err
	}
	if actual.claim.storageID != expected.storageID || !equal {
		return errors.New("callback maintenance mutation claim diverges from stored authority")
	}
	return nil
}

func requireCurrentCloseMutation(
	current leaseMutationHead,
	present bool,
	expected CloseIntentClaim,
) error {
	if err := validateCloseIntentClaim(expected); err != nil {
		return err
	}
	actual, ok := current.(closeLeaseMutationHead)
	if !present || !ok {
		return errors.New("callback close mutation head no longer exists")
	}
	if actual.claim.digest != expected.digest {
		return errors.New("callback close mutation changed before precise transition")
	}
	if actual.claim.storageID != expected.storageID ||
		!closeMutationEntriesExactlyEqual(actual.claim.entry, expected.entry) {
		return errors.New("callback close mutation claim diverges from stored authority")
	}
	return nil
}

func validateMutationOperationClaim(claim OperationIntentClaim) error {
	if claim.digest == ([sha256.Size]byte{}) || claim.entry == nil || claim.entry.IntentID == "" {
		return errors.New("callback operation mutation claim has no durable capability")
	}
	if !claim.storageID.Valid() || claim.storageID.String() != claim.entry.BackendStorageID {
		return errors.New("callback operation mutation claim has invalid storage authority")
	}
	return validateOperationIntentEntry(*claim.entry, claim.entry.LeaseUUID)
}

func validateInitialMaintenanceMutation(claim MaintenanceIntentClaim) error {
	if err := validateMaintenanceIntentClaim(claim); err != nil {
		return err
	}
	if claim.entry.AppendStarted || maintenanceTargetBound(claim.entry) ||
		!claim.entry.EffectNotStarted {
		return errors.New("new maintenance mutation must be unstarted and unbound")
	}
	return nil
}

func validateInitialCloseMutation(claim CloseIntentClaim) error {
	if err := validateCloseIntentClaim(claim); err != nil {
		return err
	}
	if claim.ExecutionGeneration().Number() != 0 {
		return errors.New("new close mutation carries a started execution generation")
	}
	return nil
}

func maintenanceTargetBound(entry maintenanceIntentEntry) bool {
	return entry.TargetReleaseVersion > 0 && entry.TargetReleaseDigest != ""
}

func maintenanceMutationIdentityEqual(left, right maintenanceIntentEntry) (bool, error) {
	left = cloneMaintenanceIntentEntry(left)
	right = cloneMaintenanceIntentEntry(right)
	left.AppendStarted = false
	left.TargetReleaseVersion = 0
	left.TargetReleaseDigest = ""
	left.EffectNotStarted = false
	right.AppendStarted = false
	right.TargetReleaseVersion = 0
	right.TargetReleaseDigest = ""
	right.EffectNotStarted = false
	leftData, err := marshalMaintenanceIntent(left)
	if err != nil {
		return false, err
	}
	rightData, err := marshalMaintenanceIntent(right)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftData, rightData), nil
}

func maintenanceMutationEntriesExactlyEqual(left, right maintenanceIntentEntry) (bool, error) {
	leftData, err := marshalMaintenanceIntent(left)
	if err != nil {
		return false, err
	}
	rightData, err := marshalMaintenanceIntent(right)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftData, rightData), nil
}

func operationMutationAuthorityEqual(left, right operationIntentEntry) bool {
	return left.LeaseUUID == right.LeaseUUID &&
		left.Backend == right.Backend &&
		left.BackendStorageID == right.BackendStorageID &&
		left.Tenant == right.Tenant &&
		left.ProviderUUID == right.ProviderUUID
}

func operationMutationIdentityEqual(left, right operationIntentEntry) bool {
	return left.IntentID == right.IntentID &&
		left.CreatedAt.Equal(right.CreatedAt) &&
		operationIntentEntriesEqual(left, right)
}

func operationMutationEntriesExactlyEqual(left, right operationIntentEntry) bool {
	return operationMutationIdentityEqual(left, right) &&
		left.EffectNotStarted == right.EffectNotStarted &&
		left.State == right.State &&
		left.SettledAt.Equal(right.SettledAt) &&
		left.SettlementError == right.SettlementError &&
		left.FailurePredecessor == right.FailurePredecessor
}

func validateOperationMutationReceipt(
	previous OperationIntentClaim,
	receipt operationIntentEntry,
) error {
	if err := validateOperationIntentEntry(receipt, previous.LeaseUUID()); err != nil {
		return err
	}
	if receipt.State == operationIntentPending {
		return errors.New("operation transition receipt is not terminal")
	}
	if !operationMutationIdentityEqual(*previous.entry, receipt) {
		return errors.New("operation transition carries a divergent receipt")
	}
	if previous.entry.State != operationIntentPending &&
		(previous.entry.State != receipt.State ||
			!previous.entry.SettledAt.Equal(receipt.SettledAt) ||
			previous.entry.SettlementError != receipt.SettlementError ||
			previous.entry.FailurePredecessor != receipt.FailurePredecessor) {
		return errors.New("operation transition changes a terminal receipt")
	}
	return nil
}

func closeMutationIdentityEqual(left, right closeIntentEntry) bool {
	return left.IntentID == right.IntentID &&
		left.CreatedAt.Equal(right.CreatedAt) &&
		closeIntentEntryMatchesSpec(left, right)
}

func closeMutationEntriesExactlyEqual(left, right closeIntentEntry) bool {
	return closeMutationIdentityEqual(left, right) &&
		left.ExecutionGeneration == right.ExecutionGeneration
}
