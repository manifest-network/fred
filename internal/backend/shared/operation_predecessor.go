package shared

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// operationFailurePredecessorKind is the durable sum tag for the active
// generation observed when an operation definitively fails. Absence is an
// explicit variant: an omitted field can never be mistaken for proof that no
// predecessor existed.
type operationFailurePredecessorKind string

const (
	operationFailurePredecessorAbsent operationFailurePredecessorKind = "absent"
	operationFailurePredecessorActive operationFailurePredecessorKind = "active_release"
)

// operationFailurePredecessorRecord is the compact wire representation kept
// on a terminal Failed operation head. The active variant seals the complete
// predecessor Release through its exact version and canonical-row digest.
type operationFailurePredecessorRecord struct {
	Kind           operationFailurePredecessorKind `json:"kind"`
	ReleaseVersion int                             `json:"release_version,omitempty"`
	ReleaseDigest  string                          `json:"release_digest,omitempty"`
}

func (record operationFailurePredecessorRecord) IsZero() bool {
	return record.Kind == "" && record.ReleaseVersion == 0 && record.ReleaseDigest == ""
}

func validateOperationFailurePredecessorRecord(
	record operationFailurePredecessorRecord,
) error {
	switch record.Kind {
	case operationFailurePredecessorAbsent:
		if record.ReleaseVersion != 0 || record.ReleaseDigest != "" {
			return errors.New("absent failed-operation predecessor carries release authority")
		}
		return nil
	case operationFailurePredecessorActive:
		if record.ReleaseVersion <= 0 {
			return errors.New("active failed-operation predecessor requires a positive release version")
		}
		if _, err := parseOperationFailurePredecessorDigest(record.ReleaseDigest); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("invalid failed-operation predecessor kind %q", record.Kind)
	}
}

// operationFailurePredecessor is a sealed live counterpart to the durable sum.
// Only the pair-bound failure settlement can construct either variant.
type operationFailurePredecessor interface {
	record() operationFailurePredecessorRecord
	validFor(*OperationSettlement, OperationIntentClaim) bool
	isOperationFailurePredecessor()
}

type failedOperationWithoutPredecessor struct {
	settlement      *OperationSettlement
	successorDigest [sha256.Size]byte
}

func (failedOperationWithoutPredecessor) isOperationFailurePredecessor() {}
func (predecessor failedOperationWithoutPredecessor) record() operationFailurePredecessorRecord {
	return operationFailurePredecessorRecord{Kind: operationFailurePredecessorAbsent}
}
func (predecessor failedOperationWithoutPredecessor) validFor(
	settlement *OperationSettlement,
	successor OperationIntentClaim,
) bool {
	return settlement != nil && predecessor.settlement == settlement &&
		predecessor.successorDigest != ([sha256.Size]byte{}) &&
		predecessor.successorDigest == successor.digest
}

// failedOperationOverRelease is a store-bound proof that a terminal Failed
// operation is the successor of one exact active Release. It is deliberately
// generic over typed and adopted-v0.13 runtime authority: the full Release
// digest, not an operation token, identifies the predecessor.
type failedOperationOverRelease struct {
	callbacks       *CallbackStore
	releases        *ReleaseStore
	successorDigest [sha256.Size]byte
	predecessor     ReleaseClaim
}

func (failedOperationOverRelease) isOperationFailurePredecessor() {}
func (relation failedOperationOverRelease) record() operationFailurePredecessorRecord {
	return operationFailurePredecessorRecord{
		Kind:           operationFailurePredecessorActive,
		ReleaseVersion: relation.predecessor.Version(),
		ReleaseDigest:  encodeOperationFailurePredecessorDigest(relation.predecessor.Digest()),
	}
}
func (relation failedOperationOverRelease) validFor(
	settlement *OperationSettlement,
	successor OperationIntentClaim,
) bool {
	return settlement != nil && relation.callbacks == settlement.callbacks &&
		relation.releases == settlement.releases && relation.predecessor.issuer == relation.releases &&
		relation.predecessor.valid() && relation.successorDigest != ([sha256.Size]byte{}) &&
		relation.successorDigest == successor.digest
}

// failedOperationWithoutRelease is a store-bound proof that the exact current
// Failed operation recorded explicit predecessor absence and that this lease
// still has no Release history. This is deliberately not an
// operationFailurePredecessor: it grants only callbackless cleanup-close
// admission, never operation settlement or runtime observation authority.
type failedOperationWithoutRelease struct {
	callbacks       *CallbackStore
	releases        *ReleaseStore
	successorDigest [sha256.Size]byte
}

func (absence failedOperationWithoutRelease) validForHead(
	callbacks *CallbackStore,
	releases *ReleaseStore,
	head OperationIntentClaim,
) bool {
	return absence.callbacks == callbacks && absence.releases == releases &&
		absence.successorDigest != ([sha256.Size]byte{}) &&
		absence.successorDigest == head.digest && head.entry != nil &&
		head.entry.State == operationIntentFailed &&
		head.entry.FailurePredecessor == (operationFailurePredecessorRecord{
			Kind: operationFailurePredecessorAbsent,
		})
}

// failedOperationCloseAuthority is the sealed sum consumed by the one Failed
// -> Close aggregate transition. Its variants make an active predecessor and
// durable predecessor absence non-interchangeable while avoiding parallel
// copies of the transition machinery.
type failedOperationCloseAuthority interface {
	validForClose(OperationIntentClaim, CloseIntentClaim) bool
	isFailedOperationCloseAuthority()
}

func (failedOperationOverRelease) isFailedOperationCloseAuthority()    {}
func (failedOperationWithoutRelease) isFailedOperationCloseAuthority() {}

func (relation failedOperationOverRelease) validForClose(
	head OperationIntentClaim,
	next CloseIntentClaim,
) bool {
	predecessor := ReleaseClaim{
		issuer: relation.releases, leaseUUID: next.LeaseUUID(),
		version: next.ActiveReleaseVersion(), digest: next.ActiveReleaseDigest(),
	}
	return relation.validForHeadAndRelease(
		relation.callbacks, relation.releases, head, predecessor,
	)
}

func (absence failedOperationWithoutRelease) validForClose(
	head OperationIntentClaim,
	next CloseIntentClaim,
) bool {
	return next.CleanupOnly() && next.ActiveReleaseVersion() == 0 &&
		next.ActiveReleaseDigest() == ([sha256.Size]byte{}) &&
		next.ActiveReleaseOperationID().IsZero() &&
		absence.validForHead(absence.callbacks, absence.releases, head)
}

// captureOperationFailurePredecessorLocked observes the current active Release
// while failure settlement owns the journal pair's lease gate. The returned
// sum is written into the terminal operation head in the same callback-store
// transaction as the Failed callback.
func (s *OperationSettlement) captureOperationFailurePredecessorLocked(
	successor OperationIntentClaim,
) (operationFailurePredecessor, error) {
	if s == nil || !s.valid() || successor.settlement != s {
		return nil, errors.New("failed-operation predecessor requires the owning journal pair")
	}
	if err := s.callbacks.requireCurrentOperationClaim(successor); err != nil {
		return nil, err
	}

	var predecessor ReleaseClaim
	found := false
	err := s.releases.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(releasesBucketName)
		if bucket == nil {
			return errors.New("releases bucket missing")
		}
		data := bucket.Get([]byte(successor.LeaseUUID()))
		if data == nil {
			return nil
		}
		history, err := decodeReleaseHistory(data)
		if err != nil {
			return fmt.Errorf("decode failed-operation predecessor history: %w", err)
		}
		if err := validateReleaseHistory(history); err != nil {
			return fmt.Errorf("validate failed-operation predecessor history: %w", err)
		}
		index := latestActiveReleaseIndex(history)
		if index < 0 {
			return nil
		}
		if err := validateFailedOperationPredecessorAuthority(
			s.callbacks, successor.operationAuthority, history[index],
		); err != nil {
			return err
		}
		encoded, err := json.Marshal(history[index])
		if err != nil {
			return fmt.Errorf("marshal failed-operation predecessor release: %w", err)
		}
		predecessor = ReleaseClaim{
			issuer: s.releases, leaseUUID: successor.LeaseUUID(),
			version: history[index].Version, digest: sha256.Sum256(encoded),
		}
		found = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !found {
		return failedOperationWithoutPredecessor{
			settlement: s, successorDigest: successor.digest,
		}, nil
	}
	return failedOperationOverRelease{
		callbacks: s.callbacks, releases: s.releases,
		successorDigest: successor.digest, predecessor: predecessor,
	}, nil
}

// bindFailedOperationOverRelease converts a durable Failed-head record and an
// exact currently-active Release claim into live pair-bound authority. Callers
// already hold the shared lease gate; the release claim and head therefore
// describe one linearized pair snapshot.
func bindFailedOperationOverRelease(
	callbacks *CallbackStore,
	releases *ReleaseStore,
	head operationLeaseMutationHead,
	predecessor ReleaseClaim,
) (failedOperationOverRelease, error) {
	if callbacks == nil || releases == nil || !boltStoreIsOpen(callbacks.boltStore) ||
		!boltStoreIsOpen(releases.boltStore) || callbacks.backendAuthorityGate == nil ||
		callbacks.backendAuthorityGate != releases.backendAuthorityGate ||
		callbacks.binding == nil || releases.binding == nil ||
		callbacks.binding.backendName != releases.binding.backendName ||
		callbacks.binding.storageID != releases.binding.storageID {
		return failedOperationOverRelease{}, errors.New(
			"failed-operation predecessor requires one open journal pair",
		)
	}
	if head.claim.entry == nil || head.claim.entry.State != operationIntentFailed ||
		head.headDigest() == ([sha256.Size]byte{}) {
		return failedOperationOverRelease{}, errors.New(
			"failed-operation predecessor requires a terminal Failed successor",
		)
	}
	if predecessor.issuer != releases || !predecessor.valid() ||
		predecessor.LeaseUUID() != head.leaseUUID() {
		return failedOperationOverRelease{}, errors.New(
			"failed-operation predecessor release belongs to another journal",
		)
	}
	record := head.claim.entry.FailurePredecessor
	if err := validateOperationFailurePredecessorRecord(record); err != nil {
		return failedOperationOverRelease{}, err
	}
	if record.Kind != operationFailurePredecessorActive ||
		record.ReleaseVersion != predecessor.Version() ||
		record.ReleaseDigest != encodeOperationFailurePredecessorDigest(predecessor.Digest()) {
		return failedOperationOverRelease{}, errors.New(
			"failed operation does not seal the exact active predecessor release",
		)
	}
	release, err := reattestFailedOperationPredecessorRelease(releases, predecessor)
	if err != nil {
		return failedOperationOverRelease{}, err
	}
	if err := validateFailedOperationPredecessorAuthority(
		callbacks, head.claim.operationAuthority, release,
	); err != nil {
		return failedOperationOverRelease{}, err
	}
	if err := callbacks.view(func(tx *bolt.Tx) error {
		current, present, err := getLeaseMutationHeadTx(tx, head.leaseUUID())
		if err != nil {
			return err
		}
		return requireCurrentOperationMutation(current, present, head.claim)
	}); err != nil {
		return failedOperationOverRelease{}, fmt.Errorf(
			"re-attest failed-operation successor: %w", err,
		)
	}
	return failedOperationOverRelease{
		callbacks: callbacks, releases: releases,
		successorDigest: head.headDigest(), predecessor: predecessor,
	}, nil
}

// bindFailedOperationWithoutRelease reconstructs cleanup-only authority from
// the durable absent variant. Release absence is re-attested before the
// callback head, matching the pair's Release -> Callback lock order. The
// caller owns the shared lease gate, so no pair writer can create a Release
// between this proof and the close-head transaction.
func bindFailedOperationWithoutRelease(
	callbacks *CallbackStore,
	releases *ReleaseStore,
	head operationLeaseMutationHead,
) (failedOperationWithoutRelease, error) {
	if callbacks == nil || releases == nil || !boltStoreIsOpen(callbacks.boltStore) ||
		!boltStoreIsOpen(releases.boltStore) || callbacks.backendAuthorityGate == nil ||
		callbacks.backendAuthorityGate != releases.backendAuthorityGate ||
		callbacks.binding == nil || releases.binding == nil ||
		callbacks.binding.backendName != releases.binding.backendName ||
		callbacks.binding.storageID != releases.binding.storageID {
		return failedOperationWithoutRelease{}, errors.New(
			"failed-operation absence requires one open journal pair",
		)
	}
	if head.claim.entry == nil || head.claim.entry.State != operationIntentFailed ||
		head.headDigest() == ([sha256.Size]byte{}) ||
		head.claim.entry.FailurePredecessor != (operationFailurePredecessorRecord{
			Kind: operationFailurePredecessorAbsent,
		}) {
		return failedOperationWithoutRelease{}, errors.New(
			"failed-operation absence requires a terminal Failed successor with explicit predecessor absence",
		)
	}
	if err := releases.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(releasesBucketName)
		if bucket == nil {
			return errors.New("releases bucket missing")
		}
		key := []byte(head.leaseUUID())
		if bucket.Bucket(key) != nil || bucket.Get(key) != nil {
			return errors.New("failed operation no longer has absent Release history")
		}
		return nil
	}); err != nil {
		return failedOperationWithoutRelease{}, err
	}
	if err := callbacks.view(func(tx *bolt.Tx) error {
		current, present, err := getLeaseMutationHeadTx(tx, head.leaseUUID())
		if err != nil {
			return err
		}
		return requireCurrentOperationMutation(current, present, head.claim)
	}); err != nil {
		return failedOperationWithoutRelease{}, fmt.Errorf(
			"re-attest failed-operation absence: %w", err,
		)
	}
	return failedOperationWithoutRelease{
		callbacks: callbacks, releases: releases, successorDigest: head.headDigest(),
	}, nil
}

func reattestFailedOperationPredecessorRelease(
	releases *ReleaseStore,
	predecessor ReleaseClaim,
) (Release, error) {
	if releases == nil || predecessor.issuer != releases || !predecessor.valid() {
		return Release{}, errors.New("failed-operation predecessor release belongs to another journal")
	}
	var release Release
	err := releases.view(func(tx *bolt.Tx) error {
		history, err := readReleaseHistoryTx(tx, predecessor.LeaseUUID())
		if err != nil {
			return err
		}
		current, err := verifySourceRelease(history, predecessor)
		if err != nil {
			return fmt.Errorf("re-attest failed-operation predecessor: %w", err)
		}
		release = cloneRelease(current)
		return nil
	})
	return release, err
}

func validateFailedOperationPredecessorAuthority(
	callbacks *CallbackStore,
	successor operationAuthority,
	predecessor Release,
) error {
	identity, ok := predecessor.RuntimeIdentity()
	if !ok {
		return errors.New("failed-operation predecessor has no complete runtime authority")
	}
	if callbacks == nil || callbacks.binding == nil || successor.entry == nil ||
		successor.Backend() != callbacks.binding.backendName ||
		successor.BackendStorageID() != callbacks.binding.storageID {
		return errors.New("failed-operation successor belongs to another backend storage authority")
	}
	if identity.Tenant() != successor.Tenant() ||
		identity.ProviderUUID() != successor.ProviderUUID() {
		return errors.New(
			"failed-operation successor and predecessor have different principal authority",
		)
	}
	return nil
}

func (relation failedOperationOverRelease) validForHeadAndRelease(
	callbacks *CallbackStore,
	releases *ReleaseStore,
	head OperationIntentClaim,
	predecessor ReleaseClaim,
) bool {
	if relation.callbacks != callbacks || relation.releases != releases ||
		relation.predecessor.issuer != releases || predecessor.issuer != releases ||
		relation.successorDigest == ([sha256.Size]byte{}) ||
		relation.successorDigest != head.digest || relation.predecessor != predecessor {
		return false
	}
	record := head.entry.FailurePredecessor
	return record.Kind == operationFailurePredecessorActive &&
		record.ReleaseVersion == predecessor.Version() &&
		record.ReleaseDigest == encodeOperationFailurePredecessorDigest(predecessor.Digest())
}

func parseOperationFailurePredecessorDigest(value string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != value {
		return digest, errors.New("failed-operation predecessor digest must be canonical SHA-256")
	}
	copy(digest[:], decoded)
	if digest == ([sha256.Size]byte{}) {
		return digest, errors.New("failed-operation predecessor digest cannot be zero")
	}
	return digest, nil
}

func encodeOperationFailurePredecessorDigest(digest [sha256.Size]byte) string {
	if digest == ([sha256.Size]byte{}) {
		return ""
	}
	return hex.EncodeToString(digest[:])
}
