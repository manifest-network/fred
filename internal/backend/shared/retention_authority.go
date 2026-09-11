package shared

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendidentity"
)

var errActiveRetentionChanged = errors.New("active retention generation changed before transition")

// RestoringRetentionProof is store-issued authority for one exact Restoring
// generation. The proof contains the canonical durable row rather than a
// caller-owned RetentionEntry: callers may inspect a detached DTO, but cannot
// alter the authority later consumed by a terminal transition.
//
// Proofs are copyable and deliberately not linear. Every consumer re-reads the
// row and compares its canonical bytes while holding the retention-store lock,
// so a stale copy cannot survive an ABA transition, route move, or reopen.
type RestoringRetentionProof struct {
	issuer    *RetentionStore
	backend   string
	storageID backendidentity.ID
	row       []byte
	digest    [sha256.Size]byte
}

// ActiveRetentionCandidate is store-issued authority to attempt one destructive
// transition from one exact Active row. Candidates come only from the store's
// active selectors; a caller cannot turn a lease UUID or a detached inspection
// DTO into mutation authority. Every consumer re-attests the complete durable
// row, so a copied candidate cannot survive an ABA rewrite or store reopen.
//
// This type is deliberately distinct from ActiveRetentionProof. The latter is
// close-settlement evidence minted only after physical retained-volume
// attestation and can complete a retained close; a pre-cleanup candidate cannot.
type ActiveRetentionCandidate struct {
	issuer    *RetentionStore
	backend   string
	storageID backendidentity.ID
	row       []byte
	digest    [sha256.Size]byte
}

// ReapingRetentionProof is store-issued authority for one exact Reaping
// generation. Physical cleanup carries this opaque value and can delete only
// the unchanged tombstone it actually inspected.
type ReapingRetentionProof struct {
	issuer    *RetentionStore
	backend   string
	storageID backendidentity.ID
	row       []byte
	digest    [sha256.Size]byte
}

func retentionProofShape(
	issuer *RetentionStore,
	backend string,
	storageID backendidentity.ID,
	row []byte,
	digest [sha256.Size]byte,
) bool {
	return issuer != nil && backend != "" && storageID.Valid() && len(row) != 0 &&
		digest != ([sha256.Size]byte{}) && sha256.Sum256(row) == digest
}

// Valid reports whether the proof has a complete opaque shape. It does not
// establish current authority; terminal consumers always re-attest the row.
func (proof RestoringRetentionProof) Valid() bool {
	return retentionProofShape(
		proof.issuer, proof.backend, proof.storageID, proof.row, proof.digest,
	)
}

// Valid reports whether the proof has a complete opaque shape. It does not
// establish current authority; terminal consumers always re-attest the row.
func (proof ReapingRetentionProof) Valid() bool {
	return retentionProofShape(
		proof.issuer, proof.backend, proof.storageID, proof.row, proof.digest,
	)
}

// Valid reports whether the candidate has a complete opaque shape. It does not
// establish current authority; transition consumers always re-attest the row.
func (candidate ActiveRetentionCandidate) Valid() bool {
	return retentionProofShape(
		candidate.issuer, candidate.backend, candidate.storageID,
		candidate.row, candidate.digest,
	)
}

// Entry returns a detached read DTO. Mutating the result cannot modify the
// proof's canonical row or authorize a different transition.
func (proof RestoringRetentionProof) Entry() RetentionEntry {
	entry, err := decodeRetentionEntry(proof.row)
	if err != nil {
		return RetentionEntry{}
	}
	return entry
}

// Entry returns a detached read DTO. Mutating the result cannot modify the
// proof's canonical row or authorize a different transition.
func (proof ReapingRetentionProof) Entry() RetentionEntry {
	entry, err := decodeRetentionEntry(proof.row)
	if err != nil {
		return RetentionEntry{}
	}
	return entry
}

// Entry returns a detached read DTO for policy checks and diagnostics.
// Mutating the result cannot alter the candidate or authorize another row.
func (candidate ActiveRetentionCandidate) Entry() RetentionEntry {
	entry, err := decodeRetentionEntry(candidate.row)
	if err != nil {
		return RetentionEntry{}
	}
	return entry
}

func (s *RetentionStore) mintActiveCandidate(raw []byte) (ActiveRetentionCandidate, error) {
	if s == nil || s.boltStore == nil || s.binding == nil ||
		s.backendAuthorityGate == nil {
		return ActiveRetentionCandidate{}, errors.New(
			"active retention candidate requires an identity-bound journal",
		)
	}
	entry, err := decodeRetentionEntry(raw)
	if err != nil {
		return ActiveRetentionCandidate{}, fmt.Errorf("decode active retention candidate: %w", err)
	}
	if err := validateAuthoritativeRetentionIdentity(
		[]byte(entry.OriginalLeaseUUID), &entry,
	); err != nil {
		return ActiveRetentionCandidate{}, fmt.Errorf("validate active retention candidate: %w", err)
	}
	if err := validateRetentionEntryResourceProfiles(&entry); err != nil {
		return ActiveRetentionCandidate{}, fmt.Errorf("validate active retention candidate: %w", err)
	}
	if err := validateRetentionSourceAuthorityForBinding(&entry); err != nil {
		return ActiveRetentionCandidate{}, fmt.Errorf("validate active retention candidate: %w", err)
	}
	if entry.Status != RetentionStatusActive {
		return ActiveRetentionCandidate{}, fmt.Errorf(
			"retention record %q is %q, not active",
			entry.OriginalLeaseUUID, entry.Status,
		)
	}
	row := bytes.Clone(raw)
	return ActiveRetentionCandidate{
		issuer: s, backend: s.binding.backendName, storageID: s.binding.storageID,
		row: row, digest: sha256.Sum256(row),
	}, nil
}

func (s *RetentionStore) mintRestoringProof(raw []byte) (RestoringRetentionProof, error) {
	if s == nil || s.boltStore == nil || s.binding == nil ||
		s.backendAuthorityGate == nil {
		return RestoringRetentionProof{}, errors.New(
			"restoring retention proof requires an identity-bound journal",
		)
	}
	entry, err := decodeRetentionEntry(raw)
	if err != nil {
		return RestoringRetentionProof{}, fmt.Errorf("decode restoring retention proof: %w", err)
	}
	if err := validateAuthoritativeRetentionIdentity(
		[]byte(entry.OriginalLeaseUUID), &entry,
	); err != nil {
		return RestoringRetentionProof{}, fmt.Errorf("validate restoring retention proof: %w", err)
	}
	if err := validateRetentionEntryResourceProfiles(&entry); err != nil {
		return RestoringRetentionProof{}, fmt.Errorf("validate restoring retention proof: %w", err)
	}
	if err := validateRetentionSourceAuthorityForBinding(&entry); err != nil {
		return RestoringRetentionProof{}, fmt.Errorf("validate restoring retention proof: %w", err)
	}
	if entry.Status != RetentionStatusRestoring {
		return RestoringRetentionProof{}, fmt.Errorf(
			"%w: retention record %q is %q",
			ErrNotRestorable, entry.OriginalLeaseUUID, entry.Status,
		)
	}
	row := bytes.Clone(raw)
	return RestoringRetentionProof{
		issuer: s, backend: s.binding.backendName, storageID: s.binding.storageID,
		row: row, digest: sha256.Sum256(row),
	}, nil
}

func (s *RetentionStore) mintReapingProof(raw []byte) (ReapingRetentionProof, error) {
	if s == nil || s.boltStore == nil || s.binding == nil ||
		s.backendAuthorityGate == nil {
		return ReapingRetentionProof{}, errors.New(
			"reaping retention proof requires an identity-bound journal",
		)
	}
	entry, err := decodeRetentionEntry(raw)
	if err != nil {
		return ReapingRetentionProof{}, fmt.Errorf("decode reaping retention proof: %w", err)
	}
	if err := validateAuthoritativeRetentionIdentity(
		[]byte(entry.OriginalLeaseUUID), &entry,
	); err != nil {
		return ReapingRetentionProof{}, fmt.Errorf("validate reaping retention proof: %w", err)
	}
	if err := validateRetentionEntryResourceProfiles(&entry); err != nil {
		return ReapingRetentionProof{}, fmt.Errorf("validate reaping retention proof: %w", err)
	}
	if err := validateRetentionSourceAuthorityForBinding(&entry); err != nil {
		return ReapingRetentionProof{}, fmt.Errorf("validate reaping retention proof: %w", err)
	}
	if entry.Status != RetentionStatusReaping {
		return ReapingRetentionProof{}, fmt.Errorf(
			"retention record %q is %q, not reaping",
			entry.OriginalLeaseUUID, entry.Status,
		)
	}
	row := bytes.Clone(raw)
	return ReapingRetentionProof{
		issuer: s, backend: s.binding.backendName, storageID: s.binding.storageID,
		row: row, digest: sha256.Sum256(row),
	}, nil
}

func (s *RetentionStore) requireRestoringProofTx(
	tx *bolt.Tx,
	proof RestoringRetentionProof,
) (RetentionEntry, error) {
	if !proof.Valid() || proof.issuer != s || s.binding == nil ||
		proof.backend != s.binding.backendName || proof.storageID != s.binding.storageID {
		return RetentionEntry{}, errors.New(
			"restoring retention proof belongs to another journal lineage",
		)
	}
	entry := proof.Entry()
	if entry.OriginalLeaseUUID == "" {
		return RetentionEntry{}, errors.New("restoring retention proof has no source lease")
	}
	raw := tx.Bucket(retentionBucketName).Get([]byte(entry.OriginalLeaseUUID))
	if raw == nil {
		return RetentionEntry{}, ErrNoRetention
	}
	current := bytes.Clone(raw)
	if sha256.Sum256(current) != proof.digest || !bytes.Equal(current, proof.row) {
		return RetentionEntry{}, errors.New(
			"restoring retention generation changed before settlement",
		)
	}
	entry, err := decodeRetentionEntry(current)
	if err != nil {
		return RetentionEntry{}, err
	}
	if entry.Status != RetentionStatusRestoring {
		return RetentionEntry{}, ErrNotRestorable
	}
	return entry, nil
}

func (s *RetentionStore) requireReapingProofTx(
	tx *bolt.Tx,
	proof ReapingRetentionProof,
) (RetentionEntry, error) {
	if !proof.Valid() || proof.issuer != s || s.binding == nil ||
		proof.backend != s.binding.backendName || proof.storageID != s.binding.storageID {
		return RetentionEntry{}, errors.New(
			"reaping retention proof belongs to another journal lineage",
		)
	}
	entry := proof.Entry()
	if entry.OriginalLeaseUUID == "" {
		return RetentionEntry{}, errors.New("reaping retention proof has no lease")
	}
	raw := tx.Bucket(retentionBucketName).Get([]byte(entry.OriginalLeaseUUID))
	if raw == nil {
		return RetentionEntry{}, ErrNoRetention
	}
	current := bytes.Clone(raw)
	if sha256.Sum256(current) != proof.digest || !bytes.Equal(current, proof.row) {
		return RetentionEntry{}, errors.New(
			"reaping retention generation changed before settlement",
		)
	}
	currentEntry, err := decodeRetentionEntry(current)
	if err != nil {
		return RetentionEntry{}, err
	}
	if currentEntry.Status != RetentionStatusReaping {
		return RetentionEntry{}, errors.New("retention record is no longer reaping")
	}
	return currentEntry, nil
}

func (s *RetentionStore) requireActiveCandidateTx(
	tx *bolt.Tx,
	candidate ActiveRetentionCandidate,
) (RetentionEntry, error) {
	if !candidate.Valid() || candidate.issuer != s || s.binding == nil ||
		candidate.backend != s.binding.backendName ||
		candidate.storageID != s.binding.storageID {
		return RetentionEntry{}, errors.New(
			"active retention candidate belongs to another journal lineage",
		)
	}
	entry := candidate.Entry()
	if entry.OriginalLeaseUUID == "" {
		return RetentionEntry{}, errors.New("active retention candidate has no lease")
	}
	raw := tx.Bucket(retentionBucketName).Get([]byte(entry.OriginalLeaseUUID))
	if raw == nil {
		return RetentionEntry{}, ErrNoRetention
	}
	current := bytes.Clone(raw)
	if sha256.Sum256(current) != candidate.digest || !bytes.Equal(current, candidate.row) {
		return RetentionEntry{}, errActiveRetentionChanged
	}
	currentEntry, err := decodeRetentionEntry(current)
	if err != nil {
		return RetentionEntry{}, err
	}
	if currentEntry.Status != RetentionStatusActive {
		return RetentionEntry{}, errors.New("retention record is no longer active")
	}
	return currentEntry, nil
}

// ListActiveCandidates returns opaque transition candidates for the exact
// Active rows in one MVCC snapshot. Read-only callers should continue to use
// List; destructive policy must carry one of these store-issued candidates.
func (s *RetentionStore) ListActiveCandidates() ([]ActiveRetentionCandidate, error) {
	return s.listActiveCandidates("")
}

// ListActiveCandidatesByTenant is the indexed-policy selector for cap
// eviction. It returns only exact Active rows for tenant.
func (s *RetentionStore) ListActiveCandidatesByTenant(
	tenant string,
) ([]ActiveRetentionCandidate, error) {
	if tenant == "" {
		return nil, errors.New("active retention candidate tenant is required")
	}
	return s.listActiveCandidates(tenant)
}

// ListTenantRetentionCandidates returns one tenant's complete read snapshot
// together with mutation authority only for the Active subset, from the same
// bbolt view. Partition policy can therefore observe Restoring rows without
// granting them destructive authority, while cap eviction consumes only exact
// Active candidates.
func (s *RetentionStore) ListTenantRetentionCandidates(
	tenant string,
) ([]RetentionEntry, []ActiveRetentionCandidate, error) {
	if tenant == "" {
		return nil, nil, errors.New("retention tenant is required")
	}
	if s == nil || s.boltStore == nil || s.binding == nil ||
		s.backendAuthorityGate == nil {
		return nil, nil, errors.New(
			"tenant retention candidates require an identity-bound journal",
		)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := make([]RetentionEntry, 0)
	candidates := make([]ActiveRetentionCandidate, 0)
	err := s.view(func(tx *bolt.Tx) error {
		return tx.Bucket(retentionBucketName).ForEach(func(key, raw []byte) error {
			entry, err := decodeRetentionEntry(raw)
			if err != nil {
				return fmt.Errorf("malformed retention record %q: %w", string(key), err)
			}
			if entry.Tenant != tenant {
				return nil
			}
			if err := validateAuthoritativeRetentionIdentity(key, &entry); err != nil {
				return fmt.Errorf("invalid retention record %q: %w", string(key), err)
			}
			if err := validateRetentionEntryResourceProfiles(&entry); err != nil {
				return fmt.Errorf("invalid retention record %q: %w", string(key), err)
			}
			if err := validateRetentionSourceAuthorityForBinding(&entry); err != nil {
				return fmt.Errorf("invalid retention record %q: %w", string(key), err)
			}
			entries = append(entries, entry)
			if entry.Status != RetentionStatusActive {
				return nil
			}
			candidate, err := s.mintActiveCandidate(raw)
			if err != nil {
				return err
			}
			candidates = append(candidates, candidate)
			return nil
		})
	})
	return entries, candidates, err
}

// ListExpiredCandidates returns exact Active rows which were already expired
// in the selector snapshot. BeginExpiredReaping repeats the age check while
// consuming the candidate, so clock policy and durable authority stay atomic.
func (s *RetentionStore) ListExpiredCandidates(
	maxAge time.Duration,
) ([]ActiveRetentionCandidate, error) {
	if maxAge <= 0 {
		return []ActiveRetentionCandidate{}, nil
	}
	cutoff := time.Now().Add(-maxAge)
	return s.selectActiveCandidates(func(entry RetentionEntry) bool {
		return entry.CreatedAt.Before(cutoff)
	})
}

func (s *RetentionStore) listActiveCandidates(
	tenant string,
) ([]ActiveRetentionCandidate, error) {
	return s.selectActiveCandidates(func(entry RetentionEntry) bool {
		return tenant == "" || entry.Tenant == tenant
	})
}

func (s *RetentionStore) selectActiveCandidates(
	keep func(RetentionEntry) bool,
) ([]ActiveRetentionCandidate, error) {
	if s == nil || s.boltStore == nil || s.binding == nil ||
		s.backendAuthorityGate == nil {
		return nil, errors.New(
			"active retention candidates require an identity-bound journal",
		)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]ActiveRetentionCandidate, 0)
	err := s.view(func(tx *bolt.Tx) error {
		return tx.Bucket(retentionBucketName).ForEach(func(key, raw []byte) error {
			entry, err := decodeRetentionEntry(raw)
			if err != nil {
				return fmt.Errorf("malformed retention record %q: %w", string(key), err)
			}
			if entry.Status != RetentionStatusActive || !keep(entry) {
				return nil
			}
			candidate, err := s.mintActiveCandidate(raw)
			if err != nil {
				return err
			}
			result = append(result, candidate)
			return nil
		})
	})
	return result, err
}

// ProveRestoringSnapshot upgrades a read-only DTO to mutation authority only
// when its complete canonical encoding is still the exact durable Restoring
// row. This is the recovery bridge from ListRestoring/Get without reopening a
// caller-authored generation or callback route as a mutation API.
func (s *RetentionStore) ProveRestoringSnapshot(
	expected RetentionEntry,
) (RestoringRetentionProof, error) {
	if expected.Status != RetentionStatusRestoring {
		return RestoringRetentionProof{}, ErrNotRestorable
	}
	expectedRaw, err := marshalRetentionEntry(expected)
	if err != nil {
		return RestoringRetentionProof{}, fmt.Errorf("encode expected restoring retention: %w", err)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var proof RestoringRetentionProof
	err = s.view(func(tx *bolt.Tx) error {
		raw := tx.Bucket(retentionBucketName).Get([]byte(expected.OriginalLeaseUUID))
		if raw == nil {
			return ErrNoRetention
		}
		if !bytes.Equal(raw, expectedRaw) {
			return errors.New("restoring retention snapshot is stale")
		}
		proof, err = s.mintRestoringProof(raw)
		return err
	})
	return proof, err
}

// ListReapingProofs returns exact cleanup authority for every Reaping row in
// one MVCC snapshot. Restart recovery must select work through this method: a
// caller-supplied lease UUID is inspection data, not mutation authority.
func (s *RetentionStore) ListReapingProofs() ([]ReapingRetentionProof, error) {
	if s == nil || s.boltStore == nil || s.binding == nil ||
		s.backendAuthorityGate == nil {
		return nil, errors.New(
			"reaping retention proofs require an identity-bound journal",
		)
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	proofs := make([]ReapingRetentionProof, 0)
	err := s.view(func(tx *bolt.Tx) error {
		return tx.Bucket(retentionBucketName).ForEach(func(key, raw []byte) error {
			entry, err := decodeRetentionEntry(raw)
			if err != nil {
				return fmt.Errorf("malformed retention record %q: %w", string(key), err)
			}
			if entry.Status != RetentionStatusReaping {
				return nil
			}
			proof, err := s.mintReapingProof(raw)
			if err != nil {
				return err
			}
			proofs = append(proofs, proof)
			return nil
		})
	})
	return proofs, err
}

// BeginReaping atomically changes the candidate's exact Active row to Reaping and
// returns the resulting tombstone capability. State, generation, timestamps,
// and immutable authority are derived inside the store.
func (s *RetentionStore) BeginReaping(
	candidate ActiveRetentionCandidate,
) (ReapingRetentionProof, bool, error) {
	return s.beginReaping(candidate, 0, false)
}

// BeginExpiredReaping is BeginReaping with an in-transaction age gate. The
// caller supplies policy only; the store derives and proves the exact state.
func (s *RetentionStore) BeginExpiredReaping(
	candidate ActiveRetentionCandidate,
	maxAge time.Duration,
) (ReapingRetentionProof, bool, error) {
	if maxAge <= 0 {
		return ReapingRetentionProof{}, false, nil
	}
	return s.beginReaping(candidate, maxAge, true)
}

func (s *RetentionStore) beginReaping(
	candidate ActiveRetentionCandidate,
	maxAge time.Duration,
	requireExpired bool,
) (ReapingRetentionProof, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		proof ReapingRetentionProof
		oldE  RetentionEntry
		newE  RetentionEntry
		ok    bool
	)
	err := s.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(retentionBucketName)
		var err error
		oldE, err = s.requireActiveCandidateTx(tx, candidate)
		if err != nil {
			if errors.Is(err, ErrNoRetention) || errors.Is(err, errActiveRetentionChanged) {
				return nil
			}
			return err
		}
		if requireExpired && time.Since(oldE.CreatedAt) < maxAge {
			return nil
		}
		newE = oldE
		newE.Status = RetentionStatusReaping
		newE.ReapingSince = time.Now()
		newE.Generation++
		encoded, err := marshalRetentionEntry(newE)
		if err != nil {
			return fmt.Errorf("marshal reaping retention record %q: %w", oldE.OriginalLeaseUUID, err)
		}
		proof, err = s.mintReapingProof(encoded)
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte(oldE.OriginalLeaseUUID), encoded); err != nil {
			return err
		}
		ok = true
		return nil
	})
	if err != nil {
		return ReapingRetentionProof{}, false, err
	}
	if ok {
		s.indexApply(oldE.OriginalLeaseUUID, &oldE, &newE)
	}
	return proof, ok, nil
}

// DeleteReaped consumes one exact Reaping proof after physical absence has
// been established. A copied proof cannot delete a rewritten or reopened row.
func (s *RetentionStore) DeleteReaped(proof ReapingRetentionProof) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		oldE    RetentionEntry
		deleted bool
	)
	err := s.update(func(tx *bolt.Tx) error {
		var err error
		oldE, err = s.requireReapingProofTx(tx, proof)
		if err != nil {
			if errors.Is(err, ErrNoRetention) {
				return nil
			}
			return err
		}
		deleted = true
		return tx.Bucket(retentionBucketName).Delete([]byte(oldE.OriginalLeaseUUID))
	})
	if err != nil {
		return false, err
	}
	if deleted {
		s.indexApply(oldE.OriginalLeaseUUID, &oldE, nil)
	}
	return deleted, nil
}

// DeleteRestoring consumes one exact restore-finalizer proof. Absence is an
// idempotent success; a surviving different row is a stale-authority error.
func (s *RetentionStore) DeleteRestoring(proof RestoringRetentionProof) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		oldE    RetentionEntry
		deleted bool
	)
	err := s.update(func(tx *bolt.Tx) error {
		var err error
		oldE, err = s.requireRestoringProofTx(tx, proof)
		if err != nil {
			if errors.Is(err, ErrNoRetention) {
				return nil
			}
			return err
		}
		deleted = true
		return tx.Bucket(retentionBucketName).Delete([]byte(oldE.OriginalLeaseUUID))
	})
	if err != nil {
		return false, err
	}
	if deleted {
		s.indexApply(oldE.OriginalLeaseUUID, &oldE, nil)
	}
	return deleted, nil
}

// RollbackRestoring consumes one exact restore-finalizer proof and publishes
// the source Active row only after validating its physical quota authority.
func (s *RetentionStore) RollbackRestoring(
	proof RestoringRetentionProof,
	resourceProfiles []SKUResourceSnapshot,
) (ActiveRetentionProof, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		active ActiveRetentionProof
		oldE   RetentionEntry
		newE   RetentionEntry
	)
	err := s.update(func(tx *bolt.Tx) error {
		var err error
		oldE, err = s.requireRestoringProofTx(tx, proof)
		if err != nil {
			return err
		}
		if err := ValidateSKUResourceSnapshot(oldE.Items, resourceProfiles); err != nil {
			return fmt.Errorf(
				"invalid rollback resource profiles for retention record %q: %w",
				oldE.OriginalLeaseUUID, err,
			)
		}
		if len(oldE.ResourceProfiles) > 0 &&
			!slices.Equal(oldE.ResourceProfiles, resourceProfiles) {
			return fmt.Errorf(
				"rollback resource profiles differ from durable retention record %q",
				oldE.OriginalLeaseUUID,
			)
		}
		newE = oldE
		newE.ResourceProfiles = CloneSKUResourceSnapshot(resourceProfiles)
		newE.Status = RetentionStatusActive
		newE.Generation++
		newE.NewLeaseUUID = ""
		newE.DestinationItems = nil
		newE.DestinationResourceProfiles = nil
		newE.DestinationOperationID = OperationID{}
		newE.DestinationCallbackURL = ""
		newE.DestinationLifecycleCallbackURL = ""
		newE.RestoringSince = time.Time{}
		if err := validateRetentionEntryResourceProfiles(&newE); err != nil {
			return fmt.Errorf("invalid reverted retention record %q: %w", oldE.OriginalLeaseUUID, err)
		}
		encoded, err := marshalRetentionEntry(newE)
		if err != nil {
			return fmt.Errorf("marshal reverted retention record %q: %w", oldE.OriginalLeaseUUID, err)
		}
		if err := tx.Bucket(retentionBucketName).Put(
			[]byte(oldE.OriginalLeaseUUID), encoded,
		); err != nil {
			return err
		}
		active, err = s.activeProofFromRaw(encoded)
		return err
	})
	if err != nil {
		return ActiveRetentionProof{}, err
	}
	s.indexApply(oldE.OriginalLeaseUUID, &oldE, &newE)
	return active, nil
}

func (s *RetentionStore) activeProofFromRaw(raw []byte) (ActiveRetentionProof, error) {
	entry, err := decodeRetentionEntry(raw)
	if err != nil {
		return ActiveRetentionProof{}, err
	}
	if entry.Status != RetentionStatusActive || s.binding == nil {
		return ActiveRetentionProof{}, errors.New("retention row is not active authority")
	}
	if err := validateAuthoritativeRetentionIdentity(
		[]byte(entry.OriginalLeaseUUID), &entry,
	); err != nil {
		return ActiveRetentionProof{}, err
	}
	if err := validateRetentionEntryResourceProfiles(&entry); err != nil {
		return ActiveRetentionProof{}, err
	}
	if err := validateRetentionSourceAuthorityForBinding(&entry); err != nil {
		return ActiveRetentionProof{}, err
	}
	manifestBytes, err := json.Marshal(entry.StackManifest)
	if err != nil {
		return ActiveRetentionProof{}, err
	}
	return ActiveRetentionProof{
		issuer: s, leaseUUID: entry.OriginalLeaseUUID,
		backend: s.binding.backendName, storageID: s.binding.storageID,
		generation: entry.Generation, tenant: entry.Tenant,
		providerUUID: entry.ProviderUUID, items: slices.Clone(entry.Items),
		resourceProfiles: CloneSKUResourceSnapshot(entry.ResourceProfiles),
		retainedVolumes:  slices.Clone(entry.RetainedVolumeNames),
		manifest:         manifestBytes,
		callbackURL:      entry.CallbackURL,
		digest:           sha256.Sum256(raw),
	}, nil
}

// pruneOrphanedActive is intentionally available only to the construction-bound
// RetentionOrphanPruner. Restoring, Reaping, and rewritten generations are
// structurally outside this transition; external callers cannot choose a row.
func (s *RetentionStore) pruneOrphanedActive(
	candidate ActiveRetentionCandidate,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		oldE    RetentionEntry
		deleted bool
	)
	err := s.update(func(tx *bolt.Tx) error {
		var err error
		oldE, err = s.requireActiveCandidateTx(tx, candidate)
		if err != nil {
			if errors.Is(err, ErrNoRetention) || errors.Is(err, errActiveRetentionChanged) {
				return nil
			}
			return err
		}
		deleted = true
		return tx.Bucket(retentionBucketName).Delete([]byte(oldE.OriginalLeaseUUID))
	})
	if err != nil {
		return false, err
	}
	if deleted {
		s.indexApply(oldE.OriginalLeaseUUID, &oldE, nil)
	}
	return deleted, nil
}
