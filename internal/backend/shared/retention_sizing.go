package shared

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendidentity"
)

var errUnfrozenSizingChanged = errors.New("retention record changed after its unfrozen-sizing candidate was issued")

// UnfrozenSizingCandidate is store-issued authority to freeze immutable sizing
// onto one exact retention row that carries none. Only rows written before
// retention records persisted a resource snapshot (v0.13) lack one, and the
// store issues a candidate for no other row. Freezing re-attests the complete
// durable row, so sizing is added once, to the row that was inspected, and is
// never replaced.
type UnfrozenSizingCandidate struct {
	issuer    *RetentionStore
	backend   string
	storageID backendidentity.ID
	row       []byte
	digest    [sha256.Size]byte
}

// Valid reports whether the candidate has a complete opaque shape. It does not
// establish current authority; FreezeResourceProfiles re-attests the row.
func (candidate UnfrozenSizingCandidate) Valid() bool {
	return retentionProofShape(
		candidate.issuer, candidate.backend, candidate.storageID,
		candidate.row, candidate.digest,
	)
}

// Entry returns a detached read DTO. Mutating the result cannot alter the
// candidate or authorize another row.
func (candidate UnfrozenSizingCandidate) Entry() RetentionEntry {
	entry, err := decodeRetentionEntry(candidate.row)
	if err != nil {
		return RetentionEntry{}
	}
	return entry
}

// ListUnfrozenSizingCandidates returns a candidate for every row, in any
// status, that carries no ResourceProfiles, from one MVCC snapshot.
func (s *RetentionStore) ListUnfrozenSizingCandidates() ([]UnfrozenSizingCandidate, error) {
	if s == nil || s.boltStore == nil || s.binding == nil ||
		s.backendAuthorityGate == nil {
		return nil, errors.New("unfrozen sizing candidates require an identity-bound journal")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []UnfrozenSizingCandidate
	err := s.view(func(tx *bolt.Tx) error {
		return tx.Bucket(retentionBucketName).ForEach(func(key, raw []byte) error {
			entry, err := decodeRetentionEntry(raw)
			if err != nil {
				return fmt.Errorf("malformed retention record %q: %w", string(key), err)
			}
			if len(entry.ResourceProfiles) > 0 {
				return nil
			}
			if err := validateAuthoritativeRetentionIdentity(key, &entry); err != nil {
				return fmt.Errorf("validate unfrozen retention record %q: %w", string(key), err)
			}
			row := bytes.Clone(raw)
			result = append(result, UnfrozenSizingCandidate{
				issuer: s, backend: s.binding.backendName, storageID: s.binding.storageID,
				row: row, digest: sha256.Sum256(row),
			})
			return nil
		})
	})
	return result, err
}

// FreezeResourceProfiles writes resourceProfiles onto candidate's exact row as
// its immutable sizing authority. It refuses a candidate from another journal
// lineage, a row that changed after the candidate was issued (which includes a
// row that has since gained sizing), and a snapshot that does not exactly
// cover the row's items. Status, generation and every other field are left as
// they were: like a close retry's backfill, this adds missing authority and
// does not start a new generation.
func (s *RetentionStore) FreezeResourceProfiles(
	candidate UnfrozenSizingCandidate,
	resourceProfiles []SKUResourceSnapshot,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.update(func(tx *bolt.Tx) error {
		if !candidate.Valid() || candidate.issuer != s || s.binding == nil ||
			candidate.backend != s.binding.backendName ||
			candidate.storageID != s.binding.storageID {
			return errors.New("unfrozen sizing candidate belongs to another journal lineage")
		}
		entry := candidate.Entry()
		bucket := tx.Bucket(retentionBucketName)
		raw := bucket.Get([]byte(entry.OriginalLeaseUUID))
		if raw == nil {
			return ErrNoRetention
		}
		if sha256.Sum256(raw) != candidate.digest || !bytes.Equal(raw, candidate.row) {
			return errUnfrozenSizingChanged
		}
		if err := ValidateSKUResourceSnapshot(entry.Items, resourceProfiles); err != nil {
			return fmt.Errorf("freeze resource profiles for retention record %q: %w", entry.OriginalLeaseUUID, err)
		}
		entry.ResourceProfiles = CloneSKUResourceSnapshot(resourceProfiles)
		if err := validateRetentionEntryResourceProfiles(&entry); err != nil {
			return fmt.Errorf("validate frozen retention record %q: %w", entry.OriginalLeaseUUID, err)
		}
		encoded, err := marshalRetentionEntry(entry)
		if err != nil {
			return fmt.Errorf("marshal frozen retention record %q: %w", entry.OriginalLeaseUUID, err)
		}
		return bucket.Put([]byte(entry.OriginalLeaseUUID), encoded)
	})
}
