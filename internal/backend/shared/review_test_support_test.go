package shared

import (
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

// Historical and malformed fixtures deliberately bypass typed authority only in tests.

// Delete removes a RetentionEntry by original lease UUID. It is idempotent:
// no error is returned when the entry is absent. It reads the pre-image in-txn
// so the index can drop the deleted record's partition membership.
func (s *RetentionStore) deleteUnsafe(orig string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldE *RetentionEntry
	err := s.update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		if raw := bkt.Get([]byte(orig)); raw != nil {
			oldE = &RetentionEntry{}
			if uerr := unmarshalRetentionEntry(raw, oldE); uerr != nil {
				return fmt.Errorf("malformed retention record %q: %w", orig, uerr)
			}
		}
		return bkt.Delete([]byte(orig))
	})
	if err != nil {
		return err
	}
	s.indexApply(orig, oldE, nil) // oldE=nil when absent → no-op
	return nil
}

func (mailbox *callbackReplayMailbox) pendingCount() int {
	if mailbox == nil {
		return 0
	}
	mailbox.mu.Lock()
	defer mailbox.mu.Unlock()
	return len(mailbox.pending)
}

func (inspection ReleaseStoreInspection) checkLegacyActiveAuthorityCapacityWithinLimit(
	leaseUUID string,
	expected Release,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
	limitBytes int,
) error {
	return inspection.checkLegacyActiveAuthorityAndRuntimeCapacityWithinLimit(
		leaseUUID,
		expected,
		items,
		resourceProfiles,
		nil,
		limitBytes,
	)
}
