package shared

import (
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// Frozen pre-ENG1006 traversal: a differential oracle for validation reuse.
// Keep repeated reads here so tests can detect an omitted production pass.
func referenceCallbackHealth(s *CallbackStore) error {
	return s.view(func(tx *bolt.Tx) error {
		if err := requireCompleteCallbackSchema(tx); err != nil {
			return err
		}
		if err := visitImageInspectionsTx(tx, func(r imageInspectionRecord) error {
			name, storage := s.journalBackendIdentity("")
			if r.Backend != name || r.StorageID != storage.String() {
				return errors.New("image inspection receipt belongs to another storage lineage")
			}
			return nil
		}); err != nil {
			return err
		}
		if err := validateMaintenanceCompensationsTx(tx); err != nil {
			return err
		}
		if err := (&VolumeLaunchJournal{store: s}).validateTx(tx); err != nil {
			return err
		}
		heads := tx.Bucket(callbackLeaseMutationHeadBucketName)
		if err := heads.ForEach(func(key, value []byte) error {
			if value == nil {
				return fmt.Errorf("callback lease mutation head %q is a nested bucket", key)
			}
			_, err := decodeLeaseMutationHead(key, value)
			return err
		}); err != nil {
			return err
		}
		if err := validateLeaseMutationUUIDSlotsTx(tx); err != nil {
			return err
		}
		if err := referenceCallbackReceiptStateTx(tx); err != nil {
			return err
		}
		if err := validateCallbackQueueTx(tx); err != nil {
			return fmt.Errorf("callback queue unhealthy: %w", err)
		}
		return nil
	})
}

func referenceCallbackReceiptStateTx(tx *bolt.Tx) error {
	operationReservations, err := referenceOperationHistoryTx(tx)
	if err != nil {
		return err
	}
	maintenanceReservations, err := referenceMaintenanceHistoryTx(tx)
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

func referenceOperationHistoryTx(tx *bolt.Tx) (uint64, error) {
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

func referenceMaintenanceHistoryTx(tx *bolt.Tx) (uint64, error) {
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
