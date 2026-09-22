package shared

import (
	"context"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// callbackReceiptValidation owns a complete traversal of one immutable read
// transaction. Each lease's strictly decoded head and histories are used
// together, then discarded. No decoded payload survives into another lease or
// request, and no result grants mutation authority.
type callbackReceiptValidation struct {
	ctx                 context.Context
	tx                  *bolt.Tx
	heads               *bolt.Bucket
	operations          *bolt.Bucket
	maintenance         *bolt.Bucket
	operationReserved   uint64
	maintenanceReserved uint64
}

func newCallbackReceiptValidation(ctx context.Context, tx *bolt.Tx) (*callbackReceiptValidation, error) {
	if tx == nil || tx.Writable() {
		return nil, errors.New("callback receipt validation requires an immutable read transaction")
	}
	validation := &callbackReceiptValidation{
		ctx:         ctx,
		tx:          tx,
		heads:       tx.Bucket(callbackLeaseMutationHeadBucketName),
		operations:  tx.Bucket(callbackOperationHistoryBucketName),
		maintenance: tx.Bucket(callbackMaintenanceHistoryBucketName),
	}
	if validation.heads == nil || validation.operations == nil || validation.maintenance == nil {
		return nil, errors.New("callback receipt validation requires heads and both history buckets")
	}
	return validation, nil
}

func (validation *callbackReceiptValidation) validate() error {
	if err := walkCallbackValidationRows(validation.ctx, validation.heads, func(key, value []byte) error {
		if value == nil {
			return fmt.Errorf("callback lease mutation head %q is a nested bucket", key)
		}
		head, err := decodeLeaseMutationHead(key, value)
		if err != nil {
			return err
		}
		return validation.validateLease(string(key), head)
	}); err != nil {
		return err
	}
	// Histories can outlive their head. Validate every root member's shape and
	// key, then visit only leases not already decoded by an earlier root.
	if err := walkCallbackValidationRows(validation.ctx, validation.operations, func(key, value []byte) error {
		if value != nil {
			return fmt.Errorf("completed operation history %q is not a nested bucket", key)
		}
		if err := validateCanonicalLeaseUUID(string(key)); err != nil {
			return fmt.Errorf("completed operation history has invalid lease key: %w", err)
		}
		if validation.heads.Get(key) != nil {
			return nil
		}
		return validation.validateLease(string(key), nil)
	}); err != nil {
		return err
	}
	if err := walkCallbackValidationRows(validation.ctx, validation.maintenance, func(key, value []byte) error {
		if value != nil {
			return fmt.Errorf("completed maintenance history %q is not a nested bucket", key)
		}
		if err := validateCanonicalLeaseUUID(string(key)); err != nil {
			return err
		}
		if validation.heads.Get(key) != nil || validation.operations.Bucket(key) != nil {
			return nil
		}
		return validation.validateLease(string(key), nil)
	}); err != nil {
		return err
	}
	stored, err := callbackReceiptReservationCountTx(validation.tx)
	if err != nil {
		return err
	}
	if stored != validation.operationReserved+validation.maintenanceReserved {
		return fmt.Errorf("callback receipt reservation count mismatch: stored=%d operation=%d maintenance=%d",
			stored, validation.operationReserved, validation.maintenanceReserved)
	}
	if stored > maxCallbackReceiptReservationsGlobal {
		return fmt.Errorf("global callback receipt capacity exceeded: %d > %d", stored, maxCallbackReceiptReservationsGlobal)
	}
	return nil
}

func (validation *callbackReceiptValidation) validateLease(leaseUUID string, head leaseMutationHead) error {
	operations, err := listOperationHistoryContextTx(validation.ctx, validation.tx, leaseUUID)
	if err != nil {
		return err
	}
	validation.operationReserved += uint64(len(operations))
	if operation, ok := head.(operationLeaseMutationHead); ok && operation.claim.entry.State == operationIntentPending {
		validation.operationReserved++
	}
	if len(operations) != 0 {
		completed := operations[0]
		switch state := head.(type) {
		case operationLeaseMutationHead:
			if !operationHistoryMatchesAuthority(completed, *state.claim.entry) {
				return fmt.Errorf("operation state for lease %q crosses completed-history authority", leaseUUID)
			}
		case maintenanceLeaseMutationHead:
			if completed.Backend != state.claim.Backend() || completed.BackendStorageID != state.claim.BackendStorageID().String() ||
				completed.Tenant != state.claim.Tenant() || completed.ProviderUUID != state.claim.ProviderUUID() {
				return fmt.Errorf("maintenance state for lease %q crosses completed-history authority", leaseUUID)
			}
		case closeLeaseMutationHead:
			if completed.Backend != state.claim.Backend() || completed.BackendStorageID != state.claim.BackendStorageID().String() ||
				(!state.claim.CleanupOnly() && (completed.Tenant != state.claim.Tenant() || completed.ProviderUUID != state.claim.ProviderUUID())) {
				return fmt.Errorf("close state for lease %q crosses completed-history authority", leaseUUID)
			}
		case closedLeaseMutationHead:
			if completed.Backend != state.entry.Backend || completed.BackendStorageID != state.entry.BackendStorageID {
				return fmt.Errorf("closed state for lease %q crosses completed-history authority", leaseUUID)
			}
		}
	}
	maintenance, err := listMaintenanceReceiptsContextTx(validation.ctx, validation.tx, leaseUUID)
	if err != nil {
		return err
	}
	if len(maintenance) > maxMaintenanceReceiptsPerLease {
		return fmt.Errorf("maintenance receipt capacity exceeded for lease %q", leaseUUID)
	}
	validation.maintenanceReserved += uint64(len(maintenance))
	sequences := make(map[uint64]struct{}, len(maintenance))
	for index, record := range maintenance {
		if _, duplicate := sequences[record.CompletionSequence]; duplicate {
			return fmt.Errorf("maintenance history for lease %q repeats completion sequence %d", leaseUUID, record.CompletionSequence)
		}
		sequences[record.CompletionSequence] = struct{}{}
		if index > 0 && (record.Backend != maintenance[0].Backend || record.BackendStorageID != maintenance[0].BackendStorageID ||
			record.Tenant != maintenance[0].Tenant || record.ProviderUUID != maintenance[0].ProviderUUID) {
			return fmt.Errorf("maintenance history for lease %q crosses backend storage or principal authority", leaseUUID)
		}
	}
	if current, ok := head.(maintenanceLeaseMutationHead); ok {
		validation.maintenanceReserved++
		if len(maintenance) >= maxMaintenanceReceiptsPerLease {
			return fmt.Errorf("maintenance head for lease %q has no reserved receipt capacity", leaseUUID)
		}
		for _, record := range maintenance {
			if !maintenanceReceiptMatchesEntryAuthority(record, current.claim.entry) {
				return fmt.Errorf("maintenance head for lease %q crosses completed-history authority", leaseUUID)
			}
		}
	}
	return nil
}
