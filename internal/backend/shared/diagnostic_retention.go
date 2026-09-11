package shared

import (
	"bytes"
	"errors"

	bolt "go.etcd.io/bbolt"
)

// captureAttemptLocked keeps only the visible failure, the exact attempt which
// can still publish through the current head, and this physical capture. Older
// evidence has no public history API and cannot consume disk per retry forever.
func (diagnostics *FailureDiagnostics) captureAttemptLocked(identity diagnosticAttemptIdentity, observation FailureDiagnosticObservation) (FailureDiagnosticCapture, error) {
	if err := diagnostics.pruneAttemptCapturesLocked(identity, true); err != nil {
		return FailureDiagnosticCapture{}, err
	}
	capture, err := diagnostics.store.captureAttempt(identity, observation)
	if err != nil {
		return FailureDiagnosticCapture{}, err
	}
	capture.owner = diagnostics
	return capture, nil
}

// RetireHistoricalCapture is used after an exact captured cohort is removed.
// Retirement permission is derived afresh from the durable aggregate head;
// callers cannot select a timestamp or assert that a capture is obsolete.
func (diagnostics *FailureDiagnostics) RetireHistoricalCapture(capture FailureDiagnosticCapture) error {
	if !diagnostics.valid() || capture.owner != diagnostics || capture.store != diagnostics.store || !capture.identity.valid() {
		return errors.New("diagnostic retirement requires this store's capture")
	}
	unlock := diagnostics.operations.lockLease(capture.identity.LeaseUUID)
	defer unlock()
	return diagnostics.pruneAttemptCapturesLocked(capture.identity, false)
}

func (diagnostics *FailureDiagnostics) pruneAttemptCapturesLocked(candidate diagnosticAttemptIdentity, retainCandidate bool) error {
	var head leaseMutationHead
	var present bool
	var completedOperations []operationCompletionRecord
	var completedMaintenance []maintenanceCompletionRecord
	err := diagnostics.operations.callbacks.view(func(tx *bolt.Tx) error {
		var err error
		head, present, err = getLeaseMutationHeadTx(tx, candidate.LeaseUUID)
		if err != nil || present {
			return err
		}
		// A completed live lease legitimately has no mutable head. Its
		// permanent receipts positively prove which exact attempts can no
		// longer publish; bare journal absence remains insufficient.
		completedOperations, err = listOperationHistoryTx(tx, candidate.LeaseUUID)
		if err != nil {
			return err
		}
		completedMaintenance, err = listMaintenanceReceiptsTx(tx, candidate.LeaseUUID)
		return err
	})
	if err != nil {
		return err
	}
	if !present && len(completedOperations) == 0 && len(completedMaintenance) == 0 {
		return nil
	}
	return diagnostics.store.update(func(tx *bolt.Tx) error {
		published := tx.Bucket(diagnosticPublicationsBucketName).Get([]byte(candidate.LeaseUUID))
		bucket := tx.Bucket(attemptDiagnosticsBucketName)
		cursor := bucket.Cursor()
		prefix := []byte(candidate.keyPrefix())
		for key, value := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
			if bytes.Equal(key, published) || retainCandidate && string(key) == candidate.key() {
				continue
			}
			record, err := decodeAttemptDiagnostic(value)
			if err != nil {
				return err
			}
			if diagnosticCanPublishThroughHead(record.Identity, head) {
				continue
			}
			if !present && !diagnosticHasCompletedReceipt(record.Identity, completedOperations, completedMaintenance) {
				continue
			}
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
		return nil
	})
}

func diagnosticHasCompletedReceipt(identity diagnosticAttemptIdentity, operations []operationCompletionRecord, maintenance []maintenanceCompletionRecord) bool {
	if identity.Kind == "operation" {
		for _, receipt := range operations {
			if receipt.OperationID.String() == identity.ID && receipt.LeaseUUID == identity.LeaseUUID &&
				receipt.Backend == identity.Backend && receipt.BackendStorageID == identity.StorageID &&
				receipt.Tenant == identity.Tenant && receipt.ProviderUUID == identity.ProviderUUID {
				return true
			}
		}
	}
	if identity.Kind == "maintenance" {
		for _, receipt := range maintenance {
			if receipt.MaintenanceID.String() == identity.ID && receipt.LeaseUUID == identity.LeaseUUID &&
				receipt.Backend == identity.Backend && receipt.BackendStorageID == identity.StorageID &&
				receipt.Tenant == identity.Tenant && receipt.ProviderUUID == identity.ProviderUUID &&
				(identity.ReleaseVersion == 0 || receipt.TargetReleaseVersion == identity.ReleaseVersion) {
				return true
			}
		}
	}
	return false
}

func diagnosticCanPublishThroughHead(identity diagnosticAttemptIdentity, head leaseMutationHead) bool {
	switch head := head.(type) {
	case operationLeaseMutationHead:
		return head.claim.entry.State == operationIntentPending && identity.Kind == "operation" && identity.ID == head.claim.OperationID().String()
	case maintenanceLeaseMutationHead:
		return identity.Kind == "maintenance" && identity.ID == head.claim.MaintenanceID().String()
	case closeLeaseMutationHead:
		return identity.Kind == "operation" && identity.ID == head.claim.InterruptedOperationID().String() ||
			identity.Kind == "maintenance" && identity.ID == head.claim.InterruptedMaintenanceID().String()
	default:
		return false
	}
}
