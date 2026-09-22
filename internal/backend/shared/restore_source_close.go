package shared

import (
	"errors"
	"fmt"
)

// ReadRestorableSource is a tenant-scoped admission preflight, not a source
// transfer capability. It avoids creating a doomed destination operation when
// the source is already known to have a pending close. ClaimForRestore repeats
// the source check atomically with transfer after durable destination admission.
func (s *RestoreSettlement) ReadRestorableSource(sourceLeaseUUID, tenant string) (*RetentionEntry, error) {
	if !s.valid() {
		return nil, errors.New("restore settlement is invalid")
	}
	unlock := s.operations.lockLease(sourceLeaseUUID)
	defer unlock()
	entry, err := s.retentions.Get(sourceLeaseUUID)
	if err != nil {
		return nil, err
	}
	if entry == nil || entry.Tenant != tenant {
		return nil, ErrNoRetention
	}
	if _, pending, err := s.operations.callbacks.getCloseIntentLocked(sourceLeaseUUID); err != nil {
		return nil, err
	} else if pending {
		return nil, fmt.Errorf("%w: source close is still pending", ErrNotRestorable)
	}
	return entry, nil
}

// InterruptedRestoreSource binds a recovery-only restore subject to the exact
// retained close which still owns its source namespace. It permits observation
// of the recorded names, never deletion or terminal settlement. Docker must
// independently prove quiescence and re-attest this proof inside its guarded
// namespace mutation before returning source-canonical bytes to retention.
type InterruptedRestoreSource struct {
	settlement *RestoreSettlement
	subject    OperationPhysicalSubject
	close      CloseIntentClaim
	retention  RestoringRetentionProof
}

// ProveInterruptedSourceClose recognizes the record-first close/restore overlap
// admitted by older versions. Fresh ClaimForRestore excludes this state. The
// proof cannot be minted from a raw source ID or a caller-authored retention row.
func (s *RestoreSettlement) ProveInterruptedSourceClose(
	subject OperationPhysicalSubject,
) (InterruptedRestoreSource, error) {
	if !s.valid() || !subject.validFor(s.operations) || !subject.RecoveryCleanup() ||
		subject.Intent().Kind() != OperationIntentRestore {
		return InterruptedRestoreSource{}, errors.New("interrupted source requires exact restore cleanup authority")
	}
	intent := subject.Intent()
	unlock := s.operations.lockRestoreLeases(intent.SourceLeaseUUID(), intent.LeaseUUID())
	defer unlock()
	if err := s.operations.callbacks.requireCurrentOperationClaim(intent); err != nil {
		return InterruptedRestoreSource{}, err
	}
	close, found, err := s.operations.callbacks.getCloseIntentLocked(intent.SourceLeaseUUID())
	if err != nil {
		return InterruptedRestoreSource{}, err
	}
	if !found || !close.RetainOnClose() || close.CleanupOnly() {
		return InterruptedRestoreSource{}, errors.New("restore source has no pending retained close")
	}
	entry, err := s.retentions.Get(intent.SourceLeaseUUID())
	if err != nil {
		return InterruptedRestoreSource{}, err
	}
	if entry == nil || entry.Status != RetentionStatusRestoring ||
		entry.NewLeaseUUID != intent.LeaseUUID() || entry.Generation != intent.SourceGeneration() ||
		entry.DestinationOperationID != intent.OperationID() || entry.Tenant != intent.Tenant() ||
		entry.ProviderUUID != intent.ProviderUUID() {
		return InterruptedRestoreSource{}, errors.New("interrupted source restore generation changed")
	}
	if err := retentionEntryMatchesClose(*entry, close); err != nil {
		return InterruptedRestoreSource{}, fmt.Errorf("interrupted source close: %w", err)
	}
	retention, err := s.retentions.ProveRestoringSnapshot(*entry)
	if err != nil {
		return InterruptedRestoreSource{}, err
	}
	return InterruptedRestoreSource{settlement: s, subject: subject, close: close, retention: retention}, nil
}

// RetainedVolumeNames returns detached immutable source observations.
func (proof InterruptedRestoreSource) RetainedVolumeNames() []string {
	if proof.settlement == nil || !proof.retention.Valid() {
		return nil
	}
	return proof.retention.Entry().RetainedVolumeNames
}

// ReattestFor re-reads both exact journal heads and the whole source record.
// Its caller holds the namespace reservation across this check and the rename;
// a copied proof cannot survive a retry generation, changed row, or store reopen.
func (proof InterruptedRestoreSource) ReattestFor(subject OperationPhysicalSubject) error {
	if proof.settlement == nil || proof.subject != subject || !proof.retention.Valid() {
		return errors.New("interrupted source proof belongs to another restore")
	}
	current, err := proof.settlement.ProveInterruptedSourceClose(subject)
	if err != nil {
		return err
	}
	if current.close.digest != proof.close.digest || current.retention.digest != proof.retention.digest {
		return errors.New("interrupted source authority changed before namespace repair")
	}
	return nil
}
