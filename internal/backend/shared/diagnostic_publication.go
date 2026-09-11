package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

// FailureDiagnosticPublication carries one attempt's durable capture and the
// exact terminal proof needed to make it tenant-visible. Neither capture nor a
// historical cleanup receipt alone can construct this value. Consumption
// rechecks the current journal head under the same lease gate as admission and
// close; a copied reference cannot publish after a newer command takes over.
type FailureDiagnosticPublication struct {
	owner       *FailureDiagnostics
	capture     FailureDiagnosticCapture
	operation   OperationReleaseUncommitted
	maintenance MaintenanceReleaseFailure
}

func (publication FailureDiagnosticPublication) Valid() bool {
	return publication.owner.valid() && publication.capture.Valid() &&
		(publication.operation.Valid() != publication.maintenance.Valid())
}

func (publication FailureDiagnosticPublication) Snapshot() (DiagnosticEntry, DiagnosticCaptureStatus, error) {
	if !publication.Valid() {
		return DiagnosticEntry{}, "", errors.New("failure diagnostic publication is invalid")
	}
	return publication.capture.Snapshot()
}

// OperationFailure prepares an opaque diagnostic handoff. A pre-effect refusal
// may have no containers to capture; it still gets an explicit unavailable
// observation. Existing captures retain their original cause and logs.
func (diagnostics *FailureDiagnostics) OperationFailure(proof OperationReleaseUncommitted, observation FailureDiagnosticObservation) (FailureDiagnosticPublication, error) {
	return diagnostics.OperationFailureContext(context.Background(), proof, observation)
}

func (diagnostics *FailureDiagnostics) OperationFailureContext(ctx context.Context, proof OperationReleaseUncommitted, observation FailureDiagnosticObservation) (FailureDiagnosticPublication, error) {
	if ctx == nil {
		return FailureDiagnosticPublication{}, errors.New("diagnostic publication requires an ownership context")
	}
	if !diagnostics.valid() || !proof.Valid() || proof.settlement != diagnostics.operations {
		return FailureDiagnosticPublication{}, errors.New("failure diagnostic requires this operation's terminal proof")
	}
	claim := OperationIntentClaim{operationAuthority: cloneOperationAuthority(proof.authority), settlement: proof.settlement}
	identity := operationClaimDiagnosticIdentity(claim)
	unlock, err := diagnostics.operations.lockLeaseContext(ctx, identity.LeaseUUID)
	if err != nil {
		return FailureDiagnosticPublication{}, err
	}
	defer unlock()
	if err := diagnostics.operations.callbacks.requireCurrentOperationClaim(claim); err != nil {
		return FailureDiagnosticPublication{}, err
	}
	if err := validateOperationReleaseUncommitted(diagnostics.operations.callbacks, claim, proof); err != nil {
		return FailureDiagnosticPublication{}, err
	}
	if observation.Status == "" {
		observation.Status = DiagnosticCaptureUnavailable
	}
	capture, err := diagnostics.captureAttemptLocked(identity, observation)
	if err != nil {
		return FailureDiagnosticPublication{}, err
	}
	return FailureDiagnosticPublication{owner: diagnostics, capture: capture, operation: proof}, nil
}

func (diagnostics *FailureDiagnostics) MaintenanceFailure(proof MaintenanceReleaseFailure, observation FailureDiagnosticObservation) (FailureDiagnosticPublication, error) {
	return diagnostics.MaintenanceFailureContext(context.Background(), proof, observation)
}

func (diagnostics *FailureDiagnostics) MaintenanceFailureContext(ctx context.Context, proof MaintenanceReleaseFailure, observation FailureDiagnosticObservation) (FailureDiagnosticPublication, error) {
	if ctx == nil {
		return FailureDiagnosticPublication{}, errors.New("diagnostic publication requires an ownership context")
	}
	if !diagnostics.valid() || !proof.Valid() || proof.settlement != diagnostics.maintenance {
		return FailureDiagnosticPublication{}, errors.New("failure diagnostic requires this maintenance's terminal proof")
	}
	identity := maintenanceClaimDiagnosticIdentity(proof.Intent())
	identity.ReleaseVersion = proof.target.Version()
	unlock, err := diagnostics.maintenance.lockLeaseContext(ctx, identity.LeaseUUID)
	if err != nil {
		return FailureDiagnosticPublication{}, err
	}
	defer unlock()
	return diagnostics.maintenanceFailureLocked(proof, observation)
}

func (diagnostics *FailureDiagnostics) maintenanceFailureLocked(proof MaintenanceReleaseFailure, observation FailureDiagnosticObservation) (FailureDiagnosticPublication, error) {
	identity := maintenanceClaimDiagnosticIdentity(proof.Intent())
	identity.ReleaseVersion = proof.target.Version()
	if err := diagnostics.maintenance.callbacks.requireCurrentMaintenanceClaim(proof.intent); err != nil {
		return FailureDiagnosticPublication{}, err
	}
	if err := diagnostics.maintenance.validateFailureProofLocked(proof.intent, proof); err != nil {
		return FailureDiagnosticPublication{}, err
	}
	if observation.Status == "" {
		observation.Status = DiagnosticCaptureUnavailable
	}
	capture, err := diagnostics.captureAttemptLocked(identity, observation)
	if err != nil {
		return FailureDiagnosticPublication{}, err
	}
	if err := diagnostics.bindCompensatedDiagnosticLocked(proof, capture); err != nil {
		return FailureDiagnosticPublication{}, err
	}
	return FailureDiagnosticPublication{owner: diagnostics, capture: capture, maintenance: proof}, nil
}

// TryPublishMaintenanceFailure preserves recovery's nonblocking lease-gate
// contract while committing the exact terminal diagnostic before callback
// settlement is allowed to proceed.
func (diagnostics *FailureDiagnostics) TryPublishMaintenanceFailure(proof MaintenanceReleaseFailure, observation FailureDiagnosticObservation, failCount int) (DiagnosticEntry, bool, error) {
	if !diagnostics.valid() || !proof.Valid() || proof.settlement != diagnostics.maintenance || failCount < 0 {
		return DiagnosticEntry{}, false, errors.New("maintenance diagnostic requires this operation's terminal proof")
	}
	unlock, acquired := diagnostics.maintenance.tryLockLease(proof.LeaseUUID())
	if !acquired {
		return DiagnosticEntry{}, false, nil
	}
	defer unlock()
	publication, err := diagnostics.maintenanceFailureLocked(proof, observation)
	if err != nil {
		return DiagnosticEntry{}, true, err
	}
	if err := diagnostics.store.publishAttempt(publication.capture, failCount); err != nil {
		return DiagnosticEntry{}, true, err
	}
	entry, _, err := publication.Snapshot()
	return entry, true, err
}

// Publish is intentionally the only effect exposed by the handoff. Its argument
// is an observational counter; callers cannot substitute identity, logs,
// category, callback message, or attempt ordering.
func (publication FailureDiagnosticPublication) Publish(failCount int) error {
	return publication.PublishContext(context.Background(), failCount)
}

func (publication FailureDiagnosticPublication) PublishContext(ctx context.Context, failCount int) error {
	if ctx == nil {
		return errors.New("diagnostic publication requires an ownership context")
	}
	if !publication.Valid() || failCount < 0 {
		return errors.New("failure diagnostic publication is invalid")
	}
	diagnostics := publication.owner
	identity := publication.capture.identity
	unlock, err := diagnostics.operations.lockLeaseContext(ctx, identity.LeaseUUID)
	if err != nil {
		return err
	}
	defer unlock()
	if publication.operation.Valid() {
		proof := publication.operation
		claim := OperationIntentClaim{operationAuthority: cloneOperationAuthority(proof.authority), settlement: proof.settlement}
		if !sameDiagnosticAttempt(identity, operationClaimDiagnosticIdentity(claim)) {
			return errors.New("operation diagnostic publication identity differs from its proof")
		}
		if err := diagnostics.operations.callbacks.requireCurrentOperationClaim(claim); err != nil {
			return err
		}
		if err := validateOperationReleaseUncommitted(diagnostics.operations.callbacks, claim, proof); err != nil {
			return err
		}
	} else {
		proof := publication.maintenance
		if !sameDiagnosticAttempt(identity, maintenanceClaimDiagnosticIdentity(proof.intent)) {
			return errors.New("maintenance diagnostic publication identity differs from its proof")
		}
		if err := diagnostics.maintenance.callbacks.requireCurrentMaintenanceClaim(proof.intent); err != nil {
			return err
		}
		if err := diagnostics.maintenance.validateFailureProofLocked(proof.intent, proof); err != nil {
			return err
		}
	}
	return diagnostics.store.publishAttempt(publication.capture, failCount)
}

func (store *DiagnosticsStore) publishAttempt(capture FailureDiagnosticCapture, failCount int) error {
	if !capture.Valid() || capture.store != store {
		return errors.New("diagnostic publication belongs to another open store")
	}
	return store.update(func(tx *bolt.Tx) error {
		attempts := tx.Bucket(attemptDiagnosticsBucketName)
		data := attempts.Get([]byte(capture.identity.key()))
		if data == nil {
			return errors.New("diagnostic capture expired before publication")
		}
		record, err := decodeAttemptDiagnostic(data)
		if err != nil {
			return err
		}
		if !sameDiagnosticAttempt(record.Identity, capture.identity) {
			return errors.New("diagnostic publication does not match its durable attempt")
		}
		// Replaying publication is idempotent. A new attempt advances the
		// retained count even if the process restarted without its actor state.
		visible := tx.Bucket(diagnosticsBucketName)
		if data := visible.Get([]byte(record.Identity.LeaseUUID)); data != nil {
			var previous DiagnosticEntry
			if err := json.Unmarshal(data, &previous); err != nil {
				return err
			}
			if string(tx.Bucket(diagnosticPublicationsBucketName).Get([]byte(record.Identity.LeaseUUID))) == record.Identity.key() {
				failCount = max(failCount, previous.FailCount)
			} else {
				failCount = max(failCount, previous.FailCount+1)
			}
		}
		record.Entry.FailCount = failCount
		// The public row is a small selector; logs have one durable copy in
		// the attempt record. A later exact capture can enrich the currently
		// selected attempt without selecting a different historical attempt.
		visibleEntry := record.Entry
		visibleEntry.Logs = nil
		encoded, err := json.Marshal(visibleEntry)
		if err != nil {
			return fmt.Errorf("marshal diagnostic publication: %w", err)
		}
		if err := tx.Bucket(diagnosticsBucketName).Put([]byte(record.Identity.LeaseUUID), encoded); err != nil {
			return err
		}
		publications := tx.Bucket(diagnosticPublicationsBucketName)
		if previous := publications.Get([]byte(record.Identity.LeaseUUID)); previous != nil && string(previous) != record.Identity.key() {
			if err := attempts.Delete(previous); err != nil {
				return err
			}
		}
		return publications.Put([]byte(record.Identity.LeaseUUID), []byte(record.Identity.key()))
	})
}

// StoreRuntime retains the existing runtime-failure diagnostic path while
// preventing a snapshot of an older generation from overwriting an attempt
// capture. The current release and principal are checked under the journal
// pair's lease gate; an unresolved operation/maintenance owns publication until
// its terminal handoff completes.
func (diagnostics *FailureDiagnostics) StoreRuntime(entry DiagnosticEntry) error {
	if !diagnostics.valid() || entry.LeaseUUID == "" {
		return errors.New("runtime diagnostic publication is invalid")
	}
	unlock := diagnostics.operations.lockLease(entry.LeaseUUID)
	defer unlock()
	if err := diagnostics.operations.callbacks.view(func(tx *bolt.Tx) error {
		head, present, err := getLeaseMutationHeadTx(tx, entry.LeaseUUID)
		if err != nil || !present {
			return err
		}
		switch head := head.(type) {
		case operationLeaseMutationHead:
			if head.claim.entry.State == operationIntentPending {
				return errors.New("pending operation owns failure diagnostic publication")
			}
		case maintenanceLeaseMutationHead:
			return errors.New("pending maintenance owns failure diagnostic publication")
		}
		return nil
	}); err != nil {
		return err
	}
	release, err := diagnostics.operations.releases.LatestActive(entry.LeaseUUID)
	if err != nil {
		return err
	}
	if release == nil || entry.RuntimeReleaseVersion <= 0 || entry.RuntimeReleaseVersion != release.Version {
		return errors.New("runtime diagnostic does not describe the current active release")
	}
	identity, ok := release.RuntimeIdentity()
	if !ok || identity.Tenant() != entry.Tenant || identity.ProviderUUID() != entry.ProviderUUID {
		return errors.New("runtime diagnostic principal differs from the current release")
	}
	lifecycle := backend.ObserveLifecycleGeneration(identity.CallbackURL(), identity.LifecycleCallbackURL())
	if entry.LifecycleGeneration == nil || *entry.LifecycleGeneration != lifecycle {
		return errors.New("runtime diagnostic lifecycle differs from the current release")
	}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if len(encoded) > MaxFailureDiagnosticEncodedBytes {
		return errors.New("runtime diagnostic exceeds its encoded byte budget")
	}
	return diagnostics.store.update(func(tx *bolt.Tx) error {
		if previous := tx.Bucket(diagnosticPublicationsBucketName).Get([]byte(entry.LeaseUUID)); previous != nil {
			if err := tx.Bucket(attemptDiagnosticsBucketName).Delete(previous); err != nil {
				return err
			}
		}
		if err := tx.Bucket(diagnosticPublicationsBucketName).Delete([]byte(entry.LeaseUUID)); err != nil {
			return err
		}
		return tx.Bucket(diagnosticsBucketName).Put([]byte(entry.LeaseUUID), encoded)
	})
}
