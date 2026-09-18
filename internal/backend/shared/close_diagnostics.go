package shared

import (
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

// CloseAttemptDiagnostics is the exact interrupted attempt durably named by a
// close head. It can record observations but cannot publish before terminal
// close evidence; its identity is never selected from inventory or timestamps.
type CloseAttemptDiagnostics struct {
	owner         *FailureDiagnostics
	close         ClosePhysicalSubject
	identity      diagnosticAttemptIdentity
	operationID   OperationID
	callbackURL   string
	lifecycleURL  string
	maintenanceID MaintenanceID
}

func (attempt CloseAttemptDiagnostics) Valid() bool {
	return attempt.owner.valid() && attempt.close.Valid() && attempt.identity.valid() &&
		(attempt.operationID.Valid() || attempt.maintenanceID.Valid())
}
func (attempt CloseAttemptDiagnostics) OperationID() OperationID     { return attempt.operationID }
func (attempt CloseAttemptDiagnostics) MaintenanceID() MaintenanceID { return attempt.maintenanceID }
func (attempt CloseAttemptDiagnostics) Tenant() string               { return attempt.identity.Tenant }
func (attempt CloseAttemptDiagnostics) ProviderUUID() string         { return attempt.identity.ProviderUUID }

// MatchesTarget compares complete routing labels as observations of the exact
// archived attempt; neither a tokenless ID match nor a service name suffices.
func (attempt CloseAttemptDiagnostics) MatchesTarget(callbackURL, lifecycleURL string, maintenanceID MaintenanceID) bool {
	return attempt.Valid() && attempt.callbackURL != "" && callbackURL == attempt.callbackURL &&
		lifecycleURL == attempt.lifecycleURL && maintenanceID == attempt.maintenanceID
}

func (diagnostics *FailureDiagnostics) CloseAttempt(subject ClosePhysicalSubject) (CloseAttemptDiagnostics, bool, error) {
	if !diagnostics.valid() || !subject.Valid() ||
		subject.state.settlement.callbacks != diagnostics.operations.callbacks ||
		subject.state.settlement.releases != diagnostics.operations.releases {
		return CloseAttemptDiagnostics{}, false, errors.New("close diagnostics require the exact journal-bound close subject")
	}
	unlock := diagnostics.operations.lockLease(subject.LeaseUUID())
	defer unlock()
	return diagnostics.closeAttemptLocked(subject)
}

func (diagnostics *FailureDiagnostics) closeAttemptLocked(subject ClosePhysicalSubject) (CloseAttemptDiagnostics, bool, error) {
	claim := subject.Intent()
	if err := diagnostics.operations.callbacks.requireCloseIntent(claim); err != nil {
		return CloseAttemptDiagnostics{}, false, err
	}
	attempt := CloseAttemptDiagnostics{owner: diagnostics, close: subject}
	operationID, maintenanceID := claim.InterruptedOperationID(), claim.InterruptedMaintenanceID()
	if operationID.IsZero() && maintenanceID.IsZero() {
		return attempt, false, nil
	}
	if operationID.Valid() {
		err := diagnostics.operations.callbacks.view(func(tx *bolt.Tx) error {
			history, err := listOperationHistoryTx(tx, claim.LeaseUUID())
			if err != nil {
				return err
			}
			for _, record := range history {
				if record.OperationID != operationID {
					continue
				}
				if record.State != operationIntentFailed {
					return errors.New("close interrupted operation is not failed")
				}
				attempt.identity = diagnosticAttemptIdentity{Kind: "operation", ID: record.OperationID.String(), Backend: record.Backend,
					StorageID: record.BackendStorageID, LeaseUUID: record.LeaseUUID, Tenant: record.Tenant, ProviderUUID: record.ProviderUUID,
					Lifecycle: backend.ObserveLifecycleGeneration(record.CallbackURL, record.LifecycleCallbackURL)}
				attempt.operationID = record.OperationID
				attempt.callbackURL, attempt.lifecycleURL = record.CallbackURL, record.LifecycleCallbackURL
				return nil
			}
			return errors.New("close interrupted operation receipt is missing")
		})
		if err != nil {
			return CloseAttemptDiagnostics{}, false, err
		}
	} else {
		var receipt maintenanceCompletionRecord
		err := diagnostics.operations.callbacks.view(func(tx *bolt.Tx) error {
			record, found, err := findMaintenanceReceiptTx(tx, claim.LeaseUUID(), maintenanceID)
			if err != nil {
				return err
			}
			if !found || record.Status != backend.CallbackStatusFailed {
				return errors.New("close interrupted maintenance receipt is missing or not failed")
			}
			receipt = record
			return nil
		})
		if err != nil {
			return CloseAttemptDiagnostics{}, false, err
		}
		attempt.identity = diagnosticAttemptIdentity{Kind: "maintenance", ID: receipt.MaintenanceID.String(), Backend: receipt.Backend,
			StorageID: receipt.BackendStorageID, LeaseUUID: receipt.LeaseUUID, Tenant: receipt.Tenant, ProviderUUID: receipt.ProviderUUID,
			ReleaseVersion: receipt.TargetReleaseVersion, Lifecycle: backend.ObserveLifecycleGeneration(claim.CallbackURL(), claim.LifecycleCallbackURL())}
		attempt.maintenanceID = maintenanceID
		if receipt.TargetReleaseVersion > 0 {
			history, err := diagnostics.operations.releases.List(claim.LeaseUUID())
			if err != nil {
				return CloseAttemptDiagnostics{}, false, err
			}
			found := false
			for _, release := range history {
				if release.Version != receipt.TargetReleaseVersion || release.MaintenanceID != maintenanceID {
					continue
				}
				digest, err := maintenanceReleaseDigest(release)
				if err != nil {
					return CloseAttemptDiagnostics{}, false, err
				}
				if encodeMaintenanceDigest(digest) != receipt.TargetReleaseDigest {
					return CloseAttemptDiagnostics{}, false, errors.New("close interrupted maintenance target differs from receipt")
				}
				identity, ok := release.RuntimeIdentity()
				if !ok || identity.Tenant() != receipt.Tenant || identity.ProviderUUID() != receipt.ProviderUUID {
					return CloseAttemptDiagnostics{}, false, errors.New("close interrupted maintenance target has divergent runtime identity")
				}
				attempt.identity.Lifecycle = backend.ObserveLifecycleGeneration(identity.CallbackURL(), identity.LifecycleCallbackURL())
				attempt.operationID = release.OperationID
				attempt.callbackURL, attempt.lifecycleURL = identity.CallbackURL(), identity.LifecycleCallbackURL()
				found = true
				break
			}
			if !found {
				return CloseAttemptDiagnostics{}, false, errors.New("close interrupted maintenance target release is missing")
			}
		}
	}
	if !attempt.Valid() || attempt.identity.Backend != claim.Backend() || attempt.identity.StorageID != claim.BackendStorageID().String() ||
		attempt.identity.LeaseUUID != claim.LeaseUUID() || (!claim.CleanupOnly() &&
		(attempt.identity.Tenant != claim.Tenant() || attempt.identity.ProviderUUID != claim.ProviderUUID())) {
		return CloseAttemptDiagnostics{}, false, errors.New("close interrupted diagnostic identity differs from its close authority")
	}
	return attempt, true, nil
}

func (attempt CloseAttemptDiagnostics) Capture(observation FailureDiagnosticObservation) (FailureDiagnosticCapture, error) {
	if !attempt.Valid() {
		return FailureDiagnosticCapture{}, errors.New("close interrupted diagnostic authority is invalid")
	}
	unlock := attempt.owner.operations.lockLease(attempt.identity.LeaseUUID)
	defer unlock()
	if err := attempt.owner.operations.callbacks.requireCloseIntent(attempt.close.Intent()); err != nil {
		return FailureDiagnosticCapture{}, err
	}
	return attempt.owner.captureAttemptLocked(attempt.identity, observation)
}

// PublishCloseFailure consumes terminal physical evidence while the exact close
// head still owns the lease. It precedes CompleteClose, so diagnostics failure
// cannot destroy the last recovery obligation or compact the linked receipts.
func (diagnostics *FailureDiagnostics) PublishCloseFailure(outcome CloseTerminalOutcome) error {
	if !diagnostics.valid() {
		return errors.New("close failure diagnostics are unavailable")
	}
	var subject ClosePhysicalSubject
	switch outcome := outcome.(type) {
	case CloseExecutionDestroyed:
		if !outcome.Valid() {
			return errors.New("destroyed close diagnostic proof is invalid")
		}
		subject = outcome.subject
	case CloseExecutionRetained:
		if !outcome.Valid() {
			return errors.New("retained close diagnostic proof is invalid")
		}
		subject = outcome.subject
	default:
		return fmt.Errorf("close diagnostic publication has no terminal proof: %T", outcome)
	}
	if subject.state.settlement.callbacks != diagnostics.operations.callbacks || subject.state.settlement.releases != diagnostics.operations.releases {
		return errors.New("close diagnostic proof belongs to another journal pair")
	}
	unlock := diagnostics.operations.lockLease(subject.LeaseUUID())
	defer unlock()
	attempt, present, err := diagnostics.closeAttemptLocked(subject)
	if err != nil || !present {
		return err
	}
	capture, err := diagnostics.captureAttemptLocked(attempt.identity, FailureDiagnosticObservation{
		Message: "operation interrupted by lease close", Status: DiagnosticCaptureUnavailable,
	})
	if err != nil {
		return err
	}
	return diagnostics.store.publishAttempt(capture, 1)
}
