package shared

import (
	"encoding/json"
	"errors"
	"maps"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

type compensatedDiagnosticSource struct {
	Version      int                                    `json:"version"`
	Tenant       string                                 `json:"tenant"`
	ProviderUUID string                                 `json:"provider_uuid"`
	Lifecycle    backend.LifecycleGenerationObservation `json:"lifecycle"`
}

// RuntimeFailureLogs is a read-only view of the failed attempt whose exact
// source is still the current runtime. Its private binding is authored only by
// terminal source-ready evidence, never caller-selected diagnostic metadata.
type RuntimeFailureLogs struct {
	source compensatedDiagnosticSource
	logs   map[string]string
}

func (view RuntimeFailureLogs) MatchesProjection(version int, tenant, provider string, lifecycle backend.LifecycleGenerationObservation) bool {
	return view.source.Version > 0 && view.source.Version == version && view.source.Tenant == tenant &&
		view.source.ProviderUUID == provider && view.source.Lifecycle == lifecycle
}
func (view RuntimeFailureLogs) Logs() map[string]string { return maps.Clone(view.logs) }

func (diagnostics *FailureDiagnostics) bindCompensatedDiagnosticLocked(proof MaintenanceReleaseFailure, capture FailureDiagnosticCapture) error {
	evidence, observed := proof.PhysicalEvidence()
	sourceReady := observed && evidence.kind == maintenancePhysicalEvidenceSourceReady &&
		evidence.sourceReady.state.subject.validFor(diagnostics.maintenance) &&
		evidence.sourceReady.state.subject.Intent().MatchesIntent(proof.intent)
	if !sourceReady {
		if err := diagnostics.maintenance.callbacks.view(func(tx *bolt.Tx) error {
			record, err := readCompensationTx(tx, proof.intent)
			if err != nil {
				return err
			}
			sourceReady = record != nil && record.Phase == compensationSourceReady
			return nil
		}); err != nil {
			return err
		}
	}
	if !sourceReady {
		return nil
	}
	source, err := diagnostics.maintenance.snapshotMaintenanceSourceLocked(proof.intent)
	if err != nil {
		return err
	}
	release := source.Release()
	identity, ok := release.RuntimeIdentity()
	if !ok {
		return errors.New("compensated diagnostics require the exact source runtime identity")
	}
	binding := compensatedDiagnosticSource{Version: release.Version, Tenant: identity.Tenant(), ProviderUUID: identity.ProviderUUID(),
		Lifecycle: backend.ObserveLifecycleGeneration(identity.CallbackURL(), identity.LifecycleCallbackURL())}
	return diagnostics.store.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(attemptDiagnosticsBucketName)
		record, err := decodeAttemptDiagnostic(bucket.Get([]byte(capture.identity.key())))
		if err != nil {
			return err
		}
		if !sameDiagnosticAttempt(record.Identity, capture.identity) {
			return errors.New("compensated diagnostic capture identity changed")
		}
		record.CompensatedSource = &binding
		data, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if len(data) > MaxFailureDiagnosticEncodedBytes {
			return errors.New("compensated diagnostic exceeds its encoded budget")
		}
		return bucket.Put([]byte(capture.identity.key()), data)
	})
}

func (diagnostics *FailureDiagnostics) PublishedForRuntime(runtime RuntimeGenerationProof) (RuntimeFailureLogs, error) {
	if !diagnostics.valid() || !runtime.Valid() || runtime.releases != diagnostics.operations.releases {
		return RuntimeFailureLogs{}, errors.New("failed-attempt runtime view requires its exact current release proof")
	}
	unlock := diagnostics.operations.lockLease(runtime.LeaseUUID())
	defer unlock()
	if err := runtime.Reattest(); err != nil {
		return RuntimeFailureLogs{}, err
	}
	var view RuntimeFailureLogs
	err := diagnostics.store.view(func(tx *bolt.Tx) error {
		key := tx.Bucket(diagnosticPublicationsBucketName).Get([]byte(runtime.LeaseUUID()))
		if key == nil {
			return nil
		}
		raw := tx.Bucket(attemptDiagnosticsBucketName).Get(key)
		if raw == nil {
			return nil
		}
		record, err := decodeAttemptDiagnostic(raw)
		if err != nil {
			return err
		}
		source := record.CompensatedSource
		if source == nil || source.Version != runtime.Version() || source.Tenant != runtime.authority.Tenant() ||
			source.ProviderUUID != runtime.authority.ProviderUUID() || source.Lifecycle != backend.ObserveLifecycleGeneration(runtime.authority.CallbackURL(), runtime.authority.LifecycleCallbackURL()) {
			return nil
		}
		view = RuntimeFailureLogs{source: *source, logs: record.Entry.Logs}
		return nil
	})
	return view, err
}
