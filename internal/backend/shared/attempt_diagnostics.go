package shared

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
)

var (
	attemptDiagnosticsBucketName     = []byte("attempt_failure_diagnostics")
	diagnosticPublicationsBucketName = []byte("attempt_diagnostic_publications")
)

const (
	// Diagnostic logs retain the same aggregate budget as the Docker log API.
	// The independent encoded budget also bounds JSON escaping and metadata.
	MaxFailureDiagnosticLogBytes      = 32 << 20
	MaxFailureDiagnosticEncodedBytes  = 40 << 20
	maxFailureDiagnosticMetadataBytes = 256 << 10
	maxFailureDiagnosticContainers    = 4096
)

// DiagnosticCaptureStatus describes an observation, never cleanup authority.
type DiagnosticCaptureStatus string

const (
	DiagnosticCaptureComplete    DiagnosticCaptureStatus = "complete"
	DiagnosticCapturePartial     DiagnosticCaptureStatus = "partial"
	DiagnosticCaptureTruncated   DiagnosticCaptureStatus = "truncated"
	DiagnosticCaptureUnavailable DiagnosticCaptureStatus = "unavailable"
)

// FailureDiagnosticObservation is non-authoritative data supplied by the
// subject-bound log reader. Every identity is derived separately from the
// operation or maintenance subject, never from these fields.
type FailureDiagnosticObservation struct {
	Error        string
	Reason       backend.Reason
	Message      string
	Logs         map[string]string
	ContainerIDs []string
	Status       DiagnosticCaptureStatus
}

type diagnosticAttemptIdentity struct {
	Kind           string                                 `json:"kind"`
	ID             string                                 `json:"id"`
	Backend        string                                 `json:"backend"`
	StorageID      string                                 `json:"storage_id"`
	LeaseUUID      string                                 `json:"lease_uuid"`
	Tenant         string                                 `json:"tenant"`
	ProviderUUID   string                                 `json:"provider_uuid"`
	ReleaseVersion int                                    `json:"release_version,omitempty"`
	Lifecycle      backend.LifecycleGenerationObservation `json:"lifecycle_generation"`
}

func (identity diagnosticAttemptIdentity) key() string {
	// The discriminator keeps maintenance and operation UUIDs in separate
	// namespaces. Neither callback URLs nor timestamps participate in the key.
	return identity.keyPrefix() + identity.Kind + "/" + identity.ID
}

func (identity diagnosticAttemptIdentity) keyPrefix() string {
	return identity.StorageID + "/" + identity.LeaseUUID + "/"
}

func (identity diagnosticAttemptIdentity) valid() bool {
	return (identity.Kind == "operation" || identity.Kind == "maintenance") &&
		identity.ID != "" && identity.Backend != "" && identity.StorageID != "" &&
		identity.LeaseUUID != "" && identity.Tenant != "" && identity.ProviderUUID != ""
}

type attemptDiagnosticRecord struct {
	Identity          diagnosticAttemptIdentity    `json:"identity"`
	CompensatedSource *compensatedDiagnosticSource `json:"compensated_source,omitempty"`
	Entry             DiagnosticEntry              `json:"entry"`
	ContainerIDs      []string                     `json:"container_ids,omitempty"`
	Status            DiagnosticCaptureStatus      `json:"capture_status"`
	// ObservedFailure distinguishes an actual physical error from a recovery
	// observation that has no surviving original cause.
	ObservedFailure bool `json:"observed_failure"`
}

// FailureDiagnosticCapture is an immutable reference to an attempt record in
// one open diagnostics store. It grants observation only: cleanup still needs
// its exact physical subject and publication needs a current terminal proof.
type FailureDiagnosticCapture struct {
	owner    *FailureDiagnostics
	store    *DiagnosticsStore
	identity diagnosticAttemptIdentity
}

func (capture FailureDiagnosticCapture) Valid() bool {
	return capture.owner.valid() && capture.owner.store == capture.store && capture.store != nil && boltStoreIsOpen(capture.store.boltStore) && capture.identity.valid()
}

func (capture FailureDiagnosticCapture) Snapshot() (DiagnosticEntry, DiagnosticCaptureStatus, error) {
	if !capture.Valid() {
		return DiagnosticEntry{}, "", errors.New("failure diagnostic capture is invalid")
	}
	record, err := capture.store.readAttempt(capture.identity)
	if err != nil {
		return DiagnosticEntry{}, "", err
	}
	return record.Entry, record.Status, nil
}

// FailureDiagnostics binds observational storage to the exact journals which
// serialize new admission, terminal publication, and close. It never owns a
// substrate writer, release transition, or callback settlement capability.
type FailureDiagnostics struct {
	store       *DiagnosticsStore
	operations  *OperationSettlement
	maintenance *MaintenanceSettlement
}

func NewFailureDiagnostics(store *DiagnosticsStore, operations *OperationSettlement, maintenance *MaintenanceSettlement) (*FailureDiagnostics, error) {
	if store == nil || !boltStoreIsOpen(store.boltStore) || operations == nil || maintenance == nil ||
		!operations.valid() || !maintenance.valid() ||
		operations.callbacks != maintenance.callbacks || operations.releases != maintenance.releases {
		return nil, errors.New("failure diagnostics require one open diagnostics store and an exact operation/maintenance journal pair")
	}
	return &FailureDiagnostics{store: store, operations: operations, maintenance: maintenance}, nil
}

func (diagnostics *FailureDiagnostics) valid() bool {
	return diagnostics != nil && diagnostics.store != nil && boltStoreIsOpen(diagnostics.store.boltStore) &&
		diagnostics.operations != nil && diagnostics.operations.valid() &&
		diagnostics.maintenance != nil && diagnostics.maintenance.valid()
}

func operationDiagnosticIdentity(subject OperationPhysicalSubject) (diagnosticAttemptIdentity, error) {
	if !subject.Valid() {
		return diagnosticAttemptIdentity{}, errors.New("diagnostic capture requires an exact operation subject")
	}
	if receipt, historical := subject.FailedReceiptCleanup(); historical {
		return diagnosticAttemptIdentity{
			Kind: "operation", ID: receipt.OperationID().String(), Backend: receipt.Backend(),
			StorageID: receipt.BackendStorageID().String(), LeaseUUID: receipt.LeaseUUID(),
			Tenant: receipt.Tenant(), ProviderUUID: receipt.ProviderUUID(),
			Lifecycle: backend.ObserveLifecycleGeneration(receipt.CallbackURL(), receipt.LifecycleCallbackURL()),
		}, nil
	}
	return operationClaimDiagnosticIdentity(subject.Intent()), nil
}

func operationClaimDiagnosticIdentity(claim OperationIntentClaim) diagnosticAttemptIdentity {
	return diagnosticAttemptIdentity{
		Kind: "operation", ID: claim.OperationID().String(), Backend: claim.Backend(),
		StorageID: claim.BackendStorageID().String(), LeaseUUID: claim.LeaseUUID(),
		Tenant: claim.Tenant(), ProviderUUID: claim.ProviderUUID(),
		Lifecycle: backend.ObserveLifecycleGeneration(claim.CallbackURL(), claim.LifecycleCallbackURL()),
	}
}

func maintenanceDiagnosticIdentity(subject MaintenancePhysicalSubject) (diagnosticAttemptIdentity, error) {
	if !subject.Valid() {
		return diagnosticAttemptIdentity{}, errors.New("diagnostic capture requires an exact maintenance subject")
	}
	if receipt, historical := subject.FailedReceiptCleanup(); historical {
		target, ok := receipt.TargetRelease()
		if !ok {
			return diagnosticAttemptIdentity{}, errors.New("failed maintenance diagnostic has no target release")
		}
		identity := diagnosticAttemptIdentity{
			Kind: "maintenance", ID: receipt.MaintenanceID().String(), Backend: receipt.Backend(),
			StorageID: receipt.BackendStorageID().String(), LeaseUUID: receipt.LeaseUUID(),
			Tenant: receipt.Tenant(), ProviderUUID: receipt.ProviderUUID(), ReleaseVersion: target.Version,
		}
		if runtime, ok := releaseRuntimeIdentityFor(target); ok {
			identity.Lifecycle = backend.ObserveLifecycleGeneration(runtime.callbackURL, runtime.lifecycleCallbackURL)
		}
		return identity, nil
	}
	identity := maintenanceClaimDiagnosticIdentity(subject.Intent())
	if target, ok := subject.TargetRelease(); ok {
		identity.ReleaseVersion = target.Version
	}
	return identity, nil
}

func maintenanceClaimDiagnosticIdentity(claim MaintenanceIntentClaim) diagnosticAttemptIdentity {
	return diagnosticAttemptIdentity{
		Kind: "maintenance", ID: claim.MaintenanceID().String(), Backend: claim.Backend(),
		StorageID: claim.BackendStorageID().String(), LeaseUUID: claim.LeaseUUID(),
		Tenant: claim.Tenant(), ProviderUUID: claim.ProviderUUID(),
		Lifecycle: backend.ObserveLifecycleGeneration(claim.CallbackURL(), claim.LifecycleCallbackURL()),
	}
}

func (diagnostics *FailureDiagnostics) CaptureOperation(subject OperationPhysicalSubject, observation FailureDiagnosticObservation) (FailureDiagnosticCapture, error) {
	if !diagnostics.valid() || !subject.validFor(diagnostics.operations) {
		return FailureDiagnosticCapture{}, errors.New("operation diagnostic belongs to another execution boundary")
	}
	identity, err := operationDiagnosticIdentity(subject)
	if err != nil {
		return FailureDiagnosticCapture{}, err
	}
	unlock := diagnostics.operations.lockLease(identity.LeaseUUID)
	defer unlock()
	return diagnostics.captureAttemptLocked(identity, observation)
}

func (diagnostics *FailureDiagnostics) CaptureMaintenance(subject MaintenancePhysicalSubject, observation FailureDiagnosticObservation) (FailureDiagnosticCapture, error) {
	if !diagnostics.valid() || !subject.validFor(diagnostics.maintenance) {
		return FailureDiagnosticCapture{}, errors.New("maintenance diagnostic belongs to another execution boundary")
	}
	identity, err := maintenanceDiagnosticIdentity(subject)
	if err != nil {
		return FailureDiagnosticCapture{}, err
	}
	unlock := diagnostics.operations.lockLease(identity.LeaseUUID)
	defer unlock()
	return diagnostics.captureAttemptLocked(identity, observation)
}

func (store *DiagnosticsStore) captureAttempt(identity diagnosticAttemptIdentity, observation FailureDiagnosticObservation) (FailureDiagnosticCapture, error) {
	if !identity.valid() {
		return FailureDiagnosticCapture{}, errors.New("failure diagnostic identity is incomplete")
	}
	record := attemptDiagnosticRecord{
		Identity: identity, ContainerIDs: slices.Clone(observation.ContainerIDs), Status: observation.Status,
		ObservedFailure: observation.Error != "",
		Entry: DiagnosticEntry{
			LeaseUUID: identity.LeaseUUID, ProviderUUID: identity.ProviderUUID, Tenant: identity.Tenant,
			Error: observation.Error, Reason: observation.Reason, Message: observation.Message,
			Logs: maps.Clone(observation.Logs), LifecycleGeneration: &identity.Lifecycle, CreatedAt: time.Now().UTC(),
		},
	}
	if err := normalizeAttemptDiagnostic(&record); err != nil {
		return FailureDiagnosticCapture{}, err
	}
	err := store.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(attemptDiagnosticsBucketName)
		if bucket == nil {
			return errors.New("attempt diagnostics bucket is missing")
		}
		if data := bucket.Get([]byte(identity.key())); data != nil {
			existing, err := decodeAttemptDiagnostic(data)
			if err != nil {
				return err
			}
			if !sameDiagnosticAttempt(existing.Identity, identity) {
				return errors.New("attempt diagnostic identity conflicts with its durable record")
			}
			record, err = mergeAttemptDiagnostic(existing, record)
			if err != nil {
				return err
			}
		}
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("marshal attempt diagnostic: %w", err)
		}
		if len(data) > MaxFailureDiagnosticEncodedBytes {
			return errors.New("attempt diagnostic exceeds its encoded byte budget")
		}
		return bucket.Put([]byte(identity.key()), data)
	})
	if err != nil {
		return FailureDiagnosticCapture{}, err
	}
	return FailureDiagnosticCapture{store: store, identity: identity}, nil
}

func sameDiagnosticAttempt(left, right diagnosticAttemptIdentity) bool {
	// A pre-append observation may have no version. An assigned version is
	// checked whenever both records have one; the typed attempt ID is primary.
	versionsMatch := left.ReleaseVersion == right.ReleaseVersion || left.ReleaseVersion == 0 || right.ReleaseVersion == 0
	left.ReleaseVersion, right.ReleaseVersion = 0, 0
	return versionsMatch && left == right
}

func mergeAttemptDiagnostic(previous, current attemptDiagnosticRecord) (attemptDiagnosticRecord, error) {
	if previous.ObservedFailure {
		current.Entry.Error, current.Entry.Reason, current.Entry.Message = previous.Entry.Error, previous.Entry.Reason, previous.Entry.Message
		current.ObservedFailure = true
	}
	current.Entry.CreatedAt = previous.Entry.CreatedAt
	current.CompensatedSource = previous.CompensatedSource
	if current.Identity.ReleaseVersion == 0 {
		current.Identity.ReleaseVersion = previous.Identity.ReleaseVersion
	}
	// Empty recovery observations cannot erase the only surviving log copy.
	// Keep the first captured output for each service/instance. New late targets
	// may add keys but cannot replace earlier evidence for the same attempt.
	if current.Entry.Logs == nil {
		current.Entry.Logs = make(map[string]string)
	}
	for key, value := range previous.Entry.Logs {
		current.Entry.Logs[key] = value
	}
	current.ContainerIDs = append(current.ContainerIDs, previous.ContainerIDs...)
	if previous.Status == DiagnosticCaptureTruncated || current.Status == DiagnosticCaptureTruncated {
		current.Status = DiagnosticCaptureTruncated
	} else if len(previous.Entry.Logs) > 0 && current.Status == DiagnosticCaptureUnavailable {
		current.Status = previous.Status
	}
	err := normalizeAttemptDiagnosticPrioritizing(&current, previous.Entry.Logs)
	return current, err
}

func normalizeAttemptDiagnostic(record *attemptDiagnosticRecord) error {
	return normalizeAttemptDiagnosticPrioritizing(record, nil)
}

func normalizeAttemptDiagnosticPrioritizing(record *attemptDiagnosticRecord, retained map[string]string) error {
	switch record.Status {
	case DiagnosticCaptureComplete, DiagnosticCapturePartial, DiagnosticCaptureTruncated, DiagnosticCaptureUnavailable:
	default:
		return errors.New("failure diagnostic capture status is invalid")
	}
	// Observed error text is untrusted and may contain arbitrary process output.
	// Bound it independently so a large cause cannot prevent cleanup forever.
	for _, field := range []*string{&record.Entry.Error, &record.Entry.Message} {
		value := strings.ToValidUTF8(*field, "\uFFFD")
		prefix, _ := diagnosticLogPrefix(value, 16<<10, 32<<10)
		if prefix != *field {
			record.Status = DiagnosticCaptureTruncated
		}
		*field = prefix
	}
	if len(record.Entry.Reason) > 256 {
		return errors.New("failure diagnostic reason exceeds its byte budget")
	}
	for _, id := range record.ContainerIDs {
		if len(id) > 256 || !utf8.ValidString(id) {
			return errors.New("failure diagnostic container identity is invalid")
		}
	}
	metadata := *record
	metadata.Entry.Logs = nil
	metadata.ContainerIDs = nil
	encoded, err := json.Marshal(metadata)
	if err != nil || len(encoded) > maxFailureDiagnosticMetadataBytes {
		return errors.New("failure diagnostic metadata exceeds its byte budget")
	}
	slices.Sort(record.ContainerIDs)
	record.ContainerIDs = slices.Compact(record.ContainerIDs)
	if len(record.ContainerIDs) > maxFailureDiagnosticContainers {
		record.ContainerIDs = record.ContainerIDs[:maxFailureDiagnosticContainers]
		record.Status = DiagnosticCaptureTruncated
	}
	remainingRaw := MaxFailureDiagnosticLogBytes
	// Reserve the maximum encoded identity list even on the first capture.
	// Late container identities cannot consume bytes already promised to logs.
	remainingEncoded := MaxFailureDiagnosticEncodedBytes - 2*maxFailureDiagnosticMetadataBytes - maxFailureDiagnosticContainers*(6*256+4)
	logs := make(map[string]string)
	keys := slices.Sorted(maps.Keys(retained))
	for _, key := range slices.Sorted(maps.Keys(record.Entry.Logs)) {
		if _, present := retained[key]; !present {
			keys = append(keys, key)
		}
	}
	for _, key := range keys {
		if len(logs) >= maxFailureDiagnosticContainers || len(key) > 256 || !utf8.ValidString(key) {
			record.Status = DiagnosticCaptureTruncated
			continue
		}
		value := record.Entry.Logs[key]
		if !utf8.ValidString(value) {
			value = strings.ToValidUTF8(value, "\uFFFD")
			record.Status = DiagnosticCaptureTruncated
		}
		encodedKey, _ := json.Marshal(key)
		remainingEncoded -= len(encodedKey) + 4
		prefix, cost := diagnosticLogPrefix(value, remainingRaw, remainingEncoded)
		if len(prefix) != len(value) {
			record.Status = DiagnosticCaptureTruncated
		}
		if remainingRaw <= 0 || remainingEncoded < 0 {
			break
		}
		logs[key] = prefix
		remainingRaw -= len(prefix)
		remainingEncoded -= cost
	}
	record.Entry.Logs = logs
	return nil
}

func diagnosticLogPrefix(value string, rawBudget, encodedBudget int) (string, int) {
	end, encoded := 0, 0
	for offset, r := range value {
		n := utf8.RuneLen(r)
		cost := n
		switch {
		case r == '"' || r == '\\':
			cost = 2
		case r < 0x20 || r == '<' || r == '>' || r == '&' || r == '\u2028' || r == '\u2029':
			cost = 6
		}
		if offset+n > rawBudget || encoded+cost > encodedBudget {
			break
		}
		end, encoded = offset+n, encoded+cost
	}
	return value[:end], encoded
}

func decodeAttemptDiagnostic(data []byte) (attemptDiagnosticRecord, error) {
	if len(data) > MaxFailureDiagnosticEncodedBytes {
		return attemptDiagnosticRecord{}, errors.New("stored attempt diagnostic exceeds its byte budget")
	}
	var record attemptDiagnosticRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return record, fmt.Errorf("decode attempt diagnostic: %w", err)
	}
	if !record.Identity.valid() || record.Entry.LeaseUUID != record.Identity.LeaseUUID ||
		record.Entry.Tenant != record.Identity.Tenant || record.Entry.ProviderUUID != record.Identity.ProviderUUID {
		return record, errors.New("stored attempt diagnostic identity is inconsistent")
	}
	switch record.Status {
	case DiagnosticCaptureComplete, DiagnosticCapturePartial, DiagnosticCaptureTruncated, DiagnosticCaptureUnavailable:
	default:
		return record, errors.New("stored attempt diagnostic status is invalid")
	}
	if record.Entry.LifecycleGeneration == nil || *record.Entry.LifecycleGeneration != record.Identity.Lifecycle ||
		len(record.ContainerIDs) > maxFailureDiagnosticContainers || len(record.Entry.Logs) > maxFailureDiagnosticContainers {
		return record, errors.New("stored attempt diagnostic metadata is inconsistent")
	}
	if source := record.CompensatedSource; source != nil && (source.Version <= 0 || source.Tenant != record.Identity.Tenant || source.ProviderUUID != record.Identity.ProviderUUID) {
		return record, errors.New("stored compensated diagnostic has inconsistent source identity")
	}
	rawBytes := 0
	for key, value := range record.Entry.Logs {
		rawBytes += len(value)
		if len(key) > 256 || !utf8.ValidString(key) || !utf8.ValidString(value) || rawBytes > MaxFailureDiagnosticLogBytes {
			return record, errors.New("stored attempt diagnostic logs exceed their budget")
		}
	}
	return record, nil
}

func (store *DiagnosticsStore) readAttempt(identity diagnosticAttemptIdentity) (attemptDiagnosticRecord, error) {
	var record attemptDiagnosticRecord
	err := store.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(attemptDiagnosticsBucketName)
		if bucket == nil {
			return errors.New("attempt diagnostics bucket is missing")
		}
		data := bucket.Get([]byte(identity.key()))
		if data == nil {
			return errors.New("attempt diagnostic capture is unavailable")
		}
		var err error
		record, err = decodeAttemptDiagnostic(data)
		if err == nil && !sameDiagnosticAttempt(record.Identity, identity) {
			err = errors.New("attempt diagnostic capture belongs to another identity")
		}
		return err
	})
	return record, err
}

// RetainDuring keeps the already-durable record readable until the caller's
// bounded action ends. The read transaction prevents expiry or store closure
// from deleting the only saved copy between persistence and physical removal.
// This grants no cleanup authority; substrate adapters must separately own the
// exact captured cohort and its opaque physical subject.
func (capture FailureDiagnosticCapture) RetainDuring(action func() error) error {
	if !capture.Valid() || action == nil {
		return errors.New("durable diagnostic retention is invalid")
	}
	return capture.store.view(func(tx *bolt.Tx) error {
		data := tx.Bucket(attemptDiagnosticsBucketName).Get([]byte(capture.identity.key()))
		if data == nil {
			return errors.New("diagnostic capture expired before its owned action")
		}
		record, err := decodeAttemptDiagnostic(data)
		if err != nil {
			return err
		}
		if !sameDiagnosticAttempt(record.Identity, capture.identity) {
			return errors.New("retained diagnostic differs from the captured attempt")
		}
		return action()
	})
}
