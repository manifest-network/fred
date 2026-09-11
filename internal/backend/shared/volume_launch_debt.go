package shared

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/fsidentity"
)

var volumeLaunchDebtBucketName = []byte("docker_volume_launch_debt_v1")

// ErrVolumeLaunchUnsettled means Docker may still accept an earlier Create or
// Start using these physical directories. Inventory absence and a stopped
// container cannot revoke an outstanding daemon request.
var ErrVolumeLaunchUnsettled = errors.New("physical volume has an unsettled Docker launch")

const maxVolumeLaunchDebtBytes = 64 << 10
const maxVolumeLaunchDebts = 4096

// VolumeLaunchOrigin is one of the closed, exact live mutation subjects. A
// parsed ID, recovery cleanup subject, or historical receipt cannot launch.
type VolumeLaunchOrigin struct {
	operation    OperationPhysicalSubject
	maintenance  MaintenancePhysicalSubject
	compensation MaintenanceCompensationSubject
}

func VolumeLaunchForOperation(subject OperationPhysicalSubject) VolumeLaunchOrigin {
	if !subject.Valid() || subject.state.mode != operationPhysicalExecution {
		return VolumeLaunchOrigin{}
	}
	return VolumeLaunchOrigin{operation: subject}
}
func VolumeLaunchForMaintenance(subject MaintenancePhysicalSubject) VolumeLaunchOrigin {
	if !subject.Valid() || subject.state.mode != maintenancePhysicalExecution {
		return VolumeLaunchOrigin{}
	}
	return VolumeLaunchOrigin{maintenance: subject}
}
func VolumeLaunchForCompensation(subject MaintenanceCompensationSubject) VolumeLaunchOrigin {
	if !subject.Valid() || !subject.SourceLaunchRequired() {
		return VolumeLaunchOrigin{}
	}
	return VolumeLaunchOrigin{compensation: subject}
}

// VolumeLaunchJournal is bound to the same authoritative file and commit gate
// as its originating callback operation. It stores only unresolved launches;
// a positive synchronous effect receipt consumes debt in the same transaction
// as any maintenance target/source phase transition.
type VolumeLaunchJournal struct{ store *CallbackStore }

type volumeLaunchDebtRecord struct {
	Schema    int                   `json:"schema"`
	Backend   string                `json:"backend"`
	StorageID string                `json:"storage_id"`
	Kind      string                `json:"kind"`
	SubjectID string                `json:"subject_id"`
	LeaseUUID string                `json:"lease_uuid"`
	Volumes   []fsidentity.Identity `json:"volumes"`
}

// VolumeLaunchDebt can only be completed with the successful step of its exact
// live subject. Reopened rows intentionally cannot mint this capability: a new
// process did not observe the original synchronous Create/Start completion.
type VolumeLaunchDebt struct {
	journal *VolumeLaunchJournal
	origin  VolumeLaunchOrigin
	record  volumeLaunchDebtRecord
}

func NewVolumeLaunchJournal(store *CallbackStore) (*VolumeLaunchJournal, error) {
	if store == nil || store.boltStore == nil || store.binding == nil {
		return nil, errors.New("volume launch requires an identity-bound callback journal")
	}
	j := &VolumeLaunchJournal{store: store}
	err := store.update(func(tx *bolt.Tx) error {
		if err := requireCompleteCallbackSchema(tx); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(volumeLaunchDebtBucketName); err != nil {
			return err
		}
		return j.validateTx(tx)
	})
	if err != nil {
		return nil, err
	}
	return j, nil
}

// Check runs under the caller's physical-directory reservations, before any
// bind preparation or writer retirement. Begin repeats it in the durable write
// immediately before launch, while those same reservations remain held.
func (j *VolumeLaunchJournal) Check(origin VolumeLaunchOrigin, volumes []fsidentity.Identity) error {
	r, err := j.record(origin, volumes)
	if err != nil {
		return err
	}
	return j.store.view(func(tx *bolt.Tx) error {
		if err := j.verifyOriginTx(tx, origin); err != nil {
			return err
		}
		return j.checkTx(tx, r)
	})
}

// CheckNamespace is a read-only refusal fence for the namespace coordinator.
// An outstanding Docker request holds the originating canonical paths as well
// as their physical IDs. Destroying/renaming them and recreating a new inode
// must not make that request's old bind source appear reusable.
func (j *VolumeLaunchJournal) CheckNamespace(leaseUUID string) error {
	if j == nil || j.store == nil || !canonicalInspectionUUID(leaseUUID) {
		return errors.New("volume namespace debt check requires an exact lease UUID")
	}
	backend, storage := j.store.journalBackendIdentity("")
	return j.store.view(func(tx *bolt.Tx) error {
		return visitVolumeLaunchDebtsTx(tx, func(r volumeLaunchDebtRecord) error {
			if r.Backend != backend || r.StorageID != storage.String() {
				return errors.New("volume launch debt belongs to another storage lineage")
			}
			if r.LeaseUUID == leaseUUID {
				return fmt.Errorf("%w: %s %s retains its canonical volume namespace", ErrVolumeLaunchUnsettled, r.Kind, r.SubjectID)
			}
			return nil
		})
	})
}

func (j *VolumeLaunchJournal) Begin(origin VolumeLaunchOrigin, volumes []fsidentity.Identity) (VolumeLaunchDebt, error) {
	r, err := j.record(origin, volumes)
	if err != nil {
		return VolumeLaunchDebt{}, err
	}
	data, err := json.Marshal(r)
	if err != nil {
		return VolumeLaunchDebt{}, err
	}
	if _, err := decodeVolumeLaunchDebt([]byte(volumeLaunchKey(r)), data); err != nil {
		return VolumeLaunchDebt{}, err
	}
	err = j.store.update(func(tx *bolt.Tx) error {
		if err := j.verifyOriginTx(tx, origin); err != nil {
			return err
		}
		if err := j.checkTx(tx, r); err != nil {
			return err
		}
		bucket := tx.Bucket(volumeLaunchDebtBucketName)
		if bucket == nil {
			return errors.New("volume launch debt bucket missing")
		}
		if bucket.Stats().KeyN >= maxVolumeLaunchDebts {
			return errors.New("volume launch debt capacity exhausted")
		}
		if origin.maintenance.Valid() {
			if err := startMaintenanceTargetLaunchTx(tx, origin.maintenance); err != nil {
				return err
			}
		}
		if origin.compensation.Valid() {
			if err := startCompensationSourceLaunchTx(tx, origin.compensation); err != nil {
				return err
			}
		}
		return bucket.Put([]byte(volumeLaunchKey(r)), data)
	})
	if err != nil {
		return VolumeLaunchDebt{}, err
	}
	return VolumeLaunchDebt{journal: j, origin: origin, record: r}, nil
}

// Complete is the only debt-removal path. The receipt is non-forgeable, bound
// to the exact live subject/effect, single-use, and minted only after the full
// effect plus post-attestation succeeded. Any commit failure preserves debt.
func (j *VolumeLaunchJournal) Complete(debt VolumeLaunchDebt, completed substratemutation.CompletedStep) error {
	if j == nil || debt.journal != j || j.store == nil {
		return errors.New("volume launch debt belongs to another journal")
	}
	var consumeErr error
	switch {
	case debt.origin.operation.Valid():
		consumeErr = substratemutation.ConsumeCompletedStep(completed, debt.origin.operation, MaintenanceTargetLaunchStep)
	case debt.origin.maintenance.Valid():
		consumeErr = substratemutation.ConsumeCompletedStep(completed, debt.origin.maintenance, MaintenanceTargetLaunchStep)
	case debt.origin.compensation.Valid():
		consumeErr = substratemutation.ConsumeCompletedStep(completed, debt.origin.compensation, MaintenanceSourceLaunchStep)
	default:
		return errors.New("volume launch debt has no live subject")
	}
	if consumeErr != nil {
		return consumeErr
	}
	return j.store.update(func(tx *bolt.Tx) error {
		if err := j.verifyOriginTx(tx, debt.origin); err != nil {
			return err
		}
		bucket := tx.Bucket(volumeLaunchDebtBucketName)
		if bucket == nil {
			return errors.New("volume launch debt bucket missing")
		}
		key := []byte(volumeLaunchKey(debt.record))
		current, err := decodeVolumeLaunchDebt(key, bucket.Get(key))
		if err != nil {
			return err
		}
		if !equalVolumeLaunchDebt(current, debt.record) {
			return errors.New("volume launch debt changed")
		}
		if debt.origin.maintenance.Valid() {
			if err := recordMaintenanceTargetEffectsTx(tx, debt.origin.maintenance); err != nil {
				return err
			}
		}
		if debt.origin.compensation.Valid() {
			if err := completeCompensationSourceLaunchTx(tx, debt.origin.compensation); err != nil {
				return err
			}
		}
		return bucket.Delete(key)
	})
}

func (j *VolumeLaunchJournal) record(origin VolumeLaunchOrigin, volumes []fsidentity.Identity) (volumeLaunchDebtRecord, error) {
	if j == nil || j.store == nil {
		return volumeLaunchDebtRecord{}, errors.New("volume launch journal unavailable")
	}
	r := volumeLaunchDebtRecord{Schema: 1, Volumes: slices.Clone(volumes)}
	backend, storage := j.store.journalBackendIdentity("")
	r.Backend, r.StorageID = backend, storage.String()
	switch {
	case origin.operation.Valid():
		if origin.operation.state.settlement.callbacks != j.store || origin.operation.state.mode != operationPhysicalExecution {
			return r, errors.New("volume launch operation belongs to another journal")
		}
		r.Kind, r.SubjectID, r.LeaseUUID = "operation", origin.operation.OperationID().String(), origin.operation.LeaseUUID()
	case origin.maintenance.Valid():
		if origin.maintenance.state.settlement.callbacks != j.store || origin.maintenance.state.mode != maintenancePhysicalExecution {
			return r, errors.New("volume launch maintenance belongs to another journal")
		}
		r.Kind, r.SubjectID, r.LeaseUUID = "maintenance", origin.maintenance.MaintenanceID().String(), origin.maintenance.LeaseUUID()
	case origin.compensation.Valid():
		if origin.compensation.state.settlement.callbacks != j.store || !origin.compensation.SourceLaunchRequired() {
			return r, errors.New("volume launch compensation belongs to another journal or phase")
		}
		intent := origin.compensation.Intent()
		r.Kind, r.SubjectID, r.LeaseUUID = "compensation", intent.MaintenanceID().String(), intent.LeaseUUID()
	default:
		return r, errors.New("volume launch requires a live exact physical subject")
	}
	for _, id := range r.Volumes {
		if !id.Valid() {
			return r, errors.New("volume launch requires attested directory identities")
		}
	}
	slices.SortFunc(r.Volumes, compareVolumeIdentity)
	r.Volumes = slices.Compact(r.Volumes)
	encoded, err := json.Marshal(r)
	if err != nil {
		return r, err
	}
	if len(encoded) > maxVolumeLaunchDebtBytes {
		return r, errors.New("volume launch directory set exceeds durable receipt capacity")
	}
	return r, nil
}

func (j *VolumeLaunchJournal) verifyOriginTx(tx *bolt.Tx, origin VolumeLaunchOrigin) error {
	switch {
	case origin.operation.Valid():
		return verifyOperationIntentTx(tx, origin.operation.Intent())
	case origin.maintenance.Valid():
		return verifyMaintenanceIntentTx(tx, origin.maintenance.Intent())
	case origin.compensation.Valid():
		return verifyMaintenanceIntentTx(tx, origin.compensation.Intent())
	default:
		return errors.New("volume launch origin is invalid")
	}
}

func (j *VolumeLaunchJournal) checkTx(tx *bolt.Tx, proposed volumeLaunchDebtRecord) error {
	return visitVolumeLaunchDebtsTx(tx, func(current volumeLaunchDebtRecord) error {
		if current.Backend != proposed.Backend || current.StorageID != proposed.StorageID {
			return errors.New("volume launch debt belongs to another storage lineage")
		}
		if volumeLaunchKey(current) == volumeLaunchKey(proposed) || current.LeaseUUID == proposed.LeaseUUID {
			return fmt.Errorf("%w: %s %s", ErrVolumeLaunchUnsettled, current.Kind, current.SubjectID)
		}
		for _, id := range proposed.Volumes {
			if _, found := slices.BinarySearchFunc(current.Volumes, id, compareVolumeIdentity); found {
				return fmt.Errorf("%w: %s %s", ErrVolumeLaunchUnsettled, current.Kind, current.SubjectID)
			}
		}
		return nil
	})
}

func (j *VolumeLaunchJournal) validateTx(tx *bolt.Tx) error {
	backend, storage := j.store.journalBackendIdentity("")
	return visitVolumeLaunchDebtsTx(tx, func(r volumeLaunchDebtRecord) error {
		if r.Backend != backend || r.StorageID != storage.String() {
			return errors.New("volume launch debt belongs to another storage lineage")
		}
		return nil
	})
}

func visitVolumeLaunchDebtsTx(tx *bolt.Tx, visit func(volumeLaunchDebtRecord) error) error {
	bucket := tx.Bucket(volumeLaunchDebtBucketName)
	if bucket == nil {
		return nil
	}
	count := 0
	return bucket.ForEach(func(key, value []byte) error {
		count++
		if count > maxVolumeLaunchDebts {
			return errors.New("volume launch debt journal exceeds capacity")
		}
		r, err := decodeVolumeLaunchDebt(key, value)
		if err != nil {
			return err
		}
		if visit != nil {
			return visit(r)
		}
		return nil
	})
}

func volumeLaunchKey(r volumeLaunchDebtRecord) string { return r.Kind + ":" + r.SubjectID }
func equalVolumeLaunchDebt(a, b volumeLaunchDebtRecord) bool {
	return a.Schema == b.Schema && a.Backend == b.Backend && a.StorageID == b.StorageID && a.Kind == b.Kind && a.SubjectID == b.SubjectID && a.LeaseUUID == b.LeaseUUID && slices.Equal(a.Volumes, b.Volumes)
}
func compareVolumeIdentity(a, b fsidentity.Identity) int {
	if a.Device < b.Device || (a.Device == b.Device && a.Inode < b.Inode) {
		return -1
	}
	if a == b {
		return 0
	}
	return 1
}
func decodeVolumeLaunchDebt(key, value []byte) (volumeLaunchDebtRecord, error) {
	var r volumeLaunchDebtRecord
	if err := decodeStrictAuthoritativeObject(value, maxVolumeLaunchDebtBytes, &r); err != nil {
		return r, fmt.Errorf("decode volume launch debt: %w", err)
	}
	storage, err := backendidentity.Parse(r.StorageID)
	if err != nil || !storage.Valid() || r.Schema != 1 || r.Backend == "" || len(r.Backend) > 256 ||
		(r.Kind != "operation" && r.Kind != "maintenance" && r.Kind != "compensation") ||
		!canonicalInspectionUUID(r.SubjectID) || !canonicalInspectionUUID(r.LeaseUUID) || string(key) != volumeLaunchKey(r) || strings.ContainsAny(r.Backend, "\x00\r\n") {
		return r, errors.New("invalid volume launch debt identity")
	}
	for index, id := range r.Volumes {
		if !id.Valid() || (index > 0 && compareVolumeIdentity(r.Volumes[index-1], id) >= 0) {
			return r, errors.New("volume launch directory identities must be valid, sorted and distinct")
		}
	}
	return r, nil
}
