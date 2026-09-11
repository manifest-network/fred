package shared

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/opencontainers/go-digest"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendidentity"
)

// This optional extension lives in the already sealed callback journal, but is
// deliberately independent of its lease heads and close compaction. A helper
// may still appear after the operation which requested its Create has ended.
var imageInspectionsBucketName = []byte("docker_image_inspections_v1")

const maxImageInspectionRecordBytes = 16 << 10
const maxImageInspectionReceipts = 100_000

// ImageInspectionOrigin is preparation authority from one exact live Started
// subject. It cannot be assembled from a lease/operation ID or recovered from a
// historical receipt. Allocation rechecks its journal head in the write itself.
type ImageInspectionOrigin struct {
	operation    OperationPhysicalSubject
	maintenance  MaintenancePhysicalSubject
	compensation MaintenanceCompensationSubject
}

func ImageInspectionForOperation(subject OperationPhysicalSubject) ImageInspectionOrigin {
	if !subject.Valid() || subject.state.mode != operationPhysicalExecution {
		return ImageInspectionOrigin{}
	}
	return ImageInspectionOrigin{operation: subject}
}

func ImageInspectionForMaintenance(subject MaintenancePhysicalSubject) ImageInspectionOrigin {
	if !subject.Valid() || subject.state.mode != maintenancePhysicalExecution {
		return ImageInspectionOrigin{}
	}
	return ImageInspectionOrigin{maintenance: subject}
}

// ImageInspectionForCompensation authorizes preparation of the exact captured
// source before its first launch. A dispatched/settled recovery subject can
// observe its containers but cannot create fresh image helpers.
func ImageInspectionForCompensation(subject MaintenanceCompensationSubject) ImageInspectionOrigin {
	if !subject.Valid() || !subject.SourceLaunchRequired() {
		return ImageInspectionOrigin{}
	}
	return ImageInspectionOrigin{compensation: subject}
}

// ImageInspectionJournal issues exact helper receipts. It shares the callback
// store's pathname attestation, commit gate and close lifetime; it never opens a
// second unbound database or gains authority from a caller-selected storage ID.
type ImageInspectionJournal struct{ store *CallbackStore }

type imageInspectionRecord struct {
	Schema         int    `json:"schema"`
	ID             string `json:"id"`
	Backend        string `json:"backend"`
	StorageID      string `json:"storage_id"`
	Kind           string `json:"kind"`
	SubjectID      string `json:"subject_id"`
	LeaseUUID      string `json:"lease_uuid"`
	ImageID        string `json:"image_id"`
	ImageReference string `json:"image_reference"`
	ContainerID    string `json:"container_id,omitempty"`
}

// ImageInspectionReceipt binds one helper identity to its issuing open journal.
// A receipt with no ContainerID retains uncertain Create authority indefinitely;
// an empty inventory cannot prove that a canceled daemon call will not appear.
type ImageInspectionReceipt struct {
	journal *ImageInspectionJournal
	record  imageInspectionRecord
}

func (r ImageInspectionReceipt) ID() string      { return r.record.ID }
func (r ImageInspectionReceipt) Name() string    { return "fred-inspect-" + r.record.ID }
func (r ImageInspectionReceipt) Backend() string { return r.record.Backend }
func (r ImageInspectionReceipt) StorageID() backendidentity.ID {
	id, _ := backendidentity.Parse(r.record.StorageID)
	return id
}
func (r ImageInspectionReceipt) Kind() string           { return r.record.Kind }
func (r ImageInspectionReceipt) SubjectID() string      { return r.record.SubjectID }
func (r ImageInspectionReceipt) LeaseUUID() string      { return r.record.LeaseUUID }
func (r ImageInspectionReceipt) ImageID() string        { return r.record.ImageID }
func (r ImageInspectionReceipt) ImageReference() string { return r.record.ImageReference }
func (r ImageInspectionReceipt) ContainerID() string    { return r.record.ContainerID }

func NewImageInspectionJournal(store *CallbackStore) (*ImageInspectionJournal, error) {
	if store == nil || store.boltStore == nil || store.binding == nil {
		return nil, errors.New("image inspection requires an identity-bound callback journal")
	}
	j := &ImageInspectionJournal{store: store}
	err := store.update(func(tx *bolt.Tx) error {
		if err := requireCompleteCallbackSchema(tx); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(imageInspectionsBucketName); err != nil {
			return err
		}
		return j.validateTx(tx)
	})
	if err != nil {
		return nil, fmt.Errorf("open image inspection journal: %w", err)
	}
	return j, nil
}

// Reserve persists the random name and exact immutable image before Create can
// run. Origin and record allocation share one transaction with the current-head
// check; a close or successor cannot lend an older subject fresh create rights.
func (j *ImageInspectionJournal) Reserve(origin ImageInspectionOrigin, imageID, reference string) (ImageInspectionReceipt, error) {
	if j == nil || j.store == nil {
		return ImageInspectionReceipt{}, errors.New("image inspection journal unavailable")
	}
	r := imageInspectionRecord{Schema: 1, ID: uuid.NewString(), ImageID: imageID, ImageReference: reference}
	backendName, storage := j.store.journalBackendIdentity("")
	r.Backend, r.StorageID = backendName, storage.String()
	var check func(*bolt.Tx) error
	switch {
	case origin.operation.Valid():
		s := origin.operation
		if s.state.mode != operationPhysicalExecution || s.state.settlement.callbacks != j.store {
			return ImageInspectionReceipt{}, errors.New("image inspection operation belongs to another journal")
		}
		r.Kind, r.SubjectID, r.LeaseUUID = "operation", s.OperationID().String(), s.LeaseUUID()
		check = func(tx *bolt.Tx) error { return verifyOperationIntentTx(tx, s.Intent()) }
	case origin.maintenance.Valid():
		s := origin.maintenance
		if s.state.mode != maintenancePhysicalExecution || s.state.settlement.callbacks != j.store {
			return ImageInspectionReceipt{}, errors.New("image inspection maintenance belongs to another journal")
		}
		r.Kind, r.SubjectID, r.LeaseUUID = "maintenance", s.MaintenanceID().String(), s.LeaseUUID()
		check = func(tx *bolt.Tx) error { return verifyMaintenanceIntentTx(tx, s.Intent()) }
	case origin.compensation.Valid():
		s := origin.compensation
		if !s.SourceLaunchRequired() || s.state.settlement.callbacks != j.store {
			return ImageInspectionReceipt{}, errors.New("image inspection compensation belongs to another journal or launch phase")
		}
		r.Kind, r.SubjectID, r.LeaseUUID = "compensation", s.Intent().MaintenanceID().String(), s.Intent().LeaseUUID()
		check = func(tx *bolt.Tx) error { return verifyCompensationSourcePreparationTx(tx, s) }
	default:
		return ImageInspectionReceipt{}, errors.New("image inspection requires a live Started subject")
	}
	data, err := encodeImageInspection(r)
	if err != nil {
		return ImageInspectionReceipt{}, err
	}
	err = j.store.update(func(tx *bolt.Tx) error {
		if err := check(tx); err != nil {
			return err
		}
		bucket := tx.Bucket(imageInspectionsBucketName)
		if bucket == nil {
			return errors.New("image inspection journal bucket missing")
		}
		if bucket.Stats().KeyN >= maxImageInspectionReceipts {
			return errors.New("image inspection recovery receipt capacity exhausted")
		}
		if bucket.Get([]byte(r.ID)) != nil {
			return errors.New("image inspection identity already reserved")
		}
		return bucket.Put([]byte(r.ID), data)
	})
	if err != nil {
		return ImageInspectionReceipt{}, err
	}
	return ImageInspectionReceipt{journal: j, record: r}, nil
}

// RecordCreated records the synchronous Create response. Recovery of an
// uncertain Create deliberately does not call this: finding one late container
// is not evidence that the original daemon request has finished.
func (j *ImageInspectionJournal) RecordCreated(receipt ImageInspectionReceipt, id string) (ImageInspectionReceipt, error) {
	if receipt.record.ContainerID != "" {
		return ImageInspectionReceipt{}, errors.New("image inspection Create already recorded")
	}
	r := receipt.record
	r.ContainerID = id
	data, err := encodeImageInspection(r)
	if err != nil {
		return ImageInspectionReceipt{}, err
	}
	if id == "" {
		return ImageInspectionReceipt{}, errors.New("image inspection Create returned an empty container ID")
	}
	err = j.change(receipt, func(bucket *bolt.Bucket) error { return bucket.Put([]byte(r.ID), data) })
	if err != nil {
		return ImageInspectionReceipt{}, err
	}
	return ImageInspectionReceipt{journal: j, record: r}, nil
}

// ForgetRemoved consumes only a response-complete receipt after its owner has
// established exact absence. Response-loss receipts can never take this path;
// they remain available to remove later appearances even after workload close.
func (j *ImageInspectionJournal) ForgetRemoved(receipt ImageInspectionReceipt) error {
	if receipt.record.ContainerID == "" {
		return errors.New("uncertain image inspection Create requires a permanent recovery receipt")
	}
	return j.change(receipt, func(bucket *bolt.Bucket) error { return bucket.Delete([]byte(receipt.ID())) })
}

func (j *ImageInspectionJournal) change(receipt ImageInspectionReceipt, change func(*bolt.Bucket) error) error {
	if j == nil || j.store == nil || receipt.journal != j {
		return errors.New("image inspection receipt belongs to another journal")
	}
	return j.store.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(imageInspectionsBucketName)
		if bucket == nil {
			return errors.New("image inspection journal bucket missing")
		}
		current, err := decodeImageInspection([]byte(receipt.ID()), bucket.Get([]byte(receipt.ID())))
		if err != nil {
			return err
		}
		if current != receipt.record {
			return errors.New("image inspection receipt changed")
		}
		return change(bucket)
	})
}

func (j *ImageInspectionJournal) List() ([]ImageInspectionReceipt, error) {
	if j == nil || j.store == nil {
		return nil, errors.New("image inspection journal unavailable")
	}
	var receipts []ImageInspectionReceipt
	err := j.store.view(func(tx *bolt.Tx) error {
		if err := j.validateTx(tx); err != nil {
			return err
		}
		return tx.Bucket(imageInspectionsBucketName).ForEach(func(key, value []byte) error {
			r, err := decodeImageInspection(key, value)
			if err != nil {
				return err
			}
			receipts = append(receipts, ImageInspectionReceipt{journal: j, record: r})
			return nil
		})
	})
	return receipts, err
}

func (j *ImageInspectionJournal) validateTx(tx *bolt.Tx) error {
	backend, storage := j.store.journalBackendIdentity("")
	return visitImageInspectionsTx(tx, func(r imageInspectionRecord) error {
		if r.Backend != backend || r.StorageID != storage.String() {
			return errors.New("image inspection receipt belongs to another storage lineage")
		}
		return nil
	})
}

func visitImageInspectionsTx(tx *bolt.Tx, visit func(imageInspectionRecord) error) error {
	bucket := tx.Bucket(imageInspectionsBucketName)
	if bucket == nil {
		return nil
	} // Optional extension: drained v0.13 journals remain adoptable.
	count := 0
	return bucket.ForEach(func(key, value []byte) error {
		count++
		if count > maxImageInspectionReceipts {
			return errors.New("image inspection journal exceeds receipt limit")
		}
		r, err := decodeImageInspection(key, value)
		if err != nil {
			return err
		}
		if visit != nil {
			return visit(r)
		}
		return nil
	})
}

func encodeImageInspection(r imageInspectionRecord) ([]byte, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	_, err = decodeImageInspection([]byte(r.ID), data)
	return data, err
}

func decodeImageInspection(key, value []byte) (imageInspectionRecord, error) {
	var r imageInspectionRecord
	if err := decodeStrictAuthoritativeObject(value, maxImageInspectionRecordBytes, &r); err != nil {
		return r, fmt.Errorf("decode image inspection receipt: %w", err)
	}
	storage, storageErr := backendidentity.Parse(r.StorageID)
	if r.Schema != 1 || r.ID != string(key) || !canonicalInspectionUUID(r.ID) || storageErr != nil || !storage.Valid() ||
		!canonicalInspectionUUID(r.SubjectID) || !canonicalInspectionUUID(r.LeaseUUID) ||
		(r.Kind != "operation" && r.Kind != "maintenance" && r.Kind != "compensation") || r.Backend == "" || len(r.Backend) > 256 || !utf8.ValidString(r.Backend) ||
		r.ImageReference == "" || len(r.ImageReference) > 4096 || !utf8.ValidString(r.ImageReference) {
		return r, errors.New("invalid image inspection receipt identity")
	}
	d := digest.Digest(r.ImageID)
	if d.Validate() != nil || d.Algorithm() != digest.SHA256 {
		return r, errors.New("invalid image inspection immutable image")
	}
	if r.ContainerID != "" && (len(r.ContainerID) != 64 || strings.Trim(r.ContainerID, "0123456789abcdef") != "") {
		return r, errors.New("invalid image inspection container ID")
	}
	return r, nil
}

func canonicalInspectionUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}
