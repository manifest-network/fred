package shared

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/opencontainers/go-digest"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
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
	CreationFence  string `json:"creation_fence,omitempty"`
}

func (r imageInspectionRecord) creationSettled() bool {
	return r.ContainerID != "" || r.CreationFence == "operator" || r.CreationFence == "response"
}

// PreparedImageInspection binds a fresh helper identity to an exact live
// Started origin. It grants no cleanup authority until Reserve persists it
// inside the helper's admitted creation step.
type PreparedImageInspection struct {
	journal *ImageInspectionJournal
	record  imageInspectionRecord
	origin  ImageInspectionOrigin
	use     *imageInspectionPreparationUse
}

type imageInspectionPreparationUse struct{ reserved atomic.Bool }

const ImageInspectionCreationStep = "create image inspection helper"

// ImageInspectionReceipt binds one helper identity to its issuing open journal.
// A receipt without a response or offline fence retains uncertain Create authority;
// an empty inventory cannot prove that a canceled daemon call will not appear.
type ImageInspectionReceipt struct {
	journal  *ImageInspectionJournal
	record   imageInspectionRecord
	prepared PreparedImageInspection
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

// CreationSettled reports a completed response or explicit offline daemon fence.
// It does not prove helper absence; cleanup must still inspect exact ownership.
func (r ImageInspectionReceipt) CreationSettled() bool { return r.record.creationSettled() }

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

// Prepare creates an immutable identity after rereading the exact Started head.
// It does not allocate durable cleanup debt before mutation admission.
func (j *ImageInspectionJournal) Prepare(origin ImageInspectionOrigin, imageID, reference string) (PreparedImageInspection, error) {
	if j == nil || j.store == nil {
		return PreparedImageInspection{}, errors.New("image inspection journal unavailable")
	}
	r := imageInspectionRecord{Schema: 1, ID: uuid.NewString(), ImageID: imageID, ImageReference: reference}
	backendName, storage := j.store.journalBackendIdentity("")
	r.Backend, r.StorageID = backendName, storage.String()
	switch {
	case origin.operation.Valid():
		s := origin.operation
		if s.state.mode != operationPhysicalExecution || s.state.settlement.callbacks != j.store {
			return PreparedImageInspection{}, errors.New("image inspection operation belongs to another journal")
		}
		r.Kind, r.SubjectID, r.LeaseUUID = "operation", s.OperationID().String(), s.LeaseUUID()
	case origin.maintenance.Valid():
		s := origin.maintenance
		if s.state.mode != maintenancePhysicalExecution || s.state.settlement.callbacks != j.store {
			return PreparedImageInspection{}, errors.New("image inspection maintenance belongs to another journal")
		}
		r.Kind, r.SubjectID, r.LeaseUUID = "maintenance", s.MaintenanceID().String(), s.LeaseUUID()
	case origin.compensation.Valid():
		s := origin.compensation
		if !s.SourceLaunchRequired() || s.state.settlement.callbacks != j.store {
			return PreparedImageInspection{}, errors.New("image inspection compensation belongs to another journal or launch phase")
		}
		r.Kind, r.SubjectID, r.LeaseUUID = "compensation", s.Intent().MaintenanceID().String(), s.Intent().LeaseUUID()
	default:
		return PreparedImageInspection{}, errors.New("image inspection requires a live Started subject")
	}
	if _, err := encodeImageInspection(r); err != nil {
		return PreparedImageInspection{}, err
	}
	p := PreparedImageInspection{journal: j, record: r, origin: origin, use: new(imageInspectionPreparationUse)}
	if err := j.store.view(p.verifyOrigin); err != nil {
		return PreparedImageInspection{}, err
	}
	return p, nil
}

func (p PreparedImageInspection) verifyOrigin(tx *bolt.Tx) error {
	switch {
	case p.origin.operation.Valid():
		return verifyOperationIntentTx(tx, p.origin.operation.Intent())
	case p.origin.maintenance.Valid():
		return verifyMaintenanceIntentTx(tx, p.origin.maintenance.Intent())
	case p.origin.compensation.Valid():
		return verifyCompensationSourcePreparationTx(tx, p.origin.compensation)
	default:
		return errors.New("image inspection requires a live Started subject")
	}
}

// Reserve persists an exact prepared identity immediately before Create. The
// write rechecks its origin so a close or successor cannot lend stale rights.
func (j *ImageInspectionJournal) Reserve(p PreparedImageInspection) (ImageInspectionReceipt, error) {
	if j == nil || j.store == nil || p.journal != j || p.use == nil {
		return ImageInspectionReceipt{}, errors.New("prepared image inspection belongs to another journal")
	}
	// A copied preparation cannot recreate the same helper after cleanup has
	// removed its row. Failed allocation also spends this attempt; a fresh
	// preparation must reread the live origin and choose a new random identity.
	if !p.use.reserved.CompareAndSwap(false, true) {
		return ImageInspectionReceipt{}, errors.New("prepared image inspection was already reserved")
	}
	r := p.record
	data, err := encodeImageInspection(r)
	if err != nil {
		return ImageInspectionReceipt{}, err
	}
	err = j.store.update(func(tx *bolt.Tx) error {
		if err := p.verifyOrigin(tx); err != nil {
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
	return ImageInspectionReceipt{journal: j, record: r, prepared: p}, nil
}

// RecordCreationSettled records a daemon response only after the exact creation
// bracket passed its storage postcheck. An error response may have no ID;
// cleanup still requires exact owned-helper absence before forgetting the row.
func (j *ImageInspectionJournal) RecordCreationSettled(receipt ImageInspectionReceipt, id string, completed substratemutation.CompletedStep) (ImageInspectionReceipt, error) {
	if j == nil || j.store == nil || receipt.journal != j || receipt.prepared.journal != j {
		return ImageInspectionReceipt{}, errors.New("image inspection creation belongs to another journal")
	}
	if receipt.record.creationSettled() {
		return ImageInspectionReceipt{}, errors.New("image inspection Create already recorded")
	}
	if receipt.record != receipt.prepared.record {
		return ImageInspectionReceipt{}, errors.New("image inspection creation differs from its preparation")
	}
	r := receipt.record
	r.ContainerID = id
	r.CreationFence = "response"
	data, err := encodeImageInspection(r)
	if err != nil {
		return ImageInspectionReceipt{}, err
	}
	err = substratemutation.CommitCompletedStep(completed, receipt.prepared, ImageInspectionCreationStep, func() error {
		return j.change(receipt, func(bucket *bolt.Bucket) error { return bucket.Put([]byte(r.ID), data) })
	})
	if err != nil {
		return ImageInspectionReceipt{}, err
	}
	return ImageInspectionReceipt{journal: j, record: r, prepared: receipt.prepared}, nil
}

// ForgetRemoved consumes a response-complete or explicitly fenced receipt after
// its owner established exact absence. Unfenced response-loss receipts remain
// available to remove later appearances even after workload close.
func (j *ImageInspectionJournal) ForgetRemoved(receipt ImageInspectionReceipt) error {
	if !receipt.record.creationSettled() {
		return errors.New("uncertain image inspection Create requires completion or an offline daemon fence")
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
	return visitImageInspectionsContextTx(context.Background(), tx, visit)
}

func visitImageInspectionsContextTx(ctx context.Context, tx *bolt.Tx, visit func(imageInspectionRecord) error) error {
	bucket := tx.Bucket(imageInspectionsBucketName)
	if bucket == nil {
		return nil
	} // Optional extension: drained v0.13 journals remain adoptable.
	count := 0
	return walkCallbackValidationRows(ctx, bucket, func(key, value []byte) error {
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
	if r.CreationFence != "" && r.CreationFence != "operator" && r.CreationFence != "response" {
		return r, errors.New("invalid image inspection creation fence")
	}
	return r, nil
}

func canonicalInspectionUUID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}
