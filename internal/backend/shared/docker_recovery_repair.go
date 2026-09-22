package shared

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendidentity"
)

// DockerRecoveryInspection describes pending effects without granting runtime
// mutation authority. Acknowledgement is an exact operator statement, not a
// daemon observation: neither an empty inventory nor a timeout proves a fence.
type DockerRecoveryInspection struct {
	Verdict         string   `json:"verdict"`
	Backend         string   `json:"backend"`
	StorageID       string   `json:"storage_id"`
	Database        string   `json:"database"`
	SnapshotSHA256  string   `json:"snapshot_sha256"`
	Launches        int      `json:"launches"`
	UnknownHelpers  int      `json:"unknown_helpers"`
	Leases          []string `json:"leases"`
	Acknowledgement string   `json:"acknowledgement"`
}

// DockerRecoveryRepairResult reports the backup and exact repair outcome. A
// nonempty Backup on error must be preserved. DOCKER_EFFECTS_FENCED with nil
// error confirms completion; an error requires read-only reinspection under
// the same external fence before restarting normal recovery.
type DockerRecoveryRepairResult struct {
	Verdict        string `json:"verdict"`
	Backup         string `json:"backup,omitempty"`
	SnapshotSHA256 string `json:"snapshot_sha256"`
	Launches       int    `json:"launches"`
	UnknownHelpers int    `json:"unknown_helpers"`
}

func inspectDockerRecoveryTx(tx *bolt.Tx, database string, storage backendidentity.VerifiedStorage) (DockerRecoveryInspection, error) {
	inspection := DockerRecoveryInspection{Verdict: "DOCKER_EFFECTS_INSPECTED", Backend: storage.BackendName(), StorageID: storage.ID().String(), Database: database}
	if err := verifyStoreIdentityBinding(tx, authoritativeStoreCallbacks, storage.ID()); err != nil {
		return inspection, err
	}
	if err := requireCompleteCallbackSchema(tx); err != nil {
		return inspection, err
	}
	if err := validateMaintenanceCompensationsTx(tx); err != nil {
		return inspection, err
	}
	if err := visitVolumeLaunchDebtsTx(tx, func(record volumeLaunchDebtRecord) error {
		if record.Backend != storage.BackendName() || record.StorageID != storage.ID().String() {
			return errors.New("launch debt belongs to another backend storage lineage")
		}
		inspection.Launches++
		inspection.Leases = append(inspection.Leases, record.LeaseUUID)
		return nil
	}); err != nil {
		return inspection, err
	}
	if err := visitImageInspectionsTx(tx, func(record imageInspectionRecord) error {
		if record.Backend != storage.BackendName() || record.StorageID != storage.ID().String() {
			return errors.New("helper receipt belongs to another backend storage lineage")
		}
		if !record.creationSettled() {
			inspection.UnknownHelpers++
			inspection.Leases = append(inspection.Leases, record.LeaseUUID)
		}
		return nil
	}); err != nil {
		return inspection, err
	}
	slices.Sort(inspection.Leases)
	inspection.Leases = slices.Compact(inspection.Leases)
	digest := sha256.New()
	if _, err := tx.WriteTo(digest); err != nil {
		return inspection, err
	}
	inspection.SnapshotSHA256 = hex.EncodeToString(digest.Sum(nil))
	inspection.Acknowledgement = fmt.Sprintf("I have stopped all Fred clients for backend %q, fenced and drained all earlier Docker and container-runtime requests on its host, and prevented old clients from resuming; I authorize settling only snapshot %s of %q for storage %s", inspection.Backend, inspection.SnapshotSHA256, database, inspection.StorageID)
	return inspection, nil
}

// InspectDockerRecovery opens only an existing, identity-bound callback journal
// read-only. It does not create buckets or start maintenance workers.
func InspectDockerRecovery(ctx context.Context, path string, storage backendidentity.VerifiedStorage) (inspection DockerRecoveryInspection, err error) {
	if ctx == nil || !storage.Valid() {
		return inspection, errors.New("docker recovery inspection requires context and verified storage")
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return inspection, err
	}
	bound, err := BindAuthoritativeStorePath(path)
	if err != nil {
		return inspection, err
	}
	defer func() { err = errors.Join(err, bound.Close()) }()
	file, err := boundAuthoritativeStoreFile(bound)
	if err != nil {
		return inspection, err
	}
	db, info, err := openExistingBoltDBFile(file, true, false)
	if err != nil {
		return inspection, err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	err = db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		inspection, err = inspectDockerRecoveryTx(tx, path, storage)
		return err
	})
	return inspection, errors.Join(err, bound.VerifyPath(), verifyStoreFileIdentity(file, info), ctx.Err())
}

// fencedDockerEffects exists only in one stopped repair session. Runtime
// journals cannot mint or consume it. Its exact snapshot and statement bind the
// external operator's fencing fact to the exclusively opened database.
type fencedDockerEffects struct {
	inspection DockerRecoveryInspection
	consumed   bool
	txID       int
}

// RepairDockerRecovery is the explicit offline escape from an unknown remote
// call, not a retry policy. Exclusive database ownership excludes the backend;
// the exact acknowledgement supplies the external daemon/runtime fencing fact.
// verify must re-attest the configured daemon and storage marker lineage.
func RepairDockerRecovery(ctx context.Context, path string, storage backendidentity.VerifiedStorage, acknowledgement, backupPath string, verify func(context.Context) error) (result DockerRecoveryRepairResult, err error) {
	if ctx == nil || !storage.Valid() || verify == nil || acknowledgement == "" || backupPath == "" {
		return result, errors.New("offline Docker repair requires verified storage, exact fencing acknowledgement and a new backup path")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := verify(ctx); err != nil {
		return result, err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return result, err
	}
	gate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
	if err != nil {
		return result, err
	}
	store, err := openIdentityBoundBoltStore(boltStoreConfig{DBPath: path, BucketName: callbackBucketName, Label: "offline Docker repair"}, authoritativeStoreCallbacks, storage, gate)
	if err != nil {
		return result, fmt.Errorf("stop docker-backend before opening its repair journal: %w", err)
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	var proof fencedDockerEffects
	err = store.view(func(tx *bolt.Tx) error {
		var checkFailure error
		for checkErr := range tx.Check() {
			checkFailure = errors.Join(checkFailure, checkErr)
		}
		if checkFailure != nil {
			return checkFailure
		}
		var err error
		proof.txID = tx.ID()
		proof.inspection, err = inspectDockerRecoveryTx(tx, path, storage)
		return err
	})
	if err != nil {
		return result, err
	}
	if acknowledgement != proof.inspection.Acknowledgement {
		return result, errors.New("fencing acknowledgement does not match the current backend, storage and database snapshot; inspect again")
	}
	if proof.inspection.Launches == 0 && proof.inspection.UnknownHelpers == 0 {
		return result, errors.New("no unknown Docker effects require repair")
	}
	result.SnapshotSHA256 = proof.inspection.SnapshotSHA256
	result.Launches, result.UnknownHelpers = proof.inspection.Launches, proof.inspection.UnknownHelpers
	backup, err := BindAuthoritativeStorePath(backupPath)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, backup.Close()) }()
	if err := errors.Join(ctx.Err(), verify(ctx), backup.VerifyPath()); err != nil {
		return result, err
	}
	// Consume the session before backup publication. An error preserves the
	// created file and requires a fresh session and a different backup path.
	proof.consumed = true
	backupFile, err := backup.entry.OpenFile(os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return result, err
	}
	result.Backup = backup.entry.DisplayPath()
	defer func() { err = errors.Join(err, backupFile.Close()) }()
	defer func() {
		if err != nil {
			err = fmt.Errorf("BACKUP CREATED at %q; preserve it and inspect before retry: %w", result.Backup, err)
		}
	}()
	if err := store.view(func(tx *bolt.Tx) error { _, err := tx.WriteTo(backupFile); return err }); err != nil {
		return result, err
	}
	if err := errors.Join(backupFile.Sync(), backup.entry.SyncParent()); err != nil {
		return result, err
	}
	info, err := backupFile.Stat()
	if err != nil {
		return result, err
	}
	verifyBackup := func() error {
		if err := errors.Join(backup.VerifyPath(), verifyStoreFileIdentity(backup.entry, info)); err != nil {
			return err
		}
		if _, err := backupFile.Seek(0, io.SeekStart); err != nil {
			return err
		}
		digest := sha256.New()
		if _, err := io.Copy(digest, backupFile); err != nil {
			return err
		}
		if hex.EncodeToString(digest.Sum(nil)) != proof.inspection.SnapshotSHA256 {
			return errors.New("docker recovery backup does not match the inspected snapshot")
		}
		return nil
	}
	if err := errors.Join(ctx.Err(), verify(ctx), verifyBackup()); err != nil {
		return result, err
	}
	err = store.update(func(tx *bolt.Tx) error {
		if err := errors.Join(ctx.Err(), verify(ctx), verifyBackup()); err != nil {
			return err
		}
		return settleFencedDockerEffectsTx(tx, proof)
	})
	if err != nil {
		result.Verdict = "REPAIR_NOT_CONFIRMED"
		return result, err
	}
	result.Verdict = "REPAIR_COMMITTED"
	if err := errors.Join(store.db.Sync(), ctx.Err(), verify(ctx), verifyBackup()); err != nil {
		return result, fmt.Errorf("COMMITTED: Docker effect repair requires read-only inspection: %w", err)
	}
	if err := store.Close(); err != nil {
		return result, fmt.Errorf("COMMITTED: close Docker repair journal: %w", err)
	}
	post, err := InspectDockerRecovery(ctx, path, storage)
	if err != nil || post.Launches != 0 || post.UnknownHelpers != 0 {
		return result, fmt.Errorf("COMMITTED: Docker repair postcondition failed: %w", errors.Join(err, errors.New("pending effects must be re-inspected")))
	}
	result.Verdict = "DOCKER_EFFECTS_FENCED"
	return result, nil
}

func settleFencedDockerEffectsTx(tx *bolt.Tx, proof fencedDockerEffects) error {
	if !proof.consumed || proof.inspection.SnapshotSHA256 == "" || tx.ID() != proof.txID+1 {
		return errors.New("offline Docker fence proof unavailable")
	}
	// A writable transaction has the next txid; the exclusive owner's snapshot
	// was checked before Begin. Validate all target rows again before changing any.
	var launches []volumeLaunchDebtRecord
	if err := visitVolumeLaunchDebtsTx(tx, func(record volumeLaunchDebtRecord) error {
		if record.Backend != proof.inspection.Backend || record.StorageID != proof.inspection.StorageID {
			return errors.New("docker launch storage lineage changed")
		}
		launches = append(launches, record)
		return nil
	}); err != nil {
		return err
	}
	if len(launches) != proof.inspection.Launches {
		return errors.New("docker launch set changed")
	}
	for _, record := range launches {
		if record.Kind != "operation" {
			head, present, err := getLeaseMutationHeadTx(tx, record.LeaseUUID)
			if err != nil {
				return err
			}
			if maintenance, ok := head.(maintenanceLeaseMutationHead); present && ok && maintenance.claim.MaintenanceID().String() == record.SubjectID {
				compensation, err := readCompensationTx(tx, maintenance.claim)
				if err != nil {
					return err
				}
				if compensation != nil && compensation.Phase != compensationUnavailable {
					switch {
					case record.Kind == "maintenance" && compensation.Phase == compensationTargetDispatching:
						compensation.Phase = compensationTargetSettled
					case record.Kind == "compensation" && compensation.Phase == compensationSourceDispatching:
						compensation.Phase = compensationSourceSettled
					case record.Kind == "compensation" && compensation.Phase == compensationSourceReady:
						// Recovery may already have attested the complete ready
						// source while the original request's reply remained
						// unknown. Preserve that observation; the offline fence
						// settles only the request still held in this journal.
					default:
						return errors.New("launch debt and compensation phase disagree")
					}
					if err := writeCompensationTx(tx, *compensation); err != nil {
						return err
					}
				}
			}
		}
		if err := tx.Bucket(volumeLaunchDebtBucketName).Delete([]byte(volumeLaunchKey(record))); err != nil {
			return err
		}
	}
	var helpers []imageInspectionRecord
	if err := visitImageInspectionsTx(tx, func(record imageInspectionRecord) error {
		if !record.creationSettled() {
			helpers = append(helpers, record)
		}
		return nil
	}); err != nil {
		return err
	}
	if len(helpers) != proof.inspection.UnknownHelpers {
		return errors.New("unknown image helper set changed")
	}
	for _, record := range helpers {
		record.CreationFence = "operator"
		data, err := encodeImageInspection(record)
		if err != nil {
			return err
		}
		if err := tx.Bucket(imageInspectionsBucketName).Put([]byte(record.ID), data); err != nil {
			return err
		}
	}
	return nil
}
