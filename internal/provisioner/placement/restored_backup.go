package placement

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"

	bolt "go.etcd.io/bbolt"
)

// ErrRestoredBackupTarget means a restored-backup attestation plan no longer
// matches the exact database metadata it was derived from.
var ErrRestoredBackupTarget = errors.New("placement restored-backup attestation target changed")

const restoredBackupConfirmationDomain = "fred-placement-restored-backup-attestation-v1"

// RestoredBackupPlan binds the admission and drain evidence of one restored
// placement database to its exact current metadata record.
//
// A database restored from an older copy still carries the admission baseline
// and empty-backend drain evidence of the fleet it was copied from. Leases
// dispatched after the copy was taken have no row in it, and a current-looking
// baseline would let recordless admission read that absence as "never placed"
// and provision such a lease a second time on another backend. Attestation
// forgets both kinds of evidence, so recordless admission and backend removal
// wait for one complete inventory of the running fleet. It never grants
// authority: every other row, pin, marker, and journal is preserved exactly.
//
// Only AttemptRepair.PlanRestoredBackupAttestation mints a plan. Its zero value
// is invalid, and a plan is bound to the repair session that minted it.
type RestoredBackupPlan struct {
	issuer       *AttemptRepair
	metadata     []byte
	confirmation string
	facts        RestoredBackupFacts
}

// RestoredBackupFacts renders a plan for the operator. Required is false when
// the database already carries no admission baseline or drain evidence, so an
// attestation would change nothing.
type RestoredBackupFacts struct {
	ProviderUUID           string            `json:"provider_uuid"`
	DatabasePath           string            `json:"database_path"`
	Topology               []string          `json:"topology"`
	TopologyID             uint64            `json:"topology_id"`
	StorageIDs             map[string]string `json:"storage_ids"`
	BaselineTopologyID     uint64            `json:"baseline_topology_id"`
	InventoryTopologyID    uint64            `json:"inventory_topology_id"`
	EmptyInventoryBackends []string          `json:"empty_inventory_backends"`
	PendingInventorySweep  bool              `json:"pending_inventory_sweep"`
	Required               bool              `json:"required"`
}

// RestoredBackupResult is minted only by a successful attestation and carries
// the exact metadata record it committed.
type RestoredBackupResult struct {
	issuer   *AttemptRepair
	expected []byte
}

// ConfirmationValue is the exact operator confirmation for the plan. It binds
// the provider, the database path, and every byte of the metadata record, so
// any change between the dry run and the apply refuses the apply.
func (plan RestoredBackupPlan) ConfirmationValue() string { return plan.confirmation }

func (plan RestoredBackupPlan) Facts() RestoredBackupFacts {
	facts := plan.facts
	facts.Topology = slices.Clone(facts.Topology)
	facts.StorageIDs = maps.Clone(facts.StorageIDs)
	facts.EmptyInventoryBackends = slices.Clone(facts.EmptyInventoryBackends)
	return facts
}

// PlanRestoredBackupAttestation derives the attestation plan for the exclusively
// locked database. It performs only a read transaction.
func (repair *AttemptRepair) PlanRestoredBackupAttestation() (RestoredBackupPlan, error) {
	if repair == nil || repair.store == nil || repair.store.db == nil || repair.authority == nil {
		return RestoredBackupPlan{}, errors.New("placement repair is not open")
	}
	encoded, metadata, err := readRestoredBackupMetadata(repair.store.db)
	if err != nil {
		return RestoredBackupPlan{}, err
	}
	if metadata.TopologyID == 0 {
		return RestoredBackupPlan{}, fmt.Errorf(
			"%w: placement database has no configured backend topology", ErrRestoredBackupTarget,
		)
	}
	confirmation := restoredBackupConfirmation(metadata, repair.authority.path, encoded)
	empty := slices.Clone(metadata.EmptyInventoryBackends)
	if empty == nil {
		empty = []string{}
	}
	storageIDs := make(map[string]string, len(metadata.Topology))
	for _, backendName := range metadata.Topology {
		storageIDs[backendName] = metadata.KnownBackendStorageIDs[backendName]
	}
	return RestoredBackupPlan{
		issuer:       repair,
		metadata:     encoded,
		confirmation: confirmation,
		facts: RestoredBackupFacts{
			ProviderUUID:           metadata.ProviderUUID,
			DatabasePath:           repair.authority.path,
			Topology:               slices.Clone(metadata.Topology),
			TopologyID:             metadata.TopologyID,
			StorageIDs:             storageIDs,
			BaselineTopologyID:     metadata.BaselineTopologyID,
			InventoryTopologyID:    metadata.InventoryTopologyID,
			EmptyInventoryBackends: empty,
			PendingInventorySweep:  metadata.PendingInventorySweepID != 0,
			Required:               restoredBackupEvidencePresent(metadata),
		},
	}, nil
}

// AttestRestoredBackup forgets the plan's admission baseline and drain evidence
// in one transaction. The exact pre-mutation backup must already be published
// by CreateExactBackup on this session.
func (repair *AttemptRepair) AttestRestoredBackup(plan RestoredBackupPlan) (RestoredBackupResult, error) {
	if repair == nil || repair.store == nil || repair.store.db == nil || repair.authority == nil ||
		plan.issuer != repair {
		return RestoredBackupResult{}, fmt.Errorf(
			"%w: plan belongs to another repair session", ErrRestoredBackupTarget,
		)
	}
	if !plan.facts.Required {
		return RestoredBackupResult{}, fmt.Errorf(
			"%w: the database carries no admission baseline or drain evidence to forget",
			ErrRestoredBackupTarget,
		)
	}
	current, metadata, err := readRestoredBackupMetadata(repair.store.db)
	if err != nil {
		return RestoredBackupResult{}, err
	}
	if !bytes.Equal(current, plan.metadata) ||
		restoredBackupConfirmation(metadata, repair.authority.path, current) != plan.confirmation {
		return RestoredBackupResult{}, fmt.Errorf(
			"%w: placement metadata changed after the plan", ErrRestoredBackupTarget,
		)
	}
	attested := metadata
	attested.BaselineFingerprint = ""
	attested.BaselineTopologyID = 0
	attested.InventoryTopologyID = 0
	attested.EmptyInventoryBackends = nil
	expected, err := encodeTopologyMetadata(attested)
	if err != nil {
		return RestoredBackupResult{}, fmt.Errorf("encode attested placement metadata: %w", err)
	}
	if err := verifyBoltPhysicalConsistency(repair.store.db); err != nil {
		return RestoredBackupResult{}, fmt.Errorf(
			"validate placement db before restored-backup attestation: %w", err,
		)
	}

	store := repair.store
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := repair.verifySourcePath(); err != nil {
		return RestoredBackupResult{}, fmt.Errorf(
			"validate placement db identity before restored-backup attestation: %w", err,
		)
	}
	if err := repair.verifyPublishedBackupTarget(); err != nil {
		return RestoredBackupResult{}, fmt.Errorf(
			"validate exact backup authority immediately before restored-backup attestation: %w", err,
		)
	}
	const operation = "attest restored placement backup"
	if err := updateBoltWithExplicitOutcome(store.db, func(tx *bolt.Tx) error {
		bucket := tx.Bucket(metadataBucketName)
		if bucket == nil {
			return errors.New("placement metadata bucket missing")
		}
		if !bytes.Equal(bucket.Get(metadataStateKey), plan.metadata) {
			return ErrRestoredBackupTarget
		}
		return bucket.Put(metadataStateKey, expected)
	}); err != nil {
		err = mutationFailure(operation, err)
		if classified := classifyRepairTransactionError(operation, err); errors.Is(
			classified, ErrRepairMutationOutcomeUnknown,
		) {
			return RestoredBackupResult{}, classified
		}
		return RestoredBackupResult{}, err
	}
	store.baselineFingerprint = ""
	store.baselineTopologyID = 0
	store.inventoryTopologyID = 0
	store.emptyInventoryBackends = make(map[string]struct{})
	store.advanceAuthorityEpochLocked()
	if err := repair.verifySourcePathAfterMutation(operation); err != nil {
		return RestoredBackupResult{}, err
	}
	if err := repair.verifyPublishedBackupTargetAfterMutation(operation); err != nil {
		return RestoredBackupResult{}, err
	}
	committed, _, err := readRestoredBackupMetadata(store.db)
	if err != nil || !bytes.Equal(committed, expected) {
		return RestoredBackupResult{}, fmt.Errorf(
			"%w: %s: committed metadata differs from the attested record: %w",
			ErrRepairMutationCommitted, operation, err,
		)
	}
	return RestoredBackupResult{issuer: repair, expected: expected}, nil
}

// VerifyRestoredBackupPostcondition rechecks one attestation through a newly
// opened read-only inspector of the same inode.
func (inspector *RepairInspector) VerifyRestoredBackupPostcondition(result RestoredBackupResult) error {
	if inspector == nil || inspector.store == nil || inspector.store.db == nil ||
		result.issuer == nil || len(result.expected) == 0 {
		return errors.New("placement restored-backup postcondition is invalid")
	}
	if err := inspector.verifyOriginalRepairSource(result.issuer); err != nil {
		return fmt.Errorf("placement restored-backup postcondition source: %w", err)
	}
	committed, metadata, err := readRestoredBackupMetadata(inspector.store.db)
	if err != nil {
		return err
	}
	if !bytes.Equal(committed, result.expected) || restoredBackupEvidencePresent(metadata) {
		return errors.New("reopened placement metadata is not the attested record")
	}
	return inspector.verifyOriginalRepairSource(result.issuer)
}

func readRestoredBackupMetadata(db *bolt.DB) ([]byte, topologyMetadata, error) {
	var encoded []byte
	var metadata topologyMetadata
	if err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(metadataBucketName)
		if bucket == nil {
			return errors.New("placement metadata bucket missing")
		}
		raw := bucket.Get(metadataStateKey)
		if raw == nil {
			return errors.New("placement metadata state missing")
		}
		encoded = bytes.Clone(raw)
		var err error
		metadata, err = loadTopologyMetadata(tx)
		return err
	}); err != nil {
		return nil, topologyMetadata{}, fmt.Errorf("read placement metadata: %w", err)
	}
	return encoded, metadata, nil
}

func restoredBackupEvidencePresent(metadata topologyMetadata) bool {
	return metadata.BaselineFingerprint != "" || metadata.BaselineTopologyID != 0 ||
		metadata.InventoryTopologyID != 0 || len(metadata.EmptyInventoryBackends) != 0
}

// restoredBackupConfirmation hashes length-prefixed fields, so no field can
// absorb a neighbor's bytes.
func restoredBackupConfirmation(metadata topologyMetadata, path string, encoded []byte) string {
	digest := sha256.New()
	for _, field := range [][]byte{
		[]byte(restoredBackupConfirmationDomain), []byte(metadata.ProviderUUID), []byte(path), encoded,
	} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		digest.Write(length[:])
		digest.Write(field)
	}
	return fmt.Sprintf("attest-restored-backup:%d:%s",
		metadata.TopologyID, hex.EncodeToString(digest.Sum(nil)))
}
