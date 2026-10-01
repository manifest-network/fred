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
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/maintenanceid"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
)

// ErrBackendRetirementTarget means a retirement was refused before anything
// was written: the target is not retirable, the plan no longer matches the
// durable bytes, or a planned row would not decode as intended.
var ErrBackendRetirementTarget = errors.New("placement backend retirement refused")

// LostBackendAttestationText is the exact operator statement a retirement
// requires. It names the two facts Fred cannot prove from the placement
// database: the storage is gone, and the old host can no longer act.
const LostBackendAttestationText = "I attest the storage of this backend is irrecoverably lost and its host is fenced"

const backendRetirementConfirmationDomain = "fred-placement-backend-retirement-v1"

// retirementDisposition is what a retirement does to one durable row.
type retirementDisposition uint8

const (
	// The zero value is invalid.
	_ retirementDisposition = iota
	// retirementUnchanged: the row never named the retired backend.
	retirementUnchanged
	// retirementLost: the retired backend held the lease's data. The row
	// becomes a terminal lost placement.
	retirementLost
	// retirementStripped: the row only mentioned the retired backend as an
	// attempt or a candidate. It forgets the name and keeps its owner.
	retirementStripped
)

// retirePlacement decides one row's fate by its durable owner, never by mere
// candidacy: a lease the retired backend owned is lost, while a lease owned
// elsewhere only forgets the name, so a healthy survivor's lease is never
// closed. A lease with no surviving owner and a single surviving candidate is
// lost too, rather than silently adopting a copy nobody chose. A row that is
// already lost, cannot be interpreted, or has unknown owners is left exactly
// as it is: with owners unknown a survivor may hold the lease's data, and
// ending the lease could reap that copy.
func retirePlacement(p Placement, retired string) (Placement, retirementDisposition) {
	if _, lost := p.LostBackend(); lost || (p.unusable && !p.Conflict) || p.ConflictOwnersUnknown {
		return p, retirementUnchanged
	}
	if p.Backend != retired && p.Attempt != retired && !slices.Contains(p.ConflictBackends, retired) {
		return p, retirementUnchanged
	}
	if p.Backend == retired {
		return lostPlacement(retired), retirementLost
	}
	next := clonePlacement(p)
	if next.Attempt == retired {
		// The attempt could only have had an effect on the attested-lost
		// storage, whose host is fenced and outside the callback keyring.
		next.Attempt = ""
		clearOperationMetadata(&next)
	}
	next.ConflictBackends = slices.DeleteFunc(next.ConflictBackends, func(name string) bool {
		return name == retired
	})
	if next.Backend == "" && next.Attempt == "" && len(next.ConflictBackends) == 0 {
		// The retired backend was the only evidence of where the lease lived.
		return lostPlacement(retired), retirementLost
	}
	if next.Conflict {
		owners := make(map[string]struct{}, 2)
		for _, name := range []string{next.Backend, next.Attempt} {
			if name != "" {
				owners[name] = struct{}{}
			}
		}
		candidates := normalizeBackendNames(append(slices.Clone(next.ConflictBackends), slices.Collect(maps.Keys(owners))...))
		extra := false
		for _, name := range candidates {
			if _, owner := owners[name]; !owner {
				extra = true
				break
			}
		}
		switch {
		case !extra:
			// Only the owner remains: the contradiction was the retired
			// backend's, and the row returns to its uncontested shape.
			next.Conflict = false
			next.ConflictBackends = nil
			next.untrustedPositive = false
		case len(candidates) == 1:
			// No surviving owner and one surviving reporter: its claim could
			// only have been settled against the lost backend. Adopting it
			// would continue the lease on a copy nobody chose.
			return lostPlacement(retired), retirementLost
		default:
			next.ConflictBackends = candidates
		}
	}
	return next, retirementStripped
}

// lostPlacement is the terminal row of a lease whose data lived on the
// retired backend. RetireBackend stamps its SetAt with the retirement time.
func lostPlacement(retired string) Placement {
	return Placement{lostBackend: retired, unusable: true}
}

// namesSurvivor reports whether a row names any backend other than retired.
func namesSurvivor(p Placement, retired string) bool {
	for _, name := range placementBackendNames(p) {
		if name != retired {
			return true
		}
	}
	return false
}

// namesBackend reports whether a row names backendName as owner, attempt, or
// candidate.
func namesBackend(p Placement, backendName string) bool {
	return slices.Contains(placementBackendNames(p), backendName)
}

// verifyRetiredRow decodes one written row exactly as the store will and
// requires its planned disposition to survive the round trip, so a retirement
// can never commit a row the runtime reads as something else.
func verifyRetiredRow(
	leaseUUID string,
	encoded []byte,
	intended Placement,
	disposition retirementDisposition,
	retired string,
) error {
	decoded := decodeRecord(leaseUUID, encoded)
	lostBackend, lost := decoded.LostBackend()
	switch disposition {
	case retirementLost:
		if !lost || lostBackend != retired || decoded.revision != intended.revision {
			return fmt.Errorf("%w: lost row %q does not decode as lost", ErrBackendRetirementTarget, leaseUUID)
		}
	case retirementStripped:
		if lost || decoded.revision != intended.revision ||
			!equalPlacementIgnoringRevision(decoded, intended) || namesBackend(decoded, retired) {
			return fmt.Errorf("%w: row %q does not decode as planned", ErrBackendRetirementTarget, leaseUUID)
		}
	default:
		return fmt.Errorf("%w: row %q has no retirement disposition", ErrBackendRetirementTarget, leaseUUID)
	}
	return nil
}

// retireLifecycle scrubs one capability. A lost lease keeps only the
// evidence-free quarantine sentinel; a surviving lease loses an attempt marker
// on the retired backend, and a capability whose owner was the retired
// backend becomes the sentinel too. deleteCapability reports a detached row,
// which has no placement to keep it.
func retireLifecycle(
	capability lifecycleCapability,
	retired string,
	disposition retirementDisposition,
	placementExists bool,
) (next lifecycleCapability, changed, deleteCapability bool) {
	names := capability.backend == retired || capability.attemptBackend == retired
	if !placementExists {
		return capability, names, names
	}
	if disposition == retirementLost {
		sentinel := lifecycleCapability{unusable: true}
		return sentinel, capability != sentinel, false
	}
	if !names {
		return capability, false, false
	}
	if capability.backend == retired {
		sentinel := lifecycleCapability{unusable: true}
		return sentinel, true, false
	}
	capability.attemptBackend = ""
	capability.attemptID = lifecycle.ID{}
	return capability, true, false
}

// BackendRetirementFacts renders a retirement plan for the operator.
type BackendRetirementFacts struct {
	ProviderUUID   string   `json:"provider_uuid"`
	DatabasePath   string   `json:"database_path"`
	Backend        string   `json:"backend"`
	StorageID      string   `json:"storage_id"`
	TopologyBefore []string `json:"topology_before"`
	TopologyAfter  []string `json:"topology_after"`
	TopologyID     uint64   `json:"topology_id"`
	// LostLeases will be closed (ACTIVE) or rejected (PENDING) on chain.
	LostLeases []string `json:"lost_leases"`
	// LostWithSurvivorCopies are lost leases that a surviving backend also
	// reported. Once such a lease is terminal, the survivor's copy is an
	// ordinary orphan the survivor deprovisions under its retention policy.
	LostWithSurvivorCopies []string `json:"lost_with_survivor_copies"`
	// StrippedLeases keep their surviving owner, or two or more surviving
	// candidates, and only forget the retired name.
	StrippedLeases []string `json:"stripped_leases"`
	// UnknownOwnerConflicts are legacy quarantines this retirement leaves
	// exactly as they are, even when they name the retired backend; they stay
	// operator-only.
	UnknownOwnerConflicts []string `json:"unknown_owner_conflicts"`
	// UninterpretableLeases have rows Fred cannot decode; they are left
	// exactly as they are.
	UninterpretableLeases []string `json:"uninterpretable_leases"`
	LifecycleScrubbed     []string `json:"lifecycle_scrubbed"`
	// ReclaimedReceiptLeases lose their last authority row to this
	// retirement, so their settled maintenance receipts are deleted with it.
	ReclaimedReceiptLeases []string `json:"reclaimed_receipt_leases"`
	MaintenanceSettled     []string `json:"maintenance_settled"`
	PendingInventorySweep  bool     `json:"pending_inventory_sweep"`
	// RecordlessUnproven: no current admission baseline covered the retired
	// backend, so a live lease with no placement row may have lived there. The
	// reconciler will close such leases as lost instead of provisioning them.
	RecordlessUnproven bool `json:"recordless_unproven"`
}

type retirementRowWrite struct {
	leaseUUID   string
	before      []byte
	disposition retirementDisposition
	placement   Placement
}

type retirementCapabilityWrite struct {
	leaseUUID string
	before    []byte
	// after is nil when the detached capability is deleted.
	after []byte
	// reclaimed are the settled receipts a deletion takes with it.
	reclaimed []retirementReceipt
}

type retirementReceipt struct {
	key    []byte
	before []byte
}

// detachedReceiptsTx returns the exact settled receipts that
// reclaimDetachedMaintenanceCommandsForLeaseTx deletes once leaseUUID loses
// its last authority row.
func detachedReceiptsTx(records *bolt.Bucket, leaseUUID string) []retirementReceipt {
	prefix := []byte(leaseUUID + "\x00")
	var receipts []retirementReceipt
	cursor := records.Cursor()
	for key, value := cursor.Seek(prefix); key != nil && bytes.HasPrefix(key, prefix); key, value = cursor.Next() {
		receipts = append(receipts, retirementReceipt{key: bytes.Clone(key), before: bytes.Clone(value)})
	}
	return receipts
}

func equalRetirementReceipts(a, b []retirementReceipt) bool {
	return slices.EqualFunc(a, b, func(x, y retirementReceipt) bool {
		return bytes.Equal(x.key, y.key) && bytes.Equal(x.before, y.before)
	})
}

type retirementSettlement struct {
	leaseUUID string
	headKey   []byte
	head      []byte
	recordKey []byte
	before    []byte
	command   MaintenanceCommand
	createdAt time.Time
}

// BackendRetirementPlan binds one attested retirement to the exact durable
// bytes it rewrites. Only AttemptRepair.PlanBackendRetirement mints it; its
// zero value is invalid and it is bound to the session that minted it.
type BackendRetirementPlan struct {
	issuer       *AttemptRepair
	backendName  string
	storageID    backendidentity.ID
	metadata     []byte
	current      topologyMetadata
	rows         []retirementRowWrite
	capabilities []retirementCapabilityWrite
	settlements  []retirementSettlement
	confirmation string
	facts        BackendRetirementFacts
}

// BackendRetirementResult is minted only by a successful retirement.
type BackendRetirementResult struct {
	issuer       *AttemptRepair
	backend      string
	metadata     []byte
	rows         map[string][]byte
	dispositions map[string]retirementDisposition
	placements   map[string]Placement
}

func (plan BackendRetirementPlan) ConfirmationValue() string { return plan.confirmation }

func (plan BackendRetirementPlan) Facts() BackendRetirementFacts {
	facts := plan.facts
	facts.TopologyBefore = slices.Clone(facts.TopologyBefore)
	facts.TopologyAfter = slices.Clone(facts.TopologyAfter)
	facts.LostLeases = slices.Clone(facts.LostLeases)
	facts.LostWithSurvivorCopies = slices.Clone(facts.LostWithSurvivorCopies)
	facts.StrippedLeases = slices.Clone(facts.StrippedLeases)
	facts.UnknownOwnerConflicts = slices.Clone(facts.UnknownOwnerConflicts)
	facts.UninterpretableLeases = slices.Clone(facts.UninterpretableLeases)
	facts.LifecycleScrubbed = slices.Clone(facts.LifecycleScrubbed)
	facts.ReclaimedReceiptLeases = slices.Clone(facts.ReclaimedReceiptLeases)
	facts.MaintenanceSettled = slices.Clone(facts.MaintenanceSettled)
	return facts
}

// admissionBaselineCurrentInMetadata is hasCurrentAdmissionBaselineLocked's
// durable half: whether the stopped database proves that every lease placed
// under its topology has a placement row.
func admissionBaselineCurrentInMetadata(metadata topologyMetadata) bool {
	if metadata.TopologyID == 0 || metadata.PendingInventorySweepID != 0 ||
		metadata.BaselineTopologyID != metadata.TopologyID ||
		metadata.BaselineFingerprint == "" || metadata.BaselineFingerprint != metadata.TopologyFingerprint {
		return false
	}
	for _, backendName := range metadata.Topology {
		id, err := backendidentity.Parse(metadata.KnownBackendStorageIDs[backendName])
		if err != nil || !id.Valid() {
			return false
		}
	}
	return true
}

// PlanBackendRetirement derives the complete retirement of backendName, whose
// pinned storage must be storageID, from one read of the exclusively locked
// database. It never writes.
func (repair *AttemptRepair) PlanBackendRetirement(
	backendName string,
	storageID backendidentity.ID,
) (BackendRetirementPlan, error) {
	if repair == nil || repair.store == nil || repair.store.db == nil || repair.authority == nil {
		return BackendRetirementPlan{}, errors.New("placement repair is not open")
	}
	plan := BackendRetirementPlan{issuer: repair, backendName: backendName, storageID: storageID}
	err := repair.store.db.View(func(tx *bolt.Tx) error {
		metadataBucket := tx.Bucket(metadataBucketName)
		placements := tx.Bucket(bucketName)
		capabilities := tx.Bucket(lifecycleCapabilityBucketName)
		if metadataBucket == nil || placements == nil || capabilities == nil {
			return errors.New("placement authority buckets missing")
		}
		plan.metadata = bytes.Clone(metadataBucket.Get(metadataStateKey))
		metadata, err := loadTopologyMetadata(tx)
		if err != nil {
			return err
		}
		plan.current = metadata
		if err := validateRetirementTarget(metadata, backendName, storageID); err != nil {
			return err
		}
		dispositions := make(map[string]retirementDisposition)
		if err := placements.ForEach(func(key, value []byte) error {
			leaseUUID := string(key)
			p := decodeRecord(leaseUUID, value)
			if p.ConflictOwnersUnknown {
				plan.facts.UnknownOwnerConflicts = append(plan.facts.UnknownOwnerConflicts, leaseUUID)
			}
			if _, lost := p.LostBackend(); p.unusable && !p.Conflict && !lost {
				plan.facts.UninterpretableLeases = append(plan.facts.UninterpretableLeases, leaseUUID)
			}
			next, disposition := retirePlacement(p, backendName)
			dispositions[leaseUUID] = disposition
			if disposition == retirementUnchanged {
				return nil
			}
			plan.rows = append(plan.rows, retirementRowWrite{
				leaseUUID: leaseUUID, before: bytes.Clone(value),
				disposition: disposition, placement: next,
			})
			if disposition == retirementLost {
				plan.facts.LostLeases = append(plan.facts.LostLeases, leaseUUID)
				if namesSurvivor(p, backendName) {
					plan.facts.LostWithSurvivorCopies = append(plan.facts.LostWithSurvivorCopies, leaseUUID)
				}
			} else {
				plan.facts.StrippedLeases = append(plan.facts.StrippedLeases, leaseUUID)
			}
			return nil
		}); err != nil {
			return err
		}
		pending, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		if err := capabilities.ForEach(func(key, value []byte) error {
			leaseUUID := string(key)
			capability, decodeErr := decodeLifecycleCapability(value)
			if decodeErr != nil {
				// Undecodable authority is never rewritten; it already fails
				// closed and its bytes stay for diagnosis.
				return nil //nolint:nilerr // skipping an undecodable row is the decision, not a failure
			}
			disposition, placementExists := dispositions[leaseUUID]
			next, changed, deleteCapability := retireLifecycle(
				capability, backendName, disposition, placementExists,
			)
			if !changed {
				return nil
			}
			write := retirementCapabilityWrite{leaseUUID: leaseUUID, before: bytes.Clone(value)}
			if deleteCapability {
				if pending.Get(key) != nil {
					return fmt.Errorf("%w: detached lease %q retains a pending command", ErrMaintenanceJournalCorrupt, leaseUUID)
				}
				write.reclaimed = detachedReceiptsTx(records, leaseUUID)
				if len(write.reclaimed) != 0 {
					plan.facts.ReclaimedReceiptLeases = append(plan.facts.ReclaimedReceiptLeases, leaseUUID)
				}
			} else {
				encoded, encodeErr := encodeLifecycleCapability(next)
				if encodeErr != nil {
					return fmt.Errorf("encode retired lifecycle capability %q: %w", leaseUUID, encodeErr)
				}
				write.after = encoded
			}
			plan.capabilities = append(plan.capabilities, write)
			plan.facts.LifecycleScrubbed = append(plan.facts.LifecycleScrubbed, leaseUUID)
			return nil
		}); err != nil {
			return err
		}
		return pending.ForEach(func(key, value []byte) error {
			leaseUUID := string(key)
			id, parseErr := maintenanceid.Parse(string(value))
			if parseErr != nil {
				return fmt.Errorf("%w: pending head %q: %w", ErrMaintenanceJournalCorrupt, leaseUUID, parseErr)
			}
			recordKey := maintenanceReceiptKey(leaseUUID, id)
			encoded := records.Get(recordKey)
			command, outcome, createdAt, _, _, decodeErr := decodeMaintenanceCommand(encoded)
			if decodeErr != nil || outcome != MaintenanceOutcomePending {
				return fmt.Errorf("%w: pending command for %q is not a pending receipt", ErrMaintenanceJournalCorrupt, leaseUUID)
			}
			if dispositions[leaseUUID] != retirementLost && command.backendName != backendName {
				return nil
			}
			plan.settlements = append(plan.settlements, retirementSettlement{
				leaseUUID: leaseUUID, headKey: bytes.Clone(key), head: bytes.Clone(value),
				recordKey: recordKey, before: bytes.Clone(encoded), command: command, createdAt: createdAt,
			})
			plan.facts.MaintenanceSettled = append(plan.facts.MaintenanceSettled, leaseUUID)
			return nil
		})
	})
	if err != nil {
		return BackendRetirementPlan{}, err
	}
	metadata := plan.current
	plan.facts.ProviderUUID = metadata.ProviderUUID
	plan.facts.DatabasePath = repair.authority.path
	plan.facts.Backend = backendName
	plan.facts.StorageID = storageID.String()
	plan.facts.TopologyBefore = slices.Clone(metadata.Topology)
	plan.facts.TopologyAfter = slices.DeleteFunc(slices.Clone(metadata.Topology), func(name string) bool {
		return name == backendName
	})
	plan.facts.TopologyID = metadata.TopologyID
	plan.facts.PendingInventorySweep = metadata.PendingInventorySweepID != 0
	plan.facts.RecordlessUnproven = !admissionBaselineCurrentInMetadata(metadata)
	for _, list := range []*[]string{
		&plan.facts.LostLeases, &plan.facts.LostWithSurvivorCopies, &plan.facts.StrippedLeases,
		&plan.facts.UnknownOwnerConflicts, &plan.facts.UninterpretableLeases, &plan.facts.LifecycleScrubbed,
		&plan.facts.ReclaimedReceiptLeases, &plan.facts.MaintenanceSettled,
	} {
		if *list == nil {
			*list = []string{}
		}
		slices.Sort(*list)
	}
	plan.confirmation = backendRetirementConfirmation(plan)
	return plan, nil
}

// validateRetirementTarget refuses anything but one active, pinned, still
// unretired backend that leaves at least one survivor.
func validateRetirementTarget(metadata topologyMetadata, backendName string, storageID backendidentity.ID) error {
	if metadata.TopologyID == 0 {
		return fmt.Errorf("%w: placement database has no configured topology", ErrBackendRetirementTarget)
	}
	if _, retired := metadata.RetiredBackends[backendName]; retired {
		return fmt.Errorf("%w: backend %q is already retired", ErrBackendRetirementTarget, backendName)
	}
	if !slices.Contains(metadata.Topology, backendName) {
		return fmt.Errorf("%w: backend %q is not in the active topology %q",
			ErrBackendRetirementTarget, backendName, metadata.Topology)
	}
	if len(metadata.Topology) < 2 {
		return fmt.Errorf("%w: backend %q is the last backend; retirement needs a survivor",
			ErrBackendRetirementTarget, backendName)
	}
	if !storageID.Valid() || metadata.KnownBackendStorageIDs[backendName] != storageID.String() {
		return fmt.Errorf("%w: storage identity %s is not the pin for backend %q",
			ErrBackendRetirementTarget, storageID, backendName)
	}
	return nil
}

// backendRetirementConfirmation hashes length-prefixed fields covering the
// provider, the canonical path, the target, and every byte the retirement
// rewrites or deletes, so any change between the dry run and the apply
// refuses the apply.
func backendRetirementConfirmation(plan BackendRetirementPlan) string {
	digest := sha256.New()
	write := func(field []byte) {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(field)))
		digest.Write(length[:])
		digest.Write(field)
	}
	write([]byte(backendRetirementConfirmationDomain))
	write([]byte(plan.current.ProviderUUID))
	write([]byte(plan.facts.DatabasePath))
	write([]byte(plan.backendName))
	write([]byte(plan.storageID.String()))
	write(plan.metadata)
	for _, row := range plan.rows {
		write([]byte(row.leaseUUID))
		write(row.before)
	}
	for _, capability := range plan.capabilities {
		write([]byte(capability.leaseUUID))
		write(capability.before)
		for _, receipt := range capability.reclaimed {
			write(receipt.key)
			write(receipt.before)
		}
	}
	for _, settlement := range plan.settlements {
		write(settlement.headKey)
		write(settlement.head)
		write(settlement.recordKey)
		write(settlement.before)
	}
	return fmt.Sprintf("retire-lost-backend:%s:%d:%s",
		plan.backendName, plan.current.TopologyID, hex.EncodeToString(digest.Sum(nil)))
}

// RetireBackend applies the plan in one transaction after the exact
// pre-mutation backup is published. Every byte the plan read must still be
// current; the topology drops the backend as a new generation; the admission
// baseline and drain evidence are forgotten; lost rows, scrubbed lifecycle
// authority, and settled maintenance are written together.
func (repair *AttemptRepair) RetireBackend(
	plan BackendRetirementPlan,
	attestation string,
) (BackendRetirementResult, error) {
	if repair == nil || repair.store == nil || repair.store.db == nil || plan.issuer != repair {
		return BackendRetirementResult{}, fmt.Errorf("%w: plan belongs to another repair session", ErrBackendRetirementTarget)
	}
	if attestation != LostBackendAttestationText {
		return BackendRetirementResult{}, fmt.Errorf("%w: the lost-storage attestation is not exact", ErrBackendRetirementTarget)
	}
	current, err := repair.PlanBackendRetirement(plan.backendName, plan.storageID)
	if err != nil {
		return BackendRetirementResult{}, err
	}
	if current.confirmation != plan.confirmation {
		return BackendRetirementResult{}, fmt.Errorf("%w: durable state changed after the plan", ErrBackendRetirementTarget)
	}
	if err := verifyBoltPhysicalConsistency(repair.store.db); err != nil {
		return BackendRetirementResult{}, fmt.Errorf("validate placement db before backend retirement: %w", err)
	}

	store := repair.store
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := repair.verifySourcePath(); err != nil {
		return BackendRetirementResult{}, fmt.Errorf("validate placement db identity before backend retirement: %w", err)
	}
	if err := repair.verifyPublishedBackupTarget(); err != nil {
		return BackendRetirementResult{}, fmt.Errorf(
			"validate exact backup authority immediately before backend retirement: %w", err)
	}

	now := time.Now().UTC()
	next := plan.current
	next.Topology = plan.facts.TopologyAfter
	fingerprint, err := topologyFingerprint(next.Topology)
	if err != nil {
		return BackendRetirementResult{}, err
	}
	if next.TopologyID == ^uint64(0) {
		return BackendRetirementResult{}, errors.New("placement topology identity exhausted")
	}
	next.TopologyFingerprint = fingerprint
	next.TopologyID++
	next.BaselineFingerprint = ""
	next.BaselineTopologyID = 0
	next.InventoryTopologyID = 0
	next.EmptyInventoryBackends = nil
	if next.InventorySweepReporters != nil {
		reporters := *next.InventorySweepReporters
		reporters.Backends = slices.DeleteFunc(slices.Clone(reporters.Backends), func(name string) bool {
			return name == plan.backendName
		})
		next.InventorySweepReporters = &reporters
	}
	next.RetiredBackends = cloneRetiredBackends(next.RetiredBackends)
	if next.RetiredBackends == nil {
		next.RetiredBackends = make(map[string]retiredBackend, 1)
	}
	attestationDigest := sha256.Sum256([]byte(plan.confirmation))
	next.RetiredBackends[plan.backendName] = retiredBackend{
		RetiredAt:          now,
		AttestationSHA256:  hex.EncodeToString(attestationDigest[:]),
		RecordlessUnproven: plan.facts.RecordlessUnproven,
	}
	if err := validateTopologyMetadata(next); err != nil {
		return BackendRetirementResult{}, fmt.Errorf("%w: retired metadata is invalid: %w", ErrBackendRetirementTarget, err)
	}
	metadataEncoded, err := encodeTopologyMetadata(next)
	if err != nil {
		return BackendRetirementResult{}, fmt.Errorf("encode retired placement metadata: %w", err)
	}

	revision := store.revision
	rows := make(map[string][]byte, len(plan.rows))
	placementsAfter := make(map[string]Placement, len(plan.rows))
	dispositions := make(map[string]retirementDisposition, len(plan.rows))
	for _, row := range plan.rows {
		if revision == ^uint64(0) {
			return BackendRetirementResult{}, errors.New("placement revision exhausted")
		}
		revision++
		p := row.placement
		p.revision = revision
		if row.disposition == retirementLost {
			p.SetAt = now
		}
		encoded, encodeErr := encodePlacement(p)
		if encodeErr != nil {
			return BackendRetirementResult{}, fmt.Errorf("encode retired placement %q: %w", row.leaseUUID, encodeErr)
		}
		if err := verifyRetiredRow(row.leaseUUID, encoded, p, row.disposition, plan.backendName); err != nil {
			return BackendRetirementResult{}, err
		}
		rows[row.leaseUUID] = encoded
		placementsAfter[row.leaseUUID] = p
		dispositions[row.leaseUUID] = row.disposition
	}
	settledAt := now
	settlements := make(map[string][]byte, len(plan.settlements))
	for _, settlement := range plan.settlements {
		at := settledAt
		if at.Before(settlement.createdAt) {
			at = settlement.createdAt
		}
		encoded, _, encodeErr := encodeMaintenanceSettlement(
			settlement.command, maintenanceSettlement{outcome: MaintenanceOutcomeBackendLost},
			settlement.createdAt, at,
		)
		if encodeErr != nil {
			return BackendRetirementResult{}, fmt.Errorf("encode lost maintenance settlement %q: %w",
				settlement.leaseUUID, encodeErr)
		}
		settlements[settlement.leaseUUID] = encoded
	}

	const operation = "retire lost placement backend"
	if err := updateBoltWithExplicitOutcome(store.db, func(tx *bolt.Tx) error {
		metadataBucket := tx.Bucket(metadataBucketName)
		placements := tx.Bucket(bucketName)
		capabilities := tx.Bucket(lifecycleCapabilityBucketName)
		if metadataBucket == nil || placements == nil || capabilities == nil {
			return errors.New("placement authority buckets missing")
		}
		pending, records, err := maintenanceCommandBuckets(tx)
		if err != nil {
			return err
		}
		if !bytes.Equal(metadataBucket.Get(metadataStateKey), plan.metadata) {
			return ErrBackendRetirementTarget
		}
		for _, settlement := range plan.settlements {
			if !bytes.Equal(pending.Get(settlement.headKey), settlement.head) ||
				!bytes.Equal(records.Get(settlement.recordKey), settlement.before) {
				return ErrBackendRetirementTarget
			}
			if err := records.Put(settlement.recordKey, settlements[settlement.leaseUUID]); err != nil {
				return err
			}
			if err := pending.Delete(settlement.headKey); err != nil {
				return err
			}
		}
		for _, row := range plan.rows {
			key := []byte(row.leaseUUID)
			if !bytes.Equal(placements.Get(key), row.before) {
				return ErrBackendRetirementTarget
			}
			if err := placements.Put(key, rows[row.leaseUUID]); err != nil {
				return err
			}
		}
		for _, capability := range plan.capabilities {
			key := []byte(capability.leaseUUID)
			if !bytes.Equal(capabilities.Get(key), capability.before) {
				return ErrBackendRetirementTarget
			}
			if capability.after == nil {
				if !equalRetirementReceipts(detachedReceiptsTx(records, capability.leaseUUID), capability.reclaimed) {
					return ErrBackendRetirementTarget
				}
				if err := capabilities.Delete(key); err != nil {
					return err
				}
				if err := reclaimDetachedMaintenanceCommandsForLeaseTx(tx, capability.leaseUUID); err != nil {
					return err
				}
				continue
			}
			if err := capabilities.Put(key, capability.after); err != nil {
				return err
			}
		}
		return metadataBucket.Put(metadataStateKey, metadataEncoded)
	}); err != nil {
		err = mutationFailure(operation, err)
		if classified := classifyRepairTransactionError(operation, err); errors.Is(
			classified, ErrRepairMutationOutcomeUnknown,
		) {
			return BackendRetirementResult{}, classified
		}
		return BackendRetirementResult{}, err
	}
	// The session's cache now differs from disk; every later check reopens.
	store.revision = revision
	for leaseUUID, p := range placementsAfter {
		store.cache[leaseUUID] = p
	}
	if err := repair.verifySourcePathAfterMutation(operation); err != nil {
		return BackendRetirementResult{}, err
	}
	if err := repair.verifyPublishedBackupTargetAfterMutation(operation); err != nil {
		return BackendRetirementResult{}, err
	}
	return BackendRetirementResult{
		issuer: repair, backend: plan.backendName, metadata: metadataEncoded, rows: rows,
		dispositions: dispositions, placements: placementsAfter,
	}, nil
}

// VerifyBackendRetirementPostcondition rechecks one retirement through a
// newly opened read-only inspector of the same inode: the exact metadata and
// every rewritten row are on disk, and no row still names the backend as a
// live owner.
func (inspector *RepairInspector) VerifyBackendRetirementPostcondition(result BackendRetirementResult) error {
	if inspector == nil || inspector.store == nil || inspector.store.db == nil ||
		result.issuer == nil || len(result.metadata) == 0 || result.backend == "" {
		return errors.New("placement backend retirement postcondition is invalid")
	}
	if err := inspector.verifyOriginalRepairSource(result.issuer); err != nil {
		return fmt.Errorf("placement backend retirement postcondition source: %w", err)
	}
	if err := inspector.store.db.View(func(tx *bolt.Tx) error {
		metadataBucket := tx.Bucket(metadataBucketName)
		placements := tx.Bucket(bucketName)
		if metadataBucket == nil || placements == nil {
			return errors.New("placement authority buckets missing")
		}
		if !bytes.Equal(metadataBucket.Get(metadataStateKey), result.metadata) {
			return errors.New("reopened placement metadata is not the retired record")
		}
		for leaseUUID, encoded := range result.rows {
			stored := placements.Get([]byte(leaseUUID))
			if !bytes.Equal(stored, encoded) {
				return fmt.Errorf("reopened placement %q is not the retired row", leaseUUID)
			}
			if err := verifyRetiredRow(
				leaseUUID, stored, result.placements[leaseUUID], result.dispositions[leaseUUID], result.backend,
			); err != nil {
				return err
			}
		}
		return placements.ForEach(func(key, value []byte) error {
			p := decodeRecord(string(key), value)
			if p.ConflictOwnersUnknown {
				// Left exactly as it was: an operator-only quarantine.
				return nil
			}
			if namesBackend(p, result.backend) {
				return fmt.Errorf("placement %q still names retired backend %q", key, result.backend)
			}
			return nil
		})
	}); err != nil {
		return err
	}
	return inspector.verifyOriginalRepairSource(result.issuer)
}
