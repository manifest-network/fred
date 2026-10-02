package placement

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
)

// ErrGenerationAdoptionTarget means a lifecycle generation adoption no longer
// matches, or never matched, one lease whose quarantine its own durable rows
// do not explain.
var ErrGenerationAdoptionTarget = errors.New("placement lifecycle generation adoption target changed")

// ErrGenerationAdoptionEvidence means live inventory does not show the
// lease's backend as its sole owner reporting a settled new typed generation
// for the same principal.
var ErrGenerationAdoptionEvidence = errors.New("lifecycle generation adoption evidence is not exact")

// GenerationAttestationText is the exact statement an operator makes before a
// stored lifecycle generation is replaced by the one a backend reports.
// Inventory shows what the backend reports now; only the operator can attest
// that the report is current, not a backend restored from an older snapshot
// or a replay of an operation still in flight.
const GenerationAttestationText = "I attest this backend was not restored from an older snapshot and has no in-flight operation or callback replay for this lease"

// adoptableQuarantine returns nil only when a quarantined lifecycle capability
// is unusable for no reason its own durable rows show: clearing the
// quarantine flag alone would leave one typed generation on backendName,
// bound to its placement and able to authorize maintenance. A quarantine the
// durable pair explains is never adoptable, so adoption can only replace a
// generation the backend moved past, never repair an inconsistent record.
func adoptableQuarantine(
	placement Placement,
	capability lifecycleCapability,
	backendName, providerUUID string,
) error {
	switch {
	case capability.usable():
		return errors.New("lifecycle authority is not quarantined")
	case capability.rawCorrupt || capability.needsPersistence:
		return errors.New("the quarantine is not durably recorded on a decodable row")
	case capability.retired:
		return errors.New("lifecycle authority is retired")
	case capability.attemptBackend != "" || placement.Attempt != "":
		return errors.New("an attempt is in flight")
	case !capability.id.Valid():
		return errors.New("the stored generation is not typed")
	case placement.Backend != backendName || capability.backend != backendName:
		return fmt.Errorf("the confirmed owner is not backend %q", backendName)
	case placement.unusable || placement.Conflict || placement.lostBackend != "":
		return errors.New("the placement itself is unusable")
	}
	// Past the checks above, the capability names a typed owner, so lifting the
	// explicit quarantine is the only change that could make it usable.
	cleared := capability
	cleared.quarantined = false
	if problem := lifecycleBindingProblem(placement, cleared); problem != "" {
		return errors.New(problem)
	}
	if !maintenanceAuthorityAvailable(placement, cleared, providerUUID) {
		return errors.New("the stored pair could not authorize maintenance even without the quarantine")
	}
	return nil
}

// GenerationAdoptionCandidate is an opaque, repair-session-bound selector for
// one lease whose lifecycle quarantine is adoptable. It carries no mutation
// authority; only PlanGenerationAdoptionContext can combine it with live
// evidence to mint a GenerationAdoptionPlan.
type GenerationAdoptionCandidate struct {
	issuer           *AttemptRepair
	leaseUUID        string
	backendName      string
	providerUUID     string
	principal        runtimePrincipal
	storedID         lifecycle.ID
	revision         uint64
	topologyID       uint64
	placementEncoded []byte
	lifecycleEncoded []byte
}

// MatchGenerationQuarantine returns a candidate only when lease is confirmed
// on backendName in the active topology, its lifecycle capability is
// adoptable, both rows are the canonical durable bytes, no restore uses the
// lease as its source, and no maintenance command is pending for it.
func (repair *AttemptRepair) MatchGenerationQuarantine(
	leaseUUID, backendName string,
) (GenerationAdoptionCandidate, error) {
	if repair == nil || repair.store == nil {
		return GenerationAdoptionCandidate{}, errors.New("placement repair is not open")
	}
	if !canonicalLeaseUUID(leaseUUID) {
		return GenerationAdoptionCandidate{}, fmt.Errorf(
			"%w: lease UUID %q is not canonical", ErrGenerationAdoptionTarget, leaseUUID)
	}
	store := repair.store
	store.mu.RLock()
	defer store.mu.RUnlock()
	return repair.matchGenerationQuarantineLocked(leaseUUID, backendName)
}

// Caller holds at least repair.store.mu.RLock.
func (repair *AttemptRepair) matchGenerationQuarantineLocked(
	leaseUUID, backendName string,
) (GenerationAdoptionCandidate, error) {
	store := repair.store
	fail := func(format string, args ...any) (GenerationAdoptionCandidate, error) {
		return GenerationAdoptionCandidate{}, fmt.Errorf(
			"%w: lease %q: %s", ErrGenerationAdoptionTarget, leaseUUID, fmt.Sprintf(format, args...))
	}
	if store.topologyID == 0 {
		return fail("durable backend topology is not configured")
	}
	if _, active := store.backendTopologySet[backendName]; !active {
		return fail("backend %q is outside the active durable topology", backendName)
	}
	placementRecord, placed := store.cache[leaseUUID]
	capability, bound := store.lifecycleCache[leaseUUID]
	if !placed || !bound {
		return fail("no placement with lifecycle authority")
	}
	if err := adoptableQuarantine(placementRecord, capability, backendName, store.providerUUID); err != nil {
		return fail("%v", err)
	}
	for other, record := range store.cache {
		if record.attemptRestoreSourceLeaseUUID == leaseUUID {
			return fail("lease %q is restoring from it", other)
		}
	}
	placementEncoded, err := encodePlacement(placementRecord)
	if err != nil {
		return fail("encode placement: %v", err)
	}
	lifecycleEncoded, err := encodeLifecycleCapability(capability)
	if err != nil {
		return fail("encode lifecycle authority: %v", err)
	}
	if err := store.db.View(func(tx *bolt.Tx) error {
		return requireGenerationRowsTx(tx, leaseUUID, placementEncoded, lifecycleEncoded)
	}); err != nil {
		return fail("%v", err)
	}
	return GenerationAdoptionCandidate{
		issuer:           repair,
		leaseUUID:        leaseUUID,
		backendName:      backendName,
		providerUUID:     store.providerUUID,
		principal:        capability.principal,
		storedID:         capability.id,
		revision:         placementRecord.revision,
		topologyID:       store.topologyID,
		placementEncoded: placementEncoded,
		lifecycleEncoded: lifecycleEncoded,
	}, nil
}

// requireGenerationRowsTx proves both durable rows are exactly the bytes the
// candidate was matched from and that no maintenance command is pending.
func requireGenerationRowsTx(
	tx *bolt.Tx,
	leaseUUID string,
	placementEncoded, lifecycleEncoded []byte,
) error {
	placements := tx.Bucket(bucketName)
	capabilities := tx.Bucket(lifecycleCapabilityBucketName)
	if placements == nil || capabilities == nil {
		return errors.New("placement lifecycle buckets are missing")
	}
	if !bytes.Equal(placements.Get([]byte(leaseUUID)), placementEncoded) {
		return errors.New("durable placement row differs from its canonical encoding")
	}
	if !bytes.Equal(capabilities.Get([]byte(leaseUUID)), lifecycleEncoded) {
		return errors.New("durable lifecycle row differs from its canonical encoding")
	}
	return rejectPendingMaintenanceTx(tx, leaseUUID)
}

// LeaseUUID returns the quarantined lease.
func (candidate GenerationAdoptionCandidate) LeaseUUID() string { return candidate.leaseUUID }

// Backend returns the lease's confirmed owner, which live inventory must
// report as its sole owner.
func (candidate GenerationAdoptionCandidate) Backend() string { return candidate.backendName }

// Revision returns the durable placement revision shown to the operator.
func (candidate GenerationAdoptionCandidate) Revision() uint64 { return candidate.revision }

// StoredGenerationFingerprint identifies the quarantined generation without
// revealing it: lifecycle IDs are callback capabilities.
func (candidate GenerationAdoptionCandidate) StoredGenerationFingerprint() string {
	return candidate.storedID.Fingerprint()
}

func sameGenerationAdoptionCandidate(left, right GenerationAdoptionCandidate) bool {
	return left.issuer == right.issuer && left.leaseUUID == right.leaseUUID &&
		left.backendName == right.backendName && left.providerUUID == right.providerUUID &&
		left.principal == right.principal && left.storedID == right.storedID &&
		left.revision == right.revision && left.topologyID == right.topologyID &&
		bytes.Equal(left.placementEncoded, right.placementEncoded) &&
		bytes.Equal(left.lifecycleEncoded, right.lifecycleEncoded)
}

// GenerationAdoptionPlan is the sole capability that replaces a stored
// lifecycle generation. It is minted only from one adoptable quarantine and
// one complete, identity-bound live inventory in which the lease's backend is
// its sole owner and reports a settled new typed generation for the same
// principal. Its confirmation binds every load-bearing fact, and its authority
// expires with the context that collected the inventory.
type GenerationAdoptionPlan struct {
	issuer       *AttemptRepair
	candidate    GenerationAdoptionCandidate
	context      context.Context
	notAfter     time.Time
	digest       [sha256.Size]byte
	storageID    backendidentity.ID
	observedID   lifecycle.ID
	confirmation string
}

// GenerationAdoptionProbe performs the mandatory final complete-fleet
// re-probe while AdoptObservedGenerationContext holds mutation admission.
type GenerationAdoptionProbe func(context.Context) (RepairInventorySnapshot, error)

// PlanGenerationAdoptionContext matches complete live evidence to the exact
// current quarantine. The caller's context must have a deadline; the plan is
// bound to it and expires at that deadline.
func (repair *AttemptRepair) PlanGenerationAdoptionContext(
	ctx context.Context,
	candidate GenerationAdoptionCandidate,
	inventory RepairInventorySnapshot,
) (GenerationAdoptionPlan, error) {
	notAfter, err := bindRepairEvidenceContext(ctx)
	if err != nil {
		return GenerationAdoptionPlan{}, fmt.Errorf("%w: %w", ErrGenerationAdoptionTarget, err)
	}
	if repair == nil || repair.store == nil || candidate.issuer != repair {
		return GenerationAdoptionPlan{}, fmt.Errorf(
			"%w: candidate belongs to another repair session", ErrGenerationAdoptionTarget)
	}
	if err := repair.validateInventorySnapshot(inventory); err != nil {
		return GenerationAdoptionPlan{}, err
	}
	current, err := repair.MatchGenerationQuarantine(candidate.leaseUUID, candidate.backendName)
	if err != nil {
		return GenerationAdoptionPlan{}, err
	}
	if !sameGenerationAdoptionCandidate(current, candidate) {
		return GenerationAdoptionPlan{}, fmt.Errorf(
			"%w: the quarantined rows changed", ErrGenerationAdoptionTarget)
	}
	return repair.newGenerationAdoptionPlan(ctx, notAfter, candidate, inventory)
}

func (repair *AttemptRepair) newGenerationAdoptionPlan(
	ctx context.Context,
	notAfter time.Time,
	candidate GenerationAdoptionCandidate,
	inventory RepairInventorySnapshot,
) (GenerationAdoptionPlan, error) {
	observedID, err := matchGenerationInventory(candidate, inventory)
	if err != nil {
		return GenerationAdoptionPlan{}, err
	}
	plan := GenerationAdoptionPlan{
		issuer:     repair,
		candidate:  candidate,
		context:    ctx,
		notAfter:   notAfter,
		digest:     inventory.digest,
		storageID:  inventory.inventories[candidate.backendName].StorageIdentity,
		observedID: observedID,
	}
	plan.confirmation, err = generationAdoptionConfirmation(repair.store.db.Path(), plan)
	if err != nil {
		return GenerationAdoptionPlan{}, err
	}
	return plan, nil
}

// matchGenerationInventory returns the generation the lease's backend reports
// when that backend is its sole positive owner across every provision and
// retention, the observation is a settled provision for the stored principal,
// and its typed generation differs from the stored one.
func matchGenerationInventory(
	candidate GenerationAdoptionCandidate,
	inventory RepairInventorySnapshot,
) (lifecycle.ID, error) {
	var (
		positives int
		owner     *backend.ProvisionInfo
	)
	for _, backendName := range inventory.backends {
		observed := inventory.inventories[backendName]
		for index := range observed.Provisions {
			if observed.Provisions[index].LeaseUUID == candidate.leaseUUID {
				positives++
				if backendName == candidate.backendName {
					owner = &observed.Provisions[index]
				}
			}
		}
		for index := range observed.Retentions {
			if observed.Retentions[index].LeaseUUID == candidate.leaseUUID {
				positives++
			}
		}
	}
	fail := func(format string, args ...any) (lifecycle.ID, error) {
		return lifecycle.ID{}, fmt.Errorf("%w: lease %q: %s",
			ErrGenerationAdoptionEvidence, candidate.leaseUUID, fmt.Sprintf(format, args...))
	}
	if positives != 1 || owner == nil {
		return fail("backend %q must be the only backend reporting it, as an active provision",
			candidate.backendName)
	}
	if owner.ProviderUUID != candidate.providerUUID || owner.Tenant != candidate.principal.tenant {
		return fail("the reported tenant or provider differs from the stored principal")
	}
	switch owner.Status {
	case backend.ProvisionStatusReady, backend.ProvisionStatusFailing, backend.ProvisionStatusFailed:
	default:
		return fail("the provision is %q, not settled", owner.Status)
	}
	observation := repairLifecycleObservation(owner.LifecycleGeneration)
	if observation.Kind != LifecycleObservationTyped || validateLifecycleObservation(observation) != nil {
		return fail("the backend does not report a typed lifecycle generation")
	}
	if observation.ID == candidate.storedID {
		return fail("the backend reports the stored generation; there is nothing to adopt")
	}
	return observation.ID, nil
}

func generationAdoptionConfirmation(databasePath string, plan GenerationAdoptionPlan) (string, error) {
	candidate := plan.candidate
	if plan.issuer == nil || candidate.issuer != plan.issuer || databasePath == "" ||
		!plan.storageID.Valid() || !plan.observedID.Valid() || !candidate.storedID.Valid() ||
		plan.digest == ([sha256.Size]byte{}) || candidate.revision == 0 || candidate.topologyID == 0 {
		return "", errors.New("lifecycle generation adoption plan is incomplete")
	}
	placementDigest := sha256.Sum256(candidate.placementEncoded)
	lifecycleDigest := sha256.Sum256(candidate.lifecycleEncoded)
	payload, err := json.Marshal(struct {
		DatabasePath     string `json:"database_path"`
		LeaseUUID        string `json:"lease_uuid"`
		Backend          string `json:"backend"`
		StorageID        string `json:"storage_id"`
		ProviderUUID     string `json:"provider_uuid"`
		Tenant           string `json:"tenant"`
		Revision         uint64 `json:"revision"`
		TopologyID       uint64 `json:"topology_id"`
		PlacementSHA256  string `json:"placement_sha256"`
		LifecycleSHA256  string `json:"lifecycle_sha256"`
		StoredGeneration string `json:"stored_generation"`
		Observed         string `json:"observed_generation"`
		InventoryDigest  string `json:"inventory_digest"`
	}{
		DatabasePath:     databasePath,
		LeaseUUID:        candidate.leaseUUID,
		Backend:          candidate.backendName,
		StorageID:        plan.storageID.String(),
		ProviderUUID:     candidate.providerUUID,
		Tenant:           candidate.principal.tenant,
		Revision:         candidate.revision,
		TopologyID:       candidate.topologyID,
		PlacementSHA256:  hex.EncodeToString(placementDigest[:]),
		LifecycleSHA256:  hex.EncodeToString(lifecycleDigest[:]),
		StoredGeneration: candidate.storedID.String(),
		Observed:         plan.observedID.String(),
		InventoryDigest:  hex.EncodeToString(plan.digest[:]),
	})
	if err != nil {
		return "", fmt.Errorf("encode lifecycle generation adoption confirmation: %w", err)
	}
	digest := sha256.Sum256(payload)
	return fmt.Sprintf("adopt-generation:%s:%s:%d:%s",
		candidate.leaseUUID, candidate.backendName, candidate.revision,
		hex.EncodeToString(digest[:])), nil
}

// ConfirmationValue is the exact operator confirmation for every fact in the
// plan. It hashes the generations, so it reveals neither.
func (plan GenerationAdoptionPlan) ConfirmationValue() string { return plan.confirmation }

// LeaseUUID returns the quarantined lease.
func (plan GenerationAdoptionPlan) LeaseUUID() string { return plan.candidate.leaseUUID }

// Backend returns the lease's confirmed owner.
func (plan GenerationAdoptionPlan) Backend() string { return plan.candidate.backendName }

// Revision returns the durable placement revision the plan replaces.
func (plan GenerationAdoptionPlan) Revision() uint64 { return plan.candidate.revision }

// StoredGenerationFingerprint identifies the quarantined generation.
func (plan GenerationAdoptionPlan) StoredGenerationFingerprint() string {
	return plan.candidate.storedID.Fingerprint()
}

// ObservedGenerationFingerprint identifies the generation the backend reports.
func (plan GenerationAdoptionPlan) ObservedGenerationFingerprint() string {
	return plan.observedID.Fingerprint()
}

func sameGenerationAdoptionPlan(left, right GenerationAdoptionPlan) bool {
	return left.issuer == right.issuer &&
		sameGenerationAdoptionCandidate(left.candidate, right.candidate) &&
		left.context != nil && right.context != nil &&
		left.context.Done() == right.context.Done() && left.notAfter.Equal(right.notAfter) &&
		left.digest == right.digest && left.storageID == right.storageID &&
		left.observedID == right.observedID && left.confirmation == right.confirmation
}

// GenerationAttestation is the operator's statement bound to one plan
// confirmation and repair session. Mutation requires it and the plan; neither
// substitutes for the other.
type GenerationAttestation struct {
	issuer       *AttemptRepair
	confirmation string
}

// AttestGeneration converts the exact operator statement into a capability
// bound to one plan confirmation.
func (repair *AttemptRepair) AttestGeneration(
	confirmation, statement string,
) (GenerationAttestation, error) {
	if repair == nil || repair.store == nil {
		return GenerationAttestation{}, errors.New("placement repair is not open")
	}
	if confirmation == "" {
		return GenerationAttestation{}, errors.New("generation adoption confirmation is empty")
	}
	if statement != GenerationAttestationText {
		return GenerationAttestation{}, fmt.Errorf(
			"generation attestation must exactly equal %q", GenerationAttestationText)
	}
	return GenerationAttestation{issuer: repair, confirmation: confirmation}, nil
}

// GenerationAdoptionResult is minted only by a successful adoption and carries
// the exact rows it committed.
type GenerationAdoptionResult struct {
	leaseUUID string
	expected  Placement
	lifecycle lifecycleCapability
}

// AdoptObservedGenerationContext replaces the lease's quarantined lifecycle
// generation with the one its backend reports. While holding mutation
// admission it re-matches the rows, re-collects complete inventory, and
// requires the exact plan the operator confirmed. One transaction then
// compare-and-swaps both rows: the lifecycle capability takes the observed
// generation and the stored principal without the quarantine, and the
// placement keeps its owner but drops the old generation's operation metadata
// under a new revision.
func (repair *AttemptRepair) AdoptObservedGenerationContext(
	ctx context.Context,
	plan GenerationAdoptionPlan,
	attestation GenerationAttestation,
	finalProbe GenerationAdoptionProbe,
) (GenerationAdoptionResult, error) {
	candidate := plan.candidate
	if repair == nil || repair.store == nil || plan.issuer != repair || candidate.issuer != repair {
		return GenerationAdoptionResult{}, fmt.Errorf(
			"%w: plan belongs to another repair session", ErrGenerationAdoptionTarget)
	}
	confirmation, err := generationAdoptionConfirmation(repair.store.db.Path(), plan)
	if err != nil || confirmation != plan.confirmation {
		return GenerationAdoptionResult{}, fmt.Errorf(
			"%w: generation adoption plan is incomplete or has been altered", ErrGenerationAdoptionTarget)
	}
	if err := validateBoundRepairContext(ctx, plan.context, plan.notAfter); err != nil {
		return GenerationAdoptionResult{}, fmt.Errorf("%w: %w", ErrGenerationAdoptionTarget, err)
	}
	if attestation.issuer != repair || attestation.confirmation != plan.confirmation {
		return GenerationAdoptionResult{}, fmt.Errorf(
			"%w: generation attestation is absent or belongs to another plan", ErrGenerationAdoptionTarget)
	}
	if finalProbe == nil {
		return GenerationAdoptionResult{}, fmt.Errorf(
			"%w: final live inventory probe is required", ErrGenerationAdoptionTarget)
	}
	if err := verifyBoltPhysicalConsistency(repair.store.db); err != nil {
		return GenerationAdoptionResult{}, fmt.Errorf(
			"validate placement db before generation adoption: %w", err)
	}

	store := repair.store
	store.mu.Lock()
	defer store.mu.Unlock()
	current, err := repair.matchGenerationQuarantineLocked(candidate.leaseUUID, candidate.backendName)
	if err != nil {
		return GenerationAdoptionResult{}, err
	}
	if !sameGenerationAdoptionCandidate(current, candidate) {
		return GenerationAdoptionResult{}, fmt.Errorf(
			"%w: the quarantined rows changed before adoption", ErrGenerationAdoptionTarget)
	}

	// Mutation admission stays held across this collection and rematch, closing
	// the evidence-to-write gap: the inventory the operator confirmed must still
	// be observable immediately before bbolt.
	finalInventory, err := finalProbe(ctx)
	if err != nil {
		return GenerationAdoptionResult{}, fmt.Errorf("final generation adoption inventory probe: %w", err)
	}
	if err := repair.validateInventorySnapshotLocked(finalInventory); err != nil {
		return GenerationAdoptionResult{}, err
	}
	finalPlan, err := repair.newGenerationAdoptionPlan(plan.context, plan.notAfter, candidate, finalInventory)
	if err != nil {
		return GenerationAdoptionResult{}, err
	}
	if !sameGenerationAdoptionPlan(plan, finalPlan) {
		return GenerationAdoptionResult{}, fmt.Errorf(
			"%w: final live inventory no longer matches the operator-confirmed plan",
			ErrGenerationAdoptionTarget)
	}
	if err := validateBoundRepairContext(ctx, plan.context, plan.notAfter); err != nil {
		return GenerationAdoptionResult{}, fmt.Errorf("%w: %w", ErrGenerationAdoptionTarget, err)
	}

	next, err := store.nextRevision()
	if err != nil {
		return GenerationAdoptionResult{}, err
	}
	previous := store.cache[candidate.leaseUUID]
	adopted := Placement{Backend: candidate.backendName, SetAt: previous.SetAt, revision: next}
	capability := lifecycleCapability{
		backend:   candidate.backendName,
		id:        plan.observedID,
		principal: candidate.principal,
	}
	// The pair committed must be the pair the next open accepts.
	if problem := lifecycleBindingProblem(adopted, capability); problem != "" ||
		!maintenanceAuthorityAvailable(adopted, capability, store.providerUUID) {
		return GenerationAdoptionResult{}, fmt.Errorf(
			"%w: the adopted pair would not bind one generation", ErrGenerationAdoptionTarget)
	}
	adoptedEncoded, err := encodePlacement(adopted)
	if err != nil {
		return GenerationAdoptionResult{}, mutationFailure("encode placement for generation adoption", err)
	}
	capabilityEncoded, err := encodeLifecycleCapability(capability)
	if err != nil {
		return GenerationAdoptionResult{}, mutationFailure("encode lifecycle for generation adoption", err)
	}
	if err := repair.verifySourcePath(); err != nil {
		return GenerationAdoptionResult{}, fmt.Errorf(
			"validate placement db path immediately before generation adoption: %w", err)
	}
	if err := repair.verifyPublishedBackupTarget(); err != nil {
		return GenerationAdoptionResult{}, fmt.Errorf(
			"validate exact backup authority immediately before generation adoption: %w", err)
	}
	if err := updateBoltWithExplicitOutcome(store.db, func(tx *bolt.Tx) error {
		if err := validateBoundRepairContext(ctx, plan.context, plan.notAfter); err != nil {
			return err
		}
		if err := requireGenerationRowsTx(
			tx, candidate.leaseUUID, candidate.placementEncoded, candidate.lifecycleEncoded,
		); err != nil {
			return errors.Join(ErrGenerationAdoptionTarget, err)
		}
		if err := tx.Bucket(bucketName).Put([]byte(candidate.leaseUUID), adoptedEncoded); err != nil {
			return err
		}
		if err := tx.Bucket(lifecycleCapabilityBucketName).Put(
			[]byte(candidate.leaseUUID), capabilityEncoded,
		); err != nil {
			return err
		}
		return validateBoundRepairContext(ctx, plan.context, plan.notAfter)
	}); err != nil {
		err = mutationFailure("adopt observed lifecycle generation", err)
		if classified := classifyRepairTransactionError(
			"adopt observed lifecycle generation", err,
		); errors.Is(classified, ErrRepairMutationOutcomeUnknown) {
			return GenerationAdoptionResult{}, classified
		}
		return GenerationAdoptionResult{}, err
	}
	store.cache[candidate.leaseUUID] = adopted
	store.lifecycleCache[candidate.leaseUUID] = capability
	store.revision = next
	if err := repair.verifySourcePathAfterMutation("adopt observed lifecycle generation"); err != nil {
		return GenerationAdoptionResult{}, err
	}
	if err := repair.verifyPublishedBackupTargetAfterMutation("adopt observed lifecycle generation"); err != nil {
		return GenerationAdoptionResult{}, err
	}
	if err := verifyAdoptedGenerationLocked(store, candidate.leaseUUID, adopted, capability); err != nil {
		return GenerationAdoptionResult{}, fmt.Errorf("%w: %w", ErrRepairMutationCommitted, err)
	}
	return GenerationAdoptionResult{leaseUUID: candidate.leaseUUID, expected: adopted, lifecycle: capability}, nil
}

// VerifyGenerationAdoptionPostcondition rechecks one adoption through a newly
// opened read-only inspector, whose load re-ran the open-time binding check:
// a pair that did not bind would have come back quarantined.
func (inspector *RepairInspector) VerifyGenerationAdoptionPostcondition(
	candidate GenerationAdoptionCandidate,
	result GenerationAdoptionResult,
) error {
	if inspector == nil || inspector.store == nil || candidate.issuer == nil ||
		result.leaseUUID != candidate.leaseUUID || result.expected.revision == 0 {
		return errors.New("lifecycle generation adoption postcondition is invalid")
	}
	if err := inspector.verifyOriginalRepairSource(candidate.issuer); err != nil {
		return fmt.Errorf("lifecycle generation adoption postcondition source: %w", err)
	}
	store := inspector.store
	store.mu.RLock()
	err := verifyAdoptedGenerationLocked(store, candidate.leaseUUID, result.expected, result.lifecycle)
	store.mu.RUnlock()
	if err != nil {
		return err
	}
	return inspector.verifyOriginalRepairSource(candidate.issuer)
}

// Caller holds store.mu.
func verifyAdoptedGenerationLocked(
	store *Store,
	leaseUUID string,
	wantPlacement Placement,
	wantCapability lifecycleCapability,
) error {
	got, placed := store.cache[leaseUUID]
	if !placed || !equalPlacementIgnoringRevision(got, wantPlacement) ||
		got.revision != wantPlacement.revision {
		return errors.New("verify generation adoption: placement differs from the adopted owner")
	}
	gotCapability, bound := store.lifecycleCache[leaseUUID]
	if !bound || gotCapability != wantCapability {
		return errors.New("verify generation adoption: lifecycle authority differs from the adopted generation")
	}
	if !maintenanceAuthorityAvailable(got, gotCapability, store.providerUUID) {
		return errors.New("verify generation adoption: the adopted generation cannot authorize maintenance")
	}
	placementEncoded, err := encodePlacement(wantPlacement)
	if err != nil {
		return fmt.Errorf("verify generation adoption: encode placement: %w", err)
	}
	capabilityEncoded, err := encodeLifecycleCapability(wantCapability)
	if err != nil {
		return fmt.Errorf("verify generation adoption: encode lifecycle: %w", err)
	}
	return store.db.View(func(tx *bolt.Tx) error {
		placements := tx.Bucket(bucketName)
		capabilities := tx.Bucket(lifecycleCapabilityBucketName)
		if placements == nil || capabilities == nil {
			return errors.New("verify generation adoption: placement lifecycle buckets are missing")
		}
		if !bytes.Equal(placements.Get([]byte(leaseUUID)), placementEncoded) {
			return errors.New("verify generation adoption: durable placement differs from cache")
		}
		if !bytes.Equal(capabilities.Get([]byte(leaseUUID)), capabilityEncoded) {
			return errors.New("verify generation adoption: durable lifecycle differs from cache")
		}
		return nil
	})
}
