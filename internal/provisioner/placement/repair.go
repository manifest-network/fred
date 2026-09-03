package placement

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

var (
	// ErrAttemptRepairTarget means the supplied lease, backend, operation ID, or
	// previously inspected revision is not the exact canonical unresolved attempt
	// currently persisted in the placement database.
	ErrAttemptRepairTarget = errors.New("placement repair target does not match the current typed attempt")

	// ErrAttemptRepairSchema means the database cannot be proven to be an
	// initialized current placement store without running a migration. Offline
	// repair never initializes, migrates, or normalizes a database on open.
	ErrAttemptRepairSchema = errors.New("placement repair requires an initialized current placement database")

	// ErrConflictRepairTarget means the supplied lease, selected owner, or
	// previously inspected conflict revision/candidate set is no longer the
	// exact durable conflict held by this repair session.
	ErrConflictRepairTarget = errors.New("placement conflict repair target does not match the current durable conflict")

	// ErrRepairMutationCommitted means the repair's bbolt transaction returned
	// success, but its subsequent invariant verification failed. Callers must
	// treat the on-disk result as committed and inspect it rather than retrying.
	ErrRepairMutationCommitted = errors.New("placement repair mutation committed before verification failed")

	// ErrRepairMutationOutcomeUnknown means bbolt Commit returned an error after
	// the repair transaction was fully assembled. The on-disk effect may or may
	// not be visible; callers must preserve both files and inspect, never retry.
	ErrRepairMutationOutcomeUnknown = errors.New("placement repair commit outcome is unknown")
)

// RepairInspector holds a shared, read-only bbolt lock on an existing
// placement database. It deliberately exposes facts, not mutation
// capabilities: an operator must reopen the database exclusively and rematch
// the exact target before placement-repair can change anything.
type RepairInspector struct {
	store      *Store
	sourceInfo os.FileInfo
	authority  *offlinePlacementAuthority
}

// RepairRecord is an immutable diagnostic rendering of one durable placement
// row. All values needed to identify a repair target are explicit; invalid or
// legacy operation identities are rendered as an empty string.
type RepairRecord struct {
	LeaseUUID              string              `json:"lease_uuid"`
	State                  string              `json:"state"`
	Backend                string              `json:"backend,omitempty"`
	Attempt                string              `json:"attempt,omitempty"`
	OperationID            string              `json:"operation_id,omitempty"`
	OperationKind          string              `json:"operation_kind,omitempty"`
	RestoreSourceLeaseUUID string              `json:"restore_source_lease_uuid,omitempty"`
	PayloadHash            string              `json:"payload_hash,omitempty"`
	Tenant                 string              `json:"tenant,omitempty"`
	ProviderUUID           string              `json:"provider_uuid,omitempty"`
	RequestItems           []backend.LeaseItem `json:"request_items,omitempty"`
	Revision               uint64              `json:"revision"`
	Conflict               bool                `json:"conflict"`
	UntrustedPositive      bool                `json:"untrusted_positive"`
	ConflictBackends       []string            `json:"conflict_backends,omitempty"`
	ConflictOwnersUnknown  bool                `json:"conflict_owners_unknown"`
}

// OpenRepairInspector opens an existing placement database read-only and
// verifies that its durable authority belongs to providerUUID. The shared lock
// still conflicts with a live providerd writer, making inspection an offline
// operation without granting the process write access to the file.
func OpenRepairInspector(dbPath, providerUUID string) (*RepairInspector, error) {
	db, authority, err := openRepairDB(dbPath, providerUUID, true)
	if err != nil {
		return nil, err
	}
	store, err := loadStore(db)
	if err != nil {
		_ = db.Close()
		_ = authority.close()
		return nil, fmt.Errorf("%w: %w", ErrAttemptRepairSchema, err)
	}
	if err := store.VerifyProviderUUID(providerUUID); err != nil {
		_ = store.Close()
		_ = authority.close()
		return nil, err
	}
	return &RepairInspector{store: store, sourceInfo: authority.info, authority: authority}, nil
}

// Close releases the read-only database lock. It is idempotent.
func (inspector *RepairInspector) Close() error {
	if inspector == nil || inspector.store == nil {
		return nil
	}
	return errors.Join(inspector.store.Close(), inspector.authority.close())
}

// BackendTopology returns the exact durable active topology recorded with the
// inspected database.
func (inspector *RepairInspector) BackendTopology() []string {
	if inspector == nil || inspector.store == nil {
		return nil
	}
	inspector.store.mu.RLock()
	defer inspector.store.mu.RUnlock()
	return slices.Clone(inspector.store.backendTopology)
}

// List returns every durable placement row sorted by canonical lease identity.
func (inspector *RepairInspector) List() []RepairRecord {
	if inspector == nil || inspector.store == nil {
		return nil
	}
	placements := inspector.store.List()
	keys := slices.Sorted(maps.Keys(placements))
	records := make([]RepairRecord, 0, len(keys))
	for _, leaseUUID := range keys {
		records = append(records, newRepairRecord(leaseUUID, placements[leaseUUID]))
	}
	return records
}

// Inspect returns the exact durable row for leaseUUID. Missing rows are
// reported as absent rather than synthesized into the list output.
func (inspector *RepairInspector) Inspect(leaseUUID string) (RepairRecord, bool, error) {
	if inspector == nil || inspector.store == nil {
		return RepairRecord{}, false, errors.New("placement repair inspector is not open")
	}
	if !canonicalLeaseUUID(leaseUUID) {
		return RepairRecord{}, false, fmt.Errorf("lease UUID %q is not canonical", leaseUUID)
	}
	placements := inspector.store.List()
	p, exists := placements[leaseUUID]
	if !exists {
		return RepairRecord{}, false, nil
	}
	return newRepairRecord(leaseUUID, p), true, nil
}

func newRepairRecord(leaseUUID string, p Placement) RepairRecord {
	operationID := ""
	operationKind := ""
	restoreSourceLeaseUUID := ""
	payloadHash := ""
	requestSnapshot := BackendRequestSnapshot{}
	if p.Attempt != "" && p.attemptOperationID.Valid() {
		operationID = p.attemptOperationID.String()
	}
	if p.Attempt != "" && p.attemptOperationKind.Valid() {
		operationKind = p.attemptOperationKind.String()
	}
	if p.Attempt != "" {
		restoreSourceLeaseUUID = p.attemptRestoreSourceLeaseUUID
		payloadHash = p.attemptPayloadFingerprint.String()
		requestSnapshot = p.attemptRequestSnapshot
	}
	return RepairRecord{
		LeaseUUID:              leaseUUID,
		State:                  p.State().String(),
		Backend:                p.Backend,
		Attempt:                p.Attempt,
		OperationID:            operationID,
		OperationKind:          operationKind,
		RestoreSourceLeaseUUID: restoreSourceLeaseUUID,
		PayloadHash:            payloadHash,
		Tenant:                 requestSnapshot.Tenant(),
		ProviderUUID:           requestSnapshot.ProviderUUID(),
		RequestItems:           requestSnapshot.Items(),
		Revision:               p.Revision(),
		Conflict:               p.Conflict,
		UntrustedPositive:      p.untrustedPositive,
		ConflictBackends:       slices.Clone(p.ConflictBackends),
		ConflictOwnersUnknown:  p.ConflictOwnersUnknown,
	}
}

// AttemptRepair holds an exclusive bbolt lock on an existing placement
// database. Opening it performs only read transactions; the only exposed
// mutation is Refuse, which delegates to Store.RefuseOperation for an opaque
// candidate returned by MatchAttempt.
type AttemptRepair struct {
	store                 *Store
	sourceInfo            os.FileInfo
	authority             *offlinePlacementAuthority
	publishedBackupTarget *ExactBackupTarget
}

// verifySourcePath proves the configured pathname still identifies the exact
// inode opened by this repair session. bbolt retains its original file
// descriptor across rename-over-open, so its exclusive file lock alone cannot
// prevent a different database from being installed at db.Path().
func (repair *AttemptRepair) verifySourcePath() error {
	if repair == nil || repair.store == nil || repair.store.db == nil || repair.sourceInfo == nil {
		return errors.New("placement repair source identity is unavailable")
	}
	if repair.authority != nil {
		if err := repair.authority.verify(); err != nil {
			return fmt.Errorf("configured placement db no longer identifies the opened repair database: %w", err)
		}
		return nil
	}
	path := repair.store.db.Path()
	currentInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat configured placement db %q: %w", path, err)
	}
	if !currentInfo.Mode().IsRegular() || !os.SameFile(repair.sourceInfo, currentInfo) {
		return fmt.Errorf("configured placement db %q no longer identifies the opened repair database", path)
	}
	return nil
}

// verifySourcePathAfterMutation converts a pathname identity loss after a
// successful transaction into the committed-result class. The caller must not
// retry: the open inode was mutated even if another file now occupies the
// configured pathname.
func (repair *AttemptRepair) verifySourcePathAfterMutation(operation string) error {
	if err := repair.verifySourcePath(); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrRepairMutationCommitted, operation, err)
	}
	return nil
}

// ExpectedBackendStorageIdentity exposes only the active durable identity pin
// needed to construct offline identity-bound inventory clients.
func (repair *AttemptRepair) ExpectedBackendStorageIdentity(
	backendName string,
) (backendidentity.ID, bool) {
	if repair == nil || repair.store == nil {
		return backendidentity.ID{}, false
	}
	return repair.store.ExpectedBackendStorageIdentity(backendName)
}

// AttemptRepairCandidate is an opaque, store-bound snapshot of one exact
// current typed attempt. Its zero value is invalid. Callers may inspect its
// facts and use ConfirmationValue for an operator confirmation prompt, but
// cannot manufacture or transplant a candidate.
type AttemptRepairCandidate struct {
	issuer           *AttemptRepair
	leaseUUID        string
	backendName      string
	operationID      operation.OperationID
	providerUUID     string
	tenant           string
	revision         uint64
	topologyID       uint64
	confirmedOwner   string
	lifecycleAfter   lifecycleCapability
	retainLifecycle  bool
	placementEncoded []byte
	lifecycleEncoded []byte
}

// AttemptRepairResult describes the postcondition proved after an exact
// refusal. ConfirmedOwner is empty when the attempt-only row was removed; when
// nonempty, that owner was preserved and only its matching attempt was cleared.
type AttemptRepairResult struct {
	ConfirmedOwner string
}

// ConflictRepairCandidate is an opaque, repair-session-bound selector for one
// exact revisioned conflict and its complete durable candidate set. It carries
// no mutation authority; only PlanConflictRepairContext can combine it with
// exact live evidence to mint a ConflictRepairPlan.
type ConflictRepairCandidate struct {
	issuer            *AttemptRepair
	leaseUUID         string
	selectedBackend   string
	providerUUID      string
	expectedTenant    string
	revision          uint64
	topologyID        uint64
	candidateBackends []string
	placementEncoded  []byte
}

// AttemptRepairEvidence is an opaque, store-minted proof that complete live
// inventory was checked for this exact candidate and durable topology. Its zero
// value is invalid and it cannot be transplanted across repair sessions or
// candidates.
type AttemptRepairEvidence struct {
	issuer       *AttemptRepair
	confirmation string
	topologyID   uint64
	digest       [sha256.Size]byte
	context      context.Context
	notAfter     time.Time
}

// AttemptRepairProbe performs the mandatory final complete-fleet re-probe
// while RefuseContext holds mutation admission.
type AttemptRepairProbe func(context.Context) (RepairInventorySnapshot, error)

// ConflictRepairPlan is the sole conflict-mutation capability. It can only be
// minted from one exact durable conflict and one complete, identity-bound live
// inventory snapshot. Its confirmation binds every load-bearing fact, and its
// authority expires with the context that collected that snapshot.
type ConflictRepairPlan struct {
	issuer                *AttemptRepair
	candidate             ConflictRepairCandidate
	context               context.Context
	notAfter              time.Time
	digest                [sha256.Size]byte
	selectedStorageID     backendidentity.ID
	providerUUID          string
	tenant                string
	identityAuthoritative bool
	retained              bool
	lifecycle             LifecycleObservation
	confirmation          string
}

// ConflictRepairProbe performs the mandatory final complete-fleet re-probe
// while ResolveConflictContext holds mutation admission. A caller cannot
// substitute a prevalidated boolean: the returned opaque inventory snapshot is
// revalidated and rematched byte-for-byte to the plan before the transaction.
type ConflictRepairProbe func(context.Context) (RepairInventorySnapshot, error)

// DrainAttestation is a separate operator capability. Inventory can prove
// current presence/absence, but only an operator can attest that delayed
// backend requests, effects, and callback replay were drained. It is bound to
// one candidate confirmation value and repair session.
type DrainAttestation struct {
	issuer       *AttemptRepair
	confirmation string
}

// DrainAttestationText is the exact high-friction statement required to mint a
// DrainAttestation. Keeping it in the authority package prevents another
// caller from silently weakening the CLI's causal-safety contract.
const DrainAttestationText = "I attest all delayed backend requests/effects and callback replay are drained"

// ConflictRepairResult describes the exact confirmed owner written by a
// successful conflict resolution and whether that evidence was an active
// provision or a retained lease.
type ConflictRepairResult struct {
	ConfirmedOwner string
	Retained       bool
	expected       Placement
	lifecycle      lifecycleCapability
}

// OpenAttemptRepair opens an existing regular bbolt file in read-write mode to
// acquire an exclusive process lock, but performs no write transaction. It
// strips O_CREATE from bbolt's open flags so a path removed after the initial
// stat cannot be recreated. Missing/current-schema checks fail closed, and the
// exact durable provider authority must match providerUUID before a mutation
// capability is returned.
func OpenAttemptRepair(dbPath, providerUUID string) (*AttemptRepair, error) {
	db, authority, err := openRepairDB(dbPath, providerUUID, false)
	if err != nil {
		return nil, err
	}
	store, err := loadStore(db)
	if err != nil {
		_ = db.Close()
		_ = authority.close()
		return nil, fmt.Errorf("%w: %w", ErrAttemptRepairSchema, err)
	}
	if err := store.VerifyProviderUUID(providerUUID); err != nil {
		_ = store.Close()
		_ = authority.close()
		return nil, err
	}
	return &AttemptRepair{store: store, sourceInfo: authority.info, authority: authority}, nil
}

func openRepairDB(
	dbPath, providerUUID string,
	readOnly bool,
) (*bolt.DB, *offlinePlacementAuthority, error) {
	if !canonicalLeaseUUID(providerUUID) {
		return nil, nil, fmt.Errorf("%w: configured provider UUID %q is not canonical",
			ErrProviderAuthorityMismatch, providerUUID)
	}
	authority, err := bindOfflinePlacementAuthority(dbPath)
	if err != nil {
		return nil, nil, err
	}
	if authority.info.Size() == 0 {
		_ = authority.close()
		return nil, nil, fmt.Errorf("%w: placement database is empty", ErrAttemptRepairSchema)
	}
	db, err := authority.openBolt(readOnly)
	if err != nil {
		_ = authority.close()
		if errors.Is(err, bolt.ErrTimeout) {
			lockMode := "for offline inspection"
			if !readOnly {
				lockMode = "exclusively"
			}
			return nil, nil, fmt.Errorf("lock stopped placement db %s (is providerd still running?): %w", lockMode, err)
		}
		mode := "read-only"
		if !readOnly {
			mode = "exclusively"
		}
		return nil, nil, fmt.Errorf("open existing placement db %s: %w", mode, err)
	}
	if err := verifyBoltPhysicalConsistency(db); err != nil {
		_ = db.Close()
		_ = authority.close()
		return nil, nil, fmt.Errorf("validate placement db physical consistency: %w", err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketName) == nil {
			return errors.New("placements bucket is missing")
		}
		if tx.Bucket(lifecycleCapabilityBucketName) == nil {
			return errors.New("placement lifecycle capability bucket is missing")
		}
		if _, err := loadTopologyMetadata(tx); err != nil {
			return err
		}
		return nil
	}); err != nil {
		_ = db.Close()
		_ = authority.close()
		return nil, nil, fmt.Errorf("%w: %w", ErrAttemptRepairSchema, err)
	}

	return db, authority, nil
}

// Close releases the exclusive database lock. It is idempotent.
func (repair *AttemptRepair) Close() error {
	if repair == nil || repair.store == nil {
		return nil
	}
	return errors.Join(repair.store.Close(), repair.authority.close())
}

// Sync flushes the underlying database file and then runs bbolt's physical
// page/freelist checker. Mutating tools call this after semantic verification
// and before Close/PASS so a successful verdict includes both a durability
// barrier and same-handle physical validation.
func (repair *AttemptRepair) Sync() error {
	if repair == nil || repair.store == nil || repair.store.db == nil {
		return errors.New("placement repair is not open")
	}
	if err := repair.store.db.Sync(); err != nil {
		return fmt.Errorf("sync placement repair: %w", err)
	}
	if err := verifyBoltPhysicalConsistency(repair.store.db); err != nil {
		return fmt.Errorf("validate synced placement repair physical consistency: %w", err)
	}
	return nil
}

// CreateExactBackup publishes a byte-for-byte, no-overwrite copy of the
// exclusively locked stopped database. Mutating tools call it only after all
// evidence and attestations pass and immediately before the write transaction.
func (repair *AttemptRepair) CreateExactBackup(target *ExactBackupTarget) error {
	if repair == nil || repair.store == nil || repair.store.db == nil {
		return errors.New("placement repair is not open")
	}
	if repair.publishedBackupTarget != nil {
		return errors.New("exact pre-repair backup was already published for this repair session")
	}
	if err := writeExactBackup(repair.store.db, repair.sourceInfo, target); err != nil {
		return fmt.Errorf("create exact pre-repair backup: %w", err)
	}
	repair.publishedBackupTarget = target
	return nil
}

func (repair *AttemptRepair) verifyPublishedBackupTarget() error {
	if repair == nil || repair.publishedBackupTarget == nil {
		return errors.New("published exact pre-repair backup capability is absent")
	}
	return repair.publishedBackupTarget.VerifyPublished()
}

func (repair *AttemptRepair) verifyPublishedBackupTargetAfterMutation(operation string) error {
	if err := repair.verifyPublishedBackupTarget(); err != nil {
		return fmt.Errorf("%w: %s: exact backup authority changed: %w",
			ErrRepairMutationCommitted, operation, err)
	}
	return nil
}

// BackendTopology returns the exact durable active topology bound to this
// database. Offline tools require the supplied providerd config to match it so
// an omitted backend can never turn silence into evidence of absence.
func (repair *AttemptRepair) BackendTopology() []string {
	if repair == nil || repair.store == nil {
		return nil
	}
	repair.store.mu.RLock()
	defer repair.store.mu.RUnlock()
	return slices.Clone(repair.store.backendTopology)
}

// MatchConflict returns an opaque candidate only when the durable placement is
// one canonical revisioned conflict with a complete known candidate set, every
// candidate is in the active durable topology, and selectedBackend is one of
// those candidates. Unknown-owner and corrupt conflicts remain unrepairable by
// this command because current-fleet absence cannot account for a lost owner.
func (repair *AttemptRepair) MatchConflict(
	leaseUUID, selectedBackend string,
) (ConflictRepairCandidate, error) {
	if repair == nil || repair.store == nil {
		return ConflictRepairCandidate{}, errors.New("placement repair is not open")
	}
	if !canonicalLeaseUUID(leaseUUID) {
		return ConflictRepairCandidate{}, fmt.Errorf(
			"%w: lease UUID %q is not canonical", ErrConflictRepairTarget, leaseUUID,
		)
	}
	if selectedBackend == "" {
		return ConflictRepairCandidate{}, fmt.Errorf(
			"%w: selected backend is required", ErrConflictRepairTarget,
		)
	}

	store := repair.store
	store.mu.RLock()
	defer store.mu.RUnlock()
	p, exists := store.cache[leaseUUID]
	if !exists || p.unusable || !p.Conflict || p.ConflictOwnersUnknown ||
		p.revision == 0 || len(p.ConflictBackends) < 2 {
		return ConflictRepairCandidate{}, fmt.Errorf(
			"%w: lease %q is not a revisioned conflict with a complete owner set",
			ErrConflictRepairTarget, leaseUUID,
		)
	}
	candidates := normalizeBackendNames(p.ConflictBackends)
	if !slices.Equal(candidates, p.ConflictBackends) {
		return ConflictRepairCandidate{}, fmt.Errorf(
			"%w: conflict candidate set is not canonical", ErrConflictRepairTarget,
		)
	}
	if p.Backend != "" && !slices.Contains(candidates, p.Backend) {
		return ConflictRepairCandidate{}, fmt.Errorf(
			"%w: confirmed backend is missing from the candidate set", ErrConflictRepairTarget,
		)
	}
	if p.Attempt != "" && !slices.Contains(candidates, p.Attempt) {
		return ConflictRepairCandidate{}, fmt.Errorf(
			"%w: attempted backend is missing from the candidate set", ErrConflictRepairTarget,
		)
	}
	if p.attemptRequestSnapshot.Valid() &&
		p.attemptRequestSnapshot.ProviderUUID() != store.providerUUID {
		return ConflictRepairCandidate{}, fmt.Errorf(
			"%w: durable conflict attempt provider %q does not match store provider %q",
			ErrConflictRepairTarget,
			p.attemptRequestSnapshot.ProviderUUID(), store.providerUUID,
		)
	}
	if store.topologyID == 0 {
		return ConflictRepairCandidate{}, fmt.Errorf(
			"%w: durable backend topology is not configured", ErrConflictRepairTarget,
		)
	}
	for _, backendName := range candidates {
		if _, configured := store.backendTopologySet[backendName]; !configured {
			return ConflictRepairCandidate{}, fmt.Errorf(
				"%w: candidate backend %q is outside the active durable topology",
				ErrConflictRepairTarget, backendName,
			)
		}
	}
	if !slices.Contains(candidates, selectedBackend) {
		return ConflictRepairCandidate{}, fmt.Errorf(
			"%w: selected backend %q is not a durable conflict candidate",
			ErrConflictRepairTarget, selectedBackend,
		)
	}
	encoded, err := encodePlacement(p)
	if err != nil {
		return ConflictRepairCandidate{}, fmt.Errorf("encode conflict repair target: %w", err)
	}
	if err := store.db.View(func(tx *bolt.Tx) error {
		placements := tx.Bucket(bucketName)
		if placements == nil {
			return errors.New("placements bucket is missing")
		}
		if !bytes.Equal(placements.Get([]byte(leaseUUID)), encoded) {
			return errors.New("placement row is not the canonical current encoding")
		}
		return nil
	}); err != nil {
		return ConflictRepairCandidate{}, fmt.Errorf("%w: %w", ErrConflictRepairTarget, err)
	}
	return ConflictRepairCandidate{
		issuer:            repair,
		leaseUUID:         leaseUUID,
		selectedBackend:   selectedBackend,
		providerUUID:      store.providerUUID,
		expectedTenant:    p.attemptRequestSnapshot.Tenant(),
		revision:          p.revision,
		topologyID:        store.topologyID,
		candidateBackends: slices.Clone(candidates),
		placementEncoded:  slices.Clone(encoded),
	}, nil
}

// LeaseUUID returns the exact conflicted lease.
func (candidate ConflictRepairCandidate) LeaseUUID() string { return candidate.leaseUUID }

// SelectedBackend returns the operator-selected owner that live inventory must
// positively report exactly once.
func (candidate ConflictRepairCandidate) SelectedBackend() string {
	return candidate.selectedBackend
}

// Revision returns the exact durable conflict revision shown to the operator.
func (candidate ConflictRepairCandidate) Revision() uint64 { return candidate.revision }

// CandidateBackends returns a defensive copy of the complete durable candidate
// set that every live probe must account for.
func (candidate ConflictRepairCandidate) CandidateBackends() []string {
	return slices.Clone(candidate.candidateBackends)
}

// ConfirmationValue is the durable selector fingerprint incorporated into a
// later ConflictRepairPlan. It is not itself an operator mutation
// confirmation. Candidate names are hashed from canonical JSON so
// delimiter-bearing backend names cannot produce an ambiguous value.
func (candidate ConflictRepairCandidate) ConfirmationValue() string {
	if candidate.issuer == nil || candidate.leaseUUID == "" ||
		candidate.selectedBackend == "" || candidate.revision == 0 ||
		candidate.topologyID == 0 || len(candidate.candidateBackends) < 2 {
		return ""
	}
	payload, err := json.Marshal(struct {
		LeaseUUID         string   `json:"lease_uuid"`
		SelectedBackend   string   `json:"selected_backend"`
		Revision          uint64   `json:"revision"`
		TopologyID        uint64   `json:"topology_id"`
		CandidateBackends []string `json:"candidate_backends"`
	}{
		LeaseUUID:         candidate.leaseUUID,
		SelectedBackend:   candidate.selectedBackend,
		Revision:          candidate.revision,
		TopologyID:        candidate.topologyID,
		CandidateBackends: candidate.candidateBackends,
	})
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(payload)
	return fmt.Sprintf("resolve-conflict:%s:%s:%d:%s",
		candidate.leaseUUID, candidate.selectedBackend, candidate.revision,
		hex.EncodeToString(digest[:]),
	)
}

// PlanConflictRepairContext mints the one conflict-mutation capability only
// after matching complete live evidence to the exact current durable conflict.
// The caller's context must have a deadline; the plan is bound to that exact
// cancellation scope and expires at its deadline.
func (repair *AttemptRepair) PlanConflictRepairContext(
	ctx context.Context,
	candidate ConflictRepairCandidate,
	inventory RepairInventorySnapshot,
) (ConflictRepairPlan, error) {
	notAfter, err := bindRepairEvidenceContext(ctx)
	if err != nil {
		return ConflictRepairPlan{}, fmt.Errorf("%w: %w", ErrConflictRepairTarget, err)
	}
	if repair == nil || repair.store == nil || candidate.issuer != repair {
		return ConflictRepairPlan{}, fmt.Errorf(
			"%w: candidate belongs to another repair session", ErrConflictRepairTarget,
		)
	}
	if err := repair.validateInventorySnapshot(inventory); err != nil {
		return ConflictRepairPlan{}, err
	}
	current, err := repair.MatchConflict(candidate.leaseUUID, candidate.selectedBackend)
	if err != nil {
		return ConflictRepairPlan{}, err
	}
	if !sameConflictRepairCandidate(current, candidate) {
		return ConflictRepairPlan{}, fmt.Errorf(
			"%w: candidate revision, topology, or owner set is stale", ErrConflictRepairTarget,
		)
	}
	owner, err := repair.matchConflictInventory(candidate, inventory)
	if err != nil {
		return ConflictRepairPlan{}, err
	}
	plan := ConflictRepairPlan{
		issuer:                repair,
		candidate:             candidate,
		context:               ctx,
		notAfter:              notAfter,
		digest:                inventory.digest,
		selectedStorageID:     inventory.inventories[candidate.selectedBackend].StorageIdentity,
		providerUUID:          candidate.providerUUID,
		tenant:                owner.tenant,
		identityAuthoritative: owner.identityAuthoritative,
		retained:              owner.retained,
		lifecycle:             owner.lifecycle,
	}
	plan.confirmation, err = conflictRepairPlanConfirmation(plan)
	if err != nil {
		return ConflictRepairPlan{}, err
	}
	return plan, nil
}

type conflictRepairObservation struct {
	retained              bool
	lifecycle             LifecycleObservation
	tenant                string
	identityAuthoritative bool
}

func (repair *AttemptRepair) matchConflictInventory(
	candidate ConflictRepairCandidate,
	inventory RepairInventorySnapshot,
) (conflictRepairObservation, error) {
	type positive struct {
		backendName string
		provision   *backend.ProvisionInfo
		retention   *backend.RetainedLease
	}
	var positives []positive
	for _, backendName := range inventory.backends {
		observed := inventory.inventories[backendName]
		for index := range observed.Provisions {
			provision := &observed.Provisions[index]
			if provision.LeaseUUID == candidate.leaseUUID {
				positives = append(positives, positive{backendName: backendName, provision: provision})
			}
		}
		for index := range observed.Retentions {
			retention := &observed.Retentions[index]
			if retention.LeaseUUID == candidate.leaseUUID {
				positives = append(positives, positive{backendName: backendName, retention: retention})
			}
		}
	}
	if len(positives) != 1 || positives[0].backendName != candidate.selectedBackend {
		return conflictRepairObservation{}, fmt.Errorf(
			"%w: lease %q must have exactly one positive observation on selected backend %q",
			ErrRepairConflictEvidence, candidate.leaseUUID, candidate.selectedBackend,
		)
	}
	owner := positives[0]
	if owner.provision != nil {
		if owner.provision.ProviderUUID != candidate.providerUUID {
			return conflictRepairObservation{}, fmt.Errorf(
				"%w: selected active provision provider %q does not match durable provider %q",
				ErrRepairConflictEvidence, owner.provision.ProviderUUID, candidate.providerUUID,
			)
		}
		if candidate.expectedTenant != "" && owner.provision.Tenant != candidate.expectedTenant {
			return conflictRepairObservation{}, fmt.Errorf(
				"%w: selected active provision tenant %q does not match durable attempt tenant %q",
				ErrRepairConflictEvidence, owner.provision.Tenant, candidate.expectedTenant,
			)
		}
		lifecycleObservation := repairLifecycleObservation(owner.provision.LifecycleGeneration)
		if err := validateLifecycleObservation(lifecycleObservation); err != nil {
			return conflictRepairObservation{}, fmt.Errorf("conflict owner evidence: %w", err)
		}
		authoritative := owner.provision.Tenant != ""
		if !authoritative {
			// The routing observation remains useful, but an identity-incomplete
			// backend response must never mint runtime callback authority.
			lifecycleObservation = LifecycleObservation{Kind: LifecycleObservationUnusable}
		}
		return conflictRepairObservation{
			lifecycle:             lifecycleObservation,
			tenant:                owner.provision.Tenant,
			identityAuthoritative: authoritative,
		}, nil
	}

	retention := owner.retention
	if retention.ProviderUUID != "" && retention.ProviderUUID != candidate.providerUUID {
		return conflictRepairObservation{}, fmt.Errorf(
			"%w: selected retention provider %q does not match durable provider %q",
			ErrRepairConflictEvidence, retention.ProviderUUID, candidate.providerUUID,
		)
	}
	if candidate.expectedTenant != "" && retention.Tenant != "" &&
		retention.Tenant != candidate.expectedTenant {
		return conflictRepairObservation{}, fmt.Errorf(
			"%w: selected retention tenant %q does not match durable attempt tenant %q",
			ErrRepairConflictEvidence, retention.Tenant, candidate.expectedTenant,
		)
	}
	authoritative := retention.ProviderUUID == candidate.providerUUID && retention.Tenant != "" &&
		(candidate.expectedTenant == "" || retention.Tenant == candidate.expectedTenant)
	tenant := retention.Tenant
	if tenant == "" {
		tenant = candidate.expectedTenant
	}
	return conflictRepairObservation{
		retained:              true,
		lifecycle:             LifecycleObservation{Kind: LifecycleObservationUnusable},
		tenant:                tenant,
		identityAuthoritative: authoritative,
	}, nil
}

func conflictRepairPlanConfirmation(plan ConflictRepairPlan) (string, error) {
	candidate := plan.candidate
	if plan.issuer == nil || candidate.issuer != plan.issuer ||
		!plan.selectedStorageID.Valid() || plan.digest == ([sha256.Size]byte{}) ||
		candidate.ConfirmationValue() == "" || plan.providerUUID == "" {
		return "", errors.New("conflict repair plan is incomplete")
	}
	lifecycleID := ""
	if plan.lifecycle.ID.Valid() {
		lifecycleID = plan.lifecycle.ID.String()
	}
	ownerKind := "active"
	if plan.retained {
		ownerKind = "retained"
	}
	payload, err := json.Marshal(struct {
		LeaseUUID             string   `json:"lease_uuid"`
		SelectedBackend       string   `json:"selected_backend"`
		SelectedStorageID     string   `json:"selected_storage_id"`
		ProviderUUID          string   `json:"provider_uuid"`
		Tenant                string   `json:"tenant,omitempty"`
		IdentityAuthoritative bool     `json:"identity_authoritative"`
		OwnerKind             string   `json:"owner_kind"`
		LifecycleKind         string   `json:"lifecycle_kind"`
		LifecycleID           string   `json:"lifecycle_id,omitempty"`
		Revision              uint64   `json:"revision"`
		TopologyID            uint64   `json:"topology_id"`
		CandidateBackends     []string `json:"candidate_backends"`
		InventoryDigest       string   `json:"inventory_digest"`
	}{
		LeaseUUID:             candidate.leaseUUID,
		SelectedBackend:       candidate.selectedBackend,
		SelectedStorageID:     plan.selectedStorageID.String(),
		ProviderUUID:          plan.providerUUID,
		Tenant:                plan.tenant,
		IdentityAuthoritative: plan.identityAuthoritative,
		OwnerKind:             ownerKind,
		LifecycleKind:         string(plan.lifecycle.Kind),
		LifecycleID:           lifecycleID,
		Revision:              candidate.revision,
		TopologyID:            candidate.topologyID,
		CandidateBackends:     candidate.candidateBackends,
		InventoryDigest:       hex.EncodeToString(plan.digest[:]),
	})
	if err != nil {
		return "", fmt.Errorf("encode conflict repair plan confirmation: %w", err)
	}
	digest := sha256.Sum256(payload)
	return fmt.Sprintf("resolve-conflict:%s:%s:%d:%s",
		candidate.leaseUUID, candidate.selectedBackend, candidate.revision,
		hex.EncodeToString(digest[:]),
	), nil
}

// ConfirmationValue is the exact operator confirmation for all facts in the
// plan, including selected storage identity and the complete inventory digest.
func (plan ConflictRepairPlan) ConfirmationValue() string { return plan.confirmation }

func (plan ConflictRepairPlan) LeaseUUID() string { return plan.candidate.LeaseUUID() }

func (plan ConflictRepairPlan) SelectedBackend() string {
	return plan.candidate.SelectedBackend()
}

func (plan ConflictRepairPlan) Revision() uint64 { return plan.candidate.Revision() }

func (plan ConflictRepairPlan) CandidateBackends() []string {
	return plan.candidate.CandidateBackends()
}

// MatchAttempt returns an opaque candidate only when every supplied fact and
// both durable rows exactly match one canonical current typed attempt. It also
// requires the attempt backend to belong to the database's active topology.
func (repair *AttemptRepair) MatchAttempt(
	leaseUUID, backendName string,
	operationID operation.OperationID,
) (AttemptRepairCandidate, error) {
	return repair.matchAttempt(leaseUUID, backendName, operationID)
}

func (repair *AttemptRepair) matchAttempt(
	leaseUUID, backendName string,
	operationID operation.OperationID,
) (AttemptRepairCandidate, error) {
	if repair == nil || repair.store == nil {
		return AttemptRepairCandidate{}, errors.New("placement repair is not open")
	}
	if !canonicalLeaseUUID(leaseUUID) {
		return AttemptRepairCandidate{}, fmt.Errorf("%w: lease UUID %q is not canonical", ErrAttemptRepairTarget, leaseUUID)
	}
	if err := validateTypedAttempt(leaseUUID, backendName, operationID); err != nil {
		return AttemptRepairCandidate{}, fmt.Errorf("%w: %w", ErrAttemptRepairTarget, err)
	}

	store := repair.store
	store.mu.RLock()
	defer store.mu.RUnlock()
	placement, exists := store.cache[leaseUUID]
	if !exists {
		return AttemptRepairCandidate{}, fmt.Errorf("%w: lease %q has no placement row", ErrAttemptRepairTarget, leaseUUID)
	}
	if store.topologyID == 0 {
		return AttemptRepairCandidate{}, fmt.Errorf("%w: durable backend topology is not configured", ErrAttemptRepairTarget)
	}
	if _, configured := store.backendTopologySet[backendName]; !configured {
		return AttemptRepairCandidate{}, fmt.Errorf(
			"%w: attempted backend %q is not in the durable active topology",
			ErrAttemptRepairTarget, backendName,
		)
	}
	if placement.unusable || placement.Conflict || placement.revision == 0 ||
		placement.Attempt == "" || !placement.attemptOperationID.Valid() {
		return AttemptRepairCandidate{}, fmt.Errorf(
			"%w: lease %q is not a usable revisioned typed attempt", ErrAttemptRepairTarget, leaseUUID,
		)
	}
	if placement.Attempt != backendName || placement.attemptOperationID != operationID {
		return AttemptRepairCandidate{}, fmt.Errorf(
			"%w: supplied backend or operation ID is stale or mismatched", ErrAttemptRepairTarget,
		)
	}
	if placement.Backend != "" && placement.Backend != placement.Attempt {
		return AttemptRepairCandidate{}, fmt.Errorf(
			"%w: confirmed owner %q conflicts with attempted backend %q",
			ErrAttemptRepairTarget, placement.Backend, placement.Attempt,
		)
	}
	if !placement.attemptRequestSnapshot.Valid() ||
		placement.attemptRequestSnapshot.ProviderUUID() != store.providerUUID {
		return AttemptRepairCandidate{}, fmt.Errorf(
			"%w: durable attempt provider %q does not match store provider %q",
			ErrAttemptRepairTarget,
			placement.attemptRequestSnapshot.ProviderUUID(), store.providerUUID,
		)
	}

	capability, capabilityExists := store.lifecycleCache[leaseUUID]
	wantLifecycleID, lifecycleErr := lifecycleIDForOperation(operationID)
	if lifecycleErr != nil || !capabilityExists || capability.unusable || capability.rawCorrupt ||
		capability.attemptBackend != backendName || capability.attemptID != wantLifecycleID ||
		(placement.Backend != "" && capability.backend != placement.Backend) {
		return AttemptRepairCandidate{}, fmt.Errorf(
			"%w: lifecycle capability does not exactly match the typed attempt",
			ErrAttemptRepairTarget,
		)
	}
	if placement.Backend != "" && capability.id.Valid() && capability.id == wantLifecycleID {
		return AttemptRepairCandidate{}, fmt.Errorf(
			"%w: attempted generation is indistinguishable from the preserved prior generation",
			ErrAttemptRepairTarget,
		)
	}

	placementEncoded, err := encodePlacement(placement)
	if err != nil {
		return AttemptRepairCandidate{}, fmt.Errorf("encode current placement for repair check: %w", err)
	}
	capabilityEncoded, err := encodeLifecycleCapability(capability)
	if err != nil {
		return AttemptRepairCandidate{}, fmt.Errorf("encode current lifecycle capability for repair check: %w", err)
	}
	if err := store.db.View(func(tx *bolt.Tx) error {
		placements := tx.Bucket(bucketName)
		capabilities := tx.Bucket(lifecycleCapabilityBucketName)
		if placements == nil || capabilities == nil {
			return errors.New("placement lifecycle buckets are missing")
		}
		if raw := placements.Get([]byte(leaseUUID)); !bytes.Equal(raw, placementEncoded) {
			return errors.New("placement row is not the canonical current encoding")
		}
		if raw := capabilities.Get([]byte(leaseUUID)); !bytes.Equal(raw, capabilityEncoded) {
			return errors.New("lifecycle row is not the canonical current encoding")
		}
		return nil
	}); err != nil {
		return AttemptRepairCandidate{}, fmt.Errorf("%w: %w", ErrAttemptRepairTarget, err)
	}

	lifecycleAfter := clearAttemptLifecycle(capability, backendName, operationID)
	retainLifecycle := true
	if placement.Backend == "" {
		lifecycleAfter, retainLifecycle = lifecycleAfterPlacementDelete(capability, placement)
	}

	return AttemptRepairCandidate{
		issuer:           repair,
		leaseUUID:        leaseUUID,
		backendName:      backendName,
		operationID:      operationID,
		providerUUID:     store.providerUUID,
		tenant:           placement.attemptRequestSnapshot.Tenant(),
		revision:         placement.revision,
		topologyID:       store.topologyID,
		confirmedOwner:   placement.Backend,
		lifecycleAfter:   lifecycleAfter,
		retainLifecycle:  retainLifecycle,
		placementEncoded: slices.Clone(placementEncoded),
		lifecycleEncoded: slices.Clone(capabilityEncoded),
	}, nil
}

// LeaseUUID returns the exact target lease.
func (candidate AttemptRepairCandidate) LeaseUUID() string { return candidate.leaseUUID }

// Backend returns the exact attempted backend.
func (candidate AttemptRepairCandidate) Backend() string { return candidate.backendName }

// OperationID returns the exact attempted operation identity.
func (candidate AttemptRepairCandidate) OperationID() operation.OperationID {
	return candidate.operationID
}

// ConfirmedOwner returns the owner that Refuse must preserve, or empty when
// the target is an attempt-only row.
func (candidate AttemptRepairCandidate) ConfirmedOwner() string { return candidate.confirmedOwner }

// MatchesPreservedProvision reports whether one explicit active-provision
// lifecycle observation is exact evidence of the confirmed generation that
// predated this attempt. It deliberately exposes only a verdict: callers
// cannot extract or manufacture the candidate's prior lifecycle capability.
// Attempt-only candidates and unknown, unusable, retired, missing, mismatched,
// or attempted-generation observations never match.
func (candidate AttemptRepairCandidate) MatchesPreservedProvision(
	backendName string,
	observation LifecycleObservation,
) bool {
	if candidate.issuer == nil || candidate.confirmedOwner == "" ||
		backendName != candidate.confirmedOwner || !candidate.retainLifecycle ||
		candidate.lifecycleAfter.unusable || candidate.lifecycleAfter.rawCorrupt ||
		candidate.lifecycleAfter.retired ||
		candidate.lifecycleAfter.backend != candidate.confirmedOwner ||
		candidate.lifecycleAfter.attemptBackend != "" || candidate.lifecycleAfter.attemptID.Valid() ||
		validateLifecycleObservation(observation) != nil {
		return false
	}

	switch observation.Kind {
	case LifecycleObservationLegacy:
		return !candidate.lifecycleAfter.id.Valid()
	case LifecycleObservationTyped:
		attemptedID, err := lifecycleIDForOperation(candidate.operationID)
		return err == nil && observation.ID != attemptedID &&
			candidate.lifecycleAfter.id.Valid() && observation.ID == candidate.lifecycleAfter.id
	default:
		return false
	}
}

// ConfirmationValue is the deliberately exact operator confirmation string
// for this lease/backend/operation tuple.
func (candidate AttemptRepairCandidate) ConfirmationValue() string {
	if candidate.issuer == nil || candidate.leaseUUID == "" || candidate.backendName == "" ||
		!candidate.operationID.Valid() {
		return ""
	}
	return fmt.Sprintf("refuse-attempt:%s:%s:%s",
		candidate.leaseUUID, candidate.backendName, candidate.operationID.String())
}

// VerifyAttemptRepairEvidenceContext mints the inventory half of
// attempt-refusal authority only for an exact current candidate and complete
// identity-bound topology facts. Evidence is inseparable from the deadline and
// cancellation scope that bounded collection.
func (repair *AttemptRepair) VerifyAttemptRepairEvidenceContext(
	ctx context.Context,
	candidate AttemptRepairCandidate,
	inventory RepairInventorySnapshot,
) (AttemptRepairEvidence, error) {
	notAfter, err := bindRepairEvidenceContext(ctx)
	if err != nil {
		return AttemptRepairEvidence{}, fmt.Errorf("%w: %w", ErrAttemptRepairTarget, err)
	}
	if repair == nil || repair.store == nil || candidate.issuer != repair {
		return AttemptRepairEvidence{}, fmt.Errorf(
			"%w: candidate belongs to another repair session", ErrAttemptRepairTarget,
		)
	}
	if err := repair.validateInventorySnapshot(inventory); err != nil {
		return AttemptRepairEvidence{}, err
	}
	current, err := repair.matchAttempt(
		candidate.leaseUUID, candidate.backendName, candidate.operationID,
	)
	if err != nil {
		return AttemptRepairEvidence{}, err
	}
	if !sameAttemptRepairCandidate(current, candidate) {
		return AttemptRepairEvidence{}, fmt.Errorf("%w: candidate revision is stale", ErrAttemptRepairTarget)
	}
	if err := validateAttemptRepairInventory(candidate, inventory); err != nil {
		return AttemptRepairEvidence{}, err
	}
	return AttemptRepairEvidence{
		issuer:       repair,
		confirmation: candidate.ConfirmationValue(),
		topologyID:   candidate.topologyID,
		digest:       inventory.digest,
		context:      ctx,
		notAfter:     notAfter,
	}, nil
}

func validateAttemptRepairInventory(
	candidate AttemptRepairCandidate,
	inventory RepairInventorySnapshot,
) error {
	var present []string
	for _, backendName := range inventory.backends {
		observed := inventory.inventories[backendName]
		for _, provision := range observed.Provisions {
			if provision.LeaseUUID != candidate.leaseUUID {
				continue
			}
			if provision.ProviderUUID != candidate.providerUUID {
				return fmt.Errorf(
					"%w: backend %q target provision provider %q does not match durable provider %q",
					ErrRepairLeasePresent, backendName, provision.ProviderUUID, candidate.providerUUID,
				)
			}
			if candidate.tenant != "" && provision.Tenant != candidate.tenant {
				return fmt.Errorf(
					"%w: backend %q target provision tenant %q does not match durable attempt tenant %q",
					ErrRepairLeasePresent, backendName, provision.Tenant, candidate.tenant,
				)
			}
			if !candidate.MatchesPreservedProvision(
				backendName, repairLifecycleObservation(provision.LifecycleGeneration),
			) {
				present = append(present, backendName+"/provisions")
			}
		}
		for _, retention := range observed.Retentions {
			if retention.LeaseUUID == candidate.leaseUUID {
				present = append(present, backendName+"/retentions")
			}
		}
	}
	if len(present) != 0 {
		slices.Sort(present)
		return fmt.Errorf(
			"%w: lease %q reported by %v", ErrRepairLeasePresent, candidate.leaseUUID, present,
		)
	}
	return nil
}

// AttestDrain converts the exact operator statement into a separate
// plan-or-attempt-bound capability. Mutation requires this and store-minted
// live authority; neither capability can substitute for the other.
func (repair *AttemptRepair) AttestDrain(
	confirmation, statement string,
) (DrainAttestation, error) {
	if repair == nil || repair.store == nil {
		return DrainAttestation{}, errors.New("placement repair is not open")
	}
	if confirmation == "" {
		return DrainAttestation{}, errors.New("repair candidate confirmation is empty")
	}
	if statement != DrainAttestationText {
		return DrainAttestation{}, fmt.Errorf("drain attestation must exactly equal %q", DrainAttestationText)
	}
	return DrainAttestation{issuer: repair, confirmation: confirmation}, nil
}

func bindRepairEvidenceContext(ctx context.Context) (time.Time, error) {
	if ctx == nil || ctx.Done() == nil {
		return time.Time{}, errors.New("repair evidence requires a cancellable context with a deadline")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Time{}, errors.New("repair evidence context has no deadline")
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, fmt.Errorf("repair evidence context is already done: %w", err)
	}
	if !time.Now().Before(deadline) {
		return time.Time{}, errors.New("repair evidence deadline has expired")
	}
	return deadline, nil
}

func validateBoundRepairContext(
	ctx context.Context,
	bound context.Context,
	notAfter time.Time,
) error {
	if ctx == nil || bound == nil || ctx.Done() == nil || bound.Done() == nil ||
		ctx.Done() != bound.Done() || notAfter.IsZero() {
		return errors.New("repair evidence belongs to another cancellation scope")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("repair evidence context is done: %w", err)
	}
	if !time.Now().Before(notAfter) {
		return errors.New("repair inventory evidence expired")
	}
	return nil
}

func (repair *AttemptRepair) validateInventorySnapshot(inventory RepairInventorySnapshot) error {
	if repair == nil || repair.store == nil {
		return errors.New("placement repair is not open")
	}
	repair.store.mu.RLock()
	defer repair.store.mu.RUnlock()
	return repair.validateInventorySnapshotLocked(inventory)
}

// Caller holds at least repair.store.mu.RLock.
func (repair *AttemptRepair) validateInventorySnapshotLocked(inventory RepairInventorySnapshot) error {
	if inventory.digest == ([sha256.Size]byte{}) || len(inventory.inventories) == 0 {
		return errors.New("complete inventory digest is required")
	}
	recomputed, err := repairInventoryDigest(inventory.backends, inventory.inventories)
	if err != nil || recomputed != inventory.digest {
		return errors.New("complete inventory digest does not match its observations")
	}
	store := repair.store
	if !slices.Equal(inventory.backends, store.backendTopology) ||
		len(inventory.inventories) != len(store.backendTopology) {
		return fmt.Errorf("complete inventory identities do not match active topology")
	}
	for _, backendName := range store.backendTopology {
		observedInventory, present := inventory.inventories[backendName]
		observed := observedInventory.StorageIdentity
		expected, bound := store.backendStorageIDs[backendName]
		if !present || !observed.Valid() || !bound || !expected.Valid() || observed != expected {
			return fmt.Errorf(
				"backend %q inventory identity does not match its durable active pin", backendName,
			)
		}
	}
	for backendName := range inventory.inventories {
		if _, active := store.backendTopologySet[backendName]; !active {
			return fmt.Errorf("inventory identity supplied for inactive backend %q", backendName)
		}
	}
	return nil
}

func sameAttemptRepairCandidate(left, right AttemptRepairCandidate) bool {
	return left.issuer == right.issuer && left.leaseUUID == right.leaseUUID &&
		left.backendName == right.backendName && left.operationID == right.operationID &&
		left.providerUUID == right.providerUUID && left.tenant == right.tenant &&
		left.revision == right.revision && left.topologyID == right.topologyID &&
		left.confirmedOwner == right.confirmedOwner &&
		left.lifecycleAfter == right.lifecycleAfter && left.retainLifecycle == right.retainLifecycle &&
		bytes.Equal(left.placementEncoded, right.placementEncoded) &&
		bytes.Equal(left.lifecycleEncoded, right.lifecycleEncoded)
}

func sameConflictRepairCandidate(left, right ConflictRepairCandidate) bool {
	return left.issuer == right.issuer && left.leaseUUID == right.leaseUUID &&
		left.selectedBackend == right.selectedBackend && left.revision == right.revision &&
		left.providerUUID == right.providerUUID && left.expectedTenant == right.expectedTenant &&
		left.topologyID == right.topologyID &&
		slices.Equal(left.candidateBackends, right.candidateBackends) &&
		bytes.Equal(left.placementEncoded, right.placementEncoded)
}

// RefuseContext revalidates the exact candidate and its expiring,
// context-bound inventory evidence immediately before mutation and again from
// inside the bbolt transaction. A stale, foreign, expired, canceled, or
// superseded capability cannot mutate a newer attempt.
func (repair *AttemptRepair) RefuseContext(
	ctx context.Context,
	candidate AttemptRepairCandidate,
	evidence AttemptRepairEvidence,
	drain DrainAttestation,
	finalProbe AttemptRepairProbe,
) (AttemptRepairResult, error) {
	if repair == nil || repair.store == nil || candidate.issuer != repair {
		return AttemptRepairResult{}, fmt.Errorf("%w: candidate belongs to another repair session", ErrAttemptRepairTarget)
	}
	confirmation := candidate.ConfirmationValue()
	if evidence.issuer != repair || evidence.confirmation != confirmation ||
		evidence.topologyID != candidate.topologyID || evidence.digest == ([sha256.Size]byte{}) {
		return AttemptRepairResult{}, fmt.Errorf(
			"%w: complete inventory evidence is absent, stale, or belongs to another candidate",
			ErrAttemptRepairTarget,
		)
	}
	if err := validateBoundRepairContext(ctx, evidence.context, evidence.notAfter); err != nil {
		return AttemptRepairResult{}, fmt.Errorf("%w: %w", ErrAttemptRepairTarget, err)
	}
	if drain.issuer != repair || drain.confirmation != confirmation {
		return AttemptRepairResult{}, fmt.Errorf(
			"%w: drain attestation is absent or belongs to another candidate",
			ErrAttemptRepairTarget,
		)
	}
	if finalProbe == nil {
		return AttemptRepairResult{}, fmt.Errorf(
			"%w: final live inventory probe is required", ErrAttemptRepairTarget,
		)
	}
	current, err := repair.matchAttempt(
		candidate.leaseUUID, candidate.backendName, candidate.operationID,
	)
	if err != nil {
		return AttemptRepairResult{}, err
	}
	if !sameAttemptRepairCandidate(current, candidate) {
		return AttemptRepairResult{}, fmt.Errorf("%w: candidate revision is stale", ErrAttemptRepairTarget)
	}
	if err := verifyBoltPhysicalConsistency(repair.store.db); err != nil {
		return AttemptRepairResult{}, fmt.Errorf(
			"validate placement db before attempt repair mutation: %w", err,
		)
	}
	if err := validateBoundRepairContext(ctx, evidence.context, evidence.notAfter); err != nil {
		return AttemptRepairResult{}, fmt.Errorf("%w: %w", ErrAttemptRepairTarget, err)
	}

	repair.store.mu.Lock()
	finalInventory, probeErr := finalProbe(ctx)
	if probeErr != nil {
		repair.store.mu.Unlock()
		return AttemptRepairResult{}, fmt.Errorf("final attempt repair inventory probe: %w", probeErr)
	}
	if inventoryErr := repair.validateInventorySnapshotLocked(finalInventory); inventoryErr != nil {
		repair.store.mu.Unlock()
		return AttemptRepairResult{}, inventoryErr
	}
	if inventoryErr := validateAttemptRepairInventory(candidate, finalInventory); inventoryErr != nil {
		repair.store.mu.Unlock()
		return AttemptRepairResult{}, inventoryErr
	}
	if finalInventory.digest != evidence.digest {
		repair.store.mu.Unlock()
		return AttemptRepairResult{}, fmt.Errorf(
			"%w: final live inventory no longer matches the initial evidence digest",
			ErrAttemptRepairTarget,
		)
	}
	if contextErr := validateBoundRepairContext(ctx, evidence.context, evidence.notAfter); contextErr != nil {
		repair.store.mu.Unlock()
		return AttemptRepairResult{}, fmt.Errorf("%w: %w", ErrAttemptRepairTarget, contextErr)
	}
	if pathErr := repair.verifySourcePath(); pathErr != nil {
		repair.store.mu.Unlock()
		return AttemptRepairResult{}, fmt.Errorf(
			"validate placement db path immediately before attempt repair mutation: %w",
			pathErr,
		)
	}
	if backupErr := repair.verifyPublishedBackupTarget(); backupErr != nil {
		repair.store.mu.Unlock()
		return AttemptRepairResult{}, fmt.Errorf(
			"validate exact backup authority immediately before attempt repair mutation: %w",
			backupErr,
		)
	}
	applied, err := repair.refuseAttemptContextLocked(ctx, candidate, evidence)
	if err != nil {
		repair.store.mu.Unlock()
		if classified := classifyRepairTransactionError(
			"refuse exact placement operation", err,
		); errors.Is(classified, ErrRepairMutationOutcomeUnknown) {
			return AttemptRepairResult{}, classified
		}
		return AttemptRepairResult{}, fmt.Errorf("refuse exact placement operation: %w", err)
	}
	if !applied {
		repair.store.mu.Unlock()
		return AttemptRepairResult{}, fmt.Errorf("%w: exact attempt changed before refusal", ErrAttemptRepairTarget)
	}
	pathErr := repair.verifySourcePathAfterMutation("refuse exact placement operation")
	repair.store.mu.Unlock()
	if pathErr != nil {
		return AttemptRepairResult{}, pathErr
	}
	if err := repair.verifyPublishedBackupTargetAfterMutation("refuse exact placement operation"); err != nil {
		return AttemptRepairResult{}, err
	}
	if err := repair.verifyRefused(candidate); err != nil {
		return AttemptRepairResult{}, fmt.Errorf("%w: %w", ErrRepairMutationCommitted, err)
	}
	if err := repair.verifySourcePathAfterMutation("verify refused placement operation"); err != nil {
		return AttemptRepairResult{}, err
	}
	return AttemptRepairResult{ConfirmedOwner: candidate.confirmedOwner}, nil
}

// Caller holds repair.store.mu.
func (repair *AttemptRepair) refuseAttemptContextLocked(
	ctx context.Context,
	candidate AttemptRepairCandidate,
	evidence AttemptRepairEvidence,
) (bool, error) {
	if err := validateBoundRepairContext(ctx, evidence.context, evidence.notAfter); err != nil {
		return false, err
	}
	store := repair.store
	if store.attemptClaimedLocked(candidate.leaseUUID) {
		return false, fmt.Errorf("%w: lease %q", ErrAttemptClaimed, candidate.leaseUUID)
	}
	p, exists := store.cache[candidate.leaseUUID]
	if !exists || p.revision != candidate.revision || p.Attempt != candidate.backendName ||
		p.attemptOperationID != candidate.operationID || p.Backend != candidate.confirmedOwner {
		return false, nil
	}
	capability, capabilityExists := store.lifecycleCache[candidate.leaseUUID]
	placementEncoded, err := encodePlacement(p)
	if err != nil {
		return false, fmt.Errorf("encode placement before repair refusal: %w", err)
	}
	if !bytes.Equal(placementEncoded, candidate.placementEncoded) {
		return false, nil
	}
	capabilityEncoded, err := encodeLifecycleCapability(capability)
	if err != nil {
		return false, fmt.Errorf("encode lifecycle capability before repair refusal: %w", err)
	}
	if !capabilityExists || !bytes.Equal(capabilityEncoded, candidate.lifecycleEncoded) {
		return false, nil
	}
	next, err := store.nextRevision()
	if err != nil {
		return false, err
	}

	operationName := "refuse exact placement operation"
	if candidate.confirmedOwner == "" {
		var nextLifecycleEncoded []byte
		if candidate.retainLifecycle {
			nextLifecycleEncoded, err = encodeLifecycleCapability(candidate.lifecycleAfter)
			if err != nil {
				return false, mutationFailure("encode lifecycle capability for "+operationName, err)
			}
		}
		if err := updateBoltWithExplicitOutcome(store.db, func(tx *bolt.Tx) error {
			if err := validateBoundRepairContext(ctx, evidence.context, evidence.notAfter); err != nil {
				return err
			}
			placements := tx.Bucket(bucketName)
			capabilities := tx.Bucket(lifecycleCapabilityBucketName)
			if placements == nil || capabilities == nil {
				return errors.New("placement lifecycle buckets missing")
			}
			if !bytes.Equal(placements.Get([]byte(candidate.leaseUUID)), candidate.placementEncoded) ||
				!bytes.Equal(capabilities.Get([]byte(candidate.leaseUUID)), candidate.lifecycleEncoded) {
				return ErrAttemptRepairTarget
			}
			if err := placements.Delete([]byte(candidate.leaseUUID)); err != nil {
				return err
			}
			if err := validateBoundRepairContext(ctx, evidence.context, evidence.notAfter); err != nil {
				return err
			}
			if candidate.retainLifecycle {
				if err := capabilities.Put([]byte(candidate.leaseUUID), nextLifecycleEncoded); err != nil {
					return err
				}
			} else if err := capabilities.Delete([]byte(candidate.leaseUUID)); err != nil {
				return err
			}
			return validateBoundRepairContext(ctx, evidence.context, evidence.notAfter)
		}); err != nil {
			return false, mutationFailure(operationName, err)
		}
		delete(store.cache, candidate.leaseUUID)
		if len(store.activeSnapshots) > 0 {
			store.deleteRevisions[candidate.leaseUUID] = next
		}
		if candidate.retainLifecycle {
			store.lifecycleCache[candidate.leaseUUID] = candidate.lifecycleAfter
		} else {
			delete(store.lifecycleCache, candidate.leaseUUID)
		}
		store.revision = next
		return true, nil
	}

	p.Attempt = ""
	clearOperationMetadata(&p)
	p.revision = next
	nextPlacementEncoded, err := encodePlacement(p)
	if err != nil {
		return false, mutationFailure("encode placement for "+operationName, err)
	}
	nextLifecycleEncoded, err := encodeLifecycleCapability(candidate.lifecycleAfter)
	if err != nil {
		return false, mutationFailure("encode lifecycle capability for "+operationName, err)
	}
	if err := updateBoltWithExplicitOutcome(store.db, func(tx *bolt.Tx) error {
		if err := validateBoundRepairContext(ctx, evidence.context, evidence.notAfter); err != nil {
			return err
		}
		placements := tx.Bucket(bucketName)
		capabilities := tx.Bucket(lifecycleCapabilityBucketName)
		if placements == nil || capabilities == nil {
			return errors.New("placement lifecycle buckets missing")
		}
		if !bytes.Equal(placements.Get([]byte(candidate.leaseUUID)), candidate.placementEncoded) ||
			!bytes.Equal(capabilities.Get([]byte(candidate.leaseUUID)), candidate.lifecycleEncoded) {
			return ErrAttemptRepairTarget
		}
		if err := placements.Put([]byte(candidate.leaseUUID), nextPlacementEncoded); err != nil {
			return err
		}
		if err := validateBoundRepairContext(ctx, evidence.context, evidence.notAfter); err != nil {
			return err
		}
		if err := capabilities.Put([]byte(candidate.leaseUUID), nextLifecycleEncoded); err != nil {
			return err
		}
		return validateBoundRepairContext(ctx, evidence.context, evidence.notAfter)
	}); err != nil {
		return false, mutationFailure(operationName, err)
	}
	store.cache[candidate.leaseUUID] = p
	store.lifecycleCache[candidate.leaseUUID] = candidate.lifecycleAfter
	delete(store.deleteRevisions, candidate.leaseUUID)
	store.revision = next
	return true, nil
}

// ResolveConflictContext replaces one exact durable conflict with the sole
// positively observed owner. It holds mutation admission while invoking the
// mandatory final probe, rematches every plan-bound fact, checks cancellation
// and expiry again inside bbolt, then clears stale attempt/conflict evidence.
func (repair *AttemptRepair) ResolveConflictContext(
	ctx context.Context,
	plan ConflictRepairPlan,
	drain DrainAttestation,
	finalProbe ConflictRepairProbe,
) (ConflictRepairResult, error) {
	candidate := plan.candidate
	if repair == nil || repair.store == nil || plan.issuer != repair || candidate.issuer != repair {
		return ConflictRepairResult{}, fmt.Errorf(
			"%w: plan belongs to another repair session", ErrConflictRepairTarget,
		)
	}
	confirmation, err := conflictRepairPlanConfirmation(plan)
	if err != nil || confirmation != plan.confirmation {
		return ConflictRepairResult{}, fmt.Errorf(
			"%w: conflict repair plan is incomplete or has been altered",
			ErrConflictRepairTarget,
		)
	}
	if err := validateBoundRepairContext(ctx, plan.context, plan.notAfter); err != nil {
		return ConflictRepairResult{}, fmt.Errorf("%w: %w", ErrConflictRepairTarget, err)
	}
	if drain.issuer != repair || drain.confirmation != plan.confirmation {
		return ConflictRepairResult{}, fmt.Errorf(
			"%w: drain attestation is absent or belongs to another final plan",
			ErrConflictRepairTarget,
		)
	}
	if finalProbe == nil {
		return ConflictRepairResult{}, fmt.Errorf(
			"%w: final live inventory probe is required", ErrConflictRepairTarget,
		)
	}
	if !plan.retained {
		if err := validateLifecycleObservation(plan.lifecycle); err != nil {
			return ConflictRepairResult{}, fmt.Errorf("conflict owner evidence: %w", err)
		}
	}
	if err := verifyBoltPhysicalConsistency(repair.store.db); err != nil {
		return ConflictRepairResult{}, fmt.Errorf(
			"validate placement db before conflict repair mutation: %w", err,
		)
	}

	store := repair.store
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := validateBoundRepairContext(ctx, plan.context, plan.notAfter); err != nil {
		return ConflictRepairResult{}, fmt.Errorf("%w: %w", ErrConflictRepairTarget, err)
	}
	p, exists := store.cache[candidate.leaseUUID]
	if !exists || p.revision != candidate.revision || store.topologyID != candidate.topologyID ||
		p.unusable || !p.Conflict || p.ConflictOwnersUnknown ||
		!slices.Equal(p.ConflictBackends, candidate.candidateBackends) {
		return ConflictRepairResult{}, fmt.Errorf(
			"%w: conflict changed before resolution", ErrConflictRepairTarget,
		)
	}
	encoded, err := encodePlacement(p)
	if err != nil || !bytes.Equal(encoded, candidate.placementEncoded) {
		return ConflictRepairResult{}, fmt.Errorf(
			"%w: conflict encoding changed before resolution", ErrConflictRepairTarget,
		)
	}
	if err := store.db.View(func(tx *bolt.Tx) error {
		placements := tx.Bucket(bucketName)
		if placements == nil ||
			!bytes.Equal(placements.Get([]byte(candidate.leaseUUID)), candidate.placementEncoded) {
			return errors.New("durable conflict changed before resolution")
		}
		return nil
	}); err != nil {
		return ConflictRepairResult{}, fmt.Errorf("%w: %w", ErrConflictRepairTarget, err)
	}

	// Mutation admission remains held across this collection and rematch. This
	// closes the evidence-to-write gap: the exact same complete inventory digest
	// the operator confirmed must still be observable immediately before bbolt.
	finalInventory, err := finalProbe(ctx)
	if err != nil {
		return ConflictRepairResult{}, fmt.Errorf("final conflict repair inventory probe: %w", err)
	}
	if err := repair.validateInventorySnapshotLocked(finalInventory); err != nil {
		return ConflictRepairResult{}, err
	}
	finalOwner, err := repair.matchConflictInventory(candidate, finalInventory)
	if err != nil {
		return ConflictRepairResult{}, err
	}
	finalPlan := ConflictRepairPlan{
		issuer:                repair,
		candidate:             candidate,
		context:               plan.context,
		notAfter:              plan.notAfter,
		digest:                finalInventory.digest,
		selectedStorageID:     finalInventory.inventories[candidate.selectedBackend].StorageIdentity,
		providerUUID:          candidate.providerUUID,
		tenant:                finalOwner.tenant,
		identityAuthoritative: finalOwner.identityAuthoritative,
		retained:              finalOwner.retained,
		lifecycle:             finalOwner.lifecycle,
	}
	finalPlan.confirmation, err = conflictRepairPlanConfirmation(finalPlan)
	if err != nil || !sameConflictRepairPlan(plan, finalPlan) {
		return ConflictRepairResult{}, fmt.Errorf(
			"%w: final live inventory no longer matches the operator-confirmed plan",
			ErrConflictRepairTarget,
		)
	}
	if err := validateBoundRepairContext(ctx, plan.context, plan.notAfter); err != nil {
		return ConflictRepairResult{}, fmt.Errorf("%w: %w", ErrConflictRepairTarget, err)
	}

	next, err := store.nextRevision()
	if err != nil {
		return ConflictRepairResult{}, err
	}
	resolved := Placement{
		Backend:  candidate.selectedBackend,
		SetAt:    p.SetAt,
		revision: next,
	}
	capability := lifecycleCapability{
		backend:  candidate.selectedBackend,
		unusable: true,
	}
	retained := plan.retained
	if !plan.retained && plan.identityAuthoritative {
		switch plan.lifecycle.Kind {
		case LifecycleObservationLegacy:
			capability = lifecycleCapability{backend: candidate.selectedBackend}
		case LifecycleObservationTyped:
			capability = lifecycleCapability{
				backend: candidate.selectedBackend,
				id:      plan.lifecycle.ID,
			}
		case LifecycleObservationUnknown, LifecycleObservationUnusable:
			// Routing is repaired but runtime callback authority remains quarantined.
		default:
			return ConflictRepairResult{}, errors.New("conflict owner lifecycle evidence is invalid")
		}
	}
	resolvedEncoded, err := encodePlacement(resolved)
	if err != nil {
		return ConflictRepairResult{}, mutationFailure(
			"encode placement for resolve exact placement conflict", err,
		)
	}
	capabilityEncoded, err := encodeLifecycleCapability(capability)
	if err != nil {
		return ConflictRepairResult{}, mutationFailure(
			"encode lifecycle capability for resolve exact placement conflict", err,
		)
	}
	if err := repair.verifySourcePath(); err != nil {
		return ConflictRepairResult{}, fmt.Errorf(
			"validate placement db path immediately before conflict repair mutation: %w",
			err,
		)
	}
	if err := repair.verifyPublishedBackupTarget(); err != nil {
		return ConflictRepairResult{}, fmt.Errorf(
			"validate exact backup authority immediately before conflict repair mutation: %w",
			err,
		)
	}
	if err := updateBoltWithExplicitOutcome(store.db, func(tx *bolt.Tx) error {
		if err := validateBoundRepairContext(ctx, plan.context, plan.notAfter); err != nil {
			return err
		}
		placements := tx.Bucket(bucketName)
		capabilities := tx.Bucket(lifecycleCapabilityBucketName)
		if placements == nil || capabilities == nil {
			return errors.New("placement lifecycle buckets missing")
		}
		if !bytes.Equal(placements.Get([]byte(candidate.leaseUUID)), candidate.placementEncoded) {
			return ErrConflictRepairTarget
		}
		if err := placements.Put([]byte(candidate.leaseUUID), resolvedEncoded); err != nil {
			return err
		}
		if err := validateBoundRepairContext(ctx, plan.context, plan.notAfter); err != nil {
			return err
		}
		if err := capabilities.Put([]byte(candidate.leaseUUID), capabilityEncoded); err != nil {
			return err
		}
		return validateBoundRepairContext(ctx, plan.context, plan.notAfter)
	}); err != nil {
		err = mutationFailure("resolve exact placement conflict", err)
		if classified := classifyRepairTransactionError(
			"resolve exact placement conflict", err,
		); errors.Is(classified, ErrRepairMutationOutcomeUnknown) {
			return ConflictRepairResult{}, classified
		}
		return ConflictRepairResult{}, err
	}
	store.cache[candidate.leaseUUID] = resolved
	store.lifecycleCache[candidate.leaseUUID] = capability
	delete(store.deleteRevisions, candidate.leaseUUID)
	store.revision = next
	if err := repair.verifySourcePathAfterMutation("resolve exact placement conflict"); err != nil {
		return ConflictRepairResult{}, err
	}
	if err := repair.verifyPublishedBackupTargetAfterMutation("resolve exact placement conflict"); err != nil {
		return ConflictRepairResult{}, err
	}
	if err := verifyResolvedConflictLocked(
		store, candidate.leaseUUID, resolved, capability,
	); err != nil {
		return ConflictRepairResult{}, fmt.Errorf("%w: %w", ErrRepairMutationCommitted, err)
	}
	if err := repair.verifySourcePathAfterMutation("verify resolved placement conflict"); err != nil {
		return ConflictRepairResult{}, err
	}
	return ConflictRepairResult{
		ConfirmedOwner: candidate.selectedBackend,
		Retained:       retained,
		expected:       resolved,
		lifecycle:      capability,
	}, nil
}

func sameConflictRepairPlan(left, right ConflictRepairPlan) bool {
	return left.issuer == right.issuer &&
		sameConflictRepairCandidate(left.candidate, right.candidate) &&
		left.context != nil && right.context != nil &&
		left.context.Done() == right.context.Done() && left.notAfter.Equal(right.notAfter) &&
		left.digest == right.digest && left.selectedStorageID == right.selectedStorageID &&
		left.providerUUID == right.providerUUID && left.tenant == right.tenant &&
		left.identityAuthoritative == right.identityAuthoritative &&
		left.retained == right.retained && left.lifecycle == right.lifecycle &&
		left.confirmation == right.confirmation
}

func classifyRepairTransactionError(operation string, err error) error {
	if errors.Is(err, errBoltCommitOutcomeUnknown) {
		return fmt.Errorf("%w: %s: %w", ErrRepairMutationOutcomeUnknown, operation, err)
	}
	return err
}

// VerifyRefusalPostcondition rechecks one successful repair result through a
// newly opened read-only inspector. The candidate is opaque and session-minted,
// so callers cannot manufacture a weaker postcondition for another lease.
func (inspector *RepairInspector) VerifyRefusalPostcondition(
	candidate AttemptRepairCandidate,
	result AttemptRepairResult,
) error {
	if inspector == nil || inspector.store == nil || candidate.issuer == nil {
		return errors.New("placement repair refusal postcondition is invalid")
	}
	if err := inspector.verifyOriginalRepairSource(candidate.issuer); err != nil {
		return fmt.Errorf("placement repair refusal postcondition source: %w", err)
	}
	if result.ConfirmedOwner != candidate.confirmedOwner {
		return errors.New("placement repair refusal result differs from candidate")
	}
	if err := verifyRefusedStore(inspector.store, candidate); err != nil {
		return err
	}
	return inspector.verifyOriginalRepairSource(candidate.issuer)
}

// VerifyConflictResolutionPostcondition rechecks the exact placement and
// lifecycle authority returned by ResolveConflict through a newly opened
// read-only inspector.
func (inspector *RepairInspector) VerifyConflictResolutionPostcondition(
	candidate ConflictRepairCandidate,
	result ConflictRepairResult,
) error {
	if inspector == nil || inspector.store == nil || candidate.issuer == nil ||
		result.ConfirmedOwner != candidate.selectedBackend ||
		result.expected.revision == 0 || result.expected.Backend != result.ConfirmedOwner {
		return errors.New("placement conflict repair postcondition is invalid")
	}
	if err := inspector.verifyOriginalRepairSource(candidate.issuer); err != nil {
		return fmt.Errorf("placement conflict repair postcondition source: %w", err)
	}
	store := inspector.store
	store.mu.RLock()
	err := verifyResolvedConflictLocked(
		store, candidate.leaseUUID, result.expected, result.lifecycle,
	)
	store.mu.RUnlock()
	if err != nil {
		return err
	}
	return inspector.verifyOriginalRepairSource(candidate.issuer)
}

func (inspector *RepairInspector) verifyOriginalRepairSource(issuer *AttemptRepair) error {
	if inspector == nil || inspector.store == nil || inspector.store.db == nil ||
		inspector.sourceInfo == nil || issuer == nil || issuer.sourceInfo == nil {
		return errors.New("original repair source identity is unavailable")
	}
	if !os.SameFile(inspector.sourceInfo, issuer.sourceInfo) {
		return errors.New("reopened placement db is not the inode mutated by the repair session")
	}
	if inspector.authority != nil {
		if err := inspector.authority.verify(); err != nil {
			return fmt.Errorf("reopened placement db authority changed: %w", err)
		}
		return nil
	}
	currentInfo, err := os.Stat(inspector.store.db.Path())
	if err != nil {
		return fmt.Errorf("stat reopened placement db: %w", err)
	}
	if !currentInfo.Mode().IsRegular() || !os.SameFile(inspector.sourceInfo, currentInfo) {
		return errors.New("reopened placement db pathname no longer identifies the inspected inode")
	}
	return nil
}

// Caller holds store.mu.
func verifyResolvedConflictLocked(
	store *Store,
	leaseUUID string,
	wantPlacement Placement,
	wantCapability lifecycleCapability,
) error {
	got, exists := store.cache[leaseUUID]
	if !exists || !equalPlacementIgnoringRevision(got, wantPlacement) ||
		got.revision != wantPlacement.revision || got.State() != StateConfirmed ||
		got.Attempt != "" || got.Conflict {
		return errors.New("verify conflict repair: confirmed placement differs from resolved owner")
	}
	gotCapability, exists := store.lifecycleCache[leaseUUID]
	if !exists || gotCapability != wantCapability {
		return errors.New("verify conflict repair: lifecycle evidence differs from resolved owner")
	}
	placementEncoded, err := encodePlacement(wantPlacement)
	if err != nil {
		return fmt.Errorf("verify conflict repair: encode placement: %w", err)
	}
	capabilityEncoded, err := encodeLifecycleCapability(wantCapability)
	if err != nil {
		return fmt.Errorf("verify conflict repair: encode lifecycle: %w", err)
	}
	return store.db.View(func(tx *bolt.Tx) error {
		placements := tx.Bucket(bucketName)
		capabilities := tx.Bucket(lifecycleCapabilityBucketName)
		if placements == nil || capabilities == nil {
			return errors.New("verify conflict repair: placement lifecycle buckets are missing")
		}
		if !bytes.Equal(placements.Get([]byte(leaseUUID)), placementEncoded) {
			return errors.New("verify conflict repair: durable placement differs from cache")
		}
		if !bytes.Equal(capabilities.Get([]byte(leaseUUID)), capabilityEncoded) {
			return errors.New("verify conflict repair: durable lifecycle differs from cache")
		}
		return nil
	})
}

func (repair *AttemptRepair) verifyRefused(candidate AttemptRepairCandidate) error {
	return verifyRefusedStore(repair.store, candidate)
}

func verifyRefusedStore(store *Store, candidate AttemptRepairCandidate) error {
	store.mu.RLock()
	defer store.mu.RUnlock()
	after, exists := store.cache[candidate.leaseUUID]

	if candidate.confirmedOwner == "" {
		if exists {
			return errors.New("verify placement repair: attempt-only placement row still exists")
		}
		capability, capabilityExists := store.lifecycleCache[candidate.leaseUUID]
		if capabilityExists != candidate.retainLifecycle ||
			(capabilityExists && capability != candidate.lifecycleAfter) {
			return errors.New("verify placement repair: prior lifecycle authority was not preserved exactly")
		}
		return store.db.View(func(tx *bolt.Tx) error {
			placements := tx.Bucket(bucketName)
			capabilities := tx.Bucket(lifecycleCapabilityBucketName)
			if placements == nil || capabilities == nil {
				return errors.New("verify placement repair: placement lifecycle buckets are missing")
			}
			if placements.Get([]byte(candidate.leaseUUID)) != nil {
				return errors.New("verify placement repair: durable attempt-only placement row still exists")
			}
			rawCapability := capabilities.Get([]byte(candidate.leaseUUID))
			if !candidate.retainLifecycle {
				if rawCapability != nil {
					return errors.New("verify placement repair: detached attempt lifecycle row still exists")
				}
				return nil
			}
			encoded, err := encodeLifecycleCapability(candidate.lifecycleAfter)
			if err != nil {
				return fmt.Errorf("verify placement repair: encode prior lifecycle authority: %w", err)
			}
			if !bytes.Equal(rawCapability, encoded) {
				return errors.New("verify placement repair: durable prior lifecycle authority was not preserved exactly")
			}
			return nil
		})
	}

	if !exists || after.State() != StateConfirmed || after.Backend != candidate.confirmedOwner ||
		after.Attempt != "" || after.attemptOperationID.Valid() {
		return errors.New("verify placement repair: confirmed owner was not preserved exactly")
	}
	placementEncoded, err := encodePlacement(after)
	if err != nil {
		return fmt.Errorf("verify placement repair: encode confirmed owner: %w", err)
	}
	capability, capabilityExists := store.lifecycleCache[candidate.leaseUUID]
	if !candidate.retainLifecycle || !capabilityExists || capability != candidate.lifecycleAfter {
		return errors.New("verify placement repair: confirmed lifecycle owner was not preserved exactly")
	}
	capabilityEncoded, err := encodeLifecycleCapability(capability)
	if err != nil {
		return fmt.Errorf("verify placement repair: encode confirmed lifecycle owner: %w", err)
	}
	return store.db.View(func(tx *bolt.Tx) error {
		placements := tx.Bucket(bucketName)
		capabilities := tx.Bucket(lifecycleCapabilityBucketName)
		if placements == nil || capabilities == nil {
			return errors.New("verify placement repair: placement lifecycle buckets are missing")
		}
		if !bytes.Equal(placements.Get([]byte(candidate.leaseUUID)), placementEncoded) {
			return errors.New("verify placement repair: durable confirmed owner differs from cache")
		}
		if !bytes.Equal(capabilities.Get([]byte(candidate.leaseUUID)), capabilityEncoded) {
			return errors.New("verify placement repair: durable confirmed lifecycle owner differs from cache")
		}
		return nil
	})
}
