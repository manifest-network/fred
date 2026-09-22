package placement

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/fsidentity"
	"github.com/manifest-network/fred/internal/util"
)

var (
	// ErrFreshInitializationProof means the supplied chain, inventory, or
	// quiescence evidence cannot authorize a new empty placement authority.
	ErrFreshInitializationProof = errors.New("invalid fresh placement initialization proof")
	// ErrPlacementStoreExists prevents fresh initialization from replacing or
	// mutating any existing path, including an empty or malformed file.
	ErrPlacementStoreExists = errors.New("placement database already exists")
	// ErrFreshInitializationPublished means the atomic no-overwrite publication
	// succeeded in the proof-bound physical directory before a subsequent
	// durability, pathname, or cleanup check failed. That directory and the
	// configured pathname must be preserved and inspected instead of retrying.
	ErrFreshInitializationPublished = errors.New("fresh placement initialization published")
	// ErrFreshInitializationConsumed means a single-use initialization
	// capability has already been presented to the filesystem initializer.
	ErrFreshInitializationConsumed = errors.New("fresh placement initialization capability already consumed")
	// ErrFreshInitializationExpired means the proof deadline elapsed before
	// the new authority reached its no-overwrite publication boundary.
	ErrFreshInitializationExpired = errors.New("fresh placement initialization proof expired")
)

const freshInitializationProofMaxAge = 2 * time.Minute

// FreshInitializationTarget is the immutable operator-supplied identity of a
// genuinely new placement authority. It is deliberately separate from the
// configured-backend inventory proof: initialization requires the operator's
// independently supplied exact fleet roster to agree with live configuration.
// Its zero value is invalid.
type FreshInitializationTarget struct {
	dbPath         string
	parentPath     string
	parentIdentity fsidentity.Identity
	providerUUID   string
	backendNames   []string
}

// NewFreshInitializationTarget resolves a target to an absolute path through
// its existing parent directory and canonicalizes the provider and fleet
// identities used by the high-friction operator acknowledgement. Resolution
// happens before remote proof collection so a later parent-symlink change is
// detected at the publication boundary.
func NewFreshInitializationTarget(
	dbPath string,
	providerUUID string,
	expectedBackendNames []string,
) (FreshInitializationTarget, error) {
	canonicalPath, parentPath, parentIdentity, err := canonicalFreshTargetPath(dbPath)
	if err != nil {
		return FreshInitializationTarget{}, fmt.Errorf("%w: %w", ErrFreshInitializationProof, err)
	}
	providerID, err := uuid.Parse(providerUUID)
	if err != nil || providerID == uuid.Nil || providerID.String() != providerUUID {
		return FreshInitializationTarget{}, fmt.Errorf(
			"%w: provider UUID %q is not canonical",
			ErrFreshInitializationProof,
			providerUUID,
		)
	}
	backendNames, err := canonicalBackendTopology(expectedBackendNames)
	if err != nil {
		return FreshInitializationTarget{}, fmt.Errorf("%w: expected fleet roster: %w", ErrFreshInitializationProof, err)
	}
	return FreshInitializationTarget{
		dbPath:         canonicalPath,
		parentPath:     parentPath,
		parentIdentity: parentIdentity,
		providerUUID:   providerUUID,
		backendNames:   slices.Clone(backendNames),
	}, nil
}

// Valid reports whether the target has a canonical path, provider, and
// nonempty sorted fleet roster.
func (target FreshInitializationTarget) Valid() bool {
	if target.dbPath == "" || !filepath.IsAbs(target.dbPath) ||
		target.parentPath == "" || filepath.Dir(target.dbPath) != target.parentPath ||
		!target.parentIdentity.Valid() ||
		target.providerUUID == "" || len(target.backendNames) == 0 {
		return false
	}
	providerID, err := uuid.Parse(target.providerUUID)
	if err != nil || providerID == uuid.Nil || providerID.String() != target.providerUUID {
		return false
	}
	return validateCanonicalBackendNames(target.backendNames, false) == nil
}

// DatabasePath returns the resolved path included in the operator
// acknowledgement and used by the no-overwrite publisher.
func (target FreshInitializationTarget) DatabasePath() string { return target.dbPath }

// ProviderUUID returns the canonical provider identity included in the
// operator acknowledgement.
func (target FreshInitializationTarget) ProviderUUID() string { return target.providerUUID }

// BackendNames returns a defensive copy of the canonical expected fleet
// roster supplied independently by the operator.
func (target FreshInitializationTarget) BackendNames() []string {
	return slices.Clone(target.backendNames)
}

// Confirmation returns the exact high-friction acknowledgement required for
// this target. JSON quoting keeps paths and backend names on one unambiguous
// line even when they contain spaces or punctuation.
func (target FreshInitializationTarget) Confirmation() string {
	if !target.Valid() {
		return ""
	}
	pathJSON, _ := json.Marshal(target.dbPath)
	providerJSON, _ := json.Marshal(target.providerUUID)
	backendsJSON, _ := json.Marshal(target.backendNames)
	parentJSON, _ := json.Marshal(target.parentIdentity)
	return fmt.Sprintf(
		"I CONFIRM FRESH PLACEMENT INITIALIZATION target=%s parent=%s provider=%s backends=%s; "+
			"THE PROVIDER AND ALL BACKENDS ARE QUIESCED",
		pathJSON,
		parentJSON,
		providerJSON,
		backendsJSON,
	)
}

func canonicalFreshTargetPath(
	dbPath string,
) (string, string, fsidentity.Identity, error) {
	if dbPath == "" || !filepath.IsAbs(dbPath) || filepath.Clean(dbPath) != dbPath {
		return "", "", fsidentity.Identity{},
			errors.New("placement db path must be non-empty, absolute, and clean")
	}
	parentPath, err := filepath.EvalSymlinks(filepath.Dir(dbPath))
	if err != nil {
		return "", "", fsidentity.Identity{},
			fmt.Errorf("resolve placement db directory: %w", err)
	}
	parentPath, err = filepath.Abs(parentPath)
	if err != nil {
		return "", "", fsidentity.Identity{},
			fmt.Errorf("resolve absolute placement db directory: %w", err)
	}
	parentPath = filepath.Clean(parentPath)
	targetName := filepath.Base(dbPath)
	if targetName == "." || targetName == string(filepath.Separator) {
		return "", "", fsidentity.Identity{}, errors.New("placement db path must name a file")
	}
	identity, err := fsidentity.InspectDirectory(parentPath)
	if err != nil {
		return "", "", fsidentity.Identity{},
			fmt.Errorf("bind placement db directory identity: %w", err)
	}
	return filepath.Join(parentPath, targetName), parentPath, identity, nil
}

// FreshChainSnapshot is the narrow read-only evidence produced by the
// signer-free, height-pinned chain client. An implementation must represent a
// complete all-state query for one provider at one positive block height.
//
// The placement package copies the facts into an opaque FreshChainProof. The
// interface keeps chain transport concerns out of the authority store while
// still making the cutover command pass the actual snapshot, not a collection
// of loosely related scalar flags.
type FreshChainSnapshot interface {
	Valid() bool
	ProviderUUID() string
	BlockHeight() int64
	TotalLeases() int
	BlockingLeaseCount() int
}

// FreshChainProof is immutable evidence that one complete provider lease
// snapshot contained no lease history at all. Fresh initialization is only for
// a genuinely new provider; a lost authority for a previously used provider
// must be restored, never reconstructed from current silence. Its zero value is
// invalid and its fields cannot be manufactured by callers.
type FreshChainProof struct {
	providerUUID string
	blockHeight  int64
	totalLeases  int
}

// NewFreshChainProof validates and copies a signer-free, height-pinned chain
// snapshot. Any lease history, including terminal history, blocks creation of a
// new empty placement authority.
func NewFreshChainProof(snapshot FreshChainSnapshot) (FreshChainProof, error) {
	if util.IsNilInterface(snapshot) || !snapshot.Valid() {
		return FreshChainProof{}, fmt.Errorf("%w: chain snapshot is incomplete", ErrFreshInitializationProof)
	}
	providerID, err := uuid.Parse(snapshot.ProviderUUID())
	if err != nil || providerID == uuid.Nil || providerID.String() != snapshot.ProviderUUID() {
		return FreshChainProof{}, fmt.Errorf(
			"%w: provider UUID %q is not canonical",
			ErrFreshInitializationProof,
			snapshot.ProviderUUID(),
		)
	}
	if snapshot.BlockHeight() <= 0 || snapshot.TotalLeases() < 0 ||
		snapshot.BlockingLeaseCount() < 0 ||
		snapshot.BlockingLeaseCount() > snapshot.TotalLeases() {
		return FreshChainProof{}, fmt.Errorf("%w: malformed chain snapshot counters", ErrFreshInitializationProof)
	}
	if snapshot.BlockingLeaseCount() != 0 {
		return FreshChainProof{}, fmt.Errorf(
			"%w: provider has %d non-terminal or unknown leases at height %d",
			ErrFreshInitializationProof,
			snapshot.BlockingLeaseCount(),
			snapshot.BlockHeight(),
		)
	}
	if snapshot.TotalLeases() != 0 {
		return FreshChainProof{}, fmt.Errorf(
			"%w: provider has %d leases in chain history at height %d; fresh initialization requires a genuinely new provider",
			ErrFreshInitializationProof,
			snapshot.TotalLeases(),
			snapshot.BlockHeight(),
		)
	}
	return FreshChainProof{
		providerUUID: snapshot.ProviderUUID(),
		blockHeight:  snapshot.BlockHeight(),
		totalLeases:  snapshot.TotalLeases(),
	}, nil
}

// Valid reports whether the proof contains a complete positive-height snapshot
// identity for a provider with zero total chain lease history.
func (proof FreshChainProof) Valid() bool {
	return proof.providerUUID != "" && proof.blockHeight > 0 && proof.totalLeases == 0
}

// FreshBackendProof is immutable evidence that every configured backend
// returned complete, empty provision and retention inventories under a unique
// valid storage identity. Its zero value is invalid.
type FreshBackendProof struct {
	topology   []string
	identities map[string]backendidentity.ID
}

// NewFreshBackendProof validates one complete fleet snapshot. Nil inventory
// slices are rejected even though they have length zero: nil means the caller
// did not prove that endpoint returned a concrete empty collection.
func NewFreshBackendProof(
	configuredBackends []string,
	inventories map[string]BackendInventory,
) (FreshBackendProof, error) {
	canonical, err := canonicalBackendTopology(configuredBackends)
	if err != nil {
		return FreshBackendProof{}, fmt.Errorf("%w: %w", ErrFreshInitializationProof, err)
	}
	if len(inventories) != len(canonical) {
		return FreshBackendProof{}, fmt.Errorf(
			"%w: received %d inventories for %d configured backends",
			ErrFreshInitializationProof,
			len(inventories),
			len(canonical),
		)
	}
	configured := make(map[string]struct{}, len(canonical))
	identities := make(map[string]backendidentity.ID, len(canonical))
	owners := make(map[backendidentity.ID]string, len(canonical))
	for _, backendName := range canonical {
		configured[backendName] = struct{}{}
		inventory, present := inventories[backendName]
		if !present {
			return FreshBackendProof{}, fmt.Errorf(
				"%w: backend %q has no complete inventory",
				ErrFreshInitializationProof,
				backendName,
			)
		}
		if !inventory.StorageIdentity.Valid() {
			return FreshBackendProof{}, fmt.Errorf(
				"%w: backend %q has no valid storage identity",
				ErrFreshInitializationProof,
				backendName,
			)
		}
		if inventory.Provisions == nil || inventory.ProvisionProviderUUIDs == nil ||
			inventory.Retentions == nil {
			return FreshBackendProof{}, fmt.Errorf(
				"%w: backend %q returned a null inventory",
				ErrFreshInitializationProof,
				backendName,
			)
		}
		if len(inventory.Provisions) != 0 || len(inventory.Retentions) != 0 {
			return FreshBackendProof{}, fmt.Errorf(
				"%w: backend %q is not empty (%d provisions, %d retentions)",
				ErrFreshInitializationProof,
				backendName,
				len(inventory.Provisions),
				len(inventory.Retentions),
			)
		}
		if len(inventory.ProvisionProviderUUIDs) != 0 {
			return FreshBackendProof{}, fmt.Errorf(
				"%w: backend %q returned provider observations for an empty provisions inventory",
				ErrFreshInitializationProof,
				backendName,
			)
		}
		if owner, duplicate := owners[inventory.StorageIdentity]; duplicate {
			return FreshBackendProof{}, fmt.Errorf(
				"%w: backends %q and %q share storage identity %s",
				ErrFreshInitializationProof,
				owner,
				backendName,
				inventory.StorageIdentity,
			)
		}
		owners[inventory.StorageIdentity] = backendName
		identities[backendName] = inventory.StorageIdentity
	}
	for backendName := range inventories {
		if _, present := configured[backendName]; !present {
			return FreshBackendProof{}, fmt.Errorf(
				"%w: inventory supplied for unconfigured backend %q",
				ErrFreshInitializationProof,
				backendName,
			)
		}
	}
	return FreshBackendProof{
		topology:   slices.Clone(canonical),
		identities: identities,
	}, nil
}

// Valid reports whether the proof has at least one configured backend and one
// unique valid identity for every name.
func (proof FreshBackendProof) Valid() bool {
	if len(proof.topology) == 0 || len(proof.identities) != len(proof.topology) {
		return false
	}
	owners := make(map[backendidentity.ID]struct{}, len(proof.topology))
	for _, name := range proof.topology {
		id := proof.identities[name]
		if strings.TrimSpace(name) == "" || !id.Valid() {
			return false
		}
		if _, duplicate := owners[id]; duplicate {
			return false
		}
		owners[id] = struct{}{}
	}
	return slices.IsSorted(proof.topology)
}

// FreshQuiescenceProof records the operator's assertion that provider and
// tenant/chain mutation ingress are stopped and drained for one exact target,
// provider, and fleet roster. Every backend remains reachable for inventory,
// but is empty and has no admitted or in-flight mutation, callback, or outbox
// work. The zero value is invalid.
type FreshQuiescenceProof struct {
	targetPath     string
	parentIdentity fsidentity.Identity
	providerUUID   string
	backendNames   []string
	confirmed      bool
}

// ConfirmFreshQuiescence accepts only the target-bound high-friction
// acknowledgement printed by the offline cutover tool and deployment runbook.
func ConfirmFreshQuiescence(
	target FreshInitializationTarget,
	statement string,
) (FreshQuiescenceProof, error) {
	if !target.Valid() {
		return FreshQuiescenceProof{}, ErrFreshInitializationProof
	}
	expected := target.Confirmation()
	if statement != expected {
		return FreshQuiescenceProof{}, fmt.Errorf(
			"%w: quiescence confirmation must exactly equal:\n%s",
			ErrFreshInitializationProof,
			expected,
		)
	}
	return FreshQuiescenceProof{
		targetPath:     target.dbPath,
		parentIdentity: target.parentIdentity,
		providerUUID:   target.providerUUID,
		backendNames:   slices.Clone(target.backendNames),
		confirmed:      true,
	}, nil
}

// FreshInitializationPlan combines three independent proof classes. The
// opaque value prevents an empty map, a read taken at no block height, or a
// casual boolean flag from reaching the filesystem initializer. It is bound to
// the proof context and deadline and may be presented exactly once.
type FreshInitializationPlan struct {
	target     FreshInitializationTarget
	chain      FreshChainProof
	backends   FreshBackendProof
	quiescence FreshQuiescenceProof
	proofCtx   context.Context
	expiresAt  time.Time
	// consumed is indirect so copying the exported opaque value cannot clone a
	// fresh single-use bit and mint a second filesystem capability. Every value
	// copy produced from one constructor shares this exact consumption cell.
	consumed *atomic.Bool
}

// NewFreshInitializationPlan constructs an immutable initialization
// capability. Each component's zero value is rejected, the independent roster
// must exactly match configured inventory evidence, and ctx must carry a live
// deadline that expires the evidence.
func NewFreshInitializationPlan(
	ctx context.Context,
	target FreshInitializationTarget,
	chain FreshChainProof,
	backends FreshBackendProof,
	quiescence FreshQuiescenceProof,
) (*FreshInitializationPlan, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: proof context is required", ErrFreshInitializationProof)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: proof context: %w", ErrFreshInitializationProof, err)
	}
	now := time.Now()
	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline || !now.Before(deadline) {
		return nil, fmt.Errorf("%w: proof context requires a live deadline", ErrFreshInitializationProof)
	}
	expiresAt := now.Add(freshInitializationProofMaxAge)
	if deadline.Before(expiresAt) {
		expiresAt = deadline
	}
	if !target.Valid() || !chain.Valid() || !backends.Valid() || !quiescence.confirmed {
		return nil, ErrFreshInitializationProof
	}
	if chain.providerUUID != target.providerUUID {
		return nil, fmt.Errorf(
			"%w: chain provider %q does not match expected provider %q",
			ErrFreshInitializationProof,
			chain.providerUUID,
			target.providerUUID,
		)
	}
	if !slices.Equal(backends.topology, target.backendNames) {
		return nil, fmt.Errorf(
			"%w: configured backend topology %v does not match independently supplied roster %v",
			ErrFreshInitializationProof,
			backends.topology,
			target.backendNames,
		)
	}
	if quiescence.targetPath != target.dbPath ||
		!quiescence.parentIdentity.Equal(target.parentIdentity) ||
		quiescence.providerUUID != target.providerUUID ||
		!slices.Equal(quiescence.backendNames, target.backendNames) {
		return nil, fmt.Errorf("%w: quiescence acknowledgement is bound to a different target", ErrFreshInitializationProof)
	}
	parent, err := fsidentity.OpenBoundDirectory(target.parentPath, target.parentIdentity)
	if err != nil {
		return nil, fmt.Errorf("%w: re-attest proof-bound placement parent: %w",
			ErrFreshInitializationProof, err)
	}
	if err := parent.Close(); err != nil {
		return nil, fmt.Errorf("%w: close proof-bound placement parent: %w",
			ErrFreshInitializationProof, err)
	}
	target.backendNames = slices.Clone(target.backendNames)
	backends.topology = slices.Clone(backends.topology)
	backends.identities = cloneBackendIdentities(backends.identities)
	quiescence.backendNames = slices.Clone(quiescence.backendNames)
	return &FreshInitializationPlan{
		target:     target,
		chain:      chain,
		backends:   backends,
		quiescence: quiescence,
		proofCtx:   ctx,
		expiresAt:  expiresAt,
		consumed:   &atomic.Bool{},
	}, nil
}

func (plan *FreshInitializationPlan) validateContext(ctx context.Context) error {
	if plan == nil || !plan.target.Valid() || !plan.chain.Valid() ||
		!plan.backends.Valid() || !plan.quiescence.confirmed || plan.proofCtx == nil || plan.consumed == nil {
		return ErrFreshInitializationProof
	}
	if ctx == nil {
		return fmt.Errorf("%w: initializer context is required", ErrFreshInitializationProof)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := plan.proofCtx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("%w: %w", ErrFreshInitializationExpired, err)
		}
		return err
	}
	if !time.Now().Before(plan.expiresAt) {
		return ErrFreshInitializationExpired
	}
	return nil
}

// InitializeFreshStoreContext consumes plan exactly once and creates the bound
// prepared placement authority. It never opens, truncates, migrates, or
// replaces an existing path. Construction happens in a same-directory
// temporary file; an atomic no-replace rename publishes the fully synced and
// re-open-verified database with no-overwrite semantics and no second name.
func InitializeFreshStoreContext(
	ctx context.Context,
	plan *FreshInitializationPlan,
) error {
	return initializeFreshStoreContext(
		ctx,
		plan,
		func(parent *fsidentity.Directory, oldName, newName string) error {
			return parent.RenameNoReplace(oldName, newName)
		},
	)
}

type freshPublisher func(*fsidentity.Directory, string, string) error

func initializeFreshStoreContext(
	ctx context.Context,
	plan *FreshInitializationPlan,
	publish freshPublisher,
) (resultErr error) {
	published := false
	defer func() {
		if published && resultErr != nil &&
			!errors.Is(resultErr, ErrFreshInitializationPublished) {
			resultErr = fmt.Errorf("%w: %w", ErrFreshInitializationPublished, resultErr)
		}
	}()
	if plan == nil {
		return ErrFreshInitializationProof
	}
	if plan.consumed == nil {
		return ErrFreshInitializationProof
	}
	if !plan.consumed.CompareAndSwap(false, true) {
		return ErrFreshInitializationConsumed
	}
	if err := plan.validateContext(ctx); err != nil {
		return err
	}
	dbPath := plan.target.dbPath
	parent, err := fsidentity.OpenBoundDirectory(
		plan.target.parentPath,
		plan.target.parentIdentity,
	)
	if err != nil {
		return fmt.Errorf(
			"%w: open proof-bound placement parent: %w",
			ErrFreshInitializationProof,
			err,
		)
	}
	defer func() {
		if err := parent.Close(); resultErr == nil && err != nil {
			resultErr = fmt.Errorf("close proof-bound placement db directory: %w", err)
		}
	}()
	if err := parent.VerifyPath(); err != nil {
		return fmt.Errorf(
			"%w: re-attest placement target before initialization: %w",
			ErrFreshInitializationProof,
			err,
		)
	}
	targetName := filepath.Base(dbPath)
	exists, err := parent.EntryExists(targetName)
	if err != nil {
		return fmt.Errorf("inspect placement db destination: %w", err)
	}
	if exists {
		return ErrPlacementStoreExists
	}

	temporaryName, temporary, err := createFreshTemporary(parent)
	if err != nil {
		return fmt.Errorf("create placement db temporary file: %w", err)
	}
	temporaryOpen := true
	defer func() {
		if temporaryOpen {
			if err := temporary.Close(); resultErr == nil && err != nil {
				resultErr = fmt.Errorf("close placement db temporary file: %w", err)
			}
		}
		if err := parent.Remove(temporaryName); resultErr == nil &&
			err != nil && !errors.Is(err, os.ErrNotExist) {
			resultErr = fmt.Errorf("remove placement db temporary file: %w", err)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("set placement db temporary permissions: %w", err)
	}
	temporaryInfo, err := temporary.Stat()
	if err != nil {
		return fmt.Errorf("stat placement db temporary file: %w", err)
	}
	if !temporaryInfo.Mode().IsRegular() {
		return errors.New("placement db temporary file is not regular")
	}
	if err := temporary.Close(); err != nil {
		temporaryOpen = false
		return fmt.Errorf("close placement db temporary file before bbolt open: %w", err)
	}
	temporaryOpen = false

	db, err := openFreshBoltFile(
		parent,
		temporaryName,
		temporaryInfo,
		0o600,
		&bolt.Options{Timeout: 0},
	)
	if err != nil {
		return fmt.Errorf("open placement db temporary file: %w", err)
	}
	dbOpen := true
	defer func() {
		if dbOpen {
			if err := db.Close(); resultErr == nil && err != nil {
				resultErr = fmt.Errorf("close placement db temporary database: %w", err)
			}
		}
	}()
	fingerprint, err := topologyFingerprint(plan.backends.topology)
	if err != nil {
		return err
	}
	storageIDs := make(map[string]string, len(plan.backends.identities))
	for backendName, id := range plan.backends.identities {
		storageIDs[backendName] = id.String()
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketName) != nil || tx.Bucket(metadataBucketName) != nil ||
			tx.Bucket(lifecycleCapabilityBucketName) != nil {
			return errors.New("fresh placement temporary database is not empty")
		}
		if _, err := tx.CreateBucket(bucketName); err != nil {
			return err
		}
		if err := initializeMetadata(tx); err != nil {
			return err
		}
		metadata := topologyMetadata{
			Schema:                 topologyMetadataSchema,
			ProviderUUID:           plan.chain.providerUUID,
			Topology:               slices.Clone(plan.backends.topology),
			TopologyFingerprint:    fingerprint,
			KnownBackends:          slices.Clone(plan.backends.topology),
			KnownBackendStorageIDs: storageIDs,
			TopologyID:             1,
			BaselineFingerprint:    fingerprint,
			BaselineTopologyID:     1,
			InventoryTopologyID:    1,
			EmptyInventoryBackends: slices.Clone(plan.backends.topology),
		}
		if err := putTopologyMetadata(tx, metadata); err != nil {
			return err
		}
		return initializeLifecycleCapabilities(tx, lifecycleInitializationEpoch{})
	}); err != nil {
		return fmt.Errorf("initialize fresh placement database: %w", err)
	}
	if err := db.Sync(); err != nil {
		return fmt.Errorf("sync fresh placement database: %w", err)
	}
	if err := db.Close(); err != nil {
		dbOpen = false
		return fmt.Errorf("close fresh placement database: %w", err)
	}
	dbOpen = false

	if err := verifyFreshDatabaseAt(
		parent,
		temporaryName,
		temporaryInfo,
		plan,
	); err != nil {
		return fmt.Errorf("verify exact fresh placement authority: %w", err)
	}

	if err := plan.validateContext(ctx); err != nil {
		return err
	}
	if err := parent.VerifyPath(); err != nil {
		return fmt.Errorf(
			"%w: placement target changed before publication: %w",
			ErrFreshInitializationProof,
			err,
		)
	}
	exists, err = parent.EntryExists(targetName)
	if err != nil {
		return fmt.Errorf("inspect placement db destination before publication: %w", err)
	}
	if exists {
		return ErrPlacementStoreExists
	}
	if err := verifyFreshFileIdentity(parent, temporaryName, temporaryInfo); err != nil {
		return fmt.Errorf("reverify fresh placement publication source: %w", err)
	}
	if publish == nil {
		return errors.New("fresh placement no-overwrite publisher is required")
	}
	if err := publish(parent, temporaryName, targetName); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrPlacementStoreExists
		}
		return fmt.Errorf("publish fresh placement database: %w", err)
	}
	published = true
	if err := verifyFreshFileIdentity(parent, targetName, temporaryInfo); err != nil {
		return fmt.Errorf("verify fresh placement publication identity: %w", err)
	}
	if err := plan.validateContext(ctx); err != nil {
		return err
	}
	publishedFile, err := parent.OpenFile(targetName, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open published placement database: %w", err)
	}
	publishedInfo, err := publishedFile.Stat()
	if err != nil {
		_ = publishedFile.Close()
		return fmt.Errorf("stat opened published placement database: %w", err)
	}
	if !os.SameFile(temporaryInfo, publishedInfo) {
		_ = publishedFile.Close()
		return errors.New("opened published placement database is not the verified source inode")
	}
	if err := publishedFile.Sync(); err != nil {
		_ = publishedFile.Close()
		return fmt.Errorf("sync published placement database: %w", err)
	}
	if err := publishedFile.Close(); err != nil {
		return fmt.Errorf("close published placement database: %w", err)
	}
	if err := parent.Sync(); err != nil {
		return fmt.Errorf("sync placement db directory: %w", err)
	}
	if err := verifyFreshDatabaseAt(
		parent,
		targetName,
		temporaryInfo,
		plan,
	); err != nil {
		return fmt.Errorf("reopen and validate published placement authority: %w", err)
	}
	if err := verifyFreshFileIdentity(parent, targetName, temporaryInfo); err != nil {
		return fmt.Errorf("reverify fresh placement publication identity: %w", err)
	}
	if err := parent.VerifyPath(); err != nil {
		return fmt.Errorf(
			"%w: placement target changed after publication: %w",
			ErrFreshInitializationProof,
			err,
		)
	}
	if err := plan.validateContext(ctx); err != nil {
		return err
	}
	return nil
}

func createFreshTemporary(
	parent *fsidentity.Directory,
) (string, *os.File, error) {
	if parent == nil {
		return "", nil, errors.New("placement db parent directory is required")
	}
	return parent.CreateTemp(".fred-placement-init-", 0o600)
}

// openFreshBoltFile makes bbolt use the proof-bound directory descriptor
// instead of resolving its display pathname. O_CREATE is stripped because the
// caller must have created and identity-bound the exact regular file first.
func openFreshBoltFile(
	parent *fsidentity.Directory,
	name string,
	expectedInfo os.FileInfo,
	mode os.FileMode,
	options *bolt.Options,
) (*bolt.DB, error) {
	if parent == nil {
		return nil, errors.New("placement db parent directory is required")
	}
	if expectedInfo == nil {
		return nil, errors.New("placement db file identity is required")
	}
	if options == nil {
		options = &bolt.Options{}
	} else {
		cloned := *options
		options = &cloned
	}
	displayPath := parent.DisplayPath(name)
	options.OpenFile = func(requested string, flag int, requestedMode os.FileMode) (*os.File, error) {
		if filepath.Clean(requested) != displayPath {
			return nil, errors.New("bbolt requested an unexpected fresh placement path")
		}
		file, openErr := parent.OpenFile(name, flag&^os.O_CREATE, requestedMode)
		if openErr != nil {
			return nil, openErr
		}
		openedInfo, statErr := file.Stat()
		if statErr != nil || !openedInfo.Mode().IsRegular() ||
			!os.SameFile(expectedInfo, openedInfo) {
			_ = file.Close()
			if statErr != nil {
				return nil, fmt.Errorf("stat opened fresh placement database: %w", statErr)
			}
			return nil, errors.New("fresh placement database changed before bbolt open")
		}
		return file, nil
	}
	return bolt.Open(displayPath, mode, options)
}

func verifyFreshFileIdentity(
	parent *fsidentity.Directory,
	name string,
	expectedInfo os.FileInfo,
) error {
	if expectedInfo == nil {
		return errors.New("fresh placement file identity is required")
	}
	file, err := parent.OpenFile(name, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	openedInfo, statErr := file.Stat()
	closeErr := file.Close()
	if statErr != nil {
		return fmt.Errorf("stat fresh placement file: %w", statErr)
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(expectedInfo, openedInfo) {
		return errors.New("fresh placement path no longer names the verified source inode")
	}
	if closeErr != nil {
		return fmt.Errorf("close fresh placement identity file: %w", closeErr)
	}
	return nil
}

// verifyFreshDatabaseAt pins both physical and semantic validation to the
// proof-bound directory descriptor and exact inode. A post-publication failure
// is classified by the caller as ErrFreshInitializationPublished.
func verifyFreshDatabaseAt(
	parent *fsidentity.Directory,
	name string,
	expectedInfo os.FileInfo,
	plan *FreshInitializationPlan,
) (resultErr error) {
	db, err := openFreshBoltFile(
		parent,
		name,
		expectedInfo,
		0o600,
		&bolt.Options{ReadOnly: true, Timeout: time.Second},
	)
	if err != nil {
		return fmt.Errorf("open fresh placement database read-only: %w", err)
	}
	defer func() {
		if err := db.Close(); resultErr == nil && err != nil {
			resultErr = fmt.Errorf("close verified fresh placement database: %w", err)
		}
	}()
	if err := verifyBoltPhysicalConsistency(db); err != nil {
		return fmt.Errorf("physical consistency: %w", err)
	}
	if err := verifyFreshInitializationPostcondition(db, plan); err != nil {
		return fmt.Errorf("semantic postcondition: %w", err)
	}
	if err := verifyFreshFileIdentity(parent, name, expectedInfo); err != nil {
		return fmt.Errorf("identity postcondition: %w", err)
	}
	return nil
}

func verifyFreshInitializationPostcondition(
	db *bolt.DB,
	plan *FreshInitializationPlan,
) error {
	if db == nil || plan == nil || !plan.target.Valid() || !plan.backends.Valid() {
		return ErrFreshInitializationProof
	}
	fingerprint, err := topologyFingerprint(plan.backends.topology)
	if err != nil {
		return err
	}
	storageIDs := make(map[string]string, len(plan.backends.identities))
	for backendName, id := range plan.backends.identities {
		storageIDs[backendName] = id.String()
	}
	return db.View(func(tx *bolt.Tx) error {
		if err := verifyAuthorityBuckets(tx); err != nil {
			return err
		}
		if err := verifyExactAuthorityBucketSet(tx); err != nil {
			return err
		}
		metadata, err := loadTopologyMetadata(tx)
		if err != nil {
			return err
		}
		if metadata.ProviderUUID != plan.chain.providerUUID ||
			!slices.Equal(metadata.Topology, plan.backends.topology) ||
			metadata.TopologyFingerprint != fingerprint ||
			!slices.Equal(metadata.KnownBackends, plan.backends.topology) ||
			!maps.Equal(metadata.KnownBackendStorageIDs, storageIDs) ||
			metadata.TopologyID != 1 || metadata.BaselineFingerprint != fingerprint ||
			metadata.BaselineTopologyID != 1 || metadata.InventoryTopologyID != 1 ||
			!slices.Equal(metadata.EmptyInventoryBackends, plan.backends.topology) {
			return errors.New("fresh placement metadata differs from the exact initialization proof")
		}
		for _, name := range [][]byte{bucketName, lifecycleCapabilityBucketName} {
			bucket := tx.Bucket(name)
			if bucket == nil {
				return fmt.Errorf("fresh placement bucket %q is missing", name)
			}
			if key, _ := bucket.Cursor().First(); key != nil {
				return fmt.Errorf("fresh placement bucket %q is not empty", name)
			}
		}
		return nil
	})
}

func cloneBackendIdentities(source map[string]backendidentity.ID) map[string]backendidentity.ID {
	clone := make(map[string]backendidentity.ID, len(source))
	for name, id := range source {
		clone[name] = id
	}
	return clone
}
