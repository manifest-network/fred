package placement

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/util"
)

// ErrLegacyUpgradePreflight reports one or more schema, topology, ownership,
// or inventory discrepancies that make automatic v0.13 adoption unsafe.
var ErrLegacyUpgradePreflight = errors.New("legacy placement upgrade preflight failed")

// ErrLegacyPreparationCommitted means the v0.13 migration transaction returned
// success before a later durability check failed. The source must be treated as
// prepared and inspected; blindly retrying cannot recreate the legacy boundary.
var ErrLegacyPreparationCommitted = errors.New("legacy placement preparation committed")

// ErrLegacyPreparationOutcomeUnknown means bbolt returned an error from Commit.
// Database pages or a meta page may already have reached the filesystem, so the
// stopped source and exact backup must be preserved and inspected before any
// retry or rollback decision.
var ErrLegacyPreparationOutcomeUnknown = errors.New("legacy placement preparation commit outcome is unknown")

// ErrExactBackupPublished means the atomic no-overwrite publication succeeded before a
// later verification, durability, close, or cleanup step failed. The target is
// an operator artifact and must be preserved; a retry requires a new path.
var ErrExactBackupPublished = errors.New("exact placement backup published")

// ErrPreparationRequired means an existing pre-identity placement database
// must be verified and sealed by placement-preflight --prepare before an
// upgraded providerd may open it. Automatic first-open migration would allow
// a same-name replacement backend to mint authority for the wrong storage.
var ErrPreparationRequired = errors.New("placement database requires offline preparation")

// LegacyPreparationDrainAttestation is the exact operator statement required
// before point-in-time chain and backend observations may authorize the v0.13
// migration. Fred cannot remotely fence every backend generation yet; this
// explicit statement is the causal boundary operators must establish first.
const LegacyPreparationDrainAttestation = "I attest provider ingress, delayed backend requests/effects, and callback replay are stopped and drained"

const legacyPreparationCapabilityMaxAge = 2 * time.Minute

// ErrLegacyPreparationCapability means the opaque causal-drain authority is
// absent, expired, belongs to another preparer, or no longer matches the exact
// provider/database/evidence/backup target.
var ErrLegacyPreparationCapability = errors.New("legacy placement preparation capability is invalid")

// BackendInventory is one configured backend's complete provision and
// retention inventory. Callers must include an entry for every configured
// backend, including backends whose two inventories are empty. Provisions and
// ProvisionProviderUUIDs are an exact pair: the map must contain one observation
// for every provision and no others. An empty observed provider is preserved as
// legacy evidence; every non-empty value must match the configured provider.
type BackendInventory struct {
	StorageIdentity        backendidentity.ID
	Provisions             []string
	ProvisionProviderUUIDs map[string]string
	ProvisionItems         map[string][]backend.LeaseItem
	Retentions             []string
}

// ProviderLeaseMembershipSnapshot is a complete, height-pinned, signer-free
// all-state chain observation. It extends the fresh-initialization counters
// with exact immutable membership so a v0.13 database can be bound to a
// provider by evidence rather than by trusting configuration alone.
type ProviderLeaseMembershipSnapshot interface {
	FreshChainSnapshot
	LeaseUUIDs() []string
	LeaseItems() map[string][]backend.LeaseItem
}

// LegacyUpgradeChainProof is immutable provider-membership authority consumed
// by the stopped-database preflight. Its zero value is invalid. The private set
// prevents callers from changing the proof between validation and the bbolt
// transaction that installs provider authority.
type LegacyUpgradeChainProof struct {
	providerUUID string
	blockHeight  int64
	leaseUUIDs   map[string]struct{}
	leaseItems   map[string][]backend.LeaseItem
}

// LegacyPreparationCapability is short-lived, opaque authority for one exact
// stopped-database migration target. Its zero value is invalid. It binds the
// issuing preparer session, provider, canonical live database path, canonical
// backup destination, topology and storage/inventory evidence, and exact
// height-pinned chain membership snapshot.
type LegacyPreparationCapability struct {
	issuer       *LegacyUpgradePreparer
	targetDigest [sha256.Size]byte
	backupTarget *ExactBackupTarget
	proofCtx     context.Context
	expiresAt    time.Time
	// consumed is indirect so copying the opaque value cannot duplicate the
	// one-shot authority admitted at the exact-backup publication boundary.
	consumed *atomic.Bool
}

type legacyPreparationProviderObservation struct {
	LeaseUUID    string `json:"lease_uuid"`
	ProviderUUID string `json:"provider_uuid"`
}

type legacyPreparationBackendTarget struct {
	Name               string                                 `json:"name"`
	StorageIdentity    string                                 `json:"storage_identity"`
	Provisions         []string                               `json:"provisions"`
	ProvisionProviders []legacyPreparationProviderObservation `json:"provision_providers"`
	ProvisionItems     []legacyPreparationLeaseItems          `json:"provision_items"`
	Retentions         []string                               `json:"retentions"`
}

type legacyPreparationLeaseItems struct {
	LeaseUUID string              `json:"lease_uuid"`
	Items     []backend.LeaseItem `json:"items"`
}

type legacyPreparationTarget struct {
	ProviderUUID       string                           `json:"provider_uuid"`
	DatabasePath       string                           `json:"database_path"`
	BackupPath         string                           `json:"backup_path"`
	BackupParentDevice uint64                           `json:"backup_parent_device"`
	BackupParentInode  uint64                           `json:"backup_parent_inode"`
	Backends           []legacyPreparationBackendTarget `json:"backends"`
	ChainHeight        int64                            `json:"chain_height"`
	ChainLeases        []string                         `json:"chain_leases"`
	ChainItems         []legacyPreparationLeaseItems    `json:"chain_items"`
}

// NewLegacyUpgradeChainProof validates and copies a complete provider query.
// All lease states are accepted: active provisions and retention-only rows can
// both be legitimate v0.13 survivors, but every relevant lease must belong to
// this exact provider at the pinned height.
func NewLegacyUpgradeChainProof(
	snapshot ProviderLeaseMembershipSnapshot,
) (LegacyUpgradeChainProof, error) {
	if util.IsNilInterface(snapshot) || !snapshot.Valid() {
		return LegacyUpgradeChainProof{}, fmt.Errorf(
			"%w: provider chain snapshot is incomplete", ErrLegacyUpgradePreflight,
		)
	}
	if !canonicalLeaseUUID(snapshot.ProviderUUID()) {
		return LegacyUpgradeChainProof{}, fmt.Errorf(
			"%w: chain snapshot provider UUID %q is not canonical",
			ErrLegacyUpgradePreflight, snapshot.ProviderUUID(),
		)
	}
	leaseUUIDs := snapshot.LeaseUUIDs()
	if leaseUUIDs == nil || snapshot.BlockHeight() <= 0 || snapshot.TotalLeases() != len(leaseUUIDs) {
		return LegacyUpgradeChainProof{}, fmt.Errorf(
			"%w: malformed provider chain snapshot membership at height %d (total=%d, identities=%d)",
			ErrLegacyUpgradePreflight,
			snapshot.BlockHeight(),
			snapshot.TotalLeases(),
			len(leaseUUIDs),
		)
	}
	membership := make(map[string]struct{}, len(leaseUUIDs))
	for _, leaseUUID := range leaseUUIDs {
		if !canonicalLeaseUUID(leaseUUID) {
			return LegacyUpgradeChainProof{}, fmt.Errorf(
				"%w: provider chain snapshot contains non-canonical lease UUID %q",
				ErrLegacyUpgradePreflight, leaseUUID,
			)
		}
		if _, duplicate := membership[leaseUUID]; duplicate {
			return LegacyUpgradeChainProof{}, fmt.Errorf(
				"%w: provider chain snapshot contains duplicate lease %q",
				ErrLegacyUpgradePreflight, leaseUUID,
			)
		}
		membership[leaseUUID] = struct{}{}
	}
	observedItems := snapshot.LeaseItems()
	if observedItems == nil || len(observedItems) != len(membership) {
		return LegacyUpgradeChainProof{}, fmt.Errorf(
			"%w: malformed provider chain workload snapshot at height %d (leases=%d, workloads=%d)",
			ErrLegacyUpgradePreflight,
			snapshot.BlockHeight(),
			len(membership),
			len(observedItems),
		)
	}
	leaseItems := make(map[string][]backend.LeaseItem, len(observedItems))
	for leaseUUID, items := range observedItems {
		if _, present := membership[leaseUUID]; !present {
			return LegacyUpgradeChainProof{}, fmt.Errorf(
				"%w: provider chain workload snapshot contains unknown lease %q",
				ErrLegacyUpgradePreflight,
				leaseUUID,
			)
		}
		leaseItems[leaseUUID] = cloneLeaseItems(items)
	}
	for leaseUUID := range membership {
		if _, present := leaseItems[leaseUUID]; !present {
			return LegacyUpgradeChainProof{}, fmt.Errorf(
				"%w: provider chain workload snapshot omits lease %q",
				ErrLegacyUpgradePreflight,
				leaseUUID,
			)
		}
	}
	return LegacyUpgradeChainProof{
		providerUUID: snapshot.ProviderUUID(),
		blockHeight:  snapshot.BlockHeight(),
		leaseUUIDs:   membership,
		leaseItems:   leaseItems,
	}, nil
}

func (proof LegacyUpgradeChainProof) valid() bool {
	return canonicalLeaseUUID(proof.providerUUID) && proof.blockHeight > 0 &&
		proof.leaseUUIDs != nil && proof.leaseItems != nil &&
		len(proof.leaseUUIDs) == len(proof.leaseItems)
}

// LegacyUpgradePreflightSummary reports the coverage proved by a successful
// v0.13 placement database preflight.
type LegacyUpgradePreflightSummary struct {
	ConfiguredBackends int
	PlacementRows      int
	InventoryLeases    int
}

// LegacyUpgradeInspector holds a read-only shared lock on a stopped v0.13
// placement database. While it is open, a read-write Store (including
// providerd) cannot open the file and cross the one-time lifecycle migration
// boundary.
type LegacyUpgradeInspector struct {
	db        *bolt.DB
	authority *offlinePlacementAuthority
	closeOnce sync.Once
	closeErr  error
}

// LegacyUpgradePreparer holds the exclusive read-write lock required to prove,
// back up, and atomically seal a stopped v0.13 database. It never creates the
// source database.
type LegacyUpgradePreparer struct {
	db                *bolt.DB
	sourceInfo        os.FileInfo
	authority         *offlinePlacementAuthority
	createExactBackup func(*bolt.DB, os.FileInfo, *ExactBackupTarget) error
	updateDB          func(func(*bolt.Tx) error) error
	syncDB            func() error
	now               func() time.Time
	closeOnce         sync.Once
	closeErr          error
}

// PreparedAuthorityInspector owns the post-reopen verification contract for a
// successful legacy preparation. It wraps the generic read-only repair
// inspector so opening still proves physical consistency, current schema, and
// provider binding without exposing legacy-preparation semantics on the repair
// API itself.
type PreparedAuthorityInspector struct {
	inspector *RepairInspector
}

// OpenPreparedAuthorityInspector reopens a definitely committed legacy
// preparation read-only and verifies its physical/current-schema/provider
// boundary before exact preparation postconditions can be checked.
func OpenPreparedAuthorityInspector(
	dbPath, providerUUID string,
) (*PreparedAuthorityInspector, error) {
	if err := requireAbsoluteCleanPlacementDBPath(dbPath); err != nil {
		return nil, err
	}
	inspector, err := OpenRepairInspector(dbPath, providerUUID)
	if err != nil {
		return nil, err
	}
	return &PreparedAuthorityInspector{inspector: inspector}, nil
}

// Close releases the inspector's shared offline database lock. It is
// idempotent.
func (inspector *PreparedAuthorityInspector) Close() error {
	if inspector == nil || inspector.inspector == nil {
		return nil
	}
	return inspector.inspector.Close()
}

// OpenLegacyUpgradeInspector opens an existing placement database read-only.
// It never creates a file or bucket. A one-second lock timeout makes an active
// providerd fail the preflight instead of leaving an operator command blocked.
func OpenLegacyUpgradeInspector(dbPath string) (*LegacyUpgradeInspector, error) {
	authority, err := bindOfflinePlacementAuthority(dbPath)
	if err != nil {
		return nil, err
	}
	if authority.info.Size() == 0 {
		_ = authority.close()
		return nil, fmt.Errorf("%w: placement database is empty", ErrLegacyUpgradePreflight)
	}
	db, err := authority.openBolt(true)
	if err != nil {
		_ = authority.close()
		if errors.Is(err, bolt.ErrTimeout) {
			return nil, fmt.Errorf("lock stopped placement db read-only (is providerd still running?): %w", err)
		}
		return nil, fmt.Errorf("open placement db read-only: %w", err)
	}
	return &LegacyUpgradeInspector{db: db, authority: authority}, nil
}

func openBoundLegacyUpgradeBackupInspector(
	target *ExactBackupTarget,
) (*LegacyUpgradeInspector, error) {
	if err := target.Verify(); err != nil {
		return nil, fmt.Errorf("verify bound legacy backup parent: %w", err)
	}
	expected, err := target.publishedIdentity()
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(target.Path(), 0o600, &bolt.Options{
		ReadOnly: true,
		Timeout:  time.Second,
		OpenFile: func(_ string, flag int, mode os.FileMode) (*os.File, error) {
			file, openErr := target.entry.OpenFile(flag&^os.O_CREATE, mode)
			if openErr != nil {
				return nil, openErr
			}
			openedInfo, statErr := file.Stat()
			if statErr != nil || !openedInfo.Mode().IsRegular() ||
				!os.SameFile(expected, openedInfo) {
				_ = file.Close()
				if statErr != nil {
					return nil, fmt.Errorf("stat opened bound legacy backup: %w", statErr)
				}
				return nil, errors.New("bound legacy backup changed between publication and open")
			}
			return file, nil
		},
	})
	if err != nil {
		if errors.Is(err, bolt.ErrTimeout) {
			return nil, fmt.Errorf("lock bound legacy backup read-only: %w", err)
		}
		return nil, fmt.Errorf("open bound legacy backup read-only: %w", err)
	}
	if err := target.Verify(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("reverify bound legacy backup parent after open: %w", err)
	}
	return &LegacyUpgradeInspector{db: db}, nil
}

// OpenLegacyUpgradePreparer opens an existing stopped database exclusively.
// A one-second timeout turns an active providerd (or inspector) into a clear
// failure rather than waiting indefinitely.
func OpenLegacyUpgradePreparer(dbPath string) (*LegacyUpgradePreparer, error) {
	authority, err := bindOfflinePlacementAuthority(dbPath)
	if err != nil {
		return nil, err
	}
	// bbolt initializes an existing zero-length file even when O_CREATE is
	// stripped from its read-write open. Reject it before bolt.Open so a failed
	// preparation cannot manufacture bytes before the proof begins.
	if authority.info.Size() == 0 {
		_ = authority.close()
		return nil, fmt.Errorf("%w: placement database is empty", ErrLegacyUpgradePreflight)
	}
	db, err := authority.openBolt(false)
	if err != nil {
		_ = authority.close()
		if errors.Is(err, bolt.ErrTimeout) {
			return nil, fmt.Errorf("lock stopped placement db exclusively (is providerd still running?): %w", err)
		}
		return nil, fmt.Errorf("open placement db for preparation: %w", err)
	}
	preparer := &LegacyUpgradePreparer{
		db:         db,
		sourceInfo: authority.info,
		authority:  authority,
		updateDB: func(mutate func(*bolt.Tx) error) error {
			return updateBoltWithExplicitOutcome(db, mutate)
		},
		syncDB: db.Sync,
		now:    time.Now,
	}
	preparer.createExactBackup = func(
		opened *bolt.DB,
		_ os.FileInfo,
		target *ExactBackupTarget,
	) error {
		return writeBoundExactBackup(opened, authority, target)
	}
	return preparer, nil
}

// Close releases the inspector's read-only database lock. It is idempotent.
func (inspector *LegacyUpgradeInspector) Close() error {
	if inspector == nil || inspector.db == nil {
		return nil
	}
	inspector.closeOnce.Do(func() {
		inspector.closeErr = errors.Join(inspector.db.Close(), inspector.authority.close())
	})
	return inspector.closeErr
}

// Close releases the preparer's exclusive database lock. It is idempotent.
func (preparer *LegacyUpgradePreparer) Close() error {
	if preparer == nil || preparer.db == nil {
		return nil
	}
	preparer.closeOnce.Do(func() {
		preparer.closeErr = errors.Join(preparer.db.Close(), preparer.authority.close())
	})
	return preparer.closeErr
}

// AuthorizePreparation converts the operator's exact causal-drain attestation
// into short-lived authority for one immutable migration target. The expiry is
// never later than two minutes and is shortened to the caller's context
// deadline when present.
func (preparer *LegacyUpgradePreparer) AuthorizePreparation(
	ctx context.Context,
	providerUUID string,
	configuredBackends []string,
	inventories map[string]BackendInventory,
	chainProof LegacyUpgradeChainProof,
	backupTarget *ExactBackupTarget,
	attestation string,
) (LegacyPreparationCapability, error) {
	if preparer == nil || preparer.db == nil {
		return LegacyPreparationCapability{}, errors.New("legacy upgrade preparer is not open")
	}
	if ctx == nil {
		return LegacyPreparationCapability{}, errors.New("legacy upgrade preparation context is required")
	}
	if err := ctx.Err(); err != nil {
		return LegacyPreparationCapability{}, fmt.Errorf(
			"%w: preparation proof context is already done: %w",
			ErrLegacyPreparationCapability,
			err,
		)
	}
	if ctx.Done() == nil {
		return LegacyPreparationCapability{}, fmt.Errorf(
			"%w: preparation proof context must have a cancellable lifetime",
			ErrLegacyPreparationCapability,
		)
	}
	if attestation != LegacyPreparationDrainAttestation {
		return LegacyPreparationCapability{}, fmt.Errorf(
			"%w: causal drain attestation must exactly equal %q",
			ErrLegacyPreparationCapability,
			LegacyPreparationDrainAttestation,
		)
	}
	if !backupTarget.valid() {
		return LegacyPreparationCapability{}, fmt.Errorf(
			"%w: bound exact backup target is required",
			ErrLegacyPreparationCapability,
		)
	}
	if err := backupTarget.Verify(); err != nil {
		return LegacyPreparationCapability{}, fmt.Errorf(
			"%w: verify bound exact backup target: %w",
			ErrLegacyPreparationCapability,
			err,
		)
	}
	digest, err := preparer.legacyPreparationTargetDigest(
		providerUUID, configuredBackends, inventories, chainProof, backupTarget,
	)
	if err != nil {
		return LegacyPreparationCapability{}, fmt.Errorf("%w: %w", ErrLegacyPreparationCapability, err)
	}
	expiresAt := preparer.currentTime().Add(legacyPreparationCapabilityMaxAge)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(expiresAt) {
		expiresAt = deadline
	}
	return LegacyPreparationCapability{
		issuer: preparer, targetDigest: digest, proofCtx: ctx,
		backupTarget: backupTarget, expiresAt: expiresAt, consumed: &atomic.Bool{},
	}, nil
}

func (capability LegacyPreparationCapability) validate(
	ctx context.Context,
	preparer *LegacyUpgradePreparer,
	providerUUID string,
	configuredBackends []string,
	inventories map[string]BackendInventory,
	chainProof LegacyUpgradeChainProof,
) error {
	if ctx == nil {
		return fmt.Errorf("%w: preparation context is required", ErrLegacyPreparationCapability)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf(
			"%w: preparation proof context expired: %w",
			ErrLegacyPreparationCapability,
			err,
		)
	}
	if capability.issuer == nil || capability.issuer != preparer ||
		capability.targetDigest == ([sha256.Size]byte{}) || capability.expiresAt.IsZero() ||
		capability.backupTarget == nil || capability.consumed == nil {
		return fmt.Errorf("%w: capability is absent or belongs to another preparer", ErrLegacyPreparationCapability)
	}
	if capability.proofCtx == nil {
		return fmt.Errorf("%w: capability has no proof context", ErrLegacyPreparationCapability)
	}
	if capability.proofCtx.Done() == nil || ctx.Done() != capability.proofCtx.Done() {
		return fmt.Errorf(
			"%w: preparation must use the exact proof cancellation scope",
			ErrLegacyPreparationCapability,
		)
	}
	if err := capability.proofCtx.Err(); err != nil {
		return fmt.Errorf(
			"%w: original preparation proof context expired: %w",
			ErrLegacyPreparationCapability,
			err,
		)
	}
	if !preparer.currentTime().Before(capability.expiresAt) {
		return fmt.Errorf("%w: capability expired at %s",
			ErrLegacyPreparationCapability, capability.expiresAt.UTC().Format(time.RFC3339Nano))
	}
	verifyBackupTarget := capability.backupTarget.Verify
	if capability.consumed.Load() {
		verifyBackupTarget = capability.backupTarget.VerifyPublished
	}
	if err := verifyBackupTarget(); err != nil {
		return fmt.Errorf("%w: bound exact backup target changed: %w",
			ErrLegacyPreparationCapability, err)
	}
	digest, err := preparer.legacyPreparationTargetDigest(
		providerUUID, configuredBackends, inventories, chainProof, capability.backupTarget,
	)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrLegacyPreparationCapability, err)
	}
	if digest != capability.targetDigest {
		return fmt.Errorf(
			"%w: provider, database, topology/storage evidence, chain snapshot, or backup target changed",
			ErrLegacyPreparationCapability,
		)
	}
	return nil
}

func (capability LegacyPreparationCapability) requireUnconsumed() error {
	if capability.consumed == nil {
		return fmt.Errorf("%w: capability is absent", ErrLegacyPreparationCapability)
	}
	if capability.consumed.Load() {
		return fmt.Errorf("%w: capability was already consumed at backup publication admission",
			ErrLegacyPreparationCapability)
	}
	return nil
}

func (capability LegacyPreparationCapability) consume() error {
	if capability.consumed == nil || !capability.consumed.CompareAndSwap(false, true) {
		return fmt.Errorf("%w: capability was already consumed at backup publication admission",
			ErrLegacyPreparationCapability)
	}
	return nil
}

func (preparer *LegacyUpgradePreparer) currentTime() time.Time {
	if preparer != nil && preparer.now != nil {
		return preparer.now()
	}
	return time.Now()
}

func (preparer *LegacyUpgradePreparer) legacyPreparationTargetDigest(
	providerUUID string,
	configuredBackends []string,
	inventories map[string]BackendInventory,
	chainProof LegacyUpgradeChainProof,
	backupTarget *ExactBackupTarget,
) ([sha256.Size]byte, error) {
	if !canonicalLeaseUUID(providerUUID) {
		return [sha256.Size]byte{}, fmt.Errorf("provider UUID %q is not canonical", providerUUID)
	}
	if !chainProof.valid() || chainProof.providerUUID != providerUUID {
		return [sha256.Size]byte{}, errors.New("height-pinned provider chain proof is incomplete or mismatched")
	}
	if preparer.authority != nil {
		if err := preparer.authority.verify(); err != nil {
			return [sha256.Size]byte{}, fmt.Errorf("verify bound placement database: %w", err)
		}
	}
	databasePath := preparer.db.Path()
	if !backupTarget.valid() {
		return [sha256.Size]byte{}, errors.New("bound exact backup target is required")
	}
	if err := backupTarget.Verify(); err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("verify bound exact backup target: %w", err)
	}
	canonicalBackupPath := backupTarget.Path()
	if databasePath == canonicalBackupPath {
		return [sha256.Size]byte{}, errors.New("placement database and backup paths resolve to the same target")
	}
	backupParent := backupTarget.ParentIdentity()
	if !backupParent.Valid() {
		return [sha256.Size]byte{}, errors.New("bound exact backup parent identity is invalid")
	}
	canonicalBackends, err := canonicalBackendTopology(configuredBackends)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	if len(inventories) != len(canonicalBackends) {
		return [sha256.Size]byte{}, errors.New("backend inventory count does not match topology")
	}
	backendTargets := make([]legacyPreparationBackendTarget, 0, len(canonicalBackends))
	for _, backendName := range canonicalBackends {
		inventory, ok := inventories[backendName]
		if !ok || !inventory.StorageIdentity.Valid() {
			return [sha256.Size]byte{}, fmt.Errorf("backend %q has no valid storage identity", backendName)
		}
		providerKeys := slices.Sorted(maps.Keys(inventory.ProvisionProviderUUIDs))
		providers := make([]legacyPreparationProviderObservation, 0, len(providerKeys))
		for _, leaseUUID := range providerKeys {
			providers = append(providers, legacyPreparationProviderObservation{
				LeaseUUID: leaseUUID, ProviderUUID: inventory.ProvisionProviderUUIDs[leaseUUID],
			})
		}
		itemKeys := slices.Sorted(maps.Keys(inventory.ProvisionItems))
		provisionItems := make([]legacyPreparationLeaseItems, 0, len(itemKeys))
		for _, leaseUUID := range itemKeys {
			if err := validatePreflightWorkloadItems(inventory.ProvisionItems[leaseUUID]); err != nil {
				return [sha256.Size]byte{}, fmt.Errorf(
					"backend %q provision %q has invalid workload evidence: %w",
					backendName,
					leaseUUID,
					err,
				)
			}
			provisionItems = append(provisionItems, legacyPreparationLeaseItems{
				LeaseUUID: leaseUUID,
				Items:     canonicalPreflightWorkloadItems(inventory.ProvisionItems[leaseUUID]),
			})
		}
		provisions := slices.Clone(inventory.Provisions)
		retentions := slices.Clone(inventory.Retentions)
		slices.Sort(provisions)
		slices.Sort(retentions)
		backendTargets = append(backendTargets, legacyPreparationBackendTarget{
			Name:               backendName,
			StorageIdentity:    inventory.StorageIdentity.String(),
			Provisions:         provisions,
			ProvisionProviders: providers,
			ProvisionItems:     provisionItems,
			Retentions:         retentions,
		})
	}
	chainLeases := slices.Sorted(maps.Keys(chainProof.leaseUUIDs))
	chainItems := make([]legacyPreparationLeaseItems, 0, len(chainLeases))
	for _, leaseUUID := range chainLeases {
		items := chainProof.leaseItems[leaseUUID]
		if err := validateRawChainItemsForDigest(items); err != nil {
			return [sha256.Size]byte{}, fmt.Errorf(
				"chain lease %q has unsafe workload evidence: %w",
				leaseUUID,
				err,
			)
		}
		chainItems = append(chainItems, legacyPreparationLeaseItems{
			LeaseUUID: leaseUUID,
			// Preserve the ledger's exact item order and values. Historical
			// terminal leases are membership evidence only and may predate current
			// service-name or aggregate-quantity rules; normalizing them would
			// silently reinterpret history while constructing the capability.
			Items: cloneLeaseItems(items),
		})
	}
	encoded, err := json.Marshal(legacyPreparationTarget{
		ProviderUUID:       providerUUID,
		DatabasePath:       databasePath,
		BackupPath:         canonicalBackupPath,
		BackupParentDevice: backupParent.Device,
		BackupParentInode:  backupParent.Inode,
		Backends:           backendTargets,
		ChainHeight:        chainProof.blockHeight,
		ChainLeases:        chainLeases,
		ChainItems:         chainItems,
	})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("encode preparation capability target: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func requireAbsoluteCleanPlacementDBPath(path string) error {
	if path == "" {
		return errors.New("placement db path is required")
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("placement db path must be absolute and clean: %q", path)
	}
	return nil
}

func canonicalNewPlacementPath(path string) (string, error) {
	if path == "" || filepath.Clean(path) != path {
		return "", errors.New("path must be non-empty and clean")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolvedParent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", err
	}
	return filepath.Join(resolvedParent, filepath.Base(absolute)), nil
}

// Check proves that the held database is still at the v0.13 first-open
// boundary, exactly covers one complete inventory from every configured
// backend, and binds every survivor to the configured provider in one pinned
// all-state chain snapshot. Every discrepancy is returned deterministically and
// no transaction opened by this method can write.
func (inspector *LegacyUpgradeInspector) Check(
	providerUUID string,
	configuredBackends []string,
	inventories map[string]BackendInventory,
	chainProof LegacyUpgradeChainProof,
) (LegacyUpgradePreflightSummary, error) {
	return inspector.CheckContext(
		context.Background(), providerUUID, configuredBackends, inventories, chainProof,
	)
}

// CheckContext is Check with cancellation support. Cancellation is returned as
// a context error, not as ErrLegacyUpgradePreflight: it means the proof did not
// finish, rather than proving a discrepancy in the stopped database, backend
// inventories, or chain snapshot.
func (inspector *LegacyUpgradeInspector) CheckContext(
	ctx context.Context,
	providerUUID string,
	configuredBackends []string,
	inventories map[string]BackendInventory,
	chainProof LegacyUpgradeChainProof,
) (LegacyUpgradePreflightSummary, error) {
	if inspector == nil || inspector.db == nil {
		return LegacyUpgradePreflightSummary{}, fmt.Errorf("legacy upgrade inspector is not open")
	}
	if ctx == nil {
		return LegacyUpgradePreflightSummary{}, fmt.Errorf("legacy upgrade preflight context is required")
	}
	if err := ctx.Err(); err != nil {
		return LegacyUpgradePreflightSummary{}, fmt.Errorf("legacy upgrade preflight interrupted: %w", err)
	}
	if inspector.authority != nil {
		if err := inspector.authority.verify(); err != nil {
			return LegacyUpgradePreflightSummary{}, fmt.Errorf("verify stopped placement authority: %w", err)
		}
	}
	if err := verifyBoltPhysicalConsistency(inspector.db); err != nil {
		return LegacyUpgradePreflightSummary{}, fmt.Errorf(
			"validate placement db physical consistency: %w", err,
		)
	}

	problems := newPreflightProblems()
	if !canonicalLeaseUUID(providerUUID) {
		problems.add("configured provider UUID %q is not canonical", providerUUID)
	}
	if !chainProof.valid() {
		problems.add("height-pinned all-state provider chain proof is incomplete")
	} else if chainProof.providerUUID != providerUUID {
		problems.add(
			"chain proof belongs to provider %q, configured provider is %q",
			chainProof.providerUUID,
			providerUUID,
		)
	}
	canonicalBackends, topologyErr := canonicalBackendTopology(configuredBackends)
	if topologyErr != nil {
		problems.add("configured backend topology is invalid: %v", topologyErr)
	}
	configured := make(map[string]struct{}, len(canonicalBackends))
	for _, backendName := range canonicalBackends {
		configured[backendName] = struct{}{}
	}

	observedOwners, inventoryErr := inspectPreflightInventories(
		ctx, providerUUID, canonicalBackends, configured, inventories, problems,
	)
	if inventoryErr != nil {
		return LegacyUpgradePreflightSummary{}, fmt.Errorf("inspect backend inventories: %w", inventoryErr)
	}
	placements := make(map[string]string)
	summary := LegacyUpgradePreflightSummary{
		ConfiguredBackends: len(canonicalBackends),
		InventoryLeases:    len(observedOwners),
	}
	viewErr := inspector.db.View(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if tx.Bucket(lifecycleCapabilityBucketName) != nil {
			problems.add("bucket %q already exists", string(lifecycleCapabilityBucketName))
		}
		if tx.Bucket(metadataBucketName) != nil {
			problems.add("bucket %q already exists", string(metadataBucketName))
		}

		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			problems.add("bucket %q is missing", string(bucketName))
			return nil
		}
		return bucket.ForEach(func(key, value []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			summary.PlacementRows++
			leaseUUID := string(key)
			if leaseUUID == "" {
				problems.add("placement row has an empty lease identity")
				return nil
			}
			if !canonicalLeaseUUID(leaseUUID) {
				problems.add("placement row key %q is not a canonical non-nil UUID", leaseUUID)
			}
			if value == nil {
				problems.add("placement %q is a nested bucket, not a v0.13 row", leaseUUID)
				return nil
			}

			if revision, object, headerErr := preflightRevisionHeader(value); object {
				switch {
				case headerErr != nil:
					problems.add("placement %q has an undecodable global revision header: %v", leaseUUID, headerErr)
				case revision != 0:
					problems.add("placement %q is already revisioned (revision=%d)", leaseUUID, revision)
				}
			}

			backendName, decodeErr := decodeV013PreflightPlacement(value)
			if decodeErr != nil {
				problems.add("placement %q is not an unambiguous v0.13 confirmed owner: %v", leaseUUID, decodeErr)
				return nil
			}
			placements[leaseUUID] = backendName
			if _, ok := configured[backendName]; !ok {
				problems.add("placement %q names unconfigured backend %q", leaseUUID, backendName)
			}
			return nil
		})
	})
	if viewErr != nil {
		return summary, fmt.Errorf("inspect placement db read-only: %w", viewErr)
	}

	if err := inspectPreflightCoverage(ctx, placements, observedOwners, problems); err != nil {
		return summary, fmt.Errorf("compare placement and backend inventory coverage: %w", err)
	}
	if err := inspectPreflightChainMembership(
		ctx, providerUUID, placements, observedOwners, inventories, chainProof, problems,
	); err != nil {
		return summary, fmt.Errorf("compare placement and chain provider membership: %w", err)
	}
	if err := problems.err(); err != nil {
		return summary, err
	}
	if inspector.authority != nil {
		if err := inspector.authority.verify(); err != nil {
			return summary, fmt.Errorf("reverify stopped placement authority: %w", err)
		}
	}
	return summary, nil
}

// PrepareContext requires a matching short-lived causal-drain capability,
// reruns the complete read-only proof under the same exclusive lock, publishes
// an exact no-overwrite backup, then performs the standard lifecycle/revision
// migration and installs the observed topology identities in one bbolt
// transaction. The admission baseline deliberately remains empty: the first
// upgraded full-fleet reconciliation must establish it.
func (preparer *LegacyUpgradePreparer) PrepareContext(
	ctx context.Context,
	providerUUID string,
	configuredBackends []string,
	inventories map[string]BackendInventory,
	chainProof LegacyUpgradeChainProof,
	capability LegacyPreparationCapability,
) (LegacyUpgradePreflightSummary, error) {
	if preparer == nil || preparer.db == nil {
		return LegacyUpgradePreflightSummary{}, fmt.Errorf("legacy upgrade preparer is not open")
	}
	if ctx == nil {
		return LegacyUpgradePreflightSummary{}, fmt.Errorf("legacy upgrade preparation context is required")
	}
	if err := capability.validate(
		ctx, preparer, providerUUID, configuredBackends, inventories, chainProof,
	); err != nil {
		return LegacyUpgradePreflightSummary{}, err
	}
	backupPath := capability.backupTarget.Path()
	if err := capability.requireUnconsumed(); err != nil {
		return LegacyUpgradePreflightSummary{}, err
	}
	inspector := &LegacyUpgradeInspector{db: preparer.db, authority: preparer.authority}
	summary, err := inspector.CheckContext(
		ctx, providerUUID, configuredBackends, inventories, chainProof,
	)
	if err != nil {
		return summary, err
	}
	canonical, err := canonicalBackendTopology(configuredBackends)
	if err != nil {
		return summary, err
	}
	fingerprint, err := topologyFingerprint(canonical)
	if err != nil {
		return summary, err
	}
	storageIDs := make(map[string]string, len(canonical))
	for _, backendName := range canonical {
		inventory, ok := inventories[backendName]
		if !ok || !inventory.StorageIdentity.Valid() {
			return summary, fmt.Errorf("%w: backend %q has no valid storage identity",
				ErrLegacyUpgradePreflight, backendName)
		}
		storageIDs[backendName] = inventory.StorageIdentity.String()
	}
	if err := ctx.Err(); err != nil {
		return summary, fmt.Errorf(
			"legacy placement proof expired before exact backup; no backup or preparation mutation was attempted: %w",
			err,
		)
	}
	if err := capability.validate(
		ctx, preparer, providerUUID, configuredBackends, inventories, chainProof,
	); err != nil {
		return summary, fmt.Errorf("preparation capability before exact backup: %w", err)
	}
	if err := verifyBoltPhysicalConsistency(preparer.db); err != nil {
		return summary, fmt.Errorf("validate placement db before exact backup: %w", err)
	}
	// Consumption is the admission boundary for the first externally visible
	// side effect. Any failure from this point onward requires a fresh inventory,
	// chain proof, drain attestation, capability, and backup destination; copies
	// of this opaque value share the same one-shot bit.
	if err := capability.consume(); err != nil {
		return summary, err
	}
	createExactBackup := preparer.createExactBackup
	if createExactBackup == nil {
		createExactBackup = writeExactBackup
	}
	if err := createExactBackup(preparer.db, preparer.sourceInfo, capability.backupTarget); err != nil {
		return summary, fmt.Errorf(
			"back up legacy placement db after capability consumption; rerun the proof before retrying: %w",
			err,
		)
	}
	if err := requireFreshLegacyPreparationProof(ctx, backupPath); err != nil {
		return summary, err
	}
	if err := capability.validate(
		ctx, preparer, providerUUID, configuredBackends, inventories, chainProof,
	); err != nil {
		return summary, legacyPreparationCapabilityRejectedAfterBackup(backupPath, err)
	}
	backupInspector, err := openBoundLegacyUpgradeBackupInspector(capability.backupTarget)
	if err != nil {
		return summary, exactBackupPublishedError("reopen legacy placement backup", err)
	}
	backupSummary, verifyErr := backupInspector.CheckContext(
		ctx, providerUUID, configuredBackends, inventories, chainProof,
	)
	closeErr := backupInspector.Close()
	if verifyErr != nil {
		if errors.Is(verifyErr, context.Canceled) || errors.Is(verifyErr, context.DeadlineExceeded) {
			return summary, legacyPreparationProofExpiredAfterBackup(backupPath, verifyErr)
		}
		return summary, exactBackupPublishedError("validate legacy placement backup", verifyErr)
	}
	if closeErr != nil {
		return summary, exactBackupPublishedError("close validated placement backup", closeErr)
	}
	if backupSummary != summary {
		return summary, exactBackupPublishedError(
			"validate legacy placement backup summary",
			errors.New("validated placement backup summary differs from source"),
		)
	}
	if err := requireFreshLegacyPreparationProof(ctx, backupPath); err != nil {
		return summary, err
	}
	if err := capability.validate(
		ctx, preparer, providerUUID, configuredBackends, inventories, chainProof,
	); err != nil {
		return summary, legacyPreparationCapabilityRejectedAfterBackup(backupPath, err)
	}
	updateDB := preparer.updateDB
	if updateDB == nil {
		updateDB = func(mutate func(*bolt.Tx) error) error {
			return updateBoltWithExplicitOutcome(preparer.db, mutate)
		}
	}
	if err := updateDB(func(tx *bolt.Tx) error {
		if err := capability.validate(
			ctx, preparer, providerUUID, configuredBackends, inventories, chainProof,
		); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		epoch, err := captureLifecycleInitializationEpoch(tx)
		if err != nil {
			return err
		}
		if !epoch.permitsLegacyAdoption() {
			return fmt.Errorf("%w: database is no longer at the v0.13 migration boundary",
				ErrLegacyUpgradePreflight)
		}
		if tx.Bucket(bucketName) == nil {
			return fmt.Errorf("%w: placements bucket is missing", ErrLegacyUpgradePreflight)
		}
		if err := initializeMetadata(tx); err != nil {
			return err
		}
		metadata := topologyMetadata{
			Schema:                 topologyMetadataSchema,
			ProviderUUID:           providerUUID,
			Topology:               slices.Clone(canonical),
			TopologyFingerprint:    fingerprint,
			KnownBackends:          slices.Clone(canonical),
			KnownBackendStorageIDs: storageIDs,
			TopologyID:             1,
		}
		if err := putTopologyMetadata(tx, metadata); err != nil {
			return err
		}
		if err := initializeLifecycleCapabilities(tx, epoch); err != nil {
			return err
		}
		if err := migrateLegacyConfirmedRevisions(tx); err != nil {
			return err
		}
		if err := pruneDetachedRetiredLifecycleCapabilities(tx); err != nil {
			return err
		}
		// The proof deadline is an admission fence, not a cancellation promise
		// for local bbolt work. Refuse to commit if the evidence expired while
		// the transaction was being assembled.
		if err := capability.validate(
			ctx, preparer, providerUUID, configuredBackends, inventories, chainProof,
		); err != nil {
			return err
		}
		return ctx.Err()
	}); err != nil {
		if err = classifyLegacyPreparationTransactionError(err); errors.Is(
			err, ErrLegacyPreparationOutcomeUnknown,
		) {
			return summary, err
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return summary, legacyPreparationProofExpiredAfterBackup(backupPath, err)
		}
		if errors.Is(err, ErrLegacyPreparationCapability) {
			return summary, legacyPreparationCapabilityRejectedAfterBackup(backupPath, err)
		}
		return summary, exactBackupPublishedError(
			"prepare legacy placement db before commit", err,
		)
	}
	if err := capability.backupTarget.VerifyPublished(); err != nil {
		return summary, fmt.Errorf(
			"%w: exact legacy backup authority changed after preparation commit: %w",
			ErrLegacyPreparationCommitted,
			err,
		)
	}
	syncDB := preparer.syncDB
	if syncDB == nil {
		syncDB = preparer.db.Sync
	}
	if err := syncDB(); err != nil {
		return summary, fmt.Errorf(
			"%w: sync prepared placement db: %w",
			ErrLegacyPreparationCommitted,
			err,
		)
	}
	if err := verifyBoltPhysicalConsistency(preparer.db); err != nil {
		return summary, fmt.Errorf(
			"%w: validate prepared placement db physical consistency: %w",
			ErrLegacyPreparationCommitted,
			err,
		)
	}
	if err := verifyLegacyPreparationPostcondition(
		preparer.db, providerUUID, canonical, storageIDs, inventories, summary,
	); err != nil {
		return summary, fmt.Errorf(
			"%w: validate prepared placement db semantic postcondition: %w",
			ErrLegacyPreparationCommitted,
			err,
		)
	}
	if err := capability.backupTarget.VerifyPublished(); err != nil {
		return summary, fmt.Errorf(
			"%w: exact legacy backup authority changed during postcondition verification: %w",
			ErrLegacyPreparationCommitted,
			err,
		)
	}
	if preparer.authority != nil {
		if err := preparer.authority.verify(); err != nil {
			return summary, fmt.Errorf(
				"%w: reverify prepared placement authority: %w",
				ErrLegacyPreparationCommitted,
				err,
			)
		}
	}
	return summary, nil
}

func classifyLegacyPreparationTransactionError(err error) error {
	if errors.Is(err, errBoltCommitOutcomeUnknown) {
		return fmt.Errorf(
			"%w: prepare legacy placement db: %w",
			ErrLegacyPreparationOutcomeUnknown,
			err,
		)
	}
	return err
}

func verifyLegacyPreparationPostcondition(
	db *bolt.DB,
	providerUUID string,
	canonicalBackends []string,
	storageIDs map[string]string,
	inventories map[string]BackendInventory,
	wantSummary LegacyUpgradePreflightSummary,
) error {
	expectedOwners, err := legacyPreparationExpectedOwners(canonicalBackends, inventories)
	if err != nil {
		return err
	}
	fingerprint, err := topologyFingerprint(canonicalBackends)
	if err != nil {
		return err
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
		if metadata.ProviderUUID != providerUUID ||
			!slices.Equal(metadata.Topology, canonicalBackends) ||
			metadata.TopologyFingerprint != fingerprint ||
			!slices.Equal(metadata.KnownBackends, canonicalBackends) ||
			!maps.Equal(metadata.KnownBackendStorageIDs, storageIDs) ||
			metadata.TopologyID != 1 || metadata.BaselineFingerprint != "" ||
			metadata.BaselineTopologyID != 0 || metadata.InventoryTopologyID != 0 ||
			len(metadata.EmptyInventoryBackends) != 0 {
			return errors.New("prepared provider, topology, or backend storage identities differ from proof")
		}
		placements := tx.Bucket(bucketName)
		capabilities := tx.Bucket(lifecycleCapabilityBucketName)
		rows := 0
		seen := make(map[string]struct{}, len(expectedOwners))
		if err := placements.ForEach(func(key, value []byte) error {
			if value == nil {
				return fmt.Errorf("placement %q is a nested bucket", key)
			}
			rows++
			leaseUUID := string(key)
			expectedBackend, expected := expectedOwners[leaseUUID]
			if !expected {
				return fmt.Errorf("prepared placement %q was absent from the verified inventory", leaseUUID)
			}
			placement := decodeRecord(leaseUUID, value)
			if placement.State() != StateConfirmed || placement.revision == 0 ||
				placement.Attempt != "" || placement.Conflict || placement.Backend != expectedBackend {
				return fmt.Errorf("placement %q is not one revisioned confirmed owner", leaseUUID)
			}
			seen[leaseUUID] = struct{}{}
			rawCapability := capabilities.Get(key)
			capability, err := decodeLifecycleCapability(rawCapability)
			if err != nil || capability.unusable || capability.retired ||
				capability.backend != placement.Backend || capability.id.Valid() ||
				capability.attemptBackend != "" || capability.attemptID.Valid() {
				return fmt.Errorf("placement %q has invalid migrated legacy lifecycle authority", leaseUUID)
			}
			return nil
		}); err != nil {
			return err
		}
		if rows != wantSummary.PlacementRows {
			return fmt.Errorf("prepared placement row count is %d, want %d", rows, wantSummary.PlacementRows)
		}
		if len(expectedOwners) != wantSummary.InventoryLeases || len(seen) != len(expectedOwners) {
			return fmt.Errorf(
				"prepared exact owner membership is %d rows, want %d verified inventory leases",
				len(seen), len(expectedOwners),
			)
		}
		lifecycleRows := 0
		if err := capabilities.ForEach(func(key, value []byte) error {
			if value == nil {
				return fmt.Errorf("lifecycle capability %q is a nested bucket", key)
			}
			if _, expected := expectedOwners[string(key)]; !expected {
				return fmt.Errorf("lifecycle capability %q was absent from the verified inventory", key)
			}
			lifecycleRows++
			return nil
		}); err != nil {
			return err
		}
		if lifecycleRows != len(expectedOwners) {
			return fmt.Errorf("prepared lifecycle row count is %d, want %d", lifecycleRows, len(expectedOwners))
		}
		return nil
	})
}

func legacyPreparationExpectedOwners(
	canonicalBackends []string,
	inventories map[string]BackendInventory,
) (map[string]string, error) {
	if len(inventories) != len(canonicalBackends) {
		return nil, errors.New("verified inventory count differs from prepared topology")
	}
	owners := make(map[string]string)
	for _, backendName := range canonicalBackends {
		inventory, ok := inventories[backendName]
		if !ok {
			return nil, fmt.Errorf("verified inventory for backend %q is missing", backendName)
		}
		for _, leases := range [][]string{inventory.Provisions, inventory.Retentions} {
			for _, leaseUUID := range leases {
				if !canonicalLeaseUUID(leaseUUID) {
					return nil, fmt.Errorf("verified inventory contains non-canonical lease %q", leaseUUID)
				}
				if owner, duplicate := owners[leaseUUID]; duplicate {
					return nil, fmt.Errorf("verified inventory reports lease %q on both %q and %q",
						leaseUUID, owner, backendName)
				}
				owners[leaseUUID] = backendName
			}
		}
	}
	return owners, nil
}

// VerifyLegacyPreparationPostcondition revalidates the prepared authority from
// a newly opened read-only inspector. Opening the inspector already runs
// bbolt's physical checker and verifies the provider-bound current schema; this
// method proves the exact topology, storage pins, migrated rows, and lifecycle
// semantics that PrepareContext promised before a CLI prints PASS.
func (inspector *PreparedAuthorityInspector) VerifyLegacyPreparationPostcondition(
	providerUUID string,
	configuredBackends []string,
	inventories map[string]BackendInventory,
	wantSummary LegacyUpgradePreflightSummary,
) error {
	if inspector == nil || inspector.inspector == nil ||
		inspector.inspector.store == nil || inspector.inspector.store.db == nil {
		return errors.New("prepared authority inspector is not open")
	}
	canonical, err := canonicalBackendTopology(configuredBackends)
	if err != nil {
		return err
	}
	storageIDs := make(map[string]string, len(canonical))
	for _, backendName := range canonical {
		inventory, ok := inventories[backendName]
		if !ok || !inventory.StorageIdentity.Valid() {
			return fmt.Errorf("backend %q has no valid storage identity", backendName)
		}
		storageIDs[backendName] = inventory.StorageIdentity.String()
	}
	return verifyLegacyPreparationPostcondition(
		inspector.inspector.store.db,
		providerUUID,
		canonical,
		storageIDs,
		inventories,
		wantSummary,
	)
}

func requireFreshLegacyPreparationProof(ctx context.Context, backupPath string) error {
	if err := ctx.Err(); err != nil {
		return legacyPreparationProofExpiredAfterBackup(backupPath, err)
	}
	return nil
}

func legacyPreparationProofExpiredAfterBackup(backupPath string, cause error) error {
	return fmt.Errorf(
		"%w: legacy placement proof expired while exact backup %q was published; "+
			"no placement preparation transaction committed. Preserve that backup, "+
			"rerun the read-only preflight, and choose a new -backup path: %w",
		ErrExactBackupPublished,
		backupPath,
		cause,
	)
}

func legacyPreparationCapabilityRejectedAfterBackup(backupPath string, cause error) error {
	return fmt.Errorf(
		"%w: legacy placement preparation capability expired or changed while exact backup %q "+
			"was published; no placement preparation transaction committed. Preserve that "+
			"backup, rerun the read-only preflight and causal drain, and choose a new "+
			"-backup path: %w",
		ErrExactBackupPublished,
		backupPath,
		cause,
	)
}

func exactBackupPublishedError(stage string, cause error) error {
	return fmt.Errorf("%w: %s: %w", ErrExactBackupPublished, stage, cause)
}

func writeExactBackup(
	db *bolt.DB,
	sourceInfo os.FileInfo,
	target *ExactBackupTarget,
) error {
	if db == nil || sourceInfo == nil {
		return errors.New("placement db and source identity are required")
	}
	authority, err := bindOfflinePlacementAuthority(db.Path())
	if err != nil {
		return fmt.Errorf("bind placement db backup source: %w", err)
	}
	defer authority.close() //nolint:errcheck // the database close reports command durability
	if !os.SameFile(sourceInfo, authority.info) {
		return errors.New("placement db changed before exact backup")
	}
	return writeBoundExactBackup(db, authority, target)
}

// writeBoundExactBackup copies the already-open offline authority through its
// retained parent entry.  This keeps the rollback image bound to the inode
// locked by bbolt even if an attacker swaps the configured pathname.
func writeBoundExactBackup(
	db *bolt.DB,
	authority *offlinePlacementAuthority,
	target *ExactBackupTarget,
) (resultErr error) {
	published := false
	defer func() {
		resultErr = classifyExactBackupResult(published, resultErr)
	}()
	if db == nil {
		return errors.New("placement db is required")
	}
	if !authority.valid() {
		return errors.New("bound placement db source authority is required")
	}
	sourceInfo := authority.info
	if err := authority.verify(); err != nil {
		return fmt.Errorf("verify placement db backup source: %w", err)
	}
	if !target.valid() {
		return errors.New("bound exact backup target is required")
	}
	if err := verifyBoltPhysicalConsistency(db); err != nil {
		return fmt.Errorf("validate placement db before exact backup: %w", err)
	}
	if err := target.Verify(); err != nil {
		return fmt.Errorf("verify bound exact backup target: %w", err)
	}
	if exists, err := target.entry.Exists(); err != nil {
		return fmt.Errorf("inspect bound backup destination: %w", err)
	} else if exists {
		return errors.New("backup destination already exists")
	}
	tempName, temp, err := target.createTemporary()
	if err != nil {
		return fmt.Errorf("create backup temporary file: %w", err)
	}
	tempOpen := true
	defer func() {
		if tempOpen {
			if err := temp.Close(); resultErr == nil && err != nil {
				resultErr = fmt.Errorf("close backup temporary file: %w", err)
			}
		}
		if err := target.directory.Remove(tempName); resultErr == nil && err != nil && !errors.Is(err, os.ErrNotExist) {
			resultErr = fmt.Errorf("remove backup temporary file: %w", err)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return fmt.Errorf("set backup temporary file permissions: %w", err)
	}
	// The caller holds bbolt's exclusive stopped-database lock and has not yet
	// opened a write transaction. Copy the validated source inode byte-for-byte,
	// rather than Tx.WriteTo's logically equivalent snapshot: WriteTo rewrites
	// the two meta pages and therefore is not an exact old-binary rollback image.
	// Recheck the inode captured before bolt.Open so a pathname replacement can
	// never redirect the backup to different authority.
	source, err := authority.entry.OpenFile(os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open placement db for exact backup: %w", err)
	}
	defer func() {
		if err := source.Close(); resultErr == nil && err != nil {
			resultErr = fmt.Errorf("close placement db backup source: %w", err)
		}
	}()
	openedSourceInfo, err := source.Stat()
	if err != nil {
		return fmt.Errorf("stat placement db backup source: %w", err)
	}
	if !os.SameFile(sourceInfo, openedSourceInfo) {
		return errors.New("placement db changed between validation and exact backup")
	}
	snapshotHash := sha256.New()
	snapshotBytes, err := io.Copy(io.MultiWriter(temp, snapshotHash), source)
	if err != nil {
		return fmt.Errorf("copy exact placement database bytes: %w", err)
	}
	if snapshotBytes != openedSourceInfo.Size() {
		return fmt.Errorf(
			"copy exact placement database bytes: copied %d bytes, expected %d",
			snapshotBytes,
			openedSourceInfo.Size(),
		)
	}
	afterCopyInfo, err := source.Stat()
	if err != nil {
		return fmt.Errorf("restat placement db backup source: %w", err)
	}
	if !os.SameFile(sourceInfo, afterCopyInfo) ||
		afterCopyInfo.Size() != openedSourceInfo.Size() ||
		!afterCopyInfo.ModTime().Equal(openedSourceInfo.ModTime()) {
		return errors.New("placement db changed while exact backup was copied")
	}
	if err := authority.verify(); err != nil {
		return fmt.Errorf("reverify placement db backup source: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync backup temporary file: %w", err)
	}
	closeErr := temp.Close()
	tempOpen = false
	if closeErr != nil {
		return fmt.Errorf("close backup temporary file: %w", closeErr)
	}
	temporaryInfo, err := target.directory.Lstat(tempName)
	if err != nil {
		return fmt.Errorf("stat verified backup temporary file: %w", err)
	}
	if err := target.directory.RenameNoReplace(tempName, target.entry.Name()); err != nil {
		if errors.Is(err, os.ErrExist) {
			return errors.New("backup destination already exists")
		}
		return fmt.Errorf("publish placement backup: %w", err)
	}
	published = true
	if err := verifyPublishedEntryIdentity(target.entry, temporaryInfo); err != nil {
		return fmt.Errorf("verify exact backup publication identity: %w", err)
	}
	backup, err := target.entry.OpenFile(os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("open published placement backup: %w", err)
	}
	backupInfo, err := backup.Stat()
	if err != nil {
		_ = backup.Close()
		return fmt.Errorf("stat opened placement backup: %w", err)
	}
	if !os.SameFile(temporaryInfo, backupInfo) {
		_ = backup.Close()
		return errors.New("opened placement backup is not the verified source inode")
	}
	publishedHash := sha256.New()
	publishedBytes, err := io.Copy(publishedHash, backup)
	if err != nil {
		_ = backup.Close()
		return fmt.Errorf("verify published placement backup bytes: %w", err)
	}
	snapshotDigest := snapshotHash.Sum(nil)
	if publishedBytes != snapshotBytes ||
		!bytes.Equal(publishedHash.Sum(nil), snapshotDigest) {
		_ = backup.Close()
		return errors.New("published placement backup bytes differ from validated snapshot")
	}
	if err := backup.Sync(); err != nil {
		_ = backup.Close()
		return fmt.Errorf("sync published placement backup: %w", err)
	}
	if err := backup.Close(); err != nil {
		return fmt.Errorf("close published placement backup: %w", err)
	}
	if err := target.entry.SyncParent(); err != nil {
		return fmt.Errorf("sync backup directory: %w", err)
	}
	if err := verifyBoltEntryPhysicalConsistency(target.entry, temporaryInfo); err != nil {
		return fmt.Errorf("validate published placement backup physical consistency: %w", err)
	}
	if err := verifyPublishedEntryIdentity(target.entry, temporaryInfo); err != nil {
		return fmt.Errorf("reverify exact backup publication identity: %w", err)
	}
	if err := target.Verify(); err != nil {
		return fmt.Errorf("reverify bound exact backup parent after publication: %w", err)
	}
	var digest [sha256.Size]byte
	copy(digest[:], snapshotDigest)
	if err := target.markPublished(temporaryInfo, snapshotBytes, digest); err != nil {
		return fmt.Errorf("bind published exact backup inode: %w", err)
	}
	return nil
}

func classifyExactBackupResult(published bool, resultErr error) error {
	if published && resultErr != nil && !errors.Is(resultErr, ErrExactBackupPublished) {
		return fmt.Errorf("%w: %w", ErrExactBackupPublished, resultErr)
	}
	return resultErr
}

type preflightProblems struct {
	seen  map[string]struct{}
	items []string
}

func newPreflightProblems() *preflightProblems {
	return &preflightProblems{seen: make(map[string]struct{})}
}

func (problems *preflightProblems) add(format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	if _, exists := problems.seen[message]; exists {
		return
	}
	problems.seen[message] = struct{}{}
	problems.items = append(problems.items, message)
}

func (problems *preflightProblems) err() error {
	if len(problems.items) == 0 {
		return nil
	}
	slices.Sort(problems.items)
	return fmt.Errorf("%w:\n- %s", ErrLegacyUpgradePreflight, strings.Join(problems.items, "\n- "))
}

func inspectPreflightInventories(
	ctx context.Context,
	providerUUID string,
	canonicalBackends []string,
	configured map[string]struct{},
	inventories map[string]BackendInventory,
	problems *preflightProblems,
) (map[string]map[string]struct{}, error) {
	for _, backendName := range canonicalBackends {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, ok := inventories[backendName]; !ok {
			problems.add("configured backend %q has no complete inventory", backendName)
		}
	}

	owners := make(map[string]map[string]struct{})
	storageOwners := make(map[backendidentity.ID]string, len(inventories))
	for backendName, inventory := range inventories {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, ok := configured[backendName]; !ok {
			problems.add("inventory was supplied for unconfigured backend %q", backendName)
		}
		if !inventory.StorageIdentity.Valid() {
			problems.add("backend %q has no valid storage identity", backendName)
		} else if previous, duplicate := storageOwners[inventory.StorageIdentity]; duplicate && previous != backendName {
			problems.add("backends %q and %q report the same storage identity %s",
				previous, backendName, inventory.StorageIdentity)
		} else {
			storageOwners[inventory.StorageIdentity] = backendName
		}
		if inventory.Provisions == nil {
			problems.add("backend %q returned a null provisions inventory", backendName)
		}
		if inventory.Retentions == nil {
			problems.add("backend %q returned a null retentions inventory", backendName)
		}
		if inventory.ProvisionProviderUUIDs == nil {
			problems.add("backend %q has no per-provision provider identity observations", backendName)
		}
		if inventory.ProvisionItems == nil {
			problems.add("backend %q has no per-provision workload observations", backendName)
		}
		seenByBackend := make(map[string]string)
		provisions := make(map[string]struct{}, len(inventory.Provisions))
		for _, leaseUUID := range inventory.Provisions {
			provisions[leaseUUID] = struct{}{}
			observedProvider, present := inventory.ProvisionProviderUUIDs[leaseUUID]
			if !present {
				problems.add(
					"backend %q provision %q has no provider identity observation",
					backendName,
					leaseUUID,
				)
			} else if observedProvider != "" && observedProvider != providerUUID {
				problems.add(
					"backend %q provision %q reports provider %q, configured provider is %q",
					backendName,
					leaseUUID,
					observedProvider,
					providerUUID,
				)
			}
			items, present := inventory.ProvisionItems[leaseUUID]
			if !present {
				problems.add(
					"backend %q provision %q has no workload observation",
					backendName,
					leaseUUID,
				)
			} else if err := validatePreflightWorkloadItems(items); err != nil {
				problems.add(
					"backend %q provision %q has invalid workload observation: %v",
					backendName,
					leaseUUID,
					err,
				)
			}
		}
		for leaseUUID := range inventory.ProvisionProviderUUIDs {
			if _, present := provisions[leaseUUID]; !present {
				problems.add(
					"backend %q has provider identity for lease %q absent from its provisions inventory",
					backendName,
					leaseUUID,
				)
			}
		}
		for leaseUUID := range inventory.ProvisionItems {
			if _, present := provisions[leaseUUID]; !present {
				problems.add(
					"backend %q has workload observation for lease %q absent from its provisions inventory",
					backendName,
					leaseUUID,
				)
			}
		}
		for _, source := range []struct {
			name   string
			leases []string
		}{
			{name: "provisions", leases: inventory.Provisions},
			{name: "retentions", leases: inventory.Retentions},
		} {
			for _, leaseUUID := range source.leases {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if leaseUUID == "" {
					problems.add("backend %q %s inventory contains an empty lease identity", backendName, source.name)
					continue
				}
				if !canonicalLeaseUUID(leaseUUID) {
					problems.add("backend %q %s inventory contains non-canonical lease identity %q",
						backendName, source.name, leaseUUID)
				}
				if previous, duplicate := seenByBackend[leaseUUID]; duplicate {
					problems.add("backend %q reports lease %q more than once (%s and %s)",
						backendName, leaseUUID, previous, source.name)
				} else {
					seenByBackend[leaseUUID] = source.name
				}
				if owners[leaseUUID] == nil {
					owners[leaseUUID] = make(map[string]struct{})
				}
				owners[leaseUUID][backendName] = struct{}{}
			}
		}
	}
	return owners, nil
}

const legacyDefaultServiceName = "app"

func cloneLeaseItems(items []backend.LeaseItem) []backend.LeaseItem {
	return slices.Clone(items)
}

func validatePreflightWorkloadItems(items []backend.LeaseItem) error {
	if len(items) == 0 {
		return errors.New("workload items are empty")
	}
	seenServices := make(map[string]struct{}, len(items))
	for index, item := range items {
		for _, field := range []struct {
			name  string
			value string
		}{
			{name: "SKU", value: item.SKU},
			{name: "service name", value: item.ServiceName},
			{name: "custom domain", value: item.CustomDomain},
		} {
			if !utf8.ValidString(field.value) {
				return fmt.Errorf("item %d %s is not valid UTF-8", index, field.name)
			}
		}
		if strings.TrimSpace(item.SKU) == "" {
			return fmt.Errorf("item %d has an empty SKU", index)
		}
		serviceName := item.ServiceName
		if len(items) == 1 && serviceName == "" {
			serviceName = legacyDefaultServiceName
		}
		if serviceName == "" {
			return fmt.Errorf("item %d has an empty service name in a multi-item workload", index)
		}
		if _, duplicate := seenServices[serviceName]; duplicate {
			return fmt.Errorf("duplicate service name %q", serviceName)
		}
		seenServices[serviceName] = struct{}{}
	}
	if _, err := backend.ValidateOperationQuantities(items); err != nil {
		return fmt.Errorf("workload quantities: %w", err)
	}
	return nil
}

func validateRawChainItemsForDigest(items []backend.LeaseItem) error {
	for index, item := range items {
		for _, field := range []struct {
			name  string
			value string
		}{
			{name: "SKU", value: item.SKU},
			{name: "service name", value: item.ServiceName},
			{name: "custom domain", value: item.CustomDomain},
		} {
			if !utf8.ValidString(field.value) {
				return fmt.Errorf("item %d %s is not valid UTF-8", index, field.name)
			}
		}
	}
	return nil
}

func canonicalPreflightWorkloadItems(items []backend.LeaseItem) []backend.LeaseItem {
	canonical := cloneLeaseItems(items)
	if len(canonical) == 1 && canonical[0].ServiceName == "" {
		canonical[0].ServiceName = legacyDefaultServiceName
	}
	slices.SortFunc(canonical, func(left, right backend.LeaseItem) int {
		if order := strings.Compare(left.ServiceName, right.ServiceName); order != 0 {
			return order
		}
		return strings.Compare(left.SKU, right.SKU)
	})
	return canonical
}

// comparePreflightWorkloadItems proves the immutable paid topology exactly.
// CustomDomain is the one intentionally asymmetric field: v0.13 could defer an
// otherwise valid requested domain by persisting an empty effective label until
// DNS became ready. Empty backend evidence is therefore accepted for a
// non-empty chain domain; any non-empty observed value must match exactly.
func comparePreflightWorkloadItems(
	chainItems []backend.LeaseItem,
	backendItems []backend.LeaseItem,
) error {
	if err := validatePreflightWorkloadItems(chainItems); err != nil {
		return fmt.Errorf("invalid chain items: %w", err)
	}
	if err := validatePreflightWorkloadItems(backendItems); err != nil {
		return fmt.Errorf("invalid backend items: %w", err)
	}
	expected := canonicalPreflightWorkloadItems(chainItems)
	observed := canonicalPreflightWorkloadItems(backendItems)
	if len(expected) != len(observed) {
		return fmt.Errorf("observed %d services, expected %d", len(observed), len(expected))
	}
	for index := range expected {
		if observed[index].SKU != expected[index].SKU ||
			observed[index].Quantity != expected[index].Quantity ||
			observed[index].ServiceName != expected[index].ServiceName {
			return fmt.Errorf(
				"service topology differs (observed=%q/%q/%d expected=%q/%q/%d)",
				observed[index].ServiceName,
				observed[index].SKU,
				observed[index].Quantity,
				expected[index].ServiceName,
				expected[index].SKU,
				expected[index].Quantity,
			)
		}
		if observed[index].CustomDomain != "" &&
			observed[index].CustomDomain != expected[index].CustomDomain {
			return fmt.Errorf(
				"service %q custom domain %q differs from chain value %q",
				observed[index].ServiceName,
				observed[index].CustomDomain,
				expected[index].CustomDomain,
			)
		}
	}
	return nil
}

func inspectPreflightChainMembership(
	ctx context.Context,
	providerUUID string,
	placements map[string]string,
	observedOwners map[string]map[string]struct{},
	inventories map[string]BackendInventory,
	proof LegacyUpgradeChainProof,
	problems *preflightProblems,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !proof.valid() || proof.providerUUID != providerUUID {
		return nil
	}
	relevant := make(map[string]struct{}, len(placements)+len(observedOwners))
	for leaseUUID := range placements {
		relevant[leaseUUID] = struct{}{}
	}
	for leaseUUID := range observedOwners {
		relevant[leaseUUID] = struct{}{}
	}
	for _, leaseUUID := range slices.Sorted(maps.Keys(relevant)) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, present := proof.leaseUUIDs[leaseUUID]; !present {
			problems.add(
				"lease %q is absent from the height-%d all-state chain snapshot for configured provider %q",
				leaseUUID,
				proof.blockHeight,
				providerUUID,
			)
			continue
		}
		for backendName := range observedOwners[leaseUUID] {
			inventory := inventories[backendName]
			observedItems, provision := inventory.ProvisionItems[leaseUUID]
			if !provision {
				continue // The inventory validation reports the missing pair.
			}
			if err := comparePreflightWorkloadItems(
				proof.leaseItems[leaseUUID],
				observedItems,
			); err != nil {
				problems.add(
					"backend %q provision %q differs from height-%d chain workload: %v",
					backendName,
					leaseUUID,
					proof.blockHeight,
					err,
				)
			}
		}
	}
	return nil
}

func inspectPreflightCoverage(
	ctx context.Context,
	placements map[string]string,
	observedOwners map[string]map[string]struct{},
	problems *preflightProblems,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for leaseUUID, owners := range observedOwners {
		if err := ctx.Err(); err != nil {
			return err
		}
		names := slices.Sorted(maps.Keys(owners))
		if len(names) != 1 {
			problems.add("inventory lease %q has ambiguous owners %v", leaseUUID, names)
			continue
		}
		backendName, exists := placements[leaseUUID]
		switch {
		case !exists:
			problems.add("inventory lease %q on backend %q has no eligible placement row", leaseUUID, names[0])
		case backendName != names[0]:
			problems.add("inventory lease %q is on backend %q but placement names %q",
				leaseUUID, names[0], backendName)
		}
	}

	for leaseUUID, backendName := range placements {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(observedOwners[leaseUUID]) == 0 {
			problems.add("placement %q on backend %q is absent from complete backend inventory",
				leaseUUID, backendName)
		}
	}
	return ctx.Err()
}

func canonicalLeaseUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}

func preflightRevisionHeader(value []byte) (revision uint64, object bool, err error) {
	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return 0, false, nil
	}
	var header struct {
		Revision uint64 `json:"revision"`
	}
	if err := json.Unmarshal(trimmed, &header); err != nil {
		return 0, true, err
	}
	return header.Revision, true, nil
}

func decodeV013PreflightPlacement(value []byte) (string, error) {
	if len(value) == 0 {
		return "", errors.New("empty value")
	}
	// v0.13 selected the JSON decoder only when the first stored byte was '{'.
	// Do not trim here: interpreting a leading-space raw backend name as an
	// object could verify a different owner than the upgraded Store will load.
	if value[0] != '{' {
		if !validLegacyBackendName(value) || len(value) == 0 {
			return "", errors.New("raw backend name is empty or non-printable")
		}
		return string(value), nil
	}

	fields, err := decodeUniqueJSONObject(value)
	if err != nil {
		return "", fmt.Errorf("decode JSON object: %w", err)
	}
	for name := range fields {
		if name != "backend" && name != "set_at" {
			return "", fmt.Errorf("field %q did not exist in the v0.13 placement schema", name)
		}
	}
	backendValue, hasBackend := fields["backend"]
	setAtValue, hasSetAt := fields["set_at"]
	if !hasBackend || !hasSetAt || len(fields) != 2 {
		return "", errors.New("v0.13 JSON row must contain exactly backend and set_at")
	}
	var backendName string
	if err := json.Unmarshal(backendValue, &backendName); err != nil || backendName == "" {
		return "", errors.New("backend is empty or not a string")
	}
	var setAtText string
	if err := json.Unmarshal(setAtValue, &setAtText); err != nil {
		return "", fmt.Errorf("set_at is not a timestamp: %w", err)
	}
	if _, err := time.Parse(time.RFC3339Nano, setAtText); err != nil {
		return "", fmt.Errorf("set_at is not a timestamp: %w", err)
	}
	return backendName, nil
}

// decodeUniqueJSONObject preserves the exact v0.13 object boundary while
// rejecting duplicate JSON names. encoding/json's normal map/struct decode
// silently keeps the last duplicate, which is not an unambiguous ownership
// record and was never emitted by the v0.13 encoder.
func decodeUniqueJSONObject(value []byte) (map[string]json.RawMessage, error) {
	if !utf8.Valid(value) {
		return nil, errors.New("JSON is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	opening, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return nil, errors.New("expected JSON object")
	}

	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		nameToken, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := nameToken.(string)
		if !ok {
			return nil, errors.New("object field name is not a string")
		}
		if _, duplicate := fields[name]; duplicate {
			return nil, fmt.Errorf("duplicate field %q", name)
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return nil, fmt.Errorf("decode field %q: %w", name, err)
		}
		fields[name] = raw
	}
	closing, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return nil, errors.New("unterminated JSON object")
	}

	var trailing json.RawMessage
	switch err := decoder.Decode(&trailing); {
	case errors.Is(err, io.EOF):
		return fields, nil
	case err != nil:
		return nil, err
	default:
		return nil, errors.New("unexpected data after JSON object")
	}
}
