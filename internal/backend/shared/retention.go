package shared

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// RetentionStoreInspection distinguishes an absent journal from an existing,
// valid but empty v0.13 journal.
type RetentionStoreInspection struct {
	Exists        bool
	IdentityBound bool
	Entries       []RetentionEntry
}

// InspectRetentionStoreReadOnly decodes the authoritative retention bucket
// without creating or mutating it.
func InspectRetentionStoreReadOnly(dbPath string) (RetentionStoreInspection, error) {
	return inspectRetentionStoreReadOnlyFile(pathnameAuthoritativeStoreFile(dbPath))
}

// InspectBoundRetentionStoreReadOnly is the descriptor-relative form used by
// storage-lineage initialization after retaining the journal parent.
func InspectBoundRetentionStoreReadOnly(
	path *BoundAuthoritativeStorePath,
) (RetentionStoreInspection, error) {
	file, err := boundAuthoritativeStoreFile(path)
	if err != nil {
		return RetentionStoreInspection{}, err
	}
	return inspectRetentionStoreReadOnlyFile(file)
}

func inspectRetentionStoreReadOnlyFile(
	file authoritativeStoreFile,
) (RetentionStoreInspection, error) {
	if _, err := file.Lstat(); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return RetentionStoreInspection{}, nil
		}
		return RetentionStoreInspection{}, fmt.Errorf("stat retention database: %w", err)
	}
	db, _, err := openExistingBoltDBFile(file, true, false)
	if err != nil {
		return RetentionStoreInspection{}, fmt.Errorf("open retention database read-only: %w", err)
	}
	defer func() { _ = db.Close() }()
	inspection := RetentionStoreInspection{Exists: true}
	err = db.View(func(tx *bolt.Tx) error {
		inspection.IdentityBound = tx.Bucket(storeIdentityBucketName) != nil
		return inspectRetentionBucket(tx, func(entry RetentionEntry) {
			inspection.Entries = append(inspection.Entries, entry)
		})
	})
	if err != nil {
		return RetentionStoreInspection{}, err
	}
	return inspection, nil
}

// InspectRetentionEntriesReadOnly decodes the authoritative retention bucket
// without creating a database, bucket, index, cleanup goroutine, or write
// transaction. Backend storage-lineage initialization uses it while the old
// daemon is stopped to prove configured tenant bytes belong to the root being
// sealed before any marker is published.
func InspectRetentionEntriesReadOnly(dbPath string) ([]RetentionEntry, error) {
	inspection, err := InspectRetentionStoreReadOnly(dbPath)
	if err != nil {
		return nil, err
	}
	return inspection.Entries, nil
}

var retentionBucketName = []byte("retention")

// RetentionStatusActive is the status of an active (held) retention entry.
const RetentionStatusActive = "active"

// RetentionStatusRestoring is the status of an entry currently being restored.
const RetentionStatusRestoring = "restoring"

// RetentionStatusReaping marks a record whose volumes are pending physical
// destruction: the bytes are still on disk (so the footprint must keep counting
// in the admission projection) but the record is NOT restore-claimable. It is a
// finalizer tombstone — kept until every volume is confirmed destroyed, then
// Delete()d. See ENG-376.
const RetentionStatusReaping = "reaping"

var (
	// ErrNoRetention is returned when no retained data exists for a given lease UUID.
	ErrNoRetention = errors.New("no retained data for lease")
	// ErrNotRestorable is returned when a retained lease is not in a restorable state.
	ErrNotRestorable = errors.New("retained lease not in a restorable state")
	// ErrLegacyRestoringRetention means an unbound v0.13 retention journal still
	// contains a restore whose outcome was not resolved by the v0.13 reconciler.
	// Current destination authority cannot be reconstructed from that wire shape,
	// so storage-lineage adoption must stop before publishing a binding.
	ErrLegacyRestoringRetention = errors.New("v0.13 retention journal contains an unresolved restoring record")
)

// RetentionEntry records the data needed to restore a soft-deleted lease.
type RetentionEntry struct {
	OriginalLeaseUUID string `json:"original_lease_uuid"`
	Tenant            string `json:"tenant"`
	// Partition is an OPTIONAL cooperative sub-grouping WITHIN Tenant, declared
	// by the tenant for its own end-customers (aggregator model). "" is the
	// default whole-tenant bucket: legacy records, non-partitioned tenants, and
	// any collapsed (invalid/divergent/over-limit) declaration all land here.
	// It is grouping metadata for cap sub-division and eviction ordering ONLY —
	// never a security boundary (isolation stays keyed on Tenant) and never
	// load-bearing for restore/reap correctness (a record with a wrong or
	// missing partition remains fully restorable). Stamped only at soft-delete
	// time by the close path; see PutActiveMerged for the retry merge rule.
	Partition    string              `json:"partition,omitempty"`
	ProviderUUID string              `json:"provider_uuid"`
	Items        []backend.LeaseItem `json:"items"`
	// ResourceProfiles is the immutable sizing authority captured when the
	// footprint became retained. It is optional for backward compatibility with
	// records written before exact profile snapshots existed.
	ResourceProfiles []SKUResourceSnapshot `json:"resource_profiles,omitempty"`
	// DestinationItems and DestinationResourceProfiles are the immutable
	// ownership authority for a restore destination. ClaimForRestoreWithAuthority
	// writes them in the same transaction that changes Active to Restoring, before
	// any retained volume is adopted. They let recovery rebuild and finalize a
	// successful destination even when its initial active Release append failed
	// and the operation intent has already been consumed. Both fields are required
	// while Restoring and are cleared when rollback returns the source to Active.
	DestinationItems            []backend.LeaseItem   `json:"destination_items,omitempty"`
	DestinationResourceProfiles []SKUResourceSnapshot `json:"destination_resource_profiles,omitempty"`
	DestinationOperationID      OperationID           `json:"destination_operation_id,omitempty"`
	// DestinationCallbackURL is retained only as the paired operation identity
	// needed to validate DestinationLifecycleCallbackURL. Lifecycle observations
	// are always delivered to the latter; the operation URL is never reused to
	// settle another generation.
	DestinationCallbackURL          string                  `json:"destination_callback_url,omitempty"`
	DestinationLifecycleCallbackURL string                  `json:"destination_lifecycle_callback_url,omitempty"`
	StackManifest                   *manifest.StackManifest `json:"stack_manifest"`
	CallbackURL                     string                  `json:"callback_url"`
	RetainedVolumeNames             []string                `json:"retained_volume_names"`
	Status                          string                  `json:"status"`
	NewLeaseUUID                    string                  `json:"new_lease_uuid,omitempty"`
	Generation                      int                     `json:"generation"`
	CreatedAt                       time.Time               `json:"created_at"`
	RestoringSince                  time.Time               `json:"restoring_since,omitempty"`
	ReapingSince                    time.Time               `json:"reaping_since,omitempty"`
}

func validateRetentionEntryResourceProfiles(entry *RetentionEntry) error {
	if len(entry.ResourceProfiles) > 0 {
		if _, err := backend.ValidateOperationQuantities(entry.Items); err != nil {
			return fmt.Errorf("retention resource-profile quantities: %w", err)
		}
		if err := ValidateSKUResourceSnapshot(entry.Items, entry.ResourceProfiles); err != nil {
			return fmt.Errorf("retention resource profiles: %w", err)
		}
	}

	hasDestinationItems := len(entry.DestinationItems) > 0
	hasDestinationProfiles := len(entry.DestinationResourceProfiles) > 0
	if entry.Status != RetentionStatusRestoring {
		if hasDestinationItems || hasDestinationProfiles ||
			entry.DestinationOperationID != "" ||
			entry.DestinationCallbackURL != "" ||
			entry.DestinationLifecycleCallbackURL != "" {
			return errors.New("retention restore destination authority requires a restoring record")
		}
		return nil
	}
	if entry.OriginalLeaseUUID == "" || entry.NewLeaseUUID == "" {
		return errors.New("restoring retention record requires source and destination lease UUIDs")
	}
	if entry.OriginalLeaseUUID == entry.NewLeaseUUID {
		return errors.New("restoring retention record source and destination lease UUIDs must differ")
	}
	if entry.Generation <= 0 {
		return errors.New("restoring retention record generation must be positive")
	}
	if !hasDestinationItems || !hasDestinationProfiles {
		return errors.New("restoring retention record requires exact destination items and resource profiles")
	}
	if _, err := backend.ValidateOperationQuantities(entry.Items); err != nil {
		return fmt.Errorf("retention restore source quantities: %w", err)
	}
	if _, err := backend.ValidateOperationQuantities(entry.DestinationItems); err != nil {
		return fmt.Errorf("retention restore destination quantities: %w", err)
	}
	if err := ValidateSKUResourceSnapshot(entry.DestinationItems, entry.DestinationResourceProfiles); err != nil {
		return fmt.Errorf("retention restore destination resource profiles: %w", err)
	}
	if err := retentionItemsShapeMatch(entry.Items, entry.DestinationItems); err != nil {
		return fmt.Errorf("retention restore destination shape: %w", err)
	}
	if !entry.DestinationOperationID.Valid() {
		return errors.New("retention restore destination operation ID is not a canonical UUIDv4")
	}
	if entry.DestinationCallbackURL == "" || entry.DestinationLifecycleCallbackURL == "" {
		return errors.New("restoring retention record requires an exact operation/lifecycle callback pair")
	}
	resolved, err := backend.ResolveLifecycleCallbackURL(
		entry.DestinationCallbackURL,
		entry.DestinationLifecycleCallbackURL,
	)
	if err != nil {
		return fmt.Errorf("retention restore destination callback pair: %w", err)
	}
	if resolved != entry.DestinationLifecycleCallbackURL {
		return errors.New("retention restore destination lifecycle callback differs from operation authority")
	}
	callbackOperationID, err := parseOperationCallbackID(entry.DestinationCallbackURL)
	if err != nil {
		return fmt.Errorf("retention restore destination operation callback: %w", err)
	}
	if callbackOperationID != entry.DestinationOperationID {
		return fmt.Errorf(
			"retention restore destination operation ID %q differs from callback authority %q",
			entry.DestinationOperationID,
			callbackOperationID,
		)
	}
	return nil
}

func retentionItemsShapeMatch(source, destination []backend.LeaseItem) error {
	shape := func(items []backend.LeaseItem) map[string]int {
		result := make(map[string]int, len(items))
		for _, item := range items {
			result[item.ServiceName] += item.Quantity
		}
		return result
	}
	sourceShape, destinationShape := shape(source), shape(destination)
	if len(sourceShape) != len(destinationShape) {
		return fmt.Errorf("source has %d services, destination has %d", len(sourceShape), len(destinationShape))
	}
	for service, quantity := range sourceShape {
		if destinationShape[service] != quantity {
			return fmt.Errorf(
				"service %q source quantity %d differs from destination quantity %d",
				service, quantity, destinationShape[service],
			)
		}
	}
	return nil
}

// validateRetentionSourceAuthorityForBinding validates the source-side facts
// consumed by accounting, restore, and reaping after a storage-lineage seal.
// A legacy single unnamed item is normalized on a clone for topology checking;
// the wire bytes remain untouched. A nil manifest is accepted only for Active
// and Reaping because v0.13 deliberately retained such rows for manual data
// recovery when release hydration failed. Restoring could never be claimed
// without a usable manifest and therefore must carry one.
func validateRetentionSourceAuthorityForBinding(entry *RetentionEntry) error {
	if entry == nil {
		return errors.New("retention entry is required")
	}
	if strings.TrimSpace(entry.Tenant) == "" {
		return errors.New("retention tenant is required")
	}
	if entry.CreatedAt.IsZero() {
		return errors.New("retention creation timestamp is required")
	}
	if entry.Generation < 0 {
		return errors.New("retention generation cannot be negative")
	}
	items := slices.Clone(entry.Items)
	if err := backend.NormalizeProvisionRequest(&backend.ProvisionRequest{Items: items}); err != nil {
		return fmt.Errorf("retention source items: %w", err)
	}
	if _, err := backend.ValidateOperationQuantities(items); err != nil {
		return fmt.Errorf("retention source quantities: %w", err)
	}
	seenServices := make(map[string]struct{}, len(items))
	for index, item := range items {
		if strings.TrimSpace(item.SKU) == "" {
			return fmt.Errorf("retention source item %d has an empty SKU", index)
		}
		if _, duplicate := seenServices[item.ServiceName]; duplicate {
			return fmt.Errorf("retention source has duplicate service name %q", item.ServiceName)
		}
		seenServices[item.ServiceName] = struct{}{}
	}
	if entry.Status == RetentionStatusActive || entry.Status == RetentionStatusRestoring {
		if strings.TrimSpace(entry.CallbackURL) == "" {
			return errors.New("active/restoring retention source callback URL is required")
		}
		if err := backend.ValidateOperationCallbackURL(entry.CallbackURL); err != nil {
			return fmt.Errorf("active/restoring retention source callback URL: %w", err)
		}
	}
	if entry.StackManifest == nil {
		if entry.Status == RetentionStatusRestoring {
			return errors.New("restoring retention source manifest is required")
		}
		return nil
	}
	if err := entry.StackManifest.Validate(); err != nil {
		return fmt.Errorf("retention source manifest: %w", err)
	}
	if err := manifest.ValidateStackAgainstItems(entry.StackManifest, items); err != nil {
		return fmt.Errorf("retention source manifest topology: %w", err)
	}
	return nil
}

// RetentionStoreConfig configures the retention store.
type RetentionStoreConfig struct {
	DBPath string
	// OnReindex (nil-safe) fires after each index build with record count, duration, and
	// trigger ("open"|"manual"). A callback (not a metrics import) so this package stays
	// free of internal/metrics — mirrors boltStore.startCleanup's onPanic seam.
	OnReindex func(count int, dur time.Duration, trigger string)
}

// RetentionStore persists soft-deleted lease data in bbolt. The `retention` bucket is
// the single source of truth; byTenant/byStatus are a DERIVED in-memory index rebuilt
// from the bucket on open (never persisted, cannot drift across a restart). The bucket
// is INDEX-COUPLED: it may be mutated ONLY through this type's wrapped methods (each
// maintains the index under s.mu). Never wire boltStore.startCleanup / removeOlderThan
// to this store — they cursor.Delete directly on the bucket and would bypass the index.
type RetentionStore struct {
	*boltStore
	mu        sync.RWMutex
	byTenant  map[string]map[string]struct{} // tenant -> set of bbolt bucket keys (the authoritative lease UUID; see scanIndex/indexApply)
	byStatus  map[string]map[string]struct{} // status -> set of bbolt bucket keys (the authoritative lease UUID; see scanIndex/indexApply)
	onReindex func(count int, dur time.Duration, trigger string)
}

// NewRetentionStore opens or creates a bbolt database for retention persistence.
// No background cleanup loop is started; the docker backend drives reaping and
// eviction explicitly via the MarkReaping* / ListReaping / ListExpired methods
// (reapExpiredRetentions, evictRetentionsToCap, and the retryReapingRecords sweep
// in restore.go), plus PutReaping for deprovision give-up tombstones.
//
// Deprecated: this compatibility-only constructor creates an unbound journal.
// Application composition roots are repository-guarded to use
// OpenIdentityBoundRetentionStore and cannot obtain authority from this value.
func NewRetentionStore(cfg RetentionStoreConfig) (*RetentionStore, error) {
	return newRetentionStore(cfg, backendidentity.VerifiedStorage{}, nil)
}

// OpenIdentityBoundRetentionStore opens an initialized authoritative
// retention journal without creating or repairing it.
func OpenIdentityBoundRetentionStore(
	cfg RetentionStoreConfig,
	storage backendidentity.VerifiedStorage,
	gate *backendidentity.StorageAuthorityGate,
) (*RetentionStore, error) {
	if !storage.Valid() {
		return nil, errors.New("verified backend storage authority is required")
	}
	if gate == nil || !gate.Valid() {
		return nil, errors.New("backend storage authority gate is required")
	}
	return newRetentionStore(cfg, storage, gate)
}

func newRetentionStore(
	cfg RetentionStoreConfig,
	storage backendidentity.VerifiedStorage,
	gate *backendidentity.StorageAuthorityGate,
) (*RetentionStore, error) {
	storeCfg := boltStoreConfig{
		DBPath:     cfg.DBPath,
		BucketName: retentionBucketName,
		Label:      "retention",
	}
	var base *boltStore
	var err error
	if storage.Valid() {
		base, err = openIdentityBoundBoltStore(storeCfg, authoritativeStoreRetention, storage, gate)
	} else {
		base, err = openBoltStore(storeCfg)
	}
	if err != nil {
		return nil, err
	}
	s := &RetentionStore{boltStore: base, onReindex: cfg.OnReindex}
	// Derived index: rebuilt from the primary bucket on open, before the store is
	// published to any other goroutine (so no lock needed). Fail-closed on a malformed
	// record — a corrupt retention record is a data-integrity event, not something to
	// silently skip.
	start := time.Now()
	byTenant, byStatus, count, err := s.scanIndex()
	if err != nil {
		_ = base.Close()
		return nil, fmt.Errorf("failed to build retention index: %w", err)
	}
	s.byTenant, s.byStatus = byTenant, byStatus
	s.fireReindex(count, time.Since(start), "open")
	return s, nil
}

func inspectRetentionBucket(tx *bolt.Tx, collect func(RetentionEntry)) error {
	bucket := tx.Bucket(retentionBucketName)
	if bucket == nil {
		return errors.New("retention bucket is missing")
	}
	identityBound := tx.Bucket(storeIdentityBucketName) != nil
	budget := newStoppedAuthoritativeInspectionBudget()
	return bucket.ForEach(func(key, value []byte) error {
		if err := budget.observe(key, value); err != nil {
			return err
		}
		if value == nil {
			return fmt.Errorf("retention record with key length %d is a nested bucket", len(key))
		}
		var entry RetentionEntry
		if err := json.Unmarshal(value, &entry); err != nil {
			return fmt.Errorf("decode retention record with key length %d: %w", len(key), err)
		}
		if err := validateAuthoritativeRetentionIdentity(key, &entry); err != nil {
			return fmt.Errorf("validate retention record identity with key length %d: %w", len(key), err)
		}
		if err := validateRetentionSourceAuthorityForBinding(&entry); err != nil {
			return fmt.Errorf("validate retention record source with key length %d: %w", len(key), err)
		}
		if err := validateRetentionEntryResourceProfiles(&entry); err != nil {
			// v0.13 could durably leave a restoring row after a failed teardown,
			// re-quarantine, or finalization. That schema has no destination items,
			// resource snapshot, typed operation ID, or operation/lifecycle callback
			// pair. Inventing those facts would let the upgraded binary finalize or
			// roll back a different operation. Recognize only the unbound, exact
			// pre-authority shape and fail with an operator-actionable classification;
			// identity-bound runtime stores retain the strict current-schema error.
			if !identityBound && isLegacyRestoringRetentionEntry(&entry) {
				return fmt.Errorf(
					"%w: source %s destination %s generation %d; restart the complete matching v0.13 lineage in isolation and let its retention reconciler commit or roll back the restore, then drain callbacks, stop it, take a new backup, and retry adoption; do not edit this row or synthesize destination authority",
					ErrLegacyRestoringRetention,
					entry.OriginalLeaseUUID,
					entry.NewLeaseUUID,
					entry.Generation,
				)
			}
			return fmt.Errorf("validate retention record with key length %d: %w", len(key), err)
		}
		if collect != nil {
			collect(entry)
		}
		return nil
	})
}

// isLegacyRestoringRetentionEntry recognizes only fields that v0.13's
// ClaimForRestore could persist. It is a diagnostic predicate, never migration
// authority: callers always reject this shape and leave the journal untouched.
func isLegacyRestoringRetentionEntry(entry *RetentionEntry) bool {
	return entry != nil &&
		entry.Status == RetentionStatusRestoring &&
		entry.NewLeaseUUID != "" &&
		entry.NewLeaseUUID != entry.OriginalLeaseUUID &&
		entry.Generation > 0 &&
		!entry.CreatedAt.IsZero() &&
		!entry.RestoringSince.IsZero() &&
		entry.ReapingSince.IsZero() &&
		len(entry.ResourceProfiles) == 0 &&
		len(entry.DestinationItems) == 0 &&
		len(entry.DestinationResourceProfiles) == 0 &&
		entry.DestinationOperationID == "" &&
		entry.DestinationCallbackURL == "" &&
		entry.DestinationLifecycleCallbackURL == ""
}

func validateAuthoritativeRetentionIdentity(key []byte, entry *RetentionEntry) error {
	if entry == nil || !backend.IsCanonicalLeaseUUID(entry.OriginalLeaseUUID) {
		return errors.New("original lease UUID is not canonical")
	}
	if string(key) != entry.OriginalLeaseUUID {
		return errors.New("bucket key differs from original lease UUID")
	}
	if !backend.IsCanonicalLeaseUUID(entry.ProviderUUID) {
		return errors.New("provider UUID is not canonical")
	}
	if entry.NewLeaseUUID != "" && !backend.IsCanonicalLeaseUUID(entry.NewLeaseUUID) {
		return errors.New("destination lease UUID is not canonical")
	}
	switch entry.Status {
	case RetentionStatusActive, RetentionStatusRestoring, RetentionStatusReaping:
		return nil
	default:
		return errors.New("retention status is unsupported")
	}
}

func PrepareBoundRetentionStoreStorage(
	path *BoundAuthoritativeStorePath,
	storage backendidentity.PendingStorage,
	profile backendidentity.InitializationProfile,
) error {
	if !storage.Valid() {
		return errors.New("pending backend storage authority is required")
	}
	allowCreate, err := allowAuthoritativeStoreCreation(profile)
	if err != nil {
		return err
	}
	return initializeIdentityBoundBoltStoreBound(
		path, retentionBucketName, "retention", authoritativeStoreRetention, storage.ID(), allowCreate,
		func(tx *bolt.Tx) error { return inspectRetentionBucket(tx, nil) },
	)
}

func CheckBoundRetentionStoreStorage(
	path *BoundAuthoritativeStorePath,
	storage backendidentity.PendingStorage,
) error {
	if !storage.Valid() {
		return errors.New("pending backend storage authority is required")
	}
	return checkIdentityBoundBoltStoreBound(
		path, retentionBucketName, "retention", authoritativeStoreRetention, storage.ID(), nil,
	)
}

func VerifyBoundRetentionStoreStorage(
	path *BoundAuthoritativeStorePath,
	storage backendidentity.VerifiedStorage,
) error {
	if !storage.Valid() {
		return errors.New("verified backend storage authority is required")
	}
	return checkIdentityBoundBoltStoreBound(
		path, retentionBucketName, "retention", authoritativeStoreRetention, storage.ID(), nil,
	)
}

func VerifyRetentionStoreStorage(dbPath string, storage backendidentity.VerifiedStorage) error {
	return verifyIdentityBoundBoltStore(
		dbPath, retentionBucketName, "retention", authoritativeStoreRetention, storage, nil,
	)
}

func (s *RetentionStore) fireReindex(count int, dur time.Duration, trigger string) {
	if s.onReindex != nil {
		s.onReindex(count, dur, trigger)
	}
}

// ReIndex rebuilds the in-memory index from the primary bucket and swaps it in.
// Safe on a live store: it holds s.mu across the WHOLE scan+swap, which serializes it with
// mutators (each holds s.mu across its {db.Update + indexApply}). Holding the lock only for the
// swap would be unsafe — a mutator could commit a write and update the live index between the
// scan returning and the swap, and the swap would then overwrite that write with a pre-scan
// snapshot (lost update → drift until the next rebuild). fireReindex runs outside the lock.
// The self-heal/recovery seam (the index is never the source of truth).
func (s *RetentionStore) ReIndex() error {
	start := time.Now()
	s.mu.Lock()
	byTenant, byStatus, count, err := s.scanIndex()
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.byTenant, s.byStatus = byTenant, byStatus
	s.mu.Unlock()
	s.fireReindex(count, time.Since(start), "manual")
	return nil
}

// scanIndex builds fresh tenant/status index maps from one pass over the primary
// bucket. Identity-bound production stores decode and validate the complete
// authority record before indexing it; explicitly unbound test/migration stores
// keep the historical lightweight tenant/status projection. Returns the maps and
// record count and fails closed on every error required by the store's mode.
func (s *RetentionStore) scanIndex() (byTenant, byStatus map[string]map[string]struct{}, count int, err error) {
	byTenant = map[string]map[string]struct{}{}
	byStatus = map[string]map[string]struct{}{}
	err = s.view(func(tx *bolt.Tx) error {
		return tx.Bucket(retentionBucketName).ForEach(func(k, v []byte) error {
			if s.binding == nil {
				// Explicitly unbound stores retain the historical lightweight
				// projection scan used by tests and offline migration helpers. This
				// lets a later full-record read surface localized corruption without
				// preventing the store from opening. Production constructors cannot
				// reach this branch: identity-bound stores decode and validate every
				// authority-bearing field below before publishing an index.
				var indexed struct {
					Tenant string `json:"tenant"`
					Status string `json:"status"`
				}
				if uerr := json.Unmarshal(v, &indexed); uerr != nil {
					return fmt.Errorf("malformed retention record %q: %w", string(k), uerr)
				}
				uuid := string(k)
				idxAdd(byTenant, indexed.Tenant, uuid)
				idxAdd(byStatus, indexed.Status, uuid)
				count++
				return nil
			}
			var e RetentionEntry
			if uerr := json.Unmarshal(v, &e); uerr != nil {
				// Use the bucket key (the OriginalLeaseUUID by convention) — a
				// totally-malformed record has an empty e.OriginalLeaseUUID, so the
				// operator-facing store-open failure must name the key to be lookup-able.
				return fmt.Errorf("malformed retention record %q: %w", string(k), uerr)
			}
			// Index on the bucket key used by getAll's subsequent Get. An
			// identity-bound store validates below that this key is canonical and
			// exactly equals e.OriginalLeaseUUID; keeping the key as the set member
			// also preserves correct lookup behavior for explicitly unbound test and
			// migration stores.
			uuid := string(k)
			if err := validateRetentionEntryResourceProfiles(&e); err != nil {
				return fmt.Errorf("invalid retention record with key length %d: %w", len(k), err)
			}
			if err := validateAuthoritativeRetentionIdentity(k, &e); err != nil {
				return fmt.Errorf("invalid retention identity with key length %d: %w", len(k), err)
			}
			if err := validateRetentionSourceAuthorityForBinding(&e); err != nil {
				return fmt.Errorf("invalid retention source with key length %d: %w", len(k), err)
			}
			idxAdd(byTenant, e.Tenant, uuid)
			idxAdd(byStatus, e.Status, uuid)
			count++
			return nil
		})
	})
	return byTenant, byStatus, count, err
}

// idxAdd / idxDel maintain a set-valued index map. Caller holds s.mu (or maps not yet
// published). delete on a nil/absent map or missing key is a Go no-op.
func idxAdd(m map[string]map[string]struct{}, key, uuid string) {
	set := m[key]
	if set == nil {
		set = map[string]struct{}{}
		m[key] = set
	}
	set[uuid] = struct{}{}
}

func idxDel(m map[string]map[string]struct{}, key, uuid string) {
	set := m[key]
	if set == nil {
		return
	}
	delete(set, uuid)
	if len(set) == 0 {
		delete(m, key)
	}
}

// indexApply reconciles the index for a record transition. Caller MUST hold s.mu.
// oldE=nil → insert; newE=nil → delete; both set → move. The set member is the caller-supplied
// uuid is the bbolt bucket key that getAll resolves via Get; it must not be
// re-derived from a mutable pre/post image. Identity-bound writes guarantee the
// key and OriginalLeaseUUID are the same canonical UUID. Partitions come from
// the entries (tenant immutability is observed, not assumed/optimized).
func (s *RetentionStore) indexApply(uuid string, oldE, newE *RetentionEntry) {
	if oldE != nil {
		idxDel(s.byTenant, oldE.Tenant, uuid)
		idxDel(s.byStatus, oldE.Status, uuid)
	}
	if newE != nil {
		idxAdd(s.byTenant, newE.Tenant, uuid)
		idxAdd(s.byStatus, newE.Status, uuid)
	}
}

// Put persists a RetentionEntry, upserting by OriginalLeaseUUID. It reads the
// pre-image in-txn so the index can drop the stale partition membership of any
// record being overwritten (a status/tenant change must not leave a phantom).
func (s *RetentionStore) Put(e RetentionEntry) error {
	if err := validateRetentionEntryResourceProfiles(&e); err != nil {
		return err
	}
	e.Items = slices.Clone(e.Items)
	e.ResourceProfiles = CloneSKUResourceSnapshot(e.ResourceProfiles)
	e.DestinationItems = slices.Clone(e.DestinationItems)
	e.DestinationResourceProfiles = CloneSKUResourceSnapshot(e.DestinationResourceProfiles)
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal retention entry: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldE *RetentionEntry
	err = s.update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		if raw := bkt.Get([]byte(e.OriginalLeaseUUID)); raw != nil {
			oldE = &RetentionEntry{}
			if uerr := json.Unmarshal(raw, oldE); uerr != nil {
				return fmt.Errorf("malformed retention record %q: %w", e.OriginalLeaseUUID, uerr)
			}
		}
		return bkt.Put([]byte(e.OriginalLeaseUUID), data)
	})
	if err != nil {
		return err
	}
	s.indexApply(e.OriginalLeaseUUID, oldE, &e)
	return nil
}

// PutActiveMerged atomically upserts the soft-delete record for a closing lease,
// merging mergeVolumes into any existing record's RetainedVolumeNames. Single txn,
// so it is safe against a concurrent ClaimForRestoreWithAuthority (no Get→Put TOCTOU):
//   - absent: writes `base` fresh (caller sets CreatedAt=now, Generation=0, Status=active).
//   - existing ACTIVE: PRESERVES the stored CreatedAt and Generation, writes the
//     UNION of stored RetainedVolumeNames and base.RetainedVolumeNames (dedup), and
//     KEEPS the stored StackManifest when base's is nil (a close retry must never
//     clobber a restorable manifest with a nil one); other fields come from `base`.
//   - existing NON-active (restoring): writes NOTHING, returns ok=false — a restore owns
//     the record; a blind write would corrupt the CAS. Caller defers (keeps lease Failed).
//
// Returns (ok bool, err error): ok=false + nil err means "deferred, record is restoring".
func (s *RetentionStore) PutActiveMerged(base RetentionEntry) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		ok   bool
		oldE *RetentionEntry
	)
	err := s.update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		if raw := bkt.Get([]byte(base.OriginalLeaseUUID)); raw != nil {
			var stored RetentionEntry
			if err := json.Unmarshal(raw, &stored); err != nil {
				return fmt.Errorf("malformed retention record %q: %w", base.OriginalLeaseUUID, err)
			}
			oldE = &stored
			if stored.Status != RetentionStatusActive {
				return nil // restoring (or otherwise non-active): refuse, ok stays false
			}
			if err := mergeRetentionResourceProfiles(&base, &stored); err != nil {
				return fmt.Errorf("merge retention resource profiles for %q: %w", base.OriginalLeaseUUID, err)
			}
			// Existing ACTIVE: preserve the grace clock + CAS generation, union the names.
			base.CreatedAt = stored.CreatedAt
			base.Generation = stored.Generation
			base.RetainedVolumeNames = dedupUnion(stored.RetainedVolumeNames, base.RetainedVolumeNames)
			// Never let a nil base manifest clobber a previously-persisted one. A
			// close retry can recompute base.StackManifest == nil (e.g. a transient
			// release-store hydration failure, or the release reaped between
			// attempts), and Restore rejects nil manifests — clobbering would make
			// an otherwise-restorable lease permanently un-restorable. The manifest
			// and the partition derived from it are the two hydrated (retry-variable)
			// fields, so they are the two needing this guard.
			if base.StackManifest == nil {
				base.StackManifest = stored.StackManifest
				// Partition is derived FROM the manifest hydration that just
				// failed: "" here means extraction was starved of input, never
				// tenant intent — preserve the stored label with the manifest.
				// A retry whose hydration SUCCEEDED re-stamps from base
				// unconditionally, including a legitimate "" (source disabled,
				// label removed via update): current intent wins over history.
				// NOTE: this coupling assumes the partition is manifest-derived
				// (true for every v1 source); a future non-manifest source (e.g.
				// chain.lease) must revisit this guard or a chain-sourced label
				// could be lost on a nil-manifest retry.
				base.Partition = stored.Partition
			}
		} else if err := mergeRetentionResourceProfiles(&base, nil); err != nil {
			return fmt.Errorf("validate retention resource profiles for %q: %w", base.OriginalLeaseUUID, err)
		}
		if err := validateRetentionEntryResourceProfiles(&base); err != nil {
			return fmt.Errorf("validate retention record %q: %w", base.OriginalLeaseUUID, err)
		}
		data, err := json.Marshal(base)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		ok = true
		return bkt.Put([]byte(base.OriginalLeaseUUID), data)
	})
	if err != nil {
		return false, err
	}
	if ok {
		s.indexApply(base.OriginalLeaseUUID, oldE, &base)
	}
	return ok, nil
}

// mergeRetentionResourceProfiles applies the retry-safe resource-authority
// rule. A legacy row may be upgraded by a new exact snapshot, and a retry that
// lacks the optional field preserves an already-exact stored snapshot. Two
// exact snapshots must be identical: silently repricing a retained footprint
// would make accounting depend on retry order.
func mergeRetentionResourceProfiles(base *RetentionEntry, stored *RetentionEntry) error {
	if len(base.ResourceProfiles) > 0 {
		if err := ValidateSKUResourceSnapshot(base.Items, base.ResourceProfiles); err != nil {
			return fmt.Errorf("incoming snapshot: %w", err)
		}
	}
	if stored == nil {
		base.ResourceProfiles = CloneSKUResourceSnapshot(base.ResourceProfiles)
		return nil
	}
	if len(stored.ResourceProfiles) > 0 {
		if err := ValidateSKUResourceSnapshot(stored.Items, stored.ResourceProfiles); err != nil {
			return fmt.Errorf("stored snapshot: %w", err)
		}
	}

	switch {
	case len(stored.ResourceProfiles) == 0:
		// A new writer may safely backfill a legacy record from the close claim.
		base.ResourceProfiles = CloneSKUResourceSnapshot(base.ResourceProfiles)
	case len(base.ResourceProfiles) == 0:
		// A partial retry must never erase already-persisted sizing authority.
		base.ResourceProfiles = CloneSKUResourceSnapshot(stored.ResourceProfiles)
	case !slices.Equal(base.ResourceProfiles, stored.ResourceProfiles):
		return fmt.Errorf("incoming snapshot differs from stored immutable snapshot")
	default:
		base.ResourceProfiles = CloneSKUResourceSnapshot(base.ResourceProfiles)
	}
	if len(base.ResourceProfiles) > 0 {
		if err := ValidateSKUResourceSnapshot(base.Items, base.ResourceProfiles); err != nil {
			return fmt.Errorf("merged snapshot: %w", err)
		}
	}
	return nil
}

// PutReaping writes a reaping tombstone for an ABANDONED on-disk footprint (a
// deprovision give-up). It is idempotent and never clobbers a still-counted record:
//   - absent: writes a fresh reaping record (stamps ReapingSince=now).
//   - existing reaping: unions RetainedVolumeNames and PRESERVES ReapingSince (aging).
//   - existing active/restoring: writes NOTHING, returns ok=false — that record
//     already counts the footprint (or owns it for restore); a blind reaping write
//     would corrupt accounting/CAS. Caller treats ok=false as "already tracked".
//
// Single txn, so it is safe against a concurrent ClaimForRestoreWithAuthority. (ENG-376)
func (s *RetentionStore) PutReaping(base RetentionEntry) (bool, error) {
	base.Status = RetentionStatusReaping
	base.ReapingSince = time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		ok   bool
		oldE *RetentionEntry
	)
	err := s.update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		if raw := bkt.Get([]byte(base.OriginalLeaseUUID)); raw != nil {
			var stored RetentionEntry
			if err := json.Unmarshal(raw, &stored); err != nil {
				return fmt.Errorf("malformed retention record %q: %w", base.OriginalLeaseUUID, err)
			}
			// Capture the pre-image as a value copy BEFORE the reaping-branch
			// `base = stored` assignment below aliases stored into base.
			preImage := stored
			oldE = &preImage
			switch stored.Status {
			case RetentionStatusActive, RetentionStatusRestoring:
				return nil // already counted/owned — refuse, ok stays false
			case RetentionStatusReaping:
				if err := mergeReapingResourceProfiles(&base, &stored); err != nil {
					return fmt.Errorf("merge reaping resource profiles for %q: %w", base.OriginalLeaseUUID, err)
				}
				// Re-leak of a lease that already has a reaping tombstone: preserve the
				// stored entry's accounting/identity fields (Items/Tenant/ProviderUUID/
				// CreatedAt/ReapingSince) WHOLESALE and only union any newly discovered
				// volume names, so a future caller passing partial `base` data can never
				// clobber a still-counted footprint (mirrors PutActiveMerged's
				// preserve-stored idiom; honors this method's "never clobbers" contract).
				stored.RetainedVolumeNames = dedupUnion(stored.RetainedVolumeNames, base.RetainedVolumeNames)
				stored.ResourceProfiles = CloneSKUResourceSnapshot(base.ResourceProfiles)
				base = stored
			}
		} else {
			if err := validateRetentionEntryResourceProfiles(&base); err != nil {
				return fmt.Errorf("validate reaping resource profiles for %q: %w", base.OriginalLeaseUUID, err)
			}
			base.ResourceProfiles = CloneSKUResourceSnapshot(base.ResourceProfiles)
		}
		data, err := json.Marshal(base)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		ok = true
		return bkt.Put([]byte(base.OriginalLeaseUUID), data)
	})
	if err != nil {
		return false, err
	}
	if ok {
		s.indexApply(base.OriginalLeaseUUID, oldE, &base)
	}
	return ok, nil
}

func mergeReapingResourceProfiles(base *RetentionEntry, stored *RetentionEntry) error {
	if err := validateRetentionEntryResourceProfiles(stored); err != nil {
		return fmt.Errorf("stored snapshot: %w", err)
	}
	if err := validateRetentionEntryResourceProfiles(base); err != nil {
		return fmt.Errorf("incoming snapshot: %w", err)
	}

	switch {
	case len(stored.ResourceProfiles) == 0 && len(base.ResourceProfiles) > 0:
		// Upgrade a legacy tombstone only when the incoming snapshot also exactly
		// covers the immutable stored Items that the reaping projection counts.
		if err := ValidateSKUResourceSnapshot(stored.Items, base.ResourceProfiles); err != nil {
			return fmt.Errorf("incoming snapshot does not cover stored items: %w", err)
		}
		base.ResourceProfiles = CloneSKUResourceSnapshot(base.ResourceProfiles)
	case len(stored.ResourceProfiles) > 0 && len(base.ResourceProfiles) == 0:
		base.ResourceProfiles = CloneSKUResourceSnapshot(stored.ResourceProfiles)
	case len(stored.ResourceProfiles) > 0 && !slices.Equal(stored.ResourceProfiles, base.ResourceProfiles):
		return fmt.Errorf("incoming snapshot differs from stored immutable snapshot")
	default:
		base.ResourceProfiles = CloneSKUResourceSnapshot(base.ResourceProfiles)
	}
	return nil
}

// dedupUnion returns the order-preserving deduplicated union of a and b
// (a's entries first, then b's not already present).
func dedupUnion(a, b []string) []string {
	seen := make(map[string]bool, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, s := range a {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	for _, s := range b {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

// Get retrieves a RetentionEntry by original lease UUID.
// Returns nil, nil when absent.
func (s *RetentionStore) Get(orig string) (*RetentionEntry, error) {
	var entry *RetentionEntry
	err := s.view(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		entry = &RetentionEntry{}
		if err := json.Unmarshal(raw, entry); err != nil {
			return fmt.Errorf("malformed retention record %q: %w", orig, err)
		}
		if err := validateRetentionEntryResourceProfiles(entry); err != nil {
			return fmt.Errorf("invalid retention record %q: %w", orig, err)
		}
		return nil
	})
	return entry, err
}

// Delete removes a RetentionEntry by original lease UUID. It is idempotent:
// no error is returned when the entry is absent. It reads the pre-image in-txn
// so the index can drop the deleted record's partition membership.
func (s *RetentionStore) Delete(orig string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldE *RetentionEntry
	err := s.update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		if raw := bkt.Get([]byte(orig)); raw != nil {
			oldE = &RetentionEntry{}
			if uerr := json.Unmarshal(raw, oldE); uerr != nil {
				return fmt.Errorf("malformed retention record %q: %w", orig, uerr)
			}
		}
		return bkt.Delete([]byte(orig))
	})
	if err != nil {
		return err
	}
	s.indexApply(orig, oldE, nil) // oldE=nil when absent → no-op
	return nil
}

// DeleteIfRestoring atomically removes a restore source finalizer only while
// the exact destination and generation still own it. It is the success-side
// counterpart to RevertToActiveWithResourceProfiles' generation CAS: a stale finalizer snapshot
// must never delete a newer restore attempt or a record whose authority has
// returned to active/reaping. deleted=false means absent or changed authority.
func (s *RetentionStore) DeleteIfRestoring(
	orig string,
	newLease string,
	expectGen int,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		deleted bool
		oldE    RetentionEntry
	)
	err := s.update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		if err := json.Unmarshal(raw, &oldE); err != nil {
			return fmt.Errorf("malformed retention record %q: %w", orig, err)
		}
		if err := validateRetentionEntryResourceProfiles(&oldE); err != nil {
			return fmt.Errorf("invalid retention record %q: %w", orig, err)
		}
		if oldE.Status != RetentionStatusRestoring ||
			oldE.NewLeaseUUID != newLease ||
			oldE.Generation != expectGen {
			return nil
		}
		deleted = true
		return bkt.Delete([]byte(orig))
	})
	if err != nil {
		return false, err
	}
	if deleted {
		s.indexApply(orig, &oldE, nil)
	}
	return deleted, nil
}

// List returns all RetentionEntry records in the store.
func (s *RetentionStore) List() ([]RetentionEntry, error) {
	return s.filter(func(_ *RetentionEntry) bool { return true })
}

// Keys returns every retained lease UUID (the bbolt key) without unmarshalling the heavy
// record value (skips the dominant per-record json.Unmarshal). string(k) copies, so no
// cursor bytes escape the transaction.
func (s *RetentionStore) Keys() ([]string, error) {
	var out []string
	err := s.view(func(tx *bolt.Tx) error {
		return tx.Bucket(retentionBucketName).ForEach(func(k, _ []byte) error {
			out = append(out, string(k))
			return nil
		})
	})
	return out, err
}

// KeysPage returns one keyset page of retained lease UUIDs (the bbolt keys) in
// ascending key order, containing the keys strictly greater than `after`. It
// uses a bbolt cursor Seek so the read is O(limit), not a full-bucket scan, and
// (like Keys) skips the heavy per-record value unmarshal.
//
//   - limit <= 0 -> returns ALL keys, next "" — the unpaginated passthrough. The
//     cursor is ignored in this mode, matching PaginateRetentions/keysetPage.
//   - limit  > 0 -> returns up to limit keys strictly greater than `after`; next
//     is the last returned key iff a full page was returned AND more keys remain,
//     otherwise "".
//
// The returned slice is always non-nil so callers serialize it as [] not null.
// Precondition: keys are canonical lease UUIDs — bbolt stores keys byte-sorted,
// which matches the client's keyset cursor order (canonical-lowercase UUID).
func (s *RetentionStore) KeysPage(after string, limit int) (keys []string, next string, err error) {
	keys = []string{}
	err = s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(retentionBucketName)
		if b == nil {
			return nil
		}
		c := b.Cursor()

		var k []byte
		// limit <= 0 is the unpaginated passthrough: return every key and ignore
		// the cursor, matching PaginateRetentions/keysetPage. (ParsePageParams
		// never pairs a non-empty cursor with limit<=0, so a cursor with limit<=0
		// only reaches a direct store caller — keep the two consistent anyway.)
		if after == "" || limit <= 0 {
			k, _ = c.First()
		} else {
			// Seek lands on the first key >= after; advance past an exact match so
			// the page starts strictly after the cursor (keyset semantics).
			k, _ = c.Seek([]byte(after))
			if k != nil && string(k) == after {
				k, _ = c.Next()
			}
		}

		for ; k != nil; k, _ = c.Next() {
			if limit > 0 && len(keys) == limit {
				next = keys[len(keys)-1] // full page + at least one more key remains
				return nil
			}
			keys = append(keys, string(k)) // string(k) copies; no cursor bytes escape the txn
		}
		return nil
	})
	return keys, next, err
}

// ListExpired returns active entries whose CreatedAt is older than maxAge.
func (s *RetentionStore) ListExpired(maxAge time.Duration) ([]RetentionEntry, error) {
	cutoff := time.Now().Add(-maxAge)
	return s.filter(func(e *RetentionEntry) bool {
		return e.Status == RetentionStatusActive && e.CreatedAt.Before(cutoff)
	})
}

// keysOf copies a set's keys (caller holds the lock).
func keysOf(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for u := range set {
		out = append(out, u)
	}
	return out
}

// getAll Gets each UUID from the primary in ONE db.View (so the per-record reads share a
// single MVCC snapshot and N txns collapse to 1), skipping nil (concurrently deleted) and
// re-applying keep on the fetched entry (it may have left the partition since the index
// snapshot). Eventually-consistent: the index snapshot and the View are separate
// observations; callers re-validate via CAS before destructive action (see doc comments).
func (s *RetentionStore) getAll(uuids []string, keep func(*RetentionEntry) bool) ([]RetentionEntry, error) {
	out := make([]RetentionEntry, 0, len(uuids))
	err := s.view(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		for _, u := range uuids {
			raw := bkt.Get([]byte(u))
			if raw == nil {
				continue
			}
			var e RetentionEntry
			if uerr := json.Unmarshal(raw, &e); uerr != nil {
				return fmt.Errorf("malformed retention record %q: %w", u, uerr)
			}
			if err := validateRetentionEntryResourceProfiles(&e); err != nil {
				return fmt.Errorf("invalid retention record %q: %w", u, err)
			}
			if keep(&e) {
				out = append(out, e)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ListByTenant returns all entries for the given tenant (eventually-consistent; callers
// must re-validate via CAS before mutating — see type doc). Served from the index.
func (s *RetentionStore) ListByTenant(tenant string) ([]RetentionEntry, error) {
	s.mu.RLock()
	uuids := keysOf(s.byTenant[tenant])
	s.mu.RUnlock()
	return s.getAll(uuids, func(e *RetentionEntry) bool { return e.Tenant == tenant })
}

// ListRestoring returns all entries currently restoring (eventually-consistent). Served from the index.
func (s *RetentionStore) ListRestoring() ([]RetentionEntry, error) {
	s.mu.RLock()
	uuids := keysOf(s.byStatus[RetentionStatusRestoring])
	s.mu.RUnlock()
	return s.getAll(uuids, func(e *RetentionEntry) bool { return e.Status == RetentionStatusRestoring })
}

// RestoringSourceByDestination returns the exact source finalizer currently
// owning destinationLease, or nil when the destination is free. A duplicate is
// durable corruption and fails closed rather than picking one by bbolt order.
// Callers still need their per-destination command fence across this read and
// subsequent admission; this store query is the durable half of that guard.
func (s *RetentionStore) RestoringSourceByDestination(destinationLease string) (*RetentionEntry, error) {
	entries, err := s.ListRestoring()
	if err != nil {
		return nil, err
	}
	var found *RetentionEntry
	for i := range entries {
		if entries[i].NewLeaseUUID != destinationLease {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf(
				"multiple restore sources %q and %q own destination %q",
				found.OriginalLeaseUUID, entries[i].OriginalLeaseUUID, destinationLease,
			)
		}
		entry := entries[i]
		found = &entry
	}
	return found, nil
}

// ListReaping returns all entries currently in the reaping (pending-destroy) state.
func (s *RetentionStore) ListReaping() ([]RetentionEntry, error) {
	return s.filter(func(e *RetentionEntry) bool {
		return e.Status == RetentionStatusReaping
	})
}

// DeleteIfActive atomically removes a record ONLY if it is still ACTIVE. Returns
// (names, deleted, err); deleted=false (nil names) when absent or not active (e.g.
// concurrently claimed for restore). Used by reconcileOrphanedRetentions (ENG-370)
// to prune an orphaned active record whose backing volumes have already vanished
// out-of-band — the ACTIVE-only CAS guarantees a concurrent restore (active→restoring)
// is never clobbered. The returned names are unused there: the volumes are already
// gone, so there is nothing to destroy.
func (s *RetentionStore) DeleteIfActive(orig string) ([]string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		names   []string
		deleted bool
		oldE    RetentionEntry
	)
	err := s.update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		var e RetentionEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return fmt.Errorf("malformed retention record %q: %w", orig, err)
		}
		if e.Status != RetentionStatusActive {
			return nil
		}
		oldE = e
		names = e.RetainedVolumeNames
		deleted = true
		return bkt.Delete([]byte(orig))
	})
	if err != nil {
		return nil, false, err
	}
	if deleted {
		s.indexApply(orig, &oldE, nil)
	}
	return names, deleted, nil
}

// filter iterates all bucket entries and returns those for which keep returns true.
func (s *RetentionStore) filter(keep func(*RetentionEntry) bool) ([]RetentionEntry, error) {
	var results []RetentionEntry
	err := s.view(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		return bkt.ForEach(func(k, v []byte) error {
			var e RetentionEntry
			if err := json.Unmarshal(v, &e); err != nil {
				return fmt.Errorf("malformed retention record %q: %w", string(k), err)
			}
			if err := validateRetentionEntryResourceProfiles(&e); err != nil {
				return fmt.Errorf("invalid retention record %q: %w", string(k), err)
			}
			if keep(&e) {
				results = append(results, e)
			}
			return nil
		})
	})
	return results, err
}

// ClaimForRestoreWithAuthority atomically binds the exact destination Items and
// ResourceProfiles while transitioning the source ACTIVE -> RESTORING. The
// destination snapshot is the durable recovery/finalization authority for the
// write-ahead window in which the restore succeeded but its active Release was
// not persisted. It is deliberately part of this same transaction: adopted
// bytes must never exist without either source or destination sizing authority.
func (s *RetentionStore) ClaimForRestoreWithAuthority(
	orig, newLease string,
	maxAge time.Duration,
	destinationItems []backend.LeaseItem,
	destinationResourceProfiles []SKUResourceSnapshot,
	destinationOperationID OperationID,
	destinationCallbackURL, destinationLifecycleCallbackURL string,
) (*RetentionEntry, error) {
	return s.ClaimForRestoreWithAuthorityAt(
		orig,
		newLease,
		maxAge,
		destinationItems,
		destinationResourceProfiles,
		destinationOperationID,
		destinationCallbackURL,
		destinationLifecycleCallbackURL,
		time.Now(),
	)
}

// ClaimForRestoreWithAuthorityAt is ClaimForRestoreWithAuthority with the
// durable operation-admission timestamp supplied explicitly. Docker reuses the
// operation intent's CreatedAt here and in the destination Release so a
// pre-side-effect release-capacity proof and every later finalizer retry encode
// byte-identical authority.
func (s *RetentionStore) ClaimForRestoreWithAuthorityAt(
	orig, newLease string,
	maxAge time.Duration,
	destinationItems []backend.LeaseItem,
	destinationResourceProfiles []SKUResourceSnapshot,
	destinationOperationID OperationID,
	destinationCallbackURL, destinationLifecycleCallbackURL string,
	destinationCreatedAt time.Time,
) (*RetentionEntry, error) {
	if orig == "" || newLease == "" {
		return nil, errors.New("restore source and destination lease UUIDs are required")
	}
	if orig == newLease {
		return nil, errors.New("restore source and destination lease UUIDs must differ")
	}
	if _, err := backend.ValidateOperationQuantities(destinationItems); err != nil {
		return nil, fmt.Errorf("validate restore destination quantities: %w", err)
	}
	if err := ValidateSKUResourceSnapshot(destinationItems, destinationResourceProfiles); err != nil {
		return nil, fmt.Errorf("validate restore destination resource profiles: %w", err)
	}
	resolvedLifecycleURL, err := backend.ResolveLifecycleCallbackURL(
		destinationCallbackURL,
		destinationLifecycleCallbackURL,
	)
	if err != nil {
		return nil, fmt.Errorf("validate restore destination callback pair: %w", err)
	}
	if destinationCallbackURL == "" || resolvedLifecycleURL == "" ||
		resolvedLifecycleURL != destinationLifecycleCallbackURL {
		return nil, errors.New("restore destination requires an exact operation/lifecycle callback pair")
	}
	if !destinationOperationID.Valid() {
		return nil, errors.New("restore destination requires a canonical UUIDv4 operation ID")
	}
	callbackOperationID, err := parseOperationCallbackID(destinationCallbackURL)
	if err != nil {
		return nil, fmt.Errorf("validate restore destination operation callback: %w", err)
	}
	if callbackOperationID != destinationOperationID {
		return nil, fmt.Errorf(
			"restore destination operation ID %q differs from callback authority %q",
			destinationOperationID,
			callbackOperationID,
		)
	}
	if destinationCreatedAt.IsZero() {
		return nil, errors.New("restore destination requires a durable admission timestamp")
	}
	return s.claimForRestoreWithAuthority(
		orig,
		newLease,
		maxAge,
		slices.Clone(destinationItems),
		CloneSKUResourceSnapshot(destinationResourceProfiles),
		destinationOperationID,
		destinationCallbackURL,
		destinationLifecycleCallbackURL,
		destinationCreatedAt,
	)
}

func (s *RetentionStore) claimForRestoreWithAuthority(
	orig, newLease string,
	maxAge time.Duration,
	destinationItems []backend.LeaseItem,
	destinationResourceProfiles []SKUResourceSnapshot,
	destinationOperationID OperationID,
	destinationCallbackURL, destinationLifecycleCallbackURL string,
	destinationCreatedAt time.Time,
) (*RetentionEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		out  *RetentionEntry
		oldE RetentionEntry
	)
	err := s.update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return ErrNoRetention
		}
		var e RetentionEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return fmt.Errorf("malformed retention record %q: %w", orig, err)
		}
		if err := validateRetentionEntryResourceProfiles(&e); err != nil {
			return fmt.Errorf("invalid retention record %q: %w", orig, err)
		}
		if e.Status != RetentionStatusActive {
			return ErrNotRestorable
		}
		if maxAge > 0 && time.Since(e.CreatedAt) >= maxAge {
			return ErrNoRetention // about to be reaped
		}
		oldE = e // value copy of the ACTIVE pre-image, before mutation
		e.Status = RetentionStatusRestoring
		e.NewLeaseUUID = newLease
		e.DestinationItems = slices.Clone(destinationItems)
		e.DestinationResourceProfiles = CloneSKUResourceSnapshot(destinationResourceProfiles)
		e.DestinationOperationID = destinationOperationID
		e.DestinationCallbackURL = destinationCallbackURL
		e.DestinationLifecycleCallbackURL = destinationLifecycleCallbackURL
		e.RestoringSince = destinationCreatedAt
		e.Generation++
		if err := validateRetentionEntryResourceProfiles(&e); err != nil {
			return fmt.Errorf("invalid claimed retention record %q: %w", orig, err)
		}
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		out = &e
		return bkt.Put([]byte(orig), data)
	})
	if err != nil {
		return nil, err
	}
	if out != nil {
		s.indexApply(orig, &oldE, out)
	}
	return out, nil
}

// UpdateRestoringDestinationCallbacks atomically moves the callback route for
// one exact restore destination generation. Maintenance may move a route to a
// new base, but it cannot rotate or downgrade the lifecycle authority, nor can
// it change the operation ID that committed the destination lineage.
//
// The caller must hold the destination command fence across this CAS and its
// subsequent actor admission. updated=false means the supplied source,
// destination, or generation no longer owns the finalizer.
func (s *RetentionStore) UpdateRestoringDestinationCallbacks(
	orig, newLease string,
	expectGeneration int,
	callbackURL, lifecycleCallbackURL string,
) (updated bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err = s.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(retentionBucketName)
		raw := bucket.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		var entry RetentionEntry
		if err := json.Unmarshal(raw, &entry); err != nil {
			return fmt.Errorf("malformed retention record %q: %w", orig, err)
		}
		if err := validateRetentionEntryResourceProfiles(&entry); err != nil {
			return fmt.Errorf("invalid retention record %q: %w", orig, err)
		}
		if entry.Status != RetentionStatusRestoring ||
			entry.NewLeaseUUID != newLease ||
			entry.Generation != expectGeneration {
			return nil
		}

		resolvedOperationURL, resolvedLifecycleURL, err := backend.ResolveMaintenanceCallbackURLs(
			entry.DestinationCallbackURL,
			entry.DestinationLifecycleCallbackURL,
			lifecycleCallbackURL,
		)
		if err != nil {
			return fmt.Errorf("validate restore destination callback move: %w", err)
		}
		if resolvedOperationURL != callbackURL || resolvedLifecycleURL != lifecycleCallbackURL {
			return errors.New("restore destination callback pair is not the canonical route for its lifecycle authority")
		}
		callbackOperationID, err := parseOperationCallbackID(callbackURL)
		if err != nil {
			return fmt.Errorf("validate restore destination operation callback: %w", err)
		}
		if callbackOperationID != entry.DestinationOperationID {
			return fmt.Errorf(
				"restore destination callback operation ID %q differs from finalizer authority %q",
				callbackOperationID,
				entry.DestinationOperationID,
			)
		}

		entry.DestinationCallbackURL = callbackURL
		entry.DestinationLifecycleCallbackURL = lifecycleCallbackURL
		if err := validateRetentionEntryResourceProfiles(&entry); err != nil {
			return fmt.Errorf("invalid updated retention record %q: %w", orig, err)
		}
		encoded, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("marshal updated retention record %q: %w", orig, err)
		}
		if err := bucket.Put([]byte(orig), encoded); err != nil {
			return err
		}
		updated = true
		return nil
	})
	return updated, err
}

// MarkReapingIfActive atomically transitions an ACTIVE record to reaping and
// returns its volume names for the caller to destroy AFTER the txn commits.
// ok=false (nil names) when absent or not active (e.g. concurrently claimed for
// restore). The record is NOT deleted — it is the finalizer tombstone that keeps
// the footprint counted until the volumes are confirmed gone. (ENG-376)
func (s *RetentionStore) MarkReapingIfActive(orig string) ([]string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		names []string
		ok    bool
		oldE  RetentionEntry
		newE  RetentionEntry
	)
	err := s.update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		if err := json.Unmarshal(raw, &oldE); err != nil {
			return fmt.Errorf("malformed retention record %q: %w", orig, err)
		}
		if err := validateRetentionEntryResourceProfiles(&oldE); err != nil {
			return fmt.Errorf("invalid retention record %q: %w", orig, err)
		}
		if oldE.Status != RetentionStatusActive {
			return nil
		}
		newE = oldE
		newE.Status = RetentionStatusReaping
		newE.ReapingSince = time.Now()
		names = newE.RetainedVolumeNames
		data, err := json.Marshal(newE)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		ok = true
		return bkt.Put([]byte(orig), data)
	})
	if err != nil {
		return nil, false, err
	}
	if ok {
		s.indexApply(orig, &oldE, &newE)
	}
	return names, ok, nil
}

// MarkReapingIfExpired atomically transitions an ACTIVE, expired record to
// reaping and returns its volume names for the caller to destroy AFTER the txn
// commits. The record is NOT deleted — it stays a
// counted tombstone until the volumes are confirmed gone. Returns ok=false when
// absent, not active, or not yet expired, and a no-op when maxAge<=0. (ENG-376)
func (s *RetentionStore) MarkReapingIfExpired(orig string, maxAge time.Duration) ([]string, bool, error) {
	if maxAge <= 0 {
		return nil, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		names []string
		ok    bool
		oldE  RetentionEntry
		newE  RetentionEntry
	)
	err := s.update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		if err := json.Unmarshal(raw, &oldE); err != nil {
			return fmt.Errorf("malformed retention record %q: %w", orig, err)
		}
		if err := validateRetentionEntryResourceProfiles(&oldE); err != nil {
			return fmt.Errorf("invalid retention record %q: %w", orig, err)
		}
		if oldE.Status != RetentionStatusActive {
			return nil
		}
		if time.Since(oldE.CreatedAt) < maxAge {
			return nil
		}
		newE = oldE
		newE.Status = RetentionStatusReaping
		newE.ReapingSince = time.Now()
		names = newE.RetainedVolumeNames
		data, err := json.Marshal(newE)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		ok = true
		return bkt.Put([]byte(orig), data)
	})
	if err != nil {
		return nil, false, err
	}
	if ok {
		s.indexApply(orig, &oldE, &newE)
	}
	return names, ok, nil
}

// RevertToActiveWithResourceProfiles is the restore-rollback commit. In
// addition to the generation CAS, it binds the transition to the exact
// destination lease and atomically persists the resource snapshot whose disk
// quotas the caller has just measured and applied.
//
// Pre-snapshot rows are backfilled in the same transaction that makes them
// Active, so there is no state in which retained accounting can observe the old
// row without the quota authority just established on disk. Rows that already
// carry a snapshot must match exactly; a stale caller may never replace durable
// sizing authority. Returns false without mutation when ownership changed.
func (s *RetentionStore) RevertToActiveWithResourceProfiles(
	orig string,
	expectNewLease string,
	expectGen int,
	resourceProfiles []SKUResourceSnapshot,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		swapped bool
		oldE    RetentionEntry
		newE    RetentionEntry
	)
	err := s.update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		if err := json.Unmarshal(raw, &oldE); err != nil {
			return fmt.Errorf("malformed retention record %q: %w", orig, err)
		}
		if err := validateRetentionEntryResourceProfiles(&oldE); err != nil {
			return fmt.Errorf("invalid retention record %q: %w", orig, err)
		}
		if oldE.Status != RetentionStatusRestoring {
			return nil
		}
		if oldE.Generation != expectGen || oldE.NewLeaseUUID != expectNewLease {
			return nil
		}
		newE = oldE
		if err := ValidateSKUResourceSnapshot(oldE.Items, resourceProfiles); err != nil {
			return fmt.Errorf("invalid rollback resource profiles for retention record %q: %w", orig, err)
		}
		if len(oldE.ResourceProfiles) > 0 && !slices.Equal(oldE.ResourceProfiles, resourceProfiles) {
			return fmt.Errorf("rollback resource profiles differ from durable retention record %q", orig)
		}
		newE.ResourceProfiles = CloneSKUResourceSnapshot(resourceProfiles)
		newE.Status = RetentionStatusActive
		newE.Generation++
		newE.NewLeaseUUID = ""
		newE.DestinationItems = nil
		newE.DestinationResourceProfiles = nil
		newE.DestinationOperationID = ""
		newE.DestinationCallbackURL = ""
		newE.DestinationLifecycleCallbackURL = ""
		newE.RestoringSince = time.Time{}
		if err := validateRetentionEntryResourceProfiles(&newE); err != nil {
			return fmt.Errorf("invalid reverted retention record %q: %w", orig, err)
		}
		data, err := json.Marshal(newE)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		swapped = true
		return bkt.Put([]byte(orig), data)
	})
	if err != nil {
		return false, err
	}
	if swapped {
		s.indexApply(orig, &oldE, &newE)
	}
	return swapped, nil
}
