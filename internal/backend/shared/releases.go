package shared

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/util"
)

var releasesBucketName = []byte("releases")

// ErrReleaseHistoryCapacity identifies a release-history write whose
// load-bearing records cannot fit within the authoritative per-lease byte
// contract. Callers may safely treat this as a definitive pre-admission
// refusal only when it is returned by a Check*Capacity method; a write after a
// substrate mutation still has an ambiguous outcome until durable recovery
// proves otherwise.
var ErrReleaseHistoryCapacity = errors.New("release history capacity exceeded")

// ReleaseHistoryCapacityError reports the minimum encoded bytes needed for
// the records that cannot be discarded. It unwraps to
// [ErrReleaseHistoryCapacity] for stable classification while retaining exact
// operator diagnostics through errors.As.
type ReleaseHistoryCapacityError struct {
	LimitBytes    int
	RequiredBytes int
}

func (e *ReleaseHistoryCapacityError) Error() string {
	if e == nil {
		return ErrReleaseHistoryCapacity.Error()
	}
	return fmt.Sprintf(
		"%s: protected release authority needs %d bytes (limit %d)",
		ErrReleaseHistoryCapacity,
		e.RequiredBytes,
		e.LimitBytes,
	)
}

func (e *ReleaseHistoryCapacityError) Unwrap() error {
	return ErrReleaseHistoryCapacity
}

// Release represents a single release (deployment) of a lease.
type Release struct {
	Version  int    `json:"version"`
	Manifest []byte `json:"manifest"`
	Image    string `json:"image"`
	// MaintenanceID identifies the exact restart/update replacement cohort.
	// Provision/restore rows omit it; a maintenance generation may carry either
	// current or v0.13 runtime authority. A maintenance ID is immutable across
	// deploying, active, failed, and superseded states and is also placed on
	// every replacement container.
	MaintenanceID MaintenanceID `json:"maintenance_id,omitzero"`
	// OperationID is the exact causal token of the provision/restore generation
	// that first committed this release lineage. Maintenance releases preserve it.
	// Empty is accepted only for v0.13/legacy rows.
	OperationID OperationID `json:"operation_id,omitzero"`
	// Items is the immutable emitted instance topology for this release. It equals
	// the requested topology except that a DNS-deferred custom domain is empty,
	// matching the labels and routers actually committed to the substrate. New
	// writers persist it before reporting success so recovery can distinguish a
	// complete cohort from a set of surviving containers. It remains optional on
	// disk for releases written by older binaries.
	Items []backend.LeaseItem `json:"items,omitempty"`
	// ResourceProfiles is the immutable resource authority paired with Items.
	// New writers always persist it before reporting success. It remains
	// optional only for release rows written by v0.13 and older binaries; Docker
	// recovery snapshots the then-current configuration when migrating those
	// rows so subsequent restarts and closes cannot reprice a live lease.
	ResourceProfiles []SKUResourceSnapshot `json:"resource_profiles,omitempty"`
	// RuntimeAuthority is the current non-expiring identity needed to reconstruct a
	// terminal lease projection when an exact committed generation has no
	// surviving containers. It is a nested all-or-nothing snapshot so JSON cannot
	// represent a partially-authoritative combination of tenant, provider, and
	// callback routes. A v0.13 row uses LegacyRuntimeAuthority instead.
	RuntimeAuthority *ReleaseRuntimeAuthority `json:"runtime_authority,omitempty"`
	// LegacyRuntimeAuthority is the separately typed, tokenless identity frozen
	// from a complete v0.13 cohort. It is never interchangeable with
	// RuntimeAuthority: legacy rows have no OperationID and cannot be granted a
	// UUID-scoped capability they never received. Persisting this exact pair is
	// nevertheless required before removing the last legacy container, because
	// otherwise a later restart could not reconstruct the active release's
	// principal, callback route, or resource ownership.
	LegacyRuntimeAuthority *LegacyRuntimeAuthority `json:"legacy_runtime_authority,omitempty"`
	Status                 string                  `json:"status"`
	CreatedAt              time.Time               `json:"created_at"`
	Error                  string                  `json:"error,omitempty"`
	Reason                 backend.Reason          `json:"reason,omitempty"`
	Message                string                  `json:"message,omitempty"`
}

type releaseHistoryEnvelope struct {
	SchemaVersion uint8     `json:"schema_version"`
	Releases      []Release `json:"releases"`
}

func marshalReleaseHistory(releases []Release) ([]byte, error) {
	return json.Marshal(releaseHistoryEnvelope{
		SchemaVersion: authoritativeRowSchemaVersion,
		Releases:      releases,
	})
}

func decodeReleaseHistory(value []byte) ([]Release, error) {
	var envelope releaseHistoryEnvelope
	if err := decodeStrictAuthoritativeObject(
		value, maxAuthoritativeRecordBytes, &envelope,
	); err != nil {
		return nil, err
	}
	if envelope.SchemaVersion != authoritativeRowSchemaVersion {
		return nil, fmt.Errorf(
			"unsupported release-history schema version %d",
			envelope.SchemaVersion,
		)
	}
	return envelope.Releases, nil
}

// ReleaseRuntimeAuthority is the immutable observational identity paired with
// one operation-token lineage. Callback URLs contain unguessable capabilities
// and make the release database a sensitive backup artifact.
type ReleaseRuntimeAuthority struct {
	operationID          OperationID
	tenant               string
	providerUUID         string
	callbackURL          string
	lifecycleCallbackURL string
	valid                bool
}

type releaseRuntimeAuthorityJSON struct {
	SchemaVersion        uint8  `json:"schema_version"`
	Tenant               string `json:"tenant"`
	ProviderUUID         string `json:"provider_uuid"`
	CallbackURL          string `json:"callback_url"`
	LifecycleCallbackURL string `json:"lifecycle_callback_url"`
}

// LegacyRuntimeAuthority is an immutable, tokenless v0.13 runtime identity.
// Its zero value is invalid and all fields are private so a caller cannot mix a
// principal from one cohort with callback authority from another.
type LegacyRuntimeAuthority struct {
	tenant               string
	providerUUID         string
	callbackURL          string
	lifecycleCallbackURL string
	valid                bool
}

type legacyRuntimeAuthorityJSON struct {
	SchemaVersion        uint8  `json:"schema_version"`
	Tenant               string `json:"tenant"`
	ProviderUUID         string `json:"provider_uuid"`
	CallbackURL          string `json:"callback_url"`
	LifecycleCallbackURL string `json:"lifecycle_callback_url"`
}

// NewLegacyRuntimeAuthority validates and freezes one coherent tokenless
// operation/lifecycle callback pair. Typed callback identities are rejected:
// they belong in ReleaseRuntimeAuthority together with their OperationID.
func NewLegacyRuntimeAuthority(
	tenant, providerUUID, callbackURL, lifecycleCallbackURL string,
) (LegacyRuntimeAuthority, error) {
	if tenant == "" || providerUUID == "" || callbackURL == "" {
		return LegacyRuntimeAuthority{}, errors.New("legacy runtime authority must be complete")
	}
	if !backend.IsCanonicalLeaseUUID(providerUUID) {
		return LegacyRuntimeAuthority{}, errors.New("legacy runtime authority provider UUID is not canonical")
	}
	resolvedLifecycle, err := backend.ResolveLifecycleCallbackURL(callbackURL, lifecycleCallbackURL)
	if err != nil {
		return LegacyRuntimeAuthority{}, fmt.Errorf("legacy runtime authority callback pair: %w", err)
	}
	observation := backend.ObserveLifecycleGeneration(callbackURL, resolvedLifecycle)
	if observation.Kind != backend.LifecycleGenerationLegacy {
		return LegacyRuntimeAuthority{}, errors.New("legacy runtime authority callback pair is not tokenless")
	}
	return LegacyRuntimeAuthority{
		tenant:               tenant,
		providerUUID:         providerUUID,
		callbackURL:          callbackURL,
		lifecycleCallbackURL: resolvedLifecycle,
		valid:                true,
	}, nil
}

func (authority LegacyRuntimeAuthority) Tenant() string {
	if !authority.valid {
		return ""
	}
	return authority.tenant
}

func (authority LegacyRuntimeAuthority) ProviderUUID() string {
	if !authority.valid {
		return ""
	}
	return authority.providerUUID
}

func (authority LegacyRuntimeAuthority) CallbackURL() string {
	if !authority.valid {
		return ""
	}
	return authority.callbackURL
}

func (authority LegacyRuntimeAuthority) LifecycleCallbackURL() string {
	if !authority.valid {
		return ""
	}
	return authority.lifecycleCallbackURL
}

func (authority LegacyRuntimeAuthority) MarshalJSON() ([]byte, error) {
	if !authority.valid {
		return nil, errors.New("legacy runtime authority is invalid")
	}
	return json.Marshal(legacyRuntimeAuthorityJSON{
		SchemaVersion:        authoritativeRowSchemaVersion,
		Tenant:               authority.tenant,
		ProviderUUID:         authority.providerUUID,
		CallbackURL:          authority.callbackURL,
		LifecycleCallbackURL: authority.lifecycleCallbackURL,
	})
}

func (authority *LegacyRuntimeAuthority) UnmarshalJSON(data []byte) error {
	if authority == nil {
		return errors.New("legacy runtime authority destination is nil")
	}
	var wire legacyRuntimeAuthorityJSON
	if err := decodeStrictAuthoritativeObject(data, maxAuthoritativeRecordBytes, &wire); err != nil {
		return err
	}
	if wire.SchemaVersion != authoritativeRowSchemaVersion {
		return fmt.Errorf("unsupported legacy runtime authority schema version %d", wire.SchemaVersion)
	}
	parsed, err := NewLegacyRuntimeAuthority(
		wire.Tenant,
		wire.ProviderUUID,
		wire.CallbackURL,
		wire.LifecycleCallbackURL,
	)
	if err != nil {
		return err
	}
	*authority = parsed
	return nil
}

// NewReleaseRuntimeAuthority constructs the immutable runtime identity paired
// with one typed release lineage. Its zero value is invalid, and its fields are
// deliberately private so callers cannot detach a validated callback pair from
// the operation token it names.
func NewReleaseRuntimeAuthority(
	operationID OperationID,
	tenant, providerUUID, callbackURL, lifecycleCallbackURL string,
) (ReleaseRuntimeAuthority, error) {
	if !operationID.Valid() {
		return ReleaseRuntimeAuthority{}, errors.New("runtime authority requires a canonical UUIDv4 operation ID")
	}
	if tenant == "" || providerUUID == "" || callbackURL == "" || lifecycleCallbackURL == "" {
		return ReleaseRuntimeAuthority{}, errors.New("runtime authority must be complete")
	}
	if !backend.IsCanonicalLeaseUUID(providerUUID) {
		return ReleaseRuntimeAuthority{}, errors.New("runtime authority provider UUID is not canonical")
	}
	if err := validateCallbackDestination(callbackURL); err != nil {
		return ReleaseRuntimeAuthority{}, fmt.Errorf("runtime authority operation callback: %w", err)
	}
	callbackOperationID, err := parseOperationCallbackID(callbackURL)
	if err != nil {
		return ReleaseRuntimeAuthority{}, fmt.Errorf("runtime authority operation callback: %w", err)
	}
	if callbackOperationID != operationID {
		return ReleaseRuntimeAuthority{}, errors.New("runtime authority callback does not match release operation ID")
	}
	resolvedLifecycle, err := backend.ResolveLifecycleCallbackURL(callbackURL, lifecycleCallbackURL)
	if err != nil {
		return ReleaseRuntimeAuthority{}, fmt.Errorf("runtime authority callback pair: %w", err)
	}
	if resolvedLifecycle != lifecycleCallbackURL {
		return ReleaseRuntimeAuthority{}, errors.New("runtime authority lifecycle callback is not explicit")
	}
	return ReleaseRuntimeAuthority{
		operationID:          operationID,
		tenant:               tenant,
		providerUUID:         providerUUID,
		callbackURL:          callbackURL,
		lifecycleCallbackURL: lifecycleCallbackURL,
		valid:                true,
	}, nil
}

func (authority ReleaseRuntimeAuthority) OperationID() OperationID {
	if !authority.valid {
		return OperationID{}
	}
	return authority.operationID
}

func (authority ReleaseRuntimeAuthority) Tenant() string {
	if !authority.valid {
		return ""
	}
	return authority.tenant
}

func (authority ReleaseRuntimeAuthority) ProviderUUID() string {
	if !authority.valid {
		return ""
	}
	return authority.providerUUID
}

func (authority ReleaseRuntimeAuthority) CallbackURL() string {
	if !authority.valid {
		return ""
	}
	return authority.callbackURL
}

func (authority ReleaseRuntimeAuthority) LifecycleCallbackURL() string {
	if !authority.valid {
		return ""
	}
	return authority.lifecycleCallbackURL
}

func (authority ReleaseRuntimeAuthority) MarshalJSON() ([]byte, error) {
	if !authority.valid {
		return nil, errors.New("runtime authority is invalid")
	}
	return json.Marshal(releaseRuntimeAuthorityJSON{
		SchemaVersion:        authoritativeRowSchemaVersion,
		Tenant:               authority.tenant,
		ProviderUUID:         authority.providerUUID,
		CallbackURL:          authority.callbackURL,
		LifecycleCallbackURL: authority.lifecycleCallbackURL,
	})
}

func (authority *ReleaseRuntimeAuthority) UnmarshalJSON(data []byte) error {
	if authority == nil {
		return errors.New("runtime authority destination is nil")
	}
	var wire releaseRuntimeAuthorityJSON
	if err := decodeStrictAuthoritativeObject(data, maxAuthoritativeRecordBytes, &wire); err != nil {
		return err
	}
	if wire.SchemaVersion != authoritativeRowSchemaVersion {
		return fmt.Errorf("unsupported runtime authority schema version %d", wire.SchemaVersion)
	}
	operationID, err := parseOperationCallbackID(wire.CallbackURL)
	if err != nil {
		return err
	}
	parsed, err := NewReleaseRuntimeAuthority(
		operationID,
		wire.Tenant,
		wire.ProviderUUID,
		wire.CallbackURL,
		wire.LifecycleCallbackURL,
	)
	if err != nil {
		return err
	}
	*authority = parsed
	return nil
}

// ReleaseStore persists release history in bbolt so it survives backend restarts.
type ReleaseStore struct {
	*boltStore
	cleanupInterval time.Duration
	onCleanupPanic  util.PanicHandler
	cleanupOnce     sync.Once
}

// ReleaseStoreConfig configures the release store.
type ReleaseStoreConfig struct {
	DBPath          string
	MaxAge          time.Duration
	CleanupInterval time.Duration
	OnCleanupPanic  util.PanicHandler // Optional: invoked on cleanup-loop panic.
}

// OpenIdentityBoundReleaseStore opens an initialized authoritative release
// journal without creating or repairing it. Maintenance remains paused until
// StartMaintenance so callers can verify their complete store set first.
func OpenIdentityBoundReleaseStore(
	cfg ReleaseStoreConfig,
	storage backendidentity.VerifiedStorage,
	gate *backendidentity.StorageAuthorityGate,
) (*ReleaseStore, error) {
	if !storage.Valid() {
		return nil, errors.New("verified backend storage authority is required")
	}
	if gate == nil || !gate.Valid() {
		return nil, errors.New("backend storage authority gate is required")
	}
	return newReleaseStore(cfg, storage, gate)
}

func newReleaseStore(
	cfg ReleaseStoreConfig,
	storage backendidentity.VerifiedStorage,
	gate *backendidentity.StorageAuthorityGate,
) (*ReleaseStore, error) {
	if !storage.Valid() {
		return nil, errors.New("verified backend storage authority is required")
	}
	if gate == nil || !gate.Valid() {
		return nil, errors.New("backend storage authority gate is required")
	}
	storeCfg := boltStoreConfig{
		DBPath:     cfg.DBPath,
		BucketName: releasesBucketName,
		MaxAge:     cfg.MaxAge,
		Label:      "releases",
	}
	base, err := openIdentityBoundBoltStore(
		storeCfg, authoritativeStoreReleases, storage, gate,
	)
	if err != nil {
		return nil, err
	}
	store := &ReleaseStore{
		boltStore:       base,
		cleanupInterval: cfg.CleanupInterval,
		onCleanupPanic:  cfg.OnCleanupPanic,
	}
	if err := store.view(func(tx *bolt.Tx) error {
		if err := validateReleaseRootBuckets(tx); err != nil {
			return err
		}
		return inspectReleaseBucket(tx)
	}); err != nil {
		_ = base.Close()
		return nil, fmt.Errorf("validate bound release journal: %w", err)
	}
	return store, nil
}

func validateReleaseRootBuckets(tx *bolt.Tx) error {
	return validateAuthoritativeRootBuckets(
		tx, "release", releasesBucketName, storeIdentityBucketName,
	)
}

// StartMaintenance performs the initial pruning pass and starts the periodic
// loop exactly once.
func (s *ReleaseStore) StartMaintenance() {
	if s == nil || s.maxAge <= 0 {
		return
	}
	s.cleanupOnce.Do(func() {
		s.startCleanup("releases", s.cleanupInterval, s.removeOlderThan, s.onCleanupPanic)
	})
}

// Healthy validates the complete recovery journal, not only bbolt reachability.
// A release manifest is cleanup and restart authority; leaving a semantically
// invalid row health-green would defer the failure until the next recovery or
// close, after the original operation may already have committed externally.
func (s *ReleaseStore) Healthy() error {
	return s.view(func(tx *bolt.Tx) error {
		if err := validateReleaseRootBuckets(tx); err != nil {
			return err
		}
		return inspectReleaseBucket(tx)
	})
}

// ReleaseStoreInspection is what a read-only look at a release journal found.
// ActiveLeaseUUIDs is filled only for an identity-bound journal.
type ReleaseStoreInspection struct {
	Exists           bool
	IdentityBound    bool
	ActiveLeaseUUIDs map[string]struct{}
}

// InspectReleaseStoreReadOnly validates every durable history without
// creating a database, bucket, binding, or cleanup goroutine.
func InspectReleaseStoreReadOnly(dbPath string) (ReleaseStoreInspection, error) {
	return inspectReleaseStoreReadOnlyFile(pathnameAuthoritativeStoreFile(dbPath))
}

// InspectBoundReleaseStoreReadOnly is the descriptor-relative form used by a
// storage-lineage initialization after retaining the journal parent.
func InspectBoundReleaseStoreReadOnly(
	path *BoundAuthoritativeStorePath,
) (ReleaseStoreInspection, error) {
	file, err := boundAuthoritativeStoreFile(path)
	if err != nil {
		return ReleaseStoreInspection{}, err
	}
	return inspectReleaseStoreReadOnlyFile(file)
}

func inspectReleaseStoreReadOnlyFile(file authoritativeStoreFile) (ReleaseStoreInspection, error) {
	if _, err := file.Lstat(); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ReleaseStoreInspection{}, nil
		}
		return ReleaseStoreInspection{}, fmt.Errorf("stat release database: %w", err)
	}
	db, _, err := openExistingBoltDBFile(file, true, false)
	if err != nil {
		return ReleaseStoreInspection{}, fmt.Errorf("open release database read-only: %w", err)
	}
	defer func() { _ = db.Close() }()
	inspection := ReleaseStoreInspection{
		Exists:           true,
		ActiveLeaseUUIDs: make(map[string]struct{}),
	}
	err = db.View(func(tx *bolt.Tx) error {
		inspection.IdentityBound = tx.Bucket(storeIdentityBucketName) != nil
		if !inspection.IdentityBound {
			// A journal without a storage identity was not written by this
			// release line. Its existence is reported; its rows are not read.
			return nil
		}
		budget := newStoppedAuthoritativeInspectionBudget()
		return inspectReleaseBucketWithObserver(tx, inspection.ActiveLeaseUUIDs, budget.observe)
	})
	if err != nil {
		return ReleaseStoreInspection{}, fmt.Errorf("inspect release database: %w", err)
	}
	return inspection, nil
}

func inspectReleaseBucket(tx *bolt.Tx) error {
	return inspectReleaseBucketWithObserver(tx, nil, func(key, value []byte) error {
		return validateAuthoritativeRecord(key, value, maxAuthoritativeRecordBytes)
	})
}

// inspectReleaseBucketWithObserver validates every history in the current
// schema and, when active is non-nil, records which leases have an active
// release.
func inspectReleaseBucketWithObserver(
	tx *bolt.Tx,
	active map[string]struct{},
	observe func(key, value []byte) error,
) error {
	bucket := tx.Bucket(releasesBucketName)
	if bucket == nil {
		return errors.New("releases bucket is missing")
	}
	if observe == nil {
		return errors.New("release record observer is required")
	}
	return bucket.ForEach(func(key, value []byte) error {
		if err := observe(key, value); err != nil {
			return err
		}
		if value == nil {
			return fmt.Errorf("release history for lease %q is a nested bucket", string(key))
		}
		if !backend.IsCanonicalLeaseUUID(string(key)) {
			return fmt.Errorf("release history key with length %d is not a canonical lease UUID", len(key))
		}
		history, err := parseCurrentReleaseHistory(value)
		if err != nil {
			return fmt.Errorf("release history for lease %q: %w", string(key), err)
		}
		if active != nil && latestActiveReleaseIndex(history.strictReleases()) >= 0 {
			active[string(key)] = struct{}{}
		}
		return nil
	})
}

func PrepareBoundReleaseStoreStorage(
	path *BoundAuthoritativeStorePath,
	storage backendidentity.PendingStorage,
) error {
	if !storage.Valid() {
		return errors.New("pending backend storage authority is required")
	}
	return initializeIdentityBoundBoltStoreBound(
		path, releasesBucketName, "releases", authoritativeStoreReleases, storage.ID(), true,
		func(tx *bolt.Tx) error {
			if err := validateReleaseRootBuckets(tx); err != nil {
				return err
			}
			return inspectReleaseBucket(tx)
		},
	)
}

func CheckBoundReleaseStoreStorage(
	path *BoundAuthoritativeStorePath,
	storage backendidentity.PendingStorage,
) error {
	if !storage.Valid() {
		return errors.New("pending backend storage authority is required")
	}
	return checkIdentityBoundBoltStoreBound(
		path, releasesBucketName, "releases", authoritativeStoreReleases, storage.ID(),
		func(tx *bolt.Tx) error {
			if err := validateReleaseRootBuckets(tx); err != nil {
				return err
			}
			return inspectReleaseBucket(tx)
		},
	)
}

func VerifyBoundReleaseStoreStorage(
	path *BoundAuthoritativeStorePath,
	storage backendidentity.VerifiedStorage,
) error {
	if !storage.Valid() {
		return errors.New("verified backend storage authority is required")
	}
	return checkIdentityBoundBoltStoreBound(
		path, releasesBucketName, "releases", authoritativeStoreReleases, storage.ID(),
		func(tx *bolt.Tx) error {
			if err := validateReleaseRootBuckets(tx); err != nil {
				return err
			}
			return inspectReleaseBucket(tx)
		},
	)
}

func VerifyReleaseStoreStorage(dbPath string, storage backendidentity.VerifiedStorage) error {
	return verifyIdentityBoundBoltStore(
		dbPath, releasesBucketName, "releases", authoritativeStoreReleases, storage,
		func(tx *bolt.Tx) error {
			if err := validateReleaseRootBuckets(tx); err != nil {
				return err
			}
			return inspectReleaseBucket(tx)
		},
	)
}

// maxVersion returns the highest Version among releases, or 0 for an empty slice.
// Append derives the next version from this (not len) so that within-key pruning by
// RemoveOlderThan's keep-latest guard can never cause a version to be reused: the
// pruning always retains the index-latest entry, which holds the maximum version, so
// maxVersion(remaining)+1 stays strictly greater than every version ever issued for
// the lease (ENG-440). Uses the Go 1.21 max builtin (do not name the accumulator
// `max`, which would shadow it).
func maxVersion(releases []Release) int {
	highest := 0
	for _, r := range releases {
		highest = max(highest, r.Version)
	}
	return highest
}

// validateStoredRelease validates every durable field that is later consumed as
// recovery, cleanup, or accounting authority. Empty resource profiles remain
// readable only for v0.13 rows; whenever a snapshot is present it must exactly
// and canonically cover valid desired items.
func validateStoredRelease(release Release) error {
	if !release.MaintenanceID.IsZero() && !release.MaintenanceID.Valid() {
		return errors.New("maintenance ID is not a canonical UUIDv4")
	}
	if !release.MaintenanceID.IsZero() && release.OperationID.IsZero() &&
		release.LegacyRuntimeAuthority == nil {
		return errors.New("maintenance ID requires durable runtime authority")
	}
	typed := !release.OperationID.IsZero()
	if legacy := release.LegacyRuntimeAuthority; legacy != nil {
		if !legacy.valid {
			return errors.New("legacy runtime authority is invalid")
		}
		validated, err := NewLegacyRuntimeAuthority(
			legacy.tenant,
			legacy.providerUUID,
			legacy.callbackURL,
			legacy.lifecycleCallbackURL,
		)
		if err != nil {
			return fmt.Errorf("legacy runtime authority: %w", err)
		}
		if validated != *legacy {
			return errors.New("legacy runtime authority is not canonical")
		}
	}
	if release.OperationID.IsZero() {
		if release.RuntimeAuthority != nil {
			return errors.New("legacy release cannot carry runtime authority")
		}
	} else {
		if release.LegacyRuntimeAuthority != nil {
			return errors.New("typed release cannot carry legacy runtime authority")
		}
		if !release.OperationID.Valid() {
			return errors.New("operation ID is not a canonical UUIDv4")
		}
		if release.RuntimeAuthority == nil {
			return errors.New("typed release requires runtime authority")
		}
		if release.RuntimeAuthority.OperationID() != release.OperationID {
			return errors.New("runtime authority does not match release operation ID")
		}
	}
	if len(release.Items) == 0 {
		if typed {
			return errors.New("typed release requires exact desired items")
		}
		if release.LegacyRuntimeAuthority != nil {
			return errors.New("legacy runtime authority requires exact desired items")
		}
		if len(release.ResourceProfiles) > 0 {
			return errors.New("resource profiles require desired items")
		}
		return validateStoredReleaseManifest(release)
	}
	if _, err := backend.ValidateOperationQuantities(release.Items); err != nil {
		return fmt.Errorf("desired item quantities: %w", err)
	}
	if err := validateStoredReleaseManifest(release); err != nil {
		return err
	}
	if len(release.ResourceProfiles) == 0 {
		if release.LegacyRuntimeAuthority != nil {
			return errors.New("legacy runtime authority requires exact resource profiles")
		}
		if typed {
			return errors.New("typed release requires exact resource profiles")
		}
		return nil // An untyped release may record none; Docker recovery refuses an active one that has items.
	}
	if err := ValidateSKUResourceSnapshot(release.Items, release.ResourceProfiles); err != nil {
		return fmt.Errorf("resource profiles: %w", err)
	}
	return nil
}

func validateStoredReleaseManifest(release Release) error {
	if len(release.Manifest) == 0 {
		if len(release.Items) == 0 {
			return nil // Legacy status-only rows carry no recovery topology.
		}
		return errors.New("release manifest is required with desired items")
	}
	stack, err := manifest.ParseStoredPayload(release.Manifest)
	if err != nil {
		return fmt.Errorf("release manifest: %w", err)
	}
	if len(release.Items) == 0 {
		return nil // v0.13 manifest: parseable, but no exact topology was persisted.
	}
	if err := manifest.ValidateStackAgainstItems(stack, release.Items); err != nil {
		return fmt.Errorf("release manifest topology: %w", err)
	}
	return nil
}

func validateReleaseHistory(releases []Release) error {
	activeIndexes, err := validateReleaseHistoryRows(releases)
	if err != nil {
		return err
	}
	if len(activeIndexes) > 1 {
		return fmt.Errorf("release history has %d active records", len(activeIndexes))
	}
	return nil
}

// validateReleaseHistoryRows checks every invariant except the single-active
// rule and returns the active row indexes in history order. Its two callers
// decide what more than one active row means: validateReleaseHistory refuses
// it, and adoptV013ReleaseHistory asks the v0.13 recognizers.
func validateReleaseHistoryRows(releases []Release) ([]int, error) {
	if len(releases) == 0 {
		return nil, errors.New("release history is empty")
	}
	previousVersion := 0
	activeIndexes := make([]int, 0, 2)
	maintenanceIDs := make(map[MaintenanceID]int)
	for index := range releases {
		if err := validateStoredRelease(releases[index]); err != nil {
			return nil, fmt.Errorf("release %d: %w", releases[index].Version, err)
		}
		if releases[index].Version <= previousVersion {
			return nil, fmt.Errorf("release history has non-increasing version %d", releases[index].Version)
		}
		previousVersion = releases[index].Version
		if maintenanceID := releases[index].MaintenanceID; !maintenanceID.IsZero() {
			if previous, exists := maintenanceIDs[maintenanceID]; exists {
				return nil, fmt.Errorf(
					"release %d duplicates maintenance ID from release %d",
					releases[index].Version, previous,
				)
			}
			maintenanceIDs[maintenanceID] = releases[index].Version
		}
		switch releases[index].Status {
		case "deploying", "active", "failed", "superseded":
		default:
			return nil, fmt.Errorf("release %d has unsupported status", releases[index].Version)
		}
		if releases[index].Status == "active" {
			activeIndexes = append(activeIndexes, index)
		}
	}
	return activeIndexes, nil
}

// strictReleaseHistory is a history that passed strict validation, so it holds
// at most one active row. newStrictReleaseHistory and adoptV013ReleaseHistory
// are its only constructors, and each owns a deep copy of its rows, so neither
// the caller's slice nor a reader's copy can change a validated history. Every
// read-only inspection reports from it, and the stopped-adoption binding
// transaction compacts and writes it, so the readiness report, the capacity
// proof, and the bound store cannot disagree about which rows change.
type strictReleaseHistory struct {
	strictRows []Release
}

// strictReleases returns a deep copy of the validated rows.
func (h strictReleaseHistory) strictReleases() []Release {
	return cloneReleaseHistory(h.strictRows)
}

func newStrictReleaseHistory(releases []Release) (strictReleaseHistory, error) {
	owned := cloneReleaseHistory(releases)
	if err := validateReleaseHistory(owned); err != nil {
		return strictReleaseHistory{}, fmt.Errorf("validate release history: %w", err)
	}
	return strictReleaseHistory{strictRows: owned}, nil
}

func cloneReleaseHistory(releases []Release) []Release {
	if releases == nil {
		return nil
	}
	cloned := make([]Release, len(releases))
	for index := range releases {
		cloned[index] = cloneRelease(releases[index])
	}
	return cloned
}

func parseCurrentReleaseHistory(value []byte) (strictReleaseHistory, error) {
	releases, err := decodeReleaseHistory(value)
	if err != nil {
		return strictReleaseHistory{}, fmt.Errorf("decode release history: %w", err)
	}
	return newStrictReleaseHistory(releases)
}

func latestActiveReleaseIndex(releases []Release) int {
	for index := len(releases) - 1; index >= 0; index-- {
		if releases[index].Status == "active" {
			return index
		}
	}
	return -1
}

func cloneRelease(release Release) Release {
	release.Manifest = slices.Clone(release.Manifest)
	release.Items = slices.Clone(release.Items)
	release.ResourceProfiles = CloneSKUResourceSnapshot(release.ResourceProfiles)
	release.RuntimeAuthority = cloneReleaseRuntimeAuthority(release.RuntimeAuthority)
	release.LegacyRuntimeAuthority = cloneLegacyRuntimeAuthority(release.LegacyRuntimeAuthority)
	return release
}

func cloneReleaseRuntimeAuthority(authority *ReleaseRuntimeAuthority) *ReleaseRuntimeAuthority {
	if authority == nil {
		return nil
	}
	copy := *authority
	return &copy
}

func cloneLegacyRuntimeAuthority(authority *LegacyRuntimeAuthority) *LegacyRuntimeAuthority {
	if authority == nil {
		return nil
	}
	copy := *authority
	return &copy
}

// encodeReleaseHistory applies the same per-record ceiling enforced by stopped
// inspection before any writer reaches bbolt. Without this write-side guard a
// sequence of individually-valid manifests could produce a history that the
// next startup refuses to inspect. Returning before Put leaves the prior value
// byte-for-byte intact because every caller runs inside one bbolt transaction.
func encodeReleaseHistory(releases []Release) ([]byte, error) {
	return encodeReleaseHistoryWithinLimit(releases, backend.MaxStoredReleaseHistoryBytes)
}

func encodeReleaseHistoryWithinLimit(releases []Release, limitBytes int) ([]byte, error) {
	if limitBytes <= 0 {
		return nil, &ReleaseHistoryCapacityError{
			LimitBytes: limitBytes, RequiredBytes: releaseHistoryEnvelopeFramingBytes,
		}
	}
	encoded, err := marshalReleaseHistory(releases)
	if err != nil {
		return nil, err
	}
	if len(encoded) > limitBytes {
		return nil, &ReleaseHistoryCapacityError{
			LimitBytes: limitBytes, RequiredBytes: len(encoded),
		}
	}
	return encoded, nil
}

// The per-release sizes used by capacity planning sit between this exact v1
// envelope prefix/suffix; commas are accounted separately.
const releaseHistoryEnvelopeFramingBytes = len(`{"schema_version":1,"releases":[]}`)

// compactReleaseHistoryWithinLimit selects the largest deterministic suffix of
// non-authoritative history that fits beside the records required for recovery
// and cleanup. The index-latest row preserves max-version monotonicity and the
// most-recent active row is runtime rehydration authority. Expired disposable
// rows are removed before fresh ones; within either class the oldest version is
// removed first. A caller therefore loses the least-recent audit history while
// every safety-bearing record remains intact.
func compactReleaseHistoryWithinLimit(
	releases []Release,
	cutoff time.Time,
	limitBytes int,
) ([]Release, int, error) {
	if len(releases) == 0 {
		return nil, 0, errors.New("release history is empty")
	}
	if limitBytes <= 0 {
		return nil, 0, &ReleaseHistoryCapacityError{
			LimitBytes: limitBytes, RequiredBytes: releaseHistoryEnvelopeFramingBytes,
		}
	}

	entryBytes := make([]int, len(releases))
	encodedBytes := releaseHistoryEnvelopeFramingBytes + len(releases) - 1
	for index := range releases {
		encoded, err := json.Marshal(releases[index])
		if err != nil {
			return nil, 0, fmt.Errorf("marshal release %d for capacity planning: %w", releases[index].Version, err)
		}
		entryBytes[index] = len(encoded)
		encodedBytes += len(encoded)
	}
	if encodedBytes <= limitBytes {
		return releases, 0, nil
	}

	protected := make([]bool, len(releases))
	protected[len(releases)-1] = true
	activeIndex := latestActiveReleaseIndex(releases)
	if activeIndex >= 0 {
		protected[activeIndex] = true
	}
	removed := make([]bool, len(releases))
	removedCount := 0
	removeClass := func(expired bool) {
		for index := range releases {
			if encodedBytes <= limitBytes {
				return
			}
			if protected[index] || removed[index] {
				continue
			}
			isExpired := !cutoff.IsZero() && releases[index].CreatedAt.Before(cutoff)
			if isExpired != expired {
				continue
			}
			removed[index] = true
			removedCount++
			// A valid history always retains at least its index-latest row, so
			// removing any disposable entry also removes exactly one comma.
			encodedBytes -= entryBytes[index] + 1
		}
	}
	removeClass(true)
	removeClass(false)

	if encodedBytes > limitBytes {
		return nil, 0, &ReleaseHistoryCapacityError{
			LimitBytes: limitBytes, RequiredBytes: encodedBytes,
		}
	}

	compacted := make([]Release, 0, len(releases)-removedCount)
	for index := range releases {
		if !removed[index] {
			compacted = append(compacted, releases[index])
		}
	}
	return compacted, removedCount, nil
}

func releaseHistoryCapacityCutoff(maxAge time.Duration, now time.Time) time.Time {
	if maxAge <= 0 {
		return time.Time{}
	}
	return now.Add(-maxAge)
}

func compactAndEncodeReleaseHistory(
	releases []Release,
	cutoff time.Time,
	limitBytes int,
) ([]byte, error) {
	compacted, _, err := compactReleaseHistoryWithinLimit(releases, cutoff, limitBytes)
	if err != nil {
		return nil, err
	}
	if err := validateReleaseHistory(compacted); err != nil {
		return nil, fmt.Errorf("invalid compacted release history: %w", err)
	}
	encoded, err := encodeReleaseHistoryWithinLimit(compacted, limitBytes)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

func validReleaseStatus(status string) bool {
	switch status {
	case "deploying", "active", "failed", "superseded":
		return true
	default:
		return false
	}
}

func (s *ReleaseStore) requireCanonicalLeaseUUID(leaseUUID string) error {
	if s != nil && s.binding != nil && !backend.IsCanonicalLeaseUUID(leaseUUID) {
		return errors.New("identity-bound release journal requires a canonical lease UUID")
	}
	return nil
}

// append adds a raw release for package-local storage and schema tests. Runtime
// writers must cross a typed operation or maintenance settlement.
func (s *ReleaseStore) append(leaseUUID string, r Release) error {
	if !r.MaintenanceID.IsZero() {
		return ErrMaintenanceReleaseClaimRequired
	}
	return s.appendRelease(leaseUUID, r, false)
}

// appendActive atomically supersedes every prior active release and appends a
// new active release with the next version. Package-local tests use it to seed
// exact histories that are not the result of a runtime transition.
// An empty Status is accepted because the method itself supplies the state;
// any other explicit status must agree with that authority.
func (s *ReleaseStore) appendActive(leaseUUID string, r Release) error {
	if !r.MaintenanceID.IsZero() {
		return ErrMaintenanceReleaseClaimRequired
	}
	if r.Status != "" && r.Status != "active" {
		return errors.New("active release append requires active or empty status")
	}
	r.Status = "active"
	return s.appendRelease(leaseUUID, r, true)
}

// checkAppendActiveCapacity proves, without writing, that appendActive can
// retain every load-bearing record after deterministic capacity compaction.
// Production operation admission uses CheckOperationReleaseCapacity with a
// store-issued OperationReleaseCandidate instead.
func (s *ReleaseStore) checkAppendActiveCapacity(leaseUUID string, r Release) error {
	if !r.MaintenanceID.IsZero() {
		return ErrMaintenanceReleaseClaimRequired
	}
	if r.Status != "" && r.Status != "active" {
		return errors.New("active release capacity check requires active or empty status")
	}
	r.Status = "active"
	return s.checkAppendCapacityWithinLimit(
		leaseUUID,
		r,
		true,
		backend.MaxStoredReleaseHistoryBytes,
	)
}

func (s *ReleaseStore) appendRelease(leaseUUID string, r Release, supersedeActive bool) error {
	return s.appendWithinLimit(
		leaseUUID,
		r,
		supersedeActive,
		backend.MaxStoredReleaseHistoryBytes,
	)
}

func (s *ReleaseStore) checkAppendCapacityWithinLimit(
	leaseUUID string,
	r Release,
	supersedeActive bool,
	limitBytes int,
) error {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	if err := validateAppendRelease(r); err != nil {
		return err
	}
	r = cloneRelease(r)
	return s.view(func(tx *bolt.Tx) error {
		_, _, err := planAppendedReleaseHistory(
			tx.Bucket(releasesBucketName).Get([]byte(leaseUUID)),
			leaseUUID,
			r,
			supersedeActive,
			releaseHistoryCapacityCutoff(s.maxAge, time.Now()),
			limitBytes,
		)
		return err
	})
}

func (s *ReleaseStore) appendWithinLimit(
	leaseUUID string,
	r Release,
	supersedeActive bool,
	limitBytes int,
) error {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	if err := validateAppendRelease(r); err != nil {
		return err
	}
	r = cloneRelease(r)
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(releasesBucketName)
		releases, _, err := planAppendedReleaseHistory(
			b.Get([]byte(leaseUUID)),
			leaseUUID,
			r,
			supersedeActive,
			releaseHistoryCapacityCutoff(s.maxAge, time.Now()),
			limitBytes,
		)
		if err != nil {
			return err
		}
		encoded, err := encodeReleaseHistoryWithinLimit(releases, limitBytes)
		if err != nil {
			return fmt.Errorf("failed to marshal releases: %w", err)
		}
		return b.Put([]byte(leaseUUID), encoded)
	})
}

func validateAppendRelease(r Release) error {
	if !r.MaintenanceID.IsZero() {
		return errors.New("maintenance releases require AppendMaintenance")
	}
	if err := validateStoredRelease(r); err != nil {
		return fmt.Errorf("invalid release: %w", err)
	}
	if len(r.Items) > 0 && len(r.ResourceProfiles) == 0 {
		return errors.New("invalid release: desired items require exact resource profiles")
	}
	return nil
}

func planAppendedReleaseHistory(
	data []byte,
	leaseUUID string,
	r Release,
	supersedeActive bool,
	cutoff time.Time,
	limitBytes int,
) ([]Release, int, error) {
	return planAppendedReleaseHistoryWithPolicy(
		data, leaseUUID, r, supersedeActive, cutoff, limitBytes, false,
	)
}

func planMaintenanceAppendedReleaseHistory(
	data []byte,
	leaseUUID string,
	r Release,
	supersedeActive bool,
	cutoff time.Time,
	limitBytes int,
) ([]Release, int, error) {
	return planAppendedReleaseHistoryWithPolicy(
		data, leaseUUID, r, supersedeActive, cutoff, limitBytes, true,
	)
}

func planAppendedReleaseHistoryWithPolicy(
	data []byte,
	leaseUUID string,
	r Release,
	supersedeActive bool,
	cutoff time.Time,
	limitBytes int,
	allowMaintenanceHistory bool,
) ([]Release, int, error) {
	var releases []Release
	if data != nil {
		var err error
		releases, err = decodeReleaseHistory(data)
		if err != nil {
			return nil, 0, fmt.Errorf("corrupted release data for %s: %w", leaseUUID, err)
		}
		if err := validateReleaseHistory(releases); err != nil {
			return nil, 0, fmt.Errorf("invalid release data for %s: %w", leaseUUID, err)
		}
		if !allowMaintenanceHistory {
			// Appending a new generation may legitimately supersede an older
			// terminal maintenance generation (for example, reprovision after a
			// later runtime failure). Only an unresolved deploying target remains
			// exclusively owned by MaintenanceReleaseClaim. Other raw mutators
			// below retain the stricter all-history fence because they can alter or
			// erase a maintenance row without authoring a new generation.
			if err := rejectUnresolvedMaintenanceHistoryMutation(releases); err != nil {
				return nil, 0, err
			}
		}
	}
	if supersedeActive {
		for index := range releases {
			if releases[index].Status == "active" {
				releases[index].Status = "superseded"
			}
		}
	}

	r.Version = maxVersion(releases) + 1
	releases = append(releases, r)
	if err := validateReleaseHistory(releases); err != nil {
		return nil, 0, fmt.Errorf("invalid appended release history: %w", err)
	}
	compacted, removed, err := compactReleaseHistoryWithinLimit(releases, cutoff, limitBytes)
	if err != nil {
		return nil, 0, fmt.Errorf("compact appended release history: %w", err)
	}
	if err := validateReleaseHistory(compacted); err != nil {
		return nil, 0, fmt.Errorf("invalid compacted release history: %w", err)
	}
	if !supersedeActive && r.Status == "deploying" {
		if err := checkDeployingReleaseTerminalCapacity(compacted, cutoff, limitBytes); err != nil {
			return nil, 0, err
		}
	}
	return compacted, removed, nil
}

// checkDeployingReleaseTerminalCapacity closes the Append-before-Compose
// capacity window used by Restart and Update. Maintenance activation slightly expands
// the two status strings when the previous active row is also protected
// migration authority; proving that terminal shape now prevents an otherwise
// deterministic post-Compose write failure. The failure terminal is smaller
// once optional reason/message metadata is omitted, and
// updateLatestStatusWithinLimit has that exact fallback.
func checkDeployingReleaseTerminalCapacity(
	releases []Release,
	cutoff time.Time,
	limitBytes int,
) error {
	activated := slices.Clone(releases)
	for index := range len(activated) - 1 {
		if activated[index].Status == "active" {
			activated[index].Status = "superseded"
		}
	}
	latest := &activated[len(activated)-1]
	latest.Status = "active"
	latest.Error = ""
	latest.Reason = ""
	latest.Message = ""
	if _, _, err := compactReleaseHistoryWithinLimit(activated, cutoff, limitBytes); err != nil {
		return fmt.Errorf("reserve deploying release terminal activation: %w", err)
	}
	return nil
}

// List returns all releases for a lease. Returns nil, nil when not found.
func (s *ReleaseStore) List(leaseUUID string) ([]Release, error) {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return nil, err
	}
	var releases []Release

	err := s.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(releasesBucketName)
		data := b.Get([]byte(leaseUUID))
		if data == nil {
			return nil
		}
		var err error
		releases, err = decodeReleaseHistory(data)
		if err != nil {
			return err
		}
		if err := validateReleaseHistory(releases); err != nil {
			return fmt.Errorf("invalid release data for %s: %w", leaseUUID, err)
		}
		return nil
	})

	return releases, err
}

// Latest returns the most recent release for a lease. Returns nil, nil when not found.
func (s *ReleaseStore) Latest(leaseUUID string) (*Release, error) {
	releases, err := s.List(leaseUUID)
	if err != nil || len(releases) == 0 {
		return nil, err
	}
	return &releases[len(releases)-1], nil
}

// LatestActive returns the most recent release with "active" status for a lease.
// Returns nil, nil when no active release is found.
func (s *ReleaseStore) LatestActive(leaseUUID string) (*Release, error) {
	releases, err := s.List(leaseUUID)
	if err != nil || len(releases) == 0 {
		return nil, err
	}
	for i := len(releases) - 1; i >= 0; i-- {
		if releases[i].Status == "active" {
			return &releases[i], nil
		}
	}
	return nil, nil
}

// LeaseUUIDs returns every release-history key in deterministic byte order.
// Recovery uses this index to detect an exact active release even when Docker
// has no surviving container from which to rediscover the lease UUID.
func (s *ReleaseStore) LeaseUUIDs() ([]string, error) {
	var leaseUUIDs []string
	err := s.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(releasesBucketName)
		if bucket == nil {
			return fmt.Errorf("releases bucket missing")
		}
		return bucket.ForEach(func(key, value []byte) error {
			if value == nil {
				return fmt.Errorf("release history %q is a nested bucket", key)
			}
			if s.binding != nil && !backend.IsCanonicalLeaseUUID(string(key)) {
				return fmt.Errorf("release history key with length %d is not canonical", len(key))
			}
			leaseUUIDs = append(leaseUUIDs, string(key))
			return nil
		})
	})
	return leaseUUIDs, err
}

// updateLatestStatus updates the status and curated (reason, message) of the
// most recent release for package-local transition tests. Runtime transitions
// use proof-specific settlement APIs.
func (s *ReleaseStore) updateLatestStatus(leaseUUID, status string, reason backend.Reason, message string) error {
	return s.updateLatestStatusWithinLimit(
		leaseUUID,
		status,
		reason,
		message,
		backend.MaxStoredReleaseHistoryBytes,
	)
}

func (s *ReleaseStore) updateLatestStatusWithinLimit(
	leaseUUID, status string,
	reason backend.Reason,
	message string,
	limitBytes int,
) error {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	if !validReleaseStatus(status) {
		return fmt.Errorf("unsupported release status")
	}
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(releasesBucketName)
		data := b.Get([]byte(leaseUUID))
		if data == nil {
			return nil
		}

		var releases []Release
		var err error
		releases, err = decodeReleaseHistory(data)
		if err != nil {
			return fmt.Errorf("failed to unmarshal releases: %w", err)
		}
		if err := validateReleaseHistory(releases); err != nil {
			return fmt.Errorf("invalid release data for %s: %w", leaseUUID, err)
		}
		if len(releases) == 0 {
			return nil
		}
		if err := rejectRawMaintenanceHistoryMutation(releases); err != nil {
			return err
		}

		releases[len(releases)-1].Status = status
		releases[len(releases)-1].Reason = reason
		releases[len(releases)-1].Message = message
		// Clear any legacy verbose Error so a status update never leaves stale
		// verbose text in the store (ENG-508); the curated pair supersedes it.
		releases[len(releases)-1].Error = ""
		if err := validateReleaseHistory(releases); err != nil {
			return fmt.Errorf("invalid updated release history: %w", err)
		}

		cutoff := releaseHistoryCapacityCutoff(s.maxAge, time.Now())
		encoded, err := compactAndEncodeReleaseHistory(
			releases,
			cutoff,
			limitBytes,
		)
		if err != nil && status == "failed" && errors.Is(err, ErrReleaseHistoryCapacity) &&
			(releases[len(releases)-1].Reason != "" || releases[len(releases)-1].Message != "") {
			// Reason and Message are observational metadata. Failed without an
			// explicit reason projects as ReasonUnknown, so dropping them under
			// hard capacity pressure is safer than losing the terminal state
			// transition after the failed substrate operation has completed.
			releases[len(releases)-1].Reason = ""
			releases[len(releases)-1].Message = ""
			encoded, err = compactAndEncodeReleaseHistory(releases, cutoff, limitBytes)
		}
		if err != nil {
			return fmt.Errorf("failed to marshal releases: %w", err)
		}
		return b.Put([]byte(leaseUUID), encoded)
	})
}

// activateLatest marks the most recent release as "active" and all previous
// "active" releases as "superseded" for package-local history tests.
func (s *ReleaseStore) activateLatest(leaseUUID string) error {
	return s.activateLatestWithinLimit(leaseUUID, backend.MaxStoredReleaseHistoryBytes)
}

func (s *ReleaseStore) activateLatestWithinLimit(leaseUUID string, limitBytes int) error {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(releasesBucketName)
		data := b.Get([]byte(leaseUUID))
		if data == nil {
			return nil
		}

		var releases []Release
		var err error
		releases, err = decodeReleaseHistory(data)
		if err != nil {
			return fmt.Errorf("failed to unmarshal releases: %w", err)
		}
		if err := validateReleaseHistory(releases); err != nil {
			return fmt.Errorf("invalid release data for %s: %w", leaseUUID, err)
		}
		if len(releases) == 0 {
			return nil
		}
		if err := rejectRawMaintenanceHistoryMutation(releases); err != nil {
			return err
		}
		latest := &releases[len(releases)-1]
		if len(latest.Items) > 0 {
			if err := ValidateSKUResourceSnapshot(latest.Items, latest.ResourceProfiles); err != nil {
				return fmt.Errorf("cannot activate latest release without exact resource profiles: %w", err)
			}
		}

		for i := range len(releases) - 1 {
			if releases[i].Status == "active" {
				releases[i].Status = "superseded"
			}
		}
		releases[len(releases)-1].Status = "active"
		releases[len(releases)-1].Error = ""
		releases[len(releases)-1].Reason = ""
		releases[len(releases)-1].Message = ""

		encoded, err := compactAndEncodeReleaseHistory(
			releases,
			releaseHistoryCapacityCutoff(s.maxAge, time.Now()),
			limitBytes,
		)
		if err != nil {
			return fmt.Errorf("failed to marshal releases: %w", err)
		}
		return b.Put([]byte(leaseUUID), encoded)
	})
}

// delete is retained only for package-local schema and corruption tests. A
// production close retires release history through CloseSettlement's exact
// fenced transition; sibling packages cannot delete authority directly.
func (s *ReleaseStore) delete(leaseUUID string) error {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	return s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(releasesBucketName)
		data := b.Get([]byte(leaseUUID))
		if data == nil {
			return nil
		}
		var releases []Release
		var err error
		releases, err = decodeReleaseHistory(data)
		if err != nil {
			return fmt.Errorf("failed to unmarshal releases: %w", err)
		}
		if err := validateReleaseHistory(releases); err != nil {
			return fmt.Errorf("invalid release data for %s: %w", leaseUUID, err)
		}
		if err := rejectRawMaintenanceHistoryMutation(releases); err != nil {
			return err
		}
		return b.Delete([]byte(leaseUUID))
	})
}

func rejectRawMaintenanceHistoryMutation(releases []Release) error {
	if slices.ContainsFunc(releases, func(release Release) bool {
		return !release.MaintenanceID.IsZero()
	}) {
		return ErrMaintenanceReleaseClaimRequired
	}
	return nil
}

func rejectUnresolvedMaintenanceHistoryMutation(releases []Release) error {
	if slices.ContainsFunc(releases, func(release Release) bool {
		return !release.MaintenanceID.IsZero() && release.Status == "deploying"
	}) {
		return ErrMaintenanceReleaseClaimRequired
	}
	return nil
}

func releaseForCloseFence(releases []Release) *Release {
	for index := len(releases) - 1; index >= 0; index-- {
		if releases[index].Status == "active" {
			return &releases[index]
		}
	}
	if len(releases) == 0 {
		return nil
	}
	return &releases[len(releases)-1]
}

// removeOlderThan prunes release history older than maxAge while preserving each
// lease's load-bearing records. For every lease it ALWAYS retains its index-latest
// entry — the entry the runtime mutators append to / activate, and (because Append
// assigns the next version to the tail) the holder of the lease's maximum version,
// which keeps Append's max+1 derivation collision-free — AND, when a distinct one
// exists, the most-recent "active" release that recoverState rehydrates the
// StackManifest from (see LatestActive). So it protects one entry when the newest
// entry is itself the active release or no active release exists, two otherwise. Only
// entries that are BOTH older than the cutoff AND not protected are pruned, so a
// lease's record is never emptied and its live manifest is never removed (ENG-440).
// Corrupt or empty values are preserved and fail the sweep closed: exact release
// topology is recovery authority, so ambiguity must remain visible for repair.
// Returns the number of valid release entries removed.
func (s *ReleaseStore) removeOlderThan(maxAge time.Duration) (int, error) {
	cutoff := time.Now().Add(-maxAge)
	removed := 0

	err := s.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(releasesBucketName)

		// Decide all changes during a read-only cursor pass, then apply them after the
		// loop. Mutating the bucket (Put/Delete) while the cursor is live can split/
		// rebalance pages and invalidate the cursor. Cursor keys alias tx-lifetime
		// pages reused across iterations, so any retained key must be copied.
		type change struct {
			key     []byte
			value   []byte // nil => delete the whole key
			removed int
		}
		var changes []change

		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if s.binding != nil && !backend.IsCanonicalLeaseUUID(string(k)) {
				return fmt.Errorf("release history key with length %d is not canonical", len(k))
			}
			var releases []Release
			var err error
			releases, err = decodeReleaseHistory(v)
			if err != nil {
				// Exact release topology is now recovery authority. Deleting a
				// corrupt row would turn "unknown desired cohort" into apparent
				// permission to infer topology from whatever containers survived,
				// and the orphan reaper could then destroy the missing sibling's
				// volume. Preserve the bytes and fail closed for operator repair.
				return fmt.Errorf("corrupted release history for %s: %w", string(k), err)
			}
			if err := validateReleaseHistory(releases); err != nil {
				return fmt.Errorf("invalid release history for %s: %w", string(k), err)
			}
			if len(releases) == 0 {
				return fmt.Errorf("empty release history for %s", string(k))
			}

			// Protected indices (never pruned): the index-latest entry (holds the max
			// version) and the most-recent "active" entry (the manifest-rehydration
			// source).
			keepLatest := len(releases) - 1
			keepActive := -1
			for i := len(releases) - 1; i >= 0; i-- {
				if releases[i].Status == "active" {
					keepActive = i
					break
				}
			}

			kept := make([]Release, 0, len(releases))
			prunedHere := 0
			for i, r := range releases {
				if i != keepLatest && i != keepActive && r.CreatedAt.Before(cutoff) {
					prunedHere++
					continue
				}
				kept = append(kept, r)
			}
			if prunedHere == 0 {
				continue // unchanged: don't rewrite an untouched key
			}

			encoded, err := encodeReleaseHistory(kept)
			if err != nil {
				return fmt.Errorf("failed to marshal pruned releases for %s: %w", string(k), err)
			}
			changes = append(changes, change{key: append([]byte(nil), k...), value: encoded, removed: prunedHere})
		}

		for _, ch := range changes {
			if ch.value == nil {
				if err := b.Delete(ch.key); err != nil {
					return err
				}
			} else {
				if err := b.Put(ch.key, ch.value); err != nil {
					return err
				}
			}
			removed += ch.removed
		}
		return nil
	})

	return removed, err
}
