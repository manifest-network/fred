package shared

import (
	"bytes"
	"crypto/sha256"
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
	MaintenanceID MaintenanceID `json:"maintenance_id,omitempty"`
	// OperationID is the exact causal token of the provision/restore generation
	// that first committed this release lineage. Maintenance releases preserve it.
	// Empty is accepted only for v0.13/legacy rows.
	OperationID OperationID `json:"operation_id,omitempty"`
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
	// LegacyMigration identifies a release produced by Docker's one-time
	// legacy-to-Compose migration. It remains durable cleanup authority even when
	// a later update supersedes this release: Deprovision consumes the original
	// Items before deleting release history so crash-window `-prev` rollback
	// containers cannot outlive the evidence that names their exact cohort.
	LegacyMigration bool           `json:"legacy_migration,omitempty"`
	Status          string         `json:"status"`
	CreatedAt       time.Time      `json:"created_at"`
	Error           string         `json:"error,omitempty"`
	Reason          backend.Reason `json:"reason,omitempty"`
	Message         string         `json:"message,omitempty"`
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
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
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
		return ""
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
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
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

// NewReleaseStore opens or creates a bbolt database for release persistence.
//
// Deprecated: this compatibility-only constructor creates an unbound journal.
// Application composition roots are repository-guarded to use
// OpenIdentityBoundReleaseStore and cannot obtain authority from this value.
func NewReleaseStore(cfg ReleaseStoreConfig) (*ReleaseStore, error) {
	s, err := newReleaseStore(cfg, backendidentity.VerifiedStorage{}, nil)
	if err != nil {
		return nil, err
	}
	s.StartMaintenance()
	return s, nil
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
	storeCfg := boltStoreConfig{
		DBPath:     cfg.DBPath,
		BucketName: releasesBucketName,
		MaxAge:     cfg.MaxAge,
		Label:      "releases",
	}
	var base *boltStore
	var err error
	if storage.Valid() {
		base, err = openIdentityBoundBoltStore(storeCfg, authoritativeStoreReleases, storage, gate)
	} else {
		base, err = openBoltStore(storeCfg)
	}
	if err != nil {
		return nil, err
	}
	store := &ReleaseStore{
		boltStore:       base,
		cleanupInterval: cfg.CleanupInterval,
		onCleanupPanic:  cfg.OnCleanupPanic,
	}
	if storage.Valid() {
		if err := store.view(inspectReleaseBucket); err != nil {
			_ = base.Close()
			return nil, fmt.Errorf("validate bound release journal: %w", err)
		}
	}
	return store, nil
}

// StartMaintenance performs the initial pruning pass and starts the periodic
// loop exactly once.
func (s *ReleaseStore) StartMaintenance() {
	if s == nil || s.maxAge <= 0 {
		return
	}
	s.cleanupOnce.Do(func() {
		s.startCleanup("releases", s.cleanupInterval, s.RemoveOlderThan, s.onCleanupPanic)
	})
}

// Healthy validates the complete recovery journal, not only bbolt reachability.
// A release manifest is cleanup and restart authority; leaving a semantically
// invalid row health-green would defer the failure until the next recovery or
// close, after the original operation may already have committed externally.
func (s *ReleaseStore) Healthy() error {
	return s.view(inspectReleaseBucket)
}

// ReleaseStoreInspection is read-only v0.13 adoption evidence.
type ReleaseStoreInspection struct {
	Exists           bool
	IdentityBound    bool
	ActiveLeaseUUIDs map[string]struct{}
	ActiveReleases   map[string]Release
	// capacity retains only per-entry encoded sizes and protection metadata,
	// not manifests or complete histories. The stopped Docker adoption proof
	// uses it to simulate startup authority backfills without reopening or
	// mutating the journal and without doubling the bounded inspection payload.
	capacity map[string]releaseHistoryCapacitySnapshot
	// RequiresLegacyNormalization reports a wire-recognizable v0.13 history
	// whose redundant authority rows must be normalized atomically with identity
	// binding (duplicate RecordMigration active or semantically identical
	// post-active deploying retries).
	RequiresLegacyNormalization bool
}

type releaseHistoryCapacitySnapshot struct {
	entries     []releaseHistoryCapacityEntry
	activeIndex int
}

type releaseHistoryCapacityEntry struct {
	encodedBytes    int
	status          string
	legacyMigration bool
}

// LegacyActiveAuthorityClass makes the one v0.13 migration-marker backfill an
// explicit authority choice rather than a boolean flag. The zero value is
// invalid: callers must select ordinary observed workload authority or the
// stronger, whole-cohort-proven RecordMigration class.
type LegacyActiveAuthorityClass uint8

const (
	LegacyActiveAuthorityWorkload LegacyActiveAuthorityClass = iota + 1
	LegacyActiveAuthorityMigration
)

func (class LegacyActiveAuthorityClass) valid() bool {
	return class == LegacyActiveAuthorityWorkload ||
		class == LegacyActiveAuthorityMigration
}

// CheckLegacyActiveAuthorityCapacity simulates the exact startup backfill that
// converts one stopped v0.13 active release into current recovery authority.
// The inspection snapshot already reflects any deterministic v0.13
// normalization that the identity-binding transaction will perform. No file is
// reopened or changed.
func (inspection ReleaseStoreInspection) CheckLegacyActiveAuthorityCapacity(
	leaseUUID string,
	expected Release,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
	class LegacyActiveAuthorityClass,
) error {
	return inspection.checkLegacyActiveAuthorityAndRuntimeCapacityWithinLimit(
		leaseUUID,
		expected,
		items,
		resourceProfiles,
		class,
		nil,
		backend.MaxStoredReleaseHistoryBytes,
	)
}

// CheckLegacyActiveAuthorityAndRuntimeCapacity simulates the complete startup
// upgrade of a stopped v0.13 Release, including the separately typed tokenless
// runtime identity. This ensures the offline sealing proof cannot pass only for
// startup to fail when it persists the additional zero-survivor authority.
func (inspection ReleaseStoreInspection) CheckLegacyActiveAuthorityAndRuntimeCapacity(
	leaseUUID string,
	expected Release,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
	class LegacyActiveAuthorityClass,
	authority LegacyRuntimeAuthority,
) error {
	return inspection.checkLegacyActiveAuthorityAndRuntimeCapacityWithinLimit(
		leaseUUID,
		expected,
		items,
		resourceProfiles,
		class,
		&authority,
		backend.MaxStoredReleaseHistoryBytes,
	)
}

func (inspection ReleaseStoreInspection) checkLegacyActiveAuthorityCapacityWithinLimit(
	leaseUUID string,
	expected Release,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
	class LegacyActiveAuthorityClass,
	limitBytes int,
) error {
	return inspection.checkLegacyActiveAuthorityAndRuntimeCapacityWithinLimit(
		leaseUUID,
		expected,
		items,
		resourceProfiles,
		class,
		nil,
		limitBytes,
	)
}

func (inspection ReleaseStoreInspection) checkLegacyActiveAuthorityAndRuntimeCapacityWithinLimit(
	leaseUUID string,
	expected Release,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
	class LegacyActiveAuthorityClass,
	authority *LegacyRuntimeAuthority,
	limitBytes int,
) error {
	if expected.Version <= 0 || expected.Status != "active" {
		return errors.New("legacy active release fence must name a positive active version")
	}
	if expected.OperationID != "" || len(expected.Items) != 0 ||
		len(expected.ResourceProfiles) != 0 || expected.LegacyMigration {
		return errors.New("legacy active release fence contains current authority fields")
	}
	if !class.valid() {
		return errors.New("legacy active release authority class is invalid")
	}
	if err := ValidateSKUResourceSnapshot(items, resourceProfiles); err != nil {
		return fmt.Errorf("backfill legacy active release authority: %w", err)
	}
	stack, err := manifest.ParsePayload(expected.Manifest)
	if err != nil {
		return fmt.Errorf("backfill legacy active release manifest: %w", err)
	}
	if err := manifest.ValidateStackAgainstItems(stack, items); err != nil {
		return fmt.Errorf("backfill legacy active release topology: %w", err)
	}
	if err := inspection.requireExpectedActiveRelease(leaseUUID, expected); err != nil {
		return err
	}
	candidate := cloneRelease(expected)
	candidate.Items = slices.Clone(items)
	candidate.ResourceProfiles = CloneSKUResourceSnapshot(resourceProfiles)
	candidate.LegacyMigration = class == LegacyActiveAuthorityMigration
	if authority != nil {
		if !authority.valid {
			return errors.New("legacy runtime authority is invalid")
		}
		candidate.LegacyRuntimeAuthority = cloneLegacyRuntimeAuthority(authority)
	}
	if err := validateStoredRelease(candidate); err != nil {
		return fmt.Errorf("project legacy active release authority: %w", err)
	}
	return inspection.checkActiveReplacementCapacity(leaseUUID, candidate, limitBytes)
}

// CheckActiveResourceProfilesCapacity simulates the narrower startup backfill
// for a release that already has exact Items but predates immutable profile
// snapshots.
func (inspection ReleaseStoreInspection) CheckActiveResourceProfilesCapacity(
	leaseUUID string,
	expected Release,
	resourceProfiles []SKUResourceSnapshot,
) error {
	return inspection.checkActiveResourceProfilesCapacityWithinLimit(
		leaseUUID,
		expected,
		resourceProfiles,
		backend.MaxStoredReleaseHistoryBytes,
	)
}

func (inspection ReleaseStoreInspection) checkActiveResourceProfilesCapacityWithinLimit(
	leaseUUID string,
	expected Release,
	resourceProfiles []SKUResourceSnapshot,
	limitBytes int,
) error {
	if expected.Version <= 0 || expected.Status != "active" {
		return errors.New("active release fence must name a positive active version")
	}
	if len(expected.Items) == 0 || len(expected.ResourceProfiles) != 0 {
		return errors.New("active resource-profile backfill requires items without existing profiles")
	}
	if err := ValidateSKUResourceSnapshot(expected.Items, resourceProfiles); err != nil {
		return fmt.Errorf("backfill active release resource profiles: %w", err)
	}
	if err := inspection.requireExpectedActiveRelease(leaseUUID, expected); err != nil {
		return err
	}
	candidate := cloneRelease(expected)
	candidate.ResourceProfiles = CloneSKUResourceSnapshot(resourceProfiles)
	if err := validateStoredRelease(candidate); err != nil {
		return fmt.Errorf("project active release resource profiles: %w", err)
	}
	return inspection.checkActiveReplacementCapacity(leaseUUID, candidate, limitBytes)
}

func (inspection ReleaseStoreInspection) requireExpectedActiveRelease(
	leaseUUID string,
	expected Release,
) error {
	active, ok := inspection.ActiveReleases[leaseUUID]
	if !ok {
		return fmt.Errorf("release inspection has no active release for %s", leaseUUID)
	}
	want, err := json.Marshal(expected)
	if err != nil {
		return fmt.Errorf("marshal expected active release: %w", err)
	}
	got, err := json.Marshal(active)
	if err != nil {
		return fmt.Errorf("marshal inspected active release: %w", err)
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("active release for %s changed within stopped inspection", leaseUUID)
	}
	return nil
}

func (inspection ReleaseStoreInspection) checkActiveReplacementCapacity(
	leaseUUID string,
	candidate Release,
	limitBytes int,
) error {
	snapshot, ok := inspection.capacity[leaseUUID]
	if !ok || snapshot.activeIndex < 0 || snapshot.activeIndex >= len(snapshot.entries) {
		return fmt.Errorf("release inspection has no capacity snapshot for %s", leaseUUID)
	}
	encoded, err := json.Marshal(candidate)
	if err != nil {
		return fmt.Errorf("marshal projected active release: %w", err)
	}
	snapshot.entries = slices.Clone(snapshot.entries)
	snapshot.entries[snapshot.activeIndex] = releaseHistoryCapacityEntry{
		encodedBytes:    len(encoded),
		status:          candidate.Status,
		legacyMigration: candidate.LegacyMigration,
	}
	return checkReleaseHistoryCapacitySnapshot(
		snapshot,
		limitBytes,
	)
}

func checkReleaseHistoryCapacitySnapshot(
	snapshot releaseHistoryCapacitySnapshot,
	limitBytes int,
) error {
	if len(snapshot.entries) == 0 {
		return errors.New("release capacity snapshot is empty")
	}
	encodedBytes := releaseHistoryArrayFramingBytes + len(snapshot.entries) - 1
	for _, entry := range snapshot.entries {
		encodedBytes += entry.encodedBytes
	}
	if encodedBytes <= limitBytes {
		return nil
	}
	protected := make([]bool, len(snapshot.entries))
	protected[len(snapshot.entries)-1] = true
	for index := len(snapshot.entries) - 1; index >= 0; index-- {
		if snapshot.entries[index].status == "active" {
			protected[index] = true
			break
		}
	}
	for index := len(snapshot.entries) - 1; index >= 0; index-- {
		if snapshot.entries[index].legacyMigration {
			protected[index] = true
			break
		}
	}
	for index, entry := range snapshot.entries {
		if protected[index] {
			continue
		}
		encodedBytes -= entry.encodedBytes + 1
	}
	if encodedBytes > limitBytes {
		return &ReleaseHistoryCapacityError{
			LimitBytes: limitBytes, RequiredBytes: encodedBytes,
		}
	}
	return nil
}

// InspectReleaseStoreReadOnly validates every durable history without
// creating a database, bucket, binding, or cleanup goroutine.
func InspectReleaseStoreReadOnly(dbPath string) (ReleaseStoreInspection, error) {
	return inspectReleaseStoreReadOnlyFile(pathnameAuthoritativeStoreFile(dbPath), false)
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
	return inspectReleaseStoreReadOnlyFile(file, false)
}

// InspectBoundLegacyReleaseStoreReadOnly is the stopped-v0.13 adoption
// inspector. In addition to current histories, it accepts the one
// wire-recognizable multi-active history produced by v0.13 RecordMigration:
// an older flat-manifest active row followed by its semantically identical
// stack-wrapped active row. The later row was already v0.13 LatestActive
// authority. It also recognizes post-active v0.13 Update/Restart deploying
// rows only when every complete parsed manifest is semantically identical to
// LatestActive. The identity-binding transaction normalizes redundant older
// actives and equivalent deploying retries to superseded before a current store
// can open the journal; a distinct deploying manifest remains ambiguous and is
// rejected without mutation.
func InspectBoundLegacyReleaseStoreReadOnly(
	path *BoundAuthoritativeStorePath,
) (ReleaseStoreInspection, error) {
	file, err := boundAuthoritativeStoreFile(path)
	if err != nil {
		return ReleaseStoreInspection{}, err
	}
	return inspectReleaseStoreReadOnlyFile(file, true)
}

func inspectReleaseStoreReadOnlyFile(
	file authoritativeStoreFile,
	allowLegacyMultipleActive bool,
) (ReleaseStoreInspection, error) {
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
		ActiveReleases:   make(map[string]Release),
		capacity:         make(map[string]releaseHistoryCapacitySnapshot),
	}
	err = db.View(func(tx *bolt.Tx) error {
		inspection.IdentityBound = tx.Bucket(storeIdentityBucketName) != nil
		requiresNormalization, inspectErr := inspectReleaseBucketForAdoption(
			tx,
			inspection.ActiveLeaseUUIDs,
			inspection.ActiveReleases,
			inspection.capacity,
			allowLegacyMultipleActive,
		)
		inspection.RequiresLegacyNormalization = requiresNormalization
		return inspectErr
	})
	if err != nil {
		return ReleaseStoreInspection{}, fmt.Errorf("inspect release database: %w", err)
	}
	return inspection, nil
}

func inspectReleaseBucket(tx *bolt.Tx) error {
	_, err := inspectReleaseBucketWithObserver(
		tx,
		nil,
		nil,
		nil,
		false,
		func(key, value []byte) error {
			return validateAuthoritativeRecord(key, value, maxAuthoritativeRecordBytes)
		},
	)
	return err
}

func inspectReleaseBucketForAdoption(
	tx *bolt.Tx,
	active map[string]struct{},
	activeReleases map[string]Release,
	capacity map[string]releaseHistoryCapacitySnapshot,
	allowLegacyMultipleActive bool,
) (bool, error) {
	budget := newStoppedAuthoritativeInspectionBudget()
	return inspectReleaseBucketWithObserver(
		tx,
		active,
		activeReleases,
		capacity,
		allowLegacyMultipleActive,
		budget.observe,
	)
}

func inspectReleaseBucketWithObserver(
	tx *bolt.Tx,
	active map[string]struct{},
	activeReleases map[string]Release,
	capacity map[string]releaseHistoryCapacitySnapshot,
	allowLegacyMultipleActive bool,
	observe func(key, value []byte) error,
) (bool, error) {
	bucket := tx.Bucket(releasesBucketName)
	if bucket == nil {
		return false, errors.New("releases bucket is missing")
	}
	if observe == nil {
		return false, errors.New("release record observer is required")
	}
	requiresNormalization := false
	err := bucket.ForEach(func(key, value []byte) error {
		if err := observe(key, value); err != nil {
			return err
		}
		if value == nil {
			return fmt.Errorf("release history with key length %d is a nested bucket", len(key))
		}
		if !backend.IsCanonicalLeaseUUID(string(key)) {
			return fmt.Errorf("release history key with length %d is not a canonical lease UUID", len(key))
		}
		var releases []Release
		if err := json.Unmarshal(value, &releases); err != nil {
			return fmt.Errorf("decode release history with key length %d: %w", len(key), err)
		}
		legacyMultipleActive, err := validateReleaseHistoryForAdoption(
			releases,
			allowLegacyMultipleActive,
		)
		if err != nil {
			return fmt.Errorf("validate release history with key length %d: %w", len(key), err)
		}
		requiresNormalization = requiresNormalization || legacyMultipleActive
		capacityHistory := releases
		if legacyMultipleActive {
			capacityHistory, err = normalizeLegacyReleaseHistoryForAdoption(releases)
			if err != nil {
				return fmt.Errorf("normalize release history with key length %d for capacity proof: %w", len(key), err)
			}
		}
		activeIndex := latestActiveReleaseIndex(releases)
		if activeIndex >= 0 {
			if active != nil {
				active[string(key)] = struct{}{}
			}
			if activeReleases != nil {
				activeReleases[string(key)] = cloneRelease(releases[activeIndex])
			}
		}
		if capacity != nil {
			snapshot, snapshotErr := newReleaseHistoryCapacitySnapshot(capacityHistory)
			if snapshotErr != nil {
				return fmt.Errorf("snapshot release history with key length %d: %w", len(key), snapshotErr)
			}
			capacity[string(key)] = snapshot
		}
		return nil
	})
	return requiresNormalization, err
}

func PrepareBoundReleaseStoreStorage(
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
	validate := inspectReleaseBucket
	if profile == backendidentity.InitializationProfileExisting {
		validate = func(tx *bolt.Tx) error {
			if tx.Bucket(storeIdentityBucketName) != nil {
				// A pending-initialization replay can encounter the transaction
				// that already normalized and bound this exact store. Verify the
				// binding first and require current strict history; never use
				// adoption compatibility to repair an independently bound store.
				if err := verifyStoreIdentityBinding(
					tx,
					authoritativeStoreReleases,
					storage.ID(),
				); err != nil {
					return err
				}
				return inspectReleaseBucket(tx)
			}
			return normalizeLegacyReleaseBucketForAdoption(tx)
		}
	}
	return initializeIdentityBoundBoltStoreBound(
		path, releasesBucketName, "releases", authoritativeStoreReleases, storage.ID(), allowCreate,
		validate,
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
		path, releasesBucketName, "releases", authoritativeStoreReleases, storage.ID(), nil,
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
		path, releasesBucketName, "releases", authoritativeStoreReleases, storage.ID(), nil,
	)
}

func VerifyReleaseStoreStorage(dbPath string, storage backendidentity.VerifiedStorage) error {
	return verifyIdentityBoundBoltStore(
		dbPath, releasesBucketName, "releases", authoritativeStoreReleases, storage, nil,
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
	if release.MaintenanceID != "" && !release.MaintenanceID.Valid() {
		return errors.New("maintenance ID is not a canonical UUIDv4")
	}
	if release.MaintenanceID != "" && release.OperationID == "" &&
		release.LegacyRuntimeAuthority == nil {
		return errors.New("maintenance ID requires durable runtime authority")
	}
	typed := release.OperationID != ""
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
	if release.OperationID == "" {
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
		return nil // v0.13 compatibility; backfill freezes this before use.
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
	stack, err := manifest.ParsePayload(release.Manifest)
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
	_, err := validateReleaseHistoryForAdoption(releases, false)
	return err
}

func validateReleaseHistoryForAdoption(
	releases []Release,
	allowLegacyMultipleActive bool,
) (bool, error) {
	if len(releases) == 0 {
		return false, errors.New("release history is empty")
	}
	previousVersion := 0
	activeIndexes := make([]int, 0, 2)
	maintenanceIDs := make(map[MaintenanceID]int)
	for index := range releases {
		if err := validateStoredRelease(releases[index]); err != nil {
			return false, fmt.Errorf("release %d: %w", releases[index].Version, err)
		}
		if releases[index].Version <= previousVersion {
			return false, fmt.Errorf("release history has non-increasing version %d", releases[index].Version)
		}
		previousVersion = releases[index].Version
		if maintenanceID := releases[index].MaintenanceID; maintenanceID != "" {
			if previous, exists := maintenanceIDs[maintenanceID]; exists {
				return false, fmt.Errorf(
					"release %d duplicates maintenance ID from release %d",
					releases[index].Version, previous,
				)
			}
			maintenanceIDs[maintenanceID] = releases[index].Version
		}
		switch releases[index].Status {
		case "deploying", "active", "failed", "superseded":
		default:
			return false, fmt.Errorf("release %d has unsupported status", releases[index].Version)
		}
		if releases[index].Status == "active" {
			activeIndexes = append(activeIndexes, index)
		}
	}
	requiresNormalization := false
	if allowLegacyMultipleActive {
		deployingTails, err := validateV013StoppedDeployingTails(releases, activeIndexes)
		if err != nil {
			return false, err
		}
		requiresNormalization = len(deployingTails) > 0
	}
	if len(activeIndexes) <= 1 {
		return requiresNormalization, nil
	}
	if !allowLegacyMultipleActive {
		return false, fmt.Errorf("release history has %d active records", len(activeIndexes))
	}
	if err := validateV013RecordMigrationHistory(releases, activeIndexes); err != nil {
		return false, fmt.Errorf(
			"release history has %d active records and is not a recognizable v0.13 migration: %w",
			len(activeIndexes),
			err,
		)
	}
	return true, nil
}

// validateV013StoppedDeployingTails closes the v0.13 Update/Restart
// append-before-Compose crash window during stopped adoption. A deploying row
// after LatestActive is ambiguous because Docker may run either manifest: the
// process could have stopped before Compose or after Compose+health but before
// ActivateLatest. Container image/topology labels do not prove environment,
// command, or other manifest fields. A row is safely normalizable only when its
// complete parsed manifest is semantically identical to LatestActive; any
// number of such stopped retry rows carry no distinct workload authority and
// can be marked superseded while binding the lineage. Every post-active
// deploying row is checked independently so an older ambiguous attempt cannot
// hide behind a later failed or equivalent attempt.
func validateV013StoppedDeployingTails(
	releases []Release,
	activeIndexes []int,
) ([]int, error) {
	if len(activeIndexes) == 0 {
		if slices.ContainsFunc(releases, func(release Release) bool {
			return release.Status == "deploying"
		}) {
			return nil, errors.New(
				"stopped v0.13 release history has deploying authority but no LatestActive generation; restore a complete matching v0.13 snapshot and settle or roll back that operation before adoption",
			)
		}
		return nil, nil
	}
	latestActive := activeIndexes[len(activeIndexes)-1]
	deployingIndexes := make([]int, 0, 1)
	for index := latestActive + 1; index < len(releases); index++ {
		if releases[index].Status == "deploying" {
			deployingIndexes = append(deployingIndexes, index)
		}
	}
	if len(deployingIndexes) == 0 {
		return nil, nil
	}
	active := releases[latestActive]
	if active.OperationID != "" || len(active.Items) != 0 ||
		len(active.ResourceProfiles) != 0 || active.LegacyMigration ||
		len(active.Manifest) == 0 {
		return nil, errors.New(
			"stopped release history has post-active deploying rows whose LatestActive is not wire-recognizable v0.13 authority",
		)
	}
	for _, index := range deployingIndexes {
		candidate := releases[index]
		if candidate.OperationID != "" || len(candidate.Items) != 0 ||
			len(candidate.ResourceProfiles) != 0 || candidate.LegacyMigration ||
			candidate.Image != "stack" || len(candidate.Manifest) == 0 {
			return nil, fmt.Errorf(
				"stopped release %d is an unresolved deploying row that is not a wire-recognizable v0.13 Update/Restart row",
				candidate.Version,
			)
		}
		equal, err := semanticallyEqualReleaseManifests(active.Manifest, candidate.Manifest)
		if err != nil {
			return nil, fmt.Errorf(
				"compare stopped v0.13 deploying release %d with LatestActive: %w",
				candidate.Version,
				err,
			)
		}
		if !equal {
			return nil, fmt.Errorf(
				"stopped v0.13 release %d is deploying a manifest that differs from LatestActive; Docker may run either generation, so restore a complete matching v0.13 snapshot and settle or roll back that operation before adoption",
				candidate.Version,
			)
		}
	}
	return deployingIndexes, nil
}

// validateV013RecordMigrationHistory recognizes only the duplicate-active
// shape v0.13 itself could write. The old implementation appended one active
// stack-wrapped migration row without superseding its one older active flat
// row. It no-opped when the latest active manifest was already byte-equal, so
// an actually appended pair must be byte-different; more than two actives,
// equal raw manifests, or current authority fields are not explained by that
// writer and remain corrupt. Later non-active rows are permitted: a failed
// update appended after migration legitimately leaves LatestActive pointing at
// the migrated row while that failed row is the history tail.
func validateV013RecordMigrationHistory(releases []Release, activeIndexes []int) error {
	if len(activeIndexes) != 2 {
		return errors.New("v0.13 migration must have exactly two active records")
	}
	oldIndex, migratedIndex := activeIndexes[0], activeIndexes[1]
	for index := range releases {
		release := releases[index]
		if release.OperationID != "" || len(release.Items) != 0 ||
			len(release.ResourceProfiles) != 0 || release.LegacyMigration {
			return fmt.Errorf("release %d contains post-v0.13 authority fields", release.Version)
		}
	}
	oldRelease := releases[oldIndex]
	migratedRelease := releases[migratedIndex]
	if migratedRelease.Image != "stack" ||
		len(oldRelease.Manifest) == 0 || len(migratedRelease.Manifest) == 0 {
		return errors.New("v0.13 migration pair must end in a stack image manifest")
	}
	if bytes.Equal(oldRelease.Manifest, migratedRelease.Manifest) {
		return errors.New("v0.13 RecordMigration could not have appended a byte-equal manifest")
	}
	oldWasStack, err := releasePayloadUsesStackEnvelope(oldRelease.Manifest)
	if err != nil {
		return fmt.Errorf("inspect older active manifest envelope: %w", err)
	}
	migratedWasStack, err := releasePayloadUsesStackEnvelope(migratedRelease.Manifest)
	if err != nil {
		return fmt.Errorf("inspect migrated active manifest envelope: %w", err)
	}
	if oldWasStack || !migratedWasStack {
		return errors.New("v0.13 migration pair is not flat-manifest then stack-manifest")
	}
	oldStack, err := manifest.ParsePayload(oldRelease.Manifest)
	if err != nil {
		return fmt.Errorf("parse older active manifest: %w", err)
	}
	semanticallyEqual, err := semanticallyEqualReleaseManifests(
		oldRelease.Manifest,
		migratedRelease.Manifest,
	)
	if err != nil {
		return fmt.Errorf("compare v0.13 migration manifests: %w", err)
	}
	if !semanticallyEqual {
		return errors.New("v0.13 migration manifests are not semantically identical")
	}
	oldService := oldStack.Services[manifest.DefaultServiceName]
	if oldService == nil || (oldRelease.Image != "stack" && oldRelease.Image != oldService.Image) {
		return errors.New("older active image is not explained by its legacy flat manifest")
	}
	return nil
}

func semanticallyEqualReleaseManifests(left, right []byte) (bool, error) {
	leftStack, err := manifest.ParsePayload(left)
	if err != nil {
		return false, fmt.Errorf("parse first manifest: %w", err)
	}
	rightStack, err := manifest.ParsePayload(right)
	if err != nil {
		return false, fmt.Errorf("parse second manifest: %w", err)
	}
	leftCanonical, err := json.Marshal(leftStack)
	if err != nil {
		return false, fmt.Errorf("canonicalize first manifest: %w", err)
	}
	rightCanonical, err := json.Marshal(rightStack)
	if err != nil {
		return false, fmt.Errorf("canonicalize second manifest: %w", err)
	}
	return bytes.Equal(leftCanonical, rightCanonical), nil
}

func releasePayloadUsesStackEnvelope(payload []byte) (bool, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil {
		return false, err
	}
	_, stack := object["services"]
	return stack, nil
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
			LimitBytes: limitBytes, RequiredBytes: releaseHistoryArrayFramingBytes,
		}
	}
	encoded, err := json.Marshal(releases)
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

const releaseHistoryArrayFramingBytes = 2 // '[' + ']'

// compactReleaseHistoryWithinLimit selects the largest deterministic suffix of
// non-authoritative history that fits beside the records required for recovery
// and cleanup. The index-latest row preserves max-version monotonicity, the
// most-recent active row is runtime rehydration authority, and the most-recent
// LegacyMigration row names any rollback-window cohort. Expired disposable
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
			LimitBytes: limitBytes, RequiredBytes: releaseHistoryArrayFramingBytes,
		}
	}

	entryBytes := make([]int, len(releases))
	encodedBytes := releaseHistoryArrayFramingBytes + len(releases) - 1
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
	for index := len(releases) - 1; index >= 0; index-- {
		if releases[index].LegacyMigration {
			protected[index] = true
			break
		}
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

func newReleaseHistoryCapacitySnapshot(
	releases []Release,
) (releaseHistoryCapacitySnapshot, error) {
	snapshot := releaseHistoryCapacitySnapshot{
		entries:     make([]releaseHistoryCapacityEntry, len(releases)),
		activeIndex: latestActiveReleaseIndex(releases),
	}
	for index := range releases {
		encoded, err := json.Marshal(releases[index])
		if err != nil {
			return releaseHistoryCapacitySnapshot{}, fmt.Errorf(
				"marshal release %d: %w",
				releases[index].Version,
				err,
			)
		}
		snapshot.entries[index] = releaseHistoryCapacityEntry{
			encodedBytes:    len(encoded),
			status:          releases[index].Status,
			legacyMigration: releases[index].LegacyMigration,
		}
	}
	return snapshot, nil
}

func normalizeLegacyReleaseHistoryForAdoption(releases []Release) ([]Release, error) {
	normalized := slices.Clone(releases)
	requiresNormalization, err := validateReleaseHistoryForAdoption(normalized, true)
	if err != nil {
		return nil, err
	}
	if !requiresNormalization {
		return normalized, nil
	}
	latest := latestActiveReleaseIndex(normalized)
	for index := range normalized {
		if index != latest && normalized[index].Status == "active" {
			normalized[index].Status = "superseded"
		}
	}
	deployingTails, err := validateV013StoppedDeployingTails(normalized, []int{latest})
	if err != nil {
		return nil, fmt.Errorf("validate normalizable v0.13 deploying rows: %w", err)
	}
	for _, deployingTail := range deployingTails {
		normalized[deployingTail].Status = "superseded"
		normalized[deployingTail].Reason = ""
		normalized[deployingTail].Message = ""
		normalized[deployingTail].Error = ""
	}
	if err := validateReleaseHistory(normalized); err != nil {
		return nil, fmt.Errorf("validate normalized release history: %w", err)
	}
	return normalized, nil
}

func normalizeLegacyReleaseBucketForAdoption(tx *bolt.Tx) error {
	bucket := tx.Bucket(releasesBucketName)
	if bucket == nil {
		return errors.New("releases bucket is missing")
	}
	type rewrite struct {
		key   []byte
		value []byte
	}
	var rewrites []rewrite
	budget := newStoppedAuthoritativeInspectionBudget()
	err := bucket.ForEach(func(key, value []byte) error {
		if err := budget.observe(key, value); err != nil {
			return err
		}
		if value == nil {
			return fmt.Errorf("release history with key length %d is a nested bucket", len(key))
		}
		if !backend.IsCanonicalLeaseUUID(string(key)) {
			return fmt.Errorf("release history key with length %d is not a canonical lease UUID", len(key))
		}
		var releases []Release
		if err := json.Unmarshal(value, &releases); err != nil {
			return fmt.Errorf("decode release history with key length %d: %w", len(key), err)
		}
		requiresNormalization, err := validateReleaseHistoryForAdoption(releases, true)
		if err != nil {
			return fmt.Errorf("validate release history with key length %d: %w", len(key), err)
		}
		if !requiresNormalization {
			return nil
		}
		releases, err = normalizeLegacyReleaseHistoryForAdoption(releases)
		if err != nil {
			return fmt.Errorf("normalize release history with key length %d: %w", len(key), err)
		}
		encoded, err := compactAndEncodeReleaseHistory(
			releases,
			time.Time{},
			backend.MaxStoredReleaseHistoryBytes,
		)
		if err != nil {
			return fmt.Errorf("encode normalized release history with key length %d: %w", len(key), err)
		}
		rewrites = append(rewrites, rewrite{
			key:   slices.Clone(key),
			value: encoded,
		})
		return nil
	})
	if err != nil {
		return err
	}
	for _, entry := range rewrites {
		if err := bucket.Put(entry.key, entry.value); err != nil {
			return fmt.Errorf("normalize v0.13 release history: %w", err)
		}
	}
	return inspectReleaseBucket(tx)
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

// Append adds a new release for a lease, auto-assigning Version.
func (s *ReleaseStore) Append(leaseUUID string, r Release) error {
	if r.MaintenanceID != "" {
		return ErrMaintenanceReleaseClaimRequired
	}
	return s.append(leaseUUID, r, false)
}

// AppendActive atomically supersedes every prior active release and appends a
// new active release with the next version. Success-boundary callers use this
// instead of composing Append and ActivateLatest across two crash boundaries.
// An empty Status is accepted because the method itself supplies the state;
// any other explicit status must agree with that authority.
func (s *ReleaseStore) AppendActive(leaseUUID string, r Release) error {
	if r.MaintenanceID != "" {
		return ErrMaintenanceReleaseClaimRequired
	}
	if r.Status != "" && r.Status != "active" {
		return errors.New("active release append requires active or empty status")
	}
	r.Status = "active"
	return s.append(leaseUUID, r, true)
}

// CheckAppendActiveCapacity proves, without writing, that AppendActive can
// retain every load-bearing record after deterministic capacity compaction.
// Callers that have durably accepted an asynchronous operation use this before
// the first substrate side effect; a capacity error is then a definitive
// refusal and the write-ahead intent can be canceled safely. AppendActive still
// repeats the same plan transactionally because recovery must converge after a
// crash and a read-only proof is not itself a storage reservation.
func (s *ReleaseStore) CheckAppendActiveCapacity(leaseUUID string, r Release) error {
	if r.MaintenanceID != "" {
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

func (s *ReleaseStore) append(leaseUUID string, r Release, supersedeActive bool) error {
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
	if r.MaintenanceID != "" {
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
		if err := json.Unmarshal(data, &releases); err != nil {
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
// capacity window used by Restart and Update. ActivateLatest slightly expands
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
		if err := json.Unmarshal(data, &releases); err != nil {
			return err
		}
		if err := validateReleaseHistory(releases); err != nil {
			return fmt.Errorf("invalid release data for %s: %w", leaseUUID, err)
		}
		return nil
	})

	return releases, err
}

// BackfillActiveResourceProfiles freezes the current resource authority onto a
// pre-v0.14 active release. The expected version and items form a compare-and-
// swap fence: a concurrent update cannot receive profiles belonging to an
// older topology. Existing exact profiles are accepted idempotently; divergent
// profiles fail closed.
func (s *ReleaseStore) BackfillActiveResourceProfiles(
	leaseUUID string,
	version int,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
) error {
	return s.backfillActiveResourceProfilesWithinLimit(
		leaseUUID,
		version,
		items,
		resourceProfiles,
		backend.MaxStoredReleaseHistoryBytes,
	)
}

func (s *ReleaseStore) backfillActiveResourceProfilesWithinLimit(
	leaseUUID string,
	version int,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
	limitBytes int,
) error {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	if version <= 0 {
		return fmt.Errorf("active release version must be positive")
	}
	if err := ValidateSKUResourceSnapshot(items, resourceProfiles); err != nil {
		return fmt.Errorf("backfill active release resource profiles: %w", err)
	}
	durableItems := slices.Clone(items)
	durableProfiles := CloneSKUResourceSnapshot(resourceProfiles)
	return s.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(releasesBucketName)
		if bucket == nil {
			return fmt.Errorf("releases bucket missing")
		}
		data := bucket.Get([]byte(leaseUUID))
		if data == nil {
			return fmt.Errorf("release history for %s no longer exists", leaseUUID)
		}
		var releases []Release
		if err := json.Unmarshal(data, &releases); err != nil {
			return fmt.Errorf("corrupted release data for %s: %w", leaseUUID, err)
		}
		if err := validateReleaseHistory(releases); err != nil {
			return fmt.Errorf("invalid release data for %s: %w", leaseUUID, err)
		}
		if err := rejectRawMaintenanceHistoryMutation(releases); err != nil {
			return err
		}
		activeIndex := -1
		for index := len(releases) - 1; index >= 0; index-- {
			if releases[index].Status == "active" {
				activeIndex = index
				break
			}
		}
		if activeIndex < 0 {
			return fmt.Errorf("active release for %s no longer exists", leaseUUID)
		}
		active := &releases[activeIndex]
		if active.Version != version || !slices.Equal(active.Items, durableItems) {
			return fmt.Errorf("active release for %s changed before resource profile backfill", leaseUUID)
		}
		if len(active.ResourceProfiles) > 0 {
			if err := ValidateSKUResourceSnapshot(active.Items, active.ResourceProfiles); err != nil {
				return fmt.Errorf("active release for %s has invalid resource profiles: %w", leaseUUID, err)
			}
			if !slices.Equal(active.ResourceProfiles, durableProfiles) {
				return fmt.Errorf("active release for %s has divergent resource profiles", leaseUUID)
			}
			return nil
		}
		active.ResourceProfiles = durableProfiles
		if err := validateReleaseHistory(releases); err != nil {
			return fmt.Errorf("invalid backfilled release data for %s: %w", leaseUUID, err)
		}
		encoded, err := compactAndEncodeReleaseHistory(
			releases,
			releaseHistoryCapacityCutoff(s.maxAge, time.Now()),
			limitBytes,
		)
		if err != nil {
			return fmt.Errorf("marshal release resource profile backfill: %w", err)
		}
		return bucket.Put([]byte(leaseUUID), encoded)
	})
}

// BackfillLegacyActiveAuthority freezes the exact desired topology and
// resource profiles that v0.13 omitted from an otherwise valid active release.
// The expected release is a compare-and-swap fence over every v0.13 field, not
// merely its version: a same-version manual rewrite cannot acquire authority
// from a stale container observation. The migration class may be selected only
// when the caller holds exact whole-cohort proof of v0.13 RecordMigration;
// persisting the marker in this same transaction ensures an immediate close can
// capture rollback-container IDs before release retirement. A replay after the
// exact write committed is idempotent.
func (s *ReleaseStore) BackfillLegacyActiveAuthority(
	leaseUUID string,
	expected Release,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
	class LegacyActiveAuthorityClass,
) error {
	return s.backfillLegacyActiveAuthorityWithinLimit(
		leaseUUID,
		expected,
		items,
		resourceProfiles,
		class,
		backend.MaxStoredReleaseHistoryBytes,
	)
}

func (s *ReleaseStore) backfillLegacyActiveAuthorityWithinLimit(
	leaseUUID string,
	expected Release,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
	class LegacyActiveAuthorityClass,
	limitBytes int,
) error {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	if expected.Version <= 0 || expected.Status != "active" {
		return errors.New("legacy active release fence must name a positive active version")
	}
	if expected.OperationID != "" || len(expected.Items) != 0 ||
		len(expected.ResourceProfiles) != 0 || expected.LegacyMigration {
		return errors.New("legacy active release fence contains current authority fields")
	}
	if !class.valid() {
		return errors.New("legacy active release authority class is invalid")
	}
	legacyMigration := class == LegacyActiveAuthorityMigration
	if err := ValidateSKUResourceSnapshot(items, resourceProfiles); err != nil {
		return fmt.Errorf("backfill legacy active release authority: %w", err)
	}
	stack, err := manifest.ParsePayload(expected.Manifest)
	if err != nil {
		return fmt.Errorf("backfill legacy active release manifest: %w", err)
	}
	if err := manifest.ValidateStackAgainstItems(stack, items); err != nil {
		return fmt.Errorf("backfill legacy active release topology: %w", err)
	}
	expected = cloneRelease(expected)
	durableItems := slices.Clone(items)
	durableProfiles := CloneSKUResourceSnapshot(resourceProfiles)
	return s.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(releasesBucketName)
		if bucket == nil {
			return errors.New("releases bucket missing")
		}
		data := bucket.Get([]byte(leaseUUID))
		if data == nil {
			return fmt.Errorf("release history for %s no longer exists", leaseUUID)
		}
		var releases []Release
		if err := json.Unmarshal(data, &releases); err != nil {
			return fmt.Errorf("corrupted release data for %s: %w", leaseUUID, err)
		}
		if err := validateReleaseHistory(releases); err != nil {
			return fmt.Errorf("invalid release data for %s: %w", leaseUUID, err)
		}
		if err := rejectRawMaintenanceHistoryMutation(releases); err != nil {
			return err
		}
		activeIndex := latestActiveReleaseIndex(releases)
		if activeIndex < 0 {
			return fmt.Errorf("active release for %s no longer exists", leaseUUID)
		}
		active := &releases[activeIndex]
		if active.Version != expected.Version || active.Status != expected.Status ||
			!bytes.Equal(active.Manifest, expected.Manifest) || active.Image != expected.Image ||
			!active.CreatedAt.Equal(expected.CreatedAt) || active.Error != expected.Error ||
			active.Reason != expected.Reason || active.Message != expected.Message ||
			active.OperationID != expected.OperationID {
			return fmt.Errorf("active release for %s changed before legacy authority backfill", leaseUUID)
		}
		if len(active.Items) > 0 || len(active.ResourceProfiles) > 0 {
			if slices.Equal(active.Items, durableItems) &&
				slices.Equal(active.ResourceProfiles, durableProfiles) &&
				active.LegacyMigration == legacyMigration {
				return nil
			}
			return fmt.Errorf("active release for %s has divergent backfilled authority", leaseUUID)
		}
		if active.LegacyMigration != expected.LegacyMigration {
			return fmt.Errorf("active release for %s changed before legacy authority backfill", leaseUUID)
		}
		active.Items = durableItems
		active.ResourceProfiles = durableProfiles
		active.LegacyMigration = legacyMigration
		if err := validateReleaseHistory(releases); err != nil {
			return fmt.Errorf("invalid backfilled release data for %s: %w", leaseUUID, err)
		}
		encoded, err := compactAndEncodeReleaseHistory(
			releases,
			releaseHistoryCapacityCutoff(s.maxAge, time.Now()),
			limitBytes,
		)
		if err != nil {
			return fmt.Errorf("marshal legacy active authority backfill: %w", err)
		}
		return bucket.Put([]byte(leaseUUID), encoded)
	})
}

// BackfillLegacyRuntimeAuthority CAS-persists the exact tokenless principal
// and callback pair observed on a validated v0.13 active cohort. Callers must
// complete whole-cohort validation before invoking this method. Once durable,
// the authority permits recovery to materialize the same active Release after
// its last container has been safely removed during a replacement attempt.
//
// This intentionally does not manufacture an OperationID: the two authority
// classes remain disjoint on disk and in the type system.
func (s *ReleaseStore) BackfillLegacyRuntimeAuthority(
	leaseUUID string,
	expected Release,
	authority LegacyRuntimeAuthority,
) error {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	if expected.Version <= 0 || expected.Status != "active" {
		return errors.New("legacy runtime authority fence must name a positive active version")
	}
	if expected.OperationID != "" || expected.RuntimeAuthority != nil ||
		len(expected.Items) == 0 || len(expected.ResourceProfiles) == 0 {
		return errors.New("legacy runtime authority fence is not a fully backfilled v0.13 release")
	}
	if !authority.valid {
		return errors.New("legacy runtime authority is invalid")
	}
	expected = cloneRelease(expected)
	durableAuthority := authority
	return s.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(releasesBucketName)
		if bucket == nil {
			return errors.New("releases bucket missing")
		}
		data := bucket.Get([]byte(leaseUUID))
		if data == nil {
			return fmt.Errorf("release history for %s no longer exists", leaseUUID)
		}
		var releases []Release
		if err := json.Unmarshal(data, &releases); err != nil {
			return fmt.Errorf("corrupted release data for %s: %w", leaseUUID, err)
		}
		if err := validateReleaseHistory(releases); err != nil {
			return fmt.Errorf("invalid release data for %s: %w", leaseUUID, err)
		}
		if err := rejectRawMaintenanceHistoryMutation(releases); err != nil {
			return err
		}
		activeIndex := latestActiveReleaseIndex(releases)
		if activeIndex < 0 {
			return fmt.Errorf("active release for %s no longer exists", leaseUUID)
		}
		active := &releases[activeIndex]
		if active.Version != expected.Version || active.Status != expected.Status ||
			!bytes.Equal(active.Manifest, expected.Manifest) || active.Image != expected.Image ||
			!active.CreatedAt.Equal(expected.CreatedAt) || active.Error != expected.Error ||
			active.Reason != expected.Reason || active.Message != expected.Message ||
			active.OperationID != "" || active.RuntimeAuthority != nil ||
			active.MaintenanceID != expected.MaintenanceID ||
			active.LegacyMigration != expected.LegacyMigration ||
			!slices.Equal(active.Items, expected.Items) ||
			!slices.Equal(active.ResourceProfiles, expected.ResourceProfiles) {
			return fmt.Errorf("active release for %s changed before legacy runtime authority backfill", leaseUUID)
		}
		if active.LegacyRuntimeAuthority != nil {
			if *active.LegacyRuntimeAuthority == durableAuthority {
				return nil
			}
			return fmt.Errorf("active release for %s has divergent legacy runtime authority", leaseUUID)
		}
		active.LegacyRuntimeAuthority = &durableAuthority
		if err := validateReleaseHistory(releases); err != nil {
			return fmt.Errorf("invalid legacy runtime authority for %s: %w", leaseUUID, err)
		}
		encoded, err := compactAndEncodeReleaseHistory(
			releases,
			releaseHistoryCapacityCutoff(s.maxAge, time.Now()),
			backend.MaxStoredReleaseHistoryBytes,
		)
		if err != nil {
			return fmt.Errorf("marshal legacy runtime authority backfill: %w", err)
		}
		return bucket.Put([]byte(leaseUUID), encoded)
	})
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

// UpdateLatestStatus updates the status and curated (reason, message) of the
// most recent release. The verbose per-failure detail is intentionally NOT
// stored here — callers log it and surface only the curated pair to tenants.
func (s *ReleaseStore) UpdateLatestStatus(leaseUUID, status string, reason backend.Reason, message string) error {
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
		if err := json.Unmarshal(data, &releases); err != nil {
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

// ActivateLatest marks the most recent release as "active" and all previous
// "active" releases as "superseded" in a single transaction.
func (s *ReleaseStore) ActivateLatest(leaseUUID string) error {
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
		if err := json.Unmarshal(data, &releases); err != nil {
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

// RecordLegacyMigration appends an "active" release entry for a recover-time
// v0.13 migration so the lease's release history captures the wrap-and-rename
// step and its tokenless runtime identity in one commit. Idempotent: if the
// most-recent active entry already carries the same wrapped manifest, desired
// topology, and authority, the call is a no-op. An older manifest-only entry is
// upgraded in place so migration recovery gains an exact cohort invariant
// without inflating release history.
//
// Used exclusively by the docker backend's migrate.go (Task 9).
func (s *ReleaseStore) RecordLegacyMigration(
	leaseUUID string,
	manifest []byte,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
	authority LegacyRuntimeAuthority,
) error {
	return s.RecordLegacyMigrationAt(
		leaseUUID,
		manifest,
		items,
		resourceProfiles,
		authority,
		time.Now(),
	)
}

// CheckRecordLegacyMigrationCapacity proves that the exact migration release,
// including its runtime authority, can be committed after deterministic history
// compaction. createdAt must be reused by RecordLegacyMigrationAt so the proof
// and the post-substrate write have identical wire size; Docker calls this
// before stopping or renaming a legacy cohort.
func (s *ReleaseStore) CheckRecordLegacyMigrationCapacity(
	leaseUUID string,
	manifest []byte,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
	authority LegacyRuntimeAuthority,
	createdAt time.Time,
) error {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	if err := validateLegacyMigrationReleaseInput(items, resourceProfiles, authority, createdAt); err != nil {
		return err
	}
	durableManifest := slices.Clone(manifest)
	durableItems := slices.Clone(items)
	durableProfiles := CloneSKUResourceSnapshot(resourceProfiles)
	return s.view(func(tx *bolt.Tx) error {
		_, _, err := planMigrationReleaseHistory(
			tx.Bucket(releasesBucketName).Get([]byte(leaseUUID)),
			leaseUUID,
			durableManifest,
			durableItems,
			durableProfiles,
			authority,
			createdAt,
			releaseHistoryCapacityCutoff(s.maxAge, time.Now()),
			backend.MaxStoredReleaseHistoryBytes,
		)
		return err
	})
}

// RecordLegacyMigrationAt is RecordLegacyMigration with an explicit admission
// timestamp.
// It exists so the pre-side-effect capacity proof and post-Compose durable
// commit can describe the exact same release bytes.
func (s *ReleaseStore) RecordLegacyMigrationAt(
	leaseUUID string,
	manifest []byte,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
	authority LegacyRuntimeAuthority,
	createdAt time.Time,
) error {
	return s.recordLegacyMigrationWithinLimit(
		leaseUUID,
		manifest,
		items,
		resourceProfiles,
		authority,
		createdAt,
		backend.MaxStoredReleaseHistoryBytes,
	)
}

func (s *ReleaseStore) recordLegacyMigrationWithinLimit(
	leaseUUID string,
	manifest []byte,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
	authority LegacyRuntimeAuthority,
	createdAt time.Time,
	limitBytes int,
) error {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	if err := validateLegacyMigrationReleaseInput(items, resourceProfiles, authority, createdAt); err != nil {
		return err
	}
	durableManifest := slices.Clone(manifest)
	durableItems := slices.Clone(items)
	durableProfiles := CloneSKUResourceSnapshot(resourceProfiles)
	return s.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(releasesBucketName)
		releases, changed, err := planMigrationReleaseHistory(
			bucket.Get([]byte(leaseUUID)),
			leaseUUID,
			durableManifest,
			durableItems,
			durableProfiles,
			authority,
			createdAt,
			releaseHistoryCapacityCutoff(s.maxAge, time.Now()),
			limitBytes,
		)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}
		encoded, err := encodeReleaseHistoryWithinLimit(releases, limitBytes)
		if err != nil {
			return fmt.Errorf("marshal migration releases: %w", err)
		}
		return bucket.Put([]byte(leaseUUID), encoded)
	})
}

func validateLegacyMigrationReleaseInput(
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
	authority LegacyRuntimeAuthority,
	createdAt time.Time,
) error {
	if len(items) == 0 {
		return errors.New("migration release requires desired items")
	}
	if err := ValidateSKUResourceSnapshot(items, resourceProfiles); err != nil {
		return fmt.Errorf("migration release resource profiles: %w", err)
	}
	if !authority.valid {
		return errors.New("migration release requires valid legacy runtime authority")
	}
	if createdAt.IsZero() {
		return errors.New("migration release requires an admission timestamp")
	}
	return nil
}

func planMigrationReleaseHistory(
	data []byte,
	leaseUUID string,
	manifest []byte,
	items []backend.LeaseItem,
	resourceProfiles []SKUResourceSnapshot,
	authority LegacyRuntimeAuthority,
	createdAt time.Time,
	cutoff time.Time,
	limitBytes int,
) ([]Release, bool, error) {
	var releases []Release
	if data != nil {
		if err := json.Unmarshal(data, &releases); err != nil {
			return nil, false, fmt.Errorf("corrupted release data for %s: %w", leaseUUID, err)
		}
		if err := validateReleaseHistory(releases); err != nil {
			return nil, false, fmt.Errorf("invalid release data for %s: %w", leaseUUID, err)
		}
		if err := rejectRawMaintenanceHistoryMutation(releases); err != nil {
			return nil, false, err
		}
	}

	latestActive := latestActiveReleaseIndex(releases)
	if latestActive >= 0 && bytes.Equal(releases[latestActive].Manifest, manifest) {
		if releases[latestActive].OperationID != "" || releases[latestActive].RuntimeAuthority != nil {
			return nil, false, fmt.Errorf("active migration release for %s has typed runtime authority", leaseUUID)
		}
		if existing := releases[latestActive].LegacyRuntimeAuthority; existing != nil && *existing != authority {
			return nil, false, fmt.Errorf("active migration release for %s has divergent legacy runtime authority", leaseUUID)
		}
		if len(releases[latestActive].Items) != 0 &&
			!slices.Equal(releases[latestActive].Items, items) {
			return nil, false, fmt.Errorf("active migration release for %s has divergent desired items", leaseUUID)
		}
		if len(releases[latestActive].ResourceProfiles) > 0 &&
			!slices.Equal(releases[latestActive].ResourceProfiles, resourceProfiles) {
			return nil, false, fmt.Errorf("active migration release for %s has divergent resource profiles", leaseUUID)
		}
		if slices.Equal(releases[latestActive].Items, items) &&
			slices.Equal(releases[latestActive].ResourceProfiles, resourceProfiles) &&
			releases[latestActive].LegacyMigration &&
			releases[latestActive].LegacyRuntimeAuthority != nil {
			return releases, false, nil
		}
		releases[latestActive].Items = slices.Clone(items)
		releases[latestActive].ResourceProfiles = CloneSKUResourceSnapshot(resourceProfiles)
		releases[latestActive].LegacyMigration = true
		releases[latestActive].LegacyRuntimeAuthority = cloneLegacyRuntimeAuthority(&authority)
	} else {
		for index := range releases {
			if releases[index].Status == "active" {
				releases[index].Status = "superseded"
			}
		}
		releases = append(releases, Release{
			Version:                maxVersion(releases) + 1,
			Manifest:               slices.Clone(manifest),
			Image:                  "stack",
			Items:                  slices.Clone(items),
			ResourceProfiles:       CloneSKUResourceSnapshot(resourceProfiles),
			LegacyRuntimeAuthority: cloneLegacyRuntimeAuthority(&authority),
			LegacyMigration:        true,
			Status:                 "active",
			CreatedAt:              createdAt,
		})
	}
	if err := validateReleaseHistory(releases); err != nil {
		return nil, false, fmt.Errorf("invalid migration release data for %s: %w", leaseUUID, err)
	}
	compacted, _, err := compactReleaseHistoryWithinLimit(releases, cutoff, limitBytes)
	if err != nil {
		return nil, false, fmt.Errorf("compact migration release history: %w", err)
	}
	if err := validateReleaseHistory(compacted); err != nil {
		return nil, false, fmt.Errorf("invalid compacted migration release history: %w", err)
	}
	return compacted, true, nil
}

// Delete removes all releases for a lease. No-op if not found.
func (s *ReleaseStore) Delete(leaseUUID string) error {
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
		if err := json.Unmarshal(data, &releases); err != nil {
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
		return release.MaintenanceID != ""
	}) {
		return ErrMaintenanceReleaseClaimRequired
	}
	return nil
}

func rejectUnresolvedMaintenanceHistoryMutation(releases []Release) error {
	if slices.ContainsFunc(releases, func(release Release) bool {
		return release.MaintenanceID != "" && release.Status == "deploying"
	}) {
		return ErrMaintenanceReleaseClaimRequired
	}
	return nil
}

// DeleteCloseHistory removes a lease's release history only while the release
// selected at close admission still matches the caller's exact fence. Absence
// is success because a crash may happen after this transaction commits but
// before the close journal is resolved. A zero fence proves that no release
// existed at admission and therefore refuses to erase a subsequently-created
// history.
//
// The selected row is the most-recent active release, or the index-latest row
// when no active release exists. This mirrors close admission and lets a close
// preempt a persisted-but-incomplete update while still fencing the live
// topology it is authorized to destroy.
func (s *ReleaseStore) DeleteCloseHistory(
	leaseUUID string,
	version int,
	digest [sha256.Size]byte,
) error {
	if err := s.requireCanonicalLeaseUUID(leaseUUID); err != nil {
		return err
	}
	if version < 0 {
		return fmt.Errorf("close release version cannot be negative")
	}
	if (version == 0) != (digest == ([sha256.Size]byte{})) {
		return fmt.Errorf("close release fence must be wholly absent or wholly present")
	}

	return s.update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(releasesBucketName)
		if bucket == nil {
			return fmt.Errorf("releases bucket missing")
		}
		key := []byte(leaseUUID)
		data := bucket.Get(key)
		if data == nil {
			// Idempotent replay after the deletion transaction committed.
			return nil
		}
		if version == 0 {
			return fmt.Errorf("release history appeared after close admission for %s", leaseUUID)
		}

		var releases []Release
		if err := json.Unmarshal(data, &releases); err != nil {
			return fmt.Errorf("corrupted release data for %s: %w", leaseUUID, err)
		}
		if err := validateReleaseHistory(releases); err != nil {
			return fmt.Errorf("invalid release data for %s: %w", leaseUUID, err)
		}
		selected := releaseForCloseFence(releases)
		if selected == nil {
			return fmt.Errorf("release history for %s is empty", leaseUUID)
		}
		encoded, err := json.Marshal(selected)
		if err != nil {
			return fmt.Errorf("marshal release fence for %s: %w", leaseUUID, err)
		}
		if selected.Version != version || sha256.Sum256(encoded) != digest {
			return fmt.Errorf("release history changed after close admission for %s", leaseUUID)
		}
		return bucket.Delete(key)
	})
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

// RemoveOlderThan prunes release history older than maxAge while preserving each
// lease's load-bearing records. For every lease it ALWAYS retains its index-latest
// entry — the entry the runtime mutators append to / activate, and (because Append
// assigns the next version to the tail) the holder of the lease's maximum version,
// which keeps Append's max+1 derivation collision-free — AND, when a distinct one
// exists, the most-recent "active" release that recoverState rehydrates the
// StackManifest from (see LatestActive). So it protects one entry when the newest
// entry is itself the active release or no active release exists, two otherwise. Only
// entries that are BOTH older than the cutoff AND not protected are pruned, so a
// lease's record is never emptied and its live manifest is never removed (ENG-440).
// The most-recent LegacyMigration entry is protected too: a later update may
// supersede it while its rollback-window containers still exist, and pruning that
// row would erase the exact cleanup authority required by Deprovision.
// Corrupt or empty values are preserved and fail the sweep closed: exact release
// topology is recovery authority, so ambiguity must remain visible for repair.
// Returns the number of valid release entries removed.
func (s *ReleaseStore) RemoveOlderThan(maxAge time.Duration) (int, error) {
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
			if err := json.Unmarshal(v, &releases); err != nil {
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
			// version), the most-recent "active" entry (the manifest-rehydration
			// source), and the most-recent legacy-migration entry (exact cleanup
			// authority for any surviving rollback-window containers).
			keepLatest := len(releases) - 1
			keepActive := -1
			keepMigration := -1
			for i := len(releases) - 1; i >= 0; i-- {
				if keepActive < 0 && releases[i].Status == "active" {
					keepActive = i
				}
				if keepMigration < 0 && releases[i].LegacyMigration {
					keepMigration = i
				}
				if keepActive >= 0 && keepMigration >= 0 {
					break
				}
			}

			kept := make([]Release, 0, len(releases))
			prunedHere := 0
			for i, r := range releases {
				if i != keepLatest && i != keepActive && i != keepMigration && r.CreatedAt.Before(cutoff) {
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
