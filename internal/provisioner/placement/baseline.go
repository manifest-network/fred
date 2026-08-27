package placement

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/provisioner/operation"
)

var (
	metadataBucketName = []byte("placement_metadata")
	metadataStateKey   = []byte("topology_state")
)

const topologyMetadataSchema = 1

var (
	// ErrInvalidBackendTopology means the configured backend storage identities
	// are empty, blank, duplicated, or otherwise malformed.
	ErrInvalidBackendTopology = errors.New("invalid placement backend topology")
	// ErrBackendTopologyNotConfigured means no durable backend topology has been
	// established for this store yet.
	ErrBackendTopologyNotConfigured = errors.New("placement backend topology is not configured")
	// ErrBackendNotInTopology means a mutation names a backend outside the
	// currently configured durable topology.
	ErrBackendNotInTopology = errors.New("backend is not in the placement topology")
	// ErrBackendTopologyInUse means changing topology would orphan a durable
	// Backend, Attempt, or ConflictBackends reference.
	ErrBackendTopologyInUse = errors.New("placement backend topology is still in use")
	// ErrBackendIdentityReused means a retired durable backend storage identity
	// was added again. A retired name can never identify a different store.
	ErrBackendIdentityReused = errors.New("retired placement backend identity cannot be reused")
	// ErrInvalidAdmissionBaseline means an admission capability is zero, stale,
	// foreign, or does not match the currently configured topology.
	ErrInvalidAdmissionBaseline = errors.New("invalid placement admission baseline")
	// ErrInvalidAdmissionScope means a scoped admission capability is zero,
	// stale, foreign, or does not match the currently configured topology.
	ErrInvalidAdmissionScope = errors.New("invalid placement admission scope")
	// ErrBackendOutsideAdmissionScope means a recordless attempt names a
	// configured backend that the scope did not authorize.
	ErrBackendOutsideAdmissionScope = errors.New("backend is outside placement admission scope")
)

// AdmissionBaseline is the opaque durable-topology capability required before
// recordless or exact-owner work may create a write-ahead attempt. The zero
// value is invalid. Callers obtain the current capability from the Store after
// a complete inventory has been committed for the configured topology.
type AdmissionBaseline struct {
	issuer      *Store
	topologyID  uint64
	fingerprint string
}

// Valid reports whether this is a structurally complete capability. Its Store
// still checks the exact current durable topology when consuming it.
func (baseline AdmissionBaseline) Valid() bool {
	return baseline.issuer != nil && baseline.topologyID != 0 && baseline.fingerprint != ""
}

// AdmissionScope is the opaque, topology-bound capability required to create a
// recordless attempt. It attenuates an AdmissionBaseline to an exact set of
// eligible backends. The zero value is invalid; an explicitly issued empty
// scope is valid and authorizes no backend.
type AdmissionScope struct {
	issuer      *Store
	topologyID  uint64
	fingerprint string
	eligible    map[string]struct{}
}

// Valid reports whether scope was explicitly issued for a topology. The Store
// still validates that topology atomically when consuming the capability.
func (scope AdmissionScope) Valid() bool {
	return scope.issuer != nil && scope.topologyID != 0 && scope.fingerprint != "" &&
		scope.eligible != nil
}

// Allows reports whether backendName belongs to this scope's immutable eligible
// set. It deliberately does not turn structural validity into current authority;
// BeginNewAttempt revalidates the issuing Store and topology under its lock.
func (scope AdmissionScope) Allows(backendName string) bool {
	if !scope.Valid() || backendName == "" {
		return false
	}
	_, ok := scope.eligible[backendName]
	return ok
}

type topologyMetadata struct {
	Schema              uint64   `json:"schema"`
	Topology            []string `json:"topology,omitempty"`
	TopologyFingerprint string   `json:"topology_fingerprint,omitempty"`
	KnownBackends       []string `json:"known_backends,omitempty"`
	TopologyID          uint64   `json:"topology_id,omitempty"`
	BaselineFingerprint string   `json:"baseline_fingerprint,omitempty"`
	BaselineTopologyID  uint64   `json:"baseline_topology_id,omitempty"`
}

func emptyTopologyMetadata() topologyMetadata {
	return topologyMetadata{Schema: topologyMetadataSchema}
}

// initializeMetadata creates a schema-bearing metadata bucket for a new or
// pre-baseline database. Once the bucket exists, a missing state record is
// corruption rather than an invitation to forget retired backend identities.
func initializeMetadata(tx *bolt.Tx) error {
	b := tx.Bucket(metadataBucketName)
	if b != nil {
		if b.Get(metadataStateKey) == nil {
			return errors.New("placement metadata state missing")
		}
		return nil
	}

	var err error
	b, err = tx.CreateBucket(metadataBucketName)
	if err != nil {
		return err
	}
	encoded, err := encodeTopologyMetadata(emptyTopologyMetadata())
	if err != nil {
		return err
	}
	return b.Put(metadataStateKey, encoded)
}

func loadTopologyMetadata(tx *bolt.Tx) (topologyMetadata, error) {
	b := tx.Bucket(metadataBucketName)
	if b == nil {
		return topologyMetadata{}, errors.New("placement metadata bucket missing")
	}
	encoded := b.Get(metadataStateKey)
	if encoded == nil {
		return topologyMetadata{}, errors.New("placement metadata state missing")
	}
	var metadata topologyMetadata
	if err := json.Unmarshal(encoded, &metadata); err != nil {
		return topologyMetadata{}, fmt.Errorf("decode placement metadata: %w", err)
	}
	if err := validateTopologyMetadata(metadata); err != nil {
		return topologyMetadata{}, err
	}
	return metadata, nil
}

func encodeTopologyMetadata(metadata topologyMetadata) ([]byte, error) {
	if err := validateTopologyMetadata(metadata); err != nil {
		return nil, err
	}
	return json.Marshal(metadata)
}

func validateTopologyMetadata(metadata topologyMetadata) error {
	if metadata.Schema != topologyMetadataSchema {
		return fmt.Errorf("unsupported placement metadata schema %d", metadata.Schema)
	}
	if metadata.TopologyID == 0 {
		if len(metadata.Topology) != 0 || metadata.TopologyFingerprint != "" ||
			len(metadata.KnownBackends) != 0 || metadata.BaselineFingerprint != "" ||
			metadata.BaselineTopologyID != 0 {
			return errors.New("malformed unconfigured placement metadata")
		}
		return nil
	}
	if err := validateCanonicalBackendNames(metadata.Topology, false); err != nil {
		return fmt.Errorf("malformed placement topology: %w", err)
	}
	if err := validateCanonicalBackendNames(metadata.KnownBackends, false); err != nil {
		return fmt.Errorf("malformed known placement backends: %w", err)
	}
	known := make(map[string]struct{}, len(metadata.KnownBackends))
	for _, backendName := range metadata.KnownBackends {
		known[backendName] = struct{}{}
	}
	for _, backendName := range metadata.Topology {
		if _, ok := known[backendName]; !ok {
			return fmt.Errorf("active backend %q missing from durable identity history", backendName)
		}
	}
	wantFingerprint, err := topologyFingerprint(metadata.Topology)
	if err != nil {
		return err
	}
	if metadata.TopologyFingerprint != wantFingerprint {
		return errors.New("placement topology fingerprint does not match topology")
	}
	if (metadata.BaselineFingerprint == "") != (metadata.BaselineTopologyID == 0) {
		return errors.New("incomplete placement admission baseline metadata")
	}
	if metadata.BaselineTopologyID > metadata.TopologyID {
		return errors.New("placement admission baseline is newer than topology")
	}
	if metadata.BaselineFingerprint != "" {
		decoded, err := hex.DecodeString(metadata.BaselineFingerprint)
		if err != nil || len(decoded) != sha256.Size ||
			metadata.BaselineFingerprint != strings.ToLower(metadata.BaselineFingerprint) {
			return errors.New("malformed placement admission baseline fingerprint")
		}
	}
	return nil
}

func validateCanonicalBackendNames(names []string, allowUnsorted bool) error {
	if len(names) == 0 {
		return fmt.Errorf("%w: at least one backend name is required", ErrInvalidBackendTopology)
	}
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("%w: backend names must be nonblank", ErrInvalidBackendTopology)
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("%w: duplicate backend name %q", ErrInvalidBackendTopology, name)
		}
		seen[name] = struct{}{}
	}
	if !allowUnsorted && !slices.IsSorted(names) {
		return fmt.Errorf("%w: backend names are not canonical", ErrInvalidBackendTopology)
	}
	return nil
}

func canonicalBackendTopology(names []string) ([]string, error) {
	if err := validateCanonicalBackendNames(names, true); err != nil {
		return nil, err
	}
	return slices.Sorted(slices.Values(names)), nil
}

func topologyFingerprint(names []string) (string, error) {
	encoded, err := json.Marshal(names)
	if err != nil {
		return "", fmt.Errorf("encode placement topology: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func putTopologyMetadata(tx *bolt.Tx, metadata topologyMetadata) error {
	encoded, err := encodeTopologyMetadata(metadata)
	if err != nil {
		return err
	}
	b := tx.Bucket(metadataBucketName)
	if b == nil {
		return errors.New("placement metadata bucket missing")
	}
	return b.Put(metadataStateKey, encoded)
}

// ConfigureBackendTopology durably binds backend names to storage identities.
// Reordering is idempotent. Removing a name is allowed only after all durable
// references are gone; a removed name is retired forever and cannot later be
// reused for another backend.
func (s *Store) ConfigureBackendTopology(names []string) error {
	canonical, err := canonicalBackendTopology(names)
	if err != nil {
		return err
	}
	fingerprint, err := topologyFingerprint(canonical)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	proposed := make(map[string]struct{}, len(canonical))
	for _, backendName := range canonical {
		proposed[backendName] = struct{}{}
	}
	current := make(map[string]struct{}, len(s.backendTopology))
	for _, backendName := range s.backendTopology {
		current[backendName] = struct{}{}
	}
	for _, backendName := range canonical {
		_, active := current[backendName]
		_, known := s.knownBackendNames[backendName]
		if !active && known {
			return fmt.Errorf("%w: %q", ErrBackendIdentityReused, backendName)
		}
	}

	same := slices.Equal(canonical, s.backendTopology)
	metadata := s.topologyMetadataLocked()
	if !same {
		if s.topologyID == ^uint64(0) {
			return errors.New("placement topology identity exhausted")
		}
		metadata.Topology = slices.Clone(canonical)
		metadata.TopologyFingerprint = fingerprint
		metadata.TopologyID = s.topologyID + 1
		known := maps.Clone(s.knownBackendNames)
		for backendName := range proposed {
			known[backendName] = struct{}{}
		}
		metadata.KnownBackends = slices.Sorted(maps.Keys(known))
	}

	if err := s.db.Update(func(tx *bolt.Tx) error {
		if err := rejectRemovedDurableBackends(tx, proposed); err != nil {
			return err
		}
		if same {
			_, err := loadTopologyMetadata(tx)
			return err
		}
		return putTopologyMetadata(tx, metadata)
	}); err != nil {
		return fmt.Errorf("configure placement backend topology: %w", err)
	}
	if same {
		return nil
	}

	s.backendTopology = slices.Clone(metadata.Topology)
	s.backendTopologySet = proposed
	s.topologyFingerprint = metadata.TopologyFingerprint
	s.topologyID = metadata.TopologyID
	s.knownBackendNames = make(map[string]struct{}, len(metadata.KnownBackends))
	for _, backendName := range metadata.KnownBackends {
		s.knownBackendNames[backendName] = struct{}{}
	}
	s.advanceAuthorityEpochLocked()
	return nil
}

func rejectRemovedDurableBackends(tx *bolt.Tx, proposed map[string]struct{}) error {
	b := tx.Bucket(bucketName)
	if b == nil {
		return errors.New("placements bucket missing")
	}
	return b.ForEach(func(key, value []byte) error {
		backendNames, err := durableBackendNames(value)
		if err != nil {
			return fmt.Errorf("%w: lease %q has uninterpretable durable placement: %w",
				ErrBackendTopologyInUse, string(key), err)
		}
		for _, backendName := range backendNames {
			if _, ok := proposed[backendName]; !ok {
				return fmt.Errorf("%w: lease %q still refers to backend %q",
					ErrBackendTopologyInUse, string(key), backendName)
			}
		}
		return nil
	})
}

func durableBackendNames(value []byte) ([]string, error) {
	if len(value) == 0 {
		return nil, errors.New("empty record")
	}
	if value[0] != '{' {
		if !validLegacyBackendName(value) {
			return nil, errors.New("legacy backend name is not printable UTF-8")
		}
		return []string{string(value)}, nil
	}
	var persisted record
	if err := json.Unmarshal(value, &persisted); err != nil {
		return nil, fmt.Errorf("decode record: %w", err)
	}
	names := make([]string, 0, 2+len(persisted.ConflictBackends))
	if persisted.Backend != "" {
		names = append(names, persisted.Backend)
	}
	if persisted.Attempt != "" {
		names = append(names, persisted.Attempt)
	}
	names = append(names, persisted.ConflictBackends...)
	names = normalizeBackendNames(names)
	if persisted.Conflict && (persisted.ConflictOwnersUnknown || len(names) < 2) {
		return nil, errors.New("conflict owner set is unknown")
	}
	if len(names) == 0 {
		return nil, errors.New("record has no backend identity")
	}
	return names, nil
}

func (s *Store) topologyMetadataLocked() topologyMetadata {
	return topologyMetadata{
		Schema:              topologyMetadataSchema,
		Topology:            slices.Clone(s.backendTopology),
		TopologyFingerprint: s.topologyFingerprint,
		KnownBackends:       slices.Sorted(maps.Keys(s.knownBackendNames)),
		TopologyID:          s.topologyID,
		BaselineFingerprint: s.baselineFingerprint,
		BaselineTopologyID:  s.baselineTopologyID,
	}
}

// CurrentAdmissionBaseline returns a capability only when a complete
// projection has durably established the currently configured topology.
func (s *Store) CurrentAdmissionBaseline() AdmissionBaseline {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.hasCurrentAdmissionBaselineLocked() {
		return AdmissionBaseline{}
	}
	return AdmissionBaseline{
		issuer:      s,
		topologyID:  s.topologyID,
		fingerprint: s.topologyFingerprint,
	}
}

func (s *Store) hasCurrentAdmissionBaselineLocked() bool {
	return s.topologyID != 0 && s.baselineTopologyID == s.topologyID &&
		s.baselineFingerprint != "" &&
		s.baselineFingerprint == s.topologyFingerprint
}

func (s *Store) validateAdmissionBaselineLocked(baseline AdmissionBaseline) error {
	if !baseline.Valid() || baseline.issuer != s ||
		baseline.topologyID != s.topologyID ||
		baseline.fingerprint != s.topologyFingerprint ||
		!s.hasCurrentAdmissionBaselineLocked() {
		return ErrInvalidAdmissionBaseline
	}
	return nil
}

// ScopeAdmission attenuates a current durable baseline to the exact configured
// backends that may receive a new recordless attempt. Names are canonicalized as
// an unordered set, must be nonblank and unique, and must be a subset of the
// baseline's topology. An empty input deliberately returns a valid deny-all
// scope so callers do not need an unsafe nil-means-unrestricted convention.
func (s *Store) ScopeAdmission(
	baseline AdmissionBaseline,
	eligibleNames []string,
) (AdmissionScope, error) {
	names := slices.Clone(eligibleNames)

	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.validateAdmissionBaselineLocked(baseline); err != nil {
		return AdmissionScope{}, err
	}
	canonical, err := canonicalAdmissionScopeNames(names)
	if err != nil {
		return AdmissionScope{}, err
	}
	eligible := make(map[string]struct{}, len(canonical))
	for _, backendName := range canonical {
		if err := s.validateConfiguredBackendLocked(backendName); err != nil {
			return AdmissionScope{}, err
		}
		eligible[backendName] = struct{}{}
	}
	return AdmissionScope{
		issuer:      s,
		topologyID:  s.topologyID,
		fingerprint: s.topologyFingerprint,
		eligible:    eligible,
	}, nil
}

func canonicalAdmissionScopeNames(names []string) ([]string, error) {
	canonical := slices.Clone(names)
	seen := make(map[string]struct{}, len(canonical))
	for _, name := range canonical {
		if strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("%w: backend names must be nonblank", ErrInvalidAdmissionScope)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate backend name %q", ErrInvalidAdmissionScope, name)
		}
		seen[name] = struct{}{}
	}
	slices.Sort(canonical)
	return canonical, nil
}

func (s *Store) validateAdmissionScopeLocked(scope AdmissionScope) error {
	if !scope.Valid() || scope.issuer != s ||
		scope.topologyID != s.topologyID ||
		scope.fingerprint != s.topologyFingerprint ||
		!s.hasCurrentAdmissionBaselineLocked() {
		return ErrInvalidAdmissionScope
	}
	return nil
}

func (s *Store) validateConfiguredBackendLocked(backendName string) error {
	if s.topologyID == 0 {
		return ErrBackendTopologyNotConfigured
	}
	if _, ok := s.backendTopologySet[backendName]; !ok {
		return fmt.Errorf("%w: %q", ErrBackendNotInTopology, backendName)
	}
	return nil
}

func (s *Store) validateProjectionBackendsLocked(projection InventoryProjection) error {
	if s.topologyID == 0 {
		if projection.Complete {
			return ErrBackendTopologyNotConfigured
		}
		return nil
	}
	validate := func(backendName string) error {
		return s.validateConfiguredBackendLocked(backendName)
	}
	for _, backendName := range projection.Placements {
		if err := validate(backendName); err != nil {
			return err
		}
	}
	for _, backendNames := range projection.Conflicts {
		for _, backendName := range backendNames {
			if err := validate(backendName); err != nil {
				return err
			}
		}
	}
	return nil
}

// BeginNewAttempt records a write-ahead attempt only when the lease has no
// durable placement evidence at the exact instant of insertion and the target
// belongs to the supplied topology-bound admission scope.
func (s *Store) BeginNewAttempt(
	scope AdmissionScope,
	leaseUUID, backendName string,
	operationID operation.OperationID,
) (AttemptToken, bool, error) {
	if err := validateTypedAttempt(leaseUUID, backendName, operationID); err != nil {
		return AttemptToken{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateAdmissionScopeLocked(scope); err != nil {
		return AttemptToken{}, false, err
	}
	if err := s.validateConfiguredBackendLocked(backendName); err != nil {
		return AttemptToken{}, false, err
	}
	if !scope.Allows(backendName) {
		return AttemptToken{}, false, fmt.Errorf(
			"%w: %q", ErrBackendOutsideAdmissionScope, backendName,
		)
	}
	if s.restoreSourceClaimedLocked(leaseUUID) {
		return AttemptToken{}, false, fmt.Errorf("%w: lease %q", ErrRestoreSourceClaimed, leaseUUID)
	}
	if _, exists := s.cache[leaseUUID]; exists {
		return AttemptToken{}, false, nil
	}
	revision, err := s.setAttemptingLocked(leaseUUID, backendName, operationID)
	if err != nil {
		return AttemptToken{}, false, err
	}
	return s.newAttemptToken(leaseUUID, backendName, operationID, revision), true, nil
}

// BeginOwnedAttempt records a write-ahead attempt only for the exact confirmed
// owner revision supplied by the caller. Stale, foreign, absent, conflicted, or
// already-attempting records fail the CAS without issuing a capability.
func (s *Store) BeginOwnedAttempt(
	baseline AdmissionBaseline,
	revision RecordRevision,
	backendName string,
	operationID operation.OperationID,
) (AttemptToken, bool, error) {
	if !revision.Valid() || revision.issuer != s.recordIssuer {
		return AttemptToken{}, false, ErrInvalidRecordRevision
	}
	if err := validateTypedAttempt(revision.leaseUUID, backendName, operationID); err != nil {
		return AttemptToken{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateAdmissionBaselineLocked(baseline); err != nil {
		return AttemptToken{}, false, err
	}
	if err := s.validateConfiguredBackendLocked(backendName); err != nil {
		return AttemptToken{}, false, err
	}
	if s.restoreSourceClaimedLocked(revision.leaseUUID) {
		return AttemptToken{}, false, fmt.Errorf("%w: lease %q", ErrRestoreSourceClaimed, revision.leaseUUID)
	}
	existing, exists := s.cache[revision.leaseUUID]
	if !exists || existing.revision != revision.value ||
		existing.State() != StateConfirmed || existing.Backend != backendName ||
		existing.Attempt != "" {
		return AttemptToken{}, false, nil
	}
	next, err := s.nextRevision()
	if err != nil {
		return AttemptToken{}, false, err
	}
	existing.Attempt = backendName
	existing.attemptOperationID = operationID
	existing.revision = next
	capability, err := s.lifecycleWithAttemptLocked(
		revision.leaseUUID, backendName, operationID,
	)
	if err != nil {
		return AttemptToken{}, false, err
	}
	if err := s.putPlacementWithLifecycleLocked(
		revision.leaseUUID, existing, capability, "begin exact-owner placement attempt",
	); err != nil {
		return AttemptToken{}, false, err
	}
	s.revision = next
	return s.newAttemptToken(revision.leaseUUID, backendName, operationID, next), true, nil
}
