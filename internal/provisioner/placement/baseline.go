package placement

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/backendname"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

var (
	metadataBucketName = []byte("placement_metadata")
	metadataStateKey   = []byte("topology_state")
)

// Schema 1 existed only during development of this branch and was never
// shipped. Rejecting it avoids treating an incomplete name-only identity model
// as durable authority; the intentionally retained current schema number makes
// any such database fail closed instead of being silently reinterpreted.
const topologyMetadataSchema = 2

var (
	// ErrProviderAuthorityMismatch means a prepared placement database belongs
	// to a different provider (or predates mandatory provider binding). It must
	// never be used as absence or routing authority for the configured provider.
	ErrProviderAuthorityMismatch = errors.New("placement authority provider UUID mismatch")
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
	// ErrBackendStorageIdentityUnbound means a complete fleet observation did
	// not identify every backend in the configured topology.
	ErrBackendStorageIdentityUnbound = errors.New("backend storage identity is unbound")
	// ErrBackendStorageIdentityMismatch means a configured backend name
	// reported storage other than the identity previously bound to that name.
	ErrBackendStorageIdentityMismatch = errors.New("backend storage identity mismatch")
	// ErrBackendStorageIdentityConflict means one storage identity was reported
	// under more than one current or historical backend name.
	ErrBackendStorageIdentityConflict = errors.New("backend storage identity is bound to another backend name")
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

// CompleteBackendObservation is one backend's identity-bearing, concrete
// provision-and-retention observation at a topology-change boundary. Its zero
// value is invalid and its fields are private so callers cannot manufacture an
// "empty" boolean independently of the two inventory slices they received.
//
// This evidence authorizes only later removal of a backend that was observed
// empty. It never establishes an admission baseline.
type CompleteBackendObservation struct {
	storageIdentity backendidentity.ID
	empty           bool
}

// NewCompleteBackendObservation validates and attenuates two concrete complete
// inventory slices to the only facts topology configuration needs: immutable
// storage identity and whether both inventories were empty. Nil is distinct
// from empty because a nil slice does not prove that an endpoint returned a
// concrete collection.
func NewCompleteBackendObservation[P, R any](
	storageIdentity backendidentity.ID,
	provisions []P,
	retentions []R,
) (CompleteBackendObservation, error) {
	if !storageIdentity.Valid() {
		return CompleteBackendObservation{}, ErrBackendStorageIdentityUnbound
	}
	if provisions == nil || retentions == nil {
		return CompleteBackendObservation{}, errors.New("complete backend observation requires concrete provision and retention inventories")
	}
	return CompleteBackendObservation{
		storageIdentity: storageIdentity,
		empty:           len(provisions) == 0 && len(retentions) == 0,
	}, nil
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
	Schema                 uint64            `json:"schema"`
	ProviderUUID           string            `json:"provider_uuid,omitempty"`
	Topology               []string          `json:"topology,omitempty"`
	TopologyFingerprint    string            `json:"topology_fingerprint,omitempty"`
	KnownBackends          []string          `json:"known_backends,omitempty"`
	KnownBackendStorageIDs map[string]string `json:"known_backend_storage_ids,omitempty"`
	TopologyID             uint64            `json:"topology_id,omitempty"`
	BaselineFingerprint    string            `json:"baseline_fingerprint,omitempty"`
	BaselineTopologyID     uint64            `json:"baseline_topology_id,omitempty"`
	// InventoryTopologyID binds EmptyInventoryBackends to the latest complete
	// inventory committed for exactly this topology generation. An explicitly
	// present ID with an empty list means every backend reported something; zero
	// means no complete drain evidence exists. Membership changes clear both.
	InventoryTopologyID    uint64   `json:"inventory_topology_id,omitempty"`
	EmptyInventoryBackends []string `json:"empty_inventory_backends,omitempty"`
}

func emptyTopologyMetadata() topologyMetadata {
	return topologyMetadata{Schema: topologyMetadataSchema}
}

// initializeMetadata creates a schema-bearing metadata bucket for a new or
// pre-baseline database. Once the bucket exists, a missing state record is
// corruption rather than an invitation to forget durable backend identity
// history.
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
	metadata, err := decodeTopologyMetadata(encoded)
	if err != nil {
		return topologyMetadata{}, fmt.Errorf("decode placement metadata: %w", err)
	}
	if err := validateTopologyMetadata(metadata); err != nil {
		return topologyMetadata{}, err
	}
	return metadata, nil
}

// decodeTopologyMetadata treats the single global authority record as a
// security boundary, not as a best-effort configuration document. The normal
// encoding/json struct decoder silently accepts duplicate names, unknown
// fields, and null scalar values; any of those would make the provider,
// topology, or removal evidence ambiguous under corruption or manual edits.
func decodeTopologyMetadata(encoded []byte) (topologyMetadata, error) {
	if !utf8.Valid(encoded) {
		return topologyMetadata{}, errors.New("placement metadata is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	opening, err := decoder.Token()
	if err != nil {
		return topologyMetadata{}, fmt.Errorf("decode JSON object: %w", err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return topologyMetadata{}, errors.New("placement metadata root must be a JSON object")
	}

	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		nameToken, tokenErr := decoder.Token()
		if tokenErr != nil {
			return topologyMetadata{}, fmt.Errorf("decode placement metadata field name: %w", tokenErr)
		}
		name, ok := nameToken.(string)
		if !ok {
			return topologyMetadata{}, errors.New("placement metadata field name is not a string")
		}
		if _, duplicate := fields[name]; duplicate {
			return topologyMetadata{}, fmt.Errorf("duplicate placement metadata field %q", name)
		}
		var raw json.RawMessage
		if decodeErr := decoder.Decode(&raw); decodeErr != nil {
			return topologyMetadata{}, fmt.Errorf(
				"decode placement metadata field %q: %w", name, decodeErr,
			)
		}
		fields[name] = raw
	}
	closing, err := decoder.Token()
	if err != nil {
		return topologyMetadata{}, fmt.Errorf("close placement metadata JSON object: %w", err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return topologyMetadata{}, errors.New("malformed placement metadata JSON object")
	}
	var trailing json.RawMessage
	switch trailingErr := decoder.Decode(&trailing); {
	case errors.Is(trailingErr, io.EOF):
	case trailingErr != nil:
		return topologyMetadata{}, fmt.Errorf("trailing placement metadata data: %w", trailingErr)
	default:
		return topologyMetadata{}, errors.New("trailing JSON value after placement metadata")
	}

	rawSchema, present := fields["schema"]
	if !present {
		return topologyMetadata{}, errors.New(`missing placement metadata field "schema"`)
	}
	if bytes.Equal(bytes.TrimSpace(rawSchema), []byte("null")) {
		return topologyMetadata{}, errors.New(`placement metadata field "schema" must not be null`)
	}
	var schema uint64
	if err := json.Unmarshal(rawSchema, &schema); err != nil {
		return topologyMetadata{}, fmt.Errorf("decode placement metadata schema: %w", err)
	}
	if schema != topologyMetadataSchema {
		return topologyMetadata{}, fmt.Errorf("unsupported placement metadata schema %d", schema)
	}
	for name, raw := range fields {
		if !topologyMetadataFieldAllowed(name) {
			return topologyMetadata{}, fmt.Errorf(
				"unknown placement metadata schema %d field %q", schema, name,
			)
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return topologyMetadata{}, fmt.Errorf(
				"placement metadata field %q must not be null", name,
			)
		}
	}
	if rawStorageIDs, present := fields["known_backend_storage_ids"]; present {
		if err := validateUniqueTopologyStorageIdentityNames(rawStorageIDs); err != nil {
			return topologyMetadata{}, err
		}
	}

	strictDecoder := json.NewDecoder(bytes.NewReader(encoded))
	strictDecoder.DisallowUnknownFields()
	var metadata topologyMetadata
	if err := strictDecoder.Decode(&metadata); err != nil {
		return topologyMetadata{}, fmt.Errorf("decode placement metadata fields: %w", err)
	}
	if err := strictDecoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return topologyMetadata{}, errors.New("trailing JSON value after placement metadata")
		}
		return topologyMetadata{}, fmt.Errorf("trailing placement metadata data: %w", err)
	}
	return metadata, nil
}

func validateUniqueTopologyStorageIdentityNames(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	opening, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode placement storage identity map: %w", err)
	}
	if delimiter, ok := opening.(json.Delim); !ok || delimiter != '{' {
		return errors.New("placement metadata field \"known_backend_storage_ids\" must be a JSON object")
	}
	seen := make(map[string]struct{})
	for decoder.More() {
		nameToken, tokenErr := decoder.Token()
		if tokenErr != nil {
			return fmt.Errorf("decode placement storage identity name: %w", tokenErr)
		}
		name, ok := nameToken.(string)
		if !ok {
			return errors.New("placement storage identity name is not a string")
		}
		if _, duplicate := seen[name]; duplicate {
			return fmt.Errorf("duplicate placement storage identity name %q", name)
		}
		seen[name] = struct{}{}
		var raw json.RawMessage
		if decodeErr := decoder.Decode(&raw); decodeErr != nil {
			return fmt.Errorf("decode placement storage identity %q: %w", name, decodeErr)
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("close placement storage identity map: %w", err)
	}
	if delimiter, ok := closing.(json.Delim); !ok || delimiter != '}' {
		return errors.New("malformed placement storage identity map")
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value after placement storage identity map")
		}
		return fmt.Errorf("trailing placement storage identity map data: %w", err)
	}
	return nil
}

func topologyMetadataFieldAllowed(name string) bool {
	switch name {
	case "schema", "topology", "topology_fingerprint", "known_backends", "topology_id",
		"baseline_fingerprint", "baseline_topology_id", "provider_uuid",
		"known_backend_storage_ids", "inventory_topology_id", "empty_inventory_backends":
		return true
	default:
		return false
	}
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
		if metadata.ProviderUUID != "" && !canonicalLeaseUUID(metadata.ProviderUUID) {
			return fmt.Errorf("malformed placement provider UUID %q", metadata.ProviderUUID)
		}
		if len(metadata.Topology) != 0 || metadata.TopologyFingerprint != "" ||
			len(metadata.KnownBackends) != 0 || len(metadata.KnownBackendStorageIDs) != 0 ||
			metadata.BaselineFingerprint != "" ||
			metadata.BaselineTopologyID != 0 || metadata.InventoryTopologyID != 0 ||
			len(metadata.EmptyInventoryBackends) != 0 {
			return errors.New("malformed unconfigured placement metadata")
		}
		return nil
	}
	if !canonicalLeaseUUID(metadata.ProviderUUID) {
		return fmt.Errorf("malformed placement provider UUID %q", metadata.ProviderUUID)
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
	storageOwners := make(map[backendidentity.ID]string, len(metadata.KnownBackendStorageIDs))
	for backendName, encodedID := range metadata.KnownBackendStorageIDs {
		if _, ok := known[backendName]; !ok {
			return fmt.Errorf("storage identity for unknown backend %q", backendName)
		}
		id, err := backendidentity.Parse(encodedID)
		if err != nil {
			return fmt.Errorf("malformed storage identity for backend %q: %w", backendName, err)
		}
		if owner, duplicate := storageOwners[id]; duplicate && owner != backendName {
			return fmt.Errorf("%w: backends %q and %q", ErrBackendStorageIdentityConflict, owner, backendName)
		}
		storageOwners[id] = backendName
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
	if metadata.InventoryTopologyID != 0 && metadata.InventoryTopologyID != metadata.TopologyID {
		return errors.New("placement empty-inventory evidence does not match current topology")
	}
	if metadata.InventoryTopologyID == 0 && len(metadata.EmptyInventoryBackends) != 0 {
		return errors.New("placement empty-inventory evidence has no topology identity")
	}
	if len(metadata.EmptyInventoryBackends) != 0 {
		if err := validateCanonicalBackendNames(metadata.EmptyInventoryBackends, false); err != nil {
			return fmt.Errorf("malformed empty-inventory backend set: %w", err)
		}
		active := make(map[string]struct{}, len(metadata.Topology))
		for _, backendName := range metadata.Topology {
			active[backendName] = struct{}{}
		}
		for _, backendName := range metadata.EmptyInventoryBackends {
			if _, present := active[backendName]; !present {
				return fmt.Errorf("empty-inventory evidence names inactive backend %q", backendName)
			}
		}
	}
	if metadata.BaselineFingerprint != "" {
		decoded, err := hex.DecodeString(metadata.BaselineFingerprint)
		if err != nil || len(decoded) != sha256.Size ||
			metadata.BaselineFingerprint != strings.ToLower(metadata.BaselineFingerprint) {
			return errors.New("malformed placement admission baseline fingerprint")
		}
		for _, backendName := range metadata.Topology {
			if metadata.KnownBackendStorageIDs[backendName] == "" {
				return fmt.Errorf("%w: %q", ErrBackendStorageIdentityUnbound, backendName)
			}
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
		if err := backendname.Validate(name); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidBackendTopology, err)
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

// VerifyBackendTopology verifies that names are exactly the already-committed
// active topology and that every active backend has a durable storage-identity
// pin. It never changes durable state. Runtime components use this check so a
// standalone Manager or Reconciler cannot bypass the composition root's
// identity-bearing topology probe.
func (s *Store) VerifyBackendTopology(names []string) error {
	canonical, err := canonicalBackendTopology(names)
	if err != nil {
		return err
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if !slices.Equal(canonical, s.backendTopology) {
		return fmt.Errorf(
			"%w: configured topology %v does not match durable topology %v",
			ErrInvalidBackendTopology, canonical, s.backendTopology,
		)
	}
	for _, backendName := range canonical {
		if !s.backendStorageIDs[backendName].Valid() {
			return fmt.Errorf("%w: %q", ErrBackendStorageIdentityUnbound, backendName)
		}
	}
	return nil
}

// VerifyProviderUUID binds this placement authority to the exact provider
// whose chain history authorized its creation or v0.13 preparation.
func (s *Store) VerifyProviderUUID(providerUUID string) error {
	if !canonicalLeaseUUID(providerUUID) {
		return fmt.Errorf("%w: configured provider UUID %q is not canonical",
			ErrProviderAuthorityMismatch, providerUUID)
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.providerUUID != providerUUID {
		return fmt.Errorf(
			"%w: database belongs to %q, configured provider is %q",
			ErrProviderAuthorityMismatch,
			s.providerUUID,
			providerUUID,
		)
	}
	return nil
}

// ConfigureBackendTopologyWithStorageIdentities atomically verifies and pins
// every proposed active name to the storage UUID observed before a topology
// change. This identity-only boundary supports controlled migrations and test
// fixtures; production topology changes use complete observations so the same
// transaction can retain safe rollback evidence.
func (s *Store) ConfigureBackendTopologyWithStorageIdentities(
	names []string,
	identities map[string]backendidentity.ID,
) error {
	return s.configureBackendTopology(names, identities, nil, false)
}

// ConfigureBackendTopologyWithCompleteObservations atomically binds a proposed
// topology to the identities returned by one complete fleet probe and records
// which backends returned two concrete empty inventories. The latter is
// removal-only evidence for this exact new topology generation: admission stays
// disabled until ordinary reconciliation commits its stronger projection.
func (s *Store) ConfigureBackendTopologyWithCompleteObservations(
	names []string,
	observations map[string]CompleteBackendObservation,
) error {
	canonical, err := canonicalBackendTopology(names)
	if err != nil {
		return err
	}
	if len(observations) != len(canonical) {
		return fmt.Errorf(
			"%w: proposed topology observed %d of %d backends",
			ErrBackendStorageIdentityUnbound, len(observations), len(canonical),
		)
	}
	identities := make(map[string]backendidentity.ID, len(canonical))
	emptyBackends := make(map[string]struct{}, len(canonical))
	for _, backendName := range canonical {
		observation, present := observations[backendName]
		if !present || !observation.storageIdentity.Valid() {
			return fmt.Errorf("%w: %q", ErrBackendStorageIdentityUnbound, backendName)
		}
		identities[backendName] = observation.storageIdentity
		if observation.empty {
			emptyBackends[backendName] = struct{}{}
		}
	}
	for backendName := range observations {
		if _, active := identities[backendName]; !active {
			return fmt.Errorf(
				"%w: observation supplied for inactive backend %q",
				ErrInvalidBackendTopology, backendName,
			)
		}
	}
	return s.configureBackendTopology(canonical, identities, emptyBackends, true)
}

func (s *Store) configureBackendTopology(
	names []string,
	identities map[string]backendidentity.ID,
	emptyBackends map[string]struct{},
	completeInventory bool,
) error {
	canonical, err := canonicalBackendTopology(names)
	if err != nil {
		return err
	}
	fingerprint, err := topologyFingerprint(canonical)
	if err != nil {
		return err
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	proposed := make(map[string]struct{}, len(canonical))
	for _, backendName := range canonical {
		proposed[backendName] = struct{}{}
	}
	if len(identities) != len(canonical) {
		return fmt.Errorf(
			"%w: proposed topology identified %d of %d backends",
			ErrBackendStorageIdentityUnbound, len(identities), len(canonical),
		)
	}
	same := slices.Equal(canonical, s.backendTopology)
	metadata := s.topologyMetadataLocked()
	nextStorageIDs := maps.Clone(s.backendStorageIDs)
	if nextStorageIDs == nil {
		nextStorageIDs = make(map[string]backendidentity.ID, len(identities))
	}
	if !same {
		if s.topologyID == ^uint64(0) {
			return errors.New("placement topology identity exhausted")
		}
		metadata.Topology = slices.Clone(canonical)
		metadata.TopologyFingerprint = fingerprint
		metadata.TopologyID = s.topologyID + 1
		metadata.BaselineFingerprint = ""
		metadata.BaselineTopologyID = 0
		metadata.InventoryTopologyID = 0
		metadata.EmptyInventoryBackends = nil
		known := maps.Clone(s.knownBackendNames)
		for backendName := range proposed {
			known[backendName] = struct{}{}
		}
		metadata.KnownBackends = slices.Sorted(maps.Keys(known))
	}
	identitiesChanged := false
	if metadata.KnownBackendStorageIDs == nil {
		metadata.KnownBackendStorageIDs = make(map[string]string, len(identities))
	}
	observedOwners := make(map[backendidentity.ID]string, len(identities))
	for _, backendName := range canonical {
		observed, present := identities[backendName]
		if !present || !observed.Valid() {
			return fmt.Errorf("%w: %q", ErrBackendStorageIdentityUnbound, backendName)
		}
		if expected, bound := s.backendStorageIDs[backendName]; bound && expected != observed {
			return fmt.Errorf(
				"%w: backend %q reported %s, expected %s",
				ErrBackendStorageIdentityMismatch, backendName, observed, expected,
			)
		}
		if owner, duplicate := observedOwners[observed]; duplicate && owner != backendName {
			return fmt.Errorf(
				"%w: backends %q and %q reported %s",
				ErrBackendStorageIdentityConflict, owner, backendName, observed,
			)
		}
		observedOwners[observed] = backendName
		for historicalName, historicalID := range s.backendStorageIDs {
			if historicalName != backendName && historicalID == observed {
				return fmt.Errorf(
					"%w: backend %q reported identity owned by %q",
					ErrBackendStorageIdentityConflict, backendName, historicalName,
				)
			}
		}
		if metadata.KnownBackendStorageIDs[backendName] != observed.String() {
			metadata.KnownBackendStorageIDs[backendName] = observed.String()
			identitiesChanged = true
		}
		nextStorageIDs[backendName] = observed
	}
	for backendName := range identities {
		if _, active := proposed[backendName]; !active {
			return fmt.Errorf("%w: identity supplied for inactive backend %q",
				ErrInvalidBackendTopology, backendName)
		}
	}
	if identitiesChanged {
		metadata.BaselineFingerprint = ""
		metadata.BaselineTopologyID = 0
		metadata.InventoryTopologyID = 0
		metadata.EmptyInventoryBackends = nil
	}
	if completeInventory {
		metadata.InventoryTopologyID = metadata.TopologyID
		metadata.EmptyInventoryBackends = slices.Sorted(maps.Keys(emptyBackends))
	}
	inventoryChanged := metadata.InventoryTopologyID != s.inventoryTopologyID ||
		!slices.Equal(metadata.EmptyInventoryBackends, slices.Sorted(maps.Keys(s.emptyInventoryBackends)))

	if err := s.updateRuntimeAuthority(func(tx *bolt.Tx) error {
		if !same {
			if err := rejectUnsafeBackendRemoval(
				tx,
				proposed,
				s.backendTopology,
				s.inventoryTopologyID,
				s.emptyInventoryBackends,
				s.topologyID,
			); err != nil {
				return err
			}
		}
		if same && !identitiesChanged && !inventoryChanged {
			_, err := loadTopologyMetadata(tx)
			return err
		}
		return putTopologyMetadata(tx, metadata)
	}); err != nil {
		return fmt.Errorf("configure placement backend topology: %w", err)
	}
	if same && !identitiesChanged && !inventoryChanged {
		return nil
	}

	s.backendTopology = slices.Clone(metadata.Topology)
	s.backendTopologySet = proposed
	s.topologyFingerprint = metadata.TopologyFingerprint
	s.topologyID = metadata.TopologyID
	s.baselineFingerprint = metadata.BaselineFingerprint
	s.baselineTopologyID = metadata.BaselineTopologyID
	s.inventoryTopologyID = metadata.InventoryTopologyID
	s.emptyInventoryBackends = make(map[string]struct{}, len(metadata.EmptyInventoryBackends))
	for _, backendName := range metadata.EmptyInventoryBackends {
		s.emptyInventoryBackends[backendName] = struct{}{}
	}
	s.knownBackendNames = make(map[string]struct{}, len(metadata.KnownBackends))
	for _, backendName := range metadata.KnownBackends {
		s.knownBackendNames[backendName] = struct{}{}
	}
	s.backendStorageIDs = nextStorageIDs
	s.advanceAuthorityEpochLocked()
	return nil
}

func rejectUnsafeBackendRemoval(
	tx *bolt.Tx,
	proposed map[string]struct{},
	currentTopology []string,
	inventoryTopologyID uint64,
	emptyInventoryBackends map[string]struct{},
	currentTopologyID uint64,
) error {
	removed := make(map[string]struct{})
	for _, backendName := range currentTopology {
		if _, retained := proposed[backendName]; !retained {
			removed[backendName] = struct{}{}
		}
	}
	if len(removed) != 0 {
		if inventoryTopologyID == 0 || inventoryTopologyID != currentTopologyID {
			return fmt.Errorf(
				"%w: no complete current-topology inventory proves removed backends are empty",
				ErrBackendTopologyInUse,
			)
		}
		for backendName := range removed {
			if _, empty := emptyInventoryBackends[backendName]; !empty {
				return fmt.Errorf(
					"%w: latest complete inventory did not prove backend %q empty",
					ErrBackendTopologyInUse,
					backendName,
				)
			}
		}
	}

	b := tx.Bucket(bucketName)
	if b == nil {
		return errors.New("placements bucket missing")
	}
	if err := b.ForEach(func(key, value []byte) error {
		backendNames, err := durableBackendNames(string(key), value)
		if err != nil {
			return fmt.Errorf("%w: lease %q has uninterpretable durable placement: %w",
				ErrBackendTopologyInUse, string(key), err)
		}
		for _, backendName := range backendNames {
			if _, configured := proposed[backendName]; !configured {
				return fmt.Errorf("%w: lease %q still refers to backend %q",
					ErrBackendTopologyInUse, string(key), backendName)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	capabilities := tx.Bucket(lifecycleCapabilityBucketName)
	if capabilities == nil {
		return errors.New("placement lifecycle capability bucket missing")
	}
	return capabilities.ForEach(func(key, value []byte) error {
		capability, err := decodeLifecycleCapability(value)
		if err != nil {
			return fmt.Errorf(
				"%w: lease %q has uninterpretable durable lifecycle authority: %w",
				ErrBackendTopologyInUse,
				string(key),
				err,
			)
		}
		for _, backendName := range []string{capability.backend, capability.attemptBackend} {
			if backendName == "" {
				continue
			}
			if _, configured := proposed[backendName]; !configured {
				return fmt.Errorf(
					"%w: lease %q still carries lifecycle authority for backend %q",
					ErrBackendTopologyInUse,
					string(key),
					backendName,
				)
			}
		}
		return nil
	})
}

func durableBackendNames(leaseUUID string, value []byte) ([]string, error) {
	if len(value) == 0 {
		return nil, errors.New("empty record")
	}
	persisted := decodeRecord(leaseUUID, value)
	if persisted.unusable {
		return nil, errors.New("record failed placement decoding or structural validation")
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
	storageIDs := make(map[string]string, len(s.backendStorageIDs))
	for backendName, id := range s.backendStorageIDs {
		storageIDs[backendName] = id.String()
	}
	return topologyMetadata{
		Schema:                 topologyMetadataSchema,
		ProviderUUID:           s.providerUUID,
		Topology:               slices.Clone(s.backendTopology),
		TopologyFingerprint:    s.topologyFingerprint,
		KnownBackends:          slices.Sorted(maps.Keys(s.knownBackendNames)),
		KnownBackendStorageIDs: storageIDs,
		TopologyID:             s.topologyID,
		BaselineFingerprint:    s.baselineFingerprint,
		BaselineTopologyID:     s.baselineTopologyID,
		InventoryTopologyID:    s.inventoryTopologyID,
		EmptyInventoryBackends: slices.Sorted(maps.Keys(s.emptyInventoryBackends)),
	}
}

// ExpectedBackendStorageIdentity returns the immutable storage identity
// already bound to backendName. False means the name is not yet bound; it does
// not grant permission to issue side effects.
func (s *Store) ExpectedBackendStorageIdentity(
	backendName string,
) (backendidentity.ID, bool) {
	if err := s.reattestRuntimeAuthority(); err != nil {
		return backendidentity.ID{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, active := s.backendTopologySet[backendName]; !active {
		return backendidentity.ID{}, false
	}
	id, ok := s.backendStorageIDs[backendName]
	return id, ok && id.Valid()
}

// BackendTopologyRequiresIdentityProbe reports whether configuring names would
// change membership or introduce an active name without an immutable storage
// pin. A false result allows startup during a transient backend outage because
// the exact topology/identity baseline is already durable; a true result means
// callers must collect the whole proposed fleet and use
// ConfigureBackendTopologyWithCompleteObservations atomically.
func (s *Store) BackendTopologyRequiresIdentityProbe(names []string) (bool, error) {
	canonical, err := canonicalBackendTopology(names)
	if err != nil {
		return false, err
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
		return false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !slices.Equal(canonical, s.backendTopology) {
		return true, nil
	}
	for _, backendName := range canonical {
		if !s.backendStorageIDs[backendName].Valid() {
			return true, nil
		}
	}
	return false, nil
}

// CurrentAdmissionBaseline returns a capability only when a complete
// projection has durably established the currently configured topology.
func (s *Store) CurrentAdmissionBaseline() AdmissionBaseline {
	if err := s.reattestRuntimeAuthority(); err != nil {
		return AdmissionBaseline{}
	}
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
	if s.runtimeAuthorityFailure() != nil {
		return false
	}
	if s.topologyID == 0 || s.baselineTopologyID != s.topologyID ||
		s.baselineFingerprint == "" || s.baselineFingerprint != s.topologyFingerprint {
		return false
	}
	for _, backendName := range s.backendTopology {
		if !s.backendStorageIDs[backendName].Valid() {
			return false
		}
	}
	return true
}

func (s *Store) validateAdmissionBaselineLocked(baseline AdmissionBaseline) error {
	if err := s.runtimeAuthorityFailure(); err != nil {
		return err
	}
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
	if err := s.reattestRuntimeAuthority(); err != nil {
		return AdmissionScope{}, err
	}

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
	if err := s.runtimeAuthorityFailure(); err != nil {
		return err
	}
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
	for _, backendName := range projection.EmptyBackends {
		if err := validate(backendName); err != nil {
			return err
		}
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
	for _, backendNames := range projection.UntrustedPositives {
		for _, backendName := range backendNames {
			if err := validate(backendName); err != nil {
				return err
			}
		}
	}
	observedOwners := make(map[backendidentity.ID]string, len(projection.BackendStorageIdentities))
	for backendName, observedID := range projection.BackendStorageIdentities {
		if err := validate(backendName); err != nil {
			return err
		}
		if !observedID.Valid() {
			return fmt.Errorf("%w: %q", ErrBackendStorageIdentityUnbound, backendName)
		}
		if expectedID, bound := s.backendStorageIDs[backendName]; bound && expectedID != observedID {
			return fmt.Errorf(
				"%w: backend %q reported %s, expected %s",
				ErrBackendStorageIdentityMismatch, backendName, observedID, expectedID,
			)
		}
		if owner, duplicate := observedOwners[observedID]; duplicate && owner != backendName {
			return fmt.Errorf(
				"%w: backends %q and %q reported %s",
				ErrBackendStorageIdentityConflict, owner, backendName, observedID,
			)
		}
		observedOwners[observedID] = backendName
		for historicalName, historicalID := range s.backendStorageIDs {
			if historicalName != backendName && historicalID == observedID {
				return fmt.Errorf(
					"%w: backend %q reported identity owned by %q",
					ErrBackendStorageIdentityConflict, backendName, historicalName,
				)
			}
		}
	}
	if projection.Complete {
		if len(projection.BackendStorageIdentities) != len(s.backendTopology) {
			return fmt.Errorf(
				"%w: complete projection identified %d of %d backends",
				ErrBackendStorageIdentityUnbound,
				len(projection.BackendStorageIdentities), len(s.backendTopology),
			)
		}
		for _, backendName := range s.backendTopology {
			if !projection.BackendStorageIdentities[backendName].Valid() {
				return fmt.Errorf("%w: %q", ErrBackendStorageIdentityUnbound, backendName)
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
	payloadFingerprint PayloadFingerprint,
	requestSnapshot BackendRequestSnapshot,
	callbackPair CallbackPair,
) (AttemptToken, bool, error) {
	if err := validateTypedAttempt(leaseUUID, backendName, operationID); err != nil {
		return AttemptToken{}, false, err
	}
	if !validAttemptMetadata(
		leaseUUID, operationID, operation.KindProvision, "", payloadFingerprint,
		requestSnapshot, callbackPair,
	) {
		return AttemptToken{}, false, ErrInvalidAttemptToken
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
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
	revision, err := s.setAttemptingLocked(
		leaseUUID, backendName, operationID, operation.KindProvision, "",
		payloadFingerprint, requestSnapshot, callbackPair,
	)
	if err != nil {
		return AttemptToken{}, false, err
	}
	return s.newAttemptToken(
		leaseUUID, backendName, operationID, operation.KindProvision, "",
		payloadFingerprint, requestSnapshot, callbackPair, revision,
	), true, nil
}

// BeginOwnedAttempt records a write-ahead attempt only for the exact confirmed
// owner revision supplied by the caller. Stale, foreign, absent, conflicted, or
// already-attempting records fail the CAS without issuing a capability.
func (s *Store) BeginOwnedAttempt(
	baseline AdmissionBaseline,
	revision RecordRevision,
	backendName string,
	operationID operation.OperationID,
	payloadFingerprint PayloadFingerprint,
	requestSnapshot BackendRequestSnapshot,
	callbackPair CallbackPair,
) (AttemptToken, bool, error) {
	if !revision.Valid() || revision.issuer != s.recordIssuer {
		return AttemptToken{}, false, ErrInvalidRecordRevision
	}
	if err := validateTypedAttempt(revision.leaseUUID, backendName, operationID); err != nil {
		return AttemptToken{}, false, err
	}
	if !validAttemptMetadata(
		revision.leaseUUID, operationID, operation.KindProvision, "",
		payloadFingerprint, requestSnapshot, callbackPair,
	) {
		return AttemptToken{}, false, ErrInvalidAttemptToken
	}
	if err := s.reattestRuntimeAuthority(); err != nil {
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
	existing.attemptOperationKind = operation.KindProvision
	existing.attemptRestoreSourceLeaseUUID = ""
	existing.attemptPayloadFingerprint = payloadFingerprint
	existing.attemptRequestSnapshot = requestSnapshot
	existing.attemptCallbackPair = callbackPair
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
	return s.newAttemptToken(
		revision.leaseUUID, backendName, operationID, operation.KindProvision, "",
		payloadFingerprint, requestSnapshot, callbackPair, next,
	), true, nil
}
