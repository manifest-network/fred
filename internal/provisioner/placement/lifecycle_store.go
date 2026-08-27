package placement

import (
	"encoding/json"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

var lifecycleCapabilityBucketName = []byte("placement_lifecycle_capabilities")

// LifecycleVerdict is the exhaustive result of checking a typed lifecycle
// callback capability. Its zero value grants no authority.
type LifecycleVerdict uint8

const (
	LifecycleVerdictInvalid LifecycleVerdict = iota
	LifecycleVerdictMissing
	LifecycleVerdictLegacy
	LifecycleVerdictStale
	LifecycleVerdictUnusable
	// LifecycleVerdictTeardownOnly means the placement has been deleted but its
	// exact current capability is deliberately retained long enough to consume
	// one delayed terminal deprovision observation. It grants no runtime status
	// publication and cannot be reissued for maintenance.
	LifecycleVerdictTeardownOnly
	LifecycleVerdictAuthorized
	LifecycleVerdictRetired
)

// LifecycleAuthorization is an immutable authorization result. Backend is
// exposed only when the store has a current, teardown-only, or already-retired
// owner; callback payload metadata never selects it.
type LifecycleAuthorization struct {
	backend string
	id      lifecycle.ID
	verdict LifecycleVerdict
	retired bool
}

// Verdict reports the exhaustive authorization outcome.
func (result LifecycleAuthorization) Verdict() LifecycleVerdict { return result.verdict }

// Authorized reports whether the exact capability currently authorizes a
// lifecycle observation.
func (result LifecycleAuthorization) Authorized() bool {
	return result.verdict == LifecycleVerdictAuthorized && result.backend != ""
}

// Retired reports whether this exact capability was already retired. This is
// a terminal, idempotent result rather than renewed mutation authority.
func (result LifecycleAuthorization) Retired() bool {
	return result.verdict == LifecycleVerdictRetired && result.backend != ""
}

// RetiredNow reports that this exact call atomically changed an active
// capability to retired. It is false for an idempotent repeat, allowing a
// caller to publish a best-effort terminal observation at most once even when
// duplicate requests race.
func (result LifecycleAuthorization) RetiredNow() bool {
	return result.Retired() && result.retired
}

// Backend returns the store-authoritative backend for a typed active/retired,
// teardown-only capability or an explicitly migrated legacy owner. Every
// other verdict returns an empty name.
func (result LifecycleAuthorization) Backend() string {
	if !result.Authorized() && !result.Retired() &&
		result.verdict != LifecycleVerdictLegacy &&
		result.verdict != LifecycleVerdictTeardownOnly {
		return ""
	}
	return result.backend
}

// ID returns the exact typed capability only for a current authorized or
// already-retired lifecycle. Legacy, missing, stale, and invalid results never
// expose an ID that a caller could mistake for authority.
func (result LifecycleAuthorization) ID() lifecycle.ID {
	if !result.Authorized() && !result.Retired() {
		return lifecycle.ID{}
	}
	return result.id
}

type lifecycleCapability struct {
	backend        string
	id             lifecycle.ID
	retired        bool
	attemptBackend string
	attemptID      lifecycle.ID
}

type persistedLifecycleCapability struct {
	Backend        string `json:"backend,omitempty"`
	ID             string `json:"id,omitempty"`
	Retired        bool   `json:"retired,omitempty"`
	AttemptBackend string `json:"attempt_backend,omitempty"`
	AttemptID      string `json:"attempt_id,omitempty"`
}

func initializeLifecycleCapabilities(tx *bolt.Tx) error {
	// The bucket's creation is the one-way migration boundary. Once it exists,
	// a missing record is corruption rather than another opportunity to mint a
	// legacy authority record for a placement that may previously have been
	// typed.
	if tx.Bucket(lifecycleCapabilityBucketName) != nil {
		return nil
	}
	capabilities, err := tx.CreateBucketIfNotExists(lifecycleCapabilityBucketName)
	if err != nil {
		return fmt.Errorf("create placement lifecycle capability bucket: %w", err)
	}
	placements := tx.Bucket(bucketName)
	if placements == nil {
		return errors.New("placements bucket missing")
	}

	// A confirmed owner written before lifecycle capabilities existed remains a
	// deliberately legacy, ID-empty owner. Minting an ID here would create a
	// bearer capability that the live backend has never received.
	return placements.ForEach(func(key, value []byte) error {
		if capabilities.Get(key) != nil {
			return nil
		}
		placement := decodeRecord(string(key), value)
		if placement.State() != StateConfirmed || placement.Backend == "" {
			return nil
		}
		encoded, encodeErr := encodeLifecycleCapability(lifecycleCapability{
			backend: placement.Backend,
		})
		if encodeErr != nil {
			return fmt.Errorf("encode legacy lifecycle capability %q: %w", string(key), encodeErr)
		}
		if putErr := capabilities.Put(key, encoded); putErr != nil {
			return fmt.Errorf("write legacy lifecycle capability %q: %w", string(key), putErr)
		}
		return nil
	})
}

func loadLifecycleCapabilities(tx *bolt.Tx) (map[string]lifecycleCapability, error) {
	bucket := tx.Bucket(lifecycleCapabilityBucketName)
	if bucket == nil {
		return nil, errors.New("placement lifecycle capability bucket missing")
	}
	cache := make(map[string]lifecycleCapability)
	err := bucket.ForEach(func(key, value []byte) error {
		leaseUUID := string(key)
		if leaseUUID == "" {
			return errors.New("placement lifecycle capability has empty lease UUID")
		}
		capability, decodeErr := decodeLifecycleCapability(value)
		if decodeErr != nil {
			return fmt.Errorf("decode placement lifecycle capability %q: %w", leaseUUID, decodeErr)
		}
		cache[leaseUUID] = capability
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cache, nil
}

// validateLifecycleBindings rejects durable capability state that could grant
// authority different from the placement state loaded in the same snapshot.
// A current capability may intentionally outlive its placement, and an old
// typed attempt may intentionally have no marker, but a persisted attempt
// marker must always describe the exact durable attempt that created it.
func validateLifecycleBindings(
	placements map[string]Placement,
	capabilities map[string]lifecycleCapability,
) error {
	for leaseUUID, placement := range placements {
		if placement.State() != StateConfirmed || placement.Backend == "" {
			continue
		}
		capability, exists := capabilities[leaseUUID]
		if !exists {
			return fmt.Errorf(
				"confirmed placement %q has no lifecycle capability",
				leaseUUID,
			)
		}
		if capability.backend != placement.Backend {
			return fmt.Errorf(
				"lifecycle capability %q binds backend %q, placement binds %q",
				leaseUUID, capability.backend, placement.Backend,
			)
		}
	}

	for leaseUUID, capability := range capabilities {
		placement, exists := placements[leaseUUID]
		if !exists {
			if capability.attemptBackend != "" {
				return fmt.Errorf(
					"lifecycle capability %q has an attempt marker without a placement",
					leaseUUID,
				)
			}
			continue
		}

		if placement.Backend != "" && capability.backend != placement.Backend {
			return fmt.Errorf(
				"lifecycle capability %q binds backend %q, placement binds %q",
				leaseUUID, capability.backend, placement.Backend,
			)
		}
		if capability.attemptBackend == "" {
			continue
		}
		if placement.Attempt != capability.attemptBackend {
			return fmt.Errorf(
				"lifecycle capability %q binds attempt backend %q, placement binds %q",
				leaseUUID, capability.attemptBackend, placement.Attempt,
			)
		}
		wantID, err := lifecycleIDForOperation(placement.attemptOperationID)
		if err != nil || wantID != capability.attemptID {
			return fmt.Errorf(
				"lifecycle capability %q does not match its placement attempt identity",
				leaseUUID,
			)
		}
	}
	return nil
}

func decodeLifecycleCapability(value []byte) (lifecycleCapability, error) {
	if len(value) == 0 {
		return lifecycleCapability{}, errors.New("empty lifecycle capability")
	}
	var persisted persistedLifecycleCapability
	if err := json.Unmarshal(value, &persisted); err != nil {
		return lifecycleCapability{}, err
	}

	currentID, err := parseOptionalLifecycleID(persisted.ID)
	if err != nil {
		return lifecycleCapability{}, fmt.Errorf("current ID: %w", err)
	}
	attemptID, err := parseOptionalLifecycleID(persisted.AttemptID)
	if err != nil {
		return lifecycleCapability{}, fmt.Errorf("attempt ID: %w", err)
	}
	capability := lifecycleCapability{
		backend:        persisted.Backend,
		id:             currentID,
		retired:        persisted.Retired,
		attemptBackend: persisted.AttemptBackend,
		attemptID:      attemptID,
	}
	if err := validateLifecycleCapability(capability); err != nil {
		return lifecycleCapability{}, err
	}
	return capability, nil
}

func parseOptionalLifecycleID(value string) (lifecycle.ID, error) {
	if value == "" {
		return lifecycle.ID{}, nil
	}
	return lifecycle.ParseID(value)
}

func encodeLifecycleCapability(capability lifecycleCapability) ([]byte, error) {
	if err := validateLifecycleCapability(capability); err != nil {
		return nil, err
	}
	persisted := persistedLifecycleCapability{
		Backend:        capability.backend,
		Retired:        capability.retired,
		AttemptBackend: capability.attemptBackend,
	}
	if capability.id.Valid() {
		persisted.ID = capability.id.String()
	}
	if capability.attemptID.Valid() {
		persisted.AttemptID = capability.attemptID.String()
	}
	return json.Marshal(persisted)
}

func validateLifecycleCapability(capability lifecycleCapability) error {
	if capability.backend == "" && capability.id.Valid() {
		return errors.New("lifecycle capability ID has no backend")
	}
	if capability.retired && capability.backend == "" {
		return errors.New("retired lifecycle capability is incomplete")
	}
	if (capability.attemptBackend == "") != !capability.attemptID.Valid() {
		return errors.New("lifecycle attempt marker is incomplete")
	}
	if capability.backend == "" && capability.attemptBackend == "" {
		return errors.New("lifecycle capability has no current owner or attempt")
	}
	return nil
}

func lifecycleIDForOperation(operationID operation.OperationID) (lifecycle.ID, error) {
	return lifecycle.FromOperationID(operationID)
}

// AuthorizeLifecycle checks one exact typed bearer capability, or the explicit
// zero-ID selector for a migrated legacy owner, without consulting
// payload-supplied backend metadata. Pending attempt markers never grant
// authority.
func (s *Store) AuthorizeLifecycle(
	leaseUUID string,
	id lifecycle.ID,
) LifecycleAuthorization {
	if leaseUUID == "" {
		return LifecycleAuthorization{verdict: LifecycleVerdictInvalid}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	capability, exists := s.lifecycleCache[leaseUUID]
	if !exists {
		return LifecycleAuthorization{verdict: LifecycleVerdictMissing}
	}
	if s.lifecyclePlacementUnusableLocked(leaseUUID) {
		return LifecycleAuthorization{verdict: LifecycleVerdictUnusable}
	}
	result := authorizeLifecycleCapability(capability, id)
	if !s.lifecycleHasRuntimeOwnerLocked(leaseUUID, capability.backend) &&
		(result.Authorized() || result.verdict == LifecycleVerdictLegacy) {
		result.verdict = LifecycleVerdictTeardownOnly
	}
	return result
}

// CurrentLifecycle returns the store-authoritative callback capability for a
// lease without accepting caller-supplied authority. It is used only to place
// the current typed route on trusted Fred-to-backend maintenance requests.
// Legacy owners remain explicitly tokenless. Missing, retired, and
// teardown-only owners grant no new callback route.
func (s *Store) CurrentLifecycle(leaseUUID string) LifecycleAuthorization {
	if leaseUUID == "" {
		return LifecycleAuthorization{verdict: LifecycleVerdictInvalid}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	capability, exists := s.lifecycleCache[leaseUUID]
	if !exists {
		return LifecycleAuthorization{verdict: LifecycleVerdictMissing}
	}
	if s.lifecyclePlacementUnusableLocked(leaseUUID) {
		return LifecycleAuthorization{verdict: LifecycleVerdictUnusable}
	}
	result := authorizeLifecycleCapability(capability, capability.id)
	if !s.lifecycleHasRuntimeOwnerLocked(leaseUUID, capability.backend) &&
		(result.Authorized() || result.verdict == LifecycleVerdictLegacy) {
		result.verdict = LifecycleVerdictTeardownOnly
	}
	return result
}

// lifecyclePlacementUnusableLocked reports whether the placement record has
// deliberately withdrawn single-backend authority. An absent placement is not
// unusable here: lifecycle capability retention across placement deletion is
// what lets a delayed terminal teardown observation retire itself safely.
// Caller holds at least s.mu.RLock.
func (s *Store) lifecyclePlacementUnusableLocked(leaseUUID string) bool {
	current, exists := s.cache[leaseUUID]
	return exists && current.State() == StateUnusable
}

// lifecycleHasRuntimeOwnerLocked reports whether the placement still names
// the capability's exact backend as a confirmed owner. An absent or
// attempt-only placement does not restore runtime observation authority to a
// retained older capability; it remains usable only for terminal teardown.
// Caller holds at least s.mu.RLock.
func (s *Store) lifecycleHasRuntimeOwnerLocked(
	leaseUUID, backendName string,
) bool {
	current, exists := s.cache[leaseUUID]
	return exists && current.State() == StateConfirmed &&
		current.Backend == backendName
}

func authorizeLifecycleCapability(
	capability lifecycleCapability,
	id lifecycle.ID,
) LifecycleAuthorization {
	if capability.backend == "" {
		return LifecycleAuthorization{verdict: LifecycleVerdictMissing}
	}
	if !capability.id.Valid() {
		if capability.retired && !id.Valid() {
			return LifecycleAuthorization{
				backend: capability.backend,
				verdict: LifecycleVerdictRetired,
			}
		}
		if id.Valid() {
			return LifecycleAuthorization{verdict: LifecycleVerdictStale}
		}
		return LifecycleAuthorization{
			backend: capability.backend,
			verdict: LifecycleVerdictLegacy,
		}
	}
	if capability.id != id {
		return LifecycleAuthorization{verdict: LifecycleVerdictStale}
	}
	if capability.retired {
		return LifecycleAuthorization{
			backend: capability.backend,
			id:      capability.id,
			verdict: LifecycleVerdictRetired,
		}
	}
	return LifecycleAuthorization{
		backend: capability.backend,
		id:      capability.id,
		verdict: LifecycleVerdictAuthorized,
	}
}

// RetireLifecycle atomically consumes one exact active capability. Repeating
// retirement with the same ID returns LifecycleVerdictRetired without another
// write. The zero ID consumes only an active legacy capability; it can never
// consume a typed capability, and a typed ID can never consume a legacy one.
func (s *Store) RetireLifecycle(
	leaseUUID string,
	id lifecycle.ID,
) (LifecycleAuthorization, error) {
	if leaseUUID == "" {
		return LifecycleAuthorization{verdict: LifecycleVerdictInvalid}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	capability, exists := s.lifecycleCache[leaseUUID]
	if !exists {
		return LifecycleAuthorization{verdict: LifecycleVerdictMissing}, nil
	}
	if s.lifecyclePlacementUnusableLocked(leaseUUID) {
		return LifecycleAuthorization{verdict: LifecycleVerdictUnusable}, nil
	}
	result := authorizeLifecycleCapability(capability, id)
	legacy := result.verdict == LifecycleVerdictLegacy && !id.Valid()
	if !result.Authorized() && !legacy {
		return result, nil
	}

	capability.retired = true
	encoded, err := encodeLifecycleCapability(capability)
	if err != nil {
		return LifecycleAuthorization{}, mutationFailure("encode retired lifecycle capability", err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(lifecycleCapabilityBucketName)
		if bucket == nil {
			return errors.New("placement lifecycle capability bucket missing")
		}
		return bucket.Put([]byte(leaseUUID), encoded)
	}); err != nil {
		return LifecycleAuthorization{}, mutationFailure("retire lifecycle capability", err)
	}
	s.lifecycleCache[leaseUUID] = capability
	return LifecycleAuthorization{
		backend: capability.backend,
		id:      capability.id,
		verdict: LifecycleVerdictRetired,
		retired: true,
	}, nil
}

func (s *Store) lifecycleWithAttemptLocked(
	leaseUUID, backendName string,
	operationID operation.OperationID,
) (lifecycleCapability, error) {
	id, err := lifecycleIDForOperation(operationID)
	if err != nil {
		return lifecycleCapability{}, err
	}
	capability := s.lifecycleCache[leaseUUID]
	capability.attemptBackend = backendName
	capability.attemptID = id
	return capability, nil
}

func promoteAttemptLifecycle(
	capability lifecycleCapability,
	backendName string,
	operationID operation.OperationID,
) lifecycleCapability {
	wantID, _ := lifecycleIDForOperation(operationID)
	if capability.attemptBackend == backendName && capability.attemptID == wantID {
		capability.backend = backendName
		capability.id = capability.attemptID
		capability.retired = false
	} else if capability.backend != backendName {
		// A pre-capability attempt has no marker to promote. Record the positive
		// owner as legacy rather than minting authority after the fact.
		capability.backend = backendName
		capability.id = lifecycle.ID{}
		capability.retired = false
	}
	capability.attemptBackend = ""
	capability.attemptID = lifecycle.ID{}
	return capability
}

func rotateMaintenanceLifecycle(
	capability lifecycleCapability,
	backendName string,
	operationID operation.OperationID,
) (lifecycleCapability, error) {
	id, err := lifecycleIDForOperation(operationID)
	if err != nil {
		return lifecycleCapability{}, err
	}
	capability.backend = backendName
	capability.id = id
	capability.retired = false
	capability.attemptBackend = ""
	capability.attemptID = lifecycle.ID{}
	return capability, nil
}

func clearAttemptLifecycle(
	capability lifecycleCapability,
	backendName string,
	operationID operation.OperationID,
) lifecycleCapability {
	wantID, _ := lifecycleIDForOperation(operationID)
	if capability.attemptBackend == backendName && capability.attemptID == wantID {
		capability.attemptBackend = ""
		capability.attemptID = lifecycle.ID{}
	}
	return capability
}

func projectPositiveLifecycle(
	capability lifecycleCapability,
	existing Placement,
	exists bool,
	backendName string,
) lifecycleCapability {
	if exists && existing.Attempt == backendName {
		return promoteAttemptLifecycle(
			capability, backendName, existing.attemptOperationID,
		)
	}
	if capability.backend == backendName {
		// A capability intentionally outlives placement deletion. Rediscovering
		// that same backend must preserve both its typed identity and retirement
		// state; inventory neither delivers a new token nor proves that a retired
		// generation is safe to resurrect. This also prevents a delete/recreate
		// cycle from silently downgrading typed authority to legacy.
		return capability
	}
	// Inventory discovered an owner that was not introduced by a typed attempt.
	// Preserve the positive binding but keep it tokenless: the backend never
	// received a lifecycle ID from this store.
	return lifecycleCapability{backend: backendName}
}

func lifecycleAfterPlacementDelete(
	capability lifecycleCapability,
	placement Placement,
) (lifecycleCapability, bool) {
	capability.attemptBackend = ""
	capability.attemptID = lifecycle.ID{}
	if placement.State() == StateUnusable && capability.backend != "" {
		// Deleting a conflict/corruption quarantine must not accidentally make
		// its formerly bound backend authoritative again merely because the
		// placement record is now absent.
		capability.retired = true
	}
	return capability, capability.backend != ""
}

// putPlacementWithLifecycleLocked commits the placement and lifecycle records
// in one bbolt transaction, then updates both caches. Caller holds s.mu.
func (s *Store) putPlacementWithLifecycleLocked(
	leaseUUID string,
	placement Placement,
	capability lifecycleCapability,
	operationName string,
) error {
	placementEncoded, err := encodePlacement(placement)
	if err != nil {
		return mutationFailure("encode placement for "+operationName, err)
	}
	capabilityEncoded, err := encodeLifecycleCapability(capability)
	if err != nil {
		return mutationFailure("encode lifecycle capability for "+operationName, err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		placements := tx.Bucket(bucketName)
		capabilities := tx.Bucket(lifecycleCapabilityBucketName)
		if placements == nil || capabilities == nil {
			return errors.New("placement lifecycle buckets missing")
		}
		if err := placements.Put([]byte(leaseUUID), placementEncoded); err != nil {
			return err
		}
		return capabilities.Put([]byte(leaseUUID), capabilityEncoded)
	}); err != nil {
		return mutationFailure(operationName, err)
	}
	s.cache[leaseUUID] = placement
	s.lifecycleCache[leaseUUID] = capability
	delete(s.deleteRevisions, leaseUUID)
	return nil
}

func (s *Store) putLifecycleLocked(
	leaseUUID string,
	capability lifecycleCapability,
	operationName string,
) error {
	encoded, err := encodeLifecycleCapability(capability)
	if err != nil {
		return mutationFailure("encode lifecycle capability for "+operationName, err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(lifecycleCapabilityBucketName)
		if bucket == nil {
			return errors.New("placement lifecycle capability bucket missing")
		}
		return bucket.Put([]byte(leaseUUID), encoded)
	}); err != nil {
		return mutationFailure(operationName, err)
	}
	s.lifecycleCache[leaseUUID] = capability
	return nil
}
