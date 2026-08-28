package placement

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

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
// exposed only when the store has a current, teardown-only, already-retired,
// or explicitly adopted legacy owner; callback payload metadata never selects
// it.
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
// teardown-only, or explicitly adopted legacy capability. Every other verdict
// returns an empty name.
func (result LifecycleAuthorization) Backend() string {
	if !result.Authorized() && !result.Retired() &&
		result.verdict != LifecycleVerdictLegacy &&
		result.verdict != LifecycleVerdictTeardownOnly {
		return ""
	}
	return result.backend
}

// ID returns the exact typed capability only for a current authorized or
// already-retired lifecycle. Legacy, missing, stale, unusable, and invalid
// results never expose an ID that a caller could mistake for authority.
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
	// unusable is a fail-closed per-lease quarantine. A decoded persisted
	// sentinel may carry a new exact attempt marker, while an undecodable row is
	// represented only in memory so its original bytes remain available for
	// operator diagnosis.
	unusable bool
	// rawCorrupt distinguishes an undecodable durable row from a missing or
	// decodable-but-invalid binding. Inventory may durably replace the latter
	// with an explicit unusable sentinel, but must preserve the former's bytes.
	rawCorrupt bool
}

type persistedLifecycleCapability struct {
	Backend        string `json:"backend,omitempty"`
	ID             string `json:"id,omitempty"`
	Retired        bool   `json:"retired,omitempty"`
	AttemptBackend string `json:"attempt_backend,omitempty"`
	AttemptID      string `json:"attempt_id,omitempty"`
	Unusable       bool   `json:"unusable,omitempty"`
}

// lifecycleInitializationEpoch captures upgrade provenance before NewStore
// creates either post-v0.13 bucket or migrates any placement revision. Legacy
// adoption is safe only when the whole database, not merely one lease, is
// provably crossing that boundary for the first time.
type lifecycleInitializationEpoch struct {
	lifecycleBucketAbsent  bool
	metadataBucketAbsent   bool
	noRevisionedPlacements bool
}

func captureLifecycleInitializationEpoch(
	tx *bolt.Tx,
) (lifecycleInitializationEpoch, error) {
	epoch := lifecycleInitializationEpoch{
		lifecycleBucketAbsent:  tx.Bucket(lifecycleCapabilityBucketName) == nil,
		metadataBucketAbsent:   tx.Bucket(metadataBucketName) == nil,
		noRevisionedPlacements: true,
	}
	placements := tx.Bucket(bucketName)
	if placements == nil {
		return epoch, nil
	}
	if err := placements.ForEach(func(_ []byte, value []byte) error {
		trimmed := bytes.TrimSpace(value)
		if len(trimmed) == 0 || trimmed[0] != '{' {
			// Pre-revision raw backend records cannot carry a revision field.
			return nil
		}
		var header struct {
			Revision uint64 `json:"revision"`
		}
		if decodeErr := json.Unmarshal(trimmed, &header); decodeErr != nil {
			// An unreadable object cannot prove that it lacks a revision, so the
			// entire epoch is ineligible even though that row is also quarantined.
			epoch.noRevisionedPlacements = false
		} else if header.Revision != 0 {
			epoch.noRevisionedPlacements = false
		}
		return nil
	}); err != nil {
		return lifecycleInitializationEpoch{}, fmt.Errorf(
			"scan placement revisions for lifecycle initialization: %w", err,
		)
	}
	return epoch, nil
}

func (epoch lifecycleInitializationEpoch) permitsLegacyAdoption() bool {
	return epoch.lifecycleBucketAbsent && epoch.metadataBucketAbsent &&
		epoch.noRevisionedPlacements
}

func initializeLifecycleCapabilities(
	tx *bolt.Tx,
	epoch lifecycleInitializationEpoch,
) error {
	capabilities := tx.Bucket(lifecycleCapabilityBucketName)
	var err error
	if capabilities == nil {
		capabilities, err = tx.CreateBucketIfNotExists(lifecycleCapabilityBucketName)
	}
	if err != nil {
		return fmt.Errorf("create placement lifecycle capability bucket: %w", err)
	}
	placements := tx.Bucket(bucketName)
	if placements == nil {
		return errors.New("placements bucket missing")
	}

	// Only a first-open v0.13 epoch plus a revision-zero placement proves this
	// database has never issued typed lifecycle authority. If either post-v0.13
	// bucket already existed, or any placement was already revisioned before
	// initialization, every missing row remains fail-closed: recreating it as
	// tokenless could downgrade formerly typed authority after corruption,
	// pruning, downgrade, or bucket deletion.
	if !epoch.permitsLegacyAdoption() {
		return nil
	}
	return placements.ForEach(func(key, value []byte) error {
		placement := decodeRecord(string(key), value)
		if placement.revision != 0 ||
			placement.State() != StateConfirmed || placement.Backend == "" ||
			placement.Attempt != "" || placement.Conflict ||
			capabilities.Get(key) != nil {
			return nil
		}
		encoded, encodeErr := encodeLifecycleCapability(lifecycleCapability{
			backend: placement.Backend,
		})
		if encodeErr != nil {
			return fmt.Errorf("encode adopted legacy lifecycle capability %q: %w", string(key), encodeErr)
		}
		if putErr := capabilities.Put(key, encoded); putErr != nil {
			return fmt.Errorf("write adopted legacy lifecycle capability %q: %w", string(key), putErr)
		}
		return nil
	})
}

// pruneDetachedRetiredLifecycleCapabilities removes completed lifecycle
// history left by older versions. An active capability without a placement is
// outstanding teardown authority and must remain durable until its exact
// terminal observation consumes it. A retired capability that still shares a
// row with an unresolved typed attempt must likewise remain until settlement.
func pruneDetachedRetiredLifecycleCapabilities(tx *bolt.Tx) error {
	placements := tx.Bucket(bucketName)
	capabilities := tx.Bucket(lifecycleCapabilityBucketName)
	if placements == nil || capabilities == nil {
		return errors.New("placement lifecycle buckets missing")
	}

	var prune [][]byte
	if err := capabilities.ForEach(func(key, value []byte) error {
		capability, decodeErr := decodeLifecycleCapability(value)
		if decodeErr == nil && capability.retired && capability.attemptBackend == "" &&
			placements.Get(key) == nil {
			prune = append(prune, append([]byte(nil), key...))
		}
		return nil
	}); err != nil {
		return fmt.Errorf("scan detached retired lifecycle capabilities: %w", err)
	}
	for _, key := range prune {
		if err := capabilities.Delete(key); err != nil {
			return fmt.Errorf("delete detached retired lifecycle capability %q: %w", string(key), err)
		}
	}
	return nil
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
			slog.Warn("placement: loaded lifecycle capability with empty lease UUID")
			return nil
		}
		capability, decodeErr := decodeLifecycleCapability(value)
		if decodeErr != nil {
			slog.Warn("placement: loaded unparseable lifecycle capability",
				"lease_uuid", leaseUUID, "error", decodeErr)
			cache[leaseUUID] = lifecycleCapability{
				unusable:   true,
				rawCorrupt: true,
			}
			return nil
		}
		cache[leaseUUID] = capability
		return nil
	})
	if err != nil {
		return nil, err
	}
	return cache, nil
}

// quarantineLifecycleBindings withdraws lifecycle authority only for leases
// whose two durable records cannot be interpreted together. A bad row must not
// prevent unrelated leases or the provider process from starting. Current
// capabilities may intentionally outlive placement deletion, but every typed
// placement attempt must have the exact marker written in the same transaction.
func quarantineLifecycleBindings(
	placements map[string]Placement,
	capabilities map[string]lifecycleCapability,
) {
	quarantine := func(leaseUUID, reason string) {
		slog.Warn("placement: lifecycle capability is unusable",
			"lease_uuid", leaseUUID, "reason", reason)
		capabilities[leaseUUID] = lifecycleCapability{
			unusable:   true,
			rawCorrupt: capabilities[leaseUUID].rawCorrupt,
		}
	}

	for leaseUUID, placement := range placements {
		capability, exists := capabilities[leaseUUID]
		if !exists {
			quarantine(leaseUUID, "placement has no lifecycle capability")
			continue
		}
		if capability.unusable {
			continue
		}
		// Placement usability is checked before lifecycle authority is exposed.
		// Keep an independently valid capability behind that gate so a future
		// explicit repair path can retain the exact ID. No current reconciler caller
		// supplies such a repair. The binding checks below still quarantine any
		// owner or attempt mismatch.
		if placement.Backend != "" && capability.backend != placement.Backend {
			quarantine(leaseUUID, fmt.Sprintf(
				"capability backend %q does not match placement backend %q",
				capability.backend, placement.Backend,
			))
			continue
		}
		if placement.Attempt == "" {
			if capability.attemptBackend != "" {
				quarantine(leaseUUID, "capability attempt marker has no placement attempt")
			}
			continue
		}
		wantID, err := lifecycleIDForOperation(placement.attemptOperationID)
		if err != nil || capability.attemptBackend != placement.Attempt ||
			capability.attemptID != wantID {
			quarantine(leaseUUID, "capability attempt marker does not match placement attempt")
		}
	}

	for leaseUUID, capability := range capabilities {
		if capability.unusable {
			continue
		}
		_, exists := placements[leaseUUID]
		if !exists {
			if capability.attemptBackend != "" {
				quarantine(leaseUUID, "capability attempt marker has no placement")
			}
			continue
		}
	}
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
		unusable:       persisted.Unusable,
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
		Unusable:       capability.unusable,
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
	if capability.rawCorrupt {
		return errors.New("raw corrupt lifecycle capability cannot be encoded")
	}
	if capability.unusable {
		if capability.backend != "" || capability.id.Valid() || capability.retired {
			return errors.New("unusable lifecycle capability grants current authority")
		}
		if (capability.attemptBackend == "") != !capability.attemptID.Valid() {
			return errors.New("lifecycle attempt marker is incomplete")
		}
		return nil
	}
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

// AuthorizeLifecycle checks one exact typed bearer capability, or the zero-ID
// selector for a provenance-gated legacy owner, without consulting
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
	if s.lifecyclePlacementUnusableLocked(leaseUUID) {
		return LifecycleAuthorization{verdict: LifecycleVerdictUnusable}
	}
	capability, exists := s.lifecycleCache[leaseUUID]
	if !exists {
		return LifecycleAuthorization{verdict: LifecycleVerdictMissing}
	}
	if capability.unusable {
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
// Legacy owners remain explicitly tokenless. Missing, unusable, retired, and
// teardown-only owners grant no new callback route.
func (s *Store) CurrentLifecycle(leaseUUID string) LifecycleAuthorization {
	if leaseUUID == "" {
		return LifecycleAuthorization{verdict: LifecycleVerdictInvalid}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.lifecyclePlacementUnusableLocked(leaseUUID) {
		return LifecycleAuthorization{verdict: LifecycleVerdictUnusable}
	}
	capability, exists := s.lifecycleCache[leaseUUID]
	if !exists {
		return LifecycleAuthorization{verdict: LifecycleVerdictMissing}
	}
	if capability.unusable {
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
	if capability.unusable {
		return LifecycleAuthorization{verdict: LifecycleVerdictUnusable}
	}
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
// write while a placement record still needs the tombstone. If placement was
// already deleted, the exact retirement deletes the capability itself; a
// duplicate then returns Missing. The durable delete is still the at-most-once
// publication boundary.
func (s *Store) RetireLifecycle(
	leaseUUID string,
	id lifecycle.ID,
) (LifecycleAuthorization, error) {
	if leaseUUID == "" {
		return LifecycleAuthorization{verdict: LifecycleVerdictInvalid}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lifecyclePlacementUnusableLocked(leaseUUID) {
		return LifecycleAuthorization{verdict: LifecycleVerdictUnusable}, nil
	}
	capability, exists := s.lifecycleCache[leaseUUID]
	if !exists {
		return LifecycleAuthorization{verdict: LifecycleVerdictMissing}, nil
	}
	if capability.unusable {
		return LifecycleAuthorization{verdict: LifecycleVerdictUnusable}, nil
	}
	result := authorizeLifecycleCapability(capability, id)
	legacy := result.verdict == LifecycleVerdictLegacy && !id.Valid()
	if !result.Authorized() && !legacy {
		return result, nil
	}

	if _, placementExists := s.cache[leaseUUID]; !placementExists {
		if err := s.db.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(lifecycleCapabilityBucketName)
			if bucket == nil {
				return errors.New("placement lifecycle capability bucket missing")
			}
			return bucket.Delete([]byte(leaseUUID))
		}); err != nil {
			return LifecycleAuthorization{}, mutationFailure("retire detached lifecycle capability", err)
		}
		delete(s.lifecycleCache, leaseUUID)
		return LifecycleAuthorization{
			backend: capability.backend,
			id:      capability.id,
			verdict: LifecycleVerdictRetired,
			retired: true,
		}, nil
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
	capability, capabilityExists := s.lifecycleCache[leaseUUID]
	placement := s.cache[leaseUUID]
	if capability.unusable || (!capabilityExists && placement.State() == StateConfirmed) {
		// A new exact operation may replace quarantined authority, but the old
		// owner remains unusable until that operation settles successfully.
		capability = lifecycleCapability{unusable: true}
	}
	capability.attemptBackend = backendName
	capability.attemptID = id
	return capability, nil
}

func promoteAttemptLifecycle(
	backendName string,
	operationID operation.OperationID,
) lifecycleCapability {
	wantID, _ := lifecycleIDForOperation(operationID)
	return lifecycleCapability{backend: backendName, id: wantID}
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
	capability.unusable = false
	capability.rawCorrupt = false
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
	capabilityExists bool,
	existing Placement,
	exists bool,
	backendName string,
) (lifecycleCapability, bool) {
	if exists && existing.Attempt == backendName &&
		existing.attemptOperationID.Valid() {
		wantID, err := lifecycleIDForOperation(existing.attemptOperationID)
		if err == nil && capabilityExists && !capability.unusable &&
			capability.attemptBackend == backendName && capability.attemptID == wantID {
			return promoteAttemptLifecycle(backendName, existing.attemptOperationID), true
		}
		// Inventory is observation-only. A missing or corrupt marker cannot be
		// reconstructed from it, even if the placement still records an operation
		// ID; only an exact operation settlement may establish new authority.
		return quarantineProjectedLifecycle(capability, capabilityExists)
	}
	if capabilityExists && !capability.unusable && capability.backend == backendName {
		// A capability intentionally outlives placement deletion. Rediscovering
		// that same backend must preserve both its typed identity and retirement
		// state; inventory neither delivers a new token nor proves that a retired
		// generation is safe to resurrect. This also prevents a delete/recreate
		// cycle from silently downgrading typed authority to legacy.
		return capability, true
	}
	// Inventory discovered an owner without an exact durable capability it can
	// prove the backend received. Preserve placement routing, but quarantine
	// lifecycle authority instead of manufacturing or downgrading a token.
	return quarantineProjectedLifecycle(capability, capabilityExists)
}

func quarantineProjectedLifecycle(
	capability lifecycleCapability,
	capabilityExists bool,
) (lifecycleCapability, bool) {
	if capabilityExists && capability.rawCorrupt {
		// Keep undecodable bytes intact. The cache remains fail-closed, and every
		// reopen reconstructs the same raw-corruption quarantine.
		return capability, false
	}
	// Missing rows and decodable mismatches have no diagnostic bytes that must
	// survive. Persisting an explicit sentinel prevents a later placement change
	// from making an older decodable capability current again after restart.
	return lifecycleCapability{unusable: true}, true
}

func lifecycleAfterPlacementDelete(
	capability lifecycleCapability,
	placement Placement,
) (lifecycleCapability, bool) {
	capability.attemptBackend = ""
	capability.attemptID = lifecycle.ID{}
	if capability.unusable || capability.retired || placement.State() == StateUnusable {
		// Exact placement deletion may remove fail-closed evidence tied to that
		// placement. A consumed or conflict-withdrawn capability cannot regain
		// authority because later inventory without a capability is quarantined.
		return lifecycleCapability{}, false
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
