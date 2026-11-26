package placement

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// This file holds the v0.13 placement migration, for tests only. A lease that
// existed when a provider upgraded from v0.13 keeps a tokenless lifecycle
// capability, and production still serves such a lease. The migration that
// turned v0.13 rows into that state ran once, inside the 0.14 line's
// placement-preflight, and production no longer has it. It is kept below,
// unchanged, so tests can still build a store that holds adopted leases from
// v0.13 rows. Delete it together with the adopted-lease code.

// adoptV013PlacementsForTest turns a database that holds only v0.13 placement
// rows into the store a 0.14 preparation left behind: bound to providerUUID and
// to the given backends, with every row revisioned and holding a tokenless
// lifecycle capability.
func adoptV013PlacementsForTest(t *testing.T, dbPath, providerUUID string, backends ...string) {
	t.Helper()
	storageIDs := make(map[string]string, len(backends))
	for _, backendName := range backends {
		storageIDs[backendName] = testBackendStorageID(backendName).String()
	}
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return prepareV013Placements(tx, providerUUID, storageIDs)
	}))
	require.NoError(t, db.Close())
}

// prepareV013Placements is the transaction the 0.14 preparation committed.
func prepareV013Placements(tx *bolt.Tx, providerUUID string, storageIDs map[string]string) error {
	epoch, err := captureLifecycleInitializationEpoch(tx)
	if err != nil {
		return err
	}
	if !epoch.permitsLegacyAdoption() {
		return errors.New("database is no longer at the v0.13 migration boundary")
	}
	if tx.Bucket(bucketName) == nil {
		return errors.New("placements bucket is missing")
	}
	if err := initializeMetadata(tx); err != nil {
		return err
	}
	topology := slices.Sorted(maps.Keys(storageIDs))
	fingerprint, err := topologyFingerprint(topology)
	if err != nil {
		return err
	}
	if err := putTopologyMetadata(tx, topologyMetadata{
		Schema:                 topologyMetadataSchema,
		ProviderUUID:           providerUUID,
		Topology:               slices.Clone(topology),
		TopologyFingerprint:    fingerprint,
		KnownBackends:          slices.Clone(topology),
		KnownBackendStorageIDs: storageIDs,
		TopologyID:             1,
	}); err != nil {
		return err
	}
	if err := adoptLegacyLifecycleCapabilities(tx, epoch); err != nil {
		return err
	}
	if err := migrateLegacyConfirmedRevisions(tx); err != nil {
		return err
	}
	return pruneDetachedRetiredLifecycleCapabilities(tx)
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

// adoptLegacyLifecycleCapabilities gives every v0.13 placement row a tokenless
// lifecycle capability, as the 0.14 preparation transaction did.
func adoptLegacyLifecycleCapabilities(
	tx *bolt.Tx,
	epoch lifecycleInitializationEpoch,
) error {
	if err := initializeLifecycleCapabilities(tx); err != nil {
		return err
	}
	capabilities := tx.Bucket(lifecycleCapabilityBucketName)
	placements := tx.Bucket(bucketName)
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
		// v0.13 chose raw-vs-object decoding solely from the first stored
		// byte. Reuse that exact, strict grammar here so printable raw names
		// which are also JSON scalars (for example "null" or "[]") receive
		// the same legacy lifecycle authority proved by preflight.
		backendName, decodeErr := decodeV013PreflightPlacement(value)
		if decodeErr != nil {
			// Invalid v0.13 rows are quarantined lease-locally. They must not
			// prevent valid peers in the same proven legacy epoch from adopting
			// their tokenless lifecycle authority.
			backendName = ""
		}
		if backendName == "" || capabilities.Get(key) != nil {
			return nil
		}
		encoded, encodeErr := encodeLifecycleCapability(lifecycleCapability{
			backend: backendName,
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

// migrateLegacyConfirmedRevisions upgrades the unambiguous confirmed-owner
// records written by v0.13 and earlier onto the revisioned schema. All records
// are examined before any write so new revisions start strictly above every
// existing durable revision. The caller's bbolt update transaction makes the
// entire migration atomic, including revision exhaustion or a failed Put.
//
// Attempts, conflicts, and unusable records deliberately remain byte-for-byte
// untouched. None of them is safe to turn into an exact confirmed-owner
// capability merely because it predates record revisions.
func migrateLegacyConfirmedRevisions(tx *bolt.Tx) error {
	b := tx.Bucket(bucketName)
	if b == nil {
		return errors.New("placements bucket missing")
	}

	type migration struct {
		leaseUUID string
		placement Placement
	}
	var migrations []migration
	var revision uint64
	if err := b.ForEach(func(k, v []byte) error {
		leaseUUID := string(k)
		// v0.13 selected the object decoder only when the first byte was
		// '{'. Consequently printable raw backend names such as "null",
		// "true", "123", and "[]" were valid even though they are also
		// valid non-object JSON roots. The current decoder deliberately rejects
		// those roots, so the one-time migration must reuse the strict historical
		// decoder after the preflight/epoch proof instead of silently leaving a
		// previously confirmed owner unusable. Object rows continue through the
		// current structural decoder so their historical SetAt value is retained.
		p := decodeRecord(leaseUUID, v)
		if _, lost := p.LostBackend(); p.unusable && !lost {
			legacy, legacyErr := decodeV013PlacementForMigration(v)
			if legacyErr == nil {
				p = legacy
			}
		}
		if p.revision > revision {
			revision = p.revision
		}
		if leaseUUID != "" && p.revision == 0 && p.State() == StateConfirmed &&
			p.Attempt == "" && !p.Conflict {
			migrations = append(migrations, migration{
				leaseUUID: leaseUUID,
				placement: p,
			})
		}
		return nil
	}); err != nil {
		return fmt.Errorf("scan legacy placement revisions: %w", err)
	}

	for _, migration := range migrations {
		if revision == math.MaxUint64 {
			return errors.New("placement revision exhausted during legacy migration")
		}
		revision++
		migration.placement.revision = revision
		encoded, err := encodePlacement(migration.placement)
		if err != nil {
			return fmt.Errorf("encode migrated placement %q: %w", migration.leaseUUID, err)
		}
		if err := b.Put([]byte(migration.leaseUUID), encoded); err != nil {
			return fmt.Errorf("write migrated placement %q: %w", migration.leaseUUID, err)
		}
	}
	return nil
}

func validLegacyBackendName(v []byte) bool {
	if !utf8.Valid(v) {
		return false
	}
	for _, r := range string(v) {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

// decodeV013PlacementForMigration is callable only from the stopped,
// database-wide preparation transaction. decodeV013PreflightPlacement owns the
// exact historical raw-vs-object grammar and field allow-list; this helper
// additionally preserves ENG-335's set_at timestamp while minting the current
// schema and revision.
func decodeV013PlacementForMigration(value []byte) (Placement, error) {
	backendName, err := decodeV013PreflightPlacement(value)
	if err != nil {
		return Placement{}, err
	}
	placement := Placement{Backend: backendName}
	if value[0] != '{' {
		return placement, nil
	}
	fields, err := decodeUniqueJSONObject(value)
	if err != nil {
		return Placement{}, err
	}
	if err := json.Unmarshal(fields["set_at"], &placement.SetAt); err != nil {
		return Placement{}, fmt.Errorf("decode v0.13 placement set_at: %w", err)
	}
	return placement, nil
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
