package placement

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/provisioner/operation"
)

func testBackendStorageID(name string) backendidentity.ID {
	digest := sha256.Sum256([]byte("fred-placement-test-storage:" + name))
	digest[6] = (digest[6] & 0x0f) | 0x40
	digest[8] = (digest[8] & 0x3f) | 0x80
	id, err := backendidentity.Parse(fmt.Sprintf("%x-%x-%x-%x-%x",
		digest[0:4], digest[4:6], digest[6:8], digest[8:10], digest[10:16]))
	if err != nil {
		panic(err)
	}
	return id
}

func testBackendStorageIDs(names ...string) map[string]backendidentity.ID {
	identities := make(map[string]backendidentity.ID, len(names))
	for _, name := range names {
		identities[name] = testBackendStorageID(name)
	}
	return identities
}

// testCallbackPair deterministically binds both typed callback authorities to
// one operation. Invalid IDs deliberately produce the safe-zero pair so tests
// can exercise Store validation without manufacturing malformed capabilities.
func testCallbackPair(id operation.OperationID) CallbackPair {
	if !id.Valid() {
		return CallbackPair{}
	}
	pair, err := NewCallbackPair(
		id,
		"https://provider.test/proxy/callbacks/provision?trace=a%2Fb&&z=last&operation_id="+id.String(),
		"https://provider.test/proxy/callbacks/provision?trace=a%2Fb&&z=last&lifecycle_id="+id.String(),
	)
	if err != nil {
		panic(err)
	}
	return pair
}

func testBackendRequestSnapshot(t testing.TB) BackendRequestSnapshot {
	t.Helper()
	snapshot, err := NewBackendRequestSnapshot(
		"tenant-test", "provider-test",
		[]backend.LeaseItem{{SKU: "sku-test", Quantity: 1, ServiceName: "app"}},
	)
	require.NoError(t, err)
	return snapshot
}

func TestPayloadFingerprintRepresentsEverySHA256AndKeepsZeroPayloadless(t *testing.T) {
	assert.False(t, (PayloadFingerprint{}).Valid())
	assert.Nil(t, (PayloadFingerprint{}).Bytes())
	assert.Empty(t, (PayloadFingerprint{}).String())

	allZeroDigest := make([]byte, sha256.Size)
	fingerprint, err := NewPayloadFingerprint(allZeroDigest)
	require.NoError(t, err)
	require.True(t, fingerprint.Valid())
	assert.Equal(t, allZeroDigest, fingerprint.Bytes())
	assert.Equal(t, strings.Repeat("0", sha256.Size*2), fingerprint.String())
	decoded, err := decodePayloadFingerprint(fingerprint.String())
	require.NoError(t, err)
	assert.Equal(t, fingerprint, decoded)

	_, err = NewPayloadFingerprint(nil)
	require.Error(t, err)
	_, err = NewPayloadFingerprint(make([]byte, sha256.Size-1))
	require.Error(t, err)
	_, err = decodePayloadFingerprint(strings.Repeat("A", sha256.Size*2))
	require.Error(t, err, "persisted fingerprints must use one canonical wire form")
}

func TestCallbackPairRejectsMixedOrMismatchedAuthorities(t *testing.T) {
	id := requireOperationID(t, "42")
	other := requireOperationID(t, "43")
	valid := testCallbackPair(id)
	require.True(t, valid.ValidFor(id))
	assert.False(t, valid.ValidFor(other))
	assert.NotEmpty(t, valid.OperationURL())
	assert.NotEmpty(t, valid.LifecycleURL())
	assert.Equal(t,
		"https://provider.test/proxy/callbacks/provision?trace=a%2Fb&&z=last&operation_id="+id.String(),
		valid.OperationURL(),
	)
	assert.Equal(t,
		"https://provider.test/proxy/callbacks/provision?trace=a%2Fb&&z=last&lifecycle_id="+id.String(),
		valid.LifecycleURL(),
		"exact derivation must preserve unrelated raw query bytes and order",
	)

	tests := []struct {
		name         string
		operationURL string
		lifecycleURL string
	}{
		{name: "empty operation URL", lifecycleURL: valid.LifecycleURL()},
		{
			name:         "operation URL carries lifecycle authority",
			operationURL: valid.LifecycleURL(), lifecycleURL: valid.LifecycleURL(),
		},
		{
			name:         "operation URL carries another generation",
			operationURL: testCallbackPair(other).OperationURL(), lifecycleURL: valid.LifecycleURL(),
		},
		{
			name:         "duplicate operation ID",
			operationURL: valid.OperationURL() + "&operation_id=" + id.String(),
			lifecycleURL: valid.LifecycleURL(),
		},
		{
			name:         "lifecycle URL also carries operation authority",
			operationURL: valid.OperationURL(),
			lifecycleURL: valid.LifecycleURL() + "&operation_id=" + id.String(),
		},
		{
			name:         "malformed query",
			operationURL: "https://provider.test/callback?operation_id=%", lifecycleURL: valid.LifecycleURL(),
		},
		{
			name:         "relative operation URL",
			operationURL: "/callback?operation_id=" + id.String(), lifecycleURL: valid.LifecycleURL(),
		},
		{
			name:         "non HTTP operation URL",
			operationURL: "ftp://provider.test/callback?operation_id=" + id.String(),
			lifecycleURL: valid.LifecycleURL(),
		},
		{
			name:         "operation URL with user info",
			operationURL: "https://operator@provider.test/callback?operation_id=" + id.String(),
			lifecycleURL: valid.LifecycleURL(),
		},
		{
			name:         "operation URL with fragment",
			operationURL: valid.OperationURL() + "#not-sent", lifecycleURL: valid.LifecycleURL(),
		},
		{
			name:         "operation URL with empty fragment marker",
			operationURL: valid.OperationURL() + "#", lifecycleURL: valid.LifecycleURL(),
		},
		{
			name:         "lifecycle URL with fragment",
			operationURL: valid.OperationURL(), lifecycleURL: valid.LifecycleURL() + "#not-sent",
		},
		{
			name:         "lifecycle URL changes host",
			operationURL: valid.OperationURL(),
			lifecycleURL: strings.Replace(valid.LifecycleURL(), "provider.test", "other.test", 1),
		},
		{
			name:         "lifecycle URL changes path",
			operationURL: valid.OperationURL(),
			lifecycleURL: strings.Replace(valid.LifecycleURL(), "/proxy/", "/other/", 1),
		},
		{
			name:         "lifecycle URL changes unrelated query",
			operationURL: valid.OperationURL(),
			lifecycleURL: strings.Replace(valid.LifecycleURL(), "trace=a%2Fb", "trace=changed", 1),
		},
		{
			name:         "lifecycle URL reorders unrelated query",
			operationURL: valid.OperationURL(),
			lifecycleURL: "https://provider.test/proxy/callbacks/provision?z=last&&trace=a%2Fb&lifecycle_id=" + id.String(),
		},
		{
			name:         "operation URL targets arbitrary path",
			operationURL: strings.Replace(valid.OperationURL(), "/callbacks/provision", "/arbitrary", 1),
			lifecycleURL: strings.Replace(valid.LifecycleURL(), "/callbacks/provision", "/arbitrary", 1),
		},
		{
			name:         "operation URL with port-only host",
			operationURL: "https://:443/callback?operation_id=" + id.String(),
			lifecycleURL: valid.LifecycleURL(),
		},
		{
			name:         "lifecycle URL with invalid port",
			operationURL: valid.OperationURL(),
			lifecycleURL: "https://provider.test:65536/callback?lifecycle_id=" + id.String(),
		},
		{
			name:         "operation URL with unstable raw query byte",
			operationURL: strings.Replace(valid.OperationURL(), "?", "?trace=a b&", 1),
			lifecycleURL: valid.LifecycleURL(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pair, err := NewCallbackPair(id, test.operationURL, test.lifecycleURL)
			require.Error(t, err)
			assert.False(t, pair.ValidFor(id))
		})
	}
}

func beginTestRestore(
	t *testing.T,
	s *Store,
	baseline AdmissionBaseline,
	sourceLeaseUUID, targetLeaseUUID string,
	id operation.OperationID,
) (RestoreClaim, error) {
	t.Helper()
	return s.beginRestore(
		baseline, sourceLeaseUUID, targetLeaseUUID, id,
		testBackendRequestSnapshot(t), testCallbackPair(id),
	)
}

// newStore is deliberately test-only fixture machinery. It can construct the
// pre-preparation and corrupt database shapes exercised below; production has
// no create-on-open or migrate-on-open entry point.
func newStore(
	dbPath string,
	allowUnpreparedLegacyForTest bool,
	opts ...Option,
) (*Store, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("placement db path is required")
	}

	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open placement test fixture: %w", err)
	}
	var preparationRequired bool
	if err := db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(metadataBucketName) != nil {
			return nil
		}
		if lifecycleBucket := tx.Bucket(lifecycleCapabilityBucketName); lifecycleBucket != nil {
			preparationRequired = true
			return nil
		}
		placements := tx.Bucket(bucketName)
		if placements == nil {
			return nil
		}
		key, _ := placements.Cursor().First()
		preparationRequired = key != nil
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("inspect placement test fixture: %w", err)
	}
	if preparationRequired && !allowUnpreparedLegacyForTest {
		_ = db.Close()
		return nil, fmt.Errorf("%w: test fixture is unprepared", ErrPreparationRequired)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		initializationEpoch, epochErr := captureLifecycleInitializationEpoch(tx)
		if epochErr != nil {
			return epochErr
		}
		if _, createErr := tx.CreateBucketIfNotExists(bucketName); createErr != nil {
			return createErr
		}
		if metadataErr := initializeMetadata(tx); metadataErr != nil {
			return metadataErr
		}
		metadata, metadataErr := loadTopologyMetadata(tx)
		if metadataErr != nil {
			return metadataErr
		}
		if metadata.ProviderUUID == "" {
			metadata.ProviderUUID = freshTestProviderUUID
			if metadataErr = putTopologyMetadata(tx, metadata); metadataErr != nil {
				return metadataErr
			}
		}
		if lifecycleErr := initializeLifecycleCapabilities(tx, initializationEpoch); lifecycleErr != nil {
			return lifecycleErr
		}
		if migrationErr := migrateLegacyConfirmedRevisions(tx); migrationErr != nil {
			return migrationErr
		}
		return pruneDetachedRetiredLifecycleCapabilities(tx)
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize placement test fixture: %w", err)
	}

	return loadStore(db, opts...)
}

func newStoreForTest(dbPath string, opts ...Option) (*Store, error) {
	return newStore(dbPath, false, opts...)
}

func configureBackendTopologyForTest(s *Store, names []string) error {
	return s.ConfigureBackendTopologyWithStorageIdentities(
		names, testBackendStorageIDs(names...),
	)
}

func newTestStore(t *testing.T, opts ...Option) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := newStoreForTest(dbPath, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestOpenStoreIsExistingPreparedAndReadOnlyAtStartup(t *testing.T) {
	t.Run("missing path is not created", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing.db")
		_, err := OpenStore(path, freshTestProviderUUID)
		require.ErrorIs(t, err, os.ErrNotExist)
		_, statErr := os.Stat(path)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("empty file is not initialized", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "empty.db")
		require.NoError(t, os.WriteFile(path, nil, 0o600))
		_, err := OpenStore(path, freshTestProviderUUID)
		require.ErrorIs(t, err, ErrPreparationRequired)
		info, statErr := os.Stat(path)
		require.NoError(t, statErr)
		assert.Zero(t, info.Size())
	})

	t.Run("legacy authority requires offline preparation without mutation", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "legacy.db")
		db, err := bolt.Open(path, 0o600, nil)
		require.NoError(t, err)
		require.NoError(t, db.Update(func(tx *bolt.Tx) error {
			bucket, createErr := tx.CreateBucket(bucketName)
			if createErr != nil {
				return createErr
			}
			return bucket.Put([]byte("lease"), []byte("backend-a"))
		}))
		require.NoError(t, db.Close())
		before, err := os.ReadFile(path)
		require.NoError(t, err)

		_, err = OpenStore(path, freshTestProviderUUID)
		require.ErrorIs(t, err, ErrPreparationRequired)
		after, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		assert.Equal(t, before, after)
	})

	t.Run("prepared authority opens without changing bytes", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "prepared.db")
		require.NoError(t, InitializeFreshStoreContext(
			t.Context(), freshTestPlan(t, path, []string{"backend-a"}),
		))
		before, err := os.ReadFile(path)
		require.NoError(t, err)

		opened, err := OpenStore(path, freshTestProviderUUID)
		require.NoError(t, err)
		require.NoError(t, opened.Close())
		after, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, before, after)
	})

	t.Run("ambiguous prepared metadata is rejected without changing bytes", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "prepared-corrupt.db")
		replaceTopologyMetadataForTest(t, path, []byte(`{"schema":2,"schema":1}`))
		before, err := os.ReadFile(path)
		require.NoError(t, err)

		opened, err := OpenStore(path, freshTestProviderUUID)
		require.ErrorContains(t, err, `duplicate placement metadata field "schema"`)
		assert.Nil(t, opened)
		after, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		assert.Equal(t, before, after,
			"failed production open must not normalize ambiguous authority metadata")
	})
}

func replaceTopologyMetadataForTest(t *testing.T, dbPath string, encoded []byte) {
	t.Helper()
	require.NoError(t, InitializeFreshStoreContext(
		t.Context(), freshTestPlan(t, dbPath, []string{"backend-a"}),
	))
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(metadataBucketName).Put(metadataStateKey, encoded)
	}))
	require.NoError(t, db.Close())
}

func requireOperationID(t *testing.T, value string) operation.OperationID {
	t.Helper()
	sequence, err := strconv.ParseUint(value, 10, 64)
	require.NoError(t, err)
	id, err := testOperationID(sequence)
	require.NoError(t, err)
	return id
}

func requireAdmissionBaseline(
	t *testing.T,
	s *Store,
	backendNames ...string,
) AdmissionBaseline {
	t.Helper()
	require.NoError(t, s.ConfigureBackendTopologyWithStorageIdentities(
		backendNames, testBackendStorageIDs(backendNames...),
	))
	fence := s.BeginInventorySession()
	_, err := s.ProjectInventory(fence, InventoryProjection{
		Complete:                 true,
		BackendStorageIdentities: testBackendStorageIDs(backendNames...),
		EmptyBackends:            append([]string(nil), backendNames...),
	})
	s.EndInventorySession(fence)
	require.NoError(t, err)
	baseline := s.CurrentAdmissionBaseline()
	require.True(t, baseline.Valid())
	return baseline
}

func requireTestAdmission(t *testing.T, s *Store) {
	t.Helper()
	if s.CurrentAdmissionBaseline().Valid() {
		return
	}
	s.mu.RLock()
	names := append([]string(nil), s.backendTopology...)
	s.mu.RUnlock()
	seen := make(map[string]struct{}, len(names)+2)
	for _, name := range names {
		seen[name] = struct{}{}
	}
	for _, current := range s.List() {
		for _, name := range append(
			[]string{current.Backend, current.Attempt},
			current.ConflictBackends...,
		) {
			if name != "" {
				seen[name] = struct{}{}
			}
		}
	}
	seen["backend-a"] = struct{}{}
	seen["backend-b"] = struct{}{}
	names = names[:0]
	for name := range seen {
		names = append(names, name)
	}
	requireAdmissionBaseline(t, s, names...)
}

func invalidateTestAdmission(t *testing.T, s *Store) {
	t.Helper()
	s.mu.RLock()
	names := append([]string(nil), s.backendTopology...)
	nextID := s.topologyID + 1
	s.mu.RUnlock()
	seen := make(map[string]struct{}, len(names)+3)
	for _, name := range names {
		seen[name] = struct{}{}
	}
	seen["backend-a"] = struct{}{}
	seen["backend-b"] = struct{}{}
	seen[fmt.Sprintf("test-topology-%d", nextID)] = struct{}{}
	names = names[:0]
	for name := range seen {
		names = append(names, name)
	}
	require.NoError(t, configureBackendTopologyForTest(s, names))
	require.False(t, s.CurrentAdmissionBaseline().Valid())
}

func testOperationID(sequence uint64) (operation.OperationID, error) {
	return operation.ParseID(fmt.Sprintf("00000000-0000-4000-8000-%012x", sequence))
}

func testRevision(s *Store) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revision
}

func projectInventoryForTest(
	t *testing.T,
	s *Store,
	projection InventoryProjection,
) ProjectionResult {
	t.Helper()
	if projection.Complete && projection.BackendStorageIdentities == nil {
		s.mu.RLock()
		projection.BackendStorageIdentities = testBackendStorageIDs(s.backendTopology...)
		s.mu.RUnlock()
	}
	if projection.Complete && projection.EmptyBackends == nil {
		projection.EmptyBackends = emptyBackendsForTest(s.backendTopology, projection)
	}
	fence := s.BeginInventorySession()
	result, err := s.ProjectInventory(fence, projection)
	s.EndInventorySession(fence)
	require.NoError(t, err)
	return result
}

func emptyBackendsForTest(topology []string, projection InventoryProjection) []string {
	nonempty := make(map[string]struct{})
	for _, backendName := range projection.Placements {
		nonempty[backendName] = struct{}{}
	}
	for _, backendNames := range projection.Conflicts {
		for _, backendName := range backendNames {
			nonempty[backendName] = struct{}{}
		}
	}
	for _, backendNames := range projection.UntrustedPositives {
		for _, backendName := range backendNames {
			nonempty[backendName] = struct{}{}
		}
	}
	empty := make([]string, 0, len(topology))
	for _, backendName := range topology {
		if _, present := nonempty[backendName]; !present {
			empty = append(empty, backendName)
		}
	}
	return empty
}

func requireConfirmedPlacement(t *testing.T, s *Store, leaseUUID, backendName string) Placement {
	t.Helper()
	projectInventoryForTest(t, s, InventoryProjection{
		Placements: map[string]string{leaseUUID: backendName},
	})
	current := s.Lookup(leaseUUID)
	require.Equal(t, StateConfirmed, current.State())
	require.Equal(t, backendName, current.Backend)
	return current

}

func requireConflictPlacement(t *testing.T, s *Store, leaseUUID string, backendNames ...string) Placement {
	t.Helper()
	projectInventoryForTest(t, s, InventoryProjection{
		Conflicts: map[string][]string{leaseUUID: backendNames},
	})
	current := s.Lookup(leaseUUID)
	require.True(t, current.Conflict)
	return current
}

func requireTypedAttempt(
	t *testing.T,
	s *Store,
	leaseUUID, backendName string,
	operationID operation.OperationID,
) AttemptToken {
	t.Helper()
	if !s.CurrentAdmissionBaseline().Valid() {
		requireTestAdmission(t, s)
	}
	baseline := s.CurrentAdmissionBaseline()
	current := s.Lookup(leaseUUID)
	var token AttemptToken
	var applied bool
	var err error
	switch current.State() {
	case StateAbsent:
		scope, scopeErr := s.ScopeAdmission(baseline, []string{backendName})
		require.NoError(t, scopeErr)
		token, applied, err = s.BeginNewAttempt(
			scope, leaseUUID, backendName, operationID, PayloadFingerprint{},
			testBackendRequestSnapshot(t), testCallbackPair(operationID),
		)
	case StateConfirmed:
		token, applied, err = s.BeginOwnedAttempt(
			baseline, current.RecordRevision(), backendName, operationID,
			PayloadFingerprint{}, testBackendRequestSnapshot(t), testCallbackPair(operationID))

	default:
		require.FailNow(t, "placement cannot begin a typed test attempt", "state=%s", current.State())
	}
	require.NoError(t, err)
	require.True(t, applied)
	require.True(t, token.Valid())
	return token
}

func requireDeleteRecord(t *testing.T, s *Store, leaseUUID string) {
	t.Helper()
	revision := s.Lookup(leaseUUID).RecordRevision()
	require.True(t, revision.Valid())
	deleted, err := s.DeleteRecord(revision)
	require.NoError(t, err)
	require.True(t, deleted)
}

func writeRawRecords(t *testing.T, dbPath string, records map[string][]byte) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		for key, value := range records {
			if err := b.Put([]byte(key), value); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())
}

func readRawRecords(t *testing.T, dbPath string, leaseUUIDs ...string) map[string][]byte {
	t.Helper()
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)

	records := make(map[string][]byte, len(leaseUUIDs))
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return errors.New("placements bucket missing")
		}
		for _, leaseUUID := range leaseUUIDs {
			records[leaseUUID] = append([]byte(nil), b.Get([]byte(leaseUUID))...)
		}
		return nil
	}))
	require.NoError(t, db.Close())
	return records
}

func TestPlacement_StateAndString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		placement Placement
		want      State
		wantText  string
	}{
		{name: "zero is absent", want: StateAbsent, wantText: "absent"},
		{
			name:      "attempt without owner is attempting",
			placement: Placement{Attempt: "backend-a"},
			want:      StateAttempting,
			wantText:  "attempting",
		},
		{
			name:      "backend is confirmed",
			placement: Placement{Backend: "backend-a"},
			want:      StateConfirmed,
			wantText:  "confirmed",
		},
		{
			name:      "confirmed wins when an attempt coexists",
			placement: Placement{Backend: "backend-a", Attempt: "backend-a"},
			want:      StateConfirmed,
			wantText:  "confirmed",
		},
		{
			name:      "timestamp without a fact is unusable",
			placement: Placement{SetAt: time.Unix(1, 0)},
			want:      StateUnusable,
			wantText:  "unusable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.placement.State())
			assert.Equal(t, tt.wantText, tt.placement.State().String())
		})
	}
	assert.Equal(t, "State(99)", State(99).String())
}

func TestStore_TypedAttemptSettlementRequiresExactToken(t *testing.T) {
	s := newTestStore(t)
	requireTestAdmission(t, s)

	confirmed, err := s.ConfirmAttempt(AttemptToken{})
	require.ErrorIs(t, err, ErrInvalidAttemptToken)
	assert.False(t, confirmed)
	refused, err := s.RefuseAttempt(AttemptToken{})
	require.ErrorIs(t, err, ErrInvalidAttemptToken)
	assert.False(t, refused)

	first := requireTypedAttempt(t, s,
		"lease-1", "backend-a", requireOperationID(t, "201"))
	refused, err = s.RefuseAttempt(first)
	require.NoError(t, err)
	assert.True(t, refused)

	secondID := requireOperationID(t, "202")
	second := requireTypedAttempt(t, s, "lease-1", "backend-a", secondID)
	confirmed, err = s.ConfirmAttempt(first)
	require.NoError(t, err)
	assert.False(t, confirmed, "a stale token must not settle a later attempt")
	assert.Equal(t, secondID, s.Lookup("lease-1").AttemptOperationID())

	other := newTestStore(t)
	requireTestAdmission(t, other)
	foreign := requireTypedAttempt(t, other,
		"lease-1", "backend-a", requireOperationID(t, "203"))
	confirmed, err = s.ConfirmAttempt(foreign)
	require.ErrorIs(t, err, ErrInvalidAttemptToken)
	assert.False(t, confirmed, "a token from another store is not a capability here")
	assert.Equal(t, secondID, s.Lookup("lease-1").AttemptOperationID())

	confirmed, err = s.ConfirmAttempt(second)
	require.NoError(t, err)
	assert.True(t, confirmed)
	p := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, p.State())
	assert.Equal(t, "backend-a", p.Backend)
	assert.False(t, p.AttemptOperationID().Valid())
	confirmed, err = s.ConfirmAttempt(second)
	require.NoError(t, err)
	assert.False(t, confirmed, "a consumed token is stale")

	third := requireTypedAttempt(t, s,
		"lease-1", "backend-a", requireOperationID(t, "204"))
	refused, err = s.RefuseAttempt(third)
	require.NoError(t, err)
	assert.True(t, refused)
	p = s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, p.State(),
		"refusing a new action must retain previously confirmed affinity")
	assert.False(t, p.AttemptOperationID().Valid())
}

func TestStore_CallbackOperationSettlementRequiresPersistedIdentity(t *testing.T) {
	s := newTestStore(t)
	requireTestAdmission(t, s)
	firstID := requireOperationID(t, "271")
	secondID := requireOperationID(t, "272")

	requireTypedAttempt(t, s, "lease-1", "backend-a", firstID)
	var err error
	settled, err := s.ConfirmOperation("lease-1", "backend-a", secondID)
	require.NoError(t, err)
	assert.False(t, settled)
	settled, err = s.RefuseOperation("lease-1", "backend-a", secondID)
	require.NoError(t, err)
	assert.False(t, settled)
	assert.Equal(t, firstID, s.Lookup("lease-1").AttemptOperationID())

	settled, err = s.RefuseOperation("lease-1", "backend-a", firstID)
	require.NoError(t, err)
	assert.True(t, settled)
	requireTypedAttempt(t, s, "lease-1", "backend-a", secondID)
	settled, err = s.ConfirmOperation("lease-1", "backend-a", firstID)
	require.NoError(t, err)
	assert.False(t, settled,
		"an older same-backend operation cannot settle the current attempt")
	assert.Equal(t, secondID, s.Lookup("lease-1").AttemptOperationID())

	settled, err = s.ConfirmOperation("lease-1", "backend-a", secondID)
	require.NoError(t, err)
	assert.True(t, settled)
	confirmed := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, confirmed.State())
	assert.False(t, confirmed.AttemptOperationID().Valid())
	beforeRevision := confirmed.RecordRevision()
	settled, err = s.ConfirmOperation("lease-1", "backend-a", firstID)
	require.NoError(t, err)
	assert.False(t, settled,
		"an older same-backend operation cannot rotate a confirmed lifecycle generation")
	assert.Equal(t, beforeRevision, s.Lookup("lease-1").RecordRevision(),
		"a stale operation must not mutate the record")
	settled, err = s.ConfirmOperation("lease-1", "backend-a", secondID)
	require.NoError(t, err)
	assert.True(t, settled, "the exact current generation is idempotent")

	requireTypedAttempt(t, s,
		"lease-1", "backend-a", requireOperationID(t, "273"))
	settled, err = s.RefuseOperation(
		"lease-1", "backend-a", requireOperationID(t, "273"),
	)
	require.NoError(t, err)
	assert.True(t, settled)
	assert.Equal(t, StateConfirmed, s.Lookup("lease-1").State(),
		"a refusal must preserve previously confirmed ownership")
}

func TestStore_CallbackOperationSettlementRejectsInvalidAndLegacyAttempts(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		"legacy-attempt": []byte(`{"attempt":"backend-a","set_at":"2026-08-25T15:00:00Z","revision":1}`),
	})
	s, err := newStore(dbPath, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	settled, err := s.ConfirmOperation(
		"lease-1", "backend-a", operation.OperationID{},
	)
	require.ErrorIs(t, err, operation.ErrInvalidID)
	assert.False(t, settled)
	settled, err = s.RefuseOperation(
		"lease-1", "backend-a", operation.OperationID{},
	)
	require.ErrorIs(t, err, operation.ErrInvalidID)
	assert.False(t, settled)

	legacy := s.Lookup("legacy-attempt")
	assert.Equal(t, StateUnusable, legacy.State())
	assert.False(t, legacy.AttemptOperationID().Valid())
	validID := requireOperationID(t, "281")
	settled, err = s.ConfirmOperation("legacy-attempt", "backend-a", validID)
	require.ErrorIs(t, err, ErrUnusablePlacement)
	assert.False(t, settled)
	settled, err = s.RefuseOperation("legacy-attempt", "backend-a", validID)
	require.NoError(t, err)
	assert.False(t, settled)
	assert.Equal(t, legacy, s.Lookup("legacy-attempt"))
}

func TestStore_CallbackOperationSettlementSurvivesReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	opID := requireOperationID(t, "291")
	s1, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	requireTestAdmission(t, s1)
	requireTypedAttempt(t, s1, "lease-1", "backend-a", opID)
	require.NoError(t, s1.Close())

	s2, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	defer s2.Close()
	settled, err := s2.ConfirmOperation("lease-1", "backend-a", opID)
	require.NoError(t, err)
	assert.True(t, settled)
	assert.Equal(t, StateConfirmed, s2.Lookup("lease-1").State())
	assert.False(t, s2.Lookup("lease-1").AttemptOperationID().Valid())
}

func TestStore_DurableAttemptClaimSurvivesReopenAndFencesInventory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	operationID := requireOperationID(t, "292")
	staleID := requireOperationID(t, "293")
	s1, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	requireTestAdmission(t, s1)
	oldProcessToken := requireTypedAttempt(t, s1, "lease-1", "backend-a", operationID)
	require.NoError(t, s1.Close())

	s2, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	claim, claimed, err := s2.ClaimAttempt("lease-1", staleID)
	require.NoError(t, err)
	assert.False(t, claimed)
	assert.False(t, claim.Valid(), "a stale operation must receive no recovery authority")

	claim, claimed, err = s2.ClaimAttempt("lease-1", operationID)
	require.NoError(t, err)
	require.True(t, claimed)
	require.True(t, claim.Valid())
	assert.Equal(t, "backend-a", claim.Backend())
	confirmed, err := s2.ConfirmAttempt(oldProcessToken)
	require.ErrorIs(t, err, ErrInvalidAttemptToken)
	assert.False(t, confirmed,
		"the recovery claim cannot make a token from the closed store valid")
	live := s2.Lookup("lease-1")
	liveToken := s2.newAttemptToken(
		"lease-1", "backend-a", operationID, operation.KindProvision, "",
		PayloadFingerprint{}, testBackendRequestSnapshot(t),
		testCallbackPair(operationID), live.Revision(),
	)
	confirmed, err = s2.ConfirmAttempt(liveToken)
	require.ErrorIs(t, err, ErrAttemptClaimed)
	assert.False(t, confirmed, "the live recovery claim is exclusive across store mutators")

	_, claimed, err = s2.ClaimAttempt("lease-1", operationID)
	require.ErrorIs(t, err, ErrAttemptClaimed)
	assert.False(t, claimed, "claim contention must remain retryable")

	// The inventory snapshot predates callback recovery. Applying its positive
	// observation while the claim is live must not promote the attempt out from
	// under a failure callback's chain settlement.
	fence := s2.BeginInventorySession()
	result, err := s2.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
	})
	s2.EndInventorySession(fence)
	require.NoError(t, err)
	assert.Contains(t, result.Fenced, "lease-1")
	assert.Equal(t, StateAttempting, s2.Lookup("lease-1").State())

	refused, err := s2.RefuseClaimedAttempt(claim)
	require.NoError(t, err)
	require.True(t, refused)
	assert.Equal(t, StateAbsent, s2.Lookup("lease-1").State())
	assert.False(t, s2.ReleaseAttemptClaim(claim), "a settled claim is consumed")
}

func TestStore_DurableAttemptClaimPreservesNegativeInventoryAndRejectsStaleABA(t *testing.T) {
	s := newTestStore(t)
	requireTestAdmission(t, s)
	firstID := requireOperationID(t, "294")
	secondID := requireOperationID(t, "295")
	requireTypedAttempt(t, s, "lease-1", "backend-a", firstID)

	first, claimed, err := s.ClaimAttempt("lease-1", firstID)
	require.NoError(t, err)
	require.True(t, claimed)
	projectInventoryForTest(t, s, InventoryProjection{Complete: true})
	assert.Equal(t, firstID, s.Lookup("lease-1").AttemptOperationID(),
		"inventory silence cannot clear an ambiguously dispatched attempt")
	require.True(t, s.ReleaseAttemptClaim(first))

	refused, err := s.RefuseOperation("lease-1", "backend-a", firstID)
	require.NoError(t, err)
	require.True(t, refused)
	requireTypedAttempt(t, s, "lease-1", "backend-a", secondID)

	_, claimed, err = s.ClaimAttempt("lease-1", firstID)
	require.NoError(t, err)
	assert.False(t, claimed, "an older same-backend callback cannot claim a newer attempt")
	second, claimed, err := s.ClaimAttempt("lease-1", secondID)
	require.NoError(t, err)
	require.True(t, claimed)
	confirmed, err := s.ConfirmClaimedAttempt(second)
	require.NoError(t, err)
	require.True(t, confirmed)
	assert.Equal(t, StateConfirmed, s.Lookup("lease-1").State())
}

func TestStore_DurableAttemptClaimRejectsConflictingPersistedOwner(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	operationID := requireOperationID(t, "296")
	writeRawRecords(t, dbPath, map[string][]byte{
		"lease-1": []byte(fmt.Sprintf(
			`{"backend":"backend-a","attempt":"backend-b","operation_id":%q,"operation_kind":"provision","callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-31T00:00:00Z","revision":7}`,
			operationID.String(), testCallbackPair(operationID).OperationURL(),
			testCallbackPair(operationID).LifecycleURL(),
		)),
	})
	s, err := newStore(dbPath, true)
	require.NoError(t, err)
	defer s.Close()

	claim, claimed, err := s.ClaimAttempt("lease-1", operationID)
	require.NoError(t, err)
	assert.False(t, claimed)
	assert.False(t, claim.Valid(),
		"a callback cannot resolve contradictory persisted ownership by choosing the attempt")
}

func TestStore_DurableAttemptClaimCoversInventoryConfirmedGeneration(t *testing.T) {
	s := newTestStore(t)
	requireTestAdmission(t, s)
	operationID := requireOperationID(t, "297")
	staleID := requireOperationID(t, "298")
	requireTypedAttempt(t, s, "lease-1", "backend-a", operationID)

	projectInventoryForTest(t, s, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
		Lifecycles: map[string]LifecycleObservation{
			"lease-1": {
				Kind: LifecycleObservationTyped,
				ID:   lifecycleIDFromOperation(t, operationID),
			},
		},
	})
	confirmed := s.Lookup("lease-1")
	require.Equal(t, StateConfirmed, confirmed.State())
	require.Empty(t, confirmed.Attempt)

	_, claimed, err := s.ClaimAttempt("lease-1", staleID)
	require.NoError(t, err)
	assert.False(t, claimed, "an older generation receives no confirmed authority")

	claim, claimed, err := s.ClaimAttempt("lease-1", operationID)
	require.NoError(t, err)
	require.True(t, claimed)
	assert.True(t, claim.HasSameBackendOwner())
	assert.Equal(t, "backend-a", claim.Backend())

	// Even an idempotent observation is fenced while chain acknowledgement for
	// this exact confirmed generation is in progress.
	fence := s.BeginInventorySession()
	projection, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
	})
	s.EndInventorySession(fence)
	require.NoError(t, err)
	assert.Contains(t, projection.Fenced, "lease-1")

	settled, err := s.RefuseClaimedAttempt(claim)
	require.NoError(t, err)
	require.True(t, settled)
	assert.Equal(t, confirmed, s.Lookup("lease-1"),
		"a failure for an inventory-confirmed generation must not demote its owner")
	assert.False(t, s.ReleaseAttemptClaim(claim), "settlement consumes the exact claim")
}

func TestStore_DeleteRecordRequiresExactTypedRevision(t *testing.T) {
	s := newTestStore(t)
	requireConfirmedPlacement(t, s, "lease-1", "backend-a")
	requireConfirmedPlacement(t, s, "lease-2", "backend-a")
	lease1 := s.Lookup("lease-1")
	lease2 := s.Lookup("lease-2")
	require.True(t, lease1.RecordRevision().Valid())
	require.True(t, lease2.RecordRevision().Valid())

	deleted, err := s.DeleteRecord(RecordRevision{})
	require.ErrorIs(t, err, ErrInvalidRecordRevision)
	assert.False(t, deleted)

	other := newTestStore(t)
	requireConfirmedPlacement(t, other, "lease-1", "backend-a")
	deleted, err = s.DeleteRecord(other.Lookup("lease-1").RecordRevision())
	require.ErrorIs(t, err, ErrInvalidRecordRevision)
	assert.False(t, deleted)

	deleted, err = s.DeleteRecord(lease2.RecordRevision())
	require.NoError(t, err)
	assert.True(t, deleted, "the capability derives its own lease target")
	assert.Equal(t, lease1, s.Lookup("lease-1"))
	assert.Equal(t, StateAbsent, s.Lookup("lease-2").State())

	deleted, err = s.DeleteRecord(lease1.RecordRevision())
	require.NoError(t, err)
	assert.True(t, deleted)
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())
	deleted, err = s.DeleteRecord(lease1.RecordRevision())
	require.NoError(t, err)
	assert.False(t, deleted)
}

func TestStore_TypedAttemptAndDurableBaselinePersistAcrossReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	opID := requireOperationID(t, "301")
	s1, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	requireTestAdmission(t, s1)
	token := requireTypedAttempt(t, s1, "lease-1", "backend-a", opID)
	wantRevision := s1.Lookup("lease-1").RecordRevision()
	require.True(t, wantRevision.Valid())
	require.NoError(t, s1.Close())
	var encoded []byte
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		encoded = append(encoded, tx.Bucket(bucketName).Get([]byte("lease-1"))...)
		return nil
	}))
	require.NoError(t, db.Close())
	assert.Contains(t, string(encoded), `"operation_id":"00000000-0000-4000-8000-00000000012d"`,
		"the durable operation identity is a canonical UUID string")

	s2, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	defer s2.Close()
	p := s2.Lookup("lease-1")
	assert.Equal(t, StateAttempting, p.State())
	assert.Equal(t, "backend-a", p.Attempt)
	assert.Equal(t, opID, p.AttemptOperationID())
	assert.Equal(t, wantRevision.value, p.RecordRevision().value,
		"the durable record version survives restart")
	assert.NotEqual(t, wantRevision, p.RecordRevision(),
		"a reopened store mints a new process-local capability")
	deleted, err := s2.DeleteRecord(wantRevision)
	require.ErrorIs(t, err, ErrInvalidRecordRevision)
	assert.False(t, deleted, "a revision from the closed issuer is foreign")

	newToken := requireTypedAttempt(t, s2,
		"lease-2", "backend-a", requireOperationID(t, "302"))
	assert.True(t, newToken.Valid(),
		"the topology-matching durable baseline remains authoritative after restart")
	confirmed, err := s2.ConfirmAttempt(token)
	require.ErrorIs(t, err, ErrInvalidAttemptToken,
		"process-local capabilities are not transferable to a reopened store")
	assert.False(t, confirmed)
}

func TestStore_TypedAttemptUpgradesV013ConfirmedRecord(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		"lease-legacy": []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})

	s, err := newStore(dbPath, true)
	require.NoError(t, err)
	legacy := s.Lookup("lease-legacy")
	assert.Equal(t, StateConfirmed, legacy.State())
	assert.True(t, legacy.RecordRevision().Valid(),
		"opening the store must migrate an unambiguous v0.13 owner")
	assert.NotZero(t, legacy.Revision())
	assert.False(t, legacy.AttemptOperationID().Valid())

	opID := requireOperationID(t, "401")
	_, applied, err := s.BeginOwnedAttempt(
		AdmissionBaseline{}, legacy.RecordRevision(), "backend-a", opID,
		PayloadFingerprint{}, testBackendRequestSnapshot(t), testCallbackPair(opID))

	require.ErrorIs(t, err, ErrInvalidAdmissionBaseline)
	assert.False(t, applied)
	requireTestAdmission(t, s)
	requireTypedAttempt(t, s, "lease-legacy", "backend-a", opID)
	upgraded := s.Lookup("lease-legacy")
	assert.True(t, upgraded.RecordRevision().Valid())
	assert.Equal(t, opID, upgraded.AttemptOperationID())
	require.NoError(t, s.Close())

	reopened, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	defer reopened.Close()
	upgraded = reopened.Lookup("lease-legacy")
	assert.Equal(t, StateConfirmed, upgraded.State())
	assert.Equal(t, "backend-a", upgraded.Attempt)
	assert.Equal(t, opID, upgraded.AttemptOperationID())
}

func TestDecodeRecord_StrictAttemptOperationMetadata(t *testing.T) {
	const operationID = "123e4567-e89b-42d3-a456-426614174000"
	const callbackMetadata = `,"callback_url":"https://provider.test/callbacks/provision?operation_id=` + operationID +
		`","lifecycle_callback_url":"https://provider.test/callbacks/provision?lifecycle_id=` + operationID + `"`
	const requestMetadata = `,"tenant":"tenant-test","provider_uuid":"provider-test",` +
		`"request_items":[{"sku":"sku-test","quantity":1,"service_name":"app"}]`
	tests := []struct {
		name       string
		leaseUUID  string
		record     string
		wantState  State
		wantKind   operation.Kind
		wantSource string
	}{
		{
			name:      "complete provision",
			leaseUUID: "target",
			record: `{"attempt":"backend-a","operation_id":"` + operationID +
				`","operation_kind":"provision"` + requestMetadata + callbackMetadata +
				`,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
			wantState: StateAttempting,
			wantKind:  operation.KindProvision,
		},
		{
			name:      "complete restore",
			leaseUUID: "target",
			record: `{"attempt":"backend-a","operation_id":"` + operationID +
				`","operation_kind":"restore","restore_source_lease_uuid":"source"` + requestMetadata + callbackMetadata +
				`,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
			wantState:  StateAttempting,
			wantKind:   operation.KindRestore,
			wantSource: "source",
		},
		{
			name:      "attempt missing all operation metadata",
			leaseUUID: "target",
			record:    `{"attempt":"backend-a","set_at":"2026-08-25T15:00:00Z","revision":1}`,
			wantState: StateUnusable,
		},
		{
			name:      "operation ID without kind",
			leaseUUID: "target",
			record: `{"attempt":"backend-a","operation_id":"` + operationID +
				`"` + callbackMetadata + `,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
			wantState: StateUnusable,
		},
		{
			name:      "unknown kind",
			leaseUUID: "target",
			record: `{"attempt":"backend-a","operation_id":"` + operationID +
				`","operation_kind":"restart"` + callbackMetadata +
				`,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
			wantState: StateUnusable,
		},
		{
			name:      "restore missing source",
			leaseUUID: "target",
			record: `{"attempt":"backend-a","operation_id":"` + operationID +
				`","operation_kind":"restore"` + callbackMetadata +
				`,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
			wantState: StateUnusable,
		},
		{
			name:      "restore source equals target",
			leaseUUID: "target",
			record: `{"attempt":"backend-a","operation_id":"` + operationID +
				`","operation_kind":"restore","restore_source_lease_uuid":"target"` + callbackMetadata +
				`,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
			wantState: StateUnusable,
		},
		{
			name:      "provision carries source",
			leaseUUID: "target",
			record: `{"attempt":"backend-a","operation_id":"` + operationID +
				`","operation_kind":"provision","restore_source_lease_uuid":"source"` + callbackMetadata +
				`,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
			wantState: StateUnusable,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoded := decodeRecord(test.leaseUUID, []byte(test.record))
			assert.Equal(t, test.wantState, decoded.State())
			assert.Equal(t, test.wantKind, decoded.attemptOperationKind)
			assert.Equal(t, test.wantSource, decoded.attemptRestoreSourceLeaseUUID)
		})
	}
}

func TestDecodeRecord_StrictExactRequestMetadata(t *testing.T) {
	id := requireOperationID(t, "450")
	pair := testCallbackPair(id)
	allZeroHash := strings.Repeat("0", sha256.Size*2)
	const requestMetadata = `,"tenant":"tenant-test","provider_uuid":"provider-test",` +
		`"request_items":[{"sku":"sku-test","quantity":1,"service_name":"app"}]`

	valid := decodeRecord("target", []byte(fmt.Sprintf(
		`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision","payload_hash":%q`+requestMetadata+`,"callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
		id.String(), allZeroHash, pair.OperationURL(), pair.LifecycleURL(),
	)))
	require.Equal(t, StateAttempting, valid.State())
	require.True(t, valid.attemptPayloadFingerprint.Valid())
	assert.Equal(t, allZeroHash, valid.attemptPayloadFingerprint.String())

	other := requireOperationID(t, "451")
	for _, test := range []struct {
		name  string
		value string
	}{
		{
			name: "typed attempt missing exact callback pair",
			value: fmt.Sprintf(
				`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision"`+requestMetadata+`,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
				id.String(),
			),
		},
		{
			name: "typed attempt missing exact request snapshot",
			value: fmt.Sprintf(
				`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision","callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
				id.String(), pair.OperationURL(), pair.LifecycleURL(),
			),
		},
		{
			name: "request item contains duplicate field",
			value: fmt.Sprintf(
				`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision","tenant":"tenant-test","provider_uuid":"provider-test","request_items":[{"sku":"sku-test","sku":"sku-other","quantity":1,"service_name":"app"}],"callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
				id.String(), pair.OperationURL(), pair.LifecycleURL(),
			),
		},
		{
			name: "request item contains unknown field",
			value: fmt.Sprintf(
				`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision","tenant":"tenant-test","provider_uuid":"provider-test","request_items":[{"sku":"sku-test","quantity":1,"service_name":"app","surprise":true}],"callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
				id.String(), pair.OperationURL(), pair.LifecycleURL(),
			),
		},
		{
			name: "request item snapshot is noncanonical",
			value: fmt.Sprintf(
				`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision","tenant":"tenant-test","provider_uuid":"provider-test","request_items":[ {"sku":"sku-test","quantity":1,"service_name":"app"}],"callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
				id.String(), pair.OperationURL(), pair.LifecycleURL(),
			),
		},
		{
			name: "request item aggregate exceeds backend boundary",
			value: fmt.Sprintf(
				`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision","tenant":"tenant-test","provider_uuid":"provider-test","request_items":[{"sku":"sku-a","quantity":1024},{"sku":"sku-b","quantity":1}],"callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
				id.String(), pair.OperationURL(), pair.LifecycleURL(),
			),
		},
		{
			name: "operation callback names another generation",
			value: fmt.Sprintf(
				`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision"`+requestMetadata+`,"callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
				id.String(), testCallbackPair(other).OperationURL(), pair.LifecycleURL(),
			),
		},
		{
			name: "callback destinations are not the exact derived pair",
			value: fmt.Sprintf(
				`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision"`+requestMetadata+`,"callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
				id.String(), pair.OperationURL(), strings.Replace(pair.LifecycleURL(), "trace=a%2Fb", "trace=changed", 1),
			),
		},
		{
			name: "callback destinations target arbitrary path",
			value: fmt.Sprintf(
				`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision"`+requestMetadata+`,"callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
				id.String(),
				strings.Replace(pair.OperationURL(), "/callbacks/provision", "/arbitrary", 1),
				strings.Replace(pair.LifecycleURL(), "/callbacks/provision", "/arbitrary", 1),
			),
		},
		{
			name: "persisted operation callback has port-only host",
			value: fmt.Sprintf(
				`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision"`+requestMetadata+`,"callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
				id.String(), "https://:443/callback?operation_id="+id.String(), pair.LifecycleURL(),
			),
		},
		{
			name: "persisted lifecycle callback has invalid port",
			value: fmt.Sprintf(
				`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision"`+requestMetadata+`,"callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
				id.String(), pair.OperationURL(), "https://provider.test:65536/callback?lifecycle_id="+id.String(),
			),
		},
		{
			name: "persisted callback has empty fragment marker",
			value: fmt.Sprintf(
				`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision"`+requestMetadata+`,"callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
				id.String(), pair.OperationURL()+"#", pair.LifecycleURL(),
			),
		},
		{
			name: "persisted callback has unstable raw query byte",
			value: fmt.Sprintf(
				`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision"`+requestMetadata+`,"callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
				id.String(), strings.Replace(pair.OperationURL(), "?", "?trace=a b&", 1), pair.LifecycleURL(),
			),
		},
		{
			name: "restore carries provision payload fingerprint",
			value: fmt.Sprintf(
				`{"attempt":"backend-a","operation_id":%q,"operation_kind":"restore","restore_source_lease_uuid":"source","payload_hash":%q`+requestMetadata+`,"callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
				id.String(), allZeroHash, pair.OperationURL(), pair.LifecycleURL(),
			),
		},
		{
			name: "uppercase fingerprint is noncanonical",
			value: fmt.Sprintf(
				`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision","payload_hash":%q`+requestMetadata+`,"callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
				id.String(), strings.Repeat("A", sha256.Size*2), pair.OperationURL(), pair.LifecycleURL(),
			),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, StateUnusable, decodeRecord("target", []byte(test.value)).State())
		})
	}

	invalidUTF8Record := fmt.Sprintf(
		`{"attempt":"backend-a","operation_id":%q,"operation_kind":"provision"`+requestMetadata+`,"callback_url":%q,"lifecycle_callback_url":%q,"set_at":"2026-08-25T15:00:00Z","revision":1}`,
		id.String(), pair.OperationURL(), pair.LifecycleURL(),
	)
	invalidUTF8Bytes := []byte(invalidUTF8Record)
	invalidUTF8Bytes[strings.Index(invalidUTF8Record, "sku-test")] = 0xff
	assert.Equal(t, StateUnusable, decodeRecord("target", invalidUTF8Bytes).State(),
		"invalid nested UTF-8 must not be normalized into a different durable request")
}

func TestBackendRequestSnapshotIsValidatedDetachedAndComparable(t *testing.T) {
	var zero BackendRequestSnapshot
	assert.False(t, zero.Valid())
	assert.Empty(t, zero.Tenant())
	assert.Empty(t, zero.ProviderUUID())
	assert.Nil(t, zero.Items())

	items := []backend.LeaseItem{{
		SKU: "sku-a", Quantity: 2, ServiceName: "web",
		CustomDomain: "original.example.test",
	}}
	snapshot, err := NewBackendRequestSnapshot("tenant-a", "provider-a", items)
	require.NoError(t, err)
	require.True(t, snapshot.Valid())
	assert.Equal(t, "tenant-a", snapshot.Tenant())
	assert.Equal(t, "provider-a", snapshot.ProviderUUID())
	comparable := map[BackendRequestSnapshot]struct{}{snapshot: {}}
	_, found := comparable[snapshot]
	assert.True(t, found,
		"the private canonical representation must remain comparable for CAS tokens")

	items[0].CustomDomain = "mutated-input.example.test"
	detached := snapshot.Items()
	require.Len(t, detached, 1)
	assert.Equal(t, "original.example.test", detached[0].CustomDomain)
	detached[0].CustomDomain = "mutated-output.example.test"
	assert.Equal(t, "original.example.test", snapshot.Items()[0].CustomDomain)
	for _, test := range []struct {
		name     string
		tenant   string
		provider string
		items    []backend.LeaseItem
	}{
		{name: "missing tenant", provider: "provider-a", items: items},
		{name: "blank tenant", tenant: " ", provider: "provider-a", items: items},
		{name: "missing provider", tenant: "tenant-a", items: items},
		{name: "blank provider", tenant: "tenant-a", provider: "\t", items: items},
		{name: "missing items", tenant: "tenant-a", provider: "provider-a"},
		{name: "missing SKU", tenant: "tenant-a", provider: "provider-a", items: []backend.LeaseItem{{Quantity: 1}}},
		{name: "zero quantity", tenant: "tenant-a", provider: "provider-a", items: []backend.LeaseItem{{SKU: "sku-a"}}},
		{name: "negative quantity", tenant: "tenant-a", provider: "provider-a", items: []backend.LeaseItem{{SKU: "sku-a", Quantity: -1}}},
		{name: "max int quantity", tenant: "tenant-a", provider: "provider-a", items: []backend.LeaseItem{{SKU: "sku-a", Quantity: math.MaxInt}}},
		{name: "aggregate quantity", tenant: "tenant-a", provider: "provider-a", items: []backend.LeaseItem{
			{SKU: "sku-a", Quantity: backend.MaxOperationQuantity},
			{SKU: "sku-b", Quantity: 1},
		}},
		{name: "invalid UTF-8 tenant", tenant: string([]byte{0xff}), provider: "provider-a", items: items},
		{name: "invalid UTF-8 provider", tenant: "tenant-a", provider: string([]byte{0xff}), items: items},
		{name: "invalid UTF-8 SKU", tenant: "tenant-a", provider: "provider-a", items: []backend.LeaseItem{{SKU: string([]byte{0xff}), Quantity: 1}}},
		{name: "invalid UTF-8 service", tenant: "tenant-a", provider: "provider-a", items: []backend.LeaseItem{{SKU: "sku-a", Quantity: 1, ServiceName: string([]byte{0xff})}}},
		{name: "invalid UTF-8 domain", tenant: "tenant-a", provider: "provider-a", items: []backend.LeaseItem{{SKU: "sku-a", Quantity: 1, CustomDomain: string([]byte{0xff})}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid, err := NewBackendRequestSnapshot(test.tenant, test.provider, test.items)
			require.Error(t, err)
			assert.False(t, invalid.Valid())
		})
	}
}

func TestStore_ConfirmedOperationMetadataSurvivesReopenForCallbackRecovery(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	store1, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	requireTestAdmission(t, store1)
	operationID := requireOperationID(t, "402")
	attempt := requireTypedAttempt(t, store1, "lease-1", "backend-a", operationID)
	confirmed, err := store1.ConfirmAttempt(attempt)
	require.NoError(t, err)
	require.True(t, confirmed)
	require.NoError(t, store1.Close())

	store2, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	defer store2.Close()
	claim, claimed, err := store2.ClaimAttempt("lease-1", operationID)
	require.NoError(t, err)
	require.True(t, claimed)
	require.True(t, claim.Valid())
	assert.Equal(t, operation.KindProvision, claim.Metadata().Kind())
	assert.Equal(t, operationID, claim.Metadata().OperationID())
	assert.Empty(t, claim.Metadata().RestoreSourceLeaseUUID())
	assert.True(t, store2.ReleaseAttemptClaim(claim))
}

func TestStore_ProjectInventoryAcceptsExplicitRevisionZeroFence(t *testing.T) {
	s := newTestStore(t)
	fence := s.BeginInventorySession()
	defer s.EndInventorySession(fence)
	require.True(t, fence.Valid())

	result, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
	})
	require.NoError(t, err)
	revision := s.Lookup("lease-1").RecordRevision()
	assert.True(t, revision.Valid())
	assert.Empty(t, result.Fenced)
}

func TestStore_ProjectInventoryUpgradesIdempotentV013RecordRevision(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		"lease-legacy": []byte(`{"backend":"backend-a","set_at":"2026-08-25T15:00:00Z"}`),
	})
	s, err := newStore(dbPath, true)
	require.NoError(t, err)
	revision := s.Lookup("lease-legacy").RecordRevision()
	require.True(t, revision.Valid(),
		"opening the store must make legacy ownership immediately usable by typed CAS")
	fence := s.BeginInventorySession()

	_, err = s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"lease-legacy": "backend-a"},
		Lifecycles: map[string]LifecycleObservation{
			"lease-legacy": {Kind: LifecycleObservationLegacy},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, revision, s.Lookup("lease-legacy").RecordRevision())
	s.EndInventorySession(fence)
	require.NoError(t, s.Close())

	reopened, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	defer reopened.Close()
	reopenedRevision := reopened.Lookup("lease-legacy").RecordRevision()
	assert.Equal(t, revision.value, reopenedRevision.value)
	assert.NotEqual(t, revision, reopenedRevision,
		"record capabilities are rebound to the reopened store")
}

func TestStore_ProjectInventoryAppliesPositiveAndConflictOutcomesAtomically(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := newStoreForTest(dbPath)
	require.NoError(t, err)

	requireAdmissionBaseline(t, s,
		"backend-a", "backend-b", "backend-attempted", "backend-possible",
		"backend-positive", "backend-replacement",
	)
	requireTypedAttempt(t, s, "mismatched", "backend-attempted", requireOperationID(t, "481"))
	requireTypedAttempt(t, s, "conflict", "backend-possible", requireOperationID(t, "482"))
	fence := s.BeginInventorySession()

	result, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{
			"positive":   "backend-positive",
			"mismatched": "backend-replacement",
		},
		Conflicts: map[string][]string{
			"conflict": {"backend-a", "backend-b"},
		},
	})
	require.NoError(t, err)
	s.EndInventorySession(fence)
	assert.Empty(t, result.Fenced)
	for _, leaseUUID := range []string{"positive", "mismatched", "conflict"} {
		revision := s.Lookup(leaseUUID).RecordRevision()
		assert.True(t, revision.Valid(), leaseUUID)
	}

	positive := s.Lookup("positive")
	assert.Equal(t, StateConfirmed, positive.State())
	assert.Equal(t, "backend-positive", positive.Backend)
	mismatched := s.Lookup("mismatched")
	assert.Equal(t, StateConfirmed, mismatched.State())
	assert.Equal(t, "backend-replacement", mismatched.Backend)
	assert.Equal(t, "backend-attempted", mismatched.Attempt,
		"an observation from another backend cannot disprove the pending attempt")
	assert.True(t, mismatched.AttemptOperationID().Valid(),
		"an observation from another backend must preserve the typed pending operation identity")
	conflict := s.Lookup("conflict")
	assert.Equal(t, StateUnusable, conflict.State())
	assert.ElementsMatch(t,
		[]string{"backend-a", "backend-b", "backend-possible"},
		conflict.ConflictBackends,
	)
	require.NoError(t, s.Close())

	reopened, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	defer reopened.Close()
	assert.Equal(t, "backend-positive", reopened.Lookup("positive").Backend)
	assert.Equal(t, "backend-replacement", reopened.Lookup("mismatched").Backend)
	assert.Equal(t, "backend-attempted", reopened.Lookup("mismatched").Attempt)
	assert.True(t, reopened.Lookup("conflict").Conflict)
}

func TestStore_ProjectInventoryPositiveExactObservationConfirmsAttempt(t *testing.T) {
	s := newTestStore(t)
	requireTestAdmission(t, s)
	opID := requireOperationID(t, "491")
	requireTypedAttempt(t, s, "lease-1", "backend-a", opID)
	fence := s.BeginInventorySession()
	defer s.EndInventorySession(fence)

	result, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
		Lifecycles: map[string]LifecycleObservation{
			"lease-1": {
				Kind: LifecycleObservationTyped,
				ID:   lifecycleIDFromOperation(t, opID),
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, result.Fenced)

	confirmed := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, confirmed.State())
	assert.Equal(t, "backend-a", confirmed.Backend)
	assert.Empty(t, confirmed.Attempt)
	assert.False(t, confirmed.AttemptOperationID().Valid())
}

func TestStore_ProjectInventorySilencePreservesAttemptsAndConflicts(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a", "backend-b")

	firstID := requireOperationID(t, "492")
	requireTypedAttempt(t, s, "attempt-only", "backend-a", firstID)
	requireConfirmedPlacement(t, s, "confirmed-attempt", "backend-b")
	secondID := requireOperationID(t, "493")
	requireTypedAttempt(t, s, "confirmed-attempt", "backend-b", secondID)

	conflictFence := s.BeginInventorySession()
	_, err := s.ProjectInventory(conflictFence, InventoryProjection{
		Conflicts: map[string][]string{"conflict": {"backend-a", "backend-b"}},
	})
	s.EndInventorySession(conflictFence)
	require.NoError(t, err)

	before := s.List()
	beforeRevision := testRevision(s)
	silenceFence := s.BeginInventorySession()
	result, err := s.ProjectInventory(silenceFence, InventoryProjection{
		Complete: true,
		EmptyBackends: []string{
			"backend-a", "backend-b",
		},
		BackendStorageIdentities: testBackendStorageIDs(
			"backend-a", "backend-b",
		),
	})
	s.EndInventorySession(silenceFence)
	require.NoError(t, err)
	assert.Empty(t, result.Fenced)
	assert.Equal(t, beforeRevision, testRevision(s))
	assert.Equal(t, before, s.List())
	assert.Equal(t, firstID, s.Lookup("attempt-only").AttemptOperationID())
	assert.Equal(t, secondID, s.Lookup("confirmed-attempt").AttemptOperationID())
	assert.True(t, s.Lookup("conflict").Conflict)
}

func TestNormalizeInventoryProjectionRejectsUnsoundEmptyBackendEvidence(t *testing.T) {
	tests := map[string]InventoryProjection{
		"complete without raw evidence": {
			Complete: true,
		},
		"partial with raw evidence": {
			EmptyBackends: []string{},
		},
		"positive owner certified empty": {
			Complete:      true,
			EmptyBackends: []string{"backend-a"},
			Placements:    map[string]string{"lease": "backend-a"},
		},
		"conflict reporter certified empty": {
			Complete:      true,
			EmptyBackends: []string{"backend-a"},
			Conflicts: map[string][]string{
				"lease": {"backend-a", "backend-b"},
			},
		},
	}
	for name, projection := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := normalizeInventoryProjection(projection)
			require.ErrorIs(t, err, ErrInvalidPlacement)
		})
	}
}

func TestStore_ProjectInventoryFencesInvalidForeignAndStaleEvidence(t *testing.T) {
	s := newTestStore(t)
	input := InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
	}

	_, err := s.ProjectInventory(InventoryFence{}, input)
	require.ErrorIs(t, err, ErrInvalidInventoryFence)
	other := newTestStore(t)
	foreignFence := other.BeginInventorySession()
	_, err = s.ProjectInventory(foreignFence, input)
	other.EndInventorySession(foreignFence)
	require.ErrorIs(t, err, ErrInvalidInventoryFence)

	invalidated := s.BeginInventorySession()
	s.EndInventorySession(invalidated)
	invalidateTestAdmission(t, s)
	_, err = s.ProjectInventory(invalidated, input)
	require.ErrorIs(t, err, ErrInvalidInventoryFence)
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())

	requireConfirmedPlacement(t, s, "lease-newer", "backend-a")
	requireTestAdmission(t, s)
	fence := s.BeginInventorySession()
	current := s.Lookup("lease-newer")
	_, applied, err := s.BeginOwnedAttempt(
		s.CurrentAdmissionBaseline(), current.RecordRevision(), "backend-a",
		requireOperationID(t, "571"), PayloadFingerprint{},
		testBackendRequestSnapshot(t),
		testCallbackPair(requireOperationID(t, "571")))

	require.NoError(t, err)
	require.True(t, applied)
	result, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"lease-newer": "backend-b"},
	})
	require.NoError(t, err)
	assert.Contains(t, result.Fenced, "lease-newer")
	assert.Equal(t, "backend-a", s.Lookup("lease-newer").Backend)
	s.EndInventorySession(fence)
}

func TestStore_TypedAttemptAndInventoryRejectEmptyPlacementIDs(t *testing.T) {
	s := newTestStore(t)
	operationID := requireOperationID(t, "494")

	for _, test := range []struct {
		name        string
		leaseUUID   string
		backendName string
	}{
		{name: "attempt lease", backendName: "backend-a"},
		{name: "attempt backend", leaseUUID: "lease-1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			token, applied, err := s.BeginNewAttempt(
				AdmissionScope{}, test.leaseUUID, test.backendName, operationID,
				PayloadFingerprint{}, testBackendRequestSnapshot(t), testCallbackPair(operationID))

			require.ErrorIs(t, err, ErrInvalidPlacement)
			assert.False(t, applied)
			assert.False(t, token.Valid())
		})
	}

	fence := s.BeginInventorySession()
	defer s.EndInventorySession(fence)
	for _, test := range []struct {
		name       string
		placements map[string]string
	}{
		{name: "inventory lease", placements: map[string]string{"": "backend-a"}},
		{name: "inventory backend", placements: map[string]string{"lease-1": ""}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := s.ProjectInventory(fence, InventoryProjection{Placements: test.placements})
			require.ErrorIs(t, err, ErrInvalidPlacement)
		})
	}
}

func TestStore_ProjectInventoryRollsBackEveryKeyWhenBoltRejectsOne(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	fence := s.BeginInventorySession()
	beforeRevision := testRevision(s)
	oversizedLease := strings.Repeat("z", bolt.MaxKeySize+1)

	_, err = s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{
			"a-valid":      "backend-a",
			oversizedLease: "backend-b",
		},
	})
	require.Error(t, err)
	assert.Equal(t, beforeRevision, testRevision(s))
	assert.Equal(t, StateAbsent, s.Lookup("a-valid").State(),
		"the earlier bbolt Put must roll back with the later rejected key")
	assert.Equal(t, StateAbsent, s.Lookup(oversizedLease).State())
	s.EndInventorySession(fence)
	require.NoError(t, s.Close())

	reopened, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	defer reopened.Close()
	assert.Equal(t, StateAbsent, reopened.Lookup("a-valid").State())
	assert.Equal(t, StateAbsent, reopened.Lookup(oversizedLease).State())
}

func TestStore_ProjectInventoryVerifiesEmptyAndIdempotentProjection(t *testing.T) {
	s := newTestStore(t)
	requireConfirmedPlacement(t, s, "lease-1", "backend-a")
	fence := s.BeginInventorySession()
	defer s.EndInventorySession(fence)
	beforeRevision := testRevision(s)

	result, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
	})
	require.NoError(t, err)
	assert.Empty(t, result.Fenced)
	assert.Equal(t, beforeRevision, testRevision(s))

	require.NoError(t, s.Close())
	_, err = s.ProjectInventory(fence, InventoryProjection{})
	require.Error(t, err,
		"an empty projection must still prove the durable store is readable")
}

func TestStore_PersistsAllStatesAndRevisions(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	fixed := time.Date(2026, 8, 25, 17, 11, 15, 0, time.UTC)

	s1, err := newStoreForTest(dbPath, WithClock(func() time.Time { return fixed }))
	require.NoError(t, err)
	requireAdmissionBaseline(t, s1, "backend-a", "backend-b", "backend-c")
	requireTypedAttempt(t, s1, "attempting", "backend-a", requireOperationID(t, "601"))
	requireConfirmedPlacement(t, s1, "confirmed", "backend-b")
	requireConfirmedPlacement(t, s1, "confirmed-attempt", "backend-c")
	requireTypedAttempt(t, s1, "confirmed-attempt", "backend-c", requireOperationID(t, "602"))
	want := s1.List()
	wantRevision := testRevision(s1)
	require.NoError(t, s1.Close())

	s2, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	got := s2.List()
	for leaseUUID, p := range want {
		p.recordRevision = RecordRevision{}
		want[leaseUUID] = p
	}
	for leaseUUID, p := range got {
		p.recordRevision = RecordRevision{}
		got[leaseUUID] = p
	}
	assert.Equal(t, want, got,
		"durable placement facts persist while process-local capabilities rebind")
	assert.Equal(t, wantRevision, testRevision(s2))
	assert.Equal(t, StateAttempting, s2.Lookup("attempting").State())
	assert.Equal(t, StateConfirmed, s2.Lookup("confirmed").State())
	assert.Equal(t, "backend-c", s2.Lookup("confirmed-attempt").Attempt)
}

func TestStore_DeletePersists(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s1, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	requireConfirmedPlacement(t, s1, "lease-1", "backend-a")
	requireConfirmedPlacement(t, s1, "lease-2", "backend-b")
	requireDeleteRecord(t, s1, "lease-1")
	require.NoError(t, s1.Close())

	s2, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	defer s2.Close()
	assert.Equal(t, StateAbsent, s2.Lookup("lease-1").State())
	assert.Equal(t, "backend-b", s2.Lookup("lease-2").Backend)
	assert.Len(t, s2.List(), 1)
}

func TestStore_OpenMigratesLegacyRawAndJSONRecordsMonotonically(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	setAt := "2026-08-25T15:00:00Z"
	writeRawRecords(t, dbPath, map[string][]byte{
		"already-revised": []byte(`{"backend":"backend-current","set_at":"2026-08-24T15:00:00Z","revision":11}`),
		"legacy":          []byte("backend-legacy"),
		"old-json":        []byte(`{"backend":"backend-json","set_at":"` + setAt + `"}`),
	})

	s, err := newStore(dbPath, true)
	require.NoError(t, err)

	legacy := s.Lookup("legacy")
	assert.Equal(t, StateConfirmed, legacy.State())
	assert.Equal(t, "backend-legacy", legacy.Backend)
	assert.True(t, legacy.SetAt.IsZero())
	assert.Equal(t, uint64(12), legacy.Revision(),
		"migrated revisions start above the existing durable clock")
	assert.True(t, legacy.RecordRevision().Valid())

	oldJSON := s.Lookup("old-json")
	assert.Equal(t, StateConfirmed, oldJSON.State())
	assert.Equal(t, "backend-json", oldJSON.Backend)
	assert.Equal(t, setAt, oldJSON.SetAt.Format(time.RFC3339))
	assert.Equal(t, uint64(13), oldJSON.Revision())
	assert.True(t, oldJSON.RecordRevision().Valid())
	assert.Equal(t, uint64(11), s.Lookup("already-revised").Revision())
	assert.Equal(t, uint64(13), testRevision(s))

	require.NoError(t, s.Close())
	reopened, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	defer reopened.Close()
	assert.Equal(t, uint64(12), reopened.Lookup("legacy").Revision())
	assert.Equal(t, "backend-legacy", reopened.Lookup("legacy").Backend)
	assert.True(t, reopened.Lookup("legacy").SetAt.IsZero())
	assert.Equal(t, uint64(13), reopened.Lookup("old-json").Revision())
	assert.Equal(t, setAt, reopened.Lookup("old-json").SetAt.Format(time.RFC3339))
	assert.Equal(t, uint64(13), testRevision(reopened),
		"reopening an already migrated store is idempotent")
}

func TestStore_OpenMigrationLeavesAmbiguousAndUnusableRecordsUntouched(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	records := map[string][]byte{
		"confirmed": []byte(`{"backend":"backend-confirmed","set_at":"2026-08-25T15:00:00Z"}`),
		"confirmed-with-attempt": []byte(
			`{"backend":"backend-owner","attempt":"backend-target","set_at":"2026-08-25T15:00:00Z"}`,
		),
		"attempt-only": []byte(`{"attempt":"backend-target","set_at":"2026-08-25T15:00:00Z"}`),
		"conflict": []byte(
			`{"backend":"backend-owner","set_at":"2026-08-25T15:00:00Z","conflict":true,"conflict_backends":["backend-owner","backend-other"]}`,
		),
		"operation-without-attempt": []byte(
			`{"backend":"backend-owner","operation_id":"00000000-0000-4000-8000-000000000001","set_at":"2026-08-25T15:00:00Z"}`,
		),
		"malformed-json": []byte(`{"backend":`),
		"invalid-raw":    {0xff, 0xfe},
	}
	writeRawRecords(t, dbPath, records)

	s, err := newStore(dbPath, true)
	require.NoError(t, err)
	assert.True(t, s.Lookup("confirmed").RecordRevision().Valid())
	for _, leaseUUID := range []string{
		"confirmed-with-attempt", "attempt-only", "conflict",
		"operation-without-attempt", "malformed-json", "invalid-raw",
	} {
		assert.Equal(t, StateUnusable, s.Lookup(leaseUUID).State(), leaseUUID)
		assert.False(t, s.Lookup(leaseUUID).RecordRevision().Valid(), leaseUUID)
	}
	require.NoError(t, s.Close())

	unsafeKeys := []string{
		"confirmed-with-attempt", "attempt-only", "conflict",
		"operation-without-attempt", "malformed-json", "invalid-raw",
	}
	durable := readRawRecords(t, dbPath, unsafeKeys...)
	for _, leaseUUID := range unsafeKeys {
		assert.Equal(t, records[leaseUUID], durable[leaseUUID], leaseUUID)
	}
}

func TestStore_OpenMigrationRollsBackAllRecordsOnRevisionExhaustion(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	records := map[string][]byte{
		"a-legacy": []byte("backend-a"),
		"b-legacy": []byte(`{"backend":"backend-b","set_at":"2026-08-25T15:00:00Z"}`),
		"z-revised": []byte(fmt.Sprintf(
			`{"backend":"backend-z","set_at":"2026-08-25T15:00:00Z","revision":%d}`,
			uint64(math.MaxUint64-1),
		)),
	}
	writeRawRecords(t, dbPath, records)

	s, err := newStore(dbPath, true)
	require.ErrorContains(t, err, "placement revision exhausted during legacy migration")
	assert.Nil(t, s)

	durable := readRawRecords(t, dbPath, "a-legacy", "b-legacy", "z-revised")
	assert.Equal(t, records, durable,
		"a later migration failure must roll back every earlier record rewrite")
}

func TestStore_LegacyRawBackendNamesRequirePrintableUTF8(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	valid := map[string]string{
		"ascii":   "backend-legacy_01",
		"unicode": "backend-montréal-東京",
	}
	writeRawRecords(t, dbPath, map[string][]byte{
		"ascii":        []byte(valid["ascii"]),
		"unicode":      []byte(valid["unicode"]),
		"invalid-utf8": {0xff, 0xfe},
		"nul":          []byte("backend\x00hidden"),
		"newline":      []byte("backend-a\nbackend-b"),
		"escape":       []byte("backend-\x1b[31m"),
		"zero-width":   []byte("backend-\u200bhidden"),
	})

	s, err := newStore(dbPath, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	for leaseUUID, backendName := range valid {
		p := s.Lookup(leaseUUID)
		assert.Equal(t, StateConfirmed, p.State(), leaseUUID)
		assert.Equal(t, backendName, p.Backend, leaseUUID)
		assert.NotZero(t, p.Revision(), leaseUUID)
		assert.True(t, p.RecordRevision().Valid(), leaseUUID)
	}
	for _, leaseUUID := range []string{"invalid-utf8", "nul", "newline", "escape", "zero-width"} {
		p := s.Lookup(leaseUUID)
		assert.Equal(t, StateUnusable, p.State(), leaseUUID)
		assert.Empty(t, p.Backend, leaseUUID)
		assert.Contains(t, s.List(), leaseUUID,
			"invalid legacy bytes must remain present and fail closed")
		assert.False(t, p.RecordRevision().Valid(),
			"corrupt records cannot mint an exact mutation capability")
	}
}

func TestStore_MalformedRecordsRemainUnusable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		"empty":            {},
		"malformed-json":   []byte(`{not-json`),
		"empty-object":     []byte(`{}`),
		"structured-empty": []byte(`{"backend":"","set_at":"2026-08-25T15:00:00Z","revision":7}`),
		"legacy":           []byte("backend-legacy"),
	})

	fixed := time.Date(2026, 8, 25, 18, 0, 0, 0, time.UTC)
	s, err := newStore(dbPath, true, WithClock(func() time.Time { return fixed }))
	require.NoError(t, err)
	defer s.Close()

	for _, leaseUUID := range []string{"empty", "malformed-json", "empty-object", "structured-empty"} {
		assert.Equal(t, StateUnusable, s.Lookup(leaseUUID).State(), leaseUUID)
		assert.Contains(t, s.List(), leaseUUID, "unusable records remain visible in snapshots")
	}
	assert.Len(t, s.List(), 5)
	assert.Equal(t, uint64(8), testRevision(s),
		"the valid legacy owner is migrated above the existing revision")
	assert.Equal(t, uint64(7), s.Lookup("structured-empty").Revision())

	// Positive fleet inventory may repair a corrupt derived-index entry.
	projectInventoryForTest(t, s, InventoryProjection{
		Placements: map[string]string{"empty-object": "backend-a"},
	})
	repaired := s.Lookup("empty-object")
	assert.Equal(t, StateConfirmed, repaired.State())
	assert.Equal(t, "backend-a", repaired.Backend)
	assert.Equal(t, fixed, repaired.SetAt)
	assert.Greater(t, repaired.Revision(), uint64(7))

	assert.False(t, s.Lookup("malformed-json").RecordRevision().Valid())
	projectInventoryForTest(t, s, InventoryProjection{Placements: map[string]string{
		"malformed-json": "backend-a", "structured-empty": "backend-a",
	}})
	requireDeleteRecord(t, s, "malformed-json")
	requireDeleteRecord(t, s, "structured-empty")
}

func TestDecodeRecordRejectsAmbiguousJSONButAllowsUnknownFields(t *testing.T) {
	invalidUTF8 := append([]byte(`{"backend":"backend-a","future":"`), 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(`"}`)...)
	tests := []struct {
		name        string
		value       []byte
		wantState   State
		wantBackend string
	}{
		{
			name: "duplicate ownership field",
			value: []byte(`{"backend":"backend-a","backend":"backend-b",` +
				`"set_at":"2026-08-25T15:00:00Z","revision":1}`),
			wantState: StateUnusable,
		},
		{name: "array root", value: []byte(`[]`), wantState: StateUnusable},
		{name: "null root", value: []byte(`null`), wantState: StateUnusable},
		{
			name: "trailing value",
			value: []byte(`{"backend":"backend-a",` +
				`"set_at":"2026-08-25T15:00:00Z","revision":1} {}`),
			wantState: StateUnusable,
		},
		{name: "invalid UTF-8", value: invalidUTF8, wantState: StateUnusable},
		{
			name: "unknown field is forward compatible",
			value: []byte(`{"backend":"backend-a",` +
				`"set_at":"2026-08-25T15:00:00Z","revision":1,` +
				`"future":{"nested":true}}`),
			wantState: StateConfirmed, wantBackend: "backend-a",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoded := decodeRecord("lease", test.value)
			assert.Equal(t, test.wantState, decoded.State())
			assert.Equal(t, test.wantBackend, decoded.Backend)
		})
	}
}

func TestStore_ListReturnsIndependentAtomicSnapshot(t *testing.T) {
	s := newTestStore(t)
	requireTestAdmission(t, s)
	requireConfirmedPlacement(t, s, "lease-1", "backend-a")
	requireTypedAttempt(t, s, "lease-2", "backend-b", requireOperationID(t, "801"))

	snapshot := s.List()
	assert.Len(t, snapshot, 2)
	delete(snapshot, "lease-1")
	p := snapshot["lease-2"]
	p.Attempt = "mutated"
	snapshot["lease-2"] = p

	assert.Equal(t, "backend-a", s.Lookup("lease-1").Backend)
	assert.Equal(t, "backend-b", s.Lookup("lease-2").Attempt)
	assert.Len(t, s.List(), 2)
}

func TestStore_DurableWriteFailuresLeaveCacheAndRevisionUnchanged(t *testing.T) {
	s := newTestStore(t)
	requireConfirmedPlacement(t, s, "confirmed", "backend-a")
	requireConflictPlacement(t, s, "conflict", "backend-a", "backend-b")
	before := s.List()
	beforeCache := maps.Clone(s.cache)
	for leaseUUID, placement := range beforeCache {
		placement.ConflictBackends = slices.Clone(placement.ConflictBackends)
		beforeCache[leaseUUID] = placement
	}
	beforeRevision := testRevision(s)
	beforeDeleteRevisions := len(s.deleteRevisions)
	fence := s.BeginInventorySession()
	// Close only bbolt to inject a definitely pre-commit storage failure. A real
	// Store.Close deliberately withdraws all cached read authority and has its
	// own regression in runtime_authority_test.go.
	require.NoError(t, s.db.Close())

	_, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"batch": "backend-a"},
	})
	require.Error(t, err, "an empty inventory must not report a durable sync against a closed store")
	assert.Equal(t, beforeCache, s.cache,
		"failed durable projection must not mutate the internal cache")
	assert.Equal(t, beforeRevision, testRevision(s))

	deleted, err := s.DeleteRecord(before["confirmed"].RecordRevision())
	require.Error(t, err)
	assert.False(t, deleted)
	assert.Equal(t, beforeCache, s.cache, "failed durable delete must not evict the cache")
	assert.Equal(t, beforeRevision, testRevision(s))
	assert.Equal(t, beforeDeleteRevisions, len(s.deleteRevisions),
		"failed durable deletes must not advance the stale-snapshot barrier")
}

func TestStore_EncodingFailureLeavesCacheAndRevisionUnchanged(t *testing.T) {
	invalidJSONTime := time.Date(10_000, 1, 1, 0, 0, 0, 0, time.UTC)
	s := newTestStore(t, WithClock(func() time.Time { return invalidJSONTime }))

	fence := s.BeginInventorySession()
	_, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
	})
	s.EndInventorySession(fence)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encode inventory projection")
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())
	assert.Zero(t, testRevision(s))
}

func TestStore_ProjectInventoryHonorsPostSnapshotDeleteTombstone(t *testing.T) {
	s := newTestStore(t)
	requireTestAdmission(t, s)
	requireTypedAttempt(t, s, "lease-1", "backend-a", requireOperationID(t, "851"))
	fence := s.BeginInventorySession()
	defer s.EndInventorySession(fence)
	requireDeleteRecord(t, s, "lease-1")

	result, err := s.ProjectInventory(fence, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
	})
	require.NoError(t, err)
	assert.Contains(t, result.Fenced, "lease-1")
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State(),
		"stale positive inventory must not recreate a record deleted after fetch began")
}

func TestStore_InventoryDeleteFencesAreLeaseLocal(t *testing.T) {
	s := newTestStore(t)
	requireAdmissionBaseline(t, s, "backend-a", "backend-b", "backend-c")
	requireConfirmedPlacement(t, s, "deleted", "backend-a")
	fence := s.BeginInventorySession()
	requireDeleteRecord(t, s, "deleted")

	result, err := s.ProjectInventory(fence, InventoryProjection{Placements: map[string]string{
		"deleted": "backend-a", "observed": "backend-b",
	}})
	require.NoError(t, err)
	assert.Contains(t, result.Fenced, "deleted")
	assert.NotContains(t, result.Fenced, "observed")
	assert.Equal(t, StateAbsent, s.Lookup("deleted").State())
	assert.Equal(t, "backend-b", s.Lookup("observed").Backend,
		"one lease's deletion must not suppress another lease's positive inventory")

	s.EndInventorySession(fence)
	assert.Empty(t, s.deleteRevisions, "ending the last snapshot releases every tombstone")
}

func TestStore_InventorySnapshotRefcountsAndPrunesDeleteFences(t *testing.T) {
	s := newTestStore(t)
	requireConfirmedPlacement(t, s, "lease-1", "backend-a")
	first := s.BeginInventorySession()
	second := s.BeginInventorySession()
	require.Equal(t, first.revision, second.revision)
	requireDeleteRecord(t, s, "lease-1")
	assert.Contains(t, s.deleteRevisions, "lease-1")

	s.EndInventorySession(first)
	assert.Contains(t, s.deleteRevisions, "lease-1",
		"one of two callers at the same cutoff still needs the exact-key fence")
	result, err := s.ProjectInventory(second, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
	})
	require.NoError(t, err)
	assert.Contains(t, result.Fenced, "lease-1")

	s.EndInventorySession(second)
	assert.Empty(t, s.deleteRevisions)
	assert.Empty(t, s.activeSnapshots)

	requireConfirmedPlacement(t, s, "outside-snapshot", "backend-a")
	requireDeleteRecord(t, s, "outside-snapshot")
	assert.Empty(t, s.deleteRevisions,
		"ordinary mutations after every inventory ends must not retain tombstones")
}

func TestStore_InventorySnapshotPrunesAgainstOldestActiveCutoff(t *testing.T) {
	t.Run("newer snapshot saw deletion", func(t *testing.T) {
		s := newTestStore(t)
		requireConfirmedPlacement(t, s, "lease-1", "backend-a")
		older := s.BeginInventorySession()
		requireDeleteRecord(t, s, "lease-1")
		newer := s.BeginInventorySession()
		require.Greater(t, newer.revision, older.revision)

		s.EndInventorySession(older)
		assert.NotContains(t, s.deleteRevisions, "lease-1",
			"the remaining newer snapshot began after the deletion")
		s.EndInventorySession(newer)
		assert.Empty(t, s.activeSnapshots)
	})

	t.Run("older snapshot still needs deletion", func(t *testing.T) {
		s := newTestStore(t)
		requireConfirmedPlacement(t, s, "lease-1", "backend-a")
		older := s.BeginInventorySession()
		requireDeleteRecord(t, s, "lease-1")
		newer := s.BeginInventorySession()
		require.Greater(t, newer.revision, older.revision)

		s.EndInventorySession(newer)
		assert.Contains(t, s.deleteRevisions, "lease-1",
			"ending the newer snapshot cannot discard the older snapshot's fence")
		s.EndInventorySession(older)
		assert.Empty(t, s.deleteRevisions)
		assert.Empty(t, s.activeSnapshots)
	})
}

func TestStore_DeleteFencesAreBoundedBySnapshotLifetime(t *testing.T) {
	s := newTestStore(t)
	fence := s.BeginInventorySession()
	const deletedKeys = 256
	for i := range deletedKeys {
		leaseUUID := fmt.Sprintf("deleted-%03d", i)
		requireConfirmedPlacement(t, s, leaseUUID, "backend-a")
		requireDeleteRecord(t, s, leaseUUID)
	}
	assert.Len(t, s.deleteRevisions, deletedKeys)

	s.EndInventorySession(fence)
	assert.Empty(t, s.deleteRevisions,
		"completed inventory cannot leave process-lifetime per-lease tombstones")
}

func TestStore_ClearConflictDeletionBlocksOlderInventory(t *testing.T) {
	s := newTestStore(t)
	requireConflictPlacement(t, s, "lease-1", "backend-a", "backend-b")
	staleFence := s.BeginInventorySession()
	defer s.EndInventorySession(staleFence)
	requireDeleteRecord(t, s, "lease-1")
	require.Greater(t, s.deleteRevisions["lease-1"], staleFence.revision)
	result, err := s.ProjectInventory(staleFence, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
	})
	require.NoError(t, err)
	assert.Contains(t, result.Fenced, "lease-1")
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State(),
		"inventory older than conflict deletion must not recreate the placement")

	projectInventoryForTest(t, s, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-a"},
	})
	assert.Equal(t, StateConfirmed, s.Lookup("lease-1").State())
}

func TestStore_ConflictDeleteFencesAreLeaseLocal(t *testing.T) {
	s := newTestStore(t)
	requireConfirmedPlacement(t, s, "deleted", "backend-a")
	fence := s.BeginInventorySession()
	defer s.EndInventorySession(fence)
	requireDeleteRecord(t, s, "deleted")

	result, err := s.ProjectInventory(fence, InventoryProjection{Conflicts: map[string][]string{
		"deleted":  {"backend-a", "backend-b"},
		"observed": {"backend-a", "backend-b"},
	}})
	require.NoError(t, err)
	assert.Contains(t, result.Fenced, "deleted")
	assert.NotContains(t, result.Fenced, "observed")
	assert.Equal(t, StateAbsent, s.Lookup("deleted").State())
	p := s.Lookup("observed")
	assert.Equal(t, StateUnusable, p.State())
	assert.Equal(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
}

func TestStore_ConflictQuarantineSurvivesRestartAndRejectsInventoryResolution(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	requireConfirmedPlacement(t, s, "lease-1", "backend-a")
	requireConflictPlacement(t, s, "lease-1", "backend-b", "backend-a")
	p := s.Lookup("lease-1")
	assert.True(t, p.Conflict)
	assert.Equal(t, StateUnusable, p.State())
	assert.Equal(t, "backend-a", p.Backend,
		"quarantine must preserve the last exact confirmed-owner fact")
	assert.Equal(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
	assert.False(t, p.ConflictOwnersUnknown)
	require.NoError(t, s.Close())

	s, err = newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	p = s.Lookup("lease-1")
	assert.True(t, p.Conflict, "quarantine must survive a providerd restart")
	assert.Equal(t, StateUnusable, p.State())
	assert.Equal(t, "backend-a", p.Backend)
	assert.Equal(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
	assert.False(t, p.ConflictOwnersUnknown)

	projectInventoryForTest(t, s, InventoryProjection{
		Placements: map[string]string{"lease-1": "backend-b"},
	})
	p = s.Lookup("lease-1")
	assert.True(t, p.Conflict)
	assert.Equal(t, StateUnusable, p.State())
	assert.Equal(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
	assert.False(t, p.ConflictOwnersUnknown)

	requireConflictPlacement(t, s, "lease-1", "backend-a", "backend-b")
	requireDeleteRecord(t, s, "lease-1")
	assert.Equal(t, StateAbsent, s.Lookup("lease-1").State())
}

func TestStore_UntrustedPositiveQuarantineSurvivesRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a", "backend-b"}))

	projectInventoryForTest(t, s, InventoryProjection{
		UntrustedPositives: map[string][]string{"lease-1": {"backend-a"}},
	})
	before := s.Lookup("lease-1")
	require.Equal(t, StateUnusable, before.State())
	assert.True(t, before.Conflict)
	assert.Equal(t, []string{"backend-a"}, before.ConflictBackends)
	assert.False(t, before.ConflictOwnersUnknown)
	assert.True(t, before.CanResolveUntrustedPositive("backend-a"))
	assert.False(t, before.CanResolveUntrustedPositive("backend-b"))
	require.NoError(t, s.Close())

	s, err = newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	after := s.Lookup("lease-1")
	require.Equal(t, StateUnusable, after.State())
	assert.Equal(t, []string{"backend-a"}, after.ConflictBackends)
	assert.True(t, after.CanResolveUntrustedPositive("backend-a"),
		"the typed quarantine reason must survive process restart")
}

func TestStore_CompleteMatchingInventoryResolvesSoleUntrustedPositive(t *testing.T) {
	s := newTestStore(t)
	require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a", "backend-b"}))
	projectInventoryForTest(t, s, InventoryProjection{
		UntrustedPositives: map[string][]string{"lease-1": {"backend-a"}},
	})

	projectInventoryForTest(t, s, InventoryProjection{
		Complete:   true,
		Placements: map[string]string{"lease-1": "backend-a"},
	})
	resolved := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, resolved.State())
	assert.Equal(t, "backend-a", resolved.Backend)
	assert.False(t, resolved.Conflict)
	assert.False(t, resolved.CanResolveUntrustedPositive("backend-a"))
}

func TestStore_RepeatedSoleUntrustedPositiveRemainsResolvableAfterRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := newStoreForTest(dbPath)
	require.NoError(t, err)
	require.NoError(t, configureBackendTopologyForTest(s, []string{"backend-a", "backend-b"}))
	projectInventoryForTest(t, s, InventoryProjection{
		UntrustedPositives: map[string][]string{"lease-1": {"backend-a"}},
	})
	require.NoError(t, s.Close())

	s, err = newStoreForTest(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	projectInventoryForTest(t, s, InventoryProjection{
		UntrustedPositives: map[string][]string{"lease-1": {"backend-a"}},
	})
	repeated := s.Lookup("lease-1")
	require.False(t, repeated.ConflictOwnersUnknown,
		"repeating the same known rejected reporter must not manufacture unknown history")
	require.True(t, repeated.CanResolveUntrustedPositive("backend-a"))

	projectInventoryForTest(t, s, InventoryProjection{
		Complete:   true,
		Placements: map[string]string{"lease-1": "backend-a"},
	})
	resolved := s.Lookup("lease-1")
	assert.Equal(t, StateConfirmed, resolved.State())
	assert.Equal(t, "backend-a", resolved.Backend)
}

func TestStore_UntrustedPositiveCannotOverlapOrBecomeEmptyEvidence(t *testing.T) {
	s := newTestStore(t)
	fence := s.BeginInventorySession()
	defer s.EndInventorySession(fence)

	_, err := s.ProjectInventory(fence, InventoryProjection{
		Placements:         map[string]string{"lease-1": "backend-a"},
		UntrustedPositives: map[string][]string{"lease-1": {"backend-a"}},
	})
	require.ErrorIs(t, err, ErrInvalidPlacement)

	_, err = s.ProjectInventory(fence, InventoryProjection{
		UntrustedPositives: map[string][]string{"lease-2": {"", ""}},
	})
	require.ErrorIs(t, err, ErrInvalidPlacement)
}

func TestStore_LegacyConflictWithoutCandidatesLoadsFailClosed(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	writeRawRecords(t, dbPath, map[string][]byte{
		"lease-legacy-conflict":  []byte(`{"backend":"","set_at":"2026-08-25T12:00:00Z","revision":7,"conflict":true}`),
		"lease-partial-conflict": []byte(`{"backend":"","set_at":"2026-08-25T12:00:00Z","revision":8,"conflict":true,"conflict_backends":["backend-a"]}`),
	})

	s, err := newStore(dbPath, true)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	for _, leaseUUID := range []string{"lease-legacy-conflict", "lease-partial-conflict"} {
		p := s.Lookup(leaseUUID)
		require.Equal(t, StateUnusable, p.State())
		assert.True(t, p.Conflict)
		assert.True(t, p.ConflictOwnersUnknown,
			"incomplete conflict records must not treat the current router as the complete historical owner set")
	}

	projectInventoryForTest(t, s, InventoryProjection{Conflicts: map[string][]string{
		"lease-partial-conflict": {"backend-a", "backend-b"},
	}})
	p := s.Lookup("lease-partial-conflict")
	assert.Equal(t, []string{"backend-a", "backend-b"}, p.ConflictBackends)
	assert.True(t, p.ConflictOwnersUnknown,
		"later reports cannot prove which candidate was omitted from an incomplete historical record")
}

func TestStore_StaleConflictSnapshotCannotOverwriteNewAttempt(t *testing.T) {
	s := newTestStore(t)
	requireTestAdmission(t, s)
	fence := s.BeginInventorySession()
	requireTypedAttempt(t, s, "lease-1", "backend-a", requireOperationID(t, "871"))
	result, err := s.ProjectInventory(fence, InventoryProjection{Conflicts: map[string][]string{
		"lease-1": {"backend-a", "backend-b"},
	}})
	s.EndInventorySession(fence)
	require.NoError(t, err)
	assert.Contains(t, result.Fenced, "lease-1")
	p := s.Lookup("lease-1")
	assert.False(t, p.Conflict)
	assert.Equal(t, StateAttempting, p.State())
	assert.Equal(t, "backend-a", p.Attempt)
}

func TestStore_DeleteCASCannotLoseRacingAttempt(t *testing.T) {
	s := newTestStore(t)
	requireTestAdmission(t, s)

	for i := range 32 {
		leaseUUID := fmt.Sprintf("lease-%d", i)
		requireConfirmedPlacement(t, s, leaseUUID, "backend-a")
		stale := s.Lookup(leaseUUID)
		baseline := s.CurrentAdmissionBaseline()
		opID := requireOperationID(t, strconv.Itoa(900+i))

		start := make(chan struct{})
		var wg sync.WaitGroup
		var deleted, attempted bool
		var token AttemptToken
		var deleteErr, attemptErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			deleted, deleteErr = s.DeleteRecord(stale.RecordRevision())
		}()
		go func() {
			defer wg.Done()
			<-start
			token, attempted, attemptErr = s.BeginOwnedAttempt(
				baseline, stale.RecordRevision(), "backend-a", opID,
				PayloadFingerprint{}, testBackendRequestSnapshot(t), testCallbackPair(opID))

		}()
		close(start)
		wg.Wait()

		require.NoError(t, deleteErr)
		require.NoError(t, attemptErr)
		assert.NotEqual(t, deleted, attempted,
			"the exact record capability permits either delete or attempt, never both")
		p := s.Lookup(leaseUUID)
		if deleted {
			assert.Equal(t, StateAbsent, p.State())
		} else {
			assert.Equal(t, StateConfirmed, p.State())
			assert.Equal(t, "backend-a", p.Backend)
			assert.Equal(t, "backend-a", p.Attempt)
			settled, err := s.RefuseAttempt(token)
			require.NoError(t, err)
			require.True(t, settled)
			requireDeleteRecord(t, s, leaseUUID)
		}
	}
}

func TestStore_HealthCloseAndConstruction(t *testing.T) {
	_, err := newStoreForTest("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "placement db path is required")

	s := newTestStore(t, WithClock(nil))
	requireConfirmedPlacement(t, s, "lease-1", "backend-a")
	assert.False(t, s.Lookup("lease-1").SetAt.IsZero())
	assert.NoError(t, s.Healthy())
	require.NoError(t, s.Close())
	assert.Error(t, s.Healthy())
	assert.NoError(t, s.Close(), "Close must be idempotent")
}

func TestStore_HealthyRejectsMissingOrCorruptTopologyMetadata(t *testing.T) {
	t.Run("missing bucket", func(t *testing.T) {
		s := newTestStore(t)
		require.NoError(t, s.db.Update(func(tx *bolt.Tx) error {
			return tx.DeleteBucket(metadataBucketName)
		}))
		err := s.Healthy()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "placement metadata bucket missing")
	})

	t.Run("missing state", func(t *testing.T) {
		s := newTestStore(t)
		require.NoError(t, s.db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket(metadataBucketName).Delete(metadataStateKey)
		}))
		err := s.Healthy()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "placement metadata state missing")
	})

	t.Run("malformed state", func(t *testing.T) {
		s := newTestStore(t)
		require.NoError(t, s.db.Update(func(tx *bolt.Tx) error {
			return tx.Bucket(metadataBucketName).Put(metadataStateKey, []byte(`{"schema":2,"topology_id":1}`))
		}))
		err := s.Healthy()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "malformed")
	})
}

func TestErrorsAreDistinctSemanticSentinels(t *testing.T) {
	sentinels := []error{
		ErrInvalidPlacement,
		ErrAttemptConflict,
		ErrBackendConflict,
		ErrUnusablePlacement,
		ErrAttemptMismatch,
		ErrInvalidAttemptToken,
		ErrInvalidInventoryFence,
		ErrInvalidRecordRevision,
	}
	for i, left := range sentinels {
		for j, right := range sentinels {
			if i == j {
				assert.True(t, errors.Is(left, right))
				continue
			}
			assert.False(t, errors.Is(left, right))
		}
	}
}

// fakeClock is a trivial settable clock for deterministic transition tests.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }
