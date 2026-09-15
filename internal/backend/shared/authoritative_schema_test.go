package shared

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

const schemaTestLeaseUUID = "11111111-1111-4111-8111-111111111111"

var errSchemaPrepareInterrupted = errors.New("test interrupted after store preparation")

// initializePendingSchemaStore reproduces the only compatibility boundary for
// an earlier build of this branch: the marker anchor is still pending, but the
// same Prepare Existing transaction already bound the store before the process
// stopped. Runtime Open/Check/Verify never enter this path.
func initializePendingSchemaStore(
	t *testing.T,
	bucketName []byte,
	makeHooks func(*BoundAuthoritativeStorePath) backendidentity.MarkerPairStoreHooks,
) (string, *backendidentity.BoundMarkerPair, backendidentity.MarkerPairStoreHooks) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "authority.db")
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		_, createErr := tx.CreateBucket(bucketName)
		return createErr
	}))
	require.NoError(t, db.Close())

	bound, err := BindAuthoritativeStorePath(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bound.Close()) })
	pair, err := backendidentity.BindMarkerPair(
		filepath.Join(dir, "primary.json"),
		filepath.Join(dir, "anchor.json"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pair.Close()) })

	hooks := makeHooks(bound)
	interrupted := hooks
	prepare := hooks.Prepare
	interrupted.Prepare = func(
		storage backendidentity.PendingStorage,
		profile backendidentity.InitializationProfile,
	) error {
		if err := prepare(storage, profile); err != nil {
			return err
		}
		return errSchemaPrepareInterrupted
	}
	_, err = pair.InitializeWithStores("docker-a", "daemon-a", interrupted)
	require.ErrorIs(t, err, errSchemaPrepareInterrupted)
	return dbPath, pair, hooks
}

func releaseSchemaHooks(
	bound *BoundAuthoritativeStorePath,
) backendidentity.MarkerPairStoreHooks {
	return backendidentity.MarkerPairStoreHooks{
		Profile: backendidentity.InitializationProfileExisting,
		Prepare: func(storage backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
			return PrepareBoundReleaseStoreStorage(bound, storage, profile)
		},
		Check: func(storage backendidentity.PendingStorage) error {
			return CheckBoundReleaseStoreStorage(bound, storage)
		},
		Verify: func(storage backendidentity.VerifiedStorage) error {
			return VerifyBoundReleaseStoreStorage(bound, storage)
		},
	}
}

func retentionSchemaHooks(
	bound *BoundAuthoritativeStorePath,
) backendidentity.MarkerPairStoreHooks {
	return backendidentity.MarkerPairStoreHooks{
		Profile: backendidentity.InitializationProfileExisting,
		Prepare: func(storage backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
			return PrepareBoundRetentionStoreStorage(bound, storage, profile)
		},
		Check: func(storage backendidentity.PendingStorage) error {
			return CheckBoundRetentionStoreStorage(bound, storage)
		},
		Verify: func(storage backendidentity.VerifiedStorage) error {
			return VerifyBoundRetentionStoreStorage(bound, storage)
		},
	}
}

func initializeBoundRetentionStore(t *testing.T) (string, backendidentity.VerifiedStorage) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "retention.db")
	pair, err := backendidentity.BindMarkerPair(
		filepath.Join(dir, "primary.json"),
		filepath.Join(dir, "anchor.json"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pair.Close()) })
	bound, err := BindAuthoritativeStorePath(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bound.Close()) })
	storage, err := pair.InitializeWithStores(
		"docker-a",
		"daemon-a",
		backendidentity.MarkerPairStoreHooks{
			Profile: backendidentity.InitializationProfileFresh,
			Prepare: func(pending backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
				return PrepareBoundRetentionStoreStorage(bound, pending, profile)
			},
			Check: func(pending backendidentity.PendingStorage) error {
				return CheckBoundRetentionStoreStorage(bound, pending)
			},
			Verify: func(verified backendidentity.VerifiedStorage) error {
				return VerifyBoundRetentionStoreStorage(bound, verified)
			},
		},
	)
	require.NoError(t, err)
	return dbPath, storage
}

func putSchemaTestRecord(t *testing.T, dbPath string, bucketName []byte, value []byte) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(schemaTestLeaseUUID), value)
	}))
	require.NoError(t, db.Close())
}

func TestIdentityBoundReleaseOpenRejectsNonCurrentWireShapes(t *testing.T) {
	tests := map[string]struct {
		value   string
		wantErr string
	}{
		"versionless": {
			value:   `[{"version":1,"status":"active"}]`,
			wantErr: "expected JSON object",
		},
		"future version": {
			value:   `{"schema_version":2,"releases":[]}`,
			wantErr: "unsupported release-history schema version 2",
		},
		"unknown field": {
			value:   `{"schema_version":1,"releases":[],"future":true}`,
			wantErr: `unknown field "future"`,
		},
		"case alias": {
			value:   `{"Schema_version":1,"releases":[]}`,
			wantErr: `unknown field "Schema_version"`,
		},
		"duplicate field": {
			value:   `{"schema_version":1,"schema_version":1,"releases":[]}`,
			wantErr: `duplicate field "schema_version"`,
		},
		"trailing value": {
			value:   `{"schema_version":1,"releases":[]} true`,
			wantErr: "unexpected data after JSON",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			dbPath, storage := initializeBoundReleaseStore(t)
			putSchemaTestRecord(t, dbPath, releasesBucketName, []byte(test.value))
			store, err := OpenIdentityBoundReleaseStore(
				ReleaseStoreConfig{DBPath: dbPath}, storage, newTestStorageAuthorityGate(t),
			)
			assert.Nil(t, store)
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestIdentityBoundRetentionOpenRejectsNonCurrentWireShapes(t *testing.T) {
	tests := map[string]struct {
		value   string
		wantErr string
	}{
		"versionless": {
			value:   `{"original_lease_uuid":"` + schemaTestLeaseUUID + `"}`,
			wantErr: `unknown field "original_lease_uuid"`,
		},
		"future version": {
			value:   `{"schema_version":2,"entry":{}}`,
			wantErr: "unsupported retention-entry schema version 2",
		},
		"unknown field": {
			value:   `{"schema_version":1,"entry":{},"future":true}`,
			wantErr: `unknown field "future"`,
		},
		"case alias": {
			value:   `{"Schema_version":1,"entry":{}}`,
			wantErr: `unknown field "Schema_version"`,
		},
		"duplicate field": {
			value:   `{"schema_version":1,"schema_version":1,"entry":{}}`,
			wantErr: `duplicate field "schema_version"`,
		},
		"trailing value": {
			value:   `{"schema_version":1,"entry":{}} true`,
			wantErr: "unexpected data after JSON",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			dbPath, storage := initializeBoundRetentionStore(t)
			putSchemaTestRecord(t, dbPath, retentionBucketName, []byte(test.value))
			if name == "versionless" {
				require.ErrorContains(
					t, VerifyRetentionStoreStorage(dbPath, storage), test.wantErr,
					"the committed-marker verifier must enforce the same v1 boundary as Open",
				)
			}
			store, err := OpenIdentityBoundRetentionStore(
				RetentionStoreConfig{DBPath: dbPath}, storage, newTestStorageAuthorityGate(t),
			)
			assert.Nil(t, store)
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestReleaseRuntimeAuthorityRejectsCaseAlias(t *testing.T) {
	var authority ReleaseRuntimeAuthority
	err := json.Unmarshal([]byte(`{
		"schema_version":1,
		"Tenant":"tenant-a",
		"provider_uuid":"22222222-2222-4222-8222-222222222222",
		"callback_url":"https://fred.example/callbacks/provision?operation_id=550e8400-e29b-41d4-a716-446655440000",
		"lifecycle_callback_url":"https://fred.example/callbacks/provision?lifecycle_id=550e8400-e29b-41d4-a716-446655440000"
	}`), &authority)
	require.ErrorContains(t, err, `unknown field "Tenant"`)
}

func TestIdentityBoundReleaseAndRetentionOpenRejectUnknownRoot(t *testing.T) {
	tests := []struct {
		name       string
		initialize func(*testing.T) (string, backendidentity.VerifiedStorage)
		open       func(string, backendidentity.VerifiedStorage) error
	}{
		{
			name:       "release",
			initialize: initializeBoundReleaseStore,
			open: func(path string, storage backendidentity.VerifiedStorage) error {
				_, err := OpenIdentityBoundReleaseStore(
					ReleaseStoreConfig{DBPath: path}, storage, newTestStorageAuthorityGate(t),
				)
				return err
			},
		},
		{
			name:       "retention",
			initialize: initializeBoundRetentionStore,
			open: func(path string, storage backendidentity.VerifiedStorage) error {
				_, err := OpenIdentityBoundRetentionStore(
					RetentionStoreConfig{DBPath: path}, storage, newTestStorageAuthorityGate(t),
				)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dbPath, storage := test.initialize(t)
			db, err := bolt.Open(dbPath, 0o600, nil)
			require.NoError(t, err)
			require.NoError(t, db.Update(func(tx *bolt.Tx) error {
				_, err := tx.CreateBucket([]byte("future_authority"))
				return err
			}))
			require.NoError(t, db.Close())
			require.ErrorContains(t, test.open(dbPath, storage), "unsupported top-level bucket")
		})
	}
}

func TestAuthorityStoreHealthyRejectsUnknownRootAddedAfterOpen(t *testing.T) {
	type openedStore struct {
		db      *bolt.DB
		healthy func() error
		close   func() error
	}
	tests := []struct {
		name string
		open func(*testing.T) openedStore
	}{
		{
			name: "callback",
			open: func(t *testing.T) openedStore {
				path, storage := initializeBoundCallbackStore(t)
				store, err := OpenIdentityBoundCallbackStore(
					CallbackStoreConfig{DBPath: path}, storage, newTestStorageAuthorityGate(t),
				)
				require.NoError(t, err)
				return openedStore{store.db, store.Healthy, store.Close}
			},
		},
		{
			name: "release",
			open: func(t *testing.T) openedStore {
				path, storage := initializeBoundReleaseStore(t)
				store, err := OpenIdentityBoundReleaseStore(
					ReleaseStoreConfig{DBPath: path}, storage, newTestStorageAuthorityGate(t),
				)
				require.NoError(t, err)
				return openedStore{store.db, store.Healthy, store.Close}
			},
		},
		{
			name: "retention",
			open: func(t *testing.T) openedStore {
				path, storage := initializeBoundRetentionStore(t)
				store, err := OpenIdentityBoundRetentionStore(
					RetentionStoreConfig{DBPath: path}, storage, newTestStorageAuthorityGate(t),
				)
				require.NoError(t, err)
				return openedStore{store.db, store.Healthy, store.Close}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := test.open(t)
			t.Cleanup(func() { require.NoError(t, store.close()) })
			require.NoError(t, store.healthy())
			require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
				_, err := tx.CreateBucket([]byte("future_authority"))
				return err
			}))
			require.ErrorContains(t, store.healthy(), "unsupported top-level bucket")
		})
	}
}

func TestPrepareExistingUpgradesCompleteVersionlessReleaseJournal(t *testing.T) {
	dbPath, pair, hooks := initializePendingSchemaStore(
		t, releasesBucketName, releaseSchemaHooks,
	)
	leaseUUIDs := []string{
		schemaTestLeaseUUID,
		"22222222-2222-4222-8222-222222222222",
	}
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(releasesBucketName)
		for _, leaseUUID := range leaseUUIDs {
			value, marshalErr := json.Marshal([]Release{{
				Version: 1, Manifest: []byte(`{"image":"alpine:3.22"}`),
				Image:  "alpine:3.22",
				Status: "active", CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
			}})
			if marshalErr != nil {
				return marshalErr
			}
			if putErr := bucket.Put([]byte(leaseUUID), value); putErr != nil {
				return putErr
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())

	_, err = pair.InitializeWithStores("docker-a", "daemon-a", hooks)
	require.NoError(t, err)
	db, err = bolt.Open(dbPath, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for _, leaseUUID := range leaseUUIDs {
			history, decodeErr := decodeReleaseHistory(
				tx.Bucket(releasesBucketName).Get([]byte(leaseUUID)),
			)
			require.NoError(t, decodeErr)
			require.Len(t, history, 1)
			assert.Equal(t, "alpine:3.22", history[0].Image)
		}
		return nil
	}))
	require.NoError(t, db.Close())
}

func TestPrepareExistingRejectsIncompleteVersionlessReleaseJournalAtomically(t *testing.T) {
	dbPath, pair, hooks := initializePendingSchemaStore(
		t, releasesBucketName, releaseSchemaHooks,
	)
	validRaw := []byte(`[{
		"version":1,
		"manifest":"eyJpbWFnZSI6ImFscGluZTozLjIyIn0=",
		"image":"alpine:3.22",
		"status":"active",
		"created_at":"2026-01-02T03:04:05Z"
	}]`)
	invalidRaw := []byte(`[{"version":1,"status":"active","legacy_migration":true}]`)
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(releasesBucketName)
		if err := bucket.Put([]byte(schemaTestLeaseUUID), validRaw); err != nil {
			return err
		}
		return bucket.Put(
			[]byte("22222222-2222-4222-8222-222222222222"), invalidRaw,
		)
	}))
	require.NoError(t, db.Close())

	_, err = pair.InitializeWithStores("docker-a", "daemon-a", hooks)
	require.ErrorContains(t, err, `unknown field "legacy_migration"`)
	db, err = bolt.Open(dbPath, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		assert.Equal(t, validRaw, tx.Bucket(releasesBucketName).Get([]byte(schemaTestLeaseUUID)))
		return nil
	}))
	require.NoError(t, db.Close())
}

func versionlessRetentionSchemaEntry(leaseUUID string) RetentionEntry {
	return RetentionEntry{
		OriginalLeaseUUID: leaseUUID,
		Tenant:            "tenant-a",
		ProviderUUID:      "33333333-3333-4333-8333-333333333333",
		Items: []backend.LeaseItem{{
			SKU: "docker-small", Quantity: 1, ServiceName: "app",
		}},
		ResourceProfiles: []SKUResourceSnapshot{{
			SKU: "docker-small", CPUCores: 1, MemoryMB: 512, DiskMB: 1024,
		}},
		StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
			"app": {Image: "docker.io/library/alpine:3.22"},
		}},
		CallbackURL:         "https://fred.example/callbacks/provision",
		RetainedVolumeNames: []string{"fred-retained-" + leaseUUID + "-app-0"},
		Status:              RetentionStatusActive,
		CreatedAt:           time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}
}

func TestPrepareExistingUpgradesCompleteVersionlessRetentionJournal(t *testing.T) {
	dbPath, pair, hooks := initializePendingSchemaStore(
		t, retentionBucketName, retentionSchemaHooks,
	)
	leaseUUIDs := []string{
		schemaTestLeaseUUID,
		"22222222-2222-4222-8222-222222222222",
	}
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(retentionBucketName)
		for _, leaseUUID := range leaseUUIDs {
			value, marshalErr := json.Marshal(versionlessRetentionSchemaEntry(leaseUUID))
			if marshalErr != nil {
				return marshalErr
			}
			if putErr := bucket.Put([]byte(leaseUUID), value); putErr != nil {
				return putErr
			}
		}
		return nil
	}))
	require.NoError(t, db.Close())

	_, err = pair.InitializeWithStores("docker-a", "daemon-a", hooks)
	require.NoError(t, err)
	db, err = bolt.Open(dbPath, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		for _, leaseUUID := range leaseUUIDs {
			entry, decodeErr := decodeRetentionEntry(
				tx.Bucket(retentionBucketName).Get([]byte(leaseUUID)),
			)
			require.NoError(t, decodeErr)
			assert.Equal(t, leaseUUID, entry.OriginalLeaseUUID)
			assert.NotEmpty(t, entry.ResourceProfiles)
		}
		return nil
	}))
	require.NoError(t, db.Close())
}

func TestPrepareExistingRejectsIncompleteVersionlessRetentionJournalAtomically(t *testing.T) {
	dbPath, pair, hooks := initializePendingSchemaStore(
		t, retentionBucketName, retentionSchemaHooks,
	)
	validRaw, err := json.Marshal(versionlessRetentionSchemaEntry(schemaTestLeaseUUID))
	require.NoError(t, err)
	invalidRaw := append(
		[]byte(nil),
		validRaw[:len(validRaw)-1]...,
	)
	invalidRaw = append(invalidRaw, []byte(`,"future":true}`)...)
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(retentionBucketName)
		if err := bucket.Put([]byte(schemaTestLeaseUUID), validRaw); err != nil {
			return err
		}
		return bucket.Put(
			[]byte("22222222-2222-4222-8222-222222222222"), invalidRaw,
		)
	}))
	require.NoError(t, db.Close())

	_, err = pair.InitializeWithStores("docker-a", "daemon-a", hooks)
	require.ErrorContains(t, err, `unknown field "future"`)
	db, err = bolt.Open(dbPath, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		assert.Equal(t, validRaw, tx.Bucket(retentionBucketName).Get([]byte(schemaTestLeaseUUID)))
		return nil
	}))
	require.NoError(t, db.Close())
}
