package shared

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendidentity"
)

type fakeAuthoritativeStoreWriteTransaction struct {
	commitErr     error
	rollbackErr   error
	commitCalls   int
	rollbackCalls int
}

func newTestStorageAuthorityGate(t *testing.T) *backendidentity.StorageAuthorityGate {
	t.Helper()
	gate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
	require.NoError(t, err)
	return gate
}

func (tx *fakeAuthoritativeStoreWriteTransaction) Commit() error {
	tx.commitCalls++
	return tx.commitErr
}

func (tx *fakeAuthoritativeStoreWriteTransaction) Rollback() error {
	tx.rollbackCalls++
	return tx.rollbackErr
}

func TestAuthoritativeStoreCommitErrorIsOutcomeUnknown(t *testing.T) {
	commitErr := errors.New("synthetic commit failure")
	tx := &fakeAuthoritativeStoreWriteTransaction{commitErr: commitErr}

	err := finishAuthoritativeStoreWriteTransaction(tx, func() error { return nil })

	require.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	require.ErrorIs(t, err, commitErr)
	assert.Equal(t, 1, tx.commitCalls)
	assert.Equal(t, 1, tx.rollbackCalls)
}

func TestAuthoritativeStoreWithdrawalBeforeCommitRollsBackWithoutCommit(t *testing.T) {
	gate := newTestStorageAuthorityGate(t)
	cause := errors.Join(backendidentity.ErrIdentityDrift, errors.New("release journal withdrew authority"))
	require.EqualError(t, gate.Latch(cause), cause.Error())
	tx := &fakeAuthoritativeStoreWriteTransaction{}

	err := finishAuthoritativeStoreWriteTransactionAtBoundary(
		tx,
		func() error { return nil },
		func(commit func() error) (bool, error) {
			attempted := false
			err := gate.Run(func() error {
				attempted = true
				return commit()
			})
			return attempted, err
		},
	)

	assert.EqualError(t, err, cause.Error())
	assert.Equal(t, 0, tx.commitCalls)
	assert.Equal(t, 1, tx.rollbackCalls)
}

func TestAuthoritativeStoreFileRequiresExactOwnerOnlyMode(t *testing.T) {
	for _, mode := range []os.FileMode{0o640, 0o644, 0o660, 0o666} {
		t.Run(mode.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "authority.db")
			require.NoError(t, os.WriteFile(path, []byte("authority"), 0o600))
			require.NoError(t, os.Chmod(path, mode))
			info, err := os.Lstat(path)
			require.NoError(t, err)

			err = validateAuthoritativeStoreFile(path, info)
			require.Error(t, err)
			assert.ErrorContains(t, err, "exact mode 0600")
		})
	}
}

func TestBoundInitializationRejectsReadableAuthoritativeStoreBeforeBinding(t *testing.T) {
	for _, mode := range []os.FileMode{0o640, 0o644} {
		t.Run(mode.String(), func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "callbacks.db")
			store, err := NewCallbackStore(CallbackStoreConfig{DBPath: dbPath})
			require.NoError(t, err)
			require.NoError(t, store.Close())
			require.NoError(t, os.Chmod(dbPath, mode))
			before, err := os.ReadFile(dbPath)
			require.NoError(t, err)

			err = prepareExistingBoundStoreForTest(
				t, dbPath, PrepareBoundCallbackStoreStorage,
			)
			require.Error(t, err)
			assert.ErrorContains(t, err, "exact mode 0600")
			after, readErr := os.ReadFile(dbPath)
			require.NoError(t, readErr)
			assert.Equal(t, before, after, "rejected initialization must not bind or rewrite the store")
			info, statErr := os.Stat(dbPath)
			require.NoError(t, statErr)
			assert.Equal(t, mode, info.Mode().Perm())

			db, openErr := bolt.Open(dbPath, 0o600, &bolt.Options{ReadOnly: true})
			require.NoError(t, openErr)
			require.NoError(t, db.View(func(tx *bolt.Tx) error {
				assert.Nil(t, tx.Bucket(storeIdentityBucketName))
				return nil
			}))
			require.NoError(t, db.Close())
		})
	}
}

func TestIdentityBoundOpenRejectsReadableAuthoritativeStore(t *testing.T) {
	for _, mode := range []os.FileMode{0o640, 0o644} {
		t.Run(mode.String(), func(t *testing.T) {
			dbPath, storage := initializeBoundCallbackStore(t)
			require.NoError(t, os.Chmod(dbPath, mode))

			store, err := OpenIdentityBoundCallbackStore(
				CallbackStoreConfig{DBPath: dbPath}, storage, newTestStorageAuthorityGate(t),
			)
			require.Error(t, err)
			assert.Nil(t, store)
			assert.ErrorContains(t, err, "exact mode 0600")
			info, statErr := os.Stat(dbPath)
			require.NoError(t, statErr)
			assert.Equal(t, mode, info.Mode().Perm())
		})
	}
}

func TestIdentityBoundStoreRejectedMutationDoesNotWithdrawAuthority(t *testing.T) {
	dbPath, storage := initializeBoundCallbackStore(t)
	store, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: dbPath}, storage, newTestStorageAuthorityGate(t),
	)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	rejected := errors.New("synthetic domain rejection")
	err = store.update(func(tx *bolt.Tx) error {
		require.NoError(t, tx.Bucket(callbackBucketName).Put([]byte("rejected"), []byte("rolled-back")))
		return rejected
	})
	require.ErrorIs(t, err, rejected)
	assert.NotErrorIs(t, err, backendidentity.ErrIdentityDrift)
	assert.NotErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)

	require.NoError(t, store.update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackBucketName).Put([]byte("accepted"), []byte("committed"))
	}))
	require.NoError(t, store.VerifyStorageIdentity(storage))
	require.NoError(t, store.view(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(callbackBucketName)
		assert.Nil(t, bucket.Get([]byte("rejected")))
		assert.Equal(t, []byte("committed"), bucket.Get([]byte("accepted")))
		return nil
	}))
}

func TestAuthorityPublicationReadCannotDeadlockBboltRemap(t *testing.T) {
	dbPath, storage := initializeBoundCallbackStore(t)
	gate := newTestStorageAuthorityGate(t)
	store, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: dbPath}, storage, gate,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	// Hold bbolt's mmap read lock while an authoritative boundary grows the DB
	// enough to require a remap. The writer then owns gate.mu and waits for the
	// read transaction. Gate.Error must remain a lock-free publication read;
	// otherwise the reader waits for gate.mu and recreates the production cycle:
	// reader mmap -> gate, writer gate -> mmap.
	readTx, err := store.db.Begin(false)
	require.NoError(t, err)
	readOpen := true
	defer func() {
		if readOpen {
			require.NoError(t, readTx.Rollback())
		}
	}()

	writePrepared := make(chan struct{})
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- gate.Run(func() error {
			return store.db.Update(func(tx *bolt.Tx) error {
				if err := tx.Bucket(callbackBucketName).Put(
					[]byte("force-remap"), make([]byte, 8<<20),
				); err != nil {
					return err
				}
				close(writePrepared)
				return nil
			})
		})
	}()
	select {
	case <-writePrepared:
	case <-time.After(time.Second):
		t.Fatal("authoritative writer did not prepare the remapping transaction")
	}
	select {
	case writeErr := <-writeDone:
		t.Fatalf("test precondition failed: remapping commit did not wait for read transaction: %v", writeErr)
	case <-time.After(50 * time.Millisecond):
	}

	authorityRead := make(chan error, 1)
	go func() { authorityRead <- gate.Error() }()
	select {
	case authorityErr := <-authorityRead:
		require.NoError(t, authorityErr)
	case <-time.After(time.Second):
		// Release the other half of the cycle so an implementation regression
		// fails cleanly instead of stranding the test process.
		require.NoError(t, readTx.Rollback())
		readOpen = false
		<-writeDone
		t.Fatal("authority publication read deadlocked behind bbolt remap")
	}

	require.NoError(t, readTx.Rollback())
	readOpen = false
	require.NoError(t, <-writeDone)
}

func initializeBoundCallbackStore(t *testing.T) (string, backendidentity.VerifiedStorage) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "callbacks.db")
	primary := filepath.Join(dir, "primary.json")
	anchor := filepath.Join(dir, "anchor.json")
	pair, err := backendidentity.BindMarkerPair(primary, anchor)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pair.Close()) })
	bound, err := BindAuthoritativeStorePath(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bound.Close()) })
	hooks := backendidentity.MarkerPairStoreHooks{
		Profile: backendidentity.InitializationProfileFresh,
		Prepare: func(storage backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
			return PrepareBoundCallbackStoreStorage(bound, storage, profile)
		},
		Check: func(storage backendidentity.PendingStorage) error {
			return CheckBoundCallbackStoreStorage(bound, storage)
		},
		Verify: func(storage backendidentity.VerifiedStorage) error {
			return VerifyBoundCallbackStoreStorage(bound, storage)
		},
	}
	storage, err := pair.InitializeWithStores("docker-a", "daemon-a", hooks)
	require.NoError(t, err)
	return dbPath, storage
}

func initializeBoundReleaseStore(t *testing.T) (string, backendidentity.VerifiedStorage) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "releases.db")
	primary := filepath.Join(dir, "primary.json")
	anchor := filepath.Join(dir, "anchor.json")
	pair, err := backendidentity.BindMarkerPair(primary, anchor)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pair.Close()) })
	bound, err := BindAuthoritativeStorePath(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bound.Close()) })
	hooks := backendidentity.MarkerPairStoreHooks{
		Profile: backendidentity.InitializationProfileFresh,
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
	storage, err := pair.InitializeWithStores("docker-a", "daemon-a", hooks)
	require.NoError(t, err)
	return dbPath, storage
}

func TestIdentityBoundStoreOpenRequiresConstructedAuthorityGate(t *testing.T) {
	dbPath, storage := initializeBoundCallbackStore(t)
	var zeroGate backendidentity.StorageAuthorityGate
	_, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: dbPath}, storage, &zeroGate,
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "backend storage authority gate is required")

	tests := []struct {
		name string
		open func() error
	}{
		{
			name: "callback",
			open: func() error {
				_, err := OpenIdentityBoundCallbackStore(CallbackStoreConfig{DBPath: dbPath}, storage, nil)
				return err
			},
		},
		{
			name: "release",
			open: func() error {
				_, err := OpenIdentityBoundReleaseStore(ReleaseStoreConfig{DBPath: dbPath}, storage, nil)
				return err
			},
		},
		{
			name: "retention",
			open: func() error {
				_, err := OpenIdentityBoundRetentionStore(RetentionStoreConfig{DBPath: dbPath}, storage, nil)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.open()
			require.Error(t, err)
			assert.ErrorContains(t, err, "backend storage authority gate is required")
		})
	}
}

func TestIdentityBoundCallbackStoreRefusesMissingSchemaBuckets(t *testing.T) {
	for _, bucketName := range callbackCurrentSchemaBuckets() {
		bucketName := append([]byte(nil), bucketName...)
		t.Run(string(bucketName), func(t *testing.T) {
			dbPath, storage := initializeBoundCallbackStore(t)
			db, err := bolt.Open(dbPath, 0o600, nil)
			require.NoError(t, err)
			require.NoError(t, db.Update(func(tx *bolt.Tx) error { return tx.DeleteBucket(bucketName) }))
			require.NoError(t, db.Close())

			err = VerifyCallbackStoreStorage(dbPath, storage)
			require.Error(t, err)
			assert.ErrorContains(t, err, "missing")

			store, err := OpenIdentityBoundCallbackStore(
				CallbackStoreConfig{DBPath: dbPath}, storage, newTestStorageAuthorityGate(t),
			)
			require.Error(t, err)
			assert.Nil(t, store)

			db, err = bolt.Open(dbPath, 0o600, &bolt.Options{ReadOnly: true})
			require.NoError(t, err)
			require.NoError(t, db.View(func(tx *bolt.Tx) error {
				assert.Nil(t, tx.Bucket(bucketName), "normal open must not repair deleted schema")
				return nil
			}))
			require.NoError(t, db.Close())
		})
	}
}

func TestIdentityBoundStorePostCommitReplacementIsAmbiguousAndSticky(t *testing.T) {
	dbPath, storage := initializeBoundCallbackStore(t)
	store, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: dbPath}, storage, newTestStorageAuthorityGate(t),
	)
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	originalBytes, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	originalPath := dbPath + ".opened"
	err = store.update(func(tx *bolt.Tx) error {
		require.NoError(t, os.Rename(dbPath, originalPath))
		require.NoError(t, os.WriteFile(dbPath, originalBytes, 0o600))
		return tx.Bucket(callbackBucketName).Put([]byte("race"), []byte("committed-to-old-inode"))
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous)
	assert.ErrorIs(t, err, backendidentity.ErrIdentityDrift)

	require.NoError(t, os.Remove(dbPath))
	require.NoError(t, os.Rename(originalPath, dbPath))
	err = store.update(func(tx *bolt.Tx) error {
		return tx.Bucket(callbackBucketName).Put([]byte("later"), []byte("must-not-run"))
	})
	assert.ErrorIs(t, err, backendidentity.ErrMutationOutcomeAmbiguous,
		"restoring the old pathname must not clear the process-lifetime latch")
}

func TestIdentityBoundOpenDoesNotInitializeZeroLengthReplacement(t *testing.T) {
	_, storage := initializeBoundCallbackStore(t)
	path := filepath.Join(t.TempDir(), "zero.db")
	require.NoError(t, os.WriteFile(path, nil, 0o600))

	store, err := OpenIdentityBoundCallbackStore(
		CallbackStoreConfig{DBPath: path}, storage, newTestStorageAuthorityGate(t),
	)
	require.Error(t, err)
	assert.Nil(t, store)
	info, statErr := os.Stat(path)
	require.NoError(t, statErr)
	assert.Zero(t, info.Size(), "strict normal open must leave an empty replacement untouched")
}

func TestBoundAuthoritativeStoreNeverPublishesIntoReplacementParent(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "authority")
	retired := filepath.Join(root, "authority-retired")
	require.NoError(t, os.Mkdir(parent, 0o700))
	dbPath := filepath.Join(parent, "callbacks.db")

	bound, err := BindAuthoritativeStorePath(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bound.Close() })
	markerDir := filepath.Join(root, "markers")
	require.NoError(t, os.Mkdir(markerDir, 0o700))
	pair, err := backendidentity.BindMarkerPair(
		filepath.Join(markerDir, "primary.json"),
		filepath.Join(markerDir, "anchor.json"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	_, err = pair.InitializeWithStores("docker-a", "daemon-a", backendidentity.MarkerPairStoreHooks{
		Profile: backendidentity.InitializationProfileFresh,
		Prepare: func(storage backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
			require.NoError(t, os.Rename(parent, retired))
			require.NoError(t, os.Mkdir(parent, 0o700))
			return PrepareBoundCallbackStoreStorage(bound, storage, profile)
		},
		Check: func(storage backendidentity.PendingStorage) error {
			if err := CheckBoundCallbackStoreStorage(bound, storage); err != nil {
				return err
			}
			return bound.VerifyPath()
		},
		Verify: func(backendidentity.VerifiedStorage) error { return nil },
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "directory identity changed")

	_, err = os.Lstat(dbPath)
	assert.ErrorIs(t, err, os.ErrNotExist,
		"bbolt's pathname argument must never redirect its custom opener into replacement storage")
	_, err = os.Stat(filepath.Join(retired, "callbacks.db"))
	require.NoError(t, err, "the descriptor-relative open must remain on the retained parent")
}

func TestRetentionBindingRejectsRedirectedRecordIdentity(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "retention.db")
	store, err := NewRetentionStore(RetentionStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	require.NoError(t, store.Put(RetentionEntry{
		OriginalLeaseUUID: "11111111-1111-4111-8111-111111111111",
		ProviderUUID:      "22222222-2222-4222-8222-222222222222",
		Status:            RetentionStatusActive,
	}))
	require.NoError(t, store.Close())
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(retentionBucketName)
		value := append([]byte(nil), bucket.Get([]byte("11111111-1111-4111-8111-111111111111"))...)
		require.NoError(t, bucket.Delete([]byte("11111111-1111-4111-8111-111111111111")))
		return bucket.Put([]byte("33333333-3333-4333-8333-333333333333"), value)
	}))
	require.NoError(t, db.Close())

	err = prepareExistingBoundStoreForTest(
		t, dbPath, PrepareBoundRetentionStoreStorage,
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "bucket key differs")
}

func TestReleaseBindingRejectsImpossibleHistory(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "releases.db")
	store, err := NewReleaseStore(ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	require.NoError(t, store.Append("11111111-1111-4111-8111-111111111111", Release{Status: "active"}))
	require.NoError(t, store.Close())
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(releasesBucketName).Put(
			[]byte("11111111-1111-4111-8111-111111111111"),
			[]byte(`[{"version":0,"status":"unknown"}]`),
		)
	}))
	require.NoError(t, db.Close())
	err = prepareExistingBoundStoreForTest(
		t, dbPath, PrepareBoundReleaseStoreStorage,
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "non-increasing version")
}

func TestReleaseBindingNormalizesWireFaithfulV013RecordMigrationHistory(t *testing.T) {
	const leaseUUID = "11111111-1111-4111-8111-111111111111"
	dbPath := filepath.Join(t.TempDir(), "releases.db")
	createdAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	writeRawReleaseHistory(t, dbPath, leaseUUID, []Release{
		{
			Version: 1, Manifest: []byte(`{"image":"nginx:1.25"}`),
			Image: "nginx:1.25", Status: "active", CreatedAt: createdAt,
		},
		{
			Version:  2,
			Manifest: []byte(`{"services":{"app":{"image":"nginx:1.25"}}}`),
			Image:    "stack", Status: "active", CreatedAt: createdAt.Add(time.Minute),
		},
		{
			Version:  3,
			Manifest: []byte(`{"services":{"app":{"image":"nginx:2"}}}`),
			Image:    "stack", Status: "failed", CreatedAt: createdAt.Add(2 * time.Minute),
		},
	})

	_, err := InspectReleaseStoreReadOnly(dbPath)
	require.ErrorContains(t, err, "2 active records",
		"current runtime inspection must never accept duplicate active authority")
	bound, err := BindAuthoritativeStorePath(dbPath)
	require.NoError(t, err)
	inspection, err := InspectBoundLegacyReleaseStoreReadOnly(bound)
	require.NoError(t, err)
	require.NoError(t, bound.Close())
	require.True(t, inspection.RequiresLegacyNormalization)
	require.Equal(t, 2, inspection.ActiveReleases[leaseUUID].Version,
		"v0.13 LatestActive made the migrated row authoritative even with a failed tail")

	require.NoError(t, prepareExistingBoundStoreForTest(
		t, dbPath, PrepareBoundReleaseStoreStorage,
	))
	strict, err := InspectReleaseStoreReadOnly(dbPath)
	require.NoError(t, err)
	require.True(t, strict.IdentityBound)
	require.False(t, strict.RequiresLegacyNormalization)
	releases := readRawReleaseHistory(t, dbPath, leaseUUID)
	require.Len(t, releases, 3)
	assert.Equal(t, "superseded", releases[0].Status)
	assert.Equal(t, "active", releases[1].Status)
	assert.Equal(t, "failed", releases[2].Status)
}

func TestReleaseBindingRejectsUnrecognizedDuplicateActiveHistoryWithoutMutation(t *testing.T) {
	const leaseUUID = "11111111-1111-4111-8111-111111111111"
	dbPath := filepath.Join(t.TempDir(), "releases.db")
	createdAt := time.Now().UTC()
	writeRawReleaseHistory(t, dbPath, leaseUUID, []Release{
		{
			Version:  1,
			Manifest: []byte(`{"services":{"app":{"image":"nginx:1"}}}`),
			Image:    "stack", Status: "active", CreatedAt: createdAt,
		},
		{
			Version:  2,
			Manifest: []byte(`{"services":{"app":{"image":"nginx:1"}}}`),
			Image:    "stack", Status: "active", CreatedAt: createdAt.Add(time.Minute),
		},
	})
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)

	err = prepareExistingBoundStoreForTest(t, dbPath, PrepareBoundReleaseStoreStorage)
	require.ErrorContains(t, err, "not a recognizable v0.13 migration")
	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after,
		"a rejected adoption must roll back both normalization and identity binding")
}

func TestReleaseBindingNormalizesSemanticallyIdenticalV013DeployingTail(t *testing.T) {
	const leaseUUID = "11111111-1111-4111-8111-111111111111"
	dbPath := filepath.Join(t.TempDir(), "releases.db")
	createdAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	writeRawReleaseHistory(t, dbPath, leaseUUID, []Release{
		{
			Version:  1,
			Manifest: []byte(`{"image":"nginx:1.25","env":{"MODE":"safe"},"command":["serve"]}`),
			Image:    "nginx:1.25", Status: "active", CreatedAt: createdAt,
		},
		{
			Version:  2,
			Manifest: []byte(`{"services":{"app":{"command":["serve"],"env":{"MODE":"safe"},"image":"nginx:1.25"}}}`),
			Image:    "stack", Status: "deploying", CreatedAt: createdAt.Add(time.Minute),
		},
		{
			Version:  3,
			Manifest: []byte(`{"services":{"app":{"image":"nginx:2"}}}`),
			Image:    "stack", Status: "failed", CreatedAt: createdAt.Add(2 * time.Minute),
		},
		{
			Version:  4,
			Manifest: []byte(`{"services":{"app":{"env":{"MODE":"safe"},"image":"nginx:1.25","command":["serve"]}}}`),
			Image:    "stack", Status: "deploying", CreatedAt: createdAt.Add(3 * time.Minute),
		},
	})

	bound, err := BindAuthoritativeStorePath(dbPath)
	require.NoError(t, err)
	inspection, err := InspectBoundLegacyReleaseStoreReadOnly(bound)
	require.NoError(t, err)
	require.NoError(t, bound.Close())
	require.True(t, inspection.RequiresLegacyNormalization)
	require.Equal(t, 1, inspection.ActiveReleases[leaseUUID].Version)

	require.NoError(t, prepareExistingBoundStoreForTest(
		t, dbPath, PrepareBoundReleaseStoreStorage,
	))
	releases := readRawReleaseHistory(t, dbPath, leaseUUID)
	require.Len(t, releases, 4)
	assert.Equal(t, "active", releases[0].Status)
	assert.Equal(t, "superseded", releases[1].Status,
		"an equivalent stopped deploying generation carries no distinct workload authority")
	assert.Equal(t, "failed", releases[2].Status)
	assert.Equal(t, "superseded", releases[3].Status,
		"every equivalent post-active deploying retry is normalized")
}

func TestReleaseBindingRejectsAmbiguousV013DeployingTailWithoutMutation(t *testing.T) {
	const leaseUUID = "11111111-1111-4111-8111-111111111111"
	for _, test := range []struct {
		name    string
		history []Release
	}{
		{
			name: "same image and topology but different environment",
			history: []Release{
				{
					Version:  1,
					Manifest: []byte(`{"services":{"app":{"image":"nginx:1.25","env":{"MODE":"old"}}}}`),
					Image:    "stack", Status: "active",
				},
				{
					Version:  2,
					Manifest: []byte(`{"services":{"app":{"image":"nginx:1.25","env":{"MODE":"new"}}}}`),
					Image:    "stack", Status: "deploying",
				},
			},
		},
		{
			name: "one of multiple deploying rows differs",
			history: []Release{
				{
					Version: 1, Manifest: []byte(`{"image":"nginx:1.25"}`),
					Image: "nginx:1.25", Status: "active",
				},
				{
					Version:  2,
					Manifest: []byte(`{"services":{"app":{"image":"nginx:1.25"}}}`),
					Image:    "stack", Status: "deploying",
				},
				{
					Version:  3,
					Manifest: []byte(`{"services":{"app":{"image":"nginx:1.25","command":["other"]}}}`),
					Image:    "stack", Status: "deploying",
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dbPath := filepath.Join(t.TempDir(), "releases.db")
			writeRawReleaseHistory(t, dbPath, leaseUUID, test.history)
			before, err := os.ReadFile(dbPath)
			require.NoError(t, err)

			bound, err := BindAuthoritativeStorePath(dbPath)
			require.NoError(t, err)
			_, inspectErr := InspectBoundLegacyReleaseStoreReadOnly(bound)
			require.NoError(t, bound.Close())
			require.Error(t, inspectErr)
			assert.ErrorContains(t, inspectErr, "deploying")

			err = prepareExistingBoundStoreForTest(t, dbPath, PrepareBoundReleaseStoreStorage)
			require.Error(t, err)
			after, readErr := os.ReadFile(dbPath)
			require.NoError(t, readErr)
			assert.Equal(t, before, after,
				"ambiguous stopped update authority must not be normalized or identity-bound")
		})
	}
}

func TestReleasePreparationWrongExistingBindingLeavesLegacyHistoryUnchanged(t *testing.T) {
	dbPath, _ := initializeBoundReleaseStore(t)
	const leaseUUID = "11111111-1111-4111-8111-111111111111"
	createdAt := time.Now().UTC()
	writeRawReleaseHistory(t, dbPath, leaseUUID, []Release{
		{
			Version: 1, Manifest: []byte(`{"image":"nginx:1"}`),
			Image: "nginx:1", Status: "active", CreatedAt: createdAt,
		},
		{
			Version:  2,
			Manifest: []byte(`{"services":{"app":{"image":"nginx:1"}}}`),
			Image:    "stack", Status: "active", CreatedAt: createdAt.Add(time.Minute),
		},
	})
	before, err := os.ReadFile(dbPath)
	require.NoError(t, err)
	bound, err := BindAuthoritativeStorePath(dbPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, bound.Close()) }()
	pair, err := backendidentity.BindMarkerPair(
		filepath.Join(filepath.Dir(dbPath), "wrong-primary.json"),
		filepath.Join(filepath.Dir(dbPath), "wrong-anchor.json"),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pair.Close()) }()

	_, err = pair.InitializeWithStores("docker-b", "daemon-b", backendidentity.MarkerPairStoreHooks{
		Profile: backendidentity.InitializationProfileExisting,
		Prepare: func(storage backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
			return PrepareBoundReleaseStoreStorage(bound, storage, profile)
		},
		Check:  func(backendidentity.PendingStorage) error { return nil },
		Verify: func(backendidentity.VerifiedStorage) error { return nil },
	})
	require.ErrorIs(t, err, ErrStoreIdentityMismatch)
	after, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	assert.Equal(t, before, after,
		"a foreign pending marker must not normalize an already-bound release journal")
}

func writeRawReleaseHistory(t *testing.T, dbPath, leaseUUID string, releases []Release) {
	t.Helper()
	encoded, err := json.Marshal(releases)
	require.NoError(t, err)
	db, err := bolt.Open(dbPath, 0o600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket, createErr := tx.CreateBucketIfNotExists(releasesBucketName)
		if createErr != nil {
			return createErr
		}
		return bucket.Put([]byte(leaseUUID), encoded)
	}))
	require.NoError(t, db.Close())
}

func readRawReleaseHistory(t *testing.T, dbPath, leaseUUID string) []Release {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{ReadOnly: true})
	require.NoError(t, err)
	var releases []Release
	require.NoError(t, db.View(func(tx *bolt.Tx) error {
		return json.Unmarshal(tx.Bucket(releasesBucketName).Get([]byte(leaseUUID)), &releases)
	}))
	require.NoError(t, db.Close())
	return releases
}

func prepareExistingBoundStoreForTest(
	t *testing.T,
	dbPath string,
	prepare func(
		*BoundAuthoritativeStorePath,
		backendidentity.PendingStorage,
		backendidentity.InitializationProfile,
	) error,
) error {
	t.Helper()
	bound, err := BindAuthoritativeStorePath(dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = bound.Close() }()
	pair, err := backendidentity.BindMarkerPair(
		filepath.Join(filepath.Dir(dbPath), "test-primary.json"),
		filepath.Join(filepath.Dir(dbPath), "test-anchor.json"),
	)
	if err != nil {
		return err
	}
	defer func() { _ = pair.Close() }()
	_, err = pair.InitializeWithStores("docker-a", "daemon-a", backendidentity.MarkerPairStoreHooks{
		Profile: backendidentity.InitializationProfileExisting,
		Prepare: func(storage backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
			return prepare(bound, storage, profile)
		},
		Check:  func(backendidentity.PendingStorage) error { return nil },
		Verify: func(backendidentity.VerifiedStorage) error { return nil },
	})
	return err
}

func TestIdentityBoundReleaseStoreRejectsNonCanonicalRuntimeKeysWithoutMutation(t *testing.T) {
	dbPath, storage := initializeBoundReleaseStore(t)
	store, err := OpenIdentityBoundReleaseStore(
		ReleaseStoreConfig{DBPath: dbPath}, storage, newTestStorageAuthorityGate(t),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	const invalidLeaseUUID = "not-a-canonical-lease-uuid"
	err = store.Append(invalidLeaseUUID, Release{Status: "active", CreatedAt: time.Now()})
	require.ErrorContains(t, err, "canonical lease UUID")
	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		assert.Nil(t, tx.Bucket(releasesBucketName).Get([]byte(invalidLeaseUUID)))
		return nil
	}))

	encoded, err := json.Marshal([]Release{{
		Version: 1, Status: "active", CreatedAt: time.Now().Add(-time.Hour),
	}})
	require.NoError(t, err)
	require.NoError(t, store.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(releasesBucketName).Put([]byte(invalidLeaseUUID), encoded)
	}))
	wantRaw := append([]byte(nil), encoded...)

	_, err = store.List(invalidLeaseUUID)
	require.ErrorContains(t, err, "canonical lease UUID")
	require.ErrorContains(t, store.Delete(invalidLeaseUUID), "canonical lease UUID")
	_, err = store.LeaseUUIDs()
	require.ErrorContains(t, err, "not canonical")
	_, err = store.RemoveOlderThan(time.Minute)
	require.ErrorContains(t, err, "not canonical")

	require.NoError(t, store.db.View(func(tx *bolt.Tx) error {
		assert.Equal(t, wantRaw, tx.Bucket(releasesBucketName).Get([]byte(invalidLeaseUUID)))
		return nil
	}))
}
