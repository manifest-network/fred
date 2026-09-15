package backendidentity

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/fsidentity"
)

func TestBoundMarkerPairDoesNotMintPendingAuthorityBeforeAnchorSync(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	primaryPath := filepath.Join(dir, "primary.json")
	anchorPath := filepath.Join(dir, "anchor.json")
	pair, err := bindMarkerPair(primaryPath, anchorPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })
	id, err := New()
	require.NoError(t, err)
	barrier, err := ensureBoundMarkerRecordTransition(
		pair.anchor,
		"docker-a",
		"daemon-a",
		id,
		markerStatePending,
		InitializationProfileFresh,
	)
	require.NoError(t, err)
	// Drive the real state-transition pieces across a genuine filesystem
	// boundary: revoking the retained descriptor makes its required fsync fail.
	require.NoError(t, pair.anchor.directory.Close())

	pending, err := pendingStorageAfterDurabilityBarrier(id, barrier)

	assert.ErrorIs(t, err, fsidentity.ErrDirectoryClosed)
	assert.False(t, pending.Valid(), "pending authority must not escape before its anchor is durable")
	assert.NoFileExists(t, primaryPath)
}

func TestBoundMarkerPairResumesPendingAnchor(t *testing.T) {
	dir := t.TempDir()
	primaryPath := filepath.Join(dir, "primary.json")
	anchorPath := filepath.Join(dir, "anchor.json")
	id, err := New()
	require.NoError(t, err)
	seed, err := BindMarkerPair(primaryPath, anchorPath)
	require.NoError(t, err)
	require.NoError(t, ensureBoundMarkerRecord(
		seed.anchor,
		"docker-a",
		"daemon-a",
		id,
		markerStatePending,
		InitializationProfileFresh,
	))
	require.NoError(t, seed.Close())

	pair, err := BindMarkerPair(primaryPath, anchorPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })
	prepareCalls := 0
	got, err := pair.InitializeWithStores("docker-a", "daemon-a", MarkerPairStoreHooks{
		Profile: InitializationProfileFresh,
		Prepare: func(storage PendingStorage, profile InitializationProfile) error {
			prepareCalls++
			assert.Equal(t, id, storage.ID())
			assert.Equal(t, InitializationProfileFresh, profile)
			return nil
		},
		Check:  func(PendingStorage) error { return nil },
		Verify: func(VerifiedStorage) error { return nil },
	})
	require.NoError(t, err)
	assert.Equal(t, id, got.ID())
	assert.Equal(t, 1, prepareCalls)
	loaded, err := LoadMarkerPair(primaryPath, anchorPath, "docker-a", "daemon-a")
	require.NoError(t, err)
	assert.Equal(t, id, loaded)
}

func TestBoundMarkerPairRevokesEscapedPendingAuthority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		checkErr  error
		wantValid bool
	}{
		{name: "after success", wantValid: true},
		{name: "after failure", checkErr: errors.New("injected store check failure")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			pair, err := BindMarkerPair(
				filepath.Join(dir, "primary.json"),
				filepath.Join(dir, "anchor.json"),
			)
			require.NoError(t, err)
			t.Cleanup(func() { _ = pair.Close() })

			var escaped PendingStorage
			var pendingID ID
			storage, err := pair.InitializeWithStores("docker-a", "daemon-a", MarkerPairStoreHooks{
				Profile: InitializationProfileFresh,
				Prepare: func(pending PendingStorage, _ InitializationProfile) error {
					escaped = pending
					pendingID = pending.ID()
					return nil
				},
				Check: func(PendingStorage) error { return test.checkErr },
				Verify: func(VerifiedStorage) error {
					assert.False(t, escaped.Valid(),
						"pending authority must expire before committed-store verification")
					return nil
				},
			})

			if test.checkErr != nil {
				assert.ErrorIs(t, err, test.checkErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, pendingID, storage.ID())
			}
			assert.Equal(t, test.wantValid, storage.Valid())
			assert.True(t, pendingID.Valid(), "the hook must receive live pending authority")
			assert.False(t, escaped.Valid(), "pending authority must expire when initialization returns")
			assert.Equal(t, ID{}, escaped.ID(), "revoked authority must not reveal a usable identity")
		})
	}
}

func TestBoundMarkerPairNeverPublishesIntoReplacementParent(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "authority")
	retired := filepath.Join(root, "authority-retired")
	require.NoError(t, os.Mkdir(parent, 0o700))
	primaryPath := filepath.Join(parent, "primary.json")
	anchorPath := filepath.Join(parent, "anchor.json")

	pair, err := BindMarkerPair(primaryPath, anchorPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })

	_, err = pair.InitializeWithStores("docker-a", "daemon-a", MarkerPairStoreHooks{
		Profile: InitializationProfileFresh,
		Prepare: func(PendingStorage, InitializationProfile) error {
			// The pending anchor has been published. Replace its pathname before
			// the store proof returns and the primary marker is authorized.
			require.NoError(t, os.Rename(parent, retired))
			return os.Mkdir(parent, 0o700)
		},
		Check:  func(PendingStorage) error { return nil },
		Verify: func(VerifiedStorage) error { return nil },
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "parent changed")

	replacementEntries, readErr := os.ReadDir(parent)
	require.NoError(t, readErr)
	assert.Empty(t, replacementEntries, "replacement storage must remain unsealed")
	_, err = os.Stat(filepath.Join(retired, filepath.Base(anchorPath)))
	require.NoError(t, err, "the crash-resumable pending intent remains on the retained parent")
	_, err = os.Lstat(filepath.Join(retired, filepath.Base(primaryPath)))
	assert.ErrorIs(t, err, os.ErrNotExist,
		"path drift must stop the first committed marker publication")
}
