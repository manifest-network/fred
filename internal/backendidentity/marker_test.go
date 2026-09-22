package backendidentity

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/fsidentity"
)

func TestMarkerPairInitializationStateMatrixNeverReturnsZeroIdentityOnSuccess(t *testing.T) {
	t.Parallel()

	type markerState string
	const (
		absent    markerState = "absent"
		pending   markerState = "pending"
		committed markerState = "committed"
		invalid   markerState = "invalid"
	)
	states := []markerState{absent, pending, committed, invalid}
	first, err := Parse(canonicalTestID)
	require.NoError(t, err)
	second, err := Parse("6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	require.NoError(t, err)

	for _, primaryState := range states {
		for _, anchorState := range states {
			for _, sameIdentity := range []bool{true, false} {
				name := fmt.Sprintf("primary_%s_anchor_%s_same_%t", primaryState, anchorState, sameIdentity)
				t.Run(name, func(t *testing.T) {
					directory := t.TempDir()
					primaryPath := filepath.Join(directory, "primary.json")
					anchorPath := filepath.Join(directory, "anchor.json")
					writePairState(t, primaryPath, primaryState, first)
					anchorID := first
					if !sameIdentity {
						anchorID = second
					}
					writePairState(t, anchorPath, anchorState, anchorID)

					id, initializeErr := initializeBoundMarkerPairForTest(
						primaryPath, anchorPath, "docker-a", "daemon-a",
					)
					wantSuccess := primaryState == absent && anchorState == absent ||
						primaryState == absent && anchorState == pending ||
						sameIdentity && primaryState == committed && anchorState == pending ||
						sameIdentity && primaryState == committed && anchorState == committed
					if wantSuccess {
						require.NoError(t, initializeErr)
						assert.True(t, id.Valid())
						loaded, loadErr := LoadMarkerPair(
							primaryPath, anchorPath, "docker-a", "daemon-a",
						)
						require.NoError(t, loadErr)
						assert.Equal(t, id, loaded)
						return
					}
					assert.Error(t, initializeErr)
					assert.Equal(t, ID{}, id)
				})
			}
		}
	}
}

func TestLoadMarkerPairRefusesAbsentOrPendingStateWithoutMutation(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	id, err := initializeBoundMarkerPairForTest(primaryPath, anchorPath, "docker-a", "daemon-a")
	require.NoError(t, err)
	require.True(t, id.Valid())

	require.NoError(t, os.Remove(primaryPath))
	before, err := os.ReadFile(anchorPath)
	require.NoError(t, err)
	loaded, err := LoadMarkerPair(primaryPath, anchorPath, "docker-a", "daemon-a")
	assert.ErrorIs(t, err, ErrInvalidMarker)
	assert.Equal(t, ID{}, loaded)
	after, err := os.ReadFile(anchorPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestInitializeMarkerPairRejectsOversizedBindingBeforePublication(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")

	id, err := initializeBoundMarkerPairForTest(
		primaryPath,
		anchorPath,
		"docker-a",
		strings.Repeat("s", maxMarkerBindingValueBytes+1),
	)
	assert.ErrorIs(t, err, ErrInvalidMarkerBinding)
	assert.Equal(t, ID{}, id)
	assert.NoFileExists(t, primaryPath)
	assert.NoFileExists(t, anchorPath)
}

func TestInitializeMarkerPairRejectsOversizedEncodedMarkerBeforePublication(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	// encoding/json escapes each ampersand as six bytes. Both individual values
	// satisfy the input bound, but their serialized representation exceeds the
	// complete marker bound and must be rejected before the pending anchor lands.
	escapeHeavy := strings.Repeat("&", maxMarkerBindingValueBytes)

	id, err := initializeBoundMarkerPairForTest(
		primaryPath,
		anchorPath,
		escapeHeavy,
		escapeHeavy,
	)
	assert.ErrorIs(t, err, ErrInvalidMarkerBinding)
	assert.Equal(t, ID{}, id)
	assert.NoFileExists(t, primaryPath)
	assert.NoFileExists(t, anchorPath)
}

func TestMarkerPairSupportsMaximumNonEscapingBindingValues(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	backendName := strings.Repeat("b", maxMarkerBindingValueBytes)
	substrateID := strings.Repeat("s", maxMarkerBindingValueBytes)

	id, err := initializeBoundMarkerPairForTest(
		primaryPath,
		anchorPath,
		backendName,
		substrateID,
	)
	require.NoError(t, err)
	require.True(t, id.Valid())

	loaded, err := LoadMarkerPair(
		primaryPath,
		anchorPath,
		backendName,
		substrateID,
	)
	require.NoError(t, err)
	assert.Equal(t, id, loaded)
}

func TestInitializeMarkerPairWithStoresResumesExactPendingIdentityAndProfile(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	prepareCalls := 0
	var pendingID ID
	hooks := MarkerPairStoreHooks{
		Profile: InitializationProfileExisting,
		Prepare: func(storage PendingStorage, profile InitializationProfile) error {
			id := storage.ID()
			prepareCalls++
			assert.Equal(t, InitializationProfileExisting, profile)
			if prepareCalls == 1 {
				pendingID = id
				return errors.New("injected prepare interruption")
			}
			assert.Equal(t, pendingID, id)
			return nil
		},
		Check: func(storage PendingStorage) error {
			id := storage.ID()
			assert.Equal(t, pendingID, id)
			return nil
		},
		Verify: func(storage VerifiedStorage) error {
			assert.True(t, storage.Valid())
			assert.Equal(t, pendingID, storage.ID())
			return nil
		},
	}

	id, err := initializeBoundMarkerPairWithStoresForTest(
		primaryPath, anchorPath, "docker-a", "daemon-a", hooks,
	)
	assert.ErrorContains(t, err, "injected prepare interruption")
	assert.Equal(t, ID{}, id)
	assert.NoFileExists(t, primaryPath)
	storedProfile, pending, err := pendingBoundMarkerPairInitializationProfileForTest(
		primaryPath, anchorPath, "docker-a", "daemon-a",
	)
	require.NoError(t, err)
	assert.True(t, pending)
	assert.Equal(t, InitializationProfileExisting, storedProfile)

	id, err = initializeBoundMarkerPairWithStoresForTest(
		primaryPath, anchorPath, "docker-a", "daemon-a", hooks,
	)
	require.NoError(t, err)
	assert.Equal(t, pendingID, id)
	assert.Equal(t, 2, prepareCalls)

	loaded, err := LoadMarkerPair(
		primaryPath, anchorPath, "docker-a", "daemon-a",
	)
	require.NoError(t, err)
	assert.Equal(t, pendingID, loaded)
}

func TestBoundMarkerPairCannotBypassStoreAwarePendingWithMissingHooks(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	hooks := MarkerPairStoreHooks{
		Profile: InitializationProfileFresh,
		Prepare: func(PendingStorage, InitializationProfile) error {
			return errors.New("injected prepare interruption")
		},
		Check:  func(PendingStorage) error { return nil },
		Verify: func(VerifiedStorage) error { return nil },
	}

	_, err := initializeBoundMarkerPairWithStoresForTest(
		primaryPath, anchorPath, "docker-a", "daemon-a", hooks,
	)
	require.Error(t, err)
	anchorBefore, err := os.ReadFile(anchorPath)
	require.NoError(t, err)

	pair, err := BindMarkerPair(primaryPath, anchorPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pair.Close()) })
	storage, err := pair.InitializeWithStores(
		"docker-a", "daemon-a", MarkerPairStoreHooks{},
	)
	assert.ErrorIs(t, err, ErrInvalidMarkerBinding)
	assert.Equal(t, VerifiedStorage{}, storage)
	assert.NoFileExists(t, primaryPath)
	anchorAfter, err := os.ReadFile(anchorPath)
	require.NoError(t, err)
	assert.Equal(t, anchorBefore, anchorAfter)
}

func TestInitializeMarkerPairWithStoresCannotAdoptProfilelessPendingMarker(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	id, err := Parse(canonicalTestID)
	require.NoError(t, err)
	writeProfilelessPendingPairState(t, anchorPath, id)
	anchorBefore, err := os.ReadFile(anchorPath)
	require.NoError(t, err)
	prepareCalls := 0
	checkCalls := 0

	initialized, err := initializeBoundMarkerPairWithStoresForTest(
		primaryPath,
		anchorPath,
		"docker-a",
		"daemon-a",
		MarkerPairStoreHooks{
			Profile: InitializationProfileFresh,
			Prepare: func(PendingStorage, InitializationProfile) error {
				prepareCalls++
				return nil
			},
			Check: func(PendingStorage) error {
				checkCalls++
				return nil
			},
			Verify: func(VerifiedStorage) error { return nil },
		},
	)

	assert.ErrorIs(t, err, ErrMarkerBindingMismatch)
	assert.Equal(t, ID{}, initialized)
	assert.Zero(t, prepareCalls)
	assert.Zero(t, checkCalls)
	assert.NoFileExists(t, primaryPath)
	anchorAfter, readErr := os.ReadFile(anchorPath)
	require.NoError(t, readErr)
	assert.Equal(t, anchorBefore, anchorAfter)
}

func TestInitializeMarkerPairWithStoresRefusesPendingProfileChange(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	existingHooks := MarkerPairStoreHooks{
		Profile: InitializationProfileExisting,
		Prepare: func(PendingStorage, InitializationProfile) error {
			return errors.New("injected existing-profile interruption")
		},
		Check:  func(PendingStorage) error { return nil },
		Verify: func(VerifiedStorage) error { return nil },
	}
	_, err := initializeBoundMarkerPairWithStoresForTest(
		primaryPath, anchorPath, "docker-a", "daemon-a", existingHooks,
	)
	require.ErrorContains(t, err, "injected existing-profile interruption")
	anchorBefore, err := os.ReadFile(anchorPath)
	require.NoError(t, err)
	prepareCalls := 0
	checkCalls := 0

	initialized, err := initializeBoundMarkerPairWithStoresForTest(
		primaryPath,
		anchorPath,
		"docker-a",
		"daemon-a",
		MarkerPairStoreHooks{
			Profile: InitializationProfileFresh,
			Prepare: func(PendingStorage, InitializationProfile) error {
				prepareCalls++
				return nil
			},
			Check: func(PendingStorage) error {
				checkCalls++
				return nil
			},
			Verify: func(VerifiedStorage) error { return nil },
		},
	)

	assert.ErrorIs(t, err, ErrMarkerBindingMismatch)
	assert.Equal(t, ID{}, initialized)
	assert.Zero(t, prepareCalls)
	assert.Zero(t, checkCalls)
	assert.NoFileExists(t, primaryPath)
	anchorAfter, readErr := os.ReadFile(anchorPath)
	require.NoError(t, readErr)
	assert.Equal(t, anchorBefore, anchorAfter)
}

func TestInitializeMarkerPairWithStoresCommittedRerunIsVerifyOnly(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	prepareCalls := 0
	checkCalls := 0
	verifyCalls := 0
	hooks := MarkerPairStoreHooks{
		Profile: InitializationProfileFresh,
		Prepare: func(PendingStorage, InitializationProfile) error {
			prepareCalls++
			return nil
		},
		Check: func(PendingStorage) error {
			checkCalls++
			return nil
		},
		Verify: func(VerifiedStorage) error {
			verifyCalls++
			return nil
		},
	}

	first, err := initializeBoundMarkerPairWithStoresForTest(
		primaryPath, anchorPath, "docker-a", "daemon-a", hooks,
	)
	require.NoError(t, err)
	second, err := initializeBoundMarkerPairWithStoresForTest(
		primaryPath, anchorPath, "docker-a", "daemon-a", hooks,
	)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Equal(t, 1, prepareCalls)
	assert.Equal(t, 1, checkCalls)
	assert.Equal(t, 2, verifyCalls)
}

func TestObservedMarkerPublicationSuccessRequiresLocalDirectorySync(t *testing.T) {
	t.Parallel()

	id, err := Parse(canonicalTestID)
	require.NoError(t, err)

	for _, test := range []struct {
		name string
		run  func(boundMarkerEntry) (boundMarkerDurabilityBarrier, error)
	}{
		{
			name: "exact existing publication",
			run: func(marker boundMarkerEntry) (boundMarkerDurabilityBarrier, error) {
				return ensureBoundMarkerRecordTransition(
					marker, "docker-a", "daemon-a", id, "", "",
				)
			},
		},
		{
			name: "already committed finalization",
			run: func(marker boundMarkerEntry) (boundMarkerDurabilityBarrier, error) {
				return finalizeBoundPendingMarkerTransition(
					marker, "docker-a", "daemon-a", id,
				)
			},
		},
		{
			name: "already recovered publication link",
			run: func(marker boundMarkerEntry) (boundMarkerDurabilityBarrier, error) {
				return recoverBoundMarkerTemporaryLinkTransition(marker)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			markerPath := filepath.Join(directory, "marker.json")
			writePairState(t, markerPath, "committed", id)
			marker, err := bindMarkerEntry(markerPath)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, marker.directory.Close()) })

			barrier, err := test.run(marker)
			require.NoError(t, err)
			require.NoError(t, marker.directory.Close())
			err = barrier.sync()

			assert.ErrorIs(t, err, fsidentity.ErrDirectoryClosed)
		})
	}
}

func TestVerifyCommittedMarkerPairWithStoresRecoversAndVerifies(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	id, err := Parse(canonicalTestID)
	require.NoError(t, err)
	writePairState(t, primaryPath, "committed", id)
	writePairState(t, anchorPath, "committed", id)
	temporaryID, err := New()
	require.NoError(t, err)
	temporaryPath := filepath.Join(
		directory,
		"."+filepath.Base(primaryPath)+".tmp-"+temporaryID.String(),
	)
	require.NoError(t, os.Link(primaryPath, temporaryPath))

	verifyCalls := 0
	verifiedID, committed, err := verifyCommittedBoundMarkerPairWithStoresForTest(
		primaryPath, anchorPath, "docker-a", "daemon-a",
		func(storage VerifiedStorage) error {
			verifyCalls++
			assert.Equal(t, id, storage.ID())
			return nil
		},
	)
	require.NoError(t, err)
	assert.True(t, committed)
	assert.Equal(t, id, verifiedID)
	assert.Equal(t, 1, verifyCalls)
	assert.NoFileExists(t, temporaryPath)
}

func TestVerifyCommittedMarkerPairWithStoresNeverInitializesAbsentPair(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	verifyCalls := 0

	verifiedID, committed, err := verifyCommittedBoundMarkerPairWithStoresForTest(
		primaryPath, anchorPath, "docker-a", "daemon-a",
		func(VerifiedStorage) error {
			verifyCalls++
			return nil
		},
	)

	require.NoError(t, err)
	assert.False(t, committed)
	assert.Equal(t, ID{}, verifiedID)
	assert.Zero(t, verifyCalls)
	assert.NoFileExists(t, primaryPath)
	assert.NoFileExists(t, anchorPath)
}

func TestVerifyCommittedMarkerPairWithStoresReattestsAfterStoreVerification(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	id, err := Parse(canonicalTestID)
	require.NoError(t, err)
	replacementID, err := Parse("6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	require.NoError(t, err)
	writePairState(t, primaryPath, "committed", id)
	writePairState(t, anchorPath, "committed", id)

	verifiedID, committed, err := verifyCommittedBoundMarkerPairWithStoresForTest(
		primaryPath, anchorPath, "docker-a", "daemon-a",
		func(storage VerifiedStorage) error {
			assert.Equal(t, id, storage.ID())
			writePairState(t, primaryPath, "committed", replacementID)
			return nil
		},
	)

	assert.ErrorIs(t, err, ErrMarkerBindingMismatch)
	assert.False(t, committed)
	assert.Equal(t, ID{}, verifiedID)
}

func TestInitializeMarkerPairRecoversPublishedTemporaryHardLink(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	id, err := Parse(canonicalTestID)
	require.NoError(t, err)
	writePairState(t, primaryPath, "committed", id)
	writePairState(t, anchorPath, "committed", id)
	temporaryID, err := Parse("6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	require.NoError(t, err)
	temporaryPath := filepath.Join(
		directory,
		"."+filepath.Base(primaryPath)+".tmp-"+temporaryID.String(),
	)
	require.NoError(t, os.Link(primaryPath, temporaryPath))

	loaded, err := initializeBoundMarkerPairForTest(
		primaryPath, anchorPath, "docker-a", "daemon-a",
	)
	require.NoError(t, err)
	assert.Equal(t, id, loaded)
	assert.NoFileExists(t, temporaryPath)
	require.NoError(t, VerifyMarkerPair(
		primaryPath, anchorPath, "docker-a", "daemon-a", id,
	))
}

func TestInitializeMarkerPairRecoveryIgnoresUnrelatedCanonicalTemporaryFiles(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	id, err := Parse(canonicalTestID)
	require.NoError(t, err)
	writePairState(t, primaryPath, "committed", id)
	writePairState(t, anchorPath, "committed", id)
	prefix := "." + filepath.Base(primaryPath) + ".tmp-"
	// Unrelated orphan temporaries are not aliases and therefore do not consume
	// the single recoverable publication-link allowance.
	for range 101 {
		unrelatedID, newErr := New()
		require.NoError(t, newErr)
		require.NoError(t, os.WriteFile(
			filepath.Join(directory, prefix+unrelatedID.String()),
			[]byte("unrelated"),
			0o600,
		))
	}
	publicationID, err := New()
	require.NoError(t, err)
	publicationPath := filepath.Join(directory, prefix+publicationID.String())
	require.NoError(t, os.Link(primaryPath, publicationPath))

	loaded, err := initializeBoundMarkerPairForTest(
		primaryPath, anchorPath, "docker-a", "daemon-a",
	)
	require.NoError(t, err)
	assert.Equal(t, id, loaded)
	assert.NoFileExists(t, publicationPath)
}

func TestInitializeMarkerPairRefusesUnknownHardLinkWithoutMutation(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	id, err := Parse(canonicalTestID)
	require.NoError(t, err)
	writePairState(t, primaryPath, "committed", id)
	writePairState(t, anchorPath, "committed", id)
	unknownLink := filepath.Join(directory, "operator-backup.json")
	require.NoError(t, os.Link(primaryPath, unknownLink))
	primaryBefore, err := os.ReadFile(primaryPath)
	require.NoError(t, err)

	loaded, err := initializeBoundMarkerPairForTest(
		primaryPath, anchorPath, "docker-a", "daemon-a",
	)
	assert.ErrorIs(t, err, ErrInvalidMarker)
	assert.Equal(t, ID{}, loaded)
	assert.FileExists(t, unknownLink)
	primaryAfter, err := os.ReadFile(primaryPath)
	require.NoError(t, err)
	assert.Equal(t, primaryBefore, primaryAfter)
}

func TestMarkerPairRejectsHardLinkedAnchorsWithoutMutation(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	id, err := Parse(canonicalTestID)
	require.NoError(t, err)
	writePairState(t, primaryPath, "committed", id)
	require.NoError(t, os.Link(primaryPath, anchorPath))

	before, err := os.ReadFile(primaryPath)
	require.NoError(t, err)
	loaded, err := LoadMarkerPair(primaryPath, anchorPath, "docker-a", "daemon-a")
	assert.ErrorIs(t, err, ErrInvalidMarker)
	assert.Equal(t, ID{}, loaded)
	initialized, err := initializeBoundMarkerPairForTest(primaryPath, anchorPath, "docker-a", "daemon-a")
	assert.ErrorIs(t, err, ErrInvalidMarker)
	assert.Equal(t, ID{}, initialized)
	assert.ErrorIs(t,
		VerifyMarkerPair(primaryPath, anchorPath, "docker-a", "daemon-a", id),
		ErrInvalidMarker,
	)
	after, err := os.ReadFile(primaryPath)
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

func TestMarkerPairRejectsMarkerWithAnotherHardLink(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	id, err := initializeBoundMarkerPairForTest(primaryPath, anchorPath, "docker-a", "daemon-a")
	require.NoError(t, err)
	require.NoError(t, os.Link(primaryPath, filepath.Join(directory, "hidden-link.json")))

	loaded, err := LoadMarkerPair(primaryPath, anchorPath, "docker-a", "daemon-a")
	assert.ErrorIs(t, err, ErrInvalidMarker)
	assert.Equal(t, ID{}, loaded)
	assert.ErrorIs(t,
		VerifyMarkerPair(primaryPath, anchorPath, "docker-a", "daemon-a", id),
		ErrInvalidMarker,
	)
}

func TestInitializeMarkerPairConcurrentSameBindingPublishesOneIdentity(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	type result struct {
		id  ID
		err error
	}
	const contenders = 32
	start := make(chan struct{})
	results := make(chan result, contenders)
	var group sync.WaitGroup
	for range contenders {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			id, err := initializeBoundMarkerPairForTest(
				primaryPath, anchorPath, "docker-a", "daemon-a",
			)
			results <- result{id: id, err: err}
		}()
	}
	close(start)
	group.Wait()
	close(results)

	var published ID
	successes := 0
	for result := range results {
		if result.err != nil {
			assert.Equal(t, ID{}, result.id)
			continue
		}
		successes++
		require.True(t, result.id.Valid())
		if !published.Valid() {
			published = result.id
		}
		assert.Equal(t, published, result.id, "no contender may publish another lineage")
	}
	require.Positive(t, successes)

	loaded, err := LoadMarkerPair(primaryPath, anchorPath, "docker-a", "daemon-a")
	require.NoError(t, err)
	assert.Equal(t, published, loaded)
}

func TestInitializeMarkerPairConcurrentDifferentBindingsNeverOverwriteWinner(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	type result struct {
		substrate string
		id        ID
		err       error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var group sync.WaitGroup
	for _, substrate := range []string{"daemon-a", "daemon-b"} {
		substrate := substrate
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			id, err := initializeBoundMarkerPairForTest(
				primaryPath, anchorPath, "docker-a", substrate,
			)
			results <- result{substrate: substrate, id: id, err: err}
		}()
	}
	close(start)
	group.Wait()
	close(results)

	var winner result
	successes := 0
	for result := range results {
		if result.err != nil {
			assert.Equal(t, ID{}, result.id)
			continue
		}
		successes++
		winner = result
	}
	require.Equal(t, 1, successes, "different physical bindings cannot both adopt one pair")
	require.True(t, winner.id.Valid())

	loaded, err := LoadMarkerPair(
		primaryPath, anchorPath, "docker-a", winner.substrate,
	)
	require.NoError(t, err)
	assert.Equal(t, winner.id, loaded)
	loserSubstrate := "daemon-a"
	if winner.substrate == loserSubstrate {
		loserSubstrate = "daemon-b"
	}
	_, err = LoadMarkerPair(primaryPath, anchorPath, "docker-a", loserSubstrate)
	assert.ErrorIs(t, err, ErrMarkerBindingMismatch)
}

func TestLoadMarkerPairPendingStatesAreReadOnly(t *testing.T) {
	t.Parallel()

	id, err := Parse(canonicalTestID)
	require.NoError(t, err)
	for _, test := range []struct {
		name         string
		primaryState string
	}{
		{name: "primary absent", primaryState: "absent"},
		{name: "primary committed", primaryState: "committed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			primaryPath := filepath.Join(directory, "primary.json")
			anchorPath := filepath.Join(directory, "anchor.json")
			writePairState(t, primaryPath, test.primaryState, id)
			writePairState(t, anchorPath, "pending", id)
			anchorBefore, err := os.ReadFile(anchorPath)
			require.NoError(t, err)
			var primaryBefore []byte
			if test.primaryState != "absent" {
				primaryBefore, err = os.ReadFile(primaryPath)
				require.NoError(t, err)
			}

			loaded, err := LoadMarkerPair(primaryPath, anchorPath, "docker-a", "daemon-a")
			assert.ErrorIs(t, err, ErrInvalidMarker)
			assert.Equal(t, ID{}, loaded)
			anchorAfter, err := os.ReadFile(anchorPath)
			require.NoError(t, err)
			assert.Equal(t, anchorBefore, anchorAfter, "ordinary startup must not commit pending intent")
			if test.primaryState == "absent" {
				_, statErr := os.Lstat(primaryPath)
				assert.ErrorIs(t, statErr, os.ErrNotExist)
			} else {
				primaryAfter, readErr := os.ReadFile(primaryPath)
				require.NoError(t, readErr)
				assert.Equal(t, primaryBefore, primaryAfter)
			}
		})
	}
}

func TestVerifyMarkerPairIsReadOnlyAndChecksBothBindings(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	primaryPath := filepath.Join(directory, "primary.json")
	anchorPath := filepath.Join(directory, "anchor.json")
	id, err := initializeBoundMarkerPairForTest(primaryPath, anchorPath, "docker-a", "daemon-a")
	require.NoError(t, err)
	other, err := Parse("6ba7b811-9dad-41d1-80b4-00c04fd430c8")
	require.NoError(t, err)
	primaryBefore, err := os.ReadFile(primaryPath)
	require.NoError(t, err)
	anchorBefore, err := os.ReadFile(anchorPath)
	require.NoError(t, err)

	require.NoError(t, VerifyMarkerPair(
		primaryPath, anchorPath, "docker-a", "daemon-a", id,
	))
	assert.ErrorIs(t, VerifyMarkerPair(
		primaryPath, anchorPath, "docker-a", "daemon-a", other,
	), ErrMarkerBindingMismatch)
	assert.ErrorIs(t, VerifyMarkerPair(
		primaryPath, anchorPath, "docker-b", "daemon-a", id,
	), ErrMarkerBindingMismatch)
	assert.ErrorIs(t, VerifyMarkerPair(
		primaryPath, anchorPath, "docker-a", "daemon-b", id,
	), ErrMarkerBindingMismatch)

	primaryAfter, err := os.ReadFile(primaryPath)
	require.NoError(t, err)
	anchorAfter, err := os.ReadFile(anchorPath)
	require.NoError(t, err)
	assert.Equal(t, primaryBefore, primaryAfter)
	assert.Equal(t, anchorBefore, anchorAfter)
}

func TestMarkerPairMalformedSymlinkAndMissingStatesNeverMutate(t *testing.T) {
	t.Parallel()

	id, err := Parse(canonicalTestID)
	require.NoError(t, err)

	t.Run("malformed primary", func(t *testing.T) {
		directory := t.TempDir()
		primaryPath := filepath.Join(directory, "primary.json")
		anchorPath := filepath.Join(directory, "anchor.json")
		require.NoError(t, os.WriteFile(primaryPath, []byte(`{"schema":`), 0o600))
		writePairState(t, anchorPath, "committed", id)
		primaryBefore, err := os.ReadFile(primaryPath)
		require.NoError(t, err)
		anchorBefore, err := os.ReadFile(anchorPath)
		require.NoError(t, err)

		loaded, loadErr := LoadMarkerPair(primaryPath, anchorPath, "docker-a", "daemon-a")
		assert.ErrorIs(t, loadErr, ErrInvalidMarker)
		assert.Equal(t, ID{}, loaded)
		initialized, initializeErr := initializeBoundMarkerPairForTest(primaryPath, anchorPath, "docker-a", "daemon-a")
		assert.ErrorIs(t, initializeErr, ErrInvalidMarker)
		assert.Equal(t, ID{}, initialized)
		assert.ErrorIs(t, VerifyMarkerPair(
			primaryPath, anchorPath, "docker-a", "daemon-a", id,
		), ErrInvalidMarker)

		primaryAfter, err := os.ReadFile(primaryPath)
		require.NoError(t, err)
		anchorAfter, err := os.ReadFile(anchorPath)
		require.NoError(t, err)
		assert.Equal(t, primaryBefore, primaryAfter)
		assert.Equal(t, anchorBefore, anchorAfter)
	})

	t.Run("symlink primary", func(t *testing.T) {
		directory := t.TempDir()
		targetPath := filepath.Join(directory, "target.json")
		primaryPath := filepath.Join(directory, "primary.json")
		anchorPath := filepath.Join(directory, "anchor.json")
		writePairState(t, targetPath, "committed", id)
		writePairState(t, anchorPath, "committed", id)
		require.NoError(t, os.Symlink(targetPath, primaryPath))
		targetBefore, err := os.ReadFile(targetPath)
		require.NoError(t, err)
		anchorBefore, err := os.ReadFile(anchorPath)
		require.NoError(t, err)

		loaded, loadErr := LoadMarkerPair(primaryPath, anchorPath, "docker-a", "daemon-a")
		assert.ErrorIs(t, loadErr, ErrInvalidMarker)
		assert.Equal(t, ID{}, loaded)
		initialized, initializeErr := initializeBoundMarkerPairForTest(primaryPath, anchorPath, "docker-a", "daemon-a")
		assert.ErrorIs(t, initializeErr, ErrInvalidMarker)
		assert.Equal(t, ID{}, initialized)
		assert.ErrorIs(t, VerifyMarkerPair(
			primaryPath, anchorPath, "docker-a", "daemon-a", id,
		), ErrInvalidMarker)

		linkTarget, err := os.Readlink(primaryPath)
		require.NoError(t, err)
		assert.Equal(t, targetPath, linkTarget)
		targetAfter, err := os.ReadFile(targetPath)
		require.NoError(t, err)
		anchorAfter, err := os.ReadFile(anchorPath)
		require.NoError(t, err)
		assert.Equal(t, targetBefore, targetAfter)
		assert.Equal(t, anchorBefore, anchorAfter)
	})

	t.Run("one missing", func(t *testing.T) {
		directory := t.TempDir()
		primaryPath := filepath.Join(directory, "primary.json")
		anchorPath := filepath.Join(directory, "anchor.json")
		writePairState(t, primaryPath, "committed", id)
		primaryBefore, err := os.ReadFile(primaryPath)
		require.NoError(t, err)

		loaded, loadErr := LoadMarkerPair(primaryPath, anchorPath, "docker-a", "daemon-a")
		assert.Error(t, loadErr)
		assert.Equal(t, ID{}, loaded)
		initialized, initializeErr := initializeBoundMarkerPairForTest(primaryPath, anchorPath, "docker-a", "daemon-a")
		assert.Error(t, initializeErr)
		assert.Equal(t, ID{}, initialized)
		assert.Error(t, VerifyMarkerPair(
			primaryPath, anchorPath, "docker-a", "daemon-a", id,
		))

		primaryAfter, err := os.ReadFile(primaryPath)
		require.NoError(t, err)
		assert.Equal(t, primaryBefore, primaryAfter)
		_, statErr := os.Lstat(anchorPath)
		assert.ErrorIs(t, statErr, os.ErrNotExist)
	})
}

// These helpers deliberately acquire a fresh bound parent capability for every
// invocation. In particular, concurrent tests exercise the same production
// acquisition and descriptor-relative mutating state machine used by each
// competing initializer; no pathname mutator exists in non-test code.
func initializeBoundMarkerPairForTest(
	primaryPath, anchorPath, backendName, substrateID string,
) (ID, error) {
	hooks := MarkerPairStoreHooks{
		Profile: InitializationProfileFresh,
		Prepare: func(PendingStorage, InitializationProfile) error { return nil },
		Check:   func(PendingStorage) error { return nil },
		Verify:  func(VerifiedStorage) error { return nil },
	}
	return initializeBoundMarkerPairWithStoresForTest(
		primaryPath, anchorPath, backendName, substrateID, hooks,
	)
}

func initializeBoundMarkerPairWithStoresForTest(
	primaryPath, anchorPath, backendName, substrateID string,
	hooks MarkerPairStoreHooks,
) (result ID, resultErr error) {
	pair, err := BindMarkerPair(primaryPath, anchorPath)
	if err != nil {
		return ID{}, err
	}
	defer func() {
		if closeErr := pair.Close(); resultErr == nil && closeErr != nil {
			result = ID{}
			resultErr = closeErr
		}
	}()
	storage, err := pair.InitializeWithStores(backendName, substrateID, hooks)
	if err != nil {
		return ID{}, err
	}
	return storage.ID(), nil
}

func pendingBoundMarkerPairInitializationProfileForTest(
	primaryPath, anchorPath, backendName, substrateID string,
) (profile InitializationProfile, pending bool, resultErr error) {
	pair, err := BindMarkerPair(primaryPath, anchorPath)
	if err != nil {
		return "", false, err
	}
	defer func() {
		if closeErr := pair.Close(); resultErr == nil && closeErr != nil {
			profile = ""
			pending = false
			resultErr = closeErr
		}
	}()
	return pair.PendingInitializationProfile(backendName, substrateID)
}

func verifyCommittedBoundMarkerPairWithStoresForTest(
	primaryPath, anchorPath, backendName, substrateID string,
	verify func(VerifiedStorage) error,
) (id ID, committed bool, resultErr error) {
	pair, err := BindMarkerPair(primaryPath, anchorPath)
	if err != nil {
		return ID{}, false, err
	}
	defer func() {
		if closeErr := pair.Close(); resultErr == nil && closeErr != nil {
			id = ID{}
			committed = false
			resultErr = closeErr
		}
	}()
	return pair.VerifyCommittedWithStores(backendName, substrateID, verify)
}

func writePairState[T ~string](t *testing.T, path string, state T, id ID) {
	t.Helper()
	if string(state) == "absent" {
		return
	}
	if string(state) == "invalid" {
		require.NoError(t, os.WriteFile(path, []byte(`{"schema":1,"state":"unknown"}`), 0o600))
		return
	}
	record := markerRecord{
		Schema:      markerSchemaVersion,
		StorageID:   id.String(),
		BackendName: "docker-a",
		SubstrateID: "daemon-a",
	}
	if string(state) == "pending" {
		record.State = markerStatePending
		record.InitializationProfile = string(InitializationProfileFresh)
	}
	data, err := json.Marshal(record)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
}

func writeProfilelessPendingPairState(t *testing.T, path string, id ID) {
	t.Helper()
	data, err := json.Marshal(markerRecord{
		Schema:      markerSchemaVersion,
		StorageID:   id.String(),
		BackendName: "docker-a",
		SubstrateID: "daemon-a",
		State:       markerStatePending,
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
}
