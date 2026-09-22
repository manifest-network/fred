package placement

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/fsidentity"
)

const freshTestProviderUUID = "44ed3e51-6912-4f6f-8f29-ac2fdd4455d4"

type freshTestChainSnapshot struct {
	valid    bool
	provider string
	height   int64
	total    int
	blocking int
}

func (snapshot freshTestChainSnapshot) Valid() bool             { return snapshot.valid }
func (snapshot freshTestChainSnapshot) ProviderUUID() string    { return snapshot.provider }
func (snapshot freshTestChainSnapshot) BlockHeight() int64      { return snapshot.height }
func (snapshot freshTestChainSnapshot) TotalLeases() int        { return snapshot.total }
func (snapshot freshTestChainSnapshot) BlockingLeaseCount() int { return snapshot.blocking }

func TestInitializeFreshStorePublishesPreparedAuthority(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "placements.db")
	plan := freshTestPlan(t, path, []string{"backend-b", "backend-a"})

	require.NoError(t, InitializeFreshStoreContext(t.Context(), plan))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	store, err := OpenStore(path, freshTestProviderUUID)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.VerifyBackendTopology([]string{"backend-a", "backend-b"}))
	require.True(t, store.CurrentAdmissionBaseline().Valid())
	require.Equal(t, StateAbsent, store.Lookup("11638ef8-1401-4f14-a355-1ae02afeb35b").State())
}

func TestOpenStoreBindsAuthorityToFreshProofProvider(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "placements.db")
	require.NoError(t, InitializeFreshStoreContext(
		t.Context(), freshTestPlan(t, path, []string{"backend-a"}),
	))
	otherProvider := "1e1698c3-a922-460a-8296-70efdbc03032"
	store, err := OpenStore(path, otherProvider)
	require.ErrorIs(t, err, ErrProviderAuthorityMismatch)
	require.Nil(t, store)

	store, err = OpenStore(path, freshTestProviderUUID)
	require.NoError(t, err)
	require.NoError(t, store.VerifyProviderUUID(freshTestProviderUUID))
	require.ErrorIs(t, store.VerifyProviderUUID(otherProvider), ErrProviderAuthorityMismatch)
	require.NoError(t, store.Close())
}

func TestInitializeFreshStoreNeverReplacesExistingPath(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	path := filepath.Join(directory, "placements.db")
	original := []byte("operator-owned bytes")
	require.NoError(t, os.WriteFile(path, original, 0o600))

	err := InitializeFreshStoreContext(t.Context(), freshTestPlan(t, path, []string{"backend-a"}))
	require.ErrorIs(t, err, ErrPlacementStoreExists)
	contents, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, original, contents)
}

func TestInitializeFreshStorePublicationRaceHasOneWinner(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "placements.db")
	plans := []*FreshInitializationPlan{
		freshTestPlan(t, path, []string{"backend-a"}),
		freshTestPlan(t, path, []string{"backend-a"}),
	}
	start := make(chan struct{})
	errorsByCaller := make(chan error, 2)
	var workers sync.WaitGroup
	for index := range 2 {
		workers.Go(func() {
			<-start
			errorsByCaller <- InitializeFreshStoreContext(t.Context(), plans[index])
		})
	}
	close(start)
	workers.Wait()
	close(errorsByCaller)

	var successes, existsFailures int
	for err := range errorsByCaller {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrPlacementStoreExists):
			existsFailures++
		default:
			t.Fatalf("unexpected initialization result: %v", err)
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, 1, existsFailures)

	store, err := OpenStore(path, freshTestProviderUUID)
	require.NoError(t, err)
	require.NoError(t, store.Close())
}

func TestFreshChainProofFailsClosed(t *testing.T) {
	t.Parallel()

	valid := freshTestChainSnapshot{valid: true, provider: freshTestProviderUUID, height: 912}
	tests := map[string]freshTestChainSnapshot{
		"invalid snapshot":     {provider: freshTestProviderUUID, height: 912},
		"invalid provider":     {valid: true, provider: "not-a-uuid", height: 912},
		"zero height":          {valid: true, provider: freshTestProviderUUID},
		"negative total":       {valid: true, provider: freshTestProviderUUID, height: 912, total: -1},
		"negative blocking":    {valid: true, provider: freshTestProviderUUID, height: 912, blocking: -1},
		"blocking exceeds all": {valid: true, provider: freshTestProviderUUID, height: 912, total: 1, blocking: 2},
		"active lease":         {valid: true, provider: freshTestProviderUUID, height: 912, total: 7, blocking: 1},
		"terminal history":     {valid: true, provider: freshTestProviderUUID, height: 912, total: 7},
	}
	for name, snapshot := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			proof, err := NewFreshChainProof(snapshot)
			require.ErrorIs(t, err, ErrFreshInitializationProof)
			require.False(t, proof.Valid())
		})
	}
	proof, err := NewFreshChainProof(valid)
	require.NoError(t, err)
	require.True(t, proof.Valid())
}

func TestFreshBackendProofRequiresCompleteEmptyUniqueFleet(t *testing.T) {
	t.Parallel()

	idA := freshTestStorageID(t, "1a8e7e65-6a8e-4552-8d20-a5f7e86c10ba")
	idB := freshTestStorageID(t, "40a0ddad-ff86-4d18-980f-cd9cafc75561")
	emptyA := BackendInventory{
		StorageIdentity: idA, Provisions: []string{}, ProvisionProviderUUIDs: map[string]string{},
		Retentions: []string{},
	}
	emptyB := BackendInventory{
		StorageIdentity: idB, Provisions: []string{}, ProvisionProviderUUIDs: map[string]string{},
		Retentions: []string{},
	}
	tests := map[string]map[string]BackendInventory{
		"missing backend": {"backend-a": emptyA},
		"extra backend": {
			"backend-a": emptyA, "backend-b": emptyB, "backend-c": emptyB,
		},
		"nil provisions": {
			"backend-a": {
				StorageIdentity: idA, ProvisionProviderUUIDs: map[string]string{}, Retentions: []string{},
			},
			"backend-b": emptyB,
		},
		"nil retentions": {
			"backend-a": {
				StorageIdentity: idA, Provisions: []string{}, ProvisionProviderUUIDs: map[string]string{},
			},
			"backend-b": emptyB,
		},
		"nil provider observations": {
			"backend-a": {
				StorageIdentity: idA, Provisions: []string{}, Retentions: []string{},
			},
			"backend-b": emptyB,
		},
		"provider observation without provision": {
			"backend-a": {
				StorageIdentity: idA, Provisions: []string{},
				ProvisionProviderUUIDs: map[string]string{"lease": freshTestProviderUUID},
				Retentions:             []string{},
			},
			"backend-b": emptyB,
		},
		"live provision": {
			"backend-a": {
				StorageIdentity: idA, Provisions: []string{"lease"},
				ProvisionProviderUUIDs: map[string]string{"lease": freshTestProviderUUID},
				Retentions:             []string{},
			},
			"backend-b": emptyB,
		},
		"live retention": {
			"backend-a": {
				StorageIdentity: idA, Provisions: []string{}, ProvisionProviderUUIDs: map[string]string{},
				Retentions: []string{"lease"},
			},
			"backend-b": emptyB,
		},
		"duplicate identity": {
			"backend-a": emptyA,
			"backend-b": {
				StorageIdentity: idA, Provisions: []string{}, ProvisionProviderUUIDs: map[string]string{},
				Retentions: []string{},
			},
		},
	}
	for name, inventories := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			proof, err := NewFreshBackendProof([]string{"backend-a", "backend-b"}, inventories)
			require.ErrorIs(t, err, ErrFreshInitializationProof)
			require.False(t, proof.Valid())
		})
	}

	proof, err := NewFreshBackendProof(
		[]string{"backend-b", "backend-a"},
		map[string]BackendInventory{"backend-a": emptyA, "backend-b": emptyB},
	)
	require.NoError(t, err)
	require.True(t, proof.Valid())
}

func TestFreshInitializationPlanRejectsZeroAndWrongConfirmation(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "placements.db")
	target, err := NewFreshInitializationTarget(
		path, freshTestProviderUUID, []string{"backend-a"},
	)
	require.NoError(t, err)
	confirmation, err := ConfirmFreshQuiescence(target, "yes")
	require.ErrorIs(t, err, ErrFreshInitializationProof)
	require.False(t, confirmation.confirmed)

	confirmation, err = ConfirmFreshQuiescence(target, target.Confirmation())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	_, err = NewFreshInitializationPlan(
		ctx, target, FreshChainProof{}, FreshBackendProof{}, confirmation,
	)
	require.ErrorIs(t, err, ErrFreshInitializationProof)

	require.ErrorIs(t, InitializeFreshStoreContext(t.Context(), nil), ErrFreshInitializationProof)
	_, statErr := os.Lstat(path)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestFreshInitializationTargetBindsCanonicalPathProviderAndIndependentRoster(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	alias := filepath.Join(directory, "alias")
	realDirectory := filepath.Join(directory, "authority")
	require.NoError(t, os.Mkdir(realDirectory, 0o700))
	require.NoError(t, os.Symlink(realDirectory, alias))
	target, err := NewFreshInitializationTarget(
		filepath.Join(alias, "placements.db"),
		freshTestProviderUUID,
		[]string{"backend-b", "backend-a"},
	)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(realDirectory, "placements.db"), target.DatabasePath())
	require.Equal(t, freshTestProviderUUID, target.ProviderUUID())
	require.Equal(t, []string{"backend-a", "backend-b"}, target.BackendNames())
	require.Equal(t,
		fmt.Sprintf(
			`I CONFIRM FRESH PLACEMENT INITIALIZATION target="%s" parent={"device":%d,"inode":%d} provider="%s" backends=["backend-a","backend-b"]; THE PROVIDER AND ALL BACKENDS ARE QUIESCED`,
			filepath.Join(realDirectory, "placements.db"),
			target.parentIdentity.Device,
			target.parentIdentity.Inode,
			freshTestProviderUUID,
		),
		target.Confirmation(),
	)

	mutatedRoster := target.BackendNames()
	mutatedRoster[0] = "different"
	require.Equal(t, []string{"backend-a", "backend-b"}, target.BackendNames())
}

func TestFreshInitializationConfirmationBindsPhysicalParentAcrossInvocations(t *testing.T) {
	root := t.TempDir()
	parentPath := filepath.Join(root, "authority")
	movedPath := filepath.Join(root, "authority-before")
	require.NoError(t, os.Mkdir(parentPath, 0o700))
	dbPath := filepath.Join(parentPath, "placements.db")

	before, err := NewFreshInitializationTarget(
		dbPath,
		freshTestProviderUUID,
		[]string{"backend-a"},
	)
	require.NoError(t, err)
	statement := before.Confirmation()

	require.NoError(t, os.Rename(parentPath, movedPath))
	require.NoError(t, os.Mkdir(parentPath, 0o700))
	after, err := NewFreshInitializationTarget(
		dbPath,
		freshTestProviderUUID,
		[]string{"backend-a"},
	)
	require.NoError(t, err)
	require.NotEqual(t, before.parentIdentity, after.parentIdentity)
	require.NotEqual(t, statement, after.Confirmation(),
		"the printed acknowledgement must not survive parent replacement")
	_, err = ConfirmFreshQuiescence(after, statement)
	require.ErrorIs(t, err, ErrFreshInitializationProof)
}

func TestInitializeFreshStoreRejectsParentReplacementDuringProofWindow(t *testing.T) {
	root := t.TempDir()
	parentPath := filepath.Join(root, "authority")
	movedPath := filepath.Join(root, "authority-before")
	require.NoError(t, os.Mkdir(parentPath, 0o700))
	dbPath := filepath.Join(parentPath, "placements.db")
	plan := freshTestPlan(t, dbPath, []string{"backend-a"})

	require.NoError(t, os.Rename(parentPath, movedPath))
	require.NoError(t, os.Mkdir(parentPath, 0o700))
	err := InitializeFreshStoreContext(t.Context(), plan)
	require.ErrorIs(t, err, ErrFreshInitializationProof)
	require.ErrorContains(t, err, "directory identity changed")
	for _, path := range []string{
		dbPath,
		filepath.Join(movedPath, filepath.Base(dbPath)),
	} {
		_, statErr := os.Lstat(path)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	}
	replacementEntries, err := os.ReadDir(parentPath)
	require.NoError(t, err)
	require.Empty(t, replacementEntries)
	originalEntries, err := os.ReadDir(movedPath)
	require.NoError(t, err)
	require.Empty(t, originalEntries)
}

func TestFreshInitializationPlanRejectsConfiguredFleetDifferentFromIndependentRoster(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "placements.db")
	chainProof, err := NewFreshChainProof(freshTestChainSnapshot{
		valid: true, provider: freshTestProviderUUID, height: 912,
	})
	require.NoError(t, err)
	id := freshTestStorageID(t, "1a8e7e65-6a8e-4552-8d20-a5f7e86c10ba")
	backendProof, err := NewFreshBackendProof(
		[]string{"backend-a"},
		map[string]BackendInventory{
			"backend-a": {
				StorageIdentity:        id,
				Provisions:             []string{},
				ProvisionProviderUUIDs: map[string]string{},
				Retentions:             []string{},
			},
		},
	)
	require.NoError(t, err)
	target, err := NewFreshInitializationTarget(
		path, freshTestProviderUUID, []string{"backend-a", "backend-b"},
	)
	require.NoError(t, err)
	quiescence, err := ConfirmFreshQuiescence(target, target.Confirmation())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	plan, err := NewFreshInitializationPlan(ctx, target, chainProof, backendProof, quiescence)
	require.ErrorIs(t, err, ErrFreshInitializationProof)
	require.ErrorContains(t, err, "does not match independently supplied roster")
	require.Nil(t, plan)
}

func TestInitializeFreshStoreCapabilityIsSingleUse(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "placements.db")
	plan := freshTestPlan(t, path, []string{"backend-a"})
	require.NoError(t, InitializeFreshStoreContext(t.Context(), plan))
	require.ErrorIs(
		t,
		InitializeFreshStoreContext(t.Context(), plan),
		ErrFreshInitializationConsumed,
	)
}

func TestInitializeFreshStoreCopiedCapabilitySharesSingleUseState(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "placements.db")
	plan := freshTestPlan(t, path, []string{"backend-a"})
	copied := *plan
	require.NoError(t, InitializeFreshStoreContext(t.Context(), plan))
	require.ErrorIs(
		t,
		InitializeFreshStoreContext(t.Context(), &copied),
		ErrFreshInitializationConsumed,
		"copying an opaque plan value must not duplicate its authority",
	)
}

func TestInitializeFreshStoreCancellationBeforePublicationLeavesTargetAbsent(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "placements.db")
	plan := freshTestPlan(t, path, []string{"backend-a"})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	linkCalled := false
	err := initializeFreshStoreContext(ctx, plan, func(
		*fsidentity.Directory, string, string,
	) error {
		linkCalled = true
		return errors.New("publication must not be attempted")
	})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, linkCalled)
	_, statErr := os.Lstat(path)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestInitializeFreshStoreCancellationAfterPublicationIsCategoricallyPublished(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "placements.db")
	plan := freshTestPlan(t, path, []string{"backend-a"})
	ctx, cancel := context.WithCancel(t.Context())
	err := initializeFreshStoreContext(ctx, plan, func(
		parent *fsidentity.Directory, oldName, newName string,
	) error {
		if err := parent.LinkNoReplace(oldName, newName); err != nil {
			return err
		}
		cancel()
		return nil
	})
	require.ErrorIs(t, err, ErrFreshInitializationPublished)
	require.ErrorIs(t, err, context.Canceled)
	_, statErr := os.Stat(path)
	require.NoError(t, statErr)
}

func TestInitializeFreshStoreCancellationAtPublicationLeavesTargetAbsent(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "placements.db")
	plan := freshTestPlan(t, path, []string{"backend-a"})
	ctx, cancel := context.WithCancel(t.Context())
	err := initializeFreshStoreContext(ctx, plan, func(
		_ *fsidentity.Directory, _, _ string,
	) error {
		cancel()
		return ctx.Err()
	})
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrFreshInitializationPublished)
	_, statErr := os.Lstat(path)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestInitializeFreshStoreExpiredProofLeavesTargetAbsent(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "placements.db")
	plan := freshTestPlan(t, path, []string{"backend-a"})
	plan.expiresAt = time.Now().Add(-time.Second)
	err := InitializeFreshStoreContext(t.Context(), plan)
	require.ErrorIs(t, err, ErrFreshInitializationExpired)
	_, statErr := os.Lstat(path)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestNewFreshInitializationTargetRejectsRelativeAuthorityPath(t *testing.T) {
	t.Parallel()

	target, err := NewFreshInitializationTarget(
		"placements.db", freshTestProviderUUID, []string{"backend-a"},
	)
	require.ErrorIs(t, err, ErrFreshInitializationProof)
	require.ErrorContains(t, err, "absolute")
	require.False(t, target.Valid())
}

func TestNewFreshInitializationPlanCapsLongCallerDeadline(t *testing.T) {
	t.Parallel()

	seed := freshTestPlan(
		t, filepath.Join(t.TempDir(), "seed.db"), []string{"backend-a"},
	)
	target, err := NewFreshInitializationTarget(
		filepath.Join(t.TempDir(), "placements.db"),
		freshTestProviderUUID,
		[]string{"backend-a"},
	)
	require.NoError(t, err)
	quiescence, err := ConfirmFreshQuiescence(target, target.Confirmation())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	t.Cleanup(cancel)
	started := time.Now()
	plan, err := NewFreshInitializationPlan(
		ctx, target, seed.chain, seed.backends, quiescence,
	)
	finished := time.Now()
	require.NoError(t, err)
	require.False(t, plan.expiresAt.After(finished.Add(freshInitializationProofMaxAge)),
		"the package maximum must override a longer caller deadline")
	require.True(t, plan.expiresAt.After(started.Add(freshInitializationProofMaxAge-time.Second)),
		"the fixed cap should start when the package mints the plan")
}

func TestInitializeFreshStoreRejectsPublicationThatIsNotTheVerifiedInode(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "placements.db")
	plan := freshTestPlan(t, path, []string{"backend-a"})
	err := initializeFreshStoreContext(t.Context(), plan, func(
		parent *fsidentity.Directory, oldName, newName string,
	) error {
		contents, readErr := os.ReadFile(parent.DisplayPath(oldName))
		if readErr != nil {
			return readErr
		}
		return os.WriteFile(parent.DisplayPath(newName), contents, 0o600)
	})
	require.ErrorIs(t, err, ErrFreshInitializationPublished)
	require.ErrorContains(t, err, "source inode")
}

func TestInitializeFreshStorePublicationCannotMoveIntoReplacementParent(t *testing.T) {
	root := t.TempDir()
	parentPath := filepath.Join(root, "authority")
	movedPath := filepath.Join(root, "authority-bound")
	replacementPath := filepath.Join(root, "authority-replacement")
	require.NoError(t, os.Mkdir(parentPath, 0o700))
	require.NoError(t, os.Mkdir(replacementPath, 0o700))
	dbPath := filepath.Join(parentPath, "placements.db")
	decoy := []byte("replacement-owned bytes")
	require.NoError(t, os.WriteFile(
		filepath.Join(replacementPath, filepath.Base(dbPath)),
		decoy,
		0o600,
	))
	plan := freshTestPlan(t, dbPath, []string{"backend-a"})

	err := initializeFreshStoreContext(t.Context(), plan, func(
		parent *fsidentity.Directory, oldName, newName string,
	) error {
		require.NoError(t, os.Rename(parentPath, movedPath))
		require.NoError(t, os.Rename(replacementPath, parentPath))
		return parent.LinkNoReplace(oldName, newName)
	})
	require.ErrorIs(t, err, ErrFreshInitializationPublished)
	require.ErrorIs(t, err, ErrFreshInitializationProof)
	require.ErrorContains(t, err, "changed after publication")

	contents, readErr := os.ReadFile(dbPath)
	require.NoError(t, readErr)
	require.Equal(t, decoy, contents,
		"descriptor-relative publication must not overwrite or enter the replacement")

	publishedPath := filepath.Join(movedPath, filepath.Base(dbPath))
	store, openErr := OpenStore(publishedPath, freshTestProviderUUID)
	require.NoError(t, openErr)
	require.NoError(t, store.Close())
}

func TestInitializeFreshStoreTreatsDanglingDestinationSymlinkAsExisting(t *testing.T) {
	t.Parallel()

	parentPath := t.TempDir()
	path := filepath.Join(parentPath, "placements.db")
	require.NoError(t, os.Symlink("missing-authority.db", path))
	err := InitializeFreshStoreContext(
		t.Context(),
		freshTestPlan(t, path, []string{"backend-a"}),
	)
	require.ErrorIs(t, err, ErrPlacementStoreExists)
	info, statErr := os.Lstat(path)
	require.NoError(t, statErr)
	require.NotZero(t, info.Mode()&os.ModeSymlink)
}

func TestInitializeFreshStoreReopensExactPublishedAuthority(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "placements.db")
	plan := freshTestPlan(t, path, []string{"backend-a"})
	err := initializeFreshStoreContext(t.Context(), plan, func(
		parent *fsidentity.Directory, oldName, newName string,
	) error {
		if linkErr := parent.LinkNoReplace(oldName, newName); linkErr != nil {
			return linkErr
		}
		db, openErr := bolt.Open(parent.DisplayPath(newName), 0o600, nil)
		if openErr != nil {
			return openErr
		}
		encoded, encodeErr := encodePlacement(Placement{
			Backend: "backend-a", SetAt: time.Now().UTC(), revision: 1,
		})
		if encodeErr == nil {
			encodeErr = db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(bucketName).Put(
					[]byte("11638ef8-1401-4f14-a355-1ae02afeb35b"), encoded,
				)
			})
		}
		return errors.Join(encodeErr, db.Close())
	})
	require.ErrorIs(t, err, ErrFreshInitializationPublished)
	require.ErrorContains(t, err, "semantic postcondition")
	require.ErrorContains(t, err, "is not empty")
}

func TestVerifyFreshInitializationPostconditionRejectsAuthorityDrift(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		mutate func(*bolt.Tx) error
		want   string
	}{
		"storage identity pin": {
			mutate: func(tx *bolt.Tx) error {
				metadata, err := loadTopologyMetadata(tx)
				if err != nil {
					return err
				}
				metadata.KnownBackendStorageIDs["backend-a"] =
					"40a0ddad-ff86-4d18-980f-cd9cafc75561"
				return putTopologyMetadata(tx, metadata)
			},
			want: "metadata differs",
		},
		"unexpected authority bucket": {
			mutate: func(tx *bolt.Tx) error {
				_, err := tx.CreateBucket([]byte("foreign_authority"))
				return err
			},
			want: "unexpected top-level",
		},
		"placement row": {
			mutate: func(tx *bolt.Tx) error {
				encoded, err := encodePlacement(Placement{
					Backend: "backend-a", SetAt: time.Now().UTC(), revision: 1,
				})
				if err != nil {
					return err
				}
				return tx.Bucket(bucketName).Put(
					[]byte("11638ef8-1401-4f14-a355-1ae02afeb35b"), encoded,
				)
			},
			want: "is not empty",
		},
		"lifecycle row": {
			mutate: func(tx *bolt.Tx) error {
				encoded, err := encodeLifecycleCapability(lifecycleCapability{backend: "backend-a"})
				if err != nil {
					return err
				}
				return tx.Bucket(lifecycleCapabilityBucketName).Put(
					[]byte("11638ef8-1401-4f14-a355-1ae02afeb35b"), encoded,
				)
			},
			want: "is not empty",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "placements.db")
			plan := freshTestPlan(t, path, []string{"backend-a"})
			require.NoError(t, InitializeFreshStoreContext(t.Context(), plan))
			db, err := bolt.Open(path, 0o600, nil)
			require.NoError(t, err)
			require.NoError(t, db.Update(test.mutate))
			require.NoError(t, db.Close())

			db, err = bolt.Open(path, 0o600, &bolt.Options{ReadOnly: true})
			require.NoError(t, err)
			err = verifyFreshInitializationPostcondition(db, plan)
			require.ErrorContains(t, err, test.want)
			require.NoError(t, db.Close())
		})
	}
}

func TestNewFreshInitializationPlanRequiresLiveBoundedContext(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "placements.db")
	validPlan := freshTestPlan(t, path, []string{"backend-a"})
	target := validPlan.target
	chainProof := validPlan.chain
	backendProof := validPlan.backends
	quiescence := validPlan.quiescence

	plan, err := NewFreshInitializationPlan(
		context.Background(), target, chainProof, backendProof, quiescence,
	)
	require.ErrorIs(t, err, ErrFreshInitializationProof)
	require.ErrorContains(t, err, "requires a live deadline")
	require.Nil(t, plan)

	canceledCtx, cancel := context.WithCancel(t.Context())
	cancel()
	plan, err = NewFreshInitializationPlan(
		canceledCtx, target, chainProof, backendProof, quiescence,
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, plan)
}

func freshTestPlan(
	t *testing.T,
	path string,
	backendNames []string,
) *FreshInitializationPlan {
	t.Helper()
	chainProof, err := NewFreshChainProof(freshTestChainSnapshot{
		valid: true, provider: freshTestProviderUUID, height: 912,
	})
	require.NoError(t, err)
	inventories := make(map[string]BackendInventory, len(backendNames))
	ids := []string{
		"1a8e7e65-6a8e-4552-8d20-a5f7e86c10ba",
		"40a0ddad-ff86-4d18-980f-cd9cafc75561",
		"94c6bed4-8d67-4ec9-b7de-33c3cf4dcb94",
	}
	for index, backendName := range backendNames {
		require.Less(t, index, len(ids))
		inventories[backendName] = BackendInventory{
			StorageIdentity:        freshTestStorageID(t, ids[index]),
			Provisions:             []string{},
			ProvisionProviderUUIDs: map[string]string{},
			Retentions:             []string{},
		}
	}
	backendProof, err := NewFreshBackendProof(backendNames, inventories)
	require.NoError(t, err)
	target, err := NewFreshInitializationTarget(path, freshTestProviderUUID, backendNames)
	require.NoError(t, err)
	quiescence, err := ConfirmFreshQuiescence(target, target.Confirmation())
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	plan, err := NewFreshInitializationPlan(ctx, target, chainProof, backendProof, quiescence)
	require.NoError(t, err)
	return plan
}

func freshTestStorageID(t *testing.T, text string) backendidentity.ID {
	t.Helper()
	id, err := backendidentity.Parse(text)
	require.NoError(t, err)
	return id
}
