package docker

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestRecoverPendingProvisionWithItemslessV013Predecessor(t *testing.T) {
	for _, state := range []string{
		"ready", "empty", "partial", "failed", "older survivor",
		"hidden older survivor", "strict inventory failure",
	} {
		t.Run(state, func(t *testing.T) {
			spec := dockerOperationIntentSpec(t, backendidentity.ID{})
			spec.Items[0].Quantity = 2
			spec.ResourceProfiles[0].DiskMB = 0
			spec.ResourceProfiles[0].ScratchDiskMB = 64
			inventory := []ContainerInfo{
				dockerIntentContainer(spec, "candidate-0", spec.Items[0].SKU, 0),
				dockerIntentContainer(spec, "candidate-1", spec.Items[0].SKU, 1),
			}
			switch state {
			case "empty":
				inventory = nil
			case "partial":
				inventory = inventory[:1]
			case "failed":
				for index := range inventory {
					inventory[index].Status = "exited"
				}
			case "older survivor", "hidden older survivor":
				older := inventory[0]
				older.ContainerID = "older-larger-container"
				older.SKU = "docker-medium"
				older.CallbackURL = "https://fred.example/callbacks/provision"
				older.LifecycleCallbackURL = older.CallbackURL
				inventory = append(inventory, older)
			}
			strictInventory := slices.Clone(inventory)
			if state == "hidden older survivor" {
				// The tolerant inventory can omit a rejected cohort. Its complete
				// current prefix must not masquerade as strict predecessor absence.
				inventory = inventory[:2]
			}
			mutations := 0
			mock := &mockDockerClient{
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
					return slices.Clone(inventory), nil
				},
				InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
					for _, container := range inventory {
						if container.ContainerID == id {
							return &container, nil
						}
					}
					return nil, assert.AnError
				},
				RemoveContainerFn: func(context.Context, string) error { mutations++; return nil },
				StopContainerFn:   func(context.Context, string, time.Duration) error { mutations++; return nil },
			}
			b, stores := newItemslessV013PredecessorRecoveryFixture(t, spec, mock)
			b.docker = itemslessPredecessorStrictInventory{
				dockerReadClient: b.docker,
				strict: func(ctx context.Context) ([]ContainerInfo, error) {
					_, bounded := ctx.Deadline()
					require.True(t, bounded, "strict inventory must retain its bounded read context")
					if state == "strict inventory failure" {
						return nil, assert.AnError
					}
					return slices.Clone(strictInventory), nil
				},
			}
			b.compose.(*mockComposeExecutor).DownFn = func(context.Context, string, time.Duration) error {
				mutations++
				return nil
			}
			claim := createdDockerOperationClaim(t, beginOperationIntentForSettlementTest(t, stores.operations, spec))
			startPendingOperationForRecoveryTest(t, b)
			beforeRelease, err := stores.releases.LatestActive(spec.LeaseUUID)
			require.NoError(t, err)
			require.NotNil(t, beforeRelease)
			require.Empty(t, beforeRelease.Items, "this must not use an already-backfilled v0.13 fixture")
			require.Empty(t, beforeRelease.ResourceProfiles)
			require.Nil(t, beforeRelease.LegacyRuntimeAuthority)
			if state != "ready" {
				// Even an empty inventory cannot erase previously owned CPU or disk:
				// old canonical volumes may still exist without any container witness.
				require.NoError(t, b.pool.TryAllocateResolved(spec.LeaseUUID+"-app-0", spec.Tenant,
					shared.SKUResourceSnapshot{SKU: "docker-medium", CPUCores: 2, MemoryMB: 2048, DiskMB: 4096}))
			}
			beforePool := b.pool.ListAllocations()
			for range 2 {
				err = b.recoverState(t.Context())
				if state == "ready" {
					require.NoError(t, err, "a complete current generation supersedes an unbackfilled legacy row")
					assert.Equal(t, 2*spec.ResourceProfiles[0].CPUCores, b.pool.Stats().AllocatedCPU)
					assert.Equal(t, 2*spec.ResourceProfiles[0].MemoryMB, b.pool.Stats().AllocatedMemoryMB)
					assert.Equal(t, 2*spec.ResourceProfiles[0].ScratchDiskMB, b.pool.Stats().AllocatedDiskMB)
				} else {
					if state == "strict inventory failure" {
						require.ErrorIs(t, err, assert.AnError)
						require.ErrorContains(t, err, "read strict inventory for unsized pending provision predecessor")
					} else {
						require.ErrorContains(t, err, "classify unsized pending provision predecessor",
							"unknown predecessor authority must refuse before publication")
					}
					assert.Equal(t, beforePool, b.pool.ListAllocations())
					assert.Empty(t, b.provisions)
				}
				afterRelease, readErr := stores.releases.LatestActive(spec.LeaseUUID)
				require.NoError(t, readErr)
				assert.Equal(t, beforeRelease, afterRelease, "state recovery must not manufacture legacy sizing or teardown authority")
			}
			if state == "ready" {
				require.NoError(t, b.recoverOperationIntents(t.Context()))
				active, readErr := stores.releases.LatestActive(spec.LeaseUUID)
				require.NoError(t, readErr)
				require.NotNil(t, active)
				assert.Equal(t, claim.OperationID(), active.OperationID)
				assert.Equal(t, claim.EffectiveItems(), active.Items)
				pending, readErr := stores.operations.ListOperationIntents()
				require.NoError(t, readErr)
				assert.Empty(t, pending)
			}
			assert.Zero(t, mutations, "neither supersession nor refusal may authorize predecessor teardown")
		})
	}
}

type itemslessPredecessorStrictInventory struct {
	dockerReadClient
	strict func(context.Context) ([]ContainerInfo, error)
}

func (client itemslessPredecessorStrictInventory) ListManagedContainersStrict(ctx context.Context) ([]ContainerInfo, error) {
	return client.strict(ctx)
}

// Seal empty stores, then write the active release exactly as the 0.14 adoption
// stored a v0.13 row: the current envelope around the v0.13 fields, with no
// Items and no resource authority. No Backfill helper is allowed to replace the
// shape under test.
func newItemslessV013PredecessorRecoveryFixture(
	t *testing.T,
	spec shared.OperationIntentSpec,
	mock *mockDockerClient,
) (*Backend, closeRecoveryStores) {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.CallbackDBPath = filepath.Join(dir, "callbacks.db")
	cfg.ReleasesDBPath = filepath.Join(dir, "releases.db")
	cfg.RetentionDBPath = filepath.Join(dir, "retention.db")
	paths, err := bindDockerStorageInitializationPaths(
		cfg, filepath.Join(dir, "storage-identity.json"), filepath.Join(dir, "storage-identity-anchor.json"),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, paths.Close()) }()
	_, err = paths.markers.InitializeWithStores(cfg.Name, "close-recovery-daemon", backendidentity.MarkerPairStoreHooks{
		Profile: backendidentity.InitializationProfileFresh,
		Prepare: func(storage backendidentity.PendingStorage, profile backendidentity.InitializationProfile) error {
			if err := shared.PrepareBoundCallbackStoreStorage(paths.callbacks, storage); err != nil {
				return err
			}
			if err := shared.PrepareBoundReleaseStoreStorage(paths.releases, storage); err != nil {
				return err
			}
			return shared.PrepareBoundRetentionStoreStorage(paths.retention, storage)
		},
		Check: func(storage backendidentity.PendingStorage) error {
			if err := shared.CheckBoundCallbackStoreStorage(paths.callbacks, storage); err != nil {
				return err
			}
			if err := shared.CheckBoundReleaseStoreStorage(paths.releases, storage); err != nil {
				return err
			}
			return shared.CheckBoundRetentionStoreStorage(paths.retention, storage)
		},
		Verify: func(storage backendidentity.VerifiedStorage) error {
			return verifyBoundDockerAuthoritativeStoreSet(paths, storage)
		},
	})
	require.NoError(t, err)
	writeItemslessActiveRelease(t, cfg.ReleasesDBPath, spec.LeaseUUID, spec.Manifest)
	b, stores := openCloseRecoveryBackend(t, dir, mock, nil)
	b.cfg.RetentionDBPath = cfg.RetentionDBPath
	b.releaseBackfiller, err = shared.NewReleaseBackfiller(stores.callbacks, stores.releases)
	require.NoError(t, err)
	t.Cleanup(func() { closeCloseRecoveryBackend(t, b, stores) })
	return b, stores
}

// writeItemslessActiveRelease writes one active release carrying only the
// fields v0.13 persisted into an already sealed release journal.
func writeItemslessActiveRelease(t *testing.T, dbPath, leaseUUID string, stackManifest []byte) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: time.Second})
	require.NoError(t, err)
	data, err := json.Marshal(struct {
		SchemaVersion uint8            `json:"schema_version"`
		Releases      []shared.Release `json:"releases"`
	}{SchemaVersion: 1, Releases: []shared.Release{{
		Version: 1, Manifest: stackManifest, Image: "stack", Status: "active", CreatedAt: time.Now(),
	}}})
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("releases"))
		if bucket == nil {
			return errors.New("releases bucket is missing")
		}
		return bucket.Put([]byte(leaseUUID), data)
	}))
	require.NoError(t, db.Close())
}
