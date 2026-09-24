package docker

import (
	"context"
	"errors"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

type startupImagePinObserver struct{ calls int }

func (o *startupImagePinObserver) ObserveImagePins(_ context.Context, subject shared.ImagePinBackfillSubject) ([]shared.ImagePinBackfillObservation, error) {
	o.calls++
	stack, err := manifest.ParseStoredPayload(subject.Release().Manifest)
	if err != nil {
		return nil, err
	}
	return []shared.ImagePinBackfillObservation{{
		Reference: stack.Services["app"].Image, ImageID: testImageID,
		Platform: ocispec.Platform{OS: "linux", Architecture: "amd64"},
	}}, nil
}

func TestStart_ImagePinUpgradeFollowsFatalRecoveryChecks(t *testing.T) {
	for _, failQuota := range []bool{true, false} {
		name := "ready upgrades"
		if failQuota {
			name = "quota refusal preserves rollback"
		}
		t.Run(name, func(t *testing.T) {
			const lease = "f5ab7a6a-2222-4222-8222-222222222222"
			mock := &mockDockerClient{PingFn: func(context.Context) error { return nil }}
			b := newBackendForProvisionTest(t, mock, nil)
			bindTestStorageIdentity(t, b, mock)
			t.Cleanup(func() { b.stopCancel(); b.wg.Wait() })
			b.cfg.VolumeDataPath = t.TempDir()
			b.provisions[lease] = &provision{ProvisionState: leasesm.ProvisionState{
				LeaseUUID: lease, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
				Status: backend.ProvisionStatusReady, Quantity: 1,
				Items:         []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
				StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{"app": {Image: "example.invalid/app:legacy"}}},
				ContainerIDs:  []string{"container-0"}, ServiceContainers: map[string][]string{"app": {"container-0"}},
			}}
			release := seedProvisionReleaseFromProjectionForBackendTest(t, b, lease)
			p := b.provisions[lease]
			p.ActiveReleaseVersion = release.Version
			mock.ListManagedContainersFn = func(context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{{
					ContainerID: "container-0", LeaseUUID: lease, BackendName: b.cfg.Name,
					Tenant: p.Tenant, ProviderUUID: p.ProviderUUID, SKU: "docker-micro", ServiceName: "app",
					Image: "example.invalid/app:legacy", Status: "running",
					CallbackURL: p.CallbackURL, LifecycleCallbackURL: p.LifecycleCallbackURL,
				}}, nil
			}
			quotaErr := errors.New("quota refused after state recovery")
			quotaCalls := 0
			b.volumes = &mockVolumeManager{
				ListFn: func() ([]string, error) { return []string{canonicalVolumeName(lease, "app", 0)}, nil },
				EnsureQuotaFn: func(context.Context, string, int64) error {
					quotaCalls++
					if failQuota {
						return quotaErr
					}
					return nil
				},
			}
			bindRetentionOrphanPrunerForTest(t, b)
			pins, err := shared.NewImagePinJournal(b.callbackStore, b.releaseStore, b.retentionStore)
			require.NoError(t, err)
			observer := &startupImagePinObserver{}
			backfiller, err := shared.NewImagePinBackfiller(pins, observer)
			require.NoError(t, err)
			b.imageCapacity = &imageCapacityManager{backfiller: backfiller}

			err = b.Start(t.Context())
			if failQuota {
				require.ErrorIs(t, err, quotaErr)
				require.Zero(t, observer.calls, "failed startup must not enter the optional schema upgrade")
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, observer.calls, "the same legacy release upgrades after successful startup checks")
			}
			require.Equal(t, 1, quotaCalls)
			b.stopCancel()
			b.wg.Wait()
			require.NoError(t, b.callbackStore.Close())
			db, err := bolt.Open(b.cfg.CallbackDBPath, 0o600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
			require.NoError(t, err)
			defer db.Close()
			require.NoError(t, db.View(func(tx *bolt.Tx) error {
				bucket := tx.Bucket([]byte("docker_image_pins_v1"))
				if failQuota {
					require.Nil(t, bucket, "even an empty extension bucket blocks the previous binary")
				} else {
					require.NotNil(t, bucket)
					require.Equal(t, 1, bucket.Stats().KeyN)
				}
				return nil
			}))
		})
	}
}
