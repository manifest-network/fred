package docker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/substratemutation"
)

func TestImageCapacityStartedMaintenanceReusesCachedImageWhileTenantStagesAreFull(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{shared.MaintenanceIntentRestart, shared.MaintenanceIntentUpdate} {
		t.Run(string(kind), func(t *testing.T) {
			m, daemon, _ := imageCapacityFixture(t)
			callbacks, err := newBoundCallbackStoreForTest(t, shared.CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
			require.NoError(t, err)
			t.Cleanup(func() { _ = callbacks.Close() })
			releases, retentions, operations, _, _ := operationHandoffForCallbackTest(t, callbacks)
			value, ok := operationIntentTestAuthorities.Load(callbacks)
			require.True(t, ok)
			authority := value.(*operationIntentTestAuthority)
			spec := dockerOperationIntentSpec(t, authority.storage.ID())
			source := shared.Release{
				Manifest: spec.Manifest, Image: "stack", OperationID: dockerOperationIntentID,
				Items: spec.Items, ResourceProfiles: spec.ResourceProfiles,
				RuntimeAuthority: mustTestReleaseRuntimeAuthority(t, dockerOperationIntentID, spec.Tenant, spec.ProviderUUID, spec.CallbackURL, spec.LifecycleCallbackURL),
			}
			seedProvisionReleaseForLeaseTest(t, callbacks, releases, operations, spec.LeaseUUID, source)
			settlement, err := shared.NewMaintenanceSettlement(callbacks, releases)
			require.NoError(t, err)
			m.pins, err = shared.NewImagePinJournal(callbacks, releases, retentions)
			require.NoError(t, err)
			b := &Backend{cfg: DefaultConfig(), stopCtx: t.Context(), storageIdentity: authority.storage.ID(), storeAuthorityGate: authority.gate,
				storageVerifier: testDockerRuntimeStorageVerifier{id: authority.storage.ID()}, imageCapacity: m}
			m.runtime = (&mockDockerClient{InspectImageFn: func(context.Context, string) (*ImageInfo, error) {
				return &ImageInfo{ID: testImageID}, nil
			}}).imageAdmitter()
			daemon.imageInspect = func(context.Context, string, ...client.ImageInspectOption) (image.InspectResponse, error) {
				return image.InspectResponse{ID: testImageID, Size: imageMiB}, nil
			}
			for range maxImageStages {
				release, err := m.tenantShares.acquire(t.Context(), imageShareForTest(&m.tenantShares, spec.Tenant))
				require.NoError(t, err)
				defer release()
			}
			var prepared bool
			var preparationErr error
			require.NoError(t, shared.BindMaintenanceSubstrateExecutor(settlement, b.authorizeStorageMutation, b.completeStorageMutation,
				func(runner substratemutation.Runner, subject shared.MaintenancePhysicalSubject) func(context.Context) error {
					mutations := newMaintenanceStorageMutations(runner, subject, storageMutationOperations{backend: b})
					mutations.tenant = "caller-selected-tenant-is-not-authority"
					return func(ctx context.Context) error {
						admitted, err := mutations.admitImage(ctx, "docker.io/library/nginx:1.27")
						prepared, preparationErr = admitted.ID() == testImageID, err
						return err
					}
				},
				func(ctx context.Context, run func(context.Context) error, _ shared.MaintenancePhysicalSubject) error {
					return run(ctx)
				},
				func(context.Context, shared.MaintenancePhysicalSubject) (shared.MaintenancePhysicalEvidence, error) {
					return shared.MaintenancePhysicalEvidence{}, errors.New("fixture leaves maintenance pending after image admission")
				},
			))
			source, sourceClaim, err := settlement.ClaimLatestActive(spec.LeaseUUID)
			require.NoError(t, err)
			target := source
			target.Version, target.Status = 0, "deploying"
			payload := []byte(nil)
			if kind == shared.MaintenanceIntentUpdate {
				payload = target.Manifest
			}
			request, err := settlement.NewMaintenanceRequestAuthority(mustParseMaintenanceID(t, uuid.NewString()), kind, spec.LeaseUUID, spec.LifecycleCallbackURL, payload)
			require.NoError(t, err)
			candidate, err := settlement.NewMaintenanceIntentCandidate(request, sourceClaim, target)
			require.NoError(t, err)
			admission, err := settlement.BeginMaintenanceIntent(candidate)
			require.NoError(t, err)
			appendClaim, err := settlement.StartMaintenanceAppend(createdTestMaintenanceDispatch(t, admission))
			require.NoError(t, err)
			targetClaim, err := settlement.AppendMaintenance(appendClaim)
			require.NoError(t, err)
			targetClaim, err = settlement.BindMaintenanceIntentTarget(targetClaim)
			require.NoError(t, err)
			execution, err := settlement.StartMaintenanceExecution(targetClaim)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_ = settlement.ExecuteMaintenance(testMaintenanceLifetime(t, ctx), execution)
			require.NoError(t, preparationErr)
			require.True(t, prepared, "a real Started maintenance subject must admit cached content despite the occupied staging pool")
			pin, err := m.pins.Lookup(spec.LeaseUUID, target.Manifest, "docker.io/library/nginx:1.27")
			require.NoError(t, err)
			require.NotNil(t, pin)
			require.Equal(t, testImageID, pin.ImageID)
			require.Equal(t, maxImageStages, m.tenantShares.used, "cached maintenance never consumes a staging slot")
			require.Empty(t, m.tenantShares.waiters)
		})
	}
}
