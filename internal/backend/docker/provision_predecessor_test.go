package docker

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// A predecessor release that records no runtime authority is one the first
// 0.14 start never completed. A provision over it used to record the authority
// it observed on the surviving cohort. It is now refused before anything is
// durable: no operation intent, no callback, and the release as it was.
func TestProvisionOverPredecessorWithoutRecordedAuthorityRefusesBeforeDurableAdmission(t *testing.T) {
	store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	items := []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", Quantity: 2}}
	spec := shared.OperationIntentSpec{
		LeaseUUID: durableCallbackTestLeaseUUID, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
		CallbackURL: "https://legacy.example/callbacks/provision?route=v013", Items: items,
		ResourceProfiles: testResourceProfiles(t, items),
		Manifest:         validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"}),
	}
	seedV013OperationReleaseForTest(t, store, spec.LeaseUUID, shared.Release{
		Manifest: spec.Manifest, Image: "stack", Items: slices.Clone(items), ResourceProfiles: spec.ResourceProfiles,
		Status: "active", CreatedAt: time.Now().Add(-time.Hour),
	})
	// One of the two instances survives. Recovery reports that incomplete
	// cohort as a failed lease instead of refusing to start, so the release
	// reaches provision admission.
	survivor := dockerIntentContainer(spec, "legacy-survivor", "docker-small", 0)
	b := newOperationIntentRecoveryBackend(t, store, backendidentity.ID{}, []ContainerInfo{survivor}, nil)
	t.Cleanup(func() { b.stopCancel(); b.wg.Wait() })
	require.NoError(t, b.recoverState(t.Context()))
	info, err := b.GetProvision(t.Context(), spec.LeaseUUID)
	require.NoError(t, err)
	require.Equal(t, backend.ProvisionStatusFailed, info.Status)
	before, err := b.releaseStore.LatestActive(spec.LeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, before)
	require.Nil(t, before.LegacyRuntimeAuthority)
	originalAllocs := b.pool.ListAllocations()
	forbidOperationRecoveryTeardown(t, b)

	err = b.Provision(t.Context(), newProvisionRequest(spec.LeaseUUID, spec.Tenant, "docker-small", 2, spec.Manifest))
	require.ErrorIs(t, err, backend.ErrInvalidState)
	require.ErrorContains(t, err, "incomplete runtime authority")

	claims, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Empty(t, claims)
	callbacks, err := store.ListPending()
	require.NoError(t, err)
	require.Empty(t, callbacks)
	after, err := b.releaseStore.LatestActive(spec.LeaseUUID)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.ElementsMatch(t, originalAllocs, b.pool.ListAllocations())
}

// A provision over the release of a lease adopted from v0.13 is admitted on the
// authority that release records. A foreign principal is refused before
// anything is durable. An accepted replacement that the pool then refuses
// settles as one durable Failed callback, even after its caller disconnects,
// and leaves the recorded authority, the release and the pool as they were.
func TestProvisionOverAdoptedPredecessorKeepsItsRecordedAuthority(t *testing.T) {
	for _, mode := range []string{"foreign principal", "capacity refusal", "caller cancels after admission"} {
		t.Run(mode, func(t *testing.T) {
			store, err := newBoundOperationIntentTestStore(t, shared.CallbackStoreConfig{DBPath: filepath.Join(t.TempDir(), "callbacks.db")})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			items := []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", Quantity: 2}}
			spec := shared.OperationIntentSpec{
				LeaseUUID: durableCallbackTestLeaseUUID, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
				CallbackURL: "https://legacy.example/callbacks/provision?route=v013", Items: items,
				ResourceProfiles: testResourceProfiles(t, items),
				Manifest:         validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"}),
			}
			seedAdoptedOperationReleaseForTest(t, store, spec, shared.Release{
				Manifest: spec.Manifest, Image: "stack", Items: slices.Clone(items), ResourceProfiles: spec.ResourceProfiles,
				Status: "active", CreatedAt: time.Now().Add(-time.Hour),
			})
			first := dockerIntentContainer(spec, "legacy-survivor", "docker-small", 0)
			second := dockerIntentContainer(spec, "legacy-second", "docker-small", 1)
			b := newOperationIntentRecoveryBackend(t, store, backendidentity.ID{}, []ContainerInfo{first}, nil)
			t.Cleanup(func() { b.stopCancel(); b.wg.Wait() })
			require.NoError(t, b.recoverState(t.Context()))
			info, err := b.GetProvision(t.Context(), spec.LeaseUUID)
			require.NoError(t, err)
			require.Equal(t, backend.ProvisionStatusFailed, info.Status)
			before, err := b.releaseStore.LatestActive(spec.LeaseUUID)
			require.NoError(t, err)
			require.NotNil(t, before)
			require.NotNil(t, before.LegacyRuntimeAuthority)
			originalAllocs := b.pool.ListAllocations()
			forbidOperationRecoveryTeardown(t, b)
			caller, cancel := context.WithCancel(t.Context())
			defer cancel()
			mock := b.docker.(*mockDockerClient)
			mock.ListManagedContainersFn = func(context.Context) ([]ContainerInfo, error) { return []ContainerInfo{first, second}, nil }
			mock.InspectContainerFn = func(_ context.Context, id string) (*ContainerInfo, error) {
				if id == first.ContainerID {
					copy := first
					return &copy, nil
				}
				copy := second
				return &copy, nil
			}
			req := newProvisionRequest(spec.LeaseUUID, spec.Tenant, "docker-small", 2, spec.Manifest)

			accepted := mode != "foreign principal"
			switch mode {
			case "foreign principal":
				req.Tenant = "tenant-b"
				err = b.Provision(caller, req)
				require.ErrorIs(t, err, backend.ErrInvalidState)
				require.ErrorContains(t, err, "belongs to a different tenant or provider")
			case "capacity refusal":
				hold := b.pool.HoldUnaccountedFootprint()
				defer hold.Release()
				err = b.Provision(caller, req)
				require.ErrorIs(t, err, backend.ErrInsufficientResources)
			case "caller cancels after admission":
				hold := b.pool.HoldUnaccountedFootprint()
				defer hold.Release()
				planner := &pausedProvisionCapacityPlanner{
					next: b.releaseCapacityPlanner, entered: make(chan struct{}), resume: make(chan struct{}),
				}
				b.releaseCapacityPlanner = planner
				returned := make(chan error, 1)
				go func() { returned <- b.Provision(caller, req) }()
				waitForOperationWorker(t, planner.entered)
				cancel()
				close(planner.resume)
				err = <-returned
				require.ErrorIs(t, err, backend.ErrInsufficientResources,
					"an accepted replacement owns its settlement after HTTP disconnect")
			}

			claims, err := b.operationSettlement.ListOperationIntents()
			require.NoError(t, err)
			require.Empty(t, claims, "no refusal may strand an un-settleable Pending head")
			callbacks, err := store.ListPending()
			require.NoError(t, err)
			after, err := b.releaseStore.LatestActive(spec.LeaseUUID)
			require.NoError(t, err)
			if accepted {
				require.Len(t, callbacks, 1, "the accepted refusal is durably representable")
				require.Equal(t, backend.CallbackStatusFailed, callbacks[0].Status)
			} else {
				require.Empty(t, callbacks, "no operation was accepted or failed")
			}
			require.Equal(t, before, after, "the recorded authority and the release are unchanged")
			require.Equal(t, spec.CallbackURL, after.LegacyRuntimeAuthority.CallbackURL())
			require.ElementsMatch(t, originalAllocs, b.pool.ListAllocations())
			require.True(t, b.provisionStore.Exists(spec.LeaseUUID))
		})
	}
}

func TestPreparedProvisionAdmissionRequiresOwnedCandidate(t *testing.T) {
	claim, created, err := (preparedProvisionOperation{}).begin()
	require.Error(t, err)
	require.False(t, created)
	require.False(t, claim.Valid())
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	request := newProvisionOperationInput(backend.ProvisionRequest{
		LeaseUUID: spec.LeaseUUID, Tenant: spec.Tenant, ProviderUUID: spec.ProviderUUID,
		Items: slices.Clone(spec.Items), CallbackURL: spec.CallbackURL,
		LifecycleCallbackURL: spec.LifecycleCallbackURL, Payload: slices.Clone(spec.Manifest),
	})
	stack, err := manifest.ParseStoredPayload(spec.Manifest)
	require.NoError(t, err)
	ingress, err := b.admitIngressPlan(t.Context(), stack, request.Items)
	require.NoError(t, err)
	admitted, err := b.operationSettlement.AdmitProvisionManifest(t.Context(), request.LeaseUUID, request.Tenant, request.ProviderUUID, request.Items, request.Payload)
	require.NoError(t, err)
	prepared, err := b.prepareProvisionOperation(request, request.Items, ingress, spec.ResourceProfiles, nil, admitted)
	require.NoError(t, err)
	request.Items[0].Quantity++
	request.Payload[0] = '!'
	request.Tenant = "foreign"
	claim, created, err = prepared.begin()
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, spec.Items, claim.EffectiveItems())
	require.Equal(t, spec.Manifest, claim.Manifest())
	require.Equal(t, spec.Tenant, claim.Tenant())
	replay, created, err := prepared.begin()
	require.NoError(t, err)
	require.False(t, created)
	require.False(t, replay.Valid(), "exact retry cannot yield a second dispatch")
}
