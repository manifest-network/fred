package docker

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// Model an adopted, sized v0.13 release losing one member before the first
// startup snapshot. Recovery can conservatively publish Failed, but cannot
// freeze the missing cohort's callback authority. A new request must not leave
// a Pending head whose eventual failure is impossible to settle.
func TestProvisionPartialLegacyPredecessorRefusesBeforeDurableAdmission(t *testing.T) {
	for _, mode := range []string{"partial cohort", "inspect unavailable", "caller cancels during inspection", "foreign principal", "mixed callback generation", "restored cohort capacity refusal", "caller cancels after admission"} {
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
			seedV013OperationReleaseForTest(t, store, spec.LeaseUUID, shared.Release{
				Manifest: spec.Manifest, Image: "stack", Items: slices.Clone(items), ResourceProfiles: spec.ResourceProfiles,
				Status: "active", CreatedAt: time.Now().Add(-time.Hour),
			})
			first := dockerIntentContainer(spec, "legacy-survivor", "docker-small", 0)
			b := newOperationIntentRecoveryBackend(t, store, backendidentity.ID{}, []ContainerInfo{first}, nil)
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
			caller, cancel := context.WithCancel(t.Context())
			defer cancel()
			mock := b.docker.(*mockDockerClient)
			if mode != "partial cohort" {
				second := dockerIntentContainer(spec, "legacy-second", "docker-small", 1)
				if mode == "mixed callback generation" {
					second.CallbackURL = "https://foreign.example/callbacks/provision?route=v013"
				}
				mock.ListManagedContainersFn = func(context.Context) ([]ContainerInfo, error) { return []ContainerInfo{first, second}, nil }
				mock.InspectContainerFn = func(_ context.Context, id string) (*ContainerInfo, error) {
					if mode == "inspect unavailable" {
						return nil, errors.New("Docker inspect unavailable")
					}
					if mode == "caller cancels during inspection" {
						cancel()
					}
					if id == first.ContainerID {
						copy := first
						return &copy, nil
					}
					copy := second
					return &copy, nil
				}
			}
			req := newProvisionRequest(spec.LeaseUUID, spec.Tenant, "docker-small", 2, spec.Manifest)
			completePredecessor := mode == "restored cohort capacity refusal" || mode == "caller cancels after admission"
			if completePredecessor {
				hold := b.pool.HoldUnaccountedFootprint()
				defer hold.Release()
			}
			if mode == "foreign principal" {
				req.Tenant = "tenant-b"
			}
			if mode == "caller cancels after admission" {
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
					"an accepted replacement owns its bounded backfill after HTTP disconnect")
			} else {
				err = b.Provision(caller, req)
			}
			require.Error(t, err)
			claims, err := b.operationSettlement.ListOperationIntents()
			require.NoError(t, err)
			require.Empty(t, claims, "pre-admission uncertainty must not strand an un-settleable Pending head")
			callbacks, err := store.ListPending()
			require.NoError(t, err)
			after, err := b.releaseStore.LatestActive(spec.LeaseUUID)
			require.NoError(t, err)
			if completePredecessor {
				require.Len(t, callbacks, 1, "the accepted refusal is now durably representable")
				require.Equal(t, backend.CallbackStatusFailed, callbacks[0].Status)
				require.NotNil(t, after.LegacyRuntimeAuthority)
				require.Equal(t, spec.CallbackURL, after.LegacyRuntimeAuthority.CallbackURL())
				require.Equal(t, before.Version, after.Version)
				require.Equal(t, before.Items, after.Items)
			} else {
				require.Empty(t, callbacks, "no operation was accepted or failed")
				require.Equal(t, before, after)
			}
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
	prepared, err := b.prepareProvisionOperation(t.Context(), request, request.Items, spec.ResourceProfiles, nil)
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
