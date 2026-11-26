package docker

import (
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
