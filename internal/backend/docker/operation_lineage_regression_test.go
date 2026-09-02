package docker

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

func TestSettleCommittedOperationBeforeCloseRequiresExactOperationLineage(t *testing.T) {
	for _, kind := range []shared.OperationIntentKind{
		shared.OperationIntentProvision,
		shared.OperationIntentRestore,
	} {
		t.Run(string(kind), func(t *testing.T) {
			dir := t.TempDir()
			b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
			t.Cleanup(func() { closeCloseRecoveryBackend(t, b, stores) })

			spec := dockerOperationIntentSpec(t, b.storageIdentity)
			spec.Kind = kind
			if kind == shared.OperationIntentRestore {
				const sourceLeaseUUID = "123e4567-e89b-42d3-a456-426614174001"
				spec.SourceLeaseUUID = sourceLeaseUUID
				spec.SourceGeneration = 1
				stack, err := manifest.ParsePayload(spec.Manifest)
				require.NoError(t, err)
				retentions, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
					DBPath: filepath.Join(dir, "retentions.db"),
				})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, retentions.Close()) })
				b.retentionStore = retentions
				require.NoError(t, retentions.Put(shared.RetentionEntry{
					OriginalLeaseUUID: sourceLeaseUUID,
					Tenant:            spec.Tenant,
					ProviderUUID:      spec.ProviderUUID,
					Items:             spec.Items,
					ResourceProfiles:  spec.ResourceProfiles,
					StackManifest:     stack,
					Status:            shared.RetentionStatusActive,
					CreatedAt:         time.Now(),
				}))
			}

			admission, err := stores.callbacks.BeginOperationIntent(spec)
			require.NoError(t, err)
			require.Equal(t, shared.OperationIntentAdmissionCreated, admission.Disposition)
			if kind == shared.OperationIntentRestore {
				claimed, claimErr := b.retentionStore.ClaimForRestoreWithAuthority(
					spec.SourceLeaseUUID,
					spec.LeaseUUID,
					0,
					spec.Items,
					spec.ResourceProfiles,
					admission.Claim.OperationID(),
					spec.CallbackURL,
					spec.LifecycleCallbackURL,
				)
				require.NoError(t, claimErr)
				require.Equal(t, spec.SourceGeneration, claimed.Generation)
			}

			const olderOperationID = shared.OperationID("9a72fbc1-38c8-4f31-87f7-f689979b9324")
			require.NotEqual(t, admission.Claim.OperationID(), olderOperationID)
			olderCallbackURL := strings.Replace(
				spec.CallbackURL, admission.Claim.OperationID().String(), olderOperationID.String(), 1,
			)
			olderLifecycleCallbackURL := strings.Replace(
				spec.LifecycleCallbackURL, admission.Claim.OperationID().String(), olderOperationID.String(), 1,
			)
			olderAuthority, authorityErr := shared.NewReleaseRuntimeAuthority(
				olderOperationID,
				spec.Tenant,
				spec.ProviderUUID,
				olderCallbackURL,
				olderLifecycleCallbackURL,
			)
			require.NoError(t, authorityErr)
			require.NoError(t, stores.releases.Append(spec.LeaseUUID, shared.Release{
				Manifest:         spec.Manifest,
				Image:            "stack",
				OperationID:      olderOperationID,
				Items:            spec.Items,
				ResourceProfiles: spec.ResourceProfiles,
				RuntimeAuthority: &olderAuthority,
				Status:           "active",
				CreatedAt:        time.Now(),
			}))

			require.NoError(t, b.settleCommittedOperationBeforeClose(spec.LeaseUUID))
			intents, err := stores.callbacks.ListOperationIntents()
			require.NoError(t, err)
			require.Len(t, intents, 1,
				"same content from an older operation must not settle the current intent")
			assert.Equal(t, admission.Claim.OperationID(), intents[0].OperationID())
			pending, err := stores.callbacks.ListPending()
			require.NoError(t, err)
			assert.Empty(t, pending)

			// The exact causal release proves that this operation crossed its commit
			// boundary before Close reached the actor drain point.
			require.NoError(t, stores.releases.AppendActive(spec.LeaseUUID, shared.Release{
				Manifest:         spec.Manifest,
				Image:            "stack",
				OperationID:      admission.Claim.OperationID(),
				Items:            spec.Items,
				ResourceProfiles: spec.ResourceProfiles,
				RuntimeAuthority: mustReleaseRuntimeAuthorityForIntent(t, admission.Claim),
				Status:           "active",
				CreatedAt:        time.Now(),
			}))
			require.NoError(t, b.settleCommittedOperationBeforeClose(spec.LeaseUUID))
			intents, err = stores.callbacks.ListOperationIntents()
			require.NoError(t, err)
			assert.Empty(t, intents)
			pending, err = stores.callbacks.ListPending()
			require.NoError(t, err)
			require.Len(t, pending, 1)
			assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
			assert.Equal(t, spec.CallbackURL, pending[0].CallbackURL)
		})
	}
}

func TestSettleCommittedOperationBeforeMaintenanceFencesUnresolvedLineage(t *testing.T) {
	dir := t.TempDir()
	b, stores := openCloseRecoveryBackend(t, dir, &mockDockerClient{}, nil)
	t.Cleanup(func() { closeCloseRecoveryBackend(t, b, stores) })

	spec := dockerOperationIntentSpec(t, b.storageIdentity)
	admission, err := stores.callbacks.BeginOperationIntent(spec)
	require.NoError(t, err)
	require.Equal(t, shared.OperationIntentAdmissionCreated, admission.Disposition)

	err = b.settleCommittedOperationBeforeMaintenance(spec.LeaseUUID)
	require.ErrorIs(t, err, backend.ErrInvalidState)
	intents, err := stores.callbacks.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1, "uncommitted operation must retain sole mutation authority")
	pending, err := stores.callbacks.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)

	require.NoError(t, stores.releases.AppendActive(spec.LeaseUUID, shared.Release{
		Manifest:         spec.Manifest,
		Image:            "stack",
		OperationID:      admission.Claim.OperationID(),
		Items:            spec.Items,
		ResourceProfiles: spec.ResourceProfiles,
		RuntimeAuthority: mustReleaseRuntimeAuthorityForIntent(t, admission.Claim),
		Status:           "active",
		CreatedAt:        admission.Claim.CreatedAt(),
	}))

	require.NoError(t, b.settleCommittedOperationBeforeMaintenance(spec.LeaseUUID))
	intents, err = stores.callbacks.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err = stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, shared.CallbackDeliveryKindOperation, pending[0].DeliveryKind)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	assert.Equal(t, spec.CallbackURL, pending[0].CallbackURL)
}

func TestRecoverCommittedRestoreWithNoSurvivorsRetainsExactAuthority(t *testing.T) {
	const (
		sourceLeaseUUID      = "0192f1a0-1111-7abc-8def-000000000501"
		destinationLeaseUUID = "0192f1a0-2222-7abc-8def-000000000502"
		operationID          = shared.OperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
		providerUUID         = "22222222-2222-4222-8222-222222222222"
	)
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	t.Cleanup(b.stopCancel)

	retentions, err := shared.NewRetentionStore(shared.RetentionStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "retentions.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, retentions.Close()) })
	b.retentionStore = retentions
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "releases.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, releases.Close()) })
	b.releaseStore = releases
	callbacks, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: b.cfg.CallbackDBPath,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbacks.Close()) })
	b.callbackStore = callbacks
	b.operationIntents = callbacks

	sourceItems := []backend.LeaseItem{{
		SKU: "docker-small", ServiceName: manifest.DefaultServiceName, Quantity: 2,
	}}
	destinationItems := []backend.LeaseItem{{
		SKU: "destination-tier", ServiceName: manifest.DefaultServiceName, Quantity: 2,
	}}
	destinationProfiles := []shared.SKUResourceSnapshot{{
		SKU: "destination-tier", CPUCores: 1.25, MemoryMB: 768, DiskMB: 2048,
	}}
	stack := restoreStackManifest()
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	require.NoError(t, retentions.Put(shared.RetentionEntry{
		OriginalLeaseUUID: sourceLeaseUUID,
		Tenant:            "tenant-a",
		ProviderUUID:      providerUUID,
		Items:             sourceItems,
		ResourceProfiles:  testResourceProfiles(t, sourceItems),
		StackManifest:     stack,
		Status:            shared.RetentionStatusActive,
		CreatedAt:         time.Now(),
	}))
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID.String()
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	admission, err := callbacks.BeginOperationIntent(shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentRestore,
		LeaseUUID:            destinationLeaseUUID,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
		Backend:              b.Name(),
		BackendStorageID:     b.storageIdentity,
		Tenant:               "tenant-a",
		ProviderUUID:         providerUUID,
		Items:                destinationItems,
		ResourceProfiles:     destinationProfiles,
		EffectiveItems:       destinationItems,
		Manifest:             manifestBytes,
		SourceLeaseUUID:      sourceLeaseUUID,
		SourceGeneration:     1,
	})
	require.NoError(t, err)
	require.Equal(t, operationID, admission.Claim.OperationID())
	claimed, err := retentions.ClaimForRestoreWithAuthority(
		sourceLeaseUUID,
		destinationLeaseUUID,
		0,
		destinationItems,
		destinationProfiles,
		admission.Claim.OperationID(),
		callbackURL,
		lifecycleCallbackURL,
	)
	require.NoError(t, err)
	require.Equal(t, 1, claimed.Generation)
	runtimeAuthority, err := shared.NewReleaseRuntimeAuthority(
		admission.Claim.OperationID(),
		"tenant-a",
		providerUUID,
		callbackURL,
		lifecycleCallbackURL,
	)
	require.NoError(t, err)
	require.NoError(t, releases.Append(destinationLeaseUUID, shared.Release{
		Manifest:         manifestBytes,
		Image:            "stack",
		OperationID:      admission.Claim.OperationID(),
		Items:            destinationItems,
		ResourceProfiles: destinationProfiles,
		RuntimeAuthority: &runtimeAuthority,
		Status:           "active",
		CreatedAt:        time.Now(),
	}))

	ctx := context.Background()
	require.NoError(t, b.RefreshState(ctx))
	assertRecoveredCommittedRestore(
		t, b, destinationLeaseUUID, providerUUID, destinationItems, destinationProfiles,
	)
	require.NoError(t, b.preflightOperationIntentRecovery(ctx))
	require.NoError(t, b.reconcileRetentions(ctx))
	require.NoError(t, b.recoverOperationIntents(ctx, nil))

	intents, err := callbacks.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	assert.Equal(t, callbackURL, pending[0].CallbackURL)

	// The operation callback has settled, but the source row remains the durable
	// tenant/provider identity for a committed destination with no survivors.
	// A second cold-style refresh must therefore reconstruct the same complete
	// failed projection and reservations without duplicating settlement.
	require.NoError(t, b.RefreshState(ctx))
	assertRecoveredCommittedRestore(
		t, b, destinationLeaseUUID, providerUUID, destinationItems, destinationProfiles,
	)
	remaining, err := retentions.Get(sourceLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, remaining)
	assert.Equal(t, shared.RetentionStatusRestoring, remaining.Status)
	assert.Equal(t, destinationLeaseUUID, remaining.NewLeaseUUID)
	assert.Equal(t, operationID, remaining.DestinationOperationID)
	assert.Equal(t, callbackURL, remaining.DestinationCallbackURL)
	assert.Equal(t, lifecycleCallbackURL, remaining.DestinationLifecycleCallbackURL)
	pending, err = callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1, "repeated recovery must not duplicate operation settlement")
}

func assertRecoveredCommittedRestore(
	t *testing.T,
	b *Backend,
	leaseUUID string,
	providerUUID string,
	items []backend.LeaseItem,
	profiles []shared.SKUResourceSnapshot,
) {
	t.Helper()
	b.provisionsMu.RLock()
	projected := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projected)
	assert.Equal(t, backend.ProvisionStatusFailed, projected.Status)
	assert.Equal(t, "tenant-a", projected.Tenant)
	assert.Equal(t, providerUUID, projected.ProviderUUID)
	assert.Equal(t, items, projected.Items)
	assert.Equal(t, profiles, projected.ResourceProfiles)
	assert.Equal(t, 2, projected.Quantity)

	for _, suffix := range []string{"0", "1"} {
		allocation := b.pool.GetAllocation(leaseUUID + "-app-" + suffix)
		require.NotNil(t, allocation)
		assert.Equal(t, "tenant-a", allocation.Tenant)
		assert.Equal(t, "destination-tier", allocation.SKU)
		assert.Equal(t, 1.25, allocation.CPUCores)
		assert.Equal(t, int64(768), allocation.MemoryMB)
		assert.Equal(t, int64(2048), allocation.DiskMB)
	}
	assert.Equal(t, 2, b.pool.Stats().AllocationCount)
}
