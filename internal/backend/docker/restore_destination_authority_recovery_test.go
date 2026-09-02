package docker

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

type resolveFailingOperationIntentJournal struct {
	operationIntentJournal
	err error
}

func (j resolveFailingOperationIntentJournal) ResolveOperationIntent(
	shared.OperationIntentClaim,
	backend.CallbackStatus,
	string,
) (shared.CallbackEntry, error) {
	return shared.CallbackEntry{}, j.err
}

func TestRestoreDestinationAuthority_RecoversAfterReleaseFailureIntentSettlementAndConfigDrift(t *testing.T) {
	for _, mutateConfig := range []struct {
		name string
		fn   func(*Config)
	}{
		{
			name: "destination SKU removed",
			fn: func(cfg *Config) {
				delete(cfg.SKUProfiles, "retired-destination")
			},
		},
		{
			name: "destination SKU resized",
			fn: func(cfg *Config) {
				cfg.SKUProfiles["retired-destination"] = SKUProfile{
					CPUCores: 0.25, MemoryMB: 128, DiskMB: 64,
				}
			},
		},
	} {
		t.Run(mutateConfig.name, func(t *testing.T) {
			const (
				sourceLease      = "0192f1a0-1111-7abc-8def-000000000101"
				destinationLease = "0192f1a0-2222-7abc-8def-000000000102"
				operationID      = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
				providerUUID     = "22222222-2222-4222-8222-222222222222"
			)
			dir := t.TempDir()
			retentionPath := filepath.Join(dir, "retention.db")
			releasePath := filepath.Join(dir, "release.db")
			callbackPath := filepath.Join(dir, "callback.db")

			sourceItems := []backend.LeaseItem{{
				SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
			}}
			destinationItems := []backend.LeaseItem{{
				SKU: "retired-destination", Quantity: 1, ServiceName: manifest.DefaultServiceName,
			}}
			sourceProfiles := testResourceProfiles(t, sourceItems)
			destinationProfiles := []shared.SKUResourceSnapshot{{
				SKU: "retired-destination", CPUCores: 1.75, MemoryMB: 1536, DiskMB: 3072,
			}}
			stack := restoreStackManifest()
			manifestBytes, err := json.Marshal(stack)
			require.NoError(t, err)

			retentions, err := shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: retentionPath})
			require.NoError(t, err)
			require.NoError(t, retentions.Put(shared.RetentionEntry{
				OriginalLeaseUUID: sourceLease,
				Tenant:            "tenant-a",
				ProviderUUID:      providerUUID,
				Items:             sourceItems,
				ResourceProfiles:  sourceProfiles,
				StackManifest:     stack,
				Status:            shared.RetentionStatusActive,
				CreatedAt:         time.Now(),
			}))

			callbacks, err := shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: callbackPath})
			require.NoError(t, err)
			storageID, err := backendidentity.Parse("9a72fbc1-38c8-4f31-87f7-f689979b9324")
			require.NoError(t, err)
			callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID
			lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
			require.NoError(t, err)
			admission, err := callbacks.BeginOperationIntent(shared.OperationIntentSpec{
				Kind:                 shared.OperationIntentRestore,
				LeaseUUID:            destinationLease,
				CallbackURL:          callbackURL,
				LifecycleCallbackURL: lifecycleURL,
				Backend:              "docker",
				BackendStorageID:     storageID,
				Tenant:               "tenant-a",
				ProviderUUID:         providerUUID,
				Items:                destinationItems,
				ResourceProfiles:     destinationProfiles,
				EffectiveItems:       destinationItems,
				Manifest:             manifestBytes,
				SourceLeaseUUID:      sourceLease,
				SourceGeneration:     1,
			})
			require.NoError(t, err)
			claimed, err := retentions.ClaimForRestoreWithAuthority(
				sourceLease, destinationLease, 0, destinationItems, destinationProfiles,
				admission.Claim.OperationID(), callbackURL, lifecycleURL,
			)
			require.NoError(t, err)

			releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: releasePath})
			require.NoError(t, err)
			require.NoError(t, releases.Close(), "inject ownership Release failure")
			live := &provision{ //exhaustruct:enforce
				ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
					LeaseUUID:            destinationLease,
					Tenant:               "tenant-a",
					ProviderUUID:         providerUUID,
					SKU:                  destinationItems[0].SKU,
					Status:               backend.ProvisionStatusReady,
					Quantity:             1,
					CreatedAt:            time.Now(),
					FailCount:            0,
					LastError:            "",
					Reason:               "",
					Message:              "",
					CallbackURL:          callbackURL,
					LifecycleCallbackURL: lifecycleURL,
					Items:                destinationItems,
					ResourceProfiles:     shared.CloneSKUResourceSnapshot(destinationProfiles),
					ContainerIDs:         []string{"container-1"},
					StackManifest:        stack,
					ServiceContainers:    map[string][]string{"app": {"container-1"}},
				},
				ResourceProfiles:      destinationProfiles,
				VolumeCleanupAttempts: 0,
			}
			beforeCrash := newBackendForTest(&mockDockerClient{}, map[string]*provision{destinationLease: live})
			beforeCrash.retentionStore = retentions
			beforeCrash.releaseStore = releases
			require.Error(t, beforeCrash.finalizeRestoredLeaseStrict(destinationLease, claimed, destinationItems))
			_, err = callbacks.ResolveOperationIntent(
				admission.Claim, backend.CallbackStatusSuccess, "",
			)
			require.NoError(t, err, "the operation may settle even though its ownership finalizer remains")
			intents, err := callbacks.ListOperationIntents()
			require.NoError(t, err)
			assert.Empty(t, intents)
			require.NoError(t, retentions.Close())
			require.NoError(t, callbacks.Close())
			beforeCrash.stopCancel()

			container := ContainerInfo{
				ContainerID:          "container-1",
				LeaseUUID:            destinationLease,
				Tenant:               "tenant-a",
				ProviderUUID:         providerUUID,
				SKU:                  destinationItems[0].SKU,
				ServiceName:          manifest.DefaultServiceName,
				InstanceIndex:        0,
				CallbackURL:          callbackURL,
				LifecycleCallbackURL: lifecycleURL,
				Image:                stack.Services[manifest.DefaultServiceName].Image,
				Status:               "running",
				Health:               HealthStatusNone,
				CreatedAt:            time.Now(),
				Name:                 "fred-" + destinationLease + "-app-0",
			}
			afterCrash := newBackendForTest(&mockDockerClient{
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
					return []ContainerInfo{container}, nil
				},
			}, nil)
			t.Cleanup(afterCrash.stopCancel)
			mutateConfig.fn(&afterCrash.cfg)

			retentions, err = shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: retentionPath})
			require.NoError(t, err)
			t.Cleanup(func() { _ = retentions.Close() })
			releases, err = shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: releasePath})
			require.NoError(t, err)
			t.Cleanup(func() { _ = releases.Close() })
			callbacks, err = shared.NewCallbackStore(shared.CallbackStoreConfig{DBPath: callbackPath})
			require.NoError(t, err)
			t.Cleanup(func() { _ = callbacks.Close() })
			afterCrash.retentionStore = retentions
			afterCrash.releaseStore = releases
			afterCrash.callbackStore = callbacks
			afterCrash.operationIntents = callbacks

			require.NoError(t, afterCrash.recoverState(context.Background()))
			afterCrash.provisionsMu.RLock()
			recovered := afterCrash.provisions[destinationLease]
			require.NotNil(t, recovered)
			assert.Equal(t, destinationItems, recovered.Items)
			assert.Equal(t, destinationProfiles, recovered.ResourceProfiles)
			afterCrash.provisionsMu.RUnlock()
			allocation := afterCrash.pool.GetAllocation(destinationLease + "-app-0")
			require.NotNil(t, allocation)
			assert.Equal(t, 1.75, allocation.CPUCores)
			assert.Equal(t, int64(1536), allocation.MemoryMB)
			assert.Equal(t, int64(3072), allocation.DiskMB)

			require.NoError(t, afterCrash.reconcileRetentions(context.Background()))
			remaining, err := retentions.Get(sourceLease)
			require.NoError(t, err)
			assert.Nil(t, remaining)
			active, err := releases.LatestActive(destinationLease)
			require.NoError(t, err)
			require.NotNil(t, active)
			assert.Equal(t, destinationItems, active.Items)
			assert.Equal(t, destinationProfiles, active.ResourceProfiles)
		})
	}
}

func TestRestoreRejectsNonActiveSourceBeforeIntentOrProjection(t *testing.T) {
	for _, status := range []string{
		shared.RetentionStatusRestoring,
		shared.RetentionStatusReaping,
	} {
		t.Run(status, func(t *testing.T) {
			b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
			retentions := attachRetentionStore(t, b)
			source := shared.RetentionEntry{
				OriginalLeaseUUID: "source",
				NewLeaseUUID:      "some-other-destination",
				Tenant:            "tenant-a",
				ProviderUUID:      "prov-1",
				Items: []backend.LeaseItem{{
					SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
				}},
				StackManifest: restoreStackManifest(),
				Status:        status,
				Generation:    7,
				CreatedAt:     time.Now(),
			}
			if status == shared.RetentionStatusRestoring {
				putRestoringRetention(t, retentions, source)
			} else {
				require.NoError(t, retentions.Put(source))
			}
			b.operationIntents = fixedOperationIntentProbeJournal{
				disposition: shared.OperationIntentAdmissionNone,
			}

			err := b.Restore(context.Background(), restoreRequest(
				"new-destination", "source", "http://localhost/callbacks/provision",
			))
			require.ErrorIs(t, err, backend.ErrInvalidState)
			assert.NotContains(t, err.Error(), "unexpected BeginOperationIntent")
			b.provisionsMu.RLock()
			_, projected := b.provisions["new-destination"]
			b.provisionsMu.RUnlock()
			assert.False(t, projected)
			stored, getErr := retentions.Get("source")
			require.NoError(t, getErr)
			require.NotNil(t, stored)
			assert.Equal(t, status, stored.Status)
			assert.Equal(t, 7, stored.Generation)
		})
	}
}

func TestPendingRestoreFinalizerBlocksDestinationReuseUntilHandback(t *testing.T) {
	const (
		sourceLease     = "33333333-3333-4333-8333-333333333333"
		destination     = "44444444-4444-4444-8444-444444444444"
		differentSource = "55555555-5555-4555-8555-555555555555"
	)
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	retentions := attachRetentionStore(t, b)
	source := shared.RetentionEntry{
		OriginalLeaseUUID: sourceLease,
		NewLeaseUUID:      destination,
		Tenant:            "tenant-a",
		ProviderUUID:      "prov-1",
		Items: []backend.LeaseItem{{
			SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
		}},
		StackManifest: restoreStackManifest(),
		Status:        shared.RetentionStatusRestoring,
		Generation:    4,
		CreatedAt:     time.Now(),
	}
	claimed := putRestoringRetention(t, retentions, source)

	provisionErr := b.Provision(context.Background(), backend.ProvisionRequest{
		LeaseUUID:    destination,
		Tenant:       "tenant-a",
		ProviderUUID: "prov-1",
		CallbackURL:  testOperationCallbackURL("http://localhost/callbacks/provision"),
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 0, ServiceName: "app"}},
		Payload:      []byte(`{}`),
	})
	require.ErrorIs(t, provisionErr, backend.ErrInvalidState)

	restoreErr := b.Restore(context.Background(), restoreRequest(
		destination, differentSource, "http://localhost/callbacks/provision",
	))
	require.ErrorIs(t, restoreErr, backend.ErrInvalidState)
	restartErr := b.Restart(context.Background(), backend.RestartRequest{LeaseUUID: destination})
	require.ErrorIs(t, restartErr, backend.ErrInvalidState)
	updateErr := b.Update(context.Background(), backend.UpdateRequest{LeaseUUID: destination})
	require.ErrorIs(t, updateErr, backend.ErrInvalidState)

	ok, err := retentions.RevertToActiveWithResourceProfiles(
		sourceLease, destination, claimed.Generation, claimed.ResourceProfiles,
	)
	require.NoError(t, err)
	require.True(t, ok)
	assert.NoError(t, b.ensureRestoreDestinationUnowned(destination))

	// The same deliberately-invalid requests now pass the finalizer guard and
	// fail at their ordinary validation/source checks, proving handback releases
	// the namespace without launching substrate work.
	provisionErr = b.Provision(context.Background(), backend.ProvisionRequest{
		LeaseUUID:    destination,
		Tenant:       "tenant-a",
		ProviderUUID: "prov-1",
		CallbackURL:  testOperationCallbackURL("http://localhost/callbacks/provision"),
		Items:        []backend.LeaseItem{{SKU: "docker-small", Quantity: 0, ServiceName: "app"}},
		Payload:      []byte(`{}`),
	})
	require.Error(t, provisionErr)
	assert.NotErrorIs(t, provisionErr, backend.ErrInvalidState)
	restoreErr = b.Restore(context.Background(), restoreRequest(
		destination, differentSource, "http://localhost/callbacks/provision",
	))
	require.ErrorIs(t, restoreErr, backend.ErrNotRetained)
	restartErr = b.Restart(context.Background(), backend.RestartRequest{LeaseUUID: destination})
	require.ErrorIs(t, restartErr, backend.ErrNotProvisioned)
	updateErr = b.Update(context.Background(), backend.UpdateRequest{LeaseUUID: destination})
	require.ErrorIs(t, updateErr, backend.ErrNotProvisioned)
}

func TestRecoverState_PendingRestoreCleanupCountsAllocationWithoutRestartableProjection(t *testing.T) {
	const (
		sourceLease      = "0192f1a0-1111-7abc-8def-000000000201"
		destinationLease = "0192f1a0-2222-7abc-8def-000000000202"
	)
	b := newBackendForProvisionTest(t, &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
	}, nil)
	retentions := attachRetentionStore(t, b)
	sourceItems := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}
	destinationItems := []backend.LeaseItem{{
		SKU: "removed-destination", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}
	destinationProfiles := []shared.SKUResourceSnapshot{{
		SKU: "removed-destination", CPUCores: 1.25, MemoryMB: 768, DiskMB: 2048,
	}}
	require.NoError(t, retentions.Put(shared.RetentionEntry{
		OriginalLeaseUUID: sourceLease,
		Tenant:            "tenant-a",
		ProviderUUID:      "provider-a",
		Items:             sourceItems,
		ResourceProfiles:  testResourceProfiles(t, sourceItems),
		StackManifest:     restoreStackManifest(),
		Status:            shared.RetentionStatusActive,
		CreatedAt:         time.Now(),
	}))
	operationID, callbackURL, lifecycleURL := restoreDestinationAuthority(t)
	_, err := retentions.ClaimForRestoreWithAuthority(
		sourceLease, destinationLease, 0, destinationItems, destinationProfiles,
		operationID, callbackURL, lifecycleURL,
	)
	require.NoError(t, err)

	require.NoError(t, b.RefreshState(context.Background()))
	b.provisionsMu.RLock()
	_, projected := b.provisions[destinationLease]
	b.provisionsMu.RUnlock()
	assert.False(t, projected, "zero-survivor cleanup must not manufacture Restart-admissible Failed state")
	allocation := b.pool.GetAllocation(destinationLease + "-app-0")
	require.NotNil(t, allocation, "durable destination authority must keep the pending footprint counted")
	assert.Equal(t, int64(2048), allocation.DiskMB)

	restartErr := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID: destinationLease,
	})
	require.ErrorIs(t, restartErr, backend.ErrInvalidState)
}

func TestRecoverState_ActiveRestoreReleaseWithNoSurvivorsRemainsRepairableAcrossRestarts(t *testing.T) {
	const (
		sourceLease      = "0192f1a0-1111-7abc-8def-000000000301"
		destinationLease = "0192f1a0-2222-7abc-8def-000000000302"
		providerUUID     = "22222222-2222-4222-8222-222222222222"
	)
	b := newBackendForProvisionTest(t, &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
	}, nil)
	retentions := attachRetentionStore(t, b)
	releases := attachReleaseStore(t, b)
	sourceItems := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}
	destinationItems := []backend.LeaseItem{{
		SKU: "destination-tier", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}
	destinationProfiles := []shared.SKUResourceSnapshot{{
		SKU: "destination-tier", CPUCores: 1.25, MemoryMB: 768, DiskMB: 2048,
	}}
	stack := restoreStackManifest()
	require.NoError(t, retentions.Put(shared.RetentionEntry{
		OriginalLeaseUUID: sourceLease,
		Tenant:            "tenant-a",
		ProviderUUID:      providerUUID,
		Items:             sourceItems,
		ResourceProfiles:  testResourceProfiles(t, sourceItems),
		StackManifest:     stack,
		Status:            shared.RetentionStatusActive,
		CreatedAt:         time.Now(),
	}))
	operationID, callbackURL, lifecycleURL := restoreDestinationAuthority(t)
	_, err := retentions.ClaimForRestoreWithAuthority(
		sourceLease, destinationLease, 0, destinationItems, destinationProfiles,
		operationID, callbackURL, lifecycleURL,
	)
	require.NoError(t, err)
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	require.NoError(t, releases.Append(destinationLease, shared.Release{
		Manifest:         manifestBytes,
		Image:            "stack",
		OperationID:      operationID,
		Items:            destinationItems,
		ResourceProfiles: destinationProfiles,
		RuntimeAuthority: mustTestReleaseRuntimeAuthority(
			t, operationID, "tenant-a", providerUUID, callbackURL, lifecycleURL,
		),
		Status:    "active",
		CreatedAt: time.Now(),
	}))

	require.NoError(t, b.RefreshState(context.Background()),
		"an exact lingering restore finalizer must explain the empty active-release cohort")
	b.provisionsMu.RLock()
	projected := b.provisions[destinationLease]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projected)
	assert.Equal(t, backend.ProvisionStatusFailed, projected.Status)
	assert.Equal(t, "tenant-a", projected.Tenant)
	assert.Equal(t, providerUUID, projected.ProviderUUID)
	require.NotNil(t, b.pool.GetAllocation(destinationLease+"-app-0"))

	record, err := retentions.Get(sourceLease)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.NoError(t, b.reconcileRestoring(context.Background(), *record))
	record, err = retentions.Get(sourceLease)
	require.NoError(t, err)
	require.NotNil(t, record)
	assert.Equal(t, shared.RetentionStatusRestoring, record.Status,
		"the finalizer remains durable identity while the committed destination is Failed")
	require.NotNil(t, b.pool.GetAllocation(destinationLease+"-app-0"))
	active, err := releases.LatestActive(destinationLease)
	require.NoError(t, err)
	require.NotNil(t, active, "runtime failure must not erase the committed destination")
	assert.NoError(t, b.ensureRestoreDestinationRestartAvailable(destinationLease),
		"settled committed destinations must remain repairable")
	require.ErrorIs(t, b.ensureRestoreDestinationUnowned(destinationLease), backend.ErrInvalidState)
	require.NoError(t, b.RefreshState(context.Background()),
		"the retained identity finalizer must make a second cold start safe")
	b.provisionsMu.RLock()
	projected = b.provisions[destinationLease]
	b.provisionsMu.RUnlock()
	require.NotNil(t, projected)
	assert.Equal(t, backend.ProvisionStatusFailed, projected.Status)
	require.NotNil(t, b.pool.GetAllocation(destinationLease+"-app-0"))
}

func TestUnacceptedRestoreSettlementFailureRetainsPeriodicRollbackAuthority(t *testing.T) {
	const (
		sourceLease      = "0192f1a0-1111-7abc-8def-000000000401"
		destinationLease = "0192f1a0-2222-7abc-8def-000000000402"
		operationID      = "6ba7b810-9dad-41d1-80b4-00c04fd430c8"
		providerUUID     = "22222222-2222-4222-8222-222222222222"
	)
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	bindTestStorageIdentity(t, b, mock)
	retentions := attachRetentionStore(t, b)
	attachReleaseStore(t, b)
	callbacks, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbacks.Close()) })
	b.callbackStore = callbacks

	sourceItems := []backend.LeaseItem{{
		SKU: "source-tier", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}
	sourceProfiles := []shared.SKUResourceSnapshot{{
		SKU: "source-tier", CPUCores: 0.5, MemoryMB: 512, DiskMB: 100,
	}}
	destinationItems := []backend.LeaseItem{{
		SKU: "destination-tier", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}
	destinationProfiles := []shared.SKUResourceSnapshot{{
		SKU: "destination-tier", CPUCores: 1, MemoryMB: 1024, DiskMB: 200,
	}}
	retainedVolume := retainedName(canonicalVolumeName(sourceLease, manifest.DefaultServiceName, 0))
	stack := restoreStackManifest()
	require.NoError(t, retentions.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   sourceLease,
		Tenant:              "tenant-a",
		ProviderUUID:        providerUUID,
		Items:               sourceItems,
		ResourceProfiles:    sourceProfiles,
		StackManifest:       stack,
		RetainedVolumeNames: []string{retainedVolume},
		Status:              shared.RetentionStatusActive,
		CreatedAt:           time.Now(),
	}))
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	storageID := b.storageIdentity
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	claimed, err := retentions.ClaimForRestoreWithAuthority(
		sourceLease, destinationLease, 0, destinationItems, destinationProfiles,
		shared.OperationID(operationID), callbackURL, lifecycleURL,
	)
	require.NoError(t, err)
	admission, err := callbacks.BeginOperationIntent(shared.OperationIntentSpec{
		Kind:                 shared.OperationIntentRestore,
		LeaseUUID:            destinationLease,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleURL,
		Backend:              b.Name(),
		BackendStorageID:     storageID,
		Tenant:               "tenant-a",
		ProviderUUID:         providerUUID,
		Items:                destinationItems,
		ResourceProfiles:     destinationProfiles,
		EffectiveItems:       destinationItems,
		Manifest:             manifestBytes,
		SourceLeaseUUID:      sourceLease,
		SourceGeneration:     claimed.Generation,
	})
	require.NoError(t, err)

	b.provisionsMu.Lock()
	b.provisions[destinationLease] = &provision{ //exhaustruct:enforce
		ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
			LeaseUUID:            destinationLease,
			Tenant:               "tenant-a",
			ProviderUUID:         providerUUID,
			SKU:                  destinationItems[0].SKU,
			Status:               backend.ProvisionStatusProvisioning,
			Quantity:             destinationItems[0].Quantity,
			CreatedAt:            time.Now(),
			FailCount:            0,
			LastError:            "",
			Reason:               "",
			Message:              "",
			CallbackURL:          callbackURL,
			LifecycleCallbackURL: lifecycleURL,
			Items:                destinationItems,
			ResourceProfiles:     shared.CloneSKUResourceSnapshot(destinationProfiles),
			ContainerIDs:         nil,
			StackManifest:        stack,
			ServiceContainers:    nil,
		},
		ResourceProfiles:      destinationProfiles,
		VolumeCleanupAttempts: 0,
	}
	b.provisionsMu.Unlock()
	allocationID := destinationLease + "-app-0"
	require.NoError(t, b.pool.TryAllocateResolved(
		allocationID, "tenant-a", destinationProfiles[0],
	))
	b.compose = &mockComposeExecutor{
		DownFn: func(context.Context, string, time.Duration) error { return nil },
	}
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(string, string) error { return nil },
		UsageFn:        func(context.Context, string) (int64, error) { return 0, nil },
		EnsureQuotaFn:  func(context.Context, string, int64) error { return nil },
	}
	settlementErr := errors.New("callback bbolt unavailable")
	b.operationIntents = resolveFailingOperationIntentJournal{
		operationIntentJournal: callbacks,
		err:                    settlementErr,
	}

	err = b.rollbackUnacceptedRestoreAdoption(
		context.Background(),
		destinationLease,
		[]string{allocationID},
		claimed,
		admission.Claim,
		errors.New("actor rejected restore"),
		b.logger,
	)
	require.ErrorIs(t, err, settlementErr)
	stored, err := retentions.Get(sourceLease)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, shared.RetentionStatusRestoring, stored.Status)
	assert.NotNil(t, b.pool.GetAllocation(allocationID),
		"live allocation must remain until durable source handback")
	b.provisionsMu.RLock()
	_, projected := b.provisions[destinationLease]
	b.provisionsMu.RUnlock()
	assert.False(t, projected, "dead Provisioning state must not block the retry sweep")
	intents, err := callbacks.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)

	b.operationIntents = callbacks
	require.NoError(t, b.reconcileRestoring(context.Background(), *stored))
	stored, err = retentions.Get(sourceLease)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, shared.RetentionStatusActive, stored.Status)
	assert.Nil(t, b.pool.GetAllocation(allocationID))
	intents, err = callbacks.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
}

func restoreDestinationAuthority(t *testing.T) (shared.OperationID, string, string) {
	t.Helper()
	operationID := shared.OperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID.String()
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	return operationID, callbackURL, lifecycleURL
}

type committedRestoreCallbackFixture struct {
	backend              *Backend
	retentions           *shared.RetentionStore
	releases             *shared.ReleaseStore
	callbacks            *shared.CallbackStore
	sourceLease          string
	destinationLease     string
	providerUUID         string
	operationID          shared.OperationID
	callbackURL          string
	lifecycleCallbackURL string
	items                []backend.LeaseItem
	resourceProfiles     []shared.SKUResourceSnapshot
	stack                *manifest.StackManifest
}

func newCommittedRestoreCallbackFixture(t *testing.T) committedRestoreCallbackFixture {
	t.Helper()
	const (
		sourceLease      = "0192f1a0-1111-7abc-8def-000000000501"
		destinationLease = "0192f1a0-2222-7abc-8def-000000000502"
		providerUUID     = "22222222-2222-4222-8222-222222222222"
	)
	dockerClient := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	b := newBackendForProvisionTest(t, dockerClient, nil)
	bindTestStorageIdentity(t, b, dockerClient)
	retentions := attachRetentionStore(t, b)
	releases := attachReleaseStore(t, b)
	callbacks, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: b.cfg.CallbackDBPath,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = callbacks.Close() })
	b.callbackStore = callbacks
	b.operationIntents = callbacks

	items := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}
	resourceProfiles := testResourceProfiles(t, items)
	stack := restoreStackManifest()
	require.NoError(t, retentions.Put(shared.RetentionEntry{
		OriginalLeaseUUID: sourceLease,
		Tenant:            "tenant-a",
		ProviderUUID:      providerUUID,
		Items:             items,
		ResourceProfiles:  resourceProfiles,
		StackManifest:     stack,
		Status:            shared.RetentionStatusActive,
		CreatedAt:         time.Now(),
	}))
	operationID, callbackURL, lifecycleCallbackURL := restoreDestinationAuthority(t)
	_, err = retentions.ClaimForRestoreWithAuthority(
		sourceLease, destinationLease, 0,
		items, resourceProfiles,
		operationID, callbackURL, lifecycleCallbackURL,
	)
	require.NoError(t, err)
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	runtimeAuthority, err := shared.NewReleaseRuntimeAuthority(
		operationID, "tenant-a", providerUUID, callbackURL, lifecycleCallbackURL,
	)
	require.NoError(t, err)
	require.NoError(t, releases.Append(destinationLease, shared.Release{
		Manifest:         manifestBytes,
		Image:            "stack",
		OperationID:      operationID,
		Items:            items,
		ResourceProfiles: resourceProfiles,
		RuntimeAuthority: &runtimeAuthority,
		Status:           "active",
		CreatedAt:        time.Now(),
	}))

	return committedRestoreCallbackFixture{
		backend:              b,
		retentions:           retentions,
		releases:             releases,
		callbacks:            callbacks,
		sourceLease:          sourceLease,
		destinationLease:     destinationLease,
		providerUUID:         providerUUID,
		operationID:          operationID,
		callbackURL:          callbackURL,
		lifecycleCallbackURL: lifecycleCallbackURL,
		items:                items,
		resourceProfiles:     resourceProfiles,
		stack:                stack,
	}
}

func (f committedRestoreCallbackFixture) projectDestination(
	callbackURL, lifecycleCallbackURL string,
) {
	f.backend.provisionsMu.Lock()
	defer f.backend.provisionsMu.Unlock()
	f.backend.provisions[f.destinationLease] = &provision{ //exhaustruct:enforce
		ProvisionState: leasesm.ProvisionState{ //exhaustruct:enforce
			LeaseUUID:            f.destinationLease,
			Tenant:               "tenant-a",
			ProviderUUID:         f.providerUUID,
			SKU:                  f.items[0].SKU,
			Status:               backend.ProvisionStatusReady,
			Quantity:             1,
			CreatedAt:            time.Now(),
			FailCount:            0,
			LastError:            "",
			Reason:               "",
			Message:              "",
			CallbackURL:          callbackURL,
			LifecycleCallbackURL: lifecycleCallbackURL,
			Items:                f.items,
			ResourceProfiles:     shared.CloneSKUResourceSnapshot(f.resourceProfiles),
			ContainerIDs:         nil,
			StackManifest:        f.stack,
			ServiceContainers:    map[string][]string{},
		},
		ResourceProfiles:      f.resourceProfiles,
		VolumeCleanupAttempts: 0,
	}
}

func TestCommittedRestoreClose_KeepsFinalizerRouteUntilMovedCloseJournalHandoff(t *testing.T) {
	fixture := newCommittedRestoreCallbackFixture(t)
	movedLifecycleURL := "https://moved.example/callbacks/provision?lifecycle_id=" + fixture.operationID.String()
	movedCallbackURL, canonicalLifecycleURL, err := backend.ResolveMaintenanceCallbackURLs(
		fixture.callbackURL,
		fixture.lifecycleCallbackURL,
		movedLifecycleURL,
	)
	require.NoError(t, err)
	require.Equal(t, movedLifecycleURL, canonicalLifecycleURL)
	fixture.projectDestination(movedCallbackURL, movedLifecycleURL)
	manifestBytes, err := json.Marshal(fixture.stack)
	require.NoError(t, err)
	movedAuthority, err := shared.NewReleaseRuntimeAuthority(
		fixture.operationID,
		"tenant-a",
		fixture.providerUUID,
		movedCallbackURL,
		movedLifecycleURL,
	)
	require.NoError(t, err)
	require.NoError(t, fixture.releases.AppendActive(fixture.destinationLease, shared.Release{
		Manifest:         manifestBytes,
		Image:            "stack",
		OperationID:      fixture.operationID,
		Items:            fixture.items,
		ResourceProfiles: fixture.resourceProfiles,
		RuntimeAuthority: &movedAuthority,
		Status:           "active",
		CreatedAt:        time.Now(),
	}))

	unlock := fixture.backend.commandFence.Lock(fixture.destinationLease)
	require.NoError(t, fixture.backend.ensureCommittedRestoreDestinationForClose(fixture.destinationLease))
	stored, err := fixture.retentions.Get(fixture.sourceLease)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, fixture.callbackURL, stored.DestinationCallbackURL)
	assert.Equal(t, fixture.lifecycleCallbackURL, stored.DestinationLifecycleCallbackURL)

	closeClaim, found, err := fixture.backend.acquireCloseIntent(
		context.Background(),
		fixture.destinationLease,
		true,
		"tenant-a",
		fixture.providerUUID,
		fixture.items,
		fixture.stack,
		movedCallbackURL,
		movedLifecycleURL,
	)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, movedCallbackURL, closeClaim.CallbackURL())
	assert.Equal(t, movedLifecycleURL, closeClaim.LifecycleCallbackURL())
	require.NoError(t, fixture.backend.handoffCommittedRestoreToClose(
		fixture.destinationLease, closeClaim, true,
	))
	unlock()

	stored, err = fixture.retentions.Get(fixture.sourceLease)
	require.NoError(t, err)
	assert.Nil(t, stored, "the close journal now owns the exact restored destination")
}

func TestCommittedRestoreClose_RejectsDifferentLifecycleIDBeforeCloseJournal(t *testing.T) {
	fixture := newCommittedRestoreCallbackFixture(t)
	const differentID = "7ba7b810-9dad-41d1-80b4-00c04fd430c9"
	differentCallbackURL := "https://moved.example/callbacks/provision?operation_id=" + differentID
	differentLifecycleURL := "https://moved.example/callbacks/provision?lifecycle_id=" + differentID
	fixture.projectDestination(differentCallbackURL, differentLifecycleURL)

	err := fixture.backend.Deprovision(context.Background(), fixture.destinationLease)
	require.ErrorIs(t, err, backend.ErrInvalidState)
	_, found, readErr := fixture.callbacks.GetCloseIntent(fixture.destinationLease)
	require.NoError(t, readErr)
	assert.False(t, found, "different lifecycle authority must fail before close admission")
	stored, readErr := fixture.retentions.Get(fixture.sourceLease)
	require.NoError(t, readErr)
	require.NotNil(t, stored)
	assert.Equal(t, fixture.callbackURL, stored.DestinationCallbackURL)
	assert.Equal(t, fixture.lifecycleCallbackURL, stored.DestinationLifecycleCallbackURL)
}
