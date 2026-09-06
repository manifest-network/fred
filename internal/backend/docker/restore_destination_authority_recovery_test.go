package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

type resolveFailingOperationIntentJournal struct {
	callbackPublicationService
	err error
}

func (j resolveFailingOperationIntentJournal) PublishOperationFailureContext(
	context.Context,
	shared.OperationReleaseUncommitted,
	string,
) error {
	return j.err
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

			retentions, err := newBoundRetentionStoreForTest(t, shared.RetentionStoreConfig{DBPath: retentionPath})
			require.NoError(t, err)
			storeSet := retentionFixtureAuthorityForTest(t, retentions)
			callbacks := storeSet.callbacks
			releases := storeSet.releases
			require.NoError(t, putRetentionForTest(t, retentions, shared.RetentionEntry{
				OriginalLeaseUUID: sourceLease,
				Tenant:            "tenant-a",
				ProviderUUID:      providerUUID,
				Items:             sourceItems,
				ResourceProfiles:  sourceProfiles,
				StackManifest:     stack,
				Status:            shared.RetentionStatusActive,
				CreatedAt:         time.Now(),
			}))

			storageID := storeSet.storage.ID()
			callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID
			lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
			require.NoError(t, err)
			spec := shared.OperationIntentSpec{
				Kind:                 shared.OperationIntentRestore,
				LeaseUUID:            destinationLease,
				CallbackURL:          callbackURL,
				LifecycleCallbackURL: lifecycleURL,
				Tenant:               "tenant-a",
				ProviderUUID:         providerUUID,
				Items:                destinationItems,
				ResourceProfiles:     destinationProfiles,
				EffectiveItems:       destinationItems,
				Manifest:             manifestBytes,
				SourceLeaseUUID:      sourceLease,
				SourceGeneration:     1,
			}
			admission, err := beginDockerTestOperationIntent(t, callbacks, spec, storageID)
			require.NoError(t, err)
			operationClaim := createdDockerOperationClaim(t, admission)
			claimed, err := claimRetentionForTest(t, retentions,
				sourceLease, destinationLease, 0, destinationItems, destinationProfiles,
				operationClaim.OperationID(), callbackURL, lifecycleURL,
			)
			require.NoError(t, err)

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
					ActiveReleaseVersion: 0,
					ActiveOperationID:    mustDockerOperationID(operationID),
					Items:                destinationItems,
					ResourceProfiles:     shared.CloneSKUResourceSnapshot(destinationProfiles),
					ContainerIDs:         []string{"container-1"},
					StackManifest:        stack,
					ServiceContainers:    map[string][]string{"app": {"container-1"}},
				},
			}
			beforeCrash := newBackendForTest(&mockDockerClient{}, map[string]*provision{destinationLease: live})
			bindBackendToRetentionFixtureStore(t, beforeCrash, retentions)
			releaseCandidate, err := storeSet.operations.PrepareOperationRelease(operationClaim)
			require.NoError(t, err)
			_, err = storeSet.operations.StartOperationExecution(releaseCandidate)
			require.NoError(t, err)
			require.NoError(t, releases.Close(), "inject ownership Release failure")
			require.Error(t, beforeCrash.finalizeRestoredLeaseStrict(
				t.Context(), destinationLease, claimed, destinationItems,
			))
			intents, err := listOperationIntentsForCallbackTest(t, callbacks)
			require.NoError(t, err)
			require.Len(t, intents, 1,
				"success is impossible until the exact ownership Release commits")
			require.NoError(t, retentions.Close())
			require.NoError(t, callbacks.Close())
			beforeCrash.stopCancel()

			container := ContainerInfo{
				ContainerID:          "container-1",
				BackendName:          "docker",
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
				InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
					observed := container
					return &observed, nil
				},
			}, nil)
			t.Cleanup(afterCrash.stopCancel)
			mutateConfig.fn(&afterCrash.cfg)

			retentions, err = newBoundRetentionStoreForTest(t, shared.RetentionStoreConfig{DBPath: retentionPath})
			require.NoError(t, err)
			t.Cleanup(func() { _ = retentions.Close() })
			storeSet = retentionFixtureAuthorityForTest(t, retentions)
			releases = storeSet.releases
			bindBackendToRetentionFixtureStore(t, afterCrash, retentions)

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

			require.NoError(t, afterCrash.recoverOperationIntents(context.Background()))
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
	const (
		sourceLease      = "33333333-3333-4333-8333-333333333333"
		destinationLease = "44444444-4444-4444-8444-444444444444"
		otherDestination = "55555555-5555-4555-8555-555555555555"
	)
	for _, status := range []string{
		shared.RetentionStatusRestoring,
		shared.RetentionStatusReaping,
	} {
		t.Run(status, func(t *testing.T) {
			b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
			retentions := attachRetentionStore(t, b)
			source := shared.RetentionEntry{
				OriginalLeaseUUID: sourceLease,
				NewLeaseUUID:      otherDestination,
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
				source = *putRestoringRetention(t, retentions, source)
			} else {
				require.NoError(t, putRetentionForTest(t, retentions, source))
				stored, getErr := retentions.Get(sourceLease)
				require.NoError(t, getErr)
				require.NotNil(t, stored)
				source = *stored
			}
			b.operationSettlement = fixedOperationIntentProbeJournal{
				disposition: shared.OperationIntentAdmissionNone,
			}

			err := b.Restore(context.Background(), restoreRequest(
				destinationLease, sourceLease, "http://localhost/callbacks/provision",
			))
			require.ErrorIs(t, err, backend.ErrInvalidState)
			assert.NotContains(t, err.Error(), "unexpected BeginOperationIntent")
			b.provisionsMu.RLock()
			_, projected := b.provisions[destinationLease]
			b.provisionsMu.RUnlock()
			assert.False(t, projected)
			stored, getErr := retentions.Get(sourceLease)
			require.NoError(t, getErr)
			require.NotNil(t, stored)
			assert.Equal(t, status, stored.Status)
			assert.Equal(t, source.Generation, stored.Generation)
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
	require.ErrorIs(t, provisionErr, shared.ErrOperationIntentConflict)

	restoreErr := b.Restore(context.Background(), restoreRequest(
		destination, differentSource, "http://localhost/callbacks/provision",
	))
	require.ErrorIs(t, restoreErr, shared.ErrOperationIntentConflict)
	restartErr := b.Restart(context.Background(), backend.RestartRequest{
		MaintenanceID: newTestMaintenanceID(t), LeaseUUID: destination,
		CallbackURL: testMaintenanceLifecycleCallbackURL,
	})
	require.ErrorIs(t, restartErr, backend.ErrInvalidState)
	updateErr := b.Update(context.Background(), backend.UpdateRequest{
		MaintenanceID: newTestMaintenanceID(t), LeaseUUID: destination,
		CallbackURL: testMaintenanceLifecycleCallbackURL,
	})
	require.ErrorIs(t, updateErr, backend.ErrInvalidState)

	proof, err := retentions.ProveRestoringSnapshot(*claimed)
	require.NoError(t, err)
	claims, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1)
	require.NoError(t, b.resolvePreEffectOperationRefusal(
		claims[0], "test restore abandoned before effects",
	))
	acknowledgePendingCallbacksForTest(t, b.callbackStore)
	_, err = retentions.RollbackRestoring(proof, claimed.ResourceProfiles)
	require.NoError(t, err)
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
	restartErr = b.Restart(context.Background(), backend.RestartRequest{
		MaintenanceID: newTestMaintenanceID(t), LeaseUUID: destination,
		CallbackURL: testMaintenanceLifecycleCallbackURL,
	})
	require.ErrorIs(t, restartErr, backend.ErrNotProvisioned)
	updateErr = b.Update(context.Background(), backend.UpdateRequest{
		MaintenanceID: newTestMaintenanceID(t), LeaseUUID: destination,
		CallbackURL: testMaintenanceLifecycleCallbackURL,
	})
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
	require.NoError(t, putRetentionForTest(t, retentions, shared.RetentionEntry{
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
	_, err := claimRetentionForTest(t, retentions,
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

	restartErr := b.Restart(context.Background(), backend.RestartRequest{MaintenanceID: newTestMaintenanceID(t),
		LeaseUUID: destinationLease, CallbackURL: testMaintenanceLifecycleCallbackURL,
	})
	require.ErrorIs(t, restartErr, backend.ErrInvalidState)
}

func TestRestore_CanceledWhileWaitingForRecoverySnapshotHasNoSideEffects(t *testing.T) {
	const (
		sourceLease      = "0192f1a0-1111-7abc-8def-000000000209"
		destinationLease = "0192f1a0-2222-7abc-8def-000000000210"
	)
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	retentions := attachRetentionStore(t, b)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	seedActiveRetained(t, retentions, sourceLease)
	original, err := retentions.Get(sourceLease)
	require.NoError(t, err)
	require.NotNil(t, original)
	preSnapshotReadReached := make(chan struct{})
	b.volumes = &mockVolumeManager{
		UsageFn: func(context.Context, string) (int64, error) {
			close(preSnapshotReadReached)
			return 0, nil
		},
	}

	b.recoverySnapshotMu.Lock()
	var unlockSnapshot sync.Once
	unlock := func() { unlockSnapshot.Do(b.recoverySnapshotMu.Unlock) }
	t.Cleanup(unlock)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	restoreDone := make(chan error, 1)
	req := restoreRequest(
		destinationLease, sourceLease, "http://localhost/callbacks/provision",
	)
	// Force the read-only demote proof so Usage gives the test a positive
	// synchronization point at the final external read before recoverySnapshotMu.
	req.Items[0].SKU = "docker-micro"
	go func() {
		restoreDone <- b.Restore(ctx, req)
	}()

	// Prove Restore completed its last observable pre-lock read. With the writer
	// still held, it cannot cross into durable admission.
	waitForTestSignal(t, preSnapshotReadReached, "Restore's pre-snapshot demote proof")
	select {
	case err := <-restoreDone:
		t.Fatalf("Restore returned before reaching the recovery snapshot gate: %v", err)
	default:
	}
	cancel()

	unlock()
	require.ErrorIs(t, waitForAsyncTestResult(t, restoreDone, "canceled Restore"), context.Canceled)

	intents, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, intents)
	b.provisionsMu.RLock()
	_, projected := b.provisions[destinationLease]
	b.provisionsMu.RUnlock()
	assert.False(t, projected)
	assert.Nil(t, b.pool.GetAllocation(destinationLease+"-app-0"))
	current, err := retentions.Get(sourceLease)
	require.NoError(t, err)
	require.NotNil(t, current)
	assert.Equal(t, *original, *current)
}

func TestRecoverState_RestoreAdmissionAndRollbackCannotABA(t *testing.T) {
	const (
		sourceLease      = "0192f1a0-1111-7abc-8def-000000000211"
		destinationLease = "0192f1a0-2222-7abc-8def-000000000212"
		stableLease      = "0192f1a0-3333-7abc-8def-000000000213"
	)
	stableOperationID := mustDockerOperationID("9a72fbc2-38c8-4f31-87f7-f689979b9324")
	stableCallbackURL := "https://stable.example/callbacks/provision?operation_id=" + stableOperationID.String()
	stableLifecycleURL, err := backend.ResolveLifecycleCallbackURL(stableCallbackURL, "")
	require.NoError(t, err)
	stableItems := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}
	stableContainer := ContainerInfo{
		ContainerID: "stable-container", Name: "fred-" + stableLease + "-app-0", LeaseUUID: stableLease,
		Tenant: "tenant-b", ProviderUUID: nominalDockerProviderUUID,
		SKU: "docker-small", ServiceName: manifest.DefaultServiceName, InstanceIndex: 0,
		Image: "nginx:latest", CallbackURL: stableCallbackURL, LifecycleCallbackURL: stableLifecycleURL,
		Status: "running", Health: HealthStatusNone, CreatedAt: time.Now().Add(-time.Hour),
	}

	inventoryEntered := make(chan struct{})
	releaseInventory := make(chan struct{})
	var releaseInventoryOnce sync.Once
	t.Cleanup(func() { releaseInventoryOnce.Do(func() { close(releaseInventory) }) })
	rollbackInventoryEntered := make(chan struct{})
	releaseRollbackInventory := make(chan struct{})
	var releaseRollbackInventoryOnce sync.Once
	t.Cleanup(func() { releaseRollbackInventoryOnce.Do(func() { close(releaseRollbackInventory) }) })
	var firstRecovery = true
	var blockRollbackRecovery bool
	var staleRestoreContainer ContainerInfo
	var staleRestorePresent bool
	var removedStaleRestore int
	var inventoryMu sync.Mutex
	var b *Backend
	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			inventoryMu.Lock()
			if firstRecovery {
				firstRecovery = false
				inventoryMu.Unlock()
				close(inventoryEntered)
				<-releaseInventory
				return []ContainerInfo{stableContainer}, nil
			}
			if blockRollbackRecovery {
				containers := []ContainerInfo{stableContainer}
				if staleRestorePresent {
					containers = append(containers, staleRestoreContainer)
				}
				// Failed-operation cleanup deliberately runs before recoverState
				// opens its publication snapshot. Let those strict observations
				// see and remove the exact late generation. Park only the later
				// ordinary inventory while the snapshot write lock is held.
				publicationSnapshotHeld := !b.recoverySnapshotMu.TryLock()
				if !publicationSnapshotHeld {
					b.recoverySnapshotMu.Unlock()
					inventoryMu.Unlock()
					return containers, nil
				}
				blockRollbackRecovery = false
				inventoryMu.Unlock()
				close(rollbackInventoryEntered)
				<-releaseRollbackInventory
				return containers, nil
			}
			inventoryMu.Unlock()
			return []ContainerInfo{stableContainer}, nil
		},
		RemoveContainerFn: func(_ context.Context, containerID string) error {
			inventoryMu.Lock()
			defer inventoryMu.Unlock()
			if containerID != staleRestoreContainer.ContainerID || !staleRestorePresent {
				return fmt.Errorf("unexpected targeted removal of container %q", containerID)
			}
			staleRestorePresent = false
			removedStaleRestore++
			return nil
		},
		PullImageFn: func(context.Context, string, time.Duration) error {
			return nil
		},
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			inventoryMu.Lock()
			defer inventoryMu.Unlock()
			switch containerID {
			case stableContainer.ContainerID:
				observed := stableContainer
				return &observed, nil
			case staleRestoreContainer.ContainerID:
				observed := staleRestoreContainer
				return &observed, nil
			default:
				return nil, fmt.Errorf("unexpected inspect of container %q", containerID)
			}
		},
	}
	b = newBackendForProvisionTest(t, mock, nil)
	retentions := attachRetentionStore(t, b)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	seedActiveRetained(t, retentions, sourceLease)
	stableProfiles := testResourceProfiles(t, stableItems)
	stableAuthority, err := shared.NewReleaseRuntimeAuthority(
		stableOperationID, "tenant-b", nominalDockerProviderUUID,
		stableCallbackURL, stableLifecycleURL,
	)
	require.NoError(t, err)
	operations, ok := b.operationSettlement.(*shared.OperationSettlement)
	require.True(t, ok)
	seedProvisionReleaseForLeaseTest(t, b.callbackStore, b.releaseStore, operations, stableLease, shared.Release{
		Manifest: validManifestJSON("nginx:latest"), Image: "stack", OperationID: stableOperationID,
		Items: stableItems, ResourceProfiles: stableProfiles, RuntimeAuthority: &stableAuthority,
		Status: "active", CreatedAt: stableContainer.CreatedAt,
	})
	b.cfg.StartupVerifyDuration = time.Millisecond
	b.cfg.ProvisionTimeout = time.Millisecond
	b.compose = &mockComposeExecutor{
		UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error {
			return errors.New("compose up boom")
		},
		DownFn: func(context.Context, string, time.Duration) error {
			return nil
		},
	}
	restorePreSnapshotReadReached := make(chan struct{})
	rollbackHandoffReached := make(chan struct{})
	var signalRestorePreSnapshotRead sync.Once
	var signalRollbackHandoff sync.Once
	var rollbackHandoffMu sync.Mutex
	observeRollbackHandoff := false
	sourceRetainedVolume := retainedName(canonicalVolumeName(
		sourceLease, manifest.DefaultServiceName, 0,
	))
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(string, string) error { return nil },
		UsageFn: func(context.Context, string) (int64, error) {
			signalRestorePreSnapshotRead.Do(func() { close(restorePreSnapshotReadReached) })
			return 0, nil
		},
		EnsureQuotaFn: func(_ context.Context, name string, _ int64) error {
			rollbackHandoffMu.Lock()
			observe := observeRollbackHandoff
			rollbackHandoffMu.Unlock()
			if observe && name == sourceRetainedVolume {
				signalRollbackHandoff.Do(func() { close(rollbackHandoffReached) })
			}
			return nil
		},
	}

	operationAdmitted := make(chan struct{})
	b.operationSettlement = &stagedRecoveryOperationIntentJournal{
		operationSettlementService: b.operationSettlement,
		delegate:                   b.operationSettlement,
		operationAdmitted:          operationAdmitted,
	}
	recoverErr := make(chan error, 1)
	go func() { recoverErr <- b.recoverState(context.Background()) }()
	select {
	case <-inventoryEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("recovery did not enter Docker inventory")
	}

	// Recovery owns the write side after capturing an absent destination
	// baseline. Restore may complete all read-only validation, but it must not
	// create its durable operation intent (or any substrate/accounting state)
	// until that snapshot publishes. Otherwise the whole accepted restore could
	// fit between recovery's endpoint reads and stale inventory could resurrect
	// its rolled-back generation.
	restoreReq := restoreRequest(
		destinationLease, sourceLease, "http://localhost/callbacks/provision",
	)
	// Force a read-only demote measurement immediately before snapshot admission,
	// giving this test a positive pre-lock progress barrier.
	restoreReq.Items[0].SKU = "docker-micro"
	restoreLifecycleURL, err := backend.ResolveLifecycleCallbackURL(restoreReq.CallbackURL, "")
	require.NoError(t, err)
	staleRestoreContainer = ContainerInfo{
		ContainerID: "transient-restore-container", Name: "fred-" + destinationLease + "-app-0",
		BackendName: b.cfg.Name,
		LeaseUUID:   destinationLease, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
		SKU: restoreReq.Items[0].SKU, ServiceName: manifest.DefaultServiceName, InstanceIndex: 0,
		Image: "nginx:latest", CallbackURL: restoreReq.CallbackURL,
		LifecycleCallbackURL: restoreLifecycleURL, Status: "running",
		Health: HealthStatusNone, CreatedAt: time.Now(),
	}
	restoreDone := make(chan error, 1)
	go func() {
		restoreDone <- b.Restore(context.Background(), restoreReq)
	}()
	waitForTestSignal(t, restorePreSnapshotReadReached, "Restore's pre-snapshot demote proof")
	select {
	case <-operationAdmitted:
		t.Fatal("restore admitted while recovery held an absent destination snapshot")
	default:
	}

	releaseInventoryOnce.Do(func() { close(releaseInventory) })
	require.NoError(t, waitForAsyncTestResult(t, recoverErr, "initial recovery publication"))
	select {
	case <-operationAdmitted:
	case <-time.After(3 * time.Second):
		t.Fatal("restore did not admit after recovery published")
	}
	require.NoError(t, waitForAsyncTestResult(t, restoreDone, "Restore admission"))

	// The unrelated lease must still converge in the same recovery pass; the
	// restore fence is a lifecycle boundary, not a reason to abandon the fleet
	// snapshot or globally defer otherwise-stable leases.
	b.provisionsMu.RLock()
	stable := b.provisions[stableLease]
	b.provisionsMu.RUnlock()
	require.NotNil(t, stable)
	assert.Equal(t, backend.ProvisionStatusReady, stable.Status)
	assert.NotNil(t, b.pool.GetAllocation(stableLease+"-app-0"))

	// Drive the accepted operation through its real actor handoff. A failed
	// Compose call is ambiguous until the live operation-recovery lane obtains
	// actor quiescence and performs a second strict absence observation; ordinary
	// state recovery cannot mint that causal proof.
	require.Eventually(t, func() bool {
		actorClaim := b.tryClaimLeaseActorQuiescence(destinationLease)
		if actorClaim != nil {
			actorClaim.Release()
		}
		return actorClaim != nil
	}, 5*time.Second, 10*time.Millisecond, "accepted restore actor must become quiescent")
	require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
	claims, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	assert.Empty(t, claims, "operation recovery must durably settle the failed restore")
	entry, err := retentions.Get(sourceLease)
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, shared.RetentionStatusRestoring, entry.Status)

	// Now park a second recovery after it inventories the failed restore's
	// transient container. The production reconciler may perform slow physical
	// cleanup concurrently, but its final intent/source/pool/projection handoff
	// must wait for recovery publication. Without that read-side boundary it can
	// disappear completely while the stale snapshot is open, after which recovery
	// resurrects the destination allocation (the absent→transient→absent ABA).
	inventoryMu.Lock()
	blockRollbackRecovery = true
	staleRestorePresent = true
	inventoryMu.Unlock()
	rollbackRecoveryDone := make(chan error, 1)
	go func() { rollbackRecoveryDone <- b.recoverState(context.Background()) }()
	select {
	case <-rollbackInventoryEntered:
	case recoveryErr := <-rollbackRecoveryDone:
		t.Fatalf("rollback recovery returned before opening its publication snapshot: %v", recoveryErr)
	case <-time.After(3 * time.Second):
		t.Fatal("rollback recovery did not inventory the transient restore container")
	}
	inventoryMu.Lock()
	assert.False(t, staleRestorePresent,
		"durably failed restore substrate must be removed before ordinary recovery can publish it")
	assert.Equal(t, 1, removedStaleRestore)
	inventoryMu.Unlock()
	reconcileDone := make(chan error, 1)
	rollbackHandoffMu.Lock()
	observeRollbackHandoff = true
	rollbackHandoffMu.Unlock()
	go func() { reconcileDone <- b.reconcileRestoring(context.Background(), *entry) }()
	// EnsureQuota on the re-quarantined source is the final external mutation
	// before the recoverySnapshotMu read-side handoff. Reaching it proves the
	// reconciler progressed beyond teardown and is parked at the intended gate.
	waitForTestSignal(t, rollbackHandoffReached, "restore rollback's pre-snapshot quota handoff")
	select {
	case reconcileErr := <-reconcileDone:
		t.Fatalf("restore handback crossed an open recovery snapshot: %v", reconcileErr)
	default:
	}
	stillRestoring, err := retentions.Get(sourceLease)
	require.NoError(t, err)
	require.NotNil(t, stillRestoring)
	assert.Equal(t, shared.RetentionStatusRestoring, stillRestoring.Status)

	releaseRollbackInventoryOnce.Do(func() { close(releaseRollbackInventory) })
	require.NoError(t, waitForAsyncTestResult(t, rollbackRecoveryDone, "rollback recovery publication"))
	require.NoError(t, waitForAsyncTestResult(t, reconcileDone, "restore source handback"))

	entry, err = retentions.Get(sourceLease)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, shared.RetentionStatusActive, entry.Status)
	allocationID := destinationLease + "-" + manifest.DefaultServiceName + "-0"
	assert.Nil(t, b.pool.GetAllocation(allocationID))
	b.provisionsMu.RLock()
	_, destinationExists := b.provisions[destinationLease]
	b.provisionsMu.RUnlock()
	assert.False(t, destinationExists)
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
	require.NoError(t, putRetentionForTest(t, retentions, shared.RetentionEntry{
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
	_, err := claimRetentionForTest(t, retentions,
		sourceLease, destinationLease, 0, destinationItems, destinationProfiles,
		operationID, callbackURL, lifecycleURL,
	)
	require.NoError(t, err)
	commitPendingOperationReleaseForTest(t, b.operationSettlement, destinationLease)

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
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok)
	b.operationSettlement = operations
	b.releaseCapacityPlanner = operations
	retentions := attachRetentionStore(t, b)
	attachReleaseStore(t, b)
	callbacks := b.callbackStore
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
	require.NoError(t, putRetentionForTest(t, retentions, shared.RetentionEntry{
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
	callbackURL := "https://fred.example/callbacks/provision?operation_id=" + operationID
	lifecycleURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	claimed, err := claimRetentionForTest(t, retentions,
		sourceLease, destinationLease, 0, destinationItems, destinationProfiles,
		mustDockerOperationID(operationID), callbackURL, lifecycleURL,
	)
	require.NoError(t, err)
	claims, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	var operationClaim shared.OperationIntentClaim
	for _, claim := range claims {
		if claim.LeaseUUID() == destinationLease && claim.OperationID() == mustDockerOperationID(operationID) {
			operationClaim = claim
			break
		}
	}
	require.True(t, operationClaim.Valid(), "restore claim must remain recoverable from the durable journal")

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
			ActiveReleaseVersion: 0,
			ActiveOperationID:    mustDockerOperationID(operationID),
			Items:                destinationItems,
			ResourceProfiles:     shared.CloneSKUResourceSnapshot(destinationProfiles),
			ContainerIDs:         nil,
			StackManifest:        stack,
			ServiceContainers:    nil,
		},
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
	workingPublisher := b.callbackPublisher
	b.callbackPublisher = resolveFailingOperationIntentJournal{
		callbackPublicationService: b.callbackPublisher,
		err:                        settlementErr,
	}

	err = b.rollbackUnacceptedRestoreAdoption(
		destinationLease,
		[]string{allocationID},
		claimed,
		operationClaim,
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
	intents, err := listOperationIntentsForCallbackTest(t, callbacks)
	require.NoError(t, err)
	require.Len(t, intents, 1)

	b.callbackPublisher = workingPublisher
	require.NoError(t, b.recoverLiveOperationIntents(context.Background()))
	require.NoError(t, b.reconcileRestoring(context.Background(), *stored))
	stored, err = retentions.Get(sourceLease)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, shared.RetentionStatusActive, stored.Status)
	assert.Nil(t, b.pool.GetAllocation(allocationID))
	intents, err = listOperationIntentsForCallbackTest(t, callbacks)
	require.NoError(t, err)
	assert.Empty(t, intents)
	pending, err := callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
}

func restoreDestinationAuthority(t *testing.T) (shared.OperationID, string, string) {
	t.Helper()
	operationID := mustDockerOperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
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
	operations, ok := concreteOperationSettlementForTest(b.operationSettlement)
	require.True(t, ok)
	b.operationSettlement = operations
	b.releaseCapacityPlanner = operations
	retentions := attachRetentionStore(t, b)
	releases := attachReleaseStore(t, b)
	callbacks := b.callbackStore
	t.Cleanup(func() { _ = callbacks.Close() })

	items := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}
	resourceProfiles := testResourceProfiles(t, items)
	stack := restoreStackManifest()
	require.NoError(t, putRetentionForTest(t, retentions, shared.RetentionEntry{
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
	_, err := claimRetentionForTest(t, retentions,
		sourceLease, destinationLease, 0,
		items, resourceProfiles,
		operationID, callbackURL, lifecycleCallbackURL,
	)
	require.NoError(t, err)
	committed := commitPendingOperationReleaseForTest(t, b.operationSettlement, destinationLease)
	require.NoError(t, b.callbackPublisher.PublishOperationSuccessContext(context.Background(), committed))
	acknowledgePendingCallbacksForTest(t, callbacks)

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
			ActiveReleaseVersion: 0,
			ActiveOperationID:    f.operationID,
			Items:                f.items,
			ResourceProfiles:     shared.CloneSKUResourceSnapshot(f.resourceProfiles),
			ContainerIDs:         nil,
			StackManifest:        f.stack,
			ServiceContainers:    map[string][]string{},
		},
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
	maintenanceActive := activateMaintenanceReleaseForTest(
		t, fixture.backend.maintenanceSettlement, fixture.destinationLease,
		shared.MaintenanceIntentUpdate, shared.Release{
			Manifest:         manifestBytes,
			Image:            "stack",
			OperationID:      fixture.operationID,
			Items:            fixture.items,
			ResourceProfiles: fixture.resourceProfiles,
			RuntimeAuthority: &movedAuthority,
			Status:           "active",
			CreatedAt:        time.Now(),
		})
	require.NoError(t, fixture.backend.callbackPublisher.PublishMaintenanceSuccessContext(
		context.Background(), maintenanceActive,
	))
	acknowledgePendingCallbacksForTest(t, fixture.callbacks)

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
