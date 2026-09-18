package docker

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backendidentity"
)

func TestProvisionRetryWithoutCommittedReleaseReservesResources(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440191"
	pullFails := true
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: id, Status: "running"}, nil
		},
		PullImageFn: func(context.Context, string, time.Duration) error {
			if pullFails {
				return errors.New("image is unavailable")
			}
			return nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	b.cfg.StartupVerifyDuration = time.Millisecond
	rebuildCallbackSender(b, &http.Client{Transport: dockerReplayRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}, nil
	})})
	startCallbackReplayForTest(b)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	payload := validManifestJSON("nginx:latest")
	req := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, payload)
	require.NoError(t, b.Provision(t.Context(), req))
	awaitProvisionWorkerQuiescence(t, b, leaseUUID)
	failed, err := b.GetProvision(t.Context(), leaseUUID)
	require.NoError(t, err)
	require.Equal(t, backend.ProvisionStatusFailed, failed.Status)
	active, err := b.releaseStore.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.Nil(t, active, "the first pre-effect failure never committed a predecessor release")
	require.Empty(t, b.pool.ListAllocations())
	require.Eventually(t, func() bool {
		pending, err := b.callbackStore.ListPending()
		return err == nil && len(pending) == 0
	}, provisionFlowTimeout, time.Millisecond, "the first failure callback must finish before a successor is admitted")

	pullFails = false
	retry := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, payload)
	require.NoError(t, b.Provision(t.Context(), retry))
	awaitProvisionWorkerQuiescence(t, b, leaseUUID)
	ready, err := b.GetProvision(t.Context(), leaseUUID)
	require.NoError(t, err)
	require.Equal(t, backend.ProvisionStatusReady, ready.Status)
	containers, err := b.docker.ListManagedContainersStrict(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, containers)
	active, err = b.releaseStore.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	allocation := b.pool.GetAllocation(leaseUUID + "-app-0")
	require.NotNil(t, allocation, "every Ready replacement must own its exact immutable reservation")
	require.Len(t, active.ResourceProfiles, 1)
	assert.Equal(t, active.ResourceProfiles[0].CPUCores, allocation.CPUCores)
	assert.Equal(t, active.ResourceProfiles[0].MemoryMB, allocation.MemoryMB)
	assert.Positive(t, b.pool.Stats().AllocatedCPU)
}

func TestProvisionReplacementRefusesBeforePredecessorTeardown(t *testing.T) {
	for _, reason := range []string{"accounting hold", "capacity"} {
		t.Run(reason, func(t *testing.T) {
			const leaseUUID = "550e8400-e29b-41d4-a716-446655440192"
			mutations := 0
			mock := &mockDockerClient{
				RemoveContainerFn: func(context.Context, string) error {
					mutations++
					return nil
				},
				PullImageFn: func(context.Context, string, time.Duration) error {
					mutations++
					return nil
				},
			}
			b := newBackendForProvisionTest(t, mock, map[string]*provision{
				leaseUUID: {ProvisionState: leasesm.ProvisionState{
					LeaseUUID: leaseUUID, Status: backend.ProvisionStatusFailed,
					Quantity: 1, FailCount: 2, ContainerIDs: []string{"predecessor"},
				}},
			})
			t.Cleanup(func() { b.stopCancel(); b.wg.Wait() })
			payload := validManifestJSON("nginx:latest")
			prepareFailedProvisionReplacement(t, b, mock, leaseUUID, "tenant-a", "docker-small", payload)
			b.pool = shared.NewResourcePool(2, 8192, 16384, b.cfg.GetSKUProfile, nil)
			old := b.provisions[leaseUUID].ResourceProfiles[0]
			require.NoError(t, b.pool.TryAllocateResolved(leaseUUID+"-app-0", "tenant-a", old))
			before := b.pool.ListAllocations()
			projectionBefore := recoveredFromProvision(b.provisions[leaseUUID])
			activeBefore, err := b.releaseStore.LatestActive(leaseUUID)
			require.NoError(t, err)
			compose := b.compose.(*mockComposeExecutor)
			originalDown := compose.DownFn
			compose.DownFn = func(ctx context.Context, project string, timeout time.Duration) error {
				mutations++
				return originalDown(ctx, project, timeout)
			}
			req := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, payload)
			if reason == "accounting hold" {
				hold := b.pool.HoldUnaccountedFootprint()
				defer hold.Release()
			} else {
				b.cfg.SKUProfiles["docker-huge"] = SKUProfile{CPUCores: 3, MemoryMB: 512, DiskMB: 1024}
				req.Items[0].SKU = "docker-huge"
			}
			err = b.Provision(t.Context(), req)
			require.ErrorIs(t, err, backend.ErrInsufficientResources)
			if reason == "accounting hold" {
				require.ErrorIs(t, err, shared.ErrResourceAccountingIncomplete)
			}
			require.Zero(t, mutations, "a refused reservation must not tear down the predecessor or start work")
			assert.Equal(t, before, b.pool.ListAllocations())
			assert.Equal(t, projectionBefore, recoveredFromProvision(b.provisions[leaseUUID]))
			activeAfter, err := b.releaseStore.LatestActive(leaseUUID)
			require.NoError(t, err)
			assert.Equal(t, activeBefore, activeAfter)
			claims, err := b.operationSettlement.ListOperationIntents()
			require.NoError(t, err)
			assert.Empty(t, claims, "definitive pre-effect refusal must settle the candidate")
		})
	}
}

func TestProvisionInterruptedDowngradeRebuildsPredecessorEnvelope(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440193"
	mock := &mockDockerClient{
		RemoveContainerFn: func(context.Context, string) error { return errors.New("predecessor is busy") },
	}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: leaseUUID, Status: backend.ProvisionStatusFailed,
			Quantity: 1, FailCount: 2, ContainerIDs: []string{"predecessor"},
		}},
	})
	t.Cleanup(func() { b.stopCancel(); b.wg.Wait() })
	payload := validManifestJSON("nginx:latest")
	prepareFailedProvisionReplacement(t, b, mock, leaseUUID, "tenant-a", "docker-medium", payload)
	old := b.provisions[leaseUUID].ResourceProfiles[0]
	require.NoError(t, b.pool.TryAllocateResolved(leaseUUID+"-app-0", "tenant-a", old))
	b.compose.(*mockComposeExecutor).DownFn = func(context.Context, string, time.Duration) error {
		return errors.New("predecessor teardown is temporarily unavailable")
	}
	req := newProvisionRequest(leaseUUID, "tenant-a", "docker-micro", 1, payload)
	require.NoError(t, b.Provision(t.Context(), req))
	awaitProvisionWorkerQuiescence(t, b, leaseUUID)
	claims, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, claims, 1, "ambiguous teardown retains the exact candidate intent")
	assert.Equal(t, old.CPUCores, b.pool.Stats().AllocatedCPU)
	assert.Equal(t, old.MemoryMB, b.pool.Stats().AllocatedMemoryMB)
	assert.Equal(t, old.DiskMB, b.pool.Stats().AllocatedDiskMB)
	containers, err := b.docker.ListManagedContainersStrict(t.Context())
	require.NoError(t, err)
	require.Len(t, containers, 1, "the exact predecessor still consumes its larger footprint")
	b.stopCancel()
	b.wg.Wait()
	closeOperationSettlementForCallbackTest(t, b.callbackStore)
	require.NoError(t, b.callbackStore.Close())
	restartGate, err := backendidentity.NewStorageAuthorityGate(func(error) {})
	require.NoError(t, err)
	callbacks, err := shared.OpenIdentityBoundCallbackStore(
		shared.CallbackStoreConfig{DBPath: b.cfg.CallbackDBPath}, b.storageAuthority, restartGate,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callbacks.Close()) })
	operationIntentTestStoreIdentities.Store(callbacks, b.storageIdentity)
	operationIntentTestAuthorities.Store(callbacks, &operationIntentTestAuthority{
		storage: b.storageAuthority, gate: restartGate,
		releasePath: b.cfg.ReleasesDBPath, retentionPath: b.cfg.RetentionDBPath,
	})

	// Rebuild with a fresh process-local pool and projection, using only the same
	// durable journal pair and actual surviving predecessor inventory. No live
	// admission token crosses this boundary, and mutable SKU resizing is irrelevant.
	recovered := newOperationIntentRecoveryBackend(t, callbacks, b.storageIdentity, containers, nil)
	recovered.cfg.ProvisionTimeout = time.Hour
	recovered.cfg.SKUProfiles["docker-medium"] = SKUProfile{CPUCores: 7, MemoryMB: 7000, DiskMB: 7000}
	require.Empty(t, recovered.pool.ListAllocations())
	require.NoError(t, recovered.recoverState(t.Context()))
	assert.Equal(t, old.CPUCores, recovered.pool.Stats().AllocatedCPU)
	assert.Equal(t, old.MemoryMB, recovered.pool.Stats().AllocatedMemoryMB)
	assert.Equal(t, old.DiskMB, recovered.pool.Stats().AllocatedDiskMB)
}
