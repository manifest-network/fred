package docker

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// The real release-capacity interface is reached after durable intent creation
// and while fresh admission still holds the recovery publication bridge.
// Pausing it controls the disconnect/enqueue ordering without production hooks.
type pausedProvisionCapacityPlanner struct {
	next interface {
		CheckOperationReleaseCapacity(shared.OperationReleaseCandidate) error
	}
	entered chan struct{}
	resume  chan struct{}
}

// Actor entry publishes through this existing consumer interface before it
// acknowledges the command. The pause therefore proves successful enqueue
// without permitting worker admission or a terminal reply yet.
type pausedProvisionEntryStore struct {
	leasesm.LeaseProvisionStore
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (store *pausedProvisionEntryStore) UpdateFn(leaseUUID string, update func(*leasesm.ProvisionState)) bool {
	store.once.Do(func() {
		close(store.entered)
		<-store.resume
	})
	return store.LeaseProvisionStore.UpdateFn(leaseUUID, update)
}

func TestProvisionDisconnectAfterEnqueuePreservesUnknownAcceptance(t *testing.T) {
	for _, interruption := range []string{"caller disconnect", "backend shutdown", "operation deadline"} {
		t.Run(interruption, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const leaseUUID = "550e8400-e29b-41d4-a716-446655440272"
				var pulls atomic.Int32
				b := newBackendForProvisionTest(t, &mockDockerClient{
					PullImageFn: func(context.Context, string, time.Duration) error {
						pulls.Add(1)
						return nil
					},
					InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
						return &ContainerInfo{ContainerID: id, Status: "running"}, nil
					},
				}, nil)
				b.cfg.StartupVerifyDuration = time.Millisecond
				if interruption == "operation deadline" {
					b.cfg.ProvisionTimeout = 100 * time.Millisecond
				}
				store := &pausedProvisionEntryStore{
					LeaseProvisionStore: b.provisionStore,
					entered:             make(chan struct{}), resume: make(chan struct{}),
				}
				b.provisionStore = store
				var releaseEntry sync.Once
				t.Cleanup(func() {
					releaseEntry.Do(func() { close(store.resume) })
					b.stopCancel()
					b.wg.Wait()
				})
				request := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
				caller, disconnect := context.WithCancel(t.Context())
				defer disconnect()
				returned := make(chan error, 1)
				go func() { returned <- b.Provision(caller, request) }()
				waitForOperationWorker(t, store.entered)
				before := b.pool.Stats()
				require.Positive(t, before.AllocatedCPU)
				switch interruption {
				case "caller disconnect":
					disconnect()
				case "backend shutdown":
					b.stopCancel()
				}
				select {
				case err := <-returned:
					require.ErrorContains(t, err, "provision acceptance is unknown")
				case <-time.After(provisionFlowTimeout):
					t.Fatal("acceptance wait outlived the caller/backend operation lifetime")
				}
				intents, err := b.operationSettlement.ListOperationIntents()
				require.NoError(t, err)
				require.Len(t, intents, 1)
				assert.Equal(t, shared.OperationExecutionBeforeEffects, intents[0].ExecutionPhase())
				assert.Equal(t, before.AllocatedCPU, b.pool.Stats().AllocatedCPU)
				assert.Zero(t, pulls.Load())
				pending, err := b.callbackStore.ListPending()
				require.NoError(t, err)
				assert.Empty(t, pending, "unknown acceptance must not publish a definitive refusal")
				if interruption != "caller disconnect" {
					return
				}
				require.NoError(t, b.Provision(t.Context(), request), "exact replay recognizes the queued durable operation")
				releaseEntry.Do(func() { close(store.resume) })
				awaitProvisionWorkerQuiescence(t, b, leaseUUID)
				provision, err := b.GetProvision(t.Context(), leaseUUID)
				require.NoError(t, err)
				assert.Equal(t, backend.ProvisionStatusReady, provision.Status)
				assert.Equal(t, int32(1), pulls.Load())
			})
		})
	}
}

func (planner *pausedProvisionCapacityPlanner) CheckOperationReleaseCapacity(candidate shared.OperationReleaseCandidate) error {
	close(planner.entered)
	<-planner.resume
	return planner.next.CheckOperationReleaseCapacity(candidate)
}

func TestProvisionDisconnectBeforeEnqueueKeepsExactDurableOperation(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440271"
	workerEntered := make(chan struct{})
	workerResume := make(chan struct{})
	var pulls atomic.Int32
	var releaseWorker sync.Once
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, _ string, _ time.Duration) error {
			if pulls.Add(1) == 1 {
				close(workerEntered)
			}
			select {
			case <-workerResume:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: id, Status: "running"}, nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	b.cfg.StartupVerifyDuration = time.Millisecond
	planner := &pausedProvisionCapacityPlanner{
		next: b.releaseCapacityPlanner, entered: make(chan struct{}), resume: make(chan struct{}),
	}
	b.releaseCapacityPlanner = planner
	var releaseAdmission sync.Once
	t.Cleanup(func() {
		releaseAdmission.Do(func() { close(planner.resume) })
		releaseWorker.Do(func() { close(workerResume) })
		b.stopCancel()
		b.wg.Wait()
	})
	request := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	caller, disconnect := context.WithCancel(t.Context())
	defer disconnect()
	returned := make(chan error, 1)
	go func() { returned <- b.Provision(caller, request) }()
	waitForOperationWorker(t, planner.entered)
	intents, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	operationID := intents[0].OperationID()
	require.Equal(t, shared.OperationExecutionBeforeEffects, intents[0].ExecutionPhase())
	require.Zero(t, pulls.Load())

	disconnect()
	releaseAdmission.Do(func() { close(planner.resume) })
	select {
	case err = <-returned:
		if err != nil {
			assert.Contains(t, err.Error(), "provision acceptance is unknown",
				"disconnect can abandon the response, not the durable operation")
		}
	case <-time.After(provisionFlowTimeout):
		t.Fatal("disconnected admission did not return")
	}
	intents, err = b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1, "disconnect before enqueue must not persist terminal Failed")
	assert.Equal(t, operationID, intents[0].OperationID())
	waitForOperationWorker(t, workerEntered)
	require.NoError(t, b.Provision(t.Context(), request), "provider recovery redelivers the exact operation")
	assert.Equal(t, int32(1), pulls.Load(), "exact replay must not launch a second worker")

	releaseWorker.Do(func() { close(workerResume) })
	awaitProvisionWorkerQuiescence(t, b, leaseUUID)
	provision, err := b.GetProvision(t.Context(), leaseUUID)
	require.NoError(t, err)
	assert.Equal(t, backend.ProvisionStatusReady, provision.Status)
	active, err := b.releaseStore.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, operationID, active.OperationID)
	pending, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, backend.CallbackStatusSuccess, pending[0].Status)
	require.NoError(t, b.Provision(t.Context(), request))
	assert.Equal(t, int32(1), pulls.Load())
}

func TestProvisionShutdownBeforeEnqueueReleasesUnacceptedAdmission(t *testing.T) {
	const leaseUUID = "550e8400-e29b-41d4-a716-446655440273"
	var pulls atomic.Int32
	b := newBackendForProvisionTest(t, &mockDockerClient{
		PullImageFn: func(context.Context, string, time.Duration) error {
			pulls.Add(1)
			return nil
		},
	}, nil)
	planner := &pausedProvisionCapacityPlanner{
		next: b.releaseCapacityPlanner, entered: make(chan struct{}), resume: make(chan struct{}),
	}
	b.releaseCapacityPlanner = planner
	var releaseAdmission sync.Once
	t.Cleanup(func() {
		releaseAdmission.Do(func() { close(planner.resume) })
		b.stopCancel()
		b.wg.Wait()
	})
	before := b.pool.Stats()
	request := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	returned := make(chan error, 1)
	go func() { returned <- b.Provision(t.Context(), request) }()
	waitForOperationWorker(t, planner.entered)
	intents, err := b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1, "shutdown happens after the exact durable intent exists")
	operationID := intents[0].OperationID()
	b.stopCancel()
	releaseAdmission.Do(func() { close(planner.resume) })
	select {
	case err = <-returned:
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "provision acceptance is unknown", "no command reached an actor")
	case <-time.After(provisionFlowTimeout):
		t.Fatal("shutdown did not stop admission")
	}
	assert.Zero(t, pulls.Load())
	assert.Equal(t, before.AllocatedCPU, b.pool.Stats().AllocatedCPU)
	assert.Equal(t, before.AllocatedMemoryMB, b.pool.Stats().AllocatedMemoryMB)
	b.actorsMu.Lock()
	assert.Empty(t, b.actors)
	b.actorsMu.Unlock()
	// Shutdown prevents callback publication. The exact pre-effect head remains
	// for recovery; releasing an admission never substitutes for settlement.
	intents, err = b.operationSettlement.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, intents, 1)
	assert.Equal(t, operationID, intents[0].OperationID())
	assert.Equal(t, shared.OperationExecutionBeforeEffects, intents[0].ExecutionPhase())
	pending, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestProvisionAdmissionExplicitRejectionRemainsDefinitive(t *testing.T) {
	const leaseUUID = durableCallbackTestLeaseUUID
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: leaseUUID, Status: backend.ProvisionStatusReady,
		}},
	})
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	require.Equal(t, backend.ProvisionStatusReady, b.actorFor(leaseUUID).State())
	admission := actorOperationClaimForTest(t, leaseUUID)
	acceptance, err := b.handoffProvisionAdmission(t.Context(), admission)
	require.Error(t, err)
	assert.Equal(t, asyncAcceptanceRejected, acceptance)
	assert.False(t, admission.Valid(), "explicit actor rejection releases the unbegun admission")
}

func TestProvisionRefusalDiagnosticKeepsCauseAndCuratedCallback(t *testing.T) {
	b := newBackendForProvisionTest(t, &mockDockerClient{}, nil)
	defer b.stopCancel()
	var logs bytes.Buffer
	b.logger = slog.New(slog.NewTextHandler(&logs, nil))
	planner := &refusingReleaseHistoryCapacityPlanner{}
	b.releaseCapacityPlanner = planner
	request := newProvisionRequest(durableCallbackTestLeaseUUID, "tenant-a", "docker-small", 1,
		validManifestJSON("nginx:latest"))
	err := b.Provision(t.Context(), request)
	require.ErrorIs(t, err, shared.ErrReleaseHistoryCapacity)
	assertCapacityRefusalSettled(t, b.callbackStore)
	assert.Contains(t, logs.String(), "operation refused before asynchronous acceptance")
	assert.Contains(t, logs.String(), "lease_uuid="+durableCallbackTestLeaseUUID)
	assert.Contains(t, logs.String(), "operation_fingerprint=")
	assert.Contains(t, logs.String(), planner.capacityError().Error())
	assert.NotContains(t, logs.String(), request.CallbackURL)
	assert.NotContains(t, logs.String(), string(request.Payload))
}
