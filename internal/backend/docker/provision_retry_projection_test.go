package docker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

func TestProvisionRetryPublishesActorTransitionBeforeWorkerCompletion(t *testing.T) {
	for _, workerFails := range []bool{false, true} {
		name := "success"
		if workerFails {
			name = "ambiguous failure"
		}
		t.Run(name, func(t *testing.T) {
			const leaseUUID = "550e8400-e29b-41d4-a716-446655440190"
			payload := validManifestJSON("nginx:latest")
			started := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			mock := &mockDockerClient{
				PullImageFn: func(ctx context.Context, _ string, _ time.Duration) error {
					close(started)
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-release:
					}
					if workerFails {
						return errors.New("replacement image pull lost its response")
					}
					return nil
				},
			}
			b := newBackendForProvisionTest(t, mock, map[string]*provision{
				leaseUUID: {ProvisionState: leasesm.ProvisionState{
					LeaseUUID: leaseUUID, Status: backend.ProvisionStatusFailed,
					FailCount: 2, Quantity: 1, ContainerIDs: []string{"old-container"},
					LastError: "old diagnostics", Reason: backend.ReasonContainerExited,
					Message: "old container exited",
				}},
			})
			t.Cleanup(func() {
				unblock()
				b.stopCancel()
				b.wg.Wait()
			})
			require.NoError(t, b.pool.TryAllocate(leaseUUID+"-app-0", "docker-small", "tenant-a"))
			prepareFailedProvisionReplacement(t, b, mock, leaseUUID, "tenant-a", "docker-small", payload)
			b.cfg.StartupVerifyDuration = time.Millisecond
			previous, err := b.releaseStore.LatestActive(leaseUUID)
			require.NoError(t, err)
			require.NotNil(t, previous)

			req := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, payload)
			require.NoError(t, b.Provision(t.Context(), req))
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("replacement worker did not reach the image pull")
			}

			// Both externally visible projections must describe the actor's new
			// generation while its physical worker is blocked.
			info, err := b.GetProvision(t.Context(), leaseUUID)
			require.NoError(t, err)
			assert.Equal(t, backend.ProvisionStatusProvisioning, info.Status)
			assert.Empty(t, info.Reason)
			assert.Empty(t, info.Message)
			assert.Equal(t, 2, info.FailCount)
			listed, err := b.ListProvisions(t.Context())
			require.NoError(t, err)
			require.Len(t, listed, 1)
			assert.Equal(t, *info, listed[0])
			b.provisionsMu.RLock()
			assert.Empty(t, b.provisions[leaseUUID].LastError)
			assert.Equal(t, req.CallbackURL, b.provisions[leaseUUID].CallbackURL)
			b.provisionsMu.RUnlock()
			assert.Equal(t, backend.ProvisionStatusProvisioning, b.actorFor(leaseUUID).State())

			intents, err := b.operationSettlement.ListOperationIntents()
			require.NoError(t, err)
			require.Len(t, intents, 1)
			assert.Equal(t, shared.OperationExecutionStarted, intents[0].ExecutionPhase())
			assert.NotEqual(t, previous.OperationID, intents[0].OperationID())
			assert.NotNil(t, b.pool.GetAllocation(leaseUUID+"-app-0"))
			pending, err := b.callbackStore.ListPending()
			require.NoError(t, err)
			assert.Empty(t, pending)

			unblock()
			awaitProvisionWorkerQuiescence(t, b, leaseUUID)
			intents, err = b.operationSettlement.ListOperationIntents()
			require.NoError(t, err)
			active, err := b.releaseStore.LatestActive(leaseUUID)
			require.NoError(t, err)
			require.NotNil(t, active)
			info, err = b.GetProvision(t.Context(), leaseUUID)
			require.NoError(t, err)
			if workerFails {
				require.Len(t, intents, 1, "the exact Started attempt remains recoverable")
				assert.Equal(t, shared.OperationExecutionStarted, intents[0].ExecutionPhase())
				assert.Equal(t, *previous, *active, "failure must preserve predecessor runtime authority")
				assert.NotNil(t, b.pool.GetAllocation(leaseUUID+"-app-0"))
				assert.Equal(t, backend.ProvisionStatusProvisioning, info.Status,
					"an ambiguous worker result cannot publish terminal failure")
				pending, err = b.callbackStore.ListPending()
				require.NoError(t, err)
				assert.Empty(t, pending)
			} else {
				assert.Empty(t, intents)
				assert.NotEqual(t, previous.OperationID, active.OperationID)
				assert.Equal(t, backend.ProvisionStatusReady, info.Status)
			}
			assert.Empty(t, info.Reason)
			assert.Empty(t, info.Message)
		})
	}
}
