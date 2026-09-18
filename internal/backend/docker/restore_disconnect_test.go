package docker

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

func TestRestoreDisconnectRetainsBackendOwnedAdmission(t *testing.T) {
	for _, window := range []string{"before enqueue", "after enqueue"} {
		t.Run(window, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newVolumeWriterLaunchFixture(t, func(context.Context) ([]ContainerInfo, error) { return nil, nil })
				request := retainedRestoreRequest(t, f, "db")
				entered, resume := make(chan struct{}), make(chan struct{})
				release := sync.OnceFunc(func() { close(resume) })
				t.Cleanup(release)
				if window == "before enqueue" {
					f.b.releaseCapacityPlanner = &pausedProvisionCapacityPlanner{
						next: f.b.releaseCapacityPlanner, entered: entered, resume: resume,
					}
				} else {
					f.b.provisionStore = &pausedProvisionEntryStore{
						LeaseProvisionStore: f.b.provisionStore, entered: entered, resume: resume,
					}
				}
				caller, disconnect := context.WithCancel(t.Context())
				defer disconnect()
				returned := make(chan error, 1)
				go func() { returned <- f.b.Restore(caller, request) }()
				waitForOperationWorker(t, entered)
				disconnect()
				if window == "before enqueue" {
					release()
				}
				select {
				case err := <-returned:
					if err != nil {
						require.ErrorContains(t, err, "restore acceptance is unknown")
					}
				case <-time.After(provisionFlowTimeout):
					t.Fatal("restore admission exceeded its backend-owned acceptance bound")
				}
				if window == "after enqueue" {
					pending, err := f.b.operationSettlement.ListOperationIntents()
					require.NoError(t, err)
					require.Len(t, pending, 1)
					require.Equal(t, shared.OperationExecutionBeforeEffects, pending[0].ExecutionPhase())
					source, err := f.b.retentionStore.Get(f.source)
					require.NoError(t, err)
					require.Equal(t, shared.RetentionStatusRestoring, source.Status)
					require.Positive(t, f.b.pool.Stats().AllocatedCPU)
					callbacks, err := f.b.callbackStore.ListPending()
					require.NoError(t, err)
					require.Empty(t, callbacks)
				}
				release()
				awaitProvisionWorkerQuiescence(t, f.b, f.target)
				p, err := f.b.GetProvision(t.Context(), f.target)
				require.NoError(t, err)
				require.Equal(t, backend.ProvisionStatusReady, p.Status)
				callbacks, err := f.b.callbackStore.ListPending()
				require.NoError(t, err)
				require.Len(t, callbacks, 1)
				require.Equal(t, backend.CallbackStatusSuccess, callbacks[0].Status)
				require.NoError(t, f.b.Restore(t.Context(), request), "exact replay does not create another restore")
			})
		})
	}
}
