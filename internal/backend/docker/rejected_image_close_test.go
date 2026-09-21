package docker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

// Image-pull refusal settles before a Release exists, but leaves a public
// Failed projection. Closing that projection must consume its exact terminal
// operation even after the failure callback has left the outbox.
func TestDeprovisionRetiresRejectedImageProjection(t *testing.T) {
	for _, mode := range []string{"failure callback pending", "failure callback acknowledged", "close recovery after reopen"} {
		t.Run(mode, func(t *testing.T) {
			const leaseUUID = "01a0b5d3-c4e2-7bc4-beed-92e8bd3732bc"
			var callbackMu sync.Mutex
			var received []backend.CallbackStatus
			server := newCallbackTestServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body backend.CallbackPayload
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				callbackMu.Lock()
				received = append(received, body.Status)
				callbackMu.Unlock()
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			var unavailable atomic.Bool
			var launched atomic.Int32
			var composeDown, destroyed atomic.Int32
			mock := &mockDockerClient{
				PullImageFn: func(context.Context, string, time.Duration) error {
					return errors.New("unauthorized: authentication required")
				},
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
					if unavailable.Load() {
						return nil, errors.New("Docker inventory temporarily unavailable")
					}
					return nil, nil
				},
			}
			dir := t.TempDir()
			volumes := &mockVolumeManager{DestroyFn: func(context.Context, string) error {
				destroyed.Add(1)
				return nil
			}}
			b, stores := openCloseRecoveryBackend(t, dir, mock, volumes)
			// Retention policy cannot strand a refusal that created no volumes.
			b.cfg.RetainOnClose = mode == "failure callback acknowledged"
			b.releaseCapacityPlanner = stores.operations
			b.compose = &mockComposeExecutor{
				UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error { launched.Add(1); return nil },
				DownFn: func(context.Context, string, time.Duration) error {
					composeDown.Add(1)
					return nil
				},
			}
			rebuildCallbackSender(b, server.Client())
			t.Cleanup(func() { b.stopCancel(); b.wg.Wait(); closeCloseRecoveryBackend(t, b, stores) })
			req := newProvisionRequest(leaseUUID, "tenant-a", "docker-small", 1, validManifestJSON("docker.io/lifted/demo-games:tetris"))
			req.CallbackURL = testOperationCallbackURL(server.URL)
			require.NoError(t, b.Provision(t.Context(), req))
			require.Eventually(t, func() bool {
				info, err := b.GetProvision(t.Context(), leaseUUID)
				pending, pendingErr := stores.callbacks.ListPending()
				return err == nil && pendingErr == nil && info.Status == backend.ProvisionStatusFailed && len(pending) == 1
			}, 5*time.Second, time.Millisecond)
			releaseHistory, err := stores.releases.List(leaseUUID)
			require.NoError(t, err)
			require.Empty(t, releaseHistory)
			require.Zero(t, launched.Load(), "registry refusal precedes Compose dispatch")
			failures, err := stores.operations.ListFailedOperationReceipts()
			require.NoError(t, err)
			require.Len(t, failures, 1)
			failedOperation := failures[0].OperationID()
			if mode != "failure callback pending" {
				startCallbackReplayForTest(b)
				require.Eventually(t, func() bool { pending, err := stores.callbacks.ListPending(); return err == nil && len(pending) == 0 }, 5*time.Second, time.Millisecond)
			}
			if mode == "close recovery after reopen" {
				unavailable.Store(true)
				require.ErrorContains(t, b.Deprovision(t.Context(), leaseUUID), "Docker inventory temporarily unavailable")
				claim, found, err := stores.close.GetCloseIntent(leaseUUID)
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, failedOperation, claim.InterruptedOperationID())
				b.stopCancel()
				b.wg.Wait()
				closeCloseRecoveryBackend(t, b, stores)
				unavailable.Store(false)
				b, stores = openCloseRecoveryBackend(t, dir, mock, nil)
				rebuildCallbackSender(b, server.Client())
				require.NoError(t, b.recoverState(t.Context()))
			} else {
				require.NoError(t, b.Deprovision(t.Context(), leaseUUID))
			}
			if mode == "failure callback acknowledged" {
				require.Zero(t, composeDown.Load(), "the exact failed receipt selects captured-cohort cleanup")
				require.Zero(t, destroyed.Load(), "empty actual retention inventory needs no physical mutation")
			}
			provisions, err := b.ListProvisions(t.Context())
			require.NoError(t, err)
			require.Empty(t, provisions, "retired image refusal must not be rediscovered as an orphan")
			require.Zero(t, b.pool.Stats().AllocationCount)
			closeIntents, err := stores.close.ListCloseIntents()
			require.NoError(t, err)
			require.Empty(t, closeIntents)
			receipts, err := stores.callbacks.LookupClosedLeaseReceipts([]string{leaseUUID})
			require.NoError(t, err)
			require.Len(t, receipts, 1)
			require.False(t, receipts[0].CleanupOnly(), "a live projection retains its exact lifecycle callback authority")
			if mode == "failure callback pending" {
				pending, err := stores.callbacks.ListPending()
				require.NoError(t, err)
				require.Len(t, pending, 2)
				require.Equal(t, backend.CallbackStatusFailed, pending[0].Status)
				require.Equal(t, backend.CallbackStatusDeprovisioned, pending[1].Status)
				require.Less(t, pending[0].Sequence, pending[1].Sequence)
			}
			if mode != "failure callback acknowledged" {
				startCallbackReplayForTest(b)
			}
			b.callbackSender.NotifyPendingCallbacks()
			require.Eventually(t, func() bool { pending, err := stores.callbacks.ListPending(); return err == nil && len(pending) == 0 }, 5*time.Second, time.Millisecond)
			callbackMu.Lock()
			actual := append([]backend.CallbackStatus(nil), received...)
			callbackMu.Unlock()
			require.Equal(t, []backend.CallbackStatus{backend.CallbackStatusFailed, backend.CallbackStatusDeprovisioned}, actual)
			require.NoError(t, b.Deprovision(t.Context(), leaseUUID), "terminal retry is idempotent")
		})
	}
}
