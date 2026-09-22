package docker

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics/background"
)

// The real merge emits this diagnostic while holding provisionsMu. A handler
// failure exercises the production panic boundary without adding a test hook or
// corrupting the projection that later passes must recover.
type mergePanicHandler struct {
	slog.Handler
	panicked atomic.Bool
}

func (h *mergePanicHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *mergePanicHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "cold-start: adjusted FailCount for already-failed provision" &&
		h.panicked.CompareAndSwap(false, true) {
		panic("merge diagnostic handler failed")
	}
	return h.Handler.Handle(ctx, record)
}

func TestReconciliationMergePanicReleasesLeaseLocksAndRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const leaseUUID = "0192f1a0-1111-4abc-8def-000000000999"
		mock := &mockDockerClient{
			ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
				return []ContainerInfo{{
					ContainerID: "failed-container", LeaseUUID: leaseUUID,
					Tenant: "t", SKU: "docker-small", ServiceName: "app", Status: "exited",
				}}, nil
			},
			InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
				return nil, errors.New("container disappeared before diagnostics")
			},
		}
		b := newBackendForTest(mock, nil)
		attachBoundOperationHandoffStores(t, b)
		b.cfg.ReconcileInterval = time.Minute
		handler := &mergePanicHandler{Handler: slog.DiscardHandler}
		b.logger = slog.New(handler)
		panics := background.CleanupPanicsTotal.WithLabelValues("docker_reconciliation")
		beforePanics := testutil.ToFloat64(panics)
		beforeSuccess := testutil.ToFloat64(reconciliationTotal.WithLabelValues("success"))
		defer func() { b.stopCancel(); b.wg.Wait() }()
		b.wg.Go(b.reconcileLoop)
		synctest.Wait()

		time.Sleep(time.Minute)
		synctest.Wait()
		require.True(t, handler.panicked.Load(), "the panic must occur inside the real merge, not before locking")
		require.Equal(t, beforePanics+1, testutil.ToFloat64(panics))
		require.Equal(t, beforeSuccess, testutil.ToFloat64(reconciliationTotal.WithLabelValues("success")))
		// Check the lock before invoking another blocking operation. The original
		// manual unlock fails here immediately, rather than hanging the test.
		require.True(t, b.provisionsMu.TryLock(), "contained merge panic leaked the lease projection lock")
		b.provisionsMu.Unlock()
		provisions, err := b.ListProvisions(t.Context())
		require.NoError(t, err)
		require.Empty(t, provisions, "a panicking candidate must not publish partial state")
		_, err = b.GetProvision(t.Context(), leaseUUID)
		require.ErrorIs(t, err, backend.ErrNotProvisioned)

		time.Sleep(time.Minute)
		synctest.Wait()
		require.Equal(t, beforeSuccess+1, testutil.ToFloat64(reconciliationTotal.WithLabelValues("success")),
			"the same worker must complete its next recovery pass")
		require.Equal(t, beforePanics+1, testutil.ToFloat64(panics))
		provision, err := b.GetProvision(t.Context(), leaseUUID)
		require.NoError(t, err)
		require.Equal(t, backend.ProvisionStatusFailed, provision.Status)
		require.Equal(t, 1, provision.FailCount, "an abandoned candidate must not increment live state")
		provisions, err = b.ListProvisions(t.Context())
		require.NoError(t, err)
		require.Len(t, provisions, 1)

		b.stopCancel()
		b.wg.Wait()
		time.Sleep(time.Minute)
		require.Equal(t, beforeSuccess+1, testutil.ToFloat64(reconciliationTotal.WithLabelValues("success")))
	})
}
