package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/chain"
	"github.com/manifest-network/fred/internal/chain/chaintest"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

type deferredCloseBackend struct {
	*mockManagerBackend
	rows  []backend.ProvisionInfo
	close func(context.Context, string) error
}

func (client *deferredCloseBackend) ListProvisionsWithIdentity(context.Context) ([]backend.ProvisionInfo, backendidentity.ID, error) {
	return client.rows, testBackendStorageID(client.name), nil
}

func (client *deferredCloseBackend) Deprovision(ctx context.Context, leaseUUID string) error {
	return client.close(ctx, leaseUUID)
}

// Only production inventory receipts issue the deferrals used by scheduler
// tests. The helper keeps the collection open until the caller projects it.
func newDeferredCloseFixture(t *testing.T, count int, closeCall func(context.Context, string) error) (
	*Manager, []placement.DeferredDeprovision, func(),
) {
	t.Helper()
	client := &deferredCloseBackend{mockManagerBackend: &mockManagerBackend{name: "backend-a"}, close: closeCall}
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}}})
	require.NoError(t, err)
	manager, err := newTestManager(t, ManagerConfig{}, router, &chaintest.MockClient{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- manager.Start(ctx) }()
	<-manager.Running()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, manager.Close())
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("manager did not stop")
		}
	})
	projection := placement.ReconciliationProjection{Placements: make(map[string]string, count)}
	for i := range count {
		lease := fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1)
		client.rows = append(client.rows, backend.ProvisionInfo{LeaseUUID: lease, Status: backend.ProvisionStatusReady})
		projection.Placements[lease] = client.name
	}
	reconciler, err := manager.executionCoordinator.ReconciliationCoordinator(nil, nil)
	require.NoError(t, err)
	sweep, err := reconciler.BeginSweep()
	require.NoError(t, err)
	t.Cleanup(sweep.End)
	provisions, err := sweep.CollectProvisionInventory(t.Context(), client.name)
	require.NoError(t, err)
	retentions, err := sweep.CollectRetentionInventory(t.Context(), client.name)
	require.NoError(t, err)
	_, err = sweep.RecordBackendInventory(provisions, retentions)
	require.NoError(t, err)
	coordinator := manager.handlers.events.orchestrator.coordinator
	proofs := make([]placement.DeferredDeprovision, 0, count)
	for _, row := range client.rows {
		result := coordinator.DeprovisionEvent(t.Context(), row.LeaseUUID)
		require.Equal(t, placement.DeprovisionEventDeferred, result.Disposition(), "%v", result.Err())
		proofs = append(proofs, result.Deferred())
	}
	return manager, proofs, func() {
		require.NoError(t, sweep.SealInventory())
		_, err := sweep.Project(projection)
		require.NoError(t, err)
		sweep.End()
	}
}

func TestManagerCloseEventResumesAfterInventoryProjectionWithoutRedelivery(t *testing.T) {
	var calls atomic.Int32
	manager, proofs, project := newDeferredCloseFixture(t, 1, func(context.Context, string) error {
		calls.Add(1)
		return nil
	})
	poisonedBefore := promtestutil.ToFloat64(metrics.PoisonedMessagesTotal)
	require.NoError(t, manager.PublishLeaseEvent(chain.LeaseEvent{
		Type: chain.LeaseClosed, LeaseUUID: proofs[0].LeaseUUID(), Tenant: "tenant-test",
	}))
	require.Eventually(t, func() bool {
		manager.deferredCloses.mu.Lock()
		defer manager.deferredCloses.mu.Unlock()
		return len(manager.deferredCloses.entries) == 1
	}, 5*time.Second, time.Millisecond)
	time.Sleep(time.Second) // Longer than all three original event retry delays.
	require.Zero(t, calls.Load(), "unprojected positive remains a hard dispatch fence")
	project()
	require.Eventually(t, func() bool { return calls.Load() == 1 }, 7*time.Second, time.Millisecond)
	require.Equal(t, poisonedBefore, promtestutil.ToFloat64(metrics.PoisonedMessagesTotal))
}

func TestManagerCloseEventDefersTransportLifecyclePendingWithoutPoisoning(t *testing.T) {
	const lease = "00000000-0000-4000-8000-000000000001"
	const backendName = "pending-close"
	var drained atomic.Bool
	var inventoryReady atomic.Bool
	var calls, teardown atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(backendidentity.ResponseHeader, testBackendStorageID(backendName).String())
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/provisions":
			if !inventoryReady.Load() {
				_, _ = w.Write([]byte(`{"provisions":[]}`))
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"provisions": []backend.ProvisionInfo{
				{LeaseUUID: lease, Status: backend.ProvisionStatusProvisioning},
			}})
		case "/retentions":
			_, _ = w.Write([]byte(`{"retentions":[]}`))
		case "/deprovision":
			calls.Add(1)
			if !drained.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":"pending","code":"lifecycle_pending"}`))
				return
			}
			teardown.Add(1)
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	client := newBackendHTTPClientForTest(t, backendHTTPClientConfig{
		Name: backendName, BaseURL: server.URL, Secret: fleetSecret, Timeout: time.Second,
	})
	router, err := backend.NewRouter(backend.RouterConfig{Backends: []backend.BackendEntry{{Backend: client, IsDefault: true}}})
	require.NoError(t, err)
	manager, err := newTestManager(t, ManagerConfig{}, router, &chaintest.MockClient{})
	require.NoError(t, err)
	inventoryReady.Store(true)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- manager.Start(ctx) }()
	<-manager.Running()
	t.Cleanup(func() {
		cancel()
		require.NoError(t, manager.Close())
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("manager did not stop")
		}
	})
	reconciler, err := manager.executionCoordinator.ReconciliationCoordinator(nil, nil)
	require.NoError(t, err)
	sweep, err := reconciler.BeginSweep()
	require.NoError(t, err)
	t.Cleanup(sweep.End)
	provisions, err := sweep.CollectProvisionInventory(t.Context(), backendName)
	require.NoError(t, err)
	retentions, err := sweep.CollectRetentionInventory(t.Context(), backendName)
	require.NoError(t, err)
	_, err = sweep.RecordBackendInventory(provisions, retentions)
	require.NoError(t, err)
	require.NoError(t, sweep.SealInventory())
	_, err = sweep.Project(placement.ReconciliationProjection{Placements: map[string]string{lease: backendName}})
	require.NoError(t, err)
	sweep.End()
	result := manager.handlers.events.orchestrator.coordinator.DeprovisionEvent(t.Context(), lease)
	require.Equal(t, placement.DeprovisionEventDeferred, result.Disposition(), "%v", result.Err())
	require.Equal(t, placement.DeprovisionDeferredLifecycle, result.Deferred().Reason())
	require.Zero(t, teardown.Load())
	poisonedBefore := promtestutil.ToFloat64(metrics.PoisonedMessagesTotal)
	require.NoError(t, manager.PublishLeaseEvent(chain.LeaseEvent{
		Type: chain.LeaseClosed, LeaseUUID: lease, Tenant: "tenant-test",
	}))
	require.Eventually(t, func() bool {
		manager.deferredCloses.mu.Lock()
		defer manager.deferredCloses.mu.Unlock()
		return len(manager.deferredCloses.entries) == 1
	}, 5*time.Second, time.Millisecond)
	time.Sleep(time.Second) // Exceed the complete original Watermill retry budget.
	require.Zero(t, teardown.Load(), "pending worker ownership forbids teardown")
	require.Equal(t, poisonedBefore, promtestutil.ToFloat64(metrics.PoisonedMessagesTotal))
	drained.Store(true)
	require.Eventually(t, func() bool {
		manager.deferredCloses.mu.Lock()
		defer manager.deferredCloses.mu.Unlock()
		return len(manager.deferredCloses.entries) == 0
	}, 7*time.Second, time.Millisecond)
	require.EqualValues(t, 1, teardown.Load(), "the retained hint must retry cleanup exactly once after drain")
	require.GreaterOrEqual(t, calls.Load(), int32(3))
	require.Equal(t, poisonedBefore, promtestutil.ToFloat64(metrics.PoisonedMessagesTotal))
}

func TestDeferredCloseCoalescesAndRetainsCapacityThroughExecution(t *testing.T) {
	var active, maximum atomic.Int32
	var release chan struct{}
	_, proofs, project := newDeferredCloseFixture(t, deferredCloseCapacity+1, func(ctx context.Context, _ string) error {
		current := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); current > old && !maximum.CompareAndSwap(old, current); old = maximum.Load() {
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	project()
	synctest.Test(t, func(t *testing.T) {
		release = make(chan struct{})
		scheduler := newDeferredCloseScheduler()
		scheduler.start(t.Context())
		defer func() { scheduler.stop(); scheduler.wg.Wait() }()
		for _, proof := range proofs[:deferredCloseCapacity] {
			require.NoError(t, scheduler.enqueue(proof))
		}
		require.NoError(t, scheduler.enqueue(proofs[0]), "duplicate hints do not consume another slot")
		require.ErrorIs(t, scheduler.enqueue(proofs[deferredCloseCapacity]), errDeferredCloseUnavailable)
		time.Sleep(2 * time.Second)
		synctest.Wait()
		require.EqualValues(t, deferredCloseWorkers, active.Load())
		require.EqualValues(t, deferredCloseWorkers, maximum.Load())
		require.ErrorIs(t, scheduler.enqueue(proofs[deferredCloseCapacity]), errDeferredCloseUnavailable,
			"running attempts retain their queue slots")
		scheduler.stop()
		scheduler.wg.Wait()
		require.Zero(t, active.Load(), "shutdown cancels and joins every admitted attempt")
		require.Empty(t, scheduler.entries)
		require.ErrorIs(t, scheduler.enqueue(proofs[0]), errDeferredCloseUnavailable)
	})
}

func TestDeferredCloseUnknownFailureIsObservableAndNotSuccess(t *testing.T) {
	var calls atomic.Int32
	_, proofs, project := newDeferredCloseFixture(t, 1, func(context.Context, string) error {
		calls.Add(1)
		return errors.New("remote write reply was lost")
	})
	project()
	synctest.Test(t, func(t *testing.T) {
		scheduler := newDeferredCloseScheduler()
		failed := metrics.DeferredClosesTotal.WithLabelValues("failed", string(proofs[0].Reason()))
		dispatched := metrics.DeferredClosesTotal.WithLabelValues("dispatched", string(proofs[0].Reason()))
		beforeFailed, beforeDispatched := promtestutil.ToFloat64(failed), promtestutil.ToFloat64(dispatched)
		require.ErrorIs(t, scheduler.enqueue(proofs[0]), errDeferredCloseUnavailable)
		require.Error(t, scheduler.enqueue(placement.DeferredDeprovision{}))
		scheduler.start(t.Context())
		defer func() { scheduler.stop(); scheduler.wg.Wait() }()
		require.NoError(t, scheduler.enqueue(proofs[0]))
		time.Sleep(2 * time.Second)
		synctest.Wait()
		require.EqualValues(t, 1, calls.Load())
		require.Equal(t, beforeFailed+1, promtestutil.ToFloat64(failed))
		require.Equal(t, beforeDispatched, promtestutil.ToFloat64(dispatched))
		require.Empty(t, scheduler.entries, "unknown errors return to level-triggered recovery")
	})
}

func TestDeferredCloseOverdueReportsOnceWithoutRelinquishingRetryOwnership(t *testing.T) {
	manager, proofs, _ := newDeferredCloseFixture(t, 1, func(context.Context, string) error { return nil })
	scheduler := manager.deferredCloses
	proof := proofs[0]
	before := promtestutil.ToFloat64(metrics.DeferredClosesTotal.WithLabelValues("overdue", string(proof.Reason())))
	logPath := filepath.Join(t.TempDir(), "overdue.log")
	logs, err := os.Create(logPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, logs.Close()) })
	oldLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(oldLogger) })
	require.NoError(t, scheduler.enqueue(proof))
	scheduler.mu.Lock()
	entry := scheduler.entries[proof.LeaseUUID()]
	admitted := time.Now()
	entry.admittedAt = admitted
	scheduler.updateOldestAgeLocked(admitted.Add(deferredCloseOverdueAfter - time.Nanosecond))
	require.Equal(t, before, promtestutil.ToFloat64(metrics.DeferredClosesTotal.WithLabelValues("overdue", string(proof.Reason()))))
	scheduler.updateOldestAgeLocked(admitted.Add(deferredCloseOverdueAfter))
	scheduler.mu.Unlock()
	require.NoError(t, scheduler.enqueue(proof), "coalescing must retain the same overdue reporting owner")
	scheduler.mu.Lock()
	scheduler.updateOldestAgeLocked(admitted.Add(2 * deferredCloseOverdueAfter))
	require.Same(t, entry, scheduler.entries[proof.LeaseUUID()])
	require.True(t, entry.hint.proof.Valid(), "overdue reporting must not revoke the close capability")
	scheduler.mu.Unlock()
	require.Equal(t, before+1, promtestutil.ToFloat64(metrics.DeferredClosesTotal.WithLabelValues("overdue", string(proof.Reason()))))
	logBytes, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.Contains(t, string(logBytes), "level=ERROR")
	require.Contains(t, string(logBytes), "deferred lease close overdue; retry ownership retained")
	require.Contains(t, string(logBytes), proof.LeaseUUID())
}
