package provisioner

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testsupport/placementstore"
)

// mockRejecter implements LeaseRejecter for testing.
type mockRejecter struct {
	rejectFn func(ctx context.Context, uuids []string, reason string) (uint64, []string, error)
}

func (m *mockRejecter) RejectLeases(ctx context.Context, uuids []string, reason string) (uint64, []string, error) {
	return m.rejectFn(ctx, uuids, reason)
}

type timeoutLeaseReader struct {
	mu     sync.RWMutex
	leases map[string]*billingtypes.Lease
}

func (reader *timeoutLeaseReader) GetLease(
	_ context.Context,
	leaseUUID string,
) (*billingtypes.Lease, error) {
	reader.mu.RLock()
	defer reader.mu.RUnlock()
	lease := reader.leases[leaseUUID]
	if lease == nil {
		return nil, nil
	}
	copy := *lease
	copy.Items = append([]billingtypes.LeaseItem(nil), lease.Items...)
	return &copy, nil
}

func (reader *timeoutLeaseReader) put(lease *billingtypes.Lease) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.leases[lease.Uuid] = lease
}

type timeoutBackendRuntime struct {
	mu       sync.RWMutex
	selected string
	backends map[string]backend.Backend
}

func (runtime *timeoutBackendRuntime) selectBackend(name string) {
	runtime.mu.Lock()
	runtime.selected = name
	runtime.mu.Unlock()
}

func (runtime *timeoutBackendRuntime) Route(string) backend.Backend {
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	return runtime.backends[runtime.selected]
}

func (runtime *timeoutBackendRuntime) RouteForProvision(
	context.Context,
	string,
	map[string]int,
) backend.Backend {
	return runtime.Route("")
}

func (runtime *timeoutBackendRuntime) RouteForProvisionAmong(
	ctx context.Context,
	sku string,
	eligible map[string]struct{},
	inFlight map[string]int,
) backend.Backend {
	candidate := runtime.RouteForProvision(ctx, sku, inFlight)
	if candidate == nil {
		return nil
	}
	if _, allowed := eligible[candidate.Name()]; !allowed {
		return nil
	}
	return candidate
}

func (runtime *timeoutBackendRuntime) GetBackendByName(name string) backend.Backend {
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	return runtime.backends[name]
}

func (runtime *timeoutBackendRuntime) HasBackend(name string) bool {
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	_, ok := runtime.backends[name]
	return ok
}

func (runtime *timeoutBackendRuntime) Backends() []backend.Backend {
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	result := make([]backend.Backend, 0, len(runtime.backends))
	for _, client := range runtime.backends {
		result = append(result, client)
	}
	return result
}

type timeoutTestHarness struct {
	coordinator *placement.OperationCoordinator
	execution   *placement.ExecutionCoordinator
	runtime     operation.RuntimeController
	provision   *placement.ProvisionCoordinator
	reader      *timeoutLeaseReader
	chain       *callbackChainStub
	backends    *timeoutBackendRuntime
}

func newTimeoutTestHarness(t *testing.T) *timeoutTestHarness {
	t.Helper()
	clients := map[string]backend.Backend{
		"backend-a":    backend.NewMockBackend(backend.MockBackendConfig{Name: "backend-a"}),
		"test-backend": backend.NewMockBackend(backend.MockBackendConfig{Name: "test-backend"}),
	}
	backends := &timeoutBackendRuntime{selected: "test-backend", backends: clients}
	store := newTestPlacementAuthority(t)
	armTestPlacementTopology(t, store, []string{"backend-a", "test-backend"})
	coordinator, err := store.BindOperationCoordinator(func(count int) {
		metrics.InFlightProvisions.Set(float64(count))
	})
	require.NoError(t, err)
	execution := bindTestBackendRuntime(t, coordinator, backends)
	reader := &timeoutLeaseReader{leases: make(map[string]*billingtypes.Lease)}
	chain := &callbackChainStub{getLease: reader.GetLease}
	bindTestReconciliationCoordinator(t, store, execution, chain, nil, nil)
	provision, err := execution.ProvisionCoordinator(nil)
	require.NoError(t, err)
	return &timeoutTestHarness{
		coordinator: coordinator,
		execution:   execution,
		runtime:     coordinator.RuntimeController(),
		provision:   provision,
		reader:      reader,
		chain:       chain,
		backends:    backends,
	}
}

func (harness *timeoutTestHarness) TrackInFlight(
	t *testing.T,
	leaseUUID, tenant string,
	items []backend.LeaseItem,
	backendName string,
) operation.OperationID {
	t.Helper()
	chainItems := make([]billingtypes.LeaseItem, 0, len(items))
	for _, item := range items {
		chainItems = append(chainItems, billingtypes.LeaseItem{
			SkuUuid: item.SKU, Quantity: uint64(item.Quantity), ServiceName: item.ServiceName,
		})
	}
	harness.reader.put(&billingtypes.Lease{
		Uuid: leaseUUID, Tenant: tenant, ProviderUuid: placementstore.ProviderUUID,
		State: billingtypes.LEASE_STATE_PENDING, Items: chainItems,
	})
	harness.backends.selectBackend(backendName)
	request, err := placement.NewProvisionEventRequest(leaseUUID, tenant)
	require.NoError(t, err)
	result := harness.provision.ExecuteCurrentLease(t.Context(), request)
	require.Equal(t, placement.ProvisionEventStarted, result.Disposition(), result.Err())
	metadata, exists := harness.coordinator.Lookup(leaseUUID)
	require.True(t, exists)
	require.True(t, metadata.ID().Valid())
	return metadata.ID()
}

func (harness *timeoutTestHarness) IsInFlight(leaseUUID string) bool {
	return harness.runtime.Contains(leaseUUID)
}

func newTimeoutCoordinatorForTest(
	t *testing.T,
	harness *timeoutTestHarness,
	rejecter LeaseRejecter,
) *placement.TimeoutCoordinator {
	t.Helper()
	harness.chain.reject = rejecter.RejectLeases
	coordinator, err := harness.execution.TimeoutCoordinator()
	require.NoError(t, err)
	return coordinator
}

func newTimeoutCheckerForTest(
	t *testing.T,
	harness *timeoutTestHarness,
	rejecter LeaseRejecter,
	timeout time.Duration,
) *TimeoutChecker {
	t.Helper()
	checker, err := NewTimeoutChecker(TimeoutCheckerConfig{
		Coordinator:   newTimeoutCoordinatorForTest(t, harness, rejecter),
		Timeout:       timeout,
		CheckInterval: time.Hour, // irrelevant; we call CheckOnce directly
	})
	require.NoError(t, err)
	return checker
}

func TestNewTimeoutChecker_RetainsOnlyTypedCapabilities(t *testing.T) {
	rejecter := &mockRejecter{rejectFn: func(
		context.Context, []string, string,
	) (uint64, []string, error) {
		return 0, nil, nil
	}}
	harness := newTimeoutTestHarness(t)
	coordinator := newTimeoutCoordinatorForTest(t, harness, rejecter)
	checker, err := NewTimeoutChecker(TimeoutCheckerConfig{
		Coordinator:   coordinator,
		Timeout:       time.Minute,
		CheckInterval: time.Minute,
	})
	require.NoError(t, err)
	assert.Same(t, coordinator, checker.coordinator)

	var typedNilOperations *placement.TimeoutCoordinator
	checker, err = NewTimeoutChecker(TimeoutCheckerConfig{
		Coordinator: typedNilOperations,
	})
	assert.Nil(t, checker)
	assert.Error(t, err)

	checker, err = NewTimeoutChecker(TimeoutCheckerConfig{
		Coordinator: coordinator,
		Timeout:     -time.Second, CheckInterval: time.Minute,
	})
	assert.Nil(t, checker)
	assert.Error(t, err)
}

func TestTimeoutChecker_StartStopsWithContext(t *testing.T) {
	rejecter := &mockRejecter{rejectFn: func(
		context.Context, []string, string,
	) (uint64, []string, error) {
		return 0, nil, nil
	}}
	coordinator := newTimeoutCoordinatorForTest(t, newTimeoutTestHarness(t), rejecter)
	checker, err := NewTimeoutChecker(TimeoutCheckerConfig{
		Coordinator:   coordinator,
		Timeout:       time.Minute,
		CheckInterval: time.Millisecond,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		checker.Start(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout checker did not stop after context cancellation")
	}
}

func TestTimeoutOperationLabel(t *testing.T) {
	assert.Equal(t, metrics.OperationProvision, timeoutOperationLabel(operation.KindProvision))
	assert.Equal(t, metrics.OperationRestore, timeoutOperationLabel(operation.KindRestore))
	assert.Equal(t, metrics.OperationProvision, timeoutOperationLabel(operation.KindInvalid),
		"invalid records cannot come from Registry; the fallback remains bounded")
}

func TestCheckOnce_NoTimeouts(t *testing.T) {
	tracker := newTimeoutTestHarness(t)
	// Track a recent provision (not timed out).
	tracker.TrackInFlight(t, "lease-1", "tenant-1", []backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend")

	rejecter := &mockRejecter{
		rejectFn: func(_ context.Context, _ []string, _ string) (uint64, []string, error) {
			t.Fatal("RejectLeases should not be called when nothing is timed out")
			return 0, nil, nil
		},
	}

	checker := newTimeoutCheckerForTest(t, tracker, rejecter, 10*time.Minute)
	checker.CheckOnce(context.Background())

	assert.True(t, tracker.IsInFlight("lease-1"), "lease should still be in-flight")
}

func TestCheckOnce_SingleTimeout_RejectsAndUntracks(t *testing.T) {
	tracker := newTimeoutTestHarness(t)
	// A nanosecond threshold makes the just-started real application operation
	// deterministically eligible without a test-only clock or Registry bypass.
	tracker.TrackInFlight(t, "lease-old", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend")

	var rejectedUUIDs []string
	rejecter := &mockRejecter{
		rejectFn: func(_ context.Context, uuids []string, reason string) (uint64, []string, error) {
			rejectedUUIDs = uuids
			assert.Equal(t, "callback timeout", reason)
			return uint64(len(uuids)), []string{"tx-1"}, nil
		},
	}

	checker := newTimeoutCheckerForTest(t, tracker, rejecter, time.Nanosecond)
	checker.CheckOnce(context.Background())

	require.Len(t, rejectedUUIDs, 1)
	assert.Equal(t, "lease-old", rejectedUUIDs[0])
	assert.False(t, tracker.IsInFlight("lease-old"), "lease should be untracked after rejection")
}

func TestCheckOnce_ConcurrentSweepsRejectGenerationOnce(t *testing.T) {
	tracker := newTimeoutTestHarness(t)
	tracker.TrackInFlight(t, "lease-old", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "backend-a")

	var rejectCalls atomic.Int32
	rejectStarted := make(chan struct{})
	allowReject := make(chan struct{})
	rejecter := &mockRejecter{rejectFn: func(_ context.Context, _ []string, _ string) (uint64, []string, error) {
		if rejectCalls.Add(1) == 1 {
			close(rejectStarted)
		}
		<-allowReject
		return 1, []string{"tx"}, nil
	}}
	checker := newTimeoutCheckerForTest(t, tracker, rejecter, time.Nanosecond)

	firstDone := make(chan struct{})
	go func() {
		checker.CheckOnce(context.Background())
		close(firstDone)
	}()
	<-rejectStarted

	secondDone := make(chan struct{})
	go func() {
		checker.CheckOnce(context.Background())
		close(secondDone)
	}()
	select {
	case <-secondDone:
		// The second sweep saw the claim and skipped this generation.
	case <-time.After(time.Second):
		close(allowReject)
		<-firstDone
		<-secondDone
		t.Fatal("second timeout sweep did not skip the claimed generation")
	}

	close(allowReject)
	<-firstDone
	assert.Equal(t, int32(1), rejectCalls.Load())
	assert.False(t, tracker.IsInFlight("lease-old"))
}

func TestCheckOnce_PanickingRejecterPreservesOneClaimAndHealthyLaneProgresses(t *testing.T) {
	tracker := newTimeoutTestHarness(t)
	for _, leaseUUID := range []string{"lease-panic", "lease-healthy"} {
		tracker.TrackInFlight(t, leaseUUID, "tenant-1",
			[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "backend-a")
	}
	panicLease := true
	rejecter := &mockRejecter{rejectFn: func(
		_ context.Context, leaseUUIDs []string, _ string,
	) (uint64, []string, error) {
		if leaseUUIDs[0] == "lease-panic" && panicLease {
			panic("synthetic chain panic")
		}
		return 1, []string{"tx-healthy"}, nil
	}}
	checker, err := NewTimeoutChecker(TimeoutCheckerConfig{
		Coordinator: newTimeoutCoordinatorForTest(t, tracker, rejecter),
		Timeout:     time.Nanosecond, CheckInterval: time.Hour,
		SettlementBudget: time.Second, Workers: 2,
	})
	require.NoError(t, err)

	assert.NotPanics(t, func() { checker.CheckOnce(t.Context()) })
	assert.True(t, tracker.IsInFlight("lease-panic"),
		"a panic is ambiguous and must retain the exact operation")
	assert.False(t, tracker.IsInFlight("lease-healthy"),
		"an independent timeout lane must still settle")
	panicLease = false
	checker.CheckOnce(t.Context())
	assert.False(t, tracker.IsInFlight("lease-panic"),
		"panic unwinding must release the exact timeout claim for a later application retry")
}

func TestCheckOnce_StalledRejectIsSweepBoundedAndCursorAdvances(t *testing.T) {
	tracker := newTimeoutTestHarness(t)
	for _, leaseUUID := range []string{"lease-a-stalled", "lease-b-healthy"} {
		tracker.TrackInFlight(t, leaseUUID, "tenant-1",
			[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "backend-a")
	}
	stall := true
	rejecter := &mockRejecter{rejectFn: func(
		ctx context.Context, leaseUUIDs []string, _ string,
	) (uint64, []string, error) {
		if leaseUUIDs[0] == "lease-a-stalled" && stall {
			<-ctx.Done()
			return 0, nil, ctx.Err()
		}
		return 1, []string{"tx-healthy"}, nil
	}}
	const budget = 40 * time.Millisecond
	checker, err := NewTimeoutChecker(TimeoutCheckerConfig{
		Coordinator: newTimeoutCoordinatorForTest(t, tracker, rejecter),
		Timeout:     time.Nanosecond, CheckInterval: time.Hour,
		SettlementBudget: budget, Workers: 1,
	})
	require.NoError(t, err)

	started := time.Now()
	checker.CheckOnce(t.Context())
	assert.Less(t, time.Since(started), 5*budget,
		"one stalled chain call must consume one aggregate budget, not an unbounded pass")
	assert.True(t, tracker.IsInFlight("lease-a-stalled"))
	assert.True(t, tracker.IsInFlight("lease-b-healthy"))

	// The first pass advanced past the stalled candidate. The next pass begins
	// at the healthy candidate, settles it, and may then spend one bounded window
	// retrying the still-ambiguous head.
	checker.CheckOnce(t.Context())
	assert.False(t, tracker.IsInFlight("lease-b-healthy"),
		"a stalled candidate must not starve later candidates across cadences")
	assert.True(t, tracker.IsInFlight("lease-a-stalled"))
	stall = false
	checker.CheckOnce(t.Context())
	assert.False(t, tracker.IsInFlight("lease-a-stalled"),
		"deadline unwinding must release the exact timeout claim for a later application retry")
}

func TestCheckOnce_RejectFailure_KeepsInFlight(t *testing.T) {
	tracker := newTimeoutTestHarness(t)
	tracker.TrackInFlight(t, "lease-stuck", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend")

	rejectCalls := 0
	rejecter := &mockRejecter{
		rejectFn: func(_ context.Context, _ []string, _ string) (uint64, []string, error) {
			rejectCalls++
			if rejectCalls == 1 {
				return 0, nil, fmt.Errorf("chain unavailable")
			}
			return 1, []string{"tx"}, nil
		},
	}

	checker := newTimeoutCheckerForTest(t, tracker, rejecter, time.Nanosecond)
	checker.CheckOnce(context.Background())

	assert.True(t, tracker.IsInFlight("lease-stuck"),
		"lease should remain in-flight when rejection fails")

	checker.CheckOnce(context.Background())
	assert.Equal(t, 2, rejectCalls, "a retryable failure must release the claim for the next sweep")
	assert.False(t, tracker.IsInFlight("lease-stuck"))
}

// TestCheckOnce_ActiveReprovisionNotPending_UntracksAndHandsBack covers ENG-337.
// The reconciler registers ACTIVE-lease re-provisions in the SAME shared in-flight
// tracker the checker scans. When such a re-provision's callback is lost, the
// timed-out lease is no longer PENDING, so the chain rejects RejectLeases with
// ErrLeaseNotPending. The checker must NOT keep retrying reject forever (which
// wedges the lease in-flight permanently and inflates InFlightProvisions); it must
// untrack the lease and hand it back to the reconciler, which owns the ACTIVE-lease
// re-provision / FailCount / close path.
func TestCheckOnce_ActiveReprovisionNotPending_UntracksAndHandsBack(t *testing.T) {
	tracker := newTimeoutTestHarness(t)
	tracker.TrackInFlight(t, "lease-active", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend")
	tracker.reader.put(&billingtypes.Lease{
		Uuid: "lease-active", Tenant: "tenant-1", ProviderUuid: placementstore.ProviderUUID,
		State: billingtypes.LEASE_STATE_ACTIVE,
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-1", Quantity: 1}},
	})

	rejectCalls := 0
	rejecter := &mockRejecter{
		rejectFn: func(_ context.Context, _ []string, _ string) (uint64, []string, error) {
			rejectCalls++
			return 0, nil, billingtypes.ErrLeaseNotPending
		},
	}

	checker := newTimeoutCheckerForTest(t, tracker, rejecter, time.Nanosecond)
	checker.CheckOnce(context.Background())

	assert.Equal(t, 1, rejectCalls, "should attempt reject once, not retry a non-pending lease")
	assert.False(t, tracker.IsInFlight("lease-active"),
		"non-pending lease must be untracked and handed back to the reconciler, not kept in-flight")
}

// TestCheckOnce_LeaseNotFoundPreserves proves that a wrong, reset, or lagging
// endpoint cannot erase an unresolved operation. The billing ledger does not
// delete leases, so NotFound is uncertainty rather than terminal evidence.
func TestCheckOnce_LeaseNotFoundPreserves(t *testing.T) {
	tracker := newTimeoutTestHarness(t)
	tracker.TrackInFlight(t, "lease-gone", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend")

	rejectCalls := 0
	rejecter := &mockRejecter{
		rejectFn: func(_ context.Context, _ []string, _ string) (uint64, []string, error) {
			rejectCalls++
			return 0, nil, billingtypes.ErrLeaseNotFound
		},
	}

	checker := newTimeoutCheckerForTest(t, tracker, rejecter, time.Nanosecond)
	checker.CheckOnce(context.Background())

	assert.True(t, tracker.IsInFlight("lease-gone"),
		"NotFound cannot consume operation evidence from an immutable ledger")
	checker.CheckOnce(context.Background())
	assert.Equal(t, 2, rejectCalls, "the exact operation must remain retryable")
}

func TestCheckOnce_ContextCanceled_StopsEarly(t *testing.T) {
	tracker := newTimeoutTestHarness(t)
	// Add two timed-out provisions.
	tracker.TrackInFlight(t, "lease-a", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend")
	tracker.TrackInFlight(t, "lease-b", "tenant-2",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend")

	ctx, cancel := context.WithCancel(context.Background())

	rejectCalls := 0
	rejecter := &mockRejecter{
		rejectFn: func(_ context.Context, uuids []string, _ string) (uint64, []string, error) {
			rejectCalls++
			// Cancel context after first rejection to simulate shutdown.
			cancel()
			return uint64(len(uuids)), []string{"tx-1"}, nil
		},
	}

	checker := newTimeoutCheckerForTest(t, tracker, rejecter, time.Nanosecond)
	// A single lane makes this a test of cancellation at the submission
	// boundary. With multiple lanes, work already admitted before the first
	// callback cancels is intentionally allowed to finish.
	checker.workers = 1
	checker.CheckOnce(ctx)

	// At most one rejection should have been processed before ctx was canceled.
	assert.Equal(t, 1, rejectCalls, "should stop processing after context cancellation")
}

func TestCheckOnce_MultipleTimeouts_PartialFailure(t *testing.T) {
	tracker := newTimeoutTestHarness(t)
	tracker.TrackInFlight(t, "lease-ok", "tenant-1",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend")
	tracker.TrackInFlight(t, "lease-fail", "tenant-2",
		[]backend.LeaseItem{{SKU: "sku-1", Quantity: 1}}, "test-backend")

	rejecter := &mockRejecter{
		rejectFn: func(_ context.Context, uuids []string, _ string) (uint64, []string, error) {
			if uuids[0] == "lease-fail" {
				return 0, nil, fmt.Errorf("chain error")
			}
			return 1, []string{"tx-1"}, nil
		},
	}

	checker := newTimeoutCheckerForTest(t, tracker, rejecter, time.Nanosecond)
	checker.CheckOnce(context.Background())

	// The successfully rejected lease should be untracked.
	// The failed one should remain.
	assert.False(t, tracker.IsInFlight("lease-ok"), "successfully rejected lease should be untracked")
	assert.True(t, tracker.IsInFlight("lease-fail"), "failed rejection should keep lease in-flight")
}
