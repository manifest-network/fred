package provisioner

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
)

// suppressLogs replaces the default logger with a no-op logger for the duration of the test.
// Returns a cleanup function that restores the original logger.
func suppressLogs(t testing.TB) {
	t.Helper()
	original := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() {
		slog.SetDefault(original)
	})
}

// TestManager_StressTest_10K pushes 10,000 events through the system.
func TestManager_StressTest_10K(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	runManagerStressTest(t, 10_000, 50)
}

// TestManager_StressTest_50K pushes 50,000 events through the system.
func TestManager_StressTest_50K(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	runManagerStressTest(t, 50_000, 100)
}

// TestManager_StressTest_100K pushes 100,000 events through the system.
func TestManager_StressTest_100K(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	runManagerStressTest(t, 100_000, 200)
}

// TestManager_StressTest_500K pushes 500,000 events through the system.
// Skipped by default due to high resource requirements.
// Run with: STRESS_TEST_LARGE=1 go test -run TestManager_StressTest_500K
func TestManager_StressTest_500K(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	if os.Getenv("STRESS_TEST_LARGE") == "" {
		t.Skip("skipping large stress test; set STRESS_TEST_LARGE=1 to run")
	}
	runManagerStressTest(t, 500_000, 500)
}

// TestManager_StressTest_1M pushes 1,000,000 events through the system.
// Skipped by default due to high resource requirements.
// Run with: STRESS_TEST_LARGE=1 go test -run TestManager_StressTest_1M
func TestManager_StressTest_1M(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	if os.Getenv("STRESS_TEST_LARGE") == "" {
		t.Skip("skipping large stress test; set STRESS_TEST_LARGE=1 to run")
	}
	runManagerStressTest(t, 1_000_000, 1000)
}

func runManagerStressTest(t *testing.T, numEvents, numGoroutines int) {
	suppressLogs(t)

	var provisionCount atomic.Int64
	var getLeaseCount atomic.Int64

	mockBackend := &mockBenchBackend{name: "test", count: &provisionCount}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			getLeaseCount.Add(1)
			return &billingtypes.Lease{
				Uuid:         uuid,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       "manifest1test",
				ProviderUuid: "provider-uuid",
			}, nil
		},
	}

	mgr, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-uuid",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	// Start manager in goroutine
	go func() {
		mgr.Start(ctx)
	}()

	// Wait for router to be ready
	select {
	case <-mgr.Running():
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for manager to start")
	}

	// Collect memory stats before
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	var wg sync.WaitGroup
	start := time.Now()
	eventsPerGoroutine := numEvents / numGoroutines

	// Publish events from multiple goroutines
	for g := range numGoroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range eventsPerGoroutine {
				event := chain.LeaseEvent{
					Type:         chain.LeaseCreated,
					LeaseUUID:    fmt.Sprintf("lease-%d-%d", gid, i),
					ProviderUUID: "provider-uuid",
					Tenant:       "manifest1test",
				}
				mgr.PublishLeaseEvent(event)
			}
		}(g)
	}

	wg.Wait()
	publishDone := time.Since(start)

	t.Logf("Published %d events in %v (%.0f events/sec)",
		numEvents, publishDone, float64(numEvents)/publishDone.Seconds())

	// Wait for processing with progress updates
	deadline := time.Now().Add(60 * time.Second)
	lastLog := time.Now()
	lastCount := int64(0)

	for time.Now().Before(deadline) && provisionCount.Load() < int64(numEvents) {
		time.Sleep(100 * time.Millisecond)

		// Log progress every 2 seconds
		if time.Since(lastLog) > 2*time.Second {
			current := provisionCount.Load()
			rate := float64(current-lastCount) / time.Since(lastLog).Seconds()
			t.Logf("  Progress: %d/%d (%.0f%%) - %.0f events/sec",
				current, numEvents, float64(current)/float64(numEvents)*100, rate)
			lastLog = time.Now()
			lastCount = current
		}
	}

	elapsed := time.Since(start)
	processed := provisionCount.Load()

	// Collect memory stats after
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Clean shutdown
	cancel()
	mgr.Close()

	// Results
	t.Logf("")
	t.Logf("=== STRESS TEST RESULTS ===")
	t.Logf("Events: %d", numEvents)
	t.Logf("Goroutines: %d", numGoroutines)
	t.Logf("")
	t.Logf("Publishing:")
	t.Logf("  Duration: %v", publishDone)
	t.Logf("  Rate: %.0f events/sec", float64(numEvents)/publishDone.Seconds())
	t.Logf("")
	t.Logf("Processing:")
	t.Logf("  Processed: %d/%d (%.1f%%)", processed, numEvents, float64(processed)/float64(numEvents)*100)
	t.Logf("  GetLease calls: %d", getLeaseCount.Load())
	t.Logf("  Total time: %v", elapsed)
	t.Logf("  Throughput: %.0f events/sec", float64(processed)/elapsed.Seconds())
	t.Logf("")
	t.Logf("Memory:")
	t.Logf("  Alloc before: %d MB", memBefore.Alloc/1024/1024)
	t.Logf("  Alloc after: %d MB", memAfter.Alloc/1024/1024)
	t.Logf("  Total alloc: %d MB", memAfter.TotalAlloc/1024/1024)
	t.Logf("  Num GC: %d", memAfter.NumGC-memBefore.NumGC)

	// Verify we processed most events
	successRate := float64(processed) / float64(numEvents) * 100
	assert.GreaterOrEqual(t, successRate, float64(95), "Expected at least 95%% success rate, got %.1f%%", successRate)
}

// TestManager_SustainedLoad tests sustained load over time.
func TestManager_SustainedLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping sustained load test in short mode")
	}
	suppressLogs(t)

	var provisionCount atomic.Int64
	mockBackend := &mockBenchBackend{name: "test", count: &provisionCount}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         uuid,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       "manifest1test",
				ProviderUuid: "provider-uuid",
			}, nil
		},
	}

	mgr, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-uuid",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		mgr.Start(ctx)
	}()

	select {
	case <-mgr.Running():
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for manager to start")
	}

	// Sustained load: 1000 events/sec for 10 seconds
	const (
		targetRPS  = 1000
		duration   = 10 * time.Second
		numWorkers = 10
	)

	eventsPerWorkerPerSec := targetRPS / numWorkers
	intervalPerEvent := time.Second / time.Duration(eventsPerWorkerPerSec)

	var wg sync.WaitGroup
	var totalSent atomic.Int64
	start := time.Now()

	// Start workers
	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ticker := time.NewTicker(intervalPerEvent)
			defer ticker.Stop()

			eventID := 0
			for {
				select {
				case <-ticker.C:
					if time.Since(start) >= duration {
						return
					}
					event := chain.LeaseEvent{
						Type:         chain.LeaseCreated,
						LeaseUUID:    fmt.Sprintf("sustained-%d-%d", workerID, eventID),
						ProviderUUID: "provider-uuid",
						Tenant:       "manifest1test",
					}
					mgr.PublishLeaseEvent(event)
					totalSent.Add(1)
					eventID++
				}
			}
		}(w)
	}

	// Monitor progress
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		lastCount := int64(0)
		lastTime := start

		for range ticker.C {
			if time.Since(start) >= duration+5*time.Second {
				return
			}
			current := provisionCount.Load()
			sent := totalSent.Load()
			elapsed := time.Since(lastTime)
			rate := float64(current-lastCount) / elapsed.Seconds()
			t.Logf("  Sent: %d, Processed: %d, Rate: %.0f/sec", sent, current, rate)
			lastCount = current
			lastTime = time.Now()
		}
	}()

	wg.Wait()
	sendDone := time.Since(start)
	sent := totalSent.Load()

	// Wait for processing to catch up
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && provisionCount.Load() < sent {
		time.Sleep(100 * time.Millisecond)
	}

	elapsed := time.Since(start)
	processed := provisionCount.Load()

	cancel()
	mgr.Close()

	actualRPS := float64(sent) / sendDone.Seconds()
	processedRPS := float64(processed) / elapsed.Seconds()

	t.Logf("")
	t.Logf("=== SUSTAINED LOAD RESULTS ===")
	t.Logf("Target: %d events/sec for %v", targetRPS, duration)
	t.Logf("Actual send rate: %.0f events/sec", actualRPS)
	t.Logf("Total sent: %d", sent)
	t.Logf("Total processed: %d (%.1f%%)", processed, float64(processed)/float64(sent)*100)
	t.Logf("Processing rate: %.0f events/sec", processedRPS)

	assert.GreaterOrEqual(t, processed, int64(float64(sent)*0.95), "Expected at least 95%% processed, got %.1f%%", float64(processed)/float64(sent)*100)
}

// TestManager_WithBackendLatency tests event processing with realistic backend latency.
func TestManager_WithBackendLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping latency test in short mode")
	}

	var provisionCount atomic.Int64
	latencies := []time.Duration{1 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond}

	for _, latency := range latencies {
		t.Run(fmt.Sprintf("latency=%v", latency), func(t *testing.T) {
			provisionCount.Store(0)

			mockBackend := &mockLatencyBackend{
				name:    "test",
				count:   &provisionCount,
				latency: latency,
			}

			router, _ := backend.NewRouter(backend.RouterConfig{
				Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
			})

			mockChain := &chain.MockClient{
				GetLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
					return &billingtypes.Lease{
						Uuid:         uuid,
						State:        billingtypes.LEASE_STATE_PENDING,
						Tenant:       "manifest1test",
						ProviderUuid: "provider-uuid",
					}, nil
				},
			}

			mgr, err := NewManager(ManagerConfig{
				ProviderUUID:    "provider-uuid",
				CallbackBaseURL: "http://localhost:8080",
			}, router, mockChain)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())

			go func() {
				mgr.Start(ctx)
			}()

			select {
			case <-mgr.Running():
			case <-time.After(5 * time.Second):
				require.Fail(t, "timeout waiting for manager to start")
			}

			const numEvents = 1000
			start := time.Now()

			for i := range numEvents {
				event := chain.LeaseEvent{
					Type:         chain.LeaseCreated,
					LeaseUUID:    fmt.Sprintf("latency-lease-%d", i),
					ProviderUUID: "provider-uuid",
					Tenant:       "manifest1test",
				}
				mgr.PublishLeaseEvent(event)
			}

			publishDone := time.Since(start)

			// Wait for processing with extended timeout for latency
			timeout := time.Duration(numEvents) * latency * 2
			if timeout < 30*time.Second {
				timeout = 30 * time.Second
			}
			deadline := time.Now().Add(timeout)

			for time.Now().Before(deadline) && provisionCount.Load() < int64(numEvents) {
				time.Sleep(100 * time.Millisecond)
			}

			elapsed := time.Since(start)
			processed := provisionCount.Load()

			cancel()
			mgr.Close()

			t.Logf("Backend latency: %v", latency)
			t.Logf("  Published: %d in %v", numEvents, publishDone)
			t.Logf("  Processed: %d/%d in %v (%.0f events/sec)",
				processed, numEvents, elapsed, float64(processed)/elapsed.Seconds())

			assert.GreaterOrEqual(t, processed, int64(numEvents*90/100), "Expected at least 90%% processed, got %d/%d", processed, numEvents)
		})
	}
}

// mockLatencyBackend adds artificial latency to simulate real backends.
type mockLatencyBackend struct {
	name    string
	count   *atomic.Int64
	latency time.Duration
}

func (m *mockLatencyBackend) Name() string { return m.name }
func (m *mockLatencyBackend) Provision(ctx context.Context, req backend.ProvisionRequest) error {
	time.Sleep(m.latency)
	if m.count != nil {
		m.count.Add(1)
	}
	return nil
}
func (m *mockLatencyBackend) GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error) {
	time.Sleep(m.latency)
	info := backend.LeaseInfo{Host: "10.0.0.1"}
	return &info, nil
}
func (m *mockLatencyBackend) Deprovision(ctx context.Context, leaseUUID string) error {
	time.Sleep(m.latency)
	return nil
}
func (m *mockLatencyBackend) ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error) {
	return nil, nil
}
func (m *mockLatencyBackend) LookupProvisions(ctx context.Context, uuids []string) ([]backend.ProvisionInfo, error) {
	return nil, nil
}
func (m *mockLatencyBackend) Health(ctx context.Context) error       { return nil }
func (m *mockLatencyBackend) RefreshState(ctx context.Context) error { return nil }
func (m *mockLatencyBackend) GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
	return nil, backend.ErrNotProvisioned
}
func (m *mockLatencyBackend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	return nil, backend.ErrNotProvisioned
}
func (m *mockLatencyBackend) Restart(ctx context.Context, req backend.RestartRequest) error {
	return nil
}
func (m *mockLatencyBackend) Update(ctx context.Context, req backend.UpdateRequest) error {
	return nil
}
func (m *mockLatencyBackend) GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
	return nil, backend.ErrNotProvisioned
}

// TestManager_HighConcurrencySustained tests sustained high concurrency.
func TestManager_HighConcurrencySustained(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high concurrency test in short mode")
	}
	suppressLogs(t)

	var provisionCount atomic.Int64
	mockBackend := &mockBenchBackend{name: "test", count: &provisionCount}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         uuid,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       "manifest1test",
				ProviderUuid: "provider-uuid",
			}, nil
		},
	}

	mgr, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-uuid",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		mgr.Start(ctx)
	}()

	select {
	case <-mgr.Running():
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for manager to start")
	}

	// Sustained high concurrency: 5000 events/sec for 30 seconds
	const (
		targetRPS  = 5000
		duration   = 30 * time.Second
		numWorkers = 50
	)

	eventsPerWorkerPerSec := targetRPS / numWorkers
	intervalPerEvent := time.Second / time.Duration(eventsPerWorkerPerSec)

	var wg sync.WaitGroup
	var totalSent atomic.Int64
	start := time.Now()

	// Collect memory stats before
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ticker := time.NewTicker(intervalPerEvent)
			defer ticker.Stop()

			eventID := 0
			for {
				select {
				case <-ticker.C:
					if time.Since(start) >= duration {
						return
					}
					event := chain.LeaseEvent{
						Type:         chain.LeaseCreated,
						LeaseUUID:    fmt.Sprintf("highconc-%d-%d", workerID, eventID),
						ProviderUUID: "provider-uuid",
						Tenant:       "manifest1test",
					}
					mgr.PublishLeaseEvent(event)
					totalSent.Add(1)
					eventID++
				}
			}
		}(w)
	}

	// Monitor progress
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		lastCount := int64(0)
		lastTime := start

		for range ticker.C {
			if time.Since(start) >= duration+10*time.Second {
				return
			}
			current := provisionCount.Load()
			sent := totalSent.Load()
			elapsed := time.Since(lastTime)
			rate := float64(current-lastCount) / elapsed.Seconds()
			t.Logf("  Sent: %d, Processed: %d, Rate: %.0f/sec, Backlog: %d",
				sent, current, rate, sent-current)
			lastCount = current
			lastTime = time.Now()
		}
	}()

	wg.Wait()
	sendDone := time.Since(start)
	sent := totalSent.Load()

	// Wait for processing to catch up
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && provisionCount.Load() < sent {
		time.Sleep(100 * time.Millisecond)
	}

	elapsed := time.Since(start)
	processed := provisionCount.Load()

	// Collect memory stats after
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	cancel()
	mgr.Close()

	actualRPS := float64(sent) / sendDone.Seconds()
	processedRPS := float64(processed) / elapsed.Seconds()

	t.Logf("")
	t.Logf("=== HIGH CONCURRENCY SUSTAINED RESULTS ===")
	t.Logf("Target: %d events/sec for %v", targetRPS, duration)
	t.Logf("Actual send rate: %.0f events/sec", actualRPS)
	t.Logf("Total sent: %d", sent)
	t.Logf("Total processed: %d (%.1f%%)", processed, float64(processed)/float64(sent)*100)
	t.Logf("Processing rate: %.0f events/sec", processedRPS)
	t.Logf("")
	t.Logf("Memory:")
	t.Logf("  Alloc before: %d MB", memBefore.Alloc/1024/1024)
	t.Logf("  Alloc after: %d MB", memAfter.Alloc/1024/1024)
	t.Logf("  Total alloc: %d MB", memAfter.TotalAlloc/1024/1024)
	t.Logf("  Num GC: %d", memAfter.NumGC-memBefore.NumGC)

	assert.GreaterOrEqual(t, processed, int64(float64(sent)*0.95), "Expected at least 95%% processed, got %.1f%%", float64(processed)/float64(sent)*100)
}

// BenchmarkManager_EndToEnd benchmarks the full event processing flow.
func BenchmarkManager_EndToEnd(b *testing.B) {
	suppressLogs(b)

	var provisionCount atomic.Int64
	mockBackend := &mockBenchBackend{name: "test", count: &provisionCount}

	router, _ := backend.NewRouter(backend.RouterConfig{
		Backends: []backend.BackendEntry{{Backend: mockBackend, IsDefault: true}},
	})

	mockChain := &chain.MockClient{
		GetLeaseFunc: func(ctx context.Context, uuid string) (*billingtypes.Lease, error) {
			return &billingtypes.Lease{
				Uuid:         uuid,
				State:        billingtypes.LEASE_STATE_PENDING,
				Tenant:       "manifest1test",
				ProviderUuid: "provider-uuid",
			}, nil
		},
	}

	mgr, err := NewManager(ManagerConfig{
		ProviderUUID:    "provider-uuid",
		CallbackBaseURL: "http://localhost:8080",
	}, router, mockChain)
	if err != nil {
		b.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		mgr.Start(ctx)
	}()

	select {
	case <-mgr.Running():
	case <-time.After(5 * time.Second):
		b.Fatal("timeout waiting for manager to start")
	}

	b.ResetTimer()
	// Use b.Loop() for Go 1.24+ - faster and more accurate benchmarking
	i := 0
	for b.Loop() {
		event := chain.LeaseEvent{
			Type:         chain.LeaseCreated,
			LeaseUUID:    fmt.Sprintf("bench-lease-%d", i),
			ProviderUUID: "provider-uuid",
			Tenant:       "manifest1test",
		}
		mgr.PublishLeaseEvent(event)
		i++
	}

	// Wait for all to be processed
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && provisionCount.Load() < int64(b.N) {
		time.Sleep(10 * time.Millisecond)
	}

	b.StopTimer()
	mgr.Close()

	b.ReportMetric(float64(provisionCount.Load()), "processed")
}
