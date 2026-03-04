package provisioner

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/chain"
)

// mockBenchBackend implements backend.Backend for benchmarks.
type mockBenchBackend struct {
	name  string
	count *atomic.Int64
}

func (m *mockBenchBackend) Name() string { return m.name }
func (m *mockBenchBackend) Provision(ctx context.Context, req backend.ProvisionRequest) error {
	if m.count != nil {
		m.count.Add(1)
	}
	return nil
}
func (m *mockBenchBackend) GetInfo(ctx context.Context, leaseUUID string) (*backend.LeaseInfo, error) {
	info := backend.LeaseInfo{Host: "10.0.0.1"}
	return &info, nil
}
func (m *mockBenchBackend) Deprovision(ctx context.Context, leaseUUID string) error { return nil }
func (m *mockBenchBackend) ListProvisions(ctx context.Context) ([]backend.ProvisionInfo, error) {
	return nil, nil
}
func (m *mockBenchBackend) Health(ctx context.Context) error       { return nil }
func (m *mockBenchBackend) RefreshState(ctx context.Context) error { return nil }
func (m *mockBenchBackend) GetProvision(ctx context.Context, leaseUUID string) (*backend.ProvisionInfo, error) {
	return nil, backend.ErrNotProvisioned
}
func (m *mockBenchBackend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	return nil, backend.ErrNotProvisioned
}
func (m *mockBenchBackend) Restart(ctx context.Context, req backend.RestartRequest) error { return nil }
func (m *mockBenchBackend) Update(ctx context.Context, req backend.UpdateRequest) error   { return nil }
func (m *mockBenchBackend) GetReleases(ctx context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
	return nil, backend.ErrNotProvisioned
}

// BenchmarkWatermill_Publish benchmarks Watermill message publishing.
func BenchmarkWatermill_Publish(b *testing.B) {
	logger := watermill.NopLogger{}
	pubSub := gochannel.NewGoChannel(gochannel.Config{
		OutputChannelBuffer: 1000,
	}, logger)

	// Create a consumer to drain messages
	messages, err := pubSub.Subscribe(context.Background(), "test-topic")
	if err != nil {
		b.Fatal(err)
	}

	go func() {
		for range messages {
			// Drain messages
		}
	}()

	event := chain.LeaseEvent{
		Type:         chain.LeaseCreated,
		LeaseUUID:    "test-lease",
		ProviderUUID: "provider-uuid",
		Tenant:       "manifest1test",
	}
	payload, _ := json.Marshal(event)

	b.ResetTimer()
	// Use b.Loop() for Go 1.24+ - faster and more accurate benchmarking
	i := 0
	for b.Loop() {
		msg := message.NewMessage(fmt.Sprintf("msg-%d", i), payload)
		if err := pubSub.Publish("test-topic", msg); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

// BenchmarkWatermill_Publish_Parallel benchmarks concurrent publishing.
func BenchmarkWatermill_Publish_Parallel(b *testing.B) {
	logger := watermill.NopLogger{}
	pubSub := gochannel.NewGoChannel(gochannel.Config{
		OutputChannelBuffer: 10000,
	}, logger)

	messages, _ := pubSub.Subscribe(context.Background(), "test-topic")
	go func() {
		for range messages {
		}
	}()

	event := chain.LeaseEvent{
		Type:         chain.LeaseCreated,
		LeaseUUID:    "test-lease",
		ProviderUUID: "provider-uuid",
		Tenant:       "manifest1test",
	}
	payload, _ := json.Marshal(event)

	var counter atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := counter.Add(1)
			msg := message.NewMessage(fmt.Sprintf("msg-%d", id), payload)
			pubSub.Publish("test-topic", msg)
		}
	})
}

// BenchmarkWatermill_PublishSubscribe benchmarks full pub/sub cycle.
func BenchmarkWatermill_PublishSubscribe(b *testing.B) {
	logger := watermill.NopLogger{}
	pubSub := gochannel.NewGoChannel(gochannel.Config{
		OutputChannelBuffer: 1000,
		Persistent:          true,
	}, logger)

	messages, _ := pubSub.Subscribe(context.Background(), "test-topic")

	event := chain.LeaseEvent{
		Type:      chain.LeaseCreated,
		LeaseUUID: "test-lease",
	}
	payload, _ := json.Marshal(event)

	// Drain and ack messages concurrently to prevent publish from blocking
	// when OutputChannelBuffer fills.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for msg := range messages {
			msg.Ack()
		}
	}()

	b.ResetTimer()
	// Use b.Loop() for Go 1.24+ - faster and more accurate benchmarking
	i := 0
	for b.Loop() {
		msg := message.NewMessage(fmt.Sprintf("msg-%d", i), payload)
		pubSub.Publish("test-topic", msg)
		i++
	}
	// Close the pubsub so the drainer goroutine exits, then wait for it.
	// Note: b.Loop() already stopped the timer on its final false return.
	pubSub.Close()
	<-done
}

// TestManager_HighThroughput tests manager under high message throughput.
// Use -short to skip this test.
func TestManager_HighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high throughput test in short mode")
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

	// Start manager in goroutine (Start() blocks)
	go func() {
		mgr.Start(ctx)
	}()

	// Wait for router to be ready
	select {
	case <-mgr.Running():
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for manager to start")
	}

	const (
		numEvents     = 1000
		numGoroutines = 10
	)

	var wg sync.WaitGroup
	start := time.Now()

	for g := range numGoroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			eventsPerGoroutine := numEvents / numGoroutines

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

	// Wait for processing to complete (with timeout)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && provisionCount.Load() < numEvents {
		time.Sleep(50 * time.Millisecond)
	}

	elapsed := time.Since(start)
	processed := provisionCount.Load()

	// Clean shutdown
	cancel()
	mgr.Close()

	t.Logf("High throughput test results:")
	t.Logf("  Events published: %d in %v (%.0f/sec)", numEvents, publishDone, float64(numEvents)/publishDone.Seconds())
	t.Logf("  Events processed: %d in %v (%.0f/sec)", processed, elapsed, float64(processed)/elapsed.Seconds())

	assert.GreaterOrEqual(t, processed, int64(numEvents*90/100), "expected at least 90%% events processed, got %d/%d", processed, numEvents)
}

// TestManager_BurstTraffic tests manager's ability to handle traffic bursts.
// Use -short to skip this test.
func TestManager_BurstTraffic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping burst traffic test in short mode")
	}

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

	// Start manager in goroutine (Start() blocks)
	go func() {
		mgr.Start(ctx)
	}()

	// Wait for router to be ready
	select {
	case <-mgr.Running():
	case <-time.After(5 * time.Second):
		require.Fail(t, "timeout waiting for manager to start")
	}

	const (
		numBursts      = 5
		eventsPerBurst = 200
		burstInterval  = 100 * time.Millisecond
	)

	totalEvents := numBursts * eventsPerBurst
	start := time.Now()

	for burst := range numBursts {
		for i := range eventsPerBurst {
			event := chain.LeaseEvent{
				Type:         chain.LeaseCreated,
				LeaseUUID:    fmt.Sprintf("lease-burst%d-%d", burst, i),
				ProviderUUID: "provider-uuid",
				Tenant:       "manifest1test",
			}
			mgr.PublishLeaseEvent(event)
		}

		if burst < numBursts-1 {
			time.Sleep(burstInterval)
		}
	}

	burstsDone := time.Since(start)

	// Wait for processing
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && provisionCount.Load() < int64(totalEvents) {
		time.Sleep(50 * time.Millisecond)
	}

	elapsed := time.Since(start)
	processed := provisionCount.Load()

	// Clean shutdown
	cancel()
	mgr.Close()

	t.Logf("Burst traffic test results:")
	t.Logf("  Total events: %d sent in %v", totalEvents, burstsDone)
	t.Logf("  Events processed: %d in %v (%.0f/sec)", processed, elapsed, float64(processed)/elapsed.Seconds())

	assert.GreaterOrEqual(t, processed, int64(totalEvents*90/100), "expected at least 90%% events processed, got %d/%d", processed, totalEvents)
}
