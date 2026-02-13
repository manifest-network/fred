package backend

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockBenchBackend implements Backend for benchmarks.
type mockBenchBackend struct {
	name string
}

func (m *mockBenchBackend) Name() string                                              { return m.name }
func (m *mockBenchBackend) Provision(ctx context.Context, req ProvisionRequest) error { return nil }
func (m *mockBenchBackend) GetInfo(ctx context.Context, leaseUUID string) (*LeaseInfo, error) {
	info := LeaseInfo{"host": "10.0.0.1", "port": 8080}
	return &info, nil
}
func (m *mockBenchBackend) Deprovision(ctx context.Context, leaseUUID string) error { return nil }
func (m *mockBenchBackend) ListProvisions(ctx context.Context) ([]ProvisionInfo, error) {
	return nil, nil
}
func (m *mockBenchBackend) Health(ctx context.Context) error       { return nil }
func (m *mockBenchBackend) RefreshState(ctx context.Context) error { return nil }
func (m *mockBenchBackend) GetProvision(ctx context.Context, leaseUUID string) (*ProvisionInfo, error) {
	return nil, ErrNotProvisioned
}
func (m *mockBenchBackend) GetLogs(ctx context.Context, leaseUUID string, tail int) (map[string]string, error) {
	return nil, ErrNotProvisioned
}
func (m *mockBenchBackend) Restart(ctx context.Context, req RestartRequest) error { return nil }
func (m *mockBenchBackend) Update(ctx context.Context, req UpdateRequest) error   { return nil }
func (m *mockBenchBackend) GetReleases(ctx context.Context, leaseUUID string) ([]ReleaseInfo, error) {
	return nil, ErrNotProvisioned
}

// BenchmarkRouter_Route benchmarks SKU-based routing decisions.
func BenchmarkRouter_Route(b *testing.B) {
	// Create router with multiple backends
	backends := []BackendEntry{
		{Backend: &mockBenchBackend{name: "kubernetes"}, Match: MatchCriteria{SKUPrefix: "k8s-"}},
		{Backend: &mockBenchBackend{name: "gpu"}, Match: MatchCriteria{SKUPrefix: "gpu-"}},
		{Backend: &mockBenchBackend{name: "vm"}, Match: MatchCriteria{SKUPrefix: "vm-"}},
		{Backend: &mockBenchBackend{name: "storage"}, Match: MatchCriteria{SKUPrefix: "storage-"}},
		{Backend: &mockBenchBackend{name: "default"}, IsDefault: true},
	}

	router, err := NewRouter(RouterConfig{Backends: backends})
	if err != nil {
		b.Fatal(err)
	}

	// SKUs to test routing
	skus := []string{
		"k8s-small",
		"k8s-large",
		"gpu-a100",
		"gpu-h100",
		"vm-basic",
		"vm-premium",
		"storage-ssd",
		"unknown-sku", // Falls back to default
	}

	b.ResetTimer()
	// Use b.Loop() for Go 1.24+ - faster and more accurate benchmarking
	i := 0
	for b.Loop() {
		sku := skus[i%len(skus)]
		backend := router.Route(sku)
		if backend == nil {
			b.Fatal("expected backend, got nil")
		}
		i++
	}
}

// BenchmarkRouter_Route_Parallel benchmarks concurrent routing.
func BenchmarkRouter_Route_Parallel(b *testing.B) {
	backends := []BackendEntry{
		{Backend: &mockBenchBackend{name: "gpu"}, Match: MatchCriteria{SKUPrefix: "gpu-"}},
		{Backend: &mockBenchBackend{name: "k8s"}, Match: MatchCriteria{SKUPrefix: "k8s-"}},
		{Backend: &mockBenchBackend{name: "default"}, IsDefault: true},
	}

	router, err := NewRouter(RouterConfig{Backends: backends})
	if err != nil {
		b.Fatal(err)
	}

	skus := []string{"gpu-a100", "k8s-small", "unknown"}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			router.Route(skus[i%len(skus)])
		}
	})
}

// BenchmarkRouter_GetBackendByName benchmarks direct backend lookup.
func BenchmarkRouter_GetBackendByName(b *testing.B) {
	names := []string{"gpu", "k8s", "vm", "storage", "default"}
	backends := make([]BackendEntry, len(names))
	for i, name := range names {
		backends[i] = BackendEntry{
			Backend:   &mockBenchBackend{name: name},
			IsDefault: name == "default",
		}
		if name != "default" {
			backends[i].Match = MatchCriteria{SKUPrefix: name + "-"}
		}
	}

	router, err := NewRouter(RouterConfig{Backends: backends})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	// Use b.Loop() for Go 1.24+ - faster and more accurate benchmarking
	i := 0
	for b.Loop() {
		name := names[i%len(names)]
		backend := router.GetBackendByName(name)
		if backend == nil {
			b.Fatal("expected backend")
		}
		i++
	}
}

// BenchmarkHTTPClient_Provision benchmarks the provision HTTP call overhead.
func BenchmarkHTTPClient_Provision(b *testing.B) {
	var requestCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"provision_id":"test-123"}`))
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test",
		BaseURL: server.URL,
		Timeout: 10 * time.Second,
	})

	ctx := context.Background()

	b.ResetTimer()
	// Use b.Loop() for Go 1.24+ - faster and more accurate benchmarking
	for b.Loop() {
		req := ProvisionRequest{
			LeaseUUID:    "test-lease",
			Tenant:       "manifest1test",
			ProviderUUID: "provider-uuid",
			Items:        []LeaseItem{{SKU: "test-sku", Quantity: 1}},
			CallbackURL:  "http://localhost/callback",
		}
		err := client.Provision(ctx, req)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(requestCount.Load()), "requests")
}

// BenchmarkHTTPClient_Provision_Parallel benchmarks concurrent provision calls.
func BenchmarkHTTPClient_Provision_Parallel(b *testing.B) {
	var requestCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"provision_id":"test-123"}`))
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test",
		BaseURL: server.URL,
		Timeout: 10 * time.Second,
	})

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := ProvisionRequest{
				LeaseUUID:    "test-lease",
				Tenant:       "manifest1test",
				ProviderUUID: "provider-uuid",
				Items:        []LeaseItem{{SKU: "test-sku", Quantity: 1}},
				CallbackURL:  "http://localhost/callback",
			}
			err := client.Provision(ctx, req)
			if err != nil {
				b.Error(err)
			}
		}
	})
}

// BenchmarkHTTPClient_GetInfo benchmarks connection info retrieval.
func BenchmarkHTTPClient_GetInfo(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"lease_uuid": "test",
			"status": "ready",
			"connection": {
				"host": "10.0.0.1",
				"port": 8080,
				"protocol": "https"
			}
		}`))
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test",
		BaseURL: server.URL,
		Timeout: 10 * time.Second,
	})

	ctx := context.Background()

	b.ResetTimer()
	// Use b.Loop() for Go 1.24+ - faster and more accurate benchmarking
	for b.Loop() {
		_, err := client.GetInfo(ctx, "test-lease")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHTTPClient_GetInfo_Parallel benchmarks concurrent info retrieval.
func BenchmarkHTTPClient_GetInfo_Parallel(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{
			"lease_uuid": "test",
			"status": "ready",
			"connection": {
				"host": "10.0.0.1",
				"port": 8080,
				"protocol": "https"
			}
		}`))
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:    "test",
		BaseURL: server.URL,
		Timeout: 10 * time.Second,
	})

	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := client.GetInfo(ctx, "test-lease")
			if err != nil {
				b.Error(err)
			}
		}
	})
}

// TestHTTPClient_HighThroughput tests client under high concurrent load.
func TestHTTPClient_HighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high throughput test in short mode")
	}

	var requestCount atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"provision_id":"test"}`))
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Name:                "test",
		BaseURL:             server.URL,
		Timeout:             10 * time.Second,
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
	})

	const (
		numGoroutines  = 100
		reqsPerRoutine = 100
	)

	var wg sync.WaitGroup
	var errors atomic.Int64
	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()

			for i := 0; i < reqsPerRoutine; i++ {
				req := ProvisionRequest{
					LeaseUUID:    "test-lease",
					Tenant:       "manifest1test",
					ProviderUUID: "provider-uuid",
					Items:        []LeaseItem{{SKU: "test-sku", Quantity: 1}},
					CallbackURL:  "http://localhost/callback",
				}
				if err := client.Provision(ctx, req); err != nil {
					errors.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := requestCount.Load()
	rps := float64(total) / elapsed.Seconds()

	t.Logf("High throughput test results:")
	t.Logf("  Total requests: %d in %v", total, elapsed)
	t.Logf("  Requests/sec: %.0f", rps)
	t.Logf("  Errors: %d", errors.Load())

	if errors.Load() > 0 {
		t.Errorf("unexpected errors: %d", errors.Load())
	}
}
