# Fred Performance Benchmarks

This document presents performance benchmarks and stress test results for Fred's event processing pipeline.

## Test Environment

- **Platform**: Linux 6.18.4-1-MANJARO
- **Go Version**: 1.24+
- **Event Library**: Watermill with GoChannel (in-memory pub/sub)
- **Backend**: Mock backend (instant response, no I/O)

All tests use mock backends to isolate Fred's event processing performance from external factors like network latency and backend processing time.

## Stress Test Results

### Burst Traffic Tests

Tests publish all events as fast as possible, then measure processing time.

| Test | Events | Goroutines | Publishing Rate | Processing Rate | Success | Total Time |
|------|--------|------------|-----------------|-----------------|---------|------------|
| 10K | 10,000 | 50 | 147,921/sec | 100% | 100% | < 1s |
| 50K | 50,000 | 100 | 148,318/sec | 100% | 100% | ~1s |
| 100K | 100,000 | 200 | 143,552/sec | 58,890/sec | 100% | ~2s |
| 500K | 500,000 | 500 | ~150,000/sec | ~95,000/sec | 100% | 8.7s |
| 1M | 1,000,000 | 1,000 | 147,166/sec | ~90,000/sec | 100% | 17.7s |

**Key Findings:**
- Publishing rate consistently hits **~147,000 events/sec**
- End-to-end processing sustains **90,000+ events/sec**
- 100% success rate across all tests
- No event loss under burst conditions

### Memory Usage (1M Event Test)

| Metric | Value |
|--------|-------|
| Heap before | 4 MB |
| Heap after | 1,259 MB |
| Total allocated | 5,899 MB |
| GC cycles | 14 |

Memory usage scales linearly with event volume. The Go garbage collector keeps heap size reasonable.

## Sustained Load Tests

Tests maintain a constant event rate over time to verify steady-state behavior.

### Standard Sustained Load (1000 events/sec for 10s)

| Metric | Value |
|--------|-------|
| Target rate | 1,000 events/sec |
| Duration | 10 seconds |
| Total sent | 9,990 |
| Total processed | 9,990 (100%) |
| Actual rate | 999 events/sec |

### High Concurrency Sustained Load (5000 events/sec for 30s)

| Metric | Value |
|--------|-------|
| Target rate | 5,000 events/sec |
| Duration | 30 seconds |
| Workers | 50 |
| Total sent | 149,950 |
| Total processed | 149,950 (100%) |
| Max backlog | 35 events |
| Memory after | 54 MB |
| GC cycles | 50 |

**Key Findings:**
- System maintains target throughput with near-zero backlog
- Memory remains stable at 54 MB for sustained load
- No degradation over 30-second test window

## Backend Latency Tests

Tests simulate realistic backend response times to verify concurrent processing.

| Backend Latency | Events | Processing Rate | Success |
|-----------------|--------|-----------------|---------|
| 1ms | 1,000 | 9,403/sec | 100% |
| 5ms | 1,000 | 9,412/sec | 100% |
| 10ms | 1,000 | 9,613/sec | 100% |

**Key Findings:**
- Processing rate remains consistent regardless of backend latency
- Handlers process events concurrently (not blocked by backend calls)
- System handles 10ms latency without throughput degradation

## Comparison with Other Solutions

### Fred vs Kubernetes Operators

| Metric | Fred | K8s Operators (Kubeflow) |
|--------|------|--------------------------|
| Event processing | 90,000+/sec | ~17/sec (1000/min) |
| Default queue rate | N/A (async) | 50 items/sec |
| Controller workers | Concurrent handlers | 10-30 |
| API latency under load | < 1ms | 600ms+ |

Fred processes events **~5,000x faster** than typical Kubernetes operators. Note that K8s operators perform real API calls, while these benchmarks use mock backends.

### Fred vs Watermill Claims

Watermill claims "hundreds of thousands of messages per second" for in-memory GoChannel transport. Fred achieves **147,000 events/sec publishing**, confirming we're utilizing Watermill effectively.

### Production Considerations

In production, the limiting factors will be:

1. **Chain block times** (~6 seconds on Cosmos) - limits event arrival rate
2. **Backend provisioning time** (seconds to minutes) - real work takes time
3. **Network latency** - round-trip to backends and chain

Fred's 90,000+ events/sec capacity far exceeds realistic chain event rates, ensuring **Fred will never be the bottleneck**.

## Running Benchmarks

### Quick Benchmarks

```bash
# Run all benchmarks
go test -bench=. ./internal/provisioner/ -benchtime=1s

# Run specific benchmark
go test -bench=BenchmarkManager_EndToEnd ./internal/provisioner/
```

### Stress Tests

```bash
# Run all stress tests (skipped in short mode)
go test -v -run "StressTest|SustainedLoad|HighConcurrency|Latency" ./internal/provisioner/ -timeout 10m

# Run specific stress test
go test -v -run TestManager_StressTest_100K ./internal/provisioner/ -timeout 5m

# Run 1M event test
go test -v -run TestManager_StressTest_1M ./internal/provisioner/ -timeout 15m
```

### Watermill Benchmarks

```bash
# Benchmark Watermill pub/sub directly
go test -bench=BenchmarkWatermill ./internal/provisioner/
```

### PayloadStore Benchmarks

```bash
# Benchmark payload storage (bbolt)
go test -bench=. ./internal/provisioner/ -run=^$ -bench=PayloadStore
```

## Benchmark Files

| File | Description |
|------|-------------|
| `internal/provisioner/manager_bench_test.go` | Watermill and Manager benchmarks |
| `internal/provisioner/manager_stress_test.go` | Stress tests (10K-1M events) |
| `internal/provisioner/payload_bench_test.go` | PayloadStore benchmarks |
| `internal/api/token_tracker_bench_test.go` | Token tracker benchmarks |
| `internal/backend/router_bench_test.go` | Backend router benchmarks |

## Tuning Recommendations

Based on benchmark results:

1. **Watermill GoChannel buffer**: Default 256 is sufficient for most workloads
2. **Handler concurrency**: Watermill handles this automatically with goroutines
3. **Batch acknowledgments**: Ack batcher (50 items / 500ms) reduces chain transactions
4. **PayloadStore batch size**: 100-200 provides good write throughput
5. **Memory**: Allocate 2-4 GB for sustained high-throughput workloads

## Methodology Notes

- All tests run with `testing.Short()` skip for CI compatibility
- Stress tests use atomic counters for thread-safe progress tracking
- Memory stats collected via `runtime.ReadMemStats()`
- Progress logging every 2-5 seconds for long-running tests
- 95% success rate threshold for pass/fail determination
- Tests include proper cleanup with `context.WithCancel()` and `mgr.Close()`
