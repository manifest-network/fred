// Package main provides a load testing tool for fred.
//
// Usage:
//
//	loadtest -fixtures fixtures.json -tenant-key-file tenant.hex -scenario connection
//
// Scenarios:
//   - payload: Upload supplied payloads for existing leases
//   - connection: Retrieve connection information for supplied leases
//   - callback: Replay exact recorded callback requests
//   - mixed: Mix payload and connection fixtures, optionally recorded callbacks
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	target := flag.String("target", "http://localhost:8080", "Target Fred origin")
	duration := flag.Duration("duration", 30*time.Second, "Test duration")
	concurrency := flag.Int("concurrency", 50, "Number of concurrent workers")
	scenario := flag.String("scenario", "mixed", "Scenario: payload, connection, callback, mixed")
	traffic := flag.String("traffic", "authenticated", "Traffic: authenticated (requires fixtures) or rejection")
	fixtures := flag.String("fixtures", "", "JSON file containing existing leases/payloads and exact recorded callback requests")
	keyFile := flag.String("tenant-key-file", "", "File containing the tenant secp256k1 private key as 64 hex digits; never printed")
	prefix := flag.String("bech32-prefix", "manifest", "Tenant address prefix")
	payloadSize := flag.Int("payload-size", 1024, "Random payload size for rejection traffic only")
	rampUp := flag.Duration("ramp-up", 5*time.Second, "Ramp-up time to reach full concurrency")
	callbackSecret := flag.String("callback-secret", "", "HMAC key for the recorded backend callback fixtures (min 32 bytes)")
	verbose := flag.Bool("verbose", false, "Verbose output")
	flag.Parse()
	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	if *duration <= 0 || *concurrency <= 0 || *concurrency > 10000 || *rampUp < 0 {
		slog.Error("duration and concurrency must be positive, concurrency at most 10000, and ramp-up nonnegative")
		os.Exit(1)
	}
	work, err := loadWorkload(workloadConfig{
		target: *target, scenario: *scenario, traffic: *traffic, fixtures: *fixtures,
		keyFile: *keyFile, prefix: *prefix, callbackSecret: *callbackSecret, payloadSize: *payloadSize,
	})
	if err != nil {
		slog.Error("invalid load-test configuration", "error", err)
		os.Exit(1)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = *concurrency * 2
	transport.MaxIdleConnsPerHost = *concurrency * 2
	client := &http.Client{Timeout: 30 * time.Second, Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}
	defer client.CloseIdleConnections()
	slog.Info("starting load test", "target", *target, "traffic", *traffic, "scenario", *scenario,
		"duration", *duration, "concurrency", *concurrency)
	lt := &LoadTester{duration: *duration, concurrency: *concurrency, rampUp: *rampUp, client: client, work: work}
	lt.Run().Print()
}

// LoadTester executes a constructed workload with bounded worker concurrency.
type LoadTester struct {
	duration    time.Duration
	concurrency int
	rampUp      time.Duration
	client      *http.Client
	work        workload
}

// Results holds load test results.
type Results struct {
	Duration      time.Duration
	TotalRequests int64
	SuccessCount  int64
	ErrorCount    int64
	StatusCodes   map[int]int64
	Latencies     []time.Duration
	Errors        map[string]int64
	BytesSent     int64
	BytesReceived int64

	mu sync.Mutex
}

func NewResults() *Results {
	return &Results{
		StatusCodes: make(map[int]int64),
		Errors:      make(map[string]int64),
		Latencies:   make([]time.Duration, 0, 10000),
	}
}

func (r *Results) Record(latency time.Duration, statusCode int, err error, bytesSent, bytesReceived int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.TotalRequests++
	r.BytesSent += bytesSent
	r.BytesReceived += bytesReceived

	if err != nil {
		r.ErrorCount++
		errStr := err.Error()
		if len(errStr) > 50 {
			errStr = errStr[:50]
		}
		r.Errors[errStr]++
	} else if statusCode >= 200 && statusCode < 300 {
		r.SuccessCount++
	} else {
		r.ErrorCount++
	}

	r.StatusCodes[statusCode]++
	r.Latencies = append(r.Latencies, latency)
}

func (r *Results) Print() {
	r.mu.Lock()
	defer r.mu.Unlock()

	fmt.Println("\n" + "============================================================")
	fmt.Println("LOAD TEST RESULTS")
	fmt.Println("============================================================")

	fmt.Printf("\nDuration: %v\n", r.Duration)
	fmt.Printf("Total Requests: %d\n", r.TotalRequests)

	if r.TotalRequests > 0 {
		fmt.Printf("Successful: %d (%.1f%%)\n", r.SuccessCount, float64(r.SuccessCount)/float64(r.TotalRequests)*100)
		fmt.Printf("Failed: %d (%.1f%%)\n", r.ErrorCount, float64(r.ErrorCount)/float64(r.TotalRequests)*100)
	} else {
		fmt.Printf("Successful: %d (N/A)\n", r.SuccessCount)
		fmt.Printf("Failed: %d (N/A)\n", r.ErrorCount)
	}

	durationSecs := r.Duration.Seconds()
	if durationSecs > 0 {
		fmt.Printf("Requests/sec: %.2f\n", float64(r.TotalRequests)/durationSecs)
	} else {
		fmt.Printf("Requests/sec: N/A\n")
	}

	// Throughput
	fmt.Printf("\nThroughput:\n")
	if durationSecs > 0 {
		fmt.Printf("  Sent: %.2f MB (%.2f MB/s)\n", float64(r.BytesSent)/1024/1024, float64(r.BytesSent)/1024/1024/durationSecs)
		fmt.Printf("  Received: %.2f MB (%.2f MB/s)\n", float64(r.BytesReceived)/1024/1024, float64(r.BytesReceived)/1024/1024/durationSecs)
	} else {
		fmt.Printf("  Sent: %.2f MB\n", float64(r.BytesSent)/1024/1024)
		fmt.Printf("  Received: %.2f MB\n", float64(r.BytesReceived)/1024/1024)
	}

	// Status codes
	fmt.Printf("\nStatus Codes:\n")
	for code, count := range r.StatusCodes {
		fmt.Printf("  %d: %d\n", code, count)
	}

	// Latency percentiles
	if len(r.Latencies) > 0 {
		sort.Slice(r.Latencies, func(i, j int) bool {
			return r.Latencies[i] < r.Latencies[j]
		})

		fmt.Printf("\nLatency Percentiles:\n")
		fmt.Printf("  Min: %v\n", r.Latencies[0])
		fmt.Printf("  P50: %v\n", r.Latencies[len(r.Latencies)*50/100])
		fmt.Printf("  P90: %v\n", r.Latencies[len(r.Latencies)*90/100])
		fmt.Printf("  P95: %v\n", r.Latencies[len(r.Latencies)*95/100])
		fmt.Printf("  P99: %v\n", r.Latencies[len(r.Latencies)*99/100])
		fmt.Printf("  Max: %v\n", r.Latencies[len(r.Latencies)-1])

		// Calculate average
		var total time.Duration
		for _, l := range r.Latencies {
			total += l
		}
		fmt.Printf("  Avg: %v\n", total/time.Duration(len(r.Latencies)))
	}

	// Errors
	if len(r.Errors) > 0 {
		fmt.Printf("\nError Summary:\n")
		for errStr, count := range r.Errors {
			fmt.Printf("  %s: %d\n", errStr, count)
		}
	}

	fmt.Println("\n" + "============================================================")
}

// Run executes the prepared request mix until its context deadline.
func (lt *LoadTester) Run() *Results {
	results := NewResults()
	ctx, cancel := context.WithTimeout(context.Background(), lt.duration)
	defer cancel()
	start := time.Now()
	var workers sync.WaitGroup
	var next atomic.Uint64
	delay := lt.rampUp / time.Duration(lt.concurrency)
	for i := range lt.concurrency {
		workers.Go(func() {
			if err := waitFor(ctx, delay*time.Duration(i)); err != nil {
				return
			}
			for ctx.Err() == nil {
				factory := lt.work.requests[(next.Add(1)-1)%uint64(len(lt.work.requests))]
				req, err := factory(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					results.Record(0, 0, err, 0, 0)
					continue
				}
				lt.execute(req, results)
			}
		})
	}
	workers.Wait()
	results.Duration = time.Since(start)
	return results
}

func (lt *LoadTester) execute(req *http.Request, results *Results) {
	start := time.Now()
	resp, err := lt.client.Do(req)
	if err != nil {
		results.Record(time.Since(start), 0, err, req.ContentLength, 0)
		return
	}
	defer resp.Body.Close()
	const maxResponseBytes = 1 << 20
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if len(body) > maxResponseBytes {
		err = fmt.Errorf("response exceeds %d bytes", maxResponseBytes)
	}
	results.Record(time.Since(start), resp.StatusCode, err, req.ContentLength, int64(len(body)))
}

func waitFor(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
