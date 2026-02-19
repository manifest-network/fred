// Package main provides a load testing tool for fred.
//
// Usage:
//
//	loadtest -target http://localhost:8080 -duration 30s -concurrency 50 -scenario mixed
//
// Scenarios:
//   - payload: Test payload upload endpoint
//   - connection: Test connection info retrieval
//   - callback: Test backend callback processing
//   - mixed: Realistic mix of all operations
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
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

	"github.com/google/uuid"

	"github.com/manifest-network/fred/internal/api"
)

func main() {
	var (
		target         = flag.String("target", "http://localhost:8080", "Target fred URL")
		duration       = flag.Duration("duration", 30*time.Second, "Test duration")
		concurrency    = flag.Int("concurrency", 50, "Number of concurrent workers")
		scenario       = flag.String("scenario", "mixed", "Test scenario: payload, connection, callback, mixed")
		payloadSize    = flag.Int("payload-size", 1024, "Payload size in bytes")
		rampUp         = flag.Duration("ramp-up", 5*time.Second, "Ramp-up time to reach full concurrency")
		callbackSecret = flag.String("callback-secret", "", "HMAC secret for callback signing (min 32 bytes, required for callback scenario)")
		verbose        = flag.Bool("verbose", false, "Verbose output")
	)
	flag.Parse()

	// Configure logging
	logLevel := slog.LevelInfo
	if *verbose {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	// Validate
	if *scenario == "callback" && *callbackSecret == "" {
		slog.Error("callback-secret is required for callback scenario")
		os.Exit(1)
	}

	slog.Info("starting load test",
		"target", *target,
		"duration", *duration,
		"concurrency", *concurrency,
		"scenario", *scenario,
		"payload_size", *payloadSize,
		"ramp_up", *rampUp,
	)

	// Create load tester
	var callbackAuth *api.CallbackAuthenticator
	if *callbackSecret != "" {
		var err error
		callbackAuth, err = api.NewCallbackAuthenticator(*callbackSecret)
		if err != nil {
			slog.Error("invalid callback secret", "error", err)
			os.Exit(1)
		}
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		slog.Error("failed to generate key pair", "error", err)
		os.Exit(1)
	}

	lt := &LoadTester{
		target:       *target,
		duration:     *duration,
		concurrency:  *concurrency,
		payloadSize:  *payloadSize,
		rampUp:       *rampUp,
		callbackAuth: callbackAuth,
		pub:          pub,
		priv:         priv,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        *concurrency * 2,
				MaxIdleConnsPerHost: *concurrency * 2,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}

	// Run scenario
	var results *Results
	switch *scenario {
	case "payload":
		results = lt.RunPayloadTest()
	case "connection":
		results = lt.RunConnectionTest()
	case "callback":
		results = lt.RunCallbackTest()
	case "mixed":
		results = lt.RunMixedTest()
	default:
		slog.Error("unknown scenario", "scenario", *scenario)
		os.Exit(1)
	}

	// Print results
	results.Print()
}

// LoadTester performs load testing against fred.
type LoadTester struct {
	target       string
	duration     time.Duration
	concurrency  int
	payloadSize  int
	rampUp       time.Duration
	callbackAuth *api.CallbackAuthenticator
	client       *http.Client

	// Pre-generated key pair reused across all requests to avoid the cost
	// of ed25519.GenerateKey on every iteration.
	pub  ed25519.PublicKey
	priv ed25519.PrivateKey
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

// RunPayloadTest tests the payload upload endpoint.
func (lt *LoadTester) RunPayloadTest() *Results {
	slog.Info("running payload upload test")
	results := NewResults()

	ctx, cancel := context.WithTimeout(context.Background(), lt.duration)
	defer cancel()

	var wg sync.WaitGroup
	startTime := time.Now()

	// Ramp up workers gradually
	workerDelay := lt.rampUp / time.Duration(lt.concurrency)

	for i := 0; i < lt.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Stagger start
			time.Sleep(workerDelay * time.Duration(workerID))

			for {
				select {
				case <-ctx.Done():
					return
				default:
					lt.doPayloadRequest(ctx, results)
				}
			}
		}(i)
	}

	wg.Wait()
	results.Duration = time.Since(startTime)
	return results
}

func (lt *LoadTester) doPayloadRequest(ctx context.Context, results *Results) {
	// Generate test data
	leaseUUID := uuid.New().String()
	payload := make([]byte, lt.payloadSize)
	rand.Read(payload)
	hash := sha256.Sum256(payload)
	hashStr := hex.EncodeToString(hash[:])

	// Generate auth token (simplified - real implementation would need proper signing)
	token := lt.generateAuthToken(leaseUUID, hashStr)

	url := fmt.Sprintf("%s/v1/leases/%s/data", lt.target, leaseUUID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		results.Record(0, 0, err, 0, 0)
		return
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Authorization", "Bearer "+token)

	start := time.Now()
	resp, err := lt.client.Do(req)
	latency := time.Since(start)

	if err != nil {
		results.Record(latency, 0, err, int64(lt.payloadSize), 0)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		results.Record(latency, resp.StatusCode, fmt.Errorf("read body: %w", err), int64(lt.payloadSize), int64(len(body)))
		return
	}
	results.Record(latency, resp.StatusCode, nil, int64(lt.payloadSize), int64(len(body)))
}

// RunConnectionTest tests the connection info endpoint.
func (lt *LoadTester) RunConnectionTest() *Results {
	slog.Info("running connection info test")
	results := NewResults()

	ctx, cancel := context.WithTimeout(context.Background(), lt.duration)
	defer cancel()

	var wg sync.WaitGroup
	startTime := time.Now()

	workerDelay := lt.rampUp / time.Duration(lt.concurrency)

	for i := 0; i < lt.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			time.Sleep(workerDelay * time.Duration(workerID))

			for {
				select {
				case <-ctx.Done():
					return
				default:
					lt.doConnectionRequest(ctx, results)
				}
			}
		}(i)
	}

	wg.Wait()
	results.Duration = time.Since(startTime)
	return results
}

func (lt *LoadTester) doConnectionRequest(ctx context.Context, results *Results) {
	leaseUUID := uuid.New().String()
	token := lt.generateAuthToken(leaseUUID, "")

	url := fmt.Sprintf("%s/v1/leases/%s/connection", lt.target, leaseUUID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		results.Record(0, 0, err, 0, 0)
		return
	}

	req.Header.Set("Authorization", "Bearer "+token)

	start := time.Now()
	resp, err := lt.client.Do(req)
	latency := time.Since(start)

	if err != nil {
		results.Record(latency, 0, err, 0, 0)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		results.Record(latency, resp.StatusCode, fmt.Errorf("read body: %w", err), 0, int64(len(body)))
		return
	}
	results.Record(latency, resp.StatusCode, nil, 0, int64(len(body)))
}

// RunCallbackTest tests the callback endpoint.
func (lt *LoadTester) RunCallbackTest() *Results {
	slog.Info("running callback test")
	results := NewResults()

	ctx, cancel := context.WithTimeout(context.Background(), lt.duration)
	defer cancel()

	var wg sync.WaitGroup
	startTime := time.Now()

	workerDelay := lt.rampUp / time.Duration(lt.concurrency)

	for i := 0; i < lt.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			time.Sleep(workerDelay * time.Duration(workerID))

			for {
				select {
				case <-ctx.Done():
					return
				default:
					lt.doCallbackRequest(ctx, results)
				}
			}
		}(i)
	}

	wg.Wait()
	results.Duration = time.Since(startTime)
	return results
}

func (lt *LoadTester) doCallbackRequest(ctx context.Context, results *Results) {
	leaseUUID := uuid.New().String()

	// Create callback payload
	callback := map[string]interface{}{
		"lease_uuid": leaseUUID,
		"status":     "success",
		"connection": map[string]interface{}{
			"host":     "10.0.0.1",
			"port":     8080,
			"protocol": "https",
		},
	}

	body, _ := json.Marshal(callback)

	// Sign the callback
	signature := lt.signCallback(body)

	url := fmt.Sprintf("%s/callbacks/provision", lt.target)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		results.Record(0, 0, err, 0, 0)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Fred-Signature", signature)

	start := time.Now()
	resp, err := lt.client.Do(req)
	latency := time.Since(start)

	if err != nil {
		results.Record(latency, 0, err, int64(len(body)), 0)
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		results.Record(latency, resp.StatusCode, fmt.Errorf("read body: %w", err), int64(len(body)), int64(len(respBody)))
		return
	}
	results.Record(latency, resp.StatusCode, nil, int64(len(body)), int64(len(respBody)))
}

// RunMixedTest runs a realistic mix of operations.
func (lt *LoadTester) RunMixedTest() *Results {
	slog.Info("running mixed workload test")
	results := NewResults()

	ctx, cancel := context.WithTimeout(context.Background(), lt.duration)
	defer cancel()

	var wg sync.WaitGroup
	var opCounter atomic.Int64
	startTime := time.Now()

	workerDelay := lt.rampUp / time.Duration(lt.concurrency)

	for i := 0; i < lt.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			time.Sleep(workerDelay * time.Duration(workerID))

			for {
				select {
				case <-ctx.Done():
					return
				default:
					op := opCounter.Add(1)
					switch op % 10 {
					case 0, 1, 2, 3: // 40% payload uploads
						lt.doPayloadRequest(ctx, results)
					case 4, 5, 6, 7, 8: // 50% connection checks
						lt.doConnectionRequest(ctx, results)
					case 9: // 10% callbacks (if secret provided)
						if lt.callbackAuth != nil {
							lt.doCallbackRequest(ctx, results)
						} else {
							lt.doConnectionRequest(ctx, results)
						}
					}
				}
			}
		}(i)
	}

	wg.Wait()
	results.Duration = time.Since(startTime)
	return results
}

// generateAuthToken creates a test auth token using the pre-generated key pair.
// Note: These tokens will be rejected by fred (invalid on-chain identity).
// For realistic testing, you need real keys and proper ADR-036 signing.
func (lt *LoadTester) generateAuthToken(leaseUUID, metaHash string) string {
	timestamp := time.Now().Unix()
	tenant := "manifest1loadtest" + leaseUUID[:8]

	msg := fmt.Sprintf("manifest lease data %s %s %d", leaseUUID, metaHash, timestamp)
	signature := ed25519.Sign(lt.priv, []byte(msg))

	token := map[string]interface{}{
		"tenant":     tenant,
		"lease_uuid": leaseUUID,
		"meta_hash":  metaHash,
		"timestamp":  timestamp,
		"pub_key":    base64.StdEncoding.EncodeToString(lt.pub),
		"signature":  base64.StdEncoding.EncodeToString(signature),
	}

	tokenBytes, _ := json.Marshal(token)
	return base64.StdEncoding.EncodeToString(tokenBytes)
}

// signCallback signs a callback payload using the CallbackAuthenticator.
func (lt *LoadTester) signCallback(payload []byte) string {
	return lt.callbackAuth.ComputeSignature(payload)
}
