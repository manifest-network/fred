package shared

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backendidentity"
)

const (
	callbackBenchmarkStorageID = "550e8400-e29b-41d4-a716-446655440000"
	callbackBenchmarkURL       = "https://fred.example/callbacks/provision?lifecycle_id=650e8400-e29b-41d4-a716-446655440000"
)

// BenchmarkCallbackOutboxLifecycleEnqueue measures lifecycle replacement as
// the number of unrelated lease queues grows. Each timed enqueue replaces the
// target lease's prior observation, so the durable backlog stays at the stated
// size instead of growing with the benchmark iteration count.
func BenchmarkCallbackOutboxLifecycleEnqueue(b *testing.B) {
	for _, leaseCount := range callbackBenchmarkScales() {
		b.Run(fmt.Sprintf("leases=%d", leaseCount), func(b *testing.B) {
			store := newCallbackBenchmarkStore(b)
			leases := seedCallbackBenchmarkBacklog(b, store, leaseCount)
			assertCallbackBenchmarkBacklog(b, store, leaseCount)

			entry := callbackBenchmarkEntry(leases[0])
			b.ReportAllocs()
			b.ReportMetric(float64(leaseCount), "backlog_leases")
			b.ResetTimer()
			for b.Loop() {
				if _, err := store.StoreEntry(entry); err != nil {
					b.Fatal(err)
				}
			}

			// b.Loop stops the timer before returning false. Keep the structural
			// sanity check out of the measured enqueue path.
			assertCallbackBenchmarkBacklog(b, store, leaseCount)
			target, err := store.listPending(leases[0])
			if err != nil {
				b.Fatal(err)
			}
			if len(target) != 1 {
				b.Fatalf("target lifecycle queue contains %d callbacks, want 1", len(target))
			}
		})
	}
}

// BenchmarkCallbackOutboxReplayBacklog exercises the real replay discovery,
// per-lease scan, payload construction, and bounded worker fan-out. The local
// transport returns a deterministic 503, making every callback remain due for
// the next iteration without network I/O, backoff sleeps, or fixture rebuilds.
func BenchmarkCallbackOutboxReplayBacklog(b *testing.B) {
	for _, leaseCount := range callbackBenchmarkScales() {
		b.Run(fmt.Sprintf("leases=%d", leaseCount), func(b *testing.B) {
			store := newCallbackBenchmarkStore(b)
			seedCallbackBenchmarkBacklog(b, store, leaseCount)
			assertCallbackBenchmarkBacklog(b, store, leaseCount)

			storageIdentity, err := backendidentity.Parse(callbackBenchmarkStorageID)
			if err != nil {
				b.Fatal(err)
			}
			var requests atomic.Int64
			client := &http.Client{Transport: callbackBenchmarkRoundTripper(func(*http.Request) (*http.Response, error) {
				requests.Add(1)
				return &http.Response{
					StatusCode: http.StatusServiceUnavailable,
					Header:     make(http.Header),
					Body:       http.NoBody,
				}, nil
			})}
			noBackoff := [CallbackMaxAttempts]time.Duration{}
			sender, err := NewCallbackSender(CallbackSenderConfig{
				Store:           store,
				HTTPClient:      client,
				Secret:          "callback-benchmark-secret-value!",
				Logger:          slog.New(slog.DiscardHandler),
				StopCtx:         context.Background(),
				Backoff:         &noBackoff,
				BeforeReplay:    func(context.Context) error { return nil },
				BeforeDelivery:  func(context.Context) error { return nil },
				StorageIdentity: storageIdentity,
			})
			if err != nil {
				b.Fatal(err)
			}

			iterations := int64(0)
			b.ReportAllocs()
			b.ReportMetric(float64(leaseCount), "due_callbacks")
			b.ResetTimer()
			for b.Loop() {
				sender.ReplayPendingCallbacks()
				iterations++
			}

			// Failed delivery must leave the complete backlog durable, and every
			// due head must receive exactly the configured bounded retry chain.
			assertCallbackBenchmarkBacklog(b, store, leaseCount)
			wantRequests := iterations * int64(leaseCount) * CallbackMaxAttempts
			if got := requests.Load(); got != wantRequests {
				b.Fatalf("transport received %d requests, want %d", got, wantRequests)
			}
			if iterations > 0 {
				b.ReportMetric(float64(requests.Load())/float64(iterations), "requests/op")
			}
		})
	}
}

type callbackBenchmarkRoundTripper func(*http.Request) (*http.Response, error)

func (roundTrip callbackBenchmarkRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

func newCallbackBenchmarkStore(b *testing.B) *CallbackStore {
	b.Helper()
	store, err := NewCallbackStore(CallbackStoreConfig{
		DBPath: filepath.Join(b.TempDir(), "callbacks.db"),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close callback benchmark store: %v", err)
		}
	})
	return store
}

func seedCallbackBenchmarkBacklog(
	b *testing.B,
	store *CallbackStore,
	leaseCount int,
) []string {
	b.Helper()
	leases := make([]string, leaseCount)
	for index := range leaseCount {
		leaseUUID := callbackBenchmarkLeaseUUID(index)
		leases[index] = leaseUUID
		if _, err := store.StoreEntry(callbackBenchmarkEntry(leaseUUID)); err != nil {
			b.Fatalf("seed callback %d: %v", index, err)
		}
	}
	return leases
}

func assertCallbackBenchmarkBacklog(
	b *testing.B,
	store *CallbackStore,
	want int,
) {
	b.Helper()
	pending, err := store.ListPending()
	if err != nil {
		b.Fatal(err)
	}
	if len(pending) != want {
		b.Fatalf("callback backlog contains %d entries, want %d", len(pending), want)
	}
}

func callbackBenchmarkEntry(leaseUUID string) CallbackEntry {
	return CallbackEntry{
		LeaseUUID:        leaseUUID,
		CallbackURL:      callbackBenchmarkURL,
		DeliveryKind:     CallbackDeliveryKindLifecycle,
		Success:          true,
		Status:           backend.CallbackStatusSuccess,
		Backend:          "benchmark",
		BackendStorageID: callbackBenchmarkStorageID,
		CreatedAt:        time.Unix(1, 0).UTC(),
	}
}

func callbackBenchmarkLeaseUUID(index int) string {
	// Version and variant bits make these canonical UUIDv4 values while the
	// numeric suffix keeps fixture construction deterministic.
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", index+1)
}

func callbackBenchmarkScales() [3]int {
	return [...]int{1, 128, 1024}
}
