package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

type runnerTransport func(*http.Request) (*http.Response, error)

func (f runnerTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type observedResponseBody struct {
	io.Reader
	read   int
	closed bool
}

func (b *observedResponseBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.read += n
	return n, err
}
func (b *observedResponseBody) Close() error { b.closed = true; return nil }

type failedResponseReader struct{}

func (failedResponseReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestRunnerAccountsForRefusalAndIncompleteResponses(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		body       io.Reader
		failure    error
		wantBytes  int64
		wantError  bool
		wantPrefix string
	}{
		{name: "successful body", status: 201, body: strings.NewReader("created"), wantBytes: 7},
		{name: "tenant rate refusal", status: 429, body: strings.NewReader("limited"), wantBytes: 7, wantError: true},
		{name: "oversized success is incomplete", status: 200, body: strings.NewReader(strings.Repeat("x", 2<<20)), wantBytes: (1 << 20) + 1, wantError: true, wantPrefix: "response exceeds"},
		{name: "truncated body", status: 200, body: failedResponseReader{}, wantError: true, wantPrefix: "unexpected EOF"},
		{name: "transport failure", failure: errors.New(strings.Repeat("unavailable", 10)), wantError: true, wantPrefix: "unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &observedResponseBody{Reader: tc.body}
			runner := LoadTester{client: &http.Client{Transport: runnerTransport(func(*http.Request) (*http.Response, error) {
				if tc.failure != nil {
					return nil, tc.failure
				}
				return &http.Response{StatusCode: tc.status, Header: make(http.Header), Body: body}, nil
			})}}
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://example.test/request", strings.NewReader("input"))
			require.NoError(t, err)
			result := NewResults()
			runner.execute(req, result)
			require.EqualValues(t, 1, result.TotalRequests)
			require.EqualValues(t, 5, result.BytesSent)
			require.Equal(t, tc.wantBytes, result.BytesReceived)
			require.EqualValues(t, 1, result.StatusCodes[tc.status])
			if tc.wantError {
				require.EqualValues(t, 1, result.ErrorCount)
				require.Zero(t, result.SuccessCount)
			} else {
				require.EqualValues(t, 1, result.SuccessCount)
				require.Zero(t, result.ErrorCount)
			}
			if tc.failure == nil {
				require.True(t, body.closed)
				require.LessOrEqual(t, body.read, (1<<20)+1, "do not drain an unbounded response")
			}
			if tc.wantPrefix != "" {
				require.Len(t, result.Errors, 1)
				for message := range result.Errors {
					require.Contains(t, message, tc.wantPrefix)
					require.LessOrEqual(t, len(message), 50)
				}
			}
		})
	}
}

func TestRunnerDeadlineCancelsInFlightAndUnstartedWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var started atomic.Int64
		runner := LoadTester{duration: time.Second, concurrency: 4, rampUp: 8 * time.Second,
			client: &http.Client{Transport: runnerTransport(func(r *http.Request) (*http.Response, error) {
				started.Add(1)
				<-r.Context().Done()
				return nil, r.Context().Err()
			})},
			work: workload{requests: []requestFactory{func(ctx context.Context) (*http.Request, error) {
				return http.NewRequestWithContext(ctx, http.MethodGet, "http://example.test/slow", nil)
			}}},
		}
		result := runner.Run()
		require.Equal(t, time.Second, result.Duration)
		require.EqualValues(t, 1, started.Load(), "workers still in ramp-up must not dispatch after the deadline")
		require.EqualValues(t, 1, result.TotalRequests)
		require.EqualValues(t, 1, result.ErrorCount)
	})
}

func TestRunnerRecordsFactoryFailureAndStopsCanceledFactory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		runner := LoadTester{duration: time.Second, concurrency: 1,
			work: workload{requests: []requestFactory{func(ctx context.Context) (*http.Request, error) {
				calls++
				if calls == 1 {
					return nil, errors.New("fixture unavailable")
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}}},
		}
		result := runner.Run()
		require.Equal(t, 2, calls)
		require.EqualValues(t, 1, result.TotalRequests, "shutdown cancellation is not a second workload failure")
		require.EqualValues(t, 1, result.Errors["fixture unavailable"])
		require.Equal(t, time.Second, result.Duration)
	})
}

func captureRunnerOutput(t *testing.T, run func()) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "output")
	require.NoError(t, err)
	defer file.Close()
	previous := os.Stdout
	os.Stdout = file
	defer func() { os.Stdout = previous }()
	run()
	_, err = file.Seek(0, io.SeekStart)
	require.NoError(t, err)
	data, err := io.ReadAll(file)
	require.NoError(t, err)
	return string(data)
}

func TestCommandReportsHTTPResultsWithoutFollowingRedirects(t *testing.T) {
	var redirected atomic.Int64
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirected.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer destination.Close()
	for _, tc := range []struct {
		name, scenario string
		status         int
	}{
		{"refused redirect", "connection", http.StatusFound},
		{"accepted callback HTTP response", "callback", http.StatusNoContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int64
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if tc.scenario == "connection" {
					require.Equal(t, "Bearer intentionally-invalid-token", r.Header.Get("Authorization"))
					w.Header().Set("Location", destination.URL)
				} else {
					require.Empty(t, r.Header.Get("X-Fred-Signature"))
				}
				w.WriteHeader(tc.status)
			}))
			defer target.Close()
			previousFlags, previousArgs, previousLogger := flag.CommandLine, os.Args, slog.Default()
			t.Cleanup(func() { flag.CommandLine, os.Args = previousFlags, previousArgs; slog.SetDefault(previousLogger) })
			flag.CommandLine = flag.NewFlagSet("loadtest", flag.ContinueOnError)
			os.Args = []string{"loadtest", "-target", target.URL, "-traffic=rejection", "-scenario=" + tc.scenario,
				"-duration=1s", "-concurrency=1", "-ramp-up=0", "-verbose"}
			output := captureRunnerOutput(t, main)
			require.Positive(t, requests.Load())
			require.Zero(t, redirected.Load(), "the command must not forward even invalid credentials to redirect destinations")
			require.Contains(t, output, "LOAD TEST RESULTS")
			require.Contains(t, output, "Latency Percentiles:")
			if tc.status == http.StatusFound {
				require.Contains(t, output, "Successful: 0 (")
				require.Contains(t, output, "302:")
			} else {
				require.Regexp(t, `Successful: [1-9][0-9]* \(`, output)
				require.Contains(t, output, "204:")
			}
		})
	}
}

func TestEmptyResultsExplainUnavailableRates(t *testing.T) {
	output := captureRunnerOutput(t, NewResults().Print)
	require.Contains(t, output, "Successful: 0 (N/A)")
	require.Contains(t, output, "Failed: 0 (N/A)")
	require.Contains(t, output, "Requests/sec: N/A")
	require.NotContains(t, output, "NaN")
	require.NotContains(t, output, "Inf")
}
