package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const truncationMarker = "…(truncated)"

func TestTruncateLogValue(t *testing.T) {
	assert.Equal(t, "short", truncateLogValue("short", 8))
	assert.Equal(t, "12345678", truncateLogValue("12345678", 8), "a value at the limit is kept whole")
	assert.Equal(t, "1234"+truncationMarker, truncateLogValue("123456789", 4))

	// "é" is two bytes at offsets 3 and 4; a cut at 4 would split it.
	got := truncateLogValue("abcédef", 4)
	assert.Equal(t, "abc"+truncationMarker, got)
	assert.True(t, utf8.ValidString(got))
}

// TestLoggingMiddleware_TruncatesClientChosenFields is the log half of
// ENG-2150: the request log line carries a bounded prefix of a client's method
// and path, never the whole token.
func TestLoggingMiddleware_TruncatesClientChosenFields(t *testing.T) {
	logs := captureDefaultLogger(t)
	h := loggingMiddleware(http.NewServeMux())

	method := strings.Repeat("M", 1_000)
	path := "/" + strings.Repeat("p", 10_000)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(method, path, nil))

	record := logs.record(t, "http request")
	assert.Equal(t, method[:maxLoggedMethodBytes]+truncationMarker, record["method"])
	assert.Equal(t, path[:maxLoggedPathBytes]+truncationMarker, record["path"])
}

// TestRateLimiter_TruncatesLoggedPath covers the rejection log, which an
// unauthenticated client can produce once per refused request.
func TestRateLimiter_TruncatesLoggedPath(t *testing.T) {
	logs := captureDefaultLogger(t)
	limiter := NewRateLimiter(1, 1, nil)
	h := limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))

	path := "/" + strings.Repeat("q", 10_000)
	for range 2 {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, path, nil))
	}

	record := logs.record(t, "rate limit exceeded")
	assert.Equal(t, path[:maxLoggedPathBytes]+truncationMarker, record["path"])
}

// TestServer_RejectsOversizedRequestHead pins maxHeaderBytes on the real
// listener: a request head over it is answered 431 before any handler runs,
// so it reaches neither the metrics nor the request log, while a head with a
// generous cookie still succeeds.
func TestServer_RejectsOversizedRequestHead(t *testing.T) {
	addr := freePort(t)
	s := newTestServer(t, addr)
	startAndWaitForServer(t, s, addr)
	t.Cleanup(func() { _ = s.Shutdown(context.Background()) })

	// Go allows 4 KiB above MaxHeaderBytes before refusing.
	oversized := strings.Repeat("X", maxHeaderBytes+8<<10)
	for name, head := range map[string]string{
		"method": oversized + " /health HTTP/1.1\r\nHost: fred\r\n\r\n",
		"header": "GET /health HTTP/1.1\r\nHost: fred\r\nX-Pad: " + oversized + "\r\n\r\n",
	} {
		assert.Equal(t, http.StatusRequestHeaderFieldsTooLarge, rawRequestStatus(t, addr, head), name)
	}
	assert.NotContains(t, gatheredLabels(t, "method"), oversized)

	cookie := strings.Repeat("c", 32<<10)
	assert.Equal(t, http.StatusOK,
		rawRequestStatus(t, addr, "GET /health HTTP/1.1\r\nHost: fred\r\nCookie: session="+cookie+"\r\n\r\n"),
		"a 32 KiB cookie fits")
}

// rawRequestStatus writes head on a new connection and returns the response
// status. A refused head may still be partly unread when the server answers,
// so the write runs alongside the read.
func rawRequestStatus(t *testing.T, addr, head string) int {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))

	go func() { _, _ = conn.Write([]byte(head)) }()
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}

// capturedLogs collects the default logger's JSON records for one test.
type capturedLogs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *capturedLogs) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

// record returns the first record with message msg.
func (c *capturedLogs) record(t *testing.T, msg string) map[string]any {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for line := range strings.SplitSeq(c.buf.String(), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if record[slog.MessageKey] == msg {
			return record
		}
	}
	require.FailNow(t, fmt.Sprintf("no %q record logged", msg))
	return nil
}

// captureDefaultLogger points the default logger at a buffer until the test
// ends. Tests that call it must not run in parallel.
func captureDefaultLogger(t *testing.T) *capturedLogs {
	t.Helper()
	logs := &capturedLogs{}
	// slog.SetDefault also redirects the log package, and restoring the
	// original default logger does not undo that, so restore log too.
	previous, previousOutput, previousFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previous)
		log.SetOutput(previousOutput)
		log.SetFlags(previousFlags)
	})
	return logs
}
