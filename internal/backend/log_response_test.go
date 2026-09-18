package backend

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLogResponseFragmentEncodingMatchesJSON(t *testing.T) {
	for _, value := range []string{
		"", "live\nlogs", "\x00\b\f\n\r\t\\\"<>&\u2028\u2029",
		strings.Repeat("x", (8<<10)-1) + "😀é界" + strings.Repeat("\xff", 8<<10),
		strings.Repeat("\x80", 2*(8<<10)+1), strings.Repeat("\xf0\x90\x80", 3000),
	} {
		logs := map[string]string{"web/0": value, "failed/web/0": "failed\n"}
		response, err := NewLogResponse(logs)
		require.NoError(t, err)
		want, err := json.Marshal(logs)
		require.NoError(t, err)
		logs["web/0"] = "mutated after snapshot"
		var got bytes.Buffer
		require.NoError(t, response.WriteJSON(&got))
		require.Equal(t, string(want), got.String())
	}
}

func TestLogResponseEnvelopeAndEmptyShapes(t *testing.T) {
	for _, logs := range []map[string]string{nil, {}, {"live/0": "live", "failed/live/0": "failed"}} {
		response, err := NewLogResponse(logs)
		require.NoError(t, err)
		for _, metadata := range []map[string]string{nil, {}, {"lease_uuid": "lease", "tenant": "tenant", "provider_uuid": "provider"}} {
			var got bytes.Buffer
			require.NoError(t, response.WriteEnvelopeJSON(&got, metadata))
			want := make(map[string]any)
			for key, value := range metadata {
				want[key] = value
			}
			want["logs"] = logs
			encoded, err := json.Marshal(want)
			require.NoError(t, err)
			require.JSONEq(t, string(encoded), got.String())
		}
	}
	response, err := NewLogResponse(nil)
	require.NoError(t, err)
	require.Error(t, response.WriteEnvelopeJSON(io.Discard, map[string]string{"logs": "replace"}))
}

func TestDecodeLogResponseRejectsMalformedAndOverBudget(t *testing.T) {
	for _, body := range []string{
		`[]`, `"logs"`, `{"a":null}`, `{"a":1}`, `{"a":[]}`, `{"a":{}}`,
		`{"a":"one","a":"two"}`, `{"a":"one"} {}`, `null null`, `{`, `{"a":"one"`,
		`{"` + strings.Repeat("k", maxProjectedLogKeyBytes+1) + `":"v"}`,
	} {
		_, err := decodeLogResponse(io.NopCloser(strings.NewReader(body)), DefaultMaxLogsBytes)
		require.Error(t, err, "%q", body)
	}
	for _, body := range []string{`{"a":"one"}`, `null`, `{}`} {
		_, err := decodeLogResponse(io.NopCloser(strings.NewReader(body)), int64(len(body)))
		require.NoError(t, err)
		_, err = decodeLogResponse(io.NopCloser(strings.NewReader(body+" ")), int64(len(body)))
		require.ErrorIs(t, err, ErrResponseTooLarge, "trailing whitespace consumes the wire budget")
	}
	logs := make(map[string]string, maxProjectedLogEntries+1)
	for i := range maxProjectedLogEntries + 1 {
		logs[fmt.Sprint(i)] = ""
	}
	_, err := NewLogResponse(logs)
	require.ErrorIs(t, err, ErrResponseTooLarge)
	body, err := json.Marshal(logs)
	require.NoError(t, err)
	_, err = decodeLogResponse(io.NopCloser(bytes.NewReader(body)), DefaultMaxLogsBytes)
	require.ErrorIs(t, err, ErrResponseTooLarge)
	_, err = NewLogResponse(map[string]string{"a": strings.Repeat("x", MaxLogContentBytes+maxProjectedLogEntries*(len(AggregateLogLimitMessage)+1)+1)})
	require.ErrorIs(t, err, ErrResponseTooLarge)
}

func TestLogResponsePreservesRepairedBinaryContentBudget(t *testing.T) {
	// One invalid source byte becomes three decoded UTF-8 bytes. Charging the
	// decoded byte count directly would reject formerly valid full captures.
	logs := map[string]string{"live/0": strings.Repeat("\ufffd", MaxLogContentBytes)}
	response, err := NewLogResponse(logs)
	require.NoError(t, err)
	require.NoError(t, response.WriteJSON(io.Discard))
}

type logErrorWriter struct{ err error }

func (w logErrorWriter) Write([]byte) (int, error) { return 0, w.err }

func TestLogResponseStopsOnWriterFailure(t *testing.T) {
	want := errors.New("client gone")
	response, err := NewLogResponse(map[string]string{"a": strings.Repeat("x", 20<<10)})
	require.NoError(t, err)
	require.ErrorIs(t, response.WriteJSON(logErrorWriter{want}), want)
	require.ErrorIs(t, response.WriteEnvelopeJSON(logErrorWriter{want}, map[string]string{"tenant": "tenant"}), want)
}

func BenchmarkLogResponseWriteEscaped(b *testing.B) {
	response, err := NewLogResponse(map[string]string{"web/0": strings.Repeat("\x00", MaxLogContentBytes)})
	require.NoError(b, err)
	b.SetBytes(MaxLogContentBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		require.NoError(b, response.WriteJSON(io.Discard))
	}
}

type logFragmentCounter struct {
	bytes   int
	largest int
}

func (w *logFragmentCounter) Write(p []byte) (int, error) {
	w.bytes += len(p)
	w.largest = max(w.largest, len(p))
	return len(p), nil
}

func TestLogResponseWritesFullEscapedBudgetInBoundedFragments(t *testing.T) {
	response, err := NewLogResponse(map[string]string{"web/0": strings.Repeat("\x00", MaxLogContentBytes)})
	require.NoError(t, err)
	var writes logFragmentCounter
	require.NoError(t, response.WriteJSON(&writes))
	require.Equal(t, 6*MaxLogContentBytes+len(`{"web/0":""}`), writes.bytes)
	require.LessOrEqual(t, writes.largest, 6*(8<<10), "the encoder must not materialize the full escaped document before its first write")
	require.LessOrEqual(t, writes.bytes, MaxProjectedLogsResponseBytes)
}

func TestHTTPClientGetLogsPreservesBackendContentBudget(t *testing.T) {
	for _, test := range []struct {
		name string
		logs map[string]string
	}{
		{"full aggregate content", fullLogContentBudget()},
		{"JSON expansion", map[string]string{"web/0": strings.Repeat("\x00", 3<<20)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(test.logs)
			}))
			defer server.Close()
			client := newUnboundHTTPClientForTest(HTTPClientConfig{
				Name: "log-content-budget", BaseURL: server.URL, Timeout: 30 * time.Second,
			})
			logs, err := client.GetLogs(t.Context(), "lease-1", 100)
			require.NoError(t, err, "the default transport must carry valid backend output above 16 MiB")
			require.Equal(t, test.logs, logs)
		})
	}
}

func fullLogContentBudget() map[string]string {
	logs := make(map[string]string)
	remaining := MaxLogContentBytes
	for instance := 0; remaining > 0; instance++ {
		content := min(remaining, 5<<20)
		logs[fmt.Sprintf("web/%d", instance)] = strings.Repeat("x", content)
		remaining -= content
	}
	return logs
}
