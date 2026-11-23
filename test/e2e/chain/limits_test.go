//go:build integration

package chain

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/test/e2e"
)

// The tests in this file are about what providerd's API gives a client that
// asks too much of it, or asks while something providerd depends on is down:
// a bounded answer, in bounded time (G27, G32, G33).

// unthrottled are provider settings that keep the API's rate limits out of
// the way of a test that sends many requests from one address and as one
// tenant. The limits themselves are tested in-package.
var unthrottled = map[string]any{"rate_limit_rps": 1000, "rate_limit_burst": 1000, "tenant_rate_limit_rps": 0}

// send sends one request without a token to providerd and returns the status
// it answers with.
func send(t *testing.T, p *Provider, method, path string, headers map[string]string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, p.URL+path, nil)
	require.NoError(t, err)
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err, "%s %.60s", method, path)
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

// TestLimits_InventedMethodsAddNoMetricSeries sends three hundred requests,
// each with an HTTP method of its own invention. providerd counts its
// requests by method, and every series of a metric stays in memory for the
// life of the process, so a client that could name its own series could fill
// that memory (ENG-2150). All of them must be counted in one series.
func TestLimits_InventedMethodsAddNoMetricSeries(t *testing.T) {
	const requests, family = "fred_api_requests_total", "fred_api_request_duration_seconds_count"
	p := StartProviderWith(t, 1, Options{ProviderSettings: unthrottled})
	// One of each kind first, so that the series they are counted in exist
	// before the count: an invented method, and the read of the metrics.
	require.Equal(t, http.StatusMethodNotAllowed, send(t, p, "FRED0", "/health", nil))
	p.metricLines(t, requests)
	before := len(p.metricLines(t, requests))
	counted := p.Metric(t, requests, `method="other"`)

	const invented = 300
	for i := 1; i <= invented; i++ {
		require.Equal(t, http.StatusMethodNotAllowed, send(t, p, fmt.Sprintf("FRED%d", i), "/health", nil))
	}
	after := p.metricLines(t, requests)
	require.Len(t, after, before, "invented methods must not add a series:\n%s", strings.Join(after, "\n"))
	for _, line := range append(after, p.metricLines(t, family)...) {
		require.NotContains(t, line, "FRED", "a client's method reached a metric label")
	}
	require.InDelta(t, counted+invented, p.Metric(t, requests, `method="other"`), 0, "every request must still be counted")
}

// TestLimits_OversizedRequestHeadIsRefused sends requests whose header, or
// whose request line, is larger than providerd reads. Each must be refused
// as too large, and providerd must go on serving. A large head within the
// limit is served.
func TestLimits_OversizedRequestHeadIsRefused(t *testing.T) {
	p := StartProvider(t, 1)
	padding := func(kib int) string { return strings.Repeat("a", kib<<10) }

	require.Equal(t, http.StatusOK, send(t, p, http.MethodGet, "/health", map[string]string{"X-Padding": padding(32)}),
		"a header of 32 KiB is within the limit")
	require.Equal(t, http.StatusRequestHeaderFieldsTooLarge, send(t, p, http.MethodGet, "/health", map[string]string{"X-Padding": padding(80)}),
		"a header of 80 KiB")
	require.Equal(t, http.StatusRequestHeaderFieldsTooLarge, send(t, p, http.MethodGet, "/health?padding="+padding(80), nil),
		"a request line of 80 KiB")
	require.Equal(t, http.StatusOK, send(t, p, http.MethodGet, "/health", nil))
}

// TestLimits_LogsAreServedOneAtATimeFromABoundedQueue reads a lease's logs
// from a client that does not take the answer. providerd holds a whole log
// answer in memory while it writes it, so it writes one at a time and lets
// only a few requests wait. While the first answer is stuck, the requests
// behind it must wait, one more than the queue holds must be refused at once
// with a Retry-After, and when the stuck client goes away the waiting ones
// must be answered.
func TestLimits_LogsAreServedOneAtATimeFromABoundedQueue(t *testing.T) {
	// How many requests may wait behind the one being answered.
	const queue = 8
	p := StartProviderWith(t, 1, Options{ProviderSettings: unthrottled})
	backend, tenant := p.Backends[0], p.NewTenant(t)
	// 10,000 lines, the most one request may ask for, of 400 characters that
	// JSON writes as six bytes each: an answer of some 24 MB, far more than a
	// connection buffers.
	lease := deployActive(t, p, tenant, p.SKUs.Stateless, map[string]any{
		"image": e2e.BusyboxImage,
		"command": []string{"sh", "-c",
			`line=$(printf '%0400d' 0 | tr 0 '<'); i=0; while [ $i -lt 10000 ]; do echo "$line"; i=$((i+1)); done; touch /tmp/written; exec sleep 3600`},
	})
	workload := running(t, backend, lease)
	eventually(t, "the workload of lease "+lease+" has written its log", func() bool {
		code, _ := backend.TryExec(t, workload.ID, "test", "-f", "/tmp/written")
		return code == 0
	})
	// Reading logs does not use a token up, so one serves every request.
	token := tenant.Token(t, lease)
	logs := "/v1/leases/" + lease + "/logs"

	// The first client asks for everything and reads only the status line,
	// through a receive buffer of a kilobyte.
	address := strings.TrimPrefix(p.URL, "http://")
	dialer := net.Dialer{Timeout: 10 * time.Second, Control: func(_, _ string, raw syscall.RawConn) error {
		var failed error
		if err := raw.Control(func(fd uintptr) {
			failed = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 1024)
		}); err != nil {
			return err
		}
		return failed
	}}
	stuck, err := dialer.DialContext(t.Context(), "tcp", address)
	require.NoError(t, err)
	defer func() { _ = stuck.Close() }()
	_, err = fmt.Fprintf(stuck, "GET %s?tail=10000 HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\n\r\n", logs, address, token)
	require.NoError(t, err)
	require.NoError(t, stuck.SetReadDeadline(time.Now().Add(time.Minute)))
	status := make([]byte, len("HTTP/1.1 200"))
	_, err = io.ReadFull(stuck, status)
	require.NoError(t, err)
	require.Equal(t, "HTTP/1.1 200", string(status), "the first answer must have begun")

	waiting := make(chan Answer, queue)
	for range queue {
		go func() {
			answer, err := tenant.exchange(context.Background(), http.MethodGet, logs+"?tail=1", token, nil, nil)
			if err != nil {
				answer.Error = err.Error()
			}
			waiting <- answer
		}()
	}
	select {
	case answer := <-waiting:
		t.Fatalf("a request was answered %d while another answer was being written: %.200s", answer.Status, answer.Body+answer.Error)
	case <-time.After(3 * time.Second):
	}

	asked := time.Now()
	refused := tenant.AskWith(t, token, http.MethodGet, lease, "logs?tail=1", nil, nil)
	require.Equal(t, http.StatusServiceUnavailable, refused.Status, "one request more than may wait: %.200s", refused.Body)
	require.Equal(t, "log response capacity exhausted", refused.Error)
	require.Equal(t, "1", refused.RetryAfter)
	require.Less(t, time.Since(asked), 2*time.Second, "a request that cannot wait must be refused at once")

	require.NoError(t, stuck.Close())
	for range queue {
		select {
		case answer := <-waiting:
			require.Equal(t, http.StatusOK, answer.Status, "a request that waited: %.200s", answer.Body+answer.Error)
		case <-time.After(time.Minute):
			t.Fatal("a waiting request was never answered")
		}
	}
	again := tenant.AskWith(t, token, http.MethodGet, lease, "logs?tail=1", nil, nil)
	require.Equal(t, http.StatusOK, again.Status, "there must be room again: %.200s", again.Body)
}

// TestHealth_AnswersWithinItsBudget asks providerd for its health with its
// backend stopped, with the chain not answering, and with both. The answer
// tells a load balancer whether providerd itself is alive, so it must come
// every time, as a 200, within its budget: three seconds for the chain and
// the backends together, and the time to look at providerd's own stores. It
// must say which of the two it could not reach.
func TestHealth_AnswersWithinItsBudget(t *testing.T) {
	const budget = 5 * time.Second
	p := StartProvider(t, 1)
	backend := p.Backends[0]
	backendCheck := "backend:" + backend.Name
	type health struct {
		Status string `json:"status"`
		Checks map[string]struct {
			Status string `json:"status"`
		} `json:"checks"`
	}
	read := func(when string) health {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL+"/health", nil)
		require.NoError(t, err)
		asked := time.Now()
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err, when)
		defer func() { _ = response.Body.Close() }()
		var answer health
		require.NoError(t, json.NewDecoder(response.Body).Decode(&answer), when)
		require.Less(t, time.Since(asked), budget, "%s: the answer took too long", when)
		require.Equal(t, http.StatusOK, response.StatusCode, when)
		return answer
	}
	requireHealth := func(when, status, chain, backendStatus string) {
		t.Helper()
		answer := read(when)
		require.Equal(t, status, answer.Status, "%s: %+v", when, answer)
		require.Equal(t, chain, answer.Checks["chain"].Status, "%s: %+v", when, answer)
		require.Equal(t, backendStatus, answer.Checks[backendCheck].Status, "%s: %+v", when, answer)
	}
	healthy := func() bool { return read("with everything up").Status == "healthy" }
	eventually(t, "providerd reports itself healthy", healthy)

	backend.Stop()
	requireHealth("with the backend stopped", "degraded", "healthy", "unhealthy")

	// The chain is resumed even when the test fails first.
	t.Cleanup(func() { p.Chain.Resume(t) })
	p.Chain.Pause(t)
	requireHealth("with the backend stopped and the chain not answering", "degraded", "unhealthy", "unhealthy")

	backend.Start()
	requireHealth("with the chain not answering", "degraded", "unhealthy", "healthy")

	p.Chain.Resume(t)
	eventually(t, "providerd reports itself healthy again", healthy)
}

// TestToken_IsAcceptedOnce uses a tenant's token twice, on a request that a
// token is good for once. The second use must be refused, and still be
// refused after providerd has restarted: what providerd remembers of a token
// has to outlive the process, or a restart would give every recent token a
// second life. A token just as old that was never used is still accepted
// after the restart, so it is the use, and not the age, that refuses the
// first one.
func TestToken_IsAcceptedOnce(t *testing.T) {
	p := StartProvider(t, 1)
	tenant := p.NewTenant(t)
	used := deployActive(t, p, tenant, p.SKUs.Stateless, runs(e2e.BusyboxImage))
	spare := deployActive(t, p, tenant, p.SKUs.Stateless, runs(e2e.BusyboxImage))
	connection := func(token, leaseUUID string) Answer {
		return tenant.AskWith(t, token, http.MethodGet, leaseUUID, "connection", nil, nil)
	}
	// Dated ahead, as far as providerd takes a token, so that both tokens
	// outlive the restart: a token is good for thirty seconds from its date.
	dated := time.Now().Add(9 * time.Second)
	usedToken, spareToken := tenant.TokenAt(t, used, dated), tenant.TokenAt(t, spare, dated)
	expires := dated.Add(30 * time.Second)

	first := connection(usedToken, used)
	require.Equal(t, http.StatusOK, first.Status, "the first use: %s", first.Body)
	requireRefused(t, connection(usedToken, used), http.StatusUnauthorized, "", "the second use")

	p.Stop()
	p.Start()
	// A complete pass: providerd knows where its leases are again.
	p.AwaitSweeps(t, 1, SweepComplete)
	require.True(t, time.Now().Before(expires.Add(-5*time.Second)), "the restart took so long that the tokens have expired; the test cannot tell")
	requireRefused(t, connection(usedToken, used), http.StatusUnauthorized, "", "the used token after the restart")
	fresh := connection(spareToken, spare)
	require.Equal(t, http.StatusOK, fresh.Status, "a token of the same age that was never used: %s", fresh.Body)
	requireRefused(t, connection(spareToken, spare), http.StatusUnauthorized, "", "the second use of the other token")
}
