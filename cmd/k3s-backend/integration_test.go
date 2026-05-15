//go:build integration

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/hmacauth"
)

// TestIntegration_BinaryProvisionsAndCallsBack exec's the real k3s-backend
// binary, POSTs a signed /provision against it, and asserts the signed
// failure callback per ENG-133 AC3. End-to-end coverage of the entry-point
// + YAML config + HMAC middleware + stub provisioner + callback sender +
// graceful shutdown chain — proves the operator-facing surface works as a
// single unit.
//
// /health is intentionally not exercised here: T7c covers it via fake K8s
// API at the unit layer, and engineer's T6 live smoke confirmed it against
// the user's real K3s. Combining both into the integration test adds setup
// complexity without new coverage of the binary's behavior.
func TestIntegration_BinaryProvisionsAndCallsBack(t *testing.T) {
	const secret = "integration-test-secret-32-chars!!"

	binPath := buildBinary(t)
	port := freePort(t)
	kubeconfigPath := writeBogusKubeconfig(t)
	fred, callbacks := startIntegrationFred(t, secret)
	cfgPath := writeBackendConfig(t, port, secret, kubeconfigPath)

	startBinary(t, binPath, cfgPath)
	waitForListening(t, port)

	// POST /provision with HMAC-signed body.
	leaseUUID := "00000000-0000-0000-0000-deadbeef0001"
	provisionReq := backend.ProvisionRequest{
		LeaseUUID:    leaseUUID,
		Tenant:       "manifest1test",
		ProviderUUID: "prov-1",
		Items: []backend.LeaseItem{
			{SKU: "00000000-0000-0000-0000-000000000001", Quantity: 1},
		},
		CallbackURL: fred.URL + "/callbacks/provision",
	}
	bodyBytes, err := json.Marshal(provisionReq)
	require.NoError(t, err)

	httpReq, err := http.NewRequest(
		"POST",
		fmt.Sprintf("http://127.0.0.1:%d/provision", port),
		bytes.NewReader(bodyBytes),
	)
	require.NoError(t, err)
	httpReq.Header.Set(hmacauth.SignatureHeader, hmacauth.Sign(secret, httpReq.Method, httpReq.URL.RequestURI(), bodyBytes))
	httpReq.Header.Set("Content-Type", "application/json")

	// Bounded timeout so a handler deadlock or stalled connection fails the
	// test in seconds, not minutes — the default go-test 10-min timeout is
	// far too coarse to detect /provision unresponsiveness here.
	provisionClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := provisionClient.Do(httpReq)
	require.NoError(t, err)
	respBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusAccepted, resp.StatusCode,
		"POST /provision must return 202; got %d, body=%q", resp.StatusCode, string(respBody))

	// Await the signed failure callback. Engineer's T6 smoke observed
	// ~1.5s; 5s gives 3× headroom for CI variance.
	select {
	case p := <-callbacks:
		assert.Equal(t, leaseUUID, p.LeaseUUID)
		assert.Equal(t, backend.CallbackStatusFailed, p.Status)
		assert.Equal(t, "not implemented", p.Error)
		assert.Equal(t, "k3s", p.Backend)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for binary's failure callback")
	}
	// SIGTERM + wait is handled by t.Cleanup registered in startBinary.
}

// --- Helpers --------------------------------------------------------------

// buildBinary compiles ./cmd/k3s-backend into t.TempDir() and returns the
// absolute path. Skips the test if the go toolchain is unavailable.
func buildBinary(t *testing.T) string {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("go toolchain not available: %v", err)
	}
	binPath := filepath.Join(t.TempDir(), "k3s-backend")
	cmd := exec.Command(goBin, "build", "-o", binPath, ".")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build failed: %v\n%s", err, out)
	}
	return binPath
}

// freePort opens a listener on :0, captures the OS-assigned port, closes
// the listener, and returns the port for the binary to re-bind. Race
// window between close and the binary's bind is sub-millisecond.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

// writeBogusKubeconfig writes a syntactically valid kubeconfig pointing at
// an unreachable server. The integration test never invokes /health, so
// the kubeconfig's unreachability is fine; it satisfies any path-existence
// expectation without requiring a real cluster.
func writeBogusKubeconfig(t *testing.T) string {
	t.Helper()
	const kubeconfig = `apiVersion: v1
kind: Config
current-context: ctx
clusters:
- name: cluster
  cluster:
    server: https://127.0.0.1:1
    insecure-skip-tls-verify: true
contexts:
- name: ctx
  context:
    cluster: cluster
    user: user
users:
- name: user
  user: {}
`
	path := filepath.Join(t.TempDir(), "kubeconfig.yaml")
	require.NoError(t, os.WriteFile(path, []byte(kubeconfig), 0o600))
	return path
}

// startIntegrationFred returns an httptest.Server that plays Fred for the
// integration test: HMAC-verifies inbound, unmarshals the body to a
// backend.CallbackPayload, and pushes it to a buffered channel. Closes
// via t.Cleanup. Buffer size 4 prevents handler block if the binary
// retries — though zero-backoff isn't configurable from the binary side,
// the buffer keeps the handler non-blocking under any sender behavior.
func startIntegrationFred(t *testing.T, secret string) (*httptest.Server, <-chan backend.CallbackPayload) {
	t.Helper()
	ch := make(chan backend.CallbackPayload, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("fake Fred: read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		sig := r.Header.Get(hmacauth.SignatureHeader)
		if err := hmacauth.Verify(secret, r.Method, r.URL.RequestURI(), body, sig, 5*time.Minute); err != nil {
			t.Errorf("fake Fred: HMAC verify failed: %v (sig=%q)", err, sig)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var p backend.CallbackPayload
		if err := json.Unmarshal(body, &p); err != nil {
			t.Errorf("fake Fred: unmarshal payload: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		ch <- p
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server, ch
}

// writeBackendConfig writes the k3s-backend YAML to t.TempDir() with the
// supplied port + secret + kubeconfig path + tempdir bbolt paths.
func writeBackendConfig(t *testing.T, port int, secret, kubeconfigPath string) string {
	t.Helper()
	dir := t.TempDir()
	// SKUMapping intentionally omitted: yaml.v3 merges maps with
	// DefaultConfig, so providing one mapping would trigger the reverse
	// unreachability check against the 4 default profiles. The stub
	// provisioner doesn't dereference SKUs (no GetSKUProfile call path),
	// so leaving SKUMapping empty gates off the check without affecting
	// the Provision flow under test.
	yaml := fmt.Sprintf(`name: k3s
listen_addr: ":%d"
kubeconfig_path: %q
total_cpu_cores: 8.0
total_memory_mb: 16384
total_disk_mb: 102400
allowed_registries: ["docker.io"]
callback_secret: %q
host_address: "127.0.0.1"
callback_db_path: %q
diagnostics_db_path: %q
releases_db_path: %q
reconcile_interval: 5m
`,
		port,
		kubeconfigPath,
		secret,
		filepath.Join(dir, "callbacks.db"),
		filepath.Join(dir, "diagnostics.db"),
		filepath.Join(dir, "releases.db"),
	)
	path := filepath.Join(dir, "k3s.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o600))
	return path
}

// startBinary launches the binary as a subprocess with stdout/stderr piped
// to t.Logf. Registers a t.Cleanup that SIGTERMs the process and waits up
// to 10s for graceful exit (escalating to Kill on timeout). Returns the
// command handle for callers that want to inspect process state.
func startBinary(t *testing.T, binPath, cfgPath string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(binPath, "--config", cfgPath)

	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	stderr, err := cmd.StderrPipe()
	require.NoError(t, err)

	require.NoError(t, cmd.Start())

	// Pipe binary output into the test's log so failure modes surface.
	// relayWg tracks both relay goroutines so the cleanup function can join
	// them before returning. Without this, the goroutines' final
	// scanner.Err() + t.Logf calls race against the test being marked done
	// — Go's testing runtime panics on t.Logf after test.done is set.
	var relayWg sync.WaitGroup
	relayWg.Add(2)
	go func() {
		defer relayWg.Done()
		relayOutput(t, "stdout", stdout)
	}()
	go func() {
		defer relayWg.Done()
		relayOutput(t, "stderr", stderr)
	}()

	t.Cleanup(func() {
		if cmd.Process == nil {
			return
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Log("binary did not exit within 10s of SIGTERM; sending SIGKILL")
			_ = cmd.Process.Kill()
			<-done
		}
		// Join relay goroutines AFTER cmd.Wait so the pipes have closed and
		// scanner.Scan returns false; both goroutines will then exit cleanly
		// and their final t.Logf calls land before this cleanup returns.
		relayWg.Wait()
	})
	return cmd
}

// relayOutput forwards a binary stream into the test log line-by-line.
// scanner.Err() is logged after the loop so a pipe error (closed stream,
// I/O failure) or a line exceeding bufio.MaxScanTokenSize (64 KiB) is
// visible to anyone debugging a failed test rather than silently
// truncating the captured output.
func relayOutput(t *testing.T, label string, r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		t.Logf("binary %s: %s", label, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		t.Logf("binary %s: scan error: %v", label, err)
	}
}

// waitForListening polls /metrics (unauth, no backend dependency — promhttp
// serves standalone) at 100ms intervals until 200 or timeout. /metrics is
// the right probe because it doesn't require a working K8s client; the
// binary's listener accepting connections is the only invariant we need.
func waitForListening(t *testing.T, port int) {
	t.Helper()
	// Per-attempt timeout (500ms) is longer than the 100ms poll cadence
	// but well under the overall 3s deadline — a stuck connection on the
	// loopback bind window can't consume the whole 3s budget in a single
	// try, so the polling loop keeps a fast cancel cadence and still has
	// roughly six attempts before the deadline.
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(3 * time.Second)
	url := fmt.Sprintf("http://127.0.0.1:%d/metrics", port)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url) // nolint:gosec // localhost test request
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for binary to listen on port %d", port)
}
