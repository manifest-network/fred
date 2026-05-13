package k3s

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newBackendForHealth constructs a Backend whose KubeconfigPath points at the
// caller-supplied location and whose three bbolt stores live under t.TempDir().
// b.Stop is registered in t.Cleanup so the stores close (and their cleanup
// goroutines exit) on test teardown. Health is not invoked here — callers
// drive it directly.
func newBackendForHealth(t *testing.T, kubeconfigPath string) *Backend {
	t.Helper()
	cfg := validConfig()
	dir := t.TempDir()
	cfg.CallbackDBPath = filepath.Join(dir, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(dir, "diagnostics.db")
	cfg.ReleasesDBPath = filepath.Join(dir, "releases.db")
	cfg.KubeconfigPath = kubeconfigPath

	b, err := New(cfg, slog.Default())
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Stop() })
	return b
}

// writeFakeKubeconfig drops a minimal kubeconfig YAML into t.TempDir() pointing
// at serverURL with insecure-skip-tls-verify: true so the test's httptest TLS
// server (self-signed cert) is acceptable. Returns the absolute file path.
//
// The YAML shape is the minimum client-go's BuildConfigFromFlags accepts —
// any further trimming triggers parser errors.
func writeFakeKubeconfig(t *testing.T, serverURL string) string {
	t.Helper()
	const tmpl = `apiVersion: v1
kind: Config
current-context: ctx
clusters:
- name: cluster
  cluster:
    server: %s
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
	require.NoError(t, os.WriteFile(path, []byte(fmt.Sprintf(tmpl, serverURL)), 0o600))
	return path
}

// fakeAPIServer starts an httptest TLS server with the caller's handler and
// auto-closes it on test teardown. Discovery().ServerVersion() hits GET
// /version on the API server — a public, unauthenticated endpoint — so the
// fake doesn't need TLS-cert auth or kube-style authn.
func fakeAPIServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	return server
}

func TestHealth_Success(t *testing.T) {
	server := fakeAPIServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/version" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintln(w, `{"major":"1","minor":"36","gitVersion":"v1.36.0+k3s1","platform":"linux/amd64"}`)
			return
		}
		http.NotFound(w, r)
	})

	kubeconfig := writeFakeKubeconfig(t, server.URL)
	b := newBackendForHealth(t, kubeconfig)

	require.NoError(t, b.Health(context.Background()))
}

func TestHealth_KubeconfigNotFound(t *testing.T) {
	b := newBackendForHealth(t, "/nonexistent/path/kubeconfig.yaml")

	err := b.Health(context.Background())
	require.Error(t, err)
	assert.ErrorContains(t, err, "k3s API unreachable")
	// The resolver wraps the underlying clientcmd error with this prefix.
	assert.ErrorContains(t, err, "loading kubeconfig")
}

func TestHealth_DiscoveryServerError(t *testing.T) {
	server := fakeAPIServer(t, func(w http.ResponseWriter, r *http.Request) {
		// Build succeeds (the kubeconfig is structurally valid); only the
		// Discovery.ServerVersion() HTTP request fails. This exercises
		// health.go's terminal "k3s API unreachable: %w" wrap at line 54
		// (the post-build path), distinct from the build-time wrap at line 33.
		http.Error(w, "internal server error", http.StatusInternalServerError)
	})

	kubeconfig := writeFakeKubeconfig(t, server.URL)
	b := newBackendForHealth(t, kubeconfig)

	err := b.Health(context.Background())
	require.Error(t, err)
	assert.ErrorContains(t, err, "k3s API unreachable")
}

func TestHealth_LazyBuild(t *testing.T) {
	// Verifies the sync.Once + atomic.Pointer cache:
	//   - b.kube is nil prior to any Health call (no eager build in New).
	//   - First Health stores a *kubernetes.Clientset.
	//   - Second Health re-uses the same pointer (Do() body runs at most once).
	server := fakeAPIServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintln(w, `{"major":"1","minor":"36","gitVersion":"v1.36.0+k3s1","platform":"linux/amd64"}`)
	})

	kubeconfig := writeFakeKubeconfig(t, server.URL)
	b := newBackendForHealth(t, kubeconfig)

	assert.Nil(t, b.kube.Load(), "client must not be built until first Health call")

	require.NoError(t, b.Health(context.Background()))
	first := b.kube.Load()
	require.NotNil(t, first, "client must be built after first Health call")

	require.NoError(t, b.Health(context.Background()))
	assert.Same(t, first, b.kube.Load(),
		"second Health call must reuse the same client pointer (sync.Once held)")
}

func TestHealth_CachedFailure(t *testing.T) {
	// Verifies the cached-build-failure path:
	//   - First Health captures the build error in b.kubeBuildErr.
	//   - Subsequent Health calls return that same wrapped error byte-for-byte
	//     (proves Do() didn't rerun and a fresh wrap wasn't constructed).
	//   - b.kube stays nil for the process lifetime.
	b := newBackendForHealth(t, "/nonexistent/path/kubeconfig.yaml")

	err1 := b.Health(context.Background())
	err2 := b.Health(context.Background())

	require.Error(t, err1)
	require.Error(t, err2)
	assert.EqualError(t, err1, err2.Error(),
		"cached failure must re-serve the identical wrapped error")
	assert.Nil(t, b.kube.Load(), "no client must ever be stored on the failure path")
}
