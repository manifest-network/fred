//go:build integration

package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/manifest-network/fred/internal/backendidentity"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testutil"
)

const (
	// BackendBinDirEnv names the directory holding the docker-backend binary
	// under test. `make test-e2e` builds it and sets this.
	BackendBinDirEnv = "FRED_E2E_BACKEND_BIN_DIR"
	// LogDirEnv, when set, receives each process's log so CI can upload it.
	LogDirEnv = "FRED_E2E_LOG_DIR"
	// CoverDirEnv, when set, receives the coverage data of each process built
	// with `go build -cover`, one subdirectory per process.
	CoverDirEnv = "FRED_E2E_COVER_DIR"

	// StatelessSKU has no disk: its leases own no volume.
	StatelessSKU = "5f0c2f0e-6a3b-4e52-9c58-1a2b3c4d5e01"
	// StatefulSKU has disk: an image VOLUME becomes a quota-capped XFS
	// directory that outlives the lease as retained data.
	StatefulSKU = "5f0c2f0e-6a3b-4e52-9c58-1a2b3c4d5e02"

	// backendNamePrefix marks every Docker resource this suite creates: the
	// backend labels its containers and networks with its configured name.
	backendNamePrefix = "e2e-"

	// The account docker-backend runs as, standing in for the service account
	// of the production unit, and the ambient capabilities that unit grants
	// (DEPLOYMENT.md).
	backendUID     = 65534
	backendGID     = 65534
	productionCaps = "cap_chown,cap_dac_override,cap_fowner,cap_sys_admin"

	clientCertName = "providerd"
	dockerSocket   = "/var/run/docker.sock"

	// startTimeout covers recovery before the listener binds; the backend's
	// own startup budget is 30 seconds.
	startTimeout = 90 * time.Second
	// shutdownGrace is how long a SIGTERM'd backend gets before SIGKILL. Its
	// own shutdown budget is 75 seconds.
	shutdownGrace = 100 * time.Second
)

// Backend is one docker-backend process and everything it needs: an XFS
// loopback, a storage identity, a listen port, a throwaway CA and a sink for
// its callbacks. Each test starts its own, so tests share only the Docker
// daemon.
type Backend struct {
	// Client talks to this backend.
	Client *Client
	// Name is the configured backend name. The backend's containers and
	// networks carry it in the fred.backend_name label.
	Name string
	// VolumePath is the backend's volume_data_path on the XFS loopback.
	VolumePath string

	t         *testing.T
	binary    string
	config    string
	logPath   string
	coverDir  string
	dockerGID uint32
	healthURL string

	// stop and exited are nil while the process is not running.
	stop   context.CancelFunc
	exited chan error
}

// StartBackend seals a fresh storage identity and starts docker-backend on it.
// It skips the test when the host cannot run the suite. Cleanup stops the
// process, removes its Docker resources and unmounts its storage.
func StartBackend(t *testing.T) *Backend {
	t.Helper()
	if os.Getuid() != 0 {
		t.Skip("e2e needs root: it mounts an XFS loopback and drops docker-backend to a non-root account")
	}
	binDir := os.Getenv(BackendBinDirEnv)
	if binDir == "" {
		t.Skipf("%s is not set; run `make test-e2e`", BackendBinDirEnv)
	}
	if _, err := exec.LookPath("capsh"); err != nil {
		t.Skip("capsh (libcap2-bin) not found:", err)
	}
	docker := dockerClient(t)
	socket, err := os.Stat(dockerSocket)
	require.NoError(t, err)

	b := &Backend{
		Name:      backendNamePrefix + randomHex(t, 4),
		t:         t,
		binary:    filepath.Join(binDir, "docker-backend"),
		dockerGID: socket.Sys().(*syscall.Stat_t).Gid,
	}
	_, err = os.Stat(b.binary)
	require.NoError(t, err, "%s must hold a docker-backend binary", BackendBinDirEnv)

	// Registered first so it runs last: the mount outlives the process and
	// the containers that bind into it.
	mount := testutil.SetupXFSLoopback(t)
	b.VolumePath = filepath.Join(mount, "volumes")
	state := t.TempDir()
	b.config = filepath.Join(state, "docker-backend.yaml")
	b.logPath = filepath.Join(dirFromEnv(t, LogDirEnv), b.Name+".log")
	b.coverDir = filepath.Join(dirFromEnv(t, CoverDirEnv), b.Name)
	for _, dir := range []string{b.VolumePath, b.coverDir} {
		require.NoError(t, os.MkdirAll(dir, 0o700))
	}
	// The backend creates its stores, volumes and coverage data as its own
	// account.
	for _, dir := range []string{state, b.VolumePath, b.coverDir} {
		require.NoError(t, os.Chown(dir, backendUID, backendGID))
	}

	ca := newCertAuthority(t)
	serverCert, serverKey := ca.issue(t, state, "server", "docker-backend", true)
	listenAddr := freeLoopbackAddr(t)
	b.healthURL = "https://" + listenAddr + "/health"

	// One key signs requests in both directions, as callback_secret does in
	// production.
	secret := randomHex(t, 32)
	sink := httptest.NewServer(acknowledgeCallbacks(secret))
	t.Cleanup(sink.Close)
	callbacks, err := placement.NewCallbackRouteFactory(sink.URL)
	require.NoError(t, err)
	b.Client = &Client{
		http:      &http.Client{Timeout: time.Minute, Transport: &http.Transport{TLSClientConfig: ca.clientConfig(t)}},
		baseURL:   "https://" + listenAddr,
		secret:    secret,
		callbacks: callbacks,
	}

	config := map[string]any{
		"name":                     b.Name,
		"listen_addr":              listenAddr,
		"tls_cert_file":            serverCert,
		"tls_key_file":             serverKey,
		"tls_client_ca_file":       ca.certFile,
		"tls_client_allowed_names": []string{clientCertName},
		"docker_host":              "unix://" + dockerSocket,
		"total_cpu_cores":          2.0,
		"total_memory_mb":          2048,
		"total_disk_mb":            256,
		// The loopback is far smaller than the production free-space floor.
		"image_disk_min_free_mb": 1,
		"sku_mapping": map[string]string{
			StatelessSKU: "e2e-stateless",
			StatefulSKU:  "e2e-stateful",
		},
		"sku_profiles": map[string]any{
			"e2e-stateless": map[string]any{"cpu_cores": 0.25, "memory_mb": 128, "disk_mb": 0},
			"e2e-stateful":  map[string]any{"cpu_cores": 0.25, "memory_mb": 128, "disk_mb": 64},
		},
		"allowed_registries":  []string{"docker.io"},
		"callback_secret":     secret,
		"callback_db_path":    filepath.Join(state, "callbacks.db"),
		"diagnostics_db_path": filepath.Join(state, "diagnostics.db"),
		"releases_db_path":    filepath.Join(state, "releases.db"),
		"retention_db_path":   filepath.Join(state, "retention.db"),
		"retain_on_close":     true,
		"host_address":        "127.0.0.1",
		"host_bind_ip":        "127.0.0.1",
		"volume_data_path":    b.VolumePath,
		"volume_mount_path":   mount,
		// Test workloads run `sleep`, which ignores SIGTERM: do not wait out
		// the production stop grace or the production startup watch.
		"container_stop_timeout":  "1s",
		"startup_verify_duration": "1s",
		"provision_timeout":       "2m",
	}
	// A containerd image store keeps its content outside Docker's data root,
	// and the backend must be told where (see integration.yml).
	if path := os.Getenv("FRED_TEST_IMAGE_DATA_PATH"); path != "" {
		config["image_data_path"] = path
	}
	encoded, err := yaml.Marshal(config)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(b.config, encoded, 0o600))
	require.NoError(t, os.Chown(b.config, backendUID, backendGID))

	t.Cleanup(func() {
		b.Stop()
		removeDockerResources(t, docker, func(name string) bool { return name == b.Name })
		if t.Failed() {
			t.Logf("docker-backend log (%s):\n%s", b.logPath, tail(b.logPath))
		}
	})

	// Normal startup only verifies a storage identity: a new backend is sealed
	// first, as an operator would.
	seal := b.command(t.Context(), "-config", b.config, "-initialize-storage-identity", "new")
	out, err := seal.CombinedOutput()
	require.NoError(t, err, "seal storage identity: %s", out)

	b.Start()
	return b
}

// Start runs docker-backend on the fixture's storage and waits until it
// serves. StartBackend calls it; a test calls it again after Stop.
func (b *Backend) Start() {
	b.t.Helper()
	require.Nil(b.t, b.stop, "docker-backend is already running")

	// World-readable: CI uploads the log as the unprivileged runner account.
	logFile, err := os.OpenFile(b.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644) //nolint:gosec // G302, G304: a test log at a fixture-built path
	require.NoError(b.t, err)
	defer func() { _ = logFile.Close() }()

	// Not the test's context: that one is canceled before cleanup runs, and
	// the process must outlive the test body until Stop.
	ctx, cancel := context.WithCancel(context.Background())
	cmd := b.command(ctx, "-config", b.config)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = shutdownGrace
	if err := cmd.Start(); err != nil {
		cancel()
		require.NoError(b.t, err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	b.stop, b.exited = cancel, exited

	deadline := time.After(startTimeout)
	poll := time.NewTicker(200 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case err := <-exited:
			b.stop, b.exited = nil, nil
			cancel()
			b.t.Fatalf("docker-backend exited during startup: %v\n%s", err, tail(b.logPath))
		case <-deadline:
			b.t.Fatalf("docker-backend did not serve %s within %s\n%s", b.healthURL, startTimeout, tail(b.logPath))
		case <-poll.C:
			if id, ok := b.servingIdentity(); ok {
				b.Client.storageID = id
				return
			}
		}
	}
}

// Stop sends SIGTERM and waits for docker-backend to exit. Containers keep
// running, as they do across a production restart. A no-op when the process
// is not running.
func (b *Backend) Stop() {
	b.t.Helper()
	if b.stop == nil {
		return
	}
	b.stop()
	err := <-b.exited
	b.stop, b.exited = nil, nil
	// exec reports a zero exit after the cancel signal as the context's
	// error. Anything else is the process's own failure.
	assert.ErrorIs(b.t, err, context.Canceled, "docker-backend did not shut down cleanly")
}

// command runs the binary as the backend account with the production
// capabilities as its ambient set. capsh narrows the capabilities while still
// root, changes identity keeping the permitted set, raises the ambient set,
// and execs bash, which execs the binary: the PID never changes, so a signal
// to the command reaches docker-backend.
func (b *Backend) command(ctx context.Context, args ...string) *exec.Cmd {
	argv := []string{
		"--caps=" + productionCaps + "+eip cap_setpcap,cap_setuid,cap_setgid+ep",
		"--keep=1",
		fmt.Sprintf("--gid=%d", backendGID),
		// The Docker socket's group, as SupplementaryGroups=docker grants.
		fmt.Sprintf("--groups=%d", b.dockerGID),
		fmt.Sprintf("--uid=%d", backendUID),
		"--addamb=" + productionCaps,
		"--", "-c", `exec "$0" "$@"`, b.binary,
	}
	cmd := exec.CommandContext(ctx, "capsh", append(argv, args...)...) //nolint:gosec // G204: the binary under test and fixture-built arguments
	// Nothing inherited may override the config file (DOCKER_BACKEND_*,
	// DOCKER_HOST). The backend finds xfs_quota through PATH.
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "GOCOVERDIR=" + b.coverDir}
	return cmd
}

// servingIdentity reports the storage identity a healthy backend answers with.
func (b *Backend) servingIdentity() (backendidentity.ID, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.healthURL, nil)
	if err != nil {
		return backendidentity.ID{}, false
	}
	resp, err := b.Client.http.Do(req)
	if err != nil {
		return backendidentity.ID{}, false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return backendidentity.ID{}, false
	}
	id, err := backendidentity.Parse(resp.Header.Get(backendidentity.ResponseHeader))
	return id, err == nil
}

// acknowledgeCallbacks is the callback sink: it stands where providerd's
// callback endpoint does. A backend holds a lease's next operation until the
// previous completion is acknowledged, so every authentic callback gets a 200.
// Tests observe leases through the client, not through callbacks.
func acknowledgeCallbacks(secret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, "unreadable callback", http.StatusBadRequest)
			return
		}
		signature := r.Header.Get(hmacauth.SignatureHeader)
		if err := hmacauth.VerifyRequest(secret, r, body, signature, 5*time.Minute); err != nil {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// freeLoopbackAddr picks a free loopback port for a process to listen on. The
// harness chooses the port because docker-backend cannot report one it chose:
// it probes its listen address, releases it, binds again after recovery, and
// logs only the configured value. The port is free when picked, not reserved;
// a backend that loses it exits at startup and fails the test with its log.
func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}

// dirFromEnv returns the directory an environment variable names, created if
// missing, or a per-test temporary directory when the variable is unset.
func dirFromEnv(t *testing.T, name string) string {
	t.Helper()
	dir := os.Getenv(name)
	if dir == "" {
		return t.TempDir()
	}
	require.NoError(t, os.MkdirAll(dir, 0o755)) //nolint:gosec // G301: CI reads these as the unprivileged runner account
	return dir
}

func randomHex(t *testing.T, bytes int) string {
	t.Helper()
	raw := make([]byte, bytes)
	_, err := rand.Read(raw)
	require.NoError(t, err)
	return hex.EncodeToString(raw)
}

// tail returns the end of a log file, enough to explain a failure without
// flooding the test output.
func tail(path string) string {
	const limit = 64 << 10
	data, err := os.ReadFile(path) //nolint:gosec // G304: a log file the fixture created
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "(no log written)"
		}
		return fmt.Sprintf("(log unreadable: %v)", err)
	}
	if len(data) > limit {
		data = data[len(data)-limit:]
	}
	return strings.TrimRight(string(data), "\n")
}
