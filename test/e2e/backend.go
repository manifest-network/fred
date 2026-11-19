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

	"github.com/docker/docker/client"
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

	// The ambient capabilities the production unit grants docker-backend
	// (DEPLOYMENT.md).
	productionCaps = "cap_chown,cap_dac_override,cap_fowner,cap_sys_admin"

	clientCertName = "providerd"
)

// SKUs are the SKU UUIDs a backend serves, one per profile.
type SKUs struct {
	// Stateless has no disk: its leases own no volume.
	Stateless string
	// Stateful has disk: an image VOLUME becomes a quota-capped XFS directory
	// that outlives the lease as retained data.
	Stateful string
	// Pinned, when set, is a second SKU without disk. A provider with several
	// backends gives each its own, so a test can put a lease on a chosen one.
	Pinned string
}

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
	// SKUs are the SKUs this backend serves.
	SKUs SKUs

	// What a providerd needs to reach this backend: its base URL, the CA that
	// signed its server certificate, a client certificate it accepts, and the
	// key that signs requests and callbacks. The service account can read the
	// files.
	URL            string
	CAFile         string
	ClientCertFile string
	ClientKeyFile  string
	HMACSecret     string

	t         *testing.T
	process   *Process
	binary    string
	config    string
	docker    *client.Client
	dockerGID uint32
	healthURL string
}

// StartBackend starts a backend on the first Docker daemon, serving
// StatelessSKU and StatefulSKU, for tests that drive the backend themselves.
func StartBackend(t *testing.T) *Backend {
	t.Helper()
	return StartBackendFor(t, DockerSockets()[0], StandInProviderUUID, SKUs{Stateless: StatelessSKU, Stateful: StatefulSKU})
}

// StartBackendFor seals a fresh storage identity and starts docker-backend
// on it, on the Docker daemon behind dockerSocket, serving the given SKUs.
// No other backend may use that daemon. Leases created through the backend's
// Client belong to the given provider. It skips the test when the host cannot
// run the suite. Cleanup stops the process, removes its Docker resources and
// unmounts its storage.
func StartBackendFor(t *testing.T, dockerSocket, providerUUID string, skus SKUs) *Backend {
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
	docker := dockerClient(t, dockerSocket)
	socket, err := os.Stat(dockerSocket)
	require.NoError(t, err)

	b := &Backend{
		Name:      backendNamePrefix + randomHex(t, 4),
		SKUs:      skus,
		t:         t,
		binary:    filepath.Join(binDir, "docker-backend"),
		docker:    docker,
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
	b.process = NewProcess(t, b.Name)
	require.NoError(t, os.MkdirAll(b.VolumePath, 0o700))
	// The backend creates its stores and volumes as its own account.
	GiveToServiceAccount(t, state, b.VolumePath)

	ca := newCertAuthority(t)
	serverCert, serverKey := ca.issue(t, state, "server", "docker-backend", true)
	b.CAFile = ca.certFile
	b.ClientCertFile, b.ClientKeyFile = ca.issue(t, state, "client", clientCertName, false)
	listenAddr := FreeLoopbackAddr(t)
	b.URL = "https://" + listenAddr
	b.healthURL = b.URL + "/health"

	// One key signs requests in both directions, as callback_secret does in
	// production.
	secret := randomHex(t, 32)
	b.HMACSecret = secret
	sink := httptest.NewServer(acknowledgeCallbacks(secret))
	t.Cleanup(sink.Close)
	callbacks, err := placement.NewCallbackRouteFactory(sink.URL)
	require.NoError(t, err)
	b.Client = &Client{
		http:         &http.Client{Timeout: time.Minute, Transport: &http.Transport{TLSClientConfig: ca.clientConfig(t)}},
		baseURL:      b.URL,
		secret:       secret,
		providerUUID: providerUUID,
		callbacks:    callbacks,
	}

	mapping := map[string]string{skus.Stateless: "e2e-stateless", skus.Stateful: "e2e-stateful"}
	if skus.Pinned != "" {
		mapping[skus.Pinned] = "e2e-stateless"
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
		"sku_mapping":            mapping,
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
	GiveToServiceAccount(t, b.config)

	t.Cleanup(func() {
		b.Stop()
		removeDockerResources(t, docker, func(name string) bool { return name == b.Name })
		b.process.LogOnFailure()
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
	b.process.Start(
		func(ctx context.Context) *exec.Cmd { return b.command(ctx, "-config", b.config) },
		func() bool {
			id, ok := b.servingIdentity()
			if ok {
				b.Client.storageID = id
			}
			return ok
		},
	)
}

// Stop sends SIGTERM and waits for docker-backend to exit. Containers keep
// running, as they do across a production restart. A no-op when the process
// is not running.
func (b *Backend) Stop() {
	b.t.Helper()
	b.process.Stop()
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
		fmt.Sprintf("--gid=%d", serviceGID),
		// The Docker socket's group, as SupplementaryGroups=docker grants.
		fmt.Sprintf("--groups=%d", b.dockerGID),
		fmt.Sprintf("--uid=%d", serviceUID),
		"--addamb=" + productionCaps,
		"--", "-c", `exec "$0" "$@"`, b.binary,
	}
	cmd := exec.CommandContext(ctx, "capsh", append(argv, args...)...) //nolint:gosec // G204: the binary under test and fixture-built arguments
	cmd.Env = b.process.Env()
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

// FreeLoopbackAddr picks a free loopback port for a process to listen on. The
// harness chooses the port because docker-backend cannot report one it chose:
// it probes its listen address, releases it, binds again after recovery, and
// logs only the configured value. The port is free when picked, not reserved;
// a process that loses it exits at startup and fails the test with its log.
func FreeLoopbackAddr(t *testing.T) string {
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
