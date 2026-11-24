//go:build integration

package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	networktypes "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/hmacauth"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

const (
	testCallbackSecret = "integration-test-secret-at-least-32-chars!"
	testProviderUUID   = "11111111-1111-4111-8111-111111111111"
)

func newIntegrationLeaseUUID() string {
	return uuid.NewString()
}

// integrationDockerConfig keeps deployment facts explicit. Docker's Info API
// cannot report an independently managed containerd root; the integration
// runner must declare its actual root instead of silently using DockerRootDir.
func integrationDockerConfig() Config {
	cfg := DefaultConfig()
	cfg.ImageDataPath = os.Getenv("FRED_TEST_IMAGE_DATA_PATH")
	// Loopback quota fixtures deliberately have small data filesystems. Keep
	// the real floor enabled at a scale compatible with those test substrates.
	cfg.ImageDiskMinFreeMB = 1
	return cfg
}

// testBackendWithRealDocker creates a Backend connected to the real Docker daemon.
// The backend is stopped and all test containers/networks are cleaned up via t.Cleanup.
func testBackendWithRealDocker(t *testing.T, cfgFn func(*Config)) *Backend {
	t.Helper()

	docker, err := NewDockerClient(t.Context(), "", "")
	require.NoError(t, err)

	ctx := context.Background()
	if err := docker.Ping(ctx); err != nil {
		t.Skip("Docker not available:", err)
	}

	cfg := integrationDockerConfig()
	cfg.SKUProfiles = defaultTestSKUProfiles()
	cfg.Name = fmt.Sprintf("test-%s-%d", t.Name(), time.Now().UnixNano())
	cfg.CallbackSecret = testCallbackSecret
	cfg.HostAddress = "127.0.0.1"
	cfg.StartupVerifyDuration = 1 * time.Second
	cfg.ReconcileInterval = 1 * time.Hour // disable during tests
	cfg.ProvisionTimeout = 2 * time.Minute
	// Integration tests use `sleep` as PID 1, which does not install a
	// SIGTERM handler. Docker's default 30s grace period is pure wasted
	// wall time per container — multiply that across 50+ integration
	// tests and we blow the -timeout 15m budget. Shorten to 1s so Docker
	// goes straight to SIGKILL; production default (30s in DefaultConfig)
	// is unchanged.
	cfg.ContainerStopTimeout = 1 * time.Second
	// Isolate DB stores per test to avoid replaying stale callbacks from previous runs.
	tmpDir := t.TempDir()
	cfg.CallbackDBPath = filepath.Join(tmpDir, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(tmpDir, "diagnostics.db")
	cfg.ReleasesDBPath = filepath.Join(tmpDir, "releases.db")
	cfg.RetentionDBPath = filepath.Join(tmpDir, "retention.db")

	if cfgFn != nil {
		cfgFn(&cfg)
	}

	// If the test didn't set VolumeDataPath, zero out DiskMB on all profiles
	// so config validation doesn't require a volume filesystem.
	if cfg.VolumeDataPath == "" {
		for name, p := range cfg.SKUProfiles {
			p.DiskMB = 0
			cfg.SKUProfiles[name] = p
		}
	}

	logger := slog.Default()
	initializeFreshIntegrationStorageIdentity(t, ctx, cfg, logger)
	b, err := New(cfg, logger)
	require.NoError(t, err)

	err = b.Start(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = b.Stop()
		cleanupTestContainers(t, docker, cfg.Name)
		cleanupTestNetworks(t, docker, cfg.Name)
	})

	return b
}

// initializeFreshIntegrationStorageIdentity makes the test's fresh-substrate
// claim explicit. Production startup is deliberately verify-only, so an
// integration fixture backed by a new temp directory must cross the same
// one-shot initialization boundary as an operator provisioning a new backend.
func initializeFreshIntegrationStorageIdentity(
	t *testing.T,
	ctx context.Context,
	cfg Config,
	logger *slog.Logger,
) {
	t.Helper()
	_, err := InitializeStorageIdentityForConfig(
		ctx, cfg, logger, StorageIdentityInitializeNew,
	)
	require.NoError(t, err)
}

// cleanupTestContainers removes all containers managed by the test backend.
func cleanupTestContainers(t *testing.T, docker *DockerClient, backendName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	f := filters.NewArgs(
		filters.Arg("label", LabelManaged+"=true"),
	)
	if backendName != "" {
		f.Add("label", LabelBackendName+"="+backendName)
	}
	containers, err := docker.client.ContainerList(ctx, container.ListOptions{
		All:     true,
		Filters: f,
	})
	if err != nil {
		t.Logf("cleanup: failed to list containers: %v", err)
		return
	}

	for _, c := range containers {
		if err := docker.client.ContainerRemove(ctx, c.ID, container.RemoveOptions{Force: true}); err != nil {
			if !client.IsErrNotFound(err) {
				t.Logf("cleanup: failed to remove container %s: %v", c.ID[:12], err)
			}
		}
	}
}

// cleanupTestNetworks removes all networks managed by Fred.
func cleanupTestNetworks(t *testing.T, docker *DockerClient, backendName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	f := filters.NewArgs(
		filters.Arg("label", LabelManaged+"=true"),
	)
	if backendName != "" {
		f.Add("label", LabelBackendName+"="+backendName)
	}
	networks, err := docker.client.NetworkList(ctx, networktypes.ListOptions{
		Filters: f,
	})
	if err != nil {
		t.Logf("cleanup: failed to list networks: %v", err)
		return
	}

	for _, n := range networks {
		if err := docker.client.NetworkRemove(ctx, n.ID); err != nil {
			t.Logf("cleanup: failed to remove network %s: %v", n.Name, err)
		}
	}
}

// integrationCallbackDelivery keeps a callback's decoded observation coupled
// to the exact authenticated wire identity that delivered it. Embedding the
// payload preserves ergonomic read-only assertions without allowing tests to
// reconstruct a different route or signature for settlement.
type integrationCallbackDelivery struct {
	backend.CallbackPayload
	method     string
	requestURI string
	body       []byte
	signature  string
}

func startProjectedCallbackServer[T any](
	t *testing.T,
	project func(integrationCallbackDelivery) T,
) (*httptest.Server, <-chan T) {
	t.Helper()
	ch := make(chan T, 10)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Logf("callback: failed to read body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Verify HMAC signature
		sig := r.Header.Get(hmacauth.SignatureHeader)
		if sig == "" {
			t.Logf("callback: missing signature header")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if err := hmacauth.Verify(testCallbackSecret, r.Method, r.URL.RequestURI(), body, sig, 5*time.Minute); err != nil {
			t.Logf("callback: invalid signature: %v", err)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		var payload backend.CallbackPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Logf("callback: failed to unmarshal: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		ch <- project(integrationCallbackDelivery{
			CallbackPayload: payload,
			method:          r.Method,
			requestURI:      r.URL.RequestURI(),
			body:            append([]byte(nil), body...),
			signature:       sig,
		})
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server, ch
}

// startCallbackServer creates an httptest server that receives HMAC-signed
// callbacks and exposes their decoded observation.
func startCallbackServer(t *testing.T) (*httptest.Server, <-chan backend.CallbackPayload) {
	t.Helper()
	return startProjectedCallbackServer(t, func(delivery integrationCallbackDelivery) backend.CallbackPayload {
		return delivery.CallbackPayload
	})
}

// startCallbackDeliveryServer additionally preserves the actual signed wire
// request for tests that apply the callback to providerd's typed coordinator.
func startCallbackDeliveryServer(t *testing.T) (*httptest.Server, <-chan integrationCallbackDelivery) {
	t.Helper()
	return startProjectedCallbackServer(t, func(delivery integrationCallbackDelivery) integrationCallbackDelivery {
		return delivery
	})
}

// integrationCallbackAuthority gives a provision request the same exact,
// typed callback pair that providerd supplies in production. Maintenance
// requests carry the lifecycle URL so they retain the provisioned release's
// immutable generation while allowing the callback endpoint itself to move.
type integrationCallbackAuthority struct {
	operationURL string
	lifecycleURL string
}

func newIntegrationCallbackAuthority(
	t *testing.T,
	callbackBaseURL string,
) integrationCallbackAuthority {
	t.Helper()
	routes, err := placement.NewCallbackRouteFactory(callbackBaseURL)
	require.NoError(t, err)
	operationID, err := operation.ParseID(uuid.NewString())
	require.NoError(t, err)
	pair, err := routes.ForOperation(operationID)
	require.NoError(t, err)
	return integrationCallbackAuthority{
		operationURL: pair.OperationURL(),
		lifecycleURL: pair.LifecycleURL(),
	}
}

// waitForCallback reads from the callback channel until it finds a callback
// matching the expected lease UUID. Callbacks for other leases (e.g., death
// events from the container event loop) are logged and skipped.
func waitForCallback(t *testing.T, ch <-chan backend.CallbackPayload, leaseUUID string, timeout time.Duration) backend.CallbackPayload {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case cb := <-ch:
			if cb.LeaseUUID == leaseUUID {
				return cb
			}
			t.Logf("skipping callback for lease %s (status=%s, waiting for %s)", cb.LeaseUUID, cb.Status, leaseUUID)
		case <-deadline:
			t.Fatalf("timeout waiting for callback for lease %s", leaseUUID)
		}
	}
}

// --- Helpers for edge-case tests ---

// waitForContainerExited polls until the container reaches "exited" state.
// ContainerKill is asynchronous — the SIGKILL is sent but the container needs
// time to transition to exited. This helper ensures the transition is complete.
func waitForContainerExited(t *testing.T, containerID string) {
	t.Helper()
	docker, err := NewDockerClient(t.Context(), "", "")
	require.NoError(t, err)
	defer func() { _ = docker.Close() }()

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		info, err := docker.InspectContainer(ctx, containerID)
		if err != nil {
			return false
		}
		return info.Status == "exited"
	}, 10*time.Second, 100*time.Millisecond, "container %s did not reach exited state", containerID)
}

// killContainer sends SIGKILL to a container, leaving it in "exited" state.
// The container remains visible to Docker (and recoverState) unlike ContainerRemove.
func killContainer(t *testing.T, containerID string) {
	t.Helper()
	sdk := newImageSecurityFixtureClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := sdk.ContainerKill(ctx, containerID, "KILL")
	require.NoError(t, err, "failed to kill container %s", containerID)
}

// waitForProvisionStatus polls ListProvisions until the given lease reaches the expected status.
func waitForProvisionStatus(t *testing.T, b *Backend, leaseUUID string, expected backend.ProvisionStatus, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		provisions, err := b.ListProvisions(context.Background())
		if err != nil {
			return false
		}
		for _, p := range provisions {
			if p.LeaseUUID == leaseUUID && p.Status == expected {
				return true
			}
		}
		return false
	}, timeout, 100*time.Millisecond, "provision %s did not reach status %s", leaseUUID, expected)
}

// inspectProvisionContainers lists containers for a lease by label.
func inspectProvisionContainers(t *testing.T, leaseUUID string) []container.Summary {
	t.Helper()
	docker, err := NewDockerClient(t.Context(), "", "")
	require.NoError(t, err)
	defer func() { _ = docker.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	containers, err := docker.client.ContainerList(ctx, container.ListOptions{
		All: true,
		Filters: filters.NewArgs(
			filters.Arg("label", LabelManaged+"=true"),
			filters.Arg("label", LabelLeaseUUID+"="+leaseUUID),
		),
	})
	require.NoError(t, err)
	return containers
}

// requireProvisionContainerImage checks both sides of the image contract:
// Docker executes an immutable ID, while labels and backend inventory preserve
// the exact manifest reference used for release comparisons and recovery.
func requireProvisionContainerImage(t *testing.T, summary container.Summary, reference string) {
	t.Helper()
	docker, err := NewDockerClient(t.Context(), "", "")
	require.NoError(t, err)
	defer func() { _ = docker.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	raw, err := docker.client.ContainerInspect(ctx, summary.ID)
	require.NoError(t, err)
	require.NotNil(t, raw.ContainerJSONBase)
	require.NotNil(t, raw.Config)
	require.Regexp(t, `^sha256:[0-9a-f]{64}$`, raw.Image, "container must execute an immutable image ID")
	require.Equal(t, raw.Image, raw.Config.Image, "configured image must equal Docker's actual execution ID")
	require.Equal(t, raw.Image, raw.Config.Labels[LabelImageID], "reference binding must identify the executed image")
	require.Equal(t, reference, raw.Config.Labels[LabelImageReference], "container must preserve the exact manifest reference")
	require.Equal(t, raw.Image, summary.ImageID, "list and inspect must agree on the executed image")
	require.Equal(t, raw.Config.Image, summary.Image, "list must expose the immutable configured image")
	require.Equal(t, reference, summary.Labels[LabelImageReference])
	require.Equal(t, raw.Image, summary.Labels[LabelImageID])

	projected, err := docker.InspectContainer(ctx, summary.ID)
	require.NoError(t, err)
	require.Equal(t, reference, projected.Image, "backend inventory must retain the original release reference")
}

// getProvisionInfo returns the ProvisionInfo for a specific lease, or fails.
func getProvisionInfo(t *testing.T, b *Backend, leaseUUID string) backend.ProvisionInfo {
	t.Helper()
	provisions, err := b.ListProvisions(context.Background())
	require.NoError(t, err)
	for _, p := range provisions {
		if p.LeaseUUID == leaseUUID {
			return p
		}
	}
	t.Fatalf("provision not found for lease %s", leaseUUID)
	return backend.ProvisionInfo{}
}

// --- Edge-case integration tests ---

// NOTE: The former TestIntegration_Docker_PortConflict was removed with ENG-605.
// It provisioned a manifest pinning a fixed host_port to force an "address
// already in use" collision, but fixed host ports are now rejected at admission
// (ValidateNoFixedHostPorts), so that scenario is unreachable via a tenant
// manifest — dynamic ports are assigned free ephemeral ports and never
// deterministically collide. Admission rejection is covered by the unit/wiring
// tests (TestProvision_RejectsFixedHostPort, TestUpdate_RejectsFixedHostPort).

// TestIntegration_Docker_DuplicateProvision_Rejected verifies that provisioning
// the same lease UUID twice returns ErrAlreadyProvisioned while the first
// provision is still active.
func TestIntegration_Docker_DuplicateProvision_Rejected(t *testing.T) {
	callbackServer, callbackCh := startCallbackServer(t)

	b := testBackendWithRealDocker(t, func(cfg *Config) {
		cfg.NetworkIsolation = ptrBool(false)
	})

	ctx := context.Background()
	leaseUUID := newIntegrationLeaseUUID()

	appManifest := manifest.Manifest{
		Image:   "busybox:latest",
		Command: []string{"sleep", "3600"},
	}
	payload, err := json.Marshal(appManifest)
	require.NoError(t, err)
	callbacks := newIntegrationCallbackAuthority(t, callbackServer.URL)

	req := backend.ProvisionRequest{
		LeaseUUID:            leaseUUID,
		Tenant:               "test-tenant",
		ProviderUUID:         testProviderUUID,
		Items:                []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
		CallbackURL:          callbacks.operationURL,
		LifecycleCallbackURL: callbacks.lifecycleURL,
		Payload:              payload,
	}

	// First provision should succeed
	err = b.Provision(ctx, req)
	require.NoError(t, err)

	// Wait for the first provision to complete
	select {
	case cb := <-callbackCh:
		require.Equal(t, backend.CallbackStatusSuccess, cb.Status)
	case <-time.After(2 * time.Minute):
		t.Fatal("timeout waiting for first provision callback")
	}
	// The callback server publishes before writing HTTP 200. Wait until the
	// sender has consumed that response and removed the exact durable outbox row
	// before taking the replay baseline.
	require.Eventually(t, func() bool {
		pending, listErr := b.callbackStore.ListPending()
		return listErr == nil && len(pending) == 0
	}, 10*time.Second, 10*time.Millisecond, "initial callback outbox row was not acknowledged")

	// An exact replay of req is idempotent: once the original actor is quiescent,
	// it must neither append a release nor create another durable callback.
	awaitProvisionWorkerQuiescence(t, b, leaseUUID)
	releasesBeforeReplay, err := b.releaseStore.List(leaseUUID)
	require.NoError(t, err)
	require.NoError(t, b.Provision(ctx, req))
	awaitProvisionWorkerQuiescence(t, b, leaseUUID)
	releasesAfterReplay, err := b.releaseStore.List(leaseUUID)
	require.NoError(t, err)
	require.Equal(t, releasesBeforeReplay, releasesAfterReplay)
	pendingAfterReplay, err := b.callbackStore.ListPending()
	require.NoError(t, err)
	require.Empty(t, pendingAfterReplay, "exact operation replay must not append an outbox row")
	select {
	case callback := <-callbackCh:
		t.Fatalf("exact operation replay emitted another callback: %+v", callback)
	default:
	}

	// A distinct operation authority for the same live lease is the conflicting
	// duplicate that must be rejected.
	duplicateCallbacks := newIntegrationCallbackAuthority(t, callbackServer.URL)
	duplicateReq := req
	duplicateReq.CallbackURL = duplicateCallbacks.operationURL
	duplicateReq.LifecycleCallbackURL = duplicateCallbacks.lifecycleURL
	err = b.Provision(ctx, duplicateReq)
	require.Error(t, err)
	assert.ErrorIs(t, err, backend.ErrAlreadyProvisioned,
		"duplicate provision should return ErrAlreadyProvisioned")

	// Should still have exactly 1 container (the first provision)
	containers := inspectProvisionContainers(t, leaseUUID)
	assert.Len(t, containers, 1, "duplicate provision should not create extra containers")

	// Original provision should still be healthy
	info := getProvisionInfo(t, b, leaseUUID)
	assert.Equal(t, backend.ProvisionStatusReady, info.Status)
}

// --- Restart, Update, and GetReleases integration tests ---

// --- Stack integration tests ---
