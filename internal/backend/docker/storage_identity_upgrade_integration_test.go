package docker

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared/manifest"
	"github.com/manifest-network/fred/internal/backendidentity"
)

// TestIntegrationStorageIdentityInitializationV013Modes drives the exported
// operator entrypoint against a protocol-faithful, unprivileged Docker Engine
// boundary. The callback database and marker pair are real bbolt/filesystem
// artifacts, so this covers the v0.13 evidence -> seal -> verify-only restart
// sequence without depending on a host Docker daemon or quota filesystem.
func TestIntegrationStorageIdentityInitializationV013Modes(t *testing.T) {
	t.Run("empty lineage rejects adopt and initializes new", func(t *testing.T) {
		server := newStorageIdentityDockerServer(t, nil)
		cfg := storageIdentityIntegrationConfig(t, server.URL)
		writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
		writeLegacyAuthorityStores(t, cfg)

		_, err := InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(), StorageIdentityInitializeAdopt,
		)
		require.Error(t, err)
		assert.ErrorContains(t, err, "-initialize-storage-identity new")
		assertNoStorageIdentityMarkers(t, cfg)

		initialized, err := InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(), StorageIdentityInitializeNew,
		)
		require.NoError(t, err)
		require.True(t, initialized.Valid())
		primaryPath := filepath.Clean(cfg.CallbackDBPath) + ".storage-identity.json"
		interruptedPublish := filepath.Join(
			filepath.Dir(primaryPath),
			"."+filepath.Base(primaryPath)+".tmp-6ba7b811-9dad-41d1-80b4-00c04fd430c8",
		)
		require.NoError(t, os.Link(primaryPath, interruptedPublish))
		rerun, err := InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(), StorageIdentityInitializeNew,
		)
		require.NoError(t, err)
		assert.Equal(t, initialized, rerun, "committed initializer rerun must be verify-only")
		_, err = os.Lstat(interruptedPublish)
		assert.ErrorIs(t, err, os.ErrNotExist, "recognized interrupted marker publication must be recovered")
		assertStorageIdentitySurvivesVerifyOnlyRestart(t, cfg, initialized)
	})

	t.Run("managed v0.13 lineage rejects new and adopts", func(t *testing.T) {
		// v0.13 persisted one tokenless callback URL and had no lifecycle
		// callback label. Keep this fixture byte-shape faithful so adoption does
		// not accidentally rely on labels introduced by the new protocol.
		const callbackURL = "https://fred.example/callbacks/provision"
		const leaseUUID = "550e8400-e29b-41d4-a716-446655440000"
		server := newStorageIdentityDockerServer(t, []container.Summary{{
			ID:    "managed-container-a",
			Names: []string{"/fred-" + leaseUUID + "-app-0"},
			Image: "docker.io/library/alpine:3.22",
			Labels: map[string]string{
				LabelManaged:       "true",
				LabelBackendName:   "docker",
				LabelLeaseUUID:     leaseUUID,
				LabelTenant:        "tenant-a",
				LabelProviderUUID:  "22222222-2222-4222-8222-222222222222",
				LabelSKU:           "sku-stateless",
				LabelInstanceIndex: "0",
				LabelServiceName:   manifest.DefaultServiceName,
				LabelCallbackURL:   callbackURL,
			},
		}})
		cfg := storageIdentityIntegrationConfig(t, server.URL)
		writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
		writeLegacyAuthorityStores(t, cfg)
		writeRawV013ReleaseHistory(t, cfg.ReleasesDBPath, leaseUUID, []v013ReleaseWire{{
			Version:  1,
			Manifest: []byte(`{"services":{"app":{"image":"docker.io/library/alpine:3.22"}}}`),
			Image:    "stack",
			Status:   "active",
		}})

		_, err := InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(), StorageIdentityInitializeNew,
		)
		require.Error(t, err)
		assert.ErrorContains(t, err, "use adopt for a verified v0.13 lineage")
		assertNoStorageIdentityMarkers(t, cfg)

		initialized, err := InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(), StorageIdentityInitializeAdopt,
		)
		require.NoError(t, err)
		require.True(t, initialized.Valid())
		rerun, err := InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(), StorageIdentityInitializeAdopt,
		)
		require.NoError(t, err)
		assert.Equal(t, initialized, rerun, "committed initializer rerun must be verify-only")
		assertStorageIdentitySurvivesVerifyOnlyRestart(t, cfg, initialized)
	})

	t.Run("committed rerun refuses a daemon swap before marker recovery", func(t *testing.T) {
		var infoReads atomic.Int64
		server := newStorageIdentityDockerServerWithDaemonID(t, nil, func() string {
			// A fresh initialization performs two identity reads. The committed
			// rerun must perform the same pre-publication re-attestation: its
			// first read still sees A and its barrier read sees B.
			if infoReads.Add(1) >= 8 {
				return "daemon-system-b"
			}
			return "daemon-system-a"
		})
		cfg := storageIdentityIntegrationConfig(t, server.URL)
		writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
		writeLegacyAuthorityStores(t, cfg)

		_, err := InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(), StorageIdentityInitializeNew,
		)
		require.NoError(t, err)
		primaryPath := filepath.Clean(cfg.CallbackDBPath) + ".storage-identity.json"
		interruptedPublish := filepath.Join(
			filepath.Dir(primaryPath),
			"."+filepath.Base(primaryPath)+".tmp-6ba7b812-9dad-41d1-80b4-00c04fd430c8",
		)
		require.NoError(t, os.Link(primaryPath, interruptedPublish))

		_, err = InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(), StorageIdentityInitializeNew,
		)
		require.ErrorContains(t, err, "docker daemon identity changed during lineage proof")
		_, statErr := os.Lstat(interruptedPublish)
		require.NoError(t, statErr,
			"stale substrate evidence must be rejected before marker recovery mutates the lineage")
	})

	t.Run("committed rerun rejects a daemon swap after marker verification", func(t *testing.T) {
		var infoReads atomic.Int64
		server := newStorageIdentityDockerServerWithDaemonID(t, nil, func() string {
			// Fresh initialization consumes six reads (initial, committed-only
			// precheck, evidence barrier, store-hook check, pre-reread barrier,
			// and post-seal barrier). The rerun's initial, pre-operation, and
			// pre-reread observations remain A; only its final barrier sees B.
			if infoReads.Add(1) >= 10 {
				return "daemon-system-b"
			}
			return "daemon-system-a"
		})
		cfg := storageIdentityIntegrationConfig(t, server.URL)
		writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
		writeLegacyAuthorityStores(t, cfg)

		_, err := InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(), StorageIdentityInitializeNew,
		)
		require.NoError(t, err)
		_, err = InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(), StorageIdentityInitializeNew,
		)
		require.ErrorContains(t, err, "docker daemon identity changed during lineage proof")
	})
}

func storageIdentityIntegrationConfig(t *testing.T, dockerHost string) Config {
	t.Helper()
	dir := t.TempDir()
	cfg := validConfig()
	cfg.DockerHost = dockerHost
	cfg.CallbackDBPath = filepath.Join(dir, "callbacks.db")
	cfg.DiagnosticsDBPath = filepath.Join(dir, "diagnostics.db")
	cfg.ReleasesDBPath = filepath.Join(dir, "releases.db")
	cfg.RetentionDBPath = filepath.Join(dir, "retentions.db")
	cfg.VolumeDataPath = ""
	cfg.VolumeMountPath = ""
	cfg.SKUMapping = map[string]string{"sku-stateless": "stateless"}
	cfg.SKUProfiles = map[string]SKUProfile{
		"stateless": {CPUCores: 0.5, MemoryMB: 512},
	}
	return cfg
}

func discardStorageIdentityLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newStorageIdentityDockerServer(
	t *testing.T,
	containers []container.Summary,
) *httptest.Server {
	t.Helper()
	return newStorageIdentityDockerServerWithDaemonID(t, containers, func() string {
		return "daemon-system-a"
	})
}

func newStorageIdentityDockerServerWithDaemonID(
	t *testing.T,
	containers []container.Summary,
	daemonID func() string,
) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/_ping" || strings.HasSuffix(r.URL.Path, "/_ping"):
			w.Header().Set("API-Version", "1.51")
			_, err := io.WriteString(w, "OK")
			assert.NoError(t, err)
		case strings.HasSuffix(r.URL.Path, "/version"):
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]string{"ApiVersion": "1.51"}))
		case strings.HasSuffix(r.URL.Path, "/info"):
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"ID":              daemonID(),
				"Driver":          "overlay2",
				"DriverStatus":    [][]string{{"Backing Filesystem", "xfs"}},
				"SecurityOptions": []string{"name=seccomp,profile=default"},
				"IPv4Forwarding":  true,
			}))
		case strings.HasSuffix(r.URL.Path, "/containers/json"):
			assert.NoError(t, json.NewEncoder(w).Encode(containers))
		default:
			http.Error(w, "unexpected Docker API path "+r.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func assertNoStorageIdentityMarkers(t *testing.T, cfg Config) {
	t.Helper()
	for _, path := range []string{
		filepath.Clean(cfg.CallbackDBPath) + ".storage-identity.json",
		filepath.Clean(cfg.CallbackDBPath) + ".storage-identity-anchor.json",
	} {
		_, err := os.Lstat(path)
		assert.ErrorIs(t, err, os.ErrNotExist)
	}
}

func assertStorageIdentitySurvivesVerifyOnlyRestart(
	t *testing.T,
	cfg Config,
	want backendidentity.ID,
) {
	t.Helper()
	dockerClient, err := NewDockerClient(t.Context(), cfg.DockerHost, cfg.Name)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dockerClient.Close()) })
	got, err := (existingDockerStorageIdentity{}).resolve(
		t.Context(), cfg, dockerClient, &noopVolumeManager{},
	)
	require.NoError(t, err)
	assert.Equal(t, want, got.ID())
	for attempt := range 2 {
		backend, err := NewWithContext(t.Context(), cfg, discardStorageIdentityLogger())
		require.NoErrorf(t, err, "production New attempt %d", attempt+1)
		assert.Equal(t, want, backend.StorageIdentity())
		require.NoError(t, backend.Stop())
	}
}
