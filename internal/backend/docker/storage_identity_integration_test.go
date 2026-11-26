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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/volume"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backendidentity"
)

// TestIntegrationStorageIdentityInitializationV013Modes drives the exported
// operator entrypoint against a protocol-faithful, unprivileged Docker Engine
// boundary. The callback database and marker pair are real bbolt/filesystem
// artifacts, so this covers the v0.13 evidence -> seal -> verify-only restart
// sequence without depending on a host Docker daemon or quota filesystem.
func TestIntegrationStorageIdentityInitialization(t *testing.T) {
	t.Run("empty backend initializes and reruns verify-only", func(t *testing.T) {
		server := newStorageIdentityDockerServer(t, nil)
		cfg := storageIdentityIntegrationConfig(t, server.URL)

		initialized, err := InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(),
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
			t.Context(), cfg, discardStorageIdentityLogger(),
		)
		require.NoError(t, err)
		assert.Equal(t, initialized, rerun, "committed initializer rerun must be verify-only")
		_, err = os.Lstat(interruptedPublish)
		assert.ErrorIs(t, err, os.ErrNotExist, "recognized interrupted marker publication must be recovered")
		assertStorageIdentitySurvivesVerifyOnlyRestart(t, cfg, initialized)
	})

	// Journals that exist without a storage identity are a v0.13 lineage. This
	// release seals only an empty backend, so it refuses them, empty or not,
	// and leaves them unsealed for the final 0.14 release to adopt.
	t.Run("v0.13 journals are refused and left unsealed", func(t *testing.T) {
		server := newStorageIdentityDockerServer(t, nil)
		cfg := storageIdentityIntegrationConfig(t, server.URL)
		writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
		writeLegacyAuthorityStores(t, cfg)

		_, err := InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(),
		)
		require.ErrorContains(t, err, "3 of 3 authoritative backend journals exist without a storage identity")
		assert.ErrorContains(t, err, "final 0.14 release")
		assertNoStorageIdentityMarkers(t, cfg)
	})

	// A sealed journal whose marker pair is gone is not an empty backend. A
	// second seal would mint a new identity over bound journals, so it is
	// refused and no marker is written.
	t.Run("sealed journals without their markers are not resealed", func(t *testing.T) {
		server := newStorageIdentityDockerServer(t, nil)
		cfg := storageIdentityIntegrationConfig(t, server.URL)
		_, err := InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(),
		)
		require.NoError(t, err)
		for _, path := range []string{
			filepath.Clean(cfg.CallbackDBPath) + ".storage-identity.json",
			filepath.Clean(cfg.CallbackDBPath) + ".storage-identity-anchor.json",
		} {
			require.NoError(t, os.Remove(path))
		}

		_, err = InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(),
		)
		require.ErrorContains(t, err, "already identity-bound")
		assertNoStorageIdentityMarkers(t, cfg)
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

		_, err := InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(),
		)
		require.NoError(t, err)
		primaryPath := filepath.Clean(cfg.CallbackDBPath) + ".storage-identity.json"
		interruptedPublish := filepath.Join(
			filepath.Dir(primaryPath),
			"."+filepath.Base(primaryPath)+".tmp-6ba7b812-9dad-41d1-80b4-00c04fd430c8",
		)
		require.NoError(t, os.Link(primaryPath, interruptedPublish))

		_, err = InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(),
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

		_, err := InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(),
		)
		require.NoError(t, err)
		_, err = InitializeStorageIdentityForConfig(
			t.Context(), cfg, discardStorageIdentityLogger(),
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
	var imageCacheMarkerMu sync.Mutex
	var imageCacheMarker *volume.Volume
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
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/volumes/create"):
			var options volume.CreateOptions
			require.NoError(t, json.NewDecoder(r.Body).Decode(&options))
			imageCacheMarkerMu.Lock()
			defer imageCacheMarkerMu.Unlock()
			if imageCacheMarker == nil {
				imageCacheMarker = &volume.Volume{Name: options.Name, Driver: options.Driver, Labels: options.Labels}
			}
			w.WriteHeader(http.StatusCreated)
			assert.NoError(t, json.NewEncoder(w).Encode(imageCacheMarker))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/volumes/"+imageCacheOwnerVolume):
			imageCacheMarkerMu.Lock()
			defer imageCacheMarkerMu.Unlock()
			if imageCacheMarker == nil {
				http.Error(w, `{"message":"volume not found"}`, http.StatusNotFound)
				return
			}
			assert.NoError(t, json.NewEncoder(w).Encode(imageCacheMarker))
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
