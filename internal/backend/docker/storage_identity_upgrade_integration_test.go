package docker

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
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
			Names: []string{"/fred-" + leaseUUID + "-0"},
			Image: "docker.io/library/alpine:3.22",
			Labels: map[string]string{
				LabelManaged:       "true",
				LabelBackendName:   "docker",
				LabelLeaseUUID:     leaseUUID,
				LabelTenant:        "tenant-a",
				LabelProviderUUID:  "22222222-2222-4222-8222-222222222222",
				LabelSKU:           "sku-stateless",
				LabelInstanceIndex: "0",
				LabelCallbackURL:   callbackURL,
			},
		}})
		cfg := storageIdentityIntegrationConfig(t, server.URL)
		writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
		writeLegacyAuthorityStores(t, cfg)
		releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: cfg.ReleasesDBPath})
		require.NoError(t, err)
		require.NoError(t, releases.Append(leaseUUID, shared.Release{
			Version:  1,
			Manifest: []byte(`{"image":"docker.io/library/alpine:3.22"}`),
			Image:    "docker.io/library/alpine:3.22",
			Status:   "active",
		}))
		require.NoError(t, releases.Close())

		_, err = InitializeStorageIdentityForConfig(
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

func TestIntegrationStorageIdentityPreflightClassifiesV013MigrationCrashArtifacts(t *testing.T) {
	const (
		leaseUUID    = "550e8400-e29b-41d4-a716-446655440000"
		providerUUID = "22222222-2222-4222-8222-222222222222"
		callbackURL  = "https://fred.example/callbacks/provision"
		imageName    = "docker.io/library/nginx:1.27"
	)
	prev := func(index int) container.Summary {
		return container.Summary{
			ID:    fmt.Sprintf("prev-%d", index),
			Names: []string{fmt.Sprintf("/fred-%s-app-%d-prev", leaseUUID, index)},
			Image: imageName,
			State: "exited",
			Labels: map[string]string{
				LabelManaged:       "true",
				LabelBackendName:   "docker",
				LabelLeaseUUID:     leaseUUID,
				LabelTenant:        "tenant-a",
				LabelProviderUUID:  providerUUID,
				LabelSKU:           "sku-stateless",
				LabelInstanceIndex: strconv.Itoa(index),
				LabelCallbackURL:   callbackURL,
			},
		}
	}
	stack := func(index int) container.Summary {
		return container.Summary{
			ID:    fmt.Sprintf("stack-%d", index),
			Names: []string{fmt.Sprintf("/fred-%s-app-%d", leaseUUID, index)},
			Image: imageName,
			State: "running",
			Labels: map[string]string{
				LabelManaged:       "true",
				LabelLeaseUUID:     leaseUUID,
				LabelTenant:        "tenant-a",
				LabelSKU:           "sku-stateless",
				LabelInstanceIndex: strconv.Itoa(index),
				LabelServiceName:   manifest.DefaultServiceName,
				// Wire-faithful v0.13 migration omission: BackendName,
				// ProviderUUID, CallbackURL, and LifecycleCallbackURL are all
				// absent. Strict inventory surfaces this only for whole-cohort
				// fail-closed classification.
			},
		}
	}

	for _, test := range []struct {
		name       string
		containers []container.Summary
		release    shared.Release
		want       string
		wantIDs    string
	}{
		{
			name:       "pre RecordMigration complete cohorts",
			containers: []container.Summary{prev(0), prev(1), stack(1), stack(0)},
			release: shared.Release{
				Manifest: []byte(`{"image":"docker.io/library/nginx:1.27"}`),
				Image:    imageName, Status: "active",
			},
			want:    "pre-RecordMigration",
			wantIDs: `immutable_stack_container_ids=["stack-0","stack-1"]`,
		},
		{
			name:       "post RecordMigration partial rollback cleanup",
			containers: []container.Summary{prev(1), stack(0), stack(1)},
			release: shared.Release{
				Manifest: []byte(`{"services":{"app":{"image":"docker.io/library/nginx:1.27"}}}`),
				Image:    "stack", Status: "active",
			},
			want: "post-RecordMigration",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := newStorageIdentityDockerServer(t, test.containers)
			cfg := storageIdentityIntegrationConfig(t, server.URL)
			writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
			writeLegacyAuthorityStores(t, cfg)
			releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: cfg.ReleasesDBPath})
			require.NoError(t, err)
			require.NoError(t, releases.Append(leaseUUID, test.release))
			require.NoError(t, releases.Close())

			_, err = PreflightStorageIdentityAdoptionForConfig(
				t.Context(), cfg, discardStorageIdentityLogger(),
			)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrV013InterruptedMigration))
			assert.ErrorContains(t, err, test.want)
			if test.wantIDs != "" {
				assert.ErrorContains(t, err, test.wantIDs)
			} else {
				assert.NotContains(t, err.Error(), "immutable_stack_container_ids")
			}
			assertNoStorageIdentityMarkers(t, cfg)
		})
	}
}

func TestIntegrationStorageIdentityPreflightClassifiesStoppedOrphanPrev(t *testing.T) {
	const (
		leaseUUID    = "550e8400-e29b-41d4-a716-446655440000"
		providerUUID = "22222222-2222-4222-8222-222222222222"
	)
	prev := func(index int, id string) container.Summary {
		state := "exited"
		if index == 0 {
			state = "created"
		}
		return container.Summary{
			ID:    id,
			Names: []string{fmt.Sprintf("/fred-%s-app-%d-prev", leaseUUID, index)},
			Image: "docker.io/library/nginx:1.27",
			State: state,
			Labels: map[string]string{
				LabelManaged:       "true",
				LabelBackendName:   "docker",
				LabelLeaseUUID:     leaseUUID,
				LabelTenant:        "tenant-a",
				LabelProviderUUID:  providerUUID,
				LabelSKU:           "sku-stateless",
				LabelInstanceIndex: strconv.Itoa(index),
				LabelCallbackURL:   "https://fred.example/callbacks/provision",
			},
		}
	}
	server := newStorageIdentityDockerServer(t, []container.Summary{
		prev(1, "orphan-prev-z"),
		prev(0, "orphan-prev-a"),
	})
	cfg := storageIdentityIntegrationConfig(t, server.URL)
	writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
	writeLegacyAuthorityStores(t, cfg)

	_, err := PreflightStorageIdentityAdoptionForConfig(
		t.Context(), cfg, discardStorageIdentityLogger(),
	)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrV013OrphanRollbackRemnant))
	assert.ErrorContains(t, err,
		`immutable_container_ids=["orphan-prev-a","orphan-prev-z"]`)
	assert.ErrorContains(t, err, `backend="docker"`)
	assert.ErrorContains(t, err, `provider="22222222-2222-4222-8222-222222222222"`)
	assert.ErrorContains(t, err, "placement-preflight -prove-terminal-orphan")
	assert.ErrorContains(t, err, "necessary but not sufficient")
	assertNoStorageIdentityMarkers(t, cfg)
}

func TestSortedImmutableContainerIDsAreDeterministicAndBounded(t *testing.T) {
	t.Parallel()

	ids, err := sortedImmutableContainerIDs([]ContainerInfo{
		{ContainerID: "container-z"},
		{ContainerID: "container-a"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"container-a", "container-z"}, ids)

	for name, containers := range map[string][]ContainerInfo{
		"empty": nil,
		"duplicate": {
			{ContainerID: "container-a"},
			{ContainerID: "container-a"},
		},
		"unsafe":       {{ContainerID: "container\nforged"}},
		"oversized id": {{ContainerID: strings.Repeat("a", maxDiagnosticContainerIDBytes+1)}},
		"oversized cohort": func() []ContainerInfo {
			result := make([]ContainerInfo, backend.MaxOperationQuantity+1)
			for index := range result {
				result[index].ContainerID = fmt.Sprintf("container-%d", index)
			}
			return result
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := sortedImmutableContainerIDs(containers)
			require.Error(t, err)
		})
	}
}

func TestProveStoppedV013RollbackCohortRequiresExactDenseIdentity(t *testing.T) {
	t.Parallel()
	const (
		leaseUUID    = "550e8400-e29b-41d4-a716-446655440000"
		providerUUID = "22222222-2222-4222-8222-222222222222"
		callbackURL  = "https://fred.example/callbacks/provision"
	)
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	base := []ContainerInfo{
		{
			ContainerID: "prev-a", Name: "fred-" + leaseUUID + "-app-0-prev",
			LeaseUUID: leaseUUID, Tenant: "tenant-a", ProviderUUID: providerUUID,
			BackendName: "docker", SKU: "sku-stateless", Image: "nginx:1.27",
			CallbackURL: callbackURL, LifecycleCallbackURL: lifecycleCallbackURL,
			CustomDomain: "legacy.example", InstanceIndex: 0, Status: "created",
		},
		{
			ContainerID: "prev-b", Name: "fred-" + leaseUUID + "-app-1-prev",
			LeaseUUID: leaseUUID, Tenant: "tenant-a", ProviderUUID: providerUUID,
			BackendName: "docker", SKU: "sku-stateless", Image: "nginx:1.27",
			CallbackURL: callbackURL, LifecycleCallbackURL: lifecycleCallbackURL,
			CustomDomain: "legacy.example", InstanceIndex: 1, Status: "exited",
		},
	}
	proof, err := proveStoppedV013RollbackCohort(base, "docker")
	require.NoError(t, err)
	assert.Equal(t, providerUUID, proof.providerUUID)
	assert.Equal(t, []string{"prev-a", "prev-b"}, proof.containerIDs)

	clone := func() []ContainerInfo { return append([]ContainerInfo(nil), base...) }
	for name, mutate := range map[string]func([]ContainerInfo) []ContainerInfo{
		"wrong exact name": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].Name = "fred-" + leaseUUID + "-app-9-prev"
			return cohort
		},
		"service label present": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].ServiceName = "app"
			return cohort
		},
		"running": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].Status = "running"
			return cohort
		},
		"paused": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].Status = "paused"
			return cohort
		},
		"foreign backend": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].BackendName = "docker-b"
			return cohort
		},
		"foreign provider": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].ProviderUUID = "33333333-3333-4333-8333-333333333333"
			return cohort
		},
		"foreign tenant": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].Tenant = "tenant-b"
			return cohort
		},
		"divergent SKU": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].SKU = "sku-other"
			return cohort
		},
		"divergent image": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].Image = "alpine:3.22"
			return cohort
		},
		"divergent domain": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].CustomDomain = "other.example"
			return cohort
		},
		"divergent operation callback": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].CallbackURL = "https://other.example/callbacks/provision"
			return cohort
		},
		"divergent lifecycle callback": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].LifecycleCallbackURL = "https://other.example/callbacks/provision"
			return cohort
		},
		"negative index": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].InstanceIndex = -1
			cohort[1].Name = "fred-" + leaseUUID + "-app--1-prev"
			return cohort
		},
		"duplicate index": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].InstanceIndex = 0
			cohort[1].Name = "fred-" + leaseUUID + "-app-0-prev"
			return cohort
		},
		"sparse index": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].InstanceIndex = 2
			cohort[1].Name = "fred-" + leaseUUID + "-app-2-prev"
			return cohort
		},
		"unsafe ID": func(cohort []ContainerInfo) []ContainerInfo {
			cohort[1].ContainerID = "forged\nidentifier"
			return cohort
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := proveStoppedV013RollbackCohort(mutate(clone()), "docker")
			require.Error(t, err)
		})
	}
}

func TestIntegrationStorageIdentityPreflightRejectsDivergentLegacyCallbacks(t *testing.T) {
	const (
		leaseUUID    = "550e8400-e29b-41d4-a716-446655440000"
		providerUUID = "22222222-2222-4222-8222-222222222222"
		imageName    = "docker.io/library/nginx:1.27"
	)
	prev := func(index int, callbackURL string) container.Summary {
		return container.Summary{
			ID:    fmt.Sprintf("prev-%d", index),
			Names: []string{fmt.Sprintf("/fred-%s-app-%d-prev", leaseUUID, index)},
			Image: imageName,
			State: "exited",
			Labels: map[string]string{
				LabelManaged:       "true",
				LabelBackendName:   "docker",
				LabelLeaseUUID:     leaseUUID,
				LabelTenant:        "tenant-a",
				LabelProviderUUID:  providerUUID,
				LabelSKU:           "sku-stateless",
				LabelInstanceIndex: strconv.Itoa(index),
				LabelCallbackURL:   callbackURL,
			},
		}
	}
	stack := func(index int) container.Summary {
		return container.Summary{
			ID:    fmt.Sprintf("stack-%d", index),
			Names: []string{fmt.Sprintf("/fred-%s-app-%d", leaseUUID, index)},
			Image: imageName,
			State: "running",
			Labels: map[string]string{
				LabelManaged:       "true",
				LabelLeaseUUID:     leaseUUID,
				LabelTenant:        "tenant-a",
				LabelSKU:           "sku-stateless",
				LabelInstanceIndex: strconv.Itoa(index),
				LabelServiceName:   manifest.DefaultServiceName,
			},
		}
	}
	callbacks := []string{
		"https://fred-a.example/callbacks/provision",
		"https://fred-b.example/callbacks/provision",
	}

	for _, test := range []struct {
		name       string
		containers []container.Summary
	}{
		{
			name: "rollback-only migration cohort",
			containers: []container.Summary{
				prev(0, callbacks[0]), prev(1, callbacks[1]),
			},
		},
		{
			name: "pre-RecordMigration mixed cohort",
			containers: []container.Summary{
				prev(0, callbacks[0]), prev(1, callbacks[1]), stack(0), stack(1),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := newStorageIdentityDockerServer(t, test.containers)
			cfg := storageIdentityIntegrationConfig(t, server.URL)
			writeLegacyCallbackStore(t, cfg.CallbackDBPath, nil)
			writeLegacyAuthorityStores(t, cfg)
			releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: cfg.ReleasesDBPath})
			require.NoError(t, err)
			require.NoError(t, releases.Append(leaseUUID, shared.Release{
				Manifest: []byte(`{"image":"docker.io/library/nginx:1.27"}`),
				Image:    imageName, Status: "active",
			}))
			require.NoError(t, releases.Close())
			before := snapshotStorageIdentityAuthorityFiles(t, cfg)

			_, err = PreflightStorageIdentityAdoptionForConfig(
				t.Context(), cfg, discardStorageIdentityLogger(),
			)
			require.Error(t, err)
			assert.ErrorContains(t, err, "callback_url differs")
			assertStorageIdentityAuthorityUnchanged(t, cfg, before)
		})
	}
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
			w.Header().Set("API-Version", "1.47")
			_, err := io.WriteString(w, "OK")
			assert.NoError(t, err)
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
	dockerClient, err := NewDockerClient(cfg.DockerHost, cfg.Name)
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
