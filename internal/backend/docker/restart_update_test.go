package docker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// --- Restart tests ---

func TestRestart_NotProvisioned(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID: "nonexistent",
	})
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestRestart_InvalidState_Provisioning(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusProvisioning},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID: "lease-1",
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
	assert.Contains(t, err.Error(), "provisioning")
}

func TestRestart_InvalidState_Restarting(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusRestarting},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID: "lease-1",
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestRestart_InvalidState_Updating(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusUpdating},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID: "lease-1",
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestRestart_NoManifest(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusReady}, // No stored manifest
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID: "lease-1",
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
	assert.Contains(t, err.Error(), "no stored manifest")
}

// --- Update tests ---

func TestUpdate_NotProvisioned(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID: "nonexistent",
		Payload:   validManifestJSON("nginx:latest"),
	})
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestUpdate_InvalidState_Provisioning(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusProvisioning,
			SKU:    "docker-small"},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID: "lease-1",
		Payload:   validManifestJSON("nginx:latest"),
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestUpdate_InvalidState_Restarting(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusRestarting,
			SKU:    "docker-small"},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID: "lease-1",
		Payload:   validManifestJSON("nginx:latest"),
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestUpdate_InvalidState_Updating(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusUpdating,
			SKU:    "docker-small"},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID: "lease-1",
		Payload:   validManifestJSON("nginx:latest"),
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestUpdate_ImageNotAllowed(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusReady,
			SKU:    "docker-small"},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	// AllowedRegistries defaults to ["docker.io"] in DefaultConfig

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID: "lease-1",
		Payload:   validManifestJSON("evil.registry.com/malware:latest"),
	})
	assert.ErrorIs(t, err, backend.ErrValidation)
}

// --- GetReleases tests ---

func TestGetReleases_NotProvisioned(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)

	releases, err := b.GetReleases(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
	assert.Nil(t, releases)
}

func TestGetReleases_NilReleaseStore(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusReady},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	b.releaseStore = nil

	releases, err := b.GetReleases(context.Background(), "lease-1")
	assert.NoError(t, err)
	assert.Nil(t, releases)
}

func TestGetReleases_WithReleases(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "releases.db")
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer releaseStore.Close()

	// Seed some releases
	now := time.Now()
	require.NoError(t, releaseStore.Append("lease-1", shared.Release{
		Manifest:  []byte(`{"image":"nginx:1.25"}`),
		Image:     "nginx:1.25",
		Status:    "superseded",
		CreatedAt: now.Add(-1 * time.Hour),
	}))
	require.NoError(t, releaseStore.Append("lease-1", shared.Release{
		Manifest:  []byte(`{"image":"nginx:1.26"}`),
		Image:     "nginx:1.26",
		Status:    "active",
		CreatedAt: now,
	}))

	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusReady},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	b.releaseStore = releaseStore

	releases, err := b.GetReleases(context.Background(), "lease-1")
	require.NoError(t, err)
	require.Len(t, releases, 2)

	assert.Equal(t, 1, releases[0].Version)
	assert.Equal(t, "nginx:1.25", releases[0].Image)
	assert.Equal(t, "superseded", releases[0].Status)

	assert.Equal(t, 2, releases[1].Version)
	assert.Equal(t, "nginx:1.26", releases[1].Image)
	assert.Equal(t, "active", releases[1].Status)
}

func TestGetReleases_EmptyHistory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "releases.db")
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer releaseStore.Close()

	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusReady},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	b.releaseStore = releaseStore

	releases, err := b.GetReleases(context.Background(), "lease-1")
	require.NoError(t, err)
	assert.Empty(t, releases)
}

// --- RecoverState: Restarting/Updating preserved ---

func TestRecoverState_RestartingPreserved(t *testing.T) {
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	existing := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusRestarting,
			CreatedAt: time.Now()},
		},
	}
	b := newBackendForTest(mock, existing)

	err := b.recoverState(context.Background())
	require.NoError(t, err)

	prov := b.provisions["lease-1"]
	require.NotNil(t, prov)
	assert.Equal(t, backend.ProvisionStatusRestarting, prov.Status,
		"restarting provision should be preserved through recoverState")
}

func TestRecoverState_UpdatingPreserved(t *testing.T) {
	mock := &mockDockerClient{
		ListManagedContainersFn: func(ctx context.Context) ([]ContainerInfo, error) {
			return nil, nil
		},
	}
	existing := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusUpdating,
			CreatedAt: time.Now()},
		},
	}
	b := newBackendForTest(mock, existing)

	err := b.recoverState(context.Background())
	require.NoError(t, err)

	prov := b.provisions["lease-1"]
	require.NotNil(t, prov)
	assert.Equal(t, backend.ProvisionStatusUpdating, prov.Status,
		"updating provision should be preserved through recoverState")
}

// --- Deprovision cleans up releases ---

func TestDeprovision_CleansUpReleases(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "releases.db")
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer releaseStore.Close()

	// Seed a release
	require.NoError(t, releaseStore.Append("lease-1", shared.Release{
		Manifest:  []byte(`{"image":"nginx:1.25"}`),
		Image:     "nginx:1.25",
		Status:    "active",
		CreatedAt: time.Now(),
	}))

	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant:       "tenant-a",
			ProviderUUID: "prov-1",
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1"},
			Quantity:     1},
		},
	}

	mock := &mockDockerClient{
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
	}

	b := newBackendForTest(mock, provisions)
	b.releaseStore = releaseStore
	b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	err = b.Deprovision(context.Background(), "lease-1")
	require.NoError(t, err)

	// Verify releases were cleaned up
	releases, err := releaseStore.List("lease-1")
	require.NoError(t, err)
	assert.Empty(t, releases)
}

// --- Initial release recorded on Provision success ---

func TestProvision_RecordsInitialRelease(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "releases.db")
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer releaseStore.Close()

	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return nil
		},
		InspectContainerFn: func(ctx context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}

	composeMock := &mockComposeExecutor{
		UpFn: func(ctx context.Context, project *composetypes.Project, opts composeUpOpts) error {
			return nil
		},
		PSFn: func(ctx context.Context, projectName string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{
				{ID: "c1", Service: manifest.DefaultServiceName, State: "running"},
			}, nil
		},
	}

	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		close(callbackReceived)
	}))
	defer callbackServer.Close()

	b := newBackendForProvisionTest(t, mock, nil)
	b.compose = composeMock
	b.releaseStore = releaseStore
	b.httpClient = callbackServer.Client()
	rebuildCallbackSender(b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = callbackServer.URL

	err = b.Provision(context.Background(), req)
	require.NoError(t, err)

	select {
	case <-callbackReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}

	// Verify initial release was recorded.
	// Post-Task-15 the Image field carries the marker "stack" (the
	// per-service images live inside Manifest payload, which we
	// verify contains the original image string).
	releases, err := releaseStore.List("lease-1")
	require.NoError(t, err)
	require.Len(t, releases, 1)
	assert.Equal(t, 1, releases[0].Version)
	assert.Equal(t, "stack", releases[0].Image,
		"post-Task-15 release.Image is the stack-marker; per-service images live in Manifest payload")
	assert.Equal(t, "active", releases[0].Status)
	assert.Contains(t, string(releases[0].Manifest), "nginx:latest",
		"Manifest payload must carry the original tenant-submitted JSON, which includes the per-service image")
}

// TestRestart_ActiveProvisionsGauge verifies that the activeProvisions gauge
// is adjusted correctly across restart success and failure paths.

// TestUpdate_ActiveProvisionsGauge verifies that the activeProvisions gauge
// is adjusted correctly when an update image pull fails (preflight failure).
