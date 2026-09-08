package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// --- Restart tests ---

func TestReplaceOpReason(t *testing.T) {
	// doReplaceContainers runs for restart, update, AND restore — each must map
	// to its own category, and an unknown op must not be misclassified (ENG-508).
	assert.Equal(t, backend.ReasonRestartFailed, replaceOpReason("restart"))
	assert.Equal(t, backend.ReasonUpdateFailed, replaceOpReason("update"))
	assert.Equal(t, backend.ReasonRestoreFailed, replaceOpReason("restore"))
	assert.Equal(t, backend.ReasonInternal, replaceOpReason("something-else"))
}

func TestResolveMaintenanceCallbackURLs(t *testing.T) {
	const (
		id      = "550e8400-e29b-41d4-a716-446655440000"
		otherID = "123e4567-e89b-42d3-a456-426614174000"
	)
	typedOperation := "https://old.example/callbacks/provision?trace=keep&operation_id=" + id
	typedLifecycle := "https://old.example/callbacks/provision?trace=keep&lifecycle_id=" + id
	newOperation := "https://new.example/v2/callbacks/provision?trace=new&operation_id=" + id
	newLifecycle := "https://new.example/v2/callbacks/provision?trace=new&lifecycle_id=" + id

	tests := []struct {
		name             string
		callbackURL      string
		lifecycleURL     string
		requestURL       string
		wantOperationURL string
		wantLifecycleURL string
		wantErr          bool
	}{
		{
			name:             "typed persisted pair moves base without rotating ID",
			callbackURL:      typedOperation,
			lifecycleURL:     typedLifecycle,
			requestURL:       newLifecycle,
			wantOperationURL: newOperation,
			wantLifecycleURL: newLifecycle,
		},
		{
			name:             "typed lifecycle-only recovered record moves base",
			lifecycleURL:     typedLifecycle,
			requestURL:       newLifecycle,
			wantOperationURL: newOperation,
			wantLifecycleURL: newLifecycle,
		},
		{
			name:             "empty autonomous request preserves typed pair",
			callbackURL:      typedOperation,
			lifecycleURL:     typedLifecycle,
			wantOperationURL: typedOperation,
			wantLifecycleURL: typedLifecycle,
		},
		{
			name:             "legacy persisted pair moves base tokenlessly",
			callbackURL:      "https://old.example/callbacks/provision?trace=keep",
			lifecycleURL:     "https://old.example/callbacks/provision?trace=keep",
			requestURL:       "https://new.example/callbacks/provision?trace=new",
			wantOperationURL: "https://new.example/callbacks/provision?trace=new",
			wantLifecycleURL: "https://new.example/callbacks/provision?trace=new",
		},
		{
			name:             "oldest record adopts trusted typed request",
			requestURL:       newLifecycle,
			wantOperationURL: newOperation,
			wantLifecycleURL: newLifecycle,
		},
		{
			name:         "mismatched persisted pair fails closed",
			callbackURL:  typedOperation,
			lifecycleURL: "https://other.example/callbacks/provision?trace=keep&lifecycle_id=" + id,
			requestURL:   newLifecycle,
			wantErr:      true,
		},
		{
			name:         "typed request cannot rotate ID",
			callbackURL:  typedOperation,
			lifecycleURL: typedLifecycle,
			requestURL:   "https://new.example/callbacks/provision?lifecycle_id=" + otherID,
			wantErr:      true,
		},
		{
			name:         "typed request cannot downgrade",
			callbackURL:  typedOperation,
			lifecycleURL: typedLifecycle,
			requestURL:   "https://new.example/callbacks/provision",
			wantErr:      true,
		},
		{
			name:         "legacy request cannot acquire typed authority",
			callbackURL:  "https://old.example/callbacks/provision",
			lifecycleURL: "https://old.example/callbacks/provision",
			requestURL:   newLifecycle,
			wantErr:      true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			operationURL, lifecycleURL, err := resolveMaintenanceCallbackURLs(
				test.callbackURL, test.lifecycleURL, test.requestURL,
			)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.wantOperationURL, operationURL)
			assert.Equal(t, test.wantLifecycleURL, lifecycleURL)
		})
	}
}

func TestRestart_NotProvisioned(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	attachBoundMaintenanceCallbackStore(t, b)

	err := b.Restart(context.Background(), backend.RestartRequest{MaintenanceID: newTestMaintenanceID(t),
		CallbackURL: testMaintenanceLifecycleCallbackURL,
		LeaseUUID:   "21638ef8-1401-4f14-a355-1ae02afeb35b",
	})
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestRestart_InvalidState_Provisioning(t *testing.T) {
	provisions := map[string]*provision{
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Status: backend.ProvisionStatusProvisioning},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	attachBoundMaintenanceCallbackStore(t, b)

	err := b.Restart(context.Background(), backend.RestartRequest{MaintenanceID: newTestMaintenanceID(t),
		CallbackURL: testMaintenanceLifecycleCallbackURL,
		LeaseUUID:   "11638ef8-1401-4f14-a355-1ae02afeb35b",
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
	assert.Contains(t, err.Error(), "provisioning")
}

func TestRestart_InvalidState_Restarting(t *testing.T) {
	provisions := map[string]*provision{
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Status: backend.ProvisionStatusRestarting},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	attachBoundMaintenanceCallbackStore(t, b)

	err := b.Restart(context.Background(), backend.RestartRequest{MaintenanceID: newTestMaintenanceID(t),
		CallbackURL: testMaintenanceLifecycleCallbackURL,
		LeaseUUID:   "11638ef8-1401-4f14-a355-1ae02afeb35b",
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestRestart_InvalidState_Updating(t *testing.T) {
	provisions := map[string]*provision{
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Status: backend.ProvisionStatusUpdating},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	attachBoundMaintenanceCallbackStore(t, b)

	err := b.Restart(context.Background(), backend.RestartRequest{MaintenanceID: newTestMaintenanceID(t),
		CallbackURL: testMaintenanceLifecycleCallbackURL,
		LeaseUUID:   "11638ef8-1401-4f14-a355-1ae02afeb35b",
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestRestart_NoManifest(t *testing.T) {
	provisions := map[string]*provision{
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Status: backend.ProvisionStatusReady}, // No stored manifest
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	attachBoundMaintenanceCallbackStore(t, b)

	err := b.Restart(context.Background(), backend.RestartRequest{MaintenanceID: newTestMaintenanceID(t),
		CallbackURL: testMaintenanceLifecycleCallbackURL,
		LeaseUUID:   "11638ef8-1401-4f14-a355-1ae02afeb35b",
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
	assert.Contains(t, err.Error(), "no stored manifest")
}

// --- Update tests ---

func TestUpdate_NotProvisioned(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	attachBoundMaintenanceCallbackStore(t, b)

	err := b.Update(context.Background(), backend.UpdateRequest{MaintenanceID: newTestMaintenanceID(t),
		CallbackURL: testMaintenanceLifecycleCallbackURL,
		LeaseUUID:   "21638ef8-1401-4f14-a355-1ae02afeb35b",
		Payload:     validManifestJSON("nginx:latest"),
	})
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
}

func TestUpdate_InvalidState_Provisioning(t *testing.T) {
	provisions := map[string]*provision{
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Status: backend.ProvisionStatusProvisioning,
			SKU:    "docker-small"},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	attachBoundMaintenanceCallbackStore(t, b)

	err := b.Update(context.Background(), backend.UpdateRequest{MaintenanceID: newTestMaintenanceID(t),
		CallbackURL: testMaintenanceLifecycleCallbackURL,
		LeaseUUID:   "11638ef8-1401-4f14-a355-1ae02afeb35b",
		Payload:     validManifestJSON("nginx:latest"),
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestUpdate_InvalidState_Restarting(t *testing.T) {
	provisions := map[string]*provision{
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Status: backend.ProvisionStatusRestarting,
			SKU:    "docker-small"},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	attachBoundMaintenanceCallbackStore(t, b)

	err := b.Update(context.Background(), backend.UpdateRequest{MaintenanceID: newTestMaintenanceID(t),
		CallbackURL: testMaintenanceLifecycleCallbackURL,
		LeaseUUID:   "11638ef8-1401-4f14-a355-1ae02afeb35b",
		Payload:     validManifestJSON("nginx:latest"),
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestUpdate_InvalidState_Updating(t *testing.T) {
	provisions := map[string]*provision{
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Status: backend.ProvisionStatusUpdating,
			SKU:    "docker-small"},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	attachBoundMaintenanceCallbackStore(t, b)

	err := b.Update(context.Background(), backend.UpdateRequest{MaintenanceID: newTestMaintenanceID(t),
		CallbackURL: testMaintenanceLifecycleCallbackURL,
		LeaseUUID:   "11638ef8-1401-4f14-a355-1ae02afeb35b",
		Payload:     validManifestJSON("nginx:latest"),
	})
	assert.ErrorIs(t, err, backend.ErrInvalidState)
}

func TestUpdate_ImageNotAllowed(t *testing.T) {
	provisions := map[string]*provision{
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Status: backend.ProvisionStatusReady,
			SKU:    "docker-small"},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	attachBoundMaintenanceCallbackStore(t, b)
	// AllowedRegistries defaults to ["docker.io"] in DefaultConfig

	err := b.Update(context.Background(), backend.UpdateRequest{MaintenanceID: newTestMaintenanceID(t),
		CallbackURL: testMaintenanceLifecycleCallbackURL,
		LeaseUUID:   "11638ef8-1401-4f14-a355-1ae02afeb35b",
		Payload:     validManifestJSON("evil.registry.com/malware:latest"),
	})
	assert.ErrorIs(t, err, backend.ErrValidation)
}

func TestUpdate_RejectsFixedHostPort(t *testing.T) {
	provisions := map[string]*provision{
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Status: backend.ProvisionStatusReady,
			SKU:    "docker-small",
			Items:  []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	attachBoundMaintenanceCallbackStore(t, b)

	// A tenant must not be able to introduce a squatted fixed host port via
	// an update (ENG-605), just as at provision time.
	err := b.Update(context.Background(), backend.UpdateRequest{MaintenanceID: newTestMaintenanceID(t),
		CallbackURL: testMaintenanceLifecycleCallbackURL,
		LeaseUUID:   "11638ef8-1401-4f14-a355-1ae02afeb35b",
		Payload:     []byte(`{"image":"docker.io/library/nginx:latest","ports":{"8080/tcp":{"host_port":8080}}}`),
	})
	require.ErrorIs(t, err, backend.ErrInvalidManifest)
	assert.Contains(t, err.Error(), "host_port")
}

// --- GetReleases tests ---

func TestGetReleases_NotProvisioned(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)

	releases, err := b.GetReleases(context.Background(), "21638ef8-1401-4f14-a355-1ae02afeb35b")
	assert.ErrorIs(t, err, backend.ErrNotProvisioned)
	assert.Nil(t, releases)
}

func TestGetReleases_NilReleaseStore(t *testing.T) {
	provisions := map[string]*provision{
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Status: backend.ProvisionStatusReady},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	b.releaseStore = nil

	releases, err := b.GetReleases(context.Background(), "11638ef8-1401-4f14-a355-1ae02afeb35b")
	assert.NoError(t, err)
	assert.Nil(t, releases)
}

func TestGetReleases_WithReleases(t *testing.T) {
	const leaseUUID = "11638ef8-1401-4f14-a355-1ae02afeb35b"
	provisions := map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: leaseUUID,
			Status: backend.ProvisionStatusReady},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	attachBoundOperationHandoffStores(t, b)
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}
	profiles := testResourceProfiles(t, items)
	operationID, callbackURL, lifecycleCallbackURL := newTestRestoreCallbackAuthority(t)
	authority := mustTestReleaseRuntimeAuthority(
		t, operationID, "tenant-a", nominalDockerProviderUUID, callbackURL, lifecycleCallbackURL,
	)
	seedProvisionReleaseForBackendTest(t, b, leaseUUID, shared.Release{
		Manifest: validStackManifestJSON(map[string]string{"app": "nginx:1.25"}),
		Image:    "nginx:1.25", OperationID: operationID, Items: items,
		ResourceProfiles: profiles, RuntimeAuthority: authority,
	})
	activateMaintenanceReleaseForTest(t, b.maintenanceSettlement, leaseUUID,
		shared.MaintenanceIntentUpdate, shared.Release{
			Manifest: validStackManifestJSON(map[string]string{"app": "nginx:1.26"}),
			Image:    "nginx:1.26", OperationID: operationID, Items: items,
			ResourceProfiles: profiles, RuntimeAuthority: authority,
		})

	releases, err := b.GetReleases(context.Background(), leaseUUID)
	require.NoError(t, err)
	require.Len(t, releases, 2)

	assert.Equal(t, 1, releases[0].Version)
	assert.Equal(t, "stack", releases[0].Image)
	assert.Contains(t, string(releases[0].Manifest), "nginx:1.25")
	assert.Equal(t, "superseded", releases[0].Status)

	assert.Equal(t, 2, releases[1].Version)
	assert.Equal(t, "nginx:1.26", releases[1].Image)
	assert.Contains(t, string(releases[1].Manifest), "nginx:1.26")
	assert.Equal(t, "active", releases[1].Status)
}

func TestGetReleases_EmptyHistory(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "releases.db")
	releaseStore, err := newBoundReleaseStoreForTest(t, shared.ReleaseStoreConfig{DBPath: dbPath})
	require.NoError(t, err)
	defer releaseStore.Close()

	provisions := map[string]*provision{
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Status: backend.ProvisionStatusReady},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	b.releaseStore = releaseStore

	releases, err := b.GetReleases(context.Background(), "11638ef8-1401-4f14-a355-1ae02afeb35b")
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
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusRestarting,
			CreatedAt: time.Now()},
		},
	}
	b := newBackendForTest(mock, existing)

	err := b.recoverState(context.Background())
	require.NoError(t, err)

	prov := b.provisions["11638ef8-1401-4f14-a355-1ae02afeb35b"]
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
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Tenant:    "tenant-a",
			Status:    backend.ProvisionStatusUpdating,
			CreatedAt: time.Now()},
		},
	}
	b := newBackendForTest(mock, existing)

	err := b.recoverState(context.Background())
	require.NoError(t, err)

	prov := b.provisions["11638ef8-1401-4f14-a355-1ae02afeb35b"]
	require.NotNil(t, prov)
	assert.Equal(t, backend.ProvisionStatusUpdating, prov.Status,
		"updating provision should be preserved through recoverState")
}

// --- Deprovision cleans up releases ---

func TestDeprovision_CleansUpReleases(t *testing.T) {
	const (
		leaseUUID    = "11638ef8-1401-4f14-a355-1ae02afeb35b"
		providerUUID = "22222222-2222-4222-8222-222222222222"
	)
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}
	provisions := map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{LeaseUUID: leaseUUID,
			Tenant:       "tenant-a",
			ProviderUUID: providerUUID,
			SKU:          "docker-small",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1"},
			Quantity:     1,
			Items:        items},
		},
	}

	mock := &mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
		RemoveContainerFn: func(ctx context.Context, containerID string) error {
			return nil
		},
	}

	b := newBackendForTest(mock, provisions)
	attachBoundOperationHandoffStores(t, b)
	releaseStore := b.releaseStore
	profiles := testResourceProfiles(t, items)
	b.provisions[leaseUUID].ResourceProfiles = profiles
	operationID, callbackURL, lifecycleCallbackURL := newTestRestoreCallbackAuthority(t)
	b.provisions[leaseUUID].CallbackURL = callbackURL
	b.provisions[leaseUUID].LifecycleCallbackURL = lifecycleCallbackURL
	authority := mustTestReleaseRuntimeAuthority(
		t, operationID, "tenant-a", providerUUID, callbackURL, lifecycleCallbackURL,
	)
	seedProvisionReleaseForBackendTest(t, b, leaseUUID, shared.Release{
		Manifest: validStackManifestJSON(map[string]string{"app": "nginx:1.25"}),
		Image:    "nginx:1.25", OperationID: operationID, Items: items,
		ResourceProfiles: profiles, RuntimeAuthority: authority,
	})
	b.pool.TryAllocate("lease-1-0", "docker-small", "tenant-a")

	err := b.Deprovision(context.Background(), leaseUUID)
	require.NoError(t, err)

	// Verify releases were cleaned up
	releases, err := releaseStore.List(leaseUUID)
	require.NoError(t, err)
	assert.Empty(t, releases)
}

// --- Initial release recorded on Provision success ---

func TestProvision_RecordsInitialRelease(t *testing.T) {
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
	installStackStrictCohortInventory(t, mock, composeMock)
	releaseStore := b.releaseStore
	rebuildCallbackSender(b, callbackServer.Client())
	b.wg.Go(b.callbackSender.RunReplayLoop)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
	})
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	req := newProvisionRequest("11638ef8-1401-4f14-a355-1ae02afeb35b", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = testOperationCallbackURL(callbackServer.URL + "/callbacks/provision")

	err := b.Provision(context.Background(), req)
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
	releases, err := releaseStore.List("11638ef8-1401-4f14-a355-1ae02afeb35b")
	require.NoError(t, err)
	require.Len(t, releases, 1)
	assert.Equal(t, 1, releases[0].Version)
	assert.Equal(t, "stack", releases[0].Image,
		"post-Task-15 release.Image is the stack-marker; per-service images live in Manifest payload")
	assert.Equal(t, "active", releases[0].Status)
	assert.Contains(t, string(releases[0].Manifest), "nginx:latest",
		"Manifest payload must carry the original tenant-submitted JSON, which includes the per-service image")
	expectedItems := append([]backend.LeaseItem(nil), req.Items...)
	expectedItems[0].ServiceName = manifest.DefaultServiceName
	assert.Equal(t, expectedItems, releases[0].Items,
		"the durable release records the normalized service identity emitted to Docker")
}

func TestDoReplace_ActivationPersistenceFailurePreservesTargetForRecovery(t *testing.T) {
	for _, operation := range []string{"restart", "update"} {
		t.Run(operation, func(t *testing.T) {
			leaseUUID := uuid.NewString()
			operationID := mustDockerOperationID(uuid.NewString())
			providerUUID := uuid.NewString()
			oldCallbackURL := "https://old.example/callbacks/provision?operation_id=" + operationID.String()
			oldLifecycleURL := "https://old.example/callbacks/provision?lifecycle_id=" + operationID.String()
			newCallbackURL := "https://new.example/callbacks/provision?operation_id=" + operationID.String()
			newLifecycleURL := "https://new.example/callbacks/provision?lifecycle_id=" + operationID.String()
			stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
				"app": {Image: "docker.io/library/nginx:1.27"},
			}}
			items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}
			resourceProfiles := testResourceProfiles(t, items)
			provisions := map[string]*provision{
				leaseUUID: {ProvisionState: leasesm.ProvisionState{
					LeaseUUID: leaseUUID, Tenant: "tenant-a", ProviderUUID: providerUUID,
					Status: backend.ProvisionStatusReady, StackManifest: stack,
					CallbackURL:          oldCallbackURL,
					LifecycleCallbackURL: oldLifecycleURL,
					Items:                items, ContainerIDs: []string{"old"},
					ServiceContainers: map[string][]string{"app": {"old"}},
				}},
			}
			var strictContainers []ContainerInfo
			mock := &mockDockerClient{
				PullImageFn: func(context.Context, string, time.Duration) error {
					return nil
				},
				InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
					return &ContainerInfo{ContainerID: "new", Status: "running"}, nil
				},
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
					return slices.Clone(strictContainers), nil
				},
			}
			b := newBackendForTest(mock, provisions)
			defer b.stopCancel()

			attachBoundOperationHandoffStores(t, b)
			store := b.releaseStore
			settlement := b.maintenanceSettlement

			sourceAuthority := mustTestReleaseRuntimeAuthority(
				t, operationID, "tenant-a", providerUUID, oldCallbackURL, oldLifecycleURL,
			)
			targetAuthority := mustTestReleaseRuntimeAuthority(
				t, operationID, "tenant-a", providerUUID, newCallbackURL, newLifecycleURL,
			)
			seedProvisionReleaseForBackendTest(t, b, leaseUUID, shared.Release{
				Manifest: []byte(`{"services":{"app":{"image":"docker.io/library/nginx:1.26"}}}`),
				Image:    "stack", OperationID: operationID, Items: items,
				ResourceProfiles: resourceProfiles, RuntimeAuthority: sourceAuthority,
				Status: "active", CreatedAt: time.Now().Add(-time.Minute),
			})
			active, sourceClaim, err := settlement.ClaimLatestActive(leaseUUID)
			require.NoError(t, err)
			b.provisionsMu.Lock()
			b.provisions[leaseUUID].ActiveReleaseVersion = active.Version
			b.provisions[leaseUUID].ActiveOperationID = active.OperationID
			b.provisionsMu.Unlock()
			kind := shared.MaintenanceIntentRestart
			if operation == "update" {
				kind = shared.MaintenanceIntentUpdate
			}
			targetTemplate := shared.Release{
				Manifest: []byte(`{"services":{"app":{"image":"docker.io/library/nginx:1.27"}}}`),
				Image:    "stack", OperationID: operationID, Items: items,
				ResourceProfiles: resourceProfiles, RuntimeAuthority: targetAuthority,
				Status: "deploying", CreatedAt: time.Now(),
			}
			admission, err := settlement.BeginMaintenanceIntent(newTestMaintenanceIntentSpec(
				t, settlement, newTestMaintenanceID(t), kind, sourceClaim, targetTemplate,
			))
			require.NoError(t, err)
			appendClaim, err := settlement.StartMaintenanceAppend(
				createdTestMaintenanceDispatch(t, admission),
			)
			require.NoError(t, err)
			targetClaim, err := settlement.AppendMaintenance(appendClaim)
			require.NoError(t, err)
			targetClaim, err = settlement.BindMaintenanceIntentTarget(targetClaim)
			require.NoError(t, err)
			maintenance := targetClaim.Intent()
			strictContainers = []ContainerInfo{{
				ContainerID:          "new",
				LeaseUUID:            leaseUUID,
				Tenant:               "tenant-a",
				ProviderUUID:         providerUUID,
				SKU:                  "docker-small",
				ServiceName:          "app",
				InstanceIndex:        0,
				CallbackURL:          newCallbackURL,
				LifecycleCallbackURL: newLifecycleURL,
				MaintenanceID:        maintenance.MaintenanceID(),
				Image:                "docker.io/library/nginx:1.27",
				Status:               "running",
			}}
			upCalls := 0
			var projects []*composetypes.Project
			upStarted := make(chan struct{})
			releaseUp := make(chan struct{})
			b.compose = &mockComposeExecutor{
				UpFn: func(_ context.Context, project *composetypes.Project, _ composeUpOpts) error {
					upCalls++
					projects = append(projects, project)
					close(upStarted)
					<-releaseUp
					return nil
				},
				PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
					id := "new"
					if upCalls > 1 {
						id = "restored"
					}
					return []composeContainerSummary{{ID: id, Service: "app", State: "running"}}, nil
				},
			}
			b.cfg.StartupVerifyDuration = time.Millisecond
			var command leasesm.ActorCommand
			var reply leasesm.ActorReply
			if operation == "restart" {
				command, reply, err = leasesm.NewRestartCommand(t.Context(), targetClaim)
			} else {
				command, reply, err = leasesm.NewUpdateCommand(t.Context(), targetClaim)
			}
			require.NoError(t, err)
			require.True(t, b.routeToLease(leaseUUID, command))
			require.NoError(t, <-reply.Result())
			select {
			case <-upStarted:
			case <-time.After(time.Second):
				t.Fatal("maintenance substrate did not enter Compose")
			}
			require.NoError(t, store.Close(), "inject exact activation persistence failure after substrate entry")
			close(releaseUp)
			require.Eventually(t, func() bool {
				return !b.actorOwnsMaintenance(leaseUUID, maintenance.MaintenanceID())
			}, time.Second, time.Millisecond)
			assert.NotEqual(t, backend.ProvisionStatusReady, b.actorFor(leaseUUID).State(),
				"a failed activation must not publish a successful actor terminal")
			assert.Equal(t, 1, upCalls, "recovery, not an unsafe rollback, owns an ambiguous activation outcome")
			require.Len(t, projects, 1)
			assert.Equal(t,
				newLifecycleURL,
				projects[0].Services["app"].Labels[LabelLifecycleCallbackURL],
				"replacement cohort must receive the pending route",
			)
			// The injected handle is intentionally unusable after Close. Reopen the
			// same identity-bound release journal, as a backend restart would, and
			// prove the non-terminal intent/target pair survived for recovery.
			reopenedReleases, err := shared.OpenIdentityBoundReleaseStore(
				shared.ReleaseStoreConfig{DBPath: b.cfg.ReleasesDBPath},
				b.storageAuthority,
				b.storeAuthorityGate,
			)
			require.NoError(t, err)
			defer func() { require.NoError(t, reopenedReleases.Close()) }()
			reopenedSettlement, err := shared.NewMaintenanceSettlement(
				b.callbackStore,
				reopenedReleases,
			)
			require.NoError(t, err)
			_, found, err := reopenedSettlement.GetMaintenanceIntent(leaseUUID)
			require.NoError(t, err)
			assert.True(t, found, "the WAL must remain for exact restart recovery")
		})
	}
}

// TestRestart_ActiveProvisionsGauge verifies that the activeProvisions gauge
// is adjusted correctly across restart success and failure paths.

// TestUpdate_ActiveProvisionsGauge verifies that the activeProvisions gauge
// is adjusted correctly when an update image pull fails (preflight failure).

// --- ENG-230: prelude no longer speculatively writes prov.Status/CallbackURL ---

// TestRestart_RoutingFailureLeavesStatusUnchanged pins that the Restart
// prelude performs NO speculative write of prov.Status / prov.CallbackURL:
// when the handoff to the lease actor fails (here: backend shutting down,
// so routeToLeaseBlocking fails fast), the lease must remain exactly as it
// was. Pre-ENG-230 this invariant was upheld by restartRollback restoring
// the fields; post-ENG-230 it holds because the prelude never wrote them.
func TestRestart_RoutingFailureLeavesStatusUnchanged(t *testing.T) {
	const lifecycleID = "550e8400-e29b-41d4-a716-446655440000"
	oldOperationURL := "https://old.example/callbacks/provision?operation_id=" + lifecycleID
	oldLifecycleURL := "https://old.example/callbacks/provision?lifecycle_id=" + lifecycleID
	provisions := map[string]*provision{
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:            "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Status:               backend.ProvisionStatusReady,
			CallbackURL:          oldOperationURL,
			LifecycleCallbackURL: oldLifecycleURL,
			StackManifest:        &manifest.StackManifest{},
		}},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	// Cancel stopCtx so routeToLeaseBlocking fails fast with "backend
	// shutting down" before any actor work — exercising the routing-fail
	// error path that previously triggered restartRollback.
	b.stopCancel()

	err := b.Restart(context.Background(), backend.RestartRequest{MaintenanceID: newTestMaintenanceID(t),
		LeaseUUID:   "11638ef8-1401-4f14-a355-1ae02afeb35b",
		CallbackURL: "https://new.example/callbacks/provision?lifecycle_id=" + lifecycleID,
	})
	require.Error(t, err)

	b.provisionsMu.RLock()
	status := b.provisions["11638ef8-1401-4f14-a355-1ae02afeb35b"].Status
	callbackURL := b.provisions["11638ef8-1401-4f14-a355-1ae02afeb35b"].CallbackURL
	lifecycleCallbackURL := b.provisions["11638ef8-1401-4f14-a355-1ae02afeb35b"].LifecycleCallbackURL
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, status,
		"routing failure must leave Status unchanged (no speculative write)")
	assert.Equal(t, oldOperationURL, callbackURL,
		"routing failure must leave CallbackURL unchanged (no speculative write)")
	assert.Equal(t, oldLifecycleURL, lifecycleCallbackURL,
		"routing failure must leave LifecycleCallbackURL unchanged (no speculative write)")
}

func TestRestart_RotatesCallbackBaseWithoutRotatingTypedAuthority(t *testing.T) {
	callbackRequests := make(chan string, 4)
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		select {
		case callbackRequests <- r.URL.RequestURI():
		default:
		}
	}))
	defer callbackServer.Close()
	const lifecycleID = "550e8400-e29b-41d4-a716-446655440000"
	const leaseUUID = durableCallbackTestLeaseUUID2
	oldOperationURL := callbackServer.URL + "/old/callbacks/provision?operation_id=" + lifecycleID
	oldLifecycleURL := callbackServer.URL + "/old/callbacks/provision?lifecycle_id=" + lifecycleID
	newOperationURL := callbackServer.URL + "/new/callbacks/provision?operation_id=" + lifecycleID
	newLifecycleURL := callbackServer.URL + "/new/callbacks/provision?lifecycle_id=" + lifecycleID
	items := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}
	stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
		manifest.DefaultServiceName: {Image: "docker.io/library/nginx:latest"},
	}}
	resourceProfiles := testResourceProfiles(t, items)
	const providerUUID = "22222222-2222-4222-8222-222222222222"
	var replacementMaintenanceID shared.MaintenanceID

	provisions := map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:            leaseUUID,
			Tenant:               "tenant-a",
			ProviderUUID:         providerUUID,
			Status:               backend.ProvisionStatusReady,
			CallbackURL:          oldOperationURL,
			LifecycleCallbackURL: oldLifecycleURL,
			Items:                items,
			ResourceProfiles:     resourceProfiles,
			ContainerIDs:         []string{"old-container"},
			StackManifest:        stack,
		}},
	}
	dockerMock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{{
				ContainerID:          "new-container",
				LeaseUUID:            leaseUUID,
				Tenant:               "tenant-a",
				ProviderUUID:         providerUUID,
				BackendName:          "docker",
				SKU:                  "docker-small",
				ServiceName:          manifest.DefaultServiceName,
				InstanceIndex:        0,
				CallbackURL:          newOperationURL,
				LifecycleCallbackURL: newLifecycleURL,
				MaintenanceID:        replacementMaintenanceID,
				Image:                "docker.io/library/nginx:latest",
				Status:               "running",
				CreatedAt:            time.Now().Add(-time.Minute),
			}}, nil
		},
	}
	b := newBackendForTest(dockerMock, provisions)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	attachBoundOperationHandoffStores(t, b)
	releaseStore := b.releaseStore
	callbackStore := b.callbackStore
	defer func() {
		b.stopCancel()
		b.wg.Wait()
	}()
	b.releaseStore = releaseStore
	b.callbackStore = callbackStore
	rebuildCallbackSender(b, callbackServer.Client())
	b.wg.Go(b.callbackSender.RunReplayLoop)
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	oldAuthority, err := shared.NewReleaseRuntimeAuthority(
		mustDockerOperationID(lifecycleID),
		"tenant-a",
		providerUUID,
		oldOperationURL,
		oldLifecycleURL,
	)
	require.NoError(t, err)
	seedProvisionReleaseForBackendTest(t, b, leaseUUID, shared.Release{
		Manifest:         manifestBytes,
		Image:            "stack",
		OperationID:      mustDockerOperationID(lifecycleID),
		Items:            items,
		ResourceProfiles: resourceProfiles,
		RuntimeAuthority: &oldAuthority,
		Status:           "active",
		CreatedAt:        time.Now(),
	})

	projectReady := make(chan *composetypes.Project, 1)
	releaseWorker := make(chan struct{})
	b.compose = &mockComposeExecutor{
		UpFn: func(_ context.Context, project *composetypes.Project, _ composeUpOpts) error {
			replacementMaintenanceID = mustParseMaintenanceID(
				t, project.Services[manifest.DefaultServiceName].Labels[LabelMaintenanceID],
			)
			projectReady <- project
			<-releaseWorker
			return nil
		},
		PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{{
				ID: "new-container", Service: manifest.DefaultServiceName, State: "running",
			}}, nil
		},
	}

	require.NoError(t, b.Restart(context.Background(), backend.RestartRequest{MaintenanceID: newTestMaintenanceID(t),
		LeaseUUID:   leaseUUID,
		CallbackURL: newLifecycleURL,
	}))

	b.provisionsMu.RLock()
	assert.Equal(t, oldOperationURL, b.provisions[leaseUUID].CallbackURL)
	assert.Equal(t, oldLifecycleURL, b.provisions[leaseUUID].LifecycleCallbackURL,
		"accepted restart must keep the runtime route pending until replacement commits")
	b.provisionsMu.RUnlock()

	var project *composetypes.Project
	select {
	case project = <-projectReady:
	case <-time.After(2 * time.Second):
		t.Fatal("restart worker did not build the replacement project")
	}
	require.Contains(t, project.Services, manifest.DefaultServiceName)
	labels := project.Services[manifest.DefaultServiceName].Labels
	assert.Equal(t, newOperationURL, labels[LabelCallbackURL])
	assert.Equal(t, newLifecycleURL, labels[LabelLifecycleCallbackURL],
		"recreated containers must persist the same authority at the current callback base")

	close(releaseWorker)
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		return b.provisions[leaseUUID].Status == backend.ProvisionStatusReady &&
			b.provisions[leaseUUID].CallbackURL == newOperationURL &&
			b.provisions[leaseUUID].LifecycleCallbackURL == newLifecycleURL
	}, 2*time.Second, 5*time.Millisecond)
	wantCallbackRequestURI := "/new/callbacks/provision?lifecycle_id=" + lifecycleID
	callbackDeadline := time.NewTimer(2 * time.Second)
	defer callbackDeadline.Stop()
	callbackDelivered := false
	for !callbackDelivered {
		select {
		case callbackRequestURI := <-callbackRequests:
			if callbackRequestURI == wantCallbackRequestURI {
				callbackDelivered = true
			}
		case <-callbackDeadline.C:
			t.Fatal("restart completion callback was not delivered")
		}
	}

	active, err := releaseStore.LatestActive(leaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.NotNil(t, active.RuntimeAuthority)
	assert.Equal(t, newOperationURL, active.RuntimeAuthority.CallbackURL())
	assert.Equal(t, newLifecycleURL, active.RuntimeAuthority.LifecycleCallbackURL())

	// Cold recovery must read the same route from the active Release and the
	// replacement labels; a successful base move cannot make the cohort appear
	// split-brained on the next process start.
	coldContainer := ContainerInfo{
		ContainerID:          "new-container",
		LeaseUUID:            leaseUUID,
		Tenant:               "tenant-a",
		ProviderUUID:         providerUUID,
		SKU:                  "docker-small",
		ServiceName:          manifest.DefaultServiceName,
		InstanceIndex:        0,
		CallbackURL:          newOperationURL,
		LifecycleCallbackURL: newLifecycleURL,
		MaintenanceID:        replacementMaintenanceID,
		Image:                "docker.io/library/nginx:latest",
		Status:               "running",
		CreatedAt:            time.Now().Add(-time.Minute),
	}
	cold := newBackendForTest(&mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{coldContainer}, nil
		},
		InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
			copy := coldContainer
			return &copy, nil
		},
	}, nil)
	defer cold.stopCancel()
	cold.releaseStore = releaseStore
	require.NoError(t, cold.recoverState(context.Background()))
	cold.provisionsMu.RLock()
	recovered := cold.provisions[leaseUUID]
	cold.provisionsMu.RUnlock()
	require.NotNil(t, recovered)
	assert.Equal(t, backend.ProvisionStatusReady, recovered.Status,
		"cold recovery must accept the exact maintenance generation")
	assert.Equal(t, newOperationURL, recovered.CallbackURL)
	assert.Equal(t, newLifecycleURL, recovered.LifecycleCallbackURL)
}

func TestRestart_RejectsLifecycleAuthorityMismatchWithoutMutation(t *testing.T) {
	const (
		currentID = "550e8400-e29b-41d4-a716-446655440000"
		otherID   = "123e4567-e89b-42d3-a456-426614174000"
	)
	oldOperationURL := "https://old.example/callbacks/provision?operation_id=" + currentID
	oldLifecycleURL := "https://old.example/callbacks/provision?lifecycle_id=" + currentID
	provisions := map[string]*provision{
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:            "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Status:               backend.ProvisionStatusReady,
			CallbackURL:          oldOperationURL,
			LifecycleCallbackURL: oldLifecycleURL,
			StackManifest:        &manifest.StackManifest{},
		}},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Restart(context.Background(), backend.RestartRequest{MaintenanceID: newTestMaintenanceID(t),
		LeaseUUID:   "11638ef8-1401-4f14-a355-1ae02afeb35b",
		CallbackURL: "https://new.example/callbacks/provision?lifecycle_id=" + otherID,
	})
	require.ErrorIs(t, err, backend.ErrValidation)

	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, b.provisions["11638ef8-1401-4f14-a355-1ae02afeb35b"].Status)
	assert.Equal(t, oldOperationURL, b.provisions["11638ef8-1401-4f14-a355-1ae02afeb35b"].CallbackURL)
	assert.Equal(t, oldLifecycleURL, b.provisions["11638ef8-1401-4f14-a355-1ae02afeb35b"].LifecycleCallbackURL)
}

// blockingDiagGatherer pins a lease in Failing for the duration of a test by
// blocking GatherDiagnostics until the diag context is cancelled — which
// onExitFailing does on any transition out of Failing. Returning "" then lets
// gatherDiagAsync's Canceled path suppress the diagGatheredMsg, so the lease
// stays in Failing instead of racing to Failed.
type blockingDiagGatherer struct{}

func (blockingDiagGatherer) GatherDiagnostics(ctx context.Context, _ string, _ *leasesm.InstanceState) string {
	<-ctx.Done()
	return ""
}

// newActorMaintenanceClaim gives direct actor tests the same bound durable
// capability that Restart and Update construct before routing in production.
// Keeping the fixture on the real stores prevents these tests from weakening
// LeaseActor validation with a test-only escape hatch.
func newActorMaintenanceTarget(
	t *testing.T,
	b *Backend,
	leaseUUID string,
	kind shared.MaintenanceIntentKind,
) shared.MaintenanceReleaseClaim {
	t.Helper()
	attachBoundOperationHandoffStores(t, b)

	operationID, callbackURL, lifecycleCallbackURL := newTestRestoreCallbackAuthority(t)
	items := []backend.LeaseItem{{
		SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
	}}
	profiles := testResourceProfiles(t, items)
	stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
		manifest.DefaultServiceName: {Image: "docker.io/library/nginx:1.27"},
	}}
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	authority := mustTestReleaseRuntimeAuthority(
		t,
		operationID,
		"tenant-a",
		"22222222-2222-4222-8222-222222222222",
		callbackURL,
		lifecycleCallbackURL,
	)
	source := shared.Release{
		Manifest:         manifestBytes,
		Image:            "stack",
		OperationID:      operationID,
		Items:            items,
		ResourceProfiles: profiles,
		RuntimeAuthority: authority,
		Status:           "active",
		CreatedAt:        time.Now(),
	}
	settlement := b.maintenanceSettlement
	active, sourceClaim, err := settlement.ClaimLatestActive(leaseUUID)
	if err != nil {
		seedProvisionReleaseForBackendTest(t, b, leaseUUID, source)
		_, sourceClaim, err = settlement.ClaimLatestActive(leaseUUID)
		require.NoError(t, err)
	} else {
		// An observation test may already have installed the exact runtime
		// generation needed to authorize a container-death event. Reuse that
		// store-issued source rather than manufacturing a second active
		// generation merely to route the following maintenance command.
		source = active
		identity, ok := source.RuntimeIdentity()
		require.True(t, ok)
		lifecycleCallbackURL = identity.LifecycleCallbackURL()
	}
	target := source
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()
	requestPayload := []byte(nil)
	if kind != shared.MaintenanceIntentRestart {
		requestPayload = manifestBytes
	}
	request, err := settlement.NewMaintenanceRequestAuthority(
		newTestMaintenanceID(t), kind, leaseUUID, lifecycleCallbackURL,
		requestPayload,
	)
	require.NoError(t, err)

	candidate, err := settlement.NewMaintenanceIntentCandidate(request, sourceClaim, target)
	require.NoError(t, err)
	admission, err := settlement.BeginMaintenanceIntent(candidate)
	require.NoError(t, err)
	appendClaim, err := settlement.StartMaintenanceAppend(
		createdTestMaintenanceDispatch(t, admission),
	)
	require.NoError(t, err)
	targetClaim, err := settlement.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	targetClaim, err = settlement.BindMaintenanceIntentTarget(targetClaim)
	require.NoError(t, err)
	return targetClaim
}

// TestContainerDiedThenRestart_Succeeds is the ENG-230 §6.3(c) matrix case:
// it proves the ContainerDied-races-Restart case is resolved by the actor's
// serial message ordering, NOT by the (now-removed) onEnterFailing Status
// recheck. A container death drives Ready→Failing (onEnterFailing runs to
// completion: Status=Failing, FailCount=1); a subsequent restart then SUCCEEDS
// via Failing.Permit(evRestartRequested) — Failing intentionally permits
// restart retries — driving Failing→Restarting with onEnterRestarting writing
// Status=Restarting and exactly one replace worker spawned. (Contrast with
// §6.3(a)/(b): a restart from Restarting/Updating/Deprovisioning is rejected
// 409; from Failing it succeeds.)
func TestContainerDiedThenRestart_Succeeds(t *testing.T) {
	const leaseUUID = durableCallbackTestLeaseUUID
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer callbackServer.Close()

	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			// Guard sees a terminally-exited container → Ready→Failing.
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
	}
	provisions := map[string]*provision{
		leaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:    leaseUUID,
			Tenant:       "tenant-a",
			Status:       backend.ProvisionStatusReady,
			ContainerIDs: []string{"c1"},
			CallbackURL:  callbackServer.URL + "/callbacks/provision",
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
				manifest.DefaultServiceName: {Image: "busybox"},
			}},
		}},
	}
	b := newBackendForTest(mock, provisions)
	defer b.stopCancel()
	installReadyRuntimeProofForTest(t, b, leaseUUID)
	// Inject the blocking gatherer BEFORE the actor is created (first
	// routeToLease below) so the diag goroutine spawned by onEnterFailing
	// can't fire diagGatheredMsg and flip Failing→Failed before the restart.
	b.gatherer = blockingDiagGatherer{}

	// activeProvisions gauge baseline (captured while the lease is Ready, but
	// note the seed does NOT itself Inc — only entry actions move the gauge).
	// The death below Dec's it (-1) and a successful restart must re-Inc it
	// back, netting to activeBefore. This is the ENG-230 gauge-drift guard
	// (Copilot PR #93 finding #2): before the fix, the re-Inc was gated on a
	// stale prelude route-time snapshot and skipped, leaving the gauge at activeBefore-1.
	activeBefore := testutil.ToFloat64(activeProvisions)

	// 1) Container death: Ready→Failing, onEnterFailing runs to completion.
	b.handleContainerDeath("c1")
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		return b.provisions[leaseUUID].Status == backend.ProvisionStatusFailing
	}, time.Second, time.Millisecond)

	b.provisionsMu.RLock()
	statusAfterDeath := b.provisions[leaseUUID].Status
	failCount := b.provisions[leaseUUID].FailCount
	b.provisionsMu.RUnlock()
	require.Equal(t, backend.ProvisionStatusFailing, statusAfterDeath,
		"container death must drive the lease to Failing")
	require.Equal(t, 1, failCount, "onEnterFailing must bump FailCount to 1")
	require.Equal(t, activeBefore-1, testutil.ToFloat64(activeProvisions),
		"Ready→Failing must Dec activeProvisions by 1")

	// 2) Restart: route directly to the actor. (The b.Restart prelude
	// fast-fails a Failing lease, but the SM permits Failing→Restarting —
	// exactly the serial-ordering path this test exercises.)
	workerRelease := make(chan struct{})
	workerStarted := make(chan struct{}, 1)
	target := newActorMaintenanceTarget(t, b, leaseUUID, shared.MaintenanceIntentRestart)
	cleanup := registerMaintenanceExecutionForTest(
		t, b.maintenanceSettlement, target,
		maintenanceSeedTargetReady, workerStarted, workerRelease,
	)
	defer cleanup()
	command, reply, err := leasesm.NewRestartCommand(t.Context(), target)
	require.NoError(t, err)
	require.True(t, b.routeToLease(leaseUUID, command))

	select {
	case err := <-reply.Result():
		require.NoError(t, err, "Failing→Restarting must be accepted by the SM (Permit, not Ignore)")
	case <-time.After(2 * time.Second):
		t.Fatal("no ack from handleRestartRequested")
	}
	select {
	case <-workerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("construction-bound maintenance worker did not start")
	}

	b.provisionsMu.RLock()
	statusAfterRestart := b.provisions[leaseUUID].Status
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusRestarting, statusAfterRestart,
		"restart from Failing must end in Restarting via serial ordering (no recheck)")
	assert.Equal(t, backend.ProvisionStatusRestarting, b.actorFor(leaseUUID).State(),
		"SM must be in Restarting with the replace worker spawned")

	close(workerRelease) // let the worker complete so the SM reaches Ready
	require.Eventually(t, func() bool { return b.actorFor(leaseUUID).State() == backend.ProvisionStatusReady },
		2*time.Second, 5*time.Millisecond,
		"lease must reach Ready after the restart worker completes")
	select {
	case <-workerStarted:
		t.Fatal("more than one maintenance worker was spawned")
	default:
	}
	assert.Equal(t, activeBefore, testutil.ToFloat64(activeProvisions),
		"activeProvisions must net to its pre-death value: the Ready→Failing Dec must be "+
			"balanced by a re-Inc when the restart returns the lease to Ready (gauge-drift fix)")
}

// TestUpdate_RoutingFailureLeavesStatusUnchanged is the Update mirror of
// TestRestart_RoutingFailureLeavesStatusUnchanged.
func TestUpdate_RoutingFailureLeavesStatusUnchanged(t *testing.T) {
	const lifecycleID = "550e8400-e29b-41d4-a716-446655440000"
	oldOperationURL := "https://old.example/callbacks/provision?operation_id=" + lifecycleID
	oldLifecycleURL := "https://old.example/callbacks/provision?lifecycle_id=" + lifecycleID
	provisions := map[string]*provision{
		"11638ef8-1401-4f14-a355-1ae02afeb35b": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:            "11638ef8-1401-4f14-a355-1ae02afeb35b",
			Status:               backend.ProvisionStatusReady,
			SKU:                  "docker-small",
			CallbackURL:          oldOperationURL,
			LifecycleCallbackURL: oldLifecycleURL,
			Items:                []backend.LeaseItem{{SKU: "docker-small", Quantity: 1}},
		}},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	b.stopCancel()

	err := b.Update(context.Background(), backend.UpdateRequest{MaintenanceID: newTestMaintenanceID(t),
		LeaseUUID:   "11638ef8-1401-4f14-a355-1ae02afeb35b",
		CallbackURL: "https://new.example/callbacks/provision?lifecycle_id=" + lifecycleID,
		Payload:     validManifestJSON("nginx:latest"),
	})
	require.Error(t, err)

	b.provisionsMu.RLock()
	status := b.provisions["11638ef8-1401-4f14-a355-1ae02afeb35b"].Status
	callbackURL := b.provisions["11638ef8-1401-4f14-a355-1ae02afeb35b"].CallbackURL
	lifecycleCallbackURL := b.provisions["11638ef8-1401-4f14-a355-1ae02afeb35b"].LifecycleCallbackURL
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, status,
		"routing failure must leave Status unchanged (no speculative write)")
	assert.Equal(t, oldOperationURL, callbackURL,
		"routing failure must leave CallbackURL unchanged (no speculative write)")
	assert.Equal(t, oldLifecycleURL, lifecycleCallbackURL,
		"routing failure must leave LifecycleCallbackURL unchanged (no speculative write)")
}

// --- ENG-230 / PR #93: restart-preflight recovered-vs-failed is derived by
// the ACTOR from its serial replaceWasActive (via the RecoveredIfSourceActive
// flag that doRestart sets), NOT a stale prelude status snapshot ---
//
// A restart that fails SKU-profile preflight touches NO containers, so the
// lease is left exactly in its replace-start state. "Recovered to Ready" is
// correct iff its containers were running at replace-start — i.e. iff the SM
// source was Ready (== wasActive). Cases (1)–(4) pin the actor MAPPING (a
// stub Work returning a preflight ReplaceResult, routed directly to the
// actor); case (5) pins doRestart SETTING the flag. (1)+(5) together cover
// both halves.

// preflightCallbackServer returns a 200-OK callback server (cleaned up via t).
func preflightCallbackServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// newPreflightBackend builds a backend with a single canonical lease seeded at
// the given SM source status, wired to a 200 callback server.
func newPreflightBackend(t *testing.T, mock *mockDockerClient, source backend.ProvisionStatus) *Backend {
	t.Helper()
	srv := preflightCallbackServer(t)
	provisions := map[string]*provision{
		durableCallbackTestLeaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:    durableCallbackTestLeaseUUID,
			Tenant:       "tenant-a",
			Status:       source,
			ContainerIDs: []string{"c1"},
			CallbackURL:  srv.URL + "/callbacks/provision",
			StackManifest: &manifest.StackManifest{Services: map[string]*manifest.Manifest{
				manifest.DefaultServiceName: {Image: "busybox"},
			}},
			Items: []backend.LeaseItem{{
				SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName,
			}},
		}},
	}
	b := newBackendForTest(mock, provisions)
	t.Cleanup(b.stopCancel)
	return b
}

// routeRestartFailure routes a typed maintenance failure directly to the
// fixture actor. Terminal status and operation kind come from the exact failed
// target proof rather than caller-writable result fields.
func routeRestartFailure(
	t *testing.T,
	b *Backend,
	restored, recoverFromSource, oldStopped bool,
) {
	t.Helper()
	target := newActorMaintenanceTarget(t, b, durableCallbackTestLeaseUUID, shared.MaintenanceIntentRestart)
	b.provisionsMu.RLock()
	status := b.provisions[durableCallbackTestLeaseUUID].Status
	b.provisionsMu.RUnlock()
	// The construction-bound classifier, rather than a caller-authored result,
	// decides whether the exact source cohort was recovered. `oldStopped` is
	// retained in this helper's call shape only to keep the historical gauge
	// matrix readable; physical evidence supersedes that writable flag.
	_ = oldStopped
	kind := maintenanceSeedAbsent
	if restored || (recoverFromSource && status == backend.ProvisionStatusReady) {
		kind = maintenanceSeedSourceReady
	}
	cleanup := registerMaintenanceExecutionForTest(
		t, b.maintenanceSettlement, target, kind, nil, nil,
	)
	// Actor acknowledgement means the command was accepted, not that its
	// asynchronous worker has crossed and classified the substrate boundary.
	// Keep the exact physical-evidence plan alive for the worker's lifetime.
	t.Cleanup(cleanup)
	command, reply, err := leasesm.NewRestartCommand(t.Context(), target)
	require.NoError(t, err)
	require.True(t, b.routeToLease(durableCallbackTestLeaseUUID, command))
	require.NoError(t, <-reply.Result(), "restart must be accepted by the SM")
}

// awaitSettled waits for the fixture lease to settle at (status, failCount).
func awaitSettled(t *testing.T, b *Backend, status backend.ProvisionStatus, failCount int, msg string) {
	t.Helper()
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		p := b.provisions[durableCallbackTestLeaseUUID]
		return p.Status == status && p.FailCount == failCount
	}, 3*time.Second, 10*time.Millisecond, msg)
}

// (2) source=Ready (wasActive=true): restart preflight failure → recovered →
// stays Ready (containers were never touched). The FailCount=1 gate
// distinguishes the settled recovered state from the initial Ready.
func TestRestartPreflight_FromReady_Recovers(t *testing.T) {
	b := newPreflightBackend(t, &mockDockerClient{}, backend.ProvisionStatusReady)
	activeBefore := testutil.ToFloat64(activeProvisions)
	routeRestartFailure(t, b, false, true, false)
	awaitSettled(t, b, backend.ProvisionStatusReady, 1,
		"restart preflight from Ready (wasActive=true) must recover→Ready")
	// Gauge (FIX #1c): the lease was active (Ready) at replace-start and ends
	// Ready, so activeProvisions must be UNCHANGED — catches a spurious Inc.
	assert.Equal(t, activeBefore, testutil.ToFloat64(activeProvisions),
		"recovered-from-active (Ready→Ready) must NOT change activeProvisions")
}

// (3) source=Failed (wasActive=false): restart preflight failure → failed →
// stays Failed.
func TestRestartPreflight_FromFailed_StaysFailed(t *testing.T) {
	b := newPreflightBackend(t, &mockDockerClient{}, backend.ProvisionStatusFailed)
	routeRestartFailure(t, b, false, true, false)
	awaitSettled(t, b, backend.ProvisionStatusFailed, 1,
		"restart preflight from Failed (wasActive=false) must stay Failed")
}

// (4) update preflight failure → stays Failed regardless of source. The
// update stub sets Restored:false and does NOT set the flag — the intentional
// update asymmetry (a missed image pull never achieved the desired state).
func TestUpdatePreflight_StaysFailed(t *testing.T) {
	b := newPreflightBackend(t, &mockDockerClient{}, backend.ProvisionStatusReady)
	activeBefore := testutil.ToFloat64(activeProvisions)
	target := newActorMaintenanceTarget(t, b, durableCallbackTestLeaseUUID, shared.MaintenanceIntentUpdate)
	cleanup := registerMaintenanceExecutionForTest(
		t, b.maintenanceSettlement, target, maintenanceSeedAbsent, nil, nil,
	)
	defer cleanup()
	command, reply, err := leasesm.NewUpdateCommand(t.Context(), target)
	require.NoError(t, err)
	require.True(t, b.routeToLease(durableCallbackTestLeaseUUID, command))
	require.NoError(t, <-reply.Result(), "update must be accepted by the SM")
	awaitSettled(t, b, backend.ProvisionStatusFailed, 1,
		"update preflight must stay Failed regardless of source (no flag; intentional asymmetry)")
	// Gauge (FIX #1a): the lease was active (Ready) at replace-start and ends
	// Failed, so activeProvisions must be decremented by 1.
	assert.Equal(t, activeBefore-1, testutil.ToFloat64(activeProvisions),
		"update preflight from Ready (active) → Failed must Dec activeProvisions by 1")
}

// (b) recovered-from-non-active via POST-replace rollback (NOT preflight):
// a restart from Failed whose replace fails but whose rollback restores the
// lease to Ready must INCREMENT activeProvisions (the lease was not counted at
// replace-start and now ends Ready). This path previously did no gauge op.
// Restored:true with NO RecoveredIfSourceActive flag drives the recovered
// outcome via result.Restored (post-replace rollback semantics), distinct
// from the preflight cases above.
func TestRestartRecovered_FromFailed_IncsGauge(t *testing.T) {
	b := newPreflightBackend(t, &mockDockerClient{}, backend.ProvisionStatusFailed)
	activeBefore := testutil.ToFloat64(activeProvisions)
	routeRestartFailure(t, b, true, false, true)
	awaitSettled(t, b, backend.ProvisionStatusReady, 1,
		"recovered-from-Failed (rollback restored Ready) must settle Ready with FailCount=1")
	// Gauge (FIX #1b): the lease was NOT active (Failed) at replace-start and
	// ends Ready, so activeProvisions must be incremented by 1.
	assert.Equal(t, activeBefore+1, testutil.ToFloat64(activeProvisions),
		"recovered-from-non-active (Failed→Ready) must Inc activeProvisions by 1")
}

// (1) source=Failing (death-then-restart) — the latent-bug case, RED→GREEN.
// Death lands first (Ready→Failing, FailCount=1), then a restart whose
// preflight failure carries RecoveredIfSourceActive. The actor maps
// recovered=wasActive (source=Failing → false) → the lease must end FAILED,
// NOT wrongly recovered to Ready with dead containers.
//
// This is a TEST-FILE red→green (no production toggle):
//   - GREEN (committed): stub = restartPreflightResult(true)  [flag set]   → Failed.
//   - RED:  stub = leasesm.ReplaceResult{Err:…, Restored:true} [old output] → actor
//     recovers→Ready → the assert-Failed below FAILS.
func TestContainerDiedThenRestartPreflight_EndsFailed(t *testing.T) {
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "exited", ExitCode: 1}, nil
		},
	}
	b := newPreflightBackend(t, mock, backend.ProvisionStatusReady)
	installReadyRuntimeProofForTest(t, b, durableCallbackTestLeaseUUID)
	// Pin the lease in Failing (block the diag goroutine) so the restart is
	// processed from SM source = Failing, not Failed.
	b.gatherer = blockingDiagGatherer{}

	b.handleContainerDeath("c1")
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		return b.provisions[durableCallbackTestLeaseUUID].Status == backend.ProvisionStatusFailing
	}, time.Second, time.Millisecond)
	b.provisionsMu.RLock()
	require.Equal(t, backend.ProvisionStatusFailing, b.provisions[durableCallbackTestLeaseUUID].Status,
		"container death must drive the lease to Failing")
	b.provisionsMu.RUnlock()

	routeRestartFailure(t, b, false, true, false)
	awaitSettled(t, b, backend.ProvisionStatusFailed, 2,
		"death-then-restart + preflight (source Failing) must end FAILED, not wrongly recovered to Ready")
}

// TestDoUpdate_PreflightFailure_ReasonIsImagePullFailed verifies that an
// image-pull preflight failure in doUpdate authors the SPECIFIC
// ReasonImagePullFailed on the ReplaceFailureInfo — not the generic
// ReasonUpdateFailed — so tenants see the precise failure category (ENG-508).
func TestDoUpdate_PreflightFailure_ReasonIsImagePullFailed(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(ctx context.Context, imageName string, timeout time.Duration) error {
			return fmt.Errorf("manifest unknown: no such image %s", imageName)
		},
	}
	b := newBackendForTest(mock, nil)
	defer b.stopCancel()
	b.cfg.Name = "docker-a"

	stack := &manifest.StackManifest{
		Services: map[string]*manifest.Manifest{
			"web": {Image: "nginx:latest"},
		},
	}
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "web"}}
	resourceProfiles := testResourceProfiles(t, items)
	leaseUUID := uuid.NewString()
	operationID, callbackURL, lifecycleCallbackURL := newTestRestoreCallbackAuthority(t)
	authority := mustTestReleaseRuntimeAuthority(
		t, operationID, "tenant-a", "22222222-2222-4222-8222-222222222222",
		callbackURL, lifecycleCallbackURL,
	)
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	attachBoundOperationHandoffStores(t, b)
	seedProvisionReleaseForBackendTest(t, b, leaseUUID, shared.Release{
		Manifest:         manifestBytes,
		Image:            "stack",
		OperationID:      operationID,
		Items:            items,
		ResourceProfiles: resourceProfiles,
		RuntimeAuthority: authority,
		Status:           "active",
		CreatedAt:        time.Now(),
	})
	active, sourceClaim, err := b.maintenanceSettlement.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	request, err := b.maintenanceSettlement.NewMaintenanceRequestAuthority(
		mustParseMaintenanceID(t, uuid.NewString()), shared.MaintenanceIntentUpdate,
		leaseUUID, lifecycleCallbackURL, manifestBytes,
	)
	require.NoError(t, err)
	admission, err := b.admitMaintenance(request, sourceClaim, shared.Release{
		Manifest:         manifestBytes,
		Image:            "stack",
		OperationID:      operationID,
		Items:            items,
		ResourceProfiles: resourceProfiles,
		RuntimeAuthority: authority,
		Status:           "deploying",
		CreatedAt:        time.Now(),
	})
	require.NoError(t, err)
	require.True(t, admission.created())
	b.provisionsMu.Lock()
	b.provisions[leaseUUID] = &provision{ProvisionState: leasesm.ProvisionState{
		LeaseUUID: leaseUUID, Tenant: "tenant-a",
		ProviderUUID: "22222222-2222-4222-8222-222222222222",
		Status:       backend.ProvisionStatusReady, CallbackURL: callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL, ActiveOperationID: operationID,
		ActiveReleaseVersion: active.Version, Items: slices.Clone(items),
		ResourceProfiles: shared.CloneSKUResourceSnapshot(resourceProfiles), StackManifest: stack,
	}}
	b.provisionsMu.Unlock()
	command, reply, err := leasesm.NewUpdateCommand(t.Context(), admission.target)
	require.NoError(t, err)
	require.True(t, b.routeToLease(leaseUUID, command))
	require.NoError(t, <-reply.Result())
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		return b.provisions[leaseUUID].Status == backend.ProvisionStatusFailed
	}, 2*time.Second, time.Millisecond)
	b.provisionsMu.RLock()
	result := b.provisions[leaseUUID].ProvisionState
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ReasonImagePullFailed, result.Reason,
		"construction-bound update image pull must retain the specific failure reason")
	assert.NotEqual(t, backend.ReasonUpdateFailed, result.Reason,
		"image pull refusal must not be misclassified as a generic update failure")
	assert.Equal(t, backend.MsgImagePullFailed, result.Message,
		"callback message and typed reason must be derived from the same physical error")
}
