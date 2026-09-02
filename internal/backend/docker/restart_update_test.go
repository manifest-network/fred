package docker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"sync/atomic"
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

func TestUpdate_RejectsFixedHostPort(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Status: backend.ProvisionStatusReady,
			SKU:    "docker-small",
			Items:  []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}},
		},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	// A tenant must not be able to introduce a squatted fixed host port via
	// an update (ENG-605), just as at provision time.
	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID: "lease-1",
		Payload:   []byte(`{"image":"docker.io/library/nginx:latest","ports":{"8080/tcp":{"host_port":8080}}}`),
	})
	require.ErrorIs(t, err, backend.ErrInvalidManifest)
	assert.Contains(t, err.Error(), "host_port")
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
	rebuildCallbackSender(b, callbackServer.Client())
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	req := newProvisionRequest("lease-1", "tenant-a", "docker-small", 1, validManifestJSON("nginx:latest"))
	req.CallbackURL = callbackServer.URL + "/callbacks/provision"

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
	assert.Equal(t, req.Items, releases[0].Items)
}

func TestDoReplace_ActivationPersistenceFailurePreservesTargetForRecovery(t *testing.T) {
	for _, operation := range []string{"restart", "update"} {
		t.Run(operation, func(t *testing.T) {
			leaseUUID := uuid.NewString()
			operationID := shared.OperationID(uuid.NewString())
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
				InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
					return &ContainerInfo{ContainerID: "new", Status: "running"}, nil
				},
				ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
					return slices.Clone(strictContainers), nil
				},
			}
			b := newBackendForTest(mock, provisions)
			defer b.stopCancel()

			store, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "activate-failure.db"),
			})
			require.NoError(t, err)
			callbackStore, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
				DBPath: filepath.Join(t.TempDir(), "activate-failure-callbacks.db"),
			})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, callbackStore.Close()) })

			sourceAuthority := mustTestReleaseRuntimeAuthority(
				t, operationID, "tenant-a", providerUUID, oldCallbackURL, oldLifecycleURL,
			)
			targetAuthority := mustTestReleaseRuntimeAuthority(
				t, operationID, "tenant-a", providerUUID, newCallbackURL, newLifecycleURL,
			)
			require.NoError(t, store.AppendActive(leaseUUID, shared.Release{
				Manifest: []byte(`{"services":{"app":{"image":"docker.io/library/nginx:1.26"}}}`),
				Image:    "stack", OperationID: operationID, Items: items,
				ResourceProfiles: resourceProfiles, RuntimeAuthority: sourceAuthority,
				Status: "active", CreatedAt: time.Now().Add(-time.Minute),
			}))
			active, sourceClaim, err := store.ClaimLatestActive(leaseUUID)
			require.NoError(t, err)
			source, err := newReplaceSourceSnapshot(active)
			require.NoError(t, err)
			kind := shared.MaintenanceIntentRestart
			if operation == "update" {
				kind = shared.MaintenanceIntentUpdate
			}
			admission, err := callbackStore.BeginMaintenanceIntent(shared.MaintenanceIntentSpec{
				Kind:          kind,
				SourceRelease: sourceClaim,
				TargetRelease: shared.Release{
					Manifest: []byte(`{"services":{"app":{"image":"docker.io/library/nginx:1.27"}}}`),
					Image:    "stack", OperationID: operationID, Items: items,
					ResourceProfiles: resourceProfiles, RuntimeAuthority: targetAuthority,
					Status: "deploying", CreatedAt: time.Now(),
				},
				Backend:          b.Name(),
				BackendStorageID: b.storageIdentity,
			})
			require.NoError(t, err)
			appendClaim, err := callbackStore.StartMaintenanceAppend(admission)
			require.NoError(t, err)
			targetClaim, err := store.AppendMaintenance(appendClaim)
			require.NoError(t, err)
			maintenance, err := callbackStore.BindMaintenanceIntentTarget(
				appendClaim.Intent(), targetClaim,
			)
			require.NoError(t, err)
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
			require.NoError(t, store.Close(), "inject exact activation persistence failure")

			upCalls := 0
			var projects []*composetypes.Project
			b.releaseStore = store
			b.compose = &mockComposeExecutor{
				UpFn: func(_ context.Context, project *composetypes.Project, _ composeUpOpts) error {
					upCalls++
					projects = append(projects, project)
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
			result := b.doReplaceContainers(context.Background(), replaceContainersOp{
				LeaseUUID: leaseUUID, Stack: stack, Items: items,
				ResourceProfiles:     resourceProfiles,
				Operation:            operation,
				CallbackURL:          newCallbackURL,
				LifecycleCallbackURL: newLifecycleURL,
				Maintenance:          maintenance,
				TargetRelease:        targetClaim,
				Source:               source,
				TargetMaintenanceID:  maintenance.MaintenanceID(),
				Logger:               b.logger,
			})
			require.Error(t, result.Err)
			assert.Contains(t, result.Err.Error(), "activate successful "+operation+" release")
			assert.False(t, result.Restored,
				"an ambiguous activation outcome must preserve the exact target substrate for recovery")
			assert.True(t, result.Failure.PreserveMaintenance)
			assert.Empty(t, result.Success.ContainerIDs, "release activation failure must not report replace success")
			assert.Equal(t, 1, upCalls, "recovery, not an unsafe rollback, owns an ambiguous activation outcome")
			require.Len(t, projects, 1)
			assert.Equal(t,
				newLifecycleURL,
				projects[0].Services["app"].Labels[LabelLifecycleCallbackURL],
				"replacement cohort must receive the pending route",
			)
			_, found, err := callbackStore.GetMaintenanceIntent(leaseUUID)
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
		"lease-1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:            "lease-1",
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

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: "https://new.example/callbacks/provision?lifecycle_id=" + lifecycleID,
	})
	require.Error(t, err)

	b.provisionsMu.RLock()
	status := b.provisions["lease-1"].Status
	callbackURL := b.provisions["lease-1"].CallbackURL
	lifecycleCallbackURL := b.provisions["lease-1"].LifecycleCallbackURL
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, status,
		"routing failure must leave Status unchanged (no speculative write)")
	assert.Equal(t, oldOperationURL, callbackURL,
		"routing failure must leave CallbackURL unchanged (no speculative write)")
	assert.Equal(t, oldLifecycleURL, lifecycleCallbackURL,
		"routing failure must leave LifecycleCallbackURL unchanged (no speculative write)")
}

func TestRestart_RotatesCallbackBaseWithoutRotatingTypedAuthority(t *testing.T) {
	var callbackRequestURI string
	callbackReceived := make(chan struct{})
	callbackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbackRequestURI = r.URL.RequestURI()
		w.WriteHeader(http.StatusOK)
		select {
		case <-callbackReceived:
		default:
			close(callbackReceived)
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
			ContainerIDs:         []string{"old-container"},
			StackManifest:        stack,
		}, ResourceProfiles: resourceProfiles},
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
			}}, nil
		},
	}
	b := newBackendForTest(dockerMock, provisions)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	dir := t.TempDir()
	releaseStore, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(dir, "typed-base-move-releases.db"),
	})
	require.NoError(t, err)
	callbackStore, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(dir, "typed-base-move-callbacks.db"),
	})
	require.NoError(t, err)
	defer callbackStore.Close()
	defer releaseStore.Close()
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
		shared.OperationID(lifecycleID),
		"tenant-a",
		providerUUID,
		oldOperationURL,
		oldLifecycleURL,
	)
	require.NoError(t, err)
	require.NoError(t, releaseStore.AppendActive(leaseUUID, shared.Release{
		Manifest:         manifestBytes,
		Image:            "stack",
		OperationID:      shared.OperationID(lifecycleID),
		Items:            items,
		ResourceProfiles: resourceProfiles,
		RuntimeAuthority: &oldAuthority,
		Status:           "active",
		CreatedAt:        time.Now(),
	}))

	projectReady := make(chan *composetypes.Project, 1)
	releaseWorker := make(chan struct{})
	b.compose = &mockComposeExecutor{
		UpFn: func(_ context.Context, project *composetypes.Project, _ composeUpOpts) error {
			replacementMaintenanceID = shared.MaintenanceID(
				project.Services[manifest.DefaultServiceName].Labels[LabelMaintenanceID],
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

	require.NoError(t, b.Restart(context.Background(), backend.RestartRequest{
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
	select {
	case <-callbackReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("restart completion callback was not delivered")
	}
	assert.Equal(t, "/new/callbacks/provision?lifecycle_id="+lifecycleID, callbackRequestURI,
		"restart completion must use the relocated lifecycle route")

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
		CreatedAt:            time.Now(),
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

func TestRecoverIgnoresUnacceptedDeployingRuntimeRoute(t *testing.T) {
	const (
		leaseUUID    = "550e8400-e29b-41d4-a716-446655440000"
		operationID  = shared.OperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
		providerUUID = "22222222-2222-4222-8222-222222222222"
	)
	oldOperationURL := "https://old.example/callbacks/provision?operation_id=" + operationID.String()
	oldLifecycleURL := "https://old.example/callbacks/provision?lifecycle_id=" + operationID.String()
	newOperationURL := "https://new.example/callbacks/provision?operation_id=" + operationID.String()
	newLifecycleURL := "https://new.example/callbacks/provision?lifecycle_id=" + operationID.String()
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: "app"}}
	profiles := testResourceProfiles(t, items)
	manifestBytes := validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"})
	oldAuthority, err := shared.NewReleaseRuntimeAuthority(
		operationID, "tenant-a", providerUUID, oldOperationURL, oldLifecycleURL,
	)
	require.NoError(t, err)
	newAuthority, err := shared.NewReleaseRuntimeAuthority(
		operationID, "tenant-a", providerUUID, newOperationURL, newLifecycleURL,
	)
	require.NoError(t, err)
	store, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(t.TempDir(), "unaccepted-deploying.db"),
	})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.AppendActive(leaseUUID, shared.Release{
		Manifest: manifestBytes, Image: "stack", OperationID: operationID,
		Items: items, ResourceProfiles: profiles, RuntimeAuthority: &oldAuthority,
		Status: "active", CreatedAt: time.Now().Add(-time.Minute),
	}))
	require.NoError(t, store.Append(leaseUUID, shared.Release{
		Manifest: manifestBytes, Image: "stack", OperationID: operationID,
		Items: items, ResourceProfiles: profiles, RuntimeAuthority: &newAuthority,
		Status: "deploying", CreatedAt: time.Now(),
	}))
	container := ContainerInfo{
		ContainerID: "old-container", LeaseUUID: leaseUUID,
		Tenant: "tenant-a", ProviderUUID: providerUUID,
		SKU: "docker-small", ServiceName: "app", InstanceIndex: 0,
		CallbackURL: oldOperationURL, LifecycleCallbackURL: oldLifecycleURL,
		Image: "docker.io/library/nginx:1.27", Status: "running", CreatedAt: time.Now(),
	}
	b := newBackendForTest(&mockDockerClient{
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return []ContainerInfo{container}, nil
		},
		InspectContainerFn: func(context.Context, string) (*ContainerInfo, error) {
			copy := container
			return &copy, nil
		},
	}, nil)
	defer b.stopCancel()
	b.releaseStore = store
	require.NoError(t, b.recoverState(context.Background()))
	b.provisionsMu.RLock()
	recovered := b.provisions[leaseUUID]
	b.provisionsMu.RUnlock()
	require.NotNil(t, recovered)
	assert.Equal(t, oldOperationURL, recovered.CallbackURL)
	assert.Equal(t, oldLifecycleURL, recovered.LifecycleCallbackURL)
}

func TestRestart_RejectsLifecycleAuthorityMismatchWithoutMutation(t *testing.T) {
	const (
		currentID = "550e8400-e29b-41d4-a716-446655440000"
		otherID   = "123e4567-e89b-42d3-a456-426614174000"
	)
	oldOperationURL := "https://old.example/callbacks/provision?operation_id=" + currentID
	oldLifecycleURL := "https://old.example/callbacks/provision?lifecycle_id=" + currentID
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:            "lease-1",
			Status:               backend.ProvisionStatusReady,
			CallbackURL:          oldOperationURL,
			LifecycleCallbackURL: oldLifecycleURL,
			StackManifest:        &manifest.StackManifest{},
		}},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: "https://new.example/callbacks/provision?lifecycle_id=" + otherID,
	})
	require.ErrorIs(t, err, backend.ErrValidation)

	b.provisionsMu.RLock()
	defer b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, b.provisions["lease-1"].Status)
	assert.Equal(t, oldOperationURL, b.provisions["lease-1"].CallbackURL)
	assert.Equal(t, oldLifecycleURL, b.provisions["lease-1"].LifecycleCallbackURL)
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
func newActorMaintenanceClaim(
	t *testing.T,
	b *Backend,
	leaseUUID string,
	kind shared.MaintenanceIntentKind,
) shared.MaintenanceIntentClaim {
	t.Helper()
	dir := t.TempDir()
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(dir, "actor-maintenance-releases.db"),
	})
	require.NoError(t, err)
	callbacks, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(dir, "actor-maintenance-callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})

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
	require.NoError(t, releases.AppendActive(leaseUUID, source))
	_, sourceClaim, err := releases.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	target := source
	target.Version = 0
	target.Status = "deploying"
	target.CreatedAt = time.Now()

	admission, err := callbacks.BeginMaintenanceIntent(shared.MaintenanceIntentSpec{
		Kind:             kind,
		SourceRelease:    sourceClaim,
		TargetRelease:    target,
		Backend:          b.Name(),
		BackendStorageID: b.storageIdentity,
	})
	require.NoError(t, err)
	appendClaim, err := callbacks.StartMaintenanceAppend(admission)
	require.NoError(t, err)
	targetClaim, err := releases.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	claim, err := callbacks.BindMaintenanceIntentTarget(appendClaim.Intent(), targetClaim)
	require.NoError(t, err)
	return claim
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
			LeaseUUID:     leaseUUID,
			Tenant:        "tenant-a",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   callbackServer.URL + "/callbacks/provision",
			StackManifest: &manifest.StackManifest{},
		}},
	}
	b := newBackendForTest(mock, provisions)
	defer b.stopCancel()
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
	var workerCount atomic.Int64
	workerRelease := make(chan struct{})
	ack := make(chan error, 1)
	maintenance := newActorMaintenanceClaim(t, b, leaseUUID, shared.MaintenanceIntentRestart)
	require.True(t, b.routeToLease(leaseUUID, leasesm.RestartRequestedMsg{
		Cancel:               func() {},
		CallbackURL:          maintenance.CallbackURL(),
		LifecycleCallbackURL: maintenance.LifecycleCallbackURL(),
		Maintenance:          maintenance,
		Work: func() leasesm.ReplaceResult {
			workerCount.Add(1)
			<-workerRelease
			// A benign success result — the gauge correctness must come from
			// the REAL death→restart ordering driven through the SM above (the
			// death Dec'd the gauge; the success path must re-Inc because the
			// actor observed the lease was non-Ready (Failing) at replace-start
			// via replaceWasActive), NOT from any worker-result field. Leaving
			// the result zero-valued deliberately avoids re-introducing a field
			// that could mask the drift.
			return leasesm.ReplaceResult{Success: leasesm.ReplaceSuccessResult{}}
		},
		Ack: ack,
	}))

	select {
	case err := <-ack:
		require.NoError(t, err, "Failing→Restarting must be accepted by the SM (Permit, not Ignore)")
	case <-time.After(2 * time.Second):
		t.Fatal("no ack from handleRestartRequested")
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
	require.Equal(t, int64(1), workerCount.Load(), "exactly one replace worker must be spawned")
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
		"lease-1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:            "lease-1",
			Status:               backend.ProvisionStatusReady,
			SKU:                  "docker-small",
			CallbackURL:          oldOperationURL,
			LifecycleCallbackURL: oldLifecycleURL,
			Items:                []backend.LeaseItem{{SKU: "docker-small", Quantity: 1}},
		}},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	b.stopCancel()

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: "https://new.example/callbacks/provision?lifecycle_id=" + lifecycleID,
		Payload:     validManifestJSON("nginx:latest"),
	})
	require.Error(t, err)

	b.provisionsMu.RLock()
	status := b.provisions["lease-1"].Status
	callbackURL := b.provisions["lease-1"].CallbackURL
	lifecycleCallbackURL := b.provisions["lease-1"].LifecycleCallbackURL
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
			LeaseUUID:     durableCallbackTestLeaseUUID,
			Tenant:        "tenant-a",
			Status:        source,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   srv.URL + "/callbacks/provision",
			StackManifest: &manifest.StackManifest{},
			Items:         []backend.LeaseItem{{SKU: "docker-small"}},
		}},
	}
	b := newBackendForTest(mock, provisions)
	t.Cleanup(b.stopCancel)
	return b
}

// restartPreflightResult builds a doRestart-preflight-shaped ReplaceResult
// (no container touched). recoveredIfActive sets the §9.7 flag.
func restartPreflightResult(recoveredIfActive bool) leasesm.ReplaceResult {
	return leasesm.ReplaceResult{
		CallbackErr:             "restart failed",
		Err:                     fmt.Errorf("SKU preflight failed (test)"),
		RecoveredIfSourceActive: recoveredIfActive,
		Failure: leasesm.ReplaceFailureInfo{
			Operation:   "restart",
			CallbackErr: "restart failed",
			LastError:   "SKU preflight failed (test)",
		},
	}
}

// routeRestartStub routes a restart whose Work returns the given preflight
// result directly to the fixture lease's actor and requires the SM accepts it.
func routeRestartStub(t *testing.T, b *Backend, result leasesm.ReplaceResult) {
	t.Helper()
	ack := make(chan error, 1)
	maintenance := newActorMaintenanceClaim(t, b, durableCallbackTestLeaseUUID, shared.MaintenanceIntentRestart)
	require.True(t, b.routeToLease(durableCallbackTestLeaseUUID, leasesm.RestartRequestedMsg{
		Cancel:               func() {},
		Work:                 func() leasesm.ReplaceResult { return result },
		Ack:                  ack,
		CallbackURL:          maintenance.CallbackURL(),
		LifecycleCallbackURL: maintenance.LifecycleCallbackURL(),
		Maintenance:          maintenance,
	}))
	require.NoError(t, <-ack, "restart must be accepted by the SM")
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
	routeRestartStub(t, b, restartPreflightResult(true))
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
	routeRestartStub(t, b, restartPreflightResult(true))
	awaitSettled(t, b, backend.ProvisionStatusFailed, 1,
		"restart preflight from Failed (wasActive=false) must stay Failed")
}

// (4) update preflight failure → stays Failed regardless of source. The
// update stub sets Restored:false and does NOT set the flag — the intentional
// update asymmetry (a missed image pull never achieved the desired state).
func TestUpdatePreflight_StaysFailed(t *testing.T) {
	b := newPreflightBackend(t, &mockDockerClient{}, backend.ProvisionStatusReady)
	activeBefore := testutil.ToFloat64(activeProvisions)
	ack := make(chan error, 1)
	maintenance := newActorMaintenanceClaim(t, b, durableCallbackTestLeaseUUID, shared.MaintenanceIntentUpdate)
	require.True(t, b.routeToLease(durableCallbackTestLeaseUUID, leasesm.UpdateRequestedMsg{
		Cancel:               func() {},
		CallbackURL:          maintenance.CallbackURL(),
		LifecycleCallbackURL: maintenance.LifecycleCallbackURL(),
		Maintenance:          maintenance,
		Work: func() leasesm.ReplaceResult {
			return leasesm.ReplaceResult{
				CallbackErr: "image pull failed",
				Err:         fmt.Errorf("image pull failed (test)"),
				Restored:    false, // update preflight: unconditionally failed, NO flag
				Failure: leasesm.ReplaceFailureInfo{
					Operation:   "update",
					CallbackErr: "image pull failed",
					LastError:   "image pull failed (test)",
				},
			}
		},
		Ack: ack,
	}))
	require.NoError(t, <-ack, "update must be accepted by the SM")
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
	routeRestartStub(t, b, leasesm.ReplaceResult{
		CallbackErr: "restart failed",
		Err:         fmt.Errorf("replace failed but rolled back to previous (test)"),
		Restored:    true, // post-replace rollback restored the old containers; NO preflight flag
		Failure: leasesm.ReplaceFailureInfo{
			Operation:   "restart",
			OldStopped:  true,
			CallbackErr: "restart failed",
			LastError:   "replace failed but rolled back to previous (test)",
		},
	})
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
	// Pin the lease in Failing (block the diag goroutine) so the restart is
	// processed from SM source = Failing, not Failed.
	b.gatherer = blockingDiagGatherer{}

	b.handleContainerDeath("c1")
	b.provisionsMu.RLock()
	require.Equal(t, backend.ProvisionStatusFailing, b.provisions[durableCallbackTestLeaseUUID].Status,
		"container death must drive the lease to Failing")
	b.provisionsMu.RUnlock()

	routeRestartStub(t, b, restartPreflightResult(true))
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
	dir := t.TempDir()
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(dir, "releases.db"),
	})
	require.NoError(t, err)
	callbacks, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(dir, "callbacks.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})
	b.releaseStore = releases
	b.callbackStore = callbacks
	require.NoError(t, releases.AppendActive(leaseUUID, shared.Release{
		Manifest:         manifestBytes,
		Image:            "stack",
		OperationID:      operationID,
		Items:            items,
		ResourceProfiles: resourceProfiles,
		RuntimeAuthority: authority,
		Status:           "active",
		CreatedAt:        time.Now(),
	}))
	active, sourceClaim, err := releases.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	source, err := newReplaceSourceSnapshot(active)
	require.NoError(t, err)
	maintenance, target, err := b.admitMaintenance(shared.MaintenanceIntentUpdate, sourceClaim, shared.Release{
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

	result := b.doUpdate(context.Background(), leaseUUID, stack,
		resourceProfiles,
		items,
		callbackURL, lifecycleCallbackURL,
		maintenance, target, source,
		b.logger)

	require.Error(t, result.Err, "image pull failure must fail doUpdate preflight")
	assert.Equal(t, backend.ReasonImagePullFailed, result.Failure.Reason,
		"doUpdate image-pull preflight must author ReasonImagePullFailed (specific), not ReasonUpdateFailed (generic)")
	assert.NotEqual(t, backend.ReasonUpdateFailed, result.Failure.Reason,
		"must NOT fall back to the generic update-failed reason for an image-pull preflight failure")
	assert.Equal(t, backend.MsgImagePullFailed, result.Failure.CallbackErr,
		"CallbackErr must be the curated MsgImagePullFailed const so the (reason,message) pair cannot drift")
}
