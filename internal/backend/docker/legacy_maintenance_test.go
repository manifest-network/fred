package docker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"sync"
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

func seedLegacyStackMaintenanceAuthority(
	t *testing.T,
	b *Backend,
	leaseUUID string,
	stack *manifest.StackManifest,
	items []backend.LeaseItem,
	callbackURL, lifecycleCallbackURL string,
	client *http.Client,
) {
	t.Helper()
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	profiles := testResourceProfiles(t, items)
	authority, err := shared.NewLegacyRuntimeAuthority(
		"tenant-a", nominalDockerProviderUUID, callbackURL, lifecycleCallbackURL,
	)
	require.NoError(t, err)

	dir := t.TempDir()
	releases, err := shared.NewReleaseStore(shared.ReleaseStoreConfig{
		DBPath: filepath.Join(dir, "legacy-maintenance-releases.db"),
	})
	require.NoError(t, err)
	callbacks, err := shared.NewCallbackStore(shared.CallbackStoreConfig{
		DBPath: filepath.Join(dir, "legacy-maintenance-callbacks.db"),
	})
	require.NoError(t, err)
	b.releaseStore = releases
	b.callbackStore = callbacks
	require.NoError(t, releases.AppendActive(leaseUUID, shared.Release{
		Manifest:               manifestBytes,
		Image:                  "stack",
		Items:                  slices.Clone(items),
		ResourceProfiles:       shared.CloneSKUResourceSnapshot(profiles),
		LegacyRuntimeAuthority: &authority,
		Status:                 "active",
		CreatedAt:              time.Now().Add(-time.Minute),
	}))

	b.provisionsMu.Lock()
	provision := b.provisions[leaseUUID]
	require.NotNil(t, provision)
	provision.LeaseUUID = leaseUUID
	provision.Tenant = authority.Tenant()
	provision.ProviderUUID = authority.ProviderUUID()
	provision.CallbackURL = authority.CallbackURL()
	provision.LifecycleCallbackURL = authority.LifecycleCallbackURL()
	provision.ResourceProfiles = shared.CloneSKUResourceSnapshot(profiles)
	b.provisionsMu.Unlock()

	rebuildCallbackSender(b, client)
	b.wg.Go(b.callbackSender.RunReplayLoop)
	t.Cleanup(func() {
		b.stopCancel()
		b.wg.Wait()
		require.NoError(t, callbacks.Close())
		require.NoError(t, releases.Close())
	})
}

func TestV013LegacyReleaseFirstMaintenanceOperations(t *testing.T) {
	for _, operation := range []string{"restart", "update", "custom_domain"} {
		t.Run(operation, func(t *testing.T) {
			oldStack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
				"app": {
					Image: "docker.io/library/nginx:1.26",
					Ports: map[string]manifest.PortConfig{"80/tcp": {}},
				},
			}}
			items := []backend.LeaseItem{{
				SKU: "docker-small", ServiceName: "app", Quantity: 1,
			}}
			provisions := map[string]*provision{
				stackMaintenanceLeaseUUID: {ProvisionState: leasesm.ProvisionState{
					LeaseUUID: stackMaintenanceLeaseUUID,
					Tenant:    "tenant-a", ProviderUUID: nominalDockerProviderUUID,
					Status: backend.ProvisionStatusReady, StackManifest: oldStack,
					Items: slices.Clone(items), ContainerIDs: []string{"old-app"},
					ServiceContainers: map[string][]string{"app": {"old-app"}},
				}},
			}
			mock := &mockDockerClient{
				PullImageFn: func(context.Context, string, time.Duration) error { return nil },
				InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
					return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
				},
			}
			compose := &mockComposeExecutor{
				UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error { return nil },
				PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
					return []composeContainerSummary{{ID: "new-app", Service: "app", State: "running"}}, nil
				},
			}
			installStackStrictCohortInventory(t, mock, compose)

			var callback backend.CallbackPayload
			var requestURI string
			received := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewDecoder(r.Body).Decode(&callback)
				requestURI = r.URL.RequestURI()
				w.WriteHeader(http.StatusOK)
				close(received)
			}))
			defer server.Close()
			oldCallbackURL := server.URL + "/old/callbacks/provision"
			newCallbackURL := server.URL + "/new/callbacks/provision"

			b := newBackendForProvisionTest(t, mock, provisions)
			b.compose = compose
			b.cfg.StartupVerifyDuration = time.Millisecond
			seedLegacyStackMaintenanceAuthority(
				t, b, stackMaintenanceLeaseUUID, oldStack, items,
				oldCallbackURL, oldCallbackURL, server.Client(),
			)

			expectedCallbackURL := newCallbackURL
			switch operation {
			case "restart":
				require.NoError(t, b.Restart(context.Background(), backend.RestartRequest{
					LeaseUUID: stackMaintenanceLeaseUUID, CallbackURL: newCallbackURL,
				}))
			case "update":
				require.NoError(t, b.Update(context.Background(), backend.UpdateRequest{
					LeaseUUID:   stackMaintenanceLeaseUUID,
					CallbackURL: newCallbackURL,
					Payload: validStackManifestJSON(map[string]string{
						"app": "docker.io/library/nginx:1.27",
					}),
				}))
			case "custom_domain":
				expectedCallbackURL = oldCallbackURL
				b.cfg.Ingress = IngressConfig{
					Enabled: true, WildcardDomain: "example.net", Entrypoint: "websecure",
				}
				b.customDomainDNSReady = func(context.Context, string) bool { return true }
				desired := slices.Clone(items)
				desired[0].CustomDomain = "tenant.example.org"
				require.NoError(t, b.ReconcileCustomDomain(
					context.Background(), stackMaintenanceLeaseUUID, desired,
				))
			}

			awaitStackMaintenanceCallback(t, b, received)
			assert.Equal(t, backend.CallbackStatusSuccess, callback.Status)
			assert.Equal(t, expectedCallbackURL[len(server.URL):], requestURI)
			active, err := b.releaseStore.LatestActive(stackMaintenanceLeaseUUID)
			require.NoError(t, err)
			require.NotNil(t, active)
			assert.Empty(t, active.OperationID)
			assert.Nil(t, active.RuntimeAuthority)
			require.NotNil(t, active.LegacyRuntimeAuthority)
			assert.Equal(t, expectedCallbackURL, active.LegacyRuntimeAuthority.CallbackURL())
			assert.Equal(t, expectedCallbackURL, active.LegacyRuntimeAuthority.LifecycleCallbackURL())
			assert.True(t, active.MaintenanceID.Valid(),
				"the exact replacement WAL still uses a UUIDv4 for legacy runtime authority")
		})
	}
}

func TestV013LegacyReleaseSubsequentMaintenanceRollbackPreservesSourceAuthority(t *testing.T) {
	oldStack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
		"app": {Image: "docker.io/library/nginx:1.26"},
	}}
	items := []backend.LeaseItem{{
		SKU: "docker-small", ServiceName: "app", Quantity: 1,
	}}
	provisions := map[string]*provision{
		stackMaintenanceLeaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: stackMaintenanceLeaseUUID,
			Tenant:    "tenant-a", ProviderUUID: nominalDockerProviderUUID,
			Status: backend.ProvisionStatusReady, StackManifest: oldStack,
			Items: slices.Clone(items), ContainerIDs: []string{"old-app"},
			ServiceContainers: map[string][]string{"app": {"old-app"}},
		}},
	}

	type projectAuthority struct {
		callbackURL          string
		lifecycleCallbackURL string
		maintenanceID        shared.MaintenanceID
	}
	var (
		upMu       sync.Mutex
		upCalls    int
		projects   []projectAuthority
		callbackMu sync.Mutex
		callbacks  []struct {
			payload backend.CallbackPayload
			uri     string
		}
	)
	compose := &mockComposeExecutor{
		UpFn: func(_ context.Context, project *composetypes.Project, _ composeUpOpts) error {
			labels := project.Services["app"].Labels
			upMu.Lock()
			upCalls++
			call := upCalls
			projects = append(projects, projectAuthority{
				callbackURL:          labels[LabelCallbackURL],
				lifecycleCallbackURL: labels[LabelLifecycleCallbackURL],
				maintenanceID:        shared.MaintenanceID(labels[LabelMaintenanceID]),
			})
			upMu.Unlock()
			if call == 2 {
				return errors.New("simulated subsequent restart failure")
			}
			return nil
		},
		PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{{
				ID: "current-app", Service: "app", State: "running",
			}}, nil
		},
	}
	mock := &mockDockerClient{
		PullImageFn: func(context.Context, string, time.Duration) error { return nil },
		InspectContainerFn: func(_ context.Context, containerID string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: containerID, Status: "running"}, nil
		},
	}
	installStackStrictCohortInventory(t, mock, compose)

	callbackReceived := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload backend.CallbackPayload
		_ = json.NewDecoder(r.Body).Decode(&payload)
		callbackMu.Lock()
		callbacks = append(callbacks, struct {
			payload backend.CallbackPayload
			uri     string
		}{payload: payload, uri: r.URL.RequestURI()})
		callbackMu.Unlock()
		w.WriteHeader(http.StatusOK)
		callbackReceived <- struct{}{}
	}))
	defer server.Close()
	oldCallbackURL := server.URL + "/old/callbacks/provision"
	newCallbackURL := server.URL + "/new/callbacks/provision"

	b := newBackendForProvisionTest(t, mock, provisions)
	b.compose = compose
	b.cfg.StartupVerifyDuration = time.Millisecond
	seedLegacyStackMaintenanceAuthority(
		t, b, stackMaintenanceLeaseUUID, oldStack, items,
		oldCallbackURL, oldCallbackURL, server.Client(),
	)

	// Establish a real prior maintenance generation. The next restart must treat
	// this active target (including its MaintenanceID) as rollback authority.
	require.NoError(t, b.Restart(t.Context(), backend.RestartRequest{
		LeaseUUID: stackMaintenanceLeaseUUID, CallbackURL: oldCallbackURL,
	}))
	awaitStackMaintenanceCallback(t, b, callbackReceived)
	source, err := b.releaseStore.LatestActive(stackMaintenanceLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, source)
	require.True(t, source.MaintenanceID.Valid())
	sourceMaintenanceID := source.MaintenanceID
	sourceAuthority := mustDockerReleaseRuntimeIdentity(t, *source)
	require.Equal(t, shared.ReleaseAuthorityLegacy, sourceAuthority.Class())
	require.Equal(t, oldCallbackURL, sourceAuthority.CallbackURL())

	// The forward replacement publishes the requested new callback base, then
	// fails. Rollback succeeds and must rebuild the prior exact generation rather
	// than relabeling it with the failed target's route or MaintenanceID.
	require.NoError(t, b.Restart(t.Context(), backend.RestartRequest{
		LeaseUUID: stackMaintenanceLeaseUUID, CallbackURL: newCallbackURL,
	}))
	awaitStackMaintenanceCallback(t, b, callbackReceived)

	callbackMu.Lock()
	require.Len(t, callbacks, 2)
	firstCallback := callbacks[0]
	secondCallback := callbacks[1]
	callbackMu.Unlock()
	assert.Equal(t, backend.CallbackStatusSuccess, firstCallback.payload.Status)
	assert.Equal(t, "/old/callbacks/provision", firstCallback.uri)
	assert.Equal(t, backend.CallbackStatusFailed, secondCallback.payload.Status)
	assert.Equal(t, "/new/callbacks/provision", secondCallback.uri,
		"failed maintenance completion belongs to the new target route")

	active, err := b.releaseStore.LatestActive(stackMaintenanceLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	activeAuthority := mustDockerReleaseRuntimeIdentity(t, *active)
	assert.Equal(t, shared.ReleaseAuthorityLegacy, activeAuthority.Class())
	assert.Equal(t, sourceMaintenanceID, active.MaintenanceID)
	assert.Equal(t, oldCallbackURL, activeAuthority.CallbackURL())
	assert.Equal(t, oldCallbackURL, activeAuthority.LifecycleCallbackURL())
	assert.Empty(t, active.OperationID)
	assert.Nil(t, active.RuntimeAuthority)

	b.provisionsMu.RLock()
	projectedStatus := b.provisions[stackMaintenanceLeaseUUID].Status
	projectedCallbackURL := b.provisions[stackMaintenanceLeaseUUID].CallbackURL
	projectedLifecycleCallbackURL := b.provisions[stackMaintenanceLeaseUUID].LifecycleCallbackURL
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, projectedStatus)
	assert.Equal(t, oldCallbackURL, projectedCallbackURL)
	assert.Equal(t, oldCallbackURL, projectedLifecycleCallbackURL)

	upMu.Lock()
	require.Len(t, projects, 3, "first maintenance, failed target, successful rollback")
	failedTarget := projects[1]
	rollback := projects[2]
	upMu.Unlock()
	assert.Equal(t, newCallbackURL, failedTarget.callbackURL)
	assert.Equal(t, newCallbackURL, failedTarget.lifecycleCallbackURL)
	assert.True(t, failedTarget.maintenanceID.Valid())
	assert.NotEqual(t, sourceMaintenanceID, failedTarget.maintenanceID)
	assert.Equal(t, oldCallbackURL, rollback.callbackURL)
	assert.Equal(t, oldCallbackURL, rollback.lifecycleCallbackURL)
	assert.Equal(t, sourceMaintenanceID, rollback.maintenanceID,
		"rollback labels must name the prior active maintenance generation")
}

func TestReleaseRuntimeAuthoritiesForMaintenanceRejectsAuthorityClassChange(t *testing.T) {
	legacyURL := "https://fred.example/callbacks/provision"
	authority, err := shared.NewLegacyRuntimeAuthority(
		"tenant-a", nominalDockerProviderUUID, legacyURL, legacyURL,
	)
	require.NoError(t, err)
	active := shared.Release{LegacyRuntimeAuthority: &authority, Status: "active"}

	typedID := shared.OperationID("6ba7b810-9dad-41d1-80b4-00c04fd430c8")
	_, _, err = releaseRuntimeAuthoritiesForMaintenance(
		active,
		authority.Tenant(),
		authority.ProviderUUID(),
		"https://fred.example/callbacks/provision?operation_id="+typedID.String(),
		"https://fred.example/callbacks/provision?lifecycle_id="+typedID.String(),
	)
	require.Error(t, err)
}
