package docker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/docker/imageexec"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// v013Workload is the identity Fred v0.13.0 wrote onto every container of one
// lease: the fred.* labels of compose_project.go at v0.13.0, without ingress.
// There is no lifecycle callback, maintenance generation or immutable image
// binding label, and the daemon resolved the image reference itself.
type v013Workload struct {
	leaseUUID, tenant, providerUUID, callbackURL, backendName string
	item                                                      backend.LeaseItem
	imageReference, imageID                                   string
}

func v013WorkloadFor(t *testing.T, h *maintenanceRecoveryHarness, imageID string) v013Workload {
	t.Helper()
	authority, ok := h.source.RuntimeIdentity()
	require.True(t, ok)
	require.Equal(t, shared.ReleaseAuthorityLegacy, authority.Class())
	stack, err := manifest.ParseStoredPayload(h.source.Manifest)
	require.NoError(t, err)
	item := h.source.Items[0]
	return v013Workload{
		leaseUUID: h.leaseUUID, tenant: authority.Tenant(), providerUUID: authority.ProviderUUID(),
		callbackURL: authority.CallbackURL(), backendName: h.b.Name(), item: item,
		imageReference: stack.Services[item.ServiceName].Image, imageID: imageID,
	}
}

func (w v013Workload) inspected(index int) container.InspectResponse {
	labels := map[string]string{
		LabelManaged:       "true",
		LabelLeaseUUID:     w.leaseUUID,
		LabelTenant:        w.tenant,
		LabelProviderUUID:  w.providerUUID,
		LabelSKU:           w.item.SKU,
		LabelCreatedAt:     time.Now().Add(-time.Hour).Format(time.RFC3339),
		LabelInstanceIndex: strconv.Itoa(index),
		LabelFailCount:     "0",
		LabelCallbackURL:   w.callbackURL,
		LabelBackendName:   w.backendName,
		LabelServiceName:   w.item.ServiceName,
	}
	return container.InspectResponse{
		ContainerJSONBase: &container.ContainerJSONBase{
			ID:    fmt.Sprintf("v013-%s-%d", w.item.ServiceName, index),
			Name:  fmt.Sprintf("/fred-%s-%s-%d", w.leaseUUID, w.item.ServiceName, index),
			Image: w.imageID,
			State: &container.State{Status: "running", Running: true},
			HostConfig: &container.HostConfig{
				ReadonlyRootfs: true, CapDrop: []string{"ALL"},
				Resources: container.Resources{Memory: 512 << 20},
			},
		},
		Config: &container.Config{Image: w.imageReference, Labels: labels},
		NetworkSettings: &container.NetworkSettings{Networks: map[string]*network.EndpointSettings{
			"fred-tenant": {Aliases: []string{w.item.ServiceName}},
		}},
	}
}

// cohort reads every instance through the production reader, so the fixture
// cannot drift from how the backend observes a v0.13 container.
func (w v013Workload) cohort(t *testing.T) []ContainerInfo {
	t.Helper()
	cohort := make([]ContainerInfo, 0, w.item.Quantity)
	for index := range w.item.Quantity {
		info := readInspectedContainer(t, w.inspected(index))
		require.Empty(t, info.LifecycleCallbackURL, "v0.13 never wrote a lifecycle callback label")
		require.True(t, info.MaintenanceID.IsZero(), "v0.13 never wrote a maintenance generation label")
		cohort = append(cohort, info)
	}
	return cohort
}

func readInspectedContainer(t *testing.T, inspected container.InspectResponse) ContainerInfo {
	t.Helper()
	reader := &DockerClient{client: dockerSDKView{
		containerInspect: func(context.Context, string) (container.InspectResponse, error) { return inspected, nil },
	}}
	info, err := reader.InspectContainer(t.Context(), inspected.ID)
	require.NoError(t, err)
	require.NotNil(t, info.execution)
	return *info
}

func outcomeCause(outcome shared.MaintenanceExecutionOutcome) error {
	switch typed := outcome.(type) {
	case shared.MaintenanceExecutionFailure:
		return typed.Cause()
	case shared.MaintenanceExecutionAmbiguous:
		return typed.Cause()
	}
	return nil
}

// The release's authority class decides how a container's callback labels
// compare: a legacy release accepts the lifecycle route v0.13 never wrote,
// while a typed release still requires every label exactly (ENG-1253).
func TestMaintenanceGenerationContainerFollowsReleaseAuthorityClass(t *testing.T) {
	const leaseUUID = "0192f1a0-1111-4abc-8def-00000000e253"
	stack, err := json.Marshal(&manifest.StackManifest{Services: map[string]*manifest.Manifest{"web": {Image: "docker.io/library/nginx:1.27"}}})
	require.NoError(t, err)
	items := []backend.LeaseItem{{SKU: "docker-small", ServiceName: "web", Quantity: 1}}
	legacyURL := "https://fred.example/callbacks/provision"
	legacyAuthority, err := shared.NewLegacyRuntimeAuthority("tenant-a", nominalDockerProviderUUID, legacyURL, "")
	require.NoError(t, err)
	legacy := shared.Release{Manifest: stack, Items: items, LegacyRuntimeAuthority: &legacyAuthority}
	operationID, typedURL, typedLifecycleURL := newTestRestoreCallbackAuthority(t)
	typed := shared.Release{Manifest: stack, Items: items, OperationID: operationID,
		RuntimeAuthority: mustTestReleaseRuntimeAuthority(t, operationID, "tenant-a", nominalDockerProviderUUID, typedURL, typedLifecycleURL)}
	observed := func(callbackURL, lifecycleURL string) ContainerInfo {
		return ContainerInfo{
			ContainerID: "web-0", LeaseUUID: leaseUUID, BackendName: "docker-a", Tenant: "tenant-a",
			ProviderUUID: nominalDockerProviderUUID, CallbackURL: callbackURL, LifecycleCallbackURL: lifecycleURL,
			ServiceName: "web", SKU: "docker-small", Image: "docker.io/library/nginx:1.27",
		}
	}
	for _, test := range []struct {
		name     string
		release  shared.Release
		observed ContainerInfo
		accepted bool
	}{
		{"legacy release, v0.13 labels", legacy, observed(legacyURL, ""), true},
		{"legacy release, derived lifecycle label", legacy, observed(legacyURL, legacyAuthority.LifecycleCallbackURL()), true},
		{"legacy release, foreign lifecycle label", legacy, observed(legacyURL, "https://other.example/callbacks/provision"), false},
		{"legacy release, foreign callback", legacy, observed("https://other.example/callbacks/provision", ""), false},
		{"typed release, exact labels", typed, observed(typedURL, typedLifecycleURL), true},
		{"typed release, no lifecycle label", typed, observed(typedURL, ""), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateMaintenanceGenerationContainer(leaseUUID, shared.MaintenanceID{}, "docker-a", test.release, test.observed)
			if test.accepted {
				require.NoError(t, err)
				return
			}
			var deferred *maintenanceObservationDeferred
			require.ErrorAs(t, err, &deferred)
		})
	}
}

// A lease adopted from v0.13 keeps the containers v0.13 created. Its first
// typed restart or update must capture that exact source cohort, then replace
// it, instead of failing before any effect (ENG-1253).
func TestV013AdoptedCohortFirstMaintenanceCapturesItsSource(t *testing.T) {
	for _, kind := range []shared.MaintenanceIntentKind{shared.MaintenanceIntentRestart, shared.MaintenanceIntentUpdate} {
		t.Run(string(kind), func(t *testing.T) {
			h := newLegacyMaintenanceRecoveryHarnessForKind(t, kind)
			h.appendTarget(true)
			mock := h.b.docker.(*mockDockerClient)
			imageID := fixtureImageID("v0.13 daemon-pulled image")
			h.inventory.containers = v013WorkloadFor(t, h, imageID).cohort(t)
			mock.InspectImageFn = func(context.Context, string) (*ImageInfo, error) { return &ImageInfo{ID: imageID}, nil }
			mock.PullImageFn = func(context.Context, string, time.Duration) error { return nil }
			targets := h.containersFor(h.targetRelease, 2, "running", "")
			h.b.compose = &mockComposeExecutor{
				LaunchFn: func(_ context.Context, project *composetypes.Project, _ composeUpOpts) daemonLaunchOutcome {
					for _, service := range project.Services {
						require.Equal(t, imageID, service.Image)
					}
					h.inventory.containers = slices.Clone(targets)
					return daemonLaunchOutcome{settled: true}
				},
				PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
					return []composeContainerSummary{
						{ID: targets[0].ContainerID, Service: "web-0", State: "running"},
						{ID: targets[1].ContainerID, Service: "web-1", State: "running"},
					}, nil
				},
			}
			ops, err := storageMutationOperationsForTest(h.b)
			require.NoError(t, err)
			require.NoError(t, bindDockerMaintenanceCompensation(h.b, ops))

			execution, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
			require.NoError(t, err)
			outcome := h.b.maintenanceSettlement.ExecuteMaintenance(testMaintenanceLifetime(t, t.Context()), execution)
			success, ok := outcome.(shared.MaintenanceExecutionSuccess)
			require.True(t, ok, "outcome = %T: %v", outcome, outcomeCause(outcome))
			active, err := h.b.maintenanceSettlement.ActivateMaintenance(success)
			require.NoError(t, err)
			require.True(t, active.Valid())
		})
	}
}

// When the target fails after its effects, compensation must restore the
// adopted v0.13 source from its frozen plan: the replayed containers keep the
// v0.13 identity labels, and a v0.13 instance that survived the partial
// replacement is retired by the source's own identity even after the target
// moved to a new callback base.
func TestV013AdoptedCohortCompensationRestoresItsSource(t *testing.T) {
	for _, test := range []struct {
		name     string
		survivor bool
		newBase  string
	}{
		{name: "every instance replaced"},
		{name: "v0.13 survivor after a moved callback base", survivor: true, newBase: "https://new-provider.example/callbacks/provision"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var h *maintenanceRecoveryHarness
			if test.newBase != "" {
				h = newLegacyMaintenanceRecoveryHarnessForKindAtCallback(t, shared.MaintenanceIntentUpdate, test.newBase)
			} else {
				h = newLegacyMaintenanceRecoveryHarnessForKind(t, shared.MaintenanceIntentUpdate)
			}
			h.appendTarget(true)
			mock := h.b.docker.(*mockDockerClient)
			sourceID, targetID := fixtureImageID("v0.13 source image"), fixtureImageID("moved mutable tag")
			currentTag := sourceID
			sources := v013WorkloadFor(t, h, sourceID).cohort(t)
			h.inventory.containers = slices.Clone(sources)
			mock.InspectImageFn = func(_ context.Context, reference string) (*ImageInfo, error) {
				if strings.HasPrefix(reference, "sha256:") {
					return &ImageInfo{ID: reference}, nil
				}
				return &ImageInfo{ID: currentTag}, nil
			}
			mock.PullImageFn = func(context.Context, string, time.Duration) error {
				currentTag = targetID
				return nil
			}
			mock.ContainerLogsFn = func(context.Context, string, int) (string, error) { return "target startup error", nil }
			mock.RemoveContainerFn = h.inventory.remove
			failedTargets := h.containersFor(h.targetRelease, 2, "exited", "")
			replaced := failedTargets
			if test.survivor {
				// Compose replaced instance 0 and failed before instance 1.
				replaced = []ContainerInfo{failedTargets[0], sources[1]}
			}
			h.b.compose = &mockComposeExecutor{
				LaunchFn: func(context.Context, *composetypes.Project, composeUpOpts) daemonLaunchOutcome {
					h.inventory.containers = slices.Clone(replaced)
					return daemonLaunchOutcome{settled: true}
				},
				PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
					summaries := make([]composeContainerSummary, 0, len(replaced))
					for _, info := range replaced {
						summaries = append(summaries, composeContainerSummary{
							ID: info.ContainerID, Service: fmt.Sprintf("web-%d", info.InstanceIndex), State: info.Status,
						})
					}
					return summaries, nil
				},
			}
			var restored []map[string]string
			mock.CreateCompensationContainerFn = func(_ context.Context, image imageexec.Image, snapshot compensationContainer) (string, error) {
				require.Equal(t, sourceID, image.ID(), "the source replays its frozen image, not the moved tag")
				labels := maps.Clone(snapshot.Config.Labels)
				labels[LabelCreatedAt] = time.Now().Format(time.RFC3339)
				restored = append(restored, labels)
				config := *snapshot.Config
				config.Labels = labels
				info := readInspectedContainer(t, container.InspectResponse{
					ContainerJSONBase: &container.ContainerJSONBase{
						ID: "restored-" + snapshot.Name, Name: "/" + snapshot.Name, Image: image.ID(),
						State: &container.State{Status: "created"}, HostConfig: snapshot.Host,
					},
					Config:          &config,
					NetworkSettings: &container.NetworkSettings{Networks: snapshot.Networks.EndpointsConfig},
				})
				h.inventory.mu.Lock()
				h.inventory.containers = append(h.inventory.containers, info)
				h.inventory.mu.Unlock()
				return info.ContainerID, nil
			}
			mock.StartContainerFn = func(_ context.Context, id string, _ time.Duration) error {
				h.inventory.mu.Lock()
				defer h.inventory.mu.Unlock()
				for index := range h.inventory.containers {
					if h.inventory.containers[index].ContainerID == id {
						h.inventory.containers[index].Status = "running"
						return nil
					}
				}
				return errors.New("start of unknown source")
			}
			ops, err := storageMutationOperationsForTest(h.b)
			require.NoError(t, err)
			require.NoError(t, bindDockerMaintenanceCompensation(h.b, ops))

			execution, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
			require.NoError(t, err)
			outcome := h.b.maintenanceSettlement.ExecuteMaintenance(testMaintenanceLifetime(t, t.Context()), execution)
			failed, ok := outcome.(shared.MaintenanceExecutionFailure)
			require.True(t, ok, "outcome = %T: %v", outcome, outcomeCause(outcome))
			require.True(t, failed.SourceRecovered(), "compensation must yield the adopted source again")
			require.Len(t, restored, 2)
			for _, labels := range restored {
				assert.NotContains(t, labels, LabelLifecycleCallbackURL, "replay keeps the v0.13 identity it froze")
				assert.NotContains(t, labels, LabelMaintenanceID)
				assert.Equal(t, sources[0].CallbackURL, labels[LabelCallbackURL])
			}
			if test.survivor {
				assert.Contains(t, h.inventory.removed, sources[1].ContainerID, "the surviving v0.13 instance is retired before its name is reused")
			}
			proof, err := h.b.maintenanceSettlement.FailMaintenance(failed, backend.ReasonUpdateFailed, "update failed")
			require.NoError(t, err)
			require.True(t, proof.Valid())
		})
	}
}

// A failure before any effect keeps the adopted v0.13 source as it is: the
// compensation classifier must recognize it as the exact, ready source.
func TestV013AdoptedCohortPrelaunchFailureKeepsItsSource(t *testing.T) {
	h := newLegacyMaintenanceRecoveryHarnessForKind(t, shared.MaintenanceIntentUpdate)
	h.appendTarget(true)
	mock := h.b.docker.(*mockDockerClient)
	imageID := fixtureImageID("intact v0.13 source")
	sources := v013WorkloadFor(t, h, imageID).cohort(t)
	h.inventory.containers = slices.Clone(sources)
	mock.InspectImageFn = func(context.Context, string) (*ImageInfo, error) { return &ImageInfo{ID: imageID}, nil }
	mock.PullImageFn = func(context.Context, string, time.Duration) error { return errors.New("registry unavailable") }
	mock.CreateCompensationContainerFn = func(context.Context, imageexec.Image, compensationContainer) (string, error) {
		t.Fatal("an intact adopted source must not be recreated")
		return "", nil
	}
	ops, err := storageMutationOperationsForTest(h.b)
	require.NoError(t, err)
	require.NoError(t, bindDockerMaintenanceCompensation(h.b, ops))

	execution, err := h.b.maintenanceSettlement.StartMaintenanceExecution(h.target)
	require.NoError(t, err)
	outcome := h.b.maintenanceSettlement.ExecuteMaintenance(testMaintenanceLifetime(t, t.Context()), execution)
	failed, ok := outcome.(shared.MaintenanceExecutionFailure)
	require.True(t, ok, "outcome = %T: %v", outcome, outcomeCause(outcome))
	require.True(t, failed.SourceRecovered())
	require.Empty(t, h.inventory.removed)
	var physical *physicalOperationError
	require.ErrorAs(t, failed.Cause(), &physical)
	require.Equal(t, backend.ReasonImagePullFailed, physical.reason)
}

// A stateful adopted lease (EverClaw) captures its v0.13 source, volume roots
// included, then retires its v0.13 volume writers before the replacement
// mounts their volumes: they are the source generation, not interference.
func TestVolumeWriterRetirementRetiresAdoptedV013Writers(t *testing.T) {
	h := newLegacyMaintenanceRecoveryHarnessForKind(t, shared.MaintenanceIntentRestart)
	h.appendTarget(true)
	sourceImageID := fixtureImageID("v0.13 stateful image")
	workload := v013WorkloadFor(t, h, sourceImageID)
	f := newWriterRetirementHarnessFor(t, h, workload.cohort(t))
	// Read each writer again with the bind mount the harness gave it, so its
	// execution snapshot carries the volume like a real v0.13 container.
	for index, source := range f.sources {
		inspected := workload.inspected(index)
		for _, bind := range source.Mounts {
			inspected.Mounts = append(inspected.Mounts, container.MountPoint{
				Type: mount.TypeBind, Source: bind.Source, Destination: bind.Target, RW: !bind.ReadOnly,
			})
		}
		f.sources[index] = readInspectedContainer(t, inspected)
		require.Equal(t, source.Mounts, f.sources[index].Mounts)
	}
	h.inventory.containers = slices.Clone(f.sources)
	targetImage := f.docker.InspectImageFn
	f.docker.InspectImageFn = func(ctx context.Context, reference string) (*ImageInfo, error) {
		if reference == sourceImageID {
			return &ImageInfo{ID: sourceImageID}, nil
		}
		return targetImage(ctx, reference)
	}
	ops, err := storageMutationOperationsForTest(h.b)
	require.NoError(t, err)
	require.NoError(t, bindDockerMaintenanceCompensation(h.b, ops))

	f.execute(t)
	require.Equal(t, 1, f.launches, "the target launches only after capturing the source")
	require.Equal(t, 2, f.stops)
	require.Equal(t, 2, f.removes)
}

// adoptedV013Backend is one lease adopted from v0.13 behind the backend API:
// the v0.13 cohort on its old callback route, its upgraded legacy release, and
// source capture bound as in production. Each test starts one maintenance and
// waits for its callback.
type adoptedV013Backend struct {
	b              *Backend
	mock           *mockDockerClient
	items          []backend.LeaseItem
	serverURL      string
	oldCallbackURL string
	newCallbackURL string
	logs           *syncLog
	received       chan struct{}
	callback       backend.CallbackPayload
	requestURI     string
}

func newAdoptedV013Backend(t *testing.T, imageID string) *adoptedV013Backend {
	t.Helper()
	oldStack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
		"app": {Image: "docker.io/library/nginx:1.26", Ports: map[string]manifest.PortConfig{"80/tcp": {}}},
	}}
	f := &adoptedV013Backend{
		items: []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", Quantity: 1}},
		logs:  &syncLog{}, received: make(chan struct{}),
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&f.callback)
		f.requestURI = r.URL.RequestURI()
		w.WriteHeader(http.StatusOK)
		close(f.received)
	}))
	t.Cleanup(server.Close)
	f.serverURL = server.URL
	f.oldCallbackURL = server.URL + "/old/callbacks/provision"
	f.newCallbackURL = server.URL + "/new/callbacks/provision"

	var (
		cohortMu sync.Mutex
		cohort   []ContainerInfo
	)
	f.mock = &mockDockerClient{
		PullImageFn:    func(context.Context, string, time.Duration) error { return nil },
		InspectImageFn: func(context.Context, string) (*ImageInfo, error) { return &ImageInfo{ID: imageID}, nil },
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: id, Status: "running"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			cohortMu.Lock()
			defer cohortMu.Unlock()
			return slices.Clone(cohort), nil
		},
	}
	compose := &mockComposeExecutor{
		UpFn: func(context.Context, *composetypes.Project, composeUpOpts) error { return nil },
		PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{{ID: "new-app", Service: "app", State: "running"}}, nil
		},
	}
	installStackStrictCohortInventory(t, f.mock, compose)
	f.b = newBackendForProvisionTest(t, f.mock, map[string]*provision{
		stackMaintenanceLeaseUUID: {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: stackMaintenanceLeaseUUID, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
			Status: backend.ProvisionStatusReady, StackManifest: oldStack, Items: slices.Clone(f.items),
			ContainerIDs: []string{"v013-app-0"}, ServiceContainers: map[string][]string{"app": {"v013-app-0"}},
		}},
	})
	f.b.compose = compose
	f.b.cfg.StartupVerifyDuration = time.Millisecond
	f.b.logger = slog.New(slog.NewTextHandler(f.logs, nil))
	seedLegacyStackMaintenanceAuthority(t, f.b, stackMaintenanceLeaseUUID, oldStack, f.items, f.oldCallbackURL, f.oldCallbackURL, server.Client())
	workload := v013Workload{
		leaseUUID: stackMaintenanceLeaseUUID, tenant: "tenant-a", providerUUID: nominalDockerProviderUUID,
		callbackURL: f.oldCallbackURL, backendName: f.b.Name(), item: f.items[0],
		imageReference: oldStack.Services["app"].Image, imageID: imageID,
	}
	cohortMu.Lock()
	cohort = workload.cohort(t)
	cohortMu.Unlock()
	ops, err := storageMutationOperationsForTest(f.b)
	require.NoError(t, err)
	require.NoError(t, bindDockerMaintenanceCompensation(f.b, ops))
	return f
}

func (f *adoptedV013Backend) restart(t *testing.T, callbackURL string) {
	t.Helper()
	require.NoError(t, f.b.Restart(context.Background(), backend.RestartRequest{
		MaintenanceID: newTestMaintenanceID(t), LeaseUUID: stackMaintenanceLeaseUUID, CallbackURL: callbackURL,
	}))
}

// awaitCallback returns the settled callback and its request URI; reading
// them after delivery is ordered by the server's close of received.
func (f *adoptedV013Backend) awaitCallback(t *testing.T) (backend.CallbackPayload, string) {
	t.Helper()
	awaitStackMaintenanceCallback(t, f.b, f.received)
	return f.callback, f.requestURI
}

// syncLog is a log sink the backend's goroutines write while a test reads it.
type syncLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *syncLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *syncLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// The first restart, update or custom-domain change of an adopted lease runs
// through the backend API with source capture bound as in production. A
// tenant restart or update moves the lease to the callback base of its
// request; a provider custom-domain redeploy keeps the old one. Either way the
// release stays in the legacy authority class.
func TestV013AdoptedLeaseFirstMaintenanceThroughBackend(t *testing.T) {
	for _, operation := range []string{"restart", "update", "custom_domain"} {
		t.Run(operation, func(t *testing.T) {
			f := newAdoptedV013Backend(t, fixtureImageID("v0.13 daemon-pulled nginx"))
			expectedCallbackURL := f.newCallbackURL
			switch operation {
			case "restart":
				f.restart(t, f.newCallbackURL)
			case "update":
				require.NoError(t, f.b.Update(context.Background(), backend.UpdateRequest{
					MaintenanceID: newTestMaintenanceID(t), LeaseUUID: stackMaintenanceLeaseUUID, CallbackURL: f.newCallbackURL,
					Payload: validStackManifestJSON(map[string]string{"app": "docker.io/library/nginx:1.27"}),
				}))
			case "custom_domain":
				expectedCallbackURL = f.oldCallbackURL
				f.b.cfg.Ingress = IngressConfig{Enabled: true, WildcardDomain: "example.net", Entrypoint: "websecure"}
				f.b.customDomainDNSReady = func(context.Context, string) bool { return true }
				desired := slices.Clone(f.items)
				desired[0].CustomDomain = "tenant.example.org"
				require.NoError(t, f.b.ReconcileCustomDomain(context.Background(), stackMaintenanceLeaseUUID, desired))
			}

			callback, requestURI := f.awaitCallback(t)
			assert.Equal(t, backend.CallbackStatusSuccess, callback.Status, "callback error: %s", callback.Error)
			assert.Equal(t, expectedCallbackURL[len(f.serverURL):], requestURI)
			active, err := f.b.releaseStore.LatestActive(stackMaintenanceLeaseUUID)
			require.NoError(t, err)
			require.NotNil(t, active)
			assert.True(t, active.OperationID.IsZero())
			assert.Nil(t, active.RuntimeAuthority)
			require.NotNil(t, active.LegacyRuntimeAuthority)
			assert.Equal(t, expectedCallbackURL, active.LegacyRuntimeAuthority.CallbackURL())
			assert.Equal(t, expectedCallbackURL, active.LegacyRuntimeAuthority.LifecycleCallbackURL())
			assert.True(t, active.MaintenanceID.Valid(),
				"the exact replacement WAL still uses a UUIDv4 for legacy runtime authority")
		})
	}
}

// A maintenance that fails before any effect keeps its tenant surface curated,
// while the operator gets the actual cause in the log and in the attempt's
// published diagnostic (ENG-1253).
func TestMaintenancePreparationFailureCauseIsOperatorVisible(t *testing.T) {
	f := newAdoptedV013Backend(t, fixtureImageID("v0.13 daemon-pulled nginx"))
	f.mock.InspectImageFn = func(context.Context, string) (*ImageInfo, error) {
		return nil, errors.New("daemon source image metadata unavailable")
	}
	f.restart(t, f.oldCallbackURL)

	callback, _ := f.awaitCallback(t)
	assert.Equal(t, backend.CallbackStatusFailed, callback.Status)
	assert.Equal(t, "restart failed", callback.Error, "the tenant surface stays curated")
	entry, err := f.b.diagnosticsStore.Get(stackMaintenanceLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.Equal(t, backend.ReasonRestartFailed, entry.Reason)
	assert.Equal(t, "restart failed", entry.Message)
	assert.Contains(t, entry.Error, "admit captured source image")
	assert.Contains(t, entry.Error, "daemon source image metadata unavailable")
	logs := f.logs.String()
	assert.Contains(t, logs, "maintenance failed (verbose detail retained operator-side)")
	assert.Contains(t, logs, "daemon source image metadata unavailable")
	assert.Contains(t, logs, stackMaintenanceLeaseUUID)
}
