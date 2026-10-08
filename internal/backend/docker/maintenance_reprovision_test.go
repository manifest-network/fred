package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
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

// unpullableUpdateImage names a digest no registry serves, as in the ENG-1313
// reproduction.
const unpullableUpdateImage = "ghcr.io/everclaw/everclaw@sha256:0000000000000000000000000000000000000000000000000000000000000000"

// deadAfterMaintenanceFixture is one Ready lease behind the backend API, with
// source capture bound as in production: either adopted from v0.13, so its
// release keeps the legacy authority class, or provisioned through the backend
// with typed authority. Docker reports each container running until the test
// kills it. Each Compose Up replaces the lease's cohort with containers that
// have new IDs and read back with their labels and immutable execution
// snapshot, so a re-provision never inherits a dead one.
type deadAfterMaintenanceFixture struct {
	b       *Backend
	items   []backend.LeaseItem
	payload []byte
	// maintenanceCallbackURL is the lifecycle route providerd sends with a
	// restart or update of this lease.
	maintenanceCallbackURL string
	reprovisionCallbackURL string

	mu         sync.Mutex
	callbacks  []backend.CallbackPayload
	dead       map[string]bool
	cohort     []ContainerInfo
	services   map[string]string // container ID -> Compose service
	generation int
}

func newDeadAfterMaintenanceFixture(t *testing.T, adopted bool) *deadAfterMaintenanceFixture {
	t.Helper()
	imageID := fixtureImageID("daemon-pulled nginx")
	stack := &manifest.StackManifest{Services: map[string]*manifest.Manifest{
		"app": {Image: "docker.io/library/nginx:1.26"},
	}}
	payload, err := json.Marshal(stack)
	require.NoError(t, err)
	f := &deadAfterMaintenanceFixture{
		items:    []backend.LeaseItem{{SKU: "docker-small", ServiceName: "app", Quantity: 1}},
		payload:  payload,
		dead:     make(map[string]bool),
		services: make(map[string]string),
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var callback backend.CallbackPayload
		_ = json.NewDecoder(r.Body).Decode(&callback)
		f.mu.Lock()
		f.callbacks = append(f.callbacks, callback)
		f.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	oldCallbackURL := server.URL + "/old/callbacks/provision"
	f.reprovisionCallbackURL = server.URL + "/new/callbacks/provision"

	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, image string, _ time.Duration) error {
			if image == unpullableUpdateImage {
				return errors.New("manifest unknown")
			}
			return nil
		},
		InspectImageFn: func(context.Context, string) (*ImageInfo, error) { return &ImageInfo{ID: imageID}, nil },
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			for _, container := range f.inventory() {
				if container.ContainerID == id {
					return &container, nil
				}
			}
			return nil, fmt.Errorf("no such container: %s", id)
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return f.inventory(), nil },
	}
	compose := &mockComposeExecutor{
		UpFn: func(_ context.Context, project *composetypes.Project, _ composeUpOpts) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.generation++
			launched := make([]ContainerInfo, 0, len(project.Services))
			for service, config := range project.Services {
				labels := config.Labels
				image, err := containerImageReference(config.Image, config.Image, labels)
				if err != nil {
					return err
				}
				index, err := strconv.Atoi(labels[LabelInstanceIndex])
				if err != nil {
					return err
				}
				createdAt, err := time.Parse(time.RFC3339, labels[LabelCreatedAt])
				if err != nil {
					return err
				}
				maintenanceID, err := parseContainerMaintenanceID(labels[LabelMaintenanceID])
				if err != nil {
					return err
				}
				id := fmt.Sprintf("%s-generation-%d", service, f.generation)
				info := ContainerInfo{
					ContainerID: id, Name: "fred-" + labels[LabelLeaseUUID] + "-" + service,
					LeaseUUID: labels[LabelLeaseUUID], Tenant: labels[LabelTenant],
					ProviderUUID: labels[LabelProviderUUID], BackendName: labels[LabelBackendName],
					SKU: labels[LabelSKU], ServiceName: labels[LabelServiceName], InstanceIndex: index,
					CallbackURL: labels[LabelCallbackURL], LifecycleCallbackURL: labels[LabelLifecycleCallbackURL],
					MaintenanceID: maintenanceID, Image: image, Status: "running", CreatedAt: createdAt,
					CustomDomain: labels[LabelCustomDomain],
				}
				info.execution = frozenCompensationFixture(info, config.Image)
				launched = append(launched, info)
				f.services[id] = service
			}
			f.cohort = launched
			return nil
		},
		PSFn: func(context.Context, string) ([]composeContainerSummary, error) {
			containers := f.inventory()
			f.mu.Lock()
			defer f.mu.Unlock()
			summaries := make([]composeContainerSummary, 0, len(containers))
			for _, container := range containers {
				summaries = append(summaries, composeContainerSummary{
					ID: container.ContainerID, Service: f.services[container.ContainerID], State: container.Status,
				})
			}
			return summaries, nil
		},
		DownFn: func(context.Context, string, time.Duration) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.cohort = nil
			return nil
		},
	}

	if adopted {
		f.b = newBackendForProvisionTest(t, mock, map[string]*provision{
			stackMaintenanceLeaseUUID: {ProvisionState: leasesm.ProvisionState{
				LeaseUUID: stackMaintenanceLeaseUUID, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
				Status: backend.ProvisionStatusReady, StackManifest: stack, Items: slices.Clone(f.items),
				ContainerIDs: []string{"v013-app-0"}, ServiceContainers: map[string][]string{"app": {"v013-app-0"}},
			}},
		})
		f.b.compose = compose
		seedLegacyStackMaintenanceAuthority(t, f.b, stackMaintenanceLeaseUUID, stack, f.items,
			oldCallbackURL, oldCallbackURL, server.Client())
		f.cohort = v013Workload{
			leaseUUID: stackMaintenanceLeaseUUID, tenant: "tenant-a", providerUUID: nominalDockerProviderUUID,
			callbackURL: oldCallbackURL, backendName: f.b.Name(), item: f.items[0],
			imageReference: stack.Services["app"].Image, imageID: imageID,
		}.cohort(t)
		f.services[f.cohort[0].ContainerID] = "app"
		// A v0.13 lease keeps its tokenless route: a maintenance carries the
		// callback base of its request.
		f.maintenanceCallbackURL = f.reprovisionCallbackURL
	} else {
		f.b = newBackendForProvisionTest(t, mock, nil)
		f.b.compose = compose
		rebuildCallbackSender(f.b, server.Client())
		f.b.wg.Go(f.b.callbackSender.RunReplayLoop)
		t.Cleanup(func() {
			f.b.stopCancel()
			f.b.wg.Wait()
		})
	}
	f.b.cfg.StartupVerifyDuration = time.Millisecond
	ops, err := storageMutationOperationsForTest(f.b)
	require.NoError(t, err)
	require.NoError(t, bindDockerMaintenanceCompensation(f.b, ops))
	if adopted {
		return f
	}

	operationURL := testOperationCallbackURL(oldCallbackURL)
	require.NoError(t, f.provision(t, operationURL))
	callback := f.awaitCallback(t, 0)
	require.Equal(t, backend.CallbackStatusSuccess, callback.Status, "callback error: %s", callback.Error)
	f.maintenanceCallbackURL, err = backend.ResolveLifecycleCallbackURL(operationURL, "")
	require.NoError(t, err)
	return f
}

// inventory is Docker's view of the lease: every container it still has, with
// the ones the test killed reported exited.
func (f *deadAfterMaintenanceFixture) inventory() []ContainerInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	inventory := slices.Clone(f.cohort)
	for index := range inventory {
		if f.dead[inventory[index].ContainerID] {
			inventory[index].Status = "exited"
			inventory[index].ExitCode = 137
		}
	}
	return inventory
}

// provision sends what providerd sends for the lease: its stored payload,
// which an update replaces only after the update succeeds.
func (f *deadAfterMaintenanceFixture) provision(t *testing.T, callbackURL string) error {
	t.Helper()
	return f.b.Provision(t.Context(), backend.ProvisionRequest{
		LeaseUUID: stackMaintenanceLeaseUUID, Tenant: "tenant-a", ProviderUUID: nominalDockerProviderUUID,
		Items: slices.Clone(f.items), CallbackURL: callbackURL, Payload: slices.Clone(f.payload),
	})
}

// reprovision is providerd's next pass over the ACTIVE lease once it is Failed.
func (f *deadAfterMaintenanceFixture) reprovision(t *testing.T) error {
	t.Helper()
	return f.provision(t, testOperationCallbackURL(f.reprovisionCallbackURL))
}

// awaitCallback waits for callback number previous+1 and for its durable row
// to leave the journal, then returns it.
func (f *deadAfterMaintenanceFixture) awaitCallback(t *testing.T, previous int) backend.CallbackPayload {
	t.Helper()
	require.Eventually(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.callbacks) > previous
	}, provisionFlowTimeout, time.Millisecond, "callback %d was never delivered", previous+1)
	require.Eventually(t, func() bool {
		pending, err := f.b.callbackStore.ListPending()
		return err == nil && len(pending) == 0
	}, provisionFlowTimeout, time.Millisecond, "callback delivery must remove its exact durable row")
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callbacks[previous]
}

func (f *deadAfterMaintenanceFixture) callbackCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.callbacks)
}

func (f *deadAfterMaintenanceFixture) status(t *testing.T) backend.ProvisionStatus {
	t.Helper()
	info, err := f.b.GetProvision(t.Context(), stackMaintenanceLeaseUUID)
	require.NoError(t, err)
	return info.Status
}

// maintain runs one restart or update and returns its settled callback.
func (f *deadAfterMaintenanceFixture) maintain(t *testing.T, maintenance string) backend.CallbackPayload {
	t.Helper()
	delivered := f.callbackCount()
	switch maintenance {
	case "failed update":
		require.NoError(t, f.b.Update(t.Context(), backend.UpdateRequest{
			MaintenanceID: newTestMaintenanceID(t), LeaseUUID: stackMaintenanceLeaseUUID,
			CallbackURL: f.maintenanceCallbackURL,
			Payload:     validStackManifestJSON(map[string]string{"app": unpullableUpdateImage}),
		}))
	case "successful restart":
		require.NoError(t, f.b.Restart(t.Context(), backend.RestartRequest{
			MaintenanceID: newTestMaintenanceID(t), LeaseUUID: stackMaintenanceLeaseUUID,
			CallbackURL: f.maintenanceCallbackURL,
		}))
	default:
		t.Fatalf("unknown maintenance %q", maintenance)
	}
	return f.awaitCallback(t, delivered)
}

// kill makes Docker report every container of the lease exited, as after an
// OOM kill or a host reboot, lets the periodic recovery pass observe it, and
// waits for the lifecycle failure that tells providerd to re-provision.
func (f *deadAfterMaintenanceFixture) kill(t *testing.T) {
	t.Helper()
	inventory, err := f.b.docker.ListManagedContainers(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, inventory)
	delivered := f.callbackCount()
	f.mu.Lock()
	for _, container := range inventory {
		f.dead[container.ContainerID] = true
	}
	f.mu.Unlock()
	require.NoError(t, f.b.recoverState(t.Context()))
	death := f.awaitCallback(t, delivered)
	require.Equal(t, backend.CallbackStatusFailed, death.Status)
	require.Equal(t, backend.ProvisionStatusFailed, f.status(t))
}

// requireReprovisioned checks that the accepted re-provision settled Ready on
// a typed release of the active manifest without erasing earlier history.
func (f *deadAfterMaintenanceFixture) requireReprovisioned(
	t *testing.T,
	delivered int,
	before []shared.Release,
) {
	t.Helper()
	settled := f.awaitCallback(t, delivered)
	require.Equal(t, backend.CallbackStatusSuccess, settled.Status, "callback error: %s", settled.Error)
	require.Equal(t, backend.ProvisionStatusReady, f.status(t))
	active, err := f.b.releaseStore.LatestActive(stackMaintenanceLeaseUUID)
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.True(t, active.OperationID.Valid(), "the re-provision commits a typed release")
	assert.Equal(t, f.payload, active.Manifest, "the re-provision runs the active release's manifest")
	after, err := f.b.releaseStore.List(stackMaintenanceLeaseUUID)
	require.NoError(t, err)
	for _, release := range before {
		if release.MaintenanceID.IsZero() {
			continue
		}
		assert.True(t, slices.ContainsFunc(after, func(kept shared.Release) bool {
			return kept.MaintenanceID == release.MaintenanceID
		}), "maintenance row %s stays in the history", release.MaintenanceID)
	}
}

// A lease adopted from v0.13 keeps the legacy authority class through every
// restart or update, failed or successful, so its release history gains a
// maintenance row while its runtime authority is already durable. When the
// lease's container later dies, providerd re-provisions it from the active
// release, and that re-provision must be accepted and succeed (ENG-1313). A
// typed lease takes the same path.
func TestLeaseReprovisionsAfterMaintenanceAndContainerDeath(t *testing.T) {
	for _, tc := range []struct {
		name        string
		adopted     bool
		maintenance string
		outcome     backend.CallbackStatus
	}{
		{"adopted v0.13 lease, failed update", true, "failed update", backend.CallbackStatusFailed},
		{"adopted v0.13 lease, successful restart", true, "successful restart", backend.CallbackStatusSuccess},
		{"typed lease, failed update", false, "failed update", backend.CallbackStatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeadAfterMaintenanceFixture(t, tc.adopted)
			maintained := f.maintain(t, tc.maintenance)
			require.Equal(t, tc.outcome, maintained.Status, "callback error: %s", maintained.Error)
			if tc.outcome == backend.CallbackStatusFailed {
				require.Equal(t, backend.MsgImagePullFailed, maintained.Error)
			}
			require.Equal(t, backend.ProvisionStatusReady, f.status(t))
			history, err := f.b.releaseStore.List(stackMaintenanceLeaseUUID)
			require.NoError(t, err)
			require.True(t, history[len(history)-1].MaintenanceID.Valid(),
				"the maintenance row is the lease's newest release")
			if tc.adopted {
				active, err := f.b.releaseStore.LatestActive(stackMaintenanceLeaseUUID)
				require.NoError(t, err)
				require.NotNil(t, active.LegacyRuntimeAuthority, "the lease stays in the legacy authority class")
			}

			f.kill(t)
			delivered := f.callbackCount()
			require.NoError(t, f.reprovision(t), "the dead lease re-provisions from its active release")
			f.requireReprovisioned(t, delivered, history)
		})
	}
}

// Leases trapped before the fix were refused on every pass, each refusal
// settled before acceptance. That settlement leaves nothing that blocks the
// next pass, so an upgraded backend re-provisions them on its own: no repair
// step is needed.
func TestRefusedReprovisionOfDeadLeaseSucceedsOnTheNextPass(t *testing.T) {
	f := newDeadAfterMaintenanceFixture(t, true)
	maintained := f.maintain(t, "failed update")
	require.Equal(t, backend.CallbackStatusFailed, maintained.Status)
	history, err := f.b.releaseStore.List(stackMaintenanceLeaseUUID)
	require.NoError(t, err)
	f.kill(t)

	delivered := f.callbackCount()
	hold := f.b.pool.HoldUnaccountedFootprint()
	err = f.reprovision(t)
	hold.Release()
	require.ErrorIs(t, err, backend.ErrInsufficientResources, "this pass is refused after its intent is durable")
	refused := f.awaitCallback(t, delivered)
	require.Equal(t, backend.CallbackStatusFailed, refused.Status)
	require.Equal(t, backend.ProvisionStatusFailed, f.status(t))

	delivered = f.callbackCount()
	require.NoError(t, f.reprovision(t), "the next pass is accepted")
	f.requireReprovisioned(t, delivered, history)
}
