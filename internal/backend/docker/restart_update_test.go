package docker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
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

// --- ENG-230: prelude no longer speculatively writes prov.Status/CallbackURL ---

// TestRestart_RoutingFailureLeavesStatusUnchanged pins that the Restart
// prelude performs NO speculative write of prov.Status / prov.CallbackURL:
// when the handoff to the lease actor fails (here: backend shutting down,
// so routeToLeaseBlocking fails fast), the lease must remain exactly as it
// was. Pre-ENG-230 this invariant was upheld by restartRollback restoring
// the fields; post-ENG-230 it holds because the prelude never wrote them.
func TestRestart_RoutingFailureLeavesStatusUnchanged(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:     "lease-1",
			Status:        backend.ProvisionStatusReady,
			CallbackURL:   "old-cb",
			StackManifest: &manifest.StackManifest{},
		}},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	// Cancel stopCtx so routeToLeaseBlocking fails fast with "backend
	// shutting down" before any actor work — exercising the routing-fail
	// error path that previously triggered restartRollback.
	b.stopCancel()

	err := b.Restart(context.Background(), backend.RestartRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: "new-cb",
	})
	require.Error(t, err)

	b.provisionsMu.RLock()
	status := b.provisions["lease-1"].Status
	callbackURL := b.provisions["lease-1"].CallbackURL
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, status,
		"routing failure must leave Status unchanged (no speculative write)")
	assert.Equal(t, "old-cb", callbackURL,
		"routing failure must leave CallbackURL unchanged (no speculative write)")
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
		"lease-1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:     "lease-1",
			Tenant:        "tenant-a",
			Status:        backend.ProvisionStatusReady,
			ContainerIDs:  []string{"c1"},
			CallbackURL:   callbackServer.URL,
			StackManifest: &manifest.StackManifest{},
		}},
	}
	b := newBackendForTest(mock, provisions)
	defer b.stopCancel()
	// Inject the blocking gatherer BEFORE the actor is created (first
	// routeToLease below) so the diag goroutine spawned by onEnterFailing
	// can't fire diagGatheredMsg and flip Failing→Failed before the restart.
	b.gatherer = blockingDiagGatherer{}

	// 1) Container death: Ready→Failing, onEnterFailing runs to completion.
	b.handleContainerDeath("c1")

	b.provisionsMu.RLock()
	statusAfterDeath := b.provisions["lease-1"].Status
	failCount := b.provisions["lease-1"].FailCount
	b.provisionsMu.RUnlock()
	require.Equal(t, backend.ProvisionStatusFailing, statusAfterDeath,
		"container death must drive the lease to Failing")
	require.Equal(t, 1, failCount, "onEnterFailing must bump FailCount to 1")

	// 2) Restart: route directly to the actor. (The b.Restart prelude
	// fast-fails a Failing lease, but the SM permits Failing→Restarting —
	// exactly the serial-ordering path this test exercises.)
	var workerCount atomic.Int64
	workerRelease := make(chan struct{})
	ack := make(chan error, 1)
	require.True(t, b.routeToLease("lease-1", leasesm.RestartRequestedMsg{
		Cancel:      func() {},
		CallbackURL: callbackServer.URL,
		Work: func() leasesm.ReplaceResult {
			workerCount.Add(1)
			<-workerRelease
			return leasesm.ReplaceResult{Success: leasesm.ReplaceSuccessResult{PrevStatus: backend.ProvisionStatusFailed}}
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
	statusAfterRestart := b.provisions["lease-1"].Status
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusRestarting, statusAfterRestart,
		"restart from Failing must end in Restarting via serial ordering (no recheck)")
	assert.Equal(t, backend.ProvisionStatusRestarting, b.actorFor("lease-1").State(),
		"SM must be in Restarting with the replace worker spawned")

	close(workerRelease) // let the worker complete so the actor can quiesce
	require.Eventually(t, func() bool { return workerCount.Load() == 1 }, time.Second, 5*time.Millisecond,
		"exactly one replace worker must be spawned")
}

// TestUpdate_RoutingFailureLeavesStatusUnchanged is the Update mirror of
// TestRestart_RoutingFailureLeavesStatusUnchanged.
func TestUpdate_RoutingFailureLeavesStatusUnchanged(t *testing.T) {
	provisions := map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID:   "lease-1",
			Status:      backend.ProvisionStatusReady,
			SKU:         "docker-small",
			CallbackURL: "old-cb",
			Items:       []backend.LeaseItem{{SKU: "docker-small", Quantity: 1}},
		}},
	}
	b := newBackendForTest(&mockDockerClient{}, provisions)
	b.stopCancel()

	err := b.Update(context.Background(), backend.UpdateRequest{
		LeaseUUID:   "lease-1",
		CallbackURL: "new-cb",
		Payload:     validManifestJSON("nginx:latest"),
	})
	require.Error(t, err)

	b.provisionsMu.RLock()
	status := b.provisions["lease-1"].Status
	callbackURL := b.provisions["lease-1"].CallbackURL
	b.provisionsMu.RUnlock()
	assert.Equal(t, backend.ProvisionStatusReady, status,
		"routing failure must leave Status unchanged (no speculative write)")
	assert.Equal(t, "old-cb", callbackURL,
		"routing failure must leave CallbackURL unchanged (no speculative write)")
}
