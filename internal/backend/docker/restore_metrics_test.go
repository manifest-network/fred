package docker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
)

const (
	restoreMetricsSourceLease = "11111111-1111-4111-8111-111111111111"
	restoreMetricsTargetLease = "22222222-2222-4222-8222-222222222222"
)

// observerSampleCount reads the number of observations recorded by a prometheus
// histogram (or histogram-vec child). The metrics are package-global and
// persist across tests in this package, so callers compare a before/after delta
// rather than an absolute count.
func observerSampleCount(t *testing.T, o prometheus.Observer) uint64 {
	t.Helper()
	m, ok := o.(prometheus.Metric)
	require.True(t, ok, "observer must implement prometheus.Metric to be readable")
	var d dto.Metric
	require.NoError(t, m.Write(&d))
	return d.GetHistogram().GetSampleCount()
}

// TestReplaceContainers_RecordsPhaseDurationsByOperation pins the per-phase
// instrumentation inside the shared replace machinery: a successful
// doReplaceContainers must record exactly one observation for each of its four
// internal phases (image_setup, volume_setup, compose_up, verify_startup),
// labeled by the operation it was invoked for ("restart" here).
func TestReplaceContainers_RecordsPhaseDurationsByOperation(t *testing.T) {
	var strictContainers []ContainerInfo
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			for _, container := range strictContainers {
				if container.ContainerID == id {
					observed := container
					return &observed, nil
				}
			}
			return nil, errors.New("container is absent")
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) {
			return append([]ContainerInfo(nil), strictContainers...), nil
		},
	}
	b := newBackendForProvisionTest(t, mock, nil)
	defer b.stopCancel()
	b.compose = &mockComposeExecutor{
		UpFn: func(_ context.Context, _ *composetypes.Project, _ composeUpOpts) error { return nil },
		PSFn: func(_ context.Context, _ string) ([]composeContainerSummary, error) {
			return []composeContainerSummary{
				{ID: "c1", Service: manifest.DefaultServiceName, State: "running"},
			}, nil
		},
	}
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond

	const (
		leaseUUID    = "11111111-1111-4111-8111-111111111113"
		providerUUID = "22222222-2222-4222-8222-222222222223"
	)
	items := []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}}
	profiles := testResourceProfiles(t, items)
	stack := restoreStackManifest()
	manifestBytes, err := json.Marshal(stack)
	require.NoError(t, err)
	operationID, callbackURL, lifecycleCallbackURL := newTestRestoreCallbackAuthority(t)
	runtimeAuthority := mustTestReleaseRuntimeAuthority(
		t, operationID, "tenant-a", providerUUID, callbackURL, lifecycleCallbackURL,
	)

	attachBoundOperationHandoffStores(t, b)
	seedProvisionReleaseForBackendTest(t, b, leaseUUID, shared.Release{
		Manifest:         manifestBytes,
		Image:            "stack",
		OperationID:      operationID,
		Items:            items,
		ResourceProfiles: profiles,
		RuntimeAuthority: runtimeAuthority,
		Status:           "active",
		CreatedAt:        time.Now(),
	})
	settlement := b.maintenanceSettlement
	active, sourceClaim, err := settlement.ClaimLatestActive(leaseUUID)
	require.NoError(t, err)
	_ = active
	targetTemplate := shared.Release{
		Manifest:         manifestBytes,
		Image:            "stack",
		OperationID:      operationID,
		Items:            items,
		ResourceProfiles: profiles,
		RuntimeAuthority: runtimeAuthority,
		Status:           "deploying",
		CreatedAt:        time.Now(),
	}
	admission, err := settlement.BeginMaintenanceIntent(newTestMaintenanceIntentSpec(
		t, settlement, newTestMaintenanceID(t), shared.MaintenanceIntentRestart,
		sourceClaim, targetTemplate,
	))
	require.NoError(t, err)
	appendClaim, err := settlement.StartMaintenanceAppend(
		createdTestMaintenanceDispatch(t, admission),
	)
	require.NoError(t, err)
	targetRelease, err := settlement.AppendMaintenance(appendClaim)
	require.NoError(t, err)
	targetRelease, err = settlement.BindMaintenanceIntentTarget(targetRelease)
	require.NoError(t, err)
	maintenance := targetRelease.Intent()
	strictContainers = []ContainerInfo{{
		ContainerID:          "c1",
		BackendName:          b.cfg.Name,
		LeaseUUID:            leaseUUID,
		Tenant:               "tenant-a",
		ProviderUUID:         providerUUID,
		SKU:                  "docker-small",
		ServiceName:          manifest.DefaultServiceName,
		InstanceIndex:        0,
		CallbackURL:          callbackURL,
		LifecycleCallbackURL: lifecycleCallbackURL,
		MaintenanceID:        maintenance.MaintenanceID(),
		Image:                "nginx:latest",
		Status:               "running",
		CreatedAt:            time.Now().Add(-time.Minute),
	}}

	phases := []string{phaseImageSetup, phaseVolumeSetup, phaseComposeUp, phaseVerifyStartup}
	before := make(map[string]uint64, len(phases))
	for _, p := range phases {
		before[p] = observerSampleCount(t, replacePhaseDurationSeconds.WithLabelValues("restart", p))
	}

	execution, err := settlement.StartMaintenanceExecution(targetRelease)
	require.NoError(t, err)
	physical := settlement.ExecuteMaintenance(context.Background(), execution)
	success, ok := physical.(shared.MaintenanceExecutionSuccess)
	if !ok {
		var cause error
		switch outcome := physical.(type) {
		case shared.MaintenanceExecutionFailure:
			cause = outcome.Cause()
		case shared.MaintenanceExecutionAmbiguous:
			cause = outcome.Cause()
		}
		t.Fatalf("construction-bound replacement did not produce exact success (%T): %v", physical, cause)
	}
	_, err = settlement.ActivateMaintenance(success)
	require.NoError(t, err)
	current, found, err := settlement.GetMaintenanceIntent(leaseUUID)
	require.NoError(t, err)
	require.True(t, found)
	_, err = settlement.ProveMaintenanceActive(current)
	require.NoError(t, err, "construction-bound replacement must activate its exact target")

	for _, p := range phases {
		after := observerSampleCount(t, replacePhaseDurationSeconds.WithLabelValues("restart", p))
		assert.Equalf(t, before[p]+1, after,
			"phase %q must record exactly one observation under operation=restart", p)
	}
}

// TestRestore_RecordsRestoreDurationAndPhases pins the restore-specific
// instrumentation across a full successful Restore():
//   - restore_duration_seconds records one observation (the async re-deploy worker
//     span, success only; excludes the adopt prelude),
//   - the adopt phase is recorded under operation=restore (volume rename in the
//     synchronous prelude), and
//   - the shared compose_up phase is recorded under operation=restore (proving the
//     operation label flows through doReplaceContainers on the restore path).
func TestRestore_RecordsRestoreDurationAndPhases(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: id, Status: "running"}, nil
		},
		ListManagedContainersFn: func(context.Context) ([]ContainerInfo, error) { return nil, nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	// Defer teardown so the backend's goroutines (lease actor, restore worker,
	// reaper) are cancelled and awaited even when an assertion below fails early.
	defer func() { b.stopCancel(); b.wg.Wait() }()
	rs := attachRetentionStore(t, b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	seedActiveRetained(t, rs, restoreMetricsSourceLease)

	var mu sync.Mutex
	var downProjects []string
	compose := happyComposeMock(t, mock, &mu, &downProjects, nil)
	b.compose = compose
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { return nil },
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	durBefore := observerSampleCount(t, restoreDurationSeconds)
	adoptBefore := observerSampleCount(t, replacePhaseDurationSeconds.WithLabelValues("restore", phaseAdopt))
	composeBefore := observerSampleCount(t, replacePhaseDurationSeconds.WithLabelValues("restore", phaseComposeUp))
	succBefore := testutil.ToFloat64(restoresTotal.WithLabelValues("success"))
	failBefore := testutil.ToFloat64(restoresTotal.WithLabelValues("failure"))

	err := b.Restore(context.Background(), restoreRequest(
		restoreMetricsTargetLease,
		restoreMetricsSourceLease,
		server.URL+"/callbacks/provision",
	))
	require.NoError(t, err)

	// The restore worker records restore_duration_seconds in its terminal defer,
	// which runs before the actor flips Status=Ready — so this bounded wait for
	// Ready is a sufficient, fail-fast gate for the metric assertions below.
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		p, ok := b.provisions[restoreMetricsTargetLease]
		return ok && p.Status == backend.ProvisionStatusReady
	}, 5*time.Second, 20*time.Millisecond, "restore target must reach Ready")

	assert.Equal(t, durBefore+1, observerSampleCount(t, restoreDurationSeconds),
		"restore_duration_seconds must record exactly one observation on a successful restore")
	assert.Equal(t, adoptBefore+1,
		observerSampleCount(t, replacePhaseDurationSeconds.WithLabelValues("restore", phaseAdopt)),
		"adopt phase must be recorded under operation=restore")
	assert.Equal(t, composeBefore+1,
		observerSampleCount(t, replacePhaseDurationSeconds.WithLabelValues("restore", phaseComposeUp)),
		"compose_up phase must be recorded under operation=restore")
	assert.Equal(t, succBefore+1, testutil.ToFloat64(restoresTotal.WithLabelValues("success")),
		"a successful restore must increment restore_total{outcome=\"success\"}")
	assert.Equal(t, failBefore, testutil.ToFloat64(restoresTotal.WithLabelValues("failure")),
		"a successful restore must not increment restore_total{outcome=\"failure\"}")
}

// TestRestore_PostEffectErrorDoesNotManufactureTerminalMetric drives a Compose
// Up error. The request may already have reached Docker, so this is not a
// definitive failure until durable recovery classifies a fresh inventory.
func TestRestore_PostEffectErrorDoesNotManufactureTerminalMetric(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	defer func() { b.stopCancel(); b.wg.Wait() }()
	rs := attachRetentionStore(t, b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	seedActiveRetained(t, rs, restoreMetricsSourceLease)

	var mu sync.Mutex
	var downProjects []string
	b.compose = happyComposeMock(t, mock, &mu, &downProjects, errors.New("compose up boom"))
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { return nil },
	}

	succBefore := testutil.ToFloat64(restoresTotal.WithLabelValues("success"))
	failBefore := testutil.ToFloat64(restoresTotal.WithLabelValues("failure"))
	durBefore := observerSampleCount(t, restoreDurationSeconds)

	err := b.Restore(context.Background(), restoreRequest(
		restoreMetricsTargetLease,
		restoreMetricsSourceLease,
		"http://127.0.0.1:1/callbacks/provision",
	))
	require.NoError(t, err) // route+ack succeed; the failure is asynchronous

	awaitProvisionWorkerQuiescence(t, b, restoreMetricsTargetLease)

	assert.Equal(t, failBefore, testutil.ToFloat64(restoresTotal.WithLabelValues("failure")),
		"an ambiguous Compose result must not be mislabeled as failure")
	assert.Equal(t, succBefore, testutil.ToFloat64(restoresTotal.WithLabelValues("success")),
		"an ambiguous Compose result must not be mislabeled as success")
	assert.Equal(t, durBefore, observerSampleCount(t, restoreDurationSeconds),
		"restore_duration_seconds is success-only and must not move on ambiguity")
}

// TestRestore_WorkerPanic_DoesNotManufactureTerminalMetric drives a panic at
// the post-effect Compose Up boundary. Because the result is ambiguous until a
// fresh inventory classifies it, neither terminal metric may move.
func TestRestore_WorkerPanic_DoesNotManufactureTerminalMetric(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	defer func() { b.stopCancel(); b.wg.Wait() }()
	rs := attachRetentionStore(t, b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	seedActiveRetained(t, rs, restoreMetricsSourceLease)

	b.compose = &mockComposeExecutor{
		UpFn: func(_ context.Context, _ *composetypes.Project, _ composeUpOpts) error {
			panic("induced restore worker panic")
		},
		DownFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
	}
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { return nil },
	}

	succBefore := testutil.ToFloat64(restoresTotal.WithLabelValues("success"))
	failBefore := testutil.ToFloat64(restoresTotal.WithLabelValues("failure"))

	err := b.Restore(context.Background(), restoreRequest(
		restoreMetricsTargetLease,
		restoreMetricsSourceLease,
		"http://127.0.0.1:1/callbacks/provision",
	))
	require.NoError(t, err) // route+ack succeed; the panic is asynchronous
	awaitProvisionWorkerQuiescence(t, b, restoreMetricsTargetLease)

	assert.Equal(t, failBefore, testutil.ToFloat64(restoresTotal.WithLabelValues("failure")),
		"an ambiguous restore must not be mislabeled as a definitive failure")
	assert.Equal(t, succBefore, testutil.ToFloat64(restoresTotal.WithLabelValues("success")),
		"an ambiguous restore must not be mislabeled as success")
}

// TestRestore_AdoptFailureDoesNotManufactureTerminalMetric pins the durable
// worker boundary of restore_total. Adoption is now an effect of the Started
// operation subject, after actor acceptance, so Restore returns successfully
// while a rename error remains ambiguous for exact recovery. Neither terminal
// outcome may be recorded from that transport/storage error alone.
func TestRestore_AdoptFailureDoesNotManufactureTerminalMetric(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	defer func() { b.stopCancel(); b.wg.Wait() }()
	rs := attachRetentionStore(t, b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	seedActiveRetained(t, rs, restoreMetricsSourceLease)

	// Fail the Started adopt rename (retained→canonical). The actor owns the
	// accepted operation, and exact recovery—not the raw error—decides its outcome.
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { return assert.AnError },
	}

	succBefore := testutil.ToFloat64(restoresTotal.WithLabelValues("success"))
	failBefore := testutil.ToFloat64(restoresTotal.WithLabelValues("failure"))

	err := b.Restore(context.Background(), restoreRequest(
		restoreMetricsTargetLease,
		restoreMetricsSourceLease,
		"http://127.0.0.1:1/callbacks/provision",
	))
	require.NoError(t, err, "actor acceptance must not be confused with the later physical outcome")
	awaitProvisionWorkerQuiescence(t, b, restoreMetricsTargetLease)

	assert.Equal(t, succBefore, testutil.ToFloat64(restoresTotal.WithLabelValues("success")),
		"an ambiguous adopt failure must not increment restore_total{outcome=\"success\"}")
	assert.Equal(t, failBefore, testutil.ToFloat64(restoresTotal.WithLabelValues("failure")),
		"an ambiguous adopt failure must not be manufactured into a terminal failure")
}
