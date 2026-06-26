package docker

import (
	"context"
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
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
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
	mock := &mockDockerClient{
		InspectContainerFn: func(_ context.Context, id string) (*ContainerInfo, error) {
			return &ContainerInfo{ContainerID: id, Status: "running"}, nil
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

	profile, err := b.cfg.GetSKUProfile("docker-small")
	require.NoError(t, err)

	op := replaceContainersOp{
		LeaseUUID: "lease-1",
		Stack:     restoreStackManifest(),
		Items:     []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}},
		Profiles:  map[string]SKUProfile{"docker-small": profile},
		Operation: "restart",
		Logger:    b.logger,
	}

	phases := []string{phaseImageSetup, phaseVolumeSetup, phaseComposeUp, phaseVerifyStartup}
	before := make(map[string]uint64, len(phases))
	for _, p := range phases {
		before[p] = observerSampleCount(t, replacePhaseDurationSeconds.WithLabelValues("restart", p))
	}

	res := b.doReplaceContainers(context.Background(), op)
	require.NoError(t, res.Err, "replace must succeed")

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
	}
	b := newBackendForProvisionTest(t, mock, nil)
	// Defer teardown so the backend's goroutines (lease actor, restore worker,
	// reaper) are cancelled and awaited even when an assertion below fails early.
	defer func() { b.stopCancel(); b.wg.Wait() }()
	rs := attachRetentionStore(t, b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	seedActiveRetained(t, rs, "u1")

	var mu sync.Mutex
	var downProjects []string
	b.compose = happyComposeMock(&mu, &downProjects, nil)
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

	err := b.Restore(context.Background(), restoreRequest("u2", "u1", server.URL))
	require.NoError(t, err)

	// The restore worker records restore_duration_seconds in its terminal defer,
	// which runs before the actor flips Status=Ready — so this bounded wait for
	// Ready is a sufficient, fail-fast gate for the metric assertions below.
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		p, ok := b.provisions["u2"]
		return ok && p.Status == backend.ProvisionStatusReady
	}, 5*time.Second, 20*time.Millisecond, "u2 must reach Ready")

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

// TestRestore_FailedWorker_IncrementsRestoreTotalFailure drives a restore whose
// re-deploy worker FAILS (compose Up errors) and asserts the worker's terminal
// defer increments restore_total{outcome="failure"} exactly once — the gap ENG-408
// closes (a failed restore previously incremented nothing on the docker-backend
// side). It also pins the success-only invariant of restore_duration_seconds: that
// histogram must NOT move on a failure.
func TestRestore_FailedWorker_IncrementsRestoreTotalFailure(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	defer func() { b.stopCancel(); b.wg.Wait() }()
	rs := attachRetentionStore(t, b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	seedActiveRetained(t, rs, "u1")

	var mu sync.Mutex
	var downProjects []string
	b.compose = happyComposeMock(&mu, &downProjects, errors.New("compose up boom"))
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { return nil },
	}

	succBefore := testutil.ToFloat64(restoresTotal.WithLabelValues("success"))
	failBefore := testutil.ToFloat64(restoresTotal.WithLabelValues("failure"))
	durBefore := observerSampleCount(t, restoreDurationSeconds)

	err := b.Restore(context.Background(), restoreRequest("u2", "u1", "http://127.0.0.1:0/cb"))
	require.NoError(t, err) // route+ack succeed; the failure is asynchronous

	// The worker's terminal defer (which increments restore_total) runs before the
	// actor flips Status=Failed, so waiting for Failed is a sufficient gate.
	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		p, ok := b.provisions["u2"]
		return ok && p.Status == backend.ProvisionStatusFailed
	}, 5*time.Second, 20*time.Millisecond, "u2 must settle Failed")

	assert.Equal(t, failBefore+1, testutil.ToFloat64(restoresTotal.WithLabelValues("failure")),
		"a failed restore worker must increment restore_total{outcome=\"failure\"}")
	assert.Equal(t, succBefore, testutil.ToFloat64(restoresTotal.WithLabelValues("success")),
		"a failed restore must not increment restore_total{outcome=\"success\"}")
	assert.Equal(t, durBefore, observerSampleCount(t, restoreDurationSeconds),
		"restore_duration_seconds is success-only and must not move on a failed restore")
}

// TestRestore_WorkerPanic_IncrementsRestoreTotalFailure drives a restore whose
// worker PANICS (compose Up panics) and asserts the panic-recovery branch of the
// terminal defer counts the panic as a failure: restore_total{outcome="failure"}
// increments, and a panic is never mistaken for success.
func TestRestore_WorkerPanic_IncrementsRestoreTotalFailure(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	defer func() { b.stopCancel(); b.wg.Wait() }()
	rs := attachRetentionStore(t, b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	seedActiveRetained(t, rs, "u1")

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

	err := b.Restore(context.Background(), restoreRequest("u2", "u1", "http://127.0.0.1:0/cb"))
	require.NoError(t, err) // route+ack succeed; the panic is asynchronous

	require.Eventually(t, func() bool {
		b.provisionsMu.RLock()
		defer b.provisionsMu.RUnlock()
		p, ok := b.provisions["u2"]
		return ok && p.Status == backend.ProvisionStatusFailed
	}, 5*time.Second, 20*time.Millisecond, "u2 must settle Failed after panic")

	assert.Equal(t, failBefore+1, testutil.ToFloat64(restoresTotal.WithLabelValues("failure")),
		"a restore worker panic must increment restore_total{outcome=\"failure\"}")
	assert.Equal(t, succBefore, testutil.ToFloat64(restoresTotal.WithLabelValues("success")),
		"a panicked restore must not increment restore_total{outcome=\"success\"}")
}

// TestRestore_SyncAdoptFailure_DoesNotIncrementRestoreTotal pins the WORKER-ONLY
// scope of restore_total: a restore that fails in the synchronous adopt prelude
// (the retained→canonical rename) BEFORE the async worker spawns returns a
// synchronous error to the caller and must increment NEITHER outcome — mirroring
// provisionsTotal, which counts only doProvision's worker outcome. This locks an
// intentional boundary: such synchronous failures surface to the tenant as the
// Restore() error and are deliberately excluded from this worker success-rate
// counter (and are likewise not counted by providerd's ProvisioningTotal).
func TestRestore_SyncAdoptFailure_DoesNotIncrementRestoreTotal(t *testing.T) {
	mock := &mockDockerClient{
		PullImageFn: func(_ context.Context, _ string, _ time.Duration) error { return nil },
	}
	b := newBackendForProvisionTest(t, mock, nil)
	defer func() { b.stopCancel(); b.wg.Wait() }()
	rs := attachRetentionStore(t, b)
	b.cfg.StartupVerifyDuration = 10 * time.Millisecond
	seedActiveRetained(t, rs, "u1")

	// Fail the synchronous adopt rename (retained→canonical): Restore() returns an
	// error before doRestore spawns, so neither outcome series moves.
	b.volumes = &mockVolumeManager{
		RenameVolumeFn: func(_, _ string) error { return assert.AnError },
	}

	succBefore := testutil.ToFloat64(restoresTotal.WithLabelValues("success"))
	failBefore := testutil.ToFloat64(restoresTotal.WithLabelValues("failure"))

	err := b.Restore(context.Background(), restoreRequest("u2", "u1", "http://127.0.0.1:0/cb"))
	require.Error(t, err, "a synchronous adopt-rename failure must return an error from Restore()")

	assert.Equal(t, succBefore, testutil.ToFloat64(restoresTotal.WithLabelValues("success")),
		"a synchronous (pre-worker) restore failure must not increment restore_total{outcome=\"success\"}")
	assert.Equal(t, failBefore, testutil.ToFloat64(restoresTotal.WithLabelValues("failure")),
		"restore_total is worker-scoped: a synchronous adopt failure (worker never spawns) must not increment it")
}
