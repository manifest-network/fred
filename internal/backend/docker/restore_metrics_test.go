package docker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/prometheus/client_golang/prometheus"
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
}
