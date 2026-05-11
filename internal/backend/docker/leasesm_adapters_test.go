package docker

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// TestDockerStatusToPhase verifies the truth-table mirroring of
// containerStatusToProvisionStatus for the SM's "terminally gone?"
// decision. Statuses that previously mapped to ProvisionStatusFailed
// must end up in PhaseExited or PhaseFailed; everything else maps to
// PhaseRunning or PhaseUnknown so the SM does not treat them as
// terminal.
func TestDockerStatusToPhase(t *testing.T) {
	cases := []struct {
		status string
		want   leasesm.Phase
	}{
		{"running", leasesm.PhaseRunning},
		{"paused", leasesm.PhaseRunning},
		{"RUNNING", leasesm.PhaseRunning},
		{"exited", leasesm.PhaseExited},
		{"removing", leasesm.PhaseFailed},
		{"dead", leasesm.PhaseFailed},
		{"created", leasesm.PhaseUnknown},
		{"restarting", leasesm.PhaseUnknown},
		{"", leasesm.PhaseUnknown},
		{"bogus", leasesm.PhaseUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.status, func(t *testing.T) {
			assert.Equal(t, tc.want, dockerStatusToPhase(tc.status))
		})
	}
}

// TestContainerInfoToInstanceState_NilInput returns nil; behavior the
// adapter relies on so a docker call that legitimately returns nil
// info doesn't synthesize a fake state.
func TestContainerInfoToInstanceState_NilInput(t *testing.T) {
	assert.Nil(t, containerInfoToInstanceState(nil))
}

// TestContainerInfoToInstanceState_RunningHasNoExitCode preserves the
// "ExitCode is meaningful only after termination" semantic — Docker
// reports 0 for still-running containers, and that 0 must not surface
// as a successful clean exit on the substrate-agnostic side.
func TestContainerInfoToInstanceState_RunningHasNoExitCode(t *testing.T) {
	info := &ContainerInfo{Status: "running", ExitCode: 0}
	state := containerInfoToInstanceState(info)
	require.NotNil(t, state)
	assert.Equal(t, leasesm.PhaseRunning, state.Phase)
	assert.Nil(t, state.ExitCode, "still-running containers must not carry an ExitCode pointer")
}

// TestContainerInfoToInstanceState_ExitedAttachesExitCode covers the
// terminal path: ExitCode is wrapped into a *int so callers can
// distinguish "not yet exited" (nil) from "exited with code 0".
func TestContainerInfoToInstanceState_ExitedAttachesExitCode(t *testing.T) {
	info := &ContainerInfo{Status: "exited", ExitCode: 137, OOMKilled: true}
	state := containerInfoToInstanceState(info)
	require.NotNil(t, state)
	assert.Equal(t, leasesm.PhaseExited, state.Phase)
	require.NotNil(t, state.ExitCode)
	assert.Equal(t, 137, *state.ExitCode)
	assert.True(t, state.OOMKilled)
}

// TestContainerInfoToInstanceState_FailedStatusAlsoAttachesExitCode
// covers the docker-removing/dead → PhaseFailed branch.
func TestContainerInfoToInstanceState_FailedStatusAlsoAttachesExitCode(t *testing.T) {
	info := &ContainerInfo{Status: "dead", ExitCode: 1}
	state := containerInfoToInstanceState(info)
	require.NotNil(t, state)
	assert.Equal(t, leasesm.PhaseFailed, state.Phase)
	require.NotNil(t, state.ExitCode)
	assert.Equal(t, 1, *state.ExitCode)
}

// TestDockerInstanceInspector_InspectInstance verifies the inspector
// wraps DockerClient.InspectContainer and converts via the helper.
func TestDockerInstanceInspector_InspectInstance(t *testing.T) {
	t.Run("happy path returns translated state", func(t *testing.T) {
		mock := &mockDockerClient{
			InspectContainerFn: func(ctx context.Context, cid string) (*ContainerInfo, error) {
				assert.Equal(t, "c1", cid)
				return &ContainerInfo{Status: "exited", ExitCode: 42}, nil
			},
		}
		insp := &dockerInstanceInspector{docker: mock}
		state, err := insp.InspectInstance(context.Background(), "c1")
		require.NoError(t, err)
		require.NotNil(t, state)
		assert.Equal(t, leasesm.PhaseExited, state.Phase)
		require.NotNil(t, state.ExitCode)
		assert.Equal(t, 42, *state.ExitCode)
	})

	t.Run("propagates inspect error", func(t *testing.T) {
		want := errors.New("inspect failed")
		mock := &mockDockerClient{
			InspectContainerFn: func(ctx context.Context, cid string) (*ContainerInfo, error) {
				return nil, want
			},
		}
		insp := &dockerInstanceInspector{docker: mock}
		state, err := insp.InspectInstance(context.Background(), "c1")
		require.ErrorIs(t, err, want)
		assert.Nil(t, state)
	})
}

// TestDockerDiagnosticsGatherer_GatherDiagnostics confirms the gatherer
// is a thin pass-through to Backend.containerFailureDiagnostics.
func TestDockerDiagnosticsGatherer_GatherDiagnostics(t *testing.T) {
	mock := &mockDockerClient{
		ContainerLogsFn: func(ctx context.Context, cid string, tail int) (string, error) {
			return "boom", nil
		},
	}
	b := newBackendForTest(mock, nil)
	g := &dockerDiagnosticsGatherer{backend: b}

	exitCode := 9
	state := &leasesm.InstanceState{Phase: leasesm.PhaseFailed, ExitCode: &exitCode, OOMKilled: true}
	got := g.GatherDiagnostics(context.Background(), "c1", state)

	assert.Contains(t, got, "exit_code=9")
	assert.Contains(t, got, "oom_killed=true")
	assert.Contains(t, got, "boom")
}

// TestBackendProvisionStore_Get covers the snapshot read path.
func TestBackendProvisionStore_Get(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady},
	})
	s := &backendProvisionStore{backend: b}

	t.Run("known lease returns snapshot + ok", func(t *testing.T) {
		state, ok := s.Get("lease-1")
		assert.True(t, ok)
		require.NotNil(t, state)
		assert.Equal(t, backend.ProvisionStatusReady, state.Status)
		assert.Equal(t, "lease-1", state.LeaseUUID)
	})

	t.Run("unknown lease returns nil + ok=false", func(t *testing.T) {
		state, ok := s.Get("nope")
		assert.False(t, ok)
		assert.Nil(t, state)
	})
}

// TestBackendProvisionStore_UpdateFn applies the closure under one
// critical section (verified by inspecting the mutated provision
// struct after the call).
func TestBackendProvisionStore_UpdateFn(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		"lease-1": {LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady, FailCount: 2},
	})
	s := &backendProvisionStore{backend: b}

	t.Run("compound update applies all writes atomically", func(t *testing.T) {
		applied := s.UpdateFn("lease-1", func(p *leasesm.ProvisionState) {
			p.Status = backend.ProvisionStatusFailed
			p.LastError = "container exited"
			p.FailCount++
		})
		assert.True(t, applied)

		b.provisionsMu.RLock()
		prov := b.provisions["lease-1"]
		b.provisionsMu.RUnlock()
		require.NotNil(t, prov)
		assert.Equal(t, backend.ProvisionStatusFailed, prov.Status)
		assert.Equal(t, "container exited", prov.LastError)
		assert.Equal(t, 3, prov.FailCount)
	})

	t.Run("unknown lease returns false; closure not invoked", func(t *testing.T) {
		called := false
		applied := s.UpdateFn("nope", func(p *leasesm.ProvisionState) {
			called = true
		})
		assert.False(t, applied)
		assert.False(t, called)
	})
}
