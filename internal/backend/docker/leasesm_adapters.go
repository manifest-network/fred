package docker

import (
	"context"
	"strings"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

// containerInfoToInstanceState converts the Docker-shaped ContainerInfo
// to the substrate-agnostic leasesm.InstanceState the SM consumes.
// Returns nil when info is nil.
//
// ExitCode is attached only when the container has actually terminated
// (Phase is Exited or Failed). Docker's ContainerJSON reports
// ExitCode=0 for still-running containers, which would otherwise be
// indistinguishable from a successful clean exit.
func containerInfoToInstanceState(info *ContainerInfo) *leasesm.InstanceState {
	if info == nil {
		return nil
	}
	state := &leasesm.InstanceState{
		Phase:       dockerStatusToPhase(info.Status),
		OOMKilled:   info.OOMKilled,
		ServiceName: info.ServiceName,
	}
	if state.Phase == leasesm.PhaseExited || state.Phase == leasesm.PhaseFailed {
		ec := info.ExitCode
		state.ExitCode = &ec
	}
	return state
}

// dockerStatusToPhase maps a Docker container status string to the
// substrate-agnostic leasesm.Phase. Hand-tabulated to mirror the
// existing containerStatusToProvisionStatus mapping for the SM's
// "is this container terminally gone?" decision: the Docker statuses
// {"removing", "exited", "dead"} that previously mapped to
// ProvisionStatusFailed now split into PhaseExited (clean exit) and
// PhaseFailed (forced/abnormal termination), both treated as terminal
// by the SM's `Phase != PhaseRunning && Phase != PhaseUnknown` check.
func dockerStatusToPhase(status string) leasesm.Phase {
	switch strings.ToLower(status) {
	case "running", "paused":
		return leasesm.PhaseRunning
	case "exited":
		return leasesm.PhaseExited
	case "removing", "dead":
		return leasesm.PhaseFailed
	default:
		// "created", "restarting", "" and any unknown status:
		// containerStatusToProvisionStatus mapped these to Provisioning
		// or Unknown — neither "Failed" — so the SM did NOT treat them
		// as terminal. PhaseUnknown preserves that.
		return leasesm.PhaseUnknown
	}
}

// dockerInstanceInspector adapts DockerClient.InspectContainer to the
// leasesm.InstanceInspector interface. The Docker-shaped ContainerInfo
// is translated to InstanceState at the seam.
type dockerInstanceInspector struct {
	docker dockerClient
}

// InspectInstance implements leasesm.InstanceInspector.
func (a *dockerInstanceInspector) InspectInstance(ctx context.Context, instanceID string) (*leasesm.InstanceState, error) {
	info, err := a.docker.InspectContainer(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	return containerInfoToInstanceState(info), nil
}

// dockerDiagnosticsGatherer adapts Backend.containerFailureDiagnostics
// to the leasesm.DiagnosticsGatherer interface. The body of
// containerFailureDiagnostics already accepts InstanceState directly
// (signature changed in this PR); the adapter is a thin pass-through
// that lets the SM inject diagnostic gathering without referencing
// *Backend at the SM seam.
type dockerDiagnosticsGatherer struct {
	backend *Backend
}

// GatherDiagnostics implements leasesm.DiagnosticsGatherer.
func (g *dockerDiagnosticsGatherer) GatherDiagnostics(ctx context.Context, instanceID string, state *leasesm.InstanceState) string {
	return g.backend.containerFailureDiagnostics(ctx, instanceID, state)
}

// backendProvisionStore adapts Backend.provisions + Backend.provisionsMu
// to the leasesm.LeaseProvisionStore interface. The adapter takes the
// SAME mutex as direct accessors so cross-path atomicity is preserved.
//
// The 3-method interface intentionally covers only simple status reads
// and single-field writes; compound critical sections (multi-field
// writes that must atomically observe-and-update) keep direct access
// to b.provisionsMu in the SM/actor/recover paths.
type backendProvisionStore struct {
	backend *Backend
}

// UpdateStatus implements leasesm.LeaseProvisionStore.
func (s *backendProvisionStore) UpdateStatus(leaseUUID string, status backend.ProvisionStatus, lastErr string) {
	s.backend.provisionsMu.Lock()
	defer s.backend.provisionsMu.Unlock()
	if p, ok := s.backend.provisions[leaseUUID]; ok {
		p.Status = status
		p.LastError = lastErr
	}
}

// IncFailCount implements leasesm.LeaseProvisionStore.
func (s *backendProvisionStore) IncFailCount(leaseUUID string) {
	s.backend.provisionsMu.Lock()
	defer s.backend.provisionsMu.Unlock()
	if p, ok := s.backend.provisions[leaseUUID]; ok {
		p.FailCount++
	}
}

// Get implements leasesm.LeaseProvisionStore.
func (s *backendProvisionStore) Get(leaseUUID string) (backend.ProvisionStatus, bool) {
	s.backend.provisionsMu.RLock()
	defer s.backend.provisionsMu.RUnlock()
	if p, ok := s.backend.provisions[leaseUUID]; ok {
		return p.Status, true
	}
	return "", false
}
