package docker

import (
	"context"
	"strings"

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
// SAME mutex as direct accessors (recover.go, deprovision.go,
// startup-time mutators) so cross-path atomicity is preserved.
//
// The closure-style UpdateFn captures any compound multi-field update
// inside one Lock acquisition — atomicity is preserved without the
// interface needing one method per transition.
type backendProvisionStore struct {
	backend *Backend
}

// Get implements leasesm.LeaseProvisionStore. Returns a SHALLOW
// value-copy snapshot of the embedded ProvisionState — slices/maps
// alias the underlying record (no deep copy). Caller does NOT hold
// any lock after Get returns. Docker-private wrapper fields
// (VolumeCleanupAttempts, RestoringFrom) are NOT exposed through this seam — the SM
// has no business reading them.
func (s *backendProvisionStore) Get(leaseUUID string) (*leasesm.ProvisionState, bool) {
	s.backend.provisionsMu.RLock()
	defer s.backend.provisionsMu.RUnlock()
	p, ok := s.backend.provisions[leaseUUID]
	if !ok {
		return nil, false
	}
	snap := p.ProvisionState
	return &snap, true
}

// UpdateFn implements leasesm.LeaseProvisionStore. Runs fn under one
// Lock acquisition on b.provisionsMu, passing a pointer to the embedded
// ProvisionState. Mutations made by fn persist on the underlying
// record. Returns true when the lease existed and fn was applied.
// Docker-private wrapper fields (VolumeCleanupAttempts, RestoringFrom) are NOT
// reachable through fn — only the substrate-agnostic ProvisionState
// fields the SM consumes.
func (s *backendProvisionStore) UpdateFn(leaseUUID string, fn func(*leasesm.ProvisionState)) bool {
	s.backend.provisionsMu.Lock()
	defer s.backend.provisionsMu.Unlock()
	p, ok := s.backend.provisions[leaseUUID]
	if !ok {
		return false
	}
	fn(&p.ProvisionState)
	return true
}

// Delete implements leasesm.LeaseProvisionStore. Removes the lease's live
// record under the same provisionsMu as Get/UpdateFn, so a concurrent Get
// probe (e.g. handleDeprovision's terminated check) observes the removal.
func (s *backendProvisionStore) Delete(leaseUUID string) bool {
	s.backend.provisionsMu.Lock()
	defer s.backend.provisionsMu.Unlock()
	_, ok := s.backend.provisions[leaseUUID]
	delete(s.backend.provisions, leaseUUID)
	return ok
}
