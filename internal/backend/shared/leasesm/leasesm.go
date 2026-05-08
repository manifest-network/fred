// Package leasesm declares the substrate-agnostic seams the per-lease
// state machine and actor consume. The Docker backend implements the
// interfaces against its DockerClient + provision-record map; future
// substrates (K3s, etc.) implement them against their own primitives.
//
// The package contains scaffolding only — no behavior. The actual SM
// and actor still live in internal/backend/docker/ as of ENG-148 PR4;
// PR5 will move them here without further interface changes.
package leasesm

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
)

// Phase is a substrate-agnostic container/instance lifecycle phase.
//
// PhaseUnknown is the iota zero value (deliberately first) because
// Kubernetes pod status legitimately enters Unknown — defaulting to
// any other phase would silently misclassify those cases as Running,
// Exited, or Failed.
type Phase int

const (
	// PhaseUnknown is the zero value. Substrates that cannot determine
	// the instance's phase MUST report this rather than guessing.
	PhaseUnknown Phase = iota

	// PhaseRunning means the instance is alive and operating. For
	// Docker this maps to "running" or "paused" status; for K8s pods
	// this maps to PodRunning with the relevant container in
	// containerStatuses.state.running.
	PhaseRunning

	// PhaseExited means the instance has terminated. ExitCode should
	// be set when this phase is reported. Used for Docker "exited"
	// status; K8s containerStatuses.state.terminated with reason
	// "Completed" or similar non-failure terminations.
	PhaseExited

	// PhaseFailed means the instance was forcibly killed or otherwise
	// terminated abnormally. Used for Docker "removing"/"dead";
	// K8s containerStatuses.state.terminated with non-zero exit code
	// or reasons like "OOMKilled", "Error", "ContainerCannotRun".
	PhaseFailed
)

// InstanceState is a substrate-agnostic snapshot of a workload
// instance. The Docker backend translates ContainerInfo into this
// shape; the K3s backend will translate a Pod's containerStatuses
// entry.
//
// OOMKilled is exposed as a separate field rather than encoded inside
// Phase so callers can distinguish "exited cleanly" from "killed by
// the OOM killer" without losing the orthogonal Phase signal.
//
// ServiceName is the per-instance service name within a multi-service
// (stack) deployment. It is a Fred-shared concept rather than a strict
// substrate-level property — substrate adapters populate it from their
// own conventions (Docker labels, K8s pod annotations, etc.) and leave
// it empty for single-service leases where there is no meaningful
// distinction. The SM uses it for log continuity at death events; no
// behavior decisions branch on it.
type InstanceState struct {
	Phase       Phase
	ExitCode    *int      // nil when not yet exited
	OOMKilled   bool
	FinishedAt  time.Time // zero value when still running
	Reason      string    // substrate-specific termination reason ("OOMKilled", "Error", etc.)
	ServiceName string    // per-instance service name in a multi-service deployment; "" when not applicable
}

// InstanceInspector wraps the substrate-specific "inspect this
// instance" operation. The Docker backend implements this around
// DockerClient.InspectContainer; K3s will implement it around
// pod-status inspection.
type InstanceInspector interface {
	InspectInstance(ctx context.Context, instanceID string) (*InstanceState, error)
}

// DiagnosticsGatherer wraps the substrate-specific failure diagnostic
// gathering. Output flows into prov.LastError ONLY — never into
// callbacks. The callback-error-sanitization invariant requires that
// hardcoded callback strings stay hardcoded; diagnostic blobs carry
// substrate-specific data that must not leak on-chain.
type DiagnosticsGatherer interface {
	GatherDiagnostics(ctx context.Context, instanceID string, state *InstanceState) string
}

// LeaseProvisionStore is the substrate-agnostic seam for the simple
// provision-record reads/writes the SM and actor perform.
//
// Compound multi-field updates (Status + ContainerIDs + Manifest in a
// single critical section) are not expressible through this interface
// without losing atomicity; substrate implementations and consumers
// keep direct access for those sites. Implementations MUST guard
// accesses with the same mutex that direct callers use, so atomicity
// is preserved across the two access paths.
type LeaseProvisionStore interface {
	// UpdateStatus sets Status and LastError under one critical section.
	UpdateStatus(leaseUUID string, status backend.ProvisionStatus, lastErr string)

	// IncFailCount atomically increments the FailCount.
	IncFailCount(leaseUUID string)

	// Get returns the current Status and ok=true when the lease exists.
	Get(leaseUUID string) (status backend.ProvisionStatus, ok bool)
}

// LeaseActorConfig groups the dependencies a lease actor receives at
// construction. Substrate-private concerns (compose project access,
// container lifecycle calls, etc.) stay on the actor's substrate
// pointer; only the listed dependencies move here.
//
// OnTerminated is invoked by the actor's exit path so substrate code
// can clean up the actor registry without the actor reaching back
// through a substrate pointer for that purpose. Implementations should
// preserve their existing "only delete if I'm still the registered
// actor" semantics by closing over the actor pointer.
type LeaseActorConfig struct {
	LeaseUUID      string
	Logger         *slog.Logger
	StopCtx        context.Context
	WG             *sync.WaitGroup
	Inspector      InstanceInspector
	Diag           DiagnosticsGatherer
	CallbackSender *shared.CallbackSender
	ProvisionStore LeaseProvisionStore
	OnTerminated   func(leaseUUID string)
}
