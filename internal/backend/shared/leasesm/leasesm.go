// Package leasesm owns the per-lease state machine and actor, plus the
// substrate-agnostic seams they consume. The state machine + actor
// implementations live in lease_sm.go and lease_actor.go in this
// package; substrate-specific concerns are injected via the narrow interfaces
// and construction-bound handlers declared here. The Docker backend implements
// those against its Docker client, durable settlement coordinators, and
// provision projection; future substrates provide their own implementations.
//
// This package exports no test scaffolding. Substrate code and tests obtain
// sealed actor messages only from validating constructors, route them through
// LeaseActor.TryEnqueue, and synchronize through receive-only reply handles or
// terminal observables. Helpers private to leasesm itself live in *_test.go.
package leasesm

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/manifest"
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
	ExitCode    *int // nil when not yet exited
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

// ProvisionState is the substrate-agnostic snapshot of a lease's
// provision record. The lease state machine and actor reason about
// these fields exclusively; substrate-specific state is kept
// substrate-side and never reaches this struct.
//
// Substrate implementations may embed ProvisionState in a private wrapper, but
// the LeaseProvisionStore exposes only ProvisionState. Substrate-private data
// is therefore not reachable by the state machine.
//
// Manifest and StackManifest are substrate-shared schema (lifted to
// internal/backend/shared/manifest in PR2) — they live here even
// though substrates translate them to substrate-specific shapes
// (Docker compose-spec, K8s pod spec) at provision time.
type ProvisionState struct {
	LeaseUUID            string
	Tenant               string
	ProviderUUID         string
	SKU                  string
	Status               backend.ProvisionStatus
	Quantity             int
	CreatedAt            time.Time
	FailCount            int
	LastError            string
	Reason               backend.Reason // curated failure-category code (ENG-508), authored at source
	Message              string         // curated human message (== on-chain CallbackErr)
	CallbackURL          string
	LifecycleCallbackURL string
	// ActiveReleaseVersion is the exact durable Release generation represented
	// by this projection. It is set only from a committed or recovered Release;
	// zero means no active runtime exists yet. Maintenance preserves the
	// originating OperationID, so observations must compare this version too.
	ActiveReleaseVersion int
	// ActiveOperationID is the typed generation which owns the current runtime.
	// Autonomous observations must return this exact identity to the callback
	// publisher; zero deliberately cannot authorize lifecycle publication.
	ActiveOperationID shared.OperationID
	Items             []backend.LeaseItem
	// ResourceProfiles is the immutable capacity authority paired with Items.
	// It belongs in the actor-owned projection so a recovered maintenance
	// target cannot publish new topology while retaining source-generation
	// resource accounting.
	ResourceProfiles []shared.SKUResourceSnapshot
	ContainerIDs     []string
	// Manifest field deleted in Task 15 — all leases are stack-shaped
	// post-migration; per-service refs go through StackManifest.Services.
	StackManifest     *manifest.StackManifest
	ServiceContainers map[string][]string
}

// LeaseProvisionStore is the substrate-agnostic seam for the
// provision-record reads and writes the SM and actor perform. The
// closure-style UpdateFn captures any compound multi-field update
// inside one mutex acquisition, so atomicity is preserved without
// the interface needing one method per transition.
//
// Implementations MUST guard accesses with the same mutex that any
// substrate-internal direct access uses, so cross-path atomicity is
// preserved.
//
// # UpdateFn closure contract
//
// UpdateFn runs the supplied closure under one mutex Lock acquisition
// on the implementation's internal mutex. The closure body sees a
// live *ProvisionState whose mutations persist on the underlying
// record. The closure MUST NOT:
//
//   - block on any external resource (network, disk I/O, channel
//     send/receive) — it runs under the mutex and will starve other
//     UpdateFn / LookupStatus / Exists callers
//   - call any other method on the same LeaseProvisionStore (deadlock
//     under typical mutex implementations)
//   - retain the *ProvisionState pointer beyond closure return — the
//     pointer is only valid for the duration of the call
//
// # Communicating outcomes from inside the closure
//
// The closure's signature is `func(*ProvisionState)` with no return.
// When callers need to communicate decision data (callback URL,
// diagnostic snapshots, "applied vs skipped" flags) from inside the
// critical section to post-Unlock code, capture outer-scope
// variables. The pattern:
//
//	var callbackURL string
//	cfg.ProvisionStore.UpdateFn(uuid, func(p *ProvisionState) {
//	    p.Status = backend.ProvisionStatusReady
//	    p.LastError = ""
//	    callbackURL = p.CallbackURL // capture for post-Unlock use
//	})
//	// post-Unlock work uses the captured callbackURL
//	cfg.SendOperationSuccessFn(uuid, callbackURL, committedRelease)
//
// Pick outer-capture for ALL UpdateFn call sites — mixing capture-style
// and a hypothetical "UpdateFn returns values" extension is a
// readability tax. The current API is locked at no-return.
//
// # Idempotence requirement (forward-looking, ENG-154)
//
// Closure bodies SHOULD be idempotent: running the closure multiple
// times against the same starting state should produce the same end
// state. The current mutex+map implementation never re-runs the
// closure, but a future implementation (ENG-154) may swap to
// atomic.Pointer-based copy-on-write with CAS retry — under contention,
// the closure can be re-invoked against a fresh snapshot. Closure
// authors MUST avoid:
//
//   - side effects that aren't idempotent (e.g., logging-with-counters,
//     metric increments — keep those OUTSIDE the closure, in
//     post-UpdateFn code using captured outer flags)
//   - reading mutable outer state inside the closure (the snapshot may
//     have changed between retries; the closure should derive everything
//     from the *ProvisionState parameter)
//
// The 9 SM compound sections shipped with PR5 are all read-then-write
// against the *ProvisionState alone, with side effects (metric Inc/Dec,
// callback dispatch, log lines) factored out into post-UpdateFn code
// behind captured flags. PR5 sets that pattern; future contributors
// must preserve it.
//
// # Actor projection-writer invariant (ENG-229)
//
// Live ProvisionState transitions occur on the lease actor goroutine via
// UpdateFn/Delete. A worker may publish the exact terminal projection before
// handing its sealed result back to the actor:
//
//  1. The success-path pre-publish of ContainerIDs/ServiceContainers in
//     spawnProvisionWorker/spawnReplaceWorker runs on the WORKER goroutine
//     before sendTerminal. It is required so a Deprovision that preempts an
//     in-flight worker observes the new container IDs under provisionsMu and
//     tears them down instead of orphaning them. Correctness rests on the
//     workers-barrier happens-before: the pre-publish UpdateFn completes before
//     workers.Done() (the outermost defer); onExitProvisioning calls
//     workCancel() then waitForWorkers() (blocked on workers.Zero()); only then
//     does handleDeprovision invoke doDeprovision, which reads ContainerIDs.
//     Routing this through an actor message is PROHIBITED: the actor is blocked
//     in waitForWorkers() and cannot dequeue the publish message the worker must
//     send to release the barrier (actor self-deadlock). Bounded escape: a worker
//     exceeding WorkerDrainTimeout (75s by default;
//     diagnosticsGatherTimeout 30s is the inner budget) refuses the state
//     transition and therefore cannot authorize conflicting teardown.
//
// Destructive close progress is not projected through this store. It belongs
// to the durable close journal and its typed execution-generation protocol, so
// a process restart cannot reset progress or turn a retry count into authority.
type LeaseProvisionStore interface {
	// LookupStatus returns only the scalar actor projection needed to seed and
	// guard the FSM. Reference-bearing provision fields never escape the store
	// lock through this seam.
	LookupStatus(leaseUUID string) (backend.ProvisionStatus, bool)

	// Exists performs the two deprovision existence checks without manufacturing
	// a broad mutable snapshot.
	Exists(leaseUUID string) bool

	// ClassifyReadyRuntime compares an exact durable runtime generation with the
	// current actor projection. It must return Current only when the projection
	// is Ready and carries the same positive release version plus either the
	// same typed operation ID or the explicit legacy class with a zero operation
	// ID.
	ClassifyReadyRuntime(shared.RuntimeGenerationProof) ObservationGenerationState

	// ClassifyReadyInstance additionally requires instanceID to belong to that
	// exact Ready projection.
	ClassifyReadyInstance(shared.RuntimeGenerationProof, string) ObservationGenerationState

	// UpdateFn applies fn to the provision record under one critical
	// section. Returns true if the lease existed and fn was applied,
	// false otherwise. Implementations MUST hold the same mutex that
	// guards direct accesses for the duration of fn — the closure
	// runs inside the lock. Substrate observability derived from a status
	// transition (for example, Ready population) MUST be committed before that
	// same critical section is released; callers do not emit a second delta.
	UpdateFn(leaseUUID string, fn func(*ProvisionState)) bool

	// Delete removes the lease's live record. Returns true if an entry was
	// present. Takes the same mutex as LookupStatus/Exists/UpdateFn, so a concurrent scalar read
	// observes the removal. Like UpdateFn, MUST NOT be called from inside an
	// UpdateFn closure (re-entrant lock → deadlock).
	Delete(leaseUUID string) bool
}

// SMMetrics is the observability seam for substrate-specific metric
// emission. Each substrate provides its own implementation, owning
// the prometheus registry, naming, and labels — the Docker backend
// keeps `fred_docker_backend_lease_*` names; K3s will choose its own
// substrate-flavored namespacing. The SM/actor code calls these
// methods at fixed sites; the substrate adapter shapes the
// counter/label semantics.
//
// WorkerPanic's workerType is one of "provision" / "replace" / "diag"
// (the three categories of worker goroutines the actor spawns).
// ActorPanic is the distinct site for panics in the actor's own
// message-handler goroutine (recovered by handle()'s defer); it has
// no label because the actor goroutine is a single category.
// TerminalEventDropped's event is a short tag identifying the event
// type that was dropped (e.g., "diag_gathered", "provision_completed").
type SMMetrics interface {
	SMTransition(source, dest, trigger string)
	ActorCreated()
	WorkerPanic(workerType string)
	ActorPanic()
	TerminalEventDropped(event string)
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
	LeaseUUID string
	Logger    *slog.Logger
	StopCtx   context.Context
	WG        *sync.WaitGroup
	// WorkerDrainTimeout bounds transitions that must cancel and join an
	// in-flight mutation worker before the destination state is safe to enter.
	// Zero selects the package default. Tests and future substrates may use a
	// shorter positive value; production callers should normally leave it zero.
	WorkerDrainTimeout time.Duration
	Inspector          InstanceInspector
	Diag               DiagnosticsGatherer
	ProvisionStore     LeaseProvisionStore
	OnTerminated       func(leaseUUID string, actor *LeaseActor)
	Metrics            SMMetrics

	// Mutation handlers are fixed once, at actor construction. Commands carry
	// only their exact journal-issued authority; they cannot splice authority
	// from one operation together with a caller-selected closure that mutates a
	// different substrate target before outcome validation. The maintenance
	// handler dispatches restart/update from target.Intent().Kind().
	ProvisionWorkFn   func(context.Context, shared.ProvisionResourceExecution) ProvisionWorkOutcome
	RestoreWorkFn     func(context.Context, shared.OperationIntentClaim) ReplaceWorkOutcome
	MaintenanceWorkFn func(
		context.Context,
		shared.MaintenanceReleaseClaim,
	) ReplaceWorkOutcome

	// PersistDiagnosticsFn writes a failure diagnostic to the
	// substrate's diagnostics store, including a fresh fetch of
	// container logs from the supplied containerIDs (substrate-side
	// concern). The optional keys map overrides default index-based
	// log keys (e.g., "web/0" for stack services). Best-effort: errors
	// log internally and are not propagated.
	PersistDiagnosticsFn func(entry shared.DiagnosticEntry, containerIDs []string, keys map[string]string)

	// SendOperationSuccessFn dispatches an exact Provision/Restore success only
	// with the opaque proof returned after its active Release committed.
	SendOperationSuccessFn func(
		committed shared.OperationReleaseCommitted,
	)
	// SendOperationFailureFn dispatches a definitive Provision/Restore failure.
	// Separate function types prevent a caller-selected status from bypassing
	// the committed-release proof required by success.
	SendOperationFailureFn func(shared.OperationReleaseUncommitted, string)

	// SendLifecycleFailureFn is the sole callback surface for autonomous
	// substrate observations. Success is not representable here; successful
	// maintenance and operation completions require their exact terminal proof.
	SendLifecycleFailureFn func(shared.RuntimeGenerationProof, string)

	// Maintenance completion is split by terminal proof type. The actor cannot
	// select a status independently of the exact ReleaseStore fact committed by
	// its worker, and close cannot coalesce either exact completion.
	SendMaintenanceSuccessFn func(
		active shared.MaintenanceReleaseActive,
	)
	SendMaintenanceFailureFn func(
		failed shared.MaintenanceReleaseFailure,
		errMsg string,
	)

	// RecoveryLineage binds this actor to its backend's exact recovery
	// coordinator. It cannot mint authority by itself; handleDeprovision combines
	// it with the exact actor identity after the transition drains workers.
	RecoveryLineage shared.RecoveryLineage

	// DoDeprovisionFn dispatches substrate-specific deprovision with the
	// callback-lifetime authority minted by the exact actor only after its
	// deprovision transition has drained mutation workers.
	//
	// The ctx threaded in MUST be the actor-owned ctx delivered with
	// the inbound deprovision command (which itself carries the caller's
	// ctx from Backend.Deprovision). Substrate implementers MUST NOT
	// substitute a caller-imported ctx or a fresh ctx here — the
	// actor's serial processing of inbound messages depends on each
	// handler honoring the inbound ctx for cancellation semantics.
	DoDeprovisionFn func(ctx context.Context, scope ActorCloseScope) error
}
