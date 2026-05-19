// Package leasesm owns the per-lease state machine and actor, plus the
// substrate-agnostic seams they consume. The state machine + actor
// implementations live in lease_sm.go and lease_actor.go in this
// package; substrate-specific concerns are injected via the interfaces
// declared here (InstanceInspector, DiagnosticsGatherer,
// LeaseProvisionStore, SMMetrics) and the closure-bridge fields on
// LeaseActorConfig (PersistDiagnosticsFn, SendCallbackFn,
// DoDeprovisionFn, OnTerminated). The Docker backend implements those
// against its DockerClient + provision-record map; future substrates
// (K3s, etc.) implement them against their own primitives.
//
// Cross-package test access goes through the bounded set of ForTest
// helpers in testhelpers.go (the ONLY exported scaffolding for tests);
// substrate-side tests that need to drive the SM synchronously compose
// these helpers around their own substrate calls.
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
// Substrate implementations typically embed ProvisionState in a
// wrapper struct alongside any substrate-private fields they need to
// keep per-lease. The Docker backend uses
//
//	type provision struct {
//	    leasesm.ProvisionState
//	    VolumeCleanupAttempts int
//	}
//
// so the per-lease counter rides on the same allocation as the
// ProvisionState and gets cleared structurally on each new provision.
// The LeaseProvisionStore adapter passes &p.ProvisionState through the
// Get / UpdateFn seams; substrate-private wrapper fields are NOT
// reachable through the interface — the SM has no business reading
// them.
//
// Manifest and StackManifest are substrate-shared schema (lifted to
// internal/backend/shared/manifest in PR2) — they live here even
// though substrates translate them to substrate-specific shapes
// (Docker compose-spec, K8s pod spec) at provision time.
type ProvisionState struct {
	LeaseUUID    string
	Tenant       string
	ProviderUUID string
	SKU          string
	Status       backend.ProvisionStatus
	Quantity     int
	CreatedAt    time.Time
	FailCount    int
	LastError    string
	CallbackURL  string
	Items        []backend.LeaseItem
	ContainerIDs []string
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
//     UpdateFn / Get callers
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
//	var raceSkipped bool
//	cfg.ProvisionStore.UpdateFn(uuid, func(p *ProvisionState) {
//	    if p.Status != backend.ProvisionStatusReady {
//	        raceSkipped = true
//	        return
//	    }
//	    p.Status = backend.ProvisionStatusFailing
//	    p.FailCount++
//	    p.LastError = errMsg
//	    callbackURL = p.CallbackURL
//	})
//	if raceSkipped {
//	    metrics.RaceSkipped.Inc()
//	    return nil
//	}
//	// post-Unlock work uses the captured callbackURL
//	cfg.SendCallbackFn(uuid, callbackURL, ...)
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
type LeaseProvisionStore interface {
	// Get returns a SHALLOW value-copy snapshot of the provision state
	// and ok=true when the lease exists. Callers receive the copy by
	// value — they do NOT hold any lock after Get returns. Slices
	// and maps inside ProvisionState are shared with the underlying
	// record (no deep copy); callers must NOT mutate them. Use
	// UpdateFn for any mutation.
	Get(leaseUUID string) (state *ProvisionState, ok bool)

	// UpdateFn applies fn to the provision record under one critical
	// section. Returns true if the lease existed and fn was applied,
	// false otherwise. Implementations MUST hold the same mutex that
	// guards direct accesses for the duration of fn — the closure
	// runs inside the lock.
	UpdateFn(leaseUUID string, fn func(*ProvisionState)) bool
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
	FailingRaceSkipped()
	WorkerPanic(workerType string)
	ActorPanic()
	TerminalEventDropped(event string)
	ActiveProvisionsInc()
	ActiveProvisionsDec()
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
	Metrics        SMMetrics

	// PersistDiagnosticsFn writes a failure diagnostic to the
	// substrate's diagnostics store, including a fresh fetch of
	// container logs from the supplied containerIDs (substrate-side
	// concern). The optional keys map overrides default index-based
	// log keys (e.g., "web/0" for stack services). Best-effort: errors
	// log internally and are not propagated.
	PersistDiagnosticsFn func(entry shared.DiagnosticEntry, containerIDs []string, keys map[string]string)

	// PersistDiagnosticsWithLogsFn writes a failure diagnostic to the
	// substrate's diagnostics store using PRE-CAPTURED logs. Used by
	// failure-path workers that captured logs BEFORE cleanup tore the
	// containers down (re-fetching after cleanup would hit deleted
	// containers).
	PersistDiagnosticsWithLogsFn func(entry shared.DiagnosticEntry, logs map[string]string)

	// SendCallbackFn dispatches a callback (success or failure) for
	// the given lease/URL pair. The substrate adapter handles error
	// truncation, HMAC signing, retry, and store persistence; the SM
	// only supplies the inputs.
	SendCallbackFn func(leaseUUID, callbackURL string, status backend.CallbackStatus, errMsg string)

	// DoDeprovisionFn dispatches the substrate-specific deprovision
	// flow for the given lease. Called from handleDeprovision after
	// the SM's evDeprovisionRequested fires.
	//
	// The ctx threaded in MUST be the actor-owned ctx delivered with
	// the inbound DeprovisionMsg (which itself carries the caller's
	// ctx from Backend.Deprovision). Substrate implementers MUST NOT
	// substitute a caller-imported ctx or a fresh ctx here — the
	// actor's serial processing of inbound messages depends on each
	// handler honoring the inbound ctx for cancellation semantics.
	DoDeprovisionFn func(ctx context.Context, leaseUUID string) error
}
