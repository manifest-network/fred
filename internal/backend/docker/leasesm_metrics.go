package docker

import (
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
)

// dockerSMMetrics implements leasesm.SMMetrics against the series bound in
// series.go. The counters themselves stay in this package — the adapter is a
// thin pass-through so the lifted SM/actor code (in shared/leasesm/) doesn't
// have to import docker. Every value the actor reports is one of leasesm's
// closed lists, so each lands on a series that exists from startup.
//
// K3s (ENG-133) will provide its own SMMetrics implementation against its
// own prometheus registry with K3s-flavored metric names; Docker keeps
// `fred_docker_backend_lease_*` for backward compatibility with existing
// operator dashboards.
//
// Zero-value ready (no state); the type is a struct rather than a
// function-set so the implementation is named at each method site for
// stack-trace clarity.
type dockerSMMetrics struct{}

// SMTransition records a single SM state transition.
func (dockerSMMetrics) SMTransition(transition leasesm.Transition) {
	leaseTransitions.inc(transition)
}

// ActorCreated records one lease-actor construction.
func (dockerSMMetrics) ActorCreated() {
	leaseActorsCreatedTotal.Inc()
}

// WorkerPanic records a worker-goroutine panic recovered by the actor's
// defer.
func (dockerSMMetrics) WorkerPanic(worker leasesm.Worker) {
	leaseWorkerPanics.inc(worker)
}

// ActorPanic records a panic recovered by the actor's handle() defer
// — a panic in the message-handler goroutine itself, distinct from
// the worker categories WorkerPanic covers.
func (dockerSMMetrics) ActorPanic() {
	leaseActorPanicsTotal.Inc()
}

// TerminalEventDropped records a terminal SM event that could not be
// delivered (actor exited or inbox wedged).
func (dockerSMMetrics) TerminalEventDropped(event leasesm.TerminalEvent) {
	leaseTerminalDrops.inc(event)
}

// LeaseFailureRecorded records one failure the terminal budget recorded,
// labeled by its sealed attribution (a closed set; ENG-799).
func (dockerSMMetrics) LeaseFailureRecorded(cause failurecause.Cause) {
	leaseFailures.inc(cause.Label())
}

// PendingOperationUnstamped records a provision or restore whose projection
// could not await its operation (ENG-1125).
func (dockerSMMetrics) PendingOperationUnstamped() {
	leasePendingOperationUnstampedTotal.Inc()
}
