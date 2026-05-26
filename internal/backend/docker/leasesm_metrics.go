package docker

// dockerSMMetrics implements leasesm.SMMetrics against the package-global
// prometheus counters declared in metrics.go. The counters themselves stay
// in this package — the adapter is a thin pass-through so the lifted
// SM/actor code (in shared/leasesm/) doesn't have to import docker.
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

// SMTransition records a single SM state transition with source / dest /
// trigger labels (matching the existing leaseSMTransitionsTotal vec).
func (dockerSMMetrics) SMTransition(source, dest, trigger string) {
	leaseSMTransitionsTotal.WithLabelValues(source, dest, trigger).Inc()
}

// ActorCreated records one lease-actor construction.
func (dockerSMMetrics) ActorCreated() {
	leaseActorsCreatedTotal.Inc()
}

// WorkerPanic records a worker-goroutine panic recovered by the actor's
// defer. workerType identifies the worker category
// ("provision" / "replace" / "diag").
func (dockerSMMetrics) WorkerPanic(workerType string) {
	leaseWorkerPanicsTotal.WithLabelValues(workerType).Inc()
}

// ActorPanic records a panic recovered by the actor's handle() defer
// — a panic in the message-handler goroutine itself, distinct from
// the worker categories WorkerPanic covers.
func (dockerSMMetrics) ActorPanic() {
	leaseActorPanicsTotal.Inc()
}

// TerminalEventDropped records a terminal SM event that could not be
// delivered (actor exited or inbox wedged). event is a short tag
// identifying the dropped event type.
func (dockerSMMetrics) TerminalEventDropped(event string) {
	leaseTerminalEventDroppedTotal.WithLabelValues(event).Inc()
}

// ActiveProvisionsInc increments the active-provisions gauge.
func (dockerSMMetrics) ActiveProvisionsInc() {
	activeProvisions.Inc()
}

// ActiveProvisionsDec decrements the active-provisions gauge.
func (dockerSMMetrics) ActiveProvisionsDec() {
	activeProvisions.Dec()
}
