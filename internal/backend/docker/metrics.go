package docker

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/manifest-network/fred/internal/backend/shared"
)

const (
	metricsNamespace = "fred"
	metricsSubsystem = "docker_backend"
)

var (
	// provisionsTotal tracks the total number of provision attempts by outcome.
	provisionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "provisions_total",
		Help:      "Total number of provision attempts",
	}, []string{"outcome"})

	// deprovisionsTotal tracks the total number of deprovision operations.
	deprovisionsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "deprovisions_total",
		Help:      "Total number of deprovision operations",
	})

	// activeProvisions tracks the current number of active provisions.
	activeProvisions = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "active_provisions",
		Help:      "Current number of active provisions",
	})

	// resourceCPUAllocatedRatio tracks the ratio of allocated to total CPU.
	resourceCPUAllocatedRatio = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "resource_cpu_allocated_ratio",
		Help:      "Ratio of allocated CPU to total CPU",
	})

	// resourceMemoryAllocatedRatio tracks the ratio of allocated to total memory.
	resourceMemoryAllocatedRatio = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "resource_memory_allocated_ratio",
		Help:      "Ratio of allocated memory to total memory",
	})

	// resourceDiskAllocatedRatio tracks the ratio of allocated to total disk.
	resourceDiskAllocatedRatio = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "resource_disk_allocated_ratio",
		Help:      "Ratio of allocated disk to total disk",
	})

	// provisionDurationSeconds tracks the end-to-end provision time.
	provisionDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "provision_duration_seconds",
		Help:      "End-to-end provision duration in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.5, 2, 12), // 0.5s to ~17min
	})

	// callbackDeliveryTotal tracks callback delivery outcomes.
	callbackDeliveryTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "callback_delivery_total",
		Help:      "Total number of callback delivery attempts by outcome",
	}, []string{"outcome"})

	// callbackStoreErrorsTotal tracks bbolt persistence failures for callbacks.
	callbackStoreErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "callback_store_errors_total",
		Help:      "Total number of callback persistence failures (bbolt store errors)",
	})

	// imagePullDurationSeconds tracks image pull duration.
	imagePullDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "image_pull_duration_seconds",
		Help:      "Duration of image pull operations in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.5, 2, 12), // 0.5s to ~17min
	})

	// containerCreateDurationSeconds tracks container creation duration.
	containerCreateDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "container_create_duration_seconds",
		Help:      "Duration of container creation operations in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.1, 2, 10), // 0.1s to ~51s
	})

	// reconciliationTotal tracks reconciliation runs by outcome.
	reconciliationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "reconciliation_total",
		Help:      "Total number of reconciliation runs by outcome",
	}, []string{"outcome"})

	// reconcilerLastSuccessTimestamp records the unix timestamp of the last
	// successful reconciliation run for this docker backend.
	reconcilerLastSuccessTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "reconciliation_last_success_timestamp_seconds",
		Help:      "Unix timestamp of the last successful reconciliation run",
	})

	// idempotentOpsTotal tracks Docker operations skipped because the daemon
	// reported the work was already done. We increment this when Docker tells
	// us the operation was already complete; the caller sees success. Spikes
	// on remove/in_progress suggest reconciler/event-handler races; spikes on
	// create/already_exists suggest repeated crash-replay.
	idempotentOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "idempotent_ops_total",
		Help:      "Docker operations skipped because the daemon reported the work was already done",
	}, []string{"op", "reason"})

	// containerRemovalWaitFailuresTotal counts RemoveContainer calls where
	// the "removal in progress" wait did not confirm NotFound before the
	// bound elapsed (timeout, context cancellation, or persistent inspect
	// errors). These surface as errors to the caller; the counter gives
	// operators a direct signal when the race becomes a sustained problem.
	containerRemovalWaitFailuresTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "container_removal_wait_failures_total",
		Help:      "Count of RemoveContainer calls where the 'in progress' wait failed before confirming removal",
	})

	// leaseSMTransitionsTotal counts SM transitions by (from, to, event).
	// Spikes on specific paths surface unexpected flows — e.g., a rise in
	// (Failing, Deprovisioning, DeprovisionRequested) indicates frequent
	// Deprovision-preempts-Failing preemption, which is interesting for
	// load analysis.
	leaseSMTransitionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "lease_sm_transitions_total",
		Help:      "Count of lease state-machine transitions by (from_state, to_state, event)",
	}, []string{"from", "to", "event"})

	// leaseActorsCreatedTotal counts how many per-lease actors have been
	// created for the lifetime of this backend. Under normal operation,
	// this tracks distinct leases processed. A runaway counter (with
	// static lease count) would signal actor-leak or churn.
	leaseActorsCreatedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "lease_actors_created_total",
		Help:      "Cumulative number of per-lease actors created since backend start",
	})

	// leaseActorStuckSeconds is the age, in seconds, of the oldest
	// in-flight actor handle() call across all leases. 0 when every
	// actor is idle (waiting on inbox).
	//
	// Legitimate long-running operations like Deprovision run synchronously
	// inside the actor's handle() and can hold it for minutes (container
	// removal, volume cleanup). Set the alerting threshold above the
	// longest expected legitimate duration — e.g., 15 minutes — to catch
	// real stuck actors without false positives during normal slow work.
	leaseActorStuckSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "lease_actor_stuck_seconds",
		Help:      "Age of the oldest in-flight actor message across all leases; 0 if all actors are idle",
	})

	// leaseActorInboxDepth is a histogram of actor inbox depths sampled
	// periodically across all live actors. A healthy system has p99 near 0
	// (actors keep up with their inbox). A rising p99 signals event
	// arrival faster than processing — either workload shift or an
	// upstream I/O slowdown pinning an actor.
	leaseActorInboxDepth = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "lease_actor_inbox_depth",
		Help:      "Distribution of per-actor inbox depths sampled across all live actors",
		Buckets:   []float64{0, 1, 2, 4, 8, 12, 16}, // inbox cap is leaseActorInboxSize (16)
	})

	// leaseActorPanicsTotal counts panics recovered in lease actor
	// handlers. Any non-zero value is a bug — the actor survives the
	// panic (blast radius contained to a single message), but the
	// message that triggered it did not complete its transition.
	leaseActorPanicsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "lease_actor_panics_total",
		Help:      "Count of panics recovered in lease actor handlers (any non-zero is a bug)",
	})

	// leaseTerminalEventDroppedTotal counts terminal SM events whose
	// actor.send() was refused because the backend was shutting down.
	// The work has already happened on the host (container swap complete,
	// provision succeeded, etc.) but the SM never recorded it, so the
	// release store / provision struct may be out of sync with Docker.
	// Recovery on next startup should re-reconcile; sustained non-zero
	// values under clean shutdown indicate a real data-loss pattern.
	leaseTerminalEventDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "lease_terminal_event_dropped_total",
		Help:      "Terminal SM events dropped because the actor inbox refused delivery during shutdown",
	}, []string{"event"})

	// dieEventDroppedTotal counts container-death signals whose actor.send
	// was refused — either because the backend is shutting down, the actor
	// has already exited (Delete race), or the inbox was wedged. The
	// reconciler re-detects missed deaths within its cycle (default 5m), so
	// these drops are not data loss; the counter gives operators a signal
	// when the realtime event path is degraded.
	dieEventDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "die_event_dropped_total",
		Help:      "Container-death signals dropped at the actor inbox; reconciler re-detects on next cycle",
	}, []string{"source"})

	// leaseFailingRaceSkippedTotal counts onEnterFailing invocations that
	// bailed because another caller (Restart/Update) flipped prov.Status
	// off Ready between the SM guard and this entry action. Non-zero
	// values indicate the Ready-vs-Restart race is being hit in practice;
	// a sustained rate suggests the synchronous Status flip in
	// Backend.Restart/Update should be moved into the actor.
	leaseFailingRaceSkippedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "lease_failing_race_skipped_total",
		Help:      "onEnterFailing bails due to concurrent Restart/Update flipping prov.Status off Ready",
	})

	// leaseWorkerPanicsTotal counts panics recovered in lease worker
	// goroutines (provision, replace, diag), labeled by worker type.
	// Workers are Docker-interaction code that is NOT expected to panic;
	// any non-zero value indicates a latent bug. The recover keeps the
	// process alive and drives the SM to a terminal Failed state so the
	// lease doesn't wedge — but the real fix is always to eliminate the
	// panic at its source.
	leaseWorkerPanicsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "lease_worker_panics_total",
		Help:      "Panics recovered in lease worker goroutines, by worker type",
	}, []string{"worker_type"})
)

// updateResourceMetrics updates the resource allocation ratio gauges.
func updateResourceMetrics(stats shared.ResourceStats) {
	if stats.TotalCPU > 0 {
		resourceCPUAllocatedRatio.Set(stats.AllocatedCPU / stats.TotalCPU)
	}
	if stats.TotalMemoryMB > 0 {
		resourceMemoryAllocatedRatio.Set(float64(stats.AllocatedMemoryMB) / float64(stats.TotalMemoryMB))
	}
	if stats.TotalDiskMB > 0 {
		resourceDiskAllocatedRatio.Set(float64(stats.AllocatedDiskMB) / float64(stats.TotalDiskMB))
	}
}
