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

// Phase labels for replacePhaseDurationSeconds. They name the timed sub-steps
// of the shared replace machinery (doReplaceContainers) plus the restore-only
// volume adoption that runs in Restore's synchronous prelude. Kept as
// constants so the production instrumentation and the dashboard/tests cannot
// drift on a string typo.
const (
	phaseAdopt         = "adopt"          // restore-only: rename retained → canonical volumes
	phaseImageSetup    = "image_setup"    // inspect image VOLUMEs / user / writable paths
	phaseVolumeSetup   = "volume_setup"   // create volume binds + VOLUME-subdir chown
	phaseComposeUp     = "compose_up"     // compose Up (stop old + create/start new)
	phaseVerifyStartup = "verify_startup" // health-wait / fixed startup verification
)

// Skip reasons for retentionOrphanSkipsTotal (ENG-370). Kept as
// constants so the reconcile, the pre-init, and the tests cannot drift on a typo.
const (
	orphanSkipListError        = "list_error"        // volumes.List() failed (uncertain → fail-safe skip)
	orphanSkipRootUnverifiable = "root_unverifiable" // volume data root absent OR unreadable (fail-safe skip; the specific cause is in the warn log's error field)
	orphanSkipRaced            = "raced"             // record no longer ACTIVE-and-present at delete time: concurrently restore-claimed (active→restoring) OR already removed (e.g. cap-eviction) — benign, another path owns it
	orphanSkipDisabled         = "disabled"          // retention_orphan_confirmations == 0 (kill-switch)
	orphanSkipStoreError       = "store_error"       // retentionStore.List() failed (fail-safe skip)
)

// orphanSkipReasons is the closed reason set, used to pre-initialize the
// CounterVec series to 0 so absence/ratio alert queries return 0, not no-data.
var orphanSkipReasons = []string{orphanSkipListError, orphanSkipRootUnverifiable, orphanSkipRaced, orphanSkipDisabled, orphanSkipStoreError}

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

	// restoreDurationSeconds tracks the restore re-deploy worker span — the async
	// re-deploy of a retained lease via the replace machinery, recorded only on
	// success. It measures doRestore (compose up + verify startup) and therefore
	// EXCLUDES the synchronous adopt prelude (the volume rename, tracked separately
	// as replace_phase_duration_seconds{phase=adopt}) and stops when the worker
	// returns, before the actor flips the lease to ACTIVE. Buckets mirror
	// provisionDurationSeconds (itself the doProvision worker span) so the two can
	// be overlaid on the dashboard for the restore-vs-fresh-provision question
	// ENG-357 targets. Caveat: the overlay is approximate, not like-for-like —
	// provisionDurationSeconds is observed on BOTH success and failure (provision.go,
	// before its error branch) and carries no outcome label, whereas this histogram
	// is success-only. The comparison is robust at the median but biased at the tail
	// by failed-provision latencies (image-pull timeouts, failure-path cleanup).
	// Read it as indicative.
	restoreDurationSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "restore_duration_seconds",
		Help:      "Restore re-deploy worker duration in seconds (success only; excludes the synchronous adopt prelude)",
		Buckets:   prometheus.ExponentialBuckets(0.5, 2, 12), // 0.5s to ~17min
	})

	// replacePhaseDurationSeconds breaks the shared replace machinery
	// (restart/update/restore) into per-phase timings so the dominant cost of a
	// restore re-deploy is visible. Labeled by operation (restart|update|restore)
	// and phase (adopt|image_setup|volume_setup|compose_up|verify_startup). Finer,
	// lower-floor buckets than the end-to-end histograms because individual phases
	// (adopt, image_setup) are routinely sub-second while others (compose_up,
	// verify_startup) run for seconds.
	replacePhaseDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "replace_phase_duration_seconds",
		Help:      "Per-phase duration of the shared replace machinery (restart/update/restore) in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.05, 2, 16), // 50ms to ~27min
	}, []string{"operation", "phase"})

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

	// retentionOrphansPrunedTotal counts retention records pruned because all
	// their retained volumes were confirmed absent for >= N consecutive sweeps
	// (ENG-370).
	retentionOrphansPrunedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_orphans_pruned_total",
		Help:      "Total retention records pruned due to confirmed-absent backing volumes",
	})

	// retentionOrphanSkipsTotal counts orphan-reconcile skips by reason, at TWO
	// granularities: whole-sweep bailouts (list_error/root_unverifiable/store_error/
	// disabled — one per skipped sweep) AND per-record prune attempts skipped (raced
	// — one per record). Filter by reason rather than summing across (the units
	// differ); the name says "skips", not "sweeps", deliberately. Without it, a sweep
	// that skips forever (e.g. a mis-mounted volume root) is indistinguishable from a
	// healthy "0 pruned" on the success counter alone. reason ∈ orphanSkipReasons.
	retentionOrphanSkipsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_orphan_skips_total",
		Help:      "Orphan-reconcile skips by reason (sweep-level bailouts + per-record raced prune attempts)",
	}, []string{"reason"})

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

	// leaseTerminalEventDroppedTotal counts terminal SM events that sendTerminal
	// refused to deliver. Delivery is refused on three distinct conditions:
	// the actor has exited (hasExited), the actor is mid-exit past the
	// drainInbox point (isExiting), or the inbox is wedged (the buffered send
	// times out after terminalSendTimeout). The work has already happened on
	// the host (container swap complete, provision succeeded, etc.) but the SM
	// never recorded it, so the release store / provision struct may be out of
	// sync with Docker. Recovery on next startup should re-reconcile; sustained
	// non-zero values under clean shutdown indicate a real data-loss pattern.
	leaseTerminalEventDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "lease_terminal_event_dropped_total",
		Help:      "Terminal SM events sendTerminal refused to deliver (actor exited, mid-exit, or inbox wedged via send timeout)",
	}, []string{"event"})

	// dieEventDroppedTotal counts container-death signals routeToLease refused
	// to deliver to the lease actor. Refusal happens when stopCtx has been
	// canceled (backend shutting down) or the inbox is full and the
	// non-blocking send gives up. The reconciler re-detects missed deaths on
	// its next cycle (default 5m), so drops degrade the realtime event path
	// but do not cause data loss; the counter lets operators spot when that
	// path is degraded.
	dieEventDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "die_event_dropped_total",
		Help:      "Container-death signals routeToLease could not deliver (stopCtx canceled or inbox full); reconciler re-detects on next cycle",
	}, []string{"source"})

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

	// retainedVolumeBytes is the reserved disk capacity (sum of active retained
	// leases' SKU disk_mb, converted to bytes) currently pinned by soft-deleted
	// volumes. Reserved quota, not measured on-disk usage (see ENG-360 spec).
	retainedVolumeBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retained_volume_bytes",
		Help:      "Reserved disk capacity (SKU quota) pinned by retained/soft-deleted volumes, in bytes",
	})

	// retainedLeases is the number of active retained (soft-deleted) leases.
	retainedLeases = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retained_leases",
		Help:      "Number of active retained (soft-deleted) leases",
	})

	// retentionRefusedTotal counts close-time refuse-to-retain events caused by
	// the max_retained_disk_mb cap (volumes destroyed instead of retained).
	retentionRefusedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_refused_total",
		Help:      "Close-time refuse-to-retain events due to the max_retained_disk_mb cap",
	})

	// diskPoolBytes is the admission ceiling (total_disk_mb in bytes). A
	// denominator so dashboards don't hardcode the pool size.
	diskPoolBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "disk_pool_bytes",
		Help:      "Total disk admission pool (total_disk_mb) in bytes",
	})

	// retainedDiskCapBytes is the per-provider retained cap (max_retained_disk_mb
	// in bytes), set unconditionally by setStaticPoolMetrics (0 when the cap is
	// unset, since a registered gauge always exports). Alert denominator.
	retainedDiskCapBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retained_disk_cap_bytes",
		Help:      "Per-provider retained-volume cap (max_retained_disk_mb) in bytes; 0 when unset",
	})

	// retentionLeakedTotal counts leak events: a reap/evict/sweep that left a volume
	// on disk after a failed destroy, a deprovision give-up that abandoned a footprint,
	// or a restore-rollback whose revert did not commit. Always incremented even when
	// the store is too broken to take the tombstone write — the observable backstop.
	retentionLeakedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_leaked_total",
		Help:      "Retained-volume leak events (failed destroy / give-up / uncommitted revert) — see ENG-376",
	})

	// retentionReapingBytes is the reserved disk footprint (SKU quota) of records in
	// the reaping (pending-destroy) state — bytes still on disk awaiting reclaim.
	retentionReapingBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_reaping_bytes",
		Help:      "Reserved disk footprint of reaping (pending-destroy) retained records, in bytes",
	})

	// retentionReapingLeases is the count of reaping records (DLQ-style depth). A
	// sustained non-zero value means the sweep cannot reclaim a volume → operator action.
	retentionReapingLeases = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_reaping_leases",
		Help:      "Number of retained records stuck in the reaping (pending-destroy) state",
	})

	// retentionIndexReindexTotal counts retention in-memory index (re)builds by
	// trigger: "open" (one per store open / process start) and "manual" (an explicit
	// ReIndex self-heal). A rising "manual" rate signals operator-driven rebuilds; a
	// jump in "open" beyond restart cadence signals store churn (ENG-385).
	retentionIndexReindexTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem, // => fred_docker_backend_retention_index_reindex_total
		Name:      "retention_index_reindex_total",
		Help:      "Count of retention in-memory index (re)builds, by trigger (open|manual).",
	}, []string{"trigger"})
)

// reindexTriggers is the closed trigger set, used to pre-initialize the
// retentionIndexReindexTotal series to 0 so absence/ratio alert queries return 0,
// not no-data, before the first (re)build.
var reindexTriggers = []string{"open", "manual"}

func init() {
	// Pre-init both reindex-trigger series to 0, mirroring the orphan-skip pre-init.
	for _, tr := range reindexTriggers {
		retentionIndexReindexTotal.WithLabelValues(tr).Add(0)
	}
}

const bytesPerMiB = 1 << 20

// updateRetentionMetrics sets the retained-volume gauges. retainedMB is the
// ADMISSION total (active + reaping); count is the ACTIVE-only lease count;
// reapingMB/reapingCount are the reaping subset.
func updateRetentionMetrics(retainedMB int64, count int, reapingMB int64, reapingCount int) {
	retainedVolumeBytes.Set(float64(retainedMB) * bytesPerMiB)
	retainedLeases.Set(float64(count))
	retentionReapingBytes.Set(float64(reapingMB) * bytesPerMiB)
	retentionReapingLeases.Set(float64(reapingCount))
}

// setStaticPoolMetrics sets the constant denominator gauges once at startup.
func setStaticPoolMetrics(cfg Config) {
	diskPoolBytes.Set(float64(cfg.TotalDiskMB) * bytesPerMiB)
	// Set unconditionally so a re-construction with no cap resets the gauge to 0
	// (a registered gauge always exports; 0 == "no cap configured").
	retainedDiskCapBytes.Set(float64(cfg.MaxRetainedDiskMB) * bytesPerMiB)
}

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
