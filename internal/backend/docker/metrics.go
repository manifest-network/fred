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

// Scope labels for retentionRefusedByScopeTotal — the cap level that tripped a
// close-time refuse-to-retain. Kept as constants so the disk gate, the pre-init,
// and the tests cannot drift on a typo.
const (
	refuseScopeGlobal    = "global"    // L0 max_retained_disk_mb
	refuseScopeTenant    = "tenant"    // L1 per-tenant aggregate disk
	refuseScopePartition = "partition" // L2 per-partition disk sub-cap
)

// Check labels for retentionCapCheckFailedTotal — the retention cap check that
// failed open on a store error (fail-open is data-safe but must be alertable).
// This set deliberately EXTENDS the spec §6.4 enumeration (evict|breach|bound)
// with refuse_get so that EVERY store-read fail-open in a cap decision is counted
// — including the existing-record Get guard in shouldRefuseRetention, which the
// spec's three-way enumeration omitted.
const (
	capCheckEvict     = "evict"      // an eviction pass's snapshot/CAS store error
	capCheckBreach    = "breach"     // the disk-gate List() read error (breachRetentionCaps)
	capCheckBound     = "bound"      // the partition-bound snapshot read error (boundPartition)
	capCheckRefuseGet = "refuse_get" // the existing-record Get read error (shouldRefuseRetention)
)

// refuseScopes / capChecks are the closed label sets, used to pre-initialize the
// CounterVec series to 0 so absence/ratio alert queries return 0, not no-data.
var refuseScopes = []string{refuseScopeGlobal, refuseScopeTenant, refuseScopePartition}
var capChecks = []string{capCheckEvict, capCheckBreach, capCheckBound, capCheckRefuseGet}

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

	// restoresTotal counts restore re-deploy WORKER outcomes by result. Unlike
	// restoreDurationSeconds (success-only), it increments on BOTH the success and
	// failure branches of doRestore's terminal defer (the rollbackRestoreAdoption
	// path, panics included), so a docker-backend-side restore success rate is
	// computable from the worker's own metrics — mirroring provisionsTotal, which
	// likewise counts only doProvision's worker outcome (ENG-408).
	//
	// Worker-scoped like restore_duration_seconds: a restore that fails in the
	// SYNCHRONOUS adopt prelude (claim/rename/route/ack) before the worker spawns
	// returns a synchronous error to the caller and is counted by NEITHER outcome
	// here — exactly as provisionsTotal omits synchronous provision failures. Such
	// failures surface to the tenant as the Restore() error / HTTP status. Note
	// providerd's fred_provisioner_provisioning_total{operation=restore} does NOT
	// backfill this gap: it counts the async callback/timeout outcomes, not the
	// backend's synchronous errors (see internal/api/handlers.go restore handler).
	// outcome ∈ restoreOutcomes ("success"/"failure", matching provisionsTotal).
	restoresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "restore_total",
		Help:      "Total number of restore re-deploy worker attempts by outcome",
	}, []string{"outcome"})

	// volumeQuotaBackfillTotal counts per-volume quota re-application attempts by
	// the startup reconcile (reconcileVolumeQuotas), which re-tags + re-limits
	// existing volumes so leases provisioned before the daemon held CAP_SYS_ADMIN
	// get their disk_mb enforced without a re-provision. outcome ∈ {applied,failed}.
	// (ENG-454)
	volumeQuotaBackfillTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "volume_quota_backfill_total",
		Help:      "Startup quota-backfill per-volume re-application attempts by outcome",
	}, []string{"outcome"})

	// volumeQuotaClearFailedTotal counts XFS project-quota clear failures during
	// volume Destroy (ENG-459). Destroy resets a project's bhard limit to 0 so its
	// quota-table entry drops out of xfs_quota's scans once the directory is gone;
	// the clear is best-effort (it must not wedge teardown — a leaked limit entry
	// holds no disk), so a failure is logged and counted here rather than propagated.
	// The observable backstop, mirroring retentionLeakedTotal: a rising rate means
	// the project-quota table is regrowing and needs operator cleanup.
	volumeQuotaClearFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "volume_quota_clear_failed_total",
		Help:      "XFS project-quota clear failures on volume destroy (leaked table entry needs operator cleanup) — see ENG-459",
	})

	// restoreDemoteRefusedTotal counts restores refused by the demote
	// fit-gate (checkDemoteFit) because the retained data does not fit the
	// requested smaller tier, by backend and reason. Fixed-cardinality
	// labels: backend ∈ {btrfs,xfs,zfs,noop,mock}; reason ∈
	// {measured_exceeds, unmeasurable_read_error, unmeasurable_backend,
	// ephemeral_tier}. These are synchronous-prelude refusals NOT counted by
	// restoresTotal (which is worker-scoped). (ENG-438)
	restoreDemoteRefusedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "restore_demote_refused_total",
		Help:      "Restores refused because retained data exceeds the requested smaller tier, by backend and reason",
	}, []string{"backend", "reason"})

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

	// retentionWritablePathReclaimedTotal counts managed volumes that, on close,
	// were destroyed (reclaimed) rather than soft-deleted because they held only
	// ephemeral writable-path scaffolding (a _wp/ subtree, no declared-VOLUME
	// data). Such volumes are non-durable by the ENG-367 wipe-contract, so
	// retaining them would only pollute the retention record/slot/dir/budget
	// (ENG-406). A rising rate quantifies how much retention pollution the
	// per-class reclaim policy is avoiding; pairs with retention_refused_total
	// and the retained-volume gauges on the ENG-405 dashboards.
	retentionWritablePathReclaimedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_writable_path_reclaimed_total",
		Help:      "Total writable-path-only volumes destroyed (reclaimed) at close instead of retained",
	})

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

	// retentionEvictedTotal counts close-time per-tenant cap evictions: each time
	// evictRetentionsToCap evicts one of a CLOSING tenant's OWN oldest active
	// retained leases to honor max_retained_leases_per_tenant. Counted ONCE per
	// record at the active→reaping transition — BEFORE and independent of the volume
	// destroy, which may fail and leave the record reaping. So an increment means
	// "evicted from the active cap set (marked reaping)", NOT "successfully
	// destroyed" (a failed destroy is separately visible via retention_leaked_total
	// / retention_reaping_*). DISTINCT from retentionRefusedTotal, the GLOBAL
	// max_retained_disk_mb refuse-to-retain path — do not conflate the two. ENG-407.
	retentionEvictedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_evicted_total",
		Help:      "Close-time per-tenant cap evictions (max_retained_leases_per_tenant); a tenant's oldest retained lease evicted from the active set (marked reaping) to make room",
	})

	// retentionPartitionCollapsedTotal counts aggregator partition declarations
	// collapsed to the default ("") bucket at close, by reason. Counted per close
	// ATTEMPT (retries re-count): on a hydration-degraded retry of a labeled
	// record, no_input fires while the PutActiveMerged guard preserves the stored
	// label — expected, not a fault. NO tenant or partition label is EVER attached
	// (the values are tenant-supplied and unbounded — the zero-identity-label
	// posture is deliberate).
	retentionPartitionCollapsedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_partition_collapsed_total",
		Help:      "Aggregator partition declarations collapsed to the default bucket at close, by reason (per close attempt)",
	}, []string{"reason"})

	// retentionPartitionStampedTotal counts close attempts that resolved a
	// non-empty retention partition. It is the adoption/typo detector: a source
	// configured and a tenant allowlisted but this flat at 0 ⇒ the key never
	// matches (a manifest typo or an unpopulated label/env).
	retentionPartitionStampedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_partition_stamped_total",
		Help:      "Close attempts that resolved a non-empty retention partition",
	})

	// retentionPartitions is the number of distinct non-empty (tenant, partition)
	// buckets across active+restoring retained records. Set from
	// refreshRetentionAccounting's existing full List() pass (zero extra store
	// I/O). Alert headroom must allow the bounded concurrent-close overshoot.
	retentionPartitions = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_partitions",
		Help:      "Distinct non-empty (tenant, partition) buckets across active+restoring retained records",
	})

	// retentionPartitionEvictedTotal counts close-time per-partition sub-cap
	// evictions — L2 ONLY. This is an aggregator's own noisy-customer containment
	// tool, NOT a provider capacity signal. retentionEvictedTotal keeps its
	// deployed L1-per-tenant meaning; the two are never conflated.
	retentionPartitionEvictedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_partition_evicted_total",
		Help:      "Close-time per-partition sub-cap evictions (an aggregator's own noisy-customer containment; NOT a provider capacity signal)",
	})

	// retentionRefusedByScopeTotal counts close-time refuse-to-retain events by
	// the tripped cap scope (global|tenant|partition). The bare
	// retentionRefusedTotal keeps its deployed L0-global-only meaning (the
	// BackendRetentionRefused alert keys on it), so this is the scoped superset.
	retentionRefusedByScopeTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_refused_by_scope_total",
		Help:      "Close-time refuse-to-retain events by tripped cap scope (global|tenant|partition)",
	}, []string{"scope"})

	// retentionCapCheckFailedTotal counts retention cap checks that failed open on
	// a store error, by check (evict|breach|bound|refuse_get). Fail-open is correct
	// (never destroy on uncertainty) but must be alertable: a sustained rate means
	// the quota gates are silently off.
	retentionCapCheckFailedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_cap_check_failed_total",
		Help:      "Retention cap checks that failed open on a store error, by check (evict|breach|bound|refuse_get)",
	}, []string{"check"})

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

	// restoreFinalizerPendingTotal counts restore finalizations kept pending: a restore
	// succeeded (new lease Ready) but its active-release write failed, so the retention
	// record is LEFT restoring to keep protecting the adopted volume as its finalizer
	// (ENG-523). Reconcile sweeps retry, so a lingering record re-increments this each
	// sweep — a sustained rate means the release store is failing (mirrors the
	// observable-backstop role of retentionLeakedTotal).
	restoreFinalizerPendingTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "restore_finalizer_pending_total",
		Help:      "Restore-finalizer kept-pending events: a successful restore whose active-release write failed, so the retention record stays restoring to keep protecting the adopted volume (ENG-523). A sustained rate means the release store is failing.",
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

// restoreOutcomes is the closed outcome set for restoresTotal, pre-initialized to 0
// so a restore success-rate / failure-ratio query returns 0, not no-data, before the
// first restore completes (ENG-408). Values mirror provisionsTotal.
var restoreOutcomes = []string{"success", "failure"}

// quotaBackfillOutcomes is the closed outcome set for volumeQuotaBackfillTotal,
// pre-initialized to 0 so a backfill failure-ratio query returns 0, not no-data,
// before the first startup reconcile (ENG-454).
var quotaBackfillOutcomes = []string{"applied", "failed"}

func init() {
	// Pre-init both reindex-trigger series to 0, mirroring the orphan-skip pre-init.
	for _, tr := range reindexTriggers {
		retentionIndexReindexTotal.WithLabelValues(tr).Add(0)
	}
	// Pre-init both restore-outcome series to 0 (ENG-408) so the docker-backend restore
	// success rate reads 0, not no-data, before the first restore.
	for _, oc := range restoreOutcomes {
		restoresTotal.WithLabelValues(oc).Add(0)
	}
	// Pre-init the quota-backfill outcomes to 0 (ENG-454).
	for _, oc := range quotaBackfillOutcomes {
		volumeQuotaBackfillTotal.WithLabelValues(oc).Add(0)
	}
	// Pre-init the partition-collapse reasons, refuse scopes, and cap-check series
	// to 0 so absence/ratio alert queries return 0, not no-data, before the first
	// close.
	for _, r := range shared.PartitionCollapseReasons {
		retentionPartitionCollapsedTotal.WithLabelValues(r).Add(0)
	}
	for _, s := range refuseScopes {
		retentionRefusedByScopeTotal.WithLabelValues(s).Add(0)
	}
	for _, c := range capChecks {
		retentionCapCheckFailedTotal.WithLabelValues(c).Add(0)
	}
	// Pre-init the two unlabeled partition counters to 0 so a stamp/eviction-rate
	// query reads 0, not no-data, before the first event. Their increments are
	// wired at the close path (stamped) and the L2 eviction pass (evicted) in the
	// close-path restructure that follows this commit.
	retentionPartitionStampedTotal.Add(0)
	retentionPartitionEvictedTotal.Add(0)
}

const bytesPerMiB = 1 << 20

// updateRetentionMetrics sets the retained-volume gauges. retainedMB is the
// ADMISSION total (active + reaping); count is the ACTIVE-only lease count;
// reapingMB/reapingCount are the reaping subset; partitionCount is the number of
// distinct non-empty (tenant, partition) buckets across active+restoring records.
func updateRetentionMetrics(retainedMB int64, count int, reapingMB int64, reapingCount int, partitionCount int) {
	retainedVolumeBytes.Set(float64(retainedMB) * bytesPerMiB)
	retainedLeases.Set(float64(count))
	retentionReapingBytes.Set(float64(reapingMB) * bytesPerMiB)
	retentionReapingLeases.Set(float64(reapingCount))
	retentionPartitions.Set(float64(partitionCount))
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
