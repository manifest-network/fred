package docker

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
	"github.com/manifest-network/fred/internal/hmacauth"
)

// This file is the vocabulary of every labeled counter docker-backend
// exports, and the only file in this package that creates a counter series. A
// series that does not exist until its first event hides that event:
// increase() and rate() need a sample before the increment, so an alert on the
// first refusal, drop or failed pass after a restart never fires (ENG-1116).
// Prometheus's guidance is to export every series that may exist at 0 from
// startup and to keep references to the children of labeled metrics.
//
// Recording code therefore never names a label; it increments a handle bound
// here, at package initialization unless noted:
//
//   - a site that always records the same series increments a named field of
//     a handle struct;
//   - a call site that labels its own series (a destroy site, a teardown
//     operation, a refusal scope) passes a value declared here that carries
//     its handles, so an unbound site cannot be passed;
//   - a label computed from a domain value is selected by a total function
//     from that value to a field, or from a counterSet bound from the value
//     type's own complete list;
//   - a label known only from configuration (the volume backend's kind, a
//     configured rotation key) is bound by a bind function where the component
//     that writes it is constructed.
//
// internal/testutil's counter series guard keeps child-series creation inside
// this file and binders in constructors.

// seriesLabel is a call site's label value. It prints and logs as itself, so
// a site value can stand in for its label in a log line.
type seriesLabel string

func (l seriesLabel) String() string { return string(l) }

// LogValue logs the label as its text.
func (l seriesLabel) LogValue() slog.Value { return slog.StringValue(string(l)) }

// counterSet is one series per value of a closed set, all created when the
// set is bound. Its values come from the value type's own complete list, so
// every value the type can produce has a series; a value outside the list is
// the type's invalid value and counts nothing.
type counterSet[K comparable] struct{ byValue map[K]prometheus.Counter }

func bindCounterSet[K comparable](values []K, bind func(K) prometheus.Counter) counterSet[K] {
	byValue := make(map[K]prometheus.Counter, len(values))
	for _, value := range values {
		byValue[value] = bind(value)
	}
	return counterSet[K]{byValue: byValue}
}

// inc counts one event of value.
func (s counterSet[K]) inc(value K) { s.add(value, 1) }

// add counts n events of value.
func (s counterSet[K]) add(value K, n float64) {
	if counter, ok := s.byValue[value]; ok {
		counter.Add(n)
	}
}

// outcomeCounters splits a counter by a success-or-failure outcome.
type outcomeCounters struct{ success, failure prometheus.Counter }

// Provision and restore worker outcomes (provisions_total, restore_total).
var (
	provisionOutcomes = outcomeCounters{ //exhaustruct:enforce
		success: provisionsTotal.WithLabelValues("success"),
		failure: provisionsTotal.WithLabelValues("failure"),
	}
	restoreOutcomes = outcomeCounters{ //exhaustruct:enforce
		success: restoresTotal.WithLabelValues("success"),
		failure: restoresTotal.WithLabelValues("failure"),
	}
	// callbackDeliveries is callback_delivery_total, one per delivery outcome.
	callbackDeliveries = outcomeCounters{ //exhaustruct:enforce
		success: callbackDeliveryTotal.WithLabelValues("success"),
		failure: callbackDeliveryTotal.WithLabelValues("failure"),
	}
)

// record counts one outcome.
func (c outcomeCounters) record(succeeded bool) {
	if succeeded {
		c.success.Inc()
		return
	}
	c.failure.Inc()
}

// passCounters is a periodic loop's passes by outcome. Exactly one is counted
// per pass, so their sum is the loop's heartbeat and it exists from startup.
type passCounters struct{ ok, error prometheus.Counter }

var (
	// reconcilePasses is reconciliation_total, the reconcile loop's heartbeat.
	reconcilePasses = passCounters{ //exhaustruct:enforce
		ok:    reconciliationTotal.WithLabelValues("success"),
		error: reconciliationTotal.WithLabelValues("error"),
	}
	runtimeInventoryPasses = passCounters{ //exhaustruct:enforce
		ok:    runtimeInventoryTotal.WithLabelValues("ok"),
		error: runtimeInventoryTotal.WithLabelValues("error"),
	}
	seccompCensusPasses = passCounters{ //exhaustruct:enforce
		ok:    tenantSeccompCensusTotal.WithLabelValues(tenantSeccompCensusOK),
		error: tenantSeccompCensusTotal.WithLabelValues(tenantSeccompCensusError),
	}
	// retentionSweeps is retention_sweep_total (ENG-680).
	retentionSweeps = passCounters{ //exhaustruct:enforce
		ok:    retentionSweepTotal.WithLabelValues(sweepOutcomeSuccess),
		error: retentionSweepTotal.WithLabelValues(sweepOutcomeError),
	}
)

// runtimeRefreshLeases is runtime_refresh_total by each lease's final outcome.
var runtimeRefreshLeases = bindCounterSet(runtimeRefreshOutcomes, func(outcome RuntimeRefreshOutcome) prometheus.Counter {
	return runtimeRefreshTotal.WithLabelValues(string(outcome))
})

// imageGCCounters is image_gc_total by decision.
type imageGCCounters struct {
	busy, inhibited, shared, belowThreshold, removed, error, panic prometheus.Counter
}

var imageGCDecisions = imageGCCounters{ //exhaustruct:enforce
	busy:           imageGCTotal.WithLabelValues("busy"),
	inhibited:      imageGCTotal.WithLabelValues("inhibited"),
	shared:         imageGCTotal.WithLabelValues("shared"),
	belowThreshold: imageGCTotal.WithLabelValues("below_threshold"),
	removed:        imageGCTotal.WithLabelValues("removed"),
	error:          imageGCTotal.WithLabelValues("error"),
	panic:          imageGCTotal.WithLabelValues("panic"),
}

// idempotentOpCounters is idempotent_ops_total by Docker operation and the
// reason the daemon reported it already done.
type idempotentOpCounters struct {
	createAlreadyExists, removeNotFound, removeInProgress prometheus.Counter
}

var idempotentOps = idempotentOpCounters{ //exhaustruct:enforce
	createAlreadyExists: idempotentOpsTotal.WithLabelValues("create", "already_exists"),
	removeNotFound:      idempotentOpsTotal.WithLabelValues("remove", "not_found"),
	removeInProgress:    idempotentOpsTotal.WithLabelValues("remove", "in_progress"),
}

// networkReclamationCounters is network_reclamation_total for outcomes that do
// not depend on the network's scope.
type networkReclamationCounters struct {
	removed, absent, inUse, error, listError, budgetExhausted prometheus.Counter
}

var networkReclamations = networkReclamationCounters{ //exhaustruct:enforce
	removed:         networkReclamationTotal.WithLabelValues("removed"),
	absent:          networkReclamationTotal.WithLabelValues("absent"),
	inUse:           networkReclamationTotal.WithLabelValues("in_use"),
	error:           networkReclamationTotal.WithLabelValues("error"),
	listError:       networkReclamationTotal.WithLabelValues("list_error"),
	budgetExhausted: networkReclamationTotal.WithLabelValues("budget_exhausted"),
}

// networkReclamationScope is the scope of a managed network the reclaimer
// left alone because its owner is still live: <scope>_active when the owner
// is active, <scope>_busy when its lifecycle is busy.
type networkReclamationScope struct {
	seriesLabel
	active, busy prometheus.Counter
}

var (
	leaseNetworkReclamation  = bindNetworkReclamationScope("lease")
	tenantNetworkReclamation = bindNetworkReclamationScope("tenant")
)

func bindNetworkReclamationScope(scope seriesLabel) networkReclamationScope {
	return networkReclamationScope{ //exhaustruct:enforce
		seriesLabel: scope,
		active:      networkReclamationTotal.WithLabelValues(string(scope) + "_active"),
		busy:        networkReclamationTotal.WithLabelValues(string(scope) + "_busy"),
	}
}

// quotaBackfillCounters is volume_quota_backfill_total by outcome (ENG-454).
type quotaBackfillCounters struct{ applied, failed, deletePending prometheus.Counter }

var quotaBackfills = quotaBackfillCounters{ //exhaustruct:enforce
	applied:       volumeQuotaBackfillTotal.WithLabelValues("applied"),
	failed:        volumeQuotaBackfillTotal.WithLabelValues("failed"),
	deletePending: volumeQuotaBackfillTotal.WithLabelValues(quotaBackfillDeletePending),
}

// reapSkipCounters is retention_reap_skips_total by reason (ENG-659).
type reapSkipCounters struct{ claimUnreadable, ownerClaimed prometheus.Counter }

var reapSkips = reapSkipCounters{ //exhaustruct:enforce
	claimUnreadable: retentionReapSkipsTotal.WithLabelValues(reapSkipClaimUnreadable),
	ownerClaimed:    retentionReapSkipsTotal.WithLabelValues(reapSkipOwnerClaimed),
}

// orphanSkipCounters is retention_orphan_skips_total by reason (ENG-370).
type orphanSkipCounters struct{ listError, raced, disabled, storeError prometheus.Counter }

var orphanSkips = orphanSkipCounters{ //exhaustruct:enforce
	listError:  retentionOrphanSkipsTotal.WithLabelValues(orphanSkipListError),
	raced:      retentionOrphanSkipsTotal.WithLabelValues(orphanSkipRaced),
	disabled:   retentionOrphanSkipsTotal.WithLabelValues(orphanSkipDisabled),
	storeError: retentionOrphanSkipsTotal.WithLabelValues(orphanSkipStoreError),
}

// capCheckCounters is retention_cap_check_failed_total by check.
type capCheckCounters struct{ evict, breach, bound, refuseGet prometheus.Counter }

var capCheckFailures = capCheckCounters{ //exhaustruct:enforce
	evict:     retentionCapCheckFailedTotal.WithLabelValues(capCheckEvict),
	breach:    retentionCapCheckFailedTotal.WithLabelValues(capCheckBreach),
	bound:     retentionCapCheckFailedTotal.WithLabelValues(capCheckBound),
	refuseGet: retentionCapCheckFailedTotal.WithLabelValues(capCheckRefuseGet),
}

// partitionCollapses is retention_partition_collapsed_total by reason, from
// shared's closed reason set.
var partitionCollapses = bindCounterSet(shared.PartitionCollapseReasons, func(reason shared.PartitionCollapseReason) prometheus.Counter {
	return retentionPartitionCollapsedTotal.WithLabelValues(string(reason))
})

// retentionRefusalScope is the cap level that tripped a close-time
// refuse-to-retain, with its retention_refused_by_scope_total series.
type retentionRefusalScope struct {
	seriesLabel
	refused prometheus.Counter
}

var (
	refuseScopeGlobal    = bindRetentionRefusalScope("global")    // L0 max_retained_disk_mb
	refuseScopeTenant    = bindRetentionRefusalScope("tenant")    // L1 per-tenant aggregate disk
	refuseScopePartition = bindRetentionRefusalScope("partition") // L2 per-partition disk sub-cap
)

func bindRetentionRefusalScope(scope seriesLabel) retentionRefusalScope {
	return retentionRefusalScope{seriesLabel: scope, refused: retentionRefusedByScopeTotal.WithLabelValues(string(scope))}
}

// retentionReindexCounters is retention_index_reindex_total by trigger.
type retentionReindexCounters struct{ open, manual prometheus.Counter }

var retentionReindexes = retentionReindexCounters{ //exhaustruct:enforce
	open:   retentionIndexReindexTotal.WithLabelValues(shared.RetentionReindexOpen.String()),
	manual: retentionIndexReindexTotal.WithLabelValues(shared.RetentionReindexManual.String()),
}

// of is the trigger's series; the trigger's invalid zero value has none.
func (c retentionReindexCounters) of(trigger shared.RetentionReindexTrigger) (prometheus.Counter, bool) {
	switch trigger {
	case shared.RetentionReindexOpen:
		return c.open, true
	case shared.RetentionReindexManual:
		return c.manual, true
	default:
		return nil, false
	}
}

// destroySite is an exact-authority destroy path the ownership choke point
// can turn away (ENG-658), with its volume_destroy_refused_total series by
// reason. Each caller of volumeOp.destroy passes its own site. Both reasons
// mean "the bytes are still on disk", but they differ in who is expected to
// clear them: claimed is another lease's live claim and resolves itself when
// that lease's restore commits or rolls back, whereas unreadable is the
// fail-safe — ownership could not be established at all, so nothing was
// destroyed. noDestroyer cannot happen with any volumeManager in this repo
// (all four implement Destroy, and New fails fast if the configured one does
// not); it exists because the capability is obtained by assertion, so a
// future decorator around b.volumes would silently drop it, and a refusal is
// the safe reading.
type destroySite struct {
	seriesLabel
	claimed, unreadable, noDestroyer prometheus.Counter
}

var (
	destroySiteDeprovisionDestroy = bindDestroySite("deprovision_destroy") // doDeprovision's non-retain arm
	destroySiteDeprovisionReclaim = bindDestroySite("deprovision_reclaim") // doDeprovision's writable-path-only reclaim (ENG-406)
	destroySiteRetentionRefused   = bindDestroySite("retention_refused")   // destroyOnRefuseToRetain, a breached retained-disk cap
	destroySiteReaping            = bindDestroySite("reaping")             // destroyReapingVolumes, the retention finalizer
	destroySiteStartupRollback    = bindDestroySite("startup_rollback")    // rollbackStartupFailure, a definite startup failure's own volumes (ENG-1125)
)

func bindDestroySite(site seriesLabel) destroySite {
	refused := func(reason string) prometheus.Counter {
		return volumeDestroyRefusedTotal.WithLabelValues(string(site), reason)
	}
	return destroySite{ //exhaustruct:enforce
		seriesLabel: site,
		claimed:     refused(destroyRefusedClaimed),
		unreadable:  refused(destroyRefusedUnreadable),
		noDestroyer: refused(destroyRefusedNoDestroyer),
	}
}

// teardownOperation is a per-container teardown recovery path (ENG-647),
// with its teardown_fallback_total series by outcome: recovered when
// recovery proved the destination containers absent, failed when a container
// may still be running.
type teardownOperation struct {
	seriesLabel
	recovered, failed prometheus.Counter
}

var (
	teardownOpRestoreReconcile = bindTeardownOperation("restore_reconcile") // reconcileRestoring's orphaned arm (boot + retention sweep)
	teardownOpDeprovision      = bindTeardownOperation("deprovision")       // doDeprovision's close-path teardown
	teardownOpProvisionCleanup = bindTeardownOperation("provision_cleanup") // failed provision retains its WAL and accounting for recovery
)

func bindTeardownOperation(operation seriesLabel) teardownOperation {
	return teardownOperation{ //exhaustruct:enforce
		seriesLabel: operation,
		recovered:   teardownFallbackTotal.WithLabelValues(string(operation), teardownOutcomeRecovered),
		failed:      teardownFallbackTotal.WithLabelValues(string(operation), teardownOutcomeFailed),
	}
}

// treeRemovalOutcomeCounters is tree_removals_total for one site, by outcome.
type treeRemovalOutcomeCounters struct {
	removed, cutRefused, treeChanged, crossDevice, undeletable, canceled, error prometheus.Counter
}

// treeRemovalSite is where fstree removes a tenant-shaped tree (ENG-1117),
// with its removal and cut series.
type treeRemovalSite struct {
	seriesLabel
	outcomes treeRemovalOutcomeCounters
	cuts     prometheus.Counter
}

var (
	// treeRemovalSiteDeleteStage is the XFS deletion of a condemned volume.
	treeRemovalSiteDeleteStage = bindTreeRemovalSite("delete_stage")
	// treeRemovalSiteWritablePath is the writable-path wipe before reseeding.
	treeRemovalSiteWritablePath = bindTreeRemovalSite("writable_path")
)

func bindTreeRemovalSite(site seriesLabel) treeRemovalSite {
	outcome := func(label string) prometheus.Counter {
		return treeRemovalsTotal.WithLabelValues(string(site), label)
	}
	return treeRemovalSite{ //exhaustruct:enforce
		seriesLabel: site,
		outcomes: treeRemovalOutcomeCounters{ //exhaustruct:enforce
			removed:     outcome(treeRemovalOutcomeRemoved),
			cutRefused:  outcome(treeRemovalOutcomeCutRefused),
			treeChanged: outcome(treeRemovalOutcomeTreeChanged),
			crossDevice: outcome(treeRemovalOutcomeCrossDevice),
			undeletable: outcome(treeRemovalOutcomeUndeletable),
			canceled:    outcome(treeRemovalOutcomeCanceled),
			error:       outcome(treeRemovalOutcomeError),
		},
		cuts: treeRemovalCutsTotal.WithLabelValues(string(site)),
	}
}

// volumeDeleteCounters is volume_delete_outcomes_total by outcome (ENG-1117).
type volumeDeleteCounters struct {
	completed, heldRemoval, heldUnsized, heldResidual, latched prometheus.Counter
}

var volumeDeleteOutcomes = volumeDeleteCounters{ //exhaustruct:enforce
	completed:    volumeDeleteOutcomesTotal.WithLabelValues(volumeDeleteOutcomeCompleted),
	heldRemoval:  volumeDeleteOutcomesTotal.WithLabelValues(volumeDeleteOutcomeHeldRemoval),
	heldUnsized:  volumeDeleteOutcomesTotal.WithLabelValues(volumeDeleteOutcomeHeldUnsized),
	heldResidual: volumeDeleteOutcomesTotal.WithLabelValues(volumeDeleteOutcomeHeldResidual),
	latched:      volumeDeleteOutcomesTotal.WithLabelValues(volumeDeleteOutcomeLatched),
}

// projidAudits is volume_projid_audit_total by audit outcome.
var projidAudits = bindCounterSet(projidAuditOutcomes[:], func(outcome projidAuditOutcome) prometheus.Counter {
	return volumeProjidAuditTotal.WithLabelValues(outcome.label())
})

// seccompProfileRefusals is seccomp_profile_refusals_total by creation sink.
var seccompProfileRefusals = bindCounterSet(tenantSeccompSinks[:], func(sink tenantSeccompSink) prometheus.Counter {
	return tenantSeccompProfileRefusalsTotal.WithLabelValues(sink.label())
})

// requestAuthRefusals is request_auth_failures_total by refusal reason.
var requestAuthRefusals = bindCounterSet(requestAuthFailures[:], func(failure requestAuthFailure) prometheus.Counter {
	return requestAuthFailuresTotal.WithLabelValues(failure.label())
})

// RequestSignatureMetrics counts providerd request signatures verified, by key
// slot. Its series exist for the slots configured when it was bound.
type RequestSignatureMetrics struct {
	current prometheus.Counter
	// next is nil when no rotation key is configured: no request can then
	// verify against the next slot.
	next prometheus.Counter
}

// BindRequestSignatureMetrics creates the request key series for keys: the
// current slot always, the next slot only while a next key is configured, so
// a zero on "next" means unused rather than not configured. docker-backend
// binds it once, where it builds its server.
func BindRequestSignatureMetrics(keys hmacauth.VerifyKeys) RequestSignatureMetrics {
	metrics := RequestSignatureMetrics{current: requestSignatureKeyTotal.WithLabelValues(requestKeySlotCurrent), next: nil}
	if keys.HasRotation() {
		metrics.next = requestSignatureKeyTotal.WithLabelValues(requestKeySlotNext)
		requestNextKeyConfigured.Set(1)
		return metrics
	}
	requestNextKeyConfigured.Set(0)
	return metrics
}

// restoreDemoteRefusalCounters is restore_demote_refused_total for the
// configured volume backend, by reason (ENG-438).
type restoreDemoteRefusalCounters struct {
	ephemeralTier, unmeasurableReadError, unmeasurableBackend, measuredExceeds prometheus.Counter
}

// bindRestoreDemoteRefusals creates the demote-refusal series of one volume
// backend kind. New binds the configured backend's kind.
func bindRestoreDemoteRefusals(backendKind string) restoreDemoteRefusalCounters {
	reason := func(reason string) prometheus.Counter {
		return restoreDemoteRefusedTotal.WithLabelValues(backendKind, reason)
	}
	return restoreDemoteRefusalCounters{ //exhaustruct:enforce
		ephemeralTier:         reason("ephemeral_tier"),
		unmeasurableReadError: reason("unmeasurable_read_error"),
		unmeasurableBackend:   reason("unmeasurable_backend"),
		measuredExceeds:       reason("measured_exceeds"),
	}
}

// maintenanceReadinessBranch is the maintenance recovery branch whose
// readiness wait is pending, with its maintenance_readiness_pending_total
// series.
type maintenanceReadinessBranch struct {
	seriesLabel
	pending prometheus.Counter
}

var (
	readinessSourceOnly      = bindMaintenanceReadinessBranch("source_only")
	readinessCommittedTarget = bindMaintenanceReadinessBranch("committed_target")
	readinessDeployingTarget = bindMaintenanceReadinessBranch("deploying_target")
	readinessCleanupSource   = bindMaintenanceReadinessBranch("cleanup_source")
)

func bindMaintenanceReadinessBranch(branch seriesLabel) maintenanceReadinessBranch {
	return maintenanceReadinessBranch{seriesLabel: branch, pending: maintenanceReadinessPendingTotal.WithLabelValues(string(branch))}
}

// operationRecoveryTimeout is a bounded cold-recovery timeout an interrupted
// provision intent can exhaust, with its
// operation_intent_recovery_timeout_exhaustions_total series.
type operationRecoveryTimeout struct {
	seriesLabel
	exhausted prometheus.Counter
}

var operationRecoveryTimeoutProvision = operationRecoveryTimeout{
	seriesLabel: "provision_timeout",
	exhausted:   operationIntentRecoveryTimeoutExhaustionsTotal.WithLabelValues("provision_timeout"),
}

// operationCleanupRetries is operation_intent_recovery_cleanup_retries_total
// by intent kind.
var operationCleanupRetries = bindCounterSet(
	[]shared.OperationIntentKind{shared.OperationIntentProvision, shared.OperationIntentRestore},
	func(kind shared.OperationIntentKind) prometheus.Counter {
		return operationIntentRecoveryCleanupRetriesTotal.WithLabelValues(string(kind))
	},
)

// terminalSubstrateCleanupRetries is terminal_substrate_cleanup_retries_total
// by receipt authority.
var terminalSubstrateCleanupRetries = bindCounterSet(terminalReceiptKinds, func(kind terminalReceiptKind) prometheus.Counter {
	return terminalSubstrateCleanupRetriesTotal.WithLabelValues(string(kind))
})

// containerEventStreamCounters is container_event_stream_total by outcome
// (ENG-799).
type containerEventStreamCounters struct{ connected, reconnect, exited prometheus.Counter }

var containerEventStreams = containerEventStreamCounters{ //exhaustruct:enforce
	connected: containerEventStreamTotal.WithLabelValues(containerEventStreamConnected),
	reconnect: containerEventStreamTotal.WithLabelValues(containerEventStreamReconnect),
	exited:    containerEventStreamTotal.WithLabelValues(containerEventStreamExited),
}

// deathSource is where a container death was observed, with its
// die_event_dropped_total series.
type deathSource struct {
	seriesLabel
	dropped prometheus.Counter
}

var (
	// eventLoopDeathsDropped is the event loop's die_event_dropped_total
	// series. The container event reader updates it directly: it must be a
	// plain client_golang collector, so updating it calls no package code
	// (internal/testutil's container event reader guard).
	eventLoopDeathsDropped = dieEventDroppedTotal.WithLabelValues("event_loop")
	// deathSourceEventLoop is the container event loop's live stream.
	deathSourceEventLoop = deathSource{seriesLabel: "event_loop", dropped: eventLoopDeathsDropped}
	// deathSourceReconcile is the reconcile sweep's re-detection.
	deathSourceReconcile = bindDeathSource("reconcile")
)

func bindDeathSource(source seriesLabel) deathSource {
	return deathSource{seriesLabel: source, dropped: dieEventDroppedTotal.WithLabelValues(string(source))}
}

// The lease state machine's series, bound from leasesm's and failurecause's
// own complete lists: every transition, worker panic, dropped terminal event
// and failure attribution the actor can report exists from startup.
var (
	leaseTransitions = bindCounterSet(leasesm.Transitions(), func(transition leasesm.Transition) prometheus.Counter {
		return leaseSMTransitionsTotal.WithLabelValues(transition.From(), transition.To(), transition.Event())
	})
	leaseWorkerPanics = bindCounterSet(leasesm.Workers(), func(worker leasesm.Worker) prometheus.Counter {
		return leaseWorkerPanicsTotal.WithLabelValues(worker.Label())
	})
	// imageFlightPanics is lease_worker_panics_total for the image
	// preparation worker, which this package runs rather than leasesm.
	imageFlightPanics  = leaseWorkerPanicsTotal.WithLabelValues("image_flight")
	leaseTerminalDrops = bindCounterSet(leasesm.TerminalEvents(), func(event leasesm.TerminalEvent) prometheus.Counter {
		return leaseTerminalEventDroppedTotal.WithLabelValues(event.Label())
	})
	// leaseFailures is keyed by attribution label, not by Cause: only
	// failurecause mints a Cause, because a counting one closes a lease.
	leaseFailures = bindCounterSet(failurecause.Labels(), func(attribution string) prometheus.Counter {
		return leaseFailuresTotal.WithLabelValues(attribution)
	})
)
