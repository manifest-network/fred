package metrics

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// This file is the vocabulary of every labeled counter in this package. A
// counter series that does not exist until its first event hides that event:
// increase() and rate() need a sample before the increment, so an alert on
// the first panic, refusal or failed sweep after a restart never fires
// (ENG-1116). Prometheus's own guidance is to export every series you know may
// exist at 0 from startup, and to keep references to the children of labeled
// metrics.
//
// That is enforced here by construction. Every counter vec in this package is
// unexported, so no other package can create a child series. A caller can
// only increment a handle this file bound:
//
//   - a closed vocabulary is a package-level handle struct, bound at package
//     initialization;
//   - a family whose series must exist only while something is configured (a
//     snapshot schedule, a sub-signer pool) is bound by a Bind function that
//     returns every series of the family at once, called where that thing is
//     constructed;
//   - a label known only from configuration (a backend name, an ack lane, a
//     signer address) is bound the same way, once per configured value, where
//     the component that writes it is constructed. A recording path only looks
//     series up: ForBackend never creates one;
//   - a label set that is open by nature (an HTTP status code) is written
//     through a recording function (ObserveAPIRequest) and documented as
//     created on first event. Those families are not alerted with increase()
//     or rate(). Such a function derives its other labels itself, from a
//     fixed vocabulary, so no caller supplies a label string.
//
// internal/testutil's counter series guard keeps child-series creation inside
// the files that declare a vocabulary.

// ReconcilerRunCounters is fred_reconciler_runs_total by outcome. Their sum is
// the reconcile loop's heartbeat, so it exists from startup and a first sweep
// that never completes is visible.
type ReconcilerRunCounters struct {
	Success, Partial, Error, Degraded prometheus.Counter
}

// ReconcilerRuns counts reconciliation sweeps.
var ReconcilerRuns = ReconcilerRunCounters{
	Success:  reconciliationTotal.WithLabelValues(OutcomeSuccess),
	Partial:  reconciliationTotal.WithLabelValues(OutcomePartial),
	Error:    reconciliationTotal.WithLabelValues(OutcomeError),
	Degraded: reconciliationTotal.WithLabelValues(OutcomeDegraded),
}

// ReconcilerActionCounters is fred_reconciler_actions_total by action.
type ReconcilerActionCounters struct {
	Provisioned, Acknowledged, Deprovisioned, Anomaly, LeaseError prometheus.Counter
}

// ReconcilerActions counts actions taken during reconciliation.
var ReconcilerActions = ReconcilerActionCounters{
	Provisioned:   reconciliationActions.WithLabelValues(ActionProvisioned),
	Acknowledged:  reconciliationActions.WithLabelValues(ActionAcknowledged),
	Deprovisioned: reconciliationActions.WithLabelValues(ActionDeprovisioned),
	Anomaly:       reconciliationActions.WithLabelValues(ActionAnomaly),
	LeaseError:    reconciliationActions.WithLabelValues(ActionLeaseError),
}

// ReconcilerPanicCounters is fred_provisioner_reconciler_panics_total by stage.
type ReconcilerPanicCounters struct {
	ProcessLease, ProcessOrphan                     prometheus.Counter
	FetchProvisions, FetchRetentions, FetchReleases prometheus.Counter
	FetchChainLeases, CheckPlacementMarker          prometheus.Counter
	PlacementCleanup                                prometheus.Counter
}

// ReconcilerPanics counts panics recovered in reconciler per-unit goroutines.
var ReconcilerPanics = ReconcilerPanicCounters{
	ProcessLease:         reconcilerPanicsTotal.WithLabelValues("process_lease"),
	ProcessOrphan:        reconcilerPanicsTotal.WithLabelValues("process_orphan"),
	FetchProvisions:      reconcilerPanicsTotal.WithLabelValues("fetch_provisions"),
	FetchRetentions:      reconcilerPanicsTotal.WithLabelValues("fetch_retentions"),
	FetchReleases:        reconcilerPanicsTotal.WithLabelValues("fetch_releases"),
	FetchChainLeases:     reconcilerPanicsTotal.WithLabelValues("fetch_chain_leases"),
	CheckPlacementMarker: reconcilerPanicsTotal.WithLabelValues("check_placement_marker"),
	PlacementCleanup:     reconcilerPanicsTotal.WithLabelValues("placement_cleanup"),
}

// ReconcilerLostLeaseCounters is fred_provisioner_reconciler_lost_leases_total
// by outcome.
type ReconcilerLostLeaseCounters struct {
	Closed, Rejected, Deferred, Error prometheus.Counter
}

// ReconcilerLostLeases counts reconciliation of leases lost with a retired
// backend's storage.
var ReconcilerLostLeases = ReconcilerLostLeaseCounters{
	Closed:   reconcilerLostLeasesTotal.WithLabelValues("closed"),
	Rejected: reconcilerLostLeasesTotal.WithLabelValues("rejected"),
	Deferred: reconcilerLostLeasesTotal.WithLabelValues("deferred"),
	Error:    reconcilerLostLeasesTotal.WithLabelValues(OutcomeError),
}

// ReconcilerTerminalVerdictCounters is fred_reconciler_terminal_verdicts_total
// by the backend's consecutive-failure verdict (ENG-799).
type ReconcilerTerminalVerdictCounters struct {
	Absent, Unknown, Retry, Exhausted prometheus.Counter
}

// ReconcilerTerminalVerdicts counts terminal-budget verdicts per Failed lease
// per sweep.
var ReconcilerTerminalVerdicts = ReconcilerTerminalVerdictCounters{
	Absent:    reconcilerTerminalVerdictsTotal.WithLabelValues("absent"),
	Unknown:   reconcilerTerminalVerdictsTotal.WithLabelValues("unknown"),
	Retry:     reconcilerTerminalVerdictsTotal.WithLabelValues("retry"),
	Exhausted: reconcilerTerminalVerdictsTotal.WithLabelValues("exhausted"),
}

// CleanupSkipReasonCounters is fred_reconciler_cleanup_skips_total for one
// pass, by reason. See reconcilerCleanupSkipsTotal for what each reason means.
type CleanupSkipReasonCounters struct {
	ChainLive, ChainUnknown, ChainUnknownState, ChainError prometheus.Counter
	BackendSilent, AttemptPending                          prometheus.Counter
}

// ForReason returns the pass's series of a CleanupSkip* reason. It only looks
// up series bound at initialization, so an unknown reason has none.
func (counters CleanupSkipReasonCounters) ForReason(reason string) (prometheus.Counter, bool) {
	switch reason {
	case CleanupSkipChainLive:
		return counters.ChainLive, true
	case CleanupSkipChainUnknown:
		return counters.ChainUnknown, true
	case CleanupSkipChainUnknownState:
		return counters.ChainUnknownState, true
	case CleanupSkipChainError:
		return counters.ChainError, true
	case CleanupSkipBackendSilent:
		return counters.BackendSilent, true
	case CleanupSkipAttemptPending:
		return counters.AttemptPending, true
	default:
		return nil, false
	}
}

// CleanupSkipCounters is fred_reconciler_cleanup_skips_total by pass.
type CleanupSkipCounters struct {
	Orphan, Payload, Placement CleanupSkipReasonCounters
}

// ReconcilerCleanupSkips counts destructive cleanup the reconciler declined
// for lack of positive evidence (ENG-654).
var ReconcilerCleanupSkips = CleanupSkipCounters{
	Orphan:    bindCleanupSkipReasons(CleanupPassOrphan),
	Payload:   bindCleanupSkipReasons(CleanupPassPayload),
	Placement: bindCleanupSkipReasons(CleanupPassPlacement),
}

func bindCleanupSkipReasons(pass string) CleanupSkipReasonCounters {
	return CleanupSkipReasonCounters{
		ChainLive:         reconcilerCleanupSkipsTotal.WithLabelValues(pass, CleanupSkipChainLive),
		ChainUnknown:      reconcilerCleanupSkipsTotal.WithLabelValues(pass, CleanupSkipChainUnknown),
		ChainUnknownState: reconcilerCleanupSkipsTotal.WithLabelValues(pass, CleanupSkipChainUnknownState),
		ChainError:        reconcilerCleanupSkipsTotal.WithLabelValues(pass, CleanupSkipChainError),
		BackendSilent:     reconcilerCleanupSkipsTotal.WithLabelValues(pass, CleanupSkipBackendSilent),
		AttemptPending:    reconcilerCleanupSkipsTotal.WithLabelValues(pass, CleanupSkipAttemptPending),
	}
}

// BackendInvocationPanicCounters is
// fred_provisioner_backend_invocation_panics_total by operation.
type BackendInvocationPanicCounters struct {
	Provision, Restore, Deprovision prometheus.Counter
	Restart, Update                 prometheus.Counter
	ReconcileCustomDomain           prometheus.Counter
}

// BackendInvocationPanics counts panics recovered at the backend execution
// boundary.
var BackendInvocationPanics = BackendInvocationPanicCounters{
	Provision:             backendInvocationPanicsTotal.WithLabelValues(OperationProvision),
	Restore:               backendInvocationPanicsTotal.WithLabelValues(OperationRestore),
	Deprovision:           backendInvocationPanicsTotal.WithLabelValues(OperationDeprovision),
	Restart:               backendInvocationPanicsTotal.WithLabelValues(OperationRestart),
	Update:                backendInvocationPanicsTotal.WithLabelValues(OperationUpdate),
	ReconcileCustomDomain: backendInvocationPanicsTotal.WithLabelValues(OperationReconcileCustomDomain),
}

// LifecycleEventSinkPanicCounters is
// fred_provisioner_lifecycle_event_sink_panics_total by event.
type LifecycleEventSinkPanicCounters struct {
	ProvisionStarting, RestoreRestarting, RestoreRefused, Callback prometheus.Counter
}

// LifecycleEventSinkPanics counts panics recovered from best-effort lifecycle
// event sinks.
var LifecycleEventSinkPanics = LifecycleEventSinkPanicCounters{
	ProvisionStarting: lifecycleEventSinkPanicsTotal.WithLabelValues(LifecycleEventProvisionStarting),
	RestoreRestarting: lifecycleEventSinkPanicsTotal.WithLabelValues(LifecycleEventRestoreRestarting),
	RestoreRefused:    lifecycleEventSinkPanicsTotal.WithLabelValues(LifecycleEventRestoreRefused),
	Callback:          lifecycleEventSinkPanicsTotal.WithLabelValues(LifecycleEventCallback),
}

// CallbackStatusCounters splits a callback counter by the callback's status.
type CallbackStatusCounters struct {
	Success, Failed, Deprovisioned prometheus.Counter
}

// LifecycleCallbackVerdictCounters splits a lifecycle callback outcome by its
// authorization verdict.
type LifecycleCallbackVerdictCounters struct {
	Authorized, Legacy, TeardownOnly, Retired, Invalid CallbackStatusCounters
	Missing, Stale, Unusable, Unknown                  CallbackStatusCounters
}

// LifecycleCallbackOutcomeCounters is
// fred_provisioner_lifecycle_callback_outcomes_total by outcome, verdict and
// status.
type LifecycleCallbackOutcomeCounters struct {
	Applied, Dropped, Retryable LifecycleCallbackVerdictCounters
}

// LifecycleCallbackOutcomes classifies every authenticated lifecycle callback
// exactly once.
var LifecycleCallbackOutcomes = LifecycleCallbackOutcomeCounters{
	Applied:   bindLifecycleCallbackVerdicts(LifecycleCallbackOutcomeApplied),
	Dropped:   bindLifecycleCallbackVerdicts(LifecycleCallbackOutcomeDropped),
	Retryable: bindLifecycleCallbackVerdicts(LifecycleCallbackOutcomeRetryable),
}

func bindLifecycleCallbackVerdicts(outcome string) LifecycleCallbackVerdictCounters {
	bind := func(verdict string) CallbackStatusCounters {
		return bindCallbackStatuses(lifecycleCallbackOutcomesTotal.MustCurryWith(prometheus.Labels{
			"outcome": outcome, "verdict": verdict,
		}))
	}
	return LifecycleCallbackVerdictCounters{
		Authorized:   bind(LifecycleCallbackVerdictAuthorized),
		Legacy:       bind(LifecycleCallbackVerdictLegacy),
		TeardownOnly: bind(LifecycleCallbackVerdictTeardownOnly),
		Retired:      bind(LifecycleCallbackVerdictRetired),
		Invalid:      bind(LifecycleCallbackVerdictInvalid),
		Missing:      bind(LifecycleCallbackVerdictMissing),
		Stale:        bind(LifecycleCallbackVerdictStale),
		Unusable:     bind(LifecycleCallbackVerdictUnusable),
		Unknown:      bind(LifecycleCallbackVerdictUnknown),
	}
}

// bindCallbackStatuses binds the one remaining status label of a curried vec.
func bindCallbackStatuses(vec *prometheus.CounterVec) CallbackStatusCounters {
	return CallbackStatusCounters{ //exhaustruct:enforce
		Success:       vec.WithLabelValues(callbackStatusSuccess),
		Failed:        vec.WithLabelValues(callbackStatusFailed),
		Deprovisioned: vec.WithLabelValues(callbackStatusDeprovisioned),
	}
}

// The callback statuses of the backend wire contract (backend.CallbackStatus),
// which this package cannot import.
const (
	callbackStatusSuccess       = "success"
	callbackStatusFailed        = "failed"
	callbackStatusDeprovisioned = "deprovisioned"
)

// Callbacks whose reporting backend is empty or not configured are counted
// under these two labels instead of the claimed name.
var (
	NonInFlightCallbacksUnknownBackend = bindCallbackStatuses(nonInFlightCallbacksTotal.MustCurryWith(prometheus.Labels{"backend": "unknown"}))
	NonInFlightCallbacksInvalidBackend = bindCallbackStatuses(nonInFlightCallbacksTotal.MustCurryWith(prometheus.Labels{"backend": "invalid"}))
)

// DeferredCloseOutcomeCounters is fred_provisioner_deferred_closes_total for
// one deferral reason, by outcome.
type DeferredCloseOutcomeCounters struct {
	Parked, Unavailable, Coalesced, Full, Queued prometheus.Counter
	Retry, Dispatched, Failed, Overdue, Stopped  prometheus.Counter
}

// DeferredCloseCounters is fred_provisioner_deferred_closes_total by the
// deprovision deferral reason (placement.DeprovisionDeferralReason).
type DeferredCloseCounters struct {
	InventoryPending, LifecycleBusy, BackendUnavailable, BackendFenced DeferredCloseOutcomeCounters
}

// DeferredCloses counts close retry scheduling outcomes.
var DeferredCloses = DeferredCloseCounters{
	InventoryPending:   bindDeferredCloseOutcomes("inventory_pending"),
	LifecycleBusy:      bindDeferredCloseOutcomes("lifecycle_busy"),
	BackendUnavailable: bindDeferredCloseOutcomes("backend_unavailable"),
	BackendFenced:      bindDeferredCloseOutcomes("backend_fenced"),
}

func bindDeferredCloseOutcomes(reason string) DeferredCloseOutcomeCounters {
	bind := func(outcome string) prometheus.Counter {
		return deferredClosesTotal.WithLabelValues(outcome, reason)
	}
	return DeferredCloseOutcomeCounters{
		Parked: bind("parked"), Unavailable: bind("unavailable"), Coalesced: bind("coalesced"),
		Full: bind("full"), Queued: bind("queued"), Retry: bind("retry"),
		Dispatched: bind("dispatched"), Failed: bind("failed"), Overdue: bind("overdue"),
		Stopped: bind("stopped"),
	}
}

// MessageOutcomeCounters splits a message counter by processing outcome.
type MessageOutcomeCounters struct {
	Success, Error prometheus.Counter
}

// MessageTopicCounters holds one value per internal message topic a handler
// consumes.
type MessageTopicCounters[T any] struct {
	LeaseCreated, LeaseClosed, LeaseExpired T
	BackendCallback, PayloadReceived        T
}

func bindMessageTopics[T any](bind func(topic string) T) MessageTopicCounters[T] {
	return MessageTopicCounters[T]{
		LeaseCreated:    bind("events.lease.created"),
		LeaseClosed:     bind("events.lease.closed"),
		LeaseExpired:    bind("events.lease.expired"),
		BackendCallback: bind("events.backend.callback"),
		PayloadReceived: bind("events.payload.received"),
	}
}

// WatermillMessages counts internal messages processed, by topic and outcome.
var WatermillMessages = bindMessageTopics(func(topic string) MessageOutcomeCounters {
	return MessageOutcomeCounters{
		Success: watermillMessagesTotal.WithLabelValues(topic, OutcomeSuccess),
		Error:   watermillMessagesTotal.WithLabelValues(topic, OutcomeError),
	}
})

// MalformedMessages counts internal messages that could not be decoded, by
// topic.
var MalformedMessages = bindMessageTopics(func(topic string) prometheus.Counter {
	return malformedMessagesTotal.WithLabelValues(topic)
})

// LeaseEventCounters holds one counter per chain lease event type
// (chain.LeaseEventType).
type LeaseEventCounters struct {
	Created, Acknowledged, Rejected, Closed, Expired, AutoClosed prometheus.Counter
}

// EventsDropped counts lease events dropped on full subscriber channels.
var EventsDropped = LeaseEventCounters{
	Created:      eventsDroppedTotal.WithLabelValues("lease_created"),
	Acknowledged: eventsDroppedTotal.WithLabelValues("lease_acknowledged"),
	Rejected:     eventsDroppedTotal.WithLabelValues("lease_rejected"),
	Closed:       eventsDroppedTotal.WithLabelValues("lease_closed"),
	Expired:      eventsDroppedTotal.WithLabelValues("lease_expired"),
	AutoClosed:   eventsDroppedTotal.WithLabelValues("lease_auto_closed"),
}

// ChainTxCounters is fred_chain_transactions_total by type and outcome.
type ChainTxCounters struct {
	Acknowledge, Reject, Withdraw, Close MessageOutcomeCounters
}

// ChainTx counts chain transactions.
var ChainTx = ChainTxCounters{
	Acknowledge: bindChainTx("acknowledge"),
	Reject:      bindChainTx("reject"),
	Withdraw:    bindChainTx("withdraw"),
	Close:       bindChainTx("close"),
}

func bindChainTx(txType string) MessageOutcomeCounters {
	return MessageOutcomeCounters{
		Success: chainTxTotal.WithLabelValues(txType, OutcomeSuccess),
		Error:   chainTxTotal.WithLabelValues(txType, OutcomeError),
	}
}

// SignerOOGRetryCounters is fred_chain_signer_oog_retries_total by result.
type SignerOOGRetryCounters struct {
	Retried, Exhausted prometheus.Counter
}

// SignerOOGRetries counts out-of-gas retry decisions.
var SignerOOGRetries = SignerOOGRetryCounters{
	Retried:   signerOOGRetriesTotal.WithLabelValues("retried"),
	Exhausted: signerOOGRetriesTotal.WithLabelValues("exhausted"),
}

// GasSimulationCounters is fred_chain_gas_simulation_total by result.
type GasSimulationCounters struct {
	Simulated, Fallback, Refused prometheus.Counter
}

// GasSimulation counts per-tx gas-simulation outcomes.
var GasSimulation = GasSimulationCounters{
	Simulated: gasSimulationTotal.WithLabelValues("simulated"),
	Fallback:  gasSimulationTotal.WithLabelValues("fallback"),
	Refused:   gasSimulationTotal.WithLabelValues("refused"),
}

// PayloadUploadCounters is fred_payload_uploads_total by outcome.
type PayloadUploadCounters struct {
	Success, InvalidAuth, BillingOnly, HashMismatch prometheus.Counter
	RestoreInProgress, Conflict, Error              prometheus.Counter
}

// PayloadUploads counts payload uploads.
var PayloadUploads = PayloadUploadCounters{
	Success:           payloadUploadsTotal.WithLabelValues(OutcomeSuccess),
	InvalidAuth:       payloadUploadsTotal.WithLabelValues("invalid_auth"),
	BillingOnly:       payloadUploadsTotal.WithLabelValues("billing_only"),
	HashMismatch:      payloadUploadsTotal.WithLabelValues("hash_mismatch"),
	RestoreInProgress: payloadUploadsTotal.WithLabelValues("restore_in_progress"),
	Conflict:          payloadUploadsTotal.WithLabelValues("conflict"),
	Error:             payloadUploadsTotal.WithLabelValues(OutcomeError),
}

// RateLimitRejectionCounters is fred_api_rate_limit_rejections_total by
// limiter.
type RateLimitRejectionCounters struct {
	Global, Tenant, CallbackIngress, CallbackStorage prometheus.Counter
}

// RateLimitRejections counts requests refused by rate limiting.
var RateLimitRejections = RateLimitRejectionCounters{
	Global:          rateLimitRejectionsTotal.WithLabelValues("global"),
	Tenant:          rateLimitRejectionsTotal.WithLabelValues("tenant"),
	CallbackIngress: rateLimitRejectionsTotal.WithLabelValues("callback_ingress"),
	CallbackStorage: rateLimitRejectionsTotal.WithLabelValues("callback_storage"),
}

// CallbackAuthFailureCounters is fred_api_callback_auth_failures_total by
// reason.
type CallbackAuthFailureCounters struct {
	Missing, Format, Expired, Future, Mismatch, UnknownStorage, Fenced prometheus.Counter
}

// CallbackAuthFailures counts callbacks refused at signature verification.
var CallbackAuthFailures = CallbackAuthFailureCounters{
	Missing:        apiCallbackAuthFailuresTotal.WithLabelValues(CallbackAuthFailureMissing),
	Format:         apiCallbackAuthFailuresTotal.WithLabelValues(CallbackAuthFailureFormat),
	Expired:        apiCallbackAuthFailuresTotal.WithLabelValues(CallbackAuthFailureExpired),
	Future:         apiCallbackAuthFailuresTotal.WithLabelValues(CallbackAuthFailureFuture),
	Mismatch:       apiCallbackAuthFailuresTotal.WithLabelValues(CallbackAuthFailureMismatch),
	UnknownStorage: apiCallbackAuthFailuresTotal.WithLabelValues(CallbackAuthFailureUnknownStorage),
	Fenced:         apiCallbackAuthFailuresTotal.WithLabelValues(CallbackAuthFailureFenced),
}

// MaintenanceAdmissionRefusalCounters is
// fred_maintenance_admission_refusals_total by reason.
type MaintenanceAdmissionRefusalCounters struct {
	Count, Bytes, ReservedCount, ReservedBytes prometheus.Counter
}

// MaintenanceAdmissionRefusals counts fresh maintenance commands refused by
// the global pending budget or its newcomer reservation.
var MaintenanceAdmissionRefusals = MaintenanceAdmissionRefusalCounters{
	Count:         maintenanceAdmissionRefusalsTotal.WithLabelValues("count"),
	Bytes:         maintenanceAdmissionRefusalsTotal.WithLabelValues("bytes"),
	ReservedCount: maintenanceAdmissionRefusalsTotal.WithLabelValues("reserved_count"),
	ReservedBytes: maintenanceAdmissionRefusalsTotal.WithLabelValues("reserved_bytes"),
}

// SignerGrantCheckCounters is fred_signer_grant_check_total by outcome.
type SignerGrantCheckCounters struct {
	Success, Error prometheus.Counter
}

// BindSignerGrantCheck creates both grant-check series. Call it only when the
// signer pool has sub-signers: a provider without them never checks grants and
// must export no grant-check series.
func BindSignerGrantCheck() SignerGrantCheckCounters {
	return SignerGrantCheckCounters{
		Success: signerGrantCheckTotal.WithLabelValues(OutcomeSuccess),
		Error:   signerGrantCheckTotal.WithLabelValues(OutcomeError),
	}
}

// PlacementSnapshotPruneFailureCounters is
// fred_placement_snapshot_prune_failures_total by reason.
type PlacementSnapshotPruneFailureCounters struct {
	List, Inspect, NotOwned, LiveDatabase, Remove, Sync prometheus.Counter
}

// PlacementSnapshotCounters is fred_placement_snapshots_total by outcome, and
// its prune failures.
type PlacementSnapshotCounters struct {
	Success, Error, InsufficientSpace prometheus.Counter
	PruneFailures                     PlacementSnapshotPruneFailureCounters
}

// BindPlacementSnapshots creates every snapshot series. Call it only when
// snapshots are configured: increase(...{outcome="success"}[2h]) == 0 is a
// heartbeat, so a provider that never enabled snapshots must export none.
func BindPlacementSnapshots() PlacementSnapshotCounters {
	prune := func(reason string) prometheus.Counter {
		return placementSnapshotPruneFailuresTotal.WithLabelValues(reason)
	}
	return PlacementSnapshotCounters{
		Success:           placementSnapshotsTotal.WithLabelValues(OutcomeSuccess),
		Error:             placementSnapshotsTotal.WithLabelValues(OutcomeError),
		InsufficientSpace: placementSnapshotsTotal.WithLabelValues("insufficient_space"),
		PruneFailures: PlacementSnapshotPruneFailureCounters{
			List: prune("list"), Inspect: prune("inspect"), NotOwned: prune("not_owned"),
			LiveDatabase: prune("live_database"), Remove: prune("remove"), Sync: prune("sync"),
		},
	}
}

// AckBatchLaneCounters are the ack batcher's counters for one lane.
type AckBatchLaneCounters struct {
	FeeGasErrors, IndividualFallbacks, Restarts prometheus.Counter
}

// BindAckBatchLane creates one lane's ack-batch series. The batcher binds each
// lane it starts.
func BindAckBatchLane(lane string) AckBatchLaneCounters {
	return AckBatchLaneCounters{
		FeeGasErrors:        ackBatchFeeGasErrorsTotal.WithLabelValues(lane),
		IndividualFallbacks: ackBatchIndividualFallbacksTotal.WithLabelValues(lane),
		Restarts:            ackBatcherLaneRestartsTotal.WithLabelValues(lane),
	}
}

// BindSignerBalanceQueryFailure creates the balance-query failure series of
// one signer address and denom. The balance collector binds every address it
// will query.
func BindSignerBalanceQueryFailure(role, address, denom string) prometheus.Counter {
	return signerBalanceQueryFailures.WithLabelValues(role, address, denom)
}

// ProvisioningOutcomeCounters is fred_provisioner_provisioning_total for one
// backend and operation, by outcome.
type ProvisioningOutcomeCounters struct {
	Success, Failed, Error prometheus.Counter
}

// ProvisioningCounters is fred_provisioner_provisioning_total for one backend,
// by operation.
type ProvisioningCounters struct {
	Provision, Restore ProvisioningOutcomeCounters
}

// BackendFetchCounters is fred_reconciler_backend_fetch_total for one backend,
// by outcome.
type BackendFetchCounters struct {
	OK, Error, CircuitOpen, Fenced prometheus.Counter
}

// BackendInventoryCounters is fred_reconciler_backend_inventory_total for one
// backend, by outcome.
type BackendInventoryCounters struct {
	Authoritative, Partial, Untrusted, ProvisionsOnly prometheus.Counter
	RetentionsOnly, Unanswered, Fenced                prometheus.Counter
}

// CapacityVerdictCounters is fred_backend_insufficient_resources_total for one
// backend, by verdict.
type CapacityVerdictCounters struct {
	CodedRefusal, Ambiguous prometheus.Counter
}

// CallbackKeySlotCounters is fred_api_callback_signature_key_total for one
// backend, by key slot.
type CallbackKeySlotCounters struct {
	Current, Previous prometheus.Counter
}

// BackendSeries is every backend-labeled counter for one configured backend.
type BackendSeries struct {
	Provisioning          ProvisioningCounters
	ReconcileFetch        BackendFetchCounters
	Inventory             BackendInventoryCounters
	InsufficientResources CapacityVerdictCounters
	CallbackSignatureKey  CallbackKeySlotCounters
	NonInFlightCallbacks  CallbackStatusCounters
}

var backendSeries = struct {
	sync.RWMutex
	byName map[string]BackendSeries
}{byName: make(map[string]BackendSeries)}

// unboundBackend is what ForBackend returns for a name nothing bound: every
// counter is fred_backend_unbound_events_total, so a lookup never creates a
// series and never attributes an event to a backend label a real backend
// could have.
var unboundBackend = backendSeriesOf(unboundBackendEventsTotal)

// backendSeriesOf is a BackendSeries whose every counter is counter.
func backendSeriesOf(counter prometheus.Counter) BackendSeries {
	outcomes := ProvisioningOutcomeCounters{ //exhaustruct:enforce
		Success: counter, Failed: counter, Error: counter,
	}
	return BackendSeries{ //exhaustruct:enforce
		Provisioning: ProvisioningCounters{ //exhaustruct:enforce
			Provision: outcomes, Restore: outcomes,
		},
		ReconcileFetch: BackendFetchCounters{ //exhaustruct:enforce
			OK: counter, Error: counter, CircuitOpen: counter, Fenced: counter,
		},
		Inventory: BackendInventoryCounters{ //exhaustruct:enforce
			Authoritative: counter, Partial: counter, Untrusted: counter, ProvisionsOnly: counter,
			RetentionsOnly: counter, Unanswered: counter, Fenced: counter,
		},
		InsufficientResources: CapacityVerdictCounters{ //exhaustruct:enforce
			CodedRefusal: counter, Ambiguous: counter,
		},
		CallbackSignatureKey: CallbackKeySlotCounters{ //exhaustruct:enforce
			Current: counter, Previous: counter,
		},
		NonInFlightCallbacks: CallbackStatusCounters{ //exhaustruct:enforce
			Success: counter, Failed: counter, Deprovisioned: counter,
		},
	}
}

// BindBackends creates every backend-labeled series of each named backend.
// providerd binds a backend when it builds that backend's client, before
// anything can record an event for it. Call it only where a component is
// constructed (internal/testutil's counter series guard).
func BindBackends(names ...string) {
	backendSeries.Lock()
	defer backendSeries.Unlock()
	for _, name := range names {
		if _, ok := backendSeries.byName[name]; !ok {
			backendSeries.byName[name] = bindBackend(name)
		}
	}
}

// ForBackend returns the series BindBackends created for a backend. It never
// creates one: an event naming a backend nothing bound counts in
// fred_backend_unbound_events_total, which exists from startup.
func ForBackend(name string) BackendSeries {
	backendSeries.RLock()
	defer backendSeries.RUnlock()
	if series, ok := backendSeries.byName[name]; ok {
		return series
	}
	return unboundBackend
}

func bindBackend(name string) BackendSeries {
	provisioning := func(operation string) ProvisioningOutcomeCounters {
		return ProvisioningOutcomeCounters{ //exhaustruct:enforce
			Success: provisioningTotal.WithLabelValues(OutcomeSuccess, name, operation),
			Failed:  provisioningTotal.WithLabelValues(OutcomeFailed, name, operation),
			Error:   provisioningTotal.WithLabelValues(OutcomeError, name, operation),
		}
	}
	fetch := func(outcome string) prometheus.Counter {
		return reconcilerBackendFetchTotal.WithLabelValues(name, outcome)
	}
	inventory := func(outcome string) prometheus.Counter {
		return reconcilerBackendInventoryTotal.WithLabelValues(name, outcome)
	}
	return BackendSeries{ //exhaustruct:enforce
		Provisioning: ProvisioningCounters{ //exhaustruct:enforce
			Provision: provisioning(OperationProvision),
			Restore:   provisioning(OperationRestore),
		},
		ReconcileFetch: BackendFetchCounters{ //exhaustruct:enforce
			OK: fetch(FetchOutcomeOK), Error: fetch(FetchOutcomeError),
			CircuitOpen: fetch(FetchOutcomeCircuitOpen), Fenced: fetch(FetchOutcomeFenced),
		},
		Inventory: BackendInventoryCounters{ //exhaustruct:enforce
			Authoritative:  inventory(InventoryOutcomeAuthoritative),
			Partial:        inventory(InventoryOutcomePartial),
			Untrusted:      inventory(InventoryOutcomeUntrusted),
			ProvisionsOnly: inventory(InventoryOutcomeProvisionsOnly),
			RetentionsOnly: inventory(InventoryOutcomeRetentionsOnly),
			Unanswered:     inventory(InventoryOutcomeUnanswered),
			Fenced:         inventory(InventoryOutcomeFenced),
		},
		InsufficientResources: CapacityVerdictCounters{ //exhaustruct:enforce
			CodedRefusal: backendInsufficientResourcesTotal.WithLabelValues(name, CapacityVerdictCodedRefusal),
			Ambiguous:    backendInsufficientResourcesTotal.WithLabelValues(name, CapacityVerdictAmbiguous),
		},
		CallbackSignatureKey: CallbackKeySlotCounters{ //exhaustruct:enforce
			Current:  apiCallbackSignatureKeyTotal.WithLabelValues(name, CallbackKeySlotCurrent),
			Previous: apiCallbackSignatureKeyTotal.WithLabelValues(name, CallbackKeySlotPrevious),
		},
		NonInFlightCallbacks: bindCallbackStatuses(nonInFlightCallbacksTotal.MustCurryWith(prometheus.Labels{"backend": name})),
	}
}

// ObserveAPIRequest records one API request in fred_api_requests_total and
// fred_api_request_duration_seconds. It takes the request rather than label
// strings, and both families are unexported, so every label is derived here
// and nothing a client sends can become a label value of its own (ENG-2150):
//
//   - method is one of apiMethodLabel's fixed values;
//   - path is the route the request matched, or "unmatched" (apiPathLabel);
//   - status is the HTTP status code the handler wrote. Fred chooses it, but
//     the set is open, so each method, path and status series is created by
//     its first request, and nothing alerts on these families with
//     increase() or rate().
func ObserveAPIRequest(r *http.Request, status int, elapsed time.Duration) {
	method, path, code := apiMethodLabel(r.Method), apiPathLabel(r), strconv.Itoa(status)
	apiRequestDuration.WithLabelValues(method, path, code).Observe(elapsed.Seconds())
	apiRequestsTotal.WithLabelValues(method, path, code).Inc()
}

// apiMethodOther is the method label of a request whose method net/http does
// not name.
const apiMethodOther = "other"

// apiMethodLabel maps a request method to the method label: the nine methods
// net/http names, or apiMethodOther. Go's server accepts any token as a
// method and ServeMux answers an unknown one 405, so labeling the raw method
// let an unauthenticated client create a series per invented method, which is
// never evicted. client_golang's own handler instrumentation fixed the same
// flaw by mapping methods onto a fixed set (CVE-2022-21698).
//
// Methods are case-sensitive (RFC 9110, section 9.1), as ServeMux's matching
// is, so "get" is apiMethodOther. Each case returns the constant rather than
// its argument: the HTTP/1 server slices the method out of the request line,
// so keeping the argument as a label would keep that whole line in memory.
func apiMethodLabel(method string) string {
	switch method {
	case http.MethodGet:
		return http.MethodGet
	case http.MethodHead:
		return http.MethodHead
	case http.MethodPost:
		return http.MethodPost
	case http.MethodPut:
		return http.MethodPut
	case http.MethodPatch:
		return http.MethodPatch
	case http.MethodDelete:
		return http.MethodDelete
	case http.MethodConnect:
		return http.MethodConnect
	case http.MethodOptions:
		return http.MethodOptions
	case http.MethodTrace:
		return http.MethodTrace
	default:
		return apiMethodOther
	}
}

// apiPathUnmatched is the path label of a request that matched no route.
const apiPathUnmatched = "unmatched"

// apiPathLabel maps a request to the path label: the ServeMux pattern it
// matched, without the pattern's method and host (for example
// "/v1/leases/{lease_uuid}/status"), or apiPathUnmatched. ServeMux sets
// r.Pattern to one of the patterns registered at startup, so the label takes
// at most one value per route plus one. Labeling r.URL.Path instead let an
// unauthenticated path scan create a series per path (ENG-448, F28).
func apiPathLabel(r *http.Request) string {
	if r.Pattern == "" {
		return apiPathUnmatched
	}
	// A pattern is "[METHOD ][HOST]/path"; the path starts at the first '/'.
	if i := strings.IndexByte(r.Pattern, '/'); i >= 0 {
		return r.Pattern[i:]
	}
	return r.Pattern
}

// BackendRequestCounters is fred_backend_requests_total for one backend and
// operation, by status, and fred_backend_malformed_error_body_total for the
// same pair.
type BackendRequestCounters struct {
	Success, Fenced, Pending, Error prometheus.Counter
	MalformedErrorBody              prometheus.Counter
}

// BindBackendRequest creates the request series of one backend operation. The
// backend client cannot import this package, so providerd hands it this
// binder and the client binds every operation it can send when it is built.
func BindBackendRequest(backend, operation string) BackendRequestCounters {
	status := func(status string) prometheus.Counter {
		return backendRequestsTotal.WithLabelValues(backend, operation, status)
	}
	return BackendRequestCounters{
		Success: status(OutcomeSuccess), Fenced: status("fenced"),
		Pending: status("pending"), Error: status(OutcomeError),
		MalformedErrorBody: backendMalformedErrorBodyTotal.WithLabelValues(backend, operation),
	}
}
