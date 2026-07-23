// Package metrics provides Prometheus metrics for fred observability.
//
// All metrics use the `fred` namespace and are registered via promauto,
// which uses Prometheus's default registerer. Importing this package is
// enough to make the metrics available at any /metrics endpoint that
// serves the default gatherer (e.g. promhttp.Handler), which is how
// providerd wires its endpoint.
//
// The docker backend defines its own set of metrics under
// `fred_docker_backend_*` in its package-local metrics file.
//
// For a categorized reference of every metric and its labels, see
// ARCHITECTURE.md.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	namespace = "fred"
)

// Provisioning metrics
var (
	// InFlightProvisions tracks the number of provisions currently in progress.
	InFlightProvisions = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "provisioner",
		Name:      "in_flight_provisions",
		Help:      "Number of provisions currently in progress",
	})

	// ProvisioningTotal tracks the total number of provisioning operations by
	// outcome and operation (provision|restore). A restore is a kind of
	// provisioning operation; the operation label keeps it separable from fresh
	// provisions (ENG-357/ENG-358) while sum()-ing across it stays meaningful.
	ProvisioningTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "provisioner",
		Name:      "provisioning_total",
		Help:      "Total number of provisioning operations by outcome and operation (provision|restore)",
	}, []string{"outcome", "backend", "operation"})

	// ProvisioningDuration tracks the duration of provisioning operations,
	// labeled by operation (provision|restore) so restore latency stays
	// separable from fresh-provision latency (ENG-358).
	ProvisioningDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "provisioner",
		Name:      "provisioning_duration_seconds",
		Help:      "Duration of provisioning operations in seconds by operation (provision|restore)",
		Buckets:   prometheus.ExponentialBuckets(0.1, 2, 12), // 0.1s to ~7min
	}, []string{"backend", "operation"})

	// AckBatchFeeGasErrorsTotal counts ack-batch failures classified as
	// insufficient-fee or out-of-gas that are surfaced to callers without
	// being individualized. Sustained non-zero values indicate that the
	// per-signer broadcast-retry loop is exhausting its gas budget (e.g.
	// maxGasLimit too tight) or that the configured fee is insufficient.
	AckBatchFeeGasErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "provisioner",
		Name:      "ack_batch_fee_gas_errors_total",
		Help:      "Total ack-batch failures due to insufficient fee or out-of-gas, surfaced without individualization",
	}, []string{"lane"})

	// AckBatchIndividualFallbacksTotal counts ack-batch failures that fall
	// through to per-lease retries (i.e. non-fee/gas errors).
	AckBatchIndividualFallbacksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "provisioner",
		Name:      "ack_batch_individual_fallbacks_total",
		Help:      "Total ack-batch failures that fell back to individual per-lease retries",
	}, []string{"lane"})

	// AckBatcherLaneRestartsTotal counts ack-batcher lanes respawned after a
	// panic (ENG-589). A lane whose flush panics is recovered and restarted so
	// acknowledgment does not stall permanently (at the default single lane a
	// dead lane would disable ALL acking and cause the timeout checker to
	// wrongly reject healthy leases). Sustained non-zero values mean a lane is
	// crash-looping — pair with goroutine_panics_total{component="ack_batcher"}
	// and alert.
	AckBatcherLaneRestartsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "provisioner",
		Name:      "ack_batcher_lane_restarts_total",
		Help:      "Total ack-batcher lane respawns after a recovered panic",
	}, []string{"lane"})

	// ReconcilerInflightSkipsTotal counts reconciler decisions to skip
	// acknowledging a ready lease because the main flow is already processing
	// it (IsInFlight == true). A sudden spike can indicate stuck in-flight
	// leases; cross-check against timeout-checker rejection metrics.
	ReconcilerInflightSkipsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "provisioner",
		Name:      "reconciler_inflight_skips_total",
		Help:      "Total ready leases the reconciler skipped because the main flow owns them",
	})

	// ReconcilerPanicsTotal counts panics recovered inside reconciler
	// per-unit goroutines (per-lease, per-orphan, per-backend-fetch). The
	// recover exists specifically to prevent one bad lease/orphan/backend
	// from crashing the fred process. Any non-zero value is a latent bug
	// to fix at its source — not business-as-usual. Label values:
	// "process_lease", "process_orphan", "fetch_provisions", "fetch_retentions".
	ReconcilerPanicsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "provisioner",
		Name:      "reconciler_panics_total",
		Help:      "Panics recovered in reconciler per-unit goroutines, by stage",
	}, []string{"stage"})

	// CleanupPanicsTotal counts panics recovered inside background
	// cleanup loops (token tracker, callback store, diagnostics store,
	// etc.) driven by util.StartCleanupLoop. Any non-zero value is a
	// latent bug in the cleanup function. Labeled by component.
	CleanupPanicsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "background",
		Name:      "cleanup_panics_total",
		Help:      "Panics recovered in background cleanup loops, by component",
	}, []string{"component"})

	// GoroutinePanicsTotal is the catch-all for long-lived background
	// goroutines that add their own recover() (payload store writer,
	// ack batcher lanes, etc.). Labeled by component for correlation.
	GoroutinePanicsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "background",
		Name:      "goroutine_panics_total",
		Help:      "Panics recovered in long-lived background goroutines, by component",
	}, []string{"component"})

	// SignerOOGRetriesTotal counts out-of-gas retry decisions at the
	// transaction broadcast layer. Label values: "retried" = gas was
	// increased and the tx will be retried; "exhausted" = already at the
	// cap, retry is futile and the error is surfaced to the caller.
	SignerOOGRetriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "chain",
		Name:      "signer_oog_retries_total",
		Help:      "Total out-of-gas retry decisions by outcome (retried, exhausted)",
	}, []string{"result"})

	// GasSimulationTotal counts per-tx gas-simulation outcomes.
	// result ∈ {simulated, fallback, refused}.
	GasSimulationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "chain",
		Name:      "gas_simulation_total",
		Help:      "Per-tx gas simulation outcomes (simulated, fallback, refused).",
	}, []string{"result"})

	// GasSimulated records the gas magnitude (simulated or fallback) chosen for
	// a broadcast, so over-pay magnitude is observable at the default unbounded
	// max_gas_limit. Label-free (cardinality discipline).
	GasSimulated = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "chain",
		Name:      "gas_simulated",
		Help:      "Gas magnitude (simulated or fallback) declared for a broadcast.",
		Buckets:   []float64{100_000, 200_000, 300_000, 500_000, 1_000_000, 2_000_000, 4_000_000, 8_000_000},
	})
)

// Reconciliation metrics
var (
	// ReconciliationTotal tracks the total number of reconciliation runs.
	ReconciliationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "reconciler",
		Name:      "runs_total",
		Help:      "Total number of reconciliation runs by outcome",
	}, []string{"outcome"})

	// ReconciliationDuration tracks the duration of reconciliation runs.
	ReconciliationDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "reconciler",
		Name:      "duration_seconds",
		Help:      "Duration of reconciliation runs in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.5, 2, 10), // 0.5s to ~8min
	})

	// ReconcilerLastSuccessTimestamp records the unix timestamp of the last successful reconciliation.
	ReconcilerLastSuccessTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "reconciler",
		Name:      "last_success_timestamp_seconds",
		Help:      "Unix timestamp of the last successful reconciliation run",
	})

	// ReconciliationActions tracks actions taken during reconciliation.
	ReconciliationActions = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "reconciler",
		Name:      "actions_total",
		Help:      "Total number of actions taken during reconciliation",
	}, []string{"action"}) // action: provisioned, acknowledged, deprovisioned, anomaly
)

// Payload metrics
var (
	// PayloadUploadsTotal tracks the total number of payload uploads.
	PayloadUploadsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "payload",
		Name:      "uploads_total",
		Help:      "Total number of payload uploads by outcome",
	}, []string{"outcome"}) // outcome: success, invalid_auth, hash_mismatch, conflict, error

	// PayloadStoredCount tracks the number of payloads currently stored.
	PayloadStoredCount = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "payload",
		Name:      "stored_count",
		Help:      "Number of payloads currently stored awaiting provisioning",
	})

	// PayloadSizeBytes tracks the size of uploaded payloads.
	PayloadSizeBytes = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "payload",
		Name:      "size_bytes",
		Help:      "Size of uploaded payloads in bytes",
		Buckets:   prometheus.ExponentialBuckets(1024, 2, 14), // 1KB to ~16MB
	})

	// LeasesAwaitingPayload tracks the number of leases currently waiting for payload upload.
	LeasesAwaitingPayload = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "payload",
		Name:      "leases_awaiting",
		Help:      "Number of leases currently waiting for payload upload",
	})
)

// Backend metrics
var (
	// BackendRequestDuration tracks backend request latency.
	BackendRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "backend",
		Name:      "request_duration_seconds",
		Help:      "Backend request duration in seconds",
		Buckets:   prometheus.DefBuckets,
	}, []string{"backend", "operation", "status"}) // operation: provision, deprovision, get_info, list_provisions

	// BackendRequestsTotal tracks total backend requests.
	BackendRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "backend",
		Name:      "requests_total",
		Help:      "Total number of backend requests",
	}, []string{"backend", "operation", "status"})

	// BackendInsufficientResourcesTotal tracks backend capacity rejections.
	BackendInsufficientResourcesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "backend",
		Name:      "insufficient_resources_total",
		Help:      "Total number of provisions rejected due to insufficient backend resources",
	}, []string{"backend"})

	// BackendHealthy tracks backend health status (1 = healthy, 0 = unhealthy).
	BackendHealthy = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "backend",
		Name:      "healthy",
		Help:      "Backend health status (1 = healthy, 0 = unhealthy)",
	}, []string{"backend"})

	// BackendCircuitBreakerState tracks circuit breaker state (0=closed, 1=half-open, 2=open).
	BackendCircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "backend",
		Name:      "circuit_breaker_state",
		Help:      "Circuit breaker state (0=closed, 1=half-open, 2=open)",
	}, []string{"backend"})

	// BackendAllocatedCPURatio tracks the allocated-CPU ratio fred observed for
	// each backend at provision-routing time (allocated/total). Distinct from the
	// backend's own resource_cpu_allocated_ratio: this is fred's view, including
	// staleness, recorded only on multi-candidate provision-routing decisions.
	BackendAllocatedCPURatio = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "backend",
		Name:      "allocated_cpu_ratio",
		Help:      "Allocated-CPU ratio observed by the router at provision time (allocated/total). Per-backend router-decision signal, event-sampled on multi-candidate routing; not intended for cross-backend aggregation (avg of ratios is not statistically valid) — use the backends' own /stats component gauges for fleet views.",
	}, []string{"backend"})

	// RoutingFallbackTotal counts provision-routing decisions that fell back to
	// round-robin because no SKU-matching backend exposed usable load stats.
	RoutingFallbackTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "backend",
		Name:      "routing_fallback_total",
		Help:      "Provision-routing decisions that fell back to round-robin (no usable backend load stats)",
	})
)

// Rate limit metrics
var (
	// RateLimitRejectionsTotal tracks requests rejected by rate limiting.
	RateLimitRejectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "api",
		Name:      "rate_limit_rejections_total",
		Help:      "Total number of requests rejected by rate limiting",
	}, []string{"limiter"}) // limiter: global, tenant
)

// API metrics
var (
	// APIRequestDuration tracks API request latency.
	APIRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "api",
		Name:      "request_duration_seconds",
		Help:      "API request duration in seconds",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "path", "status"})

	// APIRequestsTotal tracks total API requests.
	APIRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "api",
		Name:      "requests_total",
		Help:      "Total number of API requests",
	}, []string{"method", "path", "status"})
)

// Chain metrics
var (
	// ChainTxTotal tracks chain transactions by type and outcome.
	ChainTxTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "chain",
		Name:      "transactions_total",
		Help:      "Total number of chain transactions",
	}, []string{"type", "outcome"}) // type: acknowledge, reject, withdraw, close

	// ChainQueryDuration tracks chain query latency.
	ChainQueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "chain",
		Name:      "query_duration_seconds",
		Help:      "Chain query duration in seconds",
		Buckets:   prometheus.DefBuckets,
	}, []string{"query"})
)

// Withdraw metrics
var (
	// WithdrawIncompleteCyclesTotal counts provider-wide withdrawal cycles that
	// hit the per-cycle iteration bound (max_withdraw_iterations) while the
	// pagination cursor was still non-empty — i.e. the provider was not fully
	// drained in that cycle. This is not fund loss (lease close settles
	// everything) but defers active-lease collection to the next cycle. A
	// sustained non-zero rate means the iteration bound is too low for the
	// provider's active-lease count (ENG-475).
	WithdrawIncompleteCyclesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "withdraw",
		Name:      "incomplete_cycles_total",
		Help:      "Provider-wide withdrawal cycles that hit the iteration bound with the pagination cursor still non-empty (provider not fully drained).",
	})

	// WithdrawSkippedByGuardTotal counts scheduler wakes that skipped the paid
	// withdrawal because the withdraw-cadence guard had not elapsed since the
	// last full drain (ENG-524). Only increments when guard_active=1.
	WithdrawSkippedByGuardTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "withdraw",
		Name:      "skipped_by_guard_total",
		Help:      "Scheduler wakes that skipped the paid withdrawal because the withdraw-interval guard had not elapsed.",
	})

	// CreditCheckZeroDeferredTotal counts credit checks that read a tenant's
	// balance as empty but DEFERRED lease closure because the empty balance had
	// not yet persisted for credit_check_zero_grace_period (ENG-591). This is an
	// aggregate, label-free counter (no per-tenant attribution): occasional
	// increments are transient stale reads absorbed by the grace window, whereas a
	// sustained or rising aggregate rate points at chronic chain-node lag or a
	// mistuned grace period. Correlate with fred_chain_transactions_total{type=
	// "close"} to see how many deferrals ultimately still closed.
	CreditCheckZeroDeferredTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "withdraw",
		Name:      "credit_check_zero_deferred_total",
		Help:      "Credit checks that read an empty balance but deferred lease closure pending the zero-balance grace period; aggregate (no tenant label) (ENG-591).",
	})

	// WithdrawGuardActive is 1 when the withdraw-cadence guard is active
	// (credit_check_interval < withdraw_interval), else 0. Disambiguates a zero
	// skipped_by_guard_total (guard inert vs. active-but-never-skipped).
	WithdrawGuardActive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "withdraw",
		Name:      "guard_active",
		Help:      "1 when the withdraw-cadence guard is active (credit_check_interval < withdraw_interval), else 0.",
	})
)

// Watermill metrics
var (
	// WatermillMessagesTotal tracks Watermill message processing.
	WatermillMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "watermill",
		Name:      "messages_total",
		Help:      "Total number of Watermill messages processed",
	}, []string{"topic", "outcome"}) // outcome: success, error, dropped

	// PoisonedMessagesTotal tracks messages sent to the poison queue after all retries exhausted.
	PoisonedMessagesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "watermill",
		Name:      "poisoned_messages_total",
		Help:      "Total messages sent to poison queue after all retries exhausted.",
	})
)

// Event subscriber metrics
var (
	// EventsDroppedTotal tracks events dropped due to full subscriber channels.
	EventsDroppedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "events",
		Name:      "dropped_total",
		Help:      "Total number of events dropped due to full subscriber channels",
	}, []string{"event_type"})
)

// Message processing metrics
var (
	// MalformedMessagesTotal tracks malformed messages that couldn't be processed.
	MalformedMessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "messages",
		Name:      "malformed_total",
		Help:      "Total number of malformed messages that couldn't be parsed",
	}, []string{"topic"})

	// ReconciliationConflictsTotal tracks reconciliation conflicts (lease already in-flight).
	ReconciliationConflictsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "reconciler",
		Name:      "conflicts_total",
		Help:      "Total number of reconciliation conflicts (lease already being provisioned)",
	})
)

// Callback metrics
var (
	// CallbackTimeoutsTotal tracks provisions that timed out waiting for backend callback.
	CallbackTimeoutsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "provisioner",
		Name:      "callback_timeouts_total",
		Help:      "Total number of provisions that timed out waiting for backend callback",
	})

	// NonInFlightCallbacksTotal tracks callbacks received for leases not in the in-flight tracker,
	// labeled by the reporting backend and the callback status.
	NonInFlightCallbacksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "api",
		Name:      "non_in_flight_callbacks_total",
		Help:      "Callbacks received for leases not in the in-flight tracker (restart/update completions, late delivery, or intentional deprovision), labeled by backend and status",
	}, []string{"backend", "status"})
)

// Signer pool metrics
var (
	// SignerPoolSize tracks the total number of signers (1 = single, >1 = parallel).
	SignerPoolSize = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "signer",
		Name:      "pool_size",
		Help:      "Total number of signers in the pool (primary + sub-signers)",
	})

	// SignerPoolLaneCount tracks the number of active batcher lanes.
	SignerPoolLaneCount = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "signer",
		Name:      "pool_lane_count",
		Help:      "Number of active batcher lanes for parallel signing",
	})

	// SignerBalanceQueryFailures counts per-address bank balance query failures
	// observed by the SignerBalanceCollector during a /metrics scrape. The
	// matching `fred_signer_balance` gauge series is dropped for the failing
	// address on that scrape. Labeled by role ("provider" or "sub_signer"),
	// bech32 address, and bank denom queried (no index — the index is a
	// gauge-only label that would inflate counter cardinality without adding
	// signal; denom is included so an operator scraping a deployment with a
	// non-umfx fee denom can still correlate the counter to the gauge).
	SignerBalanceQueryFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "signer",
		Name:      "balance_query_failures_total",
		Help:      "Total per-address signer balance query failures during /metrics scrapes",
	}, []string{"role", "address", "denom"})
)

// Outcome constants for consistent labeling
const (
	OutcomeSuccess = "success"
	OutcomePartial = "partial"
	OutcomeError   = "error"
	OutcomeFailed  = "failed"
)

// Operation constants for the `operation` label on provisioning_total /
// provisioning_duration_seconds. A restore is a kind of provisioning operation,
// so it shares those metrics and is differentiated by this label rather than a
// separate metric name (ENG-358).
const (
	OperationProvision = "provision"
	OperationRestore   = "restore"
)

// Action constants for reconciliation
const (
	ActionProvisioned   = "provisioned"
	ActionAcknowledged  = "acknowledged"
	ActionDeprovisioned = "deprovisioned"
	ActionAnomaly       = "anomaly"
	ActionLeaseError    = "lease_error"
)
