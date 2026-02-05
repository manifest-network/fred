// Package metrics provides Prometheus metrics for fred observability.
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

	// ProvisioningTotal tracks the total number of provisioning operations by outcome.
	ProvisioningTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "provisioner",
		Name:      "provisioning_total",
		Help:      "Total number of provisioning operations by outcome",
	}, []string{"outcome", "backend"})

	// ProvisioningDuration tracks the duration of provisioning operations.
	ProvisioningDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "provisioner",
		Name:      "provisioning_duration_seconds",
		Help:      "Duration of provisioning operations in seconds",
		Buckets:   prometheus.ExponentialBuckets(0.1, 2, 12), // 0.1s to ~7min
	}, []string{"backend"})
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

	// LeasesAwaitingPayloadTotal tracks leases that are waiting for payload upload.
	LeasesAwaitingPayloadTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "payload",
		Name:      "leases_awaiting_total",
		Help:      "Total number of leases seen that require payload upload",
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

	// BackendCircuitBreakerState tracks circuit breaker state (0=closed, 1=half-open, 2=open).
	BackendCircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Subsystem: "backend",
		Name:      "circuit_breaker_state",
		Help:      "Circuit breaker state (0=closed, 1=half-open, 2=open)",
	}, []string{"backend"})
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

	// DuplicateCallbacksTotal tracks duplicate callbacks that were ignored (idempotency).
	DuplicateCallbacksTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: "api",
		Name:      "duplicate_callbacks_total",
		Help:      "Total number of duplicate callbacks ignored due to idempotency check",
	})
)

// Outcome constants for consistent labeling
const (
	OutcomeSuccess = "success"
	OutcomeError   = "error"
	OutcomeFailed  = "failed"
)

// Action constants for reconciliation
const (
	ActionProvisioned   = "provisioned"
	ActionAcknowledged  = "acknowledged"
	ActionDeprovisioned = "deprovisioned"
	ActionAnomaly       = "anomaly"
)
