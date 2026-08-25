package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unlabelledMetricNames is every metric this package exports the moment a binary
// links it: the collectors declared WITHOUT labels, which promauto registers on
// the default registerer at package init and which therefore materialise — at a
// permanent 0 — on any binary that imports this package whether it writes them
// or not. That set is the blast radius of importing `internal/metrics`, which is
// why it is spelled out here rather than derived (ENG-712).
//
// These are Gather() family names; the three histograms each expand into
// _bucket/_sum/_count in the text exposition, so 24 collectors here are 30
// metric names on the wire.
var unlabelledMetricNames = []string{
	"fred_backend_health_probe_panics_total",
	"fred_backend_routing_fallback_total",
	"fred_chain_gas_simulated",
	"fred_payload_leases_awaiting",
	"fred_payload_size_bytes",
	"fred_payload_stored_count",
	"fred_placement_write_failures_total",
	"fred_provisioner_callback_placement_semantic_conflicts_total",
	"fred_provisioner_callback_settlement_claim_wait_timeouts_total",
	"fred_provisioner_callback_timeouts_total",
	"fred_provisioner_in_flight_provisions",
	"fred_provisioner_reconciler_deferred_leases_total",
	"fred_provisioner_reconciler_inflight_skips_total",
	"fred_reconciler_conflicts_total",
	"fred_reconciler_duration_seconds",
	"fred_reconciler_last_success_timestamp_seconds",
	"fred_reconciler_sweep_complete",
	"fred_signer_pool_lane_count",
	"fred_signer_pool_size",
	"fred_watermill_poisoned_messages_total",
	"fred_withdraw_credit_check_zero_deferred_total",
	"fred_withdraw_guard_active",
	"fred_withdraw_incomplete_cycles_total",
	"fred_withdraw_skipped_by_guard_total",
}

// labelledMetricNames is every *Vec in the package. A Vec registers eagerly too,
// but exports no child series until its first WithLabelValues, so it costs a
// binary that never writes it nothing. Listed so TestMetricSurface can tell a
// legitimately-written Vec family apart from a collector that was renamed, or
// added without being declared here.
var labelledMetricNames = []string{
	"fred_api_non_in_flight_callbacks_total",
	"fred_api_rate_limit_rejections_total",
	"fred_api_request_duration_seconds",
	"fred_api_requests_total",
	"fred_backend_allocated_cpu_ratio",
	"fred_backend_circuit_breaker_state",
	"fred_backend_healthy",
	"fred_backend_insufficient_resources_total",
	"fred_backend_malformed_error_body_total",
	"fred_backend_request_duration_seconds",
	"fred_backend_requests_total",
	"fred_chain_gas_simulation_total",
	"fred_chain_query_duration_seconds",
	"fred_chain_signer_oog_retries_total",
	"fred_chain_transactions_total",
	"fred_events_dropped_total",
	"fred_health_check_healthy",
	"fred_messages_malformed_total",
	"fred_payload_persist_failures_total",
	"fred_payload_uploads_total",
	"fred_provisioner_ack_batch_fee_gas_errors_total",
	"fred_provisioner_ack_batch_individual_fallbacks_total",
	"fred_provisioner_ack_batcher_lane_restarts_total",
	"fred_provisioner_provisioning_duration_seconds",
	"fred_provisioner_provisioning_total",
	"fred_provisioner_reconciler_panics_total",
	"fred_reconciler_actions_total",
	"fred_reconciler_backend_fetch_total",
	"fred_reconciler_cleanup_skips_total",
	"fred_reconciler_runs_total",
	"fred_signer_balance_query_failures_total",
	"fred_signer_grant_check_total",
	"fred_watermill_messages_total",
}

// allCollectors is every collector this package declares, exhaustively. The
// count assertion in TestMetricsRegistered ties it to the two name lists above,
// so a new collector cannot be added to the package without landing in all
// three.
func allCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		// Provisioning
		PlacementWriteFailuresTotal,
		InFlightProvisions,
		ProvisioningTotal,
		ProvisioningDuration,
		AckBatchFeeGasErrorsTotal,
		AckBatchIndividualFallbacksTotal,
		AckBatcherLaneRestartsTotal,
		ReconcilerInflightSkipsTotal,
		ReconcilerDeferredLeasesTotal,
		ReconcilerPanicsTotal,
		SignerOOGRetriesTotal,
		GasSimulationTotal,
		GasSimulated,
		// Reconciliation
		ReconciliationTotal,
		ReconciliationDuration,
		ReconcilerLastSuccessTimestamp,
		ReconciliationActions,
		ReconcilerBackendFetchTotal,
		ReconcilerSweepComplete,
		ReconcilerCleanupSkipsTotal,
		// Payload
		PayloadUploadsTotal,
		PayloadStoredCount,
		PayloadPersistFailuresTotal,
		PayloadSizeBytes,
		LeasesAwaitingPayload,
		// Backend
		BackendRequestDuration,
		BackendRequestsTotal,
		BackendInsufficientResourcesTotal,
		BackendMalformedErrorBodyTotal,
		BackendHealthy,
		BackendCircuitBreakerState,
		BackendAllocatedCPURatio,
		RoutingFallbackTotal,
		BackendHealthProbePanicsTotal,
		// Rate limit / API / health
		RateLimitRejectionsTotal,
		APIRequestDuration,
		APIRequestsTotal,
		HealthCheckHealthy,
		// Chain
		ChainTxTotal,
		ChainQueryDuration,
		// Withdraw
		WithdrawIncompleteCyclesTotal,
		WithdrawSkippedByGuardTotal,
		CreditCheckZeroDeferredTotal,
		WithdrawGuardActive,
		// Watermill / events / messages
		WatermillMessagesTotal,
		PoisonedMessagesTotal,
		EventsDroppedTotal,
		MalformedMessagesTotal,
		ReconciliationConflictsTotal,
		// Callback
		CallbackPlacementSemanticConflictsTotal,
		CallbackSettlementClaimWaitTimeoutsTotal,
		CallbackTimeoutsTotal,
		NonInFlightCallbacksTotal,
		// Signer pool
		SignerPoolSize,
		SignerPoolLaneCount,
		SignerBalanceQueryFailures,
		SignerGrantCheckTotal,
	}
}

func TestMetricsRegistered(t *testing.T) {
	// A pedantic registry re-registers every collector from scratch, which
	// validates each Desc and catches two collectors that disagree on help text
	// or label names for the same metric name.
	reg := prometheus.NewPedanticRegistry()

	collectors := allCollectors()
	require.Len(t, collectors, len(unlabelledMetricNames)+len(labelledMetricNames),
		"allCollectors() is out of sync with unlabelledMetricNames + labelledMetricNames — "+
			"a new collector must be added to all three")

	for _, c := range collectors {
		require.NoError(t, reg.Register(c))
	}

	families, err := reg.Gather()
	require.NoError(t, err)

	known := knownMetricNames()
	for _, f := range families {
		name := f.GetName()
		assert.True(t, strings.HasPrefix(name, "fred_"), "metric %q should start with fred_", name)
		assert.True(t, known[name], "metric %q is not declared in unlabelledMetricNames or labelledMetricNames", name)
	}
}

// TestMetricSurface pins the exact set of metric names this package adds to the
// default registry of every binary that links it. Because the package registers
// via promauto at init, this test's own process is that surface: gathering the
// default gatherer here shows precisely what a providerd — or, before ENG-712,
// a docker-backend — exports on account of importing `internal/metrics`.
//
// Two directions, both load-bearing:
//
//   - every unlabelled name must still be exported, so a rename is caught;
//   - every exported fred_ name must be declared, so a newly added unlabelled
//     collector cannot slip in undeclared. That is the ENG-712 hazard: an
//     unlabelled collector is exported at 0 by every binary that links this
//     package, and a 0 satisfies ordinary gauge comparisons.
//
// The second direction tolerates Vec families, which appear only once another
// test in this package has written one; that is why labelledMetricNames exists
// and why this test does not assert an exact set.
func TestMetricSurface(t *testing.T) {
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	known := knownMetricNames()
	gathered := make(map[string]bool, len(families))
	for _, f := range families {
		name := f.GetName()
		if !strings.HasPrefix(name, "fred_") {
			continue // go_* / process_* collectors registered by client_golang itself
		}
		gathered[name] = true
		assert.True(t, known[name],
			"metric %q is exported by internal/metrics but declared in neither unlabelledMetricNames "+
				"nor labelledMetricNames; every fred_ series this package adds to a binary must be listed (ENG-712)",
			name)
	}

	for _, name := range unlabelledMetricNames {
		assert.True(t, gathered[name],
			"metric %q is declared unlabelled but was not exported — was it renamed or moved out of this package?",
			name)
	}
}

func knownMetricNames() map[string]bool {
	known := make(map[string]bool, len(unlabelledMetricNames)+len(labelledMetricNames))
	for _, n := range unlabelledMetricNames {
		known[n] = true
	}
	for _, n := range labelledMetricNames {
		known[n] = true
	}
	return known
}

func TestCounterVecLabels(t *testing.T) {
	// Verify each Vec metric accepts its documented labels without panic.
	assert.NotPanics(t, func() {
		ProvisioningTotal.WithLabelValues("success", "docker", "provision")
		ProvisioningTotal.WithLabelValues("success", "docker", "restore")
	})
	assert.NotPanics(t, func() {
		ProvisioningDuration.WithLabelValues("docker", "provision")
		ProvisioningDuration.WithLabelValues("docker", "restore")
	})
	assert.NotPanics(t, func() {
		AckBatchFeeGasErrorsTotal.WithLabelValues("0")
		AckBatchIndividualFallbacksTotal.WithLabelValues("0")
		AckBatcherLaneRestartsTotal.WithLabelValues("0")
	})
	assert.NotPanics(t, func() {
		ReconcilerPanicsTotal.WithLabelValues("process_lease")
		ReconcilerPanicsTotal.WithLabelValues("process_orphan")
		ReconcilerPanicsTotal.WithLabelValues("fetch_provisions")
		ReconcilerPanicsTotal.WithLabelValues("fetch_retentions")
	})
	assert.NotPanics(t, func() {
		SignerOOGRetriesTotal.WithLabelValues("retried")
		SignerOOGRetriesTotal.WithLabelValues("exhausted")
	})
	assert.NotPanics(t, func() {
		GasSimulationTotal.WithLabelValues("simulated")
		GasSimulationTotal.WithLabelValues("fallback")
		GasSimulationTotal.WithLabelValues("refused")
	})
	assert.NotPanics(t, func() {
		ReconciliationTotal.WithLabelValues(OutcomeSuccess)
		ReconciliationTotal.WithLabelValues(OutcomePartial)
		ReconciliationTotal.WithLabelValues(OutcomeDegraded)
		ReconciliationTotal.WithLabelValues(OutcomeError)
	})
	assert.NotPanics(t, func() {
		ReconciliationActions.WithLabelValues("provisioned")
	})
	assert.NotPanics(t, func() {
		ReconcilerBackendFetchTotal.WithLabelValues("docker", FetchOutcomeOK)
		ReconcilerBackendFetchTotal.WithLabelValues("docker", FetchOutcomeError)
		ReconcilerBackendFetchTotal.WithLabelValues("docker", FetchOutcomeCircuitOpen)
		ReconcilerBackendFetchTotal.WithLabelValues("docker", FetchOutcomePanic)
	})
	assert.NotPanics(t, func() {
		// Both label sets are closed; every combination the reconciler can emit.
		for _, pass := range []string{CleanupPassOrphan, CleanupPassPayload, CleanupPassPlacement} {
			for _, reason := range []string{
				CleanupSkipChainLive, CleanupSkipChainUnknown, CleanupSkipChainUnknownState,
				CleanupSkipChainError, CleanupSkipBackendSilent,
			} {
				ReconcilerCleanupSkipsTotal.WithLabelValues(pass, reason)
			}
		}
	})
	assert.NotPanics(t, func() {
		PayloadUploadsTotal.WithLabelValues("success")
	})
	assert.NotPanics(t, func() {
		BackendRequestDuration.WithLabelValues("docker", "provision", "200")
	})
	assert.NotPanics(t, func() {
		BackendRequestsTotal.WithLabelValues("docker", "provision", "200")
	})
	assert.NotPanics(t, func() {
		BackendCircuitBreakerState.WithLabelValues("docker")
	})
	assert.NotPanics(t, func() {
		BackendAllocatedCPURatio.WithLabelValues("docker")
	})
	assert.NotPanics(t, func() {
		APIRequestDuration.WithLabelValues("GET", "/health", "200")
	})
	assert.NotPanics(t, func() {
		APIRequestsTotal.WithLabelValues("GET", "/health", "200")
	})
	assert.NotPanics(t, func() {
		for _, check := range []string{"chain", "token_tracker", "placement_store", "payload_store"} {
			HealthCheckHealthy.WithLabelValues(check)
		}
	})
	assert.NotPanics(t, func() {
		ChainTxTotal.WithLabelValues("acknowledge", "success")
	})
	assert.NotPanics(t, func() {
		ChainQueryDuration.WithLabelValues("get_pending_leases")
	})
	assert.NotPanics(t, func() {
		WatermillMessagesTotal.WithLabelValues("provision", "success")
	})
	assert.NotPanics(t, func() {
		EventsDroppedTotal.WithLabelValues("lease_created")
	})
	assert.NotPanics(t, func() {
		MalformedMessagesTotal.WithLabelValues("provision")
	})
	assert.NotPanics(t, func() {
		RateLimitRejectionsTotal.WithLabelValues("global")
	})
	assert.NotPanics(t, func() {
		PayloadPersistFailuresTotal.WithLabelValues("update")
	})
	assert.NotPanics(t, func() {
		BackendInsufficientResourcesTotal.WithLabelValues("docker")
		BackendMalformedErrorBodyTotal.WithLabelValues("docker", "restore")
	})
	assert.NotPanics(t, func() {
		BackendHealthy.WithLabelValues("docker")
	})
	assert.NotPanics(t, func() {
		NonInFlightCallbacksTotal.WithLabelValues("docker", "success")
	})
	assert.NotPanics(t, func() {
		SignerBalanceQueryFailures.WithLabelValues("provider", "manifest1abc", "umfx")
	})
	assert.NotPanics(t, func() {
		SignerBalanceQueryFailures.WithLabelValues("sub_signer", "manifest1xyz", "umfx")
	})
}

func TestOutcomeConstants(t *testing.T) {
	outcomes := []string{OutcomeSuccess, OutcomePartial, OutcomeError, OutcomeFailed}

	for _, o := range outcomes {
		assert.NotEmpty(t, o)
	}

	seen := make(map[string]bool)
	for _, o := range outcomes {
		assert.False(t, seen[o], "duplicate outcome constant: %s", o)
		seen[o] = true
	}
}

func TestActionConstants(t *testing.T) {
	actions := []string{ActionProvisioned, ActionAcknowledged, ActionDeprovisioned, ActionAnomaly, ActionLeaseError}

	for _, a := range actions {
		assert.NotEmpty(t, a)
	}

	seen := make(map[string]bool)
	for _, a := range actions {
		assert.False(t, seen[a], "duplicate action constant: %s", a)
		seen[a] = true
	}
}
