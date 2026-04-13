package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsRegistered(t *testing.T) {
	// Register all collectors in a fresh registry and Gather() to verify
	// names without relying on debug string formats.
	reg := prometheus.NewPedanticRegistry()

	collectors := []prometheus.Collector{
		InFlightProvisions,
		ProvisioningTotal,
		ProvisioningDuration,
		CallbackTimeoutsTotal,
		ReconciliationTotal,
		ReconciliationDuration,
		ReconciliationActions,
		ReconciliationConflictsTotal,
		PayloadUploadsTotal,
		PayloadStoredCount,
		PayloadSizeBytes,
		LeasesAwaitingPayload,
		BackendRequestDuration,
		BackendRequestsTotal,
		BackendInsufficientResourcesTotal,
		BackendHealthy,
		BackendCircuitBreakerState,
		RateLimitRejectionsTotal,
		ReconcilerLastSuccessTimestamp,
		APIRequestDuration,
		APIRequestsTotal,
		NonInFlightCallbacksTotal,
		ChainTxTotal,
		ChainQueryDuration,
		WatermillMessagesTotal,
		PoisonedMessagesTotal,
		EventsDroppedTotal,
		MalformedMessagesTotal,
		SignerPoolSize,
		SignerPoolLaneCount,
	}

	for _, c := range collectors {
		require.NoError(t, reg.Register(c))
	}

	families, err := reg.Gather()
	require.NoError(t, err)

	names := make(map[string]bool, len(families))
	for _, f := range families {
		names[f.GetName()] = true
	}

	// Every gathered metric name must start with "fred_".
	for name := range names {
		assert.True(t, strings.HasPrefix(name, "fred_"), "metric %q should start with fred_", name)
	}

	// Non-vec metrics (Gauge, Counter, Histogram) appear immediately in Gather;
	// Vec metrics only appear after WithLabelValues, so we check those separately
	// in TestCounterVecLabels. Here we verify the non-vec subset.
	for _, expected := range []string{
		"fred_provisioner_in_flight_provisions",
		"fred_provisioner_callback_timeouts_total",
		"fred_reconciler_duration_seconds",
		"fred_reconciler_conflicts_total",
		"fred_payload_stored_count",
		"fred_payload_size_bytes",
		"fred_payload_leases_awaiting",
		"fred_api_non_in_flight_callbacks_total",
		"fred_watermill_poisoned_messages_total",
		"fred_reconciler_last_success_timestamp_seconds",
		"fred_signer_pool_size",
		"fred_signer_pool_lane_count",
	} {
		assert.True(t, names[expected], "metric %q should be gathered", expected)
	}
}

func TestCounterVecLabels(t *testing.T) {
	// Verify each Vec metric accepts its documented labels without panic.
	assert.NotPanics(t, func() {
		ProvisioningTotal.WithLabelValues("success", "docker")
	})
	assert.NotPanics(t, func() {
		ProvisioningDuration.WithLabelValues("docker")
	})
	assert.NotPanics(t, func() {
		ReconciliationTotal.WithLabelValues("success")
	})
	assert.NotPanics(t, func() {
		ReconciliationActions.WithLabelValues("provisioned")
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
		APIRequestDuration.WithLabelValues("GET", "/health", "200")
	})
	assert.NotPanics(t, func() {
		APIRequestsTotal.WithLabelValues("GET", "/health", "200")
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
		BackendInsufficientResourcesTotal.WithLabelValues("docker")
	})
	assert.NotPanics(t, func() {
		BackendHealthy.WithLabelValues("docker")
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
