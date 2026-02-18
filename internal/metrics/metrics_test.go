package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

// describeNames collects fqName strings from a prometheus.Collector via Describe().
func describeNames(c prometheus.Collector) []string {
	ch := make(chan *prometheus.Desc, 10)
	go func() {
		c.Describe(ch)
		close(ch)
	}()
	var names []string
	for d := range ch {
		// Desc.String() returns: "Desc{fqName: \"...\", help: ...}"
		s := d.String()
		if idx := strings.Index(s, "fqName: \""); idx >= 0 {
			s = s[idx+len("fqName: \""):]
			if end := strings.Index(s, "\""); end >= 0 {
				names = append(names, s[:end])
			}
		}
	}
	return names
}

func TestMetricsRegistered(t *testing.T) {
	// Collect all metric collectors and verify their fqNames start with "fred_".
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
		LeasesAwaitingPayloadTotal,
		BackendRequestDuration,
		BackendRequestsTotal,
		BackendCircuitBreakerState,
		APIRequestDuration,
		APIRequestsTotal,
		DuplicateCallbacksTotal,
		ChainTxTotal,
		ChainQueryDuration,
		WatermillMessagesTotal,
		PoisonedMessagesTotal,
		EventsDroppedTotal,
		MalformedMessagesTotal,
	}

	var allNames []string
	for _, c := range collectors {
		names := describeNames(c)
		assert.NotEmpty(t, names, "collector should describe at least one metric")
		for _, n := range names {
			assert.True(t, strings.HasPrefix(n, "fred_"), "metric %q should start with fred_", n)
		}
		allNames = append(allNames, names...)
	}

	// We expect at least 24 distinct metric names (one per collector).
	assert.GreaterOrEqual(t, len(allNames), 24, "should have at least 24 metric names")
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
}

func TestOutcomeConstants(t *testing.T) {
	outcomes := []string{OutcomeSuccess, OutcomePartial, OutcomeError, OutcomeFailed}

	// All must be non-empty.
	for _, o := range outcomes {
		assert.NotEmpty(t, o)
	}

	// All must be distinct.
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
