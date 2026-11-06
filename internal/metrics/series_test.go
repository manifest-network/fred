package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClosedCounterFamiliesExistAtInit pins ENG-1116 for every closed
// vocabulary: each series exists at 0 from package initialization, so
// increase() sees the first event after a restart. The two families bound only
// when their feature is configured must not exist yet; no test in this package
// binds them.
func TestClosedCounterFamiliesExistAtInit(t *testing.T) {
	for name, test := range map[string]struct {
		vec  *prometheus.CounterVec
		want int
	}{
		"runs":                      {reconciliationTotal, 4},
		"actions":                   {reconciliationActions, 5},
		"reconciler panics":         {reconcilerPanicsTotal, 8},
		"lost leases":               {reconcilerLostLeasesTotal, 4},
		"terminal verdicts":         {reconcilerTerminalVerdictsTotal, 4},
		"cleanup skips":             {reconcilerCleanupSkipsTotal, 3 * 6},
		"backend invocation panics": {backendInvocationPanicsTotal, 6},
		"event sink panics":         {lifecycleEventSinkPanicsTotal, 4},
		"lifecycle callbacks":       {lifecycleCallbackOutcomesTotal, 3 * 9 * 3},
		"deferred closes":           {deferredClosesTotal, 10 * 4},
		"watermill messages":        {watermillMessagesTotal, 5 * 2},
		"malformed messages":        {malformedMessagesTotal, 5},
		"dropped events":            {eventsDroppedTotal, 6},
		"chain transactions":        {chainTxTotal, 4 * 2},
		"out-of-gas retries":        {signerOOGRetriesTotal, 2},
		"gas simulation":            {gasSimulationTotal, 3},
		"payload uploads":           {payloadUploadsTotal, 7},
		"rate limit rejections":     {rateLimitRejectionsTotal, 4},
		"callback auth failures":    {apiCallbackAuthFailuresTotal, 7},
		"maintenance refusals":      {maintenanceAdmissionRefusalsTotal, 4},
		"signer grant checks":       {signerGrantCheckTotal, 0},
		"placement snapshots":       {placementSnapshotsTotal, 0},
		"snapshot prune failures":   {placementSnapshotPruneFailuresTotal, 0},
	} {
		assert.Equal(t, test.want, testutil.CollectAndCount(test.vec), name)
	}
	assert.Equal(t, 0.0, testutil.ToFloat64(ReconcilerRuns.Success), "a series exists before its first event at 0")
}

// TestBindersCreateTheirWholeFamilyAtOnce pins that each binder creates every
// series of its family when called, not one series per event.
func TestBindersCreateTheirWholeFamilyAtOnce(t *testing.T) {
	const backend = "series-test-backend"
	BindBackends(backend)
	for name, test := range map[string]struct {
		vec  *prometheus.CounterVec
		want int
	}{
		"provisioning":           {provisioningTotal, 3 * 2},
		"reconcile fetch":        {reconcilerBackendFetchTotal, 4},
		"inventory":              {reconcilerBackendInventoryTotal, 7},
		"insufficient resources": {backendInsufficientResourcesTotal, 2},
		"callback signature key": {apiCallbackSignatureKeyTotal, 2},
		"non-in-flight":          {nonInFlightCallbacksTotal, 3},
	} {
		assert.Equal(t, test.want, seriesOfBackend(t, test.vec, backend), name)
	}
	assert.Equal(t, ForBackend(backend), ForBackend(backend), "a backend's series are bound once")

	ForBackend("never-bound").ReconcileFetch.Error.Inc()
	for _, vec := range []*prometheus.CounterVec{
		provisioningTotal, reconcilerBackendFetchTotal, reconcilerBackendInventoryTotal,
		backendInsufficientResourcesTotal, apiCallbackSignatureKeyTotal, nonInFlightCallbacksTotal,
	} {
		assert.Zero(t, seriesOfBackend(t, vec, "never-bound"), "a lookup never creates a series")
	}
	assert.Equal(t, 1.0, testutil.ToFloat64(unconfiguredBackend.ReconcileFetch.Error),
		"an event naming an unbound backend counts under backend=\"unconfigured\"")

	BindBackendRequest(backend, "provision")
	assert.Equal(t, 4, seriesOfBackend(t, backendRequestsTotal, backend), "every request status")
	assert.Equal(t, 1, seriesOfBackend(t, backendMalformedErrorBodyTotal, backend))

	BindAckBatchLane("series-test-lane")
	for _, vec := range []*prometheus.CounterVec{
		ackBatchFeeGasErrorsTotal, ackBatchIndividualFallbacksTotal, ackBatcherLaneRestartsTotal,
	} {
		assert.Equal(t, 1, seriesWithLabel(t, vec, "lane", "series-test-lane"))
	}
}

func seriesOfBackend(t *testing.T, vec *prometheus.CounterVec, backend string) int {
	t.Helper()
	return seriesWithLabel(t, vec, "backend", backend)
}

func seriesWithLabel(t *testing.T, collector prometheus.Collector, name, value string) int {
	t.Helper()
	registry := prometheus.NewPedanticRegistry()
	require.NoError(t, registry.Register(collector))
	families, err := registry.Gather()
	require.NoError(t, err)
	count := 0
	for _, family := range families {
		for _, series := range family.GetMetric() {
			for _, pair := range series.GetLabel() {
				if pair.GetName() == name && pair.GetValue() == value {
					count++
				}
			}
		}
	}
	return count
}
