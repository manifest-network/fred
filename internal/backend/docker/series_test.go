package docker

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm/failurecause"
)

// seriesFreshProcess marks the child process that runs
// TestCounterSeriesExistAtStartup's assertions.
const seriesFreshProcess = "FRED_TEST_DOCKER_SERIES_FRESH_PROCESS"

// TestCounterSeriesExistAtStartup pins ENG-1116 for docker-backend: every
// series of every labeled counter exists at 0 from package initialization, so
// increase() sees the first event after a restart. The two families bound at
// construction must not exist before their component is built. Series are
// process-global and other tests create them, so the assertions run in a
// fresh test process.
func TestCounterSeriesExistAtStartup(t *testing.T) {
	if os.Getenv(seriesFreshProcess) == "" {
		command := exec.CommandContext(t.Context(), os.Args[0],
			"-test.run=^TestCounterSeriesExistAtStartup$", "-test.count=1")
		command.Env = append(os.Environ(), seriesFreshProcess+"=1")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "fresh process:\n%s", output)
		return
	}

	for name, test := range map[string]struct {
		vec  *prometheus.CounterVec
		want int
	}{
		"provisions":                               {provisionsTotal, 2},
		"restores":                                 {restoresTotal, 2},
		"callback deliveries":                      {callbackDeliveryTotal, 2},
		"reconciliation passes":                    {reconciliationTotal, 2},
		"runtime inventory passes":                 {runtimeInventoryTotal, 2},
		"runtime refresh leases":                   {runtimeRefreshTotal, 3},
		"seccomp census passes":                    {tenantSeccompCensusTotal, 2},
		"seccomp profile refusals":                 {tenantSeccompProfileRefusalsTotal, len(tenantSeccompSinks)},
		"retention sweeps":                         {retentionSweepTotal, 2},
		"image GC decisions":                       {imageGCTotal, 7},
		"idempotent operations":                    {idempotentOpsTotal, 3},
		"network reclamation":                      {networkReclamationTotal, 6 + 2*2},
		"quota backfill":                           {volumeQuotaBackfillTotal, 3},
		"reap skips":                               {retentionReapSkipsTotal, 2},
		"orphan skips":                             {retentionOrphanSkipsTotal, 4},
		"cap check failures":                       {retentionCapCheckFailedTotal, 4},
		"partition collapses":                      {retentionPartitionCollapsedTotal, len(shared.PartitionCollapseReasons)},
		"refusals by scope":                        {retentionRefusedByScopeTotal, 3},
		"index rebuilds":                           {retentionIndexReindexTotal, 2},
		"destroy refusals":                         {volumeDestroyRefusedTotal, 5 * 3},
		"teardown fallbacks":                       {teardownFallbackTotal, 3 * 2},
		"tree removals":                            {treeRemovalsTotal, 2 * 7},
		"tree removal cuts":                        {treeRemovalCutsTotal, 2},
		"volume deletes":                           {volumeDeleteOutcomesTotal, 5},
		"projid audits":                            {volumeProjidAuditTotal, len(projidAuditOutcomes)},
		"request auth failures":                    {requestAuthFailuresTotal, len(requestAuthFailures)},
		"maintenance readiness":                    {maintenanceReadinessPendingTotal, 4},
		"recovery timeouts":                        {operationIntentRecoveryTimeoutExhaustionsTotal, 1},
		"operation cleanup retries":                {operationIntentRecoveryCleanupRetriesTotal, 2},
		"terminal substrate retries":               {terminalSubstrateCleanupRetriesTotal, len(terminalReceiptKinds)},
		"container event stream":                   {containerEventStreamTotal, 3},
		"dropped deaths":                           {dieEventDroppedTotal, 2},
		"lease transitions":                        {leaseSMTransitionsTotal, len(leasesm.Transitions())},
		"lease worker panics":                      {leaseWorkerPanicsTotal, len(leasesm.Workers()) + 1},
		"dropped terminal events":                  {leaseTerminalEventDroppedTotal, len(leasesm.TerminalEvents())},
		"lease failures":                           {leaseFailuresTotal, len(failurecause.Labels())},
		"demote refusals (bound by New)":           {restoreDemoteRefusedTotal, 0},
		"request signature keys (bound by server)": {requestSignatureKeyTotal, 0},
	} {
		assert.Equal(t, test.want, testutil.CollectAndCount(test.vec), name)
	}

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if !strings.HasPrefix(family.GetName(), "fred_docker_backend_") || family.GetMetric()[0].GetCounter() == nil {
			continue
		}
		for _, metric := range family.GetMetric() {
			assert.Zero(t, metric.GetCounter().GetValue(), "%s%v starts at 0", family.GetName(), metric.GetLabel())
		}
	}
}

// TestConfiguredFamiliesBindTheirWholeSetAtOnce pins that a construction-time
// binder creates every series of its family when called.
func TestConfiguredFamiliesBindTheirWholeSetAtOnce(t *testing.T) {
	const kind = "series-test-volumes"
	refusals := bindRestoreDemoteRefusals(kind)
	for reason, counter := range map[string]prometheus.Counter{
		"ephemeral_tier":          refusals.ephemeralTier,
		"unmeasurable_read_error": refusals.unmeasurableReadError,
		"unmeasurable_backend":    refusals.unmeasurableBackend,
		"measured_exceeds":        refusals.measuredExceeds,
	} {
		assert.Equal(t, restoreDemoteRefusedTotal.WithLabelValues(kind, reason), counter, reason)
	}
}

// TestClosedListsHoldEveryValueTheirTypeLabels pins that each counterSet's
// list is its type's whole closed set: every value whose label is non-empty
// is listed, so none can reach a recording site without a series.
func TestClosedListsHoldEveryValueTheirTypeLabels(t *testing.T) {
	assert.ElementsMatch(t, labeledValues(func(v uint8) string { return projidAuditOutcome(v).label() }),
		uint8s(projidAuditOutcomes[:]), "projidAuditOutcomes")
	assert.ElementsMatch(t, labeledValues(func(v uint8) string { return tenantSeccompSink(v).label() }),
		uint8s(tenantSeccompSinks[:]), "tenantSeccompSinks")
	assert.ElementsMatch(t, labeledValues(func(v uint8) string { return requestAuthFailure(v).label() }),
		uint8s(requestAuthFailures[:]), "requestAuthFailures")
}

func labeledValues(label func(uint8) string) []uint8 {
	var values []uint8
	for value := range 256 {
		if label(uint8(value)) != "" {
			values = append(values, uint8(value))
		}
	}
	return values
}

func uint8s[T ~uint8](values []T) []uint8 {
	converted := make([]uint8, 0, len(values))
	for _, value := range values {
		converted = append(converted, uint8(value))
	}
	return converted
}
