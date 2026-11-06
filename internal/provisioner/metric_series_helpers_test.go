package provisioner

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/metrics"
)

// These helpers resolve a series from the label a test table names. They only
// select series metrics already bound; none can create one.

func inventorySeries(t testing.TB, backendName, outcome string) prometheus.Counter {
	t.Helper()
	metrics.BindBackends(backendName)
	counters := metrics.ForBackend(backendName).Inventory
	series, ok := map[string]prometheus.Counter{
		metrics.InventoryOutcomeAuthoritative:  counters.Authoritative,
		metrics.InventoryOutcomePartial:        counters.Partial,
		metrics.InventoryOutcomeUntrusted:      counters.Untrusted,
		metrics.InventoryOutcomeProvisionsOnly: counters.ProvisionsOnly,
		metrics.InventoryOutcomeRetentionsOnly: counters.RetentionsOnly,
		metrics.InventoryOutcomeUnanswered:     counters.Unanswered,
		metrics.InventoryOutcomeFenced:         counters.Fenced,
	}[outcome]
	require.True(t, ok, "unknown inventory outcome %q", outcome)
	return series
}

func terminalVerdictSeries(t testing.TB, verdict string) prometheus.Counter {
	t.Helper()
	verdicts := metrics.ReconcilerTerminalVerdicts
	series, ok := map[string]prometheus.Counter{
		"absent": verdicts.Absent, "unknown": verdicts.Unknown,
		"retry": verdicts.Retry, "exhausted": verdicts.Exhausted,
	}[verdict]
	require.True(t, ok, "unknown terminal verdict %q", verdict)
	return series
}

func cleanupSkipSeries(t testing.TB, pass metrics.CleanupSkipReasonCounters, reason string) prometheus.Counter {
	t.Helper()
	series, ok := pass.ForReason(reason)
	require.True(t, ok, "unknown cleanup skip reason %q", reason)
	return series
}

func capacityVerdictSeries(t testing.TB, backendName, verdict string) prometheus.Counter {
	t.Helper()
	metrics.BindBackends(backendName)
	verdicts := metrics.ForBackend(backendName).InsufficientResources
	series, ok := map[string]prometheus.Counter{
		metrics.CapacityVerdictCodedRefusal: verdicts.CodedRefusal,
		metrics.CapacityVerdictAmbiguous:    verdicts.Ambiguous,
	}[verdict]
	require.True(t, ok, "unknown capacity verdict %q", verdict)
	return series
}
