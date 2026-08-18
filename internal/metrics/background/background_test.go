package background

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectors is every collector this package declares. The point of the package
// is that this list stays small and stays labelled, so it is spelled out rather
// than derived.
func collectors() []prometheus.Collector {
	return []prometheus.Collector{CleanupPanicsTotal, GoroutinePanicsTotal}
}

// TestCollectorsAreLazyAndKeepTheirNames asserts the package invariant and the
// wire contract in one test, because the first assertion is only meaningful
// before the second one writes: these collectors are package globals, so a
// separate test function would depend on Go running them in source order.
//
// Laziness is the invariant. Every fred binary links this package — providerd,
// docker-backend, k3s-backend — so anything registered here materialises in all
// of them. That is only free because each collector is a *Vec: registered
// eagerly like everything else, but exporting no child series until its first
// WithLabelValues. An unlabelled Counter, Gauge or Histogram added here would
// export a series at a permanent 0 on every binary that never writes it, which
// is exactly the trap ENG-712 fixed — and a 0 is not harmless, it satisfies
// ordinary gauge comparisons.
//
// The names are the contract. Both are consumed by name in OPERATIONS.md and in
// manifest-deploy alert rules, and both are emitted by more than one binary, so
// a rename here breaks two repos silently.
func TestCollectorsAreLazyAndKeepTheirNames(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	for _, c := range collectors() {
		require.NoError(t, reg.Register(c))
	}

	assert.Empty(t, gatheredNames(t, reg),
		"every collector in internal/metrics/background must be a *Vec so a binary that never "+
			"writes it exports nothing; these families appeared with no write (ENG-712)")

	CleanupPanicsTotal.WithLabelValues("retention").Inc()
	GoroutinePanicsTotal.WithLabelValues("payload_writer").Inc()

	assert.ElementsMatch(t, []string{
		"fred_background_cleanup_panics_total",
		"fred_background_goroutine_panics_total",
	}, gatheredNames(t, reg))
}

func gatheredNames(t *testing.T, g prometheus.Gatherer) []string {
	t.Helper()

	families, err := g.Gather()
	require.NoError(t, err)

	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	return names
}
