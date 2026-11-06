package testutil

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// CounterValue reads one counter series from the default registry by metric
// name and exact label set, and reports whether the series exists. Tests use
// it for labeled families whose collectors are unexported, so a test can
// address a series by its label values without being able to create one.
func CounterValue(t testing.TB, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather default registry: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			pairs := metric.GetLabel()
			if len(pairs) != len(labels) {
				continue
			}
			matched := true
			for _, pair := range pairs {
				if value, ok := labels[pair.GetName()]; !ok || value != pair.GetValue() {
					matched = false
					break
				}
			}
			if matched {
				return metric.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}
