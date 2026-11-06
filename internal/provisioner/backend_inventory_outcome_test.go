package provisioner

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/manifest-network/fred/internal/metrics"

	"github.com/stretchr/testify/assert"

	"github.com/manifest-network/fred/internal/provisioner/placement"
)

func TestClassifyBackendInventoryCoversEveryEvidenceShape(t *testing.T) {
	dispositions := []placement.BackendInventoryDisposition{
		placement.BackendInventoryInvalid,
		placement.BackendInventoryAuthoritative,
		placement.BackendInventoryUntrusted,
		placement.BackendInventoryPartial,
	}
	for _, disposition := range dispositions {
		assert.Equal(t, backendInventoryOutcomeUnanswered,
			classifyBackendInventory(false, false, disposition))
		assert.Equal(t, backendInventoryOutcomeProvisionsOnly,
			classifyBackendInventory(true, false, disposition))
		assert.Equal(t, backendInventoryOutcomeRetentionsOnly,
			classifyBackendInventory(false, true, disposition))
	}
	assert.Equal(t, backendInventoryOutcomeAuthoritative,
		classifyBackendInventory(true, true, placement.BackendInventoryAuthoritative))
	assert.Equal(t, backendInventoryOutcomePartial,
		classifyBackendInventory(true, true, placement.BackendInventoryPartial))
	assert.Equal(t, backendInventoryOutcomeUntrusted,
		classifyBackendInventory(true, true, placement.BackendInventoryUntrusted))
	assert.Equal(t, backendInventoryOutcomeUntrusted,
		classifyBackendInventory(true, true, placement.BackendInventoryInvalid),
		"a paired answer the sweep did not accept never counts as answered")
}

func TestBackendInventoryOutcomeSeriesAreClosedAndOnlyKeptEvidenceAnswers(t *testing.T) {
	metrics.BindBackends("inventory-outcome-test")
	counters := metrics.ForBackend("inventory-outcome-test").Inventory
	_, ok := backendInventoryOutcomeInvalid.counter(counters)
	assert.False(t, ok, "the zero value is never exported")
	assert.False(t, backendInventoryOutcomeInvalid.answered())

	seen := make(map[prometheus.Counter]struct{}, len(backendInventoryOutcomes))
	for _, outcome := range backendInventoryOutcomes {
		counter, ok := outcome.counter(counters)
		assert.True(t, ok)
		assert.NotContains(t, seen, counter, "every outcome has its own series")
		seen[counter] = struct{}{}
	}
	for _, outcome := range backendInventoryOutcomes {
		want := outcome == backendInventoryOutcomeAuthoritative ||
			outcome == backendInventoryOutcomePartial
		assert.Equal(t, want, outcome.answered(), "outcome %d", outcome)
	}
}
