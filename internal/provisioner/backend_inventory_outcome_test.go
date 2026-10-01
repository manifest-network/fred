package provisioner

import (
	"testing"

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

func TestBackendInventoryOutcomeLabelsAreClosedAndOnlyKeptEvidenceAnswers(t *testing.T) {
	assert.Empty(t, backendInventoryOutcomeInvalid.label(), "the zero value is never exported")
	assert.False(t, backendInventoryOutcomeInvalid.answered())

	labels := make(map[string]struct{}, len(backendInventoryOutcomes))
	for _, outcome := range backendInventoryOutcomes {
		label := outcome.label()
		assert.NotEmpty(t, label)
		assert.NotContains(t, labels, label, "every outcome has its own label")
		labels[label] = struct{}{}
	}
	for _, outcome := range backendInventoryOutcomes {
		want := outcome == backendInventoryOutcomeAuthoritative ||
			outcome == backendInventoryOutcomePartial
		assert.Equal(t, want, outcome.answered(), outcome.label())
	}
}
