package provisioner

import (
	"github.com/manifest-network/fred/internal/metrics"
	"github.com/manifest-network/fred/internal/provisioner/placement"
)

// backendInventoryOutcome is the closed disposition of one configured
// backend's inventory evidence in a sealed sweep. The zero value is invalid
// and is never exported.
type backendInventoryOutcome uint8

const (
	backendInventoryOutcomeInvalid backendInventoryOutcome = iota
	backendInventoryOutcomeAuthoritative
	backendInventoryOutcomePartial
	backendInventoryOutcomeUntrusted
	backendInventoryOutcomeProvisionsOnly
	backendInventoryOutcomeRetentionsOnly
	backendInventoryOutcomeUnanswered
	// backendInventoryOutcomeFenced: the operator fenced the backend, so it
	// was not asked. Distinct from unanswered so silence alerts can skip it.
	backendInventoryOutcomeFenced
)

// backendInventoryOutcomes lists every exported outcome.
var backendInventoryOutcomes = [...]backendInventoryOutcome{
	backendInventoryOutcomeAuthoritative,
	backendInventoryOutcomePartial,
	backendInventoryOutcomeUntrusted,
	backendInventoryOutcomeProvisionsOnly,
	backendInventoryOutcomeRetentionsOnly,
	backendInventoryOutcomeUnanswered,
	backendInventoryOutcomeFenced,
}

// classifyBackendInventory maps which endpoints answered, and how the sweep
// disposed a paired answer, to exactly one exported outcome. A paired answer
// the sweep did not accept counts as untrusted, never as answered.
func classifyBackendInventory(
	provisionAnswered, retentionAnswered bool,
	disposition placement.BackendInventoryDisposition,
) backendInventoryOutcome {
	switch {
	case provisionAnswered && retentionAnswered:
		switch disposition {
		case placement.BackendInventoryAuthoritative:
			return backendInventoryOutcomeAuthoritative
		case placement.BackendInventoryPartial:
			return backendInventoryOutcomePartial
		default:
			return backendInventoryOutcomeUntrusted
		}
	case provisionAnswered:
		return backendInventoryOutcomeProvisionsOnly
	case retentionAnswered:
		return backendInventoryOutcomeRetentionsOnly
	default:
		return backendInventoryOutcomeUnanswered
	}
}

// label is the outcome's metric label; the invalid zero value has none.
func (outcome backendInventoryOutcome) label() string {
	switch outcome {
	case backendInventoryOutcomeAuthoritative:
		return metrics.InventoryOutcomeAuthoritative
	case backendInventoryOutcomePartial:
		return metrics.InventoryOutcomePartial
	case backendInventoryOutcomeUntrusted:
		return metrics.InventoryOutcomeUntrusted
	case backendInventoryOutcomeProvisionsOnly:
		return metrics.InventoryOutcomeProvisionsOnly
	case backendInventoryOutcomeRetentionsOnly:
		return metrics.InventoryOutcomeRetentionsOnly
	case backendInventoryOutcomeUnanswered:
		return metrics.InventoryOutcomeUnanswered
	case backendInventoryOutcomeFenced:
		return metrics.InventoryOutcomeFenced
	default:
		return ""
	}
}

// answered reports whether the backend answered both inventories with its
// pinned storage identity: exactly the dispositions whose evidence the sweep
// keeps for placement.
func (outcome backendInventoryOutcome) answered() bool {
	return outcome == backendInventoryOutcomeAuthoritative ||
		outcome == backendInventoryOutcomePartial
}

// recordBackendInventoryOutcomes exports one sealed sweep: one counter
// increment and one answered-gauge write per configured backend.
func recordBackendInventoryOutcomes(outcomes map[string]backendInventoryOutcome) {
	for backendName, outcome := range outcomes {
		label := outcome.label()
		if label == "" {
			continue
		}
		metrics.ReconcilerBackendInventoryTotal.WithLabelValues(backendName, label).Inc()
		answered := 0.0
		if outcome.answered() {
			answered = 1
		}
		metrics.ReconcilerBackendInventoryAnswered.WithLabelValues(backendName).Set(answered)
	}
}

// initializeBackendInventoryMetrics creates every configured backend's series
// at zero, so increase() sees the first sweep's increments and a readiness
// gate refuses until a sweep has heard the backend.
func initializeBackendInventoryMetrics(backendNames []string) {
	for _, backendName := range backendNames {
		for _, outcome := range backendInventoryOutcomes {
			metrics.ReconcilerBackendInventoryTotal.WithLabelValues(backendName, outcome.label())
		}
		metrics.ReconcilerBackendInventoryAnswered.WithLabelValues(backendName).Set(0)
	}
}
