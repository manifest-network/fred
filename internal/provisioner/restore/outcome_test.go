package restore

import (
	"maps"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/provisioner/placement"
	"github.com/manifest-network/fred/internal/testutil"
)

// TestOutcomeForDisposition_EveryDeclaredDisposition requires a deliberate
// projection for every restore disposition placement declares. Before
// ENG-2161 an unlisted disposition fell through to a silent, retryable 503.
// Now a new one fails this test, and an undeclared value is an internal
// failure the transport logs.
func TestOutcomeForDisposition_EveryDeclaredDisposition(t *testing.T) {
	decisions := map[string]struct {
		disposition placement.RestoreApplicationDisposition
		outcome     Outcome
	}{
		"RestoreApplicationInvalid":              {placement.RestoreApplicationInvalid, OutcomeInvalid},
		"RestoreApplicationTargetNotPending":     {placement.RestoreApplicationTargetNotPending, OutcomeTargetNotPending},
		"RestoreApplicationSourceNotFound":       {placement.RestoreApplicationSourceNotFound, OutcomeSourceNotFound},
		"RestoreApplicationSourceUnavailable":    {placement.RestoreApplicationSourceUnavailable, OutcomeSourceUnavailable},
		"RestoreApplicationSourceBusy":           {placement.RestoreApplicationSourceBusy, OutcomeSourceBusy},
		"RestoreApplicationTargetBusy":           {placement.RestoreApplicationTargetBusy, OutcomeTargetBusy},
		"RestoreApplicationServiceUnavailable":   {placement.RestoreApplicationServiceUnavailable, OutcomeServiceUnavailable},
		"RestoreApplicationNotRetained":          {placement.RestoreApplicationNotRetained, OutcomeNotRetained},
		"RestoreApplicationSourceLost":           {placement.RestoreApplicationSourceLost, OutcomeSourceLost},
		"RestoreApplicationTargetBillingOnly":    {placement.RestoreApplicationTargetBillingOnly, OutcomeTargetBillingOnly},
		"RestoreApplicationTargetHasPayload":     {placement.RestoreApplicationTargetHasPayload, OutcomeTargetHasPayload},
		"RestoreApplicationInventoryReconciling": {placement.RestoreApplicationInventoryReconciling, OutcomeInventoryReconciling},
		// Execute projects these two itself, before this table: acceptance has
		// no failure outcome, and a backend refusal is classified from the
		// sealed refusal. Reaching the table with either is a wiring fault.
		"RestoreApplicationAccepted":        {placement.RestoreApplicationAccepted, OutcomeInternalFailure},
		"RestoreApplicationBackendRejected": {placement.RestoreApplicationBackendRejected, OutcomeInternalFailure},
	}
	declared := testutil.DeclaredConstantNames(t,
		filepath.Join("..", "placement", "restore_coordinator.go"), "RestoreApplicationDisposition")
	require.ElementsMatch(t, declared, slices.Collect(maps.Keys(decisions)),
		"decide the restore outcome for every declared disposition")
	for name, decision := range decisions {
		assert.Equal(t, decision.outcome, outcomeForDisposition(decision.disposition), name)
	}
	assert.Equal(t, OutcomeInternalFailure, outcomeForDisposition(placement.RestoreApplicationDisposition(255)),
		"an undeclared disposition fails closed")
}
