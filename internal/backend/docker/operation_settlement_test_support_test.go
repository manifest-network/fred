package docker

import (
	"testing"

	"github.com/manifest-network/fred/internal/backend/shared"
)

// commitPreEffectOperationFailureForTest follows the same typed refusal path as
// production. Tests that need an uncommitted-release proof must first prove the
// exact intent is still BeforeEffects; mere ReleaseStore absence is not enough.
func commitPreEffectOperationFailureForTest(
	t testing.TB,
	settlement operationSettlementService,
	claim shared.OperationIntentClaim,
) shared.OperationReleaseUncommitted {
	t.Helper()
	candidate, err := settlement.PrepareOperationRelease(claim)
	if err != nil {
		t.Fatalf("prepare operation refusal: %v", err)
	}
	failure, err := settlement.RefuseOperationExecution(candidate)
	if err != nil {
		t.Fatalf("mint pre-effect operation refusal: %v", err)
	}
	uncommitted, err := settlement.CommitOperationFailure(failure)
	if err != nil {
		t.Fatalf("commit pre-effect operation refusal: %v", err)
	}
	return uncommitted
}
