package shared

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperationSettlementAdmissionRejectsZeroAndCrossPairCapabilities(t *testing.T) {
	t.Parallel()

	var zero *OperationSettlement
	_, err := zero.NewOperationIntentProbe(
		"44efad2b-35fe-4242-a99e-1f89eb472d75",
		"https://provider.example/callback?operation_id=ea0f1e58-24e6-4f2b-8e62-535325624e78",
	)
	require.ErrorContains(t, err, "invalid")
	_, err = zero.NewOperationIntentCandidate(OperationIntentSpec{})
	require.ErrorContains(t, err, "invalid")
	_, err = zero.BeginOperationIntent(OperationIntentCandidate{})
	require.ErrorContains(t, err, "invalid")
	_, err = zero.ProbeOperationIntent(OperationIntentProbe{})
	require.ErrorContains(t, err, "invalid")
	_, err = zero.ListOperationIntents()
	require.ErrorContains(t, err, "invalid")
	_, err = zero.ListOperationRecoveryStates()
	require.ErrorContains(t, err, "invalid")
	_, err = zero.ListFailedOperationReceipts()
	require.ErrorContains(t, err, "invalid")
	_, err = zero.LookupOperationRecovery(OperationIntentProbe{})
	require.ErrorContains(t, err, "invalid")

	storesA := openOperationHandoffStores(t, "docker-operation-a")
	storesB := openOperationHandoffStores(t, "docker-operation-b")
	spec := testOperationIntentSpec(t, "settlement-admission-cross-pair")
	candidate, err := storesA.settlement.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	_, err = storesB.settlement.BeginOperationIntent(candidate)
	require.ErrorContains(t, err, "another journal pair")

	admission, err := storesA.settlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim := createdOperationClaim(t, admission)
	assert.Equal(t, spec.LeaseUUID, claim.LeaseUUID())

	probe, err := storesA.settlement.NewOperationIntentProbe(spec.LeaseUUID, spec.CallbackURL)
	require.NoError(t, err)
	_, err = storesB.settlement.ProbeOperationIntent(probe)
	require.ErrorContains(t, err, "another journal pair")
	_, err = storesB.settlement.LookupOperationRecovery(probe)
	require.ErrorContains(t, err, "another journal pair")

	disposition, err := storesA.settlement.ProbeOperationIntent(probe)
	require.NoError(t, err)
	assert.Equal(t, OperationIntentAdmissionExisting, disposition)
	recovered, err := storesA.settlement.LookupOperationRecovery(probe)
	require.NoError(t, err)
	assert.Equal(t, claim.OperationID(), recovered.OperationID())
}

func TestOperationSettlementCapabilitiesBindSameCallbackStoreToExactReleaseStore(t *testing.T) {
	t.Parallel()

	stores := openOperationHandoffStores(t, "docker-operation-pair")
	alternateReleases, err := OpenIdentityBoundReleaseStore(
		ReleaseStoreConfig{DBPath: stores.alternateReleasePath}, stores.storage, stores.gate,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, alternateReleases.Close()) })
	alternate, err := NewOperationSettlement(stores.callbacks, alternateReleases)
	require.NoError(t, err)

	spec := testOperationIntentSpec(t, "same-callback-different-release")
	candidate, err := stores.settlement.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	_, err = alternate.BeginOperationIntent(candidate)
	require.ErrorContains(t, err, "another journal pair")

	admission, err := stores.settlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim := createdOperationClaim(t, admission)
	_, err = alternate.PrepareOperationRelease(claim)
	require.ErrorContains(t, err, "another journal pair")

	probe, err := stores.settlement.NewOperationIntentProbe(spec.LeaseUUID, spec.CallbackURL)
	require.NoError(t, err)
	_, err = alternate.ProbeOperationIntent(probe)
	require.ErrorContains(t, err, "another journal pair")
	_, err = alternate.LookupOperationRecovery(probe)
	require.ErrorContains(t, err, "another journal pair")

	// Recovery through the alternate pair must explicitly remint a claim from
	// that pair; merely sharing the callback journal grants no authority.
	recovered, err := alternate.ListOperationIntents()
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	_, err = alternate.PrepareOperationRelease(recovered[0])
	require.NoError(t, err)
}

func TestOperationSettlementCapabilitiesBindExactCoordinatorWithSameStores(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-operation-exact-coordinator")
	other, err := NewOperationSettlement(stores.callbacks, stores.releases)
	require.NoError(t, err)

	spec := testOperationIntentSpec(t, "settlement-same-stores")
	probe, err := stores.settlement.NewOperationIntentProbe(spec.LeaseUUID, spec.CallbackURL)
	require.NoError(t, err)
	_, err = other.ProbeOperationIntent(probe)
	require.ErrorContains(t, err, "another journal pair")
	candidate, err := stores.settlement.NewOperationIntentCandidate(spec)
	require.NoError(t, err)
	_, err = other.BeginOperationIntent(candidate)
	require.ErrorContains(t, err, "another journal pair")

	admission, err := stores.settlement.BeginOperationIntent(candidate)
	require.NoError(t, err)
	claim := createdOperationClaim(t, admission)
	_, err = other.PrepareOperationRelease(claim)
	require.ErrorContains(t, err, "another journal pair")

	releaseCandidate, err := stores.settlement.PrepareOperationRelease(claim)
	require.NoError(t, err)
	require.ErrorContains(t, other.CheckOperationReleaseCapacity(releaseCandidate), "another journal pair")
	_, err = other.StartOperationExecution(releaseCandidate)
	require.ErrorContains(t, err, "another journal pair")

	committed := commitHandoffOperation(t, stores.settlement, releaseCandidate)
	_, err = other.resolveOperationSuccess(committed)
	require.ErrorContains(t, err, "another journal pair")

	failureStores := openOperationHandoffStores(t, "docker-operation-exact-failure")
	failureOther, err := NewOperationSettlement(failureStores.callbacks, failureStores.releases)
	require.NoError(t, err)
	failureClaim := beginHandoffOperation(t, failureStores.settlement,
		testOperationIntentSpec(t, "settlement-same-stores-failure"))
	failureCandidate, err := failureStores.settlement.PrepareOperationRelease(failureClaim)
	require.NoError(t, err)
	failure, err := failureStores.settlement.RefuseOperationExecution(failureCandidate)
	require.NoError(t, err)
	uncommitted, err := failureStores.settlement.CommitOperationFailure(failure)
	require.NoError(t, err)
	_, err = failureOther.resolveOperationFailure(uncommitted, "refused")
	require.ErrorContains(t, err, "another journal pair")
}
