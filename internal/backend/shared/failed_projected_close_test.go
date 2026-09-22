package shared

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
)

func TestProjectedCloseConsumesFailedOperationAfterReopen(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	spec := testOperationIntentSpec(t, "failed-projected-close-reopen")
	operation := beginHandoffOperation(t, stores.settlement, spec)
	failure, err := stores.settlement.resolveOperationFailure(commitHandoffRefusal(t, stores.settlement, operation), "registry authentication required")
	require.NoError(t, err)
	require.NoError(t, stores.callbacks.removeEntry(failure), "the durable head survives outbox acknowledgment")
	require.NoError(t, stores.callbacks.Close())
	require.NoError(t, stores.releases.Close())
	require.NoError(t, stores.retentions.Close())
	stores.callbacks, err = OpenIdentityBoundCallbackStore(CallbackStoreConfig{DBPath: stores.callbackPath}, stores.storage, stores.gate)
	require.NoError(t, err)
	stores.releases, err = OpenIdentityBoundReleaseStore(ReleaseStoreConfig{DBPath: stores.releasePath}, stores.storage, stores.gate)
	require.NoError(t, err)
	stores.retentions, err = OpenIdentityBoundRetentionStore(RetentionStoreConfig{DBPath: stores.retentionPath}, stores.storage, stores.gate)
	require.NoError(t, err)
	settlement := newCloseSettlementForTest(t, stores)
	request, err := settlement.NewCloseRequest(spec.LeaseUUID, true)
	require.NoError(t, err)
	admission, err := settlement.BeginClose(request)
	require.NoError(t, err)
	claim := admission.Claim()
	require.False(t, admission.OperationPreempted(), "do not invent a second failure")
	require.False(t, claim.CleanupOnly())
	require.True(t, claim.RetainOnClose())
	require.Equal(t, operation.Tenant(), claim.Tenant())
	require.Equal(t, operation.ProviderUUID(), claim.ProviderUUID())
	require.Equal(t, operation.CallbackURL(), claim.CallbackURL())
	require.Equal(t, operation.LifecycleCallbackURL(), claim.LifecycleCallbackURL())
	require.Equal(t, operation.EffectiveItems(), claim.Items())
	require.Equal(t, operation.ResourceProfiles(), claim.ResourceProfiles())
	require.Equal(t, operation.Manifest(), claim.Manifest())
	require.Equal(t, operation.OperationID(), claim.InterruptedOperationID())
	require.Zero(t, claim.ActiveReleaseVersion())
	_, err = completeDestroyedForTest(settlement, claim)
	require.NoError(t, err)
	pending, err := stores.callbacks.ListPending()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, backend.CallbackStatusDeprovisioned, pending[0].Status)
}

func TestProjectedFailedCloseWitnessRejectsDivergentAuthority(t *testing.T) {
	stores := openOperationHandoffStores(t, "docker-a")
	foreign := openOperationHandoffStores(t, "docker-b")
	spec := testOperationIntentSpec(t, "failed-projected-close-properties")
	operation := beginHandoffOperation(t, stores.settlement, spec)
	_, err := stores.settlement.resolveOperationFailure(commitHandoffRefusal(t, stores.settlement, operation), "image pull failed")
	require.NoError(t, err)
	settlement := newCloseSettlementForTest(t, stores)
	derivation, err := settlement.deriveCloseSpecLocked(spec.LeaseUUID, false, false)
	require.NoError(t, err)
	projected, ok := derivation.(failedOperationProjectedCloseIntentDerivation)
	require.True(t, ok)
	for _, tc := range []struct {
		name   string
		mutate func(*closeIntentSpec, *failedOperationProjectedClose)
	}{
		{"zero witness", func(_ *closeIntentSpec, p *failedOperationProjectedClose) { *p = failedOperationProjectedClose{} }},
		{"foreign issuer", func(_ *closeIntentSpec, p *failedOperationProjectedClose) { p.absence.callbacks = foreign.callbacks }},
		{"foreign release store", func(_ *closeIntentSpec, p *failedOperationProjectedClose) { p.absence.releases = foreign.releases }},
		{"different attempt", func(_ *closeIntentSpec, p *failedOperationProjectedClose) { p.absence.successorDigest[0] ^= 1 }},
		{"different lease", func(s *closeIntentSpec, _ *failedOperationProjectedClose) {
			s.LeaseUUID = testOperationIntentSpec(t, "another-lease").LeaseUUID
		}},
		{"different tenant", func(s *closeIntentSpec, _ *failedOperationProjectedClose) { s.Tenant += "-foreign" }},
		{"different provider", func(s *closeIntentSpec, _ *failedOperationProjectedClose) {
			s.ProviderUUID = "ad6eb97c-85da-42a1-87f8-703e5e071f51"
		}},
		{"different callback", func(s *closeIntentSpec, _ *failedOperationProjectedClose) {
			s.CallbackURL = testOperationIntentSpec(t, "another-callback").CallbackURL
		}},
		{"different lifecycle callback", func(s *closeIntentSpec, _ *failedOperationProjectedClose) { s.LifecycleCallbackURL += "/foreign" }},
		{"different topology", func(s *closeIntentSpec, _ *failedOperationProjectedClose) { s.Items[0].Quantity++ }},
		{"missing topology", func(s *closeIntentSpec, _ *failedOperationProjectedClose) { s.Items = nil }},
		{"different sizing", func(s *closeIntentSpec, _ *failedOperationProjectedClose) { s.ResourceProfiles[0].MemoryMB++ }},
		{"different manifest", func(s *closeIntentSpec, _ *failedOperationProjectedClose) { s.Manifest = append(s.Manifest, ' ') }},
		{"cleanup escalation", func(s *closeIntentSpec, _ *failedOperationProjectedClose) {
			s.CleanupOnly = true
			s.Tenant = ""
			s.ProviderUUID = ""
			s.CallbackURL = ""
			s.LifecycleCallbackURL = ""
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidateSpec, proof := cloneCloseIntentSpec(projected.value), projected.authority
			tc.mutate(&candidateSpec, &proof)
			candidate, err := newCloseIntentCandidate(stores.callbacks, candidateSpec, stores.callbacks.binding.backendName, stores.callbacks.binding.storageID)
			if err == nil {
				_, err = stores.callbacks.beginProjectedCloseAfterFailedOperationLocked(candidate, proof)
			}
			require.Error(t, err)
			_, found, err := stores.callbacks.currentOperationHeadLocked(spec.LeaseUUID)
			require.NoError(t, err)
			require.True(t, found, "refusal must preserve the failed head")
		})
	}
}
