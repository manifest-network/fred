package provisioner

import (
	"testing"

	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/lifecycle"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/placement"
	restoreapp "github.com/manifest-network/fred/internal/provisioner/restore"
)

// TestFleet_DuplicateResponseCannotAuthorizeUninstalledLifecycleGeneration is
// the cross-layer regression for the duplicate provision/restore window. Each
// subtest crosses the signed HTTP client, operation registry, bbolt placement
// authority, and real inventory projection. The backend already owns an older
// generation, so its 409 cannot prove that it installed the fresh callback
// pair carried by the rejected request.
func TestFleet_DuplicateResponseCannotAuthorizeUninstalledLifecycleGeneration(t *testing.T) {
	t.Run("provision", func(t *testing.T) {
		f := newFleet(t, fleetOptions{backendCount: 1})
		require.NoError(t, f.sweep(), "arm topology-bound admission")

		const lease = "duplicate-provision-target"
		leaseUUID := fleetLeaseUUID(lease)
		f.addLease(lease, billingtypes.LEASE_STATE_PENDING)
		owner := f.backendAt(1)
		oldID := seedFleetTypedGeneration(
			t, owner, lease, f.providerUUID,
			"550e8400-e29b-41d4-a716-446655440010",
		)

		orchestrator, err := NewProvisionOrchestrator(
			f.providerUUID,
			"http://fred.invalid",
			f.router,
			f.tracker.Operations(),
			f.placement,
			nil,
		)
		require.NoError(t, err)
		leaseRecord, err := f.chain.GetLease(t.Context(), leaseUUID)
		require.NoError(t, err)
		require.NotNil(t, leaseRecord)

		err = startTestProvisioning(t, orchestrator, t.Context(), leaseRecord, ProvisionOpts{})
		require.ErrorIs(t, err, backend.ErrAlreadyProvisioned)
		require.Equal(t, 1, owner.provisionCount(lease))
		request, ok := owner.provisionRequest(lease)
		require.True(t, ok)
		freshID := requireRequestLifecycleGeneration(t, request.CallbackURL, request.LifecycleCallbackURL)
		require.NotEqual(t, oldID, freshID)
		require.Equal(t, placement.StateAttempting, f.placement.Lookup(leaseUUID).State())

		require.NoError(t, f.sweep())
		assertDuplicateGenerationFenced(t, f, owner, leaseUUID, oldID, freshID)
	})

	t.Run("restore", func(t *testing.T) {
		f := newFleet(t, fleetOptions{backendCount: 1})
		const (
			source = "duplicate-restore-source"
			target = "duplicate-restore-target"
		)
		sourceUUID := fleetLeaseUUID(source)
		targetUUID := fleetLeaseUUID(target)
		owner := f.backendAt(1)

		f.addLease(source, billingtypes.LEASE_STATE_CLOSED, "sku-restore")
		owner.seedRetention(source)
		require.NoError(t, f.sweep(), "establish the retained source's exact backend")
		f.assertPlacementPinned(source, owner.name)

		// The target appears on the backend after Fred's inventory boundary but
		// before restore dispatch: precisely the race which produces the coded 409.
		f.addLease(target, billingtypes.LEASE_STATE_PENDING, "sku-restore")
		oldID := seedFleetTypedGeneration(
			t, owner, target, f.providerUUID,
			"550e8400-e29b-41d4-a716-446655440020",
		)

		service, err := restoreapp.NewService(restoreapp.Config{
			ProviderUUID: f.providerUUID,
			CallbackURL: func(id operation.OperationID) (string, error) {
				return BuildCallbackURLForOperation("http://fred.invalid", id)
			},
			Leases:     f.chain,
			Backends:   restoreapp.BackendResolverFunc(f.resolveRestoreBackend),
			Operations: f.tracker.Operations(),
			Authority:  f.placement,
		})
		require.NoError(t, err)

		result := service.Execute(t.Context(), restoreapp.Command{
			TargetLeaseUUID: targetUUID,
			Tenant:          "tenant-1",
			SourceLeaseUUID: sourceUUID,
		})
		require.Equal(t, restoreapp.OutcomeAlreadyProvisioned, result.Outcome)
		require.Equal(t, 1, owner.restoreCount(target))
		request, ok := owner.restoreRequest(target)
		require.True(t, ok)
		freshID := requireRequestLifecycleGeneration(t, request.CallbackURL, request.LifecycleCallbackURL)
		require.NotEqual(t, oldID, freshID)
		require.Equal(t, placement.StateAttempting, f.placement.Lookup(targetUUID).State())

		require.NoError(t, f.sweep())
		assertDuplicateGenerationFenced(t, f, owner, targetUUID, oldID, freshID)
	})
}

func seedFleetTypedGeneration(
	t *testing.T,
	owner *fakeBackendServer,
	leaseUUID, providerUUID, operationText string,
) lifecycle.ID {
	t.Helper()
	operationID, err := operation.ParseID(operationText)
	require.NoError(t, err)
	callbackURL, err := BuildCallbackURLForOperation("http://old-fred.invalid", operationID)
	require.NoError(t, err)
	lifecycleCallbackURL, err := backend.ResolveLifecycleCallbackURL(callbackURL, "")
	require.NoError(t, err)
	owner.seedProvisionWithCallbacks(
		t,
		leaseUUID,
		providerUUID,
		backend.ProvisionStatusReady,
		callbackURL,
		lifecycleCallbackURL,
	)
	id, err := lifecycle.FromOperationID(operationID)
	require.NoError(t, err)
	return id
}

func requireRequestLifecycleGeneration(
	t *testing.T,
	callbackURL, lifecycleCallbackURL string,
) lifecycle.ID {
	t.Helper()
	observation := backend.ObserveLifecycleGeneration(callbackURL, lifecycleCallbackURL)
	require.Equal(t, backend.LifecycleGenerationTyped, observation.Kind)
	id, err := lifecycle.ParseID(observation.ID)
	require.NoError(t, err)
	return id
}

func assertDuplicateGenerationFenced(
	t *testing.T,
	f *fleet,
	owner *fakeBackendServer,
	leaseUUID string,
	oldID, freshID lifecycle.ID,
) {
	t.Helper()

	record := f.placement.Lookup(leaseUUID)
	require.Equal(t, owner.name, record.Backend,
		"positive inventory may establish the backend that actually owns the lease")
	require.Equal(t, owner.name, record.Attempt,
		"the rejected fresh generation must remain an unresolved exact attempt")
	require.Equal(t, freshID.String(), record.AttemptOperationID().String())
	require.Equal(t, placement.LifecycleVerdictAuthorized,
		f.placement.AuthorizeLifecycle(leaseUUID, oldID).Verdict())
	require.Equal(t, placement.LifecycleVerdictStale,
		f.placement.AuthorizeLifecycle(leaseUUID, freshID).Verdict(),
		"a 409 plus positive inventory must not authorize a generation the backend never installed")

	installed := owner.lifecycleObservation(leaseUUID)
	require.NotNil(t, installed)
	require.Equal(t, backend.LifecycleGenerationTyped, installed.Kind)
	require.Equal(t, oldID.String(), installed.ID)
	require.NotEqual(t, freshID.String(), installed.ID)

	// The fence is durable rather than a process-local coincidence.
	f.restartReconciler()
	record = f.placement.Lookup(leaseUUID)
	require.Equal(t, owner.name, record.Backend)
	require.Equal(t, owner.name, record.Attempt)
	require.Equal(t, freshID.String(), record.AttemptOperationID().String())
	require.Equal(t, placement.LifecycleVerdictAuthorized,
		f.placement.AuthorizeLifecycle(leaseUUID, oldID).Verdict())
	require.Equal(t, placement.LifecycleVerdictStale,
		f.placement.AuthorizeLifecycle(leaseUUID, freshID).Verdict())
}
