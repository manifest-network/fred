package placement

import (
	"context"
	"errors"
	"testing"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/metrics"
)

const releaseTestLease = "00000000-0000-4000-8000-000000001442"

func releaseReaderForTest(t *testing.T, client backend.Backend) *ReconciliationCoordinator {
	t.Helper()
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, store, client.Name())
	base, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(client))
	coordinator, err := reconciliationCoordinatorWithReaderForTest(t, execution, &reconciliationSweepReader{})
	require.NoError(t, err)
	return coordinator
}

func releasesFor(history ...backend.ReleaseInfo) func(context.Context, string) ([]backend.ReleaseInfo, error) {
	return func(context.Context, string) ([]backend.ReleaseInfo, error) {
		return history, nil
	}
}

func TestActiveReleaseManifestSelectsTheHighestActiveVersion(t *testing.T) {
	coordinator := releaseReaderForTest(t, &executionTestBackend{
		name: "backend-a",
		releases: releasesFor(
			backend.ReleaseInfo{Version: 4, Status: backend.ReleaseStatusActive, Manifest: []byte("v4")},
			backend.ReleaseInfo{Version: 2, Status: backend.ReleaseStatusActive, Manifest: []byte("v2")},
			backend.ReleaseInfo{Version: 5, Status: "failed", Manifest: []byte("v5")},
			backend.ReleaseInfo{Version: 3, Status: "superseded", Manifest: []byte("v3")},
		),
	})

	manifest, version, err := coordinator.activeReleaseManifest(t.Context(), "backend-a", releaseTestLease)
	require.NoError(t, err)
	assert.Equal(t, []byte("v4"), manifest)
	assert.Equal(t, 4, version)
}

func TestActiveReleaseManifestReturnsADetachedCopy(t *testing.T) {
	history := []backend.ReleaseInfo{{Version: 1, Status: backend.ReleaseStatusActive, Manifest: []byte("restored")}}
	coordinator := releaseReaderForTest(t, &executionTestBackend{
		name: "backend-a", releases: releasesFor(history...),
	})

	manifest, _, err := coordinator.activeReleaseManifest(t.Context(), "backend-a", releaseTestLease)
	require.NoError(t, err)
	manifest[0] = 'X'
	assert.Equal(t, []byte("restored"), history[0].Manifest)
}

func TestActiveReleaseManifestRefusesWithoutAUsableActiveRelease(t *testing.T) {
	tests := map[string][]backend.ReleaseInfo{
		"no history":           nil,
		"no active release":    {{Version: 1, Status: "superseded", Manifest: []byte("old")}, {Version: 2, Status: "failed", Manifest: []byte("new")}},
		"empty active release": {{Version: 1, Status: backend.ReleaseStatusActive}},
		"empty newest active":  {{Version: 1, Status: backend.ReleaseStatusActive, Manifest: []byte("v1")}, {Version: 2, Status: backend.ReleaseStatusActive}},
	}
	for name, history := range tests {
		t.Run(name, func(t *testing.T) {
			coordinator := releaseReaderForTest(t, &executionTestBackend{
				name: "backend-a", releases: releasesFor(history...),
			})
			manifest, _, err := coordinator.activeReleaseManifest(t.Context(), "backend-a", releaseTestLease)
			require.ErrorIs(t, err, errNoActiveRelease)
			assert.Nil(t, manifest)
		})
	}
}

func TestActiveReleaseManifestPropagatesTheBackendError(t *testing.T) {
	cause := errors.New("backend unavailable")
	coordinator := releaseReaderForTest(t, &executionTestBackend{
		name: "backend-a",
		releases: func(context.Context, string) ([]backend.ReleaseInfo, error) {
			return nil, cause
		},
	})

	_, _, err := coordinator.activeReleaseManifest(t.Context(), "backend-a", releaseTestLease)
	require.ErrorIs(t, err, cause)
	assert.NotErrorIs(t, err, errNoActiveRelease)
}

func TestActiveReleaseManifestRecoversABackendPanic(t *testing.T) {
	counter := metrics.ReconcilerPanicsTotal.WithLabelValues("fetch_releases")
	before := promtestutil.ToFloat64(counter)
	coordinator := releaseReaderForTest(t, &executionTestBackend{
		name: "backend-a",
		releases: func(context.Context, string) ([]backend.ReleaseInfo, error) {
			panic("backend bug")
		},
	})

	manifest, _, err := coordinator.activeReleaseManifest(t.Context(), "backend-a", releaseTestLease)
	require.Error(t, err)
	assert.Nil(t, manifest)
	assert.Equal(t, before+1, promtestutil.ToFloat64(counter))
}

func TestActiveReleaseManifestRefusesAFencedOwner(t *testing.T) {
	coordinator := releaseReaderForTest(t, fencedClientForTest(t, "backend-a"))

	_, _, err := coordinator.activeReleaseManifest(t.Context(), "backend-a", releaseTestLease)
	require.ErrorIs(t, err, backend.ErrBackendFenced)
}

// releaseActionFixture issues observed actions for leases whose chain state
// and placement the test chooses, against one backend recording release reads.
type releaseActionFixture struct {
	store       *Store
	coordinator *ReconciliationCoordinator
	projected   *ProjectedReconciliationSweep
	reads       []string
	provisions  []backend.ProvisionRequest
}

func newReleaseActionFixture(
	t *testing.T,
	leases map[string]billingtypes.Lease,
	confirmed ...string,
) *releaseActionFixture {
	t.Helper()
	fixture := &releaseActionFixture{}
	fixture.store = newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, fixture.store, "backend-a")
	for _, leaseUUID := range confirmed {
		requireConfirmedPlacement(t, fixture.store, leaseUUID, "backend-a")
	}
	client := &overlapActionBackend{unrecordedPositiveInventoryBackend: &unrecordedPositiveInventoryBackend{
		executionTestBackend: &executionTestBackend{
			name: "backend-a",
			provision: func(_ context.Context, request backend.ProvisionRequest) error {
				fixture.provisions = append(fixture.provisions, request)
				return nil
			},
			releases: func(_ context.Context, leaseUUID string) ([]backend.ReleaseInfo, error) {
				fixture.reads = append(fixture.reads, leaseUUID)
				return []backend.ReleaseInfo{{
					Version: 1, Status: backend.ReleaseStatusActive, Manifest: []byte("restored"),
				}}, nil
			},
		},
	}}
	chain := &overlapActionChain{
		reconciliationSweepReader: &reconciliationSweepReader{},
		leases:                    leases,
	}
	base, err := fixture.store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(client))
	fixture.coordinator, err = reconciliationCoordinatorWithReaderForTest(t, execution, chain)
	require.NoError(t, err)
	var sweep *ReconciliationSweep
	sweep, fixture.projected = completeEmptyReconciliationSweep(t, fixture.coordinator)
	t.Cleanup(sweep.End)
	return fixture
}

func (fixture *releaseActionFixture) observe(t *testing.T, leaseUUID string) ObservedReconciliationAction {
	t.Helper()
	action, disposition, err := fixture.projected.ObserveLiveAction(t.Context(), leaseUUID)
	require.NoError(t, err)
	require.Equal(t, ReconciliationObservationReady, disposition)
	require.True(t, action.Valid())
	return action
}

func releaseTestChainLease(leaseUUID string, state billingtypes.LeaseState, metaHash []byte) billingtypes.Lease {
	return billingtypes.Lease{
		Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
		State: state, MetaHash: metaHash,
		Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1, ServiceName: "app"}},
	}
}

func TestObservedActiveReleaseManifestReadsOnlyAnActiveLeasesConfirmedOwner(t *testing.T) {
	const (
		active     = "00000000-0000-4000-8000-000000001443"
		pending    = "00000000-0000-4000-8000-000000001444"
		recordless = "00000000-0000-4000-8000-000000001445"
	)
	fixture := newReleaseActionFixture(t, map[string]billingtypes.Lease{
		active:     releaseTestChainLease(active, billingtypes.LEASE_STATE_ACTIVE, nil),
		pending:    releaseTestChainLease(pending, billingtypes.LEASE_STATE_PENDING, nil),
		recordless: releaseTestChainLease(recordless, billingtypes.LEASE_STATE_ACTIVE, nil),
	}, active, pending)
	coordinator := fixture.coordinator

	for _, leaseUUID := range []string{pending, recordless} {
		action := fixture.observe(t, leaseUUID)
		_, _, err := coordinator.ObservedActiveReleaseManifest(t.Context(), action)
		assert.Error(t, err, "lease %s has no ACTIVE confirmed owner to read from", leaseUUID)
		require.True(t, coordinator.ReleaseAction(action))
	}
	assert.Empty(t, fixture.reads, "neither a PENDING nor a recordless lease reaches a backend")

	action := fixture.observe(t, active)
	manifest, version, err := coordinator.ObservedActiveReleaseManifest(t.Context(), action)
	require.NoError(t, err)
	assert.Equal(t, []byte("restored"), manifest)
	assert.Equal(t, 1, version)
	assert.Equal(t, []string{active}, fixture.reads)

	require.True(t, coordinator.ReleaseAction(action))
	_, _, err = coordinator.ObservedActiveReleaseManifest(t.Context(), action)
	assert.Error(t, err, "a released action no longer authorizes a read")
	assert.Equal(t, []string{active}, fixture.reads)
}

func TestObservedActiveReleaseManifestRefusesAPlacementChangedSinceObservation(t *testing.T) {
	fixture := newReleaseActionFixture(t, map[string]billingtypes.Lease{
		releaseTestLease: releaseTestChainLease(releaseTestLease, billingtypes.LEASE_STATE_ACTIVE, nil),
	}, releaseTestLease)
	action := fixture.observe(t, releaseTestLease)
	requireTypedAttempt(t, fixture.store, releaseTestLease, "backend-a", requireOperationID(t, "1442"))

	_, _, err := fixture.coordinator.ObservedActiveReleaseManifest(t.Context(), action)
	require.ErrorIs(t, err, ErrReconciliationBoundaryStale)
	assert.Empty(t, fixture.reads, "only the live confirmed owner record authorizes the read")
}

func TestReconciliationProvisionRefusesAPayloadBearingLeaseWithoutItsPayload(t *testing.T) {
	metaHash := make([]byte, 32)
	metaHash[0] = 1
	fixture := newReleaseActionFixture(t, map[string]billingtypes.Lease{
		releaseTestLease: releaseTestChainLease(releaseTestLease, billingtypes.LEASE_STATE_ACTIVE, metaHash),
	}, releaseTestLease)
	action := fixture.observe(t, releaseTestLease)

	result := fixture.coordinator.Provision(t.Context(), action, nil, PayloadFingerprint{})
	require.Error(t, result.Err())
	assert.Empty(t, fixture.provisions,
		"a backend refuses a payloadless provision as invalid, which would close the ACTIVE lease")
	assert.Empty(t, fixture.store.Lookup(releaseTestLease).Attempt,
		"no attempt is written for a refused provision")
}
