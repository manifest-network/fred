package placement

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/operation"
	"github.com/manifest-network/fred/internal/provisioner/payload"
)

const (
	admissionSourceLease = "71638ef8-1401-4f14-a355-1ae02afeb35d"
	admissionTargetLease = "81638ef8-1401-4f14-a355-1ae02afeb35d"
)

var admissionPayload = []byte(`{"image":"nginx"}`)

type payloadAdmissionFixture struct {
	store        *Store
	coordinator  *OperationCoordinator
	execution    *ExecutionCoordinator
	payloads     *payload.Store
	admitter     PayloadAdmitter
	restore      *RestoreCoordinator
	restoreCalls *atomic.Int32
}

// newPayloadAdmissionFixture binds an admitter and a restore coordinator the
// way providerd does, from one provision authority and one payload store. The
// source lease is CLOSED with data retained on backend-a; every other lease is
// PENDING. restoreErr is what the backend answers a restore with.
func newPayloadAdmissionFixture(t *testing.T, restoreErr error) payloadAdmissionFixture {
	t.Helper()
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, store, "backend-a")
	projectInventoryForTest(t, store, InventoryProjection{
		Placements: map[string]string{admissionSourceLease: "backend-a"},
	})
	base, err := newOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	var calls atomic.Int32
	client := &executionTestBackend{name: "backend-a", restore: func(context.Context, backend.RestoreRequest) error {
		calls.Add(1)
		return restoreErr
	}}
	execution := bindExecutionForTest(t, base, newExecutionTestRuntime(client))
	setProviderControlPlaneForTest(t, execution, executionLeaseReaderFunc(
		func(_ context.Context, leaseUUID string) (*billingtypes.Lease, error) {
			state := billingtypes.LEASE_STATE_PENDING
			if leaseUUID == admissionSourceLease {
				state = billingtypes.LEASE_STATE_CLOSED
			}
			return &billingtypes.Lease{
				Uuid: leaseUUID, Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
				State: state, Items: []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
			}, nil
		}))
	payloads, err := payload.NewStore(payload.StoreConfig{DBPath: filepath.Join(t.TempDir(), "payloads.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = payloads.Close() })
	provisions, err := execution.ProvisionCoordinatorWithPayloads(nil, payloads)
	require.NoError(t, err)
	restore, err := provisions.RestoreCoordinator(nil)
	require.NoError(t, err)
	return payloadAdmissionFixture{
		store: store, coordinator: base, execution: execution, payloads: payloads,
		admitter: provisions.PayloadAdmitter(), restore: restore, restoreCalls: &calls,
	}
}

func (fixture payloadAdmissionFixture) restoreTarget(t *testing.T) RestoreApplicationResult {
	t.Helper()
	request, err := NewRestoreApplicationRequest(admissionTargetLease, "tenant-test", admissionSourceLease)
	require.NoError(t, err)
	return fixture.restore.ExecuteApplication(t.Context(), request)
}

func (fixture payloadAdmissionFixture) storedPayload(t *testing.T) bool {
	t.Helper()
	stored, err := fixture.payloads.Has(admissionTargetLease)
	require.NoError(t, err)
	return stored
}

// TestPayloadAdmission_RestoreAndUploadExcludeEachOther pins ENG-2117 in both
// orders. A restore deploys the source's retained manifest and never reads a
// payload, so whichever admission takes the lease first, the other is refused
// and no payload is left stored for a restored lease.
func TestPayloadAdmission_RestoreAndUploadExcludeEachOther(t *testing.T) {
	t.Run("upload first", func(t *testing.T) {
		fixture := newPayloadAdmissionFixture(t, nil)
		require.Equal(t, PayloadAdmitted,
			fixture.admitter.Admit(t.Context(), admissionTargetLease, admissionPayload))

		result := fixture.restoreTarget(t)

		assert.Equal(t, RestoreApplicationTargetHasPayload, result.Disposition(), result.Err())
		assert.Zero(t, fixture.restoreCalls.Load(), "a refused restore never reaches the backend")
		assert.Equal(t, StateAbsent, fixture.store.Lookup(admissionTargetLease).State())
		assert.True(t, fixture.storedPayload(t), "the admitted payload stays for its provision")
	})
	t.Run("restore first", func(t *testing.T) {
		fixture := newPayloadAdmissionFixture(t, nil)
		require.Equal(t, RestoreApplicationAccepted, fixture.restoreTarget(t).Disposition())
		require.Equal(t, StateConfirmed, fixture.store.Lookup(admissionTargetLease).State(),
			"an accepted restore confirms its target before the callback")
		require.True(t, fixture.coordinator.RuntimeController().Contains(admissionTargetLease))

		assert.Equal(t, PayloadRestoreOwned,
			fixture.admitter.Admit(t.Context(), admissionTargetLease, admissionPayload))
		assert.False(t, fixture.storedPayload(t))
	})
}

// TestPayloadAdmission_SiblingAuthoritiesShareOnePayloadStore pins the review
// reproduction on #285: exclusion holds only while both admissions consult one
// store, so a second provision authority minted from the same execution cannot
// bring its own, nil included, and a sibling bound to the same store still
// refuses the restore.
func TestPayloadAdmission_SiblingAuthoritiesShareOnePayloadStore(t *testing.T) {
	fixture := newPayloadAdmissionFixture(t, nil)
	require.Equal(t, PayloadAdmitted,
		fixture.admitter.Admit(t.Context(), admissionTargetLease, admissionPayload))
	other, err := payload.NewStore(payload.StoreConfig{DBPath: filepath.Join(t.TempDir(), "other.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = other.Close() })

	for name, payloads := range map[string]*payload.Store{"nil store": nil, "other store": other} {
		_, err := fixture.execution.ProvisionCoordinatorWithPayloads(nil, payloads)
		require.ErrorIs(t, err, ErrPayloadStoreRebound, name)
	}

	sibling, err := fixture.execution.ProvisionCoordinatorWithPayloads(nil, fixture.payloads)
	require.NoError(t, err, "the bound store is accepted again")
	restore, err := sibling.RestoreCoordinator(nil)
	require.NoError(t, err)
	request, err := NewRestoreApplicationRequest(admissionTargetLease, "tenant-test", admissionSourceLease)
	require.NoError(t, err)

	result := restore.ExecuteApplication(t.Context(), request)

	assert.Equal(t, RestoreApplicationTargetHasPayload, result.Disposition(), result.Err())
	assert.Zero(t, fixture.restoreCalls.Load())
	assert.True(t, fixture.storedPayload(t))
}

// TestExecutionCoordinatorBindsNoPayloadStoreOnce pins that an execution bound
// without a payload store never gains one: a restore minted there found no
// store, so an admitter that stores payloads must not join it.
func TestExecutionCoordinatorBindsNoPayloadStoreOnce(t *testing.T) {
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, store, "backend-a")
	base, err := newOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	execution := bindExecutionForTest(t, base, executionRuntime("backend-a"))
	_, err = execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err)
	_, err = execution.ProvisionCoordinatorWithPayloads(nil, nil)
	require.NoError(t, err, "the absent store is accepted again")
	payloads, err := payload.NewStore(payload.StoreConfig{DBPath: filepath.Join(t.TempDir(), "payloads.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = payloads.Close() })

	_, err = execution.ProvisionCoordinatorWithPayloads(nil, payloads)

	require.ErrorIs(t, err, ErrPayloadStoreRebound)
}

// TestPayloadAdmitter_DecidesFromPlacementUnderClaim covers every placement
// state an admission can find once it holds the lease claim, as after a
// restart, when no Registry operation names a restore any more.
func TestPayloadAdmitter_DecidesFromPlacementUnderClaim(t *testing.T) {
	tests := []struct {
		name       string
		prepare    func(*testing.T, payloadAdmissionFixture)
		want       PayloadAdmission
		wantStored bool
	}{
		{
			name: "absent", prepare: func(*testing.T, payloadAdmissionFixture) {},
			want: PayloadAdmitted, wantStored: true,
		},
		{
			// An upload of the same bytes repairs a provision attempt whose
			// payload was lost.
			name: "provision attempt",
			prepare: func(t *testing.T, fixture payloadAdmissionFixture) {
				requireTypedAttempt(t, fixture.store, admissionTargetLease, "backend-a", requireOperationID(t, "9401"))
			},
			want: PayloadAdmitted, wantStored: true,
		},
		{
			// An ambiguous restore keeps its durable attempt but not its
			// Registry operation.
			name: "restore attempt",
			prepare: func(t *testing.T, fixture payloadAdmissionFixture) {
				fixture.restoreTarget(t)
				require.Equal(t, StateAttempting, fixture.store.Lookup(admissionTargetLease).State())
				require.False(t, fixture.coordinator.RuntimeController().Contains(admissionTargetLease))
			},
			want: PayloadRestoreOwned,
		},
		{
			name: "confirmed",
			prepare: func(t *testing.T, fixture payloadAdmissionFixture) {
				projectInventoryForTest(t, fixture.store, InventoryProjection{
					Placements: map[string]string{
						admissionSourceLease: "backend-a", admissionTargetLease: "backend-a",
					},
				})
			},
			want: PayloadLeaseDeployed,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newPayloadAdmissionFixture(t, errors.New("ambiguous restore transport outcome"))
			tt.prepare(t, fixture)

			assert.Equal(t, tt.want, fixture.admitter.Admit(t.Context(), admissionTargetLease, admissionPayload))
			assert.Equal(t, tt.wantStored, fixture.storedPayload(t))
			claim := fixture.coordinator.operations.TryClaimLeaseNow(admissionTargetLease)
			require.True(t, claim.Acquired(), "admission must release the lease claim")
			fixture.coordinator.operations.ReleaseLease(claim.Claim())
		})
	}
}

func TestPayloadAdmitter_RefusesUnusablePlacement(t *testing.T) {
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	requireAdmissionBaseline(t, store, "backend-a", "backend-b")
	projectInventoryForTest(t, store, InventoryProjection{
		Conflicts: map[string][]string{admissionTargetLease: {"backend-a", "backend-b"}},
	})
	require.Equal(t, StateUnusable, store.Lookup(admissionTargetLease).State())
	base, err := newOperationCoordinator(store, operation.NewRegistry())
	require.NoError(t, err)
	payloads, err := payload.NewStore(payload.StoreConfig{DBPath: filepath.Join(t.TempDir(), "payloads.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = payloads.Close() })
	admitter := PayloadAdmitter{coordinator: base, payloads: payloads}

	assert.Equal(t, PayloadPlacementUnusable, admitter.Admit(t.Context(), admissionTargetLease, admissionPayload))
	stored, err := payloads.Has(admissionTargetLease)
	require.NoError(t, err)
	assert.False(t, stored)
}

func TestPayloadAdmitter_BusyClaim(t *testing.T) {
	t.Run("restore operation owns the lease", func(t *testing.T) {
		fixture := newPayloadAdmissionFixture(t, nil)
		claim := fixture.coordinator.operations.TryClaimLeaseNow(admissionTargetLease)
		require.True(t, claim.Acquired())
		// A restore registers its operation before writing its attempt.
		require.True(t, fixture.coordinator.operations.TryInitiateRestoreClaimed(
			claim.Claim(), testRestoreInitiation(t, admissionTargetLease, "tenant-test"),
		).Started())

		assert.Equal(t, PayloadRestoreOwned,
			fixture.admitter.Admit(t.Context(), admissionTargetLease, admissionPayload))
		assert.False(t, fixture.storedPayload(t))
	})
	t.Run("provision of a stored payload", func(t *testing.T) {
		fixture := newPayloadAdmissionFixture(t, nil)
		require.True(t, fixture.payloads.Store(admissionTargetLease, admissionPayload))
		claim := fixture.coordinator.operations.TryClaimLeaseNow(admissionTargetLease)
		require.True(t, claim.Acquired())
		require.True(t, fixture.coordinator.operations.TryInitiateProvisionClaimed(
			claim.Claim(), testProvisionInitiation(t, admissionTargetLease, "tenant-test", "backend-a"),
		).Started())

		assert.Equal(t, PayloadAlreadyStored,
			fixture.admitter.Admit(t.Context(), admissionTargetLease, admissionPayload))
	})
	t.Run("transient claim is waited out", func(t *testing.T) {
		fixture := newPayloadAdmissionFixture(t, nil)
		claim := fixture.coordinator.operations.TryClaimLeaseNow(admissionTargetLease)
		require.True(t, claim.Acquired())
		released := make(chan struct{})
		go func() {
			defer close(released)
			time.Sleep(3 * payloadAdmissionPollInterval)
			fixture.coordinator.operations.ReleaseLease(claim.Claim())
		}()

		assert.Equal(t, PayloadAdmitted,
			fixture.admitter.Admit(t.Context(), admissionTargetLease, admissionPayload))
		<-released
		assert.True(t, fixture.storedPayload(t))
	})
	t.Run("claim held past the deadline", func(t *testing.T) {
		fixture := newPayloadAdmissionFixture(t, nil)
		claim := fixture.coordinator.operations.TryClaimLeaseNow(admissionTargetLease)
		require.True(t, claim.Acquired())
		ctx, cancel := context.WithTimeout(t.Context(), 4*payloadAdmissionPollInterval)
		defer cancel()

		assert.Equal(t, PayloadLeaseUnavailable, fixture.admitter.Admit(ctx, admissionTargetLease, admissionPayload))
		assert.False(t, fixture.storedPayload(t))
	})
	t.Run("draining", func(t *testing.T) {
		fixture := newPayloadAdmissionFixture(t, nil)
		fixture.coordinator.RuntimeController().BeginDrain()

		assert.Equal(t, PayloadLeaseUnavailable,
			fixture.admitter.Admit(t.Context(), admissionTargetLease, admissionPayload))
		assert.False(t, fixture.storedPayload(t))
	})
}

func TestPayloadAdmitter_InvalidAndUnconfigured(t *testing.T) {
	fixture := newPayloadAdmissionFixture(t, nil)
	assert.Equal(t, PayloadAdmissionInvalid,
		PayloadAdmitter{}.Admit(t.Context(), admissionTargetLease, admissionPayload))
	assert.Equal(t, PayloadAdmissionInvalid, fixture.admitter.Admit(t.Context(), "", admissionPayload))
	assert.Equal(t, PayloadAdmissionInvalid, fixture.admitter.Admit(t.Context(), admissionTargetLease, nil))
	unconfigured := PayloadAdmitter{coordinator: fixture.coordinator}
	assert.Equal(t, PayloadNotStored,
		unconfigured.Admit(t.Context(), admissionTargetLease, admissionPayload),
		"without a payload store nothing is stored, so a restore never finds a payload")
}

func TestPayloadAdmitter_ReportsAnExistingPayload(t *testing.T) {
	fixture := newPayloadAdmissionFixture(t, nil)
	require.Equal(t, PayloadAdmitted, fixture.admitter.Admit(t.Context(), admissionTargetLease, admissionPayload))
	assert.Equal(t, PayloadAlreadyStored, fixture.admitter.Admit(t.Context(), admissionTargetLease, admissionPayload))
}

// TestRestoreRefusesTargetWhosePayloadCannotBeRead fails closed: an unreadable
// payload store cannot prove the target holds no payload.
func TestRestoreRefusesTargetWhosePayloadCannotBeRead(t *testing.T) {
	fixture := newPayloadAdmissionFixture(t, nil)
	require.NoError(t, fixture.payloads.Close())

	result := fixture.restoreTarget(t)

	assert.Equal(t, RestoreApplicationServiceUnavailable, result.Disposition())
	require.Error(t, result.Err())
	assert.Zero(t, fixture.restoreCalls.Load())
	assert.Equal(t, StateAbsent, fixture.store.Lookup(admissionTargetLease).State())
}
