package placement

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type forgedNotFoundReadError struct{}

func (forgedNotFoundReadError) Error() string { return "forged not found" }
func (forgedNotFoundReadError) Is(target error) bool {
	return target == billingtypes.ErrLeaseNotFound
}

func boundProviderControlPlaneForTest(t *testing.T) *boundProviderControlPlane {
	t.Helper()
	store := newTestStore(t, WithCallbackRouteFactory(testCallbackRoutes(t)))
	coordinator, err := store.BindOperationCoordinator(nil)
	require.NoError(t, err)
	execution := bindExecutionForTest(t, coordinator, executionRuntime("backend-a"))
	return execution.controlPlane
}

func TestProviderControlPlaneExactObservationIsSealedAndDeepCloned(t *testing.T) {
	control := boundProviderControlPlaneForTest(t)
	closedAt := time.Unix(123, 456)
	original := &billingtypes.Lease{
		Uuid:         "lease-exact",
		Tenant:       "tenant-test",
		ProviderUuid: freshTestProviderUUID,
		State:        billingtypes.LEASE_STATE_CLOSED,
		Items:        []billingtypes.LeaseItem{{SkuUuid: "sku-test", Quantity: 1}},
		MetaHash:     []byte{1, 2, 3},
		ClosedAt:     &closedAt,
	}
	setProviderControlPlaneForTest(t, control.execution, executionLeaseReaderFunc(func(
		context.Context, string,
	) (*billingtypes.Lease, error) {
		return original, nil
	}))

	observation := control.observeLease(t.Context(), original.Uuid, original.Tenant)
	exact, ok := observation.(observedExactLease)
	require.True(t, ok)
	exact.lease.Items[0].SkuUuid = "mutated"
	exact.lease.MetaHash[0] = 9
	*exact.lease.ClosedAt = time.Time{}
	assert.Equal(t, "sku-test", original.Items[0].SkuUuid)
	assert.Equal(t, byte(1), original.MetaHash[0])
	assert.Equal(t, closedAt, *original.ClosedAt)
}

func TestProviderControlPlaneObservationRejectsAmbiguousAndSplicedResults(t *testing.T) {
	tests := []struct {
		name      string
		lease     *billingtypes.Lease
		err       error
		want      any
		wantErrIs error
	}{
		{
			name: "single wrapped not found is unknown",
			err:  fmt.Errorf("wrapped: %w", billingtypes.ErrLeaseNotFound),
			want: observedLeaseUnknown{}, wantErrIs: billingtypes.ErrLeaseNotFound,
		},
		{
			name: "joined not found and transient is unknown",
			err:  errors.Join(billingtypes.ErrLeaseNotFound, context.DeadlineExceeded),
			want: observedLeaseUnknown{}, wantErrIs: context.DeadlineExceeded,
		},
		{
			name: "custom Is cannot manufacture absence",
			err:  forgedNotFoundReadError{},
			want: observedLeaseUnknown{}, wantErrIs: billingtypes.ErrLeaseNotFound,
		},
		{
			name:  "non nil plus not found is unknown",
			lease: &billingtypes.Lease{Uuid: "lease-exact"},
			err:   billingtypes.ErrLeaseNotFound,
			want:  observedLeaseUnknown{}, wantErrIs: billingtypes.ErrLeaseNotFound,
		},
		{
			name: "nil without error is not found",
			want: observedLeaseNotFound{}, wantErrIs: billingtypes.ErrLeaseNotFound,
		},
		{
			name: "wrong UUID is unknown",
			lease: &billingtypes.Lease{
				Uuid: "lease-other", Tenant: "tenant-test", ProviderUuid: freshTestProviderUUID,
			},
			want: observedLeaseUnknown{},
		},
		{
			name: "wrong provider is positively unauthorized",
			lease: &billingtypes.Lease{
				Uuid: "lease-exact", Tenant: "tenant-test", ProviderUuid: "provider-other",
			},
			want: observedLeaseUnauthorized{},
		},
		{
			name: "wrong tenant is positively unauthorized",
			lease: &billingtypes.Lease{
				Uuid: "lease-exact", Tenant: "tenant-other", ProviderUuid: freshTestProviderUUID,
			},
			want: observedLeaseUnauthorized{},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			control := boundProviderControlPlaneForTest(t)
			setProviderControlPlaneForTest(t, control.execution, executionLeaseReaderFunc(func(
				context.Context, string,
			) (*billingtypes.Lease, error) {
				return test.lease, test.err
			}))
			observation := control.observeLease(t.Context(), "lease-exact", "tenant-test")
			assert.IsType(t, test.want, observation)
			if test.wantErrIs != nil {
				assert.ErrorIs(t, exactLeaseObservationError(observation), test.wantErrIs)
			}
		})
	}
}

func TestProviderControlPlaneInventoryRequiresExactProviderStateAndIdentity(t *testing.T) {
	tests := []struct {
		name   string
		leases []billingtypes.Lease
	}{
		{
			name: "foreign provider",
			leases: []billingtypes.Lease{{
				Uuid: "lease-a", ProviderUuid: "provider-other",
				State: billingtypes.LEASE_STATE_PENDING,
			}},
		},
		{
			name: "wrong state",
			leases: []billingtypes.Lease{{
				Uuid: "lease-a", ProviderUuid: freshTestProviderUUID,
				State: billingtypes.LEASE_STATE_ACTIVE,
			}},
		},
		{
			name: "empty identity",
			leases: []billingtypes.Lease{{
				ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_PENDING,
			}},
		},
		{
			name: "duplicate identity",
			leases: []billingtypes.Lease{
				{Uuid: "lease-a", ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_PENDING},
				{Uuid: "lease-a", ProviderUuid: freshTestProviderUUID, State: billingtypes.LEASE_STATE_PENDING},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			control := boundProviderControlPlaneForTest(t)
			reader := &reconciliationSweepReader{pending: test.leases}
			setProviderControlPlaneForTest(t, control.execution, reader)
			leases, err := control.inventoryLeases(t.Context(), billingtypes.LEASE_STATE_PENDING)
			assert.Nil(t, leases)
			require.Error(t, err)
		})
	}
}
