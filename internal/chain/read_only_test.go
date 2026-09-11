package chain

import (
	"context"
	"errors"
	"testing"

	"github.com/cosmos/cosmos-sdk/types/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/backend"
)

const (
	readOnlyProviderUUID = "01234567-89ab-cdef-0123-456789abcdef"
	readOnlyLeaseOne     = "018f1f78-7e16-7c21-bf3a-4f8921f9c125"
	readOnlyLeaseTwo     = "018f1f78-7e16-7c21-bf3a-4f8921f9c126"
	readOnlyLeaseThree   = "018f1f78-7e16-7c21-bf3a-4f8921f9c127"
	readOnlyLeaseFour    = "018f1f78-7e16-7c21-bf3a-4f8921f9c128"
)

type providerLeasePageResult struct {
	response *billingtypes.QueryLeasesByProviderResponse
	height   int64
	err      error
}

type providerLeasePageCall struct {
	request      billingtypes.QueryLeasesByProviderRequest
	pinnedHeight int64
}

type fakeProviderLeasePageQuerier struct {
	results []providerLeasePageResult
	calls   []providerLeasePageCall
}

func (fake *fakeProviderLeasePageQuerier) LeasesByProviderPage(
	_ context.Context,
	request *billingtypes.QueryLeasesByProviderRequest,
	pinnedHeight int64,
) (*billingtypes.QueryLeasesByProviderResponse, int64, error) {
	requestCopy := *request
	if request.Pagination != nil {
		paginationCopy := *request.Pagination
		paginationCopy.Key = append([]byte(nil), request.Pagination.Key...)
		requestCopy.Pagination = &paginationCopy
	}
	fake.calls = append(fake.calls, providerLeasePageCall{
		request:      requestCopy,
		pinnedHeight: pinnedHeight,
	})
	if len(fake.results) == 0 {
		return nil, 0, errors.New("unexpected query")
	}
	result := fake.results[0]
	fake.results = fake.results[1:]
	return result.response, result.height, result.err
}

func TestReadOnlyClientSnapshotProviderLeasesPinsAllPagesAndFailsClosedOnStates(t *testing.T) {
	t.Parallel()

	pages := &fakeProviderLeasePageQuerier{results: []providerLeasePageResult{
		{
			height: 42,
			response: &billingtypes.QueryLeasesByProviderResponse{
				Leases: []billingtypes.Lease{
					{
						Uuid: readOnlyLeaseOne, ProviderUuid: readOnlyProviderUUID,
						State: billingtypes.LEASE_STATE_CLOSED,
						Items: []billingtypes.LeaseItem{{
							SkuUuid: "sku-a", Quantity: 2, ServiceName: "web",
							CustomDomain: "tenant.example",
						}},
					},
					{Uuid: readOnlyLeaseTwo, ProviderUuid: readOnlyProviderUUID, State: billingtypes.LEASE_STATE_PENDING},
				},
				Pagination: &query.PageResponse{NextKey: []byte("next")},
			},
		},
		{
			height: 42,
			response: &billingtypes.QueryLeasesByProviderResponse{
				Leases: []billingtypes.Lease{
					{Uuid: readOnlyLeaseThree, ProviderUuid: readOnlyProviderUUID, State: billingtypes.LEASE_STATE_ACTIVE},
					{Uuid: readOnlyLeaseFour, ProviderUuid: readOnlyProviderUUID, State: billingtypes.LeaseState(99)},
				},
				Pagination: &query.PageResponse{},
			},
		},
	}}
	client := &ReadOnlyClient{pages: pages, queryPageLimit: 17}

	snapshot, err := client.SnapshotProviderLeases(context.Background(), readOnlyProviderUUID)
	require.NoError(t, err)
	require.True(t, snapshot.Valid())
	assert.Equal(t, readOnlyProviderUUID, snapshot.ProviderUUID())
	assert.Equal(t, int64(42), snapshot.BlockHeight())
	assert.Equal(t, 4, snapshot.TotalLeases())
	assert.Equal(t, []string{
		readOnlyLeaseOne, readOnlyLeaseTwo, readOnlyLeaseThree, readOnlyLeaseFour,
	}, snapshot.LeaseUUIDs())
	assert.Equal(t, []BlockingProviderLease{
		{LeaseUUID: readOnlyLeaseTwo, State: billingtypes.LEASE_STATE_PENDING},
		{LeaseUUID: readOnlyLeaseThree, State: billingtypes.LEASE_STATE_ACTIVE},
		{LeaseUUID: readOnlyLeaseFour, State: billingtypes.LeaseState(99)},
	}, snapshot.BlockingLeases())
	assert.Equal(t, []string{
		readOnlyLeaseTwo, readOnlyLeaseThree, readOnlyLeaseFour,
	}, snapshot.BlockingLeaseUUIDs())

	require.Len(t, pages.calls, 2)
	assert.Equal(t, int64(0), pages.calls[0].pinnedHeight)
	assert.Equal(t, int64(42), pages.calls[1].pinnedHeight)
	for _, call := range pages.calls {
		assert.Equal(t, readOnlyProviderUUID, call.request.ProviderUuid)
		assert.Equal(t, billingtypes.LEASE_STATE_UNSPECIFIED, call.request.StateFilter)
		assert.Equal(t, uint64(17), call.request.Pagination.Limit)
	}
	assert.Empty(t, pages.calls[0].request.Pagination.Key)
	assert.Equal(t, []byte("next"), pages.calls[1].request.Pagination.Key)

	// Accessors must not expose the snapshot's backing storage.
	blocking := snapshot.BlockingLeases()
	blocking[0].LeaseUUID = "mutated"
	assert.Equal(t, readOnlyLeaseTwo, snapshot.BlockingLeases()[0].LeaseUUID)
	blockingLeaseUUIDs := snapshot.BlockingLeaseUUIDs()
	blockingLeaseUUIDs[0] = "mutated"
	assert.Equal(t, readOnlyLeaseTwo, snapshot.BlockingLeaseUUIDs()[0])
	leaseUUIDs := snapshot.LeaseUUIDs()
	leaseUUIDs[0] = "mutated"
	assert.Equal(t, readOnlyLeaseOne, snapshot.LeaseUUIDs()[0])
	assert.Equal(t, []backend.LeaseItem{{
		SKU: "sku-a", Quantity: 2, ServiceName: "web", CustomDomain: "tenant.example",
	}}, snapshot.LeaseItems()[readOnlyLeaseOne])
	leaseItems := snapshot.LeaseItems()
	leaseItems[readOnlyLeaseOne][0].SKU = "mutated"
	delete(leaseItems, readOnlyLeaseTwo)
	assert.Equal(t, "sku-a", snapshot.LeaseItems()[readOnlyLeaseOne][0].SKU)
	assert.Contains(t, snapshot.LeaseItems(), readOnlyLeaseTwo)
}

func TestReadOnlyClientSnapshotProviderLeasesAcceptsTerminalOnlyEmptyProof(t *testing.T) {
	t.Parallel()

	pages := &fakeProviderLeasePageQuerier{results: []providerLeasePageResult{{
		height: 73,
		response: &billingtypes.QueryLeasesByProviderResponse{
			Leases: []billingtypes.Lease{
				{Uuid: readOnlyLeaseOne, ProviderUuid: readOnlyProviderUUID, State: billingtypes.LEASE_STATE_CLOSED},
				{Uuid: readOnlyLeaseTwo, ProviderUuid: readOnlyProviderUUID, State: billingtypes.LEASE_STATE_REJECTED},
				{Uuid: readOnlyLeaseThree, ProviderUuid: readOnlyProviderUUID, State: billingtypes.LEASE_STATE_EXPIRED},
			},
		},
	}}}
	client := &ReadOnlyClient{pages: pages, queryPageLimit: 100}

	snapshot, err := client.SnapshotProviderLeases(context.Background(), readOnlyProviderUUID)
	require.NoError(t, err)
	assert.True(t, snapshot.Valid())
	assert.Empty(t, snapshot.BlockingLeases())
	assert.Equal(t, 3, snapshot.TotalLeases())
	assert.Equal(t, []string{
		readOnlyLeaseOne, readOnlyLeaseTwo, readOnlyLeaseThree,
	}, snapshot.LeaseUUIDs())
}

func TestReadOnlyClientSnapshotProviderLeasesRejectsInconsistentEvidence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		results []providerLeasePageResult
		wantErr string
	}{
		{
			name: "height changes",
			results: []providerLeasePageResult{
				{height: 41, response: &billingtypes.QueryLeasesByProviderResponse{Pagination: &query.PageResponse{NextKey: []byte("next")}}},
				{height: 42, response: &billingtypes.QueryLeasesByProviderResponse{}},
			},
			wantErr: "changed block height",
		},
		{
			name:    "invalid height",
			results: []providerLeasePageResult{{height: 0, response: &billingtypes.QueryLeasesByProviderResponse{}}},
			wantErr: "invalid block height",
		},
		{
			name:    "nil response",
			results: []providerLeasePageResult{{height: 42}},
			wantErr: "nil response",
		},
		{
			name:    "query failure",
			results: []providerLeasePageResult{{err: errors.New("rpc unavailable")}},
			wantErr: "rpc unavailable",
		},
		{
			name: "repeated continuation",
			results: []providerLeasePageResult{
				{height: 42, response: &billingtypes.QueryLeasesByProviderResponse{Pagination: &query.PageResponse{NextKey: []byte("next")}}},
				{height: 42, response: &billingtypes.QueryLeasesByProviderResponse{Pagination: &query.PageResponse{NextKey: []byte("next")}}},
			},
			wantErr: "repeated continuation key",
		},
		{
			name: "duplicate lease",
			results: []providerLeasePageResult{
				{height: 42, response: &billingtypes.QueryLeasesByProviderResponse{
					Leases:     []billingtypes.Lease{{Uuid: readOnlyLeaseOne, ProviderUuid: readOnlyProviderUUID, State: billingtypes.LEASE_STATE_CLOSED}},
					Pagination: &query.PageResponse{NextKey: []byte("next")},
				}},
				{height: 42, response: &billingtypes.QueryLeasesByProviderResponse{
					Leases: []billingtypes.Lease{{Uuid: readOnlyLeaseOne, ProviderUuid: readOnlyProviderUUID, State: billingtypes.LEASE_STATE_CLOSED}},
				}},
			},
			wantErr: "duplicate lease",
		},
		{
			name: "foreign provider",
			results: []providerLeasePageResult{{height: 42, response: &billingtypes.QueryLeasesByProviderResponse{
				Leases: []billingtypes.Lease{{Uuid: readOnlyLeaseOne, ProviderUuid: "other", State: billingtypes.LEASE_STATE_CLOSED}},
			}}},
			wantErr: "for provider",
		},
		{
			name: "noncanonical lease",
			results: []providerLeasePageResult{{height: 42, response: &billingtypes.QueryLeasesByProviderResponse{
				Leases: []billingtypes.Lease{{Uuid: "not-a-uuid", ProviderUuid: readOnlyProviderUUID, State: billingtypes.LEASE_STATE_CLOSED}},
			}}},
			wantErr: "non-canonical lease UUID",
		},
		{
			name: "quantity cannot fit int",
			results: []providerLeasePageResult{{height: 42, response: &billingtypes.QueryLeasesByProviderResponse{
				Leases: []billingtypes.Lease{{
					Uuid: readOnlyLeaseOne, ProviderUuid: readOnlyProviderUUID,
					State: billingtypes.LEASE_STATE_CLOSED,
					Items: []billingtypes.LeaseItem{{SkuUuid: "sku", Quantity: ^uint64(0)}},
				}},
			}}},
			wantErr: "larger than int",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := &ReadOnlyClient{
				pages:          &fakeProviderLeasePageQuerier{results: test.results},
				queryPageLimit: 100,
			}
			_, err := client.SnapshotProviderLeases(context.Background(), readOnlyProviderUUID)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantErr)
		})
	}
}

func TestReadOnlyClientSnapshotProviderLeasesValidatesConstructionAndContext(t *testing.T) {
	t.Parallel()

	var nilClient *ReadOnlyClient
	_, err := nilClient.SnapshotProviderLeases(context.Background(), readOnlyProviderUUID)
	assert.Error(t, err)

	client := &ReadOnlyClient{pages: &fakeProviderLeasePageQuerier{}, queryPageLimit: 100}
	_, err = client.SnapshotProviderLeases(nil, readOnlyProviderUUID) //nolint:staticcheck // verifies nil context rejection
	assert.Error(t, err)
	_, err = client.SnapshotProviderLeases(context.Background(), "")
	assert.Error(t, err)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.SnapshotProviderLeases(canceled, readOnlyProviderUUID)
	assert.ErrorIs(t, err, context.Canceled)

	assert.NoError(t, nilClient.Close())
}
