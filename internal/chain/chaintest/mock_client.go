// Package chaintest provides test doubles for the chain package.
//
// This package lives outside of internal/chain so the production build does
// not link mock implementations into providerd.
package chaintest

import (
	"context"

	sdktypes "github.com/cosmos/cosmos-sdk/types"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

// MockClient is a mock implementation of chain client methods for testing.
type MockClient struct {
	GetLeaseFunc                  func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	GetActiveLeaseFunc            func(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error)
	GetPendingLeasesFunc          func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
	GetActiveLeasesByProviderFunc func(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
	AcknowledgeLeasesFunc         func(ctx context.Context, leaseUUIDs []string) (uint64, []string, error)
	RejectLeasesFunc              func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
	CloseLeasesFunc               func(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
	WithdrawByProviderFunc        func(ctx context.Context, providerUUID string) (string, error)
	GetProviderWithdrawableFunc   func(ctx context.Context, providerUUID string) (sdktypes.Coins, error)
	GetCreditAccountFunc          func(ctx context.Context, tenant string) (*billingtypes.CreditAccount, sdktypes.Coins, error)
	PingFunc                      func(ctx context.Context) error
}

// GetLease calls the mock function if set, otherwise returns nil.
func (m *MockClient) GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	if m.GetLeaseFunc != nil {
		return m.GetLeaseFunc(ctx, leaseUUID)
	}
	return nil, nil
}

// GetActiveLease calls the mock function if set, otherwise returns nil.
func (m *MockClient) GetActiveLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	if m.GetActiveLeaseFunc != nil {
		return m.GetActiveLeaseFunc(ctx, leaseUUID)
	}
	return nil, nil
}

// GetPendingLeases calls the mock function if set, otherwise returns empty slice.
func (m *MockClient) GetPendingLeases(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
	if m.GetPendingLeasesFunc != nil {
		return m.GetPendingLeasesFunc(ctx, providerUUID)
	}
	return nil, nil
}

// GetActiveLeasesByProvider calls the mock function if set, otherwise returns empty slice.
func (m *MockClient) GetActiveLeasesByProvider(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
	if m.GetActiveLeasesByProviderFunc != nil {
		return m.GetActiveLeasesByProviderFunc(ctx, providerUUID)
	}
	return nil, nil
}

// AcknowledgeLeases calls the mock function if set, otherwise returns 0.
func (m *MockClient) AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
	if m.AcknowledgeLeasesFunc != nil {
		return m.AcknowledgeLeasesFunc(ctx, leaseUUIDs)
	}
	return 0, nil, nil
}

// RejectLeases calls the mock function if set, otherwise returns 0.
func (m *MockClient) RejectLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
	if m.RejectLeasesFunc != nil {
		return m.RejectLeasesFunc(ctx, leaseUUIDs, reason)
	}
	return 0, nil, nil
}

// CloseLeases calls the mock function if set, otherwise returns 0.
func (m *MockClient) CloseLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
	if m.CloseLeasesFunc != nil {
		return m.CloseLeasesFunc(ctx, leaseUUIDs, reason)
	}
	return 0, nil, nil
}

// WithdrawByProvider calls the mock function if set, otherwise returns empty string.
func (m *MockClient) WithdrawByProvider(ctx context.Context, providerUUID string) (string, error) {
	if m.WithdrawByProviderFunc != nil {
		return m.WithdrawByProviderFunc(ctx, providerUUID)
	}
	return "", nil
}

// GetProviderWithdrawable calls the mock function if set, otherwise returns nil.
func (m *MockClient) GetProviderWithdrawable(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
	if m.GetProviderWithdrawableFunc != nil {
		return m.GetProviderWithdrawableFunc(ctx, providerUUID)
	}
	return nil, nil
}

// GetCreditAccount calls the mock function if set, otherwise returns nil.
func (m *MockClient) GetCreditAccount(ctx context.Context, tenant string) (*billingtypes.CreditAccount, sdktypes.Coins, error) {
	if m.GetCreditAccountFunc != nil {
		return m.GetCreditAccountFunc(ctx, tenant)
	}
	return nil, nil, nil
}

// Ping calls the mock function if set, otherwise returns nil.
func (m *MockClient) Ping(ctx context.Context) error {
	if m.PingFunc != nil {
		return m.PingFunc(ctx)
	}
	return nil
}

// NewMockLease creates a mock lease for testing.
func NewMockLease(uuid, tenant, providerUUID string, state billingtypes.LeaseState) *billingtypes.Lease {
	return &billingtypes.Lease{
		Uuid:         uuid,
		Tenant:       tenant,
		ProviderUuid: providerUUID,
		State:        state,
	}
}

// NewMockLeaseWithSKU creates a mock lease with SKU items for testing.
func NewMockLeaseWithSKU(uuid, tenant, providerUUID string, state billingtypes.LeaseState, skuUUIDs ...string) *billingtypes.Lease {
	lease := NewMockLease(uuid, tenant, providerUUID, state)
	for _, skuUUID := range skuUUIDs {
		lease.Items = append(lease.Items, billingtypes.LeaseItem{
			SkuUuid:  skuUUID,
			Quantity: 1,
		})
	}
	return lease
}
