package chain

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"cosmossdk.io/math"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/authz"
	banktypes "github.com/cosmos/cosmos-sdk/x/bank/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// mockAuthzQuerier implements authzQuerier for tests.
type mockAuthzQuerier struct {
	grantsFn func(ctx context.Context, in *authz.QueryGrantsRequest, opts ...grpc.CallOption) (*authz.QueryGrantsResponse, error)
}

func (m *mockAuthzQuerier) Grants(ctx context.Context, in *authz.QueryGrantsRequest, opts ...grpc.CallOption) (*authz.QueryGrantsResponse, error) {
	if m.grantsFn != nil {
		return m.grantsFn(ctx, in, opts...)
	}
	return &authz.QueryGrantsResponse{}, nil
}

// mockBankQuerier implements bankQuerier for tests.
type mockBankQuerier struct {
	balanceFn func(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error)
}

func (m *mockBankQuerier) Balance(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error) {
	if m.balanceFn != nil {
		return m.balanceFn(ctx, in, opts...)
	}
	return &banktypes.QueryBalanceResponse{
		Balance: &sdk.Coin{Denom: "umfx", Amount: math.NewInt(100_000_000)},
	}, nil
}

// mockTxBroadcaster implements txBroadcaster for tests.
type mockTxBroadcaster struct {
	broadcastTxFn        func(ctx context.Context, msg sdk.Msg) (string, error)
	broadcastMultiMsgFn  func(ctx context.Context, msgs []sdk.Msg) (string, error)
}

func (m *mockTxBroadcaster) broadcastTx(ctx context.Context, msg sdk.Msg) (string, error) {
	if m.broadcastTxFn != nil {
		return m.broadcastTxFn(ctx, msg)
	}
	return "tx-hash", nil
}

func (m *mockTxBroadcaster) broadcastMultiMsgTx(ctx context.Context, msgs []sdk.Msg) (string, error) {
	if m.broadcastMultiMsgFn != nil {
		return m.broadcastMultiMsgFn(ctx, msgs)
	}
	return "tx-hash", nil
}

func TestEnsureGrants_AllGrantsMissing(t *testing.T) {
	pool := newTestSignerPool(t, 3)
	var broadcastCount atomic.Int32

	authzQ := &mockAuthzQuerier{} // returns empty grants (all missing)
	broadcaster := &mockTxBroadcaster{
		broadcastMultiMsgFn: func(ctx context.Context, msgs []sdk.Msg) (string, error) {
			broadcastCount.Add(1)
			// 3 sub-signers × 3 msg types = 9 grants
			assert.Len(t, msgs, 9)
			return "tx-hash", nil
		},
	}

	err := EnsureGrants(t.Context(), authzQ, broadcaster, pool)
	require.NoError(t, err)
	assert.Equal(t, int32(1), broadcastCount.Load(), "should batch all grants in one tx")
}

func TestEnsureGrants_AllGrantsExist(t *testing.T) {
	pool := newTestSignerPool(t, 3)
	var broadcastCount atomic.Int32

	authzQ := &mockAuthzQuerier{
		grantsFn: func(ctx context.Context, in *authz.QueryGrantsRequest, opts ...grpc.CallOption) (*authz.QueryGrantsResponse, error) {
			// All grants exist
			return &authz.QueryGrantsResponse{
				Grants: []*authz.Grant{{}},
			}, nil
		},
	}
	broadcaster := &mockTxBroadcaster{
		broadcastMultiMsgFn: func(ctx context.Context, msgs []sdk.Msg) (string, error) {
			broadcastCount.Add(1)
			return "tx-hash", nil
		},
	}

	err := EnsureGrants(t.Context(), authzQ, broadcaster, pool)
	require.NoError(t, err)
	assert.Equal(t, int32(0), broadcastCount.Load(), "should not broadcast when all grants exist")
}

func TestEnsureGrants_BatchFails_FallbackToIndividual(t *testing.T) {
	pool := newTestSignerPool(t, 1)
	var individualCount atomic.Int32

	authzQ := &mockAuthzQuerier{} // all missing
	broadcaster := &mockTxBroadcaster{
		broadcastMultiMsgFn: func(ctx context.Context, msgs []sdk.Msg) (string, error) {
			return "", errors.New("batch failed")
		},
		broadcastTxFn: func(ctx context.Context, msg sdk.Msg) (string, error) {
			individualCount.Add(1)
			return "tx-hash", nil
		},
	}

	err := EnsureGrants(t.Context(), authzQ, broadcaster, pool)
	require.NoError(t, err)
	assert.Equal(t, int32(3), individualCount.Load(), "should fall back to 3 individual grants")
}

func TestEnsureGrants_NoSubSigners(t *testing.T) {
	pool := newTestSignerPool(t, 0)

	err := EnsureGrants(t.Context(), &mockAuthzQuerier{}, &mockTxBroadcaster{}, pool)
	require.NoError(t, err) // no-op
}

func TestEnsureFunding_SubSignersBelowMinimum(t *testing.T) {
	pool := newTestSignerPool(t, 3)
	var broadcastCount atomic.Int32
	subAddrs := pool.SubSignerAddresses()

	bankQ := &mockBankQuerier{
		balanceFn: func(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error) {
			if in.Address == pool.ProviderAddress() {
				return &banktypes.QueryBalanceResponse{
					Balance: &sdk.Coin{Denom: "umfx", Amount: math.NewInt(500_000_000)},
				}, nil
			}
			// Sub-signers 1 and 2 are below minimum, sub-signer 3 is funded
			if in.Address == subAddrs[0] || in.Address == subAddrs[1] {
				return &banktypes.QueryBalanceResponse{
					Balance: &sdk.Coin{Denom: "umfx", Amount: math.NewInt(1_000)},
				}, nil
			}
			return &banktypes.QueryBalanceResponse{
				Balance: &sdk.Coin{Denom: "umfx", Amount: math.NewInt(50_000_000)},
			}, nil
		},
	}
	broadcaster := &mockTxBroadcaster{
		broadcastMultiMsgFn: func(ctx context.Context, msgs []sdk.Msg) (string, error) {
			broadcastCount.Add(1)
			assert.Len(t, msgs, 2, "should fund 2 sub-signers")
			return "tx-hash", nil
		},
	}

	minBalance := sdk.NewCoin("umfx", math.NewInt(10_000_000))
	topUp := sdk.NewCoin("umfx", math.NewInt(50_000_000))

	err := EnsureFunding(t.Context(), bankQ, broadcaster, pool, minBalance, topUp)
	require.NoError(t, err)
	assert.Equal(t, int32(1), broadcastCount.Load())
}

func TestEnsureFunding_AllSubSignersFunded(t *testing.T) {
	pool := newTestSignerPool(t, 3)
	var broadcastCount atomic.Int32

	bankQ := &mockBankQuerier{} // default returns 100 MFX for all
	broadcaster := &mockTxBroadcaster{
		broadcastMultiMsgFn: func(ctx context.Context, msgs []sdk.Msg) (string, error) {
			broadcastCount.Add(1)
			return "tx-hash", nil
		},
	}

	minBalance := sdk.NewCoin("umfx", math.NewInt(10_000_000))
	topUp := sdk.NewCoin("umfx", math.NewInt(50_000_000))

	err := EnsureFunding(t.Context(), bankQ, broadcaster, pool, minBalance, topUp)
	require.NoError(t, err)
	assert.Equal(t, int32(0), broadcastCount.Load(), "no funding needed")
}

func TestEnsureFunding_ProviderInsufficientBalance(t *testing.T) {
	pool := newTestSignerPool(t, 3)

	bankQ := &mockBankQuerier{
		balanceFn: func(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error) {
			if in.Address == pool.ProviderAddress() {
				return &banktypes.QueryBalanceResponse{
					Balance: &sdk.Coin{Denom: "umfx", Amount: math.NewInt(100)}, // very low
				}, nil
			}
			return &banktypes.QueryBalanceResponse{
				Balance: &sdk.Coin{Denom: "umfx", Amount: math.NewInt(0)}, // sub-signers empty
			}, nil
		},
	}
	broadcaster := &mockTxBroadcaster{}

	minBalance := sdk.NewCoin("umfx", math.NewInt(10_000_000))
	topUp := sdk.NewCoin("umfx", math.NewInt(50_000_000))

	err := EnsureFunding(t.Context(), bankQ, broadcaster, pool, minBalance, topUp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "insufficient")
}

func TestEnsureGrants_AuthzNotFoundTreatedAsMissing(t *testing.T) {
	// The authz module returns an error (not empty grants) when no grant exists.
	// grantExists must treat "authorization not found" as false, not as an error.
	pool := newTestSignerPool(t, 1)
	var broadcastCount atomic.Int32

	authzQ := &mockAuthzQuerier{
		grantsFn: func(ctx context.Context, in *authz.QueryGrantsRequest, opts ...grpc.CallOption) (*authz.QueryGrantsResponse, error) {
			return nil, errors.New("codespace authz code 2: authorization not found: authorization not found for /liftedinit.billing.v1.MsgAcknowledgeLease type")
		},
	}
	broadcaster := &mockTxBroadcaster{
		broadcastMultiMsgFn: func(ctx context.Context, msgs []sdk.Msg) (string, error) {
			broadcastCount.Add(1)
			return "tx-hash", nil
		},
	}

	err := EnsureGrants(t.Context(), authzQ, broadcaster, pool)
	require.NoError(t, err)
	assert.Equal(t, int32(1), broadcastCount.Load(), "should create grants when authz returns not-found error")
}

func TestEnsureGrants_QueryError(t *testing.T) {
	pool := newTestSignerPool(t, 1)

	authzQ := &mockAuthzQuerier{
		grantsFn: func(ctx context.Context, in *authz.QueryGrantsRequest, opts ...grpc.CallOption) (*authz.QueryGrantsResponse, error) {
			return nil, errors.New("authz query unavailable")
		},
	}
	broadcaster := &mockTxBroadcaster{}

	err := EnsureGrants(t.Context(), authzQ, broadcaster, pool)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to query grant")
}

func TestEnsureFunding_BatchFails_FallbackToIndividual(t *testing.T) {
	pool := newTestSignerPool(t, 2)
	var individualCount atomic.Int32

	bankQ := &mockBankQuerier{
		balanceFn: func(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error) {
			if in.Address == pool.ProviderAddress() {
				return &banktypes.QueryBalanceResponse{
					Balance: &sdk.Coin{Denom: "umfx", Amount: math.NewInt(500_000_000)},
				}, nil
			}
			return &banktypes.QueryBalanceResponse{
				Balance: &sdk.Coin{Denom: "umfx", Amount: math.NewInt(0)}, // needs funding
			}, nil
		},
	}
	broadcaster := &mockTxBroadcaster{
		broadcastMultiMsgFn: func(ctx context.Context, msgs []sdk.Msg) (string, error) {
			return "", errors.New("batch failed")
		},
		broadcastTxFn: func(ctx context.Context, msg sdk.Msg) (string, error) {
			individualCount.Add(1)
			return "tx-hash", nil
		},
	}

	minBalance := sdk.NewCoin("umfx", math.NewInt(10_000_000))
	topUp := sdk.NewCoin("umfx", math.NewInt(50_000_000))

	err := EnsureFunding(t.Context(), bankQ, broadcaster, pool, minBalance, topUp)
	require.NoError(t, err)
	assert.Equal(t, int32(2), individualCount.Load(), "should fall back to 2 individual sends")
}

func TestEnsureFunding_BalanceQueryFailure_SkipsSubSigner(t *testing.T) {
	pool := newTestSignerPool(t, 2)
	subAddrs := pool.SubSignerAddresses()
	var broadcastCount atomic.Int32

	bankQ := &mockBankQuerier{
		balanceFn: func(ctx context.Context, in *banktypes.QueryBalanceRequest, opts ...grpc.CallOption) (*banktypes.QueryBalanceResponse, error) {
			if in.Address == pool.ProviderAddress() {
				return &banktypes.QueryBalanceResponse{
					Balance: &sdk.Coin{Denom: "umfx", Amount: math.NewInt(500_000_000)},
				}, nil
			}
			// First sub-signer: query fails
			if in.Address == subAddrs[0] {
				return nil, errors.New("rpc unavailable")
			}
			// Second sub-signer: needs funding
			return &banktypes.QueryBalanceResponse{
				Balance: &sdk.Coin{Denom: "umfx", Amount: math.NewInt(0)},
			}, nil
		},
	}
	broadcaster := &mockTxBroadcaster{
		broadcastMultiMsgFn: func(ctx context.Context, msgs []sdk.Msg) (string, error) {
			broadcastCount.Add(1)
			// Only sub-signer 2 should need funding (sub-signer 1 was skipped)
			assert.Len(t, msgs, 1)
			return "tx-hash", nil
		},
	}

	minBalance := sdk.NewCoin("umfx", math.NewInt(10_000_000))
	topUp := sdk.NewCoin("umfx", math.NewInt(50_000_000))

	err := EnsureFunding(t.Context(), bankQ, broadcaster, pool, minBalance, topUp)
	require.NoError(t, err)
	assert.Equal(t, int32(1), broadcastCount.Load(), "should fund only the sub-signer whose balance was queryable")
}
