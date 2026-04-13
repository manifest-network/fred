package chain

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdktypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/query"
	"github.com/cosmos/cosmos-sdk/types/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	skutypes "github.com/manifest-network/manifest-ledger/x/sku/types"
)

// ---------------------------------------------------------------------------
// Mock gRPC clients
// ---------------------------------------------------------------------------

type mockBillingQuery struct {
	billingtypes.QueryClient
	LeasesByProviderFn     func(ctx context.Context, req *billingtypes.QueryLeasesByProviderRequest, opts ...grpc.CallOption) (*billingtypes.QueryLeasesByProviderResponse, error)
	LeaseFn                func(ctx context.Context, req *billingtypes.QueryLeaseRequest, opts ...grpc.CallOption) (*billingtypes.QueryLeaseResponse, error)
	CreditAccountFn        func(ctx context.Context, req *billingtypes.QueryCreditAccountRequest, opts ...grpc.CallOption) (*billingtypes.QueryCreditAccountResponse, error)
	ProviderWithdrawableFn func(ctx context.Context, req *billingtypes.QueryProviderWithdrawableRequest, opts ...grpc.CallOption) (*billingtypes.QueryProviderWithdrawableResponse, error)
}

func (m *mockBillingQuery) LeasesByProvider(ctx context.Context, req *billingtypes.QueryLeasesByProviderRequest, opts ...grpc.CallOption) (*billingtypes.QueryLeasesByProviderResponse, error) {
	if m.LeasesByProviderFn != nil {
		return m.LeasesByProviderFn(ctx, req, opts...)
	}
	panic("mockBillingQuery.LeasesByProvider not implemented")
}

func (m *mockBillingQuery) Lease(ctx context.Context, req *billingtypes.QueryLeaseRequest, opts ...grpc.CallOption) (*billingtypes.QueryLeaseResponse, error) {
	if m.LeaseFn != nil {
		return m.LeaseFn(ctx, req, opts...)
	}
	panic("mockBillingQuery.Lease not implemented")
}

func (m *mockBillingQuery) CreditAccount(ctx context.Context, req *billingtypes.QueryCreditAccountRequest, opts ...grpc.CallOption) (*billingtypes.QueryCreditAccountResponse, error) {
	if m.CreditAccountFn != nil {
		return m.CreditAccountFn(ctx, req, opts...)
	}
	panic("mockBillingQuery.CreditAccount not implemented")
}

func (m *mockBillingQuery) ProviderWithdrawable(ctx context.Context, req *billingtypes.QueryProviderWithdrawableRequest, opts ...grpc.CallOption) (*billingtypes.QueryProviderWithdrawableResponse, error) {
	if m.ProviderWithdrawableFn != nil {
		return m.ProviderWithdrawableFn(ctx, req, opts...)
	}
	panic("mockBillingQuery.ProviderWithdrawable not implemented")
}

type mockSKUQuery struct {
	skutypes.QueryClient
	ProviderFn func(ctx context.Context, req *skutypes.QueryProviderRequest, opts ...grpc.CallOption) (*skutypes.QueryProviderResponse, error)
}

func (m *mockSKUQuery) Provider(ctx context.Context, req *skutypes.QueryProviderRequest, opts ...grpc.CallOption) (*skutypes.QueryProviderResponse, error) {
	if m.ProviderFn != nil {
		return m.ProviderFn(ctx, req, opts...)
	}
	panic("mockSKUQuery.Provider not implemented")
}

type mockAuthQuery struct {
	authtypes.QueryClient
	AccountFn func(ctx context.Context, req *authtypes.QueryAccountRequest, opts ...grpc.CallOption) (*authtypes.QueryAccountResponse, error)
}

func (m *mockAuthQuery) Account(ctx context.Context, req *authtypes.QueryAccountRequest, opts ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
	if m.AccountFn != nil {
		return m.AccountFn(ctx, req, opts...)
	}
	panic("mockAuthQuery.Account not implemented")
}

type mockTxService struct {
	tx.ServiceClient
	BroadcastTxFn func(ctx context.Context, req *tx.BroadcastTxRequest, opts ...grpc.CallOption) (*tx.BroadcastTxResponse, error)
	GetTxFn       func(ctx context.Context, req *tx.GetTxRequest, opts ...grpc.CallOption) (*tx.GetTxResponse, error)
}

func (m *mockTxService) BroadcastTx(ctx context.Context, req *tx.BroadcastTxRequest, opts ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
	if m.BroadcastTxFn != nil {
		return m.BroadcastTxFn(ctx, req, opts...)
	}
	panic("mockTxService.BroadcastTx not implemented")
}

func (m *mockTxService) GetTx(ctx context.Context, req *tx.GetTxRequest, opts ...grpc.CallOption) (*tx.GetTxResponse, error) {
	if m.GetTxFn != nil {
		return m.GetTxFn(ctx, req, opts...)
	}
	panic("mockTxService.GetTx not implemented")
}

// ---------------------------------------------------------------------------
// Helper to build a testable Client with mock dependencies
// ---------------------------------------------------------------------------

// newTestSignerPoolFromSigner wraps a single signer in a SignerPool for testing.
func newTestSignerPoolFromSigner(s *Signer) *SignerPool {
	return &SignerPool{primary: s}
}

func newMockClient(opts ...func(*Client)) *Client {
	c := &Client{
		billingQuery:   &mockBillingQuery{},
		skuQuery:       &mockSKUQuery{},
		authQuery:      &mockAuthQuery{},
		txService:      &mockTxService{},
		txPollInterval: 10 * time.Millisecond,
		txTimeout:      500 * time.Millisecond,
		queryPageLimit: 100,
	}
	for _, opt := range opts {
		opt(c)
	}
	// Auto-derive providerAddress from signerPool if not explicitly set.
	if c.providerAddress == "" && c.signerPool != nil {
		c.providerAddress = c.signerPool.ProviderAddress()
	}
	return c
}

// ---------------------------------------------------------------------------
// Existing tests (TLS, defaults, constants, retry logic, etc.)
// ---------------------------------------------------------------------------

func TestBuildTLSConfig_Defaults(t *testing.T) {
	cfg, err := buildTLSConfig("", false)
	require.NoError(t, err)

	require.NotNil(t, cfg)
	assert.False(t, cfg.InsecureSkipVerify)
	assert.Nil(t, cfg.RootCAs)
}

func TestBuildTLSConfig_SkipVerify(t *testing.T) {
	cfg, err := buildTLSConfig("", true)
	require.NoError(t, err)

	assert.True(t, cfg.InsecureSkipVerify)
}

func TestBuildTLSConfig_WithCAFile(t *testing.T) {
	tempDir := t.TempDir()
	caFile := filepath.Join(tempDir, "ca.pem")
	testCert := generateTestCertificate(t)

	err := os.WriteFile(caFile, testCert, 0600)
	require.NoError(t, err)

	cfg, err := buildTLSConfig(caFile, false)
	require.NoError(t, err)

	assert.NotNil(t, cfg.RootCAs)
}

func TestBuildTLSConfig_InvalidCAFile(t *testing.T) {
	_, err := buildTLSConfig("/nonexistent/ca.pem", false)
	assert.Error(t, err)
}

func TestBuildTLSConfig_InvalidCertificate(t *testing.T) {
	tempDir := t.TempDir()
	caFile := filepath.Join(tempDir, "invalid.pem")

	err := os.WriteFile(caFile, []byte("not a valid certificate"), 0600)
	require.NoError(t, err)

	_, err = buildTLSConfig(caFile, false)
	assert.Error(t, err)
}

func TestNewClient_Defaults(t *testing.T) {
	tests := []struct {
		name               string
		txPollInterval     time.Duration
		txTimeout          time.Duration
		queryPageLimit     int
		wantPollInterval   time.Duration
		wantTimeout        time.Duration
		wantQueryPageLimit uint64
	}{
		{
			name:               "zero values get defaults",
			wantPollInterval:   500 * time.Millisecond,
			wantTimeout:        defaultTxTimeout,
			wantQueryPageLimit: 100,
		},
		{
			name:               "custom values preserved",
			txPollInterval:     time.Second,
			txTimeout:          time.Minute,
			queryPageLimit:     50,
			wantPollInterval:   time.Second,
			wantTimeout:        time.Minute,
			wantQueryPageLimit: 50,
		},
		{
			name:               "negative page limit gets default",
			queryPageLimit:     -1,
			wantPollInterval:   500 * time.Millisecond,
			wantTimeout:        defaultTxTimeout,
			wantQueryPageLimit: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := newTestSignerPoolFromSigner(newTestSigner(t))
			c, err := NewClient(ClientConfig{
				Endpoint:       "localhost:9999",
				TxPollInterval: tt.txPollInterval,
				TxTimeout:      tt.txTimeout,
				QueryPageLimit: tt.queryPageLimit,
			}, pool)
			require.NoError(t, err)
			defer c.Close()

			assert.Equal(t, tt.wantPollInterval, c.txPollInterval)
			assert.Equal(t, tt.wantTimeout, c.txTimeout)
			assert.Equal(t, tt.wantQueryPageLimit, c.queryPageLimit)
		})
	}
}

func TestMaxLeasesPerBatch(t *testing.T) {
	assert.Equal(t, 100, maxLeasesPerBatch)
}

func TestBatchBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		count     int
		batchSize int
		wantCount int
	}{
		{name: "exact batch size", count: 100, batchSize: 100, wantCount: 1},
		{name: "one over batch size", count: 101, batchSize: 100, wantCount: 2},
		{name: "two full batches", count: 200, batchSize: 100, wantCount: 2},
		{name: "partial batch", count: 50, batchSize: 100, wantCount: 1},
		{name: "empty input", count: 0, batchSize: 100, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := make([]string, tt.count)
			for i := range input {
				input[i] = "item"
			}
			var batchCount int
			for range chunk(input, tt.batchSize) {
				batchCount++
			}
			assert.Equal(t, tt.wantCount, batchCount)
		})
	}
}

func chunk[S ~[]E, E any](s S, n int) func(func(S) bool) {
	return func(yield func(S) bool) {
		for i := 0; i < len(s); i += n {
			end := i + n
			if end > len(s) {
				end = len(s)
			}
			if !yield(s[i:end]) {
				return
			}
		}
	}
}

func TestIsRetryableTxError(t *testing.T) {
	client := &Client{}

	tests := []struct {
		name      string
		err       error
		wantRetry bool
	}{
		{name: "nil error", err: nil, wantRetry: false},
		{name: "sequence mismatch", err: &ChainTxError{Code: 32, Codespace: "sdk", RawLog: "account sequence mismatch"}, wantRetry: true},
		{name: "other chain error", err: &ChainTxError{Code: 4, Codespace: "sdk", RawLog: "unauthorized"}, wantRetry: false},
		{name: "billing module error", err: &ChainTxError{Code: 1, Codespace: "billing", RawLog: "lease not found"}, wantRetry: false},
		{name: "wrapped sequence mismatch", err: fmt.Errorf("broadcast: %w", &ChainTxError{Code: 32, Codespace: "sdk"}), wantRetry: true},
		{name: "wrapped non-retryable chain", err: fmt.Errorf("tx: %w", &ChainTxError{Code: 1, Codespace: "billing"}), wantRetry: false},
		{name: "gRPC unavailable", err: status.Error(codes.Unavailable, "conn refused"), wantRetry: true},
		{name: "gRPC deadline exceeded", err: status.Error(codes.DeadlineExceeded, "deadline"), wantRetry: true},
		{name: "gRPC resource exhausted", err: status.Error(codes.ResourceExhausted, "rate limited"), wantRetry: true},
		{name: "gRPC aborted", err: status.Error(codes.Aborted, "aborted"), wantRetry: true},
		{name: "gRPC internal", err: status.Error(codes.Internal, "internal"), wantRetry: true},
		{name: "gRPC unknown", err: status.Error(codes.Unknown, "unknown"), wantRetry: true},
		{name: "context deadline exceeded", err: context.DeadlineExceeded, wantRetry: true},
		{name: "context canceled", err: context.Canceled, wantRetry: true},
		{name: "wrapped gRPC unavailable", err: fmt.Errorf("connect: %w", status.Error(codes.Unavailable, "server unavailable")), wantRetry: true},
		{name: "gRPC not found", err: status.Error(codes.NotFound, "not found"), wantRetry: false},
		{name: "gRPC invalid argument", err: status.Error(codes.InvalidArgument, "invalid"), wantRetry: false},
		{name: "gRPC permission denied", err: status.Error(codes.PermissionDenied, "denied"), wantRetry: false},
		{name: "plain error", err: fmt.Errorf("some error"), wantRetry: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantRetry, client.isRetryableTxError(tt.err))
		})
	}
}

func TestTxRetryConstants(t *testing.T) {
	assert.True(t, txMaxRetries >= 1 && txMaxRetries <= 10)
	assert.True(t, txInitialBackoff >= 100*time.Millisecond && txInitialBackoff <= 5*time.Second)
	assert.True(t, txMaxBackoff >= txInitialBackoff)
}

func TestIsRetryableGRPCCode(t *testing.T) {
	c := &Client{}

	retryable := []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Internal, codes.Unknown}
	for _, code := range retryable {
		assert.True(t, c.isRetryableGRPCCode(code), "code %s should be retryable", code)
	}

	nonRetryable := []codes.Code{codes.OK, codes.Canceled, codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition, codes.OutOfRange, codes.Unimplemented, codes.DataLoss}
	for _, code := range nonRetryable {
		assert.False(t, c.isRetryableGRPCCode(code), "code %s should NOT be retryable", code)
	}
}

// ---------------------------------------------------------------------------
// Query function tests
// ---------------------------------------------------------------------------

func TestClient_GetPendingLeases(t *testing.T) {
	t.Run("success single page", func(t *testing.T) {
		bq := &mockBillingQuery{
			LeasesByProviderFn: func(_ context.Context, req *billingtypes.QueryLeasesByProviderRequest, _ ...grpc.CallOption) (*billingtypes.QueryLeasesByProviderResponse, error) {
				assert.Equal(t, billingtypes.LEASE_STATE_PENDING, req.StateFilter)
				return &billingtypes.QueryLeasesByProviderResponse{
					Leases: []billingtypes.Lease{{Uuid: "l1"}, {Uuid: "l2"}},
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		leases, err := c.GetPendingLeases(t.Context(), "prov-1")
		require.NoError(t, err)
		assert.Len(t, leases, 2)
	})

	t.Run("success multi page", func(t *testing.T) {
		var calls int
		bq := &mockBillingQuery{
			LeasesByProviderFn: func(_ context.Context, req *billingtypes.QueryLeasesByProviderRequest, _ ...grpc.CallOption) (*billingtypes.QueryLeasesByProviderResponse, error) {
				calls++
				if calls == 1 {
					return &billingtypes.QueryLeasesByProviderResponse{
						Leases:     []billingtypes.Lease{{Uuid: "l1"}},
						Pagination: &query.PageResponse{NextKey: []byte("page2")},
					}, nil
				}
				assert.Equal(t, []byte("page2"), req.Pagination.Key)
				return &billingtypes.QueryLeasesByProviderResponse{
					Leases: []billingtypes.Lease{{Uuid: "l2"}},
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		leases, err := c.GetPendingLeases(t.Context(), "prov-1")
		require.NoError(t, err)
		assert.Len(t, leases, 2)
		assert.Equal(t, 2, calls)
	})

	t.Run("gRPC error", func(t *testing.T) {
		bq := &mockBillingQuery{
			LeasesByProviderFn: func(context.Context, *billingtypes.QueryLeasesByProviderRequest, ...grpc.CallOption) (*billingtypes.QueryLeasesByProviderResponse, error) {
				return nil, status.Error(codes.Unavailable, "down")
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		_, err := c.GetPendingLeases(t.Context(), "prov-1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to query pending leases")
	})
}

func TestClient_GetActiveLeasesByProvider(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		bq := &mockBillingQuery{
			LeasesByProviderFn: func(_ context.Context, req *billingtypes.QueryLeasesByProviderRequest, _ ...grpc.CallOption) (*billingtypes.QueryLeasesByProviderResponse, error) {
				assert.Equal(t, billingtypes.LEASE_STATE_ACTIVE, req.StateFilter)
				return &billingtypes.QueryLeasesByProviderResponse{
					Leases: []billingtypes.Lease{{Uuid: "a1", State: billingtypes.LEASE_STATE_ACTIVE}},
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		leases, err := c.GetActiveLeasesByProvider(t.Context(), "prov-1")
		require.NoError(t, err)
		assert.Len(t, leases, 1)
	})

	t.Run("gRPC error", func(t *testing.T) {
		bq := &mockBillingQuery{
			LeasesByProviderFn: func(context.Context, *billingtypes.QueryLeasesByProviderRequest, ...grpc.CallOption) (*billingtypes.QueryLeasesByProviderResponse, error) {
				return nil, status.Error(codes.Internal, "oops")
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		_, err := c.GetActiveLeasesByProvider(t.Context(), "prov-1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to query active leases")
	})
}

func TestClient_GetLease(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		bq := &mockBillingQuery{
			LeaseFn: func(_ context.Context, req *billingtypes.QueryLeaseRequest, _ ...grpc.CallOption) (*billingtypes.QueryLeaseResponse, error) {
				return &billingtypes.QueryLeaseResponse{
					Lease: billingtypes.Lease{Uuid: req.LeaseUuid, State: billingtypes.LEASE_STATE_ACTIVE},
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		lease, err := c.GetLease(t.Context(), "lease-123")
		require.NoError(t, err)
		require.NotNil(t, lease)
		assert.Equal(t, "lease-123", lease.Uuid)
	})

	t.Run("not found returns nil nil", func(t *testing.T) {
		bq := &mockBillingQuery{
			LeaseFn: func(context.Context, *billingtypes.QueryLeaseRequest, ...grpc.CallOption) (*billingtypes.QueryLeaseResponse, error) {
				return nil, status.Error(codes.NotFound, "not found")
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		lease, err := c.GetLease(t.Context(), "nope")
		assert.NoError(t, err)
		assert.Nil(t, lease)
	})

	t.Run("gRPC error", func(t *testing.T) {
		bq := &mockBillingQuery{
			LeaseFn: func(context.Context, *billingtypes.QueryLeaseRequest, ...grpc.CallOption) (*billingtypes.QueryLeaseResponse, error) {
				return nil, status.Error(codes.Internal, "server error")
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		_, err := c.GetLease(t.Context(), "nope")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to query lease")
	})
}

func TestClient_GetActiveLease(t *testing.T) {
	t.Run("active lease", func(t *testing.T) {
		bq := &mockBillingQuery{
			LeaseFn: func(context.Context, *billingtypes.QueryLeaseRequest, ...grpc.CallOption) (*billingtypes.QueryLeaseResponse, error) {
				return &billingtypes.QueryLeaseResponse{
					Lease: billingtypes.Lease{Uuid: "a1", State: billingtypes.LEASE_STATE_ACTIVE},
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		lease, err := c.GetActiveLease(t.Context(), "a1")
		require.NoError(t, err)
		require.NotNil(t, lease)
		assert.Equal(t, billingtypes.LEASE_STATE_ACTIVE, lease.State)
	})

	t.Run("not found returns nil", func(t *testing.T) {
		bq := &mockBillingQuery{
			LeaseFn: func(context.Context, *billingtypes.QueryLeaseRequest, ...grpc.CallOption) (*billingtypes.QueryLeaseResponse, error) {
				return nil, status.Error(codes.NotFound, "not found")
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		lease, err := c.GetActiveLease(t.Context(), "missing")
		assert.NoError(t, err)
		assert.Nil(t, lease)
	})

	t.Run("non-active returns nil", func(t *testing.T) {
		bq := &mockBillingQuery{
			LeaseFn: func(context.Context, *billingtypes.QueryLeaseRequest, ...grpc.CallOption) (*billingtypes.QueryLeaseResponse, error) {
				return &billingtypes.QueryLeaseResponse{
					Lease: billingtypes.Lease{Uuid: "p1", State: billingtypes.LEASE_STATE_PENDING},
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		lease, err := c.GetActiveLease(t.Context(), "p1")
		require.NoError(t, err)
		assert.Nil(t, lease)
	})

	t.Run("gRPC error", func(t *testing.T) {
		bq := &mockBillingQuery{
			LeaseFn: func(context.Context, *billingtypes.QueryLeaseRequest, ...grpc.CallOption) (*billingtypes.QueryLeaseResponse, error) {
				return nil, status.Error(codes.Internal, "err")
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		_, err := c.GetActiveLease(t.Context(), "x")
		require.Error(t, err)
	})
}

func TestClient_GetProvider(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		sq := &mockSKUQuery{
			ProviderFn: func(_ context.Context, req *skutypes.QueryProviderRequest, _ ...grpc.CallOption) (*skutypes.QueryProviderResponse, error) {
				return &skutypes.QueryProviderResponse{
					Provider: skutypes.Provider{Uuid: req.Uuid},
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.skuQuery = sq })

		prov, err := c.GetProvider(t.Context(), "prov-abc")
		require.NoError(t, err)
		assert.Equal(t, "prov-abc", prov.Uuid)
	})

	t.Run("gRPC error", func(t *testing.T) {
		sq := &mockSKUQuery{
			ProviderFn: func(context.Context, *skutypes.QueryProviderRequest, ...grpc.CallOption) (*skutypes.QueryProviderResponse, error) {
				return nil, status.Error(codes.NotFound, "not found")
			},
		}
		c := newMockClient(func(c *Client) { c.skuQuery = sq })

		_, err := c.GetProvider(t.Context(), "nope")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to query provider")
	})
}

func TestClient_GetCreditAccount(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		bq := &mockBillingQuery{
			CreditAccountFn: func(_ context.Context, req *billingtypes.QueryCreditAccountRequest, _ ...grpc.CallOption) (*billingtypes.QueryCreditAccountResponse, error) {
				return &billingtypes.QueryCreditAccountResponse{
					CreditAccount: billingtypes.CreditAccount{Tenant: req.Tenant},
					Balances:      sdktypes.NewCoins(sdktypes.NewInt64Coin("umfx", 1000)),
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		acct, balances, err := c.GetCreditAccount(t.Context(), "tenant-1")
		require.NoError(t, err)
		assert.Equal(t, "tenant-1", acct.Tenant)
		assert.False(t, balances.IsZero())
	})

	t.Run("gRPC error", func(t *testing.T) {
		bq := &mockBillingQuery{
			CreditAccountFn: func(context.Context, *billingtypes.QueryCreditAccountRequest, ...grpc.CallOption) (*billingtypes.QueryCreditAccountResponse, error) {
				return nil, status.Error(codes.NotFound, "not found")
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		_, _, err := c.GetCreditAccount(t.Context(), "t")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to query credit account")
	})
}

func TestClient_GetProviderWithdrawable(t *testing.T) {
	t.Run("success verifies limit", func(t *testing.T) {
		bq := &mockBillingQuery{
			ProviderWithdrawableFn: func(_ context.Context, req *billingtypes.QueryProviderWithdrawableRequest, _ ...grpc.CallOption) (*billingtypes.QueryProviderWithdrawableResponse, error) {
				// queryPageLimit=100, so limit should be 100*10=1000
				assert.Equal(t, uint64(1000), req.Limit)
				return &billingtypes.QueryProviderWithdrawableResponse{
					Amounts: sdktypes.NewCoins(sdktypes.NewInt64Coin("umfx", 500)),
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		amounts, err := c.GetProviderWithdrawable(t.Context(), "prov-1")
		require.NoError(t, err)
		assert.False(t, amounts.IsZero())
	})

	t.Run("gRPC error", func(t *testing.T) {
		bq := &mockBillingQuery{
			ProviderWithdrawableFn: func(context.Context, *billingtypes.QueryProviderWithdrawableRequest, ...grpc.CallOption) (*billingtypes.QueryProviderWithdrawableResponse, error) {
				return nil, status.Error(codes.Internal, "err")
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		_, err := c.GetProviderWithdrawable(t.Context(), "prov-1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to query provider withdrawable")
	})
}

func TestClient_Ping(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		aq := &mockAuthQuery{
			AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
				return &authtypes.QueryAccountResponse{}, nil
			},
		}
		s := newTestSigner(t)
		pool := newTestSignerPoolFromSigner(s)
		c := newMockClient(func(c *Client) { c.authQuery = aq; c.signerPool = pool })

		err := c.Ping(t.Context())
		assert.NoError(t, err)
	})

	t.Run("error", func(t *testing.T) {
		aq := &mockAuthQuery{
			AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
				return nil, status.Error(codes.Unavailable, "down")
			},
		}
		s := newTestSigner(t)
		pool := newTestSignerPoolFromSigner(s)
		c := newMockClient(func(c *Client) { c.authQuery = aq; c.signerPool = pool })

		err := c.Ping(t.Context())
		assert.Error(t, err)
	})
}

func TestClient_GetLeasesByProvider_Pagination(t *testing.T) {
	t.Run("zero results", func(t *testing.T) {
		bq := &mockBillingQuery{
			LeasesByProviderFn: func(context.Context, *billingtypes.QueryLeasesByProviderRequest, ...grpc.CallOption) (*billingtypes.QueryLeasesByProviderResponse, error) {
				return &billingtypes.QueryLeasesByProviderResponse{}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		leases, err := c.getLeasesByProviderWithState(t.Context(), "p", billingtypes.LEASE_STATE_ACTIVE)
		require.NoError(t, err)
		assert.Empty(t, leases)
	})

	t.Run("three pages", func(t *testing.T) {
		var calls int
		bq := &mockBillingQuery{
			LeasesByProviderFn: func(_ context.Context, req *billingtypes.QueryLeasesByProviderRequest, _ ...grpc.CallOption) (*billingtypes.QueryLeasesByProviderResponse, error) {
				calls++
				switch calls {
				case 1:
					assert.Nil(t, req.Pagination.Key)
					return &billingtypes.QueryLeasesByProviderResponse{
						Leases:     []billingtypes.Lease{{Uuid: "1"}},
						Pagination: &query.PageResponse{NextKey: []byte("k2")},
					}, nil
				case 2:
					assert.Equal(t, []byte("k2"), req.Pagination.Key)
					return &billingtypes.QueryLeasesByProviderResponse{
						Leases:     []billingtypes.Lease{{Uuid: "2"}},
						Pagination: &query.PageResponse{NextKey: []byte("k3")},
					}, nil
				default:
					assert.Equal(t, []byte("k3"), req.Pagination.Key)
					return &billingtypes.QueryLeasesByProviderResponse{
						Leases: []billingtypes.Lease{{Uuid: "3"}},
					}, nil
				}
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		leases, err := c.getLeasesByProviderWithState(t.Context(), "p", billingtypes.LEASE_STATE_ACTIVE)
		require.NoError(t, err)
		assert.Len(t, leases, 3)
		assert.Equal(t, 3, calls)
	})
}

// ---------------------------------------------------------------------------
// Transaction tests
// ---------------------------------------------------------------------------

// setupTxMocks wires mock auth + tx service for a successful broadcast flow.
// Returns the Client and a counter for broadcast invocations.
func setupTxMocks(t *testing.T) (*Client, *atomic.Int32) {
	t.Helper()

	s := newTestSigner(t)
	addr, err := sdktypes.AccAddressFromBech32(s.address)
	require.NoError(t, err)

	accountAny := newTestAccountAny(t, addr, 1, 0)
	broadcastCount := &atomic.Int32{}

	aq := &mockAuthQuery{
		AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			return &authtypes.QueryAccountResponse{Account: accountAny}, nil
		},
	}

	ts := &mockTxService{
		BroadcastTxFn: func(_ context.Context, _ *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			broadcastCount.Add(1)
			return &tx.BroadcastTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: fmt.Sprintf("TX%d", broadcastCount.Load())},
			}, nil
		},
		GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
			return &tx.GetTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash},
			}, nil
		},
	}

	pool := newTestSignerPoolFromSigner(s)
	c := newMockClient(func(c *Client) {
		c.signerPool = pool
		c.authQuery = aq
		c.txService = ts
	})

	return c, broadcastCount
}

func TestClient_DoBroadcastTx(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		c, _ := setupTxMocks(t)

		msg := newTestMsg(c.providerAddress)
		hash, err := c.doBroadcastTxWithSigner(t.Context(), c.signerPool.Primary(), msg)
		require.NoError(t, err)
		assert.NotEmpty(t, hash)
	})

	t.Run("account query failure", func(t *testing.T) {
		s := newTestSigner(t)
		pool := newTestSignerPoolFromSigner(s)
		aq := &mockAuthQuery{
			AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
				return nil, status.Error(codes.Unavailable, "down")
			},
		}
		c := newMockClient(func(c *Client) { c.signerPool = pool; c.authQuery = aq })

		_, err := c.doBroadcastTxWithSigner(t.Context(), pool.Primary(), newTestMsg(s.Address()))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to query account")
	})

	t.Run("broadcast failure", func(t *testing.T) {
		s := newTestSigner(t)
		pool := newTestSignerPoolFromSigner(s)
		addr, _ := sdktypes.AccAddressFromBech32(s.address)
		accountAny := newTestAccountAny(t, addr, 1, 0)

		aq := &mockAuthQuery{
			AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
				return &authtypes.QueryAccountResponse{Account: accountAny}, nil
			},
		}
		ts := &mockTxService{
			BroadcastTxFn: func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
				return nil, status.Error(codes.Internal, "broadcast fail")
			},
		}
		c := newMockClient(func(c *Client) { c.signerPool = pool; c.authQuery = aq; c.txService = ts })

		_, err := c.doBroadcastTxWithSigner(t.Context(), pool.Primary(), newTestMsg(s.Address()))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to broadcast transaction")
	})

	t.Run("mempool rejection", func(t *testing.T) {
		s := newTestSigner(t)
		pool := newTestSignerPoolFromSigner(s)
		addr, _ := sdktypes.AccAddressFromBech32(s.address)
		accountAny := newTestAccountAny(t, addr, 1, 0)

		aq := &mockAuthQuery{
			AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
				return &authtypes.QueryAccountResponse{Account: accountAny}, nil
			},
		}
		ts := &mockTxService{
			BroadcastTxFn: func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 19, Codespace: "sdk", RawLog: "tx too large"},
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.signerPool = pool; c.authQuery = aq; c.txService = ts })

		_, err := c.doBroadcastTxWithSigner(t.Context(), pool.Primary(), newTestMsg(s.Address()))
		require.Error(t, err)
		var chainErr *ChainTxError
		require.ErrorAs(t, err, &chainErr)
		assert.Equal(t, uint32(19), chainErr.Code)
	})

	t.Run("execution failure after polling", func(t *testing.T) {
		s := newTestSigner(t)
		pool := newTestSignerPoolFromSigner(s)
		addr, _ := sdktypes.AccAddressFromBech32(s.address)
		accountAny := newTestAccountAny(t, addr, 1, 0)

		aq := &mockAuthQuery{
			AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
				return &authtypes.QueryAccountResponse{Account: accountAny}, nil
			},
		}
		ts := &mockTxService{
			BroadcastTxFn: func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TXEXEC"},
				}, nil
			},
			GetTxFn: func(context.Context, *tx.GetTxRequest, ...grpc.CallOption) (*tx.GetTxResponse, error) {
				return &tx.GetTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 5, Codespace: "billing", RawLog: "insufficient funds"},
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.signerPool = pool; c.authQuery = aq; c.txService = ts })

		_, err := c.doBroadcastTxWithSigner(t.Context(), pool.Primary(), newTestMsg(s.Address()))
		require.Error(t, err)
		var chainErr *ChainTxError
		require.ErrorAs(t, err, &chainErr)
		assert.Equal(t, uint32(5), chainErr.Code)
		assert.Equal(t, "billing", chainErr.Codespace)
	})
}

func TestClient_WaitForTx(t *testing.T) {
	t.Run("immediate success", func(t *testing.T) {
		ts := &mockTxService{
			GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
				return &tx.GetTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash},
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.txService = ts })

		resp, err := c.waitForTx(t.Context(), "TX1")
		require.NoError(t, err)
		assert.Equal(t, "TX1", resp.TxResponse.TxHash)
	})

	t.Run("success after 3 polls", func(t *testing.T) {
		var calls atomic.Int32
		ts := &mockTxService{
			GetTxFn: func(context.Context, *tx.GetTxRequest, ...grpc.CallOption) (*tx.GetTxResponse, error) {
				n := calls.Add(1)
				if n < 3 {
					return nil, status.Error(codes.NotFound, "not indexed yet")
				}
				return &tx.GetTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TX2"},
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.txService = ts })

		resp, err := c.waitForTx(t.Context(), "TX2")
		require.NoError(t, err)
		assert.Equal(t, "TX2", resp.TxResponse.TxHash)
		assert.GreaterOrEqual(t, int(calls.Load()), 3)
	})

	t.Run("context timeout", func(t *testing.T) {
		ts := &mockTxService{
			GetTxFn: func(context.Context, *tx.GetTxRequest, ...grpc.CallOption) (*tx.GetTxResponse, error) {
				return nil, status.Error(codes.NotFound, "not indexed")
			},
		}
		c := newMockClient(func(c *Client) {
			c.txService = ts
			c.txTimeout = 100 * time.Millisecond
		})

		_, err := c.waitForTx(t.Context(), "TX3")
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestClient_BroadcastBatchedMsgs(t *testing.T) {
	t.Run("empty input", func(t *testing.T) {
		c := newMockClient()
		n, hashes, err := c.broadcastBatchedMsgs(t.Context(), nil, "test", "tested",
			func(batch []string) sdktypes.Msg { return nil })
		assert.Equal(t, uint64(0), n)
		assert.Nil(t, hashes)
		assert.NoError(t, err)
	})

	t.Run("single batch", func(t *testing.T) {
		c, count := setupTxMocks(t)

		uuids := make([]string, 50)
		for i := range uuids {
			uuids[i] = fmt.Sprintf("l-%d", i)
		}

		n, hashes, err := c.broadcastBatchedMsgs(t.Context(), uuids, "acknowledge", "acknowledged",
			func(batch []string) sdktypes.Msg {
				return &billingtypes.MsgAcknowledgeLease{
					Sender:     c.providerAddress,
					LeaseUuids: batch,
				}
			})
		require.NoError(t, err)
		assert.Equal(t, uint64(50), n)
		assert.Len(t, hashes, 1)
		assert.Equal(t, int32(1), count.Load())
	})

	t.Run("multiple batches 250 leases", func(t *testing.T) {
		c, count := setupTxMocks(t)

		uuids := make([]string, 250)
		for i := range uuids {
			uuids[i] = fmt.Sprintf("l-%d", i)
		}

		n, hashes, err := c.broadcastBatchedMsgs(t.Context(), uuids, "acknowledge", "acknowledged",
			func(batch []string) sdktypes.Msg {
				return &billingtypes.MsgAcknowledgeLease{
					Sender:     c.providerAddress,
					LeaseUuids: batch,
				}
			})
		require.NoError(t, err)
		assert.Equal(t, uint64(250), n)
		assert.Len(t, hashes, 3) // 100 + 100 + 50
		assert.Equal(t, int32(3), count.Load())
	})

	t.Run("error on second batch returns partial", func(t *testing.T) {
		s := newTestSigner(t)
		pool := newTestSignerPoolFromSigner(s)
		addr, _ := sdktypes.AccAddressFromBech32(s.address)
		accountAny := newTestAccountAny(t, addr, 1, 0)

		var calls atomic.Int32
		aq := &mockAuthQuery{
			AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
				return &authtypes.QueryAccountResponse{Account: accountAny}, nil
			},
		}
		ts := &mockTxService{
			BroadcastTxFn: func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
				n := calls.Add(1)
				if n > 1 {
					return nil, status.Error(codes.Internal, "node down")
				}
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TX1"},
				}, nil
			},
			GetTxFn: func(context.Context, *tx.GetTxRequest, ...grpc.CallOption) (*tx.GetTxResponse, error) {
				return &tx.GetTxResponse{TxResponse: &sdktypes.TxResponse{Code: 0}}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.signerPool = pool; c.authQuery = aq; c.txService = ts })

		uuids := make([]string, 150)
		for i := range uuids {
			uuids[i] = fmt.Sprintf("l-%d", i)
		}

		n, hashes, err := c.broadcastBatchedMsgs(t.Context(), uuids, "acknowledge", "acknowledged",
			func(batch []string) sdktypes.Msg {
				return &billingtypes.MsgAcknowledgeLease{Sender: s.Address(), LeaseUuids: batch}
			})
		require.Error(t, err)
		assert.Equal(t, uint64(100), n) // first batch succeeded
		assert.Len(t, hashes, 1)
	})
}

func TestClient_AcknowledgeLeases(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		c, _ := setupTxMocks(t)

		n, hashes, err := c.AcknowledgeLeases(t.Context(), []string{"l1", "l2"})
		require.NoError(t, err)
		assert.Equal(t, uint64(2), n)
		assert.Len(t, hashes, 1)
	})

	t.Run("empty input", func(t *testing.T) {
		c := newMockClient()
		n, hashes, err := c.AcknowledgeLeases(t.Context(), nil)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), n)
		assert.Nil(t, hashes)
	})
}

func TestClient_RejectLeases(t *testing.T) {
	c, _ := setupTxMocks(t)

	n, hashes, err := c.RejectLeases(t.Context(), []string{"l1"}, "out of capacity")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), n)
	assert.Len(t, hashes, 1)
}

func TestClient_CloseLeases(t *testing.T) {
	c, _ := setupTxMocks(t)

	n, hashes, err := c.CloseLeases(t.Context(), []string{"l1", "l2"}, "maintenance")
	require.NoError(t, err)
	assert.Equal(t, uint64(2), n)
	assert.Len(t, hashes, 1)
}

func TestClient_WithdrawByProvider(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		c, _ := setupTxMocks(t)

		hash, err := c.WithdrawByProvider(t.Context(), "prov-1")
		require.NoError(t, err)
		assert.NotEmpty(t, hash)
	})

	t.Run("broadcast failure", func(t *testing.T) {
		s := newTestSigner(t)
		pool := newTestSignerPoolFromSigner(s)
		addr, _ := sdktypes.AccAddressFromBech32(s.address)
		accountAny := newTestAccountAny(t, addr, 1, 0)

		aq := &mockAuthQuery{
			AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
				return &authtypes.QueryAccountResponse{Account: accountAny}, nil
			},
		}
		ts := &mockTxService{
			BroadcastTxFn: func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
				return nil, status.Error(codes.Internal, "node down")
			},
		}
		c := newMockClient(func(c *Client) { c.signerPool = pool; c.authQuery = aq; c.txService = ts })

		_, err := c.WithdrawByProvider(t.Context(), "prov-1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to withdraw")
	})
}

// ---------------------------------------------------------------------------
// MsgExec wrapping tests
// ---------------------------------------------------------------------------

func TestClient_BroadcastBatchedMsgs_MsgExecWrapping(t *testing.T) {
	// With sub-signers, broadcastBatchedMsgs should wrap messages in MsgExec
	pool := newTestSignerPool(t, 2)
	primaryAddr := pool.ProviderAddress()

	// Track which address was used for the account query (should be sub-signer, not primary)
	var queriedAddrs []string
	var mu sync.Mutex

	s := pool.Primary()
	addr, err := sdktypes.AccAddressFromBech32(s.address)
	require.NoError(t, err)
	accountAny := newTestAccountAny(t, addr, 1, 0)

	// For sub-signers, we need accounts too
	subAddrs := pool.SubSignerAddresses()
	subAccAddr0, _ := sdktypes.AccAddressFromBech32(subAddrs[0])
	subAccountAny0 := newTestAccountAny(t, subAccAddr0, 2, 0)
	subAccAddr1, _ := sdktypes.AccAddressFromBech32(subAddrs[1])
	subAccountAny1 := newTestAccountAny(t, subAccAddr1, 3, 0)

	aq := &mockAuthQuery{
		AccountFn: func(_ context.Context, req *authtypes.QueryAccountRequest, _ ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			mu.Lock()
			queriedAddrs = append(queriedAddrs, req.Address)
			mu.Unlock()
			// Return appropriate account for each address
			switch req.Address {
			case subAddrs[0]:
				return &authtypes.QueryAccountResponse{Account: subAccountAny0}, nil
			case subAddrs[1]:
				return &authtypes.QueryAccountResponse{Account: subAccountAny1}, nil
			default:
				return &authtypes.QueryAccountResponse{Account: accountAny}, nil
			}
		},
	}

	var broadcastedBytes [][]byte
	ts := &mockTxService{
		BroadcastTxFn: func(_ context.Context, req *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			mu.Lock()
			broadcastedBytes = append(broadcastedBytes, req.TxBytes)
			mu.Unlock()
			return &tx.BroadcastTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TX1"},
			}, nil
		},
		GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
			return &tx.GetTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash},
			}, nil
		},
	}

	c := newMockClient(func(c *Client) {
		c.signerPool = pool
		c.providerAddress = primaryAddr
		c.authQuery = aq
		c.txService = ts
	})

	n, hashes, err := c.AcknowledgeLeases(t.Context(), []string{"l1", "l2"})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), n)
	assert.Len(t, hashes, 1)

	// The account query should have been for a sub-signer address, not the primary
	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, queriedAddrs)
	assert.NotEqual(t, primaryAddr, queriedAddrs[0],
		"should query sub-signer account, not primary")
	assert.Contains(t, subAddrs, queriedAddrs[0],
		"queried address should be one of the sub-signers")
}

func TestClient_BroadcastBatchedMsgs_NoWrapping_SingleSigner(t *testing.T) {
	// With no sub-signers, broadcastBatchedMsgs should NOT wrap in MsgExec
	c, _ := setupTxMocks(t)

	n, hashes, err := c.AcknowledgeLeases(t.Context(), []string{"l1"})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), n)
	assert.Len(t, hashes, 1)
	// The test passes if no panic and no wrapping — setupTxMocks uses a single-signer pool
}

func TestClient_BroadcastBatchedMsgs_NoWrapping_AfterDemotion(t *testing.T) {
	// After DemoteToSingleSigner, broadcastBatchedMsgs should use primary (no MsgExec)
	pool := newTestSignerPool(t, 2)
	primaryAddr := pool.ProviderAddress()

	// Demote before creating client — simulates grant failure at startup
	pool.DemoteToSingleSigner()

	var queriedAddr string
	s := pool.Primary()
	addr, err := sdktypes.AccAddressFromBech32(s.address)
	require.NoError(t, err)
	accountAny := newTestAccountAny(t, addr, 1, 0)

	aq := &mockAuthQuery{
		AccountFn: func(_ context.Context, req *authtypes.QueryAccountRequest, _ ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			queriedAddr = req.Address
			return &authtypes.QueryAccountResponse{Account: accountAny}, nil
		},
	}
	broadcastCount := &atomic.Int32{}
	ts := &mockTxService{
		BroadcastTxFn: func(_ context.Context, _ *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			broadcastCount.Add(1)
			return &tx.BroadcastTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TX1"},
			}, nil
		},
		GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
			return &tx.GetTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash},
			}, nil
		},
	}

	c := newMockClient(func(c *Client) {
		c.signerPool = pool
		c.providerAddress = primaryAddr
		c.authQuery = aq
		c.txService = ts
	})

	n, _, err := c.AcknowledgeLeases(t.Context(), []string{"l1"})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), n)

	// After demotion, account query should be for the primary address (no sub-signer)
	assert.Equal(t, primaryAddr, queriedAddr, "should use primary signer after demotion")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func generateTestCertificate(t *testing.T) []byte {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Test CA"}},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
}
