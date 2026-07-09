package chain

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cosmossdk.io/math"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/query"
	"github.com/cosmos/cosmos-sdk/types/tx"
	authsigning "github.com/cosmos/cosmos-sdk/x/auth/signing"
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
	SimulateFn    func(ctx context.Context, req *tx.SimulateRequest, opts ...grpc.CallOption) (*tx.SimulateResponse, error)
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

func (m *mockTxService) Simulate(ctx context.Context, req *tx.SimulateRequest, opts ...grpc.CallOption) (*tx.SimulateResponse, error) {
	if m.SimulateFn != nil {
		return m.SimulateFn(ctx, req, opts...)
	}
	panic("mockTxService.Simulate not implemented")
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
		now:            time.Now,
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
// Task-0 gas-test helpers
// ---------------------------------------------------------------------------

// newTestClientWithGas returns a *Client (single-signer pool, authQuery returning
// a fixed account for the signer) plus the *Signer it uses, with the given gas
// params. Random-key signer — fine for gas-VALUE assertions; use
// newDeterministicTestSigner for byte-level assertions.
func newTestClientWithGas(t *testing.T, gasLimit, maxGasLimit uint64, adj float64) (*Client, *Signer) {
	t.Helper()
	s := newTestSigner(t)
	s.gasLimit, s.maxGasLimit = gasLimit, maxGasLimit
	if adj > 0 {
		d, err := math.LegacyNewDecFromStr(strconv.FormatFloat(adj, 'f', -1, 64))
		if err != nil {
			t.Fatalf("adjustment: %v", err)
		}
		s.gasAdjustment = d
	}
	addr, err := sdktypes.AccAddressFromBech32(s.Address())
	if err != nil {
		t.Fatalf("addr: %v", err)
	}
	acct := newTestAccountAny(t, addr, 1, 1)
	c := newMockClient(func(c *Client) {
		c.signerPool = newTestSignerPoolFromSigner(s)
		c.authQuery = &mockAuthQuery{AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			return &authtypes.QueryAccountResponse{Account: acct}, nil
		}}
	})
	return c, s
}

func okSimulate(gasUsed uint64) func(context.Context, *tx.SimulateRequest, ...grpc.CallOption) (*tx.SimulateResponse, error) {
	return func(context.Context, *tx.SimulateRequest, ...grpc.CallOption) (*tx.SimulateResponse, error) {
		return &tx.SimulateResponse{GasInfo: &sdktypes.GasInfo{GasUsed: gasUsed}}, nil
	}
}
func okBroadcast() func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
	return func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
		return &tx.BroadcastTxResponse{TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "ABCD"}}, nil
	}
}
func okGetTx() func(context.Context, *tx.GetTxRequest, ...grpc.CallOption) (*tx.GetTxResponse, error) {
	return func(context.Context, *tx.GetTxRequest, ...grpc.CallOption) (*tx.GetTxResponse, error) {
		return &tx.GetTxResponse{TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "ABCD"}}, nil
	}
}

// captureGas returns a BroadcastTxFn that decodes the broadcast tx, records its
// declared gas into *out, and returns success.
func captureGas(t *testing.T, out *uint64, _ *Client, signer *Signer) func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
	return func(_ context.Context, req *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
		decoded, err := signer.txConfig.TxDecoder()(req.TxBytes)
		if err != nil {
			t.Fatalf("decode broadcast tx: %v", err)
		}
		*out = decoded.(sdktypes.FeeTx).GetGas()
		return &tx.BroadcastTxResponse{TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "ABCD"}}, nil
	}
}

// countingAuthQuery wraps an authQuery, incrementing *n on each Account call.
func countingAuthQuery(inner authtypes.QueryClient, n *int) authtypes.QueryClient {
	return &mockAuthQuery{AccountFn: func(ctx context.Context, req *authtypes.QueryAccountRequest, opts ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
		*n++
		return inner.Account(ctx, req, opts...)
	}}
}

// outOfGasGetTxOnce returns a GetTxFn yielding an out-of-gas (code 11) execution
// result on the first call and success thereafter — drives exactly one OOG ladder
// bump. (OOG is an execution result surfaced by GetTx, not the BroadcastTx ack;
// mirrors the existing retry tests' code 11.)
func outOfGasGetTxOnce() func(context.Context, *tx.GetTxRequest, ...grpc.CallOption) (*tx.GetTxResponse, error) {
	var n int
	return func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
		n++
		code := uint32(0)
		if n == 1 {
			code = 11
		}
		return &tx.GetTxResponse{TxResponse: &sdktypes.TxResponse{Code: code, Codespace: "sdk", RawLog: "out of gas", TxHash: req.Hash}}, nil
	}
}

// seqMismatchBroadcastOnce returns a BroadcastTxFn yielding a sequence-mismatch
// (code 32) ack on the first call and success thereafter (mirrors the existing
// retry tests' code 32 at client_test.go:1410).
func seqMismatchBroadcastOnce() func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
	var n int
	return func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
		n++
		if n == 1 {
			return &tx.BroadcastTxResponse{TxResponse: &sdktypes.TxResponse{Code: 32, Codespace: "sdk", RawLog: "account sequence mismatch, expected 5, got 4"}}, nil
		}
		return &tx.BroadcastTxResponse{TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "OK"}}, nil
	}
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

func TestNewClient_NilPoolRejected(t *testing.T) {
	_, err := NewClient(ClientConfig{Endpoint: "localhost:9999"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "signer pool is required")
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
		{name: "out of gas", err: &ChainTxError{Code: 11, Codespace: "sdk", RawLog: "out of gas"}, wantRetry: true},
		{name: "wrapped out of gas", err: fmt.Errorf("exec: %w", &ChainTxError{Code: 11, Codespace: "sdk", RawLog: "out of gas"}), wantRetry: true},
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
	assert.True(t, txInitialBackoff >= time.Second && txInitialBackoff <= 30*time.Second)
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
	t.Run("success verifies page limit", func(t *testing.T) {
		bq := &mockBillingQuery{
			ProviderWithdrawableFn: func(_ context.Context, req *billingtypes.QueryProviderWithdrawableRequest, _ ...grpc.CallOption) (*billingtypes.QueryProviderWithdrawableResponse, error) {
				// queryPageLimit=100, so the page limit should be 100*10=1000
				require.NotNil(t, req.Pagination)
				assert.Equal(t, uint64(1000), req.Pagination.Limit)
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

	t.Run("sums amounts across pages", func(t *testing.T) {
		var calls int
		bq := &mockBillingQuery{
			ProviderWithdrawableFn: func(_ context.Context, req *billingtypes.QueryProviderWithdrawableRequest, _ ...grpc.CallOption) (*billingtypes.QueryProviderWithdrawableResponse, error) {
				calls++
				require.NotNil(t, req.Pagination)
				if calls == 1 {
					assert.Empty(t, req.Pagination.Key, "first page starts from an empty cursor")
					return &billingtypes.QueryProviderWithdrawableResponse{
						Amounts:    sdktypes.NewCoins(sdktypes.NewInt64Coin("umfx", 300)),
						Pagination: &query.PageResponse{NextKey: []byte("k1")},
					}, nil
				}
				assert.Equal(t, []byte("k1"), req.Pagination.Key, "second page echoes the first next_key")
				return &billingtypes.QueryProviderWithdrawableResponse{
					Amounts: sdktypes.NewCoins(sdktypes.NewInt64Coin("umfx", 200)),
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.billingQuery = bq })

		amounts, err := c.GetProviderWithdrawable(t.Context(), "prov-1")
		require.NoError(t, err)
		assert.Equal(t, int64(500), amounts.AmountOf("umfx").Int64(), "amounts summed across both pages")
		assert.Equal(t, 2, calls)
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
		SimulateFn: okSimulate(200000),
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
		hash, err := c.doBroadcastTxWithSigner(t.Context(), c.signerPool.Primary(), []sdktypes.Msg{msg}, nil, nil, nil)
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

		_, err := c.doBroadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, nil, nil, nil)
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

		_, err := c.doBroadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, nil, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to broadcast transaction")
	})

	t.Run("code 19 without tx hash returns error", func(t *testing.T) {
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

		_, err := c.doBroadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, nil, nil, nil)
		require.Error(t, err)
		var chainErr *ChainTxError
		require.ErrorAs(t, err, &chainErr)
		assert.Equal(t, uint32(19), chainErr.Code)
	})

	t.Run("code 19 falls through to waitForTx", func(t *testing.T) {
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
					TxResponse: &sdktypes.TxResponse{
						Code:      19,
						Codespace: "sdk",
						RawLog:    "tx already exists in cache",
						TxHash:    "MEMPOOL_TX",
					},
				}, nil
			},
			GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
				return &tx.GetTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash},
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.signerPool = pool; c.authQuery = aq; c.txService = ts })

		hash, err := c.doBroadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, nil, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, "MEMPOOL_TX", hash)
	})

	t.Run("code 19 with polling delay", func(t *testing.T) {
		s := newTestSigner(t)
		pool := newTestSignerPoolFromSigner(s)
		addr, _ := sdktypes.AccAddressFromBech32(s.address)
		accountAny := newTestAccountAny(t, addr, 1, 0)

		aq := &mockAuthQuery{
			AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
				return &authtypes.QueryAccountResponse{Account: accountAny}, nil
			},
		}
		var getTxCalls atomic.Int32
		ts := &mockTxService{
			BroadcastTxFn: func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{
						Code:      19,
						Codespace: "sdk",
						RawLog:    "tx already exists in cache",
						TxHash:    "MEMPOOL_DELAYED",
					},
				}, nil
			},
			GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
				n := getTxCalls.Add(1)
				if n < 3 {
					return nil, status.Error(codes.NotFound, "not indexed yet")
				}
				return &tx.GetTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash},
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.signerPool = pool; c.authQuery = aq; c.txService = ts })

		hash, err := c.doBroadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, nil, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, "MEMPOOL_DELAYED", hash)
		assert.GreaterOrEqual(t, int(getTxCalls.Load()), 3)
	})

	t.Run("code 19 falls through but execution fails", func(t *testing.T) {
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
					TxResponse: &sdktypes.TxResponse{
						Code:      19,
						Codespace: "sdk",
						RawLog:    "tx already exists in cache",
						TxHash:    "MEMPOOL_FAIL",
					},
				}, nil
			},
			GetTxFn: func(context.Context, *tx.GetTxRequest, ...grpc.CallOption) (*tx.GetTxResponse, error) {
				return &tx.GetTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 5, Codespace: "billing", RawLog: "insufficient funds"},
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.signerPool = pool; c.authQuery = aq; c.txService = ts })

		_, err := c.doBroadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, nil, nil, nil)
		require.Error(t, err)
		var chainErr *ChainTxError
		require.ErrorAs(t, err, &chainErr)
		assert.Equal(t, uint32(5), chainErr.Code)
		assert.Equal(t, "billing", chainErr.Codespace)
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

		_, err := c.doBroadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, nil, nil, nil)
		require.Error(t, err)
		var chainErr *ChainTxError
		require.ErrorAs(t, err, &chainErr)
		assert.Equal(t, uint32(5), chainErr.Code)
		assert.Equal(t, "billing", chainErr.Codespace)
	})

	t.Run("success with sequence override", func(t *testing.T) {
		s := newTestSigner(t)
		pool := newTestSignerPoolFromSigner(s)
		addr, _ := sdktypes.AccAddressFromBech32(s.address)
		// Account has sequence 5, but we override with 10
		accountAny := newTestAccountAny(t, addr, 1, 5)

		aq := &mockAuthQuery{
			AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
				return &authtypes.QueryAccountResponse{Account: accountAny}, nil
			},
		}
		ts := &mockTxService{
			BroadcastTxFn: func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TXSEQOVERRIDE"},
				}, nil
			},
			GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
				return &tx.GetTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash},
				}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.signerPool = pool; c.authQuery = aq; c.txService = ts })

		overrideSeq := uint64(10)
		hash, err := c.doBroadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, &overrideSeq, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, "TXSEQOVERRIDE", hash)
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
			SimulateFn: okSimulate(200000),
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

// withdrawTxData builds the hex-encoded TxResponse.Data a GetTx would return
// for a MsgWithdraw tx carrying the given response, mirroring how the chain
// packs MsgResponses into sdk.TxMsgData.
func withdrawTxData(t *testing.T, resp *billingtypes.MsgWithdrawResponse) string {
	t.Helper()
	respBz, err := resp.Marshal()
	require.NoError(t, err)
	msgData := sdktypes.TxMsgData{
		MsgResponses: []*codectypes.Any{{
			TypeUrl: sdktypes.MsgTypeURL(&billingtypes.MsgWithdrawResponse{}),
			Value:   respBz,
		}},
	}
	dataBz, err := msgData.Marshal()
	require.NoError(t, err)
	return hex.EncodeToString(dataBz)
}

func TestClient_WithdrawByProvider(t *testing.T) {
	t.Run("success decodes cursor from response", func(t *testing.T) {
		s := newTestSigner(t)
		pool := newTestSignerPoolFromSigner(s)
		addr, err := sdktypes.AccAddressFromBech32(s.address)
		require.NoError(t, err)
		accountAny := newTestAccountAny(t, addr, 1, 0)

		aq := &mockAuthQuery{
			AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
				return &authtypes.QueryAccountResponse{Account: accountAny}, nil
			},
		}
		data := withdrawTxData(t, &billingtypes.MsgWithdrawResponse{
			WithdrawalCount: 7,
			HasMore:         true,
			NextKey:         []byte("cursor-1"),
		})
		ts := &mockTxService{
			SimulateFn: okSimulate(200000),
			BroadcastTxFn: func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
				return &tx.BroadcastTxResponse{TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TXWITHDRAW"}}, nil
			},
			GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
				return &tx.GetTxResponse{TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash, Data: data}}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.signerPool = pool; c.authQuery = aq; c.txService = ts })

		hash, resp, err := c.WithdrawByProvider(t.Context(), "prov-1", nil)
		require.NoError(t, err)
		assert.Equal(t, "TXWITHDRAW", hash)
		require.NotNil(t, resp)
		assert.Equal(t, uint64(7), resp.WithdrawalCount)
		assert.True(t, resp.HasMore)
		assert.Equal(t, []byte("cursor-1"), resp.NextKey)
	})

	t.Run("forwards cursor into MsgWithdraw.Key", func(t *testing.T) {
		s := newTestSigner(t)
		pool := newTestSignerPoolFromSigner(s)
		addr, err := sdktypes.AccAddressFromBech32(s.address)
		require.NoError(t, err)
		accountAny := newTestAccountAny(t, addr, 1, 0)

		aq := &mockAuthQuery{
			AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
				return &authtypes.QueryAccountResponse{Account: accountAny}, nil
			},
		}
		data := withdrawTxData(t, &billingtypes.MsgWithdrawResponse{})
		var sentKey []byte
		ts := &mockTxService{
			SimulateFn: okSimulate(200000),
			BroadcastTxFn: func(_ context.Context, req *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
				decoded, decErr := s.txConfig.TxDecoder()(req.TxBytes)
				require.NoError(t, decErr)
				msgs := decoded.GetMsgs()
				require.Len(t, msgs, 1)
				wd, ok := msgs[0].(*billingtypes.MsgWithdraw)
				require.True(t, ok, "broadcast msg must be *MsgWithdraw")
				sentKey = wd.Key
				return &tx.BroadcastTxResponse{TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TXKEY"}}, nil
			},
			GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
				return &tx.GetTxResponse{TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash, Data: data}}, nil
			},
		}
		c := newMockClient(func(c *Client) { c.signerPool = pool; c.authQuery = aq; c.txService = ts })

		_, _, err = c.WithdrawByProvider(t.Context(), "prov-1", []byte("cursor-xyz"))
		require.NoError(t, err)
		assert.Equal(t, []byte("cursor-xyz"), sentKey, "the opaque cursor must be forwarded into MsgWithdraw.Key")
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
			SimulateFn: okSimulate(200000),
			BroadcastTxFn: func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
				return nil, status.Error(codes.Internal, "node down")
			},
		}
		c := newMockClient(func(c *Client) { c.signerPool = pool; c.authQuery = aq; c.txService = ts })

		_, _, err := c.WithdrawByProvider(t.Context(), "prov-1", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to withdraw")
	})
}

func TestDecodeWithdrawResponse(t *testing.T) {
	valid := withdrawTxData(t, &billingtypes.MsgWithdrawResponse{WithdrawalCount: 3, HasMore: false})

	t.Run("valid", func(t *testing.T) {
		resp, err := decodeWithdrawResponse(valid)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), resp.WithdrawalCount)
		assert.Empty(t, resp.NextKey)
	})

	t.Run("uppercase hex", func(t *testing.T) {
		resp, err := decodeWithdrawResponse(strings.ToUpper(valid))
		require.NoError(t, err)
		assert.Equal(t, uint64(3), resp.WithdrawalCount)
	})

	t.Run("empty data has no responses", func(t *testing.T) {
		_, err := decodeWithdrawResponse("")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no message responses")
	})

	t.Run("invalid hex", func(t *testing.T) {
		_, err := decodeWithdrawResponse("zz")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "decode tx data hex")
	})

	t.Run("wrong response type", func(t *testing.T) {
		otherBz, err := (&billingtypes.MsgWithdraw{}).Marshal()
		require.NoError(t, err)
		msgData := sdktypes.TxMsgData{MsgResponses: []*codectypes.Any{{
			TypeUrl: "/some.other.Type",
			Value:   otherBz,
		}}}
		bz, err := msgData.Marshal()
		require.NoError(t, err)

		_, err = decodeWithdrawResponse(hex.EncodeToString(bz))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected msg response type")
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
		SimulateFn: okSimulate(200000),
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
		SimulateFn: okSimulate(200000),
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

func TestClient_BroadcastTxWithSigner_SequenceMismatchRetry(t *testing.T) {
	// Sequence mismatch (code 32) should trigger retry and succeed on 2nd attempt
	s := newTestSigner(t)
	pool := newTestSignerPoolFromSigner(s)
	addr, _ := sdktypes.AccAddressFromBech32(s.address)
	accountAny := newTestAccountAny(t, addr, 1, 0)

	var attempts atomic.Int32
	aq := &mockAuthQuery{
		AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			return &authtypes.QueryAccountResponse{Account: accountAny}, nil
		},
	}
	ts := &mockTxService{
		SimulateFn: okSimulate(200000),
		BroadcastTxFn: func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			n := attempts.Add(1)
			if n == 1 {
				// First attempt: sequence mismatch
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 32, Codespace: "sdk", RawLog: "account sequence mismatch"},
				}, nil
			}
			// Second attempt: success
			return &tx.BroadcastTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TX_RETRY"},
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
		c.authQuery = aq
		c.txService = ts
	})

	hash, err := c.broadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, defaultBroadcastOpts())
	require.NoError(t, err)
	assert.Equal(t, "TX_RETRY", hash)
	assert.GreaterOrEqual(t, int(attempts.Load()), 2, "should have retried after sequence mismatch")
}

func TestClient_BroadcastTxWithSigner_SequenceHintUsed(t *testing.T) {
	// When the chain returns a sequence mismatch with "expected N", the retry
	// must sign with sequence N (verified by decoding the tx bytes).
	s := newTestSigner(t)
	pool := newTestSignerPoolFromSigner(s)
	addr, _ := sdktypes.AccAddressFromBech32(s.address)
	// On-chain sequence is 5, but chain expects 7 (stale query)
	accountAny := newTestAccountAny(t, addr, 1, 5)

	var attempts atomic.Int32
	var secondAttemptSeq uint64
	aq := &mockAuthQuery{
		AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			// Always return stale sequence 5
			return &authtypes.QueryAccountResponse{Account: accountAny}, nil
		},
	}

	ts := &mockTxService{
		SimulateFn: okSimulate(200000),
		BroadcastTxFn: func(_ context.Context, req *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			n := attempts.Add(1)
			if n == 1 {
				// First attempt: sequence mismatch, chain says expected 7
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{
						Code:      32,
						Codespace: "sdk",
						RawLog:    "account sequence mismatch, expected 7, got 5: incorrect account sequence",
					},
				}, nil
			}
			// Decode the tx to verify the sequence hint was applied
			secondAttemptSeq = mustDecodeTxSequence(s, req.TxBytes)
			return &tx.BroadcastTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TX_SEQHINT"},
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
		c.authQuery = aq
		c.txService = ts
	})

	hash, err := c.broadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, defaultBroadcastOpts())
	require.NoError(t, err)
	assert.Equal(t, "TX_SEQHINT", hash)
	assert.Equal(t, int32(2), attempts.Load(), "should have retried exactly once")
	assert.Equal(t, uint64(7), secondAttemptSeq, "retry must use the expected sequence from the error, not the stale query")
}

func TestClient_BroadcastTxWithSigner_ConsecutiveMismatches(t *testing.T) {
	// Two consecutive sequence mismatches with different expected values.
	// The override must update on each iteration, not just the first.
	s := newTestSigner(t)
	pool := newTestSignerPoolFromSigner(s)
	addr, _ := sdktypes.AccAddressFromBech32(s.address)
	accountAny := newTestAccountAny(t, addr, 1, 5)

	var attempts atomic.Int32
	var secondAttemptSeq, thirdAttemptSeq uint64
	aq := &mockAuthQuery{
		AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			return &authtypes.QueryAccountResponse{Account: accountAny}, nil
		},
	}

	ts := &mockTxService{
		SimulateFn: okSimulate(200000),
		BroadcastTxFn: func(_ context.Context, req *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			n := attempts.Add(1)
			switch n {
			case 1:
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{
						Code: 32, Codespace: "sdk",
						RawLog: "account sequence mismatch, expected 7, got 5: incorrect account sequence",
					},
				}, nil
			case 2:
				// Verify the first override was applied, then return another mismatch
				secondAttemptSeq = mustDecodeTxSequence(s, req.TxBytes)
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{
						Code: 32, Codespace: "sdk",
						RawLog: "account sequence mismatch, expected 8, got 7: incorrect account sequence",
					},
				}, nil
			case 3:
				thirdAttemptSeq = mustDecodeTxSequence(s, req.TxBytes)
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TX_DOUBLE"},
				}, nil
			default:
				panic(fmt.Sprintf("unexpected broadcast attempt %d", n))
			}
		},
		GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
			return &tx.GetTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash},
			}, nil
		},
	}
	c := newMockClient(func(c *Client) {
		c.signerPool = pool
		c.authQuery = aq
		c.txService = ts
	})

	hash, err := c.broadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, defaultBroadcastOpts())
	require.NoError(t, err)
	assert.Equal(t, "TX_DOUBLE", hash)
	assert.Equal(t, int32(3), attempts.Load())
	assert.Equal(t, uint64(7), secondAttemptSeq, "first retry must use the first expected sequence")
	assert.Equal(t, uint64(8), thirdAttemptSeq, "second retry must use the updated expected sequence")
}

func TestClient_BroadcastTxWithSigner_OverrideClearedOnNonSequenceError(t *testing.T) {
	// Sequence mismatch sets the override, then a non-sequence retryable error
	// (gRPC Unavailable) must clear it so the next attempt re-queries the chain.
	s := newTestSigner(t)
	pool := newTestSignerPoolFromSigner(s)
	addr, _ := sdktypes.AccAddressFromBech32(s.address)

	var attempts atomic.Int32
	var queryCount atomic.Int32
	var secondAttemptSeq, thirdAttemptSeq uint64
	// First two queries see stale seq 5; third sees updated seq 9
	aq := &mockAuthQuery{
		AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			n := queryCount.Add(1)
			if n >= 3 {
				// Chain has caught up by the third attempt
				return &authtypes.QueryAccountResponse{
					Account: newTestAccountAny(t, addr, 1, 9),
				}, nil
			}
			return &authtypes.QueryAccountResponse{
				Account: newTestAccountAny(t, addr, 1, 5),
			}, nil
		},
	}

	ts := &mockTxService{
		SimulateFn: okSimulate(200000),
		BroadcastTxFn: func(_ context.Context, req *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			n := attempts.Add(1)
			switch n {
			case 1:
				// Sequence mismatch — sets override to 7
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{
						Code: 32, Codespace: "sdk",
						RawLog: "account sequence mismatch, expected 7, got 5: incorrect account sequence",
					},
				}, nil
			case 2:
				// Verify the override was applied, then return a transient gRPC error
				secondAttemptSeq = mustDecodeTxSequence(s, req.TxBytes)
				return nil, status.Error(codes.Unavailable, "connection reset")
			case 3:
				// Third attempt: should use fresh chain query (seq 9), not stale override (7)
				thirdAttemptSeq = mustDecodeTxSequence(s, req.TxBytes)
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TX_CLEARED"},
				}, nil
			default:
				panic(fmt.Sprintf("unexpected broadcast attempt %d", n))
			}
		},
		GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
			return &tx.GetTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash},
			}, nil
		},
	}
	c := newMockClient(func(c *Client) {
		c.signerPool = pool
		c.authQuery = aq
		c.txService = ts
	})

	hash, err := c.broadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, defaultBroadcastOpts())
	require.NoError(t, err)
	assert.Equal(t, "TX_CLEARED", hash)
	assert.Equal(t, int32(3), attempts.Load())
	assert.Equal(t, uint64(7), secondAttemptSeq, "second attempt must use the override from the mismatch")
	assert.Equal(t, uint64(9), thirdAttemptSeq, "after a non-sequence error, override must be cleared and chain query used")
}

func TestClient_BroadcastTxWithSigner_Code19WaitForTxTimeoutIsNotRetried(t *testing.T) {
	// When code 19 fires and waitForTx times out, the error must NOT be retried
	// (re-broadcasting identical bytes would hit code 19 again).
	s := newTestSigner(t)
	pool := newTestSignerPoolFromSigner(s)
	addr, _ := sdktypes.AccAddressFromBech32(s.address)
	accountAny := newTestAccountAny(t, addr, 1, 0)

	var broadcastCount atomic.Int32
	aq := &mockAuthQuery{
		AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			return &authtypes.QueryAccountResponse{Account: accountAny}, nil
		},
	}
	ts := &mockTxService{
		SimulateFn: okSimulate(200000),
		BroadcastTxFn: func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			broadcastCount.Add(1)
			return &tx.BroadcastTxResponse{
				TxResponse: &sdktypes.TxResponse{
					Code:      19,
					Codespace: "sdk",
					RawLog:    "tx already exists in cache",
					TxHash:    "MEMPOOL_TIMEOUT",
				},
			}, nil
		},
		GetTxFn: func(context.Context, *tx.GetTxRequest, ...grpc.CallOption) (*tx.GetTxResponse, error) {
			return nil, status.Error(codes.NotFound, "not indexed")
		},
	}
	c := newMockClient(func(c *Client) {
		c.signerPool = pool
		c.authQuery = aq
		c.txService = ts
		c.txTimeout = 100 * time.Millisecond
	})

	_, err := c.broadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, defaultBroadcastOpts())
	require.Error(t, err)
	assert.Equal(t, int32(1), broadcastCount.Load(), "should not retry after code 19 timeout")

	var chainErr *ChainTxError
	require.ErrorAs(t, err, &chainErr)
	assert.Equal(t, uint32(19), chainErr.Code)
}

func TestClient_BroadcastTxWithSigner_FirstTimeoutThenCode19Recovery(t *testing.T) {
	// End-to-end: first broadcast accepted → waitForTx timeout → retry →
	// code 19 (same tx still in mempool) → waitForTx succeeds.
	s := newTestSigner(t)
	pool := newTestSignerPoolFromSigner(s)
	addr, _ := sdktypes.AccAddressFromBech32(s.address)
	accountAny := newTestAccountAny(t, addr, 1, 0)

	var broadcastCount atomic.Int32
	var round2GetTxCount atomic.Int32

	aq := &mockAuthQuery{
		AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			return &authtypes.QueryAccountResponse{Account: accountAny}, nil
		},
	}
	ts := &mockTxService{
		SimulateFn: okSimulate(200000),
		BroadcastTxFn: func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			n := broadcastCount.Add(1)
			if n == 1 {
				// First attempt: accepted into mempool
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TX_GHOST"},
				}, nil
			}
			// Retry: same bytes → code 19
			return &tx.BroadcastTxResponse{
				TxResponse: &sdktypes.TxResponse{
					Code:      19,
					Codespace: "sdk",
					RawLog:    "tx already exists in cache",
					TxHash:    "TX_GHOST",
				},
			}, nil
		},
		GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
			if broadcastCount.Load() == 1 {
				// First waitForTx invocation: always not found (ghost tx)
				return nil, status.Error(codes.NotFound, "not indexed")
			}
			// Second waitForTx invocation (after code 19): succeed on 2nd poll
			n := round2GetTxCount.Add(1)
			if n < 2 {
				return nil, status.Error(codes.NotFound, "not indexed yet")
			}
			return &tx.GetTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash},
			}, nil
		},
	}
	c := newMockClient(func(c *Client) {
		c.signerPool = pool
		c.authQuery = aq
		c.txService = ts
		c.txTimeout = 200 * time.Millisecond
	})

	hash, err := c.broadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, defaultBroadcastOpts())
	require.NoError(t, err)
	assert.Equal(t, "TX_GHOST", hash)
	assert.Equal(t, int32(2), broadcastCount.Load(), "should broadcast exactly twice")
}

// mustDecodeTxSequence decodes transaction bytes and returns the sequence from the sole signature.
// Panics on failure to keep the signature simple for use inside mock callbacks.
func mustDecodeTxSequence(s *Signer, txBytes []byte) uint64 {
	decodedTx, err := s.txConfig.TxDecoder()(txBytes)
	if err != nil {
		panic(fmt.Sprintf("failed to decode tx: %v", err))
	}
	sigTx, ok := decodedTx.(authsigning.SigVerifiableTx)
	if !ok {
		panic("decoded tx does not implement authsigning.SigVerifiableTx")
	}
	sigs, err := sigTx.GetSignaturesV2()
	if err != nil {
		panic(fmt.Sprintf("failed to get signatures: %v", err))
	}
	if len(sigs) == 0 {
		panic("expected at least one signature")
	}
	return sigs[0].Sequence
}

// mustDecodeTxGasLimit decodes transaction bytes and returns the gas limit.
// Panics on failure to keep the signature simple for use inside mock callbacks.
func mustDecodeTxGasLimit(s *Signer, txBytes []byte) uint64 {
	decodedTx, err := s.txConfig.TxDecoder()(txBytes)
	if err != nil {
		panic(fmt.Sprintf("failed to decode tx: %v", err))
	}
	feeTx, ok := decodedTx.(sdktypes.FeeTx)
	if !ok {
		panic("decoded tx does not implement sdk.FeeTx")
	}
	return feeTx.GetGas()
}

func TestClient_BroadcastTxWithSigner_OutOfGasRetry(t *testing.T) {
	// Out-of-gas (code 11) at execution should trigger retry with 1.5x gas limit.
	s := newTestSigner(t)
	pool := newTestSignerPoolFromSigner(s)
	addr, _ := sdktypes.AccAddressFromBech32(s.address)
	accountAny := newTestAccountAny(t, addr, 1, 0)

	var attempts atomic.Int32
	var retryGasLimit uint64
	aq := &mockAuthQuery{
		AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			return &authtypes.QueryAccountResponse{Account: accountAny}, nil
		},
	}
	ts := &mockTxService{
		SimulateFn: okSimulate(200000),
		BroadcastTxFn: func(_ context.Context, req *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			n := attempts.Add(1)
			if n == 2 {
				retryGasLimit = mustDecodeTxGasLimit(s, req.TxBytes)
			}
			// Mempool always accepts
			return &tx.BroadcastTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: fmt.Sprintf("TX_%d", n)},
			}, nil
		},
		GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
			n := attempts.Load()
			if n == 1 {
				// First attempt: execution fails with out-of-gas
				return &tx.GetTxResponse{
					TxResponse: &sdktypes.TxResponse{
						Code:      11,
						Codespace: "sdk",
						RawLog:    "out of gas in location: WriteFlat; gasWanted: 200000, gasUsed: 200100: out of gas",
						TxHash:    req.Hash,
					},
				}, nil
			}
			// Second attempt: success
			return &tx.GetTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash},
			}, nil
		},
	}
	c := newMockClient(func(c *Client) {
		c.signerPool = pool
		c.authQuery = aq
		c.txService = ts
	})

	hash, err := c.broadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, defaultBroadcastOpts())
	require.NoError(t, err)
	assert.Equal(t, "TX_2", hash)
	assert.Equal(t, int32(2), attempts.Load(), "should have retried exactly once")
	assert.Equal(t, s.gasLimit+s.gasLimit/2, retryGasLimit, "retry must use 1.5x the default gas limit")
}

func TestClient_BroadcastTxWithSigner_ConsecutiveOutOfGas(t *testing.T) {
	// Two consecutive out-of-gas errors: gas must compound (1.5x, then 2.25x of original).
	s := newTestSigner(t)
	pool := newTestSignerPoolFromSigner(s)
	addr, _ := sdktypes.AccAddressFromBech32(s.address)
	accountAny := newTestAccountAny(t, addr, 1, 0)

	var attempts atomic.Int32
	var secondGasLimit, thirdGasLimit uint64
	aq := &mockAuthQuery{
		AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			return &authtypes.QueryAccountResponse{Account: accountAny}, nil
		},
	}
	ts := &mockTxService{
		SimulateFn: okSimulate(200000),
		BroadcastTxFn: func(_ context.Context, req *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			n := attempts.Add(1)
			switch n {
			case 2:
				secondGasLimit = mustDecodeTxGasLimit(s, req.TxBytes)
			case 3:
				thirdGasLimit = mustDecodeTxGasLimit(s, req.TxBytes)
			}
			return &tx.BroadcastTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: fmt.Sprintf("TX_%d", n)},
			}, nil
		},
		GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
			n := attempts.Load()
			if n <= 2 {
				// First two attempts: execution fails with out-of-gas
				return &tx.GetTxResponse{
					TxResponse: &sdktypes.TxResponse{
						Code:      11,
						Codespace: "sdk",
						RawLog:    "out of gas",
						TxHash:    req.Hash,
					},
				}, nil
			}
			// Third attempt: success
			return &tx.GetTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash},
			}, nil
		},
	}
	c := newMockClient(func(c *Client) {
		c.signerPool = pool
		c.authQuery = aq
		c.txService = ts
	})

	hash, err := c.broadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, defaultBroadcastOpts())
	require.NoError(t, err)
	assert.Equal(t, "TX_3", hash)
	assert.Equal(t, int32(3), attempts.Load())

	expectedFirst := s.gasLimit + s.gasLimit/2        // 1.5x
	expectedSecond := expectedFirst + expectedFirst/2 // 2.25x of original
	assert.Equal(t, expectedFirst, secondGasLimit, "first retry must use 1.5x gas")
	assert.Equal(t, expectedSecond, thirdGasLimit, "second retry must use 2.25x gas (compounding)")
}

func TestClient_BroadcastTxWithSigner_OutOfGasRetryCappedByMaxGasLimit(t *testing.T) {
	// When maxGasLimit is set, the gas override must not exceed it.
	s := newTestSigner(t)
	s.maxGasLimit = 250000 // cap below the 1.5x value (300000)
	pool := newTestSignerPoolFromSigner(s)
	addr, _ := sdktypes.AccAddressFromBech32(s.address)
	accountAny := newTestAccountAny(t, addr, 1, 0)

	var attempts atomic.Int32
	var retryGasLimit uint64
	aq := &mockAuthQuery{
		AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			return &authtypes.QueryAccountResponse{Account: accountAny}, nil
		},
	}
	ts := &mockTxService{
		SimulateFn: okSimulate(200000),
		BroadcastTxFn: func(_ context.Context, req *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			n := attempts.Add(1)
			if n == 2 {
				retryGasLimit = mustDecodeTxGasLimit(s, req.TxBytes)
			}
			return &tx.BroadcastTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: fmt.Sprintf("TX_%d", n)},
			}, nil
		},
		GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
			n := attempts.Load()
			if n == 1 {
				return &tx.GetTxResponse{
					TxResponse: &sdktypes.TxResponse{
						Code: 11, Codespace: "sdk", RawLog: "out of gas", TxHash: req.Hash,
					},
				}, nil
			}
			return &tx.GetTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash},
			}, nil
		},
	}
	c := newMockClient(func(c *Client) {
		c.signerPool = pool
		c.authQuery = aq
		c.txService = ts
	})

	hash, err := c.broadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, defaultBroadcastOpts())
	require.NoError(t, err)
	assert.Equal(t, "TX_2", hash)
	assert.Equal(t, uint64(250000), retryGasLimit, "retry gas must be capped at maxGasLimit")
}

func TestClient_BroadcastTxWithSigner_OutOfGasOverridePersistsThroughTransientError(t *testing.T) {
	// Gas override must NOT be cleared on non-OOG errors (unlike seqOverride).
	// Scenario: OOG on attempt 1, gRPC Unavailable on attempt 2, attempt 3 must still use elevated gas.
	s := newTestSigner(t)
	pool := newTestSignerPoolFromSigner(s)
	addr, _ := sdktypes.AccAddressFromBech32(s.address)
	accountAny := newTestAccountAny(t, addr, 1, 0)

	var attempts atomic.Int32
	var thirdAttemptGas uint64
	aq := &mockAuthQuery{
		AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			return &authtypes.QueryAccountResponse{Account: accountAny}, nil
		},
	}
	ts := &mockTxService{
		SimulateFn: okSimulate(200000),
		BroadcastTxFn: func(_ context.Context, req *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			n := attempts.Add(1)
			switch n {
			case 1:
				// Mempool accepts, execution will OOG
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TX_1"},
				}, nil
			case 2:
				// Transient gRPC error
				return nil, status.Error(codes.Unavailable, "connection reset")
			case 3:
				thirdAttemptGas = mustDecodeTxGasLimit(s, req.TxBytes)
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TX_3"},
				}, nil
			default:
				panic(fmt.Sprintf("unexpected broadcast attempt %d", n))
			}
		},
		GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
			n := attempts.Load()
			if n == 1 {
				// First attempt: execution OOG
				return &tx.GetTxResponse{
					TxResponse: &sdktypes.TxResponse{
						Code: 11, Codespace: "sdk", RawLog: "out of gas", TxHash: req.Hash,
					},
				}, nil
			}
			return &tx.GetTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash},
			}, nil
		},
	}
	c := newMockClient(func(c *Client) {
		c.signerPool = pool
		c.authQuery = aq
		c.txService = ts
	})

	hash, err := c.broadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, defaultBroadcastOpts())
	require.NoError(t, err)
	assert.Equal(t, "TX_3", hash)
	assert.Equal(t, int32(3), attempts.Load())
	assert.Equal(t, s.gasLimit+s.gasLimit/2, thirdAttemptGas,
		"gas override must persist through transient gRPC error, not be cleared")
}

func TestClient_BroadcastTxWithSigner_OutOfGasThenSequenceMismatch(t *testing.T) {
	// Both overrides active: OOG sets gasLimitOverride, then sequence mismatch
	// sets seqOverride. The final attempt must use both.
	s := newTestSigner(t)
	pool := newTestSignerPoolFromSigner(s)
	addr, _ := sdktypes.AccAddressFromBech32(s.address)
	accountAny := newTestAccountAny(t, addr, 1, 5)

	var attempts atomic.Int32
	var thirdAttemptGas uint64
	var thirdAttemptSeq uint64
	aq := &mockAuthQuery{
		AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			return &authtypes.QueryAccountResponse{Account: accountAny}, nil
		},
	}
	ts := &mockTxService{
		SimulateFn: okSimulate(200000),
		BroadcastTxFn: func(_ context.Context, req *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			n := attempts.Add(1)
			switch n {
			case 1:
				// Mempool accepts, execution will OOG
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TX_1"},
				}, nil
			case 2:
				// Sequence mismatch at mempool
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{
						Code: 32, Codespace: "sdk",
						RawLog: "account sequence mismatch, expected 7, got 5: incorrect account sequence",
					},
				}, nil
			case 3:
				thirdAttemptGas = mustDecodeTxGasLimit(s, req.TxBytes)
				thirdAttemptSeq = mustDecodeTxSequence(s, req.TxBytes)
				return &tx.BroadcastTxResponse{
					TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TX_3"},
				}, nil
			default:
				panic(fmt.Sprintf("unexpected broadcast attempt %d", n))
			}
		},
		GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
			n := attempts.Load()
			if n == 1 {
				return &tx.GetTxResponse{
					TxResponse: &sdktypes.TxResponse{
						Code: 11, Codespace: "sdk", RawLog: "out of gas", TxHash: req.Hash,
					},
				}, nil
			}
			return &tx.GetTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: req.Hash},
			}, nil
		},
	}
	c := newMockClient(func(c *Client) {
		c.signerPool = pool
		c.authQuery = aq
		c.txService = ts
	})

	hash, err := c.broadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, defaultBroadcastOpts())
	require.NoError(t, err)
	assert.Equal(t, "TX_3", hash)
	assert.Equal(t, int32(3), attempts.Load())
	assert.Equal(t, s.gasLimit+s.gasLimit/2, thirdAttemptGas,
		"gas override from OOG must persist through sequence mismatch")
	assert.Equal(t, uint64(7), thirdAttemptSeq,
		"sequence override from mismatch must also be applied")
}

func TestClient_BroadcastTxWithSigner_OutOfGasRetriesExhausted(t *testing.T) {
	// All 3 attempts fail with OOG. Verify the error propagates with ChainTxError details.
	s := newTestSigner(t)
	pool := newTestSignerPoolFromSigner(s)
	addr, _ := sdktypes.AccAddressFromBech32(s.address)
	accountAny := newTestAccountAny(t, addr, 1, 0)

	var attempts atomic.Int32
	aq := &mockAuthQuery{
		AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			return &authtypes.QueryAccountResponse{Account: accountAny}, nil
		},
	}
	ts := &mockTxService{
		SimulateFn: okSimulate(200000),
		BroadcastTxFn: func(_ context.Context, _ *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			n := attempts.Add(1)
			return &tx.BroadcastTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: fmt.Sprintf("TX_%d", n)},
			}, nil
		},
		GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
			return &tx.GetTxResponse{
				TxResponse: &sdktypes.TxResponse{
					Code: 11, Codespace: "sdk", RawLog: "out of gas", TxHash: req.Hash,
				},
			}, nil
		},
	}
	c := newMockClient(func(c *Client) {
		c.signerPool = pool
		c.authQuery = aq
		c.txService = ts
	})

	_, err := c.broadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, defaultBroadcastOpts())
	require.Error(t, err)
	assert.Equal(t, int32(3), attempts.Load(), "should exhaust all 3 attempts")

	// The error must be unwrappable to ChainTxError for callers to inspect
	var chainErr *ChainTxError
	require.True(t, errors.As(err, &chainErr), "exhausted error must contain ChainTxError")
	assert.True(t, chainErr.IsOutOfGas(), "ChainTxError must be out-of-gas")
}

func TestClient_BroadcastTxWithSigner_OutOfGasFutileRetryAtCap(t *testing.T) {
	// When maxGasLimit == gasLimit, the first OOG retry is already capped at the same value.
	// The code should detect this and stop immediately (backoff.Permanent) instead of
	// wasting fees on retries with identical gas.
	s := newTestSigner(t)
	s.maxGasLimit = s.gasLimit // cap equals base — no room to escalate
	pool := newTestSignerPoolFromSigner(s)
	addr, _ := sdktypes.AccAddressFromBech32(s.address)
	accountAny := newTestAccountAny(t, addr, 1, 0)

	var attempts atomic.Int32
	aq := &mockAuthQuery{
		AccountFn: func(context.Context, *authtypes.QueryAccountRequest, ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			return &authtypes.QueryAccountResponse{Account: accountAny}, nil
		},
	}
	ts := &mockTxService{
		SimulateFn: okSimulate(200000),
		BroadcastTxFn: func(_ context.Context, _ *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			n := attempts.Add(1)
			return &tx.BroadcastTxResponse{
				TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: fmt.Sprintf("TX_%d", n)},
			}, nil
		},
		GetTxFn: func(_ context.Context, req *tx.GetTxRequest, _ ...grpc.CallOption) (*tx.GetTxResponse, error) {
			return &tx.GetTxResponse{
				TxResponse: &sdktypes.TxResponse{
					Code: 11, Codespace: "sdk", RawLog: "out of gas", TxHash: req.Hash,
				},
			}, nil
		},
	}
	c := newMockClient(func(c *Client) {
		c.signerPool = pool
		c.authQuery = aq
		c.txService = ts
	})

	_, err := c.broadcastTxWithSigner(t.Context(), pool.Primary(), []sdktypes.Msg{newTestMsg(s.Address())}, defaultBroadcastOpts())
	require.Error(t, err)
	assert.Equal(t, int32(1), attempts.Load(),
		"must stop after 1 attempt — retrying at the same gas is futile")

	var chainErr *ChainTxError
	require.True(t, errors.As(err, &chainErr))
	assert.True(t, chainErr.IsOutOfGas())
}

func TestClient_BroadcastBatchedMsgs_SignerAcquiredOnce(t *testing.T) {
	// With >100 leases (multiple sub-batches), Acquire should be called exactly once.
	// We verify via account queries: each sub-batch queries the signer's account,
	// so if signer is acquired once, all queries target the same address.
	pool := newTestSignerPool(t, 2)

	var mu sync.Mutex
	var queriedAddrs []string

	s := pool.Primary()
	addr, _ := sdktypes.AccAddressFromBech32(s.address)
	accountAny := newTestAccountAny(t, addr, 1, 0)
	subAddrs := pool.SubSignerAddresses()
	subAddr0, _ := sdktypes.AccAddressFromBech32(subAddrs[0])
	subAccountAny0 := newTestAccountAny(t, subAddr0, 2, 0)
	subAddr1, _ := sdktypes.AccAddressFromBech32(subAddrs[1])
	subAccountAny1 := newTestAccountAny(t, subAddr1, 3, 0)

	aq := &mockAuthQuery{
		AccountFn: func(_ context.Context, req *authtypes.QueryAccountRequest, _ ...grpc.CallOption) (*authtypes.QueryAccountResponse, error) {
			mu.Lock()
			queriedAddrs = append(queriedAddrs, req.Address)
			mu.Unlock()
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
	ts := &mockTxService{
		SimulateFn: okSimulate(200000),
		BroadcastTxFn: func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
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
		c.providerAddress = pool.ProviderAddress()
		c.authQuery = aq
		c.txService = ts
	})

	// Create 150 leases — should produce 2 sub-batches (100 + 50) with maxLeasesPerBatch=100
	uuids := make([]string, 150)
	for i := range uuids {
		uuids[i] = fmt.Sprintf("l-%d", i)
	}

	n, hashes, err := c.AcknowledgeLeases(t.Context(), uuids)
	require.NoError(t, err)
	assert.Equal(t, uint64(150), n)
	assert.Len(t, hashes, 2, "should produce 2 sub-batches")

	// All account queries should be for the same address (signer acquired once)
	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(queriedAddrs), 2, "should have at least 2 account queries (one per sub-batch)")
	for _, addr := range queriedAddrs[1:] {
		assert.Equal(t, queriedAddrs[0], addr, "all sub-batches should query the same signer address")
	}
}

// ---------------------------------------------------------------------------
// Task 7: simulateGas tests
// ---------------------------------------------------------------------------

func TestClient_simulateGas_happyPath(t *testing.T) {
	c, signer := newTestClientWithGas(t /*gasLimit*/, 1_500_000 /*maxGasLimit*/, 0 /*adj*/, 1.2)
	c.txService = &mockTxService{SimulateFn: func(_ context.Context, _ *tx.SimulateRequest, _ ...grpc.CallOption) (*tx.SimulateResponse, error) {
		return &tx.SimulateResponse{GasInfo: &sdktypes.GasInfo{GasUsed: 200000}}, nil
	}}
	gas, acct, err := c.simulateGas(context.Background(), signer, []sdktypes.Msg{newTestMsg(signer.Address())})
	if err != nil {
		t.Fatalf("simulateGas: %v", err)
	}
	if gas != 240000 { // floor(1.2 * 200000)
		t.Fatalf("gas = %d, want 240000", gas)
	}
	if acct == nil {
		t.Fatalf("expected preAccount returned for reuse")
	}
}

func TestClient_simulateGas_rejectsOverCap(t *testing.T) {
	c, signer := newTestClientWithGas(t, 1_500_000 /*gasLimit*/, 2_000_000 /*maxGasLimit*/, 1.2)
	c.txService = &mockTxService{SimulateFn: func(_ context.Context, _ *tx.SimulateRequest, _ ...grpc.CallOption) (*tx.SimulateResponse, error) {
		return &tx.SimulateResponse{GasInfo: &sdktypes.GasInfo{GasUsed: 3_400_000}}, nil // adjusted ≈ 4.08M > 2M cap
	}}
	_, _, err := c.simulateGas(context.Background(), signer, []sdktypes.Msg{newTestMsg(signer.Address())})
	if !errors.Is(err, errGasExceedsCap) {
		t.Fatalf("want errGasExceedsCap, got %v", err)
	}
}

func TestClient_simulateGas_transientError(t *testing.T) {
	c, signer := newTestClientWithGas(t, 1_500_000, 0, 1.2)
	c.txService = &mockTxService{SimulateFn: func(_ context.Context, _ *tx.SimulateRequest, _ ...grpc.CallOption) (*tx.SimulateResponse, error) {
		return nil, status.Error(codes.Unavailable, "rpc down")
	}}
	_, acct, err := c.simulateGas(context.Background(), signer, []sdktypes.Msg{newTestMsg(signer.Address())})
	if err == nil || errors.Is(err, errGasExceedsCap) {
		t.Fatalf("want transient (non-cap) error, got %v", err)
	}
	if acct == nil {
		t.Fatalf("transient error must still return the account for attempt-1 reuse")
	}
}

// ---------------------------------------------------------------------------
// Pre-seed behavior tests (Task 8)
// ---------------------------------------------------------------------------

func TestClient_broadcast_preSeedsSimulatedGas(t *testing.T) {
	// simulateGas returns 200000 gas used; with adj=1.2, adjustGas(200000)=240000.
	// broadcastTxWithSigner must seed gasLimitOverride=240000 so the tx is signed at that limit.
	c, signer := newTestClientWithGas(t, 1_500_000, 0, 1.2)
	var signedGas uint64
	c.txService = &mockTxService{
		SimulateFn:    okSimulate(200000),
		BroadcastTxFn: captureGas(t, &signedGas, c, signer),
		GetTxFn:       okGetTx(),
	}
	_, err := c.broadcastTxWithSigner(context.Background(), signer, []sdktypes.Msg{newTestMsg(signer.Address())}, defaultBroadcastOpts())
	if err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	if signedGas != 240000 {
		t.Fatalf("broadcast declared gas = %d, want 240000 (simulated×1.2)", signedGas)
	}
}

func TestClient_broadcast_refusedIsTerminal(t *testing.T) {
	// simulateGas returns gasUsed=3_400_000; with adj=1.2, adjusted≈4.08M > 2M cap →
	// errGasExceedsCap is returned; the tx must NOT be broadcast.
	c, signer := newTestClientWithGas(t, 1_500_000, 2_000_000, 1.2)
	broadcastCalled := false
	c.txService = &mockTxService{
		SimulateFn: okSimulate(3_400_000),
		BroadcastTxFn: func(_ context.Context, _ *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			broadcastCalled = true
			return nil, nil
		},
	}
	_, err := c.broadcastTxWithSigner(context.Background(), signer, []sdktypes.Msg{newTestMsg(signer.Address())}, defaultBroadcastOpts())
	if !errors.Is(err, errGasExceedsCap) {
		t.Fatalf("want errGasExceedsCap, got %v", err)
	}
	if broadcastCalled {
		t.Fatalf("refused tx must NOT be broadcast")
	}
}

func TestClient_broadcast_fallbackOnTransientSim(t *testing.T) {
	// simulateGas fails with a transient RPC error; broadcast must proceed using
	// FallbackGas() = floor(1.2 * 1_500_000) = 1_800_000 (not raw gasLimit).
	c, signer := newTestClientWithGas(t, 1_500_000, 0, 1.2)
	var signedGas uint64
	c.txService = &mockTxService{
		SimulateFn: func(_ context.Context, _ *tx.SimulateRequest, _ ...grpc.CallOption) (*tx.SimulateResponse, error) {
			return nil, status.Error(codes.Unavailable, "down")
		},
		BroadcastTxFn: captureGas(t, &signedGas, c, signer),
		GetTxFn:       okGetTx(),
	}
	_, err := c.broadcastTxWithSigner(context.Background(), signer, []sdktypes.Msg{newTestMsg(signer.Address())}, defaultBroadcastOpts())
	if err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	if signedGas != 1_800_000 { // FallbackGas() = floor(1.2 * 1.5M), NOT raw 1.5M
		t.Fatalf("fallback declared gas = %d, want 1800000", signedGas)
	}
}

// ---------------------------------------------------------------------------
// Task-9: circuit-breaker tests
// ---------------------------------------------------------------------------

func TestClient_simBreaker_opensAfterConsecutiveFailures(t *testing.T) {
	c, signer := newTestClientWithGas(t, 1_500_000, 0, 1.2)
	var simCalls int
	c.txService = &mockTxService{
		SimulateFn: func(_ context.Context, _ *tx.SimulateRequest, _ ...grpc.CallOption) (*tx.SimulateResponse, error) {
			simCalls++
			return nil, status.Error(codes.Unavailable, "down")
		},
		BroadcastTxFn: okBroadcast(), GetTxFn: okGetTx(),
	}
	for i := 0; i < simBreakerThreshold+2; i++ {
		_, _ = c.broadcastTxWithSigner(context.Background(), signer, []sdktypes.Msg{newTestMsg(signer.Address())}, defaultBroadcastOpts())
	}
	if simCalls > simBreakerThreshold {
		t.Fatalf("breaker open: Simulate should be skipped after %d failures, got %d calls", simBreakerThreshold, simCalls)
	}
}

// §8.17 — half-open recovery: advancing the injected clock past the cooldown
// re-attempts Simulate (this is the whole reason `now` is a seam).
func TestClient_simBreaker_halfOpenRecovers(t *testing.T) {
	c, signer := newTestClientWithGas(t, 1_500_000, 0, 1.2)
	clock := time.Unix(1_000_000, 0)
	c.now = func() time.Time { return clock } // controllable clock (overrides newMockClient's time.Now)
	failing := true
	var simCalls int
	c.txService = &mockTxService{
		SimulateFn: func(_ context.Context, _ *tx.SimulateRequest, _ ...grpc.CallOption) (*tx.SimulateResponse, error) {
			simCalls++
			if failing {
				return nil, status.Error(codes.Unavailable, "down")
			}
			return &tx.SimulateResponse{GasInfo: &sdktypes.GasInfo{GasUsed: 200000}}, nil
		},
		BroadcastTxFn: okBroadcast(), GetTxFn: okGetTx(),
	}
	for i := 0; i < simBreakerThreshold; i++ { // open the breaker
		_, _ = c.broadcastTxWithSigner(context.Background(), signer, []sdktypes.Msg{newTestMsg(signer.Address())}, defaultBroadcastOpts())
	}
	openCalls := simCalls
	_, _ = c.broadcastTxWithSigner(context.Background(), signer, []sdktypes.Msg{newTestMsg(signer.Address())}, defaultBroadcastOpts())
	if simCalls != openCalls {
		t.Fatalf("breaker should be open (Simulate skipped); got an extra call")
	}
	failing = false
	clock = clock.Add(simBreakerCooldown + time.Second) // advance past cooldown → half-open
	_, _ = c.broadcastTxWithSigner(context.Background(), signer, []sdktypes.Msg{newTestMsg(signer.Address())}, defaultBroadcastOpts())
	if simCalls != openCalls+1 {
		t.Fatalf("after cooldown, Simulate must be re-attempted (half-open); got %d, want %d", simCalls, openCalls+1)
	}
}

// ---------------------------------------------------------------------------
// Task 10: broadcastMultiMsgTx fold + preAccount single-query
// ---------------------------------------------------------------------------

func TestClient_broadcastMultiMsgTx_simulatesGas(t *testing.T) {
	c, signer := newTestClientWithGas(t, 1_500_000, 0, 1.2)
	var signedGas uint64
	c.txService = &mockTxService{SimulateFn: okSimulate(180000), BroadcastTxFn: captureGas(t, &signedGas, c, signer), GetTxFn: okGetTx()}
	_, err := c.broadcastMultiMsgTx(context.Background(), []sdktypes.Msg{newTestMsg(signer.Address()), newTestMsg(signer.Address())})
	if err != nil {
		t.Fatalf("broadcastMultiMsgTx: %v", err)
	}
	if signedGas != 216000 { // floor(1.2 * 180000) — simulated, not the static gas_limit
		t.Fatalf("multi-msg declared gas = %d, want 216000 (simulated)", signedGas)
	}
}

func TestClient_broadcast_singleAccountQueryOnFirstAttempt(t *testing.T) {
	c, signer := newTestClientWithGas(t, 1_500_000, 0, 1.2)
	var accountQueries int
	c.authQuery = countingAuthQuery(c.authQuery, &accountQueries)
	c.txService = &mockTxService{SimulateFn: okSimulate(200000), BroadcastTxFn: okBroadcast(), GetTxFn: okGetTx()}
	_, _ = c.broadcastTxWithSigner(context.Background(), signer, []sdktypes.Msg{newTestMsg(signer.Address())}, defaultBroadcastOpts())
	if accountQueries != 1 {
		t.Fatalf("attempt 1 must reuse the sim account; want 1 Account query, got %d", accountQueries)
	}
}

// ---------------------------------------------------------------------------
// Task 11: Startup batch fail-fast budget lock
// ---------------------------------------------------------------------------

func TestClient_broadcastMultiMsgTx_failsFast(t *testing.T) {
	c, signer := newTestClientWithGas(t, 1_500_000, 0, 1.2)
	var broadcastCalls int
	c.txService = &mockTxService{
		SimulateFn: okSimulate(180000),
		BroadcastTxFn: func(context.Context, *tx.BroadcastTxRequest, ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			broadcastCalls++
			return nil, status.Error(codes.Unavailable, "batch down") // retryable
		},
	}
	_, err := c.broadcastMultiMsgTx(context.Background(), []sdktypes.Msg{newTestMsg(signer.Address()), newTestMsg(signer.Address())})
	if err == nil {
		t.Fatalf("expected error")
	}
	if broadcastCalls != 1 {
		t.Fatalf("broadcastMultiMsgTx must fail fast: want 1 broadcast attempt, got %d (ladder would retry)", broadcastCalls)
	}
}

// ---------------------------------------------------------------------------
// Task 11b: §8 invariant regression tests
// ---------------------------------------------------------------------------

// §8.8 — OOG ladder climbs FROM the simulated value (not from the configured gas_limit).
func TestClient_ladderClimbsFromSimulated(t *testing.T) {
	c, signer := newTestClientWithGas(t, 1_500_000, 0, 1.2)
	var declared []uint64
	c.txService = &mockTxService{
		SimulateFn: okSimulate(200000), // → 240000 seed (gasAdjustment 1.2)
		BroadcastTxFn: func(_ context.Context, req *tx.BroadcastTxRequest, _ ...grpc.CallOption) (*tx.BroadcastTxResponse, error) {
			decoded, _ := signer.txConfig.TxDecoder()(req.TxBytes)
			declared = append(declared, decoded.(sdktypes.FeeTx).GetGas())
			return &tx.BroadcastTxResponse{TxResponse: &sdktypes.TxResponse{Code: 0, TxHash: "TX"}}, nil
		},
		GetTxFn: outOfGasGetTxOnce(), // first execution OOGs (code 11) → exactly one ladder bump
	}
	if _, err := c.broadcastTxWithSigner(context.Background(), signer, []sdktypes.Msg{newTestMsg(signer.Address())}, defaultBroadcastOpts()); err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	if len(declared) < 2 || declared[0] != 240000 || declared[1] != 360000 { // 240000 then ×1.5
		t.Fatalf("ladder must climb from simulated 240000→360000, got %v", declared)
	}
}

// §8.11 — Simulate runs exactly once per call; a seq-mismatch retry must NOT re-simulate.
func TestClient_simulateOncePerCall(t *testing.T) {
	c, signer := newTestClientWithGas(t, 1_500_000, 0, 1.2)
	var simCalls int
	c.txService = &mockTxService{
		SimulateFn: func(_ context.Context, _ *tx.SimulateRequest, _ ...grpc.CallOption) (*tx.SimulateResponse, error) {
			simCalls++
			return &tx.SimulateResponse{GasInfo: &sdktypes.GasInfo{GasUsed: 200000}}, nil
		},
		BroadcastTxFn: seqMismatchBroadcastOnce(), // 1st ack: code-32 seq-mismatch → retry; 2nd: ok
		GetTxFn:       okGetTx(),
	}
	_, _ = c.broadcastTxWithSigner(context.Background(), signer, []sdktypes.Msg{newTestMsg(signer.Address())}, defaultBroadcastOpts())
	if simCalls != 1 {
		t.Fatalf("Simulate must run once per call (gas is seq-independent), even across a seq-mismatch retry; got %d", simCalls)
	}
}

// §8.15 — FallbackGas overflow degrades to the raw gas_limit, NEVER to 0.
func TestClient_fallbackOverflowDegradesToRaw(t *testing.T) {
	const big = uint64(1)<<63 - 1 // MaxInt64; ×3.0 overflows uint64 in adjustGas
	c, signer := newTestClientWithGas(t, big, 0, 3.0)
	var declared uint64
	c.txService = &mockTxService{
		SimulateFn: func(context.Context, *tx.SimulateRequest, ...grpc.CallOption) (*tx.SimulateResponse, error) {
			return nil, status.Error(codes.Unavailable, "down")
		},
		BroadcastTxFn: captureGas(t, &declared, c, signer),
		GetTxFn:       okGetTx(),
	}
	_, _ = c.broadcastTxWithSigner(context.Background(), signer, []sdktypes.Msg{newTestMsg(signer.Address())}, defaultBroadcastOpts())
	if declared != big { // degrades to raw gas_limit, NEVER 0
		t.Fatalf("FallbackGas overflow must degrade to raw gas_limit %d, got %d", big, declared)
	}
}

// §8.6 — max_gas_limit==0 means unbounded: a large simulated GasUsed must NOT be refused or capped.
func TestClient_noCapWhenMaxGasLimitZero(t *testing.T) {
	c, signer := newTestClientWithGas(t, 1_500_000, 0 /*maxGasLimit*/, 1.2)
	// Set SimulateFn BEFORE calling simulateGas — the default mockTxService panics when SimulateFn is nil.
	c.txService = &mockTxService{SimulateFn: okSimulate(10_000_000)}
	got, _, err := c.simulateGas(context.Background(), signer, []sdktypes.Msg{newTestMsg(signer.Address())})
	if err != nil {
		t.Fatalf("must not refuse when max_gas_limit==0: %v", err)
	}
	if got != 12_000_000 {
		t.Fatalf("uncapped gas = %d, want 12000000", got)
	}
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
