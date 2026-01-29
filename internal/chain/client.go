package chain

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	sdktypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/query"
	"github.com/cosmos/cosmos-sdk/types/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	skutypes "github.com/manifest-network/manifest-ledger/x/sku/types"

	"github.com/manifest-network/fred/internal/metrics"
)

const (
	// maxLeasesPerBatch is the maximum number of leases to process in a single transaction.
	maxLeasesPerBatch = 100

	// txMaxRetries is the maximum number of retries for failed transactions.
	txMaxRetries = 3

	// txInitialBackoff is the initial backoff duration between retries.
	txInitialBackoff = 500 * time.Millisecond

	// txMaxBackoff is the maximum backoff duration between retries.
	txMaxBackoff = 5 * time.Second

	// txPollInitialInterval is the initial polling interval for tx confirmation.
	txPollInitialInterval = 500 * time.Millisecond

	// txPollMaxInterval is the maximum polling interval (for exponential backoff).
	txPollMaxInterval = 5 * time.Second

	// txPollBackoffFactor is the multiplier for exponential backoff.
	txPollBackoffFactor = 1.5

	// defaultTxTimeout is the default timeout for waiting for tx inclusion.
	defaultTxTimeout = 60 * time.Second
)

// Client provides methods to query and submit transactions to the chain.
type Client struct {
	conn           *grpc.ClientConn
	signer         *Signer
	billingQuery   billingtypes.QueryClient
	skuQuery       skutypes.QueryClient
	authQuery      authtypes.QueryClient
	txService      tx.ServiceClient
	txPollInterval time.Duration
	txTimeout      time.Duration
	queryPageLimit uint64
}

// ClientConfig holds configuration for the chain client.
type ClientConfig struct {
	Endpoint       string
	TLSEnabled     bool
	TLSCAFile      string        // Path to CA certificate file (optional, uses system CAs if empty)
	TLSSkipVerify  bool          // Skip certificate verification (for testing only)
	TxPollInterval time.Duration // Interval for polling tx status (default: 500ms)
	TxTimeout      time.Duration // Timeout for waiting for tx inclusion (default: 30s)
	QueryPageLimit int           // Page limit for paginated queries (default: 100)
}

// NewClient creates a new chain client connected to the given gRPC endpoint.
func NewClient(cfg ClientConfig, signer *Signer) (*Client, error) {
	var dialOpts []grpc.DialOption

	if cfg.TLSEnabled {
		tlsConfig, err := buildTLSConfig(cfg.TLSCAFile, cfg.TLSSkipVerify)
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
		slog.Info("gRPC TLS enabled", "ca_file", cfg.TLSCAFile, "skip_verify", cfg.TLSSkipVerify)
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(cfg.Endpoint, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC endpoint: %w", err)
	}

	// Apply defaults for timeouts
	txPollInterval := cfg.TxPollInterval
	if txPollInterval == 0 {
		txPollInterval = 500 * time.Millisecond
	}
	txTimeout := cfg.TxTimeout
	if txTimeout == 0 {
		txTimeout = defaultTxTimeout
	}
	queryPageLimit := cfg.QueryPageLimit
	if queryPageLimit <= 0 {
		queryPageLimit = 100
	}

	return &Client{
		conn:           conn,
		signer:         signer,
		billingQuery:   billingtypes.NewQueryClient(conn),
		skuQuery:       skutypes.NewQueryClient(conn),
		authQuery:      authtypes.NewQueryClient(conn),
		txService:      tx.NewServiceClient(conn),
		txPollInterval: txPollInterval,
		txTimeout:      txTimeout,
		queryPageLimit: uint64(queryPageLimit),
	}, nil
}

// buildTLSConfig creates a TLS configuration for gRPC.
func buildTLSConfig(caFile string, skipVerify bool) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: skipVerify,
	}

	// If a CA file is specified, load it
	if caFile != "" {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %w", err)
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		tlsConfig.RootCAs = certPool
	}

	return tlsConfig, nil
}

// Close closes the gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// recordQueryMetrics records duration for a chain query.
func recordQueryMetrics(queryName string, start time.Time, err error) {
	duration := time.Since(start).Seconds()
	metrics.ChainQueryDuration.WithLabelValues(queryName).Observe(duration)
}

// recordTxMetrics records outcome for a chain transaction.
func recordTxMetrics(txType string, err error) {
	outcome := metrics.OutcomeSuccess
	if err != nil {
		outcome = metrics.OutcomeError
	}
	metrics.ChainTxTotal.WithLabelValues(txType, outcome).Inc()
}

// Ping checks if the chain connection is healthy by querying the signer's account.
// Returns nil if healthy, otherwise returns the error.
func (c *Client) Ping(ctx context.Context) error {
	// Use a short timeout for health checks
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := c.authQuery.Account(ctx, &authtypes.QueryAccountRequest{
		Address: c.signer.Address(),
	})
	return err
}

// getLeasesByProviderWithState fetches all leases for a provider with the given state filter.
func (c *Client) getLeasesByProviderWithState(ctx context.Context, providerUUID string, state billingtypes.LeaseState) ([]billingtypes.Lease, error) {
	var allLeases []billingtypes.Lease
	var nextKey []byte

	for {
		resp, err := c.billingQuery.LeasesByProvider(ctx, &billingtypes.QueryLeasesByProviderRequest{
			ProviderUuid: providerUUID,
			StateFilter:  state,
			Pagination: &query.PageRequest{
				Key:   nextKey,
				Limit: c.queryPageLimit,
			},
		})
		if err != nil {
			return nil, err
		}

		allLeases = append(allLeases, resp.Leases...)

		if resp.Pagination == nil || len(resp.Pagination.NextKey) == 0 {
			break
		}
		nextKey = resp.Pagination.NextKey
	}

	return allLeases, nil
}

// GetPendingLeases returns all pending leases for a provider.
func (c *Client) GetPendingLeases(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
	start := time.Now()
	leases, err := c.getLeasesByProviderWithState(ctx, providerUUID, billingtypes.LEASE_STATE_PENDING)
	recordQueryMetrics("get_pending_leases", start, err)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending leases: %w", err)
	}
	return leases, nil
}

// GetLease returns a lease by UUID regardless of state.
// Returns nil if the lease is not found.
func (c *Client) GetLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	start := time.Now()
	resp, err := c.billingQuery.Lease(ctx, &billingtypes.QueryLeaseRequest{
		LeaseUuid: leaseUUID,
	})
	recordQueryMetrics("get_lease", start, err)
	if err != nil {
		return nil, fmt.Errorf("failed to query lease: %w", err)
	}

	return &resp.Lease, nil
}

// GetActiveLease returns an active lease by UUID, or nil if not found or not active.
func (c *Client) GetActiveLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	lease, err := c.GetLease(ctx, leaseUUID)
	if err != nil {
		return nil, err
	}

	if lease.State != billingtypes.LEASE_STATE_ACTIVE {
		return nil, nil
	}

	return lease, nil
}

// GetProvider returns provider details by UUID.
func (c *Client) GetProvider(ctx context.Context, providerUUID string) (*skutypes.Provider, error) {
	resp, err := c.skuQuery.Provider(ctx, &skutypes.QueryProviderRequest{
		Uuid: providerUUID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query provider: %w", err)
	}

	return &resp.Provider, nil
}

// broadcastBatchedMsgs broadcasts messages in batches for lease operations.
// It processes leaseUUIDs in chunks of maxLeasesPerBatch, creating a message
// for each batch using msgFactory, broadcasting it, and recording metrics.
//
// Parameters:
//   - metricType: base verb for metrics and errors (e.g., "acknowledge", "reject", "close")
//   - opName: operation name for structured log field (e.g., "acknowledged", "rejected", "closed")
//   - msgFactory: creates the appropriate message type for each batch
//   - extraLogAttrs: additional key-value pairs to include in log output
//
// Returns the total number of leases processed, transaction hashes, and any error.
// On error, returns partial results (leases processed before the error occurred).
func (c *Client) broadcastBatchedMsgs(
	ctx context.Context,
	leaseUUIDs []string,
	metricType string,
	opName string,
	msgFactory func(batch []string) sdktypes.Msg,
	extraLogAttrs ...any,
) (uint64, []string, error) {
	if len(leaseUUIDs) == 0 {
		return 0, nil, nil
	}

	var totalProcessed uint64
	var txHashes []string

	for batch := range slices.Chunk(leaseUUIDs, maxLeasesPerBatch) {
		msg := msgFactory(batch)

		txHash, err := c.broadcastTx(ctx, msg)
		recordTxMetrics(metricType, err)
		if err != nil {
			return totalProcessed, txHashes, fmt.Errorf("failed to %s leases: %w", metricType, err)
		}

		// Build log attributes: operation, count, extra attrs, tx_hash
		attrs := []any{"operation", opName, "count", len(batch)}
		attrs = append(attrs, extraLogAttrs...)
		attrs = append(attrs, "tx_hash", txHash)
		slog.Info("lease batch processed", attrs...)

		txHashes = append(txHashes, txHash)
		totalProcessed += uint64(len(batch))
	}

	return totalProcessed, txHashes, nil
}

// AcknowledgeLeases acknowledges the given leases. Returns the number of leases acknowledged and tx hashes.
func (c *Client) AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
	return c.broadcastBatchedMsgs(ctx, leaseUUIDs, "acknowledge", "acknowledged",
		func(batch []string) sdktypes.Msg {
			return &billingtypes.MsgAcknowledgeLease{
				Sender:     c.signer.Address(),
				LeaseUuids: batch,
			}
		},
	)
}

// WithdrawByProvider withdraws funds from all active leases for a provider.
// Returns the transaction hash on success.
func (c *Client) WithdrawByProvider(ctx context.Context, providerUUID string) (string, error) {
	msg := &billingtypes.MsgWithdraw{
		Sender:       c.signer.Address(),
		ProviderUuid: providerUUID,
		Limit:        maxLeasesPerBatch,
	}

	txHash, err := c.broadcastTx(ctx, msg)
	recordTxMetrics("withdraw", err)
	if err != nil {
		return "", fmt.Errorf("failed to withdraw: %w", err)
	}

	slog.Info("withdrawal completed", "tx_hash", txHash)
	return txHash, nil
}

// broadcastTx signs and broadcasts a transaction, waits for execution, returns tx hash.
// It automatically retries on transient errors and sequence mismatches with exponential backoff.
func (c *Client) broadcastTx(ctx context.Context, msg sdktypes.Msg) (string, error) {
	var txHash string

	// Configure exponential backoff with jitter
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = txInitialBackoff
	bo.MaxInterval = txMaxBackoff
	bo.MaxElapsedTime = 0 // We control max retries via WithMaxRetries

	// Wrap with context and max retries
	boCtx := backoff.WithContext(
		backoff.WithMaxRetries(bo, txMaxRetries-1), // -1 because first attempt doesn't count as retry
		ctx,
	)

	operation := func() error {
		var err error
		txHash, err = c.doBroadcastTx(ctx, msg)
		if err == nil {
			return nil
		}

		// Check if error is retryable
		if !c.isRetryableTxError(err) {
			return backoff.Permanent(err)
		}

		slog.Warn("transaction failed, will retry",
			"error", err,
		)
		return err
	}

	if err := backoff.Retry(operation, boCtx); err != nil {
		return "", fmt.Errorf("transaction failed: %w", err)
	}

	return txHash, nil
}

// isRetryableTxError checks if a transaction error is retryable.
// Uses proper gRPC status codes instead of brittle string matching.
func (c *Client) isRetryableTxError(err error) bool {
	if err == nil {
		return false
	}

	// Check for ChainTxError to see if it's a sequence mismatch
	var chainErr *ChainTxError
	if errors.As(err, &chainErr) {
		// Code 32 is "incorrect account sequence" in SDK
		// Codespace "sdk" with code 32 = ErrWrongSequence
		if chainErr.Codespace == "sdk" && chainErr.Code == 32 {
			return true
		}
		// Other chain errors are generally not retryable
		return false
	}

	// Check for context errors (deadline exceeded, canceled)
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	// Check for retryable gRPC status codes
	// Extract the gRPC status from the error (works with wrapped errors)
	st, ok := status.FromError(err)
	if ok {
		switch st.Code() {
		case codes.Unavailable:
			// Server temporarily unavailable (connection refused, reset, etc.)
			return true
		case codes.DeadlineExceeded:
			// Request timed out
			return true
		case codes.ResourceExhausted:
			// Rate limited or resource exhausted - may recover
			return true
		case codes.Aborted:
			// Operation was aborted (e.g., concurrency conflict) - may succeed on retry
			return true
		case codes.Internal:
			// Internal server error - transient issues may resolve
			return true
		case codes.Unknown:
			// Unknown error - could be transient network issue
			return true
		default:
			// Other codes (InvalidArgument, NotFound, PermissionDenied, etc.)
			// are not retryable as they indicate client errors or permanent failures
			return false
		}
	}

	// If we can't extract a gRPC status, check the error chain for wrapped gRPC errors
	// by unwrapping and checking each level
	var unwrapped error = err
	for unwrapped != nil {
		if st, ok := status.FromError(unwrapped); ok && st.Code() != codes.OK {
			// Found a gRPC status in the chain - recursively check
			return c.isRetryableGRPCCode(st.Code())
		}
		unwrapped = errors.Unwrap(unwrapped)
	}

	return false
}

// isRetryableGRPCCode returns true if the gRPC status code indicates a retryable error.
func (c *Client) isRetryableGRPCCode(code codes.Code) bool {
	switch code {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted,
		codes.Aborted, codes.Internal, codes.Unknown:
		return true
	default:
		return false
	}
}

// doBroadcastTx performs a single broadcast attempt.
func (c *Client) doBroadcastTx(ctx context.Context, msg sdktypes.Msg) (string, error) {
	// Get account info for sequence/account number
	accResp, err := c.authQuery.Account(ctx, &authtypes.QueryAccountRequest{
		Address: c.signer.Address(),
	})
	if err != nil {
		return "", fmt.Errorf("failed to query account: %w", err)
	}

	// Build, sign, and broadcast the transaction
	txBytes, err := c.signer.SignTx(ctx, msg, accResp.Account)
	if err != nil {
		return "", fmt.Errorf("failed to sign transaction: %w", err)
	}

	resp, err := c.txService.BroadcastTx(ctx, &tx.BroadcastTxRequest{
		TxBytes: txBytes,
		Mode:    tx.BroadcastMode_BROADCAST_MODE_SYNC,
	})
	if err != nil {
		return "", fmt.Errorf("failed to broadcast transaction: %w", err)
	}

	// SYNC mode only confirms mempool acceptance, not execution
	if resp.TxResponse.Code != 0 {
		return "", &ChainTxError{
			Code:      resp.TxResponse.Code,
			Codespace: resp.TxResponse.Codespace,
			RawLog:    resp.TxResponse.RawLog,
		}
	}

	// Wait for tx to be included in a block and check execution result
	txHash := resp.TxResponse.TxHash
	execResp, err := c.waitForTx(ctx, txHash)
	if err != nil {
		return "", fmt.Errorf("failed waiting for tx %s: %w", txHash, err)
	}

	if execResp.TxResponse.Code != 0 {
		return "", &ChainTxError{
			Code:      execResp.TxResponse.Code,
			Codespace: execResp.TxResponse.Codespace,
			RawLog:    execResp.TxResponse.RawLog,
		}
	}

	return txHash, nil
}

// waitForTx polls for a transaction until it's included in a block.
// Uses exponential backoff to handle slow chain indexing.
func (c *Client) waitForTx(ctx context.Context, txHash string) (*tx.GetTxResponse, error) {
	// Start with initial poll interval, then use exponential backoff
	currentInterval := c.txPollInterval
	if currentInterval == 0 {
		currentInterval = txPollInitialInterval
	}

	// Use NewTimer instead of time.After to avoid timer leak on early return
	timeoutTimer := time.NewTimer(c.txTimeout)
	defer timeoutTimer.Stop()

	var consecutiveErrors int
	var lastErr error
	var pollAttempts int

	for {
		// Create timer for current poll interval
		pollTimer := time.NewTimer(currentInterval)

		select {
		case <-ctx.Done():
			pollTimer.Stop()
			return nil, ctx.Err()
		case <-timeoutTimer.C:
			pollTimer.Stop()
			if lastErr != nil {
				return nil, fmt.Errorf("timeout waiting for tx %s after %d attempts (last error: %v)", txHash, pollAttempts, lastErr)
			}
			return nil, fmt.Errorf("timeout waiting for tx %s after %d attempts", txHash, pollAttempts)
		case <-pollTimer.C:
			pollAttempts++
			resp, err := c.txService.GetTx(ctx, &tx.GetTxRequest{Hash: txHash})
			if err != nil {
				consecutiveErrors++
				lastErr = err
				// Log warning after several consecutive failures to help with debugging
				if consecutiveErrors == 5 {
					slog.Warn("repeated errors polling for tx",
						"tx_hash", txHash,
						"consecutive_errors", consecutiveErrors,
						"current_interval", currentInterval,
						"last_error", err,
					)
				}

				// Apply exponential backoff for next poll
				currentInterval = time.Duration(float64(currentInterval) * txPollBackoffFactor)
				if currentInterval > txPollMaxInterval {
					currentInterval = txPollMaxInterval
				}

				// Keep polling - tx may not be indexed yet
				continue
			}

			if pollAttempts > 1 {
				slog.Debug("tx confirmed after polling",
					"tx_hash", txHash,
					"attempts", pollAttempts,
				)
			}
			return resp, nil
		}
	}
}

// GetActiveLeasesByProvider returns all active leases for a provider.
func (c *Client) GetActiveLeasesByProvider(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
	start := time.Now()
	leases, err := c.getLeasesByProviderWithState(ctx, providerUUID, billingtypes.LEASE_STATE_ACTIVE)
	recordQueryMetrics("get_active_leases", start, err)
	if err != nil {
		return nil, fmt.Errorf("failed to query active leases: %w", err)
	}
	return leases, nil
}

// GetCreditAccount returns the credit account and balances for a tenant.
func (c *Client) GetCreditAccount(ctx context.Context, tenant string) (*billingtypes.CreditAccount, sdktypes.Coins, error) {
	resp, err := c.billingQuery.CreditAccount(ctx, &billingtypes.QueryCreditAccountRequest{
		Tenant: tenant,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query credit account: %w", err)
	}

	return &resp.CreditAccount, resp.Balances, nil
}

// GetProviderWithdrawable returns the total withdrawable amounts for a provider.
func (c *Client) GetProviderWithdrawable(ctx context.Context, providerUUID string) (sdktypes.Coins, error) {
	// Use a higher limit for totals query (10x normal page limit)
	resp, err := c.billingQuery.ProviderWithdrawable(ctx, &billingtypes.QueryProviderWithdrawableRequest{
		ProviderUuid: providerUUID,
		Limit:        c.queryPageLimit * 10,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query provider withdrawable: %w", err)
	}

	return resp.Amounts, nil
}

// RejectLeases rejects the given pending leases with an optional reason.
// Returns the number of leases rejected and tx hashes.
// This is used when provisioning fails and the provider cannot fulfill the lease.
func (c *Client) RejectLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
	return c.broadcastBatchedMsgs(ctx, leaseUUIDs, "reject", "rejected",
		func(batch []string) sdktypes.Msg {
			return &billingtypes.MsgRejectLease{
				Sender:     c.signer.Address(),
				LeaseUuids: batch,
				Reason:     reason,
			}
		},
		"reason", reason,
	)
}

// CloseLeases closes the given leases with an optional reason. Returns the number of leases closed and tx hashes.
func (c *Client) CloseLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
	return c.broadcastBatchedMsgs(ctx, leaseUUIDs, "close", "closed",
		func(batch []string) sdktypes.Msg {
			return &billingtypes.MsgCloseLease{
				Sender:     c.signer.Address(),
				LeaseUuids: batch,
				Reason:     reason,
			}
		},
		"reason", reason,
	)
}
