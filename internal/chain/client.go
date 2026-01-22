package chain

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	sdktypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/query"
	"github.com/cosmos/cosmos-sdk/types/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	skutypes "github.com/manifest-network/manifest-ledger/x/sku/types"
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
		txTimeout = 30 * time.Second
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
	leases, err := c.getLeasesByProviderWithState(ctx, providerUUID, billingtypes.LEASE_STATE_PENDING)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending leases: %w", err)
	}
	return leases, nil
}

// GetActiveLease returns an active lease by UUID, or nil if not found or not active.
func (c *Client) GetActiveLease(ctx context.Context, leaseUUID string) (*billingtypes.Lease, error) {
	resp, err := c.billingQuery.Lease(ctx, &billingtypes.QueryLeaseRequest{
		LeaseUuid: leaseUUID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query lease: %w", err)
	}

	if resp.Lease.State != billingtypes.LEASE_STATE_ACTIVE {
		return nil, nil
	}

	return &resp.Lease, nil
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

// AcknowledgeLeases acknowledges the given leases. Returns the number of leases acknowledged and tx hashes.
func (c *Client) AcknowledgeLeases(ctx context.Context, leaseUUIDs []string) (uint64, []string, error) {
	if len(leaseUUIDs) == 0 {
		return 0, nil, nil
	}

	// Batch up to 100 leases per transaction
	const batchSize = 100
	var totalAcknowledged uint64
	var txHashes []string

	for i := 0; i < len(leaseUUIDs); i += batchSize {
		end := i + batchSize
		if end > len(leaseUUIDs) {
			end = len(leaseUUIDs)
		}
		batch := leaseUUIDs[i:end]

		msg := &billingtypes.MsgAcknowledgeLease{
			Sender:     c.signer.Address(),
			LeaseUuids: batch,
		}

		txHash, err := c.broadcastTx(ctx, msg)
		if err != nil {
			return totalAcknowledged, txHashes, fmt.Errorf("failed to acknowledge leases: %w", err)
		}

		slog.Info("acknowledged leases", "count", len(batch), "tx_hash", txHash)
		txHashes = append(txHashes, txHash)
		totalAcknowledged += uint64(len(batch))
	}

	return totalAcknowledged, txHashes, nil
}

// WithdrawByProvider withdraws funds from all active leases for a provider.
// Returns the total amounts withdrawn, whether there are more leases to process, and the tx hash.
func (c *Client) WithdrawByProvider(ctx context.Context, providerUUID string) (sdktypes.Coins, bool, string, error) {
	msg := &billingtypes.MsgWithdraw{
		Sender:       c.signer.Address(),
		ProviderUuid: providerUUID,
		Limit:        100,
	}

	txHash, err := c.broadcastTx(ctx, msg)
	if err != nil {
		return nil, false, "", fmt.Errorf("failed to withdraw: %w", err)
	}

	slog.Info("withdrawal completed", "tx_hash", txHash)
	return nil, false, txHash, nil
}

// broadcastTx signs and broadcasts a transaction, waits for execution, returns tx hash.
func (c *Client) broadcastTx(ctx context.Context, msg sdktypes.Msg) (string, error) {
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
		return "", fmt.Errorf("transaction rejected: %s", resp.TxResponse.RawLog)
	}

	// Wait for tx to be included in a block and check execution result
	txHash := resp.TxResponse.TxHash
	execResp, err := c.waitForTx(ctx, txHash)
	if err != nil {
		return "", fmt.Errorf("failed waiting for tx %s: %w", txHash, err)
	}

	if execResp.TxResponse.Code != 0 {
		return "", fmt.Errorf("transaction failed (code %d): %s", execResp.TxResponse.Code, execResp.TxResponse.RawLog)
	}

	return txHash, nil
}

// waitForTx polls for a transaction until it's included in a block.
func (c *Client) waitForTx(ctx context.Context, txHash string) (*tx.GetTxResponse, error) {
	ticker := time.NewTicker(c.txPollInterval)
	defer ticker.Stop()

	timeout := time.After(c.txTimeout)
	var consecutiveErrors int
	var lastErr error

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeout:
			if lastErr != nil {
				return nil, fmt.Errorf("timeout waiting for tx %s (last error: %v)", txHash, lastErr)
			}
			return nil, fmt.Errorf("timeout waiting for tx %s", txHash)
		case <-ticker.C:
			resp, err := c.txService.GetTx(ctx, &tx.GetTxRequest{Hash: txHash})
			if err != nil {
				consecutiveErrors++
				lastErr = err
				// Log warning after several consecutive failures to help with debugging
				if consecutiveErrors == 5 {
					slog.Warn("repeated errors polling for tx",
						"tx_hash", txHash,
						"consecutive_errors", consecutiveErrors,
						"last_error", err,
					)
				}
				// Keep polling - tx may not be indexed yet
				continue
			}
			return resp, nil
		}
	}
}

// GetActiveLeasesByProvider returns all active leases for a provider.
func (c *Client) GetActiveLeasesByProvider(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error) {
	leases, err := c.getLeasesByProviderWithState(ctx, providerUUID, billingtypes.LEASE_STATE_ACTIVE)
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

// CloseLeases closes the given leases with an optional reason. Returns the number of leases closed and tx hashes.
func (c *Client) CloseLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error) {
	if len(leaseUUIDs) == 0 {
		return 0, nil, nil
	}

	// Batch up to 100 leases per transaction
	const batchSize = 100
	var totalClosed uint64
	var txHashes []string

	for i := 0; i < len(leaseUUIDs); i += batchSize {
		end := i + batchSize
		if end > len(leaseUUIDs) {
			end = len(leaseUUIDs)
		}
		batch := leaseUUIDs[i:end]

		msg := &billingtypes.MsgCloseLease{
			Sender:     c.signer.Address(),
			LeaseUuids: batch,
			Reason:     reason,
		}

		txHash, err := c.broadcastTx(ctx, msg)
		if err != nil {
			return totalClosed, txHashes, fmt.Errorf("failed to close leases: %w", err)
		}

		slog.Info("closed leases", "count", len(batch), "reason", reason, "tx_hash", txHash)
		txHashes = append(txHashes, txHash)
		totalClosed += uint64(len(batch))
	}

	return totalClosed, txHashes, nil
}
