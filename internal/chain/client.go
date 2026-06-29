package chain

import (
	"cmp"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	stdmath "math"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	sdktypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/query"
	"github.com/cosmos/cosmos-sdk/types/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	"github.com/cosmos/cosmos-sdk/x/authz"

	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
	skutypes "github.com/manifest-network/manifest-ledger/x/sku/types"

	"github.com/manifest-network/fred/internal/metrics"
)

const (
	// maxLeasesPerBatch is the maximum number of leases to acknowledge in a single transaction.
	// Used by AcknowledgeLeases to batch acknowledgements.
	maxLeasesPerBatch = 100

	// txMaxRetries is the maximum number of retries for failed transactions.
	txMaxRetries = 3

	// txInitialBackoff is the initial backoff duration between retries.
	// Aligned with block time (~6s on mainnet) so that a sequence mismatch
	// (stale committed state) resolves after the pending tx lands in a block.
	txInitialBackoff = 6 * time.Second

	// txMaxBackoff is the maximum backoff duration between retries.
	// ~3 blocks: enough for the chain to include any in-flight txs.
	txMaxBackoff = 18 * time.Second

	// txPollInitialInterval is the initial polling interval for tx confirmation.
	txPollInitialInterval = 500 * time.Millisecond

	// txPollMaxInterval is the maximum polling interval (for exponential backoff).
	txPollMaxInterval = 5 * time.Second

	// txPollBackoffFactor is the multiplier for exponential backoff.
	txPollBackoffFactor = 1.5

	// defaultTxTimeout is the default timeout for waiting for tx inclusion.
	defaultTxTimeout = 60 * time.Second

	// simBreakerThreshold is the number of consecutive Simulate failures before
	// the circuit-breaker opens and Simulate is skipped for simBreakerCooldown.
	simBreakerThreshold = 5

	// simBreakerCooldown is the duration the breaker stays open (skipping
	// Simulate) after simBreakerThreshold consecutive failures.
	simBreakerCooldown = 30 * time.Second

	// simTimeout is the per-Simulate RPC deadline, decoupled from the broadcast
	// deadline so a slow node doesn't exhaust the whole broadcast budget.
	simTimeout = 5 * time.Second
)

// simBreaker is a consecutive-failure circuit-breaker for the Simulate RPC.
// Zero value is ready to use (breaker starts closed). All methods are safe for
// concurrent use.
type simBreaker struct {
	mu           sync.Mutex
	consecutive  int
	openUntilUTC time.Time
}

// allow reports whether Simulate should be attempted at time now.
// Returns true when the breaker is closed or the cooldown has expired.
func (b *simBreaker) allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !now.Before(b.openUntilUTC)
}

// recordSuccess resets the consecutive-failure counter and closes the breaker.
func (b *simBreaker) recordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutive = 0
	b.openUntilUTC = time.Time{}
}

// recordFailure increments the consecutive counter and opens the breaker when
// the threshold is reached.
func (b *simBreaker) recordFailure(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutive++
	if b.consecutive >= simBreakerThreshold {
		b.openUntilUTC = now.Add(simBreakerCooldown)
	}
}

// fallbackGasOrRaw returns the FallbackGas for the signer, degrading to the
// raw gas_limit on overflow (never returns 0).
func fallbackGasOrRaw(signer *Signer) uint64 {
	fb, err := signer.FallbackGas()
	if err != nil {
		slog.Error("FallbackGas overflow; using raw gas_limit", "error", err, "gas_limit", signer.gasLimit)
		return signer.gasLimit
	}
	return fb
}

// broadcastOpts carries per-call broadcast tuning.
type broadcastOpts struct {
	maxRetries uint64 // total attempts for the OOG/sequence ladder (default txMaxRetries)
}

func defaultBroadcastOpts() broadcastOpts { return broadcastOpts{maxRetries: txMaxRetries} }

// Client provides methods to query and submit transactions to the chain.
type Client struct {
	conn            *grpc.ClientConn
	signerPool      *SignerPool
	providerAddress string
	billingQuery    billingtypes.QueryClient
	skuQuery        skutypes.QueryClient
	authQuery       authtypes.QueryClient
	txService       tx.ServiceClient
	txPollInterval  time.Duration
	txTimeout       time.Duration
	queryPageLimit  uint64
	simBreaker      simBreaker       // consecutive-failure circuit-breaker for Simulate
	now             func() time.Time // injectable clock (time.Now in production; overridable in tests)
}

// ClientConfig holds configuration for the chain client.
type ClientConfig struct {
	Endpoint       string
	TLSEnabled     bool
	TLSCAFile      string        // Path to CA certificate file (optional, uses system CAs if empty)
	TLSSkipVerify  bool          // Skip certificate verification (for testing only)
	TxPollInterval time.Duration // Interval for polling tx status (default: 500ms)
	TxTimeout      time.Duration // Timeout for waiting for tx inclusion (default: 60s)
	QueryPageLimit int           // Page limit for paginated queries (default: 100)
}

// NewClient creates a new chain client connected to the given gRPC endpoint.
// The pool parameter is required; pass a single-signer pool for backward compatibility.
func NewClient(cfg ClientConfig, pool *SignerPool) (*Client, error) {
	if pool == nil {
		return nil, fmt.Errorf("signer pool is required")
	}
	dialOpts := []grpc.DialOption{
		// Keepalive must respect the server's enforcement policy.
		// Cosmos SDK / gRPC defaults: MinTime=5m, PermitWithoutStream=false.
		// Pinging more aggressively triggers GOAWAY ENHANCE_YOUR_CALM.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                5 * time.Minute,
			Timeout:             10 * time.Second,
			PermitWithoutStream: false,
		}),
	}

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

	// Apply defaults using cmp.Or (returns first non-zero value)
	txPollInterval := cmp.Or(cfg.TxPollInterval, 500*time.Millisecond)
	txTimeout := cmp.Or(cfg.TxTimeout, defaultTxTimeout)
	queryPageLimit := cmp.Or(max(cfg.QueryPageLimit, 0), 100)

	return &Client{
		conn:            conn,
		signerPool:      pool,
		providerAddress: pool.ProviderAddress(),
		billingQuery:    billingtypes.NewQueryClient(conn),
		skuQuery:        skutypes.NewQueryClient(conn),
		authQuery:       authtypes.NewQueryClient(conn),
		txService:       tx.NewServiceClient(conn),
		txPollInterval:  txPollInterval,
		txTimeout:       txTimeout,
		queryPageLimit:  uint64(queryPageLimit),
		now:             time.Now,
	}, nil
}

// buildTLSConfig creates a TLS configuration for gRPC.
func buildTLSConfig(caFile string, skipVerify bool) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: skipVerify, //nolint:gosec // G402: opt-in operator config grpc_tls_skip_verify (rejected under production_mode, see config.Validate); not tenant-controlled. Matches tlsconfig.go's annotation.
	}

	// If a CA file is specified, load it
	if caFile != "" {
		caCert, err := os.ReadFile(caFile) //nolint:gosec // G304: caFile is the operator-configured gRPC TLS CA path, read at startup — not tenant-reachable
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

// Conn returns the underlying gRPC connection for creating query clients.
func (c *Client) Conn() *grpc.ClientConn {
	return c.conn
}

// broadcastMultiMsgTx broadcasts multiple messages in a single transaction
// using the primary signer. Returns error on failure; callers handle fallback.
func (c *Client) broadcastMultiMsgTx(ctx context.Context, msgs []sdktypes.Msg) (string, error) {
	if len(msgs) == 0 {
		return "", nil
	}
	if len(msgs) == 1 {
		return c.broadcastTx(ctx, msgs[0])
	}

	signer := c.signerPool.Primary()
	accResp, err := c.authQuery.Account(ctx, &authtypes.QueryAccountRequest{
		Address: signer.Address(),
	})
	if err != nil {
		return "", fmt.Errorf("failed to query account: %w", err)
	}

	txBytes, err := signer.SignTxMulti(ctx, msgs, accResp.Account)
	if err != nil {
		return "", fmt.Errorf("failed to sign multi-msg transaction: %w", err)
	}

	resp, err := c.txService.BroadcastTx(ctx, &tx.BroadcastTxRequest{
		TxBytes: txBytes,
		Mode:    tx.BroadcastMode_BROADCAST_MODE_SYNC,
	})
	if err != nil {
		return "", fmt.Errorf("failed to broadcast transaction: %w", err)
	}

	if resp.TxResponse.Code != 0 {
		return "", &ChainTxError{
			Code:      resp.TxResponse.Code,
			Codespace: resp.TxResponse.Codespace,
			RawLog:    resp.TxResponse.RawLog,
		}
	}

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

// simulateGas estimates gas for msgs via the Simulate RPC, applies
// gas_adjustment, and enforces the max_gas_limit reject-cap. It returns the
// adjusted gas, the account it queried (so the caller can reuse it for the
// first broadcast attempt — same committed sequence, no duplicate query), and
// an error. A returned errGasExceedsCap is TERMINAL (do not fall back).
func (c *Client) simulateGas(ctx context.Context, signer *Signer, msgs []sdktypes.Msg) (uint64, *codectypes.Any, error) {
	accResp, err := c.authQuery.Account(ctx, &authtypes.QueryAccountRequest{Address: signer.Address()})
	if err != nil {
		return 0, nil, fmt.Errorf("simulate: query account: %w", err)
	}
	simBytes, err := signer.BuildSimTx(msgs, accResp.Account, simDeclaredGas) // small fixed declared gas (NOT gasLimit/maxGasLimit)
	if err != nil {
		return 0, accResp.Account, fmt.Errorf("simulate: build: %w", err)
	}
	simCtx, cancel := context.WithTimeout(ctx, simTimeout)
	defer cancel()
	simRes, err := c.txService.Simulate(simCtx, &tx.SimulateRequest{TxBytes: simBytes})
	if err != nil {
		return 0, accResp.Account, fmt.Errorf("simulate: %w", err)
	}
	if simRes.GasInfo == nil { // malformed/empty response from a buggy RPC — classify transient (→ fallback), don't nil-panic
		return 0, accResp.Account, fmt.Errorf("simulate: nil gas_info")
	}
	adjusted, err := signer.adjustGas(simRes.GasInfo.GasUsed) // read GasUsed; adjustment only (no clamp)
	if err != nil {
		return 0, accResp.Account, err
	}
	if signer.maxGasLimit > 0 && adjusted > signer.maxGasLimit {
		metrics.GasSimulationTotal.WithLabelValues("refused").Inc()
		return 0, accResp.Account, fmt.Errorf("%w: estimated gas %d exceeds max_gas_limit %d", errGasExceedsCap, adjusted, signer.maxGasLimit)
	}
	return adjusted, accResp.Account, nil
}

// recordQueryMetrics records duration for a chain query.
func recordQueryMetrics(queryName string, start time.Time) {
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
		Address: c.providerAddress,
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
	recordQueryMetrics("get_pending_leases", start)
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
	recordQueryMetrics("get_lease", start)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
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

	if lease == nil || lease.State != billingtypes.LEASE_STATE_ACTIVE {
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

	// Acquire signer once for all sub-batches — fixed for the entire call.
	// The signer stays the same across sub-batches and retries to avoid
	// sequence mismatches.
	signer, isSub, release := c.signerPool.Acquire()
	defer release()

	var granteeAddr sdktypes.AccAddress
	if isSub {
		var err error
		granteeAddr, err = sdktypes.AccAddressFromBech32(signer.Address())
		if err != nil {
			return 0, nil, fmt.Errorf("invalid sub-signer address: %w", err)
		}
	}

	var totalProcessed uint64
	var txHashes []string

	for batch := range slices.Chunk(leaseUUIDs, maxLeasesPerBatch) {
		innerMsg := msgFactory(batch)

		var msg sdktypes.Msg
		if isSub {
			execMsg := authz.NewMsgExec(granteeAddr, []sdktypes.Msg{innerMsg})
			msg = &execMsg
		} else {
			msg = innerMsg
		}

		txHash, err := c.broadcastTxWithSigner(ctx, signer, []sdktypes.Msg{msg}, defaultBroadcastOpts())
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
				Sender:     c.providerAddress,
				LeaseUuids: batch,
			}
		},
	)
}

// WithdrawByProvider withdraws funds from all active leases for a provider.
// Returns the transaction hash on success.
// It uses Limit: 0 to let the chain use its configured DefaultProviderWithdrawLimit,
// which matches the behavior of the CLI's `manifestd tx billing withdraw --provider` command.
func (c *Client) WithdrawByProvider(ctx context.Context, providerUUID string) (string, error) {
	msg := &billingtypes.MsgWithdraw{
		Sender:       c.providerAddress,
		ProviderUuid: providerUUID,
		Limit:        0, // Use chain's default limit
	}

	txHash, err := c.broadcastTx(ctx, msg)
	recordTxMetrics("withdraw", err)
	if err != nil {
		return "", fmt.Errorf("failed to withdraw: %w", err)
	}

	slog.Info("withdrawal completed", "tx_hash", txHash)
	return txHash, nil
}

// broadcastTx signs and broadcasts a transaction using the primary signer.
// Used for operations that must use the provider key (withdrawals, grants, funding).
func (c *Client) broadcastTx(ctx context.Context, msg sdktypes.Msg) (string, error) {
	return c.broadcastTxWithSigner(ctx, c.signerPool.Primary(), []sdktypes.Msg{msg}, defaultBroadcastOpts())
}

// broadcastTxWithSigner signs and broadcasts a transaction using the given signer.
// The signer is fixed for the entire retry cycle (signer affinity). A single
// Simulate call pre-seeds the gas before the backoff loop; the OOG ladder then
// climbs from that value if execution fails.
func (c *Client) broadcastTxWithSigner(ctx context.Context, signer *Signer, msgs []sdktypes.Msg, opts broadcastOpts) (string, error) {
	var txHash string
	var seqOverride *uint64 // sequence override from a previous sequence-mismatch error

	// Pre-seed: simulate gas once before the backoff loop. The circuit-breaker
	// skips Simulate while open (after simBreakerThreshold consecutive failures)
	// and re-attempts after simBreakerCooldown. Terminal refusal is returned
	// immediately; transient failure degrades to FallbackGas.
	var simGas uint64
	var preAccount *codectypes.Any
	if c.simBreaker.allow(c.now()) {
		var simErr error
		simGas, preAccount, simErr = c.simulateGas(ctx, signer, msgs)
		switch {
		case errors.Is(simErr, errGasExceedsCap):
			// Simulate itself succeeded — the result merely exceeded the cap.
			// Count it as a success so a cap-reject doesn't trip the breaker.
			c.simBreaker.recordSuccess()
			return "", simErr
		case simErr != nil:
			c.simBreaker.recordFailure(c.now())
			simGas = fallbackGasOrRaw(signer)
			metrics.GasSimulationTotal.WithLabelValues("fallback").Inc()
			slog.Warn("gas simulation failed; using fallback ceiling", "error", simErr, "fallback_gas", simGas)
		default:
			c.simBreaker.recordSuccess()
			metrics.GasSimulationTotal.WithLabelValues("simulated").Inc()
		}
	} else {
		simGas = fallbackGasOrRaw(signer) // breaker open: skip Simulate (no RPC round-trip)
		metrics.GasSimulationTotal.WithLabelValues("fallback").Inc()
	}
	metrics.GasSimulated.Observe(float64(simGas))
	// SOLE declaration of gasLimitOverride at function scope. The OOG closure
	// below reassigns this variable (gasLimitOverride = &increased), not a new
	// inner one, so the ladder climbs from simGas.
	gasLimitOverride := &simGas

	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = txInitialBackoff
	bo.MaxInterval = txMaxBackoff
	bo.MaxElapsedTime = 0

	boCtx := backoff.WithContext(
		backoff.WithMaxRetries(bo, opts.maxRetries-1),
		ctx,
	)

	firstAccount := preAccount
	operation := func() error {
		var err error
		txHash, err = c.doBroadcastTxWithSigner(ctx, signer, msgs, seqOverride, gasLimitOverride, firstAccount)
		firstAccount = nil // retries re-query for a fresh sequence
		if err == nil {
			return nil
		}

		if !c.isRetryableTxError(err) {
			return backoff.Permanent(err)
		}

		// On sequence mismatch, extract the expected sequence so the next
		// attempt uses it directly instead of re-querying stale state.
		// Clear on any other error so we don't carry a stale value.
		seqOverride = nil
		var chainErr *ChainTxError
		if errors.As(err, &chainErr) {
			if expected, ok := chainErr.ExpectedSequence(); ok {
				seqOverride = &expected
			} else if chainErr.IsSequenceMismatch() {
				slog.Warn("sequence mismatch but could not parse expected sequence from error",
					"raw_log", chainErr.RawLog,
				)
			}

			// On out-of-gas, increase gas limit by 1.5x for the next attempt.
			// Unlike seqOverride, gasLimitOverride is NOT cleared on non-OOG errors
			// because the message's gas requirement (determined by lease count) does
			// not decrease between retries.
			if chainErr.IsOutOfGas() {
				currentGas := signer.gasLimit
				if gasLimitOverride != nil {
					currentGas = *gasLimitOverride
				}
				increased := currentGas + currentGas/2
				if increased < currentGas {
					// uint64 overflow; clamp to MaxInt64.
					increased = stdmath.MaxInt64
				}
				if signer.maxGasLimit > 0 {
					increased = min(increased, signer.maxGasLimit)
				}
				// Ensure the increased value fits in int64 for fee calculation.
				increased = min(increased, stdmath.MaxInt64)
				if increased == currentGas {
					// Already at the cap; retrying with the same gas is futile.
					metrics.SignerOOGRetriesTotal.WithLabelValues("exhausted").Inc()
					return backoff.Permanent(err)
				}
				metrics.SignerOOGRetriesTotal.WithLabelValues("retried").Inc()
				gasLimitOverride = &increased
			}
		}

		// Dereference for logging so slog prints the value, not a pointer address.
		var seqLog any
		if seqOverride != nil {
			seqLog = *seqOverride
		}
		var gasLog any
		if gasLimitOverride != nil {
			gasLog = *gasLimitOverride
		}
		slog.Warn("transaction failed, will retry",
			"error", err,
			"seq_override", seqLog,
			"gas_limit_override", gasLog,
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

	// Check for ChainTxError: sequence mismatches and out-of-gas are retryable
	var chainErr *ChainTxError
	if errors.As(err, &chainErr) {
		if chainErr.IsSequenceMismatch() || chainErr.IsOutOfGas() {
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
		return c.isRetryableGRPCCode(st.Code())
	}

	// If we can't extract a gRPC status, check the error chain for wrapped gRPC errors
	// by unwrapping and checking each level
	unwrapped := err
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

// doBroadcastTxWithSigner performs a single broadcast attempt using the given signer.
// If seqOverride is non-nil, its value is used instead of the on-chain sequence. This
// handles races where the queried sequence is already stale by the time the transaction
// is broadcast (e.g. another tx from the same signer was committed between query and
// broadcast, or the mempool already holds a tx at the queried sequence).
// If gasLimitOverride is non-nil, its value is used instead of the configured gas limit
// (e.g. after an out-of-gas error triggered a retry with increased gas).
// If preAccount is non-nil, it is used directly (reusing the account queried by
// simulateGas on the first attempt, avoiding a duplicate query).
func (c *Client) doBroadcastTxWithSigner(ctx context.Context, signer *Signer, msgs []sdktypes.Msg, seqOverride *uint64, gasLimitOverride *uint64, preAccount *codectypes.Any) (string, error) {
	// Get account info for the signer's sequence/account number. Reuse the
	// account from simulateGas when preAccount is set (first attempt only);
	// retries pass nil and re-query for a fresh sequence.
	var accountAny *codectypes.Any
	if preAccount != nil {
		accountAny = preAccount
	} else {
		accResp, err := c.authQuery.Account(ctx, &authtypes.QueryAccountRequest{
			Address: signer.Address(),
		})
		if err != nil {
			return "", fmt.Errorf("failed to query account: %w", err)
		}
		accountAny = accResp.Account
	}

	// Build, sign, and broadcast the transaction.
	// When seqOverride is set (from a previous sequence-mismatch error),
	// use it instead of the potentially stale on-chain sequence.
	txBytes, err := signer.signTxInternal(ctx, msgs, accountAny, seqOverride, gasLimitOverride)
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
		chainErr := &ChainTxError{
			Code:      resp.TxResponse.Code,
			Codespace: resp.TxResponse.Codespace,
			RawLog:    resp.TxResponse.RawLog,
		}

		// Code 19: the exact same tx bytes are already in the mempool cache.
		// This happens when waitForTx timed out on a previous attempt and the
		// retry re-signed with the same sequence, producing identical bytes.
		// The tx IS being processed — wait for it instead of returning an error.
		if chainErr.IsTxInMempool() && resp.TxResponse.TxHash != "" {
			slog.Info("tx already in mempool, waiting for confirmation",
				"tx_hash", resp.TxResponse.TxHash,
			)
			txHash := resp.TxResponse.TxHash
			execResp, err := c.waitForTx(ctx, txHash)
			if err != nil {
				// Tx was in mempool but didn't confirm in time. Preserve both
				// the wait error (for timeout/cancel classification) and the
				// code 19 ChainTxError (non-retryable) to prevent the retry
				// loop from re-broadcasting identical bytes.
				return "", fmt.Errorf("tx %s in mempool but did not confirm: %w", txHash, errors.Join(err, chainErr))
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

		return "", chainErr
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
	// Apply transaction timeout to context (takes minimum of parent deadline and txTimeout)
	ctx, cancel := context.WithTimeout(ctx, c.txTimeout)
	defer cancel()

	// Start with initial poll interval, then use exponential backoff
	currentInterval := c.txPollInterval
	if currentInterval == 0 {
		currentInterval = txPollInitialInterval
	}

	var consecutiveErrors int
	var lastErr error
	var pollAttempts int

	for {
		// Create timer for current poll interval
		pollTimer := time.NewTimer(currentInterval)

		select {
		case <-ctx.Done():
			pollTimer.Stop()
			if lastErr != nil {
				return nil, fmt.Errorf("waiting for tx %s after %d attempts: %w (last error: %w)", txHash, pollAttempts, ctx.Err(), lastErr)
			}
			return nil, fmt.Errorf("waiting for tx %s after %d attempts: %w", txHash, pollAttempts, ctx.Err())
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
	recordQueryMetrics("get_active_leases", start)
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
				Sender:     c.providerAddress,
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
				Sender:     c.providerAddress,
				LeaseUuids: batch,
				Reason:     reason,
			}
		},
		"reason", reason,
	)
}
