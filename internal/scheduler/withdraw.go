package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	sdktypes "github.com/cosmos/cosmos-sdk/types"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"
)

const (
	// Retry configuration for transient failures
	maxRetries     = 3
	initialBackoff = 1 * time.Second
	maxBackoff     = 10 * time.Second

	// depletionCheckBuffer is how far before estimated depletion to schedule the next check.
	// This ensures we catch depletion before it happens.
	depletionCheckBuffer = 10 * time.Second
)

// ChainClient defines the interface for chain operations needed by the scheduler.
type ChainClient interface {
	GetProviderWithdrawable(ctx context.Context, providerUUID string) (sdktypes.Coins, error)
	WithdrawByProvider(ctx context.Context, providerUUID string) (string, error)
	GetActiveLeasesByProvider(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
	GetCreditAccount(ctx context.Context, tenant string) (*billingtypes.CreditAccount, sdktypes.Coins, error)
	CloseLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
}

// tenantState tracks a tenant's balance history for burn rate calculation.
//
// Note: The current implementation assumes a single denomination for credit.
// While the data structures support multiple denoms, the burn rate calculation
// treats each denom independently and finds the earliest depletion time.
// Multi-denom support with proportional burn rates is not implemented.
type tenantState struct {
	lastBalance     sdktypes.Coins
	lastCheckTime   time.Time
	consecutiveErrs int // Track consecutive errors fetching credit account
}

// WithdrawScheduler periodically withdraws funds from active leases and
// auto-closes leases for tenants with depleted credit.
type WithdrawScheduler struct {
	client       ChainClient
	providerUUID string
	interval     time.Duration

	// Configurable limits
	maxWithdrawIterations     int
	creditCheckErrorThreshold int
	creditCheckRetryInterval  time.Duration

	tenants map[string]*tenantState
	mu      sync.Mutex

	// nextCheckTime is dynamically adjusted based on estimated depletion times
	nextCheckTime time.Time

	// ctx is the context for the scheduler, protected by ctxMu
	ctx    context.Context
	cancel context.CancelFunc
	ctxMu  sync.RWMutex

	// wg tracks spawned goroutines for graceful shutdown
	wg *sync.WaitGroup // Pointer to avoid copy-by-value issues
}

// WithdrawSchedulerConfig holds configuration for the withdrawal scheduler.
type WithdrawSchedulerConfig struct {
	ProviderUUID              string
	Interval                  time.Duration
	MaxWithdrawIterations     int
	CreditCheckErrorThreshold int
	CreditCheckRetryInterval  time.Duration
}

// NewWithdrawScheduler creates a new withdrawal scheduler.
func NewWithdrawScheduler(client ChainClient, cfg WithdrawSchedulerConfig) *WithdrawScheduler {
	// Apply defaults
	maxIterations := cfg.MaxWithdrawIterations
	if maxIterations <= 0 {
		maxIterations = 100
	}
	errorThreshold := cfg.CreditCheckErrorThreshold
	if errorThreshold <= 0 {
		errorThreshold = 3
	}
	retryInterval := cfg.CreditCheckRetryInterval
	if retryInterval <= 0 {
		retryInterval = 30 * time.Second
	}

	// Create initial context that can be cancelled on shutdown.
	// This ensures TriggerWithdraw works correctly even before Start() is called.
	// Start() will replace this with a context derived from the passed-in context.
	ctx, cancel := context.WithCancel(context.Background())

	return &WithdrawScheduler{
		client:                    client,
		providerUUID:              cfg.ProviderUUID,
		interval:                  cfg.Interval,
		maxWithdrawIterations:     maxIterations,
		creditCheckErrorThreshold: errorThreshold,
		creditCheckRetryInterval:  retryInterval,
		tenants:                   make(map[string]*tenantState),
		ctx:                       ctx,
		cancel:                    cancel,
		wg:                        &sync.WaitGroup{},
	}
}

// WithdrawOnce performs a single withdrawal and credit check cycle.
// This should be called at startup before Start() to ensure the transaction
// completes before other startup transactions (like lease acknowledgment).
func (s *WithdrawScheduler) WithdrawOnce(ctx context.Context) {
	s.withdraw(ctx)
	s.checkCreditsAndClose(ctx)
}

// TriggerWithdraw triggers an immediate withdrawal cycle.
// This can be called from event handlers when cross-provider credit depletion is detected.
// Uses the scheduler's context so it can be cancelled on shutdown.
func (s *WithdrawScheduler) TriggerWithdraw() {
	slog.Info("withdrawal triggered by cross-provider credit depletion")

	// Get context under read lock - always valid since initialized in NewWithdrawScheduler
	s.ctxMu.RLock()
	ctx := s.ctx
	s.ctxMu.RUnlock()

	// Check if context is already cancelled (scheduler stopped)
	if ctx.Err() != nil {
		slog.Debug("ignoring TriggerWithdraw, scheduler context cancelled")
		return
	}

	// Track the goroutine for graceful shutdown
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.withdrawAndCheckCredits(ctx)
	}()
}

// Stop cancels the scheduler's internal context, stopping all operations.
// This is useful for testing or for cases where you want to stop the scheduler
// without cancelling the parent context passed to Start().
// After Stop() is called, TriggerWithdraw() calls will be ignored.
func (s *WithdrawScheduler) Stop() {
	s.ctxMu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	s.ctxMu.Unlock()

	// Wait for any in-flight operations to complete
	s.wg.Wait()
	slog.Info("withdrawal scheduler stopped via Stop()")
}

// Start begins the periodic withdrawal and credit checking process.
// Note: Call WithdrawOnce() before Start() if you need an initial withdrawal
// that must complete before other operations.
//
// Context inheritance: Start() creates an internal context derived from the passed ctx.
// When ctx is cancelled, the internal context is also cancelled, which stops any
// TriggerWithdraw goroutines that were spawned during operation.
func (s *WithdrawScheduler) Start(ctx context.Context) error {
	slog.Info("starting withdrawal scheduler",
		"provider_uuid", s.providerUUID,
		"interval", s.interval,
	)

	// Replace context for use by TriggerWithdraw (protected by mutex).
	// IMPORTANT: Create the new context BEFORE cancelling the old one to avoid a race
	// where TriggerWithdraw() reads a cancelled context. The sequence is:
	// 1. Create new context
	// 2. Atomically swap the context and cancel function
	// 3. Cancel old context (any TriggerWithdraw reading context will get the new one)
	newCtx, newCancel := context.WithCancel(ctx)
	s.ctxMu.Lock()
	oldCancel := s.cancel
	s.ctx = newCtx
	s.cancel = newCancel
	s.ctxMu.Unlock()
	// Cancel old context AFTER releasing lock - this ensures TriggerWithdraw()
	// always reads the new valid context, never the cancelled old one.
	if oldCancel != nil {
		oldCancel()
	}

	// Ensure internal context is cancelled when Start() returns for any reason.
	// This handles edge cases where Start() might return due to an error.
	defer func() {
		s.ctxMu.Lock()
		if s.cancel != nil {
			s.cancel()
		}
		s.ctxMu.Unlock()
	}()

	// Set initial next check time
	s.mu.Lock()
	s.nextCheckTime = time.Now().Add(s.interval)
	s.mu.Unlock()

	for {
		s.mu.Lock()
		waitDuration := time.Until(s.nextCheckTime)
		if waitDuration < 0 {
			waitDuration = 0
		}
		s.mu.Unlock()

		timer := time.NewTimer(waitDuration)

		select {
		case <-ctx.Done():
			if !timer.Stop() {
				// Drain the channel if timer already fired
				select {
				case <-timer.C:
				default:
				}
			}
			// Wait for any TriggerWithdraw goroutines to complete.
			// Since s.ctx is a child of ctx, it's already cancelled,
			// so TriggerWithdraw goroutines should exit promptly.
			s.wg.Wait()
			slog.Info("withdrawal scheduler stopped")
			return ctx.Err()

		case <-timer.C:
			// Timer fired, no need to stop but good practice to ensure cleanup
		}

		s.withdrawAndCheckCredits(ctx)
	}
}

// withdrawAndCheckCredits performs withdrawal, checks tenant credits,
// and schedules the next check based on estimated depletion times.
func (s *WithdrawScheduler) withdrawAndCheckCredits(ctx context.Context) {
	// First, perform the withdrawal
	s.withdraw(ctx)

	// Then check credits and auto-close depleted leases
	nextCheck := s.checkCreditsAndClose(ctx)

	// Schedule next check
	s.mu.Lock()
	s.nextCheckTime = nextCheck
	s.mu.Unlock()

	slog.Debug("next credit check scheduled", "at", nextCheck, "in", time.Until(nextCheck))
}

// withdraw performs a withdrawal, handling pagination if there are more leases.
func (s *WithdrawScheduler) withdraw(ctx context.Context) {
	slog.Info("checking withdrawable amounts", "provider_uuid", s.providerUUID)

	// Check if there's anything to withdraw before executing transaction (with retry)
	var withdrawable sdktypes.Coins
	var err error
	backoff := initialBackoff

	for attempt := 1; attempt <= maxRetries; attempt++ {
		withdrawable, err = s.client.GetProviderWithdrawable(ctx, s.providerUUID)
		if err == nil {
			break
		}

		if attempt == maxRetries {
			slog.Error("failed to check withdrawable amounts after retries",
				"error", err,
				"attempts", attempt,
			)
			return
		}

		slog.Warn("transient error checking withdrawable amounts, retrying",
			"error", err,
			"attempt", attempt,
			"backoff", backoff,
		)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		// Exponential backoff capped at maxBackoff
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	if withdrawable.IsZero() {
		slog.Info("no funds to withdraw, skipping transaction", "provider_uuid", s.providerUUID)
		return
	}

	slog.Info("withdrawable amounts available", "amounts", withdrawable)

	txHash, err := s.client.WithdrawByProvider(ctx, s.providerUUID)
	if err != nil {
		slog.Error("withdrawal failed", "error", err)
		return
	}

	slog.Info("withdrawal complete",
		"provider_uuid", s.providerUUID,
		"tx_hash", txHash,
	)
}

// checkCreditsAndClose checks tenant credit balances and closes leases for
// tenants with depleted credit. Returns the time for the next check.
func (s *WithdrawScheduler) checkCreditsAndClose(ctx context.Context) time.Time {
	now := time.Now()
	defaultNextCheck := now.Add(s.interval)

	// Get all active leases for this provider
	leases, err := s.client.GetActiveLeasesByProvider(ctx, s.providerUUID)
	if err != nil {
		slog.Error("failed to get active leases for credit check", "error", err)
		return defaultNextCheck
	}

	if len(leases) == 0 {
		slog.Debug("no active leases to check")
		// Clear tenant state when no leases
		s.mu.Lock()
		clear(s.tenants)
		s.mu.Unlock()
		return defaultNextCheck
	}

	// Group leases by tenant
	tenantLeases := make(map[string][]string)
	for _, lease := range leases {
		tenantLeases[lease.Tenant] = append(tenantLeases[lease.Tenant], lease.Uuid)
	}

	// Process tenants under lock, collect results
	var earliestDepletion time.Time
	var leasesToClose []string

	s.mu.Lock()

	// Check each tenant's credit balance
	for tenant, leaseUUIDs := range tenantLeases {
		// Check for context cancellation between iterations
		if ctx.Err() != nil {
			break
		}

		_, balances, err := s.client.GetCreditAccount(ctx, tenant)
		if err != nil {
			// Track consecutive errors - if we consistently can't check, schedule earlier retry
			state, exists := s.tenants[tenant]
			if !exists {
				state = &tenantState{}
				s.tenants[tenant] = state
			}
			state.consecutiveErrs++

			slog.Warn("failed to get credit account",
				"tenant", tenant,
				"error", err,
				"consecutive_errors", state.consecutiveErrs,
				"lease_count", len(leaseUUIDs),
			)

			// If we've failed multiple times, schedule an earlier check
			// but don't close leases yet (could be transient network issue)
			if state.consecutiveErrs >= s.creditCheckErrorThreshold {
				earlyRetry := now.Add(s.creditCheckRetryInterval)
				if earliestDepletion.IsZero() || earlyRetry.Before(earliestDepletion) {
					earliestDepletion = earlyRetry
				}
			}
			continue
		}

		// Reset consecutive errors on success
		if state, exists := s.tenants[tenant]; exists {
			state.consecutiveErrs = 0
		}

		// Check if balance is zero (depleted)
		if balances.IsZero() {
			slog.Warn("credit exhausted, closing leases",
				"tenant", tenant,
				"lease_count", len(leaseUUIDs),
			)
			leasesToClose = append(leasesToClose, leaseUUIDs...)
			delete(s.tenants, tenant)
			continue
		}

		// Get or create tenant state for burn rate calculation
		state, exists := s.tenants[tenant]
		if !exists {
			// First time seeing this tenant, just record state
			s.tenants[tenant] = &tenantState{
				lastBalance:   balances,
				lastCheckTime: now,
			}
			continue
		}

		// Calculate burn rate from balance change
		elapsed := now.Sub(state.lastCheckTime)
		if elapsed > 0 {
			depletionTime := s.estimateDepletionTime(state.lastBalance, balances, elapsed, now)

			if !depletionTime.IsZero() && (earliestDepletion.IsZero() || depletionTime.Before(earliestDepletion)) {
				earliestDepletion = depletionTime
			}
		}

		// Update tenant state
		state.lastBalance = balances
		state.lastCheckTime = now
	}

	// Remove tenants that no longer have active leases
	for tenant := range s.tenants {
		if _, exists := tenantLeases[tenant]; !exists {
			delete(s.tenants, tenant)
		}
	}

	s.mu.Unlock()

	// Check for context cancellation before making chain call
	if ctx.Err() != nil {
		return defaultNextCheck
	}

	// Close depleted leases (outside the lock to avoid blocking)
	if len(leasesToClose) > 0 {
		closed, txHashes, err := s.client.CloseLeases(ctx, leasesToClose, "credit exhausted")
		if err != nil {
			slog.Error("failed to close depleted leases", "error", err)
		} else {
			slog.Info("closed depleted leases", "count", closed, "tx_hashes", txHashes)
		}
	}

	// Determine next check time
	if !earliestDepletion.IsZero() && earliestDepletion.Before(defaultNextCheck) {
		// Schedule check before estimated depletion to catch it in time
		nextCheck := earliestDepletion.Add(-depletionCheckBuffer)
		if nextCheck.Before(now) {
			// If depletion is imminent, check again after the buffer period
			nextCheck = now.Add(depletionCheckBuffer)
		}
		slog.Info("tenant approaching credit depletion, scheduling early check",
			"depletion_estimate", earliestDepletion,
			"next_check", nextCheck,
		)
		return nextCheck
	}

	return defaultNextCheck
}

// estimateDepletionTime calculates when a tenant's credit will be depleted
// based on observed burn rate.
//
// Note: This implementation only supports a single denomination. While it iterates
// over all denoms in the balance, it assumes each denom is being spent independently.
// For multi-denom scenarios where spending is proportional across denoms, this would
// need to be extended to calculate a weighted burn rate.
func (s *WithdrawScheduler) estimateDepletionTime(prevBalance, currBalance sdktypes.Coins, elapsed time.Duration, now time.Time) time.Time {
	// Calculate burn rate for each denom and find the earliest depletion
	var earliestDepletion time.Time

	// Ensure we have enough elapsed time to calculate a meaningful burn rate
	elapsedSeconds := int64(elapsed.Seconds())
	if elapsedSeconds <= 0 {
		return earliestDepletion // Return zero time if elapsed time is too small
	}

	for _, prevCoin := range prevBalance {
		currCoin := currBalance.AmountOf(prevCoin.Denom)
		prevAmount := prevCoin.Amount

		// Skip if no previous balance or balance increased (deposit)
		if prevAmount.IsZero() || currCoin.GTE(prevAmount) {
			continue
		}

		// Calculate burn rate: (prev - curr) / elapsed
		burned := prevAmount.Sub(currCoin)
		burnRatePerSec := burned.ToLegacyDec().QuoInt64(elapsedSeconds)

		if burnRatePerSec.IsZero() || burnRatePerSec.IsNegative() {
			continue
		}

		// Estimate time to depletion: current / burn_rate
		secondsRemaining := currCoin.ToLegacyDec().Quo(burnRatePerSec)
		if secondsRemaining.IsPositive() {
			depletion := now.Add(time.Duration(secondsRemaining.TruncateInt64()) * time.Second)

			if earliestDepletion.IsZero() || depletion.Before(earliestDepletion) {
				earliestDepletion = depletion

				slog.Debug("estimated depletion time",
					"denom", prevCoin.Denom,
					"current_balance", currCoin,
					"burn_rate_per_sec", burnRatePerSec,
					"seconds_remaining", secondsRemaining.TruncateInt64(),
					"depletion_time", depletion,
				)
			}
		}
	}

	return earliestDepletion
}
