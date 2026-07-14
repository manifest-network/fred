package scheduler

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"sync"
	"time"

	sdktypes "github.com/cosmos/cosmos-sdk/types"
	billingtypes "github.com/manifest-network/manifest-ledger/x/billing/types"

	"github.com/manifest-network/fred/internal/metrics"
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
	WithdrawByProvider(ctx context.Context, providerUUID string, key []byte) (string, *billingtypes.MsgWithdrawResponse, error)
	GetActiveLeasesByProvider(ctx context.Context, providerUUID string) ([]billingtypes.Lease, error)
	GetCreditAccount(ctx context.Context, tenant string) (*billingtypes.CreditAccount, sdktypes.Coins, error)
	CloseLeases(ctx context.Context, leaseUUIDs []string, reason string) (uint64, []string, error)
}

// tenantState tracks a tenant's balance history for burn rate calculation.
//
// Note: Multi-denom credit is supported by computing a per-denom burn rate
// and taking the earliest depletion time across denoms. Proportional burn
// rates (e.g. fixed ratios across denominations under load) are not modeled —
// each denom is treated as draining independently.
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

	// Cadence guard: paid withdraw is rate-limited to withdrawInterval; credit
	// checks run every creditCheckInterval (ENG-524).
	creditCheckInterval time.Duration // credit-check / wake cadence
	withdrawInterval    time.Duration // paid-withdraw cadence
	guardActive         bool          // set once at construction; safe to read without mu
	lastWithdrawTime    time.Time     // guarded by mu

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
	WithdrawInterval          time.Duration // paid-withdraw cadence
	CreditCheckInterval       time.Duration // credit-check / wake cadence; 0 => = WithdrawInterval
	MaxWithdrawIterations     int
	CreditCheckErrorThreshold int
	CreditCheckRetryInterval  time.Duration
}

// NewWithdrawScheduler creates a new withdrawal scheduler.
func NewWithdrawScheduler(client ChainClient, cfg WithdrawSchedulerConfig) *WithdrawScheduler {
	// Apply defaults using cmp.Or (returns first non-zero value)
	// For int/duration fields, use max() to convert negative values to 0
	maxIterations := cmp.Or(max(cfg.MaxWithdrawIterations, 0), 100)
	errorThreshold := cmp.Or(max(cfg.CreditCheckErrorThreshold, 0), 3)
	retryInterval := cmp.Or(max(cfg.CreditCheckRetryInterval, 0), 30*time.Second)

	withdrawInterval := cfg.WithdrawInterval
	creditCheckInterval := cmp.Or(max(cfg.CreditCheckInterval, 0), cfg.WithdrawInterval)

	// Create initial context that can be canceled on shutdown.
	// This ensures TriggerWithdraw works correctly even before Start() is called.
	// Start() will replace this with a context derived from the passed-in context.
	ctx, cancel := context.WithCancel(context.Background())

	return &WithdrawScheduler{
		client:                    client,
		providerUUID:              cfg.ProviderUUID,
		withdrawInterval:          withdrawInterval,
		creditCheckInterval:       creditCheckInterval,
		guardActive:               creditCheckInterval < withdrawInterval,
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
func (s *WithdrawScheduler) WithdrawOnce(ctx context.Context) error {
	if err := s.withdraw(ctx); err != nil {
		return err
	}
	s.checkCreditsAndClose(ctx)
	return nil
}

// TriggerWithdraw triggers an immediate withdrawal cycle.
// This can be called from event handlers when cross-provider credit depletion is detected.
// Uses the scheduler's context so it can be canceled on shutdown.
func (s *WithdrawScheduler) TriggerWithdraw() {
	slog.Info("withdrawal triggered by cross-provider credit depletion")

	// Get context under read lock - always valid since initialized in NewWithdrawScheduler
	s.ctxMu.RLock()
	ctx := s.ctx
	s.ctxMu.RUnlock()

	// Check if context is already canceled (scheduler stopped)
	if ctx.Err() != nil {
		slog.Debug("ignoring TriggerWithdraw, scheduler context canceled")
		return
	}

	// Track the goroutine for graceful shutdown (using WaitGroup.Go for Go 1.25+).
	// Panic recovery: withdrawAndCheckCredits invokes chain RPCs (cosmos
	// SDK marshaling). A panic there would otherwise crash fred. Log +
	// metric and exit; the next TriggerWithdraw or interval tick retries.
	s.wg.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("withdrawal scheduler panic — recovering to keep fred alive",
					"panic", r,
					"stack", string(debug.Stack()),
				)
				metrics.GoroutinePanicsTotal.WithLabelValues("withdraw_scheduler").Inc()
			}
		}()
		s.withdrawAndCheckCredits(ctx, true)
	})
}

// Stop cancels the scheduler's internal context, stopping all operations.
// This is useful for testing or for cases where you want to stop the scheduler
// without canceling the parent context passed to Start().
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
// When ctx is canceled, the internal context is also canceled, which stops any
// TriggerWithdraw goroutines that were spawned during operation.
func (s *WithdrawScheduler) Start(ctx context.Context) error {
	slog.Info("starting withdrawal scheduler",
		"provider_uuid", s.providerUUID,
		"withdraw_interval", s.withdrawInterval,
		"credit_check_interval", s.creditCheckInterval,
		"guard_active", s.guardActive,
	)

	if s.guardActive {
		metrics.WithdrawGuardActive.Set(1)
	} else {
		metrics.WithdrawGuardActive.Set(0)
	}

	// Replace context for use by TriggerWithdraw (protected by mutex).
	// IMPORTANT: Create the new context BEFORE canceling the old one to avoid a race
	// where TriggerWithdraw() reads a canceled context. The sequence is:
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
	// always reads the new valid context, never the canceled old one.
	if oldCancel != nil {
		oldCancel()
	}

	// Ensure internal context is canceled when Start() returns for any reason.
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
	s.nextCheckTime = time.Now().Add(s.creditCheckInterval)
	s.mu.Unlock()

	for {
		s.mu.Lock()
		waitDuration := time.Until(s.nextCheckTime)
		if waitDuration < 0 {
			waitDuration = 0
		}
		s.mu.Unlock()

		timer := time.NewTimer(waitDuration)

		// Get internal context for select - this allows Stop() to wake us up
		s.ctxMu.RLock()
		internalCtx := s.ctx
		s.ctxMu.RUnlock()

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
			// Since s.ctx is a child of ctx, it's already canceled,
			// so TriggerWithdraw goroutines should exit promptly.
			s.wg.Wait()
			slog.Info("withdrawal scheduler stopped")
			return ctx.Err()

		case <-internalCtx.Done():
			// Stop() was called - exit the loop
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			s.wg.Wait()
			slog.Info("withdrawal scheduler stopped via Stop()")
			return nil

		case <-timer.C:
			// Timer consumed - proceed to work
		}

		// Check if we should stop before doing work
		if internalCtx.Err() != nil {
			s.wg.Wait()
			return nil
		}

		s.withdrawAndCheckCredits(ctx, false)
	}
}

// withdrawAndCheckCredits checks tenant credits (always) and performs the
// paid withdrawal (gated by the cadence guard, unless force is set), then
// schedules the next check based on estimated depletion times.
//
// force bypasses the guard for on-demand triggers (TriggerWithdraw), where
// an immediate withdraw is the whole point of the call.
func (s *WithdrawScheduler) withdrawAndCheckCredits(ctx context.Context, force bool) {
	s.mu.Lock()
	last := s.lastWithdrawTime
	s.mu.Unlock()

	if force || !s.guardActive || time.Since(last) >= s.withdrawInterval {
		if err := s.withdraw(ctx); err != nil {
			slog.Error("withdrawal failed", "error", err)
		}
	} else {
		metrics.WithdrawSkippedByGuardTotal.Inc()
		slog.Debug("paid withdrawal skipped by cadence guard",
			"since_last", time.Since(last),
			"withdraw_interval", s.withdrawInterval,
		)
	}

	// Then check credits and auto-close depleted leases
	nextCheck := s.checkCreditsAndClose(ctx)

	// Schedule next check
	s.mu.Lock()
	s.nextCheckTime = nextCheck
	s.mu.Unlock()

	slog.Debug("next credit check scheduled", "at", nextCheck, "in", time.Until(nextCheck))
}

// setLastWithdraw records "provider is drained as of now" to anchor the paid
// withdraw guard. Called at each nothing-deferred success return of withdraw().
func (s *WithdrawScheduler) setLastWithdraw() {
	s.mu.Lock()
	s.lastWithdrawTime = time.Now()
	s.mu.Unlock()
}

// withdraw drains a provider's withdrawable lease fees for one cycle.
//
// Provider-wide withdrawal is paginated by an opaque cursor: each MsgWithdraw
// settles up to the chain's page limit and returns a next_key that resumes
// strictly past the settled leases (keyset pagination). We thread that cursor,
// looping until it is empty (the provider is fully drained) or we hit
// maxWithdrawIterations. Hitting the bound is not fund loss — lease close
// settles everything — but it defers active-lease collection to the next cycle,
// so it is surfaced via WithdrawIncompleteCyclesTotal for alerting.
func (s *WithdrawScheduler) withdraw(ctx context.Context) error {
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
			return fmt.Errorf("check withdrawable amounts after %d retries: %w", attempt, err)
		}

		slog.Warn("transient error checking withdrawable amounts, retrying",
			"error", err,
			"attempt", attempt,
			"backoff", backoff,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
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
		// Empty withdrawable == drained: anchor the interval so we don't re-probe every credit-check wake (bounded ≤ withdrawInterval delay for later accrual is the intended rate-limit; funds are safe on-chain).
		s.setLastWithdraw()
		return nil
	}

	slog.Info("withdrawable amounts available", "amounts", withdrawable)

	// Page through the provider's active leases, threading the opaque cursor.
	// A page error is fatal for this cycle: WithdrawByProvider already retries
	// transient tx failures internally, and re-driving a possibly-committed
	// withdraw could double-process — the next tick is the cycle-level retry.
	var key []byte
	for page := 1; page <= s.maxWithdrawIterations; page++ {
		txHash, resp, err := s.client.WithdrawByProvider(ctx, s.providerUUID, key)
		if err != nil {
			return fmt.Errorf("withdrawal failed (page %d): %w", page, err)
		}
		if resp == nil {
			return fmt.Errorf("withdrawal returned nil response (page %d)", page)
		}

		slog.Info("withdrawal page complete",
			"provider_uuid", s.providerUUID,
			"tx_hash", txHash,
			"page", page,
			"leases_settled", resp.WithdrawalCount,
		)

		if len(resp.NextKey) == 0 {
			slog.Info("withdrawal cycle complete",
				"provider_uuid", s.providerUUID,
				"pages", page,
			)
			s.setLastWithdraw()
			return nil
		}
		key = resp.NextKey

		// Abort before starting the next page if the context was canceled
		// (e.g. shutdown). The check is deliberately after the first page so a
		// cycle always issues at least one withdrawal attempt; the next tick
		// resumes from the beginning.
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	// Cursor still advancing after maxWithdrawIterations pages: the provider has
	// more active leases than one cycle drains at the current bound. Not fund
	// loss, but active-lease revenue is deferred to the next cycle — surface it
	// so operators can raise max_withdraw_iterations.
	metrics.WithdrawIncompleteCyclesTotal.Inc()
	slog.Warn("withdrawal cycle hit iteration bound; provider not fully drained this cycle",
		"provider_uuid", s.providerUUID,
		"max_iterations", s.maxWithdrawIterations,
	)
	return nil
}

// checkCreditsAndClose checks tenant credit balances and closes leases for
// tenants with depleted credit. Returns the time for the next check.
func (s *WithdrawScheduler) checkCreditsAndClose(ctx context.Context) time.Time {
	now := time.Now()
	defaultNextCheck := now.Add(s.creditCheckInterval)

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

	// Fetch credit balances WITHOUT holding the lock (network I/O)
	results := make(map[string]creditResult, len(tenantLeases))
	for tenant := range tenantLeases {
		if ctx.Err() != nil {
			break
		}
		_, balances, err := s.client.GetCreditAccount(ctx, tenant)
		results[tenant] = creditResult{balances, err}
	}

	// Apply results under the scheduler lock. Extracted so the lock is released
	// via defer even if the body panics — a panic here previously leaked s.mu
	// and wedged the scheduler (ENG-500).
	earliestDepletion, leasesToClose := s.applyCreditResults(tenantLeases, results, now)

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

// creditResult holds the fetched credit balance (or error) for one tenant.
type creditResult struct {
	balances sdktypes.Coins
	err      error
}

// applyCreditResults folds the fetched per-tenant credit results into burn-rate
// state, returning the earliest estimated depletion and any leases whose credit
// is exhausted. The scheduler lock is taken here and released via defer, so a
// panic in the body cannot leak it and wedge the scheduler (ENG-500).
func (s *WithdrawScheduler) applyCreditResults(tenantLeases map[string][]string, results map[string]creditResult, now time.Time) (earliestDepletion time.Time, leasesToClose []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for tenant, leaseUUIDs := range tenantLeases {
		result, checked := results[tenant]
		if !checked {
			continue // skipped due to context cancellation
		}

		if result.err != nil {
			// Track consecutive errors - if we consistently can't check, schedule earlier retry
			state, exists := s.tenants[tenant]
			if !exists {
				state = &tenantState{}
				s.tenants[tenant] = state
			}
			state.consecutiveErrs++

			slog.Warn("failed to get credit account",
				"tenant", tenant,
				"error", result.err,
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
		if result.balances.IsZero() {
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
				lastBalance:   result.balances,
				lastCheckTime: now,
			}
			continue
		}

		// Calculate burn rate from balance change
		elapsed := now.Sub(state.lastCheckTime)
		if elapsed > 0 {
			depletionTime := s.estimateDepletionTime(state.lastBalance, result.balances, elapsed, now)

			if !depletionTime.IsZero() && (earliestDepletion.IsZero() || depletionTime.Before(earliestDepletion)) {
				earliestDepletion = depletionTime
			}
		}

		// Update tenant state
		state.lastBalance = result.balances
		state.lastCheckTime = now
	}

	// Remove tenants that no longer have active leases
	for tenant := range s.tenants {
		if _, exists := tenantLeases[tenant]; !exists {
			delete(s.tenants, tenant)
		}
	}
	return
}

// maxDepletionSeconds bounds the depletion horizon so that neither
// LegacyDec.TruncateInt64() (which panics on int64 overflow) nor the
// time.Duration(n)*time.Second multiply (which overflows int64 nanoseconds and
// wraps to a bogus time) can be reached from a large balance / tiny burn
// (ENG-500). ~292 years; beyond this the exact depletion time is irrelevant.
const maxDepletionSeconds = int64(math.MaxInt64) / int64(time.Second)

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

		// Estimate time to depletion: current / burn_rate. Clamp to a finite
		// horizon: an enormous balance with a tiny burn makes secondsRemaining
		// exceed int64, which would panic in TruncateInt64 and overflow the
		// time.Duration multiply into a bogus (past) time (ENG-500).
		secondsRemaining := currCoin.ToLegacyDec().Quo(burnRatePerSec)
		if secondsRemaining.IsPositive() {
			secs := maxDepletionSeconds
			if t := secondsRemaining.TruncateInt(); t.IsInt64() && t.Int64() < maxDepletionSeconds {
				secs = t.Int64()
			}
			depletion := now.Add(time.Duration(secs) * time.Second)

			if earliestDepletion.IsZero() || depletion.Before(earliestDepletion) {
				earliestDepletion = depletion

				slog.Debug("estimated depletion time",
					"denom", prevCoin.Denom,
					"current_balance", currCoin,
					"burn_rate_per_sec", burnRatePerSec,
					"seconds_remaining", secs,
					"depletion_time", depletion,
				)
			}
		}
	}

	return earliestDepletion
}
