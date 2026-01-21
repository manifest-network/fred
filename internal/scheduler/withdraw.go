package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	sdktypes "github.com/cosmos/cosmos-sdk/types"

	"github.com/manifest-network/fred/internal/chain"
)

// tenantState tracks a tenant's balance history for burn rate calculation.
type tenantState struct {
	lastBalance   sdktypes.Coins
	lastCheckTime time.Time
	leaseUUIDs    []string
}

// WithdrawScheduler periodically withdraws funds from active leases and
// auto-closes leases for tenants with depleted credit.
type WithdrawScheduler struct {
	client       *chain.Client
	providerUUID string
	interval     time.Duration

	tenants map[string]*tenantState
	mu      sync.Mutex

	// nextCheckTime is dynamically adjusted based on estimated depletion times
	nextCheckTime time.Time
}

// NewWithdrawScheduler creates a new withdrawal scheduler.
func NewWithdrawScheduler(client *chain.Client, providerUUID string, interval time.Duration) *WithdrawScheduler {
	return &WithdrawScheduler{
		client:       client,
		providerUUID: providerUUID,
		interval:     interval,
		tenants:      make(map[string]*tenantState),
	}
}

// WithdrawOnce performs a single withdrawal cycle.
// This should be called at startup before Start() to ensure the transaction
// completes before other startup transactions (like lease acknowledgment).
func (s *WithdrawScheduler) WithdrawOnce(ctx context.Context) {
	s.withdraw(ctx)
}

// Start begins the periodic withdrawal and credit checking process.
// Note: Call WithdrawOnce() before Start() if you need an initial withdrawal
// that must complete before other operations.
func (s *WithdrawScheduler) Start(ctx context.Context) error {
	slog.Info("starting withdrawal scheduler",
		"provider_uuid", s.providerUUID,
		"interval", s.interval,
	)

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
			timer.Stop()
			slog.Info("withdrawal scheduler stopped")
			return ctx.Err()

		case <-timer.C:
			s.withdrawAndCheckCredits(ctx)
		}
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
	slog.Debug("starting withdrawal cycle", "provider_uuid", s.providerUUID)

	// Check if there's anything to withdraw before executing transaction
	withdrawable, err := s.client.GetProviderWithdrawable(ctx, s.providerUUID)
	if err != nil {
		slog.Error("failed to check withdrawable amounts", "error", err)
		return
	}

	if withdrawable.IsZero() {
		slog.Debug("no funds to withdraw", "provider_uuid", s.providerUUID)
		return
	}

	slog.Info("withdrawable amounts available", "amounts", withdrawable)

	totalWithdrawn := make(map[string]int64) // denom -> amount
	iterations := 0
	maxIterations := 100 // Safety limit

	for iterations < maxIterations {
		iterations++

		amounts, hasMore, err := s.client.WithdrawByProvider(ctx, s.providerUUID)
		if err != nil {
			slog.Error("withdrawal failed", "error", err, "iteration", iterations)
			break
		}

		// Accumulate withdrawn amounts
		for _, coin := range amounts {
			totalWithdrawn[coin.Denom] += coin.Amount.Int64()
		}

		if !hasMore {
			break
		}

		slog.Debug("more leases to withdraw", "iteration", iterations)
	}

	if iterations >= maxIterations {
		slog.Warn("reached maximum withdrawal iterations", "max", maxIterations)
	}

	// Log summary
	if len(totalWithdrawn) > 0 {
		slog.Info("withdrawal cycle complete",
			"provider_uuid", s.providerUUID,
			"iterations", iterations,
			"amounts", totalWithdrawn,
		)
	}
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
		s.tenants = make(map[string]*tenantState)
		s.mu.Unlock()
		return defaultNextCheck
	}

	// Group leases by tenant
	tenantLeases := make(map[string][]string)
	for _, lease := range leases {
		tenantLeases[lease.Tenant] = append(tenantLeases[lease.Tenant], lease.Uuid)
	}

	var earliestDepletion time.Time
	var leasesToClose []string

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check each tenant's credit balance
	for tenant, leaseUUIDs := range tenantLeases {
		_, balances, err := s.client.GetCreditAccount(ctx, tenant)
		if err != nil {
			slog.Error("failed to get credit account", "tenant", tenant, "error", err)
			continue
		}

		// Check if balance is zero (depleted)
		if balances.IsZero() {
			slog.Warn("tenant credit depleted, closing leases",
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
				leaseUUIDs:    leaseUUIDs,
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
		state.leaseUUIDs = leaseUUIDs
	}

	// Remove tenants that no longer have active leases
	for tenant := range s.tenants {
		if _, exists := tenantLeases[tenant]; !exists {
			delete(s.tenants, tenant)
		}
	}

	// Close depleted leases (outside the lock would be better, but we need to release lock first)
	if len(leasesToClose) > 0 {
		// Release lock before making chain calls
		s.mu.Unlock()
		closed, err := s.client.CloseLeases(ctx, leasesToClose)
		s.mu.Lock()

		if err != nil {
			slog.Error("failed to close depleted leases", "error", err)
		} else {
			slog.Info("closed depleted leases", "count", closed)
		}
	}

	// Determine next check time
	if !earliestDepletion.IsZero() && earliestDepletion.Before(defaultNextCheck) {
		// Add a small buffer (10 seconds) to avoid checking too early
		nextCheck := earliestDepletion.Add(-10 * time.Second)
		if nextCheck.Before(now) {
			nextCheck = now.Add(10 * time.Second)
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
func (s *WithdrawScheduler) estimateDepletionTime(prevBalance, currBalance sdktypes.Coins, elapsed time.Duration, now time.Time) time.Time {
	// Calculate burn rate for each denom and find the earliest depletion
	var earliestDepletion time.Time

	for _, prevCoin := range prevBalance {
		currCoin := currBalance.AmountOf(prevCoin.Denom)
		prevAmount := prevCoin.Amount

		// Skip if no previous balance or balance increased (deposit)
		if prevAmount.IsZero() || currCoin.GTE(prevAmount) {
			continue
		}

		// Calculate burn rate: (prev - curr) / elapsed
		burned := prevAmount.Sub(currCoin)
		burnRatePerSec := burned.ToLegacyDec().QuoInt64(int64(elapsed.Seconds()))

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
