package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/manifest-network/fred/internal/chain"
)

// WithdrawScheduler periodically withdraws funds from active leases.
type WithdrawScheduler struct {
	client       *chain.Client
	providerUUID string
	interval     time.Duration
}

// NewWithdrawScheduler creates a new withdrawal scheduler.
func NewWithdrawScheduler(client *chain.Client, providerUUID string, interval time.Duration) *WithdrawScheduler {
	return &WithdrawScheduler{
		client:       client,
		providerUUID: providerUUID,
		interval:     interval,
	}
}

// Start begins the periodic withdrawal process.
func (s *WithdrawScheduler) Start(ctx context.Context) error {
	slog.Info("starting withdrawal scheduler",
		"provider_uuid", s.providerUUID,
		"interval", s.interval,
	)

	// Perform initial withdrawal
	s.withdraw(ctx)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("withdrawal scheduler stopped")
			return ctx.Err()
		case <-ticker.C:
			s.withdraw(ctx)
		}
	}
}

// withdraw performs a withdrawal, handling pagination if there are more leases.
func (s *WithdrawScheduler) withdraw(ctx context.Context) {
	slog.Debug("starting withdrawal cycle", "provider_uuid", s.providerUUID)

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
	} else {
		slog.Debug("no funds to withdraw", "provider_uuid", s.providerUUID)
	}
}
