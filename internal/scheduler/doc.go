// Package scheduler implements periodic withdrawal of accumulated lease fees
// and credit-depletion monitoring.
//
// # WithdrawScheduler
//
// WithdrawScheduler runs two coupled loops:
//
//  1. A timer-driven withdrawal at a fixed interval (`withdraw_interval`,
//     default 1h). Each tick submits MsgWithdrawByProvider to drain accumulated
//     fees from active leases into the provider's account.
//
//  2. A credit-depletion check that runs after each withdrawal. It samples
//     tenant credit balances, estimates depletion time, and schedules an
//     earlier follow-up withdrawal if any tenant would deplete before the
//     next scheduled tick.
//
// The estimator extrapolates depletion time from the rate of credit consumption
// observed between two withdrawals; the check runs `depletionCheckBuffer`
// (10 seconds) before the estimated depletion to ensure we catch it.
//
// When credit is exhausted, MsgCloseLease is submitted for each affected lease
// and the provisioner's normal lease_closed event flow handles the cleanup.
//
// # External triggers
//
// TriggerWithdraw can be called by other components (notably the watcher,
// for cross-provider credit-depletion events) to force an immediate
// withdrawal between scheduled ticks.
//
// # Failure handling
//
// Transient chain errors are retried with exponential backoff up to
// `maxRetries` (3) and `maxBackoff` (10s). Persistent failures increment
// `credit_check_error_threshold`; once the threshold is exceeded, credit
// monitoring is disabled until the next successful withdrawal.
package scheduler
