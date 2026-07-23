// Package scheduler implements periodic withdrawal of accumulated lease fees
// and credit-depletion monitoring.
//
// # WithdrawScheduler
//
// WithdrawScheduler runs two coupled loops:
//
//  1. A timer-driven withdrawal at a fixed interval (`withdraw_interval`,
//     default 1h). Each tick pages MsgWithdraw through all of the provider's
//     active leases — threading the chain's opaque next_key cursor until it is
//     empty, bounded by `max_withdraw_iterations` — to drain accumulated fees
//     into the provider's account.
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
// # Zero-balance confirmation (ENG-591)
//
// Lease closure is destructive (MsgCloseLease → volume soft-delete + grace), so
// a single empty credit read never closes leases on its own: a transient stale
// read (e.g. fred's chain node briefly lagging a tenant top-up) would otherwise
// wrongfully soft-delete a paying tenant's data. Instead the first empty read
// stamps `firstZeroAt` and defers, scheduling an early re-check; closure fires
// only once the balance has stayed empty for the whole `credit_check_zero_grace_period`
// (default 5m — the credit-check analogue of Kubernetes' `tolerationSeconds`).
// Any non-zero read clears `firstZeroAt`, so a recovery starts a fresh window
// (hysteresis). Deferrals are counted by `fred_withdraw_credit_check_zero_deferred_total`.
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
// `maxRetries` (3) and `maxBackoff` (10s). Per-tenant credit-query errors
// are tracked as `consecutiveErrs` in the tenant's state; once that count
// reaches `credit_check_error_threshold` (default 3), the scheduler treats
// the failures as potentially transient: it does not close the tenant's
// leases on the still-failing data, but does schedule an earlier follow-up
// run after `credit_check_retry_interval` (default 30s). A successful
// query resets the counter.
package scheduler
