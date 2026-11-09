// -*- mode: Bluespec; -*-
//
// manifest-ledger x/billing: one tenant credit account, two leases with one
// provider, lazy settlement, auto-close on exhaustion, reservations and counters.
//
// Code refs (manifest-network/manifest-ledger @ 43e2e1a):
//   x/billing/keeper/msg_server.go  createLeaseInternal (reserve), AcknowledgeLease
//                                    (LastSettledAt = blockTime), RejectLease, CancelLease,
//                                    CloseLease, withdrawFromLeases, settleLease
//                                    (LastSettledAt = settleTime)
//   x/billing/keeper/keeper.go      ShouldAutoCloseLease (accrued >= balance),
//                                    AutoCloseLease, ExpirePendingLease, ReleaseLeaseReservation
//   x/billing/keeper/accrual.go     CalculateAccruedAmount: whole seconds, int64(duration / time.Second)
//   x/billing/keeper/settlement.go  performSettlementCore: transfer min(accrued, balance)
//
// Time is in milliseconds: CometBFT block times carry sub-second precision.
// Per-second rates are integers (x/sku enforces divisibility).

module billing {
  // false = code as written (LastSettledAt := settle time);
  // true  = proposed fix (advance LastSettledAt by the whole seconds charged)
  const CARRY_FRACTION: bool

  pure val LEASES = Set(1, 2)
  pure val RATE = 2               // per second, per lease
  pure val MIN_LEASE_SECONDS = 3  // min_lease_duration (reservation horizon)
  pure val PENDING_TIMEOUT_MS = 6000
  pure val MAX_TIME_MS = 12000
  pure val MAX_FUNDED = 60

  type State = None | Pending | Active | Closed | Rejected | Expired

  var now: int                     // block time, ms
  var funded: int                  // total ever deposited into the credit account
  var balance: int                 // credit address bank balance
  var providerBal: int             // provider payout balance
  var reserved: int                // CreditAccount.ReservedAmounts
  var pendingCount: int
  var activeCount: int
  var state: int -> State
  var createdAt: int -> int
  var ackAt: int -> int
  var lastSettled: int -> int
  var paid: int -> int             // total settled to the provider for this lease
  var exhausted: int -> bool       // closed by credit exhaustion
  var lastAction: str

  pure def reservation: int = RATE * MIN_LEASE_SECONDS
  pure def wholeSeconds(ms: int): int = ms / 1000

  action init = all {
    now' = 0, funded' = 0, balance' = 0, providerBal' = 0, reserved' = 0,
    pendingCount' = 0, activeCount' = 0,
    state' = LEASES.mapBy(_ => None), createdAt' = LEASES.mapBy(_ => 0),
    ackAt' = LEASES.mapBy(_ => 0), lastSettled' = LEASES.mapBy(_ => 0),
    paid' = LEASES.mapBy(_ => 0), exhausted' = LEASES.mapBy(_ => false),
    lastAction' = "init",
  }

  action keepLeases = all {
    state' = state, createdAt' = createdAt, ackAt' = ackAt, lastSettled' = lastSettled,
    paid' = paid, exhausted' = exhausted,
  }

  // New block: time advances by a non-integral number of seconds.
  action newBlock = all {
    now < MAX_TIME_MS,
    nondet dt = Set(1300, 1700, 2500, 5300).oneOf()
    all {
      now' = now + dt,
      funded' = funded, balance' = balance, providerBal' = providerBal, reserved' = reserved,
      pendingCount' = pendingCount, activeCount' = activeCount, keepLeases,
      lastAction' = "new block",
    }
  }

  action fund = all {
    funded < MAX_FUNDED,
    nondet amt = Set(5, 10, 20).oneOf()
    all {
      funded' = funded + amt, balance' = balance + amt,
      now' = now, providerBal' = providerBal, reserved' = reserved,
      pendingCount' = pendingCount, activeCount' = activeCount, keepLeases,
      lastAction' = "tenant funds credit",
    }
  }

  action create = all {
    nondet i = LEASES.oneOf()
    all {
      state.get(i) == None, balance - reserved >= reservation,
      state' = state.set(i, Pending), createdAt' = createdAt.set(i, now),
      lastSettled' = lastSettled.set(i, now),
      reserved' = reserved + reservation, pendingCount' = pendingCount + 1,
      ackAt' = ackAt, paid' = paid, exhausted' = exhausted,
      now' = now, funded' = funded, balance' = balance, providerBal' = providerBal,
      activeCount' = activeCount,
      lastAction' = "tenant creates lease (reserve)",
    }
  }

  action acknowledge = all {
    nondet i = LEASES.oneOf()
    all {
      state.get(i) == Pending,
      state' = state.set(i, Active), ackAt' = ackAt.set(i, now), lastSettled' = lastSettled.set(i, now),
      pendingCount' = pendingCount - 1, activeCount' = activeCount + 1,
      createdAt' = createdAt, paid' = paid, exhausted' = exhausted,
      now' = now, funded' = funded, balance' = balance, providerBal' = providerBal, reserved' = reserved,
      lastAction' = "provider acknowledges",
    }
  }

  // RejectLease / CancelLease / ExpirePendingLease all release the same way.
  action endPending = all {
    nondet i = LEASES.oneOf()
    nondet how = Set(Rejected, Expired).oneOf()
    all {
      state.get(i) == Pending,
      how == Rejected or now - createdAt.get(i) >= PENDING_TIMEOUT_MS,
      state' = state.set(i, how),
      reserved' = reserved - reservation, pendingCount' = pendingCount - 1,
      createdAt' = createdAt, ackAt' = ackAt, lastSettled' = lastSettled, paid' = paid, exhausted' = exhausted,
      now' = now, funded' = funded, balance' = balance, providerBal' = providerBal, activeCount' = activeCount,
      lastAction' = if (how == Rejected) "provider rejects / tenant cancels" else "EndBlock expires pending lease",
    }
  }

  // Withdraw (keep open) or CloseLease (close). Both run ShouldAutoCloseLease first.
  action settle = all {
    nondet i = LEASES.oneOf()
    nondet closing = Set(true, false).oneOf()
    val elapsedMs = now - lastSettled.get(i)
    val accrued = RATE * wholeSeconds(elapsedMs)
    val autoClose = if (elapsedMs > 0) accrued >= balance else balance == 0
    val transfer = if (accrued <= balance) accrued else balance
    val closes = closing or autoClose
    val newLast = if (closes or not(CARRY_FRACTION)) now
                  else lastSettled.get(i) + 1000 * wholeSeconds(elapsedMs)
    all {
      state.get(i) == Active, elapsedMs >= 0,
      balance' = balance - transfer, providerBal' = providerBal + transfer,
      paid' = paid.set(i, paid.get(i) + transfer),
      lastSettled' = lastSettled.set(i, newLast),
      state' = if (closes) state.set(i, Closed) else state,
      exhausted' = if (autoClose) exhausted.set(i, true) else exhausted,
      activeCount' = if (closes) activeCount - 1 else activeCount,
      reserved' = if (closes) reserved - reservation else reserved,
      createdAt' = createdAt, ackAt' = ackAt,
      now' = now, funded' = funded, pendingCount' = pendingCount,
      lastAction' = if (autoClose) "touch: auto-close on exhaustion"
                    else if (closing) "CloseLease: settle and close"
                    else "provider Withdraw: settle",
    }
  }

  action step = any { newBlock, fund, create, acknowledge, endPending, settle }

  // ------------------------------------------------------------- properties

  // Tokens are only moved, never created or destroyed.
  val conservation: bool = balance + providerBal == funded and balance >= 0

  // Reservations equal the sum over PENDING and ACTIVE leases.
  val reservationsConsistent: bool =
    reserved == LEASES.filter(i => Set(Pending, Active).contains(state.get(i))).size() * reservation

  val countersConsistent: bool =
    pendingCount == LEASES.filter(i => state.get(i) == Pending).size() and
    activeCount == LEASES.filter(i => state.get(i) == Active).size()

  // A provider is paid exactly the whole seconds its lease ran (up to the last
  // settlement), unless the tenant ran out of credit.
  val providerPaidForEverySecond: bool = LEASES.forall(i =>
    (Set(Active, Closed).contains(state.get(i)) and not(exhausted.get(i)))
      implies paid.get(i) == RATE * wholeSeconds(lastSettled.get(i) - ackAt.get(i)))
}

module current {
  import billing(CARRY_FRACTION = false).*
}

module fixed {
  import billing(CARRY_FRACTION = true).*
}
