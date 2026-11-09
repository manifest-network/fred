// -*- mode: Bluespec; -*-
//
// manifest-ledger x/billing: who gets paid when a tenant's credit runs out.
//
// One tenant credit balance, two leases at two different providers. Each
// provider settles on its own cadence (modeled as "any party may settle any
// lease at any time"). Time is in whole seconds; per-second rates are integers.
//
// Code refs (manifest-network/manifest-ledger, deps checkout):
//   x/billing/keeper/msg_server.go  create: AvailableCredit = balance - ReservedAmounts,
//                                    reservation = rate * MinLeaseDuration (constant
//                                    until the lease leaves PENDING/ACTIVE)
//   x/billing/keeper/settlement.go  performSettlementCore: transfer min(accrued, balance)
//                                    from the shared credit address, first come first served
//   x/billing/keeper/keeper.go      ShouldAutoCloseLease: accrued >= balance (or a zero
//                                    balance when nothing accrued) closes on touch
//   x/billing/README.md             "credits are reserved to guarantee that sufficient
//                                    funds exist for at least min_lease_duration seconds"
//
// Simplification: accrual starts at creation (the chain starts it at
// acknowledgement; shifting both leases by their ack delay changes nothing here).
//
// EARMARK = false: the code as written.
// EARMARK = true:  proposed design. A lease's reservation is the unpaid part of
//   its guaranteed window, rate * (createdAt + MIN - lastSettled), so it shrinks
//   as that window is paid; a settlement may never take another lease's
//   reservation. Creation keeps the same form: balance - reserved >= rate * MIN.
module fairness {
  const EARMARK: bool

  pure val LEASES = Set("L1", "L2")
  pure val RATE = Map("L1" -> 1, "L2" -> 1)
  pure val MIN = 3          // min_lease_duration, seconds
  pure val HORIZON = 8      // seconds of model time
  pure val INITIAL = 3      // tenant's first deposit: exactly one lease's reservation

  var now: int
  var bal: int
  var funded: int                 // total ever deposited
  var fundsLeft: int              // remaining top-ups the tenant may make
  var created: str -> bool
  var active: str -> bool
  var createdAt: str -> int
  var last: str -> int            // LastSettledAt
  var paid: str -> int            // total paid to the lease's provider
  var closedAt: str -> int
  var lastAct: { act: str, l: str, amt: int }

  pure def max(a: int, b: int): int = if (a > b) a else b
  pure def min(a: int, b: int): int = if (a < b) a else b

  // Unpaid part of lease j's guaranteed window (the proposed reservation).
  def earmark(j: str): int =
    if (active.get(j)) RATE.get(j) * max(0, createdAt.get(j) + MIN - last.get(j)) else 0

  // ReservedAmounts as the code keeps it: constant per open lease.
  def reservedNow: int =
    if (EARMARK) LEASES.fold(0, (s, j) => s + earmark(j))
    else LEASES.fold(0, (s, j) => s + (if (active.get(j)) RATE.get(j) * MIN else 0))

  def available: int = bal - reservedNow

  // What a settlement of lease i may draw.
  def cap(i: str): int =
    if (EARMARK) bal - LEASES.exclude(Set(i)).fold(0, (s, j) => s + earmark(j))
    else bal

  def served(i: str): int =
    if (not(created.get(i))) 0
    else RATE.get(i) * ((if (active.get(i)) now else closedAt.get(i)) - createdAt.get(i))

  action init = all {
    now' = 0,
    bal' = INITIAL,
    funded' = INITIAL,
    fundsLeft' = 1,
    created' = LEASES.mapBy(_ => false),
    active' = LEASES.mapBy(_ => false),
    createdAt' = LEASES.mapBy(_ => 0),
    last' = LEASES.mapBy(_ => 0),
    paid' = LEASES.mapBy(_ => 0),
    closedAt' = LEASES.mapBy(_ => 0),
    lastAct' = { act: "init", l: "", amt: 0 },
  }

  action keepLeases = all {
    created' = created, active' = active, createdAt' = createdAt,
    last' = last, paid' = paid, closedAt' = closedAt,
  }

  action tick = all {
    now < HORIZON,
    now' = now + 1,
    bal' = bal, funded' = funded, fundsLeft' = fundsLeft,
    keepLeases,
    lastAct' = { act: "tick", l: "", amt: now + 1 },
  }

  action fund(k: int): bool = all {
    fundsLeft > 0,
    now' = now,
    bal' = bal + k,
    funded' = funded + k,
    fundsLeft' = fundsLeft - 1,
    keepLeases,
    lastAct' = { act: "fund", l: "", amt: k },
  }

  // MsgCreateLease (+ acknowledgement): AvailableCredit >= rate * MIN.
  action create(i: str): bool = all {
    not(created.get(i)),
    available >= RATE.get(i) * MIN,
    now' = now, bal' = bal, funded' = funded, fundsLeft' = fundsLeft,
    created' = created.set(i, true),
    active' = active.set(i, true),
    createdAt' = createdAt.set(i, now),
    last' = last.set(i, now),
    paid' = paid,
    closedAt' = closedAt,
    lastAct' = { act: "create", l: i, amt: now },
  }

  // MsgWithdraw by the lease's provider, or MsgCloseLease (voluntary = true)
  // by tenant or provider. Lazy settlement plus the auto-close check.
  action settle(i: str, voluntary: bool): bool = {
    val accrued = RATE.get(i) * (now - last.get(i))
    val c = cap(i)
    val transfer = min(accrued, c)
    val exhausted = if (accrued > 0) accrued >= c else c <= 0
    val closing = voluntary or exhausted
    all {
      active.get(i),
      now' = now,
      bal' = bal - transfer,
      funded' = funded, fundsLeft' = fundsLeft,
      created' = created,
      active' = if (closing) active.set(i, false) else active,
      createdAt' = createdAt,
      last' = last.set(i, now),
      paid' = paid.set(i, paid.get(i) + transfer),
      closedAt' = if (closing) closedAt.set(i, now) else closedAt,
      lastAct' = { act: if (closing) "settle+close" else "settle", l: i, amt: transfer },
    }
  }

  action step = {
    nondet i = oneOf(LEASES)
    nondet k = oneOf(1.to(3))
    nondet v = oneOf(Set(true, false))
    any { tick, fund(k), create(i), settle(i, v) }
  }

  // ---- properties ----

  // Money is neither created nor destroyed.
  val conservation = bal + LEASES.fold(0, (s, j) => s + paid.get(j)) == funded

  // The README's promise: a lease's provider is paid for its first
  // min_lease_duration seconds of service (or its whole life, if shorter).
  val guaranteeHonored = LEASES.forall(i =>
    (created.get(i) and not(active.get(i))) implies
      paid.get(i) >= RATE.get(i) * min(MIN, closedAt.get(i) - createdAt.get(i)))

  // Design invariant of the EARMARK variant: the balance always covers every
  // open lease's unpaid guaranteed window.
  val earmarksCovered = bal >= LEASES.fold(0, (s, j) => s + earmark(j))

  // Worst case probe: a provider served at least MIN seconds and was paid nothing.
  val noFreeRide = LEASES.forall(i =>
    (created.get(i) and not(active.get(i)) and served(i) >= RATE.get(i) * MIN) implies paid.get(i) > 0)
}

module current {
  import fairness(EARMARK = false).*
}

module fixed {
  import fairness(EARMARK = true).*
}
