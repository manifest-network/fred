// ENG-2127 follow-up: finite flush budget, exact observation, strict progress.
// Each member represents a distinct lease; duplicate caller fan-out is covered
// by Go tests. Queries are abstracted into one observation step. Time can expire
// between observation and broadcast; RPCs can fail independently of state.
// This checks safety and bounded work, not guaranteed acknowledgment by a deadline.
module ackRetryBudget {
  const CHECK_DEADLINE: bool
  pure val LEASES = Set("a", "b", "c", "d")
  type State = Pending | Active | Gone | Unbound
  type Phase = Send | Observe | Individual | Done
  type Result = Waiting | Acked | Failed
  var chain: str -> State
  var result: str -> Result
  var remaining: Set[str]
  var batch: Set[str]
  var phase: Phase
  var budget: int
  var retries: int
  var wroteAfterDeadline: bool
  var retriedWithoutEvidence: bool

  action init = all {
    chain' = LEASES.mapBy(_ => Pending),
    result' = LEASES.mapBy(_ => Waiting),
    remaining' = LEASES, batch' = LEASES, phase' = Send,
    budget' = 6, retries' = 0,
    wroteAfterDeadline' = false, retriedWithoutEvidence' = false,
  }

  action change = all {
    phase != Done,
    nondet l = LEASES.oneOf()
    nondet nextState = Set(Active, Gone, Unbound).oneOf()
    all {
      chain.get(l) == Pending,
      chain' = chain.set(l, nextState),
      result' = result, remaining' = remaining, batch' = batch, phase' = phase,
      budget' = budget, retries' = retries,
      wroteAfterDeadline' = wroteAfterDeadline, retriedWithoutEvidence' = retriedWithoutEvidence,
    },
  }

  action timePasses = all {
    phase != Done, budget > 0,
    budget' = budget - 1,
    chain' = chain, result' = result, remaining' = remaining, batch' = batch, phase' = phase,
    retries' = retries, wroteAfterDeadline' = wroteAfterDeadline,
    retriedWithoutEvidence' = retriedWithoutEvidence,
  }

  action observe = all {
    phase == Observe,
    val pending = remaining.filter(l => chain.get(l) == Pending)
    // A stale reader can miss the change that caused ErrLeaseNotPending.
    nondet stale = Set(false, true).oneOf()
    val observed = if (budget == 0) Set() else if (stale) remaining else pending
    all {
      batch' = observed, remaining' = observed,
      result' = LEASES.mapBy(l =>
        if (remaining.contains(l) and not(observed.contains(l)))
          (if (budget > 0 and chain.get(l) == Active) Acked else Failed)
        else result.get(l)),
      phase' = if (observed == Set()) Done
        else if (observed == remaining) Individual else Send,
      retries' = if (observed != Set() and observed != remaining) retries + 1 else retries,
      retriedWithoutEvidence' = (retriedWithoutEvidence or
        (observed != Set() and observed != remaining and not(observed.subseteq(pending)))),
      chain' = chain, budget' = budget,
      wroteAfterDeadline' = wroteAfterDeadline,
    }
  }

  action send = all {
    phase == Send,
    if (CHECK_DEADLINE and budget == 0) all {
      result' = LEASES.mapBy(l => if (remaining.contains(l)) Failed else result.get(l)),
      phase' = Done, chain' = chain, wroteAfterDeadline' = wroteAfterDeadline,
    } else all {
      wroteAfterDeadline' = (wroteAfterDeadline or budget == 0),
      if (batch.forall(l => chain.get(l) == Pending)) all {
        chain' = LEASES.mapBy(l => if (batch.contains(l)) Active else chain.get(l)),
        result' = LEASES.mapBy(l => if (batch.contains(l)) Acked else result.get(l)),
        phase' = Done,
      } else all {
        phase' = Observe, chain' = chain, result' = result,
      },
    },
    budget' = if (budget > 0) budget - 1 else 0,
    batch' = batch, remaining' = remaining, retries' = retries,
    retriedWithoutEvidence' = retriedWithoutEvidence,
  }

  // Non-state transaction failures: fee/gas surfaces immediately; unrelated
  // errors retain the existing individual fallback. Neither replenishes time.
  action failedRPC = all {
    phase == Send,
    nondet feeGas = Set(false, true).oneOf()
    all {
      phase' = if (feeGas) Done else Individual,
      result' = LEASES.mapBy(l => if (feeGas and remaining.contains(l)) Failed else result.get(l)),
      chain' = chain, budget' = if (budget > 0) budget - 1 else 0,
      batch' = batch, remaining' = remaining, retries' = retries,
      wroteAfterDeadline' = wroteAfterDeadline, retriedWithoutEvidence' = retriedWithoutEvidence,
    },
  }

  action individual = all {
    phase == Individual,
    nondet l = remaining.oneOf()
    val acked = budget > 0 and (chain.get(l) == Pending or chain.get(l) == Active)
    all {
      result' = result.set(l, if (acked) Acked else Failed),
      chain' = if (acked) chain.set(l, Active) else chain,
      remaining' = remaining.exclude(Set(l)),
      phase' = if (remaining == Set(l)) Done else Individual,
      budget' = if (budget > 0) budget - 1 else 0,
      batch' = batch, retries' = retries,
      wroteAfterDeadline' = wroteAfterDeadline, retriedWithoutEvidence' = retriedWithoutEvidence,
    }
  }

  action stutter = all {
    phase == Done,
    chain' = chain, result' = result, remaining' = remaining, batch' = batch,
    phase' = phase, budget' = budget, retries' = retries,
    wroteAfterDeadline' = wroteAfterDeadline, retriedWithoutEvidence' = retriedWithoutEvidence,
  }

  action step = any { change, timePasses, observe, send, failedRPC, individual, stutter }
  val deadlineRespected = not(wroteAfterDeadline)
  val retryRequiresEvidence = not(retriedWithoutEvidence)
  val retriesBounded = retries < LEASES.size()
  val successRequiresActive = LEASES.forall(l => result.get(l) == Acked implies chain.get(l) == Active)
}

module fixed {
  import ackRetryBudget(CHECK_DEADLINE = true).*
}

module missingDeadlineCheck {
  import ackRetryBudget(CHECK_DEADLINE = false).*
}
