// -*- mode: Bluespec; -*-
//
// Fred provider daemon: ONE flush of ONE acknowledgment lane.
//
// Scope (code refs are to manifest-network/fred @ a802bca):
//   - internal/provisioner/ack_batcher.go      (batchLoop.flush, acknowledgeIndividually)
//   - internal/provisioner/ack_observation.go  (observeAcknowledgment, acknowledgeCurrent)
//   - internal/chain/client.go                 (AcknowledgeLeases: one MsgAcknowledgeLease
//                                               per <=100 leases)
//   - manifest-ledger x/billing/keeper/msg_server.go AcknowledgeLease:
//       validatePendingLeaseBatch is fail-fast, so ONE non-PENDING lease fails
//       the whole message
//
// Question asked of the model:
//   "Does a caller ever get an acknowledgment error for a lease that stayed
//    PENDING (i.e. that could have been acknowledged) during the flush?"
//
// Timing abstraction: time is counted in blocks. A committed tx costs one block
// (the client waits for inclusion). A batch that fails simulation costs none.
// The flush budget is AckFlushTimeout / block time = tx_timeout (30s default)
// / ~5s (PERFORMANCE.md) = 6 blocks.
//
// The environment may move any PENDING lease out of PENDING at any time:
// MsgCancelLease by the tenant, or the chain's pending_timeout expiry in
// EndBlock. Slow provisions finishing near their pending timeout make the
// latter likely under load.

module ackLane {
  // false = code as written (a failed batch falls back to one tx per lease);
  // true  = proposed fix (re-observe, drop non-PENDING leases, retry the batch)
  const RETRY_FILTERED_BATCH: bool
  const LEASES: Set[str]
  const BUDGET: int

  type LeaseState = Pending | Active | Gone
  type Phase = Collecting | Broadcasting | Fallback | Done
  type Res = Unset | Acked | Err

  var chain: str -> LeaseState
  var phase: Phase
  var toAck: Set[str]      // leases in the batch about to be broadcast
  var remaining: Set[str]  // individual-fallback queue
  var blocks: int          // blocks consumed by this flush
  var result: str -> Res   // what each waiting caller receives

  action init = all {
    chain' = LEASES.mapBy(_ => Pending),
    phase' = Collecting,
    toAck' = Set(),
    remaining' = Set(),
    blocks' = 0,
    result' = LEASES.mapBy(_ => Unset),
  }

  def unresolved: Set[str] = LEASES.filter(l => result.get(l) == Unset)

  // ------------------------------------------------------------ environment

  action leaseLeavesPending = all {
    phase != Done,
    val candidates = LEASES.filter(l => chain.get(l) == Pending)
    all {
      candidates != Set(),
      nondet l = candidates.oneOf()
      chain' = chain.set(l, Gone),
      phase' = phase, toAck' = toAck, remaining' = remaining,
      blocks' = blocks, result' = result,
    }
  }

  // ------------------------------------------------------------------ lane

  // GetPendingLeases, then observeAcknowledgment for anything missing from it.
  action query = all {
    phase == Collecting,
    val pending = unresolved.filter(l => chain.get(l) == Pending)
    val active  = unresolved.filter(l => chain.get(l) == Active)
    val gone    = unresolved.filter(l => chain.get(l) == Gone)
    all {
      toAck' = pending,
      result' = LEASES.mapBy(l =>
        if (active.contains(l)) Acked
        else if (gone.contains(l)) Err
        else result.get(l)),
      phase' = Broadcasting,
      chain' = chain, remaining' = remaining, blocks' = blocks,
    }
  }

  // AcknowledgeLeases(toAck): all-or-nothing on chain.
  action broadcast = all {
    phase == Broadcasting,
    if (toAck == Set()) all {
      phase' = Done,
      chain' = chain, toAck' = toAck, remaining' = remaining, blocks' = blocks, result' = result,
    } else if (blocks >= BUDGET) all {
      // flush context deadline
      result' = LEASES.mapBy(l => if (toAck.contains(l)) Err else result.get(l)),
      phase' = Done,
      chain' = chain, toAck' = Set(), remaining' = remaining, blocks' = blocks,
    } else if (toAck.forall(l => chain.get(l) == Pending)) all {
      chain' = LEASES.mapBy(l => if (toAck.contains(l)) Active else chain.get(l)),
      result' = LEASES.mapBy(l => if (toAck.contains(l)) Acked else result.get(l)),
      blocks' = blocks + 1,
      phase' = Done,
      toAck' = Set(), remaining' = remaining,
    } else all {
      // ErrLeaseNotPending for the whole message
      phase' = if (RETRY_FILTERED_BATCH) Collecting else Fallback,
      remaining' = if (RETRY_FILTERED_BATCH) remaining else toAck,
      toAck' = Set(),
      chain' = chain, blocks' = blocks, result' = result,
    }
  }

  // acknowledgeIndividually -> acknowledgeCurrent, one lease at a time.
  action fallbackStep = all {
    phase == Fallback,
    if (remaining == Set()) all {
      phase' = Done,
      chain' = chain, toAck' = toAck, remaining' = remaining, blocks' = blocks, result' = result,
    } else {
      nondet l = remaining.oneOf()
      val st = chain.get(l)
      all {
        remaining' = remaining.exclude(Set(l)),
        if (blocks >= BUDGET) all {
          // observeAcknowledgment returns ctx.Err() once the flush deadline passed
          result' = result.set(l, Err), chain' = chain, blocks' = blocks,
        } else if (st == Active) all {
          result' = result.set(l, Acked), chain' = chain, blocks' = blocks,
        } else if (st == Gone) all {
          result' = result.set(l, Err), chain' = chain, blocks' = blocks,
        } else all {
          result' = result.set(l, Acked), chain' = chain.set(l, Active), blocks' = blocks + 1,
        },
        phase' = phase, toAck' = toAck,
      }
    }
  }

  action stutter = all {
    phase == Done,
    chain' = chain, phase' = phase, toAck' = toAck, remaining' = remaining,
    blocks' = blocks, result' = result,
  }

  action step = any { leaseLeavesPending, query, broadcast, fallbackStep, stutter }

  // ------------------------------------------------------------- properties

  // A caller receives an error only when its lease genuinely cannot be
  // acknowledged any more (it left PENDING).
  val noSpuriousAckError: bool =
    LEASES.forall(l => result.get(l) == Err implies chain.get(l) != Pending)

  val spuriousErrors: int =
    LEASES.filter(l => result.get(l) == Err and chain.get(l) == Pending).size()
}

module current {
  import ackLane(
    RETRY_FILTERED_BATCH = false,
    LEASES = Set("l1", "l2", "l3", "l4", "l5", "l6", "l7", "l8"),
    BUDGET = 6
  ).*
}

module fixed {
  import ackLane(
    RETRY_FILTERED_BATCH = true,
    LEASES = Set("l1", "l2", "l3", "l4", "l5", "l6", "l7", "l8"),
    BUDGET = 6
  ).*
}

// Smaller instances for exhaustive bounded checking with Apalache.
module currentSmall {
  import ackLane(
    RETRY_FILTERED_BATCH = false,
    LEASES = Set("l1", "l2", "l3", "l4", "l5"),
    BUDGET = 3
  ).*
}

module fixedSmall {
  import ackLane(
    RETRY_FILTERED_BATCH = true,
    LEASES = Set("l1", "l2", "l3", "l4", "l5"),
    BUDGET = 3
  ).*
}
