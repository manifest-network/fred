// -*- mode: Bluespec; -*-
//
// Fred <-> backend maintenance commands (tenant restart / update) for ONE lease,
// two commands in sequence, with crashes, lost HTTP responses, completions
// that race the HTTP response, and the lease ending mid-command.
//
// Code refs (manifest-network/fred @ a802bca):
//   provider side (internal/provisioner/placement):
//     maintenance_command.go      beginMaintenanceCommand (one pending pendingHead per lease)
//     maintenance_coordinator.go  executeMaintenance, classifyMaintenanceCall,
//                                 completeConfirmedUpdate (payload, then receipt)
//     maintenance_callback.go     recordMaintenanceCompletion (Update only; exact id,
//                                 pending pendingHead, admission stamp; unknown -> 2xx no-op)
//     maintenance_application.go  dispatch, RecoverPending (re-send / wait / finalize)
//   backend side (internal/backend/shared):
//     maintenance_receipt.go      exact replay accepted; one maintenance at a time
//     callbacks.go                per-lease FIFO, row removed only after 2xx
//   ARCHITECTURE.md "Tenant restart/update admission"
//
// Abstractions: one backend; at most one in-flight HTTP call; the backend's
// own crash recovery is folded into its worker (its journal makes the outcome
// durable); receipt-window eviction and admission-stamp expiry are out of scope.

module maintenance {
  type Kind = Restart | Update
  type Phase = NoPhase | DeliveryOutstanding | CompletionOutstanding | PayloadConfirmed
  type Outcome = NotAdmitted | Pending | Accepted | ExecutionFailed | InvalidState | LeaseEnded
  type BState = BUnknown | BExecuting | BSucceeded | BFailed
  type Resp = RNone | RAccepted | RRefusedBusy

  pure val IDS = Set(1, 2)

  // provider journal
  var kind: int -> Kind
  var outcome: int -> Outcome
  var phase: int -> Phase
  var pendingHead: int                 // pending pendingHead (0 = none)
  var payloadFrom: int          // command whose update payload is in the payload store (0 = original)
  // one HTTP call
  var inflight: int             // command id of the call in flight (0 = none)
  var reached: bool             // the backend processed it
  var resp: Resp
  // backend
  var bstate: int -> BState
  var fifo: List[{ id: int, ok: bool }]   // durable completion queue (Update and Restart both queue)
  // chain
  var live: bool
  var lastAction: str

  action init = all {
    kind' = IDS.mapBy(_ => Restart),
    outcome' = IDS.mapBy(_ => NotAdmitted),
    phase' = IDS.mapBy(_ => NoPhase),
    pendingHead' = 0, payloadFrom' = 0,
    inflight' = 0, reached' = false, resp' = RNone,
    bstate' = IDS.mapBy(_ => BUnknown),
    fifo' = List(),
    live' = true,
    lastAction' = "init",
  }

  action unchangedProvider = all {
    kind' = kind, outcome' = outcome, phase' = phase, pendingHead' = pendingHead, payloadFrom' = payloadFrom,
  }
  action unchangedCall = all { inflight' = inflight, reached' = reached, resp' = resp }
  action unchangedBackend = all { bstate' = bstate, fifo' = fifo }

  // ---------------------------------------------------------- tenant / API
  // beginMaintenanceCommand: next unused id, only when no pending pendingHead.
  action submit = all {
    live, pendingHead == 0,
    val unused = IDS.filter(i => outcome.get(i) == NotAdmitted)
    all {
      unused != Set(),
      val id = if (unused.contains(1)) 1 else 2
      nondet k = Set(Restart, Update).oneOf()
      all {
        kind' = kind.set(id, k),
        outcome' = outcome.set(id, Pending),
        phase' = phase.set(id, DeliveryOutstanding),
        pendingHead' = id, payloadFrom' = payloadFrom,
        unchangedCall, unchangedBackend, live' = live,
        lastAction' = "provider: admit command",
      }
    }
  }

  // ------------------------------------------------------------ dispatch
  // Live request or RecoverPending: (re)send the exact command while its
  // delivery is outstanding.
  action send = all {
    pendingHead != 0, outcome.get(pendingHead) == Pending, phase.get(pendingHead) == DeliveryOutstanding, inflight == 0,
    inflight' = pendingHead, reached' = false, resp' = RNone,
    unchangedProvider, unchangedBackend, live' = live,
    lastAction' = "provider: send command",
  }

  // Backend admission: exact replay of a known id is accepted; a new id is
  // refused while another maintenance executes; otherwise admitted.
  action backendReceive = all {
    inflight != 0, not(reached), live,
    val id = inflight
    val known = bstate.get(id) != BUnknown
    val busy = IDS.exists(j => j != id and bstate.get(j) == BExecuting)
    all {
      reached' = true,
      resp' = if (known or not(busy)) RAccepted else RRefusedBusy,
      bstate' = if (not(known) and not(busy)) bstate.set(id, BExecuting) else bstate,
      fifo' = fifo, inflight' = inflight,
      unchangedProvider, live' = live,
      lastAction' = if (known) "backend: exact replay accepted"
                    else if (busy) "backend: refused, another maintenance executing"
                    else "backend: admitted",
    }
  }

  // The provider receives the HTTP response (executeMaintenance).
  action receiveResponse = all {
    inflight != 0, reached,
    val id = inflight
    val stillPending = outcome.get(id) == Pending
    all {
      if (not(stillPending)) all {
        // e.g. a failed completion already retired it: keep the receipt
        outcome' = outcome, phase' = phase, pendingHead' = pendingHead,
      } else if (resp == RRefusedBusy) all {
        outcome' = outcome.set(id, InvalidState), phase' = phase.set(id, NoPhase),
        pendingHead' = 0,
      } else if (kind.get(id) == Restart) all {
        // restart settles on HTTP acceptance
        outcome' = outcome.set(id, Accepted), phase' = phase.set(id, NoPhase),
        pendingHead' = 0,
      } else all {
        // update: acceptMaintenanceUpdate keeps PayloadConfirmed if the
        // completion already arrived
        outcome' = outcome,
        phase' = if (phase.get(id) == DeliveryOutstanding) phase.set(id, CompletionOutstanding) else phase,
        pendingHead' = pendingHead,
      },
      kind' = kind, payloadFrom' = payloadFrom,
      inflight' = 0, reached' = false, resp' = RNone,
      unchangedBackend, live' = live,
      lastAction' = "provider: HTTP response applied",
    }
  }

  // Transport failure / timeout / provider crash: the response is lost.
  action loseResponse = all {
    inflight != 0,
    inflight' = 0, reached' = false, resp' = RNone,
    unchangedProvider, unchangedBackend, live' = live,
    lastAction' = if (reached) "network: response lost after backend processed it"
                  else "network: request lost",
  }

  // ----------------------------------------------------------- backend work
  action backendWork = all {
    IDS.exists(i => bstate.get(i) == BExecuting),
    nondet id = IDS.filter(i => bstate.get(i) == BExecuting).oneOf()
    nondet ok = Set(true, false).oneOf()
    all {
      bstate' = bstate.set(id, if (ok) BSucceeded else BFailed),
      fifo' = fifo.append({ id: id, ok: ok }),
      unchangedProvider, unchangedCall, live' = live,
      lastAction' = if (ok) "backend: maintenance succeeded, completion queued"
                    else "backend: maintenance failed, completion queued",
    }
  }

  // ------------------------------------------------------------ completion
  // recordMaintenanceCompletion, then 2xx (the backend removes the row).
  action deliverCompletion = all {
    fifo.length() > 0,
    val c = fifo.head()
    val applies = kind.get(c.id) == Update and outcome.get(c.id) == Pending and pendingHead == c.id and
                  Set(DeliveryOutstanding, CompletionOutstanding).contains(phase.get(c.id))
    all {
      phase' = if (applies and c.ok) phase.set(c.id, PayloadConfirmed)
               else if (applies) phase.set(c.id, NoPhase)
               else phase,
      outcome' = if (applies and not(c.ok)) outcome.set(c.id, ExecutionFailed) else outcome,
      pendingHead' = if (applies and not(c.ok)) 0 else pendingHead,
      kind' = kind, payloadFrom' = payloadFrom,
      fifo' = fifo.tail(), bstate' = bstate,
      unchangedCall, live' = live,
      lastAction' = if (not(applies)) "provider: completion acknowledged without effect"
                    else if (c.ok) "provider: update success recorded"
                    else "provider: update failure settled",
    }
  }

  // completeConfirmedUpdate: payload store first, then the terminal receipt.
  action finalize = all {
    pendingHead != 0, outcome.get(pendingHead) == Pending, phase.get(pendingHead) == PayloadConfirmed,
    payloadFrom' = pendingHead,
    outcome' = outcome.set(pendingHead, Accepted), phase' = phase.set(pendingHead, NoPhase), pendingHead' = 0,
    kind' = kind, unchangedCall, unchangedBackend, live' = live,
    lastAction' = "provider: payload persisted, update settled",
  }

  // A crash between payload write and receipt (payload written, still pending).
  action finalizeCrashMidway = all {
    pendingHead != 0, outcome.get(pendingHead) == Pending, phase.get(pendingHead) == PayloadConfirmed,
    payloadFrom' = pendingHead,
    outcome' = outcome, phase' = phase, pendingHead' = pendingHead,
    kind' = kind, inflight' = 0, reached' = false, resp' = RNone,
    unchangedBackend, live' = live,
    lastAction' = "provider: crash after payload write, before receipt",
  }

  // --------------------------------------------------------------- lease end
  action leaseEnds = all {
    live,
    live' = false,
    // close preempts executing maintenance; close owns settlement (no completion)
    bstate' = IDS.mapBy(i => if (bstate.get(i) == BExecuting) BFailed else bstate.get(i)),
    fifo' = fifo,
    unchangedProvider, unchangedCall,
    lastAction' = "chain: lease ended, backend close preempts maintenance",
  }

  action cancelOnLeaseEnd = all {
    not(live), pendingHead != 0, outcome.get(pendingHead) == Pending,
    outcome' = outcome.set(pendingHead, LeaseEnded), phase' = phase.set(pendingHead, NoPhase), pendingHead' = 0,
    kind' = kind, payloadFrom' = payloadFrom,
    unchangedCall, unchangedBackend, live' = live,
    lastAction' = "provider: pending command cancelled (lease ended)",
  }

  def providerCanAct: bool =
    (pendingHead != 0 and outcome.get(pendingHead) == Pending and phase.get(pendingHead) == DeliveryOutstanding and inflight == 0) or
    (inflight != 0 and not(reached) and live) or
    (inflight != 0 and reached) or
    IDS.exists(i => bstate.get(i) == BExecuting) or
    fifo.length() > 0 or
    (pendingHead != 0 and outcome.get(pendingHead) == Pending and phase.get(pendingHead) == PayloadConfirmed) or
    (not(live) and pendingHead != 0 and outcome.get(pendingHead) == Pending)

  action step = any {
    submit, send, backendReceive, receiveResponse, loseResponse,
    backendWork, deliverCompletion, finalize, finalizeCrashMidway,
    leaseEnds, cancelOnLeaseEnd,
  }

  // ------------------------------------------------------------- properties

  // The payload store only ever holds an update the backend actually applied.
  val payloadOnlyFromSucceededUpdate: bool =
    payloadFrom == 0 or (kind.get(payloadFrom) == Update and bstate.get(payloadFrom) == BSucceeded)

  // The provider's terminal receipt agrees with what the backend did.
  val receiptsAgreeWithBackend: bool = IDS.forall(i =>
    (outcome.get(i) == Accepted and kind.get(i) == Update implies bstate.get(i) == BSucceeded) and
    (outcome.get(i) == Accepted and kind.get(i) == Restart implies bstate.get(i) != BUnknown) and
    (outcome.get(i) == ExecutionFailed implies bstate.get(i) == BFailed) and
    (outcome.get(i) == InvalidState implies bstate.get(i) == BUnknown))

  // A successful update is never lost: when nothing can act any more and the
  // lease is still live, every update the backend applied is in the payload store
  // (or was superseded by a later applied update).
  val noLostUpdate: bool =
    (not(providerCanAct) and live) implies
      IDS.forall(i => (kind.get(i) == Update and bstate.get(i) == BSucceeded) implies payloadFrom >= i)

  // No immortal command: when nothing can act any more, nothing is pending.
  val noImmortalPending: bool =
    not(providerCanAct) implies pendingHead == 0
}
