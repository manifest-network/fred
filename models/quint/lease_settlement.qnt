// -*- mode: Bluespec; -*-
//
// Fred provider daemon: settlement of ONE lease's provision across
// chain x placement x operation registry x backend.
//
// Actors: provision dispatch, backend worker + durable callback outbox,
// callback application, callback-timeout checker, reconciler, tenant/chain
// environment, and providerd crash/restart.
//
// Code refs (manifest-network/fred @ a802bca):
//   internal/provisioner/operation/registry.go             tryClaimSettlement, timedOut
//   internal/provisioner/placement/operation_coordinator.go tryClaimCallback/Timeout,
//                                                            finish*Callback, finishTimeout*
//   internal/provisioner/placement/callback_apply.go        Success / Failed branches
//   internal/provisioner/placement/callback_coordinator.go  TimeoutCoordinator.Settle
//   internal/provisioner/reconcile_plan.go                  planLease
//   internal/provisioner/reconciler.go                      processLease, processOrphan
//   internal/backend/shared/callback_sender.go              durable outbox + replay
//
// Abstractions:
//   - one backend, one provision operation (ACTIVE re-provision is out of scope)
//   - a process's claim is held across its chain call, as in the code
//   - "time" is a flag: the operation is older than callback_timeout (10 min)
//   - the reconciler acts on an inventory snapshot taken earlier (stale reads)
//   - crash loses the registry and every in-progress claim; chain, placement,
//     backend and its callback outbox are durable

module settlement {
  // false = code as written; true = proposed fix: the Registry remembers that
  // the operation's Success callback was authenticated, and the timeout
  // checker leaves such an operation to the callback's retries.
  const SKIP_TIMEOUT_AFTER_SUCCESS: bool
  // true = code as written (timeout rejects a PENDING lease); false = ENG-709
  // direction: the timeout only untracks the operation and leaves the lease to
  // positive evidence (callbacks, reconciler) or the chain's pending_timeout.
  const TIMEOUT_REJECTS: bool

  type Chain = Pending | Active | Rejected | Closed | Expired
  type Attempt = NoAttempt | Unresolved | Confirmed
  type Backend = BNone | BProvisioning | BReady | BFailed
  type Outbox = CNone | CSuccess | CFailed
  type Claim = Free | ByCallback | ByTimeout | ByReconciler
  type CbPhase = CbIdle | CbSuccess | CbFailed

  var chain: Chain
  var attempt: Attempt          // durable placement attempt/owner for the operation
  var reg: bool                 // operation tracked in the volatile Registry (PhaseActive)
  var timedOut: bool            // operation older than callback_timeout
  var claim: Claim              // exclusive settlement / lease claim
  var cb: CbPhase               // callback application in progress
  var be: Backend               // backend provision status
  var beGot: bool               // backend received the provision request
  var outbox: Outbox            // head of the backend's durable callback queue for this lease
  var snapBe: Backend           // reconciler inventory snapshot
  var snapTaken: bool
  var dispatched: bool          // the lease-created event handler has run
  // monitors
  var successAuthenticated: bool  // Fred authenticated a Success callback for the operation
  var regSuccessSeen: bool        // volatile: Registry op carries "Success authenticated" (fix only)
  var rejectedBy: str             // who moved the lease to Rejected
  var lastAction: str

  pure def terminal(c: Chain): bool = Set(Rejected, Closed, Expired).contains(c)

  action init = all {
    chain' = Pending, attempt' = NoAttempt, reg' = false, timedOut' = false,
    claim' = Free, cb' = CbIdle, be' = BNone, beGot' = false, outbox' = CNone,
    snapBe' = BNone, snapTaken' = false, dispatched' = false,
    successAuthenticated' = false, regSuccessSeen' = false, rejectedBy' = "", lastAction' = "init",
  }

  // ------------------------------------------------------------- dispatch
  // Write-ahead attempt + Registry op, then the synchronous provision call.
  // The call may be accepted, or ambiguous (sent or not sent).
  def canDispatch: bool = not(dispatched) and chain == Pending and attempt == NoAttempt and claim == Free

  action dispatch = all {
    canDispatch,
    nondet received = Set(true, false).oneOf()
    all {
      dispatched' = true,
      attempt' = Unresolved, reg' = true, timedOut' = false,
      beGot' = received,
      be' = if (received) BProvisioning else be,
      chain' = chain, claim' = claim, cb' = cb, outbox' = outbox,
      snapBe' = snapBe, snapTaken' = snapTaken,
      successAuthenticated' = successAuthenticated, regSuccessSeen' = false, rejectedBy' = rejectedBy,
      lastAction' = if (received) "dispatch(received)" else "dispatch(lost)",
    }
  }

  // --------------------------------------------------------------- backend
  def canBackendWork: bool = be == BProvisioning and outbox == CNone

  action backendWork = all {
    canBackendWork,
    nondet ok = Set(true, false).oneOf()
    all {
      be' = if (ok) BReady else BFailed,
      outbox' = if (ok) CSuccess else CFailed,
      chain' = chain, attempt' = attempt, reg' = reg, timedOut' = timedOut,
      claim' = claim, cb' = cb, beGot' = beGot, snapBe' = snapBe, snapTaken' = snapTaken,
      dispatched' = dispatched, successAuthenticated' = successAuthenticated, regSuccessSeen' = regSuccessSeen,
      rejectedBy' = rejectedBy,
      lastAction' = if (ok) "backend: Ready, queue Success callback" else "backend: Failed, queue Failed callback",
    }
  }

  // -------------------------------------------------------------- callback
  // Claim: exact Registry op if present, otherwise the recovery claim through
  // the durable attempt. No attempt -> NotFound: the backend's head advances.
  def canCbClaim: bool = outbox != CNone and cb == CbIdle and claim == Free and attempt != NoAttempt
  def canCbNotFound: bool = outbox != CNone and cb == CbIdle and attempt == NoAttempt

  action cbClaim = all {
    canCbClaim,
    claim' = ByCallback,
    cb' = if (outbox == CSuccess) CbSuccess else CbFailed,
    successAuthenticated' = (successAuthenticated or outbox == CSuccess),
    regSuccessSeen' = (regSuccessSeen or (reg and outbox == CSuccess)),
    chain' = chain, attempt' = attempt, reg' = reg, timedOut' = timedOut, be' = be,
    beGot' = beGot, outbox' = outbox, snapBe' = snapBe, snapTaken' = snapTaken,
    dispatched' = dispatched, rejectedBy' = rejectedBy,
    lastAction' = if (outbox == CSuccess) "callback(Success) claimed" else "callback(Failed) claimed",
  }

  action cbNotFound = all {
    canCbNotFound,
    outbox' = CNone,
    chain' = chain, attempt' = attempt, reg' = reg, timedOut' = timedOut, claim' = claim,
    cb' = cb, be' = be, beGot' = beGot, snapBe' = snapBe, snapTaken' = snapTaken,
    dispatched' = dispatched, successAuthenticated' = successAuthenticated, regSuccessSeen' = regSuccessSeen, rejectedBy' = rejectedBy,
    lastAction' = "callback: NotFound, head advances",
  }

  // Success: acknowledge. On ack error, an exact reread decides: ACTIVE or
  // terminal settles; still PENDING releases both claims and returns a
  // retryable error (the backend replays the head).
  def canCbSuccess: bool = cb == CbSuccess

  action cbSuccess = all {
    canCbSuccess,
    nondet ackOk = Set(true, false).oneOf()
    val settles = chain != Pending or ackOk
    all {
      chain' = if (chain == Pending and ackOk) Active else chain,
      attempt' = if (settles) Confirmed else attempt,
      reg' = if (settles) false else reg,
      outbox' = if (settles) CNone else outbox,
      claim' = Free,
      cb' = CbIdle,
      timedOut' = timedOut, be' = be, beGot' = beGot, snapBe' = snapBe, snapTaken' = snapTaken,
      dispatched' = dispatched, successAuthenticated' = successAuthenticated, regSuccessSeen' = regSuccessSeen, rejectedBy' = rejectedBy,
      lastAction' = if (chain == Pending and ackOk) "callback(Success): acked, settled"
                    else if (settles) "callback(Success): lease not pending, settled"
                    else "callback(Success): ack failed, claims released, retryable error",
    }
  }

  // Failed: reject a PENDING lease (or settle if ACTIVE/terminal), then clear
  // the attempt (finishRefusedCallback).
  def canCbFailed: bool = cb == CbFailed

  action cbFailed = all {
    canCbFailed,
    nondet rejectOk = Set(true, false).oneOf()
    val settles = chain != Pending or rejectOk
    all {
      chain' = if (chain == Pending and rejectOk) Rejected else chain,
      rejectedBy' = if (chain == Pending and rejectOk)
                      (if (successAuthenticated) "failed-callback-after-authenticated-success" else "failed-callback")
                    else rejectedBy,
      attempt' = if (settles) NoAttempt else attempt,
      reg' = if (settles) false else reg,
      outbox' = if (settles) CNone else outbox,
      claim' = Free,
      cb' = CbIdle,
      timedOut' = timedOut, be' = be, beGot' = beGot, snapBe' = snapBe, snapTaken' = snapTaken,
      dispatched' = dispatched, successAuthenticated' = successAuthenticated, regSuccessSeen' = regSuccessSeen,
      lastAction' = if (settles) "callback(Failed): settled" else "callback(Failed): reject failed, released",
    }
  }

  // --------------------------------------------------------------- timeout
  // TimedOut lists PhaseActive ops older than callback_timeout. Settle claims,
  // rejects, and on error rereads; it never clears the attempt.
  def canTimeout: bool = reg and timedOut and claim == Free and cb == CbIdle and
                         not(SKIP_TIMEOUT_AFTER_SUCCESS and regSuccessSeen)

  action timeoutSettle = all {
    canTimeout,
    nondet rejectOk = Set(true, false).oneOf()
    val rejects = TIMEOUT_REJECTS and chain == Pending and rejectOk
    val settles = not(TIMEOUT_REJECTS) or chain != Pending or rejectOk
    all {
      chain' = if (rejects) Rejected else chain,
      rejectedBy' = if (rejects)
                      (if (successAuthenticated) "timeout-after-authenticated-success" else "timeout")
                    else rejectedBy,
      reg' = if (settles) false else reg,
      timedOut' = if (settles) false else timedOut,
      claim' = Free,
      attempt' = attempt, cb' = cb, be' = be, beGot' = beGot, outbox' = outbox,
      snapBe' = snapBe, snapTaken' = snapTaken, dispatched' = dispatched,
      successAuthenticated' = successAuthenticated, regSuccessSeen' = regSuccessSeen,
      lastAction' = if (rejects) "timeout: rejected lease, op untracked"
                    else if (not(TIMEOUT_REJECTS)) "timeout: op untracked, lease left to evidence / chain expiry"
                    else if (settles) "timeout: lease not pending, op untracked"
                    else "timeout: reject failed, retry next tick",
    }
  }

  // ------------------------------------------------------------ reconciler
  def canSnapshot: bool = not(snapTaken)

  action recSnapshot = all {
    canSnapshot,
    snapBe' = be, snapTaken' = true,
    chain' = chain, attempt' = attempt, reg' = reg, timedOut' = timedOut, claim' = claim,
    cb' = cb, be' = be, beGot' = beGot, outbox' = outbox, dispatched' = dispatched,
    successAuthenticated' = successAuthenticated, regSuccessSeen' = regSuccessSeen, rejectedBy' = rejectedBy,
    lastAction' = "reconciler: inventory snapshot",
  }

  // planLease (PENDING), processOrphan (terminal), attempt recovery and
  // inventory promotion, each under the lease claim after an exact chain reread.
  def recPendingAckFor(sb: Backend): bool = chain == Pending and sb == BReady and not(reg)
  def recPendingRejectFor(sb: Backend): bool = chain == Pending and sb == BFailed
  def recRedeliverFor(sb: Backend): bool = chain == Pending and sb == BNone and attempt == Unresolved and not(beGot) and not(reg)
  def recStartFor(sb: Backend): bool = chain == Pending and sb == BNone and attempt == NoAttempt and dispatched and not(reg)
  def recOrphanFor(sb: Backend): bool = terminal(chain) and sb != BNone and not(reg)
  def recPromoteFor(sb: Backend): bool = sb != BNone and attempt == Unresolved
  def recTerminalAttemptFor(sb: Backend): bool = terminal(chain) and sb == BNone and attempt != NoAttempt and not(reg)
  def recHasWorkFor(sb: Backend): bool = recPendingAckFor(sb) or recPendingRejectFor(sb) or recRedeliverFor(sb) or
                         recStartFor(sb) or recOrphanFor(sb) or recPromoteFor(sb) or recTerminalAttemptFor(sb)
  def recPendingAck: bool = recPendingAckFor(snapBe)
  def recPendingReject: bool = recPendingRejectFor(snapBe)
  def recRedeliver: bool = recRedeliverFor(snapBe)
  def recStart: bool = recStartFor(snapBe)
  def recOrphan: bool = recOrphanFor(snapBe)
  def recPromote: bool = recPromoteFor(snapBe)
  def recTerminalAttempt: bool = recTerminalAttemptFor(snapBe)
  def recHasWork: bool = recHasWorkFor(snapBe)
  def canRecAct: bool = snapTaken and claim == Free and cb == CbIdle and recHasWork

  action recAct = all {
    canRecAct,
    all {
      chain' = if (recPendingAck) Active else if (recPendingReject) Rejected else chain,
      rejectedBy' = if (recPendingReject)
                      (if (successAuthenticated) "reconciler-after-authenticated-success" else "reconciler")
                    else rejectedBy,
      attempt' = if (recPromote or recPendingAck) Confirmed
                 else if (recStart) Unresolved
                 else if (recTerminalAttempt) NoAttempt
                 else attempt,
      reg' = if (recStart) true else reg,
      timedOut' = if (recStart) false else timedOut,
      beGot' = if (recRedeliver or recStart) true else beGot,
      be' = if (recOrphan) BNone
            else if ((recRedeliver or recStart) and be == BNone) BProvisioning
            else be,
      claim' = Free, cb' = cb, outbox' = outbox,
      snapBe' = snapBe, snapTaken' = false, dispatched' = dispatched,
      successAuthenticated' = successAuthenticated, regSuccessSeen' = if (recStart) false else regSuccessSeen,
      lastAction' = if (recPendingAck) "reconciler: acknowledge"
                    else if (recPendingReject) "reconciler: reject"
                    else if (recOrphan) "reconciler: deprovision orphan"
                    else if (recRedeliver) "reconciler: redeliver attempt"
                    else if (recStart) "reconciler: start"
                    else if (recTerminalAttempt) "reconciler: terminal attempt teardown"
                    else "reconciler: promote attempt",
    }
  }

  // A snapshot with nothing to do is discarded (next sweep).
  action recIdle = all {
    snapTaken, not(recHasWork),
    snapTaken' = false,
    chain' = chain, attempt' = attempt, reg' = reg, timedOut' = timedOut, claim' = claim,
    cb' = cb, be' = be, beGot' = beGot, outbox' = outbox, snapBe' = snapBe,
    dispatched' = dispatched, successAuthenticated' = successAuthenticated, regSuccessSeen' = regSuccessSeen, rejectedBy' = rejectedBy,
    lastAction' = "reconciler: nothing to do",
  }

  // ------------------------------------------------------------ environment
  action timePasses = all {
    reg, not(timedOut),
    timedOut' = true,
    chain' = chain, attempt' = attempt, reg' = reg, claim' = claim, cb' = cb, be' = be,
    beGot' = beGot, outbox' = outbox, snapBe' = snapBe, snapTaken' = snapTaken,
    dispatched' = dispatched, successAuthenticated' = successAuthenticated, regSuccessSeen' = regSuccessSeen, rejectedBy' = rejectedBy,
    lastAction' = "time: op exceeds callback_timeout",
  }

  action tenantOrChainEnds = all {
    chain == Pending or chain == Active,
    nondet ending = (if (chain == Pending) Set(Closed, Expired) else Set(Closed)).oneOf()
    all {
      chain' = ending,
      attempt' = attempt, reg' = reg, timedOut' = timedOut, claim' = claim, cb' = cb, be' = be,
      beGot' = beGot, outbox' = outbox, snapBe' = snapBe, snapTaken' = snapTaken,
      dispatched' = dispatched, successAuthenticated' = successAuthenticated, regSuccessSeen' = regSuccessSeen, rejectedBy' = rejectedBy,
      lastAction' = "env: lease ends on chain",
    }
  }

  action crash = all {
    chain' = chain, attempt' = attempt, reg' = false, timedOut' = false, claim' = Free,
    cb' = CbIdle, be' = be, beGot' = beGot, outbox' = outbox, snapBe' = snapBe,
    snapTaken' = false, dispatched' = true,
    successAuthenticated' = successAuthenticated, regSuccessSeen' = false, rejectedBy' = rejectedBy,
    lastAction' = "providerd crash + restart",
  }

  def fredOrBackendCanAct: bool =
    canDispatch or canBackendWork or canCbClaim or canCbNotFound or canCbSuccess or
    canCbFailed or canTimeout or canRecAct

  action step = any {
    dispatch, backendWork, cbClaim, cbNotFound, cbSuccess, cbFailed,
    timeoutSettle, recSnapshot, recAct, recIdle,
    timePasses, tenantOrChainEnds, crash,
  }

  // Same, without the environment ending leases (tenant close, pending expiry).
  action stepNoLeaseEnd = any {
    dispatch, backendWork, cbClaim, cbNotFound, cbSuccess, cbFailed,
    timeoutSettle, recSnapshot, recAct, recIdle,
    timePasses, crash,
  }

  // ------------------------------------------------------------- properties

  // Fred never rejects a lease AFTER it has authenticated the backend's Success
  // callback for that lease's provision. (A Success that arrives after the
  // timeout already rejected is the documented callback_timeout trade-off and
  // is not flagged.)
  val noRejectAfterAuthenticatedSuccess: bool =
    not(Set("timeout-after-authenticated-success",
            "failed-callback-after-authenticated-success",
            "reconciler-after-authenticated-success").contains(rejectedBy))

  // When nothing in Fred or the backend can act any more (a fresh reconciler
  // sweep included), the lease is consistent: no workload on an ended lease,
  // no billing without a running workload, no lease stuck in PENDING with
  // a running workload.
  val quiescent: bool =
    not(canDispatch or canBackendWork or canCbClaim or canCbNotFound or canCbSuccess or
        canCbFailed or canTimeout) and
    claim == Free and not(recHasWorkFor(be))

  val quiescentIsConsistent: bool =
    quiescent implies (
      (terminal(chain) implies be == BNone) and
      (chain == Active implies be == BReady) and
      (chain == Pending implies be != BReady)
    )
}

module current {
  import settlement(SKIP_TIMEOUT_AFTER_SUCCESS = false, TIMEOUT_REJECTS = true).*
}

module fixed {
  import settlement(SKIP_TIMEOUT_AFTER_SUCCESS = true, TIMEOUT_REJECTS = true).*
}

// ENG-709 direction: the timeout never rejects.
module noTimeoutReject {
  import settlement(SKIP_TIMEOUT_AFTER_SUCCESS = false, TIMEOUT_REJECTS = false).*
}
