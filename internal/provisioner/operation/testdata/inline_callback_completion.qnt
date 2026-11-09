// ENG-2129 / PR #296: process-local inline callback and dispatch completion.
// Unlike the original lease_settlement.qnt bundle, this model separates the
// synchronous return from callback claim/acknowledgment and same-ID recovery.
// One exact operation is modeled; crashes, unrelated operations, failure
// callbacks, and chain expiry are outside this focused safety check.
//
// Mapping:
// - claimSuccess/ackFailed: authenticated callback, ObserveCallbackSuccess,
//   retryable acknowledgment, and ReleaseCallback (Registry + placement).
// - complete: callbackOwnsDispatch + completeInitiation, for both provision
//   and restore; source reservation cleanup is covered by Go regressions.
// - recover: accepted attempt redelivery and RecoverClaimed for the same ID.
// - timeout: the live claim guard, which already rejects recorded success.
//
// Quint 0.33.0, e.g.:
// quint run inline_callback_completion.qnt --main=fixed --backend=typescript \
//   --invariant='noTimeoutAfterSuccess and successKeepsSettlementAuthority' \
//   --max-samples=20000 --max-steps=30 --seed=1517
module inlineCompletion {
  const KEEP_SUCCESS_ON_RETURN: bool
  const CALLBACK_OWNS_SUCCESS: bool

  type Phase = Calling | Active | Missing
  type Attempt = Unresolved | Confirmed | Absent
  type Chain = Pending | Billing | Rejected
  type Outcome = Accepted | Ambiguous | Refused

  var s: {
    phase: Phase, attempt: Attempt, chain: Chain,
    successSeen: bool, authenticated: bool, callback: bool,
    returned: bool, finished: bool, outbox: bool, timedOut: bool,
    lastAction: str,
  }

  // The backend has produced its Success outbox entry while its synchronous
  // Provision/Restore call is still on the stack.
  action init = s' = {
    phase: Calling, attempt: Unresolved, chain: Pending,
    successSeen: false, authenticated: false, callback: false,
    returned: false, finished: false, outbox: true, timedOut: false,
    lastAction: "backend Success queued; synchronous call pending",
  }

  action claimSuccess = all {
    s.outbox, not(s.callback), s.phase != Missing,
    s.attempt != Absent, s.chain == Pending,
    s' = s.with("callback", true).with("authenticated", true)
          .with("successSeen", true).with("lastAction", "Success claimed and recorded"),
  }

  action ackFailed = all {
    s.callback,
    s' = s.with("callback", false).with("lastAction", "ack failed; both claims released"),
  }

  action ackSucceeded = all {
    s.callback,
    s' = s.with("callback", false).with("finished", true).with("outbox", false)
          .with("chain", Billing).with("attempt", Confirmed)
          .with("phase", if (s.returned) Missing else Calling)
          .with("lastAction", "ack succeeded; callback settled"),
  }

  action complete(outcome: Outcome): bool = all {
    not(s.returned),
    val callbackOwns = s.finished or s.callback or (CALLBACK_OWNS_SUCCESS and s.successSeen)
    val keepRegistry = s.callback or (KEEP_SUCCESS_ON_RETURN and s.successSeen) or outcome == Accepted
    val phaseAfter = if (s.finished or not(keepRegistry)) Missing else Active
    val attemptAfter = if (callbackOwns) s.attempt
                       else if (outcome == Accepted) Confirmed
                       else if (outcome == Refused) Absent
                       else s.attempt
    s' = s.with("returned", true).with("phase", phaseAfter).with("attempt", attemptAfter)
          .with("successSeen", phaseAfter != Missing and s.successSeen)
          .with("lastAction", if (outcome == Ambiguous) "synchronous ambiguous return"
                              else if (outcome == Refused) "synchronous refusal"
                              else "synchronous accepted return"),
  }

  action recover = all {
    s.returned, s.phase == Missing, s.attempt == Unresolved, s.chain == Pending,
    s' = s.with("phase", Active).with("attempt", Confirmed).with("successSeen", false)
          .with("lastAction", "accepted redelivery reinstalls the same ID"),
  }

  action timePasses = all {
    not(s.timedOut),
    s' = s.with("timedOut", true).with("lastAction", "callback timeout elapsed"),
  }

  action timeout = all {
    s.phase == Active, s.timedOut, not(s.callback), not(s.successSeen), s.chain == Pending,
    s' = s.with("phase", Missing).with("chain", Rejected)
          .with("lastAction", "timeout rejects"),
  }

  action step = any {
    claimSuccess, ackFailed, ackSucceeded,
    complete(Accepted), complete(Ambiguous), complete(Refused),
    recover, timePasses, timeout,
  }

  val noTimeoutAfterSuccess = not(s.authenticated and s.chain == Rejected)
  val successKeepsSettlementAuthority =
    (s.authenticated and not(s.finished) and s.chain == Pending) implies
      (s.phase != Missing and s.successSeen and s.attempt != Absent)
}

// PR #296 before the follow-up: the timeout marker exists, but completion can
// erase it, and the coordinator recognizes only live/finished callback claims.
module current {
  import inlineCompletion(KEEP_SUCCESS_ON_RETURN = false, CALLBACK_OWNS_SUCCESS = false).*
}

// Retaining the Registry row alone leaves synchronous refusal able to delete
// the durable attempt. The shared callback-ownership decision is also needed.
module registryOnly {
  import inlineCompletion(KEEP_SUCCESS_ON_RETURN = true, CALLBACK_OWNS_SUCCESS = false).*
}

module fixed {
  import inlineCompletion(KEEP_SUCCESS_ON_RETURN = true, CALLBACK_OWNS_SUCCESS = true).*
}
