// ENG-2154: docker-backend's periodic recovery pass resumes every close intent
// in its snapshot, including a close that a live request is still running. The
// live close's committed head has already released the lease's command fence,
// so recovery takes the fence, reads the intent as still pending, and queues
// its own close behind the live one. When the live close succeeds, its actor
// retires and rejects the queued close without handling it.
//
// The answer to a close is either the outcome of a close the actor handled, or
// the fact that it handled nothing (NotRun). Each policy is one way recovery
// may treat NotRun:
//   - ReportError: the historical behavior. A close nobody ran is logged as a
//     failed close.
//   - Readmit: the fix. Admit the close again under a fresh command fence,
//     which re-reads the intent, and route it only if it is still pending.
//   - RerouteUnread: a mutant that routes again without re-reading the intent.
//   - Drop: a mutant that treats NotRun as done.
//
// `replacement` abstracts any later admission of the same lease UUID once its
// close has settled and the fence is free. A close that runs after it tears the
// replacement down. The single live close runs on the first actor; an actor
// that retires for another reason (a stale-generation observation) also
// rejects what it has queued, with the intent still pending. Recovery's
// blocking and TryLock entry points share the admission modeled here.
module closeRecoveryPolicies {
  type NotRunPolicy = ReportError | Readmit | RerouteUnread | Drop
}

module closeRecoveryRouting {
  import closeRecoveryPolicies.*
  const POLICY: NotRunPolicy

  type Intent = Pending | Settled
  type Live = LiveTearingDown | LiveSucceeded | LiveFailed
  type Actor = ActorOpen | ActorRetiring | ActorGone
  type Fence = FenceFree | FenceRecovery
  type Recovery = RecAdmitting | RecHolding | RecQueued | RecNotRun | RecRanOk | RecRanFailed
    | RecUnreadReroute | RecDone | RecErrored

  var intent: Intent
  var live: Live
  var actor: Actor
  var fence: Fence
  var rec: Recovery
  var replacement: bool
  var staleTeardown: bool
  var closeFailed: bool

  action init = {
    // Either a live close is still tearing down (the reported race), or an
    // earlier close failed and its intent waits for recovery, with or without
    // a registered actor.
    nondet liveInit = Set(LiveTearingDown, LiveFailed).oneOf()
    nondet actorInit = Set(ActorOpen, ActorGone).oneOf()
    all {
      intent' = Pending,
      live' = liveInit,
      actor' = if (liveInit == LiveTearingDown) ActorOpen else actorInit,
      // A committed close head has given the fence back.
      fence' = FenceFree,
      rec' = RecAdmitting,
      replacement' = false,
      staleTeardown' = false,
      closeFailed' = false,
    }
  }

  // The live close ends. Success settles the intent and retires its actor;
  // failure keeps both, and the actor handles the next queued close.
  action liveFinishes = all {
    live == LiveTearingDown,
    nondet succeeded = Set(true, false).oneOf()
    all {
      live' = if (succeeded) LiveSucceeded else LiveFailed,
      intent' = if (succeeded) Settled else intent,
      actor' = if (succeeded) ActorRetiring else actor,
      fence' = fence, rec' = rec, replacement' = replacement,
      staleTeardown' = staleTeardown, closeFailed' = closeFailed,
    }
  }

  // An idle actor retires for a reason of its own, intent untouched.
  action actorRetiresStale = all {
    actor == ActorOpen, live != LiveTearingDown,
    actor' = ActorRetiring,
    intent' = intent, live' = live, fence' = fence, rec' = rec,
    replacement' = replacement, staleTeardown' = staleTeardown, closeFailed' = closeFailed,
  }

  // Retirement rejects every queued command unhandled, which gives its fence
  // back, and only then unregisters the actor.
  action retirementCompletes = all {
    actor == ActorRetiring,
    actor' = ActorGone,
    rec' = if (rec == RecQueued) RecNotRun else rec,
    fence' = if (rec == RecQueued) FenceFree else fence,
    intent' = intent, live' = live, replacement' = replacement,
    staleTeardown' = staleTeardown, closeFailed' = closeFailed,
  }

  // Admission: take the fence, re-read the intent, and keep the fence only when
  // a close is still pending.
  action recAdmits = all {
    rec == RecAdmitting, fence == FenceFree,
    rec' = if (intent == Settled) RecDone else RecHolding,
    fence' = if (intent == Settled) FenceFree else FenceRecovery,
    intent' = intent, live' = live, actor' = actor, replacement' = replacement,
    staleTeardown' = staleTeardown, closeFailed' = closeFailed,
  }

  // Routing: a retiring actor refuses new commands, so routing waits until it
  // is gone and then reaches a fresh actor. The command carries the fence.
  action recRoutes = all {
    rec == RecHolding, actor != ActorRetiring,
    rec' = RecQueued,
    actor' = if (actor == ActorGone) ActorOpen else actor,
    intent' = intent, live' = live, fence' = fence, replacement' = replacement,
    staleTeardown' = staleTeardown, closeFailed' = closeFailed,
  }

  // The mutant's re-route takes a free fence but reads nothing under it.
  action recReroutesUnread = all {
    rec == RecUnreadReroute, fence == FenceFree, actor != ActorRetiring,
    rec' = RecQueued,
    fence' = FenceRecovery,
    actor' = if (actor == ActorGone) ActorOpen else actor,
    intent' = intent, live' = live, replacement' = replacement,
    staleTeardown' = staleTeardown, closeFailed' = closeFailed,
  }

  // An idle open actor handles the queued close. The close gives the fence back
  // at its head or at its end; a successful close settles and retires.
  action actorRunsQueuedClose = all {
    rec == RecQueued, actor == ActorOpen, live != LiveTearingDown,
    nondet succeeded = Set(true, false).oneOf()
    all {
      rec' = if (succeeded) RecRanOk else RecRanFailed,
      intent' = if (succeeded) Settled else intent,
      actor' = if (succeeded) ActorRetiring else actor,
      fence' = FenceFree,
      staleTeardown' = (staleTeardown or replacement),
      live' = live, replacement' = replacement, closeFailed' = closeFailed,
    }
  }

  action recSettlesAnswer = all {
    Set(RecNotRun, RecRanOk, RecRanFailed).contains(rec),
    rec' = if (rec == RecRanOk) RecDone
      else if (rec == RecRanFailed) RecErrored
      else match POLICY {
        | ReportError => RecErrored
        | Readmit => RecAdmitting
        | RerouteUnread => RecUnreadReroute
        | Drop => RecDone
      },
    closeFailed' = (closeFailed or rec == RecRanFailed),
    intent' = intent, live' = live, actor' = actor, fence' = fence,
    replacement' = replacement, staleTeardown' = staleTeardown,
  }

  // Environment: the lease UUID is admitted again after its close settled.
  action replacementAdmitted = all {
    not(replacement), intent == Settled, fence == FenceFree,
    replacement' = true,
    intent' = intent, live' = live, actor' = actor, fence' = fence, rec' = rec,
    staleTeardown' = staleTeardown, closeFailed' = closeFailed,
  }

  action stutter = all {
    Set(RecDone, RecErrored).contains(rec),
    intent' = intent, live' = live, actor' = actor, fence' = fence, rec' = rec,
    replacement' = replacement, staleTeardown' = staleTeardown, closeFailed' = closeFailed,
  }

  action step = any {
    liveFinishes, actorRetiresStale, retirementCompletes, recAdmits, recRoutes,
    recReroutesUnread, actorRunsQueuedClose, recSettlesAnswer, replacementAdmitted, stutter,
  }

  // Recovery reports a failure only for a close it routed that ran and failed.
  val noSpuriousCloseError = rec == RecErrored implies closeFailed
  // No close recovery routes ever runs against a replacement admission.
  val noStaleTeardown = not(staleTeardown)
  // Recovery finishes only once the close it was resuming has settled.
  val noDroppedPendingClose = rec == RecDone implies intent == Settled
}

module current {
  import closeRecoveryPolicies.*
  import closeRecoveryRouting(POLICY = ReportError).*
}

module fixed {
  import closeRecoveryPolicies.*
  import closeRecoveryRouting(POLICY = Readmit).*
}

module rerouteUnread {
  import closeRecoveryPolicies.*
  import closeRecoveryRouting(POLICY = RerouteUnread).*
}

module dropNotRun {
  import closeRecoveryPolicies.*
  import closeRecoveryRouting(POLICY = Drop).*
}
