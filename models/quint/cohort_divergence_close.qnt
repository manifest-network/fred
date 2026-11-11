// ENG-2155: docker-backend's recovery pass publishes a Ready lease whose
// runtime cohort diverged, releases its snapshot, and only later routes the
// divergence report to the lease's actor. Routing delivers only while the
// projection reads Ready. A close admitted in between owns the actor while the
// projection still reads Ready (until its teardown marks it Deprovisioning), so
// the report is delivered and queues behind the close.
//
// The actor answers a report it applied with the outcome (success or a handled
// failure), and a report it did not apply as superseded: the lease left Ready,
// a close owns it, or the actor retired. Each policy is one way the pass and
// actor may behave:
//   - ErrorOnSuperseded: the historical behavior. The superseded answers for a
//     close (owns the lease, actor retired) are error values that abort the
//     pass; a stale generation answers nil.
//   - TypedAnswer: the fix. Superseded is not an error: the pass skips it and
//     aborts only on a handled failure.
//   - SkipAllErrors: a mutant that stops aborting on any answer, hiding a
//     handled failure.
//   - ApplyDespiteClose: a mutant actor that applies the report although a
//     failed close still owns the lease.
module cohortDivergencePolicies {
  type Policy = ErrorOnSuperseded | TypedAnswer | SkipAllErrors | ApplyDespiteClose
}

module cohortDivergenceClose {
  import cohortDivergencePolicies.*
  const POLICY: Policy

  type Projection = ProjReady | ProjDeprovisioning | ProjGone | ProjCloseFailed
  type Close = NoClose | Admitting | TearingDown | ClosedOk | ClosedFailed
  type Report = Unsent | Queued | Refused | HandledOk | HandledFailed | Superseded
  type Pass = Routing | Done | Aborted

  var projection: Projection
  var close: Close
  var report: Report
  var pass: Pass
  var handledFailure: bool
  var appliedUnderClose: bool

  action init = all {
    projection' = ProjReady,
    close' = NoClose,
    report' = Unsent,
    pass' = Routing,
    handledFailure' = false,
    appliedUnderClose' = false,
  }

  // A tenant's close of the lease is admitted after publication. Its actor owns
  // the lease at once; the projection still reads Ready.
  action closeAdmitted = all {
    close == NoClose,
    close' = Admitting,
    projection' = projection, report' = report, pass' = pass,
    handledFailure' = handledFailure, appliedUnderClose' = appliedUnderClose,
  }

  action closeTearsDown = all {
    close == Admitting,
    close' = TearingDown,
    projection' = ProjDeprovisioning,
    report' = report, pass' = pass,
    handledFailure' = handledFailure, appliedUnderClose' = appliedUnderClose,
  }

  // A successful close removes the projection and retires the actor; a failed
  // one keeps both, and the close still owns the lease.
  action closeEnds = all {
    close == TearingDown,
    nondet succeeded = Set(true, false).oneOf()
    all {
      close' = if (succeeded) ClosedOk else ClosedFailed,
      projection' = if (succeeded) ProjGone else ProjCloseFailed,
      report' = report, pass' = pass,
      handledFailure' = handledFailure, appliedUnderClose' = appliedUnderClose,
    }
  }

  // The pass routes the report. A projection that left Ready refuses it. An
  // idle actor handles it at once, which may fail; an actor owning a close
  // queues it behind that close.
  action passRoutes = all {
    report == Unsent, pass == Routing,
    nondet handlerFails = Set(true, false).oneOf()
    all {
      report' = if (projection != ProjReady) Refused
        else if (close == NoClose) (if (handlerFails) HandledFailed else HandledOk)
        else Queued,
      handledFailure' = (handledFailure or (projection == ProjReady and close == NoClose and handlerFails)),
      projection' = projection, close' = close, pass' = pass,
      appliedUnderClose' = appliedUnderClose,
    }
  }

  // The actor reaches the queued report once its close ended.
  action actorAnswersQueued = all {
    report == Queued, Set(ClosedOk, ClosedFailed).contains(close),
    val applies = POLICY == ApplyDespiteClose and close == ClosedFailed
    all {
      report' = if (applies) HandledOk else Superseded,
      appliedUnderClose' = (appliedUnderClose or applies),
      projection' = projection, close' = close, pass' = pass,
      handledFailure' = handledFailure,
    }
  }

  action passReadsAnswer = all {
    pass == Routing,
    Set(Refused, HandledOk, HandledFailed, Superseded).contains(report),
    pass' = if (report == HandledFailed and POLICY != SkipAllErrors) Aborted
      else if (report == Superseded and POLICY == ErrorOnSuperseded) Aborted
      else Done,
    projection' = projection, close' = close, report' = report,
    handledFailure' = handledFailure, appliedUnderClose' = appliedUnderClose,
  }

  action stutter = all {
    Set(Done, Aborted).contains(pass),
    projection' = projection, close' = close, report' = report, pass' = pass,
    handledFailure' = handledFailure, appliedUnderClose' = appliedUnderClose,
  }

  action step = any {
    closeAdmitted, closeTearsDown, closeEnds, passRoutes, actorAnswersQueued, passReadsAnswer, stutter,
  }

  // The pass aborts only for a report its actor handled and failed.
  val noAbortOnSupersededReport = pass == Aborted implies handledFailure
  // A handled failure is never skipped.
  val handledFailureAborts = handledFailure implies pass != Done
  // A divergence is never applied to a lease a close owns.
  val noDivergenceUnderClose = not(appliedUnderClose)
}

module current {
  import cohortDivergencePolicies.*
  import cohortDivergenceClose(POLICY = ErrorOnSuperseded).*
}

module fixed {
  import cohortDivergencePolicies.*
  import cohortDivergenceClose(POLICY = TypedAnswer).*
}

module skipAllErrors {
  import cohortDivergencePolicies.*
  import cohortDivergenceClose(POLICY = SkipAllErrors).*
}

module applyDespiteClose {
  import cohortDivergencePolicies.*
  import cohortDivergenceClose(POLICY = ApplyDespiteClose).*
}
