// -*- mode: Bluespec; -*-
//
// Restore of retained data from a closed source lease S into a new PENDING
// destination lease D, across Fred and the docker backend.
//
// Question: can a tenant's retained data end up somewhere no restore can ever
// reach it again (short of the age reaper destroying it on schedule)?
//
// Code refs (manifest-network/fred @ a802bca):
//   Fred
//     placement/restore_coordinator.go   ExecuteApplication: source must be CLOSED or EXPIRED
//     placement/operation_coordinator.go validateRestoreSource (attempt recovery): CLOSED only
//     placement/callback_apply.go        Success -> ack; Failed + PENDING -> reject
//     placement/callback_coordinator.go  TimeoutCoordinator.Settle -> reject (10 min)
//     reconciler.go                      terminal lease with a workload -> deprovision
//   docker-backend
//     restore.go / README "Soft-delete & Restore": active -> restoring; commit = exact
//       destination Release; failure before commit hands the data back to S
//     deprovision.go handoffCommittedRestoreToClose: closing a committed destination
//       consumes S's finalizer; close_settlement.go retains D's volumes under D
//       (retain_on_close does not look at chain state)
//
// Abstractions: one tenant data set; the backend's internal rollback/quota steps
// are folded into "handback" and "commit"; retention age is a flag.

module restore {
  // Chain states the restore admission accepts for a source lease.
  const ADMISSIBLE_SOURCE_STATES: Set[str]
  // true = code as written; false = ENG-709 direction (timeout never rejects)
  const TIMEOUT_REJECTS: bool

  // Where the tenant's data is.
  type Loc = RetainedS | RestoringToD | LiveD | RetainedD | Destroyed

  var loc: Loc
  var chainD: str          // "PENDING" | "ACTIVE" | "REJECTED" | "EXPIRED" | "CLOSED"
  var committed: bool      // destination Release written (restore commit marker)
  var successQueued: bool  // backend queued the restore Success callback
  var expired: bool        // retention_max_age passed for the retained record
  var runtimeFailed: bool  // the restored workload failed after commit
  var lastAction: str

  pure val chainS = "CLOSED"

  action init = all {
    loc' = RetainedS, chainD' = "PENDING", committed' = false, successQueued' = false,
    expired' = false, runtimeFailed' = false, lastAction' = "init",
  }

  // ------------------------------------------------------------- restore path
  action admitRestore = all {
    loc == RetainedS, chainD == "PENDING", ADMISSIBLE_SOURCE_STATES.contains(chainS), not(expired),
    loc' = RestoringToD,
    chainD' = chainD, committed' = committed, successQueued' = successQueued, expired' = expired, runtimeFailed' = runtimeFailed,
    lastAction' = "Fred+backend: restore admitted (S active -> restoring)",
  }

  action commit = all {
    loc == RestoringToD, not(committed),
    committed' = true, successQueued' = true, loc' = LiveD,
    chainD' = chainD, expired' = expired, runtimeFailed' = runtimeFailed,
    lastAction' = "backend: volumes adopted, destination Release committed, Success queued",
  }

  action failBeforeCommit = all {
    loc == RestoringToD, not(committed),
    loc' = RetainedS,
    chainD' = chainD, committed' = committed, successQueued' = successQueued, expired' = expired, runtimeFailed' = runtimeFailed,
    lastAction' = "backend: restore failed before commit, data handed back to S",
  }

  // Fred applies the Success callback: ack a PENDING destination.
  action deliverSuccess = all {
    successQueued,
    nondet ackOk = Set(true, false).oneOf()
    all {
      chainD' = if (chainD == "PENDING" and ackOk) "ACTIVE" else chainD,
      // ack failed while PENDING: claims released, retried later
      successQueued' = (chainD == "PENDING" and not(ackOk)),
      loc' = loc, committed' = committed, expired' = expired, runtimeFailed' = runtimeFailed,
      lastAction' = if (chainD == "PENDING" and ackOk) "Fred: Success applied, D acknowledged"
                    else if (chainD == "PENDING") "Fred: Success authenticated, ack failed (ENG-2129 window)"
                    else "Fred: Success for a terminal D, settled quietly",
    }
  }

  // ------------------------------------------------------ chain / Fred timers
  action timeoutRejectsD = all {
    TIMEOUT_REJECTS, chainD == "PENDING", loc != RetainedS,
    chainD' = "REJECTED",
    loc' = loc, committed' = committed, successQueued' = successQueued, expired' = expired, runtimeFailed' = runtimeFailed,
    lastAction' = "Fred: callback timeout rejects D",
  }

  action pendingExpiresD = all {
    chainD == "PENDING", loc != RetainedS,
    chainD' = "EXPIRED",
    loc' = loc, committed' = committed, successQueued' = successQueued, expired' = expired, runtimeFailed' = runtimeFailed,
    lastAction' = "chain: D's pending_timeout expires it",
  }

  action tenantClosesD = all {
    chainD == "ACTIVE",
    chainD' = "CLOSED",
    loc' = loc, committed' = committed, successQueued' = successQueued, expired' = expired, runtimeFailed' = runtimeFailed,
    lastAction' = "tenant closes D",
  }

  // Fred deprovisions a terminal D that holds a workload; the backend's close
  // hands the committed restore off and retains D's volumes under D.
  action deprovisionD = all {
    Set("REJECTED", "EXPIRED", "CLOSED").contains(chainD), loc == LiveD,
    loc' = RetainedD,
    chainD' = chainD, committed' = committed, successQueued' = successQueued, expired' = expired, runtimeFailed' = runtimeFailed,
    lastAction' = "Fred: deprovision terminal D; backend retains D's volumes under D",
  }

  // A second restore, from D, into a fresh destination (modelled as the data
  // becoming live again under a new ACTIVE lease).
  action restoreFromD = all {
    loc == RetainedD, ADMISSIBLE_SOURCE_STATES.contains(chainD), not(expired),
    loc' = LiveD, chainD' = "ACTIVE",
    committed' = committed, successQueued' = false, expired' = expired, runtimeFailed' = runtimeFailed,
    lastAction' = "tenant restores D's retained data into a new lease",
  }

  action ageReaper = all {
    not(expired), Set(RetainedS, RetainedD).contains(loc),
    expired' = true, loc' = Destroyed, runtimeFailed' = runtimeFailed,
    chainD' = chainD, committed' = committed, successQueued' = successQueued,
    lastAction' = "backend: retention_max_age reaper destroys the record",
  }

  // The committed workload fails before Fred acknowledges D.
  action runtimeFails = all {
    loc == LiveD, committed, not(runtimeFailed),
    runtimeFailed' = true,
    loc' = loc, chainD' = chainD, committed' = committed, successQueued' = successQueued, expired' = expired,
    lastAction' = "backend: restored workload fails after commit",
  }

  // planLease: PENDING + Failed -> Reject (a positive failure fact).
  action reconcilerRejectsFailedD = all {
    chainD == "PENDING", runtimeFailed,
    chainD' = "REJECTED",
    loc' = loc, committed' = committed, successQueued' = successQueued, expired' = expired,
    runtimeFailed' = runtimeFailed,
    lastAction' = "Fred: reconciler rejects PENDING D with a Failed workload",
  }

  action step = any {
    runtimeFails, reconcilerRejectsFailedD,
    admitRestore, commit, failBeforeCommit, deliverSuccess,
    timeoutRejectsD, pendingExpiresD, tenantClosesD, deprovisionD,
    restoreFromD, ageReaper,
  }

  // --------------------------------------------------------------- property

  // Retained data is always reachable by a restore (or live, or in flight),
  // until the age reaper destroys it on schedule.
  val retainedDataStaysRestorable: bool =
    loc == RetainedD implies ADMISSIBLE_SOURCE_STATES.contains(chainD)
}

module current {
  import restore(ADMISSIBLE_SOURCE_STATES = Set("CLOSED", "EXPIRED"), TIMEOUT_REJECTS = true).*
}

module fixed {
  import restore(ADMISSIBLE_SOURCE_STATES = Set("CLOSED", "EXPIRED", "REJECTED"), TIMEOUT_REJECTS = true).*
}

// ENG-709 direction alone: timeout never rejects, admission unchanged.
module noTimeoutReject {
  import restore(ADMISSIBLE_SOURCE_STATES = Set("CLOSED", "EXPIRED"), TIMEOUT_REJECTS = false).*
}

// Both changes.
module both {
  import restore(ADMISSIBLE_SOURCE_STATES = Set("CLOSED", "EXPIRED", "REJECTED"), TIMEOUT_REJECTS = false).*
}
