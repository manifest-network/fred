// ENG-2130: source admission and restart recovery must agree. The original
// restore_retention model covers stranded data but has no recovery action.
// `owned` abstracts exact UUID/tenant/provider and placement/backend authority.
// Redelivered means Fred authorized the same durable operation ID, NOT that the
// backend accepted or completed it. Retention is checked by the backend, not by
// validateRestoreSource; an absent source placement permits this exact retry.
module restoreSourceRecovery {
  const ADMIT_REJECTED: bool
  const RECOVER_ALL_TERMINAL: bool
  const REQUIRE_RETENTION_ON_RETRY: bool
  const BYPASS_RECOVERY_OWNERSHIP: bool
  type SourceState = Closed | Expired | Rejected | Pending | Active | Unknown
  type DataState = Retained | ConsumedBySameOperation | Reaped
  type Phase = Fresh | Accepted | Interrupted | Redelivered | Preserved | Refused
  var source: SourceState
  var owned: bool
  var data: DataState
  var phase: Phase

  pure val terminal = Set(Closed, Expired, Rejected)
  def admissible = Set(Closed, Expired).contains(source) or (ADMIT_REJECTED and source == Rejected)
  def recoverable = if (RECOVER_ALL_TERMINAL) terminal.contains(source) else source == Closed

  action init = {
    nondet initial = Set(Closed, Expired, Rejected, Pending, Active, Unknown).oneOf()
    nondet exact = Set(true, false).oneOf()
    nondet initialData = Set(Retained, Reaped).oneOf()
    all { source' = initial, owned' = exact, data' = initialData, phase' = Fresh }
  }

  // Accepted abstracts both Fred's admission and the backend's retention check.
  action admit = all {
    phase == Fresh,
    phase' = if (owned and admissible and data == Retained) Accepted else Refused,
    source' = source, owned' = owned, data' = data,
  }

  action interrupt = all {
    phase == Accepted, phase' = Interrupted,
    source' = source, owned' = owned, data' = data,
  }

  action loseAuthority = all {
    phase == Interrupted, owned' = false,
    source' = source, data' = data, phase' = phase,
  }

  // Either the same operation consumed the source or retention was reaped.
  // Neither is a Fred retry gate. The backend distinguishes idempotent replay
  // from a request for missing data; this model stops before that response.
  action removeRetention = all {
    phase == Interrupted, data == Retained,
    nondet outcome = Set(ConsumedBySameOperation, Reaped).oneOf()
    all { data' = outcome, source' = source, owned' = owned, phase' = phase }
  }

  action recover = all {
    phase == Interrupted,
    phase' = if ((owned or BYPASS_RECOVERY_OWNERSHIP) and recoverable and
                 (not(REQUIRE_RETENTION_ON_RETRY) or data == Retained))
             Redelivered else Preserved,
    source' = source, owned' = owned, data' = data,
  }

  action stutter = all {
    Set(Redelivered, Preserved, Refused).contains(phase),
    source' = source, owned' = owned, data' = data, phase' = phase,
  }

  action step = any { admit, interrupt, loseAuthority, removeRetention, recover, stutter }
  val acceptedSourceCanRecover =
    (Set(Interrupted, Preserved).contains(phase) and owned) implies recoverable
  val consumedSourceCanRetry =
    (phase == Preserved and owned and terminal.contains(source)) implies data != ConsumedBySameOperation
  val onlyOwnedTerminalSourceDispatched =
    Set(Accepted, Redelivered).contains(phase) implies (owned and terminal.contains(source))
}

module current {
  import restoreSourceRecovery(ADMIT_REJECTED = false, RECOVER_ALL_TERMINAL = false,
    REQUIRE_RETENTION_ON_RETRY = false, BYPASS_RECOVERY_OWNERSHIP = false).*
}

module admissionOnly {
  import restoreSourceRecovery(ADMIT_REJECTED = true, RECOVER_ALL_TERMINAL = false,
    REQUIRE_RETENTION_ON_RETRY = false, BYPASS_RECOVERY_OWNERSHIP = false).*
}

module fixed {
  import restoreSourceRecovery(ADMIT_REJECTED = true, RECOVER_ALL_TERMINAL = true,
    REQUIRE_RETENTION_ON_RETRY = false, BYPASS_RECOVERY_OWNERSHIP = false).*
}

module retentionGatedRetry {
  import restoreSourceRecovery(ADMIT_REJECTED = true, RECOVER_ALL_TERMINAL = true,
    REQUIRE_RETENTION_ON_RETRY = true, BYPASS_RECOVERY_OWNERSHIP = false).*
}

module uncheckedRecoveryOwnership {
  import restoreSourceRecovery(ADMIT_REJECTED = true, RECOVER_ALL_TERMINAL = true,
    REQUIRE_RETENTION_ON_RETRY = false, BYPASS_RECOVERY_OWNERSHIP = true).*
}
