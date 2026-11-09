// ENG-2130: source admission and restart recovery must agree. The original
// restore_retention model covers stranded data but has no recovery action.
// `owned` abstracts exact UUID/tenant/provider and placement/backend authority;
// the Go tests exercise those mismatches individually. Retention remains a
// backend decision, independent of the chain's terminal state.
module restoreSourceRecovery {
  const ADMIT_REJECTED: bool
  const RECOVER_ALL_TERMINAL: bool
  type SourceState = Closed | Expired | Rejected | Pending | Active | Unknown
  type Phase = Fresh | Accepted | Interrupted | Recovered | Preserved | Refused
  var source: SourceState
  var owned: bool
  var retained: bool
  var phase: Phase

  pure val terminal = Set(Closed, Expired, Rejected)
  def admissible = Set(Closed, Expired).contains(source) or (ADMIT_REJECTED and source == Rejected)
  def recoverable = if (RECOVER_ALL_TERMINAL) terminal.contains(source) else source == Closed

  action init = {
    nondet initial = Set(Closed, Expired, Rejected, Pending, Active, Unknown).oneOf()
    nondet exact = Set(true, false).oneOf()
    nondet dataExists = Set(true, false).oneOf()
    all { source' = initial, owned' = exact, retained' = dataExists, phase' = Fresh }
  }

  action admit = all {
    phase == Fresh,
    phase' = if (owned and admissible and retained) Accepted else Refused,
    source' = source, owned' = owned, retained' = retained,
  }

  // The durable attempt survives an ambiguous response or providerd restart.
  action interrupt = all {
    phase == Accepted, phase' = Interrupted,
    source' = source, owned' = owned, retained' = retained,
  }

  // Lost authority or reaped data must not become a successful restore merely
  // because a prior attempt had an eligible source state.
  action loseEvidence = all {
    phase == Interrupted,
    nondet loseOwnership = Set(true, false).oneOf()
    all {
      owned' = if (loseOwnership) false else owned,
      retained' = if (loseOwnership) retained else false,
      source' = source, phase' = phase,
    }
  }

  action recover = all {
    phase == Interrupted,
    phase' = if (owned and recoverable and retained) Recovered else Preserved,
    source' = source, owned' = owned, retained' = retained,
  }

  action stutter = all {
    Set(Recovered, Preserved, Refused).contains(phase),
    source' = source, owned' = owned, retained' = retained, phase' = phase,
  }

  action step = any { admit, interrupt, loseEvidence, recover, stutter }
  val acceptedSourceCanRecover =
    (Set(Interrupted, Preserved).contains(phase) and owned and retained) implies recoverable
  val onlyOwnedTerminalDataRestored =
    Set(Accepted, Recovered).contains(phase) implies (owned and retained and terminal.contains(source))
}

module current {
  import restoreSourceRecovery(ADMIT_REJECTED = false, RECOVER_ALL_TERMINAL = false).*
}

module admissionOnly {
  import restoreSourceRecovery(ADMIT_REJECTED = true, RECOVER_ALL_TERMINAL = false).*
}

module fixed {
  import restoreSourceRecovery(ADMIT_REJECTED = true, RECOVER_ALL_TERMINAL = true).*
}
