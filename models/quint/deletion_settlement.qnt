// Deletion by rename (proposed, not implemented): when may a volume's caller
// settle?
//
// A destroy renames the volume to a tombstone, syncs the parent directory,
// makes one bounded cleanup attempt, publishes accounting and returns. The
// lease's reservation is durable. It is released either by the destroy's own
// caller, on the destroy's answer, or by a settlement reader (a close verdict,
// an operation rollback, the reaper) that reads the volume listing without the
// lease's lock. Accounting publications also run on their own, in two steps:
// a snapshot of the tombstones, then the pool update and the acknowledgments.
//
// The model has two leases. Lease 2's volume is created only after lease 1's
// volume is fully reclaimed, and it takes the same project ID. That is the
// case in which a key that is not unique to one deletion can be recycled.
//
// A crash loses memory (marks, tokens, the pool, the publication in flight)
// and resolves each unsynced rename either way. A failed parent sync is a
// storage-authority failure and stops the process, so it is the same step.
//
// Each constant is one rule of the plan. A variant that turns one off is a
// negative control.
//   - GUARD_READERS: the listing adds every name with an uncounted deletion.
//   - ACK_NEEDS_DURABLE: a mark is acknowledged only after the parent sync.
//   - ACK_ON_RECLAIM: complete, durable reclamation discharges a mark.
//   - ACK_BY_TOKEN: an acknowledgment names one deletion, not a project ID.
//   - RETURN_AFTER_PUBLISH: a destroy answers only after its own publication.
//   - RESTART_UNCOUNTED: after a restart every tombstone is uncounted.
//
// Not modeled: sizes and quota (deletion_quota.qnt), cleanup steps and their
// failure classes, executor fairness, storage identity checks, and a new
// volume created under a name whose old tombstone still exists.
module deletionSettlement {
  const GUARD_READERS: bool
  const ACK_NEEDS_DURABLE: bool
  const ACK_ON_RECLAIM: bool
  const ACK_BY_TOKEN: bool
  const RETURN_AFTER_PUBLISH: bool
  const RESTART_UNCOUNTED: bool

  // Volatile: renamed, parent directory not synced yet.
  type Tomb = NoTomb | Volatile | Durable | Reclaimed
  // Pending: marked, rename not known durable. Eligible: durable, uncounted.
  type Mark = Unmarked | Pending | Eligible | Acked
  type Call = Idle | Marked | Renamed | Synced | Attempted | Publishing | Returned | Failed

  type State = {
    up: bool,
    // Disk and durable records.
    live: int -> bool,
    tomb: int -> Tomb,
    record: int -> bool,     // the caller's reservation for this lease's volume
    closing: int -> bool,    // a destroy of this lease's volume was requested
    created2: bool,
    // Memory of the volume manager and the pool.
    mark: int -> Mark,
    seq: int -> int,
    nextSeq: int,
    call: int -> Call,       // the destroy holding this lease's lock
    answered: int -> bool,   // a destroy of this lease answered success
    snapActive: bool,
    snapCounted: Set[int],
    snapTokens: Set[int],
    snapOwner: int,
    published: Set[int],
    poolReady: bool,
  }

  var s: State
  pure val LEASES = Set(1, 2)

  def pending(l: int): bool = s.tomb.get(l) == Volatile or s.tomb.get(l) == Durable
  def uncounted(l: int): bool = s.mark.get(l) == Pending or s.mark.get(l) == Eligible
  def listed(l: int): bool = s.live.get(l) or (GUARD_READERS and uncounted(l))
  def created(l: int): bool = l == 1 or s.created2
  def tokenable(l: int): bool = and {
    pending(l),
    s.mark.get(l) == Eligible or (not(ACK_NEEDS_DURABLE) and s.mark.get(l) == Pending),
  }
  def discharged(l: int): Mark = if (ACK_ON_RECLAIM) Acked else s.mark.get(l)

  // A publication first reads the tombstones and the marks it may acknowledge.
  def snapshot(owner: int): State = {
    ...s,
    snapActive: true,
    snapCounted: LEASES.filter(l => pending(l)),
    snapTokens: LEASES.filter(l => tokenable(l)).map(l => s.seq.get(l)),
    snapOwner: owner,
  }

  action init = s' = {
    up: true,
    live: Map(1 -> true, 2 -> false),
    tomb: Map(1 -> NoTomb, 2 -> NoTomb),
    record: Map(1 -> true, 2 -> false),
    closing: Map(1 -> false, 2 -> false),
    created2: false,
    mark: Map(1 -> Unmarked, 2 -> Unmarked),
    seq: Map(1 -> 0, 2 -> 0),
    nextSeq: 1,
    call: Map(1 -> Idle, 2 -> Idle),
    answered: Map(1 -> false, 2 -> false),
    snapActive: false,
    snapCounted: Set(),
    snapTokens: Set(),
    snapOwner: 0,
    published: Set(),
    poolReady: true,
  }

  // Destroy, step 2 of the plan: mark the deletion uncounted before the rename.
  action destroyMark = {
    nondet l = LEASES.oneOf()
    all {
      s.up, s.call.get(l) == Idle, s.live.get(l),
      s' = {
        ...s,
        closing: s.closing.set(l, true),
        mark: s.mark.set(l, Pending),
        seq: s.seq.set(l, s.nextSeq),
        nextSeq: s.nextSeq + 1,
        call: s.call.set(l, Marked),
      },
    }
  }

  action destroyRename = {
    nondet l = LEASES.oneOf()
    all {
      s.up, s.call.get(l) == Marked,
      s' = {
        ...s,
        live: s.live.set(l, false),
        tomb: s.tomb.set(l, Volatile),
        call: s.call.set(l, Renamed),
      },
    }
  }

  // The parent sync succeeds. Its failure is the crash action.
  action destroySync = {
    nondet l = LEASES.oneOf()
    all {
      s.up, s.call.get(l) == Renamed,
      s' = {
        ...s,
        tomb: s.tomb.set(l, Durable),
        mark: s.mark.set(l, if (s.mark.get(l) == Pending) Eligible else s.mark.get(l)),
        call: s.call.set(l, Synced),
      },
    }
  }

  // The bounded attempt either reclaims the volume completely or leaves it.
  action destroyAttempt = {
    nondet l = LEASES.oneOf()
    nondet done = Set(true, false).oneOf()
    all {
      s.up, s.call.get(l) == Synced,
      s' = {
        ...s,
        tomb: s.tomb.set(l, if (done) Reclaimed else s.tomb.get(l)),
        mark: s.mark.set(l, if (done) discharged(l) else s.mark.get(l)),
        call: s.call.set(l, Attempted),
      },
    }
  }

  // The destroy's own publication. Publications are serialized.
  action destroyPublish = {
    nondet l = LEASES.oneOf()
    all {
      s.up, s.poolReady, s.call.get(l) == Attempted, not(s.snapActive),
      s' = { ...snapshot(l), call: s.call.set(l, Publishing) },
    }
  }

  // Before a process's first full refresh, a destroy's publication publishes
  // no term and acknowledges nothing. The destroy still answers. Startup's own
  // reap runs in that window. It takes the same lock as every publication, so
  // it cannot overtake a refresh whose snapshot is already taken.
  action destroyPublishEarly = {
    nondet l = LEASES.oneOf()
    all {
      s.up, not(s.poolReady), not(s.snapActive), s.call.get(l) == Attempted,
      s' = {
        ...s,
        call: s.call.set(l, Returned),
        answered: s.answered.set(l, true),
      },
    }
  }

  // An accounting refresh that no destroy asked for. After a restart, the
  // first one is the full refresh that lets the pool serve.
  action publishBackground = all {
    s.up, not(s.snapActive),
    s' = snapshot(0),
  }

  action publishApply = {
    val acked = LEASES.filter(l => and {
      uncounted(l),
      if (ACK_BY_TOKEN) s.snapTokens.contains(s.seq.get(l)) else s.snapTokens != Set(),
    })
    val returns = LEASES.filter(l => l == s.snapOwner and s.call.get(l) == Publishing)
    all {
      s.up, s.snapActive,
      s' = {
        ...s,
        snapActive: false,
        snapOwner: 0,
        published: s.snapCounted,
        poolReady: true,
        mark: LEASES.mapBy(l => if (acked.contains(l)) Acked else s.mark.get(l)),
        call: LEASES.mapBy(l => if (returns.contains(l)) Returned else s.call.get(l)),
        answered: LEASES.mapBy(l => s.answered.get(l) or returns.contains(l)),
      },
    }
  }

  // A failed publication changes nothing and fails the destroy that asked.
  action publishFail = all {
    s.up, s.snapActive,
    s' = {
      ...s,
      snapActive: false,
      snapOwner: 0,
      call: LEASES.mapBy(l =>
        if (l == s.snapOwner and s.call.get(l) == Publishing) Failed else s.call.get(l)),
    },
  }

  action callerSettle = {
    nondet l = LEASES.oneOf()
    all {
      s.up, s.record.get(l),
      or {
        s.call.get(l) == Returned,
        not(RETURN_AFTER_PUBLISH) and (s.call.get(l) == Synced or s.call.get(l) == Attempted),
      },
      s' = { ...s, record: s.record.set(l, false) },
    }
  }

  action destroyRelease = {
    nondet l = LEASES.oneOf()
    all {
      s.up, s.call.get(l) == Returned or s.call.get(l) == Failed,
      s' = { ...s, call: s.call.set(l, Idle) },
    }
  }

  // A retried destroy that finds its own tombstone publishes again.
  action destroyRetry = {
    nondet l = LEASES.oneOf()
    all {
      s.up, s.call.get(l) == Idle, s.closing.get(l), s.record.get(l),
      s.tomb.get(l) == Durable or s.tomb.get(l) == Reclaimed,
      s' = { ...s, call: s.call.set(l, Attempted) },
    }
  }

  // A reader that holds no lease lock settles on the listing alone.
  action readerSettle = {
    nondet l = LEASES.oneOf()
    all {
      s.up, s.record.get(l), s.closing.get(l), not(listed(l)),
      s' = { ...s, record: s.record.set(l, false) },
    }
  }

  // The background executor finishes a tombstone under the lease's lock.
  action sweep = {
    nondet l = LEASES.oneOf()
    all {
      s.up, s.poolReady, s.call.get(l) == Idle, s.tomb.get(l) == Durable,
      s' = {
        ...s,
        tomb: s.tomb.set(l, Reclaimed),
        mark: s.mark.set(l, discharged(l)),
      },
    }
  }

  // Lease 2's volume reuses the project ID that lease 1's deletion released.
  action createSecond = all {
    s.up, s.poolReady, not(s.created2), s.tomb.get(1) == Reclaimed,
    s' = {
      ...s,
      created2: true,
      live: s.live.set(2, true),
      record: s.record.set(2, true),
    },
  }

  // Memory is lost. An unsynced rename either reached the disk or did not.
  action crash = {
    nondet kept1 = Set(true, false).oneOf()
    nondet kept2 = Set(true, false).oneOf()
    val kept = Map(1 -> kept1, 2 -> kept2)
    all {
      s.up,
      s' = {
        ...s,
        up: false,
        live: LEASES.mapBy(l =>
          if (s.tomb.get(l) == Volatile and not(kept.get(l))) true else s.live.get(l)),
        tomb: LEASES.mapBy(l =>
          if (s.tomb.get(l) == Volatile) { if (kept.get(l)) Durable else NoTomb }
          else s.tomb.get(l)),
        mark: LEASES.mapBy(l => Unmarked),
        seq: LEASES.mapBy(l => 0),
        nextSeq: 1,
        call: LEASES.mapBy(l => Idle),
        answered: LEASES.mapBy(l => false),
        snapActive: false,
        snapCounted: Set(),
        snapTokens: Set(),
        snapOwner: 0,
        published: Set(),
        poolReady: false,
      },
    }
  }

  // Startup finds the tombstones on disk. The pool serves only after the
  // first successful publication.
  action restart = all {
    not(s.up),
    s' = {
      ...s,
      up: true,
      mark: LEASES.mapBy(l =>
        if (s.tomb.get(l) == Durable) { if (RESTART_UNCOUNTED) Eligible else Acked }
        else Unmarked),
      seq: LEASES.mapBy(l => l),
      nextSeq: 3,
    },
  }

  action step = any {
    destroyMark, destroyRename, destroySync, destroyAttempt, destroyPublish,
    destroyPublishEarly,
    publishBackground, publishApply, publishFail, callerSettle, destroyRelease,
    destroyRetry, readerSettle, sweep, createSecond, crash, restart,
  }

  // The same steps without crashes, failed publications or refreshes: the
  // schedule in which everything goes well.
  action progressStep = any {
    destroyMark, destroyRename, destroySync, destroyAttempt, destroyPublish,
    publishApply, callerSettle, destroyRelease, sweep, createSecond,
  }

  // One schedule that random sampling misses at the default budget: a
  // publication snapshots lease 1's deletion, lease 1 is reclaimed, lease 2
  // reuses its project ID and is renamed, and only then the publication
  // acknowledges. It ends when the bounded attempt leaves lease 1 unreclaimed.
  action recycledKeyWitness =
    if (s.live.get(1) and s.call.get(1) == Idle) destroyMark
    else if (s.call.get(1) == Marked or s.call.get(2) == Marked) destroyRename
    else if (s.call.get(1) == Renamed) destroySync
    else if (s.call.get(1) == Synced and not(s.snapActive)) publishBackground
    else if (s.call.get(1) == Synced) destroyAttempt
    else if (not(s.created2)) createSecond
    else if (s.live.get(2) and s.call.get(2) == Idle) destroyMark
    else if (s.snapActive) publishApply
    else readerSettle

  // While the pool serves, bytes that exist are counted: by the lease's
  // reservation or by a published tombstone.
  val accounted = (s.up and s.poolReady) implies LEASES.forall(l =>
    (s.live.get(l) or pending(l)) implies (s.record.get(l) or s.published.contains(l)))

  // A reservation is released only for a deletion that a crash cannot undo.
  val durableSettlement = LEASES.forall(l =>
    (created(l) and not(s.record.get(l))) implies
      (s.tomb.get(l) == Durable or s.tomb.get(l) == Reclaimed))

  // Once a destroy answered success for a fully reclaimed volume, no mark
  // keeps its name in the listing.
  val noGhostName = LEASES.forall(l =>
    (s.answered.get(l) and s.tomb.get(l) == Reclaimed) implies not(uncounted(l)))

  // Reachability control, expected to be violated: both leases end reclaimed
  // and settled, so the model is not vacuous.
  val neverBothDone = not(and {
    s.created2,
    LEASES.forall(l => s.tomb.get(l) == Reclaimed and not(s.record.get(l))),
  })
}

// The plan, revision 6.
module plan {
  import deletionSettlement(
    GUARD_READERS = true, ACK_NEEDS_DURABLE = true, ACK_ON_RECLAIM = true,
    ACK_BY_TOKEN = true, RETURN_AFTER_PUBLISH = true, RESTART_UNCOUNTED = true
  ).*
}

// Revision 4: readers settle on a missing name alone.
module absenceOnly {
  import deletionSettlement(
    GUARD_READERS = false, ACK_NEEDS_DURABLE = true, ACK_ON_RECLAIM = true,
    ACK_BY_TOKEN = true, RETURN_AFTER_PUBLISH = true, RESTART_UNCOUNTED = true
  ).*
}

// Revision 5: any publication that counted a tombstone clears its mark.
module ackBeforeDurable {
  import deletionSettlement(
    GUARD_READERS = true, ACK_NEEDS_DURABLE = false, ACK_ON_RECLAIM = true,
    ACK_BY_TOKEN = true, RETURN_AFTER_PUBLISH = true, RESTART_UNCOUNTED = true
  ).*
}

// Revision 5: nothing clears the mark of a volume reclaimed before publication.
module noAckOnReclaim {
  import deletionSettlement(
    GUARD_READERS = true, ACK_NEEDS_DURABLE = true, ACK_ON_RECLAIM = false,
    ACK_BY_TOKEN = true, RETURN_AFTER_PUBLISH = true, RESTART_UNCOUNTED = true
  ).*
}

// Mutant: an acknowledgment is keyed by project ID, which a later deletion
// can reuse.
module ackByProjectID {
  import deletionSettlement(
    GUARD_READERS = true, ACK_NEEDS_DURABLE = true, ACK_ON_RECLAIM = true,
    ACK_BY_TOKEN = false, RETURN_AFTER_PUBLISH = true, RESTART_UNCOUNTED = true
  ).*
}

// Mutant: the destroy answers after the sync, before its own publication.
module returnBeforePublish {
  import deletionSettlement(
    GUARD_READERS = true, ACK_NEEDS_DURABLE = true, ACK_ON_RECLAIM = true,
    ACK_BY_TOKEN = true, RETURN_AFTER_PUBLISH = false, RESTART_UNCOUNTED = true
  ).*
}

// Variant: tombstones found at startup are treated as already counted.
module restartCounted {
  import deletionSettlement(
    GUARD_READERS = true, ACK_NEEDS_DURABLE = true, ACK_ON_RECLAIM = true,
    ACK_BY_TOKEN = true, RETURN_AFTER_PUBLISH = true, RESTART_UNCOUNTED = false
  ).*
}
