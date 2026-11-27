// Deletion by rename (proposed, not implemented): do a deleted volume's bytes
// stay bounded and counted, and can its project ID reach another volume while
// it still has usage?
//
// One old volume owns project 1 with a block limit of 2. It starts with one
// byte of content, or with three: usage can be above a limit that was set or
// lowered after the bytes were written. A leftover writer may still hold a file of it open: a rename does
// not close descriptors, and a deleted file that is still open keeps charging
// its project. The destroy renames the volume to a tombstone whose name
// records a size. Cleanup detaches the tombstone's root from the project,
// removes the tree, clears the limit and removes the tombstone. A tombstone
// whose root is still in the project always shows usage, because the
// directory itself is charged.
//
// Two creators want a project ID for a new volume each. Both start from the
// same candidate, as two names with one CRC32 would, and probe upwards. The
// kernel read happens outside the manager's lock and may fail.
//
// Each constant is one rule of the plan. A variant that turns one off is a
// negative control.
//   - BOUND_IS_CEILING: the size is max(used, limit), not the measured usage.
//   - BOUND_COUNTS_EXCESS: the ceiling is max(used, limit), not the limit alone.
//   - CLEAR_NEEDS_ZERO: the limit is cleared only at zero usage.
//   - REMOVE_NEEDS_ZERO: the tombstone stays until usage is zero and the
//     limit is cleared.
//   - DETACH_ROOT: the tombstone's root is moved out of the project.
//   - KERNEL_CHECK: the allocator skips an ID the kernel reports populated.
//   - RESERVE: the allocator reserves its candidate before the kernel read.
//
// Not modeled: when the caller settles (deletion_settlement.qnt), the
// individual cleanup failures, block rounding, inode limits as a separate
// resource, and a crash between two steps of one cleanup.
module deletionQuota {
  const BOUND_IS_CEILING: bool
  const BOUND_COUNTS_EXCESS: bool
  const CLEAR_NEEDS_ZERO: bool
  const REMOVE_NEEDS_ZERO: bool
  const DETACH_ROOT: bool
  const KERNEL_CHECK: bool
  const RESERVE: bool

  type Old = Live | TombTagged | TombDetached | Gone
  type Phase = Idle | Picked | Read | Claimed | Refused

  type State = {
    // The old volume, project 1.
    old: Old,
    tree: bool,
    writer: bool,
    open: int,          // bytes of the file the writer still holds open
    limit: bool,
    bound: int,         // the size recorded in the tombstone's name
    // The allocator.
    phase: int -> Phase,
    cand: int -> int,
    skip: int -> Set[int],
    reserved: Set[int],
    claimed: int -> int,
    inherited: bool,    // a new volume was given a project that still had usage
  }

  var s: State
  pure val CREATORS = Set(1, 2)
  pure val IDS = Set(1, 2, 3)
  pure val LIMIT = 2
  pure val CAP = 4      // what an unlimited writer can reach in this model

  val used = (if (s.tree) 1 else 0) + s.open
  val tomb = s.old == TombTagged or s.old == TombDetached
  // Blocks and inodes: the tree, an open file, or a root still in the project.
  val charged = s.tree or s.writer or s.old == TombTagged or s.old == Live
  val populated1 = charged or s.limit
  // What admission counts for the deleted volume. A live volume is counted by
  // its lease's reservation, which this model does not question.
  val counted = if (tomb) s.bound else 0

  def taken(c: int): Set[int] = (if (s.old == Gone) Set() else Set(1))
    .union(CREATORS.map(o => s.claimed.get(o)))
    .union(s.reserved)
    .union(s.skip.get(c))
  def populated(c: int, p: int): bool = or {
    p == 1 and populated1,
    CREATORS.exists(o => o != c and s.claimed.get(o) == p),
  }

  action init = {
    nondet excess = Set(true, false).oneOf()
    s' = {
      old: Live, tree: true, writer: true, limit: true, bound: 0,
      open: if (excess) 2 else 0,
      phase: Map(1 -> Idle, 2 -> Idle),
      cand: Map(1 -> 0, 2 -> 0),
      skip: Map(1 -> Set(), 2 -> Set()),
      reserved: Set(),
      claimed: Map(1 -> 0, 2 -> 0),
      inherited: false,
    }
  }

  // The writer grows its open file as far as the limit, or the cap, allows.
  action write = all {
    s.writer, used < (if (s.limit) LIMIT else CAP),
    s' = { ...s, open: s.open + 1 },
  }

  action writerCloses = all {
    s.writer,
    s' = { ...s, writer: false, open: 0 },
  }

  action destroy = all {
    s.old == Live,
    s' = {
      ...s,
      old: TombTagged,
      bound:
        if (not(BOUND_IS_CEILING)) used
        else if (BOUND_COUNTS_EXCESS and used > LIMIT) used
        else LIMIT,
    },
  }

  // The detach can be refused and is then retried on a later pass.
  action detachRoot = {
    nondet ok = Set(true, false).oneOf()
    all {
      DETACH_ROOT, s.old == TombTagged,
      s' = { ...s, old: if (ok) TombDetached else TombTagged },
    }
  }

  action removeTree = all {
    tomb, s.tree,
    s' = { ...s, tree: false },
  }

  action clearLimit = all {
    tomb, not(s.tree), s.limit,
    CLEAR_NEEDS_ZERO implies not(charged),
    s' = { ...s, limit: false },
  }

  action removeTombstone = all {
    tomb, not(s.tree),
    REMOVE_NEEDS_ZERO implies (not(charged) and not(s.limit)),
    s' = { ...s, old: Gone },
  }

  // Under the manager's lock: the lowest ID that nothing holds.
  action pick = {
    nondet c = CREATORS.oneOf()
    val free = IDS.exclude(taken(c))
    val p = if (free.contains(1)) 1 else if (free.contains(2)) 2 else 3
    all {
      s.phase.get(c) == Idle, free != Set(),
      s' = {
        ...s,
        phase: s.phase.set(c, Picked),
        cand: s.cand.set(c, p),
        reserved: if (RESERVE) s.reserved.union(Set(p)) else s.reserved,
      },
    }
  }

  // Outside the lock: ask the kernel about the candidate.
  action kernelRead = {
    nondet c = CREATORS.oneOf()
    nondet failed = Set(true, false).oneOf()
    val p = s.cand.get(c)
    val refuse = KERNEL_CHECK and failed
    val skipIt = KERNEL_CHECK and not(failed) and populated(c, p)
    all {
      s.phase.get(c) == Picked,
      s' = {
        ...s,
        phase: s.phase.set(c, if (refuse) Refused else if (skipIt) Idle else Read),
        skip: if (skipIt) s.skip.set(c, s.skip.get(c).union(Set(p))) else s.skip,
        reserved: if (refuse or skipIt) s.reserved.exclude(Set(p)) else s.reserved,
      },
    }
  }

  // Under the lock again: the new volume takes the ID, its limit and usage.
  action claim = {
    nondet c = CREATORS.oneOf()
    val p = s.cand.get(c)
    all {
      s.phase.get(c) == Read,
      s' = {
        ...s,
        phase: s.phase.set(c, Claimed),
        claimed: s.claimed.set(c, p),
        reserved: s.reserved.exclude(Set(p)),
        inherited: s.inherited or (p == 1 and populated1),
      },
    }
  }

  // A restart forgets reservations and unfinished allocations. The IDs of
  // volumes and tombstones on disk are read back.
  action restart = all {
    s' = {
      ...s,
      phase: CREATORS.mapBy(c => if (s.phase.get(c) == Claimed) Claimed else Idle),
      skip: CREATORS.mapBy(c => Set()),
      reserved: Set(),
    },
  }

  action step = any {
    write, writerCloses, destroy, detachRoot, removeTree, clearLimit,
    removeTombstone, pick, kernelRead, claim, restart,
  }

  // Once the volume is deleted, admission counts at least what its project
  // can still charge.
  val boundCovers = s.old == Live or used <= counted

  // No project is handed to a new volume while it still has usage or a limit.
  val noInheritedUsage = not(s.inherited)

  // Two volumes never share a project.
  val exclusiveIDs = (s.claimed.get(1) == 0) or (s.claimed.get(1) != s.claimed.get(2))

  // Reachability control. With the plan it is violated: a tombstone can be
  // fully reclaimed. With a root that never leaves the project it holds:
  // usage never reaches zero and the tombstone is never removed.
  val neverReclaimed = s.old != Gone
}

// The plan, revision 6.
module plan {
  import deletionQuota(
    BOUND_IS_CEILING = true, CLEAR_NEEDS_ZERO = true, REMOVE_NEEDS_ZERO = true,
    DETACH_ROOT = true, KERNEL_CHECK = true, RESERVE = true,
    BOUND_COUNTS_EXCESS = true
  ).*
}

// First review: the name records measured usage.
module measuredBound {
  import deletionQuota(
    BOUND_IS_CEILING = false, CLEAR_NEEDS_ZERO = true, REMOVE_NEEDS_ZERO = true,
    DETACH_ROOT = true, KERNEL_CHECK = true, RESERVE = true,
    BOUND_COUNTS_EXCESS = true
  ).*
}

// The design doc's "clear the limits, with no wait for zero usage".
module clearLimitEarly {
  import deletionQuota(
    BOUND_IS_CEILING = true, CLEAR_NEEDS_ZERO = false, REMOVE_NEEDS_ZERO = true,
    DETACH_ROOT = true, KERNEL_CHECK = true, RESERVE = true,
    BOUND_COUNTS_EXCESS = true
  ).*
}

// An earlier proposal: remove the tombstone once the tree is gone. The
// kernel check still keeps the project from another volume.
module removeTombstoneEarly {
  import deletionQuota(
    BOUND_IS_CEILING = true, CLEAR_NEEDS_ZERO = true, REMOVE_NEEDS_ZERO = false,
    DETACH_ROOT = true, KERNEL_CHECK = true, RESERVE = true,
    BOUND_COUNTS_EXCESS = true
  ).*
}

// The same without the kernel check: neither layer is left.
module noReuseGuard {
  import deletionQuota(
    BOUND_IS_CEILING = true, CLEAR_NEEDS_ZERO = true, REMOVE_NEEDS_ZERO = false,
    DETACH_ROOT = true, KERNEL_CHECK = false, RESERVE = true,
    BOUND_COUNTS_EXCESS = true
  ).*
}

// The plan without the kernel check: the tombstone rule alone.
module tombstoneRuleOnly {
  import deletionQuota(
    BOUND_IS_CEILING = true, CLEAR_NEEDS_ZERO = true, REMOVE_NEEDS_ZERO = true,
    DETACH_ROOT = true, KERNEL_CHECK = false, RESERVE = true,
    BOUND_COUNTS_EXCESS = true
  ).*
}

// First review: the kernel read without a reservation.
module unreservedAllocator {
  import deletionQuota(
    BOUND_IS_CEILING = true, CLEAR_NEEDS_ZERO = true, REMOVE_NEEDS_ZERO = true,
    DETACH_ROOT = true, KERNEL_CHECK = true, RESERVE = false,
    BOUND_COUNTS_EXCESS = true
  ).*
}

// Second review: a tombstone whose root stays in the project.
module rootStaysTagged {
  import deletionQuota(
    BOUND_IS_CEILING = true, CLEAR_NEEDS_ZERO = true, REMOVE_NEEDS_ZERO = true,
    DETACH_ROOT = false, KERNEL_CHECK = true, RESERVE = true,
    BOUND_COUNTS_EXCESS = true
  ).*
}

// Mutant: the name records the limit alone, although usage can exceed it.
module limitOnlyBound {
  import deletionQuota(
    BOUND_IS_CEILING = true, CLEAR_NEEDS_ZERO = true, REMOVE_NEEDS_ZERO = true,
    DETACH_ROOT = true, KERNEL_CHECK = true, RESERVE = true,
    BOUND_COUNTS_EXCESS = false
  ).*
}
