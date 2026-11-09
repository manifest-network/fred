// -*- mode: Bluespec; -*-
//
// Fred docker-backend: routing of live container deaths for ONE lease.
//
// Scope (code refs are to manifest-network/fred @ a802bca):
//   - internal/backend/shared/leasesm/lease_sm.go     (transition table)
//   - internal/backend/docker/container_event_loop.go (dispatchLiveContainerDeath,
//                                                      redispatchStartupDeaths)
//   - internal/backend/docker/live_death_ledger.go    (record-before-lookup ledger)
//   - internal/backend/docker/leasesm_adapters.go     (UpdateFn Ready-entry hook,
//                                                      classifyReadyProjection)
//   - internal/backend/docker/lease_actor_routing.go  (observation.Current gate)
//
// Question asked of the model:
//   "Can a lease sit in Ready with a dead container that nothing will ever tell
//    the lease actor about?"  (Only the 5-minute reconcile sweep would find it,
//    unattributed, so it never counts toward the ENG-799 failure budget.)
//
// ENG-1125 closed this race for Provisioning -> Ready. The model checks whether
// the same race exists for the replace machinery (restart, update, restore).
//
// Abstractions:
//   - one new container per worker (a cohort of size 1 is enough for the race)
//   - container ids are small ints, bounded by MAX_ID to keep the space finite
//   - the diag goroutine, callbacks and maintenance recovery are out of scope

module deathRouting {
  // false = code as written; true = proposed fix (redispatch on every Ready
  // entry that installs a new cohort, not only Provisioning -> Ready)
  const REDISPATCH_ON_REPLACE: bool

  type Status = Reserved | Provisioning | Restarting | Updating | Ready | Failing | Failed

  pure val MAX_ID = 3

  var status: Status
  // ContainerIDs in the live projection (what findLeaseByContainerID scans)
  var projection: Set[int]
  // containers created by the in-flight worker; empty when no worker runs
  var cohort: Set[int]
  // worker passed startup verification (its last inspection)
  var verified: bool
  // running containers
  var alive: Set[int]
  // liveDeathLedger: recorded deaths nobody has taken yet
  var ledger: Set[int]
  // deaths recorded by the event loop but not yet looked up by the dispatcher
  var dispatchQ: Set[int]
  // deaths admitted to the actor inbox, not yet handled
  var inbox: Set[int]
  var nextId: int
  // last action, for readable traces
  var lastAction: { act: str, c: int }

  action init = all {
    status' = Reserved,
    projection' = Set(),
    cohort' = Set(),
    verified' = false,
    alive' = Set(),
    ledger' = Set(),
    dispatchQ' = Set(),
    inbox' = Set(),
    nextId' = 0,
    lastAction' = { act: "init", c: -1 },
  }

  // ---------------------------------------------------------------- workers

  // Reserved/Failed --ProvisionRequested--> Provisioning (spawnProvisionWorker)
  action requestProvision = all {
    Set(Reserved, Failed).contains(status),
    nextId < MAX_ID,
    status' = Provisioning,
    cohort' = Set(nextId),
    verified' = false,
    alive' = alive.union(Set(nextId)),
    nextId' = nextId + 1,
    projection' = projection, ledger' = ledger, dispatchQ' = dispatchQ, inbox' = inbox,
    lastAction' = { act: "requestProvision", c: -1 },
  }

  // Ready/Failed --Restart/UpdateRequested--> Restarting/Updating, and
  // Reserved --RestoreRequested--> Restarting. All three ride spawnReplaceWorker.
  action requestReplace(target: Status, label: str): bool = all {
    nextId < MAX_ID,
    status' = target,
    cohort' = Set(nextId),
    verified' = false,
    alive' = alive.union(Set(nextId)),
    nextId' = nextId + 1,
    projection' = projection, ledger' = ledger, dispatchQ' = dispatchQ, inbox' = inbox,
    lastAction' = { act: label, c: -1 },
  }
  action requestRestart = all { Set(Ready, Failed).contains(status), requestReplace(Restarting, "requestRestart") }
  action requestUpdate  = all { Set(Ready, Failed).contains(status), requestReplace(Updating, "requestUpdate") }
  action requestRestore = all { status == Reserved, requestReplace(Restarting, "requestRestore") }

  def workerRunning: bool = Set(Provisioning, Restarting, Updating).contains(status) and cohort != Set()

  // The worker's last inspection: every new container is running.
  action verify = all {
    workerRunning, not(verified), cohort.subseteq(alive),
    verified' = true,
    status' = status, projection' = projection, cohort' = cohort, alive' = alive,
    ledger' = ledger, dispatchQ' = dispatchQ, inbox' = inbox, nextId' = nextId,
    lastAction' = { act: "verify", c: -1 },
  }

  // A cohort container died before verification: the worker reports a startup
  // failure (ProvisionErrored / ReplaceFailed). Simplified to -> Failed.
  action startupFailure = all {
    workerRunning, not(verified), not(cohort.subseteq(alive)),
    status' = Failed,
    cohort' = Set(),
    verified' = false,
    projection' = projection, alive' = alive, ledger' = ledger,
    dispatchQ' = dispatchQ, inbox' = inbox, nextId' = nextId,
    lastAction' = { act: "startupFailure", c: -1 },
  }

  // Success path pre-publish (lease_actor.go: provision worker and replace
  // worker both UpdateFn ContainerIDs before sendTerminal). Status unchanged.
  action prePublish = all {
    workerRunning, verified, projection != cohort,
    projection' = cohort,
    status' = status, cohort' = cohort, verified' = verified, alive' = alive,
    ledger' = ledger, dispatchQ' = dispatchQ, inbox' = inbox, nextId' = nextId,
    lastAction' = { act: "prePublish", c: -1 },
  }

  // The actor handles ProvisionCompleted / ReplaceCompleted -> Ready.
  // UpdateFn (leasesm_adapters.go) re-dispatches ledger deaths of the new
  // cohort only when wasProvisioning && isReady.
  action complete = all {
    workerRunning, verified, projection == cohort,
    val redispatch = status == Provisioning or
                     (REDISPATCH_ON_REPLACE and Set(Restarting, Updating).contains(status))
    val taken = if (redispatch) ledger.intersect(cohort) else Set()
    all {
      status' = Ready,
      cohort' = Set(),
      verified' = false,
      // takeLiveDeaths + dispatchLiveContainerDeath on a worker after the Ready
      // projection is visible; the Current gate then passes, so they reach the inbox
      inbox' = inbox.union(taken),
      ledger' = ledger.exclude(taken),
      projection' = projection, alive' = alive, dispatchQ' = dispatchQ, nextId' = nextId,
      lastAction' = if (redispatch) { act: "complete+redispatch", c: -1 } else { act: "complete", c: -1 },
    }
  }

  // ------------------------------------------------------------ environment

  // A container dies. The event loop's recorder writes the ledger first, then
  // forwards to the dispatcher (modelled as a separate step).
  action die = all {
    alive != Set(),
    nondet c = alive.oneOf()
    all {
      alive' = alive.exclude(Set(c)),
      ledger' = ledger.union(Set(c)),
      dispatchQ' = dispatchQ.union(Set(c)),
      status' = status, projection' = projection, cohort' = cohort, verified' = verified,
      inbox' = inbox, nextId' = nextId,
      lastAction' = { act: "die", c: c },
    }
  }

  // dispatchLiveContainerDeath + routeActorObservationDisposition:
  //   - not in any projection, or projection is Provisioning: return silently
  //     (relying on the ledger + Ready-entry redispatch)
  //   - otherwise route only if observation.Current: status Ready and the
  //     instance is in ContainerIDs (classifyReadyProjection); else refuse and
  //     count dieEventDroppedTotal ("the reconcile sweep will re-detect it")
  action dispatch = all {
    dispatchQ != Set(),
    nondet c = dispatchQ.oneOf()
    val found = projection.contains(c)
    val current = found and status == Ready
    all {
      dispatchQ' = dispatchQ.exclude(Set(c)),
      inbox' = if (current) inbox.union(Set(c)) else inbox,
      status' = status, projection' = projection, cohort' = cohort, verified' = verified,
      alive' = alive, ledger' = ledger, nextId' = nextId,
      lastAction' = if (current) { act: "dispatch->actor", c: c }
                    else if (not(found) or status == Provisioning) { act: "dispatch->ledgerOnly", c: c }
                    else { act: "dispatch->refused(dropped)", c: c },
    }
  }

  // ------------------------------------------------------------------ actor

  // Ready --ContainerDied(guard)--> Failing; ignored elsewhere.
  action handleDeath = all {
    inbox != Set(),
    nondet c = inbox.oneOf()
    val fires = status == Ready and projection.contains(c) and not(alive.contains(c))
    all {
      inbox' = inbox.exclude(Set(c)),
      status' = if (fires) Failing else status,
      projection' = projection, cohort' = cohort, verified' = verified, alive' = alive,
      ledger' = ledger, dispatchQ' = dispatchQ, nextId' = nextId,
      lastAction' = if (fires) { act: "handleDeath->Failing", c: c }
                    else { act: "handleDeath->ignored", c: c },
    }
  }

  // Failing --DiagGathered--> Failed
  action diagGathered = all {
    status == Failing,
    status' = Failed,
    projection' = projection, cohort' = cohort, verified' = verified, alive' = alive,
    ledger' = ledger, dispatchQ' = dispatchQ, inbox' = inbox, nextId' = nextId,
    lastAction' = { act: "diagGathered", c: -1 },
  }

  action step = any {
    requestProvision, requestRestart, requestUpdate, requestRestore,
    verify, startupFailure, prePublish, complete,
    die, dispatch, handleDeath, diagGathered,
  }

  // Same system without the replace machinery: only fresh provisions and
  // re-provisions. Used to confirm the model reproduces ENG-1125's fix.
  action provisionOnlyStep = any {
    requestProvision,
    verify, startupFailure, prePublish, complete,
    die, dispatch, handleDeath, diagGathered,
  }

  // ------------------------------------------------------------- properties

  // A Ready lease never holds a dead container that the actor will not hear
  // about: every dead container in the Ready projection is still in flight
  // (awaiting dispatch, or already in the actor inbox).
  val deadReadyContainerIsInFlight: bool =
    status == Ready implies projection.forall(c =>
      alive.contains(c) or dispatchQ.contains(c) or inbox.contains(c))

  // Witness used to check the model can reach a replace completion at all.
  val noReplaceEverCompletes: bool = lastAction.act != "complete"
}

module current {
  import deathRouting(REDISPATCH_ON_REPLACE = false).*
}

module fixed {
  import deathRouting(REDISPATCH_ON_REPLACE = true).*
}
