// Custom-domain handoff between two tenants on one docker-backend host.
//
// One domain d. Lease "A" starts out owning d on chain and serving it through
// Traefik; lease "B" belongs to another tenant on the same backend (so the same
// Traefik, the same public IP, the same ACME store).
//
// What is modeled, and where it comes from in Fred @ a802bca:
//   - Chain: CustomDomainIndex uniqueness among PENDING/ACTIVE leases. A clear
//     (tenant, authority or allowed_list) or a close (tenant, credit auto-close,
//     authority) frees d at once; any ACTIVE lease may then claim it.
//   - providerd: there is no event handler for custom-domain set/clear. Only the
//     periodic reconcile sweep (reconcile_plan.go: ACTIVE and not Failed) calls
//     ReconcileCustomDomain, lease by lease, in no fixed order.
//   - docker-backend ReconcileCustomDomain: acts only when the provision is
//     Ready; a set passes the DNS gate when d resolves to any IP (d already
//     points here, so it always passes); a clear is never gated. The redeploy is
//     a custom_domain maintenance through routeReplaceRestart.
//   - Failed maintenance: the captured source is recreated with its exact labels
//     (snapshotCompensationContainer clones Config.Labels), and "a verified
//     source returns the lease to Ready" (docker README). So a failed CLEAR puts
//     A's Host(d) router back.
//   - Close: event-driven deprovision; modeled as asynchronous.
//   - Traefik: every live container labeled Host(d) is a router for d. Router
//     names are per lease, so two leases give two routers with the same rule and
//     the same default priority; which one wins is not defined.
//   - ACME HTTP-01 is answered by this Traefik, so whoever routes d here gets a
//     certificate for it whenever d's DNS points here.
//
// Fix switches:
//   EXCLUSIVE           backend refuses to emit d (provision or redeploy) while
//                       another lease on this backend emits it or has a pending
//                       "set" for it; the reconciler retries next sweep.
//   STRIP_ON_COMPENSATE a failed clear compensates to the source WITHOUT the
//                       cleared custom-domain router.
//   VERIFY              ownership proof (ENG-62): only the tenant that controls
//                       d's DNS may claim d.
module handoff {
  const EXCLUSIVE: bool
  const STRIP_ON_COMPENSATE: bool
  const VERIFY: bool

  pure val LEASES = Set("A", "B")
  // Tenant A really owns d; its DNS points at this provider.
  pure val DNS_CONTROLLER = "A"

  // chain
  var owner: str            // "" | lease holding d in CustomDomainIndex
  var active: str -> bool   // lease is ACTIVE on chain
  // backend
  var live: str -> bool     // containers exist
  var ready: str -> bool    // ProvisionStatusReady
  var emitted: str -> bool  // live containers carry the Host(d) router labels
  var pending: str -> str   // "" | "set" | "clear" custom-domain redeploy in flight
  // monitors
  var clearSettled: str -> bool  // a clear redeploy for l finished since the last chain change
  var lastAct: { act: str, l: str }

  def routes = LEASES.filter(l => live.get(l) and emitted.get(l))

  def conflictFor(l) = LEASES.exists(m =>
    m != l and ((live.get(m) and emitted.get(m)) or pending.get(m) == "set"))
  def mayEmit(l) = not(EXCLUSIVE) or not(conflictFor(l))

  pure val NO_SETTLED = Map("A" -> false, "B" -> false)

  action keepChain = all { owner' = owner, active' = active }
  action keepBackend = all { live' = live, ready' = ready, emitted' = emitted, pending' = pending }

  action init = all {
    owner' = "A",
    active' = Map("A" -> true, "B" -> true),
    live' = Map("A" -> true, "B" -> false),
    ready' = Map("A" -> true, "B" -> false),
    emitted' = Map("A" -> true, "B" -> false),
    pending' = Map("A" -> "", "B" -> ""),
    clearSettled' = NO_SETTLED,
    lastAct' = { act: "init", l: "" },
  }

  // ---- chain ----

  // MsgSetItemCustomDomain with an empty domain (tenant, authority, allowed_list).
  action chainClear = all {
    owner != "",
    owner' = "",
    active' = active,
    keepBackend,
    clearSettled' = NO_SETTLED,
    lastAct' = { act: "chainClear", l: owner },
  }

  // Close by tenant, credit auto-close, or authority. Frees the index entry.
  action chainClose(l) = all {
    active.get(l),
    active' = active.set(l, false),
    owner' = if (owner == l) "" else owner,
    keepBackend,
    clearSettled' = NO_SETTLED,
    lastAct' = { act: "chainClose", l: l },
  }

  // MsgSetItemCustomDomain on an ACTIVE lease once d is free.
  action chainClaim(l) = all {
    active.get(l),
    owner == "",
    not(VERIFY) or l == DNS_CONTROLLER,
    owner' = l,
    active' = active,
    keepBackend,
    clearSettled' = NO_SETTLED,
    lastAct' = { act: "chainClaim", l: l },
  }

  // ---- backend ----

  // Initial provision (admitIngressPlan: DNS gate, which passes because d
  // already resolves here).
  action provision(l) = {
    val want = owner == l and mayEmit(l)
    all {
      active.get(l),
      not(live.get(l)),
      keepChain,
      live' = live.set(l, true),
      ready' = ready.set(l, true),
      emitted' = emitted.set(l, want),
      pending' = pending,
      clearSettled' = clearSettled,
      lastAct' = { act: "provision", l: l },
    }
  }

  // A tenant restart/update (or any other non-Ready spell) that keeps the
  // current containers and labels. ReconcileCustomDomain skips non-Ready.
  action maintStart(l) = all {
    live.get(l), ready.get(l), pending.get(l) == "",
    keepChain,
    live' = live, emitted' = emitted, pending' = pending,
    ready' = ready.set(l, false),
    clearSettled' = clearSettled,
    lastAct' = { act: "maintStart", l: l },
  }

  action maintEnd(l) = all {
    live.get(l), not(ready.get(l)), pending.get(l) == "",
    keepChain,
    live' = live, emitted' = emitted, pending' = pending,
    ready' = ready.set(l, true),
    clearSettled' = clearSettled,
    lastAct' = { act: "maintEnd", l: l },
  }

  // The reconcile sweep reaches lease l: planLease → ReconcileCustomDomain.
  action sweep(l) = {
    val want = owner == l
    all {
      active.get(l), live.get(l), ready.get(l), pending.get(l) == "",
      want != emitted.get(l),
      not(want) or mayEmit(l),
      keepChain,
      live' = live, emitted' = emitted,
      pending' = pending.set(l, if (want) "set" else "clear"),
      ready' = ready.set(l, false),
      clearSettled' = clearSettled,
      lastAct' = { act: if (want) "sweepSet" else "sweepClear", l: l },
    }
  }

  action redeployOk(l) = all {
    live.get(l), pending.get(l) != "",
    keepChain,
    live' = live,
    emitted' = emitted.set(l, pending.get(l) == "set"),
    pending' = pending.set(l, ""),
    ready' = ready.set(l, true),
    clearSettled' = if (pending.get(l) == "clear") clearSettled.set(l, true) else clearSettled,
    lastAct' = { act: "redeployOk", l: l },
  }

  // Startup verification of the target failed; the captured source is
  // recreated (labels included) and the lease returns to Ready.
  action redeployFail(l) = all {
    live.get(l), pending.get(l) != "",
    keepChain,
    live' = live,
    emitted' = if (pending.get(l) == "clear" and STRIP_ON_COMPENSATE) emitted.set(l, false) else emitted,
    pending' = pending.set(l, ""),
    ready' = ready.set(l, true),
    clearSettled' = if (pending.get(l) == "clear") clearSettled.set(l, true) else clearSettled,
    lastAct' = { act: "redeployFail", l: l },
  }

  // Close event → deprovision, asynchronously; preempts a pending redeploy.
  action deprovision(l) = all {
    not(active.get(l)), live.get(l),
    keepChain,
    live' = live.set(l, false),
    ready' = ready.set(l, false),
    emitted' = emitted.set(l, false),
    pending' = pending.set(l, ""),
    clearSettled' = clearSettled,
    lastAct' = { act: "deprovision", l: l },
  }

  action step = {
    nondet l = oneOf(LEASES)
    any {
      chainClear,
      chainClose(l),
      chainClaim(l),
      provision(l),
      maintStart(l),
      maintEnd(l),
      sweep(l),
      redeployOk(l),
      redeployFail(l),
      deprovision(l),
    }
  }

  // ---- properties ----

  // I1: d is routed to at most one lease.
  val exclusiveRoute = routes.size() <= 1

  // I2: once a clear redeploy has finished, the lease no longer routes d
  // (unless it owns d again).
  val settledClearRemovesRoute = LEASES.forall(l =>
    (clearSettled.get(l) and owner != l) implies not(live.get(l) and emitted.get(l)))

  // I3 (ENG-62): only the tenant that controls d's DNS is ever served on d.
  val onlyDnsControllerServes = routes.subseteq(Set(DNS_CONTROLLER))

  // Witness: a legitimate handoff to B completes (expect a "violation").
  val handoffNeverCompletes = not(owner == "B" and routes == Set("B"))
}

module current {
  import handoff(EXCLUSIVE = false, STRIP_ON_COMPENSATE = false, VERIFY = false).*
}

module stripOnly {
  import handoff(EXCLUSIVE = false, STRIP_ON_COMPENSATE = true, VERIFY = false).*
}

module exclusiveOnly {
  import handoff(EXCLUSIVE = true, STRIP_ON_COMPENSATE = false, VERIFY = false).*
}

module fixed {
  import handoff(EXCLUSIVE = true, STRIP_ON_COMPENSATE = true, VERIFY = false).*
}

module verified {
  import handoff(EXCLUSIVE = true, STRIP_ON_COMPENSATE = true, VERIFY = true).*
}
