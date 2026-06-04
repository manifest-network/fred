# ENG-193 — Type-enforced actor-sole-writer boundary for bootstrap provision writes — Design

**Ticket:** ENG-193 (child of the ENG-229 epic, "the lease actor is the sole writer of live `ProvisionState`"). Category **B** — bootstrap writes that cannot run on the actor because no actor exists yet.

**Status:** approved design (red-teamed against the codebase before writing).

## Goal

Make "the lease actor is the sole writer of a **published** `*provision`" hold **by construction** for the two bootstrap paths that populate `b.provisions` before any actor exists — recovery (`recover.go`) and creation (`provision.go`) — without changing observable behavior, and fix the two latent footguns the design review surfaced along the way.

## Honest framing (what this is and is not)

This is **structural hardening plus two real footgun fixes**, behavior-preserving — exactly as the epic framed it ("there is no live data race today — this is structural hardening, not a bug fix").

The deeper design review **could not substantiate** a reconcile-time "last-writer-wins clobber window" for `Ready` leases (an earlier hypothesis): a lease reaches `Status==Ready` only *after* the actor writes fresh `ContainerIDs` (`lease_sm.go:500-512`, `:544-555`), and restart/update hold `Restarting`/`Updating` for their whole duration — statuses recovery already preserves. Recovery's behavior is therefore kept **byte-equivalent to today's whole-map swap**; the change is the *construction mechanism*, not the semantics.

The genuine wins:
- **Type-enforced construction** of recovered provisions (the recovery code builds a value it cannot mutate after publish; the set of writers of a published `*provision` becomes small, named, and enumerable — the ticket's acceptance criterion).
- **Bug fix:** the `req.Items` aliasing footgun (`provision.go:199` publishes the caller's slice by reference; `NormalizeProvisionRequest` mutates `req.Items[0]` in place at `client.go:159`).
- **Latent-edge fix:** recovery has no explicit `Deprovisioning` case — today it implicitly resets such a lease to a container-derived status; we make it an explicit preserve-case.

## Category-B write-site inventory (the surface this ticket addresses)

Authoritative, current (line numbers drift; treat as anchors):

| Site | What it writes | Goroutine | Actor exists? |
|------|----------------|-----------|---------------|
| `provision.go:79` | reservation slot insert (`Status=Provisioning` marker + identity + `CallbackURL`) | HTTP handler, under `provisionsMu` | no (spawned at `:216`) |
| `provision.go:196-202` | in-place enrichment: `SKU`, `Items`, `StackManifest` | HTTP handler, under `provisionsMu` | no |
| `recover.go` build loop (~125-225) | whole `*provision` built in a function-local (unpublished) map | recover goroutine, under `recoverMu` | n/a (unpublished) |
| `recover.go` merge span (~251-340) | preserve-existing / FailCount anti-regression / `Failing→Failed` normalization (`:332` in-place on a live struct) | recover goroutine, under `provisionsMu` | reconcile path: yes for already-live leases |
| `recover.go:341` | whole-map swap `b.provisions = recovered` (the publish point) | recover goroutine, under `provisionsMu` | — |
| `recover.go:422-428` | post-publish `LastError = ErrMsgContainerExited + ": " + diag`, gated `Status==Failed` | recover goroutine, under a second `provisionsMu` span | cold-start: no |

**Explicitly out of scope** (different invariant — the ENG-229 *worker* exception class, documented, not touched here): the worker pre-publish writes `provision.go:577-581`, `restart_update.go:487-492`, `lease_actor.go` worker pre-publish; and the deprovision volume-retry block (tracked as ENG-285).

## Design

### 1. `recoveredProvision` — the off-map value builder

A new value type in the docker package, carrying **every** field needed to materialize a complete `*provision`:

```go
// recoveredProvision is a fully-built, NOT-YET-PUBLISHED provision snapshot.
// It is the typed payload the bootstrap paths construct off-map; it reaches
// b.provisions only via materialize() at the two publish points (§2). It has no
// method that mutates a map-resident *provision. Field-complete by construction:
// every ProvisionState field plus every *provision wrapper field appears here
// (enforced by test).
type recoveredProvision struct {
	state                 leasesm.ProvisionState // all 14 fields, by value
	volumeCleanupAttempts int                    // the only wrapper field today
}
```

`ProvisionState` fields (14): `LeaseUUID, Tenant, ProviderUUID, SKU, Status, Quantity, CreatedAt, FailCount, LastError, CallbackURL, Items, ContainerIDs, StackManifest, ServiceContainers`. Wrapper field (1): `VolumeCleanupAttempts`.

The recover build loop already assembles each entry in a function-local map (it allocates `Items`/`ContainerIDs`/`ServiceContainers` fresh, so it is alias-free); it now produces `recoveredProvision` values instead of `*provision`.

### 2. `materialize()` — the typed constructor + the two publish points

The structural guarantee rests on one typed constructor:

```go
// materialize is the ONLY function that turns recovery/creation data into a
// heap *provision. recoveredProvision has no other path into b.provisions, so
// the build/recovery code cannot publish a half-built pointer or hold a
// writable handle to a published *provision.
func (rec recoveredProvision) materialize() *provision
```

`recoveredProvision` is an off-map **value**; you cannot place it in `b.provisions` without `materialize()`. There are exactly **two** publish points — the only code that assigns into `b.provisions`:

1. **`provision.go` reservation** — `b.provisions[uuid] = rec.materialize()` (a `Provisioning`-marker value), under `provisionsMu`.
2. **`recover.go` install** — builds the replacement map (`recovered[uuid] = rec.materialize()` for rebuilt entries; `recovered[uuid] = existing` for preserve-statuses) and performs the existing atomic whole-map swap.

In-place field writers are **out of charter** and unchanged: `UpdateFn` (`leasesm_adapters.go:133`), the provision-path `enrichReserved` (§4), and all actor/SM/deprovision writes mutate `&p.ProvisionState` in place and never assign the slot.

> The map stays `map[string]*provision`. We do **not** introduce a distinct `pendingProvision` Go type as a map value — that would break the actor's in-place `UpdateFn`/`Get` model (`leasesm_adapters.go:115,126-135`). The "reservation marker" is a `*provision` in `Status==Provisioning`: a documented **state**, not a type.

### 3. `installRecovered` — recovery publish (behavior-preserving)

**Keeps the existing atomic whole-map swap** (the simplest byte-equivalent shape) and builds the replacement map from values under the **existing single `provisionsMu` span** — which already covers `pool.Reset` at `recover.go:357`, so there is no new lock-ordering or atomicity concern (`pool.Reset` never re-enters `provisionsMu`). Per-lease **status policy**:

- **Container-rebuilt entries** (e.g. `Ready` recovered from live containers): `recovered[uuid] = rec.materialize()` — a **freshly-materialized `*provision` from the complete value**, which is *byte-equivalent to today's fresh-`&provision{}` + swap*. A fresh struct clears for free exactly the fields the old swap cleared (`LastError = ""`, `VolumeCleanupAttempts = 0`), because `recoveredProvision` carries — and `materialize` sets — every field. This is why the value type must be field-complete: any field it omits would silently revert to zero on a rebuilt lease (a latent staleness bug, guarded by the exhaustiveness test).
- **Actor-owned in-flight statuses** — `Provisioning`, `Restarting`, `Updating`, `Failing` (normalized), `Failed`, and a **new explicit `Deprovisioning` case** — **preserve `existing` untouched** (`recovered[uuid] = existing`), exactly as today, keeping the existing defensive clones (`recover.go:396/446`) and the FailCount anti-regression.

> **Do not** rewrite this as per-slot *in-place* assignment onto live entries: a partial in-place merge would leak stale `LastError`/`VolumeCleanupAttempts`/`ServiceContainers`/`Items` (which `cleanupOrphanedVolumes` and reconcile then mis-compute against). Keeping the whole-map swap + materializing rebuilt entries from complete values is what makes the change provably behavior-preserving.

The `Failing→Failed` normalization currently done in place on a live struct (`recover.go:332`) is expressed as part of deciding the kept entry under the lock rather than a free-floating off-actor mutation of a published struct.

### 4. `provision.go` — reservation + in-place enrichment (kept) + aliasing fix

- The reservation at `:79` is published as `b.provisions[uuid] = rec.materialize()` (the marker `recoveredProvision`) but **must still set** `CallbackURL` and identity fields (`LeaseUUID, Tenant, ProviderUUID, Quantity, CreatedAt, FailCount`) — locked as an **invariant** so a failure/`Deprovision` racing a mid-validation provision in the `79→197` window resolves the real `CallbackURL` (`backend.go:661-662`, `deprovision.go:64`) and does not silently drop the callback. Only `SKU`/`Items`/`StackManifest` are legitimately deferred to enrichment.
- Enrichment at `:196-202` **stays an in-place field update** under the lock (converting it to a whole-struct install would discard fields a concurrent reconcile-time `recoverState` merged into the entry — a logical regression). It is expressed through a single controlled mutator so that "these three fields can only be set here, post-reservation" is structural:

```go
// enrichReserved sets the post-validation workload metadata on a reserved
// provision. The ONLY place SKU/Items/StackManifest are written outside the
// actor. Caller holds b.provisionsMu; the slot is a Provisioning reservation.
func (p *provision) enrichReserved(sku string, items []backend.LeaseItem, sm *manifest.StackManifest)
```

- **Aliasing fix:** `enrichReserved` **deep-copies** `Items` (`append([]backend.LeaseItem(nil), req.Items...)`) before storing it, defeating the `NormalizeProvisionRequest` in-place mutation of the caller slice (`client.go:159`). `StackManifest` is freshly parsed (`provision.go:152`) and `ContainerIDs`/`ServiceContainers` are nil at reservation, so no other copy is needed here.

### 5. `LastError` post-publish write (`recover.go:422-428`) — named exception, refined

Route-onto-actor is **structurally impossible** and the spec records *why*, so nobody "fixes" it later by pre-spawning actors: the write targets cold-start `Failed` leases (`recover.go:387-388`) which by construction have no prior in-memory state (`!hasExisting`, `recover.go:285`) and thus no live actor; a force-spawned actor seeds from store `Status=Failed` (`readProvisionStatus`, `lease_sm.go:879-885`), and the `Failed` state **`Ignores evContainerDied`** (`lease_sm.go:158`) — the message would be silently swallowed.

Resolution:
1. **Fold the baseline** `LastError = ErrMsgContainerExited` into the `recoveredProvision` value (it is already an in-lock write at `recover.go:287`, so it rides the install for free).
2. The **enriched `": " + diag` suffix** stays a documented post-publish field mutation (its diag comes from `InspectInstance`/`GatherDiagnostics` I/O done deliberately *outside* the lock, `recover.go:389-414` — it cannot be part of the pre-publish value).
3. **Cleanliness upgrade (in scope):** rewrite that suffix write to go through `ProvisionStore.UpdateFn` with the `Status==Failed` re-check *inside* the closure, replacing the raw `b.provisions[uuid]` access at `recover.go:424`. This removes the **last raw map access** from recovery and reuses the actor's own `LastError` mechanism; the existing re-check (already safe) is preserved.

### 6. Defensive-copy requirements (reference fields)

`ProvisionState` has four reference fields. The rules:
- **`Items`** — deep-copy at the provision path (`enrichReserved`); the recover build loop allocates it fresh (`recover.go:193/205`) and needs no copy.
- **`ContainerIDs`, `ServiceContainers`** — defensive clone whenever a `recoveredProvision` is derived from a *live/kept* entry (workers re-point these headers off-actor: `restart_update.go:489-490`, `lease_actor.go:714/842-844`); recovery already clones at `recover.go:396/446` — preserve those inside the install span. The build loop allocates them fresh; nil at provision reservation.
- **`StackManifest`** — pointer field, freshly produced on both paths (`provision.go:152`, `recover.go:151`); no copy needed, but note it is a pointer so future code does not assume value semantics.

## Testing

- **`recoveredProvision` field-exhaustiveness guard** — a test (or small reflection-based check) that fails if any `ProvisionState` or `*provision` wrapper field is absent from `recoveredProvision` or from `installRecovered`'s assignment list. Any omission is a latent staleness bug.
- **Behavior-preservation tests** — recovery of `Ready`-from-containers, in-flight (`Provisioning`/`Restarting`/`Updating`), `Failing`/`Failed`, and the new explicit `Deprovisioning` case all yield the same observable `b.provisions` state as before (same `Status`/`Items`/`ContainerIDs`/`FailCount`/`LastError`/`VolumeCleanupAttempts`), including the clear-on-rebuild of `LastError`/`VolumeCleanupAttempts`.
- **Aliasing regression test** — provisioning a lease then observing that a later in-place mutation of the *caller's* `req.Items` does not change the published `Items`.
- **Concurrent-reader tests** (a synchronous `-race` run proves nothing here — cf. ENG-278/ENG-266 lineage): (a) a reader/actor issuing `UpdateFn` writes racing recovery's install span; (b) a reader (`ListProvisions`/`GetInfo`/reconcile) racing a mid-validation provision in the `79→197` window.

## Acceptance criteria

- The only code that assigns into `b.provisions` is the two publish points (`provision.go` reservation insert + `recover.go` whole-map swap), and every newly-born `*provision` is obtained via `recoveredProvision.materialize()` — recovery/creation code never `&provision{…}`s directly.
- Recovery constructs `recoveredProvision` values; it holds no writable handle to a published `*provision`, and `recover.go` has **zero** raw `b.provisions[uuid]` field accesses left (the `:424` write goes through `UpdateFn`).
- The provision path sets the deferred metadata only through `enrichReserved`, which deep-copies `Items`.
- Recovery behavior is byte-equivalent to the prior swap for every status, plus an explicit `Deprovisioning` preserve-case.
- The surviving off-actor write exceptions (the enriched-`LastError` suffix; the worker pre-publish class) are explicitly named in code and documented as the *only* exceptions.
- All existing docker + leasesm tests stay green under `-race`; the new exhaustiveness, behavior-preservation, aliasing, and concurrent-reader tests pass.

## Non-goals / out of scope

- The lock-free `atomic.Pointer` read optimization (ENG-154).
- The worker pre-publish exception class and the deprovision volume-retry block (ENG-285) — different invariant, left as documented exceptions.
- Any change to recovery's *semantics* (status policy, FailCount rules, pool reset) — behavior is preserved.
