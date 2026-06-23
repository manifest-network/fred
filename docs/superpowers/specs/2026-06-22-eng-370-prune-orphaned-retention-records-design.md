# ENG-370 — Prune orphaned retention records when their volumes vanish out-of-band

**Status:** Design (awaiting approval)
**Date:** 2026-06-22
**Branch:** `felix/eng-370-prune-orphaned-retention-records`
**Component:** `internal/backend/docker` (retention store reconciliation)

## Problem

A retained-lease **record** in the docker backend's `retention.db` is removed only by
restore-consume, the 90-day grace reaper, or per-tenant cap eviction. None of these fires
when the backing **volume** disappears out-of-band (host/docker churn, `docker volume prune`,
a `/data/docker` reset on redeploy while `/data/fred/retention.db` survives). Orphaned records
then persist for the full grace window (default 90 days), accumulate (≈13.9k observed on dev,
8 MB `retention.db` per backend with **zero** backing volumes), and inflate the O(N) cross-backend
retention inventory polled by providerd.

## Verified scope correction (vs the ticket)

The ticket proposed three fixes. Investigation of the current tree (HEAD `4ba85ce`) and the
dev commit (`2ffc02d`) changes the scope:

1. **Claim #2 ("writes a record even with zero managed volumes") is ALREADY satisfied.**
   `doDeprovision` writes the retention record only inside `if len(canonical) > 0`
   (`deprovision.go:166`), where `canonical` is the lease's *actual* on-disk managed volumes.
   This guard was introduced in the ENG-325 PR (`ef03aa3`); the dev version `2ffc02d`
   descends from it and **already had the guard**. A tmpfs / non-`VOLUME` lease therefore
   produces **no** record. `PutActiveMerged` is the only record-creation path. **No code change
   is needed for claim #2.** (The reconcile below incidentally cleans up any *legacy* zero-volume
   records that may predate the guard, since they trivially have all-absent volumes.)

2. **Claim #3 (admin purge) is deferred (YAGNI).** See "Decisions backed by research".

So the substance of this work is **claim #1**: reconcile records against volume existence.

## Decisions backed by research

The user asked for the idiomatic solution. A multi-source web study (Kubernetes GC,
controller-runtime, kubelet, external-dns, Terraform, Envoy/ELB/Prometheus health idioms,
Crossplane, SRE toil guidance), adversarially verified (high confidence) and then independently
re-reviewed against best practices, converged on a **layered, fail-closed** design.

The "delete state because a LIST returned transiently empty" failure mode is documented real data
loss — external-dns #2858 (API timeout → empty source → deleted all DNS records) and kubelet
#72257 (missing volumes dir at startup treated as empty → wiped still-mounted data). Our hazard is
identical: `listVolumeIDs` returns `(nil, nil)` — empty, **no error** — when the volume root is
missing (`fs.ErrNotExist`); a naive "all volumes absent → delete record" during a transient
unmount / boot-before-mount would prune every legitimately-retained record.

**Mechanism of harm (corrected):** the volume itself is *not* physically destroyed — the inverse
`cleanupOrphanedVolumes` deliberately **skips** `isRetainedVolume` (`recover.go:607`). The harm is
that discarding the record throws away the **only restore handle**: once the root remounts, the
still-present `fred-retained-*` volume is **un-restorable** (`Restore` → `ErrNotRetained`),
**un-reapable** (no record for the reaper/evictor to act on), and **leaked on disk forever** —
effective data loss for the tenant. Either way the fail-safe direction is identical: **never prune
on uncertainty.** (Note: "over-refuse is safe" inverts when refuse=destroy; here the gated action
is *deletion*, so refuse-to-prune = preserve = safe.)

**Adopted guards** (every layer defaults to inaction):

- **G1 — Hard-skip on enumeration error.** `b.volumes.List()` error ⇒ skip the whole pass.
- **G2 — Warm-view gate.** If `VolumeDataPath` is configured but **absent or unreadable** on disk,
  skip the whole pass. Probed via the house `pathExists` helper (`volume.go`), which distinguishes
  present / absent / stat-error — deliberately *not* an `os.IsNotExist`-only check, because an
  unreadable root (permission denied, EIO) is as uncertain as a missing one (kubelet #72257 fix:
  a missing-or-unreadable root ≠ "no volumes").
- **G3 — In-memory consecutive confirmation.** A record must be observed fully-orphaned on
  ≥ N consecutive periodic sweeps before deletion (default N=3, the K8s probe `failureThreshold`
  idiom). Any reappearance resets its streak. State is **in-memory by design**: a process restart
  resets all streaks, so a freshly-booted backend can **never** prune on its first sweep
  (defeats the boot-before-mount race). G1/G2 skips also reset streaks.
- **G4 — ACTIVE-only CAS delete.** Prune only via the store's existing `DeleteIfActive`, so a
  concurrent restore (active→restoring) is never clobbered.
- **Confirmable-orphaned only.** A record is eligible only when its volume absence is *positively
  verifiable*: all `RetainedVolumeNames` absent from a trusted `List()` **and** either the root is
  configured + G2-healthy, or the record has no volume names at all (a legacy zero-volume record —
  nothing to verify). A non-empty name set with **no** configured root (noop manager) is
  unverifiable and is **never** streaked or pruned.

**Deliberate divergences from the generic synthesis** (fred specifics — confirmed correct by the
independent review; do not naively "correct" them):

- **Do NOT AND with the 90-day grace window.** That grace exists to let *users restore*; an
  orphaned record has no data left to restore, so gating on it would re-introduce the exact
  90-day delay ENG-370 fixes. G3's N-sweep confirmation *is* the orphan path's (short) grace.
  Every industry soft-delete cooldown (GCS/Azure/ACR, Key Vault purge-protection) protects
  *recoverable* data; the object pruned here is metadata pointing at already-gone data.
- **No blast-radius refuse-cap.** The dev backlog is 14k *legitimately* orphaned records; a
  "refuse if too many look orphaned" cap would block the cleanup we want. G3 already neutralizes
  transient mass false-positives (a blip can't persist N sweeps).
- **No soft (non-blocking) per-pass deletion cap either.** A rate-limit (max-deletes-per-sweep,
  drain over many sweeps) is the idiomatic alternative to a refuse-cap, but it's also unnecessary:
  the deleted object is an already-dead bbolt record whose volume is already gone (no availability
  hazard, unlike a K8s eviction), each delete is an individual CAS txn on a small local file (no
  giant-txn / write-amp concern), and 1h cadence × N-sweep confirmation × G1/G2 hard-skip already
  bound any runaway. Deferred (YAGNI).
- **No chain dimension.** "Are this record's volumes on this node" is answered purely locally;
  the chain lease list is irrelevant here.

## Design

### New reconcile function (`restore.go`)

```go
// reconcileOrphanedRetentions prunes ACTIVE retention records whose every
// RetainedVolumeName has been absent from the node for >= N consecutive sweeps
// (ENG-370). Fail-safe: any uncertainty (list error, missing/unreadable volume
// root) skips the whole pass and resets confirmation streaks. Returns the count
// pruned. No ctx: the prune does no context-bound IO (volumes are already gone,
// nothing to Destroy), unlike reapExpiredRetentions.
func (b *Backend) reconcileOrphanedRetentions() (int, error)
```

Algorithm:
1. No-op if `retentionStore == nil`. If `RetentionOrphanConfirmations <= 0` (disabled / kill-switch):
   emit one info log (so a flip-to-0 isn't silent), bump `…_skipped_total{reason="disabled"}`, return.
2. **G2:** if `VolumeDataPath != ""`, probe it with `pathExists` (three-way: present→`(true,nil)`,
   absent→`(false,nil)`, other stat error→`(false,err)`). On a stat **error** *or* absent root →
   log, reset all streaks, bump `…_skipped_total{reason="root_unverifiable"}`, return. (When
   `VolumeDataPath == ""` the manager is noop — handled by the per-record verifiability rule below.)
3. **G1:** `existing, err := b.volumes.List()`; on error → log, reset streaks, bump
   `…_skipped_total{reason="list_error"}`, return err. Build `present` set from `existing` (retained
   volumes carry the `fred-` prefix, so a present retained volume appears here).
4. Read `recs` from `retentionStore.List()`; on a store error → reset streaks, bump
   `…_skipped_total{reason="store_error"}`, return err (same fail-safe shape as G1). For each record:
   - Skip non-`ACTIVE` (restoring records have had volumes renamed away — would look "absent").
   - **Confirmable-orphaned** iff all `RetainedVolumeNames` are absent from `present` **and**
     (`VolumeDataPath != ""` *or* `len(RetainedVolumeNames) == 0`). If not confirmable → drop its
     streak (do not carry).
   - Else `streak = prev + 1`. If `streak >= N`: `DeleteIfActive` (volumes already gone — nothing
     to destroy); on `deleted` → count + `…_pruned_total` + log. If `!deleted` (record no longer
     ACTIVE-and-present — restore-claimed OR already removed, e.g. cap-eviction)
     → drop streak + bump `…_skipped_total{reason="raced"}`. On store error → keep streak,
     retry next sweep. If `streak < N`: carry it forward.
5. Replace `b.orphanStreaks` with the freshly-rebuilt map (so vanished records don't leak memory).

Empty `RetainedVolumeNames` ⇒ vacuously all-absent ⇒ pruned after N sweeps (cleans legacy
zero-volume records, independent of root state). Mount state cannot affect them, but uniform
N-sweep treatment is simpler and the extra delay is harmless.

### State (`backend.go`)

```go
// orphanStreaks counts consecutive retention sweeps an ACTIVE record's volumes
// were all absent (ENG-370). Two invariants protect it:
//   1. Single-writer confinement: touched ONLY by reconcileOrphanedRetentions,
//      reachable only via runRetentionSweep on the single StartCleanupLoop
//      goroutine (boot-eager retention work runs before that goroutine starts and
//      never touches it) — so no mutex is needed. Do not add a second writer.
//   2. In-memory by design: a restart resets it so a cold boot can never prune on
//      its first sweep (the boot-before-mount fail-safe). Do not persist it.
// Separately, the prune itself relies on DeleteIfActive's in-txn CAS as the
// load-bearing guard against a concurrent restore (ClaimForRestore active→restoring
// on a request goroutine) — do not "simplify" it into an unconditional Delete.
orphanStreaks map[string]int
```

Initialized to `map[string]int{}` in `newBackend` (alongside `retentionStore`).

### Wiring (`restore.go`)

`runRetentionSweep` (periodic only) gains a final step after the restoring-reconcile:
```go
if _, err := b.reconcileOrphanedRetentions(); err != nil {
    return err
}
```
**Not** added to the boot path (`Start`): boot is exactly when mounts may be cold. First periodic
tick is after `interval` (default 1h, no immediate run), and G3 needs N more sweeps — so the first
possible prune is ~N hours post-boot, after mounts warm. Rides the existing
`retentionSweepInterval()` enable gate (default-on since `RetentionMaxAge=90d>0`).

### Config (`config.go`)

```go
// RetentionOrphanConfirmations is the number of consecutive retention sweeps a
// soft-deleted record must be observed with ALL its retained volumes missing
// before the record is pruned (ENG-370). It is a SWEEP COUNT, not a duration:
// the effective confirmation window is N × RetentionReapInterval (≈3h at the
// default 1h interval), so shortening RetentionReapInterval proportionally
// shrinks the window — re-tune N to keep a fixed grace. 0 disables orphan
// pruning entirely (kill-switch). Defaults to 3.
RetentionOrphanConfirmations int `yaml:"retention_orphan_confirmations"`
```
Default `3`; validation rejects negative values (mirrors the other retention knobs).

### Metrics (`metrics.go`)

```go
// retentionOrphansPrunedTotal counts retention records pruned because all their
// retained volumes were confirmed absent (ENG-370).
retentionOrphansPrunedTotal = promauto.NewCounter(prometheus.CounterOpts{
    Namespace: metricsNamespace, Subsystem: metricsSubsystem,
    Name: "retention_orphans_pruned_total",
    Help: "Total retention records pruned due to confirmed-absent backing volumes",
})

// retentionOrphanSkipsTotal counts orphan-reconcile skips by reason at TWO
// granularities: whole-sweep bailouts (list_error/root_unverifiable/store_error/
// disabled — one per skipped sweep) AND per-record prune attempts skipped (raced —
// one per record). Filter by reason; don't sum across (units differ). The name says
// "skips" not "sweeps" deliberately. Without this, a sweep that G2-skips forever
// (e.g. a mis-mounted root) is indistinguishable from a healthy "0 pruned" on the
// success counter alone. reason ∈ {list_error, root_unverifiable, raced, disabled,
// store_error}. Mirrors the existing {op,reason} label convention (idempotent_ops_total).
retentionOrphanSkipsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
    Namespace: metricsNamespace, Subsystem: metricsSubsystem,
    Name: "retention_orphan_skips_total",
    Help: "Orphan-reconcile skips by reason (sweep-level bailouts + per-record raced prune attempts)",
}, []string{"reason"})
```

No second boolean flag (the `N=0` early-return is the kill-switch) and no candidates/streaks gauge
(an in-memory streak gauge would zero-flap on every restart/skip and mislead). A last-success
timestamp belongs to the `runRetentionSweep` wrapper, which has none today — out of scope here.

## Concurrency & safety

- `orphanStreaks` is touched only by the periodic sweep goroutine; the boot-eager reaper runs
  before that goroutine starts and never touches it. Single-writer ⇒ no lock.
- Deletion uses `DeleteIfActive` (existing CAS): a restore that claims the record between our
  snapshot and the delete returns `deleted=false`, so a mid-restore record is never removed. Same
  primitive `evictRetentionsToCap` already relies on (DRY).
- Retained-volume names can't reappear while a record is ACTIVE (only soft-delete creates them,
  and the lease is already closed; restore renames them *away*), so when we prune there is nothing
  to `Destroy`. `cleanupOrphanedVolumes` deliberately **skips** `isRetainedVolume` (`recover.go:607`),
  so it is *not* a backstop for retained volumes — but none is needed, because a stray retained
  volume for an already-closed lease cannot arise.

## Testing (TDD)

White-box tests in `internal/backend/docker` with a fake `volumeManager` (controllable `List` and
data path) and a real bbolt `RetentionStore` (temp file):

1. All volumes absent: not pruned at sweep < N; pruned exactly at sweep N; `…_pruned_total` += 1.
2. A present volume → never pruned; streak stays cleared.
3. Volume reappears mid-streak → streak resets; not pruned.
4. `restoring` record with absent volumes → never pruned (status guard).
5. Missing `VolumeDataPath` root (G2, `pathExists`→`(false,nil)`) → never prunes across many
   sweeps; streaks reset; `…_skipped_total{reason="root_unverifiable"}` increments.
6. **Unreadable root** (G2, `pathExists`→`(false, err)`, non-ENOENT) → also skips (pins the
   non-`IsNotExist` branch, not just the missing-path branch).
7. `List()` error (G1) → never prunes; streaks reset; `…_skipped_total{reason="list_error"}`; error surfaced.
8. **Genuinely concurrent restore race:** drive a real `Restore`/`ClaimForRestore` (active→restoring)
   concurrently with `reconcileOrphanedRetentions` (run under `-race`) and assert the record
   survives (`DeleteIfActive` `deleted=false` honored). A hand-faked `deleted=false` under a
   synchronous test would not exercise the only concurrent path that matters.
9. Empty `RetainedVolumeNames` record → pruned after N sweeps (root state irrelevant).
10. Volume-bearing record with `VolumeDataPath == ""` (noop) → **never** pruned (unverifiable).
11. `RetentionOrphanConfirmations=0` → never prunes (kill-switch); info log + `…_skipped_total{reason="disabled"}`.
12. Mixed set (many absent + a few present) → only absent pruned after N.
13. Config default = 3; negative rejected by `Validate`.

## Notes & rejected alternatives

- **noop manager / `VolumeDataPath` unset:** `List()` is always `(nil, nil)`, so G2 is bypassed.
  Under a no-volume backend every record *looks* absent — but the per-record verifiability rule
  prunes only legacy/empty-name records (current `doDeprovision` writes records only when
  `len(canonical) > 0`, so a no-volume backend produces no *new* volume-bearing records). This is
  the intended legacy cleanup, not a bug; volume-bearing records under an unset root are skipped.
- **Event-driven on volume-delete (rejected):** fred cannot observe out-of-band volume deletions
  (host churn / `docker volume prune`) as events — docker's event stream is a non-durable
  in-memory ring (~256 events) — so a polling level-based reconcile is inherent.
- **Finalizer + ownerRef cascade (rejected):** no API-owned `deletionTimestamp` hook exists for an
  out-of-band volume deletion; inapplicable.
- **Persisted two-phase mark / tombstone (rejected):** strictly worse — re-introduces the cold-boot
  prune hazard that G3's in-memory, restart-resetting streak exists to block.

## Non-goals

- Admin/CLI purge endpoint (deferred; if a stuck state is later observed, add a thin command over
  this same idempotent path).
- Changing the 90-day grace reaper, cap eviction, restore, or the inverse volume cleanup.
- Any change to record creation (claim #2 already satisfied).
- A streaks/candidates gauge or last-success-timestamp metric (scope creep; see Metrics).

## Key references (citations corrected per adversarial review)

- Empty-list-deletes-everything (real incidents): external-dns #2858; kubelet #72257.
- Fail-safe-on-uncertainty: Kubernetes GC architecture page; Wikipedia "Fail-safe".
- Consecutive-confirmation hysteresis: K8s probe `failureThreshold` (default 3); Envoy outlier
  detection; AWS ELB `UnhealthyThresholdCount`; Prometheus `for`.
- Warm-view / don't-act-on-cold-state: controller-runtime `WaitForCacheSync`; K8s GC
  `absentOwnerCache`; kubelet ordering PR #44781.
- Auto-GC default, manual purge deferred: K8s GC architecture page; Force Delete StatefulSet Pods;
  SRE toil guidance; Crossplane managed-resources (stops only on undeterminable result).
- Metric reason-label convention: existing `idempotent_ops_total{op,reason}` in `metrics.go`.
