# ENG-376 — Reaping-tombstone fix for broken-store retained-disk under-count

**Ticket:** ENG-376 (Low; Backlog) — "Retained-disk accounting under-counts on a broken-store
reap/evict double-fault (invisible fred-retained-* orphan)."
**Related:** ENG-325 (soft-delete/restore), ENG-360 / PR #131 (retained-disk accounting).
**Date:** 2026-06-23

## Problem

The retained-disk admission projection is *derived-from-store*: `computeRetainedDiskMB`
sums `leaseDiskMB(items)` over **ACTIVE** retention records and `refreshRetentionAccounting`
pushes it to the admission pool + gauges. Several abandon paths delete (or fail to keep) a
record while the `fred-retained-*` (or leaked-canonical) bytes physically persist on disk.
The projection then **under-counts** a still-on-disk volume → admission **over-commits** →
a later tenant passes the disk gate and hits **physical ENOSPC** at write time. The leaked
volume is `fred-retained-*`-named, so `cleanupOrphanedVolumes` *intentionally* skips it
(`recover.go` `isRetainedVolume`) — it is invisible, uncounted, and never auto-reclaimed.

### Four abandon sites (same family)

1. **`reapExpiredRetentions`** (`restore.go:265-304`) — `ReapIfExpired` removes the record
   in-txn, `volumes.Destroy` fails, the compensating re-record `Put(e)` **also** fails
   (double fault) → trailing `refreshRetentionAccounting` re-derives without the record →
   under-count. Does **not** self-heal (the record is gone; reboot rebuilds from the store).
2. **`evictRetentionsToCap`** (`restore.go:201-258`) — identical shape via `DeleteIfActive`
   → Destroy fail → re-record `Put` fail → trailing refresh.
3. **`doDeprovision` give-up terminal** (`deprovision.go:352-401`) — after
   `maxVolumeCleanupAttempts` failures, `releaseLive()` runs unconditionally, the provision
   is deleted, "MANUAL CLEANUP REQUIRED" is logged, `return nil`. The leaked volumes (canonical
   for the non-retain arm; various sub-cases on the retain arm) are counted in **neither** live
   nor retained. Permanent, manual-only.
4. **`rollbackRestoreAdoption` revert-error** (`restore.go:741-751`) — on a `RevertToActive`
   store error/`!ok`, the record stays `restoring` (excluded from the projection) yet
   `releaseAll` still drops the live allocation while the re-quarantined `fred-retained-*`
   bytes sit on disk → F counted in neither → under-count. *Self-heals* within one
   `reconcileRestoring` sweep, so its window is bounded — unlike sites 1-3.

All sites are gated behind a degraded bbolt store (a `Put`/`RevertToActive` failure implies an
already-broken store), hence **Low** — but sites 1-3 do **not** self-heal, so a tracked fix.

## Why the ticket's "Option A addend" is not the idiomatic shape

The ticket proposes Option A: a separate `retention_leaked` bbolt bucket whose footprint is
*added* to the projection. That works but introduces a **second source of truth** that can
itself diverge, and a double-count edge on site 4's self-heal (addend **+** a reappearing
active record). Research into the canonical patterns for "metadata store and physical resource
diverge under partial failure" points elsewhere:

- **Kubernetes finalizer / deletion-timestamp:** *"the metadata record persists as a safety
  lock during external resource cleanup"* — the external resource is deleted **first**, and the
  record is removed **only after** cleanup is confirmed; while pending, the object stays
  (terminating) and cleanup is retried idempotently.
- **Compensating-transaction / saga:** record progress so an interrupted step can **resume**;
  every step idempotent; retry transient failures; the record outlives the resource.
- **Make-before-break handoff** (for the live→retained ownership transfer in site 4): establish
  the new owner before releasing the old — *overlap, never gap*; if the new owner's commit
  fails, the old still holds, so there is no uncounted window.

fred's bug is precisely the **inverted ordering**: it deletes the record in-txn, *then*
destroys the volume; the existing `Put(e)` re-record-on-failure is a poor-man's finalizer that
can itself fail. And fred **already uses make-before-break internally** — the deprovision
retain hand-off comment says *"release live AFTER the retained projection is refreshed,
ensuring overlap, never a gap,"* and the rollback **success** arm refreshes-before-releasing.
Site 4 is just the **failure** arm breaking that established idiom.

## Design: reaping tombstone + make-before-break handoff

One unifying principle: **never delete a retention record before its volumes are confirmed
destroyed; never release a live allocation before the destination record is durably confirmed.**
The record/allocation is the accounting safety-lock until physical state catches up. No second
ledger — the existing record (with a new status) is the single source of truth.

### A. Data model (`internal/backend/shared/retention.go`)

- New status `RetentionStatusReaping = "reaping"`. A reaping record is a **tombstone**: its
  volumes are pending destruction, the bytes are still on disk, and it is **not**
  restore-claimable. `ClaimForRestore` and `PutActiveMerged` already reject non-`active`
  records, so the restore race stays closed with **zero new guard code**.
- New store methods:
  - `MarkReapingIfExpired(orig, maxAge) (names []string, ok bool, err error)` — atomic
    `active`→`reaping` for an expired record; returns the volume names. Replaces the
    delete-then-re-record dance of `ReapIfExpired`.
  - `MarkReapingIfActive(orig) (names []string, ok bool, err error)` — atomic
    `active`→`reaping`; replaces `DeleteIfActive` for cap-eviction.
  - `ListReaping() ([]RetentionEntry, error)` — for the retry sweep.
  - `PutReaping(entry) (ok bool, err error)` — create a fresh reaping tombstone **only if** no
    existing `active`/`restoring` record covers the lease (idempotent; never clobbers, never
    double-counts). For site 3's no-record sub-cases. `RetainedVolumeNames` may hold canonical
    names here (documented; the field means "volume names this record is responsible for
    destroying").
- **Remove** the now-unused `ReapIfExpired` / `DeleteIfActive` (single production caller each;
  store-test assertions migrate to the mark-reaping methods).

### B. Accounting + metrics

- `computeRetainedDiskMB` counts `active` **+** `reaping` (the bytes occupy disk → never
  under-counts a still-on-disk volume). The `retained_leases` **count** gauge stays
  `active`-only (reaping = being removed, not "held/restorable").
- `retained_volume_bytes` now includes reaping footprints — documented; it stays equal to the
  admission `SetRetainedDisk` value (the projection and the gauge remain identical).
- New metrics (`metrics.go`):
  - `fred_docker_backend_retention_leaked_total` (counter) — incremented on every give-up /
    stuck-reap leak-log path. Satisfies AC #2 ("leak exported as a metric, not only logged");
    always works even when the store is too broken to take the tombstone write.
  - `fred_docker_backend_retention_reaping_bytes` (gauge) — outstanding pending-cleanup
    footprint (reaping subset), set during refresh. Alertable: a non-zero, non-decreasing value
    means a leak the sweep cannot reclaim → operator action.

### C. The four sites

1. **reap** — `MarkReapingIfExpired` → `volumes.Destroy` → `Delete` **only on confirmed
   destroy**; on Destroy failure, leave the record `reaping` (no `Put` re-record at all). The
   whole double-fault disappears: there is no second store-write to fail, and the record is
   never absent while bytes persist.
2. **evict** — `MarkReapingIfActive` (removes it from the active count-cap set → room is made)
   → `Destroy` → `Delete` on success; leave `reaping` on failure.
3. **deprovision give-up** — if `retentionStore != nil`: enumerate the lease's still-on-disk
   volumes (`volumes.List`, canonical + retained prefix; fall back to deriving canonical names
   from items on List error), `PutReaping` a tombstone for them (idempotent; skips if an active
   record already counts F) → `releaseLive()` (unchanged) → the deferred `refreshRetentionAccounting`
   counts the tombstone. The footprint hands off live→reaping with no gap, and the sweep now
   **auto-retries** the destroy — turning today's permanent manual-only leak into a self-healing
   one. When `retentionStore == nil`: `retention_leaked_total`++ + the existing log (unchanged
   manual path; no projection exists to correct).
4. **rollbackRestoreAdoption revert-error** (make-before-break) — on `RevertToActive`
   err/`!ok`, **do NOT `releaseAll`**: keep the live allocation counted (F stays counted as
   *live* — no under-count window), increment `retention_leaked_total`, and still
   `removeProvision` for `dropProvision=true`. `reconcileRestoring`'s orphaned arm then resumes
   the revert on the next sweep and releases the **same** `liveIDs` (`{NewLeaseUUID}-{svc}-{idx}`,
   identical to `allocatedIDs`) once it commits. This avoids the permanent-leak regression noted
   in the ticket's comment #4 — that regression was the *keep-the-provision* variant; removing
   the provision routes reconcile to the orphaned arm, which reclaims the live allocation. **No
   addend bucket needed anywhere.**

### D. Reconcile / sweep (`restore.go`, `recover.go`)

- New `retryReapingRecords(ctx)`: `ListReaping` → re-`Destroy` each record's volume names
  (idempotent; already-gone names no-op) → `Delete` the record when **all** destroys succeed;
  leave `reaping` + `retention_leaked_total`++ otherwise.
- Wire it into both the periodic `runRetentionSweep` and the boot `reconcileRetentions`.
- Fail-closed: on a store/`List` error, keep the records (keep counting). A reaping record's
  canonical volume is **not** added to `cleanupOrphanedVolumes`' protected set (we *want* it
  reaped); `fred-retained-*` reaping volumes are already skipped by `isRetainedVolume` and are
  handled by `retryReapingRecords`.

## Concurrency & safety review

- **Restore race stays closed.** A `reaping` record is non-`active`, so `ClaimForRestore`
  (active-only) and `PutActiveMerged` (active-only) reject it — same protection the old in-txn
  delete gave, now without deleting.
- **Mark-reaping failure is safe.** If `MarkReaping*` itself fails (broken store), the record
  stays `active` → still counted → no under-count. There is no longer any store path that drops
  a record while bytes persist (sites 1-2).
- **No double-count.** Single source of truth (the record's status). `PutReaping` refuses when
  an active/restoring record already covers the lease.
- **Site 4 reclaim is real.** `reconcileRestoring` orphaned arm releases `liveIDs` derived as
  `{NewLeaseUUID}-{svc}-{idx}` after a successful `RevertToActive` — exactly the allocation the
  rollback left counted; `releaseAll` is idempotent so a later deprovision/recover is harmless.
- **Idempotent destroy** is relied upon throughout (existing contract: already-destroyed names
  no-op).

## Testing (TDD)

- **AC #1 (store, docker):** a reap where `DestroyFn` errors leaves the record `reaping` and
  `computeRetainedDiskMB` **still counts F** (stronger than the old double-fault: the
  compensating `Put` is gone, so there is no second failure point). Replaces
  `restore_test.go:504` / `:1508` re-record assertions.
- **evict:** `MarkReapingIfActive` removes it from the active cap set; a destroy failure leaves
  it `reaping` and counted.
- **give-up:** a tombstone is written for leaked volumes, F stays counted after `releaseLive`,
  and a later sweep with a now-succeeding `Destroy` reclaims + deletes the record.
- **site 4:** a `RevertToActive` error leaves the live allocation counted (projection ≠
  under-count); a subsequent `reconcileRestoring` releases it after a successful revert.
- **metrics:** `retention_leaked_total` increments on each leak path; `retention_reaping_bytes`
  reflects the outstanding reaping footprint.
- **store-method unit tests** for `MarkReaping*` / `ListReaping` / `PutReaping` (CAS, guards,
  idempotency), mirroring the existing `RevertToActive`/`ReapIfExpired` tests.
- Run with `-race -short` (fred convention).

## Docs

- **OPERATIONS.md** — new subsection under "Reclaiming retained volumes under disk pressure":
  "Reclaiming leaked / stuck-reaping orphan volumes" — keyed off `retention_leaked_total` /
  `retention_reaping_bytes`, explaining that `fred-retained-*` (and leaked-canonical) orphans
  are deliberately skipped by `cleanupOrphanedVolumes`, that the sweep now auto-retries reaping
  records, and the manual `docker volume`/`rm -rf <data-dir>` reclaim procedure for a volume the
  sweep cannot destroy (with the caution to confirm no live/restoring record references it).
- **Linear ENG-376** — comment that reaping-tombstone + make-before-break supersedes Option A
  (single source of truth; no addend bucket; site 4 fixed via the codebase's own make-before-break
  idiom rather than a side-ledger).

## Out of scope

- A per-tenant retained-disk quota (noted in OPERATIONS as a separate follow-up).
- Any change to the chain interface or the soft-delete/restore happy path.
