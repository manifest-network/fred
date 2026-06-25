# ENG-376 Reaping-Tombstone Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the retained-disk admission projection from under-counting still-on-disk
`fred-retained-*` (and leaked-canonical) volumes when a reap/evict/deprovision/rollback path
abandons a record under a degraded bbolt store, by making the retention record a finalizer
tombstone (`reaping`) that outlives the volume until destroy is confirmed.

**Architecture:** Idiomatic finalizer ordering (record outlives the resource) for sites 1-3 via
a new `reaping` status (`active`→`reaping`→delete-on-confirmed-destroy, counted the whole time);
make-before-break handoff for site 4 (keep the live allocation counted until `RevertToActive`
durably commits, letting `reconcileRestoring` resume and reclaim). One source of truth (the
record + its status) — no separate ledger. Plus a `retention_leaked_total` counter,
`retention_reaping_bytes`/`_leases` gauges, and an OPERATIONS runbook.

**Tech Stack:** Go, bbolt (`go.etcd.io/bbolt`), Prometheus (`promauto`), testify. Tests run with
`go test -race -short` (fred convention: the unguarded stress tests OOM the runner under `-race`).

**Spec:** `docs/superpowers/specs/2026-06-23-eng-376-reaping-tombstone-design.md`

---

## File map

| File | Responsibility | Change |
| --- | --- | --- |
| `internal/backend/shared/retention.go` | bbolt retention store + entry model | add `reaping` status + `ReapingSince`; add `MarkReapingIfActive`, `MarkReapingIfExpired`, `ListReaping`, `PutReaping`; remove `ReapIfExpired` (DeleteIfActive RETAINED — ENG-370's `reconcileOrphanedRetentions` uses it; only the eviction use is replaced) |
| `internal/backend/shared/retention_test.go` | store unit tests | tests for new methods; remove old-method tests |
| `internal/backend/docker/metrics.go` | metric vars + setters | add `retentionLeakedTotal`, `retentionReapingBytes`, `retentionReapingLeases`; extend `updateRetentionMetrics` |
| `internal/backend/docker/retention_accounting.go` | projection compute + refresh | add `computeReapingDiskMB`; refresh sets pool to active+reaping |
| `internal/backend/docker/restore.go` | reap/evict/sweep/reconcile/rollback | sites 1, 2, 4; `destroyReapingVolumes`, `retryReapingRecords`; boot reaping arm |
| `internal/backend/docker/deprovision.go` | lease teardown | site 3 give-up tombstone (`recordGiveUpLeak`) |
| `internal/backend/docker/restore_test.go` | docker-level tests | reaping behavior; replace re-record tests |
| `internal/backend/docker/retention_accounting_test.go` | accounting tests | reaping counted in pool, not in cap |
| `internal/backend/docker/deprovision_test.go` | give-up tests | tombstone written + auto-retry |
| `OPERATIONS.md` | runbook | reclaim section + alert row |

---

## Migration & rollback compatibility

The `reaping` status is an **additive** schema change to the bbolt retention records (a new
`Status` string value + an optional `ReapingSince` field). No data-migration step and no feature
flag are needed (the happy paths are behavior-preserving — only the failure/abandon arms change):

- **New binary, old records:** fully backward-compatible — every pre-existing `active`/`restoring`
  record reads and behaves exactly as before.
- **Old binary, new records (rollback during a fault window):** a `reaping` record is only ever
  created under a degraded-store fault. An older binary has no `reaping` case, so its status
  switches fall through to their default (**ignore**) — it will not destroy, restore, or reap the
  record. The footprint is merely uncounted (the pre-existing under-count this ticket fixes) and
  is reclaimed on re-upgrade. No data loss and no destructive mishandling: the unknown-status
  default is the *inert/safe* direction (the canonical "map unknown enum → no-op" forward-compat
  guard). This is the same safe-direction reasoning as the destroy-gate analysis in the spec.

## Testing conventions

- Run every task's tests with `go test -race -short` (fred convention: unguarded stress tests
  OOM the runner under `-race`). **NEVER run `-race` without `-short`** — the stress tests
  (e.g. the 100K-event manager tests) exit 137 (OOM-kill) under the race detector. Scoped
  `-run <SpecificTest>` invocations may omit `-race` for speed, but any `-race` run MUST add
  `-short`.
- **Do NOT add `t.Parallel()`** to tests that assert on the global `retention_*` Prometheus
  metrics — they share process-global state and would flake. Counter assertions use a
  before/after delta (`leakBefore := testutil.ToFloat64(...)`) so they stay order-independent;
  gauge assertions (`Set`-based) require the test to own the store state at assertion time.
- For the store guard matrices (active / non-active / absent / expired / fresh / zero-age), a
  **table-driven** form is idiomatic Go and preferred over copy-pasted cases.
- The concurrency test (Task 5b) loops the interleaving many times under `-race`; a single pass
  of a concurrent test proves nothing.

---

## Task 1: Add `reaping` status + `ReapingSince` field

**Files:**
- Modify: `internal/backend/shared/retention.go:20-21` (status consts), `:30-44` (struct)

- [ ] **Step 1: Add the status constant**

In `internal/backend/shared/retention.go`, after the `RetentionStatusRestoring` const (line 21), add:

```go
// RetentionStatusReaping marks a record whose volumes are pending physical
// destruction: the bytes are still on disk (so the footprint must keep counting
// in the admission projection) but the record is NOT restore-claimable. It is a
// finalizer tombstone — kept until every volume is confirmed destroyed, then
// Delete()d. See ENG-376.
const RetentionStatusReaping = "reaping"
```

- [ ] **Step 2: Add the `ReapingSince` field**

In the `RetentionEntry` struct, after `RestoringSince` (line 43), add:

```go
	ReapingSince        time.Time               `json:"reaping_since,omitempty"`
```

- [ ] **Step 3: Verify it compiles**

Run: `go build ./internal/backend/shared/`
Expected: exit 0, no output.

- [ ] **Step 4: Commit**

```bash
git add internal/backend/shared/retention.go
git commit -m "feat(retention): add reaping tombstone status + ReapingSince (ENG-376)"
```

---

## Task 2: `MarkReapingIfActive` store method

**Files:**
- Modify: `internal/backend/shared/retention.go` (add method near `DeleteIfActive`)
- Test: `internal/backend/shared/retention_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/backend/shared/retention_test.go`:

```go
// TestMarkReapingIfActive verifies active→reaping is atomic, returns the volume
// names, stamps ReapingSince, and refuses non-active records (ok=false, untouched).
func TestMarkReapingIfActive(t *testing.T) {
	s := newTestRetentionStore(t)
	require.NoError(t, s.Put(sampleEntry("lease-a"))) // active, vols [vol-a vol-b]

	names, ok, err := s.MarkReapingIfActive("lease-a")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.ElementsMatch(t, []string{"vol-a", "vol-b"}, names)

	got, err := s.Get("lease-a")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, RetentionStatusReaping, got.Status)
	assert.False(t, got.ReapingSince.IsZero(), "ReapingSince must be stamped")

	// Second call: already reaping → ok=false, no error.
	_, ok, err = s.MarkReapingIfActive("lease-a")
	require.NoError(t, err)
	assert.False(t, ok)

	// Absent key → ok=false, no error.
	_, ok, err = s.MarkReapingIfActive("nonexistent")
	require.NoError(t, err)
	assert.False(t, ok)
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `go test ./internal/backend/shared/ -run TestMarkReapingIfActive`
Expected: FAIL — `b.MarkReapingIfActive undefined`.

- [ ] **Step 3: Implement the method**

In `internal/backend/shared/retention.go`, add after `DeleteIfActive` (before `RevertToActive`):

```go
// MarkReapingIfActive atomically transitions an ACTIVE record to reaping and
// returns its volume names for the caller to destroy AFTER the txn commits.
// ok=false (nil names) when absent or not active (e.g. concurrently claimed for
// restore). The record is NOT deleted — it is the finalizer tombstone that keeps
// the footprint counted until the volumes are confirmed gone. (ENG-376)
func (s *RetentionStore) MarkReapingIfActive(orig string) ([]string, bool, error) {
	var (
		names []string
		ok    bool
	)
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		var e RetentionEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return fmt.Errorf("failed to unmarshal retention entry: %w", err)
		}
		if e.Status != RetentionStatusActive {
			return nil
		}
		e.Status = RetentionStatusReaping
		e.ReapingSince = time.Now()
		names = e.RetainedVolumeNames
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		ok = true
		return bkt.Put([]byte(orig), data)
	})
	return names, ok, err
}
```

- [ ] **Step 4: Run the test**

Run: `go test ./internal/backend/shared/ -run TestMarkReapingIfActive`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/shared/retention.go internal/backend/shared/retention_test.go
git commit -m "feat(retention): MarkReapingIfActive (replaces DeleteIfActive for evict) (ENG-376)"
```

---

## Task 3: `MarkReapingIfExpired` store method

**Files:**
- Modify: `internal/backend/shared/retention.go`
- Test: `internal/backend/shared/retention_test.go`

- [ ] **Step 1: Write the failing test**

```go
// TestMarkReapingIfExpired verifies the reap mark only fires for active+expired
// records, stamps ReapingSince, returns names, and guards fresh/non-active/zero-age.
func TestMarkReapingIfExpired(t *testing.T) {
	s := newTestRetentionStore(t)
	maxAge := time.Hour

	expired := sampleEntry("lease-exp")
	expired.CreatedAt = time.Now().Add(-2 * time.Hour)
	require.NoError(t, s.Put(expired))

	names, ok, err := s.MarkReapingIfExpired("lease-exp", maxAge)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.ElementsMatch(t, []string{"vol-a", "vol-b"}, names)
	got, err := s.Get("lease-exp")
	require.NoError(t, err)
	assert.Equal(t, RetentionStatusReaping, got.Status)
	assert.False(t, got.ReapingSince.IsZero())

	// Fresh record → not reaped.
	fresh := sampleEntry("lease-fresh") // CreatedAt = now
	require.NoError(t, s.Put(fresh))
	_, ok, err = s.MarkReapingIfExpired("lease-fresh", maxAge)
	require.NoError(t, err)
	assert.False(t, ok)

	// maxAge<=0 → no-op.
	_, ok, err = s.MarkReapingIfExpired("lease-exp", 0)
	require.NoError(t, err)
	assert.False(t, ok)
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `go test ./internal/backend/shared/ -run TestMarkReapingIfExpired`
Expected: FAIL — `MarkReapingIfExpired undefined`.

- [ ] **Step 3: Implement the method**

Add after `MarkReapingIfActive`:

```go
// MarkReapingIfExpired atomically transitions an ACTIVE, expired record to
// reaping (mirrors ReapIfExpired's guards) and returns its volume names for the
// caller to destroy AFTER the txn commits. The record is NOT deleted — it stays a
// counted tombstone until the volumes are confirmed gone. Returns ok=false when
// absent, not active, or not yet expired, and a no-op when maxAge<=0. (ENG-376)
func (s *RetentionStore) MarkReapingIfExpired(orig string, maxAge time.Duration) ([]string, bool, error) {
	if maxAge <= 0 {
		return nil, false, nil
	}
	var (
		names []string
		ok    bool
	)
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		raw := bkt.Get([]byte(orig))
		if raw == nil {
			return nil
		}
		var e RetentionEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			return fmt.Errorf("failed to unmarshal retention entry: %w", err)
		}
		if e.Status != RetentionStatusActive {
			return nil
		}
		if time.Since(e.CreatedAt) < maxAge {
			return nil
		}
		e.Status = RetentionStatusReaping
		e.ReapingSince = time.Now()
		names = e.RetainedVolumeNames
		data, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		ok = true
		return bkt.Put([]byte(orig), data)
	})
	return names, ok, err
}
```

- [ ] **Step 4: Run the test** — Run: `go test ./internal/backend/shared/ -run TestMarkReapingIfExpired` → PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/shared/retention.go internal/backend/shared/retention_test.go
git commit -m "feat(retention): MarkReapingIfExpired (replaces ReapIfExpired) (ENG-376)"
```

---

## Task 4: `ListReaping` store method

**Files:**
- Modify: `internal/backend/shared/retention.go`
- Test: `internal/backend/shared/retention_test.go`

- [ ] **Step 1: Write the failing test**

```go
// TestListReaping returns only reaping records.
func TestListReaping(t *testing.T) {
	s := newTestRetentionStore(t)
	require.NoError(t, s.Put(sampleEntry("active-1")))             // active
	reaping := sampleEntry("reaping-1")
	reaping.Status = RetentionStatusReaping
	require.NoError(t, s.Put(reaping))
	restoring := sampleEntry("restoring-1")
	restoring.Status = RetentionStatusRestoring
	require.NoError(t, s.Put(restoring))

	got, err := s.ListReaping()
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "reaping-1", got[0].OriginalLeaseUUID)
}
```

- [ ] **Step 2: Run it to confirm it fails** — Run: `go test ./internal/backend/shared/ -run TestListReaping` → FAIL (`ListReaping undefined`).

- [ ] **Step 3: Implement** — add near `ListRestoring`:

```go
// ListReaping returns all entries currently in the reaping (pending-destroy) state.
func (s *RetentionStore) ListReaping() ([]RetentionEntry, error) {
	return s.filter(func(e *RetentionEntry) bool {
		return e.Status == RetentionStatusReaping
	})
}
```

- [ ] **Step 4: Run the test** — `go test ./internal/backend/shared/ -run TestListReaping` → PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/shared/retention.go internal/backend/shared/retention_test.go
git commit -m "feat(retention): ListReaping (ENG-376)"
```

---

## Task 5: `PutReaping` store method

**Files:**
- Modify: `internal/backend/shared/retention.go`
- Test: `internal/backend/shared/retention_test.go`

- [ ] **Step 1: Write the failing test**

```go
// TestPutReaping writes a fresh reaping tombstone, refuses to clobber an
// active/restoring record (ok=false), and unions names + preserves ReapingSince
// when an existing reaping record is present (idempotent re-leak).
func TestPutReaping(t *testing.T) {
	s := newTestRetentionStore(t)

	base := sampleEntry("lease-x")
	base.RetainedVolumeNames = []string{"fred-lease-x-app-0"}
	ok, err := s.PutReaping(base)
	require.NoError(t, err)
	assert.True(t, ok)
	got, err := s.Get("lease-x")
	require.NoError(t, err)
	assert.Equal(t, RetentionStatusReaping, got.Status)
	assert.False(t, got.ReapingSince.IsZero())
	first := got.ReapingSince

	// Re-leak with an extra volume → union, ReapingSince preserved.
	base2 := sampleEntry("lease-x")
	base2.RetainedVolumeNames = []string{"fred-lease-x-app-0", "fred-lease-x-app-1"}
	ok, err = s.PutReaping(base2)
	require.NoError(t, err)
	assert.True(t, ok)
	got, err = s.Get("lease-x")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"fred-lease-x-app-0", "fred-lease-x-app-1"}, got.RetainedVolumeNames)
	assert.Equal(t, first, got.ReapingSince, "ReapingSince preserved across re-leak")

	// Refuse to clobber an ACTIVE record.
	require.NoError(t, s.Put(sampleEntry("lease-active")))
	ok, err = s.PutReaping(sampleEntry("lease-active"))
	require.NoError(t, err)
	assert.False(t, ok)
	got, err = s.Get("lease-active")
	require.NoError(t, err)
	assert.Equal(t, RetentionStatusActive, got.Status, "active record must be untouched")
}
```

- [ ] **Step 2: Run it to confirm it fails** — `go test ./internal/backend/shared/ -run TestPutReaping` → FAIL (`PutReaping undefined`).

- [ ] **Step 3: Implement** — add after `PutActiveMerged` (it reuses `dedupUnion`):

```go
// PutReaping writes a reaping tombstone for an ABANDONED on-disk footprint (a
// deprovision give-up). It is idempotent and never clobbers a still-counted record:
//   - absent: writes a fresh reaping record (stamps ReapingSince=now).
//   - existing reaping: unions RetainedVolumeNames and PRESERVES ReapingSince (aging).
//   - existing active/restoring: writes NOTHING, returns ok=false — that record
//     already counts the footprint (or owns it for restore); a blind reaping write
//     would corrupt accounting/CAS. Caller treats ok=false as "already tracked".
// Single txn, so it is safe against a concurrent ClaimForRestore. (ENG-376)
func (s *RetentionStore) PutReaping(base RetentionEntry) (bool, error) {
	base.Status = RetentionStatusReaping
	base.ReapingSince = time.Now()
	var ok bool
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(retentionBucketName)
		if raw := bkt.Get([]byte(base.OriginalLeaseUUID)); raw != nil {
			var stored RetentionEntry
			if err := json.Unmarshal(raw, &stored); err != nil {
				return fmt.Errorf("failed to unmarshal retention entry: %w", err)
			}
			switch stored.Status {
			case RetentionStatusActive, RetentionStatusRestoring:
				return nil // already counted/owned — refuse, ok stays false
			case RetentionStatusReaping:
				base.ReapingSince = stored.ReapingSince // preserve aging
				base.RetainedVolumeNames = dedupUnion(stored.RetainedVolumeNames, base.RetainedVolumeNames)
			}
		}
		data, err := json.Marshal(base)
		if err != nil {
			return fmt.Errorf("failed to marshal retention entry: %w", err)
		}
		ok = true
		return bkt.Put([]byte(base.OriginalLeaseUUID), data)
	})
	return ok, err
}
```

- [ ] **Step 4: Run the test** — `go test ./internal/backend/shared/ -run TestPutReaping` → PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/shared/retention.go internal/backend/shared/retention_test.go
git commit -m "feat(retention): PutReaping tombstone for abandoned footprints (ENG-376)"
```

---

## Task 5b: Concurrency — mark-reaping vs claim-for-restore CAS safety

The reaper goroutine (`runRetentionSweep` → mark-reaping) and a `Restore()` call
(`ClaimForRestore`) race on the same record. The old in-txn *delete* closed this race; the new
mark-reaping must too. bbolt serializes the two `Update` txns, so this is a state-machine
guard-composition test, not a data-race hunt — but per fred discipline a green `-race` on
synchronous tests proves nothing, so we exercise the real interleaving many times.

**Files:**
- Test: `internal/backend/shared/retention_test.go`

- [ ] **Step 1: Write the failing test**

```go
// TestMarkReaping_VsClaimForRestore_Concurrent races mark-reaping against a restore
// claim on the SAME record many times and asserts the two atomic transitions never
// both win: a record is never both reaping AND restoring, and exactly one wins each
// round (whichever bbolt serializes first; the loser observes it and no-ops). ENG-376.
func TestMarkReaping_VsClaimForRestore_Concurrent(t *testing.T) {
	s := newTestRetentionStore(t)
	for iter := 0; iter < 200; iter++ {
		require.NoError(t, s.Put(sampleEntry("lease-c"))) // reset to active each round

		var (
			wg       sync.WaitGroup
			reapOK   bool
			reapErr  error
			claimErr error
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, reapOK, reapErr = s.MarkReapingIfActive("lease-c")
		}()
		go func() {
			defer wg.Done()
			_, claimErr = s.ClaimForRestore("lease-c", "new", time.Hour)
		}()
		wg.Wait()

		require.NoError(t, reapErr)
		claimOK := claimErr == nil
		if !claimOK {
			require.ErrorIs(t, claimErr, ErrNotRestorable, "claim loses only because the record is reaping")
		}
		assert.False(t, reapOK && claimOK, "mark-reaping and claim-for-restore must never both succeed")
		assert.True(t, reapOK || claimOK, "exactly one transition must win each round")

		got, err := s.Get("lease-c")
		require.NoError(t, err)
		if reapOK {
			assert.Equal(t, RetentionStatusReaping, got.Status)
		} else {
			assert.Equal(t, RetentionStatusRestoring, got.Status)
		}
	}
}
```

(Add `"sync"` to the test file's imports if not already present.)

- [ ] **Step 2: Run it under the race detector**

Run: `go test -race -short ./internal/backend/shared/ -run TestMarkReaping_VsClaimForRestore_Concurrent -count=1`
Expected: PASS, no data races. (`-short` is mandatory with `-race`; the `-run` filter already
scopes to this one test. If `MarkReapingIfActive` is missing, this fails to compile — it should
already exist from Task 2.)

- [ ] **Step 3: Commit**

```bash
git add internal/backend/shared/retention_test.go
git commit -m "test(retention): concurrent mark-reaping vs claim-for-restore CAS safety (ENG-376)"
```

---

## Task 6: Metrics + accounting (reaping counted in pool, not in cap)

**Files:**
- Modify: `internal/backend/docker/metrics.go` (declare metrics ~after line 341; extend `updateRetentionMetrics` ~line 347)
- Modify: `internal/backend/docker/retention_accounting.go` (add `computeReapingDiskMB`; rewrite `refreshRetentionAccounting`)
- Test: `internal/backend/docker/retention_accounting_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/backend/docker/retention_accounting_test.go`:

```go
// TestRefreshCountsReapingInPoolNotCap verifies a reaping record's footprint is
// included in the admission pool (SetRetainedDisk) and the reaping gauges, but is
// EXCLUDED from computeRetainedDiskMB (the active-only cap-breach input — a destroy
// gate that must never over-count). ENG-376.
func TestRefreshCountsReapingInPoolNotCap(t *testing.T) {
	b, rs := newBackendWithRetention(t)
	withMicroSKU(b, 1024)

	active := retentionEntryFixture("lease-a", "t1", time.Now()) // qty 2 → 2048 MB
	require.NoError(t, rs.Put(active))
	reaping := retentionEntryFixture("lease-b", "t1", time.Now()) // qty 2 → 2048 MB
	reaping.Status = shared.RetentionStatusReaping
	require.NoError(t, rs.Put(reaping))

	// Cap-breach input stays active-only (data-safe destroy direction).
	activeMB, activeCount, err := b.computeRetainedDiskMB()
	require.NoError(t, err)
	assert.Equal(t, int64(2048), activeMB)
	assert.Equal(t, 1, activeCount)

	// Reaping subset.
	reapMB, reapCount, err := b.computeReapingDiskMB()
	require.NoError(t, err)
	assert.Equal(t, int64(2048), reapMB)
	assert.Equal(t, 1, reapCount)

	// Admission pool = active + reaping (over-admit prevention).
	b.refreshRetentionAccounting()
	assert.Equal(t, int64(4096), b.pool.Stats().RetainedDiskMB)
	assert.Equal(t, float64(4096)*bytesPerMiB, testutil.ToFloat64(retainedVolumeBytes))
	assert.Equal(t, float64(1), testutil.ToFloat64(retainedLeases), "count gauge active-only")
	assert.Equal(t, float64(2048)*bytesPerMiB, testutil.ToFloat64(retentionReapingBytes))
	assert.Equal(t, float64(1), testutil.ToFloat64(retentionReapingLeases))
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `go test ./internal/backend/docker/ -run TestRefreshCountsReapingInPoolNotCap`
Expected: FAIL — `b.computeReapingDiskMB undefined` / `retentionReapingBytes undefined`.

- [ ] **Step 3: Declare the metrics**

In `internal/backend/docker/metrics.go`, inside the `var (...)` block after `retainedDiskCapBytes` (around line 340, before the closing `)`), add:

```go
	// retentionLeakedTotal counts leak events: a reap/evict/sweep that left a volume
	// on disk after a failed destroy, a deprovision give-up that abandoned a footprint,
	// or a restore-rollback whose revert did not commit. Always incremented even when
	// the store is too broken to take the tombstone write — the observable backstop.
	retentionLeakedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_leaked_total",
		Help:      "Retained-volume leak events (failed destroy / give-up / uncommitted revert) — see ENG-376",
	})

	// retentionReapingBytes is the reserved disk footprint (SKU quota) of records in
	// the reaping (pending-destroy) state — bytes still on disk awaiting reclaim.
	retentionReapingBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_reaping_bytes",
		Help:      "Reserved disk footprint of reaping (pending-destroy) retained records, in bytes",
	})

	// retentionReapingLeases is the count of reaping records (DLQ-style depth). A
	// sustained non-zero value means the sweep cannot reclaim a volume → operator action.
	retentionReapingLeases = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_reaping_leases",
		Help:      "Number of retained records stuck in the reaping (pending-destroy) state",
	})
```

- [ ] **Step 4: Extend `updateRetentionMetrics`**

Replace `internal/backend/docker/metrics.go:345-350`:

```go
// updateRetentionMetrics sets the retained-volume gauges from the current
// projection (MB) and active-lease count.
func updateRetentionMetrics(retainedMB int64, count int) {
	retainedVolumeBytes.Set(float64(retainedMB) * bytesPerMiB)
	retainedLeases.Set(float64(count))
}
```

with:

```go
// updateRetentionMetrics sets the retained-volume gauges. retainedMB is the
// ADMISSION total (active + reaping); count is the ACTIVE-only lease count;
// reapingMB/reapingCount are the reaping subset.
func updateRetentionMetrics(retainedMB int64, count int, reapingMB int64, reapingCount int) {
	retainedVolumeBytes.Set(float64(retainedMB) * bytesPerMiB)
	retainedLeases.Set(float64(count))
	retentionReapingBytes.Set(float64(reapingMB) * bytesPerMiB)
	retentionReapingLeases.Set(float64(reapingCount))
}
```

- [ ] **Step 5: Add `computeReapingDiskMB`**

In `internal/backend/docker/retention_accounting.go`, after `computeRetainedDiskMB` (line 127), add:

```go
// computeReapingDiskMB derives the reaping (pending-destroy) footprint from the
// retention store: the sum of leaseDiskMB over REAPING records, plus their count.
// These bytes are still physically on disk, so they count toward the admission
// projection (refreshRetentionAccounting adds them to SetRetainedDisk) — but NOT
// toward breachRetentionCap, whose true result DESTROYS data (over-counting a
// destroy gate is the dangerous direction). (ENG-376)
func (b *Backend) computeReapingDiskMB() (mb int64, count int, err error) {
	if b.retentionStore == nil {
		return 0, 0, nil
	}
	entries, err := b.retentionStore.List()
	if err != nil {
		return 0, 0, err
	}
	for _, e := range entries {
		if e.Status != shared.RetentionStatusReaping {
			continue
		}
		count++
		emb, _ := b.leaseDiskMB(e.Items)
		mb += emb
	}
	return mb, count, nil
}
```

- [ ] **Step 6: Rewrite `refreshRetentionAccounting`**

Replace the body of `refreshRetentionAccounting` (`internal/backend/docker/retention_accounting.go:142-152`):

```go
func (b *Backend) refreshRetentionAccounting() {
	b.retentionAccountingMu.Lock()
	defer b.retentionAccountingMu.Unlock()
	mb, count, err := b.computeRetainedDiskMB()
	if err != nil {
		b.logger.Warn("failed to recompute retained disk accounting; keeping last value", "error", err)
		return
	}
	b.pool.SetRetainedDisk(mb)
	updateRetentionMetrics(mb, count)
}
```

with:

```go
func (b *Backend) refreshRetentionAccounting() {
	b.retentionAccountingMu.Lock()
	defer b.retentionAccountingMu.Unlock()
	activeMB, activeCount, err := b.computeRetainedDiskMB()
	if err != nil {
		b.logger.Warn("failed to recompute retained disk accounting; keeping last value", "error", err)
		return
	}
	reapingMB, reapingCount, err := b.computeReapingDiskMB()
	if err != nil {
		b.logger.Warn("failed to recompute reaping disk accounting; keeping last value", "error", err)
		return
	}
	// Admission pool = active + reaping (reaping bytes are still on disk → counting
	// them prevents over-admit/ENOSPC; this gate only DENIES provisions, so an
	// over-count is safe). breachRetentionCap stays active-only (it DESTROYS).
	b.pool.SetRetainedDisk(activeMB + reapingMB)
	updateRetentionMetrics(activeMB+reapingMB, activeCount, reapingMB, reapingCount)
}
```

- [ ] **Step 7: Run the test**

Run: `go test ./internal/backend/docker/ -run 'TestRefreshCountsReapingInPoolNotCap|TestComputeRetainedDiskMB|TestRefreshRetentionAccounting'`
Expected: PASS (existing accounting tests still green — `computeRetainedDiskMB` unchanged).

- [ ] **Step 8: Commit**

```bash
git add internal/backend/docker/metrics.go internal/backend/docker/retention_accounting.go internal/backend/docker/retention_accounting_test.go
git commit -m "feat(metrics): retention_leaked + reaping gauges; count reaping in admission pool not cap (ENG-376)"
```

---

## Task 7: Site 1 — `reapExpiredRetentions` via mark-reaping

**Files:**
- Modify: `internal/backend/docker/restore.go:260-304` (rewrite) + add `destroyReapingVolumes`
- Test: `internal/backend/docker/restore_test.go` (replace the re-record test at ~1508)

- [ ] **Step 1: Write the failing test**

Add to `internal/backend/docker/restore_test.go` (and DELETE the old re-record test whose comment is at ~`:1508` — search for the test func that asserts `reapExpiredRetentions` re-records on destroy failure and remove it):

```go
// TestReap_DestroyFail_LeavesReapingCounted verifies the finalizer fix: when a
// volume Destroy fails during reap, the record is left in the REAPING state (NOT
// deleted, NOT re-recorded) so computeRetainedDiskMB-via-pool keeps counting the
// footprint, the leak counter increments, and no under-count occurs. ENG-376 AC#1.
func TestReap_DestroyFail_LeavesReapingCounted(t *testing.T) {
	leakBefore := testutil.ToFloat64(retentionLeakedTotal)
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{})
	withMicroSKU(b, 1024)
	b.cfg.RetentionMaxAge = time.Hour
	rs := attachRetentionStore(t, b)

	exp := retentionEntryFixture("lease-exp", "t1", time.Now().Add(-2*time.Hour)) // expired, qty 2
	exp.RetainedVolumeNames = []string{"fred-retained-lease-exp-app-0"}
	require.NoError(t, rs.Put(exp))

	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, _ string) error { return errors.New("EBUSY") },
	}

	n, err := b.reapExpiredRetentions(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, n, "destroy failed → not counted as reaped")

	got, err := rs.Get("lease-exp")
	require.NoError(t, err)
	require.NotNil(t, got, "record must NOT be deleted on destroy failure")
	assert.Equal(t, shared.RetentionStatusReaping, got.Status)

	// Footprint still counted in the admission pool (no under-count).
	assert.Equal(t, int64(2048), b.pool.Stats().RetainedDiskMB)
	assert.Greater(t, testutil.ToFloat64(retentionLeakedTotal), leakBefore)
}

// TestReap_DestroySuccess_DeletesRecord verifies the happy path: destroy succeeds
// → record deleted → projection drops to 0.
func TestReap_DestroySuccess_DeletesRecord(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{})
	withMicroSKU(b, 1024)
	b.cfg.RetentionMaxAge = time.Hour
	rs := attachRetentionStore(t, b)

	exp := retentionEntryFixture("lease-exp", "t1", time.Now().Add(-2*time.Hour))
	exp.RetainedVolumeNames = []string{"fred-retained-lease-exp-app-0"}
	require.NoError(t, rs.Put(exp))

	b.volumes = &mockVolumeManager{DestroyFn: func(_ context.Context, _ string) error { return nil }}

	n, err := b.reapExpiredRetentions(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	got, err := rs.Get("lease-exp")
	require.NoError(t, err)
	assert.Nil(t, got, "record deleted after confirmed destroy")
	assert.Equal(t, int64(0), b.pool.Stats().RetainedDiskMB)
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `go test ./internal/backend/docker/ -run 'TestReap_Destroy'`
Expected: FAIL — `reapExpiredRetentions` still deletes/re-records (record nil on fail, or status not reaping).

- [ ] **Step 3: Add `destroyReapingVolumes` + rewrite `reapExpiredRetentions`**

In `internal/backend/docker/restore.go`, add this helper (e.g. just above `reapExpiredRetentions`):

```go
// destroyReapingVolumes destroys every volume of a reaping record and, ONLY if all
// destroys succeed, Delete()s the record. Returns true iff the record was fully
// reaped (deleted). On any destroy failure it LEAVES the record reaping (the
// finalizer retry) and bumps retentionLeakedTotal — the footprint stays counted
// and the next sweep retries. Idempotent: already-gone volume names no-op, and a
// Delete failure leaves the record reaping for a later retry (no under-count). (ENG-376)
func (b *Backend) destroyReapingVolumes(ctx context.Context, orig string, names []string) bool {
	destroyFailed := false
	for _, name := range names {
		if derr := b.volumes.Destroy(ctx, name); derr != nil {
			b.logger.Error("reap: destroy volume", "volume", name, "error", derr)
			destroyFailed = true
		}
	}
	if destroyFailed {
		retentionLeakedTotal.Inc()
		b.logger.Warn("reap: volume(s) still on disk; record left reaping for retry (footprint stays counted)",
			"lease_uuid", orig)
		return false
	}
	if derr := b.retentionStore.Delete(orig); derr != nil {
		b.logger.Warn("reap: destroy ok but record delete failed; next sweep retries", "lease_uuid", orig, "error", derr)
		return false
	}
	return true
}
```

Replace `reapExpiredRetentions` (`restore.go:265-304`) with:

```go
func (b *Backend) reapExpiredRetentions(ctx context.Context) (int, error) {
	if b.retentionStore == nil || b.cfg.RetentionMaxAge <= 0 {
		return 0, nil
	}
	candidates, err := b.retentionStore.ListExpired(b.cfg.RetentionMaxAge)
	if err != nil {
		return 0, err
	}
	var n int
	for _, e := range candidates {
		// Atomic active→reaping (the record is NEVER deleted before its volumes are
		// confirmed gone, so a destroy failure cannot drop a still-on-disk footprint).
		names, ok, merr := b.retentionStore.MarkReapingIfExpired(e.OriginalLeaseUUID, b.cfg.RetentionMaxAge)
		if merr != nil {
			b.logger.Error("reap: store error", "lease_uuid", e.OriginalLeaseUUID, "error", merr)
			continue
		}
		if !ok {
			continue // concurrently claimed/changed since the snapshot — skip
		}
		if b.destroyReapingVolumes(ctx, e.OriginalLeaseUUID, names) {
			n++
		}
	}
	b.refreshRetentionAccounting()
	return n, nil
}
```

Update the doc comment above `reapExpiredRetentions` (lines 260-264) to describe the mark-reaping behavior instead of "re-records the entry".

- [ ] **Step 4: Run the test** — `go test ./internal/backend/docker/ -run 'TestReap_Destroy'` → PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/docker/restore.go internal/backend/docker/restore_test.go
git commit -m "fix(retention): reap leaves record reaping on destroy fail, never under-counts (ENG-376)"
```

---

## Task 8: `retryReapingRecords` + sweep/boot wiring

**Files:**
- Modify: `internal/backend/docker/restore.go` (add `retryReapingRecords`; wire `runRetentionSweep:311-327`; add reaping arm to `reconcileRetentions:91-108`)
- Test: `internal/backend/docker/restore_test.go`

- [ ] **Step 1: Write the failing test**

```go
// TestRetryReapingRecords_ReclaimsWhenDestroyRecovers verifies a stuck reaping
// record is auto-reclaimed by a later sweep once Destroy starts succeeding.
func TestRetryReapingRecords_ReclaimsWhenDestroyRecovers(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{})
	withMicroSKU(b, 1024)
	rs := attachRetentionStore(t, b)

	reaping := retentionEntryFixture("lease-r", "t1", time.Now())
	reaping.Status = shared.RetentionStatusReaping
	reaping.RetainedVolumeNames = []string{"fred-retained-lease-r-app-0"}
	require.NoError(t, rs.Put(reaping))

	var fail atomic.Bool
	fail.Store(true)
	b.volumes = &mockVolumeManager{
		DestroyFn: func(_ context.Context, _ string) error {
			if fail.Load() {
				return errors.New("EBUSY")
			}
			return nil
		},
	}

	// First sweep: destroy fails → record stays reaping.
	require.NoError(t, b.retryReapingRecords(context.Background()))
	got, err := rs.Get("lease-r")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, shared.RetentionStatusReaping, got.Status)

	// Destroy recovers; next sweep reclaims + deletes.
	fail.Store(false)
	require.NoError(t, b.retryReapingRecords(context.Background()))
	got, err = rs.Get("lease-r")
	require.NoError(t, err)
	assert.Nil(t, got, "reaping record deleted after destroy recovers")
}
```

- [ ] **Step 2: Run it to confirm it fails** — `go test ./internal/backend/docker/ -run TestRetryReapingRecords` → FAIL (`retryReapingRecords undefined`).

- [ ] **Step 3: Implement + wire**

Add to `internal/backend/docker/restore.go`:

```go
// retryReapingRecords re-attempts destruction of every reaping record's volumes
// (the finalizer retry) and deletes each record whose volumes are confirmed gone.
// Runs on the periodic sweep AND at boot. Fail-closed: on a store List error the
// records are kept (footprint keeps counting). (ENG-376)
func (b *Backend) retryReapingRecords(ctx context.Context) error {
	if b.retentionStore == nil {
		return nil
	}
	recs, err := b.retentionStore.ListReaping()
	if err != nil {
		return err
	}
	for _, e := range recs {
		b.destroyReapingVolumes(ctx, e.OriginalLeaseUUID, e.RetainedVolumeNames)
	}
	return nil
}
```

Wire into `runRetentionSweep` (replace `restore.go:311-327`):

```go
func (b *Backend) runRetentionSweep(ctx context.Context) error {
	if _, err := b.reapExpiredRetentions(ctx); err != nil {
		return err
	}
	if b.retentionStore == nil {
		return nil
	}
	if err := b.retryReapingRecords(ctx); err != nil {
		return err
	}
	recs, err := b.retentionStore.ListRestoring()
	if err != nil {
		return err
	}
	for _, e := range recs {
		b.reconcileRestoring(ctx, e)
	}
	b.refreshRetentionAccounting()
	return nil
}
```

Add a reaping arm to the boot `reconcileRetentions` switch (`restore.go:92-106`) — after the `case shared.RetentionStatusRestoring:` block, add:

```go
		case shared.RetentionStatusReaping:
			// Finalizer retry at boot: re-attempt destroy of any record stranded
			// reaping by a prior crash/destroy-failure; delete it when confirmed gone.
			b.destroyReapingVolumes(ctx, e.OriginalLeaseUUID, e.RetainedVolumeNames)
```

- [ ] **Step 4: Run the test** — `go test ./internal/backend/docker/ -run TestRetryReapingRecords` → PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/docker/restore.go internal/backend/docker/restore_test.go
git commit -m "feat(retention): retryReapingRecords sweep + boot reaping reconcile (ENG-376)"
```

---

## Task 9: Site 2 — `evictRetentionsToCap` via mark-reaping

**Files:**
- Modify: `internal/backend/docker/restore.go:227-257` (eviction loop)
- Test: `internal/backend/docker/restore_test.go` (replace the re-record test at ~`:504`)

- [ ] **Step 1: Write the failing test**

Add this test and DELETE the old evict re-record test (search ~`:504` for the test asserting `evictRetentionsToCap` re-records on destroy failure):

```go
// TestEvict_DestroyFail_LeavesReapingCounted verifies cap-eviction marks the
// evicted record reaping (removing it from the active cap set), and a destroy
// failure leaves it reaping + counted in the pool, never under-counting. ENG-376.
func TestEvict_DestroyFail_LeavesReapingCounted(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{})
	withMicroSKU(b, 1024)
	b.cfg.MaxRetainedLeasesPerTenant = 1
	rs := attachRetentionStore(t, b)

	// Two active records for the same tenant → evicting to cap (1) removes the oldest.
	old := retentionEntryFixture("lease-old", "t1", time.Now().Add(-2*time.Hour))
	old.RetainedVolumeNames = []string{"fred-retained-lease-old-app-0"}
	require.NoError(t, rs.Put(old))
	newer := retentionEntryFixture("lease-new", "t1", time.Now())
	require.NoError(t, rs.Put(newer))

	b.volumes = &mockVolumeManager{DestroyFn: func(_ context.Context, _ string) error { return errors.New("EBUSY") }}

	err := b.evictRetentionsToCap(context.Background(), "t1", 1, "lease-new")
	require.NoError(t, err)

	got, err := rs.Get("lease-old")
	require.NoError(t, err)
	require.NotNil(t, got, "evicted record kept as reaping tombstone on destroy fail")
	assert.Equal(t, shared.RetentionStatusReaping, got.Status)
	// Pool still counts both footprints (active lease-new + reaping lease-old).
	assert.Equal(t, int64(2*2048), b.pool.Stats().RetainedDiskMB)
}
```

- [ ] **Step 2: Run it to confirm it fails** — `go test ./internal/backend/docker/ -run TestEvict_DestroyFail` → FAIL.

- [ ] **Step 3: Rewrite the eviction loop**

Replace `restore.go:227-256` (the `for i := 0; ...` loop through the trailing `b.refreshRetentionAccounting()`) with:

```go
	for i := 0; i <= len(active)-maxPerTenant; i++ {
		b.logger.Warn("evicting tenant's oldest retained lease to honor cap", "tenant", tenant, "lease_uuid", active[i].OriginalLeaseUUID)
		// Atomic active→reaping (TOCTOU-safe: a record concurrently claimed for
		// restore returns ok=false and is skipped). The record is the finalizer
		// tombstone — it is removed from the active cap set immediately (making room)
		// but stays counted in the admission pool until its volumes are confirmed gone.
		names, ok, merr := b.retentionStore.MarkReapingIfActive(active[i].OriginalLeaseUUID)
		if merr != nil {
			return merr
		}
		if !ok {
			continue // concurrently claimed for restore (or already gone) — skip
		}
		b.destroyReapingVolumes(ctx, active[i].OriginalLeaseUUID, names)
	}
	b.refreshRetentionAccounting()
	return nil
```

Update the eviction doc comment block (`restore.go:222-254` inline comments) to describe mark-reaping instead of `DeleteIfActive` + re-record.

- [ ] **Step 4: Run the test** — `go test ./internal/backend/docker/ -run TestEvict_DestroyFail` → PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/docker/restore.go internal/backend/docker/restore_test.go
git commit -m "fix(retention): evict marks reaping on destroy fail, never under-counts (ENG-376)"
```

---

## Task 10: Remove the now-unused `ReapIfExpired`

> **Post-merge correction (ENG-370):** as originally written this task also removed `DeleteIfActive`,
> which was correct for ENG-376 *in isolation* (only the eviction path used it, now replaced by
> `MarkReapingIfActive`). After merging `origin/main`, ENG-370's `reconcileOrphanedRetentions`
> (`internal/backend/docker/restore.go`) uses `DeleteIfActive` as its ACTIVE-only CAS delete, so
> **`DeleteIfActive` MUST be retained** — removing it breaks the build. Only `ReapIfExpired` is removed.

**Files:**
- Modify: `internal/backend/shared/retention.go` (delete `ReapIfExpired` + its doc comment; KEEP `DeleteIfActive`)
- Modify: `internal/backend/shared/retention_test.go` (delete `TestRetentionStore_ReapIfExpired_Guards`; KEEP `TestDeleteIfActive_*`)

- [ ] **Step 1: Confirm no remaining callers of `ReapIfExpired`**

Run: `grep -rn "ReapIfExpired" internal --include="*.go"`
Expected: only the definition in `retention.go` and its guard test. If any production caller remains, STOP — an earlier task is incomplete. (Do NOT grep-and-remove `DeleteIfActive`; `reconcileOrphanedRetentions` depends on it.)

- [ ] **Step 2: Delete the method**

In `internal/backend/shared/retention.go`, delete the `ReapIfExpired` method (and its doc comment). Leave `DeleteIfActive` in place.

- [ ] **Step 3: Delete the obsolete store test**

In `internal/backend/shared/retention_test.go`, delete `TestRetentionStore_ReapIfExpired_Guards` (its behavior is now covered by the `MarkReaping*` tests). Keep the `TestDeleteIfActive_*` tests.

- [ ] **Step 4: Verify build + package tests**

Run: `go build ./... && go test ./internal/backend/shared/ -run 'Retention|Reaping|DeleteIfActive|ReapIfExpired'`
Expected: build exit 0; tests PASS; no reference to the deleted symbols.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/shared/retention.go internal/backend/shared/retention_test.go
git commit -m "refactor(retention): remove ReapIfExpired superseded by MarkReapingIfExpired (ENG-376)"
```

---

## Task 11: Site 4 — `rollbackRestoreAdoption` make-before-break

**Files:**
- Modify: `internal/backend/docker/restore.go:737-752` (revert arm)
- Test: `internal/backend/docker/restore_test.go`

- [ ] **Step 1: Write the failing test**

```go
// TestRollback_RevertStoreError_KeepsLiveCounted verifies make-before-break: when
// RevertToActive returns a STORE ERROR during rollback, the live allocation is NOT
// released (F stays counted as live, no under-count), the leak counter increments,
// and reconcileRestoring later releases it after a successful revert. ENG-376 site 4.
func TestRollback_RevertStoreError_KeepsLiveCounted(t *testing.T) {
	leakBefore := testutil.ToFloat64(retentionLeakedTotal)
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{})
	withMicroSKU(b, 1024)
	rs := attachRetentionStore(t, b)

	// A restoring record + a live allocation for the new lease (qty 1 → 1024 MB).
	rec := retentionEntryFixture("orig", "t1", time.Now())
	rec.Items = []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}}
	rec.Status = shared.RetentionStatusRestoring
	rec.NewLeaseUUID = "new"
	rec.Generation = 2
	require.NoError(t, rs.Put(rec))
	require.NoError(t, b.pool.TryAllocateAdopt("new-app-0", "docker-micro", "t1"))

	b.volumes = &mockVolumeManager{RenameVolumeFn: func(_, _ string) error { return nil }} // re-quarantine succeeds

	// Capture the live footprint AFTER allocation but BEFORE forcing the revert error.
	// (Assert against this delta, not a literal MB — the pool sizes the live allocation
	// from its own SKU profiles, which differ from b.cfg.GetSKUProfile's withMicroSKU view.)
	allocBefore := b.pool.Stats().AllocatedDiskMB
	require.Greater(t, allocBefore, int64(0), "sanity: live allocation is counted")
	require.NoError(t, rs.Close()) // force RevertToActive to ERROR

	allocated := []string{"new-app-0"}
	recCopy := rec
	b.rollbackRestoreAdoption(context.Background(), "new", allocated, &recCopy, true, b.logger)

	// Live allocation NOT released on a revert store-error → still counted (no under-count).
	assert.Equal(t, allocBefore, b.pool.Stats().AllocatedDiskMB, "live stays counted on revert store-error")
	assert.Greater(t, testutil.ToFloat64(retentionLeakedTotal), leakBefore)
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `go test ./internal/backend/docker/ -run TestRollback_RevertStoreError`
Expected: FAIL — current code calls `releaseAll` unconditionally, so `AllocatedDiskMB` is 0.

- [ ] **Step 3: Rewrite the revert arm**

Replace `restore.go:737-752` (from the `// Re-quarantine succeeded:` comment through the closing brace of `rollbackRestoreAdoption`):

```go
	// Re-quarantine succeeded. Make-before-break: only release the live allocation
	// AFTER the destination owner (the active record) durably commits. A store ERROR
	// means the revert did NOT commit, so we KEEP live counted (F stays counted as
	// live — no under-count) and let reconcileRestoring resume the revert and release
	// the same liveIDs. We still removeProvision for dropProvision=true so reconcile's
	// orphaned arm (live=false) runs — a lingering Provisioning provision would make
	// reconcileRestoring defer forever.
	ok, rerr := b.retentionStore.RevertToActive(rec.OriginalLeaseUUID, rec.Generation)
	if rerr != nil {
		retentionLeakedTotal.Inc()
		logger.Error("restore rollback: revert record failed; keeping live allocation counted until reconcile resumes the revert",
			"lease_uuid", rec.OriginalLeaseUUID, "error", rerr)
		if dropProvision {
			b.removeProvision(leaseUUID)
		}
		return
	}
	if !ok {
		// Generation changed: the record was reverted/re-claimed elsewhere (now active
		// or owned by a new restore). It is counted there, so RELEASE live to avoid a
		// 2F double-count — matching the prior behavior for this benign case.
		logger.Warn("restore rollback: record generation changed; reaper will reconcile", "lease_uuid", rec.OriginalLeaseUUID)
	}
	b.refreshRetentionAccounting() // retained += F (record now active, if ok)
	releaseAll(b.pool, allocatedIDs)
	updateResourceMetrics(b.pool.Stats())
	if dropProvision {
		b.removeProvision(leaseUUID)
	}
}
```

Update the `rollbackRestoreAdoption` doc comment (lines 705-712) to note the make-before-break behavior on a revert store-error (keep live counted; reconcile resumes).

- [ ] **Step 4: Run the test** — `go test ./internal/backend/docker/ -run TestRollback_RevertStoreError` → PASS.

- [ ] **Step 5: Run the existing rollback tests for regressions**

Run: `go test ./internal/backend/docker/ -run 'TestRestore|Rollback|Reconcile'`
Expected: PASS (the `!ok` and success arms keep prior behavior; only the store-error arm changed).

- [ ] **Step 6: Commit**

```bash
git add internal/backend/docker/restore.go internal/backend/docker/restore_test.go
git commit -m "fix(restore): make-before-break — keep live counted on rollback revert store-error (ENG-376)"
```

---

## Task 12: Site 3 — `doDeprovision` give-up tombstone

**Files:**
- Modify: `internal/backend/docker/deprovision.go` (add `recordGiveUpLeak`; call it in the give-up branch ~line 352-367)
- Test: `internal/backend/docker/deprovision_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/backend/docker/deprovision_test.go` (mirror the existing deprovision test setup — `newBackendForProvisionTest` + `attachRetentionStore` + `mockVolumeManager`):

```go
// TestDeprovisionGiveUp_WritesReapingTombstone verifies a give-up (max volume
// cleanup attempts) writes a reaping tombstone for the leaked canonical volumes so
// the footprint keeps counting + the sweep auto-retries, instead of a silent
// uncounted leak. ENG-376 site 3.
func TestDeprovisionGiveUp_WritesReapingTombstone(t *testing.T) {
	leakBefore := testutil.ToFloat64(retentionLeakedTotal)
	mock := &mockDockerClient{RemoveContainerFn: func(_ context.Context, _ string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"u1": {ProvisionState: leasesm.ProvisionState{
			LeaseUUID: "u1", Tenant: "t1", Status: backend.ProvisionStatusReady, Quantity: 1,
			Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1, ServiceName: "app"}},
		}, volumeCleanupAttempts: maxVolumeCleanupAttempts - 1}, // next failure → give up
	})
	withMicroSKU(b, 1024)
	rs := attachRetentionStore(t, b) // RetainOnClose stays false → non-retain destroy arm

	b.volumes = &mockVolumeManager{
		ListFn:    func() ([]string, error) { return []string{"fred-u1-app-0"}, nil },
		DestroyFn: func(_ context.Context, _ string) error { return errors.New("EBUSY") },
	}

	// The give-up branch returns nil to the actor (it abandons to manual cleanup and
	// fires a failed callback), so do not assert on Deprovision's return value here —
	// the load-bearing assertions are the tombstone + the leak counter below.
	_ = b.Deprovision(context.Background(), "u1")

	// Poll for the reaping tombstone.
	var got *shared.RetentionEntry
	require.Eventually(t, func() bool {
		g, e := rs.Get("u1")
		if e != nil || g == nil {
			return false
		}
		got = g
		return true
	}, 5*time.Second, 20*time.Millisecond, "reaping tombstone for u1 must be written at give-up")

	assert.Equal(t, shared.RetentionStatusReaping, got.Status)
	assert.ElementsMatch(t, []string{"fred-u1-app-0"}, got.RetainedVolumeNames)
	assert.Greater(t, testutil.ToFloat64(retentionLeakedTotal), leakBefore)
}
```

> **Note for the implementer:** `volumeCleanupAttempts` is the docker-private wrapper field on the
> `provision` struct (lowercase; accessible from package `docker`). Seeding it to
> `maxVolumeCleanupAttempts - 1` makes the single forced Destroy failure trip the give-up branch.
> Confirm the field name against the `provision` struct if the literal does not compile.

- [ ] **Step 2: Run it to confirm it fails** — `go test ./internal/backend/docker/ -run TestDeprovisionGiveUp_WritesReapingTombstone` → FAIL (no tombstone written).

- [ ] **Step 3: Add `recordGiveUpLeak`**

In `internal/backend/docker/deprovision.go`, add (the file already imports `strings`, `time`, `shared`, `backend`, `slog`):

```go
// recordGiveUpLeak handles a deprovision give-up's abandoned on-disk footprint. It
// always increments retentionLeakedTotal (the observable backstop). When a retention
// store is configured it also writes a reaping tombstone for the lease's still-on-disk
// volumes (ground-truthed from disk; canonical names derived from items on List error)
// so the footprint keeps counting in the admission projection and the retention sweep
// auto-retries the destroy — turning a permanent manual-only leak into a self-healing
// one. PutReaping is idempotent and refuses to clobber an active/restoring record, so a
// footprint an existing record already counts is left untouched. (ENG-376)
func (b *Backend) recordGiveUpLeak(ctx context.Context, leaseUUID, tenant, providerUUID string, items []backend.LeaseItem, logger *slog.Logger) {
	retentionLeakedTotal.Inc()
	if b.retentionStore == nil {
		return // no projection to correct; metric + the give-up log are the record
	}
	var leaked []string
	if all, err := b.volumes.List(); err == nil {
		cprefix := leaseVolumePrefix(leaseUUID)  // fred-{lease}-
		rprefix := retainedName(cprefix)         // fred-retained-{lease}-
		for _, id := range all {
			if strings.HasPrefix(id, cprefix) || strings.HasPrefix(id, rprefix) {
				leaked = append(leaked, id)
			}
		}
	} else {
		logger.Warn("give-up leak: volume list failed; deriving canonical names from items", "error", err)
		for _, item := range items {
			for i := range item.Quantity {
				leaked = append(leaked, canonicalVolumeName(leaseUUID, item.ServiceName, i))
			}
		}
	}
	if len(leaked) == 0 {
		return // nothing on disk to account for
	}
	rec := shared.RetentionEntry{
		OriginalLeaseUUID:   leaseUUID,
		Tenant:              tenant,
		ProviderUUID:        providerUUID,
		Items:               items,
		RetainedVolumeNames: leaked,
		Status:              shared.RetentionStatusReaping,
		CreatedAt:           time.Now(),
	}
	if ok, err := b.retentionStore.PutReaping(rec); err != nil {
		logger.Error("give-up leak: failed to record reaping tombstone; footprint UNTRACKED until manual cleanup", "lease_uuid", leaseUUID, "error", err)
	} else if !ok {
		logger.Info("give-up leak: an active/restoring record already counts this footprint; no tombstone written", "lease_uuid", leaseUUID)
	}
	// Reflect the new tombstone immediately (the deferred refresh only runs on the
	// retain path; a non-retain give-up would otherwise wait for the next sweep).
	b.refreshRetentionAccounting()
}
```

- [ ] **Step 4: Call it from the give-up branch**

In `internal/backend/docker/deprovision.go`, in the `if attempts >= maxVolumeCleanupAttempts {` block, immediately BEFORE the existing `releaseLive()` call (line 367), insert:

```go
				// Persist the abandoned footprint as a reaping tombstone BEFORE releasing
				// live, so the bytes hand off live→reaping with no uncounted gap. (ENG-376)
				b.recordGiveUpLeak(ctx, leaseUUID, tenant, providerUUID, items, logger)
```

- [ ] **Step 5: Run the test** — `go test ./internal/backend/docker/ -run TestDeprovisionGiveUp_WritesReapingTombstone` → PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/backend/docker/deprovision.go internal/backend/docker/deprovision_test.go
git commit -m "fix(deprovision): give-up writes reaping tombstone; footprint stays counted + auto-retried (ENG-376)"
```

---

## Task 13: "Apply it evenly" status-branch audit (§E)

**Files:**
- Modify: `internal/backend/docker/recover.go` (verify reaping NOT protected) + any site needing a fix
- Test: `internal/backend/docker/restore_test.go` (audit tests)

- [ ] **Step 1: Enumerate every status-branch site**

Run: `grep -rn "\.Status ==\|\.Status !=\|RetentionStatus" internal/backend/docker internal/backend/shared --include="*.go" | grep -v _test.go`
Confirm each is covered by the spec §E table. The expected handling:
- `computeRetainedDiskMB` — active only; `computeReapingDiskMB` — reaping only ✓ (Task 6)
- `evictRetentionsToCap` active filter — reaping excluded ✓ (Task 9)
- `ClaimForRestore`, `PutActiveMerged`, `PutReaping`, `MarkReaping*` — active-only guards ✓
- `reconcileRetentions` switch — reaping arm ✓ (Task 8)
- `cleanupOrphanedVolumes` (`recover.go`) — only `active`/`restoring` add to the protected set; **reaping must NOT be protected** (we want it reaped). Verify the switch has no reaping case.
- `ListExpired` — active-only (never re-marks reaping) ✓
- `shouldRefuseRetention` — `rec != nil → return false` (a non-nil reaping record → don't refuse; data-safe) ✓

- [ ] **Step 2: Write the store-level audit tests (shared package)**

Add to `internal/backend/shared/retention_test.go` (reuses the package's `newTestRetentionStore` + `sampleEntry`):

```go
// TestStatusAudit_ClaimForRestore_RejectsReaping ensures a reaping record cannot be restored.
func TestStatusAudit_ClaimForRestore_RejectsReaping(t *testing.T) {
	s := newTestRetentionStore(t)
	r := sampleEntry("lease-r")
	r.Status = RetentionStatusReaping
	require.NoError(t, s.Put(r))
	_, err := s.ClaimForRestore("lease-r", "new", time.Hour)
	require.Error(t, err, "ClaimForRestore must reject a reaping record")
}

// TestStatusAudit_ListExpired_ExcludesReaping ensures the reaper never re-marks a reaping record.
func TestStatusAudit_ListExpired_ExcludesReaping(t *testing.T) {
	s := newTestRetentionStore(t)
	r := sampleEntry("lease-r")
	r.Status = RetentionStatusReaping
	r.CreatedAt = time.Now().Add(-2 * time.Hour)
	require.NoError(t, s.Put(r))
	got, err := s.ListExpired(time.Hour)
	require.NoError(t, err)
	assert.Empty(t, got, "ListExpired returns active-only")
}
```

- [ ] **Step 3: Write the `cleanupOrphanedVolumes` audit test (docker package)**

```go
// TestStatusAudit_Cleanup_DoesNotProtectReapingCanonical ensures a reaping record's
// canonical volume is NOT added to the orphan-cleanup protected set (it must be reaped).
func TestStatusAudit_Cleanup_DoesNotProtectReapingCanonical(t *testing.T) {
	mock := &mockDockerClient{}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{})
	rs := attachRetentionStore(t, b)

	reaping := retentionEntryFixture("lease-r", "t1", time.Now())
	reaping.Status = shared.RetentionStatusReaping
	reaping.RetainedVolumeNames = []string{"fred-retained-lease-r-app-0"}
	require.NoError(t, rs.Put(reaping))

	var destroyed []string
	b.volumes = &mockVolumeManager{
		ListFn:    func() ([]string, error) { return []string{"fred-lease-r-app-0"}, nil }, // a stray CANONICAL
		DestroyFn: func(_ context.Context, id string) error { destroyed = append(destroyed, id); return nil },
	}

	require.NoError(t, b.cleanupOrphanedVolumes(context.Background()))
	assert.Contains(t, destroyed, "fred-lease-r-app-0", "reaping canonical must NOT be protected from orphan cleanup")
}
```

- [ ] **Step 4: Run the audit tests + fix any gap**

Run: `go test ./internal/backend/shared/ ./internal/backend/docker/ -run 'StatusAudit'`
Expected: PASS. If `TestStatusAudit_Cleanup_DoesNotProtectReapingCanonical` fails, confirm `recover.go`'s `cleanupOrphanedVolumes` switch has only `active`/`restoring` cases (no reaping case) — that is the intended state; do not add one.

- [ ] **Step 5: Commit**

```bash
git add internal/backend/shared/retention_test.go internal/backend/docker/restore_test.go
git commit -m "test(retention): status-branch audit — reaping handled evenly across all readers (ENG-376)"
```

---

## Task 14: OPERATIONS runbook + alert

**Files:**
- Modify: `OPERATIONS.md` (new subsection under "Reclaiming retained volumes under disk pressure"; new row in "Common alerts" ~line 42)

- [ ] **Step 1: Add the alert-table row**

In `OPERATIONS.md`, in the "Common alerts and what they mean" table (after the existing retention row at line 42), add:

```markdown
| `fred_docker_backend_retention_reaping_bytes` > 0 sustained across several sweeps (or `..._retention_leaked_total` rising) | A retained/leaked volume's destroy keeps failing — uncounted-then-tracked orphan the sweep can't reclaim | [Reclaiming leaked / stuck-reaping orphan volumes](#reclaiming-leaked--stuck-reaping-orphan-volumes) |
```

- [ ] **Step 2: Add the reclaim subsection**

After the "Reclaiming retained volumes under disk pressure" section (after line 141, before the `---`), add:

```markdown
### Reclaiming leaked / stuck-reaping orphan volumes

A `fred-retained-*` (or leaked-canonical) volume whose destroy fails under a degraded
filesystem/store becomes a **reaping tombstone**: its footprint keeps counting in the admission
pool (so it never silently over-admits) and the retention sweep **auto-retries** the destroy
every interval. `cleanupOrphanedVolumes` deliberately never touches `fred-retained-*` names, so
the sweep is the only automatic reclaimer.

- **Signal.** `fred_docker_backend_retention_reaping_bytes` / `..._retention_reaping_leases` > 0.
  A transient EBUSY clears within one sweep; a value sustained across several sweeps is a stuck
  volume. `..._retention_leaked_total` increments on every failed-destroy / give-up / uncommitted
  revert (DLQ-style: even one warrants a look).
- **Diagnose.** Find the volume(s): `ls <volume_data_path> | grep -E 'fred-retained-|fred-'`.
  Check why destroy fails — a container still bind-mounting it (`docker ps`, then stop it), or a
  filesystem error (`dmesg`).
- **Reclaim (only after confirming no live/restoring lease references it).** Once the blocker is
  cleared the next sweep reclaims it automatically. To force it sooner, restart the backend
  (boot runs the reaping reconcile). If the volume is genuinely unrecoverable, remove it
  manually (`docker volume rm <name>` or `rm -rf <volume_data_path>/<name>`) — the next sweep
  then deletes the now-dangling tombstone (its destroy is an idempotent no-op).
```

- [ ] **Step 3: Verify the doc renders (no broken anchor)**

Run: `grep -n "Reclaiming leaked / stuck-reaping orphan volumes" OPERATIONS.md`
Expected: two matches (the alert-table link target + the heading).

- [ ] **Step 4: Commit**

```bash
git add OPERATIONS.md
git commit -m "docs(ops): runbook + alert for leaked/stuck-reaping orphan volumes (ENG-376)"
```

---

## Task 15: Full verification + Linear ticket update

**Files:** none (verification + ticket)

- [ ] **Step 1: Full package test with the race detector (fred convention)**

Run: `go test -race -short ./internal/backend/docker/ ./internal/backend/shared/`
Expected: PASS, no data races.

- [ ] **Step 2: Full build + vet**

Run: `go build ./... && go vet ./internal/backend/...`
Expected: exit 0.

- [ ] **Step 3: Lint (if configured)**

Run: `golangci-lint run ./internal/backend/... 2>/dev/null || echo "golangci-lint not available; skipping"`
Expected: clean, or skip note.

- [ ] **Step 4: Confirm acceptance criteria are met**
  - AC #1 — `TestReap_DestroyFail_LeavesReapingCounted` asserts no under-count after a destroy failure. ✓
  - AC #2 — `retention_leaked_total` exported + asserted in the reap/give-up/rollback tests. ✓
  - AC #3 — OPERATIONS reclaim subsection added. ✓

- [ ] **Step 5: Update the Linear ticket** (outward-facing — only after the work is verified)

Post a comment on ENG-376 recording that the implemented fix is the **reaping-tombstone +
make-before-break** design (single source of truth; no addend bucket), that site 4 is fixed via
the codebase's existing make-before-break idiom rather than the addend proposed in comment #4,
and link the spec/plan + the merged PR. (Do this via the Linear MCP `save_comment` tool.)

- [ ] **Step 6: Final sanity commit (if any docs changed)** — none expected; the plan ends here.
```
