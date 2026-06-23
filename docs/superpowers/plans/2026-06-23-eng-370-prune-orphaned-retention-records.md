# ENG-370 — Prune Orphaned Retention Records Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Periodically prune docker-backend retention records whose backing volumes have been confirmed absent for N consecutive sweeps, so orphaned records stop accumulating — without ever pruning a record whose absence is merely transient.

**Architecture:** A new periodic reconcile (`reconcileOrphanedRetentions`) runs as the last step of the existing `runRetentionSweep`. It is layered fail-closed: a `List()` error or a missing/unreadable volume root skips the whole pass; a record is pruned only after its volumes are observed absent on ≥ N consecutive sweeps (in-memory streak, reset on reappearance and on restart); deletion goes through the existing `DeleteIfActive` CAS. Two pure helpers (`volumeRootUnverifiable`, `allVolumesAbsent`) isolate the testable decisions.

**Tech Stack:** Go, bbolt (`shared.RetentionStore`), Prometheus (`promauto`), testify, `prometheus/client_golang/prometheus/testutil`.

**Design doc:** `docs/superpowers/specs/2026-06-22-eng-370-prune-orphaned-retention-records-design.md`

**Commit policy:** Each task ends with a commit step (standard TDD rhythm). Per the repo owner's "commit only when asked" rule, the executor should **pause for the owner's go-ahead before running each `git commit`** (or batch them at the end on request). All commits go on branch `felix/eng-370-prune-orphaned-retention-records`. End every commit message with the Co-Authored-By trailer.

**Global verification commands:**
- Build: `go build ./...`
- Lint: `golangci-lint run ./internal/backend/docker/...`
- Tests (race + short, per repo convention): `go test -race -short ./internal/backend/docker/...`

---

## File Structure

- **Modify** `internal/backend/docker/config.go` — add `RetentionOrphanConfirmations` field, default, validation.
- **Modify** `internal/backend/docker/config_test.go` — extend retention default + validation tests.
- **Modify** `internal/backend/docker/metrics.go` — add `retentionOrphansPrunedTotal`, `retentionOrphanSweepsSkippedTotal`.
- **Modify** `internal/backend/docker/backend.go` — add `orphanStreaks` field + init in `newBackend`.
- **Modify** `internal/backend/docker/restore.go` — add `reconcileOrphanedRetentions`, `volumeRootUnverifiable`, `allVolumesAbsent`; wire into `runRetentionSweep`.
- **Create** `internal/backend/docker/retention_orphan_test.go` — all new behavior tests.
- **Modify** `internal/backend/docker/README.md` — document the new config knob + metrics (doc-sync).

---

## Task 1: Config field, default, validation

**Files:**
- Modify: `internal/backend/docker/config.go` (field after `MaxRetainedLeasesPerTenant` ~line 221; default in `DefaultConfig` ~line 333; validation after the `MaxRetainedLeasesPerTenant` check ~line 499)
- Test: `internal/backend/docker/config_test.go` (`TestConfig_RetentionDefaults` ~line 611; `TestConfig_RetentionValidation` ~line 620)

- [ ] **Step 1: Write the failing tests**

In `config_test.go`, add one assertion line inside `TestConfig_RetentionDefaults` (after the `MaxRetainedLeasesPerTenant` assertion):

```go
	assert.Equal(t, 3, cfg.RetentionOrphanConfirmations)
```

In `TestConfig_RetentionValidation`, add a table entry alongside the other negative-value cases (inside the `tests := []struct{...}{ ... }` slice):

```go
		{
			name:    "negative retention_orphan_confirmations",
			mutate:  func(c *Config) { c.RetentionOrphanConfirmations = -1 },
			wantErr: "retention_orphan_confirmations must be non-negative",
		},
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/backend/docker/ -run 'TestConfig_RetentionDefaults|TestConfig_RetentionValidation' -v`
Expected: compile error (`cfg.RetentionOrphanConfirmations` undefined) — that counts as the failing state.

- [ ] **Step 3: Add the config field**

In `config.go`, immediately after the `MaxRetainedLeasesPerTenant` field (~line 221):

```go
	// RetentionOrphanConfirmations is the number of consecutive retention sweeps a
	// soft-deleted record must be observed with ALL its retained volumes missing
	// before the record is pruned (ENG-370). It is a SWEEP COUNT, not a duration:
	// the effective confirmation window is N × RetentionReapInterval (≈3h at the
	// default 1h interval), so shortening RetentionReapInterval proportionally
	// shrinks the window — re-tune N to keep a fixed grace. 0 is valid and
	// disables orphan pruning entirely (kill-switch); negative values are rejected
	// by Validate. Defaults to 3.
	RetentionOrphanConfirmations int `yaml:"retention_orphan_confirmations"`
```

- [ ] **Step 4: Add the default**

In `DefaultConfig()` (~line 333), after `RetentionReapInterval: time.Hour,`:

```go
		RetentionOrphanConfirmations: 3,
```

- [ ] **Step 5: Add the validation**

In `Validate()`, after the `MaxRetainedLeasesPerTenant` non-negative check (~line 499):

```go
	if c.RetentionOrphanConfirmations < 0 {
		return fmt.Errorf("retention_orphan_confirmations must be non-negative")
	}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test ./internal/backend/docker/ -run 'TestConfig_RetentionDefaults|TestConfig_RetentionValidation' -v`
Expected: PASS

- [ ] **Step 7: Commit** (pause for owner go-ahead)

```bash
git add internal/backend/docker/config.go internal/backend/docker/config_test.go
git commit -m "feat(retention): add retention_orphan_confirmations config knob (ENG-370)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Metrics

**Files:**
- Modify: `internal/backend/docker/metrics.go` (add to the `var ( ... )` block; namespace/subsystem consts already exist: `fred` / `docker_backend`)

No standalone test — these are declarative `promauto` registrations exercised by Task 3/4/5 tests. `promauto.NewCounter*` panics at init on a duplicate registration, so a successful `go build` + later tests are the verification.

- [ ] **Step 1: Add the reason constants**

In `metrics.go`, add a `const` block near the existing `phaseAdopt`/`phaseImageSetup` constants (which exist precisely "so the production instrumentation and the dashboard/tests cannot drift on a string typo"):

```go
// Skip reasons for retentionOrphanSweepsSkippedTotal (ENG-370). Kept as
// constants so the reconcile, the pre-init, and the tests cannot drift on a typo.
const (
	orphanSkipListError    = "list_error"    // volumes.List() failed (uncertain → fail-safe skip)
	orphanSkipMissingRoot  = "missing_root"  // volume data root absent/unreadable (fail-safe skip)
	orphanSkipRacedRestore = "raced_restore" // record claimed for restore between snapshot and delete
	orphanSkipDisabled     = "disabled"      // retention_orphan_confirmations == 0 (kill-switch)
)

// orphanSkipReasons is the closed reason set, used to pre-initialize the
// CounterVec series to 0 so absence/ratio alert queries return 0, not no-data.
var orphanSkipReasons = []string{orphanSkipListError, orphanSkipMissingRoot, orphanSkipRacedRestore, orphanSkipDisabled}
```

- [ ] **Step 2: Add the two metrics**

In `metrics.go`, inside the top-level `var (` block (e.g., after `idempotentOpsTotal` ~line 170-175), add:

```go
	// retentionOrphansPrunedTotal counts retention records pruned because all
	// their retained volumes were confirmed absent for >= N consecutive sweeps
	// (ENG-370).
	retentionOrphansPrunedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_orphans_pruned_total",
		Help:      "Total retention records pruned due to confirmed-absent backing volumes",
	})

	// retentionOrphanSweepsSkippedTotal counts orphan-reconcile passes/prunes
	// that bailed in the fail-safe direction, by reason. Without it, a sweep that
	// skips forever (e.g. a mis-mounted volume root) is indistinguishable from a
	// healthy "0 pruned" on the success counter alone. reason ∈ orphanSkipReasons.
	retentionOrphanSweepsSkippedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace,
		Subsystem: metricsSubsystem,
		Name:      "retention_orphan_sweeps_skipped_total",
		Help:      "Orphan-reconcile passes/prunes skipped in the fail-safe direction, by reason",
	}, []string{"reason"})
```

- [ ] **Step 3: Verify it builds**

Run: `go build ./internal/backend/docker/...`
Expected: success (no duplicate-metric panic — that only surfaces at test/run init, confirmed in Task 4).

- [ ] **Step 4: Commit** (pause for owner go-ahead)

```bash
git add internal/backend/docker/metrics.go
git commit -m "feat(metrics): add retention orphan prune + skip counters (ENG-370)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: State field + pure helpers (TDD)

**Files:**
- Modify: `internal/backend/docker/backend.go` (struct field after `retentionStore` ~line 111; init in `newBackend` literal ~line 494)
- Modify: `internal/backend/docker/restore.go` (add the two pure helpers near the other retention helpers)
- Create: `internal/backend/docker/retention_orphan_test.go`

- [ ] **Step 1: Write the failing helper tests**

Create `internal/backend/docker/retention_orphan_test.go`:

```go
package docker

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVolumeRootUnverifiable(t *testing.T) {
	// present root → verifiable (do not skip)
	assert.False(t, volumeRootUnverifiable(true, nil))
	// absent root (pathExists → false,nil) → unverifiable (skip)
	assert.True(t, volumeRootUnverifiable(false, nil))
	// unreadable root (non-ENOENT stat error → false,err) → unverifiable (skip).
	// This pins the branch so a future "IsNotExist-only" simplification fails.
	assert.True(t, volumeRootUnverifiable(false, errors.New("permission denied")))
}

func TestAllVolumesAbsent(t *testing.T) {
	present := map[string]bool{"fred-retained-u1-app-0": true}
	// every name present → not absent
	assert.False(t, allVolumesAbsent([]string{"fred-retained-u1-app-0"}, present))
	// a name missing → absent
	assert.True(t, allVolumesAbsent([]string{"fred-retained-u2-app-0"}, present))
	// mixed (one present) → not absent
	assert.False(t, allVolumesAbsent([]string{"fred-retained-u1-app-0", "fred-retained-u2-app-0"}, present))
	// empty name set → vacuously absent
	assert.True(t, allVolumesAbsent(nil, present))
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/backend/docker/ -run 'TestVolumeRootUnverifiable|TestAllVolumesAbsent' -v`
Expected: compile error (`volumeRootUnverifiable` / `allVolumesAbsent` undefined).

- [ ] **Step 3: Add the pure helpers**

In `restore.go`, near the other retention helpers (e.g., just above `reapExpiredRetentions`):

```go
// volumeRootUnverifiable reports whether a volume-root probe means the orphan
// reconcile must skip this pass (fail-safe). exists/statErr come from pathExists:
// an absent root (false,nil) OR any stat error (false,err — permission denied,
// EIO, …) is unverifiable. Deliberately NOT an os.IsNotExist-only check: an
// unreadable root is as uncertain as a missing one (kubelet #72257 hazard).
func volumeRootUnverifiable(exists bool, statErr error) bool {
	return statErr != nil || !exists
}

// allVolumesAbsent reports whether none of names is in the present set. An empty
// name set is vacuously absent (covers legacy zero-volume records).
func allVolumesAbsent(names []string, present map[string]bool) bool {
	for _, n := range names {
		if present[n] {
			return false
		}
	}
	return true
}
```

- [ ] **Step 4: Add the state field + init**

In `backend.go`, after the `retentionStore` field (~line 111):

```go
	// orphanStreaks counts consecutive retention sweeps an ACTIVE record's volumes
	// were all absent (ENG-370). Two invariants protect it:
	//   1. Single-writer confinement: touched ONLY by reconcileOrphanedRetentions,
	//      reachable only via runRetentionSweep on the single StartCleanupLoop
	//      goroutine (boot-eager retention work runs before that goroutine starts
	//      and never touches it) — so no mutex is needed. Do not add a second writer.
	//   2. In-memory by design: a restart resets it so a cold boot can never prune
	//      on its first sweep (the boot-before-mount fail-safe). Do not persist it.
	// Separately, the prune itself relies on DeleteIfActive's in-txn CAS as the
	// load-bearing guard against a concurrent restore (ClaimForRestore
	// active→restoring on a request goroutine) — do not "simplify" it into an
	// unconditional Delete.
	orphanStreaks map[string]int
```

In `newBackend`, in the `b := &Backend{ ... }` literal (~line 494, after `retentionStore: retentionStore,`):

```go
		orphanStreaks:    make(map[string]int),
```

Then, immediately after the `b := &Backend{...}` literal closes (before `b.stopCtx, b.stopCancel = ...` ~line 500), pre-initialize the skip-counter series so the spec's absence/ratio alerts query from 0 rather than no-data:

```go
	// Pre-initialize the orphan-skip counter series to 0 (ENG-370): the reason
	// set is closed and known, so alert queries see 0 instead of no-data before
	// the first skip event.
	for _, r := range orphanSkipReasons {
		retentionOrphanSweepsSkippedTotal.WithLabelValues(r).Add(0)
	}
```

- [ ] **Step 5: Run to verify helper tests pass**

Run: `go test ./internal/backend/docker/ -run 'TestVolumeRootUnverifiable|TestAllVolumesAbsent' -v`
Expected: PASS

- [ ] **Step 6: Commit** (pause for owner go-ahead)

```bash
git add internal/backend/docker/backend.go internal/backend/docker/restore.go internal/backend/docker/retention_orphan_test.go
git commit -m "feat(retention): orphan-streak state + pure reconcile helpers (ENG-370)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `reconcileOrphanedRetentions` + behavior tests (TDD)

**Files:**
- Modify: `internal/backend/docker/restore.go` (add `reconcileOrphanedRetentions` near the helpers)
- Modify: `internal/backend/docker/retention_orphan_test.go`

This task implements the reconcile and its full behavior suite. Build the test harness once, then add behaviors.

- [ ] **Step 1: Write the failing behavior tests**

Append to `internal/backend/docker/retention_orphan_test.go`. First extend the import block to (note: `sync` and `context` are added later in Task 5 when their tests need them — do not add them yet or the build fails with "imported and not used"):

```go
import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/manifest-network/fred/internal/backend/shared"
)
```

Then add the harness + tests:

```go
// newOrphanReconcileBackend builds a Backend with a real retention store, a
// controllable volume manager, and orphan pruning enabled (N=confirmations).
// presentVolumes is what volumes.List() returns; listErr (if set) makes it fail.
// rootExists controls whether cfg.VolumeDataPath points at a real dir.
func newOrphanReconcileBackend(t *testing.T, confirmations int, rootExists bool, presentVolumes []string, listErr error) (*Backend, *shared.RetentionStore) {
	t.Helper()
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.cfg.RetentionOrphanConfirmations = confirmations
	if rootExists {
		b.cfg.VolumeDataPath = t.TempDir() // exists → G2 passes
	} else {
		b.cfg.VolumeDataPath = filepath.Join(t.TempDir(), "missing") // absent → G2 skips
	}
	b.orphanStreaks = make(map[string]int)
	b.volumes = &mockVolumeManager{
		ListFn: func() ([]string, error) {
			if listErr != nil {
				return nil, listErr
			}
			return presentVolumes, nil
		},
	}
	s, err := shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: filepath.Join(t.TempDir(), "retention.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	b.retentionStore = s
	return b, s
}

func putActiveRetention(t *testing.T, s *shared.RetentionStore, lease string, volumeNames []string) {
	t.Helper()
	require.NoError(t, s.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   lease,
		Tenant:              "t1",
		Status:              shared.RetentionStatusActive,
		RetainedVolumeNames: volumeNames,
		CreatedAt:           time.Now(),
	}))
}

// Test #1 + #12: absent volumes prune exactly at sweep N; present ones never do.
func TestReconcileOrphaned_PrunesAfterNSweeps(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 3, true, []string{"fred-retained-uB-app-0"}, nil)
	putActiveRetention(t, s, "uA", []string{"fred-retained-uA-app-0"}) // absent
	putActiveRetention(t, s, "uB", []string{"fred-retained-uB-app-0"}) // present

	before := testutil.ToFloat64(retentionOrphansPrunedTotal)

	// Sweeps 1 and 2: not yet confirmed.
	for i := 0; i < 2; i++ {
		pruned, err := b.reconcileOrphanedRetentions()
		require.NoError(t, err)
		assert.Equal(t, 0, pruned)
		got, _ := s.Get("uA")
		assert.NotNil(t, got, "uA must survive before N sweeps")
	}
	// Sweep 3: confirmed → pruned.
	pruned, err := b.reconcileOrphanedRetentions()
	require.NoError(t, err)
	assert.Equal(t, 1, pruned)

	goneA, _ := s.Get("uA")
	assert.Nil(t, goneA, "uA pruned after N sweeps")
	keptB, _ := s.Get("uB")
	assert.NotNil(t, keptB, "uB (present volume) never pruned")
	assert.NotContains(t, b.orphanStreaks, "uB", "present-volume record must not accumulate a streak")
	assert.Equal(t, before+1, testutil.ToFloat64(retentionOrphansPrunedTotal))
}

// Test #3: a volume reappearing mid-streak resets confirmation.
func TestReconcileOrphaned_ReappearanceResetsStreak(t *testing.T) {
	present := []string{} // start absent
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.cfg.RetentionOrphanConfirmations = 3
	b.cfg.VolumeDataPath = t.TempDir()
	b.orphanStreaks = make(map[string]int)
	b.volumes = &mockVolumeManager{ListFn: func() ([]string, error) { return present, nil }}
	s, err := shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: filepath.Join(t.TempDir(), "retention.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	b.retentionStore = s
	putActiveRetention(t, s, "uA", []string{"fred-retained-uA-app-0"})

	_, _ = b.reconcileOrphanedRetentions() // streak 1
	_, _ = b.reconcileOrphanedRetentions() // streak 2
	present = []string{"fred-retained-uA-app-0"} // volume reappears
	_, _ = b.reconcileOrphanedRetentions()       // streak reset → 0
	present = []string{}                          // absent again
	pruned, err := b.reconcileOrphanedRetentions() // streak 1, NOT >= 3
	require.NoError(t, err)
	assert.Equal(t, 0, pruned)
	got, _ := s.Get("uA")
	assert.NotNil(t, got, "reset streak must prevent prune")
}

// Test #4: a restoring record with absent volumes is never pruned.
func TestReconcileOrphaned_SkipsRestoringRecords(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, true, nil, nil) // N=1: would prune immediately if active
	require.NoError(t, s.Put(shared.RetentionEntry{
		OriginalLeaseUUID:   "uR",
		Tenant:              "t1",
		Status:              shared.RetentionStatusRestoring,
		NewLeaseUUID:        "uNew",
		RetainedVolumeNames: []string{"fred-retained-uR-app-0"},
		CreatedAt:           time.Now(),
	}))
	pruned, err := b.reconcileOrphanedRetentions()
	require.NoError(t, err)
	assert.Equal(t, 0, pruned)
	got, _ := s.Get("uR")
	assert.NotNil(t, got, "restoring record must never be pruned")
}

// Test #5: a missing volume root skips the whole pass (fail-safe) forever.
func TestReconcileOrphaned_MissingRootSkips(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, false /*root missing*/, nil, nil)
	putActiveRetention(t, s, "uA", []string{"fred-retained-uA-app-0"})
	before := testutil.ToFloat64(retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipMissingRoot))

	for i := 0; i < 5; i++ {
		pruned, err := b.reconcileOrphanedRetentions()
		require.NoError(t, err)
		assert.Equal(t, 0, pruned)
	}
	got, _ := s.Get("uA")
	assert.NotNil(t, got, "missing root must prevent any prune")
	assert.Equal(t, before+5, testutil.ToFloat64(retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipMissingRoot)))
}

// Test #7: a List() error skips the pass (fail-safe) and surfaces the error.
func TestReconcileOrphaned_ListErrorSkips(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, true, nil, errors.New("list boom"))
	putActiveRetention(t, s, "uA", []string{"fred-retained-uA-app-0"})
	before := testutil.ToFloat64(retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipListError))

	pruned, err := b.reconcileOrphanedRetentions()
	require.Error(t, err)
	assert.Equal(t, 0, pruned)
	got, _ := s.Get("uA")
	assert.NotNil(t, got, "list error must prevent prune")
	assert.Equal(t, before+1, testutil.ToFloat64(retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipListError)))
}

// Test #9: an empty-name (legacy zero-volume) record prunes after N regardless of root.
func TestReconcileOrphaned_EmptyNamesPruned(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, true, nil, nil)
	putActiveRetention(t, s, "uLegacy", nil) // no volume names
	pruned, err := b.reconcileOrphanedRetentions()
	require.NoError(t, err)
	assert.Equal(t, 1, pruned)
	got, _ := s.Get("uLegacy")
	assert.Nil(t, got, "legacy zero-volume record pruned")
}

// Test #10: a volume-bearing record under an UNCONFIGURED root is never pruned.
func TestReconcileOrphaned_UnconfiguredRootSkipsVolumeRecords(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, nil)
	b.cfg.RetentionOrphanConfirmations = 1
	b.cfg.VolumeDataPath = "" // noop manager / unconfigured
	b.orphanStreaks = make(map[string]int)
	b.volumes = &noopVolumeManager{} // List() → (nil,nil)
	s, err := shared.NewRetentionStore(shared.RetentionStoreConfig{DBPath: filepath.Join(t.TempDir(), "retention.db")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	b.retentionStore = s
	putActiveRetention(t, s, "uV", []string{"fred-retained-uV-app-0"}) // has volumes

	for i := 0; i < 3; i++ {
		pruned, err := b.reconcileOrphanedRetentions()
		require.NoError(t, err)
		assert.Equal(t, 0, pruned)
	}
	got, _ := s.Get("uV")
	assert.NotNil(t, got, "volume-bearing record unverifiable without a root → never pruned")
}

// Test #11: N=0 is the kill-switch — never prunes, records the disabled skip.
func TestReconcileOrphaned_DisabledKillSwitch(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 0 /*disabled*/, true, nil, nil)
	putActiveRetention(t, s, "uA", []string{"fred-retained-uA-app-0"})
	before := testutil.ToFloat64(retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipDisabled))

	pruned, err := b.reconcileOrphanedRetentions()
	require.NoError(t, err)
	assert.Equal(t, 0, pruned)
	got, _ := s.Get("uA")
	assert.NotNil(t, got, "kill-switch must prevent prune")
	assert.Equal(t, before+1, testutil.ToFloat64(retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipDisabled)))
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/backend/docker/ -run 'TestReconcileOrphaned' -v`
Expected: compile error (`reconcileOrphanedRetentions` undefined).

- [ ] **Step 3: Implement `reconcileOrphanedRetentions`**

In `restore.go`, after the helpers added in Task 3:

```go
// reconcileOrphanedRetentions prunes ACTIVE retention records whose every
// RetainedVolumeName has been absent from the node for >= N consecutive periodic
// sweeps (ENG-370 — records orphaned when their backing volumes vanish out-of-band).
//
// Fail-safe by construction: any uncertainty skips the whole pass and resets the
// in-memory confirmation streaks, because the gated action is DELETION — discarding
// a record throws away the only restore handle for a volume that may merely be
// transiently unlisted (a missing/unreadable volume root makes listVolumeIDs return
// empty-with-no-error). Streaks are in-memory so a cold restart can never prune on
// its first sweep (boot-before-mount fail-safe). Returns the number pruned.
//
// No ctx: the prune does no context-bound IO (volumes are already gone, so there is
// nothing to Destroy), unlike reapExpiredRetentions which Destroys under ctx.
func (b *Backend) reconcileOrphanedRetentions() (int, error) {
	if b.retentionStore == nil {
		return 0, nil
	}
	n := b.cfg.RetentionOrphanConfirmations
	if n <= 0 {
		// Kill-switch (0 = disabled). Log once per sweep so a flip-to-0 is not silent.
		b.logger.Info("orphan retention reconcile disabled (retention_orphan_confirmations=0)")
		retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipDisabled).Inc()
		return 0, nil
	}

	// G2 — warm-view gate. A configured-but-absent/unreadable volume root makes the
	// volume enumeration untrustworthy. Skip + reset streaks. An unconfigured root
	// (noop manager) is allowed through here; the per-record verifiability check below
	// handles it.
	rootConfigured := b.cfg.VolumeDataPath != ""
	if rootConfigured {
		exists, statErr := pathExists(b.cfg.VolumeDataPath)
		if volumeRootUnverifiable(exists, statErr) {
			b.logger.Warn("orphan retention reconcile skipped: volume data root absent or unreadable (fail-safe)",
				"path", b.cfg.VolumeDataPath, "error", statErr)
			b.orphanStreaks = map[string]int{}
			retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipMissingRoot).Inc()
			return 0, nil
		}
	}

	// G1 — a failed enumeration is uncertainty, not "no volumes". Skip + reset.
	// No local log: returning err makes StartCleanupLoop log it once (matches the
	// sibling reapExpiredRetentions, which bare-returns store errors). The metric is
	// the precise alerting signal.
	existing, err := b.volumes.List()
	if err != nil {
		b.orphanStreaks = map[string]int{}
		retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipListError).Inc()
		return 0, err
	}
	present := make(map[string]bool, len(existing))
	for _, v := range existing {
		present[v] = true
	}

	recs, err := b.retentionStore.List()
	if err != nil {
		return 0, err
	}

	next := make(map[string]int, len(b.orphanStreaks))
	var pruned int
	for _, e := range recs {
		if e.Status != shared.RetentionStatusActive {
			continue // never touch a restoring record (volumes renamed away → would look absent)
		}
		if !allVolumesAbsent(e.RetainedVolumeNames, present) {
			continue // a volume is present → not orphaned → streak resets (omit from next)
		}
		if !rootConfigured && len(e.RetainedVolumeNames) > 0 {
			continue // unverifiable without a configured root → never prune
		}
		streak := b.orphanStreaks[e.OriginalLeaseUUID] + 1
		if streak < n {
			next[e.OriginalLeaseUUID] = streak // not yet confirmed; carry forward
			continue
		}
		// Confirmed across >= n consecutive sweeps. Prune via the ACTIVE-only CAS so a
		// concurrent restore (active→restoring) is never clobbered. Volumes are already
		// gone — nothing to Destroy.
		_, deleted, derr := b.retentionStore.DeleteIfActive(e.OriginalLeaseUUID)
		switch {
		case derr != nil:
			b.logger.Error("orphan retention reconcile: delete failed", "lease_uuid", e.OriginalLeaseUUID, "error", derr)
			next[e.OriginalLeaseUUID] = streak // keep streak; retry next sweep
		case !deleted:
			retentionOrphanSweepsSkippedTotal.WithLabelValues(orphanSkipRacedRestore).Inc() // raced into restore; leave it
		default:
			pruned++
			retentionOrphansPrunedTotal.Inc()
			b.logger.Info("pruned orphaned retention record (all retained volumes confirmed absent)",
				"lease_uuid", e.OriginalLeaseUUID, "confirmations", streak)
		}
	}
	b.orphanStreaks = next
	return pruned, nil
}
```

- [ ] **Step 4: Run to verify all behavior tests pass**

Run: `go test ./internal/backend/docker/ -run 'TestReconcileOrphaned' -v`
Expected: PASS (all 8 behavior tests)

- [ ] **Step 5: Commit** (pause for owner go-ahead)

```bash
git add internal/backend/docker/restore.go internal/backend/docker/retention_orphan_test.go
git commit -m "feat(retention): reconcile + prune orphaned retention records (ENG-370)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Wire into the sweep + concurrent restore-race test + full verify

**Files:**
- Modify: `internal/backend/docker/restore.go` (`runRetentionSweep` ~lines 293-308)
- Modify: `internal/backend/docker/retention_orphan_test.go`

- [ ] **Step 1: Write the failing concurrent-race test**

Add `"sync"` to the test file's import block (used below), then append to `internal/backend/docker/retention_orphan_test.go`:

```go
// Test #8: a real restore (ClaimForRestore active→restoring) racing the reconcile's
// DeleteIfActive must never corrupt state — the record ends EITHER pruned (reconcile
// won) OR restoring under the new lease (restore won), never deleted-while-restoring.
//
// The post-state either/or assertion (testify) is the proof of correctness; -race is
// a regression guard for any future shared backend state (today both contended methods
// run inside bolt.DB.Update, which serializes them, so -race rarely fires). The loop
// re-creates a clean ACTIVE record each iteration so both bbolt-serialized interleavings
// recur within a single run; N=1 so one reconcile sweep attempts the delete.
func TestReconcileOrphaned_ConcurrentRestoreRace(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, true, nil /*all absent*/, nil)

	var sawPruned, sawRestored bool
	for i := 0; i < 100; i++ {
		require.NoError(t, s.Put(shared.RetentionEntry{
			OriginalLeaseUUID:   "uA",
			Tenant:              "t1",
			Status:              shared.RetentionStatusActive,
			RetainedVolumeNames: []string{"fred-retained-uA-app-0"},
			CreatedAt:           time.Now(),
		}))
		b.orphanStreaks = make(map[string]int) // start the streak fresh so N=1 deletes this sweep

		var wg sync.WaitGroup
		wg.Add(2)
		var claimErr error
		go func() { defer wg.Done(); _, _ = b.reconcileOrphanedRetentions() }()
		go func() { defer wg.Done(); _, claimErr = s.ClaimForRestore("uA", "uNew", time.Hour) }()
		wg.Wait()

		rec, err := s.Get("uA")
		require.NoError(t, err)
		if claimErr == nil {
			// Restore won the CAS: record survives in restoring state — never deleted-while-restoring.
			require.NotNil(t, rec, "restore claimed the record; reconcile must not have deleted it")
			assert.Equal(t, shared.RetentionStatusRestoring, rec.Status)
			assert.Equal(t, "uNew", rec.NewLeaseUUID)
			sawRestored = true
		} else {
			// Reconcile won: record pruned, claim observed it gone.
			assert.Nil(t, rec, "reconcile pruned the record; claim must have failed")
			sawPruned = true
		}
	}
	// Both interleavings preserve the invariant; over 100 iterations we expect each at
	// least once. Logged, not hard-asserted, to avoid scheduler-dependent flakiness.
	t.Logf("concurrent race: observed pruned=%v restored=%v", sawPruned, sawRestored)
}
```

- [ ] **Step 2: Run to verify it fails (reconcile not yet wired is irrelevant here; this test calls reconcile directly, so it should already pass IF Task 4 is in). Run it to confirm green before wiring:**

Run: `go test -race ./internal/backend/docker/ -run 'TestReconcileOrphaned_ConcurrentRestoreRace' -v`
Expected: PASS, no race detected. (If it fails, the CAS reuse in Task 4 is wrong — fix before proceeding.)

- [ ] **Step 3: Wire reconcile into `runRetentionSweep`**

In `restore.go`, in `runRetentionSweep`, replace the final restoring-reconcile loop + return:

```go
	for _, e := range recs {
		b.reconcileRestoring(ctx, e)
	}
	return nil
}
```

with:

```go
	for _, e := range recs {
		b.reconcileRestoring(ctx, e)
	}
	if _, err := b.reconcileOrphanedRetentions(); err != nil {
		return err
	}
	return nil
}
```

- [ ] **Step 4: Write a wiring test that drives the sweep end-to-end**

Append to `internal/backend/docker/retention_orphan_test.go`:

```go
// Wiring: runRetentionSweep invokes the orphan reconcile. With N=1 and an absent
// volume, a single sweep prunes the orphaned record.
func TestRunRetentionSweep_PrunesOrphans(t *testing.T) {
	b, s := newOrphanReconcileBackend(t, 1, true, nil /*absent*/, nil)
	b.cfg.RetentionMaxAge = 90 * 24 * time.Hour // keep the grace reaper a no-op for this record
	putActiveRetention(t, s, "uA", []string{"fred-retained-uA-app-0"})

	require.NoError(t, b.runRetentionSweep(context.Background()))

	got, _ := s.Get("uA")
	assert.Nil(t, got, "runRetentionSweep must prune the orphaned record")
}
```

Add `"context"` to the test file's import block.

- [ ] **Step 5: Run the wiring test**

Run: `go test ./internal/backend/docker/ -run 'TestRunRetentionSweep_PrunesOrphans' -v`
Expected: PASS

- [ ] **Step 6: Full package verification**

Run: `go build ./... && golangci-lint run ./internal/backend/docker/... && go test -race -short ./internal/backend/docker/...`
Expected: build clean, lint clean, all tests pass (no data race).

- [ ] **Step 7: Commit** (pause for owner go-ahead)

```bash
git add internal/backend/docker/restore.go internal/backend/docker/retention_orphan_test.go
git commit -m "feat(retention): wire orphan reconcile into the periodic sweep (ENG-370)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Documentation sync (`README.md`)

The repo keeps `internal/backend/docker/README.md` in sync with retention config/behavior (e.g. commits `cc944dc`, `155b1f5`). Mirror the sibling retention knobs.

**Files:**
- Modify: `internal/backend/docker/README.md` (config table ~line 89; soft-delete narrative ~line 207)

- [ ] **Step 1: Add the config-table row**

In the "Soft-delete & Retention" table, immediately after the `MaxRetainedLeasesPerTenant` row:

```markdown
| RetentionOrphanConfirmations | `retention_orphan_confirmations` | int | `3` | Number of consecutive retention sweeps a soft-deleted record must be observed with **all** its retained volumes missing before the record is pruned (ENG-370). Catches records orphaned when their backing volumes vanish out-of-band (host/docker churn, `docker volume prune`, data-root reset) so they don't linger for the full grace window. Fail-safe: a sweep that cannot enumerate volumes, or finds the volume root absent/unreadable, skips rather than pruning. This is a **sweep count**, not a duration — the effective confirmation window is `N × retention_reap_interval` (≈3h at the 1h default). `0` disables orphan pruning entirely (kill-switch). |
```

- [ ] **Step 2: Add the narrative point**

In the "## Soft-delete & Restore" numbered list, after point 4 (the grace reaper), add:

```markdown
5. If a retained lease's backing volumes disappear **out-of-band** (host/docker churn, `docker volume prune`, a data-root reset on redeploy) while its record survives, the periodic sweep prunes the now-orphaned record after it is observed fully volume-less for `retention_orphan_confirmations` consecutive sweeps (default 3). This keeps dead records from accumulating for the full grace window. The prune is fail-safe — a sweep that errors listing volumes, or finds the volume root absent/unreadable, skips entirely rather than risk pruning a record whose volumes are merely transiently unavailable. Observable via `fred_docker_backend_retention_orphans_pruned_total` and `fred_docker_backend_retention_orphan_sweeps_skipped_total{reason}`.
```

- [ ] **Step 3: Verify the docs render and reference real symbols**

Run: `grep -n "retention_orphan_confirmations\|retention_orphans_pruned_total\|retention_orphan_sweeps_skipped_total" internal/backend/docker/README.md`
Expected: the new row + narrative point appear; metric names match `metrics.go`.

- [ ] **Step 4: Commit** (pause for owner go-ahead)

```bash
git add internal/backend/docker/README.md
git commit -m "docs(retention): document orphan-record pruning knob + metrics (ENG-370)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Notes on intentional test scoping

- **Spec test #6 (unreadable, non-ENOENT root)** is covered at the helper level (`TestVolumeRootUnverifiable(false, err)`), not at the reconcile level: the white-box harness can only point `VolumeDataPath` at a real tempdir or an ENOENT path, and the btrfs CI runs as root (a `chmod 0o000` gate would be ineffective). The helper test fully pins the non-ENOENT branch.
- **The `derr != nil` delete-retry branch** is intentionally left untested: `retentionStore` is a concrete `*shared.RetentionStore` with no interface seam, and forcing a `DeleteIfActive` error would need a fragile mid-test DB close or a test-only interface — which conflicts with the repo's no-test-only-seams preference. The branch is a 2-line log + streak-carry; skipped by design.

---

## Spec coverage check

| Spec element | Task |
|---|---|
| Config `retention_orphan_confirmations` (default 3, 0=kill-switch, validation) | 1 |
| Metrics `retention_orphans_pruned_total`, `retention_orphan_sweeps_skipped_total{reason}` | 2, 4 |
| `orphanStreaks` in-memory state + invariant comment | 3 |
| Pure helpers `volumeRootUnverifiable` (G2, incl. non-ENOENT) + `allVolumesAbsent` | 3 |
| G1 list-error skip + reset + metric | 4 (TestReconcileOrphaned_ListErrorSkips) |
| G2 missing/unreadable root skip + reset + metric | 3 (unit), 4 (TestReconcileOrphaned_MissingRootSkips) |
| G3 N-sweep confirmation; reappearance resets | 4 (PrunesAfterNSweeps, ReappearanceResetsStreak) |
| G4 ACTIVE-only CAS; restoring untouched; raced restore | 4 (SkipsRestoringRecords), 5 (ConcurrentRestoreRace) |
| Confirmable-only (empty names prune; unconfigured-root volume records skip) | 4 (EmptyNamesPruned, UnconfiguredRootSkipsVolumeRecords) |
| Kill-switch (N=0) | 4 (DisabledKillSwitch) |
| Wiring into periodic `runRetentionSweep` only (not boot) | 5 (RunRetentionSweep_PrunesOrphans) |
| Skip-counter series pre-initialized to 0 (alertable from t=0) | 3 (newBackend pre-init loop) |
| `reason` label constants (no string drift) | 2 |
| Doc-sync: README config row + behavior note + metrics | 6 |

All spec requirements map to a task. No boot-path wiring (intentional). No admin purge (deferred). No record-creation change (claim #2 already satisfied).
