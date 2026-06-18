# ENG-335 — Reconciler Prune Grace Window + Deprovision Fail-Safe — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the reconciler from pruning placements for leases that provisioned during a slow sweep, and make lease-close deprovision the lease's actual backend (or fail safe across all backends) instead of a phantom no-op against the default backend.

**Architecture:** Two independent changes in package `internal/provisioner`. **Part A** (deprovision fail-safe) removes the SKU→default-backend guess from `ProvisionOrchestrator.Deprovision` and falls back to an idempotent all-backends sweep. **Part B** (prune grace window) adds a first-seen `SetAt` timestamp to placement records and makes the reconciler's sole pruner skip placements younger than `2 × reconcile interval`, using the sweep-start time as the reference clock.

**Tech Stack:** Go 1.25, `go.etcd.io/bbolt` (placement store), `log/slog`, `testify` (assert/require), existing `mockPlacementStore`/`mockManagerBackend`/`mockBackendRouter`/`mockReconcilerBackend` test doubles, `chaintest.MockClient`.

---

## File Structure

| File | Responsibility | Change |
| --- | --- | --- |
| `internal/provisioner/orchestrator.go` | Provision/deprovision coordination | `Deprovision` drops `skuHint`, removes Case 2 (SKU→default), sweeps all backends on unresolved, summary log |
| `internal/provisioner/handler_set.go` | Watermill event handlers | `processLeaseClose` drops the now-dead SKU-hint fetch; calls `Deprovision(ctx, uuid)` |
| `internal/provisioner/placement/store.go` | bbolt-backed lease→backend index | Add `SetAt`, clock seam, JSON record encoding w/ legacy load |
| `internal/provisioner/interfaces.go` | `PlacementStore` interface | Add `SetAt(leaseUUID) (time.Time, bool)` |
| `internal/provisioner/reconciler.go` | Reconcile loop + sole placement pruner | `cleanupOrphanedPlacements` gains `now` param + grace gate |
| `internal/provisioner/placement/store_test.go` | Store unit tests | New tests: SetAt round-trip, legacy load, SetBatch preserve, clock |
| `internal/provisioner/orchestrator_test.go` | Orchestrator unit tests | Drop arg; remove/rewrite Case-2 tests; add ENG-335 regression test |
| `internal/provisioner/handler_set_test.go` | `mockPlacementStore` + handler tests | Add `SetAt`/`setWithTime` to mock; adjust close tests |
| `internal/provisioner/reconciler_test.go` | Reconciler tests | Backdate `SetAt` in existing prune tests; add grace-window test |

Part A (Task 1) and Part B (Tasks 2–4) are independent; Task 5 verifies the whole.

---

## Task 1: Part A — Deprovision fail-safe (remove SKU→default guess)

**Files:**
- Modify: `internal/provisioner/orchestrator.go:204-307` (`Deprovision`)
- Modify: `internal/provisioner/handler_set.go:172-194` (`processLeaseClose`)
- Test: `internal/provisioner/orchestrator_test.go` (deprovision tests)

- [ ] **Step 1: Write the failing regression test**

Add to `internal/provisioner/orchestrator_test.go` (after `TestOrchestrator_Deprovision_PartialBackendSuccess`):

```go
// TestOrchestrator_Deprovision_PlacementMissing_SweepsAllBackends is the ENG-335
// regression guard. With no placement and no in-flight entry, Deprovision must
// NOT route to a single default backend (the old SKU-route → defaultBackend
// phantom that reported success against docker-1 while the real volume on
// docker-2 was stranded). It must sweep ALL backends so the real holder is
// torn down.
func TestOrchestrator_Deprovision_PlacementMissing_SweepsAllBackends(t *testing.T) {
	mb1 := &mockManagerBackend{name: "docker-1"} // default/first — did NOT hold the lease
	mb2 := &mockManagerBackend{name: "docker-2"} // the actual holder
	mb3 := &mockManagerBackend{name: "docker-3"}
	router := &mockBackendRouter{
		// Route() would have returned the default (docker-1) — the phantom path.
		routeFn:    func(sku string) backend.Backend { return mb1 },
		backendsFn: func() []backend.Backend { return []backend.Backend{mb1, mb2, mb3} },
	}
	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, NewInFlightTracker(), &mockPlacementStore{})

	require.NoError(t, orch.Deprovision(context.Background(), "lease-1"))

	for _, mb := range []*mockManagerBackend{mb1, mb2, mb3} {
		mb.mu.Lock()
		assert.Equal(t, []string{"lease-1"}, mb.deprovisionCalls, "backend %s must be swept", mb.name)
		mb.mu.Unlock()
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/provisioner/ -run TestOrchestrator_Deprovision_PlacementMissing_SweepsAllBackends`
Expected: COMPILE FAIL — `not enough arguments in call to orch.Deprovision` (signature still takes `skuHint`). This confirms the test targets the new signature.

- [ ] **Step 3: Rewrite `Deprovision` (drop `skuHint`, remove Case 2, sweep-all fallback)**

Replace the whole body of `Deprovision` in `internal/provisioner/orchestrator.go` (currently `func (o *ProvisionOrchestrator) Deprovision(ctx context.Context, leaseUUID string, skuHint string) error { ... }`, lines ~204-307) with:

```go
// Deprovision tears down a lease's backend resources. The backend is resolved
// POSITIVELY — from the placement record, then the in-flight tracker. It never
// guesses a default backend from the SKU: in a multi-backend pool a SKU is not
// pinned to one backend, so a guessed deprovision is a phantom no-op that
// reports success while stranding the real volume on another backend (ENG-335).
// When the backend cannot be positively resolved, all backends are swept;
// deprovision is idempotent, so the real holder is torn down and the rest are
// harmless no-ops.
//
// Returns nil on success or if the lease was not provisioned anywhere.
// Returns an error only if every attempted deprovision failed.
func (o *ProvisionOrchestrator) Deprovision(ctx context.Context, leaseUUID string) error {
	provision, wasInFlight := o.tracker.PopInFlight(leaseUUID)

	var backendClient backend.Backend

	// Case 0: placement store (most reliable for completed provisions).
	if o.placementStore != nil {
		if placedBackend := o.placementStore.Get(leaseUUID); placedBackend != "" {
			backendClient = o.router.GetBackendByName(placedBackend)
			if backendClient != nil {
				slog.Debug("routing deprovision by placement",
					"lease_uuid", leaseUUID, "backend", placedBackend)
			} else {
				slog.Warn("placement backend not found, will sweep all backends",
					"lease_uuid", leaseUUID, "backend_name", placedBackend)
			}
		}
	}

	// Case 1: in-flight tracked backend.
	if backendClient == nil && wasInFlight && provision.Backend != "" {
		backendClient = o.router.GetBackendByName(provision.Backend)
		if backendClient == nil {
			slog.Warn("in-flight backend not found, will sweep all backends",
				"lease_uuid", leaseUUID, "backend_name", provision.Backend)
		}
	}

	if backendClient != nil {
		if err := backendClient.Deprovision(ctx, leaseUUID); err != nil {
			slog.Error("failed to deprovision",
				"lease_uuid", leaseUUID, "backend", backendClient.Name(), "error", err)
			return fmt.Errorf("%w: lease %s: %w", ErrDeprovisionFailed, leaseUUID, err)
		}
		// Placement is intentionally NOT deleted here (ENG-333). It is a derived
		// index of where the lease's data lives; if the backend retained the
		// volumes, the placement must survive close so a restore can route to it.
		// The reconciler is the sole pruner (cleanupOrphanedPlacements).
		slog.Info("deprovisioned successfully",
			"lease_uuid", leaseUUID, "backend", backendClient.Name())
		return nil
	}

	// Fallback: backend could not be positively resolved → sweep all backends.
	// Idempotent, so the holder is torn down and the rest no-op. We deliberately
	// do NOT emit a per-backend "deprovisioned successfully" here — that
	// phantom-success line (against a backend that never held the lease) is what
	// made ENG-335 hard to diagnose. One summary line names the outcome instead.
	backends := o.router.Backends()
	var lastErr error
	swept := make([]string, 0, len(backends))
	failed := make([]string, 0)
	for _, b := range backends {
		if err := b.Deprovision(ctx, leaseUUID); err != nil {
			lastErr = err
			failed = append(failed, b.Name())
		} else {
			swept = append(swept, b.Name())
		}
	}
	slog.Warn("deprovision swept all backends (placement unresolved, ENG-335)",
		"lease_uuid", leaseUUID,
		"swept_ok_or_noop", swept,
		"failed", failed,
	)
	// Placement is intentionally NOT deleted here (ENG-333); see resolved path.
	if len(swept) == 0 && lastErr != nil {
		return fmt.Errorf("%w: lease %s: %w", ErrDeprovisionFailed, leaseUUID, lastErr)
	}
	return nil
}
```

- [ ] **Step 4: Update the production caller in `processLeaseClose`**

In `internal/provisioner/handler_set.go`, delete the now-dead SKU-hint fetch (lines ~172-184, the `var skuHint string` block through the `skuHint = ExtractRoutingSKU(lease)` else-branch — this also removes an unnecessary chain `GetLease` RPC on every close) and change the final call. The block to DELETE is exactly:

```go
	// Get SKU hint from chain for routing if lease is not in-flight.
	// The orchestrator will try in-flight tracking first, then fall back to SKU hint.
	var skuHint string
	lease, err := h.deps.ChainClient.GetLease(msg.Context(), event.LeaseUUID)
	if err != nil {
		slog.Warn("failed to fetch lease details for deprovision routing",
			"lease_uuid", event.LeaseUUID,
			"error", err,
		)
		// Continue without SKU hint - orchestrator will try all backends
	} else if lease != nil {
		skuHint = ExtractRoutingSKU(lease)
	}

```

Then change the delegation line from:

```go
	return h.deps.Orchestrator.Deprovision(msg.Context(), event.LeaseUUID, skuHint)
```

to:

```go
	return h.deps.Orchestrator.Deprovision(msg.Context(), event.LeaseUUID)
```

(`ExtractRoutingSKU` and `ChainClient.GetLease` remain used elsewhere — `handler_set.go:81,329,442`, `orchestrator.go:52`, `reconciler.go:423` — so no imports go unused. The ENG-329 comment block immediately below stays.)

- [ ] **Step 5: Migrate existing deprovision tests in `orchestrator_test.go`**

(a) **Drop the second arg** from these call sites (behavior unchanged — Case 0/1/all-backends): lines for `TestOrchestrator_Deprovision_ViaInFlightTracking`, `_FallbackAllBackends`, `_AllBackendsFail`, `_PartialBackendSuccess`, `_ViaPlacement`, `_PlacementTakesPriorityOverInFlight`, `_FallbackAllBackends_KeepsPlacement`, `_KeepsPlacement`. Change `orch.Deprovision(context.Background(), "lease-1", "")` → `orch.Deprovision(context.Background(), "lease-1")` and `orch.Deprovision(context.Background(), "lease-1", "sku-1")` → `orch.Deprovision(context.Background(), "lease-1")`.

(b) **Delete** these two tests entirely — they assert the removed Case-2 SKU-routing-for-deprovision behavior, now subsumed by the all-backends sweep:
- `TestOrchestrator_Deprovision_ViaSKURouting`
- `TestOrchestrator_Deprovision_SKURoutingFails`

(c) **Rewrite** `TestOrchestrator_Deprovision_InFlightBackendNotFound_FallsToSKU` → in-flight backend gone now sweeps all backends:

```go
func TestOrchestrator_Deprovision_InFlightBackendNotFound_FallsToAllBackends(t *testing.T) {
	mb := &mockManagerBackend{name: "real-backend"}
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend { return nil }, // in-flight backend gone
		backendsFn:         func() []backend.Backend { return []backend.Backend{mb} },
	}
	tracker := NewInFlightTracker()
	tracker.TrackInFlight("lease-1", "t", testItems("sku-1"), "deleted-backend")

	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, nil)

	require.NoError(t, orch.Deprovision(context.Background(), "lease-1"))

	mb.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mb.deprovisionCalls)
	mb.mu.Unlock()
}
```

(d) **Rewrite** `TestOrchestrator_Deprovision_StalePlacement_FallsToSKU` → stale placement sweeps all backends, placement still survives:

```go
func TestOrchestrator_Deprovision_StalePlacement_FallsToAllBackends(t *testing.T) {
	mb := &mockManagerBackend{name: "real-backend"}
	router := &mockBackendRouter{
		getBackendByNameFn: func(name string) backend.Backend {
			if name == "real-backend" {
				return mb
			}
			return nil // "removed-backend" is no longer configured
		},
		backendsFn: func() []backend.Backend { return []backend.Backend{mb} },
	}
	tracker := NewInFlightTracker()
	ps := &mockPlacementStore{}
	ps.Set("lease-1", "removed-backend") // stale placement → GetBackendByName misses

	orch := NewProvisionOrchestrator("prov-1", "http://localhost:8080", router, tracker, ps)

	require.NoError(t, orch.Deprovision(context.Background(), "lease-1"))

	mb.mu.Lock()
	assert.Equal(t, []string{"lease-1"}, mb.deprovisionCalls)
	mb.mu.Unlock()

	// ENG-333: stale placement survives; the reconciler prunes orphans later.
	assert.Equal(t, "removed-backend", ps.Get("lease-1"), "stale placement must survive deprovision (ENG-333)")
}
```

- [ ] **Step 6: Run the orchestrator tests**

Run: `go test ./internal/provisioner/ -run 'TestOrchestrator_Deprovision'`
Expected: PASS (including the new `_PlacementMissing_SweepsAllBackends`).

- [ ] **Step 7: Build the package and fix any handler_set close-test fallout**

Run: `go build ./... && go test ./internal/provisioner/ -run 'ProcessLeaseClose|LeaseClosed|LeaseExpired'`
Expected: PASS. If a close handler test asserted `ChainClient.GetLease` is called during close, update it to no longer expect that call (the SKU-hint fetch was removed). If no such assertion exists, no change needed.

- [ ] **Step 8: Commit**

```bash
git add internal/provisioner/orchestrator.go internal/provisioner/handler_set.go internal/provisioner/orchestrator_test.go internal/provisioner/handler_set_test.go
git commit -m "fix(eng-335): deprovision fails safe across all backends instead of guessing default

Close no longer SKU-routes a missing-placement lease to the default backend
(a phantom no-op that reported success against docker-1 while stranding the
real volume). It now sweeps all backends idempotently. Drops the now-dead
SKU-hint chain RPC from processLeaseClose.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Part B — Placement store `SetAt` timestamp

**Files:**
- Modify: `internal/provisioner/placement/store.go`
- Test: `internal/provisioner/placement/store_test.go`

- [ ] **Step 1: Write failing tests**

Add to `internal/provisioner/placement/store_test.go` (add `"encoding/json"`, `"time"`, and `bolt "go.etcd.io/bbolt"` to the imports):

```go
func TestStore_SetAt_RoundTrip(t *testing.T) {
	fixed := time.Date(2026, 6, 18, 17, 11, 15, 0, time.UTC)
	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath, WithClock(func() time.Time { return fixed }))
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.Set("lease-1", "backend-a"))

	got, ok := s.SetAt("lease-1")
	require.True(t, ok)
	assert.True(t, got.Equal(fixed), "SetAt should equal the injected clock time")

	_, ok = s.SetAt("missing")
	assert.False(t, ok)
}

func TestStore_SetAt_PersistsAcrossReopen(t *testing.T) {
	fixed := time.Date(2026, 6, 18, 17, 11, 15, 0, time.UTC)
	dbPath := filepath.Join(t.TempDir(), "placements.db")

	s1, err := NewStore(dbPath, WithClock(func() time.Time { return fixed }))
	require.NoError(t, err)
	require.NoError(t, s1.Set("lease-1", "backend-a"))
	require.NoError(t, s1.Close())

	s2, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s2.Close()

	assert.Equal(t, "backend-a", s2.Get("lease-1"))
	got, ok := s2.SetAt("lease-1")
	require.True(t, ok)
	assert.True(t, got.Equal(fixed))
}

func TestStore_LegacyRawValue_LoadsWithZeroSetAt(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "placements.db")

	// Write a legacy raw backend-name value (pre-ENG-335 format) directly.
	db, err := bolt.Open(dbPath, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("placements"))
		if err != nil {
			return err
		}
		return b.Put([]byte("legacy-lease"), []byte("backend-legacy"))
	}))
	require.NoError(t, db.Close())

	s, err := NewStore(dbPath)
	require.NoError(t, err)
	defer s.Close()

	assert.Equal(t, "backend-legacy", s.Get("legacy-lease"), "legacy backend name must load")
	got, ok := s.SetAt("legacy-lease")
	require.True(t, ok)
	assert.True(t, got.IsZero(), "legacy record must have zero SetAt (immediately prunable)")
}

func TestStore_SetBatch_PreservesExistingSetAt(t *testing.T) {
	t0 := time.Date(2026, 6, 18, 10, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 18, 11, 0, 0, 0, time.UTC)
	clk := &fakeClock{now: t0}

	dbPath := filepath.Join(t.TempDir(), "placements.db")
	s, err := NewStore(dbPath, WithClock(clk.Now))
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, s.Set("lease-1", "backend-a")) // SetAt = t0

	clk.now = t1
	require.NoError(t, s.SetBatch(map[string]string{
		"lease-1": "backend-a", // existing → SetAt preserved at t0
		"lease-2": "backend-b", // new → SetAt = t1
	}))

	at1, _ := s.SetAt("lease-1")
	at2, _ := s.SetAt("lease-2")
	assert.True(t, at1.Equal(t0), "existing placement SetAt must be preserved (not reset by sync)")
	assert.True(t, at2.Equal(t1), "new placement SetAt must be the sync time")
}

// fakeClock is a trivial settable clock for deterministic time in tests.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/provisioner/placement/ -run 'SetAt|Legacy|PreservesExisting'`
Expected: COMPILE FAIL — `undefined: WithClock`, `s.SetAt undefined`.

- [ ] **Step 3: Implement the store changes**

Rewrite `internal/provisioner/placement/store.go`. The cache becomes `map[string]record`, values are JSON-encoded in bbolt, a first-byte `'{'` discriminates new vs legacy, and a `now` clock seam is added. Full file:

```go
package placement

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var bucketName = []byte("placements")

// record is the stored placement: which backend serves a lease and when we
// first learned that placement (SetAt). SetAt gates the reconciler's pruner so
// a placement set during a slow reconcile sweep is not mistaken for an orphan
// (ENG-335). Encoded as JSON in bbolt.
type record struct {
	Backend string    `json:"backend"`
	SetAt   time.Time `json:"set_at"`
}

// Store is a bbolt-backed placement store with an in-memory cache.
// It maps lease UUIDs to a backend name plus a first-seen timestamp.
//
// Reads hit only the in-memory cache (protected by RWMutex).
// Writes go to bbolt first, then update the cache.
type Store struct {
	db        *bolt.DB
	cache     map[string]record
	now       func() time.Time
	mu        sync.RWMutex
	closeOnce sync.Once
	closeErr  error
}

// Option configures a Store at construction.
type Option func(*Store)

// WithClock injects the clock used to stamp SetAt. Defaults to time.Now.
// This is a real dependency seam (always set), used for deterministic tests.
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// NewStore opens or creates a bbolt database and loads all existing
// placements into memory.
func NewStore(dbPath string, opts ...Option) (*Store, error) {
	if dbPath == "" {
		return nil, fmt.Errorf("placement db path is required")
	}

	db, err := bolt.Open(dbPath, 0600, &bolt.Options{
		Timeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open placement db: %w", err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create placement bucket: %w", err)
	}

	// Load all entries into cache.
	cache := make(map[string]record)
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		return b.ForEach(func(k, v []byte) error {
			cache[string(k)] = decodeRecord(v)
			return nil
		})
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to load placements into cache: %w", err)
	}

	s := &Store{
		db:    db,
		cache: cache,
		now:   time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// decodeRecord parses a stored value. New values are JSON objects (first byte
// '{'); anything else is a legacy raw backend name written before ENG-335,
// loaded with a zero SetAt so the pruner may remove it immediately.
func decodeRecord(v []byte) record {
	if len(v) > 0 && v[0] == '{' {
		var r record
		if err := json.Unmarshal(v, &r); err == nil {
			return r
		}
		// Corrupt JSON: fall through to legacy interpretation rather than drop.
	}
	return record{Backend: string(v)}
}

func encodeRecord(r record) ([]byte, error) {
	return json.Marshal(r)
}

// Get returns the backend name for a lease UUID, or "" if not found.
func (s *Store) Get(leaseUUID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cache[leaseUUID].Backend
}

// SetAt returns the first-seen time recorded for a lease and whether a
// placement exists. A legacy record returns a zero time with ok=true.
func (s *Store) SetAt(leaseUUID string) (time.Time, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.cache[leaseUUID]
	return r.SetAt, ok
}

// Set records a lease→backend mapping, stamping SetAt with the current clock.
// Holds the write lock for the entire operation so concurrent reads never see
// stale data.
func (s *Store) Set(leaseUUID, backendName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := record{Backend: backendName, SetAt: s.now()}
	return s.put(leaseUUID, r)
}

// put writes one record to bbolt then the cache. Caller holds s.mu.
func (s *Store) put(leaseUUID string, r record) error {
	enc, err := encodeRecord(r)
	if err != nil {
		return fmt.Errorf("failed to encode placement: %w", err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(leaseUUID), enc)
	}); err != nil {
		return fmt.Errorf("failed to set placement: %w", err)
	}
	s.cache[leaseUUID] = r
	return nil
}

// Delete removes a placement. Holds the write lock for the entire operation.
func (s *Store) Delete(leaseUUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Delete([]byte(leaseUUID))
	}); err != nil {
		slog.Warn("failed to delete placement from bbolt",
			"lease_uuid", leaseUUID,
			"error", err,
		)
	}

	delete(s.cache, leaseUUID)
}

// SetBatch records multiple placements in a single bbolt transaction. An
// existing record's SetAt is PRESERVED (this is passive sync from backend
// state, run every sweep — it must not reset the first-seen clock); new
// entries are stamped with the current clock.
func (s *Store) SetBatch(placements map[string]string) error {
	if len(placements) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	merged := make(map[string]record, len(placements))
	for leaseUUID, backendName := range placements {
		setAt := now
		if existing, ok := s.cache[leaseUUID]; ok {
			setAt = existing.SetAt
		}
		merged[leaseUUID] = record{Backend: backendName, SetAt: setAt}
	}

	if err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		for leaseUUID, r := range merged {
			enc, err := encodeRecord(r)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(leaseUUID), enc); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("failed to set batch placements: %w", err)
	}

	maps.Copy(s.cache, merged)
	return nil
}

// Count returns the number of placements in the cache.
func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.cache)
}

// List returns all lease UUIDs that have placements.
func (s *Store) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Collect(maps.Keys(s.cache))
}

// Healthy checks that the bbolt database and bucket are accessible.
func (s *Store) Healthy() error {
	return s.db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketName) == nil {
			return errors.New("placements bucket missing")
		}
		return nil
	})
}

// Close closes the bbolt database. Safe to call multiple times.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}
```

- [ ] **Step 4: Run the placement tests**

Run: `go test ./internal/provisioner/placement/ -race`
Expected: PASS (new tests + all existing `TestStore_*` still green — `Get`/`Set`/`SetBatch`/`Delete`/`List`/`Count` semantics and persistence are unchanged).

- [ ] **Step 5: Commit**

```bash
git add internal/provisioner/placement/store.go internal/provisioner/placement/store_test.go
git commit -m "feat(eng-335): placement records carry a first-seen SetAt timestamp

bbolt values become JSON records (backend + set_at); legacy raw-string values
load with a zero SetAt. Adds a time.Now clock seam (WithClock) and preserve-on-
sync semantics for SetBatch. Foundation for the reconciler prune grace window.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Part B — Extend `PlacementStore` interface + mock

**Files:**
- Modify: `internal/provisioner/interfaces.go:34-43`
- Modify: `internal/provisioner/handler_set_test.go` (`mockPlacementStore`, ~lines 41-95)

- [ ] **Step 1: Add `SetAt` to the interface**

In `internal/provisioner/interfaces.go`, add the method and the `time` import. The interface becomes:

```go
type PlacementStore interface {
	Get(leaseUUID string) string
	SetAt(leaseUUID string) (time.Time, bool)
	Set(leaseUUID, backendName string) error
	Delete(leaseUUID string)
	SetBatch(placements map[string]string) error
	Count() int
	List() []string
	Healthy() error
	Close() error
}
```

Add `"time"` to the import block at the top of the file.

- [ ] **Step 2: Run build to verify the mock no longer satisfies the interface**

Run: `go vet ./internal/provisioner/ 2>&1 | head`
Expected: FAIL — `*mockPlacementStore does not implement PlacementStore (missing method SetAt)` (and the compile-time check `var _ PlacementStore = (*placement.Store)(nil)` passes because Task 2 added `SetAt` to the real store).

- [ ] **Step 3: Add `SetAt` + first-seen tracking + a test helper to the mock**

In `internal/provisioner/handler_set_test.go`, extend `mockPlacementStore`. Add a `setAt` map field, implement `SetAt`, stamp `setAt` in `Set`/`SetBatch` (preserve existing in both, mirroring the real store's `SetBatch`; for `Set`, stamp `time.Now()` only if absent so `setWithTime` overrides stick), and add a `setWithTime` helper. Add `"time"` to the test file imports if not present.

Replace the mock's `Set` and `SetBatch` and add the new methods:

```go
func (m *mockPlacementStore) Set(leaseUUID, backendName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.placements == nil {
		m.placements = make(map[string]string)
	}
	if m.setAt == nil {
		m.setAt = make(map[string]time.Time)
	}
	m.placements[leaseUUID] = backendName
	if _, ok := m.setAt[leaseUUID]; !ok {
		m.setAt[leaseUUID] = time.Now()
	}
	return nil
}

func (m *mockPlacementStore) SetBatch(placements map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.placements == nil {
		m.placements = make(map[string]string)
	}
	if m.setAt == nil {
		m.setAt = make(map[string]time.Time)
	}
	for k, v := range placements {
		m.placements[k] = v
		if _, ok := m.setAt[k]; !ok {
			m.setAt[k] = time.Now()
		}
	}
	return nil
}

func (m *mockPlacementStore) SetAt(leaseUUID string) (time.Time, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.setAt[leaseUUID]
	return t, ok
}

// setWithTime sets a placement with an explicit first-seen time (test helper
// for the reconciler grace-window tests).
func (m *mockPlacementStore) setWithTime(leaseUUID, backendName string, t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.placements == nil {
		m.placements = make(map[string]string)
	}
	if m.setAt == nil {
		m.setAt = make(map[string]time.Time)
	}
	m.placements[leaseUUID] = backendName
	m.setAt[leaseUUID] = t
}
```

And add the field to the struct definition:

```go
type mockPlacementStore struct {
	mu         sync.Mutex
	placements map[string]string
	setAt      map[string]time.Time
}
```

- [ ] **Step 4: Run build/vet**

Run: `go vet ./internal/provisioner/`
Expected: PASS (interface satisfied by both real store and mock). Tests in the package may still fail to compile until Task 4 updates the pruner call sites — that is expected and fixed next.

- [ ] **Step 5: Commit**

```bash
git add internal/provisioner/interfaces.go internal/provisioner/handler_set_test.go
git commit -m "feat(eng-335): add SetAt to PlacementStore interface + test mock

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Part B — Reconciler prune grace window

**Files:**
- Modify: `internal/provisioner/reconciler.go:388` (call site) and `:1180-1218` (`cleanupOrphanedPlacements`)
- Test: `internal/provisioner/reconciler_test.go`

- [ ] **Step 1: Write the failing grace-window test**

Add to `internal/provisioner/reconciler_test.go` (in the "Placement pruning tests" section, after `TestReconciler_PrunesOrphanedPlacement`). It calls the pruner directly (white-box, like `TestCleanupOrphanedPlacements_GateD`):

```go
// TestCleanupOrphanedPlacements_GraceWindow verifies ENG-335: a placement that
// is chain-terminal, absent from all backends, and not in-flight is still KEPT
// when it was set within 2× the reconcile interval (a lease that provisioned
// during a slow sweep is absent from the stale snapshot but is live). Once it
// ages past the grace window it is pruned.
func TestCleanupOrphanedPlacements_GraceWindow(t *testing.T) {
	const interval = time.Minute // grace = 2*interval = 2m
	t0 := time.Date(2026, 6, 18, 17, 11, 15, 0, time.UTC)

	ps := &mockPlacementStore{}
	ps.setWithTime("young-lease", "backend-a", t0) // set at t0

	// chain-terminal (absent from chain), absent from backends, not in-flight.
	chainLeases := map[string]billingtypes.Lease{}
	backendLeases := map[string]struct{}{}
	tracker := newMockInFlightTracker(nil)
	r := &Reconciler{placementStore: ps, tracker: tracker, interval: interval}

	// now = t0 + 1m  → within the 2m grace → KEEP.
	pruned := r.cleanupOrphanedPlacements(context.Background(), chainLeases, backendLeases, true, t0.Add(time.Minute))
	assert.Equal(t, 0, pruned, "young placement within grace must be kept")
	assert.Equal(t, "backend-a", ps.Get("young-lease"))

	// now = t0 + 2m + 1s → past grace → PRUNE.
	pruned = r.cleanupOrphanedPlacements(context.Background(), chainLeases, backendLeases, true, t0.Add(2*time.Minute+time.Second))
	assert.Equal(t, 1, pruned, "aged placement past grace must be pruned")
	assert.Equal(t, "", ps.Get("young-lease"))
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./internal/provisioner/ -run TestCleanupOrphanedPlacements_GraceWindow`
Expected: COMPILE FAIL — `too many arguments in call to r.cleanupOrphanedPlacements` (method still takes 4 args).

- [ ] **Step 3: Add the `now` param + grace gate to `cleanupOrphanedPlacements`**

In `internal/provisioner/reconciler.go`, change the signature and add the grace gate. Replace the method header and the pre-Delete section:

Change the signature from:

```go
func (r *Reconciler) cleanupOrphanedPlacements(
	ctx context.Context,
	chainLeases map[string]billingtypes.Lease,
	backendLeases map[string]struct{},
	retentionsComplete bool,
) int {
```

to:

```go
func (r *Reconciler) cleanupOrphanedPlacements(
	ctx context.Context,
	chainLeases map[string]billingtypes.Lease,
	backendLeases map[string]struct{},
	retentionsComplete bool,
	now time.Time,
) int {
```

Then, immediately before the `r.placementStore.Delete(leaseUUID)` line (currently after the chain-terminal `continue`), insert the grace gate:

```go
		// ENG-335: keep a placement that was set within the grace window. A lease
		// that provisioned entirely during a slow reconcile sweep is absent from
		// this sweep's (stale) snapshot of chain + backends, yet is live; pruning
		// it here strands its volume at close. The placement is a derived index —
		// keeping a young one is harmless (processOrphan GCs the real resource and
		// a closed lease is never restored) — so we never prune within 2× the
		// reconcile interval, comfortably longer than one sweep.
		grace := 2 * r.interval
		if setAt, ok := r.placementStore.SetAt(leaseUUID); ok && grace > 0 && now.Sub(setAt) < grace {
			slog.Debug("reconcile: keeping placement within grace window",
				"lease_uuid", leaseUUID, "age", now.Sub(setAt), "grace", grace)
			continue
		}
```

(Place this AFTER the existing chain-terminal `continue` block and BEFORE `r.placementStore.Delete(leaseUUID)`.)

- [ ] **Step 4: Update the call site to pass the sweep-start time**

In `internal/provisioner/reconciler.go:388`, change:

```go
	prunedPlacements := r.cleanupOrphanedPlacements(ctx, chainLeases, backendLeases, retentionsComplete)
```

to:

```go
	prunedPlacements := r.cleanupOrphanedPlacements(ctx, chainLeases, backendLeases, retentionsComplete, startTime)
```

(`startTime` is captured at `reconciler.go:152` and is in scope.)

- [ ] **Step 5: Fix the existing direct prune test for the new signature + grace**

In `internal/provisioner/reconciler_test.go`, `TestCleanupOrphanedPlacements_GateD`: it builds `r := &Reconciler{placementStore: ps, tracker: tracker}` (interval 0 ⇒ grace 0 ⇒ gate disabled) and calls the method with 4 args. Set a non-zero interval and pass a `now` far past the (real-time) `ps.Set` stamps so the grace gate is satisfied and the original gate-(d) assertions still hold. Change the construction + call lines:

```go
	r := &Reconciler{placementStore: ps, tracker: tracker, interval: time.Minute}
	pruned := r.cleanupOrphanedPlacements(context.Background(), chainLeases, backendLeases, true, time.Now().Add(time.Hour))
```

(The placements were `ps.Set(...)` at test start; `now = +1h` makes their age ≫ the 2m grace, so gate (d)'s prune/keep outcomes are unchanged. Add `"time"` to the test imports if not already present.)

- [ ] **Step 6: Fix the existing end-to-end prune test for the grace window**

`TestReconciler_PrunesOrphanedPlacement` runs through `RunOnce` with the default 5m interval (grace = 10m) and expects `gone-lease` (set "just now") to be pruned — now it would be within grace and kept. Backdate `gone-lease` past the grace window using the mock helper. Change:

```go
	ps := &mockPlacementStore{}
	ps.Set("gone-lease", "backend-a")
	ps.Set("retained-1", "backend-a")
```

to:

```go
	ps := &mockPlacementStore{}
	// Backdate well past the 10m grace (default 5m interval × 2) so the orphan is
	// prunable this sweep; a freshly-set placement would be kept by the ENG-335
	// grace window (covered by TestCleanupOrphanedPlacements_GraceWindow).
	ps.setWithTime("gone-lease", "backend-a", time.Now().Add(-time.Hour))
	ps.Set("retained-1", "backend-a")
```

(Add `"time"` to the test imports if not present. `retained-1` is kept by the on-backend gate regardless of age, so it does not need backdating.)

- [ ] **Step 7: Run the reconciler prune tests**

Run: `go test ./internal/provisioner/ -run 'Prune|cleanupOrphaned|CleanupOrphaned|GraceWindow|GateD'`
Expected: PASS — `TestCleanupOrphanedPlacements_GraceWindow`, `TestCleanupOrphanedPlacements_GateD`, `TestReconciler_PrunesOrphanedPlacement`, `TestReconciler_DoesNotPruneOnIncompleteRetentions`, and the keep-placement ReconcileAll tests.

- [ ] **Step 8: Commit**

```bash
git add internal/provisioner/reconciler.go internal/provisioner/reconciler_test.go
git commit -m "fix(eng-335): reconciler keeps placements within a prune grace window

cleanupOrphanedPlacements now skips any placement set within 2x the reconcile
interval, using the sweep-start time as reference. A lease that provisions
during a slow sweep is absent from the stale snapshot but live; pruning it
stranded its volume at close. The placement is a derived index, so keeping a
young one is harmless.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Full provisioner suite with race detector**

Run: `go test ./internal/provisioner/... -race`
Expected: PASS, 0 failures.

- [ ] **Step 2: Build, vet, and whole-repo tests**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: PASS. (If a pre-existing unrelated test is flaky/red on `main`, note it; do not fix out of scope.)

- [ ] **Step 3: Lint**

Run: `golangci-lint run ./internal/provisioner/... 2>/dev/null || echo "golangci-lint not installed; skipping"`
Expected: no new findings in changed files (e.g. no unused `skuHint`/`ExtractRoutingSKU` import fallout in `handler_set.go`).

- [ ] **Step 4: Confirm the trace is addressed (manual reasoning check, no code)**

Re-read the ENG-335 trace in the spec. Confirm in the diff: (1) a missing-placement close now sweeps all backends (Part A) so the real backend is soft-deleted at close — no ~110 s stall; (2) the placement for a just-provisioned lease is no longer pruned within grace (Part B) so close resolves the right backend via Case 0 in the common path.

---

## Self-Review

**Spec coverage:**
- Part A (drop Case 2, all-backends fallback) → Task 1. ✅
- Part A improved logging (no misleading per-backend success; one summary line) → Task 1 Step 3. ✅
- Part A `skuHint` full removal from `Deprovision` + `processLeaseClose` → Task 1 Steps 3–4. ✅
- Part B `SetAt` field + JSON encoding + legacy first-byte discriminator + clock seam + `SetBatch` preserve → Task 2. ✅
- Part B interface + mock `SetAt` → Task 3. ✅
- Part B grace gate `2 × interval` using sweep-start `now` → Task 4. ✅
- Testing (orchestrator regression, reconciler grace, store round-trip/legacy/preserve, `-race`) → Tasks 1,2,4,5. ✅
- Non-goal (retention inventory perf) → not in plan, tracked separately. ✅

**Placeholder scan:** No TBD/TODO/"handle edge cases". Every code step shows complete code; every test step shows the assertion. ✅

**Type consistency:** `Deprovision(ctx, leaseUUID)` (2 args) used consistently in impl + all test call sites + caller. `cleanupOrphanedPlacements(ctx, chainLeases, backendLeases, retentionsComplete, now)` (5 args) consistent at def + both call sites (prod `:388` + tests). `record{Backend, SetAt}`, `WithClock`, `SetAt(leaseUUID) (time.Time, bool)`, `setWithTime` names match across Tasks 2–4. ✅
