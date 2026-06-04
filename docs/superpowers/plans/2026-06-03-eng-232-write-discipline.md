# ENG-232 — Tighten remaining provision-write discipline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route `doDeprovision`'s direct `b.provisions` writes through the `LeaseProvisionStore` seam (add a `Delete` method; migrate the initial-mark + partial-failure writes to `UpdateFn`, the success-delete to `store.Delete`), keep the volume-retry block as one documented atomic exception, and document the actor-sole-writer invariant.

**Architecture:** Add `Delete(leaseUUID) bool` to the substrate-agnostic store interface + both implementors. Migrate three of the four direct-write sites in `doDeprovision` (which runs on the actor goroutine) to the seam, keeping side-effects (`activeProvisions.Dec()`, `persistDiagnostics`) outside the closures. The volume-retry block stays direct because `VolumeCleanupAttempts` is a docker-private wrapper field unreachable through `func(*ProvisionState)` and the block is an atomic read-modify-write (tracked: ENG-285). The worker pre-publish (Part 2) is KEPT as a documented lock-guarded, barrier-ordered exception.

**Tech Stack:** Go; `internal/backend/shared/leasesm` (the store seam + the actor) and `internal/backend/docker` (the adapter + `doDeprovision` + tests).

**Spec:** `docs/superpowers/specs/2026-06-03-eng-232-write-discipline-design.md`

---

## File Structure

| File | Change |
|------|--------|
| `internal/backend/shared/leasesm/leasesm.go` | Add `Delete(leaseUUID string) bool` to `LeaseProvisionStore` + the package-doc invariant block (Part 2). |
| `internal/backend/shared/leasesm/mocks_test.go` | Add `mockProvisionStore.Delete`. |
| `internal/backend/docker/leasesm_adapters.go` | Add `backendProvisionStore.Delete`. |
| `internal/backend/docker/deprovision.go` | Migrate sites 1/2/5 to `UpdateFn`/`Delete`; document the kept volume-retry block. |
| `internal/backend/docker/provision_test.go` | Land the 2 named stub tests with bodies + add the under-limit + `Delete` unit tests. |
| `internal/backend/docker/leasesm_adapters_test.go` (new, optional) | `backendProvisionStore.Delete` unit test. |

The existing actor preemption tests (`lease_actor_test.go:334`, `:794`) are **not modified** — they guard Part 2 and must stay green.

---

## Task 1: Add `Delete` to the store seam

TDD via the compile-time interface guard: adding `Delete` to the interface breaks compilation until both implementors gain it.

**Files:** `leasesm.go`, `leasesm_adapters.go`, `mocks_test.go`

- [ ] **Step 1: Add `Delete` to the interface (red — breaks compilation)**

In `internal/backend/shared/leasesm/leasesm.go`, add to the `LeaseProvisionStore` interface (after `UpdateFn`):

```go
	// Delete removes the lease's live record. Returns true if an entry was
	// present. Takes the same mutex as Get/UpdateFn, so a concurrent Get
	// observes the removal. Like UpdateFn, MUST NOT be called from inside an
	// UpdateFn closure (re-entrant lock → deadlock).
	Delete(leaseUUID string) bool
```

- [ ] **Step 2: Run — verify the guard fails**

Run: `go build ./... 2>&1 | head`
Expected: FAIL — `*backendProvisionStore` does not implement `LeaseProvisionStore` (missing `Delete`), and (for tests) the guard `var _ LeaseProvisionStore = (*mockProvisionStore)(nil)` (`lease_actor_test.go:1211`) fails.

- [ ] **Step 3: Implement `backendProvisionStore.Delete`** (in `leasesm_adapters.go`, after `UpdateFn`, mirroring its shape)

```go
// Delete implements leasesm.LeaseProvisionStore. Removes the lease's live
// record under the same provisionsMu as Get/UpdateFn, so a concurrent Get
// probe (e.g. handleDeprovision's terminated check) observes the removal.
func (s *backendProvisionStore) Delete(leaseUUID string) bool {
	s.backend.provisionsMu.Lock()
	defer s.backend.provisionsMu.Unlock()
	_, ok := s.backend.provisions[leaseUUID]
	delete(s.backend.provisions, leaseUUID)
	return ok
}
```

- [ ] **Step 4: Implement `mockProvisionStore.Delete`** (in `mocks_test.go`; inline the delete — do NOT call the existing `remove()` helper, which re-locks `m.mu`)

```go
// Delete implements LeaseProvisionStore. Removes the seeded state under the
// same mutex as Get/UpdateFn. Returns true if present.
func (m *mockProvisionStore) Delete(uuid string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.states[uuid]
	delete(m.states, uuid)
	return ok
}
```

- [ ] **Step 5: Add a `Delete` unit test** (new file `internal/backend/docker/leasesm_adapters_test.go`, `package docker`)

```go
package docker

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/backend/shared/leasesm"
)

func TestBackendProvisionStore_Delete(t *testing.T) {
	b := newBackendForTest(&mockDockerClient{}, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1", Status: backend.ProvisionStatusReady}},
	})
	// Present → true, then the entry is gone (Get probe returns !ok).
	assert.True(t, b.provisionStore.Delete("lease-1"))
	_, ok := b.provisionStore.Get("lease-1")
	assert.False(t, ok, "entry must be gone after Delete")
	// Absent → false (idempotent).
	assert.False(t, b.provisionStore.Delete("lease-1"))
}
```

- [ ] **Step 6: Run — green**

Run: `go build ./... && go test ./internal/backend/shared/leasesm/ ./internal/backend/docker/ -run 'TestBackendProvisionStore_Delete|TestMockProvisionStore' -count=1`
Expected: PASS (compiles; `Delete` unit test passes; the mock-store concurrency test still passes).

- [ ] **Step 7: Commit**

```bash
git add internal/backend/shared/leasesm/leasesm.go internal/backend/shared/leasesm/mocks_test.go internal/backend/docker/leasesm_adapters.go internal/backend/docker/leasesm_adapters_test.go
git commit -m "feat(leasesm): add LeaseProvisionStore.Delete seam method (ENG-232)"
```

---

## Task 2: Characterize the un-migrated deprovision branches (safety net before migrating)

The volume-retry (give-up + under-limit) and partial-container-failure branches are **currently untested** — all `DestroyFn` mocks return nil, so those branches never run. Write real tests against the **current** code (they pass — characterization) so the Task 3 migration is provably behavior-preserving.

**Files:** `internal/backend/docker/provision_test.go`

- [ ] **Step 1: Add the `testutil` import** to `provision_test.go` (for the counter-delta assertion):

```go
	"github.com/prometheus/client_golang/prometheus/testutil"
```

- [ ] **Step 2: Implement the give-up test** (replaces the comment-only stub at `provision_test.go:~844`)

```go
func TestDeprovision_VolumeExhaustionSendsFailedCallback(t *testing.T) {
	var received backend.CallbackPayload
	callbackDone := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
		select {
		case callbackDone <- struct{}{}:
		default:
		}
	}))
	defer server.Close()

	mock := &mockDockerClient{RemoveContainerFn: func(ctx context.Context, id string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 1,
			ContainerIDs: []string{"c1"}, CallbackURL: server.URL,
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}}},
			VolumeCleanupAttempts: maxVolumeCleanupAttempts - 1}, // one failure from give-up
	})
	// Container removal succeeds (compose Down nil default) so the volume path runs;
	// volume Destroy always fails → attempts 2→3 → give-up.
	b.volumes = &mockVolumeManager{DestroyFn: func(ctx context.Context, id string) error {
		return fmt.Errorf("permission denied")
	}}
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	before := testutil.ToFloat64(deprovisionsTotal)
	require.NoError(t, b.Deprovision(context.Background(), "lease-1")) // give-up returns nil, not error

	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for failed callback")
	}
	assert.Equal(t, backend.CallbackStatusFailed, received.Status)
	assert.Equal(t, "volume cleanup exhausted", received.Error)

	b.provisionsMu.RLock()
	_, ok := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	assert.False(t, ok, "provision must be deleted after give-up")
	assert.Equal(t, 1.0, testutil.ToFloat64(deprovisionsTotal)-before, "give-up increments deprovisionsTotal")
}
```

- [ ] **Step 3: Implement the under-limit volume-retry test** (new test)

```go
func TestDeprovision_UnderLimitVolumeRetryKeepsProvisionFailed(t *testing.T) {
	mock := &mockDockerClient{RemoveContainerFn: func(ctx context.Context, id string) error { return nil }}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 1,
			ContainerIDs: []string{"c1"},
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}}}},
	})
	b.volumes = &mockVolumeManager{DestroyFn: func(ctx context.Context, id string) error {
		return fmt.Errorf("device busy")
	}}

	err := b.Deprovision(context.Background(), "lease-1")
	require.Error(t, err, "under-limit volume failure returns an error")

	b.provisionsMu.RLock()
	p, ok := b.provisions["lease-1"]
	b.provisionsMu.RUnlock()
	require.True(t, ok, "provision stays visible for retry under the limit")
	assert.Equal(t, backend.ProvisionStatusFailed, p.Status)
	assert.Nil(t, p.ContainerIDs, "containers are gone")
	assert.Equal(t, 1, p.VolumeCleanupAttempts)
}
```

- [ ] **Step 4: Implement the partial-failure-then-retry test** (replaces the comment-only stub at `provision_test.go:~849`)

```go
func TestDeprovision_RetryAfterPartialFailureFiresOneCallback(t *testing.T) {
	var statuses []backend.CallbackStatus
	var mu sync.Mutex
	callbackDone := make(chan struct{}, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p backend.CallbackPayload
		json.NewDecoder(r.Body).Decode(&p)
		w.WriteHeader(http.StatusOK)
		mu.Lock()
		statuses = append(statuses, p.Status)
		mu.Unlock()
		callbackDone <- struct{}{}
	}))
	defer server.Close()

	removeShouldFail := true
	mock := &mockDockerClient{RemoveContainerFn: func(ctx context.Context, id string) error {
		if removeShouldFail {
			return fmt.Errorf("container removal failed")
		}
		return nil
	}}
	b := newBackendForProvisionTest(t, mock, map[string]*provision{
		"lease-1": {ProvisionState: leasesm.ProvisionState{LeaseUUID: "lease-1",
			Tenant: "tenant-a", Status: backend.ProvisionStatusReady, Quantity: 1,
			ContainerIDs: []string{"c1"}, CallbackURL: server.URL,
			Items: []backend.LeaseItem{{SKU: "docker-small", Quantity: 1, ServiceName: manifest.DefaultServiceName}}}},
	})
	// Force the RemoveContainer fallback on both calls (compose Down fails).
	b.compose = &mockComposeExecutor{DownFn: func(ctx context.Context, project string, t time.Duration) error {
		return fmt.Errorf("compose down failed")
	}}
	b.httpClient = server.Client()
	b.cfg.CallbackSecret = "test-secret-that-is-long-enough-32chars"
	rebuildCallbackSender(b)

	// Call 1: partial container failure → error, NO callback, provision stays Failed.
	require.Error(t, b.Deprovision(context.Background(), "lease-1"))
	b.provisionsMu.RLock()
	p, ok := b.provisions["lease-1"]
	gotStatus := p.Status
	gotIDs := append([]string(nil), p.ContainerIDs...)
	b.provisionsMu.RUnlock()
	require.True(t, ok)
	assert.Equal(t, backend.ProvisionStatusFailed, gotStatus)
	assert.Equal(t, []string{"c1"}, gotIDs, "ContainerIDs narrowed to the stuck containers")
	select {
	case <-callbackDone:
		t.Fatal("partial failure must NOT fire a callback")
	case <-time.After(200 * time.Millisecond):
	}

	// Call 2: removal now succeeds → exactly one Deprovisioned callback.
	removeShouldFail = false
	require.NoError(t, b.Deprovision(context.Background(), "lease-1"))
	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the deprovisioned callback")
	}
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, statuses, 1, "exactly one callback across both calls")
	assert.Equal(t, backend.CallbackStatusDeprovisioned, statuses[0])
}
```

- [ ] **Step 5: Run — verify these PASS on the current (un-migrated) code**

Run: `go test ./internal/backend/docker/ -run 'TestDeprovision_VolumeExhaustionSendsFailedCallback|TestDeprovision_UnderLimitVolumeRetryKeepsProvisionFailed|TestDeprovision_RetryAfterPartialFailureFiresOneCallback' -race -count=1`
Expected: PASS — these characterize the current behavior of the (previously untested) branches.

- [ ] **Step 6: Commit**

```bash
git add internal/backend/docker/provision_test.go
git commit -m "test(docker-backend): cover deprovision volume-retry + partial-failure branches (ENG-232)"
```

---

## Task 3: Migrate `doDeprovision` writes through the seam

Migrate the initial-mark and partial-failure writes to `UpdateFn` and the success-delete to `store.Delete`. Keep the volume-retry block atomic + documented. The Task 2 tests + the existing suite prove behavior is preserved.

**Files:** `internal/backend/docker/deprovision.go`

- [ ] **Step 1: Migrate the initial-mark site** — replace the first `b.provisionsMu.Lock()` block (`deprovision.go:47-71`, from `b.provisionsMu.Lock()` through the `b.provisionsMu.Unlock()` after `callbackURL := prov.CallbackURL`) with:

```go
	// Mark Deprovisioning before removing containers (the in-memory marker lets
	// Provision's status guard reject concurrent re-provision during the removal
	// window). Capture the teardown inputs inside the closure; the metric Dec is
	// a side effect kept OUTSIDE the closure (UpdateFn no-side-effect contract).
	var (
		wasReady     bool
		containerIDs []string
		items        []backend.LeaseItem
		tenant       string
		callbackURL  string
	)
	exists := b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
		wasReady = p.Status == backend.ProvisionStatusReady
		p.Status = backend.ProvisionStatusDeprovisioning
		containerIDs = append([]string(nil), p.ContainerIDs...)
		items = append([]backend.LeaseItem(nil), p.Items...)
		tenant = p.Tenant
		callbackURL = p.CallbackURL
	})
	if !exists {
		// Already deprovisioned — idempotent success.
		return nil
	}
	// Decrement activeProvisions on the Ready→Deprovisioning transition so the
	// gauge stays accurate even if Deprovision later fails partially.
	if wasReady {
		activeProvisions.Dec()
	}
```

- [ ] **Step 2: Migrate the partial-container-failure site** — replace the `if len(errs) > 0 { ... }` block's `b.provisionsMu.Lock()`/`if p, ok := ...`/`Unlock()` (`deprovision.go:114-122`) with the `UpdateFn` form, keeping `persistDiagnostics` + the error return:

```go
	if len(errs) > 0 {
		// Partial failure: keep provision visible with only the stuck containers.
		var diagSnap shared.DiagnosticEntry
		b.provisionStore.UpdateFn(leaseUUID, func(p *leasesm.ProvisionState) {
			p.Status = backend.ProvisionStatusFailed
			p.ContainerIDs = failedIDs
			p.LastError = fmt.Sprintf("deprovision partially failed: %s", errors.Join(errs...))
			diagSnap = leasesm.DiagnosticSnapshot(p)
		})
		// Unlike the initial-mark migration, do NOT early-return on UpdateFn==false:
		// still persist diagnostics and surface the error. If the entry is gone,
		// diagSnap is zero-value and persistDiagnostics no-ops on its empty guard.
		b.persistDiagnostics(diagSnap, failedIDs)
		return fmt.Errorf("deprovision partially failed: %w", errors.Join(errs...))
	}
```

- [ ] **Step 3: Document the kept volume-retry block** — at the top of the `if len(volumeErrs) > 0 {` block (`deprovision.go:143`), prepend a comment (do NOT change the code inside):

```go
	if len(volumeErrs) > 0 {
		// ENG-232: this block stays a single direct provisionsMu span (NOT routed
		// through the store seam) because VolumeCleanupAttempts is a docker-private
		// wrapper field (not on ProvisionState, unreachable through UpdateFn) and is
		// incremented atomically with ContainerIDs/Status — seaming the
		// ProvisionState writes would split one atomic read-modify-write into two.
		// The give-up-branch delete stays inline for the same reason (calling
		// store.Delete here would re-enter provisionsMu). Tracked: ENG-285.
		var diagSnap shared.DiagnosticEntry
		// ... (existing block unchanged) ...
```

- [ ] **Step 4: Migrate the success-delete site** — replace the final delete block (`deprovision.go:218-220`):

```go
	// All containers and volumes removed — delete via the store seam. Docker-private
	// state (VolumeCleanupAttempts) lives on the provision wrapper, so the single
	// map delete drops it too.
	b.provisionStore.Delete(leaseUUID)
```

- [ ] **Step 5: Run — the migration is behavior-preserving**

Run: `go build ./... && go test ./internal/backend/docker/ -run TestDeprovision -race -count=1`
Expected: PASS — the Task 2 branch tests + `TestDeprovision_SendsDeprovisionedCallback` + all other `TestDeprovision_*` stay green (same Status/ContainerIDs/gauge/callback/delete outcomes).

- [ ] **Step 6: Run the broader actor + docker suites under `-race`**

Run: `go test ./internal/backend/shared/leasesm/ -run 'DeprovisionWaits' -race -count=1 && go test ./internal/backend/docker/ -race -count=1`
Expected: PASS — `TestProvision_DeprovisionWaitsForInFlightGoroutine` + `TestLeaseActor_RestartDeprovisionWaitsForInFlightGoroutine` (the Part 2 preemption guards) stay green; full docker package green.

- [ ] **Step 7: Commit**

```bash
git add internal/backend/docker/deprovision.go
git commit -m "refactor(docker-backend): route doDeprovision writes through the store seam (ENG-232)"
```

---

## Task 4: Document the invariant + final verification

**Files:** `internal/backend/shared/leasesm/leasesm.go`

- [ ] **Step 1: Add the invariant to the `leasesm` package doc** — append to the `LeaseProvisionStore` doc block (or the package doc) a block stating:

```go
// # Actor-sole-writer invariant (ENG-229)
//
// All live ProvisionState mutations occur on the lease actor goroutine via
// UpdateFn/Delete, with exactly TWO documented exceptions:
//
//  1. The success-path pre-publish of ContainerIDs/ServiceContainers in
//     spawnProvisionWorker/spawnReplaceWorker runs on the WORKER goroutine
//     before sendTerminal. It is required so a Deprovision that preempts an
//     in-flight worker observes the new container IDs under provisionsMu and
//     tears them down instead of orphaning them. Correctness rests on the
//     workers-barrier happens-before: the pre-publish UpdateFn completes before
//     workers.Done() (the outermost defer); onExitProvisioning calls
//     workCancel() then waitForWorkers() (blocked on workers.Zero()); only then
//     does handleDeprovision invoke doDeprovision, which reads ContainerIDs.
//     Routing this through an actor message is PROHIBITED: the actor is blocked
//     in waitForWorkers() and cannot dequeue the publish the worker must send to
//     release the barrier (actor self-deadlock). Bounded escape: a worker
//     exceeding workExitWaitTimeout (75s; diagnosticsGatherTimeout 30s is the
//     inner budget) degrades to a recoverState-reconciled zombie, never to state
//     corruption.
//  2. The deprovision volume-retry block (docker backend) stays a single direct
//     provisionsMu span because it is coupled to the docker-private
//     VolumeCleanupAttempts counter (not ProvisionState). It runs inline on the
//     actor goroutine, so a hung volume.Destroy blocks the actor for that lease
//     until ctx/timeout. Tracked for full seaming: ENG-285.
```

- [ ] **Step 2: Run gofmt/vet + the full affected suites under `-race`**

Run: `gofmt -l internal/backend/shared/leasesm/leasesm.go && go vet ./internal/backend/docker/ ./internal/backend/shared/leasesm/ && go test ./internal/backend/docker/ ./internal/backend/shared/leasesm/ -race -count=1`
Expected: gofmt clean, vet clean, all tests pass under `-race`.

- [ ] **Step 3: Commit**

```bash
git add internal/backend/shared/leasesm/leasesm.go
git commit -m "docs(leasesm): document the actor-sole-writer invariant + its two exceptions (ENG-229/ENG-232)"
```

- [ ] **Step 4 (controller, post-merge or now): update ENG-229 category C** in Linear to record the (a) decision + this invariant, and that the volume-retry residual is tracked as ENG-285. *(Linear action — not a code change.)*

---

## Self-Review (against the spec)

**Spec coverage:**
- Add `LeaseProvisionStore.Delete` + both implementors + mock → Task 1. ✔
- Migrate initial-mark/partial-failure → `UpdateFn`, success-delete → `Delete` → Task 3 Steps 1/2/4. ✔
- Keep + document the volume-retry block (docker-private + atomic RMW) → Task 3 Step 3. ✔
- `wasReady` read before `Status`; `Dec()`/`persistDiagnostics` outside the closure; partial-failure asymmetric false-semantics (no early-return) → Task 3 Steps 1/2. ✔
- Real branch tests for the previously-untested give-up/under-limit/partial-failure paths; land the two named stubs → Task 2. ✔
- Existing preemption tests stay green under `-race` (Part 2 guard) → Task 3 Step 6. ✔
- `mockProvisionStore` gains `Delete` (compile-time) → Task 1 Step 4. ✔
- Package-doc invariant (two exceptions, barrier rationale, failure modes) → Task 4 Step 1; ENG-229 category C → Task 4 Step 4. ✔

**Placeholder scan:** none — every code step shows the exact code; every run step has an exact command + expected red/green state (Task 1 Step 2 red via the compile guard; Task 2 Step 5 characterization-green; Task 3 Step 5 behavior-preserving-green).

**Type consistency:** `Delete(leaseUUID string) bool` is identical across the interface, `backendProvisionStore`, and `mockProvisionStore`. `b.provisionStore` (field on `*Backend`, `backend.go:149`) is the handle used in `doDeprovision`. `leasesm.DiagnosticSnapshot(p)` takes `*ProvisionState` (the closure param). `CallbackPayload.Error` is the asserted error field.
