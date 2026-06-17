# ENG-333 Restore Backend Affinity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On a multi-backend pool, route a lease restore to the backend that physically holds the source lease's retained data, and keep that affinity self-healing.

**Architecture:** fred's placement store (lease→backend) is a derived index reconciled from backend ground truth. We make the reconciler the single authority for placement of retained leases (it queries a new `GET /retentions` endpoint and unions it into the placement sync, plus prunes orphans with strict gates), keep placement alive across close (orchestrator stops deleting it), route restore via the source lease's placement, and honor existing placement before least-loaded routing so a restored lease never drifts off its data. Edge writes (restore-success bookkeeping) are latency optimizations owned by the orchestrator. No `Backend.Deprovision` signature change.

**Tech Stack:** Go 1.25, `net/http` + `httptest`, testify (`require`/`assert`), bbolt (unchanged). Design doc: `docs/superpowers/specs/2026-06-17-eng-333-restore-backend-affinity-design.md`.

---

## File Structure

| File | Responsibility | Change |
|------|----------------|--------|
| `internal/backend/client.go` | `RetainedLease`/`ListRetentionsResponse` types, `DefaultMaxRetentionsBytes`, `HTTPClient.ListRetentions`, `Backend.ListRetentions` | Modify |
| `internal/backend/client_test.go` | `HTTPClient.ListRetentions` decode test | Modify |
| `internal/backend/mock.go` | `MockBackend.ListRetentions` + `SetRetentions` knob | Modify |
| `internal/backend/mock_test.go` | mock knob test | Modify |
| `internal/backend/docker/retention.go` (new) or `backend.go` | `docker.Backend.ListRetentions` from `retentionStore.List()` | Create/Modify |
| `internal/backend/k3s/provision_stub.go` | `k3s.Backend.ListRetentions` (empty) | Modify |
| `cmd/docker-backend/main.go` | `GET /retentions` route + `handleListRetentions`; `backendService.ListRetentions` | Modify |
| `cmd/k3s-backend/server.go` | `GET /retentions` route + `handleListRetentions` (empty); `backendService.ListRetentions` | Modify |
| `internal/provisioner/orchestrator.go` | stop deleting placement on Deprovision; honor-placement routing; `RecordRestorePlacement` | Modify |
| `internal/provisioner/reconciler.go` | honor-placement routing; `fetchAllRetentions`; union sync; `cleanupOrphanedPlacements` | Modify |
| `internal/api/handlers.go` | auth-only helper; restore routes via source placement; `RestorePlacementRecorder` seam | Modify |
| `internal/api/server.go` | wire `RestoreRecorder` | Modify |
| `cmd/providerd/main.go` | pass orchestrator as `RestoreRecorder` | Modify |
| `*_test.go` test fakes (router_bench, manager_bench, manager_stress, manager_test, reconciler_test) | add `ListRetentions` to backend fakes | Modify |

---

## Task 1: `RetainedLease` type + `HTTPClient.ListRetentions` (concrete only)

**Files:**
- Modify: `internal/backend/client.go`
- Test: `internal/backend/client_test.go`

> The `Backend` *interface* method is added in Task 2 (together with every implementer) so the package always compiles. Task 1 adds only the wire types and the concrete `*HTTPClient` method.

- [ ] **Step 1: Write the failing test**

Add to `internal/backend/client_test.go` (imports `context`, `net/http`, `net/http/httptest`, testify already present):

```go
func TestHTTPClient_ListRetentions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/retentions", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"retentions":[{"lease_uuid":"lease-a"},{"lease_uuid":"lease-b"}]}`))
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{Name: "test", BaseURL: server.URL})

	got, err := client.ListRetentions(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "lease-a", got[0].LeaseUUID)
	assert.Equal(t, "lease-b", got[1].LeaseUUID)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/backend/ -run TestHTTPClient_ListRetentions -v`
Expected: compile failure — `ListRetentions`, `RetainedLease` undefined.

- [ ] **Step 3: Add the wire types**

In `internal/backend/client.go`, immediately after the `ListProvisionsResponse` struct (around line 272), add:

```go
// RetainedLease identifies a lease whose data this backend currently retains
// (soft-deleted, awaiting restore or grace-reap). The reconciler consumes this
// to keep placement affinity for retained leases so a restore routes to the
// backend holding the source data (ENG-333).
type RetainedLease struct {
	LeaseUUID string `json:"lease_uuid"`
}

// ListRetentionsResponse is the response from the GET /retentions endpoint.
type ListRetentionsResponse struct {
	Retentions []RetainedLease `json:"retentions"`
}
```

- [ ] **Step 4: Add the size-limit constant, struct field, config field, and initializer**

In `internal/backend/client.go`:

(a) In the `const (...)` block of default size limits (ends with `DefaultMaxStatsBytes ...`, ~line 490), add as the last entry:

```go
	DefaultMaxRetentionsBytes int64 = 8 << 20 // 8 MiB — list of retained leases (UUIDs only)
```

(b) In the `HTTPClient` struct, after `maxStatsBytes int64` (~line 470), add:

```go
	maxRetentionsBytes int64
```

(c) In `HTTPClientConfig`, after `MaxStatsBytes int64 // GetLoadStats response limit (default: 1 MiB)` (~line 524), add:

```go
	MaxRetentionsBytes int64 // ListRetentions response limit (default: 8 MiB)
```

(d) In `NewHTTPClient`, in the returned `&HTTPClient{...}` literal, after `maxStatsBytes: positiveOr(cfg.MaxStatsBytes, DefaultMaxStatsBytes),` (~line 619), add:

```go
		maxRetentionsBytes:       positiveOr(cfg.MaxRetentionsBytes, DefaultMaxRetentionsBytes),
```

- [ ] **Step 5: Add the concrete `HTTPClient.ListRetentions` method**

In `internal/backend/client.go`, immediately after the `GetLoadStats` method (~line 1189), add:

```go
// ListRetentions retrieves the leases whose data this backend currently retains
// (GET /retentions). Used by the reconciler for restore backend affinity.
func (c *HTTPClient) ListRetentions(ctx context.Context) ([]RetainedLease, error) {
	result, err := doGet[ListRetentionsResponse](c, ctx, "list_retentions", c.baseURL+"/retentions", c.maxRetentionsBytes)
	if err != nil {
		return nil, err
	}
	return result.Retentions, nil
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `go test ./internal/backend/ -run TestHTTPClient_ListRetentions -v`
Expected: PASS.
Run: `go build ./...`
Expected: success (purely additive; interface unchanged this task).

- [ ] **Step 7: Commit**

```bash
git add internal/backend/client.go internal/backend/client_test.go
git commit -m "feat(backend): add RetainedLease + HTTPClient.ListRetentions (ENG-333)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Add `ListRetentions` to the `Backend` interface + all implementers

**Files:**
- Modify: `internal/backend/client.go` (interface), `internal/backend/mock.go`, `internal/backend/docker/retention.go` (new), `internal/backend/k3s/provision_stub.go`
- Modify (test fakes): `internal/backend/router_bench_test.go`, `internal/provisioner/manager_bench_test.go`, `internal/provisioner/manager_stress_test.go`, `internal/provisioner/manager_test.go`, `internal/provisioner/reconciler_test.go`
- Test: `internal/backend/mock_test.go`

> Adding to the interface forces every implementer to provide the method in the same commit so `go build ./...` stays green.

- [ ] **Step 1: Write the failing mock test**

Add to `internal/backend/mock_test.go` (ensure `context` is imported):

```go
func TestMockBackend_ListRetentions(t *testing.T) {
	m := NewMockBackend(MockBackendConfig{Name: "m"})

	// Unset → empty, non-nil (a backend with no retentions).
	got, err := m.ListRetentions(context.Background())
	require.NoError(t, err)
	assert.Empty(t, got)
	assert.NotNil(t, got)

	m.SetRetentions([]RetainedLease{{LeaseUUID: "x"}, {LeaseUUID: "y"}})
	got, err = m.ListRetentions(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "x", got[0].LeaseUUID)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/backend/ -run TestMockBackend_ListRetentions -v`
Expected: compile failure — `SetRetentions`/`ListRetentions` undefined on `*MockBackend`.

- [ ] **Step 3: Add the interface method**

In `internal/backend/client.go`, in the `Backend` interface, immediately before `// Name returns ...` / `Name() string` (~line 110), add:

```go
	// ListRetentions returns the leases whose data this backend currently
	// retains (soft-deleted, awaiting restore or grace-reap). Used by the
	// reconciler to keep placement affinity for retained leases (ENG-333).
	// Backends without retention (e.g. k3s) return an empty slice.
	ListRetentions(ctx context.Context) ([]RetainedLease, error)
```

- [ ] **Step 4: Implement on `MockBackend`**

In `internal/backend/mock.go`:

(a) In the `MockBackend` struct, after `loadStats *LoadStats` (~line 29), add:

```go
	// retentions is returned by ListRetentions. nil/empty = no retained leases.
	retentions []RetainedLease
```

(b) After the `GetLoadStats` method (~line 90), add:

```go
// SetRetentions configures the slice returned by ListRetentions.
func (m *MockBackend) SetRetentions(r []RetainedLease) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retentions = r
}

// ListRetentions returns the configured retained leases (a copy), or an empty
// non-nil slice when unset.
func (m *MockBackend) ListRetentions(_ context.Context) ([]RetainedLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]RetainedLease, len(m.retentions))
	copy(out, m.retentions)
	return out, nil
}
```

- [ ] **Step 5: Implement on `docker.Backend`**

Create `internal/backend/docker/retention.go`:

```go
package docker

import (
	"context"

	"github.com/manifest-network/fred/internal/backend"
)

// ListRetentions returns the leases this docker backend currently retains
// (soft-deleted, awaiting restore or grace-reap), read from the retention
// store. Used by fred's reconciler for restore backend affinity (ENG-333).
func (b *Backend) ListRetentions(_ context.Context) ([]backend.RetainedLease, error) {
	entries, err := b.retentionStore.List()
	if err != nil {
		return nil, err
	}
	out := make([]backend.RetainedLease, 0, len(entries))
	for _, e := range entries {
		out = append(out, backend.RetainedLease{LeaseUUID: e.OriginalLeaseUUID})
	}
	return out, nil
}
```

- [ ] **Step 6: Implement on `k3s.Backend`**

In `internal/backend/k3s/provision_stub.go`, after the `ListProvisions` method (~line 348+), add:

```go
// ListRetentions returns an empty slice: the k3s backend has no retention
// store (retention/restore is docker-only, ENG-325). Implemented so the k3s
// backend satisfies the backend.Backend interface (ENG-333).
func (b *Backend) ListRetentions(_ context.Context) ([]backend.RetainedLease, error) {
	return []backend.RetainedLease{}, nil
}
```

- [ ] **Step 7: Add `ListRetentions` to every test fake the compiler flags**

Run: `go build ./...`
The build will fail for each test double implementing `backend.Backend`. For each flagged fake type (in `internal/backend/router_bench_test.go`, `internal/provisioner/manager_bench_test.go`, `internal/provisioner/manager_stress_test.go`, `internal/provisioner/manager_test.go`, `internal/provisioner/reconciler_test.go`), add this method (adjust the receiver name/type to match the fake):

```go
func (m *<fakeType>) ListRetentions(_ context.Context) ([]backend.RetainedLease, error) {
	return nil, nil
}
```

Re-run `go build ./...` until green. (Search each file for its existing `Deprovision` method to find the exact receiver type.)

- [ ] **Step 8: Run tests + build to verify**

Run: `go test ./internal/backend/ -run TestMockBackend_ListRetentions -v`
Expected: PASS.
Run: `go build ./... && go test ./internal/backend/... -count=1`
Expected: success.

- [ ] **Step 9: Commit**

```bash
git add internal/backend/client.go internal/backend/mock.go internal/backend/mock_test.go internal/backend/docker/retention.go internal/backend/k3s/provision_stub.go internal/backend/router_bench_test.go internal/provisioner/manager_bench_test.go internal/provisioner/manager_stress_test.go internal/provisioner/manager_test.go internal/provisioner/reconciler_test.go
git commit -m "feat(backend): add ListRetentions to Backend interface + all implementers (ENG-333)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Expose `GET /retentions` on both backend HTTP servers

**Files:**
- Modify: `cmd/docker-backend/main.go`, `cmd/k3s-backend/server.go`
- Test: `cmd/docker-backend/main_test.go` (mirror existing handler tests)

- [ ] **Step 1: Write the failing handler test**

Find an existing list handler test (e.g. for `handleListProvisions`) in `cmd/docker-backend/main_test.go` and mirror it. Add:

```go
func TestHandleListRetentions(t *testing.T) {
	svc := &mockBackendService{
		listRetentionsFunc: func(ctx context.Context) ([]backend.RetainedLease, error) {
			return []backend.RetainedLease{{LeaseUUID: "ret-1"}}, nil
		},
	}
	s := NewServer(svc, "secret", slog.Default())

	req := httptest.NewRequest(http.MethodGet, "/retentions", nil)
	w := httptest.NewRecorder()
	s.handleListRetentions(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp backend.ListRetentionsResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.Len(t, resp.Retentions, 1)
	assert.Equal(t, "ret-1", resp.Retentions[0].LeaseUUID)
}
```

(If the test file's existing backend mock is named differently than `mockBackendService`, use that type and add a `listRetentionsFunc` field + method to it, mirroring its existing `ListProvisions` mock.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./cmd/docker-backend/ -run TestHandleListRetentions -v`
Expected: compile failure — `handleListRetentions` / `listRetentionsFunc` undefined.

- [ ] **Step 3: Add `ListRetentions` to the docker `backendService` interface**

In `cmd/docker-backend/main.go`, in the `backendService interface` (~line 243), add (near `ListProvisions`):

```go
	ListRetentions(ctx context.Context) ([]backend.RetainedLease, error)
```

If the test file defines a mock implementing `backendService`, add the field + method there too:

```go
	listRetentionsFunc func(ctx context.Context) ([]backend.RetainedLease, error)
```
```go
func (m *mockBackendService) ListRetentions(ctx context.Context) ([]backend.RetainedLease, error) {
	if m.listRetentionsFunc != nil {
		return m.listRetentionsFunc(ctx)
	}
	return nil, nil
}
```

- [ ] **Step 4: Add the handler + route**

In `cmd/docker-backend/main.go`, add the handler (near `handleListProvisions`):

```go
func (s *Server) handleListRetentions(w http.ResponseWriter, r *http.Request) {
	retentions, err := s.backend.ListRetentions(r.Context())
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	// Serialize as `[]` not `null` even if the backend returned a nil slice.
	if retentions == nil {
		retentions = []backend.RetainedLease{}
	}
	s.writeJSON(w, http.StatusOK, backend.ListRetentionsResponse{Retentions: retentions})
}
```

Register the route in the mux setup (near `mux.Handle("GET /provisions", ...)`, ~line 286):

```go
	mux.Handle("GET /retentions", authMw(http.HandlerFunc(s.handleListRetentions)))
```

- [ ] **Step 5: Mirror on the k3s server**

In `cmd/k3s-backend/server.go`:

(a) Add to the `backendService interface` (~line 37):

```go
	ListRetentions(ctx context.Context) ([]backend.RetainedLease, error)
```

(b) Add the handler (near `handleListProvisions`):

```go
func (s *Server) handleListRetentions(w http.ResponseWriter, r *http.Request) {
	retentions, err := s.backend.ListRetentions(r.Context())
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	if retentions == nil {
		retentions = []backend.RetainedLease{}
	}
	s.writeJSON(w, http.StatusOK, backend.ListRetentionsResponse{Retentions: retentions})
}
```

(c) Register the route (near `mux.Handle("GET /provisions", ...)`, ~line 79):

```go
	mux.Handle("GET /retentions", authMw(http.HandlerFunc(s.handleListRetentions)))
```

- [ ] **Step 6: Run tests + build to verify**

Run: `go test ./cmd/docker-backend/ -run TestHandleListRetentions -v`
Expected: PASS.
Run: `go build ./...`
Expected: success (docker `Backend` and k3s `Backend` already satisfy the new `backendService` method from Task 2).

- [ ] **Step 7: Commit**

```bash
git add cmd/docker-backend/main.go cmd/docker-backend/main_test.go cmd/k3s-backend/server.go
git commit -m "feat(backend): expose GET /retentions on docker + k3s servers (ENG-333)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `orchestrator.Deprovision` stops deleting placement (#1)

**Files:**
- Modify: `internal/provisioner/orchestrator.go`
- Test: `internal/provisioner/orchestrator_test.go`

> Placement now survives close; the reconciler (Task 9) is the sole pruner. `DeletePlacement` (used by the rejected-PENDING path in `handler_set.go`) is unchanged — a rejected provision still cleans up immediately.

- [ ] **Step 1: Write the failing test**

Add to `internal/provisioner/orchestrator_test.go` (use the existing mock placement store in that file; if it tracks `Delete` calls, assert none):

```go
func TestOrchestrator_Deprovision_KeepsPlacement(t *testing.T) {
	placements := newMockPlacementStore() // existing test helper in this package
	placements.Set("lease-1", "backend-a")

	mb := backend.NewMockBackend(backend.MockBackendConfig{Name: "backend-a"})
	router := newMockBackendRouter() // existing helper; GetBackendByName("backend-a") => mb
	router.byName["backend-a"] = mb

	o := NewProvisionOrchestrator("provider-1", "http://cb", router, NewInFlightTracker(), placements)

	require.NoError(t, o.Deprovision(context.Background(), "lease-1", "sku-1"))

	// Placement must SURVIVE deprovision (restore affinity); reconciler prunes later.
	assert.Equal(t, "backend-a", placements.Get("lease-1"))
}
```

(Adapt `newMockPlacementStore`/`newMockBackendRouter` to the actual helper names already present in `orchestrator_test.go`. If a deprovision test already exists asserting placement *deletion*, update its expectation to assert the placement is retained.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/provisioner/ -run TestOrchestrator_Deprovision_KeepsPlacement -v`
Expected: FAIL — placement is currently deleted, so `Get` returns "".

- [ ] **Step 3: Remove the two placement-delete blocks in `Deprovision`**

In `internal/provisioner/orchestrator.go`, in the `Deprovision` method:

(a) Delete this block in the resolved-backend success path (~lines 217-220):

```go
		// Clean up placement record
		if o.placementStore != nil {
			o.placementStore.Delete(leaseUUID)
		}
```

(b) Delete this block in the fallback path (~lines 254-258):

```go
	// Always clean up placement record after fallback deprovision —
	// if we reached the fallback path, the placement was already stale.
	if o.placementStore != nil {
		o.placementStore.Delete(leaseUUID)
	}
```

Add a short comment where (a) was, to document the deliberate change:

```go
		// Placement is intentionally NOT deleted here (ENG-333). It is a derived
		// index of where the lease's data lives; if the backend retained the
		// volumes, the placement must survive close so a restore can route to it.
		// The reconciler is the sole pruner (cleanupOrphanedPlacements), gated on
		// the lease being terminal on chain AND absent from all backends.
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/provisioner/ -run TestOrchestrator_Deprovision -v`
Expected: PASS.
Run: `go build ./...`
Expected: success.

- [ ] **Step 5: Commit**

```bash
git add internal/provisioner/orchestrator.go internal/provisioner/orchestrator_test.go
git commit -m "feat(provisioner): keep placement across deprovision; reconciler is sole pruner (ENG-333)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Honor existing placement before least-loaded routing (#3)

**Files:**
- Modify: `internal/provisioner/orchestrator.go`, `internal/provisioner/reconciler.go`
- Test: `internal/provisioner/orchestrator_test.go`, `internal/provisioner/reconciler_test.go`

> A restored (or any already-placed) lease must re-provision on the backend that holds its data, not the least-loaded one. Both provisioning paths check placement first.

- [ ] **Step 1: Write the failing orchestrator test**

Add to `internal/provisioner/orchestrator_test.go`:

```go
func TestOrchestrator_StartProvisioning_HonorsPlacement(t *testing.T) {
	placements := newMockPlacementStore()
	placements.Set("lease-1", "backend-pinned")

	pinned := backend.NewMockBackend(backend.MockBackendConfig{Name: "backend-pinned"})
	leastLoaded := backend.NewMockBackend(backend.MockBackendConfig{Name: "backend-least"})
	router := newMockBackendRouter()
	router.byName["backend-pinned"] = pinned
	router.byName["backend-least"] = leastLoaded
	// RouteForProvision would pick the least-loaded backend (NOT the pinned one).
	router.routeForProvisionFn = func(_ context.Context, _ string, _ map[string]int) backend.Backend {
		return leastLoaded
	}

	o := NewProvisionOrchestrator("provider-1", "http://cb", router, NewInFlightTracker(), placements)
	lease := &billingtypes.Lease{Uuid: "lease-1", Tenant: "t", /* items as existing tests set them */}

	require.NoError(t, o.StartProvisioning(context.Background(), lease, ProvisionOpts{}))

	// Provision must go to the pinned backend, not the least-loaded one.
	_, onPinned := pinned.GetProvisionState("lease-1") // use whatever the mock exposes; or assert via callback/Name
	assert.True(t, onPinned, "provision should land on the placement-pinned backend")
}
```

(Adapt assertions to the MockBackend surface used elsewhere in the file — e.g. asserting which backend received `Provision` via a recorded call. The key behavior: with placement set, `RouteForProvision` is bypassed.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/provisioner/ -run TestOrchestrator_StartProvisioning_HonorsPlacement -v`
Expected: FAIL — provision currently routes to the least-loaded backend.

- [ ] **Step 3: Add a shared routing helper**

In `internal/provisioner/orchestrator.go`, add a package-level helper (after the `Deprovision` method):

```go
// routeForProvisionHonoringPlacement returns the backend that already holds the
// lease's data (from placement) when one is recorded and reachable; otherwise it
// falls back to least-loaded selection. This keeps a restored or already-placed
// lease pinned to the backend with its volumes (ENG-333), preventing data drift
// on re-provision/reconcile.
func routeForProvisionHonoringPlacement(
	ctx context.Context,
	router BackendRouter,
	placementStore PlacementStore,
	leaseUUID, sku string,
	inFlightByBackend map[string]int,
) backend.Backend {
	if placementStore != nil {
		if name := placementStore.Get(leaseUUID); name != "" {
			if b := router.GetBackendByName(name); b != nil {
				return b
			}
			slog.Warn("placement backend not found, falling back to least-loaded routing",
				"lease_uuid", leaseUUID, "placement_backend", name)
		}
	}
	return router.RouteForProvision(ctx, sku, inFlightByBackend)
}
```

- [ ] **Step 4: Use the helper in `StartProvisioning`**

In `internal/provisioner/orchestrator.go`, replace the routing line in `StartProvisioning` (~line 56):

```go
	backendClient := o.router.RouteForProvision(ctx, sku, o.tracker.InFlightCountsByBackend())
```

with:

```go
	backendClient := routeForProvisionHonoringPlacement(ctx, o.router, o.placementStore, lease.Uuid, sku, o.tracker.InFlightCountsByBackend())
```

- [ ] **Step 5: Use the helper in `reconciler.doStartProvisioning`**

In `internal/provisioner/reconciler.go`, replace the routing line in `doStartProvisioning` (~line 393):

```go
	backendClient := r.backendRouter.RouteForProvision(ctx, sku, inFlightByBackend)
```

with:

```go
	backendClient := routeForProvisionHonoringPlacement(ctx, r.backendRouter, r.placementStore, lease.Uuid, sku, inFlightByBackend)
```

- [ ] **Step 6: Write + run a reconciler test**

Add an analogous `TestReconciler_doStartProvisioning_HonorsPlacement` to `internal/provisioner/reconciler_test.go` (mirror the existing reconciler provisioning tests; set `placementStore` to return a pinned backend and a `routeForProvisionFn` that returns a different one; assert the pinned one receives `Provision`).

Run: `go test ./internal/provisioner/ -run HonorsPlacement -v`
Expected: PASS.
Run: `go build ./...`
Expected: success.

- [ ] **Step 7: Commit**

```bash
git add internal/provisioner/orchestrator.go internal/provisioner/reconciler.go internal/provisioner/orchestrator_test.go internal/provisioner/reconciler_test.go
git commit -m "feat(provisioner): honor existing placement before least-loaded routing (ENG-333)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Route restore via the source lease's placement (#2)

**Files:**
- Modify: `internal/api/handlers.go`
- Test: `internal/api/handlers_test.go`

> Restore must authenticate the NEW lease but resolve the backend from the SOURCE lease's placement. We split `authenticateAndResolve` into an auth-only helper so restore does not resolve a backend by the new lease.

- [ ] **Step 1: Write the failing test**

Add to `internal/api/handlers_test.go` (mirror existing `RestoreLease` tests; the key assertion is which backend receives `Restore`):

```go
func TestRestoreLease_RoutesToSourcePlacementBackend(t *testing.T) {
	// Source lease's data lives on "backend-src". The new lease has no placement.
	placement := &mockPlacementLookup{getFunc: func(leaseUUID string) string {
		if leaseUUID == "source-lease" {
			return "backend-src"
		}
		return ""
	}}

	srcCalled := false
	srcBackend := /* a mock backend.Backend whose Restore sets srcCalled = true and returns nil */
	otherBackend := /* a mock backend whose Restore must NOT be called */
	router := /* backend.Router (or test router) where GetBackendByName("backend-src") => srcBackend */

	h := NewHandlers(HandlersConfig{
		Client:          /* chain mock returning a PENDING new lease */,
		BackendRouter:   router,
		PlacementLookup: placement,
		ProviderUUID:    "prov-1",
	})

	// POST /v1/leases/new-lease/restore {from_lease_uuid: source-lease}
	// ... build authenticated request for "new-lease" (PENDING) ...

	// Execute handler, assert 202 and srcCalled == true, otherBackend.Restore not called.
	assert.True(t, srcCalled, "restore must route to the source placement backend")
}

func TestRestoreLease_NoSourcePlacement_Returns404(t *testing.T) {
	placement := &mockPlacementLookup{getFunc: func(string) string { return "" }} // no placement
	// ... build PENDING new-lease restore request ...
	// Assert HTTP 404 (ErrNotRetained-equivalent) and NO backend Restore call.
}
```

(Use the existing `RestoreLease` test scaffolding in `handlers_test.go` for request construction/auth. `BackendRouter` is `*backend.Router`; build one with named `MockBackend`s via `backend.NewRouter`.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/api/ -run TestRestoreLease_Routes -v`
Expected: FAIL — restore currently routes via the new lease (arbitrary backend), not the source placement.

- [ ] **Step 3: Add an auth-only helper**

In `internal/api/handlers.go`, after `authenticateAndResolve` (~line 204), add:

```go
// authenticateLease authenticates a lease request WITHOUT resolving a backend.
// Restore uses this because it must resolve the backend from the SOURCE lease's
// placement (read from the request body), not from the path-param (new) lease.
func (h *Handlers) authenticateLease(w http.ResponseWriter, r *http.Request, checkReplay, requireActive bool) (auth *AuthenticatedRequest, leaseUUID string, ok bool) {
	leaseUUID = r.PathValue("lease_uuid")
	auth, status, err := h.AuthenticateLeaseRequest(r, leaseUUID, checkReplay, requireActive)
	if err != nil {
		writeError(w, err.Error(), status)
		return nil, leaseUUID, false
	}
	if h.backendRouter == nil {
		slog.Error("backend router not configured")
		writeError(w, errMsgServiceNotConfigured, http.StatusServiceUnavailable)
		return nil, leaseUUID, false
	}
	return auth, leaseUUID, true
}
```

- [ ] **Step 4: Rewrite the head of `RestoreLease` to resolve by source placement**

In `internal/api/handlers.go`, in `RestoreLease`, replace the opening resolution (the `authenticateAndResolve` call, ~lines 565-568):

```go
	auth, leaseUUID, backendClient, ok := h.authenticateAndResolve(w, r, true, false)
	if !ok {
		return
	}
```

with auth-only:

```go
	auth, leaseUUID, ok := h.authenticateLease(w, r, true, false)
	if !ok {
		return
	}
```

Then, AFTER the body is decoded and `body.FromLeaseUUID` is validated (after the `IsValidUUID` check, ~line 595), insert backend resolution by the SOURCE placement:

```go
	// Resolve the backend that holds the SOURCE lease's retained data. Restore
	// is same-backend: the retained volumes live only where the source lease was
	// provisioned (ENG-333). No placement / backend gone ⇒ no retained data here.
	backendClient := h.resolveBackendByPlacement(body.FromLeaseUUID)
	if backendClient == nil {
		writeError(w, "no retained data found for that lease", http.StatusNotFound)
		return
	}
```

- [ ] **Step 5: Add the placement-only resolver**

In `internal/api/handlers.go`, after `resolveBackend` (~line 221), add:

```go
// resolveBackendByPlacement returns the backend recorded in placement for the
// given lease, or nil when there is no placement or the named backend is gone.
// Unlike resolveBackend it never falls back to SKU routing — for restore, a
// missing placement means "no retained data here", which must surface as 404,
// not a guess at an arbitrary backend (ENG-333).
func (h *Handlers) resolveBackendByPlacement(leaseUUID string) backend.Backend {
	if h.placementLookup == nil {
		return nil
	}
	name := h.placementLookup.Get(leaseUUID)
	if name == "" {
		return nil
	}
	return h.backendRouter.GetBackendByName(name)
}
```

- [ ] **Step 6: Run tests + build**

Run: `go test ./internal/api/ -run TestRestoreLease -v`
Expected: PASS (new tests + existing restore tests; update any existing restore test that assumed new-lease routing to seed a `from_lease_uuid` placement).
Run: `go build ./...`
Expected: success.

- [ ] **Step 7: Commit**

```bash
git add internal/api/handlers.go internal/api/handlers_test.go
git commit -m "feat(api): route restore to the source lease's placement backend (ENG-333)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Restore-success placement bookkeeping owned by the orchestrator (#4)

**Files:**
- Modify: `internal/provisioner/orchestrator.go`, `internal/api/handlers.go`, `internal/api/server.go`
- Test: `internal/provisioner/orchestrator_test.go`, `internal/api/handlers_test.go`

> On a successful restore the new lease adopts the source's backend. Record `placement[new]=backend` and release `placement[source]` immediately (the 5-minute reconcile interval makes this window real). The write is owned by the orchestrator; the API calls it through a small recorder seam.

- [ ] **Step 1: Write the failing orchestrator test**

Add to `internal/provisioner/orchestrator_test.go`:

```go
func TestOrchestrator_RecordRestorePlacement(t *testing.T) {
	placements := newMockPlacementStore()
	placements.Set("source-lease", "backend-a") // preserved across close

	o := NewProvisionOrchestrator("prov-1", "http://cb", newMockBackendRouter(), NewInFlightTracker(), placements)

	o.RecordRestorePlacement("new-lease", "source-lease", "backend-a")

	assert.Equal(t, "backend-a", placements.Get("new-lease"), "new lease adopts the source backend")
	assert.Equal(t, "", placements.Get("source-lease"), "source placement released on adopt")
}

func TestOrchestrator_RecordRestorePlacement_NilStore(t *testing.T) {
	o := NewProvisionOrchestrator("prov-1", "http://cb", newMockBackendRouter(), NewInFlightTracker(), nil)
	// Must not panic with a nil placement store.
	o.RecordRestorePlacement("new-lease", "source-lease", "backend-a")
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/provisioner/ -run TestOrchestrator_RecordRestorePlacement -v`
Expected: compile failure — `RecordRestorePlacement` undefined.

- [ ] **Step 3: Implement `RecordRestorePlacement` on the orchestrator**

In `internal/provisioner/orchestrator.go`, after `DeletePlacement` (~line 148), add:

```go
// RecordRestorePlacement records placement bookkeeping after a successful
// restore: the new lease adopts the backend that held the source's retained
// data, and the source lease's placement is released. Typed-nil safe. The
// reconciler converges to the same state from backend ground truth; this just
// closes the post-restore reconcile window (ENG-333).
func (o *ProvisionOrchestrator) RecordRestorePlacement(newLeaseUUID, sourceLeaseUUID, backendName string) {
	if o.placementStore == nil {
		return
	}
	if err := o.placementStore.Set(newLeaseUUID, backendName); err != nil {
		slog.Warn("failed to record restore placement",
			"lease_uuid", newLeaseUUID, "backend", backendName, "error", err)
	}
	o.placementStore.Delete(sourceLeaseUUID)
}
```

- [ ] **Step 4: Add the recorder seam to the API and call it**

In `internal/api/handlers.go`:

(a) After the `PlacementLookup` interface (~line 47), add:

```go
// RestorePlacementRecorder records placement bookkeeping after a successful
// restore (new lease adopts the source's backend; source placement released).
// Optional — when nil, the reconciler still converges (ENG-333).
type RestorePlacementRecorder interface {
	RecordRestorePlacement(newLeaseUUID, sourceLeaseUUID, backendName string)
}
```

(b) In the `Handlers` struct (~line 50), after `placementLookup PlacementLookup`, add:

```go
	restoreRecorder   RestorePlacementRecorder
```

(c) In `HandlersConfig` (~line 65), after `PlacementLookup PlacementLookup`, add:

```go
	RestoreRecorder RestorePlacementRecorder // optional — restore placement bookkeeping (ENG-333)
```

(d) In `NewHandlers` (~line 80), after `placementLookup: cfg.PlacementLookup,`, add:

```go
		restoreRecorder: cfg.RestoreRecorder,
```

(e) In `RestoreLease`, after the `backendClient.Restore(...)` call returns success (after the `if err != nil { ... }` block, before/around the `h.eventBroker` publish, ~line 627), add:

```go
	// Adopt bookkeeping: the new lease now lives on the source's backend; release
	// the source placement. Owned by the orchestrator (state owner); the
	// reconciler converges to the same regardless (ENG-333).
	if h.restoreRecorder != nil {
		h.restoreRecorder.RecordRestorePlacement(leaseUUID, body.FromLeaseUUID, backendClient.Name())
	}
```

- [ ] **Step 5: Wire `RestoreRecorder` through the server**

In `internal/api/server.go`:

(a) In `ServerDeps` (~line 100), after `PlacementLookup PlacementLookup`, add:

```go
	RestoreRecorder RestorePlacementRecorder // Optional — restore placement bookkeeping (ENG-333).
```

(b) Where `HandlersConfig` is constructed (near `PlacementLookup: placementLookup,`, ~line 147), add:

```go
		RestoreRecorder: deps.RestoreRecorder,
```

- [ ] **Step 6: Write + run an API test for the recorder call**

Add to `internal/api/handlers_test.go` a test that injects a fake `RestorePlacementRecorder` (records its args) and asserts that after a successful restore it was called with `(newLease, sourceLease, "backend-src")`. Mirror `TestRestoreLease_RoutesToSourcePlacementBackend` for setup.

```go
type fakeRestoreRecorder struct {
	newLease, sourceLease, backend string
	called                        bool
}

func (f *fakeRestoreRecorder) RecordRestorePlacement(n, s, b string) {
	f.called, f.newLease, f.sourceLease, f.backend = true, n, s, b
}
```

Run: `go test ./internal/api/ -run TestRestoreLease -v`
Expected: PASS.
Run: `go build ./...`
Expected: success.

- [ ] **Step 7: Commit**

```bash
git add internal/provisioner/orchestrator.go internal/api/handlers.go internal/api/server.go internal/provisioner/orchestrator_test.go internal/api/handlers_test.go
git commit -m "feat: record restore placement on adopt; release source placement (ENG-333)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Reconciler derives placement from retentions (#5, additive)

**Files:**
- Modify: `internal/provisioner/reconciler.go`
- Test: `internal/provisioner/reconciler_test.go`

> The reconciler unions each backend's retained leases into the placement sync, so retained-lease placement is re-derived from ground truth (self-heals a missed edge write or a providerd restart).

- [ ] **Step 1: Write the failing test**

Add to `internal/provisioner/reconciler_test.go` (mirror existing reconciler tests; the key is that a `MockBackend` with `SetRetentions` causes the reconciler to record placement for the retained lease):

```go
func TestReconciler_SyncsPlacementFromRetentions(t *testing.T) {
	mb := backend.NewMockBackend(backend.MockBackendConfig{Name: "backend-a"})
	mb.SetRetentions([]backend.RetainedLease{{LeaseUUID: "retained-1"}})

	placements := newMockPlacementStore()
	r := newTestReconciler(t, []backend.Backend{mb}, placements) // mirror existing reconciler test setup
	// chain returns no leases; backend has no active provisions, one retention.

	require.NoError(t, r.RunOnce(context.Background()))

	assert.Equal(t, "backend-a", placements.Get("retained-1"),
		"reconciler must derive placement for a retained lease")
}
```

(Adapt `newTestReconciler` to the existing reconciler test harness — chain client mock returning empty pending/active, the backend router wrapping `mb`, and the placement store.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./internal/provisioner/ -run TestReconciler_SyncsPlacementFromRetentions -v`
Expected: FAIL — placement for `retained-1` is empty (retentions not synced yet).

- [ ] **Step 3: Add `fetchAllRetentions`**

In `internal/provisioner/reconciler.go`, after `fetchAllProvisions` (~line 668), add:

```go
// fetchAllRetentions queries every backend's retained leases in parallel,
// returning leaseUUID→backendName and whether ALL backends answered
// successfully. The complete flag gates placement pruning: a partial result
// must never prune (a transient backend outage would look like "data gone").
func (r *Reconciler) fetchAllRetentions(ctx context.Context) (map[string]string, bool) {
	backends := r.backendRouter.Backends()

	var mu sync.Mutex
	out := make(map[string]string)
	complete := true

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(len(backends))
	for _, b := range backends {
		g.Go(func() (goErr error) {
			defer func() {
				if rec := recover(); rec != nil {
					slog.Error("reconciler retentions fetch panic — recovering",
						"backend", b.Name(), "panic", rec, "stack", string(debug.Stack()))
					mu.Lock()
					complete = false
					mu.Unlock()
					goErr = nil
				}
			}()
			retentions, err := b.ListRetentions(gctx)
			if err != nil {
				slog.Warn("failed to list retentions from backend",
					"backend", b.Name(), "error", err)
				mu.Lock()
				complete = false
				mu.Unlock()
				return nil // collect from other backends; don't cancel
			}
			mu.Lock()
			for _, ret := range retentions {
				out[ret.LeaseUUID] = b.Name()
			}
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait() // closures never return non-nil; errors captured via complete

	return out, complete
}
```

- [ ] **Step 4: Fetch retentions and union them into the placement sync**

In `internal/provisioner/reconciler.go`, in `RunOnce`, immediately after the `allProvisions` fetch + log (~line 198), add:

```go
	// Retained leases also pin a backend (restore affinity, ENG-333). Derive
	// their placement from ground truth alongside active provisions.
	allRetentions, retentionsComplete := r.fetchAllRetentions(ctx)
	slog.Info("fetched backend retentions", "total", len(allRetentions), "complete", retentionsComplete)
```

Then update the placement-sync block (~lines 206-218) to union retentions. Replace:

```go
	if r.placementStore != nil && len(allProvisions) > 0 {
		placements := make(map[string]string, len(allProvisions))
		for leaseUUID, provision := range allProvisions {
			if provision.BackendName != "" {
				placements[leaseUUID] = provision.BackendName
			}
		}
		if len(placements) > 0 {
			if err := r.placementStore.SetBatch(placements); err != nil {
				slog.Warn("failed to sync placements from backend state", "error", err)
			}
		}
	}
```

with:

```go
	if r.placementStore != nil && (len(allProvisions) > 0 || len(allRetentions) > 0) {
		placements := make(map[string]string, len(allProvisions)+len(allRetentions))
		for leaseUUID, provision := range allProvisions {
			if provision.BackendName != "" {
				placements[leaseUUID] = provision.BackendName
			}
		}
		// Retained leases pin their backend too. Active provisions take precedence
		// (a lease cannot be both active and retained on the same backend; if a
		// stale retention races a fresh provision, the provision wins).
		for leaseUUID, backendName := range allRetentions {
			if _, isActive := placements[leaseUUID]; !isActive {
				placements[leaseUUID] = backendName
			}
		}
		if len(placements) > 0 {
			if err := r.placementStore.SetBatch(placements); err != nil {
				slog.Warn("failed to sync placements from backend state", "error", err)
			}
		}
	}
```

- [ ] **Step 5: Run tests + build**

Run: `go test ./internal/provisioner/ -run TestReconciler_SyncsPlacementFromRetentions -v`
Expected: PASS.
Run: `go build ./... && go test ./internal/provisioner/... -short -count=1`
Expected: success.

- [ ] **Step 6: Commit**

```bash
git add internal/provisioner/reconciler.go internal/provisioner/reconciler_test.go
git commit -m "feat(provisioner): derive placement from backend retentions (ENG-333)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Reconciler prunes orphaned placements (#5, gated)

**Files:**
- Modify: `internal/provisioner/reconciler.go`
- Test: `internal/provisioner/reconciler_test.go`

> The sole pruner. Strictly gated to answer the documented additive-only race: prune `placement[L]` only when retentions were fully fetched AND L is absent from all backends (provisions ∪ retentions) AND L is not in-flight AND L is chain-terminal (absent from chain, or closed/rejected).

- [ ] **Step 1: Write the failing tests (gates + happy path)**

Add to `internal/provisioner/reconciler_test.go`:

```go
func TestReconciler_PrunesOrphanedPlacement(t *testing.T) {
	// "gone-lease" has a placement but is on no backend, not in-flight, not on chain.
	placements := newMockPlacementStore()
	placements.Set("gone-lease", "backend-a")
	placements.Set("retained-1", "backend-a") // still retained → must survive

	mb := backend.NewMockBackend(backend.MockBackendConfig{Name: "backend-a"})
	mb.SetRetentions([]backend.RetainedLease{{LeaseUUID: "retained-1"}})

	r := newTestReconciler(t, []backend.Backend{mb}, placements) // chain returns no leases
	require.NoError(t, r.RunOnce(context.Background()))

	assert.Equal(t, "", placements.Get("gone-lease"), "orphan pruned")
	assert.Equal(t, "backend-a", placements.Get("retained-1"), "retained lease kept")
}

func TestReconciler_DoesNotPruneOnIncompleteRetentions(t *testing.T) {
	placements := newMockPlacementStore()
	placements.Set("gone-lease", "backend-a")

	// A backend whose ListRetentions errors → retentionsComplete=false → no prune.
	failing := &statsErrRetentionBackend{MockBackend: backend.NewMockBackend(backend.MockBackendConfig{Name: "backend-a"})}

	r := newTestReconciler(t, []backend.Backend{failing}, placements)
	require.NoError(t, r.RunOnce(context.Background()))

	assert.Equal(t, "backend-a", placements.Get("gone-lease"),
		"must NOT prune when a backend's retentions could not be fetched")
}

func TestReconciler_DoesNotPruneActiveOrInFlight(t *testing.T) {
	placements := newMockPlacementStore()
	placements.Set("active-lease", "backend-a")   // ACTIVE on chain
	placements.Set("inflight-lease", "backend-a") // in the in-flight tracker

	mb := backend.NewMockBackend(backend.MockBackendConfig{Name: "backend-a"})
	tracker := NewInFlightTracker()
	tracker.TrackInFlight("inflight-lease", "t", nil, "backend-a")

	r := newTestReconcilerWithTracker(t, []backend.Backend{mb}, placements, tracker,
		/* chain active leases: {"active-lease"} */)
	require.NoError(t, r.RunOnce(context.Background()))

	assert.Equal(t, "backend-a", placements.Get("active-lease"), "active lease kept")
	assert.Equal(t, "backend-a", placements.Get("inflight-lease"), "in-flight lease kept")
}
```

Add the test helper backend that fails `ListRetentions`:

```go
type statsErrRetentionBackend struct {
	*backend.MockBackend
}

func (b *statsErrRetentionBackend) ListRetentions(context.Context) ([]backend.RetainedLease, error) {
	return nil, errors.New("retentions unavailable")
}
```

(Adapt `newTestReconciler` / `newTestReconcilerWithTracker` to the existing reconciler harness; the second seeds the in-flight tracker and chain active-lease set.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/provisioner/ -run 'TestReconciler_Prunes|TestReconciler_DoesNotPrune' -v`
Expected: FAIL/compile error — `cleanupOrphanedPlacements` not implemented.

- [ ] **Step 3: Implement `cleanupOrphanedPlacements`**

In `internal/provisioner/reconciler.go`, after `cleanupOrphanedPayloads` (~line 1073), add (modeled on it):

```go
// cleanupOrphanedPlacements is the SOLE pruner of the placement index (ENG-333).
// It deletes a placement only when ALL of these hold, so it never races a
// concurrent StartProvisioning Set nor wipes valid placement on a backend
// outage:
//   - retentionsComplete: every backend answered ListRetentions this sweep
//     (provisions completeness is already guaranteed — fetchAllProvisions aborts
//     reconciliation on any backend error before we reach here);
//   - the lease is absent from backendLeases (provisions ∪ retentions);
//   - the lease is not in-flight (a just-Set placement the backends haven't
//     reported yet — the exact race the old additive-only code avoided);
//   - the lease is chain-terminal: absent from chainLeases, or present but
//     neither PENDING nor ACTIVE (closed/rejected/expired).
//
// Returns the number of placements pruned.
func (r *Reconciler) cleanupOrphanedPlacements(
	ctx context.Context,
	chainLeases map[string]billingtypes.Lease,
	backendLeases map[string]struct{},
	retentionsComplete bool,
) int {
	if r.placementStore == nil || !retentionsComplete {
		return 0
	}

	cleaned := 0
	for _, leaseUUID := range r.placementStore.List() {
		if ctx.Err() != nil {
			break
		}
		// Data still lives on a backend (active provision or retained) → keep.
		if _, onBackend := backendLeases[leaseUUID]; onBackend {
			continue
		}
		// A provision Set this placement moments ago; backends/chain may not
		// reflect it yet → keep (the documented additive-only race).
		if r.tracker != nil {
			if _, inFlight := r.tracker.GetInFlight(leaseUUID); inFlight {
				continue
			}
		}
		// Keep if the lease is still PENDING/ACTIVE on chain (the reconciler's
		// main loop owns re-provisioning those; pruning would race it).
		if lease, exists := chainLeases[leaseUUID]; exists &&
			(lease.State == billingtypes.LEASE_STATE_PENDING || lease.State == billingtypes.LEASE_STATE_ACTIVE) {
			continue
		}
		// Chain-terminal, absent from all backends, not in-flight → orphan.
		r.placementStore.Delete(leaseUUID)
		cleaned++
		slog.Info("reconcile: pruned orphaned placement",
			"lease_uuid", leaseUUID)
	}
	return cleaned
}
```

- [ ] **Step 4: Call the pruner from `RunOnce`**

In `internal/provisioner/reconciler.go`, near where `cleanupOrphanedPayloads` is called (~line 352), add a call. First build the `backendLeases` set from provisions ∪ retentions (place this right after the placement-sync block from Task 8, ~line 220):

```go
	// Authoritative set of leases whose data lives on some backend (active or
	// retained), used by placement pruning below.
	backendLeases := make(map[string]struct{}, len(allProvisions)+len(allRetentions))
	for leaseUUID := range allProvisions {
		backendLeases[leaseUUID] = struct{}{}
	}
	for leaseUUID := range allRetentions {
		backendLeases[leaseUUID] = struct{}{}
	}
```

Then near the orphaned-payload cleanup call (~line 352), add:

```go
	prunedPlacements := r.cleanupOrphanedPlacements(ctx, chainLeases, backendLeases, retentionsComplete)
	if prunedPlacements > 0 {
		slog.Info("reconcile: pruned orphaned placements", "count", prunedPlacements)
	}
```

- [ ] **Step 5: Run tests + build (with race)**

Run: `go test ./internal/provisioner/ -run 'TestReconciler_Prunes|TestReconciler_DoesNotPrune|TestReconciler_SyncsPlacement' -race -v`
Expected: PASS.
Run: `go build ./... && go test ./internal/provisioner/... -short -race -count=1`
Expected: success.

- [ ] **Step 6: Commit**

```bash
git add internal/provisioner/reconciler.go internal/provisioner/reconciler_test.go
git commit -m "feat(provisioner): prune orphaned placements (gated, sole pruner) (ENG-333)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: Wire the recorder in providerd + integration test + full verification

**Files:**
- Modify: `cmd/providerd/main.go`
- Test: `internal/backend/docker/integration_restore_test.go` (mirror existing docker integration tests)

- [ ] **Step 1: Wire the orchestrator as `RestoreRecorder`**

In `cmd/providerd/main.go`, where `api.ServerDeps{...}` (or the equivalent server construction) is built, set the new field to the orchestrator instance that owns the placement store:

```go
		RestoreRecorder: orchestrator, // *provisioner.ProvisionOrchestrator implements RestorePlacementRecorder
```

(Find the existing `PlacementLookup:` wiring in `cmd/providerd/main.go` and add `RestoreRecorder:` alongside it, using the same orchestrator variable that was constructed with the placement store.)

Run: `go build ./...`
Expected: success (compile-time check that `*ProvisionOrchestrator` satisfies `api.RestorePlacementRecorder`).

- [ ] **Step 2: Add a compile-time assertion**

In `internal/provisioner/orchestrator.go`, near the top (after imports), add:

```go
// Compile-time check that the orchestrator can serve as the API's restore
// placement recorder (ENG-333).
var _ interface {
	RecordRestorePlacement(newLeaseUUID, sourceLeaseUUID, backendName string)
} = (*ProvisionOrchestrator)(nil)
```

- [ ] **Step 3: Write the multi-backend restore integration test**

In `internal/backend/docker/integration_restore_test.go` (guarded like the other docker integration tests — they require a docker daemon), add a test that mirrors the existing restore integration setup but uses TWO backends and asserts affinity. If a two-backend docker integration harness does not exist, add a focused reconciler-level integration test in `internal/provisioner/reconciler_test.go` instead:

```go
func TestRestoreAffinity_EndToEnd_MultiBackend(t *testing.T) {
	// b1, b2 are mock backends in one router. The source lease's data is retained
	// on b2 only. A fresh restore must route to b2 regardless of least-loaded order.
	b1 := backend.NewMockBackend(backend.MockBackendConfig{Name: "b1"})
	b2 := backend.NewMockBackend(backend.MockBackendConfig{Name: "b2"})
	b2.SetRetentions([]backend.RetainedLease{{LeaseUUID: "source"}})
	// Make b1 the least-loaded so naive routing would pick the WRONG backend.
	b1.SetLoadStats(&backend.LoadStats{TotalCPUCores: 100, AllocatedCPUCores: 0})
	b2.SetLoadStats(&backend.LoadStats{TotalCPUCores: 100, AllocatedCPUCores: 90})

	placements := newMockPlacementStore()
	r := newTestReconciler(t, []backend.Backend{b1, b2}, placements)
	require.NoError(t, r.RunOnce(context.Background())) // derives placement[source]=b2

	assert.Equal(t, "b2", placements.Get("source"),
		"restore affinity: source placement resolves to the retaining backend, not the least-loaded one")
}
```

- [ ] **Step 4: Run the test + full suite**

Run: `go test ./internal/provisioner/ -run TestRestoreAffinity_EndToEnd_MultiBackend -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/providerd/main.go internal/provisioner/orchestrator.go internal/backend/docker/integration_restore_test.go internal/provisioner/reconciler_test.go
git commit -m "feat: wire restore recorder + multi-backend restore affinity test (ENG-333)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final Verification

- [ ] **Build the whole module**

Run: `go build ./...`
Expected: success.

- [ ] **Run the relevant suites with the race detector (short)**

Run: `go test -short -race ./internal/backend/... ./internal/provisioner/... ./internal/api/... ./cmd/docker-backend/... ./cmd/k3s-backend/...`
Expected: all PASS.

- [ ] **Lint (if configured)**

Run: `golangci-lint run ./internal/backend/... ./internal/provisioner/... ./internal/api/... ./cmd/...`
Expected: no new findings. (Watch for `misspell` and unused-parameter lints, as in ENG-318.)

---

## Acceptance Criteria Coverage (from ENG-333)

| Criterion | Task |
|-----------|------|
| Restore routes to the backend holding the source lease's retained data, independent of new-lease routing | 6 (resolve via source placement) + 1/2/3/8 (placement populated from retentions) |
| Source lease's placement survives a retain-close | 4 (stop deleting on deprovision) |
| Source placement cleaned up on adopt-success | 7 (`RecordRestorePlacement`) |
| Source placement cleaned up on grace-reap | 8/9 (reconciler re-derives, then prunes when retention gone) |
| Source has no retained placement ⇒ `ErrNotRetained` (404), unchanged | 6 (`resolveBackendByPlacement` → 404, no SKU fallback) |
| Restart/reconcile of a restored lease stays on the same backend | 5 (honor placement before `RouteForProvision`) |
| manifest-loadtest restore scenarios 14/15/16 green | external validation on the dev pool |

## Notes / Self-review

- **No `Backend.Deprovision` signature change** (interface-segregation/YAGNI). Placement survives close by deletion-removal (Task 4); the reconciler is the sole pruner (Task 9).
- **Pruning is gated** to answer the documented additive-only race (Task 9 gates: retentionsComplete + absent-from-backends + not-in-flight + chain-terminal). Provisions completeness is guaranteed because `fetchAllProvisions` aborts reconciliation on any backend error.
- **Edge optimizations justified by the 5-minute reconcile interval** (`reconciler.go:104`): Task 4 (placement survives close, no post-close window) and Task 7 (post-restore placement write, no post-restore window). The reconciler converges regardless.
- **Type consistency:** `RetainedLease{LeaseUUID string}`, `ListRetentionsResponse{Retentions []RetainedLease}`, `ListRetentions(ctx) ([]RetainedLease, error)`, `RecordRestorePlacement(new, source, backend string)`, `routeForProvisionHonoringPlacement(...)`, `resolveBackendByPlacement(leaseUUID)`, `RestorePlacementRecorder` — used consistently across tasks.
- **k3s parity:** k3s implements `ListRetentions` (empty) and exposes `GET /retentions` (empty) so a k3s backend in the pool does not disable pruning (which requires every backend to answer).
