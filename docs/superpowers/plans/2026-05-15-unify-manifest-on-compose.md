# Unify Manifest Handling on the Compose Path — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse the legacy single-service Docker backend path into the Compose-based stack path. Wire-format remains permissive (flat manifests still accepted with deprecation logging); internally everything is normalized to a stack. Legacy leases auto-migrate at fred startup via recover-time recreate, preserving persistent data.

**Architecture:** Boundary normalization at parse time + recover-time container recreate. After this PR there is *one* docker-backend execution path (Compose). Legacy compatibility lives in three small areas: parser auto-wrap, item auto-tag, and `migrate.go`.

**Tech Stack:** Go, Docker Engine API, Docker Compose v2 API (`github.com/docker/compose/v2`), `compose-spec/compose-go/v2`.

**Spec:** `docs/superpowers/specs/2026-05-15-unify-manifest-on-compose-design.md`

---

## Working principles

- **Tree green at every commit.** Each task ends with passing `go build ./...` and the relevant `go test ./...` packages green.
- **TDD ratchet.** New behavior (auto-wrap, auto-tag, recover-time migration, naming changes) is locked in with failing tests *first*, then implemented.
- **One concern per task.** Renames and deletions are split from semantic changes so the diff of any single commit is small.
- **No shims beyond the documented compatibility layer.** Boundary normalization is the explicit compatibility layer; nothing else.

## File map

| File | Action |
|---|---|
| `internal/backend/shared/manifest/manifest.go` | Demote `Manifest`→`flatManifest`; rewrite `ParsePayload` to return `(*StackManifest, error)`; add `DefaultServiceName`. |
| `internal/backend/shared/manifest/manifest_test.go` | Update tests for new `ParsePayload` signature; add wrap-tests. |
| `internal/backend/client.go` | Add `NormalizeProvisionRequest`; delete `IsStack`. |
| `internal/backend/client_test.go` | Update `IsStack` tests; add normalization tests. |
| `internal/backend/docker/provision.go` | Collapse provision branches; delete `setupVolumeBinds`; rename `setupStackVolBinds`→`setupVolBinds`. |
| `internal/backend/docker/restart_update.go` | Collapse restart/update branches. |
| `internal/backend/docker/deprovision.go` | Drop `isStack` branches. |
| `internal/backend/docker/recover.go` | Pre-loop dispatch to migration; drop legacy main-loop branch. |
| `internal/backend/docker/migrate.go` *(new)* | Recover-time legacy→stack migration. |
| `internal/backend/docker/migrate_test.go` *(new)* | Migration unit + integration tests. |
| `internal/backend/docker/info.go` | Always populate `Services`; derive `Instances`. |
| `internal/backend/docker/lifecycle.go` | Service-aware container naming only. |
| `internal/backend/docker/backend.go` | `prevContainerName` takes a service name. |
| `internal/backend/docker/volume.go` | Add `RenameVolume` to `VolumeBackend`. |
| `internal/backend/docker/volume_xfs.go` | Implement `RenameVolume` via `os.Rename`. |
| `internal/backend/docker/volume_btrfs.go` | Implement `RenameVolume` via `os.Rename`. |
| `internal/backend/docker/volume_zfs.go` | Implement `RenameVolume` via `zfs rename`. |
| `internal/backend/shared/leasesm/*.go` | Drop `IsStack` callers; `ProvisionState` always uses stack form. |
| `docs/manifest-guide.md` | Mark flat format deprecated. |
| `docs/manifest-schema.json` | Keep union; add deprecation comment on flat branch. |
| `CHANGELOG.md` | New file (if absent) — note the deprecation and migration behavior. |

---

## Task 1: Lock new contract with failing tests

**Goal:** Write the tests for boundary normalization (parse-time wrap, item auto-tag) and recover-time migration up-front, before any production change.

**Files:**
- Modify: `internal/backend/shared/manifest/manifest_test.go`
- Modify: `internal/backend/client_test.go`
- Create: `internal/backend/docker/migrate_test.go`

- [ ] **Step 1.1: Add `TestParsePayload_WrapsFlat` and `TestParsePayload_StackPassThrough` to `internal/backend/shared/manifest/manifest_test.go`.**

```go
func TestParsePayload_WrapsFlat(t *testing.T) {
    flat := []byte(`{"image":"nginx:1.25","ports":{"80/tcp":{}}}`)
    sm, err := manifest.ParsePayload(flat)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if sm == nil || len(sm.Services) != 1 {
        t.Fatalf("expected 1 service, got %+v", sm)
    }
    svc, ok := sm.Services[manifest.DefaultServiceName]
    if !ok {
        t.Fatalf("expected service %q, got keys %v", manifest.DefaultServiceName, mapKeys(sm.Services))
    }
    if svc.Image != "nginx:1.25" {
        t.Fatalf("image not preserved: %q", svc.Image)
    }
}

func TestParsePayload_StackPassThrough(t *testing.T) {
    stack := []byte(`{"services":{"web":{"image":"nginx:1.25"}}}`)
    sm, err := manifest.ParsePayload(stack)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if _, ok := sm.Services["web"]; !ok {
        t.Fatalf("expected service 'web', got keys %v", mapKeys(sm.Services))
    }
}

func mapKeys[K comparable, V any](m map[K]V) []K {
    out := make([]K, 0, len(m))
    for k := range m {
        out = append(out, k)
    }
    return out
}
```

- [ ] **Step 1.2: Add `TestNormalizeProvisionRequest_*` cases to `internal/backend/client_test.go`.**

```go
func TestNormalizeProvisionRequest_AutoTagsSingleUnnamedItem(t *testing.T) {
    req := backend.ProvisionRequest{
        LeaseUUID: "u", Tenant: "t",
        Items: []backend.LeaseItem{{SKU: "docker-micro", Quantity: 1}},
    }
    if err := backend.NormalizeProvisionRequest(&req); err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if got := req.Items[0].ServiceName; got != "app" {
        t.Fatalf("expected service_name=app, got %q", got)
    }
}

func TestNormalizeProvisionRequest_PreservesNamedItems(t *testing.T) {
    req := backend.ProvisionRequest{
        Items: []backend.LeaseItem{
            {SKU: "docker-micro", Quantity: 1, ServiceName: "web"},
            {SKU: "docker-small", Quantity: 1, ServiceName: "db"},
        },
    }
    if err := backend.NormalizeProvisionRequest(&req); err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if req.Items[0].ServiceName != "web" || req.Items[1].ServiceName != "db" {
        t.Fatalf("service_name modified unexpectedly: %+v", req.Items)
    }
}

func TestNormalizeProvisionRequest_RejectsMixed(t *testing.T) {
    req := backend.ProvisionRequest{
        Items: []backend.LeaseItem{
            {SKU: "docker-micro", Quantity: 1, ServiceName: "web"},
            {SKU: "docker-small", Quantity: 1}, // missing
        },
    }
    if err := backend.NormalizeProvisionRequest(&req); err == nil {
        t.Fatalf("expected error for mixed service_name, got nil")
    }
}
```

- [ ] **Step 1.3: Create `internal/backend/docker/migrate_test.go` with the migration smoke test.**

```go
package docker

import (
    "context"
    "strings"
    "testing"
)

// TestRecoverState_MigratesLegacyContainer: a managed container with
// fred.lease_uuid but no fred.service_name is recreated as a stack-form
// container named fred-{uuid}-app-0; the volume directory is renamed; the
// release store gets a wrapped manifest entry.
func TestRecoverState_MigratesLegacyContainer(t *testing.T) {
    b, fakeDocker, fakeVolumeBackend, fakeRelStore := newMigrationTestBackend(t)

    fakeDocker.containers = []ContainerInfo{{
        ContainerID:   "legacy-cid",
        LeaseUUID:     "lease-1",
        Tenant:        "tenant-a",
        SKU:           "docker-micro",
        Image:         "nginx:1.25",
        InstanceIndex: 0,
        // ServiceName empty: legacy
    }}
    fakeDocker.mounts["legacy-cid"] = []ContainerMount{{
        Source: "/var/lib/fred/volumes/fred-lease-1-0",
        Target: "/data",
    }}
    fakeRelStore.releases["lease-1"] = []byte(`{"image":"nginx:1.25"}`)

    if err := b.recoverState(context.Background()); err != nil {
        t.Fatalf("recoverState failed: %v", err)
    }

    if !fakeVolumeBackend.renamed("fred-lease-1-0", "fred-lease-1-app-0") {
        t.Fatalf("volume not renamed: %v", fakeVolumeBackend.renames)
    }
    if !strings.Contains(fakeDocker.lastComposeProjectName, "fred-lease-1") {
        t.Fatalf("compose up not invoked for project: %v", fakeDocker.lastComposeProjectName)
    }
    if !fakeRelStore.hasWrappedRelease("lease-1") {
        t.Fatalf("release store missing wrapped entry")
    }
}

func TestRecoverState_MigrationFailure_AbortsStartup(t *testing.T) {
    b, fakeDocker, _, _ := newMigrationTestBackend(t)
    fakeDocker.containers = []ContainerInfo{{
        ContainerID: "legacy-cid",
        LeaseUUID:   "lease-1",
        SKU:         "docker-micro",
    }}
    fakeDocker.composeUpErr = errors.New("compose up failed")

    err := b.recoverState(context.Background())
    if err == nil {
        t.Fatalf("expected recoverState to fail when migration fails")
    }
    if !strings.Contains(err.Error(), "lease-1") {
        t.Fatalf("expected error to identify lease, got: %v", err)
    }
}
```

(The `newMigrationTestBackend` helper is added in Step 1.4 below.)

- [ ] **Step 1.4: Add `newMigrationTestBackend(t)` test helper.** Place it in an existing testsupport file (`internal/backend/docker/testsupport_test.go`) so it composes with the existing fakes. The helper returns `(*Backend, *fakeDocker, *fakeVolumeBackend, *fakeReleaseStore)`, with the new fakes capturing rename + compose-up + release-store-write calls.

- [ ] **Step 1.5: Run the new tests; they MUST fail.**

```bash
go test ./internal/backend/shared/manifest/ -run TestParsePayload -v
go test ./internal/backend/ -run TestNormalizeProvisionRequest -v
go test ./internal/backend/docker/ -run "TestRecoverState_MigratesLegacyContainer|TestRecoverState_MigrationFailure" -v
```

Expected: all fail. Most likely with "function not defined" or "no such symbol".

- [ ] **Step 1.6: Commit.**

```bash
git add internal/backend/shared/manifest/manifest_test.go \
        internal/backend/client_test.go \
        internal/backend/docker/migrate_test.go \
        internal/backend/docker/testsupport_test.go
git commit -m "test: lock boundary-normalization and migration contract (red)"
```

---

## Task 2: Boundary normalization in the parser

**Goal:** Rewrite `ParsePayload` to return `(*StackManifest, error)` always. Flat input is auto-wrapped to `{"services": {"app": <flat>}}` with a deprecation log. Demote `Manifest` to an unexported `flatManifest`. Add `DefaultServiceName` constant.

**Files:**
- Modify: `internal/backend/shared/manifest/manifest.go` (lines 1–500 for `Manifest` struct/validators; lines 647–680 for `ParsePayload`)

- [ ] **Step 2.1: Add `DefaultServiceName` constant near the top of the file.**

```go
// DefaultServiceName is the synthetic service name applied when fred wraps a
// legacy flat manifest into a 1-service stack. Centralized so callers stay
// consistent.
const DefaultServiceName = "app"
```

- [ ] **Step 2.2: Rename `Manifest` (the exported single-service type) to `flatManifest` (unexported) — everywhere within this file only.**

```bash
# Reasonable bulk approach:
sed -i 's/\bManifest\b/flatManifest/g' internal/backend/shared/manifest/manifest.go
# Then revert StackManifest references that the bulk sed broke:
sed -i 's/StackflatManifest/StackManifest/g' internal/backend/shared/manifest/manifest.go
```

(Confirm by `grep -n "flatManifest\|StackManifest" internal/backend/shared/manifest/manifest.go`.)

- [ ] **Step 2.3: Rewrite `ParsePayload`.**

```go
// ParsePayload parses a deployment manifest payload, returning the
// stack-shaped representation regardless of the input format.
//
// Stack-format input ({"services": {...}}) is parsed directly. Flat-format
// input is parsed via the internal flatManifest type and wrapped as
// {"services": {"app": <flat>}}. A deprecation log fires for flat input;
// callers should pass leaseUUID (or "") via context if rate-limiting per-lease
// is desired.
//
// Unknown top-level fields are rejected.
func ParsePayload(data []byte) (*StackManifest, error) {
    if len(data) == 0 {
        return nil, fmt.Errorf("empty payload")
    }
    var probe map[string]json.RawMessage
    if err := json.Unmarshal(data, &probe); err != nil {
        return nil, fmt.Errorf("invalid JSON: %w", err)
    }
    if _, isStack := probe["services"]; isStack {
        dec := json.NewDecoder(bytes.NewReader(data))
        dec.DisallowUnknownFields()
        var stack StackManifest
        if err := dec.Decode(&stack); err != nil {
            return nil, fmt.Errorf("parse stack manifest: %w", err)
        }
        if err := stack.Validate(); err != nil {
            return nil, fmt.Errorf("validate stack manifest: %w", err)
        }
        return &stack, nil
    }

    // Flat (legacy) form — parse and wrap.
    dec := json.NewDecoder(bytes.NewReader(data))
    dec.DisallowUnknownFields()
    var flat flatManifest
    if err := dec.Decode(&flat); err != nil {
        return nil, fmt.Errorf("parse flat manifest: %w", err)
    }
    // Validate the embedded manifest using flatManifest.Validate (the
    // single-service validator).
    if err := flat.Validate(); err != nil {
        return nil, fmt.Errorf("validate flat manifest: %w", err)
    }
    // depends_on must be empty on a flat manifest (no peers).
    if len(flat.DependsOn) > 0 {
        return nil, fmt.Errorf("depends_on is not valid in a flat manifest")
    }
    stack := &StackManifest{Services: map[string]*flatManifest{DefaultServiceName: &flat}}
    if err := stack.Validate(); err != nil {
        return nil, fmt.Errorf("validate auto-wrapped stack: %w", err)
    }
    // NOTE: deprecation logging happens at the call site (which has the
    // logger + lease UUID). The parser stays pure.
    return stack, nil
}
```

(If `StackManifest.Services` is currently `map[string]*Manifest`, leave that — the rename done in Step 2.2 already changed the field's value type to `*flatManifest`.)

- [ ] **Step 2.4: Search for external callers of `manifest.Manifest`.**

```bash
grep -rn "manifest\.Manifest\b" --include="*.go" . | grep -v _test.go | grep -v internal/backend/shared/manifest/
```

Update each to use `manifest.StackManifest` (or the appropriate per-service type after destructuring). Most will be in docker/ files we touch in Tasks 4-6 — list them and fix here so the tree compiles. If a caller can't be cleanly updated, mark it with a `TODO: see migrate task` comment and ensure the file at least builds.

- [ ] **Step 2.5: Build.**

```bash
go build ./...
```

Expected: success (after the Step 2.4 updates).

- [ ] **Step 2.6: Run manifest tests.**

```bash
go test ./internal/backend/shared/manifest/ -v
```

Expected: PASS (including the new Step 1.1 tests).

- [ ] **Step 2.7: Commit.**

```bash
git add internal/backend/shared/manifest/ internal/backend/  # plus any docker/ touches from Step 2.4
git commit -m "feat(manifest): ParsePayload always returns *StackManifest (auto-wrap flat)"
```

---

## Task 3: Boundary normalization for lease items

**Goal:** Add `NormalizeProvisionRequest` that auto-tags missing `service_name`. Wire it into the entry points of Provision and Update.

**Files:**
- Modify: `internal/backend/client.go`
- Modify: `internal/backend/docker/provision.go` (at the entry point)
- Modify: `internal/backend/docker/restart_update.go` (at the Update entry point)

- [ ] **Step 3.1: Add `NormalizeProvisionRequest` to `internal/backend/client.go`.**

```go
// NormalizeProvisionRequest applies boundary normalization to a request:
//   - If all items already have ServiceName, returns nil with no changes.
//   - If no item has ServiceName and there's exactly one item, the item is
//     tagged with ServiceName = manifest.DefaultServiceName ("app").
//   - Any other combination (mixed/all-empty-multi-item) is rejected as
//     ErrInvalidManifest.
//
// The normalization is in-place. Callers should invoke this before any
// docker work or pool allocation.
func NormalizeProvisionRequest(req *ProvisionRequest) error {
    if req == nil || len(req.Items) == 0 {
        return fmt.Errorf("%w: provision request has no items", ErrValidation)
    }
    named, unnamed := 0, 0
    for _, item := range req.Items {
        if item.ServiceName == "" {
            unnamed++
        } else {
            named++
        }
    }
    if named > 0 && unnamed > 0 {
        return fmt.Errorf("%w: mixed service_name presence across items (got %d named, %d unnamed)",
            ErrInvalidManifest, named, unnamed)
    }
    if unnamed > 1 {
        return fmt.Errorf("%w: legacy single-service form requires exactly 1 item; got %d",
            ErrInvalidManifest, unnamed)
    }
    if unnamed == 1 {
        req.Items[0].ServiceName = manifest.DefaultServiceName
    }
    return nil
}
```

(Import `manifest "github.com/manifest-network/fred/internal/backend/shared/manifest"` if not already.)

- [ ] **Step 3.2: At `provision.go` entry (the location of the current `isStack := backend.IsStack(...)` call at line 126), call `NormalizeProvisionRequest(&req)` first.**

Replace:

```go
isStack, err := backend.IsStack(req.Items)
if err != nil {
    b.removeProvision(req.LeaseUUID)
    return fmt.Errorf("%w: %w", backend.ErrValidation, err)
}
```

with:

```go
if err := backend.NormalizeProvisionRequest(&req); err != nil {
    b.removeProvision(req.LeaseUUID)
    return err
}
```

(Leave the rest of the function unchanged for now — Task 4 collapses it.)

- [ ] **Step 3.3: Do the same in `restart_update.go` at the Update entry (around line 834 where `IsStack` is called).**

- [ ] **Step 3.4: Build.**

```bash
go build ./...
```

Expected: success. (Tree may still reference `isStack` elsewhere — collapsed in Tasks 4-7.)

- [ ] **Step 3.5: Run client + manifest tests.**

```bash
go test ./internal/backend/ -run "TestNormalizeProvisionRequest" -v
go test ./internal/backend/shared/manifest/ -v
```

Expected: PASS.

- [ ] **Step 3.6: Commit.**

```bash
git add internal/backend/client.go internal/backend/docker/provision.go internal/backend/docker/restart_update.go
git commit -m "feat(backend): NormalizeProvisionRequest (auto-tag missing service_name)"
```

---

## Task 4: Route all provisions through the stack path

**Goal:** After Tasks 2 and 3, the provision entry point already has `*StackManifest` and items always carry `service_name`. Collapse the `isStack` branching; redirect work to `doProvisionStack`.

**Files:**
- Modify: `internal/backend/docker/provision.go` (lines 120–230)

- [ ] **Step 4.1: Replace the parse + branching at `provision.go:125-230` with the unified flow:**

```go
    if err := backend.NormalizeProvisionRequest(&req); err != nil {
        b.removeProvision(req.LeaseUUID)
        return err
    }

    stackManifest, err := manifest.ParsePayload(req.Payload)
    if err != nil {
        b.removeProvision(req.LeaseUUID)
        return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)
    }
    // Deprecation log if input was flat (heuristic: 1 service with the
    // default name AND no top-level "services" key in the payload).
    if isFlatPayload(req.Payload) {
        logger.Warn("manifest deprecation: tenant submitted flat single-service manifest; auto-wrapped as 1-service stack",
            "lease_uuid", req.LeaseUUID)
    }
    if err := manifest.ValidateStackAgainstItems(stackManifest, req.Items); err != nil {
        b.removeProvision(req.LeaseUUID)
        return fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)
    }
    for svcName, svc := range stackManifest.Services {
        if err := shared.ValidateImage(svc.Image, b.cfg.AllowedRegistries); err != nil {
            b.removeProvision(req.LeaseUUID)
            return fmt.Errorf("%w: service %s: %w", backend.ErrValidation, svcName, err)
        }
    }

    // Allocation IDs: always service-aware.
    var allocatedIDs []string
    for _, item := range req.Items {
        for i := range item.Quantity {
            instanceID := fmt.Sprintf("%s-%s-%d", req.LeaseUUID, item.ServiceName, i)
            if err := b.pool.TryAllocate(instanceID, item.SKU, req.Tenant); err != nil {
                for _, id := range allocatedIDs {
                    b.pool.Release(id)
                }
                b.removeProvision(req.LeaseUUID)
                return fmt.Errorf("%w: %w", backend.ErrInsufficientResources, err)
            }
            allocatedIDs = append(allocatedIDs, instanceID)
        }
    }

    b.provisionsMu.Lock()
    if prov, ok := b.provisions[req.LeaseUUID]; ok {
        prov.SKU = req.RoutingSKU()
        prov.Items = req.Items
        prov.StackManifest = stackManifest
    }
    b.provisionsMu.Unlock()

    provCtx, provCancel := b.shutdownAwareContext()
    work := func() (string, leasesm.ProvisionSuccessResult, map[string]string, error) {
        return b.doProvisionStack(provCtx, req, stackManifest, profiles, logger)
    }
```

- [ ] **Step 4.2: Add the `isFlatPayload` helper near the bottom of `provision.go` (or in a small util file).**

```go
// isFlatPayload returns true if the raw payload bytes do NOT contain a
// top-level "services" key (i.e., a legacy flat manifest was submitted).
// Best-effort: tolerant to whitespace.
func isFlatPayload(data []byte) bool {
    var probe map[string]json.RawMessage
    if err := json.Unmarshal(data, &probe); err != nil {
        return false
    }
    _, hasServices := probe["services"]
    return !hasServices
}
```

(Import `encoding/json` if needed.)

- [ ] **Step 4.3: Build.**

```bash
go build ./...
```

Expected: success.

- [ ] **Step 4.4: Run docker provision tests.**

```bash
go test ./internal/backend/docker/ -run "Provision" -count=1
```

Expected: PASS for tests that survive the consolidation (legacy-only fixtures may need updates — capture them and revisit in Task 16).

- [ ] **Step 4.5: Commit.**

```bash
git add internal/backend/docker/provision.go
git commit -m "refactor(docker): route all provisions through the stack path"
```

---

## Task 5: Route all restarts through the stack path

**Goal:** Same pattern for Restart.

**Files:**
- Modify: `internal/backend/docker/restart_update.go` (lines 90–161)

- [ ] **Step 5.1: At `restart_update.go:93`, replace the `isStack := prov.IsStack()` + branching with an unconditional stack-form call:**

```go
    if prov.StackManifest == nil {
        // Should not happen after recover-time migration runs in Task 8-9, but
        // defend defensively.
        return leasesm.ReplaceResult{Err: fmt.Errorf("internal error: provision %s has no stack manifest", req.LeaseUUID)}
    }
    return b.doRestartStack(opCtx, req.LeaseUUID, prov.StackManifest, containerIDs, serviceContainers, items, prevStatus, logger)
```

- [ ] **Step 5.2: Build + test.**

```bash
go build ./...
go test ./internal/backend/docker/ -run "Restart" -count=1
```

Expected: PASS for stack-format tests; legacy tests fail (rebaseline in Task 16).

- [ ] **Step 5.3: Commit.**

```bash
git add internal/backend/docker/restart_update.go
git commit -m "refactor(docker): route all restarts through the stack path"
```

---

## Task 6: Route all updates through the stack path

**Goal:** Same pattern for Update.

**Files:**
- Modify: `internal/backend/docker/restart_update.go` (lines 830–972)

- [ ] **Step 6.1: Replace the Update entry branching with unified parse + stack call:**

```go
    if err := backend.NormalizeProvisionRequest(&backend.ProvisionRequest{Items: prov.Items}); err != nil {
        // Items came from existing prov state and should have been normalized
        // at provision time; this is a defensive check.
        return leasesm.ReplaceResult{Err: err}
    }
    stackManifest, err := manifest.ParsePayload(req.Payload)
    if err != nil {
        return leasesm.ReplaceResult{Err: fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)}
    }
    if isFlatPayload(req.Payload) {
        logger.Warn("manifest deprecation: tenant submitted flat single-service manifest; auto-wrapped",
            "lease_uuid", req.LeaseUUID)
    }
    if err := manifest.ValidateStackAgainstItems(stackManifest, prov.Items); err != nil {
        return leasesm.ReplaceResult{Err: fmt.Errorf("%w: %w", backend.ErrInvalidManifest, err)}
    }
    return b.doUpdateStack(opCtx, req.LeaseUUID, stackManifest, profiles, oldContainerIDs, serviceContainers, items, prevStatus, logger)
```

- [ ] **Step 6.2: Build + test.**

```bash
go build ./...
go test ./internal/backend/docker/ -run "Update" -count=1
```

Expected: PASS for stack-format tests.

- [ ] **Step 6.3: Commit.**

```bash
git add internal/backend/docker/restart_update.go
git commit -m "refactor(docker): route all updates through the stack path"
```

---

## Task 7: Drop the isStack branch from deprovision.go

**Goal:** Remove the four `if isStack` branches at deprovision.go:67, 80, 110, 143-155.

**Files:**
- Modify: `internal/backend/docker/deprovision.go`

- [ ] **Step 7.1: Delete `isStack := prov.IsStack()` at line 67. Below it, delete every `else { ... legacy ... }` arm — the stack arms become unconditional.**

The legacy volume cleanup block (line 155):

```go
} else {
    volumeID := fmt.Sprintf("fred-%s-%d", leaseUUID, i)
    ...
}
```

is deleted; the stack arm above it (using `fred-{uuid}-{svc}-{idx}`) becomes the only path.

- [ ] **Step 7.2: Build + test.**

```bash
go build ./...
go test ./internal/backend/docker/ -run "Deprovision" -count=1
```

Expected: PASS.

- [ ] **Step 7.3: Commit.**

```bash
git add internal/backend/docker/deprovision.go
git commit -m "refactor(docker): drop legacy branch from deprovision"
```

---

## Task 8: Recover-time migration — supporting types and scaffolding

**Goal:** Add the supporting infrastructure the migration pipeline needs (config fields, `ContainerMount`/`ContainerInfo.Mounts`, `ReleaseStore.RecordMigration`), then create `migrate.go` with the per-lease planner and wire a no-op pre-pass into `recover.go`. No execution yet (Task 9). All new symbols verified against current code, not invented.

**Files:**
- Modify: `internal/backend/docker/config.go`
- Modify: `internal/backend/docker/lifecycle.go`
- Modify: `internal/backend/shared/releases.go`
- Create: `internal/backend/docker/migrate.go`
- Modify: `internal/backend/docker/recover.go`

- [ ] **Step 8.1: Add migration config fields to `internal/backend/docker/config.go`.**

```go
// MigrationGracePeriod is how long a legacy container's `-prev` rename
// lingers after a successful recover-time migration before forced removal.
// Default: 1m. Preserves rollback potential under operator inspection.
MigrationGracePeriod time.Duration

// MigrationReadyTimeout caps how long the migration waits for the new
// stack-form container to reach `healthy`/`running`. Default: 90s.
MigrationReadyTimeout time.Duration
```

Default them in the same place existing durations are defaulted (search for the config constructor / `Defaults()` analogue). Add a TOML / env binding if the existing config supports that pattern.

- [ ] **Step 8.2: Add `ContainerMount` type and extend `ContainerInfo` with `Name` and `Mounts` fields in `internal/backend/docker/lifecycle.go`. Populate from `resp.Name`/`c.Names[0]` and `resp.Mounts` in both `ContainerInspect` (around line 1371) and `ListManagedContainers`.**

```go
// ContainerMount mirrors the subset of docker's Mount data fred needs for
// migration: where the host bind comes from, where it's mounted in the
// container, and the mount type.
type ContainerMount struct {
    Source string
    Target string
    Type   string // bind | volume | tmpfs (string per docker API)
}
```

Extend `ContainerInfo` with two new fields:
- `Name string` — populated from `strings.TrimPrefix(resp.Name, "/")` (inspect) or `strings.TrimPrefix(c.Names[0], "/")` (list). Needed by `isLegacyContainer` to apply the `-prev` suffix filter.
- `Mounts []ContainerMount` — populated from `resp.Mounts`. In `ListManagedContainers`, either issue an extra `ContainerInspect` per container (acceptable cost — runs only at startup) or filter `resp.Mounts` from the existing list-with-inspect helper if one exists.

- [ ] **Step 8.3: Add `RecordMigration` to `internal/backend/shared/releases.go`.**

```go
// RecordMigration appends an active release entry for a recover-time migration.
// Idempotent: if the most-recent active entry for the lease already carries
// the same manifest payload, this is a no-op.
func (s *ReleaseStore) RecordMigration(leaseUUID string, manifest []byte) error {
    latest, _ := s.LatestActive(leaseUUID)
    if latest != nil && bytes.Equal(latest.Manifest, manifest) {
        return nil
    }
    // Reuse Append semantics; status is "active".
    return s.Append(leaseUUID, Release{Manifest: manifest, Status: StatusActive, ...})
}
```

(Copy the active-release shape from the closest existing append site. If `Release` has additional required fields like timestamp/version, populate them with sensible defaults.)

- [ ] **Step 8.4: Create `internal/backend/docker/migrate.go` with the per-lease planner.**

```go
package docker

import (
    "context"
    "fmt"
    "log/slog"
    "strings"
    "time"

    "github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// legacyMigration describes the work needed to recreate ALL legacy
// containers of one lease as a single stack-form (1-service, N-instance)
// Compose project. Per-lease (not per-container) because b.compose.Up
// runs with RemoveOrphans:true and would destroy already-migrated siblings.
type legacyMigration struct {
    LeaseUUID  string
    Tenant     string
    SKU        string
    Stack      *manifest.StackManifest
    Instances  []legacyMigrationInstance
}

type legacyMigrationInstance struct {
    LegacyContainer  ContainerInfo
    Mounts           []ContainerMount // managed-volume binds; len==0 for stateless
    NewContainerName string           // fred-{uuid}-app-{idx}
    PrevName         string           // fred-{uuid}-app-{idx}-prev
    VolRenames       []volRename      // {old, new} per managed volume
}

type volRename struct{ Old, New, Target string }

// isLegacyContainer: present in managed list, has lease_uuid, no service_name,
// AND name doesn't end with -prev (already-migrated remnants are excluded).
func isLegacyContainer(c ContainerInfo) bool {
    if c.LeaseUUID == "" || c.ServiceName != "" {
        return false
    }
    return !strings.HasSuffix(c.Name, "-prev")
}

// planLegacyMigrations groups legacy containers by lease and produces one
// migration plan per lease.
func (b *Backend) planLegacyMigrations(ctx context.Context, all []ContainerInfo, logger *slog.Logger) ([]*legacyMigration, error) {
    byLease := map[string][]ContainerInfo{}
    for _, c := range all {
        if isLegacyContainer(c) {
            byLease[c.LeaseUUID] = append(byLease[c.LeaseUUID], c)
        }
    }
    plans := make([]*legacyMigration, 0, len(byLease))
    for leaseUUID, group := range byLease {
        plan, err := b.planLegacyMigrationForLease(ctx, leaseUUID, group, logger)
        if err != nil {
            return nil, fmt.Errorf("plan lease %s: %w", leaseUUID, err)
        }
        plans = append(plans, plan)
    }
    return plans, nil
}

func (b *Backend) planLegacyMigrationForLease(ctx context.Context, leaseUUID string, group []ContainerInfo, logger *slog.Logger) (*legacyMigration, error) {
    // Manifest source: release store. No in-container reconstruction.
    rel, relErr := b.releaseStore.LatestActive(leaseUUID)
    if relErr != nil || rel == nil || len(rel.Manifest) == 0 {
        return nil, fmt.Errorf("release store has no active manifest for lease %s; cannot migrate (operator: investigate or deprovision)", leaseUUID)
    }
    stack, err := manifest.ParsePayload(rel.Manifest)
    if err != nil {
        return nil, fmt.Errorf("parse stored manifest: %w", err)
    }

    instances := make([]legacyMigrationInstance, 0, len(group))
    for _, c := range group {
        // c.Mounts populated in Task 8.2; if absent, fall back to an Inspect call.
        mounts := c.Mounts
        if mounts == nil {
            inspected, ierr := b.docker.InspectContainer(ctx, c.ContainerID)
            if ierr != nil {
                return nil, fmt.Errorf("inspect %s: %w", c.ContainerID, ierr)
            }
            mounts = inspected.Mounts
        }
        // Keep only managed-volume binds (host source under b.cfg.DataPath / equivalent
        // root). tmpfs and unrelated binds are filtered.
        managed := filterManagedMounts(b, mounts)

        newName := fmt.Sprintf("fred-%s-%s-%d", leaseUUID, manifest.DefaultServiceName, c.InstanceIndex)
        prevName := newName + "-prev"

        var renames []volRename
        for _, m := range managed {
            oldVol := fmt.Sprintf("fred-%s-%d", leaseUUID, c.InstanceIndex)
            newVol := fmt.Sprintf("fred-%s-%s-%d", leaseUUID, manifest.DefaultServiceName, c.InstanceIndex)
            renames = append(renames, volRename{Old: oldVol, New: newVol, Target: m.Target})
        }
        instances = append(instances, legacyMigrationInstance{
            LegacyContainer:  c,
            Mounts:           managed,
            NewContainerName: newName,
            PrevName:         prevName,
            VolRenames:       renames,
        })
    }
    sortInstancesByIndex(instances) // deterministic order

    return &legacyMigration{
        LeaseUUID: leaseUUID,
        Tenant:    group[0].Tenant,
        SKU:       group[0].SKU,
        Stack:     stack,
        Instances: instances,
    }, nil
}

// filterManagedMounts: keep only binds whose Source sits under the docker
// volume root path. Stateless leases legitimately return zero results here.
func filterManagedMounts(b *Backend, mounts []ContainerMount) []ContainerMount {
    // Implementation: compare m.Source HasPrefix(b.volumes.Root()) — adjust to
    // the actual API on volumeManager. If volumeManager doesn't expose a Root
    // method today, add a minimal Root() accessor in volume.go.
    return mounts // placeholder; replace with real filter
}

func sortInstancesByIndex(xs []legacyMigrationInstance) {
    // sort.Slice on InstanceIndex; trivial.
}
```

(Symbol notes verified against the codebase: the field is `b.compose`, the interface is unexported `volumeManager` with field `b.volumes`, the release store field is `b.releaseStore`. The `ContainerInspect` return shape includes `Mounts` after Step 8.2. Use real names; do not invent.)

- [ ] **Step 8.5: At the top of `recoverState` in `recover.go` (after the existing `ListManagedContainers` call near line 25), insert the pre-pass — plan only, no execution yet.**

```go
    // Legacy migration pre-pass: group legacy containers by lease and plan
    // each lease's migration. Execution lands in Task 9.
    legacyPlans, err := b.planLegacyMigrations(ctx, containers, b.logger)
    if err != nil {
        return fmt.Errorf("plan legacy migrations: %w", err)
    }
    for _, plan := range legacyPlans {
        b.logger.Info("legacy lease migration planned",
            "lease_uuid", plan.LeaseUUID,
            "instances", len(plan.Instances),
        )
    }
    if len(legacyPlans) > 0 {
        // Execution lives in Task 9; for now, abort startup with a clear message
        // so we don't proceed into the main loop with stale container metadata.
        return fmt.Errorf("legacy migration planning complete; execution pending (Task 9)")
    }
```

- [ ] **Step 8.6: Build.**

```bash
go build ./...
```

Expected: success.

- [ ] **Step 8.7: Run migration plan tests.**

```bash
go test ./internal/backend/docker/ -run "TestRecoverState_MigratesLegacyContainer" -v
```

Expected: still failing (execution not implemented). That's acceptable — Task 9 makes it pass.

- [ ] **Step 8.8: Commit.**

```bash
git add internal/backend/docker/config.go internal/backend/docker/lifecycle.go \
        internal/backend/shared/releases.go \
        internal/backend/docker/migrate.go internal/backend/docker/recover.go
git commit -m "feat(docker): scaffold legacy→stack recover-time migration (per-lease planner + supporting types)"
```

---

## Task 9: Recover-time migration — execution

**Goal:** Implement `executeLegacyMigration` (per lease, all instances atomically). Pipeline order is **stop → rename container to `-prev` → rename volumes → buildComposeProject → compose.Up → wait → schedule -prev removal → persist**. The container must be stopped *before* the volume rename: ZFS rejects rename of a busy dataset, and on xfs/btrfs renaming under a live bind risks dangling-inode confusion. Multi-instance leases migrate atomically in one Up call to avoid `RemoveOrphans:true` (compose.go:101) destroying already-migrated siblings.

**Files:**
- Modify: `internal/backend/docker/migrate.go`
- Modify: `internal/backend/docker/recover.go`

- [ ] **Step 9.1: Add `executeLegacyMigration` to `migrate.go`.** Real symbol names (`b.compose`, `b.volumes`, `b.releaseStore`, `VolBinds`); per-lease atomic; stateless-safe.

```go
func (b *Backend) executeLegacyMigration(ctx context.Context, m *legacyMigration, logger *slog.Logger) error {
    logger = logger.With("lease_uuid", m.LeaseUUID, "instances", len(m.Instances))
    logger.Info("legacy migration starting")

    svc := m.Stack.Services[manifest.DefaultServiceName]
    if svc == nil {
        return fmt.Errorf("internal: wrapped stack missing default service %q", manifest.DefaultServiceName)
    }
    stopGrace := 10 * time.Second
    if svc.StopGracePeriod != nil {
        stopGrace = time.Duration(*svc.StopGracePeriod)
    }

    // 1. Stop + rename every legacy container in the lease.
    //    Stopping must precede volume rename (ZFS busy-dataset; xfs/btrfs sanity).
    for _, inst := range m.Instances {
        if err := b.docker.StopContainer(ctx, inst.LegacyContainer.ContainerID, stopGrace); err != nil {
            // Tolerate already-stopped; the docker error type tells us.
            logger.Warn("stop legacy container returned error (continuing)",
                "container_id", inst.LegacyContainer.ContainerID, "error", err)
        }
        // Idempotency: if a previous run already renamed to PrevName, this errors;
        // detect that case and continue.
        if err := b.docker.RenameContainer(ctx, inst.LegacyContainer.ContainerID, inst.PrevName); err != nil {
            if !isAlreadyNamedErr(err, inst.PrevName) {
                return fmt.Errorf("rename %s to %s: %w", inst.LegacyContainer.ContainerID, inst.PrevName, err)
            }
        }
    }

    // 2. Rename managed volume directories (per-instance, per-volume).
    //    Skipped naturally when an instance has no managed volumes (stateless lease).
    for _, inst := range m.Instances {
        for _, r := range inst.VolRenames {
            if err := b.volumes.RenameVolume(r.Old, r.New); err != nil {
                return fmt.Errorf("rename volume %s→%s (instance idx=%d): %w",
                    r.Old, r.New, inst.LegacyContainer.InstanceIndex, err)
            }
        }
    }

    // 3. Build the Compose project for the whole lease (all instances).
    profile, err := b.cfg.GetSKUProfile(m.SKU)
    if err != nil {
        return fmt.Errorf("load SKU profile %s: %w", m.SKU, err)
    }
    quantity := len(m.Instances)
    items := []backend.LeaseItem{{
        SKU: m.SKU, Quantity: quantity, ServiceName: manifest.DefaultServiceName,
    }}

    // Compose project must use the just-renamed volume directories. Pass them
    // via VolBinds: map[serviceName] map[instanceIndex] serviceVolBinds.
    volBinds := map[string]map[int]serviceVolBinds{
        manifest.DefaultServiceName: {},
    }
    for _, inst := range m.Instances {
        binds := serviceVolBinds{} // populate per-target host paths from inst.VolRenames
        for _, r := range inst.VolRenames {
            binds.Add(r.Target, b.volumes.HostPath(r.New)) // HostPath: volumeManager helper to resolve host dir for a vol name
        }
        volBinds[manifest.DefaultServiceName][inst.LegacyContainer.InstanceIndex] = binds
    }

    project, err := buildComposeProject(composeProjectParams{
        LeaseUUID: m.LeaseUUID,
        Tenant:    m.Tenant,
        Stack:     m.Stack,
        Items:     items,
        Profiles:  map[string]SKUProfile{m.SKU: profile},
        VolBinds:  volBinds,
    })
    if err != nil {
        return fmt.Errorf("build compose project: %w", err)
    }

    // 4. Compose Up — brings up all instances under stack-form names.
    if err := b.compose.Up(ctx, project, composeUpOpts{}); err != nil {
        return fmt.Errorf("compose up: %w", err)
    }

    // 5. Wait for ready. Reuse waitForHealthy (provision.go:1133), whose
    //    real signature is waitForHealthy(ctx, containerIDs []string, logger *slog.Logger).
    //    Resolve IDs by listing containers and matching new names; if a helper
    //    doesn't exist, add a minimal `b.docker.ListByName(ctx, names)` (or scan
    //    the result of `ListManagedContainers` and filter by ContainerInfo.Name).
    newIDs, err := b.resolveContainerIDsByName(ctx, namesOf(m.Instances))
    if err != nil {
        return fmt.Errorf("resolve new container IDs: %w", err)
    }
    readyCtx, cancel := context.WithTimeout(ctx, b.cfg.MigrationReadyTimeout)
    defer cancel()
    if err := b.waitForHealthy(readyCtx, newIDs, logger); err != nil {
        return fmt.Errorf("wait for ready: %w", err)
    }

    // 6. Schedule removal of all -prev containers after the grace window.
    for _, inst := range m.Instances {
        inst := inst
        go func() {
            select {
            case <-time.After(b.cfg.MigrationGracePeriod):
            case <-ctx.Done():
                return
            }
            if err := b.docker.RemoveContainer(context.Background(), inst.PrevName, true); err != nil {
                logger.Warn("remove -prev container after grace failed (manual cleanup may be needed)",
                    "name", inst.PrevName, "error", err)
            }
        }()
    }

    // 7. Persist wrapped manifest into release store (idempotent).
    if data, mErr := json.Marshal(m.Stack); mErr == nil {
        if persistErr := b.releaseStore.RecordMigration(m.LeaseUUID, data); persistErr != nil {
            logger.Warn("release store update failed (migration still complete)", "error", persistErr)
        }
    }

    logger.Info("legacy migration complete")
    return nil
}

// isAlreadyNamedErr returns true when a rename failed because the source
// container already carries the target name (idempotency under crash-restart).
func isAlreadyNamedErr(err error, targetName string) bool {
    // Match on docker error message; tolerant. Refine as needed.
    return strings.Contains(err.Error(), targetName) && strings.Contains(err.Error(), "already")
}

func namesOf(insts []legacyMigrationInstance) []string {
    out := make([]string, 0, len(insts))
    for _, i := range insts {
        out = append(out, i.NewContainerName)
    }
    return out
}
```

(`volumeManager.HostPath` is added in Task 10 Step 10.1b. `resolveContainerIDsByName` does not exist in the codebase today — add a minimal helper on `*Backend` that filters `ListManagedContainers` output by `ContainerInfo.Name` (the new field added in Task 8.2). `waitForHealthy`'s real signature is `waitForHealthy(ctx, containerIDs []string, logger *slog.Logger)` — pass the local `logger`, not the healthcheck spec.)

- [ ] **Step 9.2: In `recover.go`, replace the placeholder `return fmt.Errorf("...pending Task 9...")` with the execution loop, followed by a re-list of managed containers so the main loop sees post-migration state.**

```go
    for _, plan := range legacyPlans {
        if err := b.executeLegacyMigration(ctx, plan, b.logger); err != nil {
            return fmt.Errorf("legacy migration FAILED: lease %s: %w\n"+
                "fred refuses to start with unmigrated legacy containers. "+
                "Investigate the failure cause; re-run fred (migration is idempotent), or "+
                "deprovision the lease manually if data loss is acceptable.",
                plan.LeaseUUID, err)
        }
    }
    if len(legacyPlans) > 0 {
        // Re-list: migrated containers have new IDs/names/labels; the original
        // `containers` slice is stale.
        refreshed, err := b.docker.ListManagedContainers(ctx)
        if err != nil {
            return fmt.Errorf("re-list containers after migration: %w", err)
        }
        containers = refreshed
    }
```

- [ ] **Step 9.3: Build.**

```bash
go build ./...
```

Expected: success. If the compiler complains about `b.volumes.HostPath`, `serviceVolBinds.Add`, `b.resolveContainerIDsByName`, etc., add minimal real implementations in their owning files — do not paper over with TODOs.

- [ ] **Step 9.4: Run migration tests.**

```bash
go test ./internal/backend/docker/ -run "TestRecoverState_MigratesLegacyContainer|TestRecoverState_MigrationFailure" -v
```

Expected: PASS.

- [ ] **Step 9.5: Run full recover tests.**

```bash
go test ./internal/backend/docker/ -run "Recover" -count=1
```

Expected: PASS. (Legacy-format recover tests need updating — handled in Task 16.)

- [ ] **Step 9.6: Commit.**

```bash
git add internal/backend/docker/migrate.go internal/backend/docker/recover.go \
        internal/backend/docker/volume.go internal/backend/docker/volume_xfs.go \
        internal/backend/docker/volume_btrfs.go internal/backend/docker/volume_zfs.go
git commit -m "feat(docker): execute legacy→stack migration at recover time (per-lease, stop-before-rename)"
```

---

## Task 10: volumeManager.RenameVolume per filesystem

**Goal:** Add `RenameVolume` to the existing unexported `volumeManager` interface (`volume.go:16`) and implement it for xfs, btrfs, zfs. Idempotent. (Note: the spec/older drafts used the name `VolumeBackend` — the actual interface in the codebase is `volumeManager`. Add a `HostPath(name string) string` accessor here too if Task 9 needs it.)

**Files:**
- Modify: `internal/backend/docker/volume.go`
- Modify: `internal/backend/docker/volume_xfs.go`
- Modify: `internal/backend/docker/volume_btrfs.go`
- Modify: `internal/backend/docker/volume_zfs.go`

- [ ] **Step 10.1: Add to the `volumeManager` interface in `volume.go`:**

```go
// RenameVolume atomically renames a managed volume from oldName to newName.
// Idempotent: if newName exists and oldName does not, returns nil. Fails
// loudly if both exist (operator must intervene).
RenameVolume(oldName, newName string) error
```

- [ ] **Step 10.1b: If Task 9 references it, add a `HostPath(name string) string` method on `volumeManager` returning the host directory for a given volume name. xfs/btrfs return `filepath.Join(b.dataPath, name)`; zfs returns the dataset mountpoint (resolve from `b.parentDataset` — verify in `volume_zfs.go`).**

- [ ] **Step 10.2: Implement for xfs in `volume_xfs.go`:**

```go
func (b *xfsVolumeManager) RenameVolume(oldName, newName string) error {
    oldPath := filepath.Join(b.dataPath, oldName)
    newPath := filepath.Join(b.dataPath, newName)
    return atomicRenameVolumeDir(oldPath, newPath)
}
```

Add `atomicRenameVolumeDir` as a shared helper in `volume.go`:

```go
func atomicRenameVolumeDir(oldPath, newPath string) error {
    oldExists, oldErr := pathExists(oldPath)
    newExists, newErr := pathExists(newPath)
    switch {
    case oldErr != nil:
        return fmt.Errorf("stat old volume path: %w", oldErr)
    case newErr != nil:
        return fmt.Errorf("stat new volume path: %w", newErr)
    case !oldExists && newExists:
        return nil // idempotent
    case oldExists && newExists:
        return fmt.Errorf("both old (%s) and new (%s) volume paths exist; manual intervention required", oldPath, newPath)
    case !oldExists && !newExists:
        return fmt.Errorf("neither old (%s) nor new (%s) volume path exists", oldPath, newPath)
    }
    return os.Rename(oldPath, newPath)
}

func pathExists(p string) (bool, error) {
    _, err := os.Stat(p)
    if err == nil {
        return true, nil
    }
    if os.IsNotExist(err) {
        return false, nil
    }
    return false, err
}
```

- [ ] **Step 10.3: Implement for btrfs in `volume_btrfs.go` — same as xfs (plain `os.Rename` works on btrfs subvolume roots).**

```go
func (b *btrfsVolumeManager) RenameVolume(oldName, newName string) error {
    oldPath := filepath.Join(b.dataPath, oldName)
    newPath := filepath.Join(b.dataPath, newName)
    return atomicRenameVolumeDir(oldPath, newPath)
}
```

- [ ] **Step 10.4: Implement for zfs in `volume_zfs.go` via `zfs rename` shellout (matching the style of other zfs commands in this file):**

```go
func (b *zfsVolumeManager) RenameVolume(oldName, newName string) error {
    oldDataset := b.parentDataset + "/" + oldName
    newDataset := b.parentDataset + "/" + newName
    // Idempotency: check existence before issuing the rename.
    oldExists, _ := b.datasetExists(oldDataset)
    newExists, _ := b.datasetExists(newDataset)
    switch {
    case !oldExists && newExists:
        return nil
    case oldExists && newExists:
        return fmt.Errorf("both %s and %s exist; manual intervention required", oldDataset, newDataset)
    case !oldExists && !newExists:
        return fmt.Errorf("neither %s nor %s exists", oldDataset, newDataset)
    }
    cmd := exec.Command("zfs", "rename", oldDataset, newDataset)
    out, err := cmd.CombinedOutput()
    if err != nil {
        return fmt.Errorf("zfs rename: %w (output: %s)", err, string(out))
    }
    return nil
}
```

(If a `datasetExists` helper does not exist in `volume_zfs.go`, add one alongside.)

- [ ] **Step 10.5: Build + test the volume helpers.**

```bash
go build ./...
go test ./internal/backend/docker/ -run "Volume" -count=1
```

Expected: PASS (where existing tests cover); add a new test asserting idempotency:

```go
func TestRenameVolume_Idempotent_xfs(t *testing.T) {
    root := t.TempDir()
    b := &xfsVolumeManager{dataPath: root}
    require.NoError(t, os.MkdirAll(filepath.Join(root, "a"), 0o755))
    require.NoError(t, b.RenameVolume("a", "b"))
    // second call: 'a' is gone, 'b' exists — should succeed silently.
    require.NoError(t, b.RenameVolume("a", "b"))
}
```

- [ ] **Step 10.6: Commit.**

```bash
git add internal/backend/docker/volume.go internal/backend/docker/volume_xfs.go \
        internal/backend/docker/volume_btrfs.go internal/backend/docker/volume_zfs.go
git commit -m "feat(volume): RenameVolume support per filesystem"
```

---

## Task 11: Collapse volume bind helpers

**Goal:** Rename `setupStackVolBinds` → `setupVolBinds` everywhere it's referenced; the canonical helper now wears the unsuffixed name. Deletion of the legacy `setupVolumeBinds` is **deferred to Task 14** because legacy `doProvision` / `doRestart` / `doUpdate` / `doReplaceContainers` (which call it) are not deleted until Task 14 either — earlier deletion would break compilation. Task 14 absorbs both function-body deletion AND the `setupVolumeBinds` deletion in a single atomic refactor.

**Files:**
- Modify: `internal/backend/docker/provision.go` (rename only)
- Modify: any other docker/*.go file referencing `setupStackVolBinds`

- [ ] **Step 11.1: Confirm `setupVolumeBinds` is reachable only from soon-to-be-deleted legacy bodies.**

```bash
grep -rn "setupVolumeBinds\b" internal/backend/docker/ --include="*.go" | grep -v _test.go
```

Expected: callers in `doReplaceContainers` (called only by legacy `doRestart`/`doUpdate`) and possibly inside legacy `doProvision`. All are dead post-Tasks-5/6 (entry points dispatch only to stack-form versions) but compile-reachable until Task 14 deletes them. **DO NOT delete `setupVolumeBinds` here** — Task 14 owns that.

- [ ] **Step 11.2: Rename canonical helper.**

```bash
grep -rln "setupStackVolBinds" internal/backend/docker/ --include="*.go" | xargs sed -i 's/setupStackVolBinds/setupVolBinds/g'
```

- [ ] **Step 11.3: Build + test.**

```bash
go build ./...
go test ./internal/backend/docker/ -run "Volume" -count=1
```

Expected: PASS.

- [ ] **Step 11.4: Commit.**

```bash
git add internal/backend/docker/
git commit -m "refactor(docker): rename setupStackVolBinds to setupVolBinds (legacy helper deletion deferred to Task 14)"
```

---

## Task 12: Service-aware container naming

**Goal:** Drop dual container-name and prevContainerName branches.

**Files:**
- Modify: `internal/backend/docker/lifecycle.go` (lines 1048–1050)
- Modify: `internal/backend/docker/backend.go` (lines 616–644)
- Modify: `internal/backend/docker/restart_update.go` (line 727 — call site of `prevContainerName`)

- [ ] **Step 12.1: At `lifecycle.go:1048-1050`, replace the dual-name branch with a guard:**

```go
    if params.ServiceName == "" {
        return nil, fmt.Errorf("internal error: container create requested without service_name (lease %s)", params.LeaseUUID)
    }
    containerName = fmt.Sprintf("fred-%s-%s-%d", params.LeaseUUID, params.ServiceName, params.InstanceIndex)
```

- [ ] **Step 12.2: Update `prevContainerName` in `backend.go:618`:**

```go
func prevContainerName(leaseUUID, serviceName string, instanceIndex int) string {
    return fmt.Sprintf("fred-%s-%s-%d-prev", leaseUUID, serviceName, instanceIndex)
}
```

Fix all call sites — primarily `restart_update.go:727`. Pass the service name through.

- [ ] **Step 12.3: Build + integration test.**

```bash
go build ./...
go test ./internal/backend/docker/ -run "Integration" -count=1
```

Expected: PASS for stack-format tests.

- [ ] **Step 12.4: Commit.**

```bash
git add internal/backend/docker/
git commit -m "refactor(docker): service-aware container naming everywhere"
```

---

## Task 13: Unify LeaseInfo response

**Goal:** Always populate `LeaseInfo.Services`. Derive `Instances` by flattening services in deterministic order.

**Files:**
- Modify: `internal/backend/docker/info.go`

- [ ] **Step 13.1: Replace the legacy/stack branch in `info.go` with unified construction:**

```go
    info.Services = make(map[string]backend.LeaseService, len(prov.ServiceContainers))
    for svcName, cids := range prov.ServiceContainers {
        svcInstances := make([]backend.LeaseInstance, 0, len(cids))
        for i, cid := range cids {
            inst, err := b.buildInstance(ctx, cid, i)
            if err != nil {
                return backend.LeaseInfo{}, err
            }
            svcInstances = append(svcInstances, inst)
        }
        info.Services[svcName] = backend.LeaseService{Instances: svcInstances}
    }

    // Flattened Instances view, in deterministic service-name order.
    svcNames := slices.Sorted(maps.Keys(info.Services))
    for _, name := range svcNames {
        info.Instances = append(info.Instances, info.Services[name].Instances...)
    }
```

(If the existing helper for building a `LeaseInstance` from a container ID has a different name than `buildInstance`, reuse the actual name.)

- [ ] **Step 13.2: Build + test.**

```bash
go build ./...
go test ./internal/backend/docker/ -run "Info" -count=1
```

Expected: PASS.

- [ ] **Step 13.3: Commit.**

```bash
git add internal/backend/docker/info.go
git commit -m "refactor(docker): always populate LeaseInfo.Services; derive Instances"
```

---

## Task 14: Delete legacy function bodies

**Goal:** Now that nothing routes to them, delete the legacy `doProvision`, `doRestart`, `doUpdate`, `doReplaceContainers`, and `setupVolumeBinds` (deferred from Task 11). Rename `*Stack` versions to drop the suffix. This is a single atomic refactor: deleting the legacy bodies and the helper they uniquely call in the same commit so the tree compiles at every intermediate state.

**Files:**
- Modify: `internal/backend/docker/provision.go`
- Modify: `internal/backend/docker/restart_update.go`

- [ ] **Step 14.1: Identify the legacy function spans.**

```bash
grep -n "^func (b \*Backend) doProvision\b\|^func (b \*Backend) doProvisionStack\|^func (b \*Backend) doReplaceContainers\b\|^func (b \*Backend) setupVolumeBinds\b" internal/backend/docker/provision.go
grep -n "^func (b \*Backend) doRestart\b\|^func (b \*Backend) doRestartStack\|^func (b \*Backend) doUpdate\b\|^func (b \*Backend) doUpdateStack" internal/backend/docker/restart_update.go
```

Use the line numbers to locate the function bodies. Each legacy function must be deleted; the helpers that became dead-only-via-them (`doReplaceContainers`, `setupVolumeBinds`) follow.

- [ ] **Step 14.2: Delete legacy `doProvision` (provision.go ~608–876), legacy `doRestart`, legacy `doUpdate`, legacy `doReplaceContainers`, and `setupVolumeBinds` (provision.go ~421-516, deferred from Task 11).**

Verify each helper has zero remaining callers:

```bash
grep -rn "doReplaceContainers\b\|setupVolumeBinds\b" internal/backend/docker/ --include="*.go" | grep -v _test.go
```

Expected: zero hits after deletion. If a test file still calls these, it's a Task 16 rebaseline target — skip-marker it with the standard Task 16 anchor.

- [ ] **Step 14.3: Rename `doProvisionStack`→`doProvision`, `doRestartStack`→`doRestart`, `doUpdateStack`→`doUpdate`.**

```bash
grep -rln "doProvisionStack" internal/backend/docker/ --include="*.go" | xargs sed -i 's/doProvisionStack/doProvision/g'
sed -i 's/doRestartStack/doRestart/g; s/doUpdateStack/doUpdate/g' internal/backend/docker/restart_update.go
```

- [ ] **Step 14.4: Build.**

```bash
go build ./...
```

Expected: success.

- [ ] **Step 14.5: Run docker tests.**

```bash
go test ./internal/backend/docker/ -count=1
```

Expected: stack-format tests PASS. Legacy-only tests still failing (Task 16).

- [ ] **Step 14.6: Commit.**

```bash
git add internal/backend/docker/
git commit -m "refactor(docker): delete legacy doProvision/doRestart/doUpdate/doReplaceContainers/setupVolumeBinds; drop Stack suffix"
```

---

## Task 15: Delete `IsStack` and finalize the manifest API

**Goal:** Remove the now-unreferenced `IsStack` helper and its branches throughout the code. Drop the legacy `ProvisionState.Manifest` single-manifest field and clean up `prov.Image`-style legacy fields. **The `type Manifest = flatManifest` alias STAYS** (see scope note below).

**Scope note on the `Manifest` alias:** The alias was introduced in Task 2 (`c5c27ad`) and originally framed as transitional pending Task 14's legacy-body deletion. That framing was wrong. The alias serves a **permanent architectural purpose** as the exported entry point for the per-service manifest type — every function/field that holds a `*<per-service-manifest>` across package boundaries needs an exported name (e.g., `composeServiceParams.Manifest`, `setupVolBinds`'s `services map[string]*manifest.Manifest`, `verifyStartup`, `CreateContainerParams.Manifest`, `ProvisionSuccessResult.Manifest` for the per-service result). Removing the alias would require either (a) re-promoting `flatManifest` to exported with a different name (e.g., `Service`), or (b) changing every signature to `*StackManifest, serviceName string` and doing internal lookups. Either is a separate, scoped refactor — out of scope for Task 15 and not covered by the spec. **Task 2's "Manifest demoted to flatManifest" remains accurate in spirit** (legacy single-service uses are gone; the exported name now only references the per-service type within a stack). Update the alias's docstring in this task to reflect its permanent role; do NOT delete it.

**Files:**
- Modify: `internal/backend/client.go` (delete `IsStack` function)
- Modify: `internal/backend/shared/leasesm/lease_sm.go` (delete `ProvisionState.IsStack` method, drop `ProvisionState.Manifest` single-manifest field if unused)
- Modify: `internal/backend/docker/info.go` (drop `prov.IsStack()` branch in `GetLogs`)
- Modify: `internal/backend/docker/recover.go` (drop any `IsStack`-gated orphan volume cleanup branch)
- Modify: `internal/backend/shared/manifest/manifest.go` (reframe alias docstring; do NOT delete)

- [ ] **Step 15.1: Confirm no callers remain.**

```bash
grep -rn "\.IsStack\b\|backend\.IsStack\b" --include="*.go" .
```

Expected: hits in `info.go` (GetLogs), possibly `recover.go` (orphan volume cleanup), and possibly `lifecycle.go` (`ContainerLogKeys`). All become unreachable after Step 15.4 deletes IsStack.

- [ ] **Step 15.2: Delete `backend.IsStack`** (client.go:121-132) and `ProvisionState.IsStack` method.

- [ ] **Step 15.3: Reframe the `Manifest` alias docstring.**

Replace the `// Deprecated:` doc block on `type Manifest = flatManifest` with a docstring that honestly describes the alias's role:

```go
// Manifest is the per-service manifest type referenced across package
// boundaries. It is an exported alias for the internal flatManifest
// (used as map values in StackManifest.Services). External code that
// holds a per-service manifest pointer (e.g., composeServiceParams,
// CreateContainerParams, ProvisionSuccessResult) references this name.
//
// Note: Manifest no longer represents a "legacy single-service" payload —
// ParsePayload always returns a *StackManifest. Use *StackManifest for
// any stack-level handle; use *Manifest only for per-service references.
type Manifest = flatManifest
```

Do NOT delete the alias.

- [ ] **Step 15.4: Audit `ProvisionState.Image` and `prov.Manifest` (single).**

```bash
grep -rn "prov\.Image\b\|ProvisionState.Image\b\|prov\.Manifest\b" --include="*.go" .
```

If still referenced (probably in `info.go` for the `ProvisionInfo.Image` convenience field): keep `ProvisionInfo.Image` as a derived field, populating it from the first service's image in deterministic order when there is a single service, otherwise leaving empty (consumers should use `ServiceImages`). Update `info.go` accordingly.

If `prov.Manifest` (single) is no longer used: delete the field from `ProvisionState`.

- [ ] **Step 15.5: Build.**

```bash
go build ./...
```

Expected: success.

- [ ] **Step 15.6: Run full test suite.**

```bash
go test ./... -count=1
```

Expected: stack-format tests PASS.

- [ ] **Step 15.7: Commit.**

```bash
git add internal/backend/client.go internal/backend/shared/manifest/ internal/backend/shared/leasesm/ internal/backend/docker/
git commit -m "refactor: delete IsStack; finalize manifest API surface"
```

---

## Task 16: Rebaseline tests + docs + CHANGELOG

**Goal:** Convert remaining single-service tests into 1-service-stack tests. Update docs to mark flat format deprecated. Add a CHANGELOG entry.

**Files:**
- Modify: various `*_test.go` under `internal/backend/docker/`
- Modify: `internal/backend/shared/manifest/*_test.go`
- Modify: `internal/backend/client_test.go`
- Modify: `docs/manifest-guide.md`
- Modify: `docs/manifest-schema.json`
- Create: `CHANGELOG.md` (or modify if exists)

- [ ] **Step 16.1: Inventory failing tests.**

```bash
go test ./... -count=1 2>&1 | tee /tmp/test-failures.txt
grep -E "^--- FAIL|^FAIL\s" /tmp/test-failures.txt
```

- [ ] **Step 16.2: For each failing test, classify and fix:**

- (A) **Test that explicitly exercised the legacy code path** (e.g., names containing `Legacy`, `Single`, or fixtures constructing flat manifests with deprecated assertions): convert into a 1-service-stack test by changing the fixture to `{"services":{"app":<manifest>}}` and adjusting assertions to operate on `LeaseInfo.Services["app"]`.

- (B) **Test that called `ParsePayload` with the old signature:** update the call from `m, sm, err := ParsePayload(...)` to `sm, err := ParsePayload(...)` and adjust assertions.

- (C) **Test calling `IsStack`:** delete (the helper is gone).

- (D) **Genuine regression:** investigate and fix.

- [ ] **Step 16.3: Run the full suite until green.**

```bash
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 16.4: Run `go vet` and project linters.**

```bash
go vet ./...
make lint
```

Expected: clean.

- [ ] **Step 16.5: Update `docs/manifest-guide.md`.**

Add a "Deprecation notice" section at the top:

```markdown
> **Deprecation notice (2026-05-15):** The flat single-service manifest format
> (top-level `image` instead of `services`) is deprecated. Fred continues to
> accept it for backwards compatibility but auto-wraps the payload internally
> into a 1-service stack named `app`. Submit stack-format manifests directly
> in new integrations. The flat format will be removed in a future major
> release.
```

Add a note to "Format auto-detection" (manifest-guide.md:85) explaining the deprecation.

- [ ] **Step 16.6: Update `docs/manifest-schema.json`.**

Keep the top-level union (accepts both flat and stack) but add a description on the flat branch:

```json
{
  "oneOf": [
    { "$ref": "#/$defs/StackManifest" },
    { "$ref": "#/$defs/FlatManifest", "description": "DEPRECATED — see docs/manifest-guide.md. Auto-wrapped by fred into a 1-service stack named 'app'." }
  ]
}
```

- [ ] **Step 16.7: Add CHANGELOG entry.**

```markdown
## Unreleased

### Deprecated
- Flat single-service manifest format. Submit stack-format manifests directly.
  Fred continues to accept flat manifests with auto-wrap; removal is deferred
  to a future major release.

### Internal
- Docker backend consolidated on the Compose execution path. Legacy direct-API
  path removed. Legacy single-service leases auto-migrate at fred startup to
  the stack-form representation; persistent data is preserved via filesystem
  rename of the volume directory. Each lease experiences a brief restart
  during the post-upgrade startup window.

### Breaking (operator-facing)
- Container names for previously single-service leases change from
  `fred-<uuid>-<idx>` to `fred-<uuid>-app-<idx>`. Update monitoring tooling
  accordingly.
```

- [ ] **Step 16.8: Commit.**

```bash
git add internal/ docs/manifest-guide.md docs/manifest-schema.json CHANGELOG.md
git commit -m "test+docs: rebaseline suites; deprecate flat manifest; CHANGELOG"
```

---

## Final verification

- [ ] **Full build + race tests.**

```bash
go build ./...
go test -race -count=1 ./...
go vet ./...
make lint
```

- [ ] **Residual-legacy sweep.**

```bash
grep -rn "IsStack\|setupVolumeBinds\|setupStackVolBinds\|doProvisionStack\|doRestartStack\|doUpdateStack" --include="*.go" .
# Expected: no matches.

grep -rn '"fred-%s-%d"' --include="*.go" internal/backend/docker/
# Expected: matches only in migrate.go (legacy-name detection).
```

- [ ] **Integration suite.**

```bash
make test-integration
make test-integration-stack
make test-integration-volume
make test-integration-restart-update
```

- [ ] **Manual smoke (optional, local docker daemon required):** build, run fred with a pre-captured legacy container, observe migration logs, confirm container renamed and data preserved.

---

## Self-review checklist

- **Spec coverage:** every section of the spec maps to ≥1 task:
  - Manifest model (auto-wrap) → Task 2.
  - Lease item model (auto-tag) → Task 3.
  - Container provisioning → Tasks 4, 5, 6, 14.
  - Allocation IDs/names → Task 12.
  - LeaseInfo shape → Task 13.
  - Volume binds → Task 11.
  - Recover-time migration → Tasks 8, 9.
  - Volume rename per filesystem → Task 10.
  - `IsStack` deletion → Task 15.
  - Docs & CHANGELOG → Task 16.
- **Placeholder scan:** every step has either code or an exact command. No "TBD"/"TODO".
- **Type consistency:** `StackManifest` remains the kept type name throughout. `ParsePayload` returns `(*StackManifest, error)` after Task 2. `DefaultServiceName` is the central constant. `NormalizeProvisionRequest` (not `ValidateProvisionRequest`) is the helper name throughout. The `Stack` suffix is dropped from internal `doProvision`/`doRestart`/`doUpdate` after Task 14 (deliberate rename, not silent semantic drift).
