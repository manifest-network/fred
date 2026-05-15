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

## Task 8: Recover-time migration — scaffolding

**Goal:** Add the `migrate.go` file with the migration pipeline scaffolding (struct, plan-build, dry-run logging). No execution yet. Wire `recover.go` to call it (also no-op for now).

**Files:**
- Create: `internal/backend/docker/migrate.go`
- Modify: `internal/backend/docker/recover.go`

- [ ] **Step 8.1: Create `internal/backend/docker/migrate.go` with the migration plan type and a `planLegacyMigration` function.**

```go
package docker

import (
    "context"
    "fmt"
    "log/slog"

    "github.com/manifest-network/fred/internal/backend/shared/manifest"
)

// legacyMigration describes the work needed to recreate one legacy
// single-service container as a stack-form (1-service) Compose project.
type legacyMigration struct {
    LeaseUUID       string
    LegacyContainer ContainerInfo
    InspectedMounts []ContainerMount // existing volume binds from ContainerInspect
    Stack           *manifest.StackManifest
    NewContainerName  string // fred-{uuid}-app-{idx}
    OldVolumeName     string // fred-{uuid}-{idx}
    NewVolumeName     string // fred-{uuid}-app-{idx}
}

// planLegacyMigration assembles a legacyMigration for a given legacy container.
// Reads the persisted manifest from the release store; if absent, reconstructs
// from the container's existing config.
func (b *Backend) planLegacyMigration(ctx context.Context, c ContainerInfo, logger *slog.Logger) (*legacyMigration, error) {
    if c.ServiceName != "" {
        return nil, fmt.Errorf("not a legacy container: service_name=%q", c.ServiceName)
    }

    inspected, err := b.docker.InspectContainer(ctx, c.ContainerID)
    if err != nil {
        return nil, fmt.Errorf("inspect legacy container: %w", err)
    }

    // Reconstruct the manifest.
    var rawManifest []byte
    if b.releaseStore != nil {
        if rel, relErr := b.releaseStore.LatestActive(c.LeaseUUID); relErr == nil && rel != nil && len(rel.Manifest) > 0 {
            rawManifest = rel.Manifest
        }
    }
    var stack *manifest.StackManifest
    if rawManifest != nil {
        stack, err = manifest.ParsePayload(rawManifest)
        if err != nil {
            return nil, fmt.Errorf("parse stored manifest: %w", err)
        }
    } else {
        // Reconstruct minimal manifest from container inspect.
        stack, err = reconstructManifestFromContainer(inspected)
        if err != nil {
            return nil, fmt.Errorf("reconstruct manifest: %w", err)
        }
        logger.Warn("legacy migration: release store missing entry; reconstructed from container",
            "lease_uuid", c.LeaseUUID, "container_id", c.ContainerID)
    }

    return &legacyMigration{
        LeaseUUID:        c.LeaseUUID,
        LegacyContainer:  c,
        InspectedMounts:  inspected.Mounts,
        Stack:            stack,
        NewContainerName: fmt.Sprintf("fred-%s-%s-%d", c.LeaseUUID, manifest.DefaultServiceName, c.InstanceIndex),
        OldVolumeName:    fmt.Sprintf("fred-%s-%d", c.LeaseUUID, c.InstanceIndex),
        NewVolumeName:    fmt.Sprintf("fred-%s-%s-%d", c.LeaseUUID, manifest.DefaultServiceName, c.InstanceIndex),
    }, nil
}

// reconstructManifestFromContainer builds a minimal 1-service stack manifest
// from an inspected container. Falls back when the release store has no entry.
func reconstructManifestFromContainer(inspected ContainerInspect) (*manifest.StackManifest, error) {
    // Minimal: image + env + ports + healthcheck.
    // Detailed implementation deferred to QA — extract ports from inspected.NetworkSettings.Ports,
    // env from inspected.Config.Env, healthcheck from inspected.Config.Healthcheck.
    // Return an error if the container has volumes but no manifest record exists, since the
    // tmpfs/user/etc. cannot be safely inferred.
    return nil, fmt.Errorf("manifest reconstruction not implemented; release store entry required for lease %s", inspected.LeaseUUID())
}
```

(The `ContainerInspect`, `ContainerMount` types live in `lifecycle.go` — reuse rather than redefine. If `LeaseUUID()` is not a method on `ContainerInspect`, read it from `inspected.Config.Labels[LabelLeaseUUID]`.)

- [ ] **Step 8.2: At the top of `recoverState` in `recover.go` (after the `ListManagedContainers` call), iterate to find legacy containers and call `planLegacyMigration` for each — log the plan but do not execute yet.**

```go
    // Legacy migration pre-pass: detect any container missing fred.service_name
    // and plan its migration. Actual execution lands in Task 9.
    var legacyMigrations []*legacyMigration
    for _, c := range containers {
        if c.LeaseUUID != "" && c.ServiceName == "" {
            plan, err := b.planLegacyMigration(ctx, c, b.logger)
            if err != nil {
                return fmt.Errorf("plan legacy migration for lease %s container %s: %w",
                    c.LeaseUUID, c.ContainerID, err)
            }
            legacyMigrations = append(legacyMigrations, plan)
            b.logger.Info("legacy container migration planned",
                "lease_uuid", plan.LeaseUUID,
                "container_id", plan.LegacyContainer.ContainerID,
                "new_container_name", plan.NewContainerName,
                "old_volume", plan.OldVolumeName,
                "new_volume", plan.NewVolumeName,
            )
        }
    }
    if len(legacyMigrations) > 0 {
        // Execution lives in Task 9; for now, abort startup with a clear message.
        return fmt.Errorf("legacy migration planning complete; execution pending (Task 9)")
    }
```

- [ ] **Step 8.3: Build.**

```bash
go build ./...
```

Expected: success.

- [ ] **Step 8.4: Run migration plan tests.**

```bash
go test ./internal/backend/docker/ -run "TestRecoverState_MigratesLegacyContainer" -v
```

Expected: still failing (execution not implemented). That's acceptable — Task 9 makes it pass.

- [ ] **Step 8.5: Commit.**

```bash
git add internal/backend/docker/migrate.go internal/backend/docker/recover.go
git commit -m "feat(docker): scaffold legacy→stack recover-time migration"
```

---

## Task 9: Recover-time migration — execution

**Goal:** Implement `executeLegacyMigration` doing the full rename + recreate.

**Files:**
- Modify: `internal/backend/docker/migrate.go`
- Modify: `internal/backend/docker/recover.go`

- [ ] **Step 9.1: Add `executeLegacyMigration` to `migrate.go`.**

```go
func (b *Backend) executeLegacyMigration(ctx context.Context, m *legacyMigration, logger *slog.Logger) error {
    logger = logger.With("lease_uuid", m.LeaseUUID, "container_id", m.LegacyContainer.ContainerID)
    logger.Info("legacy migration starting")

    // 1. Volume directory rename (idempotent).
    if err := b.volumeBackend.RenameVolume(m.OldVolumeName, m.NewVolumeName); err != nil {
        return fmt.Errorf("rename volume %s→%s: %w", m.OldVolumeName, m.NewVolumeName, err)
    }
    logger.Info("volume renamed", "old", m.OldVolumeName, "new", m.NewVolumeName)

    // 2. Stop + rename legacy container.
    stopGrace := 10 * time.Second
    if svc := m.Stack.Services[manifest.DefaultServiceName]; svc != nil && svc.StopGracePeriod != nil {
        stopGrace = time.Duration(*svc.StopGracePeriod)
    }
    if err := b.docker.StopContainer(ctx, m.LegacyContainer.ContainerID, stopGrace); err != nil {
        // Tolerate already-stopped.
        logger.Warn("stop legacy container returned error (continuing)", "error", err)
    }
    prevName := m.NewContainerName + "-prev"
    if err := b.docker.RenameContainer(ctx, m.LegacyContainer.ContainerID, prevName); err != nil {
        return fmt.Errorf("rename legacy container to %s: %w", prevName, err)
    }

    // 3. Build the Compose project and bring it up.
    profile, err := b.cfg.GetSKUProfile(m.LegacyContainer.SKU)
    if err != nil {
        return fmt.Errorf("load SKU profile: %w", err)
    }
    items := []backend.LeaseItem{{
        SKU: m.LegacyContainer.SKU, Quantity: 1, ServiceName: manifest.DefaultServiceName,
    }}
    project, err := buildComposeProject(composeProjectParams{
        LeaseUUID:  m.LeaseUUID,
        Tenant:     m.LegacyContainer.Tenant,
        Stack:      m.Stack,
        Items:      items,
        Profiles:   map[string]SKUProfile{m.LegacyContainer.SKU: profile},
        // Reuse the volume path we just renamed: pass the bind override via
        // composeProjectParams.VolumeBinds.
        VolumeBinds: map[string]map[string]string{
            manifest.DefaultServiceName: {
                m.InspectedMounts[0].Target: filepath.Join(b.cfg.VolumeRoot, m.NewVolumeName),
            },
        },
    })
    if err != nil {
        return fmt.Errorf("build compose project: %w", err)
    }
    if err := b.composeService.Up(ctx, project, composeUpOpts{}); err != nil {
        return fmt.Errorf("compose up: %w", err)
    }

    // 4. Wait for health/run.
    if err := b.waitForServiceReady(ctx, m.LeaseUUID, manifest.DefaultServiceName, m.Stack.Services[manifest.DefaultServiceName].HealthCheck, defaultMigrationReadyTimeout); err != nil {
        return fmt.Errorf("wait for service ready: %w", err)
    }

    // 5. Schedule removal of the -prev container after the grace window.
    go func() {
        select {
        case <-time.After(b.cfg.MigrationGracePeriod):
        case <-ctx.Done():
            return
        }
        if err := b.docker.RemoveContainer(context.Background(), prevName, true); err != nil {
            logger.Warn("remove -prev container after grace failed (manual cleanup may be needed)",
                "name", prevName, "error", err)
        }
    }()

    // 6. Persist wrapped manifest into release store.
    if b.releaseStore != nil {
        if data, mErr := json.Marshal(m.Stack); mErr == nil {
            if persistErr := b.releaseStore.RecordMigration(m.LeaseUUID, data); persistErr != nil {
                logger.Warn("release store update failed (migration still complete)", "error", persistErr)
            }
        }
    }

    logger.Info("legacy migration complete")
    return nil
}

const defaultMigrationReadyTimeout = 90 * time.Second
```

(`composeProjectParams.VolumeBinds`, `b.composeService`, `b.volumeBackend`, `b.cfg.MigrationGracePeriod`, `b.releaseStore.RecordMigration` are anticipated to exist; if not, add them in this same task in the relevant files. `waitForServiceReady` likewise — if the stack provision flow has an equivalent helper, factor it out and reuse.)

- [ ] **Step 9.2: In `recover.go`, replace the placeholder `return fmt.Errorf("...pending Task 9...")` with the execution loop.**

```go
    for _, plan := range legacyMigrations {
        if err := b.executeLegacyMigration(ctx, plan, b.logger); err != nil {
            return fmt.Errorf("legacy migration FAILED: lease %s: %w\n"+
                "fred refuses to start with unmigrated legacy containers. "+
                "Investigate the failure cause; re-run fred (migration is idempotent), or "+
                "deprovision the lease manually if data loss is acceptable.",
                plan.LeaseUUID, err)
        }
    }
```

- [ ] **Step 9.3: Build.**

```bash
go build ./...
```

Expected: success. Add any helper types/fields that `executeLegacyMigration` referenced if the compiler complains; do not silently delete references.

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
git add internal/backend/docker/migrate.go internal/backend/docker/recover.go
git commit -m "feat(docker): execute legacy→stack migration at recover time"
```

---

## Task 10: VolumeBackend.RenameVolume per filesystem

**Goal:** Add `RenameVolume` to the volume backend interface and implement it for xfs, btrfs, zfs. Idempotent.

**Files:**
- Modify: `internal/backend/docker/volume.go`
- Modify: `internal/backend/docker/volume_xfs.go`
- Modify: `internal/backend/docker/volume_btrfs.go`
- Modify: `internal/backend/docker/volume_zfs.go`

- [ ] **Step 10.1: Add to the `VolumeBackend` interface in `volume.go`:**

```go
// RenameVolume atomically renames a managed volume from oldName to newName.
// Idempotent: if newName exists and oldName does not, returns nil. Fails
// loudly if both exist (operator must intervene).
RenameVolume(oldName, newName string) error
```

- [ ] **Step 10.2: Implement for xfs in `volume_xfs.go`:**

```go
func (b *xfsVolumeBackend) RenameVolume(oldName, newName string) error {
    oldPath := filepath.Join(b.root, oldName)
    newPath := filepath.Join(b.root, newName)
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
func (b *btrfsVolumeBackend) RenameVolume(oldName, newName string) error {
    oldPath := filepath.Join(b.root, oldName)
    newPath := filepath.Join(b.root, newName)
    return atomicRenameVolumeDir(oldPath, newPath)
}
```

- [ ] **Step 10.4: Implement for zfs in `volume_zfs.go` via `zfs rename` shellout (matching the style of other zfs commands in this file):**

```go
func (b *zfsVolumeBackend) RenameVolume(oldName, newName string) error {
    oldDataset := b.dataset + "/" + oldName
    newDataset := b.dataset + "/" + newName
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
    b := &xfsVolumeBackend{root: root}
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

**Goal:** Delete `setupVolumeBinds`; rename `setupStackVolBinds` → `setupVolBinds`.

**Files:**
- Modify: `internal/backend/docker/provision.go`

- [ ] **Step 11.1: Confirm no callers of `setupVolumeBinds` remain after Tasks 4-7.**

```bash
grep -rn "setupVolumeBinds\b" internal/backend/docker/ --include="*.go" | grep -v _test.go
```

Expected: only the function definition. If any caller remains, redirect it to the stack helper.

- [ ] **Step 11.2: Delete the function (provision.go:421-516).**

- [ ] **Step 11.3: Rename.**

```bash
grep -rln "setupStackVolBinds" internal/backend/docker/ --include="*.go" | xargs sed -i 's/setupStackVolBinds/setupVolBinds/g'
```

- [ ] **Step 11.4: Build + test.**

```bash
go build ./...
go test ./internal/backend/docker/ -run "Volume" -count=1
```

Expected: PASS.

- [ ] **Step 11.5: Commit.**

```bash
git add internal/backend/docker/
git commit -m "refactor(docker): collapse volume bind helpers into one"
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

**Goal:** Now that nothing routes to them, delete the legacy `doProvision`, `doRestart`, `doUpdate`. Rename `*Stack` versions to drop the suffix.

**Files:**
- Modify: `internal/backend/docker/provision.go`
- Modify: `internal/backend/docker/restart_update.go`

- [ ] **Step 14.1: Identify the legacy `doProvision` function span (provision.go:608–876) and delete it entirely.**

```bash
grep -n "^func (b \*Backend) doProvision\b\|^func (b \*Backend) doProvisionStack" internal/backend/docker/provision.go
```

Use the line numbers to locate the function body. Delete the legacy version.

- [ ] **Step 14.2: Rename `doProvisionStack`→`doProvision`.**

```bash
grep -rln "doProvisionStack" internal/backend/docker/ --include="*.go" | xargs sed -i 's/doProvisionStack/doProvision/g'
```

- [ ] **Step 14.3: Same for `doRestart` and `doUpdate` in `restart_update.go`.**

```bash
grep -n "^func (b \*Backend) doRestart\b\|^func (b \*Backend) doRestartStack\|^func (b \*Backend) doUpdate\b\|^func (b \*Backend) doUpdateStack" internal/backend/docker/restart_update.go
```

Delete legacy versions; rename stack versions:

```bash
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
git commit -m "refactor(docker): delete legacy doProvision/doRestart/doUpdate; drop Stack suffix"
```

---

## Task 15: Delete `IsStack` and finalize the manifest API

**Goal:** Remove the now-unreferenced `IsStack` helper. Clean up `prov.Image`-style legacy fields if they remain unused.

**Files:**
- Modify: `internal/backend/client.go`
- Modify: `internal/backend/shared/leasesm/leasesm.go`, `lease_sm.go`

- [ ] **Step 15.1: Confirm no callers remain.**

```bash
grep -rn "\.IsStack\b\|backend\.IsStack\b" --include="*.go" .
```

Expected: no matches. If matches remain, update them to use the unified path.

- [ ] **Step 15.2: Delete `IsStack` (client.go:121-132).**

- [ ] **Step 15.3: Audit `ProvisionState.Image` and `prov.Manifest` (single).**

```bash
grep -rn "prov\.Image\b\|ProvisionState.Image\b\|prov\.Manifest\b" --include="*.go" .
```

If still referenced (probably in `info.go` for the `ProvisionInfo.Image` convenience field): keep `ProvisionInfo.Image` as a derived field, populating it from the first service's image in deterministic order when there is a single service, otherwise leaving empty (consumers should use `ServiceImages`). Update `info.go` accordingly.

If `prov.Manifest` (single) is no longer used: delete the field from `ProvisionState`.

- [ ] **Step 15.4: Build.**

```bash
go build ./...
```

Expected: success.

- [ ] **Step 15.5: Run full test suite.**

```bash
go test ./... -count=1
```

Expected: stack-format tests PASS.

- [ ] **Step 15.6: Commit.**

```bash
git add internal/backend/client.go internal/backend/shared/leasesm/ internal/backend/docker/
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
